use std::{path::Path, sync::Mutex};

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::Value;

use crate::{
    AcpEvent, AcpEventDirection, AcpEventKind, AcpNegotiation, AcpProtocolProfile, AgentHostError,
    AgentInputMode, AgentLifecycle, AgentSession, AuthorityAttestation, CompactionReporting,
    EventPage, FilesystemAuthority, NetworkAuthority, PermissionAuthority, PermissionDecision,
    PermissionResolution, PromptAccepted, PromptDisposition, RootAuthority, SandboxAuthority,
    SessionStatus, SteeringSupport,
};

const SCHEMA_VERSION: i64 = 2;
const MAX_EVENT_PAGE: u64 = 1_000;

pub(crate) struct AgentStore {
    connection: Mutex<Connection>,
}

pub(crate) struct RecoverableSession {
    pub(crate) native_session_id: String,
    pub(crate) agent_id: String,
    pub(crate) working_directory: String,
    pub(crate) metadata_json: String,
    pub(crate) protocol_profile: AcpProtocolProfile,
}

impl AgentStore {
    pub(crate) fn open(path: impl AsRef<Path>) -> Result<Self, AgentHostError> {
        Self::initialize(Connection::open(path).map_err(storage_error)?)
    }

    pub(crate) fn open_in_memory() -> Result<Self, AgentHostError> {
        Self::initialize(Connection::open_in_memory().map_err(storage_error)?)
    }

    fn initialize(mut connection: Connection) -> Result<Self, AgentHostError> {
        connection
            .execute_batch(
                "PRAGMA busy_timeout = 5000;
                 PRAGMA foreign_keys = ON;
                 PRAGMA journal_mode = WAL;
                 PRAGMA synchronous = FULL;",
            )
            .map_err(storage_error)?;
        let version = connection
            .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
            .map_err(storage_error)?;
        match version {
            0 => {
                migrate_v0_to_v1(&mut connection)?;
                migrate_v1_to_v2(&mut connection)?;
            }
            1 => migrate_v1_to_v2(&mut connection)?,
            SCHEMA_VERSION => {}
            _ => return Err(AgentHostError::StorageUnavailable),
        }

        let store = Self {
            connection: Mutex::new(connection),
        };
        let interrupted_sessions = {
            let connection = store.lock()?;
            let mut statement = connection
                .prepare(
                    "SELECT session_id, updated_at_ms FROM sessions
                     WHERE lifecycle IN ('Starting', 'Ready', 'Busy', 'Detaching', 'Stopping')",
                )
                .map_err(storage_error)?;
            statement
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                })
                .map_err(storage_error)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(storage_error)?
        };
        for (session_id, updated_at_ms) in interrupted_sessions {
            store.set_lifecycle(&session_id, &AgentLifecycle::Failed, db_u64(updated_at_ms)?)?;
        }
        Ok(store)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_starting_session(
        &self,
        session_id: &str,
        agent_id: &str,
        working_directory: &str,
        metadata_json: &str,
        protocol_profile: &AcpProtocolProfile,
        authority: &AuthorityAttestation,
        now_ms: u64,
    ) -> Result<(), AgentHostError> {
        let connection = self.lock()?;
        connection
            .execute(
                "INSERT INTO sessions (
                    session_id, native_session_id, agent_id, working_directory, metadata_json,
                    lifecycle, protocol_profile, protocol_version, steering, compaction,
                    capabilities_json, authority_verified_at_ms, authority_evidence_json,
                    active_run_id, last_sequence, created_at_ms, updated_at_ms
                 ) VALUES (?1, '', ?2, ?3, ?4, 'Starting', ?5, 0, 'TurnBoundaryQueue',
                           'OpaqueAgentManaged', '{}', ?6, ?7, NULL, 0, ?8, ?8)",
                params![
                    session_id,
                    agent_id,
                    working_directory,
                    metadata_json,
                    protocol_profile_tag(protocol_profile)?,
                    db_i64(authority.verified_at_ms)?,
                    authority.evidence_json,
                    db_i64(now_ms)?,
                ],
            )
            .map_err(storage_error)?;
        Ok(())
    }

    pub(crate) fn set_ready(
        &self,
        session_id: &str,
        native_session_id: &str,
        negotiation: &AcpNegotiation,
        now_ms: u64,
    ) -> Result<AgentSession, AgentHostError> {
        let connection = self.lock()?;
        let changed = connection
            .execute(
                "UPDATE sessions
                 SET native_session_id = ?2, lifecycle = 'Ready', protocol_profile = ?3,
                     protocol_version = ?4, steering = ?5, compaction = ?6,
                     capabilities_json = ?7, updated_at_ms = ?8
                 WHERE session_id = ?1 AND lifecycle = 'Starting'",
                params![
                    session_id,
                    native_session_id,
                    protocol_profile_tag(&negotiation.protocol_profile)?,
                    db_i64(negotiation.protocol_version)?,
                    steering_tag(&negotiation.steering)?,
                    compaction_tag(&negotiation.compaction_reporting)?,
                    negotiation.agent_capabilities_json,
                    db_i64(now_ms)?,
                ],
            )
            .map_err(storage_error)?;
        if changed != 1 {
            return Err(AgentHostError::UnknownSession);
        }
        drop(connection);
        self.session(session_id)
    }

    pub(crate) fn session(&self, session_id: &str) -> Result<AgentSession, AgentHostError> {
        let connection = self.lock()?;
        connection
            .query_row(
                "SELECT native_session_id, agent_id, protocol_version, protocol_profile,
                        steering, compaction, capabilities_json, authority_verified_at_ms,
                        authority_evidence_json
                 FROM sessions WHERE session_id = ?1",
                params![session_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, i64>(7)?,
                        row.get::<_, String>(8)?,
                    ))
                },
            )
            .optional()
            .map_err(storage_error)?
            .ok_or(AgentHostError::UnknownSession)
            .and_then(
                |(
                    native_session_id,
                    agent_id,
                    protocol_version,
                    profile,
                    steering,
                    compaction,
                    capabilities,
                    verified_at,
                    evidence,
                )| {
                    Ok(AgentSession {
                        session_id: session_id.to_owned(),
                        native_session_id,
                        agent_id,
                        negotiation: AcpNegotiation {
                            protocol_version: db_u64(protocol_version)?,
                            protocol_profile: parse_protocol_profile(&profile)?,
                            steering: parse_steering(&steering)?,
                            compaction_reporting: parse_compaction(&compaction)?,
                            agent_capabilities_json: capabilities,
                        },
                        authority: authority(db_u64(verified_at)?, evidence),
                    })
                },
            )
    }

    pub(crate) fn recoverable_session(
        &self,
        session_id: &str,
    ) -> Result<RecoverableSession, AgentHostError> {
        let connection = self.lock()?;
        let row = connection
            .query_row(
                "SELECT native_session_id, agent_id, working_directory, metadata_json,
                        protocol_profile, lifecycle
                 FROM sessions WHERE session_id = ?1",
                params![session_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                    ))
                },
            )
            .optional()
            .map_err(storage_error)?
            .ok_or(AgentHostError::UnknownSession)?;
        if !matches!(row.5.as_str(), "Failed" | "Detached") || row.0.trim().is_empty() {
            return Err(AgentHostError::SessionResumeUnavailable);
        }
        Ok(RecoverableSession {
            native_session_id: row.0,
            agent_id: row.1,
            working_directory: row.2,
            metadata_json: row.3,
            protocol_profile: parse_protocol_profile(&row.4)?,
        })
    }

    pub(crate) fn prepare_resume(
        &self,
        session_id: &str,
        authority: &AuthorityAttestation,
        now_ms: u64,
    ) -> Result<(), AgentHostError> {
        let connection = self.lock()?;
        let changed = connection
            .execute(
                "UPDATE sessions
                 SET lifecycle = 'Starting', authority_verified_at_ms = ?2,
                     authority_evidence_json = ?3, active_run_id = NULL, updated_at_ms = ?4
                 WHERE session_id = ?1 AND lifecycle IN ('Failed', 'Detached')
                       AND native_session_id <> ''",
                params![
                    session_id,
                    db_i64(authority.verified_at_ms)?,
                    authority.evidence_json,
                    db_i64(now_ms)?,
                ],
            )
            .map_err(storage_error)?;
        if changed != 1 {
            return Err(AgentHostError::SessionResumeUnavailable);
        }
        Ok(())
    }

    pub(crate) fn lifecycle_for_agent(
        &self,
        agent_id: &str,
    ) -> Result<AgentLifecycle, AgentHostError> {
        let connection = self.lock()?;
        let lifecycle = connection
            .query_row(
                "SELECT lifecycle FROM sessions WHERE agent_id = ?1
                 ORDER BY CASE lifecycle
                    WHEN 'Busy' THEN 0 WHEN 'Starting' THEN 1 WHEN 'Ready' THEN 2
                    WHEN 'Stopping' THEN 3 ELSE 4 END,
                    updated_at_ms DESC
                 LIMIT 1",
                params![agent_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(storage_error)?;
        match lifecycle.as_deref() {
            Some("Stopped") | None => Ok(AgentLifecycle::Discovered),
            Some(lifecycle) => parse_lifecycle(lifecycle),
        }
    }

    pub(crate) fn status(&self, session_id: &str) -> Result<SessionStatus, AgentHostError> {
        let connection = self.lock()?;
        connection
            .query_row(
                "SELECT lifecycle, last_sequence, active_run_id FROM sessions WHERE session_id = ?1",
                params![session_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(storage_error)?
            .ok_or(AgentHostError::UnknownSession)
            .and_then(|(lifecycle, last_sequence, active_run_id)| {
                Ok(SessionStatus {
                    session_id: session_id.to_owned(),
                    lifecycle: parse_lifecycle(&lifecycle)?,
                    last_sequence: db_u64(last_sequence)?,
                    active_run_id,
                })
            })
    }

    pub(crate) fn accept_prompt(
        &self,
        request: &crate::PromptRequest,
        run_id: &str,
        disposition: &PromptDisposition,
        activate_run: bool,
        interruption: Option<(&str, u64)>,
        now_ms: u64,
    ) -> Result<(PromptAccepted, bool), AgentHostError> {
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        if let Some(existing) =
            query_prompt(&transaction, &request.session_id, &request.client_turn_id)?
        {
            let matches = existing.mode == input_mode_tag(&request.mode)?
                && existing.native_prompt_json == request.native_prompt_json;
            if !matches {
                return Err(AgentHostError::DuplicateTurnConflict);
            }
            let accepted = existing.accepted(&request.session_id)?;
            transaction.commit().map_err(storage_error)?;
            return Ok((accepted, false));
        }

        let state = match disposition {
            PromptDisposition::QueuedForTurnBoundary
            | PromptDisposition::CancelRequestedThenQueued => "Queued",
            PromptDisposition::StartedForegroundWork
            | PromptDisposition::ContributedToActiveWork => "Running",
            PromptDisposition::Unknown { .. } => return Err(AgentHostError::StorageUnavailable),
        };
        let expects_interruption =
            matches!(disposition, PromptDisposition::CancelRequestedThenQueued);
        if expects_interruption != interruption.is_some()
            || (activate_run && interruption.is_some())
        {
            return Err(AgentHostError::StorageUnavailable);
        }
        let interrupted_run_id = interruption.map(|(run_id, _)| run_id);
        let cancel_requested_at_ms = interruption
            .map(|(_, requested_at_ms)| db_i64(requested_at_ms))
            .transpose()?;
        transaction
            .execute(
                "INSERT INTO prompts (
                    session_id, client_turn_id, run_id, mode, native_prompt_json,
                    disposition, state, accepted_at_ms, interrupted_run_id,
                    cancel_requested_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    request.session_id,
                    request.client_turn_id,
                    run_id,
                    input_mode_tag(&request.mode)?,
                    request.native_prompt_json,
                    disposition_tag(disposition)?,
                    state,
                    db_i64(now_ms)?,
                    interrupted_run_id,
                    cancel_requested_at_ms,
                ],
            )
            .map_err(storage_error)?;
        if activate_run {
            let changed = transaction
                .execute(
                    "UPDATE sessions SET lifecycle = 'Busy', active_run_id = ?2, updated_at_ms = ?3
                     WHERE session_id = ?1 AND lifecycle IN ('Ready', 'Busy')",
                    params![request.session_id, run_id, db_i64(now_ms)?],
                )
                .map_err(storage_error)?;
            if changed != 1 {
                return Err(AgentHostError::SessionClosed);
            }
        }
        transaction.commit().map_err(storage_error)?;
        Ok((
            PromptAccepted {
                session_id: request.session_id.clone(),
                run_id: run_id.to_owned(),
                accepted_at_ms: now_ms,
                disposition: disposition.clone(),
                interrupted_run_id: interrupted_run_id.map(str::to_owned),
                cancel_requested_at_ms: cancel_requested_at_ms.map(db_u64).transpose()?,
            },
            true,
        ))
    }

    pub(crate) fn existing_prompt(
        &self,
        request: &crate::PromptRequest,
    ) -> Result<Option<PromptAccepted>, AgentHostError> {
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(storage_error)?;
        let Some(existing) =
            query_prompt(&transaction, &request.session_id, &request.client_turn_id)?
        else {
            return Ok(None);
        };
        if existing.mode != input_mode_tag(&request.mode)?
            || existing.native_prompt_json != request.native_prompt_json
        {
            return Err(AgentHostError::DuplicateTurnConflict);
        }
        let accepted = existing.accepted(&request.session_id)?;
        transaction.commit().map_err(storage_error)?;
        Ok(Some(accepted))
    }

    pub(crate) fn activate_queued_run(
        &self,
        session_id: &str,
        run_id: &str,
        now_ms: u64,
    ) -> Result<(), AgentHostError> {
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let changed = transaction
            .execute(
                "UPDATE prompts SET state = 'Running'
                 WHERE session_id = ?1 AND run_id = ?2 AND state = 'Queued'",
                params![session_id, run_id],
            )
            .map_err(storage_error)?;
        if changed != 1 {
            return Err(AgentHostError::UnknownRun);
        }
        transaction
            .execute(
                "UPDATE sessions SET lifecycle = 'Busy', active_run_id = ?2, updated_at_ms = ?3
                 WHERE session_id = ?1 AND lifecycle = 'Ready'",
                params![session_id, run_id, db_i64(now_ms)?],
            )
            .map_err(storage_error)?;
        transaction.commit().map_err(storage_error)?;
        Ok(())
    }

    pub(crate) fn complete_run(
        &self,
        session_id: &str,
        run_id: &str,
        state: &str,
        now_ms: u64,
    ) -> Result<(), AgentHostError> {
        let outcome = match state {
            "Completed" => "completed",
            "Failed" => "failed",
            _ => return Err(AgentHostError::StorageUnavailable),
        };
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let changed = transaction
            .execute(
                "UPDATE prompts SET state = ?3 WHERE session_id = ?1 AND run_id = ?2",
                params![session_id, run_id, state],
            )
            .map_err(storage_error)?;
        if changed == 0 {
            return Err(AgentHostError::UnknownRun);
        }
        ensure_run_finished_event(&transaction, session_id, run_id, outcome, now_ms)?;
        let changed = transaction
            .execute(
                "UPDATE sessions SET lifecycle = 'Ready', active_run_id = NULL, updated_at_ms = ?3
                 WHERE session_id = ?1 AND active_run_id = ?2",
                params![session_id, run_id, db_i64(now_ms)?],
            )
            .map_err(storage_error)?;
        if changed != 1 {
            return Err(AgentHostError::UnknownRun);
        }
        transaction.commit().map_err(storage_error)?;
        Ok(())
    }

    pub(crate) fn fail_prompt(
        &self,
        session_id: &str,
        client_turn_id: &str,
    ) -> Result<(), AgentHostError> {
        let connection = self.lock()?;
        let changed = connection
            .execute(
                "UPDATE prompts SET state = 'Failed'
                 WHERE session_id = ?1 AND client_turn_id = ?2",
                params![session_id, client_turn_id],
            )
            .map_err(storage_error)?;
        if changed != 1 {
            return Err(AgentHostError::UnknownRun);
        }
        Ok(())
    }

    pub(crate) fn cancel_queued_run(
        &self,
        session_id: &str,
        run_id: &str,
    ) -> Result<bool, AgentHostError> {
        let connection = self.lock()?;
        let changed = connection
            .execute(
                "UPDATE prompts SET state = 'Cancelled'
                 WHERE session_id = ?1 AND run_id = ?2 AND state = 'Queued'",
                params![session_id, run_id],
            )
            .map_err(storage_error)?;
        Ok(changed == 1)
    }

    pub(crate) fn set_lifecycle(
        &self,
        session_id: &str,
        lifecycle: &AgentLifecycle,
        now_ms: u64,
    ) -> Result<(), AgentHostError> {
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        if matches!(
            lifecycle,
            AgentLifecycle::Detached | AgentLifecycle::Failed | AgentLifecycle::Stopped
        ) {
            let unfinished_runs = {
                let mut statement = transaction
                    .prepare(
                        "SELECT DISTINCT run_id FROM prompts
                         WHERE session_id = ?1 AND state IN ('Queued', 'Running')
                         ORDER BY accepted_at_ms, run_id",
                    )
                    .map_err(storage_error)?;
                statement
                    .query_map(params![session_id], |row| row.get::<_, String>(0))
                    .map_err(storage_error)?
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(storage_error)?
            };
            for run_id in unfinished_runs {
                ensure_run_finished_event(&transaction, session_id, &run_id, "failed", now_ms)?;
            }
        }
        let changed = transaction
            .execute(
                "UPDATE sessions SET lifecycle = ?2, active_run_id = CASE
                    WHEN ?2 IN ('Ready', 'Busy') THEN active_run_id ELSE NULL END,
                    updated_at_ms = ?3 WHERE session_id = ?1",
                params![session_id, lifecycle_tag(lifecycle)?, db_i64(now_ms)?],
            )
            .map_err(storage_error)?;
        if changed != 1 {
            return Err(AgentHostError::UnknownSession);
        }
        if matches!(
            lifecycle,
            AgentLifecycle::Detached | AgentLifecycle::Failed | AgentLifecycle::Stopped
        ) {
            transaction
                .execute(
                    "UPDATE prompts SET state = 'Failed'
                     WHERE session_id = ?1 AND state IN ('Queued', 'Running')",
                    params![session_id],
                )
                .map_err(storage_error)?;
        }
        transaction.commit().map_err(storage_error)
    }

    pub(crate) fn record_native_line(
        &self,
        session_id: &str,
        direction: AcpEventDirection,
        line: &str,
        now_ms: u64,
    ) -> Result<(), AgentHostError> {
        let parsed = serde_json::from_str::<Value>(line).ok();
        let kind = classify_event(parsed.as_ref());
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let (last_sequence, active_run_id) = transaction
            .query_row(
                "SELECT last_sequence, active_run_id FROM sessions WHERE session_id = ?1",
                params![session_id],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<String>>(1)?)),
            )
            .optional()
            .map_err(storage_error)?
            .ok_or(AgentHostError::UnknownSession)?;
        let sequence = last_sequence
            .checked_add(1)
            .ok_or(AgentHostError::StorageUnavailable)?;
        transaction
            .execute(
                "INSERT INTO events (
                    session_id, sequence, run_id, observed_at_ms, kind, direction, native_event_json
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    session_id,
                    sequence,
                    active_run_id,
                    db_i64(now_ms)?,
                    event_kind_tag(&kind)?,
                    direction_tag(&direction)?,
                    line,
                ],
            )
            .map_err(storage_error)?;
        transaction
            .execute(
                "UPDATE sessions SET last_sequence = ?2, updated_at_ms = ?3 WHERE session_id = ?1",
                params![session_id, sequence, db_i64(now_ms)?],
            )
            .map_err(storage_error)?;

        if matches!(direction, AcpEventDirection::AgentToClient)
            && let Some((request_id, native_request)) = permission_request(parsed.as_ref(), line)
        {
            transaction
                .execute(
                    "INSERT INTO permissions (
                        session_id, request_id, native_request_json, native_response_json,
                        resolved_at_ms
                     ) VALUES (?1, ?2, ?3, NULL, NULL)
                     ON CONFLICT(session_id, request_id) DO UPDATE
                     SET native_request_json = excluded.native_request_json",
                    params![session_id, request_id, native_request],
                )
                .map_err(storage_error)?;
        }
        transaction.commit().map_err(storage_error)?;
        Ok(())
    }

    pub(crate) fn read_events(
        &self,
        session_id: &str,
        after_sequence: u64,
        limit: u64,
    ) -> Result<EventPage, AgentHostError> {
        if limit > MAX_EVENT_PAGE {
            return Err(AgentHostError::InvalidCursor);
        }
        let connection = self.lock()?;
        let last_sequence = connection
            .query_row(
                "SELECT last_sequence FROM sessions WHERE session_id = ?1",
                params![session_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(storage_error)?
            .ok_or(AgentHostError::UnknownSession)
            .and_then(db_u64)?;
        if after_sequence > last_sequence {
            return Err(AgentHostError::InvalidCursor);
        }
        let mut statement = connection
            .prepare(
                "SELECT sequence, run_id, observed_at_ms, kind, direction, native_event_json
                 FROM events WHERE session_id = ?1 AND sequence > ?2
                 ORDER BY sequence ASC LIMIT ?3",
            )
            .map_err(storage_error)?;
        let rows = statement
            .query_map(
                params![session_id, db_i64(after_sequence)?, db_i64(limit)?],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                    ))
                },
            )
            .map_err(storage_error)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(storage_error)?;
        let events = rows
            .into_iter()
            .map(
                |(sequence, run_id, observed_at_ms, kind, direction, native_event_json)| {
                    Ok(AcpEvent {
                        session_id: session_id.to_owned(),
                        run_id,
                        sequence: db_u64(sequence)?,
                        observed_at_ms: db_u64(observed_at_ms)?,
                        kind: parse_event_kind(&kind)?,
                        direction: parse_direction(&direction)?,
                        native_event_json,
                    })
                },
            )
            .collect::<Result<Vec<_>, AgentHostError>>()?;
        let next_sequence = events.last().map_or(after_sequence, |event| event.sequence);
        Ok(EventPage {
            events,
            next_sequence,
            caught_up: next_sequence == last_sequence,
        })
    }

    pub(crate) fn record_permission_resolution(
        &self,
        session_id: &str,
        request_id: &str,
        native_request_json: &str,
        native_response_json: &str,
        now_ms: u64,
    ) -> Result<(), AgentHostError> {
        let connection = self.lock()?;
        connection
            .execute(
                "INSERT INTO permissions (
                    session_id, request_id, native_request_json, native_response_json,
                    resolved_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(session_id, request_id) DO UPDATE SET
                    native_response_json = excluded.native_response_json,
                    resolved_at_ms = excluded.resolved_at_ms",
                params![
                    session_id,
                    request_id,
                    native_request_json,
                    native_response_json,
                    db_i64(now_ms)?,
                ],
            )
            .map_err(storage_error)?;
        Ok(())
    }

    pub(crate) fn permission_resolution(
        &self,
        session_id: &str,
        request_id: &str,
        native_request_json: &str,
    ) -> Result<PermissionResolution, AgentHostError> {
        let connection = self.lock()?;
        let stored = connection
            .query_row(
                "SELECT native_request_json, native_response_json
                 FROM permissions WHERE session_id = ?1 AND request_id = ?2",
                params![session_id, request_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
            )
            .optional()
            .map_err(storage_error)?
            .ok_or(AgentHostError::UnknownPermission)?;
        let supplied: Value = serde_json::from_str(native_request_json)
            .map_err(|_| AgentHostError::InvalidNativePayload)?;
        let recorded: Value =
            serde_json::from_str(&stored.0).map_err(|_| AgentHostError::StorageUnavailable)?;
        if supplied != recorded {
            return Err(AgentHostError::InvalidNativePayload);
        }
        Ok(PermissionResolution {
            request_id: request_id.to_owned(),
            decision: PermissionDecision::AllowUnrestricted,
            native_response_json: stored.1.ok_or(AgentHostError::UnknownPermission)?,
        })
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, AgentHostError> {
        self.connection
            .lock()
            .map_err(|_| AgentHostError::StorageUnavailable)
    }
}

struct StoredPrompt {
    run_id: String,
    mode: String,
    native_prompt_json: String,
    disposition: String,
    accepted_at_ms: i64,
    interrupted_run_id: Option<String>,
    cancel_requested_at_ms: Option<i64>,
}

impl StoredPrompt {
    fn accepted(self, session_id: &str) -> Result<PromptAccepted, AgentHostError> {
        Ok(PromptAccepted {
            session_id: session_id.to_owned(),
            run_id: self.run_id,
            accepted_at_ms: db_u64(self.accepted_at_ms)?,
            disposition: parse_disposition(&self.disposition)?,
            interrupted_run_id: self.interrupted_run_id,
            cancel_requested_at_ms: self.cancel_requested_at_ms.map(db_u64).transpose()?,
        })
    }
}

fn query_prompt(
    transaction: &Transaction<'_>,
    session_id: &str,
    client_turn_id: &str,
) -> Result<Option<StoredPrompt>, AgentHostError> {
    transaction
        .query_row(
            "SELECT run_id, mode, native_prompt_json, disposition, accepted_at_ms,
                    interrupted_run_id, cancel_requested_at_ms
             FROM prompts WHERE session_id = ?1 AND client_turn_id = ?2",
            params![session_id, client_turn_id],
            |row| {
                Ok(StoredPrompt {
                    run_id: row.get(0)?,
                    mode: row.get(1)?,
                    native_prompt_json: row.get(2)?,
                    disposition: row.get(3)?,
                    accepted_at_ms: row.get(4)?,
                    interrupted_run_id: row.get(5)?,
                    cancel_requested_at_ms: row.get(6)?,
                })
            },
        )
        .optional()
        .map_err(storage_error)
}

fn migrate_v0_to_v1(connection: &mut Connection) -> Result<(), AgentHostError> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(storage_error)?;
    transaction
        .execute_batch(
            "CREATE TABLE sessions (
                session_id TEXT PRIMARY KEY,
                native_session_id TEXT NOT NULL,
                agent_id TEXT NOT NULL,
                working_directory TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                lifecycle TEXT NOT NULL,
                protocol_profile TEXT NOT NULL,
                protocol_version INTEGER NOT NULL,
                steering TEXT NOT NULL,
                compaction TEXT NOT NULL,
                capabilities_json TEXT NOT NULL,
                authority_verified_at_ms INTEGER NOT NULL,
                authority_evidence_json TEXT NOT NULL,
                active_run_id TEXT,
                last_sequence INTEGER NOT NULL,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL
             );
             CREATE INDEX sessions_agent_lifecycle ON sessions(agent_id, lifecycle);
             CREATE TABLE prompts (
                session_id TEXT NOT NULL,
                client_turn_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                mode TEXT NOT NULL,
                native_prompt_json TEXT NOT NULL,
                disposition TEXT NOT NULL,
                state TEXT NOT NULL,
                accepted_at_ms INTEGER NOT NULL,
                PRIMARY KEY(session_id, client_turn_id),
                FOREIGN KEY(session_id) REFERENCES sessions(session_id)
             );
             CREATE INDEX prompts_session_run ON prompts(session_id, run_id);
             CREATE TABLE events (
                session_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                run_id TEXT,
                observed_at_ms INTEGER NOT NULL,
                kind TEXT NOT NULL,
                direction TEXT NOT NULL,
                native_event_json TEXT NOT NULL,
                PRIMARY KEY(session_id, sequence),
                FOREIGN KEY(session_id) REFERENCES sessions(session_id)
             );
             CREATE TABLE permissions (
                session_id TEXT NOT NULL,
                request_id TEXT NOT NULL,
                native_request_json TEXT NOT NULL,
                native_response_json TEXT,
                resolved_at_ms INTEGER,
                PRIMARY KEY(session_id, request_id),
                FOREIGN KEY(session_id) REFERENCES sessions(session_id)
             );
             PRAGMA user_version = 1;",
        )
        .map_err(storage_error)?;
    transaction.commit().map_err(storage_error)
}

fn migrate_v1_to_v2(connection: &mut Connection) -> Result<(), AgentHostError> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(storage_error)?;
    transaction
        .execute_batch(
            "ALTER TABLE prompts ADD COLUMN interrupted_run_id TEXT;
             ALTER TABLE prompts ADD COLUMN cancel_requested_at_ms INTEGER;
             PRAGMA user_version = 2;",
        )
        .map_err(storage_error)?;
    transaction.commit().map_err(storage_error)
}

fn authority(verified_at_ms: u64, evidence_json: String) -> AuthorityAttestation {
    AuthorityAttestation {
        sandbox: SandboxAuthority::DisabledAndVerified,
        permissions: PermissionAuthority::YoloAndVerified,
        filesystem: FilesystemAuthority::UnrestrictedAndVerified,
        network: NetworkAuthority::UnrestrictedAndVerified,
        root: RootAuthority::PasswordlessSudoAndVerified,
        verified_at_ms,
        evidence_json,
    }
}

fn ensure_run_finished_event(
    transaction: &Transaction<'_>,
    session_id: &str,
    run_id: &str,
    outcome: &str,
    now_ms: u64,
) -> Result<(), AgentHostError> {
    let (last_sequence, has_terminal_event) = transaction
        .query_row(
            "SELECT last_sequence, EXISTS(
                SELECT 1 FROM events
                WHERE session_id = ?1 AND run_id = ?2 AND kind = 'RunFinished'
             )
             FROM sessions WHERE session_id = ?1",
            params![session_id, run_id],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, bool>(1)?)),
        )
        .optional()
        .map_err(storage_error)?
        .ok_or(AgentHostError::UnknownSession)?;
    if has_terminal_event {
        return Ok(());
    }
    let sequence = last_sequence
        .checked_add(1)
        .ok_or(AgentHostError::StorageUnavailable)?;
    let terminal_event = serde_json::to_string(&serde_json::json!({
        "jsonrpc": "2.0",
        "method": "crab/run_finished",
        "params": {
            "sessionId": session_id,
            "runId": run_id,
            "outcome": outcome,
        },
    }))
    .map_err(|_| AgentHostError::StorageUnavailable)?;
    transaction
        .execute(
            "INSERT INTO events (
                session_id, sequence, run_id, observed_at_ms, kind, direction, native_event_json
             ) VALUES (?1, ?2, ?3, ?4, 'RunFinished', 'AgentToClient', ?5)",
            params![
                session_id,
                sequence,
                run_id,
                db_i64(now_ms)?,
                terminal_event,
            ],
        )
        .map_err(storage_error)?;
    transaction
        .execute(
            "UPDATE sessions SET last_sequence = ?2, updated_at_ms = ?3 WHERE session_id = ?1",
            params![session_id, sequence, db_i64(now_ms)?],
        )
        .map_err(storage_error)?;
    Ok(())
}

fn classify_event(message: Option<&Value>) -> AcpEventKind {
    let Some(message) = message else {
        return AcpEventKind::Other;
    };
    if message.pointer("/result/stopReason").is_some() {
        return AcpEventKind::RunFinished;
    }
    let Some(method) = message.get("method").and_then(Value::as_str) else {
        return AcpEventKind::Other;
    };
    match method {
        "session/request_permission" => AcpEventKind::PermissionRequest,
        "session/prompt" => AcpEventKind::Message,
        "session/cancel" | "session/close" => AcpEventKind::SessionState,
        method if method.starts_with("terminal/") => AcpEventKind::Terminal,
        "fs/write_text_file" => AcpEventKind::FileDiff,
        "session/update" => {
            let update = message
                .pointer("/params/update/sessionUpdate")
                .and_then(Value::as_str)
                .unwrap_or_default();
            match update {
                "agent_message_chunk" | "agent_message" | "user_message_chunk" | "user_message" => {
                    AcpEventKind::Message
                }
                "agent_thought_chunk" | "agent_thought" => AcpEventKind::Thought,
                "plan" | "plan_update" | "plan_removed" => AcpEventKind::Plan,
                "tool_call" | "tool_call_content_chunk" => AcpEventKind::ToolCall,
                "tool_call_update" => {
                    let status = message
                        .pointer("/params/update/status")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    if matches!(status, "completed" | "failed") {
                        AcpEventKind::ToolResult
                    } else {
                        AcpEventKind::ToolCall
                    }
                }
                "terminal_update" | "terminal_output_chunk" => AcpEventKind::Terminal,
                "usage_update" => AcpEventKind::Usage,
                "compaction_update" | "compaction_summary_chunk" => AcpEventKind::Compaction,
                "state_update"
                    if message
                        .pointer("/params/update/state")
                        .and_then(Value::as_str)
                        == Some("idle") =>
                {
                    AcpEventKind::RunFinished
                }
                "state_update"
                | "current_mode_update"
                | "config_option_update"
                | "session_info_update" => AcpEventKind::SessionState,
                _ => AcpEventKind::Other,
            }
        }
        _ => AcpEventKind::Other,
    }
}

fn permission_request(message: Option<&Value>, raw: &str) -> Option<(String, String)> {
    let message = message?;
    if message.get("method")?.as_str()? != "session/request_permission" {
        return None;
    }
    let id = message.get("id")?;
    let request_id = id
        .as_str()
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| id.to_string());
    Some((request_id, raw.to_owned()))
}

fn protocol_profile_tag(value: &AcpProtocolProfile) -> Result<&'static str, AgentHostError> {
    match value {
        AcpProtocolProfile::V1Stable => Ok("V1Stable"),
        AcpProtocolProfile::V2Draft => Ok("V2Draft"),
        AcpProtocolProfile::Unknown { .. } => Err(AgentHostError::StorageUnavailable),
    }
}

fn parse_protocol_profile(value: &str) -> Result<AcpProtocolProfile, AgentHostError> {
    match value {
        "V1Stable" => Ok(AcpProtocolProfile::V1Stable),
        "V2Draft" => Ok(AcpProtocolProfile::V2Draft),
        _ => Err(AgentHostError::StorageUnavailable),
    }
}

fn steering_tag(value: &SteeringSupport) -> Result<&'static str, AgentHostError> {
    match value {
        SteeringSupport::TurnBoundaryQueue => Ok("TurnBoundaryQueue"),
        SteeringSupport::AcpV2ConcurrentPrompt => Ok("AcpV2ConcurrentPrompt"),
        SteeringSupport::AgentExtension => Ok("AgentExtension"),
        SteeringSupport::Unknown { .. } => Err(AgentHostError::StorageUnavailable),
    }
}

fn parse_steering(value: &str) -> Result<SteeringSupport, AgentHostError> {
    match value {
        "TurnBoundaryQueue" => Ok(SteeringSupport::TurnBoundaryQueue),
        "AcpV2ConcurrentPrompt" => Ok(SteeringSupport::AcpV2ConcurrentPrompt),
        "AgentExtension" => Ok(SteeringSupport::AgentExtension),
        _ => Err(AgentHostError::StorageUnavailable),
    }
}

fn compaction_tag(value: &CompactionReporting) -> Result<&'static str, AgentHostError> {
    match value {
        CompactionReporting::OpaqueAgentManaged => Ok("OpaqueAgentManaged"),
        CompactionReporting::DraftLifecycleUpdates => Ok("DraftLifecycleUpdates"),
        CompactionReporting::Unknown { .. } => Err(AgentHostError::StorageUnavailable),
    }
}

fn parse_compaction(value: &str) -> Result<CompactionReporting, AgentHostError> {
    match value {
        "OpaqueAgentManaged" => Ok(CompactionReporting::OpaqueAgentManaged),
        "DraftLifecycleUpdates" => Ok(CompactionReporting::DraftLifecycleUpdates),
        _ => Err(AgentHostError::StorageUnavailable),
    }
}

fn lifecycle_tag(value: &AgentLifecycle) -> Result<&'static str, AgentHostError> {
    match value {
        AgentLifecycle::Discovered => Ok("Discovered"),
        AgentLifecycle::Starting => Ok("Starting"),
        AgentLifecycle::Ready => Ok("Ready"),
        AgentLifecycle::Busy => Ok("Busy"),
        AgentLifecycle::Detaching => Ok("Detaching"),
        AgentLifecycle::Detached => Ok("Detached"),
        AgentLifecycle::Stopping => Ok("Stopping"),
        AgentLifecycle::Stopped => Ok("Stopped"),
        AgentLifecycle::Failed => Ok("Failed"),
        AgentLifecycle::Unknown { .. } => Err(AgentHostError::StorageUnavailable),
    }
}

fn parse_lifecycle(value: &str) -> Result<AgentLifecycle, AgentHostError> {
    match value {
        "Discovered" => Ok(AgentLifecycle::Discovered),
        "Starting" => Ok(AgentLifecycle::Starting),
        "Ready" => Ok(AgentLifecycle::Ready),
        "Busy" => Ok(AgentLifecycle::Busy),
        "Detaching" => Ok(AgentLifecycle::Detaching),
        "Detached" => Ok(AgentLifecycle::Detached),
        "Stopping" => Ok(AgentLifecycle::Stopping),
        "Stopped" => Ok(AgentLifecycle::Stopped),
        "Failed" => Ok(AgentLifecycle::Failed),
        _ => Err(AgentHostError::StorageUnavailable),
    }
}

fn input_mode_tag(value: &AgentInputMode) -> Result<&'static str, AgentHostError> {
    match value {
        AgentInputMode::Queue => Ok("Queue"),
        AgentInputMode::Steer => Ok("Steer"),
        AgentInputMode::InterruptAndQueue => Ok("InterruptAndQueue"),
        AgentInputMode::Unknown { .. } => Err(AgentHostError::InvalidNativePayload),
    }
}

fn disposition_tag(value: &PromptDisposition) -> Result<&'static str, AgentHostError> {
    match value {
        PromptDisposition::StartedForegroundWork => Ok("StartedForegroundWork"),
        PromptDisposition::ContributedToActiveWork => Ok("ContributedToActiveWork"),
        PromptDisposition::QueuedForTurnBoundary => Ok("QueuedForTurnBoundary"),
        PromptDisposition::CancelRequestedThenQueued => Ok("CancelRequestedThenQueued"),
        PromptDisposition::Unknown { .. } => Err(AgentHostError::StorageUnavailable),
    }
}

fn parse_disposition(value: &str) -> Result<PromptDisposition, AgentHostError> {
    match value {
        "StartedForegroundWork" => Ok(PromptDisposition::StartedForegroundWork),
        "ContributedToActiveWork" => Ok(PromptDisposition::ContributedToActiveWork),
        "QueuedForTurnBoundary" => Ok(PromptDisposition::QueuedForTurnBoundary),
        "CancelRequestedThenQueued" => Ok(PromptDisposition::CancelRequestedThenQueued),
        _ => Err(AgentHostError::StorageUnavailable),
    }
}

fn event_kind_tag(value: &AcpEventKind) -> Result<&'static str, AgentHostError> {
    match value {
        AcpEventKind::Message => Ok("Message"),
        AcpEventKind::Thought => Ok("Thought"),
        AcpEventKind::Plan => Ok("Plan"),
        AcpEventKind::ToolCall => Ok("ToolCall"),
        AcpEventKind::ToolResult => Ok("ToolResult"),
        AcpEventKind::Terminal => Ok("Terminal"),
        AcpEventKind::FileDiff => Ok("FileDiff"),
        AcpEventKind::PermissionRequest => Ok("PermissionRequest"),
        AcpEventKind::Usage => Ok("Usage"),
        AcpEventKind::Compaction => Ok("Compaction"),
        AcpEventKind::SessionState => Ok("SessionState"),
        AcpEventKind::RunFinished => Ok("RunFinished"),
        AcpEventKind::Other => Ok("Other"),
        AcpEventKind::Unknown { .. } => Err(AgentHostError::StorageUnavailable),
    }
}

fn parse_event_kind(value: &str) -> Result<AcpEventKind, AgentHostError> {
    match value {
        "Message" => Ok(AcpEventKind::Message),
        "Thought" => Ok(AcpEventKind::Thought),
        "Plan" => Ok(AcpEventKind::Plan),
        "ToolCall" => Ok(AcpEventKind::ToolCall),
        "ToolResult" => Ok(AcpEventKind::ToolResult),
        "Terminal" => Ok(AcpEventKind::Terminal),
        "FileDiff" => Ok(AcpEventKind::FileDiff),
        "PermissionRequest" => Ok(AcpEventKind::PermissionRequest),
        "Usage" => Ok(AcpEventKind::Usage),
        "Compaction" => Ok(AcpEventKind::Compaction),
        "SessionState" => Ok(AcpEventKind::SessionState),
        "RunFinished" => Ok(AcpEventKind::RunFinished),
        "Other" => Ok(AcpEventKind::Other),
        _ => Err(AgentHostError::StorageUnavailable),
    }
}

fn direction_tag(value: &AcpEventDirection) -> Result<&'static str, AgentHostError> {
    match value {
        AcpEventDirection::ClientToAgent => Ok("ClientToAgent"),
        AcpEventDirection::AgentToClient => Ok("AgentToClient"),
        AcpEventDirection::Unknown { .. } => Err(AgentHostError::StorageUnavailable),
    }
}

fn parse_direction(value: &str) -> Result<AcpEventDirection, AgentHostError> {
    match value {
        "ClientToAgent" => Ok(AcpEventDirection::ClientToAgent),
        "AgentToClient" => Ok(AcpEventDirection::AgentToClient),
        _ => Err(AgentHostError::StorageUnavailable),
    }
}

fn db_i64(value: u64) -> Result<i64, AgentHostError> {
    i64::try_from(value).map_err(|_| AgentHostError::StorageUnavailable)
}

fn db_u64(value: i64) -> Result<u64, AgentHostError> {
    u64::try_from(value).map_err(|_| AgentHostError::StorageUnavailable)
}

fn storage_error(_: rusqlite::Error) -> AgentHostError {
    AgentHostError::StorageUnavailable
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use super::{AgentStore, SCHEMA_VERSION, migrate_v0_to_v1};

    #[test]
    fn schema_one_migrates_additive_interruption_receipt_columns() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("agent-host-v1.sqlite");
        let mut connection = Connection::open(&path).expect("database opens");
        migrate_v0_to_v1(&mut connection).expect("schema one is created");
        drop(connection);

        let store = AgentStore::open(&path).expect("schema one migrates");
        let connection = store.lock().expect("migrated database locks");
        let version = connection
            .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
            .expect("schema version reads");
        assert_eq!(version, SCHEMA_VERSION);
        let columns = connection
            .prepare("PRAGMA table_info(prompts)")
            .expect("prompt columns prepare")
            .query_map([], |row| row.get::<_, String>(1))
            .expect("prompt columns query")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("prompt columns collect");
        assert!(columns.contains(&"interrupted_run_id".to_owned()));
        assert!(columns.contains(&"cancel_requested_at_ms".to_owned()));
    }
}
