use std::{path::Path, sync::Mutex};

use boxology_import_agent_host::{AcpEvent, AcpEventDirection, AcpEventKind};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    ContextRealization, InputDisposition, InteractionReceipt, ReadSubAgentEventsRequest,
    SpawnSubAgentRequest, SubAgentContextMode, SubAgentEvent, SubAgentEventKind, SubAgentEventPage,
    SubAgentHostError, SubAgentInputMode, SubAgentLifecycle, SubAgentRecord, SubAgentStatus,
};

const SCHEMA_VERSION: i64 = 1;
const MAX_EVENT_PAGE: u64 = 1_000;
const RECOVERY_PENDING_ERROR: &str = "runtime restarted; native child recovery pending";

#[derive(Clone, Copy)]
pub(crate) enum InteractionDirection {
    ParentToChild,
    ChildToParent,
}

pub(crate) struct SpawnStart {
    pub(crate) record: SubAgentRecord,
    pub(crate) inserted: bool,
}

pub(crate) struct InteractionStart {
    pub(crate) completed: Option<InteractionReceipt>,
    pub(crate) should_dispatch: bool,
}

pub(crate) struct RecoveryCandidate {
    pub(crate) record: SubAgentRecord,
    pub(crate) crash_restart_limit: u64,
    pub(crate) restart_count: u64,
}

pub(crate) struct SubAgentStore {
    connection: Mutex<Connection>,
}

impl SubAgentStore {
    pub(crate) fn open(path: impl AsRef<Path>) -> Result<Self, SubAgentHostError> {
        Self::initialize(Connection::open(path).map_err(storage_error)?)
    }

    pub(crate) fn open_in_memory() -> Result<Self, SubAgentHostError> {
        Self::initialize(Connection::open_in_memory().map_err(storage_error)?)
    }

    fn initialize(mut connection: Connection) -> Result<Self, SubAgentHostError> {
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
            0 => migrate_v0_to_v1(&mut connection)?,
            SCHEMA_VERSION => {}
            _ => return Err(SubAgentHostError::StorageUnavailable),
        }
        let transaction = connection.transaction().map_err(storage_error)?;
        transaction
            .execute(
                "UPDATE sub_agents SET lifecycle = 'Failed',
                    last_error = ?1
                 WHERE lifecycle IN ('Starting', 'Running', 'Idle')",
                params![RECOVERY_PENDING_ERROR],
            )
            .map_err(storage_error)?;
        transaction
            .execute(
                "UPDATE sub_agents SET lifecycle = 'Failed',
                    last_error = 'runtime restarted while child shutdown was in progress'
                 WHERE lifecycle = 'Stopping'",
                [],
            )
            .map_err(storage_error)?;
        transaction
            .execute(
                "UPDATE interactions SET state = 'Failed' WHERE state = 'Pending'",
                [],
            )
            .map_err(storage_error)?;
        transaction.commit().map_err(storage_error)?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    pub(crate) fn begin_spawn(
        &self,
        request: &SpawnSubAgentRequest,
        realization: &ContextRealization,
        request_fingerprint: &str,
        now_ms: u64,
    ) -> Result<SpawnStart, SubAgentHostError> {
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        if let Some(existing) = transaction
            .query_row(
                "SELECT request_fingerprint FROM sub_agents WHERE client_sub_agent_id = ?1",
                params![request.client_sub_agent_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(storage_error)?
        {
            if existing != request_fingerprint {
                return Err(SubAgentHostError::DuplicateIdConflict);
            }
            let sub_agent_id = transaction
                .query_row(
                    "SELECT sub_agent_id FROM sub_agents WHERE client_sub_agent_id = ?1",
                    params![request.client_sub_agent_id],
                    |row| row.get::<_, String>(0),
                )
                .map_err(storage_error)?;
            transaction.commit().map_err(storage_error)?;
            drop(connection);
            return Ok(SpawnStart {
                record: self.record(&sub_agent_id)?,
                inserted: false,
            });
        }

        let sub_agent_id = format!("subagent_{}", Uuid::new_v4());
        transaction
            .execute(
                "INSERT INTO sub_agents (
                    sub_agent_id, client_sub_agent_id, request_fingerprint, parent_session_id,
                    child_session_id, native_child_session_id, agent_id, working_directory,
                    context_mode, context_realization, context_through_sequence,
                    process_identity, lifecycle, started_at_ms, last_sequence, child_cursor,
                    crash_restart_limit, restart_count, last_error
                 ) VALUES (?1, ?2, ?3, ?4, '', '', ?5, ?6, ?7, ?8, ?9, '', 'Starting',
                           ?10, 0, 0, ?11, 0, NULL)",
                params![
                    sub_agent_id,
                    request.client_sub_agent_id,
                    request_fingerprint,
                    request.parent_session_id,
                    request.agent_id,
                    request.working_directory,
                    context_mode_tag(&request.context_mode)?,
                    context_realization_tag(realization)?,
                    request
                        .parent_context_through_sequence
                        .map(db_i64)
                        .transpose()?,
                    db_i64(now_ms)?,
                    db_i64(request.crash_restart_limit)?,
                ],
            )
            .map_err(storage_error)?;
        append_event(
            &transaction,
            &sub_agent_id,
            &SubAgentEventKind::Lifecycle,
            &json!({ "lifecycle": "Starting" }).to_string(),
            now_ms,
        )?;
        transaction.commit().map_err(storage_error)?;
        drop(connection);
        Ok(SpawnStart {
            record: self.record(&sub_agent_id)?,
            inserted: true,
        })
    }

    pub(crate) fn existing_spawn(
        &self,
        client_sub_agent_id: &str,
        request_fingerprint: &str,
    ) -> Result<Option<SubAgentRecord>, SubAgentHostError> {
        let connection = self.lock()?;
        let existing = connection
            .query_row(
                "SELECT sub_agent_id, request_fingerprint FROM sub_agents
                 WHERE client_sub_agent_id = ?1",
                params![client_sub_agent_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(storage_error)?;
        let Some((sub_agent_id, existing_fingerprint)) = existing else {
            return Ok(None);
        };
        if existing_fingerprint != request_fingerprint {
            return Err(SubAgentHostError::DuplicateIdConflict);
        }
        query_record(&connection, &sub_agent_id)
    }

    pub(crate) fn complete_spawn(
        &self,
        sub_agent_id: &str,
        child_session_id: &str,
        native_child_session_id: &str,
        now_ms: u64,
    ) -> Result<SubAgentRecord, SubAgentHostError> {
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let process_identity = format!("agent-host-session:{child_session_id}");
        let changed = transaction
            .execute(
                "UPDATE sub_agents SET child_session_id = ?2, native_child_session_id = ?3,
                    process_identity = ?4, lifecycle = 'Running', last_error = NULL
                 WHERE sub_agent_id = ?1 AND lifecycle = 'Starting'",
                params![
                    sub_agent_id,
                    child_session_id,
                    native_child_session_id,
                    process_identity
                ],
            )
            .map_err(storage_error)?;
        if changed != 1 {
            return Err(SubAgentHostError::UnknownSubAgent);
        }
        append_event(
            &transaction,
            sub_agent_id,
            &SubAgentEventKind::Lifecycle,
            &json!({ "lifecycle": "Running" }).to_string(),
            now_ms,
        )?;
        transaction.commit().map_err(storage_error)?;
        drop(connection);
        self.record(sub_agent_id)
    }

    pub(crate) fn set_context_realization(
        &self,
        sub_agent_id: &str,
        realization: &ContextRealization,
        now_ms: u64,
    ) -> Result<(), SubAgentHostError> {
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let realization = context_realization_tag(realization)?;
        let changed = transaction
            .execute(
                "UPDATE sub_agents SET context_realization = ?2
                 WHERE sub_agent_id = ?1 AND lifecycle = 'Starting'",
                params![sub_agent_id, realization],
            )
            .map_err(storage_error)?;
        if changed != 1 {
            return Err(SubAgentHostError::UnknownSubAgent);
        }
        append_event(
            &transaction,
            sub_agent_id,
            &SubAgentEventKind::Lifecycle,
            &json!({ "contextRealization": realization }).to_string(),
            now_ms,
        )?;
        transaction.commit().map_err(storage_error)
    }

    pub(crate) fn fail_spawn(
        &self,
        sub_agent_id: &str,
        reason: &str,
        now_ms: u64,
    ) -> Result<(), SubAgentHostError> {
        self.set_lifecycle(
            sub_agent_id,
            &SubAgentLifecycle::Failed,
            Some(reason),
            now_ms,
        )
    }

    pub(crate) fn record(&self, sub_agent_id: &str) -> Result<SubAgentRecord, SubAgentHostError> {
        let connection = self.lock()?;
        query_record(&connection, sub_agent_id)?.ok_or(SubAgentHostError::UnknownSubAgent)
    }

    pub(crate) fn status(&self, sub_agent_id: &str) -> Result<SubAgentStatus, SubAgentHostError> {
        let connection = self.lock()?;
        let record =
            query_record(&connection, sub_agent_id)?.ok_or(SubAgentHostError::UnknownSubAgent)?;
        let (last_sequence, restart_count, last_error) = connection
            .query_row(
                "SELECT last_sequence, restart_count, last_error
                 FROM sub_agents WHERE sub_agent_id = ?1",
                params![sub_agent_id],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                },
            )
            .map_err(storage_error)?;
        let pending_parent_to_child = pending_count(
            &connection,
            sub_agent_id,
            &InteractionDirection::ParentToChild,
        )?;
        let pending_child_to_parent = pending_count(
            &connection,
            sub_agent_id,
            &InteractionDirection::ChildToParent,
        )?;
        Ok(SubAgentStatus {
            record,
            last_sequence: db_u64(last_sequence)?,
            pending_parent_to_child,
            pending_child_to_parent,
            restart_count: db_u64(restart_count)?,
            last_error,
        })
    }

    pub(crate) fn recovery_candidates(&self) -> Result<Vec<RecoveryCandidate>, SubAgentHostError> {
        let connection = self.lock()?;
        let mut statement = connection
            .prepare(
                "SELECT sub_agent_id, crash_restart_limit, restart_count
                 FROM sub_agents
                 WHERE lifecycle = 'Failed' AND last_error = ?1
                 ORDER BY started_at_ms, sub_agent_id",
            )
            .map_err(storage_error)?;
        let rows = statement
            .query_map(params![RECOVERY_PENDING_ERROR], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .map_err(storage_error)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(storage_error)?;
        rows.into_iter()
            .map(|(sub_agent_id, crash_restart_limit, restart_count)| {
                Ok(RecoveryCandidate {
                    record: query_record(&connection, &sub_agent_id)?
                        .ok_or(SubAgentHostError::UnknownSubAgent)?,
                    crash_restart_limit: db_u64(crash_restart_limit)?,
                    restart_count: db_u64(restart_count)?,
                })
            })
            .collect()
    }

    pub(crate) fn complete_recovery(
        &self,
        sub_agent_id: &str,
        expected_child_session_id: &str,
        expected_native_session_id: &str,
        lifecycle: &SubAgentLifecycle,
        now_ms: u64,
    ) -> Result<SubAgentRecord, SubAgentHostError> {
        if !matches!(
            lifecycle,
            SubAgentLifecycle::Running | SubAgentLifecycle::Idle
        ) {
            return Err(SubAgentHostError::StorageUnavailable);
        }
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let next = lifecycle_tag(lifecycle)?;
        let changed = transaction
            .execute(
                "UPDATE sub_agents
                 SET lifecycle = ?4, restart_count = restart_count + 1, last_error = NULL
                 WHERE sub_agent_id = ?1 AND child_session_id = ?2
                   AND native_child_session_id = ?3 AND lifecycle = 'Failed'
                   AND last_error = ?5 AND crash_restart_limit > restart_count",
                params![
                    sub_agent_id,
                    expected_child_session_id,
                    expected_native_session_id,
                    next,
                    RECOVERY_PENDING_ERROR,
                ],
            )
            .map_err(storage_error)?;
        if changed != 1 {
            return Err(SubAgentHostError::StorageUnavailable);
        }
        let restart_count = transaction
            .query_row(
                "SELECT restart_count FROM sub_agents WHERE sub_agent_id = ?1",
                params![sub_agent_id],
                |row| row.get::<_, i64>(0),
            )
            .map_err(storage_error)
            .and_then(db_u64)?;
        append_event(
            &transaction,
            sub_agent_id,
            &SubAgentEventKind::Lifecycle,
            &json!({
                "lifecycle": next,
                "recovery": "native_session_resume",
                "restartCount": restart_count,
            })
            .to_string(),
            now_ms,
        )?;
        transaction.commit().map_err(storage_error)?;
        drop(connection);
        self.record(sub_agent_id)
    }

    pub(crate) fn fail_recovery(
        &self,
        sub_agent_id: &str,
        reason: &str,
        attempted: bool,
        now_ms: u64,
    ) -> Result<(), SubAgentHostError> {
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let changed = transaction
            .execute(
                "UPDATE sub_agents
                 SET last_error = ?2, restart_count = restart_count + ?3
                 WHERE sub_agent_id = ?1 AND lifecycle = 'Failed' AND last_error = ?4",
                params![
                    sub_agent_id,
                    reason,
                    i64::from(attempted),
                    RECOVERY_PENDING_ERROR,
                ],
            )
            .map_err(storage_error)?;
        if changed != 1 {
            return Err(SubAgentHostError::StorageUnavailable);
        }
        append_event(
            &transaction,
            sub_agent_id,
            &SubAgentEventKind::Failed,
            &json!({ "lifecycle": "Failed", "error": reason }).to_string(),
            now_ms,
        )?;
        transaction.commit().map_err(storage_error)
    }

    pub(crate) fn begin_interaction(
        &self,
        sub_agent_id: &str,
        client_message_id: &str,
        direction: &InteractionDirection,
        mode: &SubAgentInputMode,
        payload_json: &str,
        now_ms: u64,
    ) -> Result<InteractionStart, SubAgentHostError> {
        let fingerprint = interaction_fingerprint(direction, mode, payload_json)?;
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        if !sub_agent_exists(&transaction, sub_agent_id)? {
            return Err(SubAgentHostError::UnknownSubAgent);
        }
        if let Some((existing, state, disposition, accepted_at)) = transaction
            .query_row(
                "SELECT fingerprint, state, disposition, accepted_at_ms FROM interactions
                 WHERE sub_agent_id = ?1 AND direction = ?2 AND client_message_id = ?3",
                params![sub_agent_id, direction_tag(direction), client_message_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(storage_error)?
        {
            if existing != fingerprint {
                return Err(SubAgentHostError::DuplicateIdConflict);
            }
            if state == "Delivered" {
                return Ok(InteractionStart {
                    completed: Some(InteractionReceipt {
                        sub_agent_id: sub_agent_id.to_owned(),
                        client_message_id: client_message_id.to_owned(),
                        disposition: parse_disposition(
                            disposition
                                .as_deref()
                                .ok_or(SubAgentHostError::StorageUnavailable)?,
                        )?,
                        accepted_at_ms: db_u64(accepted_at)?,
                    }),
                    should_dispatch: false,
                });
            }
            require_active_sub_agent(&transaction, sub_agent_id)?;
            transaction
                .execute(
                    "UPDATE interactions SET state = 'Pending' WHERE sub_agent_id = ?1
                     AND direction = ?2 AND client_message_id = ?3",
                    params![sub_agent_id, direction_tag(direction), client_message_id],
                )
                .map_err(storage_error)?;
            transaction.commit().map_err(storage_error)?;
            return Ok(InteractionStart {
                completed: None,
                should_dispatch: true,
            });
        }

        require_active_sub_agent(&transaction, sub_agent_id)?;
        transaction
            .execute(
                "INSERT INTO interactions (
                    sub_agent_id, direction, client_message_id, fingerprint, mode,
                    payload_json, state, disposition, accepted_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'Pending', NULL, ?7)",
                params![
                    sub_agent_id,
                    direction_tag(direction),
                    client_message_id,
                    fingerprint,
                    input_mode_tag(mode)?,
                    payload_json,
                    db_i64(now_ms)?,
                ],
            )
            .map_err(storage_error)?;
        let kind = match direction {
            InteractionDirection::ParentToChild => SubAgentEventKind::ParentToChild,
            InteractionDirection::ChildToParent => SubAgentEventKind::ChildToParent,
        };
        append_event(
            &transaction,
            sub_agent_id,
            &kind,
            &json!({
                "clientMessageId": client_message_id,
                "mode": input_mode_tag(mode)?,
                "message": parse_json(payload_json)?,
            })
            .to_string(),
            now_ms,
        )?;
        transaction.commit().map_err(storage_error)?;
        Ok(InteractionStart {
            completed: None,
            should_dispatch: true,
        })
    }

    pub(crate) fn complete_interaction(
        &self,
        sub_agent_id: &str,
        client_message_id: &str,
        direction: &InteractionDirection,
        disposition: &InputDisposition,
        accepted_at_ms: u64,
    ) -> Result<InteractionReceipt, SubAgentHostError> {
        let connection = self.lock()?;
        let changed = connection
            .execute(
                "UPDATE interactions SET state = 'Delivered', disposition = ?4,
                    accepted_at_ms = ?5 WHERE sub_agent_id = ?1 AND direction = ?2
                    AND client_message_id = ?3",
                params![
                    sub_agent_id,
                    direction_tag(direction),
                    client_message_id,
                    disposition_tag(disposition)?,
                    db_i64(accepted_at_ms)?,
                ],
            )
            .map_err(storage_error)?;
        if changed != 1 {
            return Err(SubAgentHostError::UnknownSubAgent);
        }
        Ok(InteractionReceipt {
            sub_agent_id: sub_agent_id.to_owned(),
            client_message_id: client_message_id.to_owned(),
            disposition: disposition.clone(),
            accepted_at_ms,
        })
    }

    pub(crate) fn record_native_event(
        &self,
        sub_agent_id: &str,
        event: &AcpEvent,
    ) -> Result<(), SubAgentHostError> {
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let cursor = transaction
            .query_row(
                "SELECT child_cursor FROM sub_agents WHERE sub_agent_id = ?1",
                params![sub_agent_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(storage_error)?
            .ok_or(SubAgentHostError::UnknownSubAgent)
            .and_then(db_u64)?;
        if event.sequence <= cursor {
            return Ok(());
        }
        if event.sequence != cursor.saturating_add(1) {
            return Err(SubAgentHostError::StorageUnavailable);
        }
        let payload = json!({
            "childSequence": event.sequence,
            "runId": event.run_id,
            "direction": acp_direction_tag(&event.direction)?,
            "kind": acp_kind_tag(&event.kind)?,
            "nativeEvent": parse_json(&event.native_event_json)?,
        })
        .to_string();
        let kind = if matches!(event.kind, AcpEventKind::Compaction) {
            SubAgentEventKind::Compaction
        } else {
            SubAgentEventKind::NativeAcp
        };
        let local_sequence = append_event(
            &transaction,
            sub_agent_id,
            &kind,
            &payload,
            event.observed_at_ms,
        )?;
        transaction
            .execute(
                "INSERT INTO native_events (sub_agent_id, child_sequence, local_sequence)
                 VALUES (?1, ?2, ?3)",
                params![
                    sub_agent_id,
                    db_i64(event.sequence)?,
                    db_i64(local_sequence)?
                ],
            )
            .map_err(storage_error)?;
        transaction
            .execute(
                "UPDATE sub_agents SET child_cursor = ?2 WHERE sub_agent_id = ?1",
                params![sub_agent_id, db_i64(event.sequence)?],
            )
            .map_err(storage_error)?;
        transaction.commit().map_err(storage_error)
    }

    pub(crate) fn child_cursor(&self, sub_agent_id: &str) -> Result<u64, SubAgentHostError> {
        let connection = self.lock()?;
        connection
            .query_row(
                "SELECT child_cursor FROM sub_agents WHERE sub_agent_id = ?1",
                params![sub_agent_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(storage_error)?
            .ok_or(SubAgentHostError::UnknownSubAgent)
            .and_then(db_u64)
    }

    pub(crate) fn set_lifecycle(
        &self,
        sub_agent_id: &str,
        lifecycle: &SubAgentLifecycle,
        last_error: Option<&str>,
        now_ms: u64,
    ) -> Result<(), SubAgentHostError> {
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let current = transaction
            .query_row(
                "SELECT lifecycle, last_error FROM sub_agents WHERE sub_agent_id = ?1",
                params![sub_agent_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
            )
            .optional()
            .map_err(storage_error)?
            .ok_or(SubAgentHostError::UnknownSubAgent)?;
        let next = lifecycle_tag(lifecycle)?;
        if current.0 == next && current.1.as_deref() == last_error {
            return Ok(());
        }
        transaction
            .execute(
                "UPDATE sub_agents SET lifecycle = ?2, last_error = ?3 WHERE sub_agent_id = ?1",
                params![sub_agent_id, next, last_error],
            )
            .map_err(storage_error)?;
        if matches!(
            lifecycle,
            SubAgentLifecycle::Completed | SubAgentLifecycle::Failed
        ) {
            transaction
                .execute(
                    "UPDATE interactions SET state = 'Failed'
                     WHERE sub_agent_id = ?1 AND state = 'Pending'",
                    params![sub_agent_id],
                )
                .map_err(storage_error)?;
        }
        append_event(
            &transaction,
            sub_agent_id,
            if matches!(lifecycle, SubAgentLifecycle::Failed) {
                &SubAgentEventKind::Failed
            } else {
                &SubAgentEventKind::Lifecycle
            },
            &json!({ "lifecycle": next, "error": last_error }).to_string(),
            now_ms,
        )?;
        transaction.commit().map_err(storage_error)
    }

    pub(crate) fn read_events(
        &self,
        request: &ReadSubAgentEventsRequest,
    ) -> Result<SubAgentEventPage, SubAgentHostError> {
        if request.limit == 0 || request.limit > MAX_EVENT_PAGE {
            return Err(SubAgentHostError::InvalidContextBoundary);
        }
        let connection = self.lock()?;
        let last_sequence = connection
            .query_row(
                "SELECT last_sequence FROM sub_agents WHERE sub_agent_id = ?1",
                params![request.sub_agent_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(storage_error)?
            .ok_or(SubAgentHostError::UnknownSubAgent)
            .and_then(db_u64)?;
        if request.after_sequence > last_sequence {
            return Err(SubAgentHostError::InvalidContextBoundary);
        }
        let mut statement = connection
            .prepare(
                "SELECT sequence, observed_at_ms, kind, payload_json FROM events
                 WHERE sub_agent_id = ?1 AND sequence > ?2 ORDER BY sequence LIMIT ?3",
            )
            .map_err(storage_error)?;
        let events = statement
            .query_map(
                params![
                    request.sub_agent_id,
                    db_i64(request.after_sequence)?,
                    db_i64(request.limit)?
                ],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .map_err(storage_error)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(storage_error)?
            .into_iter()
            .map(|(sequence, observed, kind, payload)| {
                Ok(SubAgentEvent {
                    sub_agent_id: request.sub_agent_id.clone(),
                    sequence: db_u64(sequence)?,
                    observed_at_ms: db_u64(observed)?,
                    kind: parse_event_kind(&kind)?,
                    payload_json: payload,
                })
            })
            .collect::<Result<Vec<_>, SubAgentHostError>>()?;
        let next_sequence = events
            .last()
            .map_or(request.after_sequence, |event| event.sequence);
        Ok(SubAgentEventPage {
            events,
            next_sequence,
            caught_up: next_sequence >= last_sequence,
        })
    }

    pub(crate) fn stop(&self, sub_agent_id: &str, now_ms: u64) -> Result<(), SubAgentHostError> {
        self.set_lifecycle(sub_agent_id, &SubAgentLifecycle::Completed, None, now_ms)
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, SubAgentHostError> {
        self.connection
            .lock()
            .map_err(|_| SubAgentHostError::StorageUnavailable)
    }
}

fn migrate_v0_to_v1(connection: &mut Connection) -> Result<(), SubAgentHostError> {
    let transaction = connection.transaction().map_err(storage_error)?;
    transaction
        .execute_batch(
            "CREATE TABLE sub_agents (
                sub_agent_id TEXT PRIMARY KEY,
                client_sub_agent_id TEXT NOT NULL UNIQUE,
                request_fingerprint TEXT NOT NULL,
                parent_session_id TEXT NOT NULL,
                child_session_id TEXT NOT NULL,
                native_child_session_id TEXT NOT NULL,
                agent_id TEXT NOT NULL,
                working_directory TEXT NOT NULL,
                context_mode TEXT NOT NULL,
                context_realization TEXT NOT NULL,
                context_through_sequence INTEGER,
                process_identity TEXT NOT NULL,
                lifecycle TEXT NOT NULL,
                started_at_ms INTEGER NOT NULL,
                last_sequence INTEGER NOT NULL,
                child_cursor INTEGER NOT NULL,
                crash_restart_limit INTEGER NOT NULL,
                restart_count INTEGER NOT NULL,
                last_error TEXT
             );
             CREATE TABLE interactions (
                sub_agent_id TEXT NOT NULL REFERENCES sub_agents(sub_agent_id),
                direction TEXT NOT NULL,
                client_message_id TEXT NOT NULL,
                fingerprint TEXT NOT NULL,
                mode TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                state TEXT NOT NULL,
                disposition TEXT,
                accepted_at_ms INTEGER NOT NULL,
                PRIMARY KEY(sub_agent_id, direction, client_message_id)
             );
             CREATE TABLE events (
                sub_agent_id TEXT NOT NULL REFERENCES sub_agents(sub_agent_id),
                sequence INTEGER NOT NULL,
                observed_at_ms INTEGER NOT NULL,
                kind TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY(sub_agent_id, sequence)
             );
             CREATE TABLE native_events (
                sub_agent_id TEXT NOT NULL REFERENCES sub_agents(sub_agent_id),
                child_sequence INTEGER NOT NULL,
                local_sequence INTEGER NOT NULL,
                PRIMARY KEY(sub_agent_id, child_sequence),
                UNIQUE(sub_agent_id, local_sequence)
             );
             PRAGMA user_version = 1;",
        )
        .map_err(storage_error)?;
    transaction.commit().map_err(storage_error)
}

fn query_record(
    connection: &Connection,
    sub_agent_id: &str,
) -> Result<Option<SubAgentRecord>, SubAgentHostError> {
    connection
        .query_row(
            "SELECT parent_session_id, child_session_id, native_child_session_id, agent_id,
                    lifecycle, context_mode, context_realization, context_through_sequence,
                    process_identity, started_at_ms FROM sub_agents WHERE sub_agent_id = ?1",
            params![sub_agent_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, Option<i64>>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, i64>(9)?,
                ))
            },
        )
        .optional()
        .map_err(storage_error)?
        .map(
            |(
                parent,
                child,
                native,
                agent,
                lifecycle,
                context_mode,
                realization,
                boundary,
                process,
                started,
            )| {
                Ok(SubAgentRecord {
                    sub_agent_id: sub_agent_id.to_owned(),
                    parent_session_id: parent,
                    child_session_id: child,
                    native_child_session_id: native,
                    agent_id: agent,
                    lifecycle: parse_lifecycle(&lifecycle)?,
                    context_mode: parse_context_mode(&context_mode)?,
                    context_realization: parse_context_realization(&realization)?,
                    context_through_sequence: boundary.map(db_u64).transpose()?,
                    process_identity: process,
                    started_at_ms: db_u64(started)?,
                })
            },
        )
        .transpose()
}

fn append_event(
    transaction: &Transaction<'_>,
    sub_agent_id: &str,
    kind: &SubAgentEventKind,
    payload_json: &str,
    observed_at_ms: u64,
) -> Result<u64, SubAgentHostError> {
    parse_json(payload_json)?;
    let last = transaction
        .query_row(
            "SELECT last_sequence FROM sub_agents WHERE sub_agent_id = ?1",
            params![sub_agent_id],
            |row| row.get::<_, i64>(0),
        )
        .map_err(storage_error)
        .and_then(db_u64)?;
    let next = last
        .checked_add(1)
        .ok_or(SubAgentHostError::StorageUnavailable)?;
    transaction
        .execute(
            "INSERT INTO events (sub_agent_id, sequence, observed_at_ms, kind, payload_json)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                sub_agent_id,
                db_i64(next)?,
                db_i64(observed_at_ms)?,
                event_kind_tag(kind)?,
                payload_json
            ],
        )
        .map_err(storage_error)?;
    transaction
        .execute(
            "UPDATE sub_agents SET last_sequence = ?2 WHERE sub_agent_id = ?1",
            params![sub_agent_id, db_i64(next)?],
        )
        .map_err(storage_error)?;
    Ok(next)
}

fn pending_count(
    connection: &Connection,
    sub_agent_id: &str,
    direction: &InteractionDirection,
) -> Result<u64, SubAgentHostError> {
    connection
        .query_row(
            "SELECT COUNT(*) FROM interactions
             WHERE sub_agent_id = ?1 AND direction = ?2 AND state = 'Pending'",
            params![sub_agent_id, direction_tag(direction)],
            |row| row.get::<_, i64>(0),
        )
        .map_err(storage_error)
        .and_then(db_u64)
}

fn sub_agent_exists(
    connection: &Connection,
    sub_agent_id: &str,
) -> Result<bool, SubAgentHostError> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sub_agents WHERE sub_agent_id = ?1)",
            params![sub_agent_id],
            |row| row.get(0),
        )
        .map_err(storage_error)
}

fn require_active_sub_agent(
    connection: &Connection,
    sub_agent_id: &str,
) -> Result<(), SubAgentHostError> {
    let active = connection
        .query_row(
            "SELECT lifecycle IN ('Running', 'Idle') AND child_session_id != ''
             FROM sub_agents WHERE sub_agent_id = ?1",
            params![sub_agent_id],
            |row| row.get::<_, bool>(0),
        )
        .optional()
        .map_err(storage_error)?
        .ok_or(SubAgentHostError::UnknownSubAgent)?;
    if active {
        Ok(())
    } else {
        Err(SubAgentHostError::UnknownSubAgent)
    }
}

fn interaction_fingerprint(
    direction: &InteractionDirection,
    mode: &SubAgentInputMode,
    payload_json: &str,
) -> Result<String, SubAgentHostError> {
    Ok(json!({
        "direction": direction_tag(direction),
        "mode": input_mode_tag(mode)?,
        "payload": parse_json(payload_json)?,
    })
    .to_string())
}

pub(crate) fn spawn_fingerprint(
    request: &SpawnSubAgentRequest,
) -> Result<String, SubAgentHostError> {
    Ok(json!({
        "clientSubAgentId": request.client_sub_agent_id,
        "parentSessionId": request.parent_session_id,
        "agentId": request.agent_id,
        "workingDirectory": request.working_directory,
        "contextMode": context_mode_tag(&request.context_mode)?,
        "parentContextThroughSequence": request.parent_context_through_sequence,
        "allowPortableSnapshot": request.allow_portable_snapshot,
        "nativeTaskPrompt": parse_json(&request.native_task_prompt_json)?,
        "metadata": parse_json(&request.metadata_json)?,
        "crashRestartLimit": request.crash_restart_limit,
    })
    .to_string())
}

fn direction_tag(direction: &InteractionDirection) -> &'static str {
    match direction {
        InteractionDirection::ParentToChild => "ParentToChild",
        InteractionDirection::ChildToParent => "ChildToParent",
    }
}

fn context_mode_tag(mode: &SubAgentContextMode) -> Result<&'static str, SubAgentHostError> {
    match mode {
        SubAgentContextMode::Fresh => Ok("Fresh"),
        SubAgentContextMode::InheritParent => Ok("InheritParent"),
        SubAgentContextMode::Unknown { .. } => Err(SubAgentHostError::InvalidNativePayload),
    }
}

fn parse_context_mode(value: &str) -> Result<SubAgentContextMode, SubAgentHostError> {
    match value {
        "Fresh" => Ok(SubAgentContextMode::Fresh),
        "InheritParent" => Ok(SubAgentContextMode::InheritParent),
        _ => Err(SubAgentHostError::StorageUnavailable),
    }
}

fn context_realization_tag(
    realization: &ContextRealization,
) -> Result<&'static str, SubAgentHostError> {
    match realization {
        ContextRealization::FreshSession => Ok("FreshSession"),
        ContextRealization::NativeAcpFork => Ok("NativeAcpFork"),
        ContextRealization::PortableSnapshot => Ok("PortableSnapshot"),
        ContextRealization::Unknown { .. } => Err(SubAgentHostError::InvalidNativePayload),
    }
}

fn parse_context_realization(value: &str) -> Result<ContextRealization, SubAgentHostError> {
    match value {
        "FreshSession" => Ok(ContextRealization::FreshSession),
        "NativeAcpFork" => Ok(ContextRealization::NativeAcpFork),
        "PortableSnapshot" => Ok(ContextRealization::PortableSnapshot),
        _ => Err(SubAgentHostError::StorageUnavailable),
    }
}

fn input_mode_tag(mode: &SubAgentInputMode) -> Result<&'static str, SubAgentHostError> {
    match mode {
        SubAgentInputMode::Queue => Ok("Queue"),
        SubAgentInputMode::Steer => Ok("Steer"),
        SubAgentInputMode::InterruptAndSteer => Ok("InterruptAndSteer"),
        SubAgentInputMode::Unknown { .. } => Err(SubAgentHostError::InvalidNativePayload),
    }
}

fn lifecycle_tag(lifecycle: &SubAgentLifecycle) -> Result<&'static str, SubAgentHostError> {
    match lifecycle {
        SubAgentLifecycle::Starting => Ok("Starting"),
        SubAgentLifecycle::Running => Ok("Running"),
        SubAgentLifecycle::Idle => Ok("Idle"),
        SubAgentLifecycle::Stopping => Ok("Stopping"),
        SubAgentLifecycle::Completed => Ok("Completed"),
        SubAgentLifecycle::Failed => Ok("Failed"),
        SubAgentLifecycle::Unknown { .. } => Err(SubAgentHostError::StorageUnavailable),
    }
}

fn parse_lifecycle(value: &str) -> Result<SubAgentLifecycle, SubAgentHostError> {
    match value {
        "Starting" => Ok(SubAgentLifecycle::Starting),
        "Running" => Ok(SubAgentLifecycle::Running),
        "Idle" => Ok(SubAgentLifecycle::Idle),
        "Stopping" => Ok(SubAgentLifecycle::Stopping),
        "Completed" => Ok(SubAgentLifecycle::Completed),
        "Failed" => Ok(SubAgentLifecycle::Failed),
        _ => Err(SubAgentHostError::StorageUnavailable),
    }
}

fn disposition_tag(disposition: &InputDisposition) -> Result<&'static str, SubAgentHostError> {
    match disposition {
        InputDisposition::StartedForegroundWork => Ok("StartedForegroundWork"),
        InputDisposition::ContributedToActiveWork => Ok("ContributedToActiveWork"),
        InputDisposition::QueuedForTurnBoundary => Ok("QueuedForTurnBoundary"),
        InputDisposition::CancelRequestedThenQueued => Ok("CancelRequestedThenQueued"),
        InputDisposition::Unknown { .. } => Err(SubAgentHostError::StorageUnavailable),
    }
}

fn parse_disposition(value: &str) -> Result<InputDisposition, SubAgentHostError> {
    match value {
        "StartedForegroundWork" => Ok(InputDisposition::StartedForegroundWork),
        "ContributedToActiveWork" => Ok(InputDisposition::ContributedToActiveWork),
        "QueuedForTurnBoundary" => Ok(InputDisposition::QueuedForTurnBoundary),
        "CancelRequestedThenQueued" => Ok(InputDisposition::CancelRequestedThenQueued),
        _ => Err(SubAgentHostError::StorageUnavailable),
    }
}

fn event_kind_tag(kind: &SubAgentEventKind) -> Result<&'static str, SubAgentHostError> {
    match kind {
        SubAgentEventKind::Lifecycle => Ok("Lifecycle"),
        SubAgentEventKind::NativeAcp => Ok("NativeAcp"),
        SubAgentEventKind::ParentToChild => Ok("ParentToChild"),
        SubAgentEventKind::ChildToParent => Ok("ChildToParent"),
        SubAgentEventKind::Compaction => Ok("Compaction"),
        SubAgentEventKind::Failed => Ok("Failed"),
        SubAgentEventKind::Unknown { .. } => Err(SubAgentHostError::StorageUnavailable),
    }
}

fn parse_event_kind(value: &str) -> Result<SubAgentEventKind, SubAgentHostError> {
    match value {
        "Lifecycle" => Ok(SubAgentEventKind::Lifecycle),
        "NativeAcp" => Ok(SubAgentEventKind::NativeAcp),
        "ParentToChild" => Ok(SubAgentEventKind::ParentToChild),
        "ChildToParent" => Ok(SubAgentEventKind::ChildToParent),
        "Compaction" => Ok(SubAgentEventKind::Compaction),
        "Failed" => Ok(SubAgentEventKind::Failed),
        _ => Err(SubAgentHostError::StorageUnavailable),
    }
}

fn acp_direction_tag(direction: &AcpEventDirection) -> Result<&'static str, SubAgentHostError> {
    match direction {
        AcpEventDirection::ClientToAgent => Ok("ClientToAgent"),
        AcpEventDirection::AgentToClient => Ok("AgentToClient"),
        AcpEventDirection::Unknown { .. } => Err(SubAgentHostError::InvalidNativePayload),
    }
}

fn acp_kind_tag(kind: &AcpEventKind) -> Result<&'static str, SubAgentHostError> {
    match kind {
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
        AcpEventKind::Unknown { .. } => Err(SubAgentHostError::InvalidNativePayload),
    }
}

fn parse_json(value: &str) -> Result<Value, SubAgentHostError> {
    serde_json::from_str(value).map_err(|_| SubAgentHostError::InvalidNativePayload)
}

fn db_i64(value: u64) -> Result<i64, SubAgentHostError> {
    i64::try_from(value).map_err(|_| SubAgentHostError::StorageUnavailable)
}

fn db_u64(value: i64) -> Result<u64, SubAgentHostError> {
    u64::try_from(value).map_err(|_| SubAgentHostError::StorageUnavailable)
}

fn storage_error(_: rusqlite::Error) -> SubAgentHostError {
    SubAgentHostError::StorageUnavailable
}

#[cfg(test)]
mod tests {
    use boxology_import_agent_host::{AcpEvent, AcpEventDirection, AcpEventKind};
    use rusqlite::Connection;

    use super::{
        ContextRealization, InputDisposition, InteractionDirection, ReadSubAgentEventsRequest,
        SpawnSubAgentRequest, SubAgentContextMode, SubAgentEventKind, SubAgentHostError,
        SubAgentInputMode, SubAgentLifecycle, SubAgentStore, spawn_fingerprint,
    };

    fn request(client_id: &str) -> SpawnSubAgentRequest {
        SpawnSubAgentRequest {
            client_sub_agent_id: client_id.into(),
            parent_session_id: "parent-1".into(),
            agent_id: "agent-1".into(),
            working_directory: "/tmp".into(),
            context_mode: SubAgentContextMode::Fresh,
            parent_context_through_sequence: None,
            allow_portable_snapshot: false,
            native_task_prompt_json: r#"[{"type":"text","text":"work"}]"#.into(),
            metadata_json: r#"{"purpose":"test"}"#.into(),
            crash_restart_limit: 0,
        }
    }

    #[test]
    fn durable_ids_interactions_and_native_events_are_retry_safe() {
        let store = SubAgentStore::open_in_memory().expect("store opens");
        let request = request("client-1");
        let fingerprint = spawn_fingerprint(&request).expect("request fingerprints");
        let start = store
            .begin_spawn(
                &request,
                &ContextRealization::FreshSession,
                &fingerprint,
                10,
            )
            .expect("spawn starts");
        assert!(start.inserted);
        let retry = store
            .begin_spawn(
                &request,
                &ContextRealization::FreshSession,
                &fingerprint,
                20,
            )
            .expect("spawn retry resolves");
        assert!(!retry.inserted);
        assert_eq!(retry.record.sub_agent_id, start.record.sub_agent_id);

        let mut conflicting = request.clone();
        conflicting.agent_id = "changed".into();
        assert!(matches!(
            store.begin_spawn(
                &conflicting,
                &ContextRealization::FreshSession,
                &spawn_fingerprint(&conflicting).expect("conflict fingerprints"),
                30,
            ),
            Err(SubAgentHostError::DuplicateIdConflict)
        ));

        let record = store
            .complete_spawn(&start.record.sub_agent_id, "child-1", "native-1", 40)
            .expect("spawn completes");
        assert_eq!(record.lifecycle, SubAgentLifecycle::Running);
        let interaction = store
            .begin_interaction(
                &record.sub_agent_id,
                "message-1",
                &InteractionDirection::ParentToChild,
                &SubAgentInputMode::Queue,
                r#"[{"type":"text","text":"hello"}]"#,
                50,
            )
            .expect("interaction begins");
        assert!(interaction.should_dispatch);
        let receipt = store
            .complete_interaction(
                &record.sub_agent_id,
                "message-1",
                &InteractionDirection::ParentToChild,
                &InputDisposition::StartedForegroundWork,
                60,
            )
            .expect("interaction completes");
        let interaction_retry = store
            .begin_interaction(
                &record.sub_agent_id,
                "message-1",
                &InteractionDirection::ParentToChild,
                &SubAgentInputMode::Queue,
                r#"[{"type":"text","text":"hello"}]"#,
                70,
            )
            .expect("interaction retry resolves");
        assert_eq!(interaction_retry.completed, Some(receipt));
        assert!(matches!(
            store.begin_interaction(
                &record.sub_agent_id,
                "message-1",
                &InteractionDirection::ParentToChild,
                &SubAgentInputMode::Queue,
                r#"[{"type":"text","text":"changed"}]"#,
                80,
            ),
            Err(SubAgentHostError::DuplicateIdConflict)
        ));

        let event = AcpEvent {
            session_id: "child-1".into(),
            run_id: Some("run-1".into()),
            sequence: 1,
            observed_at_ms: 90,
            kind: AcpEventKind::Message,
            direction: AcpEventDirection::AgentToClient,
            native_event_json: r#"{"jsonrpc":"2.0","method":"session/update"}"#.into(),
        };
        store
            .record_native_event(&record.sub_agent_id, &event)
            .expect("native event records");
        store
            .record_native_event(&record.sub_agent_id, &event)
            .expect("native event retry deduplicates");
        let page = store
            .read_events(&ReadSubAgentEventsRequest {
                sub_agent_id: record.sub_agent_id,
                after_sequence: 0,
                limit: 100,
            })
            .expect("events read");
        assert_eq!(
            page.events
                .iter()
                .map(|event| event.kind.clone())
                .collect::<Vec<_>>(),
            [
                SubAgentEventKind::Lifecycle,
                SubAgentEventKind::Lifecycle,
                SubAgentEventKind::ParentToChild,
                SubAgentEventKind::NativeAcp,
            ]
        );
        assert!(page.caught_up);
    }

    #[test]
    fn restart_stages_children_for_recovery_and_future_schema_fails_closed() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("sub-agents.sqlite");
        let sub_agent_id = {
            let store = SubAgentStore::open(&path).expect("store opens");
            let request = request("client-restart");
            let start = store
                .begin_spawn(
                    &request,
                    &ContextRealization::FreshSession,
                    &spawn_fingerprint(&request).expect("request fingerprints"),
                    10,
                )
                .expect("spawn starts");
            store
                .complete_spawn(&start.record.sub_agent_id, "child", "native", 20)
                .expect("spawn completes");
            store
                .begin_interaction(
                    &start.record.sub_agent_id,
                    "pending",
                    &InteractionDirection::ChildToParent,
                    &SubAgentInputMode::Queue,
                    r#"{"progress":1}"#,
                    30,
                )
                .expect("pending interaction records");
            start.record.sub_agent_id
        };
        let restarted = SubAgentStore::open(&path).expect("store restarts");
        let status = restarted.status(&sub_agent_id).expect("status survives");
        assert_eq!(status.record.lifecycle, SubAgentLifecycle::Failed);
        assert_eq!(status.pending_child_to_parent, 0);
        assert!(
            status
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("recovery pending"))
        );
        let candidates = restarted
            .recovery_candidates()
            .expect("recovery candidates read");
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].record.sub_agent_id, sub_agent_id);
        assert_eq!(candidates[0].crash_restart_limit, 0);
        assert_eq!(candidates[0].restart_count, 0);
        drop(restarted);

        let connection = Connection::open(&path).expect("database opens directly");
        connection
            .pragma_update(None, "user_version", 99)
            .expect("future schema is installed");
        drop(connection);
        assert!(matches!(
            SubAgentStore::open(&path),
            Err(SubAgentHostError::StorageUnavailable)
        ));
    }
}
