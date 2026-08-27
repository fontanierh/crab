use std::{path::Path, sync::Mutex};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::Value;
use uuid::Uuid;

use crate::{
    AcceptedTurn, BindChannelRequest, ChannelBinding, ChannelBindingCatalog, ChannelBindingSummary,
    ChannelInputMode, ChannelLifecycle, ChannelReceipt, ChannelTurn, ChannelTurnDisposition,
    LocateBindingRequest, NativeChannelError, NativeChannelEvent, PublishReceipt,
};

const SCHEMA_VERSION: i64 = 2;
const MAX_BINDING_CATALOG: u64 = 256;

struct StoredBindingSummary {
    binding_id: String,
    channel_id: String,
    adapter_id: String,
    session_id: String,
    lifecycle: String,
    published_sequence: i64,
    pending_input_count: i64,
    last_error: Option<String>,
    updated_at_ms: i64,
}

impl StoredBindingSummary {
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            binding_id: row.get(0)?,
            channel_id: row.get(1)?,
            adapter_id: row.get(2)?,
            session_id: row.get(3)?,
            lifecycle: row.get(4)?,
            published_sequence: row.get(5)?,
            pending_input_count: row.get(6)?,
            last_error: row.get(7)?,
            updated_at_ms: row.get(8)?,
        })
    }

    fn into_contract(self) -> Result<ChannelBindingSummary, NativeChannelError> {
        Ok(ChannelBindingSummary {
            binding_id: self.binding_id,
            channel_id: self.channel_id,
            adapter_id: self.adapter_id,
            session_id: self.session_id,
            lifecycle: parse_lifecycle(&self.lifecycle)?,
            published_sequence: db_u64(self.published_sequence)?,
            pending_input_count: db_u64(self.pending_input_count)?,
            last_error: self.last_error,
            updated_at_ms: db_u64(self.updated_at_ms)?,
        })
    }
}

pub(crate) struct ChannelStore {
    connection: Mutex<Connection>,
}

impl ChannelStore {
    pub(crate) fn open(path: impl AsRef<Path>) -> Result<Self, NativeChannelError> {
        Self::initialize(Connection::open(path).map_err(storage_error)?)
    }

    pub(crate) fn open_in_memory() -> Result<Self, NativeChannelError> {
        Self::initialize(Connection::open_in_memory().map_err(storage_error)?)
    }

    fn initialize(mut connection: Connection) -> Result<Self, NativeChannelError> {
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
            _ => return Err(NativeChannelError::StorageUnavailable),
        }
        connection
            .execute(
                "UPDATE bindings
                 SET lifecycle = 'Failed', last_error = 'runtime restarted; replace session',
                     updated_at_ms = MAX(updated_at_ms, created_at_ms)
                 WHERE lifecycle IN ('Binding', 'Attached', 'Replaying')",
                [],
            )
            .map_err(storage_error)?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    pub(crate) fn bind(
        &self,
        request: &BindChannelRequest,
        now_ms: u64,
    ) -> Result<ChannelBinding, NativeChannelError> {
        validate_binding(request)?;
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let exists = transaction
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM bindings
                    WHERE adapter_id = ?1 AND channel_id = ?2 AND lifecycle != 'Detached'
                 )",
                params![request.adapter_id, request.channel_id],
                |row| row.get::<_, bool>(0),
            )
            .map_err(storage_error)?;
        if exists {
            return Err(NativeChannelError::AlreadyBound);
        }
        let binding_id = format!("binding_{}", Uuid::new_v4());
        transaction
            .execute(
                "INSERT INTO bindings (
                    binding_id, channel_id, adapter_id, session_id, native_channel_json,
                    lifecycle, published_sequence, reconciled_sequence, last_error,
                    created_at_ms, updated_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, 'Attached', 0, 0, NULL, ?6, ?6)",
                params![
                    binding_id,
                    request.channel_id,
                    request.adapter_id,
                    request.session_id,
                    request.native_channel_json,
                    db_i64(now_ms)?,
                ],
            )
            .map_err(storage_error)?;
        transaction.commit().map_err(storage_error)?;
        drop(connection);
        self.binding(&binding_id)
    }

    pub(crate) fn binding(&self, binding_id: &str) -> Result<ChannelBinding, NativeChannelError> {
        let connection = self.lock()?;
        query_binding(&connection, binding_id)?.ok_or(NativeChannelError::UnknownBinding)
    }

    pub(crate) fn list_bindings(
        &self,
        limit: u64,
    ) -> Result<ChannelBindingCatalog, NativeChannelError> {
        if limit == 0 || limit > MAX_BINDING_CATALOG {
            return Err(NativeChannelError::InvalidNativePayload);
        }
        let connection = self.lock()?;
        let total_bindings = connection
            .query_row("SELECT COUNT(*) FROM bindings", [], |row| {
                row.get::<_, i64>(0)
            })
            .map_err(storage_error)?;
        let mut statement = connection
            .prepare(
                "SELECT b.binding_id, b.channel_id, b.adapter_id, b.session_id, b.lifecycle,
                        b.published_sequence,
                        (SELECT COUNT(*) FROM turns t
                         WHERE t.binding_id = b.binding_id AND t.session_id = b.session_id
                           AND t.state = 'Pending'),
                        b.last_error, b.updated_at_ms
                 FROM bindings b
                 ORDER BY b.updated_at_ms DESC, b.binding_id ASC
                 LIMIT ?1",
            )
            .map_err(storage_error)?;
        let rows = statement
            .query_map(params![db_i64(limit)?], StoredBindingSummary::from_row)
            .map_err(storage_error)?;
        let mut bindings = Vec::new();
        for row in rows {
            bindings.push(row.map_err(storage_error)?.into_contract()?);
        }
        Ok(ChannelBindingCatalog {
            bindings,
            total_bindings: db_u64(total_bindings)?,
        })
    }

    pub(crate) fn binding_summary(
        &self,
        binding_id: &str,
    ) -> Result<ChannelBindingSummary, NativeChannelError> {
        if binding_id.trim().is_empty() {
            return Err(NativeChannelError::InvalidNativePayload);
        }
        let connection = self.lock()?;
        connection
            .query_row(
                "SELECT b.binding_id, b.channel_id, b.adapter_id, b.session_id, b.lifecycle,
                        b.published_sequence,
                        (SELECT COUNT(*) FROM turns t
                         WHERE t.binding_id = b.binding_id AND t.session_id = b.session_id
                           AND t.state = 'Pending'),
                        b.last_error, b.updated_at_ms
                 FROM bindings b WHERE b.binding_id = ?1",
                params![binding_id],
                StoredBindingSummary::from_row,
            )
            .optional()
            .map_err(storage_error)?
            .ok_or(NativeChannelError::UnknownBinding)?
            .into_contract()
    }

    pub(crate) fn find_binding(
        &self,
        request: &LocateBindingRequest,
    ) -> Result<ChannelBinding, NativeChannelError> {
        let connection = self.lock()?;
        let binding_id = connection
            .query_row(
                "SELECT binding_id FROM bindings
                 WHERE channel_id = ?1 AND adapter_id = ?2 AND lifecycle != 'Detached'",
                params![request.channel_id, request.adapter_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(storage_error)?
            .ok_or(NativeChannelError::UnknownBinding)?;
        query_binding(&connection, &binding_id)?.ok_or(NativeChannelError::UnknownBinding)
    }

    pub(crate) fn last_error(
        &self,
        binding_id: &str,
    ) -> Result<Option<String>, NativeChannelError> {
        let connection = self.lock()?;
        connection
            .query_row(
                "SELECT last_error FROM bindings WHERE binding_id = ?1",
                params![binding_id],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()
            .map_err(storage_error)?
            .ok_or(NativeChannelError::UnknownBinding)
    }

    pub(crate) fn existing_turn(
        &self,
        request: &ChannelTurn,
        session_id: &str,
        interrupt_reason: Option<&str>,
    ) -> Result<Option<AcceptedTurn>, NativeChannelError> {
        let connection = self.lock()?;
        let existing = connection
            .query_row(
                "SELECT mode, native_prompt_json, run_id, disposition, accepted_at_ms,
                        interrupted_run_id, cancel_requested_at_ms, interrupting,
                        interrupt_reason
                 FROM turns
                 WHERE binding_id = ?1 AND session_id = ?2 AND client_turn_id = ?3",
                params![request.binding_id, session_id, request.client_turn_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<i64>>(6)?,
                        row.get::<_, bool>(7)?,
                        row.get::<_, Option<String>>(8)?,
                    ))
                },
            )
            .optional()
            .map_err(storage_error)?;
        let Some((
            mode,
            prompt,
            run_id,
            disposition,
            accepted_at_ms,
            interrupted_run_id,
            cancel_requested_at_ms,
            stored_interrupting,
            stored_interrupt_reason,
        )) = existing
        else {
            return Ok(None);
        };
        if mode != input_mode_tag(&request.mode)?
            || prompt != request.native_prompt_json
            || stored_interrupting != interrupt_reason.is_some()
            || stored_interrupt_reason.as_deref() != interrupt_reason
        {
            return Err(NativeChannelError::DuplicateTurnConflict);
        }
        Ok(Some(AcceptedTurn {
            binding_id: request.binding_id.clone(),
            session_id: session_id.to_owned(),
            client_turn_id: request.client_turn_id.clone(),
            accepted_at_ms: db_u64(accepted_at_ms)?,
            mode: request.mode.clone(),
            run_id,
            disposition: parse_disposition(&disposition)?,
            interrupted_run_id,
            cancel_requested_at_ms: cancel_requested_at_ms.map(db_u64).transpose()?,
        }))
    }

    pub(crate) fn record_turn(
        &self,
        request: &ChannelTurn,
        accepted: &AcceptedTurn,
    ) -> Result<AcceptedTurn, NativeChannelError> {
        self.record_turn_inner(request, accepted, None)
    }

    pub(crate) fn record_interrupting_turn(
        &self,
        request: &ChannelTurn,
        accepted: &AcceptedTurn,
        requested_at_ms: u64,
        reason: &str,
    ) -> Result<AcceptedTurn, NativeChannelError> {
        self.record_turn_inner(request, accepted, Some((requested_at_ms, reason)))
    }

    fn record_turn_inner(
        &self,
        request: &ChannelTurn,
        accepted: &AcceptedTurn,
        interruption: Option<(u64, &str)>,
    ) -> Result<AcceptedTurn, NativeChannelError> {
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let is_interrupting = interruption.is_some();
        let is_pending = matches!(
            accepted.disposition,
            ChannelTurnDisposition::QueuedForTurnBoundary
                | ChannelTurnDisposition::CancelRequestedThenQueued
        );
        let has_cancellation =
            accepted.interrupted_run_id.is_some() && accepted.cancel_requested_at_ms.is_some();
        let valid_interruption = match (is_interrupting, &accepted.disposition, has_cancellation) {
            (true, ChannelTurnDisposition::StartedForegroundWork, false)
            | (true, ChannelTurnDisposition::CancelRequestedThenQueued, true) => true,
            (false, ChannelTurnDisposition::CancelRequestedThenQueued, _) | (false, _, true) => {
                false
            }
            (false, _, false) => true,
            (true, _, _) => false,
        };
        if accepted.interrupted_run_id.is_some() != accepted.cancel_requested_at_ms.is_some()
            || !valid_interruption
        {
            return Err(NativeChannelError::StorageUnavailable);
        }
        transaction
            .execute(
                "INSERT INTO turns (
                    binding_id, session_id, client_turn_id, mode, native_prompt_json,
                    run_id, disposition, state, received_at_ms, accepted_at_ms,
                    interrupted_run_id, cancel_requested_at_ms, interrupting,
                    interrupt_reason
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                params![
                    request.binding_id,
                    accepted.session_id,
                    request.client_turn_id,
                    input_mode_tag(&request.mode)?,
                    request.native_prompt_json,
                    accepted.run_id,
                    disposition_tag(&accepted.disposition)?,
                    if is_pending { "Pending" } else { "Active" },
                    db_i64(request.received_at_ms)?,
                    db_i64(accepted.accepted_at_ms)?,
                    accepted.interrupted_run_id,
                    accepted.cancel_requested_at_ms.map(db_i64).transpose()?,
                    is_interrupting,
                    interruption.map(|(_, reason)| reason),
                ],
            )
            .map_err(storage_error)?;
        if let (
            Some(interrupted_run_id),
            Some(cancel_requested_at_ms),
            Some((requested_at_ms, reason)),
        ) = (
            accepted.interrupted_run_id.as_deref(),
            accepted.cancel_requested_at_ms,
            interruption,
        ) {
            let pending_input_count = transaction
                .query_row(
                    "SELECT COUNT(*) FROM turns
                     WHERE binding_id = ?1 AND session_id = ?2 AND state = 'Pending'",
                    params![request.binding_id, accepted.session_id],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(storage_error)?;
            transaction
                .execute(
                    "INSERT INTO interrupts (
                        binding_id, session_id, run_id, requested_at_ms, reason,
                        cancelled_at_ms, pending_input_count
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        request.binding_id,
                        accepted.session_id,
                        interrupted_run_id,
                        db_i64(requested_at_ms)?,
                        reason,
                        db_i64(cancel_requested_at_ms)?,
                        pending_input_count,
                    ],
                )
                .map_err(storage_error)?;
        }
        transaction.commit().map_err(storage_error)?;
        Ok(accepted.clone())
    }

    pub(crate) fn reconcile(
        &self,
        binding_id: &str,
        session_id: &str,
        through_sequence: u64,
        finished_runs: &[String],
        active_run_id: Option<&str>,
    ) -> Result<(), NativeChannelError> {
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        for run_id in finished_runs {
            transaction
                .execute(
                    "UPDATE turns SET state = 'Completed'
                     WHERE binding_id = ?1 AND session_id = ?2 AND run_id = ?3",
                    params![binding_id, session_id, run_id],
                )
                .map_err(storage_error)?;
        }
        if let Some(run_id) = active_run_id {
            transaction
                .execute(
                    "UPDATE turns SET state = 'Active'
                     WHERE binding_id = ?1 AND session_id = ?2 AND run_id = ?3
                       AND state = 'Pending'",
                    params![binding_id, session_id, run_id],
                )
                .map_err(storage_error)?;
        }
        let changed = transaction
            .execute(
                "UPDATE bindings SET reconciled_sequence = MAX(reconciled_sequence, ?3)
                 WHERE binding_id = ?1 AND session_id = ?2 AND lifecycle != 'Detached'",
                params![binding_id, session_id, db_i64(through_sequence)?],
            )
            .map_err(storage_error)?;
        if changed != 1 {
            return Err(NativeChannelError::SessionMismatch);
        }
        transaction.commit().map_err(storage_error)
    }

    pub(crate) fn reconciled_sequence(
        &self,
        binding_id: &str,
        session_id: &str,
    ) -> Result<u64, NativeChannelError> {
        let connection = self.lock()?;
        let value = connection
            .query_row(
                "SELECT reconciled_sequence FROM bindings
                 WHERE binding_id = ?1 AND session_id = ?2 AND lifecycle != 'Detached'",
                params![binding_id, session_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(storage_error)?
            .ok_or(NativeChannelError::SessionMismatch)?;
        db_u64(value)
    }

    pub(crate) fn pending_count(
        &self,
        binding_id: &str,
        session_id: &str,
    ) -> Result<u64, NativeChannelError> {
        let connection = self.lock()?;
        let count = connection
            .query_row(
                "SELECT COUNT(*) FROM turns
                 WHERE binding_id = ?1 AND session_id = ?2 AND state = 'Pending'",
                params![binding_id, session_id],
                |row| row.get::<_, i64>(0),
            )
            .map_err(storage_error)?;
        db_u64(count)
    }

    pub(crate) fn record_publication(
        &self,
        event: &NativeChannelEvent,
        now_ms: u64,
    ) -> Result<PublishReceipt, NativeChannelError> {
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let existing = transaction
            .query_row(
                "SELECT native_event_json, delivery_id, published_at_ms
                 FROM publications
                 WHERE binding_id = ?1 AND session_id = ?2 AND sequence = ?3",
                params![event.binding_id, event.session_id, db_i64(event.sequence)?],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(storage_error)?;
        if let Some((native_event_json, delivery_id, published_at_ms)) = existing {
            if native_event_json != event.native_event_json {
                return Err(NativeChannelError::SequenceGap);
            }
            transaction.commit().map_err(storage_error)?;
            return Ok(PublishReceipt {
                binding_id: event.binding_id.clone(),
                sequence: event.sequence,
                delivery_id,
                published_at_ms: db_u64(published_at_ms)?,
            });
        }
        let current = transaction
            .query_row(
                "SELECT published_sequence FROM bindings
                 WHERE binding_id = ?1 AND session_id = ?2 AND lifecycle != 'Detached'",
                params![event.binding_id, event.session_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(storage_error)?
            .ok_or(NativeChannelError::SessionMismatch)?;
        let expected = db_u64(current)?
            .checked_add(1)
            .ok_or(NativeChannelError::StorageUnavailable)?;
        if event.sequence != expected {
            return Err(NativeChannelError::SequenceGap);
        }
        let delivery_id = format!(
            "delivery_{}_{}_{}",
            event.binding_id, event.session_id, event.sequence
        );
        transaction
            .execute(
                "INSERT INTO publications (
                    binding_id, session_id, sequence, native_event_json, delivery_id,
                    published_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    event.binding_id,
                    event.session_id,
                    db_i64(event.sequence)?,
                    event.native_event_json,
                    delivery_id,
                    db_i64(now_ms)?,
                ],
            )
            .map_err(storage_error)?;
        transaction
            .execute(
                "UPDATE bindings SET published_sequence = ?2, updated_at_ms = ?3
                 WHERE binding_id = ?1",
                params![event.binding_id, db_i64(event.sequence)?, db_i64(now_ms)?],
            )
            .map_err(storage_error)?;
        transaction.commit().map_err(storage_error)?;
        Ok(PublishReceipt {
            binding_id: event.binding_id.clone(),
            sequence: event.sequence,
            delivery_id,
            published_at_ms: now_ms,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_interrupt(
        &self,
        binding_id: &str,
        session_id: &str,
        run_id: &str,
        requested_at_ms: u64,
        reason: &str,
        cancelled_at_ms: u64,
        pending_input_count: u64,
    ) -> Result<(), NativeChannelError> {
        let connection = self.lock()?;
        connection
            .execute(
                "INSERT INTO interrupts (
                    binding_id, session_id, run_id, requested_at_ms, reason,
                    cancelled_at_ms, pending_input_count
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    binding_id,
                    session_id,
                    run_id,
                    db_i64(requested_at_ms)?,
                    reason,
                    db_i64(cancelled_at_ms)?,
                    db_i64(pending_input_count)?,
                ],
            )
            .map_err(storage_error)?;
        Ok(())
    }

    pub(crate) fn replace_session(
        &self,
        binding_id: &str,
        expected_session_id: &str,
        fresh_session_id: &str,
        fresh_native_channel_json: Option<&str>,
        reason: &str,
        now_ms: u64,
    ) -> Result<ChannelBinding, NativeChannelError> {
        if fresh_session_id.trim().is_empty() || fresh_session_id == expected_session_id {
            return Err(NativeChannelError::SessionMismatch);
        }
        if let Some(fresh_native_channel_json) = fresh_native_channel_json {
            let native_channel: Value = serde_json::from_str(fresh_native_channel_json)
                .map_err(|_| NativeChannelError::InvalidNativePayload)?;
            if !native_channel.is_object() {
                return Err(NativeChannelError::InvalidNativePayload);
            }
        }
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let changed = transaction
            .execute(
                "UPDATE bindings
                 SET session_id = ?3, native_channel_json = COALESCE(?4, native_channel_json), lifecycle = 'Attached', published_sequence = 0,
                     reconciled_sequence = 0, last_error = NULL, updated_at_ms = ?5
                 WHERE binding_id = ?1 AND session_id = ?2 AND lifecycle != 'Detached'",
                params![
                    binding_id,
                    expected_session_id,
                    fresh_session_id,
                    fresh_native_channel_json,
                    db_i64(now_ms)?,
                ],
            )
            .map_err(storage_error)?;
        if changed != 1 {
            return Err(if query_binding(&transaction, binding_id)?.is_some() {
                NativeChannelError::SessionMismatch
            } else {
                NativeChannelError::UnknownBinding
            });
        }
        transaction
            .execute(
                "INSERT INTO session_replacements (
                    binding_id, previous_session_id, fresh_session_id, reason, replaced_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    binding_id,
                    expected_session_id,
                    fresh_session_id,
                    reason,
                    db_i64(now_ms)?,
                ],
            )
            .map_err(storage_error)?;
        transaction.commit().map_err(storage_error)?;
        drop(connection);
        self.binding(binding_id)
    }

    pub(crate) fn recover_session(
        &self,
        binding_id: &str,
        expected_session_id: &str,
        now_ms: u64,
    ) -> Result<ChannelBinding, NativeChannelError> {
        if binding_id.trim().is_empty() || expected_session_id.trim().is_empty() {
            return Err(NativeChannelError::InvalidNativePayload);
        }
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let changed = transaction
            .execute(
                "UPDATE bindings
                 SET lifecycle = 'Attached', last_error = NULL, updated_at_ms = ?3
                 WHERE binding_id = ?1 AND session_id = ?2 AND lifecycle = 'Failed'",
                params![binding_id, expected_session_id, db_i64(now_ms)?],
            )
            .map_err(storage_error)?;
        if changed != 1 {
            return match query_binding(&transaction, binding_id)? {
                Some(binding)
                    if binding.session_id == expected_session_id
                        && matches!(binding.lifecycle, ChannelLifecycle::Attached) =>
                {
                    transaction.commit().map_err(storage_error)?;
                    Ok(binding)
                }
                Some(_) => Err(NativeChannelError::SessionMismatch),
                None => Err(NativeChannelError::UnknownBinding),
            };
        }
        transaction.commit().map_err(storage_error)?;
        drop(connection);
        self.binding(binding_id)
    }

    pub(crate) fn detach(
        &self,
        binding_id: &str,
        now_ms: u64,
    ) -> Result<ChannelReceipt, NativeChannelError> {
        let connection = self.lock()?;
        let changed = connection
            .execute(
                "UPDATE bindings SET lifecycle = 'Detached', updated_at_ms = ?2
                 WHERE binding_id = ?1 AND lifecycle != 'Detached'",
                params![binding_id, db_i64(now_ms)?],
            )
            .map_err(storage_error)?;
        if changed == 0 && query_binding(&connection, binding_id)?.is_none() {
            return Err(NativeChannelError::UnknownBinding);
        }
        Ok(ChannelReceipt {
            accepted: true,
            recorded_at_ms: now_ms,
        })
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, NativeChannelError> {
        self.connection
            .lock()
            .map_err(|_| NativeChannelError::StorageUnavailable)
    }
}

fn migrate_v0_to_v1(connection: &mut Connection) -> Result<(), NativeChannelError> {
    let transaction = connection.transaction().map_err(storage_error)?;
    transaction
        .execute_batch(
            "CREATE TABLE bindings (
                binding_id TEXT PRIMARY KEY,
                channel_id TEXT NOT NULL,
                adapter_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                native_channel_json TEXT NOT NULL,
                lifecycle TEXT NOT NULL,
                published_sequence INTEGER NOT NULL,
                reconciled_sequence INTEGER NOT NULL,
                last_error TEXT,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL
             );
             CREATE UNIQUE INDEX one_live_channel_binding
                ON bindings(adapter_id, channel_id) WHERE lifecycle != 'Detached';
             CREATE TABLE turns (
                binding_id TEXT NOT NULL REFERENCES bindings(binding_id),
                session_id TEXT NOT NULL,
                client_turn_id TEXT NOT NULL,
                mode TEXT NOT NULL,
                native_prompt_json TEXT NOT NULL,
                run_id TEXT NOT NULL,
                disposition TEXT NOT NULL,
                state TEXT NOT NULL,
                received_at_ms INTEGER NOT NULL,
                accepted_at_ms INTEGER NOT NULL,
                PRIMARY KEY(binding_id, session_id, client_turn_id)
             );
             CREATE TABLE publications (
                binding_id TEXT NOT NULL REFERENCES bindings(binding_id),
                session_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                native_event_json TEXT NOT NULL,
                delivery_id TEXT NOT NULL UNIQUE,
                published_at_ms INTEGER NOT NULL,
                PRIMARY KEY(binding_id, session_id, sequence)
             );
             CREATE TABLE interrupts (
                interrupt_id INTEGER PRIMARY KEY AUTOINCREMENT,
                binding_id TEXT NOT NULL REFERENCES bindings(binding_id),
                session_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                requested_at_ms INTEGER NOT NULL,
                reason TEXT NOT NULL,
                cancelled_at_ms INTEGER NOT NULL,
                pending_input_count INTEGER NOT NULL
             );
             CREATE TABLE session_replacements (
                replacement_id INTEGER PRIMARY KEY AUTOINCREMENT,
                binding_id TEXT NOT NULL REFERENCES bindings(binding_id),
                previous_session_id TEXT NOT NULL,
                fresh_session_id TEXT NOT NULL,
                reason TEXT NOT NULL,
                replaced_at_ms INTEGER NOT NULL
             );
             PRAGMA user_version = 1;",
        )
        .map_err(storage_error)?;
    transaction.commit().map_err(storage_error)
}

fn migrate_v1_to_v2(connection: &mut Connection) -> Result<(), NativeChannelError> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(storage_error)?;
    transaction
        .execute_batch(
            "ALTER TABLE turns ADD COLUMN interrupted_run_id TEXT;
             ALTER TABLE turns ADD COLUMN cancel_requested_at_ms INTEGER;
             ALTER TABLE turns ADD COLUMN interrupting INTEGER NOT NULL DEFAULT 0;
             ALTER TABLE turns ADD COLUMN interrupt_reason TEXT;
             PRAGMA user_version = 2;",
        )
        .map_err(storage_error)?;
    transaction.commit().map_err(storage_error)
}

fn validate_binding(request: &BindChannelRequest) -> Result<(), NativeChannelError> {
    if request.channel_id.trim().is_empty()
        || request.adapter_id.trim().is_empty()
        || request.session_id.trim().is_empty()
    {
        return Err(NativeChannelError::InvalidNativePayload);
    }
    let metadata: Value = serde_json::from_str(&request.native_channel_json)
        .map_err(|_| NativeChannelError::InvalidNativePayload)?;
    if !metadata.is_object() {
        return Err(NativeChannelError::InvalidNativePayload);
    }
    Ok(())
}

fn query_binding(
    connection: &Connection,
    binding_id: &str,
) -> Result<Option<ChannelBinding>, NativeChannelError> {
    connection
        .query_row(
            "SELECT channel_id, adapter_id, session_id, lifecycle, native_channel_json,
                    published_sequence
             FROM bindings WHERE binding_id = ?1",
            params![binding_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            },
        )
        .optional()
        .map_err(storage_error)?
        .map(
            |(channel_id, adapter_id, session_id, lifecycle, native_json, sequence)| {
                Ok(ChannelBinding {
                    binding_id: binding_id.to_owned(),
                    channel_id,
                    adapter_id,
                    session_id,
                    lifecycle: parse_lifecycle(&lifecycle)?,
                    native_channel_json: native_json,
                    published_sequence: db_u64(sequence)?,
                })
            },
        )
        .transpose()
}

fn input_mode_tag(mode: &ChannelInputMode) -> Result<&'static str, NativeChannelError> {
    match mode {
        ChannelInputMode::Queue => Ok("Queue"),
        ChannelInputMode::Steer => Ok("Steer"),
        ChannelInputMode::Unknown { .. } => Err(NativeChannelError::InvalidNativePayload),
    }
}

fn disposition_tag(
    disposition: &ChannelTurnDisposition,
) -> Result<&'static str, NativeChannelError> {
    match disposition {
        ChannelTurnDisposition::StartedForegroundWork => Ok("StartedForegroundWork"),
        ChannelTurnDisposition::ContributedToActiveWork => Ok("ContributedToActiveWork"),
        ChannelTurnDisposition::QueuedForTurnBoundary => Ok("QueuedForTurnBoundary"),
        ChannelTurnDisposition::CancelRequestedThenQueued => Ok("CancelRequestedThenQueued"),
        ChannelTurnDisposition::Unknown { .. } => Err(NativeChannelError::StorageUnavailable),
    }
}

fn parse_disposition(value: &str) -> Result<ChannelTurnDisposition, NativeChannelError> {
    match value {
        "StartedForegroundWork" => Ok(ChannelTurnDisposition::StartedForegroundWork),
        "ContributedToActiveWork" => Ok(ChannelTurnDisposition::ContributedToActiveWork),
        "QueuedForTurnBoundary" => Ok(ChannelTurnDisposition::QueuedForTurnBoundary),
        "CancelRequestedThenQueued" => Ok(ChannelTurnDisposition::CancelRequestedThenQueued),
        _ => Err(NativeChannelError::StorageUnavailable),
    }
}

fn parse_lifecycle(value: &str) -> Result<ChannelLifecycle, NativeChannelError> {
    match value {
        "Binding" => Ok(ChannelLifecycle::Binding),
        "Attached" => Ok(ChannelLifecycle::Attached),
        "Replaying" => Ok(ChannelLifecycle::Replaying),
        "Detached" => Ok(ChannelLifecycle::Detached),
        "Failed" => Ok(ChannelLifecycle::Failed),
        _ => Err(NativeChannelError::StorageUnavailable),
    }
}

fn db_i64(value: u64) -> Result<i64, NativeChannelError> {
    i64::try_from(value).map_err(|_| NativeChannelError::InvalidNativePayload)
}

fn db_u64(value: i64) -> Result<u64, NativeChannelError> {
    u64::try_from(value).map_err(|_| NativeChannelError::StorageUnavailable)
}

fn storage_error(_: rusqlite::Error) -> NativeChannelError {
    NativeChannelError::StorageUnavailable
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use super::{ChannelStore, SCHEMA_VERSION, migrate_v0_to_v1};
    use crate::{
        AcceptedTurn, BindChannelRequest, ChannelInputMode, ChannelLifecycle, ChannelTurn,
        ChannelTurnDisposition, LocateBindingRequest, NativeChannelError,
    };

    fn request() -> BindChannelRequest {
        BindChannelRequest {
            channel_id: "channel-1".into(),
            adapter_id: "native-ui".into(),
            session_id: "session-1".into(),
            native_channel_json: r#"{"title":"Jim"}"#.into(),
        }
    }

    #[test]
    fn binding_catalog_is_bounded_ordered_and_reports_pending_work() {
        let store = ChannelStore::open_in_memory().expect("store opens");
        let first = store.bind(&request(), 10).expect("first binding persists");
        let second = store
            .bind(
                &BindChannelRequest {
                    channel_id: "channel-2".into(),
                    adapter_id: "native-ui".into(),
                    session_id: "session-2".into(),
                    native_channel_json: r#"{"privateDestination":"not-catalogued"}"#.into(),
                },
                20,
            )
            .expect("second binding persists");
        store
            .record_turn(
                &ChannelTurn {
                    binding_id: first.binding_id.clone(),
                    client_turn_id: "turn-1".into(),
                    received_at_ms: 21,
                    mode: ChannelInputMode::Queue,
                    native_prompt_json: "[]".into(),
                },
                &AcceptedTurn {
                    binding_id: first.binding_id.clone(),
                    session_id: first.session_id.clone(),
                    client_turn_id: "turn-1".into(),
                    accepted_at_ms: 22,
                    mode: ChannelInputMode::Queue,
                    run_id: "run-1".into(),
                    disposition: ChannelTurnDisposition::QueuedForTurnBoundary,
                    interrupted_run_id: None,
                    cancel_requested_at_ms: None,
                },
            )
            .expect("pending turn persists");

        let first_status = store
            .binding_summary(&first.binding_id)
            .expect("binding is discoverable by id");
        assert_eq!(first_status.pending_input_count, 1);
        assert_eq!(first_status.channel_id, "channel-1");

        store
            .detach(&second.binding_id, 30)
            .expect("second binding detaches");
        let catalog = store.list_bindings(1).expect("catalog is readable");
        assert_eq!(catalog.total_bindings, 2);
        assert_eq!(catalog.bindings.len(), 1);
        assert_eq!(catalog.bindings[0].binding_id, second.binding_id);
        assert_eq!(catalog.bindings[0].lifecycle, ChannelLifecycle::Detached);
        assert!(matches!(
            store.list_bindings(0),
            Err(NativeChannelError::InvalidNativePayload)
        ));
        assert!(matches!(
            store.list_bindings(257),
            Err(NativeChannelError::InvalidNativePayload)
        ));
    }

    #[test]
    fn binding_survives_restart_and_requires_explicit_session_replacement() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("native-channel.sqlite");
        let binding = {
            let store = ChannelStore::open(&path).expect("store opens");
            store.bind(&request(), 10).expect("binding persists")
        };

        let restarted = ChannelStore::open(&path).expect("store reopens");
        let recovered = restarted
            .binding(&binding.binding_id)
            .expect("binding remains readable");
        assert_eq!(recovered.lifecycle, ChannelLifecycle::Failed);
        assert_eq!(
            restarted
                .find_binding(&LocateBindingRequest {
                    channel_id: "channel-1".into(),
                    adapter_id: "native-ui".into(),
                })
                .expect("runtime can locate a crash-orphaned binding"),
            recovered
        );
        assert_eq!(
            restarted
                .last_error(&binding.binding_id)
                .expect("restart reason remains readable")
                .as_deref(),
            Some("runtime restarted; replace session")
        );
        let replaced = restarted
            .replace_session(
                &binding.binding_id,
                "session-1",
                "session-2",
                Some(r#"{"title":"Recovered"}"#),
                "fresh context",
                20,
            )
            .expect("explicit replacement recovers binding");
        assert_eq!(replaced.lifecycle, ChannelLifecycle::Attached);
        assert_eq!(replaced.session_id, "session-2");
        assert_eq!(replaced.native_channel_json, r#"{"title":"Recovered"}"#);
    }

    #[test]
    fn unknown_schema_version_fails_closed() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("future.sqlite");
        let connection = Connection::open(&path).expect("database opens");
        connection
            .pragma_update(None, "user_version", 99)
            .expect("future version is recorded");
        drop(connection);
        assert!(matches!(
            ChannelStore::open(&path),
            Err(NativeChannelError::StorageUnavailable)
        ));
    }

    #[test]
    fn schema_one_migrates_additive_interrupting_turn_columns() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("native-channel-v1.sqlite");
        let mut connection = Connection::open(&path).expect("database opens");
        migrate_v0_to_v1(&mut connection).expect("schema one is created");
        drop(connection);

        let store = ChannelStore::open(&path).expect("schema one migrates");
        let connection = store.lock().expect("migrated database locks");
        let version = connection
            .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
            .expect("schema version reads");
        assert_eq!(version, SCHEMA_VERSION);
        let columns = connection
            .prepare("PRAGMA table_info(turns)")
            .expect("turn columns prepare")
            .query_map([], |row| row.get::<_, String>(1))
            .expect("turn columns query")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("turn columns collect");
        for expected in [
            "interrupted_run_id",
            "cancel_requested_at_ms",
            "interrupting",
            "interrupt_reason",
        ] {
            assert!(columns.contains(&expected.to_owned()));
        }
    }
}
