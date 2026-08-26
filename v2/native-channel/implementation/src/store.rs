use std::{path::Path, sync::Mutex};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::Value;
use uuid::Uuid;

use crate::{
    AcceptedTurn, BindChannelRequest, ChannelBinding, ChannelInputMode, ChannelLifecycle,
    ChannelReceipt, ChannelTurn, ChannelTurnDisposition, LocateBindingRequest, NativeChannelError,
    NativeChannelEvent, PublishReceipt,
};

const SCHEMA_VERSION: i64 = 1;

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
            0 => migrate_v0_to_v1(&mut connection)?,
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
    ) -> Result<Option<AcceptedTurn>, NativeChannelError> {
        let connection = self.lock()?;
        let existing = connection
            .query_row(
                "SELECT mode, native_prompt_json, run_id, disposition, accepted_at_ms
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
                    ))
                },
            )
            .optional()
            .map_err(storage_error)?;
        let Some((mode, prompt, run_id, disposition, accepted_at_ms)) = existing else {
            return Ok(None);
        };
        if mode != input_mode_tag(&request.mode)? || prompt != request.native_prompt_json {
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
        }))
    }

    pub(crate) fn record_turn(
        &self,
        request: &ChannelTurn,
        accepted: &AcceptedTurn,
    ) -> Result<AcceptedTurn, NativeChannelError> {
        let connection = self.lock()?;
        connection
            .execute(
                "INSERT INTO turns (
                    binding_id, session_id, client_turn_id, mode, native_prompt_json,
                    run_id, disposition, state, received_at_ms, accepted_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    request.binding_id,
                    accepted.session_id,
                    request.client_turn_id,
                    input_mode_tag(&request.mode)?,
                    request.native_prompt_json,
                    accepted.run_id,
                    disposition_tag(&accepted.disposition)?,
                    if matches!(
                        accepted.disposition,
                        ChannelTurnDisposition::QueuedForTurnBoundary
                    ) {
                        "Pending"
                    } else {
                        "Active"
                    },
                    db_i64(request.received_at_ms)?,
                    db_i64(accepted.accepted_at_ms)?,
                ],
            )
            .map_err(storage_error)?;
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
        ChannelTurnDisposition::Unknown { .. } => Err(NativeChannelError::StorageUnavailable),
    }
}

fn parse_disposition(value: &str) -> Result<ChannelTurnDisposition, NativeChannelError> {
    match value {
        "StartedForegroundWork" => Ok(ChannelTurnDisposition::StartedForegroundWork),
        "ContributedToActiveWork" => Ok(ChannelTurnDisposition::ContributedToActiveWork),
        "QueuedForTurnBoundary" => Ok(ChannelTurnDisposition::QueuedForTurnBoundary),
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

    use super::ChannelStore;
    use crate::{BindChannelRequest, ChannelLifecycle, LocateBindingRequest, NativeChannelError};

    fn request() -> BindChannelRequest {
        BindChannelRequest {
            channel_id: "channel-1".into(),
            adapter_id: "native-ui".into(),
            session_id: "session-1".into(),
            native_channel_json: r#"{"title":"Jim"}"#.into(),
        }
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
}
