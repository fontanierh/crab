use std::{path::Path, sync::Mutex};

use rusqlite::{Connection, OptionalExtension, Row, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    ClaimTriggers, EnqueueTrigger, ExtendLease, SettleTrigger, SettlementOutcome,
    TriggerAttachment, TriggerBatch, TriggerInboxError, TriggerLease, TriggerMode, TriggerReceipt,
    TriggerRecord, TriggerReference, TriggerSource, TriggerState,
};

const SCHEMA_VERSION: i64 = 1;
const MAX_CLAIM_LIMIT: u64 = 1_024;

const RECORD_COLUMNS: &str = "
    sequence, trigger_id, source, source_id, deduplication_key, target_channel_id, lane, mode,
    state, enqueued_at_ms, initial_not_before_ms, not_before_ms, message_json, attachments_json,
    attempt, lease_token, worker_id, leased_at_ms, lease_expires_at_ms
";

pub(crate) struct TriggerStore {
    connection: Mutex<Connection>,
}

impl TriggerStore {
    pub(crate) fn open(path: impl AsRef<Path>) -> Result<Self, TriggerInboxError> {
        let connection = Connection::open(path).map_err(storage_error)?;
        Self::initialize(connection)
    }

    pub(crate) fn open_in_memory() -> Result<Self, TriggerInboxError> {
        let connection = Connection::open_in_memory().map_err(storage_error)?;
        Self::initialize(connection)
    }

    fn initialize(mut connection: Connection) -> Result<Self, TriggerInboxError> {
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
            _ => return Err(TriggerInboxError::StorageUnavailable),
        }

        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    pub(crate) fn enqueue(
        &self,
        request: EnqueueTrigger,
        enqueued_at_ms: u64,
    ) -> Result<TriggerReceipt, TriggerInboxError> {
        validate_enqueue(&request)?;
        let enqueued_at = input_i64(enqueued_at_ms, TriggerInboxError::InvalidPayload)?;
        let not_before = input_i64(request.not_before_ms, TriggerInboxError::InvalidPayload)?;
        let source = source_tag(&request.source)?;
        let mode = mode_tag(&request.mode)?;
        let attachments = encode_attachments(&request.attachments)?;

        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let existing = query_by_deduplication_key(
            &transaction,
            &request.source_id,
            &request.deduplication_key,
        )?;
        if let Some(existing) = existing {
            let matches = existing.matches_enqueue(&request)?;
            let record = existing.into_record()?;
            transaction.commit().map_err(storage_error)?;
            if !matches {
                return Err(TriggerInboxError::DuplicateKeyConflict);
            }
            return Ok(TriggerReceipt {
                trigger_id: record.trigger_id,
                state: record.state,
                deduplicated: true,
                recorded_at_ms: record.enqueued_at_ms,
            });
        }

        let trigger_id = format!("trigger_{}", Uuid::new_v4());
        transaction
            .execute(
                "INSERT INTO triggers (
                    trigger_id, source, source_id, deduplication_key, target_channel_id, lane, mode,
                    state, enqueued_at_ms, initial_not_before_ms, not_before_ms, message_json,
                    attachments_json, attempt
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'Pending', ?8, ?9, ?9, ?10, ?11, 0)",
                params![
                    trigger_id,
                    source,
                    request.source_id,
                    request.deduplication_key,
                    request.target_channel_id,
                    request.lane,
                    mode,
                    enqueued_at,
                    not_before,
                    request.message_json,
                    attachments,
                ],
            )
            .map_err(storage_error)?;
        transaction.commit().map_err(storage_error)?;

        Ok(TriggerReceipt {
            trigger_id,
            state: TriggerState::Pending,
            deduplicated: false,
            recorded_at_ms: enqueued_at_ms,
        })
    }

    pub(crate) fn claim(&self, request: ClaimTriggers) -> Result<TriggerBatch, TriggerInboxError> {
        validate_claim(&request)?;
        if request.limit == 0 {
            return Ok(TriggerBatch { leases: Vec::new() });
        }
        let now = input_i64(request.now_ms, TriggerInboxError::InvalidClaim)?;
        let expires_at_ms = request
            .now_ms
            .checked_add(request.lease_duration_ms)
            .ok_or(TriggerInboxError::InvalidClaim)?;
        let expires_at = input_i64(expires_at_ms, TriggerInboxError::InvalidClaim)?;

        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        transaction
            .execute(
                "UPDATE triggers
                 SET state = 'Pending', lease_token = NULL, worker_id = NULL,
                     leased_at_ms = NULL, lease_expires_at_ms = NULL
                 WHERE lane = ?1 AND state = 'Leased' AND lease_expires_at_ms <= ?2",
                params![request.lane, now],
            )
            .map_err(storage_error)?;

        let active_lease = transaction
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM triggers WHERE lane = ?1 AND state = 'Leased'
                 )",
                params![request.lane],
                |row| row.get::<_, bool>(0),
            )
            .map_err(storage_error)?;
        if active_lease {
            transaction.commit().map_err(storage_error)?;
            return Ok(TriggerBatch { leases: Vec::new() });
        }

        let candidates = {
            let sql = format!(
                "SELECT {RECORD_COLUMNS} FROM triggers
                 WHERE lane = ?1 AND state IN ('Pending', 'RetryScheduled')
                 ORDER BY sequence ASC LIMIT ?2"
            );
            let mut statement = transaction.prepare(&sql).map_err(storage_error)?;
            let rows = statement
                .query_map(
                    params![
                        request.lane,
                        i64::try_from(request.limit).unwrap_or(i64::MAX)
                    ],
                    StoredRecord::from_row,
                )
                .map_err(storage_error)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(storage_error)?
        };

        let mut leases = Vec::new();
        for candidate in candidates {
            let mut record = candidate.into_record()?;
            if record.not_before_ms > request.now_ms {
                break;
            }
            let attempt = record
                .attempt
                .checked_add(1)
                .ok_or(TriggerInboxError::StorageUnavailable)?;
            let attempt_db = input_i64(attempt, TriggerInboxError::StorageUnavailable)?;
            let lease_token = format!("lease_{}", Uuid::new_v4());
            let changed = transaction
                .execute(
                    "UPDATE triggers
                     SET state = 'Leased', attempt = ?2, lease_token = ?3, worker_id = ?4,
                         leased_at_ms = ?5, lease_expires_at_ms = ?6
                     WHERE trigger_id = ?1 AND state IN ('Pending', 'RetryScheduled')",
                    params![
                        record.trigger_id,
                        attempt_db,
                        lease_token,
                        request.worker_id,
                        now,
                        expires_at,
                    ],
                )
                .map_err(storage_error)?;
            if changed != 1 {
                return Err(TriggerInboxError::StorageUnavailable);
            }
            record.state = TriggerState::Leased;
            record.attempt = attempt;
            leases.push(TriggerLease {
                trigger: record,
                lease_token,
                worker_id: request.worker_id.clone(),
                leased_at_ms: request.now_ms,
                expires_at_ms,
            });
        }

        transaction.commit().map_err(storage_error)?;
        Ok(TriggerBatch { leases })
    }

    pub(crate) fn extend_lease(
        &self,
        request: ExtendLease,
    ) -> Result<TriggerLease, TriggerInboxError> {
        if request.trigger_id.trim().is_empty()
            || request.lease_token.trim().is_empty()
            || request.extend_by_ms == 0
        {
            return Err(TriggerInboxError::InvalidLease);
        }
        input_i64(request.now_ms, TriggerInboxError::InvalidLease)?;

        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let stored = query_by_trigger_id(&transaction, &request.trigger_id)?
            .ok_or(TriggerInboxError::UnknownTrigger)?;
        require_current_lease(&stored, &request.lease_token, request.now_ms)?;
        let current_expiry = stored
            .lease_expires_at_ms
            .ok_or(TriggerInboxError::StorageUnavailable)?;
        let extended_expiry = current_expiry
            .checked_add(input_i64(
                request.extend_by_ms,
                TriggerInboxError::InvalidLease,
            )?)
            .ok_or(TriggerInboxError::InvalidLease)?;
        transaction
            .execute(
                "UPDATE triggers SET lease_expires_at_ms = ?2 WHERE trigger_id = ?1",
                params![request.trigger_id, extended_expiry],
            )
            .map_err(storage_error)?;
        let worker_id = stored
            .worker_id
            .clone()
            .ok_or(TriggerInboxError::StorageUnavailable)?;
        let leased_at_ms = stored
            .leased_at_ms
            .ok_or(TriggerInboxError::StorageUnavailable)
            .and_then(db_u64)?;
        let record = stored.into_record()?;
        transaction.commit().map_err(storage_error)?;

        Ok(TriggerLease {
            trigger: record,
            lease_token: request.lease_token,
            worker_id,
            leased_at_ms,
            expires_at_ms: db_u64(extended_expiry)?,
        })
    }

    pub(crate) fn settle(
        &self,
        request: SettleTrigger,
    ) -> Result<TriggerReceipt, TriggerInboxError> {
        if request.trigger_id.trim().is_empty() || request.lease_token.trim().is_empty() {
            return Err(TriggerInboxError::InvalidSettlement);
        }
        let settled_at = input_i64(request.settled_at_ms, TriggerInboxError::InvalidSettlement)?;

        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let stored = query_by_trigger_id(&transaction, &request.trigger_id)?
            .ok_or(TriggerInboxError::UnknownTrigger)?;
        require_current_lease(&stored, &request.lease_token, request.settled_at_ms)?;

        let (state, retry_not_before) = match request.outcome {
            SettlementOutcome::Completed if request.retry_not_before_ms.is_none() => {
                (TriggerState::Completed, None)
            }
            SettlementOutcome::DeadLetter if request.retry_not_before_ms.is_none() => {
                (TriggerState::DeadLettered, None)
            }
            SettlementOutcome::Retry => {
                let retry_at = request
                    .retry_not_before_ms
                    .filter(|retry_at| *retry_at >= request.settled_at_ms)
                    .ok_or(TriggerInboxError::InvalidSettlement)?;
                (
                    TriggerState::RetryScheduled,
                    Some(input_i64(retry_at, TriggerInboxError::InvalidSettlement)?),
                )
            }
            SettlementOutcome::Unknown { .. }
            | SettlementOutcome::Completed
            | SettlementOutcome::DeadLetter => {
                return Err(TriggerInboxError::InvalidSettlement);
            }
        };
        let state_tag = state_tag(&state)?;
        transaction
            .execute(
                "UPDATE triggers
                 SET state = ?2, not_before_ms = COALESCE(?3, not_before_ms),
                     lease_token = NULL, worker_id = NULL, leased_at_ms = NULL,
                     lease_expires_at_ms = NULL, settlement_detail = ?4, settled_at_ms = ?5
                 WHERE trigger_id = ?1",
                params![
                    request.trigger_id,
                    state_tag,
                    retry_not_before,
                    request.detail,
                    settled_at,
                ],
            )
            .map_err(storage_error)?;
        transaction.commit().map_err(storage_error)?;

        Ok(TriggerReceipt {
            trigger_id: request.trigger_id,
            state,
            deduplicated: false,
            recorded_at_ms: request.settled_at_ms,
        })
    }

    pub(crate) fn inspect(
        &self,
        request: TriggerReference,
    ) -> Result<TriggerRecord, TriggerInboxError> {
        if request.trigger_id.trim().is_empty() {
            return Err(TriggerInboxError::UnknownTrigger);
        }
        let connection = self.lock()?;
        query_by_trigger_id(&connection, &request.trigger_id)?
            .ok_or(TriggerInboxError::UnknownTrigger)?
            .into_record()
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, TriggerInboxError> {
        self.connection
            .lock()
            .map_err(|_| TriggerInboxError::StorageUnavailable)
    }
}

fn migrate_v0_to_v1(connection: &mut Connection) -> Result<(), TriggerInboxError> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(storage_error)?;
    transaction
        .execute_batch(
            "CREATE TABLE triggers (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                trigger_id TEXT NOT NULL UNIQUE,
                source TEXT NOT NULL,
                source_id TEXT NOT NULL,
                deduplication_key TEXT NOT NULL,
                target_channel_id TEXT NOT NULL,
                lane TEXT NOT NULL,
                mode TEXT NOT NULL,
                state TEXT NOT NULL,
                enqueued_at_ms INTEGER NOT NULL,
                initial_not_before_ms INTEGER NOT NULL,
                not_before_ms INTEGER NOT NULL,
                message_json TEXT NOT NULL,
                attachments_json TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                lease_token TEXT,
                worker_id TEXT,
                leased_at_ms INTEGER,
                lease_expires_at_ms INTEGER,
                settlement_detail TEXT,
                settled_at_ms INTEGER,
                UNIQUE(source_id, deduplication_key)
             );
             CREATE INDEX trigger_lane_order ON triggers(lane, sequence);
             CREATE INDEX trigger_lease_expiry ON triggers(lane, state, lease_expires_at_ms);",
        )
        .map_err(storage_error)?;
    transaction
        .pragma_update(None, "user_version", SCHEMA_VERSION)
        .map_err(storage_error)?;
    transaction.commit().map_err(storage_error)
}

fn validate_enqueue(request: &EnqueueTrigger) -> Result<(), TriggerInboxError> {
    if request.source_id.trim().is_empty() || request.deduplication_key.trim().is_empty() {
        return Err(TriggerInboxError::InvalidSource);
    }
    source_tag(&request.source)?;
    if request.target_channel_id.trim().is_empty() {
        return Err(TriggerInboxError::InvalidTarget);
    }
    if request.lane.trim().is_empty() {
        return Err(TriggerInboxError::InvalidLane);
    }
    mode_tag(&request.mode)?;
    serde_json::from_str::<serde_json::Value>(&request.message_json)
        .map_err(|_| TriggerInboxError::InvalidPayload)?;
    if request.attachments.iter().any(|attachment| {
        attachment.media_type.trim().is_empty() || attachment.content_handle.trim().is_empty()
    }) {
        return Err(TriggerInboxError::InvalidPayload);
    }
    Ok(())
}

fn validate_claim(request: &ClaimTriggers) -> Result<(), TriggerInboxError> {
    if request.worker_id.trim().is_empty()
        || request.lane.trim().is_empty()
        || request.limit > MAX_CLAIM_LIMIT
        || request.lease_duration_ms == 0
    {
        return Err(TriggerInboxError::InvalidClaim);
    }
    Ok(())
}

fn query_by_deduplication_key(
    connection: &Connection,
    source_id: &str,
    deduplication_key: &str,
) -> Result<Option<StoredRecord>, TriggerInboxError> {
    let sql = format!(
        "SELECT {RECORD_COLUMNS} FROM triggers
         WHERE source_id = ?1 AND deduplication_key = ?2"
    );
    connection
        .query_row(
            &sql,
            params![source_id, deduplication_key],
            StoredRecord::from_row,
        )
        .optional()
        .map_err(storage_error)
}

fn query_by_trigger_id(
    connection: &Connection,
    trigger_id: &str,
) -> Result<Option<StoredRecord>, TriggerInboxError> {
    let sql = format!("SELECT {RECORD_COLUMNS} FROM triggers WHERE trigger_id = ?1");
    connection
        .query_row(&sql, params![trigger_id], StoredRecord::from_row)
        .optional()
        .map_err(storage_error)
}

fn require_current_lease(
    stored: &StoredRecord,
    lease_token: &str,
    at_ms: u64,
) -> Result<(), TriggerInboxError> {
    if stored.state != "Leased" || stored.lease_token.as_deref() != Some(lease_token) {
        return Err(TriggerInboxError::LeaseMismatch);
    }
    let expires_at = stored
        .lease_expires_at_ms
        .ok_or(TriggerInboxError::StorageUnavailable)
        .and_then(db_u64)?;
    if at_ms >= expires_at {
        return Err(TriggerInboxError::LeaseExpired);
    }
    Ok(())
}

#[derive(Debug)]
struct StoredRecord {
    sequence: i64,
    trigger_id: String,
    source: String,
    source_id: String,
    deduplication_key: String,
    target_channel_id: String,
    lane: String,
    mode: String,
    state: String,
    enqueued_at_ms: i64,
    initial_not_before_ms: i64,
    not_before_ms: i64,
    message_json: String,
    attachments_json: String,
    attempt: i64,
    lease_token: Option<String>,
    worker_id: Option<String>,
    leased_at_ms: Option<i64>,
    lease_expires_at_ms: Option<i64>,
}

impl StoredRecord {
    fn from_row(row: &Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            sequence: row.get(0)?,
            trigger_id: row.get(1)?,
            source: row.get(2)?,
            source_id: row.get(3)?,
            deduplication_key: row.get(4)?,
            target_channel_id: row.get(5)?,
            lane: row.get(6)?,
            mode: row.get(7)?,
            state: row.get(8)?,
            enqueued_at_ms: row.get(9)?,
            initial_not_before_ms: row.get(10)?,
            not_before_ms: row.get(11)?,
            message_json: row.get(12)?,
            attachments_json: row.get(13)?,
            attempt: row.get(14)?,
            lease_token: row.get(15)?,
            worker_id: row.get(16)?,
            leased_at_ms: row.get(17)?,
            lease_expires_at_ms: row.get(18)?,
        })
    }

    fn into_record(self) -> Result<TriggerRecord, TriggerInboxError> {
        let _ = self.sequence;
        Ok(TriggerRecord {
            trigger_id: self.trigger_id,
            source: parse_source(&self.source)?,
            source_id: self.source_id,
            deduplication_key: self.deduplication_key,
            target_channel_id: self.target_channel_id,
            lane: self.lane,
            mode: parse_mode(&self.mode)?,
            state: parse_state(&self.state)?,
            enqueued_at_ms: db_u64(self.enqueued_at_ms)?,
            not_before_ms: db_u64(self.not_before_ms)?,
            message_json: self.message_json,
            attachments: decode_attachments(&self.attachments_json)?,
            attempt: db_u64(self.attempt)?,
        })
    }

    fn matches_enqueue(&self, request: &EnqueueTrigger) -> Result<bool, TriggerInboxError> {
        Ok(parse_source(&self.source)? == request.source
            && self.source_id == request.source_id
            && self.deduplication_key == request.deduplication_key
            && self.target_channel_id == request.target_channel_id
            && self.lane == request.lane
            && parse_mode(&self.mode)? == request.mode
            && db_u64(self.initial_not_before_ms)? == request.not_before_ms
            && self.message_json == request.message_json
            && decode_attachments(&self.attachments_json)? == request.attachments)
    }
}

#[derive(Serialize, Deserialize)]
struct StoredAttachment {
    media_type: String,
    name: Option<String>,
    content_handle: String,
}

fn encode_attachments(attachments: &[TriggerAttachment]) -> Result<String, TriggerInboxError> {
    let stored = attachments
        .iter()
        .map(|attachment| StoredAttachment {
            media_type: attachment.media_type.clone(),
            name: attachment.name.clone(),
            content_handle: attachment.content_handle.clone(),
        })
        .collect::<Vec<_>>();
    serde_json::to_string(&stored).map_err(|_| TriggerInboxError::InvalidPayload)
}

fn decode_attachments(value: &str) -> Result<Vec<TriggerAttachment>, TriggerInboxError> {
    serde_json::from_str::<Vec<StoredAttachment>>(value)
        .map_err(|_| TriggerInboxError::StorageUnavailable)
        .map(|stored| {
            stored
                .into_iter()
                .map(|attachment| TriggerAttachment {
                    media_type: attachment.media_type,
                    name: attachment.name,
                    content_handle: attachment.content_handle,
                })
                .collect()
        })
}

fn source_tag(source: &TriggerSource) -> Result<&'static str, TriggerInboxError> {
    match source {
        TriggerSource::Bridge => Ok("Bridge"),
        TriggerSource::Scheduler => Ok("Scheduler"),
        TriggerSource::SelfWork => Ok("SelfWork"),
        TriggerSource::Operator => Ok("Operator"),
        TriggerSource::Unknown { .. } => Err(TriggerInboxError::InvalidSource),
    }
}

fn parse_source(value: &str) -> Result<TriggerSource, TriggerInboxError> {
    match value {
        "Bridge" => Ok(TriggerSource::Bridge),
        "Scheduler" => Ok(TriggerSource::Scheduler),
        "SelfWork" => Ok(TriggerSource::SelfWork),
        "Operator" => Ok(TriggerSource::Operator),
        _ => Err(TriggerInboxError::StorageUnavailable),
    }
}

fn mode_tag(mode: &TriggerMode) -> Result<&'static str, TriggerInboxError> {
    match mode {
        TriggerMode::Queue => Ok("Queue"),
        TriggerMode::Steer => Ok("Steer"),
        TriggerMode::InterruptAndSteer => Ok("InterruptAndSteer"),
        TriggerMode::Unknown { .. } => Err(TriggerInboxError::InvalidPayload),
    }
}

fn parse_mode(value: &str) -> Result<TriggerMode, TriggerInboxError> {
    match value {
        "Queue" => Ok(TriggerMode::Queue),
        "Steer" => Ok(TriggerMode::Steer),
        "InterruptAndSteer" => Ok(TriggerMode::InterruptAndSteer),
        _ => Err(TriggerInboxError::StorageUnavailable),
    }
}

fn state_tag(state: &TriggerState) -> Result<&'static str, TriggerInboxError> {
    match state {
        TriggerState::Pending => Ok("Pending"),
        TriggerState::Leased => Ok("Leased"),
        TriggerState::Completed => Ok("Completed"),
        TriggerState::RetryScheduled => Ok("RetryScheduled"),
        TriggerState::DeadLettered => Ok("DeadLettered"),
        TriggerState::Unknown { .. } => Err(TriggerInboxError::StorageUnavailable),
    }
}

fn parse_state(value: &str) -> Result<TriggerState, TriggerInboxError> {
    match value {
        "Pending" => Ok(TriggerState::Pending),
        "Leased" => Ok(TriggerState::Leased),
        "Completed" => Ok(TriggerState::Completed),
        "RetryScheduled" => Ok(TriggerState::RetryScheduled),
        "DeadLettered" => Ok(TriggerState::DeadLettered),
        _ => Err(TriggerInboxError::StorageUnavailable),
    }
}

fn input_i64(value: u64, error: TriggerInboxError) -> Result<i64, TriggerInboxError> {
    i64::try_from(value).map_err(|_| error)
}

fn db_u64(value: i64) -> Result<u64, TriggerInboxError> {
    u64::try_from(value).map_err(|_| TriggerInboxError::StorageUnavailable)
}

fn storage_error(_: rusqlite::Error) -> TriggerInboxError {
    TriggerInboxError::StorageUnavailable
}
