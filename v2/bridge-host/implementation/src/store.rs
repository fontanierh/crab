use std::{path::Path, sync::Mutex};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    AuthenticationChallenge, AuthenticationMethod, BridgeHostError, BridgeInbound,
    BridgeIngressMode, BridgeLifecycle, BridgeOutbound, BridgeReceipt, BridgeRecord, BridgeSpec,
    BridgeStatus, CredentialLifecycle, CredentialStatus, DeliveryLifecycle, DeliveryReceipt,
    HealthObservation, TriggerIntent,
};

const SCHEMA_VERSION: i64 = 1;

pub(crate) struct BridgeStore {
    connection: Mutex<Connection>,
}

impl BridgeStore {
    pub(crate) fn open(path: impl AsRef<Path>) -> Result<Self, BridgeHostError> {
        Self::initialize(Connection::open(path).map_err(storage_error)?)
    }

    pub(crate) fn open_in_memory() -> Result<Self, BridgeHostError> {
        Self::initialize(Connection::open_in_memory().map_err(storage_error)?)
    }

    fn initialize(mut connection: Connection) -> Result<Self, BridgeHostError> {
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
            _ => return Err(BridgeHostError::StorageUnavailable),
        }
        let transaction = connection.transaction().map_err(storage_error)?;
        transaction
            .execute(
                "UPDATE bridges SET lifecycle = 'Stopped',
                    last_error = 'runtime restarted; reconciliation required'
                 WHERE lifecycle IN (
                    'Starting', 'AwaitingAuthentication', 'Healthy', 'Degraded', 'BackingOff'
                 )",
                [],
            )
            .map_err(storage_error)?;
        transaction
            .execute("DELETE FROM health", [])
            .map_err(storage_error)?;
        transaction
            .execute(
                "UPDATE challenges SET state = 'Expired' WHERE state = 'Pending'",
                [],
            )
            .map_err(storage_error)?;
        transaction.commit().map_err(storage_error)?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    pub(crate) fn register(
        &self,
        spec: &BridgeSpec,
        now_ms: u64,
    ) -> Result<(BridgeRecord, bool), BridgeHostError> {
        let fingerprint = spec_fingerprint(spec)?;
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        if let Some(existing) = transaction
            .query_row(
                "SELECT spec_fingerprint FROM bridges WHERE bridge_id = ?1",
                params![spec.bridge_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(storage_error)?
        {
            if existing != fingerprint {
                return Err(BridgeHostError::DuplicateBridgeConflict);
            }
            transaction.commit().map_err(storage_error)?;
            drop(connection);
            return Ok((self.record(&spec.bridge_id)?, false));
        }
        let methods = encode_authentication_methods(&spec.authentication_methods)?;
        transaction
            .execute(
                "INSERT INTO bridges (
                    bridge_id, package_id, display_name, launch_json, configuration_json,
                    authentication_methods_json, ingress_mode, desired_running,
                    health_interval_ms, credential_validation_interval_ms, restart_limit,
                    restart_window_ms, lifecycle, generation, registered_at_ms,
                    consecutive_failures, next_restart_at_ms, last_error, spec_fingerprint
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
                           'Registered', 1, ?13, 0, NULL, NULL, ?14)",
                params![
                    spec.bridge_id,
                    spec.package_id,
                    spec.display_name,
                    spec.launch_json,
                    spec.configuration_json,
                    methods,
                    ingress_mode_tag(&spec.ingress_mode)?,
                    spec.desired_running,
                    db_i64(spec.health_interval_ms)?,
                    db_i64(spec.credential_validation_interval_ms)?,
                    db_i64(spec.restart_limit)?,
                    db_i64(spec.restart_window_ms)?,
                    db_i64(now_ms)?,
                    fingerprint,
                ],
            )
            .map_err(storage_error)?;
        transaction
            .execute(
                "INSERT INTO credentials (
                    bridge_id, lifecycle, credential_handle, validated_at_ms,
                    expires_at_ms, account_hint, detail_json
                 ) VALUES (?1, 'Missing', NULL, NULL, NULL, NULL, '{}')",
                params![spec.bridge_id],
            )
            .map_err(storage_error)?;
        transaction
            .execute(
                "INSERT INTO generation_audit (
                    bridge_id, generation, changed_at_ms, spec_fingerprint
                 ) VALUES (?1, 1, ?2, ?3)",
                params![spec.bridge_id, db_i64(now_ms)?, fingerprint],
            )
            .map_err(storage_error)?;
        transaction.commit().map_err(storage_error)?;
        drop(connection);
        Ok((self.record(&spec.bridge_id)?, true))
    }

    pub(crate) fn replace(
        &self,
        spec: &BridgeSpec,
        expected_generation: u64,
        now_ms: u64,
    ) -> Result<BridgeRecord, BridgeHostError> {
        let fingerprint = spec_fingerprint(spec)?;
        let methods = encode_authentication_methods(&spec.authentication_methods)?;
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let changed = transaction
            .execute(
                "UPDATE bridges SET
                    package_id = ?3, display_name = ?4, launch_json = ?5,
                    configuration_json = ?6, authentication_methods_json = ?7,
                    ingress_mode = ?8, desired_running = ?9, health_interval_ms = ?10,
                    credential_validation_interval_ms = ?11, restart_limit = ?12,
                    restart_window_ms = ?13, lifecycle = 'Registered', generation = generation + 1,
                    consecutive_failures = 0, next_restart_at_ms = NULL, last_error = NULL,
                    spec_fingerprint = ?14
                 WHERE bridge_id = ?1 AND generation = ?2",
                params![
                    spec.bridge_id,
                    db_i64(expected_generation)?,
                    spec.package_id,
                    spec.display_name,
                    spec.launch_json,
                    spec.configuration_json,
                    methods,
                    ingress_mode_tag(&spec.ingress_mode)?,
                    spec.desired_running,
                    db_i64(spec.health_interval_ms)?,
                    db_i64(spec.credential_validation_interval_ms)?,
                    db_i64(spec.restart_limit)?,
                    db_i64(spec.restart_window_ms)?,
                    fingerprint,
                ],
            )
            .map_err(storage_error)?;
        if changed != 1 {
            return Err(if bridge_exists(&transaction, &spec.bridge_id)? {
                BridgeHostError::GenerationConflict
            } else {
                BridgeHostError::UnknownBridge
            });
        }
        transaction
            .execute(
                "INSERT INTO generation_audit (
                    bridge_id, generation, changed_at_ms, spec_fingerprint
                 ) SELECT bridge_id, generation, ?2, spec_fingerprint
                   FROM bridges WHERE bridge_id = ?1",
                params![spec.bridge_id, db_i64(now_ms)?],
            )
            .map_err(storage_error)?;
        transaction
            .execute(
                "UPDATE challenges SET state = 'Superseded'
                 WHERE bridge_id = ?1 AND state = 'Pending'",
                params![spec.bridge_id],
            )
            .map_err(storage_error)?;
        transaction.commit().map_err(storage_error)?;
        drop(connection);
        self.record(&spec.bridge_id)
    }

    pub(crate) fn spec(&self, bridge_id: &str) -> Result<BridgeSpec, BridgeHostError> {
        let connection = self.lock()?;
        connection
            .query_row(
                "SELECT package_id, display_name, launch_json, configuration_json,
                        authentication_methods_json, ingress_mode, desired_running,
                        health_interval_ms, credential_validation_interval_ms,
                        restart_limit, restart_window_ms
                 FROM bridges WHERE bridge_id = ?1",
                params![bridge_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, bool>(6)?,
                        row.get::<_, i64>(7)?,
                        row.get::<_, i64>(8)?,
                        row.get::<_, i64>(9)?,
                        row.get::<_, i64>(10)?,
                    ))
                },
            )
            .optional()
            .map_err(storage_error)?
            .ok_or(BridgeHostError::UnknownBridge)
            .and_then(
                |(
                    package_id,
                    display_name,
                    launch_json,
                    configuration_json,
                    methods,
                    ingress_mode,
                    desired_running,
                    health_interval,
                    validation_interval,
                    restart_limit,
                    restart_window,
                )| {
                    Ok(BridgeSpec {
                        bridge_id: bridge_id.to_owned(),
                        package_id,
                        display_name,
                        launch_json,
                        configuration_json,
                        authentication_methods: decode_authentication_methods(&methods)?,
                        ingress_mode: parse_ingress_mode(&ingress_mode)?,
                        desired_running,
                        health_interval_ms: db_u64(health_interval)?,
                        credential_validation_interval_ms: db_u64(validation_interval)?,
                        restart_limit: db_u64(restart_limit)?,
                        restart_window_ms: db_u64(restart_window)?,
                    })
                },
            )
    }

    pub(crate) fn desired_bridge_ids(&self) -> Result<Vec<String>, BridgeHostError> {
        let connection = self.lock()?;
        let mut statement = connection
            .prepare(
                "SELECT bridge_id FROM bridges WHERE desired_running = TRUE ORDER BY bridge_id",
            )
            .map_err(storage_error)?;
        statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(storage_error)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(storage_error)
    }

    pub(crate) fn record(&self, bridge_id: &str) -> Result<BridgeRecord, BridgeHostError> {
        let connection = self.lock()?;
        connection
            .query_row(
                "SELECT package_id, display_name, lifecycle, ingress_mode, desired_running,
                        generation, registered_at_ms
                 FROM bridges WHERE bridge_id = ?1",
                params![bridge_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, bool>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, i64>(6)?,
                    ))
                },
            )
            .optional()
            .map_err(storage_error)?
            .ok_or(BridgeHostError::UnknownBridge)
            .and_then(
                |(package_id, display_name, lifecycle, mode, desired, generation, registered)| {
                    Ok(BridgeRecord {
                        bridge_id: bridge_id.to_owned(),
                        package_id,
                        display_name,
                        lifecycle: parse_lifecycle(&lifecycle)?,
                        ingress_mode: parse_ingress_mode(&mode)?,
                        desired_running: desired,
                        generation: db_u64(generation)?,
                        registered_at_ms: db_u64(registered)?,
                    })
                },
            )
    }

    pub(crate) fn status(
        &self,
        bridge_id: &str,
        now_ms: u64,
    ) -> Result<BridgeStatus, BridgeHostError> {
        let connection = self.lock()?;
        let (lifecycle, generation, failures, next_restart, last_error, restart_window_ms) =
            connection
                .query_row(
                    "SELECT lifecycle, generation, consecutive_failures, next_restart_at_ms,
                            last_error, restart_window_ms
                     FROM bridges WHERE bridge_id = ?1",
                    params![bridge_id],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, Option<i64>>(3)?,
                            row.get::<_, Option<String>>(4)?,
                            row.get::<_, i64>(5)?,
                        ))
                    },
                )
                .optional()
                .map_err(storage_error)?
                .ok_or(BridgeHostError::UnknownBridge)?;
        let last_health = query_health(&connection, bridge_id)?;
        let since = now_ms.saturating_sub(db_u64(restart_window_ms)?);
        let restart_count = connection
            .query_row(
                "SELECT COUNT(*) FROM restart_events
                 WHERE bridge_id = ?1 AND attempted_at_ms >= ?2",
                params![bridge_id, db_i64(since)?],
                |row| row.get::<_, i64>(0),
            )
            .map_err(storage_error)?;
        Ok(BridgeStatus {
            bridge_id: bridge_id.to_owned(),
            lifecycle: parse_lifecycle(&lifecycle)?,
            generation: db_u64(generation)?,
            consecutive_failures: db_u64(failures)?,
            restart_count_in_window: db_u64(restart_count)?,
            next_restart_at_ms: next_restart.map(db_u64).transpose()?,
            last_health,
            last_error,
        })
    }

    pub(crate) fn set_desired(
        &self,
        bridge_id: &str,
        expected_generation: u64,
        desired_running: bool,
        now_ms: u64,
    ) -> Result<BridgeRecord, BridgeHostError> {
        let mut spec = self.spec(bridge_id)?;
        spec.desired_running = desired_running;
        let fingerprint = spec_fingerprint(&spec)?;
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let previous_desired = transaction
            .query_row(
                "SELECT desired_running FROM bridges
                 WHERE bridge_id = ?1 AND generation = ?2",
                params![bridge_id, db_i64(expected_generation)?],
                |row| row.get::<_, bool>(0),
            )
            .optional()
            .map_err(storage_error)?;
        let Some(previous_desired) = previous_desired else {
            return Err(if bridge_exists(&transaction, bridge_id)? {
                BridgeHostError::GenerationConflict
            } else {
                BridgeHostError::UnknownBridge
            });
        };
        let changed = transaction
            .execute(
                "UPDATE bridges SET desired_running = ?3,
                    generation = CASE WHEN desired_running = ?3 THEN generation ELSE generation + 1 END,
                    spec_fingerprint = ?4
                 WHERE bridge_id = ?1 AND generation = ?2",
                params![
                    bridge_id,
                    db_i64(expected_generation)?,
                    desired_running,
                    fingerprint
                ],
            )
            .map_err(storage_error)?;
        if changed != 1 {
            return Err(if bridge_exists(&transaction, bridge_id)? {
                BridgeHostError::GenerationConflict
            } else {
                BridgeHostError::UnknownBridge
            });
        }
        if previous_desired != desired_running {
            transaction
                .execute(
                    "INSERT INTO generation_audit (
                        bridge_id, generation, changed_at_ms, spec_fingerprint
                     ) SELECT bridge_id, generation, ?2, spec_fingerprint
                       FROM bridges WHERE bridge_id = ?1",
                    params![bridge_id, db_i64(now_ms)?],
                )
                .map_err(storage_error)?;
        }
        transaction.commit().map_err(storage_error)?;
        drop(connection);
        self.record(bridge_id)
    }

    pub(crate) fn set_lifecycle(
        &self,
        bridge_id: &str,
        lifecycle: &BridgeLifecycle,
        last_error: Option<&str>,
        next_restart_at_ms: Option<u64>,
    ) -> Result<(), BridgeHostError> {
        let connection = self.lock()?;
        let changed = connection
            .execute(
                "UPDATE bridges SET lifecycle = ?2, last_error = ?3, next_restart_at_ms = ?4
                 WHERE bridge_id = ?1",
                params![
                    bridge_id,
                    lifecycle_tag(lifecycle)?,
                    last_error,
                    next_restart_at_ms.map(db_i64).transpose()?,
                ],
            )
            .map_err(storage_error)?;
        if changed != 1 {
            return Err(BridgeHostError::UnknownBridge);
        }
        Ok(())
    }

    pub(crate) fn set_backoff(
        &self,
        bridge_id: &str,
        next_restart_at_ms: u64,
    ) -> Result<(), BridgeHostError> {
        let connection = self.lock()?;
        let changed = connection
            .execute(
                "UPDATE bridges SET lifecycle = 'BackingOff',
                    consecutive_failures = consecutive_failures + 1,
                    last_error = 'package launch or health probe failed',
                    next_restart_at_ms = ?2 WHERE bridge_id = ?1",
                params![bridge_id, db_i64(next_restart_at_ms)?],
            )
            .map_err(storage_error)?;
        if changed != 1 {
            return Err(BridgeHostError::UnknownBridge);
        }
        Ok(())
    }

    pub(crate) fn record_start_attempt(
        &self,
        spec: &BridgeSpec,
        now_ms: u64,
    ) -> Result<u64, BridgeHostError> {
        let window_start = now_ms.saturating_sub(spec.restart_window_ms);
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        transaction
            .execute(
                "DELETE FROM restart_events WHERE bridge_id = ?1 AND attempted_at_ms < ?2",
                params![spec.bridge_id, db_i64(window_start)?],
            )
            .map_err(storage_error)?;
        let count = transaction
            .query_row(
                "SELECT COUNT(*) FROM restart_events WHERE bridge_id = ?1",
                params![spec.bridge_id],
                |row| row.get::<_, i64>(0),
            )
            .map_err(storage_error)?;
        if db_u64(count)? >= spec.restart_limit {
            return Err(BridgeHostError::RestartBudgetExhausted);
        }
        transaction
            .execute(
                "INSERT INTO restart_events (bridge_id, event_id, attempted_at_ms)
                 VALUES (?1, ?2, ?3)",
                params![spec.bridge_id, Uuid::new_v4().to_string(), db_i64(now_ms)?,],
            )
            .map_err(storage_error)?;
        transaction.commit().map_err(storage_error)?;
        db_u64(count + 1)
    }

    pub(crate) fn report_health(
        &self,
        observation: &HealthObservation,
    ) -> Result<BridgeStatus, BridgeHostError> {
        validate_health(observation)?;
        let healthy = observation.process_alive
            && observation.service_connected
            && observation.can_receive
            && observation.can_send
            && matches!(observation.credential_lifecycle, CredentialLifecycle::Valid);
        let lifecycle = if healthy {
            BridgeLifecycle::Healthy
        } else if observation.process_alive {
            BridgeLifecycle::Degraded
        } else {
            BridgeLifecycle::BackingOff
        };
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        if !bridge_exists(&transaction, &observation.bridge_id)? {
            return Err(BridgeHostError::UnknownBridge);
        }
        transaction
            .execute(
                "INSERT INTO health (
                    bridge_id, observed_at_ms, process_alive, service_connected,
                    can_receive, can_send, credential_lifecycle, detail_json
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT(bridge_id) DO UPDATE SET
                    observed_at_ms = excluded.observed_at_ms,
                    process_alive = excluded.process_alive,
                    service_connected = excluded.service_connected,
                    can_receive = excluded.can_receive,
                    can_send = excluded.can_send,
                    credential_lifecycle = excluded.credential_lifecycle,
                    detail_json = excluded.detail_json",
                params![
                    observation.bridge_id,
                    db_i64(observation.observed_at_ms)?,
                    observation.process_alive,
                    observation.service_connected,
                    observation.can_receive,
                    observation.can_send,
                    credential_lifecycle_tag(&observation.credential_lifecycle)?,
                    observation.detail_json,
                ],
            )
            .map_err(storage_error)?;
        transaction
            .execute(
                "UPDATE bridges SET lifecycle = ?2,
                    consecutive_failures = CASE WHEN ?4 THEN 0 ELSE consecutive_failures END,
                    last_error = CASE WHEN ?3 THEN NULL ELSE 'active health probe failed' END
                 WHERE bridge_id = ?1",
                params![
                    observation.bridge_id,
                    lifecycle_tag(&lifecycle)?,
                    healthy,
                    observation.process_alive
                ],
            )
            .map_err(storage_error)?;
        transaction.commit().map_err(storage_error)?;
        drop(connection);
        self.status(&observation.bridge_id, observation.observed_at_ms)
    }

    pub(crate) fn create_challenge(
        &self,
        bridge_id: &str,
        challenge: &crate::PackageChallenge,
        now_ms: u64,
    ) -> Result<AuthenticationChallenge, BridgeHostError> {
        parse_json(&challenge.presentation_json)?;
        let challenge_id = format!("challenge_{}", Uuid::new_v4());
        let mut connection = self.lock()?;
        let transaction = connection.transaction().map_err(storage_error)?;
        transaction
            .execute(
                "INSERT INTO challenges (
                    bridge_id, challenge_id, method, expires_at_ms, presentation_json,
                    state, created_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, 'Pending', ?6)",
                params![
                    bridge_id,
                    challenge_id,
                    authentication_method_tag(&challenge.method)?,
                    challenge.expires_at_ms.map(db_i64).transpose()?,
                    "{}",
                    db_i64(now_ms)?,
                ],
            )
            .map_err(storage_error)?;
        transaction
            .execute(
                "UPDATE credentials SET lifecycle = 'Challenged' WHERE bridge_id = ?1",
                params![bridge_id],
            )
            .map_err(storage_error)?;
        transaction.commit().map_err(storage_error)?;
        Ok(AuthenticationChallenge {
            bridge_id: bridge_id.to_owned(),
            challenge_id,
            method: challenge.method.clone(),
            expires_at_ms: challenge.expires_at_ms,
            presentation_json: challenge.presentation_json.clone(),
        })
    }

    pub(crate) fn verify_challenge(
        &self,
        bridge_id: &str,
        challenge_id: &str,
        now_ms: u64,
    ) -> Result<(), BridgeHostError> {
        let connection = self.lock()?;
        let expires = connection
            .query_row(
                "SELECT expires_at_ms FROM challenges
                 WHERE bridge_id = ?1 AND challenge_id = ?2 AND state = 'Pending'",
                params![bridge_id, challenge_id],
                |row| row.get::<_, Option<i64>>(0),
            )
            .optional()
            .map_err(storage_error)?
            .ok_or(BridgeHostError::AuthenticationUnavailable)?;
        if expires.is_some_and(|expires| db_u64(expires).is_ok_and(|expires| expires <= now_ms)) {
            return Err(BridgeHostError::ChallengeExpired);
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn set_credential(
        &self,
        bridge_id: &str,
        challenge_id: &str,
        handle: &str,
        validated_at_ms: u64,
        expires_at_ms: Option<u64>,
        account_hint: Option<&str>,
        detail_json: &str,
    ) -> Result<CredentialStatus, BridgeHostError> {
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let changed = transaction
            .execute(
                "UPDATE challenges SET state = 'Completed'
                 WHERE bridge_id = ?1 AND challenge_id = ?2 AND state = 'Pending'",
                params![bridge_id, challenge_id],
            )
            .map_err(storage_error)?;
        if changed != 1 {
            return Err(BridgeHostError::AuthenticationUnavailable);
        }
        transaction
            .execute(
                "UPDATE credentials SET lifecycle = 'Valid', credential_handle = ?2,
                    validated_at_ms = ?3, expires_at_ms = ?4, account_hint = ?5, detail_json = ?6
                 WHERE bridge_id = ?1",
                params![
                    bridge_id,
                    handle,
                    db_i64(validated_at_ms)?,
                    expires_at_ms.map(db_i64).transpose()?,
                    account_hint,
                    detail_json,
                ],
            )
            .map_err(storage_error)?;
        transaction.commit().map_err(storage_error)?;
        drop(connection);
        self.credential(bridge_id)
    }

    pub(crate) fn update_validation(
        &self,
        bridge_id: &str,
        lifecycle: &CredentialLifecycle,
        validated_at_ms: u64,
        expires_at_ms: Option<u64>,
        account_hint: Option<&str>,
        detail_json: &str,
    ) -> Result<CredentialStatus, BridgeHostError> {
        let connection = self.lock()?;
        let changed = connection
            .execute(
                "UPDATE credentials SET lifecycle = ?2, validated_at_ms = ?3,
                    expires_at_ms = ?4, account_hint = ?5, detail_json = ?6
                 WHERE bridge_id = ?1",
                params![
                    bridge_id,
                    credential_lifecycle_tag(lifecycle)?,
                    db_i64(validated_at_ms)?,
                    expires_at_ms.map(db_i64).transpose()?,
                    account_hint,
                    detail_json,
                ],
            )
            .map_err(storage_error)?;
        if changed != 1 {
            return Err(BridgeHostError::UnknownBridge);
        }
        drop(connection);
        self.credential(bridge_id)
    }

    pub(crate) fn clear_credential_reference(
        &self,
        bridge_id: &str,
        lifecycle: &CredentialLifecycle,
        observed_at_ms: u64,
    ) -> Result<(), BridgeHostError> {
        let connection = self.lock()?;
        let changed = connection
            .execute(
                "UPDATE credentials SET lifecycle = ?2, credential_handle = NULL,
                    validated_at_ms = ?3, expires_at_ms = NULL, account_hint = NULL,
                    detail_json = '{}'
                 WHERE bridge_id = ?1",
                params![
                    bridge_id,
                    credential_lifecycle_tag(lifecycle)?,
                    db_i64(observed_at_ms)?
                ],
            )
            .map_err(storage_error)?;
        if changed != 1 {
            return Err(BridgeHostError::UnknownBridge);
        }
        Ok(())
    }

    pub(crate) fn credential(&self, bridge_id: &str) -> Result<CredentialStatus, BridgeHostError> {
        let connection = self.lock()?;
        query_credential(&connection, bridge_id)?.ok_or(BridgeHostError::UnknownBridge)
    }

    pub(crate) fn revoke_credential(
        &self,
        bridge_id: &str,
        now_ms: u64,
    ) -> Result<BridgeReceipt, BridgeHostError> {
        let connection = self.lock()?;
        let changed = connection
            .execute(
                "UPDATE credentials SET lifecycle = 'Revoked', credential_handle = NULL,
                    expires_at_ms = NULL, validated_at_ms = ?2, account_hint = NULL,
                    detail_json = '{}'
                 WHERE bridge_id = ?1",
                params![bridge_id, db_i64(now_ms)?],
            )
            .map_err(storage_error)?;
        if changed != 1 {
            return Err(BridgeHostError::UnknownBridge);
        }
        Ok(BridgeReceipt {
            accepted: true,
            recorded_at_ms: now_ms,
        })
    }

    pub(crate) fn record_inbound(
        &self,
        request: &BridgeInbound,
        intent: &TriggerIntent,
    ) -> Result<TriggerIntent, BridgeHostError> {
        let fingerprint = inbound_fingerprint(request)?;
        let connection = self.lock()?;
        let existing = connection
            .query_row(
                "SELECT fingerprint, source_id, target_channel_id, ingress_mode,
                        message_json, attachment_handles_json, trigger_id,
                        deduplicated, recorded_at_ms
                 FROM inbound_events WHERE bridge_id = ?1 AND external_event_id = ?2",
                params![request.bridge_id, request.external_event_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, bool>(7)?,
                        row.get::<_, i64>(8)?,
                    ))
                },
            )
            .optional()
            .map_err(storage_error)?;
        if let Some((
            existing_fingerprint,
            source_id,
            target,
            mode,
            message,
            handles,
            trigger_id,
            deduplicated,
            recorded,
        )) = existing
        {
            if existing_fingerprint != fingerprint {
                return Err(BridgeHostError::DuplicateMessageConflict);
            }
            return Ok(TriggerIntent {
                source_id,
                deduplication_key: request.external_event_id.clone(),
                target_channel_id: target,
                ingress_mode: parse_ingress_mode(&mode)?,
                message_json: message,
                attachment_handles: decode_string_list(&handles)?,
                trigger_id,
                deduplicated,
                recorded_at_ms: db_u64(recorded)?,
            });
        }
        connection
            .execute(
                "INSERT INTO inbound_events (
                    bridge_id, external_event_id, fingerprint, source_id, target_channel_id,
                    ingress_mode, message_json, attachment_handles_json, trigger_id,
                    deduplicated, recorded_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    request.bridge_id,
                    request.external_event_id,
                    fingerprint,
                    intent.source_id,
                    intent.target_channel_id,
                    ingress_mode_tag(&intent.ingress_mode)?,
                    intent.message_json,
                    serde_json::to_string(&intent.attachment_handles)
                        .map_err(|_| BridgeHostError::StorageUnavailable)?,
                    intent.trigger_id,
                    intent.deduplicated,
                    db_i64(intent.recorded_at_ms)?,
                ],
            )
            .map_err(storage_error)?;
        Ok(intent.clone())
    }

    pub(crate) fn begin_delivery(
        &self,
        request: &BridgeOutbound,
        now_ms: u64,
    ) -> Result<(DeliveryReceipt, bool), BridgeHostError> {
        let fingerprint = outbound_fingerprint(request)?;
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        if let Some((existing_fingerprint, receipt)) =
            query_delivery(&transaction, &request.bridge_id, &request.message_id)?
        {
            if existing_fingerprint != fingerprint {
                return Err(BridgeHostError::DuplicateMessageConflict);
            }
            if matches!(receipt.lifecycle, DeliveryLifecycle::Delivered) {
                transaction.commit().map_err(storage_error)?;
                return Ok((receipt, false));
            }
            transaction
                .execute(
                    "UPDATE deliveries SET lifecycle = 'Sending', attempt = attempt + 1,
                        updated_at_ms = ?3
                     WHERE bridge_id = ?1 AND message_id = ?2",
                    params![request.bridge_id, request.message_id, db_i64(now_ms)?],
                )
                .map_err(storage_error)?;
            transaction.commit().map_err(storage_error)?;
            drop(connection);
            return Ok((
                self.delivery(&request.bridge_id, &request.message_id)?,
                true,
            ));
        }
        transaction
            .execute(
                "INSERT INTO deliveries (
                    bridge_id, message_id, fingerprint, lifecycle, external_delivery_id,
                    attempt, updated_at_ms, detail_json
                 ) VALUES (?1, ?2, ?3, 'Sending', NULL, 1, ?4, '{}')",
                params![
                    request.bridge_id,
                    request.message_id,
                    fingerprint,
                    db_i64(now_ms)?,
                ],
            )
            .map_err(storage_error)?;
        transaction.commit().map_err(storage_error)?;
        drop(connection);
        Ok((
            self.delivery(&request.bridge_id, &request.message_id)?,
            true,
        ))
    }

    pub(crate) fn complete_delivery(
        &self,
        bridge_id: &str,
        message_id: &str,
        external_delivery_id: &str,
        detail_json: &str,
        now_ms: u64,
    ) -> Result<DeliveryReceipt, BridgeHostError> {
        self.set_delivery_state(
            bridge_id,
            message_id,
            "Delivered",
            Some(external_delivery_id),
            detail_json,
            now_ms,
        )
    }

    pub(crate) fn fail_delivery(
        &self,
        bridge_id: &str,
        message_id: &str,
        now_ms: u64,
    ) -> Result<(), BridgeHostError> {
        self.set_delivery_state(bridge_id, message_id, "Retrying", None, "{}", now_ms)?;
        Ok(())
    }

    pub(crate) fn delivery(
        &self,
        bridge_id: &str,
        message_id: &str,
    ) -> Result<DeliveryReceipt, BridgeHostError> {
        let connection = self.lock()?;
        query_delivery(&connection, bridge_id, message_id)?
            .map(|(_, receipt)| receipt)
            .ok_or(BridgeHostError::UnknownDelivery)
    }

    fn set_delivery_state(
        &self,
        bridge_id: &str,
        message_id: &str,
        lifecycle: &str,
        external_delivery_id: Option<&str>,
        detail_json: &str,
        now_ms: u64,
    ) -> Result<DeliveryReceipt, BridgeHostError> {
        let connection = self.lock()?;
        let changed = connection
            .execute(
                "UPDATE deliveries SET lifecycle = ?3, external_delivery_id = ?4,
                    detail_json = ?5, updated_at_ms = ?6
                 WHERE bridge_id = ?1 AND message_id = ?2",
                params![
                    bridge_id,
                    message_id,
                    lifecycle,
                    external_delivery_id,
                    detail_json,
                    db_i64(now_ms)?,
                ],
            )
            .map_err(storage_error)?;
        if changed != 1 {
            return Err(BridgeHostError::UnknownDelivery);
        }
        drop(connection);
        self.delivery(bridge_id, message_id)
    }

    pub(crate) fn stop(
        &self,
        bridge_id: &str,
        now_ms: u64,
    ) -> Result<BridgeReceipt, BridgeHostError> {
        let mut spec = self.spec(bridge_id)?;
        spec.desired_running = false;
        let fingerprint = spec_fingerprint(&spec)?;
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let previous_desired = transaction
            .query_row(
                "SELECT desired_running FROM bridges WHERE bridge_id = ?1",
                params![bridge_id],
                |row| row.get::<_, bool>(0),
            )
            .optional()
            .map_err(storage_error)?
            .ok_or(BridgeHostError::UnknownBridge)?;
        let changed = transaction
            .execute(
                "UPDATE bridges SET lifecycle = 'Stopped', desired_running = FALSE,
                    generation = CASE WHEN desired_running THEN generation + 1 ELSE generation END,
                    next_restart_at_ms = NULL, last_error = NULL, spec_fingerprint = ?2
                 WHERE bridge_id = ?1",
                params![bridge_id, fingerprint],
            )
            .map_err(storage_error)?;
        if changed != 1 {
            return Err(BridgeHostError::UnknownBridge);
        }
        if previous_desired {
            transaction
                .execute(
                    "INSERT INTO generation_audit (
                        bridge_id, generation, changed_at_ms, spec_fingerprint
                     ) SELECT bridge_id, generation, ?2, spec_fingerprint
                       FROM bridges WHERE bridge_id = ?1",
                    params![bridge_id, db_i64(now_ms)?],
                )
                .map_err(storage_error)?;
        }
        transaction
            .execute(
                "UPDATE challenges SET state = 'Cancelled'
                 WHERE bridge_id = ?1 AND state = 'Pending'",
                params![bridge_id],
            )
            .map_err(storage_error)?;
        transaction.commit().map_err(storage_error)?;
        Ok(BridgeReceipt {
            accepted: true,
            recorded_at_ms: now_ms,
        })
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, BridgeHostError> {
        self.connection
            .lock()
            .map_err(|_| BridgeHostError::StorageUnavailable)
    }
}

fn migrate_v0_to_v1(connection: &mut Connection) -> Result<(), BridgeHostError> {
    let transaction = connection.transaction().map_err(storage_error)?;
    transaction
        .execute_batch(
            "CREATE TABLE bridges (
                bridge_id TEXT PRIMARY KEY,
                package_id TEXT NOT NULL,
                display_name TEXT NOT NULL,
                launch_json TEXT NOT NULL,
                configuration_json TEXT NOT NULL,
                authentication_methods_json TEXT NOT NULL,
                ingress_mode TEXT NOT NULL,
                desired_running INTEGER NOT NULL,
                health_interval_ms INTEGER NOT NULL,
                credential_validation_interval_ms INTEGER NOT NULL,
                restart_limit INTEGER NOT NULL,
                restart_window_ms INTEGER NOT NULL,
                lifecycle TEXT NOT NULL,
                generation INTEGER NOT NULL,
                registered_at_ms INTEGER NOT NULL,
                consecutive_failures INTEGER NOT NULL,
                next_restart_at_ms INTEGER,
                last_error TEXT,
                spec_fingerprint TEXT NOT NULL
             );
             CREATE TABLE generation_audit (
                bridge_id TEXT NOT NULL REFERENCES bridges(bridge_id),
                generation INTEGER NOT NULL,
                changed_at_ms INTEGER NOT NULL,
                spec_fingerprint TEXT NOT NULL,
                PRIMARY KEY(bridge_id, generation)
             );
             CREATE TABLE restart_events (
                bridge_id TEXT NOT NULL REFERENCES bridges(bridge_id),
                event_id TEXT NOT NULL,
                attempted_at_ms INTEGER NOT NULL,
                PRIMARY KEY(bridge_id, event_id)
             );
             CREATE TABLE health (
                bridge_id TEXT PRIMARY KEY REFERENCES bridges(bridge_id),
                observed_at_ms INTEGER NOT NULL,
                process_alive INTEGER NOT NULL,
                service_connected INTEGER NOT NULL,
                can_receive INTEGER NOT NULL,
                can_send INTEGER NOT NULL,
                credential_lifecycle TEXT NOT NULL,
                detail_json TEXT NOT NULL
             );
             CREATE TABLE credentials (
                bridge_id TEXT PRIMARY KEY REFERENCES bridges(bridge_id),
                lifecycle TEXT NOT NULL,
                credential_handle TEXT,
                validated_at_ms INTEGER,
                expires_at_ms INTEGER,
                account_hint TEXT,
                detail_json TEXT NOT NULL
             );
             CREATE TABLE challenges (
                bridge_id TEXT NOT NULL REFERENCES bridges(bridge_id),
                challenge_id TEXT NOT NULL,
                method TEXT NOT NULL,
                expires_at_ms INTEGER,
                presentation_json TEXT NOT NULL,
                state TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                PRIMARY KEY(bridge_id, challenge_id)
             );
             CREATE TABLE inbound_events (
                bridge_id TEXT NOT NULL REFERENCES bridges(bridge_id),
                external_event_id TEXT NOT NULL,
                fingerprint TEXT NOT NULL,
                source_id TEXT NOT NULL,
                target_channel_id TEXT NOT NULL,
                ingress_mode TEXT NOT NULL,
                message_json TEXT NOT NULL,
                attachment_handles_json TEXT NOT NULL,
                trigger_id TEXT NOT NULL,
                deduplicated INTEGER NOT NULL,
                recorded_at_ms INTEGER NOT NULL,
                PRIMARY KEY(bridge_id, external_event_id)
             );
             CREATE TABLE deliveries (
                bridge_id TEXT NOT NULL REFERENCES bridges(bridge_id),
                message_id TEXT NOT NULL,
                fingerprint TEXT NOT NULL,
                lifecycle TEXT NOT NULL,
                external_delivery_id TEXT,
                attempt INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                detail_json TEXT NOT NULL,
                PRIMARY KEY(bridge_id, message_id)
             );
             PRAGMA user_version = 1;",
        )
        .map_err(storage_error)?;
    transaction.commit().map_err(storage_error)
}

fn bridge_exists(connection: &Connection, bridge_id: &str) -> Result<bool, BridgeHostError> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM bridges WHERE bridge_id = ?1)",
            params![bridge_id],
            |row| row.get(0),
        )
        .map_err(storage_error)
}

fn query_health(
    connection: &Connection,
    bridge_id: &str,
) -> Result<Option<HealthObservation>, BridgeHostError> {
    connection
        .query_row(
            "SELECT observed_at_ms, process_alive, service_connected, can_receive,
                    can_send, credential_lifecycle, detail_json
             FROM health WHERE bridge_id = ?1",
            params![bridge_id],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, bool>(1)?,
                    row.get::<_, bool>(2)?,
                    row.get::<_, bool>(3)?,
                    row.get::<_, bool>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                ))
            },
        )
        .optional()
        .map_err(storage_error)?
        .map(
            |(observed, alive, connected, receive, send, credential, detail)| {
                Ok(HealthObservation {
                    bridge_id: bridge_id.to_owned(),
                    observed_at_ms: db_u64(observed)?,
                    process_alive: alive,
                    service_connected: connected,
                    can_receive: receive,
                    can_send: send,
                    credential_lifecycle: parse_credential_lifecycle(&credential)?,
                    detail_json: detail,
                })
            },
        )
        .transpose()
}

fn query_credential(
    connection: &Connection,
    bridge_id: &str,
) -> Result<Option<CredentialStatus>, BridgeHostError> {
    connection
        .query_row(
            "SELECT lifecycle, credential_handle, validated_at_ms, expires_at_ms,
                    account_hint, detail_json FROM credentials WHERE bridge_id = ?1",
            params![bridge_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, String>(5)?,
                ))
            },
        )
        .optional()
        .map_err(storage_error)?
        .map(|(lifecycle, handle, validated, expires, account, detail)| {
            Ok(CredentialStatus {
                bridge_id: bridge_id.to_owned(),
                lifecycle: parse_credential_lifecycle(&lifecycle)?,
                credential_handle: handle,
                validated_at_ms: validated.map(db_u64).transpose()?,
                expires_at_ms: expires.map(db_u64).transpose()?,
                account_hint: account,
                detail_json: detail,
            })
        })
        .transpose()
}

fn query_delivery(
    connection: &Connection,
    bridge_id: &str,
    message_id: &str,
) -> Result<Option<(String, DeliveryReceipt)>, BridgeHostError> {
    connection
        .query_row(
            "SELECT fingerprint, lifecycle, external_delivery_id, attempt,
                    updated_at_ms, detail_json
             FROM deliveries WHERE bridge_id = ?1 AND message_id = ?2",
            params![bridge_id, message_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, String>(5)?,
                ))
            },
        )
        .optional()
        .map_err(storage_error)?
        .map(
            |(fingerprint, lifecycle, external_id, attempt, updated, detail)| {
                Ok((
                    fingerprint,
                    DeliveryReceipt {
                        bridge_id: bridge_id.to_owned(),
                        message_id: message_id.to_owned(),
                        lifecycle: parse_delivery_lifecycle(&lifecycle)?,
                        external_delivery_id: external_id,
                        attempt: db_u64(attempt)?,
                        updated_at_ms: db_u64(updated)?,
                        detail_json: detail,
                    },
                ))
            },
        )
        .transpose()
}

fn spec_fingerprint(spec: &BridgeSpec) -> Result<String, BridgeHostError> {
    serde_json::to_string(&json!({
        "bridgeId": spec.bridge_id,
        "packageId": spec.package_id,
        "displayName": spec.display_name,
        "launch": parse_json(&spec.launch_json)?,
        "configuration": parse_json(&spec.configuration_json)?,
        "authenticationMethods": spec.authentication_methods.iter()
            .map(authentication_method_tag)
            .collect::<Result<Vec<_>, _>>()?,
        "ingressMode": ingress_mode_tag(&spec.ingress_mode)?,
        "desiredRunning": spec.desired_running,
        "healthIntervalMs": spec.health_interval_ms,
        "credentialValidationIntervalMs": spec.credential_validation_interval_ms,
        "restartLimit": spec.restart_limit,
        "restartWindowMs": spec.restart_window_ms,
    }))
    .map_err(|_| BridgeHostError::InvalidSpec)
}

fn inbound_fingerprint(request: &BridgeInbound) -> Result<String, BridgeHostError> {
    serde_json::to_string(&json!({
        "receivedAtMs": request.received_at_ms,
        "targetChannelId": request.target_channel_id,
        "sender": parse_json(&request.sender_json)?,
        "message": parse_json(&request.message_json)?,
        "attachments": request.attachments.iter().map(|attachment| json!({
            "mediaType": attachment.media_type,
            "name": attachment.name,
            "contentHandle": attachment.content_handle,
        })).collect::<Vec<_>>(),
    }))
    .map_err(|_| BridgeHostError::InvalidSpec)
}

fn outbound_fingerprint(request: &BridgeOutbound) -> Result<String, BridgeHostError> {
    serde_json::to_string(&json!({
        "destination": parse_json(&request.destination_json)?,
        "message": parse_json(&request.message_json)?,
        "attachments": request.attachments.iter().map(|attachment| json!({
            "mediaType": attachment.media_type,
            "name": attachment.name,
            "contentHandle": attachment.content_handle,
        })).collect::<Vec<_>>(),
        "idempotencyKey": request.idempotency_key,
    }))
    .map_err(|_| BridgeHostError::InvalidSpec)
}

fn validate_health(observation: &HealthObservation) -> Result<(), BridgeHostError> {
    parse_json(&observation.detail_json)?;
    if observation.bridge_id.trim().is_empty() {
        return Err(BridgeHostError::InvalidSpec);
    }
    Ok(())
}

fn parse_json(value: &str) -> Result<Value, BridgeHostError> {
    serde_json::from_str(value).map_err(|_| BridgeHostError::InvalidSpec)
}

fn encode_authentication_methods(
    methods: &[AuthenticationMethod],
) -> Result<String, BridgeHostError> {
    serde_json::to_string(
        &methods
            .iter()
            .map(authentication_method_tag)
            .collect::<Result<Vec<_>, _>>()?,
    )
    .map_err(|_| BridgeHostError::StorageUnavailable)
}

fn decode_authentication_methods(
    value: &str,
) -> Result<Vec<AuthenticationMethod>, BridgeHostError> {
    serde_json::from_str::<Vec<String>>(value)
        .map_err(|_| BridgeHostError::StorageUnavailable)?
        .into_iter()
        .map(|method| parse_authentication_method(&method))
        .collect()
}

fn decode_string_list(value: &str) -> Result<Vec<String>, BridgeHostError> {
    serde_json::from_str(value).map_err(|_| BridgeHostError::StorageUnavailable)
}

fn authentication_method_tag(
    method: &AuthenticationMethod,
) -> Result<&'static str, BridgeHostError> {
    match method {
        AuthenticationMethod::QrCode => Ok("QrCode"),
        AuthenticationMethod::PhoneCode => Ok("PhoneCode"),
        AuthenticationMethod::OAuth => Ok("OAuth"),
        AuthenticationMethod::Browser => Ok("Browser"),
        AuthenticationMethod::Terminal => Ok("Terminal"),
        AuthenticationMethod::Manual => Ok("Manual"),
        AuthenticationMethod::Unknown { .. } => Err(BridgeHostError::InvalidSpec),
    }
}

fn parse_authentication_method(value: &str) -> Result<AuthenticationMethod, BridgeHostError> {
    match value {
        "QrCode" => Ok(AuthenticationMethod::QrCode),
        "PhoneCode" => Ok(AuthenticationMethod::PhoneCode),
        "OAuth" => Ok(AuthenticationMethod::OAuth),
        "Browser" => Ok(AuthenticationMethod::Browser),
        "Terminal" => Ok(AuthenticationMethod::Terminal),
        "Manual" => Ok(AuthenticationMethod::Manual),
        _ => Err(BridgeHostError::StorageUnavailable),
    }
}

fn ingress_mode_tag(mode: &BridgeIngressMode) -> Result<&'static str, BridgeHostError> {
    match mode {
        BridgeIngressMode::Queue => Ok("Queue"),
        BridgeIngressMode::Steer => Ok("Steer"),
        BridgeIngressMode::InterruptAndSteer => Ok("InterruptAndSteer"),
        BridgeIngressMode::Unknown { .. } => Err(BridgeHostError::InvalidSpec),
    }
}

fn parse_ingress_mode(value: &str) -> Result<BridgeIngressMode, BridgeHostError> {
    match value {
        "Queue" => Ok(BridgeIngressMode::Queue),
        "Steer" => Ok(BridgeIngressMode::Steer),
        "InterruptAndSteer" => Ok(BridgeIngressMode::InterruptAndSteer),
        _ => Err(BridgeHostError::StorageUnavailable),
    }
}

fn lifecycle_tag(lifecycle: &BridgeLifecycle) -> Result<&'static str, BridgeHostError> {
    match lifecycle {
        BridgeLifecycle::Registered => Ok("Registered"),
        BridgeLifecycle::Starting => Ok("Starting"),
        BridgeLifecycle::AwaitingAuthentication => Ok("AwaitingAuthentication"),
        BridgeLifecycle::Healthy => Ok("Healthy"),
        BridgeLifecycle::Degraded => Ok("Degraded"),
        BridgeLifecycle::BackingOff => Ok("BackingOff"),
        BridgeLifecycle::Stopped => Ok("Stopped"),
        BridgeLifecycle::Failed => Ok("Failed"),
        BridgeLifecycle::Unknown { .. } => Err(BridgeHostError::StorageUnavailable),
    }
}

fn parse_lifecycle(value: &str) -> Result<BridgeLifecycle, BridgeHostError> {
    match value {
        "Registered" => Ok(BridgeLifecycle::Registered),
        "Starting" => Ok(BridgeLifecycle::Starting),
        "AwaitingAuthentication" => Ok(BridgeLifecycle::AwaitingAuthentication),
        "Healthy" => Ok(BridgeLifecycle::Healthy),
        "Degraded" => Ok(BridgeLifecycle::Degraded),
        "BackingOff" => Ok(BridgeLifecycle::BackingOff),
        "Stopped" => Ok(BridgeLifecycle::Stopped),
        "Failed" => Ok(BridgeLifecycle::Failed),
        _ => Err(BridgeHostError::StorageUnavailable),
    }
}

fn credential_lifecycle_tag(
    lifecycle: &CredentialLifecycle,
) -> Result<&'static str, BridgeHostError> {
    match lifecycle {
        CredentialLifecycle::Missing => Ok("Missing"),
        CredentialLifecycle::Challenged => Ok("Challenged"),
        CredentialLifecycle::Validating => Ok("Validating"),
        CredentialLifecycle::Valid => Ok("Valid"),
        CredentialLifecycle::Expiring => Ok("Expiring"),
        CredentialLifecycle::Rejected => Ok("Rejected"),
        CredentialLifecycle::Revoked => Ok("Revoked"),
        CredentialLifecycle::Unknown { .. } => Err(BridgeHostError::StorageUnavailable),
    }
}

fn parse_credential_lifecycle(value: &str) -> Result<CredentialLifecycle, BridgeHostError> {
    match value {
        "Missing" => Ok(CredentialLifecycle::Missing),
        "Challenged" => Ok(CredentialLifecycle::Challenged),
        "Validating" => Ok(CredentialLifecycle::Validating),
        "Valid" => Ok(CredentialLifecycle::Valid),
        "Expiring" => Ok(CredentialLifecycle::Expiring),
        "Rejected" => Ok(CredentialLifecycle::Rejected),
        "Revoked" => Ok(CredentialLifecycle::Revoked),
        _ => Err(BridgeHostError::StorageUnavailable),
    }
}

fn parse_delivery_lifecycle(value: &str) -> Result<DeliveryLifecycle, BridgeHostError> {
    match value {
        "Queued" => Ok(DeliveryLifecycle::Queued),
        "Sending" => Ok(DeliveryLifecycle::Sending),
        "Delivered" => Ok(DeliveryLifecycle::Delivered),
        "Retrying" => Ok(DeliveryLifecycle::Retrying),
        "Rejected" => Ok(DeliveryLifecycle::Rejected),
        "Failed" => Ok(DeliveryLifecycle::Failed),
        _ => Err(BridgeHostError::StorageUnavailable),
    }
}

fn db_i64(value: u64) -> Result<i64, BridgeHostError> {
    i64::try_from(value).map_err(|_| BridgeHostError::InvalidSpec)
}

fn db_u64(value: i64) -> Result<u64, BridgeHostError> {
    u64::try_from(value).map_err(|_| BridgeHostError::StorageUnavailable)
}

fn storage_error(_: rusqlite::Error) -> BridgeHostError {
    BridgeHostError::StorageUnavailable
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use super::BridgeStore;
    use crate::{
        AuthenticationMethod, BridgeHostError, BridgeIngressMode, BridgeLifecycle, BridgeSpec,
        CredentialLifecycle, HealthObservation, PackageChallenge,
    };

    fn spec(desired_running: bool) -> BridgeSpec {
        BridgeSpec {
            bridge_id: "bridge-1".into(),
            package_id: "fixture".into(),
            display_name: "Fixture".into(),
            launch_json: r#"{"executable":"/fixture"}"#.into(),
            configuration_json: "{}".into(),
            authentication_methods: Vec::new(),
            ingress_mode: BridgeIngressMode::Queue,
            desired_running,
            health_interval_ms: 10,
            credential_validation_interval_ms: 20,
            restart_limit: 3,
            restart_window_ms: 1_000,
        }
    }

    #[test]
    fn desired_state_is_idempotent_and_generation_audited() {
        let store = BridgeStore::open_in_memory().expect("store opens");
        let (registered, inserted) = store.register(&spec(true), 10).expect("bridge registers");
        assert!(inserted);
        assert_eq!(registered.generation, 1);

        let stopped = store
            .set_desired("bridge-1", 1, false, 20)
            .expect("desired state changes");
        assert_eq!(stopped.generation, 2);
        let (same, inserted) = store
            .register(&spec(false), 30)
            .expect("registration fingerprint follows desired state");
        assert!(!inserted);
        assert_eq!(same.generation, 2);
        assert_eq!(
            store
                .set_desired("bridge-1", 2, false, 40)
                .expect("same desired state is idempotent")
                .generation,
            2
        );
        store.stop("bridge-1", 50).expect("repeated stop is safe");
        assert_eq!(
            store.record("bridge-1").expect("record exists").generation,
            2
        );

        let mut replacement = spec(false);
        replacement.ingress_mode = BridgeIngressMode::Steer;
        assert_eq!(
            store
                .replace(&replacement, 2, 60)
                .expect("new policy generation installs")
                .generation,
            3
        );
        let audit_count = store
            .lock()
            .expect("store lock")
            .query_row("SELECT COUNT(*) FROM generation_audit", [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("audit count");
        assert_eq!(audit_count, 3);
    }

    #[test]
    fn restart_preserves_desired_state_but_invalidates_live_health() {
        let directory = tempfile::tempdir().expect("temporary state directory");
        let path = directory.path().join("bridges.sqlite");
        {
            let store = BridgeStore::open(&path).expect("store opens");
            store.register(&spec(true), 1).expect("bridge registers");
            store
                .report_health(&HealthObservation {
                    bridge_id: "bridge-1".into(),
                    observed_at_ms: 2,
                    process_alive: true,
                    service_connected: true,
                    can_receive: true,
                    can_send: true,
                    credential_lifecycle: CredentialLifecycle::Valid,
                    detail_json: "{}".into(),
                })
                .expect("health persists");
        }

        let restarted = BridgeStore::open(&path).expect("store reopens");
        assert!(
            restarted
                .spec("bridge-1")
                .expect("spec persists")
                .desired_running
        );
        let status = restarted.status("bridge-1", 3).expect("status persists");
        assert_eq!(status.lifecycle, BridgeLifecycle::Stopped);
        assert!(status.last_health.is_none());
    }

    #[test]
    fn future_schema_fails_closed() {
        let directory = tempfile::tempdir().expect("temporary state directory");
        let path = directory.path().join("future.sqlite");
        let connection = Connection::open(&path).expect("sqlite opens");
        connection
            .pragma_update(None, "user_version", 2)
            .expect("future version is written");
        drop(connection);
        assert!(matches!(
            BridgeStore::open(path),
            Err(BridgeHostError::StorageUnavailable)
        ));
    }

    #[test]
    fn authentication_presentation_is_never_persisted() {
        let store = BridgeStore::open_in_memory().expect("store opens");
        store.register(&spec(true), 1).expect("bridge registers");
        store
            .create_challenge(
                "bridge-1",
                &PackageChallenge {
                    method: AuthenticationMethod::QrCode,
                    expires_at_ms: Some(100),
                    presentation_json: r#"{"qr":"temporary-secret"}"#.into(),
                },
                2,
            )
            .expect("challenge is created");
        let persisted = store
            .lock()
            .expect("store lock")
            .query_row("SELECT presentation_json FROM challenges", [], |row| {
                row.get::<_, String>(0)
            })
            .expect("presentation metadata exists");
        assert_eq!(persisted, "{}");
    }
}
