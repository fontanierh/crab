use std::{path::Path, sync::Mutex};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use crate::{ChannelRoute, PutRouteRequest, TurnRouterError};

const SCHEMA_VERSION: i64 = 1;

pub(crate) struct RouteStore {
    connection: Mutex<Connection>,
}

impl RouteStore {
    pub(crate) fn open(path: impl AsRef<Path>) -> Result<Self, TurnRouterError> {
        Self::initialize(Connection::open(path).map_err(storage_error)?)
    }

    pub(crate) fn open_in_memory() -> Result<Self, TurnRouterError> {
        Self::initialize(Connection::open_in_memory().map_err(storage_error)?)
    }

    fn initialize(mut connection: Connection) -> Result<Self, TurnRouterError> {
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
            _ => return Err(TurnRouterError::StorageUnavailable),
        }
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    pub(crate) fn put(
        &self,
        request: &PutRouteRequest,
        now_ms: u64,
    ) -> Result<ChannelRoute, TurnRouterError> {
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let existing = query_route(&transaction, &request.target_channel_id)?;
        let route = match existing {
            None => {
                if request.expected_generation.is_some() {
                    return Err(TurnRouterError::RouteConflict);
                }
                let route = ChannelRoute {
                    target_channel_id: request.target_channel_id.clone(),
                    lane: request.lane.clone(),
                    binding_id: request.binding_id.clone(),
                    generation: 1,
                    updated_at_ms: now_ms,
                };
                transaction
                    .execute(
                        "INSERT INTO routes (
                            target_channel_id, lane, binding_id, generation, updated_at_ms
                         ) VALUES (?1, ?2, ?3, 1, ?4)",
                        params![
                            route.target_channel_id,
                            route.lane,
                            route.binding_id,
                            db_i64(route.updated_at_ms)?,
                        ],
                    )
                    .map_err(storage_error)?;
                route
            }
            Some(existing)
                if existing.lane == request.lane && existing.binding_id == request.binding_id =>
            {
                existing
            }
            Some(existing) => {
                if request.expected_generation != Some(existing.generation) {
                    return Err(TurnRouterError::RouteConflict);
                }
                let generation = existing
                    .generation
                    .checked_add(1)
                    .ok_or(TurnRouterError::StorageUnavailable)?;
                transaction
                    .execute(
                        "UPDATE routes SET lane = ?2, binding_id = ?3, generation = ?4,
                            updated_at_ms = ?5 WHERE target_channel_id = ?1",
                        params![
                            request.target_channel_id,
                            request.lane,
                            request.binding_id,
                            db_i64(generation)?,
                            db_i64(now_ms)?,
                        ],
                    )
                    .map_err(storage_error)?;
                ChannelRoute {
                    target_channel_id: request.target_channel_id.clone(),
                    lane: request.lane.clone(),
                    binding_id: request.binding_id.clone(),
                    generation,
                    updated_at_ms: now_ms,
                }
            }
        };
        transaction.commit().map_err(storage_error)?;
        Ok(route)
    }

    pub(crate) fn resolve(&self, target_channel_id: &str) -> Result<ChannelRoute, TurnRouterError> {
        let connection = self.lock()?;
        query_route(&connection, target_channel_id)?.ok_or(TurnRouterError::UnknownRoute)
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, TurnRouterError> {
        self.connection
            .lock()
            .map_err(|_| TurnRouterError::StorageUnavailable)
    }
}

fn migrate_v0_to_v1(connection: &mut Connection) -> Result<(), TurnRouterError> {
    let transaction = connection.transaction().map_err(storage_error)?;
    transaction
        .execute_batch(
            "CREATE TABLE routes (
                target_channel_id TEXT PRIMARY KEY,
                lane TEXT NOT NULL,
                binding_id TEXT NOT NULL,
                generation INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL
             );
             PRAGMA user_version = 1;",
        )
        .map_err(storage_error)?;
    transaction.commit().map_err(storage_error)
}

fn query_route(
    connection: &Connection,
    target_channel_id: &str,
) -> Result<Option<ChannelRoute>, TurnRouterError> {
    connection
        .query_row(
            "SELECT lane, binding_id, generation, updated_at_ms FROM routes
             WHERE target_channel_id = ?1",
            params![target_channel_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )
        .optional()
        .map_err(storage_error)?
        .map(|(lane, binding_id, generation, updated_at_ms)| {
            Ok(ChannelRoute {
                target_channel_id: target_channel_id.to_owned(),
                lane,
                binding_id,
                generation: db_u64(generation)?,
                updated_at_ms: db_u64(updated_at_ms)?,
            })
        })
        .transpose()
}

fn db_i64(value: u64) -> Result<i64, TurnRouterError> {
    i64::try_from(value).map_err(|_| TurnRouterError::StorageUnavailable)
}

fn db_u64(value: i64) -> Result<u64, TurnRouterError> {
    u64::try_from(value).map_err(|_| TurnRouterError::StorageUnavailable)
}

fn storage_error(_: rusqlite::Error) -> TurnRouterError {
    TurnRouterError::StorageUnavailable
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use super::{PutRouteRequest, RouteStore, TurnRouterError};

    fn request(binding_id: &str, expected_generation: Option<u64>) -> PutRouteRequest {
        PutRouteRequest {
            target_channel_id: "channel-1".into(),
            lane: "primary".into(),
            binding_id: binding_id.into(),
            expected_generation,
        }
    }

    #[test]
    fn route_registration_is_idempotent_and_replacement_is_compare_and_swap() {
        let store = RouteStore::open_in_memory().expect("store opens");
        let first = store
            .put(&request("binding-1", None), 10)
            .expect("route inserts");
        assert_eq!(first.generation, 1);
        assert_eq!(
            store
                .put(&request("binding-1", None), 20)
                .expect("exact retry resolves"),
            first
        );
        assert!(matches!(
            store.put(&request("binding-2", None), 30),
            Err(TurnRouterError::RouteConflict)
        ));
        let replaced = store
            .put(&request("binding-2", Some(1)), 40)
            .expect("matching generation replaces");
        assert_eq!(replaced.generation, 2);
        assert_eq!(
            store
                .put(&request("binding-2", Some(1)), 50)
                .expect("replacement retry resolves"),
            replaced
        );
    }

    #[test]
    fn routes_survive_restart_and_future_schema_fails_closed() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("routes.sqlite");
        {
            let store = RouteStore::open(&path).expect("store opens");
            store
                .put(&request("binding-1", None), 10)
                .expect("route inserts");
        }
        let restarted = RouteStore::open(&path).expect("store restarts");
        assert_eq!(
            restarted
                .resolve("channel-1")
                .expect("route survives")
                .binding_id,
            "binding-1"
        );
        drop(restarted);
        let connection = Connection::open(&path).expect("database opens directly");
        connection
            .pragma_update(None, "user_version", 99)
            .expect("future version writes");
        drop(connection);
        assert!(matches!(
            RouteStore::open(&path),
            Err(TurnRouterError::StorageUnavailable)
        ));
    }
}
