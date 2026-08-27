mod content;
mod contract;
mod credentials;
mod package;
mod store;

pub use content::{
    ContentStore, ContentStoreError, ContentUpload, FileContentStore, InMemoryContentStore,
    MAX_CONTENT_BYTES, StoredContent,
};
pub use contract::*;
pub use credentials::{
    CredentialStore, CredentialStoreError, FileCredentialStore, InMemoryCredentialStore,
};
pub use package::{
    BridgeCredentialReceipt, BridgeCredentialSink, BridgeCredentialUpdate, BridgeInboundSink,
    BridgePackage, BridgePackageError, BridgePackageFactory, PackageChallenge, PackageCredential,
    PackageCredentialValidation, PackageDelivery, PackageHealth, ProcessBridgePackageFactory,
};

use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex as StdMutex},
    time::{SystemTime, UNIX_EPOCH},
};

use boxology_contract::{CallContext, Caller, CancelToken, ErasedCallError, TraceContext};
use boxology_import_trigger_inbox::{
    EnqueueTrigger, TriggerAttachment, TriggerMode, TriggerSource,
};
use generated::TriggerInboxImport;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use store::{BridgeIncidentEpisode, BridgeStore};
use tokio::{
    sync::{Mutex, RwLock},
    task::JoinHandle,
};
use uuid::Uuid;

type Clock = Arc<dyn Fn() -> Result<u64, BridgeHostError> + Send + Sync>;

#[derive(Clone, Copy)]
enum BridgeIncidentKind {
    AuthenticationRequired,
    CredentialRejected,
    CredentialStoreUnavailable,
    CredentialValidationUnavailable,
    PackageUnavailable,
    RestartBudgetExhausted,
    ServiceUnavailable,
}

impl BridgeIncidentKind {
    fn tag(self) -> &'static str {
        match self {
            Self::AuthenticationRequired => "authentication-required",
            Self::CredentialRejected => "credential-rejected",
            Self::CredentialStoreUnavailable => "credential-store-unavailable",
            Self::CredentialValidationUnavailable => "credential-validation-unavailable",
            Self::PackageUnavailable => "package-unavailable",
            Self::RestartBudgetExhausted => "restart-budget-exhausted",
            Self::ServiceUnavailable => "service-unavailable",
        }
    }
}

/// Opened durable state waiting for composition-owned imports and package services.
pub struct BridgeHostState {
    store: BridgeStore,
    content: Arc<dyn ContentStore>,
}

impl BridgeHostState {
    /// Open file-backed bridge state before assembling the Boxology graph.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, BridgeHostError> {
        let content_path = path
            .as_ref()
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("bridge-content");
        Ok(Self {
            store: BridgeStore::open(path)?,
            content: Arc::new(FileContentStore::open(content_path).map_err(map_content_error)?),
        })
    }

    /// Open ephemeral bridge state before assembling the Boxology graph.
    pub fn open_in_memory() -> Result<Self, BridgeHostError> {
        Ok(Self {
            store: BridgeStore::open_in_memory()?,
            content: Arc::new(InMemoryContentStore::default()),
        })
    }

    /// Inject the composition-selected trigger inbox and runtime service boundaries.
    #[must_use]
    pub fn connect(
        self,
        trigger_inbox: TriggerInboxImport,
        packages: Arc<dyn BridgePackageFactory>,
        credentials: Arc<dyn CredentialStore>,
    ) -> BridgeHost {
        let host = BridgeHost {
            trigger_inbox: Arc::new(trigger_inbox),
            packages,
            credentials,
            content: self.content,
            store: Arc::new(self.store),
            connections: Arc::new(RwLock::new(HashMap::new())),
            active_package_instances: Arc::new(RwLock::new(HashMap::new())),
            credential_updates: Arc::new(Mutex::new(())),
            supervisors: Arc::new(StdMutex::new(HashMap::new())),
            operations: Arc::new(Mutex::new(())),
            clock: Arc::new(system_time_ms),
        };
        if tokio::runtime::Handle::try_current().is_ok()
            && let Ok(bridge_ids) = host.store.desired_bridge_ids()
        {
            for bridge_id in bridge_ids {
                host.ensure_supervisor(&bridge_id);
            }
        }
        host
    }
}

/// Durable bridge supervisor, credential broker and selected-message router.
pub struct BridgeHost {
    trigger_inbox: Arc<TriggerInboxImport>,
    packages: Arc<dyn BridgePackageFactory>,
    credentials: Arc<dyn CredentialStore>,
    content: Arc<dyn ContentStore>,
    store: Arc<BridgeStore>,
    connections: Arc<RwLock<HashMap<String, Arc<dyn BridgePackage>>>>,
    active_package_instances: Arc<RwLock<HashMap<String, String>>>,
    credential_updates: Arc<Mutex<()>>,
    supervisors: Arc<StdMutex<HashMap<String, JoinHandle<()>>>>,
    operations: Arc<Mutex<()>>,
    clock: Clock,
}

impl BridgeHost {
    fn supervisor_context(&self) -> SupervisorContext {
        SupervisorContext {
            trigger_inbox: self.trigger_inbox.clone(),
            packages: self.packages.clone(),
            credentials: self.credentials.clone(),
            content: self.content.clone(),
            store: self.store.clone(),
            connections: self.connections.clone(),
            active_package_instances: self.active_package_instances.clone(),
            credential_updates: self.credential_updates.clone(),
            operations: self.operations.clone(),
            clock: self.clock.clone(),
        }
    }

    fn inbound_sink(&self, bridge_id: &str) -> Arc<dyn BridgeInboundSink> {
        Arc::new(BridgeIngressRouter {
            bridge_id: bridge_id.to_owned(),
            trigger_inbox: self.trigger_inbox.clone(),
            store: self.store.clone(),
            content: self.content.clone(),
            operations: self.operations.clone(),
            clock: self.clock.clone(),
        })
    }

    fn credential_sink(
        &self,
        bridge_id: &str,
        package_instance_id: &str,
    ) -> Arc<dyn BridgeCredentialSink> {
        Arc::new(BridgeCredentialRouter {
            bridge_id: bridge_id.to_owned(),
            package_instance_id: package_instance_id.to_owned(),
            store: self.store.clone(),
            credentials: self.credentials.clone(),
            active_package_instances: self.active_package_instances.clone(),
            credential_updates: self.credential_updates.clone(),
        })
    }

    fn ensure_supervisor(&self, bridge_id: &str) {
        let Ok(mut supervisors) = self.supervisors.lock() else {
            return;
        };
        if supervisors
            .get(bridge_id)
            .is_some_and(|handle| !handle.is_finished())
        {
            return;
        }
        if let Some(finished) = supervisors.remove(bridge_id) {
            finished.abort();
        }
        let bridge_id = bridge_id.to_owned();
        let context = self.supervisor_context();
        let task_id = bridge_id.clone();
        let handle = tokio::spawn(async move {
            context.run(task_id).await;
        });
        supervisors.insert(bridge_id, handle);
    }

    fn stop_supervisor(&self, bridge_id: &str) {
        if let Ok(mut supervisors) = self.supervisors.lock()
            && let Some(handle) = supervisors.remove(bridge_id)
        {
            handle.abort();
        }
    }

    async fn connection(&self, bridge_id: &str) -> Result<Arc<dyn BridgePackage>, BridgeHostError> {
        self.connections
            .read()
            .await
            .get(bridge_id)
            .cloned()
            .ok_or(BridgeHostError::BridgeUnhealthy)
    }

    async fn credential_secret(&self, bridge_id: &str) -> Result<Option<String>, BridgeHostError> {
        let status = self.store.credential(bridge_id)?;
        let Some(handle) = status.credential_handle else {
            return Ok(None);
        };
        self.credentials
            .get(&handle)
            .await
            .map(Some)
            .map_err(map_credential_error)
    }

    async fn ensure_running(&self, bridge_id: &str) -> Result<BridgeStatus, BridgeHostError> {
        let spec = self.store.spec(bridge_id)?;
        if !spec.desired_running {
            return self.stop_connection(bridge_id).await;
        }
        let now_ms = (self.clock)()?;
        if self.connections.read().await.contains_key(bridge_id) {
            let result = self.probe_health(bridge_id).await;
            if result.is_err() {
                let _ = self
                    .supervisor_context()
                    .notify_incident(bridge_id, BridgeIncidentKind::PackageUnavailable, now_ms)
                    .await;
            }
            return result;
        }
        let status = self.store.status(bridge_id, now_ms)?;
        if status.next_restart_at_ms.is_some_and(|next| next > now_ms) {
            return Ok(status);
        }
        if let Err(error) = self.store.record_start_attempt(&spec, now_ms) {
            self.store.set_lifecycle(
                bridge_id,
                &BridgeLifecycle::Failed,
                Some("restart budget exhausted"),
                None,
            )?;
            let _ = self
                .supervisor_context()
                .notify_incident(
                    bridge_id,
                    BridgeIncidentKind::RestartBudgetExhausted,
                    now_ms,
                )
                .await;
            return Err(error);
        }
        self.store
            .set_lifecycle(bridge_id, &BridgeLifecycle::Starting, None, None)?;
        let package_instance_id = Uuid::new_v4().to_string();
        let package = match self
            .packages
            .launch(
                &spec,
                self.inbound_sink(bridge_id),
                self.credential_sink(bridge_id, &package_instance_id),
            )
            .await
        {
            Ok(package) => package,
            Err(_) => {
                self.schedule_backoff(bridge_id, &spec)?;
                let _ = self
                    .supervisor_context()
                    .notify_incident(bridge_id, BridgeIncidentKind::PackageUnavailable, now_ms)
                    .await;
                return Err(BridgeHostError::PackageProtocolFailed);
            }
        };
        activate_package_instance(
            &self.active_package_instances,
            &self.credential_updates,
            bridge_id,
            package_instance_id,
        )
        .await;
        self.connections
            .write()
            .await
            .insert(bridge_id.to_owned(), package);
        let result = self.probe_health(bridge_id).await;
        if result.is_err() {
            let _ = self
                .supervisor_context()
                .notify_incident(bridge_id, BridgeIncidentKind::PackageUnavailable, now_ms)
                .await;
        }
        result
    }

    fn schedule_backoff(&self, bridge_id: &str, spec: &BridgeSpec) -> Result<(), BridgeHostError> {
        let status = self.store.status(bridge_id, (self.clock)()?)?;
        let exponent = status.consecutive_failures.min(20) as u32;
        let multiplier = 1_u64.checked_shl(exponent).unwrap_or(u64::MAX);
        let delay = spec
            .health_interval_ms
            .saturating_mul(multiplier)
            .min(spec.restart_window_ms);
        let next = (self.clock)()?.saturating_add(delay);
        self.store.set_backoff(bridge_id, next)
    }

    async fn probe_health(&self, bridge_id: &str) -> Result<BridgeStatus, BridgeHostError> {
        let package = self.connection(bridge_id).await?;
        let credential = self.credential_secret(bridge_id).await?;
        let now_ms = (self.clock)()?;
        let health = match package.health(credential.as_deref()).await {
            Ok(health) => health,
            Err(_) => {
                deactivate_package_instance(
                    &self.active_package_instances,
                    &self.credential_updates,
                    bridge_id,
                )
                .await;
                self.connections.write().await.remove(bridge_id);
                let spec = self.store.spec(bridge_id)?;
                self.schedule_backoff(bridge_id, &spec)?;
                return Err(BridgeHostError::BridgeUnhealthy);
            }
        };
        let no_auth = self
            .store
            .spec(bridge_id)?
            .authentication_methods
            .is_empty();
        let credential_lifecycle = if no_auth || health.credential_valid {
            CredentialLifecycle::Valid
        } else if credential.is_none() {
            CredentialLifecycle::Missing
        } else {
            CredentialLifecycle::Rejected
        };
        let observation = HealthObservation {
            bridge_id: bridge_id.to_owned(),
            observed_at_ms: now_ms,
            process_alive: health.process_alive,
            service_connected: health.service_connected,
            can_receive: health.can_receive,
            can_send: health.can_send,
            credential_lifecycle: credential_lifecycle.clone(),
            detail_json: health.detail_json,
        };
        let mut status = self.store.report_health(&observation)?;
        if !observation.process_alive {
            deactivate_package_instance(
                &self.active_package_instances,
                &self.credential_updates,
                bridge_id,
            )
            .await;
            self.connections.write().await.remove(bridge_id);
            let spec = self.store.spec(bridge_id)?;
            self.schedule_backoff(bridge_id, &spec)?;
            return Err(BridgeHostError::BridgeUnhealthy);
        }
        if matches!(credential_lifecycle, CredentialLifecycle::Missing) && !no_auth {
            self.store.set_lifecycle(
                bridge_id,
                &BridgeLifecycle::AwaitingAuthentication,
                None,
                None,
            )?;
            status = self.store.status(bridge_id, now_ms)?;
        }
        Ok(status)
    }

    async fn stop_connection(&self, bridge_id: &str) -> Result<BridgeStatus, BridgeHostError> {
        deactivate_package_instance(
            &self.active_package_instances,
            &self.credential_updates,
            bridge_id,
        )
        .await;
        if let Some(package) = self.connections.write().await.remove(bridge_id) {
            let _ = package.stop().await;
        }
        self.store.suspend(bridge_id, (self.clock)()?)
    }
}

struct SupervisorContext {
    trigger_inbox: Arc<TriggerInboxImport>,
    packages: Arc<dyn BridgePackageFactory>,
    credentials: Arc<dyn CredentialStore>,
    content: Arc<dyn ContentStore>,
    store: Arc<BridgeStore>,
    connections: Arc<RwLock<HashMap<String, Arc<dyn BridgePackage>>>>,
    active_package_instances: Arc<RwLock<HashMap<String, String>>>,
    credential_updates: Arc<Mutex<()>>,
    operations: Arc<Mutex<()>>,
    clock: Clock,
}

impl SupervisorContext {
    async fn run(self, bridge_id: String) {
        loop {
            let interval_ms = match self.store.spec(&bridge_id) {
                Ok(spec) if spec.desired_running => spec.health_interval_ms,
                _ => break,
            };
            self.tick(&bridge_id).await;
            tokio::time::sleep(std::time::Duration::from_millis(interval_ms)).await;
        }
    }

    async fn notify_incident(
        &self,
        bridge_id: &str,
        kind: BridgeIncidentKind,
        now_ms: u64,
    ) -> Result<(), BridgeHostError> {
        let spec = self.store.spec(bridge_id)?;
        let Some(target) = spec.alert_target else {
            return Ok(());
        };
        let episode = self
            .store
            .begin_incident(bridge_id, kind.tag(), &target, now_ms)?;
        if episode.incident_trigger_id.is_some() {
            return Ok(());
        }
        let receipt = self
            .enqueue_incident(
                &episode,
                "crab.bridge.incident",
                episode.started_at_ms,
                None,
            )
            .await?;
        self.store
            .mark_incident_enqueued(bridge_id, episode.sequence, &receipt.trigger_id)
    }

    async fn notify_recovery(&self, bridge_id: &str, now_ms: u64) -> Result<(), BridgeHostError> {
        let Some(episode) = self.store.recover_incident(bridge_id, now_ms)? else {
            return Ok(());
        };
        if episode.recovery_trigger_id.is_some() {
            return Ok(());
        }
        let recovered_at_ms = episode
            .recovered_at_ms
            .ok_or(BridgeHostError::StorageUnavailable)?;
        let receipt = self
            .enqueue_incident(
                &episode,
                "crab.bridge.recovered",
                recovered_at_ms,
                Some(recovered_at_ms),
            )
            .await?;
        self.store
            .mark_recovery_enqueued(bridge_id, episode.sequence, &receipt.trigger_id)
    }

    async fn enqueue_incident(
        &self,
        episode: &BridgeIncidentEpisode,
        event: &str,
        not_before_ms: u64,
        recovered_at_ms: Option<u64>,
    ) -> Result<boxology_import_trigger_inbox::TriggerReceipt, BridgeHostError> {
        let phase = if recovered_at_ms.is_some() {
            "recovery"
        } else {
            "incident"
        };
        let message_json = json!({
            "kind": event,
            "bridgeId": episode.bridge_id,
            "generation": episode.generation,
            "incidentSequence": episode.sequence,
            "incident": episode.kind,
            "startedAtMs": episode.started_at_ms,
            "recoveredAtMs": recovered_at_ms,
        })
        .to_string();
        self.trigger_inbox
            .enqueue(
                CallContext::new(
                    Caller::System("bridge-supervisor"),
                    None,
                    CancelToken::new(),
                    TraceContext::empty(),
                    None,
                ),
                EnqueueTrigger {
                    source: TriggerSource::Bridge,
                    source_id: episode.bridge_id.clone(),
                    deduplication_key: format!(
                        "crab-supervisor:{}:{}:{phase}",
                        episode.generation, episode.sequence
                    ),
                    target_channel_id: episode.target_channel_id.clone(),
                    lane: episode.lane.clone(),
                    mode: TriggerMode::Queue,
                    not_before_ms,
                    message_json,
                    attachments: Vec::new(),
                },
            )
            .await
            .map_err(map_trigger_error)
    }

    async fn tick(&self, bridge_id: &str) {
        let _operation = self.operations.lock().await;
        let Ok(spec) = self.store.spec(bridge_id) else {
            return;
        };
        if !spec.desired_running {
            return;
        }
        let Ok(now_ms) = (self.clock)() else {
            return;
        };
        let connection = self.connections.read().await.get(bridge_id).cloned();
        let package = if let Some(connection) = connection {
            connection
        } else {
            let Ok(status) = self.store.status(bridge_id, now_ms) else {
                return;
            };
            if status.next_restart_at_ms.is_some_and(|next| next > now_ms) {
                return;
            }
            if self.store.record_start_attempt(&spec, now_ms).is_err() {
                let _ = self.store.set_lifecycle(
                    bridge_id,
                    &BridgeLifecycle::Failed,
                    Some("restart budget exhausted"),
                    None,
                );
                let _ = self
                    .notify_incident(
                        bridge_id,
                        BridgeIncidentKind::RestartBudgetExhausted,
                        now_ms,
                    )
                    .await;
                return;
            }
            let _ = self
                .store
                .set_lifecycle(bridge_id, &BridgeLifecycle::Starting, None, None);
            let inbound: Arc<dyn BridgeInboundSink> = Arc::new(BridgeIngressRouter {
                bridge_id: bridge_id.to_owned(),
                trigger_inbox: self.trigger_inbox.clone(),
                store: self.store.clone(),
                content: self.content.clone(),
                operations: self.operations.clone(),
                clock: self.clock.clone(),
            });
            let package_instance_id = Uuid::new_v4().to_string();
            let credential_updates: Arc<dyn BridgeCredentialSink> =
                Arc::new(BridgeCredentialRouter {
                    bridge_id: bridge_id.to_owned(),
                    package_instance_id: package_instance_id.clone(),
                    store: self.store.clone(),
                    credentials: self.credentials.clone(),
                    active_package_instances: self.active_package_instances.clone(),
                    credential_updates: self.credential_updates.clone(),
                });
            let Ok(package) = self
                .packages
                .launch(&spec, inbound, credential_updates)
                .await
            else {
                self.backoff(bridge_id, &spec, now_ms);
                let _ = self
                    .notify_incident(bridge_id, BridgeIncidentKind::PackageUnavailable, now_ms)
                    .await;
                return;
            };
            activate_package_instance(
                &self.active_package_instances,
                &self.credential_updates,
                bridge_id,
                package_instance_id,
            )
            .await;
            self.connections
                .write()
                .await
                .insert(bridge_id.to_owned(), package.clone());
            package
        };

        let credential_status = match self.store.credential(bridge_id) {
            Ok(status) => status,
            Err(_) => return,
        };
        let mut credential_override = None;
        let credential = match credential_status.credential_handle.as_deref() {
            Some(handle) => match self.credentials.get(handle).await {
                Ok(credential) => Some(credential),
                Err(CredentialStoreError::UnknownHandle) => {
                    credential_override = Some(CredentialLifecycle::Missing);
                    let _ = self.store.clear_credential_reference(
                        bridge_id,
                        &CredentialLifecycle::Missing,
                        now_ms,
                    );
                    None
                }
                Err(CredentialStoreError::InvalidCredential) => {
                    credential_override = Some(CredentialLifecycle::Rejected);
                    let _ = self.store.clear_credential_reference(
                        bridge_id,
                        &CredentialLifecycle::Rejected,
                        now_ms,
                    );
                    None
                }
                Err(CredentialStoreError::Unavailable) => {
                    let _ = self.store.set_lifecycle(
                        bridge_id,
                        &BridgeLifecycle::Degraded,
                        Some("credential provider unavailable"),
                        None,
                    );
                    let _ = self
                        .notify_incident(
                            bridge_id,
                            BridgeIncidentKind::CredentialStoreUnavailable,
                            now_ms,
                        )
                        .await;
                    return;
                }
            },
            None => None,
        };
        let health = match package.health(credential.as_deref()).await {
            Ok(health) => health,
            Err(_) => {
                deactivate_package_instance(
                    &self.active_package_instances,
                    &self.credential_updates,
                    bridge_id,
                )
                .await;
                self.connections.write().await.remove(bridge_id);
                self.backoff(bridge_id, &spec, now_ms);
                let _ = self
                    .notify_incident(bridge_id, BridgeIncidentKind::PackageUnavailable, now_ms)
                    .await;
                return;
            }
        };
        let no_auth = spec.authentication_methods.is_empty();
        let mut credential_lifecycle = if no_auth {
            CredentialLifecycle::Valid
        } else if let Some(lifecycle) = credential_override {
            lifecycle
        } else if health.credential_valid {
            CredentialLifecycle::Valid
        } else if credential.is_some() {
            CredentialLifecycle::Rejected
        } else {
            CredentialLifecycle::Missing
        };
        let mut validation_unavailable = false;
        if let (Some(secret), Some(_)) = (
            credential.as_deref(),
            credential_status.credential_handle.as_ref(),
        ) && now_ms.saturating_sub(credential_status.validated_at_ms.unwrap_or(0))
            >= spec.credential_validation_interval_ms
        {
            match package.validate_credentials(secret).await {
                Ok(validation) => {
                    credential_lifecycle = if validation.valid {
                        CredentialLifecycle::Valid
                    } else {
                        CredentialLifecycle::Rejected
                    };
                    let _ = self.store.update_validation(
                        bridge_id,
                        &credential_lifecycle,
                        now_ms,
                        validation.expires_at_ms,
                        validation.account_hint.as_deref(),
                        &validation.detail_json,
                    );
                }
                Err(_) => validation_unavailable = true,
            }
        }
        let process_alive = health.process_alive;
        let service_connected = health.service_connected;
        let can_receive = health.can_receive;
        let can_send = health.can_send;
        let _ = self.store.report_health(&HealthObservation {
            bridge_id: bridge_id.to_owned(),
            observed_at_ms: now_ms,
            process_alive: health.process_alive,
            service_connected,
            can_receive,
            can_send,
            credential_lifecycle: credential_lifecycle.clone(),
            detail_json: health.detail_json,
        });
        if !process_alive {
            deactivate_package_instance(
                &self.active_package_instances,
                &self.credential_updates,
                bridge_id,
            )
            .await;
            self.connections.write().await.remove(bridge_id);
            self.backoff(bridge_id, &spec, now_ms);
            let _ = self
                .notify_incident(bridge_id, BridgeIncidentKind::PackageUnavailable, now_ms)
                .await;
            return;
        }
        if validation_unavailable {
            let _ = self.store.set_lifecycle(
                bridge_id,
                &BridgeLifecycle::Degraded,
                Some("active credential validation failed"),
                None,
            );
            let _ = self
                .notify_incident(
                    bridge_id,
                    BridgeIncidentKind::CredentialValidationUnavailable,
                    now_ms,
                )
                .await;
            return;
        }
        if matches!(credential_lifecycle, CredentialLifecycle::Missing) && !no_auth {
            let _ = self.store.set_lifecycle(
                bridge_id,
                &BridgeLifecycle::AwaitingAuthentication,
                None,
                None,
            );
            let _ = self
                .notify_incident(
                    bridge_id,
                    BridgeIncidentKind::AuthenticationRequired,
                    now_ms,
                )
                .await;
            return;
        }
        if matches!(credential_lifecycle, CredentialLifecycle::Rejected) && !no_auth {
            let _ = self
                .notify_incident(bridge_id, BridgeIncidentKind::CredentialRejected, now_ms)
                .await;
            return;
        }
        if !service_connected || !can_receive || !can_send {
            let _ = self
                .notify_incident(bridge_id, BridgeIncidentKind::ServiceUnavailable, now_ms)
                .await;
            return;
        }
        let _ = self.notify_recovery(bridge_id, now_ms).await;
    }

    fn backoff(&self, bridge_id: &str, spec: &BridgeSpec, now_ms: u64) {
        let failures = self
            .store
            .status(bridge_id, now_ms)
            .map_or(0, |status| status.consecutive_failures);
        let multiplier = 1_u64
            .checked_shl(failures.min(20) as u32)
            .unwrap_or(u64::MAX);
        let delay = spec
            .health_interval_ms
            .saturating_mul(multiplier)
            .min(spec.restart_window_ms);
        let _ = self
            .store
            .set_backoff(bridge_id, now_ms.saturating_add(delay));
    }
}

struct BridgeIngressRouter {
    bridge_id: String,
    trigger_inbox: Arc<TriggerInboxImport>,
    store: Arc<BridgeStore>,
    content: Arc<dyn ContentStore>,
    operations: Arc<Mutex<()>>,
    clock: Clock,
}

struct BridgeCredentialRouter {
    bridge_id: String,
    package_instance_id: String,
    store: Arc<BridgeStore>,
    credentials: Arc<dyn CredentialStore>,
    active_package_instances: Arc<RwLock<HashMap<String, String>>>,
    credential_updates: Arc<Mutex<()>>,
}

#[async_trait::async_trait]
impl BridgeInboundSink for BridgeIngressRouter {
    async fn accept(&self, request: BridgeInbound) -> Result<TriggerIntent, BridgeHostError> {
        if request.bridge_id != self.bridge_id {
            return Err(BridgeHostError::InvalidSpec);
        }
        route_inbound(
            &self.trigger_inbox,
            &self.store,
            &self.content,
            &self.operations,
            &self.clock,
            CallContext::new(
                Caller::System("bridge-package"),
                None,
                CancelToken::new(),
                TraceContext::empty(),
                None,
            ),
            request,
        )
        .await
    }

    async fn store_content(
        &self,
        request: ContentUpload,
    ) -> Result<StoredContent, BridgeHostError> {
        if request.bridge_id != self.bridge_id {
            return Err(BridgeHostError::InvalidSpec);
        }
        self.content.put(request).await.map_err(map_content_error)
    }
}

#[async_trait::async_trait]
impl BridgeCredentialSink for BridgeCredentialRouter {
    async fn persist(
        &self,
        request: BridgeCredentialUpdate,
    ) -> Result<BridgeCredentialReceipt, BridgeHostError> {
        let _update = self.credential_updates.lock().await;
        if request.bridge_id != self.bridge_id
            || !valid_credential_fingerprint(&request.previous_fingerprint)
            || self
                .active_package_instances
                .read()
                .await
                .get(&self.bridge_id)
                != Some(&self.package_instance_id)
        {
            return Err(BridgeHostError::InvalidSpec);
        }
        let handle = self
            .store
            .credential(&self.bridge_id)?
            .credential_handle
            .ok_or(BridgeHostError::CredentialRejected)?;
        let current = self
            .credentials
            .get(&handle)
            .await
            .map_err(map_credential_error)?;
        if credential_fingerprint(&current) != request.previous_fingerprint {
            return Err(BridgeHostError::GenerationConflict);
        }
        self.credentials
            .replace(&handle, &self.bridge_id, &request.credential_json)
            .await
            .map_err(map_credential_error)?;
        Ok(BridgeCredentialReceipt {
            credential_fingerprint: credential_fingerprint(&request.credential_json),
        })
    }
}

impl Drop for BridgeHost {
    fn drop(&mut self) {
        if let Ok(mut supervisors) = self.supervisors.lock() {
            for (_, handle) in supervisors.drain() {
                handle.abort();
            }
        }
    }
}

#[boxology::implementation]
impl BridgeHost {
    pub async fn register_bridge(
        &self,
        context: CallContext,
        request: BridgeSpec,
    ) -> Result<BridgeRecord, BridgeHostError> {
        let _ = context;
        let _operation = self.operations.lock().await;
        validate_spec(&request)?;
        let (record, _) = self.store.register(&request, (self.clock)()?)?;
        if record.desired_running {
            self.ensure_supervisor(&record.bridge_id);
            self.ensure_running(&record.bridge_id).await?;
        }
        self.store.record(&record.bridge_id)
    }

    pub async fn list_bridges(
        &self,
        context: CallContext,
        request: ListBridgesRequest,
    ) -> Result<BridgeCatalog, BridgeHostError> {
        let _ = (context, request);
        Ok(BridgeCatalog {
            bridges: self.store.records()?,
        })
    }

    pub async fn replace_bridge(
        &self,
        context: CallContext,
        request: ReplaceBridgeRequest,
    ) -> Result<BridgeRecord, BridgeHostError> {
        let _ = context;
        let _operation = self.operations.lock().await;
        validate_spec(&request.spec)?;
        let existing = self.store.record(&request.spec.bridge_id)?;
        if existing.generation != request.expected_generation {
            return Err(BridgeHostError::GenerationConflict);
        }
        self.stop_supervisor(&request.spec.bridge_id);
        deactivate_package_instance(
            &self.active_package_instances,
            &self.credential_updates,
            &request.spec.bridge_id,
        )
        .await;
        if let Some(package) = self
            .connections
            .write()
            .await
            .remove(&request.spec.bridge_id)
        {
            let _ = package.stop().await;
        }
        let record =
            self.store
                .replace(&request.spec, request.expected_generation, (self.clock)()?)?;
        if record.desired_running {
            self.ensure_supervisor(&record.bridge_id);
            self.ensure_running(&record.bridge_id).await?;
        }
        self.store.record(&record.bridge_id)
    }

    pub async fn reconcile_bridge(
        &self,
        context: CallContext,
        request: ReconcileBridgeRequest,
    ) -> Result<BridgeStatus, BridgeHostError> {
        let _ = context;
        let _operation = self.operations.lock().await;
        let record = self.store.set_desired(
            &request.bridge_id,
            request.expected_generation,
            request.desired_running,
            (self.clock)()?,
        )?;
        if record.desired_running {
            self.ensure_supervisor(&record.bridge_id);
            self.ensure_running(&record.bridge_id).await
        } else {
            self.stop_supervisor(&record.bridge_id);
            self.stop_connection(&record.bridge_id).await
        }
    }

    pub async fn report_health(
        &self,
        context: CallContext,
        request: HealthObservation,
    ) -> Result<BridgeStatus, BridgeHostError> {
        let _ = context;
        let _operation = self.operations.lock().await;
        self.store.report_health(&request)
    }

    pub async fn begin_authentication(
        &self,
        context: CallContext,
        request: BeginAuthenticationRequest,
    ) -> Result<AuthenticationChallenge, BridgeHostError> {
        let _ = context;
        let _operation = self.operations.lock().await;
        validate_json_object(&request.context_json)?;
        let spec = self.store.spec(&request.bridge_id)?;
        if let Some(method) = &request.preferred_method
            && !spec.authentication_methods.contains(method)
        {
            return Err(BridgeHostError::AuthenticationUnavailable);
        }
        if spec.authentication_methods.is_empty() {
            return Err(BridgeHostError::AuthenticationUnavailable);
        }
        let package = self.connection(&request.bridge_id).await?;
        let challenge = package
            .begin_authentication(request.preferred_method.as_ref(), &request.context_json)
            .await
            .map_err(map_package_error)?;
        if !spec.authentication_methods.contains(&challenge.method) {
            return Err(BridgeHostError::PackageProtocolFailed);
        }
        self.store
            .create_challenge(&request.bridge_id, &challenge, (self.clock)()?)
    }

    pub async fn submit_authentication(
        &self,
        context: CallContext,
        request: SubmitAuthenticationRequest,
    ) -> Result<CredentialStatus, BridgeHostError> {
        let _ = context;
        let _operation = self.operations.lock().await;
        validate_json(&request.response_json)?;
        let now_ms = (self.clock)()?;
        self.store
            .verify_challenge(&request.bridge_id, &request.challenge_id, now_ms)?;
        let package = self.connection(&request.bridge_id).await?;
        let credential = package
            .submit_authentication(&request.challenge_id, &request.response_json)
            .await
            .map_err(map_package_error)?;
        let validation = package
            .validate_credentials(&credential.secret_json)
            .await
            .map_err(map_package_error)?;
        if !validation.valid {
            return Err(BridgeHostError::CredentialRejected);
        }
        let previous_handle = self.store.credential(&request.bridge_id)?.credential_handle;
        let handle = self
            .credentials
            .put(&request.bridge_id, &credential.secret_json)
            .await
            .map_err(map_credential_error)?;
        let stored = self.store.set_credential(
            &request.bridge_id,
            &request.challenge_id,
            &handle,
            now_ms,
            validation.expires_at_ms.or(credential.expires_at_ms),
            validation
                .account_hint
                .as_deref()
                .or(credential.account_hint.as_deref()),
            &validation.detail_json,
        );
        let stored = match stored {
            Ok(stored) => stored,
            Err(error) => {
                let _ = self.credentials.invalidate(&handle).await;
                return Err(error);
            }
        };
        if let Some(previous_handle) = previous_handle
            && previous_handle != handle
        {
            let _ = self.credentials.invalidate(&previous_handle).await;
        }
        if package
            .credential_committed(&credential.secret_json)
            .await
            .is_err()
        {
            deactivate_package_instance(
                &self.active_package_instances,
                &self.credential_updates,
                &request.bridge_id,
            )
            .await;
            self.connections.write().await.remove(&request.bridge_id);
            let _ = package.stop().await;
            let _ = self.store.set_lifecycle(
                &request.bridge_id,
                &BridgeLifecycle::Degraded,
                Some("credential commit acknowledgement failed"),
                None,
            );
        }
        Ok(stored)
    }

    pub async fn validate_credentials(
        &self,
        context: CallContext,
        request: BridgeReference,
    ) -> Result<CredentialStatus, BridgeHostError> {
        let _ = context;
        let _operation = self.operations.lock().await;
        let status = self.store.credential(&request.bridge_id)?;
        let handle = status
            .credential_handle
            .ok_or(BridgeHostError::CredentialRejected)?;
        let secret = self
            .credentials
            .get(&handle)
            .await
            .map_err(map_credential_error)?;
        let validation = self
            .connection(&request.bridge_id)
            .await?
            .validate_credentials(&secret)
            .await
            .map_err(map_package_error)?;
        self.store.update_validation(
            &request.bridge_id,
            if validation.valid {
                &CredentialLifecycle::Valid
            } else {
                &CredentialLifecycle::Rejected
            },
            (self.clock)()?,
            validation.expires_at_ms,
            validation.account_hint.as_deref(),
            &validation.detail_json,
        )
    }

    pub async fn invalidate_credentials(
        &self,
        context: CallContext,
        request: BridgeReference,
    ) -> Result<BridgeReceipt, BridgeHostError> {
        let _ = context;
        let _operation = self.operations.lock().await;
        let status = self.store.credential(&request.bridge_id)?;
        if let Some(handle) = status.credential_handle {
            if let Ok(secret) = self.credentials.get(&handle).await
                && let Ok(package) = self.connection(&request.bridge_id).await
            {
                let _ = package.invalidate_credentials(&secret).await;
            }
            self.credentials
                .invalidate(&handle)
                .await
                .map_err(map_credential_error)?;
        }
        self.store
            .revoke_credential(&request.bridge_id, (self.clock)()?)
    }

    pub async fn accept_inbound(
        &self,
        context: CallContext,
        request: BridgeInbound,
    ) -> Result<TriggerIntent, BridgeHostError> {
        route_inbound(
            &self.trigger_inbox,
            &self.store,
            &self.content,
            &self.operations,
            &self.clock,
            context,
            request,
        )
        .await
    }

    pub async fn import_content(
        &self,
        context: CallContext,
        request: ImportBridgeContentRequest,
    ) -> Result<ImportedBridgeContent, BridgeHostError> {
        let _ = context;
        let source_path = validate_content_import(&request)?;
        let _operation = self.operations.lock().await;
        self.store.record(&request.bridge_id)?;
        let bytes = content::read_import_source(&source_path)
            .await
            .map_err(map_content_error)?;
        let stored = self
            .content
            .put(ContentUpload {
                bridge_id: request.bridge_id,
                external_event_id: format!("agent-import:{}", request.import_id),
                media_type: request.media_type,
                name: request.name,
                bytes,
            })
            .await
            .map_err(map_content_error)?;
        Ok(ImportedBridgeContent {
            attachment: stored.attachment,
            size_bytes: stored.size,
            sha256: stored.sha256,
        })
    }

    pub async fn deliver_message(
        &self,
        context: CallContext,
        request: BridgeOutbound,
    ) -> Result<DeliveryReceipt, BridgeHostError> {
        let _ = context;
        let _operation = self.operations.lock().await;
        validate_outbound(&request)?;
        let status = self.store.status(&request.bridge_id, (self.clock)()?)?;
        if !matches!(status.lifecycle, BridgeLifecycle::Healthy)
            || !status
                .last_health
                .as_ref()
                .is_some_and(|health| health.can_send && health.service_connected)
        {
            return Err(BridgeHostError::BridgeUnhealthy);
        }
        let (receipt, should_send) = self.store.begin_delivery(&request, (self.clock)()?)?;
        if !should_send {
            return Ok(receipt);
        }
        let credential = self.credential_secret(&request.bridge_id).await?;
        let delivered = self
            .connection(&request.bridge_id)
            .await?
            .deliver(&request, credential.as_deref())
            .await;
        match delivered {
            Ok(delivered) => self.store.complete_delivery(
                &request.bridge_id,
                &request.message_id,
                &delivered.external_delivery_id,
                &delivered.detail_json,
                (self.clock)()?,
            ),
            Err(_) => {
                self.store.fail_delivery(
                    &request.bridge_id,
                    &request.message_id,
                    (self.clock)()?,
                )?;
                Err(BridgeHostError::DeliveryFailed)
            }
        }
    }

    pub async fn delivery_status(
        &self,
        context: CallContext,
        request: DeliveryReference,
    ) -> Result<DeliveryReceipt, BridgeHostError> {
        let _ = context;
        self.store.delivery(&request.bridge_id, &request.message_id)
    }

    pub async fn bridge_status(
        &self,
        context: CallContext,
        request: BridgeReference,
    ) -> Result<BridgeStatus, BridgeHostError> {
        let _ = context;
        self.store.status(&request.bridge_id, (self.clock)()?)
    }

    pub async fn stop_bridge(
        &self,
        context: CallContext,
        request: BridgeReference,
    ) -> Result<BridgeReceipt, BridgeHostError> {
        let _ = context;
        let _operation = self.operations.lock().await;
        self.stop_supervisor(&request.bridge_id);
        deactivate_package_instance(
            &self.active_package_instances,
            &self.credential_updates,
            &request.bridge_id,
        )
        .await;
        if let Some(package) = self.connections.write().await.remove(&request.bridge_id) {
            let _ = package.stop().await;
        }
        self.store.stop(&request.bridge_id, (self.clock)()?)
    }

    pub async fn suspend_bridge(
        &self,
        context: CallContext,
        request: BridgeReference,
    ) -> Result<BridgeStatus, BridgeHostError> {
        let _ = context;
        let _operation = self.operations.lock().await;
        self.stop_supervisor(&request.bridge_id);
        self.stop_connection(&request.bridge_id).await
    }
}

async fn route_inbound(
    trigger_inbox: &TriggerInboxImport,
    store: &BridgeStore,
    content: &Arc<dyn ContentStore>,
    operations: &Mutex<()>,
    clock: &Clock,
    context: CallContext,
    request: BridgeInbound,
) -> Result<TriggerIntent, BridgeHostError> {
    let _operation = operations.lock().await;
    validate_inbound(&request)?;
    for attachment in &request.attachments {
        content
            .owns(&request.bridge_id, attachment)
            .await
            .map_err(map_content_error)?;
    }
    let spec = store.spec(&request.bridge_id)?;
    let status = store.status(&request.bridge_id, clock()?)?;
    if !spec.desired_running
        || !matches!(status.lifecycle, BridgeLifecycle::Healthy)
        || !status
            .last_health
            .as_ref()
            .is_some_and(|health| health.can_receive && health.service_connected)
    {
        return Err(BridgeHostError::BridgeUnhealthy);
    }
    let message_json = normalized_inbound_message(&request)?;
    let attachment_handles = request
        .attachments
        .iter()
        .map(|attachment| attachment.content_handle.clone())
        .collect::<Vec<_>>();
    let receipt = trigger_inbox
        .enqueue(
            context,
            EnqueueTrigger {
                source: TriggerSource::Bridge,
                source_id: request.bridge_id.clone(),
                deduplication_key: request.external_event_id.clone(),
                target_channel_id: request.target_channel_id.clone(),
                lane: request.target_channel_id.clone(),
                mode: map_trigger_mode(&spec.ingress_mode)?,
                not_before_ms: request.received_at_ms,
                message_json: message_json.clone(),
                attachments: request
                    .attachments
                    .iter()
                    .map(|attachment| TriggerAttachment {
                        media_type: attachment.media_type.clone(),
                        name: attachment.name.clone(),
                        content_handle: attachment.content_handle.clone(),
                    })
                    .collect(),
            },
        )
        .await
        .map_err(map_trigger_error)?;
    store.record_inbound(
        &request,
        &TriggerIntent {
            source_id: request.bridge_id.clone(),
            deduplication_key: request.external_event_id.clone(),
            target_channel_id: request.target_channel_id.clone(),
            ingress_mode: spec.ingress_mode,
            message_json,
            attachment_handles,
            trigger_id: receipt.trigger_id,
            deduplicated: receipt.deduplicated,
            recorded_at_ms: receipt.recorded_at_ms,
        },
    )
}

fn validate_spec(spec: &BridgeSpec) -> Result<(), BridgeHostError> {
    if spec.bridge_id.trim().is_empty()
        || spec.package_id.trim().is_empty()
        || spec.display_name.trim().is_empty()
        || spec.health_interval_ms == 0
        || spec.credential_validation_interval_ms == 0
        || spec.restart_limit == 0
        || spec.restart_window_ms == 0
        || spec.alert_target.as_ref().is_some_and(|target| {
            target.channel_id.trim().is_empty()
                || target.channel_id.len() > 512
                || target.lane.trim().is_empty()
                || target.lane.len() > 512
        })
    {
        return Err(BridgeHostError::InvalidSpec);
    }
    validate_json_object(&spec.launch_json)?;
    validate_json_object(&spec.configuration_json)?;
    let mut methods = spec.authentication_methods.clone();
    methods.sort_by_key(|method| format!("{method:?}"));
    methods.dedup();
    if methods.len() != spec.authentication_methods.len()
        || matches!(spec.ingress_mode, BridgeIngressMode::Unknown { .. })
    {
        return Err(BridgeHostError::InvalidSpec);
    }
    Ok(())
}

fn validate_inbound(request: &BridgeInbound) -> Result<(), BridgeHostError> {
    if request.bridge_id.trim().is_empty()
        || request.external_event_id.trim().is_empty()
        || request.target_channel_id.trim().is_empty()
    {
        return Err(BridgeHostError::InvalidSpec);
    }
    validate_json_object(&request.sender_json)?;
    validate_json(&request.message_json)?;
    validate_attachments(&request.attachments)
}

fn validate_outbound(request: &BridgeOutbound) -> Result<(), BridgeHostError> {
    if request.bridge_id.trim().is_empty()
        || request.message_id.trim().is_empty()
        || request.idempotency_key.trim().is_empty()
    {
        return Err(BridgeHostError::InvalidSpec);
    }
    validate_json_object(&request.destination_json)?;
    validate_json(&request.message_json)?;
    validate_attachments(&request.attachments)
}

fn validate_content_import(
    request: &ImportBridgeContentRequest,
) -> Result<std::path::PathBuf, BridgeHostError> {
    if request.bridge_id.trim().is_empty()
        || request.bridge_id.len() > 512
        || request.import_id.trim().is_empty()
        || request.import_id.len() > 1024
        || request.source_path.trim().is_empty()
        || request.source_path.len() > 4096
        || request.media_type.trim().is_empty()
        || request.media_type.len() > 255
        || request
            .name
            .as_ref()
            .is_some_and(|name| name.trim().is_empty() || name.len() > 1024)
    {
        return Err(BridgeHostError::InvalidSpec);
    }
    let source_path = std::path::PathBuf::from(&request.source_path);
    if !source_path.is_absolute() {
        return Err(BridgeHostError::InvalidSpec);
    }
    Ok(source_path)
}

fn validate_attachments(attachments: &[BridgeAttachment]) -> Result<(), BridgeHostError> {
    if attachments.iter().any(|attachment| {
        attachment.media_type.trim().is_empty() || attachment.content_handle.trim().is_empty()
    }) {
        return Err(BridgeHostError::InvalidSpec);
    }
    Ok(())
}

fn validate_json(value: &str) -> Result<Value, BridgeHostError> {
    serde_json::from_str(value).map_err(|_| BridgeHostError::InvalidSpec)
}

fn validate_json_object(value: &str) -> Result<Map<String, Value>, BridgeHostError> {
    serde_json::from_str(value).map_err(|_| BridgeHostError::InvalidSpec)
}

fn normalized_inbound_message(request: &BridgeInbound) -> Result<String, BridgeHostError> {
    serde_json::to_string(&json!({
        "bridgeId": request.bridge_id,
        "externalEventId": request.external_event_id,
        "receivedAtMs": request.received_at_ms,
        "sender": validate_json(&request.sender_json)?,
        "message": validate_json(&request.message_json)?,
    }))
    .map_err(|_| BridgeHostError::InvalidSpec)
}

fn map_trigger_mode(mode: &BridgeIngressMode) -> Result<TriggerMode, BridgeHostError> {
    match mode {
        BridgeIngressMode::Queue => Ok(TriggerMode::Queue),
        BridgeIngressMode::Steer => Ok(TriggerMode::Steer),
        BridgeIngressMode::InterruptAndSteer => Ok(TriggerMode::InterruptAndSteer),
        BridgeIngressMode::Unknown { .. } => Err(BridgeHostError::InvalidSpec),
    }
}

fn map_package_error(error: BridgePackageError) -> BridgeHostError {
    match error {
        BridgePackageError::InvalidLaunch => BridgeHostError::InvalidSpec,
        BridgePackageError::LaunchFailed
        | BridgePackageError::ProtocolFailed
        | BridgePackageError::Timeout
        | BridgePackageError::Stopped => BridgeHostError::PackageProtocolFailed,
    }
}

fn map_credential_error(error: CredentialStoreError) -> BridgeHostError {
    match error {
        CredentialStoreError::UnknownHandle | CredentialStoreError::InvalidCredential => {
            BridgeHostError::CredentialRejected
        }
        CredentialStoreError::Unavailable => BridgeHostError::StorageUnavailable,
    }
}

fn map_content_error(error: ContentStoreError) -> BridgeHostError {
    match error {
        ContentStoreError::UnknownHandle | ContentStoreError::InvalidContent => {
            BridgeHostError::InvalidSpec
        }
        ContentStoreError::Unavailable => BridgeHostError::StorageUnavailable,
    }
}

fn credential_fingerprint(secret_json: &str) -> String {
    format!("{:x}", Sha256::digest(secret_json.as_bytes()))
}

fn valid_credential_fingerprint(fingerprint: &str) -> bool {
    fingerprint.len() == 64 && fingerprint.bytes().all(|byte| byte.is_ascii_hexdigit())
}

async fn activate_package_instance(
    active: &RwLock<HashMap<String, String>>,
    credential_updates: &Mutex<()>,
    bridge_id: &str,
    package_instance_id: String,
) {
    let _update = credential_updates.lock().await;
    active
        .write()
        .await
        .insert(bridge_id.to_owned(), package_instance_id);
}

async fn deactivate_package_instance(
    active: &RwLock<HashMap<String, String>>,
    credential_updates: &Mutex<()>,
    bridge_id: &str,
) {
    let _update = credential_updates.lock().await;
    active.write().await.remove(bridge_id);
}

fn map_trigger_error(error: ErasedCallError) -> BridgeHostError {
    match error {
        ErasedCallError::Domain { error_tag, .. } if error_tag == "DuplicateKeyConflict" => {
            BridgeHostError::DuplicateMessageConflict
        }
        _ => BridgeHostError::PackageProtocolFailed,
    }
}

fn system_time_ms() -> Result<u64, BridgeHostError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| BridgeHostError::StorageUnavailable)?;
    u64::try_from(duration.as_millis()).map_err(|_| BridgeHostError::StorageUnavailable)
}

pub mod generated {
    include!("../../generated/adapter/adapter.rs");
}

#[cfg(test)]
mod tests {
    use boxology_contract::{BoxId, CapabilityId};

    use super::{BridgeIngressMode, generated};

    #[test]
    fn contract_covers_supervision_auth_ingress_and_selected_delivery() {
        let descriptor = generated::implementation_descriptor();
        let names = descriptor
            .contract()
            .capabilities()
            .iter()
            .map(|capability| capability.id().name().as_str())
            .collect::<Vec<_>>();

        for required in [
            "list_bridges",
            "suspend_bridge",
            "reconcile_bridge",
            "begin_authentication",
            "validate_credentials",
            "accept_inbound",
            "import_content",
            "deliver_message",
        ] {
            assert!(
                names.contains(&required),
                "missing bridge concern: {required}"
            );
        }
        assert!(names.iter().all(|name| !name.contains("native_event")));
        assert_ne!(BridgeIngressMode::Queue, BridgeIngressMode::Steer);
        assert_ne!(
            BridgeIngressMode::Steer,
            BridgeIngressMode::InterruptAndSteer
        );
        assert_eq!(descriptor.imports().len(), 1);
        assert_eq!(
            descriptor.imports()[0].slot_id(),
            &BoxId::new("trigger-inbox").unwrap()
        );
        assert!(
            descriptor.imports()[0]
                .capabilities()
                .contains(&CapabilityId::new(
                    BoxId::new("trigger-inbox").unwrap(),
                    boxology_contract::CapabilityName::new("enqueue").unwrap()
                ))
        );
    }
}
