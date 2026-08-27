use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use nix::{
    sys::signal::{Signal, killpg},
    unistd::Pid,
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, Command},
    sync::{Mutex, mpsc, oneshot},
    task::JoinHandle,
};
use uuid::Uuid;

use crate::{
    AuthenticationMethod, BridgeAttachment, BridgeHostError, BridgeInbound, BridgeIngressMode,
    BridgeOutbound, BridgeSpec, ContentUpload, MAX_CONTENT_BYTES, StoredContent, TriggerIntent,
};

const RPC_TIMEOUT: Duration = Duration::from_secs(30);
const STOP_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_PROTOCOL_LINE_BYTES: usize = 16 * 1024 * 1024;
// A queued call can approach the protocol line limit, so keep this deliberately small.
const PACKAGE_CALL_QUEUE_CAPACITY: usize = 16;
const PACKAGE_PROTOCOL_VERSION: u64 = 2;

type PendingCalls = Arc<Mutex<HashMap<String, oneshot::Sender<Result<Value, BridgePackageError>>>>>;

/// Failures at the dynamic bridge-package process boundary. Payloads are intentionally absent so
/// credential material cannot leak through diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BridgePackageError {
    InvalidLaunch,
    LaunchFailed,
    ProtocolFailed,
    Timeout,
    Stopped,
}

/// Health returned by a package's active service probe.
#[derive(Clone, Debug, PartialEq)]
pub struct PackageHealth {
    pub process_alive: bool,
    pub service_connected: bool,
    pub can_receive: bool,
    pub can_send: bool,
    pub credential_valid: bool,
    pub detail_json: String,
}

/// Renderable authentication challenge returned by a package.
#[derive(Clone, Debug, PartialEq)]
pub struct PackageChallenge {
    pub method: AuthenticationMethod,
    pub expires_at_ms: Option<u64>,
    pub presentation_json: String,
}

/// Secret credential material and non-secret metadata returned after authentication.
pub struct PackageCredential {
    pub secret_json: String,
    pub expires_at_ms: Option<u64>,
    pub account_hint: Option<String>,
    pub detail_json: String,
}

/// Active validation result for credential material.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackageCredentialValidation {
    pub valid: bool,
    pub expires_at_ms: Option<u64>,
    pub account_hint: Option<String>,
    pub detail_json: String,
}

/// Result of one idempotent selected-message delivery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackageDelivery {
    pub external_delivery_id: String,
    pub detail_json: String,
}

/// A package-owned credential mutation. The secret payload is deliberately not debug-printable.
pub struct BridgeCredentialUpdate {
    /// Must match the package instance's registered bridge.
    pub bridge_id: String,
    /// Lowercase SHA-256 of the exact canonical JSON snapshot previously received from Crab.
    pub previous_fingerprint: String,
    /// Complete fresh credential snapshot, retained only by the opaque credential provider.
    pub credential_json: String,
}

/// Host acknowledgement proving the new credential snapshot is durable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BridgeCredentialReceipt {
    /// Lowercase SHA-256 of the canonical snapshot committed by Crab.
    pub credential_fingerprint: String,
}

/// One live bridge package connection.
#[async_trait]
pub trait BridgePackage: Send + Sync {
    async fn health(
        &self,
        credential_json: Option<&str>,
    ) -> Result<PackageHealth, BridgePackageError>;
    async fn begin_authentication(
        &self,
        method: Option<&AuthenticationMethod>,
        context_json: &str,
    ) -> Result<PackageChallenge, BridgePackageError>;
    async fn submit_authentication(
        &self,
        challenge_id: &str,
        response_json: &str,
    ) -> Result<PackageCredential, BridgePackageError>;
    async fn validate_credentials(
        &self,
        credential_json: &str,
    ) -> Result<PackageCredentialValidation, BridgePackageError>;
    /// Tell the package its initial authentication result is durably stored. Live credential
    /// updates must not begin before this acknowledgement succeeds.
    async fn credential_committed(&self, credential_json: &str) -> Result<(), BridgePackageError>;
    async fn invalidate_credentials(&self, credential_json: &str)
    -> Result<(), BridgePackageError>;
    async fn deliver(
        &self,
        request: &BridgeOutbound,
        credential_json: Option<&str>,
    ) -> Result<PackageDelivery, BridgePackageError>;
    async fn stop(&self) -> Result<(), BridgePackageError>;
}

/// Durable package-to-host ingress callback. Success means the trigger inbox committed the event.
#[async_trait]
pub trait BridgeInboundSink: Send + Sync {
    async fn accept(&self, request: BridgeInbound) -> Result<TriggerIntent, BridgeHostError>;

    /// Persist private package bytes before an inbound event references the returned handle.
    async fn store_content(
        &self,
        _request: ContentUpload,
    ) -> Result<StoredContent, BridgeHostError> {
        Err(BridgeHostError::StorageUnavailable)
    }
}

/// Durable package-to-host credential callback. Success means the opaque store atomically
/// replaced the expected prior snapshot.
#[async_trait]
pub trait BridgeCredentialSink: Send + Sync {
    /// Compare and atomically persist one live credential mutation.
    async fn persist(
        &self,
        request: BridgeCredentialUpdate,
    ) -> Result<BridgeCredentialReceipt, BridgeHostError>;
}

/// Launches arbitrary agent-installed executables that implement Crab's bridge JSON-lines
/// protocol.
#[async_trait]
pub trait BridgePackageFactory: Send + Sync {
    async fn launch(
        &self,
        spec: &BridgeSpec,
        inbound: Arc<dyn BridgeInboundSink>,
        credentials: Arc<dyn BridgeCredentialSink>,
    ) -> Result<Arc<dyn BridgePackage>, BridgePackageError>;
}

/// Production dynamic package factory.
#[derive(Debug, Default)]
pub struct ProcessBridgePackageFactory;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProcessLaunch {
    executable: PathBuf,
    #[serde(default)]
    arguments: Vec<String>,
    working_directory: PathBuf,
    #[serde(default)]
    environment_names: Vec<String>,
}

#[async_trait]
impl BridgePackageFactory for ProcessBridgePackageFactory {
    async fn launch(
        &self,
        spec: &BridgeSpec,
        inbound: Arc<dyn BridgeInboundSink>,
        credentials: Arc<dyn BridgeCredentialSink>,
    ) -> Result<Arc<dyn BridgePackage>, BridgePackageError> {
        let launch: ProcessLaunch = serde_json::from_str(&spec.launch_json)
            .map_err(|_| BridgePackageError::InvalidLaunch)?;
        validate_launch(&launch)?;
        let mut environment = BTreeMap::new();
        for name in &launch.environment_names {
            let value = std::env::var(name).map_err(|_| BridgePackageError::InvalidLaunch)?;
            environment.insert(name.clone(), value);
        }
        let mut command = Command::new(&launch.executable);
        command
            .args(&launch.arguments)
            .current_dir(&launch.working_directory)
            .env_clear()
            .envs(environment)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);
        let mut child = command
            .spawn()
            .map_err(|_| BridgePackageError::LaunchFailed)?;
        let process_group = child
            .id()
            .map(i32::try_from)
            .transpose()
            .map_err(|_| BridgePackageError::LaunchFailed)?;
        let stdin = child.stdin.take().ok_or(BridgePackageError::LaunchFailed)?;
        let stdout = child
            .stdout
            .take()
            .ok_or(BridgePackageError::LaunchFailed)?;
        let writer = Arc::new(Mutex::new(stdin));
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let protocol_alive = Arc::new(AtomicBool::new(true));
        let (package_sender, package_receiver) = package_call_channel();
        let reader = tokio::spawn(read_messages(
            BufReader::new(stdout),
            pending.clone(),
            package_sender,
            protocol_alive.clone(),
        ));
        let inbound_worker = tokio::spawn(process_package_calls(
            package_receiver,
            writer.clone(),
            inbound,
            credentials,
        ));
        let connection = ProcessBridgePackage {
            child: Mutex::new(child),
            writer,
            pending,
            tasks: StdMutex::new(vec![reader, inbound_worker]),
            process_group,
            protocol_alive,
        };
        let initialized: WireInitialize = decode(
            connection
                .call(
                    "bridge/initialize",
                    json!({
                        "protocolVersion": PACKAGE_PROTOCOL_VERSION,
                        "bridgeId": spec.bridge_id,
                        "packageId": spec.package_id,
                        "configuration": parse_json(&spec.configuration_json)?,
                    }),
                )
                .await?,
        )?;
        if initialized.protocol_version != PACKAGE_PROTOCOL_VERSION {
            return Err(BridgePackageError::ProtocolFailed);
        }
        Ok(Arc::new(connection))
    }
}

struct ProcessBridgePackage {
    child: Mutex<Child>,
    writer: Arc<Mutex<ChildStdin>>,
    pending: PendingCalls,
    tasks: StdMutex<Vec<JoinHandle<()>>>,
    process_group: Option<i32>,
    protocol_alive: Arc<AtomicBool>,
}

impl ProcessBridgePackage {
    async fn call(&self, method: &str, params: Value) -> Result<Value, BridgePackageError> {
        if !self.protocol_alive.load(Ordering::Acquire) {
            return Err(BridgePackageError::ProtocolFailed);
        }
        if self
            .child
            .lock()
            .await
            .try_wait()
            .map_err(|_| BridgePackageError::ProtocolFailed)?
            .is_some()
        {
            return Err(BridgePackageError::Stopped);
        }
        let id = Uuid::new_v4().to_string();
        let (sender, receiver) = oneshot::channel();
        self.pending.lock().await.insert(id.clone(), sender);
        let message = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        if let Err(error) = write_message(&self.writer, &message).await {
            self.pending.lock().await.remove(&id);
            return Err(error);
        }
        match tokio::time::timeout(RPC_TIMEOUT, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(BridgePackageError::Stopped),
            Err(_) => {
                self.pending.lock().await.remove(&id);
                Err(BridgePackageError::Timeout)
            }
        }
    }

    async fn terminate(&self) -> Result<(), BridgePackageError> {
        terminate_group(self.process_group, Signal::SIGTERM);
        let result = match tokio::time::timeout(STOP_TIMEOUT, self.child.lock().await.wait()).await
        {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(_)) => Err(BridgePackageError::Stopped),
            Err(_) => {
                terminate_group(self.process_group, Signal::SIGKILL);
                self.child
                    .lock()
                    .await
                    .wait()
                    .await
                    .map(|_| ())
                    .map_err(|_| BridgePackageError::Stopped)
            }
        };
        self.abort_tasks();
        fail_pending(&self.pending, BridgePackageError::Stopped).await;
        result
    }

    fn abort_tasks(&self) {
        if let Ok(mut tasks) = self.tasks.lock() {
            for task in tasks.drain(..) {
                task.abort();
            }
        }
    }
}

#[async_trait]
impl BridgePackage for ProcessBridgePackage {
    async fn health(
        &self,
        credential_json: Option<&str>,
    ) -> Result<PackageHealth, BridgePackageError> {
        decode(
            self.call(
                "bridge/health",
                json!({ "credential": optional_json(credential_json)? }),
            )
            .await?,
        )
    }

    async fn begin_authentication(
        &self,
        method: Option<&AuthenticationMethod>,
        context_json: &str,
    ) -> Result<PackageChallenge, BridgePackageError> {
        let response: WireChallenge = decode(
            self.call(
                "bridge/auth/begin",
                json!({
                    "method": method.map(authentication_method_tag).transpose()?,
                    "context": parse_json(context_json)?,
                }),
            )
            .await?,
        )?;
        Ok(PackageChallenge {
            method: parse_authentication_method(&response.method)?,
            expires_at_ms: response.expires_at_ms,
            presentation_json: serde_json::to_string(&response.presentation)
                .map_err(|_| BridgePackageError::ProtocolFailed)?,
        })
    }

    async fn submit_authentication(
        &self,
        challenge_id: &str,
        response_json: &str,
    ) -> Result<PackageCredential, BridgePackageError> {
        let response: WireCredential = decode(
            self.call(
                "bridge/auth/submit",
                json!({
                    "challengeId": challenge_id,
                    "response": parse_json(response_json)?,
                }),
            )
            .await?,
        )?;
        Ok(PackageCredential {
            secret_json: serde_json::to_string(&response.credential)
                .map_err(|_| BridgePackageError::ProtocolFailed)?,
            expires_at_ms: response.expires_at_ms,
            account_hint: response.account_hint,
            detail_json: serde_json::to_string(&response.detail)
                .map_err(|_| BridgePackageError::ProtocolFailed)?,
        })
    }

    async fn validate_credentials(
        &self,
        credential_json: &str,
    ) -> Result<PackageCredentialValidation, BridgePackageError> {
        let response: WireValidation = decode(
            self.call(
                "bridge/auth/validate",
                json!({ "credential": parse_json(credential_json)? }),
            )
            .await?,
        )?;
        Ok(PackageCredentialValidation {
            valid: response.valid,
            expires_at_ms: response.expires_at_ms,
            account_hint: response.account_hint,
            detail_json: serde_json::to_string(&response.detail)
                .map_err(|_| BridgePackageError::ProtocolFailed)?,
        })
    }

    async fn credential_committed(&self, credential_json: &str) -> Result<(), BridgePackageError> {
        self.call(
            "bridge/auth/committed",
            json!({ "credential": parse_json(credential_json)? }),
        )
        .await?;
        Ok(())
    }

    async fn invalidate_credentials(
        &self,
        credential_json: &str,
    ) -> Result<(), BridgePackageError> {
        self.call(
            "bridge/auth/invalidate",
            json!({ "credential": parse_json(credential_json)? }),
        )
        .await?;
        Ok(())
    }

    async fn deliver(
        &self,
        request: &BridgeOutbound,
        credential_json: Option<&str>,
    ) -> Result<PackageDelivery, BridgePackageError> {
        let response: WireDelivery = decode(
            self.call(
                "bridge/deliver",
                json!({
                    "messageId": request.message_id,
                    "destination": parse_json(&request.destination_json)?,
                    "message": parse_json(&request.message_json)?,
                    "attachments": request.attachments.iter().map(|attachment| json!({
                        "mediaType": attachment.media_type,
                        "name": attachment.name,
                        "contentHandle": attachment.content_handle,
                    })).collect::<Vec<_>>(),
                    "idempotencyKey": request.idempotency_key,
                    "credential": optional_json(credential_json)?,
                }),
            )
            .await?,
        )?;
        Ok(PackageDelivery {
            external_delivery_id: response.external_delivery_id,
            detail_json: serde_json::to_string(&response.detail)
                .map_err(|_| BridgePackageError::ProtocolFailed)?,
        })
    }

    async fn stop(&self) -> Result<(), BridgePackageError> {
        let _ = tokio::time::timeout(STOP_TIMEOUT, self.call("bridge/shutdown", json!({}))).await;
        self.terminate().await
    }
}

impl Drop for ProcessBridgePackage {
    fn drop(&mut self) {
        self.abort_tasks();
        terminate_group(self.process_group, Signal::SIGKILL);
        if let Ok(mut child) = self.child.try_lock() {
            let _ = child.start_kill();
        }
    }
}

enum PackageCall {
    Inbound { id: String, params: Value },
    ContentPut { id: String, params: Value },
    CredentialUpdate { id: String, params: Value },
}

fn package_call_channel() -> (mpsc::Sender<PackageCall>, mpsc::Receiver<PackageCall>) {
    mpsc::channel(PACKAGE_CALL_QUEUE_CAPACITY)
}

async fn read_messages<R>(
    mut stdout: BufReader<R>,
    pending: PendingCalls,
    package_calls: mpsc::Sender<PackageCall>,
    protocol_alive: Arc<AtomicBool>,
) where
    R: AsyncRead + Unpin,
{
    loop {
        let mut line = Vec::new();
        let read = (&mut stdout)
            .take((MAX_PROTOCOL_LINE_BYTES + 1) as u64)
            .read_until(b'\n', &mut line)
            .await;
        let Ok(bytes) = read else { break };
        if bytes == 0 || bytes > MAX_PROTOCOL_LINE_BYTES || line.last() != Some(&b'\n') {
            break;
        }
        let Ok(message) = serde_json::from_slice::<Value>(&line) else {
            break;
        };
        let Some(id) = message.get("id").and_then(Value::as_str).map(str::to_owned) else {
            continue;
        };
        let params = message.get("params").cloned().unwrap_or(Value::Null);
        let call = match message.get("method").and_then(Value::as_str) {
            Some("bridge/inbound") => Some(PackageCall::Inbound {
                id: id.clone(),
                params,
            }),
            Some("bridge/content/put") => Some(PackageCall::ContentPut {
                id: id.clone(),
                params,
            }),
            Some("bridge/credential/update") => Some(PackageCall::CredentialUpdate {
                id: id.clone(),
                params,
            }),
            _ => None,
        };
        if let Some(call) = call {
            if package_calls.send(call).await.is_err() {
                break;
            }
            continue;
        }
        let Some(sender) = pending.lock().await.remove(&id) else {
            continue;
        };
        let result = if message.get("error").is_some() {
            Err(BridgePackageError::ProtocolFailed)
        } else {
            message
                .get("result")
                .cloned()
                .ok_or(BridgePackageError::ProtocolFailed)
        };
        let _ = sender.send(result);
    }
    protocol_alive.store(false, Ordering::Release);
    fail_pending(&pending, BridgePackageError::Stopped).await;
}

async fn process_package_calls(
    mut calls: mpsc::Receiver<PackageCall>,
    writer: Arc<Mutex<ChildStdin>>,
    inbound: Arc<dyn BridgeInboundSink>,
    credentials: Arc<dyn BridgeCredentialSink>,
) {
    while let Some(call) = calls.recv().await {
        let response = match call {
            PackageCall::Inbound { id, params } => {
                match decode_inbound(params).map_err(|_| BridgeHostError::InvalidSpec) {
                    Ok(request) => match inbound.accept(request).await {
                        Ok(intent) => json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "result": encode_trigger_intent(&intent),
                        }),
                        Err(_) => package_call_error(id, "IngressRejected"),
                    },
                    Err(_) => package_call_error(id, "IngressRejected"),
                }
            }
            PackageCall::CredentialUpdate { id, params } => {
                match decode_credential_update(params) {
                    Ok(request) => match credentials.persist(request).await {
                        Ok(receipt) => json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "result": {
                                "credentialFingerprint": receipt.credential_fingerprint,
                            },
                        }),
                        Err(_) => package_call_error(id, "CredentialUpdateRejected"),
                    },
                    Err(_) => package_call_error(id, "CredentialUpdateRejected"),
                }
            }
            PackageCall::ContentPut { id, params } => match decode_content_upload(params) {
                Ok(request) => match inbound.store_content(request).await {
                    Ok(content) => json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": {
                            "contentHandle": content.attachment.content_handle,
                            "size": content.size,
                            "sha256": content.sha256,
                        },
                    }),
                    Err(_) => package_call_error(id, "ContentRejected"),
                },
                Err(_) => package_call_error(id, "ContentRejected"),
            },
        };
        if write_message(&writer, &response).await.is_err() {
            break;
        }
    }
}

async fn write_message(
    writer: &Arc<Mutex<ChildStdin>>,
    message: &Value,
) -> Result<(), BridgePackageError> {
    let mut bytes = serde_json::to_vec(message).map_err(|_| BridgePackageError::ProtocolFailed)?;
    bytes.push(b'\n');
    let mut writer = writer.lock().await;
    writer
        .write_all(&bytes)
        .await
        .map_err(|_| BridgePackageError::Stopped)?;
    writer
        .flush()
        .await
        .map_err(|_| BridgePackageError::Stopped)
}

async fn fail_pending(pending: &PendingCalls, error: BridgePackageError) {
    let calls = pending
        .lock()
        .await
        .drain()
        .map(|(_, call)| call)
        .collect::<Vec<_>>();
    for call in calls {
        let _ = call.send(Err(error));
    }
}

fn package_call_error(id: String, code: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": "bridge package call rejected" },
    })
}

fn terminate_group(process_group: Option<i32>, signal: Signal) {
    if let Some(process_group) = process_group {
        let _ = killpg(Pid::from_raw(process_group), signal);
    }
}

fn validate_launch(launch: &ProcessLaunch) -> Result<(), BridgePackageError> {
    let mut environment_names = HashSet::new();
    if !launch.executable.is_absolute()
        || !launch.working_directory.is_absolute()
        || !launch.working_directory.is_dir()
        || launch.environment_names.iter().any(|name| {
            name.trim().is_empty() || name.contains(['=', '\0']) || !environment_names.insert(name)
        })
    {
        return Err(BridgePackageError::InvalidLaunch);
    }
    Ok(())
}

fn parse_json(value: &str) -> Result<Value, BridgePackageError> {
    serde_json::from_str(value).map_err(|_| BridgePackageError::ProtocolFailed)
}

fn optional_json(value: Option<&str>) -> Result<Value, BridgePackageError> {
    value.map_or(Ok(Value::Null), parse_json)
}

fn decode<T: for<'de> Deserialize<'de>>(value: Value) -> Result<T, BridgePackageError> {
    serde_json::from_value(value).map_err(|_| BridgePackageError::ProtocolFailed)
}

fn decode_inbound(value: Value) -> Result<BridgeInbound, BridgePackageError> {
    let inbound: WireInbound = decode(value)?;
    Ok(BridgeInbound {
        bridge_id: inbound.bridge_id,
        external_event_id: inbound.external_event_id,
        received_at_ms: inbound.received_at_ms,
        target_channel_id: inbound.target_channel_id,
        sender_json: serde_json::to_string(&inbound.sender)
            .map_err(|_| BridgePackageError::ProtocolFailed)?,
        message_json: serde_json::to_string(&inbound.message)
            .map_err(|_| BridgePackageError::ProtocolFailed)?,
        attachments: inbound
            .attachments
            .into_iter()
            .map(|attachment| BridgeAttachment {
                media_type: attachment.media_type,
                name: attachment.name,
                content_handle: attachment.content_handle,
            })
            .collect(),
    })
}

fn decode_credential_update(value: Value) -> Result<BridgeCredentialUpdate, BridgePackageError> {
    let update: WireCredentialUpdate = decode(value)?;
    Ok(BridgeCredentialUpdate {
        bridge_id: update.bridge_id,
        previous_fingerprint: update.previous_fingerprint,
        credential_json: serde_json::to_string(&update.credential)
            .map_err(|_| BridgePackageError::ProtocolFailed)?,
    })
}

fn decode_content_upload(value: Value) -> Result<ContentUpload, BridgePackageError> {
    let upload: WireContentUpload = decode(value)?;
    let maximum_encoded = MAX_CONTENT_BYTES.div_ceil(3) * 4;
    if upload.bytes_base64.len() > maximum_encoded {
        return Err(BridgePackageError::ProtocolFailed);
    }
    let bytes = BASE64
        .decode(upload.bytes_base64)
        .map_err(|_| BridgePackageError::ProtocolFailed)?;
    if bytes.len() > MAX_CONTENT_BYTES {
        return Err(BridgePackageError::ProtocolFailed);
    }
    Ok(ContentUpload {
        bridge_id: upload.bridge_id,
        external_event_id: upload.external_event_id,
        media_type: upload.media_type,
        name: upload.name,
        bytes,
    })
}

fn encode_trigger_intent(intent: &TriggerIntent) -> Value {
    json!({
        "sourceId": intent.source_id,
        "deduplicationKey": intent.deduplication_key,
        "targetChannelId": intent.target_channel_id,
        "ingressMode": match intent.ingress_mode {
            BridgeIngressMode::Queue => "queue",
            BridgeIngressMode::Steer => "steer",
            BridgeIngressMode::InterruptAndSteer => "interruptAndSteer",
            BridgeIngressMode::Unknown { .. } => "unknown",
        },
        "message": parse_json(&intent.message_json).unwrap_or(Value::Null),
        "attachmentHandles": intent.attachment_handles,
        "triggerId": intent.trigger_id,
        "deduplicated": intent.deduplicated,
        "recordedAtMs": intent.recorded_at_ms,
    })
}

fn authentication_method_tag(
    method: &AuthenticationMethod,
) -> Result<&'static str, BridgePackageError> {
    match method {
        AuthenticationMethod::QrCode => Ok("qrCode"),
        AuthenticationMethod::PhoneCode => Ok("phoneCode"),
        AuthenticationMethod::OAuth => Ok("oauth"),
        AuthenticationMethod::Browser => Ok("browser"),
        AuthenticationMethod::Terminal => Ok("terminal"),
        AuthenticationMethod::Manual => Ok("manual"),
        AuthenticationMethod::Unknown { .. } => Err(BridgePackageError::ProtocolFailed),
    }
}

fn parse_authentication_method(value: &str) -> Result<AuthenticationMethod, BridgePackageError> {
    match value {
        "qrCode" => Ok(AuthenticationMethod::QrCode),
        "phoneCode" => Ok(AuthenticationMethod::PhoneCode),
        "oauth" => Ok(AuthenticationMethod::OAuth),
        "browser" => Ok(AuthenticationMethod::Browser),
        "terminal" => Ok(AuthenticationMethod::Terminal),
        "manual" => Ok(AuthenticationMethod::Manual),
        _ => Err(BridgePackageError::ProtocolFailed),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WireChallenge {
    method: String,
    expires_at_ms: Option<u64>,
    presentation: Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WireInitialize {
    protocol_version: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WireCredential {
    credential: Value,
    expires_at_ms: Option<u64>,
    account_hint: Option<String>,
    detail: Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WireValidation {
    valid: bool,
    expires_at_ms: Option<u64>,
    account_hint: Option<String>,
    detail: Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WireDelivery {
    external_delivery_id: String,
    detail: Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WireInbound {
    bridge_id: String,
    external_event_id: String,
    received_at_ms: u64,
    target_channel_id: String,
    sender: Value,
    message: Value,
    #[serde(default)]
    attachments: Vec<WireAttachment>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WireCredentialUpdate {
    bridge_id: String,
    previous_fingerprint: String,
    credential: Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WireContentUpload {
    bridge_id: String,
    external_event_id: String,
    media_type: String,
    name: Option<String>,
    bytes_base64: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WireAttachment {
    media_type: String,
    name: Option<String>,
    content_handle: String,
}

impl<'de> Deserialize<'de> for PackageHealth {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct Wire {
            process_alive: bool,
            service_connected: bool,
            can_receive: bool,
            can_send: bool,
            credential_valid: bool,
            detail: Value,
        }
        let wire = Wire::deserialize(deserializer)?;
        Ok(Self {
            process_alive: wire.process_alive,
            service_connected: wire.service_connected,
            can_receive: wire.can_receive,
            can_send: wire.can_send,
            credential_valid: wire.credential_valid,
            detail_json: serde_json::to_string(&wire.detail).map_err(serde::de::Error::custom)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::AsyncWriteExt;

    use super::*;

    #[tokio::test]
    async fn package_call_queue_applies_backpressure_at_capacity() {
        let mut input = Vec::new();
        for index in 0..=PACKAGE_CALL_QUEUE_CAPACITY {
            let mut message = serde_json::to_vec(&json!({
                "jsonrpc": "2.0",
                "id": format!("package-{index}"),
                "method": "bridge/inbound",
                "params": null,
            }))
            .expect("serialize package call");
            message.push(b'\n');
            input.extend(message);
        }

        let (mut package_stdout, host_stdout) = tokio::io::duplex(input.len() + 1);
        package_stdout
            .write_all(&input)
            .await
            .expect("write package calls");
        package_stdout
            .shutdown()
            .await
            .expect("close package stdout");

        let pending: PendingCalls = Arc::new(Mutex::new(HashMap::new()));
        let protocol_alive = Arc::new(AtomicBool::new(true));
        let (sender, mut receiver) = package_call_channel();
        assert_eq!(receiver.max_capacity(), PACKAGE_CALL_QUEUE_CAPACITY);

        let reader = tokio::spawn(read_messages(
            BufReader::new(host_stdout),
            pending,
            sender,
            protocol_alive.clone(),
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            while receiver.len() != PACKAGE_CALL_QUEUE_CAPACITY {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("queue reaches capacity");

        assert!(protocol_alive.load(Ordering::Acquire));
        assert!(!reader.is_finished());
        let Some(PackageCall::Inbound { id, .. }) = receiver.recv().await else {
            panic!("expected first inbound package call");
        };
        assert_eq!(id, "package-0");

        tokio::time::timeout(Duration::from_secs(1), reader)
            .await
            .expect("reader resumes after queue space opens")
            .expect("reader task completes");
        assert!(!protocol_alive.load(Ordering::Acquire));
        assert_eq!(receiver.len(), PACKAGE_CALL_QUEUE_CAPACITY);
    }
}
