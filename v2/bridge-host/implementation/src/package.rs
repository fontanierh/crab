use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use nix::{
    sys::signal::{Signal, killpg},
    unistd::Pid,
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{Mutex, mpsc, oneshot},
    task::JoinHandle,
};
use uuid::Uuid;

use crate::{
    AuthenticationMethod, BridgeAttachment, BridgeHostError, BridgeInbound, BridgeIngressMode,
    BridgeOutbound, BridgeSpec, TriggerIntent,
};

const RPC_TIMEOUT: Duration = Duration::from_secs(30);
const STOP_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_PROTOCOL_LINE_BYTES: usize = 1024 * 1024;

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
}

/// Launches arbitrary agent-installed executables that implement Crab's bridge JSON-lines
/// protocol.
#[async_trait]
pub trait BridgePackageFactory: Send + Sync {
    async fn launch(
        &self,
        spec: &BridgeSpec,
        inbound: Arc<dyn BridgeInboundSink>,
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
    ) -> Result<Arc<dyn BridgePackage>, BridgePackageError> {
        let launch: ProcessLaunch = serde_json::from_str(&spec.launch_json)
            .map_err(|_| BridgePackageError::InvalidLaunch)?;
        validate_launch(&launch)?;
        let mut environment = BTreeMap::new();
        for name in &launch.environment_names {
            if name.trim().is_empty() {
                return Err(BridgePackageError::InvalidLaunch);
            }
            if let Ok(value) = std::env::var(name) {
                environment.insert(name.clone(), value);
            }
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
        let (inbound_sender, inbound_receiver) = mpsc::unbounded_channel();
        let reader = tokio::spawn(read_messages(
            BufReader::new(stdout),
            pending.clone(),
            inbound_sender,
            protocol_alive.clone(),
        ));
        let inbound_worker =
            tokio::spawn(process_inbound(inbound_receiver, writer.clone(), inbound));
        let connection = ProcessBridgePackage {
            child: Mutex::new(child),
            writer,
            pending,
            tasks: StdMutex::new(vec![reader, inbound_worker]),
            process_group,
            protocol_alive,
        };
        connection
            .call(
                "bridge/initialize",
                json!({
                    "protocolVersion": 1,
                    "bridgeId": spec.bridge_id,
                    "packageId": spec.package_id,
                    "configuration": parse_json(&spec.configuration_json)?,
                }),
            )
            .await?;
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

struct InboundCall {
    id: String,
    params: Value,
}

async fn read_messages(
    mut stdout: BufReader<ChildStdout>,
    pending: PendingCalls,
    inbound: mpsc::UnboundedSender<InboundCall>,
    protocol_alive: Arc<AtomicBool>,
) {
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
        if message.get("method").and_then(Value::as_str) == Some("bridge/inbound") {
            let params = message.get("params").cloned().unwrap_or(Value::Null);
            if inbound.send(InboundCall { id, params }).is_err() {
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

async fn process_inbound(
    mut inbound: mpsc::UnboundedReceiver<InboundCall>,
    writer: Arc<Mutex<ChildStdin>>,
    sink: Arc<dyn BridgeInboundSink>,
) {
    while let Some(call) = inbound.recv().await {
        let result = decode_inbound(call.params).map_err(|_| BridgeHostError::InvalidSpec);
        let response = match result {
            Ok(request) => match sink.accept(request).await {
                Ok(intent) => json!({
                    "jsonrpc": "2.0",
                    "id": call.id,
                    "result": encode_trigger_intent(&intent),
                }),
                Err(_) => ingress_error(call.id),
            },
            Err(_) => ingress_error(call.id),
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

fn ingress_error(id: String) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": "IngressRejected", "message": "bridge ingress rejected" },
    })
}

fn terminate_group(process_group: Option<i32>, signal: Signal) {
    if let Some(process_group) = process_group {
        let _ = killpg(Pid::from_raw(process_group), signal);
    }
}

fn validate_launch(launch: &ProcessLaunch) -> Result<(), BridgePackageError> {
    if !launch.executable.is_absolute()
        || !launch.working_directory.is_absolute()
        || !launch.working_directory.is_dir()
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
