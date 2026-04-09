use std::collections::{BTreeMap, HashMap};
use std::fs;
#[cfg(all(not(test), not(coverage)))]
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::str::FromStr;
#[cfg(all(not(test), not(coverage)))]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(all(not(test), not(coverage)))]
use std::sync::{mpsc, Arc};
#[cfg(all(not(test), not(coverage)))]
use std::thread;
#[cfg(all(not(test), not(coverage)))]
use std::time::Duration;

use crab_core::config::DEFAULT_WORKSPACE_ROOT;
use crab_core::{CrabError, CrabResult};
#[cfg(test)]
use crab_discord::GatewayConversationKind;
use crab_discord::{
    CrabdInboundFrame, CrabdOutboundOp, CrabdOutboundReceipt, CrabdOutboundReceiptStatus,
    GatewayMessage,
};
#[cfg(test)]
use std::collections::VecDeque;
#[cfg(test)]
use std::path::Path;

const DEFAULT_IDLE_SLEEP_MS: u64 = 50;
const DEFAULT_CONNECTOR_RETRY_MAX_ATTEMPTS: u8 = 3;
const DEFAULT_CONNECTOR_RETRY_BACKOFF_MS: u64 = 500;
const DEFAULT_CONNECTOR_RETRY_MAX_BACKOFF_MS: u64 = 30_000;
const DEFAULT_MESSAGE_ID_MAP_FILE: &str = "state/connector_message_ids.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ConnectorRetryPolicy {
    max_attempts: u8,
    retry_backoff_ms: u64,
    max_retry_backoff_ms: u64,
}

impl Default for ConnectorRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_CONNECTOR_RETRY_MAX_ATTEMPTS,
            retry_backoff_ms: DEFAULT_CONNECTOR_RETRY_BACKOFF_MS,
            max_retry_backoff_ms: DEFAULT_CONNECTOR_RETRY_MAX_BACKOFF_MS,
        }
    }
}

impl ConnectorRetryPolicy {
    fn validate(self) -> CrabResult<Self> {
        if self.max_attempts == 0 {
            return Err(CrabError::InvalidConfig {
                key: "CRAB_CONNECTOR_RETRY_MAX_ATTEMPTS",
                value: self.max_attempts.to_string(),
                reason: "must be greater than 0",
            });
        }
        if self.retry_backoff_ms == 0 {
            return Err(CrabError::InvalidConfig {
                key: "CRAB_CONNECTOR_RETRY_BACKOFF_MS",
                value: self.retry_backoff_ms.to_string(),
                reason: "must be greater than 0",
            });
        }
        if self.max_retry_backoff_ms == 0 {
            return Err(CrabError::InvalidConfig {
                key: "CRAB_CONNECTOR_RETRY_MAX_BACKOFF_MS",
                value: self.max_retry_backoff_ms.to_string(),
                reason: "must be greater than 0",
            });
        }
        if self.retry_backoff_ms > self.max_retry_backoff_ms {
            return Err(CrabError::InvalidConfig {
                key: "CRAB_CONNECTOR_RETRY_BACKOFF_MS",
                value: self.retry_backoff_ms.to_string(),
                reason: "must be less than or equal to CRAB_CONNECTOR_RETRY_MAX_BACKOFF_MS",
            });
        }
        Ok(self)
    }

    fn backoff_ms(self, error: &DiscordApiError) -> u64 {
        let requested = match error {
            DiscordApiError::RateLimited {
                retry_after_ms: Some(retry_after_ms),
                ..
            } if *retry_after_ms > 0 => *retry_after_ms,
            _ => self.retry_backoff_ms,
        };
        requested.min(self.max_retry_backoff_ms)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ConnectorConfig {
    crabd_path: String,
    discord_token: String,
    idle_sleep_ms: u64,
    retry_policy: ConnectorRetryPolicy,
    message_id_map_path: PathBuf,
}

impl ConnectorConfig {
    fn from_map_and_args(values: &HashMap<String, String>, args: &[String]) -> CrabResult<Self> {
        let discord_token =
            values
                .get("CRAB_DISCORD_TOKEN")
                .cloned()
                .ok_or(CrabError::MissingConfig {
                    key: "CRAB_DISCORD_TOKEN",
                })?;

        let crabd_path = parse_crabd_path(args)?;
        let idle_sleep_ms = parse_optional_u64(
            values.get("CRAB_CONNECTOR_IDLE_SLEEP_MS"),
            "CRAB_CONNECTOR_IDLE_SLEEP_MS",
            DEFAULT_IDLE_SLEEP_MS,
        )?;

        let retry_policy = ConnectorRetryPolicy {
            max_attempts: parse_optional_u8(
                values.get("CRAB_CONNECTOR_RETRY_MAX_ATTEMPTS"),
                "CRAB_CONNECTOR_RETRY_MAX_ATTEMPTS",
                DEFAULT_CONNECTOR_RETRY_MAX_ATTEMPTS,
            )?,
            retry_backoff_ms: parse_optional_u64(
                values.get("CRAB_CONNECTOR_RETRY_BACKOFF_MS"),
                "CRAB_CONNECTOR_RETRY_BACKOFF_MS",
                DEFAULT_CONNECTOR_RETRY_BACKOFF_MS,
            )?,
            max_retry_backoff_ms: parse_optional_u64(
                values.get("CRAB_CONNECTOR_RETRY_MAX_BACKOFF_MS"),
                "CRAB_CONNECTOR_RETRY_MAX_BACKOFF_MS",
                DEFAULT_CONNECTOR_RETRY_MAX_BACKOFF_MS,
            )?,
        }
        .validate()?;

        let message_id_map_path = resolve_message_id_map_path(values)?;

        Ok(Self {
            crabd_path,
            discord_token,
            idle_sleep_ms,
            retry_policy,
            message_id_map_path,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DiscordApiError {
    Retryable {
        message: String,
    },
    RateLimited {
        retry_after_ms: Option<u64>,
        message: String,
    },
    Fatal {
        message: String,
    },
}

impl DiscordApiError {
    fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable { .. } | Self::RateLimited { .. })
    }

    fn message(&self) -> &str {
        match self {
            Self::Retryable { message }
            | Self::RateLimited { message, .. }
            | Self::Fatal { message } => message,
        }
    }
}

trait DiscordIo {
    fn next_gateway_message(&mut self) -> CrabResult<Option<GatewayMessage>>;

    fn post_message(&mut self, channel_id: &str, content: &str) -> Result<String, DiscordApiError>;

    fn edit_message(
        &mut self,
        channel_id: &str,
        message_id: &str,
        content: &str,
    ) -> Result<(), DiscordApiError>;

    fn wait_for_retry(&mut self, backoff_ms: u64) -> CrabResult<()>;
}

trait CrabdIo {
    fn next_outbound_op(&mut self) -> CrabResult<Option<CrabdOutboundOp>>;
    fn send_inbound_frame(&mut self, frame: &CrabdInboundFrame) -> CrabResult<()>;
}

trait ConnectorLoopControl {
    fn should_shutdown(&self) -> bool;
    fn sleep_idle(&mut self, idle_sleep_ms: u64) -> CrabResult<()>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct ConnectorLoopStats {
    gateway_messages_forwarded: u64,
    outbound_operations_processed: u64,
    retry_waits: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DeliveryIdMapStore {
    path: PathBuf,
    map: BTreeMap<String, String>,
}

impl DeliveryIdMapStore {
    fn load(path: impl Into<PathBuf>) -> CrabResult<Self> {
        let path = path.into();
        let map = if path.exists() {
            let raw = fs::read_to_string(&path).map_err(|error| CrabError::Io {
                context: "connector_message_id_map_read",
                path: Some(path.to_string_lossy().to_string()),
                message: error.to_string(),
            })?;
            if raw.trim().is_empty() {
                BTreeMap::new()
            } else {
                serde_json::from_str::<BTreeMap<String, String>>(&raw).map_err(|error| {
                    CrabError::Serialization {
                        context: "connector_message_id_map_parse",
                        path: Some(path.to_string_lossy().to_string()),
                        message: error.to_string(),
                    }
                })?
            }
        } else {
            BTreeMap::new()
        };

        Ok(Self { path, map })
    }

    fn lookup_discord_message_id(&self, channel_id: &str, delivery_id: &str) -> Option<String> {
        self.map.get(&Self::key(channel_id, delivery_id)).cloned()
    }

    fn remember_post_mapping(
        &mut self,
        channel_id: &str,
        delivery_id: &str,
        discord_message_id: &str,
    ) -> CrabResult<()> {
        self.map.insert(
            Self::key(channel_id, delivery_id),
            discord_message_id.to_string(),
        );
        self.persist()
    }

    fn persist(&self) -> CrabResult<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|error| CrabError::Io {
                context: "connector_message_id_map_parent_mkdir",
                path: Some(parent.to_string_lossy().to_string()),
                message: error.to_string(),
            })?;
        }

        let payload = serde_json::to_string_pretty(&self.map).expect(
            "serializing connector message-id map from BTreeMap<String, String> should not fail",
        );

        let tmp_path = self.path.with_extension("tmp");
        fs::write(&tmp_path, payload).map_err(|error| CrabError::Io {
            context: "connector_message_id_map_tmp_write",
            path: Some(tmp_path.to_string_lossy().to_string()),
            message: error.to_string(),
        })?;
        fs::rename(&tmp_path, &self.path).map_err(|error| CrabError::Io {
            context: "connector_message_id_map_rename",
            path: Some(self.path.to_string_lossy().to_string()),
            message: error.to_string(),
        })?;
        Ok(())
    }

    fn key(channel_id: &str, message_id: &str) -> String {
        format!("{channel_id}:{message_id}")
    }
}

fn parse_outbound_op_line(line: &str) -> CrabResult<CrabdOutboundOp> {
    let op = serde_json::from_str::<CrabdOutboundOp>(line).map_err(|error| {
        CrabError::Serialization {
            context: "connector_parse_crabd_outbound_op",
            path: None,
            message: error.to_string(),
        }
    })?;

    match &op {
        CrabdOutboundOp::Post {
            op_id,
            channel_id,
            delivery_id,
            content,
        }
        | CrabdOutboundOp::Edit {
            op_id,
            channel_id,
            delivery_id,
            content,
        } => {
            ensure_non_empty("connector_parse_crabd_outbound_op", "op_id", op_id)?;
            ensure_non_empty(
                "connector_parse_crabd_outbound_op",
                "channel_id",
                channel_id,
            )?;
            ensure_non_empty(
                "connector_parse_crabd_outbound_op",
                "delivery_id",
                delivery_id,
            )?;
            ensure_non_empty("connector_parse_crabd_outbound_op", "content", content)?;
        }
    }

    Ok(op)
}

fn run_connector_loop<D, C>(
    discord: &mut D,
    crabd: &mut C,
    delivery_id_map_store: &mut DeliveryIdMapStore,
    retry_policy: ConnectorRetryPolicy,
    idle_sleep_ms: u64,
    control: &mut dyn ConnectorLoopControl,
) -> CrabResult<ConnectorLoopStats>
where
    D: DiscordIo,
    C: CrabdIo,
{
    let mut stats = ConnectorLoopStats::default();

    while !control.should_shutdown() {
        let mut progressed = false;

        while let Some(message) = discord.next_gateway_message()? {
            tracing::debug!(
                message_id = %message.message_id,
                channel_id = %message.channel_id,
                author_id = %message.author_id,
                author_is_bot = message.author_is_bot,
                kind = ?message.conversation_kind,
                "forwarding discord gateway message to crabd"
            );
            let frame = CrabdInboundFrame::GatewayMessage(message);
            crabd.send_inbound_frame(&frame)?;
            progressed = true;
            stats.gateway_messages_forwarded = stats.gateway_messages_forwarded.saturating_add(1);
        }

        while let Some(op) = crabd.next_outbound_op()? {
            match &op {
                CrabdOutboundOp::Post {
                    op_id,
                    channel_id,
                    delivery_id,
                    content,
                } => {
                    #[rustfmt::skip]
                    tracing::debug!(op = "post", op_id = %op_id, channel_id = %channel_id, delivery_id = %delivery_id, content_len = content.len(), "processing crabd outbound op");
                }
                CrabdOutboundOp::Edit {
                    op_id,
                    channel_id,
                    delivery_id,
                    content,
                } => {
                    #[rustfmt::skip]
                    tracing::debug!(op = "edit", op_id = %op_id, channel_id = %channel_id, delivery_id = %delivery_id, content_len = content.len(), "processing crabd outbound op");
                }
            }
            let receipt =
                process_outbound_op(discord, delivery_id_map_store, retry_policy, op, &mut stats);
            if receipt.status != CrabdOutboundReceiptStatus::Ok {
                #[rustfmt::skip]
                tracing::warn!(op_id = %receipt.op_id, channel_id = %receipt.channel_id, delivery_id = %receipt.delivery_id, discord_message_id = ?receipt.discord_message_id, error = receipt.error_message.as_deref().unwrap_or("unknown error"), "outbound discord delivery returned error receipt");
            }
            let frame = CrabdInboundFrame::OutboundReceipt(receipt);
            crabd.send_inbound_frame(&frame)?;
            progressed = true;
            stats.outbound_operations_processed =
                stats.outbound_operations_processed.saturating_add(1);
        }

        if !progressed {
            control.sleep_idle(idle_sleep_ms)?;
        }
    }

    Ok(stats)
}

fn process_outbound_op<D>(
    discord: &mut D,
    delivery_id_map_store: &mut DeliveryIdMapStore,
    retry_policy: ConnectorRetryPolicy,
    op: CrabdOutboundOp,
    stats: &mut ConnectorLoopStats,
) -> CrabdOutboundReceipt
where
    D: DiscordIo,
{
    match op {
        CrabdOutboundOp::Post {
            op_id,
            channel_id,
            delivery_id,
            content,
        } => match delivery_id_map_store.lookup_discord_message_id(&channel_id, &delivery_id) {
            Some(discord_message_id) => {
                tracing::debug!(
                    op = "post",
                    op_id = %op_id,
                    channel_id = %channel_id,
                    delivery_id = %delivery_id,
                    discord_message_id = %discord_message_id,
                    "post is idempotent: delivery_id mapping already exists"
                );
                CrabdOutboundReceipt {
                    op_id,
                    status: CrabdOutboundReceiptStatus::Ok,
                    channel_id,
                    delivery_id,
                    discord_message_id: Some(discord_message_id),
                    error_message: None,
                }
            }
            None => {
                let actual_message_id = execute_with_retry(
                    "connector_post_message",
                    discord,
                    retry_policy,
                    |discord| discord.post_message(&channel_id, &content),
                    stats,
                );

                match actual_message_id {
                    Ok(actual_message_id) => match delivery_id_map_store.remember_post_mapping(
                        &channel_id,
                        &delivery_id,
                        &actual_message_id,
                    ) {
                        Ok(()) => CrabdOutboundReceipt {
                            op_id,
                            status: CrabdOutboundReceiptStatus::Ok,
                            channel_id,
                            delivery_id,
                            discord_message_id: Some(actual_message_id),
                            error_message: None,
                        },
                        Err(error) => CrabdOutboundReceipt {
                            op_id,
                            status: CrabdOutboundReceiptStatus::Error,
                            channel_id,
                            delivery_id,
                            discord_message_id: Some(actual_message_id),
                            error_message: Some(error.to_string()),
                        },
                    },
                    Err(error) => CrabdOutboundReceipt {
                        op_id,
                        status: CrabdOutboundReceiptStatus::Error,
                        channel_id,
                        delivery_id,
                        discord_message_id: None,
                        error_message: Some(error.to_string()),
                    },
                }
            }
        },
        CrabdOutboundOp::Edit {
            op_id,
            channel_id,
            delivery_id,
            content,
        } => {
            let Some(discord_message_id) =
                delivery_id_map_store.lookup_discord_message_id(&channel_id, &delivery_id)
            else {
                return CrabdOutboundReceipt {
                    op_id,
                    status: CrabdOutboundReceiptStatus::Error,
                    channel_id,
                    delivery_id,
                    discord_message_id: None,
                    error_message: Some(
                        "missing discord message id mapping for delivery_id; cannot edit"
                            .to_string(),
                    ),
                };
            };

            match execute_with_retry(
                "connector_edit_message",
                discord,
                retry_policy,
                |discord| discord.edit_message(&channel_id, &discord_message_id, &content),
                stats,
            ) {
                Ok(()) => CrabdOutboundReceipt {
                    op_id,
                    status: CrabdOutboundReceiptStatus::Ok,
                    channel_id,
                    delivery_id,
                    discord_message_id: Some(discord_message_id),
                    error_message: None,
                },
                Err(error) => CrabdOutboundReceipt {
                    op_id,
                    status: CrabdOutboundReceiptStatus::Error,
                    channel_id,
                    delivery_id,
                    discord_message_id: Some(discord_message_id),
                    error_message: Some(error.to_string()),
                },
            }
        }
    }
}

fn execute_with_retry<D, T, F>(
    context: &'static str,
    discord: &mut D,
    retry_policy: ConnectorRetryPolicy,
    mut operation: F,
    stats: &mut ConnectorLoopStats,
) -> CrabResult<T>
where
    D: DiscordIo,
    F: FnMut(&mut D) -> Result<T, DiscordApiError>,
{
    let mut attempt = 1_u32;
    let max_attempts = u32::from(retry_policy.max_attempts);

    loop {
        match operation(discord) {
            Ok(output) => return Ok(output),
            Err(error) => {
                if !error.is_retryable() {
                    // Non-retryable Discord errors (400, 403, 404) should fail
                    // the current operation but NOT kill the daemon. A bad
                    // channel ID or deleted message is a run-level failure.
                    eprintln!(
                        "[discord] non-retryable error on attempt {attempt} (context: {context}): {}",
                        error.message()
                    );
                    return Err(CrabError::Io {
                        context,
                        path: None,
                        message: format!(
                            "discord API error on attempt {attempt}: {}",
                            error.message()
                        ),
                    });
                }

                if attempt >= max_attempts {
                    eprintln!(
                        "[discord] retries exhausted after {attempt} attempts (context: {context}): {}",
                        error.message()
                    );
                    return Err(CrabError::Io {
                        context,
                        path: None,
                        message: format!(
                            "discord API retries exhausted after {attempt} attempts: {}",
                            error.message()
                        ),
                    });
                }

                let backoff_ms = retry_policy.backoff_ms(&error);
                discord.sleep_wait(backoff_ms)?;
                attempt += 1;
                stats.retry_waits = stats.retry_waits.saturating_add(1);
            }
        }
    }
}

trait DiscordWaitExt {
    fn sleep_wait(&mut self, backoff_ms: u64) -> CrabResult<()>;
}

impl<T> DiscordWaitExt for T
where
    T: DiscordIo,
{
    fn sleep_wait(&mut self, backoff_ms: u64) -> CrabResult<()> {
        self.wait_for_retry(backoff_ms)
    }
}

fn parse_crabd_path(args: &[String]) -> CrabResult<String> {
    let mut crabd_path = "crabd".to_string();
    let mut index = 0_usize;

    while index < args.len() {
        match args[index].as_str() {
            "--crabd" => {
                let value_index = index + 1;
                if value_index >= args.len() {
                    return Err(CrabError::InvalidConfig {
                        key: "--crabd",
                        value: "".to_string(),
                        reason: "must be followed by an executable path",
                    });
                }
                crabd_path = args[value_index].clone();
                index += 2;
            }
            unknown => {
                return Err(CrabError::InvalidConfig {
                    key: "connector_args",
                    value: unknown.to_string(),
                    reason: "unsupported argument",
                });
            }
        }
    }

    ensure_non_empty("connector_parse_args", "crabd_path", &crabd_path)?;
    Ok(crabd_path)
}

fn parse_optional_u64(raw: Option<&String>, key: &'static str, default: u64) -> CrabResult<u64> {
    match raw {
        Some(value) => parse_required_u64(value, key),
        None => Ok(default),
    }
}

fn parse_optional_u8(raw: Option<&String>, key: &'static str, default: u8) -> CrabResult<u8> {
    match raw {
        Some(value) => parse_required_u8(value, key),
        None => Ok(default),
    }
}

fn parse_required_u64(raw: &str, key: &'static str) -> CrabResult<u64> {
    let value = u64::from_str(raw).map_err(|_| CrabError::InvalidConfig {
        key,
        value: raw.to_string(),
        reason: "must be a positive integer",
    })?;
    if value == 0 {
        return Err(CrabError::InvalidConfig {
            key,
            value: raw.to_string(),
            reason: "must be greater than 0",
        });
    }
    Ok(value)
}

fn parse_required_u8(raw: &str, key: &'static str) -> CrabResult<u8> {
    let value = u8::from_str(raw).map_err(|_| CrabError::InvalidConfig {
        key,
        value: raw.to_string(),
        reason: "must be a positive integer",
    })?;
    if value == 0 {
        return Err(CrabError::InvalidConfig {
            key,
            value: raw.to_string(),
            reason: "must be greater than 0",
        });
    }
    Ok(value)
}

fn resolve_message_id_map_path(values: &HashMap<String, String>) -> CrabResult<PathBuf> {
    if let Some(explicit) = values.get("CRAB_CONNECTOR_MESSAGE_ID_MAP_PATH") {
        let path = expand_home_path(explicit);
        ensure_non_empty(
            "connector_message_id_map_path",
            "CRAB_CONNECTOR_MESSAGE_ID_MAP_PATH",
            path.to_string_lossy().as_ref(),
        )?;
        return Ok(path);
    }

    let workspace_root = values
        .get("CRAB_WORKSPACE_ROOT")
        .cloned()
        .unwrap_or_else(|| DEFAULT_WORKSPACE_ROOT.to_string());
    let workspace_root = expand_home_path(&workspace_root);
    Ok(workspace_root.join(DEFAULT_MESSAGE_ID_MAP_FILE))
}

fn expand_home_path(path: &str) -> PathBuf {
    let home = std::env::var_os("HOME");
    expand_home_path_with_home(path, home.as_deref())
}

fn expand_home_path_with_home(path: &str, home: Option<&std::ffi::OsStr>) -> PathBuf {
    if path == "~" {
        if let Some(home) = home {
            return PathBuf::from(home);
        }
        return PathBuf::from(path);
    }

    if let Some(remainder) = path.strip_prefix("~/") {
        if let Some(home) = home {
            return PathBuf::from(home).join(remainder);
        }
    }

    PathBuf::from(path)
}

fn ensure_non_empty(context: &'static str, field: &'static str, value: &str) -> CrabResult<()> {
    if value.trim().is_empty() {
        return Err(CrabError::InvariantViolation {
            context,
            message: format!("{field} must not be empty"),
        });
    }
    Ok(())
}

#[cfg(all(not(test), not(coverage)))]
#[derive(Debug)]
struct SystemConnectorLoopControl {
    shutdown_flag: Arc<AtomicBool>,
}

#[cfg(all(not(test), not(coverage)))]
impl SystemConnectorLoopControl {
    fn install() -> CrabResult<Self> {
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        ctrlc::set_handler({
            let shutdown_flag = Arc::clone(&shutdown_flag);
            move || shutdown_flag.store(true, Ordering::SeqCst)
        })
        .map_err(|error| CrabError::InvariantViolation {
            context: "connector_ctrlc_install",
            message: format!("failed to install Ctrl-C handler: {error}"),
        })?;

        // `kill <pid>` sends SIGTERM. If we do not handle SIGTERM, the process will terminate
        // immediately and will not run Drop handlers, leaving the `crabd` child orphaned.
        #[cfg(unix)]
        signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&shutdown_flag))
            .map_err(|error| CrabError::InvariantViolation {
                context: "connector_sigterm_install",
                message: format!("failed to install SIGTERM handler: {error}"),
            })?;

        Ok(Self { shutdown_flag })
    }
}

#[cfg(all(not(test), not(coverage)))]
impl ConnectorLoopControl for SystemConnectorLoopControl {
    fn should_shutdown(&self) -> bool {
        self.shutdown_flag.load(Ordering::SeqCst)
    }

    fn sleep_idle(&mut self, idle_sleep_ms: u64) -> CrabResult<()> {
        thread::sleep(Duration::from_millis(idle_sleep_ms));
        Ok(())
    }
}

#[cfg(all(not(test), not(coverage)))]
#[derive(Debug)]
struct ChildCrabdIo {
    child: std::process::Child,
    writer: BufWriter<std::process::ChildStdin>,
    outbound_rx: mpsc::Receiver<CrabResult<CrabdOutboundOp>>,
}

#[cfg(all(not(test), not(coverage)))]
impl ChildCrabdIo {
    fn spawn(crabd_path: &str) -> CrabResult<Self> {
        ensure_non_empty("connector_spawn_crabd", "crabd_path", crabd_path)?;

        let mut command = std::process::Command::new(crabd_path);
        command
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit());

        let mut child = command.spawn().map_err(|error| CrabError::Io {
            context: "connector_spawn_crabd",
            path: Some(crabd_path.to_string()),
            message: error.to_string(),
        })?;

        let stdin = child.stdin.take().ok_or(CrabError::InvariantViolation {
            context: "connector_spawn_crabd",
            message: "crabd stdin not available".to_string(),
        })?;
        let stdout = child.stdout.take().ok_or(CrabError::InvariantViolation {
            context: "connector_spawn_crabd",
            message: "crabd stdout not available".to_string(),
        })?;

        let (outbound_tx, outbound_rx) = mpsc::channel();
        thread::spawn(move || {
            let reader = BufReader::new(stdout);
            for line_result in reader.lines() {
                let line = match line_result {
                    Ok(line) => line,
                    Err(error) => {
                        let _ = outbound_tx.send(Err(CrabError::Io {
                            context: "connector_read_crabd_stdout",
                            path: None,
                            message: error.to_string(),
                        }));
                        break;
                    }
                };

                if line.trim().is_empty() {
                    continue;
                }

                if outbound_tx.send(parse_outbound_op_line(&line)).is_err() {
                    break;
                }
            }
        });

        Ok(Self {
            child,
            writer: BufWriter::new(stdin),
            outbound_rx,
        })
    }
}

#[cfg(all(not(test), not(coverage)))]
impl CrabdIo for ChildCrabdIo {
    fn next_outbound_op(&mut self) -> CrabResult<Option<CrabdOutboundOp>> {
        match self.outbound_rx.try_recv() {
            Ok(result) => result.map(Some),
            Err(mpsc::TryRecvError::Empty) => Ok(None),
            Err(mpsc::TryRecvError::Disconnected) => {
                match self.child.try_wait().map_err(|error| CrabError::Io {
                    context: "connector_check_crabd_status",
                    path: None,
                    message: error.to_string(),
                })? {
                    Some(status) => Err(CrabError::InvariantViolation {
                        context: "connector_crabd_stdout_closed",
                        message: format!("crabd exited with status {status}"),
                    }),
                    None => Ok(None),
                }
            }
        }
    }

    fn send_inbound_frame(&mut self, frame: &CrabdInboundFrame) -> CrabResult<()> {
        let payload = serde_json::to_string(frame).map_err(|error| CrabError::Serialization {
            context: "connector_serialize_crabd_inbound_frame",
            path: None,
            message: error.to_string(),
        })?;
        self.writer
            .write_all(payload.as_bytes())
            .map_err(|error| CrabError::Io {
                context: "connector_write_crabd_stdin",
                path: None,
                message: error.to_string(),
            })?;
        self.writer
            .write_all(b"\n")
            .map_err(|error| CrabError::Io {
                context: "connector_write_crabd_stdin",
                path: None,
                message: error.to_string(),
            })?;
        self.writer.flush().map_err(|error| CrabError::Io {
            context: "connector_flush_crabd_stdin",
            path: None,
            message: error.to_string(),
        })?;
        Ok(())
    }
}

#[cfg(all(not(test), not(coverage)))]
impl Drop for ChildCrabdIo {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[cfg(all(feature = "live-discord", not(test), not(coverage)))]
mod live_discord {
    use super::{DiscordApiError, DiscordIo};
    use crab_core::{CrabError, CrabResult};
    use crab_discord::{GatewayAttachment, GatewayConversationKind, GatewayMessage};
    use reqwest::blocking::Client;
    use reqwest::StatusCode;
    use serde_json::Value;
    use serenity::all::{Channel, ChannelType, GatewayIntents, Message as SerenityMessage, Ready};
    use serenity::async_trait;
    use serenity::client::{Client as SerenityClient, Context, EventHandler};
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    const DISCORD_API_BASE_URL: &str = "https://discord.com/api/v10";

    #[derive(Clone)]
    struct DiscordHandler {
        tx: mpsc::Sender<GatewayMessage>,
    }

    #[async_trait]
    impl EventHandler for DiscordHandler {
        async fn ready(&self, _ctx: Context, ready: Ready) {
            tracing::info!(user_id = %ready.user.id, "crab-discord-connector connected");
        }

        async fn message(&self, ctx: Context, message: SerenityMessage) {
            if message.author.bot {
                return;
            }

            let (conversation_kind, thread_id) = resolve_conversation_kind(&ctx, &message).await;

            let gateway_message = GatewayMessage {
                message_id: message.id.get().to_string(),
                author_id: message.author.id.get().to_string(),
                author_is_bot: message.author.bot,
                channel_id: message.channel_id.get().to_string(),
                guild_id: message.guild_id.map(|id| id.get().to_string()),
                thread_id,
                content: message.content,
                conversation_kind,
                attachments: message
                    .attachments
                    .iter()
                    .map(|a| GatewayAttachment {
                        url: a.url.clone(),
                        filename: a.filename.clone(),
                        size: u64::from(a.size),
                        content_type: a.content_type.clone(),
                    })
                    .collect(),
            };

            let _ = self.tx.send(gateway_message);
        }
    }

    async fn resolve_conversation_kind(
        ctx: &Context,
        message: &SerenityMessage,
    ) -> (GatewayConversationKind, Option<String>) {
        if message.guild_id.is_none() {
            return (GatewayConversationKind::DirectMessage, None);
        }

        match message.channel_id.to_channel(ctx).await {
            Ok(Channel::Guild(channel))
                if matches!(
                    channel.kind,
                    ChannelType::PublicThread
                        | ChannelType::PrivateThread
                        | ChannelType::NewsThread
                ) =>
            {
                (
                    GatewayConversationKind::Thread,
                    Some(message.channel_id.get().to_string()),
                )
            }
            _ => (GatewayConversationKind::GuildChannel, None),
        }
    }

    #[derive(Debug)]
    pub struct LiveDiscordIo {
        rx: mpsc::Receiver<GatewayMessage>,
        _gateway_thread: thread::JoinHandle<()>,
        http: Client,
        token: String,
    }

    impl LiveDiscordIo {
        pub(super) fn connect(token: String) -> CrabResult<Self> {
            if token.trim().is_empty() {
                return Err(CrabError::MissingConfig {
                    key: "CRAB_DISCORD_TOKEN",
                });
            }

            let (tx, rx) = mpsc::channel();
            let token_for_thread = token.clone();
            let gateway_thread = thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .expect("discord connector runtime should build");

                runtime.block_on(async move {
                    let intents = GatewayIntents::DIRECT_MESSAGES
                        | GatewayIntents::GUILD_MESSAGES
                        | GatewayIntents::MESSAGE_CONTENT;

                    loop {
                        let handler = DiscordHandler { tx: tx.clone() };
                        let client_result = SerenityClient::builder(&token_for_thread, intents)
                            .event_handler(handler)
                            .await;

                        let mut client = match client_result {
                            Ok(client) => client,
                            Err(error) => {
                                tracing::error!(
                                    error = %error,
                                    "crab-discord-connector failed to create discord client"
                                );
                                tokio::time::sleep(Duration::from_secs(2)).await;
                                continue;
                            }
                        };

                        if let Err(error) = client.start().await {
                            tracing::error!(
                                error = %error,
                                "crab-discord-connector discord gateway error"
                            );
                            tokio::time::sleep(Duration::from_secs(2)).await;
                        }
                    }
                });
            });

            let http = Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .map_err(|error| CrabError::InvariantViolation {
                    context: "connector_http_client",
                    message: error.to_string(),
                })?;

            Ok(Self {
                rx,
                _gateway_thread: gateway_thread,
                http,
                token,
            })
        }

        fn auth_header(&self) -> String {
            format!("Bot {}", self.token)
        }

        fn parse_retry_after_ms(payload: &Value) -> Option<u64> {
            let value = payload.get("retry_after")?;
            let retry_secs = if let Some(seconds) = value.as_f64() {
                seconds
            } else if let Some(seconds) = value.as_u64() {
                seconds as f64
            } else {
                return None;
            };

            if !retry_secs.is_finite() || retry_secs < 0.0 {
                return None;
            }

            let millis = retry_secs * 1000.0;
            if millis > u64::MAX as f64 {
                return Some(u64::MAX);
            }
            Some(millis.ceil() as u64)
        }

        fn map_send_error(error: reqwest::Error) -> DiscordApiError {
            if error.is_timeout() || error.is_connect() {
                return DiscordApiError::Retryable {
                    message: error.to_string(),
                };
            }

            DiscordApiError::Fatal {
                message: error.to_string(),
            }
        }

        fn handle_api_response(
            response: reqwest::blocking::Response,
        ) -> Result<Value, DiscordApiError> {
            let status = response.status();
            let body = response.text().unwrap_or_else(|_| "".to_string());

            if status.is_success() {
                return serde_json::from_str::<Value>(&body).map_err(|error| {
                    DiscordApiError::Fatal {
                        message: format!("failed to parse discord success payload: {error}"),
                    }
                });
            }

            if status == StatusCode::TOO_MANY_REQUESTS {
                let parsed = serde_json::from_str::<Value>(&body).unwrap_or(Value::Null);
                let retry_after_ms = Self::parse_retry_after_ms(&parsed);
                let message = parsed
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("discord rate limited")
                    .to_string();
                return Err(DiscordApiError::RateLimited {
                    retry_after_ms,
                    message,
                });
            }

            if status.is_server_error() {
                return Err(DiscordApiError::Retryable {
                    message: format!("discord server error {status}: {body}"),
                });
            }

            Err(DiscordApiError::Fatal {
                message: format!("discord request failed {status}: {body}"),
            })
        }
    }

    impl DiscordIo for LiveDiscordIo {
        fn next_gateway_message(&mut self) -> CrabResult<Option<GatewayMessage>> {
            match self.rx.try_recv() {
                Ok(message) => Ok(Some(message)),
                Err(mpsc::TryRecvError::Empty) => Ok(None),
                Err(mpsc::TryRecvError::Disconnected) => Err(CrabError::InvariantViolation {
                    context: "connector_gateway_channel",
                    message: "discord gateway channel disconnected".to_string(),
                }),
            }
        }

        fn post_message(
            &mut self,
            channel_id: &str,
            content: &str,
        ) -> Result<String, DiscordApiError> {
            let url = format!("{DISCORD_API_BASE_URL}/channels/{channel_id}/messages");
            let response = self
                .http
                .post(url)
                .header(reqwest::header::AUTHORIZATION, self.auth_header())
                .json(&serde_json::json!({ "content": content }))
                .send()
                .map_err(Self::map_send_error)?;

            let payload = Self::handle_api_response(response)?;
            let message_id = payload.get("id").and_then(Value::as_str).ok_or_else(|| {
                DiscordApiError::Fatal {
                    message: "discord create message response missing id".to_string(),
                }
            })?;
            Ok(message_id.to_string())
        }

        fn edit_message(
            &mut self,
            channel_id: &str,
            message_id: &str,
            content: &str,
        ) -> Result<(), DiscordApiError> {
            let url = format!("{DISCORD_API_BASE_URL}/channels/{channel_id}/messages/{message_id}");
            let response = self
                .http
                .patch(url)
                .header(reqwest::header::AUTHORIZATION, self.auth_header())
                .json(&serde_json::json!({ "content": content }))
                .send()
                .map_err(Self::map_send_error)?;

            let _payload = Self::handle_api_response(response)?;
            Ok(())
        }

        fn wait_for_retry(&mut self, backoff_ms: u64) -> CrabResult<()> {
            thread::sleep(Duration::from_millis(backoff_ms));
            Ok(())
        }
    }
}

#[cfg(any(test, coverage, not(feature = "live-discord")))]
mod live_discord {
    use super::{DiscordApiError, DiscordIo};
    use crab_core::{CrabError, CrabResult};
    use crab_discord::GatewayMessage;

    #[derive(Debug)]
    pub struct LiveDiscordIo;

    impl LiveDiscordIo {
        pub(super) fn connect(_token: String) -> CrabResult<Self> {
            Err(CrabError::InvariantViolation {
                context: "connector_live_discord",
                message: "live discord integration is unavailable in this build".to_string(),
            })
        }
    }

    impl DiscordIo for LiveDiscordIo {
        fn next_gateway_message(&mut self) -> CrabResult<Option<GatewayMessage>> {
            Ok(None)
        }

        fn post_message(
            &mut self,
            _channel_id: &str,
            _content: &str,
        ) -> Result<String, DiscordApiError> {
            Err(DiscordApiError::Fatal {
                message: "live discord integration is unavailable in this build".to_string(),
            })
        }

        fn edit_message(
            &mut self,
            _channel_id: &str,
            _message_id: &str,
            _content: &str,
        ) -> Result<(), DiscordApiError> {
            Err(DiscordApiError::Fatal {
                message: "live discord integration is unavailable in this build".to_string(),
            })
        }

        fn wait_for_retry(&mut self, _backoff_ms: u64) -> CrabResult<()> {
            Ok(())
        }
    }
}

#[cfg(all(not(test), not(coverage)))]
fn run_with_env_and_args() -> CrabResult<ConnectorLoopStats> {
    let values = std::env::vars().collect::<HashMap<_, _>>();
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let config = ConnectorConfig::from_map_and_args(&values, &args)?;

    tracing::info!(
        crabd_path = %config.crabd_path,
        idle_sleep_ms = config.idle_sleep_ms,
        retry_max_attempts = config.retry_policy.max_attempts,
        retry_backoff_ms = config.retry_policy.retry_backoff_ms,
        retry_max_backoff_ms = config.retry_policy.max_retry_backoff_ms,
        message_id_map_path = %config.message_id_map_path.to_string_lossy(),
        "crab-discord-connector starting"
    );

    let mut delivery_id_map_store = DeliveryIdMapStore::load(config.message_id_map_path)?;
    let mut discord = live_discord::LiveDiscordIo::connect(config.discord_token)?;
    let mut crabd = ChildCrabdIo::spawn(&config.crabd_path)?;
    let mut control = SystemConnectorLoopControl::install()?;

    run_connector_loop(
        &mut discord,
        &mut crabd,
        &mut delivery_id_map_store,
        config.retry_policy,
        config.idle_sleep_ms,
        &mut control,
    )
}

#[cfg(all(not(test), not(coverage)))]
fn main() -> std::process::ExitCode {
    crab_telemetry::init_tracing_stderr("info");
    match run_with_env_and_args() {
        Ok(stats) => {
            tracing::info!(
                inbound = stats.gateway_messages_forwarded,
                outbound = stats.outbound_operations_processed,
                retries = stats.retry_waits,
                "crab-discord-connector finished"
            );
            std::process::ExitCode::SUCCESS
        }
        Err(error) => {
            tracing::error!(error = %error, "crab-discord-connector failed");
            std::process::ExitCode::FAILURE
        }
    }
}

#[cfg(coverage)]
fn main() {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[derive(Debug, Clone, Default)]
    struct ScriptedDiscordIo {
        inbound: VecDeque<CrabResult<Option<GatewayMessage>>>,
        post_results: VecDeque<Result<String, DiscordApiError>>,
        edit_results: VecDeque<Result<(), DiscordApiError>>,
        wait_calls: Vec<u64>,
        post_calls: Vec<(String, String)>,
        edit_calls: Vec<(String, String, String)>,
    }

    impl DiscordIo for ScriptedDiscordIo {
        fn next_gateway_message(&mut self) -> CrabResult<Option<GatewayMessage>> {
            self.inbound.pop_front().unwrap_or(Ok(None))
        }

        fn post_message(
            &mut self,
            channel_id: &str,
            content: &str,
        ) -> Result<String, DiscordApiError> {
            self.post_calls
                .push((channel_id.to_string(), content.to_string()));
            self.post_results
                .pop_front()
                .unwrap_or(Ok("actual-1".to_string()))
        }

        fn edit_message(
            &mut self,
            channel_id: &str,
            message_id: &str,
            content: &str,
        ) -> Result<(), DiscordApiError> {
            self.edit_calls.push((
                channel_id.to_string(),
                message_id.to_string(),
                content.to_string(),
            ));
            self.edit_results.pop_front().unwrap_or(Ok(()))
        }

        fn wait_for_retry(&mut self, backoff_ms: u64) -> CrabResult<()> {
            self.wait_calls.push(backoff_ms);
            Ok(())
        }
    }

    #[derive(Debug, Clone, Default)]
    struct ScriptedCrabdIo {
        outbound: VecDeque<CrabResult<Option<CrabdOutboundOp>>>,
        inbound_frames: Vec<CrabdInboundFrame>,
    }

    impl CrabdIo for ScriptedCrabdIo {
        fn next_outbound_op(&mut self) -> CrabResult<Option<CrabdOutboundOp>> {
            self.outbound.pop_front().unwrap_or(Ok(None))
        }

        fn send_inbound_frame(&mut self, frame: &CrabdInboundFrame) -> CrabResult<()> {
            self.inbound_frames.push(frame.clone());
            Ok(())
        }
    }

    #[derive(Debug, Clone)]
    struct ScriptedControl {
        max_sleeps: usize,
        sleeps: Vec<u64>,
    }

    impl ScriptedControl {
        fn new(max_sleeps: usize) -> Self {
            Self {
                max_sleeps,
                sleeps: Vec::new(),
            }
        }
    }

    impl ConnectorLoopControl for ScriptedControl {
        fn should_shutdown(&self) -> bool {
            self.sleeps.len() >= self.max_sleeps
        }

        fn sleep_idle(&mut self, idle_sleep_ms: u64) -> CrabResult<()> {
            self.sleeps.push(idle_sleep_ms);
            Ok(())
        }
    }

    fn temp_file_path(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be monotonic for tests")
            .as_nanos();
        env::temp_dir().join(format!("{prefix}-{nanos}.json"))
    }

    fn gateway_message(message_id: &str, content: &str) -> GatewayMessage {
        GatewayMessage {
            message_id: message_id.to_string(),
            author_id: "user-1".to_string(),
            author_is_bot: false,
            channel_id: "channel-1".to_string(),
            guild_id: Some("guild-1".to_string()),
            thread_id: None,
            content: content.to_string(),
            conversation_kind: GatewayConversationKind::GuildChannel,
            attachments: vec![],
        }
    }

    fn ok_receipt_frame(
        op_id: &str,
        channel_id: &str,
        delivery_id: &str,
        discord_message_id: &str,
    ) -> CrabdInboundFrame {
        CrabdInboundFrame::OutboundReceipt(CrabdOutboundReceipt {
            op_id: op_id.to_string(),
            status: CrabdOutboundReceiptStatus::Ok,
            channel_id: channel_id.to_string(),
            delivery_id: delivery_id.to_string(),
            discord_message_id: Some(discord_message_id.to_string()),
            error_message: None,
        })
    }

    fn post_op(op_id: &str, channel_id: &str, delivery_id: &str, content: &str) -> CrabdOutboundOp {
        CrabdOutboundOp::Post {
            op_id: op_id.to_string(),
            channel_id: channel_id.to_string(),
            delivery_id: delivery_id.to_string(),
            content: content.to_string(),
        }
    }

    fn edit_op(op_id: &str, channel_id: &str, delivery_id: &str, content: &str) -> CrabdOutboundOp {
        CrabdOutboundOp::Edit {
            op_id: op_id.to_string(),
            channel_id: channel_id.to_string(),
            delivery_id: delivery_id.to_string(),
            content: content.to_string(),
        }
    }

    fn values(entries: &[(&str, &str)]) -> HashMap<String, String> {
        let mut map = HashMap::new();
        for (key, value) in entries {
            map.insert((*key).to_string(), (*value).to_string());
        }
        map
    }

    #[test]
    fn config_parsing_supports_defaults_and_overrides() {
        let map_path = temp_file_path("connector-config");
        let input = values(&[
            ("CRAB_DISCORD_TOKEN", "token"),
            ("CRAB_CONNECTOR_IDLE_SLEEP_MS", "42"),
            ("CRAB_CONNECTOR_RETRY_MAX_ATTEMPTS", "4"),
            ("CRAB_CONNECTOR_RETRY_BACKOFF_MS", "250"),
            ("CRAB_CONNECTOR_RETRY_MAX_BACKOFF_MS", "1000"),
            (
                "CRAB_CONNECTOR_MESSAGE_ID_MAP_PATH",
                map_path.to_str().expect("temp path should be utf8"),
            ),
        ]);

        let parsed = ConnectorConfig::from_map_and_args(
            &input,
            &["--crabd".to_string(), "./crabd".to_string()],
        )
        .expect("config should parse");

        assert_eq!(parsed.crabd_path, "./crabd");
        assert_eq!(parsed.discord_token, "token");
        assert_eq!(parsed.idle_sleep_ms, 42);
        assert_eq!(parsed.retry_policy.max_attempts, 4);
        assert_eq!(parsed.retry_policy.retry_backoff_ms, 250);
        assert_eq!(parsed.retry_policy.max_retry_backoff_ms, 1000);
        assert_eq!(parsed.message_id_map_path, map_path);

        let _ = fs::remove_file(parsed.message_id_map_path);
    }

    #[test]
    fn config_parsing_validates_required_and_numeric_fields() {
        let missing_token = ConnectorConfig::from_map_and_args(&HashMap::new(), &[])
            .expect_err("missing token should fail");
        assert_eq!(
            missing_token,
            CrabError::MissingConfig {
                key: "CRAB_DISCORD_TOKEN",
            }
        );

        let invalid_args = ConnectorConfig::from_map_and_args(
            &values(&[("CRAB_DISCORD_TOKEN", "token")]),
            &["--bad".to_string()],
        )
        .expect_err("unsupported arg should fail");
        assert_eq!(
            invalid_args,
            CrabError::InvalidConfig {
                key: "connector_args",
                value: "--bad".to_string(),
                reason: "unsupported argument",
            }
        );

        let missing_crabd_value = ConnectorConfig::from_map_and_args(
            &values(&[("CRAB_DISCORD_TOKEN", "token")]),
            &["--crabd".to_string()],
        )
        .expect_err("--crabd without value should fail");
        assert_eq!(
            missing_crabd_value,
            CrabError::InvalidConfig {
                key: "--crabd",
                value: "".to_string(),
                reason: "must be followed by an executable path",
            }
        );

        let invalid_idle = ConnectorConfig::from_map_and_args(
            &values(&[
                ("CRAB_DISCORD_TOKEN", "token"),
                ("CRAB_CONNECTOR_IDLE_SLEEP_MS", "0"),
            ]),
            &[],
        )
        .expect_err("zero idle should fail");
        assert_eq!(
            invalid_idle,
            CrabError::InvalidConfig {
                key: "CRAB_CONNECTOR_IDLE_SLEEP_MS",
                value: "0".to_string(),
                reason: "must be greater than 0",
            }
        );

        let invalid_retry = ConnectorConfig::from_map_and_args(
            &values(&[
                ("CRAB_DISCORD_TOKEN", "token"),
                ("CRAB_CONNECTOR_RETRY_MAX_ATTEMPTS", "x"),
            ]),
            &[],
        )
        .expect_err("invalid retry attempts should fail");
        assert_eq!(
            invalid_retry,
            CrabError::InvalidConfig {
                key: "CRAB_CONNECTOR_RETRY_MAX_ATTEMPTS",
                value: "x".to_string(),
                reason: "must be a positive integer",
            }
        );

        let invalid_retry_backoff = ConnectorConfig::from_map_and_args(
            &values(&[
                ("CRAB_DISCORD_TOKEN", "token"),
                ("CRAB_CONNECTOR_RETRY_BACKOFF_MS", "x"),
            ]),
            &[],
        )
        .expect_err("invalid retry backoff should fail");
        assert_eq!(
            invalid_retry_backoff,
            CrabError::InvalidConfig {
                key: "CRAB_CONNECTOR_RETRY_BACKOFF_MS",
                value: "x".to_string(),
                reason: "must be a positive integer",
            }
        );

        let invalid_max_backoff = ConnectorConfig::from_map_and_args(
            &values(&[
                ("CRAB_DISCORD_TOKEN", "token"),
                ("CRAB_CONNECTOR_RETRY_MAX_BACKOFF_MS", "0"),
            ]),
            &[],
        )
        .expect_err("zero max backoff should fail");
        assert_eq!(
            invalid_max_backoff,
            CrabError::InvalidConfig {
                key: "CRAB_CONNECTOR_RETRY_MAX_BACKOFF_MS",
                value: "0".to_string(),
                reason: "must be greater than 0",
            }
        );
    }

    #[test]
    fn message_id_map_store_load_resolve_and_persist_round_trip() {
        let path = temp_file_path("connector-map");
        let mut store =
            DeliveryIdMapStore::load(&path).expect("map should initialize when missing");

        assert_eq!(
            store.lookup_discord_message_id("channel-1", "delivery-1"),
            None
        );

        store
            .remember_post_mapping("channel-1", "delivery-1", "actual-1")
            .expect("mapping should persist");
        assert_eq!(
            store.lookup_discord_message_id("channel-1", "delivery-1"),
            Some("actual-1".to_string())
        );

        let reloaded = DeliveryIdMapStore::load(&path).expect("map should reload");
        assert_eq!(
            reloaded.lookup_discord_message_id("channel-1", "delivery-1"),
            Some("actual-1".to_string())
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn parse_outbound_op_line_validates_shape_and_required_fields() {
        let post = parse_outbound_op_line(
            r#"{"op":"post","op_id":"op-1","channel_id":"c1","delivery_id":"d1","content":"hello"}"#,
        )
        .expect("post line should parse");
        assert_eq!(
            post,
            CrabdOutboundOp::Post {
                op_id: "op-1".to_string(),
                channel_id: "c1".to_string(),
                delivery_id: "d1".to_string(),
                content: "hello".to_string(),
            }
        );

        let edit = parse_outbound_op_line(
            r#"{"op":"edit","op_id":"op-2","channel_id":"c1","delivery_id":"d1","content":"hi"}"#,
        )
        .expect("edit line should parse");
        assert_eq!(
            edit,
            CrabdOutboundOp::Edit {
                op_id: "op-2".to_string(),
                channel_id: "c1".to_string(),
                delivery_id: "d1".to_string(),
                content: "hi".to_string(),
            }
        );

        let bad_json = parse_outbound_op_line("not-json")
            .expect_err("invalid json should surface serialization error");
        assert!(matches!(
            bad_json,
            CrabError::Serialization {
                context: "connector_parse_crabd_outbound_op",
                ..
            }
        ));

        let blank_content = parse_outbound_op_line(
            r#"{"op":"post","op_id":"op-1","channel_id":"c1","delivery_id":"d1","content":"   "}"#,
        )
        .expect_err("blank content should fail invariant validation");
        assert!(matches!(
            blank_content,
            CrabError::InvariantViolation {
                context: "connector_parse_crabd_outbound_op",
                ..
            }
        ));

        let blank_channel_id = parse_outbound_op_line(
            r#"{"op":"post","op_id":"op-1","channel_id":"   ","delivery_id":"d1","content":"hello"}"#,
        )
        .expect_err("blank channel_id should fail invariant validation");
        assert_eq!(
            blank_channel_id,
            CrabError::InvariantViolation {
                context: "connector_parse_crabd_outbound_op",
                message: "channel_id must not be empty".to_string(),
            }
        );

        let blank_delivery_id = parse_outbound_op_line(
            r#"{"op":"post","op_id":"op-1","channel_id":"c1","delivery_id":"  ","content":"hello"}"#,
        )
        .expect_err("blank delivery_id should fail invariant validation");
        assert_eq!(
            blank_delivery_id,
            CrabError::InvariantViolation {
                context: "connector_parse_crabd_outbound_op",
                message: "delivery_id must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn connector_loop_forwards_inbound_and_processes_outbound_with_mapping() {
        let map_path = temp_file_path("connector-loop-map");
        let mut map_store = DeliveryIdMapStore::load(&map_path).expect("map store should init");

        let mut discord = ScriptedDiscordIo {
            inbound: VecDeque::from([Ok(Some(gateway_message("msg-1", "hello"))), Ok(None)]),
            post_results: VecDeque::from([Ok("actual-99".to_string())]),
            ..ScriptedDiscordIo::default()
        };

        let mut crabd = ScriptedCrabdIo {
            outbound: VecDeque::from([
                Ok(Some(CrabdOutboundOp::Post {
                    op_id: "op-1".to_string(),
                    channel_id: "channel-1".to_string(),
                    delivery_id: "delivery-1".to_string(),
                    content: "first".to_string(),
                })),
                Ok(Some(CrabdOutboundOp::Edit {
                    op_id: "op-2".to_string(),
                    channel_id: "channel-1".to_string(),
                    delivery_id: "delivery-1".to_string(),
                    content: "updated".to_string(),
                })),
                Ok(None),
            ]),
            ..ScriptedCrabdIo::default()
        };

        let mut control = ScriptedControl::new(1);
        let stats = run_connector_loop(
            &mut discord,
            &mut crabd,
            &mut map_store,
            ConnectorRetryPolicy::default(),
            5,
            &mut control,
        )
        .expect("loop should complete scripted run");

        assert_eq!(stats.gateway_messages_forwarded, 1);
        assert_eq!(stats.outbound_operations_processed, 2);
        assert_eq!(stats.retry_waits, 0);
        assert_eq!(discord.post_calls.len(), 1);
        assert_eq!(discord.edit_calls.len(), 1);
        assert_eq!(crabd.inbound_frames.len(), 3);
        assert_eq!(
            crabd.inbound_frames[0],
            CrabdInboundFrame::GatewayMessage(gateway_message("msg-1", "hello"))
        );
        assert_eq!(
            crabd.inbound_frames[1],
            ok_receipt_frame("op-1", "channel-1", "delivery-1", "actual-99")
        );
        assert_eq!(
            crabd.inbound_frames[2],
            ok_receipt_frame("op-2", "channel-1", "delivery-1", "actual-99")
        );
        assert_eq!(
            discord.edit_calls[0],
            (
                "channel-1".to_string(),
                "actual-99".to_string(),
                "updated".to_string(),
            )
        );

        let _ = fs::remove_file(map_path);
    }

    #[test]
    fn connector_loop_retries_rate_limits_and_reports_retry_exhaustion() {
        let map_path = temp_file_path("connector-loop-retry");
        let mut map_store = DeliveryIdMapStore::load(&map_path).expect("map store should init");

        let mut discord = ScriptedDiscordIo {
            post_results: VecDeque::from([
                Err(DiscordApiError::RateLimited {
                    retry_after_ms: Some(1234),
                    message: "rate limited".to_string(),
                }),
                Ok("actual-1".to_string()),
            ]),
            ..ScriptedDiscordIo::default()
        };

        let mut crabd = ScriptedCrabdIo {
            outbound: VecDeque::from([
                Ok(Some(CrabdOutboundOp::Post {
                    op_id: "op-1".to_string(),
                    channel_id: "channel-1".to_string(),
                    delivery_id: "delivery-1".to_string(),
                    content: "first".to_string(),
                })),
                Ok(None),
            ]),
            ..ScriptedCrabdIo::default()
        };

        let mut control = ScriptedControl::new(1);
        let stats = run_connector_loop(
            &mut discord,
            &mut crabd,
            &mut map_store,
            ConnectorRetryPolicy::default(),
            5,
            &mut control,
        )
        .expect("rate limited operation should recover");

        assert_eq!(stats.retry_waits, 1);
        assert_eq!(discord.wait_calls, vec![1234]);
        assert_eq!(
            crabd.inbound_frames,
            vec![ok_receipt_frame(
                "op-1",
                "channel-1",
                "delivery-1",
                "actual-1"
            )]
        );

        let mut exhausted_discord = ScriptedDiscordIo {
            post_results: VecDeque::from([
                Err(DiscordApiError::Retryable {
                    message: "transient".to_string(),
                }),
                Err(DiscordApiError::Retryable {
                    message: "transient".to_string(),
                }),
                Err(DiscordApiError::Retryable {
                    message: "transient".to_string(),
                }),
            ]),
            ..ScriptedDiscordIo::default()
        };

        let mut exhausted_crabd = ScriptedCrabdIo {
            outbound: VecDeque::from([Ok(Some(CrabdOutboundOp::Post {
                op_id: "op-2".to_string(),
                channel_id: "channel-1".to_string(),
                delivery_id: "delivery-2".to_string(),
                content: "first".to_string(),
            }))]),
            ..ScriptedCrabdIo::default()
        };
        let mut exhausted_control = ScriptedControl::new(1);

        let exhausted_stats = run_connector_loop(
            &mut exhausted_discord,
            &mut exhausted_crabd,
            &mut map_store,
            ConnectorRetryPolicy::default(),
            5,
            &mut exhausted_control,
        )
        .expect("retry exhaustion should be reported by receipt");
        assert_eq!(exhausted_stats.outbound_operations_processed, 1);
        assert_eq!(exhausted_stats.retry_waits, 2);
        assert_eq!(exhausted_discord.post_calls.len(), 3);
        assert_eq!(exhausted_discord.wait_calls, vec![500, 500]);
        assert_eq!(exhausted_crabd.inbound_frames.len(), 1);
        let is_error_receipt = matches!(
            exhausted_crabd.inbound_frames[0],
            CrabdInboundFrame::OutboundReceipt(CrabdOutboundReceipt {
                status: CrabdOutboundReceiptStatus::Error,
                ..
            })
        );
        assert!(is_error_receipt, "expected error receipt");

        let _ = fs::remove_file(map_path);
    }

    #[test]
    fn connector_loop_reports_fatal_discord_errors_and_idle_sleep_paths() {
        let map_path = temp_file_path("connector-loop-fatal");
        let mut map_store = DeliveryIdMapStore::load(&map_path).expect("map store should init");

        let mut discord = ScriptedDiscordIo {
            post_results: VecDeque::from([Err(DiscordApiError::Fatal {
                message: "unauthorized".to_string(),
            })]),
            ..ScriptedDiscordIo::default()
        };
        let mut crabd = ScriptedCrabdIo {
            outbound: VecDeque::from([Ok(Some(CrabdOutboundOp::Post {
                op_id: "op-1".to_string(),
                channel_id: "channel-1".to_string(),
                delivery_id: "delivery-1".to_string(),
                content: "hello".to_string(),
            }))]),
            ..ScriptedCrabdIo::default()
        };
        let mut control = ScriptedControl::new(1);

        let stats = run_connector_loop(
            &mut discord,
            &mut crabd,
            &mut map_store,
            ConnectorRetryPolicy::default(),
            10,
            &mut control,
        )
        .expect("fatal post error should emit error receipt");
        assert_eq!(stats.outbound_operations_processed, 1);
        assert_eq!(stats.retry_waits, 0);
        assert_eq!(crabd.inbound_frames.len(), 1);
        let is_error_receipt = matches!(
            crabd.inbound_frames[0],
            CrabdInboundFrame::OutboundReceipt(CrabdOutboundReceipt {
                status: CrabdOutboundReceiptStatus::Error,
                ..
            })
        );
        assert!(is_error_receipt, "expected error receipt");

        let mut idle_discord = ScriptedDiscordIo::default();
        let mut idle_crabd = ScriptedCrabdIo::default();
        let mut idle_control = ScriptedControl::new(2);

        let idle_stats = run_connector_loop(
            &mut idle_discord,
            &mut idle_crabd,
            &mut map_store,
            ConnectorRetryPolicy::default(),
            9,
            &mut idle_control,
        )
        .expect("idle loop should shutdown cleanly");
        assert_eq!(idle_stats, ConnectorLoopStats::default());
        assert_eq!(idle_control.sleeps, vec![9, 9]);
        assert_eq!(idle_crabd.inbound_frames, Vec::new());

        let _ = fs::remove_file(map_path);
    }

    #[test]
    fn connector_loop_skips_post_when_delivery_id_is_already_mapped() {
        let map_path = temp_file_path("connector-loop-skip-post");
        let mut map_store = DeliveryIdMapStore::load(&map_path).expect("map store should init");
        map_store.map.insert(
            DeliveryIdMapStore::key("channel-1", "delivery-1"),
            "actual-99".to_string(),
        );

        let mut discord = ScriptedDiscordIo::default();
        let mut crabd = ScriptedCrabdIo {
            outbound: VecDeque::from([
                Ok(Some(post_op("op-1", "channel-1", "delivery-1", "hello"))),
                Ok(None),
            ]),
            ..ScriptedCrabdIo::default()
        };
        let mut control = ScriptedControl::new(1);

        let stats = run_connector_loop(
            &mut discord,
            &mut crabd,
            &mut map_store,
            ConnectorRetryPolicy::default(),
            5,
            &mut control,
        )
        .expect("mapped delivery ids should short-circuit without posting");

        assert_eq!(stats.outbound_operations_processed, 1);
        assert_eq!(discord.post_calls.len(), 0);
        assert_eq!(
            crabd.inbound_frames,
            vec![ok_receipt_frame(
                "op-1",
                "channel-1",
                "delivery-1",
                "actual-99"
            )]
        );
        let _ = fs::remove_file(map_path);
    }

    #[test]
    fn connector_loop_reports_post_mapping_persist_errors_in_receipt() {
        let parent_file = temp_file_path("connector-loop-persist-error-parent");
        fs::write(&parent_file, "not-a-directory").expect("parent file should be writable");
        let map_path = parent_file.join("child.json");
        let mut map_store = DeliveryIdMapStore::load(&map_path).expect("map store should init");

        let mut discord = ScriptedDiscordIo::default();
        let mut crabd = ScriptedCrabdIo {
            outbound: VecDeque::from([
                Ok(Some(post_op("op-1", "channel-1", "delivery-1", "hello"))),
                Ok(None),
            ]),
            ..ScriptedCrabdIo::default()
        };
        let mut control = ScriptedControl::new(1);

        let stats = run_connector_loop(
            &mut discord,
            &mut crabd,
            &mut map_store,
            ConnectorRetryPolicy::default(),
            5,
            &mut control,
        )
        .expect("post mapping persistence failures should emit receipts");

        assert_eq!(stats.outbound_operations_processed, 1);
        assert_eq!(discord.post_calls.len(), 1);
        assert_eq!(crabd.inbound_frames.len(), 1);
        let is_expected = matches!(
            &crabd.inbound_frames[0],
            CrabdInboundFrame::OutboundReceipt(CrabdOutboundReceipt {
                op_id,
                status: CrabdOutboundReceiptStatus::Error,
                channel_id,
                delivery_id,
                discord_message_id: Some(discord_message_id),
                error_message: Some(error_message),
            }) if op_id == "op-1"
                && channel_id == "channel-1"
                && delivery_id == "delivery-1"
                && discord_message_id == "actual-1"
                && !error_message.trim().is_empty()
        );
        assert!(is_expected);

        let _ = fs::remove_file(parent_file);
    }

    #[test]
    fn connector_loop_rejects_edit_without_existing_message_id_mapping() {
        let map_path = temp_file_path("connector-loop-edit-missing-map");
        let mut map_store = DeliveryIdMapStore::load(&map_path).expect("map store should init");

        let mut discord = ScriptedDiscordIo::default();
        let mut crabd = ScriptedCrabdIo {
            outbound: VecDeque::from([
                Ok(Some(edit_op("op-1", "channel-1", "delivery-1", "updated"))),
                Ok(None),
            ]),
            ..ScriptedCrabdIo::default()
        };
        let mut control = ScriptedControl::new(1);

        let stats = run_connector_loop(
            &mut discord,
            &mut crabd,
            &mut map_store,
            ConnectorRetryPolicy::default(),
            5,
            &mut control,
        )
        .expect("missing mappings should emit error receipts");

        assert_eq!(stats.outbound_operations_processed, 1);
        assert_eq!(discord.edit_calls.len(), 0);
        assert_eq!(crabd.inbound_frames.len(), 1);
        let is_expected = matches!(
            &crabd.inbound_frames[0],
            CrabdInboundFrame::OutboundReceipt(CrabdOutboundReceipt {
                status: CrabdOutboundReceiptStatus::Error,
                discord_message_id: None,
                error_message: Some(error_message),
                ..
            }) if error_message.contains("missing discord message id mapping")
        );
        assert!(is_expected);

        let _ = fs::remove_file(map_path);
    }

    #[test]
    fn connector_loop_reports_edit_discord_api_errors_in_receipt() {
        let map_path = temp_file_path("connector-loop-edit-error");
        let mut map_store = DeliveryIdMapStore::load(&map_path).expect("map store should init");
        map_store.map.insert(
            DeliveryIdMapStore::key("channel-1", "delivery-1"),
            "actual-99".to_string(),
        );

        let mut discord = ScriptedDiscordIo {
            edit_results: VecDeque::from([Err(DiscordApiError::Fatal {
                message: "unauthorized".to_string(),
            })]),
            ..ScriptedDiscordIo::default()
        };
        let mut crabd = ScriptedCrabdIo {
            outbound: VecDeque::from([
                Ok(Some(edit_op("op-1", "channel-1", "delivery-1", "updated"))),
                Ok(None),
            ]),
            ..ScriptedCrabdIo::default()
        };
        let mut control = ScriptedControl::new(1);

        let stats = run_connector_loop(
            &mut discord,
            &mut crabd,
            &mut map_store,
            ConnectorRetryPolicy::default(),
            5,
            &mut control,
        )
        .expect("fatal edit errors should emit error receipts");

        assert_eq!(stats.outbound_operations_processed, 1);
        assert_eq!(discord.edit_calls.len(), 1);
        assert_eq!(crabd.inbound_frames.len(), 1);
        let is_expected = matches!(
            &crabd.inbound_frames[0],
            CrabdInboundFrame::OutboundReceipt(CrabdOutboundReceipt {
                status: CrabdOutboundReceiptStatus::Error,
                discord_message_id: Some(discord_message_id),
                error_message: Some(error_message),
                ..
            }) if discord_message_id == "actual-99" && !error_message.trim().is_empty()
        );
        assert!(is_expected);

        let _ = fs::remove_file(map_path);
    }

    #[test]
    fn helper_parsers_and_path_resolution_cover_edge_cases() {
        assert_eq!(parse_required_u64("5", "K").expect("valid"), 5);
        assert_eq!(parse_required_u8("3", "K").expect("valid"), 3);

        let bad_u64 = parse_required_u64("x", "K").expect_err("invalid");
        assert_eq!(
            bad_u64,
            CrabError::InvalidConfig {
                key: "K",
                value: "x".to_string(),
                reason: "must be a positive integer",
            }
        );

        let zero_u8 = parse_required_u8("0", "K").expect_err("zero invalid");
        assert_eq!(
            zero_u8,
            CrabError::InvalidConfig {
                key: "K",
                value: "0".to_string(),
                reason: "must be greater than 0",
            }
        );

        assert_eq!(
            parse_optional_u64(None, "K", 7).expect("default path should work"),
            7
        );
        assert_eq!(
            parse_optional_u8(None, "K", 2).expect("default path should work"),
            2
        );

        let retry_policy_error = ConnectorRetryPolicy {
            max_attempts: 1,
            retry_backoff_ms: 50,
            max_retry_backoff_ms: 40,
        }
        .validate()
        .expect_err("invalid backoff relation should fail");
        assert_eq!(
            retry_policy_error,
            CrabError::InvalidConfig {
                key: "CRAB_CONNECTOR_RETRY_BACKOFF_MS",
                value: "50".to_string(),
                reason: "must be less than or equal to CRAB_CONNECTOR_RETRY_MAX_BACKOFF_MS",
            }
        );

        let rate_limited_backoff =
            ConnectorRetryPolicy::default().backoff_ms(&DiscordApiError::RateLimited {
                retry_after_ms: Some(99_999),
                message: "later".to_string(),
            });
        assert_eq!(
            rate_limited_backoff,
            ConnectorRetryPolicy::default().max_retry_backoff_ms
        );

        let home = env::var("HOME").expect("HOME should be set in test env");
        assert_eq!(expand_home_path("~"), PathBuf::from(&home));
        assert_eq!(expand_home_path("~/abc"), PathBuf::from(&home).join("abc"));
        assert_eq!(expand_home_path_with_home("~", None), PathBuf::from("~"));
        assert_eq!(
            expand_home_path_with_home("~/abc", None),
            PathBuf::from("~/abc")
        );

        let resolved = resolve_message_id_map_path(&values(&[("CRAB_WORKSPACE_ROOT", "~/w")]))
            .expect("workspace-root default should resolve");
        assert!(resolved.ends_with(Path::new("w").join(DEFAULT_MESSAGE_ID_MAP_FILE)));

        let resolved_default = resolve_message_id_map_path(&HashMap::new())
            .expect("default workspace root should resolve");
        assert!(resolved_default.ends_with(Path::new(DEFAULT_MESSAGE_ID_MAP_FILE)));

        let explicit = resolve_message_id_map_path(&values(&[(
            "CRAB_CONNECTOR_MESSAGE_ID_MAP_PATH",
            "/tmp/custom-map.json",
        )]))
        .expect("explicit map path should resolve");
        assert_eq!(explicit, PathBuf::from("/tmp/custom-map.json"));

        let explicit_blank =
            resolve_message_id_map_path(&values(&[("CRAB_CONNECTOR_MESSAGE_ID_MAP_PATH", "   ")]))
                .expect_err("blank explicit map path should fail");
        assert_eq!(
            explicit_blank,
            CrabError::InvariantViolation {
                context: "connector_message_id_map_path",
                message: "CRAB_CONNECTOR_MESSAGE_ID_MAP_PATH must not be empty".to_string(),
            }
        );

        let empty_err = ensure_non_empty("ctx", "f", "  ").expect_err("blank should fail");
        assert_eq!(
            empty_err,
            CrabError::InvariantViolation {
                context: "ctx",
                message: "f must not be empty".to_string(),
            }
        );

        let fatal_message = DiscordApiError::Fatal {
            message: "x".to_string(),
        };
        assert!(!fatal_message.is_retryable());
        assert_eq!(fatal_message.message(), "x");

        let rate_limited_message = DiscordApiError::RateLimited {
            retry_after_ms: None,
            message: "y".to_string(),
        };
        assert_eq!(rate_limited_message.message(), "y");
    }

    #[test]
    fn message_id_map_store_load_surfaces_read_and_parse_errors() {
        let path = temp_file_path("connector-map-errors");

        fs::write(&path, "{").expect("write should succeed");
        let parse_error = DeliveryIdMapStore::load(&path).expect_err("parse should fail");
        assert!(matches!(
            parse_error,
            CrabError::Serialization {
                context: "connector_message_id_map_parse",
                ..
            }
        ));

        fs::write(&path, "   \n").expect("write should succeed");
        let empty_loaded = DeliveryIdMapStore::load(&path).expect("empty file should load");
        assert!(empty_loaded.map.is_empty());

        let directory_path = env::temp_dir().join(format!(
            "connector-map-dir-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock should work")
                .as_nanos()
        ));
        fs::create_dir(&directory_path).expect("create dir should succeed");

        let read_error =
            DeliveryIdMapStore::load(&directory_path).expect_err("reading directory should fail");
        assert!(matches!(
            read_error,
            CrabError::Io {
                context: "connector_message_id_map_read",
                ..
            }
        ));

        let _ = fs::remove_file(path);
        let _ = fs::remove_dir(directory_path);
    }

    #[test]
    fn connector_retry_policy_validation_covers_all_branches() {
        let attempts_error = ConnectorRetryPolicy {
            max_attempts: 0,
            ..ConnectorRetryPolicy::default()
        }
        .validate()
        .expect_err("max attempts of zero should be rejected");
        assert_eq!(
            attempts_error,
            CrabError::InvalidConfig {
                key: "CRAB_CONNECTOR_RETRY_MAX_ATTEMPTS",
                value: "0".to_string(),
                reason: "must be greater than 0",
            }
        );

        let backoff_error = ConnectorRetryPolicy {
            max_attempts: 1,
            retry_backoff_ms: 0,
            max_retry_backoff_ms: 10,
        }
        .validate()
        .expect_err("retry backoff of zero should be rejected");
        assert_eq!(
            backoff_error,
            CrabError::InvalidConfig {
                key: "CRAB_CONNECTOR_RETRY_BACKOFF_MS",
                value: "0".to_string(),
                reason: "must be greater than 0",
            }
        );

        let max_backoff_error = ConnectorRetryPolicy {
            max_attempts: 1,
            retry_backoff_ms: 10,
            max_retry_backoff_ms: 0,
        }
        .validate()
        .expect_err("max retry backoff of zero should be rejected");
        assert_eq!(
            max_backoff_error,
            CrabError::InvalidConfig {
                key: "CRAB_CONNECTOR_RETRY_MAX_BACKOFF_MS",
                value: "0".to_string(),
                reason: "must be greater than 0",
            }
        );

        let policy = ConnectorRetryPolicy::default();
        assert_eq!(
            policy.backoff_ms(&DiscordApiError::RateLimited {
                retry_after_ms: None,
                message: "x".to_string(),
            }),
            policy.retry_backoff_ms
        );
        assert_eq!(
            policy.backoff_ms(&DiscordApiError::RateLimited {
                retry_after_ms: Some(0),
                message: "x".to_string(),
            }),
            policy.retry_backoff_ms
        );
        assert_eq!(
            policy.backoff_ms(&DiscordApiError::Retryable {
                message: "x".to_string(),
            }),
            policy.retry_backoff_ms
        );

        assert_eq!(
            DiscordApiError::Retryable {
                message: "y".to_string()
            }
            .message(),
            "y"
        );
    }

    #[test]
    fn message_id_map_store_persist_surfaces_mkdir_and_write_errors() {
        let root = env::temp_dir().join(format!(
            "connector-map-persist-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock should work")
                .as_nanos()
        ));
        fs::create_dir(&root).expect("create root dir should succeed");

        let blocking_file = root.join("blocker");
        fs::write(&blocking_file, "x").expect("write blocking file");

        let path = blocking_file.join("map.json");
        let mut store = DeliveryIdMapStore::load(&path).expect("load should succeed for missing");
        let parent_error = store
            .remember_post_mapping("c", "s", "a")
            .expect_err("mkdir under file should fail");
        assert!(matches!(
            parent_error,
            CrabError::Io {
                context: "connector_message_id_map_parent_mkdir",
                ..
            }
        ));

        let path_with_tmp_file = root.join("map2.json");
        let mut store2 =
            DeliveryIdMapStore::load(&path_with_tmp_file).expect("load should succeed for missing");
        store2.map.insert("k".to_string(), "v".to_string());
        let tmp_path = path_with_tmp_file.with_extension("tmp");
        fs::create_dir(&tmp_path).expect("tmp path directory should exist");

        let write_error = store2
            .persist()
            .expect_err("writing tmp path dir should fail");
        assert!(matches!(
            write_error,
            CrabError::Io {
                context: "connector_message_id_map_tmp_write",
                ..
            }
        ));

        let rename_target = root.join("map3.json");
        let mut store3 =
            DeliveryIdMapStore::load(&rename_target).expect("load should succeed for missing");
        store3.map.insert("k".to_string(), "v".to_string());
        fs::create_dir(&rename_target).expect("rename target directory should exist");
        let rename_error = store3.persist().expect_err("rename should fail");
        assert!(matches!(
            rename_error,
            CrabError::Io {
                context: "connector_message_id_map_rename",
                ..
            }
        ));

        let _ = fs::remove_dir(tmp_path);
        let _ = fs::remove_file(rename_target.with_extension("tmp"));
        let _ = fs::remove_dir(rename_target);
        let _ = fs::remove_file(blocking_file);
        let _ = fs::remove_dir(root);
    }

    #[test]
    fn message_id_map_store_persist_skips_parent_mkdir_when_parent_is_none() {
        let store = DeliveryIdMapStore {
            path: PathBuf::from(std::path::MAIN_SEPARATOR.to_string()),
            map: BTreeMap::from([("k".to_string(), "v".to_string())]),
        };
        let error = store
            .persist()
            .expect_err("writing to filesystem root should fail");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "connector_message_id_map_tmp_write",
                ..
            }
        ));
    }

    #[test]
    fn live_discord_stub_methods_are_consistent() {
        let mut discord = live_discord::LiveDiscordIo;
        assert_eq!(
            discord.next_gateway_message().expect("next should succeed"),
            None
        );
        let post_error = discord
            .post_message("c", "hi")
            .expect_err("post should fail");
        assert!(matches!(post_error, DiscordApiError::Fatal { .. }));
        let edit_error = discord
            .edit_message("c", "m", "hi")
            .expect_err("edit should fail");
        assert!(matches!(edit_error, DiscordApiError::Fatal { .. }));
        discord
            .wait_for_retry(0)
            .expect("wait should succeed in stub");
    }

    #[cfg(coverage)]
    #[test]
    fn coverage_main_is_callable() {
        crate::main();
    }

    #[test]
    fn stub_live_discord_module_reports_unavailable() {
        let error = live_discord::LiveDiscordIo::connect("token".to_string())
            .expect_err("stub connector should reject live startup in tests");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "connector_live_discord",
                ..
            }
        ));
    }
}
