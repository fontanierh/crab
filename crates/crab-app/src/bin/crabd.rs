use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io::{self, BufRead, Write};
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;
#[cfg(test)]
use std::{cell::RefCell, thread_local};
#[cfg(not(test))]
use std::{io::BufReader, thread};

use crab_app::SystemDaemonLoopControl;
use crab_app::{
    run_daemon_loop_with_transport, DaemonConfig, DaemonDiscordIo, DaemonLoopControl,
    DaemonLoopStats, DEFAULT_DAEMON_TICK_INTERVAL_MS,
};
use crab_backends::{CodexAppServerProcess, CodexProcessHandle, OpenCodeServerHandle};
use crab_core::{CrabError, CrabResult, RuntimeConfig};
use crab_discord::{
    CrabdInboundFrame, CrabdOutboundOp, CrabdOutboundReceipt, CrabdOutboundReceiptStatus,
    GatewayMessage,
};
#[cfg(not(test))]
use crab_telemetry::init_tracing_stderr;

#[derive(Debug, Clone, Default)]
struct LocalCodexProcess;

#[cfg(test)]
type LocalCodexSpawnOverride = fn() -> CrabResult<CodexProcessHandle>;

#[cfg(test)]
thread_local! {
    static LOCAL_CODEX_SPAWN_OVERRIDE: RefCell<Option<LocalCodexSpawnOverride>> = RefCell::new(None);
}

impl CodexAppServerProcess for LocalCodexProcess {
    fn spawn_app_server(&self) -> CrabResult<CodexProcessHandle> {
        #[cfg(test)]
        if let Some(spawn_override) = LOCAL_CODEX_SPAWN_OVERRIDE.with(|cell| *cell.borrow()) {
            return spawn_override();
        }

        Ok(CodexProcessHandle {
            process_id: 1,
            started_at_epoch_ms: 1,
        })
    }

    fn is_healthy(&self, _handle: &CodexProcessHandle) -> bool {
        true
    }

    fn terminate_app_server(&self, _handle: &CodexProcessHandle) -> CrabResult<()> {
        Ok(())
    }
}

impl LocalCodexProcess {
    #[cfg(test)]
    fn set_spawn_override(spawn_override: Option<LocalCodexSpawnOverride>) {
        LOCAL_CODEX_SPAWN_OVERRIDE.with(|cell| {
            *cell.borrow_mut() = spawn_override;
        });
    }
}

#[derive(Debug, Clone, Default)]
struct LocalOpenCodeProcess;

#[cfg(test)]
type LocalOpenCodeSpawnOverride = fn() -> CrabResult<OpenCodeServerHandle>;

#[cfg(test)]
thread_local! {
    static LOCAL_OPENCODE_SPAWN_OVERRIDE: RefCell<Option<LocalOpenCodeSpawnOverride>> = RefCell::new(None);
}

impl crab_backends::OpenCodeServerProcess for LocalOpenCodeProcess {
    fn spawn_server(&self) -> CrabResult<OpenCodeServerHandle> {
        #[cfg(test)]
        if let Some(spawn_override) = LOCAL_OPENCODE_SPAWN_OVERRIDE.with(|cell| *cell.borrow()) {
            return spawn_override();
        }

        Ok(OpenCodeServerHandle {
            process_id: 2,
            started_at_epoch_ms: 1,
            server_base_url: "http://127.0.0.1:4210".to_string(),
        })
    }

    fn is_server_healthy(&self, _handle: &OpenCodeServerHandle) -> bool {
        true
    }

    fn terminate_server(&self, _handle: &OpenCodeServerHandle) -> CrabResult<()> {
        Ok(())
    }
}

impl LocalOpenCodeProcess {
    #[cfg(test)]
    fn set_spawn_override(spawn_override: Option<LocalOpenCodeSpawnOverride>) {
        LOCAL_OPENCODE_SPAWN_OVERRIDE.with(|cell| {
            *cell.borrow_mut() = spawn_override;
        });
    }
}

const DEFAULT_OUTBOUND_RECEIPT_TIMEOUT_MS: u64 = 120_000;

#[derive(Debug)]
struct StdioDiscordIo {
    inbound_messages: Arc<Mutex<VecDeque<GatewayMessage>>>,
    inbound_error: Arc<Mutex<Option<CrabError>>>,
    pending_receipts: Arc<Mutex<BTreeMap<String, mpsc::Sender<CrabdOutboundReceipt>>>>,
    early_receipts: Arc<Mutex<BTreeMap<String, CrabdOutboundReceipt>>>,
    next_outbound_op_sequence: u64,
    receipt_timeout_ms: u64,
}

#[cfg(test)]
type OutboundWriteOverride = fn(&str) -> CrabResult<()>;

#[cfg(test)]
thread_local! {
    static OUTBOUND_WRITE_OVERRIDE: RefCell<Option<OutboundWriteOverride>> = RefCell::new(None);
}

impl StdioDiscordIo {
    fn new(receipt_timeout_ms: u64) -> Self {
        Self {
            inbound_messages: Arc::new(Mutex::new(VecDeque::new())),
            inbound_error: Arc::new(Mutex::new(None)),
            pending_receipts: Arc::new(Mutex::new(BTreeMap::new())),
            early_receipts: Arc::new(Mutex::new(BTreeMap::new())),
            next_outbound_op_sequence: 0,
            receipt_timeout_ms,
        }
    }

    #[cfg(not(test))]
    fn spawn_from_stdin(receipt_timeout_ms: u64) -> CrabResult<Self> {
        let io = Self::new(receipt_timeout_ms);

        let inbound_messages = Arc::clone(&io.inbound_messages);
        let inbound_error = Arc::clone(&io.inbound_error);
        let pending_receipts = Arc::clone(&io.pending_receipts);
        let early_receipts = Arc::clone(&io.early_receipts);

        thread::spawn(move || {
            let stdin = io::stdin();
            let reader = BufReader::new(stdin);

            for line_result in reader.lines() {
                let line = match line_result {
                    Ok(line) => line,
                    Err(error) => {
                        let mut guard = inbound_error.lock().expect("lock should succeed");
                        *guard = Some(CrabError::Io {
                            context: "crabd_stdin_read",
                            path: None,
                            message: error.to_string(),
                        });
                        break;
                    }
                };

                if line.trim().is_empty() {
                    continue;
                }

                let frame = match serde_json::from_str::<CrabdInboundFrame>(&line) {
                    Ok(frame) => frame,
                    Err(error) => {
                        let mut guard = inbound_error.lock().expect("lock should succeed");
                        *guard = Some(CrabError::Serialization {
                            context: "crabd_stdin_parse_inbound_frame",
                            path: None,
                            message: error.to_string(),
                        });
                        break;
                    }
                };

                match frame {
                    CrabdInboundFrame::GatewayMessage(message) => inbound_messages
                        .lock()
                        .expect("lock should succeed")
                        .push_back(message),
                    CrabdInboundFrame::OutboundReceipt(receipt) => {
                        let mut pending = pending_receipts.lock().expect("lock should succeed");
                        if let Some(sender) = pending.remove(&receipt.op_id) {
                            let _ = sender.send(receipt);
                        } else {
                            early_receipts
                                .lock()
                                .expect("lock should succeed")
                                .insert(receipt.op_id.clone(), receipt);
                        }
                    }
                }
            }
        });

        Ok(io)
    }

    #[cfg(test)]
    fn from_reader<R: BufRead>(reader: R, receipt_timeout_ms: u64) -> CrabResult<Self> {
        let mut reader = reader;
        Self::from_bufread(&mut reader, receipt_timeout_ms)
    }

    #[cfg(test)]
    fn from_bufread(reader: &mut dyn BufRead, receipt_timeout_ms: u64) -> CrabResult<Self> {
        let mut io = Self::new(receipt_timeout_ms);

        for line in reader.lines() {
            let line = line.map_err(|error| CrabError::Io {
                context: "crabd_stdin_read",
                path: None,
                message: error.to_string(),
            })?;
            if line.trim().is_empty() {
                continue;
            }

            let frame = serde_json::from_str::<CrabdInboundFrame>(&line).map_err(|error| {
                CrabError::Serialization {
                    context: "crabd_stdin_parse_inbound_frame",
                    path: None,
                    message: error.to_string(),
                }
            })?;
            io.ingest_inbound_frame(frame);
        }

        Ok(io)
    }

    #[cfg(test)]
    fn ingest_inbound_frame(&mut self, frame: CrabdInboundFrame) {
        match frame {
            CrabdInboundFrame::GatewayMessage(message) => self
                .inbound_messages
                .lock()
                .expect("lock should succeed")
                .push_back(message),
            CrabdInboundFrame::OutboundReceipt(receipt) => {
                self.early_receipts
                    .lock()
                    .expect("lock should succeed")
                    .insert(receipt.op_id.clone(), receipt);
            }
        }
    }

    fn next_op_id(&mut self) -> String {
        self.next_outbound_op_sequence = self.next_outbound_op_sequence.saturating_add(1);
        format!("op-{}", self.next_outbound_op_sequence)
    }

    fn write_outbound_op(op: &CrabdOutboundOp) -> CrabResult<()> {
        let line = serde_json::to_string(op).expect("outbound op serialization should not fail");

        #[cfg(test)]
        if let Some(write_override) = OUTBOUND_WRITE_OVERRIDE.with(|cell| *cell.borrow()) {
            return write_override(&line);
        }

        let mut stdout = io::stdout();
        Self::write_outbound_line_to_writer(&mut stdout, &line)
    }

    #[cfg(test)]
    fn set_outbound_write_override(write_override: Option<OutboundWriteOverride>) {
        OUTBOUND_WRITE_OVERRIDE.with(|cell| {
            *cell.borrow_mut() = write_override;
        });
    }

    fn write_outbound_line_to_writer(writer: &mut dyn Write, line: &str) -> CrabResult<()> {
        writer
            .write_all(line.as_bytes())
            .map_err(|error| CrabError::Io {
                context: "crabd_stdout_write_outbound_op",
                path: None,
                message: format!("failed to write outbound payload: {error}"),
            })?;
        writer.write_all(b"\n").map_err(|error| CrabError::Io {
            context: "crabd_stdout_write_outbound_op",
            path: None,
            message: format!("failed to write outbound newline: {error}"),
        })?;
        writer.flush().map_err(|error| CrabError::Io {
            context: "crabd_stdout_write_outbound_op",
            path: None,
            message: format!("failed to flush outbound payload: {error}"),
        })?;
        Ok(())
    }

    fn send_outbound_op_and_wait(
        &mut self,
        op: CrabdOutboundOp,
    ) -> CrabResult<CrabdOutboundReceipt> {
        let op_id = match &op {
            CrabdOutboundOp::Post { op_id, .. } | CrabdOutboundOp::Edit { op_id, .. } => op_id,
        }
        .clone();

        let (sender, receiver) = mpsc::channel();
        self.pending_receipts
            .lock()
            .expect("lock should succeed")
            .insert(op_id.clone(), sender);

        if let Err(error) = Self::write_outbound_op(&op) {
            let _ = self
                .pending_receipts
                .lock()
                .expect("lock should succeed")
                .remove(&op_id);
            return Err(error);
        }

        if let Some(receipt) = self
            .early_receipts
            .lock()
            .expect("lock should succeed")
            .remove(&op_id)
        {
            // Tests can pre-load receipts before we emit the corresponding outbound op.
            let _ = self
                .pending_receipts
                .lock()
                .expect("lock should succeed")
                .remove(&op_id);
            return Ok(receipt);
        }

        match receiver.recv_timeout(Duration::from_millis(self.receipt_timeout_ms)) {
            Ok(receipt) => Ok(receipt),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let _ = self
                    .pending_receipts
                    .lock()
                    .expect("lock should succeed")
                    .remove(&op_id);
                Err(CrabError::InvariantViolation {
                    context: "crabd_outbound_receipt_timeout",
                    message: format!("timed out waiting for receipt for op_id={op_id}"),
                })
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let _ = self
                    .pending_receipts
                    .lock()
                    .expect("lock should succeed")
                    .remove(&op_id);
                Err(CrabError::InvariantViolation {
                    context: "crabd_outbound_receipt_channel",
                    message: format!("receipt channel disconnected for op_id={op_id}"),
                })
            }
        }
    }
}

impl DaemonDiscordIo for StdioDiscordIo {
    fn next_gateway_message(&mut self) -> CrabResult<Option<GatewayMessage>> {
        if let Some(error) = self
            .inbound_error
            .lock()
            .expect("lock should succeed")
            .clone()
        {
            return Err(error);
        }

        Ok(self
            .inbound_messages
            .lock()
            .expect("lock should succeed")
            .pop_front())
    }

    fn post_message(
        &mut self,
        channel_id: &str,
        delivery_id: &str,
        content: &str,
    ) -> CrabResult<()> {
        let op_id = self.next_op_id();
        let receipt = self.send_outbound_op_and_wait(CrabdOutboundOp::Post {
            op_id: op_id.clone(),
            channel_id: channel_id.to_string(),
            delivery_id: delivery_id.to_string(),
            content: content.to_string(),
        })?;
        if receipt.status == CrabdOutboundReceiptStatus::Ok {
            return Ok(());
        }

        Err(CrabError::InvariantViolation {
            context: "crabd_outbound_receipt_error",
            message: format!(
                "post failed for op_id={op_id}, delivery_id={delivery_id}: {}",
                receipt
                    .error_message
                    .unwrap_or_else(|| "unknown error".to_string())
            ),
        })
    }

    fn edit_message(
        &mut self,
        channel_id: &str,
        delivery_id: &str,
        content: &str,
    ) -> CrabResult<()> {
        let op_id = self.next_op_id();
        let receipt = self.send_outbound_op_and_wait(CrabdOutboundOp::Edit {
            op_id: op_id.clone(),
            channel_id: channel_id.to_string(),
            delivery_id: delivery_id.to_string(),
            content: content.to_string(),
        })?;
        if receipt.status == CrabdOutboundReceiptStatus::Ok {
            return Ok(());
        }

        Err(CrabError::InvariantViolation {
            context: "crabd_outbound_receipt_error",
            message: format!(
                "edit failed for op_id={op_id}, delivery_id={delivery_id}: {}",
                receipt
                    .error_message
                    .unwrap_or_else(|| "unknown error".to_string())
            ),
        })
    }
}

fn parse_daemon_config(values: &HashMap<String, String>) -> CrabResult<DaemonConfig> {
    let bot_user_id = values
        .get("CRAB_BOT_USER_ID")
        .cloned()
        .ok_or(CrabError::MissingConfig {
            key: "CRAB_BOT_USER_ID",
        })?;
    let tick_interval_ms = parse_optional_u64(
        values.get("CRAB_DAEMON_TICK_INTERVAL_MS"),
        "CRAB_DAEMON_TICK_INTERVAL_MS",
        DEFAULT_DAEMON_TICK_INTERVAL_MS,
    )?;
    let max_iterations = match values.get("CRAB_DAEMON_MAX_ITERATIONS") {
        Some(raw_value) => Some(parse_required_u64(raw_value, "CRAB_DAEMON_MAX_ITERATIONS")?),
        None => None,
    };

    let config = DaemonConfig {
        bot_user_id,
        tick_interval_ms,
        max_iterations,
    };
    config.validate()?;
    Ok(config)
}

fn parse_optional_u64(
    raw_value: Option<&String>,
    key: &'static str,
    default_value: u64,
) -> CrabResult<u64> {
    match raw_value {
        Some(value) => parse_required_u64(value, key),
        None => Ok(default_value),
    }
}

fn parse_required_u64(raw_value: &str, key: &'static str) -> CrabResult<u64> {
    let value = u64::from_str(raw_value).map_err(|_| CrabError::InvalidConfig {
        key,
        value: raw_value.to_string(),
        reason: "must be a positive integer",
    })?;
    if value == 0 {
        return Err(CrabError::InvalidConfig {
            key,
            value: raw_value.to_string(),
            reason: "must be greater than 0",
        });
    }
    Ok(value)
}

fn parse_receipt_timeout_ms(values: &HashMap<String, String>) -> CrabResult<u64> {
    parse_optional_u64(
        values.get("CRAB_OUTBOUND_RECEIPT_TIMEOUT_MS"),
        "CRAB_OUTBOUND_RECEIPT_TIMEOUT_MS",
        DEFAULT_OUTBOUND_RECEIPT_TIMEOUT_MS,
    )
}

fn run_with_values_and_discord_and_control(
    values: &HashMap<String, String>,
    discord: StdioDiscordIo,
    control: &mut dyn DaemonLoopControl,
) -> CrabResult<DaemonLoopStats> {
    let runtime_config = RuntimeConfig::from_map(values)?;
    let daemon_config = parse_daemon_config(values)?;
    tracing::info!(
        bot_user_id = %daemon_config.bot_user_id,
        workspace_root = %runtime_config.workspace_root,
        max_concurrent_lanes = runtime_config.max_concurrent_lanes,
        compaction_token_threshold = runtime_config.rotation.compaction_token_threshold,
        inactivity_timeout_secs = runtime_config.rotation.inactivity_timeout_secs,
        startup_reconciliation_grace_period_secs = runtime_config.startup_reconciliation.grace_period_secs,
        heartbeat_interval_secs = runtime_config.heartbeat.interval_secs,
        run_stall_timeout_secs = runtime_config.heartbeat.run_stall_timeout_secs,
        backend_stall_timeout_secs = runtime_config.heartbeat.backend_stall_timeout_secs,
        dispatcher_stall_timeout_secs = runtime_config.heartbeat.dispatcher_stall_timeout_secs,
        "crabd starting"
    );
    run_daemon_loop_with_transport(
        &runtime_config,
        &daemon_config,
        LocalCodexProcess,
        LocalOpenCodeProcess,
        discord,
        control,
    )
}

#[cfg(test)]
fn run_with_reader_and_control(
    values: &HashMap<String, String>,
    reader: &mut dyn BufRead,
    control: &mut dyn DaemonLoopControl,
) -> CrabResult<DaemonLoopStats> {
    let receipt_timeout_ms = parse_receipt_timeout_ms(values)?;
    let discord = StdioDiscordIo::from_bufread(reader, receipt_timeout_ms)?;
    run_with_values_and_discord_and_control(values, discord, control)
}

fn run_main_with_runner(runner: fn() -> CrabResult<DaemonLoopStats>) -> ExitCode {
    match runner() {
        Ok(stats) => {
            tracing::info!(
                iterations = stats.iterations,
                ingested = stats.ingested_messages,
                dispatched = stats.dispatched_runs,
                heartbeats = stats.heartbeat_cycles,
                "crabd finished"
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            tracing::error!(error = %error, "crabd failed");
            ExitCode::FAILURE
        }
    }
}

#[cfg(not(test))]
fn run_with_env_and_control_installer(
    control_installer: fn() -> CrabResult<SystemDaemonLoopControl>,
) -> CrabResult<DaemonLoopStats> {
    let values = std::env::vars().collect::<HashMap<_, _>>();
    let receipt_timeout_ms = parse_receipt_timeout_ms(&values)?;
    let discord = StdioDiscordIo::spawn_from_stdin(receipt_timeout_ms)?;
    let mut control = control_installer()?;
    run_with_values_and_discord_and_control(&values, discord, &mut control)
}

#[cfg(not(test))]
fn run_with_env_and_stdio() -> CrabResult<DaemonLoopStats> {
    run_with_env_and_control_installer(SystemDaemonLoopControl::install)
}

#[cfg(test)]
fn run_with_env_and_reader_and_control_installer(
    reader: &mut dyn BufRead,
    control_installer: fn() -> CrabResult<SystemDaemonLoopControl>,
) -> CrabResult<DaemonLoopStats> {
    let values = std::env::vars().collect::<HashMap<_, _>>();
    let mut control = control_installer()?;
    run_with_reader_and_control(&values, reader, &mut control)
}

#[cfg(test)]
fn run_with_env_and_stdio() -> CrabResult<DaemonLoopStats> {
    let mut reader = std::io::Cursor::new("");
    run_with_env_and_reader_and_control_installer(&mut reader, SystemDaemonLoopControl::install)
}

fn main() -> ExitCode {
    #[cfg(not(test))]
    init_tracing_stderr("info");
    run_main_with_runner(run_with_env_and_stdio)
}

#[cfg(test)]
mod tests {
    use super::{
        main, parse_daemon_config, parse_optional_u64, parse_required_u64, run_main_with_runner,
        run_with_env_and_reader_and_control_installer, run_with_reader_and_control,
        LocalCodexProcess, LocalOpenCodeProcess, StdioDiscordIo,
    };
    use crab_app::{DaemonDiscordIo, DaemonLoopControl, DaemonLoopStats};
    use crab_backends::{
        CodexAppServerProcess, CodexProcessHandle, OpenCodeServerHandle, OpenCodeServerProcess,
    };
    use crab_core::{CrabError, CrabResult};
    use crab_discord::{
        CrabdInboundFrame, CrabdOutboundOp, CrabdOutboundReceipt, CrabdOutboundReceiptStatus,
        GatewayConversationKind, GatewayMessage,
    };
    use std::collections::HashMap;
    use std::collections::VecDeque;
    use std::io::{self, Cursor, Read, Write};
    use std::mem;
    use std::sync::{Arc, Mutex, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};
    use std::{cell::RefCell, thread_local};

    thread_local! {
        static CAPTURED_OUTBOUND_LINES: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
    }

    #[derive(Debug, Clone)]
    struct ScriptedControl {
        now_values: VecDeque<u64>,
        slept: Vec<u64>,
    }

    impl ScriptedControl {
        fn with_now(now_values: Vec<u64>) -> Self {
            Self {
                now_values: VecDeque::from(now_values),
                slept: Vec::new(),
            }
        }
    }

    impl DaemonLoopControl for ScriptedControl {
        fn now_epoch_ms(&mut self) -> CrabResult<u64> {
            self.now_values
                .pop_front()
                .ok_or(CrabError::InvariantViolation {
                    context: "crabd_test_now",
                    message: "missing scripted time".to_string(),
                })
        }

        fn should_shutdown(&self) -> bool {
            false
        }

        fn sleep_tick(&mut self, tick_interval_ms: u64) -> CrabResult<()> {
            self.slept.push(tick_interval_ms);
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct ScriptedWriter {
        fail_payload_write: bool,
        fail_newline_write: bool,
        fail_flush: bool,
        write_calls: usize,
        bytes: Vec<u8>,
    }

    impl Write for ScriptedWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.write_calls = self.write_calls.saturating_add(1);
            if self.fail_payload_write && self.write_calls == 1 {
                return Err(io::Error::other("payload write failure"));
            }
            if self.fail_newline_write && self.write_calls == 2 {
                return Err(io::Error::other("newline write failure"));
            }
            self.bytes.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            if self.fail_flush {
                return Err(io::Error::other("flush failure"));
            }
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct FailingLineReader;

    impl Read for FailingLineReader {
        fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::other("line read failure"))
        }
    }

    impl io::BufRead for FailingLineReader {
        fn fill_buf(&mut self) -> io::Result<&[u8]> {
            Err(io::Error::other("line read failure"))
        }

        fn consume(&mut self, _amt: usize) {}
    }

    fn capture_outbound_write(line: &str) -> CrabResult<()> {
        CAPTURED_OUTBOUND_LINES.with(|cell| {
            cell.borrow_mut().push(line.to_string());
        });
        Ok(())
    }

    fn take_captured_outbound_lines() -> Vec<String> {
        CAPTURED_OUTBOUND_LINES.with(|cell| mem::take(&mut *cell.borrow_mut()))
    }

    fn fail_outbound_write(_line: &str) -> CrabResult<()> {
        Err(CrabError::Io {
            context: "crabd_test_outbound_write",
            path: None,
            message: "forced outbound write failure".to_string(),
        })
    }

    fn fail_control_install() -> CrabResult<crate::SystemDaemonLoopControl> {
        Err(CrabError::InvariantViolation {
            context: "crabd_control_install",
            message: "forced install failure".to_string(),
        })
    }

    fn fail_local_codex_spawn() -> CrabResult<CodexProcessHandle> {
        Err(CrabError::InvariantViolation {
            context: "crabd_local_codex_spawn",
            message: "forced codex spawn failure".to_string(),
        })
    }

    fn fail_local_opencode_spawn() -> CrabResult<OpenCodeServerHandle> {
        Err(CrabError::InvariantViolation {
            context: "crabd_local_opencode_spawn",
            message: "forced opencode spawn failure".to_string(),
        })
    }

    fn run_main_success() -> CrabResult<DaemonLoopStats> {
        Ok(DaemonLoopStats::default())
    }

    fn run_main_failure() -> CrabResult<DaemonLoopStats> {
        Err(CrabError::InvariantViolation {
            context: "test",
            message: "boom".to_string(),
        })
    }

    fn gateway_inbound_frame_json(message_id: &str, content: &str) -> String {
        let message = GatewayMessage {
            message_id: message_id.to_string(),
            author_id: "111".to_string(),
            author_is_bot: false,
            channel_id: "777".to_string(),
            guild_id: Some("555".to_string()),
            thread_id: None,
            content: content.to_string(),
            conversation_kind: GatewayConversationKind::GuildChannel,
        };
        serde_json::to_string(&CrabdInboundFrame::GatewayMessage(message))
            .expect("frame json should serialize")
    }

    fn ok_receipt_inbound_frame_json(op_id: &str, channel_id: &str, delivery_id: &str) -> String {
        serde_json::to_string(&CrabdInboundFrame::OutboundReceipt(CrabdOutboundReceipt {
            op_id: op_id.to_string(),
            status: CrabdOutboundReceiptStatus::Ok,
            channel_id: channel_id.to_string(),
            delivery_id: delivery_id.to_string(),
            discord_message_id: Some("discord-msg-1".to_string()),
            error_message: None,
        }))
        .expect("receipt json should serialize")
    }

    fn runtime_values(workspace_root: &str) -> HashMap<String, String> {
        HashMap::from([
            ("CRAB_DISCORD_TOKEN".to_string(), "test-token".to_string()),
            (
                "CRAB_WORKSPACE_ROOT".to_string(),
                workspace_root.to_string(),
            ),
            ("CRAB_BOT_USER_ID".to_string(), "999".to_string()),
            ("CRAB_DAEMON_TICK_INTERVAL_MS".to_string(), "1".to_string()),
            ("CRAB_DAEMON_MAX_ITERATIONS".to_string(), "1".to_string()),
            (
                "CRAB_OUTBOUND_RECEIPT_TIMEOUT_MS".to_string(),
                "10".to_string(),
            ),
        ])
    }

    fn temp_workspace_root(suffix: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be after epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("crabd-bin-{suffix}-{nanos}"));
        std::fs::create_dir_all(&root).expect("workspace directory should be creatable");
        root.to_string_lossy().into_owned()
    }

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn with_test_env<F>(values: &HashMap<String, String>, f: F)
    where
        F: FnOnce(),
    {
        let _guard = env_lock().lock().expect("env lock should succeed");
        let keys = [
            "CRAB_DISCORD_TOKEN",
            "CRAB_WORKSPACE_ROOT",
            "CRAB_BOT_USER_ID",
            "CRAB_DAEMON_TICK_INTERVAL_MS",
            "CRAB_DAEMON_MAX_ITERATIONS",
            "CRAB_OUTBOUND_RECEIPT_TIMEOUT_MS",
        ];
        let previous: Vec<(String, Option<String>)> = keys
            .iter()
            .map(|key| ((*key).to_string(), std::env::var(key).ok()))
            .collect();

        for key in keys {
            std::env::remove_var(key);
        }
        for (key, value) in values {
            std::env::set_var(key, value);
        }

        f();

        for (key, old_value) in previous {
            if let Some(value) = old_value {
                std::env::set_var(key, value);
            } else {
                std::env::remove_var(key);
            }
        }
    }

    #[test]
    fn local_backend_process_stubs_are_healthy_and_terminable() {
        let codex = LocalCodexProcess;
        let codex_handle = codex
            .spawn_app_server()
            .expect("codex process should spawn");
        assert!(codex.is_healthy(&codex_handle));
        codex
            .terminate_app_server(&codex_handle)
            .expect("codex process should stop");

        let opencode = LocalOpenCodeProcess;
        let opencode_handle = opencode.spawn_server().expect("opencode should spawn");
        assert!(opencode.is_server_healthy(&opencode_handle));
        opencode
            .terminate_server(&opencode_handle)
            .expect("opencode should stop");
    }

    #[test]
    fn stdio_discord_io_from_reader_parses_frames_and_skips_blank_lines() {
        let input = format!(
            "\n{}\n\n{}\n",
            gateway_inbound_frame_json("m1", "hello"),
            gateway_inbound_frame_json("m2", "world")
        );
        let mut io =
            StdioDiscordIo::from_reader(Cursor::new(input), 10).expect("jsonl reader should parse");

        let first = io
            .next_gateway_message()
            .expect("message 1 should parse")
            .expect("message 1 should be present");
        assert_eq!(first.message_id, "m1");

        let second = io
            .next_gateway_message()
            .expect("message 2 should parse")
            .expect("message 2 should be present");
        assert_eq!(second.message_id, "m2");

        assert!(io
            .next_gateway_message()
            .expect("message queue should drain")
            .is_none());
    }

    #[test]
    fn stdio_discord_io_from_reader_rejects_invalid_json() {
        let error = StdioDiscordIo::from_reader(Cursor::new("not-json\n"), 10)
            .expect_err("invalid json should fail parsing");
        assert!(matches!(
            error,
            CrabError::Serialization {
                context: "crabd_stdin_parse_inbound_frame",
                ..
            }
        ));
    }

    #[test]
    fn stdio_discord_io_from_reader_surfaces_line_read_errors() {
        let mut reader = FailingLineReader;
        let mut scratch = [0_u8; 1];
        assert!(Read::read(&mut reader, &mut scratch).is_err());
        io::BufRead::consume(&mut reader, 0);

        let error =
            StdioDiscordIo::from_reader(reader, 10).expect_err("reader errors should be mapped");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "crabd_stdin_read",
                path: None,
                ..
            }
        ));
    }

    #[test]
    fn next_gateway_message_surfaces_inbound_errors() {
        let mut io = StdioDiscordIo::new(10);
        let injected = CrabError::InvariantViolation {
            context: "test",
            message: "boom".to_string(),
        };
        *io.inbound_error.lock().expect("lock should succeed") = Some(injected.clone());

        let error = io
            .next_gateway_message()
            .expect_err("inbound error should surface");
        assert_eq!(error, injected);
    }

    #[test]
    fn send_outbound_op_and_wait_returns_early_receipt() {
        StdioDiscordIo::set_outbound_write_override(Some(capture_outbound_write));
        let _ = take_captured_outbound_lines();

        let op_id = "op-1";
        let channel_id = "channel-1";
        let delivery_id = "delivery-1";

        let mut io = StdioDiscordIo::new(10);
        io.ingest_inbound_frame(CrabdInboundFrame::OutboundReceipt(CrabdOutboundReceipt {
            op_id: op_id.to_string(),
            status: CrabdOutboundReceiptStatus::Ok,
            channel_id: channel_id.to_string(),
            delivery_id: delivery_id.to_string(),
            discord_message_id: Some("discord-msg-1".to_string()),
            error_message: None,
        }));

        let receipt = io
            .send_outbound_op_and_wait(CrabdOutboundOp::Post {
                op_id: op_id.to_string(),
                channel_id: channel_id.to_string(),
                delivery_id: delivery_id.to_string(),
                content: "hello".to_string(),
            })
            .expect("early receipt should be returned");
        assert_eq!(receipt.op_id, op_id);

        let captured = take_captured_outbound_lines();
        assert_eq!(captured.len(), 1);
        let op: CrabdOutboundOp =
            serde_json::from_str(&captured[0]).expect("captured op should parse");
        assert!(matches!(op, CrabdOutboundOp::Post { .. }));

        assert!(!io
            .pending_receipts
            .lock()
            .expect("lock should succeed")
            .contains_key(op_id));

        StdioDiscordIo::set_outbound_write_override(None);
    }

    #[test]
    fn send_outbound_op_and_wait_receives_receipt_via_channel() {
        StdioDiscordIo::set_outbound_write_override(Some(capture_outbound_write));
        let _ = take_captured_outbound_lines();

        let mut io = StdioDiscordIo::new(1000);
        let pending = Arc::clone(&io.pending_receipts);
        let (checked_tx, checked_rx) = std::sync::mpsc::channel();

        let op_id = "op-1".to_string();
        let channel_id = "channel-1".to_string();
        let delivery_id = "delivery-1".to_string();
        let receipt = CrabdOutboundReceipt {
            op_id: op_id.clone(),
            status: CrabdOutboundReceiptStatus::Ok,
            channel_id: channel_id.clone(),
            delivery_id: delivery_id.clone(),
            discord_message_id: Some("discord-msg-1".to_string()),
            error_message: None,
        };

        let sender_thread = std::thread::spawn(move || {
            let mut signalled = false;
            for _ in 0..10_000 {
                let maybe_sender = pending.lock().expect("lock should succeed").remove(&op_id);
                if !signalled {
                    let _ = checked_tx.send(());
                    signalled = true;
                }
                if let Some(sender) = maybe_sender {
                    let _ = sender.send(receipt);
                    return;
                }
                std::thread::yield_now();
            }
        });

        checked_rx
            .recv_timeout(std::time::Duration::from_millis(50))
            .expect("sender thread should observe pending map before we emit outbound op");
        let receipt = io
            .send_outbound_op_and_wait(CrabdOutboundOp::Post {
                op_id: "op-1".to_string(),
                channel_id: channel_id.clone(),
                delivery_id: delivery_id.clone(),
                content: "hello".to_string(),
            })
            .expect("receipt should be delivered through channel");
        sender_thread.join().expect("sender thread should join");

        assert_eq!(receipt.status, CrabdOutboundReceiptStatus::Ok);
        StdioDiscordIo::set_outbound_write_override(None);
    }

    #[test]
    fn send_outbound_op_and_wait_errors_on_timeout() {
        StdioDiscordIo::set_outbound_write_override(Some(capture_outbound_write));

        let mut io = StdioDiscordIo::new(1);
        let error = io
            .send_outbound_op_and_wait(CrabdOutboundOp::Post {
                op_id: "op-1".to_string(),
                channel_id: "channel-1".to_string(),
                delivery_id: "delivery-1".to_string(),
                content: "hello".to_string(),
            })
            .expect_err("missing receipt should time out");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "crabd_outbound_receipt_timeout",
                ..
            }
        ));
        StdioDiscordIo::set_outbound_write_override(None);
    }

    #[test]
    fn send_outbound_op_and_wait_errors_on_disconnected_channel() {
        StdioDiscordIo::set_outbound_write_override(Some(capture_outbound_write));

        let mut io = StdioDiscordIo::new(1000);
        let pending = Arc::clone(&io.pending_receipts);
        let (checked_tx, checked_rx) = std::sync::mpsc::channel();
        let op_id = "op-1".to_string();

        let dropper_thread = std::thread::spawn(move || {
            let mut signalled = false;
            for _ in 0..10_000 {
                if pending
                    .lock()
                    .expect("lock should succeed")
                    .remove(&op_id)
                    .is_some()
                {
                    return;
                }
                if !signalled {
                    let _ = checked_tx.send(());
                    signalled = true;
                }
                std::thread::yield_now();
            }
        });

        checked_rx
            .recv_timeout(std::time::Duration::from_millis(50))
            .expect("dropper thread should observe pending map before sender exists");
        let error = io
            .send_outbound_op_and_wait(CrabdOutboundOp::Post {
                op_id: "op-1".to_string(),
                channel_id: "channel-1".to_string(),
                delivery_id: "delivery-1".to_string(),
                content: "hello".to_string(),
            })
            .expect_err("dropping sender should disconnect channel");
        dropper_thread.join().expect("dropper thread should join");

        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "crabd_outbound_receipt_channel",
                ..
            }
        ));

        StdioDiscordIo::set_outbound_write_override(None);
    }

    #[test]
    fn post_and_edit_message_wait_for_receipts_and_emit_ops() {
        StdioDiscordIo::set_outbound_write_override(Some(capture_outbound_write));
        let _ = take_captured_outbound_lines();

        let mut io = StdioDiscordIo::new(10);
        io.ingest_inbound_frame(CrabdInboundFrame::OutboundReceipt(CrabdOutboundReceipt {
            op_id: "op-1".to_string(),
            status: CrabdOutboundReceiptStatus::Ok,
            channel_id: "channel-1".to_string(),
            delivery_id: "delivery-1".to_string(),
            discord_message_id: Some("discord-msg-1".to_string()),
            error_message: None,
        }));
        io.ingest_inbound_frame(CrabdInboundFrame::OutboundReceipt(CrabdOutboundReceipt {
            op_id: "op-2".to_string(),
            status: CrabdOutboundReceiptStatus::Ok,
            channel_id: "channel-1".to_string(),
            delivery_id: "delivery-2".to_string(),
            discord_message_id: Some("discord-msg-2".to_string()),
            error_message: None,
        }));

        // Use dynamic dispatch at least once to ensure coverage catches any vtable shims.
        let discord: &mut dyn DaemonDiscordIo = &mut io;
        discord
            .post_message("channel-1", "delivery-1", "hello")
            .expect("post should succeed");
        discord
            .edit_message("channel-1", "delivery-2", "world")
            .expect("edit should succeed");

        let captured = take_captured_outbound_lines();
        assert_eq!(captured.len(), 2);
        let first: CrabdOutboundOp =
            serde_json::from_str(&captured[0]).expect("first op should parse");
        let second: CrabdOutboundOp =
            serde_json::from_str(&captured[1]).expect("second op should parse");
        assert!(matches!(first, CrabdOutboundOp::Post { .. }));
        assert!(matches!(second, CrabdOutboundOp::Edit { .. }));

        StdioDiscordIo::set_outbound_write_override(None);
    }

    #[test]
    fn post_and_edit_message_map_error_receipts_to_invariant_violations() {
        StdioDiscordIo::set_outbound_write_override(Some(capture_outbound_write));

        let mut io = StdioDiscordIo::new(10);
        io.ingest_inbound_frame(CrabdInboundFrame::OutboundReceipt(CrabdOutboundReceipt {
            op_id: "op-1".to_string(),
            status: CrabdOutboundReceiptStatus::Error,
            channel_id: "channel-1".to_string(),
            delivery_id: "delivery-1".to_string(),
            discord_message_id: None,
            error_message: None,
        }));
        io.ingest_inbound_frame(CrabdInboundFrame::OutboundReceipt(CrabdOutboundReceipt {
            op_id: "op-2".to_string(),
            status: CrabdOutboundReceiptStatus::Error,
            channel_id: "channel-1".to_string(),
            delivery_id: "delivery-2".to_string(),
            discord_message_id: None,
            error_message: None,
        }));

        let post_error = io
            .post_message("channel-1", "delivery-1", "hello")
            .expect_err("post error receipt should fail");
        assert!(matches!(
            post_error,
            CrabError::InvariantViolation {
                context: "crabd_outbound_receipt_error",
                ..
            }
        ));

        let edit_error = io
            .edit_message("channel-1", "delivery-2", "world")
            .expect_err("edit error receipt should fail");
        assert!(matches!(
            edit_error,
            CrabError::InvariantViolation {
                context: "crabd_outbound_receipt_error",
                ..
            }
        ));

        StdioDiscordIo::set_outbound_write_override(None);
    }

    #[test]
    fn post_message_surfaces_outbound_write_errors() {
        StdioDiscordIo::set_outbound_write_override(Some(fail_outbound_write));
        let mut io = StdioDiscordIo::new(10);
        let error = io
            .post_message("channel-1", "delivery-1", "hello")
            .expect_err("outbound writes should surface errors");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "crabd_test_outbound_write",
                ..
            }
        ));
        StdioDiscordIo::set_outbound_write_override(None);
    }

    #[test]
    fn write_outbound_line_writer_errors_are_mapped() {
        let line = "payload";

        let mut fail_payload = ScriptedWriter {
            fail_payload_write: true,
            ..ScriptedWriter::default()
        };
        let payload_error = StdioDiscordIo::write_outbound_line_to_writer(&mut fail_payload, line)
            .expect_err("payload write failure should surface");
        assert!(matches!(
            payload_error,
            CrabError::Io {
                context: "crabd_stdout_write_outbound_op",
                ..
            }
        ));

        let mut fail_newline = ScriptedWriter {
            fail_newline_write: true,
            ..ScriptedWriter::default()
        };
        let newline_error = StdioDiscordIo::write_outbound_line_to_writer(&mut fail_newline, line)
            .expect_err("newline write failure should surface");
        assert!(matches!(
            newline_error,
            CrabError::Io {
                context: "crabd_stdout_write_outbound_op",
                ..
            }
        ));

        let mut fail_flush = ScriptedWriter {
            fail_flush: true,
            ..ScriptedWriter::default()
        };
        let flush_error = StdioDiscordIo::write_outbound_line_to_writer(&mut fail_flush, line)
            .expect_err("flush failure should surface");
        assert!(matches!(
            flush_error,
            CrabError::Io {
                context: "crabd_stdout_write_outbound_op",
                ..
            }
        ));

        let mut success_writer = ScriptedWriter::default();
        StdioDiscordIo::write_outbound_line_to_writer(&mut success_writer, line)
            .expect("writer success path should serialize payload and newline");
        assert!(success_writer.bytes.ends_with(b"\n"));
    }

    #[test]
    fn parse_u64_helpers_cover_defaults_and_validation() {
        let parsed = parse_required_u64("7", "KEY").expect("valid integer should parse");
        assert_eq!(parsed, 7);

        let invalid = parse_required_u64("bad", "KEY").expect_err("invalid int should fail");
        assert_eq!(
            invalid,
            CrabError::InvalidConfig {
                key: "KEY",
                value: "bad".to_string(),
                reason: "must be a positive integer",
            }
        );

        let zero = parse_required_u64("0", "KEY").expect_err("zero should fail");
        assert_eq!(
            zero,
            CrabError::InvalidConfig {
                key: "KEY",
                value: "0".to_string(),
                reason: "must be greater than 0",
            }
        );

        let no_override =
            parse_optional_u64(None, "KEY", 42).expect("default should be used when absent");
        assert_eq!(no_override, 42);

        let override_value = "9".to_string();
        let overridden = parse_optional_u64(Some(&override_value), "KEY", 42)
            .expect("provided value should override default");
        assert_eq!(overridden, 9);
    }

    #[test]
    fn parse_daemon_config_covers_missing_invalid_and_valid_paths() {
        let missing_bot =
            parse_daemon_config(&HashMap::new()).expect_err("missing bot user id should fail");
        assert_eq!(
            missing_bot,
            CrabError::MissingConfig {
                key: "CRAB_BOT_USER_ID"
            }
        );

        let blank_bot = parse_daemon_config(&HashMap::from([
            ("CRAB_BOT_USER_ID".to_string(), "   ".to_string()),
            ("CRAB_DAEMON_TICK_INTERVAL_MS".to_string(), "1".to_string()),
        ]))
        .expect_err("blank bot user id should fail validation");
        assert_eq!(
            blank_bot,
            CrabError::InvalidConfig {
                key: "CRAB_BOT_USER_ID",
                value: "   ".to_string(),
                reason: "must not be empty",
            }
        );

        let invalid_max = parse_daemon_config(&HashMap::from([
            ("CRAB_BOT_USER_ID".to_string(), "999".to_string()),
            ("CRAB_DAEMON_MAX_ITERATIONS".to_string(), "0".to_string()),
        ]))
        .expect_err("invalid max iterations should fail");
        assert_eq!(
            invalid_max,
            CrabError::InvalidConfig {
                key: "CRAB_DAEMON_MAX_ITERATIONS",
                value: "0".to_string(),
                reason: "must be greater than 0",
            }
        );

        let invalid_tick = parse_daemon_config(&HashMap::from([
            ("CRAB_BOT_USER_ID".to_string(), "999".to_string()),
            (
                "CRAB_DAEMON_TICK_INTERVAL_MS".to_string(),
                "bad".to_string(),
            ),
        ]))
        .expect_err("invalid tick interval should fail");
        assert_eq!(
            invalid_tick,
            CrabError::InvalidConfig {
                key: "CRAB_DAEMON_TICK_INTERVAL_MS",
                value: "bad".to_string(),
                reason: "must be a positive integer",
            }
        );

        let zero_tick = parse_daemon_config(&HashMap::from([
            ("CRAB_BOT_USER_ID".to_string(), "999".to_string()),
            ("CRAB_DAEMON_TICK_INTERVAL_MS".to_string(), "0".to_string()),
        ]))
        .expect_err("zero tick interval should fail");
        assert_eq!(
            zero_tick,
            CrabError::InvalidConfig {
                key: "CRAB_DAEMON_TICK_INTERVAL_MS",
                value: "0".to_string(),
                reason: "must be greater than 0",
            }
        );

        let valid = parse_daemon_config(&HashMap::from([
            ("CRAB_BOT_USER_ID".to_string(), "999".to_string()),
            ("CRAB_DAEMON_TICK_INTERVAL_MS".to_string(), "2".to_string()),
        ]))
        .expect("valid daemon config should parse");
        assert_eq!(valid.bot_user_id, "999");
        assert_eq!(valid.tick_interval_ms, 2);
        assert_eq!(valid.max_iterations, None);
    }

    #[test]
    fn run_with_reader_and_control_processes_one_message() {
        let workspace_root = temp_workspace_root("run-success");
        let values = runtime_values(&workspace_root);
        let input = format!(
            "{}\n{}\n",
            gateway_inbound_frame_json("m-1", "hello daemon"),
            ok_receipt_inbound_frame_json(
                "op-1",
                "777",
                "delivery:run:discord:channel:777:m-1:chunk:0"
            )
        );
        let mut control = ScriptedControl::with_now(vec![1_000, 1_001]);
        let mut reader = Cursor::new(input);

        let stats = run_with_reader_and_control(&values, &mut reader, &mut control)
            .expect("daemon run should succeed");
        assert_eq!(
            stats,
            DaemonLoopStats {
                iterations: 1,
                ingested_messages: 1,
                dispatched_runs: 1,
                heartbeat_cycles: 0,
            }
        );
        assert_eq!(control.slept, vec![1]);
    }

    #[test]
    fn run_with_reader_and_control_propagates_runtime_config_errors() {
        let values = HashMap::from([("CRAB_BOT_USER_ID".to_string(), "999".to_string())]);
        let mut control = ScriptedControl::with_now(vec![1_000]);
        let mut reader = Cursor::new("");
        let error = run_with_reader_and_control(&values, &mut reader, &mut control)
            .expect_err("missing runtime config should fail");
        assert_eq!(
            error,
            CrabError::MissingConfig {
                key: "CRAB_DISCORD_TOKEN"
            }
        );
    }

    #[test]
    fn run_with_reader_and_control_propagates_daemon_config_errors() {
        let values = HashMap::from([
            ("CRAB_DISCORD_TOKEN".to_string(), "test-token".to_string()),
            (
                "CRAB_WORKSPACE_ROOT".to_string(),
                temp_workspace_root("run-config-error"),
            ),
            ("CRAB_BOT_USER_ID".to_string(), "999".to_string()),
            ("CRAB_DAEMON_TICK_INTERVAL_MS".to_string(), "0".to_string()),
        ]);
        let mut control = ScriptedControl::with_now(vec![1_000]);
        let mut reader = Cursor::new("");
        let error = run_with_reader_and_control(&values, &mut reader, &mut control)
            .expect_err("invalid daemon config should fail");
        assert_eq!(
            error,
            CrabError::InvalidConfig {
                key: "CRAB_DAEMON_TICK_INTERVAL_MS",
                value: "0".to_string(),
                reason: "must be greater than 0",
            }
        );
    }

    #[test]
    fn run_with_reader_and_control_propagates_reader_errors() {
        let workspace_root = temp_workspace_root("run-reader-error");
        let values = runtime_values(&workspace_root);
        let mut control = ScriptedControl::with_now(vec![1_000]);
        let mut reader = FailingLineReader;
        let error = run_with_reader_and_control(&values, &mut reader, &mut control)
            .expect_err("reader errors should surface");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "crabd_stdin_read",
                ..
            }
        ));
    }

    #[test]
    fn run_with_reader_and_control_propagates_codex_start_errors() {
        let workspace_root = temp_workspace_root("run-codex-start-error");
        let values = runtime_values(&workspace_root);
        let mut control = ScriptedControl::with_now(vec![1_000]);
        let mut reader = Cursor::new("");
        LocalCodexProcess::set_spawn_override(Some(fail_local_codex_spawn));
        let error = run_with_reader_and_control(&values, &mut reader, &mut control)
            .expect_err("codex startup errors should surface");
        LocalCodexProcess::set_spawn_override(None);
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "crabd_local_codex_spawn",
                message: "forced codex spawn failure".to_string(),
            }
        );
    }

    #[test]
    fn run_with_reader_and_control_propagates_opencode_start_errors() {
        let workspace_root = temp_workspace_root("run-opencode-start-error");
        let values = runtime_values(&workspace_root);
        let mut control = ScriptedControl::with_now(vec![1_000]);
        let mut reader = Cursor::new("");
        LocalOpenCodeProcess::set_spawn_override(Some(fail_local_opencode_spawn));
        let error = run_with_reader_and_control(&values, &mut reader, &mut control)
            .expect_err("opencode startup errors should surface");
        LocalOpenCodeProcess::set_spawn_override(None);
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "crabd_local_opencode_spawn",
                message: "forced opencode spawn failure".to_string(),
            }
        );
    }

    #[test]
    fn run_with_env_installer_errors_are_propagated() {
        let workspace_root = temp_workspace_root("run-install-error");
        let values = runtime_values(&workspace_root);
        with_test_env(&values, || {
            let mut reader = Cursor::new("");
            let error =
                run_with_env_and_reader_and_control_installer(&mut reader, fail_control_install)
                    .expect_err("control install errors should surface");
            assert_eq!(
                error,
                CrabError::InvariantViolation {
                    context: "crabd_control_install",
                    message: "forced install failure".to_string(),
                }
            );
        });
    }

    #[test]
    fn run_main_with_runner_maps_success_and_error_to_exit_code() {
        let success = run_main_with_runner(run_main_success);
        assert_eq!(success, std::process::ExitCode::SUCCESS);

        let failure = run_main_with_runner(run_main_failure);
        assert_eq!(failure, std::process::ExitCode::FAILURE);
    }

    #[test]
    fn main_runs_end_to_end_with_env_config() {
        let workspace_root = temp_workspace_root("main");
        let values = runtime_values(&workspace_root);
        with_test_env(&values, || {
            assert_eq!(main(), std::process::ExitCode::SUCCESS);
        });
    }

    #[test]
    fn env_helper_restores_previous_values() {
        std::env::set_var("CRAB_BOT_USER_ID", "old-value");
        let workspace_root = temp_workspace_root("env");
        let values = runtime_values(&workspace_root);
        with_test_env(&values, || {
            assert_eq!(
                std::env::var("CRAB_BOT_USER_ID").expect("env var should be set"),
                "999"
            );
        });
        assert_eq!(
            std::env::var("CRAB_BOT_USER_ID").expect("previous env var should be restored"),
            "old-value"
        );
        std::env::remove_var("CRAB_BOT_USER_ID");
    }
}
