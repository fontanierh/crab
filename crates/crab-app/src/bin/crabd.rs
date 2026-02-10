use std::collections::HashMap;
use std::collections::VecDeque;
use std::io::{self, BufRead, Write};
use std::process::ExitCode;
use std::str::FromStr;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
#[cfg(test)]
use std::{cell::RefCell, thread_local};

use crab_app::SystemDaemonLoopControl;
use crab_app::{
    run_daemon_loop_with_transport, DaemonConfig, DaemonLoopControl, DaemonLoopStats,
    DEFAULT_DAEMON_TICK_INTERVAL_MS,
};
use crab_backends::{CodexAppServerProcess, CodexProcessHandle, OpenCodeServerHandle};
use crab_core::{CrabError, CrabResult, RuntimeConfig};
use crab_discord::{
    DiscordPostedMessage, DiscordRuntimeEvent, DiscordRuntimeTransport, DiscordTransportError,
    GatewayMessage,
};

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

#[derive(Debug, Clone)]
struct StdioJsonlTransport {
    inbound_events: VecDeque<DiscordRuntimeEvent>,
    next_sent_message_sequence: u64,
    transport_instance_id: String,
}

#[cfg(test)]
type OutboundWriteOverride =
    fn(&serde_json::Value) -> Result<(), crab_discord::DiscordTransportError>;

#[cfg(test)]
thread_local! {
    static OUTBOUND_WRITE_OVERRIDE: RefCell<Option<OutboundWriteOverride>> = RefCell::new(None);
}

impl StdioJsonlTransport {
    #[cfg(test)]
    fn from_reader<R: BufRead>(reader: R) -> CrabResult<Self> {
        let mut reader = reader;
        Self::from_bufread(&mut reader)
    }

    fn from_bufread(reader: &mut dyn BufRead) -> CrabResult<Self> {
        let mut inbound_events = VecDeque::new();
        for line in reader.lines() {
            let line = line.map_err(|error| CrabError::Io {
                context: "crabd_stdin_read",
                path: None,
                message: error.to_string(),
            })?;
            if line.trim().is_empty() {
                continue;
            }

            let message = serde_json::from_str::<GatewayMessage>(&line).map_err(|error| {
                CrabError::Serialization {
                    context: "crabd_stdin_parse_gateway_message",
                    path: None,
                    message: error.to_string(),
                }
            })?;
            inbound_events.push_back(DiscordRuntimeEvent::MessageCreate(message));
        }

        Ok(Self {
            inbound_events,
            next_sent_message_sequence: 0,
            transport_instance_id: Self::new_transport_instance_id(),
        })
    }

    fn new_transport_instance_id() -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        format!("{}-{nanos}", std::process::id())
    }

    fn write_outbound_json(payload: &serde_json::Value) -> Result<(), DiscordTransportError> {
        #[cfg(test)]
        if let Some(write_override) = OUTBOUND_WRITE_OVERRIDE.with(|cell| *cell.borrow()) {
            return write_override(payload);
        }

        let mut stdout = io::stdout();
        Self::write_outbound_json_to_writer(&mut stdout, payload)
    }

    #[cfg(test)]
    fn set_outbound_write_override(write_override: Option<OutboundWriteOverride>) {
        OUTBOUND_WRITE_OVERRIDE.with(|cell| {
            *cell.borrow_mut() = write_override;
        });
    }

    fn write_outbound_json_to_writer(
        writer: &mut dyn Write,
        payload: &serde_json::Value,
    ) -> Result<(), DiscordTransportError> {
        let line = payload.to_string();
        writer
            .write_all(line.as_bytes())
            .map_err(|error| DiscordTransportError::Fatal {
                message: format!("failed to write outbound payload: {error}"),
            })?;
        writer
            .write_all(b"\n")
            .map_err(|error| DiscordTransportError::Fatal {
                message: format!("failed to write outbound newline: {error}"),
            })?;
        writer
            .flush()
            .map_err(|error| DiscordTransportError::Fatal {
                message: format!("failed to flush outbound payload: {error}"),
            })?;
        Ok(())
    }
}

impl DiscordRuntimeTransport for StdioJsonlTransport {
    fn next_event(&mut self) -> CrabResult<Option<DiscordRuntimeEvent>> {
        Ok(self.inbound_events.pop_front())
    }

    fn send_message(
        &mut self,
        channel_id: &str,
        content: &str,
    ) -> Result<DiscordPostedMessage, DiscordTransportError> {
        self.next_sent_message_sequence = self.next_sent_message_sequence.saturating_add(1);
        let posted_message_id = format!(
            "discord-msg-{}-{}",
            self.transport_instance_id, self.next_sent_message_sequence
        );
        let payload = serde_json::json!({
            "op": "post",
            "channel_id": channel_id,
            "message_id": posted_message_id,
            "content": content,
        });
        Self::write_outbound_json(&payload)?;
        Ok(DiscordPostedMessage {
            message_id: posted_message_id,
        })
    }

    fn edit_message(
        &mut self,
        channel_id: &str,
        message_id: &str,
        content: &str,
    ) -> Result<(), DiscordTransportError> {
        let payload = serde_json::json!({
            "op": "edit",
            "channel_id": channel_id,
            "message_id": message_id,
            "content": content,
        });
        Self::write_outbound_json(&payload)
    }

    fn wait_for_retry(&mut self, backoff_ms: u64) -> CrabResult<()> {
        thread::sleep(Duration::from_millis(backoff_ms));
        Ok(())
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

fn run_with_reader_and_control(
    values: &HashMap<String, String>,
    reader: &mut dyn BufRead,
    control: &mut dyn DaemonLoopControl,
) -> CrabResult<DaemonLoopStats> {
    let runtime_config = RuntimeConfig::from_map(values)?;
    let daemon_config = parse_daemon_config(values)?;
    let transport = StdioJsonlTransport::from_bufread(reader)?;
    run_daemon_loop_with_transport(
        &runtime_config,
        &daemon_config,
        LocalCodexProcess,
        LocalOpenCodeProcess,
        transport,
        control,
    )
}

fn run_main_with_runner(runner: fn() -> CrabResult<DaemonLoopStats>) -> ExitCode {
    match runner() {
        Ok(stats) => {
            eprintln!(
                "crabd finished: iterations={}, ingested={}, dispatched={}, heartbeats={}",
                stats.iterations,
                stats.ingested_messages,
                stats.dispatched_runs,
                stats.heartbeat_cycles
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("crabd failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run_with_env_and_stdio() -> CrabResult<DaemonLoopStats> {
    let stdin = io::stdin();
    let mut locked_stdin = stdin.lock();
    run_with_env_and_reader_and_control_installer(
        &mut locked_stdin,
        SystemDaemonLoopControl::install,
    )
}

fn run_with_env_and_reader_and_control_installer(
    reader: &mut dyn BufRead,
    control_installer: fn() -> CrabResult<SystemDaemonLoopControl>,
) -> CrabResult<DaemonLoopStats> {
    let values = std::env::vars().collect::<HashMap<_, _>>();
    let mut control = control_installer()?;
    run_with_reader_and_control(&values, reader, &mut control)
}

fn main() -> ExitCode {
    run_main_with_runner(run_with_env_and_stdio)
}

#[cfg(test)]
mod tests {
    use super::{
        main, parse_daemon_config, parse_optional_u64, parse_required_u64, run_main_with_runner,
        run_with_env_and_reader_and_control_installer, run_with_reader_and_control,
        LocalCodexProcess, LocalOpenCodeProcess, StdioJsonlTransport,
    };
    use crab_app::{DaemonLoopControl, DaemonLoopStats};
    use crab_backends::{
        CodexAppServerProcess, CodexProcessHandle, OpenCodeServerHandle, OpenCodeServerProcess,
    };
    use crab_core::{CrabError, CrabResult};
    use crab_discord::{
        DiscordRuntimeTransport, DiscordTransportError, GatewayConversationKind, GatewayMessage,
    };
    use std::collections::HashMap;
    use std::collections::VecDeque;
    use std::io::{self, Cursor, Read, Write};
    use std::sync::{Mutex, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

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

    fn fail_outbound_write(_payload: &serde_json::Value) -> Result<(), DiscordTransportError> {
        Err(DiscordTransportError::Fatal {
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

    fn gateway_message_json(message_id: &str, content: &str) -> String {
        serde_json::to_string(&GatewayMessage {
            message_id: message_id.to_string(),
            author_id: "111".to_string(),
            author_is_bot: false,
            channel_id: "777".to_string(),
            guild_id: Some("555".to_string()),
            thread_id: None,
            content: content.to_string(),
            conversation_kind: GatewayConversationKind::GuildChannel,
        })
        .expect("message json should serialize")
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
    fn transport_from_reader_parses_events_and_skips_blank_lines() {
        let input = format!(
            "\n{}\n\n{}\n",
            gateway_message_json("m1", "hello"),
            gateway_message_json("m2", "world")
        );
        let mut transport = StdioJsonlTransport::from_reader(Cursor::new(input))
            .expect("jsonl reader should parse");

        assert!(transport
            .next_event()
            .expect("event 1 should parse")
            .is_some());
        assert!(transport
            .next_event()
            .expect("event 2 should parse")
            .is_some());
        assert!(transport
            .next_event()
            .expect("event queue should drain")
            .is_none());
    }

    #[test]
    fn transport_from_reader_rejects_invalid_json() {
        let error = StdioJsonlTransport::from_reader(Cursor::new("not-json\n"))
            .expect_err("invalid json should fail parsing");
        assert!(matches!(
            error,
            CrabError::Serialization {
                context: "crabd_stdin_parse_gateway_message",
                ..
            }
        ));
    }

    #[test]
    fn transport_from_reader_surfaces_line_read_errors() {
        let mut reader = FailingLineReader;
        let mut scratch = [0_u8; 1];
        assert!(Read::read(&mut reader, &mut scratch).is_err());
        io::BufRead::consume(&mut reader, 0);

        let error =
            StdioJsonlTransport::from_reader(reader).expect_err("reader errors should be mapped");
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
    fn transport_send_edit_and_wait_paths_execute() {
        let mut transport = StdioJsonlTransport::from_reader(Cursor::new(""))
            .expect("empty reader should initialize transport");
        let first = transport
            .send_message("777", "hello")
            .expect("send should succeed");
        assert!(first.message_id.starts_with("discord-msg-"));
        assert!(first.message_id.ends_with("-1"));
        let second = transport
            .send_message("777", "world")
            .expect("second send should increment sequence");
        assert!(second.message_id.starts_with("discord-msg-"));
        assert!(second.message_id.ends_with("-2"));
        let first_prefix = first
            .message_id
            .rsplit_once('-')
            .expect("first id should contain sequence delimiter")
            .0;
        let second_prefix = second
            .message_id
            .rsplit_once('-')
            .expect("second id should contain sequence delimiter")
            .0;
        assert_eq!(first_prefix, second_prefix);
        transport
            .edit_message("777", &second.message_id, "edit")
            .expect("edit should succeed");
        transport
            .wait_for_retry(0)
            .expect("zero backoff should sleep and return");
    }

    #[test]
    fn transport_send_message_surfaces_outbound_write_errors() {
        StdioJsonlTransport::set_outbound_write_override(Some(fail_outbound_write));
        let mut transport = StdioJsonlTransport::from_reader(Cursor::new(""))
            .expect("empty reader should initialize transport");
        let error = transport
            .send_message("777", "hello")
            .expect_err("send should fail when outbound write is overridden");
        assert_eq!(
            error,
            DiscordTransportError::Fatal {
                message: "forced outbound write failure".to_string(),
            }
        );
        StdioJsonlTransport::set_outbound_write_override(None);
    }

    #[test]
    fn write_outbound_json_writer_errors_are_mapped() {
        let payload = serde_json::json!({"op": "post"});

        let mut fail_payload = ScriptedWriter {
            fail_payload_write: true,
            ..ScriptedWriter::default()
        };
        let payload_error =
            StdioJsonlTransport::write_outbound_json_to_writer(&mut fail_payload, &payload)
                .expect_err("payload write failure should surface");
        assert!(matches!(
            payload_error,
            crab_discord::DiscordTransportError::Fatal { ref message }
                if message.contains("failed to write outbound payload")
        ));

        let mut fail_newline = ScriptedWriter {
            fail_newline_write: true,
            ..ScriptedWriter::default()
        };
        let newline_error =
            StdioJsonlTransport::write_outbound_json_to_writer(&mut fail_newline, &payload)
                .expect_err("newline write failure should surface");
        assert!(matches!(
            newline_error,
            crab_discord::DiscordTransportError::Fatal { ref message }
                if message.contains("failed to write outbound newline")
        ));

        let mut fail_flush = ScriptedWriter {
            fail_flush: true,
            ..ScriptedWriter::default()
        };
        let flush_error =
            StdioJsonlTransport::write_outbound_json_to_writer(&mut fail_flush, &payload)
                .expect_err("flush failure should surface");
        assert!(matches!(
            flush_error,
            crab_discord::DiscordTransportError::Fatal { ref message }
                if message.contains("failed to flush outbound payload")
        ));

        let mut success_writer = ScriptedWriter::default();
        StdioJsonlTransport::write_outbound_json_to_writer(&mut success_writer, &payload)
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
        let input = format!("{}\n", gateway_message_json("m-1", "hello daemon"));
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
