use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

use crab_discord::{
    CrabdInboundFrame, CrabdOutboundReceipt, CrabdOutboundReceiptStatus, GatewayConversationKind,
    GatewayMessage,
};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempWorkspace {
    path: PathBuf,
}

impl TempWorkspace {
    fn new(label: &str) -> Self {
        let suffix = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "crab-app-bin-entry-{label}-{}-{suffix}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("workspace directory should be creatable");
        Self { path }
    }
}

impl Drop for TempWorkspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn seed_ready_workspace(root: &Path) {
    for file_name in ["AGENTS.md", "SOUL.md", "IDENTITY.md", "USER.md", "MEMORY.md"] {
        fs::write(root.join(file_name), "seed\n").expect("seed file should be writable");
    }
    let _ = fs::remove_file(root.join("BOOTSTRAP.md"));
}

fn child_env(command: &mut Command, workspace_root: &Path) {
    command
        .env("CRAB_DISCORD_TOKEN", "test-token")
        .env("CRAB_WORKSPACE_ROOT", workspace_root)
        .env("CRAB_BOT_USER_ID", "999")
        .env("CRAB_DAEMON_TICK_INTERVAL_MS", "1")
        .env("CRAB_DAEMON_MAX_ITERATIONS", "3")
        .env("CRAB_OUTBOUND_RECEIPT_TIMEOUT_MS", "100")
        .env("CRAB_DAEMON_FORCE_DETERMINISTIC_CLAUDE_PROCESS", "1");
}

fn bot_gateway_frame_json(message_id: &str, content: &str) -> String {
    serde_json::to_string(&CrabdInboundFrame::GatewayMessage(GatewayMessage {
        message_id: message_id.to_string(),
        author_id: "999".to_string(),
        author_is_bot: true,
        channel_id: "777".to_string(),
        guild_id: Some("555".to_string()),
        thread_id: None,
        content: content.to_string(),
        conversation_kind: GatewayConversationKind::GuildChannel,
        attachments: vec![],
    }))
    .expect("gateway frame should serialize")
}

fn user_gateway_frame_json(message_id: &str, content: &str) -> String {
    serde_json::to_string(&CrabdInboundFrame::GatewayMessage(GatewayMessage {
        message_id: message_id.to_string(),
        author_id: "111".to_string(),
        author_is_bot: false,
        channel_id: "777".to_string(),
        guild_id: Some("555".to_string()),
        thread_id: None,
        content: content.to_string(),
        conversation_kind: GatewayConversationKind::GuildChannel,
        attachments: vec![],
    }))
    .expect("gateway frame should serialize")
}

fn receipt_frame_json(op_id: &str, delivery_id: &str) -> String {
    serde_json::to_string(&CrabdInboundFrame::OutboundReceipt(CrabdOutboundReceipt {
        op_id: op_id.to_string(),
        status: CrabdOutboundReceiptStatus::Ok,
        channel_id: "777".to_string(),
        delivery_id: delivery_id.to_string(),
        discord_message_id: Some("discord-msg-1".to_string()),
        error_message: None,
    }))
    .expect("receipt frame should serialize")
}

fn assert_help(binary_path: &str) {
    let output = Command::new(binary_path)
        .arg("--help")
        .output()
        .expect("binary should run");
    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn thin_bin_wrappers_execute_help_paths() {
    for binary in [
        env!("CARGO_BIN_EXE_crab-memory-get"),
        env!("CARGO_BIN_EXE_crab-memory-search"),
        env!("CARGO_BIN_EXE_crab-rotate"),
        env!("CARGO_BIN_EXE_crab-self-work"),
        env!("CARGO_BIN_EXE_crab-trigger"),
    ] {
        assert_help(binary);
    }
}

#[test]
fn crabd_binary_covers_stdin_success_paths() {
    let workspace = TempWorkspace::new("crabd-success");
    seed_ready_workspace(&workspace.path);

    let mut command = Command::new(env!("CARGO_BIN_EXE_crabd"));
    child_env(&mut command, &workspace.path);
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("crabd should spawn");

    let mut stdin = child.stdin.take().expect("stdin should be piped");
    writeln!(stdin).expect("blank line write should succeed");
    writeln!(stdin, "{}", bot_gateway_frame_json("m-bot", "ignore me"))
        .expect("bot frame write should succeed");
    writeln!(stdin, "{}", user_gateway_frame_json("m-1", "hello daemon"))
        .expect("user frame write should succeed");
    stdin.flush().expect("stdin flush should succeed");

    let mut stdout = BufReader::new(child.stdout.take().expect("stdout should be piped"));
    let mut outbound_line = String::new();
    stdout
        .read_line(&mut outbound_line)
        .expect("stdout read should succeed");
    assert!(!outbound_line.trim().is_empty(), "expected outbound op line");

    let outbound: serde_json::Value =
        serde_json::from_str(outbound_line.trim()).expect("stdout line should be valid json");
    let op_id = outbound["op_id"]
        .as_str()
        .expect("outbound op should contain op_id");
    let delivery_id = outbound["delivery_id"]
        .as_str()
        .expect("outbound op should contain delivery_id");

    writeln!(stdin, "{}", receipt_frame_json(op_id, delivery_id))
        .expect("receipt write should succeed");
    writeln!(stdin, "{}", receipt_frame_json("op-early", "delivery:early"))
        .expect("early receipt write should succeed");
    drop(stdin);

    let output = child.wait_with_output().expect("child should exit");
    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        outbound_line,
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn crabd_binary_reports_invalid_inbound_json() {
    let workspace = TempWorkspace::new("crabd-invalid-json");
    seed_ready_workspace(&workspace.path);

    let mut command = Command::new(env!("CARGO_BIN_EXE_crabd"));
    child_env(&mut command, &workspace.path);
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("crabd should spawn");

    let mut stdin = child.stdin.take().expect("stdin should be piped");
    writeln!(stdin, "{{not-json").expect("invalid json write should succeed");
    drop(stdin);

    let output = child.wait_with_output().expect("child should exit");
    assert!(!output.status.success(), "process should fail on invalid json");
}

#[test]
fn crabd_binary_reports_stdin_read_errors() {
    let workspace = TempWorkspace::new("crabd-read-error");
    seed_ready_workspace(&workspace.path);
    let mut command = Command::new(env!("CARGO_BIN_EXE_crabd"));
    child_env(&mut command, &workspace.path);
    let output = command
        .env("CRAB_DAEMON_MAX_ITERATIONS", "20")
        .stdin(Stdio::from(
            OpenOptions::new()
                .read(true)
                .open(&workspace.path)
                .expect("directory-backed stdin should open"),
        ))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("crabd should run");

    assert!(!output.status.success(), "process should fail on stdin read errors");
}
