use std::{
    fs::{self, OpenOptions},
    os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _},
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use agent_host_implementation::{
    CRAB_AGENT_ID_ENV, CRAB_PARENT_SESSION_ID_ENV, CRAB_SESSION_ID_ENV, CRAB_STATE_DIRECTORY_ENV,
    CRAB_SUB_AGENT_ID_ENV, CRAB_WORKING_DIRECTORY_ENV,
};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader, Lines},
    net::UnixListener,
    process::{Child, ChildStdin, ChildStdout, Command},
};

const TOKEN: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

struct McpProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
}

impl McpProcess {
    async fn start(state: &Path, child_context: bool) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_crab-v2-sub-agent-mcp"));
        command
            .env(CRAB_STATE_DIRECTORY_ENV, state)
            .env(CRAB_SESSION_ID_ENV, "session_fixture")
            .env(CRAB_AGENT_ID_ENV, "fixture-agent")
            .env(CRAB_WORKING_DIRECTORY_ENV, state)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        if child_context {
            command
                .env(CRAB_SUB_AGENT_ID_ENV, "sub_fixture")
                .env(CRAB_PARENT_SESSION_ID_ENV, "session_parent");
        } else {
            command
                .env_remove(CRAB_SUB_AGENT_ID_ENV)
                .env_remove(CRAB_PARENT_SESSION_ID_ENV);
        }
        let mut child = command.spawn().expect("MCP subprocess starts");
        let stdin = child.stdin.take().expect("MCP stdin");
        let stdout = BufReader::new(child.stdout.take().expect("MCP stdout")).lines();
        let mut process = Self {
            child,
            stdin,
            stdout,
        };
        let initialized = process
            .request(
                1,
                "initialize",
                json!({
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": { "name": "crab-test", "version": "1" }
                }),
            )
            .await;
        assert!(initialized["result"]["protocolVersion"].is_string());
        process
            .write(json!({
                "jsonrpc": "2.0",
                "method": "notifications/initialized"
            }))
            .await;
        process
    }

    async fn request(&mut self, id: u64, method: &str, params: Value) -> Value {
        self.write(json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params
        }))
        .await;
        loop {
            let line = tokio::time::timeout(Duration::from_secs(5), self.stdout.next_line())
                .await
                .expect("MCP response timeout")
                .expect("MCP stdout remains readable")
                .expect("MCP response line");
            let response: Value = serde_json::from_str(&line).expect("MCP response JSON");
            if response["id"] == id {
                return response;
            }
        }
    }

    async fn write(&mut self, message: Value) {
        self.stdin
            .write_all(format!("{message}\n").as_bytes())
            .await
            .expect("write MCP request");
        self.stdin.flush().await.expect("flush MCP request");
    }

    async fn finish(mut self) {
        drop(self.stdin);
        if tokio::time::timeout(Duration::from_secs(5), self.child.wait())
            .await
            .is_err()
        {
            self.child.kill().await.expect("kill stuck MCP fixture");
        }
    }
}

fn state_directory() -> (tempfile::TempDir, PathBuf) {
    let directory = tempfile::tempdir().expect("temporary state directory");
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
        .expect("owner-only state directory");
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(directory.path().join("channel-ipc.token"))
        .and_then(|mut file| std::io::Write::write_all(&mut file, TOKEN.as_bytes()))
        .expect("owner-only IPC token");
    let canonical = fs::canonicalize(directory.path()).expect("canonical state directory");
    (directory, canonical)
}

#[tokio::test]
async fn real_stdio_server_lists_strict_tools_and_enforces_child_context() {
    let (_temporary, state) = state_directory();
    let mut process = McpProcess::start(&state, false).await;

    let listed = process.request(2, "tools/list", json!({})).await;
    let mut names = listed["result"]["tools"]
        .as_array()
        .expect("tool catalog")
        .iter()
        .map(|tool| tool["name"].as_str().expect("tool name"))
        .collect::<Vec<_>>();
    names.sort_unstable();
    assert_eq!(
        names,
        [
            "read_sub_agent_events",
            "send_to_parent",
            "send_to_sub_agent",
            "spawn_sub_agent",
            "stop_sub_agent",
            "sub_agent_status",
        ]
    );

    let strict = process
        .request(
            3,
            "tools/call",
            json!({
                "name": "sub_agent_status",
                "arguments": { "subAgentId": "sub_fixture", "unexpected": true }
            }),
        )
        .await;
    assert_eq!(strict["error"]["code"], -32603);
    assert!(strict.to_string().contains("unexpected"));

    let parent = process
        .request(
            4,
            "tools/call",
            json!({
                "name": "send_to_parent",
                "arguments": {
                    "messageId": "progress-1",
                    "mode": "queue",
                    "message": { "phase": "working" }
                }
            }),
        )
        .await;
    assert!(parent.to_string().contains("ChildSessionRequired"));
    process.finish().await;
}

#[tokio::test]
async fn child_tool_call_uses_the_owner_only_authenticated_ipc() {
    let (_temporary, state) = state_directory();
    let socket = state.join("channel-ipc.sock");
    let listener = UnixListener::bind(&socket).expect("fixture IPC listener");
    fs::set_permissions(&socket, fs::Permissions::from_mode(0o600)).expect("owner-only IPC socket");
    let fixture = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("MCP IPC connection");
        let (reader, mut writer) = stream.into_split();
        let mut lines = BufReader::new(reader).lines();
        let request: Value = serde_json::from_str(
            &lines
                .next_line()
                .await
                .expect("read IPC")
                .expect("IPC request"),
        )
        .expect("IPC request JSON");
        assert_eq!(request["protocolVersion"], 1);
        assert_eq!(request["authentication"], TOKEN);
        assert_eq!(request["capability"], "sub-agent-host.send_to_parent");
        assert_eq!(request["input"]["sub_agent_id"], "sub_fixture");
        let response = json!({
            "protocolVersion": 1,
            "requestId": request["requestId"],
            "status": "error",
            "error": { "kind": "domain", "code": "FixtureRejected" }
        });
        writer
            .write_all(format!("{response}\n").as_bytes())
            .await
            .expect("write IPC response");
    });
    let mut process = McpProcess::start(&state, true).await;
    let response = process
        .request(
            2,
            "tools/call",
            json!({
                "name": "send_to_parent",
                "arguments": {
                    "messageId": "progress-1",
                    "mode": "steer",
                    "message": { "phase": "working" }
                }
            }),
        )
        .await;
    assert!(response.to_string().contains("FixtureRejected"));
    fixture.await.expect("IPC fixture completes");
    process.finish().await;
}
