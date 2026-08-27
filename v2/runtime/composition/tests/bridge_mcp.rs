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
        let mut command = Command::new(env!("CARGO_BIN_EXE_crab-v2-bridge-mcp"));
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

fn register_arguments(state: &Path) -> Value {
    json!({
        "bridgeId": "signal",
        "packageId": "agent.signal",
        "displayName": "Signal",
        "launch": {
            "executable": "/fixture/signal",
            "arguments": [],
            "workingDirectory": state,
            "environmentNames": []
        },
        "configuration": { "targetChannelId": "primary" },
        "authenticationMethods": ["qr-code"],
        "ingressMode": "queue",
        "alertTarget": {"channelId":"primary","lane":"primary"},
        "desiredRunning": false,
        "healthIntervalMs": 5000,
        "credentialValidationIntervalMs": 60000,
        "restartLimit": 3,
        "restartWindowMs": 300000
    })
}

#[tokio::test]
async fn real_stdio_server_lists_fourteen_strict_agent_bridge_tools() {
    let (_temporary, state) = state_directory();
    let mut process = McpProcess::start(&state, false).await;
    let listed = process.request(2, "tools/list", json!({})).await;
    let tools = listed["result"]["tools"].as_array().expect("tool catalog");
    let import = tools
        .iter()
        .find(|tool| tool["name"] == "import_bridge_content")
        .expect("content import tool");
    assert_eq!(
        import["inputSchema"]["required"],
        json!(["bridgeId", "importId", "sourcePath", "mediaType"])
    );
    assert_eq!(import["inputSchema"]["additionalProperties"], false);
    let mut names = tools
        .iter()
        .map(|tool| tool["name"].as_str().expect("tool name"))
        .collect::<Vec<_>>();
    names.sort_unstable();
    assert_eq!(
        names,
        [
            "begin_bridge_authentication",
            "bridge_delivery_status",
            "bridge_status",
            "deliver_bridge_message",
            "import_bridge_content",
            "invalidate_bridge_credentials",
            "list_bridges",
            "reconcile_bridge",
            "register_bridge",
            "replace_bridge",
            "stop_bridge",
            "submit_bridge_authentication",
            "suspend_bridge",
            "validate_bridge_credentials",
        ]
    );
    let mut invalid = register_arguments(&state);
    invalid["unexpected"] = json!(true);
    let strict = process
        .request(
            3,
            "tools/call",
            json!({ "name": "register_bridge", "arguments": invalid }),
        )
        .await;
    assert!(strict.get("error").is_some());
    assert!(strict.to_string().contains("unexpected"));
    process.finish().await;
}

#[tokio::test]
async fn child_session_bridge_calls_use_authenticated_boxology_ipc() {
    let (_temporary, state) = state_directory();
    let socket = state.join("channel-ipc.sock");
    let listener = UnixListener::bind(&socket).expect("fixture IPC listener");
    fs::set_permissions(&socket, fs::Permissions::from_mode(0o600)).expect("owner-only IPC socket");
    let fixture = tokio::spawn(async move {
        for expected in [
            "bridge-host.register_bridge",
            "bridge-host.import_content",
            "bridge-host.deliver_message",
        ] {
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
            assert_eq!(request["capability"], expected);
            if expected == "bridge-host.register_bridge" {
                assert_eq!(request["input"]["management"]["tag"], "AgentManaged");
            }
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
        }
    });
    let mut process = McpProcess::start(&state, true).await;
    let registered = process
        .request(
            2,
            "tools/call",
            json!({
                "name": "register_bridge",
                "arguments": register_arguments(&state)
            }),
        )
        .await;
    assert!(registered.to_string().contains("FixtureRejected"));
    let imported = process
        .request(
            3,
            "tools/call",
            json!({
                "name": "import_bridge_content",
                "arguments": {
                    "bridgeId": "signal",
                    "importId": "selected-file-1",
                    "sourcePath": "/tmp/selected-file.txt",
                    "mediaType": "text/plain",
                    "name": "selected-file.txt"
                }
            }),
        )
        .await;
    assert!(imported.to_string().contains("FixtureRejected"));
    let delivered = process
        .request(
            4,
            "tools/call",
            json!({
                "name": "deliver_bridge_message",
                "arguments": {
                    "bridgeId": "signal",
                    "messageId": "selected-1",
                    "destination": { "chatId": "fixture" },
                    "message": { "type": "text", "text": "selected output" }
                }
            }),
        )
        .await;
    assert!(delivered.to_string().contains("FixtureRejected"));
    fixture.await.expect("IPC fixture completes");
    process.finish().await;
}
