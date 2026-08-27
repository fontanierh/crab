use std::{
    collections::BTreeMap,
    io::{self, BufRead, Write},
};

use serde_json::{Value, json};

#[derive(Clone, Copy, Eq, PartialEq)]
enum Protocol {
    V1,
    V2,
}

struct FixtureAgent {
    protocol: Protocol,
    session_id: String,
    active_v1_request: Option<Value>,
    active_v2: bool,
    permission_pending: bool,
    session_options: BTreeMap<String, String>,
}

impl FixtureAgent {
    fn new(protocol: Protocol) -> Self {
        Self {
            protocol,
            session_id: "fixture-native-session".into(),
            active_v1_request: None,
            active_v2: false,
            permission_pending: false,
            session_options: BTreeMap::from([
                ("mode".into(), "default".into()),
                ("model".into(), "default".into()),
            ]),
        }
    }

    fn receive(&mut self, message: Value) -> io::Result<bool> {
        let method = message.get("method").and_then(Value::as_str);
        match method {
            Some("initialize") => self.initialize(&message)?,
            Some("session/new") => self.new_session(&message)?,
            Some("session/resume") => self.resume_session(&message)?,
            Some("session/set_config_option") => self.set_config_option(&message)?,
            Some("session/prompt") => return self.prompt(&message),
            Some("session/cancel") => self.cancel()?,
            Some("session/close") => {
                self.respond(&message, json!({}))?;
                return Ok(false);
            }
            _ if self.permission_pending
                && message.get("id") == Some(&json!("fixture-permission")) =>
            {
                self.permission_pending = false;
                self.message("permission granted")?;
                self.finish_active(false)?;
            }
            _ => {}
        }
        Ok(true)
    }

    fn initialize(&self, request: &Value) -> io::Result<()> {
        let result = match self.protocol {
            Protocol::V1 => {
                let capabilities = if std::env::var_os("ACP_FIXTURE_HIDE_RESUME").is_some() {
                    json!({})
                } else {
                    json!({ "sessionCapabilities": { "resume": {} } })
                };
                json!({
                    "protocolVersion": 1,
                    "agentCapabilities": capabilities,
                    "authMethods": [],
                    "agentInfo": { "name": "crab-fixture", "version": "1" }
                })
            }
            Protocol::V2 => {
                let capabilities = if std::env::var_os("ACP_FIXTURE_HIDE_STDIO_MCP").is_some() {
                    json!({})
                } else {
                    json!({ "session": { "mcp": { "stdio": {} } } })
                };
                json!({
                    "protocolVersion": 2,
                    "info": { "name": "crab-fixture", "version": "1" },
                    "capabilities": capabilities,
                    "authMethods": []
                })
            }
        };
        self.respond(request, result)
    }

    fn new_session(&mut self, request: &Value) -> io::Result<()> {
        self.respond(
            request,
            json!({
                "sessionId": self.session_id,
                "configOptions": self.config_options()
            }),
        )
    }

    fn resume_session(&mut self, request: &Value) -> io::Result<()> {
        if request.pointer("/params/sessionId") != Some(&json!(self.session_id)) {
            return self.respond_error(request, -32602, "unknown session");
        }
        self.respond(
            request,
            json!({
                "configOptions": self.config_options()
            }),
        )
    }

    fn set_config_option(&mut self, request: &Value) -> io::Result<()> {
        let config_id = request
            .pointer("/params/configId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let requested = request
            .pointer("/params/value")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !self.session_options.contains_key(config_id) || requested.is_empty() {
            return self.respond_error(request, -32602, "unsupported session option");
        }
        let effective = if std::env::var("ACP_FIXTURE_REWRITE_OPTION").as_deref() == Ok(config_id) {
            "rewritten"
        } else {
            requested
        };
        self.session_options
            .insert(config_id.to_owned(), effective.to_owned());
        self.respond(request, json!({ "configOptions": self.config_options() }))
    }

    fn config_options(&self) -> Vec<Value> {
        self.session_options
            .iter()
            .filter(|(config_id, _)| {
                std::env::var("ACP_FIXTURE_DROP_OPTION").as_deref() != Ok(config_id.as_str())
            })
            .map(|(config_id, current_value)| {
                let id_field = match self.protocol {
                    Protocol::V1 => "id",
                    Protocol::V2 => "configId",
                };
                let mut option = json!({
                    "name": config_id,
                    "type": "select",
                    "currentValue": current_value,
                    "options": [
                        { "value": "default", "name": "Default" },
                        { "value": "bypassPermissions", "name": "Bypass permissions" },
                        { "value": "opus", "name": "Opus" },
                        { "value": "rewritten", "name": "Rewritten" }
                    ]
                });
                option[id_field] = json!(config_id);
                option
            })
            .collect()
    }

    fn prompt(&mut self, request: &Value) -> io::Result<bool> {
        let text = request
            .pointer("/params/prompt")
            .and_then(Value::as_array)
            .and_then(|blocks| blocks.first())
            .and_then(|block| block.get("text"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        if text == "crash" {
            return Ok(false);
        }
        if text == "error" {
            self.respond_error(request, -32001, "fixture prompt failed")?;
            return Ok(true);
        }
        match self.protocol {
            Protocol::V1 => {
                if text == "hold" || text == "permission" {
                    self.active_v1_request = request.get("id").cloned();
                    self.message(&format!("accepted:{text}"))?;
                    if text == "permission" {
                        self.permission_pending = true;
                        self.permission_request()?;
                    }
                } else {
                    self.message(&format!("echo:{text}"))?;
                    self.respond(request, json!({ "stopReason": "end_turn" }))?;
                }
            }
            Protocol::V2 => {
                self.respond(request, json!({}))?;
                if !self.active_v2 {
                    self.active_v2 = true;
                    self.state("running", None)?;
                }
                self.message(&format!("echo:{text}"))?;
                if text == "permission" {
                    self.permission_pending = true;
                    self.permission_request()?;
                } else if text != "hold" && !self.permission_pending && text != "steer" {
                    self.finish_active(false)?;
                }
            }
        }
        Ok(true)
    }

    fn cancel(&mut self) -> io::Result<()> {
        self.finish_active(true)
    }

    fn finish_active(&mut self, cancelled: bool) -> io::Result<()> {
        match self.protocol {
            Protocol::V1 => {
                if let Some(id) = self.active_v1_request.take() {
                    emit(json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": {
                            "stopReason": if cancelled { "cancelled" } else { "end_turn" }
                        }
                    }))?;
                }
            }
            Protocol::V2 if self.active_v2 => {
                self.active_v2 = false;
                self.state(
                    "idle",
                    Some(if cancelled { "cancelled" } else { "end_turn" }),
                )?;
            }
            Protocol::V2 => {}
        }
        Ok(())
    }

    fn message(&self, text: &str) -> io::Result<()> {
        let update = match self.protocol {
            Protocol::V1 => json!({
                "sessionUpdate": "agent_message_chunk",
                "content": { "type": "text", "text": text }
            }),
            Protocol::V2 => json!({
                "sessionUpdate": "agent_message_chunk",
                "messageId": "fixture-message",
                "content": { "type": "text", "text": text }
            }),
        };
        emit(json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": { "sessionId": self.session_id, "update": update }
        }))
    }

    fn state(&self, state: &str, stop_reason: Option<&str>) -> io::Result<()> {
        let mut update = json!({ "sessionUpdate": "state_update", "state": state });
        if let Some(stop_reason) = stop_reason {
            update["stopReason"] = json!(stop_reason);
        }
        emit(json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": { "sessionId": self.session_id, "update": update }
        }))
    }

    fn permission_request(&self) -> io::Result<()> {
        let params = match self.protocol {
            Protocol::V1 => json!({
                "sessionId": self.session_id,
                "toolCall": { "toolCallId": "fixture-tool" },
                "options": [
                    { "optionId": "allow-once", "name": "Allow once", "kind": "allow_once" },
                    { "optionId": "allow-always", "name": "Always allow", "kind": "allow_always" }
                ]
            }),
            Protocol::V2 => json!({
                "sessionId": self.session_id,
                "title": "Fixture permission",
                "options": [
                    { "optionId": "allow-once", "name": "Allow once", "kind": "allow_once" },
                    { "optionId": "allow-always", "name": "Always allow", "kind": "allow_always" }
                ]
            }),
        };
        emit(json!({
            "jsonrpc": "2.0",
            "id": "fixture-permission",
            "method": "session/request_permission",
            "params": params
        }))
    }

    fn respond(&self, request: &Value, result: Value) -> io::Result<()> {
        emit(json!({
            "jsonrpc": "2.0",
            "id": request.get("id").cloned().unwrap_or(Value::Null),
            "result": result
        }))
    }

    fn respond_error(&self, request: &Value, code: i64, message: &str) -> io::Result<()> {
        emit(json!({
            "jsonrpc": "2.0",
            "id": request.get("id").cloned().unwrap_or(Value::Null),
            "error": { "code": code, "message": message }
        }))
    }
}

fn emit(message: Value) -> io::Result<()> {
    let mut stdout = io::stdout().lock();
    serde_json::to_writer(&mut stdout, &message)?;
    stdout.write_all(b"\n")?;
    stdout.flush()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protocol = match std::env::args().nth(1).as_deref() {
        Some("v1") => Protocol::V1,
        Some("v2") => Protocol::V2,
        _ => return Err("usage: acp_fixture v1|v2".into()),
    };
    let mut agent = FixtureAgent::new(protocol);
    for line in io::stdin().lock().lines() {
        let message = serde_json::from_str::<Value>(&line?)?;
        if !agent.receive(message)? {
            break;
        }
    }
    Ok(())
}
