use std::io::{self, BufRead, Write};

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
}

impl FixtureAgent {
    fn new(protocol: Protocol) -> Self {
        Self {
            protocol,
            session_id: "fixture-native-session".into(),
            active_v1_request: None,
            active_v2: false,
            permission_pending: false,
        }
    }

    fn receive(&mut self, message: Value) -> io::Result<bool> {
        let method = message.get("method").and_then(Value::as_str);
        match method {
            Some("initialize") => self.initialize(&message)?,
            Some("session/new") => self.new_session(&message)?,
            Some("session/prompt") => self.prompt(&message)?,
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
            Protocol::V1 => json!({
                "protocolVersion": 1,
                "agentCapabilities": {},
                "authMethods": [],
                "agentInfo": { "name": "crab-fixture", "version": "1" }
            }),
            Protocol::V2 => json!({
                "protocolVersion": 2,
                "info": { "name": "crab-fixture", "version": "1" },
                "capabilities": {},
                "authMethods": []
            }),
        };
        self.respond(request, result)
    }

    fn new_session(&self, request: &Value) -> io::Result<()> {
        self.respond(request, json!({ "sessionId": self.session_id }))
    }

    fn prompt(&mut self, request: &Value) -> io::Result<()> {
        let text = request
            .pointer("/params/prompt")
            .and_then(Value::as_array)
            .and_then(|blocks| blocks.first())
            .and_then(|block| block.get("text"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
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
        Ok(())
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
