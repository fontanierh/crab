use std::io::{self, BufRead, Write};

use serde_json::{Value, json};

fn main() {
    let stdin = io::stdin();
    let mut stdout = io::stdout().lock();
    for line in stdin.lock().lines() {
        let Ok(line) = line else { break };
        let Ok(request) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        if request.get("method").is_none() {
            continue;
        }
        let id = request.get("id").cloned().unwrap_or(Value::Null);
        let method = request.get("method").and_then(Value::as_str).unwrap_or("");
        let params = request.get("params").cloned().unwrap_or_else(|| json!({}));
        if method == "bridge/initialize" {
            let inbound = json!({
                "jsonrpc": "2.0",
                "id": "fixture-inbound-1",
                "method": "bridge/inbound",
                "params": {
                    "bridgeId": "fixture-bridge",
                    "externalEventId": "fixture-event-1",
                    "receivedAtMs": 1,
                    "targetChannelId": "fixture-channel",
                    "sender": { "fixture": true },
                    "message": { "text": "hello from fixture" },
                    "attachments": []
                }
            });
            writeln!(stdout, "{inbound}").expect("fixture writes inbound request");
            stdout.flush().expect("fixture flushes inbound request");
        }
        let result = match method {
            "bridge/initialize" => json!({}),
            "bridge/health" => json!({
                "processAlive": true,
                "serviceConnected": true,
                "canReceive": true,
                "canSend": true,
                "credentialValid": !params["credential"].is_null(),
                "detail": { "fixture": true }
            }),
            "bridge/auth/begin" => json!({
                "method": params["method"].as_str().unwrap_or("phoneCode"),
                "expiresAtMs": 10_000,
                "presentation": { "prompt": "enter code" }
            }),
            "bridge/auth/submit" => json!({
                "credential": { "token": "fixture-secret" },
                "expiresAtMs": null,
                "accountHint": "fixture-account",
                "detail": { "paired": true }
            }),
            "bridge/auth/validate" => json!({
                "valid": params["credential"]["token"] == "fixture-secret",
                "expiresAtMs": null,
                "accountHint": "fixture-account",
                "detail": { "validated": true }
            }),
            "bridge/auth/invalidate" => json!({}),
            "bridge/deliver" => json!({
                "externalDeliveryId": format!("external-{}", params["messageId"].as_str().unwrap_or("unknown")),
                "detail": { "sent": true }
            }),
            "bridge/shutdown" => {
                write_response(&mut stdout, id, json!({}));
                break;
            }
            _ => {
                let response = json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": { "code": -32601, "message": "unknown method" }
                });
                writeln!(stdout, "{response}").expect("fixture writes response");
                stdout.flush().expect("fixture flushes response");
                continue;
            }
        };
        write_response(&mut stdout, id, result);
    }
}

fn write_response(stdout: &mut impl Write, id: Value, result: Value) {
    let response = json!({ "jsonrpc": "2.0", "id": id, "result": result });
    writeln!(stdout, "{response}").expect("fixture writes response");
    stdout.flush().expect("fixture flushes response");
}
