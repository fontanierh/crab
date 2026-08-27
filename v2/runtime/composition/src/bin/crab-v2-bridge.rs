#![forbid(unsafe_code)]

use std::{ffi::OsString, io::Read as _, path::PathBuf, process::ExitCode};

use bridge_host_contract::{
    AuthenticationChallenge, AuthenticationMethod, BeginAuthenticationRequest, BridgeCatalog,
    BridgeLifecycle, BridgeReceipt, BridgeReference, BridgeStatus, CredentialLifecycle,
    CredentialStatus, ReconcileBridgeRequest, SubmitAuthenticationRequest,
};
use crab_v2_runtime::ChannelIpcClient;
use serde_json::{Value, json};

const USAGE: &str = "usage: crab-v2-bridge --state-dir <directory> <list | status <bridge> | reconcile <bridge> <generation> <true|false> | auth-begin <bridge> <auto|qr-code|phone-code|oauth|browser|terminal|manual> <context-json> | auth-submit <bridge> <challenge> <empty|stdin> | credentials-validate <bridge> | credentials-invalidate <bridge> | suspend <bridge> | stop <bridge>>";
const MAX_AUTH_RESPONSE_BYTES: u64 = 64 * 1024;

#[tokio::main]
async fn main() -> ExitCode {
    let arguments = match parse_arguments(std::env::args_os().skip(1)) {
        Ok(Some(arguments)) => arguments,
        Ok(None) => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Err(()) => {
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };
    let client = match ChannelIpcClient::from_state_directory(&arguments.state_directory) {
        Ok(client) => client,
        Err(error) => return failure(error),
    };
    let result = execute(&client, arguments.command).await;
    match result {
        Ok(value) => match serde_json::to_string(&value) {
            Ok(value) => {
                println!("{value}");
                ExitCode::SUCCESS
            }
            Err(_) => {
                eprintln!("crab-v2-bridge: output could not be encoded");
                ExitCode::FAILURE
            }
        },
        Err(error) => failure(error),
    }
}

fn failure(error: impl std::fmt::Display) -> ExitCode {
    eprintln!("crab-v2-bridge: {error}");
    ExitCode::FAILURE
}

async fn execute(
    client: &ChannelIpcClient,
    command: BridgeCommand,
) -> Result<Value, BridgeCliError> {
    let result = match command {
        BridgeCommand::List => client.list_bridges().await.map(catalog_json),
        BridgeCommand::Status { bridge_id } => client
            .bridge_status(BridgeReference { bridge_id })
            .await
            .map(status_json),
        BridgeCommand::Reconcile {
            bridge_id,
            expected_generation,
            desired_running,
        } => client
            .reconcile_bridge(ReconcileBridgeRequest {
                bridge_id,
                expected_generation,
                desired_running,
            })
            .await
            .map(status_json),
        BridgeCommand::AuthBegin {
            bridge_id,
            preferred_method,
            context_json,
        } => client
            .begin_bridge_authentication(BeginAuthenticationRequest {
                bridge_id,
                preferred_method,
                context_json,
            })
            .await
            .map(challenge_json),
        BridgeCommand::AuthSubmit {
            bridge_id,
            challenge_id,
            response,
        } => {
            let response_json = authentication_response(response)?;
            client
                .submit_bridge_authentication(SubmitAuthenticationRequest {
                    bridge_id,
                    challenge_id,
                    response_json,
                })
                .await
                .map(credential_json)
        }
        BridgeCommand::CredentialsValidate { bridge_id } => client
            .validate_bridge_credentials(BridgeReference { bridge_id })
            .await
            .map(credential_json),
        BridgeCommand::CredentialsInvalidate { bridge_id } => client
            .invalidate_bridge_credentials(BridgeReference { bridge_id })
            .await
            .map(receipt_json),
        BridgeCommand::Suspend { bridge_id } => client
            .suspend_bridge(BridgeReference { bridge_id })
            .await
            .map(status_json),
        BridgeCommand::Stop { bridge_id } => client
            .stop_bridge(BridgeReference { bridge_id })
            .await
            .map(receipt_json),
    };
    result.map_err(Into::into)
}

#[derive(Debug)]
enum BridgeCliError {
    Client(crab_v2_runtime::ChannelIpcClientError),
    InvalidAuthenticationResponse,
}

impl std::fmt::Display for BridgeCliError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Client(error) => error.fmt(formatter),
            Self::InvalidAuthenticationResponse => {
                formatter.write_str("authentication response must be one bounded JSON object")
            }
        }
    }
}

impl From<crab_v2_runtime::ChannelIpcClientError> for BridgeCliError {
    fn from(error: crab_v2_runtime::ChannelIpcClientError) -> Self {
        Self::Client(error)
    }
}

struct Arguments {
    state_directory: PathBuf,
    command: BridgeCommand,
}

#[derive(Debug, PartialEq)]
enum BridgeCommand {
    List,
    Status {
        bridge_id: String,
    },
    Reconcile {
        bridge_id: String,
        expected_generation: u64,
        desired_running: bool,
    },
    AuthBegin {
        bridge_id: String,
        preferred_method: Option<AuthenticationMethod>,
        context_json: String,
    },
    AuthSubmit {
        bridge_id: String,
        challenge_id: String,
        response: AuthenticationResponse,
    },
    CredentialsValidate {
        bridge_id: String,
    },
    CredentialsInvalidate {
        bridge_id: String,
    },
    Suspend {
        bridge_id: String,
    },
    Stop {
        bridge_id: String,
    },
}

#[derive(Debug, PartialEq)]
enum AuthenticationResponse {
    Empty,
    Stdin,
}

fn parse_arguments(mut values: impl Iterator<Item = OsString>) -> Result<Option<Arguments>, ()> {
    let first = next_text(&mut values).ok_or(())?;
    if first == "--help" || first == "-h" {
        return if values.next().is_none() {
            Ok(None)
        } else {
            Err(())
        };
    }
    if first != "--state-dir" {
        return Err(());
    }
    let state_directory = values.next().map(PathBuf::from).ok_or(())?;
    if state_directory.as_os_str().is_empty() {
        return Err(());
    }
    let command = next_text(&mut values).ok_or(())?;
    let command = match command.as_str() {
        "list" => BridgeCommand::List,
        "status" => BridgeCommand::Status {
            bridge_id: required_text(&mut values)?,
        },
        "reconcile" => BridgeCommand::Reconcile {
            bridge_id: required_text(&mut values)?,
            expected_generation: required_text(&mut values)?.parse().map_err(|_| ())?,
            desired_running: parse_bool(&required_text(&mut values)?).ok_or(())?,
        },
        "auth-begin" => BridgeCommand::AuthBegin {
            bridge_id: required_text(&mut values)?,
            preferred_method: parse_method(&required_text(&mut values)?).ok_or(())?,
            context_json: canonical_object(required_text(&mut values)?).ok_or(())?,
        },
        "auth-submit" => BridgeCommand::AuthSubmit {
            bridge_id: required_text(&mut values)?,
            challenge_id: required_text(&mut values)?,
            response: match required_text(&mut values)?.as_str() {
                "empty" => AuthenticationResponse::Empty,
                "stdin" => AuthenticationResponse::Stdin,
                _ => return Err(()),
            },
        },
        "credentials-validate" => BridgeCommand::CredentialsValidate {
            bridge_id: required_text(&mut values)?,
        },
        "credentials-invalidate" => BridgeCommand::CredentialsInvalidate {
            bridge_id: required_text(&mut values)?,
        },
        "suspend" => BridgeCommand::Suspend {
            bridge_id: required_text(&mut values)?,
        },
        "stop" => BridgeCommand::Stop {
            bridge_id: required_text(&mut values)?,
        },
        _ => return Err(()),
    };
    if values.next().is_some() {
        return Err(());
    }
    Ok(Some(Arguments {
        state_directory,
        command,
    }))
}

fn authentication_response(mode: AuthenticationResponse) -> Result<String, BridgeCliError> {
    match mode {
        AuthenticationResponse::Empty => Ok("{}".into()),
        AuthenticationResponse::Stdin => {
            let mut input = String::new();
            std::io::stdin()
                .take(MAX_AUTH_RESPONSE_BYTES + 1)
                .read_to_string(&mut input)
                .map_err(|_| BridgeCliError::InvalidAuthenticationResponse)?;
            if input.len() as u64 > MAX_AUTH_RESPONSE_BYTES {
                return Err(BridgeCliError::InvalidAuthenticationResponse);
            }
            validate_authentication_response(input.as_bytes())
        }
    }
}

fn validate_authentication_response(input: &[u8]) -> Result<String, BridgeCliError> {
    if input.len() as u64 > MAX_AUTH_RESPONSE_BYTES {
        return Err(BridgeCliError::InvalidAuthenticationResponse);
    }
    std::str::from_utf8(input)
        .ok()
        .and_then(|value| canonical_object(value.into()))
        .ok_or(BridgeCliError::InvalidAuthenticationResponse)
}

fn next_text(values: &mut impl Iterator<Item = OsString>) -> Option<String> {
    values.next()?.into_string().ok()
}

fn required_text(values: &mut impl Iterator<Item = OsString>) -> Result<String, ()> {
    next_text(values)
        .filter(|value| !value.trim().is_empty())
        .ok_or(())
}

fn parse_bool(value: &str) -> Option<bool> {
    match value {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

fn parse_method(value: &str) -> Option<Option<AuthenticationMethod>> {
    let method = match value {
        "auto" => return Some(None),
        "qr-code" => AuthenticationMethod::QrCode,
        "phone-code" => AuthenticationMethod::PhoneCode,
        "oauth" => AuthenticationMethod::OAuth,
        "browser" => AuthenticationMethod::Browser,
        "terminal" => AuthenticationMethod::Terminal,
        "manual" => AuthenticationMethod::Manual,
        _ => return None,
    };
    Some(Some(method))
}

fn canonical_object(value: String) -> Option<String> {
    let value = serde_json::from_str::<Value>(&value).ok()?;
    value.is_object().then(|| value.to_string())
}

fn catalog_json(catalog: BridgeCatalog) -> Value {
    json!({
        "bridges": catalog.bridges.into_iter().map(|bridge| json!({
            "bridgeId": bridge.bridge_id,
            "packageId": bridge.package_id,
            "displayName": bridge.display_name,
            "lifecycle": lifecycle_name(&bridge.lifecycle),
            "ingressMode": ingress_name(&bridge.ingress_mode),
            "alertTarget": bridge.alert_target.map(|target| json!({
                "channelId": target.channel_id,
                "lane": target.lane,
            })),
            "desiredRunning": bridge.desired_running,
            "generation": bridge.generation,
            "registeredAtMs": bridge.registered_at_ms,
        })).collect::<Vec<_>>()
    })
}

fn status_json(status: BridgeStatus) -> Value {
    json!({
        "bridgeId": status.bridge_id,
        "lifecycle": lifecycle_name(&status.lifecycle),
        "generation": status.generation,
        "consecutiveFailures": status.consecutive_failures,
        "restartCountInWindow": status.restart_count_in_window,
        "nextRestartAtMs": status.next_restart_at_ms,
        "lastHealth": status.last_health.map(|health| json!({
            "observedAtMs": health.observed_at_ms,
            "processAlive": health.process_alive,
            "serviceConnected": health.service_connected,
            "canReceive": health.can_receive,
            "canSend": health.can_send,
            "credentialLifecycle": credential_name(&health.credential_lifecycle),
            "detail": embedded_json(&health.detail_json),
        })),
        "lastError": status.last_error,
    })
}

fn challenge_json(challenge: AuthenticationChallenge) -> Value {
    json!({
        "bridgeId": challenge.bridge_id,
        "challengeId": challenge.challenge_id,
        "method": method_name(&challenge.method),
        "expiresAtMs": challenge.expires_at_ms,
        "presentation": embedded_json(&challenge.presentation_json),
    })
}

fn credential_json(status: CredentialStatus) -> Value {
    json!({
        "bridgeId": status.bridge_id,
        "lifecycle": credential_name(&status.lifecycle),
        "credentialStored": status.credential_handle.is_some(),
        "validatedAtMs": status.validated_at_ms,
        "expiresAtMs": status.expires_at_ms,
        "accountHint": status.account_hint,
        "detail": embedded_json(&status.detail_json),
    })
}

fn receipt_json(receipt: BridgeReceipt) -> Value {
    json!({"accepted": receipt.accepted, "recordedAtMs": receipt.recorded_at_ms})
}

fn embedded_json(value: &str) -> Value {
    serde_json::from_str(value).unwrap_or(Value::Null)
}

fn lifecycle_name(value: &BridgeLifecycle) -> &'static str {
    match value {
        BridgeLifecycle::Registered => "registered",
        BridgeLifecycle::Starting => "starting",
        BridgeLifecycle::AwaitingAuthentication => "awaiting-authentication",
        BridgeLifecycle::Healthy => "healthy",
        BridgeLifecycle::Degraded => "degraded",
        BridgeLifecycle::BackingOff => "backing-off",
        BridgeLifecycle::Stopped => "stopped",
        BridgeLifecycle::Failed => "failed",
        BridgeLifecycle::Unknown { .. } => "unknown",
    }
}

fn credential_name(value: &CredentialLifecycle) -> &'static str {
    match value {
        CredentialLifecycle::Missing => "missing",
        CredentialLifecycle::Challenged => "challenged",
        CredentialLifecycle::Validating => "validating",
        CredentialLifecycle::Valid => "valid",
        CredentialLifecycle::Expiring => "expiring",
        CredentialLifecycle::Rejected => "rejected",
        CredentialLifecycle::Revoked => "revoked",
        CredentialLifecycle::Unknown { .. } => "unknown",
    }
}

fn method_name(value: &AuthenticationMethod) -> &'static str {
    match value {
        AuthenticationMethod::QrCode => "qr-code",
        AuthenticationMethod::PhoneCode => "phone-code",
        AuthenticationMethod::OAuth => "oauth",
        AuthenticationMethod::Browser => "browser",
        AuthenticationMethod::Terminal => "terminal",
        AuthenticationMethod::Manual => "manual",
        AuthenticationMethod::Unknown { .. } => "unknown",
    }
}

fn ingress_name(value: &bridge_host_contract::BridgeIngressMode) -> &'static str {
    match value {
        bridge_host_contract::BridgeIngressMode::Queue => "queue",
        bridge_host_contract::BridgeIngressMode::Steer => "steer",
        bridge_host_contract::BridgeIngressMode::InterruptAndSteer => "interrupt-and-steer",
        bridge_host_contract::BridgeIngressMode::Unknown { .. } => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use bridge_host_contract::{AuthenticationMethod, CredentialLifecycle, CredentialStatus};

    use super::{
        AuthenticationResponse, BridgeCommand, MAX_AUTH_RESPONSE_BYTES, credential_json,
        parse_arguments, validate_authentication_response,
    };

    fn parse(values: &[&str]) -> BridgeCommand {
        parse_arguments(values.iter().map(OsString::from))
            .expect("arguments parse")
            .expect("run requested")
            .command
    }

    #[test]
    fn parser_accepts_strict_bridge_operations_and_canonical_json() {
        assert_eq!(
            parse(&["--state-dir", "/tmp/crab", "list"]),
            BridgeCommand::List
        );
        assert_eq!(
            parse(&[
                "--state-dir",
                "/tmp/crab",
                "auth-begin",
                "whatsapp",
                "phone-code",
                "{ \"phoneNumber\": \"+33600000000\" }",
            ]),
            BridgeCommand::AuthBegin {
                bridge_id: "whatsapp".into(),
                preferred_method: Some(AuthenticationMethod::PhoneCode),
                context_json: r#"{"phoneNumber":"+33600000000"}"#.into(),
            }
        );
        assert_eq!(
            parse(&[
                "--state-dir",
                "/tmp/crab",
                "auth-submit",
                "whatsapp",
                "challenge-1",
                "stdin",
            ]),
            BridgeCommand::AuthSubmit {
                bridge_id: "whatsapp".into(),
                challenge_id: "challenge-1".into(),
                response: AuthenticationResponse::Stdin,
            }
        );
        assert_eq!(
            parse(&[
                "--state-dir",
                "/tmp/crab",
                "reconcile",
                "whatsapp",
                "2",
                "true",
            ]),
            BridgeCommand::Reconcile {
                bridge_id: "whatsapp".into(),
                expected_generation: 2,
                desired_running: true,
            }
        );
    }

    #[test]
    fn parser_rejects_ambiguous_or_non_object_inputs() {
        for values in [
            vec!["list"],
            vec!["--state-dir", "/tmp/crab", "status"],
            vec![
                "--state-dir",
                "/tmp/crab",
                "auth-begin",
                "whatsapp",
                "phone-code",
                "[]",
            ],
            vec!["--state-dir", "/tmp/crab", "list", "extra"],
            vec![
                "--state-dir",
                "/tmp/crab",
                "auth-submit",
                "whatsapp",
                "challenge-1",
                r#"{"secret":"must-not-be-an-argument"}"#,
            ],
        ] {
            assert!(parse_arguments(values.into_iter().map(OsString::from)).is_err());
        }
    }

    #[test]
    fn authentication_response_is_one_bounded_json_object() {
        assert_eq!(
            validate_authentication_response(br#"{ "pin": "123456" }"#).expect("valid response"),
            r#"{"pin":"123456"}"#
        );
        assert!(validate_authentication_response(b"[]").is_err());
        assert!(validate_authentication_response(b"not-json").is_err());
        assert!(
            validate_authentication_response(&vec![b'x'; MAX_AUTH_RESPONSE_BYTES as usize + 1])
                .is_err()
        );
    }

    #[test]
    fn credential_output_exposes_presence_without_the_private_handle() {
        let output = credential_json(CredentialStatus {
            bridge_id: "whatsapp".into(),
            lifecycle: CredentialLifecycle::Valid,
            credential_handle: Some("private:must-never-cross-cli".into()),
            validated_at_ms: Some(42),
            expires_at_ms: None,
            account_hint: Some("paired-account".into()),
            detail_json: "{}".into(),
        });
        let encoded = serde_json::to_string(&output).expect("output encodes");
        assert_eq!(output["credentialStored"], true);
        assert!(!encoded.contains("must-never-cross-cli"));
        assert!(output.get("credentialHandle").is_none());
    }
}
