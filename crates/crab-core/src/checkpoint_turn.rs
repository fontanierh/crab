use crate::{validation::validate_non_empty_text, CrabError, CrabResult};
use serde::{Deserialize, Serialize};

pub const DEFAULT_CHECKPOINT_MAX_ATTEMPTS: u8 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointTurnArtifact {
    pub path: String,
    pub note: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointTurnDocument {
    pub summary: String,
    pub decisions: Vec<String>,
    pub open_questions: Vec<String>,
    pub next_actions: Vec<String>,
    pub artifacts: Vec<CheckpointTurnArtifact>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointTurnResolution {
    Parsed(CheckpointTurnDocument),
    Retry {
        attempt: u8,
        max_attempts: u8,
        corrective_prompt: String,
        error: String,
    },
    Exhausted {
        attempt: u8,
        max_attempts: u8,
        error: String,
    },
}

pub fn build_checkpoint_prompt() -> String {
    "You are running a hidden checkpoint turn.\n\
Return exactly one strict JSON object with this shape:\n\
{\n\
  \"summary\": \"string\",\n\
  \"decisions\": [\"string\"],\n\
  \"open_questions\": [\"string\"],\n\
  \"next_actions\": [\"string\"],\n\
  \"artifacts\": [{\"path\": \"string\", \"note\": \"string\"}]\n\
}\n\
No markdown. No prose outside the JSON object."
        .to_string()
}

pub fn resolve_checkpoint_turn_output(
    raw_output: &str,
    attempt: u8,
    max_attempts: u8,
) -> CrabResult<CheckpointTurnResolution> {
    validate_checkpoint_attempt_bounds(attempt, max_attempts)?;

    match parse_checkpoint_turn_document(raw_output) {
        Ok(document) => Ok(CheckpointTurnResolution::Parsed(document)),
        Err(error) => {
            let error_message = error.to_string();
            if attempt < max_attempts {
                return Ok(CheckpointTurnResolution::Retry {
                    attempt,
                    max_attempts,
                    corrective_prompt: build_checkpoint_retry_prompt(&error_message),
                    error: error_message,
                });
            }

            Ok(CheckpointTurnResolution::Exhausted {
                attempt,
                max_attempts,
                error: error_message,
            })
        }
    }
}

pub fn parse_checkpoint_turn_document(raw_output: &str) -> CrabResult<CheckpointTurnDocument> {
    validate_non_empty_text("checkpoint_turn_parse", "raw_output", raw_output)?;

    let parsed: CheckpointTurnDocument =
        serde_json::from_str(raw_output).map_err(|error| CrabError::Serialization {
            context: "checkpoint_turn_parse",
            path: None,
            message: error.to_string(),
        })?;

    validate_checkpoint_turn_document(&parsed)?;
    Ok(parsed)
}

fn build_checkpoint_retry_prompt(parse_error: &str) -> String {
    format!(
        "Your previous checkpoint response was invalid: {parse_error}\n\
Return a corrected response as strict JSON only.\n\
Required schema:\n\
{{\n\
  \"summary\": \"string\",\n\
  \"decisions\": [\"string\"],\n\
  \"open_questions\": [\"string\"],\n\
  \"next_actions\": [\"string\"],\n\
  \"artifacts\": [{{\"path\": \"string\", \"note\": \"string\"}}]\n\
}}\n\
No markdown. No extra keys. No surrounding prose."
    )
}

fn validate_checkpoint_attempt_bounds(attempt: u8, max_attempts: u8) -> CrabResult<()> {
    if attempt == 0 {
        return Err(CrabError::InvariantViolation {
            context: "checkpoint_turn_retry",
            message: "attempt must be greater than 0".to_string(),
        });
    }
    if max_attempts == 0 {
        return Err(CrabError::InvariantViolation {
            context: "checkpoint_turn_retry",
            message: "max_attempts must be greater than 0".to_string(),
        });
    }
    if attempt > max_attempts {
        return Err(CrabError::InvariantViolation {
            context: "checkpoint_turn_retry",
            message: format!("attempt {attempt} exceeds max_attempts {max_attempts}"),
        });
    }
    Ok(())
}

fn validate_checkpoint_turn_document(document: &CheckpointTurnDocument) -> CrabResult<()> {
    let context = "checkpoint_turn_schema";
    validate_non_empty_text(context, "summary", &document.summary)?;

    validate_non_empty_items(context, "decisions", &document.decisions)?;
    validate_non_empty_items(context, "open_questions", &document.open_questions)?;
    validate_non_empty_items(context, "next_actions", &document.next_actions)?;

    for (index, artifact) in document.artifacts.iter().enumerate() {
        validate_non_empty_text(context, &format!("artifacts[{index}].path"), &artifact.path)?;
        validate_non_empty_text(context, &format!("artifacts[{index}].note"), &artifact.note)?;
    }

    Ok(())
}

fn validate_non_empty_items(
    context: &'static str,
    field: &str,
    items: &[String],
) -> CrabResult<()> {
    for (index, value) in items.iter().enumerate() {
        validate_non_empty_text(context, &format!("{field}[{index}]"), value)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::CrabError;

    use super::{
        build_checkpoint_prompt, parse_checkpoint_turn_document, resolve_checkpoint_turn_output,
        CheckpointTurnResolution, DEFAULT_CHECKPOINT_MAX_ATTEMPTS,
    };

    fn valid_json() -> &'static str {
        r#"{
  "summary": "Session summary",
  "decisions": ["Use codex backend"],
  "open_questions": ["Need model override?"],
  "next_actions": ["Implement checkpoint writer"],
  "artifacts": [{"path": "src/main.rs", "note": "entrypoint updated"}]
}"#
    }

    fn parsed_summary(resolution: &CheckpointTurnResolution) -> Option<&str> {
        match resolution {
            CheckpointTurnResolution::Parsed(document) => Some(document.summary.as_str()),
            CheckpointTurnResolution::Retry { .. } | CheckpointTurnResolution::Exhausted { .. } => {
                None
            }
        }
    }

    fn retry_metadata(resolution: &CheckpointTurnResolution) -> Option<(u8, u8, &str, &str)> {
        match resolution {
            CheckpointTurnResolution::Retry {
                attempt,
                max_attempts,
                corrective_prompt,
                error,
            } => Some((
                *attempt,
                *max_attempts,
                corrective_prompt.as_str(),
                error.as_str(),
            )),
            CheckpointTurnResolution::Parsed(_) | CheckpointTurnResolution::Exhausted { .. } => {
                None
            }
        }
    }

    fn exhausted_metadata(resolution: &CheckpointTurnResolution) -> Option<(u8, u8, &str)> {
        match resolution {
            CheckpointTurnResolution::Exhausted {
                attempt,
                max_attempts,
                error,
            } => Some((*attempt, *max_attempts, error.as_str())),
            CheckpointTurnResolution::Parsed(_) | CheckpointTurnResolution::Retry { .. } => None,
        }
    }

    #[test]
    fn default_attempt_policy_retries_once() {
        assert_eq!(DEFAULT_CHECKPOINT_MAX_ATTEMPTS, 2);
    }

    #[test]
    fn checkpoint_prompt_describes_required_schema() {
        let prompt = build_checkpoint_prompt();
        assert!(prompt.contains("\"summary\""));
        assert!(prompt.contains("\"decisions\""));
        assert!(prompt.contains("\"open_questions\""));
        assert!(prompt.contains("\"next_actions\""));
        assert!(prompt.contains("\"artifacts\""));
        assert!(prompt.contains("No markdown"));
    }

    #[test]
    fn parses_valid_document() {
        let parsed = parse_checkpoint_turn_document(valid_json()).expect("valid json should parse");
        assert_eq!(parsed.summary, "Session summary");
        assert_eq!(parsed.decisions, vec!["Use codex backend".to_string()]);
        assert_eq!(
            parsed.open_questions,
            vec!["Need model override?".to_string()]
        );
        assert_eq!(
            parsed.next_actions,
            vec!["Implement checkpoint writer".to_string()]
        );
        assert_eq!(parsed.artifacts.len(), 1);
        assert_eq!(parsed.artifacts[0].path, "src/main.rs");
        assert_eq!(parsed.artifacts[0].note, "entrypoint updated");
    }

    #[test]
    fn parser_accepts_surrounding_whitespace() {
        let output = format!(" \n{}\n ", valid_json());
        let parsed =
            parse_checkpoint_turn_document(&output).expect("whitespace should still parse");
        assert_eq!(parsed.summary, "Session summary");
    }

    #[test]
    fn parser_rejects_blank_output() {
        let error =
            parse_checkpoint_turn_document(" ").expect_err("blank output should be rejected");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "checkpoint_turn_parse",
                message: "raw_output must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn parser_rejects_invalid_json() {
        let error =
            parse_checkpoint_turn_document("{ bad json").expect_err("invalid json should fail");
        assert!(matches!(
            error,
            CrabError::Serialization {
                context: "checkpoint_turn_parse",
                ..
            }
        ));
    }

    #[test]
    fn parser_rejects_unknown_keys() {
        let output = r#"{
  "summary": "ok",
  "decisions": [],
  "open_questions": [],
  "next_actions": [],
  "artifacts": [],
  "extra": "not allowed"
}"#;

        let error =
            parse_checkpoint_turn_document(output).expect_err("unknown key should be rejected");
        assert!(matches!(
            error,
            CrabError::Serialization {
                context: "checkpoint_turn_parse",
                ..
            }
        ));
    }

    #[test]
    fn parser_rejects_blank_summary() {
        let output = r#"{
  "summary": " ",
  "decisions": [],
  "open_questions": [],
  "next_actions": [],
  "artifacts": []
}"#;
        let error =
            parse_checkpoint_turn_document(output).expect_err("blank summary should fail schema");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "checkpoint_turn_schema",
                message: "summary must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn parser_rejects_blank_decision_item() {
        let output = r#"{
  "summary": "ok",
  "decisions": [" "],
  "open_questions": [],
  "next_actions": [],
  "artifacts": []
}"#;
        let error =
            parse_checkpoint_turn_document(output).expect_err("blank decision should fail schema");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "checkpoint_turn_schema",
                message: "decisions[0] must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn parser_rejects_blank_open_question_item() {
        let output = r#"{
  "summary": "ok",
  "decisions": [],
  "open_questions": [" "],
  "next_actions": [],
  "artifacts": []
}"#;
        let error = parse_checkpoint_turn_document(output)
            .expect_err("blank open question should fail schema");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "checkpoint_turn_schema",
                message: "open_questions[0] must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn parser_rejects_blank_next_action_item() {
        let output = r#"{
  "summary": "ok",
  "decisions": [],
  "open_questions": [],
  "next_actions": [" "],
  "artifacts": []
}"#;
        let error = parse_checkpoint_turn_document(output)
            .expect_err("blank next action should fail schema");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "checkpoint_turn_schema",
                message: "next_actions[0] must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn parser_rejects_blank_artifact_fields() {
        let blank_path = r#"{
  "summary": "ok",
  "decisions": [],
  "open_questions": [],
  "next_actions": [],
  "artifacts": [{"path": " ", "note": "good"}]
}"#;
        let error = parse_checkpoint_turn_document(blank_path)
            .expect_err("blank artifact path should fail schema");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "checkpoint_turn_schema",
                message: "artifacts[0].path must not be empty".to_string(),
            }
        );

        let blank_note = r#"{
  "summary": "ok",
  "decisions": [],
  "open_questions": [],
  "next_actions": [],
  "artifacts": [{"path": "src/main.rs", "note": " "} ]
}"#;
        let error = parse_checkpoint_turn_document(blank_note)
            .expect_err("blank artifact note should fail schema");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "checkpoint_turn_schema",
                message: "artifacts[0].note must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn resolve_returns_parsed_document_when_valid() {
        let resolution =
            resolve_checkpoint_turn_output(valid_json(), 1, 2).expect("resolution should succeed");
        assert_eq!(parsed_summary(&resolution), Some("Session summary"));
        assert_eq!(retry_metadata(&resolution), None);
        assert_eq!(exhausted_metadata(&resolution), None);
    }

    #[test]
    fn resolve_requests_retry_on_first_failure() {
        let resolution = resolve_checkpoint_turn_output("invalid", 1, 2)
            .expect("resolution should still succeed with retry decision");
        assert_eq!(parsed_summary(&resolution), None);
        let (attempt, max_attempts, corrective_prompt, error) =
            retry_metadata(&resolution).expect("resolution should include retry metadata");
        assert_eq!(attempt, 1);
        assert_eq!(max_attempts, 2);
        assert!(corrective_prompt.contains("invalid"));
        assert!(corrective_prompt.contains("\"summary\""));
        assert!(corrective_prompt.contains("No extra keys"));
        assert!(error.contains("checkpoint_turn_parse"));
        assert_eq!(exhausted_metadata(&resolution), None);
    }

    #[test]
    fn resolve_reports_exhausted_after_final_attempt() {
        let resolution = resolve_checkpoint_turn_output("invalid", 2, 2)
            .expect("resolution should produce exhausted result");
        assert_eq!(parsed_summary(&resolution), None);
        assert_eq!(retry_metadata(&resolution), None);
        let (attempt, max_attempts, error) =
            exhausted_metadata(&resolution).expect("resolution should include exhaustion metadata");
        assert_eq!(attempt, 2);
        assert_eq!(max_attempts, 2);
        assert!(error.contains("checkpoint_turn_parse"));
    }

    #[test]
    fn resolve_validates_attempt_bounds() {
        let zero_attempt = resolve_checkpoint_turn_output(valid_json(), 0, 2)
            .expect_err("attempt must be at least one");
        assert_eq!(
            zero_attempt,
            CrabError::InvariantViolation {
                context: "checkpoint_turn_retry",
                message: "attempt must be greater than 0".to_string(),
            }
        );

        let zero_max = resolve_checkpoint_turn_output(valid_json(), 1, 0)
            .expect_err("max attempts must be at least one");
        assert_eq!(
            zero_max,
            CrabError::InvariantViolation {
                context: "checkpoint_turn_retry",
                message: "max_attempts must be greater than 0".to_string(),
            }
        );

        let out_of_range = resolve_checkpoint_turn_output(valid_json(), 3, 2)
            .expect_err("attempt cannot exceed max");
        assert_eq!(
            out_of_range,
            CrabError::InvariantViolation {
                context: "checkpoint_turn_retry",
                message: "attempt 3 exceeds max_attempts 2".to_string(),
            }
        );
    }

    #[test]
    fn helper_extractors_cover_all_resolution_variants() {
        let parsed =
            resolve_checkpoint_turn_output(valid_json(), 1, 2).expect("valid output should parse");
        let retry = resolve_checkpoint_turn_output("invalid", 1, 2)
            .expect("first invalid output should request retry");
        let exhausted = resolve_checkpoint_turn_output("invalid", 2, 2)
            .expect("final invalid output should exhaust retries");

        assert_eq!(parsed_summary(&parsed), Some("Session summary"));
        assert_eq!(parsed_summary(&retry), None);
        assert_eq!(parsed_summary(&exhausted), None);

        assert!(retry_metadata(&parsed).is_none());
        assert_eq!(
            retry_metadata(&retry).map(|(attempt, _, _, _)| attempt),
            Some(1)
        );
        assert!(retry_metadata(&exhausted).is_none());

        assert!(exhausted_metadata(&parsed).is_none());
        assert!(exhausted_metadata(&retry).is_none());
        assert_eq!(
            exhausted_metadata(&exhausted).map(|(attempt, _, _)| attempt),
            Some(2)
        );
    }
}
