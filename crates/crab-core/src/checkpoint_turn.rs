use crate::{validation::validate_non_empty_text, CrabError, CrabResult};
use serde::{Deserialize, Serialize};

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

    use super::parse_checkpoint_turn_document;

    fn valid_json() -> &'static str {
        r#"{
  "summary": "Session summary",
  "decisions": ["Use codex backend"],
  "open_questions": ["Need model override?"],
  "next_actions": ["Implement checkpoint writer"],
  "artifacts": [{"path": "src/main.rs", "note": "entrypoint updated"}]
}"#
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
}
