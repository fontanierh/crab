use serde::{Deserialize, Serialize};

use crate::validation::validate_non_empty_text;
use crate::{CrabError, CrabResult};

const ONBOARDING_PROMPT_CONTEXT: &str = "onboarding_prompt";
const ONBOARDING_CAPTURE_CONTEXT: &str = "onboarding_capture_parse";
pub const ONBOARDING_SCHEMA_VERSION: &str = "v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnboardingField {
    AgentIdentity,
    OwnerIdentity,
    PrimaryGoals,
    MachineLocation,
    MachineTimezone,
}

impl OnboardingField {
    #[must_use]
    pub const fn as_key(self) -> &'static str {
        match self {
            Self::AgentIdentity => "agent_identity",
            Self::OwnerIdentity => "owner_identity",
            Self::PrimaryGoals => "primary_goals",
            Self::MachineLocation => "machine_location",
            Self::MachineTimezone => "machine_timezone",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OnboardingQuestion {
    pub field: OnboardingField,
    pub question: &'static str,
}

const ONBOARDING_QUESTIONS: [OnboardingQuestion; 5] = [
    OnboardingQuestion {
        field: OnboardingField::AgentIdentity,
        question: "Who are you as this Crab agent (name and role)?",
    },
    OnboardingQuestion {
        field: OnboardingField::OwnerIdentity,
        question: "Who is your owner and how should you work with them?",
    },
    OnboardingQuestion {
        field: OnboardingField::PrimaryGoals,
        question: "What are your primary goals on this machine? Provide an ordered list.",
    },
    OnboardingQuestion {
        field: OnboardingField::MachineLocation,
        question: "Where is the machine physically located?",
    },
    OnboardingQuestion {
        field: OnboardingField::MachineTimezone,
        question: "What timezone should be used for scheduling/date reasoning (IANA token such as Europe/Paris)?",
    },
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OnboardingCaptureDocument {
    pub schema_version: String,
    pub agent_identity: String,
    pub owner_identity: String,
    pub primary_goals: Vec<String>,
    pub machine_location: String,
    pub machine_timezone: String,
}

#[must_use]
pub fn default_onboarding_questions() -> &'static [OnboardingQuestion] {
    &ONBOARDING_QUESTIONS
}

pub fn build_onboarding_prompt(onboarding_session_id: &str) -> CrabResult<String> {
    validate_non_empty_text(
        ONBOARDING_PROMPT_CONTEXT,
        "onboarding_session_id",
        onboarding_session_id,
    )?;

    let question_block = ONBOARDING_QUESTIONS
        .iter()
        .enumerate()
        .map(|(index, question)| format!("{}. {}", index + 1, question.question))
        .collect::<Vec<_>>()
        .join("\n");

    Ok(format!(
        "Onboarding session id: {onboarding_session_id}\n\
You are running the first-run onboarding contract.\n\
Ask the owner the required questions and capture answers.\n\
Return exactly one JSON object with this schema (no markdown fences):\n\
{{\n\
  \"schema_version\": \"{ONBOARDING_SCHEMA_VERSION}\",\n\
  \"agent_identity\": \"...\",\n\
  \"owner_identity\": \"...\",\n\
  \"primary_goals\": [\"...\"],\n\
  \"machine_location\": \"...\",\n\
  \"machine_timezone\": \"...\"\n\
}}\n\
Required questions:\n\
{question_block}\n\
Rules:\n\
- Ask each required question at most once.\n\
- If a required answer is missing, ask a concise follow-up.\n\
- Do not add extra JSON keys.\n"
    ))
}

pub fn parse_onboarding_capture_document(
    raw_output: &str,
) -> CrabResult<OnboardingCaptureDocument> {
    validate_non_empty_text(ONBOARDING_CAPTURE_CONTEXT, "raw_output", raw_output)?;

    let mut parsed: OnboardingCaptureDocument =
        serde_json::from_str(raw_output).map_err(|error| CrabError::InvariantViolation {
            context: ONBOARDING_CAPTURE_CONTEXT,
            message: format!("invalid onboarding JSON: {error}"),
        })?;
    normalize_and_validate_capture(&mut parsed)?;
    Ok(parsed)
}

fn normalize_and_validate_capture(document: &mut OnboardingCaptureDocument) -> CrabResult<()> {
    document.schema_version = normalize_scalar("schema_version", &document.schema_version)?;
    if document.schema_version != ONBOARDING_SCHEMA_VERSION {
        return Err(CrabError::InvariantViolation {
            context: ONBOARDING_CAPTURE_CONTEXT,
            message: format!(
                "schema_version must be {:?}, got {:?}",
                ONBOARDING_SCHEMA_VERSION, document.schema_version
            ),
        });
    }

    document.agent_identity = normalize_scalar("agent_identity", &document.agent_identity)?;
    document.owner_identity = normalize_scalar("owner_identity", &document.owner_identity)?;
    document.machine_location = normalize_scalar("machine_location", &document.machine_location)?;
    document.machine_timezone = normalize_scalar("machine_timezone", &document.machine_timezone)?;

    if document.primary_goals.is_empty() {
        return Err(CrabError::InvariantViolation {
            context: ONBOARDING_CAPTURE_CONTEXT,
            message: "primary_goals must contain at least one goal".to_string(),
        });
    }
    for goal in &mut document.primary_goals {
        *goal = normalize_scalar("primary_goals[]", goal)?;
    }

    Ok(())
}

fn normalize_scalar(field: &str, value: &str) -> CrabResult<String> {
    let normalized = value.trim();
    validate_non_empty_text(ONBOARDING_CAPTURE_CONTEXT, field, normalized)?;
    Ok(normalized.to_string())
}

#[cfg(test)]
mod tests {
    use super::{
        build_onboarding_prompt, default_onboarding_questions, parse_onboarding_capture_document,
        OnboardingCaptureDocument, OnboardingField, ONBOARDING_SCHEMA_VERSION,
    };
    use crate::CrabError;

    fn assert_invariant_error(
        result: crate::CrabResult<impl Sized + std::fmt::Debug>,
        message: &str,
    ) {
        let error = result.expect_err("expected invariant violation");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "onboarding_capture_parse",
                message: message.to_string(),
            }
        );
    }

    fn valid_capture_document() -> OnboardingCaptureDocument {
        OnboardingCaptureDocument {
            schema_version: ONBOARDING_SCHEMA_VERSION.to_string(),
            agent_identity: "Crab".to_string(),
            owner_identity: "Henry".to_string(),
            primary_goals: vec!["Ship fast".to_string()],
            machine_location: "Paris, France".to_string(),
            machine_timezone: "Europe/Paris".to_string(),
        }
    }

    #[test]
    fn question_contract_fields_are_stable() {
        let keys = default_onboarding_questions()
            .iter()
            .map(|question| question.field.as_key())
            .collect::<Vec<_>>();

        assert_eq!(
            keys,
            vec![
                OnboardingField::AgentIdentity.as_key(),
                OnboardingField::OwnerIdentity.as_key(),
                OnboardingField::PrimaryGoals.as_key(),
                OnboardingField::MachineLocation.as_key(),
                OnboardingField::MachineTimezone.as_key(),
            ]
        );
    }

    #[test]
    fn onboarding_prompt_contains_required_questions_and_schema() {
        let prompt = build_onboarding_prompt("bootstrap-1").expect("prompt should render");
        assert!(prompt.contains("Onboarding session id: bootstrap-1"));
        assert!(prompt.contains(&format!(
            "\"schema_version\": \"{ONBOARDING_SCHEMA_VERSION}\""
        )));
        for question in default_onboarding_questions() {
            assert!(prompt.contains(question.question));
            assert!(prompt.contains(question.field.as_key()));
        }
    }

    #[test]
    fn onboarding_prompt_rejects_blank_session_id() {
        let error = build_onboarding_prompt(" ").expect_err("blank onboarding id should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "onboarding_prompt",
                message: "onboarding_session_id must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn capture_parser_normalizes_valid_document() {
        let raw = r#"{
            "schema_version": " v1 ",
            "agent_identity": " Crab ",
            "owner_identity": " Henry ",
            "primary_goals": ["Keep code quality high", "Ship fast"],
            "machine_location": " Paris, France ",
            "machine_timezone": " Europe/Paris "
        }"#;

        let parsed = parse_onboarding_capture_document(raw).expect("document should parse");
        assert_eq!(
            parsed,
            OnboardingCaptureDocument {
                schema_version: "v1".to_string(),
                agent_identity: "Crab".to_string(),
                owner_identity: "Henry".to_string(),
                primary_goals: vec![
                    "Keep code quality high".to_string(),
                    "Ship fast".to_string(),
                ],
                machine_location: "Paris, France".to_string(),
                machine_timezone: "Europe/Paris".to_string(),
            }
        );
    }

    #[test]
    fn capture_parser_rejects_blank_input() {
        assert_invariant_error(
            parse_onboarding_capture_document(" "),
            "raw_output must not be empty",
        );
    }

    #[test]
    fn capture_parser_rejects_invalid_json() {
        assert_invariant_error(
            parse_onboarding_capture_document("{"),
            "invalid onboarding JSON: EOF while parsing an object at line 1 column 1",
        );
    }

    #[test]
    fn capture_parser_rejects_unknown_fields() {
        assert_invariant_error(
            parse_onboarding_capture_document(
                r#"{
                    "schema_version":"v1",
                    "agent_identity":"Crab",
                    "owner_identity":"Henry",
                    "primary_goals":["One"],
                    "machine_location":"Paris",
                    "machine_timezone":"Europe/Paris",
                    "extra":"unexpected"
                }"#,
            ),
            "invalid onboarding JSON: unknown field `extra`, expected one of `schema_version`, `agent_identity`, `owner_identity`, `primary_goals`, `machine_location`, `machine_timezone` at line 8 column 27",
        );
    }

    #[test]
    fn capture_parser_rejects_schema_version_mismatch() {
        assert_invariant_error(
            parse_onboarding_capture_document(
                r#"{
                    "schema_version":"v2",
                    "agent_identity":"Crab",
                    "owner_identity":"Henry",
                    "primary_goals":["One"],
                    "machine_location":"Paris",
                    "machine_timezone":"Europe/Paris"
                }"#,
            ),
            "schema_version must be \"v1\", got \"v2\"",
        );
    }

    #[test]
    fn capture_parser_rejects_blank_required_fields() {
        type FieldMutation = fn(&mut OnboardingCaptureDocument);
        let cases: [(&str, FieldMutation); 5] = [
            ("schema_version", |document| {
                document.schema_version = " ".to_string()
            }),
            ("agent_identity", |document| {
                document.agent_identity = "   ".to_string()
            }),
            ("owner_identity", |document| {
                document.owner_identity = " ".to_string()
            }),
            ("machine_location", |document| {
                document.machine_location = "   ".to_string()
            }),
            ("machine_timezone", |document| {
                document.machine_timezone = " ".to_string()
            }),
        ];

        for (field_key, apply_mutation) in cases {
            let mut document = valid_capture_document();
            apply_mutation(&mut document);
            let raw = serde_json::to_string(&document).expect("document should serialize");
            let expected = format!("{field_key} must not be empty");
            assert_invariant_error(parse_onboarding_capture_document(raw.as_str()), &expected);
        }
    }

    #[test]
    fn capture_parser_rejects_empty_or_blank_goals() {
        assert_invariant_error(
            parse_onboarding_capture_document(
                r#"{
                    "schema_version":"v1",
                    "agent_identity":"Crab",
                    "owner_identity":"Henry",
                    "primary_goals":[],
                    "machine_location":"Paris",
                    "machine_timezone":"Europe/Paris"
                }"#,
            ),
            "primary_goals must contain at least one goal",
        );

        assert_invariant_error(
            parse_onboarding_capture_document(
                r#"{
                    "schema_version":"v1",
                    "agent_identity":"Crab",
                    "owner_identity":"Henry",
                    "primary_goals":["valid","   "],
                    "machine_location":"Paris",
                    "machine_timezone":"Europe/Paris"
                }"#,
            ),
            "primary_goals[] must not be empty",
        );
    }
}
