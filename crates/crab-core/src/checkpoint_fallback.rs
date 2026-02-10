use std::collections::BTreeMap;

use crate::checkpoint_turn::{CheckpointTurnArtifact, CheckpointTurnDocument};
use crate::{validation::validate_non_empty_text, CrabError, CrabResult};

pub const DEFAULT_FALLBACK_TRANSCRIPT_TAIL_LIMIT: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TranscriptEntryRole {
    User,
    Assistant,
    System,
    Tool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptEntry {
    pub role: TranscriptEntryRole,
    pub text: String,
}

pub fn build_fallback_checkpoint_document(
    transcript: &[TranscriptEntry],
    durable_metadata: &BTreeMap<String, String>,
    transcript_tail_limit: usize,
) -> CrabResult<CheckpointTurnDocument> {
    if transcript_tail_limit == 0 {
        return Err(CrabError::InvalidConfig {
            key: "CRAB_FALLBACK_TRANSCRIPT_TAIL_LIMIT",
            value: transcript_tail_limit.to_string(),
            reason: "must be greater than 0",
        });
    }

    let normalized_transcript = normalize_transcript(transcript);
    let tail = transcript_tail(&normalized_transcript, transcript_tail_limit);
    let metadata_artifacts = metadata_artifacts(durable_metadata)?;

    let summary = if tail.is_empty() {
        "Fallback checkpoint generated without transcript content.".to_string()
    } else {
        let tail_rendered = tail
            .iter()
            .map(|entry| {
                format!(
                    "{}: {}",
                    role_prefix(entry.role),
                    truncate_with_ellipsis(&entry.text, 80)
                )
            })
            .collect::<Vec<_>>()
            .join(" | ");
        truncate_with_ellipsis(
            &format!(
                "Fallback checkpoint generated from transcript tail ({} entries): {}",
                tail.len(),
                tail_rendered
            ),
            512,
        )
    };

    let mut decisions = vec![
        "Primary checkpoint turn output was invalid; deterministic fallback was used.".to_string(),
        format!("Transcript tail entries included: {}.", tail.len()),
    ];
    if !metadata_artifacts.is_empty() {
        decisions.push(format!(
            "Durable metadata entries included: {}.",
            metadata_artifacts.len()
        ));
    }

    let mut open_questions = Vec::new();
    for entry in &tail {
        if !entry.text.contains('?') {
            continue;
        }
        let candidate = format!(
            "{}: {}",
            role_prefix(entry.role),
            truncate_with_ellipsis(&entry.text, 120)
        );
        if !open_questions.contains(&candidate) {
            open_questions.push(candidate);
        }
    }
    if open_questions.is_empty() {
        open_questions.push("No explicit open questions found in transcript tail.".to_string());
    }

    let mut next_actions = vec![
        "Resume on a fresh physical session using this fallback checkpoint.".to_string(),
        "Run a hidden checkpoint turn early in the next run to replace fallback output."
            .to_string(),
    ];
    if tail.is_empty() {
        next_actions.push(
            "Capture additional transcript context before the next compaction cycle.".to_string(),
        );
    }

    let mut artifacts = vec![CheckpointTurnArtifact {
        path: "transcript/tail".to_string(),
        note: format!(
            "Fallback checkpoint used {} normalized tail entries.",
            tail.len()
        ),
    }];
    artifacts.extend(metadata_artifacts);

    Ok(CheckpointTurnDocument {
        summary,
        decisions,
        open_questions,
        next_actions,
        artifacts,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NormalizedTranscriptEntry {
    role: TranscriptEntryRole,
    text: String,
}

fn normalize_transcript(input: &[TranscriptEntry]) -> Vec<NormalizedTranscriptEntry> {
    input
        .iter()
        .filter_map(|entry| {
            let normalized = collapse_whitespace(&entry.text);
            if normalized.is_empty() {
                return None;
            }
            Some(NormalizedTranscriptEntry {
                role: entry.role,
                text: normalized,
            })
        })
        .collect()
}

fn transcript_tail(
    normalized: &[NormalizedTranscriptEntry],
    transcript_tail_limit: usize,
) -> Vec<NormalizedTranscriptEntry> {
    let tail_count = normalized.len().min(transcript_tail_limit);
    normalized[normalized.len().saturating_sub(tail_count)..].to_vec()
}

fn metadata_artifacts(
    durable_metadata: &BTreeMap<String, String>,
) -> CrabResult<Vec<CheckpointTurnArtifact>> {
    let mut artifacts = Vec::with_capacity(durable_metadata.len());
    for (key, value) in durable_metadata {
        validate_non_empty_text("fallback_checkpoint_builder", "durable_metadata key", key)?;
        validate_non_empty_text(
            "fallback_checkpoint_builder",
            &format!("durable_metadata[{key}]"),
            value,
        )?;

        artifacts.push(CheckpointTurnArtifact {
            path: format!("metadata/{key}"),
            note: truncate_with_ellipsis(&collapse_whitespace(value), 200),
        });
    }
    Ok(artifacts)
}

fn role_prefix(role: TranscriptEntryRole) -> &'static str {
    match role {
        TranscriptEntryRole::User => "user",
        TranscriptEntryRole::Assistant => "assistant",
        TranscriptEntryRole::System => "system",
        TranscriptEntryRole::Tool => "tool",
    }
}

fn collapse_whitespace(input: &str) -> String {
    input.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn truncate_with_ellipsis(input: &str, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        return input.to_string();
    }

    if max_chars <= 3 {
        return ".".repeat(max_chars);
    }

    let keep = max_chars - 3;
    let prefix = input.chars().take(keep).collect::<String>();
    format!("{prefix}...")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::CrabError;

    use super::{
        build_fallback_checkpoint_document, truncate_with_ellipsis, TranscriptEntry,
        TranscriptEntryRole, DEFAULT_FALLBACK_TRANSCRIPT_TAIL_LIMIT,
    };

    fn entry(role: TranscriptEntryRole, text: &str) -> TranscriptEntry {
        TranscriptEntry {
            role,
            text: text.to_string(),
        }
    }

    fn metadata(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    #[test]
    fn exposes_default_tail_limit_constant() {
        assert_eq!(DEFAULT_FALLBACK_TRANSCRIPT_TAIL_LIMIT, 8);
    }

    #[test]
    fn rejects_zero_tail_limit() {
        let error = build_fallback_checkpoint_document(&[], &BTreeMap::new(), 0)
            .expect_err("zero tail limit should fail");
        assert_eq!(
            error,
            CrabError::InvalidConfig {
                key: "CRAB_FALLBACK_TRANSCRIPT_TAIL_LIMIT",
                value: "0".to_string(),
                reason: "must be greater than 0",
            }
        );
    }

    #[test]
    fn builds_checkpoint_from_tail_only() {
        let transcript = vec![
            entry(TranscriptEntryRole::User, "ignored because not in tail"),
            entry(TranscriptEntryRole::Assistant, "ack"),
            entry(TranscriptEntryRole::User, "should we ship this?"),
        ];
        let checkpoint = build_fallback_checkpoint_document(&transcript, &BTreeMap::new(), 2)
            .expect("fallback checkpoint should build");

        assert!(checkpoint.summary.contains("2 entries"));
        assert!(!checkpoint.summary.contains("ignored because not in tail"));
        assert!(checkpoint.summary.contains("assistant: ack"));
        assert!(checkpoint.summary.contains("user: should we ship this?"));
    }

    #[test]
    fn normalizes_whitespace_and_drops_blank_entries() {
        let transcript = vec![
            entry(TranscriptEntryRole::User, "  one   two   "),
            entry(TranscriptEntryRole::Assistant, "   "),
            entry(TranscriptEntryRole::System, "three\n\nfour"),
        ];
        let checkpoint = build_fallback_checkpoint_document(&transcript, &BTreeMap::new(), 8)
            .expect("fallback checkpoint should build");

        assert!(checkpoint.summary.contains("user: one two"));
        assert!(checkpoint.summary.contains("system: three four"));
        assert!(!checkpoint.summary.contains("assistant:"));
    }

    #[test]
    fn extracts_question_lines_from_tail_as_open_questions() {
        let transcript = vec![
            entry(TranscriptEntryRole::User, "What backend should we use?"),
            entry(TranscriptEntryRole::Assistant, "Let's keep codex for now."),
            entry(TranscriptEntryRole::User, "What backend should we use?"),
        ];
        let checkpoint = build_fallback_checkpoint_document(&transcript, &BTreeMap::new(), 3)
            .expect("fallback checkpoint should build");

        assert_eq!(
            checkpoint.open_questions,
            vec!["user: What backend should we use?".to_string()]
        );
    }

    #[test]
    fn includes_default_open_question_note_when_none_found() {
        let transcript = vec![entry(TranscriptEntryRole::Assistant, "No question here.")];
        let checkpoint = build_fallback_checkpoint_document(&transcript, &BTreeMap::new(), 1)
            .expect("fallback checkpoint should build");
        assert_eq!(
            checkpoint.open_questions,
            vec!["No explicit open questions found in transcript tail.".to_string()]
        );
    }

    #[test]
    fn includes_sorted_metadata_artifacts() {
        let transcript = vec![entry(TranscriptEntryRole::User, "hello")];
        let checkpoint = build_fallback_checkpoint_document(
            &transcript,
            &metadata(&[("b_key", "value b"), ("a_key", "value a")]),
            1,
        )
        .expect("fallback checkpoint should build");

        assert_eq!(checkpoint.artifacts[0].path, "transcript/tail");
        assert_eq!(checkpoint.artifacts[1].path, "metadata/a_key");
        assert_eq!(checkpoint.artifacts[1].note, "value a");
        assert_eq!(checkpoint.artifacts[2].path, "metadata/b_key");
        assert_eq!(checkpoint.artifacts[2].note, "value b");
    }

    #[test]
    fn rejects_blank_metadata_key() {
        let transcript = vec![entry(TranscriptEntryRole::User, "hello")];
        let error =
            build_fallback_checkpoint_document(&transcript, &metadata(&[(" ", "value")]), 1)
                .expect_err("blank metadata key should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "fallback_checkpoint_builder",
                message: "durable_metadata key must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn rejects_blank_metadata_value() {
        let transcript = vec![entry(TranscriptEntryRole::User, "hello")];
        let error =
            build_fallback_checkpoint_document(&transcript, &metadata(&[("key", "   ")]), 1)
                .expect_err("blank metadata value should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "fallback_checkpoint_builder",
                message: "durable_metadata[key] must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn produces_stable_output_for_same_input() {
        let transcript = vec![
            entry(TranscriptEntryRole::User, "first"),
            entry(TranscriptEntryRole::Tool, "tool note?"),
            entry(TranscriptEntryRole::Assistant, "answer"),
        ];
        let durable_metadata = metadata(&[("session", "abc"), ("checkpoint", "ckpt-1")]);

        let first = build_fallback_checkpoint_document(&transcript, &durable_metadata, 2)
            .expect("first fallback checkpoint should build");
        let second = build_fallback_checkpoint_document(&transcript, &durable_metadata, 2)
            .expect("second fallback checkpoint should build");

        assert_eq!(first, second);
    }

    #[test]
    fn truncates_very_long_summary_lines() {
        let long_text = "x".repeat(400);
        let transcript = vec![
            entry(TranscriptEntryRole::User, &long_text),
            entry(TranscriptEntryRole::Assistant, &long_text),
            entry(TranscriptEntryRole::System, &long_text),
            entry(TranscriptEntryRole::Tool, &long_text),
        ];
        let checkpoint = build_fallback_checkpoint_document(&transcript, &BTreeMap::new(), 4)
            .expect("fallback checkpoint should build");

        assert!(checkpoint.summary.chars().count() <= 512);
        assert!(checkpoint.summary.ends_with("..."));
    }

    #[test]
    fn truncate_with_ellipsis_handles_tiny_limits() {
        assert_eq!(truncate_with_ellipsis("abcdef", 3), "...");
        assert_eq!(truncate_with_ellipsis("abcdef", 2), "..");
        assert_eq!(truncate_with_ellipsis("abcdef", 0), "");
    }

    #[test]
    fn empty_transcript_adds_guidance_action() {
        let checkpoint =
            build_fallback_checkpoint_document(&[], &BTreeMap::new(), 4).expect("should build");

        assert_eq!(
            checkpoint.summary,
            "Fallback checkpoint generated without transcript content."
        );
        assert!(checkpoint.next_actions.contains(
            &"Capture additional transcript context before the next compaction cycle.".to_string()
        ));
    }
}
