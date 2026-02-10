use crate::context_assembly::{
    assemble_turn_context, sort_memory_snippets, ContextAssemblyInput, ContextMemorySnippet,
};
use crate::{CrabError, CrabResult};

const CONTEXT_BUDGET_CONTEXT: &str = "context_budget";
pub const TOTAL_CONTEXT_TRUNCATION_MARKER: &str = "[CONTEXT_TRUNCATED]";
pub const MEMORY_SNIPPET_DROP_MARKER_PATH: &str = "memory/_context_budget.md";

pub const DEFAULT_CONTEXT_MAX_TOTAL_CHARS: usize = 16_000;
pub const DEFAULT_CONTEXT_MAX_SECTION_CHARS: usize = 2_000;
pub const DEFAULT_CONTEXT_MAX_TURN_INPUT_CHARS: usize = 3_000;
pub const DEFAULT_CONTEXT_MAX_MEMORY_SNIPPET_CHARS: usize = 600;
pub const DEFAULT_CONTEXT_MAX_MEMORY_SNIPPET_COUNT: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextBudgetPolicy {
    pub max_total_chars: usize,
    pub max_section_chars: usize,
    pub max_turn_input_chars: usize,
    pub max_memory_snippet_chars: usize,
    pub max_memory_snippet_count: usize,
}

impl Default for ContextBudgetPolicy {
    fn default() -> Self {
        Self {
            max_total_chars: DEFAULT_CONTEXT_MAX_TOTAL_CHARS,
            max_section_chars: DEFAULT_CONTEXT_MAX_SECTION_CHARS,
            max_turn_input_chars: DEFAULT_CONTEXT_MAX_TURN_INPUT_CHARS,
            max_memory_snippet_chars: DEFAULT_CONTEXT_MAX_MEMORY_SNIPPET_CHARS,
            max_memory_snippet_count: DEFAULT_CONTEXT_MAX_MEMORY_SNIPPET_COUNT,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextBudgetReport {
    pub truncated_sections: Vec<String>,
    pub truncated_memory_snippet_paths: Vec<String>,
    pub dropped_memory_snippet_count: usize,
    pub total_truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BudgetedContextOutput {
    pub rendered_context: String,
    pub budgeted_input: ContextAssemblyInput,
    pub report: ContextBudgetReport,
}

pub fn render_budgeted_turn_context(
    input: &ContextAssemblyInput,
    policy: &ContextBudgetPolicy,
) -> CrabResult<BudgetedContextOutput> {
    validate_policy(policy)?;

    let mut truncated_sections = Vec::new();
    let mut budgeted_input = input.clone();

    budgeted_input.soul_document = apply_text_budget(
        &budgeted_input.soul_document,
        policy.max_section_chars,
        "SOUL.md",
        &mut truncated_sections,
    );
    budgeted_input.identity_document = apply_text_budget(
        &budgeted_input.identity_document,
        policy.max_section_chars,
        "IDENTITY.md",
        &mut truncated_sections,
    );
    budgeted_input.agents_document = apply_text_budget(
        &budgeted_input.agents_document,
        policy.max_section_chars,
        "AGENTS.md",
        &mut truncated_sections,
    );
    budgeted_input.user_document = apply_text_budget(
        &budgeted_input.user_document,
        policy.max_section_chars,
        "USER.md",
        &mut truncated_sections,
    );
    budgeted_input.memory_document = apply_text_budget(
        &budgeted_input.memory_document,
        policy.max_section_chars,
        "MEMORY.md",
        &mut truncated_sections,
    );

    budgeted_input.latest_checkpoint_summary = budgeted_input
        .latest_checkpoint_summary
        .as_ref()
        .map(|value| {
            apply_text_budget(
                value,
                policy.max_section_chars,
                "LATEST_CHECKPOINT",
                &mut truncated_sections,
            )
        });

    budgeted_input.turn_input = apply_text_budget(
        &budgeted_input.turn_input,
        policy.max_turn_input_chars,
        "TURN_INPUT",
        &mut truncated_sections,
    );

    let (budgeted_snippets, truncated_memory_snippet_paths, dropped_memory_snippet_count) =
        apply_memory_snippet_budget(
            &budgeted_input.memory_snippets,
            policy.max_memory_snippet_count,
            policy.max_memory_snippet_chars,
        );
    budgeted_input.memory_snippets = budgeted_snippets;

    let rendered_context = assemble_turn_context(&budgeted_input)?;
    let (rendered_context, total_truncated) =
        apply_total_budget(&rendered_context, policy.max_total_chars);

    Ok(BudgetedContextOutput {
        rendered_context,
        budgeted_input,
        report: ContextBudgetReport {
            truncated_sections,
            truncated_memory_snippet_paths,
            dropped_memory_snippet_count,
            total_truncated,
        },
    })
}

fn validate_policy(policy: &ContextBudgetPolicy) -> CrabResult<()> {
    for (field, value) in [
        ("max_total_chars", policy.max_total_chars),
        ("max_section_chars", policy.max_section_chars),
        ("max_turn_input_chars", policy.max_turn_input_chars),
        ("max_memory_snippet_chars", policy.max_memory_snippet_chars),
        ("max_memory_snippet_count", policy.max_memory_snippet_count),
    ] {
        if value == 0 {
            return Err(CrabError::InvariantViolation {
                context: CONTEXT_BUDGET_CONTEXT,
                message: format!("{field} must be greater than 0"),
            });
        }
    }

    Ok(())
}

fn apply_text_budget(
    value: &str,
    max_chars: usize,
    label: &str,
    truncated_sections: &mut Vec<String>,
) -> String {
    let char_count = value.chars().count();
    if char_count <= max_chars {
        return value.to_string();
    }

    truncated_sections.push(label.to_string());
    let marker = format!("\n[TRUNCATED:{label}: kept {max_chars} of {char_count} chars]");
    truncate_with_marker(value, max_chars, &marker)
}

fn apply_memory_snippet_budget(
    snippets: &[ContextMemorySnippet],
    max_snippet_count: usize,
    max_snippet_chars: usize,
) -> (Vec<ContextMemorySnippet>, Vec<String>, usize) {
    let mut normalized = snippets.to_vec();
    sort_memory_snippets(&mut normalized);

    let mut truncated_paths = Vec::new();
    for snippet in &mut normalized {
        let char_count = snippet.content.chars().count();
        if char_count > max_snippet_chars {
            let marker = format!(
                "\n[TRUNCATED:MEMORY_SNIPPET:{}: kept {} of {} chars]",
                snippet.path, max_snippet_chars, char_count
            );
            snippet.content = truncate_with_marker(&snippet.content, max_snippet_chars, &marker);
            truncated_paths.push(snippet.path.clone());
        }
    }

    if normalized.len() <= max_snippet_count {
        return (normalized, truncated_paths, 0);
    }

    let keep_count = max_snippet_count.saturating_sub(1);
    let dropped_count = normalized.len().saturating_sub(keep_count);
    normalized.truncate(keep_count);
    normalized.push(ContextMemorySnippet {
        path: MEMORY_SNIPPET_DROP_MARKER_PATH.to_string(),
        start_line: 1,
        end_line: 1,
        content: format!(
            "[TRUNCATED_SNIPPETS: dropped {dropped_count} snippets due max_memory_snippet_count={max_snippet_count}]"
        ),
    });

    (normalized, truncated_paths, dropped_count)
}

fn apply_total_budget(value: &str, max_chars: usize) -> (String, bool) {
    let char_count = value.chars().count();
    if char_count <= max_chars {
        return (value.to_string(), false);
    }

    let marker =
        format!("\n{TOTAL_CONTEXT_TRUNCATION_MARKER} kept {max_chars} of {char_count} chars");
    (truncate_with_marker(value, max_chars, &marker), true)
}

fn truncate_with_marker(value: &str, max_chars: usize, marker: &str) -> String {
    let marker_chars = marker.chars().count();
    if max_chars <= marker_chars {
        return marker.chars().take(max_chars).collect();
    }

    let keep_chars = max_chars - marker_chars;
    let prefix: String = value.chars().take(keep_chars).collect();
    format!("{prefix}{marker}")
}

#[cfg(test)]
mod tests {
    use crate::context_assembly::{ContextAssemblyInput, ContextMemorySnippet};
    use crate::CrabError;

    use super::{
        render_budgeted_turn_context, BudgetedContextOutput, ContextBudgetPolicy,
        MEMORY_SNIPPET_DROP_MARKER_PATH, TOTAL_CONTEXT_TRUNCATION_MARKER,
    };

    fn sample_input() -> ContextAssemblyInput {
        ContextAssemblyInput {
            soul_document: "soul".to_string(),
            identity_document: "identity".to_string(),
            agents_document: "agents".to_string(),
            user_document: "user".to_string(),
            memory_document: "memory".to_string(),
            memory_snippets: vec![],
            latest_checkpoint_summary: Some("checkpoint".to_string()),
            turn_input: "turn".to_string(),
        }
    }

    fn generous_policy() -> ContextBudgetPolicy {
        ContextBudgetPolicy {
            max_total_chars: 10_000,
            max_section_chars: 200,
            max_turn_input_chars: 200,
            max_memory_snippet_chars: 200,
            max_memory_snippet_count: 8,
        }
    }

    fn render(input: &ContextAssemblyInput, policy: &ContextBudgetPolicy) -> BudgetedContextOutput {
        render_budgeted_turn_context(input, policy).expect("budgeting should succeed")
    }

    #[test]
    fn default_policy_constants_are_stable() {
        let default_policy = ContextBudgetPolicy::default();
        assert_eq!(default_policy.max_total_chars, 16_000);
        assert_eq!(default_policy.max_section_chars, 2_000);
        assert_eq!(default_policy.max_turn_input_chars, 3_000);
        assert_eq!(default_policy.max_memory_snippet_chars, 600);
        assert_eq!(default_policy.max_memory_snippet_count, 8);
    }

    #[test]
    fn leaves_context_unchanged_when_within_budget() {
        let input = sample_input();
        let output = render(&input, &generous_policy());

        assert!(output.report.truncated_sections.is_empty());
        assert!(output.report.truncated_memory_snippet_paths.is_empty());
        assert_eq!(output.report.dropped_memory_snippet_count, 0);
        assert!(!output.report.total_truncated);
        assert!(output.rendered_context.contains("## SOUL.md\nsoul"));
        assert!(output.rendered_context.contains("## TURN_INPUT\nturn"));
    }

    #[test]
    fn truncates_long_sections_and_turn_input_with_explicit_markers() {
        let mut input = sample_input();
        input.soul_document = "a".repeat(120);
        input.turn_input = "b".repeat(120);

        let mut policy = generous_policy();
        policy.max_section_chars = 80;
        policy.max_turn_input_chars = 90;

        let output = render(&input, &policy);
        assert!(output
            .report
            .truncated_sections
            .contains(&"SOUL.md".to_string()));
        assert!(output
            .report
            .truncated_sections
            .contains(&"TURN_INPUT".to_string()));
        assert!(output
            .rendered_context
            .contains("[TRUNCATED:SOUL.md: kept 80 of 120 chars]"));
        assert!(output
            .rendered_context
            .contains("[TRUNCATED:TURN_INPUT: kept 90 of 120 chars]"));
    }

    #[test]
    fn truncates_checkpoint_summary_when_present() {
        let mut input = sample_input();
        input.latest_checkpoint_summary = Some("c".repeat(140));

        let mut policy = generous_policy();
        policy.max_section_chars = 100;

        let output = render(&input, &policy);
        assert!(output
            .report
            .truncated_sections
            .contains(&"LATEST_CHECKPOINT".to_string()));
        assert!(output
            .rendered_context
            .contains("[TRUNCATED:LATEST_CHECKPOINT: kept 100 of 140 chars]"));
    }

    #[test]
    fn truncates_memory_snippet_content_and_reports_paths() {
        let mut input = sample_input();
        input.memory_snippets = vec![ContextMemorySnippet {
            path: "memory/users/42/2026-02-10.md".to_string(),
            start_line: 1,
            end_line: 1,
            content: "x".repeat(180),
        }];

        let mut policy = generous_policy();
        policy.max_memory_snippet_chars = 120;

        let output = render(&input, &policy);
        assert_eq!(
            output.report.truncated_memory_snippet_paths,
            vec!["memory/users/42/2026-02-10.md".to_string()]
        );
        assert!(output.rendered_context.contains(
            "[TRUNCATED:MEMORY_SNIPPET:memory/users/42/2026-02-10.md: kept 120 of 180 chars]"
        ));
    }

    #[test]
    fn caps_memory_snippet_count_and_adds_drop_marker() {
        let mut input = sample_input();
        input.memory_snippets = vec![
            ContextMemorySnippet {
                path: "memory/users/42/2026-02-08.md".to_string(),
                start_line: 1,
                end_line: 1,
                content: "a".to_string(),
            },
            ContextMemorySnippet {
                path: "memory/users/42/2026-02-09.md".to_string(),
                start_line: 1,
                end_line: 1,
                content: "b".to_string(),
            },
            ContextMemorySnippet {
                path: "memory/users/42/2026-02-10.md".to_string(),
                start_line: 1,
                end_line: 1,
                content: "c".to_string(),
            },
        ];

        let mut policy = generous_policy();
        policy.max_memory_snippet_count = 2;

        let output = render(&input, &policy);
        assert_eq!(output.report.dropped_memory_snippet_count, 2);
        assert!(output
            .rendered_context
            .contains(MEMORY_SNIPPET_DROP_MARKER_PATH));
        assert!(output
            .rendered_context
            .contains("[TRUNCATED_SNIPPETS: dropped 2 snippets due max_memory_snippet_count=2]"));
    }

    #[test]
    fn applies_total_context_budget_with_explicit_marker() {
        let mut input = sample_input();
        input.agents_document =
            "A very long section that pushes total output over budget.".to_string();

        let mut policy = generous_policy();
        policy.max_total_chars = 60;

        let output = render(&input, &policy);
        assert!(output.report.total_truncated);
        assert!(output
            .rendered_context
            .contains(TOTAL_CONTEXT_TRUNCATION_MARKER));
        assert_eq!(output.rendered_context.chars().count(), 60);
    }

    #[test]
    fn tiny_budget_still_renders_marker_deterministically() {
        let mut input = sample_input();
        input.soul_document = "abcdef".to_string();

        let mut policy = generous_policy();
        policy.max_section_chars = 3;

        let output = render(&input, &policy);
        assert!(output
            .report
            .truncated_sections
            .contains(&"SOUL.md".to_string()));
        let soul_section = output
            .rendered_context
            .split("## IDENTITY.md")
            .next()
            .expect("soul section should exist");
        assert!(soul_section.contains('['));
    }

    #[test]
    fn policy_validation_rejects_zero_values() {
        let assert_zero_field_error = |field: &str, apply_zero: fn(&mut ContextBudgetPolicy)| {
            let mut policy = generous_policy();
            apply_zero(&mut policy);

            let error = render_budgeted_turn_context(&sample_input(), &policy)
                .expect_err("zero policy values should fail");
            assert_eq!(
                error,
                CrabError::InvariantViolation {
                    context: "context_budget",
                    message: format!("{field} must be greater than 0"),
                }
            );
        };

        assert_zero_field_error("max_total_chars", |policy| policy.max_total_chars = 0);
        assert_zero_field_error("max_section_chars", |policy| policy.max_section_chars = 0);
        assert_zero_field_error("max_turn_input_chars", |policy| {
            policy.max_turn_input_chars = 0
        });
        assert_zero_field_error("max_memory_snippet_chars", |policy| {
            policy.max_memory_snippet_chars = 0
        });
        assert_zero_field_error("max_memory_snippet_count", |policy| {
            policy.max_memory_snippet_count = 0
        });
    }

    #[test]
    fn propagates_context_assembly_validation_errors() {
        let mut input = sample_input();
        input.memory_snippets = vec![ContextMemorySnippet {
            path: " ".to_string(),
            start_line: 1,
            end_line: 1,
            content: "snippet".to_string(),
        }];

        let error = render_budgeted_turn_context(&input, &generous_policy())
            .expect_err("invalid snippet should fail context assembly");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "context_assembly",
                message: "memory_snippets[].path must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn output_is_deterministic_for_same_input_and_policy() {
        let mut input = sample_input();
        input.memory_snippets = vec![
            ContextMemorySnippet {
                path: "memory/users/42/2026-02-10.md".to_string(),
                start_line: 1,
                end_line: 1,
                content: "snippet-A".to_string(),
            },
            ContextMemorySnippet {
                path: "memory/global/2026-02-10.md".to_string(),
                start_line: 1,
                end_line: 1,
                content: "snippet-B".to_string(),
            },
        ];

        let mut policy = generous_policy();
        policy.max_memory_snippet_chars = 6;
        policy.max_total_chars = 500;

        let first = render(&input, &policy);
        let second = render(&input, &policy);
        assert_eq!(first, second);
    }
}
