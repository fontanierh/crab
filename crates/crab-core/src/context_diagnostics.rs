use std::collections::BTreeMap;

use crate::context_budget::{
    estimate_token_count, BudgetedContextOutput, ContextSectionTokenUsage, ContextSnippetTokenUsage,
};

pub const CONTEXT_DIAGNOSTICS_FIXTURE_HEADER: &str = "CONTEXT_DIAGNOSTICS_V2";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextDiagnosticsFileEntry {
    pub file: String,
    pub estimated_tokens: usize,
    pub budget_tokens: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextDiagnosticsSnippetEntry {
    pub path: String,
    pub start_line: u32,
    pub end_line: u32,
    pub estimated_tokens: usize,
    pub budget_tokens: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextDiagnosticsReport {
    pub injected_files: Vec<ContextDiagnosticsFileEntry>,
    pub latest_checkpoint_tokens: usize,
    pub latest_checkpoint_budget_tokens: usize,
    pub turn_input_tokens: usize,
    pub turn_input_budget_tokens: usize,
    pub memory_snippets: Vec<ContextDiagnosticsSnippetEntry>,
    pub rendered_context_chars: usize,
    pub rendered_context_tokens: usize,
}

pub fn build_context_diagnostics_report(
    output: &BudgetedContextOutput,
) -> ContextDiagnosticsReport {
    let section_usage_map = section_usage_by_name(&output.report.section_usage);
    let snippet_usage_map = snippet_usage_by_path(&output.report.snippet_usage);

    let injected_files = vec![
        file_entry("SOUL.md", &section_usage_map),
        file_entry("IDENTITY.md", &section_usage_map),
        file_entry("USER.md", &section_usage_map),
        file_entry("MEMORY.md", &section_usage_map),
        file_entry("PROMPT_CONTRACT", &section_usage_map),
    ];

    let latest_checkpoint_tokens = section_usage_map
        .get("LATEST_CHECKPOINT")
        .map(|usage| usage.estimated_tokens)
        .unwrap_or(0);
    let latest_checkpoint_budget_tokens = section_usage_map
        .get("LATEST_CHECKPOINT")
        .map(|usage| usage.budget_tokens)
        .unwrap_or(0);

    let turn_input_tokens = section_usage_map
        .get("TURN_INPUT")
        .map(|usage| usage.estimated_tokens)
        .unwrap_or(0);
    let turn_input_budget_tokens = section_usage_map
        .get("TURN_INPUT")
        .map(|usage| usage.budget_tokens)
        .unwrap_or(0);

    let memory_snippets = output
        .budgeted_input
        .memory_snippets
        .iter()
        .map(|snippet| {
            let usage = snippet_usage_map
                .get(snippet.path.as_str())
                .expect("snippet usage should exist for each injected snippet");
            ContextDiagnosticsSnippetEntry {
                path: snippet.path.clone(),
                start_line: snippet.start_line,
                end_line: snippet.end_line,
                estimated_tokens: usage.estimated_tokens,
                budget_tokens: usage.budget_tokens,
            }
        })
        .collect();

    ContextDiagnosticsReport {
        injected_files,
        latest_checkpoint_tokens,
        latest_checkpoint_budget_tokens,
        turn_input_tokens,
        turn_input_budget_tokens,
        memory_snippets,
        rendered_context_chars: output.rendered_context.chars().count(),
        rendered_context_tokens: estimate_token_count(&output.rendered_context),
    }
}

pub fn render_context_diagnostics_fixture(report: &ContextDiagnosticsReport) -> String {
    let mut lines = vec![
        CONTEXT_DIAGNOSTICS_FIXTURE_HEADER.to_string(),
        format!(
            "rendered_context chars={} tokens={}",
            report.rendered_context_chars, report.rendered_context_tokens
        ),
        format!(
            "latest_checkpoint tokens={} budget_tokens={}",
            report.latest_checkpoint_tokens, report.latest_checkpoint_budget_tokens
        ),
        format!(
            "turn_input tokens={} budget_tokens={}",
            report.turn_input_tokens, report.turn_input_budget_tokens
        ),
        "injected_files:".to_string(),
    ];

    for file in &report.injected_files {
        lines.push(format!(
            "- {} tokens={} budget_tokens={}",
            file.file, file.estimated_tokens, file.budget_tokens
        ));
    }

    lines.push("memory_snippets:".to_string());
    if report.memory_snippets.is_empty() {
        lines.push("- (none)".to_string());
    } else {
        for snippet in &report.memory_snippets {
            lines.push(format!(
                "- {}:{}-{} tokens={} budget_tokens={}",
                snippet.path,
                snippet.start_line,
                snippet.end_line,
                snippet.estimated_tokens,
                snippet.budget_tokens
            ));
        }
    }

    lines.join("\n")
}

fn section_usage_by_name(
    section_usage: &[ContextSectionTokenUsage],
) -> BTreeMap<&str, &ContextSectionTokenUsage> {
    section_usage
        .iter()
        .map(|usage| (usage.section.as_str(), usage))
        .collect()
}

fn snippet_usage_by_path(
    snippet_usage: &[ContextSnippetTokenUsage],
) -> BTreeMap<&str, &ContextSnippetTokenUsage> {
    snippet_usage
        .iter()
        .map(|usage| (usage.path.as_str(), usage))
        .collect()
}

fn file_entry(
    name: &str,
    section_usage: &BTreeMap<&str, &ContextSectionTokenUsage>,
) -> ContextDiagnosticsFileEntry {
    let usage = section_usage
        .get(name)
        .expect("required injected section usage should exist");
    ContextDiagnosticsFileEntry {
        file: name.to_string(),
        estimated_tokens: usage.estimated_tokens,
        budget_tokens: usage.budget_tokens,
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        render_budgeted_turn_context, ContextAssemblyInput, ContextBudgetPolicy,
        ContextMemorySnippet,
    };

    use super::{
        build_context_diagnostics_report, render_context_diagnostics_fixture,
        CONTEXT_DIAGNOSTICS_FIXTURE_HEADER,
    };

    fn base_input() -> ContextAssemblyInput {
        ContextAssemblyInput {
            soul_document: "soul section".to_string(),
            identity_document: "identity section".to_string(),
            user_document: "user section".to_string(),
            memory_document: "memory section".to_string(),
            memory_snippets: vec![],
            latest_checkpoint_summary: Some("checkpoint summary".to_string()),
            prompt_contract: "prompt contract".to_string(),
            turn_input: "turn input".to_string(),
        }
    }

    fn roomy_policy() -> ContextBudgetPolicy {
        ContextBudgetPolicy {
            max_soul_tokens: 2_500,
            max_identity_tokens: 2_500,
            max_user_tokens: 2_500,
            max_memory_tokens: 2_500,
            max_prompt_contract_tokens: 2_500,
            max_latest_checkpoint_tokens: 2_500,
            max_turn_input_tokens: 2_500,
            max_memory_snippet_tokens: 2_500,
            ..ContextBudgetPolicy::default()
        }
    }

    #[test]
    fn report_tracks_token_usage_for_sections_and_snippets() {
        let mut input = base_input();
        input.memory_snippets = vec![
            ContextMemorySnippet {
                path: "memory/users/42/2026-02-09.md".to_string(),
                start_line: 1,
                end_line: 1,
                content: "alpha beta".to_string(),
            },
            ContextMemorySnippet {
                path: "memory/users/42/2026-02-10.md".to_string(),
                start_line: 2,
                end_line: 2,
                content: "gamma".to_string(),
            },
        ];

        let output = render_budgeted_turn_context(&input, &roomy_policy())
            .expect("context budgeting should work");
        let report = build_context_diagnostics_report(&output);

        assert_eq!(report.injected_files.len(), 5);
        assert!(report
            .injected_files
            .iter()
            .any(|entry| entry.file == "PROMPT_CONTRACT"));
        assert_eq!(report.memory_snippets.len(), 2);
        assert!(report.rendered_context_chars > 0);
        assert!(report.rendered_context_tokens > 0);

        let fixture = render_context_diagnostics_fixture(&report);
        assert!(fixture.contains("rendered_context chars="));
        assert!(fixture.contains("latest_checkpoint tokens="));
        assert!(fixture.contains("turn_input tokens="));
    }

    #[test]
    fn fixture_render_is_deterministic_and_structured() {
        let mut input = base_input();
        input.latest_checkpoint_summary = None;
        input.memory_snippets = vec![ContextMemorySnippet {
            path: "memory/users/42/2026-02-10.md".to_string(),
            start_line: 3,
            end_line: 4,
            content: "note".to_string(),
        }];

        let output =
            render_budgeted_turn_context(&input, &roomy_policy()).expect("budgeting should work");
        let report = build_context_diagnostics_report(&output);

        let first = render_context_diagnostics_fixture(&report);
        let second = render_context_diagnostics_fixture(&report);

        assert_eq!(first, second);
        assert!(first.starts_with(CONTEXT_DIAGNOSTICS_FIXTURE_HEADER));
        assert!(first.contains("injected_files:"));
        assert!(first.contains("memory_snippets:"));
        assert!(first.contains("memory/users/42/2026-02-10.md:3-4"));
    }

    #[test]
    fn fixture_render_marks_empty_memory_snippets_explicitly() {
        let output = render_budgeted_turn_context(&base_input(), &roomy_policy())
            .expect("budgeting should work");
        let report = build_context_diagnostics_report(&output);
        let fixture = render_context_diagnostics_fixture(&report);

        assert!(fixture.contains("memory_snippets:\n- (none)"));
    }
}
