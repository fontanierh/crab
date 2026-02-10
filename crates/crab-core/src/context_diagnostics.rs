use std::collections::BTreeSet;

use crate::context_budget::{BudgetedContextOutput, MEMORY_SNIPPET_DROP_MARKER_PATH};

pub const CONTEXT_DIAGNOSTICS_FIXTURE_HEADER: &str = "CONTEXT_DIAGNOSTICS_V1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextDiagnosticsFileEntry {
    pub file: String,
    pub chars: usize,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextDiagnosticsSnippetEntry {
    pub path: String,
    pub start_line: u32,
    pub end_line: u32,
    pub chars: usize,
    pub truncated: bool,
    pub drop_marker: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextDiagnosticsReport {
    pub injected_files: Vec<ContextDiagnosticsFileEntry>,
    pub latest_checkpoint_chars: usize,
    pub latest_checkpoint_truncated: bool,
    pub turn_input_chars: usize,
    pub turn_input_truncated: bool,
    pub memory_snippets: Vec<ContextDiagnosticsSnippetEntry>,
    pub dropped_memory_snippet_count: usize,
    pub truncated_sections: Vec<String>,
    pub truncated_memory_snippet_paths: Vec<String>,
    pub rendered_context_chars: usize,
    pub total_truncated: bool,
}

pub fn build_context_diagnostics_report(
    output: &BudgetedContextOutput,
) -> ContextDiagnosticsReport {
    let truncated_sections: BTreeSet<&str> = output
        .report
        .truncated_sections
        .iter()
        .map(String::as_str)
        .collect();
    let truncated_memory_paths: BTreeSet<&str> = output
        .report
        .truncated_memory_snippet_paths
        .iter()
        .map(String::as_str)
        .collect();

    let injected_files = vec![
        file_entry(
            "SOUL.md",
            &output.budgeted_input.soul_document,
            &truncated_sections,
        ),
        file_entry(
            "IDENTITY.md",
            &output.budgeted_input.identity_document,
            &truncated_sections,
        ),
        file_entry(
            "AGENTS.md",
            &output.budgeted_input.agents_document,
            &truncated_sections,
        ),
        file_entry(
            "USER.md",
            &output.budgeted_input.user_document,
            &truncated_sections,
        ),
        file_entry(
            "MEMORY.md",
            &output.budgeted_input.memory_document,
            &truncated_sections,
        ),
    ];

    let memory_snippets = output
        .budgeted_input
        .memory_snippets
        .iter()
        .map(|snippet| ContextDiagnosticsSnippetEntry {
            path: snippet.path.clone(),
            start_line: snippet.start_line,
            end_line: snippet.end_line,
            chars: text_char_count(&snippet.content),
            truncated: truncated_memory_paths.contains(snippet.path.as_str()),
            drop_marker: snippet.path == MEMORY_SNIPPET_DROP_MARKER_PATH,
        })
        .collect();

    ContextDiagnosticsReport {
        injected_files,
        latest_checkpoint_chars: output
            .budgeted_input
            .latest_checkpoint_summary
            .as_deref()
            .map(text_char_count)
            .unwrap_or(0),
        latest_checkpoint_truncated: truncated_sections.contains("LATEST_CHECKPOINT"),
        turn_input_chars: text_char_count(&output.budgeted_input.turn_input),
        turn_input_truncated: truncated_sections.contains("TURN_INPUT"),
        memory_snippets,
        dropped_memory_snippet_count: output.report.dropped_memory_snippet_count,
        truncated_sections: output.report.truncated_sections.clone(),
        truncated_memory_snippet_paths: output.report.truncated_memory_snippet_paths.clone(),
        rendered_context_chars: text_char_count(&output.rendered_context),
        total_truncated: output.report.total_truncated,
    }
}

pub fn render_context_diagnostics_fixture(report: &ContextDiagnosticsReport) -> String {
    let mut lines = vec![
        CONTEXT_DIAGNOSTICS_FIXTURE_HEADER.to_string(),
        format!("rendered_context_chars={}", report.rendered_context_chars),
        format!("total_truncated={}", report.total_truncated),
        format!(
            "latest_checkpoint chars={} truncated={}",
            report.latest_checkpoint_chars, report.latest_checkpoint_truncated
        ),
        format!(
            "turn_input chars={} truncated={}",
            report.turn_input_chars, report.turn_input_truncated
        ),
        format!(
            "dropped_memory_snippet_count={}",
            report.dropped_memory_snippet_count
        ),
        format!(
            "truncated_sections={}",
            render_tokens(&report.truncated_sections)
        ),
        format!(
            "truncated_memory_snippet_paths={}",
            render_tokens(&report.truncated_memory_snippet_paths)
        ),
        "injected_files:".to_string(),
    ];

    for file in &report.injected_files {
        lines.push(format!(
            "- {} chars={} truncated={}",
            file.file, file.chars, file.truncated
        ));
    }

    lines.push("memory_snippets:".to_string());
    if report.memory_snippets.is_empty() {
        lines.push("- (none)".to_string());
    } else {
        for snippet in &report.memory_snippets {
            lines.push(format!(
                "- {}:{}-{} chars={} truncated={} drop_marker={}",
                snippet.path,
                snippet.start_line,
                snippet.end_line,
                snippet.chars,
                snippet.truncated,
                snippet.drop_marker
            ));
        }
    }

    lines.join("\n")
}

fn file_entry(
    name: &str,
    value: &str,
    truncated_sections: &BTreeSet<&str>,
) -> ContextDiagnosticsFileEntry {
    ContextDiagnosticsFileEntry {
        file: name.to_string(),
        chars: text_char_count(value),
        truncated: truncated_sections.contains(name),
    }
}

fn text_char_count(value: &str) -> usize {
    value.chars().count()
}

fn render_tokens(values: &[String]) -> String {
    if values.is_empty() {
        return "(none)".to_string();
    }

    values.join(",")
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
        let seeds = [
            "soul",
            "identity",
            "agents",
            "user",
            "memory",
            "checkpoint",
            "turn",
        ];
        ContextAssemblyInput {
            soul_document: format!("{}-section", seeds[0]),
            identity_document: format!("{}-section", seeds[1]),
            agents_document: format!("{}-section", seeds[2]),
            user_document: format!("{}-section", seeds[3]),
            memory_document: format!("{}-section", seeds[4]),
            memory_snippets: vec![],
            latest_checkpoint_summary: Some(format!("{}-summary", seeds[5])),
            turn_input: format!("{}-input", seeds[6]),
        }
    }

    fn roomy_policy() -> ContextBudgetPolicy {
        ContextBudgetPolicy {
            max_total_chars: 12_000,
            max_section_chars: 2_500,
            max_turn_input_chars: 2_500,
            max_memory_snippet_chars: 2_500,
            ..ContextBudgetPolicy::default()
        }
    }

    #[test]
    fn report_tracks_sizes_and_truncation_decisions() {
        let mut input = base_input();
        input.soul_document = "s".repeat(70);
        input.turn_input = "t".repeat(80);
        input.latest_checkpoint_summary = Some("c".repeat(60));
        input.memory_snippets = vec![
            ContextMemorySnippet {
                path: "memory/users/42/2026-02-08.md".to_string(),
                start_line: 1,
                end_line: 1,
                content: "x".repeat(50),
            },
            ContextMemorySnippet {
                path: "memory/users/42/2026-02-09.md".to_string(),
                start_line: 1,
                end_line: 1,
                content: "short".to_string(),
            },
            ContextMemorySnippet {
                path: "memory/users/42/2026-02-10.md".to_string(),
                start_line: 1,
                end_line: 1,
                content: "shorter".to_string(),
            },
        ];

        let mut policy = roomy_policy();
        policy.max_section_chars = 40;
        policy.max_turn_input_chars = 30;
        policy.max_memory_snippet_chars = 20;
        policy.max_memory_snippet_count = 2;

        let output =
            render_budgeted_turn_context(&input, &policy).expect("context budgeting should work");
        let report = build_context_diagnostics_report(&output);

        assert_eq!(report.injected_files.len(), 5);
        assert!(report
            .injected_files
            .iter()
            .any(|entry| entry.file == "SOUL.md" && entry.truncated));
        assert!(report.latest_checkpoint_truncated);
        assert!(report.turn_input_truncated);

        assert_eq!(report.dropped_memory_snippet_count, 2);
        assert_eq!(report.memory_snippets.len(), 2);
        assert!(report
            .memory_snippets
            .iter()
            .any(|entry| { entry.path == "memory/users/42/2026-02-08.md" && entry.truncated }));
        assert!(report.memory_snippets.iter().any(|entry| entry.drop_marker));

        assert_eq!(
            report.rendered_context_chars,
            output.rendered_context.chars().count()
        );
        assert_eq!(report.truncated_sections, output.report.truncated_sections);
        assert_eq!(
            report.truncated_memory_snippet_paths,
            output.report.truncated_memory_snippet_paths
        );
        assert_eq!(report.total_truncated, output.report.total_truncated);

        let fixture = render_context_diagnostics_fixture(&report);
        assert!(fixture.contains("truncated_sections=SOUL.md,LATEST_CHECKPOINT,TURN_INPUT"));
        assert!(fixture.contains("truncated_memory_snippet_paths=memory/users/42/2026-02-08.md"));
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
        assert!(first.contains("truncated_sections=(none)"));
        assert!(first.contains("truncated_memory_snippet_paths=(none)"));
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
