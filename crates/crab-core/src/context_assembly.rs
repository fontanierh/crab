use crate::validation::validate_non_empty_text;
use crate::{CrabError, CrabResult};

const CONTEXT_ASSEMBLY_CONTEXT: &str = "context_assembly";
const EMPTY_SECTION_MARKER: &str = "(empty)";
const EMPTY_SNIPPETS_MARKER: &str = "(none)";
const EMPTY_CHECKPOINT_MARKER: &str = "(none)";
const CONTEXT_ROOT_TAG: &str = "crab_turn_context";
const SYSTEM_CONTEXT_TAG: &str = "crab_system_context";
const USER_INPUT_TAG: &str = "crab_user_input";

pub const CONTEXT_INJECTION_ORDER: [&str; 9] = [
    "SOUL.md",
    "IDENTITY.md",
    "USER.md",
    "MEMORY.md",
    "MEMORY_SNIPPETS",
    "LATEST_CHECKPOINT",
    "CRAB_RUNTIME_BRIEF",
    "PROMPT_CONTRACT",
    "TURN_INPUT",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextMemorySnippet {
    pub path: String,
    pub start_line: u32,
    pub end_line: u32,
    pub content: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextAssemblyInput {
    pub soul_document: String,
    pub identity_document: String,
    pub user_document: String,
    pub memory_document: String,
    pub memory_snippets: Vec<ContextMemorySnippet>,
    pub latest_checkpoint_summary: Option<String>,
    pub crab_runtime_brief: String,
    pub prompt_contract: String,
    pub turn_input: String,
}

pub fn assemble_turn_context(input: &ContextAssemblyInput) -> CrabResult<String> {
    validate_non_empty_text(CONTEXT_ASSEMBLY_CONTEXT, "turn_input", &input.turn_input)?;
    validate_memory_snippets(&input.memory_snippets)?;

    let mut system_sections = Vec::with_capacity(CONTEXT_INJECTION_ORDER.len() - 1);
    system_sections.push(render_tagged_section("soul_md", &input.soul_document));
    system_sections.push(render_tagged_section(
        "identity_md",
        &input.identity_document,
    ));
    system_sections.push(render_tagged_section("user_md", &input.user_document));
    system_sections.push(render_tagged_section("memory_md", &input.memory_document));
    system_sections.push(render_memory_snippets_section(&input.memory_snippets));

    let checkpoint = input
        .latest_checkpoint_summary
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(EMPTY_CHECKPOINT_MARKER);
    system_sections.push(render_tagged_section("latest_checkpoint", checkpoint));
    system_sections.push(render_tagged_section(
        "crab_runtime_brief",
        &input.crab_runtime_brief,
    ));
    system_sections.push(render_tagged_section(
        "prompt_contract",
        &input.prompt_contract,
    ));
    let user_input_section = render_tagged_section("turn_input", &input.turn_input);

    Ok(render_context_envelope(
        &system_sections,
        &user_input_section,
    ))
}

fn sort_memory_snippets(snippets: &mut [ContextMemorySnippet]) {
    snippets.sort_by(|left, right| {
        (
            left.path.as_str(),
            left.start_line,
            left.end_line,
            left.content.as_str(),
        )
            .cmp(&(
                right.path.as_str(),
                right.start_line,
                right.end_line,
                right.content.as_str(),
            ))
    });
}

fn validate_memory_snippets(snippets: &[ContextMemorySnippet]) -> CrabResult<()> {
    for snippet in snippets {
        validate_non_empty_text(
            CONTEXT_ASSEMBLY_CONTEXT,
            "memory_snippets[].path",
            &snippet.path,
        )?;
        if snippet.start_line == 0 {
            return Err(CrabError::InvariantViolation {
                context: CONTEXT_ASSEMBLY_CONTEXT,
                message: "memory_snippets[].start_line must be greater than 0".to_string(),
            });
        }
        if snippet.end_line < snippet.start_line {
            return Err(CrabError::InvariantViolation {
                context: CONTEXT_ASSEMBLY_CONTEXT,
                message: "memory_snippets[].end_line must be >= start_line".to_string(),
            });
        }
        validate_non_empty_text(
            CONTEXT_ASSEMBLY_CONTEXT,
            "memory_snippets[].content",
            &snippet.content,
        )?;
    }
    Ok(())
}

fn render_memory_snippets_section(snippets: &[ContextMemorySnippet]) -> String {
    if snippets.is_empty() {
        return format!(
            "<memory_snippets>\n  <none>{EMPTY_SNIPPETS_MARKER}</none>\n</memory_snippets>"
        );
    }

    let mut normalized = snippets.to_vec();
    sort_memory_snippets(&mut normalized);

    let mut lines = Vec::with_capacity(2 + normalized.len() * 6);
    lines.push("<memory_snippets>".to_string());
    for snippet in &normalized {
        lines.push("  <snippet>".to_string());
        lines.push(format!(
            "    <path>{}</path>",
            render_cdata(snippet.path.trim())
        ));
        lines.push(format!(
            "    <start_line>{}</start_line>",
            snippet.start_line
        ));
        lines.push(format!("    <end_line>{}</end_line>", snippet.end_line));
        lines.push(format!(
            "    <content>{}</content>",
            render_cdata(&normalize_section_body(&snippet.content))
        ));
        lines.push("  </snippet>".to_string());
    }
    lines.push("</memory_snippets>".to_string());
    lines.join("\n")
}

fn render_tagged_section(tag: &str, body: &str) -> String {
    format!(
        "<{tag}>{}</{tag}>",
        render_cdata(&normalize_section_body(body))
    )
}

fn render_context_envelope(system_sections: &[String], user_input_section: &str) -> String {
    let mut lines = Vec::new();
    lines.push(format!("<{CONTEXT_ROOT_TAG}>"));
    lines.push(format!("  <{SYSTEM_CONTEXT_TAG}>"));
    for section in system_sections {
        lines.push(indent_block(section, 4));
    }
    lines.push(format!("  </{SYSTEM_CONTEXT_TAG}>"));
    lines.push(format!("  <{USER_INPUT_TAG}>"));
    lines.push(indent_block(user_input_section, 4));
    lines.push(format!("  </{USER_INPUT_TAG}>"));
    lines.push(format!("</{CONTEXT_ROOT_TAG}>"));
    lines.join("\n")
}

fn indent_block(content: &str, spaces: usize) -> String {
    let prefix = " ".repeat(spaces);
    content
        .lines()
        .map(|line| format!("{prefix}{line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_cdata(content: &str) -> String {
    let normalized = content.replace("]]>", "]]]]><![CDATA[>");
    format!("<![CDATA[{normalized}]]>")
}

fn normalize_section_body(content: &str) -> String {
    let normalized = content.trim();
    if normalized.is_empty() {
        return EMPTY_SECTION_MARKER.to_string();
    }
    normalized.to_string()
}

#[cfg(test)]
mod tests {
    use super::{
        assemble_turn_context, ContextAssemblyInput, ContextMemorySnippet, CONTEXT_INJECTION_ORDER,
    };
    use crate::CrabError;

    fn sample_input() -> ContextAssemblyInput {
        ContextAssemblyInput {
            soul_document: "Soul section".to_string(),
            identity_document: "Identity section".to_string(),
            user_document: "User section".to_string(),
            memory_document: "Memory section".to_string(),
            memory_snippets: vec![
                ContextMemorySnippet {
                    path: "memory/users/42/2026-02-10.md".to_string(),
                    start_line: 9,
                    end_line: 10,
                    content: "user note".to_string(),
                },
                ContextMemorySnippet {
                    path: "memory/global/2026-02-10.md".to_string(),
                    start_line: 2,
                    end_line: 5,
                    content: "global note".to_string(),
                },
            ],
            latest_checkpoint_summary: Some("Checkpoint summary".to_string()),
            crab_runtime_brief: "Runtime brief section".to_string(),
            prompt_contract: "Prompt contract section".to_string(),
            turn_input: "Current user message".to_string(),
        }
    }

    #[test]
    fn injection_order_tokens_match_design_spec() {
        assert_eq!(
            CONTEXT_INJECTION_ORDER,
            [
                "SOUL.md",
                "IDENTITY.md",
                "USER.md",
                "MEMORY.md",
                "MEMORY_SNIPPETS",
                "LATEST_CHECKPOINT",
                "CRAB_RUNTIME_BRIEF",
                "PROMPT_CONTRACT",
                "TURN_INPUT",
            ]
        );
    }

    #[test]
    fn assembles_context_in_design_spec_order() {
        let rendered = assemble_turn_context(&sample_input()).expect("assembly should succeed");

        let expected_sections = [
            "<soul_md>",
            "<identity_md>",
            "<user_md>",
            "<memory_md>",
            "<memory_snippets>",
            "<latest_checkpoint>",
            "<crab_runtime_brief>",
            "<prompt_contract>",
            "<turn_input>",
        ];

        let mut cursor = 0usize;
        for section in expected_sections {
            let position = rendered[cursor..]
                .find(section)
                .expect("section should appear in order");
            cursor += position + section.len();
        }

        assert!(rendered.contains("<crab_turn_context>"));
        assert!(rendered.contains("<crab_system_context>"));
        assert!(rendered.contains("<crab_user_input>"));
        assert!(rendered.contains("Soul section"));
        assert!(rendered.contains("Checkpoint summary"));
        assert!(rendered.contains("Runtime brief section"));
        assert!(rendered.contains("Prompt contract section"));
        assert!(rendered.contains("Current user message"));
    }

    #[test]
    fn memory_snippets_are_sorted_deterministically() {
        let rendered = assemble_turn_context(&sample_input()).expect("assembly should succeed");
        let global_pos = rendered
            .find("<path><![CDATA[memory/global/2026-02-10.md]]></path>")
            .expect("global snippet should be first after sorting");
        let user_pos = rendered
            .find("<path><![CDATA[memory/users/42/2026-02-10.md]]></path>")
            .expect("user snippet should be second after sorting");
        assert!(global_pos < user_pos);
    }

    #[test]
    fn blank_sections_and_missing_optional_sections_use_explicit_markers() {
        let mut input = sample_input();
        input.soul_document = "  ".to_string();
        input.memory_snippets = Vec::new();
        input.latest_checkpoint_summary = Some("  ".to_string());

        let rendered = assemble_turn_context(&input).expect("assembly should succeed");
        assert!(rendered.contains("<soul_md><![CDATA[(empty)]]></soul_md>"));
        assert!(rendered.contains("<memory_snippets>"));
        assert!(rendered.contains("<none>(none)</none>"));
        assert!(rendered.contains("</memory_snippets>"));
        assert!(rendered.contains("<latest_checkpoint><![CDATA[(none)]]></latest_checkpoint>"));
    }

    #[test]
    fn checkpoint_section_uses_none_marker_when_absent() {
        let mut input = sample_input();
        input.latest_checkpoint_summary = None;
        let rendered = assemble_turn_context(&input).expect("assembly should succeed");
        assert!(rendered.contains("<latest_checkpoint><![CDATA[(none)]]></latest_checkpoint>"));
    }

    #[test]
    fn cdata_wrapping_handles_embedded_closing_marker() {
        let mut input = sample_input();
        input.turn_input = "payload ]]> with marker".to_string();
        let rendered = assemble_turn_context(&input).expect("assembly should succeed");
        assert!(rendered.contains("payload ]]]]><![CDATA[> with marker"));
    }

    #[test]
    fn rejects_blank_turn_input() {
        let mut input = sample_input();
        input.turn_input = " ".to_string();
        let error = assemble_turn_context(&input).expect_err("blank turn input should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "context_assembly",
                message: "turn_input must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn validates_memory_snippet_shapes() {
        let mut blank_path = sample_input();
        blank_path.memory_snippets[0].path = " ".to_string();
        let blank_path_error =
            assemble_turn_context(&blank_path).expect_err("blank snippet path should fail");
        assert_eq!(
            blank_path_error,
            CrabError::InvariantViolation {
                context: "context_assembly",
                message: "memory_snippets[].path must not be empty".to_string(),
            }
        );

        let mut zero_start = sample_input();
        zero_start.memory_snippets[0].start_line = 0;
        let zero_start_error =
            assemble_turn_context(&zero_start).expect_err("zero start line should fail");
        assert_eq!(
            zero_start_error,
            CrabError::InvariantViolation {
                context: "context_assembly",
                message: "memory_snippets[].start_line must be greater than 0".to_string(),
            }
        );

        let mut reversed_range = sample_input();
        reversed_range.memory_snippets[0].start_line = 11;
        reversed_range.memory_snippets[0].end_line = 10;
        let reversed_error =
            assemble_turn_context(&reversed_range).expect_err("reversed range should fail");
        assert_eq!(
            reversed_error,
            CrabError::InvariantViolation {
                context: "context_assembly",
                message: "memory_snippets[].end_line must be >= start_line".to_string(),
            }
        );

        let mut blank_content = sample_input();
        blank_content.memory_snippets[0].content = " ".to_string();
        let blank_content_error =
            assemble_turn_context(&blank_content).expect_err("blank snippet content should fail");
        assert_eq!(
            blank_content_error,
            CrabError::InvariantViolation {
                context: "context_assembly",
                message: "memory_snippets[].content must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn assembly_output_is_stable_for_same_input() {
        let input = sample_input();
        let first = assemble_turn_context(&input).expect("first render should succeed");
        let second = assemble_turn_context(&input).expect("second render should succeed");
        assert_eq!(first, second);
    }
}
