use crate::context_assembly::{assemble_turn_context, ContextAssemblyInput};
use crate::{CrabError, CrabResult};

const CONTEXT_BUDGET_CONTEXT: &str = "context_budget";

pub const DEFAULT_CONTEXT_MAX_SOUL_TOKENS: usize = 2_048;
pub const DEFAULT_CONTEXT_MAX_IDENTITY_TOKENS: usize = 2_048;
pub const DEFAULT_CONTEXT_MAX_USER_TOKENS: usize = 2_048;
pub const DEFAULT_CONTEXT_MAX_MEMORY_TOKENS: usize = 16_000;
pub const DEFAULT_CONTEXT_MAX_PROMPT_CONTRACT_TOKENS: usize = 4_096;
pub const DEFAULT_CONTEXT_MAX_LATEST_CHECKPOINT_TOKENS: usize = 4_096;
pub const DEFAULT_CONTEXT_MAX_TURN_INPUT_TOKENS: usize = 4_096;
pub const DEFAULT_CONTEXT_MAX_MEMORY_SNIPPET_TOKENS: usize = 2_048;
pub const DEFAULT_CONTEXT_MAX_MEMORY_SNIPPET_COUNT: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextBudgetPolicy {
    pub max_soul_tokens: usize,
    pub max_identity_tokens: usize,
    pub max_user_tokens: usize,
    pub max_memory_tokens: usize,
    pub max_prompt_contract_tokens: usize,
    pub max_latest_checkpoint_tokens: usize,
    pub max_turn_input_tokens: usize,
    pub max_memory_snippet_tokens: usize,
    pub max_memory_snippet_count: usize,
}

impl Default for ContextBudgetPolicy {
    fn default() -> Self {
        Self {
            max_soul_tokens: DEFAULT_CONTEXT_MAX_SOUL_TOKENS,
            max_identity_tokens: DEFAULT_CONTEXT_MAX_IDENTITY_TOKENS,
            max_user_tokens: DEFAULT_CONTEXT_MAX_USER_TOKENS,
            max_memory_tokens: DEFAULT_CONTEXT_MAX_MEMORY_TOKENS,
            max_prompt_contract_tokens: DEFAULT_CONTEXT_MAX_PROMPT_CONTRACT_TOKENS,
            max_latest_checkpoint_tokens: DEFAULT_CONTEXT_MAX_LATEST_CHECKPOINT_TOKENS,
            max_turn_input_tokens: DEFAULT_CONTEXT_MAX_TURN_INPUT_TOKENS,
            max_memory_snippet_tokens: DEFAULT_CONTEXT_MAX_MEMORY_SNIPPET_TOKENS,
            max_memory_snippet_count: DEFAULT_CONTEXT_MAX_MEMORY_SNIPPET_COUNT,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextSectionTokenUsage {
    pub section: String,
    pub estimated_tokens: usize,
    pub budget_tokens: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextSnippetTokenUsage {
    pub path: String,
    pub estimated_tokens: usize,
    pub budget_tokens: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextBudgetReport {
    pub section_usage: Vec<ContextSectionTokenUsage>,
    pub snippet_usage: Vec<ContextSnippetTokenUsage>,
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

    let mut section_usage = Vec::new();
    for (section, value, budget_tokens) in [
        (
            "SOUL.md",
            input.soul_document.as_str(),
            policy.max_soul_tokens,
        ),
        (
            "IDENTITY.md",
            input.identity_document.as_str(),
            policy.max_identity_tokens,
        ),
        (
            "USER.md",
            input.user_document.as_str(),
            policy.max_user_tokens,
        ),
        (
            "MEMORY.md",
            input.memory_document.as_str(),
            policy.max_memory_tokens,
        ),
        (
            "PROMPT_CONTRACT",
            input.prompt_contract.as_str(),
            policy.max_prompt_contract_tokens,
        ),
        (
            "LATEST_CHECKPOINT",
            input.latest_checkpoint_summary.as_deref().unwrap_or(""),
            policy.max_latest_checkpoint_tokens,
        ),
        (
            "TURN_INPUT",
            input.turn_input.as_str(),
            policy.max_turn_input_tokens,
        ),
    ] {
        validate_section_budget(section, value, budget_tokens, &mut section_usage)?;
    }

    validate_memory_snippet_count(input, policy)?;
    let snippet_usage = validate_memory_snippet_budgets(input, policy)?;

    let rendered_context = assemble_turn_context(input)?;

    Ok(BudgetedContextOutput {
        rendered_context,
        budgeted_input: input.clone(),
        report: ContextBudgetReport {
            section_usage,
            snippet_usage,
        },
    })
}

pub fn estimate_token_count(value: &str) -> usize {
    let normalized = value.trim();
    if normalized.is_empty() {
        return 0;
    }

    let word_tokens = normalized.split_whitespace().count();
    let char_tokens = normalized.chars().count().saturating_add(3) / 4;
    word_tokens.max(char_tokens)
}

fn validate_policy(policy: &ContextBudgetPolicy) -> CrabResult<()> {
    for (field, value) in [
        ("max_soul_tokens", policy.max_soul_tokens),
        ("max_identity_tokens", policy.max_identity_tokens),
        ("max_user_tokens", policy.max_user_tokens),
        ("max_memory_tokens", policy.max_memory_tokens),
        (
            "max_prompt_contract_tokens",
            policy.max_prompt_contract_tokens,
        ),
        (
            "max_latest_checkpoint_tokens",
            policy.max_latest_checkpoint_tokens,
        ),
        ("max_turn_input_tokens", policy.max_turn_input_tokens),
        (
            "max_memory_snippet_tokens",
            policy.max_memory_snippet_tokens,
        ),
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

fn validate_section_budget(
    section: &str,
    value: &str,
    budget_tokens: usize,
    section_usage: &mut Vec<ContextSectionTokenUsage>,
) -> CrabResult<()> {
    let estimated_tokens = estimate_token_count(value);
    section_usage.push(ContextSectionTokenUsage {
        section: section.to_string(),
        estimated_tokens,
        budget_tokens,
    });

    if estimated_tokens > budget_tokens {
        return Err(CrabError::InvariantViolation {
            context: CONTEXT_BUDGET_CONTEXT,
            message: format!(
                "{section} exceeds token budget: estimated_tokens={estimated_tokens} max_tokens={budget_tokens}"
            ),
        });
    }

    Ok(())
}

fn validate_memory_snippet_count(
    input: &ContextAssemblyInput,
    policy: &ContextBudgetPolicy,
) -> CrabResult<()> {
    if input.memory_snippets.len() <= policy.max_memory_snippet_count {
        return Ok(());
    }

    Err(CrabError::InvariantViolation {
        context: CONTEXT_BUDGET_CONTEXT,
        message: format!(
            "memory_snippets count {} exceeds max_memory_snippet_count {}",
            input.memory_snippets.len(),
            policy.max_memory_snippet_count
        ),
    })
}

fn validate_memory_snippet_budgets(
    input: &ContextAssemblyInput,
    policy: &ContextBudgetPolicy,
) -> CrabResult<Vec<ContextSnippetTokenUsage>> {
    let mut usage = Vec::with_capacity(input.memory_snippets.len());
    for snippet in &input.memory_snippets {
        let estimated_tokens = estimate_token_count(&snippet.content);
        usage.push(ContextSnippetTokenUsage {
            path: snippet.path.clone(),
            estimated_tokens,
            budget_tokens: policy.max_memory_snippet_tokens,
        });

        if estimated_tokens > policy.max_memory_snippet_tokens {
            return Err(CrabError::InvariantViolation {
                context: CONTEXT_BUDGET_CONTEXT,
                message: format!(
                    "memory snippet {} exceeds token budget: estimated_tokens={} max_tokens={}",
                    snippet.path, estimated_tokens, policy.max_memory_snippet_tokens
                ),
            });
        }
    }

    Ok(usage)
}

#[cfg(test)]
mod tests {
    use crate::context_assembly::{ContextAssemblyInput, ContextMemorySnippet};
    use crate::CrabError;

    use super::{
        estimate_token_count, render_budgeted_turn_context, BudgetedContextOutput,
        ContextBudgetPolicy,
    };

    fn sample_input() -> ContextAssemblyInput {
        ContextAssemblyInput {
            soul_document: "soul".to_string(),
            identity_document: "identity".to_string(),
            user_document: "user".to_string(),
            memory_document: "memory".to_string(),
            memory_snippets: vec![],
            latest_checkpoint_summary: Some("checkpoint".to_string()),
            prompt_contract: "prompt contract".to_string(),
            turn_input: "turn".to_string(),
        }
    }

    fn generous_policy() -> ContextBudgetPolicy {
        ContextBudgetPolicy {
            max_soul_tokens: 10_000,
            max_identity_tokens: 10_000,
            max_user_tokens: 10_000,
            max_memory_tokens: 10_000,
            max_prompt_contract_tokens: 10_000,
            max_latest_checkpoint_tokens: 10_000,
            max_turn_input_tokens: 10_000,
            max_memory_snippet_tokens: 10_000,
            max_memory_snippet_count: 8,
        }
    }

    fn render(input: &ContextAssemblyInput, policy: &ContextBudgetPolicy) -> BudgetedContextOutput {
        render_budgeted_turn_context(input, policy).expect("budgeting should succeed")
    }

    #[test]
    fn default_policy_constants_are_stable() {
        let default_policy = ContextBudgetPolicy::default();
        assert_eq!(default_policy.max_soul_tokens, 2_048);
        assert_eq!(default_policy.max_identity_tokens, 2_048);
        assert_eq!(default_policy.max_user_tokens, 2_048);
        assert_eq!(default_policy.max_memory_tokens, 16_000);
        assert_eq!(default_policy.max_prompt_contract_tokens, 4_096);
        assert_eq!(default_policy.max_latest_checkpoint_tokens, 4_096);
        assert_eq!(default_policy.max_turn_input_tokens, 4_096);
        assert_eq!(default_policy.max_memory_snippet_tokens, 2_048);
        assert_eq!(default_policy.max_memory_snippet_count, 8);
    }

    #[test]
    fn leaves_context_unchanged_when_within_budget() {
        let input = sample_input();
        let output = render(&input, &generous_policy());

        assert_eq!(output.budgeted_input, input);
        assert!(output.rendered_context.contains("## SOUL.md\nsoul"));
        assert!(output
            .rendered_context
            .contains("## PROMPT_CONTRACT\nprompt contract"));
        assert_eq!(output.report.section_usage.len(), 7);
        assert!(output.report.snippet_usage.is_empty());
    }

    #[test]
    fn token_estimator_prefers_more_conservative_word_or_char_estimate() {
        assert_eq!(estimate_token_count(""), 0);
        assert_eq!(estimate_token_count("a"), 1);
        assert_eq!(estimate_token_count("abcd"), 1);
        assert_eq!(estimate_token_count("abcde"), 2);
        assert_eq!(estimate_token_count("one two three"), 4);
    }

    #[test]
    fn rejects_when_section_exceeds_budget() {
        let mut input = sample_input();
        input.memory_document = "m ".repeat(20);

        let mut policy = generous_policy();
        policy.max_memory_tokens = 5;

        let error = render_budgeted_turn_context(&input, &policy)
            .expect_err("over-budget memory document should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "context_budget",
                message: "MEMORY.md exceeds token budget: estimated_tokens=20 max_tokens=5"
                    .to_string(),
            }
        );
    }

    #[test]
    fn rejects_when_memory_snippet_count_exceeds_policy() {
        let mut input = sample_input();
        input.memory_snippets = vec![
            ContextMemorySnippet {
                path: "memory/users/42/2026-02-01.md".to_string(),
                start_line: 1,
                end_line: 1,
                content: "a".to_string(),
            },
            ContextMemorySnippet {
                path: "memory/users/42/2026-02-02.md".to_string(),
                start_line: 1,
                end_line: 1,
                content: "b".to_string(),
            },
        ];

        let mut policy = generous_policy();
        policy.max_memory_snippet_count = 1;

        let error = render_budgeted_turn_context(&input, &policy)
            .expect_err("snippet count over budget should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "context_budget",
                message: "memory_snippets count 2 exceeds max_memory_snippet_count 1".to_string(),
            }
        );
    }

    #[test]
    fn rejects_when_memory_snippet_exceeds_budget() {
        let mut input = sample_input();
        input.memory_snippets = vec![ContextMemorySnippet {
            path: "memory/users/42/2026-02-01.md".to_string(),
            start_line: 1,
            end_line: 1,
            content: "word ".repeat(15),
        }];

        let mut policy = generous_policy();
        policy.max_memory_snippet_tokens = 5;

        let error = render_budgeted_turn_context(&input, &policy)
            .expect_err("snippet token budget should be enforced");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "context_budget",
                message: "memory snippet memory/users/42/2026-02-01.md exceeds token budget: estimated_tokens=19 max_tokens=5".to_string(),
            }
        );
    }

    #[test]
    fn rejects_zero_policy_values() {
        for mutate in [
            |policy: &mut ContextBudgetPolicy| policy.max_soul_tokens = 0,
            |policy: &mut ContextBudgetPolicy| policy.max_identity_tokens = 0,
            |policy: &mut ContextBudgetPolicy| policy.max_user_tokens = 0,
            |policy: &mut ContextBudgetPolicy| policy.max_memory_tokens = 0,
            |policy: &mut ContextBudgetPolicy| policy.max_prompt_contract_tokens = 0,
            |policy: &mut ContextBudgetPolicy| policy.max_latest_checkpoint_tokens = 0,
            |policy: &mut ContextBudgetPolicy| policy.max_turn_input_tokens = 0,
            |policy: &mut ContextBudgetPolicy| policy.max_memory_snippet_tokens = 0,
            |policy: &mut ContextBudgetPolicy| policy.max_memory_snippet_count = 0,
        ] {
            let mut policy = generous_policy();
            mutate(&mut policy);
            let error = render_budgeted_turn_context(&sample_input(), &policy)
                .expect_err("zero policy values should fail validation");
            assert!(matches!(
                error,
                CrabError::InvariantViolation {
                    context: "context_budget",
                    ..
                }
            ));
        }
    }
}
