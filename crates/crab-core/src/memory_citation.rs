use crate::trust::TrustSurface;

pub const SHARED_CONTEXT_DISCLOSURE_TEXT: &str =
    "Memory consulted; citations withheld in shared Discord context.";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryCitationMode {
    Auto,
    On,
    Off,
}

impl MemoryCitationMode {
    #[must_use]
    pub const fn as_token(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::On => "on",
            Self::Off => "off",
        }
    }

    #[must_use]
    pub fn parse_token(value: &str) -> Option<Self> {
        match value {
            "auto" => Some(Self::Auto),
            "on" => Some(Self::On),
            "off" => Some(Self::Off),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryRecallSource {
    CliSearch,
    CliGet,
    NativeSearch,
    NativeRead,
}

impl MemoryRecallSource {
    #[must_use]
    pub const fn as_token(self) -> &'static str {
        match self {
            Self::CliSearch => "cli_search",
            Self::CliGet => "cli_get",
            Self::NativeSearch => "native_search",
            Self::NativeRead => "native_read",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryCitationPolicyInput {
    pub mode: MemoryCitationMode,
    pub surface: TrustSurface,
    pub sender_is_owner: bool,
    pub source: MemoryRecallSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryCitationPolicyDecision {
    pub include_citation: bool,
    pub require_disclosure: bool,
}

#[must_use]
pub fn evaluate_memory_citation_policy(
    input: &MemoryCitationPolicyInput,
) -> MemoryCitationPolicyDecision {
    match input.mode {
        MemoryCitationMode::On => policy_decision(true, false),
        MemoryCitationMode::Off => policy_decision(false, false),
        MemoryCitationMode::Auto => evaluate_auto_mode_policy(input),
    }
}

#[must_use]
pub fn disclosure_text_for_source(source: MemoryRecallSource) -> &'static str {
    match source {
        MemoryRecallSource::CliSearch => {
            "Memory consulted via crab-memory-search; citations withheld in shared Discord context."
        }
        MemoryRecallSource::CliGet => {
            "Memory consulted via crab-memory-get; citations withheld in shared Discord context."
        }
        MemoryRecallSource::NativeSearch => {
            "Memory consulted via native grep/rg search; citations withheld in shared Discord context."
        }
        MemoryRecallSource::NativeRead => {
            "Memory consulted via native file reads; citations withheld in shared Discord context."
        }
    }
}

#[must_use]
pub fn format_memory_citation(path: &str, start_line: u32, end_line: u32) -> String {
    if start_line == end_line {
        return format!("{path}#L{start_line}");
    }
    format!("{path}#L{start_line}-L{end_line}")
}

fn evaluate_auto_mode_policy(input: &MemoryCitationPolicyInput) -> MemoryCitationPolicyDecision {
    match input.surface {
        TrustSurface::DirectMessage => policy_decision(true, false),
        TrustSurface::SharedDiscord => {
            if input.sender_is_owner {
                return policy_decision(false, false);
            }
            policy_decision(false, source_requires_disclosure(input.source))
        }
    }
}

fn source_requires_disclosure(source: MemoryRecallSource) -> bool {
    match source {
        MemoryRecallSource::CliSearch
        | MemoryRecallSource::CliGet
        | MemoryRecallSource::NativeSearch
        | MemoryRecallSource::NativeRead => true,
    }
}

const fn policy_decision(
    include_citation: bool,
    require_disclosure: bool,
) -> MemoryCitationPolicyDecision {
    MemoryCitationPolicyDecision {
        include_citation,
        require_disclosure,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        disclosure_text_for_source, evaluate_memory_citation_policy, format_memory_citation,
        MemoryCitationMode, MemoryCitationPolicyDecision, MemoryCitationPolicyInput,
        MemoryRecallSource,
    };
    use crate::TrustSurface;

    const ALL_SOURCES: [MemoryRecallSource; 4] = [
        MemoryRecallSource::CliSearch,
        MemoryRecallSource::CliGet,
        MemoryRecallSource::NativeSearch,
        MemoryRecallSource::NativeRead,
    ];

    fn input(
        mode: MemoryCitationMode,
        source: MemoryRecallSource,
        surface: TrustSurface,
        sender_is_owner: bool,
    ) -> MemoryCitationPolicyInput {
        MemoryCitationPolicyInput {
            mode,
            source,
            surface,
            sender_is_owner,
        }
    }

    #[test]
    fn citation_mode_tokens_round_trip() {
        let modes = [
            MemoryCitationMode::Auto,
            MemoryCitationMode::On,
            MemoryCitationMode::Off,
        ];
        for mode in modes {
            let token = mode.as_token();
            assert_eq!(MemoryCitationMode::parse_token(token), Some(mode));
        }
    }

    #[test]
    fn citation_mode_parser_rejects_unknown_tokens() {
        assert_eq!(MemoryCitationMode::parse_token("AUTO"), None);
        assert_eq!(MemoryCitationMode::parse_token(" disabled "), None);
    }

    #[test]
    fn recall_source_tokens_are_stable() {
        let expectations = [
            (MemoryRecallSource::CliSearch, "cli_search"),
            (MemoryRecallSource::CliGet, "cli_get"),
            (MemoryRecallSource::NativeSearch, "native_search"),
            (MemoryRecallSource::NativeRead, "native_read"),
        ];
        for (source, expected) in expectations {
            assert_eq!(source.as_token(), expected);
        }
    }

    #[test]
    fn mode_on_always_enables_citations_without_disclosure() {
        let expected = MemoryCitationPolicyDecision {
            include_citation: true,
            require_disclosure: false,
        };
        for surface in [TrustSurface::SharedDiscord, TrustSurface::DirectMessage] {
            for sender_is_owner in [false, true] {
                for source in ALL_SOURCES {
                    assert_eq!(
                        evaluate_memory_citation_policy(&input(
                            MemoryCitationMode::On,
                            source,
                            surface,
                            sender_is_owner,
                        )),
                        expected
                    );
                }
            }
        }
    }

    #[test]
    fn mode_off_always_disables_citations_and_disclosure() {
        let expected = MemoryCitationPolicyDecision {
            include_citation: false,
            require_disclosure: false,
        };
        for surface in [TrustSurface::SharedDiscord, TrustSurface::DirectMessage] {
            for sender_is_owner in [false, true] {
                for source in ALL_SOURCES {
                    assert_eq!(
                        evaluate_memory_citation_policy(&input(
                            MemoryCitationMode::Off,
                            source,
                            surface,
                            sender_is_owner,
                        )),
                        expected
                    );
                }
            }
        }
    }

    #[test]
    fn auto_mode_direct_messages_enable_citations_for_all_sources() {
        for sender_is_owner in [false, true] {
            for source in ALL_SOURCES {
                assert_eq!(
                    evaluate_memory_citation_policy(&input(
                        MemoryCitationMode::Auto,
                        source,
                        TrustSurface::DirectMessage,
                        sender_is_owner,
                    )),
                    MemoryCitationPolicyDecision {
                        include_citation: true,
                        require_disclosure: false,
                    }
                );
            }
        }
    }

    #[test]
    fn auto_mode_shared_context_suppresses_citations_for_owner_and_non_owner() {
        for sender_is_owner in [false, true] {
            for source in ALL_SOURCES {
                let decision = evaluate_memory_citation_policy(&input(
                    MemoryCitationMode::Auto,
                    source,
                    TrustSurface::SharedDiscord,
                    sender_is_owner,
                ));
                assert!(!decision.include_citation);
                assert_eq!(decision.require_disclosure, !sender_is_owner);
            }
        }
    }

    #[test]
    fn disclosure_text_is_non_empty_for_each_source() {
        for source in ALL_SOURCES {
            let text = disclosure_text_for_source(source);
            assert!(text.contains("shared Discord context"));
            assert!(!text.trim().is_empty());
        }
    }

    #[test]
    fn format_memory_citation_supports_single_line_and_ranges() {
        assert_eq!(
            format_memory_citation("MEMORY.md", 9, 9),
            "MEMORY.md#L9".to_string()
        );
        assert_eq!(
            format_memory_citation("memory/users/123/2026-02-10.md", 2, 5),
            "memory/users/123/2026-02-10.md#L2-L5".to_string()
        );
    }
}
