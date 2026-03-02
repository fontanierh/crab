use crate::validation::validate_non_empty_text;
use crate::{
    CrabError, CrabResult, OwnerConfig, OwnerProfileMetadata, ResolvedSenderIdentity,
    SenderConversationKind, OWNER_CANONICAL_USER_KEY,
};

const TRUST_CONTEXT: &str = "sender_trust_context";
const NON_OWNER_CANONICAL_PREFIX: &str = "discord:";
pub const OWNER_MEMORY_SCOPE_DIRECTORY: &str = "owner";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrustSurface {
    SharedDiscord,
    DirectMessage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryScopeMode {
    PerUser,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryScopeResolution {
    pub surface: TrustSurface,
    pub mode: MemoryScopeMode,
    pub user_scope_key: String,
    pub user_scope_directory: String,
    pub include_global_memory: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SenderTrustContext {
    pub sender_id: String,
    pub sender_is_owner: bool,
    pub owner_profile: Option<OwnerProfileMetadata>,
    pub memory_scope: MemoryScopeResolution,
}

pub fn resolve_sender_trust_context(
    sender: &ResolvedSenderIdentity,
    owner_config: &OwnerConfig,
) -> CrabResult<SenderTrustContext> {
    validate_sender_identity(sender)?;

    let surface = trust_surface_for_conversation(sender.conversation_kind);
    let mode = memory_scope_mode_for_surface(surface);
    let user_scope_directory = memory_scope_directory_for_sender(sender);
    let owner_profile = resolve_owner_profile(sender.sender_is_owner, owner_config);

    Ok(SenderTrustContext {
        sender_id: sender.sender_discord_user_id.clone(),
        sender_is_owner: sender.sender_is_owner,
        owner_profile,
        memory_scope: MemoryScopeResolution {
            surface,
            mode,
            user_scope_key: sender.canonical_user_key.clone(),
            user_scope_directory,
            include_global_memory: true,
        },
    })
}

fn validate_sender_identity(sender: &ResolvedSenderIdentity) -> CrabResult<()> {
    validate_non_empty_text(
        TRUST_CONTEXT,
        "sender_discord_user_id",
        &sender.sender_discord_user_id,
    )?;
    if !sender
        .sender_discord_user_id
        .bytes()
        .all(|byte| byte.is_ascii_digit())
    {
        return Err(CrabError::InvariantViolation {
            context: TRUST_CONTEXT,
            message: "sender_discord_user_id must contain only digits".to_string(),
        });
    }

    validate_non_empty_text(
        TRUST_CONTEXT,
        "canonical_user_key",
        &sender.canonical_user_key,
    )?;

    if sender.sender_is_owner {
        if sender.canonical_user_key == OWNER_CANONICAL_USER_KEY {
            return Ok(());
        }

        return Err(CrabError::InvariantViolation {
            context: TRUST_CONTEXT,
            message: format!(
                "owner sender must use canonical_user_key {:?}, got {:?}",
                OWNER_CANONICAL_USER_KEY, sender.canonical_user_key
            ),
        });
    }

    let expected_non_owner_key = format!(
        "{NON_OWNER_CANONICAL_PREFIX}{}",
        sender.sender_discord_user_id
    );
    if sender.canonical_user_key == expected_non_owner_key {
        return Ok(());
    }

    Err(CrabError::InvariantViolation {
        context: TRUST_CONTEXT,
        message: format!(
            "non-owner sender must use canonical_user_key {:?}, got {:?}",
            expected_non_owner_key, sender.canonical_user_key
        ),
    })
}

fn trust_surface_for_conversation(conversation_kind: SenderConversationKind) -> TrustSurface {
    match conversation_kind {
        SenderConversationKind::DirectMessage => TrustSurface::DirectMessage,
        SenderConversationKind::GuildChannel | SenderConversationKind::Thread => {
            TrustSurface::SharedDiscord
        }
    }
}

fn memory_scope_mode_for_surface(surface: TrustSurface) -> MemoryScopeMode {
    match surface {
        TrustSurface::SharedDiscord => MemoryScopeMode::PerUser,
        TrustSurface::DirectMessage => MemoryScopeMode::PerUser,
    }
}

fn memory_scope_directory_for_sender(sender: &ResolvedSenderIdentity) -> String {
    if sender.sender_is_owner {
        return OWNER_MEMORY_SCOPE_DIRECTORY.to_string();
    }
    sender.sender_discord_user_id.clone()
}

fn resolve_owner_profile(
    sender_is_owner: bool,
    owner_config: &OwnerConfig,
) -> Option<OwnerProfileMetadata> {
    if !sender_is_owner {
        return None;
    }

    let owner_profile = OwnerProfileMetadata {
        machine_location: owner_config.machine_location.clone(),
        machine_timezone: owner_config.machine_timezone.clone(),
        default_backend: owner_config.profile_defaults.backend,
        default_model: owner_config.profile_defaults.model.clone(),
        default_reasoning_level: owner_config.profile_defaults.reasoning_level,
    };

    if owner_profile.machine_location.is_none()
        && owner_profile.machine_timezone.is_none()
        && owner_profile.default_backend.is_none()
        && owner_profile.default_model.is_none()
        && owner_profile.default_reasoning_level.is_none()
    {
        return None;
    }

    Some(owner_profile)
}

#[cfg(test)]
mod tests {
    use crate::{
        BackendKind, OwnerProfileDefaults, ReasoningLevel, ResolvedSenderIdentity,
        SenderConversationKind,
    };

    use super::{
        resolve_sender_trust_context, MemoryScopeMode, OwnerConfig, TrustSurface,
        OWNER_MEMORY_SCOPE_DIRECTORY,
    };
    use crate::CrabError;

    fn owner_config_with_defaults() -> OwnerConfig {
        let mut owner = empty_owner_config();
        owner.aliases = vec!["captain-crab".to_string()];
        owner.profile_defaults.backend = Some(BackendKind::Claude);
        owner.profile_defaults.model = Some("claude-opus-4-6".to_string());
        owner.profile_defaults.reasoning_level = Some(ReasoningLevel::High);
        owner.machine_location = Some("Paris, France".to_string());
        owner.machine_timezone = Some("Europe/Paris".to_string());
        owner
    }

    fn empty_owner_config() -> OwnerConfig {
        OwnerConfig {
            discord_user_ids: vec!["123".to_string()],
            aliases: Vec::new(),
            profile_defaults: OwnerProfileDefaults::default(),
            machine_location: None,
            machine_timezone: None,
        }
    }

    fn sender_identity(
        conversation_kind: SenderConversationKind,
        sender_discord_user_id: &str,
        canonical_user_key: &str,
        sender_is_owner: bool,
    ) -> ResolvedSenderIdentity {
        ResolvedSenderIdentity {
            conversation_kind,
            sender_discord_user_id: sender_discord_user_id.to_string(),
            sender_username: None,
            canonical_user_key: canonical_user_key.to_string(),
            sender_is_owner,
        }
    }

    #[test]
    fn shared_surfaces_use_per_user_scope_for_non_owner_runs() {
        for conversation_kind in [
            SenderConversationKind::GuildChannel,
            SenderConversationKind::Thread,
        ] {
            let sender = sender_identity(conversation_kind, "456", "discord:456", false);
            let context = resolve_sender_trust_context(&sender, &owner_config_with_defaults())
                .expect("non-owner trust context should resolve");

            assert_eq!(context.sender_id, "456");
            assert!(!context.sender_is_owner);
            assert!(context.owner_profile.is_none());
            assert_eq!(context.memory_scope.surface, TrustSurface::SharedDiscord);
            assert_eq!(context.memory_scope.mode, MemoryScopeMode::PerUser);
            assert_eq!(context.memory_scope.user_scope_key, "discord:456");
            assert_eq!(context.memory_scope.user_scope_directory, "456");
            assert!(context.memory_scope.include_global_memory);
        }
    }

    #[test]
    fn owner_context_uses_owner_scope_and_profile_on_direct_messages() {
        let sender = sender_identity(SenderConversationKind::DirectMessage, "123", "owner", true);
        let context = resolve_sender_trust_context(&sender, &owner_config_with_defaults())
            .expect("owner trust context should resolve");

        assert_eq!(context.sender_id, "123");
        assert!(context.sender_is_owner);
        assert_eq!(context.memory_scope.surface, TrustSurface::DirectMessage);
        assert_eq!(context.memory_scope.mode, MemoryScopeMode::PerUser);
        assert_eq!(context.memory_scope.user_scope_key, "owner");
        assert_eq!(
            context.memory_scope.user_scope_directory,
            OWNER_MEMORY_SCOPE_DIRECTORY
        );
        assert!(context.memory_scope.include_global_memory);

        let owner_profile = context
            .owner_profile
            .expect("owner profile defaults should be included");
        assert_eq!(
            owner_profile.machine_location,
            Some("Paris, France".to_string())
        );
        assert_eq!(
            owner_profile.machine_timezone,
            Some("Europe/Paris".to_string())
        );
        assert_eq!(owner_profile.default_backend, Some(BackendKind::Claude));
        assert_eq!(
            owner_profile.default_model,
            Some("claude-opus-4-6".to_string())
        );
        assert_eq!(
            owner_profile.default_reasoning_level,
            Some(ReasoningLevel::High)
        );
    }

    #[test]
    fn owner_profile_is_omitted_when_owner_defaults_are_empty() {
        let sender = sender_identity(SenderConversationKind::DirectMessage, "123", "owner", true);
        let context = resolve_sender_trust_context(&sender, &empty_owner_config())
            .expect("owner trust context should still resolve");
        assert!(context.owner_profile.is_none());
    }

    #[test]
    fn rejects_blank_sender_discord_user_id() {
        let sender = sender_identity(SenderConversationKind::Thread, "   ", "discord:1", false);
        let error = resolve_sender_trust_context(&sender, &owner_config_with_defaults())
            .expect_err("blank sender id should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "sender_trust_context",
                message: "sender_discord_user_id must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn rejects_non_numeric_sender_discord_user_id() {
        let sender = sender_identity(SenderConversationKind::Thread, "abc", "discord:abc", false);
        let error = resolve_sender_trust_context(&sender, &owner_config_with_defaults())
            .expect_err("non-numeric sender id should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "sender_trust_context",
                message: "sender_discord_user_id must contain only digits".to_string(),
            }
        );
    }

    #[test]
    fn rejects_blank_canonical_user_key() {
        let sender = sender_identity(SenderConversationKind::Thread, "456", "   ", false);
        let error = resolve_sender_trust_context(&sender, &owner_config_with_defaults())
            .expect_err("blank canonical key should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "sender_trust_context",
                message: "canonical_user_key must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn rejects_owner_sender_without_owner_canonical_key() {
        let sender = sender_identity(
            SenderConversationKind::DirectMessage,
            "123",
            "discord:123",
            true,
        );
        let error = resolve_sender_trust_context(&sender, &owner_config_with_defaults())
            .expect_err("owner canonical mismatch should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "sender_trust_context",
                message: "owner sender must use canonical_user_key \"owner\", got \"discord:123\""
                    .to_string(),
            }
        );
    }

    #[test]
    fn rejects_non_owner_sender_with_owner_canonical_key() {
        let sender = sender_identity(SenderConversationKind::Thread, "456", "owner", false);
        let error = resolve_sender_trust_context(&sender, &owner_config_with_defaults())
            .expect_err("non-owner with owner key should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "sender_trust_context",
                message:
                    "non-owner sender must use canonical_user_key \"discord:456\", got \"owner\""
                        .to_string(),
            }
        );
    }

    #[test]
    fn rejects_non_owner_sender_with_mismatched_canonical_key() {
        let sender = sender_identity(SenderConversationKind::Thread, "456", "discord:999", false);
        let error = resolve_sender_trust_context(&sender, &owner_config_with_defaults())
            .expect_err("non-owner canonical mismatch should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "sender_trust_context",
                message:
                    "non-owner sender must use canonical_user_key \"discord:456\", got \"discord:999\""
                        .to_string(),
            }
        );
    }
}
