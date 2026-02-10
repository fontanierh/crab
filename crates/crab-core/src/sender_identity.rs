use crate::validation::validate_non_empty_text;
use crate::{CrabError, CrabResult, OwnerConfig};

pub const OWNER_CANONICAL_USER_KEY: &str = "owner";
const SENDER_IDENTITY_CONTEXT: &str = "sender_identity_resolve";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SenderConversationKind {
    GuildChannel,
    Thread,
    DirectMessage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SenderIdentityInput {
    pub conversation_kind: SenderConversationKind,
    pub discord_user_id: String,
    pub username: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSenderIdentity {
    pub conversation_kind: SenderConversationKind,
    pub sender_discord_user_id: String,
    pub sender_username: Option<String>,
    pub canonical_user_key: String,
    pub sender_is_owner: bool,
}

pub fn resolve_sender_identity(
    input: &SenderIdentityInput,
    owner_config: &OwnerConfig,
) -> CrabResult<ResolvedSenderIdentity> {
    let sender_discord_user_id = normalize_sender_discord_user_id(&input.discord_user_id)?;
    let sender_username = normalize_sender_username(input.username.as_deref());

    let sender_is_owner = is_owner_sender(
        &sender_discord_user_id,
        sender_username.as_deref(),
        owner_config,
    );
    let canonical_user_key =
        canonical_user_key_for_sender(&sender_discord_user_id, sender_is_owner);

    Ok(ResolvedSenderIdentity {
        conversation_kind: input.conversation_kind,
        sender_discord_user_id,
        sender_username,
        canonical_user_key,
        sender_is_owner,
    })
}

fn normalize_sender_discord_user_id(raw_user_id: &str) -> CrabResult<String> {
    let normalized = raw_user_id.trim();
    validate_non_empty_text(SENDER_IDENTITY_CONTEXT, "discord_user_id", normalized)?;

    if !normalized
        .chars()
        .all(|character| character.is_ascii_digit())
    {
        return Err(CrabError::InvariantViolation {
            context: SENDER_IDENTITY_CONTEXT,
            message: "discord_user_id must contain only digits".to_string(),
        });
    }

    Ok(normalized.to_string())
}

fn normalize_sender_username(raw_username: Option<&str>) -> Option<String> {
    let raw_username = raw_username?;
    let normalized = raw_username.trim();
    if normalized.is_empty() {
        return None;
    }
    Some(normalized.to_string())
}

fn is_owner_sender(
    sender_discord_user_id: &str,
    sender_username: Option<&str>,
    owner_config: &OwnerConfig,
) -> bool {
    if owner_config
        .discord_user_ids
        .iter()
        .any(|owner_id| owner_id == sender_discord_user_id)
    {
        return true;
    }

    let Some(sender_username) = sender_username else {
        return false;
    };
    owner_config
        .aliases
        .iter()
        .any(|owner_alias| owner_alias.eq_ignore_ascii_case(sender_username))
}

fn canonical_user_key_for_sender(sender_discord_user_id: &str, sender_is_owner: bool) -> String {
    if sender_is_owner {
        return OWNER_CANONICAL_USER_KEY.to_string();
    }
    format!("discord:{sender_discord_user_id}")
}

#[cfg(test)]
mod tests {
    use crate::{CrabError, OwnerProfileDefaults};

    use super::{
        resolve_sender_identity, OwnerConfig, SenderConversationKind, SenderIdentityInput,
        OWNER_CANONICAL_USER_KEY,
    };

    fn owner_config(owner_ids: &[&str], aliases: &[&str]) -> OwnerConfig {
        OwnerConfig {
            discord_user_ids: owner_ids.iter().map(|id| (*id).to_string()).collect(),
            aliases: aliases.iter().map(|alias| (*alias).to_string()).collect(),
            profile_defaults: OwnerProfileDefaults::default(),
            machine_location: None,
            machine_timezone: None,
        }
    }

    fn sender_input(
        conversation_kind: SenderConversationKind,
        discord_user_id: &str,
        username: Option<&str>,
    ) -> SenderIdentityInput {
        SenderIdentityInput {
            conversation_kind,
            discord_user_id: discord_user_id.to_string(),
            username: username.map(str::to_string),
        }
    }

    fn all_kinds() -> [SenderConversationKind; 3] {
        [
            SenderConversationKind::GuildChannel,
            SenderConversationKind::Thread,
            SenderConversationKind::DirectMessage,
        ]
    }

    #[test]
    fn resolves_owner_by_discord_id_across_conversation_kinds() {
        let owner = owner_config(&["123"], &[]);
        for kind in all_kinds() {
            let input = sender_input(kind, "123", None);
            let resolved = resolve_sender_identity(&input, &owner).expect("resolution should pass");
            assert!(resolved.sender_is_owner);
            assert_eq!(resolved.canonical_user_key, OWNER_CANONICAL_USER_KEY);
            assert_eq!(resolved.sender_discord_user_id, "123");
            assert_eq!(resolved.conversation_kind, kind);
        }
    }

    #[test]
    fn resolves_owner_by_alias_case_insensitively_across_conversation_kinds() {
        let owner = owner_config(&[], &["CaptainCrab"]);
        for kind in all_kinds() {
            let input = sender_input(kind, "456", Some("captaincrab"));
            let resolved = resolve_sender_identity(&input, &owner).expect("resolution should pass");
            assert!(resolved.sender_is_owner);
            assert_eq!(resolved.canonical_user_key, OWNER_CANONICAL_USER_KEY);
            assert_eq!(resolved.sender_discord_user_id, "456");
            assert_eq!(resolved.sender_username, Some("captaincrab".to_string()));
        }
    }

    #[test]
    fn normalizes_sender_id_and_username_before_resolution() {
        let owner = owner_config(&["999"], &["Commander"]);
        let input = sender_input(
            SenderConversationKind::GuildChannel,
            "  999 ",
            Some(" Commander "),
        );
        let resolved = resolve_sender_identity(&input, &owner).expect("resolution should pass");

        assert_eq!(resolved.sender_discord_user_id, "999");
        assert_eq!(resolved.sender_username, Some("Commander".to_string()));
        assert!(resolved.sender_is_owner);
    }

    #[test]
    fn resolves_non_owner_to_discord_scoped_key() {
        let owner = owner_config(&["123"], &["owner_alias"]);
        let input = sender_input(SenderConversationKind::Thread, "789", Some("not_owner"));
        let resolved = resolve_sender_identity(&input, &owner).expect("resolution should pass");

        assert!(!resolved.sender_is_owner);
        assert_eq!(resolved.canonical_user_key, "discord:789");
        assert_eq!(resolved.sender_discord_user_id, "789");
        assert_eq!(resolved.sender_username, Some("not_owner".to_string()));
    }

    #[test]
    fn ignores_blank_username_for_owner_alias_matching() {
        let owner = owner_config(&[], &["owner_alias"]);
        let input = sender_input(SenderConversationKind::DirectMessage, "555", Some("   "));
        let resolved = resolve_sender_identity(&input, &owner).expect("resolution should pass");

        assert!(!resolved.sender_is_owner);
        assert_eq!(resolved.canonical_user_key, "discord:555");
        assert_eq!(resolved.sender_username, None);
    }

    #[test]
    fn rejects_blank_sender_discord_user_id() {
        let owner = owner_config(&["123"], &[]);
        let input = sender_input(SenderConversationKind::GuildChannel, "  ", None);
        let error = resolve_sender_identity(&input, &owner).expect_err("blank id should fail");

        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "sender_identity_resolve",
                message: "discord_user_id must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn rejects_non_numeric_sender_discord_user_id() {
        let owner = owner_config(&["123"], &[]);
        let input = sender_input(SenderConversationKind::GuildChannel, "abc", None);
        let error =
            resolve_sender_identity(&input, &owner).expect_err("non-numeric id should fail");

        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "sender_identity_resolve",
                message: "discord_user_id must contain only digits".to_string(),
            }
        );
    }
}
