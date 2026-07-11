use crate::{sha256_hex, FactoryError, FactoryResult};

pub(crate) const THERMO_SKILL_SHA256: &str =
    "7faca08b51b643b2ddd0836f92af15574444024685dcc1e677dbbb39ae8c9e8f";
pub(crate) const THERMO_SKILL_COMMIT: &str = "0dda29e839d15464a137af9935665a5a47ee09b8";
pub(crate) const THERMO_SKILL_SOURCE: &str = "https://github.com/cursor/plugins/blob/0dda29e839d15464a137af9935665a5a47ee09b8/cursor-team-kit/skills/thermo-nuclear-code-quality-review/SKILL.md";
pub(crate) const THERMO_SKILL_MANIFEST_PATH: &str =
    "crates/crab-factory/vendor/thermo-nuclear-code-quality-review.md (embedded)";
pub(crate) const THERMO_RUBRIC: &str =
    include_str!("../vendor/thermo-nuclear-code-quality-review.md");

pub(crate) fn verify() -> FactoryResult<()> {
    verify_bytes(THERMO_RUBRIC.as_bytes())
}

fn verify_bytes(bytes: &[u8]) -> FactoryResult<()> {
    let actual = sha256_hex(bytes);
    if actual != THERMO_SKILL_SHA256 {
        return Err(FactoryError::new(format!(
            "vendored thermonuclear rubric failed its integrity check: expected {THERMO_SKILL_SHA256}, found {actual}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_rubric_matches_pin_and_mismatch_is_rejected() {
        verify().unwrap();
        assert!(THERMO_RUBRIC.starts_with("---\nname: thermo-nuclear"));
        assert!(THERMO_RUBRIC.ends_with("cleaner decomposition.\n"));
        let error = verify_bytes(b"changed").unwrap_err();
        assert!(error.to_string().contains("failed its integrity check"));
        assert!(THERMO_SKILL_SOURCE.contains(THERMO_SKILL_COMMIT));
    }
}
