#![deny(warnings, dead_code, unused_imports, unused_variables)]

/// Canonical crate identifier for the crab runtime.
#[must_use]
pub fn crate_id() -> &'static str {
    "crab-core"
}

#[cfg(test)]
mod tests {
    use super::crate_id;

    #[test]
    fn returns_expected_crate_id() {
        assert_eq!(crate_id(), "crab-core");
    }
}
