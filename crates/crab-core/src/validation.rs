use crate::{CrabError, CrabResult};

pub fn validate_non_empty_text(context: &'static str, field: &str, value: &str) -> CrabResult<()> {
    if value.trim().is_empty() {
        return Err(CrabError::InvariantViolation {
            context,
            message: format!("{field} must not be empty"),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::CrabError;

    use super::validate_non_empty_text;

    #[test]
    fn rejects_blank_values() {
        let error = validate_non_empty_text("validator", "field", "  ")
            .expect_err("blank values should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "validator",
                message: "field must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn accepts_non_blank_values() {
        validate_non_empty_text("validator", "field", "value").expect("value should pass");
    }
}
