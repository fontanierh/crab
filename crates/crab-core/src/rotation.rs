#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RotationTrigger {
    CliRotation,
}

#[cfg(test)]
mod tests {
    use super::RotationTrigger;

    #[test]
    fn cli_rotation_variant_is_usable() {
        let trigger = RotationTrigger::CliRotation;
        assert_eq!(trigger, RotationTrigger::CliRotation);
    }
}
