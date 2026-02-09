use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CrabError {
    MissingConfig {
        key: &'static str,
    },
    InvalidConfig {
        key: &'static str,
        value: String,
        reason: &'static str,
    },
    Io {
        context: &'static str,
        path: Option<String>,
        message: String,
    },
    Serialization {
        context: &'static str,
        path: Option<String>,
        message: String,
    },
    CorruptData {
        context: &'static str,
        path: String,
    },
    InvariantViolation {
        context: &'static str,
        message: String,
    },
}

pub type CrabResult<T> = Result<T, CrabError>;

impl Display for CrabError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingConfig { key } => write!(f, "missing required config: {key}"),
            Self::InvalidConfig { key, value, reason } => {
                write!(f, "invalid config {key}={value:?}: {reason}")
            }
            Self::Io {
                context,
                path,
                message,
            } => match path {
                Some(path) => write!(f, "{context} failed at {path}: {message}"),
                None => write!(f, "{context} failed: {message}"),
            },
            Self::Serialization {
                context,
                path,
                message,
            } => match path {
                Some(path) => write!(f, "{context} serialization failed at {path}: {message}"),
                None => write!(f, "{context} serialization failed: {message}"),
            },
            Self::CorruptData { context, path } => {
                write!(f, "{context} encountered corrupt data at {path}")
            }
            Self::InvariantViolation { context, message } => {
                write!(f, "{context} invariant violation: {message}")
            }
        }
    }
}

impl std::error::Error for CrabError {}

#[cfg(test)]
mod tests {
    use super::CrabError;

    #[test]
    fn display_is_actionable() {
        let missing = CrabError::MissingConfig {
            key: "CRAB_DISCORD_TOKEN",
        };
        assert_eq!(
            missing.to_string(),
            "missing required config: CRAB_DISCORD_TOKEN"
        );

        let invalid = CrabError::InvalidConfig {
            key: "CRAB_MAX_CONCURRENT_LANES",
            value: "abc".to_string(),
            reason: "must be a positive integer",
        };
        assert_eq!(
            invalid.to_string(),
            "invalid config CRAB_MAX_CONCURRENT_LANES=\"abc\": must be a positive integer"
        );

        let io = CrabError::Io {
            context: "session_write",
            path: Some("/tmp/session.json".to_string()),
            message: "permission denied".to_string(),
        };
        assert_eq!(
            io.to_string(),
            "session_write failed at /tmp/session.json: permission denied"
        );

        let io_without_path = CrabError::Io {
            context: "session_write",
            path: None,
            message: "permission denied".to_string(),
        };
        assert_eq!(
            io_without_path.to_string(),
            "session_write failed: permission denied"
        );

        let serialization = CrabError::Serialization {
            context: "index_load",
            path: None,
            message: "expected value".to_string(),
        };
        assert_eq!(
            serialization.to_string(),
            "index_load serialization failed: expected value"
        );

        let serialization_with_path = CrabError::Serialization {
            context: "index_load",
            path: Some("/tmp/index.json".to_string()),
            message: "unexpected token".to_string(),
        };
        assert_eq!(
            serialization_with_path.to_string(),
            "index_load serialization failed at /tmp/index.json: unexpected token"
        );

        let corruption = CrabError::CorruptData {
            context: "session_read",
            path: "/tmp/session.json".to_string(),
        };
        assert_eq!(
            corruption.to_string(),
            "session_read encountered corrupt data at /tmp/session.json"
        );

        let invariant = CrabError::InvariantViolation {
            context: "event_replay_sequence",
            message: "expected sequence 2, got 4".to_string(),
        };
        assert_eq!(
            invariant.to_string(),
            "event_replay_sequence invariant violation: expected sequence 2, got 4"
        );
    }
}
