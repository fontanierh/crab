use std::sync::OnceLock;

static TRACING_INIT: OnceLock<()> = OnceLock::new();

/// Initialize structured logging to stderr.
///
/// - Uses `RUST_LOG` directives when present; otherwise uses `default_directives`.
/// - Safe to call multiple times.
/// - Logs must go to stderr because stdout is reserved for JSONL IPC.
pub fn init_tracing_stderr(default_directives: &'static str) {
    TRACING_INIT.get_or_init(|| {
        let filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_directives));
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber);
    });
}

#[cfg(test)]
mod tests {
    use super::init_tracing_stderr;

    #[test]
    fn init_is_idempotent() {
        init_tracing_stderr("info");
        init_tracing_stderr("info");
    }
}
