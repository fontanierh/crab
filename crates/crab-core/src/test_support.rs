use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

pub(crate) struct TempDir {
    pub(crate) root: PathBuf,
}

impl TempDir {
    pub(crate) fn new(prefix: &str, label: &str) -> Self {
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "crab-{prefix}-{label}-{}-{counter}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("temp dir should be creatable");
        Self { root }
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

/// Sabotage a signal directory so subsequent writes fail: replace a written file
/// with a directory so filename collisions occur, then remove write permission on
/// the containing directory (Unix only).
pub(crate) fn sabotage_signal_dir_for_write(written_file: &std::path::Path, dir: &std::path::Path) {
    fs::remove_file(written_file).expect("remove should succeed");
    fs::create_dir_all(written_file).expect("sabotage dir should be creatable");
    #[cfg(unix)]
    deny_access(dir);
}

#[cfg(unix)]
pub(crate) fn deny_access(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o000)).expect("chmod should succeed");
}

#[cfg(unix)]
pub(crate) fn restore_access(path: &std::path::Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    let _ = fs::set_permissions(path, fs::Permissions::from_mode(mode));
}
