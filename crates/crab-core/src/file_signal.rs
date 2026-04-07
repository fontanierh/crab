use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::{CrabError, CrabResult};

/// Monotonic counter to guarantee unique filenames even within the same
/// millisecond and process. Combined with timestamp + pid this prevents
/// the overwrite bug where two writes from the same process in the same
/// millisecond would produce identical filenames.
static SIGNAL_COUNTER: AtomicU64 = AtomicU64::new(0);

pub(crate) fn write_signal_file<T: Serialize>(
    state_root: &Path,
    dir_name: &str,
    signal: &T,
    context: &'static str,
) -> CrabResult<PathBuf> {
    let dir = state_root.join(dir_name);
    fs::create_dir_all(&dir).map_err(|error| CrabError::Io {
        context,
        path: Some(dir.to_string_lossy().to_string()),
        message: error.to_string(),
    })?;

    let json = serde_json::to_string(signal).expect("signal serialization is infallible");

    let timestamp_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let seq = SIGNAL_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let filename = format!("{timestamp_ms}-{pid}-{seq:010}.json");
    let path = dir.join(&filename);

    fs::write(&path, &json).map_err(|error| CrabError::Io {
        context,
        path: Some(path.to_string_lossy().to_string()),
        message: error.to_string(),
    })?;

    Ok(path)
}

pub(crate) fn read_signal_files<T: DeserializeOwned>(
    state_root: &Path,
    dir_name: &str,
    context: &'static str,
) -> CrabResult<Vec<(PathBuf, T)>> {
    let dir = state_root.join(dir_name);
    if !dir.is_dir() {
        return Ok(Vec::new());
    }

    let entries = fs::read_dir(&dir).map_err(|error| CrabError::Io {
        context,
        path: Some(dir.to_string_lossy().to_string()),
        message: error.to_string(),
    })?;

    let mut results = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let content = match fs::read_to_string(&path) {
            Ok(content) => content,
            Err(_) => continue,
        };
        let signal: T = match serde_json::from_str(&content) {
            Ok(signal) => signal,
            Err(_) => continue,
        };
        results.push((path, signal));
    }

    // Sort by filename for deterministic, approximately chronological order.
    // Filenames are `{timestamp_ms}-{pid}-{seq:010}.json`. Lexicographic sort
    // is exact within a single process (timestamp + monotonic seq). Cross-process
    // same-millisecond writes are ordered by pid, which is arbitrary but stable.
    results.sort_by(|(a, _), (b, _)| a.file_name().cmp(&b.file_name()));

    Ok(results)
}

pub(crate) fn consume_signal_file(path: &Path, context: &'static str) -> CrabResult<()> {
    fs::remove_file(path).map_err(|error| CrabError::Io {
        context,
        path: Some(path.to_string_lossy().to_string()),
        message: error.to_string(),
    })
}
