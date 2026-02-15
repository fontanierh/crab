use std::fs;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::{CrabError, CrabResult};

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
    let random_suffix = format!("{:04x}", timestamp_ms as u16 ^ std::process::id() as u16);
    let filename = format!("{timestamp_ms}-{random_suffix}.json");
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

    Ok(results)
}

pub(crate) fn consume_signal_file(path: &Path, context: &'static str) -> CrabResult<()> {
    fs::remove_file(path).map_err(|error| CrabError::Io {
        context,
        path: Some(path.to_string_lossy().to_string()),
        message: error.to_string(),
    })
}
