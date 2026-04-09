use std::fs;
use std::io::{ErrorKind, Write};
use std::path::Path;

use serde::Serialize;

use crate::{CrabError, CrabResult};

pub(crate) fn validate_state_root(state_root: &Path, context: &'static str) -> CrabResult<()> {
    if state_root.as_os_str().is_empty() {
        return Err(CrabError::InvariantViolation {
            context,
            message: "state_root must not be empty".to_string(),
        });
    }
    Ok(())
}

pub(crate) fn write_json_atomically<T: Serialize>(
    target_path: &Path,
    value: &T,
    context: &'static str,
    stable_message: &'static str,
) -> CrabResult<()> {
    let temp_path = target_path.with_extension(format!("tmp-{}", std::process::id()));
    let encoded = serde_json::to_string_pretty(value).expect(stable_message);

    wrap_io(fs::write(&temp_path, encoded), context, &temp_path)?;
    wrap_io(fs::rename(&temp_path, target_path), context, target_path)
}

pub(crate) fn write_json_to_writer<T: Serialize>(
    writer: &mut dyn Write,
    path: &Path,
    context: &'static str,
    value: &T,
    stable_message: &'static str,
) -> CrabResult<()> {
    let encoded = serde_json::to_string(value).expect(stable_message);
    wrap_io(writer.write_all(encoded.as_bytes()), context, path)
}

pub(crate) fn read_text_file_if_present(
    path: &Path,
    context: &'static str,
    action: &'static str,
) -> CrabResult<Option<String>> {
    match fs::read_to_string(path) {
        Ok(content) => Ok(Some(content)),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(CrabError::Io {
            context,
            path: Some(path.to_string_lossy().to_string()),
            message: format!("{action}: {error}"),
        }),
    }
}

pub(crate) fn remove_file_allow_not_found(
    path: &Path,
    context: &'static str,
    action: &'static str,
) -> CrabResult<()> {
    remove_file_allow_not_found_with(path, context, action, |_| ())
}

pub(crate) fn remove_file_allow_not_found_with(
    path: &Path,
    context: &'static str,
    action: &'static str,
    before_remove: impl FnOnce(&Path),
) -> CrabResult<()> {
    before_remove(path);
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(CrabError::Io {
            context,
            path: Some(path.to_string_lossy().to_string()),
            message: format!("{action}: {error}"),
        }),
    }
}

pub(crate) fn wrap_io<T>(
    result: std::io::Result<T>,
    context: &'static str,
    path: &Path,
) -> CrabResult<T> {
    result.map_err(|error| CrabError::Io {
        context,
        path: Some(path.to_string_lossy().to_string()),
        message: error.to_string(),
    })
}
