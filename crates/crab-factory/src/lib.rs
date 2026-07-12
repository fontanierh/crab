macro_rules! try_mapped {
    ($expression:expr, $error:ident => $mapped:expr) => {{
        match $expression {
            Ok(value) => value,
            Err($error) => return Err($mapped),
        }
    }};
}

macro_rules! require_some {
    ($expression:expr, $mapped:expr) => {{
        match $expression {
            Some(value) => value,
            None => return Err($mapped),
        }
    }};
}

mod cli;
mod config;
mod controls;
mod detached;
mod gitops;
mod launch;
mod manifest;
mod orchestrator;
mod pipeline;
mod preflight;
mod prompts;
mod rubric;
mod run_lock;
mod terminal;
mod workers;

#[cfg(test)]
#[path = "../tests/support/mod.rs"]
mod factory_test_support;

#[cfg(test)]
static FACTORY_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

use std::fmt::{Display, Formatter};
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use sha2::{Digest, Sha256};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

pub use cli::run_factory_cli;

static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq)]
struct FactoryError(String);

impl FactoryError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }

    fn context(self, context: &str) -> Self {
        Self(format!("{context}: {}", self.0))
    }

    fn io(action: &str, path: &Path, error: &std::io::Error) -> Self {
        Self(format!("could not {action} {}: {error}", path.display()))
    }
}

impl Display for FactoryError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for FactoryError {}

type FactoryResult<T> = Result<T, FactoryError>;

fn io_result<T>(result: std::io::Result<T>, action: &str, path: &Path) -> FactoryResult<T> {
    match result {
        Ok(value) => Ok(value),
        Err(error) => Err(FactoryError::io(action, path, &error)),
    }
}

fn result_context<T, E: Display>(result: Result<T, E>, context: &str) -> FactoryResult<T> {
    match result {
        Ok(value) => Ok(value),
        Err(error) => Err(FactoryError::new(format!("{context}: {error}"))),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn utc_now_rfc3339() -> FactoryResult<String> {
    result_context(
        OffsetDateTime::now_utc().format(&Rfc3339),
        "could not format UTC timestamp",
    )
}

fn create_secure_dir(path: &Path) -> FactoryResult<()> {
    let created = !path.exists();
    io_result(fs::create_dir_all(path), "create directory", path)?;
    if created {
        #[rustfmt::skip]
        io_result(fs::set_permissions(path, fs::Permissions::from_mode(0o700)), "set permissions on directory", path)?;
    }
    Ok(())
}

fn create_exclusive_dir(path: &Path) -> FactoryResult<()> {
    io_result(fs::create_dir(path), "reserve run artifact directory", path)
}

fn set_secure_dir_permissions(path: &Path) -> FactoryResult<()> {
    io_result(
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)),
        "set permissions on directory",
        path,
    )
}

fn open_private_file(path: &Path, append: bool) -> FactoryResult<std::fs::File> {
    let mut options = OpenOptions::new();
    options
        .create(true)
        .write(true)
        .mode(0o600)
        .append(append)
        .truncate(!append);
    let file = io_result(options.open(path), "open private file", path)?;
    #[rustfmt::skip]
    io_result(fs::set_permissions(path, fs::Permissions::from_mode(0o600)), "set permissions on private file", path)?;
    Ok(file)
}

fn write_new_file(path: &Path, bytes: &[u8], mode: u32) -> FactoryResult<()> {
    let mut file = io_result(
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(mode)
            .open(path),
        "create file",
        path,
    )?;
    io_result(file.write_all(bytes), "write file", path)?;
    io_result(file.sync_all(), "sync file", path)?;
    #[rustfmt::skip]
    io_result(fs::set_permissions(path, fs::Permissions::from_mode(mode)), "chmod file", path)?;
    Ok(())
}

fn atomic_write(path: &Path, bytes: &[u8]) -> FactoryResult<()> {
    let parent = require_some!(
        path.parent(),
        FactoryError::new(format!("file has no parent directory: {}", path.display()))
    );
    let file_name = require_some!(
        path.file_name().and_then(|value| value.to_str()),
        FactoryError::new(format!("invalid artifact path: {}", path.display()))
    );
    let suffix = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let temporary = parent.join(format!(".{file_name}.tmp-{}-{suffix}", std::process::id()));
    let result = (|| {
        write_new_file(&temporary, bytes, 0o600)?;
        io_result(fs::rename(&temporary, path), "atomically replace", path)?;
        #[rustfmt::skip]
        io_result(fs::set_permissions(path, fs::Permissions::from_mode(0o600)), "chmod file", path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn read_bytes(path: &Path, label: &str) -> FactoryResult<Vec<u8>> {
    let action = format!("read {label} at");
    let mut file = io_result(std::fs::File::open(path), &action, path)?;
    let mut bytes = Vec::new();
    io_result(file.read_to_end(&mut bytes), &action, path)?;
    Ok(bytes)
}

fn read_managed_bytes(path: &Path, label: &str, mode: u32) -> FactoryResult<Vec<u8>> {
    use std::os::unix::fs::OpenOptionsExt as _;

    let action = format!("read {label} at");
    let mut file = io_result(
        OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path),
        &action,
        path,
    )?;
    let metadata = io_result(file.metadata(), "inspect managed file", path)?;
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o777 != mode
    {
        return Err(FactoryError::new(format!(
            "unsafe managed file: {}",
            path.display()
        )));
    }
    let mut bytes = Vec::new();
    io_result(file.read_to_end(&mut bytes), &action, path)?;
    Ok(bytes)
}

fn required_utf8(bytes: &[u8], label: &str) -> FactoryResult<String> {
    let text = result_context(
        std::str::from_utf8(bytes),
        &format!("{label} is not valid UTF-8"),
    )?;
    if text.trim().is_empty() {
        return Err(FactoryError::new(format!("{label} is empty")));
    }
    Ok(text.to_string())
}

#[cfg(test)]
#[path = "lib/tests/mod.rs"]
mod tests;
