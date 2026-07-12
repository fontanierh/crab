use std::fs::{self, OpenOptions};
use std::io::Read;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::{
    io_result, read_managed_bytes, result_context, write_new_file, FactoryError, FactoryResult,
};

const RUN_MARKER_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RunMarker {
    pub(crate) run_id: String,
    pub(crate) request_sha256: String,
    schema_version: u32,
}

pub(crate) struct RunLock {
    file: fs::File,
}

#[derive(Debug)]
pub(crate) enum RunLockError {
    Busy,
    Other(FactoryError),
}

impl RunLock {
    pub(crate) fn initialize(
        run_dir: &Path,
        run_id: &str,
        request_sha256: &str,
    ) -> FactoryResult<()> {
        let marker = RunMarker {
            run_id: run_id.to_string(),
            request_sha256: request_sha256.to_string(),
            schema_version: RUN_MARKER_SCHEMA_VERSION,
        };
        #[rustfmt::skip]
        let mut bytes = result_context(serde_json::to_vec(&marker), "could not serialize prepared-run marker")?;
        bytes.push(b'\n');
        write_new_file(&run_dir.join(".lock"), &bytes, 0o600)
    }

    pub(crate) fn marker(run_dir: &Path) -> FactoryResult<RunMarker> {
        let path = run_dir.join(".lock");
        let marker: RunMarker = result_context(
            serde_json::from_slice(&read_managed_bytes(&path, "prepared-run marker", 0o600)?),
            &format!("invalid prepared-run marker at {}", path.display()),
        )?;
        if marker.schema_version != RUN_MARKER_SCHEMA_VERSION
            || marker.run_id.is_empty()
            || marker.request_sha256.len() != 64
            || !marker
                .request_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(FactoryError::new(format!(
                "invalid prepared-run marker at {}",
                path.display()
            )));
        }
        Ok(marker)
    }

    pub(crate) fn acquire(run_dir: &Path) -> Result<Self, RunLockError> {
        let path = run_dir.join(".lock");
        let mut file = io_result(
            OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW)
                .open(&path),
            "open run lock",
            &path,
        )
        .map_err(RunLockError::Other)?;
        let metadata =
            io_result(file.metadata(), "inspect run lock", &path).map_err(RunLockError::Other)?;
        if !metadata.is_file()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.mode() & 0o777 != 0o600
        {
            return Err(RunLockError::Other(FactoryError::new(format!(
                "unsafe run lock: {}",
                path.display()
            ))));
        }
        let mut marker_bytes = Vec::new();
        io_result(file.read_to_end(&mut marker_bytes), "read run lock", &path)
            .map_err(RunLockError::Other)?;
        result_context::<RunMarker, _>(serde_json::from_slice(&marker_bytes), "invalid run lock")
            .map_err(RunLockError::Other)?;
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result != 0 {
            let error = std::io::Error::last_os_error();
            return Err(classify_lock_error(&path, &error));
        }
        Ok(Self { file })
    }
}

fn classify_lock_error(path: &Path, error: &std::io::Error) -> RunLockError {
    if error
        .raw_os_error()
        .is_some_and(|code| code == libc::EWOULDBLOCK || code == libc::EAGAIN)
    {
        RunLockError::Busy
    } else {
        RunLockError::Other(FactoryError::io("lock run", path, error))
    }
}

#[cfg(test)]
#[path = "run_lock/tests/mod.rs"]
mod tests;

impl Drop for RunLock {
    fn drop(&mut self) {
        let _ = unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
    }
}
