use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs::File;
use std::io::Error;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Child, Command, Stdio};

pub(crate) fn spawn(
    executable: &Path,
    arguments: &[OsString],
    environment: &BTreeMap<OsString, OsString>,
    cwd: &Path,
    log: &File,
) -> std::io::Result<Child> {
    let stdout = log.try_clone()?;
    let stderr = log.try_clone()?;
    let mut command = Command::new(executable);
    command
        .args(arguments)
        .current_dir(cwd)
        .env_clear()
        .envs(environment)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    // A function item keeps coverage instrumentation from inventing an
    // untestable closure function around the pre-exec hook.
    unsafe {
        command.pre_exec(set_new_session);
    }
    command.spawn()
}

fn set_new_session() -> std::io::Result<()> {
    setsid_result(unsafe { libc::setsid() }, Error::last_os_error())
}

fn setsid_result(result: i32, error: Error) -> std::io::Result<()> {
    if result == -1 {
        Err(error)
    } else {
        Ok(())
    }
}

pub(crate) fn kill_and_reap(child: &mut Child) {
    let process_group = child.id() as i32;
    let _ = unsafe { libc::kill(-process_group, libc::SIGKILL) };
    let _ = child.kill();
    let _ = child.wait();
}

#[cfg(test)]
#[path = "detached/tests/mod.rs"]
mod tests;
