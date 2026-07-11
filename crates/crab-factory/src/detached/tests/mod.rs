use std::fs;
use std::time::{Duration, Instant};

use super::*;

#[test]
fn command_spawn_starts_a_new_session_and_kill_reaps_it() {
    assert!(setsid_result(0, Error::other("unused")).is_ok());
    assert!(setsid_result(-1, Error::other("setsid failed")).is_err());
    let _ = set_new_session();
    let root = std::env::temp_dir().join(format!("crab-factory-detached-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).unwrap();
    let log = File::create(root.join("log")).unwrap();
    let environment = BTreeMap::new();
    let mut child = spawn(
        Path::new("/bin/sh"),
        &[OsString::from("-c"), OsString::from("sleep 30")],
        &environment,
        &root,
        &log,
    )
    .unwrap();
    let pid = child.id() as i32;
    assert_eq!(unsafe { libc::getsid(pid) }, pid);
    kill_and_reap(&mut child);
    assert!(child.try_wait().unwrap().is_some());
    let deadline = Instant::now() + Duration::from_secs(1);
    while unsafe { libc::kill(pid, 0) } == 0 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
    fs::remove_dir_all(root).unwrap();
}
