use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use super::*;

#[test]
fn lock_errors_distinguish_contention_from_other_failures() {
    assert!(matches!(
        classify_lock_error(
            Path::new("lock"),
            &std::io::Error::from_raw_os_error(libc::EWOULDBLOCK),
        ),
        RunLockError::Busy
    ));
    match classify_lock_error(
        Path::new("lock"),
        &std::io::Error::from_raw_os_error(libc::EPERM),
    ) {
        RunLockError::Other(error) => assert!(error.to_string().contains("lock run")),
        RunLockError::Busy => panic!("EPERM is not lock contention"),
    }
}

#[test]
fn reservation_marker_is_typed_and_lock_acquisition_never_creates_it() {
    let root = std::env::temp_dir().join(format!("crab-factory-lock-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).unwrap();
    let hash = "a".repeat(64);
    RunLock::initialize(&root, "run-id", &hash).unwrap();
    assert_eq!(
        RunLock::marker(&root).unwrap(),
        RunMarker {
            run_id: "run-id".to_string(),
            request_sha256: hash,
            schema_version: RUN_MARKER_SCHEMA_VERSION,
        }
    );
    let first = RunLock::acquire(&root).unwrap();
    assert!(matches!(RunLock::acquire(&root), Err(RunLockError::Busy)));
    drop(first);
    fs::set_permissions(root.join(".lock"), fs::Permissions::from_mode(0o644)).unwrap();
    match RunLock::acquire(&root) {
        Err(RunLockError::Other(error)) => assert!(error.to_string().contains("unsafe run lock")),
        _ => panic!("wrong-mode run lock was accepted"),
    }
    fs::set_permissions(root.join(".lock"), fs::Permissions::from_mode(0o600)).unwrap();

    let arbitrary = root.join("arbitrary");
    fs::create_dir(&arbitrary).unwrap();
    assert!(RunLock::acquire(&arbitrary).is_err());
    assert!(!arbitrary.join(".lock").exists());
    for marker in [
        RunMarker {
            run_id: "run-id".to_string(),
            request_sha256: "a".repeat(64),
            schema_version: 2,
        },
        RunMarker {
            run_id: String::new(),
            request_sha256: "a".repeat(64),
            schema_version: RUN_MARKER_SCHEMA_VERSION,
        },
        RunMarker {
            run_id: "run-id".to_string(),
            request_sha256: "short".to_string(),
            schema_version: RUN_MARKER_SCHEMA_VERSION,
        },
        RunMarker {
            run_id: "run-id".to_string(),
            request_sha256: "z".repeat(64),
            schema_version: RUN_MARKER_SCHEMA_VERSION,
        },
    ] {
        fs::write(root.join(".lock"), serde_json::to_vec(&marker).unwrap()).unwrap();
        assert!(RunLock::marker(&root).is_err());
    }
    fs::write(root.join(".lock"), "not json").unwrap();
    assert!(RunLock::marker(&root).is_err());
    fs::remove_dir_all(root).unwrap();
}
