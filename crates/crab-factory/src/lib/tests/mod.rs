use std::ffi::OsString;
use std::fs;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::PermissionsExt;

use super::*;

fn test_dir(label: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!(
        "crab-factory-lib-{label}-{}-{}",
        std::process::id(),
        TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path).expect("test directory");
    path
}

#[test]
fn helpers_hash_validate_and_write_private_files() {
    assert_eq!(
        sha256_hex(b"abc"),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    assert!(utc_now_rfc3339().expect("timestamp").ends_with('Z'));
    assert_eq!(
        required_utf8(b"caf\xc3\xa9", "request").unwrap(),
        "caf\u{e9}"
    );
    assert!(required_utf8(b" \n", "request").is_err());
    assert!(required_utf8(&[0xff], "request").is_err());

    let root = test_dir("write");
    let secure = root.join("secure");
    create_secure_dir(&secure).unwrap();
    assert_eq!(
        fs::metadata(&secure).unwrap().permissions().mode() & 0o777,
        0o700
    );
    fs::set_permissions(&secure, fs::Permissions::from_mode(0o755)).unwrap();
    create_secure_dir(&secure).unwrap();
    assert_eq!(
        fs::metadata(&secure).unwrap().permissions().mode() & 0o777,
        0o755
    );
    let exclusive = secure.join("exclusive");
    create_exclusive_dir(&exclusive).unwrap();
    set_secure_dir_permissions(&exclusive).unwrap();
    assert_eq!(
        fs::metadata(&exclusive).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert!(create_exclusive_dir(&exclusive).is_err());
    let file = exclusive.join("value");
    write_new_file(&file, b"one", 0o400).unwrap();
    assert_eq!(read_bytes(&file, "value").unwrap(), b"one");
    assert!(write_new_file(&file, b"two", 0o600).is_err());
    atomic_write(&file, b"two").unwrap();
    assert_eq!(read_bytes(&file, "value").unwrap(), b"two");
    assert_eq!(
        fs::metadata(&file).unwrap().permissions().mode() & 0o777,
        0o600
    );

    let append = exclusive.join("append");
    open_private_file(&append, false)
        .unwrap()
        .write_all(b"a")
        .unwrap();
    open_private_file(&append, true)
        .unwrap()
        .write_all(b"b")
        .unwrap();
    assert_eq!(fs::read(&append).unwrap(), b"ab");
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn error_context_and_atomic_write_errors_are_actionable() {
    assert_eq!(
        FactoryError::new("bad")
            .context("while testing")
            .to_string(),
        "while testing: bad"
    );
    let root = test_dir("errors");
    assert!(atomic_write(Path::new("/"), b"x").is_err());
    assert!(atomic_write(&root.join("missing").join("x"), b"x").is_err());
    let invalid = root.join(OsString::from_vec(vec![0xff]));
    assert!(atomic_write(&invalid, b"x").is_err());
    let directory_target = root.join("directory-target");
    fs::create_dir(&directory_target).unwrap();
    assert!(atomic_write(&directory_target, b"x").is_err());
    assert!(read_bytes(&root.join("absent"), "missing").is_err());
    assert!(read_bytes(&root, "directory").is_err());
    let blocker = root.join("blocker");
    fs::write(&blocker, "file").unwrap();
    assert!(create_secure_dir(&blocker.join("child")).is_err());
    assert!(open_private_file(&root, false).is_err());
    assert!(write_new_file(&root.join("missing/child"), b"x", 0o600).is_err());
    fs::remove_dir_all(root).unwrap();
}
