use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs;
use std::path::PathBuf;

use crab_core::{CrabError, CrabResult, LogicalSession};
use serde::{Deserialize, Serialize};

use crate::helpers::{
    read_json_with_backup, session_file_name, wrap_io, write_logical_session_atomically,
    write_session_index_atomically,
};

const INDEX_FILE_NAME: &str = "sessions.index.json";
const SESSIONS_DIR_NAME: &str = "sessions";

#[derive(Debug, Clone)]
pub struct SessionStore {
    root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct SessionIndex {
    pub(crate) sessions: BTreeMap<String, SessionIndexEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SessionIndexEntry {
    pub(crate) file_name: String,
    pub(crate) last_activity_epoch_ms: u64,
}

impl SessionStore {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn upsert_session(&self, session: &LogicalSession) -> CrabResult<()> {
        self.ensure_layout()?;

        let session_file_name = session_file_name(&session.id);
        let session_path = self.sessions_dir().join(&session_file_name);
        write_logical_session_atomically(&session_path, session, "session_write")?;

        let mut index = self.load_index()?;
        index.sessions.insert(
            session.id.clone(),
            SessionIndexEntry {
                file_name: session_file_name,
                last_activity_epoch_ms: session.last_activity_epoch_ms,
            },
        );
        self.persist_index(&index)
    }

    pub fn get_session(&self, session_id: &str) -> CrabResult<Option<LogicalSession>> {
        self.ensure_layout()?;
        let session_path = self.sessions_dir().join(session_file_name(session_id));
        read_json_with_backup(&session_path, "session_read")
    }

    pub fn list_session_ids(&self) -> CrabResult<Vec<String>> {
        self.ensure_layout()?;
        let index = self.load_index()?;
        Ok(index.sessions.keys().cloned().collect())
    }

    fn ensure_layout(&self) -> CrabResult<()> {
        wrap_io(
            fs::create_dir_all(&self.root),
            "session_store_layout",
            &self.root,
        )?;
        let sessions_dir = self.sessions_dir();
        wrap_io(
            fs::create_dir_all(&sessions_dir),
            "session_store_layout",
            &sessions_dir,
        )?;
        Ok(())
    }

    fn sessions_dir(&self) -> PathBuf {
        self.root.join(SESSIONS_DIR_NAME)
    }

    fn index_path(&self) -> PathBuf {
        self.root.join(INDEX_FILE_NAME)
    }

    fn load_index(&self) -> CrabResult<SessionIndex> {
        let index_path = self.index_path();
        match read_json_with_backup::<SessionIndex>(&index_path, "session_index_read") {
            Ok(Some(index)) => Ok(index),
            Ok(None) => Ok(SessionIndex::default()),
            Err(CrabError::CorruptData { .. }) => {
                let rebuilt = self.rebuild_index_from_session_files()?;
                self.persist_index(&rebuilt)?;
                Ok(rebuilt)
            }
            Err(error) => Err(error),
        }
    }

    fn persist_index(&self, index: &SessionIndex) -> CrabResult<()> {
        write_session_index_atomically(&self.index_path(), index, "session_index_write")
    }

    fn rebuild_index_from_session_files(&self) -> CrabResult<SessionIndex> {
        let sessions_dir = self.sessions_dir();
        let mut sessions = BTreeMap::new();
        let entries = wrap_io(
            fs::read_dir(&sessions_dir),
            "session_index_rebuild",
            &sessions_dir,
        )?;

        for entry in entries.flatten() {
            let file_name = entry.file_name().to_string_lossy().into_owned();
            let path = entry.path();
            if path.extension() != Some(OsStr::new("json")) {
                continue;
            }
            let maybe_session =
                match read_json_with_backup::<LogicalSession>(&path, "session_rebuild_read") {
                    Ok(session) => session,
                    Err(CrabError::CorruptData { .. }) => None,
                    Err(error) => return Err(error),
                };
            if let Some(session) = maybe_session {
                sessions.insert(
                    session.id.clone(),
                    SessionIndexEntry {
                        file_name,
                        last_activity_epoch_ms: session.last_activity_epoch_ms,
                    },
                );
            }
        }

        Ok(SessionIndex { sessions })
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use crab_core::CrabError;

    use crate::helpers::{read_json_file, session_file_name};
    use crate::test_support::{
        assert_io_context, cleanup, corrupt_index_file, root_as_file, sample_session,
        set_unix_mode, temp_root,
    };

    use super::{SessionIndex, SessionStore};

    #[test]
    fn upsert_and_load_round_trip() {
        let root = temp_root("upsert-and-load");
        let store = SessionStore::new(&root);
        let session = sample_session("discord:channel:abc", 101);

        store
            .upsert_session(&session)
            .expect("session upsert should succeed");

        let loaded = store
            .get_session(&session.id)
            .expect("session read should succeed")
            .expect("session should exist");
        assert_eq!(loaded, session);

        let ids = store
            .list_session_ids()
            .expect("listing session ids should succeed");
        assert_eq!(ids, vec![session.id]);

        cleanup(&root);
    }

    #[test]
    fn missing_session_returns_none() {
        let root = temp_root("missing-session");
        let store = SessionStore::new(&root);

        let loaded = store
            .get_session("discord:channel:missing")
            .expect("missing lookup should not error");
        assert!(loaded.is_none());

        cleanup(&root);
    }

    #[test]
    fn corrupted_primary_session_recovers_from_backup() {
        let root = temp_root("recover-from-backup");
        let store = SessionStore::new(&root);

        let original = sample_session("discord:channel:abc", 200);
        let updated = sample_session("discord:channel:abc", 300);

        store
            .upsert_session(&original)
            .expect("initial session write should succeed");
        store
            .upsert_session(&updated)
            .expect("second session write should succeed");

        let session_path = root
            .join("sessions")
            .join(session_file_name("discord:channel:abc"));
        fs::write(&session_path, b"{ invalid json")
            .expect("test should be able to corrupt session file");

        let recovered = store
            .get_session("discord:channel:abc")
            .expect("backup recovery should succeed")
            .expect("session should still be readable");
        assert_eq!(recovered, original);

        cleanup(&root);
    }

    #[test]
    fn corruption_without_backup_returns_error() {
        let root = temp_root("corrupt-without-backup");
        let store = SessionStore::new(&root);
        let session = sample_session("discord:channel:single-write", 50);

        store
            .upsert_session(&session)
            .expect("initial session write should succeed");

        let session_path = root
            .join("sessions")
            .join(session_file_name("discord:channel:single-write"));
        fs::write(&session_path, b"{ invalid json")
            .expect("test should be able to corrupt session file");

        let error = store
            .get_session("discord:channel:single-write")
            .expect_err("missing backup should return corruption error");
        assert_eq!(
            error,
            CrabError::CorruptData {
                context: "session_read",
                path: session_path.display().to_string(),
            }
        );

        cleanup(&root);
    }

    #[test]
    fn corrupted_index_rebuilds_from_session_files() {
        let root = temp_root("index-rebuild");
        let store = SessionStore::new(&root);
        let first = sample_session("discord:channel:first", 1);
        let second = sample_session("discord:channel:second", 2);

        store
            .upsert_session(&first)
            .expect("first session write should succeed");
        store
            .upsert_session(&second)
            .expect("second session write should succeed");

        let index_path = corrupt_index_file(&root);
        fs::write(root.join("sessions").join("ignore.txt"), b"ignore")
            .expect("test should be able to create non-json file");
        fs::write(
            root.join("sessions").join("corrupt.json"),
            b"{ invalid json",
        )
        .expect("test should be able to create corrupt json file");

        let ids = store
            .list_session_ids()
            .expect("corrupt index should rebuild from state files");
        assert_eq!(
            ids,
            vec![
                "discord:channel:first".to_string(),
                "discord:channel:second".to_string()
            ]
        );

        let rebuilt_index: SessionIndex =
            read_json_file(&index_path, "session_index_read").expect("rebuilt index should parse");
        assert_eq!(rebuilt_index.sessions.len(), 2);

        cleanup(&root);
    }

    #[test]
    fn rebuild_index_propagates_io_errors_from_session_entries() {
        let root = temp_root("index-rebuild-io-error");
        let store = SessionStore::new(&root);
        store
            .upsert_session(&sample_session("discord:channel:valid", 5))
            .expect("session write should succeed");
        let _ = corrupt_index_file(&root);
        fs::create_dir_all(root.join("sessions").join("broken.json"))
            .expect("test should be able to create directory with json extension");

        let error = store
            .list_session_ids()
            .expect_err("rebuild should propagate io errors from session entries");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "session_rebuild_read",
                ..
            }
        ));

        cleanup(&root);
    }

    #[test]
    fn corrupted_primary_and_backup_returns_corruption_error() {
        let root = temp_root("corrupt-primary-and-backup");
        let store = SessionStore::new(&root);

        let original = sample_session("discord:channel:backup-corrupt", 10);
        let updated = sample_session("discord:channel:backup-corrupt", 11);
        store
            .upsert_session(&original)
            .expect("initial write should succeed");
        store
            .upsert_session(&updated)
            .expect("second write should succeed");

        let session_path = root
            .join("sessions")
            .join(session_file_name("discord:channel:backup-corrupt"));
        let backup_path = PathBuf::from(format!("{}.bak", session_path.display()));
        fs::write(&session_path, b"{ invalid json")
            .expect("test should be able to corrupt primary session file");
        fs::write(&backup_path, b"{ invalid json")
            .expect("test should be able to corrupt backup session file");

        let error = store
            .get_session("discord:channel:backup-corrupt")
            .expect_err("double corruption should return corruption error");
        assert_eq!(
            error,
            CrabError::CorruptData {
                context: "session_read",
                path: session_path.display().to_string(),
            }
        );

        cleanup(&root);
    }

    #[test]
    fn atomic_write_leaves_no_temp_files() {
        let root = temp_root("no-temp-files");
        let store = SessionStore::new(&root);
        let session = sample_session("discord:channel:temp-check", 3);

        store
            .upsert_session(&session)
            .expect("session write should succeed");

        let root_entries: Vec<String> = fs::read_dir(&root)
            .expect("root directory should exist")
            .map(|entry| {
                entry
                    .expect("entry should be readable")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        assert!(root_entries.iter().all(|name| !name.contains(".tmp-")));

        let sessions_entries: Vec<String> = fs::read_dir(root.join("sessions"))
            .expect("sessions directory should exist")
            .map(|entry| {
                entry
                    .expect("entry should be readable")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        assert!(sessions_entries.iter().all(|name| !name.contains(".tmp-")));

        cleanup(&root);
    }

    #[test]
    fn io_error_from_session_read_is_returned() {
        let root = temp_root("session-read-io-error");
        let store = SessionStore::new(&root);
        store
            .list_session_ids()
            .expect("layout initialization should succeed");

        let session_path = root
            .join("sessions")
            .join(session_file_name("discord:channel:io"));
        fs::create_dir_all(&session_path).expect("test should create a directory at session path");

        let error = store
            .get_session("discord:channel:io")
            .expect_err("directory path should trigger io read failure");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "session_read",
                path: Some(ref path),
                ..
            } if path == &session_path.display().to_string()
        ));

        cleanup(&root);
    }

    #[test]
    fn io_error_from_index_read_is_returned() {
        let root = temp_root("index-read-io-error");
        let store = SessionStore::new(&root);
        store
            .list_session_ids()
            .expect("layout initialization should succeed");

        let index_path = root.join("sessions.index.json");
        fs::create_dir_all(&index_path).expect("test should create a directory at index path");

        let error = store
            .list_session_ids()
            .expect_err("directory index path should trigger io read failure");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "session_index_read",
                path: Some(ref path),
                ..
            } if path == &index_path.display().to_string()
        ));

        cleanup(&root);
    }

    #[test]
    fn upsert_propagates_layout_error() {
        let root = root_as_file("upsert-layout-error");
        let store = SessionStore::new(&root);
        let error = store
            .upsert_session(&sample_session("discord:channel:a", 1))
            .expect_err("root path as file should fail layout creation");
        assert_io_context(error, "session_store_layout");
        let _ = fs::remove_file(&root);
    }

    #[test]
    fn get_session_propagates_layout_error() {
        let root = root_as_file("get-layout-error");
        let store = SessionStore::new(&root);
        let error = store
            .get_session("discord:channel:a")
            .expect_err("root path as file should fail layout creation");
        assert_io_context(error, "session_store_layout");
        let _ = fs::remove_file(&root);
    }

    #[test]
    fn list_session_ids_propagates_layout_error() {
        let root = root_as_file("list-layout-error");
        let store = SessionStore::new(&root);
        let error = store
            .list_session_ids()
            .expect_err("root path as file should fail layout creation");
        assert_io_context(error, "session_store_layout");
        let _ = fs::remove_file(&root);
    }

    #[test]
    fn list_session_ids_propagates_sessions_directory_layout_error() {
        let root = temp_root("sessions-layout-error");
        fs::write(root.join("sessions"), b"not-a-directory")
            .expect("test should be able to block sessions dir path");
        let store = SessionStore::new(&root);
        let error = store
            .list_session_ids()
            .expect_err("sessions path as file should fail layout creation");
        assert_io_context(error, "session_store_layout");
        cleanup(&root);
    }

    #[test]
    fn upsert_propagates_session_write_error() {
        let root = temp_root("upsert-write-error");
        let store = SessionStore::new(&root);
        store
            .list_session_ids()
            .expect("layout initialization should succeed");

        let session_id = "discord:channel:write-error";
        let session_path = root.join("sessions").join(session_file_name(session_id));
        fs::create_dir_all(&session_path)
            .expect("test should create directory at session file path");

        let error = store
            .upsert_session(&sample_session(session_id, 2))
            .expect_err("session write should fail when target is directory");
        assert_io_context(error, "session_write");

        cleanup(&root);
    }

    #[test]
    fn upsert_propagates_index_load_error() {
        let root = temp_root("upsert-index-load-error");
        let store = SessionStore::new(&root);
        fs::create_dir_all(root.join("sessions.index.json"))
            .expect("test should create directory at index path");

        let error = store
            .upsert_session(&sample_session("discord:channel:index-load", 3))
            .expect_err("index read should fail when index path is directory");
        assert_io_context(error, "session_index_read");

        cleanup(&root);
    }

    #[cfg(unix)]
    #[test]
    fn list_session_ids_propagates_rebuild_persist_error() {
        let root = temp_root("rebuild-persist-error");
        let store = SessionStore::new(&root);
        store
            .upsert_session(&sample_session("discord:channel:persist", 4))
            .expect("session write should succeed");

        let _ = corrupt_index_file(&root);

        set_unix_mode(&root, 0o500);
        let error = store
            .list_session_ids()
            .expect_err("rebuild persist should fail on read-only root");
        assert_io_context(error, "session_index_write");
        set_unix_mode(&root, 0o700);

        cleanup(&root);
    }

    #[cfg(unix)]
    #[test]
    fn list_session_ids_propagates_rebuild_read_dir_error() {
        let root = temp_root("rebuild-read-dir-error");
        let store = SessionStore::new(&root);
        store
            .upsert_session(&sample_session("discord:channel:read-dir", 5))
            .expect("session write should succeed");

        let _ = corrupt_index_file(&root);

        let sessions_dir = root.join("sessions");
        set_unix_mode(&sessions_dir, 0o000);
        let error = store
            .list_session_ids()
            .expect_err("rebuild should fail when sessions dir is unreadable");
        assert_io_context(error, "session_index_rebuild");
        set_unix_mode(&sessions_dir, 0o700);

        cleanup(&root);
    }
}
