use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crab_core::{
    build_memory_flush_prompt, execute_rotation_sequence, finalize_hidden_memory_flush, get_memory,
    search_memory, CheckpointTurnArtifact, CheckpointTurnDocument, CrabError, CrabResult,
    MemoryFlushAck, MemoryGetInput, MemorySearchInput, RotationSequenceRuntime,
};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempWorkspace {
    root: PathBuf,
}

impl TempWorkspace {
    fn new(label: &str) -> Self {
        let suffix = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "crab-memory-continuity-{label}-{}-{suffix}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("workspace root should be creatable");
        Self { root }
    }

    fn path(&self) -> &Path {
        &self.root
    }

    fn write(&self, relative_path: &str, content: &str) {
        let absolute_path = self.root.join(relative_path);
        let parent = absolute_path
            .parent()
            .expect("joined path should have a parent");
        fs::create_dir_all(parent).expect("parent directory should be creatable");
        fs::write(absolute_path, content).expect("fixture file should be writable");
    }
}

impl Drop for TempWorkspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

struct ContinuityRuntime {
    workspace_root: PathBuf,
    user_scope: String,
    calls: Vec<&'static str>,
}

impl ContinuityRuntime {
    fn new(workspace_root: &Path, user_scope: &str) -> Self {
        Self {
            workspace_root: workspace_root.to_path_buf(),
            user_scope: user_scope.to_string(),
            calls: Vec::new(),
        }
    }
}

impl RotationSequenceRuntime for ContinuityRuntime {
    fn run_hidden_memory_flush(&mut self) -> CrabResult<()> {
        self.calls.push("memory_flush");

        let _flush_prompt = build_memory_flush_prompt("cycle-1")?;
        let flush_outcome = finalize_hidden_memory_flush("MEMORY_FLUSH_DONE")?;
        if flush_outcome.ack != MemoryFlushAck::Done {
            return Err(CrabError::InvariantViolation {
                context: "memory_flush_continuity_test",
                message: "flush ack must be MEMORY_FLUSH_DONE".to_string(),
            });
        }

        let target = self
            .workspace_root
            .join("memory")
            .join("users")
            .join(&self.user_scope)
            .join("2026-02-10.md");
        let Some(parent) = target.parent() else {
            return Err(CrabError::InvariantViolation {
                context: "memory_flush_continuity_test",
                message: "memory file path must have a parent directory".to_string(),
            });
        };
        fs::create_dir_all(parent)
            .map_err(|error| io_error("memory_flush_continuity_test", parent, error))?;
        fs::write(
            &target,
            "owner_timezone=America/Los_Angeles\nlane_queue_model=fifo\n",
        )
        .map_err(|error| io_error("memory_flush_continuity_test", &target, error))?;

        Ok(())
    }

    fn run_hidden_checkpoint_turn(&mut self) -> CrabResult<CheckpointTurnDocument> {
        self.calls.push("checkpoint_turn");
        Ok(checkpoint_document("checkpoint after memory flush"))
    }

    fn build_fallback_checkpoint(&mut self) -> CrabResult<CheckpointTurnDocument> {
        self.calls.push("checkpoint_fallback");
        Ok(checkpoint_document("fallback checkpoint"))
    }

    fn persist_checkpoint(&mut self, _checkpoint: &CheckpointTurnDocument) -> CrabResult<String> {
        self.calls.push("checkpoint_persist");
        Ok("ckpt-continuity".to_string())
    }

    fn end_physical_session(&mut self) -> CrabResult<()> {
        self.calls.push("session_end");
        Ok(())
    }

    fn clear_active_physical_session(&mut self) -> CrabResult<()> {
        self.calls.push("session_clear");
        Ok(())
    }
}

fn checkpoint_document(summary: &str) -> CheckpointTurnDocument {
    CheckpointTurnDocument {
        summary: summary.to_string(),
        decisions: vec!["persisted memory flush output".to_string()],
        open_questions: vec!["none".to_string()],
        next_actions: vec!["continue turn processing".to_string()],
        artifacts: vec![CheckpointTurnArtifact {
            path: "memory/users/222/2026-02-10.md".to_string(),
            note: "memory flush output".to_string(),
        }],
    }
}

fn io_error(context: &'static str, path: &Path, error: std::io::Error) -> CrabError {
    CrabError::InvariantViolation {
        context,
        message: format!("io failure on {}: {error}", path.to_string_lossy()),
    }
}

#[test]
fn flush_rotation_writes_are_discoverable_via_cli_and_native_lookup_paths() {
    let workspace = TempWorkspace::new("flush-cli-continuity");
    workspace.write("MEMORY.md", "curated memory\n");
    workspace.write("memory/global/2026-02-10.md", "global note\n");

    let mut runtime = ContinuityRuntime::new(workspace.path(), "222");
    let outcome =
        execute_rotation_sequence(&mut runtime).expect("rotation sequence should succeed");
    assert_eq!(outcome.checkpoint_id, "ckpt-continuity");
    assert!(!outcome.used_fallback_checkpoint);
    assert_eq!(
        runtime.calls,
        vec![
            "memory_flush",
            "checkpoint_turn",
            "checkpoint_persist",
            "session_end",
            "session_clear",
        ]
    );

    let search_input = MemorySearchInput::with_defaults(
        workspace.path(),
        "222",
        true,
        "owner_timezone America/Los_Angeles",
    );
    let search_results = search_memory(&search_input).expect("memory search should succeed");
    let search_hit = search_results
        .iter()
        .find(|entry| entry.path == "memory/users/222/2026-02-10.md")
        .expect("search should return flushed memory file");
    assert!(search_hit
        .snippet
        .contains("owner_timezone=America/Los_Angeles"));

    let get_input = MemoryGetInput::with_defaults(
        workspace.path(),
        "222",
        true,
        "memory/users/222/2026-02-10.md",
        1,
        Some(1),
    );
    let get_result = get_memory(&get_input)
        .expect("memory get should succeed")
        .expect("flushed file should exist");
    assert_eq!(get_result.path, "memory/users/222/2026-02-10.md");
    assert_eq!(get_result.start_line, 1);
    assert_eq!(get_result.end_line, 1);
    assert_eq!(get_result.content, "owner_timezone=America/Los_Angeles");

    let native_file = workspace.path().join("memory/users/222/2026-02-10.md");
    let native_read = fs::read_to_string(&native_file).expect("native file read should succeed");
    let native_grep_hits = native_read
        .lines()
        .enumerate()
        .filter_map(|(index, line)| {
            if line.contains("owner_timezone") {
                Some((index + 1, line.to_string()))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(
        native_grep_hits,
        vec![(1, "owner_timezone=America/Los_Angeles".to_string())]
    );
}
