use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::config::{
    ToolPaths, ToolVersions, CLAUDE_MODEL, CODEX_MODEL, NESTED_AGENTS_ENABLED, REASONING_EFFORT,
    WORKER_HOST_PERMISSIONS, WORKER_NETWORK_ACCESS, WORKER_SANDBOX,
};
use crate::rubric::{
    THERMO_SKILL_COMMIT, THERMO_SKILL_MANIFEST_PATH, THERMO_SKILL_SHA256, THERMO_SKILL_SOURCE,
};
use crate::{
    atomic_write, read_bytes, result_context, utc_now_rfc3339, FactoryError, FactoryResult,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct LaunchRecord {
    pub(crate) run_id: String,
    pub(crate) mode: String,
    pub(crate) queued_at: String,
    pub(crate) source_prompt: PathBuf,
    pub(crate) request_sha256: String,
    pub(crate) repo: PathBuf,
    pub(crate) base_ref: String,
    pub(crate) base_sha: String,
    pub(crate) source_was_dirty: bool,
    pub(crate) allow_dirty_source: bool,
    pub(crate) additional_review_rounds: u32,
    pub(crate) agent_timeout_seconds: u64,
    pub(crate) artifact_root: PathBuf,
    pub(crate) worktree_root: PathBuf,
    pub(crate) worktree: PathBuf,
    pub(crate) branch: String,
    pub(crate) launch_mode: Option<String>,
    pub(crate) launched_pid: Option<u32>,
    pub(crate) proc_name: String,
    pub(crate) launcher: Option<PathBuf>,
}

impl LaunchRecord {
    pub(crate) fn write(&self, path: &Path) -> FactoryResult<()> {
        atomic_write(path, &json_bytes(self)?)
    }

    pub(crate) fn read(path: &Path) -> FactoryResult<Self> {
        result_context(
            serde_json::from_slice(&read_bytes(path, "launch record")?),
            &format!("invalid launch record at {}", path.display()),
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct Manifest {
    pub(crate) schema_version: u32,
    pub(crate) run_id: String,
    pub(crate) status: String,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) request: PathBuf,
    pub(crate) request_sha256: String,
    pub(crate) repo: PathBuf,
    pub(crate) base_ref: String,
    pub(crate) base_sha: String,
    pub(crate) source_was_dirty: bool,
    pub(crate) branch: String,
    pub(crate) worktree: PathBuf,
    pub(crate) additional_review_rounds: u32,
    pub(crate) maximum_review_rounds: u32,
    pub(crate) agent_timeout_seconds: u64,
    pub(crate) models: ModelSet,
    pub(crate) worker_policy: WorkerPolicy,
    pub(crate) thermonuclear_skill: ThermonuclearSkill,
    pub(crate) tool_paths: ToolPaths,
    pub(crate) tool_versions: ToolVersions,
    pub(crate) agents: BTreeMap<String, AgentRecord>,
    pub(crate) cohorts: Vec<CohortRecord>,
    pub(crate) events: Vec<Value>,
    pub(crate) completed_review_rounds: Option<u32>,
    pub(crate) normal_review_outcome: Option<String>,
    pub(crate) thermonuclear_verdict: Option<String>,
    pub(crate) thermonuclear_addressed: Option<bool>,
    pub(crate) outcome: Option<String>,
    pub(crate) error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ModelSet {
    pub(crate) claude: ClaudeModel,
    pub(crate) codex: CodexModel,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ClaudeModel {
    pub(crate) model: String,
    pub(crate) effort: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct CodexModel {
    pub(crate) model: String,
    pub(crate) reasoning_effort: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct WorkerPolicy {
    pub(crate) host_permissions: String,
    pub(crate) sandbox: String,
    pub(crate) network_access: bool,
    pub(crate) nested_agents_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ThermonuclearSkill {
    pub(crate) path: String,
    pub(crate) sha256: String,
    pub(crate) source: String,
    pub(crate) source_commit: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AgentRecord {
    pub(crate) provider: String,
    pub(crate) command: Vec<String>,
    pub(crate) sandbox: String,
    pub(crate) permission_mode: String,
    pub(crate) network_access: bool,
    pub(crate) prompt_sha256: String,
    pub(crate) status: String,
    pub(crate) started_at: String,
    pub(crate) finished_at: Option<String>,
    pub(crate) output: PathBuf,
    pub(crate) log: PathBuf,
    pub(crate) returncode: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct CohortRecord {
    pub(crate) name: String,
    pub(crate) members: Vec<String>,
    pub(crate) prompt: PathBuf,
    pub(crate) prompt_sha256: String,
}

#[derive(Debug)]
pub(crate) struct Journal {
    path: PathBuf,
    data: Mutex<Manifest>,
}

impl Journal {
    pub(crate) fn create(path: PathBuf, manifest: Manifest) -> FactoryResult<Self> {
        let journal = Self {
            path,
            data: Mutex::new(manifest),
        };
        journal.flush()?;
        Ok(journal)
    }

    pub(crate) fn load(path: PathBuf) -> FactoryResult<Self> {
        let data = result_context(
            serde_json::from_slice(&read_bytes(&path, "run manifest")?),
            &format!("invalid run manifest at {}", path.display()),
        )?;
        Ok(Self {
            path,
            data: Mutex::new(data),
        })
    }

    pub(crate) fn snapshot(&self) -> FactoryResult<Manifest> {
        Ok(self.lock()?.clone())
    }

    pub(crate) fn set_running(&self) -> FactoryResult<()> {
        self.mutate(|manifest, now| {
            manifest.status = "running".to_string();
            manifest.updated_at = now;
        })
    }

    pub(crate) fn event(&self, event: &str, values: Value) -> FactoryResult<()> {
        self.mutate(|manifest, now| {
            let mut entry = match values {
                Value::Object(object) => object,
                _ => Map::new(),
            };
            entry.insert("at".to_string(), Value::String(now.clone()));
            entry.insert("event".to_string(), Value::String(event.to_string()));
            manifest.events.push(Value::Object(entry));
            manifest.updated_at = now;
        })
    }

    pub(crate) fn register_cohort(&self, cohort: CohortRecord) -> FactoryResult<()> {
        self.mutate(|manifest, now| {
            manifest.cohorts.push(cohort);
            manifest.updated_at = now;
        })
    }

    pub(crate) fn agent_started(&self, label: String, record: AgentRecord) -> FactoryResult<()> {
        self.mutate(|manifest, now| {
            manifest.agents.insert(label, record);
            manifest.updated_at = now;
        })
    }

    pub(crate) fn agent_finished(
        &self,
        label: &str,
        status: &str,
        returncode: Option<i32>,
    ) -> FactoryResult<()> {
        self.mutate(|manifest, now| {
            if let Some(agent) = manifest.agents.get_mut(label) {
                agent.status = status.to_string();
                agent.finished_at = Some(now.clone());
                agent.returncode = returncode;
            }
            manifest.updated_at = now;
        })
    }

    pub(crate) fn checkpoint_review(
        &self,
        completed: u32,
        normal_outcome: Option<&str>,
    ) -> FactoryResult<()> {
        self.mutate(|manifest, now| {
            manifest.completed_review_rounds = Some(completed);
            if let Some(outcome) = normal_outcome {
                manifest.normal_review_outcome = Some(outcome.to_string());
            }
            manifest.updated_at = now;
        })
    }

    pub(crate) fn checkpoint_thermo(
        &self,
        verdict: &str,
        addressed: Option<bool>,
    ) -> FactoryResult<()> {
        self.mutate(|manifest, now| {
            manifest.thermonuclear_verdict = Some(verdict.to_string());
            manifest.thermonuclear_addressed = addressed;
            manifest.updated_at = now;
        })
    }

    pub(crate) fn complete(&self, outcome: &str) -> FactoryResult<()> {
        self.mutate(|manifest, now| {
            manifest.status = "complete".to_string();
            manifest.outcome = Some(outcome.to_string());
            manifest.error = None;
            manifest.updated_at = now;
        })
    }

    pub(crate) fn fail(&self, error: &str) -> FactoryResult<()> {
        self.mutate(|manifest, now| {
            manifest.status = "failed".to_string();
            manifest.outcome = None;
            manifest.error = Some(error.to_string());
            manifest.updated_at = now;
        })
    }

    fn mutate(&self, update: impl FnOnce(&mut Manifest, String)) -> FactoryResult<()> {
        let now = utc_now_rfc3339()?;
        let mut data = self.lock()?;
        let mut next = data.clone();
        update(&mut next, now);
        write_manifest(&self.path, &next)?;
        *data = next;
        Ok(())
    }

    fn flush(&self) -> FactoryResult<()> {
        let data = self.lock()?;
        write_manifest(&self.path, &data)
    }

    fn lock(&self) -> FactoryResult<std::sync::MutexGuard<'_, Manifest>> {
        match self.data.lock() {
            Ok(data) => Ok(data),
            Err(_) => Err(FactoryError::new("manifest journal lock was poisoned")),
        }
    }
}

impl Manifest {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn initial(
        launch: &LaunchRecord,
        maximum_review_rounds: u32,
        request_path: PathBuf,
        tool_paths: ToolPaths,
        tool_versions: ToolVersions,
    ) -> FactoryResult<Self> {
        let now = utc_now_rfc3339()?;
        Ok(Self {
            schema_version: 1,
            run_id: launch.run_id.clone(),
            status: "initializing".to_string(),
            created_at: now.clone(),
            updated_at: now,
            request: request_path,
            request_sha256: launch.request_sha256.clone(),
            repo: launch.repo.clone(),
            base_ref: launch.base_ref.clone(),
            base_sha: launch.base_sha.clone(),
            source_was_dirty: launch.source_was_dirty,
            branch: launch.branch.clone(),
            worktree: launch.worktree.clone(),
            additional_review_rounds: launch.additional_review_rounds,
            maximum_review_rounds,
            agent_timeout_seconds: launch.agent_timeout_seconds,
            models: ModelSet {
                claude: ClaudeModel {
                    model: CLAUDE_MODEL.to_string(),
                    effort: REASONING_EFFORT.to_string(),
                },
                codex: CodexModel {
                    model: CODEX_MODEL.to_string(),
                    reasoning_effort: REASONING_EFFORT.to_string(),
                },
            },
            worker_policy: WorkerPolicy {
                host_permissions: WORKER_HOST_PERMISSIONS.to_string(),
                sandbox: WORKER_SANDBOX.to_string(),
                network_access: WORKER_NETWORK_ACCESS,
                nested_agents_enabled: NESTED_AGENTS_ENABLED,
            },
            thermonuclear_skill: ThermonuclearSkill {
                path: THERMO_SKILL_MANIFEST_PATH.to_string(),
                sha256: THERMO_SKILL_SHA256.to_string(),
                source: THERMO_SKILL_SOURCE.to_string(),
                source_commit: THERMO_SKILL_COMMIT.to_string(),
            },
            tool_paths,
            tool_versions,
            agents: BTreeMap::new(),
            cohorts: Vec::new(),
            events: Vec::new(),
            completed_review_rounds: None,
            normal_review_outcome: None,
            thermonuclear_verdict: None,
            thermonuclear_addressed: None,
            outcome: None,
            error: None,
        })
    }

    pub(crate) fn failure_skeleton(
        launch: &LaunchRecord,
        request_path: PathBuf,
        error: &str,
    ) -> FactoryResult<Self> {
        let maximum_review_rounds = checked_maximum_review_rounds(launch.additional_review_rounds)?;
        #[rustfmt::skip]
        let mut manifest = Self::initial(launch, maximum_review_rounds, request_path, ToolPaths { git: PathBuf::from("unavailable"), claude: PathBuf::from("unavailable"), codex: PathBuf::from("unavailable"), make: PathBuf::from("unavailable") }, ToolVersions { git: "unavailable".to_string(), claude: "unavailable".to_string(), codex: "unavailable".to_string(), make_tool: "unavailable".to_string() })?;
        manifest.mark_failed(error)?;
        Ok(manifest)
    }

    pub(crate) fn mark_failed(&mut self, error: &str) -> FactoryResult<()> {
        self.status = "failed".to_string();
        self.outcome = None;
        self.error = Some(error.to_string());
        self.updated_at = utc_now_rfc3339()?;
        Ok(())
    }
}

fn checked_maximum_review_rounds(additional_review_rounds: u32) -> FactoryResult<u32> {
    Ok(require_some!(
        additional_review_rounds.checked_add(1),
        FactoryError::new("review-round count overflow while recovering manifest")
    ))
}

fn write_manifest(path: &Path, manifest: &Manifest) -> FactoryResult<()> {
    atomic_write(path, &json_bytes(manifest)?)
}

fn json_bytes(value: &impl Serialize) -> FactoryResult<Vec<u8>> {
    let mut bytes = result_context(serde_json::to_vec_pretty(value), "could not serialize JSON")?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[cfg(test)]
#[path = "manifest/tests/mod.rs"]
mod tests;
