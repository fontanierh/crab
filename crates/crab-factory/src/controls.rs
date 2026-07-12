use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use time::format_description::well_known::Rfc3339;
use time::{OffsetDateTime, UtcOffset};

use crate::config::{validate_cohort_size, Effort};
use crate::manifest::{FactoryConfiguration, Journal, LaunchRecord, Manifest};
use crate::orchestrator::validate_prepared_metadata;
use crate::run_lock::{RunLock, RunMarker};
use crate::{
    create_secure_dir, io_result, result_context, sha256_hex, utc_now_rfc3339, FactoryError,
    FactoryResult,
};

const MAX_MESSAGE_BYTES: usize = 65_536;
const MAX_SEQUENCE: u32 = 999_999;
static STAGE_COUNTER: AtomicU64 = AtomicU64::new(0);

macro_rules! control_event {
    ($($json:tt)+) => {{
        let Value::Object(event) = json!($($json)+) else {
            unreachable!("control event literals are objects");
        };
        event
    }};
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ControlRecord {
    schema_version: u32,
    sequence: u32,
    kind: String,
    created_at: String,
    run_id: String,
    request_sha256: String,
    payload_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    effort: Option<Effort>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    plan_critics: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    codex_reviewers: Option<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Disposition {
    knob: String,
    state: String,
    stage: Option<String>,
    at: Option<String>,
    reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LedgerEntry {
    sequence: u32,
    kind: String,
    filename: String,
    payload_sha256: String,
    record_sha256: String,
    dispositions: Vec<Disposition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Ledger {
    schema_version: u32,
    run_id: String,
    request_sha256: String,
    sequences: BTreeMap<u32, LedgerEntry>,
}

struct ScannedRecord {
    record: ControlRecord,
    bytes: Vec<u8>,
}

pub(crate) enum Boundary<'a> {
    Prompt(&'a str),
    PlanCritiques(&'a str),
    Reviews(&'a str),
}

impl Boundary<'_> {
    fn stage(&self) -> &str {
        match self {
            Self::Prompt(stage) | Self::PlanCritiques(stage) | Self::Reviews(stage) => stage,
        }
    }
}

pub(crate) struct ControlPlane {
    run_dir: PathBuf,
    journal: Arc<Journal>,
}

pub(crate) struct AppliedControls {
    pub(crate) configuration: FactoryConfiguration,
    pub(crate) steering: Vec<(u32, String, String, String)>,
}

struct OpenRun {
    run_dir: PathBuf,
    marker: RunMarker,
    launch: LaunchRecord,
    manifest: Manifest,
}

#[derive(Debug)]
struct ControlLock(File);

#[derive(Debug)]
enum ControlLockError {
    Busy,
    Unsafe(FactoryError),
}

impl Drop for ControlLock {
    fn drop(&mut self) {
        let _ = unsafe { libc::flock(self.0.as_raw_fd(), libc::LOCK_UN) };
    }
}

impl ControlPlane {
    pub(crate) fn new(run_dir: PathBuf, journal: Arc<Journal>) -> Self {
        Self { run_dir, journal }
    }

    pub(crate) fn sync(&self, boundary: Boundary<'_>) -> FactoryResult<AppliedControls> {
        match self.sync_inner(boundary) {
            Ok(applied) => Ok(applied),
            Err(error) => {
                let _ = self
                    .journal
                    .event_once("control_invalid", json!({"error": error.to_string()}));
                Err(error)
            }
        }
    }

    fn sync_inner(&self, boundary: Boundary<'_>) -> FactoryResult<AppliedControls> {
        let open = open_run(&self.run_dir, false)?;
        let _lock = acquire_control_lock(&open.run_dir)?;
        let (open, records, mut ledger) = reload_validated(open, true)?;
        let now = utc_now_rfc3339()?;
        for entry in ledger.sequences.values_mut() {
            for disposition in &mut entry.dispositions {
                if disposition.state != "accepted" {
                    continue;
                }
                let applies = disposition.knob == "steering"
                    || disposition.knob == "effort"
                    || (disposition.knob == "plan_critics"
                        && matches!(boundary, Boundary::PlanCritiques(_)))
                    || (disposition.knob == "codex_reviewers"
                        && matches!(boundary, Boundary::Reviews(_)));
                let plan_too_late = disposition.knob == "plan_critics"
                    && !matches!(boundary, Boundary::PlanCritiques(_))
                    && boundary.stage() != "planning";
                if applies {
                    disposition.state = "applied".to_string();
                    disposition.stage = Some(boundary.stage().to_string());
                    disposition.at = Some(now.clone());
                } else if plan_too_late {
                    disposition.state = "rejected".to_string();
                    disposition.reason = Some("plan-critique cohort already launched".to_string());
                    disposition.at = Some(now.clone());
                }
            }
        }
        write_ledger(&open, &ledger)?;
        let applied = reconstruct(&open, &records, &ledger)?;
        self.project(&ledger, &applied.configuration)?;
        Ok(applied)
    }

    fn project(&self, ledger: &Ledger, configuration: &FactoryConfiguration) -> FactoryResult<()> {
        let mut events = Vec::new();
        for entry in ledger.sequences.values() {
            events.push(control_event!({"event": "control_accepted", "sequence": entry.sequence, "kind": entry.kind}));
            for disposition in &entry.dispositions {
                if disposition.state == "applied" {
                    events.push(control_event!({"event": "control_applied", "sequence": entry.sequence, "knob": disposition.knob, "stage": disposition.stage, "sha256": entry.payload_sha256}));
                } else if disposition.state == "rejected" {
                    events.push(control_event!({"event": "control_rejected", "sequence": entry.sequence, "knob": disposition.knob, "reason": disposition.reason}));
                }
            }
        }
        self.journal.project_controls(configuration.clone(), events)
    }
}

pub(crate) fn initialize(run_dir: &Path, run_id: &str, request_sha256: &str) -> FactoryResult<()> {
    let directory = run_dir.join("controls");
    create_secure_dir(&directory)?;
    let lock = directory.join(".controls.lock");
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&lock);
    drop(io_result(file, "create controls lock", &lock)?);
    let ledger = Ledger {
        schema_version: 1,
        run_id: run_id.to_string(),
        request_sha256: request_sha256.to_string(),
        sequences: BTreeMap::new(),
    };
    durable_replace(&directory.join("state.json"), &json_bytes(&ledger)?, 0o600)
}

pub(crate) fn steer(run_dir: &Path, message: String, stdout: &mut dyn Write) -> FactoryResult<()> {
    validate_message(&message)?;
    let open = open_run(run_dir, true)?;
    let payload_sha256 = sha256_hex(message.as_bytes());
    let record = ControlRecord {
        schema_version: 1,
        sequence: 0,
        kind: "steer".to_string(),
        created_at: utc_now_rfc3339()?,
        run_id: open.marker.run_id.clone(),
        request_sha256: open.marker.request_sha256.clone(),
        payload_sha256,
        message: Some(message),
        effort: None,
        plan_critics: None,
        codex_reviewers: None,
    };
    queue(open, record, stdout)
}

pub(crate) fn configure(
    run_dir: &Path,
    effort: Option<Effort>,
    plan_critics: Option<u8>,
    codex_reviewers: Option<u8>,
    stdout: &mut dyn Write,
) -> FactoryResult<()> {
    if effort.is_none() && plan_critics.is_none() && codex_reviewers.is_none() {
        return Err(FactoryError::new("configure requires at least one setting"));
    }
    if let Some(value) = plan_critics {
        validate_cohort_size("--plan-critics", value)?;
    }
    if let Some(value) = codex_reviewers {
        validate_cohort_size("--codex-reviewers", value)?;
    }
    let open = open_run(run_dir, true)?;
    let payload =
        json!({"effort": effort, "plan_critics": plan_critics, "codex_reviewers": codex_reviewers});
    let record = ControlRecord {
        schema_version: 1,
        sequence: 0,
        kind: "configure".to_string(),
        created_at: utc_now_rfc3339()?,
        run_id: open.marker.run_id.clone(),
        request_sha256: open.marker.request_sha256.clone(),
        payload_sha256: sha256_hex(&json_bytes(&payload)?),
        message: None,
        effort,
        plan_critics,
        codex_reviewers,
    };
    queue(open, record, stdout)
}

pub(crate) fn status(
    run_dir: &Path,
    json_output: bool,
    stdout: &mut dyn Write,
) -> FactoryResult<()> {
    let (open, records, validated_ledger) = load_validated(run_dir, false, true)?;
    let mut controls = Vec::new();
    for scanned in records.values() {
        let record = &scanned.record;
        let Some(entry) = validated_ledger.sequences.get(&record.sequence) else {
            return Err(FactoryError::new(
                "orphaned control record after ledger validation",
            ));
        };
        let dispositions = status_dispositions(entry, &open.manifest);
        controls.push(json!({"sequence": record.sequence, "kind": record.kind, "payload_sha256": record.payload_sha256, "dispositions": dispositions}));
    }
    let mut active_workers = Vec::new();
    for (label, agent) in &open.manifest.agents {
        if agent.status == "running" {
            active_workers.push(json!({"label": label, "provider": agent.provider, "pid": agent.pid, "started_at": agent.started_at}));
        }
    }
    let mut last_applied_sequence = None;
    for entry in validated_ledger.sequences.values() {
        let mut applied = false;
        for disposition in &entry.dispositions {
            if disposition.state == "applied" {
                applied = true;
                break;
            }
        }
        if applied {
            last_applied_sequence = Some(entry.sequence);
        }
    }
    let payload = json!({
        "run_id": open.manifest.run_id,
        "lifecycle": open.manifest.status,
        "outcome": open.manifest.outcome,
        "error": open.manifest.error,
        "current_stage": current_stage(&open.manifest),
        "launch": {"mode": open.launch.launch_mode, "pid": open.launch.launched_pid},
        "prepared_configuration": open.manifest.prepared_configuration,
        "effective_configuration": open.manifest.effective_configuration,
        "configuration_note": if open.manifest.prepared_configuration.is_none() { Some("configuration unavailable (run predates live controls)") } else { None },
        "active_workers": active_workers,
        "controls": controls,
        "last_applied_sequence": last_applied_sequence,
    });
    if json_output {
        writeln_result(
            stdout,
            &result_context(
                serde_json::to_string_pretty(&payload),
                "could not serialize status",
            )?,
        )
    } else {
        writeln_result(
            stdout,
            &format!("run {}: {}", open.manifest.run_id, open.manifest.status),
        )?;
        writeln_result(
            stdout,
            &format!("current stage: {}", current_stage(&open.manifest)),
        )?;
        writeln_result(
            stdout,
            &format!(
                "launch: mode={} pid={}",
                open.launch.launch_mode.as_deref().unwrap_or("unknown"),
                match open.launch.launched_pid {
                    Some(pid) => pid.to_string(),
                    None => "unavailable".to_string(),
                }
            ),
        )?;
        if open.manifest.prepared_configuration.is_none() {
            writeln_result(
                stdout,
                "configuration unavailable (run predates live controls)",
            )?;
        } else {
            writeln_result(
                stdout,
                &format!(
                    "configuration: prepared={} effective={}",
                    json!(open.manifest.prepared_configuration),
                    json!(open.manifest.effective_configuration)
                ),
            )?;
        }
        writeln_result(stdout, &format!("active workers: {}", active_workers.len()))?;
        for (label, agent) in &open.manifest.agents {
            if agent.status != "running" {
                continue;
            }
            writeln_result(
                stdout,
                &format!(
                    "worker {label}: provider={} pid={} started_at={}",
                    agent.provider,
                    match agent.pid {
                        Some(pid) => pid.to_string(),
                        None => "unavailable".to_string(),
                    },
                    agent.started_at
                ),
            )?;
        }
        for scanned in records.values() {
            let record = &scanned.record;
            let Some(entry) = validated_ledger.sequences.get(&record.sequence) else {
                return Err(FactoryError::new(
                    "orphaned control record after ledger validation",
                ));
            };
            let dispositions = status_dispositions(entry, &open.manifest);
            writeln_result(
                stdout,
                &format!(
                    "control {} {}: {}",
                    record.sequence, record.kind, dispositions
                ),
            )?;
        }
        writeln_result(
            stdout,
            &format!(
                "last applied sequence: {}",
                match last_applied_sequence {
                    Some(sequence) => sequence.to_string(),
                    None => "none".to_string(),
                }
            ),
        )?;
        Ok(())
    }
}

fn terminal_sweep_locked(run_dir: &Path, journal: &Journal) -> FactoryResult<()> {
    let open = open_run(run_dir, false)?;
    let records = scan_records(&open)?;
    let mut ledger = read_ledger(&open)?;
    ingest_records(&records, &mut ledger)?;
    let now = utc_now_rfc3339()?;
    for entry in ledger.sequences.values_mut() {
        for disposition in &mut entry.dispositions {
            if disposition.state == "accepted" {
                disposition.state = "rejected".to_string();
                disposition.reason = Some(
                    match disposition.knob.as_str() {
                        "effort" => "no remaining workers",
                        "codex_reviewers" => "no review cohort launched after acceptance",
                        _ => "run terminal before an applicable boundary",
                    }
                    .to_string(),
                );
                disposition.at = Some(now.clone());
            }
        }
    }
    write_ledger(&open, &ledger)?;
    let applied = reconstruct(&open, &records, &ledger)?;
    let mut events = Vec::new();
    for entry in ledger.sequences.values() {
        events.push(control_event!({"event": "control_accepted", "sequence": entry.sequence, "kind": entry.kind}));
        for disposition in &entry.dispositions {
            if disposition.state == "rejected" {
                events.push(control_event!({"event": "control_rejected", "sequence": entry.sequence, "knob": disposition.knob, "reason": disposition.reason}));
            }
        }
    }
    journal.project_controls(applied.configuration, events)
}

pub(crate) fn terminalize(
    run_dir: &Path,
    journal: &Journal,
    terminal_write: impl FnOnce() -> FactoryResult<()>,
) -> FactoryResult<()> {
    if !run_dir.join("controls").is_dir() {
        return terminal_write();
    }
    let _lock = match acquire_control_lock_blocking(run_dir) {
        Ok(lock) => lock,
        Err(error) => {
            journal.event_once("control_invalid", json!({"error": error.to_string()}))?;
            return terminal_write();
        }
    };
    if let Err(error) = terminal_sweep_locked(run_dir, journal) {
        journal.event_once("control_invalid", json!({"error": error.to_string()}))?;
    }
    terminal_write()
}

fn open_run(run_dir: &Path, reject_terminal: bool) -> FactoryResult<OpenRun> {
    let run_dir = canonical_run_dir(run_dir)?;
    let marker = RunLock::marker(&run_dir)?;
    validate_node(&run_dir, 0o700, true)?;
    let launch = LaunchRecord::read(&run_dir.join("launch.json"))?;
    let manifest = Journal::load(run_dir.join("manifest.json"))?.snapshot()?;
    if manifest.prepared_configuration.is_some() {
        validate_prepared_metadata(&run_dir, &launch, &manifest, &marker)?;
    }
    if reject_terminal && matches!(manifest.status.as_str(), "complete" | "failed") {
        return Err(FactoryError::new(
            "run is terminal; controls are not accepted",
        ));
    }
    let controls = run_dir.join("controls");
    if manifest.prepared_configuration.is_none() {
        if reject_terminal {
            return Err(FactoryError::new("run predates live-control support"));
        }
    } else {
        validate_node(&controls, 0o700, true)?;
        validate_node(&controls.join(".controls.lock"), 0o600, false)?;
        validate_node(&controls.join("state.json"), 0o600, false)?;
    }
    let open = OpenRun {
        run_dir,
        marker,
        launch,
        manifest,
    };
    Ok(open)
}

pub(crate) fn canonical_run_dir(run_dir: &Path) -> FactoryResult<PathBuf> {
    let metadata = io_result(
        fs::symlink_metadata(run_dir),
        "inspect prepared run directory",
        run_dir,
    )?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(FactoryError::new(format!(
            "unsafe prepared run directory: {}",
            run_dir.display()
        )));
    }
    let canonical = io_result(
        fs::canonicalize(run_dir),
        "canonicalize run directory",
        run_dir,
    )?;
    Ok(canonical)
}

fn load_validated(
    run_dir: &Path,
    reject_terminal: bool,
    allow_ledger_ahead: bool,
) -> FactoryResult<(OpenRun, BTreeMap<u32, ScannedRecord>, Ledger)> {
    let open = open_run(run_dir, reject_terminal)?;
    validate_controls(open, allow_ledger_ahead)
}

fn reload_validated(
    mut open: OpenRun,
    allow_ledger_ahead: bool,
) -> FactoryResult<(OpenRun, BTreeMap<u32, ScannedRecord>, Ledger)> {
    open.manifest = Journal::load(open.run_dir.join("manifest.json"))?.snapshot()?;
    if open.manifest.prepared_configuration.is_some() {
        validate_prepared_metadata(&open.run_dir, &open.launch, &open.manifest, &open.marker)?;
        validate_node(&open.run_dir.join("controls"), 0o700, true)?;
        validate_node(&open.run_dir.join("controls/.controls.lock"), 0o600, false)?;
        validate_node(&open.run_dir.join("controls/state.json"), 0o600, false)?;
    }
    validate_controls(open, allow_ledger_ahead)
}

fn validate_controls(
    open: OpenRun,
    allow_ledger_ahead: bool,
) -> FactoryResult<(OpenRun, BTreeMap<u32, ScannedRecord>, Ledger)> {
    let records = scan_records(&open)?;
    let mut ledger = read_ledger(&open)?;
    ingest_records(&records, &mut ledger)?;
    if open.manifest.prepared_configuration.is_some() {
        validate_projected_configuration(&open, &records, &ledger, allow_ledger_ahead)?;
    }
    Ok((open, records, ledger))
}

pub(crate) fn authenticate_prepared_controls(run_dir: &Path) -> FactoryResult<()> {
    let open = open_run(run_dir, false)?;
    let _lock = acquire_control_lock(&open.run_dir)?;
    reload_validated(open, true).map(|_| ())
}

fn validate_node(path: &Path, mode: u32, directory: bool) -> FactoryResult<()> {
    let metadata = io_result(
        fs::symlink_metadata(path),
        "inspect managed control path",
        path,
    )?;
    let correct_kind = if directory {
        metadata.is_dir()
    } else {
        metadata.is_file()
    };
    if metadata.file_type().is_symlink()
        || !correct_kind
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o777 != mode
    {
        return Err(FactoryError::new(format!(
            "unsafe managed control path: {}",
            path.display()
        )));
    }
    Ok(())
}

fn acquire_control_lock(run_dir: &Path) -> FactoryResult<ControlLock> {
    match acquire_control_lock_with_deadline(run_dir, Duration::from_secs(10)) {
        Ok(lock) => Ok(lock),
        Err(ControlLockError::Busy) => Err(FactoryError::new("controls are busy; retry")),
        Err(ControlLockError::Unsafe(error)) => Err(error),
    }
}

fn acquire_control_lock_with_deadline(
    run_dir: &Path,
    timeout: Duration,
) -> Result<ControlLock, ControlLockError> {
    let path = run_dir.join("controls/.controls.lock");
    let deadline = Instant::now() + timeout;
    loop {
        let file = nofollow_open(&path, 0o600).map_err(ControlLockError::Unsafe)?;
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
            return Ok(ControlLock(file));
        }
        if Instant::now() >= deadline {
            return Err(ControlLockError::Busy);
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn acquire_control_lock_blocking(run_dir: &Path) -> FactoryResult<ControlLock> {
    let path = run_dir.join("controls/.controls.lock");
    let file = nofollow_open(&path, 0o600)?;
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } != 0 {
        let error = std::io::Error::last_os_error();
        return Err(FactoryError::io("lock controls", &path, &error));
    }
    Ok(ControlLock(file))
}

fn nofollow_open(path: &Path, mode: u32) -> FactoryResult<File> {
    let file = io_result(
        OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path),
        "open managed control file",
        path,
    )?;
    let metadata = io_result(file.metadata(), "inspect opened control file", path)?;
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o777 != mode
    {
        return Err(FactoryError::new(format!(
            "unsafe managed control file: {}",
            path.display()
        )));
    }
    Ok(file)
}

fn read_private(path: &Path, mode: u32) -> FactoryResult<Vec<u8>> {
    let mut file = nofollow_open(path, mode)?;
    let mut bytes = Vec::new();
    io_result(
        file.read_to_end(&mut bytes),
        "read managed control file",
        path,
    )?;
    Ok(bytes)
}

fn scan_records(open: &OpenRun) -> FactoryResult<BTreeMap<u32, ScannedRecord>> {
    if open.manifest.prepared_configuration.is_none() {
        return Ok(BTreeMap::new());
    }
    let directory = open.run_dir.join("controls");
    let mut records = BTreeMap::new();
    for entry in io_result(fs::read_dir(&directory), "scan controls", &directory)? {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                return Err(FactoryError::new(format!(
                    "could not scan controls: {error}"
                )))
            }
        };
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with('.') || name == "state.json" {
            continue;
        }
        let (sequence, kind) = parse_filename(&name)?;
        if records.contains_key(&sequence) {
            return Err(FactoryError::new(format!(
                "duplicate control sequence {sequence}"
            )));
        }
        let bytes = read_private(&entry.path(), 0o400)?;
        let record: ControlRecord = result_context(
            serde_json::from_slice(&bytes),
            &format!("invalid control record {name}"),
        )?;
        validate_record(open, &record, sequence, kind)?;
        records.insert(sequence, ScannedRecord { record, bytes });
    }
    if let Some(max) = records.keys().next_back().copied() {
        let expected: BTreeSet<u32> = (1..=max).collect();
        if records.keys().copied().collect::<BTreeSet<_>>() != expected {
            return Err(FactoryError::new("control sequence gap detected"));
        }
    }
    Ok(records)
}

fn parse_filename(name: &str) -> FactoryResult<(u32, &str)> {
    let (number, suffix) = require_some!(
        name.split_once('-'),
        FactoryError::new(format!("non-conforming control filename: {name}"))
    );
    if number.len() != 6 || !number.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(FactoryError::new(format!(
            "non-conforming control filename: {name}"
        )));
    }
    let kind = require_some!(
        suffix.strip_suffix(".json"),
        FactoryError::new(format!("non-conforming control filename: {name}"))
    );
    if !matches!(kind, "steer" | "configure") {
        return Err(FactoryError::new(format!(
            "non-conforming control filename: {name}"
        )));
    }
    let sequence = result_context(number.parse(), &format!("invalid control sequence: {name}"))?;
    Ok((sequence, kind))
}

fn validate_record(
    open: &OpenRun,
    record: &ControlRecord,
    sequence: u32,
    kind: &str,
) -> FactoryResult<()> {
    if record.schema_version != 1
        || record.sequence != sequence
        || record.kind != kind
        || record.run_id != open.marker.run_id
        || record.request_sha256 != open.marker.request_sha256
    {
        return Err(FactoryError::new(format!(
            "control record identity mismatch at sequence {sequence}"
        )));
    }
    if validate_control_timestamp(&record.created_at).is_err() {
        return Err(FactoryError::new(format!(
            "invalid control record timestamp at sequence {sequence}"
        )));
    }
    let expected = if kind == "steer" {
        let Some(message) = record.message.as_deref() else {
            return Err(FactoryError::new("steering record has no message"));
        };
        validate_message(message)?;
        if record.effort.is_some()
            || record.plan_critics.is_some()
            || record.codex_reviewers.is_some()
        {
            return Err(FactoryError::new(
                "steering record contains configure fields",
            ));
        }
        sha256_hex(message.as_bytes())
    } else {
        if record.message.is_some()
            || (record.effort.is_none()
                && record.plan_critics.is_none()
                && record.codex_reviewers.is_none())
        {
            return Err(FactoryError::new("invalid configure control payload"));
        }
        if let Some(value) = record.plan_critics {
            validate_cohort_size("--plan-critics", value)?;
        }
        if let Some(value) = record.codex_reviewers {
            validate_cohort_size("--codex-reviewers", value)?;
        }
        let payload = json!({"effort": record.effort, "plan_critics": record.plan_critics, "codex_reviewers": record.codex_reviewers});
        sha256_hex(&json_bytes(&payload)?)
    };
    if expected != record.payload_sha256 {
        return Err(FactoryError::new(format!(
            "control payload hash mismatch at sequence {sequence}"
        )));
    }
    Ok(())
}

fn read_ledger(open: &OpenRun) -> FactoryResult<Ledger> {
    if open.manifest.prepared_configuration.is_none() {
        return Ok(Ledger {
            schema_version: 1,
            run_id: open.marker.run_id.clone(),
            request_sha256: open.marker.request_sha256.clone(),
            sequences: BTreeMap::new(),
        });
    }
    let path = open.run_dir.join("controls/state.json");
    let ledger: Ledger = result_context(
        serde_json::from_slice(&read_private(&path, 0o600)?),
        "invalid controls ledger",
    )?;
    if ledger.schema_version != 1
        || ledger.run_id != open.marker.run_id
        || ledger.request_sha256 != open.marker.request_sha256
    {
        return Err(FactoryError::new("controls ledger identity mismatch"));
    }
    Ok(ledger)
}

fn ingest_records(
    records: &BTreeMap<u32, ScannedRecord>,
    ledger: &mut Ledger,
) -> FactoryResult<()> {
    for (sequence, scanned) in records {
        let record = &scanned.record;
        let filename = format!("{sequence:06}-{}.json", record.kind);
        if let Some(entry) = ledger.sequences.get(sequence) {
            validate_ledger_entry(*sequence, record, &filename, &scanned.bytes, entry)?;
            continue;
        }
        let knobs = if record.kind == "steer" {
            vec!["steering"]
        } else {
            let mut knobs = Vec::new();
            if record.effort.is_some() {
                knobs.push("effort");
            }
            if record.plan_critics.is_some() {
                knobs.push("plan_critics");
            }
            if record.codex_reviewers.is_some() {
                knobs.push("codex_reviewers");
            }
            knobs
        };
        ledger.sequences.insert(
            *sequence,
            LedgerEntry {
                sequence: *sequence,
                kind: record.kind.clone(),
                filename,
                payload_sha256: record.payload_sha256.clone(),
                record_sha256: sha256_hex(&scanned.bytes),
                dispositions: knobs
                    .into_iter()
                    .map(|knob| Disposition {
                        knob: knob.to_string(),
                        state: "accepted".to_string(),
                        stage: None,
                        at: None,
                        reason: None,
                    })
                    .collect(),
            },
        );
    }
    if ledger
        .sequences
        .keys()
        .any(|sequence| !records.contains_key(sequence))
    {
        return Err(FactoryError::new("orphaned controls ledger entry"));
    }
    Ok(())
}

fn expected_knobs(record: &ControlRecord) -> Vec<&'static str> {
    if record.kind == "steer" {
        return vec!["steering"];
    }
    let mut knobs = Vec::new();
    if record.effort.is_some() {
        knobs.push("effort");
    }
    if record.plan_critics.is_some() {
        knobs.push("plan_critics");
    }
    if record.codex_reviewers.is_some() {
        knobs.push("codex_reviewers");
    }
    knobs
}

fn validate_ledger_entry(
    sequence: u32,
    record: &ControlRecord,
    filename: &str,
    record_bytes: &[u8],
    entry: &LedgerEntry,
) -> FactoryResult<()> {
    let expected = expected_knobs(record);
    let actual = entry
        .dispositions
        .iter()
        .map(|disposition| disposition.knob.as_str())
        .collect::<Vec<_>>();
    if entry.sequence != sequence
        || entry.kind != record.kind
        || entry.filename != filename
        || entry.payload_sha256 != record.payload_sha256
        || entry.record_sha256 != sha256_hex(record_bytes)
        || actual != expected
    {
        return Err(FactoryError::new(format!(
            "control ledger entry mismatch at sequence {sequence}"
        )));
    }
    for disposition in &entry.dispositions {
        let coherent = match disposition.state.as_str() {
            "accepted" => {
                disposition.stage.is_none()
                    && disposition.at.is_none()
                    && disposition.reason.is_none()
            }
            "applied" => {
                let valid_stage = match disposition.stage.as_deref() {
                    Some(stage) => known_control_stage(stage),
                    None => false,
                };
                let valid_time = match disposition.at.as_deref() {
                    Some(at) => validate_control_timestamp(at).is_ok(),
                    None => false,
                };
                valid_stage
                    && valid_time
                    && disposition.reason.is_none()
                    && match disposition.knob.as_str() {
                        "plan_critics" => disposition.stage.as_deref() == Some("plan-critiques"),
                        "codex_reviewers" => disposition.stage.as_deref() == Some("normal-reviews"),
                        _ => true,
                    }
            }
            "rejected" => {
                let valid_time = match disposition.at.as_deref() {
                    Some(at) => validate_control_timestamp(at).is_ok(),
                    None => false,
                };
                let valid_reason = match disposition.reason.as_deref() {
                    Some(reason) => known_rejection_reason(&disposition.knob, reason),
                    None => false,
                };
                disposition.stage.is_none() && valid_time && valid_reason
            }
            _ => false,
        };
        if !coherent {
            return Err(FactoryError::new(format!(
                "invalid control disposition at sequence {sequence}"
            )));
        }
    }
    Ok(())
}

fn validate_control_timestamp(value: &str) -> FactoryResult<()> {
    let timestamp = result_context(
        OffsetDateTime::parse(value, &Rfc3339),
        "invalid control timestamp",
    )?;
    if timestamp.offset() != UtcOffset::UTC || !value.ends_with('Z') {
        return Err(FactoryError::new("control timestamp is not UTC"));
    }
    Ok(())
}

fn known_control_stage(stage: &str) -> bool {
    if matches!(
        stage,
        "planning"
            | "plan-critiques"
            | "03-critique-synthesis.md"
            | "04-address-critiques.md"
            | "normal-reviews"
            | "06-thermo-nuclear-review.md"
            | "06-thermo-nuclear-address.md"
    ) {
        return true;
    }
    let Some(value) = stage.strip_prefix("review-round-") else {
        return false;
    };
    let Some((round, leaf)) = value.split_once('/') else {
        return false;
    };
    if !matches!(round.len(), 2 | 3) || !matches!(leaf, "synthesis.md" | "address.md") {
        return false;
    }
    let mut nonzero = false;
    for byte in round.bytes() {
        if !byte.is_ascii_digit() {
            return false;
        }
        if byte != b'0' {
            nonzero = true;
        }
    }
    nonzero
}

fn known_rejection_reason(knob: &str, reason: &str) -> bool {
    match knob {
        "steering" => reason == "run terminal before an applicable boundary",
        "effort" => reason == "no remaining workers",
        "plan_critics" => matches!(
            reason,
            "plan-critique cohort already launched" | "run terminal before an applicable boundary"
        ),
        "codex_reviewers" => reason == "no review cohort launched after acceptance",
        _ => false,
    }
}

fn validate_projected_configuration(
    open: &OpenRun,
    records: &BTreeMap<u32, ScannedRecord>,
    ledger: &Ledger,
    allow_ledger_ahead: bool,
) -> FactoryResult<()> {
    let Some(actual) = open.manifest.effective_configuration.as_ref() else {
        return Err(FactoryError::new("run predates live-control support"));
    };
    let reconstructed = reconstruct(open, records, ledger)?;
    if actual == &reconstructed.configuration {
        return Ok(());
    }
    if allow_ledger_ahead && configuration_is_applied_prefix(open, records, ledger, actual)? {
        return Ok(());
    }
    Err(FactoryError::new(
        "effective configuration does not match the authenticated control ledger",
    ))
}

fn configuration_is_applied_prefix(
    open: &OpenRun,
    records: &BTreeMap<u32, ScannedRecord>,
    ledger: &Ledger,
    candidate: &FactoryConfiguration,
) -> FactoryResult<bool> {
    let Some(mut configuration) = open.manifest.prepared_configuration.clone() else {
        return Err(FactoryError::new("run predates live-control support"));
    };
    if &configuration == candidate {
        return Ok(true);
    }
    for (sequence, entry) in &ledger.sequences {
        let Some(scanned) = records.get(sequence) else {
            return Err(FactoryError::new("orphaned controls ledger entry"));
        };
        let record = &scanned.record;
        for disposition in &entry.dispositions {
            if disposition.state != "applied" {
                continue;
            }
            match disposition.knob.as_str() {
                "effort" => {
                    let Some(value) = record.effort else {
                        return Err(FactoryError::new("applied effort control has no value"));
                    };
                    configuration.effort = value;
                }
                "plan_critics" => {
                    let Some(value) = record.plan_critics else {
                        return Err(FactoryError::new(
                            "applied plan-critics control has no value",
                        ));
                    };
                    configuration.plan_critics = value;
                }
                "codex_reviewers" => {
                    let Some(value) = record.codex_reviewers else {
                        return Err(FactoryError::new("applied reviewer control has no value"));
                    };
                    configuration.codex_reviewers = value;
                }
                "steering" => {}
                _ => return Err(FactoryError::new("unknown control disposition knob")),
            }
            if &configuration == candidate {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn reconstruct(
    open: &OpenRun,
    records: &BTreeMap<u32, ScannedRecord>,
    ledger: &Ledger,
) -> FactoryResult<AppliedControls> {
    let Some(mut configuration) = open.manifest.prepared_configuration.clone() else {
        return Err(FactoryError::new("run predates live-control support"));
    };
    let mut steering = Vec::new();
    for (sequence, entry) in &ledger.sequences {
        let Some(scanned) = records.get(sequence) else {
            return Err(FactoryError::new("orphaned controls ledger entry"));
        };
        let record = &scanned.record;
        for disposition in &entry.dispositions {
            if disposition.state != "applied" {
                continue;
            }
            match disposition.knob.as_str() {
                "steering" => {
                    let Some(message) = record.message.clone() else {
                        return Err(FactoryError::new("applied steering control has no value"));
                    };
                    steering.push((
                        *sequence,
                        record.payload_sha256.clone(),
                        record.created_at.clone(),
                        message,
                    ));
                }
                "effort" => {
                    let Some(value) = record.effort else {
                        return Err(FactoryError::new("applied effort control has no value"));
                    };
                    configuration.effort = value;
                }
                "plan_critics" => {
                    let Some(value) = record.plan_critics else {
                        return Err(FactoryError::new(
                            "applied plan-critics control has no value",
                        ));
                    };
                    configuration.plan_critics = value;
                }
                "codex_reviewers" => {
                    let Some(value) = record.codex_reviewers else {
                        return Err(FactoryError::new("applied reviewer control has no value"));
                    };
                    configuration.codex_reviewers = value;
                }
                _ => return Err(FactoryError::new("unknown control disposition knob")),
            }
        }
    }
    Ok(AppliedControls {
        configuration,
        steering,
    })
}

fn queue(open: OpenRun, mut record: ControlRecord, stdout: &mut dyn Write) -> FactoryResult<()> {
    let _lock = acquire_control_lock(&open.run_dir)?;
    clean_staging_files(&open.run_dir.join("controls"))?;
    let (open, records, _) = reload_validated(open, true)?;
    if matches!(open.manifest.status.as_str(), "complete" | "failed") {
        return Err(FactoryError::new(
            "run is terminal; controls are not accepted",
        ));
    }
    record.sequence = next_sequence(records.keys().next_back().copied())?;
    let filename = format!("{:06}-{}.json", record.sequence, record.kind);
    durable_replace(
        &open.run_dir.join("controls").join(filename),
        &json_bytes(&record)?,
        0o400,
    )?;
    if record.plan_critics.is_some() && stage_started(&open.manifest, "plan-critiques") {
        writeln_result(
            stdout,
            "warning: the plan-critique cohort already launched; that setting will be formally rejected at the next boundary",
        )?;
    }
    if record.codex_reviewers.is_some() && open.manifest.normal_review_outcome.is_some() {
        writeln_result(
            stdout,
            "warning: the normal review cohorts already completed; that setting will be formally rejected at a later boundary",
        )?;
    }
    writeln_result(stdout, &format!("accepted control sequence {}; applies at an eligible subsequent stage boundary; run `crab-factory status` to track", record.sequence))
}

fn next_sequence(highest: Option<u32>) -> FactoryResult<u32> {
    let highest = highest.unwrap_or(0);
    if highest >= MAX_SEQUENCE {
        return Err(FactoryError::new("control sequence limit reached"));
    }
    Ok(highest + 1)
}

fn clean_staging_files(directory: &Path) -> FactoryResult<()> {
    let entries = io_result(
        fs::read_dir(directory),
        "scan staged control files",
        directory,
    )?;
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                return Err(FactoryError::new(format!(
                    "could not scan staged controls: {error}"
                )));
            }
        };
        if entry.file_name().to_string_lossy().starts_with(".stage-") {
            io_result(
                fs::remove_file(entry.path()),
                "remove stale staged control file",
                &entry.path(),
            )?;
        }
    }
    Ok(())
}

fn stage_started(manifest: &Manifest, stage: &str) -> bool {
    manifest.events.iter().any(|event| {
        event.get("event").and_then(Value::as_str) == Some("stage_started")
            && event.get("stage").and_then(Value::as_str) == Some(stage)
    })
}

fn validate_message(message: &str) -> FactoryResult<()> {
    if message.trim().is_empty() {
        return Err(FactoryError::new("steering message must not be empty"));
    }
    if message.len() > MAX_MESSAGE_BYTES {
        return Err(FactoryError::new("steering message exceeds 65536 bytes"));
    }
    Ok(())
}

fn status_dispositions(entry: &LedgerEntry, manifest: &Manifest) -> Value {
    let mut values = Vec::new();
    for disposition in &entry.dispositions {
        let mut value = serde_json::to_value(disposition).unwrap_or(Value::Null);
        if disposition.state == "accepted" {
            value["earliest_stage"] = json!(earliest_knob(&disposition.knob, manifest));
        }
        values.push(value);
    }
    json!(values)
}

fn earliest_knob(knob: &str, manifest: &Manifest) -> &'static str {
    match knob {
        "steering" | "effort" => "next prompt boundary",
        "plan_critics" if stage_started(manifest, "plan-critiques") => "no longer applicable",
        "plan_critics" => "plan-critiques",
        "codex_reviewers" if manifest.normal_review_outcome.is_some() => "no longer applicable",
        "codex_reviewers" => "next review cohort",
        _ => "no longer applicable",
    }
}

fn current_stage(manifest: &Manifest) -> String {
    if matches!(manifest.status.as_str(), "complete" | "failed") {
        return "terminal".to_string();
    }
    for (index, event) in manifest.events.iter().enumerate().rev() {
        if event.get("event").and_then(Value::as_str) != Some("stage_started") {
            continue;
        }
        let Some(stage) = event.get("stage").and_then(Value::as_str) else {
            continue;
        };
        let mut completed = false;
        for later in &manifest.events[index + 1..] {
            if later.get("stage").and_then(Value::as_str) == Some(stage)
                && matches!(
                    later.get("event").and_then(Value::as_str),
                    Some("stage_completed" | "stage_failed")
                )
            {
                completed = true;
                break;
            }
        }
        if !completed {
            return stage.to_string();
        }
    }
    if manifest.status == "initializing" {
        "initializing".to_string()
    } else {
        "between stages".to_string()
    }
}

fn durable_replace(path: &Path, bytes: &[u8], mode: u32) -> FactoryResult<()> {
    let Some(parent) = path.parent() else {
        return Err(FactoryError::new("control path has no parent"));
    };
    let temporary = parent.join(format!(
        ".stage-{}-{}-{}",
        std::process::id(),
        STAGE_COUNTER.fetch_add(1, Ordering::Relaxed),
        sha256_hex(bytes).get(..12).unwrap_or("control")
    ));
    let result = publish_control(path, bytes, mode, parent, &temporary);
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn publish_control(
    path: &Path,
    bytes: &[u8],
    mode: u32,
    parent: &Path,
    temporary: &Path,
) -> FactoryResult<()> {
    let mut file = io_result(
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(temporary),
        "create staged control file",
        temporary,
    )?;
    io_result(
        file.write_all(bytes),
        "write staged control file",
        temporary,
    )?;
    io_result(file.sync_all(), "sync staged control file", temporary)?;
    io_result(
        fs::set_permissions(temporary, fs::Permissions::from_mode(mode)),
        "chmod staged control file",
        temporary,
    )?;
    io_result(fs::rename(temporary, path), "publish control file", path)?;
    let directory = io_result(File::open(parent), "open controls directory", parent)?;
    io_result(directory.sync_all(), "sync controls directory", parent)
}

fn write_ledger(open: &OpenRun, ledger: &Ledger) -> FactoryResult<()> {
    durable_replace(
        &open.run_dir.join("controls/state.json"),
        &json_bytes(ledger)?,
        0o600,
    )
}

fn json_bytes<T: Serialize>(value: &T) -> FactoryResult<Vec<u8>> {
    let bytes = result_context(
        serde_json::to_vec_pretty(value),
        "could not serialize control data",
    )?;
    Ok(with_newline(bytes))
}

fn with_newline(mut bytes: Vec<u8>) -> Vec<u8> {
    bytes.push(b'\n');
    bytes
}

fn writeln_result(output: &mut dyn Write, line: &str) -> FactoryResult<()> {
    match writeln!(output, "{line}") {
        Ok(()) => Ok(()),
        Err(error) => Err(FactoryError::new(format!(
            "could not write control output: {error}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ToolPaths, ToolVersions};
    use crate::manifest::AgentRecord;
    use crate::write_new_file;
    use std::os::unix::fs::symlink;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::mpsc;

    struct TestRun {
        root: PathBuf,
        run_dir: PathBuf,
        journal: Arc<Journal>,
    }

    impl TestRun {
        fn new(label: &str) -> Self {
            let requested_root = std::env::temp_dir().join(format!(
                "crab-controls-unit-{label}-{}-{}",
                std::process::id(),
                STAGE_COUNTER.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(&requested_root).unwrap();
            let root = fs::canonicalize(requested_root).unwrap();
            let run_dir = root.join(label);
            fs::create_dir(&run_dir).unwrap();
            fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o700)).unwrap();
            fs::create_dir(root.join("worktrees")).unwrap();
            let request = b"request";
            let request_sha256 = sha256_hex(request);
            let tools = ToolPaths {
                git: PathBuf::from("/usr/bin/git"),
                claude: PathBuf::from("/usr/bin/claude"),
                codex: PathBuf::from("/usr/bin/codex"),
                make: PathBuf::from("/usr/bin/make"),
            };
            let launch = LaunchRecord {
                run_id: label.to_string(),
                mode: "start".to_string(),
                queued_at: "2026-07-12T00:00:00Z".to_string(),
                source_prompt: root.join("source.md"),
                request_sha256: request_sha256.clone(),
                repo: root.join("repo"),
                base_ref: "HEAD".to_string(),
                base_sha: "b".repeat(40),
                source_was_dirty: false,
                allow_dirty_source: false,
                additional_review_rounds: 0,
                agent_timeout_seconds: 60,
                artifact_root: root.clone(),
                worktree_root: root.join("worktrees"),
                worktree: root.join("worktrees").join(label),
                branch: format!("factory/{label}"),
                launch_mode: Some("foreground".to_string()),
                launched_pid: Some(std::process::id()),
                proc_name: format!("code-factory-{label}"),
                launcher: None,
                effort: Some(Effort::High),
                plan_critics: Some(2),
                codex_reviewers: Some(2),
                tool_paths: Some(tools.clone()),
            };
            let manifest = Manifest::initial(
                &launch,
                1,
                run_dir.join("00-request.md"),
                tools,
                ToolVersions {
                    git: "git".to_string(),
                    claude: "claude".to_string(),
                    codex: "codex".to_string(),
                    make_tool: "make".to_string(),
                },
            )
            .unwrap();
            let journal =
                Arc::new(Journal::create(run_dir.join("manifest.json"), manifest).unwrap());
            RunLock::initialize(&run_dir, label, &request_sha256).unwrap();
            write_new_file(&run_dir.join("00-request.md"), request, 0o400).unwrap();
            launch.write(&run_dir.join("launch.json")).unwrap();
            initialize(&run_dir, label, &request_sha256).unwrap();
            Self {
                root,
                run_dir,
                journal,
            }
        }
    }

    impl Drop for TestRun {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn record(kind: &str) -> ControlRecord {
        let message = (kind == "steer").then(|| "direction".to_string());
        ControlRecord {
            schema_version: 1,
            sequence: 1,
            kind: kind.to_string(),
            created_at: "2026-07-12T00:00:00Z".to_string(),
            run_id: "run".to_string(),
            request_sha256: "a".repeat(64),
            payload_sha256: message.as_ref().map_or_else(
                || "payload".to_string(),
                |value| sha256_hex(value.as_bytes()),
            ),
            message,
            effort: (kind == "configure").then_some(Effort::Max),
            plan_critics: None,
            codex_reviewers: None,
        }
    }

    fn rewrite_json(path: &Path, value: &Value, mode: u32) {
        if path.exists() {
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        fs::write(
            path,
            with_newline(serde_json::to_vec_pretty(value).unwrap()),
        )
        .unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
    }

    fn control_invalid_count(journal: &Journal) -> usize {
        journal
            .snapshot()
            .unwrap()
            .events
            .iter()
            .filter(|event| event["event"] == "control_invalid")
            .count()
    }

    #[test]
    fn filenames_are_strict() {
        assert_eq!(parse_filename("000001-steer.json").unwrap(), (1, "steer"));
        for invalid in [
            "1-steer.json",
            "000001-steer.txt",
            "000001x-steer.json",
            "000001steer.json",
            "000001-unknown.json",
        ] {
            assert!(parse_filename(invalid).is_err(), "{invalid}");
        }
        assert_eq!(next_sequence(None).unwrap(), 1);
        assert_eq!(next_sequence(Some(41)).unwrap(), 42);
        assert!(next_sequence(Some(MAX_SEQUENCE)).is_err());
    }

    #[test]
    fn messages_are_bounded() {
        assert!(validate_message("  ").is_err());
        assert!(validate_message(&"x".repeat(MAX_MESSAGE_BYTES + 1)).is_err());
        validate_message("direction").unwrap();
    }

    #[test]
    fn ledger_entries_bind_exact_bytes_knobs_and_disposition_shapes() {
        let record = record("steer");
        let bytes = json_bytes(&record).unwrap();
        let mut entry = LedgerEntry {
            sequence: 1,
            kind: "steer".to_string(),
            filename: "000001-steer.json".to_string(),
            payload_sha256: record.payload_sha256.clone(),
            record_sha256: sha256_hex(&bytes),
            dispositions: vec![Disposition {
                knob: "steering".to_string(),
                state: "accepted".to_string(),
                stage: None,
                at: None,
                reason: None,
            }],
        };
        validate_ledger_entry(1, &record, "000001-steer.json", &bytes, &entry).unwrap();
        let mut changed_bytes = bytes.clone();
        changed_bytes.push(b' ');
        assert!(
            validate_ledger_entry(1, &record, "000001-steer.json", &changed_bytes, &entry).is_err()
        );
        entry.dispositions[0].state = "applied".to_string();
        assert!(validate_ledger_entry(1, &record, "000001-steer.json", &bytes, &entry).is_err());
        entry.dispositions[0].stage = Some("planning".to_string());
        entry.dispositions[0].at = Some("2026-07-12T00:00:00Z".to_string());
        validate_ledger_entry(1, &record, "000001-steer.json", &bytes, &entry).unwrap();
        for invalid_time in ["garbage", "2026-07-12T02:00:00+02:00"] {
            entry.dispositions[0].at = Some(invalid_time.to_string());
            assert!(
                validate_ledger_entry(1, &record, "000001-steer.json", &bytes, &entry).is_err()
            );
        }
        entry.dispositions[0].state = "rejected".to_string();
        entry.dispositions[0].stage = None;
        entry.dispositions[0].at = Some("2026-07-12T00:00:00Z".to_string());
        for reason in ["", "invented reason"] {
            entry.dispositions[0].reason = Some(reason.to_string());
            assert!(
                validate_ledger_entry(1, &record, "000001-steer.json", &bytes, &entry).is_err()
            );
        }
        entry.dispositions[0].reason = Some("run terminal before an applicable boundary".into());
        validate_ledger_entry(1, &record, "000001-steer.json", &bytes, &entry).unwrap();
        entry.dispositions[0].state = "bogus".to_string();
        assert!(validate_ledger_entry(1, &record, "000001-steer.json", &bytes, &entry).is_err());
        entry.dispositions.push(Disposition {
            knob: "steering".to_string(),
            state: "accepted".to_string(),
            stage: None,
            at: None,
            reason: None,
        });
        assert!(validate_ledger_entry(1, &record, "000001-steer.json", &bytes, &entry).is_err());
    }

    #[test]
    fn control_json_rejects_unknown_fields() {
        let mut value = serde_json::to_value(record("steer")).unwrap();
        value["unknown"] = json!(true);
        assert!(serde_json::from_value::<ControlRecord>(value).is_err());
    }

    #[test]
    fn direct_sync_applies_each_knob_at_its_boundary_and_is_idempotent() {
        let run = TestRun::new("sync");
        let mut output = Vec::new();
        steer(&run.run_dir, "direction".to_string(), &mut output).unwrap();
        configure(
            &run.run_dir,
            Some(Effort::Max),
            Some(1),
            Some(3),
            &mut output,
        )
        .unwrap();
        let plane = ControlPlane::new(run.run_dir.clone(), Arc::clone(&run.journal));
        let first = plane
            .sync(Boundary::PlanCritiques("plan-critiques"))
            .unwrap();
        assert_eq!(first.configuration.effort, Effort::Max);
        assert_eq!(first.configuration.plan_critics, 1);
        assert_eq!(first.configuration.codex_reviewers, 2);
        assert_eq!(first.steering.len(), 1);
        let repeated = plane.sync(Boundary::Prompt("critique-synthesis")).unwrap();
        assert_eq!(repeated.steering.len(), 1);
        let reviewed = plane.sync(Boundary::Reviews("normal-reviews")).unwrap();
        assert_eq!(reviewed.configuration.codex_reviewers, 3);
        let events = run.journal.snapshot().unwrap().events;
        assert_eq!(
            events
                .iter()
                .filter(|event| event["event"] == "control_accepted")
                .count(),
            2
        );
    }

    #[test]
    fn direct_status_and_terminal_sweep_render_every_disposition() {
        let run = TestRun::new("terminal");
        let mut output = Vec::new();
        configure(
            &run.run_dir,
            Some(Effort::Max),
            Some(4),
            Some(4),
            &mut output,
        )
        .unwrap();
        let mut human = Vec::new();
        status(&run.run_dir, false, &mut human).unwrap();
        let human = String::from_utf8(human).unwrap();
        assert!(human.contains("current stage: initializing"));
        assert!(human.contains("control 1 configure"));
        terminalize(&run.run_dir, &run.journal, || run.journal.complete("clean")).unwrap();
        let ledger = read_ledger(&open_run(&run.run_dir, false).unwrap()).unwrap();
        let dispositions = &ledger.sequences[&1].dispositions;
        assert!(dispositions.iter().all(|item| item.state == "rejected"));
        assert!(dispositions
            .iter()
            .any(|item| item.reason.as_deref() == Some("no remaining workers")));
        assert!(dispositions.iter().any(|item| {
            item.reason.as_deref() == Some("no review cohort launched after acceptance")
        }));
        let mut json_output = Vec::new();
        status(&run.run_dir, true, &mut json_output).unwrap();
        let payload: Value = serde_json::from_slice(&json_output).unwrap();
        assert_eq!(payload["current_stage"], "terminal");
        assert!(steer(&run.run_dir, "late".to_string(), &mut Vec::new()).is_err());
    }

    #[test]
    fn direct_validation_rejects_malformed_controls_and_unsafe_nodes() {
        let run = TestRun::new("invalid");
        assert!(configure(&run.run_dir, None, None, None, &mut Vec::new()).is_err());
        assert!(configure(&run.run_dir, None, Some(0), None, &mut Vec::new()).is_err());
        assert!(configure(&run.run_dir, None, None, Some(9), &mut Vec::new()).is_err());
        assert!(status(Path::new("/definitely/missing"), true, &mut Vec::new()).is_err());

        let record_path = run.run_dir.join("controls/000001-steer.json");
        fs::write(&record_path, b"not json").unwrap();
        fs::set_permissions(&record_path, fs::Permissions::from_mode(0o400)).unwrap();
        let plane = ControlPlane::new(run.run_dir.clone(), Arc::clone(&run.journal));
        assert!(plane.sync(Boundary::Prompt("planning")).is_err());
        assert!(plane.sync(Boundary::Prompt("planning")).is_err());
        assert_eq!(control_invalid_count(&run.journal), 1);
        fs::remove_file(record_path).unwrap();
        plane.sync(Boundary::Prompt("planning")).unwrap();

        let controls = run.run_dir.join("controls");
        fs::set_permissions(&controls, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(status(&run.run_dir, true, &mut Vec::new()).is_err());
    }

    #[test]
    fn pure_status_and_disposition_helpers_cover_edge_states() {
        let run = TestRun::new("helpers");
        let mut manifest = run.journal.snapshot().unwrap();
        assert_eq!(Boundary::Prompt("planning").stage(), "planning");
        assert_eq!(Boundary::Reviews("reviews").stage(), "reviews");
        assert_eq!(current_stage(&manifest), "initializing");
        manifest.status = "running".to_string();
        manifest.events.push(json!({"event": "other"}));
        manifest.events.push(json!({"event": "stage_started"}));
        manifest
            .events
            .push(json!({"event": "stage_started", "stage": "one"}));
        assert_eq!(current_stage(&manifest), "one");
        manifest
            .events
            .push(json!({"event": "stage_completed", "stage": "one"}));
        assert_eq!(current_stage(&manifest), "between stages");
        assert_eq!(earliest_knob("steering", &manifest), "next prompt boundary");
        assert_eq!(earliest_knob("plan_critics", &manifest), "plan-critiques");
        manifest
            .events
            .push(json!({"event": "stage_started", "stage": "plan-critiques"}));
        assert_eq!(
            earliest_knob("plan_critics", &manifest),
            "no longer applicable"
        );
        manifest.normal_review_outcome = Some("clean".to_string());
        assert_eq!(
            earliest_knob("codex_reviewers", &manifest),
            "no longer applicable"
        );
        assert_eq!(earliest_knob("unknown", &manifest), "no longer applicable");

        let configure = record("configure");
        assert_eq!(expected_knobs(&configure), vec!["effort"]);
        let entry = LedgerEntry {
            sequence: 1,
            kind: "configure".to_string(),
            filename: "000001-configure.json".to_string(),
            payload_sha256: configure.payload_sha256,
            record_sha256: "hash".to_string(),
            dispositions: vec![Disposition {
                knob: "effort".to_string(),
                state: "accepted".to_string(),
                stage: None,
                at: None,
                reason: None,
            }],
        };
        assert!(status_dispositions(&entry, &manifest)[0]["earliest_stage"].is_string());
    }

    #[test]
    fn configuration_reconstruction_accepts_only_ordered_applied_prefixes() {
        let run = TestRun::new("prefixes");
        let open = open_run(&run.run_dir, false).unwrap();
        let mut control = record("configure");
        control.plan_critics = Some(1);
        control.codex_reviewers = Some(3);
        let bytes = json_bytes(&control).unwrap();
        let records = BTreeMap::from([(
            1,
            ScannedRecord {
                record: control.clone(),
                bytes: bytes.clone(),
            },
        )]);
        let applied = |knob: &str| Disposition {
            knob: knob.to_string(),
            state: "applied".to_string(),
            stage: Some(
                match knob {
                    "plan_critics" => "plan-critiques",
                    "codex_reviewers" => "normal-reviews",
                    _ => "planning",
                }
                .to_string(),
            ),
            at: Some("now".to_string()),
            reason: None,
        };
        let ledger = Ledger {
            schema_version: 1,
            run_id: "prefixes".to_string(),
            request_sha256: open.marker.request_sha256.clone(),
            sequences: BTreeMap::from([(
                1,
                LedgerEntry {
                    sequence: 1,
                    kind: "configure".to_string(),
                    filename: "000001-configure.json".to_string(),
                    payload_sha256: control.payload_sha256,
                    record_sha256: sha256_hex(&bytes),
                    dispositions: vec![
                        applied("effort"),
                        applied("plan_critics"),
                        applied("codex_reviewers"),
                    ],
                },
            )]),
        };
        for candidate in [
            FactoryConfiguration {
                effort: Effort::High,
                plan_critics: 2,
                codex_reviewers: 2,
            },
            FactoryConfiguration {
                effort: Effort::Max,
                plan_critics: 2,
                codex_reviewers: 2,
            },
            FactoryConfiguration {
                effort: Effort::Max,
                plan_critics: 1,
                codex_reviewers: 2,
            },
            FactoryConfiguration {
                effort: Effort::Max,
                plan_critics: 1,
                codex_reviewers: 3,
            },
        ] {
            assert!(configuration_is_applied_prefix(&open, &records, &ledger, &candidate).unwrap());
        }
        let impossible = FactoryConfiguration {
            effort: Effort::High,
            plan_critics: 8,
            codex_reviewers: 8,
        };
        assert!(!configuration_is_applied_prefix(&open, &records, &ledger, &impossible).unwrap());
        assert!(validate_projected_configuration(&open, &records, &ledger, false).is_err());
        validate_projected_configuration(&open, &records, &ledger, true).unwrap();
    }

    #[test]
    fn late_plan_control_projects_rejection_and_terminalize_audits_unsafe_lock() {
        let run = TestRun::new("late-plan");
        configure(&run.run_dir, None, Some(4), None, &mut Vec::new()).unwrap();
        let plane = ControlPlane::new(run.run_dir.clone(), Arc::clone(&run.journal));
        let applied = plane.sync(Boundary::Prompt("implementation")).unwrap();
        assert_eq!(applied.configuration.plan_critics, 2);
        assert!(run.journal.snapshot().unwrap().events.iter().any(|event| {
            event["event"] == "control_rejected" && event["knob"] == "plan_critics"
        }));

        let lock = run.run_dir.join("controls/.controls.lock");
        fs::set_permissions(&lock, fs::Permissions::from_mode(0o400)).unwrap();
        let called = std::cell::Cell::new(false);
        terminalize(&run.run_dir, &run.journal, || {
            called.set(true);
            Ok(())
        })
        .unwrap();
        assert!(called.get());
        assert_eq!(control_invalid_count(&run.journal), 1);
        fs::set_permissions(&lock, fs::Permissions::from_mode(0o600)).unwrap();
    }

    #[test]
    fn terminalization_blocks_behind_writer_and_sweeps_before_terminal_write() {
        let run = TestRun::new("terminal-contention");
        steer(&run.run_dir, "direction".to_string(), &mut Vec::new()).unwrap();
        configure(
            &run.run_dir,
            Some(Effort::Max),
            Some(2),
            Some(2),
            &mut Vec::new(),
        )
        .unwrap();
        let held = acquire_control_lock(&run.run_dir).unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let run_dir = run.run_dir.clone();
        let journal = Arc::clone(&run.journal);
        let handle = thread::spawn(move || {
            started_tx.send(()).unwrap();
            terminalize(&run_dir, &journal, || journal.complete("clean")).unwrap();
            done_tx.send(()).unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(run.journal.snapshot().unwrap().status, "initializing");
        assert!(done_rx.try_recv().is_err());
        drop(held);
        done_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        handle.join().unwrap();
        let (open, _, ledger) = load_validated(&run.run_dir, false, false).unwrap();
        assert_eq!(open.manifest.status, "complete");
        assert!(ledger.sequences.values().all(|entry| entry
            .dispositions
            .iter()
            .all(|disposition| disposition.state != "accepted")));
        assert!(steer(&run.run_dir, "late".into(), &mut Vec::new()).is_err());
    }

    #[test]
    fn concurrent_writers_and_terminalizer_leave_no_pending_controls() {
        let run = TestRun::new("terminal-race");
        let barrier = Arc::new(std::sync::Barrier::new(5));
        let results = thread::scope(|scope| {
            let handles = (0..4)
                .map(|index| {
                    let barrier = Arc::clone(&barrier);
                    let run_dir = run.run_dir.clone();
                    scope.spawn(move || {
                        barrier.wait();
                        steer(&run_dir, format!("message {index}"), &mut Vec::new())
                    })
                })
                .collect::<Vec<_>>();
            barrier.wait();
            terminalize(&run.run_dir, &run.journal, || run.journal.complete("clean")).unwrap();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });
        let successful = results.iter().filter(|result| result.is_ok()).count();
        assert!(results
            .iter()
            .filter_map(|result| result.as_ref().err())
            .all(|error| error.to_string().contains("run is terminal")));
        let (_, _, ledger) = load_validated(&run.run_dir, false, false).unwrap();
        assert_eq!(ledger.sequences.len(), successful);
        assert!(ledger.sequences.values().all(|entry| entry
            .dispositions
            .iter()
            .all(|disposition| disposition.state != "accepted")));
    }

    #[test]
    fn busy_writer_lock_reports_retry_without_publishing() {
        let run = TestRun::new("busy");
        let held = acquire_control_lock(&run.run_dir).unwrap();
        let error = acquire_control_lock_with_deadline(&run.run_dir, Duration::ZERO).unwrap_err();
        assert!(matches!(error, ControlLockError::Busy));
        drop(held);
        let names = fs::read_dir(run.run_dir.join("controls"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        assert_eq!(names.len(), 2);
    }

    #[test]
    fn queue_rechecks_terminal_state_under_the_control_lock() {
        let run = TestRun::new("stale-open");
        let open = open_run(&run.run_dir, true).unwrap();
        run.journal.fail("terminalized elsewhere").unwrap();
        let mut control = record("steer");
        control.run_id = open.marker.run_id.clone();
        control.request_sha256 = open.marker.request_sha256.clone();
        assert!(queue(open, control, &mut Vec::new())
            .unwrap_err()
            .to_string()
            .contains("run is terminal"));
        assert_eq!(
            fs::read_dir(run.run_dir.join("controls"))
                .unwrap()
                .filter(|entry| entry
                    .as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .ends_with("-steer.json"))
                .count(),
            0
        );
    }

    #[test]
    fn public_queue_cleans_stale_stage_files_and_rejects_stage_directories() {
        let run = TestRun::new("staging-cleanup");
        let controls = run.run_dir.join("controls");
        let stale = controls.join(".stage-stale");
        fs::write(&stale, b"stale").unwrap();
        steer(&run.run_dir, "first".into(), &mut Vec::new()).unwrap();
        assert!(!stale.exists());

        let invalid = controls.join(".stage-directory");
        fs::create_dir(&invalid).unwrap();
        fs::write(invalid.join("child"), b"child").unwrap();
        assert!(steer(&run.run_dir, "second".into(), &mut Vec::new())
            .unwrap_err()
            .to_string()
            .contains("remove stale staged control file"));
        fs::remove_dir_all(invalid).unwrap();
        assert!(!controls.join("000002-steer.json").exists());
    }

    #[test]
    fn public_status_rejects_nonconforming_duplicate_and_gapped_record_sets() {
        let run = TestRun::new("record-set-shapes");
        let controls = run.run_dir.join("controls");
        for name in [
            "1-steer.json",
            "000001-steer.txt",
            "000001x-steer.json",
            "000001steer.json",
            "000001-unknown.json",
        ] {
            let path = controls.join(name);
            fs::write(&path, b"{}\n").unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o400)).unwrap();
            assert!(
                status(&run.run_dir, true, &mut Vec::new()).is_err(),
                "{name}"
            );
            fs::remove_file(path).unwrap();
        }

        steer(&run.run_dir, "first".into(), &mut Vec::new()).unwrap();
        let first = controls.join("000001-steer.json");
        fs::copy(&first, controls.join("000001-configure.json")).unwrap();
        fs::set_permissions(
            controls.join("000001-configure.json"),
            fs::Permissions::from_mode(0o400),
        )
        .unwrap();
        assert!(status(&run.run_dir, true, &mut Vec::new()).is_err());
        fs::remove_file(controls.join("000001-configure.json")).unwrap();

        let mut second: Value = serde_json::from_slice(&fs::read(&first).unwrap()).unwrap();
        second["sequence"] = json!(2);
        let second_path = controls.join("000002-steer.json");
        rewrite_json(&second_path, &second, 0o400);
        fs::remove_file(first).unwrap();
        assert!(status(&run.run_dir, true, &mut Vec::new())
            .unwrap_err()
            .to_string()
            .contains("sequence gap"));
    }

    #[test]
    fn public_status_rejects_transplanted_and_orphaned_ledgers() {
        let first = TestRun::new("ledger-first");
        let second = TestRun::new("ledger-second");
        for run in [&first, &second] {
            steer(&run.run_dir, "direction".into(), &mut Vec::new()).unwrap();
            ControlPlane::new(run.run_dir.clone(), Arc::clone(&run.journal))
                .sync(Boundary::Prompt("planning"))
                .unwrap();
        }
        let first_ledger = first.run_dir.join("controls/state.json");
        let valid = fs::read(&first_ledger).unwrap();
        fs::set_permissions(&first_ledger, fs::Permissions::from_mode(0o600)).unwrap();
        fs::write(
            &first_ledger,
            fs::read(second.run_dir.join("controls/state.json")).unwrap(),
        )
        .unwrap();
        assert!(status(&first.run_dir, true, &mut Vec::new())
            .unwrap_err()
            .to_string()
            .contains("ledger identity mismatch"));
        fs::write(&first_ledger, valid).unwrap();
        let record_path = first.run_dir.join("controls/000001-steer.json");
        let record = fs::read(&record_path).unwrap();
        fs::remove_file(&record_path).unwrap();
        assert!(status(&first.run_dir, true, &mut Vec::new())
            .unwrap_err()
            .to_string()
            .contains("orphaned controls ledger entry"));
        fs::write(&record_path, record).unwrap();
        fs::set_permissions(&record_path, fs::Permissions::from_mode(0o400)).unwrap();
    }

    #[test]
    fn public_status_rejects_record_and_ledger_time_reason_corruption() {
        let run = TestRun::new("corruption");
        steer(&run.run_dir, "direction".into(), &mut Vec::new()).unwrap();
        let record_path = run.run_dir.join("controls/000001-steer.json");
        let valid_record: Value = serde_json::from_slice(&fs::read(&record_path).unwrap()).unwrap();
        for (field, value) in [
            ("created_at", json!("garbage")),
            ("run_id", json!("another-run")),
            ("sequence", json!(2)),
            ("kind", json!("configure")),
            ("schema_version", json!(2)),
            ("payload_sha256", json!("0".repeat(64))),
        ] {
            let mut changed = valid_record.clone();
            changed[field] = value;
            rewrite_json(&record_path, &changed, 0o400);
            assert!(
                status(&run.run_dir, true, &mut Vec::new()).is_err(),
                "{field}"
            );
        }
        rewrite_json(&record_path, &valid_record, 0o400);
        for (field, value) in [("message", Value::Null), ("effort", json!("max"))] {
            let mut changed = valid_record.clone();
            changed[field] = value;
            rewrite_json(&record_path, &changed, 0o400);
            assert!(
                status(&run.run_dir, true, &mut Vec::new()).is_err(),
                "{field}"
            );
        }
        rewrite_json(&record_path, &valid_record, 0o400);
        configure(
            &run.run_dir,
            Some(Effort::Max),
            Some(3),
            Some(4),
            &mut Vec::new(),
        )
        .unwrap();
        let configure_path = run.run_dir.join("controls/000002-configure.json");
        let valid_configure: Value =
            serde_json::from_slice(&fs::read(&configure_path).unwrap()).unwrap();
        for changed in [
            {
                let mut value = valid_configure.clone();
                value["message"] = json!("not allowed");
                value
            },
            {
                let mut value = valid_configure.clone();
                value["effort"] = Value::Null;
                value
            },
            {
                let mut value = valid_configure.clone();
                value["plan_critics"] = json!(9);
                value
            },
            {
                let mut value = valid_configure.clone();
                value["codex_reviewers"] = json!(9);
                value
            },
        ] {
            rewrite_json(&configure_path, &changed, 0o400);
            assert!(status(&run.run_dir, true, &mut Vec::new()).is_err());
        }
        rewrite_json(&configure_path, &valid_configure, 0o400);
        let plane = ControlPlane::new(run.run_dir.clone(), Arc::clone(&run.journal));
        plane.sync(Boundary::Prompt("planning")).unwrap();
        let ledger_path = run.run_dir.join("controls/state.json");
        let valid_ledger: Value = serde_json::from_slice(&fs::read(&ledger_path).unwrap()).unwrap();
        let corruptions = [
            ("state", json!("bogus")),
            ("at", json!("garbage")),
            ("stage", json!("unknown-stage")),
        ];
        for (field, value) in corruptions {
            let mut changed = valid_ledger.clone();
            changed["sequences"]["1"]["dispositions"][0][field] = value;
            rewrite_json(&ledger_path, &changed, 0o600);
            assert!(
                status(&run.run_dir, true, &mut Vec::new()).is_err(),
                "{field}"
            );
        }
        for (field, value) in [
            ("sequence", json!(2)),
            ("kind", json!("configure")),
            ("filename", json!("wrong.json")),
            ("payload_sha256", json!("0".repeat(64))),
            ("record_sha256", json!("0".repeat(64))),
        ] {
            let mut changed = valid_ledger.clone();
            changed["sequences"]["1"][field] = value;
            rewrite_json(&ledger_path, &changed, 0o600);
            assert!(
                status(&run.run_dir, true, &mut Vec::new()).is_err(),
                "{field}"
            );
        }
        for dispositions in [
            json!([]),
            json!([
                {"knob": "steering", "state": "accepted", "stage": null, "at": null, "reason": null},
                {"knob": "steering", "state": "accepted", "stage": null, "at": null, "reason": null}
            ]),
            json!([{"knob": "effort", "state": "accepted", "stage": null, "at": null, "reason": null}]),
        ] {
            let mut changed = valid_ledger.clone();
            changed["sequences"]["1"]["dispositions"] = dispositions;
            rewrite_json(&ledger_path, &changed, 0o600);
            assert!(status(&run.run_dir, true, &mut Vec::new()).is_err());
        }
        let mut wrong_order = valid_ledger.clone();
        wrong_order["sequences"]["2"]["dispositions"]
            .as_array_mut()
            .unwrap()
            .reverse();
        rewrite_json(&ledger_path, &wrong_order, 0o600);
        assert!(status(&run.run_dir, true, &mut Vec::new()).is_err());
        for stage in [
            "review-round-x",
            "review-round-1/synthesis.md",
            "review-round-aa/synthesis.md",
            "review-round-00/synthesis.md",
            "review-round-01/other.md",
        ] {
            let mut changed = valid_ledger.clone();
            changed["sequences"]["1"]["dispositions"][0]["stage"] = json!(stage);
            rewrite_json(&ledger_path, &changed, 0o600);
            assert!(
                status(&run.run_dir, true, &mut Vec::new()).is_err(),
                "{stage}"
            );
        }
        for stage in [
            "review-round-01/synthesis.md",
            "review-round-100/address.md",
        ] {
            let mut changed = valid_ledger.clone();
            changed["sequences"]["1"]["dispositions"][0]["stage"] = json!(stage);
            rewrite_json(&ledger_path, &changed, 0o600);
            status(&run.run_dir, true, &mut Vec::new()).unwrap();
        }
        for field in ["at", "reason"] {
            let mut changed = valid_ledger.clone();
            changed["sequences"]["1"]["dispositions"][0] = json!({
                "knob": "steering", "state": "rejected", "stage": null,
                "at": "2026-07-12T00:00:00Z", "reason": "run terminal before an applicable boundary"
            });
            changed["sequences"]["1"]["dispositions"][0][field] = Value::Null;
            rewrite_json(&ledger_path, &changed, 0o600);
            assert!(
                status(&run.run_dir, true, &mut Vec::new()).is_err(),
                "{field}"
            );
        }
        let mut rejected = valid_ledger.clone();
        rejected["sequences"]["1"]["dispositions"][0] = json!({
            "knob": "steering", "state": "rejected", "stage": null,
            "at": "2026-07-12T00:00:00Z", "reason": "invented reason"
        });
        rewrite_json(&ledger_path, &rejected, 0o600);
        assert!(status(&run.run_dir, true, &mut Vec::new()).is_err());
        rewrite_json(&ledger_path, &valid_ledger, 0o600);
        status(&run.run_dir, true, &mut Vec::new()).unwrap();
    }

    #[test]
    fn ledger_ahead_is_readable_repairable_and_does_not_duplicate_projection() {
        let run = TestRun::new("ledger-ahead");
        configure(&run.run_dir, Some(Effort::Max), None, None, &mut Vec::new()).unwrap();
        let open = open_run(&run.run_dir, false).unwrap();
        let records = scan_records(&open).unwrap();
        let mut ledger = read_ledger(&open).unwrap();
        ingest_records(&records, &mut ledger).unwrap();
        let disposition = &mut ledger.sequences.get_mut(&1).unwrap().dispositions[0];
        disposition.state = "applied".into();
        disposition.stage = Some("planning".into());
        disposition.at = Some("2026-07-12T00:00:00Z".into());
        write_ledger(&open, &ledger).unwrap();

        status(&run.run_dir, true, &mut Vec::new()).unwrap();
        steer(&run.run_dir, "after crash".into(), &mut Vec::new()).unwrap();
        let plane = ControlPlane::new(run.run_dir.clone(), Arc::clone(&run.journal));
        let applied = plane.sync(Boundary::Prompt("planning")).unwrap();
        assert_eq!(applied.configuration.effort, Effort::Max);
        plane.sync(Boundary::Prompt("planning")).unwrap();
        let events = run.journal.snapshot().unwrap().events;
        for (sequence, knob) in [(1, "effort"), (2, "steering")] {
            assert_eq!(
                events
                    .iter()
                    .filter(|event| {
                        event["event"] == "control_applied"
                            && event["sequence"] == sequence
                            && event["knob"] == knob
                    })
                    .count(),
                1
            );
        }
    }

    #[test]
    fn mixed_late_configuration_applies_effort_rejects_count_and_warns_reviewers() {
        let run = TestRun::new("mixed-late");
        run.journal
            .event("stage_started", json!({"stage": "plan-critiques"}))
            .unwrap();
        let mut output = Vec::new();
        configure(&run.run_dir, Some(Effort::Max), Some(4), None, &mut output).unwrap();
        assert!(String::from_utf8(output)
            .unwrap()
            .contains("already launched"));
        let plane = ControlPlane::new(run.run_dir.clone(), Arc::clone(&run.journal));
        let applied = plane
            .sync(Boundary::Prompt("04-address-critiques.md"))
            .unwrap();
        assert_eq!(applied.configuration.effort, Effort::Max);
        assert_eq!(applied.configuration.plan_critics, 2);

        run.journal.checkpoint_review(1, Some("clean")).unwrap();
        let mut output = Vec::new();
        configure(&run.run_dir, None, None, Some(3), &mut output).unwrap();
        assert!(String::from_utf8(output)
            .unwrap()
            .contains("normal review cohorts already completed"));
        terminalize(&run.run_dir, &run.journal, || run.journal.complete("clean")).unwrap();
        let (_, _, ledger) = load_validated(&run.run_dir, false, false).unwrap();
        assert_eq!(
            ledger.sequences[&2].dispositions[0].reason.as_deref(),
            Some("no review cohort launched after acceptance")
        );
    }

    #[test]
    fn symlinked_run_argument_and_output_failure_are_rejected() {
        let run = TestRun::new("symlink-run");
        let alias = run.root.join("alias");
        symlink(&run.run_dir, &alias).unwrap();
        assert!(status(&alias, true, &mut Vec::new()).is_err());

        struct FailingWriter;
        impl Write for FailingWriter {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("injected output failure"))
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        assert!(status(&run.run_dir, false, &mut FailingWriter).is_err());
    }

    #[test]
    fn human_status_covers_active_between_stage_applied_and_legacy_views() {
        let run = TestRun::new("status-views");
        steer(&run.run_dir, "direction".into(), &mut Vec::new()).unwrap();
        let plane = ControlPlane::new(run.run_dir.clone(), Arc::clone(&run.journal));
        plane.sync(Boundary::Prompt("planning")).unwrap();
        run.journal.set_running().unwrap();
        run.journal
            .event("stage_started", json!({"stage": "planning"}))
            .unwrap();
        run.journal
            .event("stage_completed", json!({"stage": "planning"}))
            .unwrap();
        run.journal
            .agent_started(
                "worker-one".into(),
                AgentRecord {
                    provider: "codex".into(),
                    command: vec!["codex".into()],
                    sandbox: "disabled".into(),
                    permission_mode: "full".into(),
                    network_access: true,
                    prompt_sha256: "a".repeat(64),
                    status: "running".into(),
                    started_at: "2026-07-12T00:00:00Z".into(),
                    finished_at: None,
                    output: run.run_dir.join("output.md"),
                    log: run.run_dir.join("worker.log"),
                    returncode: None,
                    pid: Some(42),
                },
            )
            .unwrap();
        let base_agent = AgentRecord {
            provider: "claude-code".into(),
            command: vec!["claude".into()],
            sandbox: "disabled".into(),
            permission_mode: "full".into(),
            network_access: true,
            prompt_sha256: "b".repeat(64),
            status: "running".into(),
            started_at: "2026-07-12T00:00:01Z".into(),
            finished_at: None,
            output: run.run_dir.join("output-two.md"),
            log: run.run_dir.join("worker-two.log"),
            returncode: None,
            pid: None,
        };
        run.journal
            .agent_started("worker-two".into(), base_agent.clone())
            .unwrap();
        let mut finished = base_agent;
        finished.status = "succeeded".into();
        run.journal
            .agent_started("finished-worker".into(), finished)
            .unwrap();
        let launch_path = run.run_dir.join("launch.json");
        let mut launch = LaunchRecord::read(&launch_path).unwrap();
        launch.launch_mode = None;
        launch.launched_pid = None;
        launch.write(&launch_path).unwrap();
        let mut human = Vec::new();
        status(&run.run_dir, false, &mut human).unwrap();
        let human = String::from_utf8(human).unwrap();
        assert!(human.contains("current stage: between stages"));
        assert!(human.contains("launch: mode=unknown pid=unavailable"));
        assert!(human.contains("worker worker-one: provider=codex pid=42"));
        assert!(human.contains("worker worker-two: provider=claude-code pid=unavailable"));
        assert!(!human.contains("worker finished-worker:"));
        assert!(human.contains("last applied sequence: 1"));

        let manifest_path = run.run_dir.join("manifest.json");
        let mut manifest: Value =
            serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
        manifest
            .as_object_mut()
            .unwrap()
            .remove("prepared_configuration");
        manifest
            .as_object_mut()
            .unwrap()
            .remove("effective_configuration");
        rewrite_json(&manifest_path, &manifest, 0o600);
        let mut legacy = Vec::new();
        status(&run.run_dir, false, &mut legacy).unwrap();
        assert!(String::from_utf8(legacy)
            .unwrap()
            .contains("configuration unavailable (run predates live controls)"));
    }
}
