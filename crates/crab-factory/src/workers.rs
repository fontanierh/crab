use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs::File;
use std::io::{ErrorKind, Read, Write};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::json;

use crate::config::{
    allowlisted_environment, ToolPaths, CLAUDE_MODEL, CLAUDE_PERMISSION_MODE, CODEX_MODEL,
    CODEX_PERMISSION_MODE, REASONING_EFFORT, WORKER_NETWORK_ACCESS, WORKER_SANDBOX,
};
use crate::manifest::{AgentRecord, CohortRecord, Journal};
use crate::{
    atomic_write, create_secure_dir, io_result, open_private_file, read_bytes, required_utf8,
    sha256_hex, utc_now_rfc3339, FactoryError, FactoryResult,
};

const MAX_POLL_INTERVAL: Duration = Duration::from_millis(100);
// Catch immediate probe/fake failures promptly, then avoid busy polling multi-hour model workers.
const STARTUP_POLL_INTERVAL: Duration = Duration::from_millis(1);
const STARTUP_POLL_LIMIT: usize = 250;
const MAX_CAPTURE_BYTES: usize = 1_048_576;
const ERROR_EXCERPT_BYTES: usize = 2_000;

#[derive(Debug, Clone)]
pub(crate) struct CancelFlags {
    pub(crate) global: Arc<AtomicBool>,
    pub(crate) cohort: Arc<AtomicBool>,
}

impl CancelFlags {
    pub(crate) fn global_only(global: Arc<AtomicBool>) -> Self {
        Self {
            global,
            cohort: Arc::new(AtomicBool::new(false)),
        }
    }

    fn cancelled(&self) -> bool {
        self.global.load(Ordering::SeqCst) || self.cohort.load(Ordering::SeqCst)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SupervisorErrorKind {
    Cancelled,
    TimedOut,
    Other,
}

#[derive(Debug)]
pub(crate) struct SupervisorError {
    pub(crate) kind: SupervisorErrorKind,
    pub(crate) message: String,
    pub(crate) process_group: Option<i32>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

impl SupervisorError {
    fn new(
        kind: SupervisorErrorKind,
        message: impl Into<String>,
        process_group: Option<i32>,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            process_group,
            stdout: Vec::new(),
            stderr: Vec::new(),
        }
    }

    pub(crate) fn detail(&self) -> String {
        let mut detail = match self.process_group {
            Some(process_group) => format!("{} (process group {process_group})", self.message),
            None => self.message.clone(),
        };
        let excerpt = combined_excerpt(&self.stdout, &self.stderr);
        if !excerpt.is_empty() {
            detail.push_str("; captured output: ");
            detail.push_str(&excerpt);
        }
        detail
    }

    pub(crate) fn captured_output(&self) -> Vec<u8> {
        let mut output = self.stdout.clone();
        if !output.is_empty() && !output.ends_with(b"\n") && !self.stderr.is_empty() {
            output.push(b'\n');
        }
        output.extend_from_slice(&self.stderr);
        output
    }

    fn process_io(
        action: &str,
        program: &Path,
        error: &std::io::Error,
        process_group: Option<i32>,
    ) -> Self {
        Self::new(
            SupervisorErrorKind::Other,
            format!("could not {action} {}: {error}", program.display()),
            process_group,
        )
    }
}

#[derive(Debug)]
pub(crate) struct CommandSpec {
    pub(crate) program: PathBuf,
    pub(crate) args: Vec<OsString>,
    pub(crate) cwd: Option<PathBuf>,
    pub(crate) input: Option<Arc<Vec<u8>>>,
    pub(crate) timeout: Duration,
    pub(crate) cancellation: CancelFlags,
    pub(crate) inherit_environment: bool,
    pub(crate) environment_overrides: BTreeMap<OsString, OsString>,
}

impl CommandSpec {
    pub(crate) fn isolated(
        program: PathBuf,
        args: Vec<OsString>,
        cwd: Option<PathBuf>,
        input: Option<Arc<Vec<u8>>>,
        timeout: Duration,
        cancellation: CancelFlags,
    ) -> Self {
        Self {
            program,
            args,
            cwd,
            input,
            timeout,
            cancellation,
            inherit_environment: false,
            environment_overrides: BTreeMap::new(),
        }
    }

    pub(crate) fn inherited(
        program: PathBuf,
        args: Vec<OsString>,
        timeout: Duration,
        cancellation: CancelFlags,
        environment_overrides: BTreeMap<OsString, OsString>,
    ) -> Self {
        let mut spec = Self::isolated(program, args, None, None, timeout, cancellation);
        spec.inherit_environment = true;
        spec.environment_overrides = environment_overrides;
        spec
    }
}

#[derive(Debug)]
pub(crate) struct ProcessResult {
    pub(crate) returncode: i32,
    pub(crate) stdout: Vec<u8>,
    pub(crate) stderr: Vec<u8>,
    pub(crate) elapsed: Duration,
}

pub(crate) enum OutputPlan {
    Capture,
    Files { stdout: File, stderr: File },
}

pub(crate) fn supervise(
    spec: CommandSpec,
    output: OutputPlan,
) -> Result<ProcessResult, SupervisorError> {
    if spec.cancellation.cancelled() {
        return Err(SupervisorError::new(
            SupervisorErrorKind::Cancelled,
            "process cancelled before spawn",
            None,
        ));
    }
    let started = Instant::now();
    let deadline = require_some!(
        started.checked_add(spec.timeout),
        SupervisorError::new(
            SupervisorErrorKind::Other,
            "process deadline overflow",
            None,
        )
    );
    let mut command = Command::new(&spec.program);
    command.args(&spec.args).stdin(if spec.input.is_some() {
        Stdio::piped()
    } else {
        Stdio::null()
    });
    if let Some(cwd) = &spec.cwd {
        command.current_dir(cwd);
    }
    if !spec.inherit_environment {
        command.env_clear().envs(allowlisted_environment());
    }
    command.envs(spec.environment_overrides);
    let capture = matches!(output, OutputPlan::Capture);
    match output {
        OutputPlan::Capture => {
            command.stdout(Stdio::piped()).stderr(Stdio::piped());
        }
        OutputPlan::Files { stdout, stderr } => {
            command
                .stdout(Stdio::from(stdout))
                .stderr(Stdio::from(stderr));
        }
    }
    command.process_group(0);
    #[rustfmt::skip]
    let mut child = try_mapped!(command.spawn(), error => SupervisorError::process_io("spawn", &spec.program, &error, None));
    let process_group = child.id() as i32;

    let writer = spec.input.map(|bytes| {
        let mut stdin = child.stdin.take().expect("piped stdin must be available");
        thread::spawn(move || write_process_input(&mut stdin, bytes.as_slice()))
    });
    let capture_overflow = Arc::new(AtomicBool::new(false));
    let stdout_reader = if capture {
        let mut stdout = child.stdout.take().expect("piped stdout must be available");
        let overflow = Arc::clone(&capture_overflow);
        Some(thread::spawn(move || read_bounded(&mut stdout, overflow)))
    } else {
        None
    };
    let stderr_reader = if capture {
        let mut stderr = child.stderr.take().expect("piped stderr must be available");
        let overflow = Arc::clone(&capture_overflow);
        Some(thread::spawn(move || read_bounded(&mut stderr, overflow)))
    } else {
        None
    };

    #[rustfmt::skip]
    let (status, mut termination) = wait_for_child(&mut child, &spec.program, process_group, deadline, spec.timeout, &spec.cancellation, &capture_overflow)?;

    let sweep_error = kill_process_group(process_group).err();
    let writer_error = join_writer(writer);
    let stdout = join_reader(stdout_reader, "stdout", process_group)?;
    let stderr = join_reader(stderr_reader, "stderr", process_group)?;
    record_late_capture_overflow(
        &mut termination,
        capture_overflow.load(Ordering::SeqCst),
        process_group,
    );
    finish_supervision(
        termination,
        sweep_error,
        writer_error,
        status.code().unwrap_or(-1),
        stdout,
        stderr,
        started.elapsed(),
        process_group,
    )
}

trait ChildWait {
    fn try_wait_status(&mut self) -> std::io::Result<Option<ExitStatus>>;
    fn wait_status(&mut self) -> std::io::Result<ExitStatus>;
}

impl ChildWait for Child {
    fn try_wait_status(&mut self) -> std::io::Result<Option<ExitStatus>> {
        self.try_wait()
    }

    fn wait_status(&mut self) -> std::io::Result<ExitStatus> {
        self.wait()
    }
}

#[allow(clippy::too_many_arguments)]
fn wait_for_child<C: ChildWait>(
    child: &mut C,
    program: &Path,
    process_group: i32,
    deadline: Instant,
    timeout: Duration,
    cancellation: &CancelFlags,
    capture_overflow: &AtomicBool,
) -> Result<(ExitStatus, Option<SupervisorError>), SupervisorError> {
    let mut poll_index = 0;
    loop {
        match child.try_wait_status() {
            Ok(Some(status)) => return Ok((status, None)),
            Ok(None) => {}
            Err(error) => {
                let failure =
                    SupervisorError::process_io("wait for", program, &error, Some(process_group));
                let _ = kill_process_group(process_group);
                #[rustfmt::skip]
                let status = try_mapped!(child.wait_status(), wait_error => SupervisorError::process_io("reap", program, &wait_error, Some(process_group)));
                return Ok((status, Some(failure)));
            }
        }
        let termination = if cancellation.cancelled() {
            Some((
                SupervisorError::new(
                    SupervisorErrorKind::Cancelled,
                    format!("{} was cancelled", program.display()),
                    Some(process_group),
                ),
                "reap cancelled process",
            ))
        } else if capture_overflow.load(Ordering::SeqCst) {
            Some((
                SupervisorError::new(
                    SupervisorErrorKind::Other,
                    format!("captured process output exceeded the {MAX_CAPTURE_BYTES}-byte limit"),
                    Some(process_group),
                ),
                "reap output-limited process",
            ))
        } else if Instant::now() >= deadline {
            Some((
                SupervisorError::new(
                    SupervisorErrorKind::TimedOut,
                    format!(
                        "{} exceeded the {}-second timeout",
                        program.display(),
                        timeout.as_secs_f64()
                    ),
                    Some(process_group),
                ),
                "reap timed-out process",
            ))
        } else {
            None
        };
        if let Some((termination, action)) = termination {
            let _ = kill_process_group(process_group);
            #[rustfmt::skip]
            let status = try_mapped!(child.wait_status(), error => SupervisorError::process_io(action, program, &error, Some(process_group)));
            return Ok((status, Some(termination)));
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        thread::sleep(poll_interval(poll_index).min(remaining));
        poll_index = poll_index.saturating_add(1);
    }
}

fn poll_interval(index: usize) -> Duration {
    if index < STARTUP_POLL_LIMIT {
        STARTUP_POLL_INTERVAL
    } else {
        MAX_POLL_INTERVAL
    }
}

fn write_process_input(writer: &mut dyn Write, bytes: &[u8]) -> std::io::Result<()> {
    match writer.write_all(bytes) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::BrokenPipe => Ok(()),
        Err(error) => Err(error),
    }
}

fn record_late_capture_overflow(
    termination: &mut Option<SupervisorError>,
    overflow: bool,
    process_group: i32,
) {
    if termination.is_none() && overflow {
        *termination = Some(SupervisorError::new(
            SupervisorErrorKind::Other,
            format!("captured process output exceeded the {MAX_CAPTURE_BYTES}-byte limit"),
            Some(process_group),
        ));
    }
}

#[allow(clippy::too_many_arguments)]
fn finish_supervision(
    termination: Option<SupervisorError>,
    sweep_error: Option<std::io::Error>,
    writer_error: Option<std::io::Error>,
    returncode: i32,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    elapsed: Duration,
    process_group: i32,
) -> Result<ProcessResult, SupervisorError> {
    if let Some(mut error) = termination {
        error.stdout = stdout;
        error.stderr = stderr;
        return Err(error);
    }
    if let Some(error) = sweep_error {
        return Err(SupervisorError::new(
            SupervisorErrorKind::Other,
            format!("could not sweep process group {process_group}: {error}"),
            Some(process_group),
        ));
    }
    if let Some(error) = writer_error {
        return Err(SupervisorError::new(
            SupervisorErrorKind::Other,
            format!("could not feed process stdin: {error}"),
            Some(process_group),
        ));
    }
    Ok(ProcessResult {
        returncode,
        stdout,
        stderr,
        elapsed,
    })
}

fn read_bounded(reader: &mut dyn Read, overflow: Arc<AtomicBool>) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 8_192];
    loop {
        let read = reader.read(&mut chunk)?;
        if read == 0 {
            return Ok(bytes);
        }
        let remaining = MAX_CAPTURE_BYTES.saturating_sub(bytes.len());
        bytes.extend_from_slice(&chunk[..read.min(remaining)]);
        if read > remaining {
            overflow.store(true, Ordering::SeqCst);
        }
    }
}

fn combined_excerpt(stdout: &[u8], stderr: &[u8]) -> String {
    let mut bytes = stdout
        .iter()
        .chain(stderr.iter())
        .copied()
        .take(ERROR_EXCERPT_BYTES)
        .collect::<Vec<_>>();
    while bytes.last().is_some_and(|byte| byte.is_ascii_whitespace()) {
        bytes.pop();
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

fn kill_process_group(process_group: i32) -> std::io::Result<()> {
    let result = unsafe { libc::kill(-process_group, libc::SIGKILL) };
    classify_group_kill(result, std::io::Error::last_os_error())
}

fn classify_group_kill(result: i32, error: std::io::Error) -> std::io::Result<()> {
    if result == 0 {
        return Ok(());
    }
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(error)
    }
}

fn join_writer(writer: Option<thread::JoinHandle<std::io::Result<()>>>) -> Option<std::io::Error> {
    writer.and_then(|handle| match handle.join() {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(error),
        Err(_) => Some(std::io::Error::other("stdin writer thread panicked")),
    })
}

fn join_reader(
    reader: Option<thread::JoinHandle<std::io::Result<Vec<u8>>>>,
    stream: &str,
    process_group: i32,
) -> Result<Vec<u8>, SupervisorError> {
    match reader {
        None => Ok(Vec::new()),
        Some(handle) => match handle.join() {
            Ok(Ok(bytes)) => Ok(bytes),
            Ok(Err(error)) => Err(SupervisorError::new(
                SupervisorErrorKind::Other,
                format!("could not read process {stream}: {error}"),
                Some(process_group),
            )),
            Err(_) => Err(SupervisorError::new(
                SupervisorErrorKind::Other,
                format!("process {stream} reader thread panicked"),
                Some(process_group),
            )),
        },
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PromptInput {
    pub(crate) path: PathBuf,
    pub(crate) bytes: Arc<Vec<u8>>,
    pub(crate) sha256: String,
}

pub(crate) fn materialize_prompt(
    run_dir: &Path,
    relative_path: &Path,
    content: String,
    journal: &Journal,
) -> FactoryResult<PromptInput> {
    let path = run_dir.join("prompts").join(relative_path);
    let parent = path.parent().unwrap_or(run_dir);
    create_secure_dir(parent)?;
    let bytes = Arc::new(content.into_bytes());
    let sha256 = sha256_hex(bytes.as_slice());
    atomic_write(&path, bytes.as_slice())?;
    journal.event("prompt_written", json!({"path": path, "sha256": sha256}))?;
    Ok(PromptInput {
        path,
        bytes,
        sha256,
    })
}

fn verify_prompt(prompt: &PromptInput, cohort: bool) -> FactoryResult<()> {
    let actual = sha256_hex(&read_bytes(&prompt.path, "prompt artifact")?);
    if actual != prompt.sha256 {
        let kind = if cohort {
            "cohort prompt"
        } else {
            "stage prompt"
        };
        return Err(FactoryError::new(format!(
            "{kind} changed during execution: {}",
            prompt.path.display()
        )));
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub(crate) struct AgentSpec {
    pub(crate) label: String,
    pub(crate) provider: String,
    pub(crate) program: PathBuf,
    pub(crate) args: Vec<OsString>,
    pub(crate) output: PathBuf,
    pub(crate) log: PathBuf,
    pub(crate) sandbox: String,
    pub(crate) permission_mode: String,
    pub(crate) network_access: bool,
    stdout_is_output: bool,
}

pub(crate) fn codex_agent(
    tools: &ToolPaths,
    run_dir: &Path,
    label: &str,
    output: PathBuf,
) -> AgentSpec {
    AgentSpec {
        label: label.to_string(),
        provider: "codex".to_string(),
        program: tools.codex.clone(),
        args: codex_arguments(&output),
        output,
        log: run_dir.join("logs").join(format!("{label}.log")),
        sandbox: WORKER_SANDBOX.to_string(),
        permission_mode: CODEX_PERMISSION_MODE.to_string(),
        network_access: WORKER_NETWORK_ACCESS,
        stdout_is_output: false,
    }
}

pub(crate) fn claude_agent(
    tools: &ToolPaths,
    run_dir: &Path,
    label: &str,
    output: PathBuf,
) -> AgentSpec {
    AgentSpec {
        label: label.to_string(),
        provider: "claude-code".to_string(),
        program: tools.claude.clone(),
        args: claude_arguments(),
        output,
        log: run_dir.join("logs").join(format!("{label}.log")),
        sandbox: WORKER_SANDBOX.to_string(),
        permission_mode: CLAUDE_PERMISSION_MODE.to_string(),
        network_access: WORKER_NETWORK_ACCESS,
        stdout_is_output: true,
    }
}

fn codex_arguments(output: &Path) -> Vec<OsString> {
    [
        "exec".into(),
        "--model".into(),
        CODEX_MODEL.into(),
        "--config".into(),
        format!("model_reasoning_effort=\"{REASONING_EFFORT}\"").into(),
        format!("--{CODEX_PERMISSION_MODE}").into(),
        "--disable".into(),
        "multi_agent".into(),
        "--ephemeral".into(),
        "--color".into(),
        "never".into(),
        "--output-last-message".into(),
        output.as_os_str().to_os_string(),
        "-".into(),
    ]
    .into_iter()
    .collect()
}

fn claude_arguments() -> Vec<OsString> {
    let mut arguments: Vec<OsString> = [
        "--print",
        "--model",
        CLAUDE_MODEL,
        "--effort",
        REASONING_EFFORT,
        "--no-session-persistence",
        "--disable-slash-commands",
        "--dangerously-skip-permissions",
        "--tools",
        "default",
        "--disallowedTools",
        "Agent",
    ]
    .into_iter()
    .map(OsString::from)
    .collect();
    // Claude's tool-list flags are variadic. This following option terminates
    // the deny list and ensures --print still reads the prompt from stdin.
    arguments.extend([OsString::from("--output-format"), OsString::from("text")]);
    arguments
}

pub(crate) fn run_single_agent(
    journal: &Arc<Journal>,
    spec: AgentSpec,
    prompt: &PromptInput,
    cwd: &Path,
    timeout: Duration,
    global_cancel: Arc<AtomicBool>,
) -> FactoryResult<String> {
    let result = run_agent(
        Arc::clone(journal),
        spec,
        prompt.clone(),
        cwd.to_path_buf(),
        timeout,
        CancelFlags::global_only(global_cancel),
    );
    let integrity = verify_prompt(prompt, false);
    integrity?;
    result
}

pub(crate) fn run_agent_cohort(
    journal: &Arc<Journal>,
    name: &str,
    specs: Vec<AgentSpec>,
    prompt: &PromptInput,
    cwd: &Path,
    timeout: Duration,
    global_cancel: Arc<AtomicBool>,
) -> FactoryResult<Vec<String>> {
    let members: Vec<String> = specs.iter().map(|spec| spec.label.clone()).collect();
    journal.register_cohort(CohortRecord {
        name: name.to_string(),
        members: members.clone(),
        prompt: prompt.path.clone(),
        prompt_sha256: prompt.sha256.clone(),
    })?;
    journal.event(
        "cohort_started",
        json!({
            "cohort": name,
            "members": members,
            "prompt": prompt.path,
            "prompt_sha256": prompt.sha256,
        }),
    )?;
    let cohort_cancel = Arc::new(AtomicBool::new(false));
    let (sender, receiver) = mpsc::channel();
    let mut handles = Vec::with_capacity(specs.len());
    for spec in specs.iter().cloned() {
        let sender = sender.clone();
        let journal = Arc::clone(journal);
        let prompt = prompt.clone();
        let cwd = cwd.to_path_buf();
        let cancellation = CancelFlags {
            global: Arc::clone(&global_cancel),
            cohort: Arc::clone(&cohort_cancel),
        };
        handles.push(thread::spawn(move || {
            let label = spec.label.clone();
            let result = run_agent(journal, spec, prompt, cwd, timeout, cancellation);
            let _ = sender.send((label, result));
        }));
    }
    drop(sender);

    let mut outputs = BTreeMap::new();
    let mut first_error = None;
    for (label, result) in receiver {
        match result {
            Ok(output) => {
                outputs.insert(label, output);
            }
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(error);
                    cohort_cancel.store(true, Ordering::SeqCst);
                }
            }
        }
    }
    for handle in handles {
        record_cohort_join(&mut first_error, handle.join());
    }
    verify_prompt(prompt, true)?;
    if let Some(error) = first_error {
        return Err(error);
    }
    order_cohort_outputs(&members, outputs)
}

fn record_cohort_join(first_error: &mut Option<FactoryError>, result: thread::Result<()>) {
    if result.is_err() && first_error.is_none() {
        *first_error = Some(FactoryError::new("cohort worker thread panicked"));
    }
}

fn order_cohort_outputs(
    labels: &[String],
    mut outputs: BTreeMap<String, String>,
) -> FactoryResult<Vec<String>> {
    let mut ordered = Vec::with_capacity(labels.len());
    for label in labels {
        #[rustfmt::skip]
        let output = require_some!(outputs.remove(label), FactoryError::new(format!("cohort member produced no result: {label}")));
        ordered.push(output);
    }
    Ok(ordered)
}

fn run_agent(
    journal: Arc<Journal>,
    spec: AgentSpec,
    prompt: PromptInput,
    cwd: PathBuf,
    timeout: Duration,
    cancellation: CancelFlags,
) -> FactoryResult<String> {
    if let Some(parent) = spec.output.parent() {
        create_secure_dir(parent)?;
    }
    if let Some(parent) = spec.log.parent() {
        create_secure_dir(parent)?;
    }
    let output_file = open_private_file(&spec.output, false)?;
    let log_file = open_private_file(&spec.log, false)?;
    let command = std::iter::once(spec.program.as_os_str().to_string_lossy().into_owned())
        .chain(
            spec.args
                .iter()
                .map(|argument| argument.to_string_lossy().into_owned()),
        )
        .collect();
    let record = AgentRecord {
        provider: spec.provider.clone(),
        command,
        sandbox: spec.sandbox.clone(),
        permission_mode: spec.permission_mode.clone(),
        network_access: spec.network_access,
        prompt_sha256: prompt.sha256.clone(),
        status: "running".to_string(),
        started_at: utc_now_rfc3339()?,
        finished_at: None,
        output: spec.output.clone(),
        log: spec.log.clone(),
        returncode: None,
    };
    #[rustfmt::skip]
    journal.agent_started(spec.label.clone(), record)?;
    let output_plan = if spec.stdout_is_output {
        OutputPlan::Files {
            stdout: output_file,
            stderr: log_file,
        }
    } else {
        drop(output_file);
        OutputPlan::Files {
            stdout: io_result(log_file.try_clone(), "clone agent log file", &spec.log)?,
            stderr: log_file,
        }
    };
    let process = supervise(
        CommandSpec::isolated(
            spec.program.clone(),
            spec.args.clone(),
            Some(cwd),
            Some(prompt.bytes),
            timeout,
            cancellation,
        ),
        output_plan,
    );
    let (result, status, returncode) = match process {
        Ok(result) if result.returncode == 0 => {
            let content = read_bytes(&spec.output, &format!("output from {}", spec.label))
                .and_then(|bytes| required_utf8(&bytes, &format!("output from {}", spec.label)));
            let status = if content.is_ok() {
                "complete"
            } else {
                "failed"
            };
            (content, status, Some(0))
        }
        Ok(process) => (
            Err(FactoryError::new(format!(
                "agent {} exited {}; see {}",
                spec.label,
                process.returncode,
                spec.log.display()
            ))),
            "failed",
            Some(process.returncode),
        ),
        Err(error) => {
            let status = if error.kind == SupervisorErrorKind::Cancelled {
                "cancelled"
            } else {
                "failed"
            };
            let message = match error.kind {
                SupervisorErrorKind::TimedOut => format!(
                    "agent {} exceeded the {}-second timeout; see {}",
                    spec.label,
                    timeout.as_secs(),
                    spec.log.display()
                ),
                SupervisorErrorKind::Cancelled => format!("agent {} was cancelled", spec.label),
                SupervisorErrorKind::Other => format!(
                    "agent {} failed: {}; see {}",
                    spec.label,
                    error.detail(),
                    spec.log.display()
                ),
            };
            (Err(FactoryError::new(message)), status, None)
        }
    };
    journal.agent_finished(&spec.label, status, returncode)?;
    result
}

#[cfg(test)]
#[path = "workers/tests/mod.rs"]
mod tests;
