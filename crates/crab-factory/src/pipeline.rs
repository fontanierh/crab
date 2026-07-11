use std::ffi::OsString;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use crate::config::ToolPaths;
use crate::gitops::{assert_identity, assert_unchanged, GitRunner};
use crate::manifest::Journal;
use crate::prompts;
use crate::rubric;
use crate::workers::{
    claude_agent, codex_agent, materialize_prompt, run_agent_cohort, run_single_agent, supervise,
    AgentSpec, CancelFlags, CommandSpec, OutputPlan, PromptInput, SupervisorErrorKind,
};
use crate::{
    create_secure_dir, io_result, open_private_file, utc_now_rfc3339, FactoryError, FactoryResult,
};

pub(crate) struct Pipeline<'a> {
    pub(crate) run_dir: PathBuf,
    pub(crate) request: String,
    pub(crate) base_sha: String,
    pub(crate) branch: String,
    pub(crate) worktree: PathBuf,
    pub(crate) tools: ToolPaths,
    pub(crate) timeout: Duration,
    pub(crate) maximum_review_rounds: u32,
    pub(crate) journal: Arc<Journal>,
    pub(crate) git: GitRunner,
    pub(crate) cancellation: Arc<AtomicBool>,
    pub(crate) stdout: &'a mut dyn Write,
}

impl Pipeline<'_> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new<'a>(
        run_dir: PathBuf,
        request: String,
        base_sha: String,
        branch: String,
        worktree: PathBuf,
        tools: ToolPaths,
        timeout: Duration,
        maximum_review_rounds: u32,
        journal: Arc<Journal>,
        git: GitRunner,
        cancellation: Arc<AtomicBool>,
        stdout: &'a mut dyn Write,
    ) -> Pipeline<'a> {
        Pipeline {
            run_dir,
            request,
            base_sha,
            branch,
            worktree,
            tools,
            timeout,
            maximum_review_rounds,
            journal,
            git,
            cancellation,
            stdout,
        }
    }

    pub(crate) fn execute(&mut self) -> FactoryResult<String> {
        #[rustfmt::skip]
        let plan_prompt = self.prompt(Path::new("01-plan.md"), prompts::planning(&self.request, &self.worktree, &self.base_sha))?;
        #[rustfmt::skip]
        let plan = self.readonly_single("planning", claude_agent(&self.tools, &self.run_dir, "01-plan-fable", self.run_dir.join("01-plan/plan.md")), &plan_prompt, self.worktree.clone())?;

        #[rustfmt::skip]
        let critique_prompt = self.prompt(Path::new("02-plan-critiques.md"), prompts::critique(&self.request, &plan, &self.worktree, &self.base_sha))?;
        let critique_specs = (1..=4)
            .map(|index| {
                let label = format!("02-critique-codex-{index:02}");
                codex_agent(
                    &self.tools,
                    &self.run_dir,
                    &label,
                    self.run_dir
                        .join(format!("02-plan-critiques/codex-{index:02}.md")),
                )
            })
            .collect();
        let critiques = self.readonly_cohort(
            "plan-critiques",
            critique_specs,
            &critique_prompt,
            self.worktree.clone(),
        )?;

        #[rustfmt::skip]
        let synthesis_prompt = self.prompt(Path::new("03-critique-synthesis.md"), prompts::critique_synthesis(&self.request, &plan, &critiques))?;
        #[rustfmt::skip]
        let directive = self.readonly_single("critique synthesis", claude_agent(&self.tools, &self.run_dir, "03-critique-synthesis-fable", self.run_dir.join("03-critique-synthesis/compiled-plan.md")), &synthesis_prompt, self.run_dir.clone())?;

        #[rustfmt::skip]
        let implementation_prompt = self.prompt(Path::new("04-address-critiques.md"), prompts::implementation(&self.request, &directive, &self.worktree, &self.base_sha))?;
        self.write_single(
            "addressing compiled critiques",
            codex_agent(
                &self.tools,
                &self.run_dir,
                "04-address-critiques-codex",
                self.run_dir
                    .join("04-address-critiques/implementation-report.md"),
            ),
            &implementation_prompt,
        )?;

        let mut normal_outcome = None;
        for round in 1..=self.maximum_review_rounds {
            let slug = format!("review-round-{round:02}");
            let round_dir = self.run_dir.join(format!("05-{slug}"));
            #[rustfmt::skip]
            let review_prompt = self.prompt(&PathBuf::from(format!("{slug}/reviews.md")), prompts::review(&self.request, &directive, round, &self.worktree, &self.base_sha))?;
            let mut review_specs = (1..=2)
                .map(|index| {
                    let label = format!("05-{slug}-codex-{index:02}");
                    codex_agent(
                        &self.tools,
                        &self.run_dir,
                        &label,
                        round_dir.join(format!("codex-{index:02}.md")),
                    )
                })
                .collect::<Vec<_>>();
            review_specs.push(claude_agent(
                &self.tools,
                &self.run_dir,
                &format!("05-{slug}-fable-01"),
                round_dir.join("fable-01.md"),
            ));
            let reviews =
                self.readonly_cohort(&slug, review_specs, &review_prompt, self.worktree.clone())?;
            #[rustfmt::skip]
            let compile_prompt = self.prompt(&PathBuf::from(format!("{slug}/synthesis.md")), prompts::review_synthesis(&self.request, &directive, round, &reviews))?;
            #[rustfmt::skip]
            let synthesis = self.readonly_single(&format!("review round {round} synthesis"), claude_agent(&self.tools, &self.run_dir, &format!("05-{slug}-synthesis-fable"), round_dir.join("compiled-review.md")), &compile_prompt, self.run_dir.clone())?;
            let verdict = parse_verdict(&synthesis)?;
            self.journal.event(
                "review_verdict",
                json!({"round": round, "verdict": verdict}),
            )?;
            if verdict == "clean" {
                normal_outcome = Some("clean");
                self.journal.checkpoint_review(round, normal_outcome)?;
                break;
            }
            self.journal.checkpoint_review(round, None)?;
            #[rustfmt::skip]
            let address_prompt = self.prompt(&PathBuf::from(format!("{slug}/address.md")), prompts::review_address(&self.request, &directive, round, &synthesis, &self.worktree, &self.base_sha))?;
            #[rustfmt::skip]
            self.write_single(&format!("addressing review round {round}"), codex_agent(&self.tools, &self.run_dir, &format!("05-{slug}-address-codex"), round_dir.join("address-report.md")), &address_prompt)?;
            if round == self.maximum_review_rounds {
                normal_outcome = Some("addressed_unverified");
                self.journal.checkpoint_review(round, normal_outcome)?;
            }
        }
        let normal_outcome = require_normal_outcome(normal_outcome)?;

        #[rustfmt::skip]
        let thermo_prompt = self.prompt(Path::new("06-thermo-nuclear-review.md"), prompts::thermo_review(&self.request, &directive, rubric::THERMO_RUBRIC, &self.worktree, &self.base_sha))?;
        #[rustfmt::skip]
        let thermo_review = self.readonly_single("thermonuclear code-quality review", codex_agent(&self.tools, &self.run_dir, "06-thermo-nuclear-review-codex", self.run_dir.join("06-thermo-nuclear-review/review.md")), &thermo_prompt, self.worktree.clone())?;
        let thermo_verdict = parse_verdict(&thermo_review)?;
        self.journal
            .event("thermonuclear_verdict", json!({"verdict": thermo_verdict}))?;
        let outcome = if thermo_verdict == "clean" {
            self.journal.checkpoint_thermo("clean", Some(false))?;
            "clean"
        } else {
            self.journal.checkpoint_thermo("changes_required", None)?;
            #[rustfmt::skip]
            let address_prompt = self.prompt(Path::new("06-thermo-nuclear-address.md"), prompts::thermo_address(&self.request, &directive, &thermo_review, &self.worktree, &self.base_sha))?;
            #[rustfmt::skip]
            self.write_single("addressing thermonuclear review", codex_agent(&self.tools, &self.run_dir, "06-thermo-nuclear-address-codex", self.run_dir.join("06-thermo-nuclear-review/address-report.md")), &address_prompt)?;
            self.journal
                .checkpoint_thermo("changes_required", Some(true))?;
            "addressed_unverified"
        };
        self.log(&format!("Normal review outcome: {normal_outcome}"))?;
        self.run_quality()?;
        Ok(outcome.to_string())
    }

    fn prompt(&self, relative: &Path, content: String) -> FactoryResult<PromptInput> {
        materialize_prompt(&self.run_dir, relative, content, &self.journal)
    }

    fn readonly_single(
        &mut self,
        stage: &str,
        spec: AgentSpec,
        prompt: &PromptInput,
        cwd: PathBuf,
    ) -> FactoryResult<String> {
        let before = self.begin_readonly(stage, prompt)?;
        let output_path = spec.output.clone();
        let result = run_single_agent(
            &self.journal,
            spec,
            prompt,
            &cwd,
            self.timeout,
            Arc::clone(&self.cancellation),
        );
        let invariant = self.check_readonly_invariants(stage, before);
        self.finish_stage(stage, &output_path, result, invariant)
    }

    fn readonly_cohort(
        &mut self,
        stage: &str,
        specs: Vec<AgentSpec>,
        prompt: &PromptInput,
        cwd: PathBuf,
    ) -> FactoryResult<Vec<String>> {
        let before = self.begin_readonly(stage, prompt)?;
        let result = run_agent_cohort(
            &self.journal,
            stage,
            specs,
            prompt,
            &cwd,
            self.timeout,
            Arc::clone(&self.cancellation),
        );
        let invariant = self.check_readonly_invariants(stage, before);
        self.finish_stage(stage, &prompt.path, result, invariant)
    }

    fn begin_readonly(
        &mut self,
        stage: &str,
        prompt: &PromptInput,
    ) -> FactoryResult<crate::gitops::WorktreeFingerprint> {
        self.check_cancelled()?;
        self.log(&format!("Starting {stage}"))?;
        self.journal.event(
            "stage_started",
            json!({"stage": stage, "prompt": prompt.path}),
        )?;
        self.git.fingerprint(&self.worktree)
    }

    fn check_readonly_invariants(
        &mut self,
        stage: &str,
        before: crate::gitops::WorktreeFingerprint,
    ) -> FactoryResult<()> {
        let after = self.git.fingerprint(&self.worktree)?;
        assert_unchanged(&before, &after, stage)?;
        #[rustfmt::skip]
        assert_identity(&self.git, &self.worktree, &self.base_sha, &self.branch, stage)?;
        Ok(())
    }

    pub(crate) fn finish_stage<T>(
        &mut self,
        stage: &str,
        output_path: &Path,
        result: FactoryResult<T>,
        invariant: FactoryResult<()>,
    ) -> FactoryResult<T> {
        match result {
            Ok(output) => {
                if let Err(error) = invariant {
                    return self.fail_stage(stage, error, true);
                }
                self.journal.event(
                    "stage_completed",
                    json!({"stage": stage, "output": output_path}),
                )?;
                self.log(&format!("Completed {stage}"))?;
                Ok(output)
            }
            Err(worker) => match invariant {
                Ok(()) => self.fail_stage(stage, worker, false),
                Err(invariant) => self.fail_stage(
                    stage,
                    FactoryError::new(format!(
                        "{worker}; stage invariant also failed: {invariant}"
                    )),
                    true,
                ),
            },
        }
    }

    fn fail_stage<T>(
        &mut self,
        stage: &str,
        error: FactoryError,
        invariant_failed: bool,
    ) -> FactoryResult<T> {
        let event = if invariant_failed {
            "stage_invariant_violation"
        } else {
            "stage_failed"
        };
        self.journal
            .event(event, json!({"stage": stage, "error": error.to_string()}))?;
        Err(error)
    }

    fn write_single(
        &mut self,
        stage: &str,
        spec: AgentSpec,
        prompt: &PromptInput,
    ) -> FactoryResult<String> {
        self.check_cancelled()?;
        self.log(&format!("Starting {stage}"))?;
        self.journal.event(
            "stage_started",
            json!({"stage": stage, "prompt": prompt.path}),
        )?;
        let output_path = spec.output.clone();
        let result = run_single_agent(
            &self.journal,
            spec,
            prompt,
            &self.worktree,
            self.timeout,
            Arc::clone(&self.cancellation),
        );
        let invariant = assert_identity(
            &self.git,
            &self.worktree,
            &self.base_sha,
            &self.branch,
            stage,
        );
        self.finish_stage(stage, &output_path, result, invariant)
    }

    pub(crate) fn run_quality(&mut self) -> FactoryResult<()> {
        self.check_cancelled()?;
        self.log("Running canonical make quality gate")?;
        let before = self.git.fingerprint(&self.worktree)?;
        let quality_dir = self.run_dir.join("quality");
        create_secure_dir(&quality_dir)?;
        let log_path = quality_dir.join("make-quality.log");
        let log = open_private_file(&log_path, false)?;
        self.journal.event(
            "quality_started",
            json!({"command": [self.tools.make.clone(), PathBuf::from("quality")], "log": log_path}),
        )?;
        let result = supervise(
            CommandSpec::isolated(
                self.tools.make.clone(),
                vec![OsString::from("quality")],
                Some(self.worktree.clone()),
                None,
                self.timeout,
                CancelFlags::global_only(Arc::clone(&self.cancellation)),
            ),
            OutputPlan::Files {
                stdout: io_result(log.try_clone(), "clone quality log", &log_path)?,
                stderr: log,
            },
        );
        match &result {
            Ok(result) => {
                self.journal.event(
                    "quality_finished",
                    json!({
                        "returncode": result.returncode,
                        "elapsed_seconds": result.elapsed.as_secs_f64(),
                        "log": log_path,
                    }),
                )?;
            }
            Err(error) if error.kind == SupervisorErrorKind::TimedOut => {
                self.journal.event(
                    "quality_timeout",
                    json!({"timeout_seconds": self.timeout.as_secs(), "log": log_path}),
                )?;
            }
            Err(error) => {
                self.journal.event(
                    "quality_failed",
                    json!({"error": error.detail(), "log": log_path}),
                )?;
            }
        }
        let after = self.git.fingerprint(&self.worktree)?;
        let invariant = assert_quality_unchanged(&before, &after).and_then(|()| {
            assert_identity(
                &self.git,
                &self.worktree,
                &self.base_sha,
                &self.branch,
                "make quality",
            )
        });
        if let Err(error) = invariant {
            self.journal.event(
                "quality_invariant_violation",
                json!({"error": error.to_string(), "log": log_path}),
            )?;
            return Err(error);
        }
        match result {
            Ok(result) => {
                if result.returncode != 0 {
                    return Err(FactoryError::new(format!(
                        "make quality failed with exit {}; see {}",
                        result.returncode,
                        log_path.display()
                    )));
                }
            }
            Err(error) if error.kind == SupervisorErrorKind::TimedOut => {
                return Err(FactoryError::new(format!(
                    "make quality exceeded the {}-second timeout; see {}",
                    self.timeout.as_secs(),
                    log_path.display()
                )));
            }
            Err(error) => {
                return Err(FactoryError::new(format!(
                    "make quality failed under supervision: {}; see {}",
                    error.detail(),
                    log_path.display()
                )));
            }
        }
        self.log("Canonical make quality gate passed")
    }

    fn check_cancelled(&self) -> FactoryResult<()> {
        if self.cancellation.load(Ordering::SeqCst) {
            Err(FactoryError::new("factory interrupted by signal"))
        } else {
            Ok(())
        }
    }

    pub(crate) fn log(&mut self, message: &str) -> FactoryResult<()> {
        write_progress(self.stdout, message)
    }
}

pub(crate) fn require_normal_outcome(outcome: Option<&'static str>) -> FactoryResult<&'static str> {
    match outcome {
        Some(outcome) => Ok(outcome),
        None => Err(FactoryError::new(
            "normal review loop ended without a recorded outcome",
        )),
    }
}

pub(crate) fn write_progress(stdout: &mut dyn Write, message: &str) -> FactoryResult<()> {
    try_mapped!(
        writeln!(stdout, "[{}] {message}", utc_now_rfc3339()?),
        error => FactoryError::new(format!("could not write progress output: {error}"))
    );
    Ok(())
}

pub(crate) fn assert_quality_unchanged(
    before: &crate::gitops::WorktreeFingerprint,
    after: &crate::gitops::WorktreeFingerprint,
) -> FactoryResult<()> {
    if before == after {
        return Ok(());
    }
    let changed = before.changed_paths(after);
    let detail = if changed.is_empty() {
        "git status changed".to_string()
    } else {
        changed.join(", ")
    };
    Err(FactoryError::new(format!(
        "make quality modified the worktree: {detail}"
    )))
}

pub(crate) fn parse_verdict(report: &str) -> FactoryResult<&'static str> {
    let first = report.lines().map(str::trim).find(|line| !line.is_empty());
    match first {
        Some("VERDICT: CLEAN") => Ok("clean"),
        Some("VERDICT: CHANGES_REQUIRED") => Ok("changes_required"),
        _ => Err(FactoryError::new(
            "review has no valid first-line verdict; expected VERDICT: CLEAN or VERDICT: CHANGES_REQUIRED",
        )),
    }
}
