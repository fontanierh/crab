use super::*;

#[test]
fn single_agents_report_timeout_cancellation_spawn_and_prompt_integrity_errors() {
    let _environment = match crate::FACTORY_ENV_LOCK.lock() {
        Ok(lock) => lock,
        Err(error) => error.into_inner(),
    };
    let original_path = std::env::var_os("PATH");
    let fixture = Fixture::new("worker-branches", "clean");
    std::env::set_var("PATH", fixture.environment_path());
    let reserved = prepare_run(
        LaunchOptions {
            prompt_file: fixture.prompt.clone(),
            repo: fixture.repo.clone(),
            base_ref: "HEAD".to_string(),
            run_id: Some(fixture.run_id.clone()),
            additional_review_rounds: 0,
            artifact_root: Some(fixture.runs.clone()),
            worktree_root: Some(fixture.worktrees.clone()),
            agent_timeout_seconds: 60,
            allow_dirty_source: false,
            launcher: None,
            effort: None,
            plan_critics: None,
            codex_reviewers: None,
        },
        RequestedMode::Run,
    )
    .unwrap();
    let journal = Arc::new(Journal::load(reserved.run_dir.join("manifest.json")).unwrap());
    let prompt = materialize_prompt(
        &reserved.run_dir,
        Path::new("worker-branches.md"),
        "worker prompt".to_string(),
        &journal,
    )
    .unwrap();
    let agent = |label: &str, program: &str, script: &str| AgentSpec {
        label: label.to_string(),
        provider: "test".to_string(),
        program: PathBuf::from(program),
        args: vec![OsString::from("-c"), OsString::from(script)],
        output: reserved.run_dir.join(format!("{label}.md")),
        log: reserved.run_dir.join(format!("{label}.log")),
        sandbox: "disabled".to_string(),
        permission_mode: "test-bypass".to_string(),
        network_access: true,
        stdout_is_output: true,
    };

    let timed_out = run_single_agent(
        &journal,
        agent("timeout", "/bin/sh", "sleep 5"),
        &prompt,
        &fixture.repo,
        Duration::from_millis(50),
        Arc::new(AtomicBool::new(false)),
    )
    .unwrap_err();
    assert!(timed_out.to_string().contains("timeout"));

    let cancelled = run_single_agent(
        &journal,
        agent("cancelled", "/bin/sh", "printf unused"),
        &prompt,
        &fixture.repo,
        Duration::from_secs(1),
        Arc::new(AtomicBool::new(true)),
    )
    .unwrap_err();
    assert!(cancelled.to_string().contains("cancelled"));

    let missing = run_single_agent(
        &journal,
        agent(
            "missing",
            "/definitely/missing/factory-agent",
            "printf unused",
        ),
        &prompt,
        &fixture.repo,
        Duration::from_secs(1),
        Arc::new(AtomicBool::new(false)),
    )
    .unwrap_err();
    assert!(missing.to_string().contains("failed"));

    let mut bad_output = agent("bad-output", "/bin/sh", "printf unused");
    bad_output.output = PathBuf::from("/");
    assert!(run_single_agent(
        &journal,
        bad_output,
        &prompt,
        &fixture.repo,
        Duration::from_secs(1),
        Arc::new(AtomicBool::new(false)),
    )
    .is_err());
    let mut bad_log = agent("bad-log", "/bin/sh", "printf unused");
    bad_log.log = PathBuf::from("/");
    assert!(run_single_agent(
        &journal,
        bad_log,
        &prompt,
        &fixture.repo,
        Duration::from_secs(1),
        Arc::new(AtomicBool::new(false)),
    )
    .is_err());

    let mutate = format!("printf complete; printf x >> '{}'", prompt.path.display());
    let changed = run_single_agent(
        &journal,
        agent("prompt-change", "/bin/sh", &mutate),
        &prompt,
        &fixture.repo,
        Duration::from_secs(1),
        Arc::new(AtomicBool::new(false)),
    )
    .unwrap_err();
    assert!(changed.to_string().contains("stage prompt changed"));

    match original_path {
        Some(path) => std::env::set_var("PATH", path),
        None => std::env::remove_var("PATH"),
    }
}
