use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;

use sha2::{Digest, Sha256};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

mod support;

use support::{assert_success, mode, Fixture};

#[test]
fn clean_path_stops_early_preserves_isolation_and_audits_worker_contracts() {
    let fixture = Fixture::new("clean", "clean");
    let source_head = fixture.source_head();
    let source_branch = fixture.source_branch();
    fs::create_dir_all(&fixture.runs).unwrap();
    fs::create_dir_all(&fixture.worktrees).unwrap();
    fs::set_permissions(&fixture.runs, fs::Permissions::from_mode(0o755)).unwrap();
    fs::set_permissions(&fixture.worktrees, fs::Permissions::from_mode(0o755)).unwrap();
    let mut command = fixture.command(2);
    command.args(["--base", &fixture.first_sha]);
    let output = command.output().unwrap();
    assert_success(&output);

    let manifest = fixture.manifest();
    assert_eq!(manifest["status"], "complete");
    assert_eq!(manifest["outcome"], "clean");
    assert_eq!(manifest["completed_review_rounds"], 1);
    assert_eq!(manifest["normal_review_outcome"], "clean");
    assert_eq!(manifest["thermonuclear_verdict"], "clean");
    assert_eq!(manifest["thermonuclear_addressed"], false);
    assert_eq!(manifest["maximum_review_rounds"], 3);
    assert_eq!(
        manifest["worker_policy"]["host_permissions"],
        "unrestricted"
    );
    assert_eq!(manifest["worker_policy"]["sandbox"], "disabled");
    assert_eq!(manifest["worker_policy"]["network_access"], true);
    assert_eq!(manifest["worker_policy"]["nested_agents_enabled"], false);
    assert!(!fixture.run_dir().join("05-review-round-02").exists());
    for relative in [
        "00-request.md",
        "launch.json",
        "manifest.json",
        "01-plan/plan.md",
        "02-plan-critiques/codex-01.md",
        "03-critique-synthesis/compiled-plan.md",
        "04-address-critiques/implementation-report.md",
        "05-review-round-01/compiled-review.md",
        "06-thermo-nuclear-review/review.md",
        "quality/make-quality.log",
        "final-status.md",
    ] {
        assert!(fixture.run_dir().join(relative).is_file(), "{relative}");
    }
    assert_eq!(mode(&fixture.run_dir()), 0o700);
    assert_eq!(mode(&fixture.runs), 0o755);
    assert_eq!(mode(&fixture.worktrees), 0o755);
    assert_eq!(mode(&fixture.run_dir().join("manifest.json")), 0o600);
    assert_eq!(mode(&fixture.run_dir().join(".lock")), 0o600);
    assert_eq!(mode(&fixture.run_dir().join("00-request.md")), 0o400);
    assert_eq!(
        fs::read(fixture.run_dir().join("00-request.md")).unwrap(),
        fs::read(&fixture.prompt).unwrap()
    );
    OffsetDateTime::parse(manifest["created_at"].as_str().unwrap(), &Rfc3339).unwrap();

    assert_eq!(fixture.source_head(), source_head);
    assert_eq!(fixture.source_branch(), source_branch);
    assert!(git(&fixture.repo, &["status", "--porcelain"]).is_empty());
    assert_eq!(
        git(&fixture.worktree(), &["rev-parse", "HEAD"]),
        fixture.first_sha
    );
    assert_eq!(
        git(&fixture.worktree(), &["symbolic-ref", "HEAD"]),
        format!("refs/heads/factory/{}", fixture.run_id)
    );
    assert!(fixture.worktree().join("implemented.txt").is_file());
    assert!(!fixture.repo.join("implemented.txt").exists());

    assert_cohort_receipts(&fixture, &manifest);
    assert_worker_arguments_and_environment(&fixture, &manifest);
    for (label, agent) in manifest["agents"].as_object().unwrap() {
        let log = Path::new(agent["log"].as_str().unwrap());
        assert!(log.is_file(), "missing log for {label}: {}", log.display());
    }
    let final_status = fs::read_to_string(fixture.run_dir().join("final-status.md")).unwrap();
    assert!(final_status.contains(&fixture.run_id));
    assert!(final_status.contains("Status: `clean`"));
    assert!(final_status.contains(fixture.worktree().to_str().unwrap()));
    let launch: serde_json::Value =
        serde_json::from_slice(&fs::read(fixture.run_dir().join("launch.json")).unwrap()).unwrap();
    assert_eq!(launch["launch_mode"], "foreground");
    assert!(launch["launched_pid"].as_u64().is_some());
    assert_eq!(launch["base_sha"], fixture.first_sha);
}

#[test]
fn findings_use_the_requested_extra_round_and_thermo_remediation() {
    let fixture = Fixture::new("remediate", "remediate");
    let output = fixture.run(1);
    assert_success(&output);
    let manifest = fixture.manifest();
    assert_eq!(manifest["status"], "complete");
    assert_eq!(manifest["outcome"], "addressed_unverified");
    assert_eq!(manifest["completed_review_rounds"], 2);
    assert_eq!(manifest["normal_review_outcome"], "clean");
    assert_eq!(manifest["thermonuclear_verdict"], "changes_required");
    assert_eq!(manifest["thermonuclear_addressed"], true);
    assert!(fixture
        .run_dir()
        .join("05-review-round-01/address-report.md")
        .is_file());
    assert!(fixture
        .run_dir()
        .join("05-review-round-02/compiled-review.md")
        .is_file());
    assert!(fixture
        .run_dir()
        .join("06-thermo-nuclear-review/address-report.md")
        .is_file());
    assert!(fixture.worktree().join("normal-remediation.txt").is_file());
    assert!(fixture.worktree().join("thermo-remediation.txt").is_file());
    assert_worker_arguments_and_environment(&fixture, &manifest);
}

#[test]
fn last_allowed_findings_are_addressed_without_an_invented_round() {
    let fixture = Fixture::new("last-round", "remediate-final");
    let output = fixture.run(0);
    assert_success(&output);
    let manifest = fixture.manifest();
    assert_eq!(manifest["completed_review_rounds"], 1);
    assert_eq!(manifest["normal_review_outcome"], "addressed_unverified");
    assert_eq!(manifest["thermonuclear_verdict"], "clean");
    assert_eq!(manifest["outcome"], "clean");
    assert!(fixture
        .run_dir()
        .join("05-review-round-01/address-report.md")
        .is_file());
    assert!(!fixture.run_dir().join("05-review-round-02").exists());
}

fn assert_cohort_receipts(fixture: &Fixture, manifest: &serde_json::Value) {
    let codex_receipts = fixture.receipts("codex-");
    let claude_receipts = fixture.receipts("claude-");
    for cohort in manifest["cohorts"].as_array().unwrap() {
        let expected = cohort["prompt_sha256"].as_str().unwrap();
        let prompt = Path::new(cohort["prompt"].as_str().unwrap());
        assert_eq!(
            format!("{:x}", Sha256::digest(fs::read(prompt).unwrap())),
            expected
        );
        for member in cohort["members"].as_array().unwrap() {
            let label = member.as_str().unwrap();
            let actual = if label.contains("codex") {
                let output = manifest["agents"][label]["output"].as_str().unwrap();
                codex_receipts
                    .iter()
                    .find(|receipt| receipt["output"] == output)
                    .unwrap()["stdin_sha256"]
                    .as_str()
                    .unwrap()
            } else {
                claude_receipts
                    .iter()
                    .find(|receipt| receipt["role"] == "review-member")
                    .unwrap()["stdin_sha256"]
                    .as_str()
                    .unwrap()
            };
            assert_eq!(actual, expected, "{label}");
            assert_eq!(manifest["agents"][label]["prompt_sha256"], expected);
        }
    }
}

fn assert_worker_arguments_and_environment(fixture: &Fixture, manifest: &serde_json::Value) {
    let request = fs::read_to_string(&fixture.prompt).unwrap();
    let codex_receipts = fixture.receipts("codex-");
    let claude_receipts = fixture.receipts("claude-");
    for (label, agent) in manifest["agents"].as_object().unwrap() {
        let provider = agent["provider"].as_str().unwrap();
        let receipt = if provider == "codex" {
            let output = agent["output"].as_str().unwrap();
            codex_receipts
                .iter()
                .find(|receipt| receipt["output"] == output)
                .unwrap()
        } else {
            let role = claude_role(label);
            claude_receipts
                .iter()
                .find(|receipt| receipt["role"] == role)
                .unwrap()
        };
        let argv = receipt["argv"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        assert!(!argv.join(" ").contains(&request));
        let environment = receipt["env"].as_object().unwrap();
        for name in [
            "DISCORD_TOKEN",
            "GIT_DIR",
            "GIT_WORK_TREE",
            "CARGO_TARGET_DIR",
            "MAKEFLAGS",
            "BASH_ENV",
            "CODEX_SANDBOX",
            "CODEX_SANDBOX_NETWORK_DISABLED",
            "CLAUDE_DISABLE_NETWORK",
            "OPENAI_OFFLINE",
            "ANTHROPIC_NO_NETWORK",
        ] {
            assert!(
                environment[name].is_null(),
                "environment leaked {name}: {receipt}"
            );
        }
        for name in ["HTTP_PROXY", "SSL_CERT_FILE"] {
            assert_eq!(
                environment[name], "present",
                "network setting lost: {receipt}"
            );
        }
        let expected = if provider == "codex" {
            assert_eq!(
                agent["permission_mode"], "dangerously-bypass-approvals-and-sandbox",
                "{label}"
            );
            expected_codex_arguments(agent["output"].as_str().unwrap())
        } else {
            assert_eq!(
                agent["permission_mode"], "dangerously-skip-permissions",
                "{label}"
            );
            expected_claude_arguments()
        };
        assert_eq!(agent["sandbox"], "disabled", "{label}");
        assert_eq!(agent["network_access"], true, "{label}");
        assert_eq!(argv, expected, "worker argv for {label}");
        let command = agent["command"].as_array().unwrap();
        assert_eq!(
            command.len(),
            expected.len() + 1,
            "manifest command for {label}"
        );
        assert!(
            Path::new(command[0].as_str().unwrap()).is_absolute(),
            "{label}"
        );
        assert_eq!(
            command[1..]
                .iter()
                .map(|value| value.as_str().unwrap().to_string())
                .collect::<Vec<_>>(),
            expected,
            "manifest argv for {label}"
        );
    }
    let make = fixture.receipts("make-").pop().unwrap();
    assert_eq!(make["argv"][0], "quality");
    assert!(make["env"]["DISCORD_TOKEN"].is_null());
    assert!(make["env"]["CODEX_SANDBOX"].is_null());
    assert!(make["env"]["CODEX_SANDBOX_NETWORK_DISABLED"].is_null());
    assert_eq!(make["env"]["HTTP_PROXY"], "present");
    assert_eq!(make["env"]["SSL_CERT_FILE"], "present");
    for path in manifest["tool_paths"].as_object().unwrap().values() {
        assert!(Path::new(path.as_str().unwrap()).is_absolute());
    }
}

fn expected_codex_arguments(output: &str) -> Vec<String> {
    [
        "exec",
        "--model",
        "gpt-5.6-sol",
        "--config",
        "model_reasoning_effort=\"high\"",
        "--dangerously-bypass-approvals-and-sandbox",
        "--disable",
        "multi_agent",
        "--ephemeral",
        "--color",
        "never",
        "--output-last-message",
        output,
        "-",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn expected_claude_arguments() -> Vec<String> {
    [
        "--print",
        "--model",
        "claude-fable-5",
        "--effort",
        "high",
        "--no-session-persistence",
        "--disable-slash-commands",
        "--dangerously-skip-permissions",
        "--tools",
        "default",
        "--disallowedTools",
        "Agent",
        "--output-format",
        "text",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn claude_role(label: &str) -> &'static str {
    match label {
        "01-plan-fable" => "plan",
        "03-critique-synthesis-fable" => "critique-synthesis",
        "05-review-round-01-fable-01" | "05-review-round-02-fable-01" => "review-member",
        "05-review-round-01-synthesis-fable" => "review-synthesis-01",
        "05-review-round-02-synthesis-fable" => "review-synthesis-02",
        other => panic!("unexpected Claude label: {other}"),
    }
}

fn git(repo: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(repo)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}
