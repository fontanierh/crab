use std::path::Path;

pub(crate) const NO_ACTIONABLE_FINDINGS: &str = "NO_ACTIONABLE_FINDINGS";
const UNRESTRICTED_EXECUTION_POLICY: &str = "The factory intentionally gives this model process unrestricted host permissions and network access. That capability does not expand the task's authorization: obey every file-mutation limit in this prompt, and do not alter external services. Nested agents, delegation, and fan-out remain forbidden.";

fn compose(sections: &[(&str, &str)]) -> String {
    let mut prompt = String::new();
    for (index, (heading, body)) in sections.iter().enumerate() {
        if index > 0 {
            prompt.push_str("\n\n");
        }
        prompt.push_str("# ");
        prompt.push_str(heading);
        prompt.push_str("\n\n");
        prompt.push_str(body);
    }
    prompt
}

fn repository(worktree: &Path, base_sha: &str, read_only: bool) -> String {
    let policy = if read_only {
        "This is an advisory-only read-only stage. Do not modify any file or external service."
    } else {
        "Edit only this worktree. Do not commit, create or switch branches, push, open a PR, deploy, write outside the worktree, or alter external services."
    };
    format!(
        "- Root: {}\n- Base commit: {base_sha}\n- Read and obey AGENTS.md before acting.\n- {policy}",
        worktree.display()
    )
}

pub(crate) fn planning(request: &str, worktree: &Path, base_sha: &str) -> String {
    let role = "You are the sole planning agent for a Crab implementation task. Work independently. Do not spawn agents, delegate, split work among specialists, or edit any file.";
    let repo = repository(worktree, base_sha, true);
    let deliverable = "Inspect the repository deeply enough to produce a self-contained implementation plan. Cover current behavior, exact files and interfaces to change, data and control flow, compatibility and recovery concerns, edge cases, tests, documentation, and the full repository quality gate. Resolve ambiguity with explicit, conservative assumptions. The output must let a later implementation agent execute without this conversation. Do not implement anything.";
    compose(&[
        ("Role", role),
        ("Execution permissions", UNRESTRICTED_EXECUTION_POLICY),
        ("Repository", &repo),
        ("Original request", request),
        ("Deliverable", deliverable),
    ])
}

pub(crate) fn critique(request: &str, plan: &str, worktree: &Path, base_sha: &str) -> String {
    let role = "You are one of four independent plan critics. Review the entire task and plan yourself. Do not spawn agents, delegate, assign a specialty, coordinate with other critics, or edit files.";
    let repo = repository(worktree, base_sha, true);
    let deliverable = format!(
        "Find concrete omissions, incorrect assumptions, architectural risks, missing tests, quality-policy violations, and unnecessary complexity. Tie findings to files, symbols, or observed behavior where possible. Rank findings by severity and give an actionable correction. Do not merely restate the plan. If the plan has no actionable defect, output exactly `{NO_ACTIONABLE_FINDINGS}`."
    );
    compose(&[
        ("Role", role),
        ("Execution permissions", UNRESTRICTED_EXECUTION_POLICY),
        ("Repository", &repo),
        ("Original request", request),
        ("Proposed plan", plan),
        ("Deliverable", &deliverable),
    ])
}

pub(crate) fn critique_synthesis(request: &str, plan: &str, critiques: &[String]) -> String {
    let role = "You are the sole critique compiler. Do not spawn agents, delegate, split the task, or edit or create files.";
    let reports = numbered_reports("Independent critique", critiques);
    let deliverable = "Produce one self-contained implementation directive for the coding agent. Deduplicate overlapping findings, reconcile contradictions using the request and plan, and explicitly distinguish accepted corrections from rejected or inapplicable comments. Include all required context, exact implementation steps, acceptance criteria, tests, documentation, and the final quality gate. Do not refer the reader to separate critique files. Do not write code.";
    compose(&[
        ("Role", role),
        ("Execution permissions", UNRESTRICTED_EXECUTION_POLICY),
        ("Original request", request),
        ("Original plan", plan),
        ("Independent critiques", &reports),
        ("Deliverable", deliverable),
    ])
}

pub(crate) fn implementation(
    request: &str,
    directive: &str,
    worktree: &Path,
    base_sha: &str,
) -> String {
    let role = "You are the sole implementation agent. Implement the directive in the current worktree. Do not spawn agents, delegate, or split the task.";
    let repo = repository(worktree, base_sha, false);
    let execution = "Implement the complete requested change, including meaningful tests and documentation. Inspect your diff, run targeted tests, and run `make quick` before finishing. Fix failures caused by your work. The factory runs the full `make quality` gate after final review. Your final Markdown report must list files changed, design decisions, validation commands and results, and any genuine remaining risk.";
    compose(&[
        ("Role", role),
        ("Execution permissions", UNRESTRICTED_EXECUTION_POLICY),
        ("Repository", &repo),
        ("Original request", request),
        ("Compiled implementation directive", directive),
        ("Required execution", execution),
    ])
}

pub(crate) fn review(
    request: &str,
    directive: &str,
    round: u32,
    worktree: &Path,
    base_sha: &str,
) -> String {
    let role = format!(
        "You are one of three independent implementation reviewers in review round {round}. Review the whole implementation yourself. Do not spawn agents, delegate, assign a specialty, coordinate with other reviewers, or edit files."
    );
    let mut repo = repository(worktree, base_sha, true);
    repo.push_str("\n- Review every tracked and untracked change since the base commit.");
    let deliverable = format!(
        "Report only actionable correctness, reliability, security, compatibility, test, documentation, or repository-policy problems introduced or left unresolved by the implementation. For each finding, give severity, file and line or symbol, evidence, impact, and a concrete fix. Do not focus on style unless it violates an explicit policy or creates real maintenance risk. If there are no actionable findings, output exactly `{NO_ACTIONABLE_FINDINGS}`."
    );
    compose(&[
        ("Role", &role),
        ("Execution permissions", UNRESTRICTED_EXECUTION_POLICY),
        ("Repository and scope", &repo),
        ("Original request", request),
        ("Compiled implementation directive", directive),
        ("Deliverable", &deliverable),
    ])
}

pub(crate) fn review_synthesis(
    request: &str,
    directive: &str,
    round: u32,
    reviews: &[String],
) -> String {
    let role = format!(
        "You are the sole compiler for implementation review round {round}. Do not spawn agents, delegate, split the task, or edit or create files."
    );
    let reports = numbered_reports("Independent review", reviews);
    let deliverable = "Your first nonblank line must be exactly `VERDICT: CLEAN` when there are no actionable findings, or exactly `VERDICT: CHANGES_REQUIRED` when at least one valid actionable finding remains. Then produce one self-contained report. Deduplicate overlaps, reconcile contradictions, reject false positives with a concise reason, and preserve severity plus exact repair guidance for every accepted finding. Do not refer the reader to separate review files.";
    compose(&[
        ("Role", &role),
        ("Execution permissions", UNRESTRICTED_EXECUTION_POLICY),
        ("Original request", request),
        ("Compiled implementation directive", directive),
        ("Independent reviews", &reports),
        ("Deliverable", deliverable),
    ])
}

pub(crate) fn review_address(
    request: &str,
    directive: &str,
    round: u32,
    synthesis: &str,
    worktree: &Path,
    base_sha: &str,
) -> String {
    let role = format!(
        "You are the sole agent addressing review round {round}. Do not spawn agents, delegate, or split the task."
    );
    let repo = repository(worktree, base_sha, false);
    let execution = "Address every accepted finding completely, including regression tests and documentation changes. Reinspect the entire diff, run targeted tests, and run `make quick` before finishing. The factory runs the full `make quality` gate after final review. If a requested repair is demonstrably invalid, do not apply it silently: explain the evidence in the final report. List edits and exact validation results.";
    compose(&[
        ("Role", &role),
        ("Execution permissions", UNRESTRICTED_EXECUTION_POLICY),
        ("Repository", &repo),
        ("Original request", request),
        ("Compiled implementation directive", directive),
        ("Compiled review findings", synthesis),
        ("Required execution", execution),
    ])
}

pub(crate) fn thermo_review(
    request: &str,
    directive: &str,
    rubric: &str,
    worktree: &Path,
    base_sha: &str,
) -> String {
    let role = "You are a fresh, independent final code-quality review subagent. Apply the complete Cursor thermonuclear review skill below as the governing rubric. Review the whole change yourself. Do not spawn nested agents, delegate, coordinate with prior reviewers, split the task, or edit files.";
    let mut repo = repository(worktree, base_sha, true);
    repo.push_str("\n- Review every tracked and untracked change since that base, plus full surrounding files.\n- The rubric supplements, and does not weaken, repository policy.");
    let contract = "The skill above is the complete substantive rubric. For deterministic orchestration only, make the first nonblank line exactly `VERDICT: CLEAN` if its approval bar is fully met, or exactly `VERDICT: CHANGES_REQUIRED` if any actionable finding remains. Then provide the prioritized, high-conviction review the skill requires. Include file and line or symbol evidence plus a concrete structural remedy for every finding.";
    compose(&[
        ("Factory role", role),
        ("Execution permissions", UNRESTRICTED_EXECUTION_POLICY),
        ("Repository and scope", &repo),
        ("Original request", request),
        ("Compiled implementation directive", directive),
        ("Complete pinned Cursor skill", rubric),
        ("Factory output contract", contract),
    ])
}

pub(crate) fn thermo_address(
    request: &str,
    directive: &str,
    review: &str,
    worktree: &Path,
    base_sha: &str,
) -> String {
    let role = "You are the sole agent addressing the final thermonuclear code-quality review. Do not spawn agents, delegate, or split the task.";
    let repo = repository(worktree, base_sha, false);
    let execution = "Address every valid finding with ambitious but behavior-preserving structural improvements. Prefer deleting complexity over moving it. Add or update tests and documentation as needed. Reinspect the full diff, run targeted tests, and run `make quick`; the factory runs the full `make quality` gate immediately afterward. If a finding is demonstrably invalid, preserve the stronger design and explain the evidence in the final report. List every edit and exact validation result.";
    compose(&[
        ("Role", role),
        ("Execution permissions", UNRESTRICTED_EXECUTION_POLICY),
        ("Repository", &repo),
        ("Original request", request),
        ("Compiled implementation directive", directive),
        ("Thermonuclear review", review),
        ("Required execution", execution),
    ])
}

fn numbered_reports(label: &str, reports: &[String]) -> String {
    reports
        .iter()
        .enumerate()
        .map(|(index, report)| format!("## {label} {}\n\n{report}", index + 1))
        .collect::<Vec<_>>()
        .join("\n\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rubric::THERMO_RUBRIC;

    fn repo() -> (&'static Path, &'static str) {
        (Path::new("/tmp/worktree"), "base-sha")
    }

    #[test]
    fn all_builders_preserve_request_and_normative_roles() {
        let request = "request with caf\u{e9}  ";
        let (worktree, base) = repo();
        let plan = planning(request, worktree, base);
        assert!(plan.contains("sole planning agent"));
        assert!(plan.contains(request));
        let critique = critique(request, "plan", worktree, base);
        assert!(critique.contains("one of four independent plan critics"));
        assert!(critique.contains(NO_ACTIONABLE_FINDINGS));
        let synthesis = critique_synthesis(request, "plan", &["a".into(), "b".into()]);
        assert!(synthesis.contains("sole critique compiler"));
        assert!(synthesis.contains("## Independent critique 2\n\nb"));
        let implementation = implementation(request, "directive", worktree, base);
        assert!(implementation.contains("sole implementation agent"));
        assert!(implementation.contains("make quick"));
        let review = review(request, "directive", 2, worktree, base);
        assert!(
            review.contains("one of three independent implementation reviewers in review round 2")
        );
        let compiled = review_synthesis(request, "directive", 2, &["review".into()]);
        assert!(compiled.contains("sole compiler for implementation review round 2"));
        assert!(compiled.contains("VERDICT: CHANGES_REQUIRED"));
        let address = review_address(request, "directive", 2, "findings", worktree, base);
        assert!(address.contains("sole agent addressing review round 2"));
        let thermo = thermo_review(request, "directive", THERMO_RUBRIC, worktree, base);
        assert!(thermo.contains("fresh, independent final code-quality review subagent"));
        assert!(thermo.contains(THERMO_RUBRIC));
        assert!(thermo.contains("If those conditions are not met"));
        let thermo_fix = thermo_address(request, "directive", "review", worktree, base);
        assert!(thermo_fix.contains("sole agent addressing the final thermonuclear"));
        assert!(thermo_fix.contains(request));
        for prompt in [
            &plan,
            &critique,
            &synthesis,
            &implementation,
            &review,
            &compiled,
            &address,
            &thermo,
            &thermo_fix,
        ] {
            assert!(prompt.contains("unrestricted host permissions and network access"));
            assert!(prompt.contains("do not alter external services"));
            assert!(prompt.contains("Nested agents, delegation, and fan-out remain forbidden"));
        }
        assert!(plan.contains("edit any file"));
        assert!(critique.contains("edit files"));
        assert!(synthesis.contains("edit or create files"));
        assert!(review.contains("edit files"));
        assert!(compiled.contains("edit or create files"));
        assert!(thermo.contains("edit files"));
    }

    #[test]
    fn composer_and_numbering_are_deterministic() {
        assert_eq!(
            compose(&[("One", "a"), ("Two", "b")]),
            "# One\n\na\n\n# Two\n\nb"
        );
        assert_eq!(numbered_reports("Report", &[]), "");
        assert_eq!(
            numbered_reports("Report", &["x".into()]),
            "## Report 1\n\nx"
        );
    }
}
