#!/bin/sh
set -eu

provider=$1
scenario=$2
receipts=$3
shift 3

if [ "${1:-}" = "--version" ]; then
  printf '%s fake 1.0\n' "$provider"
  exit 0
fi

has_argument() {
  expected=$1
  shift
  for argument in "$@"; do
    [ "$argument" = "$expected" ] && return 0
  done
  return 1
}

has_pair() {
  expected_flag=$1
  expected_value=$2
  shift 2
  previous=
  for argument in "$@"; do
    if [ "$previous" = "$expected_flag" ] && [ "$argument" = "$expected_value" ]; then
      return 0
    fi
    previous=$argument
  done
  return 1
}

fail() {
  printf '%s\n' "$1" >&2
  exit 9
}

write_receipt() {
  : > "$receipt"
  for argument in "$@"; do
    printf 'argv\t%s\n' "$argument" >> "$receipt"
  done
  printf 'cwd\t%s\n' "$PWD" >> "$receipt"
  [ -n "${prompt_file:-}" ] && printf 'stdin\t%s\n' "$prompt_file" >> "$receipt"
  [ -n "${extra_key:-}" ] && printf 'field\t%s\t%s\n' "$extra_key" "$extra_value" >> "$receipt"
  for name in DISCORD_TOKEN GIT_DIR GIT_WORK_TREE CARGO_TARGET_DIR MAKEFLAGS BASH_ENV CODEX_SANDBOX CODEX_SANDBOX_NETWORK_DISABLED CLAUDE_DISABLE_NETWORK OPENAI_OFFLINE ANTHROPIC_NO_NETWORK HTTP_PROXY SSL_CERT_FILE; do
    eval "presence=\${${name}+present}"
    if [ "${presence:-}" = present ]; then
      printf 'env\t%s\tpresent\n' "$name" >> "$receipt"
    else
      printf 'env\t%s\tabsent\n' "$name" >> "$receipt"
    fi
  done
}

run_codex() {
  has_argument "--dangerously-bypass-approvals-and-sandbox" "$@" || fail "codex dangerous permission bypass is missing"
  has_pair "--disable" "multi_agent" "$@" || fail "codex nested-agent disable flag is missing"
  for argument in "$@"; do
    case "$argument" in
      --sandbox|read-only|workspace-write) fail "codex sandbox restriction must not be present" ;;
    esac
  done

  output=
  previous=
  for argument in "$@"; do
    [ "$previous" = "--output-last-message" ] && output=$argument
    previous=$argument
  done
  [ -n "$output" ] || fail "missing --output-last-message"
  parent=${output%/*}
  output_name=${output##*/}
  parent_name=${parent##*/}
  receipt="$receipts/codex-$parent_name-$output_name.receipt"
  prompt_file="$receipt.stdin"
  extra_key=output
  extra_value=$output
  /bin/cat > "$prompt_file"
  write_receipt "$@"

  if /usr/bin/grep -Fq "one of two independent plan critics" "$prompt_file"; then
    if [ "$scenario" = worker-fail ] && [ "$output_name" = codex-01.md ]; then
      printf 'intentional failure\n' >&2
      exit 7
    fi
    if [ "$scenario" = worker-fail ] && [ "$output_name" = codex-02.md ]; then
      /bin/sleep 120
    fi
    if [ "$scenario" = prompt-mutate ] && [ "$output_name" = codex-01.md ]; then
      run_dir=${parent%/*}
      printf x >> "$run_dir/prompts/02-plan-critiques.md"
    fi
    if [ "$scenario" = empty-output ] && [ "$output_name" = codex-01.md ]; then
      printf '   \n' > "$output"
    else
      printf 'NO_ACTIONABLE_FINDINGS\n' > "$output"
    fi
  elif /usr/bin/grep -Fq "sole implementation agent" "$prompt_file"; then
    printf 'implemented\n' > implemented.txt
    if [ "$scenario" = commit ]; then
      git add implemented.txt
      git commit -qm forbidden
    fi
    printf 'Implementation complete.\n' > "$output"
  elif /usr/bin/grep -Fq "one of three independent implementation reviewers" "$prompt_file"; then
    [ "$scenario" = readonly-write ] && printf 'bad\n' > reviewer-mutation.txt
    printf 'NO_ACTIONABLE_FINDINGS\n' > "$output"
  elif /usr/bin/grep -Fq "sole agent addressing review round" "$prompt_file"; then
    printf 'addressed\n' > normal-remediation.txt
    printf 'Normal findings addressed.\n' > "$output"
  elif /usr/bin/grep -Fq "fresh, independent final code-quality review subagent" "$prompt_file"; then
    if [ "$scenario" = remediate ]; then
      printf 'VERDICT: CHANGES_REQUIRED\nFinding.\n' > "$output"
    else
      printf 'VERDICT: CLEAN\nApproval bar met.\n' > "$output"
    fi
  elif /usr/bin/grep -Fq "sole agent addressing the final thermonuclear" "$prompt_file"; then
    printf 'addressed\n' > thermo-remediation.txt
    printf 'Thermonuclear findings addressed.\n' > "$output"
  else
    fail "unrecognized codex role"
  fi
  printf 'fake codex complete\n'
}

run_claude() {
  has_argument "--dangerously-skip-permissions" "$@" || fail "claude dangerous permission bypass is missing"
  has_pair "--tools" "default" "$@" || fail "claude default tool set is missing"
  has_pair "--disallowedTools" "Agent" "$@" || fail "claude Agent tool must be disabled"
  for argument in "$@"; do
    case "$argument" in
      --permission-mode|Read,Glob,Grep,Bash|Agent,Edit,Write,NotebookEdit) fail "claude restricted permission contract must not be present" ;;
    esac
  done

  prompt_file="$receipts/claude-$$.stdin"
  /bin/cat > "$prompt_file"
  extra_key=role
  if /usr/bin/grep -Fq "sole planning agent" "$prompt_file"; then
    extra_value=plan
    response='Self-contained plan.'
    [ "$scenario" = slow-plan ] && /bin/sleep 120
  elif /usr/bin/grep -Fq "sole critique compiler" "$prompt_file"; then
    extra_value=critique-synthesis
    response='Self-contained implementation directive.'
  elif /usr/bin/grep -Fq "one of three independent implementation reviewers" "$prompt_file"; then
    extra_value=review-member
    response=NO_ACTIONABLE_FINDINGS
  elif /usr/bin/grep -Fq "sole compiler for implementation review round" "$prompt_file"; then
    if /usr/bin/grep -Fq "round 1" "$prompt_file"; then
      suffix=01
    else
      suffix=02
    fi
    extra_value=review-synthesis-$suffix
    if { [ "$scenario" = remediate ] || [ "$scenario" = remediate-final ]; } && [ "$suffix" = 01 ]; then
      response='VERDICT: CHANGES_REQUIRED
Finding.'
    elif [ "$scenario" = invalid-verdict ]; then
      response=CLEAN
    else
      response='VERDICT: CLEAN
No findings.'
    fi
  else
    fail "unrecognized claude role"
  fi
  receipt="$receipts/claude-$extra_value.receipt"
  write_receipt "$@"
  printf '%s\n' "$response"
}

run_make() {
  receipt="$receipts/make-quality.receipt"
  prompt_file=
  extra_key=
  write_receipt "$@"
  if [ "$scenario" = quality-fail ]; then
    printf 'quality failed\n'
    exit 6
  fi
  [ "$scenario" = quality-mutate ] && printf 'mutated\n' > source.txt
  [ "$scenario" = quality-sleep ] && /bin/sleep 90
  printf 'fake make quality passed\n'
}

case "$provider" in
  codex) run_codex "$@" ;;
  claude) run_claude "$@" ;;
  make) run_make "$@" ;;
  *) fail "unknown fake provider: $provider" ;;
esac
