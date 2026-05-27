# VM Codex Coding-Agent Concurrency Development Plan

> For Hermes: use `subagent-driven-development` only after this plan is reviewed/accepted. Execution target is the Minieye VM worker plane; host-side work is planning/packaging only.

Goal: turn the current VM Codex execution-plane sidecar from a proven runtime experiment into a governed, observable, scheduler-integrated coding-agent lane with repeatable concurrency acceptance.

Architecture: keep the current `shared-state v2 -> VM worker -> Codex exec` contract, but replace ad-hoc window-runner operation with a bounded scheduler/daemon lane, explicit resource/concurrency policy, durable final-artifact extraction, and a smoke ladder from canary to read-only to isolated patch/test tasks. This is additive sidecar hardening, not a replacement for direct CLI/governed-tool lanes.

Runtime paths:
- Host control: `~/.local/bin/ssh-mini-*`
- VM shared state: `/home/mini/.hermes/shared-state`
- VM worker state: `/home/mini/.hermes/worker-state`
- Package branch: `vm-codex-window-runner-20260526`
- Package dir: `ops/vm-worker-state/`
- VM artifact landing: `/mnt/tmp/<task_id>/`
- User-visible CIFS: `//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/<task_id>/`

---

## 1. Evidence snapshot, 2026-05-26 live probes

Live health:

```text
ssh-mini: ok=True, mux=ok, extra=0/4
resource: ok_for_submit=True, cpu=128, mem_available≈113.9GiB, swap_free≈31.26GiB
background load at probe: loadavg≈16.89/7.63/4.18, dnp_real=6/32, dnp_like=10/136, mcap_rss≈4.6GiB
```

Interpretation:
- SSH residuals are clean; current concurrency observations should not be blamed on stale ssh/expect remnants.
- VM has enough memory/swap for coding-agent experiments.
- Background load is non-zero; future concurrency acceptance must record background load, not only pass/fail.

Smoke ladder evidence:

| Layer | Evidence | Result | Meaning | Limit |
|---|---|---|---|---|
| Short Codex canary | direct N=48 | all green | backend routing works for high-count trivial jobs | not real coding workload |
| Short window stress | `codex-n96-window24-20260526-100720` | final reported pass_count=96 after retry/reconcile | window + retry can clear N=96 canary | still canary workload |
| N=96 window24 first controller | `/mnt/tmp/codex_n96_window24_controller_20260526_1028/summary.json` | ok_count=92, failed=4 before later retry/reconcile | raw window24 still had timeout/artifact-lag cases | needs retry/reconcile |
| N=96 window16 first controller | `/mnt/tmp/codex_n96_window16_controller_20260526_1056/summary.json` | ok_count=85, failed=10 | lower/equal window alone did not solve failures | failure not purely window size |
| Real read-only N=12 initial | `/mnt/tmp/codex_readonly_real12_20260526_1635/summary.json` | initial ok_count=10/12, done=12 | execution completed but final-artifact check was too narrow | false negatives before RCA |
| Real read-only N=12 recheck | `/mnt/tmp/codex_readonly_real12_20260526_1635_recheck/summary.json` | ok_count=12, done=12, failed=0 | final marker promotion fixed RCA | only 12 tasks |
| Real read-only N=24 | `/mnt/tmp/codex_readonly_real24_20260526_1709/summary.json` | ok_count=24, done=24, failed=0, claimed=0, pending=0 | current strongest real repo-read proof | throughput modest; saturation uneven |

Real read-only N=24 metrics:

```text
run status: /mnt/tmp/codex-readonly-real24-run-20260526-1709/status.json
started_at: 2026-05-26T17:29:55+0800
finished_at: 2026-05-26T17:40:12+0800
exit_code: 0
wall time: 617 seconds / 10.28 minutes
throughput: 2.33 tasks/minute
average amortized wall cost: 25.7 seconds/task
configured window: 6
```

`waves.log` shows small waves and some duplicate starts (for example task 4 and 19), so the effective window was not saturated continuously. Current correctness is strong for N=24 read-only, but the concurrency plane is still a sidecar runner, not a measured stable scheduler.

---

## 2. Current progress

Already achieved:
1. Metadata gate exists in package and live VM runtime:
   - `executor_type=coding_agent`
   - `agent_backend=codex`
   - `codex_backend_enabled=true`
   - incomplete intended-Codex metadata fails closed instead of falling back.
2. Codex prompt/output contract exists:
   - `codex_prompt_override`
   - `expected_final_message`
   - `codex_output_last_message`
   - wrapped `codex exec --output-last-message` path.
3. Final artifact extraction bug fixed:
   - commit `8c77dd2c4 fix(ops): persist Codex final marker artifact`
   - runner final text is promoted from `runner.log` into `codex-last-message.txt` when needed.
4. Fork branch published, not origin:
   - branch `vm-codex-window-runner-20260526`
   - remote `fork` / `https://github.com/huzihaoo/hermes-agent.git`
   - no origin push.
5. SSH residuals remained clean at latest probe: `extra=0/4`, mux healthy.

Hard boundaries / gaps:
1. Runtime source of truth is split:
   - live VM runtime: `/home/mini/.hermes/worker-state/*.py`
   - packaged review branch: `ops/vm-worker-state/*.py`
   - daemon provisioning/source repo still not located.
2. `vm_codex_window_runner.py` is still manual sidecar:
   - launched through `ssh-mini-submit`
   - not scheduler/daemon integrated
   - no persistent lane policy or fairness/adaptive window yet.
3. Throughput metrics are not first-class:
   - summaries and waves exist
   - no standard `performance.json`, p50/p95, retry breakdown, effective concurrency, or resource snapshot in every run.
4. Strongest real case is read-only repo inspection:
   - no accepted isolated patch/test lane yet
   - no multi-file coding + test + rollback smoke yet.
5. Package docs/manifests are stale:
   - README still says metadata gate is future work.
   - manifest hashes predate latest `8c77dd2c4` content.

---

## 3. Final target state

Final acceptable state:
1. Scheduler-integrated Codex lane:
   - shared-state metadata deterministically selects `coding_agent/codex`.
   - scheduler/daemon can claim/run Codex tasks without manual window runner.
   - deterministic `direct_cli` and `governed_tool` lanes remain separate.
2. Concurrency policy:
   - read-only default window is measured and encoded.
   - adaptive behavior responds to timeouts, stale claims, SSH pressure, VM resource gate, and DNP/MCAP pressure.
   - VM-heavy/MCAP/DNP-heavy work is blocked from this Codex lane unless routed through governed wrappers.
3. Evidence contract:
   - each terminal task has canonical shared-state result, local-result, runner.log, final marker artifact, verification receipt, and artifact list.
4. Real smoke ladder:
   - canary N=48/N=96 remains green.
   - read-only N=24 and N=48 remain green.
   - isolated patch/test N>=6 passes with no repo pollution.
   - at least one real PNC-agent style lightweight coding task closes through shared-state evidence.
5. Observability:
   - Dashboard/API surfaces route, final marker, artifacts, verification, retries, and blockers in human terms.
6. Release governance:
   - fork package hashes match current files.
   - docs distinguish live runtime evidence from packaged artifact.
   - no push to origin/upstream without explicit approval.

---

## 4. Phase plan and acceptance standards

### Phase 0: Evidence and manifest reconciliation

Objective: make the current package honest and self-checking before more coding.

Files:
- Modify: `ops/vm-worker-state/README.md`
- Modify: `ops/vm-worker-state/manifest.json`
- Modify: `ops/vm-worker-state/fork-branch-manifest.json`
- Create: `ops/vm-worker-state/evidence/20260526-codex-concurrency-evidence.json`

Steps:
1. Update README: metadata gate and final marker persistence are implemented.
2. Add current N=12 recheck and N=24 read-only evidence.
3. Regenerate file size/SHA256 manifests.
4. Add compact evidence receipt with VM/CIFS paths.
5. Run `python3 -m py_compile ops/vm-worker-state/*.py` and `git diff --check`.

Acceptance:
- stale known-caveat statements: 0
- manifest hash mismatches: 0
- unreadable referenced evidence files: 0
- packaged Python syntax failures: 0

Exit criteria:
- P0 evidence receipt exists and parses.
- Commit created locally on fork branch.
- Push only to fork if publication is requested/approved.

### Phase 1: Standard performance telemetry

Objective: make concurrency performance measurable instead of manually inferred from `waves.log`.

Files:
- Modify: `ops/vm-worker-state/vm_codex_window_runner.py`
- Optional create: `ops/vm-worker-state/smoke/telemetry_fixture.py`

Implementation requirements:
- Generate `performance.json` beside `summary.json` with:
  - started/finished/wall seconds
  - count/ok/failed/pending/claimed
  - configured window
  - effective max running
  - waves count
  - retry/timeout counts
  - per-task terminal state and optional duration
  - resource snapshot if cheaply available.
- Preserve existing `summary.json` shape.
- Do not start extra tasks just for telemetry.

Acceptance cases:
- P1-01: existing N=24 evidence post-processes into performance receipt.
- P1-02: duplicate/retry wave entries are represented, not hidden.
- P1-03: missing waves file does not break summary; telemetry marks it missing.
- P1-04: summary compatibility remains unchanged.

Metrics:
- runs missing performance receipt: 0
- known N=24 parse failures: 0
- summary compatibility regressions: 0

Exit criteria:
- Existing N=24 can be post-processed without rerunning tasks.
- A fresh N=12 or fixture replay produces `performance.json`.

### Phase 2: Adaptive concurrency policy

Objective: choose a safe window based on live VM conditions and workload type.

Files:
- Modify: `ops/vm-worker-state/vm_codex_window_runner.py`
- Optional create: `ops/vm-worker-state/concurrency_policy.json`
- Modify: README policy section

Initial policy:

| Workload class | Default window | Max before extra proof | Gate |
|---|---:|---:|---|
| trivial canary | 24 | 32 | no DNP/MCAP heavy pressure, SSH clean |
| repo read-only | 6 | 8 | `ok_for_submit=True`, extra SSH <= 1, mem > 64GiB |
| isolated patch/test | 2 | 4 | clean isolated workdir/output |
| PNC/DNP heavy or MCAP | 0 | 0 | route through governed tools only |

Acceptance cases:
- P2-01: read-only default selects window 6.
- P2-02: requested window 24 for repo_read is clamped or requires explicit override.
- P2-03: simulated SSH residual pressure blocks new starts.
- P2-04: VM-heavy/MCAP metadata is blocked from Codex runner.
- P2-05: trivial canary can still use window24.

Metrics:
- unsafe-resource launches: 0
- unexplained window choices: 0
- blocked cases with reason: 100%

Exit criteria:
- Policy covered by fixture tests.
- Fresh read-only run uses policy-selected window and records policy receipt.

### Phase 3: Scheduler / daemon integration

Objective: make Codex lane a real shared-state scheduler path, not a manual sidecar.

Discovery first:
- Locate live daemon/service source and startup command.
- Locate shared-state worker startup scripts under `/home/mini/.hermes/worker-state`.
- Preserve sidecar runner as fallback.

Implementation:
1. Add dry-run selector for pending Codex tasks.
2. Add bounded live mode with policy-selected window.
3. Keep deterministic lanes unchanged.
4. Add lease/stale-claim recovery that cannot duplicate active PIDs.

Acceptance cases:
- P3-01: dry-run lists Codex tasks without moving dispatch files.
- P3-02: missing metadata fails closed; no fallback.
- P3-03: direct_cli/governed_tool route unchanged.
- P3-04: daemon-driven real read-only N=12 passes 12/12.
- P3-05: daemon restart mid-batch loses/duplicates 0 terminal tasks.

Metrics:
- OpenClaw fallback for intended Codex tasks: 0
- deterministic-lane regressions: 0
- terminal tasks missing final marker artifact: 0
- duplicate active task processes: 0

Exit criteria:
- Daemon path has rollback command.
- Daemon-driven read-only N=12 passes.
- Sidecar runner remains usable as fallback.

### Phase 4: Real isolated patch/test coding smoke

Objective: prove the lane can do actual coding safely, not only read files.

Boundaries:
- Use isolated scratch repos/worktrees under `/mnt/tmp/<task_id>/`.
- Do not mutate `/home/mini/minieye_dnp_nop` main repo in this phase.
- Real PNC repo patching requires owner approval and git audit rules.

Acceptance cases:
- P4-01: scratch patch/test: test fails before and passes after.
- P4-02: N=6 independent scratch patch/test tasks pass 6/6 without path collision.
- P4-03: intentionally broken fixture reports failed/blocked, not completed.
- P4-04: real repo `git status --short` unchanged after smoke.
- P4-05: temp repo rollback/remove succeeds.

Metrics:
- unverified completed coding tasks: 0
- real repo dirty diffs from smoke: 0
- patch tasks with test receipt: 100%
- failed tests reported as completed: 0

Exit criteria:
- Scratch patch/test N=6 green.
- One negative verification case fails safely.
- No real repo pollution.

### Phase 5: Observability and operator cockpit

Objective: expose the coding-agent lane as a human-operable shared-state/VM task surface.

Acceptance cases:
- P5-01: real N=24 task visible via API/detail.
- P5-02: list view does not hide VM/coding-agent tasks by default.
- P5-03: final marker/artifact shown in human-readable section.
- P5-04: missing final marker is shown as blocker, not success.
- P5-05: raw JSON stays behind debug/details, not main reading path.

Metrics:
- visible recent coding-agent tasks from list: 100%
- completed tasks with missing evidence shown as success: 0
- raw internal JSON on main reading path: 0

Exit criteria:
- At least one real read-only task and one patch/test smoke task are API/browser verified.
- Any synthetic smoke task is cleaned up or marked completed.

### Phase 6: Release and rollout governance

Objective: move from fork sidecar pack to controlled ops release.

Tasks:
1. Refresh README/manifests after Phases 1-5.
2. Build release receipt with commit SHA, changed files, tests/smokes, live VM runtime file mtimes, rollback, fork push status.
3. Produce offline pack if GitHub/API is flaky.
4. Do not push origin/upstream unless explicitly approved.
5. For live VM runtime changes, record backup/rollback path.

Acceptance cases:
- P6-01: fork branch points to intended commit or push output proves update.
- P6-02: all manifest hashes match.
- P6-03: offline verifier passes if offline pack is produced.
- P6-04: live runtime rollback path identified.
- P6-05: origin push count remains 0.

Metrics:
- origin pushes without approval: 0
- release files with stale hashes: 0
- rollback path missing for live runtime edits: 0

Exit criteria:
- Release receipt exists.
- Fork branch or offline pack is externally verifiable.
- User receives exact next action for merge/review.

---

## 5. Recommended immediate order

1. Phase 0 now: README + manifests + compact evidence receipt.
2. Phase 1 next: `performance.json` generation and N=24 replay.
3. Phase 2: encode policy; repo_read default window 6, max 8 until N=48 proves stable.
4. Phase 3: locate daemon source and add dry-run lane selector; daemon-driven N=12.
5. Phase 4: scratch patch/test N=6 + negative verification.
6. Phase 5/6: Dashboard/operator evidence + release receipt.

---

## 6. Resume commands

```bash
# Host health
~/.local/bin/ssh-mini-status
~/.local/bin/ssh-mini-resource --summary

# Current strongest read-only evidence
~/.local/bin/ssh-mini-agent head /mnt/tmp/codex_readonly_real24_20260526_1709/summary.json 120
~/.local/bin/ssh-mini-agent head /mnt/tmp/codex-readonly-real24-run-20260526-1709/status.json 80
~/.local/bin/ssh-mini-agent tail /mnt/tmp/codex_readonly_real24_20260526_1709/waves.log 120

# Historical false-negative RCA proof
~/.local/bin/ssh-mini-agent head /mnt/tmp/codex_readonly_real12_20260526_1635/summary.json 120
~/.local/bin/ssh-mini-agent head /mnt/tmp/codex_readonly_real12_20260526_1635_recheck/summary.json 120

# Branch/package checks
cd /Users/songying/.hermes/worktrees/hermes-agent-vm-codex-window-runner-20260526
python3 -m py_compile ops/vm-worker-state/vm_coding_worker_v2.py ops/vm-worker-state/vm_codex_window_runner.py
git diff --check -- ops/vm-worker-state
```

---

## 7. Non-regression contract

- No push to `origin` / upstream without explicit approval.
- Non-Codex tasks do not inherit Codex behavior.
- Intended Codex tasks never silently fall back to OpenClaw when metadata is incomplete.
- MCAP/open-foxglove/DNP-heavy tasks never bypass governed wrappers through this lane.
- Read-only smokes leave `/home/mini/minieye_dnp_nop` clean.
- Every user-facing VM artifact path includes both VM path and CIFS path.
- SSH residual pressure is checked before sizeable runs.
- Completion is not claimed unless terminal shared-state state, final marker artifact, and verification receipt agree.
