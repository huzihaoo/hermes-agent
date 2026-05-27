# VM worker-state Codex execution-plane sidecar

This directory packages the Minieye VM runtime sidecar files currently deployed under:

```text
/home/mini/.hermes/worker-state/
```

It is intentionally an ops sidecar pack, not a claim that these files are part of the upstream Hermes runtime source tree. The current VM runtime copies execute directly from `worker-state`; the provisioning/source repository for the live daemon has not been located yet.

## Included files

- `vm_coding_worker_v2.py`
  - routes `executor_type=coding_agent` + `agent_backend=codex` tasks to Codex execution
  - fails closed for intended Codex batches unless all required metadata is present:
    - `executor_type == "coding_agent"`
    - `agent_backend == "codex"`
    - `codex_backend_enabled == true`
  - supports `codex_prompt_override`
  - supports `expected_final_message`
  - writes/uses `--output-last-message` artifact
  - promotes `runner.log` final assistant text into `codex-last-message.txt` when Codex/embedded runner did not leave the artifact directly
- `vm_codex_window_runner.py`
  - runs existing shared-state Codex tasks with bounded windows
  - retries timeout failures
  - reconciles cases where final artifacts exist but canonical result import lagged
- `codex-concurrency-development-plan-20260526.md`
  - phase plan for performance telemetry, adaptive policy, scheduler integration, patch/test smoke, observability, and rollout governance
- `evidence/20260526-codex-concurrency-evidence.json`
  - compact machine-readable evidence snapshot for current real smoke results

## Safety boundary

Do not push this branch to `origin`. Publish only to a fork branch, for example:

```bash
git push -u fork vm-codex-window-runner-20260526
```

Do not install these files onto a live VM without an explicit ops action. This pack is for review, handoff, and fork publication. The live VM currently has matching runtime edits applied under `/home/mini/.hermes/worker-state/`, but that live state should be treated as operational evidence, not as upstream source-of-truth.

## Verified behavior

Runtime validation performed on VM:

- Direct short Codex canaries reached N=48 all green.
- Total short canary N=96 completed with window24 + timeout retry + artifact reconciliation.
- Final N=96 batch `codex-n96-window24-20260526-100720` ended with:
  - pass_count: 96
  - failed_count: 0
  - claimed_count: 0
  - pending_count: 0
- Historical real read-only N=12 initially reported 10/12 because final marker extraction was too narrow, then rechecked 12/12 after promoting final assistant text into `codex-last-message.txt`:
  - initial: `/mnt/tmp/codex_readonly_real12_20260526_1635/summary.json`
  - recheck: `/mnt/tmp/codex_readonly_real12_20260526_1635_recheck/summary.json`
- Real read-only N=24 completed with window=6 + max_retries=1:
  - summary: `/mnt/tmp/codex_readonly_real24_20260526_1709/summary.json`
  - run status: `/mnt/tmp/codex-readonly-real24-run-20260526-1709/status.json`
  - result: ok_count=24, done=24, failed=0, claimed=0, pending=0
  - wall time: 617 seconds
  - throughput: 2.33 tasks/minute
  - user-visible CIFS: `//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/codex_readonly_real24_20260526_1709/`

## Current caveats

- `vm_codex_window_runner.py` is still a runtime sidecar, not a formally integrated scheduler/daemon path.
- Effective concurrency is not yet a first-class metric: `waves.log` exists, but no standard `performance.json` with p50/p95, retry counts, effective max running, and resource snapshot is generated yet.
- Strongest real smoke is read-only repo inspection. Isolated patch/test coding smoke is still pending.
- Dashboard/operator observability for this Codex lane still needs API/list/detail proof.

## Completed rollout phases in this pack

- Phase 0: evidence and manifest reconciliation.
- Phase 1: `performance.json` telemetry generation plus `--telemetry-only` post-processing.
- Phase 2: adaptive concurrency policy receipts and fail-closed VM-heavy blocking.


## Scheduler / daemon discovery

Live VM discovery found these current candidates:

- active user service: `hermes-vm-coding-worker-daemon.service`
- active watchdog: `hermes-vm-scheduler-operator-summary-watchdog.service`
- scheduler source: `/home/mini/.hermes/worker-state/vm_coding_worker_scheduler.py`
- daemon wrapper source: `/home/mini/.hermes/worker-state/vm_coding_worker_daemon.py`
- service example: `/home/mini/.hermes/worker-state/hermes-vm-coding-worker-scheduler.service.example`

The service example is explicitly dry-run/shadow by default. Do not enable execute mode without an approved isolated/cutover drill.

## Dry-run selector

`vm_codex_window_runner.py` now supports:

```bash
python3 vm_codex_window_runner.py   --out /mnt/tmp/<selector_out>   --dry-run-selector   --selector-prefix <optional-prefix>   --selector-limit 50
```

It writes `selector.json` and `summary.json`, classifying pending tasks as:

- `eligible`: valid `executor_type=coding_agent`, `agent_backend=codex`, `codex_backend_enabled=true`
- `rejected`: intended Codex tasks with incomplete/wrong metadata
- `skipped`: non-Codex tasks

It has no dispatch side effects: it does not claim, move, or execute tasks.


### Phase 3b scheduler dry-run selector evidence

`vm_coding_worker_scheduler.py` now exposes `codex_dry_run_selector` in its dry-run plan output. It classifies pending tasks into `eligible`, `rejected`, and `skipped` using the same Codex metadata gate.

Validation used an isolated shared-state root at:

```text
/mnt/tmp/codex_scheduler_dryrun_phase3b_isolated_20260527/shared-state
```

The isolated dry-run result was 1 eligible / 1 rejected / 1 skipped and dispatch files were unchanged. A live-root attempt showed why isolated validation matters: the already-active `hermes-vm-coding-worker-daemon.service` can claim eligible pending tasks before a shadow dry-run observes them.

## Next planned phase

Follow `codex-concurrency-development-plan-20260526.md`:

1. Wire this dry-run selector into the discovered scheduler path without execute mode.
2. Run daemon-driven read-only N=12 with policy receipts and performance telemetry.
3. Run isolated scratch patch/test N=6 before any real repo mutation smoke.
4. Add Dashboard/operator visibility for policy, final marker, verification, and artifacts.



### Phase 3c live dry-run observation boundary

A live scheduler dry-run observation was executed at:

```text
/mnt/tmp/codex_scheduler_phase3c_live_observe_20260527/plan.json
```

At observation time there were no live pending eligible Codex tasks. The live dry-run result was:

- `eligible_count=0`
- `rejected_count=1`
- `skipped_count=1`
- `pending_count=2`

The only Codex-like pending task was the intentional metadata-rejected fixture (`agent_backend=openclaw` with `codex_backend_enabled=true`). Therefore the correct conclusion is:

- scheduler dry-run path works on live root without execute,
- but there is currently no honest live eligible pending Codex sample to claim as proof.

Do not fabricate a live eligible selector result from this cycle.
