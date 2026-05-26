# VM worker-state Codex execution-plane sidecar

This directory packages the Minieye VM runtime sidecar files currently deployed under:

```text
/home/mini/.hermes/worker-state/
```

It is intentionally an ops sidecar pack, not a claim that these files are part of the upstream Hermes runtime source tree. The current VM systemd daemon launches the runtime copies directly from `worker-state`; the provisioning/source repository has not been located yet.

## Included files

- `vm_coding_worker_v2.py`
  - routes `agent_backend=codex` tasks to `/usr/bin/codex exec`
  - supports `codex_prompt_override`
  - supports `expected_final_message`
  - writes/uses `--output-last-message` artifact
- `vm_codex_window_runner.py`
  - runs existing shared-state Codex tasks with bounded windows
  - retries timeout failures
  - reconciles cases where the final artifact exists but canonical result import lagged

## Safety boundary

Do not push this branch to `origin`. Publish only to a fork branch, for example:

```bash
git push -u fork vm-codex-window-runner-20260526
```

Do not install these files onto a live VM without an explicit ops action. This pack is for review, handoff, and fork publication.

## Verified behavior

Runtime validation performed on VM:

- Direct short Codex canaries reached N=48 all green.
- Total N=96 completed with window24 + timeout retry + artifact reconciliation.
- Final N=96 batch `codex-n96-window24-20260526-100720` ended with:
  - pass_count: 96
  - failed_count: 0
  - claimed_count: 0
  - pending_count: 0

## Known caveat

Tasks whose dispatch metadata does not carry `agent_backend=codex` can still fall back to OpenClaw. Before promoting this sidecar into a live scheduler, add a claim preflight gate that rejects intended Codex batches unless all of the following are true:

- `executor_type == "coding_agent"`
- `agent_backend == "codex"`
- `codex_backend_enabled == true`

## Suggested next patch

Add a fail-closed metadata gate before claim/execute for Codex batches, then run a real lightweight coding-task regression (read-only repo inspection first, then small isolated patch/test lanes).
