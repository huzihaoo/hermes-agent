# PNC-Agent Release 0.13.8 Execution Receipt

- release: PNC-Agent Release 0.13.8
- tag: pnc-agent-v0.13.8-overlay.20260512
- commit: fd98ffe758f2283acf25e2637aadcca98777440c
- artifact: dist/hermes-agent/hermes-agent-v0.13.8.tar.gz
- sha256: 0f4d0f59e99956e25e4a84966752c26aa637a42d361717da16db2b8950293095
- release doc: https://feishu.cn/docx/YQ10dU1N3o3GB0xfAHlcRL9Jnqb

## Scope

Expose pnc_specs open-foxglove through Hermes/Feishu pnc_agents. The release adds open_foxglove tool registration, default arguments, manifest routing, Feishu toolset exposure, and focused regression tests. Admission remains 1.11.2.

## Broader release dimensions reviewed

- Agent framework / runtime design: reviewed; no post-tag source delta beyond the released open_foxglove wiring, toolset exposure, and runtime cutover already recorded.
- Skills: reviewed; no skill file changes were part of this release tree. Future releases must explicitly classify every skill change as release scope, including additions, removals, optimizations, fixes, configuration changes, behavior changes, packaging, distribution, and skill-loading policy changes.
- Knowledge engineering: reviewed; no new knowledge corpus/schema/version artifact was introduced in this cutover, and any future knowledge updates should be classified as released, draft-only, or excluded.
- Security / permissions / privacy: reviewed; no ACL, approval-flow, token/secret handling, source-leak boundary, Feishu/GitLab/VM access, or new external-send surface change was part of this cutover.
- Configuration / deployment / entrypoints: released; host LaunchAgent, fresh CLI wrapper/process, and VM systemd services were restarted or verified so Feishu group entrypoints, host gateway, VM gateway, worker, and CLI all use the intended release boundary.
- API / contracts / compatibility: released; open_foxglove tool schema and pnc_agents/hermes-feishu toolset exposure were verified, with Admission remaining 1.11.2 and no incompatible API migration recorded.
- Dependencies / packaging / build artifacts: released; pyproject/hermes_cli version bump, source tarball, manifest, and sha256 were generated and checked. No dependency/lockfile change was included.
- Data / state / migration: reviewed; no persistent DB/storage migration was required. Runtime state/checkpoint files remain evidence or excluded runtime state, and resume checkpoint is recorded below.
- Observability / operations: reviewed; host health/detailed, Feishu/API connection states, VM service active/running states, PID changes, and artifact checks were captured. No new alert/metric pipeline change was included.
- Docs / release notes / user communication: released; Feishu child release doc was uploaded and read back, local receipt updated, canonical append remained fail-closed by existing policy.
- Tests / quality gates / safety scans: released; focused regression tests, py_compile, check_versions, git diff --check, artifact sha256, fresh CLI smoke, and runtime health checks passed.
- Other modules: reviewed; docs, tests, packaging, release receipts, and gateway/session behavior were checked, with only the published release outputs remaining untracked locally.

## Verification

- scripts/run_tests.sh focused open_foxglove/toolset tests: 4 passed
- python3.11 -m py_compile: passed
- python3.11 check_versions.py: passed
- git diff --check: passed
- shasum -a 256 -c dist/hermes-agent/hermes-agent-v0.13.8.tar.gz.sha256: OK
- fresh CLI process /Users/songying/bin/hermes --version: Hermes Agent v0.13.8 (2026.5.12)

## Runtime cutover

Host gateway LaunchAgent ai.hermes.gateway restarted from PID 17777 to 57756. launchctl confirms program /Users/songying/.hermes/worktrees/hermes-agent-sync-v0.13-overlay-20260508/.venv-v013-candidate/bin/python and working directory /Users/songying/.hermes/worktrees/hermes-agent-sync-v0.13-overlay-20260508. /health/detailed reported status ok, gateway_state running, Feishu connected, API server connected.

VM Hermes services restarted successfully:
- admission-service.service: 2377195 -> 2664424, active/running
- hermes-gateway.service: 2377200 -> 2664428, active/running; Node/OpenClaw gateway
- hermes-vm-coding-worker-daemon.service: 2377208 -> 2664435, active/running

VM pnc_specs preflight confirmed open-foxglove executable and manifest in /home/mini/pnc_specs/pnc_tools_ai_native/32_AI_Native_repo_骨架包_真实首批版_v1 and /home/mini/worktrees/pnc_specs/宋伟军/pnc_tools_ai_native/32_AI_Native_repo_骨架包_真实首批版_v1. Help output returned successfully.

## Notes

The Feishu child release doc readback contains all required headings and version/tag/hash tokens. MCP rendered table cells as separated text but no empty code fences were present; accepted as readable release record.

## Resume checkpoint

If this release work is resumed later, start from this state instead of redoing completed gates:

- worktree: /Users/songying/.hermes/worktrees/hermes-agent-sync-v0.13-overlay-20260508
- branch: sync/upstream-v0.13-overlay-20260508
- release tag: pnc-agent-v0.13.8-overlay.20260512
- release commit: fd98ffe758f2283acf25e2637aadcca98777440c
- artifact: dist/hermes-agent/hermes-agent-v0.13.8.tar.gz
- artifact sha256: 0f4d0f59e99956e25e4a84966752c26aa637a42d361717da16db2b8950293095
- release doc: https://feishu.cn/docx/YQ10dU1N3o3GB0xfAHlcRL9Jnqb
- host gateway PID: 57756
- VM admission PID: 2664424
- VM gateway PID: 2664428
- VM worker PID: 2664435
- verified CLI: Hermes Agent v0.13.8 (2026.5.12)
- resume rule: inspect only delta since this checkpoint; do not rerun already-verified gates unless the code or runtime changed

## Rerun release flow verification - 2026-05-12

The release flow was rerun from the verified checkpoint instead of rebuilding the release from scratch.

- Code delta since release commit: none. HEAD remains fd98ffe758f2283acf25e2637aadcca98777440c and the peeled tag pnc-agent-v0.13.8-overlay.20260512 points to the same commit.
- Core repo dirty state: only outputs/ remains untracked as local release receipts; no post-tag source delta.
- Broader module scan: active release worktree, legacy hermes-agent checkout, workspace-work, workspace, local-mcp/feishu-doc, and skills were inspected. The active release remains 0.13.8; unrelated legacy dirty trees and non-git skill optimizations are classified outside this artifact commit unless explicitly released as their own module/version.
- Gates rerun: py_compile passed; check_versions.py passed; artifact sha256 passed; git diff --check passed; focused open_foxglove/toolset tests passed: 4 passed in 5.88s.
- Host cutover rerun: LaunchAgent ai.hermes.gateway restarted from PID 57756 to 72830. /health and /health/detailed returned ok after reconnect; Feishu and API server are connected.
- VM cutover rerun: admission-service.service 2664424 -> 2680378, hermes-gateway.service 2664428 -> 2680383, hermes-vm-coding-worker-daemon.service 2664435 -> 2680414; all active/running.
- Fresh CLI rerun: /Users/songying/bin/hermes --version reports Hermes Agent v0.13.8 (2026.5.12).
- Feishu release doc readback rerun: YQ10dU1N3o3GB0xfAHlcRL9Jnqb revision 2 contains release title, open_foxglove scope, commit, tag, sha256, validation, and notes.
- Decision: no new release version/tag is required for the rerun because no release-relevant source/artifact delta exists after 0.13.8. The rerun refreshed host, VM, CLI, and Feishu evidence for the existing release.


## Hotfix verification - fallback preflight threshold - 2026-05-12

User-visible symptom after the 0.13.8 cutover: a Feishu task still emitted `Preflight compression: ~99,398 tokens >= 96,000 fallback threshold`, which should not happen while the primary model lane is healthy.

Root cause: the active Host/Feishu runtime is the release overlay worktree `/Users/songying/.hermes/worktrees/hermes-agent-sync-v0.13-overlay-20260508`, while the fallback preflight fix had been implemented first in `/Users/songying/.hermes/hermes-agent`. The overlay worktree still contained the older stable-first preflight logic that always lowered the preflight threshold to the smallest fallback context window.

Hotfix applied to the active release overlay worktree:

- `run_agent.py`: preflight compression now uses the fallback threshold only after fallback has actually activated (`_fallback_just_activated`), so healthy primary turns no longer compact at the fallback 128K/96K trigger.
- `run_agent.py`: `_fallback_just_activated` is initialized and cleared when primary runtime is restored.
- `run_agent.py`: primary runtime snapshots preserve `compressor_api_mode` when restoring the compressor.

Verification:

- Static source check confirms `Stable-first only after failover` is present and the old `Stable-first: if a fallback model has a smaller configured context` block is absent in the active overlay worktree.
- `python3 -m py_compile run_agent.py`: passed.
- `python3 -m pytest -q -o addopts='' tests/run_agent/test_fallback_model.py tests/run_agent/test_compressor_fallback_update.py`: 30 passed.
- Host LaunchAgent `ai.hermes.gateway` restarted after the hotfix; new PID observed: 89893.
- `/health/detailed` after restart reports status ok, gateway_state running, Feishu connected, API server connected.

Decision: this is a runtime hotfix to align the active 0.13.8 overlay with the already-validated fallback preflight behavior. No artifact/tag rebuild was performed in this step; this receipt records the post-release operational correction and verification.

## Communication and release-doc closure - 2026-05-12

Follow-up communication and documentation were completed after the fallback preflight hotfix:

- Feishu group notification sent to home channel `oc_16614f4ba25b8c88b69c0b8e9ebc2fb5`.
- Feishu message id: `om_x100b6f00fd8374a4b3e065edd402fbb`.
- Feishu release document `YQ10dU1N3o3GB0xfAHlcRL9Jnqb` was updated in place through the docx blocks API. The existing document/link was preserved; the doc was not deleted/recreated.
- Readback verified the release doc now contains:
  - `Hotfix - fallback preflight threshold（2026-05-12）`
  - `message_id=om_x100b6f00fd8374a4b3e065edd402fbb`
  - `健康 primary 轮次不再按 96K fallback threshold 提前压缩`
- Added reusable runtime guard script: `/Users/songying/bin/hermes-fallback-hotfix-check`.
- Guard script verification passed: active LaunchAgent working directory is the release overlay, hotfix marker is present, old preflight block is absent, and `/health/detailed` reports ok/running with Feishu and API server connected.

Rollback / recovery note for `/Users/songying/bin/hermes-fallback-hotfix-check`: remove this guard script if no longer desired; it is read-only and does not mutate runtime state.

## Follow-up: stale interactive CLI sessions (2026-05-13)

Observation:
- Active gateway/runtime guard passed after hotfix.
- Existing interactive Hermes CLI processes can still show `fallback threshold` preflight lines if they were opened before the active overlay `run_agent.py` hotfix, because Python processes do not hot-reload module code.

Current operator guard:
- `/Users/songying/bin/hermes-fallback-hotfix-check` now verifies active overlay/gateway health and prints non-failing INFO lines listing open interactive Hermes CLI processes that should be restarted to pick up the hotfix.

Convergence rule:
- Gateway/Feishu entry is judged by LaunchAgent PID + active runtime root + health endpoint + marker checks.
- Interactive CLI tabs are judged independently; restart old tabs after runtime hotfixes before treating their preflight output as a fresh regression.
