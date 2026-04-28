# VM 执行链路收敛实施计划

> For Hermes: planning only. Do not implement in this pass.

Goal: 把 Feishu/Hermes 涉及 VM 的业务执行，从“主脑 direct ssh-mini 执行 + 少量 shared-state v2 投递”的混合态，收敛为“主脑控制面 + shared-state v2 投递 + VM worker/VM 侧模型执行 + canonical result 回收”的一致链路。

Architecture: host/main 只做 intake、权限解析、任务组装、投递、状态读取、结果交付；业务代码/PNC 工具/编译测试/长任务的执行事实全部归 VM worker。direct ssh-mini 保留为只读探测、控制面维护、紧急 owner debug，不作为默认业务执行面。

Repo: /Users/songying/.hermes/hermes-agent
Branch observed: overlay/stable
Observed working tree: already has in-flight changes in gateway/platforms/feishu.py, tests/gateway/test_feishu.py, tests/test_model_tools.py, tests/tools/test_pnc_agent_tools.py, tests/tools/test_vm_task_tool.py, tools/pnc_agent_tools.py, tools/vm_task_tool.py, toolsets.py.

---

## 1. gstack-style审视结论

### 1.1 当前已经对齐的部分

Local governance 已明确：
- official demand name: 长程编码任务链路
- current implementation mechanism: shared-state v2
- host main owns control-plane work; VM worker owns execution truth
- create_task_v2.py 成功只代表 host-created
- 进入 VM canonical queue 才是 delivered-to-VM
- VM worker claim 后才是 picked-up

Implementation 已存在：
- tools/vm_task_tool.py 注册 vm_task_submit
- vm_task_submit 走 create_task_v2.py --bridge-root ... --deliver-bridge --json
- toolsets.py 已有 vm_tasks，并在 hermes-feishu 中暴露 vm_task_submit
- permission_policy.py 能识别 ssh-mini-agent run_bash_json/run_py_json/edit_file、ssh-mini-run、raw ssh mini 为 vm_direct_exec
- approval.py 已把这些 direct exec 标为 VM direct execution，提示 prefer shared-state v2 -> VM worker
- vm_task_submit 当前已经补上 trusted session owner 解析与权限检查，测试 test_vm_task_submit_* 已覆盖 owner spoofing 和 member deny

### 1.2 当前不一致 / 风险点

P0 风险：PNC 真实工具仍是 direct VM execution wrapper。
- tools/pnc_agent_tools.py 的 generate_dbc / parse_bus_data 当前通过 subprocess.run([ssh-mini-agent, run_bash_json], input=script, ...) 同步远程执行。
- 这比任意 ssh-mini 安全一些，因为有固定 CLI、用户解析、worktree ensure、路径检查、权限检查；但它仍绕过 shared-state v2 / VM worker claim / canonical result import。
- 因此它不能作为“任务投递后由 VM 侧模型/worker 执行”的一致实现。

P0 风险：owner direct VM exec 仍可默认放行。
- permission_policy.py 对 vm_direct_exec 的 fallback 是 owner ALLOW, admin CONFIRM, senior APPROVE, member DENY。
- 这适合维护/紧急 debug，但不适合作为 Feishu business-domain VM execution 的默认行为。

P1 风险：工具语义没有区分 sync smoke/read-only 和 async business task。
- pnc_agents_smoke 适合保留 direct，因为它只做路由/可用性检查。
- generate_dbc / parse_bus_data 是业务执行，应该返回 task_id 和 routing 状态，而不是同步跑完并返回远端 stdout。

P1 风险：executor 没显式入任务 payload。
- shared-state v2 能证明 VM worker 执行，但需要在 goal/meta 中显式标明 executor intent：fixed-cli / codex / claude / hermes。
- 否则后续无法审计“到底是 VM worker 固定脚本执行，还是 VM 侧模型 agent 执行”。

P2 风险：当前 focused verification 有外部失败。
- Command: python -m pytest tests/tools/test_vm_task_tool.py tests/gateway/test_feishu_vm_direct_execution_guard.py tests/test_model_tools.py -q
- Result: 26 passed, 3 failed
- Failures all in tests/test_model_tools.py around web_search not registered in this runtime/toolset; not directly caused by VM task changes, but implementation前应拆分 focused test targets，避免无关失败挡住收敛验证。

---

## 2. Target design

### 2.1 Execution policy

Business VM execution must go through:

Feishu/gateway
-> trusted user_id/display_name resolve
-> role/capability policy
-> workspace/worktree resolve intent
-> frozen task brief / ResolvedSnapshot-like payload
-> vm_task_submit
-> create_task_v2.py --deliver-bridge
-> VM canonical queue
-> VM worker claim
-> VM-side executor runs
-> worker writes result/log/proof
-> host import_worker_snapshot / canonical status reader
-> delivery adapter / redaction

Direct ssh-mini remains allowed only for:
- read-only probes: doctor, list_files, read_file, grep, head, tail
- control-plane maintenance: worker health, bridge delivery debugging, import recovery, service restart when owner/admin
- explicit owner emergency debug with audit marker

### 2.2 Tool behavior split

Keep direct/synchronous:
- pnc_agents_smoke: route/user/worktree/tool-root smoke only; no business execution.
- ssh-mini read helpers.

Convert to async shared-state task:
- generate_dbc
- parse_bus_data
- future build/test/replay/eval style PNC tools

Return shape for converted PNC tools:
```json
{
  "ok": true,
  "mode": "submitted",
  "agent": "generate-dbc",
  "task_id": "...",
  "routing": {
    "host_state": "host-created",
    "delivery_attempted": true,
    "next_truth_checks": [
      "confirm task appears in VM canonical queue before saying delivered-to-VM",
      "confirm VM worker claim before saying picked-up",
      "use canonical status reader/result import for completion truth"
    ]
  }
}
```

Do not return remote stdout/stderr as if task is complete.

---

## 3. Implementation phases

## Phase 0: Freeze behavior with tests

Objective: Add tests that define the desired policy before changing implementation.

Files:
- Modify: tests/tools/test_pnc_agent_tools.py
- Modify: tests/gateway/test_feishu_vm_direct_execution_guard.py
- Maybe modify: tests/tools/test_vm_task_tool.py

Steps:
1. Add a test that generate_dbc no longer calls ssh-mini-agent run_bash_json in default mode.
   - Monkeypatch pnc_agent_tools.vm_task_submit_json or a helper wrapper.
   - Call generate_dbc_tool({"input": "/home/mini/worktrees/pnc_specs/郭艳彬/in.dbc", "output": "/home/mini/worktrees/pnc_specs/郭艳彬/out"}, user_id="ou_guo").
   - Assert result mode == submitted.
   - Assert helper received title/goal, not remote bash script.

2. Add a test that parse_bus_data follows the same async submission path.

3. Add a test that pnc_agents_smoke remains direct smoke and still does not invoke ./generate-dbc or ./parse-bus-data.

4. Add a test for member denial before task creation.
   - Existing member denial for direct execution exists; add one for async submission path if not already covered by vm_task_submit.

5. Add or update direct-exec guard test:
   - For Feishu business-domain ssh-mini-run, expected final policy should prefer deny/degrade to vm_task_submit unless explicit emergency override is present.
   - Keep owner emergency debug as separate explicit path, not default.

Focused commands:
```bash
python -m pytest tests/tools/test_vm_task_tool.py -q
python -m pytest tests/tools/test_pnc_agent_tools.py -q
python -m pytest tests/gateway/test_feishu_vm_direct_execution_guard.py -q
```

Avoid relying on tests/test_model_tools.py until web_search registration drift is separately resolved.

---

## Phase 1: Introduce PNC task submission builder

Objective: Create a narrow internal helper that turns PNC tool args into a self-contained VM-visible task brief.

Files:
- Modify: tools/pnc_agent_tools.py
- Modify: tests/tools/test_pnc_agent_tools.py

Add helper:
```python
def _build_pnc_task_goal(agent_name: str, args: dict[str, Any], user: str, user_id: str = "") -> str:
    ...
```

Goal content must include:
- requester user and user_id
- repo: pnc_specs
- required worktree ensure command:
  `python3 /home/mini/.hermes/hermes-agent/gateway/admission/worktree_manager.py ensure <user> pnc_specs`
- resolved agent subdir:
  pnc_tools_ai_native/32_AI_Native_repo_骨架包_真实首批版_v1
- command to execute on VM worker side:
  `./generate-dbc ...` or `./parse-bus-data ...`
- absolute input/output/regression paths
- VM data landing defaults:
  - download_dir=/home/mini/nas/miniPan/tmp/pdcl_downloads
  - work_tmp_dir=/home/mini/nas/miniPan/tmp/<task_id_or_title_slug>
- constraints:
  - do not operate outside resolved user worktree unless using declared NAS tmp dir
  - record audit log before git operations
  - no force push
  - write result summary + artifacts + verification evidence
- executor intent:
  - executor: fixed-cli under VM worker
  - optional future executor: codex/claude for open-ended analysis

Test assertions:
- goal contains worktree_manager.py ensure
- goal contains correct canonical user from user_id mapping
- goal does not contain spoofed public user arg
- goal contains NAS tmp path defaults
- goal contains expected CLI command and absolute input/output paths

---

## Phase 2: Convert generate_dbc / parse_bus_data to async vm_task_submit

Objective: Make business PNC tools submit tasks instead of directly executing remote scripts.

Files:
- Modify: tools/pnc_agent_tools.py
- Modify: tests/tools/test_pnc_agent_tools.py

Implementation shape:
```python
def _submit_pnc_task(agent_name: str, args: dict[str, Any], user_id: str = "") -> str:
    # validate absolute paths as today
    # resolve trusted user as today
    # permission check as today
    # build goal via _build_pnc_task_goal
    # call vm_task_submit.vm_task_submit_json(title=..., goal=..., owner=user, user_id=user_id)
    # wrap/normalize response as tool_result or tool_error
```

Then:
```python
def generate_dbc_tool(args, user_id="", **_):
    return _submit_pnc_task("generate-dbc", args or {}, user_id=user_id)

def parse_bus_data_tool(args, user_id="", **_):
    return _submit_pnc_task("parse-bus-data", args or {}, user_id=user_id)
```

Keep old direct function only if needed for smoke/debug, renamed clearly:
- `_run_remote_agent_direct_for_debug(...)`
- not registered as public Feishu tool
- gated by env var such as PNC_TOOLS_ALLOW_DIRECT_DEBUG=1

Acceptance:
- generate_dbc / parse_bus_data tests no longer expect captured cmd == ["ssh-mini-agent", "run_bash_json"]
- direct smoke test still expects run_bash_json but only for pnc_agents_smoke
- member denial happens before vm_task_submit
- unresolved user denial happens before vm_task_submit

---

## Phase 3: Harden direct VM execution policy

Objective: Make accidental business-domain direct VM exec fail closed in Feishu sessions.

Files:
- Modify: tools/permission_policy.py
- Modify: tools/approval.py if needed
- Modify: tests/gateway/test_feishu_vm_direct_execution_guard.py
- Maybe modify: ~/.hermes/config/user-roles.json only if config needs explicit matrix update; prefer code default first, config second.

Policy recommendation:
- vm_direct_exec default:
  - owner: CONFIRM or ALLOW only when emergency override present
  - admin: CONFIRM
  - senior: APPROVE
  - member: DENY
- In Feishu gateway business-domain contexts, direct ssh-mini-run/run_bash_json should return a message that says use vm_task_submit/shared-state v2 unless the command is classified as control-plane maintenance.

Implementation options:
1. Minimal code-only:
   - Add classifier for control-plane direct commands vs business direct commands.
   - Allow direct only if command includes known maintenance/readiness scripts or env HERMES_VM_DIRECT_EXEC_EMERGENCY=1.
2. Config-driven:
   - Add `vm_direct_exec_control_patterns` and `vm_direct_exec_emergency_roles` to user-roles.json.
   - Code fail-closed if pattern not matched.

Prefer option 1 for first slice; avoid broad config migration unless necessary.

Test cases:
- member direct ssh-mini-run remains denied
- owner business direct ssh-mini-run no longer silently approved by default, or requires explicit emergency override
- read-only ssh-mini-agent list_files remains unblocked
- control-plane maintenance command remains allowed/confirmable for owner/admin

---

## Phase 4: Status/query and delivery semantics

Objective: Ensure async PNC tools give users a usable path to completion.

Files:
- Maybe add: tools/vm_task_status_tool.py or extend existing status reader integration
- Modify: toolsets.py if adding a tool
- Tests: new tests/tools/test_vm_task_status_tool.py

Minimum viable behavior:
- Tool response includes task_id and next truth checks.
- User-visible wording must not say “完成” on submit.
- Delivery should say “已投递/等待 VM 接手”.
- Completion comes only after canonical reader/import sees completed state/result.

Optional next tool:
- vm_task_status(task_id): wraps read_delegated_coding_status_v2/import_worker_snapshot_for_task.

---

## Phase 5: VM worker executor explicitness

Objective: Make it auditable whether VM worker used fixed CLI, codex, claude, or hermes.

Files likely outside host repo / VM side:
- /home/mini/.hermes/worker-state/run_vm_coding_worker_v2.sh
- VM worker implementation scripts under /home/mini/.hermes or /home/mini/.openclaw alias
- shared-state payload reader/importer if meta fields need preservation

Host-side first slice:
- Include executor intent in goal/meta.
- If create_task_v2 supports meta extension later, add structured meta field:
  - executor: fixed-cli
  - tool: generate-dbc / parse_bus_data
  - requester_user_id
  - requester_user_name
  - workspace_policy_ref / resolved_worktree if available

VM-side follow-up:
- Worker records actual executor in result/log.
- Result import preserves actual_executor.
- Status reader surfaces actual_executor.

---

## 4. Recommended exact execution order

1. Run focused baseline tests:
```bash
python -m pytest tests/tools/test_vm_task_tool.py -q
python -m pytest tests/tools/test_pnc_agent_tools.py -q
python -m pytest tests/gateway/test_feishu_vm_direct_execution_guard.py -q
```

2. Update tests for PNC async submission expectation.

3. Implement `_build_pnc_task_goal` and `_submit_pnc_task`.

4. Convert generate_dbc / parse_bus_data to `_submit_pnc_task`.

5. Keep pnc_agents_smoke direct and verify it remains narrow.

6. Harden direct-exec policy after PNC conversion, not before. This avoids breaking the only current business path before replacement exists.

7. Run focused tests again.

8. Run broader but relevant tests:
```bash
python -m pytest tests/tools/test_vm_task_tool.py tests/tools/test_pnc_agent_tools.py tests/gateway/test_feishu_vm_direct_execution_guard.py -q
```

9. Then address unrelated `tests/test_model_tools.py` web_search registration drift separately, or run it only after toolset registry is fixed.

10. Smoke on real VM only after unit tests pass:
- pnc_agents_smoke for a senior user
- generate_dbc submit path with harmless input/output under allowed test worktree/NAS tmp
- confirm status progression via canonical reader:
  - host-created
  - delivered-to-VM
  - picked-up
  - completed/failed with result evidence

---

## 5. Files likely to change

Primary:
- tools/pnc_agent_tools.py
- tests/tools/test_pnc_agent_tools.py
- tools/permission_policy.py
- tools/approval.py
- tests/gateway/test_feishu_vm_direct_execution_guard.py

Secondary:
- tools/vm_task_tool.py only if response normalization needs extra fields
- tests/tools/test_vm_task_tool.py only if vm_task_submit response schema changes
- toolsets.py only if adding vm_task_status or splitting pnc_direct_smoke from pnc_async_tasks
- gateway/platforms/feishu.py only if user-visible submit/status wording needs adjustment

Do not modify VM worker first unless host-side async task submission is already green.

---

## 6. Risks and mitigations

Risk: users expect generate_dbc to return immediate final output.
Mitigation: return clear task_id and status wording; optionally add vm_task_status.

Risk: direct debug becomes too hard for owner.
Mitigation: keep explicit emergency override with audit, but make it visibly different from normal business execution.

Risk: VM worker cannot execute fixed CLI task as currently structured.
Mitigation: first task brief can instruct VM worker to run exact fixed CLI. If worker only supports generic coding-agent prompts, executor=fixed-cli still works as a prompt contract. Later add structured executor.

Risk: path checks weaken when moving from direct script to prompt/worker.
Mitigation: include path policy in task brief now; later enforce in VM-side worker/tool wrapper.

Risk: existing tests are written around direct execution.
Mitigation: update tests intentionally; keep smoke direct tests as the proof that only the smoke path remains synchronous.

---

## 7. Go / No-Go criteria

Go when:
- vm_task_submit permission tests pass
- pnc generate/parse submit tests pass
- direct exec guard tests pass
- real smoke shows submitted task visible in VM canonical queue
- user-visible wording never claims completion on submit

No-Go when:
- generate_dbc / parse_bus_data still call ssh-mini-agent run_bash_json by default
- owner Feishu business direct ssh-mini is silently approved without emergency marker
- task submit response lacks task_id or next truth checks
- VM worker cannot pick up a submitted PNC task

---

## 8. Immediate next slice

Start with Phase 0 + Phase 1 only:
- Rewrite PNC tests from “direct run remote agent” to “submit VM task”.
- Add `_build_pnc_task_goal` with strong assertions.
- Do not touch approval hardening until async PNC path exists.

This is the safest slice because it establishes the new contract without breaking the current operator emergency path prematurely.


---

## 9. Closeout / 2026-04-28 15:01:06 +0800

Implemented:
- PNC `generate_dbc` / `parse_bus_data` now submit shared-state v2 VM tasks instead of direct `ssh-mini-agent run_bash_json` execution.
- PNC task goal now carries requester identity, worktree ensure command, fixed-cli executor intent, NAS tmp landing rules, command intent, audit/safety constraints, and required result contract.
- `pnc_agents_smoke` remains the direct VM smoke path and still does not execute `./generate-dbc` or `./parse-bus-data`.
- Gateway direct VM execution is emergency-gated: Feishu gateway `vm_direct_exec` is not bypassed by yolo/off approval modes and requires `HERMES_VM_DIRECT_EXEC_EMERGENCY=1`.
- Optional `firecrawl` import no longer prevents `web_tools` registration; missing Firecrawl is reported only if Firecrawl backend is actually used.

Verification:
- `python -m pytest tests/tools/test_vm_task_tool.py tests/tools/test_pnc_agent_tools.py tests/gateway/test_feishu_vm_direct_execution_guard.py tests/test_model_tools.py -q`
- Result: `44 passed in 15.44s`
- `git diff --check`: clean

Current diff boundary observed:
- `run_agent.py` and `tests/run_agent/test_agent_guardrails.py` already contain related/in-flight guardrail changes outside the narrow PNC conversion diff.
- Primary conversion/guard files: `tools/pnc_agent_tools.py`, `tests/tools/test_pnc_agent_tools.py`, `tools/approval.py`, `tests/gateway/test_feishu_vm_direct_execution_guard.py`, `tools/web_tools.py`.

Remaining follow-up:
- Real VM smoke with a harmless PNC task should confirm `host-created -> delivered-to-VM -> picked-up -> completed/failed` through canonical reader/result import.
- If desired, add a first-class `vm_task_status` tool so async PNC submissions have an explicit status query surface.


## 10. Final smoke / 2026-04-28 15:05:45 +0800

Additional verification:
- `~/.local/bin/ssh-mini-agent doctor --json` returned `ok: true`.
- Real host-side PNC submit smoke was executed via `generate_dbc_tool(..., user_id='ou_be96d63ed3b673924ab9ee0724b4b549')`.
- Created task: `20260428-150414-pnc-generate-dbc-task-for`.
- Submit response mode: `submitted`.
- Bridge delivery path: `/home/mini/tmp/openclaw-shared-state/inbox/state/20260428150414-20260428-150414-pnc-generate-dbc-task-for-delivery.json`.
- Remote bridge payload was read successfully via `ssh-mini-agent read_file`, proving VM-visible delivery file exists.
- Payload contains the expected generated task brief with:
  - `executor: fixed-cli under VM worker`
  - requester user `郭艳彬`
  - requester user_id `ou_be96d63ed3b673924ab9ee0724b4b549`
  - worktree ensure command
  - NAS tmp landing rules
  - `./generate-dbc ...` command intent
- Canonical reader currently reports `delivery_status: host-created` because VM worker pickup was not forced in this smoke.
- Host bridge import for this task reported `ignored-existing-delivery`, which is expected for a delivery envelope reserved for VM pickup.

Conclusion:
- Host-side submit + ssh-mini bridge delivery are verified end-to-end to VM-visible inbox.
- Full `picked-up -> terminal result` remains a worker-run validation, not required for host-side conversion closeout.


## 11. VM canonical import / 2026-04-28 15:09:43 +0800

Additional progress after final smoke:
- Checked active host canonical queue: 9 active tasks, including the PNC smoke task at `host-created` before VM import.
- Inspected VM worker entrypoint and implementation:
  - `/home/mini/.hermes/worker-state/run_vm_coding_worker_v2.sh`
  - `/home/mini/.hermes/worker-state/vm_coding_worker_v2.py`
- Confirmed the worker imports bridge deliveries before listing/claiming pending tasks.
- Executed VM-side `shared_state_v2.import_bridge_deliveries(...)` via ssh-mini-agent.
- VM import result imported 4 bridge deliveries, including:
  - `20260428-150414-pnc-generate-dbc-task-for`
  - VM canonical task dir: `/home/mini/.hermes/shared-state/tasks/20260428-150414-pnc-generate-dbc-task-for`
  - VM dispatch pending path: `/home/mini/.hermes/shared-state/dispatch/pending/20260428-150414-pnc-generate-dbc-task-for.json`

Important safety decision:
- Did not run worker claim/execution because VM pending queue contains multiple user/business tasks and worker claim order is sorted over all pending JSON files.
- Forcing `--dispatch-pending --max-dispatch 1` could claim an older unrelated task instead of this smoke task.
- Current safe validation stops at `delivered-to-VM`/VM canonical pending import. A targeted worker claim requires either a task-id filter in the worker or a controlled empty/isolated queue.

Conclusion:
- Host-created and bridge delivery were already verified.
- VM canonical import is now also verified for the PNC smoke task.
- The remaining unverified hop is targeted VM worker claim/execution for this exact task; not run to avoid consuming unrelated pending tasks.


## 12. Targeted VM worker pickup / 2026-04-28 15:36:00 +0800

Implemented on VM worker/control-plane side:
- Added task-id filtering to VM shared-state claim helper:
  - `/home/mini/.hermes/worker-state/shared_state_v2.py`
  - `/home/mini/.openclaw/worker-state/shared_state_v2.py`
  - `claim_pending_batch(..., task_id=...)` now claims only the exact pending JSON when provided.
- Added `--task-id` to VM worker:
  - `/home/mini/.hermes/worker-state/vm_coding_worker_v2.py`
  - Dry-run now reports `task_id_filter` and task-filtered `would_dispatch`.
  - Claim path passes `task_id=args.task_id` into shared-state helper.
- Added first fixed-cli executor extraction for generated PNC goals:
  - Detects `executor: fixed-cli under VM worker`.
  - Resolves `WORKTREE_PATH` from the `worktree_manager.py ensure <user> pnc_specs` line.
  - Executes the two-line command block from the task goal directly via bash instead of routing this fixed-cli task through generic OpenClaw agent prompting.

Backups created on VM:
- `/home/mini/.hermes/worker-state/shared_state_v2.py.bak.20260428T153009`
- `/home/mini/.openclaw/worker-state/shared_state_v2.py.bak.20260428T153009`
- `/home/mini/.hermes/worker-state/vm_coding_worker_v2.py.bak.20260428T153009`
- `/home/mini/.hermes/worker-state/vm_coding_worker_v2.py.bak.20260428T153346`
- `/home/mini/.hermes/worker-state/vm_coding_worker_v2.py.bak.20260428T153415`

Verification:
- `python3 -m py_compile /home/mini/.hermes/worker-state/vm_coding_worker_v2.py /home/mini/.hermes/worker-state/shared_state_v2.py`: pass.
- Dry-run for exact task:
  - command: `/usr/bin/python3 /home/mini/.hermes/worker-state/vm_coding_worker_v2.py --root /home/mini/.hermes/shared-state --worker-root /home/mini/.hermes/worker-state --repo-root /home/mini/minieye_dnp_nop --host-inbox-root /home/mini/tmp/openclaw-shared-state/inbox --dispatch-pending --max-dispatch 1 --task-id 20260428-150414-pnc-generate-dbc-task-for --dry-run --json`
  - result: `task_id_filter` matched and `would_dispatch` contained only `20260428-150414-pnc-generate-dbc-task-for`.
- Focused host tests after VM-side changes:
  - `cd /Users/songying/.hermes/hermes-agent && source venv/bin/activate && python -m pytest tests/tools/test_vm_task_tool.py tests/tools/test_pnc_agent_tools.py -q`
  - result: `19 passed in 0.19s`.
- `git diff --check`: clean.

Real targeted worker execution:
- Before running, the PNC smoke task was intentionally moved back from `dispatch/claimed` to `dispatch/pending` so the task-id filtered claim could exercise the real claim path.
- Executed exact-task worker pickup:
  - `/usr/bin/python3 /home/mini/.hermes/worker-state/vm_coding_worker_v2.py --root /home/mini/.hermes/shared-state --worker-root /home/mini/.hermes/worker-state --repo-root /home/mini/minieye_dnp_nop --host-inbox-root /home/mini/tmp/openclaw-shared-state/inbox --dispatch-pending --max-dispatch 1 --task-id 20260428-150414-pnc-generate-dbc-task-for --json`
- Result state: `failed`, exit code `1`, which is expected for this intentionally harmless smoke because input path was nonexistent / required project metadata was missing.
- Crucially, the canonical worker chain progressed through the previously unverified hop:
  - task left `dispatch/pending`
  - exact task was claimed only by `--task-id`
  - worker executed fixed-cli command on VM
  - canonical task landed in `dispatch/failed`
  - `status.md` updated to terminal state
  - `result.md` was written under `/home/mini/.hermes/shared-state/tasks/20260428-150414-pnc-generate-dbc-task-for/result.md`
  - runner evidence: `/home/mini/.hermes/worker-state/tasks/20260428-150414-pnc-generate-dbc-task-for/artifacts/runner.log`

Runner evidence excerpt:
```text
[2026-04-28T15:35:24] worker command: set -euo pipefail; cd '/home/mini/worktrees/pnc_specs/郭艳彬/pnc_tools_ai_native/32_AI_Native_repo_骨架包_真实首批版_v1'; ./generate-dbc --input '/home/mini/worktrees/pnc_specs/郭艳彬/pnc_tools_ai_native/32_AI_Native_repo_骨架包_真实首批版_v1/nonexistent-smoke.dbc' --output /home/mini/nas/miniPan/tmp/pnc-generate-dbc-smoke-output
status=error
exit_code=1
summary=missing required arguments: project, platform, profile
error_category=invalid_argument
```

Conclusion:
- The full safe smoke chain is now verified through a terminal canonical result for the exact PNC task:
  - host-created: verified
  - bridge delivery / VM-visible inbox: verified
  - VM canonical pending import: verified
  - targeted VM worker pickup: verified
  - VM-side fixed-cli execution: verified
  - canonical terminal result/log proof: verified
- Terminal state is `failed` by expected smoke input semantics, not by chain failure.

Remaining follow-up:
- Clean up / productize the VM-side task-id and fixed-cli executor patch in the actual maintained worker source if these worker-state files are generated from another source.
- Optionally add regression tests for `claim_pending_batch(task_id=...)` and `_extract_fixed_cli_command(...)` in the worker-state source tree.
- Add first-class `vm_task_status` or user-facing status reader if async PNC task status UX becomes the next priority.
