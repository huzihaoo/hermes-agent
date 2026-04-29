# VM Task Efficiency PLD

> **For Hermes:** Use subagent-driven-development only after the first slice is accepted. This PLD is grounded in live VM evidence, local QCon material, and mature queue/worker patterns.

**Goal:** Reduce VM-side delegated task waiting time and wasted worker cycles while preserving shared-state v2 correctness, Feishu topic routing, and VM/host isolation.

**Architecture:** Keep shared-state v2 as execution truth. Keep host relay as the only Feishu sender. Upgrade VM worker from “periodically run the whole wrapper” to “cheap idle detection + event-aware wakeup + bounded execution lanes”. Use leases, heartbeat, stale reaper, and metrics as safety rails.

**Current status:** P0 cron pickup delay has already been reduced by controlled daemon cutover. Current bottleneck is no longer 60s cron latency. It is expensive idle polling: the daemon wakes every 5s, but each empty wrapper run takes about 7.5s and emits about 125KB stdout, so real idle cycle is roughly 12.5s.

---

## 1. Live runtime evidence

Collected 2026-04-28 21:09 local time via `~/.local/bin/ssh-mini-agent run_py_json`.

Current VM worker:

```text
unit: hermes-vm-coding-worker-daemon.service
state: active/running
pid: 387894
exec: /usr/bin/python3 /home/mini/.hermes/worker-state/vm_coding_worker_daemon.py --interval-seconds 5 --max-dispatch 1 --log-path /home/mini/.hermes/workspace-coding/vm_coding_worker.daemon.log
```

Cron worker lines are disabled with `daemon-cutover-disabled`. The earlier 60s cron pickup path is not the active owner anymore.

Recent daemon entries:

```json
{
  "state": "ran",
  "elapsed_seconds": 7.53,
  "returncode": 0,
  "claimed_count": 0,
  "pending_before_count": 0,
  "dispatched_count": 0,
  "stdout_bytes": 125453,
  "stderr_bytes": 0
}
```

Dispatch health:

```json
{
  "pending": 0,
  "claimed": 0,
  "running": 0,
  "done": 68,
  "failed": 8,
  "dead": 0,
  "stale_claimed_count": 0,
  "mutates_state": false
}
```

Reaper watch:

```text
unit: hermes-vm-dispatch-reaper-watch.timer
period: 5 minutes
mode: dry-run
latest watch logs: apply=false, moved_count=0, candidate_count=0
```

Important code fact:

`/home/mini/.hermes/worker-state/vm_coding_worker_v2.py` does this on every wrapper run:

1. `import_bridge_deliveries(...)`
2. `import_legacy_handoff(...)`
3. list `dispatch/pending`
4. if not dry-run, claim up to `max_dispatch`
5. execute claims sequentially
6. append full `read_status(...)` into stdout

That is why empty polling is expensive. It is doing import + status serialization every time even when there is no work.

## 2. QCon design principles mapped to Hermes

### Aether unified elastic scheduling

Source:
`knowledge/raw/external/qcon-beijing-202604/2026-04-21-qcon-beijing-202604-面向-ai-原生负载的统一弹性调度体系-aether-架构实践-e30dfd31-source.md`

Relevant lines:
- 205-235: workflow orchestration, state machine, rule management, resource prediction, task coordinator, conflict arbitration, priority queue plugin, resource reservation, load-aware plugin, metrics collection, process monitoring, SLA rule engine.
- 277-311: scheduler plugin, priority queue plugin, resource reservation plugin, load-aware plugin, enqueue/select job/sort+score/bind/deploy.
- 334-351: job history service, optimizer, planner, scheduler, process monitoring, metric collection.

Mapping:

Hermes does not need a full Aether. It does need the same separation:

```text
enqueue/import -> score/sort -> reserve/claim -> execute -> heartbeat -> result -> metrics/history
```

Current system has pieces, but they are fused inside `vm_coding_worker_v2.py` and the wrapper. That makes empty checks expensive and makes queue position/SLA hard to explain to users.

### DeepResearch x OpenClaw architecture

Source:
`knowledge/raw/external/qcon-beijing-202604/2026-04-21-qcon-beijing-202604-deepresearch-x-openclaw-实现路径与-token-成本控制-e40c91bc-source.md`

Relevant lines:
- 248-263: chat adapters, gateway, session manager, lane queue, skill selector, memory system, trigger/heartbeat, local tools.
- 273-275: 24/7 execution has token cost 100x to 1000x compared with chat.

Mapping:

VM delegated tasks are “action execution”, not chat. Idle wakeups matter because 24/7 loops multiply waste. The lane queue already exists conceptually in host admission. VM side needs a matching execution model, not a blind single serial loop.

### AI diagnosis engineering

Source:
`knowledge/raw/external/qcon-beijing-202604/2026-04-21-qcon-beijing-202604-从拉群救火到ai排查-端到端智能诊断体系的工程实践-59c8ff13-source.md`

Relevant lines:
- 245-255: wrong diagnosis is worse than no diagnosis; need automated evaluation and experiment release.
- 263-283: three-layer architecture: knowledge routing, coding-agent deep diagnosis, evaluation/experiment release base.
- 287-310: progressive disclosure, route maps, AI + human dual-track knowledge updates.

Mapping:

Do not declare “VM is fast now” from one smoke. Track end-to-end stages and compare before/after. The diagnosis has to be stage-based:

```text
host-created -> delivered-to-VM -> picked-up -> running -> completed -> host-imported -> Feishu-reported
```

## 3. Mature queue/worker mechanisms worth borrowing

GitHub API was anonymous-rate-limited during inspection, so I used public raw READMEs/docs. Stable mechanisms:

### Celery

Takeaway:
- workers are long-running daemons
- queues can be routed by task type
- multiple workers can run with explicit concurrency
- routing controls which worker consumes which queue

Hermes mapping:
- standard/heavy/fast lanes should map to worker capability, not just host admission labels.
- VM worker should not pre-claim a batch it cannot execute concurrently.

### RQ

Takeaway:
- simple Redis-backed job queue
- explicit queue + background workers

Hermes mapping:
- keep simple file-backed queue for now, but expose queue state like a real job queue: waiting, claimed, running, done, failed, dead.

### Dramatiq

Takeaway:
- workers process messages concurrently through worker threads
- retry/backoff is part of normal worker behavior

Hermes mapping:
- retry should not be an ad-hoc manual requeue. It needs attempt count, next_retry_at, and reason.

### Sidekiq

Takeaway:
- high throughput comes from threads and a small Redis protocol surface
- benchmarks care about job drain speed and network/JSON overhead

Hermes mapping:
- current empty poll emits 125KB JSON. That is silly. Separate compact poll from full status read.

### BullMQ

Takeaway:
- failed jobs move to failed set
- retries respect priority when moved back to waiting
- stalled jobs are detected and eventually failed/retried
- workers emit completed/progress/failed events
- rate limiting can keep jobs waiting rather than over-claiming

Hermes mapping:
- reaper should graduate from watch-only to policy-backed requeue/archive after enough confidence.
- progress events should be first-class, not inferred from giant status snapshots.
- if a lane is rate-limited, leave tasks pending and visible, do not hide them in claimed.

### Temporal

Takeaway:
- workflows survive intermittent failures and retry activities
- workflow history is the durable truth

Hermes mapping:
- do not try to turn Hermes into Temporal. Borrow the history idea: every task run should have a small event log: imported, claimed, heartbeat, completed, failed, requeued.

### Faktory

Takeaway:
- jobs are JSON hashes
- jobs are fetched from queues
- jobs are reserved with a timeout, default 30 min

Hermes mapping:
- lease/heartbeat already exist. Make them the formal reservation contract and show them in health.

### Huey

Takeaway:
- supports multiple execution models, scheduling, retry, prioritization, result storage, expiration

Hermes mapping:
- the missing primitives are not exotic: priority, retry, result storage, expiration, worker mode.

### GitHub Actions Runner Controller

Takeaway:
- scales self-hosted runners based on queued work

Hermes mapping:
- for now, single VM means no autoscaling. But the same idea applies locally: idle loop should be cheap, active loop should be responsive, and future worker count should be tied to queue depth/lane.

## 4. Diagnosis

### What has been fixed

1. Cron wait was removed from the hot path.
2. User systemd daemon owns pickup.
3. Stale claimed legacy files were archived.
4. Dry-run reaper watch is installed and verified safe.
5. Health probe is read-only and returns current queue counts.

### What is still inefficient

1. Empty polling is expensive.
   - daemon interval is 5s
   - wrapper empty run takes about 7.5s
   - effective idle loop is about 12.5s
   - each empty run emits about 125KB stdout

2. Worker execution is still serial.
   - `--max-dispatch 1` is correct today because `vm_coding_worker_v2.py` claims a batch before sequential execution.
   - raising max_dispatch now would create hidden claimed backlog behind the first long-running task.

3. Import, claim, execute, and full status read are fused.
   - cannot cheaply ask “is there work?”
   - cannot expose queue wait clearly
   - cannot separately tune host bridge import frequency and execution frequency

4. Admission lanes do not yet own VM execution lanes.
   - host admission has lane concepts: `fast`, `standard`, `heavy`
   - VM dispatch still behaves like one serial queue

## 5. Proposed target architecture

```text
Host / Feishu
  -> shared-state bridge inbox
  -> VM importer, cheap and frequent
  -> VM dispatch queue
  -> scheduler/scorer
  -> lane-aware reservation
  -> worker executor(s)
  -> heartbeat/progress/result events
  -> host import
  -> Feishu topic relay
```

Keep the components small. No big-bang rewrite.

### Queue state contract

Use these states consistently:

```text
pending      visible, not reserved
claimed      reserved by worker, not yet subprocess-running
running      subprocess/agent actually running
done         completed
failed       terminal failure
retry_wait   scheduled retry, still visible
dead         exceeded retry/lease policy
archived     legacy/manual cleanup only
```

If `retry_wait` is too much for the first slice, keep retry as metadata on pending, but do not hide retrying jobs in claimed.

### Metrics contract

Minimum metrics:

```text
queue_depth{lane,state}
pickup_latency_seconds p50/p95/p99
empty_poll_duration_seconds p50/p95
wrapper_stdout_bytes p95
claimed_age_seconds max
running_age_seconds max
task_completion_latency_seconds p50/p95
requeue_count{reason}
archive_count{reason}
feishu_report_latency_seconds p95
```

User-facing latency should be split:

```text
intake_ack_latency
queue_wait_latency
vm_pickup_latency
execution_latency
result_relay_latency
```

This prevents burying “waiting for VM” inside generic completion time.

## 6. Implementation plan

### Task 1: Add cheap pending/import probe

Objective:
Avoid full wrapper execution when there is no bridge inbox work and no dispatch pending work.

Files:
- Modify VM: `/home/mini/.hermes/worker-state/vm_coding_worker_daemon.py`
- Create VM test: `/home/mini/.hermes/worker-state/test_vm_coding_worker_daemon_idle_probe.py`

Design:

Add a read-only function:

```python
def cheap_work_available(canonical_root: Path, bridge_root: Path) -> dict[str, Any]:
    pending_count = count_json(canonical_root / 'dispatch' / 'pending')
    inbox_count = count_json(bridge_root / 'inbox')
    legacy_count = count_json(resolve_legacy_handoff_root() / 'inbox')
    return {
        'pending_count': pending_count,
        'bridge_inbox_count': inbox_count,
        'legacy_inbox_count': legacy_count,
        'work_available': pending_count > 0 or bridge_inbox_count > 0 or legacy_count > 0,
    }
```

Important:
- If any inbox has files, run the wrapper, because wrapper owns import.
- If dispatch/pending has files, run the wrapper.
- If all zero, skip wrapper and log `state=idle_no_work`.
- Never inspect Feishu/chat metadata here.

Expected win:
- idle loop drops from ~7.5s CPU/wall wrapper call to a few filesystem stats.
- actual new work pickup remains <= interval because bridge inbox files wake the wrapper path.

Verification:

```bash
cd /home/mini/.hermes/worker-state
python3 -m unittest test_vm_coding_worker_daemon.py test_vm_coding_worker_daemon_idle_probe.py -v
python3 vm_coding_worker_daemon.py --max-runs 2 --interval-seconds 1 --log-path /tmp/vm-daemon-idle-smoke.log
 tail -5 /tmp/vm-daemon-idle-smoke.log
```

Live-adjacent smoke:
- daemon active
- no pending tasks
- new logs show `idle_no_work`
- health remains `pending=0 claimed=0 running=0`

Rollback:
- restore previous daemon file from backup
- `systemctl --user restart hermes-vm-coding-worker-daemon.service`

### Task 2: Split compact status from full status

Objective:
Stop printing full `read_status(...)` on every successful empty run.

Files:
- VM: `/home/mini/.hermes/worker-state/vm_coding_worker_v2.py`
- VM tests around JSON output parsing

Design:

Add CLI flag:

```text
--compact-status
```

When enabled:

```python
payload['status_summary'] = {
    'counts': count_dispatch(root),
    'stale_claimed_count': ...,
}
```

Do not include full `payload['status']` unless:
- a task was dispatched
- wrapper failed
- user passed `--full-status`

Expected win:
- stdout drops from ~125KB to sub-2KB for empty/successful runs.
- daemon logs remain readable.

Verification:

```bash
python3 vm_coding_worker_v2.py --dispatch-pending --max-dispatch 1 --once --json --compact-status > /tmp/compact.json
python3 - <<'PY'
import json, pathlib
p = pathlib.Path('/tmp/compact.json')
o = json.loads(p.read_text())
assert 'status' not in o or o['claimed_task_ids']
assert p.stat().st_size < 4096
PY
```

### Task 3: Add event log per task/run

Objective:
Make task state transitions auditable without full status dumps.

Files:
- VM shared-state helper: `/home/mini/.hermes/worker-state/shared_state_v2.py`
- Host shared-state helper mirror if applicable: `/Users/songying/.hermes/workspace-work/bin/shared_state_v2.py`

Events:

```json
{"event":"imported","at":"...","task_id":"..."}
{"event":"claimed","at":"...","worker":"mini-desktop","lease_until":"..."}
{"event":"running","at":"...","pid":123}
{"event":"heartbeat","at":"..."}
{"event":"completed","at":"...","exit_code":0}
{"event":"failed","at":"...","exit_code":1,"summary":"..."}
{"event":"requeued","at":"...","reason":"lease_expired"}
```

Storage:

```text
/home/mini/.hermes/shared-state/tasks/<task_id>/events.jsonl
```

Verification:
- submit one smoke task
- events show imported -> claimed -> running -> completed
- host import sees completed
- Feishu topic relay remains scoped by meta_json

### Task 4: Promote reaper from watch-only to guarded policy

Objective:
Automatically requeue safe stale tasks and archive legacy malformed claims.

Prerequisite:
- at least 24h of dry-run watch with zero false positives
- event log exists, so reaper can distinguish live running from stale reserved

Policy:

```text
missing_runtime_timestamps -> archive only if older than 24h and no running pid
expired lease + no heartbeat -> requeue
running age > hard limit -> dead or approval-required requeue
```

Safety:
- default remains dry-run
- apply mode behind explicit systemd unit or config flag
- every move writes reaper-run record

### Task 5: Introduce lane-aware execution

Objective:
Map host admission lanes to VM execution behavior.

First cut:

```text
fast: small metadata/status tasks, concurrency 1
standard: normal coding tasks, concurrency 1
heavy: long coding/data tasks, concurrency 1, exclusive lock
owner-priority: optional, pre-sort only, no preemption
```

Do not add true parallel coding-agent execution until:
- workspace isolation is guaranteed per run
- exec approval socket/file behavior is safe per run
- result relay is idempotent
- event log can show concurrent runs cleanly

### Task 6: Add queue transparency to user feedback

Objective:
Users should not feel like “VM is slow and silent”.

Expose:

```text
已接收
已送达 VM
VM 已接手
正在执行
预计等待：<band>
队列位置：<optional>
```

Avoid raw internals in Feishu. Humanized version:

```text
任务已经到 VM 了，当前没有排队，正在等执行器接手。正常会在十几秒内开始。
```

## 7. Risks

1. Fast idle probe can miss work if it checks the wrong inbox path.
   - Mitigation: only skip wrapper when canonical pending, bridge inbox, and legacy inbox are all empty.

2. Raising `--max-dispatch` too early hides tasks in claimed.
   - Mitigation: keep max_dispatch=1 until true worker pool exists.

3. Reaper apply mode can move live tasks if heartbeat semantics are weak.
   - Mitigation: dry-run first, require event log, require lease expiry + missing heartbeat.

4. Parallel workers can corrupt shared workspace.
   - Mitigation: lane-level concurrency stays 1 until per-run workspace isolation is done.

## 8. Recommended execution order

1. Ship Task 1, cheap idle probe.
2. Ship Task 2, compact status output.
3. Run 2-4h live watch, compare idle loop cost and pickup latency.
4. Ship Task 3, event log.
5. Promote reaper guarded apply only after event log exists.
6. Add lane-aware scheduling, still serial execution.
7. Only then evaluate true parallel worker pool.

## 9. Success criteria

P1 success:

```text
empty poll wall time p95 < 200ms
empty poll stdout p95 < 2KB
new task pickup p95 < 8s
stale_claimed_count = 0
Feishu result misroute = 0
```

P2 success:

```text
queue wait visible for >95% delegated tasks
host-created -> VM-picked-up p95 < 10s
completed -> Feishu-reported p95 < 15s
reaper false move = 0
worker idle CPU materially lower than current baseline
```

## 10. Notes on current local repo state

Local repo `/Users/songying/.hermes/hermes-agent` is on `overlay/stable` with unrelated modified files:

```text
 M gateway/platforms/feishu.py
 M run_agent.py
 M tests/gateway/test_feishu_approval_buttons.py
 M tests/tools/test_pnc_agent_tools.py
 M tests/tools/test_send_message_tool.py
 M tools/pnc_agent_tools.py
 M tools/send_message_tool.py
?? run_agent.py.bak-hermes-test-20260428-201245
```

Do not mix VM efficiency changes into those host modifications unless the task explicitly requires it.

## 11. Immediate next action

Implement Task 1 on VM with TDD:

- add idle probe tests
- add cheap `idle_no_work` path to daemon
- restart daemon
- verify logs show idle skips and no queue mutation
- update knowledge with the measured before/after

This is the smallest high-value next slice. It attacks the actual current bottleneck and does not change task execution semantics.
