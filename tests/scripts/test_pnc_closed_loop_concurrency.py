"""A4: production-grade concurrency stress for the closed-loop fault router.

Goal restated by the user: production-grade concurrency where ALL tasks stably
progress to closed-loop. This drives many g1q3-rca tasks of mixed fault classes
through the routing decision CONCURRENTLY and asserts:

  * every infra/self-healable task is routed away from the originator (to
    self-heal/ops) — never an orphan @-ping;
  * every genuine needs-human task with a resolvable originator would @-ping;
  * an unresolvable originator is guarded (no orphan card), never self-talk;
  * routing is deterministic — concurrent results equal serial results, with no
    cross-task contamination and zero exceptions under load.

The router is built from pure classification + per-task on-disk state keyed by
task_id, so it is inherently concurrency-safe; this pins that property.
"""
import json
from concurrent.futures import ThreadPoolExecutor
import contextvars

from scripts import pnc_completion_notice_relay as relay
from test_pnc_completion_notice_relay import set_hermes_home_override, reset_hermes_home_override


def _make_task(home, idx, *, kind, fault_class, state, with_governance):
    task_id = f"20260626-1120{idx:02d}-g1q3-rca-issue-intake-70284{idx:03d}-_{idx:03d}c"
    slug = f"g1q3_rca_issue_intake_70284{idx:03d}_{idx:03d}c"
    shared = home / "runtime" / "shared-state" / "tasks" / task_id
    shared.mkdir(parents=True, exist_ok=True)
    (shared / "meta.json").write_text(json.dumps({
        "state": state,
        "business_line": "g1q3_rca",
        "created_at": "2026-06-26T11:21:55+08:00",
        "updated_at": "2026-06-26T11:27:48+08:00",
        "artifact_root": f"/mnt/tmp/{slug}/",
    }), encoding="utf-8")
    (shared / "result.md").write_text(json.dumps({
        "business_result": {
            "gate_decision": "ready_to_download",
            "status": "need_evidence",
            "terminal_state": "need_download",
            "blocker": {"kind": kind, "fault_class": fault_class, "retryable": fault_class == "infra_self_healable"},
        },
    }), encoding="utf-8")
    if with_governance:
        gov = home / "pnc_agent" / "governance_rca"
        gov.mkdir(parents=True, exist_ok=True)
        (gov / f"{slug}.json").write_text(json.dumps({"user_id": "ou_d1d3cfeba1be0a22faa36aaf4fb3907d"}), encoding="utf-8")
    body = {"task_card": {
        "task_id": task_id, "user_state": "in_progress",
        "delivery": {"report_status": "need_download"},
        "chat_id": "oc_grp", "message_id": f"om_anchor_{idx}",
    }}
    return task_id, body


def _decide(task_id, body):
    meta = relay._load_shared_state_meta(task_id)
    return relay.maybe_notify_originator(task_id=task_id, path=None, body=json.loads(json.dumps(body)), meta=meta, send=False)


def test_concurrent_mixed_fault_routing_is_stable_and_deterministic(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        tasks = []
        expected = {}
        n = 20
        for i in range(n):
            # infra
            tid, body = _make_task(tmp_path, i, kind="translate_workdir_permission",
                                    fault_class="infra_self_healable", state="blocked", with_governance=False)
            tasks.append((tid, body)); expected[tid] = ("skipped", "infra_self_healable_no_originator_ping")
            # human, resolvable originator
            tid, body = _make_task(tmp_path, 100 + i, kind="need_source_or_evidence",
                                   fault_class="needs_human_input", state="need_input", with_governance=True)
            tasks.append((tid, body)); expected[tid] = ("ping", None)
            # human, UNresolvable originator
            tid, body = _make_task(tmp_path, 200 + i, kind="need_source_or_evidence",
                                   fault_class="needs_human_input", state="need_input", with_governance=False)
            tasks.append((tid, body)); expected[tid] = ("skipped", "originator_unresolved")

        # serial baseline
        serial = {tid: _decide(tid, body) for tid, body in tasks}
        # concurrent run. The HERMES_HOME override is a ContextVar, which
        # ThreadPoolExecutor does NOT propagate to worker threads by default;
        # copy_context() in the main thread captures it so each worker resolves
        # the same (test) home. In production HERMES_HOME is an env/default
        # (thread-inherited), so the routing reads — all keyed per task_id file —
        # are concurrency-safe without this dance.
        def _submit(ex, tid, body):
            ctx = contextvars.copy_context()
            return ex.submit(ctx.run, _decide, tid, body)
        with ThreadPoolExecutor(max_workers=16) as ex:
            futures = {tid: _submit(ex, tid, body) for tid, body in tasks}
            concurrent = {tid: f.result() for tid, f in futures.items()}

        assert len(tasks) == 3 * n
        # determinism: concurrent == serial for every task
        assert concurrent == serial
        # correctness per lane
        for tid, (lane, reason) in expected.items():
            res = concurrent[tid]
            if lane == "skipped":
                assert res.get("skipped") is True, (tid, res)
                assert res.get("reason") == reason, (tid, res)
            else:  # ping -> dry_run with a resolved originator mention
                assert res.get("dry_run") is True, (tid, res)
                assert res.get("has_mention") is True, (tid, res)
        # no infra task ever produced a mention / orphan card
        for tid, (lane, _r) in expected.items():
            if expected[tid][1] == "infra_self_healable_no_originator_ping":
                assert "has_mention" not in concurrent[tid] or concurrent[tid].get("has_mention") is not True
    finally:
        reset_hermes_home_override(token)
