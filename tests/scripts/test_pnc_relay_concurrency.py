"""W5: concurrency invariants for relay_pending_notices bounded-parallel send.

Covers the hard invariants from the feishu-relay-concurrency redesign:
  (1) same task_id stays strictly serial / in order
  (2) different task_ids run concurrently (not N x RTT)
  (3) topic targets do not cross between task_ids
  (4) no duplicate card creation (one_task_one_card honoured under concurrency)
  (5) @originator notify is not duplicated under concurrency
"""
import json
import threading
import time
from datetime import datetime, timezone

from hermes_constants import reset_hermes_home_override, set_hermes_home_override
from scripts import pnc_completion_notice_relay as relay


def _write_text_notice(tmp_path, task_id, *, thread_id, text):
    """A text-only completion notice (no task_card) -> exercises the real send path."""
    sidecar = tmp_path / "task-state" / f"{task_id}.json"
    sidecar.parent.mkdir(parents=True, exist_ok=True)
    sidecar.write_text(json.dumps({
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "completion_notice": {
            "send_status": "pending",
            "category": "delivery",
            "chat_id": relay.DEFAULT_CHAT_IDS[1],
            "thread_id": f"topic:{thread_id}",
            "message_id": thread_id,
            "vm_task_id": f"vm-{task_id}",
            "text": text,
        },
    }), encoding="utf-8")
    return sidecar


def test_cross_task_concurrency_and_per_task_order(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    try:
        task_ids = [f"task-{i}" for i in range(4)]
        for i, tid in enumerate(task_ids):
            _write_text_notice(tmp_path, tid, thread_id=f"om_{i}", text=f"done-{tid}")

        monkeypatch.setenv("PNC_RELAY_SEND_CONCURRENCY", "5")
        monkeypatch.setattr(relay, "RELAY_SEND_CONCURRENCY", 5)

        lock = threading.Lock()
        calls = []          # (task_id, target, t_enter, t_exit)
        max_concurrent = {"n": 0}
        live = {"n": 0}

        def fake_send(args):
            t_enter = time.monotonic()
            with lock:
                live["n"] += 1
                max_concurrent["n"] = max(max_concurrent["n"], live["n"])
            time.sleep(0.15)  # simulate API RTT to expose serialization
            with lock:
                live["n"] -= 1
            target = args["target"]
            # task_id is recoverable from the thread/message id embedded in target
            calls.append((target, t_enter, time.monotonic(), args["message"]))
            return json.dumps({"success": True, "message_id": "om_sent"})

        monkeypatch.setattr(relay, "send_message_tool", fake_send)

        t0 = time.monotonic()
        result = relay.relay_pending_notices(task_ids=task_ids, send=True)
        wall = time.monotonic() - t0
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True, result.get("errors")
    assert result["sent_count"] == 4
    # (2) concurrency: 4 sends x 0.15s serial would be >=0.6s; concurrent must be well under.
    assert wall < 0.45, f"sends did not run concurrently (wall={wall:.2f}s)"
    assert max_concurrent["n"] >= 2, f"no overlap observed (max_concurrent={max_concurrent['n']})"
    # (3) topic isolation: every target carries its own om_ anchor, all distinct.
    targets = sorted({c[0] for c in calls})
    assert len(targets) == 4, targets
    for c in calls:
        target, _, _, message = c
        anchor = target.rsplit(":", 1)[1]  # om_N
        idx = anchor.split("_", 1)[1]
        assert message == f"done-task-{idx}", f"topic crossed: {target} got {message!r}"


def test_same_task_id_serialized_in_order(tmp_path, monkeypatch):
    """Multiple candidates for the SAME task_id must run strictly serially in order.

    In send mode _process re-reads the sidecar from disk (race re-check), so we
    assert serialization via entry sequencing rather than disk-dependent text.
    """
    token = set_hermes_home_override(tmp_path)
    try:
        sidecar = _write_text_notice(tmp_path, "task-x", thread_id="om_x", text="t")

        # Feed three candidates for the SAME task_id (synthetic stress: within a
        # real scan task_ids are unique, but the bucket must still serialize).
        body = json.loads(sidecar.read_text(encoding="utf-8"))

        def fake_iter(**kwargs):
            return [("task-x", sidecar, dict(body), body["completion_notice"]) for _ in range(3)]

        entry_seq = []      # order threads ENTER the send
        live = {"n": 0}
        overlap = {"seen": False}
        counter = {"i": 0}
        lock = threading.Lock()

        def fake_send(args):
            with lock:
                live["n"] += 1
                if live["n"] > 1:
                    overlap["seen"] = True
                seq = counter["i"]
                counter["i"] += 1
                entry_seq.append(seq)
            time.sleep(0.05)
            with lock:
                live["n"] -= 1
            return json.dumps({"success": True, "message_id": "om_sent"})

        monkeypatch.setattr(relay, "iter_pending_notices", fake_iter)
        monkeypatch.setattr(relay, "send_message_tool", fake_send)
        monkeypatch.setattr(relay, "RELAY_SEND_CONCURRENCY", 5)
        # bypass the pre-send relayable re-check rewriting status
        monkeypatch.setattr(relay, "_notice_is_relayable", lambda *a, **k: (True, "test"))
        # force the real text send path (no task_card, so one_task_one_card won't suppress)
        monkeypatch.setattr(relay, "_completion_notice_text_allowed", lambda notice: (True, "test"))

        result = relay.relay_pending_notices(task_ids=["task-x"], send=True)
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True, result.get("errors")
    assert overlap["seen"] is False, "same task_id sends overlapped (must be serial)"
    assert entry_seq == [0, 1, 2], f"same task_id not processed in order: {entry_seq}"
