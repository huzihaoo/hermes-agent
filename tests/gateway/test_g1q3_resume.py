"""P3 resume routing: a blocked g1q3 intake must re-run (not dedup-block) when
the originator returns with the data."""
import json

from gateway import run as gateway_run


def test_g1q3_resume_bypasses_dedup_only_for_human_action_states():
    for state in ("blocked", "need_input", "awaiting_user", "BLOCKED", " need_input "):
        assert gateway_run._g1q3_resume_bypasses_dedup({"state": state}) is True
    for state in ("completed", "running", "in_progress", "done", "", "failed", "superseded"):
        assert gateway_run._g1q3_resume_bypasses_dedup({"state": state}) is False
    assert gateway_run._g1q3_resume_bypasses_dedup(None) is False
    assert gateway_run._g1q3_resume_bypasses_dedup({}) is False


def test_find_g1q3_task_by_thread_surfaces_authoritative_blocked_state(tmp_path, monkeypatch):
    # The card's user_state can lag/flap; _find must surface the authoritative
    # shared-state dispatch `state` so resume detection works.
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    task_id = "20260623-164448-g1q3-rca-issue-intake-7025381565"
    task_dir = tmp_path / "runtime" / "shared-state" / "tasks" / task_id
    task_dir.mkdir(parents=True)
    (task_dir / "meta.json").write_text(json.dumps({
        "task_id": task_id,
        "state": "blocked",
        "business_line": "g1q3_rca",
        "source_platform": "feishu",
        "source_thread_id": "topic:om_resume_test",
        # user_state deliberately stale to prove the dispatch state wins.
        "task_card": {"user_state": "in_progress", "thread_id": "topic:om_resume_test"},
    }), encoding="utf-8")

    result = gateway_run._find_g1q3_rca_task_by_thread("feishu", "topic:om_resume_test")
    assert result is not None
    assert result["task_id"] == task_id
    assert result["state"] == "blocked"
    assert gateway_run._g1q3_resume_bypasses_dedup(result) is True


def test_find_g1q3_task_by_thread_completed_task_does_not_resume(tmp_path, monkeypatch):
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    task_id = "20260623-164448-g1q3-rca-issue-intake-7015689036"
    task_dir = tmp_path / "runtime" / "shared-state" / "tasks" / task_id
    task_dir.mkdir(parents=True)
    (task_dir / "meta.json").write_text(json.dumps({
        "task_id": task_id,
        "state": "completed",
        "business_line": "g1q3_rca",
        "source_platform": "feishu",
        "source_thread_id": "topic:om_done_test",
    }), encoding="utf-8")

    result = gateway_run._find_g1q3_rca_task_by_thread("feishu", "topic:om_done_test")
    assert result is not None
    assert result["state"] == "completed"
    assert gateway_run._g1q3_resume_bypasses_dedup(result) is False
