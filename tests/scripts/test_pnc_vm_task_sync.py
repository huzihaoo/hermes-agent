import json
from pathlib import Path

import pytest

from gateway.tasks.store import TaskStore
from gateway.tasks.types import Task, TaskStatus, TaskType
from hermes_constants import reset_hermes_home_override, set_hermes_home_override
from scripts import pnc_completion_notice_relay, pnc_foxglove_delivery, pnc_vm_task_sync


def _task(task_id="task-1", *, chat_id=pnc_vm_task_sync.G1Q3_RCA_CHAT_ID, status=TaskStatus.RUNNING, vm_task_id=None):
    return Task(
        task_id=task_id,
        status=status,
        task_type=TaskType.CHAT,
        user_id="user-1",
        platform="feishu",
        request_summary="summary",
        started_at=1000.0,
        agent_route="g1q3-rca",
        chat_id=chat_id,
        chat_type="group",
        vm_task_id=vm_task_id,
    )


def test_iter_candidate_tasks_filters_to_pnc_feishu_vm_tasks(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        store = TaskStore(tmp_path / "analytics" / "tasks.db")
        store.upsert(_task("wanted", vm_task_id="vm-wanted"))
        store.upsert(_task("terminal", status=TaskStatus.COMPLETED, vm_task_id="vm-terminal"))
        store.upsert(_task("other-chat", chat_id="oc_other", vm_task_id="vm-other"))
        store.upsert(Task(
            task_id="cli-task",
            status=TaskStatus.RUNNING,
            task_type=TaskType.CHAT,
            user_id="user-1",
            platform="cli",
            request_summary="summary",
            started_at=1000.0,
            vm_task_id="vm-cli",
        ))

        candidates = pnc_vm_task_sync.iter_candidate_tasks(store=store)
    finally:
        reset_hermes_home_override(token)

    assert [task.task_id for task in candidates] == ["wanted"]


def test_sync_pnc_vm_tasks_collects_and_writes_sidecar(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    try:
        store = TaskStore(tmp_path / "analytics" / "tasks.db")
        store.upsert(_task("task-1", vm_task_id="vm-1"))
        calls = []

        def fake_collect(task_id, **kwargs):
            calls.append((task_id, kwargs))
            return {
                "state": {"value": "completed", "summary": "done", "terminal": True},
                "artifacts": ["artifact-a"],
                "vm_bridge": {
                    "summary": "done",
                    "state": "completed",
                    "work_tmp_dir": "/mnt/tmp/vm-1/",
                    "user_visible_path": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/vm-1/",
                },
                "errors": [],
            }

        monkeypatch.setattr(pnc_vm_task_sync, "collect_vm_task_status", fake_collect)

        result = pnc_vm_task_sync.sync_pnc_vm_tasks(limit=10)
        body = json.loads((tmp_path / "task-state" / "task-1.json").read_text(encoding="utf-8"))
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True
    assert result["synced_count"] == 1
    assert calls == [("vm-1", {"include_artifacts": True})]
    assert body["current_phase"] == "completed"
    assert body["vm_bridge"]["vm_task_id"] == "task-1"
    assert body["vm_bridge"]["summary"] == "done"
    assert body["artifacts"] == ["artifact-a"]
    assert body["completion_notice"]["send_status"] == "pending"
    assert body["completion_notice"]["state"] == "completed"
    assert "结论：done" in body["completion_notice"]["text"]
    assert "路径：//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/vm-1/" in body["completion_notice"]["text"]
    assert "边界：" in body["completion_notice"]["text"]
    assert "下一步：" in body["completion_notice"]["text"]


def test_sync_forwards_explicit_isolated_vm_roots(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    try:
        store = TaskStore(tmp_path / "analytics" / "tasks.db")
        store.upsert(_task("task-isolated", vm_task_id="vm-isolated"))
        calls = []

        def fake_collect(task_id, **kwargs):
            calls.append((task_id, kwargs))
            return {
                "state": {"value": "running", "summary": "中文 smoke", "terminal": False},
                "artifacts": [],
                "vm_bridge": {"summary": "中文 smoke", "state": "running"},
                "errors": [],
            }

        monkeypatch.setattr(pnc_vm_task_sync, "collect_vm_task_status", fake_collect)
        result = pnc_vm_task_sync.sync_pnc_vm_tasks(
            limit=10,
            shared_state_root="/mnt/tmp/hermes-v0182-smoke-test/shared-state",
            ssh_mini_agent="/candidate/ssh-mini-agent",
        )
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True
    assert calls == [
        (
            "vm-isolated",
            {
                "include_artifacts": True,
                "shared_state_root": "/mnt/tmp/hermes-v0182-smoke-test/shared-state",
                "ssh_mini_agent": "/candidate/ssh-mini-agent",
            },
        )
    ]


def test_main_rejects_lock_root_outside_hermes_home(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        with pytest.raises(SystemExit):
            pnc_vm_task_sync.main(["--dry-run", "--lock-root", str(tmp_path.parent / "outside")])
    finally:
        reset_hermes_home_override(token)


def test_main_forwards_isolated_roots_with_candidate_lock(tmp_path, monkeypatch, capsys):
    token = set_hermes_home_override(tmp_path)
    captured = []
    try:
        def fake_sync(**kwargs):
            captured.append(kwargs)
            return {
                "ok": True,
                "candidate_count": 0,
                "synced_count": 0,
                "dry_run": True,
                "rows": [],
                "errors": [],
            }

        monkeypatch.setattr(pnc_vm_task_sync, "sync_pnc_vm_tasks", fake_sync)
        rc = pnc_vm_task_sync.main([
            "--dry-run",
            "--json",
            "--lock-root",
            str(tmp_path / "smoke-locks"),
            "--shared-state-root",
            "/mnt/tmp/hermes-v0182-smoke-test/shared-state",
            "--ssh-mini-agent",
            "/candidate/ssh-mini-agent",
        ])
        output = json.loads(capsys.readouterr().out)
    finally:
        reset_hermes_home_override(token)

    assert rc == 0
    assert output["ok"] is True
    assert captured == [{
        "limit": 50,
        "chat_ids": pnc_vm_task_sync.DEFAULT_CHAT_IDS,
        "include_terminal": False,
        "dry_run": True,
        "no_artifacts": False,
        "shared_state_root": "/mnt/tmp/hermes-v0182-smoke-test/shared-state",
        "ssh_mini_agent": "/candidate/ssh-mini-agent",
    }]
    assert (tmp_path / "smoke-locks" / "pnc-vm-task-sync.lock").exists()


def test_pipeline_state_progress_accepts_real_vm_dict_stages_shape():
    pipeline_state = {
        "stages": {
            "s1_gate": {"status": "completed", "updated_at": "2026-07-09T16:00:00+08:00"},
            "s2_download": {"status": "completed", "updated_at": "2026-07-09T16:01:00+08:00"},
            "s5_alignment": {"status": "running", "updated_at": "2026-07-09T16:05:00+08:00"},
            "s6_report": {"status": "pending"},
        }
    }

    progress = pnc_vm_task_sync._progress_from_pipeline_state(pipeline_state)

    assert progress["status"] == "running"
    assert progress["stage"] == "s5_alignment"
    assert progress["stage_label"] == "帧对齐中"
    assert progress["phase"] == "s5_alignment"
    assert progress["message"] == "帧对齐中"
    assert progress["ts"] == "2026-07-09T16:05:00+08:00"


def test_completion_notice_uses_g1q3_l1_notice(tmp_path, monkeypatch):
    task = _task("task-1", vm_task_id="vm-1")
    task.thread_id = "topic:om_1"
    task.message_id = "om_1"
    monkeypatch.setattr(
        pnc_vm_task_sync.vm_task_completion_probe,
        "_g1q3_l1_notice",
        lambda vm_task_id, task_dir, state: "飞书问题 6986500860 RCA 检查完成：\n\nL1",
    )

    notice = pnc_vm_task_sync._completion_notice_for_task(task, {
        "state": {"value": "completed", "terminal": True, "summary": "done"},
    })

    assert notice is not None
    assert notice["source"] == "g1q3_l1_notice"
    assert notice["text"].startswith("飞书问题 6986500860 RCA 检查完成")
    assert notice["thread_id"] == "topic:om_1"
    assert notice["message_id"] == "om_1"


def test_main_dry_run_outputs_json(tmp_path, capsys):
    token = set_hermes_home_override(tmp_path)
    try:
        store = TaskStore(tmp_path / "analytics" / "tasks.db")
        store.upsert(_task("task-1", vm_task_id="vm-1"))
        rc = pnc_vm_task_sync.main(["--dry-run", "--json"])
        out = json.loads(capsys.readouterr().out)
    finally:
        reset_hermes_home_override(token)

    assert rc == 0
    assert out["dry_run"] is True
    assert out["candidate_count"] == 1
    assert out["rows"][0]["vm_task_id"] == "vm-1"


def test_sync_pnc_vm_tasks_updates_taskstore_terminal_status(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    try:
        store = TaskStore(tmp_path / "analytics" / "tasks.db")
        store.upsert(_task("task-terminal", status=TaskStatus.RUNNING, vm_task_id="vm-terminal"))

        def fake_collect(task_id, **kwargs):
            return {
                "updated_at": "2026-06-10T00:00:00+00:00",
                "state": {"value": "completed", "summary": "done", "terminal": True},
                "artifacts": [],
                "vm_bridge": {"summary": "done", "state": "completed"},
                "errors": [],
            }

        monkeypatch.setattr(pnc_vm_task_sync, "collect_vm_task_status", fake_collect)
        result = pnc_vm_task_sync.sync_pnc_vm_tasks(limit=10)
        updated = store.get("task-terminal")
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True
    assert result["rows"][0]["taskstore_status_synced"] == "completed"
    assert updated is not None
    assert updated.status == TaskStatus.COMPLETED
    assert updated.completed_at is not None


def test_sync_pnc_vm_tasks_does_not_overwrite_existing_terminal_taskstore_status(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    try:
        store = TaskStore(tmp_path / "analytics" / "tasks.db")
        store.upsert(_task("task-terminal", status=TaskStatus.FAILED, vm_task_id="vm-terminal"))

        monkeypatch.setattr(pnc_vm_task_sync, "collect_vm_task_status", lambda task_id, **kwargs: {
            "state": {"value": "completed", "summary": "done", "terminal": True},
            "artifacts": [],
            "vm_bridge": {"summary": "done", "state": "completed"},
            "errors": [],
        })
        result = pnc_vm_task_sync.sync_pnc_vm_tasks(limit=10, include_terminal=True)
        updated = store.get("task-terminal")
    finally:
        reset_hermes_home_override(token)

    assert result["rows"][0]["taskstore_status_synced"] is None
    assert updated is not None
    assert updated.status == TaskStatus.FAILED


def test_sync_pnc_vm_tasks_writes_task_card_compatible_object(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    try:
        store = TaskStore(tmp_path / "analytics" / "tasks.db")
        store.upsert(_task("task-card", vm_task_id="vm-card"))
        monkeypatch.setattr(pnc_vm_task_sync, "collect_vm_task_status", lambda task_id, **kwargs: {
            "state": {"value": "completed", "summary": "done", "terminal": True},
            "artifacts": ["//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/vm-card/report.md"],
            "vm_bridge": {"summary": "done", "state": "completed"},
            "errors": [],
        })

        result = pnc_vm_task_sync.sync_pnc_vm_tasks(limit=10)
        body = json.loads((tmp_path / "task-state" / "task-card.json").read_text(encoding="utf-8"))
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True
    assert "task_card" not in body
    proposal = body["vm_delivery_proposal"]
    assert proposal["source"] == "pnc_vm_task_sync"
    assert proposal["user_state"] == "done"
    assert proposal["delivery"]["conclusion"] == "done"
    assert proposal["delivery"]["artifact_path"].startswith("//hfs1.minieye.tech/")
    assert proposal["status_line"]

    relayed = pnc_completion_notice_relay.reconcile_vm_delivery_proposal("task-card", body)
    assert relayed["task_card"]["user_state"] == "done"
    assert relayed["task_card"]["delivery"]["conclusion"] == "done"
    assert relayed["task_card"]["delivery"]["artifact_path"].startswith("//hfs1.minieye.tech/")
    assert relayed["task_card"]["status_line"]
    assert "last_sent_hash" not in relayed["task_card"]



def test_sync_pnc_vm_tasks_preserves_one_card_intake_envelope(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    try:
        store = TaskStore(tmp_path / "analytics" / "tasks.db")
        store.upsert(_task("task-card-preserve", vm_task_id="vm-card-preserve"))
        sidecar = tmp_path / "task-state" / "task-card-preserve.json"
        sidecar.parent.mkdir(parents=True, exist_ok=True)
        sidecar.write_text(json.dumps({
            "task_card": {
                "schema_version": 1,
                "task_id": "task-card-preserve",
                "card_message_id": "om_card",
                "last_sent_hash": "old_hash",
                "last_update_ts": "2026-06-12T00:00:00+00:00",
                "one_card_policy": True,
                "scope_line": "群里只返回简要结论",
                "milestones": [{"ts": "2026-06-12T00:00:00+00:00", "label": "任务建好"}],
                "delivery": {"boundaries": ["主控 Meegle 登录已过期/未授权"]},
            }
        }), encoding="utf-8")
        monkeypatch.setattr(pnc_vm_task_sync, "collect_vm_task_status", lambda task_id, **kwargs: {
            "state": {"value": "completed", "summary": "done", "terminal": True},
            "artifacts": ["//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/vm-card-preserve/report.md"],
            "vm_bridge": {"summary": "done", "state": "completed"},
            "errors": [],
        })

        result = pnc_vm_task_sync.sync_pnc_vm_tasks(limit=10)
        body = json.loads(sidecar.read_text(encoding="utf-8"))
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True
    card = body["task_card"]
    assert card["card_message_id"] == "om_card"
    assert card["last_sent_hash"] == "old_hash"
    assert card["one_card_policy"] is True
    assert card["scope_line"] == "群里只返回简要结论"
    assert card["milestones"] == [{"ts": "2026-06-12T00:00:00+00:00", "label": "任务建好"}]
    assert "主控 Meegle 登录已过期/未授权" in card["delivery"]["boundaries"]
    assert "conclusion" not in card["delivery"]
    assert body["vm_delivery_proposal"]["delivery"]["conclusion"] == "done"

    relayed = pnc_completion_notice_relay.reconcile_vm_delivery_proposal("task-card-preserve", body)
    relayed_card = relayed["task_card"]
    assert relayed_card["card_message_id"] == "om_card"
    assert relayed_card["last_sent_hash"] == "old_hash"
    assert relayed_card["one_card_policy"] is True
    assert relayed_card["scope_line"] == "群里只返回简要结论"
    assert relayed_card["milestones"] == [{"ts": "2026-06-12T00:00:00+00:00", "label": "任务建好"}]
    assert "主控 Meegle 登录已过期/未授权" in relayed_card["delivery"]["boundaries"]
    assert relayed_card["delivery"]["conclusion"] == "done"


def test_sync_pnc_vm_tasks_preserves_originator_notify_and_guard_markers(tmp_path, monkeypatch):
    # Move3 step-1: relay owns task_card and stamps last_notify_key /
    # close_loop_guard_state when it @-pings a blocked intake. vm-task-sync must
    # leave the card untouched, and relay-side proposal reconciliation must
    # preserve those guard fields (issue 7025381565).
    token = set_hermes_home_override(tmp_path)
    try:
        store = TaskStore(tmp_path / "analytics" / "tasks.db")
        store.upsert(_task("task-notify-preserve", vm_task_id="vm-notify-preserve"))
        sidecar = tmp_path / "task-state" / "task-notify-preserve.json"
        sidecar.parent.mkdir(parents=True, exist_ok=True)
        sidecar.write_text(json.dumps({
            "task_card": {
                "schema_version": 1,
                "task_id": "task-notify-preserve",
                "card_message_id": "om_card",
                "last_notify_key": "in_progress|blocked|guard||need_input",
                "last_notify_at": "2026-06-23T19:42:05+08:00",
                "close_loop_guard_state": "blocked",
                "close_loop_guard_applied_at": "2026-06-23T19:42:05+08:00",
                "delivery": {"boundaries": []},
            }
        }), encoding="utf-8")
        monkeypatch.setattr(pnc_vm_task_sync, "collect_vm_task_status", lambda task_id, **kwargs: {
            "state": {"value": "completed", "summary": "done", "terminal": True},
            "artifacts": [],
            "vm_bridge": {"summary": "done", "state": "completed"},
            "errors": [],
        })

        pnc_vm_task_sync.sync_pnc_vm_tasks(limit=10)
        body = json.loads(sidecar.read_text(encoding="utf-8"))
        card = body["task_card"]
        relayed_card = pnc_completion_notice_relay.reconcile_vm_delivery_proposal(
            "task-notify-preserve",
            body,
        )["task_card"]
    finally:
        reset_hermes_home_override(token)

    assert card["last_notify_key"] == "in_progress|blocked|guard||need_input"
    assert card["last_notify_at"] == "2026-06-23T19:42:05+08:00"
    assert card["close_loop_guard_state"] == "blocked"
    assert card["close_loop_guard_applied_at"] == "2026-06-23T19:42:05+08:00"
    assert relayed_card["last_notify_key"] == "in_progress|blocked|guard||need_input"
    assert relayed_card["last_notify_at"] == "2026-06-23T19:42:05+08:00"
    assert relayed_card["close_loop_guard_state"] == "blocked"
    assert relayed_card["close_loop_guard_applied_at"] == "2026-06-23T19:42:05+08:00"


def test_vm_task_sync_no_longer_writes_task_card_static_guard():
    source = Path(pnc_vm_task_sync.__file__).read_text(encoding="utf-8")
    assert 'body["task_card"]' not in source
    assert "vm_delivery_proposal" in source


def test_completion_notice_generic_delivery_uses_four_fixed_sections_with_cifs_path():
    task = _task("task-structured", vm_task_id="vm-structured", chat_id=pnc_vm_task_sync.PNC_CHAT_ID)

    notice = pnc_vm_task_sync._completion_notice_for_task(task, {
        "state": {"value": "completed", "terminal": True, "summary": "all done"},
        "artifacts": [
            "VM: /mnt/tmp/vm-structured/report.md",
            "CIFS: //hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/vm-structured/report.md",
        ],
        "vm_bridge": {"summary": "all done", "state": "completed"},
        "errors": [],
    })

    assert notice is not None
    lines = notice["text"].splitlines()
    assert lines[0] == "结论：all done"
    assert lines[1] == "路径：//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/vm-structured/report.md"
    assert lines[2].startswith("边界：")
    assert lines[3].startswith("下一步：")


def test_g1q3_completed_task_card_prefers_rca_readback_html_and_attribution(monkeypatch):
    task = _task("task-g1q3-card", vm_task_id="vm-g1q3-card", chat_id=pnc_vm_task_sync.G1Q3_RCA_CHAT_ID)
    task.thread_id = "topic:om_1"
    structured = {
        "schema_version": "g1q3_rca_execution_result_v1",
        "work_item_id": "7017699515",
        "readback": {"safe_for_group": True, "text": "报告链接：file://hfs.minieye.tech/department-perception_test_team/G1Q3_RCA/cases/7017699515/index.html"},
        "status_summary": {"attribution_status": "hypothesis_ready", "report_status": "html_delivery_ready"},
        "artifacts": {
            "best": {
                "index_html": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/7017699515/index.html",
                "viz_mcap_vm": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/7017699515/7017699515.viz.mcap",
                "attribution_causal_text": "触发请求异常 -> 提前制动候选",
                "review_payload": {
                    "candidate_cause": "触发请求出现但 TTC/gate 风险上下文不足，支持提前/不该触发候选。",
                    "candidate_responsibility": "刘培瑞",
                    "review_reason": "需要人工确认候选原因、责任域与证据边界。",
                    "missing_evidence": ["部分需求面板存在但无可绘制点。"],
                }
            }
        },
    }
    monkeypatch.setattr(pnc_vm_task_sync.vm_task_completion_probe, "_read_rca_execution_result", lambda vm_task_id, task_dir: structured)
    monkeypatch.setattr(pnc_vm_task_sync, "_feishu_report_attachment_link", lambda **kwargs: "https://project.feishu.cn/goapi/v5/platform/file/stream/download/token701")

    card = pnc_vm_task_sync._task_card_for_task(task, {
        "state": {"value": "completed", "summary": "generic completed", "terminal": True},
        "artifacts": ["/mnt/tmp/generic/pipeline_result.json"],
        "vm_bridge": {"state": "completed", "summary": "generic completed"},
    })

    delivery = card["delivery"]
    assert "7017699515 RCA 报告已生成" in delivery["conclusion"]
    assert "归因状态：hypothesis_ready" in delivery["conclusion"]
    assert "候选原因：触发请求出现" in delivery["conclusion"]
    assert "责任候选：刘培瑞" in delivery["conclusion"]
    assert delivery["artifact_path"] == delivery["foxglove_url"]
    assert delivery["artifact_label"] == "打开 foxglove 可视化"
    assert delivery["report_status"] == "report_ready"
    assert delivery["rca_status"]["html_link"] == "http://192.168.26.174:18081/G1Q3_RCA/cases/7017699515/index.html"
    assert delivery["foxglove_url"].endswith("/7017699515/7017699515.viz.mcap")
    assert delivery["attribution_causal_text"] == "触发请求异常 -> 提前制动候选"
    assert "需要人工确认候选原因" in delivery["boundaries"][0]


def test_g1q3_completed_intake_without_report_renders_need_download_not_done(tmp_path, monkeypatch):
    # Regression (issue 7023754183): a completed G1Q3-RCA task that only passed
    # the read-only gate (ready_to_download) has no report; this writer must
    # render an honest need-download state so it agrees with the relay instead
    # of re-asserting done every 120s.
    token = set_hermes_home_override(tmp_path)
    try:
        vm_task_id = "20260622-110137-g1q3-rca-issue-intake-7023754183"
        task = _task("task-need-dl", vm_task_id=vm_task_id, chat_id=pnc_vm_task_sync.G1Q3_RCA_CHAT_ID)
        task.thread_id = "topic:om_1"
        shared = tmp_path / "runtime" / "shared-state" / "tasks" / vm_task_id
        shared.mkdir(parents=True)
        (shared / "log.md").write_text(
            '"status": "need_evidence", "decision": "ready_to_download"\n'
            "G4_data_structure: requires_download\n",
            encoding="utf-8",
        )
        # No g1q3_rca_execution_result_v1 -> _extract returns {}.
        monkeypatch.setattr(
            pnc_vm_task_sync.vm_task_completion_probe,
            "_read_rca_execution_result",
            lambda vm_task_id, task_dir: {},
        )
        card = pnc_vm_task_sync._task_card_for_task(task, {
            "state": {"value": "completed", "summary": f"{vm_task_id} completed", "terminal": True},
            "artifacts": [],
            "vm_bridge": {"state": "completed", "user_visible_path": f"//hfs1/dep/tmp/{vm_task_id}/"},
        })
    finally:
        reset_hermes_home_override(token)

    assert card["user_state"] != "done"
    assert card["user_state"] == "in_progress"
    assert card["delivery"]["report_status"] == "need_user_data"
    assert "ready_to_download" in card["delivery"]["conclusion"] or "未生成 RCA 报告" in card["delivery"]["conclusion"]
    # No fabricated clickable report link.
    assert not card["delivery"].get("artifact_path")


def test_g1q3_completed_intake_with_real_report_still_renders_done(tmp_path, monkeypatch):
    # Guard: the need-download path must NOT swallow genuine completed reports.
    token = set_hermes_home_override(tmp_path)
    try:
        vm_task_id = "vm-real-report"
        task = _task("task-real", vm_task_id=vm_task_id, chat_id=pnc_vm_task_sync.G1Q3_RCA_CHAT_ID)
        task.thread_id = "topic:om_1"
        structured = {
            "schema_version": "g1q3_rca_execution_result_v1",
            "work_item_id": "7017699515",
            "readback": {"text": ""},
            "status_summary": {"attribution_status": "hypothesis_ready", "report_status": "html_delivery_ready"},
            "artifacts": {"best": {"index_html": "/mnt/minieye/.../index.html", "review_payload": {}}},
        }
        monkeypatch.setattr(pnc_vm_task_sync.vm_task_completion_probe, "_read_rca_execution_result", lambda vm_task_id, task_dir: structured)
        monkeypatch.setattr(pnc_vm_task_sync, "_feishu_report_attachment_link", lambda **kwargs: "https://project.feishu.cn/token")
        card = pnc_vm_task_sync._task_card_for_task(task, {
            "state": {"value": "completed", "summary": "completed", "terminal": True},
            "artifacts": [],
            "vm_bridge": {"state": "completed"},
        })
    finally:
        reset_hermes_home_override(token)
    assert card["user_state"] == "done"
    assert "RCA 报告已生成" in card["delivery"]["conclusion"]


def test_g1q3_sync_report_ready_result_wins_over_stale_ready_to_download_log_and_preserves_case_dir(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    task_id = "host-task"
    vm_task_id = "20260624-165958-g1q3-rca-issue-intake-7026726390-26390_bc7e1d"
    try:
        store = TaskStore(tmp_path / "analytics" / "tasks.db")
        task = _task(task_id, vm_task_id=vm_task_id)
        task.thread_id = "topic:om_1"
        store.upsert(task)
        shared = tmp_path / "runtime" / "shared-state" / "tasks" / vm_task_id
        shared.mkdir(parents=True, exist_ok=True)
        shared.joinpath("meta.json").write_text(json.dumps({
            "artifact_root": "/mnt/tmp/g1q3_rca_issue_intake_7026726390_bc7e1d/",
        }), encoding="utf-8")
        shared.joinpath("log.md").write_text("gate=ready_to_download\n", encoding="utf-8")
        shared.joinpath("result.md").write_text(json.dumps({
            "schema_version": "shared_state_worker_result_v1",
            "work_item_id": "7026726390",
            "summary": {
                "terminal_state": "report_ready",
                "pipeline_status": "report_generated_need_review",
                "attribution_status": "hypothesis_ready",
            },
            "rca_observation": {"short_conclusion": "候选因果判断：实际减速度相对 OOI 加速度偏重。"},
            "verification": {"checks": [
                {"name": "index_html_exists_nonempty", "ok": True},
                {"name": "report_data_exists_nonempty", "ok": True},
                {"name": "viz_mcap_exists_nonempty", "ok": True},
            ]},
            "artifacts": {
                "artifact_root_vm": "/mnt/tmp/g1q3_rca_issue_intake_7026726390_bc7e1d/",
                "artifact_root_cifs": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/g1q3_rca_issue_intake_7026726390_bc7e1d/",
                "case_dir_vm": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/7026726390_acc",
                "index_html_vm": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/7026726390_acc/index.html",
                "report_data_vm": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/7026726390_acc/report_data.json",
                "viz_mcap_vm": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/7026726390_acc/7026726390_acc.viz.mcap",
                "attribution_causal_text": "实际减速度偏重 -> 纵向控制请求波动",
            },
        }, ensure_ascii=False), encoding="utf-8")
        monkeypatch.setattr(pnc_vm_task_sync, "collect_vm_task_status", lambda task_id, **kwargs: {
            "state": {"value": "completed", "summary": "done", "terminal": True},
            "artifacts": [],
            "vm_bridge": {"summary": "done", "state": "completed"},
            "errors": [],
        })
        monkeypatch.setattr(pnc_vm_task_sync, "_feishu_report_attachment_link", lambda **kwargs: "")
        result = pnc_vm_task_sync.sync_pnc_vm_tasks(limit=10)
        body = json.loads((tmp_path / "task-state" / f"{task_id}.json").read_text(encoding="utf-8"))
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True
    assert "task_card" not in body
    delivery = body["vm_delivery_proposal"]["delivery"]
    assert delivery["report_status"] == "html_delivery_ready"
    assert delivery["agent_artifact_root_vm"].startswith("/mnt/tmp/")
    assert delivery["business_case_dir_vm"].startswith("/mnt/minieye/pdcl/department/perception_test_team/")
    assert delivery["business_case_dir_cifs"].startswith("//hfs.minieye.tech/department-perception_test_team/")
    assert delivery["foxglove_url"].endswith("/7026726390_acc/7026726390_acc.viz.mcap")
    assert delivery["artifact_path"] == delivery["foxglove_url"]
    assert delivery["report_status"] == "report_ready"
    assert delivery["attribution_causal_text"] == "实际减速度偏重 -> 纵向控制请求波动"
    assert body["vm_delivery_proposal"]["user_state"] == "done"

    relayed = pnc_completion_notice_relay.reconcile_vm_delivery_proposal(task_id, body)
    assert relayed["task_card"]["delivery"]["report_status"] == "html_delivery_ready"
    assert relayed["task_card"]["delivery"]["business_case_dir_cifs"].startswith("//hfs.minieye.tech/department-perception_test_team/")
    assert relayed["task_card"]["user_state"] == "done"


def test_g1q3_task_card_prefers_delivery_contract_report_completed(monkeypatch):
    task = _task("task-contract", vm_task_id="vm-contract", chat_id=pnc_vm_task_sync.G1Q3_RCA_CHAT_ID)
    monkeypatch.setattr(pnc_vm_task_sync, "_feishu_report_attachment_link", lambda **kwargs: "https://project.feishu.cn/report-token")

    card = pnc_vm_task_sync._task_card_for_task(task, {
        "state": {"value": "completed", "summary": "generic completed", "terminal": True},
        "delivery_contract": {
            "schema_version": "g1q3_delivery_contract_v1",
            "work_item_id": "7026690721",
            "business_state": "report_completed",
            "presentation_state": "report_ready_needs_review",
            "report": {
                "status": "report_generated_need_review",
                "is_deliverable": True,
                "is_candidate": True,
                "candidate_owner": "殷莉奇",
                "candidate_owner_domain": "ACC",
            },
            "summary": {"l0": "7026690721 RCA 报告已生成。", "short_conclusion": "候选因果判断：实际减速度偏重。"},
            "evidence_boundary": ["parsed/L2 assets 缺失"],
            "artifacts": {
                "task_root_vm": "/mnt/tmp/vm-contract/",
                "task_root_cifs": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/vm-contract/",
                "case_dir_vm": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/7026690721_acc",
                "case_dir_cifs": "//hfs.minieye.tech/department-perception_test_team/G1Q3_RCA/cases/7026690721_acc",
                "primary_report_vm": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/7026690721_acc/index.html",
                "primary_report_cifs": "//hfs.minieye.tech/department-perception_test_team/G1Q3_RCA/cases/7026690721_acc/index.html",
                "report_data_vm": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/7026690721_acc/report_data.json",
            },
            "verification": {"terminal_state": "report_ready", "pipeline_status": "report_generated_need_review"},
        },
        "artifacts": ["/mnt/tmp/generic/pipeline_result.json"],
        "vm_bridge": {"state": "completed", "summary": "generic completed"},
    })

    delivery = card["delivery"]
    assert card["user_state"] == "done"
    assert delivery["source"] == "delivery_contract_v1"
    assert delivery["report_status"] == "html_delivery_ready"
    assert delivery["artifact_path"] == "http://192.168.26.174:18081/G1Q3_RCA/cases/7026690721_acc/index.html"
    assert delivery["business_case_dir_cifs"].startswith("//hfs.minieye.tech/department-perception_test_team/")
    assert delivery["agent_artifact_root_vm"] == "/mnt/tmp/vm-contract/"
    assert "责任候选：殷莉奇" in delivery["conclusion"]
    assert "parsed/L2 assets 缺失" in delivery["boundaries"]


def test_g1q3_contract_foxglove_url_is_byte_identical_across_host_writers(monkeypatch):
    monkeypatch.delenv("PNC_FOXGLOVE_RENDER_HOST", raising=False)
    case_key = "6986500860_fcw_FCW-合肥-G1Q3_6028车-自车右转_直行后-FCW疑似误触发"
    case_dir = f"/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/{case_key}"
    contract = {
        "schema_version": "g1q3_delivery_contract_v1",
        "work_item_id": "6986500860",
        "business_state": "report_completed",
        "presentation_state": "report_ready_needs_review",
        "report": {"status": "report_generated_need_review", "is_deliverable": True, "is_candidate": True},
        "summary": {"short_conclusion": "目标测速异常 -> ACC 纵向请求波动 -> 减速过重"},
        "artifacts": {
            "case_dir_vm": case_dir,
            "index_html_vm": f"{case_dir}/index.html",
            "primary_report_vm": f"{case_dir}/index.html",
            "viz_mcap_vm": f"{case_dir}/{case_key}.viz.mcap",
            "attribution_causal_text": "目标测速异常 -> ACC 纵向请求波动 -> 减速过重",
        },
        "verification": {"terminal_state": "report_ready", "pipeline_status": "report_generated_need_review"},
    }

    sync_delivery = pnc_vm_task_sync._delivery_from_contract(_task("fox-contract"), contract)
    relay_truth = pnc_completion_notice_relay._g1q3_contract_report_ready_truth(contract)

    expected = (
        "https://192.168.21.217/?ds=foxglove-http&ds.mcapPath="
        "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/"
        "6986500860_fcw_FCW-%E5%90%88%E8%82%A5-G1Q3_6028%E8%BD%A6-%E8%87%AA%E8%BD%A6%E5%8F%B3%E8%BD%AC_"
        "%E7%9B%B4%E8%A1%8C%E5%90%8E-FCW%E7%96%91%E4%BC%BC%E8%AF%AF%E8%A7%A6%E5%8F%91/"
        "6986500860_fcw_FCW-%E5%90%88%E8%82%A5-G1Q3_6028%E8%BD%A6-%E8%87%AA%E8%BD%A6%E5%8F%B3%E8%BD%AC_"
        "%E7%9B%B4%E8%A1%8C%E5%90%8E-FCW%E7%96%91%E4%BC%BC%E8%AF%AF%E8%A7%A6%E5%8F%91.viz.mcap"
    )
    assert sync_delivery["foxglove_url"] == expected
    assert sync_delivery["artifact_path"] == expected
    assert relay_truth["foxglove_url"] == expected
    assert sync_delivery["foxglove_url"].encode() == relay_truth["foxglove_url"].encode()
    assert sync_delivery["attribution_causal_text"] == relay_truth["attribution_causal_text"]
    assert "@" not in json.dumps(sync_delivery, ensure_ascii=False)


def test_g1q3_contract_without_html_or_verified_viz_is_not_report_ready():
    contract = {
        "schema_version": "g1q3_delivery_contract_v1",
        "business_state": "report_completed",
        "presentation_state": "report_ready_needs_review",
        "report": {"status": "report_generated_need_review", "is_deliverable": True},
        "artifacts": {"viz_mcap_vm": ""},
        "verification": {"terminal_state": "report_ready"},
    }

    assert pnc_vm_task_sync._delivery_from_contract(_task("no-surface"), contract) == {}
    assert pnc_completion_notice_relay._g1q3_contract_report_ready_truth(contract) == {}


def test_g1q3_viz_only_contract_is_done_without_claiming_html_ready():
    case_dir = "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/viz_only"
    task = _task("viz-only", vm_task_id="viz-only-vm", chat_id=pnc_vm_task_sync.G1Q3_RCA_CHAT_ID)
    card = pnc_vm_task_sync._task_card_for_task(task, {
        "state": {"value": "completed", "summary": "done", "terminal": True},
        "delivery_contract": {
            "schema_version": "g1q3_delivery_contract_v1",
            "business_state": "report_completed",
            "presentation_state": "report_ready_needs_review",
            "report": {"status": "report_ready", "is_deliverable": True, "is_candidate": True},
            "summary": {"short_conclusion": "目标输入异常 -> 纵向请求波动"},
            "artifacts": {
                "case_dir_vm": case_dir,
                "viz_mcap_vm": f"{case_dir}/viz_only.viz.mcap",
                "attribution_causal_text": "目标输入异常 -> 纵向请求波动",
            },
            "verification": {"terminal_state": "report_ready", "pipeline_status": "report_generated_need_review"},
        },
    })

    assert card["user_state"] == "done"
    assert card["delivery"]["report_status"] == "report_ready"
    assert card["delivery"]["artifact_path"] == card["delivery"]["foxglove_url"]
    assert card["delivery"]["foxglove_url"].endswith("/viz_only/viz_only.viz.mcap")


def test_foxglove_render_host_is_configurable(monkeypatch):
    monkeypatch.setenv("PNC_FOXGLOVE_RENDER_HOST", "viewer.internal:8443")

    url = pnc_foxglove_delivery.foxglove_url(
        "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/case_a/case_a.viz.mcap"
    )

    assert url.startswith("https://viewer.internal:8443/?ds=foxglove-http&ds.mcapPath=")


def test_g1q3_task_card_delivery_contract_missing_input_is_not_done():
    task = _task("task-contract-missing", vm_task_id="vm-contract-missing", chat_id=pnc_vm_task_sync.G1Q3_RCA_CHAT_ID)

    card = pnc_vm_task_sync._task_card_for_task(task, {
        "state": {"value": "completed", "summary": "generic completed", "terminal": True},
        "delivery_contract": {
            "schema_version": "g1q3_delivery_contract_v1",
            "business_state": "missing_user_input",
            "presentation_state": "need_user_input",
            "report": {"status": "html_missing", "is_deliverable": False},
            "user_action": {"requires_user_input": True, "next_action_text": "请补充问题数据地址_PDCL"},
            "artifacts": {"task_root_vm": "/mnt/tmp/vm-contract-missing/"},
        },
        "artifacts": [],
        "vm_bridge": {"state": "completed", "summary": "generic completed"},
    })

    assert card["user_state"] == "in_progress"
    assert card["delivery"]["source"] == "delivery_contract_v1"
    assert card["delivery"]["report_status"] == "need_user_data"
    assert not card["delivery"].get("artifact_path")
    assert "请补充问题数据地址_PDCL" in "；".join(card["delivery"]["boundaries"])


def test_g1q3_task_card_stitches_latest_governance_report_contract(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    try:
        gov = tmp_path / "pnc_agent" / "governance_rca"
        gov.mkdir(parents=True)
        (gov / "g1q3_rca_issue_intake_7029768863_8a6bed.json").write_text(json.dumps({
            "work_item_id": "7029768863",
            "artifact_root": "/mnt/tmp/g1q3_rca_issue_intake_7029768863_8a6bed/",
        }), encoding="utf-8")
        task = _task(
            "20260627-142804-g1q3-rca-issue-intake-7029768863-68863_4a42ba",
            vm_task_id="20260627-142804-g1q3-rca-issue-intake-7029768863-68863_4a42ba",
            chat_id=pnc_vm_task_sync.G1Q3_RCA_CHAT_ID,
        )
        task.request_summary = "分析这个问题https://project.feishu.cn/t03o4q/issue/detail/7029768863"

        latest_contract = {
            "schema_version": "g1q3_delivery_contract_v1",
            "work_item_id": "7029768863",
            "business_state": "report_completed",
            "presentation_state": "report_ready_needs_review",
            "report": {
                "status": "html_delivery_ready",
                "is_deliverable": True,
                "is_candidate": True,
                "candidate_owner": "殷莉奇",
                "candidate_owner_domain": "ACC",
            },
            "summary": {"l0": "7029768863 RCA 报告已生成。", "short_conclusion": "候选因果判断：实际减速度偏重。"},
            "evidence_boundary": ["原始 mcap 已落盘；当前报告为候选 RCA，需人工复核。"],
            "artifacts": {
                "task_root_vm": "/mnt/tmp/g1q3_rca_issue_intake_7029768863_8a6bed",
                "task_root_cifs": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/g1q3_rca_issue_intake_7029768863_8a6bed",
                "case_dir_vm": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/7029768863_acc",
                "case_dir_cifs": "//hfs.minieye.tech/department-perception_test_team/G1Q3_RCA/cases/7029768863_acc",
                "primary_report_vm": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/7029768863_acc/index.html",
                "primary_report_cifs": "//hfs.minieye.tech/department-perception_test_team/G1Q3_RCA/cases/7029768863_acc/index.html",
            },
            "verification": {"terminal_state": "report_ready", "pipeline_status": "report_generated_need_review"},
        }
        monkeypatch.setattr(pnc_vm_task_sync, "_read_vm_json_file", lambda path: latest_contract)
        monkeypatch.setattr(pnc_vm_task_sync, "_feishu_report_attachment_link", lambda **kwargs: "https://project.feishu.cn/report-token")

        card = pnc_vm_task_sync._task_card_for_task(task, {
            "state": {"value": "completed", "summary": "old blocked", "terminal": True},
            "delivery_contract": {
                "schema_version": "g1q3_delivery_contract_v1",
                "work_item_id": "7029768863",
                "business_state": "missing_user_input",
                "report": {"status": "html_missing", "is_deliverable": False},
                "user_action": {"requires_user_input": True},
                "evidence_boundary": [
                    "处理进展：需补充数据/证据",
                    "元数据门禁：skipped / out_of_scope",
                    "具体缺少：需要所属项目为 G1Q3_T1L_捷途，且发生时间不早于 2025-05-12",
                ],
            },
            "vm_bridge": {"state": "completed", "summary": "old blocked"},
        })
    finally:
        reset_hermes_home_override(token)

    assert card["user_state"] == "done"
    assert card["delivery"]["report_status"] == "html_delivery_ready"
    assert card["delivery"]["artifact_path"] == "http://192.168.26.174:18081/G1Q3_RCA/cases/7029768863_acc/index.html"
    assert "需要发起人补充" not in card["delivery"]["conclusion"]
    assert "7029768863 RCA 报告已生成" in card["delivery"]["conclusion"]
    boundary_text = "；".join(card["delivery"].get("boundaries") or [])
    assert "原始 mcap 已落盘" in boundary_text
    for stale in [
        "需补充数据",
        "out_of_scope",
        "元数据门禁",
        "具体缺少",
        "需要所属项目为 G1Q3_T1L_捷途",
        "need_input",
        "need_source_or_evidence",
    ]:
        assert stale not in boundary_text


def test_feishu_report_attachment_upload_adds_utf8_bom_charset_and_codec_version(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    uploads = []

    class FakeResponse:
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc, tb):
            return False
        def read(self):
            return '<!doctype html><title>ACC-前车切入刹车过重</title>'.encode('utf-8')

    def fake_upload(args):
        path = Path(args[2])
        uploads.append({
            'args': args,
            'bytes': path.read_bytes(),
        })
        return 0, json.dumps({'file_url': 'https://project.feishu.cn/new-bom-link', 'file_token': 'tok-new'}), ''

    try:
        monkeypatch.setattr(pnc_vm_task_sync.urllib.request, 'urlopen', lambda *a, **k: FakeResponse())
        monkeypatch.setattr('gateway.pnc_issue_context.default_meegle_runner', fake_upload)
        link = pnc_vm_task_sync._feishu_report_attachment_link(
            work_item_id='7029768863',
            vm_task_id='vm-1',
            index_html='/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/7029768863_acc/index.html',
        )
        ledger = json.loads((tmp_path / 'pnc_agent' / 'quota' / 'g1q3_report_attachments.json').read_text(encoding='utf-8'))
    finally:
        reset_hermes_home_override(token)

    assert link == 'https://project.feishu.cn/new-bom-link'
    assert len(uploads) == 1
    assert uploads[0]['bytes'].startswith(pnc_vm_task_sync.UTF8_BOM)
    assert uploads[0]['bytes'].count(pnc_vm_task_sync.UTF8_BOM) == 1
    content_type_index = uploads[0]['args'].index('--content-type') + 1
    assert uploads[0]['args'][content_type_index] == 'text/html; charset=utf-8'
    entry = next(iter(ledger['reports'].values()))
    assert entry['delivery_codec_version'] == pnc_vm_task_sync.REPORT_ATTACHMENT_CODEC_VERSION


def test_feishu_report_attachment_cache_requires_codec_version_and_bom_is_idempotent(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    uploads = []
    index_html = '/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/7029768863_acc/index.html'
    key = f'7029768863|{index_html}'

    class FakeResponse:
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc, tb):
            return False
        def read(self):
            return pnc_vm_task_sync.UTF8_BOM + '<!doctype html><title>ACC-前车切入刹车过重</title>'.encode('utf-8')

    def fake_upload(args):
        path = Path(args[2])
        uploads.append(path.read_bytes())
        return 0, json.dumps({'file_url': f'https://project.feishu.cn/new-bom-link-{len(uploads)}', 'file_token': 'tok-new'}), ''

    try:
        ledger_path = tmp_path / 'pnc_agent' / 'quota' / 'g1q3_report_attachments.json'
        ledger_path.parent.mkdir(parents=True)
        ledger_path.write_text(json.dumps({
            'reports': {
                key: {
                    'work_item_id': '7029768863',
                    'vm_task_id': 'old-vm',
                    'index_html': index_html,
                    'file_url': 'https://project.feishu.cn/old-no-codec-link',
                }
            }
        }), encoding='utf-8')
        monkeypatch.setattr(pnc_vm_task_sync.urllib.request, 'urlopen', lambda *a, **k: FakeResponse())
        monkeypatch.setattr('gateway.pnc_issue_context.default_meegle_runner', fake_upload)

        first = pnc_vm_task_sync._feishu_report_attachment_link(work_item_id='7029768863', vm_task_id='vm-2', index_html=index_html)
        second = pnc_vm_task_sync._feishu_report_attachment_link(work_item_id='7029768863', vm_task_id='vm-2', index_html=index_html)
        ledger = json.loads(ledger_path.read_text(encoding='utf-8'))
    finally:
        reset_hermes_home_override(token)

    assert first == 'https://project.feishu.cn/new-bom-link-1'
    assert second == first
    assert len(uploads) == 1
    assert uploads[0].startswith(pnc_vm_task_sync.UTF8_BOM)
    assert uploads[0].count(pnc_vm_task_sync.UTF8_BOM) == 1
    assert ledger['reports'][key]['delivery_codec_version'] == pnc_vm_task_sync.REPORT_ATTACHMENT_CODEC_VERSION


def _g1q3_laundered_contract(work_item_id="7029488224"):
    return {
        "schema_version": "g1q3_delivery_contract_v1",
        "work_item_id": work_item_id,
        "business_state": "awaiting_download",
        "presentation_state": "processing",
        "report": {"status": "need_download", "is_deliverable": False},
        "user_action": {"requires_user_input": False, "next_action": "已受理；无需发起人补数据"},
        "summary": {"l0": "已受理；无需发起人补数据"},
        "evidence_boundary": ["数据已就位"],
        "artifacts": {
            "task_root_vm": "/mnt/tmp/g1q3_rca_issue_intake_7029488224_real/",
            "task_root_cifs": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/g1q3_rca_issue_intake_7029488224_real/",
        },
        "verification": {"pipeline_status": "awaiting_download", "terminal_state": "running"},
    }


def test_card_blocked_keyframe_honest():
    task = _task(
        "20260627-120000-g1q3-rca-issue-intake-7029488224-real",
        vm_task_id="20260627-120000-g1q3-rca-issue-intake-7029488224-real",
        chat_id=pnc_vm_task_sync.G1Q3_RCA_CHAT_ID,
    )
    pipeline_result = {
        "status": "blocked",
        "stage": "s45_auto_keyframe",
        "blocker": {
            "kind": "missing_signal_keyframe",
            "message": "discover_acc_speed_unstable 缺少可定位关键帧信号，自动找帧无候选",
        },
    }

    card = pnc_vm_task_sync._task_card_for_task(task, {
        "state": {"value": "completed", "summary": "worker finished", "terminal": True},
        "delivery_contract": _g1q3_laundered_contract(),
        "pipeline_result": pipeline_result,
        "artifacts": ["/mnt/tmp/g1q3_rca_issue_intake_7029488224_real/delivery_contract.json"],
        "vm_bridge": {"state": "completed", "summary": "worker finished"},
    })

    delivery = card["delivery"]
    assert "discover_acc_speed_unstable" in delivery["conclusion"]
    assert "正在自动下载" not in delivery["conclusion"]
    assert delivery["human_action_kind"] == "need_keyframe"
    assert delivery["report_status"] == delivery["attribution_status"] == "need_keyframe"
    assert delivery["source"] == "pipeline_result_truth_override"
    assert delivery["presentation_state"] == "blocked"
    assert "contract_laundered_blocked_as_awaiting" in "；".join(delivery["boundaries"])


def _g1q3_honest_blocked_keyframe_contract(work_item_id="7029488224"):
    contract = _g1q3_laundered_contract(work_item_id)
    contract.update({
        "business_state": "blocked_need_keyframe",
        "presentation_state": "blocked",
        "human_action_kind": "need_keyframe",
        "report": {"status": "need_keyframe", "is_deliverable": False},
        "user_action": {"requires_user_input": True, "next_action": "自动找帧无候选；需人工补帧"},
        "blocker": {"kind": "frame_id_unresolved_after_auto_discovery", "message": "自动找帧无候选；需人工补帧"},
        "verification": {"pipeline_status": "blocked", "terminal_state": "need_keyframe", "pipeline_stage": "s45_auto_keyframe"},
    })
    return contract


def test_honest_blocked_keyframe_contract_does_not_override():
    task = _task(
        "20260627-120000-g1q3-rca-issue-intake-7029488224-honest",
        vm_task_id="20260627-120000-g1q3-rca-issue-intake-7029488224-honest",
        chat_id=pnc_vm_task_sync.G1Q3_RCA_CHAT_ID,
    )

    delivery = pnc_vm_task_sync._delivery_from_contract(task, {
        "delivery_contract": _g1q3_honest_blocked_keyframe_contract(),
        "pipeline_result": {
            "status": "blocked",
            "stage": "s45_auto_keyframe",
            "blocker": {"kind": "frame_id_unresolved_after_auto_discovery", "message": "自动找帧无候选；需人工补帧"},
        },
    })

    assert delivery["source"] == "delivery_contract_v1"
    assert delivery["business_state"] == "blocked_need_keyframe"
    assert delivery["presentation_state"] == "blocked"
    assert delivery["human_action_kind"] == "need_keyframe"
    assert delivery["report_status"] == "need_keyframe"


def test_genuine_download_in_progress_stays_optimistic():
    task = _task(
        "20260627-120000-g1q3-rca-issue-intake-7029488224-download",
        vm_task_id="20260627-120000-g1q3-rca-issue-intake-7029488224-download",
        chat_id=pnc_vm_task_sync.G1Q3_RCA_CHAT_ID,
    )
    card = pnc_vm_task_sync._task_card_for_task(task, {
        "state": {"value": "completed", "summary": "worker finished", "terminal": True},
        "delivery_contract": _g1q3_laundered_contract(),
        "pipeline_result": {"status": "running", "stage": "download"},
        "artifacts": [],
        "vm_bridge": {"state": "completed", "summary": "worker finished"},
    })

    delivery = card["delivery"]
    assert card["user_state"] == "in_progress"
    assert delivery["report_status"] == "need_download"
    assert delivery["human_action_kind"] == "none"
    assert "数据下载执行中" in delivery["conclusion"]
    assert delivery["source"] == "delivery_contract_v1"
