import json
import os
import re
import shutil
from pathlib import Path

import pytest

from gateway.record_only import runtime
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


def _set_record_only_env(tmp_path: Path, monkeypatch):
    records = tmp_path / "records"
    try:
        records.resolve(strict=False).relative_to(Path.home().resolve())
    except ValueError:
        pass
    else:
        records = tmp_path.parent / f"{tmp_path.name}-outbound-records"
    records.mkdir(mode=0o700)
    key_file = tmp_path / "record.key"
    key_file.write_text("ab" * 32 + "\n", encoding="ascii")
    key_file.chmod(0o600)
    census_root = Path(
        os.getenv("HERMES_OUTBOUND_CENSUS_ROOT")
        or Path(__file__).resolve().parents[3] / "evidence" / "target-outbound-census"
    )
    monkeypatch.setenv("HERMES_OUTBOUND_MODE", "record-only")
    monkeypatch.setenv("HERMES_OUTBOUND_RECORD_ROOT", str(records))
    monkeypatch.setenv("HERMES_OUTBOUND_RECORD_KEY_FILE", str(key_file))
    monkeypatch.setenv("HERMES_OUTBOUND_CENSUS_ROOT", str(census_root))
    runtime._reset_for_tests()
    return records


def _make_fixture_root(tmp_path: Path, monkeypatch) -> Path:
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv(pnc_vm_task_sync.FIXTURE_ISOLATION_ROOT_ENV, str(tmp_path.parent))
    source_census = Path(
        os.getenv("HERMES_OUTBOUND_CENSUS_ROOT")
        or Path(__file__).resolve().parents[3] / "evidence" / "target-outbound-census"
    )
    census_root = tmp_path.parent / f"{tmp_path.name}-outbound-census"
    census_root.mkdir(mode=0o700)
    for name in ("INDEX.json", "census-v4.json"):
        shutil.copy2(source_census / name, census_root / name)
    monkeypatch.setenv("HERMES_OUTBOUND_CENSUS_ROOT", str(census_root))
    fixture_root = tmp_path / pnc_vm_task_sync.FIXTURE_DIR_NAME / "case"
    fixture_root.mkdir(parents=True, mode=0o700)
    return fixture_root


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
    monkeypatch.delenv("HERMES_OUTBOUND_MODE", raising=False)
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


def test_main_dry_run_outputs_json(tmp_path, capsys, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    try:
        store = TaskStore(tmp_path / "analytics" / "tasks.db")
        store.upsert(_task("task-1", vm_task_id="vm-1"))
        receipt_path = tmp_path / "runtime" / "sync-receipt.json"
        monkeypatch.setenv("PNC_VM_TASK_SYNC_RECEIPT_PATH", str(receipt_path))
        monkeypatch.setattr(
            pnc_vm_task_sync,
            "build_process_runtime_evidence",
            lambda **_kwargs: {
                "pid": 321,
                "process_create_time": 1783890000.0,
                "started_at": "2026-07-13T00:00:00+00:00",
                "runtime_identity": {
                    "executable": "/runtime/.venv/bin/python",
                    "script": "/runtime/scripts/pnc_vm_task_sync.py",
                    "cwd": "/runtime",
                    "script_sha256": "1" * 64,
                    "interpreter_sha256": "2" * 64,
                    "plist_path": "/Users/test/Library/LaunchAgents/sync.plist",
                    "plist_sha256": "3" * 64,
                    "program_arguments_sha256": "4" * 64,
                    "environment_sha256": "5" * 64,
                },
            },
        )
        rc = pnc_vm_task_sync.main(["--dry-run", "--json"])
        out = json.loads(capsys.readouterr().out)
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    finally:
        reset_hermes_home_override(token)

    assert rc == 0
    assert out["dry_run"] is True
    assert out["candidate_count"] == 1
    assert out["rows"][0]["vm_task_id"] == "vm-1"
    assert receipt_path.stat().st_mode & 0o777 == 0o600
    assert receipt["schema_version"] == "pnc_vm_task_sync_completion_v1"
    assert receipt["service_label"] == "local.pnc.vm-task-sync"
    assert receipt["exit_code"] == 0
    assert receipt["ok"] is True
    assert receipt["skipped"] is False
    assert receipt["candidate_count"] == 1
    assert receipt["error_count"] == 0
    assert receipt["errors"] == []


def test_fixture_mode_executes_projection_notice_comment_and_sidecar_without_collector(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    fixture_root = _make_fixture_root(tmp_path, monkeypatch)
    _set_record_only_env(tmp_path, monkeypatch)
    monkeypatch.setenv("HERMES_G1Q3_REPORT_COMMENT", "1")
    task_id = "g1q3-rca-fixture-7017699515"
    vm_task_id = "g1q3-rca-issue-intake-7017699515"
    payload = {
        "state": {
            "value": "completed",
            "summary": "中文 fixture 完成",
            "terminal": True,
            "updated_at": "2026-07-12T10:00:00+00:00",
        },
        "artifacts": [],
        "errors": [],
        "vm_bridge": {"state": "completed", "summary": "中文 fixture 完成"},
        "delivery_contract": {
            "schema_version": "g1q3_delivery_contract_v1",
            "work_item_id": "7017699515",
            "business_state": "report_completed",
            "presentation_state": "report_ready_needs_review",
            "report": {"status": "report_generated_need_review", "is_deliverable": True, "is_candidate": True},
            "summary": {"l0": "报告已生成", "short_conclusion": "纵向控制请求波动"},
            "evidence_boundary": [
                "领取 //hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/l4-fixture/"
            ],
            "artifacts": {
                "index_html_vm": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/7017699515/index.html",
                "case_dir_cifs": "//hfs.minieye.tech/department-perception_test_team/G1Q3_RCA/cases/7017699515/",
                "viz_mcap_vm": (
                    "/mnt/minieye/pdcl/department/perception_test_team/"
                    "G1Q3_RCA/cases/g1q3-rca-s1-"
                    + "a" * 64
                    + "/g1q3-rca-s1-"
                    + "a" * 64
                    + ".viz.mcap"
                ),
            },
        },
    }
    fixture_file = fixture_root / f"{vm_task_id}.json"
    fixture_file.write_text(
        json.dumps(
            {
                "schema_version": pnc_vm_task_sync.FIXTURE_SCHEMA_VERSION,
                "vm_task_id": vm_task_id,
                "scenario_id": "S4",
                "payload": payload,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    fixture_file.chmod(0o600)

    def bomb_collect(*_args, **_kwargs):
        raise AssertionError("real VM/SSH collector was touched")

    monkeypatch.setattr(pnc_vm_task_sync, "collect_vm_task_status", bomb_collect)
    monkeypatch.setattr(
        pnc_vm_task_sync,
        "_read_vm_pipeline_state_json",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("fixture attempted a VM pipeline-state read")
        ),
    )
    monkeypatch.setattr(
        pnc_vm_task_sync,
        "_read_vm_json_file",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("fixture attempted a VM JSON read")
        ),
    )
    try:
        store = TaskStore(tmp_path / "analytics" / "tasks.db")
        store.upsert(_task(task_id, vm_task_id=vm_task_id))
        result = pnc_vm_task_sync.sync_pnc_vm_tasks(
            limit=10,
            fixture_root=fixture_root,
            required_fixture_scenarios=["S4"],
        )
        sidecar = json.loads((tmp_path / "task-state" / f"{task_id}.json").read_text(encoding="utf-8"))
        transport = runtime.get_record_only_transport("gateway.pnc_report_comment")
        assert transport is not None
        rows = transport.read_all()
    finally:
        runtime._reset_for_tests()
        reset_hermes_home_override(token)

    assert result["ok"] is True
    assert result["synced_count"] == 1
    assert result["fixture_mode"] is True
    assert result["pipeline_evidence_source"] == "fixture"
    assert result["rows"][0]["pipeline_evidence_source"] == "fixture"
    assert result["executed_fixture_scenarios"] == ["S4"]
    assert result["missing_fixture_scenarios"] == []
    assert sidecar["completion_notice"]["state"] == "completed"
    assert sidecar["report_comment"]["action"] == "recorded_intents"
    assert sidecar["report_comment"]["posted"] is False
    assert sidecar["vm_delivery_proposal"]["delivery"]["report_status"] == "report_ready"
    assert sidecar["vm_delivery_proposal"]["evidence_source"] == "fixture"
    assert sidecar["vm_delivery_proposal"]["delivery_contract"]["schema_version"] == "g1q3_delivery_contract_v1"
    assert [row["operation"] for row in rows] == ["project_comment_list", "project_comment_add"]
    assert not (tmp_path / "pnc_agent" / "quota" / "g1q3_report_comments.json").exists()


def test_fixture_mode_rejects_symlink_dry_run_and_real_ssh_fallback(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    fixture_root = _make_fixture_root(tmp_path, monkeypatch)
    symlink = fixture_root.parent / "linked"
    symlink.symlink_to(fixture_root, target_is_directory=True)
    try:
        with pytest.raises(ValueError, match="symlinks|aliases"):
            pnc_vm_task_sync._validate_fixture_root(symlink)
        with pytest.raises(ValueError, match="mutually exclusive"):
            pnc_vm_task_sync.sync_pnc_vm_tasks(dry_run=True, fixture_root=fixture_root)
        with pytest.raises(ValueError, match="mutually exclusive"):
            pnc_vm_task_sync.sync_pnc_vm_tasks(
                fixture_root=fixture_root,
                shared_state_root="/mnt/tmp/forbidden-real-root",
                ssh_mini_agent="/candidate/ssh-mini-agent",
            )
        with pytest.raises(SystemExit):
            pnc_vm_task_sync.main(
                ["--fixture-root", str(fixture_root), "--no-lock", "--json"]
            )
    finally:
        reset_hermes_home_override(token)


def test_fixture_mode_rejects_canonical_home_before_reading_fixture(tmp_path, monkeypatch):
    fixture_root = _make_fixture_root(tmp_path, monkeypatch)
    canonical_home = pnc_vm_task_sync._canonical_hermes_roots()[0]
    with pytest.raises(ValueError, match="canonical Hermes/OpenClaw home"):
        pnc_vm_task_sync._validate_fixture_root(fixture_root, hermes_home=canonical_home)


def test_fixture_mode_requires_valid_record_only_before_taskstore_or_collector(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    fixture_root = _make_fixture_root(tmp_path, monkeypatch)
    monkeypatch.delenv("HERMES_OUTBOUND_MODE", raising=False)
    runtime._reset_for_tests()

    def bomb_collect(*_args, **_kwargs):
        raise AssertionError("real VM/SSH collector was touched")

    monkeypatch.setattr(pnc_vm_task_sync, "collect_vm_task_status", bomb_collect)
    try:
        with pytest.raises(ValueError, match="requires HERMES_OUTBOUND_MODE=record-only"):
            pnc_vm_task_sync.sync_pnc_vm_tasks(fixture_root=fixture_root)
        assert not (tmp_path / "analytics").exists()
        assert not (tmp_path / "task-state").exists()
    finally:
        runtime._reset_for_tests()
        reset_hermes_home_override(token)


def test_fixture_mode_rejects_duplicate_json_keys_and_unsafe_output_parent(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    fixture_root = _make_fixture_root(tmp_path, monkeypatch)
    _set_record_only_env(tmp_path, monkeypatch)
    vm_task_id = "fixture-duplicate-json"
    duplicate = fixture_root / f"{vm_task_id}.json"
    duplicate.write_text(
        '{"schema_version":"pnc_vm_task_sync_fixture_v1",'
        '"vm_task_id":"fixture-duplicate-json","scenario_id":"S1",'
        '"payload":{"state":{"value":"completed","value":"failed"}}}',
        encoding="utf-8",
    )
    duplicate.chmod(0o600)
    try:
        store = TaskStore(tmp_path / "analytics" / "tasks.db")
        store.upsert(_task("fixture-duplicate-task", vm_task_id=vm_task_id))
        result = pnc_vm_task_sync.sync_pnc_vm_tasks(fixture_root=fixture_root)
        assert result["ok"] is False
        assert "duplicate JSON key" in result["errors"][0]

        outside = tmp_path / "outside-task-state"
        outside.mkdir(mode=0o700)
        task_state = tmp_path / "task-state"
        if task_state.exists():
            task_state.rmdir()
        task_state.symlink_to(outside, target_is_directory=True)
        with pytest.raises((OSError, ValueError)):
            pnc_vm_task_sync.sync_pnc_vm_tasks(fixture_root=fixture_root)
    finally:
        runtime._reset_for_tests()
        reset_hermes_home_override(token)


def test_fixture_output_swap_after_read_fails_before_any_outside_sidecar_write(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    fixture_root = _make_fixture_root(tmp_path, monkeypatch)
    _set_record_only_env(tmp_path, monkeypatch)
    vm_task_id = "fixture-hostile-swap"
    task_id = "fixture-hostile-swap-task"
    fixture_file = fixture_root / f"{vm_task_id}.json"
    fixture_file.write_text(
        json.dumps(
            {
                "schema_version": pnc_vm_task_sync.FIXTURE_SCHEMA_VERSION,
                "vm_task_id": vm_task_id,
                "scenario_id": "S1",
                "payload": {
                    "state": {"value": "completed", "summary": "done", "terminal": True},
                    "artifacts": [],
                    "vm_bridge": {"state": "completed", "summary": "done"},
                    "errors": [],
                },
            }
        ),
        encoding="utf-8",
    )
    fixture_file.chmod(0o600)
    outside = tmp_path / "outside-sidecars"
    outside.mkdir(mode=0o700)
    original_load = pnc_vm_task_sync._load_fixture_status

    def swap_after_read(root, current_vm_task_id):
        loaded = original_load(root, current_vm_task_id)
        task_state = tmp_path / "task-state"
        task_state.rmdir()
        task_state.symlink_to(outside, target_is_directory=True)
        return loaded

    monkeypatch.setattr(pnc_vm_task_sync, "_load_fixture_status", swap_after_read)
    try:
        store = TaskStore(tmp_path / "analytics" / "tasks.db")
        store.upsert(_task(task_id, vm_task_id=vm_task_id))
        result = pnc_vm_task_sync.sync_pnc_vm_tasks(fixture_root=fixture_root)
    finally:
        runtime._reset_for_tests()
        reset_hermes_home_override(token)

    assert result["ok"] is False
    assert any("task-state" in error or "post-run verification" in error for error in result["errors"])
    assert list(outside.iterdir()) == []


def test_fixture_lock_parent_swap_is_rejected_without_outside_lock_write(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    _make_fixture_root(tmp_path, monkeypatch)
    try:
        pnc_vm_task_sync._validate_isolated_hermes_home(tmp_path, prepare_outputs=True)
        binding = pnc_vm_task_sync._capture_fixture_output_binding(tmp_path)
        locks = tmp_path / "locks"
        outside = tmp_path / "outside-locks"
        outside.mkdir(mode=0o700)
        locks.rmdir()
        locks.symlink_to(outside, target_is_directory=True)

        with pytest.raises((OSError, ValueError)):
            with pnc_vm_task_sync.FixtureSingleRunLock(binding):
                raise AssertionError("unsafe fixture lock was acquired")
        assert list(outside.iterdir()) == []
    finally:
        reset_hermes_home_override(token)


def test_fixture_record_paths_reject_protected_ancestor_key_and_census(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    _make_fixture_root(tmp_path, monkeypatch)
    records = tmp_path / "records"
    records.mkdir(mode=0o700)
    protected = tmp_path / "synthetic-account" / ".hermes"
    protected.mkdir(parents=True, mode=0o700)
    key = protected / "record.key"
    key.write_text("ab" * 32 + "\n", encoding="ascii")
    key.chmod(0o600)
    monkeypatch.setenv("HERMES_OUTBOUND_RECORD_ROOT", str(records))
    monkeypatch.setenv("HERMES_OUTBOUND_RECORD_KEY_FILE", str(key))
    monkeypatch.setenv("HERMES_OUTBOUND_CENSUS_ROOT", str(protected / "outbound-census"))
    try:
        monkeypatch.setenv(
            pnc_vm_task_sync.FIXTURE_ISOLATION_ROOT_ENV,
            str(protected.parent),
        )
        with pytest.raises(ValueError, match="overlaps protected root"):
            pnc_vm_task_sync._validate_fixture_record_paths(
                tmp_path,
                protected_roots=(protected,),
            )
    finally:
        reset_hermes_home_override(token)


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


def test_sync_pnc_vm_tasks_publishes_nonterminal_delivery_proposal(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    try:
        store = TaskStore(tmp_path / "analytics" / "tasks.db")
        store.upsert(_task("task-running", vm_task_id="vm-running"))
        monkeypatch.setattr(
            pnc_vm_task_sync,
            "collect_vm_task_status",
            lambda task_id, **kwargs: {
                "updated_at": "2026-07-12T10:00:00+00:00",
                "state": {
                    "value": "running",
                    "summary": "downloading",
                    "terminal": False,
                },
                "artifacts": [],
                "vm_bridge": {"summary": "downloading", "state": "running"},
                "delivery_contract": {
                    "schema_version": "g1q3_delivery_contract_v1",
                    "business_state": "awaiting_download",
                    "report": {"status": "need_download", "is_deliverable": False},
                    "user_action": {"requires_user_input": False},
                },
                "errors": [],
            },
        )
        result = pnc_vm_task_sync.sync_pnc_vm_tasks(limit=10)
        body = json.loads(
            (tmp_path / "task-state" / "task-running.json").read_text(
                encoding="utf-8"
            )
        )
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True
    assert "completion_notice" not in body
    proposal = body["vm_delivery_proposal"]
    assert proposal["delivery"]["report_status"] == "in_progress"
    assert proposal["delivery_contract"]["business_state"] == "awaiting_download"
    assert proposal["chat_id"] == pnc_vm_task_sync.G1Q3_RCA_CHAT_ID
    assert proposal["thread_id"] is None
    relayed = pnc_completion_notice_relay.reconcile_vm_delivery_proposal("task-running", body)
    assert relayed["task_card"]["chat_id"] == pnc_vm_task_sync.G1Q3_RCA_CHAT_ID
    assert pnc_completion_notice_relay._card_target(relayed["task_card"]) == f"feishu:{pnc_vm_task_sync.G1Q3_RCA_CHAT_ID}"



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
    assert delivery["rca_status"]["html_link"] == ""
    assert delivery["internal_report_url"] == "http://192.168.26.174:18081/G1Q3_RCA/cases/7017699515/index.html"
    assert delivery["foxglove_url"].endswith("/7017699515/7017699515.viz.mcap")
    assert delivery["attribution_causal_text"] == "触发请求异常 -> 提前制动候选"
    assert "需要人工确认候选原因" in delivery["boundaries"][0]


def test_g1q3_completed_legacy_intake_without_report_projects_remote_read_not_done(tmp_path, monkeypatch):
    # Regression (issue 7023754183): a completed G1Q3-RCA task that only passed
    # the historical read-only gate has no report. Its old status markers must
    # project onto the current remote-read path instead of being shown as done.
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
    assert card["delivery"]["report_status"] == "in_progress"
    assert "ready_to_download" in card["delivery"]["conclusion"] or "未生成 RCA 报告" in card["delivery"]["conclusion"]
    assert "下载" not in card["delivery"]["conclusion"]
    # No fabricated clickable report link.
    assert not card["delivery"].get("artifact_path")


def test_g1q3_completed_intake_with_real_report_still_renders_done(tmp_path, monkeypatch):
    # Guard: the legacy projection must not swallow genuine completed reports.
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
            "artifacts": {"best": {
                "index_html": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/7017699515/index.html",
                "review_payload": {},
            }},
        }
        monkeypatch.setattr(pnc_vm_task_sync.vm_task_completion_probe, "_read_rca_execution_result", lambda vm_task_id, task_dir: structured)
        card = pnc_vm_task_sync._task_card_for_task(task, {
            "state": {"value": "completed", "summary": "completed", "terminal": True},
            "artifacts": [],
            "vm_bridge": {"state": "completed"},
        })
    finally:
        reset_hermes_home_override(token)
    assert card["user_state"] == "done"
    assert card["delivery"]["report_status"] == "html_delivery_ready"
    assert card["delivery"]["artifact_path"].endswith("/G1Q3_RCA/cases/7017699515/index.html")
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
    assert delivery["report_status"] == "report_ready"
    assert delivery["agent_artifact_root_vm"].startswith("/mnt/tmp/")
    assert delivery["business_case_dir_vm"].startswith("/mnt/minieye/pdcl/department/perception_test_team/")
    assert delivery["business_case_dir_cifs"].startswith("//hfs.minieye.tech/department-perception_test_team/")
    assert delivery["foxglove_url"].endswith("/7026726390_acc/7026726390_acc.viz.mcap")
    assert delivery["artifact_path"] == delivery["foxglove_url"]
    assert delivery["artifact_label"] == "打开 foxglove 可视化"
    assert delivery["attribution_causal_text"] == "实际减速度偏重 -> 纵向控制请求波动"
    assert body["vm_delivery_proposal"]["user_state"] == "done"

    relayed = pnc_completion_notice_relay.reconcile_vm_delivery_proposal(task_id, body)
    assert relayed["task_card"]["delivery"]["report_status"] == "report_ready"
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
    assert delivery["artifact_path"] == delivery["report_index_html_cifs"]
    assert delivery["publication_url_status"] == "blocked_missing_canonical_https"
    assert delivery["business_case_dir_cifs"].startswith("//hfs.minieye.tech/department-perception_test_team/")
    assert delivery["agent_artifact_root_vm"] == "/mnt/tmp/vm-contract/"
    assert "责任候选：殷莉奇" in delivery["conclusion"]
    assert "parsed/L2 assets 缺失" in delivery["boundaries"]


def test_vm_sync_html_artifact_uses_explicit_approved_internal_service(monkeypatch):
    monkeypatch.setenv(
        "PNC_FOXGLOVE_RENDER_HOST",
        "http://192.168.26.174:18081",
    )
    contract = {
        "schema_version": "g1q3_delivery_contract_v1",
        "work_item_id": "vm-sync-private-http",
        "business_state": "report_completed",
        "report": {"is_deliverable": True, "is_candidate": True},
        "summary": {"short_conclusion": "候选报告"},
        "artifacts": {
            "case_dir_vm": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/private-http",
            "index_html_vm": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/private-http/index.html",
            "primary_report_cifs": "//hfs.minieye.tech/department-perception_test_team/G1Q3_RCA/cases/private-http/index.html",
        },
        "verification": {"pipeline_status": "report_generated_need_review"},
    }

    delivery = pnc_vm_task_sync._delivery_from_contract(_task("vm-sync-private-http"), contract)

    assert delivery["artifact_path"] == (
        "http://192.168.26.174:18081/G1Q3_RCA/cases/private-http/index.html"
    )
    assert delivery["publication_url_status"] == "ready"
    assert delivery["rca_status"]["html_link"] == delivery["artifact_path"]
    assert delivery["internal_report_url"].startswith("http://192.168.26.174:18081/")


def test_vm_sync_html_artifact_rejects_unapproved_http_origin(monkeypatch):
    monkeypatch.setenv(
        "PNC_FOXGLOVE_RENDER_HOST",
        "http://192.168.26.175:18081",
    )
    contract = {
        "schema_version": "g1q3_delivery_contract_v1",
        "work_item_id": "vm-sync-unapproved-http",
        "business_state": "report_completed",
        "report": {"is_deliverable": True, "is_candidate": True},
        "summary": {"short_conclusion": "候选报告"},
        "artifacts": {
            "case_dir_vm": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/unapproved-http",
            "index_html_vm": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/unapproved-http/index.html",
            "primary_report_cifs": "//hfs.minieye.tech/department-perception_test_team/G1Q3_RCA/cases/unapproved-http/index.html",
        },
        "verification": {"pipeline_status": "report_generated_need_review"},
    }

    delivery = pnc_vm_task_sync._delivery_from_contract(
        _task("vm-sync-unapproved-http"), contract
    )

    assert delivery["artifact_path"] == delivery["report_index_html_cifs"]
    assert delivery["publication_url_status"] == "blocked_missing_canonical_https"
    assert delivery["rca_status"]["html_link"] == ""


def test_vm_sync_html_artifact_uses_explicit_canonical_https(monkeypatch):
    monkeypatch.setenv(
        "PNC_FOXGLOVE_RENDER_HOST",
        "https://g1q3-rca.minieye.tech",
    )
    submission = "g1q3-rca-s1-" + "a" * 64
    artifact_path = (
        f"/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/"
        f"{submission}/index.html"
    )
    contract = {
        "schema_version": "g1q3_delivery_contract_v1",
        "work_item_id": "vm-sync-public-https",
        "business_state": "report_completed",
        "report": {"is_deliverable": True, "is_candidate": True},
        "summary": {"short_conclusion": "候选报告"},
        "artifacts": {
            "case_dir_vm": artifact_path.rsplit("/", 1)[0],
            "index_html_vm": artifact_path,
            "primary_report_cifs": "//hfs.minieye.tech/department-perception_test_team/G1Q3_RCA/cases/" + submission + "/index.html",
        },
        "verification": {"pipeline_status": "report_generated_need_review"},
    }

    delivery = pnc_vm_task_sync._delivery_from_contract(_task("vm-sync-public-https"), contract)

    assert delivery["artifact_path"] == (
        "https://g1q3-rca.minieye.tech/G1Q3_RCA/cases/" + submission + "/index.html"
    )
    assert delivery["publication_url_status"] == "ready"
    assert delivery["rca_status"]["html_link"] == delivery["artifact_path"]


def test_g1q3_contract_foxglove_url_is_byte_identical_across_host_writers(monkeypatch):
    monkeypatch.delenv("PNC_FOXGLOVE_RENDER_HOST", raising=False)
    case_key = "g1q3-rca-s1-" + "a" * 64
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
        f"{case_key}/{case_key}.viz.mcap"
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


def test_foxglove_delivery_origin_is_fixed(monkeypatch):
    monkeypatch.setenv("PNC_FOXGLOVE_RENDER_HOST", "viewer.internal:8443")

    url = pnc_foxglove_delivery.foxglove_url(
        "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/case_a/case_a.viz.mcap"
    )

    assert url.startswith("https://192.168.21.217/?ds=foxglove-http&ds.mcapPath=")


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
    assert card["delivery"]["artifact_path"] == card["delivery"]["report_index_html_cifs"]
    assert card["delivery"]["publication_url_status"] == "blocked_missing_canonical_https"
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


def test_record_only_report_attachment_records_before_download_or_meegle_upload(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    _set_record_only_env(tmp_path, monkeypatch)

    def bomb(*_args, **_kwargs):
        raise AssertionError("real attachment download/upload was touched")

    monkeypatch.setattr("gateway.pnc_issue_context.default_meegle_runner", bomb)
    try:
        link = pnc_vm_task_sync._feishu_report_attachment_link(
            work_item_id="7017699515",
            vm_task_id="vm-attachment-7017699515",
            index_html="/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/7017699515/index.html",
        )
        transport = runtime.get_record_only_transport("scripts.pnc_vm_task_sync.report_attachment")
        assert transport is not None
        rows = transport.read_all()
    finally:
        runtime._reset_for_tests()
        reset_hermes_home_override(token)

    assert link == ""
    assert len(rows) == 1
    assert rows[0]["operation"] == "file_send"
    assert rows[0]["platform"] == "feishu_project"
    assert rows[0]["external_delivery_attempted"] is False
    assert len(rows[0]["links"]) == 1
    assert re.fullmatch(
        r"http://192\.168\.26\.174:18081/G1Q3_RCA/cases/"
        r"hmac-sha256:[0-9a-f]{64}/index\.html",
        rows[0]["links"][0],
    )
    assert "7017699515" not in rows[0]["links"][0]
    assert not (tmp_path / "pnc_agent" / "quota" / "g1q3_report_attachments.json").exists()


def test_live_legacy_report_attachment_writer_is_superseded(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    uploads = []

    def fake_upload(args):
        path = Path(args[2])
        uploads.append({
            'args': args,
            'bytes': path.read_bytes(),
        })
        return 0, json.dumps({'file_url': 'https://project.feishu.cn/new-bom-link', 'file_token': 'tok-new'}), ''

    try:
        monkeypatch.setattr('gateway.pnc_issue_context.default_meegle_runner', fake_upload)
        link = pnc_vm_task_sync._feishu_report_attachment_link(
            work_item_id='7029768863',
            vm_task_id='vm-1',
            index_html='/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/7029768863_acc/index.html',
        )
    finally:
        reset_hermes_home_override(token)

    assert link == ''
    assert uploads == []
    assert not (tmp_path / 'pnc_agent' / 'quota' / 'g1q3_report_attachments.json').exists()


def test_live_legacy_report_attachment_cache_cannot_revive_writer(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    uploads = []
    index_html = '/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/7029768863_acc/index.html'
    key = f'7029768863|{index_html}'

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
        monkeypatch.setattr('gateway.pnc_issue_context.default_meegle_runner', fake_upload)

        first = pnc_vm_task_sync._feishu_report_attachment_link(work_item_id='7029768863', vm_task_id='vm-2', index_html=index_html)
        second = pnc_vm_task_sync._feishu_report_attachment_link(work_item_id='7029768863', vm_task_id='vm-2', index_html=index_html)
        ledger = json.loads(ledger_path.read_text(encoding='utf-8'))
    finally:
        reset_hermes_home_override(token)

    assert first == ''
    assert second == ''
    assert uploads == []
    assert ledger['reports'][key].get('delivery_codec_version') is None


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


def test_historical_download_stage_projects_remote_read_in_progress():
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
    assert delivery["report_status"] == "in_progress"
    assert delivery["human_action_kind"] == "none"
    assert "远程读取问题数据中" in delivery["conclusion"]
    assert "数据下载执行中" not in delivery["conclusion"]
    assert "pipeline running" not in delivery["conclusion"]
    assert delivery["source"] == "delivery_contract_v1"


def test_l4_vm_sync_uses_sealed_event_clock(monkeypatch):
    monkeypatch.setenv("HERMES_OUTBOUND_MODE", "record-only")
    monkeypatch.setenv("HERMES_L4_SANDBOX_ACTIVE", "1")
    monkeypatch.setenv("HERMES_L4_EVENT_EPOCH", "1783850400")

    assert pnc_vm_task_sync._now_epoch() == 1783850400.0
    assert pnc_vm_task_sync._now_iso() == "2026-07-12T18:00:00+08:00"
