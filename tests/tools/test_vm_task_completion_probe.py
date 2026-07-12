from pathlib import Path

from tools import vm_task_completion_probe as probe


def test_g1q3_task_label_prefers_case_id():
    label = probe._g1q3_task_label(
        {"pnc_group_binding": {"handoff_contract": {"case_id": "6028", "work_item_id": "7008267126"}}},
        "task-1",
    )
    assert label == "G1Q3-6028"


def test_g1q3_task_label_falls_back_to_work_item():
    label = probe._g1q3_task_label(
        {"pnc_group_binding": {"handoff_contract": {"case_id": "", "work_item_id": "7008267126"}}},
        "task-1",
    )
    assert label == "飞书问题 7008267126"


def test_g1q3_task_label_falls_back_to_issue_intake_title():
    label = probe._g1q3_task_label(
        {"title": "G1Q3 RCA issue intake: 7008267126"},
        "20260605-151340-g1q3-rca-issue-intake-7008267126",
    )
    assert label == "飞书问题 7008267126"


def test_g1q3_l1_notice_uses_artifact_json_summary(tmp_path):
    root = tmp_path / "shared-state"
    task_id = "20260605-151340-g1q3-rca-issue-intake-7008267126"
    task_dir = root / "tasks" / task_id
    task_dir.mkdir(parents=True)
    (task_dir / "meta.json").write_text(
        '{"title":"G1Q3 RCA issue intake: 7008267126","pnc_group_binding":{"handoff_contract":{"work_item_id":"7008267126"}}}',
        encoding="utf-8",
    )

    worker_artifacts = root.parent / "worker-state" / "tasks" / task_id / "artifacts"
    worker_artifacts.mkdir(parents=True)
    (worker_artifacts / "codex-last-message.txt").write_text(
        '{"result":{"L1":["已发现正式 RCA 产物","建议人工确认目标切换时序"]}}',
        encoding="utf-8",
    )

    notice = probe._g1q3_l1_notice(task_id, task_dir, "completed")
    assert notice is not None
    assert notice.startswith("飞书问题 7008267126 RCA 检查完成（L1）：")
    assert "已发现正式 RCA 产物" in notice
    assert "建议人工确认目标切换时序" in notice


def test_g1q3_l1_notice_prefers_structured_execution_result(tmp_path):
    root = tmp_path / "shared-state"
    task_id = "g1q3_rca_issue_intake_7008267126"
    task_dir = root / "tasks" / task_id
    task_dir.mkdir(parents=True)
    (task_dir / "meta.json").write_text(
        '{"title":"G1Q3 RCA issue intake: 7008267126","pnc_group_binding":{"handoff_contract":{"work_item_id":"7008267126"}}}',
        encoding="utf-8",
    )
    (task_dir / "rca_execution_result.json").write_text(
        '{"schema_version":"g1q3_rca_execution_result_v1","status":"need_evidence","readback":{"safe_for_group":true,"text":"L0 line\\n\\n- L1 line"}}',
        encoding="utf-8",
    )
    worker_artifacts = root.parent / "worker-state" / "tasks" / task_id / "artifacts"
    worker_artifacts.mkdir(parents=True)
    (worker_artifacts / "codex-last-message.txt").write_text(
        '{"result":{"L1":["legacy line should not win"]}}',
        encoding="utf-8",
    )

    notice = probe._g1q3_l1_notice(task_id, task_dir, "completed")

    assert notice is not None
    assert notice.startswith("飞书问题 7008267126 RCA 检查完成：")
    assert "L0 line" in notice
    assert "L1 line" in notice
    assert "legacy line" not in notice


def test_g1q3_l1_notice_blocks_unsafe_structured_execution_result(tmp_path):
    root = tmp_path / "shared-state"
    task_id = "g1q3_rca_issue_intake_7008267126"
    task_dir = root / "tasks" / task_id
    task_dir.mkdir(parents=True)
    (task_dir / "meta.json").write_text('{"title":"G1Q3 RCA issue intake: 7008267126"}', encoding="utf-8")
    (task_dir / "rca_execution_result.json").write_text(
        '{"schema_version":"g1q3_rca_execution_result_v1","readback":{"safe_for_group":false,"text":"do not send"}}',
        encoding="utf-8",
    )

    assert probe._g1q3_l1_notice(task_id, task_dir, "completed") is None

def test_g1q3_l1_notice_falls_back_to_summary_markdown(tmp_path):
    root = tmp_path / "shared-state"
    task_id = "20260605-151340-g1q3-rca-issue-intake-7008267126"
    task_dir = root / "tasks" / task_id
    task_dir.mkdir(parents=True)
    (task_dir / "meta.json").write_text(
        '{"title":"G1Q3 RCA issue intake: 7008267126","pnc_group_binding":{"handoff_contract":{"work_item_id":"7008267126"}}}',
        encoding="utf-8",
    )
    (task_dir / "result.md").write_text(
        'artifact: /mnt/tmp/g1q3_rca_issue_intake_7008267126/L0_L1_issue_intake_summary.md',
        encoding="utf-8",
    )

    mounts_root = Path.home() / "Mounts" / "department-pnc_team-planning_algo-driving" / "tmp" / "g1q3_rca_issue_intake_7008267126"
    mounts_root.mkdir(parents=True, exist_ok=True)
    summary_path = mounts_root / "L0_L1_issue_intake_summary.md"
    summary_path.write_text(
        '# summary\n\n## L1\n- issue L1 line one\n- issue L1 line two\n',
        encoding="utf-8",
    )
    try:
        notice = probe._g1q3_l1_notice(task_id, task_dir, "completed")
        assert notice is not None
        assert notice.startswith("飞书问题 7008267126 RCA 检查完成（L1）：")
        assert "issue L1 line one" in notice
        assert "issue L1 line two" in notice
    finally:
        summary_path.unlink(missing_ok=True)
        try:
            mounts_root.rmdir()
        except OSError:
            pass
