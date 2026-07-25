import json
from datetime import datetime, timezone
from pathlib import Path

from gateway.pnc_report_comment import build_report_comment, is_after_not_before, maybe_comment_report_ready
from gateway.record_only import runtime

NOW = datetime(2026, 6, 12, 12, 0, tzinfo=timezone.utc)


def _runner(existing_comments_text="", add_rc=0):
    calls = []
    def run(args):
        calls.append(args)
        if args[:2] == ["comment", "list"]:
            return 0, json.dumps({"comments": [{"content": existing_comments_text}]}, ensure_ascii=False), ""
        if args[:2] == ["comment", "add"]:
            return add_rc, "ok" if add_rc == 0 else "", "boom" if add_rc else ""
        return 1, "", "unexpected"
    run.calls = calls
    return run


def _record_only_env(tmp_path: Path, monkeypatch):
    root = tmp_path / "records"
    root.mkdir(mode=0o700)
    key_file = tmp_path / "record.key"
    key_file.write_text("ab" * 32 + "\n", encoding="ascii")
    key_file.chmod(0o600)
    census_root = Path(__file__).resolve().parents[3] / "evidence" / "target-outbound-census"
    monkeypatch.setenv("HERMES_OUTBOUND_MODE", "record-only")
    monkeypatch.setenv("HERMES_OUTBOUND_RECORD_ROOT", str(root))
    monkeypatch.setenv("HERMES_OUTBOUND_RECORD_KEY_FILE", str(key_file))
    monkeypatch.setenv("HERMES_OUTBOUND_CENSUS_ROOT", str(census_root))
    monkeypatch.setenv("HERMES_G1Q3_REPORT_COMMENT", "1")
    runtime._reset_for_tests()
    return root


def test_build_report_comment_contains_compact_causal_result():
    plan = build_report_comment(
        work_item_id="7017699515",
        title="AWB 误触发",
        rca_status={
            "candidate_cause": "TTC/gate 风险上下文不足",
            "candidate_responsibility": "刘培瑞",
            "causal_chain": "目标风险证据不足，无法确认触发条件。",
            "evidence": "TTC/gate 关键字段缺少有效值。",
        },
        boundaries=["需人工复核"],
    )
    assert "G1Q3 RCA 报告已生成" in plan["signature"]
    assert "7017699515" in plan["content"]
    assert "归因结论：TTC/gate 风险上下文不足" in plan["content"]
    assert "责任模块：刘培瑞" in plan["content"]
    assert "因果关系：目标风险证据不足" in plan["content"]
    assert "关键证据：TTC/gate 关键字段缺少有效值" in plan["content"]
    assert "报告状态：" not in plan["content"]
    assert "HTML 报告：" not in plan["content"]


def test_disabled_is_plan_only(tmp_path, monkeypatch):
    monkeypatch.delenv("HERMES_G1Q3_REPORT_COMMENT", raising=False)
    runner = _runner()
    res = maybe_comment_report_ready(
        project_key="t03o4q", work_item_id="701", task_id="t1", rca_status={}, ledger_dir=tmp_path, meegle_runner=runner, now=NOW,
    )
    assert res["action"] == "planned"
    assert runner.calls == []


def test_enabled_posts_and_dedups(tmp_path, monkeypatch):
    monkeypatch.delenv("HERMES_OUTBOUND_MODE", raising=False)
    runtime._reset_for_tests()
    monkeypatch.setenv("HERMES_G1Q3_REPORT_COMMENT", "1")
    runner = _runner()
    first = maybe_comment_report_ready(
        project_key="t03o4q", work_item_id="701", task_id="t1", rca_status={"html_link":"https://project.feishu.cn/goapi/v5/platform/file/stream/download/token-x"}, ledger_dir=tmp_path, meegle_runner=runner, now=NOW,
    )
    second = maybe_comment_report_ready(
        project_key="t03o4q", work_item_id="701", task_id="t2", rca_status={"html_link":"https://project.feishu.cn/goapi/v5/platform/file/stream/download/token-x"}, ledger_dir=tmp_path, meegle_runner=runner, now=NOW,
    )
    assert first["action"] == "posted"
    assert second["action"] == "skipped_recent"
    adds = [c for c in runner.calls if c[:2] == ["comment", "add"]]
    assert len(adds) == 1
    assert adds[0][adds[0].index("--work-item-id") + 1] == "701"


def test_partial_record_only_config_fails_before_meegle_runner_or_comment_ledger(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_OUTBOUND_MODE", "record-only")
    monkeypatch.setenv("HERMES_OUTBOUND_RECORD_ROOT", str(tmp_path / "records"))
    monkeypatch.delenv("HERMES_OUTBOUND_RECORD_KEY_FILE", raising=False)
    monkeypatch.setenv("HERMES_G1Q3_REPORT_COMMENT", "1")
    (tmp_path / "records").mkdir(mode=0o700)
    runtime._reset_for_tests()

    def bomb_runner(_args):
        raise AssertionError("real Meegle runner was touched")

    result = maybe_comment_report_ready(
        project_key="t03o4q",
        work_item_id="701",
        task_id="t1",
        rca_status={},
        ledger_dir=tmp_path / "comment-ledger",
        meegle_runner=bomb_runner,
        now=NOW,
    )
    runtime._reset_for_tests()

    assert result["action"] == "comment_failed"
    assert "KEY_FILE" in result["reason"]
    assert not (tmp_path / "comment-ledger" / "g1q3_report_comments.json").exists()


def test_existing_signature_prevents_post(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_G1Q3_REPORT_COMMENT", "1")
    runner = _runner(existing_comments_text="G1Q3 RCA 报告已生成，需人工复核后结案。")
    res = maybe_comment_report_ready(
        project_key="t03o4q", work_item_id="701", task_id="t1", rca_status={}, ledger_dir=tmp_path, meegle_runner=runner, now=NOW,
    )
    assert res["action"] == "skipped_existing"
    assert [c for c in runner.calls if c[:2] == ["comment", "add"]] == []


def test_cap_and_comment_list_fail_closed(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_G1Q3_REPORT_COMMENT", "1")
    monkeypatch.setenv("HERMES_G1Q3_REPORT_COMMENT_DAILY_CAP", "0")
    res = maybe_comment_report_ready(
        project_key="t03o4q", work_item_id="701", task_id="t1", rca_status={}, ledger_dir=tmp_path, meegle_runner=_runner(), now=NOW,
    )
    assert res["action"] == "skipped_cap"

    monkeypatch.setenv("HERMES_G1Q3_REPORT_COMMENT_DAILY_CAP", "50")
    def broken(args):
        if args[:2] == ["comment", "list"]:
            return 1, "", "no auth"
        raise AssertionError("must not post")
    res2 = maybe_comment_report_ready(
        project_key="t03o4q", work_item_id="702", task_id="t1", rca_status={}, ledger_dir=tmp_path, meegle_runner=broken, now=NOW,
    )
    assert res2["action"] == "comment_failed"
    assert "dedup" in res2["reason"]


def test_not_before_gate(monkeypatch):
    monkeypatch.setenv("HERMES_G1Q3_REPORT_COMMENT_NOT_BEFORE", "2026-06-12T12:00:00+00:00")
    assert is_after_not_before("2026-06-12T12:00:00+00:00") is True
    assert is_after_not_before("2026-06-12T11:59:59+00:00") is False


def test_record_only_records_list_and_conditional_add_without_runner_or_ledger(tmp_path, monkeypatch):
    root = _record_only_env(tmp_path, monkeypatch)

    def bomb_runner(_args):
        raise AssertionError("real Meegle runner was touched")

    ledger_dir = tmp_path / "comment-ledger"
    kwargs = dict(
        project_key="t03o4q",
        work_item_id="7017699515",
        task_id="g1q3-task-7017699515",
        title="中文 RCA",
        rca_status={
            "html_link": "https://project.feishu.cn/report/index.html",
            "candidate_cause": "纵向控制请求波动",
        },
        boundaries=[
            "领取路径 //hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/g1q3-task/"
        ],
        ledger_dir=ledger_dir,
        meegle_runner=bomb_runner,
        now=NOW,
    )
    try:
        first = maybe_comment_report_ready(**kwargs)
        second = maybe_comment_report_ready(**kwargs)
        transport = runtime.get_record_only_transport("gateway.pnc_report_comment")
        assert transport is not None
        rows = transport.read_all()
    finally:
        runtime._reset_for_tests()

    assert first["action"] == "recorded_intents"
    assert first["posted"] is False
    assert first["external_delivery_attempted"] is False
    assert first["duplicate"] is False
    assert second["action"] == "recorded_intents"
    assert second["duplicate"] is True
    assert not (ledger_dir / "g1q3_report_comments.json").exists()
    assert [row["operation"] for row in rows] == [
        "project_comment_list",
        "project_comment_add",
    ]
    assert [row["attempt_count"] for row in rows] == [2, 2]
    assert rows[1]["metadata"]["conditional_on"] == "comment_signature_absent"
    assert rows[1]["links"] == [
        "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/g1q3-task/",
        "https://project.feishu.cn/report/index.html",
    ]
    serialized = json.dumps(rows, ensure_ascii=False)
    assert "7017699515" not in serialized
    assert "中文 RCA" in serialized
