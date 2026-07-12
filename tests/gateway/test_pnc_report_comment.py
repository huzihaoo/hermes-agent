import json
from datetime import datetime, timezone

from gateway.pnc_report_comment import build_report_comment, is_after_not_before, maybe_comment_report_ready

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


def test_build_report_comment_contains_rca_and_html():
    plan = build_report_comment(
        work_item_id="7017699515",
        title="AWB 误触发",
        rca_status={
            "report_status": "html_delivery_ready",
            "attribution_status": "hypothesis_ready",
            "candidate_cause": "TTC/gate 风险上下文不足",
            "candidate_responsibility": "刘培瑞",
            "html_link": "https://project.feishu.cn/goapi/v5/platform/file/stream/download/token701",
        },
        boundaries=["需人工复核"],
    )
    assert "G1Q3 RCA 报告已生成" in plan["signature"]
    assert "7017699515" in plan["content"]
    assert "hypothesis_ready" in plan["content"]
    assert "https://project.feishu.cn/goapi/v5/platform/file/stream/download/token701" in plan["content"]


def test_disabled_is_plan_only(tmp_path, monkeypatch):
    monkeypatch.delenv("HERMES_G1Q3_REPORT_COMMENT", raising=False)
    runner = _runner()
    res = maybe_comment_report_ready(
        project_key="t03o4q", work_item_id="701", task_id="t1", rca_status={}, ledger_dir=tmp_path, meegle_runner=runner, now=NOW,
    )
    assert res["action"] == "planned"
    assert runner.calls == []


def test_enabled_posts_and_dedups(tmp_path, monkeypatch):
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
