import json
from datetime import datetime, timezone

from gateway.pnc_field_gap_comment import (
    build_field_gap_comment,
    maybe_comment_field_gap,
)


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


NOW = datetime(2026, 6, 12, 12, 0, tzinfo=timezone.utc)


def test_build_comment_covers_supported_kinds_and_names_owner():
    plan = build_field_gap_comment("issue_field_missing_pdcl_download_cmd", ["张三", "李四"])
    assert "缺少 问题数据地址_PDCL" in plan["signature"]
    assert "张三、李四" in plan["content"]
    assert "mdi download event -u" in plan["content"]
    assert "mdi refresh -t" in plan["content"]
    assert "pdcl_mdi_data_download_guide.md" in plan["content"]

    assert build_field_gap_comment("missing_frame_id")["signature"] == "缺少 问题发生frameid"
    assert build_field_gap_comment("host_meegle_preread_unauthenticated") is None


def test_disabled_by_default_returns_plan_only(tmp_path, monkeypatch):
    monkeypatch.delenv("HERMES_G1Q3_FIELD_GAP_COMMENT", raising=False)
    runner = _runner()

    result = maybe_comment_field_gap(
        project_key="t03o4q", work_item_id="7015828844",
        blocker_kind="issue_field_missing_pdcl_download_cmd",
        ledger_dir=tmp_path, meegle_runner=runner, now=NOW,
    )

    assert result["action"] == "planned"
    assert "问题数据地址_PDCL" in result["comment_content"]
    assert runner.calls == []


def test_enabled_posts_comment_and_records_ledger(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_G1Q3_FIELD_GAP_COMMENT", "1")
    runner = _runner()

    result = maybe_comment_field_gap(
        project_key="t03o4q", work_item_id="7015828844",
        blocker_kind="missing_frame_id", owners=["邵祖钦"],
        ledger_dir=tmp_path, meegle_runner=runner, now=NOW,
    )

    assert result["action"] == "posted"
    add_call = [c for c in runner.calls if c[:2] == ["comment", "add"]][0]
    assert add_call[add_call.index("--work-item-id") + 1] == "7015828844"
    assert "问题发生frameid" in add_call[add_call.index("--content") + 1]
    ledger = json.loads((tmp_path / "g1q3_field_gap_comments.json").read_text())
    assert "7015828844:missing_frame_id" in ledger["issues"]
    assert ledger["daily"]["2026-06-12"] == 1


def test_repeat_within_window_is_skipped(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_G1Q3_FIELD_GAP_COMMENT", "1")
    runner = _runner()

    first = maybe_comment_field_gap(
        project_key="t03o4q", work_item_id="7015828844", blocker_kind="missing_frame_id",
        ledger_dir=tmp_path, meegle_runner=runner, now=NOW,
    )
    second = maybe_comment_field_gap(
        project_key="t03o4q", work_item_id="7015828844", blocker_kind="missing_frame_id",
        ledger_dir=tmp_path, meegle_runner=runner, now=NOW,
    )

    assert first["action"] == "posted"
    assert second["action"] == "skipped_recent"
    assert len([c for c in runner.calls if c[:2] == ["comment", "add"]]) == 1


def test_existing_comment_signature_prevents_posting(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_G1Q3_FIELD_GAP_COMMENT", "1")
    runner = _runner(existing_comments_text="【G1Q3 RCA 机器人提醒】…缺少 问题发生frameid…")

    result = maybe_comment_field_gap(
        project_key="t03o4q", work_item_id="7015828844", blocker_kind="missing_frame_id",
        ledger_dir=tmp_path, meegle_runner=runner, now=NOW,
    )

    assert result["action"] == "skipped_existing"
    assert [c for c in runner.calls if c[:2] == ["comment", "add"]] == []


def test_daily_cap_blocks_further_posts(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_G1Q3_FIELD_GAP_COMMENT", "1")
    monkeypatch.setenv("HERMES_G1Q3_FIELD_GAP_COMMENT_DAILY_CAP", "1")
    runner = _runner()

    first = maybe_comment_field_gap(
        project_key="t03o4q", work_item_id="700001", blocker_kind="missing_frame_id",
        ledger_dir=tmp_path, meegle_runner=runner, now=NOW,
    )
    second = maybe_comment_field_gap(
        project_key="t03o4q", work_item_id="700002", blocker_kind="missing_frame_id",
        ledger_dir=tmp_path, meegle_runner=runner, now=NOW,
    )

    assert first["action"] == "posted"
    assert second["action"] == "skipped_cap"


def test_unreadable_comment_list_fails_closed(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_G1Q3_FIELD_GAP_COMMENT", "1")

    def broken(args):
        if args[:2] == ["comment", "list"]:
            return 1, "", "permission denied"
        raise AssertionError("must not post without dedup check")

    result = maybe_comment_field_gap(
        project_key="t03o4q", work_item_id="7015828844", blocker_kind="missing_frame_id",
        ledger_dir=tmp_path, meegle_runner=broken, now=NOW,
    )

    assert result["action"] == "comment_failed"
    assert "dedup" in result["reason"]


def test_post_failure_is_reported_not_raised(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_G1Q3_FIELD_GAP_COMMENT", "1")
    runner = _runner(add_rc=1)

    result = maybe_comment_field_gap(
        project_key="t03o4q", work_item_id="7015828844", blocker_kind="missing_frame_id",
        ledger_dir=tmp_path, meegle_runner=runner, now=NOW,
    )

    assert result["action"] == "comment_failed"
    assert "boom" in result["reason"]


def test_invalid_pdcl_subkind_templates_are_specific_and_can_real_at():
    nas = build_field_gap_comment(
        "issue_field_invalid_pdcl_download_cmd",
        ["张三"],
        sub_kind="nas_path",
        owner_open_ids=["ou_owner"],
    )
    replay = build_field_gap_comment(
        "issue_field_invalid_pdcl_download_cmd",
        ["张三"],
        sub_kind="replay_cmd",
        owner_open_ids=["ou_owner"],
    )

    assert nas["content"] != replay["content"]
    assert "NAS" in nas["content"]
    assert "NAS staging" in nas["content"]
    assert "cyber_recorder" in replay["content"]
    assert "不是数据下载命令" in replay["content"]
    assert '<at user_id="ou_owner">张三</at>' in nas["content"]
    assert '<at user_id="ou_owner">张三</at>' in replay["content"]


def test_invalid_pdcl_empty_subkind_uses_missing_download_command_copy():
    plan = build_field_gap_comment(
        "issue_field_invalid_pdcl_download_cmd",
        ["张三"],
        sub_kind="empty",
        owner_open_ids=["ou_owner"],
    )

    assert "缺少 问题数据地址_PDCL 下载命令" in plan["signature"]
    assert "缺少 问题数据地址_PDCL 下载命令" in plan["content"]
    assert "补充" in plan["content"]
    assert "mdi download event -u" in plan["content"]
    assert "mdi refresh -t" in plan["content"]
    assert '<at user_id="ou_owner">张三</at>' in plan["content"]


def test_owner_open_id_missing_falls_back_to_name_without_raising():
    plan = build_field_gap_comment(
        "issue_field_invalid_pdcl_download_cmd",
        ["李四"],
        sub_kind="replay_cmd",
        owner_open_ids=[],
    )
    assert "李四" in plan["content"]
    assert "<at user_id=" not in plan["content"]
