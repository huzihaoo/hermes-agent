import json

import pytest

from gateway.feishu_mention import (
    build_at_mention,
    build_need_input_notify_text,
    compute_notify_key,
    resolve_display_name,
    resolve_originator_open_id,
)


def test_resolve_originator_prefers_requester_then_user_id():
    assert resolve_originator_open_id({"requester": "ou_aaa"}) == "ou_aaa"
    assert resolve_originator_open_id({"user_id": "ou_bbb"}) == "ou_bbb"
    assert resolve_originator_open_id({"source": {"user_id": "ou_ccc"}}) == "ou_ccc"


def test_resolve_originator_skips_non_open_id_session_keys():
    # integration_tools meta carries requester_session_key="agent:main:main";
    # that must never be treated as a mention target.
    assert resolve_originator_open_id({"requester": "agent:main:main"}) == ""
    assert resolve_originator_open_id({"requester": "", "user_id": "ou_real"}) == "ou_real"
    assert resolve_originator_open_id({}) == ""
    assert resolve_originator_open_id(None) == ""


def test_build_at_mention_format_and_empty():
    assert build_at_mention("ou_x", "刘旭") == '<at user_id="ou_x">刘旭</at>'
    assert build_at_mention("ou_x") == '<at user_id="ou_x"></at>'
    assert build_at_mention("") == ""


def test_resolve_display_name_reads_user_id_mapping(tmp_path, monkeypatch):
    roles = tmp_path / "user-roles.json"
    roles.write_text(json.dumps({"user_id_mapping": {"ou_e37d": "刘旭", "ou_d1d3": "胡子豪"}}), encoding="utf-8")
    monkeypatch.setattr("gateway.feishu_mention._roles_path", lambda: roles)
    # bust the mtime-keyed lru cache for the test file
    from gateway import feishu_mention
    feishu_mention._load_user_id_mapping_cached.cache_clear()
    assert resolve_display_name("ou_e37d") == "刘旭"
    assert resolve_display_name("ou_d1d3") == "胡子豪"
    assert resolve_display_name("ou_unknown") == ""
    assert resolve_display_name("") == ""


def test_resolve_display_name_missing_file_is_safe(tmp_path, monkeypatch):
    monkeypatch.setattr("gateway.feishu_mention._roles_path", lambda: tmp_path / "nope.json")
    from gateway import feishu_mention
    feishu_mention._load_user_id_mapping_cached.cache_clear()
    assert resolve_display_name("ou_x") == ""


def test_compute_notify_key_changes_on_new_transition():
    a = compute_notify_key(user_state="awaiting_user", transition_marker="need_input|triage missing fields")
    b = compute_notify_key(user_state="awaiting_user", transition_marker="need_input|owner rejected, re-run")
    assert a != b  # new state-write summary => re-ping
    same = compute_notify_key(user_state="awaiting_user", transition_marker="need_input|triage missing fields")
    assert a == same  # unchanged transition => no re-ping


def test_compute_notify_key_distinguishes_confirms():
    base = compute_notify_key(user_state="awaiting_user", transition_marker="t", pending_confirm_ids=["c1"])
    more = compute_notify_key(user_state="awaiting_user", transition_marker="t", pending_confirm_ids=["c1", "c2"])
    assert base != more


def test_notify_text_is_markdown_free_and_mentions():
    text = build_need_input_notify_text(mention='<at user_id="ou_x">刘旭</at>', reason="补 mcap 路径", task_id="t-1")
    assert '<at user_id="ou_x">刘旭</at>' in text
    assert "补 mcap 路径" in text
    assert "追踪号 t-1" in text
    # must not contain markdown lead chars that would reroute to post-type
    for line in text.splitlines():
        assert not line.lstrip().startswith(("- ", "* ", "#", "> ", "`"))


def test_notify_text_strips_markdown_that_would_break_at_mention():
    import re
    HINT = re.compile(r"(^#{1,6}\s)|(^\s*[-*]\s)|(^\s*\d+\.\s)|(```)|(`[^`\n]+`)|(\*\*[^*\n].+?\*\*)|(\[[^\]]+\]\([^)]+\))|(^>\s)", re.M)
    # Reasons that quote paths/fields with backticks/bold must not trip the
    # Feishu markdown router (which would mangle the <at> tag).
    for reason in ["补 `mcap` 路径和 **owner**", "缺: issue_ref - 版本 - 验收人", "## 标题\n- 路径\n- owner"]:
        text = build_need_input_notify_text(mention='<at user_id="ou_x">刘旭</at>', reason=reason, task_id="t-1")
        assert '<at user_id="ou_x">刘旭</at>' in text
        assert not HINT.search(text), f"markdown router would trip on: {reason!r} -> {text!r}"
        assert "`" not in text and "**" not in text


def test_notify_text_variants():
    ab = build_need_input_notify_text(mention="@x", reason="", task_id="", kind="abandoned")
    assert "超时" in ab and "追踪号" not in ab
    cf = build_need_input_notify_text(mention="@x", reason="选A还是B", task_id="t", kind="confirm")
    assert "确认" in cf and "选A还是B" in cf


def test_notify_text_handles_missing_originator():
    text = build_need_input_notify_text(mention="", reason="补路径", task_id="t-1")
    assert "未识别到发起人" in text


def test_notify_key_ignores_passive_writes_keys_on_transition_only():
    # The crux fix: notify dedup must NOT key on a write timestamp.  Two passes
    # whose only difference is a passive bump (same state, same summary) must
    # produce the same key; only a real transition (new summary/state) differs.
    base = compute_notify_key(user_state="awaiting_user", transition_marker="need_input|missing input path")
    passive_same = compute_notify_key(user_state="awaiting_user", transition_marker="need_input|missing input path")
    real_transition = compute_notify_key(user_state="abandoned", transition_marker="abandoned|need_input -> abandoned")
    assert base == passive_same
    assert base != real_transition
