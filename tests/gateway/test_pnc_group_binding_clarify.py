"""#17: template-less message in the bound G1Q3 group becomes a button-clarify
instead of a blunt reject (owner-approved conservative boundary, 2026-06-20).

Pure策略层 — no Feishu transport. Locks: clarify decision + options, and that
real RCA requests / specific rejections are NOT swallowed by the new branch.
"""
from gateway.pnc_group_binding import (
    evaluate_pnc_group_request,
    G1Q3_RCA_GROUP_ID,
    _RCA_CLARIFY_OPTIONS,
)


def _eval(text, chat=G1Q3_RCA_GROUP_ID):
    return evaluate_pnc_group_request(platform="feishu", chat_id=chat, text=text)


def test_template_less_message_clarifies_not_rejects():
    d = _eval("你好呀，在吗")
    assert d.decision == "clarify"
    assert d.reason == "unsupported_task_template"
    assert d.clarify_options == _RCA_CLARIFY_OPTIONS
    assert len(d.clarify_options) == 3


def test_clarify_options_are_the_three_rca_templates():
    d = _eval("帮个忙")
    assert d.decision == "clarify"
    ids = [o[0] for o in d.clarify_options]
    assert ids == ["rca_case_status_check", "rca_issue_intake", "rca_case_evidence_summary"]
    # every option carries a non-empty human label
    assert all(label.strip() for _, label in d.clarify_options)


def test_real_rca_request_still_routes_not_clarified():
    # has case + status keyword -> accepted/dry_run, must NOT be hijacked to clarify
    d = _eval("G1Q3-1234 现在做到哪一步了")
    assert d.decision != "clarify"


def test_specific_rejection_still_rejects_not_clarified():
    # cross-business-line signal -> reject; the clarify branch must not swallow it
    d = _eval("评测门禁的事在这个群处理一下")
    assert d.decision == "reject"
    assert d.clarify_options is None


def test_non_pnc_group_is_untouched():
    # unrelated chat -> allow (no clarify, no card)
    d = _eval("你好呀，在吗", chat="oc_some_other_random_group")
    assert d.decision == "allow"
    assert d.clarify_options is None
