"""#17 transport: pnc clarify card construction and safe callback retirement.

GatewayRunner._send_pnc_clarify_card builds the legacy 3-button card. The
callback no longer mutates that card because canonical RCA replies must cross
the activation-bound send fence. Bare
instances (__new__) avoid full gateway/SDK init. Feishu end-to-end render + the
real card callback still needs a test-group human click (noted in HANDOFF).
"""
import asyncio
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import gateway.platforms.feishu as feishu_module
from gateway.run import GatewayRunner, Platform
from gateway.platforms.feishu import (
    G1Q3_RCA_GROUP_ID,
    PNC_ALL_BUSINESS_TEST_GROUP_ID,
    FeishuAdapter,
)
from gateway.pnc_rca_provider_fence import build_write_fence_provider_claim
from gateway.pnc_group_binding import PncGroupBindingDecision, _RCA_CLARIFY_OPTIONS


def _runner():
    return GatewayRunner.__new__(GatewayRunner)


def _adapter():
    return FeishuAdapter.__new__(FeishuAdapter)


def test_send_clarify_card_builds_three_button_card():
    runner = _runner()
    captured = {}

    async def fake_send(adapter, chat_id, card, *, reply_to=None, metadata=None):
        captured.update(card=card, chat_id=chat_id, reply_to=reply_to)
        return True

    runner._send_feishu_interactive_card = fake_send
    runner.adapters = {"FEISHU_KEY": SimpleNamespace(platform=Platform.FEISHU)}
    source = SimpleNamespace(platform="FEISHU_KEY", chat_id="oc_x")
    event = SimpleNamespace(message_id="m1")
    decision = PncGroupBindingDecision(
        decision="clarify", user_message="选一个", clarify_options=_RCA_CLARIFY_OPTIONS,
    )

    assert asyncio.run(runner._send_pnc_clarify_card(source, event, decision)) is True
    card = captured["card"]
    assert captured["chat_id"] == "oc_x"
    assert captured["reply_to"] == "m1"
    actions = [e for e in card["elements"] if e.get("tag") == "action"][0]["actions"]
    assert len(actions) == 3
    assert all(b["value"]["hermes_action"] == "rca_clarify" for b in actions)
    assert [b["value"]["choice"] for b in actions] == [
        "rca_case_status_check", "rca_issue_intake", "rca_case_evidence_summary",
    ]
    assert any("选一个" in e.get("content", "") for e in card["elements"] if e.get("tag") == "markdown")


def test_send_clarify_card_non_feishu_returns_false():
    runner = _runner()
    runner.adapters = {"OTHER": SimpleNamespace(platform="not-feishu")}
    source = SimpleNamespace(platform="OTHER", chat_id="c")
    event = SimpleNamespace(message_id="m")
    decision = PncGroupBindingDecision(decision="clarify", clarify_options=_RCA_CLARIFY_OPTIONS)
    assert asyncio.run(runner._send_pnc_clarify_card(source, event, decision)) is False


def test_send_clarify_card_no_options_returns_false():
    runner = _runner()
    runner.adapters = {"FEISHU_KEY": SimpleNamespace(platform=Platform.FEISHU)}
    source = SimpleNamespace(platform="FEISHU_KEY", chat_id="c")
    event = SimpleNamespace(message_id="m")
    decision = PncGroupBindingDecision(decision="clarify", clarify_options=None)
    assert asyncio.run(runner._send_pnc_clarify_card(source, event, decision)) is False


def test_handler_suppresses_all_legacy_choice_mutations():
    a = _adapter()
    cases = {
        "rca_case_status_check": "case 状态",
        "rca_issue_intake": "问题",
        "rca_case_evidence_summary": "证据",
    }
    for choice in cases:
        resp = a._handle_rca_clarify_card_action(event=None, action_value={"choice": choice})
        assert getattr(resp, "card", None) is None


def test_handler_unknown_choice_is_also_suppressed():
    a = _adapter()
    resp = a._handle_rca_clarify_card_action(event=None, action_value={"choice": "bogus"})
    assert getattr(resp, "card", None) is None


class _CallbackResponse:
    def __init__(self):
        self.card = None


@pytest.mark.parametrize(
    "action_value",
    [
        {"hermes_action": "repo_acl_approve"},
        {"hermes_action": "task_confirm"},
        {"hermes_action": "intake_clarify"},
        {"hermes_action": "rca_clarify"},
        {"hermes_action": "clarify"},
        {"hermes_action": "approve_once"},
        {"hermes_update_prompt_action": "y"},
        {},
    ],
)
def test_all_rca_chat_callback_families_stop_before_handler_without_epoch(
    monkeypatch, action_value
):
    monkeypatch.setattr(feishu_module, "P2CardActionTriggerResponse", _CallbackResponse)
    adapter = _adapter()
    adapter._loop = SimpleNamespace(is_closed=lambda: False)
    bomb = Mock(side_effect=AssertionError("callback handler must not run"))
    for name in (
        "_handle_repo_acl_card_action",
        "_handle_task_confirm_card_action",
        "_handle_intake_clarify_card_action",
        "_handle_rca_clarify_card_action",
        "_handle_clarify_card_action",
        "_handle_approval_card_action",
        "_handle_update_prompt_card_action",
        "_submit_on_loop",
    ):
        setattr(adapter, name, bomb)
    data = SimpleNamespace(
        event=SimpleNamespace(
            context=SimpleNamespace(open_chat_id=G1Q3_RCA_GROUP_ID),
            action=SimpleNamespace(value=action_value),
        )
    )

    response = adapter._on_card_action_trigger(data)

    assert response.card is None
    bomb.assert_not_called()


def test_all_business_rca_task_callback_revalidates_and_blocks_revoked_epoch(
    monkeypatch,
):
    monkeypatch.setattr(feishu_module, "P2CardActionTriggerResponse", _CallbackResponse)
    claim = build_write_fence_provider_claim({"state": "issued"})
    monkeypatch.setattr(
        "gateway.pnc_rca_provider_fence.write_fence_claim_for_submission",
        lambda _task_id: claim,
    )
    monkeypatch.setattr(
        "gateway.pnc_rca_provider_fence.revalidate_provider_write_claim",
        Mock(side_effect=RuntimeError("external_write_fence_epoch_not_current")),
    )
    adapter = _adapter()
    adapter._loop = SimpleNamespace(is_closed=lambda: False)
    adapter._handle_task_confirm_card_action = Mock(
        side_effect=AssertionError("callback handler must not run")
    )
    data = SimpleNamespace(
        event=SimpleNamespace(
            context=SimpleNamespace(open_chat_id=PNC_ALL_BUSINESS_TEST_GROUP_ID),
            action=SimpleNamespace(
                value={"hermes_action": "task_confirm", "task_id": "rca-task"}
            ),
        )
    )

    response = adapter._on_card_action_trigger(data)

    assert response.card is None
    adapter._handle_task_confirm_card_action.assert_not_called()
