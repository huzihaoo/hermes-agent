"""#17 transport: pnc clarify card construction + button-action handler.

GatewayRunner._send_pnc_clarify_card builds the 3-button card; FeishuAdapter.
_handle_rca_clarify_card_action maps each choice to concrete guidance. Bare
instances (__new__) avoid full gateway/SDK init. Feishu end-to-end render + the
real card callback still needs a test-group human click (noted in HANDOFF).
"""
import asyncio
from types import SimpleNamespace

from gateway.run import GatewayRunner, Platform
from gateway.platforms.feishu import FeishuAdapter
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


def test_handler_maps_each_choice_to_guidance():
    a = _adapter()
    cases = {
        "rca_case_status_check": "case 状态",
        "rca_issue_intake": "问题",
        "rca_case_evidence_summary": "证据",
    }
    for choice, kw in cases.items():
        resp = a._handle_rca_clarify_card_action(event=None, action_value={"choice": choice})
        content = resp.card.data["elements"][0]["content"]
        assert kw in content


def test_handler_unknown_choice_falls_back():
    a = _adapter()
    resp = a._handle_rca_clarify_card_action(event=None, action_value={"choice": "bogus"})
    assert "G1Q3 case" in resp.card.data["elements"][0]["content"]
