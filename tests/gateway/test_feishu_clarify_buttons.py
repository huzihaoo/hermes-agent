"""Offline closed-loop test for the Feishu send_clarify native-button override.

Mirrors the Telegram clarify-button contract: a button click must resolve the
shared tools.clarify_gateway entry (the SAME backend Telegram/Discord use), and
"其他" must flip the entry into text-capture. Only the SDK event shape + the two
helper methods on self are faked; the resolution path (clarify_gateway) is REAL.
"""
import types
import threading
import time

import pytest

from tools import clarify_gateway as cg
import gateway.platforms.feishu as feishu_mod


def _fake_self():
    return types.SimpleNamespace(
        _task_confirm_response=lambda result: result,
        _clarify_state={},
    )


def _fake_event(value):
    action = types.SimpleNamespace(value=value)
    operator = types.SimpleNamespace(open_id="ou_clicker", user_id="")
    return types.SimpleNamespace(action=action, operator=operator, token="tok")


def test_button_click_resolves_clarify_gateway():
    cid = "clr-1"
    cg.register(clarify_id=cid, session_key="agent:main:main", question="哪个工具？",
                choices=["Hermes", "VM worker", "PNC RCA"])
    # a background "agent" blocks on wait_for_response, like the real flow
    box = {}
    t = threading.Thread(target=lambda: box.update(resp=cg.wait_for_response(cid, timeout=5)))
    t.start(); time.sleep(0.2)

    handler = feishu_mod.FeishuAdapter._handle_clarify_card_action
    ev = _fake_event({"hermes_action": "clarify", "clarify_id": cid, "choice": "VM worker"})
    result = handler(_fake_self(), event=ev, action_value=ev.action.value)
    assert result["ok"] and result["changed"] and result["choice"] == "VM worker"

    t.join(timeout=5)
    assert box.get("resp") == "VM worker"  # agent unblocked with the chosen text


def test_other_button_flips_to_text_capture():
    cid = "clr-2"
    cg.register(clarify_id=cid, session_key="agent:main:main", question="哪个？", choices=["A", "B"])
    handler = feishu_mod.FeishuAdapter._handle_clarify_card_action
    ev = _fake_event({"hermes_action": "clarify", "clarify_id": cid, "other": True})
    result = handler(_fake_self(), event=ev, action_value=ev.action.value)
    assert result["ok"]
    assert cg.has_pending("agent:main:main")  # still pending, now awaiting text
    cg.clear_session("agent:main:main")


def test_resolve_unknown_clarify_returns_not_ok():
    handler = feishu_mod.FeishuAdapter._handle_clarify_card_action
    ev = _fake_event({"hermes_action": "clarify", "clarify_id": "nope", "choice": "x"})
    result = handler(_fake_self(), event=ev, action_value=ev.action.value)
    assert not result["ok"]


def test_missing_clarify_id_fails_closed():
    handler = feishu_mod.FeishuAdapter._handle_clarify_card_action
    ev = _fake_event({"hermes_action": "clarify", "choice": "x"})
    result = handler(_fake_self(), event=ev, action_value=ev.action.value)
    assert not result["ok"]
