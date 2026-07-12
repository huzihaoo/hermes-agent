from types import SimpleNamespace
import json

import pytest

from gateway import run as gateway_run
from gateway.config import Platform
from gateway.platforms.base import MessageEvent, MessageType
from gateway.session import SessionSource
from hermes_constants import reset_hermes_home_override, set_hermes_home_override


G1Q3_GROUP_ID = "oc_6cfc782212009ff4cd815349909dd423"


def make_runner(receipt_dir=None):
    runner = object.__new__(gateway_run.GatewayRunner)
    runner.adapters = {}
    runner.config = SimpleNamespace()
    runner.session_store = SimpleNamespace()
    runner.pairing_store = SimpleNamespace()
    if receipt_dir is not None:
        runner._pnc_group_binding_receipt_dir = receipt_dir
    return runner


def make_feishu_event(text: str, *, chat_id: str = G1Q3_GROUP_ID) -> MessageEvent:
    return MessageEvent(
        text=text,
        message_type=MessageType.TEXT,
        source=SessionSource(
            platform=Platform.FEISHU,
            user_id="ou_test_user",
            user_name="测试用户",
            chat_id=chat_id,
            chat_name="G1Q3 归因群",
            chat_type="group",
        ),
        message_id="om_test_message",
    )


@pytest.mark.asyncio
async def test_g1q3_rca_status_request_submits_handoff_without_agent(monkeypatch, tmp_path):
    token = set_hermes_home_override(tmp_path)
    runner = make_runner()
    event = make_feishu_event("帮我看一下 case G1Q3-042 现在归因做到哪一步了")

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)
    async def _should_not_run_agent(*args, **kwargs):  # pragma: no cover - failure path
        raise AssertionError("G1Q3 handoff must not enter generic agent execution")
    monkeypatch.setattr(gateway_run.GatewayRunner, "_handle_message_with_agent", _should_not_run_agent, raising=False)
    monkeypatch.setattr(
        gateway_run,
        "_submit_g1q3_rca_status_handoff",
        lambda **kwargs: {"success": True, "task": {"task_id": "pnc_status_001"}},
    )

    try:
        response = await gateway_run.GatewayRunner._handle_message(runner, event)
        body = json.loads((tmp_path / "task-state" / "pnc_status_001.json").read_text(encoding="utf-8"))
    finally:
        reset_hermes_home_override(token)

    assert response == ""
    assert body["task_card"]["one_card_policy"] is True
    assert "已接单：正在查询 G1Q3-042 的 RCA 进展/结论/报告位置" in body["task_card"]["status_line"]
    assert event.metadata["pnc_group_binding"]["decision"] == "accepted"


@pytest.mark.asyncio
async def test_g1q3_cross_business_line_request_returns_policy_rejection(monkeypatch):
    runner = make_runner()
    event = make_feishu_event("帮我看下这次评测门禁为什么没过")

    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)
    async def _should_not_run_agent(*args, **kwargs):  # pragma: no cover - failure path
        raise AssertionError("G1Q3 rejection must not enter agent execution")
    monkeypatch.setattr(gateway_run.GatewayRunner, "_handle_message_with_agent", _should_not_run_agent, raising=False)

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert response is not None
    assert "只处理 G1Q3 RCA" in response
    assert event.metadata["pnc_group_binding"]["decision"] == "reject"
    assert event.metadata["pnc_group_binding"]["reason"] == "cross_business_line"


@pytest.mark.asyncio
async def test_g1q3_handoff_writes_jsonl_receipts(monkeypatch, tmp_path):
    token = set_hermes_home_override(tmp_path)
    runner = make_runner(receipt_dir=tmp_path)
    event = make_feishu_event("帮我看一下 case G1Q3-042 现在归因做到哪一步了")

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)
    monkeypatch.setattr(
        gateway_run,
        "_submit_g1q3_rca_status_handoff",
        lambda **kwargs: {"success": True, "task": {"task_id": "pnc_status_001"}},
    )

    try:
        response = await gateway_run.GatewayRunner._handle_message(runner, event)
    finally:
        reset_hermes_home_override(token)

    assert response == ""
    receipt_files = list(tmp_path.glob("*.jsonl"))
    assert len(receipt_files) == 1
    records = [json.loads(line) for line in receipt_files[0].read_text().splitlines()]
    assert len(records) == 2
    record = records[0]
    assert record["group_binding_id"] == "gb_g1q3_rca_feishu_group"
    assert record["group_id"] == G1Q3_GROUP_ID
    assert record["decision"] == "accepted"
    assert record["template_id"] == "rca_case_status_check"
    assert record["business_line_ref"] == "rca"
    assert record["project_space_ref"] == "g1q3_rca"
    assert record["requester"] == "ou_test_user"
    assert record["message_id"] == "om_test_message"
    assert record["gray_delivery_phase"] == "g1q3_rca_business_delivery_gray"
    assert record["route_surface"] == "rca_case_status_check"
    assert record["risk_gate"] == "execution_layer"
    assert records[1]["event_type"] == "handoff_submission"
    assert records[1]["gray_delivery_phase"] == "g1q3_rca_business_delivery_gray"
    assert records[1]["task_id"] == "pnc_status_001"


@pytest.mark.asyncio
async def test_g1q3_duplicate_request_updates_existing_card_instead_of_text(monkeypatch, tmp_path):
    token = set_hermes_home_override(tmp_path)
    runner = make_runner(receipt_dir=tmp_path)
    runner._g1q3_recent_handoffs = {"dedicated:rca_case_status_check:042": ("pnc_status_001", 0.0, 600.0)}
    event = make_feishu_event("帮我看一下 case G1Q3-042 现在归因做到哪一步了")
    sidecar_dir = tmp_path / "task-state"
    sidecar_dir.mkdir(parents=True)
    (sidecar_dir / "pnc_status_001.json").write_text(json.dumps({
        "task_card": {
            "task_id": "pnc_status_001",
            "chat_id": G1Q3_GROUP_ID,
            "thread_id": "topic:om_test_message",
            "user_state": "host-created",
            "milestones": [{"ts": "2026-06-12T00:00:00+00:00", "label": "任务建好"}],
            "delivery": {"boundaries": []},
        }
    }), encoding="utf-8")

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run.time, "monotonic", lambda: 60.0)
    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)
    monkeypatch.setattr(
        gateway_run,
        "_submit_g1q3_rca_status_handoff",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("duplicate must not submit a new task")),
    )

    try:
        response = await gateway_run.GatewayRunner._handle_message(runner, event)
        body = json.loads((sidecar_dir / "pnc_status_001.json").read_text(encoding="utf-8"))
    finally:
        reset_hermes_home_override(token)

    assert response == ""
    assert body["task_card"]["one_card_policy"] is True
    assert "重复请求已收到" in body["task_card"]["status_line"]
    assert any(item["label"] == "收到重复请求，沿用当前任务" for item in body["task_card"]["milestones"])


@pytest.mark.asyncio
async def test_g1q3_test_group_does_not_dedupe_against_dedicated_group(monkeypatch, tmp_path):
    token = set_hermes_home_override(tmp_path)
    runner = make_runner(receipt_dir=tmp_path)
    runner._g1q3_recent_handoffs = {"dedicated:rca_case_status_check:042": ("pnc_status_dedicated", 0.0, 600.0)}
    event = make_feishu_event(
        "帮我看一下 case G1Q3-042 现在归因做到哪一步了",
        chat_id="oc_16614f4ba25b8c88b69c0b8e9ebc2fb5",
    )
    submissions = []

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run.time, "monotonic", lambda: 60.0)
    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)

    def _submit(**kwargs):
        submissions.append(kwargs)
        return {"success": True, "task": {"task_id": "pnc_status_test"}}

    monkeypatch.setattr(gateway_run, "_submit_g1q3_rca_status_handoff", _submit)

    try:
        response = await gateway_run.GatewayRunner._handle_message(runner, event)
    finally:
        reset_hermes_home_override(token)

    assert response == ""
    assert len(submissions) == 1
    assert runner._g1q3_recent_handoffs["dedicated:rca_case_status_check:042"][0] == "pnc_status_dedicated"
    assert runner._g1q3_recent_handoffs["test:rca_case_status_check:042"][0] == "pnc_status_test"


@pytest.mark.asyncio
async def test_all_business_test_group_unknown_prompt_does_not_create_integration_tools_task(monkeypatch):
    runner = make_runner()
    event = make_feishu_event(
        "今天下午谁有空看下这个问题",
        chat_id="oc_16614f4ba25b8c88b69c0b8e9ebc2fb5",
    )
    agent_calls = []

    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)
    monkeypatch.setattr(
        gateway_run,
        "_submit_integration_tools_intake_handoff",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("unknown test-group prompt must not create integration-tools task")),
    )

    async def _fake_agent(self, event, source=None, quick_key=None, run_generation=None):
        agent_calls.append(event.text)
        return "generic-response"

    monkeypatch.setattr(gateway_run.GatewayRunner, "_handle_message_with_agent", _fake_agent, raising=False)

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert response == "generic-response"
    assert agent_calls == ["今天下午谁有空看下这个问题"]
    assert "integration_tools_handoff" not in event.metadata


@pytest.mark.asyncio
async def test_all_business_test_group_g1q3_prompt_does_not_create_integration_tools_task(monkeypatch, tmp_path):
    token = set_hermes_home_override(tmp_path)
    runner = make_runner(receipt_dir=tmp_path)
    event = make_feishu_event(
        "帮我看一下 case G1Q3-042 现在归因做到哪一步了",
        chat_id="oc_16614f4ba25b8c88b69c0b8e9ebc2fb5",
    )

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)
    monkeypatch.setattr(
        gateway_run,
        "_submit_integration_tools_intake_handoff",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("G1Q3 test prompt must not create integration-tools task")),
    )
    monkeypatch.setattr(
        gateway_run,
        "_submit_g1q3_rca_status_handoff",
        lambda **kwargs: {"success": True, "task": {"task_id": "pnc_status_test_2"}},
    )

    try:
        response = await gateway_run.GatewayRunner._handle_message(runner, event)
    finally:
        reset_hermes_home_override(token)

    assert response == ""
    assert event.metadata["pnc_group_binding"]["decision"] == "accepted"
    assert event.metadata["pnc_group_handoff"]["task"]["task_id"] == "pnc_status_test_2"
    assert "integration_tools_handoff" not in event.metadata
