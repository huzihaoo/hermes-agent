"""Admission routing regressions for Feishu topic preservation and worker fairness."""

from __future__ import annotations

import asyncio
import json
import threading
from collections import OrderedDict
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from gateway.admission.controller import AdmissionController
from gateway.admission.types import QueueItem
from gateway.admission.worker import QueueWorker
from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import MessageEvent, MessageType, SendResult
from gateway.platforms.feishu import (
    G1Q3_RCA_GROUP_ID,
    PNC_ALL_BUSINESS_TEST_GROUP_ID,
    FeishuAdapter,
    _build_feishu_queue_event_context,
    _looks_like_g1q3_rca_request_for_admission,
)
from gateway.session import SessionSource


class _CaptureAdmission:
    def __init__(self, lane: str = "heavy"):
        self.calls = []
        self.lane = lane

    async def admit(self, **kwargs):
        self.calls.append(kwargs)
        return True, "queued", SimpleNamespace(id="queue-1", lane=self.lane)


def _adapter_without_init(*, lane: str = "heavy") -> FeishuAdapter:
    adapter = object.__new__(FeishuAdapter)
    adapter.platform = "feishu"
    adapter.config = PlatformConfig(enabled=True)
    adapter._admission_enabled = True
    adapter._admission_controller = _CaptureAdmission(lane=lane)
    adapter.sent = []

    async def fake_send(chat_id, content, metadata=None, **kwargs):
        adapter.sent.append({"chat_id": chat_id, "content": content, "metadata": metadata, "kwargs": kwargs})
        return SimpleNamespace(success=True)

    adapter.send = fake_send
    return adapter


@pytest.mark.asyncio
async def test_feishu_admission_dispatch_preserves_topic_routing_fields():
    adapter = _adapter_without_init()
    event = MessageEvent(
        source=SessionSource(
            platform="feishu",
            user_id="ou_user",
            chat_id="oc_group",
            chat_type="group",
            thread_id="topic:om_topic",
        ),
        text="请提交一个 VM heavy 任务",
        message_type=MessageType.TEXT,
        message_id="om_request",
    )

    await adapter._dispatch_inbound_event(event)

    admission = adapter._admission_controller
    assert len(admission.calls) == 1
    call = admission.calls[0]
    assert call["user_id"] == "ou_user"
    assert call["chat_id"] == "oc_group"
    assert call["chat_type"] == "group"
    assert call["thread_id"] == "topic:om_topic"
    assert call["request_message_id"] == "om_request"
    assert call["platform"] == "feishu"
    assert adapter.sent
    assert adapter.sent[0]["chat_id"] == "oc_group"
    assert adapter.sent[0]["metadata"] == {"thread_id": "topic:om_topic"}
    assert "heavy" in adapter.sent[0]["content"]
    assert "VM" in adapter.sent[0]["content"]


@pytest.mark.asyncio
async def test_fixed_group_directed_mention_persists_trusted_reply_and_card_context():
    adapter = _adapter_without_init(lane="standard")
    issue_url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"
    event = MessageEvent(
        source=SessionSource(
            platform=Platform.FEISHU,
            user_id="ou_user",
            user_id_alt="on_user",
            user_name="RCA User",
            chat_id=G1Q3_RCA_GROUP_ID,
            chat_name="G1Q3 RCA",
            chat_type="group",
            thread_id="topic:om_root",
            is_bot=False,
        ),
        text="分析这个问题",
        message_type=MessageType.TEXT,
        message_id="om_request",
        reply_to_message_id="om_parent",
        reply_to_text=f"replied issue card {issue_url}",
        metadata={
            "feishu": {
                "message_id": "om_request",
                "root_id": "om_root",
                "parent_id": "om_parent",
                "thread_id": "topic:om_root",
                "sender_id": "ou_user",
                "sender_type": "user",
                "is_bot_sender": False,
                "is_topic": True,
                "raw_container_id": G1Q3_RCA_GROUP_ID,
                "receive_time_ms": "1720000000000",
                "ingress_source": "event_callback",
                "link_urls": [issue_url],
                "self_mentioned": True,
                "self_mention_command_directed": True,
                "mention_required": True,
            }
        },
    )

    completed = await adapter._dispatch_inbound_event(event)

    assert completed is False
    [call] = adapter._admission_controller.calls
    assert issue_url in call["message"]
    context = call["event_context"]
    assert context["route_contract"] == "g1q3_rca_manual_v1"
    assert context["source"] == {
        "platform": "feishu",
        "user_id": "ou_user",
        "user_id_alt": "on_user",
        "user_name": "RCA User",
        "chat_id": G1Q3_RCA_GROUP_ID,
        "chat_name": "G1Q3 RCA",
        "chat_type": "group",
        "thread_id": "topic:om_root",
        "is_bot": False,
    }
    assert context["event"]["reply_to_message_id"] == "om_parent"
    assert issue_url in context["event"]["reply_to_text"]
    assert context["feishu"]["link_urls"] == [issue_url]
    assert context["feishu"]["self_mentioned"] is True
    assert context["feishu"]["self_mention_command_directed"] is True
    assert call["require_durable_persistence"] is True


@pytest.mark.parametrize(
    "oversized_field",
    ["reply", "links", "link_url"],
)
def test_durable_feishu_queue_context_rejects_oversized_identity_context(
    oversized_field,
):
    event = MessageEvent(
        source=SessionSource(
            platform=Platform.FEISHU,
            user_id="ou_user",
            chat_id=G1Q3_RCA_GROUP_ID,
            chat_type="group",
        ),
        text="分析问题",
        message_id="om_oversized",
        reply_to_text="x" * (32 * 1024 + 1) if oversized_field == "reply" else None,
        metadata={
            "feishu": {
                "sender_type": "user",
                "self_mentioned": True,
                "self_mention_command_directed": True,
                "link_urls": (
                    [f"https://example.test/{index}" for index in range(33)]
                    if oversized_field == "links"
                    else ["https://example.test/" + "x" * 4096]
                    if oversized_field == "link_url"
                    else []
                ),
            }
        },
    )

    with pytest.raises(ValueError, match="too large|too many"):
        _build_feishu_queue_event_context(event, durable_rca_manual=True)


@pytest.mark.asyncio
async def test_feishu_admission_does_not_send_public_feedback_for_fast_or_standard_queue_admission():
    for lane in ("fast", "standard"):
        adapter = _adapter_without_init(lane=lane)
        event = MessageEvent(
            source=SessionSource(
                platform="feishu",
                user_id="ou_user",
                chat_id="oc_group",
                chat_type="group",
                thread_id="topic:om_topic",
            ),
            text="你好",
            message_type=MessageType.TEXT,
            message_id="om_request",
        )

        await adapter._dispatch_inbound_event(event)

        assert len(adapter._admission_controller.calls) == 1
        assert adapter.sent == []


@pytest.mark.asyncio
async def test_feishu_admission_feedback_send_failure_does_not_fall_through_to_immediate_processing():
    adapter = _adapter_without_init()
    handled = []

    async def failing_send(*args, **kwargs):
        raise RuntimeError("send failed")

    async def fake_handle(event):
        handled.append(event)

    adapter.send = failing_send
    adapter._handle_message_with_guards = fake_handle
    event = MessageEvent(
        source=SessionSource(
            platform="feishu",
            user_id="ou_user",
            chat_id="oc_group",
            chat_type="group",
            thread_id="topic:om_topic",
        ),
        text="请提交一个 VM heavy 任务",
        message_type=MessageType.TEXT,
        message_id="om_request",
    )

    await adapter._dispatch_inbound_event(event)

    assert len(adapter._admission_controller.calls) == 1
    assert handled == []


@pytest.mark.asyncio
async def test_feishu_process_queue_item_reconstructs_group_topic_event(monkeypatch):
    adapter = object.__new__(FeishuAdapter)
    adapter.platform = "feishu"
    captured = {}

    async def fake_handle(event):
        captured["event"] = event

    adapter._handle_message_with_guards = fake_handle
    item = QueueItem(
        id="queue-1",
        user_id="ou_user",
        user_role="owner",
        message="queued text",
        lane="heavy",
        priority=100,
        domain="group",
        domain_id="oc_group",
        chat_id="oc_group",
        chat_type="group",
        thread_id="topic:om_topic",
        request_message_id="om_request",
        platform="feishu",
    )

    result = await adapter._process_queue_item(item)

    assert result == {"status": "completed"}
    event = captured["event"]
    assert event.source.chat_type == "group"
    assert event.source.chat_id == "oc_group"
    assert event.source.thread_id == "topic:om_topic"
    assert event.message_id == "om_request"
    assert event.text == "queued text"


@pytest.mark.asyncio
async def test_durable_rca_worker_failure_releases_inbox_and_retry_completes(
    tmp_path,
):
    issue_url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"
    event = MessageEvent(
        source=SessionSource(
            platform=Platform.FEISHU,
            user_id="ou_user",
            chat_id=G1Q3_RCA_GROUP_ID,
            chat_type="group",
            thread_id="topic:om_root",
        ),
        text=f"分析这个问题 {issue_url}",
        message_type=MessageType.TEXT,
        message_id="om_retry",
        reply_to_message_id="om_parent",
        reply_to_text=f"issue card {issue_url}",
        metadata={
            "feishu": {
                "message_id": "om_retry",
                "root_id": "om_root",
                "parent_id": "om_parent",
                "thread_id": "topic:om_root",
                "sender_id": "ou_user",
                "sender_type": "user",
                "is_bot_sender": False,
                "raw_container_id": G1Q3_RCA_GROUP_ID,
                "ingress_source": "event_callback",
                "link_urls": [issue_url],
                "self_mentioned": True,
                "self_mention_command_directed": True,
                "mention_required": True,
            }
        },
    )
    controller = AdmissionController(
        db_path=tmp_path / "queue.db",
        audit_dir=tmp_path / "audit",
    )
    _, _, item = await controller.admit(
        user_id="ou_user",
        message=event.text,
        chat_id=G1Q3_RCA_GROUP_ID,
        chat_type="group",
        thread_id="topic:om_root",
        request_message_id="om_retry",
        platform="feishu",
        event_context=_build_feishu_queue_event_context(
            event,
            durable_rca_manual=True,
        ),
    )
    assert item is not None

    adapter = object.__new__(FeishuAdapter)
    adapter.platform = Platform.FEISHU
    adapter._chat_locks = OrderedDict()
    adapter._seen_message_ids = {}
    adapter._seen_message_order = []
    adapter._processing_message_ids = {}
    adapter._dedup_cache_size = 100
    adapter._dedup_lock = threading.Lock()
    adapter._dedup_state_path = tmp_path / "dedup.json"
    adapter._reactions_enabled = lambda: False
    sent = []
    calls = 0

    async def handle(reconstructed):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("gateway callback failed")
        reconstructed.metadata["pnc_group_binding"] = {
            "decision": "accepted",
            "route_surface": "rca_manual_intake",
        }
        reconstructed.metadata["pnc_manual_authorization"] = {"authorized": True}
        reconstructed.metadata["pnc_manual_rca_admission"] = {
            "outcome": "created",
            "submission_key": "g1q3-rca-s1-" + "a" * 64,
        }
        return "durably admitted"

    async def send(**kwargs):
        sent.append(kwargs)
        return SendResult(success=True, message_id="om_ack")

    adapter._message_handler = handle
    adapter.send = send
    assert adapter._begin_message_processing("om_retry") is True

    worker = QueueWorker(controller, adapter._process_queue_item)
    dequeued = controller.dequeue_next(item.lane, domain=item.domain)
    assert dequeued is item
    await worker._process_item(item)

    assert item.status == "queued"
    assert item.last_error == "gateway callback failed"
    assert adapter._message_processing_completed("om_retry") is False
    assert "om_retry" not in adapter._processing_message_ids

    # Feishu can now redeliver. Deterministic admission returns the same item,
    # while the worker retry reaches the durable control-store boundary once.
    assert adapter._begin_message_processing("om_retry") is True
    _, _, redelivered = await controller.admit(
        user_id="ou_user",
        message=event.text,
        chat_id=G1Q3_RCA_GROUP_ID,
        chat_type="group",
        thread_id="topic:om_root",
        request_message_id="om_retry",
        platform="feishu",
        event_context=item.event_context,
    )
    assert redelivered is item
    item.next_retry_at = None
    retried = controller.dequeue_next(item.lane, domain=item.domain)
    assert retried is item
    await worker._process_item(item)

    assert item.status == "completed"
    assert item.result["durable_feishu_completion"] is True
    assert adapter._message_processing_completed("om_retry") is True
    assert calls == 2
    assert sent[0]["reply_to"] == "om_parent"
    assert sent[0]["metadata"] == {
        "thread_id": "topic:om_root",
        "reply_to_message_id": "om_parent",
    }


@pytest.mark.asyncio
async def test_durable_rca_policy_error_is_retried_and_never_completed(tmp_path):
    event = MessageEvent(
        source=SessionSource(
            platform=Platform.FEISHU,
            user_id="ou_user",
            chat_id=G1Q3_RCA_GROUP_ID,
            chat_type="group",
            thread_id="topic:om_policy_error",
        ),
        text="分析这个问题",
        message_type=MessageType.TEXT,
        message_id="om_policy_error",
        metadata={
            "feishu": {
                "message_id": "om_policy_error",
                "thread_id": "topic:om_policy_error",
                "sender_id": "ou_user",
                "sender_type": "user",
                "is_bot_sender": False,
                "raw_container_id": G1Q3_RCA_GROUP_ID,
                "ingress_source": "event_callback",
                "self_mentioned": True,
                "self_mention_command_directed": True,
            }
        },
    )
    controller = AdmissionController(
        db_path=tmp_path / "queue.db",
        audit_dir=tmp_path / "audit",
    )
    _, _, item = await controller.admit(
        user_id="ou_user",
        message=event.text,
        chat_id=G1Q3_RCA_GROUP_ID,
        chat_type="group",
        thread_id=event.source.thread_id,
        request_message_id=event.message_id,
        platform="feishu",
        event_context=_build_feishu_queue_event_context(
            event,
            durable_rca_manual=True,
        ),
        require_durable_persistence=True,
    )

    adapter = object.__new__(FeishuAdapter)
    adapter.platform = Platform.FEISHU
    adapter._chat_locks = {}
    adapter._seen_message_ids = {}
    adapter._seen_message_order = []
    adapter._processing_message_ids = {}
    adapter._dedup_cache_size = 100
    adapter._dedup_lock = threading.Lock()
    adapter._dedup_state_path = tmp_path / "dedup.json"
    adapter._reactions_enabled = lambda: False
    adapter.send = AsyncMock(side_effect=AssertionError("policy errors must not reply"))

    async def policy_error_handler(reconstructed):
        reconstructed.metadata["pnc_group_binding_error"] = {
            "schema_version": "pnc_group_binding_error_v1",
            "code": "policy_evaluation_failed",
            "retryable": True,
        }
        return "fail-closed response"

    adapter._message_handler = policy_error_handler
    assert adapter._begin_message_processing(event.message_id) is True
    worker = QueueWorker(controller, adapter._process_queue_item)
    assert controller.dequeue_next(item.lane, domain=item.domain) is item

    await worker._process_item(item)

    assert item.status == "queued"
    assert "retryable PNC group binding error" in item.last_error
    assert adapter._message_processing_completed(event.message_id) is False
    assert event.message_id not in adapter._processing_message_ids
    adapter.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_forgeable_boolean_queue_context_cannot_open_manual_route(tmp_path):
    issue_url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"
    event = MessageEvent(
        source=SessionSource(
            platform=Platform.FEISHU,
            user_id="ou_user",
            chat_id=G1Q3_RCA_GROUP_ID,
            chat_type="group",
        ),
        text=f"分析 {issue_url}",
        message_id="om_forged",
        metadata={
            "feishu": {
                "sender_type": "user",
                "self_mentioned": True,
                "self_mention_command_directed": True,
            }
        },
    )
    context = _build_feishu_queue_event_context(event, durable_rca_manual=True)
    context["feishu"]["self_mentioned"] = "true"
    item = QueueItem(
        id="queue-forged",
        user_id="ou_user",
        user_role="member",
        message=event.text,
        lane="standard",
        priority=10,
        domain="group",
        domain_id=G1Q3_RCA_GROUP_ID,
        chat_id=G1Q3_RCA_GROUP_ID,
        chat_type="group",
        request_message_id="om_forged",
        platform="feishu",
        event_context=context,
    )
    adapter = object.__new__(FeishuAdapter)
    adapter.platform = Platform.FEISHU
    adapter._seen_message_ids = {}
    adapter._seen_message_order = []
    adapter._processing_message_ids = {"om_forged": 1.0}
    adapter._dedup_cache_size = 100
    adapter._dedup_lock = threading.Lock()
    adapter._dedup_state_path = tmp_path / "dedup.json"

    async def forbidden_handler(_event):  # pragma: no cover - fail-closed guard
        raise AssertionError("forged context reached Gateway")

    adapter._message_handler = forbidden_handler
    with pytest.raises(RuntimeError, match="route contract did not validate"):
        await adapter._process_queue_item(item)

    assert "om_forged" not in adapter._processing_message_ids


@pytest.mark.asyncio
async def test_feishu_process_inbound_message_builds_topic_source_from_root_id(monkeypatch):
    adapter = object.__new__(FeishuAdapter)
    adapter.platform = "feishu"
    adapter._admission_enabled = False
    adapter._admission_controller = None

    captured = {}

    async def fake_extract(message):
        return "持续推进", MessageType.TEXT, [], []

    async def fake_get_chat_info(chat_id):
        return {"chat_id": chat_id, "name": "项目群", "type": "group"}

    async def fake_sender_profile(sender_id):
        return {"user_id": "ou_user", "user_name": "用户", "user_id_alt": "on_user"}

    async def fake_fetch_message_text(message_id):
        return None

    async def fake_permission_request(**kwargs):
        return None

    async def fake_dispatch(event):
        captured["event"] = event

    adapter._extract_message_content = fake_extract
    adapter.get_chat_info = fake_get_chat_info
    adapter._resolve_sender_profile = fake_sender_profile
    adapter._fetch_message_text = fake_fetch_message_text
    adapter._maybe_handle_permission_request = fake_permission_request
    adapter._dispatch_inbound_event = fake_dispatch
    adapter._mentions_self = lambda _message: True
    adapter._require_mention_for = lambda _chat_id: True

    message = SimpleNamespace(
        chat_id="oc_group",
        chat_type="group",
        message_id="om_child",
        root_id="om_topic_root",
        parent_id="om_parent",
        upper_message_id=None,
        message_type="text",
    )
    sender_id = SimpleNamespace(open_id="ou_user", union_id="on_user")

    await adapter._process_inbound_message(
        data=SimpleNamespace(event=SimpleNamespace(message=message)),
        message=message,
        sender_id=sender_id,
        chat_type="group",
        message_id="om_child",
    )

    event = captured["event"]
    assert event.source.chat_id == "oc_group"
    assert event.source.chat_type == "group"
    assert event.source.thread_id == "topic:om_topic_root"
    assert event.source.user_id == "ou_user"
    assert event.source.user_id_alt == "on_user"
    assert event.message_id == "om_child"
    assert event.reply_to_message_id == "om_parent"
    assert event.metadata["feishu"]["self_mentioned"] is True
    assert event.metadata["feishu"]["mention_required"] is True


@pytest.mark.asyncio
async def test_fixed_group_reply_fetch_failure_abandons_same_id_for_redelivery(tmp_path):
    adapter = FeishuAdapter(PlatformConfig())
    adapter._dedup_state_path = tmp_path / "dedup.json"
    adapter._seen_message_ids.clear()
    adapter._seen_message_order.clear()
    adapter._processing_message_ids.clear()
    adapter._admit = Mock(return_value=None)
    self_mention = SimpleNamespace(
        is_self=True,
        name="PNC-Agent",
        open_id="ou_bot",
        is_all=False,
    )
    adapter._extract_message_content = AsyncMock(
        return_value=(
            "@PNC-Agent 分析这个问题",
            MessageType.TEXT,
            [],
            [],
            [self_mention],
        )
    )
    adapter._mentions_self = Mock(return_value=True)
    adapter._fetch_message_text = AsyncMock(
        side_effect=[
            RuntimeError("parent lookup unavailable"),
            "https://project.feishu.cn/g1q3/issue/detail/7013527412",
        ]
    )
    adapter.get_chat_info = AsyncMock(
        return_value={
            "chat_id": G1Q3_RCA_GROUP_ID,
            "name": "G1Q3 RCA",
            "type": "group",
        }
    )
    adapter._resolve_sender_profile = AsyncMock(
        return_value={
            "user_id": "ou_user",
            "user_name": "RCA User",
            "user_id_alt": None,
        }
    )
    adapter._require_mention_for = Mock(return_value=True)
    adapter._maybe_handle_direct_permission_grant = AsyncMock(return_value=None)
    adapter._maybe_handle_permission_request = AsyncMock(return_value=None)
    adapter._dispatch_inbound_event = AsyncMock(return_value=False)

    message = SimpleNamespace(
        message_id="om_reply_retry",
        message_type="text",
        content='{"text":"@PNC-Agent 分析这个问题"}',
        chat_id=G1Q3_RCA_GROUP_ID,
        chat_type="group",
        root_id="om_root",
        parent_id="om_parent",
        upper_message_id=None,
        mentions=[],
        create_time="1720000000000",
    )
    data = SimpleNamespace(
        event=SimpleNamespace(
            message=message,
            sender=SimpleNamespace(
                sender_type="user",
                sender_id=SimpleNamespace(
                    open_id="ou_user",
                    user_id=None,
                    union_id=None,
                ),
            ),
        )
    )

    with pytest.raises(RuntimeError, match="parent lookup unavailable"):
        await adapter._handle_message_event_data(data)
    assert adapter._message_processing_completed(message.message_id) is False
    assert message.message_id not in adapter._processing_message_ids
    adapter._dispatch_inbound_event.assert_not_awaited()

    assert await adapter._handle_message_event_data(data) is False
    adapter._dispatch_inbound_event.assert_awaited_once()
    redelivered = adapter._dispatch_inbound_event.await_args.args[0]
    assert redelivered.message_id == message.message_id
    assert "issue/detail/7013527412" in redelivered.reply_to_text


def _api_poll_test_item(
    chat_id: str,
    message_id: str,
    *,
    create_time: str = "2000",
    text: str = "@_user_1 分析问题",
) -> dict:
    return {
        "message_id": message_id,
        "msg_type": "text",
        "chat_id": chat_id,
        "create_time": create_time,
        "body": {"content": json.dumps({"text": text}, ensure_ascii=False)},
        "sender": {
            "id": "ou_user",
            "id_type": "open_id",
            "sender_type": "user",
        },
        "mentions": [
            {
                "id": "ou_bot",
                "id_type": "open_id",
                "key": "@_user_1",
                "name": "PNC-Agent",
            }
        ],
    }


def _reset_api_poll_persistence(adapter: FeishuAdapter, state_path) -> None:
    adapter._api_poll_state_path = state_path
    adapter._dedup_state_path = state_path.with_name(f"{state_path.stem}-inbox.json")
    adapter._seen_message_ids.clear()
    adapter._seen_message_order.clear()
    adapter._processing_message_ids.clear()
    adapter._api_poll_seen_message_ids.clear()
    adapter._api_poll_seen_message_order.clear()
    adapter._api_poll_baselined_chat_ids.clear()
    adapter._api_poll_last_seen_create_time_ms.clear()
    adapter._api_poll_cursor_message_ids.clear()
    adapter._api_poll_discovery_floor_ms.clear()
    adapter._api_poll_pending_items.clear()
    adapter._api_poll_scan_state.clear()
    adapter._api_poll_terminal_holes.clear()
    adapter._api_poll_state_error = None
    adapter._api_poll_raw_state = None
    adapter._api_poll_block_persistence = False
    adapter._api_poll_revision = 0
    adapter._api_poll_sidecar_initialized = False


class _ApiPollResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


@pytest.mark.asyncio
async def test_api_poll_pending_survives_latest_page_turnover(tmp_path):
    chat_id = G1Q3_RCA_GROUP_ID
    first_item = _api_poll_test_item(chat_id, "om_turnover_a")
    second_item = _api_poll_test_item(
        chat_id,
        "om_turnover_b",
        create_time="3000",
    )
    adapter = FeishuAdapter(
        PlatformConfig(extra={"api_poll_chat_ids": [chat_id]})
    )
    _reset_api_poll_persistence(adapter, tmp_path / "dedup.json")
    adapter._api_poll_started_at_ms = 1_500_000
    adapter._fetch_recent_chat_messages_via_api = Mock(
        side_effect=[[first_item, second_item], []]
    )
    adapter._handle_message_event_data = AsyncMock(
        side_effect=[False, True, True]
    )

    await adapter._poll_api_chat_once(chat_id)

    assert [
        pending["message_id"]
        for pending in adapter._api_poll_pending_items[chat_id]
    ] == ["om_turnover_a", "om_turnover_b"]
    assert chat_id not in adapter._api_poll_last_seen_create_time_ms
    persisted = json.loads(adapter._api_poll_state_path.read_text(encoding="utf-8"))
    assert [
        pending["message_id"]
        for pending in persisted["state"]["pending"][chat_id]
    ] == ["om_turnover_a", "om_turnover_b"]

    await adapter._poll_api_chat_once(chat_id)

    replayed_ids = [
        call.args[0].event.message.message_id
        for call in adapter._handle_message_event_data.await_args_list
    ]
    assert replayed_ids == [
        "om_turnover_a",
        "om_turnover_a",
        "om_turnover_b",
    ]
    assert chat_id not in adapter._api_poll_pending_items
    assert adapter._api_poll_last_seen_create_time_ms[chat_id] == 3_000_000


@pytest.mark.asyncio
async def test_api_poll_pending_recovers_after_adapter_restart(tmp_path):
    chat_id = G1Q3_RCA_GROUP_ID
    state_path = tmp_path / "dedup.json"
    first = FeishuAdapter(
        PlatformConfig(extra={"api_poll_chat_ids": [chat_id]})
    )
    _reset_api_poll_persistence(first, state_path)
    first._api_poll_started_at_ms = 1_500_000
    first._fetch_recent_chat_messages_via_api = Mock(
        return_value=[_api_poll_test_item(chat_id, "om_restart")]
    )
    first._handle_message_event_data = AsyncMock(return_value=False)
    await first._poll_api_chat_once(chat_id)

    restarted = FeishuAdapter(
        PlatformConfig(extra={"api_poll_chat_ids": [chat_id]})
    )
    _reset_api_poll_persistence(restarted, state_path)
    restarted._load_seen_message_ids()
    assert restarted._api_poll_pending_items[chat_id][0]["message_id"] == (
        "om_restart"
    )
    assert restarted._api_poll_discovery_floor_ms[chat_id] == 1_500_000
    restarted._fetch_recent_chat_messages_via_api = Mock(return_value=[])
    restarted._handle_message_event_data = AsyncMock(return_value=True)

    await restarted._poll_api_chat_once(chat_id)

    restarted._handle_message_event_data.assert_awaited_once()
    replayed = restarted._handle_message_event_data.await_args.args[0]
    assert replayed.event.message.message_id == "om_restart"
    assert chat_id not in restarted._api_poll_pending_items
    assert restarted._api_poll_last_seen_create_time_ms[chat_id] == 2_000_000


@pytest.mark.asyncio
async def test_api_poll_dead_letter_retires_pending_without_replay(tmp_path):
    chat_id = G1Q3_RCA_GROUP_ID
    adapter = FeishuAdapter(
        PlatformConfig(extra={"api_poll_chat_ids": [chat_id]})
    )
    _reset_api_poll_persistence(adapter, tmp_path / "dedup.json")
    item = _api_poll_test_item(chat_id, "om_dead")
    adapter._commit_api_poll_discovery(
        chat_id,
        pending_items=[item],
        mark_baselined=True,
    )
    adapter._admission_controller = SimpleNamespace(
        get_transport_item=lambda _platform, _message_id: SimpleNamespace(
            id="queue-dead-1",
            status="dead",
            result=None,
        )
    )
    adapter._fetch_recent_chat_messages_via_api = Mock(return_value=[])
    adapter._handle_message_event_data = AsyncMock(
        side_effect=AssertionError("dead-letter ownership must not be bypassed")
    )

    await adapter._poll_api_chat_once(chat_id)

    adapter._handle_message_event_data.assert_not_awaited()
    assert chat_id not in adapter._api_poll_pending_items
    assert adapter._api_poll_last_seen_create_time_ms[chat_id] == 2_000_000
    persisted = json.loads(adapter._api_poll_state_path.read_text(encoding="utf-8"))
    assert chat_id not in persisted["state"]["pending"]
    assert "om_dead" in persisted["state"]["seen_message_ids"]
    [hole] = persisted["state"]["terminal_holes"]
    assert hole["kind"] == "admission_terminal"
    assert hole["status"] == "dead"
    assert hole["message_id"] == "om_dead"
    assert hole["admission_item_id"] == "queue-dead-1"
    assert adapter.api_poll_persistence_status()["terminal_hole_count"] == 1


@pytest.mark.asyncio
async def test_api_poll_admission_owner_precedes_completed_inbox(tmp_path):
    chat_id = G1Q3_RCA_GROUP_ID
    adapter = FeishuAdapter(
        PlatformConfig(extra={"api_poll_chat_ids": [chat_id]})
    )
    _reset_api_poll_persistence(adapter, tmp_path / "owner-first.json")
    item = _api_poll_test_item(chat_id, "om_owner_first")
    adapter._commit_api_poll_discovery(
        chat_id,
        pending_items=[item],
        mark_baselined=True,
    )
    adapter._complete_message_processing("om_owner_first")
    owner = SimpleNamespace(status="processing", result=None)
    adapter._admission_controller = SimpleNamespace(
        get_transport_item=lambda _platform, _message_id: owner
    )
    adapter._fetch_recent_chat_messages_via_api = Mock(return_value=[])
    adapter._handle_message_event_data = AsyncMock(
        side_effect=AssertionError("active admission ownership must block replay")
    )

    await adapter._poll_api_chat_once(chat_id)

    assert adapter._api_poll_pending_items[chat_id][0]["message_id"] == (
        "om_owner_first"
    )
    assert chat_id not in adapter._api_poll_last_seen_create_time_ms
    owner.status = "completed"
    owner.result = {"durable_feishu_completion": True}

    await adapter._poll_api_chat_once(chat_id)

    assert chat_id not in adapter._api_poll_pending_items
    assert adapter._api_poll_last_seen_create_time_ms[chat_id] == 2_000_000
    adapter._handle_message_event_data.assert_not_awaited()


@pytest.mark.asyncio
async def test_api_poll_same_millisecond_cursor_ids_survive_restart(tmp_path):
    chat_id = G1Q3_RCA_GROUP_ID
    state_path = tmp_path / "same-ms.json"
    adapter = FeishuAdapter(
        PlatformConfig(extra={"api_poll_chat_ids": [chat_id]})
    )
    _reset_api_poll_persistence(adapter, state_path)
    adapter._api_poll_baselined_chat_ids.add(chat_id)
    adapter._api_poll_last_seen_create_time_ms[chat_id] = 2_000_000
    adapter._api_poll_cursor_message_ids[chat_id] = {"om_same_done"}
    adapter._persist_api_poll_state(require_success=True)
    adapter._fetch_recent_chat_messages_via_api = Mock(
        return_value=[
            _api_poll_test_item(chat_id, "om_same_done"),
            _api_poll_test_item(chat_id, "om_same_unseen"),
        ]
    )
    adapter._handle_message_event_data = AsyncMock(return_value=True)

    await adapter._poll_api_chat_once(chat_id)

    replayed = adapter._handle_message_event_data.await_args.args[0]
    assert replayed.event.message.message_id == "om_same_unseen"
    assert adapter._api_poll_cursor_message_ids[chat_id] == {
        "om_same_done",
        "om_same_unseen",
    }

    restarted = FeishuAdapter(
        PlatformConfig(extra={"api_poll_chat_ids": [chat_id]})
    )
    _reset_api_poll_persistence(restarted, state_path)
    restarted._load_seen_message_ids()
    assert restarted._api_poll_cursor_message_ids[chat_id] == {
        "om_same_done",
        "om_same_unseen",
    }
    restarted._fetch_recent_chat_messages_via_api = Mock(
        return_value=[_api_poll_test_item(chat_id, "om_same_third")]
    )
    restarted._handle_message_event_data = AsyncMock(return_value=True)

    await restarted._poll_api_chat_once(chat_id)

    third = restarted._handle_message_event_data.await_args.args[0]
    assert third.event.message.message_id == "om_same_third"
    assert restarted._api_poll_cursor_message_ids[chat_id] == {
        "om_same_done",
        "om_same_unseen",
        "om_same_third",
    }


def test_api_poll_seen_cache_is_bounded_and_persisted_in_order(tmp_path):
    chat_id = G1Q3_RCA_GROUP_ID
    adapter = FeishuAdapter(PlatformConfig())
    _reset_api_poll_persistence(adapter, tmp_path / "bounded-seen.json")
    adapter._dedup_cache_size = 3

    adapter._commit_api_poll_discovery(
        chat_id,
        pending_items=[],
        seen_message_ids=[f"om_seen_{index}" for index in range(10)],
        mark_baselined=True,
    )

    assert len(adapter._api_poll_seen_message_ids) == 3
    assert adapter._api_poll_seen_message_order == [
        "om_seen_7",
        "om_seen_8",
        "om_seen_9",
    ]
    persisted = json.loads(adapter._api_poll_state_path.read_text(encoding="utf-8"))
    assert persisted["state"]["seen_message_ids"] == [
        "om_seen_7",
        "om_seen_8",
        "om_seen_9",
    ]


def test_api_poll_pending_envelope_drops_unknown_large_fields():
    item = _api_poll_test_item(G1Q3_RCA_GROUP_ID, "om_minimal_envelope")
    item["unused_api_payload"] = "x" * (256 * 1024)

    envelope = FeishuAdapter._validated_api_poll_pending_item(
        item,
        expected_chat_id=G1Q3_RCA_GROUP_ID,
    )

    assert envelope["message_id"] == "om_minimal_envelope"
    assert "unused_api_payload" not in envelope


def test_api_poll_fetch_paginates_until_durable_watermark(monkeypatch):
    chat_id = G1Q3_RCA_GROUP_ID
    adapter = FeishuAdapter(
        PlatformConfig(
            extra={
                "api_poll_chat_ids": [chat_id],
                "api_poll_page_size": 2,
            }
        )
    )
    adapter._api_poll_baselined_chat_ids.add(chat_id)
    adapter._api_poll_last_seen_create_time_ms[chat_id] = 1_000_000
    adapter._fetch_tenant_access_token_via_api = Mock(return_value="token")
    payloads = [
        {
            "code": 0,
            "data": {
                "items": [
                    _api_poll_test_item(chat_id, "om_page_3", create_time="3000"),
                    _api_poll_test_item(chat_id, "om_page_2", create_time="2000"),
                ],
                "has_more": True,
                "page_token": "next-page",
            },
        },
        {
            "code": 0,
            "data": {
                "items": [
                    _api_poll_test_item(chat_id, "om_page_1", create_time="1000"),
                    _api_poll_test_item(chat_id, "om_page_old", create_time="900"),
                ],
                "has_more": True,
                "page_token": "unused-page",
            },
        },
    ]

    class Response:
        def __init__(self, payload):
            self.payload = payload

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return json.dumps(self.payload).encode("utf-8")

    urlopen = Mock(side_effect=[Response(payload) for payload in payloads])
    monkeypatch.setattr("gateway.platforms.feishu.urlopen", urlopen)

    items = adapter._fetch_recent_chat_messages_via_api(chat_id)

    assert [item["message_id"] for item in items] == [
        "om_page_3",
        "om_page_2",
        "om_page_1",
    ]
    assert urlopen.call_count == 2
    second_request = urlopen.call_args_list[1].args[0]
    assert "page_token=next-page" in second_request.full_url


def test_api_poll_fetch_chunks_gap_larger_than_pending_capacity(monkeypatch):
    chat_id = G1Q3_RCA_GROUP_ID
    adapter = FeishuAdapter(
        PlatformConfig(
            extra={
                "api_poll_chat_ids": [chat_id],
                "api_poll_page_size": 2,
            }
        )
    )
    adapter._api_poll_baselined_chat_ids.add(chat_id)
    adapter._api_poll_last_seen_create_time_ms[chat_id] = 1_000_000
    adapter._api_poll_cursor_message_ids[chat_id] = {"om_cursor"}
    adapter._fetch_tenant_access_token_via_api = Mock(return_value="token")
    monkeypatch.setattr(
        "gateway.platforms.feishu._MAX_API_POLL_PENDING_PER_CHAT",
        2,
    )
    pages = [
        {
            "code": 0,
            "data": {
                "items": [
                    _api_poll_test_item(chat_id, "om_gap_5", create_time="5000"),
                    _api_poll_test_item(chat_id, "om_gap_4", create_time="4000"),
                ],
                "has_more": True,
                "page_token": "page-2",
            },
        },
        {
            "code": 0,
            "data": {
                "items": [
                    _api_poll_test_item(chat_id, "om_gap_3", create_time="3000"),
                    _api_poll_test_item(chat_id, "om_gap_2", create_time="2000"),
                ],
                "has_more": True,
                "page_token": "page-3",
            },
        },
        {
            "code": 0,
            "data": {
                "items": [
                    _api_poll_test_item(chat_id, "om_cursor", create_time="1000"),
                    _api_poll_test_item(chat_id, "om_old", create_time="900"),
                ],
                "has_more": False,
            },
        },
    ]

    class Response:
        def __init__(self, payload):
            self.payload = payload

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return json.dumps(self.payload).encode("utf-8")

    first_urlopen = Mock(side_effect=[Response(payload) for payload in pages])
    monkeypatch.setattr("gateway.platforms.feishu.urlopen", first_urlopen)

    oldest_chunk = adapter._fetch_recent_chat_messages_via_api(chat_id)

    assert [item["message_id"] for item in oldest_chunk] == [
        "om_gap_3",
        "om_gap_2",
    ]

    adapter._api_poll_last_seen_create_time_ms[chat_id] = 3_000_000
    adapter._api_poll_cursor_message_ids[chat_id] = {"om_gap_3"}
    second_pages = [pages[0], pages[1]]
    second_pages[-1] = {
        "code": 0,
        "data": {
            "items": pages[1]["data"]["items"],
            "has_more": False,
        },
    }
    monkeypatch.setattr(
        "gateway.platforms.feishu.urlopen",
        Mock(side_effect=[Response(payload) for payload in second_pages]),
    )

    next_chunk = adapter._fetch_recent_chat_messages_via_api(chat_id)

    assert [item["message_id"] for item in next_chunk] == [
        "om_gap_5",
        "om_gap_4",
    ]


@pytest.mark.asyncio
async def test_api_poll_scan_continuation_survives_restart_after_scan_limit(
    tmp_path,
    monkeypatch,
):
    # A small limit exercises the same persisted continuation used for 10,001+
    # candidate gaps without constructing a large fixture.
    chat_id = G1Q3_RCA_GROUP_ID
    state_path = tmp_path / "scan-continuation.json"
    monkeypatch.setattr(
        "gateway.platforms.feishu._MAX_API_POLL_PENDING_PER_CHAT",
        2,
    )
    monkeypatch.setattr(
        "gateway.platforms.feishu._MAX_API_POLL_SCAN_ITEMS",
        4,
    )
    first = FeishuAdapter(
        PlatformConfig(
            extra={
                "api_poll_chat_ids": [chat_id],
                "api_poll_page_size": 2,
            }
        )
    )
    _reset_api_poll_persistence(first, state_path)
    first._api_poll_baselined_chat_ids.add(chat_id)
    first._api_poll_last_seen_create_time_ms[chat_id] = 1_000_000
    first._api_poll_cursor_message_ids[chat_id] = {"om_scan_cursor"}
    first._api_poll_discovery_floor_ms[chat_id] = 1_000_000
    first._persist_api_poll_state(require_success=True)
    first._fetch_tenant_access_token_via_api = Mock(return_value="token")
    first._handle_message_event_data = AsyncMock(
        side_effect=AssertionError("incomplete scan must not dispatch")
    )
    first_pages = [
        {
            "code": 0,
            "data": {
                "items": [
                    _api_poll_test_item(chat_id, "om_scan_5", create_time="5000"),
                    _api_poll_test_item(chat_id, "om_scan_4", create_time="4000"),
                ],
                "has_more": True,
                "page_token": "scan-page-2",
            },
        },
        {
            "code": 0,
            "data": {
                "items": [
                    _api_poll_test_item(chat_id, "om_scan_3", create_time="3000"),
                    _api_poll_test_item(chat_id, "om_scan_2", create_time="2000"),
                ],
                "has_more": True,
                "page_token": "scan-page-3",
            },
        },
    ]

    class Response:
        def __init__(self, payload):
            self.payload = payload

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return json.dumps(self.payload).encode("utf-8")

    monkeypatch.setattr(
        "gateway.platforms.feishu.urlopen",
        Mock(side_effect=[Response(payload) for payload in first_pages]),
    )

    await first._poll_api_chat_once(chat_id)

    first._handle_message_event_data.assert_not_awaited()
    assert first._api_poll_scan_state[chat_id]["page_token"] == "scan-page-3"
    assert [
        item["message_id"]
        for item in first._api_poll_scan_state[chat_id]["candidates"]
    ] == ["om_scan_3", "om_scan_2"]
    assert first.api_poll_persistence_status()["scan_continuation_count"] == 1

    restarted = FeishuAdapter(
        PlatformConfig(
            extra={
                "api_poll_chat_ids": [chat_id],
                "api_poll_page_size": 2,
            }
        )
    )
    _reset_api_poll_persistence(restarted, state_path)
    restarted._load_seen_message_ids()
    restarted._fetch_tenant_access_token_via_api = Mock(return_value="token")
    restarted._handle_message_event_data = AsyncMock(return_value=True)
    boundary_page = {
        "code": 0,
        "data": {
            "items": [
                _api_poll_test_item(chat_id, "om_scan_cursor", create_time="1000"),
                _api_poll_test_item(chat_id, "om_scan_old", create_time="900"),
            ],
            "has_more": False,
        },
    }
    resumed_urlopen = Mock(return_value=Response(boundary_page))
    monkeypatch.setattr("gateway.platforms.feishu.urlopen", resumed_urlopen)

    await restarted._poll_api_chat_once(chat_id)

    request = resumed_urlopen.call_args.args[0]
    assert "page_token=scan-page-3" in request.full_url
    replayed_ids = [
        call.args[0].event.message.message_id
        for call in restarted._handle_message_event_data.await_args_list
    ]
    assert replayed_ids == ["om_scan_2", "om_scan_3"]
    assert restarted._api_poll_scan_state == {}
    assert restarted._api_poll_last_seen_create_time_ms[chat_id] == 3_000_000


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_mode", ["expired", "loop"])
async def test_api_poll_invalid_continuation_resets_and_rescans_from_watermark(
    tmp_path,
    monkeypatch,
    failure_mode,
):
    chat_id = G1Q3_RCA_GROUP_ID
    adapter = FeishuAdapter(
        PlatformConfig(
            extra={"api_poll_chat_ids": [chat_id], "api_poll_page_size": 2}
        )
    )
    _reset_api_poll_persistence(adapter, tmp_path / f"token-{failure_mode}.json")
    adapter._api_poll_baselined_chat_ids.add(chat_id)
    adapter._api_poll_last_seen_create_time_ms[chat_id] = 1_000_000
    adapter._api_poll_cursor_message_ids[chat_id] = {"om_cursor"}
    adapter._api_poll_discovery_floor_ms[chat_id] = 1_000_000
    adapter._commit_api_poll_scan_state(
        chat_id,
        {
            "page_token": "persisted-token",
            "watermark_ms": 1_000_000,
            "cursor_message_ids": ["om_cursor"],
            "candidates": [],
            "baselined": True,
        },
    )
    adapter._fetch_tenant_access_token_via_api = Mock(return_value="token")
    adapter._handle_message_event_data = AsyncMock(return_value=True)
    rejected = [
        {"code": 234001, "msg": "page_token is invalid or expired"}
    ]
    if failure_mode == "loop":
        rejected = [
            {
                "code": 0,
                "data": {
                    "items": [],
                    "has_more": True,
                    "page_token": "next-token",
                },
            },
            {
                "code": 0,
                "data": {
                    "items": [],
                    "has_more": True,
                    "page_token": "next-token",
                },
            },
        ]
    recovered = {
        "code": 0,
        "data": {
            "items": [
                _api_poll_test_item(chat_id, "om_after_reset", create_time="2000")
            ],
            "has_more": False,
        },
    }
    urlopen = Mock(
        side_effect=[
            *[_ApiPollResponse(payload) for payload in rejected],
            _ApiPollResponse(recovered),
        ]
    )
    monkeypatch.setattr("gateway.platforms.feishu.urlopen", urlopen)

    await adapter._poll_api_chat_once(chat_id)

    first_url = urlopen.call_args_list[0].args[0].full_url
    recovered_url = urlopen.call_args_list[-1].args[0].full_url
    assert "page_token=persisted-token" in first_url
    if failure_mode == "loop":
        assert "page_token=next-token" in urlopen.call_args_list[1].args[0].full_url
    assert "page_token=" not in recovered_url
    assert adapter._api_poll_scan_state == {}
    assert adapter._api_poll_last_seen_create_time_ms[chat_id] == 2_000_000
    replayed = adapter._handle_message_event_data.await_args.args[0]
    assert replayed.event.message.message_id == "om_after_reset"
    persisted = json.loads(adapter._api_poll_state_path.read_text(encoding="utf-8"))
    assert persisted["state"]["scan_state"] == {}


@pytest.mark.asyncio
async def test_api_poll_network_failure_keeps_persisted_continuation(tmp_path):
    chat_id = G1Q3_RCA_GROUP_ID
    adapter = FeishuAdapter(
        PlatformConfig(extra={"api_poll_chat_ids": [chat_id]})
    )
    _reset_api_poll_persistence(adapter, tmp_path / "token-network.json")
    adapter._api_poll_baselined_chat_ids.add(chat_id)
    adapter._api_poll_last_seen_create_time_ms[chat_id] = 1_000_000
    adapter._commit_api_poll_scan_state(
        chat_id,
        {
            "page_token": "keep-on-network-error",
            "watermark_ms": 1_000_000,
            "cursor_message_ids": [],
            "candidates": [],
            "baselined": True,
        },
    )
    before = adapter._api_poll_state_path.read_bytes()
    adapter._fetch_recent_chat_messages_via_api = Mock(
        side_effect=TimeoutError("network timeout")
    )

    with pytest.raises(TimeoutError, match="network timeout"):
        await adapter._poll_api_chat_once(chat_id)

    assert adapter._api_poll_scan_state[chat_id]["page_token"] == (
        "keep-on-network-error"
    )
    assert adapter._api_poll_state_path.read_bytes() == before


def test_api_poll_sidecar_survives_legacy_v2_writer(tmp_path):
    chat_id = G1Q3_RCA_GROUP_ID
    state_path = tmp_path / "api-poll-sidecar.json"
    adapter = FeishuAdapter(
        PlatformConfig(extra={"api_poll_chat_ids": [chat_id]})
    )
    _reset_api_poll_persistence(adapter, state_path)
    adapter._api_poll_pending_items[chat_id] = [
        FeishuAdapter._validated_api_poll_pending_item(
            _api_poll_test_item(chat_id, "om_legacy_pending"),
            expected_chat_id=chat_id,
        )
    ]
    adapter._api_poll_baselined_chat_ids.add(chat_id)
    adapter._api_poll_discovery_floor_ms[chat_id] = 1_000_000
    adapter._api_poll_scan_state[chat_id] = {
        "page_token": "legacy-token",
        "watermark_ms": 1_000_000,
        "cursor_message_ids": [],
        "candidates": [],
        "baselined": True,
    }
    adapter._api_poll_terminal_holes.append(
        {
            "schema_version": "feishu_api_poll_terminal_hole_v1",
            "kind": "admission_terminal",
            "status": "dead",
            "message_id": "om_old_hole",
            "chat_id": chat_id,
            "create_time": "900",
            "sender_id": "ou_user",
            "payload_sha256": "",
            "original_message_id": "",
            "error": "admission_dead",
            "admission_item_id": "queue-old",
            "recorded_at": 1.0,
        }
    )
    legacy_state = json.loads(json.dumps(adapter._api_poll_state_snapshot()))
    adapter._dedup_state_path.write_text(
        json.dumps(
            {
                "schema_version": "feishu_message_inbox_v2",
                "messages": {},
                "message_ids": {},
                "api_poll": legacy_state,
            }
        ),
        encoding="utf-8",
    )
    adapter._api_poll_pending_items.clear()
    adapter._api_poll_scan_state.clear()
    adapter._api_poll_terminal_holes.clear()

    adapter._load_seen_message_ids()

    assert state_path.exists()
    sidecar = json.loads(state_path.read_text(encoding="utf-8"))
    assert sidecar["schema_version"] == "feishu_api_poll_state_v1"
    assert sidecar["rollback_readiness"] == {
        "ready": False,
        "pending_count": 1,
        "scan_continuation_count": 1,
    }
    sidecar_before_inbox_write = state_path.read_bytes()
    adapter._persist_seen_message_ids(require_success=True)
    rewritten_inbox = json.loads(
        adapter._dedup_state_path.read_text(encoding="utf-8")
    )
    assert "api_poll" not in rewritten_inbox
    assert state_path.read_bytes() == sidecar_before_inbox_write
    # Simulate the pre-sidecar writer replacing v2 without preserving unknown keys.
    adapter._dedup_state_path.write_text(
        json.dumps(
            {
                "schema_version": "feishu_message_inbox_v2",
                "messages": {},
                "message_ids": {},
            }
        ),
        encoding="utf-8",
    )
    restarted = FeishuAdapter(
        PlatformConfig(extra={"api_poll_chat_ids": [chat_id]})
    )
    _reset_api_poll_persistence(restarted, state_path)
    restarted._load_seen_message_ids()

    assert restarted._api_poll_pending_items[chat_id][0]["message_id"] == (
        "om_legacy_pending"
    )
    assert restarted._api_poll_scan_state[chat_id]["page_token"] == "legacy-token"
    assert restarted._api_poll_terminal_holes[0]["admission_item_id"] == "queue-old"
    status = restarted.api_poll_persistence_status()
    assert status["rollback_ready"] is False
    assert status["rollback_blocking_pending_count"] == 1
    assert status["rollback_blocking_scan_continuation_count"] == 1


def test_api_poll_poison_is_bounded_and_namespaced_by_chat():
    chat_a = G1Q3_RCA_GROUP_ID
    chat_b = PNC_ALL_BUSINESS_TEST_GROUP_ID
    base = _api_poll_test_item(chat_b, "om_original")
    for field_path in ("message_id", "root_id", "msg_type", "sender.id"):
        item = json.loads(json.dumps(base))
        if field_path == "sender.id":
            item["sender"]["id"] = "x" * (200 * 1024)
        else:
            item[field_path] = "x" * (200 * 1024)
        stub = FeishuAdapter._validated_api_poll_pending_item(
            item,
            expected_chat_id=chat_a,
        )
        encoded = json.dumps(stub, ensure_ascii=False).encode("utf-8")
        assert len(encoded) <= 128 * 1024
        assert stub["message_id"].startswith("poison-")
        assert stub["message_id"] != item.get("message_id")

    poison_a = FeishuAdapter._validated_api_poll_pending_item(
        base,
        expected_chat_id=chat_a,
    )
    poison_b = FeishuAdapter._validated_api_poll_pending_item(
        {**base, "mentions": ["invalid"]},
        expected_chat_id=chat_b,
    )
    same_malformed_a = FeishuAdapter._validated_api_poll_pending_item(
        {**base, "mentions": ["invalid"]},
        expected_chat_id=chat_a,
    )
    assert poison_b["message_id"] != same_malformed_a["message_id"]
    assert poison_a["message_id"] != "om_original"
    assert poison_b["message_id"] != "om_original"


@pytest.mark.asyncio
async def test_api_poll_chat_mismatch_poison_does_not_hide_valid_chat_message(tmp_path):
    chat_a = G1Q3_RCA_GROUP_ID
    chat_b = PNC_ALL_BUSINESS_TEST_GROUP_ID
    adapter = FeishuAdapter(
        PlatformConfig(extra={"api_poll_chat_ids": [chat_a, chat_b]})
    )
    _reset_api_poll_persistence(adapter, tmp_path / "cross-chat-poison.json")
    adapter._api_poll_started_at_ms = 1_500_000
    item = _api_poll_test_item(chat_b, "om_cross_chat")
    adapter._fetch_recent_chat_messages_via_api = Mock(side_effect=[[item], [item]])
    adapter._handle_message_event_data = AsyncMock(return_value=True)

    await adapter._poll_api_chat_once(chat_a)

    assert "om_cross_chat" not in adapter._api_poll_seen_message_ids
    [hole] = adapter._api_poll_terminal_holes
    assert hole["original_message_id"] == "om_cross_chat"
    assert hole["message_id"].startswith("poison-")

    await adapter._poll_api_chat_once(chat_b)

    replayed = adapter._handle_message_event_data.await_args.args[0]
    assert replayed.event.message.message_id == "om_cross_chat"
    assert "om_cross_chat" in adapter._api_poll_seen_message_ids


def test_api_poll_per_chat_budget_preserves_other_chat_state(tmp_path, monkeypatch):
    chat_a = G1Q3_RCA_GROUP_ID
    chat_b = PNC_ALL_BUSINESS_TEST_GROUP_ID
    monkeypatch.setattr(
        "gateway.platforms.feishu._MAX_API_POLL_PER_CHAT_BYTES",
        2400,
    )
    monkeypatch.setattr(
        "gateway.platforms.feishu._MAX_API_POLL_TOTAL_BYTES",
        7000,
    )
    adapter = FeishuAdapter(
        PlatformConfig(extra={"api_poll_chat_ids": [chat_a, chat_b]})
    )
    _reset_api_poll_persistence(adapter, tmp_path / "chat-budget.json")
    adapter._commit_api_poll_discovery(
        chat_a,
        pending_items=[
            _api_poll_test_item(chat_a, "om_chat_a", text="a" * 900)
        ],
        mark_baselined=True,
    )
    adapter._commit_api_poll_discovery(
        chat_b,
        pending_items=[_api_poll_test_item(chat_b, "om_chat_b", text="b" * 100)],
        mark_baselined=True,
    )
    before_b = json.loads(json.dumps(adapter._api_poll_pending_items[chat_b]))

    with pytest.raises(RuntimeError, match="sidecar write failed"):
        adapter._commit_api_poll_discovery(
            chat_a,
            pending_items=[
                _api_poll_test_item(chat_a, "om_chat_a_over", text="x" * 1800)
            ],
        )

    assert adapter._api_poll_pending_items[chat_b] == before_b
    assert [
        item["message_id"] for item in adapter._api_poll_pending_items[chat_a]
    ] == ["om_chat_a"]
    sidecar_bytes = adapter._api_poll_state_path.read_bytes()
    assert len(sidecar_bytes) <= 7000
    persisted = json.loads(sidecar_bytes)
    assert set(persisted["state"]["pending"]) == {chat_a, chat_b}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("poison_case", "expected_code"),
    [
        ("oversized", "item_exceeds_128_kib"),
        ("malformed", "invalid_mention"),
    ],
)
async def test_api_poll_poison_item_enters_hole_and_does_not_block_chat(
    tmp_path,
    poison_case,
    expected_code,
):
    chat_id = G1Q3_RCA_GROUP_ID
    adapter = FeishuAdapter(
        PlatformConfig(extra={"api_poll_chat_ids": [chat_id]})
    )
    _reset_api_poll_persistence(adapter, tmp_path / f"poison-{poison_case}.json")
    adapter._api_poll_started_at_ms = 1_500_000
    item = _api_poll_test_item(chat_id, f"om_poison_{poison_case}")
    if poison_case == "oversized":
        item["body"]["content"] = "x" * (128 * 1024)
    else:
        item["mentions"] = ["not-an-object"]
    adapter._fetch_recent_chat_messages_via_api = Mock(return_value=[item])
    adapter._handle_message_event_data = AsyncMock(
        side_effect=AssertionError("poison item must not reach callback")
    )

    await adapter._poll_api_chat_once(chat_id)

    adapter._handle_message_event_data.assert_not_awaited()
    assert adapter._api_poll_pending_items == {}
    assert adapter._api_poll_last_seen_create_time_ms[chat_id] == 2_000_000
    status = adapter.api_poll_persistence_status()
    assert status["pending_count"] == 0
    assert status["terminal_hole_count"] == 1
    persisted = json.loads(adapter._api_poll_state_path.read_text(encoding="utf-8"))
    [hole] = persisted["state"]["terminal_holes"]
    assert hole["kind"] == "poison"
    assert hole["error"] == expected_code
    assert len(hole["payload_sha256"]) == 64
    assert hole["original_message_id"] == f"om_poison_{poison_case}"
    assert "x" * 1024 not in adapter._api_poll_state_path.read_text(encoding="utf-8")


@pytest.mark.asyncio
@pytest.mark.parametrize("capacity_case", ["aggregate", "per_chat"])
async def test_api_poll_capacity_failure_has_no_partial_state_or_callback(
    tmp_path,
    monkeypatch,
    capacity_case,
):
    chat_id = G1Q3_RCA_GROUP_ID
    adapter = FeishuAdapter(
        PlatformConfig(extra={"api_poll_chat_ids": [chat_id]})
    )
    _reset_api_poll_persistence(adapter, tmp_path / f"{capacity_case}.json")
    adapter._persist_api_poll_state(require_success=True)
    before = adapter._api_poll_state_path.read_bytes()
    adapter._api_poll_started_at_ms = 1_500_000
    if capacity_case == "aggregate":
        monkeypatch.setattr(
            "gateway.platforms.feishu._MAX_API_POLL_TOTAL_BYTES",
            700,
        )
        items = [
            _api_poll_test_item(chat_id, "om_aggregate_a", text="x" * 300),
            _api_poll_test_item(chat_id, "om_aggregate_b", text="y" * 300),
        ]
        error = "sidecar write failed"
    else:
        monkeypatch.setattr(
            "gateway.platforms.feishu._MAX_API_POLL_PER_CHAT_BYTES",
            400,
        )
        items = [_api_poll_test_item(chat_id, "om_per_chat")]
        error = "sidecar write failed"
    adapter._fetch_recent_chat_messages_via_api = Mock(return_value=items)
    adapter._handle_message_event_data = AsyncMock(return_value=True)

    with pytest.raises((ValueError, RuntimeError), match=error):
        await adapter._poll_api_chat_once(chat_id)

    adapter._handle_message_event_data.assert_not_awaited()
    assert adapter._api_poll_pending_items == {}
    assert adapter._api_poll_baselined_chat_ids == set()
    assert adapter._api_poll_last_seen_create_time_ms == {}
    assert adapter._api_poll_state_path.read_bytes() == before


@pytest.mark.asyncio
async def test_api_poll_persistence_failure_rolls_back_before_callback(
    tmp_path,
    monkeypatch,
):
    chat_id = G1Q3_RCA_GROUP_ID
    adapter = FeishuAdapter(
        PlatformConfig(extra={"api_poll_chat_ids": [chat_id]})
    )
    _reset_api_poll_persistence(adapter, tmp_path / "write-failure.json")
    adapter._persist_api_poll_state(require_success=True)
    before = adapter._api_poll_state_path.read_bytes()
    adapter._api_poll_started_at_ms = 1_500_000
    adapter._fetch_recent_chat_messages_via_api = Mock(
        return_value=[_api_poll_test_item(chat_id, "om_write_failure")]
    )
    adapter._handle_message_event_data = AsyncMock(return_value=True)
    monkeypatch.setattr(
        "gateway.platforms.feishu.atomic_json_write",
        Mock(side_effect=OSError("disk unavailable")),
    )

    with pytest.raises(RuntimeError, match="sidecar write failed"):
        await adapter._poll_api_chat_once(chat_id)

    adapter._handle_message_event_data.assert_not_awaited()
    assert adapter._api_poll_pending_items == {}
    assert adapter._api_poll_baselined_chat_ids == set()
    assert adapter._api_poll_last_seen_create_time_ms == {}
    assert adapter._api_poll_state_path.read_bytes() == before


@pytest.mark.asyncio
async def test_api_poll_corrupt_persistent_inbox_fails_closed_without_overwrite(tmp_path):
    chat_id = G1Q3_RCA_GROUP_ID
    adapter = FeishuAdapter(
        PlatformConfig(extra={"api_poll_chat_ids": [chat_id]})
    )
    state_path = tmp_path / "corrupt.json"
    _reset_api_poll_persistence(adapter, state_path)
    state_path.write_text("{not-json", encoding="utf-8")

    adapter._load_seen_message_ids()

    assert adapter._api_poll_state_error
    assert adapter._api_poll_block_persistence is True
    adapter._fetch_recent_chat_messages_via_api = Mock(
        side_effect=AssertionError("unhealthy state must block API fetch")
    )
    with pytest.raises(RuntimeError, match="persistent state is unhealthy"):
        await adapter._poll_api_chat_once(chat_id)
    assert adapter._persist_seen_message_ids() is False
    assert state_path.read_text(encoding="utf-8") == "{not-json"


@pytest.mark.asyncio
async def test_queue_worker_keeps_same_domain_id_serial_and_different_domain_ids_parallel(tmp_path):
    ctrl = AdmissionController(db_path=tmp_path / "queue.db", audit_dir=tmp_path / "audit")
    # Same group/domain_id should be serial even when max_concurrent_per_domain allows parallelism.
    _, _, same_1 = await ctrl.admit("u1", "first standard task", chat_id="group-a", chat_type="group", platform="feishu")
    _, _, same_2 = await ctrl.admit("u2", "second standard task", chat_id="group-a", chat_type="group", platform="feishu")
    # Different group/domain_id should be allowed to overlap.
    _, _, other = await ctrl.admit("u3", "third standard task", chat_id="group-b", chat_type="group", platform="feishu")

    starts: dict[str, float] = {}
    ends: dict[str, float] = {}

    async def handler(item):
        starts[item.id] = asyncio.get_event_loop().time()
        await asyncio.sleep(0.15)
        ends[item.id] = asyncio.get_event_loop().time()
        return {"status": "completed"}

    worker = QueueWorker(ctrl, handler, max_concurrent_per_domain=3)
    await worker.start()
    await asyncio.sleep(0.6)
    await worker.stop()

    assert {same_1.id, same_2.id, other.id}.issubset(starts)
    first_same, second_same = sorted([same_1, same_2], key=lambda item: starts[item.id])
    assert starts[second_same.id] >= ends[first_same.id]
    assert starts[other.id] < ends[first_same.id]


@pytest.mark.asyncio
async def test_queue_worker_does_not_dequeue_more_domain_ids_than_available_domain_slots(tmp_path):
    ctrl = AdmissionController(db_path=tmp_path / "queue.db", audit_dir=tmp_path / "audit")
    _, _, first = await ctrl.admit("u1", "first standard task", chat_id="group-a", chat_type="group", platform="feishu")
    _, _, second = await ctrl.admit("u2", "second standard task", chat_id="group-b", chat_type="group", platform="feishu")

    started = asyncio.Event()

    async def handler(item):
        started.set()
        await asyncio.sleep(0.8)
        return {"status": "completed"}

    worker = QueueWorker(ctrl, handler, max_concurrent_per_domain=1)
    await worker.start()
    await asyncio.wait_for(started.wait(), timeout=1)

    statuses = {first.id: first.status, second.id: second.status}
    await worker.stop(drain_timeout=1.0)

    assert sorted(statuses.values()) == ["processing", "queued"]
    assert second.status == "queued"


@pytest.mark.asyncio
async def test_queue_worker_uses_public_slot_accounting_without_private_semaphore_value(tmp_path, monkeypatch):
    class PublicSemaphore:
        def __init__(self, value):
            self._available = value

        async def acquire(self):
            while self._available <= 0:
                await asyncio.sleep(0.01)
            self._available -= 1

        def release(self):
            self._available += 1

    monkeypatch.setattr("gateway.admission.worker.asyncio.Semaphore", PublicSemaphore)
    ctrl = AdmissionController(db_path=tmp_path / "queue.db", audit_dir=tmp_path / "audit")
    _, _, item = await ctrl.admit("u1", "standard public semaphore task", chat_id="group-a", chat_type="group", platform="feishu")
    processed = []

    async def handler(queue_item):
        processed.append(queue_item.id)
        return {"status": "completed"}

    worker = QueueWorker(ctrl, handler, max_concurrent_per_domain=1)
    await worker.start()
    await asyncio.sleep(0.2)
    await worker.stop()

    assert processed == [item.id]
    assert item.status == "completed"


def test_queue_worker_rejects_non_positive_domain_concurrency(tmp_path):
    ctrl = AdmissionController(db_path=tmp_path / "queue.db", audit_dir=tmp_path / "audit")

    async def handler(queue_item):
        return {"status": "completed"}

    with pytest.raises(ValueError, match="max_concurrent_per_domain"):
        QueueWorker(ctrl, handler, max_concurrent_per_domain=0)
    with pytest.raises(ValueError, match="max_concurrent_per_domain"):
        QueueWorker(ctrl, handler, max_concurrent_per_domain=-1)
    with pytest.raises(ValueError, match="max_concurrent_per_domain"):
        QueueWorker(ctrl, handler, max_concurrent_per_domain=1.5)
    with pytest.raises(ValueError, match="max_concurrent_per_domain"):
        QueueWorker(ctrl, handler, max_concurrent_per_domain=True)

@pytest.mark.asyncio
async def test_feishu_business_admission_uses_common_intake_ack_and_fast_reply(monkeypatch):
    adapter = _adapter_without_init(lane="standard")
    monkeypatch.setattr("gateway.platforms.feishu._integration_tools_intake_chat_ids", lambda: {"oc_it"})
    event = MessageEvent(
        source=SessionSource(
            platform="feishu",
            user_id="ou_user",
            chat_id="oc_it",
            chat_type="group",
            thread_id="topic:om_topic",
        ),
        text="我想用 logsim 回放一包 mcap，之前听说脚本没有纯 help 路径，怎么安全发起？",
        message_type=MessageType.TEXT,
        message_id="om_request",
    )

    await adapter._dispatch_inbound_event(event)

    assert adapter._admission_controller.calls == []
    assert len(adapter.sent) == 2
    assert adapter.sent[0]["metadata"] == {"thread_id": "topic:om_topic"}
    assert "不会直接触发 VM 长任务" in adapter.sent[0]["content"]
    assert "不要在主仓直接执行业务脚本" in adapter.sent[1]["content"]
    assert "受限 runner" in adapter.sent[1]["content"]


@pytest.mark.asyncio
async def test_feishu_foxglove_planning_topic_question_fast_replies_without_admission(monkeypatch):
    adapter = _adapter_without_init(lane="standard")
    monkeypatch.setattr("gateway.platforms.feishu._integration_tools_intake_chat_ids", lambda: {"oc_it"})
    event = MessageEvent(
        source=SessionSource(
            platform="feishu",
            user_id="ou_user",
            chat_id="oc_it",
            chat_type="group",
            thread_id="topic:om_topic",
        ),
        text="foxglove 打开后没有 planning topic，应该收集哪些信息？是不是可以直接跑 run_planning_visualization.sh 看看？",
        message_type=MessageType.TEXT,
        message_id="om_request",
    )

    await adapter._dispatch_inbound_event(event)

    assert adapter._admission_controller.calls == []
    assert len(adapter.sent) == 2
    assert "不会直接触发 VM 长任务" in adapter.sent[0]["content"]
    assert "planning topic" in adapter.sent[1]["content"]
    assert "不要直接在主仓裸跑" in adapter.sent[1]["content"]


@pytest.mark.asyncio
async def test_feishu_generic_heavy_feedback_behavior_preserved(monkeypatch):
    adapter = _adapter_without_init(lane="heavy")
    monkeypatch.setattr("gateway.platforms.feishu._is_integration_tools_intake_chat", lambda chat_id: False)
    event = MessageEvent(
        source=SessionSource(
            platform="feishu",
            user_id="ou_user",
            chat_id="oc_g1q3_or_generic",
            chat_type="group",
            thread_id="topic:om_topic",
        ),
        text="请提交一个 VM heavy 任务",
        message_type=MessageType.TEXT,
        message_id="om_request",
    )

    await adapter._dispatch_inbound_event(event)

    assert len(adapter._admission_controller.calls) == 1
    assert len(adapter.sent) == 1
    assert adapter.sent[0]["metadata"] == {"thread_id": "topic:om_topic"}
    assert "heavy/VM" in adapter.sent[0]["content"]


@pytest.mark.asyncio
async def test_feishu_all_business_test_group_g1q3_prompt_not_claimed_by_integration_tools_admission(monkeypatch):
    adapter = _adapter_without_init(lane="standard")
    monkeypatch.setattr("gateway.platforms.feishu._integration_tools_intake_chat_ids", lambda: {"oc_16614f4ba25b8c88b69c0b8e9ebc2fb5"})
    event = MessageEvent(
        source=SessionSource(
            platform="feishu",
            user_id="ou_user",
            chat_id="oc_16614f4ba25b8c88b69c0b8e9ebc2fb5",
            chat_type="group",
            thread_id="topic:om_topic",
        ),
        text="帮我看一下 case G1Q3-042 现在归因做到哪一步了",
        message_type=MessageType.TEXT,
        message_id="om_request",
    )

    await adapter._dispatch_inbound_event(event)

    assert len(adapter._admission_controller.calls) == 1
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_metadata_only_issue_card_is_not_claimed_by_integration_tools_and_survives_queue(
    monkeypatch,
):
    adapter = _adapter_without_init(lane="standard")
    monkeypatch.setattr(
        "gateway.platforms.feishu._integration_tools_intake_chat_ids",
        lambda: {PNC_ALL_BUSINESS_TEST_GROUP_ID},
    )
    issue_url = (
        "https://project.feishu.cn/t03o4q/issue/detail/7003183096?openScene=4"
    )
    event = MessageEvent(
        source=SessionSource(
            platform="feishu",
            user_id="ou_user",
            chat_id=PNC_ALL_BUSINESS_TEST_GROUP_ID,
            chat_type="group",
            thread_id="topic:om_topic",
        ),
        text="请回放这个 mcap 并分析问题卡片",
        message_type=MessageType.TEXT,
        message_id="om_request",
        metadata={"feishu": {"link_urls": [issue_url]}},
    )

    await adapter._dispatch_inbound_event(event)

    assert len(adapter._admission_controller.calls) == 1
    queued_message = adapter._admission_controller.calls[0]["message"]
    assert issue_url in queued_message
    assert _looks_like_g1q3_rca_request_for_admission(queued_message) is True
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_dedicated_integration_chat_cannot_claim_metadata_issue_url(
    monkeypatch,
):
    adapter = _adapter_without_init(lane="standard")
    monkeypatch.setattr(
        "gateway.platforms.feishu._integration_tools_intake_chat_ids",
        lambda: {"oc_integration_tools"},
    )
    issue_url = "https://project.feishu.cn/t03o4q/issue/detail/7003183096"
    event = MessageEvent(
        source=SessionSource(
            platform="feishu",
            user_id="ou_user",
            chat_id="oc_integration_tools",
            chat_type="group",
            thread_id="topic:om_topic",
        ),
        text="请直接回放 mcap 并分析这个问题卡片",
        message_type=MessageType.TEXT,
        message_id="om_request",
        metadata={"feishu": {"link_urls": [issue_url]}},
    )

    await adapter._dispatch_inbound_event(event)

    assert len(adapter._admission_controller.calls) == 1
    assert issue_url in adapter._admission_controller.calls[0]["message"]
    # An integration-tools fast reply would send two messages and terminate
    # before admission; the issue boundary must instead preserve the event for
    # the gateway's Kafka-only read-only route.
    assert adapter.sent == []


def test_admission_rca_classifier_accepts_issue_url_but_not_mcap_only_text():
    assert _looks_like_g1q3_rca_request_for_admission(
        "https://project.feishu.cn/t03o4q/issue/detail/7003183096"
    ) is True
    assert _looks_like_g1q3_rca_request_for_admission(
        "请回放这个 mcap"
    ) is False


@pytest.mark.asyncio
async def test_feishu_all_business_test_group_unknown_prompt_not_claimed_by_integration_tools_admission(monkeypatch):
    adapter = _adapter_without_init(lane="standard")
    monkeypatch.setattr("gateway.platforms.feishu._integration_tools_intake_chat_ids", lambda: {"oc_16614f4ba25b8c88b69c0b8e9ebc2fb5"})
    event = MessageEvent(
        source=SessionSource(
            platform="feishu",
            user_id="ou_user",
            chat_id="oc_16614f4ba25b8c88b69c0b8e9ebc2fb5",
            chat_type="group",
            thread_id="topic:om_topic",
        ),
        text="今天下午谁有空看下这个问题",
        message_type=MessageType.TEXT,
        message_id="om_request",
    )

    await adapter._dispatch_inbound_event(event)

    assert len(adapter._admission_controller.calls) == 1
    assert adapter.sent == []
