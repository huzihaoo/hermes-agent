"""Admission routing regressions for Feishu topic preservation and worker fairness."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from gateway.admission.controller import AdmissionController
from gateway.admission.types import QueueItem
from gateway.admission.worker import QueueWorker
from gateway.config import PlatformConfig
from gateway.platforms.base import MessageEvent, MessageType
from gateway.platforms.feishu import FeishuAdapter
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
