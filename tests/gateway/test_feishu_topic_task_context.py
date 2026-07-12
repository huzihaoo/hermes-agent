import pytest

from gateway.config import GatewayConfig, Platform, PlatformConfig
from gateway.platforms.base import MessageEvent
from gateway.run import GatewayRunner
from gateway.session import SessionSource
from gateway.tasks.store import TaskStore
from gateway.tasks.types import Task, TaskStatus, TaskType


def _make_runner(tmp_path):
    runner = GatewayRunner.__new__(GatewayRunner)
    runner.config = GatewayConfig(
        platforms={Platform.FEISHU: PlatformConfig(enabled=True, token="fake")},
    )
    runner.adapters = {}
    runner._model = "openai/gpt-4.1-mini"  # type: ignore[attr-defined]
    runner._base_url = None  # type: ignore[attr-defined]
    runner._decide_image_input_mode = lambda: "text"
    return runner


def _source():
    return SessionSource(
        platform=Platform.FEISHU,
        chat_id="oc_test_chat",
        chat_type="group",
        user_id="ou_user",
        thread_id="topic:om_root_123",
    )


def _insert_topic_task(store: TaskStore, task_id: str = "task_same_topic") -> None:
    store.upsert(
        Task(
            task_id=task_id,
            status=TaskStatus.RUNNING,
            task_type=TaskType.CHAT,
            user_id="ou_user",
            platform="feishu",
            request_summary="当前 topic 的评测任务",
            started_at=1000.0,
            chat_id="oc_test_chat",
            chat_type="group",
            thread_id="topic:om_root_123",
            message_id="om_root_123",
        )
    )


@pytest.mark.asyncio
async def test_prepare_inbound_message_text_appends_current_topic_task_context(tmp_path, monkeypatch):
    import gateway.run as gateway_run

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    runner = _make_runner(tmp_path)
    source = _source()
    store = TaskStore(db_path=tmp_path / "analytics" / "tasks.db")
    store.upsert(
        Task(
            task_id="task_feishu_topic_1",
            status=TaskStatus.COMPLETED,
            task_type=TaskType.CHAT,
            user_id="ou_user",
            platform="feishu",
            request_summary="dev-nop-d4q2-ota 全量评测，结果卡片发到当前群话题",
            started_at=1000.0,
            chat_id="oc_test_chat",
            chat_type="group",
            thread_id="topic:om_root_123",
            message_id="om_root_123",
            receipt_path="/tmp/receipt.json",
            delivery_verified=False,
        )
    )
    event = MessageEvent(
        text="报告为什么没有贴出来？",
        source=source,
        message_id="om_reply_1",
        metadata={
            "feishu": {
                "message_id": "om_reply_1",
                "root_id": "om_root_123",
                "thread_id": "topic:om_root_123",
                "is_topic": True,
            }
        },
    )

    result = await runner._prepare_inbound_message_text(event=event, source=source, history=[])
    assert result is not None

    assert "报告为什么没有贴出来？" in result
    assert "[Task context]" in result
    assert "Use this current topic task context first." in result
    assert "Current topic task_id: task_feishu_topic_1" in result
    assert "status: completed" in result
    assert "dev-nop-d4q2-ota 全量评测" in result
    assert "delivery_verified: no" in result
    assert "/tmp/receipt.json" in result


@pytest.mark.asyncio
async def test_prepare_inbound_message_text_prefers_same_topic_not_unrelated_chat_task(tmp_path, monkeypatch):
    import gateway.run as gateway_run

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    runner = _make_runner(tmp_path)
    source = _source()
    store = TaskStore(db_path=tmp_path / "analytics" / "tasks.db")
    store.upsert(
        Task(
            task_id="task_other_topic",
            status=TaskStatus.COMPLETED,
            task_type=TaskType.CHAT,
            user_id="ou_user",
            platform="feishu",
            request_summary="不相关的 G1Q3 RCA 方案",
            started_at=2000.0,
            chat_id="oc_test_chat",
            chat_type="group",
            thread_id="topic:om_other_root",
            message_id="om_other_root",
        )
    )
    _insert_topic_task(store)
    event = MessageEvent(
        text="现在怎么样",
        source=source,
        message_id="om_reply_2",
        metadata={"feishu": {"root_id": "om_root_123", "thread_id": "topic:om_root_123", "is_topic": True}},
    )

    result = await runner._prepare_inbound_message_text(event=event, source=source, history=[])
    assert result is not None

    assert "task_same_topic" in result
    assert "当前 topic 的评测任务" in result
    assert "task_other_topic" not in result
    assert "不相关的 G1Q3 RCA 方案" not in result


@pytest.mark.asyncio
async def test_prepare_inbound_message_text_does_not_inject_task_context_outside_feishu_topic(tmp_path, monkeypatch):
    import gateway.run as gateway_run

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    runner = _make_runner(tmp_path)
    store = TaskStore(db_path=tmp_path / "analytics" / "tasks.db")
    _insert_topic_task(store, task_id="task_should_not_leak")
    source = SessionSource(
        platform=Platform.TELEGRAM,
        chat_id="oc_test_chat",
        chat_type="group",
        user_id="ou_user",
        thread_id="topic:om_root_123",
    )
    event = MessageEvent(text="普通消息", source=source, message_id="msg_1")

    result = await runner._prepare_inbound_message_text(event=event, source=source, history=[])
    assert result == "普通消息"
    assert "[Task context]" not in result
    assert "task_should_not_leak" not in result


@pytest.mark.asyncio
async def test_prepare_inbound_message_text_does_not_inject_task_context_without_topic(tmp_path, monkeypatch):
    import gateway.run as gateway_run

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    runner = _make_runner(tmp_path)
    store = TaskStore(db_path=tmp_path / "analytics" / "tasks.db")
    _insert_topic_task(store, task_id="task_without_topic_should_not_leak")
    source = SessionSource(
        platform=Platform.FEISHU,
        chat_id="oc_test_chat",
        chat_type="group",
        user_id="ou_user",
        thread_id=None,
    )
    event = MessageEvent(text="群里普通消息", source=source, message_id="msg_2")

    result = await runner._prepare_inbound_message_text(event=event, source=source, history=[])
    assert result == "群里普通消息"
    assert "[Task context]" not in result
    assert "task_without_topic_should_not_leak" not in result
