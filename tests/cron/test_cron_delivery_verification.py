"""Tests for cron delivery verification metadata."""

import asyncio
from types import SimpleNamespace


def test_deliver_output_records_message_metadata_from_live_adapter(monkeypatch):
    import cron.scheduler as scheduler

    job = {"id": "job-feishu", "deliver": "feishu:oc_chat:om_topic", "name": "topic job"}
    sent = []
    deliveries = []

    class DummyFuture:
        def result(self, timeout=None):
            return SimpleNamespace(success=True, message_id="om_reply")

    class DummyLoop:
        def is_running(self):
            return True

    class DummyAdapter:
        async def send(self, chat_id, text, metadata=None):
            sent.append((chat_id, text, metadata))

    def fake_run_coroutine_threadsafe(coro, loop):
        asyncio.run(coro)
        return DummyFuture()

    monkeypatch.setattr(scheduler, "_resolve_delivery_targets", lambda _job: [{"platform": "feishu", "chat_id": "oc_chat", "thread_id": "om_topic"}])
    monkeypatch.setattr(scheduler, "load_config", lambda: {"cron": {"wrap_response": False}})
    import gateway.config as gateway_config
    platform = gateway_config.Platform("feishu")
    monkeypatch.setattr(gateway_config, "load_gateway_config", lambda: SimpleNamespace(platforms={platform: SimpleNamespace(enabled=True)}))
    monkeypatch.setattr(scheduler.asyncio, "run_coroutine_threadsafe", fake_run_coroutine_threadsafe)
    monkeypatch.setattr(scheduler, "_record_delivery_verification", lambda **kwargs: deliveries.append(kwargs))

    err = scheduler._deliver_result(job, "hello", adapters={platform: DummyAdapter()}, loop=DummyLoop())

    assert err is None
    assert sent == [("oc_chat", "hello", {"thread_id": "om_topic"})]
    assert deliveries == [{
        "job": job,
        "platform_name": "feishu",
        "chat_id": "oc_chat",
        "thread_id": "om_topic",
        "message_id": "om_reply",
        "delivery_verified": True,
        "failure_reason": None,
    }]


def test_record_delivery_verification_updates_task_store(tmp_path, monkeypatch):
    import cron.scheduler as scheduler
    from gateway.tasks.store import TaskStore
    from gateway.tasks.types import Task, TaskStatus, TaskType

    db_path = tmp_path / "tasks.db"
    store = TaskStore(db_path=db_path)
    store.upsert(Task(task_id="task-1", status=TaskStatus.RUNNING, task_type=TaskType.CRON, user_id="ou_user", platform="feishu", request_summary="cron", started_at=1.0))

    monkeypatch.setattr(scheduler, "_make_task_store", lambda: TaskStore(db_path=db_path), raising=False)

    scheduler._record_delivery_verification(
        job={"id": "job-1", "task_id": "task-1"},
        platform_name="feishu",
        chat_id="oc_chat",
        thread_id="om_topic",
        message_id="om_reply",
        delivery_verified=True,
        failure_reason=None,
    )

    task = store.get("task-1")
    assert task is not None
    assert task.chat_id == "oc_chat"
    assert task.thread_id == "om_topic"
    assert task.message_id == "om_reply"
    assert task.delivery_verified is True


def test_make_task_store_uses_default_analytics_tasks_db(tmp_path, monkeypatch):
    import cron.scheduler as scheduler

    hermes_home = tmp_path / "hermes-home"
    monkeypatch.setattr(scheduler, "get_hermes_home", lambda: hermes_home)

    store = scheduler._make_task_store()

    assert store.db_path == hermes_home / "analytics" / "tasks.db"
