"""Tests for task cancel and retry — store methods + gateway commands."""

import time
from types import SimpleNamespace

import pytest

from gateway.tasks.store import TaskStore
from gateway.tasks.types import Task, TaskStatus, TaskType


@pytest.fixture
def store(tmp_path):
    return TaskStore(db_path=tmp_path / "tasks.db")


def _make_task(task_id, status=TaskStatus.RUNNING, user_id="alice", **kw):
    return Task(
        task_id=task_id,
        status=status,
        task_type=TaskType.CHAT,
        user_id=user_id,
        platform="feishu",
        request_summary="test task",
        started_at=time.time(),
        **kw,
    )


# ── Store: cancel_task ──────────────────────────────────────────────

def test_cancel_running_task(store):
    store.upsert(_make_task("t1", TaskStatus.RUNNING))
    assert store.cancel_task("t1") is True
    task = store.get("t1")
    assert task.status == TaskStatus.CANCELLED
    assert task.completed_at is not None


def test_cancel_pending_task(store):
    store.upsert(_make_task("t2", TaskStatus.PENDING))
    assert store.cancel_task("t2") is True
    assert store.get("t2").status == TaskStatus.CANCELLED


def test_cancel_completed_task_returns_false(store):
    store.upsert(_make_task("t3", TaskStatus.COMPLETED))
    assert store.cancel_task("t3") is False
    assert store.get("t3").status == TaskStatus.COMPLETED


def test_cancel_already_cancelled_returns_false(store):
    store.upsert(_make_task("t4", TaskStatus.CANCELLED))
    assert store.cancel_task("t4") is False


def test_cancel_failed_task_returns_false(store):
    store.upsert(_make_task("t5", TaskStatus.FAILED))
    assert store.cancel_task("t5") is False


def test_cancel_nonexistent_returns_false(store):
    assert store.cancel_task("nope") is False


# ── Store: retry_task ───────────────────────────────────────────────

def test_retry_failed_task(store):
    store.upsert(_make_task("t10", TaskStatus.FAILED, completed_at=time.time()))
    updated = store.retry_task("t10")
    assert updated is not None
    assert updated.status == TaskStatus.PENDING
    assert updated.completed_at is None


def test_retry_cancelled_task(store):
    store.upsert(_make_task("t11", TaskStatus.CANCELLED, completed_at=time.time()))
    updated = store.retry_task("t11")
    assert updated is not None
    assert updated.status == TaskStatus.PENDING


def test_retry_running_task_returns_none(store):
    store.upsert(_make_task("t12", TaskStatus.RUNNING))
    assert store.retry_task("t12") is None
    assert store.get("t12").status == TaskStatus.RUNNING


def test_retry_completed_task_returns_none(store):
    store.upsert(_make_task("t13", TaskStatus.COMPLETED))
    assert store.retry_task("t13") is None


def test_retry_nonexistent_returns_none(store):
    assert store.retry_task("nope") is None


# ── Gateway commands ────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_cancel_command_success(tmp_path, monkeypatch):
    import gateway.run as gateway_run

    db_path = tmp_path / "analytics" / "tasks.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    store = TaskStore(db_path=db_path)
    store.upsert(_make_task("t20", TaskStatus.RUNNING, user_id="u-1"))

    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    event = SimpleNamespace(source=source, get_command_args=lambda: "t20")
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)

    result = await gateway_run.GatewayRunner._handle_task_cancel_command(runner, event)
    assert "已取消" in result
    assert store.get("t20").status == TaskStatus.CANCELLED


@pytest.mark.asyncio
async def test_cancel_command_not_found(tmp_path, monkeypatch):
    import gateway.run as gateway_run

    (tmp_path / "analytics").mkdir(parents=True, exist_ok=True)
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    event = SimpleNamespace(source=source, get_command_args=lambda: "nope")
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)

    result = await gateway_run.GatewayRunner._handle_task_cancel_command(runner, event)
    assert "未找到" in result


@pytest.mark.asyncio
async def test_cancel_command_wrong_user(tmp_path, monkeypatch):
    import gateway.run as gateway_run

    db_path = tmp_path / "analytics" / "tasks.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    store = TaskStore(db_path=db_path)
    store.upsert(_make_task("t21", TaskStatus.RUNNING, user_id="alice"))

    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="bob")
    event = SimpleNamespace(source=source, get_command_args=lambda: "t21")
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)

    result = await gateway_run.GatewayRunner._handle_task_cancel_command(runner, event)
    assert "不属于" in result


@pytest.mark.asyncio
async def test_retry_command_success(tmp_path, monkeypatch):
    import gateway.run as gateway_run

    db_path = tmp_path / "analytics" / "tasks.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    store = TaskStore(db_path=db_path)
    store.upsert(_make_task("t30", TaskStatus.FAILED, user_id="u-1", completed_at=time.time()))

    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    event = SimpleNamespace(source=source, get_command_args=lambda: "t30")
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)

    result = await gateway_run.GatewayRunner._handle_task_retry_command(runner, event)
    assert "pending" in result
    assert store.get("t30").status == TaskStatus.PENDING


@pytest.mark.asyncio
async def test_retry_command_not_retryable(tmp_path, monkeypatch):
    import gateway.run as gateway_run

    db_path = tmp_path / "analytics" / "tasks.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    store = TaskStore(db_path=db_path)
    store.upsert(_make_task("t31", TaskStatus.RUNNING, user_id="u-1"))

    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    event = SimpleNamespace(source=source, get_command_args=lambda: "t31")
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)

    result = await gateway_run.GatewayRunner._handle_task_retry_command(runner, event)
    assert "只有失败或已取消" in result


@pytest.mark.asyncio
async def test_cancel_command_no_args(tmp_path, monkeypatch):
    import gateway.run as gateway_run

    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    event = SimpleNamespace(source=source, get_command_args=lambda: "")
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)

    result = await gateway_run.GatewayRunner._handle_task_cancel_command(runner, event)
    assert "用法" in result


@pytest.mark.asyncio
async def test_retry_command_no_args(tmp_path, monkeypatch):
    import gateway.run as gateway_run

    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    event = SimpleNamespace(source=source, get_command_args=lambda: "")
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)

    result = await gateway_run.GatewayRunner._handle_task_retry_command(runner, event)
    assert "用法" in result


@pytest.mark.asyncio
async def test_task_command_routes_cancel_subcommand(tmp_path, monkeypatch):
    import gateway.run as gateway_run

    called = {}

    async def _fake_cancel(self, event):
        called["task_id"] = event.get_command_args()
        return "cancelled"

    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    event = SimpleNamespace(source=source, get_command_args=lambda: "cancel t99")
    monkeypatch.setattr(gateway_run.GatewayRunner, "_handle_task_cancel_command", _fake_cancel)

    result = await gateway_run.GatewayRunner._handle_task_command(runner, event)
    assert result == "cancelled"
    assert called["task_id"] == "t99"


@pytest.mark.asyncio
async def test_task_command_routes_retry_subcommand(tmp_path, monkeypatch):
    import gateway.run as gateway_run

    called = {}

    async def _fake_retry(self, event):
        called["task_id"] = event.get_command_args()
        return "retried"

    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    event = SimpleNamespace(source=source, get_command_args=lambda: "retry t100")
    monkeypatch.setattr(gateway_run.GatewayRunner, "_handle_task_retry_command", _fake_retry)

    result = await gateway_run.GatewayRunner._handle_task_command(runner, event)
    assert result == "retried"
    assert called["task_id"] == "t100"
