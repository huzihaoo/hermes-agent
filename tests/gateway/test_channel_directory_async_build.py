"""Regression tests for channel directory async build integration."""

import asyncio

import pytest

from gateway.channel_directory import build_channel_directory
from gateway.config import GatewayConfig, Platform, PlatformConfig


@pytest.mark.asyncio
async def test_build_channel_directory_must_be_awaited_for_directory_dict():
    """Calling code should await the async directory builder before reading it."""
    directory = await build_channel_directory({})

    assert isinstance(directory, dict)
    assert directory.get("platforms") is not None


@pytest.mark.asyncio
async def test_gateway_start_awaits_initial_channel_directory_build(monkeypatch, tmp_path):
    """Gateway.start should await build_channel_directory, not treat the coroutine as a dict."""
    from gateway.run import GatewayRunner

    config = GatewayConfig(
        platforms={Platform.API_SERVER: PlatformConfig(enabled=True, token="test-key")},
        sessions_dir=tmp_path / "sessions",
    )
    gateway = GatewayRunner(config=config)

    class FakeAdapter:
        has_fatal_error = False
        fatal_error_retryable = False
        fatal_error_code = None
        fatal_error_message = None

        def set_message_handler(self, *_args):
            pass

        def set_fatal_error_handler(self, *_args):
            pass

        def set_session_store(self, *_args):
            pass

        def set_busy_session_handler(self, *_args):
            pass

        async def connect(self):
            return True

    awaited = False

    async def fake_build_channel_directory(adapters):
        nonlocal awaited
        awaited = True
        assert Platform.API_SERVER in adapters
        return {"platforms": {"api_server": [{"id": "health", "name": "API"}]}}

    monkeypatch.setattr(gateway.hooks, "discover_and_load", lambda: None)
    monkeypatch.setattr(gateway.hooks, "emit", lambda *_args, **_kwargs: asyncio.sleep(0))
    monkeypatch.setattr(gateway, "_create_adapter", lambda *_args, **_kwargs: FakeAdapter())
    monkeypatch.setattr(gateway, "_update_runtime_status", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(gateway, "_update_platform_runtime_status", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(gateway, "_sync_voice_mode_state_to_adapter", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(gateway, "_send_update_notification", lambda: asyncio.sleep(0, result=False))
    monkeypatch.setattr(gateway, "_schedule_update_notification_watch", lambda: None)
    monkeypatch.setattr(gateway, "_send_restart_notification", lambda: asyncio.sleep(0))
    monkeypatch.setattr(gateway, "_suspend_stuck_loop_sessions", lambda: 0)
    monkeypatch.setattr("gateway.channel_directory.build_channel_directory", fake_build_channel_directory)

    created_coroutines = []

    def fake_create_task(coro):
        created_coroutines.append(coro)
        if hasattr(coro, "close"):
            coro.close()
        return None

    monkeypatch.setattr(asyncio, "create_task", fake_create_task)

    assert await gateway.start() is True
    assert awaited is True


def test_cron_ticker_refresh_schedules_async_channel_directory_on_gateway_loop(monkeypatch):
    """The cron ticker thread should schedule the async directory refresh on the gateway loop."""
    from gateway import run as gateway_run

    class StopAfterFirstTick:
        def __init__(self):
            self.calls = 0

        def is_set(self):
            return self.calls > 0

        def wait(self, timeout=None):
            self.calls += 1

    class FakeLoop:
        def is_running(self):
            return True

    class FakeFuture:
        def result(self, timeout=None):
            return {"platforms": {}}

    scheduled = []

    async def fake_build_channel_directory(adapters):
        return {"platforms": {"feishu": []}}

    def fake_run_coroutine_threadsafe(coro, loop):
        scheduled.append((coro, loop))
        coro.close()
        return FakeFuture()

    monkeypatch.setattr(gateway_run, "build_channel_directory", fake_build_channel_directory, raising=False)
    monkeypatch.setattr("cron.scheduler.tick", lambda **_kwargs: None)
    monkeypatch.setattr(gateway_run.asyncio, "run_coroutine_threadsafe", fake_run_coroutine_threadsafe)
    monkeypatch.setattr(gateway_run, "cleanup_image_cache", lambda max_age_hours: 0, raising=False)
    monkeypatch.setattr(gateway_run, "cleanup_document_cache", lambda max_age_hours: 0, raising=False)

    gateway_run._start_cron_ticker(
        StopAfterFirstTick(),
        adapters={Platform.FEISHU: object()},
        loop=FakeLoop(),
        interval=0,
        channel_dir_every=1,
        image_cache_every=100,
    )

    assert len(scheduled) == 1
    assert scheduled[0][1].is_running()
