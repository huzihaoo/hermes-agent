#!/usr/bin/env python3
"""Host-side Feishu admission smoke harness.

This is intentionally non-public: it does not call Feishu APIs and does not send
messages. It exercises the same adapter admission dispatch path with synthetic
Feishu group/topic events, then records health/metrics evidence from the running
local gateway.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import tempfile
import urllib.request
from pathlib import Path
from types import SimpleNamespace
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gateway.admission.controller import AdmissionController  # noqa: E402
from gateway.config import PlatformConfig  # noqa: E402
from gateway.platforms.base import MessageEvent, MessageType  # noqa: E402
from gateway.platforms.feishu import FeishuAdapter  # noqa: E402
from gateway.session import SessionSource  # noqa: E402


SMOKE_MESSAGES = {
    "fast": "你好",
    "standard": "请整理一下这个本机 admission smoke 的状态，不要调用外部接口。",
    "heavy": "请提交一个 VM heavy smoke 任务，只验证 admission 路由，不执行真实重型转换。",
}


def _fetch_json(url: str, timeout: float) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _fetch_text(url: str, timeout: float) -> str:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return response.read().decode("utf-8")


def _metrics_summary(metrics: str) -> dict[str, Any]:
    lines = [line.strip() for line in metrics.splitlines() if line.strip() and not line.startswith("#")]
    queue_depths = [line for line in lines if line.startswith("admission_queue_depth")]
    failed_lines = [line for line in lines if line.startswith("admission_total_failed")]
    admitted_lines = [line for line in lines if line.startswith("admission_total_admitted")]
    return {
        "admission_total_failed_zero": any(line.endswith(" 0") for line in failed_lines),
        "admission_total_failed": failed_lines,
        "admission_total_admitted": admitted_lines,
        "queue_depth_lines": queue_depths,
        "queue_depth_zero_count": sum(1 for line in queue_depths if line.endswith(" 0")),
    }


async def _run_dispatch_smoke() -> dict[str, Any]:
    tmp = tempfile.TemporaryDirectory(prefix="feishu-admission-smoke-")
    tmp_path = Path(tmp.name)
    controller = AdmissionController(db_path=tmp_path / "queue.db", audit_dir=tmp_path / "audit")

    adapter = object.__new__(FeishuAdapter)
    adapter.platform = "feishu"
    adapter.config = PlatformConfig(enabled=True)
    adapter._admission_enabled = True
    adapter._admission_controller = controller
    sent: list[dict[str, Any]] = []

    async def fake_send(chat_id: str, content: str, metadata: dict[str, Any] | None = None, **kwargs: Any):
        sent.append({"chat_id": chat_id, "content": content, "metadata": metadata, "kwargs": kwargs})
        return SimpleNamespace(success=True)

    adapter.send = fake_send

    events = []
    for lane_name, message in SMOKE_MESSAGES.items():
        event = MessageEvent(
            source=SessionSource(
                platform="feishu",
                user_id=f"ou_smoke_{lane_name}",
                user_name="admission-smoke",
                chat_id="oc_smoke_group",
                chat_type="group",
                thread_id="topic:om_smoke_thread",
            ),
            text=message,
            message_type=MessageType.TEXT,
            message_id=f"om_smoke_{lane_name}",
        )
        await adapter._dispatch_inbound_event(event)
        events.append(event)

    pending = controller.queue.list_pending()
    items = [
        {
            "message_id": item.request_message_id,
            "lane": item.lane,
            "domain": item.domain,
            "domain_id": item.domain_id,
            "chat_id": item.chat_id,
            "chat_type": item.chat_type,
            "thread_id": item.thread_id,
            "platform": item.platform,
        }
        for item in pending
    ]
    reconstructed = []

    async def capture_handle(event: MessageEvent):
        reconstructed.append(
            {
                "message_id": event.message_id,
                "chat_id": event.source.chat_id,
                "chat_type": event.source.chat_type,
                "thread_id": event.source.thread_id,
                "platform": event.source.platform,
                "text": event.text,
            }
        )

    adapter._handle_message_with_guards = capture_handle
    for item in list(pending):
        await adapter._process_queue_item(item)

    tmp.cleanup()
    return {
        "items": items,
        "public_feedback_sent": sent,
        "reconstructed": reconstructed,
        "expected": {
            "lanes": ["fast", "standard", "heavy"],
            "fast_standard_public_feedback": 0,
            "heavy_public_feedback": 1,
            "thread_id": "topic:om_smoke_thread",
            "chat_type": "group",
        },
    }


def _assert_smoke(result: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    items = result["dispatch"]["items"]
    lanes = sorted(item["lane"] for item in items)
    if lanes != ["fast", "heavy", "standard"]:
        errors.append(f"unexpected lanes: {lanes}")
    for item in items:
        if item["chat_id"] != "oc_smoke_group":
            errors.append(f"chat_id lost for {item}")
        if item["chat_type"] != "group":
            errors.append(f"chat_type lost for {item}")
        if item["thread_id"] != "topic:om_smoke_thread":
            errors.append(f"thread_id lost for {item}")
        if item["platform"] != "feishu":
            errors.append(f"platform lost for {item}")
    sent = result["dispatch"]["public_feedback_sent"]
    if len(sent) != 1:
        errors.append(f"expected exactly one heavy public feedback, got {len(sent)}")
    elif sent[0].get("metadata") != {"thread_id": "topic:om_smoke_thread"}:
        errors.append(f"heavy feedback metadata wrong: {sent[0]}")
    reconstructed = result["dispatch"]["reconstructed"]
    if len(reconstructed) != 3:
        errors.append(f"expected 3 reconstructed events, got {len(reconstructed)}")
    for event in reconstructed:
        if event["thread_id"] != "topic:om_smoke_thread" or event["chat_type"] != "group":
            errors.append(f"reconstructed route wrong: {event}")
    metrics = result.get("metrics", {})
    if metrics and metrics.get("queue_depth_zero_count") != 9:
        errors.append(f"runtime queue depths not all zero: {metrics.get('queue_depth_lines')}")
    if metrics and not metrics.get("admission_total_failed_zero"):
        errors.append(f"runtime admission_total_failed not zero: {metrics.get('admission_total_failed')}")
    health = result.get("health", {})
    if health and health.get("status") != "ok":
        errors.append(f"health status not ok: {health}")
    if health and health.get("gateway_state") != "running":
        errors.append(f"gateway not running: {health}")
    platforms = health.get("platforms", {}) if health else {}
    for name in ("feishu", "api_server"):
        if platforms and platforms.get(name, {}).get("state") != "connected":
            errors.append(f"{name} not connected: {platforms.get(name)}")
    return errors


async def _amain(args: argparse.Namespace) -> int:
    result: dict[str, Any] = {"dispatch": await _run_dispatch_smoke()}
    if not args.no_runtime:
        result["health"] = _fetch_json(args.health_url, args.timeout)
        result["metrics"] = _metrics_summary(_fetch_text(args.metrics_url, args.timeout))
    errors = _assert_smoke(result)
    result["ok"] = not errors
    result["errors"] = errors
    print(json.dumps(result, ensure_ascii=False, indent=2 if args.pretty else None, sort_keys=True))
    return 0 if not errors else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Non-public Feishu admission smoke harness")
    parser.add_argument("--health-url", default="http://127.0.0.1:18789/health/detailed")
    parser.add_argument("--metrics-url", default="http://127.0.0.1:18790/metrics")
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--no-runtime", action="store_true", help="Skip live health/metrics probes")
    parser.add_argument("--pretty", action="store_true")
    return asyncio.run(_amain(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
