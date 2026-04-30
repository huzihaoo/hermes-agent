from __future__ import annotations

import importlib.util
from pathlib import Path


repo_root = Path(__file__).parent.parent
script_path = repo_root / "scripts" / "feishu_admission_smoke.py"
spec = importlib.util.spec_from_file_location("feishu_admission_smoke", str(script_path))
smoke = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(smoke)


def test_smoke_assertions_accept_expected_non_public_dispatch_result():
    result = {
        "dispatch": {
            "items": [
                {
                    "lane": "fast",
                    "chat_id": "oc_smoke_group",
                    "chat_type": "group",
                    "thread_id": "topic:om_smoke_thread",
                    "platform": "feishu",
                },
                {
                    "lane": "standard",
                    "chat_id": "oc_smoke_group",
                    "chat_type": "group",
                    "thread_id": "topic:om_smoke_thread",
                    "platform": "feishu",
                },
                {
                    "lane": "heavy",
                    "chat_id": "oc_smoke_group",
                    "chat_type": "group",
                    "thread_id": "topic:om_smoke_thread",
                    "platform": "feishu",
                },
            ],
            "public_feedback_sent": [
                {
                    "chat_id": "oc_smoke_group",
                    "content": "heavy notice",
                    "metadata": {"thread_id": "topic:om_smoke_thread"},
                }
            ],
            "reconstructed": [
                {"chat_type": "group", "thread_id": "topic:om_smoke_thread"},
                {"chat_type": "group", "thread_id": "topic:om_smoke_thread"},
                {"chat_type": "group", "thread_id": "topic:om_smoke_thread"},
            ],
        },
        "health": {
            "status": "ok",
            "gateway_state": "running",
            "platforms": {
                "feishu": {"state": "connected"},
                "api_server": {"state": "connected"},
            },
        },
        "metrics": {
            "queue_depth_zero_count": 9,
            "admission_total_failed_zero": True,
        },
    }

    assert smoke._assert_smoke(result) == []


def test_smoke_assertions_fail_when_fast_feedback_is_public():
    result = {
        "dispatch": {
            "items": [
                {
                    "lane": "fast",
                    "chat_id": "oc_smoke_group",
                    "chat_type": "group",
                    "thread_id": "topic:om_smoke_thread",
                    "platform": "feishu",
                },
                {
                    "lane": "standard",
                    "chat_id": "oc_smoke_group",
                    "chat_type": "group",
                    "thread_id": "topic:om_smoke_thread",
                    "platform": "feishu",
                },
                {
                    "lane": "heavy",
                    "chat_id": "oc_smoke_group",
                    "chat_type": "group",
                    "thread_id": "topic:om_smoke_thread",
                    "platform": "feishu",
                },
            ],
            "public_feedback_sent": [
                {"metadata": {"thread_id": "topic:om_smoke_thread"}},
                {"metadata": {"thread_id": "topic:om_smoke_thread"}},
            ],
            "reconstructed": [
                {"chat_type": "group", "thread_id": "topic:om_smoke_thread"},
                {"chat_type": "group", "thread_id": "topic:om_smoke_thread"},
                {"chat_type": "group", "thread_id": "topic:om_smoke_thread"},
            ],
        }
    }

    errors = smoke._assert_smoke(result)

    assert any("exactly one heavy public feedback" in error for error in errors)
