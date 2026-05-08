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

release_script_path = repo_root / "scripts" / "hermes_release_smoke.py"
release_spec = importlib.util.spec_from_file_location("hermes_release_smoke", str(release_script_path))
release_smoke = importlib.util.module_from_spec(release_spec)
assert release_spec.loader is not None
release_spec.loader.exec_module(release_smoke)


def test_release_smoke_assertions_accept_healthy_host_result():
    result = {
        "config": {"agent_max_turns": 3000, "terminal_cwd": "/work"},
        "env": {"deprecated_keys_present": []},
        "health": {
            "status": "ok",
            "gateway_state": "running",
            "platforms": {
                "feishu": {"state": "connected"},
                "api_server": {"state": "connected"},
            },
        },
        "cli": {"ok": True},
        "launchd": {
            "exists": True,
            "program_arguments": ["python", "-m", "hermes_cli.main", "gateway", "run"],
            "working_directory": "/repo",
            "virtual_env": "/repo/.venv",
        },
        "logs": {"high_signal_tail": []},
        "sync_script": {"writes_deprecated_messaging_cwd": False},
        "vm": {"skipped": True},
        "thresholds": {"recommended_turn_budget": 2500},
    }

    assert release_smoke.assert_smoke(result, require_runtime=True, require_vm=False) == []


def test_release_smoke_assertions_fail_on_rebase_regression_signals():
    result = {
        "config": {"agent_max_turns": 900, "terminal_cwd": ""},
        "env": {"deprecated_keys_present": ["MESSAGING_CWD"]},
        "health": {
            "status": "ok",
            "gateway_state": "running",
            "platforms": {
                "feishu": {"state": "disconnected"},
                "api_server": {"state": "connected"},
            },
        },
        "cli": {"ok": False},
        "launchd": {
            "exists": True,
            "program_arguments": ["python", "old.py"],
            "working_directory": "/repo",
            "virtual_env": "/other/.venv",
        },
        "logs": {"high_signal_tail": ["NameError: name 'DEVELOPER_ROLE_MODELS' is not defined"]},
        "sync_script": {"writes_deprecated_messaging_cwd": True},
        "vm": {"ok": False},
        "thresholds": {"recommended_turn_budget": 2500},
    }

    errors = release_smoke.assert_smoke(result, require_runtime=True, require_vm=True)

    assert any("max_turns below" in error for error in errors)
    assert any("deprecated env keys" in error for error in errors)
    assert any("sync script still writes" in error for error in errors)
    assert any("feishu not connected" in error for error in errors)
    assert any("gateway.error.log contains high-signal" in error for error in errors)
    assert any("ssh-mini-agent doctor failed" in error for error in errors)
