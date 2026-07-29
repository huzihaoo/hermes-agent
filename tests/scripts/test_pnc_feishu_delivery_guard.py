from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import yaml

repo_root = Path(__file__).parent.parent.parent
script_path = repo_root / "scripts" / "pnc_feishu_delivery_guard.py"
spec = importlib.util.spec_from_file_location("pnc_feishu_delivery_guard", str(script_path))
guard = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = guard
assert spec.loader is not None
spec.loader.exec_module(guard)


G1Q3 = "oc_6cfc782212009ff4cd815349909dd423"
PNC = "oc_16614f4ba25b8c88b69c0b8e9ebc2fb5"
INTEGRATION_TOOLS = "oc_35039b74ffb63ab8100343dc32218c57"


def _write_config(tmp_path: Path, extra: dict) -> Path:
    path = tmp_path / "config.yaml"
    payload = {
        "platforms": {"feishu": {"extra": extra}},
        "business_lines": {
            "integration_tools": {
                "intake_chat_ids": [INTEGRATION_TOOLS, PNC],
            }
        },
    }
    path.write_text(
        yaml.safe_dump(payload, sort_keys=False),
        encoding="utf-8",
    )
    return path


def test_guard_accepts_current_pnc_feishu_delivery_contract(tmp_path):
    path = _write_config(tmp_path, {
        "default_group_policy": "disabled",
        "group_rules": {
            PNC: {"policy": "open"},
            G1Q3: {"policy": "open", "require_mention": True},
        },
        "group_allowed_chats": [PNC, G1Q3, INTEGRATION_TOOLS],
        "api_poll_chat_ids": [G1Q3, INTEGRATION_TOOLS],
    })

    result = guard.run_guard(path)

    assert result["ok"] is True
    assert result["errors"] == []


def test_guard_fails_when_group_allowed_chat_is_missing(tmp_path):
    path = _write_config(tmp_path, {
        "default_group_policy": "disabled",
        "group_rules": {
            PNC: {"policy": "open"},
            G1Q3: {"policy": "open", "require_mention": True},
        },
        "group_allowed_chats": [PNC, INTEGRATION_TOOLS],
        "api_poll_chat_ids": [G1Q3, INTEGRATION_TOOLS],
    })

    result = guard.run_guard(path)

    assert result["ok"] is False
    assert any("G1Q3 RCA missing from group_allowed_chats" in item for item in result["errors"])


def test_guard_accepts_group_allowed_chats_adapter_fallback(tmp_path):
    path = _write_config(tmp_path, {
        "default_group_policy": "disabled",
        "group_rules": {PNC: {"policy": "open"}},
        "group_allowed_chats": [PNC, G1Q3, INTEGRATION_TOOLS],
        "api_poll_chat_ids": [G1Q3, INTEGRATION_TOOLS],
    })

    result = guard.run_guard(path)
    g1q3 = [row for row in result["checks"]["business_groups"] if row["chat_id"] == G1Q3][0]

    assert result["ok"] is True
    assert g1q3["effective_policy"] == "open"
    assert any("require_mention=true" in item for item in result["warnings"])


def test_guard_audits_config_only_integration_tools_group(tmp_path):
    path = _write_config(tmp_path, {
        "default_group_policy": "disabled",
        "group_rules": {
            PNC: {"policy": "open"},
            G1Q3: {"policy": "open", "require_mention": True},
        },
        "group_allowed_chats": [PNC, G1Q3, INTEGRATION_TOOLS],
        "api_poll_chat_ids": [G1Q3, INTEGRATION_TOOLS],
    })

    result = guard.run_guard(path)
    [row] = [
        item
        for item in result["checks"]["business_groups"]
        if item["chat_id"] == INTEGRATION_TOOLS
    ]

    assert result["ok"] is True
    assert row["config_only_intake"] is True
    assert row["in_integration_tools_intake"] is True
    assert row["effective_policy"] == "open"


def test_guard_rejects_non_canonical_open_group(tmp_path):
    outside = "oc_outside_authorized_group_001"
    path = _write_config(tmp_path, {
        "default_group_policy": "disabled",
        "group_rules": {
            PNC: {"policy": "open"},
            G1Q3: {"policy": "open", "require_mention": True},
            outside: {"policy": "open"},
        },
        "group_allowed_chats": [PNC, G1Q3, INTEGRATION_TOOLS, outside],
        "api_poll_chat_ids": [G1Q3, INTEGRATION_TOOLS],
    })

    result = guard.run_guard(path)

    assert result["ok"] is False
    assert any("non-canonical groups" in item for item in result["errors"])


def test_guard_fails_when_config_only_intake_membership_is_missing(tmp_path):
    path = _write_config(tmp_path, {
        "default_group_policy": "disabled",
        "group_rules": {
            PNC: {"policy": "open"},
            G1Q3: {"policy": "open", "require_mention": True},
        },
        "group_allowed_chats": [PNC, G1Q3, INTEGRATION_TOOLS],
        "api_poll_chat_ids": [G1Q3, INTEGRATION_TOOLS],
    })
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    payload["business_lines"]["integration_tools"]["intake_chat_ids"] = [PNC]
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    result = guard.run_guard(path)

    assert result["ok"] is False
    assert any(
        "missing from integration-tools intake config" in item
        for item in result["errors"]
    )
