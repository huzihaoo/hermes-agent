#!/usr/bin/env python3
"""PNC Feishu delivery guard.

Lightweight governance check for the delivery-side Feishu task intake chain.
It validates that business groups configured for user-facing PNC task intake are
open at both Feishu adapter policy level and gateway authorization level.
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gateway.platforms.feishu import FeishuAdapter  # noqa: E402
from gateway.pnc_group_binding import (  # noqa: E402
    G1Q3_RCA_GROUP_ID,
    G1Q3_RCA_MANUAL_GROUP_IDS,
    INTEGRATION_TOOLS_INTAKE_GROUP_ID,
    PNC_ALL_BUSINESS_TEST_GROUP_ID,
)
from hermes_constants import get_config_path  # noqa: E402

DEFAULT_CONFIG = get_config_path()


@dataclass(frozen=True)
class BusinessGroup:
    slug: str
    label: str
    chat_id: str
    require_mention: bool | None = None
    require_api_poll: bool = False
    config_only_intake: bool = False


BUSINESS_GROUPS: tuple[BusinessGroup, ...] = (
    BusinessGroup(
        slug="pnc",
        label="PNC",
        chat_id=PNC_ALL_BUSINESS_TEST_GROUP_ID,
        require_mention=None,
        require_api_poll=False,
    ),
    BusinessGroup(
        slug="g1q3-rca",
        label="G1Q3 RCA",
        chat_id=G1Q3_RCA_GROUP_ID,
        require_mention=True,
        require_api_poll=True,
    ),
    BusinessGroup(
        slug="integration-tools-intake",
        label="Integration tools intake",
        chat_id=INTEGRATION_TOOLS_INTAKE_GROUP_ID,
        require_mention=None,
        require_api_poll=True,
        config_only_intake=True,
    ),
)


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def _load_config(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        payload = yaml.safe_load(fh) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"config root must be a mapping: {path}")
    return payload


def _feishu_extra(config: dict[str, Any]) -> dict[str, Any]:
    platforms = config.get("platforms") or {}
    if not isinstance(platforms, dict):
        return {}
    feishu = platforms.get("feishu") or {}
    if not isinstance(feishu, dict):
        return {}
    extra = feishu.get("extra") or {}
    return extra if isinstance(extra, dict) else {}


def _integration_tools_intake_chat_ids(config: dict[str, Any]) -> set[str]:
    business_lines = config.get("business_lines") or {}
    if not isinstance(business_lines, dict):
        return set()
    integration_tools = business_lines.get("integration_tools") or {}
    if not isinstance(integration_tools, dict):
        return set()
    values: list[str] = []
    for key in ("intake_chat_ids", "intake_chat_id", "intake_group_ids", "intake_group_id"):
        values.extend(_as_list(integration_tools.get(key)))
    return set(values)



def _dedupe_preserve_order(values: list[str], required_first: list[str] | tuple[str, ...] = ()) -> list[str]:
    result: list[str] = []
    for item in [*required_first, *values]:
        text = str(item).strip()
        if text and text not in result:
            result.append(text)
    return result


def repair_config(config_path: Path = DEFAULT_CONFIG, *, backup: bool = True) -> dict[str, Any]:
    """Repair the minimal PNC Feishu delivery contract in config.yaml.

    This is intentionally narrow: it only touches Feishu delivery group policy
    keys needed by the user-facing PNC task intake path. It does not alter
    credentials, model routing, CLI settings, or other operational config.
    """
    config = _load_config(config_path)
    platforms = config.setdefault("platforms", {})
    if not isinstance(platforms, dict):
        platforms = {}
        config["platforms"] = platforms
    feishu = platforms.setdefault("feishu", {})
    if not isinstance(feishu, dict):
        feishu = {}
        platforms["feishu"] = feishu
    extra = feishu.setdefault("extra", {})
    if not isinstance(extra, dict):
        extra = {}
        feishu["extra"] = extra

    changed: list[str] = []
    if extra.get("default_group_policy") != "disabled":
        extra["default_group_policy"] = "disabled"
        changed.append("platforms.feishu.extra.default_group_policy")

    rules = extra.get("group_rules")
    if not isinstance(rules, dict):
        rules = {}
        extra["group_rules"] = rules
        changed.append("platforms.feishu.extra.group_rules")

    for group in BUSINESS_GROUPS:
        if group.config_only_intake:
            continue
        desired: dict[str, Any] = {"policy": "open"}
        if group.require_mention is not None:
            desired["require_mention"] = group.require_mention
        existing = rules.get(group.chat_id)
        if not isinstance(existing, dict):
            rules[group.chat_id] = desired
            changed.append(f"platforms.feishu.extra.group_rules.{group.slug}")
            continue
        for key, value in desired.items():
            if existing.get(key) != value:
                existing[key] = value
                changed.append(f"platforms.feishu.extra.group_rules.{group.slug}.{key}")

    allowed = _as_list(extra.get("group_allowed_chats"))
    desired_chats = [group.chat_id for group in BUSINESS_GROUPS]
    repaired_allowed = _dedupe_preserve_order(allowed, desired_chats)
    if repaired_allowed != allowed:
        extra["group_allowed_chats"] = repaired_allowed
        changed.append("platforms.feishu.extra.group_allowed_chats")

    api_poll = _as_list(extra.get("api_poll_chat_ids"))
    desired_api_poll = [group.chat_id for group in BUSINESS_GROUPS if group.require_api_poll]
    repaired_api_poll = _dedupe_preserve_order(api_poll, desired_api_poll)
    if repaired_api_poll != api_poll:
        extra["api_poll_chat_ids"] = repaired_api_poll
        changed.append("platforms.feishu.extra.api_poll_chat_ids")

    business_lines = config.setdefault("business_lines", {})
    if not isinstance(business_lines, dict):
        business_lines = {}
        config["business_lines"] = business_lines
        changed.append("business_lines")
    integration_tools = business_lines.setdefault("integration_tools", {})
    if not isinstance(integration_tools, dict):
        integration_tools = {}
        business_lines["integration_tools"] = integration_tools
        changed.append("business_lines.integration_tools")
    intake_values: list[str] = []
    for key in (
        "intake_chat_ids",
        "intake_chat_id",
        "intake_group_ids",
        "intake_group_id",
    ):
        intake_values.extend(_as_list(integration_tools.get(key)))
    desired_intake = [
        group.chat_id for group in BUSINESS_GROUPS if group.config_only_intake
    ]
    repaired_intake = _dedupe_preserve_order(intake_values, desired_intake)
    if _as_list(integration_tools.get("intake_chat_ids")) != repaired_intake:
        integration_tools["intake_chat_ids"] = repaired_intake
        changed.append("business_lines.integration_tools.intake_chat_ids")

    backup_path = None
    if changed:
        if backup and config_path.exists():
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            backup_path = config_path.with_name(f"{config_path.name}.bak-pnc-feishu-delivery-{stamp}")
            shutil.copy2(config_path, backup_path)
        with config_path.open("w", encoding="utf-8") as fh:
            yaml.safe_dump(config, fh, allow_unicode=True, sort_keys=False)
        try:
            config_path.chmod(0o600)
        except OSError:
            pass

    result = run_guard(config_path)
    result["repaired"] = bool(changed)
    result["changed_keys"] = changed
    result["backup_path"] = str(backup_path) if backup_path else None
    return result


def run_guard(config_path: Path = DEFAULT_CONFIG) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    checks: dict[str, Any] = {}

    try:
        config = _load_config(config_path)
    except Exception as exc:
        return {
            "ok": False,
            "config_path": str(config_path),
            "errors": [f"failed to read config: {type(exc).__name__}: {exc}"],
            "warnings": [],
            "checks": {},
        }

    extra = _feishu_extra(config)
    group_rules = extra.get("group_rules") if isinstance(extra.get("group_rules"), dict) else {}
    allowed_chats = set(_as_list(extra.get("group_allowed_chats")))
    api_poll_chats = set(_as_list(extra.get("api_poll_chat_ids")))
    integration_tools_intake_chats = _integration_tools_intake_chat_ids(config)
    default_policy = str(extra.get("default_group_policy") or "").strip().lower()
    canonical_groups = set(G1Q3_RCA_MANUAL_GROUP_IDS)
    audited_groups = {group.chat_id for group in BUSINESS_GROUPS}

    effective_settings = FeishuAdapter._load_settings(extra)
    effective_rules = effective_settings.group_rules

    checks["default_group_policy"] = default_policy
    checks["business_groups"] = []
    checks["group_allowed_chats"] = sorted(allowed_chats)
    checks["api_poll_chat_ids"] = sorted(api_poll_chats)
    checks["integration_tools_intake_chat_ids"] = sorted(
        integration_tools_intake_chats
    )
    checks["canonical_group_ids"] = sorted(canonical_groups)

    if audited_groups != canonical_groups:
        errors.append("canonical PNC group audit inventory is incomplete")
    unexpected_allowed = sorted(allowed_chats - canonical_groups)
    if unexpected_allowed:
        errors.append(
            "non-canonical groups present in group_allowed_chats: "
            + ",".join(unexpected_allowed)
        )
    unexpected_open_rules = sorted(
        chat_id
        for chat_id, rule in group_rules.items()
        if chat_id not in canonical_groups
        and isinstance(rule, dict)
        and str(rule.get("policy") or "").strip().lower() == "open"
    )
    if unexpected_open_rules:
        errors.append(
            "non-canonical groups have explicit open policy: "
            + ",".join(unexpected_open_rules)
        )

    for group in BUSINESS_GROUPS:
        explicit = group_rules.get(group.chat_id)
        effective = effective_rules.get(group.chat_id)
        explicit_policy = str((explicit or {}).get("policy") or "").strip().lower() if isinstance(explicit, dict) else ""
        effective_policy = str(getattr(effective, "policy", "") or "").strip().lower() if effective is not None else ""
        explicit_require_mention = (explicit or {}).get("require_mention") if isinstance(explicit, dict) else None
        effective_require_mention = getattr(effective, "require_mention", None) if effective is not None else None
        row = {
            "slug": group.slug,
            "label": group.label,
            "chat_id": group.chat_id,
            "explicit_policy": explicit_policy or None,
            "effective_policy": effective_policy or None,
            "in_group_allowed_chats": group.chat_id in allowed_chats,
            "in_api_poll_chat_ids": group.chat_id in api_poll_chats,
            "explicit_require_mention": explicit_require_mention,
            "effective_require_mention": effective_require_mention,
            "config_only_intake": group.config_only_intake,
            "in_integration_tools_intake": (
                group.chat_id in integration_tools_intake_chats
            ),
        }
        checks["business_groups"].append(row)

        if effective_policy in {"", "disabled", "admin_only"}:
            errors.append(
                f"{group.label} adapter policy is not open for delivery intake: "
                f"effective_policy={effective_policy or 'missing'} chat_id={group.chat_id}"
            )
        if group.chat_id not in allowed_chats:
            errors.append(f"{group.label} missing from group_allowed_chats: {group.chat_id}")
        if (
            group.config_only_intake
            and group.chat_id not in integration_tools_intake_chats
        ):
            errors.append(
                f"{group.label} missing from integration-tools intake config: "
                f"{group.chat_id}"
            )
        if group.require_api_poll and group.chat_id not in api_poll_chats:
            warnings.append(f"{group.label} missing from api_poll_chat_ids fallback: {group.chat_id}")
        if group.require_mention is not None:
            # Explicit group rule should carry this so the policy is auditable;
            # effective None means the adapter inherits global require_mention.
            if explicit_require_mention is not group.require_mention:
                warnings.append(
                    f"{group.label} should explicitly set require_mention={str(group.require_mention).lower()} "
                    f"in group_rules for auditability"
                )

    return {
        "ok": not errors,
        "config_path": str(config_path),
        "errors": errors,
        "warnings": warnings,
        "checks": checks,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate PNC Feishu delivery group governance")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--repair", action="store_true", help="Repair the minimal PNC Feishu delivery group contract")
    parser.add_argument("--no-backup", action="store_true", help="Do not create a config backup when repairing")
    args = parser.parse_args()

    result = repair_config(args.config, backup=not args.no_backup) if args.repair else run_guard(args.config)
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(f"[{'OK' if result['ok'] else 'DRIFT'}] PNC Feishu delivery guard")
        print(f"config_path: {result['config_path']}")
        for row in result.get("checks", {}).get("business_groups", []):
            print(
                f"- {row['label']}: effective_policy={row['effective_policy']} "
                f"allowed={row['in_group_allowed_chats']} api_poll={row['in_api_poll_chat_ids']}"
            )
        if result.get("repaired"):
            print(f"repaired: {len(result.get('changed_keys', []))} keys")
            if result.get("backup_path"):
                print(f"backup_path: {result['backup_path']}")
        if result["warnings"]:
            print("warnings:")
            for warn in result["warnings"]:
                print(f"- {warn}")
        if result["errors"]:
            print("errors:")
            for err in result["errors"]:
                print(f"- {err}")
    return 0 if result["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
