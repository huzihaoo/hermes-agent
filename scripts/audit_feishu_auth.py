#!/usr/bin/env python3
"""Audit Feishu user authorization, role mappings, and observed unauthorized users.

Reads:
- ~/.hermes/config/user-roles.json
- ~/.hermes/pairing/feishu-approved.json
- ~/.hermes/.env FEISHU_ALLOWED_USERS
- ~/.hermes/logs/agent.log for observed unauthorized open_ids
"""

from __future__ import annotations

import json
import re
from pathlib import Path

HOME = Path.home() / ".hermes"
ROLES_PATH = HOME / "config" / "user-roles.json"
APPROVED_PATH = HOME / "pairing" / "feishu-approved.json"
ENV_PATH = HOME / ".env"
AGENT_LOG = HOME / "logs" / "agent.log"

MSG_RE = re.compile(r"Inbound group message received:.*text='([^']*)'")
UNAUTH_RE = re.compile(r"Unauthorized user: (ou_[a-z0-9]+) \(([^)]*)\) on feishu")


def _load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8") or "{}")


def _load_allowlist() -> set[str]:
    if not ENV_PATH.exists():
        return set()
    for line in ENV_PATH.read_text(errors="ignore").splitlines():
        if line.startswith("FEISHU_ALLOWED_USERS="):
            return {item.strip() for item in line.split("=", 1)[1].split(",") if item.strip()}
    return set()


def _observed_unauthorized() -> dict[str, dict]:
    observed: dict[str, dict] = {}
    if not AGENT_LOG.exists():
        return observed
    last_msg = None
    for line in AGENT_LOG.read_text(errors="ignore").splitlines():
        msg = MSG_RE.search(line)
        if msg:
            last_msg = msg.group(1)
            continue
        unauth = UNAUTH_RE.search(line)
        if not unauth:
            continue
        uid = unauth.group(1)
        entry = observed.setdefault(uid, {"count": 0, "samples": []})
        entry["count"] += 1
        if last_msg and len(entry["samples"]) < 5:
            entry["samples"].append(last_msg)
    return observed


def main() -> int:
    roles = _load_json(ROLES_PATH)
    approved = _load_json(APPROVED_PATH)
    allowed = _load_allowlist()
    mapping = roles.get("user_id_mapping", {})
    users = roles.get("users", {})
    reverse = {name: uid for uid, name in mapping.items()}
    observed = _observed_unauthorized()

    problems = 0
    print("# Feishu auth audit")
    print("\n## configured users")
    for name, role in users.items():
        if name == "default":
            continue
        uid = reverse.get(name, "")
        allow_ok = bool(uid and uid in allowed)
        approved_ok = bool(uid and uid in approved)
        status = "OK"
        if not uid:
            status = "MISSING_MAPPING"
            problems += 1
        elif not allow_ok and not approved_ok:
            status = "MAPPED_BUT_NOT_AUTHORIZED"
            problems += 1
        print(f"- {name}: role={role}, uid={uid or '-'}, allow={allow_ok}, approved={approved_ok}, status={status}")

    print("\n## observed unauthorized open_ids")
    for uid, info in sorted(observed.items(), key=lambda item: (-item[1]["count"], item[0])):
        mapped = mapping.get(uid, "")
        allow_ok = uid in allowed
        approved_ok = uid in approved
        if not mapped and not allow_ok and not approved_ok:
            problems += 1
        print(f"- {uid}: count={info['count']}, mapped={mapped or '-'}, allow={allow_ok}, approved={approved_ok}")
        for sample in info["samples"]:
            print(f"  sample: {sample}")

    print("\n## allowlist entries without name mapping")
    for uid in sorted(allowed):
        if uid not in mapping:
            problems += 1
            print(f"- {uid}")

    print(f"\nproblems={problems}")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
