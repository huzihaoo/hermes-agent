"""Permission policy — identity-based command classification and decision."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Literal, Optional

Decision = Literal["ALLOW", "CONFIRM", "APPROVE", "DENY"]
Role = Literal["owner", "admin", "senior", "member"]
OpType = Literal["read", "write", "delete_small", "dangerous"]

_CONFIG_PATH = Path.home() / ".hermes" / "config" / "user-roles.json"
_config: dict | None = None


def _normalize_user_name(display_name: str) -> str:
    return str(display_name or "").strip()


def _save_config(cfg: dict) -> None:
    global _config
    _CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    _CONFIG_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _config = cfg


def _load_config() -> dict:
    global _config
    if _config is None:
        _config = json.loads(_CONFIG_PATH.read_text())
    return _config


def get_user_role(display_name: str) -> Role:
    cfg = _load_config()
    normalized_name = _normalize_user_name(display_name)
    return cfg["users"].get(normalized_name, cfg["users"].get("default", "member"))


def set_user_role(display_name: str, role: Role) -> Role:
    cfg = _load_config()
    normalized_name = _normalize_user_name(display_name)
    if not normalized_name:
        raise ValueError("display_name is required")
    if role not in cfg.get("permission_matrix", {}):
        raise ValueError(f"unknown role: {role}")
    users = cfg.setdefault("users", {})
    users[normalized_name] = role
    _save_config(cfg)
    return role


def map_user_id(display_name: str, user_id: str) -> None:
    cfg = _load_config()
    normalized_name = _normalize_user_name(display_name)
    normalized_user_id = str(user_id or "").strip()
    if not normalized_name:
        raise ValueError("display_name is required")
    if not normalized_user_id:
        raise ValueError("user_id is required")
    mapping = cfg.setdefault("user_id_mapping", {})
    mapping[normalized_user_id] = normalized_name
    _save_config(cfg)


def find_user_id_by_name(display_name: str) -> Optional[str]:
    cfg = _load_config()
    normalized_name = _normalize_user_name(display_name)
    if not normalized_name:
        return None
    for mapped_user_id, mapped_name in cfg.get("user_id_mapping", {}).items():
        if _normalize_user_name(mapped_name) == normalized_name:
            return str(mapped_user_id).strip() or None
    return None


def get_user_role_by_id(user_id: str) -> Role:
    """Get user role by user_id (e.g., feishu open_id)."""
    cfg = _load_config()
    mapping = cfg.get("user_id_mapping", {})
    display_name = mapping.get(user_id)
    if display_name:
        return get_user_role(display_name)
    return cfg["users"].get("default", "member")


def classify_command(command: str) -> OpType:
    cfg = _load_config()
    cmd = command.strip()

    # Check critical paths first — always dangerous
    for pattern in cfg.get("critical_paths", []):
        expanded = str(Path(pattern.replace("~", str(Path.home()))).parent)
        if expanded in cmd:
            return "dangerous"

    # Match against pattern lists
    for op_type in ("dangerous", "delete_small", "write", "read"):
        for pattern in cfg["command_patterns"].get(op_type, []):
            if pattern in cmd:
                return op_type  # type: ignore[return-value]

    # Large rm -rf heuristic
    if re.search(r"rm\s+-rf?\s+", cmd):
        if re.search(r"[\*\?]", cmd) or len(cmd.split()[-1]) < 3:
            return "dangerous"
        return "delete_small"

    return "write"  # default unknown to write (safer than read)


def get_decision(display_name: str, command: str) -> Decision:
    role = get_user_role(display_name)
    op_type = classify_command(command)
    cfg = _load_config()
    return cfg["permission_matrix"][role][op_type]  # type: ignore[return-value]


def get_decision_by_id(user_id: str, command: str) -> Decision:
    """Get permission decision by user_id."""
    role = get_user_role_by_id(user_id)
    op_type = classify_command(command)
    cfg = _load_config()
    decision = cfg["permission_matrix"][role][op_type]
    _log_decision(user_id, role, command, op_type, decision)
    return decision  # type: ignore[return-value]


def _log_decision(user_id: str, role: str, command: str, op_type: str, decision: str) -> None:
    """Append approval decision to audit log."""
    import json as _json
    from datetime import datetime as _dt
    from pathlib import Path as _Path

    audit_dir = _Path.home() / ".hermes" / "audit" / "approvals"
    audit_dir.mkdir(parents=True, exist_ok=True)
    log_file = audit_dir / f"{_dt.now().strftime('%Y-%m-%d')}.jsonl"

    # Resolve display name
    cfg = _load_config()
    display_name = cfg.get("user_id_mapping", {}).get(user_id, user_id)

    event = {
        "timestamp": _dt.now().isoformat(),
        "user_id": user_id,
        "user_name": display_name,
        "role": role,
        "command": command[:200],
        "op_type": op_type,
        "decision": decision,
    }
    try:
        with log_file.open("a", encoding="utf-8") as f:
            f.write(_json.dumps(event, ensure_ascii=False) + "\n")
    except Exception:
        pass  # Don't let audit logging break the permission check
