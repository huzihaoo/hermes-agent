"""Permission policy — identity-based command classification and decision."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Literal

Decision = Literal["ALLOW", "CONFIRM", "APPROVE", "DENY"]
Role = Literal["owner", "admin", "senior", "member"]
OpType = Literal["read", "write", "delete_small", "dangerous"]

_CONFIG_PATH = Path.home() / ".hermes" / "config" / "user-roles.json"
_config: dict | None = None


def _load_config() -> dict:
    global _config
    if _config is None:
        _config = json.loads(_CONFIG_PATH.read_text())
    return _config


def get_user_role(display_name: str) -> Role:
    cfg = _load_config()
    return cfg["users"].get(display_name, cfg["users"].get("default", "member"))


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
    return cfg["permission_matrix"][role][op_type]  # type: ignore[return-value]
