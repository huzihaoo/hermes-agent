"""Minimal permission-policy shim for local VM task submission.

The VM task tool only needs identity-to-role lookup so it can fail closed before
submitting work to the shared-state VM worker.  Keep this file intentionally
small in upstream-compatible branches; broader PNC repo ACL governance belongs
to the local overlay, not Hermes Agent core.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Literal, Optional

Role = Literal["owner", "admin", "senior", "member"]

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
        try:
            _config = json.loads(_CONFIG_PATH.read_text(encoding="utf-8"))
        except FileNotFoundError:
            _config = {"users": {"default": "member"}, "user_id_mapping": {}}
    return _config


def get_user_role(display_name: str) -> Role:
    cfg = _load_config()
    normalized_name = _normalize_user_name(display_name)
    role = cfg.get("users", {}).get(normalized_name, cfg.get("users", {}).get("default", "member"))
    return role if role in {"owner", "admin", "senior", "member"} else "member"


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
    cfg = _load_config()
    display_name = cfg.get("user_id_mapping", {}).get(str(user_id or "").strip())
    if display_name:
        return get_user_role(display_name)
    return "member"
