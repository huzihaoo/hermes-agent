"""Portable G1Q3 RCA issue preread captures.

Capture is an explicit diagnostic action only: normal issue preread is
side-effect free.  When opted in, the JSON schema is pure data so VM-side yj
tools can rebuild execution requests without importing gateway modules.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from gateway.pnc_rca_schema import issue_context_from_compact_text, to_dict

CAPTURE_SCHEMA_VERSION = "g1q3_rca_issue_capture_v1"
CAPTURE_ENABLED_ENV = "HERMES_G1Q3_ISSUE_CAPTURE_ENABLED"
CAPTURE_ROOT_ENV = "HERMES_G1Q3_ISSUE_CAPTURE_ROOT"
CAPTURE_ALLOWED_ROOT = Path("/mnt/tmp")

_SENSITIVE_KEYS = {"raw", "raw_payload", "raw_feishu_payload", "full_payload", "secret", "token", "open_id", "user_key"}


def _sanitize_capture_value(value: Any) -> Any:
    if is_dataclass(value):
        return _sanitize_capture_value(asdict(value))
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key)
            if key_text.lower() in _SENSITIVE_KEYS:
                continue
            out[key_text] = _sanitize_capture_value(item)
        return out
    if isinstance(value, list):
        return [_sanitize_capture_value(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _case_dir_name(work_item_id: str, issue_context: dict[str, Any]) -> str:
    case_id = str(issue_context.get("case_id") or "").strip()
    if case_id:
        return re.sub(r"[^A-Za-z0-9_.-]+", "_", case_id).strip("_") or case_id
    item_id = str(work_item_id or issue_context.get("work_item_id") or "unknown").strip() or "unknown"
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", item_id).strip("_") or "unknown"


def capture_root() -> Path:
    raw = os.getenv(CAPTURE_ROOT_ENV, "").strip()
    if not raw:
        raise ValueError(f"{CAPTURE_ROOT_ENV} is required when capture is enabled")
    root = Path(os.path.normpath(str(Path(raw).expanduser())))
    if not root.is_absolute():
        raise ValueError(f"{CAPTURE_ROOT_ENV} must be absolute")
    try:
        relative = root.relative_to(CAPTURE_ALLOWED_ROOT)
    except ValueError as exc:
        raise ValueError(f"{CAPTURE_ROOT_ENV} must be under /mnt/tmp/<task>") from exc
    if not relative.parts:
        raise ValueError(f"{CAPTURE_ROOT_ENV} must name a task directory")
    return root


def build_issue_capture(
    *,
    project_key: str,
    work_item_id: str,
    read_source: str,
    context_text: str,
    read_status: str,
    blocker: dict[str, Any] | None = None,
    errors: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    ctx = issue_context_from_compact_text(
        project_key=str(project_key or ""),
        work_item_id=str(work_item_id or ""),
        compact_text=str(context_text or ""),
        source_quality="partial" if context_text else "unavailable",
        blockers=[blocker] if isinstance(blocker, dict) else None,
    )
    issue_context = _sanitize_capture_value(to_dict(ctx))
    payload = {
        "schema_version": CAPTURE_SCHEMA_VERSION,
        "work_item_id": str(work_item_id or ""),
        "project_key": str(project_key or ""),
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "read_source": str(read_source or ""),
        "issue_context_sanitized": issue_context,
        "read_status": str(read_status or ""),
        "blocker": _sanitize_capture_value(blocker) if blocker else None,
        "errors": _sanitize_capture_value(errors or []),
    }
    return _sanitize_capture_value(payload)


def capture_path_for(payload: dict[str, Any]) -> Path:
    issue_context = payload.get("issue_context_sanitized") if isinstance(payload.get("issue_context_sanitized"), dict) else {}
    case_dir = _case_dir_name(str(payload.get("work_item_id") or ""), issue_context)
    return capture_root() / case_dir / "issue_capture.json"


def _write_capture_via_ssh_mini(path: Path, data: str) -> None:
    agent = Path.home() / ".local" / "bin" / "ssh-mini-agent"
    if not agent.exists():
        raise FileNotFoundError(str(agent))
    completed = subprocess.run(
        [str(agent), "edit_file", str(path)],
        input=data,
        text=True,
        capture_output=True,
        timeout=20,
        check=False,
    )
    if completed.returncode != 0:
        raise OSError((completed.stderr or completed.stdout or "ssh-mini-agent edit_file failed")[:500])


def write_issue_capture(payload: dict[str, Any]) -> Path:
    path = capture_path_for(payload)
    data = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        # The explicit diagnostic target is replaceable by design.
        if path.exists():
            try:
                path.unlink()
            except OSError:
                pass
        path.write_text(data, encoding="utf-8")
    except OSError:
        # Host macOS does not mount the VM /mnt path; write the same pure JSON to
        # the explicit VM /mnt/tmp task directory through the governed bridge.
        _write_capture_via_ssh_mini(path, data)
    return path


def maybe_capture_issue_context(**kwargs: Any) -> str:
    enabled = os.getenv(CAPTURE_ENABLED_ENV, "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if not enabled:
        return ""
    if os.getenv("HERMES_G1Q3_DISABLE_ISSUE_CAPTURE", "").strip().lower() in {"1", "true", "yes", "on"}:
        return ""
    payload = build_issue_capture(**kwargs)
    return str(write_issue_capture(payload))
