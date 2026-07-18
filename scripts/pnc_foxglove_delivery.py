"""Shared Foxglove delivery URL contract for PNC host writers."""
from __future__ import annotations

import os
import re
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import quote, urlsplit


PERCEPTION_TEST_TEAM_VM_PREFIX = "/mnt/minieye/pdcl/department/perception_test_team/"
G1Q3_RCA_FORMAL_VIZ_ROOT = (
    PERCEPTION_TEST_TEAM_VM_PREFIX.rstrip("/") + "/G1Q3_RCA/cases"
)
FOXGLOVE_PATH_SAFE = "/._-()[]中文abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
DEFAULT_FOXGLOVE_RENDER_HOST = "192.168.21.217"
_SAFE_SEGMENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,191}$")


def canonical_viz_mcap_path(submission_key: Any) -> str:
    """Return the immutable Foxglove-visible path for one RCA submission."""
    key = str(submission_key or "").strip()
    if not _SAFE_SEGMENT_RE.fullmatch(key):
        return ""
    return f"{G1Q3_RCA_FORMAL_VIZ_ROOT}/{key}/{key}.viz.mcap"


def foxglove_url(viz_mcap_vm: Any) -> str:
    """Build the CVEStudio URL for one verified delivery-contract path."""
    path = str(viz_mcap_vm or "").strip()
    if not path.startswith(PERCEPTION_TEST_TEAM_VM_PREFIX) or not path.endswith(".viz.mcap"):
        return ""
    viz_path = PurePosixPath(path)
    parts = viz_path.parts
    if any(part in {".", ".."} for part in parts):
        return ""
    if viz_path.name != f"{viz_path.parent.name}.viz.mcap":
        return ""

    configured = os.getenv("PNC_FOXGLOVE_RENDER_HOST", DEFAULT_FOXGLOVE_RENDER_HOST).strip().rstrip("/")
    if not configured or any(ch.isspace() for ch in configured):
        return ""
    base = configured if configured.startswith(("http://", "https://")) else f"https://{configured}"
    parsed = urlsplit(base)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.query or parsed.fragment:
        return ""
    if parsed.path not in {"", "/"}:
        return ""
    return f"{base}/?ds=foxglove-http&ds.mcapPath={quote(path, safe=FOXGLOVE_PATH_SAFE)}"


def validate_foxglove_url(value: Any, viz_mcap_vm: Any) -> bool:
    """Require a byte-identical URL for the exact published viz path."""
    expected = foxglove_url(viz_mcap_vm)
    return bool(expected) and str(value or "").strip() == expected


def foxglove_delivery_fields(artifacts: dict[str, Any], *, causal_text: Any = "") -> dict[str, str]:
    """Return byte-identical fields for vm-task-sync and completion relay."""
    artifacts = artifacts if isinstance(artifacts, dict) else {}
    viz_mcap_vm = str(artifacts.get("viz_mcap_vm") or "").strip()
    attribution_causal_text = str(
        artifacts.get("attribution_causal_text") or causal_text or ""
    ).strip()
    return {
        "viz_mcap_vm": viz_mcap_vm,
        "foxglove_url": foxglove_url(viz_mcap_vm),
        "attribution_causal_text": attribution_causal_text,
    }
