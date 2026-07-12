"""Helpers for single-live-root runtime convergence.

These helpers centralize the machine-readable live manifest contract used by
launchd, wrappers, and diagnostics. Missing or malformed manifests must fail
closed to local defaults rather than guessing from whichever worktree happens
to be running.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from hermes_constants import get_live_manifest_path, load_live_manifest


REQUIRED_RUNTIME_KEYS = (
    "runtime_root",
    "runtime_python",
)


def get_live_manifest() -> dict[str, Any]:
    payload = load_live_manifest()
    return payload if isinstance(payload, dict) else {}


def validate_live_manifest(payload: dict[str, Any] | None = None) -> list[str]:
    manifest = payload if isinstance(payload, dict) else get_live_manifest()
    errors: list[str] = []
    if not manifest:
        return [f"missing manifest: {get_live_manifest_path()}"]

    for key in REQUIRED_RUNTIME_KEYS:
        value = str(manifest.get(key) or "").strip()
        if not value:
            errors.append(f"missing key: {key}")
            continue
        path = Path(value)
        if key.endswith("root") and not path.exists():
            errors.append(f"missing path for {key}: {path}")
        if key.endswith("python") and not path.exists():
            errors.append(f"missing path for {key}: {path}")
    return errors


def live_runtime_override() -> tuple[str, str, str] | None:
    manifest = get_live_manifest()
    errors = validate_live_manifest(manifest)
    if errors:
        return None
    runtime_root = Path(str(manifest.get("runtime_root"))).resolve()
    runtime_python = Path(str(manifest.get("runtime_python"))).resolve()
    runtime_venv = Path(str(manifest.get("runtime_venv") or runtime_root / '.venv')).resolve()
    return str(runtime_python), str(runtime_root), str(runtime_venv)
