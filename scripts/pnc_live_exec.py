#!/usr/bin/env python3
"""Fail-closed launcher for PNC services bound by LIVE_MANIFEST.json."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import subprocess
import sys
from pathlib import Path
from typing import Any


MAX_MANIFEST_BYTES = 4 * 1024 * 1024
STABLE_TARGET_REGISTRY_RELATIVE = "gateway/assets/pnc_stable_target_registry_v1.json"
STABLE_TARGET_REGISTRY_SCHEMA_VERSION = "pnc_stable_target_registry_v1"
SERVICE_TARGETS = {
    "ai.hermes.gateway": (
        "runtime_script",
        "hermes_cli/main.py",
    ),
    "local.pnc.completion-notice-relay": (
        "runtime_script",
        "scripts/pnc_completion_notice_relay.py",
    ),
    "local.pnc.context-budget-check": (
        "runtime_script",
        "scripts/hermes_context_budget_check.py",
    ),
    "local.pnc.feishu-credential-health": (
        "runtime_script",
        "scripts/feishu_credential_cron.py",
    ),
    "local.pnc.feishu-delivery-repair": (
        "runtime_script",
        "scripts/pnc_feishu_delivery_guard.py",
    ),
    "local.pnc.meegle-auth-watchdog": (
        "runtime_script",
        "scripts/pnc_meegle_auth_watchdog.py",
    ),
    "local.pnc.rca-delivery-collector": (
        "runtime_script",
        "scripts/pnc_rca_delivery_collector.py",
    ),
    "local.pnc.rca-delivery-dispatcher": (
        "runtime_script",
        "scripts/pnc_rca_delivery_dispatcher.py",
    ),
    "local.pnc.rca-kafka-consumer": (
        "runtime_script",
        "scripts/pnc_rca_kafka_consumer.py",
    ),
    "local.pnc.rca-outbox-dispatcher": (
        "runtime_script",
        "scripts/pnc_rca_outbox_dispatcher.py",
    ),
    "local.pnc.task-dashboard.viewer": (
        "runtime_file",
        "restricted_task_dashboard_proxy.py",
    ),
    "local.pnc.vm-task-sync": (
        "runtime_script",
        "scripts/pnc_vm_task_sync.py",
    ),
    "local.pnc.release-fingerprint-check": (
        "governance_tool",
        "hermes_release_fingerprint_check.py",
    ),
    "local.pnc.live-drift-guard": (
        "runtime_script",
        "scripts/hermes_live_drift_guard.py",
    ),
    "local.pnc.feishu-ops-alert": (
        "governance_tool",
        "feishu_ops_alert.py",
    ),
    "local.pnc.governance-check": (
        "governance_tool",
        "hermes_governance_check.py",
    ),
    "local.pnc.provider-failure-audit": (
        "governance_tool",
        "hermes_provider_failure_audit.py",
    ),
    "local.pnc.safe-worktree-remove": (
        "governance_tool",
        "hermes_safe_worktree_remove.py",
    ),
    "local.pnc.worktree-hygiene": (
        "governance_tool",
        "hermes_worktree_hygiene.py",
    ),
    "local.pnc.hermes-cli": (
        "runtime_script",
        "hermes_cli/main.py",
    ),
}

PNC_PYTHON_LAUNCHD_LABELS = (
    "ai.hermes.gateway",
    "local.pnc.completion-notice-relay",
    "local.pnc.feishu-credential-health",
    "local.pnc.feishu-delivery-repair",
    "local.pnc.meegle-auth-watchdog",
    "local.pnc.rca-delivery-collector",
    "local.pnc.rca-delivery-dispatcher",
    "local.pnc.rca-kafka-consumer",
    "local.pnc.rca-outbox-dispatcher",
    "local.pnc.task-dashboard.viewer",
    "local.pnc.vm-task-sync",
)

PNC_RESIDENT_LABELS = (
    "ai.hermes.gateway",
    "local.pnc.completion-notice-relay",
    "local.pnc.rca-delivery-collector",
    "local.pnc.rca-delivery-dispatcher",
    "local.pnc.rca-kafka-consumer",
    "local.pnc.rca-outbox-dispatcher",
    "local.pnc.task-dashboard.viewer",
)

REQUIRED_RCA_RESIDENT_LABELS = frozenset({
    "ai.hermes.gateway",
    "local.pnc.rca-outbox-dispatcher",
    "local.pnc.rca-delivery-collector",
    "local.pnc.rca-delivery-dispatcher",
})

OUTBOUND_MODE_RESET_LABELS = frozenset({
    "local.pnc.rca-delivery-collector",
    "local.pnc.rca-delivery-dispatcher",
})


class LiveExecError(RuntimeError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_gid,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _read_owner_file(path: Path, *, max_bytes: int) -> bytes:
    descriptor = -1
    try:
        before = path.lstat()
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_ISLNK(before.st_mode)
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
            or stat.S_IMODE(before.st_mode) & 0o022
            or before.st_size <= 0
            or before.st_size > max_bytes
        ):
            raise LiveExecError("active_runtime_file_invalid")
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        opened = os.fstat(descriptor)
        if _identity(opened) != _identity(before):
            raise LiveExecError("active_runtime_file_changed")
        chunks: list[bytes] = []
        remaining = opened.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise LiveExecError("active_runtime_file_changed")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise LiveExecError("active_runtime_file_changed")
        if _identity(os.fstat(descriptor)) != _identity(before) or _identity(
            path.lstat()
        ) != _identity(before):
            raise LiveExecError("active_runtime_file_changed")
        return b"".join(chunks)
    except LiveExecError:
        raise
    except OSError as exc:
        raise LiveExecError("active_runtime_file_unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _load_manifest(path: Path) -> tuple[bytes, dict[str, Any]]:
    raw = _read_owner_file(path, max_bytes=MAX_MANIFEST_BYTES)
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LiveExecError("active_runtime_manifest_invalid") from exc
    if not isinstance(payload, dict):
        raise LiveExecError("active_runtime_manifest_invalid")
    return raw, payload


def _absolute_path(value: Any, *, code: str) -> Path:
    selected = Path(str(value or ""))
    if not selected.is_absolute():
        raise LiveExecError(code)
    return selected


def _require_direct_child(path: Path, parent: Path, *, code: str) -> None:
    try:
        if path.parent.resolve(strict=True) != parent.resolve(strict=True):
            raise LiveExecError(code)
    except OSError as exc:
        raise LiveExecError(code) from exc


def _git_identity(runtime_root: Path) -> tuple[str, str]:
    environment = {
        "HOME": str(Path.home()),
        "PATH": "/usr/bin:/bin",
        "GIT_CONFIG_NOSYSTEM": "1",
    }
    try:
        result = subprocess.run(
            [
                "/usr/bin/git",
                "-C",
                str(runtime_root),
                "rev-parse",
                "HEAD",
                "HEAD^{tree}",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
            env=environment,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise LiveExecError("active_runtime_git_identity_unavailable") from exc
    lines = result.stdout.splitlines()
    if len(lines) != 2:
        raise LiveExecError("active_runtime_git_identity_invalid")
    try:
        status = subprocess.run(
            [
                "/usr/bin/git",
                "-C",
                str(runtime_root),
                "status",
                "--porcelain=v1",
                "--untracked-files=all",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=20,
            env=environment,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise LiveExecError("active_runtime_git_identity_unavailable") from exc
    if status.stdout:
        raise LiveExecError("active_runtime_worktree_dirty")
    return lines[0].strip(), lines[1].strip()


def _stable_target_registry(runtime_root: Path) -> dict[str, dict[str, Any]]:
    path = runtime_root / STABLE_TARGET_REGISTRY_RELATIVE
    try:
        raw = _read_owner_file(path, max_bytes=1024 * 1024)
    except LiveExecError as exc:
        raise LiveExecError("active_runtime_stable_target_registry_invalid") from exc
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LiveExecError("active_runtime_stable_target_registry_invalid") from exc
    entries = payload.get("targets") if isinstance(payload, dict) else None
    expected_labels = {
        label
        for label, (target_kind, _relative) in SERVICE_TARGETS.items()
        if target_kind in {"governance_tool", "runtime_file"}
    }
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version")
        != STABLE_TARGET_REGISTRY_SCHEMA_VERSION
        or not isinstance(entries, dict)
        or set(entries) != expected_labels
    ):
        raise LiveExecError("active_runtime_stable_target_registry_invalid")
    normalized: dict[str, dict[str, Any]] = {}
    for label in sorted(expected_labels):
        item = entries.get(label)
        relative_target = SERVICE_TARGETS[label][1]
        expected_sha = str(item.get("sha256") or "") if isinstance(item, dict) else ""
        expected_size = item.get("size") if isinstance(item, dict) else None
        if (
            not isinstance(item, dict)
            or set(item) != {"target_kind", "relative_path", "sha256", "size"}
            or item.get("target_kind") != SERVICE_TARGETS[label][0]
            or item.get("relative_path") != relative_target
            or len(expected_sha) != 64
            or any(char not in "0123456789abcdef" for char in expected_sha)
            or type(expected_size) is not int
            or expected_size <= 0
        ):
            raise LiveExecError("active_runtime_stable_target_registry_invalid")
        normalized[label] = dict(item)
    return normalized


def _resolve_once(
    *, manifest_path: Path, hermes_home: Path, service_label: str
) -> tuple[bytes, dict[str, str]]:
    target_spec = SERVICE_TARGETS.get(service_label)
    if target_spec is None:
        raise LiveExecError("active_runtime_service_not_allowed")

    raw, manifest = _load_manifest(manifest_path)
    runtime_root = _absolute_path(
        manifest.get("runtime_root"), code="active_runtime_root_invalid"
    )
    runtime_venv = _absolute_path(
        manifest.get("runtime_venv"), code="active_runtime_venv_invalid"
    )
    runtime_python = _absolute_path(
        manifest.get("runtime_python"), code="active_runtime_python_invalid"
    )
    _require_direct_child(
        runtime_root,
        hermes_home / "runtime" / "releases",
        code="active_runtime_root_invalid",
    )
    _require_direct_child(
        runtime_venv,
        hermes_home / "runtime" / "venvs",
        code="active_runtime_venv_invalid",
    )
    if runtime_python != runtime_venv / "bin" / "python":
        raise LiveExecError("active_runtime_python_invalid")
    try:
        root_stat = runtime_root.lstat()
        python_stat = runtime_python.stat()
    except OSError as exc:
        raise LiveExecError("active_runtime_path_unavailable") from exc
    if (
        not stat.S_ISDIR(root_stat.st_mode)
        or stat.S_ISLNK(root_stat.st_mode)
        or root_stat.st_uid != os.geteuid()
        or stat.S_IMODE(root_stat.st_mode) & 0o022
    ):
        raise LiveExecError("active_runtime_root_invalid")
    if not stat.S_ISREG(python_stat.st_mode) or not os.access(runtime_python, os.X_OK):
        raise LiveExecError("active_runtime_python_invalid")

    faces = manifest.get("face_git_bindings")
    runtime_face = faces.get("runtime_engine") if isinstance(faces, dict) else None
    if not isinstance(runtime_face, dict):
        raise LiveExecError("active_runtime_binding_invalid")
    expected_commit = str(runtime_face.get("commit") or "")
    expected_tree = str(runtime_face.get("tree") or "")
    expected_repo = str(runtime_face.get("repo") or "")
    if (
        len(expected_commit) != 40
        or any(char not in "0123456789abcdef" for char in expected_commit)
        or len(expected_tree) != 40
        or any(char not in "0123456789abcdef" for char in expected_tree)
        or Path(expected_repo) != runtime_root
        or str(manifest.get("promotion_source_head") or "") != expected_commit
    ):
        raise LiveExecError("active_runtime_binding_invalid")
    actual_commit, actual_tree = _git_identity(runtime_root)
    if actual_commit != expected_commit:
        raise LiveExecError("active_runtime_commit_mismatch")
    if actual_tree != expected_tree:
        raise LiveExecError("active_runtime_tree_mismatch")

    target_kind, relative_target = target_spec
    if target_kind == "runtime_script":
        target = runtime_root / relative_target
    elif target_kind == "runtime_executable":
        target = runtime_venv / relative_target
    elif target_kind == "runtime_file":
        target = hermes_home / "runtime" / relative_target
    elif target_kind == "governance_tool":
        target = hermes_home / "runtime" / "governance-tools" / relative_target
    else:
        raise LiveExecError("active_runtime_service_target_invalid")
    try:
        target_raw = _read_owner_file(target, max_bytes=64 * 1024 * 1024)
    except LiveExecError as exc:
        raise LiveExecError("active_runtime_script_invalid") from exc
    if target_kind in {"governance_tool", "runtime_file"}:
        registered = _stable_target_registry(runtime_root)[service_label]
        if (
            len(target_raw) != registered["size"]
            or hashlib.sha256(target_raw).hexdigest() != registered["sha256"]
        ):
            raise LiveExecError("active_runtime_stable_target_mismatch")
    return raw, {
        "service_label": service_label,
        "target_kind": target_kind,
        "manifest_path": str(manifest_path),
        "manifest_sha256": hashlib.sha256(raw).hexdigest(),
        "runtime_root": str(runtime_root),
        "runtime_venv": str(runtime_venv),
        "runtime_python": str(runtime_python),
        "runtime_commit": actual_commit,
        "runtime_tree": actual_tree,
        "script": str(target),
        "script_sha256": hashlib.sha256(target_raw).hexdigest(),
    }


def resolve_active_runtime(
    *, manifest_path: Path, hermes_home: Path, service_label: str
) -> dict[str, str]:
    for _attempt in range(2):
        raw, resolved = _resolve_once(
            manifest_path=manifest_path,
            hermes_home=hermes_home,
            service_label=service_label,
        )
        confirmed, _payload = _load_manifest(manifest_path)
        if confirmed == raw:
            return resolved
    raise LiveExecError("active_runtime_manifest_changed")


def _exec_environment(resolved: dict[str, str], hermes_home: Path) -> dict[str, str]:
    runtime_venv = Path(resolved["runtime_venv"])
    home = Path.home()
    stable_paths = (
        runtime_venv / "bin",
        Path(resolved["runtime_root"]) / "node_modules" / ".bin",
        home / ".local" / "bin",
        home / "bin",
        Path("/opt/homebrew/bin"),
        Path("/usr/local/bin"),
        Path("/usr/bin"),
        Path("/bin"),
        Path("/usr/sbin"),
        Path("/sbin"),
    )
    environment = dict(os.environ)
    for key in (
        "HERMES_FRAMEWORK_ROOT",
        "HERMES_NATIVE_BIN",
        "PYTHONHOME",
        "PYTHONPATH",
    ):
        environment.pop(key, None)
    service_label = resolved["service_label"]
    if service_label in REQUIRED_RCA_RESIDENT_LABELS:
        for key in tuple(environment):
            if key.startswith("HERMES_RCA_"):
                environment.pop(key, None)
    if service_label in OUTBOUND_MODE_RESET_LABELS:
        environment.pop("HERMES_OUTBOUND_MODE", None)
        environment.pop("PNC_FOXGLOVE_RENDER_HOST", None)
    environment.update({
        "HERMES_HOME": str(hermes_home),
        "PATH": ":".join(str(path) for path in stable_paths),
        "PNC_LIVE_MANIFEST_SHA256": resolved["manifest_sha256"],
        "PNC_LIVE_RUNTIME_ROOT": resolved["runtime_root"],
        "PNC_LIVE_RUNTIME_COMMIT": resolved["runtime_commit"],
        "PNC_LIVE_RUNTIME_TREE": resolved["runtime_tree"],
        "PNC_LIVE_SERVICE_LABEL": resolved["service_label"],
        "PYTHONPATH": resolved["runtime_root"],
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1",
        "PYTHONUNBUFFERED": "1",
        "VIRTUAL_ENV": str(runtime_venv),
    })
    return environment


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Execute an allowlisted PNC service from the active manifest runtime"
    )
    parser.add_argument("--check", action="store_true")
    parser.add_argument("service_label")
    parser.add_argument("service_args", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    hermes_home = Path(
        os.environ.get("HERMES_HOME") or Path.home() / ".hermes"
    ).absolute()
    manifest_path = hermes_home / "runtime" / "LIVE_MANIFEST.json"
    try:
        resolved = resolve_active_runtime(
            manifest_path=manifest_path,
            hermes_home=hermes_home,
            service_label=args.service_label,
        )
        if args.check:
            print(json.dumps({"ok": True, **resolved}, sort_keys=True))
            return 0
        os.chdir(resolved["runtime_root"])
        runtime_python = resolved["runtime_python"]
        environment = _exec_environment(resolved, hermes_home)
        os.execve(
            runtime_python,
            [runtime_python, resolved["script"], *args.service_args],
            environment,
        )
    except LiveExecError as exc:
        print(
            json.dumps({"ok": False, "error": exc.code}, sort_keys=True),
            file=sys.stderr,
        )
        return 78
    except OSError:
        print(
            json.dumps(
                {"ok": False, "error": "active_runtime_exec_failed"},
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 78
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
