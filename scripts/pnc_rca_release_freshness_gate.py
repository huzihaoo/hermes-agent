#!/usr/bin/env python3
"""Fail a PNC host release when definitions or residents are not active-bound."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import plistlib
import re
import sqlite3
import stat
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import psutil

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.pnc_live_exec import (
    PNC_PYTHON_LAUNCHD_LABELS,
    PNC_RESIDENT_LABELS,
    SERVICE_TARGETS,
    LiveExecError,
    resolve_active_runtime,
)
from gateway.pnc_rca_quality_oracle import (
    release_golden_registry_status,
    validate_golden_registry_inventory,
)
from gateway.pnc_rca_conclusion_adjudication import (
    ADJUDICATION_EFFECT_SCHEMA_VERSION,
)
from gateway.pnc_rca_delivery_contract import (
    DELIVERY_EFFECT_SCHEMA_VERSION,
    TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION,
    TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_V1,
    TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION,
)
from gateway.pnc_rca_delivery_quarantine_migration import (
    COMBINED_TARGET_SCHEMA_VERSION,
    QuarantineMigrationError,
    validate_combined_target_schema,
)
from gateway.pnc_rca_control_store import (
    CONTROL_STORE_SCHEMA_VERSION,
    RcaControlStore,
)
from gateway.pnc_rca_delivery_store import RcaDeliveryStore


WATCHDOG_LABEL = "local.pnc.watcher-staleness-watchdog"
EXPECTED_LAUNCHD_LABELS = frozenset((*PNC_PYTHON_LAUNCHD_LABELS, WATCHDOG_LABEL))
FORBIDDEN_RUNTIME_MARKERS = (
    "/runtime/releases/",
    "/runtime/venvs/",
    "/runtime/hermes-live",
)
RETIRED_STABLE_ENTRYPOINTS = (
    "hermes-g1q3-e2e-smoke",
    "hermes-live",
)
UNRESOLVED_EFFECT_STATUSES = frozenset({
    "pending",
    "claimed",
    "retry_wait",
    "uncertain",
})
ACTIVE_EVALUATOR_INVENTORY_SCHEMA_VERSION = (
    "g1q3_rca_active_evaluator_inventory_binding_v1"
)
ACTIVE_EVALUATOR_INVENTORY_SOURCE_PATH = "api/g1q3_rca/consumer_capability.py"
ACTIVE_EVALUATOR_INVENTORY_SOURCE_SYMBOL = "G1Q3_EVALUATOR_INVENTORY"
_ACTIVE_EVALUATOR_INVENTORY_FIELDS = frozenset({
    "schema_version",
    "pipeline_commit",
    "pipeline_tree",
    "source_path",
    "source_symbol",
    "source_blob_sha256",
    "evaluator_scope",
    "evaluator_ids",
    "inventory_sha256",
})
_HEX40_RE = re.compile(r"^[0-9a-f]{40}$")
_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
_EVALUATOR_SCOPE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")


def _error(code: str, **detail: Any) -> dict[str, Any]:
    return {"code": code, **detail}


def _evaluator_inventory_sha256(
    evaluator_scope: str, evaluator_ids: Sequence[str]
) -> str:
    body = {
        "evaluator_scope": evaluator_scope,
        "evaluator_ids": list(evaluator_ids),
    }
    raw = json.dumps(
        body,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _read_git_object(
    source_root: Path,
    *args: str,
    error_code: str,
) -> bytes:
    env = os.environ.copy()
    env.update({"GIT_OPTIONAL_LOCKS": "0", "LC_ALL": "C"})
    try:
        completed = subprocess.run(
            ["git", "-C", str(source_root), *args],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=10,
            env=env,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ValueError(error_code) from exc
    if completed.returncode != 0:
        raise ValueError(error_code)
    return completed.stdout


def materialize_active_evaluator_inventory_binding(
    *,
    pipeline_source_root: Path,
    pipeline_commit: str,
    pipeline_tree: str,
) -> dict[str, Any]:
    """Extract the evaluator tuple from an exact pipeline source without importing it."""

    commit = str(pipeline_commit or "").strip().lower()
    tree = str(pipeline_tree or "").strip().lower()
    if _HEX40_RE.fullmatch(commit) is None or _HEX40_RE.fullmatch(tree) is None:
        raise ValueError("pnc_release_evaluator_inventory_pipeline_binding_invalid")

    source_root = Path(pipeline_source_root).expanduser().absolute().resolve()
    repository_error = "pnc_release_evaluator_inventory_pipeline_repository_invalid"
    top_level_raw = _read_git_object(
        source_root,
        "rev-parse",
        "--show-toplevel",
        error_code=repository_error,
    )
    inside_worktree = _read_git_object(
        source_root,
        "rev-parse",
        "--is-inside-work-tree",
        error_code=repository_error,
    )
    try:
        top_level = Path(os.fsdecode(top_level_raw.rstrip(b"\r\n"))).resolve(
            strict=True
        )
    except (OSError, ValueError) as exc:
        raise ValueError(repository_error) from exc
    if top_level != source_root or inside_worktree.strip() != b"true":
        raise ValueError(repository_error)

    resolved_commit = _read_git_object(
        source_root,
        "rev-parse",
        "--verify",
        f"{commit}^{{commit}}",
        error_code="pnc_release_evaluator_inventory_pipeline_commit_invalid",
    ).strip()
    if resolved_commit != commit.encode("ascii"):
        raise ValueError("pnc_release_evaluator_inventory_pipeline_commit_invalid")
    head_commit = _read_git_object(
        source_root,
        "rev-parse",
        "--verify",
        "HEAD^{commit}",
        error_code=repository_error,
    ).strip()
    if head_commit != resolved_commit:
        raise ValueError("pnc_release_evaluator_inventory_pipeline_commit_mismatch")

    resolved_tree = _read_git_object(
        source_root,
        "rev-parse",
        "--verify",
        f"{commit}^{{tree}}",
        error_code="pnc_release_evaluator_inventory_pipeline_tree_invalid",
    ).strip()
    if resolved_tree != tree.encode("ascii"):
        raise ValueError("pnc_release_evaluator_inventory_pipeline_tree_mismatch")

    source_path = source_root / ACTIVE_EVALUATOR_INVENTORY_SOURCE_PATH
    try:
        resolved_source_path = source_path.resolve(strict=True)
        resolved_source_path.relative_to(source_root)
        source_stat = source_path.lstat()
        if (
            resolved_source_path != source_path
            or stat.S_ISLNK(source_stat.st_mode)
            or not stat.S_ISREG(source_stat.st_mode)
        ):
            raise ValueError("pnc_release_evaluator_inventory_source_invalid")
        source_bytes = source_path.read_bytes()
    except (OSError, ValueError) as exc:
        if isinstance(exc, ValueError):
            raise
        raise ValueError("pnc_release_evaluator_inventory_source_unreadable") from exc

    source_path_raw = ACTIVE_EVALUATOR_INVENTORY_SOURCE_PATH.encode("utf-8")
    tracked_paths = _read_git_object(
        source_root,
        "ls-files",
        "--error-unmatch",
        "-z",
        "--",
        ACTIVE_EVALUATOR_INVENTORY_SOURCE_PATH,
        error_code="pnc_release_evaluator_inventory_source_untracked",
    )
    if tracked_paths != source_path_raw + b"\0":
        raise ValueError("pnc_release_evaluator_inventory_source_untracked")
    source_status = _read_git_object(
        source_root,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
        "--no-renames",
        "-z",
        "--",
        ACTIVE_EVALUATOR_INVENTORY_SOURCE_PATH,
        error_code="pnc_release_evaluator_inventory_source_provenance_invalid",
    )
    if source_status:
        raise ValueError("pnc_release_evaluator_inventory_source_dirty")

    commit_source = f"{commit}:{ACTIVE_EVALUATOR_INVENTORY_SOURCE_PATH}"
    source_object_type = _read_git_object(
        source_root,
        "cat-file",
        "-t",
        commit_source,
        error_code="pnc_release_evaluator_inventory_source_blob_invalid",
    ).strip()
    if source_object_type != b"blob":
        raise ValueError("pnc_release_evaluator_inventory_source_blob_invalid")
    committed_source_bytes = _read_git_object(
        source_root,
        "cat-file",
        "blob",
        commit_source,
        error_code="pnc_release_evaluator_inventory_source_blob_invalid",
    )
    if source_bytes != committed_source_bytes:
        raise ValueError("pnc_release_evaluator_inventory_source_blob_mismatch")

    try:
        module = ast.parse(source_bytes.decode("utf-8"), filename=str(source_path))
    except (UnicodeDecodeError, SyntaxError) as exc:
        raise ValueError("pnc_release_evaluator_inventory_source_invalid") from exc

    assignments: dict[str, ast.AST] = {}
    for node in module.body:
        if isinstance(node, ast.Assign):
            targets = node.targets
        elif isinstance(node, ast.AnnAssign):
            targets = [node.target]
        else:
            continue
        for target in targets:
            if isinstance(target, ast.Name) and target.id in {
                "G1Q3_EVALUATOR_SCOPE",
                ACTIVE_EVALUATOR_INVENTORY_SOURCE_SYMBOL,
            }:
                if target.id in assignments:
                    raise ValueError("pnc_release_evaluator_inventory_source_invalid")
                assignments[target.id] = node.value

    try:
        evaluator_scope = ast.literal_eval(assignments["G1Q3_EVALUATOR_SCOPE"])
        raw_evaluator_ids = ast.literal_eval(
            assignments[ACTIVE_EVALUATOR_INVENTORY_SOURCE_SYMBOL]
        )
    except (KeyError, ValueError, TypeError, SyntaxError) as exc:
        raise ValueError("pnc_release_evaluator_inventory_source_invalid") from exc

    if not isinstance(evaluator_scope, str) or not isinstance(
        raw_evaluator_ids, (list, tuple)
    ):
        raise ValueError("pnc_release_evaluator_inventory_source_invalid")
    inventory = validate_golden_registry_inventory(
        raw_evaluator_ids,
        tuple(raw_evaluator_ids),
        present=True,
    )
    if not inventory["valid"] or _EVALUATOR_SCOPE_RE.fullmatch(evaluator_scope) is None:
        raise ValueError("pnc_release_evaluator_inventory_source_invalid")
    evaluator_ids = tuple(inventory["required_evaluator_ids"])
    source_blob_sha256 = hashlib.sha256(source_bytes).hexdigest()
    return {
        "schema_version": ACTIVE_EVALUATOR_INVENTORY_SCHEMA_VERSION,
        "pipeline_commit": commit,
        "pipeline_tree": tree,
        "source_path": ACTIVE_EVALUATOR_INVENTORY_SOURCE_PATH,
        "source_symbol": ACTIVE_EVALUATOR_INVENTORY_SOURCE_SYMBOL,
        "source_blob_sha256": source_blob_sha256,
        "evaluator_scope": evaluator_scope,
        "evaluator_ids": list(evaluator_ids),
        "inventory_sha256": _evaluator_inventory_sha256(
            evaluator_scope,
            evaluator_ids,
        ),
    }


def audit_active_evaluator_inventory(
    *, hermes_home: Path
) -> tuple[dict[str, Any], tuple[str, ...], list[dict[str, Any]]]:
    """Load the exact active pipeline inventory materialized during release."""

    manifest_path = hermes_home / "runtime" / "LIVE_MANIFEST.json"
    errors: list[dict[str, Any]] = []
    reasons: list[str] = []
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        manifest = {}
        reasons.append("manifest_unreadable")

    faces = manifest.get("face_git_bindings") if isinstance(manifest, Mapping) else {}
    pipeline = faces.get("g1q3_rca_pipeline") if isinstance(faces, Mapping) else {}
    pipeline = pipeline if isinstance(pipeline, Mapping) else {}
    active_commit = str(pipeline.get("commit") or "").strip().lower()
    active_tree = str(pipeline.get("tree") or "").strip().lower()
    binding = pipeline.get("evaluator_inventory")
    binding = binding if isinstance(binding, Mapping) else {}

    if _HEX40_RE.fullmatch(active_commit) is None:
        reasons.append("active_pipeline_commit_invalid")
    if _HEX40_RE.fullmatch(active_tree) is None:
        reasons.append("active_pipeline_tree_invalid")
    if not binding:
        reasons.append("binding_missing")
    elif set(binding) != _ACTIVE_EVALUATOR_INVENTORY_FIELDS:
        reasons.append("binding_shape_invalid")

    schema_version = str(binding.get("schema_version") or "").strip()
    bound_commit = str(binding.get("pipeline_commit") or "").strip().lower()
    bound_tree = str(binding.get("pipeline_tree") or "").strip().lower()
    source_path = str(binding.get("source_path") or "").strip()
    source_symbol = str(binding.get("source_symbol") or "").strip()
    source_blob_sha256 = str(binding.get("source_blob_sha256") or "").strip().lower()
    evaluator_scope = str(binding.get("evaluator_scope") or "").strip()
    observed_inventory_sha256 = (
        str(binding.get("inventory_sha256") or "").strip().lower()
    )
    raw_evaluator_ids = binding.get("evaluator_ids")
    covered_ids = (
        tuple(raw_evaluator_ids) if isinstance(raw_evaluator_ids, (list, tuple)) else ()
    )
    inventory_binding = validate_golden_registry_inventory(
        raw_evaluator_ids,
        covered_ids,
        present=True,
    )
    evaluator_ids = tuple(inventory_binding["required_evaluator_ids"])

    if schema_version != ACTIVE_EVALUATOR_INVENTORY_SCHEMA_VERSION:
        reasons.append("schema_version_invalid")
    if bound_commit != active_commit or _HEX40_RE.fullmatch(bound_commit) is None:
        reasons.append("pipeline_commit_mismatch")
    if bound_tree != active_tree or _HEX40_RE.fullmatch(bound_tree) is None:
        reasons.append("pipeline_tree_mismatch")
    if source_path != ACTIVE_EVALUATOR_INVENTORY_SOURCE_PATH:
        reasons.append("source_path_invalid")
    if source_symbol != ACTIVE_EVALUATOR_INVENTORY_SOURCE_SYMBOL:
        reasons.append("source_symbol_invalid")
    if _HEX64_RE.fullmatch(source_blob_sha256) is None:
        reasons.append("source_blob_sha256_invalid")
    if _EVALUATOR_SCOPE_RE.fullmatch(evaluator_scope) is None:
        reasons.append("evaluator_scope_invalid")
    reasons.extend(str(code) for code in inventory_binding["errors"])
    expected_inventory_sha256 = _evaluator_inventory_sha256(
        evaluator_scope,
        evaluator_ids,
    )
    if (
        _HEX64_RE.fullmatch(observed_inventory_sha256) is None
        or observed_inventory_sha256 != expected_inventory_sha256
    ):
        reasons.append("inventory_sha256_mismatch")

    normalized_reasons = sorted(set(reasons))
    if normalized_reasons:
        errors.append(
            _error(
                "pnc_release_golden_active_evaluator_inventory_invalid",
                reasons=normalized_reasons,
            )
        )
    evidence = {
        "manifest_path": str(manifest_path),
        "valid": not normalized_reasons,
        "active_pipeline_commit": active_commit,
        "active_pipeline_tree": active_tree,
        "schema_version": schema_version,
        "pipeline_commit": bound_commit,
        "pipeline_tree": bound_tree,
        "source_path": source_path,
        "source_symbol": source_symbol,
        "source_blob_sha256": source_blob_sha256,
        "evaluator_scope": evaluator_scope,
        "evaluator_ids": list(evaluator_ids),
        "evaluator_count": len(evaluator_ids),
        "inventory_sha256": observed_inventory_sha256,
        "expected_inventory_sha256": expected_inventory_sha256,
        "errors": normalized_reasons,
    }
    return evidence, evaluator_ids, errors


def _read_plist(path: Path) -> Mapping[str, Any]:
    try:
        payload = plistlib.loads(path.read_bytes())
    except (OSError, ValueError, plistlib.InvalidFileException) as exc:
        raise ValueError("pnc_release_plist_invalid") from exc
    if not isinstance(payload, Mapping):
        raise ValueError("pnc_release_plist_invalid")
    return payload


def _plist_text(payload: Mapping[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=True, sort_keys=True)


def audit_persisted_definitions(
    *, home: Path, hermes_home: Path
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    launch_agent_dir = home / "Library" / "LaunchAgents"
    discovered: dict[str, Path] = {}
    errors: list[dict[str, Any]] = []
    paths = [
        *sorted(launch_agent_dir.glob("local.pnc.*.plist")),
        launch_agent_dir / "ai.hermes.gateway.plist",
    ]
    for path in paths:
        if not path.exists():
            continue
        try:
            payload = _read_plist(path)
        except ValueError as exc:
            errors.append(_error(str(exc), path=str(path)))
            continue
        label = str(payload.get("Label") or "")
        if not label:
            errors.append(_error("pnc_release_plist_label_missing", path=str(path)))
            continue
        if label in discovered:
            errors.append(_error("pnc_release_plist_label_duplicate", label=label))
            continue
        discovered[label] = path

    missing = sorted(EXPECTED_LAUNCHD_LABELS - discovered.keys())
    unknown = sorted(discovered.keys() - EXPECTED_LAUNCHD_LABELS)
    errors.extend(_error("pnc_release_plist_missing", label=label) for label in missing)
    errors.extend(
        _error("pnc_release_plist_unregistered", label=label) for label in unknown
    )

    launcher = hermes_home / "runtime" / "governance-tools" / "pnc_live_exec.py"
    stable_working_directory = hermes_home / "runtime"
    watchdog = (
        hermes_home / "runtime" / "governance-tools" / "watcher-staleness-watchdog.sh"
    )
    evidence: list[dict[str, Any]] = []
    for label in sorted(EXPECTED_LAUNCHD_LABELS & discovered.keys()):
        path = discovered[label]
        payload = _read_plist(path)
        arguments = payload.get("ProgramArguments")
        environment = payload.get("EnvironmentVariables")
        text = _plist_text(payload)
        item_errors: list[str] = []
        if not isinstance(arguments, list) or any(
            not isinstance(value, str) for value in arguments
        ):
            item_errors.append("pnc_release_plist_arguments_invalid")
            arguments = []
        expected_prefix = (
            ["/bin/zsh", str(watchdog)]
            if label == WATCHDOG_LABEL
            else ["/usr/bin/python3", str(launcher), label]
        )
        if arguments[: len(expected_prefix)] != expected_prefix:
            item_errors.append("pnc_release_plist_bypasses_active_manifest")
        if payload.get("WorkingDirectory") != str(stable_working_directory):
            item_errors.append("pnc_release_plist_working_directory_unstable")
        if not isinstance(environment, Mapping):
            item_errors.append("pnc_release_plist_environment_invalid")
        elif "VIRTUAL_ENV" in environment:
            item_errors.append("pnc_release_plist_virtualenv_pinned")
        if any(marker in text for marker in FORBIDDEN_RUNTIME_MARKERS):
            item_errors.append("pnc_release_plist_runtime_pinned")
        evidence.append({
            "label": label,
            "path": str(path),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "arguments": arguments,
            "errors": item_errors,
        })
        errors.extend(_error(code, label=label) for code in item_errors)
    return evidence, errors


def _launchctl_print(label: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["launchctl", "print", f"gui/{os.getuid()}/{label}"],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )


def _parse_launchctl(
    label: str, result: subprocess.CompletedProcess[str]
) -> dict[str, Any]:
    raw = result.stdout if result.returncode == 0 else result.stderr
    parsed: dict[str, Any] = {
        "label": label,
        "loaded": result.returncode == 0,
        "program": "",
        "working_directory": "",
        "pid": None,
        "raw": raw,
        "raw_sha256": hashlib.sha256(raw.encode("utf-8")).hexdigest(),
    }
    for raw_line in raw.splitlines():
        line = raw_line.strip()
        if line.startswith("program = "):
            parsed["program"] = line.split("=", 1)[1].strip()
        elif line.startswith("working directory = "):
            parsed["working_directory"] = line.split("=", 1)[1].strip()
        elif line.startswith("pid = "):
            try:
                parsed["pid"] = int(line.split("=", 1)[1].strip())
            except ValueError:
                parsed["pid"] = None
    return parsed


def audit_loaded_definitions(
    *,
    hermes_home: Path,
    launchctl_reader: Callable[
        [str], subprocess.CompletedProcess[str]
    ] = _launchctl_print,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    launcher = hermes_home / "runtime" / "governance-tools" / "pnc_live_exec.py"
    watchdog = (
        hermes_home / "runtime" / "governance-tools" / "watcher-staleness-watchdog.sh"
    )
    stable_working_directory = str(hermes_home / "runtime")
    evidence: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for label in sorted(EXPECTED_LAUNCHD_LABELS):
        loaded = _parse_launchctl(label, launchctl_reader(label))
        raw = str(loaded.pop("raw"))
        item_errors: list[str] = []
        if not loaded["loaded"]:
            item_errors.append("pnc_release_launchd_job_missing")
        else:
            expected_program = (
                "/bin/zsh" if label == WATCHDOG_LABEL else "/usr/bin/python3"
            )
            if loaded["program"] != expected_program:
                item_errors.append("pnc_release_loaded_program_stale")
            required = (
                (str(watchdog),) if label == WATCHDOG_LABEL else (str(launcher), label)
            )
            if any(value not in raw for value in required):
                item_errors.append("pnc_release_loaded_definition_bypasses_manifest")
            if loaded["working_directory"] != stable_working_directory:
                item_errors.append("pnc_release_loaded_working_directory_unstable")
            if any(marker in raw for marker in FORBIDDEN_RUNTIME_MARKERS):
                item_errors.append("pnc_release_loaded_runtime_pinned")
        loaded["errors"] = item_errors
        evidence.append(loaded)
        errors.extend(_error(code, label=label) for code in item_errors)
    return evidence, errors


def _resolve_targets(
    *, hermes_home: Path
) -> tuple[dict[str, dict[str, str]], list[dict[str, Any]]]:
    manifest_path = hermes_home / "runtime" / "LIVE_MANIFEST.json"
    resolved: dict[str, dict[str, str]] = {}
    errors: list[dict[str, Any]] = []
    for label in PNC_PYTHON_LAUNCHD_LABELS:
        try:
            resolved[label] = resolve_active_runtime(
                manifest_path=manifest_path,
                hermes_home=hermes_home,
                service_label=label,
            )
        except LiveExecError as exc:
            errors.append(_error(exc.code, label=label))
    return resolved, errors


def _process_evidence(
    *, label: str, pid: int, resolved: Mapping[str, str]
) -> tuple[dict[str, Any], list[str]]:
    item_errors: list[str] = []
    evidence: dict[str, Any] = {"label": label, "pid": pid}
    try:
        process = psutil.Process(pid)
        cmdline = process.cmdline()
        cwd = process.cwd()
        environment = process.environ()
        evidence.update({
            "create_time": process.create_time(),
            "cwd": cwd,
            "cmdline": cmdline,
            "runtime_commit": environment.get("PNC_LIVE_RUNTIME_COMMIT", ""),
            "service_label": environment.get("PNC_LIVE_SERVICE_LABEL", ""),
            "virtual_env": environment.get("VIRTUAL_ENV", ""),
        })
    except (psutil.Error, OSError) as exc:
        evidence["process_error"] = type(exc).__name__
        return evidence, ["pnc_release_resident_process_unreadable"]

    if cwd != resolved["runtime_root"]:
        item_errors.append("pnc_release_resident_cwd_stale")
    if resolved["script"] not in cmdline:
        item_errors.append("pnc_release_resident_script_stale")
    if evidence["runtime_commit"] != resolved["runtime_commit"]:
        item_errors.append("pnc_release_resident_commit_stale")
    if evidence["service_label"] != label:
        item_errors.append("pnc_release_resident_launcher_bypassed")
    if evidence["virtual_env"] != resolved["runtime_venv"]:
        item_errors.append("pnc_release_resident_venv_stale")
    return evidence, item_errors


def audit_residents(
    *,
    loaded: list[dict[str, Any]],
    resolved: Mapping[str, Mapping[str, str]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    by_label = {str(item["label"]): item for item in loaded}
    evidence: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for label in PNC_RESIDENT_LABELS:
        item = by_label.get(label, {})
        pid = item.get("pid")
        target = resolved.get(label)
        if not isinstance(pid, int) or pid <= 0:
            errors.append(_error("pnc_release_resident_not_running", label=label))
            evidence.append({
                "label": label,
                "pid": pid,
                "errors": ["pnc_release_resident_not_running"],
            })
            continue
        if target is None:
            errors.append(_error("pnc_release_resident_target_unresolved", label=label))
            evidence.append({
                "label": label,
                "pid": pid,
                "errors": ["pnc_release_resident_target_unresolved"],
            })
            continue
        process, item_errors = _process_evidence(label=label, pid=pid, resolved=target)
        process["errors"] = item_errors
        evidence.append(process)
        errors.extend(_error(code, label=label, pid=pid) for code in item_errors)
    return evidence, errors


def audit_wrappers(home: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    evidence: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for path in sorted((home / "bin").glob("hermes*")):
        if not path.is_file():
            continue
        try:
            raw = path.read_bytes()
            text = raw.decode("utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        markers = [marker for marker in FORBIDDEN_RUNTIME_MARKERS if marker in text]
        item = {
            "path": str(path),
            "sha256": hashlib.sha256(raw).hexdigest(),
            "forbidden_markers": markers,
        }
        evidence.append(item)
        if markers:
            errors.append(
                _error(
                    "pnc_release_wrapper_runtime_pinned",
                    path=str(path),
                    markers=markers,
                )
            )
    return evidence, errors


def audit_retired_stable_entrypoints(
    home: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Require source-retired stable commands to be absent after release."""

    evidence: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for name in RETIRED_STABLE_ENTRYPOINTS:
        path = home / "bin" / name
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            evidence.append({"name": name, "path": str(path), "present": False})
            continue
        except OSError as exc:
            evidence.append({
                "name": name,
                "path": str(path),
                "present": None,
                "error": type(exc).__name__,
            })
            errors.append(
                _error(
                    "pnc_release_retired_entrypoint_unreadable",
                    name=name,
                    path=str(path),
                )
            )
            continue
        evidence.append({
            "name": name,
            "path": str(path),
            "present": True,
            "mode": stat.S_IMODE(metadata.st_mode),
        })
        errors.append(
            _error(
                "pnc_release_retired_entrypoint_present",
                name=name,
                path=str(path),
            )
        )
    return evidence, errors


def audit_versioned_stable_entrypoints(
    *, home: Path, hermes_home: Path, runtime_root: Path
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Require stable bootstrap files to equal their active-release sources."""

    pairs = [
        (
            "pnc_live_exec.py",
            hermes_home / "runtime" / "governance-tools" / "pnc_live_exec.py",
            runtime_root / "scripts" / "pnc_live_exec.py",
        ),
        (
            "pnc_rca_release_freshness_gate.py",
            hermes_home
            / "runtime"
            / "governance-tools"
            / "pnc_rca_release_freshness_gate.py",
            runtime_root / "scripts" / "pnc_rca_release_freshness_gate.py",
        ),
        (
            "watcher-staleness-watchdog.sh",
            hermes_home
            / "runtime"
            / "governance-tools"
            / "watcher-staleness-watchdog.sh",
            runtime_root / "scripts" / "watcher_staleness_watchdog.sh",
        ),
    ]
    wrapper_source = runtime_root / "scripts" / "wrappers"
    try:
        wrapper_paths = sorted(wrapper_source.iterdir())
    except OSError:
        wrapper_paths = []
        pairs.append((
            "scripts/wrappers",
            home / "bin" / ".pnc-wrapper-source-missing",
            wrapper_source,
        ))
    else:
        pairs.extend(
            (f"wrapper:{path.name}", home / "bin" / path.name, path)
            for path in wrapper_paths
        )

    evidence: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for name, stable_path, source_path in pairs:
        item: dict[str, Any] = {
            "name": name,
            "stable_path": str(stable_path),
            "source_path": str(source_path),
            "stable_sha256": "",
            "source_sha256": "",
            "exact": False,
            "errors": [],
        }
        item_errors: list[str] = []
        try:
            source_stat = source_path.lstat()
            source_raw = source_path.read_bytes()
        except OSError:
            item_errors.append("pnc_release_versioned_source_missing")
            source_stat = None
            source_raw = b""
        if source_stat is not None and (
            not stat.S_ISREG(source_stat.st_mode) or stat.S_ISLNK(source_stat.st_mode)
        ):
            item_errors.append("pnc_release_versioned_source_invalid")
        try:
            stable_stat = stable_path.lstat()
            stable_raw = stable_path.read_bytes()
        except OSError:
            item_errors.append("pnc_release_stable_entrypoint_missing")
            stable_stat = None
            stable_raw = b""
        if stable_stat is not None and (
            not stat.S_ISREG(stable_stat.st_mode) or stat.S_ISLNK(stable_stat.st_mode)
        ):
            item_errors.append("pnc_release_stable_entrypoint_invalid")
        if source_raw:
            item["source_sha256"] = hashlib.sha256(source_raw).hexdigest()
        if stable_raw:
            item["stable_sha256"] = hashlib.sha256(stable_raw).hexdigest()
        if source_raw and stable_raw and source_raw != stable_raw:
            item_errors.append("pnc_release_stable_entrypoint_stale")
        item["exact"] = bool(source_raw and stable_raw and not item_errors)
        item["errors"] = item_errors
        evidence.append(item)
        errors.extend(_error(code, name=name) for code in item_errors)
    return evidence, errors


def audit_stable_targets(
    *,
    hermes_home: Path,
    runtime_resolver: Callable[..., dict[str, str]] = resolve_active_runtime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    manifest_path = hermes_home / "runtime" / "LIVE_MANIFEST.json"
    labels = sorted(
        label
        for label, (target_kind, _relative) in SERVICE_TARGETS.items()
        if target_kind in {"governance_tool", "runtime_file"}
    )
    evidence: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for label in labels:
        try:
            resolved = runtime_resolver(
                manifest_path=manifest_path,
                hermes_home=hermes_home,
                service_label=label,
            )
        except LiveExecError as exc:
            evidence.append({"label": label, "errors": [exc.code]})
            errors.append(_error(exc.code, label=label))
            continue
        evidence.append({
            "label": label,
            "script": resolved["script"],
            "script_sha256": resolved["script_sha256"],
            "runtime_commit": resolved["runtime_commit"],
            "errors": [],
        })
    return evidence, errors


def _normalize_golden_scope_ids(value: Any) -> tuple[tuple[str, ...], bool, tuple[str, ...]]:
    """Normalize the optional high-confidence scope; an empty scope is valid."""

    if not isinstance(value, (list, tuple)):
        return (), False, ("golden_scope_evaluator_ids_not_list",)
    normalized: list[str] = []
    seen: set[str] = set()
    errors: list[str] = []
    for item in value:
        evaluator_id = str(item).strip() if isinstance(item, str) else ""
        if _EVALUATOR_SCOPE_RE.fullmatch(evaluator_id) is None:
            errors.append("golden_scope_evaluator_id_invalid")
            continue
        if evaluator_id in seen:
            errors.append("golden_scope_evaluator_ids_duplicate")
            continue
        seen.add(evaluator_id)
        normalized.append(evaluator_id)
    return tuple(sorted(normalized)), not errors, tuple(sorted(set(errors)))


def _machine_observation_evaluator_ids(
    evaluator_entries: Mapping[str, Any] | None,
) -> tuple[str, ...]:
    """Find explicit machine observations that must never become goldens."""

    markers = {
        "machine",
        "machine_observation",
        "live_machine_observation",
        "runtime_observation",
        "observed",
        "synthetic",
        "unit_fixture",
        "decoded_observation",
    }
    found: set[str] = set()
    for evaluator_id, item in (evaluator_entries or {}).items():
        if not isinstance(item, Mapping):
            continue
        provenance = item.get("provenance")
        values: list[Any] = [
            item.get("source_kind"),
            item.get("golden_source_kind"),
            item.get("origin"),
            item.get("generated_by"),
        ]
        if isinstance(provenance, Mapping):
            values.extend((provenance.get("kind"), provenance.get("origin")))
        normalized = {
            str(value).strip().lower().replace("-", "_").replace(" ", "_")
            for value in values
            if isinstance(value, str) and value.strip()
        }
        if any(
            value in markers
            or ("machine" in value and "observation" in value)
            for value in normalized
        ):
            found.add(str(evaluator_id))
    return tuple(sorted(found))


def audit_release_golden_registry(
    *,
    hermes_home: Path,
    registry: Mapping[str, Any] | None = None,
    required_evaluator_ids: Sequence[str] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if registry is not None:
        observed = dict(registry)
    else:
        # The registry covers only the explicit high-confidence scope. The
        # complete active inventory is supplied separately from LIVE_MANIFEST;
        # passing it into release_golden_registry_status would incorrectly
        # turn every active evaluator into a golden requirement.
        observed = dict(release_golden_registry_status())
    errors: list[dict[str, Any]] = []
    if observed.get("valid") is not True:
        errors.append(_error("pnc_release_golden_registry_invalid"))
    if observed.get("low_tier_golden_ready") is not True:
        errors.append(_error("pnc_release_low_tier_golden_not_ready"))
    evaluator_entries = observed.get("evaluators")
    covered_ids = tuple(
        sorted(str(value) for value in evaluator_entries)
        if isinstance(evaluator_entries, Mapping)
        else ()
    )

    # The active inventory remains an exact, provenance-bound requirement, but
    # golden coverage is a separate, potentially empty subset of that inventory.
    active_binding = validate_golden_registry_inventory(
        required_evaluator_ids,
        tuple(required_evaluator_ids or ())
        if isinstance(required_evaluator_ids, (list, tuple))
        else (),
        present=required_evaluator_ids is not None,
    )
    if required_evaluator_ids is None:
        errors.append(
            _error(
                "pnc_release_golden_active_evaluator_inventory_invalid",
                reasons=["active_inventory_not_supplied"],
            )
        )
    elif not active_binding["valid"]:
        errors.append(
            _error(
                "pnc_release_golden_active_evaluator_inventory_invalid",
                reasons=list(active_binding["errors"]),
            )
        )
    active_ids = tuple(active_binding["required_evaluator_ids"])
    active_set = set(active_ids)
    unexpected_ids = tuple(sorted(set(covered_ids) - active_set))
    if unexpected_ids:
        errors.append(
            _error(
                "pnc_release_golden_evaluator_inventory_unexpected",
                evaluator_ids=list(unexpected_ids),
            )
        )

    scope_present = bool(observed.get("golden_scope_explicit")) or (
        "golden_scope_evaluator_ids" in observed
    )
    if scope_present:
        scope_ids, scope_valid, scope_errors = _normalize_golden_scope_ids(
            observed.get("golden_scope_evaluator_ids")
        )
    else:
        scope_ids, scope_valid, scope_errors = covered_ids, True, ()
    if set(scope_ids) != set(covered_ids):
        scope_errors = tuple(
            sorted({*scope_errors, "golden_scope_evaluator_set_mismatch"})
        )
        scope_valid = False
    if not scope_valid:
        errors.append(
            _error(
                "pnc_release_golden_high_scope_invalid",
                reasons=list(scope_errors),
            )
        )
    scope_only_not_active = set(scope_ids) - active_set - set(unexpected_ids)
    if scope_only_not_active:
        errors.append(
            _error(
                "pnc_release_golden_high_scope_not_active",
                evaluator_ids=list(sorted(scope_only_not_active)),
            )
        )

    # Preserve the old explicit registry binding as an exact-set declaration,
    # but never compare it to the full active inventory here.
    observed_required_present = bool(observed.get("required_evaluator_ids_present"))
    if "required_evaluator_ids_present" not in observed:
        observed_required_present = "required_evaluator_ids" in observed
    observed_required = observed.get("required_evaluator_ids")
    if observed_required_present:
        declared_binding = validate_golden_registry_inventory(
            observed_required,
            covered_ids,
            present=True,
        )
        if not declared_binding["valid"]:
            errors.append(
                _error(
                    "pnc_release_golden_high_scope_exact_set_mismatch",
                    reasons=list(declared_binding["errors"]),
                    evaluator_ids=list(
                        sorted(
                            set(declared_binding["missing_required_evaluator_ids"])
                            | set(declared_binding["unexpected_evaluator_ids"])
                        )
                    ),
                )
            )
    machine_observation_ids = _machine_observation_evaluator_ids(
        evaluator_entries if isinstance(evaluator_entries, Mapping) else None
    )
    if machine_observation_ids:
        errors.append(
            _error(
                "pnc_release_golden_machine_observation_not_golden",
                evaluator_ids=list(machine_observation_ids),
            )
        )
    invalid_ids = observed.get("invalid_evaluator_ids")
    if isinstance(invalid_ids, (list, tuple)) and invalid_ids:
        errors.append(
            _error(
                "pnc_release_golden_evaluator_id_invalid",
                evaluator_ids=list(invalid_ids),
            )
        )
    duplicate_ids = observed.get("duplicate_evaluator_ids")
    if isinstance(duplicate_ids, (list, tuple)) and duplicate_ids:
        errors.append(
            _error(
                "pnc_release_golden_evaluator_id_duplicate",
                evaluator_ids=list(duplicate_ids),
            )
        )
    non_distinct_ids = observed.get("non_distinct_evaluator_ids")
    if isinstance(non_distinct_ids, (list, tuple)) and non_distinct_ids:
        errors.append(
            _error(
                "pnc_release_golden_evaluator_hashes_not_distinct",
                evaluator_ids=list(non_distinct_ids),
            )
        )
    invalid_source_ids = observed.get("invalid_golden_source_ids")
    if isinstance(invalid_source_ids, (list, tuple)) and invalid_source_ids:
        errors.append(
            _error(
                "pnc_release_golden_source_provenance_invalid",
                evaluator_ids=list(invalid_source_ids),
            )
        )
    observed_machine_ids = observed.get("machine_observation_evaluator_ids")
    if (
        isinstance(observed_machine_ids, (list, tuple))
        and observed_machine_ids
        and not machine_observation_ids
    ):
        errors.append(
            _error(
                "pnc_release_golden_machine_observation_not_golden",
                evaluator_ids=list(observed_machine_ids),
            )
        )
    manifest_path = hermes_home / "runtime" / "LIVE_MANIFEST.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        manifest = {}
        errors.append(_error("pnc_release_manifest_unreadable_for_golden"))
    faces = manifest.get("face_git_bindings") if isinstance(manifest, Mapping) else {}
    pipeline = faces.get("g1q3_rca_pipeline") if isinstance(faces, Mapping) else {}
    active_commit = (
        str(pipeline.get("commit") or "") if isinstance(pipeline, Mapping) else ""
    )
    active_tree = (
        str(pipeline.get("tree") or "") if isinstance(pipeline, Mapping) else ""
    )
    if (
        observed.get("pipeline_commit") != active_commit
        or observed.get("pipeline_tree") != active_tree
    ):
        errors.append(
            _error(
                "pnc_release_golden_pipeline_binding_mismatch",
                active_commit=active_commit,
                active_tree=active_tree,
            )
        )
    evidence = {
        **observed,
        "manifest_path": str(manifest_path),
        "active_pipeline_commit": active_commit,
        "active_pipeline_tree": active_tree,
        "required_evaluator_ids": list(active_ids),
        "required_evaluator_ids_present": required_evaluator_ids is not None,
        "inventory_binding_valid": bool(
            required_evaluator_ids is not None
            and active_binding["valid"]
            and not unexpected_ids
        ),
        "missing_required_evaluator_ids": [],
        "unexpected_evaluator_ids": list(unexpected_ids),
        "inventory_binding_errors": list(active_binding["errors"]),
        "golden_scope_evaluator_ids": list(scope_ids),
        "golden_scope_explicit": scope_present,
        "golden_scope_exact": bool(scope_valid and set(scope_ids) == set(covered_ids)),
        "uncovered_evaluator_ids": list(sorted(active_set - set(scope_ids))),
        "high_confidence_ready": bool(
            scope_ids
            and scope_valid
            and set(scope_ids).issubset(active_set)
            and not machine_observation_ids
            and not observed.get("invalid_golden_source_ids")
            and not observed.get("machine_observation_evaluator_ids")
            and observed.get("valid") is True
            and observed.get("low_tier_golden_ready") is True
            and not errors
        ),
        "safe_downgrade": bool(
            not scope_ids
            and observed.get("low_tier_golden_ready") is True
            and not errors
        ),
        "safe_downgrade_reason": (
            "no_genuine_high_scope" if not scope_ids else ""
        ),
        "errors": [item["code"] for item in errors],
    }
    return evidence, errors


def audit_unresolved_effect_schema_compatibility(
    *, hermes_home: Path, control_db_path: Path | None = None
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    db_path = control_db_path or (
        hermes_home
        / "runtime"
        / "pnc_agent"
        / "feishu_issue_kafka_rca"
        / "control.sqlite3"
    )
    evidence: dict[str, Any] = {
        "control_db_path": str(db_path),
        "unresolved_effect_count": 0,
        "incompatible_effect_count": 0,
        "schema_counts": {},
        "incompatible_effect_keys": [],
    }
    errors: list[dict[str, Any]] = []
    try:
        uri = f"{db_path.resolve().as_uri()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=5)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA query_only = ON")
            rows = conn.execute(
                """
                SELECT effect_key, outcome, status, payload_json
                  FROM rca_delivery_effects
                 WHERE status IN ('pending', 'claimed', 'retry_wait', 'uncertain')
                 ORDER BY effect_key
                """
            ).fetchall()
        finally:
            conn.close()
    except (OSError, sqlite3.Error) as exc:
        evidence["read_error"] = type(exc).__name__
        errors.append(_error("pnc_release_control_db_effect_preflight_unavailable"))
        return evidence, errors

    incompatible: list[str] = []
    schema_counts: dict[str, int] = {}
    for row in rows:
        try:
            payload = json.loads(str(row["payload_json"] or ""))
        except (TypeError, json.JSONDecodeError):
            payload = None
        schema_version = (
            str(payload.get("schema_version") or "")
            if isinstance(payload, Mapping)
            else "<invalid-json>"
        )
        schema_counts[schema_version] = schema_counts.get(schema_version, 0) + 1
        outcome = str(row["outcome"] or "")
        accepted = (
            schema_version
            in {DELIVERY_EFFECT_SCHEMA_VERSION, ADJUDICATION_EFFECT_SCHEMA_VERSION}
            if outcome == "success"
            else schema_version
            in {
                TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_V1,
                TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION,
                TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION,
            }
        )
        if not accepted:
            incompatible.append(str(row["effect_key"] or ""))
    evidence.update(
        unresolved_effect_count=len(rows),
        incompatible_effect_count=len(incompatible),
        schema_counts=dict(sorted(schema_counts.items())),
        incompatible_effect_keys=incompatible[:20],
    )
    if incompatible:
        errors.append(
            _error(
                "pnc_release_unresolved_effect_schema_incompatible",
                count=len(incompatible),
            )
        )
    return evidence, errors


def audit_delivery_store_schema(
    *, hermes_home: Path, control_db_path: Path | None = None
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    db_path = (
        (
            control_db_path
            or hermes_home
            / "runtime"
            / "pnc_agent"
            / "feishu_issue_kafka_rca"
            / "control.sqlite3"
        )
        .expanduser()
        .absolute()
    )
    evidence: dict[str, Any] = {
        "control_db_path": str(db_path),
        "read_mode": "ro+query_only",
        "expected_control_schema_version": CONTROL_STORE_SCHEMA_VERSION,
        "observed_control_schema_version": "",
        "expected_schema_version": COMBINED_TARGET_SCHEMA_VERSION,
        "observed_schema_version": "",
        "schema_valid": False,
        "errors": [],
    }
    errors: list[dict[str, Any]] = []
    conn: sqlite3.Connection | None = None
    try:
        before = db_path.lstat()
        if (
            stat.S_ISLNK(before.st_mode)
            or not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_size <= 0
        ):
            raise OSError("delivery store path is not a single regular file")
        uri = f"{db_path.as_uri()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=5, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only = ON")
        conn.execute("BEGIN")
        control_marker = conn.execute(
            "SELECT value FROM control_meta WHERE key = 'schema_version'"
        ).fetchone()
        evidence["observed_control_schema_version"] = (
            str(control_marker["value"] or "") if control_marker is not None else ""
        )
        marker = conn.execute(
            "SELECT value FROM rca_delivery_meta WHERE key = 'schema_version'"
        ).fetchone()
        evidence["observed_schema_version"] = (
            str(marker["value"] or "") if marker is not None else ""
        )
        validation = validate_combined_target_schema(conn)
        # The production control DB is one shared SQLite file.  Validating only
        # delivery v9 can miss a stale control schema or W6 cross-table guards
        # installed in the wrong migration order.
        RcaControlStore(db_path, require_current=True)
        RcaDeliveryStore(db_path, require_current=True)
        conn.rollback()
        after = db_path.lstat()
        if (
            before.st_dev,
            before.st_ino,
            before.st_mode,
            before.st_nlink,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_mode,
            after.st_nlink,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ):
            raise OSError("delivery store changed during schema audit")
    except (OSError, RuntimeError, sqlite3.Error, QuarantineMigrationError) as exc:
        if conn is not None and conn.in_transaction:
            conn.rollback()
        reason = (
            exc.code
            if isinstance(exc, QuarantineMigrationError)
            else str(exc) or type(exc).__name__
        )
        evidence["errors"] = [reason]
        errors.append(
            _error("pnc_release_delivery_store_schema_not_current", reason=reason)
        )
        return evidence, errors
    finally:
        if conn is not None:
            conn.close()
    evidence.update(validation)
    evidence["schema_valid"] = True
    return evidence, errors


def run_gate(*, home: Path, hermes_home: Path) -> dict[str, Any]:
    persisted, persisted_errors = audit_persisted_definitions(
        home=home, hermes_home=hermes_home
    )
    loaded, loaded_errors = audit_loaded_definitions(hermes_home=hermes_home)
    resolved, resolution_errors = _resolve_targets(hermes_home=hermes_home)
    residents, resident_errors = audit_residents(loaded=loaded, resolved=resolved)
    wrappers, wrapper_errors = audit_wrappers(home)
    retired_entrypoints, retired_entrypoint_errors = audit_retired_stable_entrypoints(
        home
    )
    runtime = next(iter(resolved.values()), {})
    stable_entrypoints: list[dict[str, Any]] = []
    stable_entrypoint_errors: list[dict[str, Any]] = []
    if runtime.get("runtime_root"):
        stable_entrypoints, stable_entrypoint_errors = (
            audit_versioned_stable_entrypoints(
                home=home,
                hermes_home=hermes_home,
                runtime_root=Path(runtime["runtime_root"]),
            )
        )
    active_evaluator_inventory, active_evaluator_ids, active_evaluator_errors = (
        audit_active_evaluator_inventory(hermes_home=hermes_home)
    )
    golden_registry, golden_errors = audit_release_golden_registry(
        hermes_home=hermes_home,
        required_evaluator_ids=active_evaluator_ids,
    )
    stable_targets, stable_target_errors = audit_stable_targets(hermes_home=hermes_home)
    delivery_store_schema, delivery_store_schema_errors = audit_delivery_store_schema(
        hermes_home=hermes_home
    )
    effect_schema_preflight, effect_schema_errors = (
        audit_unresolved_effect_schema_compatibility(hermes_home=hermes_home)
    )
    errors = [
        *persisted_errors,
        *loaded_errors,
        *resolution_errors,
        *resident_errors,
        *wrapper_errors,
        *retired_entrypoint_errors,
        *stable_entrypoint_errors,
        *active_evaluator_errors,
        *golden_errors,
        *stable_target_errors,
        *delivery_store_schema_errors,
        *effect_schema_errors,
    ]
    return {
        "schema_version": "pnc_rca_release_freshness_gate_v1",
        "ok": not errors,
        "manifest_path": str(hermes_home / "runtime" / "LIVE_MANIFEST.json"),
        "active_runtime_commit": runtime.get("runtime_commit", ""),
        "active_runtime_root": runtime.get("runtime_root", ""),
        "persisted_definitions": persisted,
        "loaded_definitions": loaded,
        "resolved_targets": [resolved[label] for label in sorted(resolved)],
        "residents": residents,
        "wrappers": wrappers,
        "retired_entrypoints": retired_entrypoints,
        "stable_entrypoints": stable_entrypoints,
        "active_evaluator_inventory": active_evaluator_inventory,
        "golden_registry": golden_registry,
        "stable_targets": stable_targets,
        "delivery_store_schema": delivery_store_schema,
        "effect_schema_preflight": effect_schema_preflight,
        "errors": errors,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Require every PNC definition and resident to use the active release"
    )
    parser.add_argument("--json", action="store_true")
    parser.add_argument(
        "--materialize-evaluator-inventory",
        type=Path,
        metavar="PIPELINE_SOURCE_ROOT",
        help="print an AST-derived evaluator inventory binding for a staged pipeline",
    )
    parser.add_argument("--pipeline-commit")
    parser.add_argument("--pipeline-tree")
    args = parser.parse_args()
    if args.materialize_evaluator_inventory is not None:
        if not args.pipeline_commit or not args.pipeline_tree:
            parser.error(
                "--materialize-evaluator-inventory requires --pipeline-commit and "
                "--pipeline-tree"
            )
        try:
            binding = materialize_active_evaluator_inventory_binding(
                pipeline_source_root=args.materialize_evaluator_inventory,
                pipeline_commit=args.pipeline_commit,
                pipeline_tree=args.pipeline_tree,
            )
        except ValueError as exc:
            print(json.dumps({"ok": False, "error": str(exc)}))
            return 2
        print(json.dumps(binding, ensure_ascii=True, indent=2, sort_keys=True))
        return 0
    home = Path.home().absolute()
    hermes_home = Path(os.environ.get("HERMES_HOME") or home / ".hermes").absolute()
    result = run_gate(home=home, hermes_home=hermes_home)
    if args.json:
        print(json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True))
    elif result["ok"]:
        print(
            "PNC release freshness OK: "
            f"{result['active_runtime_commit']} ({len(result['residents'])} residents)"
        )
    else:
        for item in result["errors"]:
            print(json.dumps(item, ensure_ascii=True, sort_keys=True), file=sys.stderr)
    return 0 if result["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
