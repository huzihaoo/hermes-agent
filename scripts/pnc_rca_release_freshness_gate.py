#!/usr/bin/env python3
"""Fail a PNC host release when definitions or residents are not active-bound."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import plistlib
import sqlite3
import stat
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable, Mapping

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
from gateway.pnc_rca_quality_oracle import release_golden_registry_status
from gateway.pnc_rca_conclusion_adjudication import (
    ADJUDICATION_EFFECT_SCHEMA_VERSION,
)
from gateway.pnc_rca_delivery_contract import (
    DELIVERY_EFFECT_SCHEMA_VERSION,
    TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION,
    TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_V1,
)


WATCHDOG_LABEL = "local.pnc.watcher-staleness-watchdog"
EXPECTED_LAUNCHD_LABELS = frozenset((*PNC_PYTHON_LAUNCHD_LABELS, WATCHDOG_LABEL))
FORBIDDEN_RUNTIME_MARKERS = (
    "/runtime/releases/",
    "/runtime/venvs/",
    "/runtime/hermes-live",
)
UNRESOLVED_EFFECT_STATUSES = frozenset(
    {"pending", "claimed", "retry_wait", "uncertain"}
)


def _error(code: str, **detail: Any) -> dict[str, Any]:
    return {"code": code, **detail}


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
        pairs.append(
            (
                "scripts/wrappers",
                home / "bin" / ".pnc-wrapper-source-missing",
                wrapper_source,
            )
        )
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
        evidence.append(
            {
                "label": label,
                "script": resolved["script"],
                "script_sha256": resolved["script_sha256"],
                "runtime_commit": resolved["runtime_commit"],
                "errors": [],
            }
        )
    return evidence, errors


def audit_release_golden_registry(
    *, hermes_home: Path, registry: Mapping[str, Any] | None = None
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    observed = dict(registry or release_golden_registry_status())
    errors: list[dict[str, Any]] = []
    if observed.get("valid") is not True:
        errors.append(_error("pnc_release_golden_registry_invalid"))
    if observed.get("low_tier_golden_ready") is not True:
        errors.append(_error("pnc_release_low_tier_golden_not_ready"))
    manifest_path = hermes_home / "runtime" / "LIVE_MANIFEST.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        manifest = {}
        errors.append(_error("pnc_release_manifest_unreadable_for_golden"))
    faces = manifest.get("face_git_bindings") if isinstance(manifest, Mapping) else {}
    pipeline = faces.get("g1q3_rca_pipeline") if isinstance(faces, Mapping) else {}
    active_commit = str(pipeline.get("commit") or "") if isinstance(pipeline, Mapping) else ""
    active_tree = str(pipeline.get("tree") or "") if isinstance(pipeline, Mapping) else ""
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


def run_gate(*, home: Path, hermes_home: Path) -> dict[str, Any]:
    persisted, persisted_errors = audit_persisted_definitions(
        home=home, hermes_home=hermes_home
    )
    loaded, loaded_errors = audit_loaded_definitions(hermes_home=hermes_home)
    resolved, resolution_errors = _resolve_targets(hermes_home=hermes_home)
    residents, resident_errors = audit_residents(loaded=loaded, resolved=resolved)
    wrappers, wrapper_errors = audit_wrappers(home)
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
    golden_registry, golden_errors = audit_release_golden_registry(
        hermes_home=hermes_home
    )
    stable_targets, stable_target_errors = audit_stable_targets(
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
        *stable_entrypoint_errors,
        *golden_errors,
        *stable_target_errors,
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
        "stable_entrypoints": stable_entrypoints,
        "golden_registry": golden_registry,
        "stable_targets": stable_targets,
        "effect_schema_preflight": effect_schema_preflight,
        "errors": errors,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Require every PNC definition and resident to use the active release"
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
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
