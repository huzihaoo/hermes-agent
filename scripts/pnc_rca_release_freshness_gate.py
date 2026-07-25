#!/usr/bin/env python3
"""Fail a PNC host release when definitions or residents are not active-bound."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import plistlib
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
    LiveExecError,
    resolve_active_runtime,
)


WATCHDOG_LABEL = "local.pnc.watcher-staleness-watchdog"
EXPECTED_LAUNCHD_LABELS = frozenset((*PNC_PYTHON_LAUNCHD_LABELS, WATCHDOG_LABEL))
FORBIDDEN_RUNTIME_MARKERS = (
    "/runtime/releases/",
    "/runtime/venvs/",
    "/runtime/hermes-live",
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


def run_gate(*, home: Path, hermes_home: Path) -> dict[str, Any]:
    persisted, persisted_errors = audit_persisted_definitions(
        home=home, hermes_home=hermes_home
    )
    loaded, loaded_errors = audit_loaded_definitions(hermes_home=hermes_home)
    resolved, resolution_errors = _resolve_targets(hermes_home=hermes_home)
    residents, resident_errors = audit_residents(loaded=loaded, resolved=resolved)
    wrappers, wrapper_errors = audit_wrappers(home)
    errors = [
        *persisted_errors,
        *loaded_errors,
        *resolution_errors,
        *resident_errors,
        *wrapper_errors,
    ]
    runtime = next(iter(resolved.values()), {})
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
