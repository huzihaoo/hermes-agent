#!/usr/bin/env python3
"""Read-only VM task status collector for Hermes Dashboard bridge.

The collector deliberately uses only bounded ssh-mini-agent read verbs. It does
not submit work, execute remote shells, edit files, or touch live services.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import fnmatch
import sys
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

_TASK_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_VM_TMP_PREFIX = "/mnt/tmp/"
_CIFS_TMP_PREFIX = "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/"
_PRODUCTION_SHARED_STATE_ROOT = "/home/mini/.hermes/shared-state"
_DEFAULT_SSH_MINI_AGENT = str(Path.home() / ".local" / "bin" / "ssh-mini-agent")
_ALLOWED_VERBS = {"read_file", "list_files", "head", "tail"}
_FILE_SUFFIX_RE = re.compile(r"\.[A-Za-z0-9]{1,12}$")


def validate_task_id(task_id: str) -> str:
    if not _TASK_ID_RE.fullmatch(task_id or ""):
        raise ValueError(f"invalid task_id: {task_id!r}")
    return task_id


def _normalize_shared_state_root(path: str | None) -> str:
    raw = str(path or _PRODUCTION_SHARED_STATE_ROOT).strip().rstrip("/")
    pure = PurePosixPath(raw)
    if not pure.is_absolute() or ".." in pure.parts:
        raise ValueError(f"invalid shared_state_root: {raw!r}")
    if raw != _PRODUCTION_SHARED_STATE_ROOT and not raw.startswith(_VM_TMP_PREFIX):
        raise ValueError(
            "non-production shared_state_root must stay under /mnt/tmp/: " + raw
        )
    if raw.startswith(_VM_TMP_PREFIX) and len(pure.parts) < 4:
        raise ValueError("isolated shared_state_root requires a task namespace")
    return raw


def _normalize_vm_tmp_root(path: str | None, task_id: str) -> str:
    validate_task_id(task_id)
    raw = str(path or "").strip()
    if not raw:
        return f"{_VM_TMP_PREFIX}{task_id}/"
    pure = PurePosixPath(raw)
    if not raw.startswith(_VM_TMP_PREFIX) or ".." in pure.parts or len(pure.parts) < 4:
        raise ValueError(f"artifact_root must stay under {_VM_TMP_PREFIX}: {raw}")
    return raw.rstrip("/") + "/"


def vm_tmp_root(task_id: str) -> str:
    return _normalize_vm_tmp_root(None, task_id)


def _normalize_cifs_tmp_root(path: str | None, task_id: str) -> str:
    validate_task_id(task_id)
    raw = str(path or "").strip()
    if not raw:
        return f"{_CIFS_TMP_PREFIX}{task_id}/"
    relative = raw[len(_CIFS_TMP_PREFIX):] if raw.startswith(_CIFS_TMP_PREFIX) else ""
    if not relative or ".." in PurePosixPath(relative).parts:
        raise ValueError(f"artifact_cifs_root must stay under {_CIFS_TMP_PREFIX}: {raw}")
    return raw.rstrip("/") + "/"


def _isolated_vm_namespace(shared_state_root: str) -> str | None:
    if shared_state_root == _PRODUCTION_SHARED_STATE_ROOT:
        return None
    parts = PurePosixPath(shared_state_root).parts
    return f"{_VM_TMP_PREFIX}{parts[3]}/"


def _require_isolated_artifact_scope(
    shared_state_root: str,
    artifact_root: str,
    artifact_cifs_root: str,
) -> None:
    namespace = _isolated_vm_namespace(shared_state_root)
    if namespace is None:
        return
    cifs_namespace = _CIFS_TMP_PREFIX + namespace[len(_VM_TMP_PREFIX):]
    if not artifact_root.startswith(namespace):
        raise ValueError(
            f"isolated artifact_root must stay under {namespace}: {artifact_root}"
        )
    if not artifact_cifs_root.startswith(cifs_namespace):
        raise ValueError(
            "isolated artifact_cifs_root must stay under "
            f"{cifs_namespace}: {artifact_cifs_root}"
        )


def cifs_tmp_root(task_id: str) -> str:
    return _normalize_cifs_tmp_root(None, task_id)


def vm_to_cifs(path: str, task_id: str, *, artifact_root: str | None = None, artifact_cifs_root: str | None = None) -> str:
    root = _normalize_vm_tmp_root(artifact_root, task_id)
    if not path.startswith(root):
        raise ValueError(f"path outside task artifact root: {path}")
    return _normalize_cifs_tmp_root(artifact_cifs_root, task_id) + path[len(root):]


def _agent_path(explicit: str | None = None) -> str:
    if explicit:
        return explicit
    return shutil.which("ssh-mini-agent") or _DEFAULT_SSH_MINI_AGENT


def _run_agent(agent: str, verb: str, path: str, *, timeout: int = 15) -> subprocess.CompletedProcess[str]:
    if verb not in _ALLOWED_VERBS:
        raise ValueError(f"disallowed ssh-mini-agent verb: {verb}")
    if ".." in PurePosixPath(path).parts:
        raise ValueError(f"path traversal is not allowed: {path}")
    return subprocess.run(
        [agent, verb, path],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
    )


def _read_json(agent: str, path: str) -> tuple[dict[str, Any], str | None]:
    result = _run_agent(agent, "read_file", path)
    if result.returncode != 0:
        return {}, result.stderr.strip() or f"read_file failed rc={result.returncode}"
    try:
        parsed = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        return {}, f"json parse failed for {path}: {exc.__class__.__name__}"
    if not isinstance(parsed, dict):
        return {}, f"json root is not object for {path}"
    return parsed, None


def _read_text(agent: str, path: str) -> tuple[str, str | None]:
    result = _run_agent(agent, "read_file", path)
    if result.returncode != 0:
        return "", result.stderr.strip() or f"read_file failed rc={result.returncode}"
    lines: list[str] = []
    for line in result.stdout.splitlines():
        # ssh-mini-agent variants may return either raw text or numbered
        # ``N|content`` lines.  Accept both so this collector stays read-only
        # and resilient across wrapper versions.
        prefix, sep, rest = line.partition("|")
        if sep and prefix.isdigit():
            lines.append(rest)
        else:
            lines.append(line)
    return "\n".join(lines), None


def _read_status(
    agent: str,
    task_id: str,
    shared_state_root: str = _PRODUCTION_SHARED_STATE_ROOT,
) -> tuple[dict[str, Any], list[str]]:
    root = _normalize_shared_state_root(shared_state_root)
    status_path = f"{root}/tasks/{task_id}/status.json"
    status, error = _read_json(agent, status_path)
    if status:
        return status, []

    errors: list[str] = []
    status_md_path = f"{root}/tasks/{task_id}/status.md"
    text, text_error = _read_text(agent, status_md_path)
    if text_error:
        if error:
            errors.append(error)
        errors.append(text_error)
        return {}, errors
    parsed: dict[str, Any] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith("- "):
            line = line[2:].strip()
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()
        if key in {"state", "status", "summary", "updated_at", "heartbeat_at", "lease_until", "run_id", "agent_host"}:
            parsed[key] = value
    if parsed:
        return parsed, []
    if error:
        errors.append(error)
    return parsed, errors


def _extract_vm_tmp_artifacts(text: str) -> list[str]:
    paths: list[str] = []
    for match in re.findall(r"/mnt/tmp/[^\s\"')`]+", text or ""):
        path = match.rstrip(".,;")
        if path and path not in paths:
            paths.append(path)
    return paths


def _looks_like_file_path(path: str) -> bool:
    name = PurePosixPath(path).name
    return bool(name and _FILE_SUFFIX_RE.search(name))


def _candidate_artifact_root_from_path(path: str) -> str | None:
    parts = PurePosixPath(path).parts
    if len(parts) < 4 or parts[1] != "mnt" or parts[2] != "tmp":
        return None
    root = f"/mnt/tmp/{parts[3]}/"
    # A path like /mnt/tmp/foo.json is a file candidate, not an artifact root.
    # Recover the known worker typo <artifact_dir>_verify_report_runtime.json
    # by deriving the intended directory, but never treating the JSON file itself
    # as a directory.
    if len(parts) == 4 and _looks_like_file_path(path):
        name = parts[3]
        suffix = "_verify_report_runtime.json"
        if name.endswith(suffix):
            return f"/mnt/tmp/{name[:-len(suffix)]}/"
        return None
    return root


def _artifact_root_from_paths(paths: list[str], task_id: str) -> str:
    for path in paths:
        root = _candidate_artifact_root_from_path(path)
        if root:
            return root
    return vm_tmp_root(task_id)


def _list_vm_tmp_dirs(agent: str) -> tuple[list[str], str | None]:
    result = subprocess.run(
        [agent, "list_files", _VM_TMP_PREFIX, "--max", "2000"],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=20,
    )
    if result.returncode != 0:
        return [], result.stderr.strip() or f"list_files failed rc={result.returncode}"
    dirs: list[str] = []
    for raw in result.stdout.splitlines():
        name = raw.strip().rstrip("/")
        if not name:
            continue
        if name.startswith(_VM_TMP_PREFIX):
            candidate = name.rstrip("/") + "/"
        else:
            candidate = _VM_TMP_PREFIX + name.lstrip("/").rstrip("/") + "/"
        if ".." in PurePosixPath(candidate).parts or _looks_like_file_path(candidate.rstrip("/")):
            continue
        dirs.append(candidate)
    return dirs, None


def _work_item_id_from_meta_or_task(task_id: str, meta: dict[str, Any]) -> str:
    for container in (meta, meta.get("work_item"), meta.get("source_refs"), meta.get("case")):
        if not isinstance(container, dict):
            continue
        for key in ("work_item_id", "issue_id", "id"):
            value = container.get(key)
            if isinstance(value, (str, int)) and str(value).strip().isdigit():
                return str(value).strip()
    match = re.search(r"g1q3[-_]rca[-_]issue[-_]intake[-_](\d{6,})", task_id, re.I)
    if match:
        return match.group(1)
    # Prefer issue-like ids after "intake" over leading date stamps.
    match = re.search(r"(?:issue[-_]intake|work[-_]item|issue)[-_](\d{6,})", task_id, re.I)
    if match:
        return match.group(1)
    match = re.search(r"(\d{6,})", task_id)
    return match.group(1) if match else ""


def _work_item_id_from_request(request: dict[str, Any]) -> str:
    """Return Feishu issue/work item id from an RCA execution request."""
    for container in (request.get("work_item"), request.get("source_refs"), request.get("case")):
        if not isinstance(container, dict):
            continue
        for key in ("work_item_id", "issue_id", "id"):
            value = container.get(key)
            if isinstance(value, (str, int)) and str(value).strip().isdigit():
                return str(value).strip()
    return ""


def _pipeline_result_is_report_ready(payload: dict[str, Any]) -> bool:
    status = str(payload.get("status") or "").strip()
    terminal = str(payload.get("rca_terminal_state") or payload.get("terminal_state") or "").strip()
    return terminal == "report_ready" or status in {"report_generated_need_review", "report_ready"}


def _discover_latest_report_ready_artifact_root(
    agent: str,
    *,
    task_id: str,
    current_root: str,
    meta: dict[str, Any],
) -> tuple[str | None, str | None]:
    """Find a newer report-ready /mnt/tmp RCA run for the same work_item_id.

    Governance datapipe can finish under a fresh trigger slug while the Feishu
    one-card sidecar remains bound to the original intake slug.  If the
    follow-up VM task is not created (permissions, queue, transient submit
    failure), the old card otherwise stays stuck at need_download even though a
    valid report exists.  This resolver is conservative: it only accepts G1Q3
    tmp dirs whose request carries the same numeric work_item_id and whose
    pipeline_result is report_ready.
    """
    work_item_id = _work_item_id_from_meta_or_task(task_id, meta)
    if not work_item_id:
        return None, None
    dirs, error = _list_vm_tmp_dirs(agent)
    if error:
        return None, error
    candidates = [
        d for d in dirs
        if work_item_id in PurePosixPath(d).name
        and PurePosixPath(d).name.startswith("g1q3_rca_")
    ]
    matches: list[str] = []
    for root in sorted(candidates):
        root = root.rstrip("/") + "/"
        pipeline, pipeline_error = _read_json(agent, root + "pipeline_result.json")
        if pipeline_error or not pipeline or not _pipeline_result_is_report_ready(pipeline):
            continue
        request, request_error = _read_json(agent, root + "rca_execution_request.json")
        if request_error or not request:
            continue
        if _work_item_id_from_request(request) != work_item_id:
            continue
        matches.append(root)
    if not matches:
        return None, None
    latest = sorted(matches)[-1]
    current = str(current_root or "").rstrip("/") + "/" if current_root else ""
    if latest == current:
        return None, None
    return latest, None


def _discover_hashed_artifact_root(agent: str, task_id: str, meta: dict[str, Any]) -> tuple[str | None, str | None]:
    work_item_id = _work_item_id_from_meta_or_task(task_id, meta)
    if not work_item_id:
        return None, None
    dirs, error = _list_vm_tmp_dirs(agent)
    if error:
        return None, error
    patterns = [f"g1q3_rca*{work_item_id}*", f"*{work_item_id}*"]
    matches = [d for d in dirs if any(fnmatch.fnmatch(PurePosixPath(d).name, pat) for pat in patterns)]
    if not matches:
        return None, None
    # ssh-mini-agent output is path-sorted, not mtime-sorted. Lexicographic last
    # is deterministic and prefers hash-suffixed reruns over the base prefix.
    return sorted(matches)[-1], None


def _list_artifacts(
    agent: str,
    task_id: str,
    *,
    artifact_root: str | None = None,
    artifact_cifs_root: str | None = None,
    max_artifacts: int = 50,
) -> tuple[list[str], list[str]]:
    root = _normalize_vm_tmp_root(artifact_root, task_id)
    if _looks_like_file_path(root.rstrip("/")):
        return [], [f"artifact root is not a directory: {root}"]
    result = _run_agent(agent, "list_files", root)
    if result.returncode != 0:
        message = result.stderr.strip() or f"list_files failed rc={result.returncode}"
        if "directory not found" in message.lower() or "not a directory" in message.lower():
            return [], [f"artifact root unavailable: {root} ({message})"]
        return [], [message]
    artifacts: list[str] = []
    errors: list[str] = []
    for raw in result.stdout.splitlines():
        name = raw.strip()
        if not name or name.endswith("/"):
            continue
        if ".." in PurePosixPath(name).parts:
            errors.append(f"skipped unsafe artifact path: {name}")
            continue
        # ssh-mini-agent list_files output may be either bare names or absolute paths.
        if name.startswith("/"):
            vm_path = name
        else:
            vm_path = root + name.lstrip("/")
        if not vm_path.startswith(root):
            errors.append(f"skipped outside artifact root: {name}")
            continue
        artifacts.extend([f"VM: {vm_path}", f"CIFS: {vm_to_cifs(vm_path, task_id, artifact_root=root, artifact_cifs_root=artifact_cifs_root)}"])
        if len(artifacts) >= max_artifacts * 2:
            break
    return artifacts, errors


def collect_vm_task_status(
    task_id: str,
    *,
    ssh_mini_agent: str | None = None,
    include_artifacts: bool = True,
    shared_state_root: str = _PRODUCTION_SHARED_STATE_ROOT,
    allow_global_artifact_discovery: bool | None = None,
) -> dict[str, Any]:
    task_id = validate_task_id(task_id)
    agent = _agent_path(ssh_mini_agent)
    shared_root = _normalize_shared_state_root(shared_state_root)
    if allow_global_artifact_discovery is None:
        allow_global_artifact_discovery = shared_root == _PRODUCTION_SHARED_STATE_ROOT
    if shared_root != _PRODUCTION_SHARED_STATE_ROOT and allow_global_artifact_discovery:
        raise ValueError("isolated shared_state_root cannot enable global artifact discovery")
    errors: list[str] = []

    status_path = f"{shared_root}/tasks/{task_id}/status.json"
    status, status_errors = _read_status(agent, task_id, shared_root)
    errors.extend(status_errors)
    meta_path = f"{shared_root}/tasks/{task_id}/meta.json"
    meta, meta_error = _read_json(agent, meta_path)
    if meta_error:
        errors.append(meta_error)
    result_text, result_error = _read_text(agent, f"{shared_root}/tasks/{task_id}/result.md")
    result_artifact_paths = _extract_vm_tmp_artifacts(result_text)
    if result_error:
        # result.md is absent while a task is running; it is not a blocker for
        # current-progress collection.
        pass

    state_value = str(status.get("state") or status.get("status") or "unknown")
    summary = str(status.get("summary") or status.get("message") or "No VM status summary yet.")
    updated_at = status.get("updated_at") if isinstance(status.get("updated_at"), str) else None
    inferred_artifact_root = _artifact_root_from_paths(result_artifact_paths, task_id)
    discovered_root = None
    discovery_error = None
    if allow_global_artifact_discovery and not meta.get("artifact_root"):
        discovered_root, discovery_error = _discover_hashed_artifact_root(agent, task_id, meta)
        if discovery_error:
            errors.append(discovery_error)
    selected_artifact_root = str(meta.get("artifact_root") or discovered_root or inferred_artifact_root)
    isolated_namespace = _isolated_vm_namespace(shared_root)
    if not selected_artifact_root and isolated_namespace is not None:
        selected_artifact_root = f"{isolated_namespace}artifacts/{task_id}/"
    work_tmp_dir = _normalize_vm_tmp_root(selected_artifact_root, task_id)
    stitched_root = None
    stitched_error = None
    if allow_global_artifact_discovery:
        stitched_root, stitched_error = _discover_latest_report_ready_artifact_root(
            agent,
            task_id=task_id,
            current_root=work_tmp_dir,
            meta=meta,
        )
    if stitched_error:
        errors.append(stitched_error)
    if stitched_root:
        work_tmp_dir = _normalize_vm_tmp_root(stitched_root, task_id)
    inferred_cifs_root = _CIFS_TMP_PREFIX + work_tmp_dir[len(_VM_TMP_PREFIX):]
    user_visible_path = _normalize_cifs_tmp_root(str(meta.get("artifact_cifs_root") or inferred_cifs_root), task_id)
    if stitched_root:
        user_visible_path = _normalize_cifs_tmp_root(inferred_cifs_root, task_id)
    _require_isolated_artifact_scope(shared_root, work_tmp_dir, user_visible_path)

    delivery_contract: dict[str, Any] = {}

    artifacts: list[str] = []
    for vm_path in result_artifact_paths:
        if vm_path.startswith(work_tmp_dir):
            artifacts.extend([f"VM: {vm_path}", f"CIFS: {vm_to_cifs(vm_path, task_id, artifact_root=work_tmp_dir, artifact_cifs_root=user_visible_path)}"])
    if include_artifacts:
        listed_artifacts, artifact_errors = _list_artifacts(
            agent,
            task_id,
            artifact_root=work_tmp_dir,
            artifact_cifs_root=user_visible_path,
        )
        for artifact in listed_artifacts:
            if artifact not in artifacts:
                artifacts.append(artifact)
        # Missing/non-directory artifact roots are diagnostic warnings; do not
        # surface them as collector blockers that pollute task-state.
        for artifact_error in artifact_errors:
            if str(artifact_error).startswith("artifact root unavailable:") or str(artifact_error).startswith("artifact root is not a directory:"):
                continue
            errors.append(artifact_error)

    contract_vm_path = ""
    expected_contract_vm_path = work_tmp_dir + "delivery_contract.json"
    for artifact in artifacts:
        if artifact.startswith("VM: ") and artifact.split("VM: ", 1)[1].strip() == expected_contract_vm_path:
            contract_vm_path = expected_contract_vm_path
            break
    if contract_vm_path:
        candidate_contract, contract_error = _read_json(agent, contract_vm_path)
        if candidate_contract and str(candidate_contract.get("schema_version") or "").strip() in {
            "g1q3_delivery_contract_v1",
            "g1q3_delivery_contract_v2",
        }:
            delivery_contract = candidate_contract
        elif contract_error and "read_file failed" not in contract_error.lower() and "not found" not in contract_error.lower():
            errors.append(contract_error)

    payload: dict[str, Any] = {
        "success": True,
        "read_only": True,
        "collector_version": "v1",
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "task_id": task_id,
        "state": {
            "value": state_value,
            "terminal": state_value in {"completed", "failed", "cancelled"},
            "summary": summary,
            "updated_at": updated_at,
        },
        "paths": {
            "shared_state_root": shared_root,
            "status_json": status_path,
            "artifact_root_vm": work_tmp_dir,
            "artifact_root_cifs": user_visible_path,
        },
        "artifacts": artifacts,
        "delivery_contract": delivery_contract,
        "vm_bridge": {
            "summary": summary,
            "state": state_value,
            "vm_task_id": task_id,
            "work_tmp_dir": work_tmp_dir,
            "user_visible_path": user_visible_path,
            "sources": {
                "status_json": bool(status),
                "artifact_scan": include_artifacts,
                "artifact_root_discovered": bool(discovered_root),
                "artifact_root_stitched_by_work_item": bool(stitched_root),
                "meta_json": bool(meta),
                "result_md": bool(result_text),
                "delivery_contract": bool(delivery_contract),
                "global_artifact_discovery": bool(allow_global_artifact_discovery),
            },
        },
        "errors": errors,
    }
    return payload


def _phase_for_state(state: str) -> str:
    if state in {"completed"}:
        return "completed"
    if state in {"failed"}:
        return "failed"
    if state in {"cancelled"}:
        return "cancelled"
    if state in {"blocked"}:
        return "blocked"
    return "vm_running"


def write_collected_status_to_sidecar(task_id: str, payload: dict[str, Any]):
    from scripts.vm_task_state_bridge import write_task_state
    from scripts.vm_task_state_bridge import _atomic_write_json, _load_existing, sidecar_path

    raw_state = payload.get("state")
    raw_bridge = payload.get("vm_bridge")
    state_obj: dict[str, Any] = dict(raw_state) if isinstance(raw_state, dict) else {}
    bridge_obj: dict[str, Any] = dict(raw_bridge) if isinstance(raw_bridge, dict) else {}
    state = str(state_obj.get("value") or bridge_obj.get("state") or "unknown")
    summary = str(state_obj.get("summary") or bridge_obj.get("summary") or "VM status collected")
    raw_artifacts = payload.get("artifacts")
    artifacts: list[Any] = raw_artifacts if isinstance(raw_artifacts, list) else []
    path = write_task_state(
        task_id,
        phase=_phase_for_state(state),
        event=summary,
        vm_summary=str(bridge_obj.get("summary") or summary),
        vm_state=state,
        work_tmp_dir=str(bridge_obj.get("work_tmp_dir") or vm_tmp_root(task_id)),
        user_visible_path=str(bridge_obj.get("user_visible_path") or cifs_tmp_root(task_id)),
    )
    for artifact in artifacts:
        path = write_task_state(task_id, artifact=str(artifact))
    raw_errors = payload.get("errors")
    errors: list[Any] = raw_errors if isinstance(raw_errors, list) else []
    if not errors:
        sidecar = sidecar_path(task_id)
        body = _load_existing(sidecar)
        blockers = [item for item in body.get("blockers", []) if not str(item).startswith("collector: ")] if isinstance(body.get("blockers"), list) else []
        if blockers != body.get("blockers"):
            body["blockers"] = blockers
            _atomic_write_json(sidecar, body)
    for error in errors:
        path = write_task_state(task_id, blocker=f"collector: {error}")
    return path


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--ssh-mini-agent")
    parser.add_argument("--shared-state-root", default=_PRODUCTION_SHARED_STATE_ROOT)
    parser.add_argument("--write-sidecar", action="store_true")
    parser.add_argument("--no-artifacts", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    payload = collect_vm_task_status(
        args.task_id,
        ssh_mini_agent=args.ssh_mini_agent,
        include_artifacts=not args.no_artifacts,
        shared_state_root=args.shared_state_root,
    )
    if args.write_sidecar:
        payload["sidecar_path"] = str(write_collected_status_to_sidecar(args.task_id, payload))
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
