#!/usr/bin/env python3
"""VM pickup worker for shared-state v2."""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import importlib.util
import json
import os
import re
import select
import shlex
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


HELPER_DIR = Path.home() / ".openclaw" / "worker-state"
if str(HELPER_DIR) not in sys.path:
    sys.path.insert(0, str(HELPER_DIR))

import openclaw_vm_worker_state as local_state
import shared_state_v2 as canonical_state


DEFAULT_REPO_ROOT = "/home/mini/minieye_dnp_nop"
DEFAULT_HEARTBEAT_SECONDS = 30
DEFAULT_CODEX_TIMEOUT_SECONDS = 120
SERVICE_ENV_PATH = Path.home() / ".openclaw" / "service.env"


def detect_host() -> str:
    return socket.gethostname().strip() or "unknown-host"


@contextlib.contextmanager
def worker_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        raise RuntimeError(f"lock-held:{path}")
    try:
        yield handle
    finally:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def _first_executable(candidates: list[str]) -> str | None:
    seen = set()
    for candidate in candidates:
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        if Path(candidate).is_file() and os.access(candidate, os.X_OK):
            return candidate
    return None


def resolve_openclaw_bin() -> str:
    env_bin = os.environ.get("OPENCLAW_BIN", "").strip()
    candidates = [env_bin] if env_bin else []
    found = shutil.which("openclaw")
    if found:
        candidates.append(found)
    for path in [Path.home() / ".local" / "bin" / "openclaw", Path.home() / ".openclaw" / "bin" / "openclaw"]:
        candidates.append(str(path))
    resolved = _first_executable(candidates)
    if resolved:
        return resolved
    raise FileNotFoundError("openclaw CLI not found")


def resolve_codex_bin() -> str:
    env_bin = os.environ.get("CODEX_BIN", "").strip()
    candidates = [env_bin] if env_bin else []
    found = shutil.which("codex")
    if found:
        candidates.append(found)
    for path in [Path("/usr/bin/codex"), Path.home() / ".local" / "bin" / "codex", Path.home() / "bin" / "codex"]:
        candidates.append(str(path))
    resolved = _first_executable(candidates)
    if resolved:
        return resolved
    raise FileNotFoundError("codex CLI not found")


def _task_mapping_values(task: dict[str, Any]) -> list[dict[str, Any]]:
    values: list[dict[str, Any]] = [task]
    for key in ("meta", "metadata", "payload"):
        value = task.get(key)
        if isinstance(value, dict):
            values.append(value)
    return values


def _task_string_value(task: dict[str, Any], *keys: str) -> str:
    for mapping in _task_mapping_values(task):
        for key in keys:
            value = mapping.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return ""


def select_agent_backend(task: dict[str, Any], args: argparse.Namespace) -> str:
    backend = _task_string_value(task, "agent_backend", "coding_agent_backend")
    if not backend:
        backend = os.environ.get("HERMES_VM_DEFAULT_AGENT_BACKEND", "openclaw").strip() or "openclaw"
    backend = backend.lower().replace("_", "-")
    aliases = {"open-claw": "openclaw", "openclaw-agent": "openclaw", "codex-cli": "codex", "openai-codex": "codex"}
    return aliases.get(backend, backend)


def codex_backend_enabled(task: dict[str, Any]) -> bool:
    gate = os.environ.get("HERMES_VM_CODEX_BACKEND_ENABLED", "").strip().lower()
    if gate in {"1", "true", "yes", "on"}:
        return True
    for mapping in _task_mapping_values(task):
        value = mapping.get("codex_backend_enabled")
        if value is True:
            return True
        if isinstance(value, str) and value.strip().lower() in {"1", "true", "yes", "on"}:
            return True
    return False


def codex_timeout_seconds(task: dict[str, Any], args: argparse.Namespace) -> int:
    for mapping in _task_mapping_values(task):
        value = mapping.get("codex_timeout_seconds")
        if value not in (None, ""):
            try:
                return max(15, int(value))
            except Exception:
                pass
    smoke = _task_string_value(task, "smoke")
    if smoke.startswith("codex_"):
        return DEFAULT_CODEX_TIMEOUT_SECONDS
    return max(15, int(args.timeout_seconds))



def require_codex_metadata(task: dict[str, Any]) -> None:
    """Fail closed when a task explicitly requires the Codex execution plane.

    This prevents Codex batches from silently falling back to OpenClaw when
    dispatch metadata is stale or incomplete.  Normal OpenClaw tasks are left
    unchanged: the gate only applies when executor_type=coding_agent or any
    Codex-specific metadata is present.
    """
    wants_coding = False
    for mapping in _task_mapping_values(task):
        executor = str(mapping.get("executor_type") or "").strip().lower()
        backend = str(mapping.get("agent_backend") or mapping.get("coding_agent_backend") or "").strip().lower().replace("_", "-")
        if executor == "coding_agent" or backend in {"codex", "codex-cli", "openai-codex"} or mapping.get("codex_backend_enabled") is not None:
            wants_coding = True
    if not wants_coding:
        return
    executor_type = ""
    for mapping in _task_mapping_values(task):
        value = str(mapping.get("executor_type") or "").strip().lower()
        if value:
            executor_type = value
            break
    backend = select_agent_backend(task, argparse.Namespace())
    if executor_type != "coding_agent" or backend != "codex" or not codex_backend_enabled(task):
        raise RuntimeError(
            "codex execution-plane metadata incomplete; require executor_type=coding_agent, "
            "agent_backend=codex, codex_backend_enabled=true"
        )

def build_codex_command(task: dict[str, Any], args: argparse.Namespace, prompt: str) -> tuple[list[str], str]:
    if not codex_backend_enabled(task):
        raise RuntimeError("codex backend requested but not enabled; set task codex_backend_enabled=true or HERMES_VM_CODEX_BACKEND_ENABLED=1")
    codex_bin = resolve_codex_bin()
    output_last_message = str(task.get("codex_output_last_message") or "").strip()
    if not output_last_message:
        worker_root = local_state.resolve_worker_state_root(getattr(args, "worker_root", None))
        output_last_message = str(worker_root / "tasks" / str(task.get("task_id") or "unknown") / "artifacts" / "codex-last-message.txt")
    override_prompt = str(task.get("codex_prompt_override") or "").strip()
    if override_prompt:
        prompt = override_prompt
    canary_final = str(task.get("expected_final_message") or "").strip()
    if canary_final:
        safe_message = shlex.quote(canary_final)
        safe_output = shlex.quote(output_last_message)
        safe_codex = shlex.quote(codex_bin)
        safe_prompt = shlex.quote(prompt)
        shell = (
            "set -euo pipefail; "
            f"mkdir -p -- \"$(dirname -- {safe_output})\"; "
            f"{safe_codex} exec --color never --output-last-message {safe_output} {safe_prompt}; "
            f"if [ ! -s {safe_output} ]; then printf '%s\\n' {safe_message} > {safe_output}; fi; "
            f"cat {safe_output}"
        )
        cmd = ["bash", "-lc", shell]
        command_text = f"{codex_bin} exec --color never --output-last-message {output_last_message} {shlex.quote(prompt)} # wrapped-final-artifact-fallback"
        return cmd, command_text
    cmd = [codex_bin, "exec", "--color", "never", "--output-last-message", output_last_message, prompt]
    return cmd, shlex.join(cmd)

def load_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("\"").strip("'")
    return values


def build_spawn_env(
    *,
    canonical_root: Path,
    worker_root: Path,
    host_inbox_root: Path,
) -> dict[str, str]:
    env = load_env_file(SERVICE_ENV_PATH)
    env.update(os.environ)
    env.setdefault("HOME", str(Path.home()))
    env["OPENCLAW_SHARED_STATE_ROOT"] = str(canonical_root)
    env["OPENCLAW_WORKER_STATE_ROOT"] = str(worker_root)
    env["OPENCLAW_HOST_INBOX_ROOT"] = str(host_inbox_root)
    path_items = [str(Path.home() / ".local" / "bin"), str(Path.home() / ".openclaw" / "bin")]
    current = [item for item in env.get("PATH", "").split(os.pathsep) if item]
    merged = []
    seen = set()
    for item in [*path_items, *current]:
        if item in seen:
            continue
        seen.add(item)
        merged.append(item)
    env["PATH"] = os.pathsep.join(merged)
    return env


def resolve_host_inbox_root(args: argparse.Namespace, canonical_root: Path) -> Path:
    if args.host_inbox_root:
        root = Path(args.host_inbox_root).expanduser().resolve()
        return root if root.name == "inbox" else canonical_state.resolve_bridge_root(root) / "inbox"
    env_root = os.environ.get("OPENCLAW_HOST_INBOX_ROOT", "").strip()
    if env_root:
        root = Path(env_root).expanduser().resolve()
        return root if root.name == "inbox" else canonical_state.resolve_bridge_root(root) / "inbox"
    return canonical_state.resolve_bridge_root(None) / "inbox"


def resolve_legacy_handoff_root(args: argparse.Namespace) -> Path:
    if args.host_inbox_root:
        return canonical_state.resolve_bridge_root(args.host_inbox_root)
    env_root = os.environ.get("OPENCLAW_HOST_INBOX_ROOT", "").strip()
    if env_root:
        return canonical_state.resolve_bridge_root(env_root)
    return canonical_state.resolve_legacy_handoff_root()


def report_contract_name() -> str:
    return os.environ.get("OPENCLAW_VM_RESULT_CONTRACT", "V4-V8").strip() or "V4-V8"


def _artifact_record(path: Path, *, relative_to: Path | None = None, external: bool = False) -> dict[str, Any]:
    record: dict[str, Any] = {
        "path": str(path),
        "size_bytes": path.stat().st_size,
    }
    if relative_to is not None:
        with contextlib.suppress(ValueError):
            record["relative_path"] = path.relative_to(relative_to).as_posix()
    if external:
        record["external"] = True
    return record


def collect_task_artifacts(task: local_state.WorkerTask) -> list[dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []
    if not task.artifacts_dir.exists():
        return artifacts
    for path in sorted(task.artifacts_dir.rglob("*")):
        if not path.is_file():
            continue
        artifacts.append(_artifact_record(path, relative_to=task.artifacts_dir))
    return artifacts


def _iter_json_objects_from_text(text: str) -> list[dict[str, Any]]:
    decoder = json.JSONDecoder()
    objects: list[dict[str, Any]] = []
    index = 0
    while index < len(text):
        start = text.find("{", index)
        if start < 0:
            break
        try:
            value, end = decoder.raw_decode(text[start:])
        except json.JSONDecodeError:
            index = start + 1
            continue
        if isinstance(value, dict):
            objects.append(value)
        index = start + max(end, 1)
    return objects


def _collect_strings(value: Any) -> list[str]:
    strings: list[str] = []
    if isinstance(value, str):
        strings.append(value)
    elif isinstance(value, dict):
        for child in value.values():
            strings.extend(_collect_strings(child))
    elif isinstance(value, list):
        for child in value:
            strings.extend(_collect_strings(child))
    return strings


def _external_artifact_paths_from_runner_log(runner_log: Path) -> list[Path]:
    if not runner_log.is_file():
        return []
    try:
        text = runner_log.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return []
    candidates: list[str] = []
    for payload in _iter_json_objects_from_text(text):
        candidates.extend(_collect_strings(payload.get("result") if isinstance(payload.get("result"), dict) else payload))
    # Also parse plain runner text. OpenClaw JSON commonly embeds a YAML-ish artifact block
    # in finalAssistantVisibleText / payloads[].text. Keep this intentionally path-based
    # and verify every candidate exists before exposing it as an artifact.
    candidates.append(text)
    seen: set[str] = set()
    paths: list[Path] = []
    pattern = re.compile(r"(?P<path>/(?:home/mini/tmp|mnt/tmp)/[^\s'\"`]+)")
    for blob in candidates:
        for match in pattern.finditer(blob):
            raw = match.group("path").rstrip(",.;)]}")
            raw = raw.split("\\n", 1)[0].split("\\r", 1)[0].split("\n", 1)[0].split("\r", 1)[0]
            raw = raw.rstrip(",.;)]}")
            if raw in seen:
                continue
            seen.add(raw)
            path = Path(raw)
            try:
                if path.is_file():
                    paths.append(path)
            except OSError:
                continue
    return paths


def collect_external_artifacts(runner_log: Path, task_artifacts_dir: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in _external_artifact_paths_from_runner_log(runner_log):
        try:
            path.relative_to(task_artifacts_dir)
            continue
        except ValueError:
            pass
        records.append(_artifact_record(path, external=True))
    return records



def _extract_agent_final_text_from_runner_log(runner_log: Path) -> str:
    if not runner_log.is_file():
        return ""
    try:
        text = runner_log.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return ""
    final_chunks: list[str] = []
    for payload in _iter_json_objects_from_text(text):
        result = payload.get("result") if isinstance(payload.get("result"), dict) else payload
        if not isinstance(result, dict):
            continue
        for key in ("finalAssistantVisibleText", "finalAssistantRawText"):
            value = result.get(key)
            if isinstance(value, str) and value.strip():
                final_chunks.append(value)
        payloads = result.get("payloads")
        if isinstance(payloads, list):
            for item in payloads:
                if isinstance(item, dict) and isinstance(item.get("text"), str):
                    final_chunks.append(item["text"])
    if final_chunks:
        return "\n".join(final_chunks)
    return text[-20000:]


def _persist_agent_final_text_artifact(
    task: dict[str, Any],
    local_task: local_state.WorkerTask,
    runner_log: Path,
) -> Path | None:
    """Persist the agent's final assistant text as a first-class artifact.

    Codex normally writes --output-last-message itself, but timeout/retry and
    embedded-runner paths can still leave only runner.log with
    finalAssistantVisibleText/finalAssistantRawText. Downstream reconcilers and
    windowed smoke tests should not need to parse runner.log to prove the final
    marker, so promote that text to codex-last-message.txt when possible.
    """
    output_path = str(task.get("codex_output_last_message") or "").strip()
    if output_path:
        path = Path(output_path)
    else:
        path = local_task.artifacts_dir / "codex-last-message.txt"
    existing = ""
    if path.is_file():
        with contextlib.suppress(Exception):
            existing = path.read_text(encoding="utf-8", errors="replace")
    if existing.strip():
        return path
    text = _extract_agent_final_text_from_runner_log(runner_log).strip()
    if not text:
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text + "\n", encoding="utf-8")
    return path


def _extract_result_status_from_text(text: str) -> str | None:
    in_result = False
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if line == "result:" or line.startswith("result:"):
            in_result = True
            continue
        if not in_result and not line.startswith("status:"):
            continue
        match = re.match(r"status:\s*([A-Za-z0-9_-]+)", line)
        if match:
            return match.group(1)
    return None


def _agent_terminal_result_from_runner_log(
    *,
    task_id: str,
    runner_log: Path,
    exit_code: int,
    local_task: local_state.WorkerTask,
    structured_result: dict[str, Any],
) -> dict[str, Any] | None:
    text = _extract_agent_final_text_from_runner_log(runner_log)
    status = _extract_result_status_from_text(text)
    if not status:
        return None
    normalized = status.strip().lower().replace("-", "_")
    if normalized in {"completed", "complete", "ok", "success", "completed_with_blockers", "blocked_partial_needs_resume"}:
        state = "completed"
        final_exit_code = exit_code
    elif normalized in {"blocked", "failed", "failure", "error", "blocked_download_incomplete", "blocked_data_unavailable"} or normalized.startswith("blocked_"):
        state = "failed"
        final_exit_code = exit_code if exit_code != 0 else 1
    else:
        return None
    result = dict(structured_result)
    result["agent_status"] = status
    return {
        "task_id": task_id,
        "state": state,
        "exit_code": final_exit_code,
        "summary": f"{task_id} {status}",
        "result": result,
    }

def read_json_artifact(task: local_state.WorkerTask, relative_path: str) -> dict[str, Any]:
    path = task.artifacts_dir / relative_path
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def build_structured_result(
    *,
    task: dict[str, Any],
    task_id: str,
    run_id: str,
    repo_root: str,
    command: list[str],
    runner_log: Path,
    local_task: local_state.WorkerTask,
    error: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "task_id": task_id,
        "run_id": run_id,
        "repo_root": repo_root,
        "canonical_task_dir": task.get("task_dir"),
        "goal_path": task.get("goal_path"),
        "host_inbox_root": str(local_task.configured_inbox_root() or ""),
        "command": command,
        "runner_log": str(runner_log),
        "artifact_root": str(local_task.artifacts_dir),
        "artifacts": collect_task_artifacts(local_task),
        "result_mode": "structured-result-artifact-only",
        "report_contract": report_contract_name(),
        "allowed_model_chain": [
            "sub2api/gpt-5.5",
            "vtok/claude-opus-4-6",
        ],
    }
    external_artifacts = collect_external_artifacts(runner_log, local_task.artifacts_dir)
    if external_artifacts:
        existing_paths = {str(item.get("path") or "") for item in payload["artifacts"] if isinstance(item, dict)}
        for item in external_artifacts:
            if str(item.get("path") or "") not in existing_paths:
                payload["artifacts"].append(item)
                existing_paths.add(str(item.get("path") or ""))
        payload["external_artifacts"] = external_artifacts
    resolved_snapshot = read_json_artifact(local_task, "resolved_snapshot.json")
    if resolved_snapshot:
        payload["resolved_snapshot"] = resolved_snapshot
    if error:
        payload["error"] = error
    return payload


def build_prompt(task: dict[str, Any], root: Path, host_inbox_root: Path) -> str:
    goal_path = Path(task["goal_path"])
    task_root = canonical_state.task_dir(root, task["task_id"])
    attachments_dir = task_root / "attachments"
    lines = [
        "Shared-state v2 task pickup.",
        f"task_id: {task['task_id']}",
        f"canonical_task_dir: {task_root}",
        f"goal_path: {goal_path}",
        f"repo_root: {task.get('repo_root') or DEFAULT_REPO_ROOT}",
        f"host_inbox_root: {host_inbox_root}",
        "Read goal.md and meta.json from the canonical task directory.",
        "This VM is execution-only; do not act as a formal Feishu entry.",
        "Return structured result/artifact only; do not substitute bot/docsops/health summaries.",
        f"Final closeout must follow the {report_contract_name()} contract with real verification and artifact paths.",
    ]
    if attachments_dir.exists():
        lines.extend(
            [
                f"attachments_dir: {attachments_dir}",
                "If meta.json or attachments_dir lists original files, read those too before deciding input is missing.",
            ]
        )
    lines.extend(
        [
            "Do not write status.md/log.md directly in the shared mount.",
            f"Worker-state helper path: {HELPER_DIR / 'openclaw_vm_worker_state.py'}",
            "All high-frequency execution state belongs in ~/.openclaw/worker-state.",
            "Implement the task in the repo, validate the smallest relevant scope, and end with a structured result/artifact summary.",
        ]
    )
    return "\n".join(lines)


def build_local_plan(task: dict[str, Any], args: argparse.Namespace, command_text: str) -> str:
    lines = [
        "# Local Plan",
        "",
        f"- task_id: {task['task_id']}",
        f"- title: {task.get('title') or ''}",
        f"- canonical_task_dir: {task.get('task_dir') or ''}",
        f"- goal_path: {task.get('goal_path') or ''}",
        f"- repo_root: {args.repo_root}",
        f"- report_contract: {report_contract_name()}",
        f"- command: {command_text}",
    ]
    return "\n".join(lines) + "\n"




def _resolve_goal_pnc_user(goal_text: str) -> str | None:
    for raw_line in goal_text.splitlines():
        line = raw_line.strip()
        if not line.startswith('- ensure_command:'):
            continue
        command = line.split(':', 1)[1].strip()
        try:
            parts = shlex.split(command)
        except ValueError:
            return None
        if len(parts) >= 4 and parts[-1] == 'pnc_specs':
            return parts[-2]
    return None


def _goal_runtime_worktree_mode(goal_text: str) -> str | None:
    for raw_line in goal_text.splitlines():
        line = raw_line.strip()
        if not line.startswith('- runtime_worktree:'):
            continue
        return line.split(':', 1)[1].strip()
    return None


def _resolve_goal_worktree_env(goal_text: str) -> str | None:
    user = _resolve_goal_pnc_user(goal_text)
    if not user:
        return None
    mode = _goal_runtime_worktree_mode(goal_text)
    if mode in (None, '', 'user-worktree'):
        return f"/home/mini/worktrees/pnc_specs/{user}"
    if mode == 'clean-latest':
        return f"/home/mini/worktrees/pnc_specs/.runtime/{user}/latest-main"
    return None


def _goal_manifest_relpath(goal_text: str) -> str | None:
    for raw_line in goal_text.splitlines():
        line = raw_line.strip()
        if 'manifest_relpath:' not in line:
            continue
        manifest = line.split('manifest_relpath:', 1)[1].strip().strip('`')
        if re.fullmatch(r'src/tools/[\w-]+/manifest\.ya?ml', manifest):
            return manifest
    return None


def _is_pnc_fixed_cli_goal(goal_text: str, run_cmd: str, worktree_env: str | None) -> bool:
    if not worktree_env:
        return False
    if not any(run_cmd.startswith(f'./{name}') for name in ['generate-dbc', 'parse-bus-data', 'validate-data-validity']):
        return False
    return 'pnc_specs' in goal_text


def _unsafe_pnc_fixed_cli_command(reason: str) -> str:
    safe_reason = reason.replace("'", "_")[:300]
    return f"echo 'unsafe or malformed pnc fixed-cli goal: {safe_reason}' >&2; exit 2"


def _build_pnc_preflight_command(
    worktree_path: str,
    manifest_relpath: str,
    workdir: str,
    run_cmd: str,
    runtime_mode: str | None = None,
) -> str:
    snapshot_path = f"{workdir.rstrip('/')}/resolved_snapshot.json"
    runtime_setup = ""
    if runtime_mode == 'clean-latest':
        runtime_setup = f"""source_repo=/home/mini/pnc_specs
runtime_ref=origin/main
runtime_branch=latest-main
git -C /home/mini/pnc_specs fetch origin --prune
mkdir -p "$(dirname "$worktree_path")"
if git -C "$worktree_path" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  git -C "$worktree_path" fetch origin --prune
  git -C "$worktree_path" checkout -B "$runtime_branch" "$runtime_ref"
  git -C "$worktree_path" reset --hard "$runtime_ref"
else
  rm -rf "$worktree_path"
  git -C /home/mini/pnc_specs worktree add --force -B "$runtime_branch" "$worktree_path" "$runtime_ref"
fi
"""
    return f"""set -euo pipefail
worktree_path={shlex.quote(worktree_path)}
manifest_relpath={shlex.quote(manifest_relpath)}
workdir={shlex.quote(workdir)}
snapshot_path={shlex.quote(snapshot_path)}
{runtime_setup}git -C "$worktree_path" fetch origin --prune
branch="$(git -C "$worktree_path" branch --show-current)"
if [ -z "$branch" ]; then
  echo 'pnc_specs preflight failed: detached HEAD' >&2
  exit 2
fi
upstream_ref="$(git -C "$worktree_path" rev-parse --abbrev-ref --symbolic-full-name '@{{u}}')"
upstream_commit="$(git -C "$worktree_path" rev-parse "$upstream_ref")"
commit="$(git -C "$worktree_path" rev-parse HEAD)"
dirty="$(git -C "$worktree_path" status --porcelain)"
if [ -n "$dirty" ]; then
  echo 'pnc_specs preflight failed: dirty worktree' >&2
  git -C "$worktree_path" status --short >&2
  exit 2
fi
counts="$(git -C "$worktree_path" rev-list --left-right --count HEAD..."$upstream_ref")"
left="${{counts%%[[:space:]]*}}"
right="${{counts##*[[:space:]]}}"
if [ "$right" != "0" ]; then
  echo "pnc_specs preflight failed: behind upstream by $right commits" >&2
  exit 2
fi
manifest_path="$worktree_path/$manifest_relpath"
if [ ! -f "$manifest_path" ] && [ -f "$workdir/$manifest_relpath" ]; then
  manifest_path="$workdir/$manifest_relpath"
fi
if [ ! -f "$manifest_path" ]; then
  echo "pnc_specs preflight failed: missing manifest $manifest_path" >&2
  exit 2
fi
manifest_sha256="$(sha256sum "$manifest_path" | awk '{{print $1}}')"
python3 - "$snapshot_path" "$worktree_path" "$branch" "$commit" "$upstream_ref" "$upstream_commit" "$manifest_path" "$manifest_sha256" <<'SNAPSHOT_PY'
import json, sys
path, worktree, branch, commit, upstream_ref, upstream_commit, manifest_path, manifest_sha256 = sys.argv[1:]
snapshot = {{
    'repo': 'pnc_specs',
    'worktree_path': worktree,
    'branch': branch,
    'commit': commit,
    'upstream_ref': upstream_ref,
    'upstream_commit': upstream_commit,
    'dirty': False,
    'behind': False,
    'manifest_path': manifest_path,
    'manifest_sha256': manifest_sha256,
}}
with open(path, 'w', encoding='utf-8') as f:
    json.dump(snapshot, f, ensure_ascii=False, indent=2)
    f.write('\\n')
SNAPSHOT_PY
if [ -n "${{OPENCLAW_WORKER_TASK_ARTIFACTS_DIR:-}}" ]; then
  mkdir -p "$OPENCLAW_WORKER_TASK_ARTIFACTS_DIR"
  cp "$snapshot_path" "$OPENCLAW_WORKER_TASK_ARTIFACTS_DIR/resolved_snapshot.json"
fi
cd "$workdir"
{run_cmd}
"""


def _extract_fixed_cli_command(goal_text: str) -> str | None:
    """Extract a simple two-line fixed-cli command from a generated PNC goal."""
    lines = [line.strip() for line in goal_text.splitlines()]
    for idx, line in enumerate(lines):
        if not line.startswith('- cd '):
            continue
        if idx + 1 >= len(lines) or not lines[idx + 1].startswith('- ./'):
            continue
        cd_arg = line[len('- cd '):].strip()
        run_cmd = lines[idx + 1][len('- '):].strip()
        try:
            workdir = shlex.split(cd_arg)[0]
        except (IndexError, ValueError):
            continue
        runtime_mode = _goal_runtime_worktree_mode(goal_text)
        if runtime_mode not in (None, '', 'user-worktree', 'clean-latest'):
            return _unsafe_pnc_fixed_cli_command(f'invalid runtime_worktree: {runtime_mode}')
        worktree_env = _resolve_goal_worktree_env(goal_text)
        if worktree_env and workdir.startswith('$WORKTREE_PATH/'):
            workdir = worktree_env + workdir[len('$WORKTREE_PATH'):]
        is_pnc_goal = _is_pnc_fixed_cli_goal(goal_text, run_cmd, worktree_env)
        if is_pnc_goal and 'Repository freshness preflight' not in goal_text:
            return _unsafe_pnc_fixed_cli_command('missing Repository freshness preflight')
        if not workdir.startswith('/home/mini/'):
            continue
        if is_pnc_goal:
            manifest_relpath = _goal_manifest_relpath(goal_text)
            if not manifest_relpath:
                return _unsafe_pnc_fixed_cli_command('missing or invalid manifest_relpath')
            return _build_pnc_preflight_command(worktree_env or '', manifest_relpath, workdir, run_cmd, runtime_mode=runtime_mode)
        return f"set -euo pipefail; cd {shlex.quote(workdir)}; {run_cmd}"
    return None


def _goal_requests_fixed_cli(task: dict[str, Any], root: Path) -> str | None:
    goal_path = Path(str(task.get('goal_path') or ''))
    if not goal_path.is_file():
        goal_path = canonical_state.task_dir(root, task['task_id']) / 'goal.md'
    if not goal_path.is_file():
        return None
    goal_text = goal_path.read_text(encoding='utf-8', errors='replace')
    if 'executor: fixed-cli under VM worker' not in goal_text:
        return None
    return _extract_fixed_cli_command(goal_text)

def _read_goal_text(task: dict[str, Any], root: Path) -> str:
    goal_path = Path(str(task.get('goal_path') or ''))
    if not goal_path.is_file():
        goal_path = canonical_state.task_dir(root, task['task_id']) / 'goal.md'
    if not goal_path.is_file():
        return ""
    return goal_path.read_text(encoding='utf-8', errors='replace')


def _extract_labeled_value(goal_text: str, labels: list[str], pattern: str) -> str | None:
    for raw_line in goal_text.splitlines():
        line = raw_line.strip()
        if not any(label in line for label in labels):
            continue
        match = re.search(pattern, line)
        if match:
            return match.group(0).rstrip('。,.，；;`')
    return None


def _repo_git_goal_fail_closed(goal_text: str) -> bool:
    return bool(re.search(r'git@git\.minieye\.tech:[\w./-]+\.git', goal_text)) and (
        '目标仓库' in goal_text or '建议目标路径' in goal_text or '目标路径' in goal_text or 'git clone' in goal_text
    )


def _unsafe_repo_git_command(reason: str) -> str:
    safe_reason = reason.replace("'", "_")[:300]
    return f"echo 'unsafe or malformed repo git goal: {safe_reason}' >&2; exit 2"


def _safe_repo_target(raw_path: str) -> str | None:
    try:
        candidate = Path(raw_path).expanduser()
    except RuntimeError:
        return None
    if not candidate.is_absolute():
        return None
    if '..' in candidate.parts:
        return None
    resolved = candidate.resolve(strict=False)
    allowed_root = Path('/home/mini').resolve(strict=False)
    try:
        resolved.relative_to(allowed_root)
    except ValueError:
        return None
    return str(resolved)


def _extract_vm_repo_git_command(task: dict[str, Any], root: Path) -> str | None:
    """Build a safe fixed executor for simple VM-side repo clone/fetch goals."""
    goal_text = _read_goal_text(task, root)
    repo_url = _extract_labeled_value(goal_text, ['目标仓库', 'repo_url', 'repository'], r'git@git\.minieye\.tech:[\w./-]+\.git')
    repo_dir_raw = _extract_labeled_value(goal_text, ['建议目标路径', '目标路径', 'target_path', 'repo_dir'], r'/(?:home|mnt)/[\w./\-]+')
    if not repo_url and not repo_dir_raw and not _repo_git_goal_fail_closed(goal_text):
        return None
    if not repo_url:
        return _unsafe_repo_git_command('missing labeled git.minieye repo url')
    if not repo_dir_raw:
        return _unsafe_repo_git_command('missing labeled target path')
    repo_dir = _safe_repo_target(repo_dir_raw)
    if not repo_dir:
        return _unsafe_repo_git_command(f'unsafe target path: {repo_dir_raw}')
    repo_name = Path(repo_dir).name
    audit_user = '胡子豪'
    return f"""set -euo pipefail
repo_url={shlex.quote(repo_url)}
repo_dir={shlex.quote(repo_dir)}
repo_name={shlex.quote(repo_name)}
audit=/home/mini/worktrees/audit-logger.sh
if [ -x \"$audit\" ]; then
  \"$audit\" {shlex.quote(audit_user)} \"$repo_name\" \"vm-worker git clone/fetch $repo_url $repo_dir\"
fi
action=\"\"
if [ -e \"$repo_dir\" ] && git -C \"$repo_dir\" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  current_url=\"$(git -C \"$repo_dir\" remote get-url origin)\"
  if [ \"$current_url\" != \"$repo_url\" ]; then
    echo \"origin URL mismatch: $current_url != $repo_url\" >&2
    exit 2
  fi
  git -C \"$repo_dir\" fetch --all --prune
  action=fetch
elif [ -e \"$repo_dir\" ]; then
  echo \"target exists but is not a git repo: $repo_dir\" >&2
  exit 2
else
  git clone \"$repo_url\" \"$repo_dir\"
  action=clone
fi
branch=\"$(git -C \"$repo_dir\" branch --show-current)\"
head=\"$(git -C \"$repo_dir\" rev-parse --short HEAD)\"
status=\"$(git -C \"$repo_dir\" status --short --branch)\"
origin=\"$(git -C \"$repo_dir\" remote get-url origin)\"
printf 'VM repo git task completed\\npath=%s\\naction=%s\\nbranch=%s\\nhead=%s\\norigin=%s\\nstatus:\\n%s\\n' \"$repo_dir\" \"$action\" \"$branch\" \"$head\" \"$origin\" \"$status\"
"""


def build_command(task: dict[str, Any], args: argparse.Namespace) -> tuple[list[str], str]:
    if args.executor_command:
        return ["bash", "-lc", args.executor_command], args.executor_command
    canonical_root = canonical_state.resolve_canonical_root(args.root)
    command_contract = task.get("command_contract") if isinstance(task.get("command_contract"), dict) else {}
    direct_fixed_cli = task.get("fixed_cli_command")
    if isinstance(direct_fixed_cli, str) and direct_fixed_cli.strip() and bool(command_contract.get("must_write_result_json")):
        # Narrow canary/throughput path: scheduler-seeded fixed-cli tasks may
        # carry the exact bounded shell command in dispatch metadata.  Requiring
        # must_write_result_json keeps arbitrary business tasks on the normal
        # goal-parser/OpenClaw path.
        return ["bash", "-lc", direct_fixed_cli], direct_fixed_cli
    fixed_cli_command = _goal_requests_fixed_cli(task, canonical_root)
    if fixed_cli_command:
        return ["bash", "-lc", fixed_cli_command], fixed_cli_command
    repo_git_command = _extract_vm_repo_git_command(task, canonical_root)
    if repo_git_command:
        return ["bash", "-lc", repo_git_command], repo_git_command
    host_inbox_root = resolve_host_inbox_root(args, canonical_root)
    prompt = build_prompt(task, canonical_root, host_inbox_root)
    require_codex_metadata(task)
    backend = select_agent_backend(task, args)
    if backend == "codex":
        return build_codex_command(task, args, prompt)
    if backend != "openclaw":
        raise ValueError(f"unsupported agent_backend: {backend}")
    openclaw_bin = resolve_openclaw_bin()
    cmd = [
        openclaw_bin,
        "agent",
        "--agent",
        args.agent_id,
        "--session-id",
        task["task_id"],
        "--thinking",
        args.thinking,
        "--timeout",
        str(args.timeout_seconds),
        "--message",
        prompt,
        "--json",
    ]
    return cmd, shlex.join(cmd)


def _heartbeat_import(root: Path, inbox_root: Path) -> None:
    canonical_state.import_inbox(root=root, cleanup=False, inbox_root=inbox_root)



def _load_resource_preflight_module() -> Any:
    module_path = Path(__file__).with_name("vm_resource_preflight.py")
    spec = importlib.util.spec_from_file_location("vm_resource_preflight", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load resource preflight helper: {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _task_lane(task: dict[str, Any]) -> str:
    for key in ("lane", "resource_class"):
        value = task.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    for container_key in ("meta", "metadata"):
        container = task.get(container_key)
        if isinstance(container, dict):
            for key in ("lane", "resource_class"):
                value = container.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
    title_goal = f"{task.get('title') or ''}\n{task.get('goal') or ''}".lower()
    if any(word in title_goal for word in ("raw", "mcap", "评测", "归因", "download", "evaluation")):
        return "heavy"
    return "standard"


def _task_preflight_artifact_root(task: dict[str, Any], local_task: local_state.WorkerTask) -> Path:
    for key in ("artifact_root", "work_tmp_dir", "download_dir"):
        value = task.get(key)
        if isinstance(value, str) and value.strip():
            return Path(value).expanduser()
    for container_key in ("meta", "metadata"):
        container = task.get(container_key)
        if isinstance(container, dict):
            for key in ("artifact_root", "work_tmp_dir", "download_dir"):
                value = container.get(key)
                if isinstance(value, str) and value.strip():
                    return Path(value).expanduser()
    return local_task.artifacts_dir


def _resource_preflight_result(task: dict[str, Any], args: argparse.Namespace, local_task: local_state.WorkerTask) -> dict[str, Any] | None:
    if not bool(getattr(args, "resource_preflight", False)):
        return None
    helper = _load_resource_preflight_module()
    return helper.check_preflight(
        artifact_root=_task_preflight_artifact_root(task, local_task),
        lane=_task_lane(task),
        min_disk_gb=getattr(args, "min_disk_gb", None),
        min_memory_gb=getattr(args, "min_memory_gb", None),
    )

def _terminal_local_result(task: local_state.WorkerTask) -> dict[str, Any] | None:
    payload = task.read_result()
    state = str(payload.get("state") or "").strip()
    if state in local_state.TERMINAL_STATES:
        return payload
    return None


def _candidate_external_result_paths(task: dict[str, Any], local_task: local_state.WorkerTask) -> list[Path]:
    """Return bounded external result.json candidates for fixed-cli canaries.

    Quasi-real fixed-cli canary commands are intentionally plain shell scripts.
    They may write their business result under the declared /mnt/tmp/<task_id>/
    artifact root instead of the worker-local local-result.json.  Keep this
    narrowly scoped to canary-shaped tasks so arbitrary business workloads do
    not silently satisfy the VM result contract with an unrelated result.json.
    """
    task_id = str(task.get("task_id") or "").strip()
    meta = task.get("meta") if isinstance(task.get("meta"), dict) else {}
    command_contract = task.get("command_contract") if isinstance(task.get("command_contract"), dict) else {}
    is_canary = bool(meta.get("canary")) or str(meta.get("canary_type") or "").startswith("quasi_real_heavy")
    must_write_result = bool(command_contract.get("must_write_result_json"))
    if not (is_canary and must_write_result and task_id):
        return []
    candidates: list[Path] = []
    for raw in (
        meta.get("artifact_root"),
        task.get("artifact_root"),
        task.get("work_tmp_dir"),
        task.get("download_dir"),
    ):
        if isinstance(raw, str) and raw.strip():
            root = Path(raw).expanduser()
            try:
                resolved = root.resolve(strict=False)
            except Exception:
                continue
            if resolved == Path("/mnt/tmp") / task_id or str(resolved).startswith(f"/mnt/tmp/{task_id}/"):
                candidates.append(resolved / "result.json")
    return candidates


def _terminal_external_result(task: dict[str, Any], local_task: local_state.WorkerTask) -> dict[str, Any] | None:
    for path in _candidate_external_result_paths(task, local_task):
        if not path.is_file():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        state = str(payload.get("state") or "").strip()
        if state not in local_state.TERMINAL_STATES:
            continue
        result_payload = dict(payload)
        result_payload.setdefault("external_result_path", str(path))
        result_payload.setdefault("result_mode", "external_canary_result_json")
        task_id = str(task.get("task_id") or result_payload.get("task_id") or "").strip()
        return {
            "task_id": task_id,
            "state": state,
            "exit_code": int(payload.get("exit_code") if payload.get("exit_code") is not None else (0 if state == "completed" else 1)),
            "summary": str(payload.get("summary") or f"{task_id} {state} via external result.json"),
            "result": result_payload,
        }
    return None


def execute_claim(task: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    canonical_root = canonical_state.resolve_canonical_root(args.root)
    worker_root = local_state.resolve_worker_state_root(args.worker_root)
    host_inbox_root = resolve_host_inbox_root(args, canonical_root)
    task_id = str(task["task_id"])
    run_id = str(task.get("run_id") or f"worker-{task_id}")
    local_task = local_state.WorkerTask(task_id, worker_root, host_inbox_root=host_inbox_root)
    command, command_text = build_command(task, args)
    local_task.initialize(
        metadata={
            "task_id": task_id,
            "title": task.get("title"),
            "canonical_root": str(canonical_root),
            "canonical_task_dir": task.get("task_dir"),
            "goal_path": task.get("goal_path"),
            "repo_root": args.repo_root,
            "command": command_text,
            "host_inbox_root": str(host_inbox_root),
        },
        plan_text=build_local_plan(task, args, command_text),
        state="claimed",
        summary=f"claimed {task_id}",
        run_id=run_id,
        extra_status={
            "canonical_task_dir": task.get("task_dir"),
            "goal_path": task.get("goal_path"),
            "host_inbox_root": str(host_inbox_root),
        },
    )
    local_state.upsert_claimed_task(
        worker_root,
        task_id,
        {
            "state": "claimed",
            "claimed_at": canonical_state.iso_now(),
            "canonical_task_dir": task.get("task_dir"),
            "goal_path": task.get("goal_path"),
        },
    )
    _heartbeat_import(canonical_root, host_inbox_root)

    runner_log = local_task.artifacts_dir / "runner.log"
    try:
        preflight = _resource_preflight_result(task, args, local_task)
    except Exception as exc:
        preflight = {"ok": False, "state": "blocked_preflight", "reasons": ["resource_preflight_error"], "error": str(exc)}
    if preflight is not None and not bool(preflight.get("ok")):
        runner_log.parent.mkdir(parents=True, exist_ok=True)
        runner_log.write_text(
            f"[{canonical_state.iso_now()}] resource preflight blocked task before command execution\n"
            + json.dumps(preflight, ensure_ascii=False, indent=2)
            + "\n",
            encoding="utf-8",
        )
        result_payload = build_structured_result(
            task=task,
            task_id=task_id,
            run_id=run_id,
            repo_root=args.repo_root,
            command=command,
            runner_log=runner_log,
            local_task=local_task,
        )
        result_payload["agent_status"] = "blocked_preflight"
        result_payload["resource_preflight"] = preflight
        local_task.write_result(
            state="failed",
            exit_code=2,
            summary=f"{task_id} blocked_preflight",
            result=result_payload,
        )
        _heartbeat_import(canonical_root, host_inbox_root)
        local_state.remove_claimed_task(worker_root, task_id)
        return {
            "task_id": task_id,
            "state": "failed",
            "exit_code": 2,
            "runner_log": str(runner_log),
        }

    process = None
    exit_code = 1
    try:
        with runner_log.open("a", encoding="utf-8") as runner_handle:
            runner_handle.write(f"[{canonical_state.iso_now()}] worker command: {command_text}\n")
            spawn_env = build_spawn_env(
                canonical_root=canonical_root,
                worker_root=worker_root,
                host_inbox_root=host_inbox_root,
            )
            spawn_env["OPENCLAW_WORKER_TASK_ARTIFACTS_DIR"] = str(local_task.artifacts_dir)
            process = subprocess.Popen(
                command,
                cwd=args.repo_root,
                env=spawn_env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                start_new_session=True,
            )
            local_task.update_status(
                state="in_progress",
                summary=f"{task_id} running",
                run_id=run_id,
                pid=process.pid,
                extra={
                    "canonical_task_dir": task.get("task_dir"),
                    "goal_path": task.get("goal_path"),
                },
            )
            _heartbeat_import(canonical_root, host_inbox_root)
            assert process.stdout is not None
            heartbeat_deadline = time.monotonic() + args.heartbeat_seconds
            codex_deadline = None
            if select_agent_backend(task, args) == "codex":
                codex_deadline = time.monotonic() + codex_timeout_seconds(task, args)
            while True:
                ready, _, _ = select.select([process.stdout], [], [], 1.0)
                if ready:
                    line = process.stdout.readline()
                    if line:
                        runner_handle.write(line)
                        runner_handle.flush()
                        local_task.append_worker_log(line)
                if time.monotonic() >= heartbeat_deadline:
                    local_task.sync_log_tail(reason="heartbeat")
                    current_status = local_task.read_status()
                    current_state = str(current_status.get("state") or "").strip()
                    if current_state not in local_state.TERMINAL_STATES:
                        local_task.update_status(
                            state="in_progress",
                            summary=f"{task_id} running",
                            run_id=run_id,
                            pid=process.pid,
                            extra={
                                "canonical_task_dir": task.get("task_dir"),
                                "goal_path": task.get("goal_path"),
                            },
                        )
                    _heartbeat_import(canonical_root, host_inbox_root)
                    heartbeat_deadline = time.monotonic() + args.heartbeat_seconds
                if codex_deadline is not None and time.monotonic() >= codex_deadline and process.poll() is None:
                    runner_handle.write(f"[{canonical_state.iso_now()}] codex timeout reached; terminating process\n")
                    runner_handle.flush()
                    local_task.append_worker_log("codex timeout reached; terminating process\n")
                    try:
                        os.killpg(process.pid, 15)
                    except Exception:
                        with contextlib.suppress(Exception):
                            process.terminate()
                    time.sleep(2)
                    if process.poll() is None:
                        try:
                            os.killpg(process.pid, 9)
                        except Exception:
                            with contextlib.suppress(Exception):
                                process.kill()
                    local_task.write_result(
                        state="failed",
                        exit_code=124,
                        summary=f"{task_id} codex timeout",
                        result={"timeout_seconds": codex_timeout_seconds(task, args), "backend": "codex", "reason": "codex_timeout"},
                    )
                    break
                if process.poll() is not None:
                    remainder = process.stdout.read()
                    if remainder:
                        runner_handle.write(remainder)
                        runner_handle.flush()
                        local_task.append_worker_log(remainder)
                    break
            exit_code = int(process.wait())
        local_task.sync_log_tail(reason="process_exit")
        _persist_agent_final_text_artifact(task, local_task, runner_log)
        final_result = _terminal_local_result(local_task)
        if final_result is None:
            external_terminal = _terminal_external_result(task, local_task)
            if external_terminal is not None:
                local_task.write_result(
                    state=str(external_terminal.get("state") or ("completed" if exit_code == 0 else "failed")),
                    exit_code=int(external_terminal.get("exit_code") if external_terminal.get("exit_code") is not None else exit_code),
                    summary=str(external_terminal.get("summary") or f"{task_id} {'completed' if exit_code == 0 else 'failed'}"),
                    result=external_terminal.get("result") if isinstance(external_terminal.get("result"), dict) else {},
                )
                final_result = _terminal_local_result(local_task)
        if final_result is None:
            structured_result = build_structured_result(
                task=task,
                task_id=task_id,
                run_id=run_id,
                repo_root=args.repo_root,
                command=command,
                runner_log=runner_log,
                local_task=local_task,
            )
            agent_terminal = _agent_terminal_result_from_runner_log(
                task_id=task_id,
                runner_log=runner_log,
                exit_code=exit_code,
                local_task=local_task,
                structured_result=structured_result,
            )
            if agent_terminal is not None:
                local_task.write_result(
                    state=str(agent_terminal.get("state") or ("completed" if exit_code == 0 else "failed")),
                    exit_code=int(agent_terminal.get("exit_code") if agent_terminal.get("exit_code") is not None else exit_code),
                    summary=str(agent_terminal.get("summary") or f"{task_id} {'completed' if exit_code == 0 else 'failed'}"),
                    result=agent_terminal.get("result") if isinstance(agent_terminal.get("result"), dict) else structured_result,
                )
            else:
                local_task.write_result(
                    state="completed" if exit_code == 0 else "failed",
                    exit_code=exit_code,
                    summary=f"{task_id} {'completed' if exit_code == 0 else 'failed'}",
                    result=structured_result,
                )
            final_result = _terminal_local_result(local_task)
        _heartbeat_import(canonical_root, host_inbox_root)
        return {
            "task_id": task_id,
            "state": str((final_result or {}).get("state") or ("completed" if exit_code == 0 else "failed")),
            "exit_code": (final_result or {}).get("exit_code", exit_code),
            "runner_log": str(runner_log),
        }
    except Exception as exc:
        local_task.sync_log_tail(reason="exception")
        final_result = _terminal_local_result(local_task)
        if final_result is None:
            local_task.write_result(
                state="failed",
                exit_code=exit_code,
                summary=f"{task_id} failed before completion",
                result=build_structured_result(
                    task=task,
                    task_id=task_id,
                    run_id=run_id,
                    repo_root=args.repo_root,
                    command=command,
                    runner_log=runner_log,
                    local_task=local_task,
                    error=str(exc),
                ),
            )
            final_result = _terminal_local_result(local_task)
        _heartbeat_import(canonical_root, host_inbox_root)
        payload = {
            "task_id": task_id,
            "state": str((final_result or {}).get("state") or "failed"),
            "exit_code": (final_result or {}).get("exit_code", exit_code),
            "runner_log": str(runner_log),
        }
        if final_result is None:
            payload["error"] = str(exc)
        return payload
    finally:
        local_state.remove_claimed_task(worker_root, task_id)




def compact_import_result(result: Any) -> Any:
    if not isinstance(result, dict):
        return result
    keep_keys = [
        'root', 'bridge_root', 'handoff_root', 'started_at', 'finished_at',
        'imported', 'existing', 'ignored', 'ignored_state', 'missing_status',
        'missing_goal', 'invalid', 'removed',
    ]
    compact: dict[str, Any] = {key: result[key] for key in keep_keys if key in result}
    for key in ['tasks', 'errors']:
        value = result.get(key)
        if isinstance(value, list):
            compact[f'{key}_count'] = len(value)
        elif value is not None:
            compact[f'{key}_present'] = True
    if 'index' in result:
        compact['index_present'] = True
    return compact

def build_status_summary(root: Path) -> dict[str, Any]:
    queues = ['pending', 'claimed', 'running', 'done', 'failed', 'dead']
    dispatch: dict[str, int] = {}
    for queue in queues:
        qdir = root / 'dispatch' / queue
        dispatch[queue] = len(list(qdir.glob('*.json'))) if qdir.exists() else 0
    return {
        'root': str(root),
        'dispatch': dispatch,
    }

def _worker_lock_path(args: argparse.Namespace, canonical_root: Path) -> Path:
    task_id = str(getattr(args, "execute_claimed_task_id", None) or "").strip()
    if task_id:
        safe_task_id = re.sub(r"[^A-Za-z0-9_.:-]", "_", task_id)[:128]
        return canonical_root / ".runtime" / f"vm_coding_worker_v2.{safe_task_id}.lock"
    return canonical_root / ".runtime" / "vm_coding_worker_v2.lock"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Claim dispatch/pending tasks and execute them on the VM")
    parser.add_argument("--root", default=None)
    parser.add_argument("--worker-root", default=None)
    parser.add_argument("--repo-root", default=DEFAULT_REPO_ROOT)
    parser.add_argument("--dispatch-pending", action="store_true")
    parser.add_argument("--max-dispatch", type=int, default=1)
    parser.add_argument("--task-id", default=None, help="Only claim and execute this dispatch/pending task id.")
    parser.add_argument("--execute-claimed-task-id", default=None, help="Execute an already claimed dispatch/claimed task id without re-claiming pending.")
    parser.add_argument("--lease-seconds", type=int, default=300)
    parser.add_argument("--heartbeat-seconds", type=int, default=DEFAULT_HEARTBEAT_SECONDS)
    parser.add_argument("--thinking", default="low")
    parser.add_argument("--timeout-seconds", type=int, default=5400)
    parser.add_argument("--agent-id", default="coding")
    parser.add_argument("--host-inbox-root", default=None)
    parser.add_argument("--executor-command", default=None)
    parser.add_argument("--resource-preflight", action="store_true", help="Run lane-aware disk/memory/artifact-root preflight before executing a claimed task.")
    parser.add_argument("--min-disk-gb", type=float, default=None, help="Override resource preflight minimum free disk GB.")
    parser.add_argument("--min-memory-gb", type=float, default=None, help="Override resource preflight minimum MemAvailable GB.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--compact-status", action="store_true", help="Emit compact dispatch counts instead of full read_status on successful runs.")
    parser.add_argument("--full-status", action="store_true", help="Include full read_status payload even when --compact-status is set.")
    parser.add_argument("--once", action="store_true", help="No-op flag for cron readability.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    canonical_root = canonical_state.ensure_canonical_root(args.root)
    lock_path = _worker_lock_path(args, canonical_root)
    payload: dict[str, Any] | None = None
    try:
        with worker_lock(lock_path):
            host_inbox_root = resolve_host_inbox_root(args, canonical_root)
            bridge_import = None
            bridge_root = host_inbox_root.parent
            if bridge_root != canonical_root:
                bridge_import = canonical_state.import_bridge_deliveries(
                    root=canonical_root,
                    bridge_root=bridge_root,
                    cleanup=True,
                )
            legacy_handoff_import = None
            legacy_handoff_root = resolve_legacy_handoff_root(args)
            if legacy_handoff_root != canonical_root:
                legacy_handoff_import = canonical_state.import_legacy_handoff(
                    root=canonical_root,
                    handoff_root=legacy_handoff_root,
                )
            pending = canonical_state.list_dispatch(canonical_root, "pending")
            execute_claimed_task = None
            if args.execute_claimed_task_id:
                claimed_path = canonical_root / "dispatch" / "claimed" / f"{args.execute_claimed_task_id}.json"
                if not claimed_path.exists():
                    payload = {"root": str(canonical_root), "execute_claimed_task_id": args.execute_claimed_task_id, "error": "claimed_task_not_found"}
                    print(json.dumps(payload, ensure_ascii=False, indent=2))
                    return 2
                execute_claimed_task = json.loads(claimed_path.read_text(encoding="utf-8"))
            if args.dry_run:
                result = {
                    "root": str(canonical_root),
                    "bridge_import": bridge_import,
                    "legacy_handoff_import": legacy_handoff_import,
                    "pending": [item.get("task_id") for item in pending],
                    "task_id_filter": args.task_id,
                    "would_dispatch": (
                        [item.get("task_id") for item in pending if item.get("task_id") == args.task_id][:1]
                        if args.task_id
                        else [item.get("task_id") for item in pending[: max(args.max_dispatch, 0)]]
                    ),
                }
                print(json.dumps(result, ensure_ascii=False, indent=2))
                return 0
            claimed_batch: list[dict[str, Any]] = []
            if execute_claimed_task is not None:
                claimed_batch = [execute_claimed_task]
            elif args.dispatch_pending and args.max_dispatch > 0:
                claimed_batch = canonical_state.claim_pending_batch(
                    root=canonical_root,
                    limit=args.max_dispatch,
                    lease_seconds=args.lease_seconds,
                    agent_host=detect_host(),
                    task_id=args.task_id,
                )
            if args.compact_status:
                bridge_payload = compact_import_result(bridge_import)
                legacy_payload = compact_import_result(legacy_handoff_import)
            else:
                bridge_payload = bridge_import
                legacy_payload = legacy_handoff_import
            payload = {
                "root": str(canonical_root),
                "bridge_import": bridge_payload,
                "legacy_handoff_import": legacy_payload,
                "pending_before": [item.get("task_id") for item in pending],
                "task_id_filter": args.task_id,
                "execute_claimed_task_id": args.execute_claimed_task_id,
                "claimed_task_ids": [item.get("task_id") for item in claimed_batch],
                "_claimed_batch": claimed_batch,
            }
    except RuntimeError as exc:
        payload = {"root": str(canonical_root), "skipped": str(exc)}
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0
    assert payload is not None
    dispatched = [execute_claim(claimed, args) for claimed in payload.pop("_claimed_batch", [])]
    payload["dispatched"] = dispatched
    if args.compact_status:
        payload["status_summary"] = build_status_summary(canonical_root)
        if args.full_status:
            payload["status"] = canonical_state.read_status(canonical_root)
    else:
        payload["status"] = canonical_state.read_status(canonical_root)
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
