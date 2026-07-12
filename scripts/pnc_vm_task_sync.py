#!/usr/bin/env python3
"""Sync PNC Feishu VM/shared-state task progress into delivery sidecars.

This is intentionally a small host-side bridge: it scans recent Feishu tasks
that already have a vm_task_id, reads remote shared-state through
vm_task_status_collect, and writes ~/.hermes/task-state sidecars so the PNC
user-facing /tasks view does not depend on manual collection.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Iterable

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gateway.tasks.store import TaskStore  # noqa: E402
from gateway.tasks.types import Task, TaskStatus  # noqa: E402
from hermes_cli.config import get_hermes_home  # noqa: E402
from scripts.vm_task_state_bridge import _atomic_write_json, _load_existing, sidecar_path  # noqa: E402
from gateway.feishu_task_card import render_status_line, stable_render_hash, render_task_card  # noqa: E402
from scripts.pnc_foxglove_delivery import foxglove_delivery_fields  # noqa: E402
from scripts.pnc_status_projection import PIPELINE_RUNNING_STAGES, PIPELINE_STAGE_DEFINITIONS, PIPELINE_STAGE_LABELS, derive_presentation  # noqa: E402
from scripts.vm_task_status_collect import collect_vm_task_status, write_collected_status_to_sidecar  # noqa: E402
from tools import vm_task_completion_probe  # noqa: E402

PNC_CHAT_ID = "oc_16614f4ba25b8c88b69c0b8e9ebc2fb5"
G1Q3_RCA_CHAT_ID = "oc_6cfc782212009ff4cd815349909dd423"
DEFAULT_CHAT_IDS = (PNC_CHAT_ID, G1Q3_RCA_CHAT_ID)
REPORT_FS_PREFIX = "/mnt/minieye/pdcl/department/perception_test_team/"
REPORT_ATTACHMENT_CODEC_VERSION = "bom-utf8-v1"
UTF8_BOM = b"\xef\xbb\xbf"
REPORT_INTERNAL_HTTP_BASE = "http://192.168.26.174:18081"
PNC_FEISHU_BUSINESS_TZ = timezone(timedelta(hours=8))

PERCEPTION_TEST_TEAM_VM_PREFIX = "/mnt/minieye/pdcl/department/perception_test_team/"
PERCEPTION_TEST_TEAM_CIFS_PREFIX = "//hfs.minieye.tech/department-perception_test_team/"
PIPELINE_STATE_READ_LINES = 500
PIPELINE_STATE_SOURCE = "vm_pipeline_state"


def _perception_test_team_cifs(vm_path: str) -> str:
    text = str(vm_path or "").strip()
    if not text.startswith(PERCEPTION_TEST_TEAM_VM_PREFIX):
        return ""
    rel = text[len(PERCEPTION_TEST_TEAM_VM_PREFIX):].lstrip("/")
    if not rel or any(part in {".", ".."} for part in rel.split("/")):
        return ""
    return PERCEPTION_TEST_TEAM_CIFS_PREFIX + rel


def _read_json_file(path: Path, *, max_bytes: int = 20 * 1024 * 1024) -> dict[str, Any]:
    try:
        if not path.is_file() or path.stat().st_size > max_bytes:
            return {}
        payload = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _work_item_id_for_task(task: Task) -> str:
    for value in (task.vm_task_id, task.task_id, task.request_summary):
        match = re.search(r"g1q3[-_]rca[-_]issue[-_]intake[-_](\d{6,})", str(value or ""), re.I)
        if match:
            return match.group(1)
        match = re.search(r"(?:issue[-_]intake|work[-_]item|issue)[-_](\d{6,})", str(value or ""), re.I)
        if match:
            return match.group(1)
    for value in (task.request_summary, task.vm_task_id, task.task_id):
        match = re.search(r"\b(\d{10,})\b", str(value or ""))
        if match:
            return match.group(1)
    return ""


def _read_vm_json_file(vm_path: str, *, max_bytes: int = 2 * 1024 * 1024) -> dict[str, Any]:
    path = str(vm_path or "").strip()
    if not path.startswith("/mnt/tmp/") or ".." in Path(path).parts:
        return {}
    agent = str(Path.home() / ".local" / "bin" / "ssh-mini-agent")
    try:
        proc = subprocess.run(
            [agent, "read_file", path],
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=20,
        )
    except (OSError, subprocess.SubprocessError):
        return {}
    if proc.returncode != 0 or len(proc.stdout.encode("utf-8", errors="ignore")) > max_bytes:
        return {}
    try:
        payload = json.loads(proc.stdout)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _strip_numbered_read_file_output(text: str) -> str:
    lines: list[str] = []
    for line in str(text or "").splitlines():
        prefix, sep, rest = line.partition("|")
        if sep and prefix.strip().isdigit():
            lines.append(rest)
        else:
            lines.append(line)
    return "\n".join(lines)


def _safe_pipeline_state_path(artifact_root: str) -> str:
    root = str(artifact_root or "").strip()
    if not root.startswith("/mnt/tmp/"):
        return ""
    parts = Path(root).parts
    if ".." in parts:
        return ""
    return root.rstrip("/") + "/pipeline_state.json"


def _read_vm_pipeline_state_json(artifact_root: str) -> tuple[dict[str, Any], str | None]:
    pipeline_state_path = _safe_pipeline_state_path(artifact_root)
    if not pipeline_state_path:
        return {}, "pipeline_state artifact_root unavailable"
    agent = str(Path.home() / ".local" / "bin" / "ssh-mini-agent")
    try:
        proc = subprocess.run(
            [agent, "read_file", pipeline_state_path, "--start", "1", "--lines", str(PIPELINE_STATE_READ_LINES)],
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=20,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return {}, f"pipeline_state read failed: {type(exc).__name__}"
    if proc.returncode != 0:
        return {}, (proc.stderr.strip() or f"pipeline_state read_file failed rc={proc.returncode}")[:300]
    try:
        parsed = json.loads(_strip_numbered_read_file_output(proc.stdout))
    except json.JSONDecodeError as exc:
        return {}, f"pipeline_state json parse failed: {exc.__class__.__name__}"
    if not isinstance(parsed, dict):
        return {}, "pipeline_state root is not object"
    return parsed, None


def _normalize_pipeline_stage(stage: Any) -> str:
    text = str(stage or "").strip().lower()
    if not text:
        return ""
    definition = PIPELINE_STAGE_DEFINITIONS.get(text)
    if isinstance(definition, dict):
        return str(definition.get("stage") or "").strip()
    return text


def _pipeline_state_terminal_completed(pipeline_state: dict[str, Any]) -> bool:
    stages = _pipeline_state_stage_items(pipeline_state)
    if not stages:
        return False
    if not all(str(item.get("status") or "").strip().lower() == "completed" for item in stages):
        return False
    exit_obj = pipeline_state.get("exit") if isinstance(pipeline_state.get("exit"), dict) else {}
    return "code" in exit_obj and exit_obj.get("code") is not None


def _pipeline_state_stage_items(pipeline_state: dict[str, Any]) -> list[dict[str, Any]]:
    raw_stages = pipeline_state.get("stages") if isinstance(pipeline_state, dict) else None
    if isinstance(raw_stages, list):
        out: list[dict[str, Any]] = []
        for item in raw_stages:
            if isinstance(item, dict):
                out.append(dict(item))
        return out
    if isinstance(raw_stages, dict):
        out = []
        for stage_name, item in raw_stages.items():
            if isinstance(item, dict):
                copied = dict(item)
                copied.setdefault("stage", str(stage_name))
                out.append(copied)
        return out
    return []


def _progress_from_pipeline_state(pipeline_state: dict[str, Any], *, observed_at: str | None = None) -> dict[str, Any]:
    if not isinstance(pipeline_state, dict) or _pipeline_state_terminal_completed(pipeline_state):
        return {}
    stages = _pipeline_state_stage_items(pipeline_state)
    running: list[dict[str, Any]] = [
        item for item in stages
        if str(item.get("status") or "").strip().lower() == "running"
    ]
    if not running:
        return {}
    raw_stage = str(running[-1].get("stage") or running[-1].get("name") or "").strip()
    norm_stage = _normalize_pipeline_stage(raw_stage)
    if norm_stage not in PIPELINE_RUNNING_STAGES:
        return {}
    label = PIPELINE_STAGE_LABELS.get(norm_stage, "")
    if not label:
        return {}
    ts = (
        str(running[-1].get("updated_at") or running[-1].get("started_at") or "").strip()
        or str(pipeline_state.get("updated_at") or pipeline_state.get("generated_at") or "").strip()
        or observed_at
        or _now_iso()
    )
    return {
        "status": "running",
        "stage": norm_stage,
        "stage_label": label,
        "source": PIPELINE_STATE_SOURCE,
        "updated_at": ts,
        # feishu_task_card._milestone_from_progress consumes phase/message/ts
        # (not stage/stage_label), so keep both projection and render fields in
        # the same progress object.
        "phase": norm_stage,
        "message": label,
        "ts": ts,
    }


def _artifact_root_for_pipeline_probe(payload: dict[str, Any]) -> str:
    paths = payload.get("paths") if isinstance(payload.get("paths"), dict) else {}
    bridge = payload.get("vm_bridge") if isinstance(payload.get("vm_bridge"), dict) else {}
    for candidate in (
        paths.get("artifact_root_vm"),
        bridge.get("work_tmp_dir"),
        payload.get("artifact_root"),
    ):
        root = str(candidate or "").strip()
        if root.startswith("/mnt/tmp/"):
            return root
    contract = payload.get("delivery_contract") if isinstance(payload.get("delivery_contract"), dict) else {}
    artifacts = contract.get("artifacts") if isinstance(contract.get("artifacts"), dict) else {}
    root = str(artifacts.get("task_root_vm") or "").strip()
    return root if root.startswith("/mnt/tmp/") else ""


def _append_pipeline_progress_event(body: dict[str, Any], progress: dict[str, Any]) -> None:
    if not isinstance(progress, dict) or not progress.get("message"):
        return
    events = body.get("recent_events") if isinstance(body.get("recent_events"), list) else []
    event = {
        "phase": str(progress.get("phase") or progress.get("stage") or "").strip(),
        "summary": str(progress.get("message") or progress.get("stage_label") or "").strip(),
        "source": PIPELINE_STATE_SOURCE,
        "ts": str(progress.get("ts") or progress.get("updated_at") or _now_iso()).strip(),
    }
    key = (event["phase"], event["summary"], event["source"])
    deduped: list[dict[str, Any]] = []
    for item in events:
        if not isinstance(item, dict):
            continue
        item_key = (
            str(item.get("phase") or "").strip(),
            str(item.get("summary") or item.get("event") or item.get("message") or "").strip(),
            str(item.get("source") or "").strip(),
        )
        if item_key == key:
            continue
        deduped.append(item)
    deduped.append(event)
    body["recent_events"] = deduped[-20:]


def _apply_pipeline_progress_to_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    state_obj = payload.get("state") if isinstance(payload.get("state"), dict) else {}
    if bool(state_obj.get("terminal")) or str(state_obj.get("value") or "").strip().lower() in {"completed", "failed", "cancelled", "abandoned", "blocked"}:
        return {}
    artifact_root = _artifact_root_for_pipeline_probe(payload)
    pipeline_state, error = _read_vm_pipeline_state_json(artifact_root)
    if error or not pipeline_state:
        return {}
    progress = _progress_from_pipeline_state(pipeline_state, observed_at=_now_iso())
    if not progress:
        return {}
    bridge = payload.get("vm_bridge") if isinstance(payload.get("vm_bridge"), dict) else {}
    bridge = dict(bridge)
    bridge["progress"] = progress
    payload["vm_bridge"] = bridge
    return progress


def _write_pipeline_progress_to_sidecar(task_id: str, progress: dict[str, Any]) -> None:
    if not progress:
        return
    path = sidecar_path(task_id)
    body = _load_existing(path)
    bridge = body.get("vm_bridge") if isinstance(body.get("vm_bridge"), dict) else {}
    bridge = dict(bridge)
    bridge["progress"] = progress
    body["vm_bridge"] = bridge
    _append_pipeline_progress_event(body, progress)
    _atomic_write_json(path, body)



def _g1q3_blocked_keyframe_kind(kind: Any) -> bool:
    text = str(kind or "").strip().lower()
    return bool(text and ("frame_id" in text or "keyframe" in text or "missing_signal" in text))


def _g1q3_blocked_keyframe_drift(contract: dict[str, Any], pipeline_result: dict[str, Any]) -> tuple[bool, list[str]]:
    """Return whether a blocked-keyframe pipeline truth disagrees with contract projection."""
    if not isinstance(pipeline_result, dict) or str(pipeline_result.get("status") or "").strip() != "blocked":
        return False, []
    blocker = pipeline_result.get("blocker") if isinstance(pipeline_result.get("blocker"), dict) else {}
    if not _g1q3_blocked_keyframe_kind(blocker.get("kind")):
        return False, []
    verification = contract.get("verification") if isinstance(contract.get("verification"), dict) else {}
    user_action = contract.get("user_action") if isinstance(contract.get("user_action"), dict) else {}
    report = contract.get("report") if isinstance(contract.get("report"), dict) else {}
    checks = {
        "business_state": (str(contract.get("business_state") or "").strip(), "blocked_need_keyframe"),
        "presentation_state": (str(contract.get("presentation_state") or "").strip(), "blocked"),
        "human_action_kind": (str(contract.get("human_action_kind") or "").strip(), "need_keyframe"),
        "verification.pipeline_status": (str(verification.get("pipeline_status") or "").strip(), "blocked"),
        "verification.terminal_state": (str(verification.get("terminal_state") or "").strip(), "need_keyframe"),
        "report.status": (str(report.get("status") or "").strip(), "need_keyframe"),
    }
    reasons = [f"{where}:{got}->{want}" for where, (got, want) in checks.items() if got and got != want]
    if user_action.get("requires_user_input") is not True:
        reasons.append("user_action.requires_user_input:false->true")
    return bool(reasons), reasons


def _pipeline_result_for_delivery_contract(contract: dict[str, Any], payload_or_contract: dict[str, Any] | None = None) -> dict[str, Any]:
    """Load authoritative G1Q3 pipeline_result for a delivery contract.

    Live contracts may have laundered verification.pipeline_status; the VM
    pipeline_result.json remains the execution truth.  Tests may pass an already
    loaded pipeline_result in the same payload, but production still self-loads
    from artifacts.task_root_vm.
    """
    payload = payload_or_contract if isinstance(payload_or_contract, dict) else {}
    for candidate in (payload.get("pipeline_result"), contract.get("pipeline_result")):
        if isinstance(candidate, dict) and candidate:
            return candidate
    artifacts = contract.get("artifacts") if isinstance(contract.get("artifacts"), dict) else {}
    task_root_vm = str(artifacts.get("task_root_vm") or "").strip().rstrip("/")
    return _read_vm_json_file(task_root_vm + "/pipeline_result.json") if task_root_vm else {}

def _contract_is_report_ready(contract: dict[str, Any], *, work_item_id: str) -> bool:
    if str(contract.get("schema_version") or "") != "g1q3_delivery_contract_v1":
        return False
    if work_item_id and str(contract.get("work_item_id") or "").strip() != work_item_id:
        return False
    report = contract.get("report") if isinstance(contract.get("report"), dict) else {}
    artifacts = contract.get("artifacts") if isinstance(contract.get("artifacts"), dict) else {}
    index_html = str(artifacts.get("index_html_vm") or "").strip()
    legacy_primary = str(artifacts.get("primary_report_vm") or "").strip()
    if not index_html and legacy_primary.lower().endswith("/index.html"):
        index_html = legacy_primary
    has_surface = bool(index_html or foxglove_delivery_fields(artifacts)["foxglove_url"])
    return (
        str(contract.get("business_state") or "").strip() in {"report_completed", "final_closed"}
        and report.get("is_deliverable") is True
        and has_surface
    )


def _latest_governance_report_contract_for_task(task: Task) -> dict[str, Any]:
    """Find report-ready datapipe output for the same issue even when follow-up submit failed.

    The governance coordinator first runs the deterministic datapipe under a
    trigger-specific /mnt/tmp slug, then submits a normal read-only VM task to
    update the original Feishu card.  If that follow-up submit is denied or
    otherwise fails, the report exists but no shared-state VM task points at it.
    Use the host governance ledger as the stitch-back index and accept only a
    matching report-ready delivery_contract from VM /mnt/tmp.
    """
    if task.chat_id != G1Q3_RCA_CHAT_ID:
        return {}
    work_item_id = _work_item_id_for_task(task)
    if not work_item_id:
        return {}
    root = get_hermes_home() / "pnc_agent" / "governance_rca"
    if not root.is_dir():
        return {}
    candidates: list[tuple[str, dict[str, Any]]] = []
    for params_path in root.glob(f"*{work_item_id}*.json"):
        params = _read_json_file(params_path, max_bytes=5 * 1024 * 1024)
        if str(params.get("work_item_id") or "").strip() != work_item_id:
            continue
        artifact_root = str(params.get("artifact_root") or "").strip().rstrip("/") + "/"
        if not artifact_root.startswith("/mnt/tmp/"):
            continue
        contract = _read_vm_json_file(artifact_root + "delivery_contract.json")
        if not _contract_is_report_ready(contract, work_item_id=work_item_id):
            continue
        generated_at = str(contract.get("generated_at") or "")
        candidates.append((generated_at, contract))
    if not candidates:
        return {}
    return sorted(candidates, key=lambda item: item[0])[-1][1]


def _shared_result_report_ready(vm_task_id: str) -> dict[str, Any]:
    task_dir = get_hermes_home() / "runtime" / "shared-state" / "tasks" / vm_task_id
    result = _read_json_file(task_dir / "result.md")
    if isinstance(result, dict):
        summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
        verification = result.get("verification") if isinstance(result.get("verification"), dict) else {}
        artifacts = result.get("artifacts") if isinstance(result.get("artifacts"), dict) else {}
        terminal = str(summary.get("terminal_state") or result.get("rca_terminal_state") or "").strip()
        pipeline_status = str(summary.get("pipeline_status") or result.get("pipeline_status") or "").strip()
        checks = verification.get("checks") if isinstance(verification.get("checks"), list) else []
        check_ok = {str(item.get("name") or ""): bool(item.get("ok")) for item in checks if isinstance(item, dict)}
        if terminal == "report_ready" or pipeline_status in {"report_generated_need_review", "report_ready"}:
            index_html_vm = str(artifacts.get("index_html_vm") or "").strip()
            viz_fields = foxglove_delivery_fields(artifacts)
            html_verified = bool(index_html_vm) and (
                not checks or (check_ok.get("index_html_exists_nonempty") and check_ok.get("report_data_exists_nonempty"))
            )
            viz_verified = bool(viz_fields["foxglove_url"]) and (
                not checks or check_ok.get("viz_mcap_exists_nonempty") is True
            )
            if html_verified or viz_verified:
                return {"source": "result_md", "artifacts": artifacts, "summary": summary}
    meta = _read_json_file(task_dir / "meta.json")
    artifact_root = str(meta.get("artifact_root") or "").strip()
    if artifact_root:
        pipeline = _read_json_file(Path(artifact_root) / "pipeline_result.json")
        if str(pipeline.get("rca_terminal_state") or "").strip() == "report_ready" or str(pipeline.get("status") or "").strip() in {"report_generated_need_review", "report_ready"}:
            index_html = str(pipeline.get("index_html") or "").strip()
            viz_url = foxglove_delivery_fields({"viz_mcap_vm": pipeline.get("viz_mcap") or pipeline.get("viz_mcap_vm")})["foxglove_url"]
            if index_html or viz_url:
                return {"source": "pipeline_result", "pipeline": pipeline}
    return {}


def _g1q3_intake_needs_download(task: "Task") -> bool:
    """True when a completed G1Q3-RCA task only passed the read-only gate.

    A stale S1 `ready_to_download` token in log.md must not override a later
    report_ready result.md/pipeline_result.
    """
    vm_task_id = str(task.vm_task_id or task.task_id or "").strip()
    if not vm_task_id:
        return False
    if _shared_result_report_ready(vm_task_id):
        return False
    log_path = get_hermes_home() / "runtime" / "shared-state" / "tasks" / vm_task_id / "log.md"
    try:
        text = log_path.read_text(encoding="utf-8", errors="ignore")[-220_000:]
    except OSError:
        return False
    import re as _re
    return bool(_re.search(r"ready_to_download|need_evidence|need_source_or_evidence|requires_download", text, _re.I))


def _task_store_path() -> Path:
    return get_hermes_home() / "analytics" / "tasks.db"


class SingleRunLock:
    def __init__(self, path: Path):
        self.path = path
        self.handle = None
        self.acquired = False

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("w", encoding="utf-8")
        try:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.acquired = False
        else:
            self.acquired = True
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.handle is not None:
            if self.acquired:
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            self.handle.close()


def iter_candidate_tasks(
    *,
    store: TaskStore,
    chat_ids: Iterable[str] = DEFAULT_CHAT_IDS,
    limit: int = 50,
    include_terminal: bool = False,
) -> list[Task]:
    seen: set[str] = set()
    candidates: list[Task] = []
    statuses = list(TaskStatus) if include_terminal else [TaskStatus.RUNNING, TaskStatus.PENDING]
    for chat_id in chat_ids:
        for status in statuses:
            for task in store.list_recent(limit=limit, status=status, platform="feishu", chat_id=chat_id):
                # Only explicit VM/shared-state handoffs are syncable.  Do not
                # guess from task_id, otherwise stale pre-bridge chat tasks cause
                # repeated remote reads with no canonical VM task.
                vm_task_id = str(task.vm_task_id or "").strip()
                if not vm_task_id or vm_task_id in seen:
                    continue
                seen.add(vm_task_id)
                candidates.append(task)
    return sorted(candidates, key=lambda task: float(task.started_at or 0), reverse=True)[:limit]


def _now_iso() -> str:
    return datetime.now(PNC_FEISHU_BUSINESS_TZ).isoformat()




def _terminal_task_status(payload: dict[str, Any]) -> TaskStatus | None:
    state_obj = payload.get("state") if isinstance(payload.get("state"), dict) else {}
    state = str(state_obj.get("value") or "").strip().lower()
    terminal = bool(state_obj.get("terminal")) or state in {"completed", "failed", "cancelled", "abandoned", "blocked"}
    if not terminal:
        return None
    if state == "completed":
        return TaskStatus.COMPLETED
    if state == "cancelled":
        return TaskStatus.CANCELLED
    if state in {"failed", "abandoned", "blocked"}:
        return TaskStatus.FAILED
    return None


def _parse_payload_updated_at(payload: dict[str, Any]) -> float | None:
    for value in (payload.get("updated_at"), (payload.get("state") or {}).get("updated_at") if isinstance(payload.get("state"), dict) else None):
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str) and value.strip():
            try:
                return datetime.fromisoformat(value.strip().replace("Z", "+00:00")).timestamp()
            except ValueError:
                continue
    return None


def _sync_taskstore_terminal_status(store: TaskStore, task: Task, payload: dict[str, Any]) -> TaskStatus | None:
    new_status = _terminal_task_status(payload)
    if new_status is None:
        return None
    if task.status in {TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED}:
        return None
    task.status = new_status
    task.completed_at = task.completed_at or _parse_payload_updated_at(payload) or datetime.now(timezone.utc).timestamp()
    state_obj = payload.get("state") if isinstance(payload.get("state"), dict) else {}
    if new_status == TaskStatus.FAILED and not task.error_message:
        task.error_message = str(state_obj.get("summary") or (payload.get("vm_bridge") or {}).get("summary") or "VM task reached failed terminal state")
    store.upsert(task)
    return new_status



def _report_internal_http_link(index_html: str) -> str:
    """Return VM-internal report HTTP URL used only as an upload source.

    This URL is intentionally not user-facing: 192.168.26.174 is the RCA VM,
    not a stable all-user report endpoint.  User delivery must use Feishu
    attachment URLs returned by ``_feishu_report_attachment_link``.
    """
    from urllib.parse import quote

    path = str(index_html or "").strip()
    if not path.startswith(REPORT_FS_PREFIX):
        return ""
    rel = path[len(REPORT_FS_PREFIX):].lstrip("/")
    base = os.getenv("HERMES_G1Q3_REPORT_INTERNAL_HTTP_BASE", REPORT_INTERNAL_HTTP_BASE).rstrip("/")
    return base + "/" + quote(rel, safe="/._-")


def _report_attachment_ledger_path() -> Path:
    return get_hermes_home() / "pnc_agent" / "quota" / "g1q3_report_attachments.json"


def _load_report_attachment_ledger() -> dict[str, Any]:
    path = _report_attachment_ledger_path()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _save_report_attachment_ledger(payload: dict[str, Any]) -> None:
    path = _report_attachment_ledger_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def _feishu_report_attachment_link(*, work_item_id: str, vm_task_id: str, index_html: str) -> str:
    """Upload report HTML as a Feishu Project attachment and return file_url.

    Production delivery cannot expose ``file://`` or VM/private-IP HTTP links:
    Feishu clients do not open ``file://`` and VM 192.168.x.x reachability is
    not guaranteed for every user.  The project attachment URL is the stable,
    clickable, permission-scoped user-facing link.
    """
    work_item_id = str(work_item_id or "").strip()
    index_html = str(index_html or "").strip()
    if not work_item_id or not index_html:
        return ""

    key = f"{work_item_id}|{index_html}"
    ledger = _load_report_attachment_ledger()
    entry = (ledger.get("reports") or {}).get(key)
    if (
        isinstance(entry, dict)
        and str(entry.get("file_url") or "").startswith("https://project.feishu.cn/")
        and str(entry.get("delivery_codec_version") or "") == REPORT_ATTACHMENT_CODEC_VERSION
    ):
        return str(entry.get("file_url"))

    source_url = _report_internal_http_link(index_html)
    if not source_url:
        return ""

    try:
        from gateway.pnc_issue_context import default_meegle_runner

        safe_issue = "".join(ch for ch in work_item_id if ch.isalnum() or ch in {"-", "_"}) or "g1q3"
        filename = f"{safe_issue}_RCA_report.html"
        with tempfile.TemporaryDirectory(prefix="g1q3_report_attach_") as tmpdir:
            local_path = Path(tmpdir) / filename
            with urllib.request.urlopen(source_url, timeout=20) as response:
                data = response.read()
            if not data.startswith(UTF8_BOM):
                data = UTF8_BOM + data
            local_path.write_bytes(data)
            rc, out, err = default_meegle_runner([
                "attachment", "+upload", str(local_path),
                "--project-key", "t03o4q",
                "--work-item-id", work_item_id,
                "--resource-type", "13",
                "--filename", filename,
                "--content-type", "text/html; charset=utf-8",
                "--format", "json",
            ])
        if rc != 0:
            return ""
        payload = json.loads(out or "{}")
        file_url = str(payload.get("file_url") or "").strip()
        file_token = str(payload.get("file_token") or "").strip()
        if not file_url.startswith("https://project.feishu.cn/"):
            return ""
        ledger.setdefault("reports", {})[key] = {
            "work_item_id": work_item_id,
            "vm_task_id": vm_task_id,
            "index_html": index_html,
            "file_url": file_url,
            "file_token": file_token,
            "delivery_codec_version": REPORT_ATTACHMENT_CODEC_VERSION,
            "uploaded_at": _now_iso(),
        }
        _save_report_attachment_ledger(ledger)
        return file_url
    except (OSError, TimeoutError, urllib.error.URLError, json.JSONDecodeError):
        return ""




def _delivery_from_contract(task: Task, payload_or_contract: dict[str, Any]) -> dict[str, Any]:
    """Map VM-produced g1q3_delivery_contract_v1 into task_card.delivery.

    This is the preferred producer/consumer path for G1Q3 RCA delivery.  It
    keeps vm-task-sync aligned with the relay and prevents the 120s sync writer
    from reasserting stale generic completed/need_download states.
    """
    contract = payload_or_contract.get("delivery_contract") if isinstance(payload_or_contract.get("delivery_contract"), dict) else payload_or_contract
    if not isinstance(contract, dict) or str(contract.get("schema_version") or "").strip() != "g1q3_delivery_contract_v1":
        return {}
    report = contract.get("report") if isinstance(contract.get("report"), dict) else {}
    artifacts = contract.get("artifacts") if isinstance(contract.get("artifacts"), dict) else {}
    summary = contract.get("summary") if isinstance(contract.get("summary"), dict) else {}
    verification = contract.get("verification") if isinstance(contract.get("verification"), dict) else {}
    user_action = contract.get("user_action") if isinstance(contract.get("user_action"), dict) else {}
    toolchain = contract.get("toolchain") if isinstance(contract.get("toolchain"), dict) else {}
    business_state = str(contract.get("business_state") or "").strip()
    translate_baseline = str(toolchain.get("translate_baseline") or contract.get("translate_baseline") or "").strip()
    translate_contract_path = str(toolchain.get("translate_contract_path") or contract.get("translate_contract_path") or "").strip()
    translate_blockers = toolchain.get("translate_blockers") if isinstance(toolchain.get("translate_blockers"), list) else []
    work_item_id = str(contract.get("work_item_id") or "").strip()
    index_html = str(artifacts.get("index_html_vm") or "").strip()
    primary_report_vm = str(artifacts.get("primary_report_vm") or "").strip()
    if not index_html and primary_report_vm.lower().endswith("/index.html"):
        index_html = primary_report_vm
    causal_text = str(artifacts.get("attribution_causal_text") or summary.get("short_conclusion") or "").strip()
    viz_fields = foxglove_delivery_fields(artifacts, causal_text=causal_text)
    report_ready = (
        business_state in {"report_completed", "final_closed"}
        and report.get("is_deliverable") is True
        and bool(index_html or viz_fields["foxglove_url"])
    )

    boundaries = contract.get("evidence_boundary") if isinstance(contract.get("evidence_boundary"), list) else []
    boundaries = [str(item).strip() for item in boundaries if str(item).strip()]
    if report_ready:
        html_link = _report_internal_http_link(index_html)
        attribution = str(verification.get("pipeline_status") or report.get("status") or "report_generated_need_review").strip()
        # candidate cause must be the report's crisp conclusion, never summary.l0 (a status line);
        # omit the line when absent rather than showing a stale intake/status summary as the cause.
        candidate_cause = str(summary.get("short_conclusion") or "").strip()
        reviewer = str(report.get("candidate_owner") or report.get("candidate_owner_domain") or "").strip()
        delivery_status = "report_ready" if viz_fields["foxglove_url"] else "html_delivery_ready"
        conclusion_parts = [f"{work_item_id} RCA 报告已生成" if work_item_id else "RCA 报告已生成"]
        conclusion_parts.append(f"报告状态：{delivery_status}")
        conclusion_parts.append("归因状态：hypothesis_ready" if report.get("is_candidate") is not False else "归因状态：reviewed")
        if candidate_cause:
            conclusion_parts.append(f"候选原因：{candidate_cause}")
        if reviewer:
            conclusion_parts.append(f"责任候选：{reviewer}")
        case_dir_vm = str(artifacts.get("case_dir_vm") or "").strip()
        report_data_vm = str(artifacts.get("report_data_vm") or "").strip()
        html_fallback = html_link or ((str(artifacts.get("primary_report_cifs") or "").strip() or None) if index_html else None)
        primary_delivery_link = viz_fields["foxglove_url"] or html_fallback
        return {
            "conclusion": "；".join(part for part in conclusion_parts if part),
            "artifact_path": primary_delivery_link,
            "artifact_label": "打开 foxglove 可视化" if viz_fields["foxglove_url"] else "打开 HTML 报告",
            "artifact_root": str(artifacts.get("case_dir_cifs") or artifacts.get("case_dir_vm") or "").strip() or None,
            "artifact_cifs": str(artifacts.get("case_dir_cifs") or "").strip() or None,
            "agent_artifact_root_vm": str(artifacts.get("task_root_vm") or "").strip() or None,
            "agent_artifact_root_cifs": str(artifacts.get("task_root_cifs") or "").strip() or None,
            "business_case_dir_vm": case_dir_vm or None,
            "business_case_dir_cifs": str(artifacts.get("case_dir_cifs") or _perception_test_team_cifs(case_dir_vm) or "").strip() or None,
            "report_index_html_vm": index_html or None,
            "report_index_html_cifs": (str(artifacts.get("primary_report_cifs") or _perception_test_team_cifs(index_html) or "").strip() or None) if index_html else None,
            "report_data_vm": report_data_vm or None,
            "report_data_cifs": str(artifacts.get("report_data_cifs") or _perception_test_team_cifs(report_data_vm) or "").strip() or None,
            "boundaries": boundaries,
            "next_options": [],
            "attribution_status": "hypothesis_ready" if report.get("is_candidate") is not False else "reviewed",
            "report_status": delivery_status,
            "candidate_cause": candidate_cause,
            **viz_fields,
            "responsibility_candidate": reviewer or None,
            "source": "delivery_contract_v1",
            "translate_baseline": translate_baseline or None,
            "translate_contract_path": translate_contract_path or None,
            "translate_blockers": translate_blockers,
            "rca_status": {
                "work_item_id": work_item_id,
                "attribution_status": "hypothesis_ready" if report.get("is_candidate") is not False else "reviewed",
                "report_status": delivery_status,
                "html_link": html_link,
                "foxglove_url": viz_fields["foxglove_url"],
                "attribution_causal_text": viz_fields["attribution_causal_text"],
                "candidate_cause": candidate_cause,
                "candidate_responsibility": reviewer,
            },
        }

    if business_state == "not_admissible" or str(contract.get("presentation_state") or "").strip() == "declined":
        reason = str(user_action.get("next_action_text") or user_action.get("next_action") or summary.get("l0") or "不在 G1Q3 RCA 自动受理范围").strip()
        blocker = contract.get("blocker") if isinstance(contract.get("blocker"), dict) else {}
        project = str(blocker.get("project") or "").strip()
        if project and "实际项目" not in reason:
            reason = f"{reason}；实际项目=「{project}」"
        return {
            "conclusion": "不予自动受理/转人工：问题所属项目未命中 G1Q3 受理范围",
            "report_status": "out_of_scope",
            "attribution_status": "not_applicable",
            "artifact_path": None,
            "artifact_root": str(artifacts.get("task_root_cifs") or artifacts.get("task_root_vm") or "").strip() or None,
            "agent_artifact_root_vm": str(artifacts.get("task_root_vm") or "").strip() or None,
            "agent_artifact_root_cifs": str(artifacts.get("task_root_cifs") or "").strip() or None,
            "boundaries": boundaries or ([reason] if reason else []),
            "next_options": [],
            "source": "delivery_contract_v1",
            "human_action_kind": str(contract.get("human_action_kind") or "need_triage").strip(),
            "business_state": business_state,
            "presentation_state": str(contract.get("presentation_state") or "declined").strip(),
            "translate_baseline": translate_baseline or None,
            "translate_contract_path": translate_contract_path or None,
            "translate_blockers": translate_blockers,
        }

    if business_state == "blocked_need_keyframe" or str(contract.get("human_action_kind") or "").strip() == "need_keyframe":
        blocker = contract.get("blocker") if isinstance(contract.get("blocker"), dict) else {}
        message = str(blocker.get("message") or user_action.get("next_action_text") or user_action.get("next_action") or "自动找帧无候选；需人工补帧").strip()
        stage = str(verification.get("pipeline_stage") or "s45_auto_keyframe").strip()
        blocked_boundaries = [
            f"卡点：{message}",
            "数据已就位，无需重传；需人工指定关键帧或确认 discover_acc_speed_unstable 所需信号是否采集",
        ]
        return {
            "conclusion": f"数据已下载，自动找帧无候选（{message}）→ 已停在 {stage}，需人工指定关键帧或确认采集信号后继续 RCA",
            "report_status": "need_keyframe",
            "attribution_status": "need_keyframe",
            "artifact_path": None,
            "artifact_label": "产物目录(暂无报告)",
            "artifact_root": str(artifacts.get("task_root_cifs") or artifacts.get("task_root_vm") or "").strip() or None,
            "agent_artifact_root_vm": str(artifacts.get("task_root_vm") or "").strip() or None,
            "agent_artifact_root_cifs": str(artifacts.get("task_root_cifs") or "").strip() or None,
            "cifs_status": "暂无报告；数据产物已就位，需补关键帧后生成 RCA 报告",
            "boundaries": (boundaries + [item for item in blocked_boundaries if item not in boundaries]) if boundaries else blocked_boundaries,
            "next_options": [],
            "source": "delivery_contract_v1",
            "human_action_kind": "need_keyframe",
            "business_state": "blocked_need_keyframe",
            "presentation_state": str(contract.get("presentation_state") or "blocked").strip(),
            "blocker_kind": str(blocker.get("kind") or "").strip() or None,
            "translate_baseline": translate_baseline or None,
            "translate_contract_path": translate_contract_path or None,
            "translate_blockers": translate_blockers,
        }

    pipeline_result = _pipeline_result_for_delivery_contract(contract, payload_or_contract)
    pipeline_blocker = pipeline_result.get("blocker") if isinstance(pipeline_result.get("blocker"), dict) else {}
    pipeline_blocker_kind = str(pipeline_blocker.get("kind") or "").strip()
    drift, drift_reasons = _g1q3_blocked_keyframe_drift(contract, pipeline_result)
    if drift and str(pipeline_result.get("status") or "").strip() == "blocked" and _g1q3_blocked_keyframe_kind(pipeline_blocker_kind):
        message = str(pipeline_blocker.get("message") or "自动找帧无候选；需人工补帧").strip()
        stage = str(pipeline_result.get("stage") or "s45_auto_keyframe").strip()
        blocked_boundaries = [
            f"卡点：{message}",
            "数据已就位，无需重传；需人工指定关键帧或确认 discover_acc_speed_unstable 所需信号是否采集",
        ]
        blocked_boundaries.append("contract_laundered_blocked_as_awaiting：契约投影与 pipeline_result 真相不一致，已按 pipeline_result 覆盖")
        if drift_reasons:
            blocked_boundaries.append("contract_drift_fields：" + ", ".join(drift_reasons[:6]))
        return {
            "conclusion": f"数据已下载，自动找帧无候选（{message}）→ 已停在 {stage}，需人工指定关键帧或确认采集信号后继续 RCA",
            "report_status": "need_keyframe",
            "attribution_status": "need_keyframe",
            "artifact_path": None,
            "artifact_label": "产物目录(暂无报告)",
            "artifact_root": str(artifacts.get("task_root_cifs") or artifacts.get("task_root_vm") or "").strip() or None,
            "agent_artifact_root_vm": str(artifacts.get("task_root_vm") or "").strip() or None,
            "agent_artifact_root_cifs": str(artifacts.get("task_root_cifs") or "").strip() or None,
            "cifs_status": "暂无报告；数据产物已就位，需补关键帧后生成 RCA 报告",
            "boundaries": (boundaries + [item for item in blocked_boundaries if item not in boundaries]) if boundaries else blocked_boundaries,
            "next_options": [],
            "source": "pipeline_result_truth_override",
            "human_action_kind": "need_keyframe",
            "business_state": "blocked_need_keyframe",
            "presentation_state": "blocked",
            "blocker_kind": pipeline_blocker_kind,
            "translate_baseline": translate_baseline or None,
            "translate_contract_path": translate_contract_path or None,
            "translate_blockers": translate_blockers,
        }

    if business_state == "awaiting_download" or (report.get("status") == "need_download" and user_action.get("requires_user_input") is False):
        reason = str(user_action.get("next_action_text") or user_action.get("next_action") or summary.get("l0") or "已受理；数据待自动下载/解析，无需发起人补数据").strip()
        return {
            "conclusion": "数据下载执行中（pipeline running）；等待解析完成后更新 RCA 状态",
            "report_status": "need_download",
            "attribution_status": "need_evidence",
            "artifact_path": None,
            "artifact_root": str(artifacts.get("task_root_cifs") or artifacts.get("task_root_vm") or "").strip() or None,
            "agent_artifact_root_vm": str(artifacts.get("task_root_vm") or "").strip() or None,
            "agent_artifact_root_cifs": str(artifacts.get("task_root_cifs") or "").strip() or None,
            "boundaries": boundaries or ([reason] if reason else []),
            "next_options": [],
            "source": "delivery_contract_v1",
            "human_action_kind": "none",
            "business_state": "awaiting_download",
            "presentation_state": str(contract.get("presentation_state") or "processing").strip(),
            "translate_baseline": translate_baseline or None,
            "translate_contract_path": translate_contract_path or None,
            "translate_blockers": translate_blockers,
        }

    if business_state in {"missing_user_input", "data_required", "intake_validated"} or user_action.get("requires_user_input") is True:
        reason = str(user_action.get("next_action_text") or user_action.get("next_action") or summary.get("l0") or "待下载/解析数据后再出 RCA 结论").strip()
        return {
            "conclusion": "intake 与准入校验完成；需要发起人补充缺失字段后再继续 RCA",
            "report_status": "need_user_data",
            "artifact_path": None,
            "artifact_root": str(artifacts.get("task_root_cifs") or artifacts.get("task_root_vm") or "").strip() or None,
            "agent_artifact_root_vm": str(artifacts.get("task_root_vm") or "").strip() or None,
            "agent_artifact_root_cifs": str(artifacts.get("task_root_cifs") or "").strip() or None,
            "boundaries": boundaries or ([reason] if reason else []),
            "next_options": [],
            "source": "delivery_contract_v1",
            "translate_baseline": translate_baseline or None,
            "translate_contract_path": translate_contract_path or None,
            "translate_blockers": translate_blockers,
        }
    return {}

def _extract_g1q3_delivery_from_result(task: Task, state: str) -> dict[str, Any]:
    """Return Feishu-card delivery fields for a completed G1Q3 RCA task.

    VM G1Q3 tasks may short-circuit at S1 when a report already exists.  In that
    case the generic artifact list only exposes pipeline_result.json while the
    user-facing RCA status, attribution state, and HTML report link live inside
    s1_gate/rca_execution_result.json.
    """
    if task.chat_id != G1Q3_RCA_CHAT_ID or state != "completed":
        return {}
    vm_task_id = str(task.vm_task_id or task.task_id or "").strip()
    if not vm_task_id:
        return {}
    host_task_dir = get_hermes_home() / "runtime" / "shared-state" / "tasks" / vm_task_id
    result = _read_json_file(host_task_dir / "result.md")
    task_dir = Path.home() / "Mounts" / "mini_root" / ".hermes" / "shared-state" / "tasks" / vm_task_id
    if not result:
        try:
            result = vm_task_completion_probe._read_rca_execution_result(vm_task_id, task_dir)
        except Exception:
            result = {}
    if isinstance(result, dict) and result.get("schema_version") == "shared_state_worker_result_v1":
        summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
        observation = result.get("rca_observation") if isinstance(result.get("rca_observation"), dict) else {}
        artifacts = result.get("artifacts") if isinstance(result.get("artifacts"), dict) else {}
        result_verification = result.get("verification") if isinstance(result.get("verification"), dict) else {}
        checks = result_verification.get("checks") if isinstance(result_verification.get("checks"), list) else []
        check_ok = {str(item.get("name") or ""): bool(item.get("ok")) for item in checks if isinstance(item, dict)}
        terminal = str(summary.get("terminal_state") or "").strip()
        pipeline_status = str(summary.get("pipeline_status") or "").strip()
        index_html = str(artifacts.get("index_html_vm") or "").strip()
        shared_causal_text = str(artifacts.get("attribution_causal_text") or observation.get("short_conclusion") or "").strip()
        viz_fields = foxglove_delivery_fields(artifacts, causal_text=shared_causal_text)
        html_verified = bool(index_html) and (
            not checks or (check_ok.get("index_html_exists_nonempty") and check_ok.get("report_data_exists_nonempty"))
        )
        viz_verified = bool(viz_fields["foxglove_url"]) and (
            not checks or check_ok.get("viz_mcap_exists_nonempty") is True
        )
        if not viz_verified:
            viz_fields = {**viz_fields, "viz_mcap_vm": "", "foxglove_url": ""}
        if (
            terminal == "report_ready" or pipeline_status in {"report_generated_need_review", "report_ready"}
        ) and (html_verified or viz_verified):
            work_item_id = str(result.get("work_item_id") or "").strip()
            attribution = str(summary.get("attribution_status") or observation.get("status") or "hypothesis_ready").strip()
            cause = str(observation.get("short_conclusion") or "").strip()
            reviewer = str(observation.get("candidate_owner_domain") or "").strip()
            html_link = _report_internal_http_link(index_html)
            delivery_status = "report_ready" if viz_fields["foxglove_url"] else "html_delivery_ready"
            conclusion_parts = [f"{work_item_id} RCA 报告已生成" if work_item_id else "RCA 报告已生成"]
            if attribution:
                conclusion_parts.append(f"归因状态：{attribution}")
            conclusion_parts.append(f"报告状态：{delivery_status}")
            if cause:
                conclusion_parts.append(f"候选原因：{cause}")
            boundaries = []
            boundary = str(observation.get("high_confidence_boundary") or "").strip()
            if boundary:
                boundaries.append(boundary)
            case_dir_vm = str(artifacts.get("case_dir_vm") or "").strip()
            index_html_vm = str(artifacts.get("index_html_vm") or "").strip()
            report_data_vm = str(artifacts.get("report_data_vm") or "").strip()
            return {
                "conclusion": "；".join(part for part in conclusion_parts if part),
                "artifact_path": viz_fields["foxglove_url"] or html_link or None,
                "artifact_label": "打开 foxglove 可视化" if viz_fields["foxglove_url"] else "打开 HTML 报告",
                "artifact_root": str(artifacts.get("artifact_root_cifs") or artifacts.get("artifact_root_vm") or "").strip() or None,
                "agent_artifact_root_vm": str(artifacts.get("artifact_root_vm") or "").strip() or None,
                "agent_artifact_root_cifs": str(artifacts.get("artifact_root_cifs") or "").strip() or None,
                "business_case_dir_vm": case_dir_vm or None,
                "business_case_dir_cifs": _perception_test_team_cifs(case_dir_vm) or None,
                "report_index_html_vm": index_html_vm or None,
                "report_index_html_cifs": _perception_test_team_cifs(index_html_vm) or None,
                "report_data_vm": report_data_vm or None,
                "report_data_cifs": _perception_test_team_cifs(report_data_vm) or None,
                "boundaries": boundaries,
                "next_options": [],
                "attribution_status": attribution,
                "report_status": delivery_status,
                "candidate_cause": cause,
                **viz_fields,
                "responsibility_candidate": reviewer or None,
                "rca_status": {
                    "work_item_id": work_item_id,
                    "attribution_status": attribution,
                    "report_status": delivery_status,
                    "html_link": html_link,
                    "foxglove_url": viz_fields["foxglove_url"],
                    "attribution_causal_text": viz_fields["attribution_causal_text"],
                    "candidate_cause": cause,
                    "candidate_responsibility": reviewer,
                },
            }
    if not isinstance(result, dict) or result.get("schema_version") != "g1q3_rca_execution_result_v1":
        return {}

    status_summary = result.get("status_summary") if isinstance(result.get("status_summary"), dict) else {}
    artifacts = result.get("artifacts") if isinstance(result.get("artifacts"), dict) else {}
    best = artifacts.get("best") if isinstance(artifacts.get("best"), dict) else {}
    review_payload = best.get("review_payload") if isinstance(best.get("review_payload"), dict) else {}
    readback = result.get("readback") if isinstance(result.get("readback"), dict) else {}

    work_item_id = str(result.get("work_item_id") or "").strip()
    attribution = str(status_summary.get("attribution_status") or best.get("report_status") or "").strip()
    report_status = str(status_summary.get("report_status") or best.get("html_validation_state") or "").strip()
    cause = str(review_payload.get("candidate_cause") or "").strip()
    reviewer = str(review_payload.get("candidate_responsibility") or "").strip()
    viz_artifacts = dict(artifacts)
    viz_artifacts["viz_mcap_vm"] = str(best.get("viz_mcap_vm") or best.get("viz_mcap") or artifacts.get("viz_mcap_vm") or "").strip()
    viz_artifacts["attribution_causal_text"] = str(best.get("attribution_causal_text") or artifacts.get("attribution_causal_text") or cause or "").strip()
    viz_fields = foxglove_delivery_fields(viz_artifacts, causal_text=cause)

    index_html = str(best.get("index_html") or "").strip()
    if not index_html:
        text = str(readback.get("text") or result.get("l1") or "")
        import re
        match = re.search(r"file://hfs\.minieye\.tech/department-perception_test_team/([^\s)]+)", text)
        if match:
            from urllib.parse import unquote
            index_html = REPORT_FS_PREFIX + unquote(match.group(1)).lstrip("/")

    html_link = _report_internal_http_link(index_html)
    if not (index_html or viz_fields["foxglove_url"]):
        return {}
    if viz_fields["foxglove_url"]:
        report_status = "report_ready"

    conclusion_parts = []
    if work_item_id:
        conclusion_parts.append(f"{work_item_id} RCA 报告已生成")
    else:
        conclusion_parts.append("RCA 报告已生成")
    if attribution:
        conclusion_parts.append(f"归因状态：{attribution}")
    if report_status:
        conclusion_parts.append(f"报告状态：{report_status}")
    if cause:
        conclusion_parts.append(f"候选原因：{cause}")
    if reviewer:
        conclusion_parts.append(f"责任候选：{reviewer}")

    boundaries: list[str] = []
    if review_payload.get("review_reason"):
        boundaries.append(str(review_payload.get("review_reason")).strip())
    missing = review_payload.get("missing_evidence") if isinstance(review_payload.get("missing_evidence"), list) else []
    for item in missing[:2]:
        text_item = str(item or "").strip()
        if text_item:
            boundaries.append(text_item)

    artifact_path = viz_fields["foxglove_url"] or html_link
    return {
        "conclusion": "；".join(part for part in conclusion_parts if part),
        "artifact_path": artifact_path or None,
        "artifact_label": "打开 foxglove 可视化" if viz_fields["foxglove_url"] else "打开 HTML 报告",
        "boundaries": boundaries,
        "next_options": [],
        "report_status": report_status,
        "candidate_cause": cause,
        **viz_fields,
        "rca_status": {
            "work_item_id": work_item_id,
            "attribution_status": attribution,
            "report_status": report_status,
            "html_link": html_link,
            "foxglove_url": viz_fields["foxglove_url"],
            "attribution_causal_text": viz_fields["attribution_causal_text"],
            "candidate_cause": cause,
            "candidate_responsibility": reviewer,
        },
    }


def _joined_payload_text(payload: dict[str, Any], *extra: Any) -> str:
    parts: list[str] = []
    for value in extra:
        if value:
            parts.append(str(value))
    for key in ("summary", "result", "log", "latest_summary", "current_phase"):
        value = payload.get(key) if isinstance(payload, dict) else None
        if isinstance(value, (dict, list)):
            try:
                parts.append(json.dumps(value, ensure_ascii=False))
            except Exception:
                parts.append(str(value))
        elif value:
            parts.append(str(value))
    bridge = payload.get("vm_bridge") if isinstance(payload, dict) and isinstance(payload.get("vm_bridge"), dict) else {}
    for key in ("summary", "state"):
        if bridge.get(key):
            parts.append(str(bridge.get(key)))
    return "\n".join(parts)


def _task_card_for_task(task: Task, payload: dict[str, Any]) -> dict[str, Any]:
    state_obj = payload.get("state") if isinstance(payload.get("state"), dict) else {}
    bridge = payload.get("vm_bridge") if isinstance(payload.get("vm_bridge"), dict) else {}
    state = str(state_obj.get("value") or bridge.get("state") or "running").strip().lower()
    terminal = bool(state_obj.get("terminal")) or state in {"completed", "failed", "cancelled", "abandoned", "blocked"}
    user_state = "done" if state == "completed" else "failed" if terminal else "running"
    summary = str(state_obj.get("summary") or bridge.get("summary") or task.request_summary or "").strip()
    artifacts = payload.get("artifacts") if isinstance(payload.get("artifacts"), list) else []
    artifact_path = str(artifacts[0]) if artifacts else str(bridge.get("user_visible_path") or "")
    delivery = _delivery_from_contract(task, payload)
    if delivery and delivery.get("report_status") not in {"html_delivery_ready", "report_ready", "report_generated_need_review"}:
        stitched_contract = _latest_governance_report_contract_for_task(task)
        if stitched_contract:
            stitched_delivery = _delivery_from_contract(task, stitched_contract)
            if stitched_delivery:
                delivery = stitched_delivery
    if delivery and delivery.get("report_status") == "need_download":
        user_state = "in_progress"
    elif delivery and delivery.get("report_status") == "need_keyframe":
        user_state = "awaiting_user"
    elif delivery and delivery.get("report_status") in {"html_delivery_ready", "report_ready", "report_generated_need_review"}:
        user_state = "done"
    if not delivery:
        delivery = _extract_g1q3_delivery_from_result(task, state)
    intake_needs_download = (
        not delivery
        and state == "completed"
        and task.chat_id == G1Q3_RCA_CHAT_ID
        and _g1q3_intake_needs_download(task)
    )
    if intake_needs_download:
        # Gate-only intake (ready_to_download / need_evidence): no report, no
        # attribution.  Render an honest need-download state so this writer
        # agrees with the completion-notice relay instead of re-asserting done.
        user_state = "in_progress"
        delivery = {
            "conclusion": "intake 与准入校验完成；尚未下载/解析数据，未生成 RCA 报告（gate=ready_to_download）",
            "report_status": "need_download",
            "artifact_path": None,
            "boundaries": [],
            "next_options": [],
        }
    elif not delivery:
        delivery = {
            "conclusion": summary if terminal else None,
            "artifact_path": artifact_path or None,
            "boundaries": [],
            "next_options": [],
        }
    card = {
        "schema_version": 1,
        "task_id": task.task_id,
        "vm_task_id": task.vm_task_id,
        "chat_id": task.chat_id,
        "thread_id": task.thread_id,
        "user_state": user_state,
        "milestones": [],
        "pending_confirms": [],
        "delivery": delivery,
    }
    delivery_report_status = str((delivery or {}).get("report_status") or "") if isinstance(delivery, dict) else ""
    g1q3_evidence = (
        task.chat_id == G1Q3_RCA_CHAT_ID
        and (
            delivery_report_status in {"html_delivery_ready", "report_ready", "report_generated_need_review", "need_download", "need_keyframe", "out_of_scope", "not_admissible"}
            or intake_needs_download
            or isinstance(payload.get("delivery_contract"), dict)
            or isinstance(payload.get("pipeline_result"), dict)
            or "g1q3-rca" in str(task.task_id or task.vm_task_id or "")
        )
    )
    if g1q3_evidence:
        pipeline_result = payload.get("pipeline_result") if isinstance(payload.get("pipeline_result"), dict) else {}
        report_ready = delivery_report_status in {"html_delivery_ready", "report_ready", "report_generated_need_review"}
        projection_contract = {**(delivery if isinstance(delivery, dict) else {}), "created_at": getattr(task, "created_at", None) or "", "updated_at": payload.get("updated_at") or ""}
        if isinstance(payload.get("delivery_contract"), dict):
            projection_contract.update(payload["delivery_contract"])
        if pipeline_result:
            projection_contract["pipeline_result"] = pipeline_result
        projection = derive_presentation(
            state,
            projection_contract,
            {
                "real_report": report_ready,
                "has_deliverable_report": report_ready,
                "report_status": delivery_report_status,
                "pipeline_result": pipeline_result,
            },
            _joined_payload_text(payload, summary),
            bridge,
        )
        card["presentation"] = projection
        card["user_state"] = str(projection.get("user_state") or card["user_state"])
        if isinstance(card.get("delivery"), dict):
            preserve_delivery_truth = card["delivery"].get("source") == "pipeline_result_truth_override" or delivery_report_status in {"need_keyframe", "html_delivery_ready", "report_ready", "report_generated_need_review"}
            if not preserve_delivery_truth:
                card["delivery"]["conclusion"] = str(projection.get("conclusion") or card["delivery"].get("conclusion") or "")
            if not (preserve_delivery_truth and delivery_report_status == "report_ready"):
                card["delivery"]["report_status"] = str(projection.get("report_status") or card["delivery"].get("report_status") or "")
            if projection.get("missing_reason"):
                card["delivery"]["missing_reason"] = str(projection.get("missing_reason") or "")
            if not preserve_delivery_truth:
                card["delivery"]["human_action_kind"] = str(projection.get("human_action_kind") or "none")
            card["delivery"]["action_category"] = str(projection.get("action_category") or "none")
            card["delivery"]["cifs_status"] = str(projection.get("cifs_status") or card["delivery"].get("cifs_status") or "")
    card["status_line"] = render_status_line(card)
    return card



def _maybe_report_comment_for_task(task: Task, task_card: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any] | None:
    state_obj = payload.get("state") if isinstance(payload.get("state"), dict) else {}
    bridge = payload.get("vm_bridge") if isinstance(payload.get("vm_bridge"), dict) else {}
    state = str(state_obj.get("value") or bridge.get("state") or "").strip().lower()
    if task.chat_id != G1Q3_RCA_CHAT_ID or state != "completed":
        return None
    delivery = task_card.get("delivery") if isinstance(task_card.get("delivery"), dict) else {}
    rca_status = delivery.get("rca_status") if isinstance(delivery.get("rca_status"), dict) else {}
    work_item_id = str(rca_status.get("work_item_id") or "").strip()
    if not work_item_id or not rca_status.get("html_link"):
        return None
    updated_at = str(payload.get("updated_at") or state_obj.get("updated_at") or "").strip()
    try:
        from gateway.pnc_report_comment import is_after_not_before, maybe_comment_report_ready
        if not is_after_not_before(updated_at):
            return {"action": "skipped_not_before", "work_item_id": work_item_id, "updated_at": updated_at}
        return maybe_comment_report_ready(
            project_key="t03o4q",
            work_item_id=work_item_id,
            task_id=str(task.vm_task_id or task.task_id),
            title=str(task.request_summary or "")[:180],
            rca_status=rca_status,
            boundaries=delivery.get("boundaries") if isinstance(delivery.get("boundaries"), list) else [],
            ledger_dir=get_hermes_home() / "pnc_agent" / "quota",
        )
    except Exception as exc:
        return {"action": "comment_failed", "work_item_id": work_item_id, "reason": f"{type(exc).__name__}: {exc}"[:200]}



def _first_cifs_path(values: list[Any], fallback: str = "") -> str:
    candidates = [str(item).strip() for item in values if str(item).strip()]
    if fallback:
        candidates.append(str(fallback).strip())
    for value in candidates:
        if value.startswith("CIFS: "):
            value = value.split("CIFS: ", 1)[1].strip()
        if value.startswith("//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/"):
            return value
    return ""


def _completion_delivery_contract() -> dict[str, Any]:
    return {
        "required": True,
        "mode": "thread_reply",
        "must_carry": ["conclusion", "cause", "fixed_state", "html_url", "verification"],
        "at_originator": True,
        "max_attempts": 3,
    }


def _structured_delivery_text(*, task: Task, state: str, summary: str, artifact_path: str, errors: list[Any] | None = None) -> str:
    conclusion = summary.strip() or f"任务 {task.task_id} 已到达终态：{state or 'unknown'}"
    path = artifact_path.strip() or "本次没有登记可直接领取的产物路径。"
    raw_errors = [str(item).strip() for item in (errors or []) if str(item).strip()]
    boundary = "；".join(raw_errors[:3]) if raw_errors else "未登记额外边界；以卡片交付区和 VM 产物为准。"
    next_step = "如需继续，我会按这条任务的话题上下文接着处理。" if state == "completed" else "我会保留失败点；需要重试或改方案时直接在话题里说。"
    return "\n".join([
        "结论：" + conclusion,
        "路径：" + path,
        "边界：" + boundary,
        "下一步：" + next_step,
        "conclusion：" + conclusion,
        "cause：" + boundary,
        "fixed_state：" + (state or "unknown"),
        "html_url：" + path,
        "verification：以 VM pipeline_state / report_data / HTML 产物为准；终态仍需人工复核业务结论。",
    ])

def _completion_notice_for_task(task: Task, payload: dict[str, Any]) -> dict[str, Any] | None:
    state_obj = payload.get("state") if isinstance(payload.get("state"), dict) else {}
    state = str(state_obj.get("value") or "").strip().lower()
    terminal = bool(state_obj.get("terminal")) or state in {"completed", "failed", "cancelled", "abandoned", "blocked"}
    if not terminal:
        return None

    vm_task_id = str(task.vm_task_id or task.task_id).strip()
    text = ""
    source = "generic"
    if task.chat_id == G1Q3_RCA_CHAT_ID:
        task_dir = Path.home() / "Mounts" / "mini_root" / ".hermes" / "shared-state" / "tasks" / vm_task_id
        try:
            text = vm_task_completion_probe._g1q3_l1_notice(vm_task_id, task_dir, state) or ""
        except Exception:
            text = ""
        if text:
            source = "g1q3_l1_notice"

    summary = str(state_obj.get("summary") or (payload.get("vm_bridge") or {}).get("summary") or "VM task reached terminal state")
    artifacts = payload.get("artifacts") if isinstance(payload.get("artifacts"), list) else []
    bridge = payload.get("vm_bridge") if isinstance(payload.get("vm_bridge"), dict) else {}
    artifact_path = _first_cifs_path(artifacts, str(bridge.get("user_visible_path") or ""))
    delivery = _delivery_from_contract(task, payload) if task.chat_id == G1Q3_RCA_CHAT_ID else {}
    blocked_need_keyframe = str(delivery.get("report_status") or "").strip() == "need_keyframe"
    if blocked_need_keyframe:
        state = "blocked"
        summary = str(delivery.get("conclusion") or summary)
        artifact_path = ""
    if not text:
        text = _structured_delivery_text(
            task=task,
            state=state or "unknown",
            summary=summary,
            artifact_path=artifact_path,
            errors=payload.get("errors") if isinstance(payload.get("errors"), list) else [],
        )

    return {
        "schema_version": 1,
        "generated_at": _now_iso(),
        "source": source,
        "state": state or "unknown",
        "text": text,
        "send_status": "pending",
        "chat_id": task.chat_id,
        "thread_id": task.thread_id,
        "message_id": task.message_id,
        "vm_task_id": vm_task_id,
        "completion_delivery": _completion_delivery_contract(),
    }


def _write_completion_notice(task: Task, payload: dict[str, Any]) -> dict[str, Any] | None:
    notice = _completion_notice_for_task(task, payload)
    if not notice:
        return None
    path = sidecar_path(task.task_id)
    body = _load_existing(path)
    existing = body.get("completion_notice") if isinstance(body.get("completion_notice"), dict) else {}
    if existing.get("send_status") in {"sent", "acknowledged"}:
        notice["send_status"] = existing.get("send_status")
        if existing.get("sent_at"):
            notice["sent_at"] = existing.get("sent_at")
    existing_delivery_contract = existing.get("completion_delivery") if isinstance(existing.get("completion_delivery"), dict) else None
    if existing_delivery_contract:
        notice["completion_delivery"] = existing_delivery_contract
    body["completion_notice"] = notice
    proposed_card = _task_card_for_task(task, payload)
    report_comment = _maybe_report_comment_for_task(task, proposed_card, payload)
    if report_comment is not None:
        body["report_comment"] = report_comment
    # Move3 step-1: relay is the sole task_card mutator.  vm_task_sync only
    # publishes VM-side facts that relay cannot derive locally; it must not
    # merge/preserve/update task_card itself, which was the cross-process flap
    # source.  Existing task_card remains byte-for-byte under relay ownership.
    body["vm_delivery_proposal"] = {
        "schema_version": 1,
        "source": "pnc_vm_task_sync",
        "generated_at": _now_iso(),
        "vm_task_id": task.vm_task_id or task.task_id,
        "delivery": proposed_card.get("delivery") if isinstance(proposed_card.get("delivery"), dict) else {},
        "user_state": str(proposed_card.get("user_state") or "").strip(),
        "status_line": str(proposed_card.get("status_line") or "").strip(),
        "vm_task_state": payload.get("state") if isinstance(payload.get("state"), dict) else {},
        "heartbeat": (
            payload.get("updated_at")
            or ((payload.get("state") or {}).get("updated_at") if isinstance(payload.get("state"), dict) else None)
        ),
        "artifacts_ref": payload.get("artifacts") if isinstance(payload.get("artifacts"), list) else [],
    }
    _atomic_write_json(path, body)
    return notice


def sync_pnc_vm_tasks(
    *,
    limit: int = 50,
    chat_ids: Iterable[str] = DEFAULT_CHAT_IDS,
    include_terminal: bool = False,
    dry_run: bool = False,
    no_artifacts: bool = False,
    shared_state_root: str | None = None,
    ssh_mini_agent: str | None = None,
) -> dict[str, Any]:
    store = TaskStore(_task_store_path())
    tasks = iter_candidate_tasks(store=store, chat_ids=chat_ids, limit=limit, include_terminal=include_terminal)
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    for task in tasks:
        vm_task_id = str(task.vm_task_id or task.task_id).strip()
        row: dict[str, Any] = {
            "task_id": task.task_id,
            "vm_task_id": vm_task_id,
            "chat_id": task.chat_id,
            "status": task.status.value,
        }
        if dry_run:
            row["dry_run"] = True
            rows.append(row)
            continue
        try:
            collect_kwargs: dict[str, Any] = {"include_artifacts": not no_artifacts}
            if shared_state_root is not None:
                collect_kwargs["shared_state_root"] = shared_state_root
            if ssh_mini_agent is not None:
                collect_kwargs["ssh_mini_agent"] = ssh_mini_agent
            payload = collect_vm_task_status(vm_task_id, **collect_kwargs)
            pipeline_progress = _apply_pipeline_progress_to_payload(payload)
            sidecar_path = write_collected_status_to_sidecar(task.task_id, payload)
            _write_pipeline_progress_to_sidecar(task.task_id, pipeline_progress)
            completion_notice = _write_completion_notice(task, payload)
            taskstore_status = _sync_taskstore_terminal_status(store, task, payload)
            row.update({
                "synced": True,
                "state": (payload.get("state") or {}).get("value"),
                "terminal": (payload.get("state") or {}).get("terminal"),
                "sidecar_path": str(sidecar_path),
                "artifact_count": len(payload.get("artifacts") or []),
                "collector_errors": payload.get("errors") or [],
                "pipeline_progress_stage": pipeline_progress.get("stage") if pipeline_progress else None,
                "completion_notice": bool(completion_notice),
                "taskstore_status_synced": taskstore_status.value if taskstore_status else None,
            })
        except Exception as exc:  # pragma: no cover - defensive path still surfaced in JSON
            message = f"{vm_task_id}: {type(exc).__name__}: {exc}"
            errors.append(message)
            row.update({"synced": False, "error": message})
        rows.append(row)
    return {
        "ok": not errors,
        "candidate_count": len(tasks),
        "synced_count": sum(1 for row in rows if row.get("synced")),
        "dry_run": dry_run,
        "rows": rows,
        "errors": errors,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--chat-id", action="append", default=[])
    parser.add_argument("--include-terminal", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-artifacts", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--no-lock", action="store_true")
    parser.add_argument("--lock-root")
    parser.add_argument("--shared-state-root")
    parser.add_argument("--ssh-mini-agent")
    args = parser.parse_args(argv)

    hermes_home = Path(get_hermes_home()).expanduser().resolve(strict=False)
    if args.lock_root:
        lock_root = Path(args.lock_root).expanduser().resolve(strict=False)
        try:
            lock_root.relative_to(hermes_home)
        except ValueError:
            parser.error("--lock-root must stay within HERMES_HOME")
    else:
        lock_root = hermes_home / "locks"
    lock_path = lock_root / "pnc-vm-task-sync.lock"
    sync_kwargs = {
        "limit": max(1, min(args.limit, 200)),
        "chat_ids": args.chat_id or DEFAULT_CHAT_IDS,
        "include_terminal": args.include_terminal,
        "dry_run": args.dry_run,
        "no_artifacts": args.no_artifacts,
        "shared_state_root": args.shared_state_root,
        "ssh_mini_agent": args.ssh_mini_agent,
    }
    if args.no_lock:
        result = sync_pnc_vm_tasks(**sync_kwargs)
    else:
        with SingleRunLock(lock_path) as lock:
            if not lock.acquired:
                result = {
                    "ok": True,
                    "skipped": True,
                    "reason": "another pnc_vm_task_sync run is active",
                    "candidate_count": 0,
                    "synced_count": 0,
                    "dry_run": args.dry_run,
                    "rows": [],
                    "errors": [],
                }
            else:
                result = sync_pnc_vm_tasks(**sync_kwargs)
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(f"[{'OK' if result['ok'] else 'ERROR'}] PNC VM task sync: {result['synced_count']}/{result['candidate_count']} synced")
        for row in result["rows"]:
            print(f"- {row['task_id']} -> {row['vm_task_id']}: {row.get('state') or row.get('status')}")
        for error in result["errors"]:
            print(f"error: {error}")
    return 0 if result["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
