#!/usr/bin/env python3
"""Compatibility helpers for the retired G1Q3-RCA download coordinator.

RCA production input now enters through either the Kafka workflow-event consumer
or an authorized explicit group ``@`` request.  Both converge on the same
admission path and use the pinned ``pdcl_pyclip`` remote reader.  This module
remains importable only because the gateway still reuses its one-card sidecar
helper.  All former download/dispatch entry points are permanently fail-closed.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

FLAG_ENV = "G1Q3_GOVERNANCE_DOWNLOAD_ENABLED"
RETIRED_ERROR_CODE = "g1q3_rca_legacy_download_coordinator_retired"
RETIRED_ERROR = (
    "legacy G1Q3 RCA download coordinator is retired; use unified Kafka/manual "
    "admission -> pdcl_pyclip remote-read -> fixed RCA service path"
)
PNC_FEISHU_BUSINESS_TZ = timezone(timedelta(hours=8))
G1Q3_RCA_CHAT_ID = "oc_6cfc782212009ff4cd815349909dd423"


def _now_iso() -> str:
    return datetime.now(PNC_FEISHU_BUSINESS_TZ).isoformat()


def governance_download_enabled(env: Dict[str, str] | None = None) -> bool:
    """The retired path cannot be re-enabled through environment settings."""
    del env
    return False


def build_datapipe_bash(**_kwargs: Any) -> str:
    """Reject compatibility callers before any command can be constructed."""
    raise RuntimeError(RETIRED_ERROR)


def dispatch_datapipe(**_kwargs: Any) -> Dict[str, Any]:
    """Reject compatibility callers without touching SSH, VM, or task state."""
    return {
        "ok": False,
        "stage": "retired",
        "error_code": RETIRED_ERROR_CODE,
        "error": RETIRED_ERROR,
        "side_effects_suppressed": True,
    }


def _sidecar_path_for_task(task_id: str) -> Path:
    from scripts.vm_task_state_bridge import sidecar_path

    return sidecar_path(task_id)


def _load_sidecar(task_id: str) -> dict[str, Any]:
    try:
        payload = json.loads(_sidecar_path_for_task(task_id).read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return {}


def _write_sidecar(task_id: str, body: dict[str, Any]) -> Path:
    from scripts.vm_task_state_bridge import _atomic_write_json

    path = _sidecar_path_for_task(task_id)
    _atomic_write_json(path, body)
    return path


def governance_sidecar_exists(task_id: str) -> bool:
    """True when the early acceptance card/sidecar already owns this followup id."""
    body = _load_sidecar(task_id)
    if not body:
        return False
    task_card = body.get("task_card") if isinstance(body.get("task_card"), dict) else {}
    if task_card.get("card_message_id") or task_card.get("one_card_policy") or body.get("governance_early_card"):
        return True
    return False


def early_progress_payload(*, ts: str | None = None) -> dict[str, Any]:
    now = ts or _now_iso()
    return {
        "status": "running",
        "stage": "dispatched",
        "stage_label": "已受理，远程读取管线启动中",
        "source": "host_dispatch",
        "updated_at": now,
        "phase": "dispatched",
        "message": "已受理，远程读取管线启动中",
        "ts": now,
    }


def seed_governance_early_card(
    *,
    task_id: str,
    chat_id: str,
    message_id: str,
    thread_id: str = "",
    artifact_root: str = "",
    artifact_cifs_root: str = "",
    submit_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create/update the host sidecar for the early one-card acceptance state.

    This is intentionally sidecar-only: relay remains the sole Feishu card
    writer.  The caller may have already called vm_task_submit once; this helper
    records that result but never submits/delivers anything itself.
    """
    task_id = str(task_id or "").strip()
    if not task_id:
        return {"ok": False, "error": "missing_task_id"}
    now = _now_iso()
    progress = early_progress_payload(ts=now)
    body = _load_sidecar(task_id)
    bridge = body.get("vm_bridge") if isinstance(body.get("vm_bridge"), dict) else {}
    bridge = dict(bridge)
    bridge.update({
        "state": "running",
        "vm_task_id": task_id,
        "work_tmp_dir": artifact_root,
        "user_visible_path": artifact_cifs_root,
        "progress": progress,
    })
    body["vm_bridge"] = bridge
    events = body.get("recent_events") if isinstance(body.get("recent_events"), list) else []
    event = {"phase": "dispatched", "summary": progress["message"], "source": "host_dispatch", "ts": now}
    events = [item for item in events if not (isinstance(item, dict) and item.get("phase") == "dispatched" and item.get("source") == "host_dispatch")]
    events.append(event)
    body["recent_events"] = events[-20:]
    card = body.get("task_card") if isinstance(body.get("task_card"), dict) else {}
    card = dict(card)
    card.update({
        "schema_version": 1,
        "task_id": task_id,
        "vm_task_id": task_id,
        "chat_id": chat_id,
        "thread_id": thread_id,
        "message_id": message_id,
        "user_state": "pending",
        "status_line": "已受理，远程读取管线启动中",
        "one_card_policy": True,
        "created_at": card.get("created_at") or now,
        "delivery": {
            **(card.get("delivery") if isinstance(card.get("delivery"), dict) else {}),
            "report_status": "in_progress",
            "artifact_root": artifact_cifs_root or artifact_root,
            "artifact_label": "产物目录(暂无报告)",
            "cifs_status": "未落地/不适用",
            "conclusion": "已受理，远程读取管线启动中；当前尚无可交付 RCA 报告。",
        },
    })
    body["task_card"] = card
    body["governance_early_card"] = {
        "created_at": now,
        "source": "gateway_dispatch",
        "submit_success": bool((submit_result or {}).get("success")) if isinstance(submit_result, dict) else False,
    }
    if isinstance(submit_result, dict):
        body["governance_early_submit"] = {
            "success": bool(submit_result.get("success")),
            "task_id": task_id,
            "notify_process": submit_result.get("notify_process") if isinstance(submit_result.get("notify_process"), dict) else {},
            "bridge_delivery": ((submit_result.get("task") or {}).get("bridge_delivery") if isinstance(submit_result.get("task"), dict) else None),
        }
    body["updated_at"] = now
    path = _write_sidecar(task_id, body)
    return {"ok": True, "path": str(path), "progress": progress}


def _read_vm_json_file(vm_path: str) -> dict[str, Any]:
    path = str(vm_path or "").strip()
    if not path.startswith("/mnt/tmp/") or ".." in Path(path).parts:
        return {}
    agent = str(Path.home() / ".local" / "bin" / "ssh-mini-agent")
    try:
        proc = subprocess.run(
            [agent, "read_file", path, "--start", "1", "--lines", "800"],
            capture_output=True,
            text=True,
            timeout=20,
        )
    except (OSError, subprocess.SubprocessError):
        return {}
    if proc.returncode != 0:
        return {}
    lines: list[str] = []
    for line in (proc.stdout or "").splitlines():
        prefix, sep, rest = line.partition("|")
        lines.append(rest if sep and prefix.strip().isdigit() else line)
    try:
        payload = json.loads("\n".join(lines))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _delivery_from_followup_contract(params: dict[str, Any], followup: dict[str, Any]) -> dict[str, Any]:
    contract = _read_vm_json_file(str(params.get("artifact_root") or "").rstrip("/") + "/delivery_contract.json")
    if contract:
        try:
            from scripts.pnc_vm_task_sync import _delivery_from_contract

            task = SimpleNamespace(
                task_id=params.get("followup_task_id", ""),
                vm_task_id=params.get("followup_task_id", ""),
                chat_id=G1Q3_RCA_CHAT_ID,
                request_summary=f"G1Q3 RCA issue intake {params.get('work_item_id') or ''}",
                thread_id="",
            )
            delivery = _delivery_from_contract(task, {"delivery_contract": contract})
            if isinstance(delivery, dict) and delivery:
                return delivery
        except Exception:
            pass
    datapipe = str(followup.get("datapipe") or "").strip()
    status = "need_pipeline_fix" if datapipe in {"failed", "dispatch_failed", "timed_out"} else "need_evidence"
    return {
        "conclusion": str(followup.get("note") or "数据管线已结束；等待只读结果确认。"),
        "report_status": status,
        "artifact_path": None,
        "artifact_root": params.get("artifact_cifs_root") or params.get("artifact_root"),
        "artifact_label": "产物目录(暂无报告)",
        "cifs_status": "未落地/不适用",
        "boundaries": [],
        "next_options": [],
        "source": "governance_followup_no_duplicate_submit",
        "human_action_kind": "none",
    }


def update_existing_governance_card(
    *,
    task_id: str,
    params: dict[str, Any],
    followup: dict[str, Any],
) -> dict[str, Any]:
    """Update the early sidecar/card without a second vm_task_submit.

    This suppresses both duplicate bridge envelopes and duplicate completion
    probes.  Relay will PATCH the same card when its hash changes.
    """
    body = _load_sidecar(task_id)
    if not body:
        return {"updated": False, "reason": "missing_sidecar"}
    now = _now_iso()
    delivery = _delivery_from_followup_contract(params, followup)
    report_status = str(delivery.get("report_status") or "")
    user_state = "done" if report_status == "html_delivery_ready" else "in_progress"
    status_line = "RCA 报告已生成。" if user_state == "done" else str(followup.get("note") or "数据管线已结束，等待结果确认。")
    body["vm_delivery_proposal"] = {
        "schema_version": 1,
        "source": "pnc_g1q3_governance_rca",
        "generated_at": now,
        "vm_task_id": task_id,
        "delivery": delivery,
        "user_state": user_state,
        "status_line": status_line,
        "vm_task_state": {"value": "completed" if user_state == "done" else "running", "terminal": user_state == "done"},
        "heartbeat": now,
        "artifacts_ref": [],
        "idempotent_update": True,
        "suppressed_deliver_bridge": True,
        "suppressed_completion_probe": True,
    }
    bridge = body.get("vm_bridge") if isinstance(body.get("vm_bridge"), dict) else {}
    bridge = dict(bridge)
    bridge["state"] = "completed" if user_state == "done" else "running"
    bridge["vm_task_id"] = task_id
    body["vm_bridge"] = bridge
    body["updated_at"] = now
    path = _write_sidecar(task_id, body)
    return {
        "updated": True,
        "path": str(path),
        "user_state": user_state,
        "report_status": report_status,
        "suppressed_deliver_bridge": True,
        "suppressed_completion_probe": True,
    }


def coordinate(params: Dict[str, Any], **_kwargs: Any) -> Dict[str, Any]:
    """Reject the removed coordinator before any task or VM side effect."""
    del params
    return {
        "ok": False,
        "stage": "retired",
        "error_code": RETIRED_ERROR_CODE,
        "error": RETIRED_ERROR,
        "side_effects_suppressed": True,
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Retired G1Q3-RCA download coordinator")
    ap.add_argument("--params-file", required=True, help="JSON file with coordinate() params")
    args = ap.parse_args(argv)
    params = json.loads(Path(args.params_file).read_text(encoding="utf-8"))
    result = coordinate(params)
    print(json.dumps(result, ensure_ascii=False))
    return 3


if __name__ == "__main__":
    raise SystemExit(main())
