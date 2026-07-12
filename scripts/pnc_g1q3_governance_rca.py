#!/usr/bin/env python3
"""Host-driven governance-path coordinator for G1Q3-RCA download-bearing intakes.

Background (2026-06-22): the VM codex worker sandbox is network-off by design
(least privilege), so S2 ``mdi download`` cannot run inside it.  The OS mount
``/mnt/tmp`` is writable and the plain-shell ssh-mini-submit "governance" path
has network egress (proven: AWB event downloaded 1.5G).  This coordinator
codifies "download/data-IO via governance path, codex stays network-off":

  1. dispatch the deterministic data pipeline (download -> materialize ->
     translate -> ... -> report) as a plain-shell governance job via
     ``ssh-mini-submit`` (network + /mnt/tmp write available there);
  2. poll it to completion (bounded);
  3. create the STANDARD read-only codex intake task (unchanged path).  Because
     the case is now materialized, the read-only gate renders report_ready; if
     the datapipe failed, it honestly renders need_download (via the shipped VM
     ``terminal_state`` contract).  Either way the Feishu card flows through the
     existing task -> codex -> card pipeline.

The whole feature is gated by ``G1Q3_GOVERNANCE_DOWNLOAD_ENABLED`` and is INERT
until that flag is set AND the gateway is restarted.  Decision logic
(command/goal builders, outcome -> follow-up) is kept pure and unit-tested
without mocking the control flow; only the ssh-mini IO is side-effectful.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

FLAG_ENV = "G1Q3_GOVERNANCE_DOWNLOAD_ENABLED"
_TRUE = {"1", "true", "yes", "on"}

# Bounded poll defaults; a real MCAP event download + translate can take minutes.
DEFAULT_POLL_TIMEOUT_SECONDS = 3600
DEFAULT_POLL_INTERVAL_SECONDS = 20
DEFAULT_MAX_DOWNLOAD_GB = 40

SSH_MINI_SUBMIT = str(Path.home() / ".local" / "bin" / "ssh-mini-submit")
SSH_MINI_RUN = str(Path.home() / ".local" / "bin" / "ssh-mini-run")
PIPELINE_PATH = "/home/mini/data3/yj-evaluation-server/api/g1q3_rca/scripts/run_rca_auto_pipeline.py"
READONLY_PATH = "/home/mini/data3/yj-evaluation-server/api/g1q3_rca/scripts/run_rca_execution_request.py"
PNC_FEISHU_BUSINESS_TZ = timezone(timedelta(hours=8))
G1Q3_RCA_CHAT_ID = "oc_6cfc782212009ff4cd815349909dd423"


def _now_iso() -> str:
    return datetime.now(PNC_FEISHU_BUSINESS_TZ).isoformat()


def governance_download_enabled(env: Dict[str, str] | None = None) -> bool:
    src = env if env is not None else os.environ
    return str(src.get(FLAG_ENV, "")).strip().lower() in _TRUE


# --------------------------------------------------------------------------
# Pure helpers (unit-tested without VM IO)
# --------------------------------------------------------------------------

def datapipe_task_id(task_slug: str) -> str:
    """Stable ssh-mini-submit task id for a given RCA task slug."""
    slug = "".join(c for c in str(task_slug or "") if c.isalnum() or c in "-_") or "unknown"
    return f"g1q3-datapipe-{slug}"


def build_datapipe_bash(*, artifact_root: str, execution_request_path: str,
                        max_download_gb: int = DEFAULT_MAX_DOWNLOAD_GB,
                        request_json: str = "",
                        translate_contract_path: str = "") -> str:
    """Plain-shell bash that runs the governed RCA data pipeline.

    Runs as a plain ssh-mini-submit job (NOT codex), so it has network + the
    now-writable /mnt/tmp.  --from-stage s2_download runs download through report.
    """
    out = str(artifact_root).rstrip("/")
    req = str(execution_request_path)
    request_b64 = base64.b64encode(str(request_json or "").encode("utf-8")).decode("ascii")
    request_bootstrap = ""
    if request_b64:
        request_bootstrap = (
            f"REQ_PATH={json.dumps(req)} REQ_B64={request_b64!r} python3 - <<'PY_REQ'\n"
            "import base64, os, pathlib\n"
            "path = pathlib.Path(os.environ['REQ_PATH'])\n"
            "path.parent.mkdir(parents=True, exist_ok=True)\n"
            "path.write_bytes(base64.b64decode(os.environ['REQ_B64']))\n"
            "path.write_bytes(path.read_bytes().rstrip(b'\\n') + b'\\n')\n"
            "PY_REQ\n"
            f"test -s {req} || {{ echo DATAPIPE_REQUEST_MISSING; exit 4; }}\n"
        )
    translate_env = ""
    if str(translate_contract_path or "").strip():
        translate_env = f"RCA_TRANSLATE_CONTRACT_PATH={json.dumps(str(translate_contract_path).strip())} "
    return (
        "set -uo pipefail\n"
        f"mkdir -p {out}/downloads || {{ echo DATAPIPE_MKDIR_FAILED; exit 3; }}\n"
        f"mkdir -p {out} || {{ echo DATAPIPE_MKDIR_FAILED; exit 3; }}\n"
        + request_bootstrap +
        "cd /home/mini/data3/yj-evaluation-server || exit 3\n"
        f"G1Q3_RCA_BRANCH_GUARD=1 {translate_env}PYTHONPATH=. python3 {PIPELINE_PATH} "
        f"--request {req} --output-dir {out}/ "
        f"--from-stage s2_download --max-download-gb {int(max_download_gb)}\n"
        "rc=$?\n"
        "echo \"DATAPIPE_EXIT=$rc\"\n"
        "exit $rc\n"
    )


def decide_followup(*, datapipe_exit: int | None, products_present: bool,
                    pipeline_status: str = "", blocker: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Decide the follow-up codex task after the datapipe job.

    Always a STANDARD read-only intake task (unchanged path): the read-only gate
    reflects reality (materialized -> report_ready; not -> need_download).  The
    decision only annotates WHY for the goal/diagnostics; it never fabricates a
    completed state when the datapipe failed.
    """
    ok = (datapipe_exit == 0)
    status = str(pipeline_status or "").strip().lower()
    if ok and status and status != "completed":
        kind = str((blocker or {}).get("kind") or status)
        return {"action": "codex_readonly", "datapipe": "blocked",
                "note": f"数据管线退出 0 但 pipeline_state={status}/{kind}；只读门禁据实渲染 need_download，不得假绿。",
                "blocker": blocker or {"kind": kind}}
    if ok and products_present:
        return {"action": "codex_readonly", "datapipe": "succeeded",
                "note": "数据管线成功，case 已 materialize；只读门禁应渲染 report_ready。"}
    if ok and not products_present:
        return {"action": "codex_readonly", "datapipe": "succeeded_no_products",
                "note": "数据管线退出 0 但未见产物；只读门禁据实渲染（多半 need_download/需人工复核）。"}
    return {"action": "codex_readonly", "datapipe": "failed",
            "note": f"数据管线失败 (exit={datapipe_exit})；只读门禁据实渲染 need_download，不得假绿。"}


def build_followup_goal(*, template_id: str, full_case_id: str, work_item_id: str,
                        source_group_id: str, message_id: str, artifact_root: str,
                        artifact_cifs_root: str, execution_request_path: str,
                        request_json: str, followup: Dict[str, Any],
                        translate_baseline: str = "production",
                        translate_contract_path: str = "") -> str:
    """Read-only intake goal for the follow-up codex task (governance variant)."""
    return (
        "执行 G1Q3 RCA 只读状态查询 handoff（治理路径数据管线后置认知）。\n"
        f"- template_id: {template_id}\n"
        f"- case_id: {full_case_id}\n"
        f"- work_item_id: {work_item_id}\n"
        f"- source_group_id: {source_group_id}\n"
        f"- request_message_id: {message_id}\n"
        "- schema_version: g1q3_rca_execution_request_v1\n"
        f"- execution_request_path: {execution_request_path}\n"
        f"- translate_baseline: {translate_baseline or 'production'}\n"
        + (f"- translate_contract_path: {translate_contract_path}\n" if translate_contract_path else "")
        + f"- datapipe_outcome: {followup.get('datapipe')}\n"
        f"- 说明：{followup.get('note')}\n"
        "- 数据下载与受控管线已在治理路径(plain shell, 有网)执行完毕；本任务在网络全关 sandbox 内"
        "只读检查 case 当前 RCA 产物/gate_result/report_data，按实际渲染，**不得在 sandbox 内下载**。\n"
        f"- VM command suggestion: python3 {READONLY_PATH} --request {execution_request_path} --output-dir {artifact_root}\n"
        "- 输出：仅 L0/L1 摘要。\n"
        f"- VM work_tmp_dir: {artifact_root}\n"
        f"- user_visible_cifs: {artifact_cifs_root}\n"
        "\n## RcaExecutionRequest JSON\n"
        f"```json\n{request_json}\n```\n"
    )


# --------------------------------------------------------------------------
# IO: dispatch + poll (side-effectful; thin)
# --------------------------------------------------------------------------

def _run(cmd: list[str], timeout: int = 60) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def dispatch_datapipe(*, task_slug: str, artifact_root: str, execution_request_path: str,
                      max_download_gb: int = DEFAULT_MAX_DOWNLOAD_GB,
                      request_json: str = "",
                      translate_contract_path: str = "",
                      home: Path | None = None) -> Dict[str, Any]:
    """Write the datapipe bash to the VM and dispatch via ssh-mini-submit."""
    home = home or Path.home()
    tid = datapipe_task_id(task_slug)
    bash = build_datapipe_bash(artifact_root=artifact_root,
                               execution_request_path=execution_request_path,
                               max_download_gb=max_download_gb,
                               request_json=request_json,
                               translate_contract_path=translate_contract_path)
    script_vm = f"/home/mini/tmp/{tid}.sh"
    agent = str(home / ".local" / "bin" / "ssh-mini-agent")
    write = subprocess.run([agent, "edit_file", script_vm], input=bash, text=True,
                           capture_output=True, timeout=60)
    if write.returncode != 0:
        return {"ok": False, "stage": "write_script", "error": (write.stderr or write.stdout)[:300]}
    submit = _run([SSH_MINI_SUBMIT, "--task-id", tid, "--work-dir", artifact_root.rstrip("/"),
                   "--queue-if-blocked", "--", "bash", script_vm], timeout=90)
    if submit.returncode != 0:
        return {"ok": False, "stage": "submit", "error": (submit.stderr or submit.stdout)[:300]}
    return {"ok": True, "task_id": tid, "submit_stdout": submit.stdout[-500:]}


def poll_datapipe_exit(*, task_slug: str, artifact_root: str,
                       timeout_seconds: int = DEFAULT_POLL_TIMEOUT_SECONDS,
                       interval_seconds: int = DEFAULT_POLL_INTERVAL_SECONDS,
                       sleep=time.sleep) -> Dict[str, Any]:
    """Poll the ssh-mini-submit job's exit.code under its work-dir."""
    exit_path = f"{artifact_root.rstrip('/')}/exit.code"
    waited = 0
    while waited <= timeout_seconds:
        proc = _run([SSH_MINI_RUN, f"cat {exit_path} 2>/dev/null || echo __none__"], timeout=40)
        out = (proc.stdout or "").strip().splitlines()
        val = out[-1].strip() if out else "__none__"
        if val.isdigit() or (val.startswith("-") and val[1:].isdigit()):
            return {"done": True, "exit": int(val)}
        sleep(interval_seconds)
        waited += interval_seconds
    return {"done": False, "exit": None, "timed_out": True}


def pipeline_state_verdict(*, artifact_root: str) -> Dict[str, Any]:
    """Read pipeline_state.json and return terminal status/blocker when present."""
    state_path = f"{artifact_root.rstrip('/')}/pipeline_state.json"
    proc = _run([SSH_MINI_RUN, f"cat {state_path} 2>/dev/null || true"], timeout=40)
    text = (proc.stdout or "").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text[text.find("{"):])
    except Exception:
        return {"status": "unparseable"}
    status = ""
    blocker: Dict[str, Any] | None = None
    if isinstance(payload, dict):
        stages = payload.get("stages")
        if isinstance(stages, dict):
            for item in stages.values():
                if isinstance(item, dict) and item.get("status") in {"blocked", "failed"}:
                    status = str(item.get("status") or "")
                    b = item.get("blocker")
                    blocker = b if isinstance(b, dict) else None
                    break
        status = status or str(payload.get("status") or payload.get("terminal_state") or "")
        b = payload.get("blocker")
        if blocker is None and isinstance(b, dict):
            blocker = b
    return {"status": status, "blocker": blocker} if status or blocker else {}


def products_present(*, artifact_root: str) -> bool:
    """Best-effort: does the materialized case carry a report under artifact_root?"""
    out = artifact_root.rstrip("/")
    proc = _run([SSH_MINI_RUN,
                 f"(ls {out}/**/index.html {out}/index.html {out}/*/report_data.json 2>/dev/null | head -1) "
                 "&& echo __present__ || echo __absent__"], timeout=40)
    return "__present__" in (proc.stdout or "")


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
        "stage_label": "已受理，数据管线启动中",
        "source": "host_dispatch",
        "updated_at": now,
        "phase": "dispatched",
        "message": "已受理，数据管线启动中",
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
        "status_line": "已受理，数据管线启动中",
        "one_card_policy": True,
        "created_at": card.get("created_at") or now,
        "delivery": {
            **(card.get("delivery") if isinstance(card.get("delivery"), dict) else {}),
            "report_status": "in_progress",
            "artifact_root": artifact_cifs_root or artifact_root,
            "artifact_label": "产物目录(暂无报告)",
            "cifs_status": "未落地/不适用",
            "conclusion": "已受理，数据管线启动中；当前尚无可交付 RCA 报告。",
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
    status = "need_download"
    user_data = False
    if datapipe in {"failed", "dispatch_failed", "timed_out"}:
        status = "need_download"
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
        "human_action_kind": "need_data" if user_data else "none",
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


def coordinate(params: Dict[str, Any], *, sleep=time.sleep) -> Dict[str, Any]:
    """End-to-end: dispatch datapipe -> poll -> create standard read-only codex task."""
    from tools.vm_task_tool import vm_task_submit  # local import; heavy deps

    task_slug = params["task_slug"]
    artifact_root = params["artifact_root"]
    execution_request_path = params["execution_request_path"]

    disp = dispatch_datapipe(task_slug=task_slug, artifact_root=artifact_root,
                             execution_request_path=execution_request_path,
                             max_download_gb=params.get("max_download_gb", DEFAULT_MAX_DOWNLOAD_GB),
                             request_json=params.get("request_json", ""),
                             translate_contract_path=params.get("translate_contract_path", ""))
    if not disp.get("ok"):
        followup = decide_followup(datapipe_exit=None, products_present=False)
        followup["datapipe"] = "dispatch_failed"
        followup["note"] = f"治理数据管线派发失败({disp.get('stage')})；只读据实渲染。"
    else:
        poll = poll_datapipe_exit(task_slug=task_slug, artifact_root=artifact_root,
                                  timeout_seconds=params.get("poll_timeout", DEFAULT_POLL_TIMEOUT_SECONDS),
                                  sleep=sleep)
        present = products_present(artifact_root=artifact_root) if poll.get("done") else False
        state = pipeline_state_verdict(artifact_root=artifact_root) if poll.get("done") else {}
        followup = decide_followup(datapipe_exit=poll.get("exit"), products_present=present,
                                   pipeline_status=str(state.get("status") or ""),
                                   blocker=state.get("blocker") if isinstance(state.get("blocker"), dict) else None)
        if poll.get("timed_out"):
            followup["datapipe"] = "timed_out"
            followup["note"] = "治理数据管线轮询超时；只读据实渲染 need_download，不得假绿。"

    goal = build_followup_goal(
        template_id=params.get("template_id", "rca_issue_intake"),
        full_case_id=params.get("full_case_id", ""),
        work_item_id=params.get("work_item_id", ""),
        source_group_id=params.get("source_group_id", ""),
        message_id=params.get("message_id", ""),
        artifact_root=artifact_root,
        artifact_cifs_root=params.get("artifact_cifs_root", ""),
        execution_request_path=execution_request_path,
        request_json=params.get("request_json", "{}"),
        followup=followup,
        translate_baseline=params.get("translate_baseline", "production"),
        translate_contract_path=params.get("translate_contract_path", ""),
    )
    followup_task_id = str(params.get("followup_task_id") or "").strip()
    existing_sidecar = bool(followup_task_id and governance_sidecar_exists(followup_task_id))
    if existing_sidecar:
        update = update_existing_governance_card(task_id=followup_task_id, params=params, followup=followup)
        submit = {
            "success": bool(update.get("updated")),
            "idempotent_update": True,
            "task": {"task_id": followup_task_id},
            "suppressed_deliver_bridge": True,
            "suppressed_completion_probe": True,
            "sidecar_update": update,
        }
    else:
        submit = vm_task_submit(
        title=params.get("title", f"G1Q3 RCA: {params.get('work_item_id') or task_slug}"),
        goal=goal,
        task_id=followup_task_id,
        user_id=params.get("user_id", ""),
        lane="standard", resource_class="pnc_data", repo_scope="unknown",
        workspace_scope="none", risk_class="normal",
        artifact_root=artifact_root, artifact_cifs_root=params.get("artifact_cifs_root", ""),
        executor_type="governed_tool", agent_backend="codex", codex_backend_enabled=True,
        )
    return {"ok": bool(submit.get("success")) if isinstance(submit, dict) else False,
            "datapipe_dispatch": disp, "followup": followup, "submit": submit}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="G1Q3-RCA governance-path datapipe coordinator")
    ap.add_argument("--params-file", required=True, help="JSON file with coordinate() params")
    args = ap.parse_args(argv)
    params = json.loads(Path(args.params_file).read_text(encoding="utf-8"))
    result = coordinate(params)
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
