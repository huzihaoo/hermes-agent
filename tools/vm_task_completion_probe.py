#!/usr/bin/env python3
"""Poll one shared-state VM task until terminal and print its user-facing result.

This wrapper is intended to be launched as a Hermes managed background process
with notify_on_complete=True.  The gateway process watcher then injects the
final stdout back into the originating Feishu topic, giving VM/shared-state
submissions the same completion-notify behavior as ordinary background tools.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.pnc_g1q3_truth import downgrade_g1q3_notice_text, reconcile_report_truth  # noqa: E402


def _load_notify_module():
    workspace_root = Path(os.environ.get("HERMES_WORKSPACE_ROOT") or Path.home() / ".hermes" / "workspace-work")
    script = workspace_root / "bin" / "notify_delegated_task_completions.py"
    sys.path.insert(0, str(script.parent))
    import importlib.util

    spec = importlib.util.spec_from_file_location("notify_delegated_task_completions", script)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load notify script: {script}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _state_for_task(root: Path, task_id: str) -> str:
    for queue in ("completed", "failed", "abandoned", "claimed", "pending"):
        dispatch = root / "dispatch" / queue / f"{task_id}.json"
        if dispatch.is_file():
            payload = _read_json(dispatch)
            return str(payload.get("state") or queue).strip().lower() or queue
    status_path = root / "tasks" / task_id / "status.md"
    if status_path.is_file():
        try:
            for line in status_path.read_text(encoding="utf-8", errors="replace").splitlines():
                stripped = line.strip()
                if stripped.startswith("- state:") or stripped.startswith("state:"):
                    status_state = stripped.split(":", 1)[1].strip().lower()
                    if status_state:
                        return status_state
        except Exception:
            pass
    meta = _read_json(root / "tasks" / task_id / "meta.json")
    return str(meta.get("state") or "").strip().lower()


def _extract_markdown_section(text: str, heading: str) -> str:
    lines = text.splitlines()
    capture = False
    captured: list[str] = []
    target = heading.strip().lower()
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("## "):
            current = stripped[3:].strip().lower()
            if capture and current != target:
                break
            capture = current == target
            continue
        if capture:
            captured.append(line.rstrip())
    return "\n".join(captured).strip()


def _handoff_contract_from_meta(meta: dict[str, Any]) -> dict[str, Any]:
    payload = meta.get("pnc_group_binding")
    if not isinstance(payload, dict):
        return {}
    handoff = payload.get("handoff_contract")
    return handoff if isinstance(handoff, dict) else {}


def _g1q3_task_label(meta: dict[str, Any], task_id: str) -> str:
    handoff = _handoff_contract_from_meta(meta)
    case_id = str(handoff.get("case_id") or "").strip()
    work_item_id = str(handoff.get("work_item_id") or "").strip()
    if case_id:
        return case_id if case_id.upper().startswith("G1Q3") else f"G1Q3-{case_id}"
    if work_item_id:
        return f"飞书问题 {work_item_id}"
    title = str(meta.get("title") or "").strip()
    import re
    issue_match = re.search(r"issue intake:\s*(\d+)", title, re.IGNORECASE)
    if issue_match:
        return f"飞书问题 {issue_match.group(1)}"
    case_match = re.search(r"G1Q3[-_ ]?(\d+)", title, re.IGNORECASE)
    if case_match:
        return f"G1Q3-{case_match.group(1)}"
    return title or task_id


def _read_rca_execution_result(task_id: str, task_dir: Path) -> dict[str, Any]:
    """Find and read the structured G1Q3 RCA execution result if present."""
    candidates: list[Path] = [task_dir / "rca_execution_result.json"]
    result_text = ""
    result_path = task_dir / "result.md"
    if result_path.is_file():
        result_text = result_path.read_text(encoding="utf-8", errors="replace")
    import re
    for raw in re.findall(r"/mnt/tmp/[^\"'\s)]+/rca_execution_result\.json", result_text):
        candidates.append(Path(raw))
    task_key = task_id.replace("-", "_")
    if "g1q3_rca_" in task_key:
        # Common governed landing path used by host-side RCA handoff.
        candidates.append(Path("/mnt/tmp") / task_key / "rca_execution_result.json")
    result_text_gate, _result_text_tail = _gate_result_from_task_dir(task_dir)
    for candidate in candidates:
        local_candidates = [candidate]
        if str(candidate).startswith("/mnt/tmp/"):
            rel = candidate.relative_to("/mnt/tmp")
            local_candidates.extend([
                Path.home() / "Mounts" / "department-pnc_team-planning_algo-driving" / "tmp" / rel,
                Path.home() / "Mounts" / "mini_root" / "mnt" / "tmp" / rel,
            ])
        for local in local_candidates:
            payload = _read_json(local)
            if payload.get("schema_version") == "g1q3_rca_execution_result_v1":
                return payload
        if str(candidate).startswith("/mnt/tmp/"):
            try:
                import subprocess
                proc = subprocess.run(
                    [str(Path.home() / ".local" / "bin" / "ssh-mini-agent"), "read_file", str(candidate), "--start", "1", "--lines", "400"],
                    text=True,
                    capture_output=True,
                    timeout=20,
                )
                if proc.returncode == 0 and proc.stdout.strip():
                    content = "\n".join(
                        line.split("|", 1)[1] if "|" in line and line.split("|", 1)[0].isdigit() else line
                        for line in proc.stdout.splitlines()
                    )
                    payload = json.loads(content)
                    if isinstance(payload, dict) and payload.get("schema_version") == "g1q3_rca_execution_result_v1":
                        if result_text_gate and not isinstance(payload.get("gate_result"), dict):
                            payload["gate_result"] = result_text_gate
                        return payload
            except Exception:
                pass
    return {}


def _notice_from_rca_execution_result(label: str, result: dict[str, Any]) -> str | None:
    readback = result.get("readback") if isinstance(result.get("readback"), dict) else {}
    if readback and readback.get("safe_for_group") is not True:
        return None
    verification = result.get("verification") if isinstance(result.get("verification"), dict) else {}
    gate_result = result.get("gate_result") if isinstance(result.get("gate_result"), dict) else (verification.get("gate_result") if isinstance(verification.get("gate_result"), dict) else result)
    report_status = result.get("html_validation_state") or result.get("report_status") or result.get("status")
    attribution_status = result.get("receipt_status") or result.get("attribution_status")
    log_text = "\n".join(str(result.get(k) or "") for k in ("l1", "l0", "summary"))
    verdict = reconcile_report_truth(gate_result, report_status, attribution_status, log_text, report_data=result)
    text = str(readback.get("text") or "").strip()
    if text:
        return f"{label} RCA 检查完成：\n\n{downgrade_g1q3_notice_text(text, verdict)}"
    l1 = str(result.get("l1") or "").strip()
    if l1:
        return f"{label} RCA 检查完成（L1）：\n\n{downgrade_g1q3_notice_text(l1, verdict)}"
    l0 = str(result.get("l0") or "").strip()
    if l0:
        return f"{label} RCA 检查完成（L0）：\n\n{downgrade_g1q3_notice_text(l0, verdict)}"
    return None


def _gate_result_from_task_dir(task_dir: Path) -> tuple[dict[str, Any], str]:
    result_text = ""
    result_path = task_dir / "result.md"
    if result_path.is_file():
        result_text = result_path.read_text(encoding="utf-8", errors="replace")
    gate = _read_json(task_dir / "gate_result.json")
    if gate:
        return gate, result_text
    import re
    decision = ""
    m = re.search(r"gate_result\.decision\s*=\s*([a-zA-Z_][a-zA-Z0-9_-]*)", result_text) or re.search(r"decision[\"'\s:=]+([a-zA-Z_][a-zA-Z0-9_-]*)", result_text)
    if m:
        decision = m.group(1)
    elif re.search(r"ready_to_download|need_source_or_evidence|requires_download|need_evidence", result_text, re.I):
        decision = re.search(r"ready_to_download|need_source_or_evidence|requires_download|need_evidence", result_text, re.I).group(0)
    return ({"decision": decision} if decision else {}), result_text


def _honest_notice(label: str, kind: str, text: str, gate_result: dict[str, Any], result_text: str) -> str:
    verdict = reconcile_report_truth(gate_result, "", "", result_text + "\n" + str(text or ""))
    return f"{label} RCA 检查完成（{kind}）：\n\n" + downgrade_g1q3_notice_text(text, verdict)


def _g1q3_l1_notice(task_id: str, task_dir: Path, state: str) -> str | None:
    task_key = task_id.replace("-", "_")
    if state != "completed" or "g1q3_rca_" not in task_key:
        return None
    meta = _read_json(task_dir / "meta.json")
    label = _g1q3_task_label(meta, task_id)
    structured_result = _read_rca_execution_result(task_id, task_dir)
    structured_notice = _notice_from_rca_execution_result(label, structured_result) if structured_result else None
    if structured_result and structured_notice is None:
        return None
    if structured_notice:
        return structured_notice
    # Normal host view: shared-state and worker-state are both below ~/.hermes.
    artifact_json = task_dir.parents[2] / "worker-state" / "tasks" / task_id / "artifacts" / "codex-last-message.txt"
    if artifact_json.is_file():
        try:
            payload = json.loads(artifact_json.read_text(encoding="utf-8", errors="replace"))
            result = payload.get("result") if isinstance(payload, dict) else {}
            l1_items = result.get("L1") if isinstance(result, dict) else None
            if isinstance(l1_items, list) and l1_items:
                lines = [str(item).strip() for item in l1_items if str(item).strip()]
                if lines:
                    gate_result, result_tail = _gate_result_from_task_dir(task_dir)
                    return _honest_notice(label, "L1", "\n".join(f"- {line}" for line in lines), gate_result, result_tail)
        except Exception:
            pass
    result_text = ""
    result_path = task_dir / "result.md"
    if result_path.is_file():
        result_text = result_path.read_text(encoding="utf-8", errors="replace")
    handoff = _handoff_contract_from_meta(meta)
    candidates: list[Path] = []
    for raw in {
        str(handoff.get("artifact_root_policy") or "").replace("<task_id>", task_id).rstrip("/") + "/L0_L1_summary.md" if handoff.get("artifact_root_policy") else "",
        *(__import__("re").findall(r"/mnt/tmp/[^\"'\s)]+/L0_L1(?:_[A-Za-z0-9_-]+)?_summary\.md", result_text)),
    }:
        if raw:
            candidates.append(Path(raw))
    for candidate in candidates:
        if not candidate.is_file():
            if str(candidate).startswith("/mnt/tmp/"):
                mapped_candidates = [
                    Path.home() / "Mounts" / "department-pnc_team-planning_algo-driving" / "tmp" / candidate.relative_to("/mnt/tmp"),
                    Path.home() / "Mounts" / "mini_root" / "mnt" / "tmp" / candidate.relative_to("/mnt/tmp"),
                ]
                mapped = next((path for path in mapped_candidates if path.is_file()), None)
                if mapped is not None:
                    candidate = mapped
                else:
                    try:
                        import subprocess
                        proc = subprocess.run(
                            [str(Path.home() / ".local" / "bin" / "ssh-mini-agent"), "read_file", str(candidate), "--start", "1", "--lines", "200"],
                            text=True,
                            capture_output=True,
                            timeout=20,
                        )
                        if proc.returncode == 0 and proc.stdout.strip():
                            summary = "\n".join(
                                line.split("|", 1)[1] if "|" in line and line.split("|", 1)[0].isdigit() else line
                                for line in proc.stdout.splitlines()
                            )
                            l1 = _extract_markdown_section(summary, "L1")
                            if l1:
                                return _honest_notice(label, "L1", l1, *_gate_result_from_task_dir(task_dir))
                            l0 = _extract_markdown_section(summary, "L0")
                            if l0:
                                return _honest_notice(label, "L0", l0, *_gate_result_from_task_dir(task_dir))
                    except Exception:
                        pass
                    continue
            else:
                continue
        summary = candidate.read_text(encoding="utf-8", errors="replace")
        l1 = _extract_markdown_section(summary, "L1")
        if l1:
            return _honest_notice(label, "L1", l1, *_gate_result_from_task_dir(task_dir))
        l0 = _extract_markdown_section(summary, "L0")
        if l0:
            return _honest_notice(label, "L0", l0, *_gate_result_from_task_dir(task_dir))
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Poll one VM shared-state task and print final user-facing result.")
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--root", default=str(Path.home() / "Mounts" / "mini_root" / ".hermes" / "shared-state"))
    parser.add_argument("--interval", type=float, default=10.0)
    parser.add_argument("--timeout", type=float, default=21600.0)
    args = parser.parse_args()

    task_id = str(args.task_id).strip()
    if not __import__("re").match(r"^\d{8}-\d{6}-[A-Za-z0-9][A-Za-z0-9_.-]*$", task_id):
        print(f"VM 任务完成通知探针拒绝非生产 task_id：{task_id}")
        return 64
    root = Path(args.root).expanduser().resolve()
    task_dir = root / "tasks" / task_id
    deadline = time.monotonic() + max(1.0, args.timeout)
    terminal_states = {"completed", "failed", "abandoned", "blocked"}
    state = ""
    while time.monotonic() < deadline:
        state = _state_for_task(root, task_id)
        if state in terminal_states:
            break
        time.sleep(max(1.0, args.interval))
    else:
        print(f"VM 任务完成通知探针超时：{task_id}\n当前状态：{state or 'unknown'}")
        return 124

    notify = _load_notify_module()
    class Row(dict):
        def __getitem__(self, key):
            return self.get(key, "")

    meta = _read_json(task_dir / "meta.json")
    item = dict(meta)
    item["state"] = state
    item.setdefault("updated_at", meta.get("updated_at", ""))
    row = Row(
        task_id=task_id,
        title=meta.get("title") or task_id,
        state=state,
        updated_at=meta.get("updated_at", ""),
        sync_version=meta.get("sync_version", ""),
        latest_summary=meta.get("latest_summary") or meta.get("summary") or "",
    )
    entries = notify.stage_entries(row, item, task_dir)
    stage_key = entries[0][0] if entries else ("terminal:" + state if state in {"completed", "failed", "abandoned"} else "state:blocked")
    print(_g1q3_l1_notice(task_id, task_dir, state) or notify.build_notice(row, item, task_dir, stage_key))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
