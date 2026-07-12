#!/usr/bin/env python3
"""PNC delivery observability status report.

Small read-only health report for the delivery-side task observability chain:
public task page reachability, governance guard status, pending/failed Feishu
completion notices, and per-business-group task counts.
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gateway.tasks.store import TaskStore  # noqa: E402
from gateway.tasks.types import TaskStatus  # noqa: E402
from hermes_cli.config import get_hermes_home  # noqa: E402
from hermes_cli.task_views import list_task_views, task_to_view  # noqa: E402
from scripts.pnc_completion_notice_relay import iter_pending_notices  # noqa: E402
from scripts.pnc_feishu_delivery_guard import BUSINESS_GROUPS, run_guard  # noqa: E402
from scripts.hermes_live_drift_guard import (  # noqa: E402
    validate_pnc_completion_notice_relay_launchd,
    validate_pnc_feishu_delivery_repair_launchd,
    validate_pnc_vm_task_sync_launchd,
)
from scripts.pnc_g1q3_gray_route_audit import build_audit as build_g1q3_gray_route_audit  # noqa: E402

DEFAULT_PUBLIC_URL = "http://127.0.0.1:9125/tasks"
USER_VISIBLE_URL = "http://192.168.14.32:9125/tasks"




def _task_store_path() -> Path:
    return get_hermes_home() / "analytics" / "tasks.db"


def _taskstore_status_mismatches(chat_id: str, *, limit: int = 10_000) -> list[dict[str, Any]]:
    store = TaskStore(_task_store_path())
    mismatches: list[dict[str, Any]] = []
    for task in store.list_recent(limit=limit, platform="feishu", chat_id=chat_id):
        effective = task_to_view(task, detail=False)
        raw_status = task.status.value
        effective_status = str(effective.get("status") or raw_status)
        if raw_status == effective_status:
            continue
        mismatches.append({
            "task_id": task.task_id,
            "raw_status": raw_status,
            "effective_status": effective_status,
            "request_summary": task.request_summary,
            "vm_task_id": task.vm_task_id,
        })
    return mismatches

def _http_probe(url: str, *, timeout: float = 5.0) -> dict[str, Any]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            body = resp.read(256)
            return {"ok": 200 <= resp.status < 400, "status": resp.status, "bytes_sampled": len(body), "url": url}
    except urllib.error.HTTPError as exc:
        return {"ok": False, "status": exc.code, "error": str(exc), "url": url}
    except Exception as exc:
        return {"ok": False, "status": None, "error": f"{type(exc).__name__}: {exc}", "url": url}


def _load_sidecar(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return value if isinstance(value, dict) else {}


def _completion_notice_summary() -> dict[str, Any]:
    root = get_hermes_home() / "task-state"
    counts: dict[str, int] = {"pending": 0, "failed": 0, "sent": 0, "acknowledged": 0}
    failed: list[dict[str, Any]] = []
    if root.exists():
        for path in root.glob("*.json"):
            body = _load_sidecar(path)
            notice = body.get("completion_notice") if isinstance(body.get("completion_notice"), dict) else None
            if not notice:
                continue
            status = str(notice.get("send_status") or "pending").strip().lower()
            counts[status] = counts.get(status, 0) + 1
            if status == "failed":
                failed.append({
                    "task_id": path.stem,
                    "chat_id": notice.get("chat_id"),
                    "vm_task_id": notice.get("vm_task_id"),
                    "attempt_count": notice.get("attempt_count"),
                    "last_attempt_at": notice.get("last_attempt_at"),
                    "send_error": notice.get("send_error"),
                })
    pending_retryable = iter_pending_notices(retry_failed_after_seconds=600, max_attempts=3)
    return {
        "counts": counts,
        "pending_or_retryable_count": len(pending_retryable),
        "failed": failed[:20],
        "failed_truncated": len(failed) > 20,
    }


def _business_group_task_summary(limit: int = 10) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for group in BUSINESS_GROUPS:
        views = list_task_views(platform="feishu", chat_id=group.chat_id, limit=limit, offset=0)
        mismatches = _taskstore_status_mismatches(group.chat_id)
        rows.append({
            "slug": group.slug,
            "label": group.label,
            "chat_id": group.chat_id,
            "counts": views.get("status_counts") or {status.value: 0 for status in TaskStatus},
            "taskstore_effective_status_mismatch_count": len(mismatches),
            "taskstore_effective_status_mismatches": mismatches[:20],
            "taskstore_effective_status_mismatches_truncated": len(mismatches) > 20,
            "latest": [
                {
                    "task_id": task.get("task_id"),
                    "status": task.get("status"),
                    "request_summary": task.get("request_summary"),
                    "started_at": task.get("started_at"),
                    "completed_at": task.get("completed_at"),
                    "vm_task_id": task.get("vm_bridge", {}).get("vm_task_id") if isinstance(task.get("vm_bridge"), dict) else None,
                }
                for task in (views.get("tasks") or [])[:5]
            ],
        })
    return rows


def build_report(*, public_url: str = DEFAULT_PUBLIC_URL) -> dict[str, Any]:
    guard = run_guard()
    launchd = {
        "vm_task_sync": validate_pnc_vm_task_sync_launchd(),
        "completion_notice_relay": validate_pnc_completion_notice_relay_launchd(),
        "feishu_delivery_repair": validate_pnc_feishu_delivery_repair_launchd(),
    }
    public_probe = _http_probe(public_url)
    notices = _completion_notice_summary()
    groups = _business_group_task_summary()
    gray_route_audit = build_g1q3_gray_route_audit(get_hermes_home() / "pnc_agent" / "receipts" / "g1q3_rca", since_days=7)
    errors: list[str] = []
    warnings: list[str] = []
    if not public_probe.get("ok"):
        errors.append(f"public task page unreachable: {public_probe.get('error') or public_probe.get('status')}")
    if not guard.get("ok"):
        errors.extend(f"feishu_delivery: {item}" for item in guard.get("errors", []))
    warnings.extend(f"feishu_delivery: {item}" for item in guard.get("warnings", []))
    for name, payload in launchd.items():
        if not payload.get("ok"):
            errors.extend(f"{name}: {item}" for item in payload.get("errors", []))
    mismatch_total = 0
    for group in groups:
        mismatch_count = int(group.get("taskstore_effective_status_mismatch_count") or 0)
        mismatch_total += mismatch_count
        if mismatch_count:
            warnings.append(f"{group.get('label')} TaskStore/effective status mismatch count={mismatch_count}")
    if notices["counts"].get("failed", 0):
        warnings.append(f"completion_notice failed count={notices['counts'].get('failed')}")
    if gray_route_audit.get("false_rejection_candidate_count", 0):
        warnings.append(f"G1Q3 gray route false-rejection candidates={gray_route_audit.get('false_rejection_candidate_count')}")
    return {
        "ok": not errors,
        "governance_ok": not errors and not warnings,
        "user_visible_url": USER_VISIBLE_URL,
        "reconcile_command": "python3 scripts/pnc_taskstore_status_reconcile.py --apply" if mismatch_total else None,
        "local_public_probe": public_probe,
        "pnc_feishu_delivery": guard,
        "launchd": launchd,
        "completion_notices": notices,
        "g1q3_gray_route_audit": gray_route_audit,
        "business_groups": groups,
        "warnings": warnings,
        "errors": errors,
    }




def _status_icon(report: dict[str, Any]) -> str:
    if not report.get("ok"):
        return "❌"
    if not report.get("governance_ok"):
        return "⚠️"
    return "✅"


def format_markdown_report(report: dict[str, Any]) -> str:
    icon = _status_icon(report)
    status = "OK" if report.get("governance_ok") else "需关注" if report.get("ok") else "异常"
    lines = [
        f"{icon} PNC 任务可观测状态：{status}",
        "",
        f"用户侧入口：{report.get('user_visible_url')}",
        f"本机探测：{report.get('local_public_probe', {}).get('status')} / ok={report.get('local_public_probe', {}).get('ok')}",
        "",
        "业务群任务概况：",
    ]
    for row in report.get("business_groups", []):
        counts = row.get("counts") or {}
        mismatch = int(row.get("taskstore_effective_status_mismatch_count") or 0)
        suffix = f"，状态分裂 {mismatch}" if mismatch else ""
        lines.append(
            f"- {row.get('label')}：running={counts.get('running', 0)}，"
            f"completed={counts.get('completed', 0)}，failed={counts.get('failed', 0)}{suffix}"
        )
    notice_counts = (report.get("completion_notices") or {}).get("counts") or {}
    lines.extend([
        "",
        "飞书回传：",
        f"- pending={notice_counts.get('pending', 0)}，failed={notice_counts.get('failed', 0)}，sent={notice_counts.get('sent', 0)}",
    ])
    gray = report.get("g1q3_gray_route_audit") or {}
    gray_counts = (gray.get("counts") or {}).get("by_decision") or {}
    lines.extend([
        "",
        "G1Q3 灰度路由：",
        f"- accepted={gray_counts.get('accepted', 0)}，dry_run={gray_counts.get('dry_run', 0)}，reject={gray_counts.get('reject', 0)}，疑似误拒绝={gray.get('false_rejection_candidate_count', 0)}",
    ])
    if report.get("reconcile_command"):
        lines.extend(["", f"状态收敛命令：`{report['reconcile_command']}`"])
    if report.get("warnings"):
        lines.append("")
        lines.append("Warnings：")
        lines.extend(f"- {warn}" for warn in report.get("warnings", []))
    if report.get("errors"):
        lines.append("")
        lines.append("Errors：")
        lines.extend(f"- {err}" for err in report.get("errors", []))
    if not report.get("warnings") and not report.get("errors"):
        lines.extend(["", "当前无治理告警。"])
    return "\n".join(lines)

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--public-url", default=DEFAULT_PUBLIC_URL)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--markdown", action="store_true", help="Emit a Feishu-friendly Markdown summary")
    args = parser.parse_args(argv)
    report = build_report(public_url=args.public_url)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    elif args.markdown:
        print(format_markdown_report(report))
    else:
        print(f"[{'OK' if report['ok'] else 'DRIFT'}] PNC delivery observability")
        print(f"user_url: {report['user_visible_url']}")
        print(f"local_probe: {report['local_public_probe'].get('status')} ok={report['local_public_probe'].get('ok')}")
        for row in report["business_groups"]:
            counts = row["counts"]
            mismatch = row.get("taskstore_effective_status_mismatch_count", 0)
            suffix = f" mismatches={mismatch}" if mismatch else ""
            print(f"- {row['label']}: running={counts.get('running', 0)} completed={counts.get('completed', 0)} failed={counts.get('failed', 0)}{suffix}")
        notice_counts = report["completion_notices"]["counts"]
        print(f"completion_notices: pending={notice_counts.get('pending', 0)} failed={notice_counts.get('failed', 0)} sent={notice_counts.get('sent', 0)}")
        gray = report.get("g1q3_gray_route_audit") or {}
        gray_counts = (gray.get("counts") or {}).get("by_decision") or {}
        print(f"g1q3_gray_route: accepted={gray_counts.get('accepted', 0)} dry_run={gray_counts.get('dry_run', 0)} reject={gray_counts.get('reject', 0)} false_rejection_candidates={gray.get('false_rejection_candidate_count', 0)}")
        if report.get("reconcile_command"):
            print(f"reconcile_command: {report['reconcile_command']}")
        for warn in report["warnings"]:
            print(f"warning: {warn}")
        for err in report["errors"]:
            print(f"error: {err}")
    return 0 if report["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
