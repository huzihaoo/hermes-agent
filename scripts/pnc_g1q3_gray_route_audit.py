#!/usr/bin/env python3
"""Audit G1Q3 RCA gray-delivery group-routing receipts.

Read-only helper for gray rollout: summarizes accepted/dry-run/rejected route
receipts and highlights likely false-rejection candidates so business-facing
routing can be tuned without loosening execution/output safety gates.
"""
from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

try:
    from hermes_cli.config import get_hermes_home
except Exception:  # pragma: no cover - direct script fallback
    def get_hermes_home() -> Path:
        return Path.home() / ".hermes"

DEFAULT_RECEIPT_DIR = Path.home() / ".hermes" / "pnc_agent" / "receipts" / "g1q3_rca"
FALSE_REJECTION_HINT_REASONS = {"unsupported_task_template", "missing_required_input", "missing_issue_identifier", "missing_case_id"}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return rows
    for line in lines:
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _parse_dt(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def iter_receipts(receipt_dir: Path, *, since_days: int) -> list[dict[str, Any]]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)
    rows: list[dict[str, Any]] = []
    for path in sorted(receipt_dir.glob("*.jsonl")):
        for row in _read_jsonl(path):
            if row.get("event_type") != "group_binding_decision":
                continue
            ts = _parse_dt(row.get("timestamp"))
            if ts is not None and ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if ts is not None and ts < cutoff:
                continue
            rows.append(row)
    return rows


def build_audit(receipt_dir: Path, *, since_days: int = 7) -> dict[str, Any]:
    rows = iter_receipts(receipt_dir, since_days=since_days)
    by_decision = Counter(str(row.get("decision") or "unknown") for row in rows)
    by_template = Counter(str(row.get("template_id") or "unknown") for row in rows)
    by_reason = Counter(str(row.get("reason") or "none") for row in rows)
    by_risk_gate = Counter(str(row.get("risk_gate") or row.get("reason") or "none") for row in rows)
    by_route_surface = Counter(str(row.get("route_surface") or row.get("template_id") or "unknown") for row in rows)

    false_rejection_candidates = []
    for row in rows:
        decision = str(row.get("decision") or "")
        reason = str(row.get("reason") or "")
        if decision == "reject" and reason in FALSE_REJECTION_HINT_REASONS:
            snap = row.get("decision_snapshot") if isinstance(row.get("decision_snapshot"), dict) else {}
            false_rejection_candidates.append({
                "timestamp": row.get("timestamp"),
                "message_id": row.get("message_id"),
                "requester": row.get("requester"),
                "reason": reason,
                "template_id": row.get("template_id"),
                "user_message": snap.get("user_message") or row.get("user_message"),
            })

    hourly: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        ts = _parse_dt(row.get("timestamp"))
        bucket = ts.strftime("%Y-%m-%dT%H:00") if ts else "unknown"
        hourly[bucket][str(row.get("decision") or "unknown")] += 1

    return {
        "schema_version": "pnc_g1q3_gray_route_audit_v1",
        "receipt_dir": str(receipt_dir),
        "since_days": since_days,
        "total_decisions": len(rows),
        "counts": {
            "by_decision": dict(by_decision),
            "by_template": dict(by_template),
            "by_reason": dict(by_reason),
            "by_risk_gate": dict(by_risk_gate),
            "by_route_surface": dict(by_route_surface),
        },
        "false_rejection_candidate_count": len(false_rejection_candidates),
        "false_rejection_candidates": false_rejection_candidates[:50],
        "false_rejection_candidates_truncated": len(false_rejection_candidates) > 50,
        "hourly": {key: dict(value) for key, value in sorted(hourly.items())},
    }


def format_markdown(audit: dict[str, Any]) -> str:
    counts = audit.get("counts") or {}
    by_decision = counts.get("by_decision") or {}
    by_reason = counts.get("by_reason") or {}
    by_route = counts.get("by_route_surface") or {}
    lines = [
        "📊 G1Q3 RCA 灰度路由审计",
        "",
        f"范围：最近 {audit.get('since_days')} 天；决策数={audit.get('total_decisions', 0)}",
        f"接单 accepted={by_decision.get('accepted', 0)}，dry_run={by_decision.get('dry_run', 0)}，reject={by_decision.get('reject', 0)}",
        f"疑似误拒绝候选={audit.get('false_rejection_candidate_count', 0)}",
        "",
        "入口类型：",
    ]
    for key, value in sorted(by_route.items(), key=lambda item: (-item[1], item[0]))[:10]:
        lines.append(f"- {key}: {value}")
    lines.append("")
    lines.append("阻塞/原因 Top：")
    for key, value in sorted(by_reason.items(), key=lambda item: (-item[1], item[0]))[:10]:
        lines.append(f"- {key}: {value}")
    candidates = audit.get("false_rejection_candidates") or []
    if candidates:
        lines.append("")
        lines.append("疑似误拒绝样例：")
        for item in candidates[:10]:
            lines.append(f"- {item.get('timestamp')} {item.get('reason')} message={item.get('message_id')}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt-dir", type=Path, default=DEFAULT_RECEIPT_DIR)
    parser.add_argument("--since-days", type=int, default=7)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--markdown", action="store_true")
    args = parser.parse_args(argv)
    audit = build_audit(args.receipt_dir, since_days=args.since_days)
    if args.json:
        print(json.dumps(audit, ensure_ascii=False, indent=2, sort_keys=True))
    elif args.markdown:
        print(format_markdown(audit))
    else:
        print(f"decisions={audit['total_decisions']} false_rejection_candidates={audit['false_rejection_candidate_count']}")
        print(f"by_decision={audit['counts']['by_decision']}")
        print(f"by_reason={audit['counts']['by_reason']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
