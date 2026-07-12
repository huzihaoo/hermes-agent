#!/usr/bin/env python3
"""Compare G1Q3 Feishu issue preread sources: Hermes MCP vs official Meegle CLI.

This is an admission diagnostic, not a hot-path switch.  It checks whether the
candidate official CLI source satisfies the current RCA intake contract before
it can be promoted from fallback/diagnostic to preferred source.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gateway.pnc_issue_context import (  # noqa: E402
    G1Q3IssueReadResult,
    fetch_g1q3_issue_context_result,
    fetch_g1q3_issue_context_result_via_meegle,
)
from gateway.pnc_rca_schema import issue_context_from_compact_text, validate_issue_context_fields  # noqa: E402

REQUIRED_LABELS = {
    "title": "title",
    "work_item_id": "work_item_id",
    "status": "当前状态",
    "pdcl_download_cmd": "数据地址",
}
OPTIONAL_BUT_EXPECTED_LABELS = {
    "project_label": "所属项目",
    "owner": "当前负责人",
    "vehicle": "车辆编号",
    "frame_id": "frame_id",
    "comments": "最近评论摘录",
}


@dataclass
class SourceEval:
    source: str
    status: str
    source_quality: str
    elapsed_ms: int
    blocker: dict[str, Any] | None
    errors: list[dict[str, Any]] | None
    fields: dict[str, bool]
    pdcl_valid: bool | None
    context_chars: int
    context_head: str

    @property
    def required_ok(self) -> bool:
        return self.status == "fields_extracted" and all(self.fields.get(k) for k in REQUIRED_LABELS) and self.pdcl_valid is True


def _has_label(text: str, label: str) -> bool:
    if label == "最近评论摘录":
        return "## 最近评论摘录" in text
    return any(line.startswith(f"- {label}:") and line.split(":", 1)[1].strip() for line in text.splitlines())


def _eval_result(source: str, result: G1Q3IssueReadResult, *, project_key: str, work_item_id: str, elapsed_ms: int) -> SourceEval:
    text = result.context_text or ""
    fields = {name: _has_label(text, label) for name, label in {**REQUIRED_LABELS, **OPTIONAL_BUT_EXPECTED_LABELS}.items()}
    issue_ctx = issue_context_from_compact_text(
        project_key=project_key,
        work_item_id=work_item_id,
        compact_text=text,
        source_quality=result.source_quality,
        blockers=[] if text else ([result.blocker] if result.blocker else []),
    )
    issue_ctx, blocker = validate_issue_context_fields(issue_ctx)
    return SourceEval(
        source=source,
        status=result.status,
        source_quality=result.source_quality,
        elapsed_ms=elapsed_ms,
        blocker=result.blocker,
        errors=result.errors,
        fields=fields,
        pdcl_valid=issue_ctx.is_pdcl_format if text else None,
        context_chars=len(text),
        context_head=text[:700],
    )


def _timed(source: str, fn, *, project_key: str, work_item_id: str) -> SourceEval:
    start = time.perf_counter()
    result = fn()
    elapsed_ms = int((time.perf_counter() - start) * 1000)
    return _eval_result(source, result, project_key=project_key, work_item_id=work_item_id, elapsed_ms=elapsed_ms)


def _decision(mcp_eval: SourceEval, meegle_eval: SourceEval) -> dict[str, Any]:
    """Per-sample source-promotion signal.

    Source switching is judged by Meegle parity/regression against the current
    MCP source.  Random homepage issues may legitimately lack PDCL or comments;
    that is a business-data gate for RCA execution, not a source regression when
    both sources agree.
    """
    field_regressions = [k for k, mcp_has in mcp_eval.fields.items() if mcp_has and not meegle_eval.fields.get(k)]
    status_regression = mcp_eval.status == "fields_extracted" and meegle_eval.status != "fields_extracted"
    pdcl_regression = mcp_eval.pdcl_valid is True and meegle_eval.pdcl_valid is not True
    if status_regression or pdcl_regression or field_regressions:
        preferred = "mcp"
        ok_to_switch = False
        reason_parts = []
        if status_regression:
            reason_parts.append(f"status regression mcp={mcp_eval.status} meegle={meegle_eval.status}")
        if pdcl_regression:
            reason_parts.append("PDCL regression vs MCP")
        if field_regressions:
            reason_parts.append(f"missing MCP-present fields: {', '.join(field_regressions)}")
        reason = "; ".join(reason_parts)
    elif mcp_eval.status == "fields_extracted" and meegle_eval.status == "fields_extracted":
        preferred = "meegle_candidate_parity"
        ok_to_switch = False
        reason = "Meegle has no per-sample regression vs MCP; aggregate soak gate decides promotion."
    else:
        preferred = "mcp"
        ok_to_switch = False
        reason = "Neither source produced a stable extracted-fields baseline for this sample."
    return {
        "ok_to_switch_preferred_source_now": ok_to_switch,
        "recommended_preferred_source": preferred,
        "reason": reason,
        "source_parity": not (status_regression or pdcl_regression or field_regressions),
        "mcp_required_ok": mcp_eval.required_ok,
        "meegle_required_ok": meegle_eval.required_ok,
        "minimum_promotion_gate": {
            "sample_size": ">=10 recent/representative G1Q3 issues",
            "source_parity_rate": "100% no status/PDCL/field regression vs MCP",
            "rca_contract_samples": "For samples where MCP has valid PDCL, Meegle must also have valid PDCL",
            "expected_field_regression": "0 missing expected fields vs MCP for project/owner/vehicle/frame/comments when MCP has them",
            "failure_classification": "auth/tool/source failures must not become PDCL missing/invalid blockers",
            "latency": "p95 acceptable for group intake; no unbounded interactive auth in hot path",
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-key", default="t03o4q")
    parser.add_argument("--work-item-id", action="append", required=True)
    parser.add_argument("--json", action="store_true", help="Print full JSON report")
    args = parser.parse_args()

    items = []
    for work_item_id in args.work_item_id:
        mcp_eval = _timed(
            "mcp",
            lambda work_item_id=work_item_id: fetch_g1q3_issue_context_result(
                project_key=args.project_key,
                work_item_id=work_item_id,
                use_meegle_fallback=False,
            ),
            project_key=args.project_key,
            work_item_id=work_item_id,
        )
        meegle_eval = _timed(
            "meegle",
            lambda work_item_id=work_item_id: fetch_g1q3_issue_context_result_via_meegle(
                project_key=args.project_key,
                work_item_id=work_item_id,
            ),
            project_key=args.project_key,
            work_item_id=work_item_id,
        )
        items.append({
            "project_key": args.project_key,
            "work_item_id": work_item_id,
            "sources": {"mcp": asdict(mcp_eval), "meegle": asdict(meegle_eval)},
            "decision": _decision(mcp_eval, meegle_eval),
        })

    report = {"schema_version": "g1q3_issue_source_compare_v1", "items": items}
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        for item in items:
            decision = item["decision"]
            print(f"issue={item['work_item_id']} switch_now={decision['ok_to_switch_preferred_source_now']} preferred={decision['recommended_preferred_source']}")
            print(f"reason={decision['reason']}")
            for name, source in item["sources"].items():
                print(f"  {name}: status={source['status']} pdcl_valid={source['pdcl_valid']} elapsed_ms={source['elapsed_ms']} required_fields="
                      f"{ {k: source['fields'][k] for k in REQUIRED_LABELS} }")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
