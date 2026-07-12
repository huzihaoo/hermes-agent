"""Shared G1Q3-RCA delivery truth reconciliation helpers.

User-facing G1Q3 status surfaces must not infer deliverability from VM terminal
state or the existence of an HTML file alone.  The gate result is authoritative:
non-green gates can only be reported as intake/admission complete and waiting for
source download/parsed evidence.
"""
from __future__ import annotations

import re
from typing import Any

NON_GREEN_GATE_DECISIONS = {"ready_to_download", "need_evidence", "need_source_or_evidence", "requires_download", "skipped"}
GREEN_GATE_DECISIONS = {"green", "pass", "passed", "ok", "delivery_ready", "report_ready", "html_delivery_ready"}
FALSE_GREEN_RE = re.compile(r"html_delivery_ready|报告可交付|报告已生成|report_generated", re.I)
GATE_RE = re.compile(r"(?:decision|gate|gate_decision)[\"'\s:=]+([a-zA-Z_][a-zA-Z0-9_-]*)", re.I)

# Worker-written business_result terminal/status values that mean "intake done,
# still waiting on source/evidence" — i.e. NOT a delivered RCA report.
_BUSINESS_NEED_DATA_TERMINALS = {"need_download", "need_source_or_evidence", "need_evidence", "need_data"}
_BUSINESS_NEED_DATA_STATES = {"need_evidence", "need_source_or_evidence", "need_download", "need_data", "blocked"}


def _nested_get(data: Any, *paths: str) -> Any:
    for path in paths:
        current = data
        ok = True
        for part in path.split("."):
            if isinstance(current, dict) and part in current:
                current = current.get(part)
            else:
                ok = False
                break
        if ok and current not in (None, "", [], {}):
            return current
    return None


def _compact(value: Any, *, limit: int = 320) -> str:
    text = str(value or "").strip()
    text = re.sub(r"\s+", " ", text)
    return text[: max(0, limit - 1)].rstrip() + "…" if len(text) > limit else text


def _gate_decision(gate_result: Any, log_text: str = "") -> str:
    if isinstance(gate_result, dict):
        decision = _nested_get(gate_result, "decision", "gate_decision", "gate_result.decision")
        if decision:
            return str(decision).strip().lower()
        status = _nested_get(gate_result, "status")
        if status and str(status).strip().lower() not in {"completed", "done", "success", "report_generated"}:
            return str(status).strip().lower()
        # Some gate payloads expose per-gate require-download signals only.
        blob = str(gate_result)
        if re.search(r"requires_download|ready_to_download|need_source_or_evidence|need_evidence", blob, re.I):
            return "requires_download"
    text = str(log_text or "")
    for match in GATE_RE.finditer(text):
        decision = match.group(1).strip().lower()
        if decision:
            return decision
    match = re.search(r"ready_to_download|need_source_or_evidence|requires_download|need_evidence", text, re.I)
    return match.group(0).lower() if match else ""


def gate_is_green(gate_result: Any = None, log_text: str = "") -> bool:
    decision = _gate_decision(gate_result, log_text)
    return decision in GREEN_GATE_DECISIONS


def parsed_l2_assets_present(gate_result: Any = None, report_data: Any = None, log_text: str = "") -> bool:
    """Best-effort parsed/L2 asset predicate for deliverability.

    Absence is fail-closed.  Explicit requires_download/missing parsed signals
    always veto.  Tests and future producers may provide parsed_l2_assets_present.
    """
    for data in (gate_result, report_data):
        if not isinstance(data, dict):
            continue
        explicit = _nested_get(
            data,
            "parsed_l2_assets_present",
            "parsed_assets_present",
            "l2_assets_present",
            "evidence.parsed_l2_assets_present",
            "evidence_boundary.parsed_l2_assets_present",
        )
        if explicit is not None:
            return bool(explicit)
        for path in (
            "G4_data_structure",
            "G5_time_alignment",
            "gates.G4_data_structure",
            "gates.G5_time_alignment",
        ):
            value = _nested_get(data, path)
            text = str(value or "").lower()
            if any(term in text for term in ("requires_download", "missing", "缺失", "need")):
                return False
        blob = str(_nested_get(data, "evidence_boundary", "boundary", "review.boundary") or "").lower()
        if any(term in blob for term in ("parsed/l2 assets 缺失", "parsed", "l2")) and any(term in blob for term in ("缺失", "missing", "requires_download")):
            return False
        if isinstance(_nested_get(data, "parsed_assets"), list) and _nested_get(data, "parsed_assets"):
            return True
        if isinstance(_nested_get(data, "l2_assets"), list) and _nested_get(data, "l2_assets"):
            return True
    text = str(log_text or "").lower()
    if re.search(r"g[45][^\n]*(requires_download|missing|缺失)", text) or "parsed/l2 assets 缺失" in text:
        return False
    if re.search(r"parsed[_ /-]?l2[_ /-]?assets[^\n]*(present|ready|ok|true|在场|齐全)", text):
        return True
    # If the gate itself is still non-green, absence of an explicit parsed/L2
    # signal should be reported as gate truth, not promoted to a generic
    # requires_download surrogate.
    if _gate_decision(gate_result, "") in NON_GREEN_GATE_DECISIONS:
        return True
    return False


def _business_result_truth(business_result: Any) -> dict[str, Any] | None:
    """Authoritative non-deliverable verdict straight from ``business_result``.

    The worker writes an explicit, structured verdict (gate_decision,
    gate_skip_reason, status, terminal_state) into result.md.  When it says the
    intake is NOT a delivered report (non-green/skipped gate, or a need-data
    terminal/status), this block is the single source of truth and must win
    over regex inference over log/report text.  Green deliverable verdicts
    return ``None`` here so they keep flowing through the inferred path (which
    derives attribution from report_data).
    """
    if not isinstance(business_result, dict):
        return None
    gate_decision = str(business_result.get("gate_decision") or "").strip().lower()
    terminal_state = str(business_result.get("terminal_state") or "").strip().lower()
    status = str(business_result.get("status") or "").strip().lower()
    if not (gate_decision or terminal_state or status):
        return None
    non_green = bool(gate_decision) and gate_decision not in GREEN_GATE_DECISIONS
    need_data = terminal_state in _BUSINESS_NEED_DATA_TERMINALS or status in _BUSINESS_NEED_DATA_STATES
    if not (non_green or need_data):
        return None
    return {
        "gate_decision": gate_decision or "skipped",
        "gate_skip_reason": str(business_result.get("gate_skip_reason") or "").strip(),
        "terminal_state": terminal_state,
        "status": status,
    }


def _reconcile_from_business_result(
    biz: dict[str, Any], *, candidate_cause: Any, responsibility_candidate: Any
) -> dict[str, Any]:
    """Honest verdict for a blocked/non-deliverable intake from business_result.

    A blocked intake read zero parsed evidence, so there is NO attribution to
    report — never ``hypothesis_ready``.  The blocker is the gate_skip_reason.
    """
    gate_decision = biz["gate_decision"] or "skipped"
    return {
        "honest_report_status": "need_download",
        "honest_attribution_status": "",
        "honest_conclusion": f"intake 与准入校验完成；待下载/解析数据后再出 RCA 结论（gate={gate_decision}）",
        "gate_decision": gate_decision,
        "gate_skip_reason": biz.get("gate_skip_reason") or "",
        "gate_green": False,
        "parsed_l2_assets_present": False,
        "anomaly": False,
        "anomaly_reasons": [],
        "candidate_cause_mode": "low_confidence_hypothesis" if candidate_cause else "",
        "responsibility_candidate_mode": "suppressed_until_human_gate" if responsibility_candidate else "",
        "truth_source": "business_result",
    }


def reconcile_report_truth(
    gate_result: Any,
    report_status: Any = "",
    attribution_status: Any = "",
    log_text: str = "",
    *,
    report_data: Any = None,
    candidate_cause: Any = "",
    responsibility_candidate: Any = "",
    business_result: Any = None,
) -> dict[str, Any]:
    """Return a single honest delivery verdict shared by relay and probe."""
    biz = _business_result_truth(business_result)
    if biz is not None:
        return _reconcile_from_business_result(
            biz,
            candidate_cause=candidate_cause,
            responsibility_candidate=responsibility_candidate,
        )
    gate_decision = _gate_decision(gate_result, log_text)
    gate_green = gate_decision in GREEN_GATE_DECISIONS
    assets_present = parsed_l2_assets_present(gate_result, report_data, log_text)
    raw_report_status = str(report_status or "").strip()
    raw_attr = str(attribution_status or "").strip()
    raw_text = "\n".join([raw_report_status, raw_attr, str(log_text or "")])
    anomaly_reasons: list[str] = []
    if raw_attr in {"hypothesis_ready", "needs_review", "need_review"}:
        anomaly_reasons.append("low_confidence_attribution")
    if gate_decision and not gate_green and FALSE_GREEN_RE.search(raw_text):
        anomaly_reasons.append("false_green_claim_under_gate")
    if gate_decision and not gate_green:
        decision_label = gate_decision or "non_green"
        return {
            "honest_report_status": "need_download",
            "honest_attribution_status": "hypothesis_ready" if raw_attr else "",
            "honest_conclusion": f"intake 与准入校验完成；待下载/解析数据后再出 RCA 结论（gate={decision_label}）",
            "gate_decision": decision_label,
            "gate_green": False,
            "parsed_l2_assets_present": assets_present,
            "anomaly": bool(anomaly_reasons),
            "anomaly_reasons": anomaly_reasons,
            "candidate_cause_mode": "low_confidence_hypothesis" if candidate_cause else "",
            "responsibility_candidate_mode": "suppressed_until_human_gate" if responsibility_candidate else "",
        }
    if raw_report_status == "html_delivery_ready" and not (gate_green and assets_present):
        anomaly_reasons.append("html_ready_without_gate_or_assets")
        return {
            "honest_report_status": "need_download",
            "honest_attribution_status": "hypothesis_ready" if raw_attr else "",
            "honest_conclusion": f"命中既有报告草稿，但证据未齐（gate={gate_decision or 'unknown'}），不作为可交付；待下载/解析后再出 RCA 结论。",
            "gate_decision": gate_decision,
            "gate_green": gate_green,
            "parsed_l2_assets_present": assets_present,
            "anomaly": True,
            "anomaly_reasons": anomaly_reasons,
            "candidate_cause_mode": "low_confidence_hypothesis" if candidate_cause else "",
            "responsibility_candidate_mode": "suppressed_until_human_gate" if responsibility_candidate else "",
        }
    if raw_report_status == "html_delivery_ready" and gate_green and assets_present:
        conclusion = "RCA 报告已生成"
    else:
        conclusion = _compact("")
    return {
        "honest_report_status": raw_report_status,
        "honest_attribution_status": raw_attr,
        "honest_conclusion": conclusion,
        "gate_decision": gate_decision,
        "gate_green": gate_green,
        "parsed_l2_assets_present": assets_present,
        "anomaly": bool(anomaly_reasons),
        "anomaly_reasons": anomaly_reasons,
        "candidate_cause_mode": "normal" if candidate_cause else "",
        "responsibility_candidate_mode": "normal" if responsibility_candidate else "",
    }


def downgrade_g1q3_notice_text(text: str, verdict: dict[str, Any]) -> str:
    """Replace false-green L0/L1 prose with the shared gate-truth wording."""
    if not isinstance(verdict, dict) or verdict.get("gate_green") is True:
        return text
    decision = str(verdict.get("gate_decision") or "non_green")
    honest = str(verdict.get("honest_conclusion") or f"intake 与准入校验完成；待下载/解析数据后再出 RCA 结论（gate={decision}）")
    original = str(text or "").strip()
    if not original:
        return honest
    # Keep a short evidence/hypothesis tail without deliverable wording.
    cleaned = FALSE_GREEN_RE.sub("待下载解析", original)
    cleaned = re.sub(r"(?im)^.*(?:报告链接|report link|html link|html_url|artifact_path|artifact link).*$", "", cleaned)
    # Under a non-green gate there is no deliverable report, so any bare CIFS/UNC
    # (//hfs...) or VM (/mnt/tmp/...) file-share link is a dead/unverified pointer
    # and must not be shown (Feishu mis-renders "//" as a refused HTTPS link, or
    # the file simply does not exist).  Regression: issue 7025381565 emitted a
    # dead html_url to an L0_L1_issue_intake_summary.md that was never written.
    cleaned = re.sub(r"(?im)^.*(?://hfs\S+|/mnt/tmp/\S+).*$", "", cleaned)
    cleaned = re.sub(r"(?im)^.*(?:责任候选|责任人|candidate_responsibility|responsibility_candidate).*$", "候选责任人：低置信假设，待人工确认", cleaned)
    cleaned = re.sub(r"(?im)^.*(?:候选原因|candidate_cause).*$", "候选原因：低置信假设，待人工确认", cleaned)
    return honest + "\n\n" + _compact(cleaned, limit=900)
