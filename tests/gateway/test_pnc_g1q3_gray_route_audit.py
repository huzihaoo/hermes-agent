import json

from scripts.pnc_g1q3_gray_route_audit import build_audit, format_markdown


def _write(path, rows):
    path.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n", encoding="utf-8")


def test_gray_route_audit_counts_decisions_and_false_rejection_candidates(tmp_path):
    _write(tmp_path / "2026-06-10.jsonl", [
        {
            "event_type": "group_binding_decision",
            "timestamp": "2026-06-10T08:00:00+00:00",
            "decision": "accepted",
            "template_id": "rca_case_status_check",
            "route_surface": "rca_case_status_check",
            "risk_gate": "execution_layer",
            "reason": None,
            "message_id": "om_ok",
        },
        {
            "event_type": "group_binding_decision",
            "timestamp": "2026-06-10T08:01:00+00:00",
            "decision": "reject",
            "reason": "unsupported_task_template",
            "message_id": "om_reject",
            "decision_snapshot": {"user_message": "通用拒答"},
        },
        {"event_type": "handoff_submission", "timestamp": "2026-06-10T08:02:00+00:00"},
    ])

    audit = build_audit(tmp_path, since_days=9999)

    assert audit["total_decisions"] == 2
    assert audit["counts"]["by_decision"] == {"accepted": 1, "reject": 1}
    assert audit["false_rejection_candidate_count"] == 1
    assert audit["false_rejection_candidates"][0]["message_id"] == "om_reject"


def test_gray_route_audit_markdown_is_business_readable(tmp_path):
    _write(tmp_path / "2026-06-10.jsonl", [
        {"event_type": "group_binding_decision", "timestamp": "2026-06-10T08:00:00+00:00", "decision": "accepted", "route_surface": "rca_case_evidence_summary", "reason": None},
    ])

    text = format_markdown(build_audit(tmp_path, since_days=9999))

    assert "G1Q3 RCA 灰度路由审计" in text
    assert "接单 accepted=1" in text
    assert "rca_case_evidence_summary" in text
