from scripts.pnc_rca_batch_rerun import _approval, _request, _terminal_failure


def _snapshot(
    *, job_status="delivered", job_outcome="success", effect_status="succeeded"
):
    return {
        "generation": 6,
        "submission_key": "submission-6",
        "delivery_id": "delivery-6",
        "job_status": job_status,
        "job_outcome": job_outcome,
        "outcome_key": "decoded_no_supported_causal_chain",
        "terminal_state": "",
        "terminal_error_code": "",
        "issue_url": "https://project.feishu.cn/t03o4q/issue/detail/7048803418",
        "report_url": "https://g1q3-rca.minieye.tech/report.html",
        "manifest_json": "{}",
        "artifacts_json": "{}",
        "effects": [
            {
                "effect_kind": "feishu_issue_comment",
                "required": 1,
                "status": effect_status,
                "remote_receipt": {
                    "remote_id": "7665000000000000000",
                    "confirmed_field_keys": ["field_9193cb", "field_8c912e"],
                    "source": "read_after_write",
                },
                "completed_at": "2026-07-24T06:00:00+00:00",
                "last_error_code": "",
            }
        ],
    }


def test_approval_accepts_issue_only_official_readback():
    approval = _approval(_snapshot())

    assert approval is not None
    assert approval["generation"] == 6
    assert approval["official_comment_id"] == "7665000000000000000"
    assert approval["official_field_keys"] == ["field_8c912e", "field_9193cb"]


def test_approval_waits_for_required_effect_and_surfaces_terminal_failure():
    pending = _snapshot(job_status="partial", effect_status="retry_wait")
    failed = _snapshot(job_status="partial", job_outcome="terminal_failed")

    assert _approval(pending) is None
    assert _terminal_failure(pending) is None
    failure = _terminal_failure(failed)
    assert failure is not None
    assert failure["job_status"] == "partial"


def test_batch_request_is_operator_issue_only_and_deterministic():
    request = _request(
        batch_id="gray-20260724",
        issue_id="7048803418",
        request_index=1,
        requester_id="operator-songying",
    )

    assert request.platform == "operator"
    assert request.chat_id == request.thread_id == ""
    assert request.message_id == "gray-20260724-7048803418-try-1"
