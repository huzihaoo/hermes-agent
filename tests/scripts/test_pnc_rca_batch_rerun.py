import json
import sqlite3

from scripts.pnc_rca_batch_rerun import (
    _approval,
    _issue_snapshot,
    _request,
    _terminal_failure,
)


def _snapshot(
    *,
    job_status="delivered",
    job_outcome="success",
    effect_status="succeeded",
    causal=True,
):
    contract = (
        {
            "report": {
                "candidate_owner": "ACC decoded 证据",
                "diagnostic_only": False,
            },
            "artifacts": {
                "attribution_causal_text": "ACC 退出判据命中，指向状态机抑制标志。"
            },
            "public_result": {
                "summary": {"status": "candidate"},
                "responsibility": {"status": "candidate"},
                "terminal_diagnostic": {},
            },
        }
        if causal
        else {
            "report": {"candidate_owner": "", "diagnostic_only": True},
            "artifacts": {"attribution_causal_text": ""},
            "public_result": {
                "summary": {"status": "diagnostic_report_ready"},
                "terminal_diagnostic": {"blocker_kind": "remote_event_not_found"},
            },
        }
    )
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
        "contract_json": json.dumps(contract),
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
    assert approval["quality"]["status"] == "causal_candidate"
    assert approval["quality"]["responsibility"] == "ACC decoded 证据"


def test_approval_rejects_delivered_noncausal_result():
    snapshot = _snapshot(causal=False)

    assert _approval(snapshot) is None
    failure = _terminal_failure(snapshot)
    assert failure is not None
    assert failure["job_status"] == "delivered"


def test_approval_accepts_decoded_data_binding_conflict_as_a_cause():
    snapshot = _snapshot()
    contract = json.loads(snapshot["contract_json"])
    contract["report"]["candidate_owner"] = "问题数据/回灌链路"
    contract["artifacts"]["attribution_causal_text"] = (
        "问题描述目标与绑定数据不一致，责任指向问题数据/回灌链路。"
    )
    contract["public_result"]["summary"]["status"] = "blocked"
    contract["public_result"]["responsibility"]["status"] = (
        "candidate_data_integrity_conflict"
    )
    snapshot["contract_json"] = json.dumps(contract)

    approval = _approval(snapshot)

    assert approval is not None
    assert approval["quality"]["responsibility"] == "问题数据/回灌链路"


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


def test_issue_snapshot_tracks_new_outbox_before_execution_watch_exists(tmp_path):
    db_path = tmp_path / "control.sqlite3"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE business_triggers (
            business_key TEXT,
            generation INTEGER,
            submission_key TEXT,
            work_item_id TEXT
        );
        CREATE TABLE rca_outbox (
            outbox_id INTEGER,
            business_key TEXT,
            generation INTEGER,
            status TEXT,
            last_error_code TEXT,
            last_error_detail TEXT,
            completed_at TEXT
        );
        CREATE TABLE rca_execution_watch (
            submission_outbox_id INTEGER
        );
        CREATE TABLE rca_delivery_jobs (
            submission_key TEXT,
            delivery_id TEXT,
            status TEXT,
            outcome TEXT,
            outcome_key TEXT,
            terminal_state TEXT,
            terminal_error_code TEXT,
            issue_url TEXT,
            report_url TEXT,
            manifest_json TEXT,
            contract_json TEXT,
            artifacts_json TEXT,
            updated_at TEXT
        );
        CREATE TABLE rca_delivery_effects (
            delivery_id TEXT,
            effect_key TEXT,
            effect_kind TEXT,
            required INTEGER,
            target_key TEXT,
            status TEXT,
            remote_receipt_json TEXT,
            last_error_code TEXT,
            completed_at TEXT,
            updated_at TEXT
        );
        INSERT INTO business_triggers VALUES (
            'business-1', 6, 'submission-6', '7048803418'
        );
        INSERT INTO rca_outbox VALUES (
            378, 'business-1', 6, 'pending', '', '', NULL
        );
        """
    )
    conn.commit()
    conn.close()

    snapshot = _issue_snapshot(db_path, "7048803418", submission_key="submission-6")

    assert snapshot is not None
    assert snapshot["generation"] == 6
    assert snapshot["submission_key"] == "submission-6"
    assert snapshot["outbox_status"] == "pending"
