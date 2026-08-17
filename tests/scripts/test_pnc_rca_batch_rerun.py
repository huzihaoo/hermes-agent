import hashlib
import json
import sqlite3
import threading
import time
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from gateway.pnc_rca_abstention_projection import (
    build_gate_a_identifier_binding,
    build_gate_a_public_result,
    project_gate_a_report,
)
from gateway.pnc_rca_issue_focus import ANALYSIS_INSUFFICIENT_STATEMENT
from scripts import pnc_rca_batch_rerun as batch_rerun
from scripts.pnc_rca_batch_rerun import (
    ACCEPTANCE_AXES,
    CONTROL_STORE_SCHEMA_VERSION,
    DRY_RUN_SCHEMA_VERSION,
    OWNER_RECEIPT_EFFECT_SCOPE,
    OWNER_RECEIPT_NO_OTHER_TASK_BOUNDARY,
    OWNER_RECEIPT_SCHEMA_VERSION,
    QUEUE_AUTHORITY_FLAGS,
    QUEUE_SCHEMA_VERSION,
    QUEUE_SCOPE,
    SCHEMA_VERSION,
    BatchRerunError,
    _approval,
    _acceptance_axes,
    _batch_completion,
    _batch_terminal_authority,
    _historical_epoch_rerun_authority,
    _historical_epoch_rerun_ineligibility,
    _issue_snapshot,
    _load_queue,
    _load_or_create_state,
    _owner_receipt_binding,
    _queue_precondition_matches,
    _request,
    _silent_terminal_authority,
    _terminal_failure,
    _write_state,
)


def _owner_receipt(
    *,
    batch_id="gray-20260724",
    queue_sha256="1" * 64,
    selected_issue_ids=None,
    requester_id="automation:rca-batch-rerun",
    **changes,
):
    value = {
        "schema_version": OWNER_RECEIPT_SCHEMA_VERSION,
        "approved": True,
        "batch_id": batch_id,
        "queue_sha256": queue_sha256,
        "selected_issue_ids": sorted(selected_issue_ids or ["7048803418"]),
        "production_effects": dict(OWNER_RECEIPT_EFFECT_SCOPE),
        "no_other_task_boundary": dict(OWNER_RECEIPT_NO_OTHER_TASK_BOUNDARY),
        "approved_by": "owner:胡子豪",
        "approved_at": datetime.now(timezone.utc).isoformat(),
        "requester_id": requester_id,
        "reason": f"production_gray_batch:{batch_id}",
        "activation_required": True,
        "runtime_commit": "a" * 40,
        "runtime_tree": "b" * 40,
    }
    value.update(changes)
    return value


def _write_owner_receipt(path, value=None):
    value = value or _owner_receipt()
    path.write_bytes(
        (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()
    )
    path.chmod(0o600)
    return path


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
        "activation_epoch_id": "rca-current",
        "activation_ledger_id": 61,
        "outbox_activation_epoch_id": "rca-current",
        "outbox_activation_ledger_id": 61,
        "current_activation_epoch_id": "rca-current",
        "current_activation_state": "steady_active",
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
                "write_phase": "settled",
                "attempt": 1,
                "provider_attempt_count": 1,
                "remote_receipt_present": True,
            }
        ],
    }


def test_historical_epoch_authority_accepts_exact_null_binding_and_rejects_write_evidence():
    snapshot = {
        **_snapshot(),
        "generation": 1,
        "submission_key": "g1q3-rca-s1-" + "a" * 64,
        "activation_epoch_id": None,
        "activation_ledger_id": None,
        "outbox_activation_epoch_id": None,
        "outbox_activation_ledger_id": None,
        "effects": [],
    }
    authority = _historical_epoch_rerun_authority(
        snapshot=snapshot,
        batch_id="batch-history",
        queue_sha256="1" * 64,
        issue_id="7048803418",
        owner_receipt_path="/tmp/owner.json",
        owner_receipt_sha256="2" * 64,
        requester_id="automation:rca-batch-rerun",
        reason="production_gray_batch:batch-history",
    )

    assert authority is not None
    assert authority["prior_activation_epoch_id"] == ""
    assert authority["target_activation_epoch_id"] == "rca-current"
    stale_task = {
        **snapshot,
        "watch_state": "pending",
        "watch_task_id": "stale-vm-task-7048803418",
        "watch_lease_token": "expired-token",
        "watch_lease_owner": "retired-worker",
        "watch_lease_expires_at": "2026-07-24T00:00:00+00:00",
    }
    assert _historical_epoch_rerun_ineligibility(stale_task) == ""
    assert _historical_epoch_rerun_authority(
        snapshot=stale_task,
        batch_id="batch-history",
        queue_sha256="1" * 64,
        issue_id="7048803418",
        owner_receipt_path="/tmp/owner.json",
        owner_receipt_sha256="2" * 64,
        requester_id="automation:rca-batch-rerun",
        reason="production_gray_batch:batch-history",
    ) is not None
    unsafe = {
        **snapshot,
        "effects": [{"write_phase": "write_started"}],
    }
    assert _historical_epoch_rerun_ineligibility(unsafe) == "prior_write_started"
    assert (
        _historical_epoch_rerun_authority(
            snapshot=unsafe,
            batch_id="batch-history",
            queue_sha256="1" * 64,
            issue_id="7048803418",
            owner_receipt_path="/tmp/owner.json",
            owner_receipt_sha256="2" * 64,
            requester_id="automation:rca-batch-rerun",
            reason="production_gray_batch:batch-history",
        )
        is None
    )


def test_approval_accepts_issue_only_official_readback():
    approval = _approval(_snapshot())

    assert approval is not None
    assert approval["generation"] == 6
    assert approval["official_comment_id"] == "7665000000000000000"
    assert approval["official_field_keys"] == ["field_8c912e", "field_9193cb"]
    assert approval["quality"]["status"] == "causal_candidate"
    assert approval["quality"]["responsibility"] == "ACC decoded 证据"
    assert approval["acceptance_axis"] == "causal"
    assert approval["acceptance"]["transport"]["status"] == "pass"
    assert approval["acceptance"]["causal_attribution"]["status"] == "pass"


def test_approval_rejects_delivered_noncausal_result():
    snapshot = _snapshot(causal=False)

    assert _approval(snapshot) is None
    failure = _terminal_failure(snapshot)
    assert failure is not None
    assert failure["job_status"] == "delivered"
    assert failure["acceptance_axis"] == "causal"
    assert failure["acceptance"]["transport"]["status"] == "pass"
    assert failure["acceptance"]["causal_attribution"] == {
        "status": "not_ready",
        "reason": "causal_quality_not_satisfied",
    }


def test_batch_completion_rejects_confirmed_noncausal_field_write():
    snapshot = _snapshot(causal=False)

    assert _batch_completion(snapshot) is None


def test_transport_completion_keeps_gate_a_observation_semantically_not_ready():
    snapshot = _snapshot(causal=False)
    snapshot["contract_json"] = json.dumps(_observational_contract())

    completion = _batch_completion(snapshot, acceptance_axis="transport")

    assert completion is not None
    assert completion["acceptance_axis"] == "transport"
    assert completion["acceptance"]["transport"] == {
        "status": "pass",
        "official_comment_id": "7665000000000000000",
        "official_field_keys": ["field_8c912e", "field_9193cb"],
        "official_readback_source": "read_after_write",
    }
    assert completion["acceptance"]["causal_attribution"] == {
        "status": "not_ready",
        "reason": "gate_a_projection_not_causal",
    }
    assert completion["quality"] == {
        "status": "not_ready",
        "reason": "gate_a_projection_not_causal",
    }
    assert _approval(snapshot) is None
    assert _terminal_failure(snapshot, acceptance_axis="transport") is None
    causal_failure = _terminal_failure(snapshot)
    assert causal_failure is not None
    assert causal_failure["acceptance"] == completion["acceptance"]


def test_transport_completion_projects_collector_execution_identity():
    readback = {
        "schema_version": "pnc_rca_execution_identity_readback_v1",
        "source": "host_collector_canonical_vm_receipts_v1",
    }
    snapshot = {
        **_snapshot(causal=False),
        "execution_identity_readback": readback,
    }

    completion = _batch_completion(snapshot, acceptance_axis="transport")

    assert completion is not None
    assert completion["execution_identity_readback"] == readback


def test_watch_execution_identity_reads_only_collector_status_projection():
    readback = {
        "schema_version": "pnc_rca_execution_identity_readback_v1",
        "source": "host_collector_canonical_vm_receipts_v1",
    }

    assert batch_rerun._watch_execution_identity(
        json.dumps({"execution_identity_readback": readback})
    ) == readback
    assert batch_rerun._watch_execution_identity(json.dumps(readback)) is None
    assert batch_rerun._watch_execution_identity("not-json") is None


def test_transport_completion_requires_official_provider_readback():
    snapshot = _snapshot(causal=False)
    snapshot["effects"][0]["remote_receipt"]["source"] = "write_response"

    assert _batch_completion(snapshot, acceptance_axis="transport") is None
    assert _acceptance_axes(snapshot) == {
        "transport": {
            "status": "not_ready",
            "reason": "write_readback_not_confirmed",
        },
        "causal_attribution": {
            "status": "not_ready",
            "reason": "causal_quality_not_satisfied",
        },
    }


@pytest.mark.parametrize("source", ["read_before_write", "recovery_read_before_write"])
def test_transport_axis_does_not_treat_reconciliation_as_release_write(source):
    snapshot = _snapshot()
    snapshot["effects"][0]["remote_receipt"]["source"] = source

    causal_completion = _batch_completion(snapshot)

    assert causal_completion is not None
    assert causal_completion["acceptance"]["transport"] == {
        "status": "not_ready",
        "reason": "write_readback_not_confirmed",
    }
    assert causal_completion["acceptance"]["causal_attribution"]["status"] == "pass"
    assert _batch_completion(snapshot, acceptance_axis="transport") is None


def test_batch_completion_requires_provider_readback():
    snapshot = _snapshot(causal=False)
    snapshot["effects"][0]["remote_receipt"]["source"] = "write_response"

    assert _batch_completion(snapshot) is None


@pytest.mark.parametrize(
    "source",
    [
        "read_after_recovery_write",
        "read_before_write",
        "recovery_read_before_write",
    ],
)
def test_batch_completion_rejects_noncausal_reconciliation_readback(source):
    snapshot = _snapshot(causal=False)
    snapshot["effects"][0]["remote_receipt"]["source"] = source

    assert _batch_completion(snapshot) is None


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


def test_approval_rejects_explicit_focus_stop_as_attribution_completion():
    from tests.gateway.test_pnc_rca_delivery_contract import _focus_payload

    snapshot = _snapshot(causal=False)
    snapshot["contract_json"] = json.dumps({
        "issue_focus": _focus_payload(
            "HMI-S弯",
            status=ANALYSIS_INSUFFICIENT_STATEMENT,
        )
    })

    assert _approval(snapshot, issue_title="HMI-S弯") is None


def _observational_contract():
    binding = build_gate_a_identifier_binding({
        "actual_evaluators": [
            {"evaluator_id": "acc_jerk", "status": "supported"},
        ],
        "actual_signals": ["ACC_AccelerationRequestMps2"],
        "actual_fields": [],
    })
    projection = project_gate_a_report(
        {
            "input_materialized": True,
            "rca_evaluators": [
                {
                    "key": "acc_jerk",
                    "status": "supported",
                    "evidence_refs": [
                        {"signal": "ACC_AccelerationRequestMps2", "max_delta": 1.0}
                    ],
                }
            ],
        },
        identifier_binding=binding,
    )
    return {
        "report": {"diagnostic_only": False},
        "artifacts": {},
        "consumer_capability": {
            "actual_evaluators": [
                {"evaluator_id": "acc_jerk", "status": "supported"},
            ],
            "actual_signals": ["ACC_AccelerationRequestMps2"],
            "actual_fields": [],
        },
        "gate_a_projection": projection,
        "public_result": build_gate_a_public_result(projection),
    }


def test_approval_rejects_canonical_l1_observation_as_attribution_completion():
    snapshot = _snapshot(causal=False)
    snapshot["contract_json"] = json.dumps(_observational_contract())

    assert _approval(snapshot) is None


@pytest.mark.parametrize(
    "mutation",
    [
        "diagnostic",
        "diagnostic_string",
        "candidate_owner",
        "causal_text",
        "causal_fields_with_projection",
        "tampered_public_result",
        "binding_mismatch",
        "terminal_diagnostic",
        "l0_abstention",
    ],
)
def test_approval_rejects_noncanonical_observational_terminal(mutation):
    snapshot = _snapshot(causal=False)
    contract = _observational_contract()
    if mutation == "diagnostic":
        contract["report"]["diagnostic_only"] = True
    elif mutation == "diagnostic_string":
        contract["report"]["diagnostic_only"] = "true"
    elif mutation == "candidate_owner":
        contract["report"]["candidate_owner"] = "ACC"
    elif mutation == "causal_text":
        contract["artifacts"]["attribution_causal_text"] = "ACC 导致问题"
    elif mutation == "causal_fields_with_projection":
        contract["report"]["candidate_owner"] = "ACC"
        contract["artifacts"]["attribution_causal_text"] = "ACC 导致问题"
    elif mutation == "tampered_public_result":
        contract["public_result"]["summary"]["short_conclusion"] = "已完成归因"
    elif mutation == "binding_mismatch":
        contract["consumer_capability"]["actual_evaluators"] = [
            {"evaluator_id": "other_evaluator", "status": "supported"},
        ]
    elif mutation == "terminal_diagnostic":
        contract["terminal_diagnostic"] = {"blocker_kind": "remote_event_not_found"}
    else:
        projection = project_gate_a_report({
            "input_materialized": False,
            "failure_class": "remote_event_not_found",
        })
        contract["gate_a_projection"] = projection
        contract["public_result"] = build_gate_a_public_result(projection)
    snapshot["contract_json"] = json.dumps(contract)

    assert _approval(snapshot) is None


def test_approval_waits_for_required_effect_and_surfaces_terminal_failure():
    pending = _snapshot(job_status="partial", effect_status="retry_wait")
    failed = _snapshot(job_status="partial", job_outcome="terminal_failed")

    assert _approval(pending) is None
    assert _terminal_failure(pending) is None
    failure = _terminal_failure(failed)
    assert failure is not None
    assert failure["job_status"] == "partial"


@pytest.mark.parametrize(
    "error_code",
    [
        "delivery_lineage_unavailable",
        "failure_receipt_missing",
        "rca_work_deadline_exceeded",
        "report_public_origin_invalid",
        "service_provenance_unavailable",
        "taxonomy_gap:derived_capacity_hfs_target_identity_mismatch",
        "taxonomy_gap:derived_capacity_reservation_activate_failed",
        "taxonomy_gap:gate_a_projection_invalid",
        "taxonomy_gap:viz_evidence_unavailable",
        "remote_evidence_domain_unsupported",
        "taxonomy_gap:remote_evidence_domain_unsupported",
    ],
)
def test_terminal_failure_surfaces_silent_watch_without_delivery_job(error_code):
    failure = _terminal_failure(
        {
            **_snapshot(job_status=""),
            "delivery_id": None,
            "effects": [],
            "watch_state": "terminal_failed",
            "watch_delivery_id": None,
            "watch_error_code": error_code,
        }
    )

    assert failure == {
        "job_status": "",
        "job_outcome": "",
        "outcome_key": "",
        "terminal_state": "watch_terminal_failed",
        "terminal_error_code": error_code,
        "effects": [],
    }


def test_terminal_failure_does_not_expand_to_other_silent_watch_codes():
    snapshot = {
        **_snapshot(job_status=""),
        "delivery_id": None,
        "effects": [],
        "watch_state": "terminal_failed",
        "watch_delivery_id": None,
        "watch_error_code": "vm_status_missing",
    }

    assert _terminal_failure(snapshot) is None


def test_batch_request_is_operator_issue_only_and_deterministic():
    request = _request(
        batch_id="gray-20260724",
        issue_id="7048803418",
        request_index=1,
        requester_id="automation:rca-batch-rerun",
    )

    assert request.platform == "operator"
    assert request.chat_id == request.thread_id == ""
    assert request.message_id == "gray-20260724-7048803418-try-1"
    assert request.requester_id == "automation:rca-batch-rerun"


@pytest.mark.parametrize(
    "error_code",
    [
        "delivery_lineage_unavailable",
        "failure_receipt_missing",
        "rca_work_deadline_exceeded",
        "service_provenance_unavailable",
        "taxonomy_gap:gate_a_projection_invalid",
        "taxonomy_gap:viz_evidence_unavailable",
        "remote_evidence_domain_unsupported",
        "taxonomy_gap:remote_evidence_domain_unsupported",
    ],
)
def test_silent_terminal_authority_requires_exact_deadline_no_delivery(
    tmp_path, error_code
):
    batch_id = "gray-20260724"
    reason = f"production_gray_batch:{batch_id}"
    snapshot = {
        **_snapshot(),
        "generation": 1,
        "submission_key": "g1q3-rca-s1-" + "a" * 64,
        "watch_state": "terminal_failed",
        "watch_delivery_id": None,
        "watch_error_code": error_code,
    }
    authority = _silent_terminal_authority(
        snapshot=snapshot,
        batch_id=batch_id,
        queue_sha256="1" * 64,
        issue_id="7048803418",
        owner_receipt_path=str(tmp_path / "owner.json"),
        owner_receipt_sha256="2" * 64,
        requester_id="automation:rca-batch-rerun",
        reason=reason,
    )

    assert authority is not None
    assert authority["prior_submission_key"] == snapshot["submission_key"]
    assert authority["owner_receipt_path"] == str(tmp_path / "owner.json")
    for changes in (
        {"watch_state": "delivery_created"},
        {"watch_delivery_id": "delivery-1"},
        {"watch_error_code": "vm_status_missing"},
    ):
        assert _silent_terminal_authority(
            snapshot={**snapshot, **changes},
            batch_id=batch_id,
            queue_sha256="1" * 64,
            issue_id="7048803418",
            owner_receipt_path=str(tmp_path / "owner.json"),
            owner_receipt_sha256="2" * 64,
            requester_id="automation:rca-batch-rerun",
            reason=reason,
        ) is None


def test_batch_terminal_authority_accepts_any_settled_owner_approved_delivery(
    tmp_path,
):
    base = {
        **_snapshot(),
        "submission_key": "g1q3-rca-s1-" + "a" * 64,
        "watch_state": "delivery_created",
        "watch_delivery_id": "delivery-6",
        "terminal_error_code": "",
    }
    authority = _batch_terminal_authority(
        snapshot=base,
        batch_id="gray-20260724",
        queue_sha256="1" * 64,
        issue_id="7048803418",
        owner_receipt_path=str(tmp_path / "owner.json"),
        owner_receipt_sha256="2" * 64,
        requester_id="automation:rca-batch-rerun",
        reason="production_gray_batch:gray-20260724",
    )
    assert authority is not None
    assert authority["terminal_mode"] == "settled_delivery_correction"
    assert _batch_terminal_authority(
        snapshot={
            **base,
            "effects": [{**base["effects"][0], "status": "retry_wait"}],
        },
        batch_id="gray-20260724",
        queue_sha256="1" * 64,
        issue_id="7048803418",
        owner_receipt_path=str(tmp_path / "owner.json"),
        owner_receipt_sha256="2" * 64,
        requester_id="automation:rca-batch-rerun",
        reason="production_gray_batch:gray-20260724",
    ) is None


def test_owner_receipt_binding_hashes_exact_owner_only_file(tmp_path):
    receipt = tmp_path / "owner-receipt.json"
    _write_owner_receipt(receipt)
    raw = receipt.read_bytes()

    path, sha256 = _owner_receipt_binding(
        receipt,
        expected_batch_id="gray-20260724",
        expected_queue_sha256="1" * 64,
        expected_issue_ids=["7048803418"],
        expected_requester_id="automation:rca-batch-rerun",
        expected_runtime_commit="a" * 40,
        expected_runtime_tree="b" * 40,
    )

    assert path == str(receipt)
    assert sha256 == hashlib.sha256(raw).hexdigest()

    receipt.chmod(0o644)
    with pytest.raises(BatchRerunError, match="batch_owner_receipt_identity_invalid"):
        _owner_receipt_binding(receipt)


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("approved", False, "batch_owner_receipt_not_approved"),
        ("selected_issue_ids", ["7048803419"], "batch_owner_receipt_selection_mismatch"),
        ("production_effects", {"other_task": True}, "batch_owner_receipt_effect_scope_invalid"),
        (
            "no_other_task_boundary",
            {"mode": "shared"},
            "batch_owner_receipt_task_boundary_invalid",
        ),
        ("activation_required", False, "batch_owner_receipt_activation_required"),
        ("runtime_commit", "0" * 40, "batch_owner_receipt_runtime_invalid"),
    ],
)
def test_owner_receipt_semantics_fail_closed(tmp_path, field, value, error):
    receipt = tmp_path / f"owner-{field}.json"
    _write_owner_receipt(receipt, _owner_receipt(**{field: value}))
    with pytest.raises(BatchRerunError, match=error):
        _owner_receipt_binding(
            receipt,
            expected_batch_id="gray-20260724",
            expected_queue_sha256="1" * 64,
            expected_issue_ids=["7048803418"],
            expected_requester_id="automation:rca-batch-rerun",
        )


def test_owner_receipt_rejects_noncanonical_and_minimal_marker(tmp_path):
    receipt = tmp_path / "owner-invalid.json"
    receipt.write_bytes(b'{"approved":true}\n')
    receipt.chmod(0o600)
    with pytest.raises(BatchRerunError, match="batch_owner_receipt_schema_invalid"):
        _owner_receipt_binding(receipt)

    valid = _owner_receipt()
    receipt.write_bytes(
        (json.dumps(valid, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()
    )
    receipt.chmod(0o600)
    with pytest.raises(BatchRerunError, match="batch_owner_receipt_noncanonical"):
        _owner_receipt_binding(receipt)


def test_batch_state_v4_binds_owner_receipt_path_hash_selection_and_gate(tmp_path):
    state_path = tmp_path / "batch-state.json"
    owner_path = str(tmp_path / "owner-receipt.json")
    values = {
        "batch_id": "gray-20260724",
        "queue_sha256": "1" * 64,
        "runtime_commit": "2" * 40,
        "runtime_tree": "4" * 40,
        "owner_receipt_path": owner_path,
        "owner_receipt_sha256": "3" * 64,
        "selected_issue_ids": ["7048803418"],
        "acceptance_axis": "transport",
    }
    state = _load_or_create_state(state_path, **values)
    _write_state(state_path, state)

    assert state["schema_version"] == SCHEMA_VERSION
    assert state["owner_receipt_path"] == owner_path
    assert state["owner_receipt_sha256"] == "3" * 64
    assert state["selected_issue_ids"] == ["7048803418"]
    assert state["activation_required"] is True
    assert state["runtime_tree"] == "4" * 40
    assert state["acceptance_axis"] == "transport"
    with pytest.raises(BatchRerunError, match="batch_state_binding_mismatch"):
        _load_or_create_state(
            state_path,
            **{**values, "owner_receipt_sha256": "4" * 64},
        )
    with pytest.raises(BatchRerunError, match="batch_state_binding_mismatch"):
        _load_or_create_state(
            state_path,
            **{**values, "runtime_commit": "5" * 40},
        )
    with pytest.raises(BatchRerunError, match="batch_state_binding_mismatch"):
        _load_or_create_state(
            state_path,
            **{**values, "acceptance_axis": "causal"},
        )


def test_batch_acceptance_axis_contract_is_explicit_and_defaults_to_causal():
    parser = batch_rerun._parser()
    required = [
        "--control-db", "control.sqlite3",
        "--queue", "queue.json",
        "--state", "state.json",
        "--batch-id", "release-canary",
        "--expected-runtime-commit", "a" * 40,
        "--expected-runtime-tree", "b" * 40,
    ]

    assert ACCEPTANCE_AXES == {"causal", "transport"}
    assert parser.parse_args(required).acceptance_axis == "causal"
    assert (
        parser.parse_args([*required, "--acceptance-axis", "transport"]).acceptance_axis
        == "transport"
    )


def _queue_value(*, current_submission_key=None, current_generation=1):
    issue_id = "7048803418"
    submission_key = (
        "g1q3-rca-s1-" + "a" * 64
        if current_submission_key is None
        else current_submission_key
    )
    return {
        "schema_version": QUEUE_SCHEMA_VERSION,
        "batch_id": "gray-20260724",
        "project_key": "t03o4q",
        "scope": {
            **QUEUE_SCOPE,
            "issue_count": 1,
            "issue_ids_sha256": hashlib.sha256(
                f"{issue_id}\n".encode("utf-8")
            ).hexdigest(),
        },
        "source_inventory_sha256": "c" * 64,
        "authority_flags": dict(QUEUE_AUTHORITY_FLAGS),
        "items": [
            {
                "issue_id": issue_id,
                "title": "ACC braking issue",
                "quality_classification": "missing",
                "current_submission_key": submission_key,
                "current_generation": current_generation,
                "priority": 1,
                "project_key": "t03o4q",
            }
        ],
    }


def test_queue_schema_binds_exact_and_scope_and_control_precondition(tmp_path):
    queue_path = tmp_path / "queue.json"
    value = _queue_value()
    queue_path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")
    items, digest = _load_queue(queue_path, expected_batch_id="gray-20260724")
    assert items[0]["queue_submission_key"].startswith("g1q3-rca-s1-")
    assert items[0]["queue_generation"] == 1
    assert len(digest) == 64
    for field, replacement in (
        ("schema_version", "wrong"),
        ("batch_id", "other-batch"),
        ("project_key", "other-project"),
    ):
        bad = dict(value)
        bad[field] = replacement
        queue_path.write_text(json.dumps(bad, sort_keys=True), encoding="utf-8")
        with pytest.raises(BatchRerunError, match="batch_queue_schema_invalid"):
            _load_queue(queue_path, expected_batch_id="gray-20260724")
    bad_item = dict(value)
    bad_item["items"] = [{**value["items"][0], "current_submission_key": ""}]
    queue_path.write_text(json.dumps(bad_item, sort_keys=True), encoding="utf-8")
    with pytest.raises(BatchRerunError, match="batch_queue_item_invalid"):
        _load_queue(queue_path, expected_batch_id="gray-20260724")

    bad_scope = dict(value)
    bad_scope["scope"] = {**value["scope"], "logic": "OR"}
    queue_path.write_text(json.dumps(bad_scope, sort_keys=True), encoding="utf-8")
    with pytest.raises(BatchRerunError, match="batch_queue_scope_invalid"):
        _load_queue(queue_path, expected_batch_id="gray-20260724")


def test_queue_accepts_explicit_absent_control_precondition(tmp_path):
    queue_path = tmp_path / "queue.json"
    queue_path.write_text(
        json.dumps(
            _queue_value(current_submission_key="", current_generation=0),
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    [item], _digest = _load_queue(
        queue_path, expected_batch_id="gray-20260724"
    )

    assert item["queue_submission_key"] == ""
    assert item["queue_generation"] == 0
    assert _queue_precondition_matches(item, None) is True
    assert _queue_precondition_matches(item, _snapshot()) is False


def _run_args(tmp_path, queue_path, owner_path):
    return SimpleNamespace(
        batch_id="gray-20260724",
        expected_runtime_commit="a" * 40,
        expected_runtime_tree="b" * 40,
        queue=str(queue_path),
        owner_receipt=str(owner_path),
        state=str(tmp_path / "state.json"),
        control_db=str(tmp_path / "control.sqlite3"),
        requester_id="automation:rca-batch-rerun",
        item_timeout_seconds=30,
        poll_seconds=1,
        retry_failed=False,
    )


def _write_dry_run_db(tmp_path, *, activation_state="steady_active"):
    db_path = tmp_path / "control.sqlite3"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE control_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE rca_activation_epochs (
            epoch_id TEXT PRIMARY KEY,
            state TEXT NOT NULL,
            is_current INTEGER NOT NULL
        );
        CREATE TABLE business_triggers (
            work_item_id TEXT NOT NULL,
            generation INTEGER NOT NULL,
            submission_key TEXT NOT NULL
        );
        CREATE TABLE rca_outbox (
            submission_key TEXT NOT NULL,
            generation INTEGER NOT NULL,
            status TEXT NOT NULL
        );
        """
    )
    conn.execute(
        "INSERT INTO control_meta(key, value) VALUES('schema_version', ?)",
        (CONTROL_STORE_SCHEMA_VERSION,),
    )
    conn.execute(
        "INSERT INTO rca_activation_epochs(epoch_id, state, is_current) "
        "VALUES('rca-activation-test', ?, 1)",
        (activation_state,),
    )
    conn.commit()
    conn.close()
    return db_path


def _file_state(path):
    if not path.exists():
        return {"present": False}
    observed = path.stat()
    return {
        "present": True,
        "device": observed.st_dev,
        "inode": observed.st_ino,
        "size": observed.st_size,
        "mtime_ns": observed.st_mtime_ns,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }


def test_dry_run_without_owner_receipt_is_read_only_and_not_authorized(
    tmp_path, monkeypatch
):
    queue_path = tmp_path / "queue.json"
    queue_path.write_text(
        json.dumps(
            _queue_value(current_submission_key="", current_generation=0),
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    db_path = _write_dry_run_db(tmp_path)
    state_path = tmp_path / "state.json"
    before = db_path.read_bytes()
    before_files = sorted(path.name for path in tmp_path.iterdir())
    args = _run_args(tmp_path, queue_path, tmp_path / "unused-owner.json")
    args.control_db = str(db_path)
    args.owner_receipt = None
    args.dry_run = True
    monkeypatch.setattr(
        batch_rerun, "_runtime_identity", lambda: ("a" * 40, "b" * 40)
    )
    monkeypatch.setattr(
        batch_rerun,
        "RcaControlStore",
        lambda _path: pytest.fail("dry-run opened the writable control store"),
    )

    result = batch_rerun.run(args)

    assert result["schema_version"] == DRY_RUN_SCHEMA_VERSION
    assert result["mode"] == "dry_run"
    assert result["execution_authorized"] is False
    assert result["owner_receipt"] == {
        "provided": False,
        "validated": False,
        "path": "",
        "sha256": "",
    }
    assert result["database"]["activation"]["ready"] is True
    assert result["database"]["preconditions"]["matched"] == 1
    assert result["database"]["source_snapshot"] == {
        "transport": "sqlite_online_backup",
        "source_open_mode": "read_only",
        "source_query_only": True,
        "copy_verified": True,
    }
    assert result["execution_policy"] == {
        "activation_required": True,
        "acceptance_axis": "causal",
        "daily_started_attempt_quota": None,
        "fixed_issue_allowlist": None,
        "selected_issue_count": 1,
    }
    assert result["external_effects_triggered"] is False
    assert not any(result["production_effects"].values())
    assert len(result["plan_sha256"]) == 64
    assert db_path.read_bytes() == before
    assert state_path.exists() is False
    assert sorted(path.name for path in tmp_path.iterdir()) == before_files


def test_dry_run_does_not_mutate_source_database_or_wal(tmp_path, monkeypatch):
    queue_path = tmp_path / "queue.json"
    queue_path.write_text(
        json.dumps(
            _queue_value(current_submission_key="", current_generation=0),
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    db_path = _write_dry_run_db(tmp_path)
    writer = sqlite3.connect(db_path)
    assert writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
    writer.execute("INSERT INTO control_meta VALUES('wal_probe', 'ready')")
    writer.commit()
    source_files = [
        db_path,
        db_path.with_name(f"{db_path.name}-wal"),
        db_path.with_name(f"{db_path.name}-shm"),
    ]
    assert all(path.exists() for path in source_files)
    before = {path.name: _file_state(path) for path in source_files}
    args = _run_args(tmp_path, queue_path, tmp_path / "unused-owner.json")
    args.control_db = str(db_path)
    args.owner_receipt = None
    args.dry_run = True
    monkeypatch.setattr(
        batch_rerun, "_runtime_identity", lambda: ("a" * 40, "b" * 40)
    )

    try:
        result = batch_rerun.run(args)
        after = {path.name: _file_state(path) for path in source_files}
        assert after[db_path.name] == before[db_path.name]
        assert after[f"{db_path.name}-wal"] == before[f"{db_path.name}-wal"]
        assert {
            key: value
            for key, value in after[f"{db_path.name}-shm"].items()
            if key != "sha256"
        } == {
            key: value
            for key, value in before[f"{db_path.name}-shm"].items()
            if key != "sha256"
        }
    finally:
        writer.close()

    assert result["database"]["source_snapshot"]["source_open_mode"] == "read_only"
    assert result["database"]["source_snapshot"]["source_query_only"] is True


def test_dry_run_uses_consistent_backup_during_active_wal_mutation(
    tmp_path, monkeypatch
):
    queue_path = tmp_path / "queue.json"
    queue_path.write_text(
        json.dumps(
            _queue_value(current_submission_key="", current_generation=0),
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    db_path = _write_dry_run_db(tmp_path)
    setup = sqlite3.connect(db_path)
    assert setup.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
    setup.execute(
        "CREATE TABLE backup_churn(id INTEGER PRIMARY KEY, value INTEGER NOT NULL)"
    )
    setup.execute("INSERT INTO backup_churn VALUES(1, 0)")
    setup.execute("CREATE TABLE backup_padding(payload BLOB NOT NULL)")
    setup.execute("INSERT INTO backup_padding VALUES(zeroblob(16777216))")
    setup.commit()
    setup.close()
    args = _run_args(tmp_path, queue_path, tmp_path / "unused-owner.json")
    args.control_db = str(db_path)
    args.owner_receipt = None
    args.dry_run = True
    monkeypatch.setattr(
        batch_rerun, "_runtime_identity", lambda: ("a" * 40, "b" * 40)
    )
    started = threading.Event()
    commits = []

    def mutate_wal():
        writer = sqlite3.connect(db_path, timeout=10)
        try:
            for value in range(1, 201):
                writer.execute("UPDATE backup_churn SET value = ? WHERE id = 1", (value,))
                writer.commit()
                commits.append(value)
                started.set()
                time.sleep(0.001)
        finally:
            writer.close()

    thread = threading.Thread(target=mutate_wal)
    thread.start()
    try:
        assert started.wait(timeout=5)
        before_backup = len(commits)
        result = batch_rerun.run(args)
        assert len(commits) > before_backup
    finally:
        thread.join(timeout=5)

    assert not thread.is_alive()
    assert result["database"]["quick_check"] == "ok"
    assert result["database"]["foreign_key_violations"] == 0
    assert result["database"]["preconditions"]["matched"] == 1


def test_dry_run_rejects_a_provided_invalid_owner_receipt_before_db(
    tmp_path, monkeypatch
):
    queue_path = tmp_path / "queue.json"
    queue_path.write_text(
        json.dumps(
            _queue_value(current_submission_key="", current_generation=0),
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    owner_path = tmp_path / "owner-invalid.json"
    _write_owner_receipt(owner_path, _owner_receipt(approved=False))
    args = _run_args(tmp_path, queue_path, owner_path)
    args.dry_run = True
    monkeypatch.setattr(
        batch_rerun, "_runtime_identity", lambda: ("a" * 40, "b" * 40)
    )
    monkeypatch.setattr(
        batch_rerun,
        "_dry_run_database_plan",
        lambda *_args: pytest.fail("invalid receipt reached the control DB"),
    )

    with pytest.raises(BatchRerunError, match="batch_owner_receipt_not_approved"):
        batch_rerun.run(args)


def test_dry_run_fails_closed_when_activation_is_not_steady(tmp_path, monkeypatch):
    queue_path = tmp_path / "queue.json"
    queue_path.write_text(
        json.dumps(
            _queue_value(current_submission_key="", current_generation=0),
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    args = _run_args(tmp_path, queue_path, tmp_path / "unused-owner.json")
    args.control_db = str(_write_dry_run_db(tmp_path, activation_state="aborted"))
    args.owner_receipt = None
    args.dry_run = True
    monkeypatch.setattr(
        batch_rerun, "_runtime_identity", lambda: ("a" * 40, "b" * 40)
    )

    with pytest.raises(BatchRerunError, match="batch_activation_not_ready"):
        batch_rerun.run(args)


def test_live_run_still_requires_owner_receipt(tmp_path, monkeypatch):
    queue_path = tmp_path / "queue.json"
    queue_path.write_text(
        json.dumps(
            _queue_value(current_submission_key="", current_generation=0),
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    args = _run_args(tmp_path, queue_path, tmp_path / "unused-owner.json")
    args.owner_receipt = None
    args.dry_run = False
    monkeypatch.setattr(
        batch_rerun, "_runtime_identity", lambda: ("a" * 40, "b" * 40)
    )

    with pytest.raises(BatchRerunError, match="batch_owner_receipt_required"):
        batch_rerun.run(args)


def test_live_run_rejects_a_receipt_not_bound_to_the_exact_queue(
    tmp_path, monkeypatch
):
    queue_path = tmp_path / "queue.json"
    queue_path.write_text(
        json.dumps(
            _queue_value(current_submission_key="", current_generation=0),
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    owner_path = tmp_path / "owner-mismatched.json"
    _write_owner_receipt(owner_path, _owner_receipt(queue_sha256="1" * 64))
    args = _run_args(tmp_path, queue_path, owner_path)
    args.dry_run = False
    monkeypatch.setattr(
        batch_rerun, "_runtime_identity", lambda: ("a" * 40, "b" * 40)
    )
    monkeypatch.setattr(
        batch_rerun,
        "_load_or_create_state",
        lambda *_args, **_kwargs: pytest.fail("mismatched receipt reached batch state"),
    )

    with pytest.raises(BatchRerunError, match="batch_owner_receipt_binding_mismatch"):
        batch_rerun.run(args)


def _write_run_inputs(tmp_path, queue):
    queue_path = tmp_path / "queue.json"
    queue_path.write_text(json.dumps(queue, sort_keys=True), encoding="utf-8")
    queue_sha256 = hashlib.sha256(queue_path.read_bytes()).hexdigest()
    owner_path = tmp_path / "owner.json"
    _write_owner_receipt(
        owner_path,
        _owner_receipt(queue_sha256=queue_sha256),
    )
    return queue_path, owner_path


def test_run_creates_initial_generation_for_exact_absent_item(tmp_path, monkeypatch):
    queue_path, owner_path = _write_run_inputs(
        tmp_path,
        _queue_value(current_submission_key="", current_generation=0),
    )
    admitted = False
    delivered = {
        **_snapshot(),
        "generation": 1,
        "submission_key": "g1q3-rca-s1-" + "c" * 64,
    }

    class FakeStore:
        def __init__(self, _path):
            pass

        def admit_manual_trigger(self, _request, **kwargs):
            nonlocal admitted
            assert "batch_terminal_rerun_authority" not in kwargs
            assert "silent_terminal_rerun_authority" not in kwargs
            admitted = True
            return SimpleNamespace(
                outcome="created",
                generation=1,
                submission_key=delivered["submission_key"],
                source_id="source-1",
            )

    def snapshot(_path, _issue_id, *, submission_key=""):
        if not admitted:
            return None
        if submission_key and submission_key != delivered["submission_key"]:
            return None
        return delivered

    monkeypatch.setattr(batch_rerun, "RcaControlStore", FakeStore)
    monkeypatch.setattr(batch_rerun, "_issue_snapshot", snapshot)
    monkeypatch.setattr(
        batch_rerun, "_runtime_identity", lambda: ("a" * 40, "b" * 40)
    )

    result = batch_rerun.run(_run_args(tmp_path, queue_path, owner_path))

    assert result["status"] == "completed"
    assert result["summary"] == {"accepted": 1, "total": 1}


def test_submit_all_admits_historical_generation_without_waiting_for_delivery(
    tmp_path, monkeypatch
):
    queue_path, owner_path = _write_run_inputs(tmp_path, _queue_value())
    prior = {
        **_snapshot(),
        "generation": 1,
        "submission_key": "g1q3-rca-s1-" + "a" * 64,
        "activation_epoch_id": None,
        "activation_ledger_id": None,
        "outbox_activation_epoch_id": None,
        "outbox_activation_ledger_id": None,
        "effects": [],
        "job_status": "",
        "delivery_id": None,
    }
    admitted = False

    class FakeStore:
        def __init__(self, _path):
            pass

        def admit_manual_trigger(self, _request, **kwargs):
            nonlocal admitted
            authority = kwargs.get("historical_epoch_rerun_authority")
            assert authority is not None
            assert kwargs["outbox_high_watermark"] == 1000
            assert authority["prior_submission_key"] == prior["submission_key"]
            admitted = True
            return SimpleNamespace(
                outcome="created",
                generation=2,
                submission_key="g1q3-rca-s1-" + "c" * 64,
                source_id="source-2",
            )

    def snapshot(_path, _issue_id, *, submission_key=""):
        assert not submission_key
        return prior

    monkeypatch.setattr(batch_rerun, "RcaControlStore", FakeStore)
    monkeypatch.setattr(batch_rerun, "_issue_snapshot", snapshot)
    monkeypatch.setattr(batch_rerun, "_runtime_identity", lambda: ("a" * 40, "b" * 40))
    args = _run_args(tmp_path, queue_path, owner_path)
    args.submit_all = True
    args.outbox_high_watermark = 1000

    result = batch_rerun.run(args)

    assert admitted is True
    assert result["status"] == "submitted_all"
    assert result["summary"] == {"accepted": 0, "submitted": 1, "total": 1}
    assert result["items"]["7048803418"]["status"] == "submitted"


def test_submit_all_refreshes_tracked_terminal_and_retries_without_waiting(
    tmp_path, monkeypatch
):
    queue_path, owner_path = _write_run_inputs(tmp_path, _queue_value())
    prior_submission_key = "g1q3-rca-s1-" + "a" * 64
    prior = {
        **_snapshot(job_status=""),
        "generation": 1,
        "submission_key": prior_submission_key,
        "delivery_id": None,
        "effects": [],
        "watch_state": "terminal_failed",
        "watch_delivery_id": None,
        "watch_error_code": "rca_work_deadline_exceeded",
    }
    state = {
        "items": {
            "7048803418": {
                "issue_id": "7048803418",
                "title": "ACC braking issue",
                "quality_classification": "missing",
                "priority": 1,
                "status": "submitted",
                "request_index": 1,
                "generation": 1,
                "submission_key": prior_submission_key,
            }
        }
    }
    admissions = []

    class FakeStore:
        def __init__(self, _path):
            pass

        def admit_manual_trigger(self, _request, **kwargs):
            assert kwargs.get("silent_terminal_rerun_authority") is not None
            admissions.append(kwargs)
            return SimpleNamespace(
                outcome="created",
                generation=2,
                submission_key="g1q3-rca-s1-" + "c" * 64,
                source_id="source-2",
            )

    monkeypatch.setattr(batch_rerun, "RcaControlStore", FakeStore)
    monkeypatch.setattr(batch_rerun, "_issue_snapshot", lambda *_args, **_kwargs: prior)
    monkeypatch.setattr(
        batch_rerun,
        "_load_or_create_state",
        lambda *_args, **_kwargs: state,
    )
    monkeypatch.setattr(
        batch_rerun, "_runtime_identity", lambda: ("a" * 40, "b" * 40)
    )
    args = _run_args(tmp_path, queue_path, owner_path)
    args.submit_all = True
    args.retry_failed = True
    args.outbox_high_watermark = 1000

    result = batch_rerun.run(args)

    assert len(admissions) == 1
    assert result["status"] == "submitted_all"
    assert result["summary"] == {"accepted": 0, "submitted": 1, "total": 1}
    item = result["items"]["7048803418"]
    assert item["status"] == "submitted"
    assert item["generation"] == 2
    assert item["request_index"] == 2
    assert "failure" not in item


def test_submit_all_defers_unavailable_authority_and_continues(tmp_path, monkeypatch):
    queue_path, owner_path = _write_run_inputs(tmp_path, _queue_value())
    prior_submission_key = "g1q3-rca-s1-" + "a" * 64
    prior = {
        **_snapshot(job_status=""),
        "generation": 1,
        "submission_key": prior_submission_key,
        "delivery_id": None,
        "effects": [],
        "outbox_status": "quarantined",
        "outbox_error_code": "host_issue_preread_failed",
    }
    state = {
        "items": {
            "7048803418": {
                "issue_id": "7048803418",
                "title": "ACC braking issue",
                "quality_classification": "missing",
                "priority": 1,
                "status": "submitted",
                "request_index": 1,
                "generation": 1,
                "submission_key": prior_submission_key,
            }
        }
    }

    class FakeStore:
        def __init__(self, _path):
            pass

        def admit_manual_trigger(self, *_args, **_kwargs):
            pytest.fail("unavailable authority reached admission")

    monkeypatch.setattr(batch_rerun, "RcaControlStore", FakeStore)
    monkeypatch.setattr(batch_rerun, "_issue_snapshot", lambda *_args, **_kwargs: prior)
    monkeypatch.setattr(
        batch_rerun,
        "_load_or_create_state",
        lambda *_args, **_kwargs: state,
    )
    monkeypatch.setattr(
        batch_rerun,
        "_refresh_authorities",
        lambda **_kwargs: (None, None, None),
    )
    monkeypatch.setattr(
        batch_rerun, "_runtime_identity", lambda: ("a" * 40, "b" * 40)
    )
    monkeypatch.setattr(
        batch_rerun.time,
        "sleep",
        lambda _seconds: pytest.fail("bulk deferral waited"),
    )
    args = _run_args(tmp_path, queue_path, owner_path)
    args.submit_all = True
    args.retry_failed = True
    args.outbox_high_watermark = 1000

    result = batch_rerun.run(args)

    assert result["status"] == "submitted_all"
    assert result["summary"] == {
        "accepted": 0,
        "submitted": 0,
        "deferred": 1,
        "total": 1,
    }
    item = result["items"]["7048803418"]
    assert item["status"] == "waiting_for_prior_terminal"
    assert item["generation"] == 1
    assert item["request_index"] == 1


def test_submit_all_defers_silent_authority_rejected_by_store(tmp_path, monkeypatch):
    queue_path, owner_path = _write_run_inputs(tmp_path, _queue_value())
    prior_submission_key = "g1q3-rca-s1-" + "a" * 64
    prior = {
        **_snapshot(job_status=""),
        "generation": 1,
        "submission_key": prior_submission_key,
        "delivery_id": None,
        "effects": [],
        "watch_state": "terminal_failed",
        "watch_delivery_id": None,
        "watch_error_code": "rca_work_deadline_exceeded",
    }
    state = {
        "items": {
            "7048803418": {
                "issue_id": "7048803418",
                "title": "ACC braking issue",
                "quality_classification": "missing",
                "priority": 1,
                "status": "submitted",
                "request_index": 1,
                "generation": 1,
                "submission_key": prior_submission_key,
            }
        }
    }

    class FakeStore:
        def __init__(self, _path):
            pass

        def admit_manual_trigger(self, *_args, **_kwargs):
            raise batch_rerun.ManualRcaAdmissionError(
                "silent_terminal_rerun_terminal_generation_required"
            )

    monkeypatch.setattr(batch_rerun, "RcaControlStore", FakeStore)
    monkeypatch.setattr(batch_rerun, "_issue_snapshot", lambda *_args, **_kwargs: prior)
    monkeypatch.setattr(
        batch_rerun,
        "_load_or_create_state",
        lambda *_args, **_kwargs: state,
    )
    monkeypatch.setattr(
        batch_rerun, "_runtime_identity", lambda: ("a" * 40, "b" * 40)
    )
    args = _run_args(tmp_path, queue_path, owner_path)
    args.submit_all = True
    args.retry_failed = True
    args.outbox_high_watermark = 1000

    result = batch_rerun.run(args)

    assert result["status"] == "submitted_all"
    assert result["summary"] == {
        "accepted": 0,
        "submitted": 0,
        "deferred": 1,
        "total": 1,
    }
    item = result["items"]["7048803418"]
    assert item["status"] == "waiting_for_prior_terminal"
    assert item["generation"] == 1
    assert item["request_index"] == 1


def test_run_refreshes_existing_success_instead_of_skipping_it(tmp_path, monkeypatch):
    queue_path, owner_path = _write_run_inputs(tmp_path, _queue_value())
    prior = {
        **_snapshot(),
        "generation": 1,
        "submission_key": "g1q3-rca-s1-" + "a" * 64,
        "watch_state": "delivery_created",
        "watch_delivery_id": "delivery-1",
    }
    refreshed = {
        **_snapshot(),
        "generation": 2,
        "submission_key": "g1q3-rca-s1-" + "c" * 64,
    }
    admitted = False

    class FakeStore:
        def __init__(self, _path):
            pass

        def admit_manual_trigger(self, _request, **kwargs):
            nonlocal admitted
            authority = kwargs.get("batch_terminal_rerun_authority")
            assert authority is not None
            assert authority["prior_submission_key"] == prior["submission_key"]
            admitted = True
            return SimpleNamespace(
                outcome="created",
                generation=2,
                submission_key=refreshed["submission_key"],
                source_id="source-2",
            )

    def snapshot(_path, _issue_id, *, submission_key=""):
        value = refreshed if admitted else prior
        if submission_key and submission_key != value["submission_key"]:
            return None
        return value

    monkeypatch.setattr(batch_rerun, "RcaControlStore", FakeStore)
    monkeypatch.setattr(batch_rerun, "_issue_snapshot", snapshot)
    monkeypatch.setattr(
        batch_rerun, "_runtime_identity", lambda: ("a" * 40, "b" * 40)
    )

    result = batch_rerun.run(_run_args(tmp_path, queue_path, owner_path))

    assert admitted is True
    assert result["items"]["7048803418"]["generation"] == 2
    assert result["status"] == "completed"


def test_run_retries_failed_state_after_noncausal_field_readback(
    tmp_path, monkeypatch
):
    queue_path, owner_path = _write_run_inputs(tmp_path, _queue_value())
    delivered = {
        **_snapshot(causal=False),
        "generation": 2,
        "submission_key": "g1q3-rca-s1-" + "c" * 64,
        "watch_state": "delivery_created",
        "watch_delivery_id": "delivery-6",
    }
    refreshed = {
        **_snapshot(),
        "generation": 3,
        "submission_key": "g1q3-rca-s1-" + "d" * 64,
    }
    admitted = False
    state = {
        "schema_version": SCHEMA_VERSION,
        "batch_id": "gray-20260724",
        "status": "blocked_on_item_failure",
        "items": {
            "7048803418": {
                "issue_id": "7048803418",
                "title": "ACC braking issue",
                "status": "failed",
                "generation": 2,
                "submission_key": delivered["submission_key"],
                "request_index": 1,
                "failure": {"job_status": "delivered"},
            }
        },
    }

    class FakeStore:
        def __init__(self, _path):
            pass

        def admit_manual_trigger(self, _request, **kwargs):
            nonlocal admitted
            authority = kwargs.get("batch_terminal_rerun_authority")
            assert authority is not None
            assert authority["prior_submission_key"] == delivered["submission_key"]
            admitted = True
            return SimpleNamespace(
                outcome="created",
                generation=3,
                submission_key=refreshed["submission_key"],
                source_id="source-3",
            )

    def snapshot(_path, _issue_id, *, submission_key=""):
        value = refreshed if admitted else delivered
        if submission_key and submission_key != value["submission_key"]:
            return None
        return value

    monkeypatch.setattr(batch_rerun, "RcaControlStore", FakeStore)
    monkeypatch.setattr(batch_rerun, "_issue_snapshot", snapshot)
    monkeypatch.setattr(batch_rerun, "_load_or_create_state", lambda *_a, **_k: state)
    monkeypatch.setattr(
        batch_rerun, "_runtime_identity", lambda: ("a" * 40, "b" * 40)
    )
    args = _run_args(tmp_path, queue_path, owner_path)
    args.retry_failed = True

    result = batch_rerun.run(args)

    item = result["items"]["7048803418"]
    assert admitted is True
    assert result["status"] == "completed"
    assert result["summary"] == {"accepted": 1, "total": 1}
    assert item["status"] == "accepted"
    assert "failure" not in item
    assert item["generation"] == 3
    assert item["approval"]["quality"]["status"] == "causal_candidate"


def test_issue_snapshot_tracks_new_outbox_before_execution_watch_exists(tmp_path):
    db_path = tmp_path / "control.sqlite3"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE business_triggers (
            business_key TEXT,
            generation INTEGER,
            submission_key TEXT,
            work_item_id TEXT,
            state TEXT,
            activation_epoch_id TEXT,
            activation_ledger_id INTEGER
        );
        CREATE TABLE rca_outbox (
            outbox_id INTEGER,
            business_key TEXT,
            generation INTEGER,
            status TEXT,
            last_error_code TEXT,
            last_error_detail TEXT,
            attempt INTEGER,
            next_attempt_at TEXT,
            claimed_at TEXT,
            completed_at TEXT,
            quarantined_at TEXT,
            result_json TEXT,
            lease_token TEXT,
            lease_owner TEXT,
            lease_expires_at TEXT,
            activation_epoch_id TEXT,
            activation_ledger_id INTEGER
        );
            CREATE TABLE rca_execution_watch (
                submission_outbox_id INTEGER,
                business_key TEXT,
                generation INTEGER,
                state TEXT,
            delivery_id TEXT,
            last_error_code TEXT,
            last_error_detail TEXT,
            last_status_json TEXT,
            terminal_at TEXT,
            task_id TEXT,
            lease_token TEXT,
            lease_owner TEXT,
            lease_expires_at TEXT
        );
        CREATE TABLE rca_delivery_subscriptions (
            business_key TEXT,
            generation INTEGER,
            subscription_key TEXT,
            effect_kind TEXT,
            required INTEGER,
            status TEXT,
            delivery_id TEXT,
            effect_key TEXT,
            reason TEXT
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
            write_phase TEXT,
            attempt INTEGER,
            lease_token TEXT,
            lease_owner TEXT,
            lease_expires_at TEXT,
            remote_receipt_json TEXT,
            last_error_code TEXT,
            completed_at TEXT,
            updated_at TEXT
        );
        CREATE TABLE rca_delivery_attempts (effect_key TEXT);
        CREATE TABLE rca_activation_epochs (
            epoch_id TEXT,
            state TEXT,
            is_current INTEGER
        );
        INSERT INTO business_triggers VALUES (
            'business-1', 6, 'submission-6', '7048803418',
            'pending', 'rca-old', 61
        );
        INSERT INTO rca_outbox VALUES (
            378, 'business-1', 6, 'pending', '', '', 0, NULL, NULL,
            NULL, NULL, NULL,
            NULL, NULL, NULL, 'rca-old', 61
        );
        INSERT INTO rca_activation_epochs VALUES (
            'rca-current', 'steady_active', 1
        );
        """
    )
    conn.commit()
    conn.close()
    before_sidecars = {
        path.name for path in tmp_path.iterdir() if path.name.endswith(("-wal", "-shm"))
    }

    snapshot = _issue_snapshot(db_path, "7048803418", submission_key="submission-6")

    assert snapshot is not None
    assert snapshot["generation"] == 6
    assert snapshot["submission_key"] == "submission-6"
    assert snapshot["outbox_status"] == "pending"
    after_sidecars = {
        path.name for path in tmp_path.iterdir() if path.name.endswith(("-wal", "-shm"))
    }
    assert after_sidecars == before_sidecars
