import copy
import json

import pytest

import gateway.pnc_rca_snapshot as snapshot_module
from gateway.pnc_rca_admission import (
    RcaAdmissionError,
    build_rca_admission,
    build_rca_trigger_context,
)
from gateway.pnc_rca_control_store import RcaControlStore, RecordConflictError
from gateway.pnc_rca_snapshot import (
    ADMISSION_SNAPSHOT_SCHEMA_VERSION,
    CANONICAL_RCA_REQUEST_SCHEMA_VERSION,
    UNISSUED_WRITE_FENCE,
    AdmissionSnapshot,
    build_admission_snapshot as _build_admission_snapshot,
    build_canonical_rca_request as _build_canonical_rca_request,
    build_snapshot_source_envelope as _build_snapshot_source_envelope,
    build_source_authority_receipt,
    canonical_json_sha256,
    canonical_ticket_title_sha256,
    compare_snapshot_shadow as _compare_snapshot_shadow,
    compose_snapshot_projection as _compose_snapshot_projection,
    legacy_semantic_projection as _legacy_semantic_projection,
    validate_admission_snapshot as _validate_admission_snapshot,
    validate_snapshot_source_envelope as _validate_snapshot_source_envelope,
)


BASE = {
    "project_key": "project-key",
    "project_simple_name": "g1q3",
    "work_item_type_key": "issue",
    "work_item_id": "7041712812",
    "rule_version": "issue-created-v1",
}
AUTHORITY_A = "1" * 64
AUTHORITY_B = "2" * 64
KAFKA_PAYLOAD_AUTHORITY = "a" * 64
MANUAL_PAYLOAD_AUTHORITY = "b" * 64
BASE_URL = "https://project.feishu.cn/g1q3/issue/detail/7041712812"
OBSERVED_AT = "2026-07-25T10:00:00+00:00"
TITLE_AUTHORITY = canonical_ticket_title_sha256("ACC braking issue")


def _policy(name: str, value=None):
    body = {} if value is None else value
    version = f"{name}-v1"
    return {
        "version": version,
        "sha256": canonical_json_sha256({"version": version, "value": body}),
        "value": body,
    }


def _contract_kwargs():
    return {
        "creation_policy": _policy("creation_policy", {"rule": "issue-created-v1"}),
        "business_profile": _policy("business_profile", {"profile_id": "g1q3"}),
        "execution_policy": _policy("execution_policy", {"resolver": "pdcl"}),
        "publication_policy": _policy("publication_policy", {"target": "issue"}),
        "correction_lineage_policy": _policy("correction_lineage_policy", {"version": 1}),
    }


def _policy_authorities(policies=None):
    observed = _contract_kwargs() if policies is None else policies
    return {
        name: observed[name]["sha256"]
        for name in (
            "creation_policy",
            "business_profile",
            "execution_policy",
            "publication_policy",
            "correction_lineage_policy",
        )
    }


def _execution_admission():
    return {
        "activation_epoch_id": "",
        "activation_ledger_id": None,
        "decision": "admit",
        "reason": "activation_legacy_unconfigured",
        "state": "legacy_unconfigured",
        "legacy_unconfigured": True,
    }


def _admission_and_context(*, trigger_kind="issue_created", generation=None):
    admission = build_rca_admission(
        **BASE,
        trigger_kind=trigger_kind,
        generation=generation,
    )
    context = build_rca_trigger_context(
        source_kind="kafka_workflow_event"
        if trigger_kind.startswith("kafka") or trigger_kind == "issue_created"
        else "feishu_group_manual",
        project_key=BASE["project_key"],
        project_simple_name=BASE["project_simple_name"],
        work_item_type_key=BASE["work_item_type_key"],
        work_item_id=BASE["work_item_id"],
        rule_version=BASE["rule_version"],
        issue_url="https://project.feishu.cn/g1q3/issue/detail/7041712812",
        title="ACC braking issue",
    )
    return admission, context


def _snapshot(*, trigger_kind="issue_created"):
    admission, context = _admission_and_context(trigger_kind=trigger_kind)
    request = build_canonical_rca_request(
        admission=admission, trigger_context=context, **_contract_kwargs()
    )
    return request, build_admission_snapshot(
        request=request,
        admission=admission,
        execution_admission=_execution_admission(),
    )


def _rerun_snapshot():
    admission, context = _admission_and_context(
        trigger_kind="manual_retrigger",
        generation=2,
    )
    request = build_canonical_rca_request(
        admission=admission,
        trigger_context=context,
        generation_reason="explicit_user_rerun",
        generation_authorization_evidence_sha256=AUTHORITY_A,
        expected_generation_authorization_evidence_sha256=AUTHORITY_A,
        **_contract_kwargs(),
    )
    return request, build_admission_snapshot(
        request=request,
        admission=admission,
        execution_admission=_execution_admission(),
        expected_generation_authorization_evidence_sha256=AUTHORITY_A,
    )


def _resign_snapshot(value):
    payload = {
        key: item
        for key, item in value.items()
        if key not in {"snapshot_id", "snapshot_sha256"}
    }
    digest = canonical_json_sha256(payload)
    value["snapshot_sha256"] = digest
    value["snapshot_id"] = f"pnc-rca-snapshot-v1-{digest}"
    return value


def _resign_envelope(value):
    payload = {
        key: item
        for key, item in value.items()
        if key not in {"source_envelope_id", "source_envelope_sha256"}
    }
    digest = canonical_json_sha256(payload)
    value["source_envelope_sha256"] = digest
    value["source_envelope_id"] = f"pnc-rca-source-envelope-v1-{digest}"
    return value


def _kafka_metadata(**changes):
    return {
        "source_kind": "kafka_workflow_event",
        "event_uid": "topic:0:1",
        "topic": "topic",
        "partition": 0,
        "offset": 1,
        "payload_sha256": KAFKA_PAYLOAD_AUTHORITY,
        "observed_at": OBSERVED_AT,
        **changes,
    }


def _manual_metadata(*, platform="feishu", **changes):
    base = {
        "source_kind": "feishu_group_manual",
        "platform": platform,
        "chat_id": "oc_chat" if platform == "feishu" else "",
        "thread_id": "topic:omt_root" if platform == "feishu" else "",
        "message_id": "om_message",
        "requester_id": (
            "ou_requester" if platform == "feishu" else "automation:w3-test"
        ),
        "mode": "run_or_join" if platform == "feishu" else "rerun",
        "payload_sha256": MANUAL_PAYLOAD_AUTHORITY,
        "observed_at": OBSERVED_AT,
    }
    return {**base, **changes}


def _ingress(*, binding_action="create", decision="admit", evidence="1" * 64):
    return {
        "requested_mode": "pending" if decision == "admit" else "shadow",
        "binding_action": binding_action,
        "decision": decision,
        "authorization_evidence_sha256": evidence,
    }


def _source_authority(
    *,
    source_id,
    source_kind,
    source_metadata,
    anchor,
    ingress_decision,
    expected_issue_target=BASE_URL,
):
    return build_source_authority_receipt(
        source_id=source_id,
        source_kind=source_kind,
        source_metadata=source_metadata,
        anchor=anchor,
        ingress_decision=ingress_decision,
        expected_issue_target=expected_issue_target,
    )


def _projection_source_authority(projection):
    source = projection["source_metadata"]
    return _source_authority(
        source_id=source["source_id"],
        source_kind=source["source_kind"],
        source_metadata=source["transport"],
        anchor=projection["anchor"],
        ingress_decision=source["ingress_decision"],
        expected_issue_target=projection["snapshot_core"]["canonical_request"][
            "ticket"
        ]["issue_url"],
    )


def build_canonical_rca_request(
    *,
    expected_ticket_title_sha256=TITLE_AUTHORITY,
    expected_policy_sha256s=None,
    **kwargs,
):
    if expected_policy_sha256s is None:
        expected_policy_sha256s = _policy_authorities()
    return _build_canonical_rca_request(
        expected_ticket_title_sha256=expected_ticket_title_sha256,
        expected_policy_sha256s=expected_policy_sha256s,
        **kwargs,
    )


def build_admission_snapshot(
    *,
    expected_ticket_title_sha256=TITLE_AUTHORITY,
    expected_policy_sha256s=None,
    **kwargs,
):
    if expected_policy_sha256s is None:
        expected_policy_sha256s = _policy_authorities()
    return _build_admission_snapshot(
        expected_ticket_title_sha256=expected_ticket_title_sha256,
        expected_policy_sha256s=expected_policy_sha256s,
        **kwargs,
    )


def validate_admission_snapshot(
    value,
    *,
    expected_ticket_title_sha256=TITLE_AUTHORITY,
    expected_policy_sha256s=None,
    expected_snapshot_sha256=None,
    **kwargs,
):
    if expected_policy_sha256s is None:
        expected_policy_sha256s = _policy_authorities()
    if expected_snapshot_sha256 is None:
        if hasattr(value, "snapshot_sha256"):
            expected_snapshot_sha256 = value.snapshot_sha256
        elif isinstance(value, dict):
            expected_snapshot_sha256 = value.get("snapshot_sha256", "0" * 64)
        else:
            expected_snapshot_sha256 = "0" * 64
    return _validate_admission_snapshot(
        value,
        expected_ticket_title_sha256=expected_ticket_title_sha256,
        expected_policy_sha256s=expected_policy_sha256s,
        expected_snapshot_sha256=expected_snapshot_sha256,
        **kwargs,
    )


def build_snapshot_source_envelope(
    *,
    expected_authorization_evidence_sha256=AUTHORITY_A,
    expected_generation_authorization_evidence_sha256=None,
    expected_ticket_title_sha256=TITLE_AUTHORITY,
    expected_source_payload_sha256=None,
    expected_policy_sha256s=None,
    expected_snapshot_sha256=None,
    expected_source_authority=None,
    **kwargs,
):
    if expected_policy_sha256s is None:
        expected_policy_sha256s = _policy_authorities()
    if expected_snapshot_sha256 is None:
        snapshot = kwargs["snapshot"]
        expected_snapshot_sha256 = (
            snapshot.snapshot_sha256
            if hasattr(snapshot, "snapshot_sha256")
            else snapshot["snapshot_sha256"]
        )
    if expected_source_authority is None:
        snapshot = kwargs["snapshot"]
        ticket = (
            snapshot.canonical_request.ticket
            if hasattr(snapshot, "canonical_request")
            else snapshot["canonical_request"]["ticket"]
        )
        expected_source_authority = _source_authority(
            source_id=kwargs["source_id"],
            source_kind=kwargs["source_kind"],
            source_metadata=kwargs["source_metadata"],
            anchor=kwargs["anchor"],
            ingress_decision=kwargs["ingress_decision"],
            expected_issue_target=ticket["issue_url"],
        )
    if expected_source_payload_sha256 is None:
        expected_source_payload_sha256 = (
            MANUAL_PAYLOAD_AUTHORITY
            if kwargs["source_kind"] == "feishu_group_manual"
            else KAFKA_PAYLOAD_AUTHORITY
        )
    return _build_snapshot_source_envelope(
        expected_authorization_evidence_sha256=(
            expected_authorization_evidence_sha256
        ),
        expected_generation_authorization_evidence_sha256=(
            expected_generation_authorization_evidence_sha256
        ),
        expected_ticket_title_sha256=expected_ticket_title_sha256,
        expected_source_payload_sha256=expected_source_payload_sha256,
        expected_policy_sha256s=expected_policy_sha256s,
        expected_snapshot_sha256=expected_snapshot_sha256,
        expected_source_authority=expected_source_authority,
        **kwargs,
    )


def validate_snapshot_source_envelope(
    value,
    *,
    expected_snapshot,
    expected_authorization_evidence_sha256=AUTHORITY_A,
    expected_generation_authorization_evidence_sha256=None,
    allow_unbound_policies_for_shadow=False,
    expected_ticket_title_sha256=TITLE_AUTHORITY,
    expected_source_payload_sha256=None,
    expected_policy_sha256s=None,
    expected_snapshot_sha256=None,
    expected_source_authority=None,
):
    if expected_policy_sha256s is None:
        expected_policy_sha256s = _policy_authorities()
    if expected_snapshot_sha256 is None:
        expected_snapshot_sha256 = (
            expected_snapshot.snapshot_sha256
            if hasattr(expected_snapshot, "snapshot_sha256")
            else expected_snapshot["snapshot_sha256"]
        )
    if expected_source_authority is None:
        envelope = value.to_dict() if hasattr(value, "to_dict") else value
        snapshot = (
            expected_snapshot.to_dict()
            if hasattr(expected_snapshot, "to_dict")
            else expected_snapshot
        )
        expected_source_authority = _source_authority(
            source_id=envelope["source_id"],
            source_kind=envelope["source_kind"],
            source_metadata=envelope["source_metadata"],
            anchor=envelope["anchor"],
            ingress_decision=envelope["ingress_decision"],
            expected_issue_target=snapshot["canonical_request"]["ticket"]["issue_url"],
        )
    if expected_source_payload_sha256 is None:
        source_kind = (
            value.source_kind
            if hasattr(value, "source_kind")
            else value["source_kind"]
        )
        expected_source_payload_sha256 = (
            MANUAL_PAYLOAD_AUTHORITY
            if source_kind == "feishu_group_manual"
            else KAFKA_PAYLOAD_AUTHORITY
        )
    return _validate_snapshot_source_envelope(
        value,
        expected_snapshot=expected_snapshot,
        expected_authorization_evidence_sha256=(
            expected_authorization_evidence_sha256
        ),
        expected_generation_authorization_evidence_sha256=(
            expected_generation_authorization_evidence_sha256
        ),
        allow_unbound_policies_for_shadow=allow_unbound_policies_for_shadow,
        expected_ticket_title_sha256=expected_ticket_title_sha256,
        expected_source_payload_sha256=expected_source_payload_sha256,
        expected_policy_sha256s=expected_policy_sha256s,
        expected_snapshot_sha256=expected_snapshot_sha256,
        expected_source_authority=expected_source_authority,
    )


def compose_snapshot_projection(
    snapshot,
    envelope,
    *,
    expected_authorization_evidence_sha256=AUTHORITY_A,
    expected_generation_authorization_evidence_sha256=None,
    allow_unbound_policies_for_shadow=False,
    expected_ticket_title_sha256=TITLE_AUTHORITY,
    expected_source_payload_sha256=None,
    expected_policy_sha256s=None,
    expected_snapshot_sha256=None,
    expected_source_authority=None,
):
    if expected_policy_sha256s is None:
        expected_policy_sha256s = _policy_authorities()
    if expected_snapshot_sha256 is None:
        expected_snapshot_sha256 = (
            snapshot.snapshot_sha256
            if hasattr(snapshot, "snapshot_sha256")
            else snapshot["snapshot_sha256"]
        )
    if expected_source_authority is None:
        source = envelope.to_dict() if hasattr(envelope, "to_dict") else envelope
        core = snapshot.to_dict() if hasattr(snapshot, "to_dict") else snapshot
        expected_source_authority = _source_authority(
            source_id=source["source_id"],
            source_kind=source["source_kind"],
            source_metadata=source["source_metadata"],
            anchor=source["anchor"],
            ingress_decision=source["ingress_decision"],
            expected_issue_target=core["canonical_request"]["ticket"]["issue_url"],
        )
    if expected_source_payload_sha256 is None:
        source_kind = (
            envelope.source_kind
            if hasattr(envelope, "source_kind")
            else envelope["source_kind"]
        )
        expected_source_payload_sha256 = (
            MANUAL_PAYLOAD_AUTHORITY
            if source_kind == "feishu_group_manual"
            else KAFKA_PAYLOAD_AUTHORITY
        )
    return _compose_snapshot_projection(
        snapshot,
        envelope,
        expected_authorization_evidence_sha256=(
            expected_authorization_evidence_sha256
        ),
        expected_generation_authorization_evidence_sha256=(
            expected_generation_authorization_evidence_sha256
        ),
        allow_unbound_policies_for_shadow=allow_unbound_policies_for_shadow,
        expected_ticket_title_sha256=expected_ticket_title_sha256,
        expected_source_payload_sha256=expected_source_payload_sha256,
        expected_policy_sha256s=expected_policy_sha256s,
        expected_snapshot_sha256=expected_snapshot_sha256,
        expected_source_authority=expected_source_authority,
    )


def compare_snapshot_shadow(
    legacy_projection,
    candidate_projection,
    *,
    expected_legacy_authorization_evidence_sha256=AUTHORITY_A,
    expected_candidate_authorization_evidence_sha256=AUTHORITY_A,
    expected_legacy_generation_authorization_evidence_sha256=None,
    expected_candidate_generation_authorization_evidence_sha256=None,
    expected_legacy_ticket_title_sha256=TITLE_AUTHORITY,
    expected_candidate_ticket_title_sha256=TITLE_AUTHORITY,
    expected_legacy_source_payload_sha256=KAFKA_PAYLOAD_AUTHORITY,
    expected_candidate_source_payload_sha256=KAFKA_PAYLOAD_AUTHORITY,
    expected_legacy_policy_sha256s=None,
    expected_candidate_policy_sha256s=None,
    expected_legacy_snapshot_sha256=None,
    expected_candidate_snapshot_sha256=None,
    expected_legacy_source_authority=None,
    expected_candidate_source_authority=None,
):
    if expected_legacy_policy_sha256s is None:
        expected_legacy_policy_sha256s = _policy_authorities()
    if expected_candidate_policy_sha256s is None:
        expected_candidate_policy_sha256s = _policy_authorities()
    if expected_legacy_snapshot_sha256 is None:
        expected_legacy_snapshot_sha256 = legacy_projection["snapshot_core"][
            "snapshot_sha256"
        ]
    if expected_candidate_snapshot_sha256 is None:
        expected_candidate_snapshot_sha256 = candidate_projection["snapshot_core"][
            "snapshot_sha256"
        ]
    if expected_legacy_source_authority is None:
        expected_legacy_source_authority = _projection_source_authority(
            legacy_projection
        )
    if expected_candidate_source_authority is None:
        expected_candidate_source_authority = _projection_source_authority(
            candidate_projection
        )
    return _compare_snapshot_shadow(
        legacy_projection,
        candidate_projection,
        expected_legacy_authorization_evidence_sha256=(
            expected_legacy_authorization_evidence_sha256
        ),
        expected_candidate_authorization_evidence_sha256=(
            expected_candidate_authorization_evidence_sha256
        ),
        expected_legacy_generation_authorization_evidence_sha256=(
            expected_legacy_generation_authorization_evidence_sha256
        ),
        expected_candidate_generation_authorization_evidence_sha256=(
            expected_candidate_generation_authorization_evidence_sha256
        ),
        expected_legacy_ticket_title_sha256=expected_legacy_ticket_title_sha256,
        expected_candidate_ticket_title_sha256=(
            expected_candidate_ticket_title_sha256
        ),
        expected_legacy_source_payload_sha256=(
            expected_legacy_source_payload_sha256
        ),
        expected_candidate_source_payload_sha256=(
            expected_candidate_source_payload_sha256
        ),
        expected_legacy_policy_sha256s=expected_legacy_policy_sha256s,
        expected_candidate_policy_sha256s=expected_candidate_policy_sha256s,
        expected_legacy_snapshot_sha256=expected_legacy_snapshot_sha256,
        expected_candidate_snapshot_sha256=expected_candidate_snapshot_sha256,
        expected_legacy_source_authority=expected_legacy_source_authority,
        expected_candidate_source_authority=expected_candidate_source_authority,
    )


def legacy_semantic_projection(
    *,
    expected_authorization_evidence_sha256=AUTHORITY_A,
    expected_ticket_title_sha256=TITLE_AUTHORITY,
    expected_source_payload_sha256=None,
    expected_policy_sha256s=None,
    expected_source_authority=None,
    **kwargs,
):
    if expected_policy_sha256s is None:
        expected_policy_sha256s = _policy_authorities()
    if expected_source_payload_sha256 is None:
        expected_source_payload_sha256 = (
            MANUAL_PAYLOAD_AUTHORITY
            if kwargs["source_metadata"]["source_kind"]
            == "feishu_group_manual"
            else KAFKA_PAYLOAD_AUTHORITY
        )
    if expected_source_authority is None:
        expected_source_authority = _source_authority(
            source_id=kwargs["source_id"],
            source_kind=kwargs["source_metadata"]["source_kind"],
            source_metadata=kwargs["source_metadata"],
            anchor=kwargs["anchor"],
            ingress_decision=kwargs["ingress_decision"],
        )
    return _legacy_semantic_projection(
        expected_authorization_evidence_sha256=(
            expected_authorization_evidence_sha256
        ),
        expected_ticket_title_sha256=expected_ticket_title_sha256,
        expected_source_payload_sha256=expected_source_payload_sha256,
        expected_policy_sha256s=expected_policy_sha256s,
        expected_source_authority=expected_source_authority,
        **kwargs,
    )


def test_request_is_source_neutral_and_snapshot_has_unissued_fence():
    request, snapshot = _snapshot()
    assert set(request.to_dict()) == {
        "schema_version",
        "ticket",
        "execution_intent",
        "creation_policy",
        "business_profile",
        "execution_policy",
        "publication_policy",
        "correction_lineage_policy",
    }
    assert request.execution_intent == {
        "kind": "analyze_ticket",
        "generation_reason": "initial",
        "generation_authorization_evidence_sha256": None,
    }
    assert set(snapshot.to_dict()) == {
        "schema_version",
        "snapshot_id",
        "snapshot_sha256",
        "request_sha256",
        "canonical_request",
        "resolved_admission",
        "execution_admission",
        "write_fence",
    }
    assert snapshot.write_fence == UNISSUED_WRITE_FENCE
    assert dict(snapshot.write_fence) == {
        "schema_version": "pnc_rca_write_fence_slot_v1",
        "state": "unissued",
    }
    assert snapshot.snapshot_id.endswith(snapshot.snapshot_sha256)
    assert validate_admission_snapshot(snapshot.to_dict()) == snapshot


def test_request_rejects_missing_authoritative_ticket_title():
    admission = build_rca_admission(**BASE, trigger_kind="manual_issue_request")
    context = build_rca_trigger_context(
        source_kind="feishu_group_manual",
        project_key=BASE["project_key"],
        project_simple_name=BASE["project_simple_name"],
        work_item_type_key=BASE["work_item_type_key"],
        work_item_id=BASE["work_item_id"],
        rule_version=BASE["rule_version"],
        issue_url=BASE_URL,
    )
    with pytest.raises(RcaAdmissionError, match="ticket_title_required"):
        build_canonical_rca_request(
            admission=admission,
            trigger_context=context,
            **_contract_kwargs(),
        )

    different_title_context = build_rca_trigger_context(
        source_kind="feishu_group_manual",
        project_key=BASE["project_key"],
        project_simple_name=BASE["project_simple_name"],
        work_item_type_key=BASE["work_item_type_key"],
        work_item_id=BASE["work_item_id"],
        rule_version=BASE["rule_version"],
        issue_url=BASE_URL,
        title="Different nonempty title",
    )
    with pytest.raises(RcaAdmissionError, match="ticket_title_authority_mismatch"):
        build_canonical_rca_request(
            admission=admission,
            trigger_context=different_title_context,
            **_contract_kwargs(),
        )


def test_policy_omission_and_tampered_digest_fail_closed():
    admission, context = _admission_and_context()
    kwargs = _contract_kwargs()
    with pytest.raises(RcaAdmissionError, match="unbound"):
        build_canonical_rca_request(
            admission=admission,
            trigger_context=context,
            **{key: value for key, value in kwargs.items() if key != "execution_policy"},
        )
    forged = copy.deepcopy(kwargs["execution_policy"])
    forged["value"]["resolver"] = "other"
    with pytest.raises(RcaAdmissionError, match="digest"):
        build_canonical_rca_request(
            admission=admission,
            trigger_context=context,
            **{**kwargs, "execution_policy": forged},
        )
    self_signed = _policy("execution_policy", {"resolver": "wrong-but-valid"})
    with pytest.raises(RcaAdmissionError, match="execution_policy_authority_mismatch"):
        build_canonical_rca_request(
            admission=admission,
            trigger_context=context,
            **{**kwargs, "execution_policy": self_signed},
        )


def test_snapshot_rejects_non_unissued_fence_and_hash_mutation():
    _request, snapshot = _snapshot()
    forged = snapshot.to_dict()
    forged["write_fence"] = {"schema_version": "pnc_rca_write_fence_v1", "state": "issued"}
    with pytest.raises(RcaAdmissionError, match="write_fence"):
        validate_admission_snapshot(forged)
    forged = snapshot.to_dict()
    forged["snapshot_sha256"] = "0" * 64
    with pytest.raises(RcaAdmissionError, match="hash|id"):
        validate_admission_snapshot(forged)


def test_strict_json_rejects_duplicate_noncanonical_and_nonfinite_values():
    from gateway.pnc_rca_snapshot import strict_canonical_json_loads

    with pytest.raises(RcaAdmissionError, match="duplicate"):
        strict_canonical_json_loads('{"a":1,"a":2}')
    with pytest.raises(RcaAdmissionError, match="canonical"):
        strict_canonical_json_loads('{ "a": 1 }')
    with pytest.raises(RcaAdmissionError, match="canonical|non_finite"):
        # NaN is rejected by the parser before it can become a durable value.
        strict_canonical_json_loads('{"a":NaN}')
    with pytest.raises(RcaAdmissionError, match="json_invalid"):
        strict_canonical_json_loads('\ufeff{"a":1}')
    with pytest.raises(RcaAdmissionError, match="non_finite"):
        strict_canonical_json_loads('{"a":Infinity}')
    with pytest.raises(RcaAdmissionError, match="json_invalid"):
        strict_canonical_json_loads('{"a":1} trailing')


def test_source_envelopes_are_one_to_many_and_source_hashes_can_differ():
    request, snapshot = _snapshot()
    base_metadata = {
        "source_kind": "kafka_workflow_event",
        "event_uid": "topic:0:1",
        "topic": "topic",
        "partition": 0,
        "offset": 1,
        "payload_sha256": KAFKA_PAYLOAD_AUTHORITY,
        "observed_at": OBSERVED_AT,
    }
    decision = {
        "requested_mode": "pending",
        "binding_action": "create",
        "decision": "admit",
        "authorization_evidence_sha256": "1" * 64,
    }
    first = build_snapshot_source_envelope(
        snapshot=snapshot,
        source_id="kafka-source-1",
        source_kind="kafka_workflow_event",
        source_metadata=base_metadata,
        anchor={"issue_target": BASE_URL, "thread_target": None},
        ingress_decision=decision,
    )
    second_metadata = {**base_metadata, "event_uid": "topic:0:2", "offset": 2}
    second = build_snapshot_source_envelope(
        snapshot=snapshot,
        source_id="kafka-source-2",
        source_kind="kafka_workflow_event",
        source_metadata=second_metadata,
        anchor={"issue_target": BASE_URL, "thread_target": None},
        ingress_decision=decision,
    )
    assert first.snapshot_id == second.snapshot_id == snapshot.snapshot_id
    assert first.source_envelope_id != second.source_envelope_id
    assert first.source_envelope_sha256 != second.source_envelope_sha256
    assert (
        validate_snapshot_source_envelope(
            second.to_dict(),
            expected_snapshot=snapshot,
        )
        == second
    )


def test_shadow_diff_allows_only_source_metadata_and_anchor_and_is_type_aware():
    request, snapshot = _snapshot()
    metadata = {
        "source_kind": "kafka_workflow_event",
        "event_uid": "topic:0:1",
        "topic": "topic",
        "partition": 0,
        "offset": 1,
        "payload_sha256": KAFKA_PAYLOAD_AUTHORITY,
        "observed_at": OBSERVED_AT,
    }
    env = build_snapshot_source_envelope(
        snapshot=snapshot,
        source_id="kafka-source",
        source_kind="kafka_workflow_event",
        source_metadata=metadata,
        anchor={"issue_target": BASE_URL, "thread_target": None},
        ingress_decision={
            "requested_mode": "pending",
            "binding_action": "create",
            "decision": "admit",
            "authorization_evidence_sha256": "1" * 64,
        },
    )
    candidate = compose_snapshot_projection(snapshot, env)
    allowed_env = build_snapshot_source_envelope(
        snapshot=snapshot,
        source_id="kafka-source-2",
        source_kind="kafka_workflow_event",
        source_metadata={
            **metadata,
            "event_uid": "topic:0:2",
            "offset": 2,
        },
        anchor={"issue_target": BASE_URL, "thread_target": None},
        ingress_decision={
            "requested_mode": "pending",
            "binding_action": "join",
            "decision": "admit",
            "authorization_evidence_sha256": "2" * 64,
        },
        expected_authorization_evidence_sha256=AUTHORITY_B,
    )
    allowed = compose_snapshot_projection(
        snapshot,
        allowed_env,
        expected_authorization_evidence_sha256=AUTHORITY_B,
    )
    assert (
        compare_snapshot_shadow(
            candidate,
            allowed,
            expected_candidate_authorization_evidence_sha256=AUTHORITY_B,
        )["outcome"]
        == "match"
    )
    forbidden = copy.deepcopy(candidate)
    forbidden["snapshot_core"]["resolved_admission"]["generation"] = 2
    result = compare_snapshot_shadow(candidate, forbidden)
    assert result["outcome"] == "mismatch"
    assert "/snapshot_core/resolved_admission/generation" in result["forbidden_diff_paths"]
    typed = copy.deepcopy(candidate)
    typed["snapshot_core"]["resolved_admission"]["generation"] = True
    assert compare_snapshot_shadow(candidate, typed)["outcome"] == "mismatch"


def test_legacy_projector_is_independent_and_detects_core_mutation():
    admission, context = _admission_and_context()
    kwargs = _contract_kwargs()
    execution = {
        "activation_epoch_id": "",
        "activation_ledger_id": None,
        "decision": "admit",
        "reason": "activation_legacy_unconfigured",
        "state": "legacy_unconfigured",
        "legacy_unconfigured": True,
    }
    source = {
        "source_kind": "kafka_workflow_event",
        "event_uid": "topic:0:1",
        "topic": "topic",
        "partition": 0,
        "offset": 1,
        "payload_sha256": KAFKA_PAYLOAD_AUTHORITY,
        "observed_at": OBSERVED_AT,
    }
    legacy = legacy_semantic_projection(
        admission=admission,
        trigger_context=context,
        source_id="legacy-projector-source",
        **kwargs,
        execution_admission=execution,
        source_metadata=source,
        anchor={"issue_target": BASE_URL, "thread_target": None},
        ingress_decision={
            "requested_mode": "pending",
            "binding_action": "create",
            "decision": "admit",
            "authorization_evidence_sha256": "1" * 64,
        },
    )
    request = build_canonical_rca_request(admission=admission, trigger_context=context, **kwargs)
    snapshot = build_admission_snapshot(
        request=request,
        admission=admission,
        execution_admission=_execution_admission(),
    )
    env = build_snapshot_source_envelope(
        snapshot=snapshot,
        source_id="legacy-projector-source",
        source_kind="kafka_workflow_event",
        source_metadata=source,
        anchor={"issue_target": BASE_URL, "thread_target": None},
        ingress_decision={
            "requested_mode": "pending",
            "binding_action": "create",
            "decision": "admit",
            "authorization_evidence_sha256": "1" * 64,
        },
    )
    assert compare_snapshot_shadow(legacy, compose_snapshot_projection(snapshot, env))["outcome"] == "match"
    forged = copy.deepcopy(legacy)
    forged["snapshot_core"]["resolved_admission"]["generation"] = 99
    assert compare_snapshot_shadow(forged, compose_snapshot_projection(snapshot, env))["outcome"] == "mismatch"


def test_kafka_and_manual_share_core_but_keep_distinct_source_envelopes():
    kafka_admission, kafka_context = _admission_and_context(
        trigger_kind="issue_created"
    )
    manual_admission, manual_context = _admission_and_context(
        trigger_kind="manual_issue_request"
    )
    kafka_request = build_canonical_rca_request(
        admission=kafka_admission,
        trigger_context=kafka_context,
        **_contract_kwargs(),
    )
    manual_request = build_canonical_rca_request(
        admission=manual_admission,
        trigger_context=manual_context,
        **_contract_kwargs(),
    )
    kafka_snapshot = build_admission_snapshot(
        request=kafka_request,
        admission=kafka_admission,
        execution_admission=_execution_admission(),
    )
    manual_snapshot = build_admission_snapshot(
        request=manual_request,
        admission=manual_admission,
        execution_admission=_execution_admission(),
    )
    assert kafka_request.request_sha256 == manual_request.request_sha256
    assert kafka_snapshot.snapshot_sha256 == manual_snapshot.snapshot_sha256
    assert kafka_snapshot.snapshot_id == manual_snapshot.snapshot_id

    ingress = {
        "requested_mode": "pending",
        "binding_action": "create",
        "decision": "admit",
        "authorization_evidence_sha256": "1" * 64,
    }
    kafka_envelope = build_snapshot_source_envelope(
        snapshot=kafka_snapshot,
        source_id="kafka-source",
        source_kind="kafka_workflow_event",
        source_metadata={
            "source_kind": "kafka_workflow_event",
            "event_uid": "topic:0:1",
            "topic": "topic",
            "partition": 0,
            "offset": 1,
            "payload_sha256": KAFKA_PAYLOAD_AUTHORITY,
            "observed_at": OBSERVED_AT,
        },
        anchor={"issue_target": BASE_URL, "thread_target": None},
        ingress_decision=ingress,
    )
    manual_envelope = build_snapshot_source_envelope(
        snapshot=manual_snapshot,
        source_id="manual-source",
        source_kind="feishu_group_manual",
        source_metadata={
            "source_kind": "feishu_group_manual",
            "platform": "feishu",
            "chat_id": "oc_chat",
            "thread_id": "topic:omt_root",
            "message_id": "om_message",
            "requester_id": "ou_requester",
            "mode": "run_or_join",
            "payload_sha256": MANUAL_PAYLOAD_AUTHORITY,
            "observed_at": OBSERVED_AT,
        },
        anchor={"issue_target": BASE_URL, "thread_target": "topic:omt_root"},
        ingress_decision={**ingress, "binding_action": "join"},
    )
    assert kafka_envelope.source_envelope_sha256 != manual_envelope.source_envelope_sha256
    assert len(
        {
            kafka_request.request_sha256,
            kafka_snapshot.snapshot_sha256,
            kafka_envelope.source_envelope_sha256,
        }
    ) == 3


def test_source_envelope_rejects_missing_manual_identity_and_mode_mismatch():
    _request, snapshot = _snapshot()
    metadata = {
        "source_kind": "feishu_group_manual",
        "platform": "feishu",
        "chat_id": "",
        "thread_id": "",
        "message_id": "om_message",
        "requester_id": "ou_requester",
        "mode": "run_or_join",
        "payload_sha256": MANUAL_PAYLOAD_AUTHORITY,
        "observed_at": OBSERVED_AT,
    }
    with pytest.raises(RcaAdmissionError, match="chat_id"):
        build_snapshot_source_envelope(
            snapshot=snapshot,
            source_id="manual-source",
            source_kind="feishu_group_manual",
            source_metadata=metadata,
            anchor={"issue_target": BASE_URL, "thread_target": None},
            ingress_decision={
                "requested_mode": "pending",
                "binding_action": "create",
                "decision": "admit",
                "authorization_evidence_sha256": "1" * 64,
            },
        )
    with pytest.raises(RcaAdmissionError, match="mode_mismatch"):
        build_snapshot_source_envelope(
            snapshot=snapshot,
            source_id="kafka-source",
            source_kind="kafka_workflow_event",
            source_metadata={
                "source_kind": "kafka_workflow_event",
                "event_uid": "topic:0:1",
                "topic": "topic",
                "partition": 0,
                "offset": 1,
                "payload_sha256": KAFKA_PAYLOAD_AUTHORITY,
                "observed_at": OBSERVED_AT,
            },
            anchor={"issue_target": BASE_URL, "thread_target": None},
            ingress_decision={
                "requested_mode": "shadow",
                "binding_action": "create",
                "decision": "admit",
                "authorization_evidence_sha256": "1" * 64,
            },
        )


def test_legacy_projection_does_not_call_candidate_builder(monkeypatch):
    admission, context = _admission_and_context()
    monkeypatch.setattr(
        snapshot_module,
        "build_canonical_rca_request",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("candidate builder called")),
    )
    projection = legacy_semantic_projection(
        admission=admission,
        trigger_context=context,
        source_id="legacy-projector-source",
        **_contract_kwargs(),
        execution_admission={
            "activation_epoch_id": "",
            "activation_ledger_id": None,
            "decision": "admit",
            "reason": "activation_legacy_unconfigured",
            "state": "legacy_unconfigured",
            "legacy_unconfigured": True,
        },
        source_metadata={
            "source_kind": "kafka_workflow_event",
            "event_uid": "topic:0:1",
            "topic": "topic",
            "partition": 0,
            "offset": 1,
            "payload_sha256": KAFKA_PAYLOAD_AUTHORITY,
            "observed_at": OBSERVED_AT,
        },
        anchor={"issue_target": BASE_URL, "thread_target": None},
        ingress_decision={
            "requested_mode": "pending",
            "binding_action": "create",
            "decision": "admit",
            "authorization_evidence_sha256": "1" * 64,
        },
    )
    assert projection["snapshot_core"]["resolved_admission"]["generation"] == 1


def test_canonical_json_rejects_non_string_keys():
    with pytest.raises(RcaAdmissionError, match="non_string_key"):
        canonical_json_sha256({1: "value"})


def _valid_kafka_projection():
    _request, snapshot = _snapshot()
    envelope = build_snapshot_source_envelope(
        snapshot=snapshot,
        source_id="kafka-source",
        source_kind="kafka_workflow_event",
        source_metadata={
            "source_kind": "kafka_workflow_event",
            "event_uid": "topic:0:1",
            "topic": "topic",
            "partition": 0,
            "offset": 1,
            "payload_sha256": KAFKA_PAYLOAD_AUTHORITY,
            "observed_at": OBSERVED_AT,
        },
        anchor={"issue_target": BASE_URL, "thread_target": None},
        ingress_decision={
            "requested_mode": "pending",
            "binding_action": "create",
            "decision": "admit",
            "authorization_evidence_sha256": "1" * 64,
        },
    )
    return snapshot, envelope, compose_snapshot_projection(snapshot, envelope)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value["source_metadata"]["transport"].__setitem__(
            "evil", True
        ),
        lambda value: value["anchor"].__setitem__("evil", True),
        lambda value: value["source_metadata"].__setitem__(
            "source_kind", "feishu_group_manual"
        ),
    ],
)
def test_shadow_rejects_malformed_allowed_namespaces(mutate):
    _snapshot_value, _envelope, projection = _valid_kafka_projection()
    pristine_authority = _projection_source_authority(projection)
    malformed = copy.deepcopy(projection)
    mutate(malformed)
    result = compare_snapshot_shadow(
        projection,
        malformed,
        expected_legacy_source_authority=pristine_authority,
        expected_candidate_source_authority=pristine_authority,
    )
    assert result["outcome"] == "mismatch"
    assert result["validation_errors"]["candidate"]
    same_malformed = compare_snapshot_shadow(
        malformed,
        copy.deepcopy(malformed),
        expected_legacy_source_authority=pristine_authority,
        expected_candidate_source_authority=pristine_authority,
    )
    assert same_malformed["outcome"] == "mismatch"
    assert same_malformed["validation_errors"]


def test_explicit_unbound_policy_is_allowed_but_missing_authority_is_not():
    admission, context = _admission_and_context()
    kwargs = _contract_kwargs()
    unbound = _policy("execution_policy", {"state": "unbound"})
    diagnostic_policies = {**kwargs, "execution_policy": unbound}
    diagnostic_authorities = _policy_authorities(diagnostic_policies)
    request = build_canonical_rca_request(
        admission=admission,
        trigger_context=context,
        expected_policy_sha256s=diagnostic_authorities,
        **diagnostic_policies,
    )
    assert request.execution_policy["value"] == {"state": "unbound"}
    with pytest.raises(RcaAdmissionError, match="exact_fields"):
        build_canonical_rca_request(
            admission=admission,
            trigger_context=context,
            expected_policy_sha256s=diagnostic_authorities,
            **{**kwargs, "execution_policy": {"state": "unbound"}},
        )
    with pytest.raises(RcaAdmissionError, match="execution_admission_unbound"):
        build_admission_snapshot(
            request=request,
            admission=admission,
            allow_unbound_policies_for_shadow=True,
            expected_policy_sha256s=diagnostic_authorities,
        )
    snapshot = build_admission_snapshot(
        request=request,
        admission=admission,
        execution_admission=_execution_admission(),
        allow_unbound_policies_for_shadow=True,
        expected_policy_sha256s=diagnostic_authorities,
    )
    with pytest.raises(RcaAdmissionError, match="execution_policy_not_switch_ready"):
        validate_admission_snapshot(
            snapshot,
            expected_policy_sha256s=diagnostic_authorities,
        )
    envelope = build_snapshot_source_envelope(
        snapshot=snapshot,
        source_id="unbound-policy-source",
        source_kind="kafka_workflow_event",
        source_metadata=_kafka_metadata(),
        anchor={"issue_target": BASE_URL, "thread_target": None},
        ingress_decision=_ingress(),
        allow_unbound_policies_for_shadow=True,
        expected_policy_sha256s=diagnostic_authorities,
    )
    projection = compose_snapshot_projection(
        snapshot,
        envelope,
        allow_unbound_policies_for_shadow=True,
        expected_policy_sha256s=diagnostic_authorities,
    )
    comparison = compare_snapshot_shadow(
        projection,
        copy.deepcopy(projection),
        expected_legacy_policy_sha256s=diagnostic_authorities,
        expected_candidate_policy_sha256s=diagnostic_authorities,
    )
    assert comparison["outcome"] == "mismatch"
    assert "not_switch_ready" in comparison["validation_errors"]["legacy"]


@pytest.mark.parametrize(
    "policy_name",
    [
        "business_profile",
        "execution_policy",
        "publication_policy",
        "correction_lineage_policy",
    ],
)
def test_empty_policy_value_cannot_self_authorize_switch(policy_name):
    admission, context = _admission_and_context()
    kwargs = {
        **_contract_kwargs(),
        policy_name: _policy(policy_name, {}),
    }
    diagnostic_authorities = _policy_authorities(kwargs)
    request = build_canonical_rca_request(
        admission=admission,
        trigger_context=context,
        expected_policy_sha256s=diagnostic_authorities,
        **kwargs,
    )
    snapshot = build_admission_snapshot(
        request=request,
        admission=admission,
        execution_admission=_execution_admission(),
        allow_unbound_policies_for_shadow=True,
        expected_policy_sha256s=diagnostic_authorities,
    )
    with pytest.raises(RcaAdmissionError, match=f"{policy_name}_not_switch_ready"):
        validate_admission_snapshot(
            snapshot,
            expected_policy_sha256s=diagnostic_authorities,
        )
    envelope = build_snapshot_source_envelope(
        snapshot=snapshot,
        source_id=f"empty-{policy_name}-source",
        source_kind="kafka_workflow_event",
        source_metadata=_kafka_metadata(),
        anchor={"issue_target": BASE_URL, "thread_target": None},
        ingress_decision=_ingress(),
        allow_unbound_policies_for_shadow=True,
        expected_policy_sha256s=diagnostic_authorities,
    )
    projection = compose_snapshot_projection(
        snapshot,
        envelope,
        allow_unbound_policies_for_shadow=True,
        expected_policy_sha256s=diagnostic_authorities,
    )
    comparison = compare_snapshot_shadow(
        projection,
        copy.deepcopy(projection),
        expected_legacy_policy_sha256s=diagnostic_authorities,
        expected_candidate_policy_sha256s=diagnostic_authorities,
    )
    assert comparison["outcome"] == "mismatch"
    assert "not_switch_ready" in comparison["validation_errors"]["legacy"]


def test_creation_policy_must_bind_the_legacy_admission_rule():
    admission, context = _admission_and_context()
    kwargs = _contract_kwargs()
    mismatched = _policy("creation_policy", {"rule": "another-rule"})
    with pytest.raises(RcaAdmissionError, match="creation_policy_admission_mismatch"):
        build_canonical_rca_request(
            admission=admission,
            trigger_context=context,
            **{**kwargs, "creation_policy": mismatched},
        )


@pytest.mark.parametrize(
    ("value", "error"),
    [
        ({}, "rule_unbound"),
        ({"state": "unbound"}, "rule_unbound"),
        (
            {"rule": "issue-created-v1", "rule_version": "another-rule"},
            "rule_conflict",
        ),
    ],
)
def test_creation_policy_rejects_unbound_and_conflicting_authority(value, error):
    admission, context = _admission_and_context()
    kwargs = _contract_kwargs()
    with pytest.raises(RcaAdmissionError, match=error):
        build_canonical_rca_request(
            admission=admission,
            trigger_context=context,
            **{**kwargs, "creation_policy": _policy("creation_policy", value)},
        )


def test_rerun_requires_explicit_generation_reason():
    admission = build_rca_admission(
        **BASE,
        trigger_kind="manual_retrigger",
        generation=2,
    )
    context = build_rca_trigger_context(
        source_kind="feishu_group_manual",
        project_key=BASE["project_key"],
        project_simple_name=BASE["project_simple_name"],
        work_item_type_key=BASE["work_item_type_key"],
        work_item_id=BASE["work_item_id"],
        rule_version=BASE["rule_version"],
        issue_url=BASE_URL,
        title="ACC braking issue",
    )
    with pytest.raises(RcaAdmissionError, match="rerun_generation_reason_unbound"):
        build_canonical_rca_request(
            admission=admission,
            trigger_context=context,
            **_contract_kwargs(),
        )
    with pytest.raises(RcaAdmissionError, match="generation_authorization_evidence"):
        build_canonical_rca_request(
            admission=admission,
            trigger_context=context,
            generation_reason="explicit_user_rerun",
            **_contract_kwargs(),
        )
    with pytest.raises(RcaAdmissionError, match="generation_authorization_evidence_mismatch"):
        build_canonical_rca_request(
            admission=admission,
            trigger_context=context,
            generation_reason="explicit_user_rerun",
            generation_authorization_evidence_sha256=AUTHORITY_A,
            expected_generation_authorization_evidence_sha256=AUTHORITY_B,
            **_contract_kwargs(),
        )
    request = build_canonical_rca_request(
        admission=admission,
        trigger_context=context,
        generation_reason="explicit_user_rerun",
        generation_authorization_evidence_sha256=AUTHORITY_A,
        expected_generation_authorization_evidence_sha256=AUTHORITY_A,
        **_contract_kwargs(),
    )
    assert request.execution_intent["generation_reason"] == "explicit_user_rerun"
    assert (
        request.execution_intent["generation_authorization_evidence_sha256"]
        == AUTHORITY_A
    )


def test_resigned_wrong_anchor_fails_expected_snapshot_validation():
    snapshot, envelope, _projection = _valid_kafka_projection()
    pristine_authority = _source_authority(
        source_id=envelope.source_id,
        source_kind=envelope.source_kind,
        source_metadata=envelope.source_metadata,
        anchor=envelope.anchor,
        ingress_decision=envelope.ingress_decision,
    )
    forged = envelope.to_dict()
    forged["anchor"]["issue_target"] = (
        "https://project.feishu.cn/g1q3/issue/detail/999999"
    )
    identity = {
        key: value
        for key, value in forged.items()
        if key not in {"source_envelope_id", "source_envelope_sha256"}
    }
    digest = canonical_json_sha256(identity)
    forged["source_envelope_sha256"] = digest
    forged["source_envelope_id"] = f"pnc-rca-source-envelope-v1-{digest}"
    with pytest.raises(RcaAdmissionError, match="source_authority_mismatch"):
        validate_snapshot_source_envelope(
            forged,
            expected_snapshot=snapshot,
            expected_source_authority=pristine_authority,
        )


@pytest.mark.parametrize(
    "reason",
    [
        "business_profile_observation_changed",
        "input_wait_terminal_new_generation_created",
        "operator_recovery",
    ],
)
def test_infrastructure_reasons_cannot_create_a_new_generation(reason):
    admission = build_rca_admission(
        **BASE,
        trigger_kind="kafka_retrigger",
        generation=2,
        topic="topic",
        partition=0,
        offset=1,
    )
    context = build_rca_trigger_context(
        source_kind="kafka_workflow_event",
        project_key=BASE["project_key"],
        project_simple_name=BASE["project_simple_name"],
        work_item_type_key=BASE["work_item_type_key"],
        work_item_id=BASE["work_item_id"],
        rule_version=BASE["rule_version"],
        issue_url=BASE_URL,
        title="ACC braking issue",
    )
    with pytest.raises(RcaAdmissionError, match="rerun_generation_reason_invalid"):
        build_canonical_rca_request(
            admission=admission,
            trigger_context=context,
            generation_reason=reason,
            **_contract_kwargs(),
        )


def test_rerun_creation_requires_human_feishu_authority():
    _request, snapshot = _rerun_snapshot()
    with pytest.raises(
        RcaAdmissionError,
        match="expected_generation_authorization_evidence_sha256_invalid",
    ):
        validate_admission_snapshot(snapshot)
    with pytest.raises(RcaAdmissionError, match="generation_authorization_evidence_mismatch"):
        validate_admission_snapshot(
            snapshot,
            expected_generation_authorization_evidence_sha256=AUTHORITY_B,
        )
    assert (
        validate_admission_snapshot(
            snapshot,
            expected_generation_authorization_evidence_sha256=AUTHORITY_A,
        )
        == snapshot
    )
    with pytest.raises(RcaAdmissionError, match="explicit_rerun_authority_invalid"):
        build_snapshot_source_envelope(
            snapshot=snapshot,
            source_id="kafka-rerun-source",
            source_kind="kafka_workflow_event",
            source_metadata=_kafka_metadata(),
            anchor={"issue_target": BASE_URL, "thread_target": None},
            ingress_decision=_ingress(),
            expected_generation_authorization_evidence_sha256=AUTHORITY_A,
        )
    with pytest.raises(RcaAdmissionError, match="explicit_rerun_authority_invalid"):
        build_snapshot_source_envelope(
            snapshot=snapshot,
            source_id="operator-rerun-source",
            source_kind="feishu_group_manual",
            source_metadata=_manual_metadata(platform="operator", mode="rerun"),
            anchor={"issue_target": BASE_URL, "thread_target": None},
            ingress_decision=_ingress(),
            expected_generation_authorization_evidence_sha256=AUTHORITY_A,
        )
    envelope = build_snapshot_source_envelope(
        snapshot=snapshot,
        source_id="human-rerun-source",
        source_kind="feishu_group_manual",
        source_metadata=_manual_metadata(mode="rerun"),
        anchor={"issue_target": BASE_URL, "thread_target": "topic:omt_root"},
        ingress_decision=_ingress(),
        expected_generation_authorization_evidence_sha256=AUTHORITY_A,
    )
    assert envelope.source_metadata["requester_id"] == "ou_requester"
    with pytest.raises(RcaAdmissionError, match="creator_evidence_mismatch"):
        build_snapshot_source_envelope(
            snapshot=snapshot,
            source_id="wrong-creator-evidence-source",
            source_kind="feishu_group_manual",
            source_metadata=_manual_metadata(mode="rerun"),
            anchor={"issue_target": BASE_URL, "thread_target": "topic:omt_root"},
            ingress_decision=_ingress(evidence=AUTHORITY_B),
            expected_authorization_evidence_sha256=AUTHORITY_B,
            expected_generation_authorization_evidence_sha256=AUTHORITY_A,
        )
    joined = build_snapshot_source_envelope(
        snapshot=snapshot,
        source_id="authorized-join-source",
        source_kind="kafka_workflow_event",
        source_metadata=_kafka_metadata(),
        anchor={"issue_target": BASE_URL, "thread_target": None},
        ingress_decision=_ingress(
            binding_action="join",
            evidence=AUTHORITY_B,
        ),
        expected_authorization_evidence_sha256=AUTHORITY_B,
        expected_generation_authorization_evidence_sha256=AUTHORITY_A,
    )
    assert (
        snapshot.canonical_request.execution_intent[
            "generation_authorization_evidence_sha256"
        ]
        == AUTHORITY_A
    )
    assert joined.ingress_decision["authorization_evidence_sha256"] == AUTHORITY_B
    created_projection = compose_snapshot_projection(
        snapshot,
        envelope,
        expected_generation_authorization_evidence_sha256=AUTHORITY_A,
    )
    joined_projection = compose_snapshot_projection(
        snapshot,
        joined,
        expected_authorization_evidence_sha256=AUTHORITY_B,
        expected_generation_authorization_evidence_sha256=AUTHORITY_A,
    )
    assert (
        compare_snapshot_shadow(
            created_projection,
            joined_projection,
            expected_candidate_authorization_evidence_sha256=AUTHORITY_B,
                expected_legacy_generation_authorization_evidence_sha256=AUTHORITY_A,
                expected_candidate_generation_authorization_evidence_sha256=AUTHORITY_A,
                expected_legacy_source_payload_sha256=MANUAL_PAYLOAD_AUTHORITY,
            )["outcome"]
        == "match"
    )
    assert (
        compare_snapshot_shadow(
            created_projection,
            joined_projection,
            expected_candidate_authorization_evidence_sha256=AUTHORITY_B,
            expected_legacy_source_payload_sha256=MANUAL_PAYLOAD_AUTHORITY,
        )["outcome"]
        == "mismatch"
    )


def test_manual_source_contract_matches_feishu_and_operator_storage_shapes():
    _request, snapshot = _snapshot(trigger_kind="manual_issue_request")
    operator = build_snapshot_source_envelope(
        snapshot=snapshot,
        source_id="operator-source",
        source_kind="feishu_group_manual",
        source_metadata=_manual_metadata(platform="operator", mode="debug"),
        anchor={"issue_target": BASE_URL, "thread_target": None},
        ingress_decision=_ingress(binding_action="join"),
    )
    assert operator.source_metadata["chat_id"] == ""
    assert operator.source_metadata["thread_id"] == ""

    with pytest.raises(RcaAdmissionError, match="platform_invalid"):
        build_snapshot_source_envelope(
            snapshot=snapshot,
            source_id="unknown-platform-source",
            source_kind="feishu_group_manual",
            source_metadata=_manual_metadata(platform="telegram"),
            anchor={"issue_target": BASE_URL, "thread_target": None},
            ingress_decision=_ingress(binding_action="join"),
        )
    with pytest.raises(RcaAdmissionError, match="anchor_thread_mismatch"):
        build_snapshot_source_envelope(
            snapshot=snapshot,
            source_id="wrong-thread-source",
            source_kind="feishu_group_manual",
            source_metadata=_manual_metadata(),
            anchor={"issue_target": BASE_URL, "thread_target": "topic:other_root"},
            ingress_decision=_ingress(binding_action="join"),
        )
    with pytest.raises(RcaAdmissionError, match="operator_mode_invalid"):
        build_snapshot_source_envelope(
            snapshot=snapshot,
            source_id="operator-mode-source",
            source_kind="feishu_group_manual",
            source_metadata=_manual_metadata(
                platform="operator",
                mode="run_or_join",
            ),
            anchor={"issue_target": BASE_URL, "thread_target": None},
            ingress_decision=_ingress(binding_action="join"),
        )
    with pytest.raises(RcaAdmissionError, match="feishu_requester_identity_invalid"):
        build_snapshot_source_envelope(
            snapshot=snapshot,
            source_id="wrong-requester-source",
            source_kind="feishu_group_manual",
            source_metadata=_manual_metadata(requester_id="automation:w3-test"),
            anchor={"issue_target": BASE_URL, "thread_target": "topic:omt_root"},
            ingress_decision=_ingress(binding_action="join"),
        )


def test_resigned_effective_decision_drift_fails_joint_validation():
    snapshot, envelope, _projection = _valid_kafka_projection()
    pristine_authority = _source_authority(
        source_id=envelope.source_id,
        source_kind=envelope.source_kind,
        source_metadata=envelope.source_metadata,
        anchor=envelope.anchor,
        ingress_decision=envelope.ingress_decision,
    )
    forged = envelope.to_dict()
    forged["ingress_decision"]["requested_mode"] = "shadow"
    forged["ingress_decision"]["decision"] = "shadow"
    _resign_envelope(forged)
    with pytest.raises(RcaAdmissionError, match="source_authority_mismatch"):
        validate_snapshot_source_envelope(
            forged,
            expected_snapshot=snapshot,
            expected_source_authority=pristine_authority,
        )


def test_resigned_snapshot_rechecks_policy_keys_and_activation_state():
    _request, snapshot = _snapshot()

    policy_forgery = snapshot.to_dict()
    policy_forgery["canonical_request"]["creation_policy"] = _policy(
        "creation_policy",
        {"rule": "another-rule"},
    )
    policy_forgery["request_sha256"] = canonical_json_sha256(
        policy_forgery["canonical_request"]
    )
    _resign_snapshot(policy_forgery)
    with pytest.raises(RcaAdmissionError, match="resolved_creation_policy_mismatch"):
        validate_admission_snapshot(policy_forgery)

    key_forgery = snapshot.to_dict()
    key_forgery["resolved_admission"]["business_key"] = "forged-business-key"
    _resign_snapshot(key_forgery)
    with pytest.raises(RcaAdmissionError, match="resolved_admission_identity_invalid"):
        validate_admission_snapshot(key_forgery)

    activation_forgery = snapshot.to_dict()
    activation_forgery["execution_admission"]["state"] = "invented-active"
    _resign_snapshot(activation_forgery)
    with pytest.raises(RcaAdmissionError, match="execution_state_invalid"):
        validate_admission_snapshot(activation_forgery)


@pytest.mark.parametrize(
    "execution_admission",
    [
        {
            "activation_epoch_id": "epoch-1",
            "activation_ledger_id": 1,
            "decision": "admit",
            "reason": "activation_admission_idempotent",
            "state": "safe_off",
            "legacy_unconfigured": False,
        },
        {
            "activation_epoch_id": "epoch-1",
            "activation_ledger_id": 1,
            "decision": "shadow",
            "reason": "activation_epoch_held_steady_active",
            "state": "steady_active",
            "legacy_unconfigured": False,
        },
        {
            "activation_epoch_id": "epoch-1",
            "activation_ledger_id": 1,
            "decision": "admit",
            "reason": "activation_admission_idempotent",
            "state": "aborted",
            "legacy_unconfigured": False,
        },
    ],
)
def test_execution_admission_rejects_impossible_state_decisions(
    execution_admission,
):
    request, _snapshot_value = _snapshot()
    admission, _context = _admission_and_context()
    with pytest.raises(RcaAdmissionError, match="state_decision_mismatch"):
        build_admission_snapshot(
            request=request,
            admission=admission,
            execution_admission=execution_admission,
        )


def test_execution_admission_accepts_unconfigured_kafka_shadow_shape():
    request, _snapshot_value = _snapshot()
    admission, _context = _admission_and_context()
    snapshot = build_admission_snapshot(
        request=request,
        admission=admission,
        execution_admission={
            "activation_epoch_id": "",
            "activation_ledger_id": None,
            "decision": "shadow",
            "reason": "activation_epoch_held_unconfigured",
            "state": "unconfigured",
            "legacy_unconfigured": False,
        },
    )
    assert snapshot.execution_admission["decision"] == "shadow"

    aborted = build_admission_snapshot(
        request=request,
        admission=admission,
        execution_admission={
            "activation_epoch_id": "epoch-1",
            "activation_ledger_id": 2,
            "decision": "shadow",
            "reason": "activation_epoch_held_ingress_aborted",
            "state": "aborted",
            "legacy_unconfigured": False,
        },
    )
    assert aborted.execution_admission["state"] == "aborted"

    with pytest.raises(RcaAdmissionError, match="shadow_reason_invalid"):
        build_admission_snapshot(
            request=request,
            admission=admission,
            execution_admission={
                "activation_epoch_id": "epoch-1",
                "activation_ledger_id": 3,
                "decision": "shadow",
                "reason": "activation_bogus",
                "state": "safe_off",
                "legacy_unconfigured": False,
            },
        )
    with pytest.raises(RcaAdmissionError, match="shadow_reason_invalid"):
        build_admission_snapshot(
            request=request,
            admission=admission,
            execution_admission={
                "activation_epoch_id": "epoch-1",
                "activation_ledger_id": 4,
                "decision": "shadow",
                "reason": "activation_bounded_slot_ambiguous",
                "state": "bounded_active",
                "legacy_unconfigured": False,
            },
        )


def test_public_validators_and_envelope_constructor_fail_closed():
    with pytest.raises(RcaAdmissionError, match="request_type_invalid"):
        snapshot_module.validate_canonical_rca_request(None)
    with pytest.raises(RcaAdmissionError, match="snapshot_type_invalid"):
        validate_admission_snapshot(None)

    snapshot, envelope, _projection = _valid_kafka_projection()
    with pytest.raises(RcaAdmissionError, match="snapshot_expected_hash_mismatch"):
        validate_admission_snapshot(
            snapshot,
            expected_snapshot_sha256="f" * 64,
        )
    envelope_kwargs = {
        "snapshot": snapshot,
        "source_id": "missing-external-authority-source",
        "source_kind": "kafka_workflow_event",
        "source_metadata": _kafka_metadata(),
        "anchor": {"issue_target": BASE_URL, "thread_target": None},
        "ingress_decision": _ingress(),
    }
    with pytest.raises(TypeError, match="expected_authorization_evidence_sha256"):
        _build_snapshot_source_envelope(**envelope_kwargs)
    with pytest.raises(TypeError, match="expected_source_payload_sha256"):
        _build_snapshot_source_envelope(
            **envelope_kwargs,
            expected_authorization_evidence_sha256=AUTHORITY_A,
            expected_ticket_title_sha256=TITLE_AUTHORITY,
        )
    with pytest.raises(RcaAdmissionError, match="authorization_evidence_mismatch"):
        build_snapshot_source_envelope(
            **envelope_kwargs,
            expected_authorization_evidence_sha256=AUTHORITY_B,
        )
    with pytest.raises(RcaAdmissionError, match="source_payload_mismatch"):
        build_snapshot_source_envelope(
            **envelope_kwargs,
            expected_source_payload_sha256=MANUAL_PAYLOAD_AUTHORITY,
        )
    with pytest.raises(RcaAdmissionError, match="event_uid_mismatch"):
        build_snapshot_source_envelope(
            **{
                **envelope_kwargs,
                "source_metadata": _kafka_metadata(event_uid="topic:0:999"),
            },
        )
    with pytest.raises(RcaAdmissionError, match="source_payload_unbound"):
        build_snapshot_source_envelope(
            **{
                **envelope_kwargs,
                "source_metadata": _kafka_metadata(payload_sha256="0" * 64),
            },
        )
    with pytest.raises(RcaAdmissionError, match="kafka_thread_invalid"):
        build_snapshot_source_envelope(
            **{
                **envelope_kwargs,
                "anchor": {
                    "issue_target": BASE_URL,
                    "thread_target": "topic:forbidden",
                },
            },
        )
    wrong_source_authority = _source_authority(
        source_id="different-source-id",
        source_kind=envelope_kwargs["source_kind"],
        source_metadata=envelope_kwargs["source_metadata"],
        anchor=envelope_kwargs["anchor"],
        ingress_decision=envelope_kwargs["ingress_decision"],
    )
    with pytest.raises(RcaAdmissionError, match="source_authority_mismatch"):
        build_snapshot_source_envelope(
            **envelope_kwargs,
            expected_source_authority=wrong_source_authority,
        )
    with pytest.raises(RcaAdmissionError, match="snapshot_expected_hash_mismatch"):
        validate_snapshot_source_envelope(
            envelope,
            expected_snapshot=snapshot,
            expected_snapshot_sha256="f" * 64,
        )
    with pytest.raises(RcaAdmissionError, match="authorization_evidence_mismatch"):
        validate_snapshot_source_envelope(
            envelope,
            expected_snapshot=snapshot,
            expected_authorization_evidence_sha256=AUTHORITY_B,
        )
    with pytest.raises(RcaAdmissionError, match="external_authority_required"):
        snapshot_module.AdmissionSnapshotSourceEnvelope(**envelope.to_dict())
    with pytest.raises(RcaAdmissionError, match="authorization_evidence_unbound"):
        build_snapshot_source_envelope(
            snapshot=snapshot,
            source_id="unbound-evidence-source",
            source_kind="kafka_workflow_event",
            source_metadata=_kafka_metadata(),
            anchor={"issue_target": BASE_URL, "thread_target": None},
            ingress_decision=_ingress(evidence="0" * 64),
        )
    with pytest.raises(RcaAdmissionError, match="timezone_missing"):
        build_snapshot_source_envelope(
            snapshot=snapshot,
            source_id="naive-timestamp-source",
            source_kind="kafka_workflow_event",
            source_metadata=_kafka_metadata(observed_at="2026-07-25T10:00:00"),
            anchor={"issue_target": BASE_URL, "thread_target": None},
            ingress_decision=_ingress(),
        )


def _seed_control_source(
    store,
    *,
    snapshot,
    source_id,
    source_kind,
    source_metadata,
    binding_action,
):
    conn = store._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        generation = int(snapshot.resolved_admission["generation"])
        if source_kind == "kafka_workflow_event":
            event_uid = source_metadata["event_uid"]
            mode = "issue_created" if generation == 1 else "kafka_retrigger"
            source_dedupe_key = (
                event_uid
                if generation == 1
                else f"{event_uid}:generation:{generation}"
            )
            kafka_event_uid = event_uid if generation == 1 else None
            conn.execute(
                """
                INSERT INTO kafka_inbox(
                    event_uid, topic, partition_id, offset_id,
                    raw_value, raw_size_bytes, raw_sha256,
                    headers_json, policy_json, creation_rule_version,
                    submission_mode, received_at
                ) VALUES(?, ?, ?, ?, X'', 0, ?, '[]', '{}', ?, 'shadow', ?)
                """,
                (
                    event_uid,
                    source_metadata["topic"],
                    source_metadata["partition"],
                    source_metadata["offset"],
                    source_metadata["payload_sha256"],
                    snapshot.resolved_admission["creation_rule_version"],
                    source_metadata["observed_at"],
                ),
            )
            manual_values = ("", "", "", "", "")
        else:
            source_dedupe_key = f"w3-test:{source_id}"
            kafka_event_uid = None
            mode = source_metadata["mode"]
            manual_values = tuple(
                source_metadata[column]
                for column in (
                    "platform",
                    "chat_id",
                    "thread_id",
                    "message_id",
                    "requester_id",
                )
            )
        conn.execute(
            """
            INSERT INTO rca_trigger_sources(
                source_id, source_kind, source_dedupe_key, payload_sha256,
                platform, chat_id, thread_id, message_id, requester_id,
                kafka_event_uid, mode, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source_id,
                source_kind,
                source_dedupe_key,
                source_metadata["payload_sha256"],
                *manual_values,
                kafka_event_uid,
                mode,
                source_metadata["observed_at"],
            ),
        )
        ticket = snapshot.canonical_request.ticket
        conn.execute(
            """
            INSERT OR IGNORE INTO business_triggers(
                business_key, generation, submission_key, creation_rule_version,
                work_item_id, project_key, work_item_type_key,
                normalized_json, state, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, '{}', 'accepted', ?)
            """,
            (
                snapshot.resolved_admission["business_key"],
                generation,
                snapshot.resolved_admission["submission_key"],
                snapshot.resolved_admission["creation_rule_version"],
                ticket["work_item_id"],
                ticket["project_key"],
                ticket["work_item_type_key"],
                source_metadata["observed_at"],
            ),
        )
        conn.execute(
            """
            INSERT INTO rca_trigger_bindings(
                source_id, business_key, generation, role, bound_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                source_id,
                snapshot.resolved_admission["business_key"],
                generation,
                "origin" if binding_action == "create" else "observer",
                source_metadata["observed_at"],
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _persist_snapshot_source(
    store,
    *,
    snapshot,
    envelope,
    expected_generation_authorization_evidence_sha256=None,
    expected_ticket_title_sha256=TITLE_AUTHORITY,
):
    return store.persist_admission_snapshot_source(
        snapshot=snapshot,
        source_envelope=envelope,
        expected_source_authority=_source_authority(
            source_id=envelope.source_id,
            source_kind=envelope.source_kind,
            source_metadata=envelope.source_metadata,
            anchor=envelope.anchor,
            ingress_decision=envelope.ingress_decision,
        ),
        expected_snapshot_sha256=snapshot.snapshot_sha256,
        expected_generation_authorization_evidence_sha256=(
            expected_generation_authorization_evidence_sha256
        ),
        expected_ticket_title_sha256=expected_ticket_title_sha256,
        expected_source_payload_sha256=envelope.source_metadata["payload_sha256"],
        expected_authorization_evidence_sha256=envelope.ingress_decision[
            "authorization_evidence_sha256"
        ],
        expected_policy_sha256s=_policy_authorities(),
    )


def test_control_store_persists_exact_creator_before_generation_two_join(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    _request, snapshot = _rerun_snapshot()
    creator_metadata = _manual_metadata(mode="rerun")
    creator = build_snapshot_source_envelope(
        snapshot=snapshot,
        source_id="human-rerun-source",
        source_kind="feishu_group_manual",
        source_metadata=creator_metadata,
        anchor={"issue_target": BASE_URL, "thread_target": "topic:omt_root"},
        ingress_decision=_ingress(evidence=AUTHORITY_A),
        expected_generation_authorization_evidence_sha256=AUTHORITY_A,
    )
    _seed_control_source(
        store,
        snapshot=snapshot,
        source_id=creator.source_id,
        source_kind=creator.source_kind,
        source_metadata=creator_metadata,
        binding_action="create",
    )

    created = _persist_snapshot_source(
        store,
        snapshot=snapshot,
        envelope=creator,
        expected_generation_authorization_evidence_sha256=AUTHORITY_A,
    )
    assert created["snapshot_created"] is True
    assert created["source_envelope_created"] is True

    join_metadata = _kafka_metadata(event_uid="topic:0:2", offset=2)
    joined = build_snapshot_source_envelope(
        snapshot=snapshot,
        source_id="authorized-kafka-join",
        source_kind="kafka_workflow_event",
        source_metadata=join_metadata,
        anchor={"issue_target": BASE_URL, "thread_target": None},
        ingress_decision=_ingress(
            binding_action="join",
            evidence=AUTHORITY_B,
        ),
        expected_authorization_evidence_sha256=AUTHORITY_B,
        expected_generation_authorization_evidence_sha256=AUTHORITY_A,
    )
    _seed_control_source(
        store,
        snapshot=snapshot,
        source_id=joined.source_id,
        source_kind=joined.source_kind,
        source_metadata=join_metadata,
        binding_action="join",
    )
    result = _persist_snapshot_source(
        store,
        snapshot=snapshot,
        envelope=joined,
        expected_generation_authorization_evidence_sha256=AUTHORITY_A,
    )

    assert result["snapshot_created"] is False
    assert result["source_envelope_created"] is True
    assert len(store.list_rows("rca_canonical_requests")) == 1
    assert len(store.list_rows("rca_admission_snapshots")) == 1
    assert len(store.list_rows("rca_source_authority_receipts")) == 2
    assert len(store.list_rows("rca_snapshot_source_envelopes")) == 2
    assert (
        store.list_rows("rca_admission_snapshots")[0][
            "creator_source_envelope_sha256"
        ]
        == creator.source_envelope_sha256
    )


def test_control_store_rejects_join_before_creator_without_partial_authority(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    _request, snapshot = _rerun_snapshot()
    metadata = _kafka_metadata(event_uid="topic:0:2", offset=2)
    joined = build_snapshot_source_envelope(
        snapshot=snapshot,
        source_id="join-before-create",
        source_kind="kafka_workflow_event",
        source_metadata=metadata,
        anchor={"issue_target": BASE_URL, "thread_target": None},
        ingress_decision=_ingress(
            binding_action="join",
            evidence=AUTHORITY_B,
        ),
        expected_authorization_evidence_sha256=AUTHORITY_B,
        expected_generation_authorization_evidence_sha256=AUTHORITY_A,
    )
    _seed_control_source(
        store,
        snapshot=snapshot,
        source_id=joined.source_id,
        source_kind=joined.source_kind,
        source_metadata=metadata,
        binding_action="join",
    )

    with pytest.raises(RecordConflictError, match="w3_snapshot_creator_missing"):
        _persist_snapshot_source(
            store,
            snapshot=snapshot,
            envelope=joined,
            expected_generation_authorization_evidence_sha256=AUTHORITY_A,
        )
    assert store.list_rows("rca_canonical_requests") == []
    assert store.list_rows("rca_admission_snapshots") == []
    assert store.list_rows("rca_source_authority_receipts") == []
    assert store.list_rows("rca_snapshot_source_envelopes") == []


@pytest.mark.parametrize("drift", ["binding", "transport"])
def test_control_store_rejects_unbound_or_transport_drifted_source(tmp_path, drift):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    _request, snapshot = _snapshot()
    metadata = _kafka_metadata()
    envelope = build_snapshot_source_envelope(
        snapshot=snapshot,
        source_id="drifted-kafka-source",
        source_kind="kafka_workflow_event",
        source_metadata=metadata,
        anchor={"issue_target": BASE_URL, "thread_target": None},
        ingress_decision=_ingress(),
    )
    _seed_control_source(
        store,
        snapshot=snapshot,
        source_id=envelope.source_id,
        source_kind=envelope.source_kind,
        source_metadata=metadata,
        binding_action="create",
    )
    conn = store._connect()
    try:
        if drift == "binding":
            conn.execute(
                "DELETE FROM rca_trigger_bindings WHERE source_id = ?",
                (envelope.source_id,),
            )
        else:
            conn.execute(
                "UPDATE kafka_inbox SET offset_id = 99 WHERE event_uid = ?",
                (metadata["event_uid"],),
            )
    finally:
        conn.close()

    expected = (
        "w3_snapshot_source_binding_mismatch"
        if drift == "binding"
        else "w3_snapshot_source_authority_mismatch"
    )
    with pytest.raises(RecordConflictError, match=expected):
        _persist_snapshot_source(
            store,
            snapshot=snapshot,
            envelope=envelope,
        )
    assert store.list_rows("rca_canonical_requests") == []
    assert store.list_rows("rca_source_authority_receipts") == []


@pytest.mark.parametrize("drift", ["offset", "event_uid", "generation_dedupe"])
def test_control_store_current_validation_detects_post_persist_transport_drift(
    tmp_path, drift
):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    _request, snapshot = _snapshot()
    metadata = _kafka_metadata()
    envelope = build_snapshot_source_envelope(
        snapshot=snapshot,
        source_id="post-persist-drift-source",
        source_kind="kafka_workflow_event",
        source_metadata=metadata,
        anchor={"issue_target": BASE_URL, "thread_target": None},
        ingress_decision=_ingress(),
    )
    _seed_control_source(
        store,
        snapshot=snapshot,
        source_id=envelope.source_id,
        source_kind=envelope.source_kind,
        source_metadata=metadata,
        binding_action="create",
    )
    _persist_snapshot_source(
        store,
        snapshot=snapshot,
        envelope=envelope,
    )
    conn = store._connect()
    try:
        if drift == "offset":
            conn.execute(
                "UPDATE kafka_inbox SET offset_id=99 WHERE event_uid=?",
                (metadata["event_uid"],),
            )
        elif drift == "event_uid":
            conn.execute(
                "UPDATE rca_trigger_sources SET kafka_event_uid=NULL "
                "WHERE source_id=?",
                (envelope.source_id,),
            )
        else:
            conn.execute(
                "UPDATE rca_trigger_sources SET source_dedupe_key=? "
                "WHERE source_id=?",
                (
                    f"{metadata['event_uid']}:generation:999",
                    envelope.source_id,
                ),
            )
    finally:
        conn.close()

    with pytest.raises(
        RuntimeError,
        match="incompatible_control_store_schema:"
        "(v11_source_authority_transport_binding|"
        "v11_envelope_source_business_binding)",
    ):
        RcaControlStore(path, require_current=True)


def test_control_store_authority_pins_fail_closed_and_exact_retry_is_idempotent(
    tmp_path,
):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    _request, snapshot = _snapshot()
    metadata = _kafka_metadata()
    envelope = build_snapshot_source_envelope(
        snapshot=snapshot,
        source_id="kafka-source",
        source_kind="kafka_workflow_event",
        source_metadata=metadata,
        anchor={"issue_target": BASE_URL, "thread_target": None},
        ingress_decision=_ingress(),
    )
    _seed_control_source(
        store,
        snapshot=snapshot,
        source_id=envelope.source_id,
        source_kind=envelope.source_kind,
        source_metadata=metadata,
        binding_action="create",
    )

    with pytest.raises(RcaAdmissionError, match="ticket_title_authority_mismatch"):
        _persist_snapshot_source(
            store,
            snapshot=snapshot,
            envelope=envelope,
            expected_ticket_title_sha256="f" * 64,
        )
    assert store.list_rows("rca_canonical_requests") == []

    first = _persist_snapshot_source(
        store,
        snapshot=snapshot,
        envelope=envelope,
    )
    second = _persist_snapshot_source(
        store,
        snapshot=snapshot,
        envelope=envelope,
    )
    assert first["snapshot_created"] is True
    assert first["source_envelope_created"] is True
    assert second["snapshot_created"] is False
    assert second["source_envelope_created"] is False


def _seed_steady_execution(
    store,
    admission,
    *,
    source_kind="kafka",
    source_identity=None,
):
    if source_identity is None:
        source_identity = {"event_uid": "topic:0:1"}
    source_identity_sha256, _normalized = store._normalize_activation_source_identity(
        source_kind,
        source_identity,
    )
    admission_key = store._activation_admission_key(
        source_kind=source_kind,
        source_identity_sha256=source_identity_sha256,
        business_key=admission.business_key,
        submission_key=admission.submission_key,
        generation=admission.generation,
    )
    conn = store._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """
            INSERT INTO rca_activation_epochs(
                epoch_id, state, is_current,
                preauthorization_fingerprint,
                preauthorization_gate_receipt_sha256,
                preauthorization_capsule_sha256,
                preproduction_fingerprint,
                preproduction_gate_receipt_sha256,
                preproduction_capsule_sha256,
                config_sha256,
                db_logical_identity_json, db_logical_identity_sha256,
                partition_start_fence_json, partition_start_fence_sha256,
                created_at, updated_at, steady_activated_at
            ) VALUES(
                'epoch-w3-test', 'steady_active', 1,
                ?, ?, ?, ?, ?, ?, ?, '{}', ?, '{}', ?, ?, ?, ?
            )
            """,
            tuple(f"{value:x}" * 64 for value in range(1, 10))
            + (OBSERVED_AT, OBSERVED_AT, OBSERVED_AT),
        )
        cursor = conn.execute(
            """
            INSERT INTO rca_activation_admission_ledger(
                epoch_id, admission_key, entrypoint, source_kind,
                source_identity_sha256, slot_kind, decision, reason,
                business_key, submission_key, generation,
                first_adjudicated_at, last_adjudicated_at, admitted_at
            ) VALUES(
                'epoch-w3-test', ?, ?, ?,
                ?, NULL, 'admit', 'activation_steady_active', ?, ?, ?, ?, ?, ?
            )
            """,
            (
                admission_key,
                "manual_admit" if source_kind == "manual" else "kafka_ingest",
                source_kind,
                source_identity_sha256,
                admission.business_key,
                admission.submission_key,
                admission.generation,
                OBSERVED_AT,
                OBSERVED_AT,
                OBSERVED_AT,
            ),
        )
        ledger_id = int(cursor.lastrowid)
        conn.commit()
        return ledger_id
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def test_control_store_roots_active_snapshot_in_exact_execution_ledger(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    admission, context = _admission_and_context()
    ledger_id = _seed_steady_execution(store, admission)
    request = build_canonical_rca_request(
        admission=admission,
        trigger_context=context,
        **_contract_kwargs(),
    )

    def active_snapshot(bound_ledger_id):
        return build_admission_snapshot(
            request=request,
            admission=admission,
            execution_admission={
                "activation_epoch_id": "epoch-w3-test",
                "activation_ledger_id": bound_ledger_id,
                "decision": "admit",
                "reason": "activation_steady_active",
                "state": "steady_active",
                "legacy_unconfigured": False,
            },
        )

    metadata = _kafka_metadata()
    wrong_snapshot = active_snapshot(ledger_id + 1)
    wrong_envelope = build_snapshot_source_envelope(
        snapshot=wrong_snapshot,
        source_id="active-kafka-source",
        source_kind="kafka_workflow_event",
        source_metadata=metadata,
        anchor={"issue_target": BASE_URL, "thread_target": None},
        ingress_decision=_ingress(),
    )
    _seed_control_source(
        store,
        snapshot=wrong_snapshot,
        source_id=wrong_envelope.source_id,
        source_kind=wrong_envelope.source_kind,
        source_metadata=metadata,
        binding_action="create",
    )

    legacy_snapshot = build_admission_snapshot(
        request=request,
        admission=admission,
        execution_admission=_execution_admission(),
    )
    legacy_envelope = build_snapshot_source_envelope(
        snapshot=legacy_snapshot,
        source_id="active-kafka-source",
        source_kind="kafka_workflow_event",
        source_metadata=metadata,
        anchor={"issue_target": BASE_URL, "thread_target": None},
        ingress_decision=_ingress(),
    )
    with pytest.raises(
        RecordConflictError, match="w3_snapshot_execution_authority_mismatch"
    ):
        _persist_snapshot_source(
            store,
            snapshot=legacy_snapshot,
            envelope=legacy_envelope,
        )

    with pytest.raises(
        RecordConflictError, match="w3_snapshot_execution_authority_mismatch"
    ):
        _persist_snapshot_source(
            store,
            snapshot=wrong_snapshot,
            envelope=wrong_envelope,
        )
    assert store.list_rows("rca_canonical_requests") == []
    assert store.list_rows("rca_source_authority_receipts") == []

    snapshot = active_snapshot(ledger_id)
    envelope = build_snapshot_source_envelope(
        snapshot=snapshot,
        source_id="active-kafka-source",
        source_kind="kafka_workflow_event",
        source_metadata=metadata,
        anchor={"issue_target": BASE_URL, "thread_target": None},
        ingress_decision=_ingress(),
    )
    result = _persist_snapshot_source(
        store,
        snapshot=snapshot,
        envelope=envelope,
    )
    assert result["snapshot_created"] is True
    assert (
        store.list_rows("rca_admission_snapshots")[0]["activation_ledger_id"]
        == ledger_id
    )

    conn = store._connect()
    try:
        conn.execute(
            "UPDATE rca_activation_epochs SET state='confirmed' "
            "WHERE epoch_id='epoch-w3-test'"
        )
    finally:
        conn.close()
    retry = _persist_snapshot_source(
        store,
        snapshot=snapshot,
        envelope=envelope,
    )
    assert retry["snapshot_created"] is False
    assert retry["source_envelope_created"] is False


def test_control_store_persists_active_operator_identity_with_durable_placeholders(
    tmp_path,
):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    admission, context = _admission_and_context(trigger_kind="manual_issue_request")
    metadata = _manual_metadata(platform="operator", mode="debug")
    ledger_id = _seed_steady_execution(
        store,
        admission,
        source_kind="manual",
        source_identity={
            "chat_id": "operator",
            "requester_id": metadata["requester_id"],
            "message_id": metadata["message_id"],
            "thread_id": "operator:issue-only",
            "issue_url": BASE_URL,
            "mode": metadata["mode"],
        },
    )
    request = build_canonical_rca_request(
        admission=admission,
        trigger_context=context,
        **_contract_kwargs(),
    )
    snapshot = build_admission_snapshot(
        request=request,
        admission=admission,
        execution_admission={
            "activation_epoch_id": "epoch-w3-test",
            "activation_ledger_id": ledger_id,
            "decision": "admit",
            "reason": "activation_steady_active",
            "state": "steady_active",
            "legacy_unconfigured": False,
        },
    )
    envelope = build_snapshot_source_envelope(
        snapshot=snapshot,
        source_id="active-operator-source",
        source_kind="feishu_group_manual",
        source_metadata=metadata,
        anchor={"issue_target": BASE_URL, "thread_target": None},
        ingress_decision=_ingress(),
    )
    _seed_control_source(
        store,
        snapshot=snapshot,
        source_id=envelope.source_id,
        source_kind=envelope.source_kind,
        source_metadata=metadata,
        binding_action="create",
    )

    result = _persist_snapshot_source(
        store,
        snapshot=snapshot,
        envelope=envelope,
    )
    assert result["snapshot_created"] is True
    assert result["source_envelope_created"] is True
