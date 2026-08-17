import copy
from dataclasses import replace
from datetime import datetime, timezone
import json
import sqlite3
from types import SimpleNamespace

import pytest

import gateway.pnc_rca_snapshot as snapshot_module
from gateway.pnc_rca_admission import (
    RcaAdmissionError,
    build_rca_admission,
    build_rca_trigger_context,
)
from gateway.pnc_rca_control_store import (
    KafkaRecord,
    MANUAL_TRIGGER_SCHEMA_VERSION,
    ManualRcaAdmissionError,
    ManualRcaTriggerRequest,
    RcaControlStore,
    RecordConflictError,
    RecordProcessingBlockedError,
    W3_AUTOMATIC_OBSERVATION_SNAPSHOT_MISMATCH_REASON,
    W3_KAFKA_OBSERVATION_JOIN_REASON,
    W3_LEGACY_PARENT_SNAPSHOT_MISSING_REASON,
    build_w3_manual_ingress_authority,
    build_w3_ticket_authority_receipt,
)
from gateway.pnc_rca_delivery_contract import DeliveryContractError
from gateway.pnc_rca_kafka_contract import WorkflowEventPolicy, WorkflowTransition
from gateway.pnc_rca_policy_config import W3_SNAPSHOT_AUTHORITY_SCHEMA_VERSION
from gateway.pnc_rca_schema import (
    RcaIssueContext,
    to_dict as rca_to_dict,
)
from gateway.pnc_rca_snapshot import (
    UNISSUED_WRITE_FENCE,
    AdmissionSnapshotExecutionBundle,
    build_admission_snapshot as _build_admission_snapshot,
    build_canonical_rca_request as _build_canonical_rca_request,
    build_snapshot_source_envelope as _build_snapshot_source_envelope,
    build_source_authority_receipt,
    canonical_json_sha256,
    canonical_ticket_title_sha256,
    compare_snapshot_shadow as _compare_snapshot_shadow,
    compose_snapshot_projection as _compose_snapshot_projection,
    legacy_semantic_projection as _legacy_semantic_projection,
    snapshot_execution_inputs,
    validate_admission_snapshot as _validate_admission_snapshot,
    validate_snapshot_source_envelope as _validate_snapshot_source_envelope,
    validate_snapshot_execution_bundle,
)
from scripts import pnc_rca_outbox_dispatcher as outbox_dispatcher
from scripts import pnc_rca_delivery_collector as delivery_collector
from tests.gateway.test_pnc_rca_control_store import _terminalize_input_wait


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
STEADY_EPOCH_ID = "epoch-w3-test"


def _activate_steady(store: RcaControlStore) -> RcaControlStore:
    if store.activation_epoch() is None:
        store.activate_direct_steady_epoch(
            epoch_id=STEADY_EPOCH_ID,
            release_fingerprint="1" * 64,
            release_binding_sha256="2" * 64,
            config_sha256="3" * 64,
            db_logical_identity={"database": "w3-control-test"},
            partition_start_fence={"topic": {"0": 0}},
            operator="w3-test",
            reason="activate steady W3 test runtime",
        )
    return store


def _steady_control_store(path) -> RcaControlStore:
    return _activate_steady(RcaControlStore(path))


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
        "business_profile": _policy(
            "business_profile",
            {
                "status": "matched",
                "profile_id": "g1q3",
                "execution_readiness": "ready",
                "resource_class": "rca_prod",
                "artifact_kind": "rca_html_report_and_viz_mcap",
                "artifact_namespace": "rca/g1q3",
            },
        ),
        "execution_policy": _policy(
            "execution_policy",
            {
                "request_schema": "g1q3_rca_execution_request_v2",
                "data_access_mode": "remote_read",
                "allow_download": False,
                "input_materialization": "forbidden",
                "derived_artifacts_allowed": True,
                "allow_feishu_writeback": False,
                "group_response_cap": "L1",
                "translate_baseline": "production",
                "translate_contract_path": "",
            },
        ),
        "publication_policy": _policy("publication_policy", {"target": "issue"}),
        "correction_lineage_policy": _policy("correction_lineage_policy", {"version": 1}),
    }


def _runtime_authority(policies=None):
    policy_set = _contract_kwargs() if policies is None else policies
    body = {
        "schema_version": W3_SNAPSHOT_AUTHORITY_SCHEMA_VERSION,
        "policies": policy_set,
    }
    return {**body, "authority_sha256": canonical_json_sha256(body)}


def _kafka_policy():
    return WorkflowEventPolicy(
        topic="topic",
        policy_version=BASE["rule_version"],
        project_keys=frozenset({BASE["project_key"]}),
        project_simple_names=frozenset({BASE["project_simple_name"]}),
        work_item_type_keys=frozenset({BASE["work_item_type_key"]}),
        status_change_types=frozenset({"Reached"}),
        transitions=(
            WorkflowTransition(
                state_key="new-problem-state",
                pre_status=1,
                cur_status=2,
            ),
        ),
    )


def _kafka_record(*, offset=1):
    return KafkaRecord(
        topic="topic",
        partition=0,
        offset=offset,
        value=json.dumps(
            {
                "id": int(BASE["work_item_id"]),
                "name": "ACC braking issue",
                "nodes": [
                    {
                        "state_key": "new-problem-state",
                        "node_name": "New problem",
                        "pre_status": 1,
                        "cur_status": 2,
                    }
                ],
                "project_key": BASE["project_key"],
                "project_simple_name": BASE["project_simple_name"],
                "status_change_type": "Reached",
                "updated_at": 1783650000000,
                "work_item_type_key": BASE["work_item_type_key"],
            },
            sort_keys=True,
        ).encode(),
    )


def _kafka_update_record(*, offset=2, title="ACC braking issue"):
    record = _kafka_record(offset=offset)
    value = json.loads(record.value)
    value["name"] = title
    value["updated_at"] += offset
    return replace(record, value=json.dumps(value, sort_keys=True).encode())


def _manual_request(*, message_id="om_manual", mode="run_or_join"):
    return ManualRcaTriggerRequest(
        schema_version=MANUAL_TRIGGER_SCHEMA_VERSION,
        issue_url=BASE_URL,
        mode=mode,
        reason="manual_explicit_issue_action",
        platform="feishu",
        chat_id="oc_allowed",
        thread_id="topic:omt_root",
        message_id=message_id,
        requester_id="ou_requester",
    )


def _gateway_runtime_identity():
    return {
        "service_label": "ai.hermes.gateway",
        "pid": 42000,
        "process_create_time": 1_783_650_000.0,
        "boot_time": 1_783_000_000.0,
        "executable": "/candidate/.venv/bin/python",
        "script": "/candidate/gateway/run.py",
        "cwd": "/candidate",
        "script_sha256": "a" * 64,
        "runtime_files_sha256": "b" * 64,
        "public_config_sha256": "c" * 64,
        "loaded_runtime_sha256": "d" * 64,
    }


def _manual_authorization(*, mode="run_or_join"):
    return {
        "schema_version": "pnc_rca_manual_authorization_v2",
        "manual_intake_enabled": True,
        "manual_chat_allowlist_valid": True,
        "manual_chat_allowlist_sha256": "1" * 64,
        "chat_allowed": True,
        "mention_verified": True,
        "debug_requested": mode in {"rerun", "debug"},
        "debug_enabled": mode in {"rerun", "debug"},
        "requester_allowed": True,
        "debug_user_allowlist_sha256": "2" * 64,
        "manual_operator_rate_limit": 3,
        "manual_operator_rate_window_seconds": 600,
        "authorized": True,
    }


def _manual_w3_roots(*, request=None, policies=None):
    manual = request or _manual_request()
    authority = _runtime_authority(policies)
    ticket = build_w3_ticket_authority_receipt(
        source_kind="feishu_official_preread",
        source_evidence_sha256="e" * 64,
        observed_at=OBSERVED_AT,
        project_key=BASE["project_key"],
        project_simple_name=BASE["project_simple_name"],
        work_item_type_key=BASE["work_item_type_key"],
        work_item_id=BASE["work_item_id"],
        issue_url=BASE_URL,
        title="ACC braking issue",
    )
    source_identity = {
        "platform": manual.platform,
        "chat_id": manual.chat_id,
        "thread_id": manual.thread_id,
        "message_id": manual.message_id,
        "requester_id": manual.requester_id,
        "issue_url": manual.issue_url,
        "mode": manual.mode,
    }
    ingress = build_w3_manual_ingress_authority(
        manual_authorization=_manual_authorization(mode=manual.mode),
        gateway_runtime_identity=_gateway_runtime_identity(),
        source_identity=source_identity,
        ticket_authority_sha256=ticket["ticket_authority_sha256"],
        snapshot_authority_sha256=authority["authority_sha256"],
    )
    return authority, ticket, ingress


def _admit_manual_w3(store, *, request=None, policies=None):
    manual = request or _manual_request()
    authority, ticket, ingress = _manual_w3_roots(
        request=manual,
        policies=policies,
    )
    return store.admit_manual_trigger(
        manual,
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        operator_authorized=manual.mode in {"rerun", "debug"},
        active_policy=_kafka_policy(),
        snapshot_authority=authority,
        snapshot_ticket_authority=ticket,
        snapshot_manual_ingress_authority=ingress,
    )


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


def _steady_execution_admission(*, ledger_id=1):
    return {
        "activation_epoch_id": STEADY_EPOCH_ID,
        "activation_ledger_id": ledger_id,
        "decision": "admit",
        "reason": "activation_steady_active",
        "state": "steady_active",
        "legacy_unconfigured": False,
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
        execution_admission=_steady_execution_admission(),
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
        execution_admission=_steady_execution_admission(),
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
    forged["value"]["translate_baseline"] = "other"
    with pytest.raises(RcaAdmissionError, match="digest"):
        build_canonical_rca_request(
            admission=admission,
            trigger_context=context,
            **{**kwargs, "execution_policy": forged},
        )
    self_signed_value = copy.deepcopy(kwargs["execution_policy"]["value"])
    self_signed_value["translate_baseline"] = "wrong-but-valid"
    self_signed = _policy("execution_policy", self_signed_value)
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
    if policy_name in {"business_profile", "execution_policy"}:
        with pytest.raises(
            RcaAdmissionError,
            match=(
                "business_profile_not_execution_ready"
                if policy_name == "business_profile"
                else "execution_request_policy_value_exact_fields_invalid"
            ),
        ):
            build_canonical_rca_request(
                admission=admission,
                trigger_context=context,
                expected_policy_sha256s=diagnostic_authorities,
                **kwargs,
            )
        return
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
    _activate_steady(store)
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
            activation_source_kind = "kafka"
            activation_source_identity = {"event_uid": event_uid}
            activation_entrypoint = "kafka_ingest"
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
            activation_source_kind = "manual"
            activation_chat_id = source_metadata["chat_id"]
            activation_thread_id = source_metadata["thread_id"]
            if source_metadata["platform"] == "operator":
                activation_chat_id = "operator"
                activation_thread_id = "operator:issue-only"
            activation_source_identity = {
                "chat_id": activation_chat_id,
                "requester_id": source_metadata["requester_id"],
                "message_id": source_metadata["message_id"],
                "thread_id": activation_thread_id,
                "issue_url": snapshot.canonical_request.ticket["issue_url"],
                "mode": source_metadata["mode"],
            }
            activation_entrypoint = "manual_admit"
        if binding_action == "create":
            store.adjudicate_activation_tx(
                conn,
                entrypoint=activation_entrypoint,
                source_kind=activation_source_kind,
                source_identity=activation_source_identity,
                business_key=snapshot.resolved_admission["business_key"],
                submission_key=snapshot.resolved_admission["submission_key"],
                generation=generation,
                new_execution=True,
                ingress_epoch_id=STEADY_EPOCH_ID,
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


def _persist_snapshot_source_tx(
    store,
    conn,
    *,
    snapshot,
    envelope,
    expected_generation_authorization_evidence_sha256=None,
    expected_ticket_title_sha256=TITLE_AUTHORITY,
):
    return store.persist_admission_snapshot_source_tx(
        conn,
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


def test_control_store_tx_persistence_requires_caller_transaction(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
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
    conn = store._connect()
    try:
        with pytest.raises(RuntimeError, match="w3_snapshot_transaction_required"):
            _persist_snapshot_source_tx(
                store,
                conn,
                snapshot=snapshot,
                envelope=envelope,
            )
    finally:
        conn.close()


def test_control_store_tx_persistence_obeys_caller_rollback(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
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
    conn = store._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        result = _persist_snapshot_source_tx(
            store,
            conn,
            snapshot=snapshot,
            envelope=envelope,
        )
        assert result["snapshot_created"] is True
        assert conn.in_transaction is True
        assert conn.execute(
            "SELECT COUNT(*) FROM rca_admission_snapshots"
        ).fetchone()[0] == 1
        conn.rollback()
    finally:
        conn.close()

    assert store.list_rows("rca_canonical_requests") == []
    assert store.list_rows("rca_source_authority_receipts") == []
    assert store.list_rows("rca_admission_snapshots") == []
    assert store.list_rows("rca_snapshot_source_envelopes") == []


def test_control_store_tx_persistence_obeys_caller_commit(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
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
    conn = store._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        _persist_snapshot_source_tx(
            store,
            conn,
            snapshot=snapshot,
            envelope=envelope,
        )
        assert conn.in_transaction is True
        conn.commit()
    finally:
        conn.close()

    assert len(store.list_rows("rca_canonical_requests")) == 1
    assert len(store.list_rows("rca_source_authority_receipts")) == 1
    assert len(store.list_rows("rca_admission_snapshots")) == 1
    assert len(store.list_rows("rca_snapshot_source_envelopes")) == 1


def test_control_store_public_persistence_rolls_back_after_tx_helper_fault(
    tmp_path, monkeypatch
):
    store = _steady_control_store(tmp_path / "control.sqlite3")
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
    original = store.persist_admission_snapshot_source_tx

    def fail_after_write(conn, **kwargs):
        assert conn.in_transaction is True
        original(conn, **kwargs)
        raise RuntimeError("injected_after_w3_tx_write")

    monkeypatch.setattr(store, "persist_admission_snapshot_source_tx", fail_after_write)
    with pytest.raises(RuntimeError, match="injected_after_w3_tx_write"):
        _persist_snapshot_source(
            store,
            snapshot=snapshot,
            envelope=envelope,
        )

    assert store.list_rows("rca_canonical_requests") == []
    assert store.list_rows("rca_source_authority_receipts") == []
    assert store.list_rows("rca_admission_snapshots") == []
    assert store.list_rows("rca_snapshot_source_envelopes") == []


def test_kafka_entrypoint_persists_w3_steady_admission_atomically(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")

    result = store.ingest_record(
        _kafka_record(),
        policy=_kafka_policy(),
        submit_enabled=True,
        snapshot_authority=_runtime_authority(),
    )

    assert result.decision == "accepted"
    [trigger] = store.list_rows("business_triggers")
    [outbox] = store.list_rows("rca_outbox")
    [source] = store.list_rows("rca_trigger_sources")
    [binding] = store.list_rows("rca_trigger_bindings")
    [snapshot] = store.list_rows("rca_admission_snapshots")
    [envelope] = store.list_rows("rca_snapshot_source_envelopes")
    inbox = store.get_inbox(result.event_uid)

    assert trigger["state"] == outbox["status"] == "pending"
    assert binding["role"] == "origin"
    assert binding["source_id"] == source["source_id"]
    assert snapshot["business_key"] == trigger["business_key"] == result.business_key
    assert snapshot["submission_key"] == outbox["submission_key"]
    assert snapshot["generation"] == trigger["generation"] == result.generation
    assert snapshot["execution_decision"] == "admit"
    assert snapshot["execution_state"] == "steady_active"
    assert envelope["decision"] == "admit"
    assert envelope["binding_action"] == "create"
    assert envelope["source_id"] == source["source_id"]
    assert envelope["snapshot_sha256"] == snapshot["snapshot_sha256"]
    assert inbox["decision"] == "accepted"
    assert store.list_rows("kafka_partition_progress")[0]["last_event_uid"] == result.event_uid


def test_kafka_entrypoint_w3_late_fault_rolls_back_classification_only(
    tmp_path, monkeypatch
):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    original = store.persist_admission_snapshot_source_tx

    def fail_after_write(conn, **kwargs):
        original(conn, **kwargs)
        raise RuntimeError("injected_after_w3_entrypoint_write")

    monkeypatch.setattr(store, "persist_admission_snapshot_source_tx", fail_after_write)
    record = _kafka_record()
    with pytest.raises(RecordProcessingBlockedError):
        store.ingest_record(
            record,
            policy=_kafka_policy(),
            submit_enabled=True,
            snapshot_authority=_runtime_authority(),
        )

    inbox = store.get_inbox(record.event_uid)
    assert inbox["decision"] == "pending"
    assert inbox["processing_attempts"] == 1
    assert inbox["last_processing_error_code"] == "record_processing_RuntimeError"
    for table in (
        "business_triggers",
        "rca_outbox",
        "rca_trigger_sources",
        "rca_trigger_bindings",
        "rca_delivery_subscriptions",
        "rca_trigger_delivery_bindings",
        "rca_canonical_requests",
        "rca_source_authority_receipts",
        "rca_admission_snapshots",
        "rca_snapshot_source_envelopes",
        "kafka_partition_progress",
    ):
        assert store.list_rows(table) == []


def test_kafka_entrypoint_exact_replay_does_not_rewrite_w3_authority(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    record = _kafka_record()
    kwargs = {
        "policy": _kafka_policy(),
        "submit_enabled": True,
        "snapshot_authority": _runtime_authority(),
    }

    first = store.ingest_record(record, **kwargs)
    before = {
        table: store.list_rows(table)
        for table in (
            "rca_canonical_requests",
            "rca_source_authority_receipts",
            "rca_admission_snapshots",
            "rca_snapshot_source_envelopes",
        )
    }
    replay = store.ingest_record(record, **kwargs)

    assert first.raw_inserted is True
    assert replay.raw_inserted is False
    assert replay.transport_duplicate is True
    assert before == {table: store.list_rows(table) for table in before}




def test_manual_entrypoint_persists_w3_creator_in_admission_transaction(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")

    result = _admit_manual_w3(store)

    assert result.outcome == "created"
    [source] = store.list_rows("rca_trigger_sources")
    [binding] = store.list_rows("rca_trigger_bindings")
    [snapshot] = store.list_rows("rca_admission_snapshots")
    [envelope] = store.list_rows("rca_snapshot_source_envelopes")
    assert source["source_kind"] == "feishu_group_manual"
    assert binding["role"] == "origin"
    assert snapshot["business_key"] == result.business_key
    assert snapshot["submission_key"] == result.submission_key
    assert snapshot["generation"] == result.generation == 1
    assert snapshot["execution_decision"] == "admit"
    assert envelope["binding_action"] == "create"
    assert envelope["decision"] == "admit"
    assert envelope["source_id"] == result.source_id



def test_manual_entrypoint_exact_replay_does_not_rewrite_w3_authority(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    request = _manual_request()
    first = _admit_manual_w3(store, request=request)
    before = {
        table: store.list_rows(table)
        for table in (
            "rca_canonical_requests",
            "rca_source_authority_receipts",
            "rca_admission_snapshots",
            "rca_snapshot_source_envelopes",
        )
    }

    replay = _admit_manual_w3(store, request=request)

    assert replay.source_id == first.source_id
    assert replay.reason == "idempotent_source_replay"
    assert before == {table: store.list_rows(table) for table in before}


def test_manual_entrypoint_w3_late_fault_rolls_back_legacy_and_snapshot(
    tmp_path, monkeypatch
):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    original = store.persist_admission_snapshot_source_tx

    def fail_after_write(conn, **kwargs):
        original(conn, **kwargs)
        raise RuntimeError("injected_after_manual_w3_write")

    monkeypatch.setattr(store, "persist_admission_snapshot_source_tx", fail_after_write)
    with pytest.raises(RuntimeError, match="injected_after_manual_w3_write"):
        _admit_manual_w3(store)

    for table in (
        "business_triggers",
        "rca_outbox",
        "rca_trigger_sources",
        "rca_trigger_bindings",
        "rca_delivery_subscriptions",
        "rca_trigger_delivery_bindings",
        "rca_canonical_requests",
        "rca_source_authority_receipts",
        "rca_admission_snapshots",
        "rca_snapshot_source_envelopes",
    ):
        assert store.list_rows(table) == []


def test_control_store_persists_exact_creator_before_generation_two_join(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
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
    store = _steady_control_store(tmp_path / "control.sqlite3")
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
    store = _steady_control_store(tmp_path / "control.sqlite3")
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
    store = _steady_control_store(path)
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
    store = _steady_control_store(tmp_path / "control.sqlite3")
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
    _activate_steady(store)
    conn = store._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        decision = store.adjudicate_activation_tx(
            conn,
            entrypoint="manual_admit" if source_kind == "manual" else "kafka_ingest",
            source_kind=source_kind,
            source_identity=source_identity,
            business_key=admission.business_key,
            submission_key=admission.submission_key,
            generation=admission.generation,
            new_execution=True,
            ingress_epoch_id=STEADY_EPOCH_ID,
        )
        conn.commit()
        assert decision.ledger_id is not None
        return decision.ledger_id
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def test_control_store_roots_active_snapshot_in_exact_execution_ledger(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
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
        RecordConflictError, match="w3_snapshot_steady_activation_required"
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
    store = _steady_control_store(tmp_path / "control.sqlite3")
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


def test_control_store_reads_approved_w3_execution_bundle(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    authority = _runtime_authority()
    admitted = _admit_manual_w3(store)

    bundle = store.read_w3_execution_snapshot(
        admitted.submission_key,
        snapshot_authority=authority,
        required=True,
    )

    assert isinstance(bundle, AdmissionSnapshotExecutionBundle)
    admission, context = snapshot_execution_inputs(bundle)
    assert admission.submission_key == admitted.submission_key
    assert admission.business_key == admitted.business_key
    assert admission.trigger_kind == "manual_issue_request"
    assert context.title == "ACC braking issue"
    assert bundle.snapshot_authority_sha256 == authority["authority_sha256"]
    assert validate_snapshot_execution_bundle(bundle.to_dict()) == bundle


def test_w3_execution_bundle_ignores_mutable_legacy_outbox_payload(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    authority = _runtime_authority()
    admitted = _admit_manual_w3(store)
    before = store.read_w3_execution_snapshot(
        admitted.submission_key,
        snapshot_authority=authority,
        required=True,
    )
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            "UPDATE rca_outbox SET payload_json = '{}' WHERE submission_key = ?",
            (admitted.submission_key,),
        )

    after = store.read_w3_execution_snapshot(
        admitted.submission_key,
        snapshot_authority=authority,
        required=True,
    )

    assert after == before
    assert snapshot_execution_inputs(after) == snapshot_execution_inputs(before)


def test_dispatcher_builds_execution_request_only_from_w3_bundle(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    authority = _runtime_authority()
    admitted = _admit_manual_w3(store)
    claim = store.claim_outbox(lease_owner="w3-dispatcher-test")
    assert claim is not None
    assert claim.submission_key == admitted.submission_key
    bundle = store.read_w3_execution_snapshot(
        claim.submission_key,
        snapshot_authority=authority,
        required=True,
    )

    admission, event = outbox_dispatcher._validated_claim_contract(
        replace(claim, payload={"forged": True}),
        snapshot_bundle=bundle,
    )
    assert admission.submission_key == admitted.submission_key
    assert event["title"] == "ACC braking issue"
    request = outbox_dispatcher.build_dispatch_execution_request(
        claim=replace(claim, payload={}),
        admission=admission,
        issue_context=RcaIssueContext(
            project_key=BASE["project_key"],
            work_item_type=BASE["work_item_type_key"],
            work_item_id=BASE["work_item_id"],
            url=BASE_URL,
            title="ACC braking issue",
            source_quality="partial",
            pdcl_download_cmd="mdi download event -u event-7041712812 -s ./",
            business_profile={
                "status": "matched",
                "profile_id": "live-profile-must-not-win",
                "execution_readiness": "ready",
                "resource_class": "rca_prod",
                "artifact_kind": "forged-live-kind",
                "artifact_namespace": "forged/live",
            },
        ),
        config=SimpleNamespace(
            allow_feishu_writeback=True,
            group_response_cap="L0",
            translate_baseline="forged-live-baseline",
            translate_contract_path="/forged/live/path",
        ),
        storage_admission_summary={"status": "pass"},
        snapshot_bundle=bundle,
    )

    assert request.toolchain["w3_execution_snapshot"] == bundle.to_dict()
    assert request.toolchain["business_profile"] == (
        bundle.snapshot.canonical_request.business_profile["value"]
    )
    assert request.execution_policy["allow_feishu_writeback"] is False
    assert request.execution_policy["group_response_cap"] == "L1"
    assert request.execution_policy["translate_baseline"] == "production"
    assert request.execution_policy["translate_contract_path"] == ""
    assert request.source_refs["snapshot_bundle_sha256"] == bundle.bundle_sha256
    assert request.source_refs["origin_source_id"] == (
        bundle.creator_source_envelope.source_id
    )


def test_execution_request_preserves_valid_w3_bundle_policy_semantics(tmp_path):
    policies = _contract_kwargs()
    policies["publication_policy"] = _policy(
        "publication_policy",
        {
            "target": "issue",
            "token": "policy-token-is-semantic-data",
            "template": "  keep <!--policy-comment--> spacing  ",
        },
    )
    authority = _runtime_authority(policies)
    store = _steady_control_store(tmp_path / "control.sqlite3")
    admitted = _admit_manual_w3(store, policies=policies)
    claim = store.claim_outbox(lease_owner="w3-policy-preservation-test")
    assert claim is not None
    bundle = store.read_w3_execution_snapshot(
        admitted.submission_key,
        snapshot_authority=authority,
        required=True,
    )
    admission, _event = outbox_dispatcher._validated_claim_contract(
        claim,
        snapshot_bundle=bundle,
    )

    request = outbox_dispatcher.build_dispatch_execution_request(
        claim=claim,
        admission=admission,
        issue_context=RcaIssueContext(
            project_key=BASE["project_key"],
            work_item_type=BASE["work_item_type_key"],
            work_item_id=BASE["work_item_id"],
            url=BASE_URL,
            title="ACC braking issue",
            source_quality="partial",
            pdcl_download_cmd="mdi download event -u event-7041712812 -s ./",
        ),
        config=SimpleNamespace(
            allow_feishu_writeback=False,
            group_response_cap="L1",
            translate_baseline="production",
            translate_contract_path="",
        ),
        storage_admission_summary={"status": "pass"},
        snapshot_bundle=bundle,
    )

    preserved = request.toolchain["w3_execution_snapshot"]
    assert preserved == bundle.to_dict()
    assert validate_snapshot_execution_bundle(preserved) == bundle
    publication = preserved["snapshot"]["canonical_request"]["publication_policy"]
    assert publication["value"] == policies["publication_policy"]["value"]
    serialized = rca_to_dict(request)
    assert serialized["toolchain"]["w3_execution_snapshot"] == bundle.to_dict()
    assert validate_snapshot_execution_bundle(
        serialized["toolchain"]["w3_execution_snapshot"]
    ) == bundle

    smuggled = copy.deepcopy(bundle.to_dict())
    smuggled["snapshot"]["canonical_request"]["publication_policy"]["value"][
        "target"
    ] = "issue<!--smuggled-->"
    tampered_request = replace(
        request,
        toolchain={**request.toolchain, "w3_execution_snapshot": smuggled},
    )
    with pytest.raises(RcaAdmissionError, match="publication_policy_digest_mismatch"):
        rca_to_dict(tampered_request)


@pytest.mark.parametrize("shape_kind", ["depth", "bytes"])
def test_dispatcher_rejects_vm_envelope_excess_before_capacity_reservation(
    tmp_path, shape_kind
):
    nested = "leaf"
    for _index in range(30):
        nested = {"n": nested}
    oversized = nested if shape_kind == "depth" else "x" * 1_000_000
    policies = _contract_kwargs()
    policies["publication_policy"] = _policy(
        "publication_policy",
        {"target": "issue", "nested": oversized},
    )
    authority = _runtime_authority(policies)
    store = _steady_control_store(tmp_path / "control.sqlite3")
    admitted = _admit_manual_w3(store, policies=policies)
    claim = store.claim_outbox(lease_owner="w3-shape-limit-test")
    assert claim is not None
    bundle = store.read_w3_execution_snapshot(
        admitted.submission_key,
        snapshot_authority=authority,
        required=True,
    )
    admission, _event = outbox_dispatcher._validated_claim_contract(
        claim,
        snapshot_bundle=bundle,
    )

    with pytest.raises(outbox_dispatcher.DispatchCircuitError) as raised:
        outbox_dispatcher.build_dispatch_execution_request(
            claim=claim,
            admission=admission,
            issue_context=RcaIssueContext(
                project_key=BASE["project_key"],
                work_item_type=BASE["work_item_type_key"],
                work_item_id=BASE["work_item_id"],
                url=BASE_URL,
                title="ACC braking issue",
                source_quality="partial",
                pdcl_download_cmd=(
                    "mdi download event -u event-7041712812 -s ./"
                ),
            ),
            config=SimpleNamespace(
                allow_feishu_writeback=False,
                group_response_cap="L1",
                translate_baseline="production",
                translate_contract_path="",
            ),
            storage_admission_summary={"status": "pass"},
            snapshot_bundle=bundle,
        )
    assert raised.value.code == "dispatcher_execution_request_envelope_invalid"
    assert raised.value.detail == (
        "rca_vm_request_json_shape_exceeded"
        if shape_kind == "depth"
        else "rca_vm_request_json_bytes_exceeded"
    )


def test_dispatcher_missing_w3_snapshot_stops_before_external_boundaries(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    admitted = store.admit_manual_trigger(
        _manual_request(),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        active_policy=_kafka_policy(),
    )
    calls: list[str] = []
    instance = object.__new__(outbox_dispatcher.OutboxDispatcher)
    instance.store = store
    instance.config = SimpleNamespace(
        dispatch_enabled=True,
        # This fixture isolates the downstream W3 fail-closed boundary; the
        # activation-required path is covered by the dispatcher activation tests.
        activation_required=False,
        lease_seconds=180,
        max_age_seconds=86_400,
        w3_snapshot_read_mode="snapshot_required",
        w3_snapshot_authority=_runtime_authority(),
    )
    instance.workspace_runtime_guard = None
    instance.stats = outbox_dispatcher.DispatchStats()
    instance.lease_owner = "w3-missing-test"
    instance.now = lambda: datetime.now(timezone.utc)
    instance._delivery_backpressure_outcome = lambda: None
    instance.enrich = lambda _event: calls.append("enrich")
    instance.storage_admission = lambda _request: calls.append("storage")
    instance.derived_capacity_reservation = lambda _request: calls.append("reserve")
    instance.derived_capacity_abort_precreate = lambda *_args: calls.append("abort")
    instance.submit = lambda *_args: calls.append("submit")

    outcome = instance.dispatch_one()

    assert admitted.submission_key == outcome.submission_key
    assert outcome.status == "quarantined"
    assert outcome.error_code == "dispatcher_snapshot_missing"
    assert calls == []


def test_collector_admission_ignores_legacy_payload_and_binds_snapshot_receipt(
    tmp_path,
):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    authority = _runtime_authority()
    admitted = _admit_manual_w3(store)
    bundle = store.read_w3_execution_snapshot(
        admitted.submission_key,
        snapshot_authority=authority,
        required=True,
    )
    admission, _context = snapshot_execution_inputs(bundle)
    envelope = bundle.creator_source_envelope
    claim = SimpleNamespace(
        submission_key=admission.submission_key,
        business_key=admission.business_key,
        generation=admission.generation,
        project_key=admission.source_refs.project_key,
        work_item_type_key=admission.source_refs.work_item_type_key,
        work_item_id=admission.source_refs.work_item_id,
        task_id=admission.submission_key,
        origin_source_id=envelope.source_id,
        trigger_origin_source_id=envelope.source_id,
        submission_payload={"forged": True},
        submission_result={
            "success": True,
            "submission_key": admission.submission_key,
            "task_id": admission.submission_key,
            "w3_execution_snapshot": delivery_collector._w3_execution_binding(
                bundle
            ),
        },
    )

    assert delivery_collector._submission_admission(claim, bundle) == admission
    claim.submission_result["w3_execution_snapshot"]["snapshot_sha256"] = "0" * 64
    with pytest.raises(
        DeliveryContractError,
        match="w3_execution_snapshot_receipt_mismatch",
    ):
        delivery_collector._submission_admission(claim, bundle)


def test_w3_execution_bundle_rejects_unapproved_policy_authority(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    admitted = _admit_manual_w3(store)
    other = copy.deepcopy(_runtime_authority())
    execution = other["policies"]["execution_policy"]
    execution["value"]["translate_baseline"] = "other"
    execution["sha256"] = canonical_json_sha256(
        {"version": execution["version"], "value": execution["value"]}
    )
    body = {
        "schema_version": other["schema_version"],
        "policies": other["policies"],
    }
    other["authority_sha256"] = canonical_json_sha256(body)

    with pytest.raises(
        RecordConflictError,
        match="w3_execution_snapshot_authority_mismatch",
    ):
        store.read_w3_execution_snapshot(
            admitted.submission_key,
            snapshot_authority=other,
            required=True,
        )


def test_w3_execution_bundle_missing_and_tampered_fail_closed(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    authority = _runtime_authority()
    assert (
        store.read_w3_execution_snapshot(
            "g1q3-rca-s1-" + "0" * 64,
            snapshot_authority=authority,
        )
        is None
    )
    with pytest.raises(RecordConflictError, match="w3_execution_snapshot_missing"):
        store.read_w3_execution_snapshot(
            "g1q3-rca-s1-" + "0" * 64,
            snapshot_authority=authority,
            required=True,
        )

    admitted = _admit_manual_w3(store)
    bundle = store.read_w3_execution_snapshot(
        admitted.submission_key,
        snapshot_authority=authority,
        required=True,
    )
    tampered = bundle.to_dict()
    tampered["snapshot"]["canonical_request"]["ticket"]["title"] = "changed"
    with pytest.raises(RcaAdmissionError):
        validate_snapshot_execution_bundle(tampered)
