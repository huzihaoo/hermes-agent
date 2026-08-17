from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta

import pytest

from gateway.pnc_rca_control_store import RcaControlStore
from gateway.pnc_rca_delivery_contract import DeliveryContractError
from gateway.pnc_rca_delivery_store import ExecutionWatchClaim
from scripts import pnc_rca_delivery_collector as collector
from scripts import pnc_rca_outbox_dispatcher as outbox_dispatcher
from tests.gateway.test_pnc_rca_control_store import (
    TOPIC,
    _policy,
    _record,
    _terminalize_input_wait,
    _value,
)
from tests.gateway.test_pnc_rca_w3_snapshot import _runtime_authority


def test_kafka_terminal_rerun_contract_crosses_control_outbox_and_collector(
    tmp_path,
):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    epoch = store.activate_direct_steady_epoch(
        epoch_id="rca-kafka-retrigger-contract-steady",
        release_fingerprint="1" * 64,
        release_binding_sha256="2" * 64,
        config_sha256="3" * 64,
        db_logical_identity={"database": "kafka-retrigger-contract"},
        partition_start_fence={TOPIC: {"2": 0}},
        operator="integration-test",
        reason="activate steady-only Kafka retrigger contract",
    )
    first = store.ingest_record(
        _record(10),
        policy=_policy(),
        submit_enabled=True,
        snapshot_authority=_runtime_authority(),
    )
    _terminalize_input_wait(store, first.submission_key)
    second = store.ingest_record(
        _record(11, value=_value(updated_at=1783659999999)),
        policy=_policy(),
        submit_enabled=True,
    )
    assert second.generation == 2

    outbox = next(
        row for row in store.list_rows("rca_outbox") if row["generation"] == 2
    )
    trigger = next(
        row
        for row in store.list_rows("business_triggers")
        if row["generation"] == 2
    )
    ledger = next(
        row
        for row in store.list_rows("rca_activation_admission_ledger")
        if row["generation"] == 2 and row["decision"] == "admit"
    )
    assert ledger["bound_at"] is not None
    assert outbox["activation_epoch_id"] == epoch["epoch_id"]
    assert outbox["activation_ledger_id"] == ledger["ledger_id"]
    assert trigger["activation_epoch_id"] == epoch["epoch_id"]
    assert trigger["activation_ledger_id"] == ledger["ledger_id"]
    claim = store.claim_outbox(
        lease_owner="kafka-rerun-contract",
        now=datetime.fromisoformat(outbox["retry_window_started_at"])
        + timedelta(seconds=1),
    )
    assert claim is not None
    admission, trigger_context = outbox_dispatcher._validated_claim_contract(claim)

    assert admission.trigger_kind == "kafka_retrigger"
    assert admission.generation == claim.generation == 2
    assert trigger_context["source_kind"] == "kafka_workflow_event"
    assert admission.source_refs.topic == claim.source_topic
    assert admission.source_refs.partition == claim.source_partition
    assert admission.source_refs.offset == claim.source_offset
    source = next(
        row
        for row in store.list_rows("rca_trigger_sources")
        if row["source_id"] == claim.origin_source_id
    )
    assert source["mode"] == "kafka_retrigger"
    assert source["kafka_event_uid"] is None

    watch_claim = ExecutionWatchClaim(
        submission_key=claim.submission_key,
        submission_outbox_id=claim.outbox_id,
        business_key=claim.business_key,
        generation=claim.generation,
        project_key=admission.source_refs.project_key,
        work_item_type_key=admission.source_refs.work_item_type_key,
        work_item_id=admission.source_refs.work_item_id,
        task_id=claim.submission_key,
        state="pending",
        poll_attempt=1,
        fence=1,
        lease_token="collector-lease",
        lease_owner="collector",
        lease_expires_at=claim.lease_expires_at,
        work_started_at=claim.created_at,
        terminal_first_seen_at=None,
        submission_payload=claim.payload,
        submission_result={
            "success": True,
            "submission_key": claim.submission_key,
            "task_id": claim.submission_key,
        },
        origin_source_id=claim.origin_source_id,
        trigger_origin_source_id=claim.origin_source_id,
    )

    assert collector._submission_admission(watch_claim) == admission

    forged_payload = deepcopy(claim.payload)
    forged_payload["admission"]["trigger_kind"] = "manual_retrigger"
    forged_outbox_claim = replace(claim, payload=forged_payload)
    with pytest.raises(outbox_dispatcher.DispatchCircuitError) as outbox_error:
        outbox_dispatcher._validated_claim_contract(forged_outbox_claim)
    assert outbox_error.value.code == "dispatcher_outbox_contract_invalid"

    forged_watch_claim = replace(
        watch_claim,
        submission_payload=forged_payload,
    )
    with pytest.raises(
        DeliveryContractError,
        match="submission_outbox_contract_invalid",
    ):
        collector._submission_admission(forged_watch_claim)
