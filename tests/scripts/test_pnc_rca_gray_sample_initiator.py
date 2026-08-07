from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

import pytest

from gateway.pnc_rca_control_store import (
    KafkaRecord,
    MANUAL_TRIGGER_SCHEMA_VERSION,
    ManualRcaAdmissionError,
    ManualRcaTriggerRequest,
    RcaControlStore,
)
from gateway.pnc_rca_delivery_store import RcaDeliveryStore
from gateway.pnc_rca_gray_samples import (
    C_TOPIC_CHAT_ID,
    C_TOPIC_FIXTURE_SCHEMA_VERSION,
    C_TOPIC_ISSUE_ID,
    C_TOPIC_TEXT,
    COMMENT_BUDGET,
    DESIGN_AUTHORITY_PATH,
    DESIGN_AUTHORITY_SHA256,
    GRAY_SAMPLE_AUTOMATION_AUTHORIZATION_SCHEMA_VERSION,
    GRAY_SAMPLE_AUTOMATION_AUTHORITY_SCHEMA_VERSION,
    GRAY_SAMPLE_CONTRACTS,
    GRAY_SAMPLE_DAILY_STARTED_ATTEMPT_QUOTA,
    GRAY_SAMPLE_REQUESTER_ID,
    build_gray_sample_message_id,
    build_gray_sample_reason,
    gray_sample_issue_url,
    sample_contract_sha256,
    validate_gray_sample_automation_authorization,
)
from gateway.pnc_rca_kafka_contract import WorkflowEventPolicy, WorkflowTransition
from scripts import pnc_rca_gray_sample_initiator as initiator


NOW = datetime(2026, 7, 28, 5, 0, tzinfo=timezone.utc)
ORIGINATOR = "ou_owner_20260728"
RUNTIME_COMMIT = "a" * 40
RUNTIME_TREE = "b" * 40
RELEASE_ID = "gray-release-20260728-r7"


def _write_json(
    path: Path, value: dict, *, owner_only: bool = False, sidecar: bool = False
) -> str:
    raw = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
    path.write_bytes(raw)
    if owner_only:
        path.chmod(0o600)
    digest = hashlib.sha256(raw).hexdigest()
    if sidecar:
        sidecar_path = path.with_suffix(path.suffix + ".sha256")
        sidecar_path.write_text(f"{digest}  {path.name}\n", encoding="ascii")
        if owner_only:
            sidecar_path.chmod(0o600)
    return digest


def _authorization(path: Path, *, valid: bool = True) -> str:
    if not valid:
        value = {
            "schema_version": "pnc_rca_capture_window_go_v1",
            "decision": "GO_C_TOPIC_IDENTITY_CAPTURE_ONLY",
            "capture_window_go": True,
            "runtime_activation_authorized": False,
        }
    else:
        value = {
            "schema_version": GRAY_SAMPLE_AUTOMATION_AUTHORIZATION_SCHEMA_VERSION,
            "artifact_id": "gray-sample-automation-authorization-test",
            "status": "PREPARED_UNTIL_C_TOPIC_FIXTURE",
            "issued_at": "2026-07-28T04:00:00+00:00",
            "not_before": "2026-07-28T04:00:00+00:00",
            "expires_at": "2026-08-04T15:59:59+00:00",
            "decision": 52,
            "design_authority": {
                "path": DESIGN_AUTHORITY_PATH,
                "section": "12",
                "decision": 52,
                "sha256": DESIGN_AUTHORITY_SHA256,
            },
            "sample_sequence_authority": {
                "path": DESIGN_AUTHORITY_PATH,
                "section": "10",
                "decision": 45,
                "sha256": DESIGN_AUTHORITY_SHA256,
                "fixed_sample_ids": list(GRAY_SAMPLE_CONTRACTS),
                "tenth_sample": "NATURAL",
                "s01_policy": "encounter_only_not_preselected",
            },
            "release_id": RELEASE_ID,
            "requester_id": GRAY_SAMPLE_REQUESTER_ID,
            "originator_identity_source": {
                "fixture_schema_version": C_TOPIC_FIXTURE_SCHEMA_VERSION,
                "canary_id": "C-TOPIC",
                "field": "originator_identity",
                "chat_id": C_TOPIC_CHAT_ID,
                "official_readback_required": True,
            },
            "lane": "production",
            "activation_required": True,
            "allowed_activation_states": ["steady_active"],
            "daily_started_attempt_quota": GRAY_SAMPLE_DAILY_STARTED_ATTEMPT_QUOTA,
            "allowed_sample_ids": list(GRAY_SAMPLE_CONTRACTS),
            "sample_contract_sha256s": {
                sample_id: sample_contract_sha256(sample_id)
                for sample_id in GRAY_SAMPLE_CONTRACTS
            },
            "comment_budget": COMMENT_BUDGET,
            "external_write_allowed": True,
            "owner_authorized": True,
            "production_actions": {
                "feishu_messages": 0,
                "kafka_offsets_committed": 0,
                "production_db_mutations": 0,
                "service_actions": 0,
            },
        }
    return _write_json(path, value, owner_only=True, sidecar=True)


def _fixture(
    path: Path,
    *,
    authorization_sha256: str,
    originator: str = ORIGINATOR,
    mention_verified: bool = True,
) -> str:
    value = {
        "schema_version": C_TOPIC_FIXTURE_SCHEMA_VERSION,
        "canary_id": "C-TOPIC",
        "status": "GREEN",
        "chat_id": C_TOPIC_CHAT_ID,
        "issue_id": C_TOPIC_ISSUE_ID,
        "exact_text": C_TOPIC_TEXT,
        "originator_identity": originator,
        "message_id": "om_c_topic_message_20260728",
        "topic_id": "topic:om_c_topic_message_20260728",
        "mention_entity": {
            "source": "feishu_message_entity",
            "target_open_id": "ou_rca_assistant_bot",
            "display_name": "胡子豪的小助手",
        },
        "mention_entity_verified": mention_verified,
        "official_readback": {
            "source": "feishu_official_api",
            "chat_id": C_TOPIC_CHAT_ID,
            "originator_identity": originator,
            "message_id": "om_c_topic_message_20260728",
            "topic_id": "topic:om_c_topic_message_20260728",
            "exact_text": C_TOPIC_TEXT,
            "mention_target_open_id": "ou_rca_assistant_bot",
            "mention_entity_verified": mention_verified,
            "read_at": NOW.isoformat(),
        },
        "authorization_sha256": authorization_sha256,
        "recorded_at": NOW.isoformat(),
    }
    return _write_json(path, value, owner_only=True)


def _policy() -> WorkflowEventPolicy:
    return WorkflowEventPolicy(
        topic="feishu-project-workflow-event",
        policy_version="gray-sample-test-v1",
        project_keys=frozenset({"project-key"}),
        project_simple_names=frozenset({"t03o4q"}),
        work_item_type_keys=frozenset({"issue"}),
        status_change_types=frozenset({"Reached"}),
        transitions=(
            WorkflowTransition(
                state_key="new-problem-state",
                pre_status=1,
                cur_status=2,
            ),
        ),
    )


def _seed_policy(store: RcaControlStore) -> None:
    store.persist_raw(
        KafkaRecord(
            topic="feishu-project-workflow-event",
            partition=0,
            offset=1,
            value=b"{}",
            key=b"test",
            timestamp_ms=1785214800000,
            headers=(),
        ),
        policy=_policy(),
        submit_enabled=True,
    )
    conn = store._connect()
    try:
        conn.execute(
            "UPDATE kafka_inbox SET decision='ignored', reason='focused_test_policy_seed'"
        )
        conn.commit()
    finally:
        conn.close()


def _human_request(sample_id: str) -> ManualRcaTriggerRequest:
    return ManualRcaTriggerRequest(
        schema_version=MANUAL_TRIGGER_SCHEMA_VERSION,
        issue_url=gray_sample_issue_url(sample_id),
        mode="run_or_join",
        reason="seed_terminal_generation",
        platform="feishu",
        chat_id="oc_seed",
        thread_id=f"topic:om_seed_{sample_id.lower()}",
        message_id=f"om_seed_{sample_id.lower()}",
        requester_id="ou_seed_operator",
    )


def _settle_delivery(store: RcaControlStore, submission_key: str) -> None:
    delivery = RcaDeliveryStore(store.db_path)
    delivery.materialize_pending_subscriptions()
    conn = delivery._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("UPDATE rca_delivery_effects SET status='succeeded'")
        job = conn.execute(
            "SELECT delivery_id FROM rca_delivery_jobs WHERE submission_key=?",
            (submission_key,),
        ).fetchone()
        assert job is not None
        delivery._aggregate_job_status(conn, str(job["delivery_id"]), NOW.isoformat())
        conn.commit()
    finally:
        conn.close()


def _terminalize(store: RcaControlStore, submission_key: str) -> None:
    conn = store._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """
            UPDATE rca_outbox SET status='quarantined', quarantined_at=?,
                   last_error_code='permanent_failure'
             WHERE submission_key=?
            """,
            (NOW.isoformat(), submission_key),
        )
        conn.execute(
            "UPDATE business_triggers SET state='quarantined' WHERE submission_key=?",
            (submission_key,),
        )
        conn.commit()
    finally:
        conn.close()
    delivery = RcaDeliveryStore(store.db_path)
    assert delivery.backfill_completed_submissions() == 1
    _settle_delivery(store, submission_key)


def _seed_terminal(store: RcaControlStore, sample_id: str) -> None:
    first = store.admit_manual_trigger(
        _human_request(sample_id),
        allowed_chat_ids={"oc_seed"},
        submit_enabled=True,
    )
    _terminalize(store, first.submission_key)


def _steady_epoch(store: RcaControlStore) -> str:
    epoch_id = "rca-gray-sample-test"
    store.create_activation_epoch(
        epoch_id=epoch_id,
        preauthorization_fingerprint="1" * 64,
        preauthorization_gate_receipt_sha256="2" * 64,
        preauthorization_capsule_sha256="3" * 64,
        config_sha256="4" * 64,
        db_logical_identity={
            "device": 7,
            "inode": 11,
            "logical_store_id": "gray-sample-test",
        },
        partition_start_fence={"feishu-project-workflow-event": {"0": 1}},
        operator="test",
        reason="focused gray sample test",
    )
    conn = store._connect()
    try:
        conn.execute(
            """
            UPDATE rca_activation_epochs
               SET state='steady_active',
                   preproduction_fingerprint=?,
                   preproduction_gate_receipt_sha256=?,
                   preproduction_capsule_sha256=?,
                   bounded_activated_at=?, confirmed_at=?, steady_activated_at=?
             WHERE epoch_id=?
            """,
            (
                "5" * 64,
                "6" * 64,
                "7" * 64,
                NOW.isoformat(),
                NOW.isoformat(),
                NOW.isoformat(),
                epoch_id,
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return epoch_id


def _counts(store: RcaControlStore) -> dict[str, int]:
    return {
        table: len(store.list_rows(table))
        for table in (
            "rca_trigger_sources",
            "business_triggers",
            "rca_outbox",
            "rca_delivery_subscriptions",
        )
    }


def _args(
    tmp_path: Path,
    *,
    sample_ids: list[str],
    originator: str = ORIGINATOR,
    fixture_originator: str | None = None,
    valid_authorization: bool = True,
    fake_authorization_sha: bool = False,
) -> tuple[argparse.Namespace, RcaControlStore]:
    db_path = tmp_path / "control.sqlite3"
    store = RcaControlStore(db_path)
    _seed_policy(store)
    for sample_id in sample_ids:
        _seed_terminal(store, sample_id)
    authorization_path = tmp_path / "automation-authorization.json"
    authorization_sha = _authorization(authorization_path, valid=valid_authorization)
    fixture_path = tmp_path / "c-topic-fixture.json"
    _fixture(
        fixture_path,
        authorization_sha256=(
            "f" * 64 if fake_authorization_sha else authorization_sha
        ),
        originator=fixture_originator or originator,
    )
    return (
        argparse.Namespace(
            control_db=str(db_path),
            originator_fixture=str(fixture_path),
            authorization=str(authorization_path),
            originator_identity=originator,
            receipt_dir=str(tmp_path / "receipts"),
            release_id=RELEASE_ID,
            expected_runtime_commit=RUNTIME_COMMIT,
            expected_runtime_tree=RUNTIME_TREE,
            sample_id=sample_ids,
            apply=True,
        ),
        store,
    )


def _patch_runtime(monkeypatch) -> None:
    monkeypatch.setattr(
        initiator, "_runtime_identity", lambda: (RUNTIME_COMMIT, RUNTIME_TREE)
    )


def test_admits_terminal_sample_and_binds_automation_to_owner_fixture(
    tmp_path, monkeypatch
):
    _patch_runtime(monkeypatch)
    args, store = _args(tmp_path, sample_ids=["S02"])
    epoch_id = _steady_epoch(store)
    before = _counts(store)

    result = initiator.run(args, now=NOW)

    assert result["ok"] is True
    assert result["results"][0]["status"] == "admitted"
    assert _counts(store)["business_triggers"] == before["business_triggers"] + 1
    receipt = json.loads((tmp_path / "receipts/S02.json").read_text())
    assert receipt["lane"] == "production"
    assert receipt["activation_required"] is True
    assert receipt["identity"]["requester_id"] == GRAY_SAMPLE_REQUESTER_ID
    assert receipt["identity"]["originator_identity"] == ORIGINATOR
    assert receipt["sample"]["expected_confidence_tier"] == "medium"
    assert receipt["sample"]["comment_budget"]["explicit_rerun_new_comment_max"] == 1
    assert (
        receipt["sample"]["comment_budget"]["infrastructure_redelivery_new_comment_max"]
        == 0
    )
    assert receipt["admission"]["generation"] == 2
    assert receipt["admission"]["audit"]["activation_epoch_id"] == epoch_id
    assert receipt["admission"]["audit"]["activation_decision"] == "admit"

    rerun = initiator.run(args, now=NOW)
    assert rerun["results"][0]["status"] == "existing_receipt_verified"
    assert _counts(store)["business_triggers"] == before["business_triggers"] + 1


def test_missing_epoch_exits_before_any_business_row_is_added(tmp_path, monkeypatch):
    _patch_runtime(monkeypatch)
    args, store = _args(tmp_path, sample_ids=["S02"])
    before = _counts(store)

    with pytest.raises(
        initiator.GraySampleInitiatorError,
        match="gray_sample_activation_epoch_missing",
    ):
        initiator.run(args, now=NOW)

    assert _counts(store) == before
    assert not (tmp_path / "receipts").exists()


@pytest.mark.parametrize(
    ("fixture_originator", "valid_authorization", "fake_authorization_sha", "error"),
    (
        (
            "ou_different_owner",
            True,
            False,
            "gray_sample_fixture_identity_mismatch",
        ),
        (
            None,
            False,
            False,
            "gray_sample_authorization_schema_invalid",
        ),
        (
            None,
            True,
            True,
            "gray_sample_fixture_authorization_sha256_mismatch",
        ),
    ),
)
def test_fixture_or_authorization_mismatch_is_fail_closed(
    tmp_path,
    monkeypatch,
    fixture_originator,
    valid_authorization,
    fake_authorization_sha,
    error,
):
    _patch_runtime(monkeypatch)
    args, store = _args(
        tmp_path,
        sample_ids=["S02"],
        fixture_originator=fixture_originator,
        valid_authorization=valid_authorization,
        fake_authorization_sha=fake_authorization_sha,
    )
    before = _counts(store)

    with pytest.raises(initiator.GraySampleInitiatorError, match=error):
        initiator.run(args, now=NOW)

    assert _counts(store) == before


def test_core_without_narrow_automation_authority_keeps_original_rejection(
    tmp_path, monkeypatch
):
    _patch_runtime(monkeypatch)
    args, store = _args(tmp_path, sample_ids=["S02"])
    _steady_epoch(store)
    authorization = json.loads(Path(args.authorization).read_text())
    fixture_sha = hashlib.sha256(Path(args.originator_fixture).read_bytes()).hexdigest()
    authority = initiator._automation_authority(
        release_id=RELEASE_ID,
        sample_id="S02",
        originator_identity=ORIGINATOR,
        fixture_sha256=fixture_sha,
        authorization_sha256=hashlib.sha256(
            Path(args.authorization).read_bytes()
        ).hexdigest(),
    )
    request = initiator._request(authority)
    before = _counts(store)
    assert authorization["decision"] == 52
    assert authorization["sample_sequence_authority"]["decision"] == 45

    with pytest.raises(
        ManualRcaAdmissionError,
        match="manual_generation_requires_explicit_user_rerun",
    ):
        store.admit_manual_trigger(
            request,
            allowed_chat_ids=set(),
            submit_enabled=True,
            operator_authorized=True,
            activation_required=True,
            now=NOW,
        )

    assert _counts(store) == before


def test_core_valid_authority_without_epoch_is_rejected_with_zero_new_rows(
    tmp_path, monkeypatch
):
    _patch_runtime(monkeypatch)
    args, store = _args(tmp_path, sample_ids=["S02"])
    fixture_sha = hashlib.sha256(Path(args.originator_fixture).read_bytes()).hexdigest()
    authorization_sha = hashlib.sha256(
        Path(args.authorization).read_bytes()
    ).hexdigest()
    authority = initiator._automation_authority(
        release_id=RELEASE_ID,
        sample_id="S02",
        originator_identity=ORIGINATOR,
        fixture_sha256=fixture_sha,
        authorization_sha256=authorization_sha,
    )
    before = _counts(store)

    with pytest.raises(
        ManualRcaAdmissionError,
        match="activation_epoch_rejected_unconfigured",
    ):
        store.admit_manual_trigger(
            initiator._request(authority),
            allowed_chat_ids=set(),
            submit_enabled=True,
            operator_authorized=True,
            activation_required=True,
            automation_authority=authority,
            now=NOW,
        )

    assert _counts(store) == before


def test_core_source_payload_binds_originator_authority(tmp_path, monkeypatch):
    _patch_runtime(monkeypatch)
    args, store = _args(tmp_path, sample_ids=["S02"])
    _steady_epoch(store)
    initiator.run(args, now=NOW)
    before = _counts(store)
    fixture_sha = hashlib.sha256(Path(args.originator_fixture).read_bytes()).hexdigest()
    authorization_sha = hashlib.sha256(
        Path(args.authorization).read_bytes()
    ).hexdigest()
    changed = initiator._automation_authority(
        release_id=RELEASE_ID,
        sample_id="S02",
        originator_identity="ou_different_owner",
        fixture_sha256=fixture_sha,
        authorization_sha256=authorization_sha,
    )

    with pytest.raises(ManualRcaAdmissionError, match="manual_source_payload_conflict"):
        store.admit_manual_trigger(
            initiator._request(changed),
            allowed_chat_ids=set(),
            submit_enabled=True,
            operator_authorized=True,
            activation_required=True,
            automation_authority=changed,
            now=NOW,
        )

    assert _counts(store) == before


def test_daily_started_attempts_are_unlimited(tmp_path, monkeypatch):
    _patch_runtime(monkeypatch)
    args, store = _args(tmp_path, sample_ids=["S07"])
    _steady_epoch(store)
    conn = store._connect()
    try:
        for index in range(5):
            conn.execute(
                """
                INSERT INTO rca_trigger_sources(
                    source_id, source_kind, source_dedupe_key, payload_sha256,
                    platform, message_id, requester_id, mode, outcome, created_at
                ) VALUES (?, 'feishu_group_manual', ?, ?, 'operator', ?, ?,
                          'rerun', 'created', ?)
                """,
                (
                    f"quota-source-{index}",
                    f"operator:quota-{index}",
                    f"{index + 1:064x}",
                    f"quota-{index}",
                    GRAY_SAMPLE_REQUESTER_ID,
                    NOW.isoformat(),
                ),
            )
        conn.commit()
    finally:
        conn.close()
    before = _counts(store)

    result = initiator.run(args, now=NOW)

    assert result["ok"] is True
    assert result["results"][0]["status"] == "admitted"
    after = _counts(store)
    assert after["rca_trigger_sources"] == before["rca_trigger_sources"] + 1
    assert after["business_triggers"] == before["business_triggers"] + 1
    status = json.loads((tmp_path / "receipts/status.json").read_text())
    assert status["daily_started_attempt_quota"] is None


def test_one_run_can_select_the_full_fixed_regression_set():
    selected = list(GRAY_SAMPLE_CONTRACTS)

    assert initiator._selected_samples(selected) == selected


@pytest.mark.parametrize(
    ("schema_version", "quota"),
    (
        ("pnc_rca_gray_sample_automation_authorization_v1", 5),
        (GRAY_SAMPLE_AUTOMATION_AUTHORIZATION_SCHEMA_VERSION, 5),
    ),
)
def test_bounded_authorization_cannot_expand_to_unlimited(
    tmp_path, schema_version, quota
):
    path = tmp_path / "authorization.json"
    _authorization(path)
    value = json.loads(path.read_text(encoding="utf-8"))
    value["schema_version"] = schema_version
    value["daily_started_attempt_quota"] = quota

    with pytest.raises(ValueError, match="gray_sample_authorization_contract_invalid"):
        validate_gray_sample_automation_authorization(
            value,
            expected_release_id=RELEASE_ID,
            now=NOW,
        )


def test_authority_contract_rejects_wrong_fixed_sample_hash():
    with pytest.raises(ValueError, match="gray_sample_contract_sha256_mismatch"):
        initiator.normalize_gray_sample_automation_authority({
            "schema_version": GRAY_SAMPLE_AUTOMATION_AUTHORITY_SCHEMA_VERSION,
            "release_id": RELEASE_ID,
            "sample_id": "S02",
            "originator_identity": ORIGINATOR,
            "originator_fixture_sha256": "1" * 64,
            "authorization_sha256": "2" * 64,
            "sample_contract_sha256": "3" * 64,
        })
    assert sample_contract_sha256("S02") != "3" * 64
    assert build_gray_sample_message_id(
        initiator._automation_authority(
            release_id=RELEASE_ID,
            sample_id="S02",
            originator_identity=ORIGINATOR,
            fixture_sha256="1" * 64,
            authorization_sha256="2" * 64,
        )
    ).startswith("gray-sample-s02-")
    assert build_gray_sample_reason(
        initiator._automation_authority(
            release_id=RELEASE_ID,
            sample_id="S02",
            originator_identity=ORIGINATOR,
            fixture_sha256="1" * 64,
            authorization_sha256="2" * 64,
        )
    ).startswith("production_gray_sample:S02:")


def test_authorization_explicitly_binds_decision_45_sample_sequence(tmp_path):
    path = tmp_path / "authorization.json"
    _authorization(path)
    value = json.loads(path.read_text(encoding="utf-8"))
    value["sample_sequence_authority"]["fixed_sample_ids"] = [
        "S01",
        *GRAY_SAMPLE_CONTRACTS,
    ]

    with pytest.raises(
        ValueError,
        match="gray_sample_authorization_sequence_binding_invalid",
    ):
        validate_gray_sample_automation_authorization(
            value,
            expected_release_id=RELEASE_ID,
            now=NOW,
        )
