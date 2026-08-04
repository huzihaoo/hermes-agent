import asyncio
from collections import namedtuple
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from gateway.pnc_rca_control_store import (
    ActivationIngressDeferredError,
    KafkaRecord,
    MANUAL_TRIGGER_SCHEMA_VERSION,
    ManualRcaTriggerRequest,
    RecordProcessingBlockedError,
    RcaControlStore,
)
from gateway.pnc_rca_kafka_contract import WorkflowEventPolicy, WorkflowTransition
from gateway.pnc_rca_write_fence import ExternalWriteFenceError
from scripts import pnc_rca_kafka_consumer as consumer_module


TOPIC = "feishu-project-workflow-event"


def _env(tmp_path):
    return {
        "HERMES_RCA_KAFKA_BOOTSTRAP_SERVERS": "broker-1:9092,broker-2:9092",
        "HERMES_RCA_KAFKA_TOPIC": TOPIC,
        "HERMES_RCA_KAFKA_USER": "rca",
        "HERMES_RCA_KAFKA_PASSWORD": "top-secret-password",
        "HERMES_RCA_KAFKA_GROUP": "rca_root_cause_analysis_agent",
        "HERMES_RCA_KAFKA_API_VERSION": "3.9.0",
        "HERMES_RCA_KAFKA_PROJECT_KEYS": "project-key",
        "HERMES_RCA_KAFKA_PROJECT_SIMPLE_NAMES": "g1q3",
        "HERMES_RCA_KAFKA_WORK_ITEM_TYPE_KEYS": "problem-type",
        "HERMES_RCA_KAFKA_STATUS_CHANGE_TYPES": "Reached",
        "HERMES_RCA_KAFKA_CREATION_RULE_VERSION": "issue-created-v1",
        "HERMES_RCA_KAFKA_SNAPSHOT_PATTERNS": "",
        "HERMES_RCA_KAFKA_SNAPSHOT_SUB_STAGES": "",
        "HERMES_RCA_KAFKA_SUBMIT_ENABLED": "false",
        "HERMES_RCA_KAFKA_ACTIVATION_REQUIRED": "false",
        "HERMES_RCA_KAFKA_STATE_TRANSITIONS_JSON": json.dumps([
            {
                "state_key": "new-problem-state",
                "node_name": "diagnostic only",
                "pre_status": 1,
                "cur_status": 2,
            }
        ]),
        "HERMES_RCA_KAFKA_CONTROL_DB_PATH": str(tmp_path / "control.sqlite3"),
        "HERMES_RCA_KAFKA_HEALTH_PATH": str(tmp_path / "health.json"),
    }


def _config(tmp_path, **updates):
    env = _env(tmp_path)
    env.update(updates)
    return consumer_module.ConsumerConfig.from_env(env, hermes_home=tmp_path)


def _value(*, work_item_id=7041712812):
    return json.dumps({
        "id": work_item_id,
        "name": "ACC braking issue",
        "nodes": [
            {
                "state_key": "new-problem-state",
                "node_name": "renamed display label",
                "pre_status": 1,
                "cur_status": 2,
            }
        ],
        "project_key": "project-key",
        "project_simple_name": "g1q3",
        "status_change_type": "Reached",
        "updated_at": 1783650000000,
        "work_item_type_key": "problem-type",
    }).encode()


def _message(offset=10, value=None):
    return SimpleNamespace(
        topic=TOPIC,
        partition=0,
        offset=offset,
        value=value if value is not None else _value(),
        key=None,
        timestamp=1783650000000,
        headers=[],
    )


def _exact_recovery_request(
    path,
    *,
    config,
    store,
    message,
    expected,
    raw_sha256=None,
):
    epoch = store.activation_epoch()
    assert epoch is not None
    now = datetime.now(timezone.utc)
    body = {
        "schema_version": consumer_module.EXACT_RECOVERY_REQUEST_SCHEMA_VERSION,
        "release_id": "rca-gray-test-20260722",
        "epoch_id": epoch["epoch_id"],
        "created_at": now.isoformat(),
        "expires_at": (now + timedelta(minutes=5)).isoformat(),
        "topic": message.topic,
        "partition": message.partition,
        "offset": message.offset,
        "event_uid": f"{message.topic}:{message.partition}:{message.offset}",
        "raw_sha256": raw_sha256 or hashlib.sha256(message.value).hexdigest(),
        "project_key": "project-key",
        "work_item_type_key": "problem-type",
        "work_item_id": "7041712812",
        "business_key": expected.business_key,
        "submission_key": expected.submission_key,
        "generation": expected.generation,
        "final_validation_sha256": "9" * 64,
        "nonce": "resident-exact-recovery-test-nonce",
    }
    body["request_sha256"] = hashlib.sha256(
        consumer_module._canonical_bytes(body)
    ).hexdigest()
    path.write_bytes(consumer_module._canonical_bytes(body) + b"\n")
    os.chmod(path, 0o600)
    return body


def _natural_canary_gate(path, *, config, store, minimum_offset=21):
    exact_receipt_path = consumer_module._exact_recovery_receipt_path(
        config.exact_recovery_request_path
    )
    exact_receipt_path.write_bytes(b"{}\n")
    os.chmod(exact_receipt_path, 0o600)
    now = datetime.now(timezone.utc)
    epoch = store.activation_epoch()
    assert epoch is not None
    body = {
        "schema_version": consumer_module.NATURAL_CANARY_GATE_SCHEMA_VERSION,
        "release_id": "rca-gray-test-20260722",
        "epoch_id": epoch["epoch_id"],
        "created_at": now.isoformat(),
        "expires_at": (now + timedelta(minutes=5)).isoformat(),
        "exact_readback_sha256": "8" * 64,
        "exact_recovery_receipt_sha256": hashlib.sha256(
            exact_receipt_path.read_bytes()
        ).hexdigest(),
        "minimum_offset": minimum_offset,
    }
    body["request_sha256"] = hashlib.sha256(
        consumer_module._canonical_bytes(body)
    ).hexdigest()
    path.write_bytes(consumer_module._canonical_bytes(body) + b"\n")
    os.chmod(path, 0o600)
    return body


def _activation_manual_identity(message_id, *, mode="run_or_join", issue_id=7041712813):
    return {
        "chat_id": "oc_activation_test",
        "thread_id": "topic:om_consumer_activation_root",
        "requester_id": "ou_activation_test",
        "message_id": message_id,
        "issue_url": f"https://project.feishu.cn/g1q3/issue/detail/{issue_id}",
        "mode": mode,
    }


def _prepare_activation_epoch(
    store,
    *,
    kafka_offset=20,
    bounded=False,
    partition_start_fence=None,
):
    epoch_id = "rca-consumer-activation-20260712"
    created = store.create_activation_epoch(
        epoch_id=epoch_id,
        preauthorization_fingerprint="1" * 64,
        preauthorization_gate_receipt_sha256="3" * 64,
        preauthorization_capsule_sha256="4" * 64,
        config_sha256="2" * 64,
        db_logical_identity={
            "device": 7,
            "inode": 11,
            "logical_store_id": "consumer-test-control",
        },
        partition_start_fence=(
            partition_start_fence
            if partition_start_fence is not None
            else {TOPIC: {"0": kafka_offset}}
        ),
        operator="consumer-test",
        reason="exercise Kafka activation wiring",
    )
    store.preauthorize_activation_epoch(
        epoch_id=epoch_id,
        preproduction_fingerprint="5" * 64,
        preproduction_gate_receipt_sha256="6" * 64,
        preproduction_capsule_sha256="7" * 64,
        expected_preauthorization_fingerprint="1" * 64,
        expected_preauthorization_gate_receipt_sha256="3" * 64,
        expected_preauthorization_capsule_sha256="4" * 64,
        expected_config_sha256=created["config_sha256"],
        expected_db_logical_identity_sha256=created[
            "db_logical_identity_sha256"
        ],
        expected_partition_start_fence_sha256=created[
            "partition_start_fence_sha256"
        ],
        operator="consumer-test",
        reason="bind exact preproduction capsule for Kafka activation wiring",
    )
    identities = {
        "kafka_success": (
            "kafka",
            {"event_uid": f"{TOPIC}:0:{kafka_offset}"},
        ),
        "manual_success": (
            "manual",
            _activation_manual_identity("om_consumer_manual_success"),
        ),
        "manual_terminal_failure": (
            "manual",
            _activation_manual_identity(
                "om_consumer_manual_terminal",
                mode="debug",
                issue_id=7041712814,
            ),
        ),
    }
    for slot_kind in ("manual_success", "manual_terminal_failure"):
        source_kind, source_identity = identities[slot_kind]
        store.authorize_activation_slot(
            epoch_id=epoch_id,
            slot_kind=slot_kind,
            source_kind=source_kind,
            source_identity=source_identity,
            operator="consumer-test",
            reason=f"authorize exact {slot_kind} consumer canary",
        )
    if bounded:
        store.transition_activation_epoch(
            epoch_id=epoch_id,
            expected_state="preauthorized",
            target_state="bounded_active",
            operator="consumer-test",
            reason="open exact bounded manual canaries",
        )
    return epoch_id, identities


def _prepare_ready_bounded_activation(
    store,
    *,
    kafka_offset=20,
    partition_start_fence=None,
):
    epoch_id, identities = _prepare_activation_epoch(
        store,
        kafka_offset=kafka_offset,
        bounded=True,
        partition_start_fence=partition_start_fence,
    )
    policy = WorkflowEventPolicy(
        topic=TOPIC,
        policy_version="issue-created-v1",
        project_keys=frozenset({"project-key"}),
        project_simple_names=frozenset({"g1q3"}),
        work_item_type_keys=frozenset({"problem-type"}),
        status_change_types=frozenset({"Reached"}),
        transitions=(
            WorkflowTransition(
                state_key="new-problem-state", pre_status=1, cur_status=2
            ),
        ),
    )
    for slot_kind in ("manual_success", "manual_terminal_failure"):
        identity = identities[slot_kind][1]
        store.admit_manual_trigger(
            ManualRcaTriggerRequest(
                schema_version=MANUAL_TRIGGER_SCHEMA_VERSION,
                issue_url=identity["issue_url"],
                mode=identity["mode"],
                reason="consumer activation canary",
                platform="feishu",
                chat_id=identity["chat_id"],
                thread_id=identity["thread_id"],
                message_id=identity["message_id"],
                requester_id=identity["requester_id"],
            ),
            allowed_chat_ids={"oc_activation_test"},
            submit_enabled=True,
            operator_authorized=slot_kind == "manual_terminal_failure",
            active_policy=policy,
            activation_required=True,
        )
    for index in range(2):
        claim = store.claim_outbox(
            lease_owner=f"consumer-activation-{index}",
            activation_required=True,
        )
        assert claim is not None
        store.complete_outbox(
            outbox_id=claim.outbox_id,
            lease_token=claim.lease_token,
            result={"outcome": "consumer_activation_canary_recorded"},
        )
    readiness = store.health()["activation"]["ingress_freeze_readiness"]
    assert readiness["ready"] is True
    return epoch_id


def _activation_confirmation_bindings(store, *, kafka_offset=20):
    bounded = store.activation_epoch()
    assert bounded is not None
    end_fence = {TOPIC: {"0": kafka_offset + 1}}
    conn = store._connect()
    try:
        conn.execute("BEGIN")
        epoch = store._current_activation_epoch_tx(conn)
        assert epoch is not None
        release_binding_sha256 = (
            store._validate_consumed_activation_executions_tx(
                conn,
                epoch=epoch,
                end_fence_json=store._normalize_partition_fence(end_fence),
            )
        )
    finally:
        if conn.in_transaction:
            conn.rollback()
        conn.close()
    return {
        "partition_end_fence": end_fence,
        "expected_config_sha256": bounded["config_sha256"],
        "expected_db_logical_identity_sha256": bounded[
            "db_logical_identity_sha256"
        ],
        "expected_partition_start_fence_sha256": bounded[
            "partition_start_fence_sha256"
        ],
        "expected_release_binding_sha256": release_binding_sha256,
    }


def _advance_activation_to_confirmed(store, *, kafka_offset=20):
    epoch_id = _prepare_ready_bounded_activation(
        store,
        kafka_offset=kafka_offset,
    )
    confirmation = _activation_confirmation_bindings(
        store,
        kafka_offset=kafka_offset,
    )
    store.transition_activation_epoch(
        epoch_id=epoch_id,
        expected_state="bounded_active",
        target_state="confirmed",
        production_fingerprint="3" * 64,
        production_gate_receipt_sha256="4" * 64,
        operator="consumer-test",
        reason="bind passing production receipt",
        **confirmation,
    )


def _advance_activation_to_steady(store, *, kafka_offset=20):
    _advance_activation_to_confirmed(store, kafka_offset=kafka_offset)
    store.transition_activation_epoch(
        epoch_id="rca-consumer-activation-20260712",
        expected_state="confirmed",
        target_state="steady_active",
        operator="consumer-test",
        reason="enter steady Kafka activation",
    )


class FakeConsumer:
    def __init__(self, batches):
        self.batches = list(batches)
        self.poll_calls = []
        self.commits = []

    def poll(self, **kwargs):
        self.poll_calls.append(kwargs)
        return self.batches.pop(0) if self.batches else {}

    def commit(self, **kwargs):
        self.commits.append(kwargs)


FreezeTopicPartition = namedtuple("FreezeTopicPartition", "topic partition")


class FreezeCapableConsumer(FakeConsumer):
    def __init__(
        self,
        batches,
        *,
        committed_offsets,
        applied_t0_offsets=None,
        initial_positions=None,
        on_poll=None,
    ):
        super().__init__(batches)
        self._assignment = {
            FreezeTopicPartition(TOPIC, int(partition))
            for partition in committed_offsets
        }
        self.committed_offsets = {
            FreezeTopicPartition(TOPIC, int(partition)): (
                None if offset is None else int(offset)
            )
            for partition, offset in committed_offsets.items()
        }
        self.applied_t0_offsets = {
            int(partition): int(offset)
            for partition, offset in (applied_t0_offsets or {}).items()
        }
        requested_positions = {
            int(partition): int(offset)
            for partition, offset in (initial_positions or {}).items()
        }
        self.positions = {}
        for partition, offset in self.committed_offsets.items():
            partition_id = int(partition.partition)
            if partition_id in requested_positions:
                position = requested_positions[partition_id]
            elif offset is not None:
                position = offset + 100
            elif partition_id in self.applied_t0_offsets:
                position = self.applied_t0_offsets[partition_id]
            else:
                raise ValueError("uncommitted test partition requires a position")
            self.positions[partition] = position
        self.paused_calls = []
        self.resumed_calls = []
        self.seek_calls = []
        self.on_poll = on_poll
        self.assignment_count = 1
        self.revocation_count = 0
        self.last_assignment_at = "2026-07-12T00:00:00+00:00"
        self._hermes_rca_initial_offset_listener = SimpleNamespace(
            diagnostics=self.diagnostics
        )

    def diagnostics(self):
        return {
            "assignment_count": self.assignment_count,
            "revocation_count": self.revocation_count,
            "callback_errors": 0,
            "assigned_partitions": sorted(
                partition.partition for partition in self._assignment
            ),
            "last_assignment_at": self.last_assignment_at,
            "applied_t0_offsets": {
                str(partition): offset
                for partition, offset in sorted(self.applied_t0_offsets.items())
            },
        }

    def assignment(self):
        return set(self._assignment)

    def pause(self, *partitions):
        self.paused_calls.append(tuple(partitions))

    def resume(self, *partitions):
        self.resumed_calls.append(tuple(partitions))

    def committed(self, partition, timeout_ms=None):
        assert timeout_ms is not None
        return self.committed_offsets.get(partition)

    def seek(self, partition, offset):
        self.seek_calls.append((partition, offset))
        self.positions[partition] = offset

    def position(self, partition, timeout_ms=None):
        assert timeout_ms is not None
        return self.positions.get(partition)

    def poll(self, **kwargs):
        batch = super().poll(**kwargs)
        if self.on_poll is not None:
            self.on_poll(self)
        return batch


class PollFailingConsumer(FakeConsumer):
    def poll(self, **kwargs):
        self.poll_calls.append(kwargs)
        raise RuntimeError("authentication failed with raw-secret-payload")


def test_config_is_shadow_by_default_and_has_no_idle_consumer_timeout(tmp_path):
    config = _config(tmp_path)
    kwargs = config.kafka_kwargs()

    assert config.topic == TOPIC
    assert config.submit_enabled is False
    assert config.activation_required is False
    assert config.request_timeout_ms == 120_000
    assert config.api_version == (3, 9, 0)
    assert config.auto_offset_reset == "none"
    assert config.isolation_level == "read_committed"
    assert config.outbox_high_watermark == 100
    assert config.outbox_resume_watermark == 50
    assert kwargs["enable_auto_commit"] is False
    assert kwargs["isolation_level"] == "read_committed"
    assert kwargs["max_partition_fetch_bytes"] == 2 * 1024 * 1024
    assert kwargs["fetch_max_bytes"] == 20 * 1024 * 1024
    assert kwargs["allow_auto_create_topics"] is False
    assert "consumer_timeout_ms" not in kwargs
    assert config.public_dict()["activation_required"] is False
    assert config.runtime_public_dict()["activation_required"] is False
    assert config.public_dict()["activation_ingress_freeze_mode"] == (
        "automatic_bounded_completion"
    )
    assert config.public_dict()["activation_ingress_freeze_restart_required"] is False


@pytest.mark.parametrize("value", ["1", "0", "yes", "on", "enabled", ""])
def test_activation_required_accepts_only_literal_boolean(tmp_path, value):
    with pytest.raises(ValueError, match="exactly true or false"):
        _config(tmp_path, HERMES_RCA_KAFKA_ACTIVATION_REQUIRED=value)


def test_activation_required_true_is_public_and_runtime_bound(tmp_path):
    config = _config(tmp_path, HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="true")

    assert config.activation_required is True
    assert config.public_dict()["activation_required"] is True
    assert config.runtime_public_dict()["activation_required"] is True
    assert "password" not in config.public_dict()
    assert "username" not in config.public_dict()
    assert "top-secret-password" not in repr(config)
    assert config.public_dict()["external_dispatch_wired"] is False
    assert config.public_dict()["allow_auto_create_topics"] is False


def test_consumer_env_loader_preserves_literal_expansion_syntax(tmp_path, monkeypatch):
    env_file = tmp_path / "consumer.env"
    env_file.write_text(
        "HERMES_RCA_KAFKA_PASSWORD=${AMBIENT_KAFKA_SECRET}\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("HERMES_RCA_KAFKA_PASSWORD", raising=False)
    monkeypatch.setenv("AMBIENT_KAFKA_SECRET", "must-not-expand")

    try:
        consumer_module.load_consumer_environment(env_file)
        assert os.environ["HERMES_RCA_KAFKA_PASSWORD"] == "${AMBIENT_KAFKA_SECRET}"
    finally:
        os.environ.pop("HERMES_RCA_KAFKA_PASSWORD", None)


@pytest.mark.parametrize(
    ("name", "value", "message"),
    [
        ("HERMES_RCA_KAFKA_USER", "legacy", "must be exactly rca"),
        ("HERMES_RCA_KAFKA_USER", "rca_", "must be exactly rca"),
        ("HERMES_RCA_KAFKA_USER", "rca_invalid principal", "must be exactly rca"),
        ("HERMES_RCA_KAFKA_USER", "rca_release_agent", "must be exactly rca"),
        ("HERMES_RCA_KAFKA_GROUP", "legacy", "must be exactly"),
        ("HERMES_RCA_KAFKA_CLIENT_ID", "legacy", "must be exactly"),
        ("HERMES_RCA_KAFKA_API_VERSION", "3.8.0", "must be exactly 3.9.0"),
        ("HERMES_RCA_KAFKA_REQUEST_TIMEOUT_MS", "119999", "must be exactly"),
    ],
)
def test_config_rejects_runtime_identity_and_protocol_drift(
    tmp_path, name, value, message
):
    with pytest.raises(ValueError, match=message):
        _config(tmp_path, **{name: value})


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("HERMES_RCA_KAFKA_SESSION_TIMEOUT_MS", "60001"),
        ("HERMES_RCA_KAFKA_MAX_POLL_INTERVAL_MS", "600001"),
        ("HERMES_RCA_KAFKA_POLL_TIMEOUT_MS", "5001"),
        ("HERMES_RCA_KAFKA_MAX_POLL_RECORDS", "11"),
        ("HERMES_RCA_KAFKA_OFFSET_LOOKUP_TIMEOUT_MS", "10001"),
        ("HERMES_RCA_KAFKA_OUTBOX_HIGH_WATERMARK", "1001"),
    ],
)
def test_config_rejects_unbounded_resource_controls(tmp_path, name, value):
    with pytest.raises(ValueError, match="must be at most"):
        _config(tmp_path, **{name: value})


def test_manual_and_consumer_share_exact_validated_workflow_policy(tmp_path):
    env = _env(tmp_path)

    standalone = consumer_module.workflow_policy_from_env(env)
    config = consumer_module.ConsumerConfig.from_env(env, hermes_home=tmp_path)

    assert standalone == config.policy
    assert standalone.to_dict() == config.runtime_public_dict()["policy"]


def test_snapshot_only_creation_policy_is_explicit_and_valid(tmp_path):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_STATUS_CHANGE_TYPES="",
        HERMES_RCA_KAFKA_STATE_TRANSITIONS_JSON="[]",
        HERMES_RCA_KAFKA_SNAPSHOT_PATTERNS="State",
        HERMES_RCA_KAFKA_SNAPSHOT_SUB_STAGES="OPEN",
    )

    assert config.policy.status_change_types == frozenset()
    assert config.policy.transitions == ()
    assert config.policy.snapshot_patterns == frozenset({"State"})
    assert config.policy.snapshot_sub_stages == frozenset({"OPEN"})


def test_offset_reset_is_fixed_fail_closed_and_rejects_broker_fallback(tmp_path):
    fail_closed = _config(tmp_path)

    assert fail_closed.kafka_kwargs()["auto_offset_reset"] == "none"
    for fallback in ("earliest", "latest"):
        with pytest.raises(ValueError, match="exactly none"):
            _config(tmp_path, HERMES_RCA_KAFKA_AUTO_OFFSET_RESET=fallback)


def test_explicit_t0_offsets_are_parsed_and_redacted_config_is_auditable(tmp_path):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_START_OFFSETS_JSON='{"2": 120, "0": 99}',
    )

    assert config.initial_offsets == ((0, 99), (2, 120))
    assert config.initial_offset_for(2) == 120
    assert config.initial_offset_for(1) is None
    assert config.public_dict()["initial_offsets"] == {"0": 99, "2": 120}
    assert config.public_dict()["initial_offset_policy"] == (
        "committed_else_explicit_t0_else_fail"
    )


@pytest.mark.parametrize(
    "value",
    ["[]", "{}", '{"x": 1}', '{"0": -1}', '{"0": true}', '{"0": 1.5}'],
)
def test_invalid_t0_offset_baseline_is_rejected(tmp_path, value):
    with pytest.raises(ValueError, match="START_OFFSETS_JSON"):
        _config(tmp_path, HERMES_RCA_KAFKA_START_OFFSETS_JSON=value)


@pytest.mark.parametrize(
    "value",
    ['{"0": 10, "0": 100}', '{"0": NaN}', '{"0": Infinity}'],
)
def test_t0_offset_baseline_rejects_ambiguous_json(tmp_path, value):
    with pytest.raises(ValueError, match="START_OFFSETS_JSON"):
        _config(tmp_path, HERMES_RCA_KAFKA_START_OFFSETS_JSON=value)


def test_transition_config_rejects_duplicate_keys_and_excessive_nesting(tmp_path):
    duplicate = (
        '[{"state_key":"start","state_key":"finish",'
        '"pre_status":"","cur_status":"start"}]'
    )
    deeply_nested = "[" * 40 + "]" * 40

    for value in (duplicate, deeply_nested):
        with pytest.raises(ValueError, match="STATE_TRANSITIONS_JSON"):
            _config(tmp_path, HERMES_RCA_KAFKA_STATE_TRANSITIONS_JSON=value)


def test_partition_assignment_prefers_committed_offsets_and_applies_t0_only_when_missing(
    tmp_path,
):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_START_OFFSETS_JSON='{"0": 99, "1": 120}',
    )
    topic_partition = namedtuple("TopicPartition", "topic partition")
    partition_0 = topic_partition(TOPIC, 0)
    partition_1 = topic_partition(TOPIC, 1)

    class AssignedConsumer:
        def __init__(self):
            self.seeks = []
            self.offset_fetches = []
            self._coordinator = self

        async def fetch_committed_offsets_async(self, partitions, *, timeout_ms):
            assert timeout_ms == 3_000
            self.offset_fetches.append(tuple(partitions))
            return {partition_0: SimpleNamespace(offset=100)}

        def seek(self, topic_partition, offset):
            self.seeks.append((topic_partition.partition, offset))

    consumer = AssignedConsumer()
    store = SimpleNamespace(
        partition_progress=lambda **_kwargs: {0: 100}
    )
    listener = consumer_module.ExplicitInitialOffsetListener(consumer, config, store)

    asyncio.run(listener.on_partitions_assigned([partition_0, partition_1]))

    assert consumer.seeks == [(1, 120)]
    assert consumer.offset_fetches == [(partition_0, partition_1)]
    assert listener.applied == {1: 120}
    assert listener.diagnostics()["assigned_partitions"] == [0, 1]
    assert listener.diagnostics()["assignment_count"] == 1
    assert listener.diagnostics()["callback_errors"] == 0


def test_partition_assignment_works_without_running_asyncio_loop(tmp_path):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_START_OFFSETS_JSON='{"0": 676}',
    )
    topic_partition = namedtuple("TopicPartition", "topic partition")
    partition = topic_partition(TOPIC, 0)

    class KafkaPythonDrivenConsumer:
        def __init__(self):
            self._coordinator = self
            self.seeks = []

        async def fetch_committed_offsets_async(self, _partitions, **_kwargs):
            return {partition: SimpleNamespace(offset=-1)}

        def seek(self, assigned_partition, offset):
            self.seeks.append((assigned_partition, offset))

    consumer = KafkaPythonDrivenConsumer()
    store = SimpleNamespace(partition_progress=lambda **_kwargs: {})
    listener = consumer_module.ExplicitInitialOffsetListener(consumer, config, store)

    callback = listener.on_partitions_assigned([partition])
    with pytest.raises(StopIteration):
        callback.send(None)

    assert consumer.seeks == [(partition, 676)]
    assert listener.diagnostics()["assigned_partitions"] == [0]
    assert listener.diagnostics()["callback_errors"] == 0


def test_partition_assignment_fails_closed_when_group_and_t0_offset_are_missing(
    tmp_path,
):
    config = _config(tmp_path)
    topic_partition = namedtuple("TopicPartition", "topic partition")
    partition = topic_partition(TOPIC, 3)
    seeks = []

    class MissingOffsetConsumer:
        def __init__(self):
            self._coordinator = self

        async def fetch_committed_offsets_async(self, _partitions, **_kwargs):
            return {}

        def seek(self, *args):
            seeks.append(args)

    consumer = MissingOffsetConsumer()
    store = SimpleNamespace(partition_progress=lambda **_kwargs: {})
    listener = consumer_module.ExplicitInitialOffsetListener(consumer, config, store)

    with pytest.raises(RuntimeError, match="initial_offset_missing_for_partitions:3"):
        asyncio.run(listener.on_partitions_assigned([partition]))
    assert seeks == []
    assert listener.diagnostics()["callback_errors"] == 1
    assert listener.diagnostics()["assigned_partitions"] == []


@pytest.mark.parametrize(
    "local_progress",
    [{}, {0: 109}],
)
def test_partition_assignment_blocks_broker_offset_ahead_of_local_durability(
    tmp_path, local_progress
):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_START_OFFSETS_JSON='{"0": 99}',
    )
    topic_partition = namedtuple("TopicPartition", "topic partition")
    partition = topic_partition(TOPIC, 0)
    seeks = []

    class IncoherentConsumer:
        def __init__(self):
            self._coordinator = self

        async def fetch_committed_offsets_async(self, _partitions, **_kwargs):
            return {partition: SimpleNamespace(offset=110)}

        def seek(self, *args):
            seeks.append(args)

    listener = consumer_module.ExplicitInitialOffsetListener(
        IncoherentConsumer(),
        config,
        SimpleNamespace(
            partition_progress=lambda **_kwargs: dict(local_progress)
        ),
    )

    with pytest.raises(RuntimeError, match="broker_local_offset_incoherent:0"):
        asyncio.run(listener.on_partitions_assigned([partition]))
    assert seeks == []


def test_create_consumer_registers_a_supported_rebalance_listener(monkeypatch, tmp_path):
    config = _config(tmp_path)
    RcaControlStore(config.control_db_path)

    class FakeAsyncListenerBase:
        async def on_partitions_revoked(self, revoked):
            raise NotImplementedError

        async def on_partitions_assigned(self, assigned):
            raise NotImplementedError

    class FakeKafkaConsumer:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.subscription = None

        def subscribe(self, *, topics, listener):
            assert isinstance(listener, FakeAsyncListenerBase)
            self.subscription = (topics, listener)

    fake_modules = {
        "kafka": SimpleNamespace(
            KafkaConsumer=FakeKafkaConsumer,
            AsyncConsumerRebalanceListener=FakeAsyncListenerBase,
        ),
    }
    monkeypatch.setattr(
        consumer_module.importlib,
        "import_module",
        lambda name: fake_modules[name],
    )

    consumer = consumer_module.create_consumer(config)

    topics, listener = consumer.subscription
    assert topics == (TOPIC,)
    assert isinstance(listener, consumer_module.ExplicitInitialOffsetListener)
    assert isinstance(listener, FakeAsyncListenerBase)
    assert asyncio.iscoroutinefunction(listener.on_partitions_assigned)
    assert consumer.kwargs["sasl_plain_username"] == "rca"
    assert consumer.kwargs["group_id"] == "rca_root_cause_analysis_agent"
    assert consumer.kwargs["client_id"] == "root_cause_analysis_agent"


def test_create_consumer_checks_materialized_store_before_kafka_client(
    monkeypatch,
    tmp_path,
):
    config = _config(tmp_path)
    client_created = False

    class UnexpectedKafkaConsumer:
        def __init__(self, **_kwargs):
            nonlocal client_created
            client_created = True

    fake_kafka = SimpleNamespace(
        KafkaConsumer=UnexpectedKafkaConsumer,
        AsyncConsumerRebalanceListener=object,
    )
    monkeypatch.setattr(
        consumer_module.importlib,
        "import_module",
        lambda name: fake_kafka if name == "kafka" else None,
    )

    with pytest.raises(RuntimeError, match="rca_control_store_existing_path_missing"):
        consumer_module.create_consumer(config)

    assert client_created is False
    assert config.control_db_path.exists() is False


def test_create_consumer_rejects_preauthorized_epoch_before_kafka_client(
    monkeypatch,
    tmp_path,
):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_SUBMIT_ENABLED="true",
        HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="true",
    )
    store = RcaControlStore(config.control_db_path)
    _prepare_activation_epoch(store, kafka_offset=20)
    client_created = False

    class UnexpectedKafkaConsumer:
        def __init__(self, **_kwargs):
            nonlocal client_created
            client_created = True

    fake_kafka = SimpleNamespace(
        KafkaConsumer=UnexpectedKafkaConsumer,
        AsyncConsumerRebalanceListener=object,
    )
    monkeypatch.setattr(
        consumer_module.importlib,
        "import_module",
        lambda name: fake_kafka if name == "kafka" else None,
    )

    with pytest.raises(ExternalWriteFenceError) as exc:
        consumer_module.create_consumer(config, store=store)

    assert exc.value.code == "resident_activation_epoch_state_invalid"
    assert client_created is False


def test_create_consumer_without_epoch_rejects_unsafe_false_flag_before_client(
    monkeypatch,
    tmp_path,
):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_SUBMIT_ENABLED="true",
        HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="false",
    )
    store = RcaControlStore(config.control_db_path)
    client_created = False

    class UnexpectedKafkaConsumer:
        def __init__(self, **_kwargs):
            nonlocal client_created
            client_created = True

    fake_kafka = SimpleNamespace(
        KafkaConsumer=UnexpectedKafkaConsumer,
        AsyncConsumerRebalanceListener=object,
    )
    monkeypatch.setattr(
        consumer_module.importlib,
        "import_module",
        lambda name: fake_kafka if name == "kafka" else None,
    )

    with pytest.raises(ExternalWriteFenceError) as exc:
        consumer_module.create_consumer(config, store=store)

    assert exc.value.code == "resident_activation_epoch_missing"
    assert client_created is False
    assert store.list_rows("kafka_inbox") == []
    assert store.list_rows("rca_outbox") == []


def test_resident_main_without_epoch_exits_nonzero_with_zero_writes(
    monkeypatch,
    tmp_path,
):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_SUBMIT_ENABLED="true",
        HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="false",
    )
    store = RcaControlStore(config.control_db_path)
    consumer_created = False

    def unexpected_consumer(*_args, **_kwargs):
        nonlocal consumer_created
        consumer_created = True
        raise AssertionError("consumer must not be created without an epoch")

    monkeypatch.setattr(
        consumer_module, "load_consumer_environment", lambda _path=None: tmp_path
    )
    monkeypatch.setattr(
        consumer_module.ConsumerConfig,
        "from_env",
        lambda *_args, **_kwargs: config,
    )
    monkeypatch.setattr(consumer_module, "create_consumer", unexpected_consumer)

    assert consumer_module.main([]) == 2
    assert consumer_created is False
    assert store.list_rows("kafka_inbox") == []
    assert store.list_rows("rca_outbox") == []


@pytest.mark.parametrize(
    ("name", "value", "message"),
    [
        ("HERMES_RCA_KAFKA_SECURITY_PROTOCOL", "PLAINTEXT", "SASL_PLAINTEXT"),
        ("HERMES_RCA_KAFKA_SASL_MECHANISM", "SCRAM-SHA-256", "exactly PLAIN"),
    ],
)
def test_fixed_service_rejects_unreviewed_kafka_security_modes(
    tmp_path, name, value, message
):
    with pytest.raises(ValueError, match=message):
        _config(tmp_path, **{name: value})


def test_consumer_timeout_env_is_rejected_instead_of_stopping_idle_service(tmp_path):
    with pytest.raises(ValueError, match="must stay resident"):
        _config(tmp_path, HERMES_RCA_KAFKA_CONSUMER_TIMEOUT_MS="120000")


def test_offset_lookup_timeout_must_fit_inside_group_session(tmp_path):
    with pytest.raises(ValueError, match="less than session timeout"):
        _config(
            tmp_path,
            HERMES_RCA_KAFKA_SESSION_TIMEOUT_MS="10000",
            HERMES_RCA_KAFKA_OFFSET_LOOKUP_TIMEOUT_MS="10000",
        )


def test_outbox_resume_watermark_must_be_below_pause_watermark(tmp_path):
    with pytest.raises(ValueError, match="resume watermark"):
        _config(
            tmp_path,
            HERMES_RCA_KAFKA_OUTBOX_HIGH_WATERMARK="10",
            HERMES_RCA_KAFKA_OUTBOX_RESUME_WATERMARK="10",
        )


def test_empty_polls_are_heartbeats_and_do_not_exit_resident_loop(tmp_path):
    config = _config(tmp_path)
    store = RcaControlStore(config.control_db_path)
    consumer = FakeConsumer([{}, {}, {}])

    stats = consumer_module.run_poll_loop(
        consumer,
        store,
        config,
        max_polls=3,
        commit_payload=lambda message: {"offset": message.offset + 1},
    )

    assert stats.polls == 3
    assert stats.idle_polls == 3
    assert len(consumer.poll_calls) == 3
    assert consumer.commits == []


def test_dispatch_backlog_pauses_and_resumes_partitions_while_polling_heartbeats(
    tmp_path, monkeypatch
):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_OUTBOX_HIGH_WATERMARK="2",
        HERMES_RCA_KAFKA_OUTBOX_RESUME_WATERMARK="1",
    )
    store = RcaControlStore(config.control_db_path)
    backlogs = iter((2, 1))
    monkeypatch.setattr(store, "dispatch_backlog_count", lambda: next(backlogs))

    class BackpressureConsumer(FakeConsumer):
        def __init__(self):
            super().__init__([{}, {}])
            self.paused = []
            self.resumed = []

        def assignment(self):
            return {"partition-0", "partition-1"}

        def pause(self, *partitions):
            self.paused.append(set(partitions))

        def resume(self, *partitions):
            self.resumed.append(set(partitions))

    consumer = BackpressureConsumer()
    stats = consumer_module.run_poll_loop(
        consumer,
        store,
        config,
        max_polls=2,
        commit_payload=lambda message: {"offset": message.offset + 1},
    )

    assert stats.polls == 2
    assert stats.backpressure_pauses == 1
    assert stats.backpressure_resumes == 1
    assert stats.max_dispatch_backlog == 2
    assert consumer.paused == [{"partition-0", "partition-1"}]
    assert consumer.resumed == [{"partition-0", "partition-1"}]


def test_record_commits_exact_offset_only_after_durable_ingest(tmp_path):
    config = _config(tmp_path)
    store = RcaControlStore(config.control_db_path)
    message = _message(offset=42)
    consumer = FakeConsumer([{"partition-0": [message]}])

    stats = consumer_module.run_poll_loop(
        consumer,
        store,
        config,
        max_polls=1,
        commit_payload=lambda item: {("tp", item.partition): item.offset + 1},
    )

    assert stats.records_seen == 1
    assert stats.records_committed == 1
    assert stats.accepted == 1
    assert consumer.commits == [{"offsets": {("tp", 0): 43}}]
    assert store.get_inbox(f"{TOPIC}:0:42")["decision"] == "accepted"


def test_activation_preauthorized_ingest_is_deferred_without_commit(tmp_path):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_SUBMIT_ENABLED="true",
        HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="true",
    )
    store = RcaControlStore(config.control_db_path)
    _prepare_activation_epoch(store, kafka_offset=20)
    message = _message(offset=20)

    class PausingConsumer(FakeConsumer):
        def __init__(self):
            super().__init__([{"partition-0": [message]}])
            self.paused = []

        def pause(self, *partitions):
            self.paused.extend(partitions)

    consumer = PausingConsumer()

    stats = consumer_module.run_poll_loop(
        consumer,
        store,
        config,
        max_polls=1,
        commit_payload=lambda item: {("tp", item.partition): item.offset + 1},
    )

    assert stats.records_committed == 0
    assert stats.activation_deferred == 1
    assert stats.blocked_partitions == 1
    assert consumer.commits == []
    assert consumer.paused == ["partition-0"]
    assert store.list_rows("kafka_inbox") == []
    assert store.list_rows("rca_outbox") == []


def test_activation_bounded_passive_kafka_ingest_is_shadow_and_commits(
    tmp_path,
):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_SUBMIT_ENABLED="true",
        HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="true",
    )
    store = RcaControlStore(config.control_db_path)
    _prepare_activation_epoch(store, kafka_offset=20, bounded=True)
    message = _message(offset=20)
    consumer = FakeConsumer([{"partition-0": [message]}])

    stats = consumer_module.run_poll_loop(
        consumer,
        store,
        config,
        max_polls=1,
        commit_payload=lambda item: {("tp", item.partition): item.offset + 1},
    )

    [outbox] = store.list_rows("rca_outbox")
    slot = next(
        row
        for row in store.list_rows("rca_activation_budget_slots")
        if row["slot_kind"] == "kafka_success"
    )
    assert stats.records_committed == 1
    assert consumer.commits == [{"offsets": {("tp", 0): 21}}]
    assert outbox["status"] == "shadow"
    assert outbox["activation_ledger_id"] is not None
    assert slot["consumed_ledger_id"] is None


def test_resident_exact_recovery_kafka_canary_is_rejected_without_progress_regression(
    tmp_path,
):
    request_path = tmp_path / "exact-recovery-request.json"
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_SUBMIT_ENABLED="true",
        HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="true",
        HERMES_RCA_KAFKA_EXACT_RECOVERY_REQUEST_PATH=str(request_path),
    )
    store = RcaControlStore(config.control_db_path)
    store.ingest_record(
        KafkaRecord(
            topic=TOPIC,
            partition=0,
            offset=50,
            value=b"{}",
        ),
        policy=config.policy,
    )
    _prepare_activation_epoch(
        store,
        kafka_offset=20,
        bounded=True,
        partition_start_fence={TOPIC: {"0": 20}},
    )
    message = _message(offset=20)
    probe = RcaControlStore(tmp_path / "probe.sqlite3")
    expected = probe.ingest_record(
        KafkaRecord(topic=TOPIC, partition=0, offset=20, value=message.value),
        policy=config.policy,
        submit_enabled=True,
    )
    request = _exact_recovery_request(
        request_path,
        config=config,
        store=store,
        message=message,
        expected=expected,
    )
    observation = {
        "assignment_mode": "explicit_single_partition",
        "assigned_partitions": [0],
        "retained_start": 10,
        "retained_end": 80,
        "group_id": None,
        "enable_auto_commit": False,
        "commit_called": False,
    }
    resident_identity = consumer_module.HealthReporter(
        config, store
    ).runtime_identity.to_dict()
    ordinary_consumer = FakeConsumer([{}])

    with pytest.raises(consumer_module.ConsumerLoopError, match="exact_recovery"):
        consumer_module.run_poll_loop(
            ordinary_consumer,
            store,
            config,
            max_polls=1,
            recover_on_start=False,
            exact_recovery_reader=lambda _config, _request: (message, observation),
            exact_recovery_runtime_identity=resident_identity,
        )

    assert ordinary_consumer.commits == []
    assert store.partition_progress(topic=TOPIC, partitions=[0]) == {0: 51}
    assert store.get_inbox(request["event_uid"]) is None
    kafka_slot = next(
        row
        for row in store.list_rows("rca_activation_budget_slots")
        if row["slot_kind"] == "kafka_success"
    )
    assert kafka_slot["consumed_ledger_id"] is None
    receipt_path = consumer_module._exact_recovery_receipt_path(request_path)
    assert receipt_path.exists() is False


def test_resident_exact_recovery_failure_stops_before_ordinary_poll(tmp_path):
    request_path = tmp_path / "exact-recovery-request.json"
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_SUBMIT_ENABLED="true",
        HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="true",
        HERMES_RCA_KAFKA_EXACT_RECOVERY_REQUEST_PATH=str(request_path),
    )
    store = RcaControlStore(config.control_db_path)
    _prepare_activation_epoch(store, kafka_offset=20, bounded=True)
    message = _message(offset=20)
    probe = RcaControlStore(tmp_path / "probe.sqlite3")
    expected = probe.ingest_record(
        KafkaRecord(topic=TOPIC, partition=0, offset=20, value=message.value),
        policy=config.policy,
        submit_enabled=True,
    )
    _exact_recovery_request(
        request_path,
        config=config,
        store=store,
        message=message,
        expected=expected,
        raw_sha256="f" * 64,
    )
    ordinary_consumer = FakeConsumer([{}])

    with pytest.raises(consumer_module.ConsumerLoopError) as caught:
        consumer_module.run_poll_loop(
            ordinary_consumer,
            store,
            config,
            max_polls=1,
            recover_on_start=False,
            exact_recovery_reader=lambda _config, _request: (
                message,
                {
                    "group_id": None,
                    "enable_auto_commit": False,
                    "commit_called": False,
                },
            ),
            exact_recovery_runtime_identity=consumer_module.HealthReporter(
                config, store
            ).runtime_identity.to_dict(),
        )

    assert caught.value.phase == "exact_recovery"
    assert ordinary_consumer.poll_calls == []
    assert ordinary_consumer.commits == []
    assert store.get_inbox(f"{TOPIC}:0:20") is None
    assert not consumer_module._exact_recovery_receipt_path(request_path).exists()


def test_exact_gray_gate_rewinds_and_holds_unrelated_bounded_kafka(tmp_path):
    request_path = tmp_path / "exact-recovery-request.json"
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_SUBMIT_ENABLED="true",
        HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="true",
        HERMES_RCA_KAFKA_EXACT_RECOVERY_REQUEST_PATH=str(request_path),
    )
    store = RcaControlStore(config.control_db_path)
    _prepare_activation_epoch(store, kafka_offset=20, bounded=True)
    consumer = FreezeCapableConsumer(
        [{FreezeTopicPartition(TOPIC, 0): [_message(offset=21)]}],
        committed_offsets={0: 20},
    )

    stats = consumer_module.run_poll_loop(
        consumer,
        store,
        config,
        max_polls=1,
        recover_on_start=False,
    )

    assert stats.polls == 1
    assert consumer.commits == []
    assert consumer.seek_calls[-1] == (FreezeTopicPartition(TOPIC, 0), 20)
    assert consumer.paused_calls[-1] == (FreezeTopicPartition(TOPIC, 0),)
    assert store.get_inbox(f"{TOPIC}:0:21") is None


def test_natural_gray_gate_commits_one_steady_issue_then_holds_batch_tail(tmp_path):
    request_path = tmp_path / "exact-recovery-request.json"
    gate_path = tmp_path / "natural-canary-gate.json"
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_SUBMIT_ENABLED="true",
        HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="true",
        HERMES_RCA_KAFKA_EXACT_RECOVERY_REQUEST_PATH=str(request_path),
        HERMES_RCA_KAFKA_NATURAL_CANARY_GATE_PATH=str(gate_path),
    )
    store = RcaControlStore(config.control_db_path)
    _advance_activation_to_steady(store, kafka_offset=20)
    _natural_canary_gate(gate_path, config=config, store=store, minimum_offset=21)
    partition = FreezeTopicPartition(TOPIC, 0)
    consumer = FreezeCapableConsumer(
        [{
            partition: [
                _message(offset=21, value=_value(work_item_id=7041712901)),
            ]
        }],
        committed_offsets={0: 21},
    )
    runtime_identity = consumer_module.HealthReporter(
        config, store
    ).runtime_identity.to_dict()

    stats = consumer_module.run_poll_loop(
        consumer,
        store,
        config,
        max_polls=1,
        recover_on_start=False,
        exact_recovery_runtime_identity=runtime_identity,
        commit_payload=lambda message: {("tp", 0): message.offset + 1},
    )

    assert consumer.poll_calls[0]["max_records"] == 1
    assert consumer.commits == [{"offsets": {("tp", 0): 22}}]
    assert store.get_inbox(f"{TOPIC}:0:21")["decision"] == "accepted"
    assert store.get_inbox(f"{TOPIC}:0:22") is None
    assert stats.natural_canary_selected == 1
    assert consumer.paused_calls[-1] == (partition,)
    receipt = json.loads(
        consumer_module._natural_canary_receipt_path(gate_path).read_text(
            encoding="utf-8"
        )
    )
    assert receipt["event_uid"] == f"{TOPIC}:0:21"
    assert receipt["activation_reason"] == "activation_steady_active"
    assert receipt["next_ordinary_record_held"] is True

    receipt["request_sha256"] = "f" * 64
    receipt_path = consumer_module._natural_canary_receipt_path(gate_path)
    receipt_path.write_bytes(consumer_module._canonical_bytes(receipt) + b"\n")
    restart_consumer = FreezeCapableConsumer([{}], committed_offsets={0: 22})
    with pytest.raises(consumer_module.ConsumerLoopError) as caught:
        consumer_module.run_poll_loop(
            restart_consumer,
            store,
            config,
            max_polls=1,
            recover_on_start=False,
            exact_recovery_runtime_identity=runtime_identity,
        )

    assert caught.value.phase == "natural_canary_gate"
    assert restart_consumer.poll_calls == []


def test_natural_existing_candidate_preserves_zero_offset():
    class CandidateStore:
        @staticmethod
        def list_rows(table):
            assert table == "kafka_inbox"
            return [
                {
                    "topic": TOPIC,
                    "offset_id": 0,
                    "decision": "accepted",
                    "processed_at": "2026-07-22T00:00:01+00:00",
                    "event_uid": f"{TOPIC}:0:0",
                }
            ]

    candidate = consumer_module._natural_canary_existing_candidate(
        CandidateStore(),
        {
            "minimum_offset": 0,
            "created_at": "2026-07-22T00:00:00+00:00",
        },
    )

    assert candidate is not None
    assert candidate["offset_id"] == 0


def test_natural_gray_gate_stops_on_first_invalid_candidate(tmp_path):
    request_path = tmp_path / "exact-recovery-request.json"
    gate_path = tmp_path / "natural-canary-gate.json"
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_SUBMIT_ENABLED="true",
        HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="true",
        HERMES_RCA_KAFKA_EXACT_RECOVERY_REQUEST_PATH=str(request_path),
        HERMES_RCA_KAFKA_NATURAL_CANARY_GATE_PATH=str(gate_path),
    )
    store = RcaControlStore(config.control_db_path)
    _advance_activation_to_steady(store, kafka_offset=20)
    _natural_canary_gate(gate_path, config=config, store=store, minimum_offset=21)
    partition = FreezeTopicPartition(TOPIC, 0)
    consumer = FreezeCapableConsumer(
        [{
            partition: [
                _message(
                    offset=21,
                    value=b"x" * (consumer_module.MAX_WORKFLOW_EVENT_BYTES + 1),
                ),
            ]
        }],
        committed_offsets={0: 21},
    )

    with pytest.raises(consumer_module.ConsumerLoopError) as caught:
        consumer_module.run_poll_loop(
            consumer,
            store,
            config,
            max_polls=1,
            recover_on_start=False,
            exact_recovery_runtime_identity=consumer_module.HealthReporter(
                config, store
            ).runtime_identity.to_dict(),
            commit_payload=lambda message: {("tp", 0): message.offset + 1},
        )

    assert caught.value.phase == "natural_canary"
    assert consumer.commits == [{"offsets": {("tp", 0): 22}}]
    assert store.get_inbox(f"{TOPIC}:0:21")["decision"] == "invalid"
    assert store.get_inbox(f"{TOPIC}:0:22") is None
    assert not consumer_module._natural_canary_receipt_path(gate_path).exists()


def test_activation_bounded_unauthorized_ingest_stays_shadow_and_commits(tmp_path):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_SUBMIT_ENABLED="true",
        HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="true",
    )
    store = RcaControlStore(config.control_db_path)
    _prepare_activation_epoch(store, kafka_offset=20, bounded=True)
    message = _message(offset=21)
    consumer = FakeConsumer([{"partition-0": [message]}])

    stats = consumer_module.run_poll_loop(
        consumer,
        store,
        config,
        max_polls=1,
        commit_payload=lambda item: {("tp", item.partition): item.offset + 1},
    )

    [outbox] = store.list_rows("rca_outbox")
    slot = next(
        row
        for row in store.list_rows("rca_activation_budget_slots")
        if row["slot_kind"] == "kafka_success"
    )
    assert stats.records_committed == 1
    assert consumer.commits == [{"offsets": {("tp", 0): 22}}]
    assert outbox["status"] == "shadow"
    assert slot["consumed_ledger_id"] is None


def test_activation_steady_ingest_is_pending_and_commits(tmp_path):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_SUBMIT_ENABLED="true",
        HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="true",
    )
    store = RcaControlStore(config.control_db_path)
    _advance_activation_to_steady(store, kafka_offset=20)
    message = _message(offset=30, value=_value(work_item_id=7041712816))
    consumer = FakeConsumer([{"partition-0": [message]}])

    stats = consumer_module.run_poll_loop(
        consumer,
        store,
        config,
        max_polls=1,
        commit_payload=lambda item: {("tp", item.partition): item.offset + 1},
    )

    [outbox] = [
        row for row in store.list_rows("rca_outbox") if row["status"] == "pending"
    ]
    assert stats.records_committed == 1
    assert consumer.commits == [{"offsets": {("tp", 0): 31}}]
    assert outbox["status"] == "pending"
    assert outbox["activation_epoch_id"] == "rca-consumer-activation-20260712"
    assert outbox["activation_ledger_id"] is not None


def test_activation_confirmed_batch_tail_resumes_monotonically_without_restart(
    tmp_path,
):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_SUBMIT_ENABLED="true",
        HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="true",
    )
    store = RcaControlStore(config.control_db_path)
    _advance_activation_to_confirmed(store, kafka_offset=20)
    messages = [
        _message(offset=21, value=_value(work_item_id=7041712816)),
        _message(offset=22, value=_value(work_item_id=7041712817)),
        _message(offset=23, value=_value(work_item_id=7041712818)),
    ]

    class PausingConsumer(FakeConsumer):
        def __init__(self):
            super().__init__([{"partition-0": messages}])
            self.paused = []
            self.resumed = []

        def pause(self, *partitions):
            self.paused.extend(partitions)
            store.transition_activation_epoch(
                epoch_id="rca-consumer-activation-20260712",
                expected_state="confirmed",
                target_state="steady_active",
                operator="consumer-test",
                reason="resume deferred fence record without a process restart",
            )

        def resume(self, *partitions):
            self.resumed.extend(partitions)

    consumer = PausingConsumer()
    stats = consumer_module.run_poll_loop(
        consumer,
        store,
        config,
        max_polls=2,
        commit_payload=lambda item: {("tp", item.partition): item.offset + 1},
    )

    assert stats.record_processing_blocks == 1
    assert stats.activation_deferred == 1
    assert stats.activation_resumed == 1
    assert stats.blocked_partitions == 0
    assert stats.records_seen == 3
    assert stats.accepted == 3
    assert stats.records_committed == 3
    assert consumer.commits == [
        {"offsets": {("tp", 0): 22}},
        {"offsets": {("tp", 0): 23}},
        {"offsets": {("tp", 0): 24}},
    ]
    assert consumer.paused == ["partition-0"]
    assert consumer.resumed == ["partition-0"]
    assert [
        store.get_inbox(f"{TOPIC}:0:{offset}")["decision"]
        for offset in (21, 22, 23)
    ] == ["accepted", "accepted", "accepted"]


def test_activation_ready_freezes_all_partitions_and_publishes_exact_receipt(
    tmp_path,
):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_SUBMIT_ENABLED="true",
        HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="true",
    )
    store = RcaControlStore(config.control_db_path)
    epoch_id = _prepare_ready_bounded_activation(store, kafka_offset=20)
    consumer = FreezeCapableConsumer([{}, {}], committed_offsets={0: 21})
    reporter = consumer_module.HealthReporter(config, store)
    reporter.set_assignment_reporter(consumer.diagnostics)

    stats = consumer_module.run_poll_loop(
        consumer,
        store,
        config,
        health=reporter,
        max_polls=2,
        commit_payload=lambda item: {("tp", item.partition): item.offset + 1},
    )

    health = json.loads(config.health_path.read_text(encoding="utf-8"))
    receipt = health["activation_freeze"]
    assert set(receipt) == {
        "schema_version",
        "epoch_id",
        "state",
        "freeze_token",
        "paused_at",
        "observed_at",
        "consumer_runtime_identity_sha256",
        "partition_positions",
        "restart_required",
    }
    assert receipt["schema_version"] == "pnc_rca_activation_ingress_freeze_v2"
    assert receipt["epoch_id"] == epoch_id
    assert receipt["state"] == "partitions_paused"
    assert receipt["partition_positions"] == {TOPIC: {"0": 21}}
    assert receipt["consumer_runtime_identity_sha256"] == (
        consumer_module.canonical_json_sha256(health["runtime_identity"])
    )
    assert receipt["restart_required"] is False
    assert len(receipt["freeze_token"]) == 64
    assert stats.activation_freezes == 1
    assert stats.activation_freeze_rebuilds == 0
    assert stats.activation_freeze_releases == 0
    assert stats.records_seen == 0
    assert len(consumer.poll_calls) == 2
    assert consumer.seek_calls == [(FreezeTopicPartition(TOPIC, 0), 21)]
    assert consumer.resumed_calls == []


def test_activation_freeze_rewinds_nonempty_batch_observed_after_poll(
    tmp_path,
    monkeypatch,
):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_SUBMIT_ENABLED="true",
        HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="true",
    )
    store = RcaControlStore(config.control_db_path)
    not_ready = {
        "epoch_id": "rca-consumer-activation-20260712",
        "state": "bounded_active",
        "ready": False,
        "reason": "activation_slots_incomplete",
        "required_slot_count": 2,
        "consumed_slot_count": 1,
        "completed_bound_slot_count": 1,
        "pending_inbox": 0,
        "unbound_ledger": 0,
        "inflight_writes": 0,
    }
    ready = {
        **not_ready,
        "ready": True,
        "reason": "ready",
        "consumed_slot_count": 2,
        "completed_bound_slot_count": 2,
    }
    readiness = iter((not_ready, ready))
    monkeypatch.setattr(
        consumer_module,
        "_activation_freeze_readiness",
        lambda _store: next(readiness),
    )
    partition = FreezeTopicPartition(TOPIC, 0)
    consumer = FreezeCapableConsumer(
        [{partition: [_message(offset=21)]}],
        committed_offsets={0: 20},
        initial_positions={0: 30},
    )
    reporter = consumer_module.HealthReporter(config, store)
    reporter.set_assignment_reporter(consumer.diagnostics)

    stats = consumer_module.run_poll_loop(
        consumer,
        store,
        config,
        health=reporter,
        max_polls=1,
        recover_on_start=False,
        commit_payload=lambda item: {("tp", item.partition): item.offset + 1},
    )

    assert stats.activation_freezes == 1
    assert stats.records_seen == 0
    assert stats.records_committed == 0
    assert consumer.commits == []
    assert consumer.seek_calls == [(partition, 20)]
    assert store.list_rows("kafka_inbox") == []


@pytest.mark.parametrize("uncommitted_position", [50, 75])
def test_activation_freeze_uses_listener_verified_t0_for_uncommitted_partition(
    tmp_path,
    uncommitted_position,
):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_SUBMIT_ENABLED="true",
        HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="true",
    )
    store = RcaControlStore(config.control_db_path)
    _prepare_ready_bounded_activation(
        store,
        kafka_offset=20,
        partition_start_fence={TOPIC: {"0": 20, "1": 50}},
    )
    uncommitted_message = _message(
        offset=50,
        value=_value(work_item_id=7041712819),
    )
    uncommitted_message.partition = 1
    consumer = FreezeCapableConsumer(
        [{FreezeTopicPartition(TOPIC, 1): [uncommitted_message]}],
        committed_offsets={0: 21, 1: None},
        applied_t0_offsets={1: 50},
        initial_positions={1: uncommitted_position},
    )
    reporter = consumer_module.HealthReporter(config, store)
    reporter.set_assignment_reporter(consumer.diagnostics)

    stats = consumer_module.run_poll_loop(
        consumer,
        store,
        config,
        health=reporter,
        max_polls=1,
    )

    health = json.loads(config.health_path.read_text(encoding="utf-8"))
    assert health["activation_freeze"]["partition_positions"] == {
        TOPIC: {"0": 21, "1": 50}
    }
    assert stats.activation_freezes == 1
    assert stats.records_seen == 0
    assert consumer.seek_calls == [
        (FreezeTopicPartition(TOPIC, 0), 21),
        (FreezeTopicPartition(TOPIC, 1), 50),
    ]
    assert consumer.commits == []
    assert store.get_inbox(f"{TOPIC}:1:50") is None


@pytest.mark.parametrize(
    ("applied_t0_offsets", "initial_position", "expected_error"),
    [
        ({}, 50, "activation_freeze_uncommitted_t0_missing"),
        (
            {1: 50},
            49,
            "activation_freeze_uncommitted_t0_position_before_start",
        ),
    ],
)
def test_activation_freeze_rejects_unproven_uncommitted_partition_position(
    tmp_path,
    applied_t0_offsets,
    initial_position,
    expected_error,
):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_SUBMIT_ENABLED="true",
        HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="true",
    )
    store = RcaControlStore(config.control_db_path)
    _prepare_ready_bounded_activation(
        store,
        kafka_offset=20,
        partition_start_fence={TOPIC: {"0": 20, "1": 50}},
    )
    consumer = FreezeCapableConsumer(
        [{}],
        committed_offsets={0: 21, 1: None},
        applied_t0_offsets=applied_t0_offsets,
        initial_positions={1: initial_position},
    )
    reporter = consumer_module.HealthReporter(config, store)
    reporter.set_assignment_reporter(consumer.diagnostics)

    with pytest.raises(
        consumer_module.ConsumerLoopError, match="activation_freeze failed"
    ) as caught:
        consumer_module.run_poll_loop(
            consumer,
            store,
            config,
            health=reporter,
            max_polls=1,
        )

    assert str(caught.value.__cause__) == expected_error
    assert consumer.poll_calls == []
    assert consumer.commits == []
    health = json.loads(config.health_path.read_text(encoding="utf-8"))
    assert health["state"] == "activation_freeze_error"
    assert health["activation_freeze"] is None


def test_bounded_ready_freezes_before_passive_kafka_batch(
    tmp_path,
):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_SUBMIT_ENABLED="true",
        HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="true",
    )
    store = RcaControlStore(config.control_db_path)
    _epoch_id, identities = _prepare_activation_epoch(
        store,
        kafka_offset=20,
        bounded=True,
    )
    for slot_kind in ("manual_success", "manual_terminal_failure"):
        identity = identities[slot_kind][1]
        store.admit_manual_trigger(
            ManualRcaTriggerRequest(
                schema_version=MANUAL_TRIGGER_SCHEMA_VERSION,
                issue_url=identity["issue_url"],
                mode=identity["mode"],
                reason="prepare two completed bounded canaries",
                platform="feishu",
                chat_id=identity["chat_id"],
                thread_id=identity["thread_id"],
                message_id=identity["message_id"],
                requester_id=identity["requester_id"],
            ),
            allowed_chat_ids={"oc_activation_test"},
            submit_enabled=True,
            operator_authorized=slot_kind == "manual_terminal_failure",
            active_policy=config.policy,
            activation_required=True,
        )
    for index in range(2):
        claim = store.claim_outbox(
            lease_owner=f"consumer-two-canaries-{index}",
            activation_required=True,
        )
        assert claim is not None
        store.complete_outbox(
            outbox_id=claim.outbox_id,
            lease_token=claim.lease_token,
            result={"outcome": "manual_canary_completed"},
        )
    initial = store.health()["activation"]["ingress_freeze_readiness"]
    assert initial["consumed_slot_count"] == 2
    assert initial["completed_bound_slot_count"] == 2
    assert initial["ready"] is True
    topic_partition = FreezeTopicPartition(TOPIC, 0)
    messages = [
        _message(offset=20),
        _message(offset=21, value=_value(work_item_id=7041712816)),
        _message(offset=22, value=_value(work_item_id=7041712817)),
    ]

    class CommitTrackingFreezeConsumer(FreezeCapableConsumer):
        def commit(self, **kwargs):
            super().commit(**kwargs)
            for partition, offset in kwargs["offsets"].items():
                self.committed_offsets[partition] = int(offset)

    consumer = CommitTrackingFreezeConsumer(
        [{topic_partition: messages}],
        committed_offsets={0: 20},
    )
    reporter = consumer_module.HealthReporter(config, store)
    reporter.set_assignment_reporter(consumer.diagnostics)

    stats = consumer_module.run_poll_loop(
        consumer,
        store,
        config,
        health=reporter,
        max_polls=1,
        commit_payload=lambda item: {
            topic_partition: item.offset + 1,
        },
    )

    health = json.loads(config.health_path.read_text(encoding="utf-8"))
    assert stats.records_seen == 0
    assert stats.records_committed == 0
    assert stats.activation_freezes == 1
    assert consumer.commits == []
    assert consumer.seek_calls == [(topic_partition, 20)]
    assert health["activation_freeze"]["partition_positions"] == {
        TOPIC: {"0": 20}
    }
    assert store.get_inbox(f"{TOPIC}:0:20") is None
    assert store.get_inbox(f"{TOPIC}:0:21") is None
    assert store.get_inbox(f"{TOPIC}:0:22") is None


def test_activation_invalid_atomic_readiness_pauses_and_fails_closed(tmp_path):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="true",
    )
    consumer = FreezeCapableConsumer([{}], committed_offsets={0: 21})

    class InvalidReadinessStore:
        @staticmethod
        def process_pending(*, limit, runtime_identity=None):
            assert limit == 1000
            assert runtime_identity is not None
            return []

        @staticmethod
        def activation_ingress_freeze_readiness():
            return {
                "epoch_id": "invalid-epoch",
                "state": "bounded_active",
                "ready": True,
                "reason": "ready",
                "required_slot_count": 3,
                "consumed_slot_count": 3,
                "completed_bound_slot_count": 2,
                "pending_inbox": 0,
                "unbound_ledger": 0,
                "inflight_writes": 0,
            }

        @staticmethod
        def health():
            return {
                "activation": {
                    "ingress_freeze_readiness": (
                        InvalidReadinessStore.activation_ingress_freeze_readiness()
                    )
                }
            }

    invalid_store = InvalidReadinessStore()
    reporter = consumer_module.HealthReporter(config, invalid_store)
    reporter.set_assignment_reporter(consumer.diagnostics)
    with pytest.raises(
        consumer_module.ConsumerLoopError, match="activation_freeze failed"
    ) as caught:
        consumer_module.run_poll_loop(
            consumer,
            invalid_store,
            config,
            health=reporter,
            max_polls=1,
        )

    assert isinstance(
        caught.value.__cause__, consumer_module.ActivationFreezeProtocolError
    )
    assert consumer.poll_calls == []
    assert consumer.paused_calls == [
        (FreezeTopicPartition(TOPIC, 0),)
    ]
    assert consumer.commits == []
    health = json.loads(config.health_path.read_text(encoding="utf-8"))
    assert health["state"] == "activation_freeze_error"
    assert health["last_error"]["phase"] == "activation_freeze"


def test_activation_freeze_releases_at_steady_without_process_restart(tmp_path):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_SUBMIT_ENABLED="true",
        HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="true",
    )
    store = RcaControlStore(config.control_db_path)
    epoch_id = _prepare_ready_bounded_activation(store, kafka_offset=20)
    confirmation = _activation_confirmation_bindings(store, kafka_offset=20)

    def activate_steady(_consumer):
        store.transition_activation_epoch(
            epoch_id=epoch_id,
            expected_state="bounded_active",
            target_state="confirmed",
            production_fingerprint="3" * 64,
            production_gate_receipt_sha256="4" * 64,
            operator="consumer-freeze-test",
            reason="confirm the frozen Kafka fence",
            **confirmation,
        )
        store.transition_activation_epoch(
            epoch_id=epoch_id,
            expected_state="confirmed",
            target_state="steady_active",
            operator="consumer-freeze-test",
            reason="release Kafka ingress in the same resident",
        )

    consumer = FreezeCapableConsumer(
        [{}],
        committed_offsets={0: 21},
        on_poll=activate_steady,
    )
    reporter = consumer_module.HealthReporter(config, store)
    reporter.set_assignment_reporter(consumer.diagnostics)
    resident_identity = reporter.runtime_identity.to_dict()

    stats = consumer_module.run_poll_loop(
        consumer,
        store,
        config,
        health=reporter,
        max_polls=1,
    )

    health = json.loads(config.health_path.read_text(encoding="utf-8"))
    assert health["runtime_identity"] == resident_identity
    assert health["activation_freeze"] is None
    assert health["activation_freeze_release"]["epoch_id"] == epoch_id
    assert health["activation_freeze_release"]["state"] == "released"
    assert health["activation_freeze_release"]["reason"] == (
        "activation_steady_active"
    )
    assert health["activation_freeze_release"]["restart_required"] is False
    assert stats.activation_freezes == 1
    assert stats.activation_freeze_releases == 1
    assert consumer.resumed_calls == [
        (FreezeTopicPartition(TOPIC, 0),)
    ]


def test_activation_freeze_remains_held_in_confirmed_state(tmp_path):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_SUBMIT_ENABLED="true",
        HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="true",
    )
    store = RcaControlStore(config.control_db_path)
    epoch_id = _prepare_ready_bounded_activation(store, kafka_offset=20)
    confirmation = _activation_confirmation_bindings(store, kafka_offset=20)

    def confirm_only(_consumer):
        store.transition_activation_epoch(
            epoch_id=epoch_id,
            expected_state="bounded_active",
            target_state="confirmed",
            production_fingerprint="3" * 64,
            production_gate_receipt_sha256="4" * 64,
            operator="consumer-freeze-test",
            reason="hold Kafka ingress while activation remains confirmed",
            **confirmation,
        )

    consumer = FreezeCapableConsumer(
        [{}],
        committed_offsets={0: 21},
        on_poll=confirm_only,
    )
    reporter = consumer_module.HealthReporter(config, store)
    reporter.set_assignment_reporter(consumer.diagnostics)

    stats = consumer_module.run_poll_loop(
        consumer,
        store,
        config,
        health=reporter,
        max_polls=1,
    )

    health = json.loads(config.health_path.read_text(encoding="utf-8"))
    assert health["activation_freeze"]["epoch_id"] == epoch_id
    assert health["activation_freeze_release"] is None
    assert stats.activation_freeze_releases == 0
    assert consumer.resumed_calls == []


def test_activation_freeze_releases_when_epoch_is_aborted(tmp_path):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_SUBMIT_ENABLED="true",
        HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="true",
    )
    store = RcaControlStore(config.control_db_path)
    epoch_id = _prepare_ready_bounded_activation(store, kafka_offset=20)

    def abort_epoch(_consumer):
        store.transition_activation_epoch(
            epoch_id=epoch_id,
            expected_state="bounded_active",
            target_state="aborted",
            operator="consumer-freeze-test",
            reason="exercise automatic abort release",
        )

    consumer = FreezeCapableConsumer(
        [{}],
        committed_offsets={0: 21},
        on_poll=abort_epoch,
    )
    reporter = consumer_module.HealthReporter(config, store)
    reporter.set_assignment_reporter(consumer.diagnostics)

    stats = consumer_module.run_poll_loop(
        consumer,
        store,
        config,
        health=reporter,
        max_polls=1,
    )

    health = json.loads(config.health_path.read_text(encoding="utf-8"))
    assert health["activation_freeze"] is None
    assert health["activation_freeze_release"]["reason"] == "activation_aborted"
    assert stats.activation_freeze_releases == 1
    assert consumer.resumed_calls == [
        (FreezeTopicPartition(TOPIC, 0),)
    ]


def test_activation_freeze_releases_on_epoch_identity_change(tmp_path):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_SUBMIT_ENABLED="true",
        HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="true",
    )
    store = RcaControlStore(config.control_db_path)
    _prepare_ready_bounded_activation(store, kafka_offset=20)
    consumer = FreezeCapableConsumer([], committed_offsets={0: 21})
    reporter = consumer_module.HealthReporter(config, store)
    reporter.set_assignment_reporter(consumer.diagnostics)
    controller = consumer_module.ActivationIngressFreezeController(
        config, reporter
    )
    stats = consumer_module.PollStats()
    controller.reconcile(
        consumer,
        consumer_module._activation_freeze_readiness(store),
        backpressure_active=False,
        blocked_partitions=set(),
        stats=stats,
    )

    controller.reconcile(
        consumer,
        {
            "epoch_id": "replacement-activation-epoch",
            "state": "preauthorized",
            "ready": False,
            "reason": "activation_slots_incomplete",
            "required_slot_count": 2,
            "consumed_slot_count": 0,
            "completed_bound_slot_count": 0,
            "pending_inbox": 0,
            "unbound_ledger": 0,
            "inflight_writes": 0,
        },
        backpressure_active=False,
        blocked_partitions=set(),
        stats=stats,
    )

    health = json.loads(config.health_path.read_text(encoding="utf-8"))
    assert controller.active is False
    assert health["activation_freeze"] is None
    assert health["activation_freeze_release"]["reason"] == (
        "activation_epoch_changed"
    )
    assert stats.activation_freeze_releases == 1
    assert consumer.resumed_calls == [
        (FreezeTopicPartition(TOPIC, 0),)
    ]


def test_activation_freeze_heartbeat_refreshes_only_observation_time(
    tmp_path,
    monkeypatch,
):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_SUBMIT_ENABLED="true",
        HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="true",
    )
    store = RcaControlStore(config.control_db_path)
    _prepare_ready_bounded_activation(store, kafka_offset=20)
    consumer = FreezeCapableConsumer([], committed_offsets={0: 21})
    reporter = consumer_module.HealthReporter(config, store)
    reporter.set_assignment_reporter(consumer.diagnostics)
    controller = consumer_module.ActivationIngressFreezeController(
        config, reporter
    )
    stats = consumer_module.PollStats()
    controller.reconcile(
        consumer,
        consumer_module._activation_freeze_readiness(store),
        backpressure_active=False,
        blocked_partitions=set(),
        stats=stats,
    )
    first = dict(reporter.activation_freeze)
    observed_at = "2026-07-12T23:59:59+00:00"
    monkeypatch.setattr(consumer_module, "_utc_now", lambda: observed_at)

    reporter.write(state="activation_frozen", stats=stats, force=True)

    refreshed = json.loads(config.health_path.read_text(encoding="utf-8"))[
        "activation_freeze"
    ]
    assert refreshed["observed_at"] == observed_at
    assert refreshed["freeze_token"] == first["freeze_token"]
    assert refreshed["paused_at"] == first["paused_at"]
    assert refreshed["consumer_runtime_identity_sha256"] == (
        first["consumer_runtime_identity_sha256"]
    )
    assert refreshed["partition_positions"] == first["partition_positions"]


def test_activation_freeze_prevents_backpressure_resume(tmp_path, monkeypatch):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_SUBMIT_ENABLED="true",
        HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="true",
        HERMES_RCA_KAFKA_OUTBOX_HIGH_WATERMARK="2",
        HERMES_RCA_KAFKA_OUTBOX_RESUME_WATERMARK="1",
    )
    store = RcaControlStore(config.control_db_path)
    _prepare_ready_bounded_activation(store, kafka_offset=20)
    backlogs = iter((2, 0))
    monkeypatch.setattr(store, "dispatch_backlog_count", lambda: next(backlogs))
    consumer = FreezeCapableConsumer([{}, {}], committed_offsets={0: 21})
    reporter = consumer_module.HealthReporter(config, store)
    reporter.set_assignment_reporter(consumer.diagnostics)

    stats = consumer_module.run_poll_loop(
        consumer,
        store,
        config,
        health=reporter,
        max_polls=2,
    )

    assert stats.backpressure_pauses == 1
    assert stats.backpressure_resumes == 1
    assert stats.activation_freezes == 1
    assert consumer.resumed_calls == []


@pytest.mark.parametrize("change_kind", ["rebalance", "position"])
def test_activation_freeze_rebuilds_token_when_kafka_position_changes(
    tmp_path,
    change_kind,
):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_SUBMIT_ENABLED="true",
        HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="true",
    )
    store = RcaControlStore(config.control_db_path)
    _prepare_ready_bounded_activation(store, kafka_offset=20)
    first_tokens = []
    reporter = consumer_module.HealthReporter(config, store)

    def mutate_assignment(consumer):
        first_tokens.append(reporter.activation_freeze["freeze_token"])
        if change_kind == "rebalance":
            replacement = FreezeTopicPartition(TOPIC, 1)
            consumer._assignment = {replacement}
            consumer.committed_offsets = {replacement: 55}
            consumer.positions = {replacement: 155}
            consumer.revocation_count += 1
            consumer.assignment_count += 1
            consumer.last_assignment_at = "2026-07-12T00:00:01+00:00"
        else:
            partition = FreezeTopicPartition(TOPIC, 0)
            consumer.positions[partition] = 99

    consumer = FreezeCapableConsumer(
        [{}],
        committed_offsets={0: 21},
        on_poll=mutate_assignment,
    )
    reporter.set_assignment_reporter(consumer.diagnostics)

    stats = consumer_module.run_poll_loop(
        consumer,
        store,
        config,
        health=reporter,
        max_polls=1,
    )

    health = json.loads(config.health_path.read_text(encoding="utf-8"))
    rebuilt = health["activation_freeze"]
    assert rebuilt["freeze_token"] != first_tokens[0]
    assert stats.activation_freezes == 1
    assert stats.activation_freeze_rebuilds == 1
    assert rebuilt["partition_positions"] == (
        {TOPIC: {"1": 55}}
        if change_kind == "rebalance"
        else {TOPIC: {"0": 21}}
    )


def test_preauthorized_persist_raw_is_deferred_before_durable_write(tmp_path):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_SUBMIT_ENABLED="true",
        HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="true",
    )
    store = RcaControlStore(config.control_db_path)
    epoch_id, _identities = _prepare_activation_epoch(store, kafka_offset=20)
    record = KafkaRecord(topic=TOPIC, partition=0, offset=20, value=_value())
    with pytest.raises(
        ActivationIngressDeferredError, match="activation_ingress_unavailable"
    ):
        store.persist_raw(
            record,
            policy=config.policy,
            submit_enabled=True,
            activation_required=True,
            activation_slot_kind="kafka_success",
        )

    store.transition_activation_epoch(
        epoch_id=epoch_id,
        expected_state="preauthorized",
        target_state="bounded_active",
        operator="consumer-test",
        reason="no preauthorized ingress was persisted",
    )

    slot = next(
        row
        for row in store.list_rows("rca_activation_budget_slots")
        if row["slot_kind"] == "kafka_success"
    )
    assert store.list_rows("kafka_inbox") == []
    assert store.list_rows("rca_outbox") == []
    assert slot["consumed_ledger_id"] is None


def test_unknown_record_failure_pauses_only_its_partition(tmp_path):
    config = _config(tmp_path)
    blocked_message = _message(offset=42)
    healthy_message = _message(offset=84)
    healthy_message.partition = 1

    class PartitionConsumer(FakeConsumer):
        def __init__(self):
            super().__init__([
                {
                    "partition-0": [blocked_message],
                    "partition-1": [healthy_message],
                }
            ])
            self.paused = []

        def pause(self, *partitions):
            self.paused.extend(partitions)

    class PartitionStore:
        @staticmethod
        def process_pending(*, limit):
            assert limit == 1000
            return []

        @staticmethod
        def dispatch_backlog_count():
            return 0

        @staticmethod
        def ingest_record(record, **_kwargs):
            if record.partition == 0:
                raise RecordProcessingBlockedError(f"{record.topic}:0:{record.offset}")
            return SimpleNamespace(decision="accepted", ack_safe=True)

    class RecordingHealth:
        def __init__(self):
            self.states = []
            self.last_error = None

        def write(self, *, state, stats, force=False):
            del stats, force
            self.states.append(state)

        def event(self):
            return None

        def error(self, phase, exc):
            self.last_error = (phase, type(exc).__name__)

        def committed(self, *, clear_error=True):
            if clear_error:
                self.last_error = None

    consumer = PartitionConsumer()
    health = RecordingHealth()
    stats = consumer_module.run_poll_loop(
        consumer,
        PartitionStore(),
        config,
        health=health,
        max_polls=1,
        commit_payload=lambda message: {("tp", message.partition): message.offset + 1},
    )

    assert stats.records_seen == 2
    assert stats.record_processing_blocks == 1
    assert stats.blocked_partitions == 1
    assert stats.ingest_errors == 1
    assert stats.accepted == 1
    assert stats.records_committed == 1
    assert consumer.paused == ["partition-0"]
    assert consumer.commits == [{"offsets": {("tp", 1): 85}}]
    assert health.states[-2:] == ["partition_blocked", "stopped"]
    assert health.last_error == ("record_processing", "RecordProcessingBlockedError")


def test_recover_pending_leaves_unknown_record_for_partition_isolation(tmp_path):
    config = _config(tmp_path)
    stats = consumer_module.PollStats()

    class BlockedStore:
        @staticmethod
        def process_pending(*, limit):
            assert limit == 1000
            raise RecordProcessingBlockedError(f"{TOPIC}:0:42")

    consumer_module.recover_pending(BlockedStore(), stats)

    assert stats.record_processing_blocks == 1
    assert stats.blocked_partitions == 1


def test_default_commit_payload_advances_exactly_one_offset(monkeypatch):
    class FakeTopicPartition:
        def __init__(self, topic, partition):
            self.topic = topic
            self.partition = partition

        def __hash__(self):
            return hash((self.topic, self.partition))

    class FakeOffsetAndMetadata:
        def __init__(self, offset, metadata, leader_epoch=-1):
            self.offset = offset
            self.metadata = metadata
            self.leader_epoch = leader_epoch

    fake_structs = SimpleNamespace(
        TopicPartition=FakeTopicPartition,
        OffsetAndMetadata=FakeOffsetAndMetadata,
    )
    monkeypatch.setattr(
        consumer_module.importlib,
        "import_module",
        lambda name: fake_structs if name == "kafka.structs" else None,
    )

    payload = consumer_module._default_commit_payload(
        SimpleNamespace(topic=TOPIC, partition=2, offset=41)
    )
    topic_partition, offset = next(iter(payload.items()))

    assert (topic_partition.topic, topic_partition.partition) == (TOPIC, 2)
    assert offset.offset == 42


def test_ingest_failure_does_not_commit_or_process_later_partition_records(tmp_path):
    config = _config(tmp_path)

    class FailingStore:
        def process_pending(self, **_kwargs):
            return []

        def ingest_record(self, *_args, **_kwargs):
            raise RuntimeError("storage unavailable")

    consumer = FakeConsumer([{"partition-0": [_message(1), _message(2)]}])
    with pytest.raises(consumer_module.ConsumerLoopError) as error:
        consumer_module.run_poll_loop(
            consumer,
            FailingStore(),
            config,
            max_polls=1,
            commit_payload=lambda item: {"offset": item.offset + 1},
        )
    stats = error.value.stats

    assert stats.records_seen == 1
    assert stats.ingest_errors == 1
    assert stats.records_committed == 0
    assert consumer.commits == []


def test_ack_unsafe_result_is_fatal_before_any_later_offset_can_commit(tmp_path):
    config = _config(tmp_path)

    class AckUnsafeStore:
        def process_pending(self, **_kwargs):
            return []

        def ingest_record(self, *_args, **_kwargs):
            return SimpleNamespace(decision="accepted", ack_safe=False)

    consumer = FakeConsumer([{"partition-0": [_message(1), _message(2)]}])

    with pytest.raises(consumer_module.ConsumerLoopError, match="ack_safety failed"):
        consumer_module.run_poll_loop(
            consumer,
            AckUnsafeStore(),
            config,
            max_polls=1,
            commit_payload=lambda item: {"offset": item.offset + 1},
        )

    assert consumer.commits == []


def test_poll_failure_is_fatal_and_redacted_in_health(tmp_path):
    config = _config(tmp_path)
    store = RcaControlStore(config.control_db_path)
    reporter = consumer_module.HealthReporter(config, store)
    consumer = PollFailingConsumer([])

    with pytest.raises(consumer_module.ConsumerLoopError, match="poll failed"):
        consumer_module.run_poll_loop(
            consumer,
            store,
            config,
            health=reporter,
            max_polls=1,
        )

    health_text = config.health_path.read_text(encoding="utf-8")
    health = json.loads(health_text)
    assert health["state"] == "error"
    assert health["last_error"]["phase"] == "poll"
    assert "raw-secret-payload" not in health_text


def test_startup_recovers_pending_raw_rows_before_first_poll(tmp_path):
    config = _config(tmp_path)
    store = RcaControlStore(config.control_db_path)
    record = KafkaRecord(topic=TOPIC, partition=0, offset=7, value=_value())
    store.persist_raw(record, policy=config.policy)
    consumer = FakeConsumer([{}])

    stats = consumer_module.run_poll_loop(
        consumer,
        store,
        config,
        max_polls=1,
        commit_payload=lambda item: {"offset": item.offset + 1},
    )

    assert stats.recovered_pending == 1
    assert stats.accepted == 1
    assert store.get_inbox(record.event_uid)["decision"] == "accepted"
    assert store.list_rows("rca_outbox")[0]["status"] == "shadow"
    assert len(consumer.poll_calls) == 1


def test_health_json_is_atomic_redacted_and_contains_no_payload(tmp_path):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="true",
    )
    store = RcaControlStore(config.control_db_path)
    store.ingest_record(
        KafkaRecord(topic=TOPIC, partition=0, offset=1, value=b"raw-secret-payload"),
        policy=config.policy,
    )
    reporter = consumer_module.HealthReporter(config, store)
    reporter.error("ingest", RuntimeError("top-secret-password raw-secret-payload"))
    reporter.write(state="error", stats=consumer_module.PollStats(), force=True)

    text = config.health_path.read_text(encoding="utf-8")
    body = json.loads(text)

    assert body["state"] == "error"
    assert body["last_error"]["type"] == "RuntimeError"
    assert body["schema_version"] == "pnc_rca_kafka_consumer_health_v2"
    assert body["healthy"] is False
    assert body["enabled"] is True
    assert body["activation_required"] is True
    assert body["config"]["activation_required"] is True
    assert "top-secret-password" not in text
    assert "raw-secret-payload" not in text
    assert body["config"]["external_dispatch_wired"] is False
    assert not list(tmp_path.glob(".health.json.*.tmp"))


def test_health_v2_binds_full_config_and_immutable_runtime_identity(tmp_path):
    config = _config(tmp_path)
    reporter = consumer_module.HealthReporter(config, RcaControlStore(config.control_db_path))
    reporter.set_assignment_reporter(
        lambda: {"assigned_partitions": [0], "callback_errors": 0}
    )

    reporter.write(state="idle", stats=consumer_module.PollStats(), force=True)
    first = json.loads(config.health_path.read_text(encoding="utf-8"))
    reporter.write(state="running", stats=consumer_module.PollStats(), force=True)
    second = json.loads(config.health_path.read_text(encoding="utf-8"))

    expected_config = config.public_dict()
    expected_config["policy"] = config.policy.to_dict()
    assert first["schema_version"] == "pnc_rca_kafka_consumer_health_v2"
    assert first["enabled"] is True
    assert first["healthy"] is True
    assert first["ok"] is True
    assert first["mode"] == "shadow"
    assert first["config"] == expected_config
    assert first["runtime_identity"]["service_label"] == (
        "local.pnc.rca-kafka-consumer"
    )
    assert first["runtime_identity"]["public_config_sha256"] == (
        consumer_module.canonical_json_sha256(expected_config)
    )
    assert len(first["runtime_identity"]["loaded_runtime_sha256"]) == 64
    assert first["runtime_identity"] == second["runtime_identity"]


def test_health_status_rejects_identity_without_loaded_runtime_digest(tmp_path):
    config = _config(tmp_path)
    reporter = consumer_module.HealthReporter(
        config, RcaControlStore(config.control_db_path)
    )
    reporter.set_assignment_reporter(
        lambda: {"assigned_partitions": [0], "callback_errors": 0}
    )
    reporter.write(state="idle", stats=consumer_module.PollStats(), force=True)
    payload = json.loads(config.health_path.read_text(encoding="utf-8"))
    payload["runtime_identity"].pop("loaded_runtime_sha256")
    config.health_path.write_text(json.dumps(payload), encoding="utf-8")

    status = consumer_module.read_health_status(config, max_age_seconds=60)

    assert status["ok"] is False
    assert status["health_check"]["reason"] == "consumer_reported_unhealthy"


def test_health_cannot_report_ok_while_any_partition_is_blocked(tmp_path):
    config = _config(tmp_path)
    reporter = consumer_module.HealthReporter(
        config, RcaControlStore(config.control_db_path)
    )
    reporter.set_assignment_reporter(
        lambda: {"assigned_partitions": [0, 1], "callback_errors": 0}
    )
    stats = consumer_module.PollStats(blocked_partitions=1)

    reporter.write(state="running", stats=stats, force=True)

    body = json.loads(config.health_path.read_text(encoding="utf-8"))
    assert body["ok"] is False
    assert body["stats"]["blocked_partitions"] == 1


def test_health_cli_requires_fresh_healthy_heartbeat(monkeypatch, tmp_path, capsys):
    env = _env(tmp_path)
    config = consumer_module.ConsumerConfig.from_env(env, hermes_home=tmp_path)
    reporter = consumer_module.HealthReporter(
        config, RcaControlStore(config.control_db_path)
    )
    reporter.set_assignment_reporter(
        lambda: {"assigned_partitions": [0], "callback_errors": 0}
    )
    reporter.write(state="idle", stats=consumer_module.PollStats(), force=True)
    for name, value in env.items():
        monkeypatch.setenv(name, value)

    rc = consumer_module.main(
        ["--health", "--health-max-age-seconds", "60", "--env-file", "/dev/null"]
    )
    body = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert body["ok"] is True
    assert body["health_check"]["fresh"] is True


def test_health_status_fails_stale_heartbeat(tmp_path):
    config = _config(tmp_path)
    reporter = consumer_module.HealthReporter(
        config, RcaControlStore(config.control_db_path)
    )
    reporter.set_assignment_reporter(
        lambda: {"assigned_partitions": [0], "callback_errors": 0}
    )
    reporter.write(state="idle", stats=consumer_module.PollStats(), force=True)
    payload = json.loads(config.health_path.read_text(encoding="utf-8"))
    payload["heartbeat_at"] = "2026-07-10T00:00:00+00:00"
    config.health_path.write_text(json.dumps(payload), encoding="utf-8")

    body = consumer_module.read_health_status(
        config,
        max_age_seconds=60,
        now=datetime(2026, 7, 10, 0, 2, tzinfo=timezone.utc),
    )

    assert body["ok"] is False
    assert body["health_check"]["fresh"] is False
    assert body["health_check"]["reason"] == "heartbeat_stale"


@pytest.mark.parametrize(
    ("future_seconds", "expected_ok", "expected_reason"),
    [
        (30, True, None),
        (31, False, "heartbeat_from_future"),
    ],
)
def test_health_status_bounds_future_heartbeat_clock_skew(
    tmp_path, future_seconds, expected_ok, expected_reason
):
    config = _config(tmp_path)
    reporter = consumer_module.HealthReporter(
        config, RcaControlStore(config.control_db_path)
    )
    reporter.set_assignment_reporter(
        lambda: {"assigned_partitions": [0], "callback_errors": 0}
    )
    reporter.write(state="idle", stats=consumer_module.PollStats(), force=True)
    payload = json.loads(config.health_path.read_text(encoding="utf-8"))
    now = datetime(2026, 7, 10, 0, 0, tzinfo=timezone.utc)
    payload["heartbeat_at"] = (now + timedelta(seconds=future_seconds)).isoformat()
    config.health_path.write_text(json.dumps(payload), encoding="utf-8")

    body = consumer_module.read_health_status(
        config,
        max_age_seconds=60,
        now=now,
    )

    assert body["ok"] is expected_ok
    assert body["health_check"]["fresh"] is expected_ok
    assert body["health_check"]["heartbeat_age_seconds"] == -future_seconds
    assert body["health_check"].get("reason") == expected_reason


def test_health_status_rejects_timezone_naive_heartbeat(tmp_path):
    config = _config(tmp_path)
    reporter = consumer_module.HealthReporter(
        config, RcaControlStore(config.control_db_path)
    )
    reporter.set_assignment_reporter(
        lambda: {"assigned_partitions": [0], "callback_errors": 0}
    )
    reporter.write(state="idle", stats=consumer_module.PollStats(), force=True)
    payload = json.loads(config.health_path.read_text(encoding="utf-8"))
    payload["heartbeat_at"] = "2026-07-10T00:00:00"
    config.health_path.write_text(json.dumps(payload), encoding="utf-8")

    body = consumer_module.read_health_status(
        config,
        max_age_seconds=60,
        now=datetime(2026, 7, 10, 0, 0, tzinfo=timezone.utc),
    )

    assert body["ok"] is False
    assert body["health_check"]["reason"] == "heartbeat_invalid"


def test_health_status_rejects_timezone_naive_observation_time(tmp_path):
    config = _config(tmp_path)
    reporter = consumer_module.HealthReporter(
        config, RcaControlStore(config.control_db_path)
    )
    reporter.set_assignment_reporter(
        lambda: {"assigned_partitions": [0], "callback_errors": 0}
    )
    reporter.write(state="idle", stats=consumer_module.PollStats(), force=True)

    body = consumer_module.read_health_status(
        config,
        max_age_seconds=60,
        now=datetime(2026, 7, 10, 0, 0),
    )

    assert body["ok"] is False
    assert body["health_check"]["reason"] == "health_observation_invalid"


def test_health_status_rejects_fresh_but_stopped_consumer(tmp_path):
    config = _config(tmp_path)
    reporter = consumer_module.HealthReporter(
        config, RcaControlStore(config.control_db_path)
    )
    reporter.set_assignment_reporter(
        lambda: {"assigned_partitions": [0], "callback_errors": 0}
    )
    reporter.write(state="idle", stats=consumer_module.PollStats(), force=True)
    payload = json.loads(config.health_path.read_text(encoding="utf-8"))
    payload["state"] = "stopped"
    payload["heartbeat_at"] = "2026-07-10T00:00:00+00:00"
    config.health_path.write_text(json.dumps(payload), encoding="utf-8")

    body = consumer_module.read_health_status(
        config,
        max_age_seconds=60,
        now=datetime(2026, 7, 10, 0, 0, 30, tzinfo=timezone.utc),
    )

    assert body["ok"] is False
    assert body["health_check"]["fresh"] is True
    assert body["health_check"]["reason"] == "consumer_reported_unhealthy"


def test_health_status_rejects_legacy_v1_schema(tmp_path):
    config = _config(tmp_path)
    reporter = consumer_module.HealthReporter(
        config, RcaControlStore(config.control_db_path)
    )
    reporter.set_assignment_reporter(
        lambda: {"assigned_partitions": [0], "callback_errors": 0}
    )
    reporter.write(state="idle", stats=consumer_module.PollStats(), force=True)
    payload = json.loads(config.health_path.read_text(encoding="utf-8"))
    payload["schema_version"] = "pnc_rca_kafka_consumer_health_v1"
    config.health_path.write_text(json.dumps(payload), encoding="utf-8")

    body = consumer_module.read_health_status(config, max_age_seconds=60)

    assert body["ok"] is False
    assert body["health_check"]["reason"] == "consumer_reported_unhealthy"


def test_shadow_outbox_is_not_promoted_when_config_later_enables_submit(tmp_path):
    shadow = _config(tmp_path)
    store = RcaControlStore(shadow.control_db_path)
    store.ingest_record(
        KafkaRecord(topic=TOPIC, partition=0, offset=1, value=_value()),
        policy=shadow.policy,
    )
    enabled = _config(tmp_path, HERMES_RCA_KAFKA_SUBMIT_ENABLED="true")
    consumer = FakeConsumer([{"partition-0": [_message(offset=1)]}])

    consumer_module.run_poll_loop(
        consumer,
        store,
        enabled,
        max_polls=1,
        commit_payload=lambda item: {"offset": item.offset + 1},
    )

    assert store.list_rows("rca_outbox")[0]["status"] == "shadow"
    assert store.list_rows("business_triggers")[0]["state"] == "shadow"


def test_check_config_does_not_create_or_connect_kafka(monkeypatch, tmp_path, capsys):
    for name, value in _env(tmp_path).items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(
        consumer_module,
        "create_consumer",
        lambda _config: (_ for _ in ()).throw(AssertionError("must not connect")),
    )

    rc = consumer_module.main([
        "--check-config",
        "--env-file",
        str(tmp_path / "missing.env"),
    ])
    output = capsys.readouterr()

    assert rc == 0
    payload = json.loads(output.out)
    assert payload["ok"] is True
    assert payload["config"]["policy"] == _config(tmp_path).policy.to_dict()
    assert "top-secret-password" not in output.out
    assert output.err == ""


def test_script_check_config_is_directly_executable_outside_repo(tmp_path):
    env = os.environ.copy()
    env.update(_env(tmp_path))
    env.pop("PYTHONPATH", None)
    script = Path(consumer_module.__file__).resolve()

    completed = subprocess.run(
        [sys.executable, str(script), "--check-config", "--env-file", "/dev/null"],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout)["ok"] is True
    assert "top-secret-password" not in completed.stdout


def test_main_recovers_pending_before_kafka_client_creation(monkeypatch, tmp_path):
    env = _env(tmp_path)
    config = consumer_module.ConsumerConfig.from_env(env, hermes_home=tmp_path)
    store = RcaControlStore(config.control_db_path)
    record = KafkaRecord(topic=TOPIC, partition=0, offset=8, value=_value())
    store.persist_raw(record, policy=config.policy)
    for name, value in env.items():
        monkeypatch.setenv(name, value)

    def fail_after_asserting_recovery(_config):
        assert (
            RcaControlStore(config.control_db_path).get_inbox(record.event_uid)[
                "decision"
            ]
            == "accepted"
        )
        raise RuntimeError("do not connect in unit test")

    monkeypatch.setattr(
        consumer_module, "create_consumer", fail_after_asserting_recovery
    )

    assert consumer_module.main(["--env-file", str(tmp_path / "missing.env")]) == 2
    health = json.loads(config.health_path.read_text(encoding="utf-8"))
    assert health["state"] == "error"
    assert health["last_error"]["phase"] == "consumer_create"
    assert store.list_rows("rca_outbox")[0]["status"] == "shadow"
