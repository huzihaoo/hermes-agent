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
    KafkaRecord,
    MANUAL_TRIGGER_SCHEMA_VERSION,
    ManualRcaTriggerRequest,
    RecordProcessingBlockedError,
    RcaControlStore,
)
from gateway.pnc_rca_kafka_contract import WorkflowEventPolicy, WorkflowTransition
from gateway.pnc_rca_delivery_store import RcaDeliveryStore
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


def _canonical_submit_updates(**updates):
    values = {
        "HERMES_RCA_KAFKA_CREATION_RULE_VERSION": "feishu-state-open-issue-v1",
        "HERMES_RCA_KAFKA_PROJECT_KEYS": "68ef617fb371dc80a10641f7",
        "HERMES_RCA_KAFKA_PROJECT_SIMPLE_NAMES": "t03o4q",
        "HERMES_RCA_KAFKA_WORK_ITEM_TYPE_KEYS": "issue",
        "HERMES_RCA_KAFKA_STATUS_CHANGE_TYPES": "",
        "HERMES_RCA_KAFKA_STATE_TRANSITIONS_JSON": "[]",
        "HERMES_RCA_KAFKA_SNAPSHOT_PATTERNS": "State",
        "HERMES_RCA_KAFKA_SNAPSHOT_SUB_STAGES": "OPEN",
        "HERMES_RCA_KAFKA_ALLOWED_PROJECT_OPTION_IDS": "6670325063",
        "HERMES_RCA_KAFKA_SUBMIT_ENABLED": "true",
    }
    values.update(updates)
    return values


def _g1q3_snapshot_value(*, work_item_id=7041712812):
    return json.dumps({
        "created_at": 1783650001000,
        "fields": [
            {"field_key": "field_052f23", "field_value": ["6670325063"]}
        ],
        "id": work_item_id,
        "name": "ACC braking issue",
        "pattern": "State",
        "project_key": "68ef617fb371dc80a10641f7",
        "project_simple_name": "t03o4q",
        "sub_stage": "OPEN",
        "updated_at": 1783650000000,
        "work_item_status": {"state_key": "open"},
        "work_item_type_key": "issue",
    }, sort_keys=True).encode()


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


def _activate_direct_steady(store, *, start_offset=20):
    return store.activate_direct_steady_epoch(
        epoch_id="rca-consumer-steady-20260817",
        release_fingerprint_sha256="1" * 64,
        release_note_sha256="2" * 64,
        config_sha256="3" * 64,
        db_logical_identity={"database": "consumer-test-control"},
        partition_start_fence={TOPIC: {"0": start_offset}},
        operator="consumer-test",
        reason="activate steady-only Kafka test",
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
    assert config.public_dict()["activation_mode"] == "steady_only"


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


def test_canonical_g1q3_consumer_requires_exact_project_option_allowlist(tmp_path):
    updates = {
        "HERMES_RCA_KAFKA_CREATION_RULE_VERSION": "feishu-state-open-issue-v1",
        "HERMES_RCA_KAFKA_PROJECT_KEYS": "68ef617fb371dc80a10641f7",
        "HERMES_RCA_KAFKA_PROJECT_SIMPLE_NAMES": "t03o4q",
        "HERMES_RCA_KAFKA_WORK_ITEM_TYPE_KEYS": "issue",
        "HERMES_RCA_KAFKA_STATUS_CHANGE_TYPES": "",
        "HERMES_RCA_KAFKA_STATE_TRANSITIONS_JSON": "[]",
        "HERMES_RCA_KAFKA_SNAPSHOT_PATTERNS": "State",
        "HERMES_RCA_KAFKA_SNAPSHOT_SUB_STAGES": "OPEN",
    }
    with pytest.raises(ValueError, match="exactly project option 6670325063"):
        _config(tmp_path, **updates)

    config = _config(
        tmp_path,
        **updates,
        HERMES_RCA_KAFKA_ALLOWED_PROJECT_OPTION_IDS="6670325063",
    )
    assert config.policy.allowed_project_option_ids == frozenset({"6670325063"})

    hybrid_updates = {
        **updates,
        "HERMES_RCA_KAFKA_ALLOWED_PROJECT_OPTION_IDS": "6670325063",
        "HERMES_RCA_KAFKA_STATUS_CHANGE_TYPES": "Reached",
        "HERMES_RCA_KAFKA_STATE_TRANSITIONS_JSON": json.dumps([
                {
                    "state_key": "new-problem-state",
                    "pre_status": 1,
                    "cur_status": 2,
                }
            ]),
    }
    with pytest.raises(ValueError, match="exactly project option 6670325063"):
        _config(tmp_path, **hybrid_updates)


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("HERMES_RCA_KAFKA_CREATION_RULE_VERSION", "drifted-policy-v1"),
        ("HERMES_RCA_KAFKA_TOPIC", "other-workflow-topic"),
    ],
)
def test_submit_enabled_rejects_noncanonical_policy_identity(tmp_path, name, value):
    updates = _canonical_submit_updates()
    updates[name] = value
    with pytest.raises(ValueError, match="snapshot-only"):
        _config(tmp_path, **updates)


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


def test_partition_assignment_accepts_current_activation_start_fence_skip(
    tmp_path,
):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_START_OFFSETS_JSON='{"0": 676}',
    )
    topic_partition = namedtuple("TopicPartition", "topic partition")
    partition = topic_partition(TOPIC, 0)

    class ActivationFenceConsumer:
        def __init__(self):
            self._coordinator = self
            self.seeks = []

        async def fetch_committed_offsets_async(self, _partitions, **_kwargs):
            return {partition: SimpleNamespace(offset=1578)}

        def seek(self, topic_partition, offset):
            self.seeks.append((topic_partition, offset))

    store = SimpleNamespace(
        partition_progress=lambda **_kwargs: {0: 975},
        activation_partition_start_fence=lambda **_kwargs: {0: 1578},
    )
    consumer = ActivationFenceConsumer()
    listener = consumer_module.ExplicitInitialOffsetListener(
        consumer, config, store
    )

    asyncio.run(listener.on_partitions_assigned([partition]))

    assert consumer.seeks == []
    assert listener.diagnostics()["assigned_partitions"] == [0]
    assert listener.diagnostics()["callback_errors"] == 0


def test_partition_assignment_seeks_activation_fence_when_group_commit_is_older(
    tmp_path,
):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_START_OFFSETS_JSON='{"0": 676}',
    )
    topic_partition = namedtuple("TopicPartition", "topic partition")
    partition = topic_partition(TOPIC, 0)

    class ActivationFenceConsumer:
        def __init__(self):
            self._coordinator = self
            self.seeks = []

        async def fetch_committed_offsets_async(self, _partitions, **_kwargs):
            return {partition: SimpleNamespace(offset=1822)}

        def seek(self, topic_partition, offset):
            self.seeks.append((topic_partition, offset))

    store = SimpleNamespace(
        partition_progress=lambda **_kwargs: {0: 1784},
        activation_partition_start_fence=lambda **_kwargs: {0: 1838},
    )
    consumer = ActivationFenceConsumer()
    listener = consumer_module.ExplicitInitialOffsetListener(
        consumer, config, store
    )

    asyncio.run(listener.on_partitions_assigned([partition]))

    assert consumer.seeks == [(partition, 1838)]
    assert listener.diagnostics()["assigned_partitions"] == [0]
    assert listener.diagnostics()["callback_errors"] == 0
    assert listener.diagnostics()["applied_t0_offsets"] == {"0": 1838}


def test_partition_assignment_blocks_group_commit_ahead_of_activation_fence(
    tmp_path,
):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_START_OFFSETS_JSON='{"0": 676}',
    )
    topic_partition = namedtuple("TopicPartition", "topic partition")
    partition = topic_partition(TOPIC, 0)

    class IncoherentConsumer:
        def __init__(self):
            self._coordinator = self
            self.seeks = []

        async def fetch_committed_offsets_async(self, _partitions, **_kwargs):
            return {partition: SimpleNamespace(offset=1840)}

        def seek(self, topic_partition, offset):
            self.seeks.append((topic_partition, offset))

    store = SimpleNamespace(
        partition_progress=lambda **_kwargs: {0: 1784},
        activation_partition_start_fence=lambda **_kwargs: {0: 1838},
    )
    consumer = IncoherentConsumer()
    listener = consumer_module.ExplicitInitialOffsetListener(
        consumer, config, store
    )

    with pytest.raises(RuntimeError, match="broker_local_offset_incoherent:0"):
        asyncio.run(listener.on_partitions_assigned([partition]))

    assert consumer.seeks == []
    assert listener.diagnostics()["callback_errors"] == 1
    assert listener.diagnostics()["assigned_partitions"] == []


def test_partition_assignment_uses_current_activation_start_fence_when_group_missing(
    tmp_path,
):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_START_OFFSETS_JSON='{"0": 676}',
    )
    topic_partition = namedtuple("TopicPartition", "topic partition")
    partition = topic_partition(TOPIC, 0)

    class ActivationFenceConsumer:
        def __init__(self):
            self._coordinator = self
            self.seeks = []

        async def fetch_committed_offsets_async(self, _partitions, **_kwargs):
            return {partition: SimpleNamespace(offset=-1)}

        def seek(self, topic_partition, offset):
            self.seeks.append((topic_partition, offset))

    store = SimpleNamespace(
        partition_progress=lambda **_kwargs: {0: 975},
        activation_partition_start_fence=lambda **_kwargs: {0: 1578},
    )
    consumer = ActivationFenceConsumer()
    listener = consumer_module.ExplicitInitialOffsetListener(
        consumer, config, store
    )

    asyncio.run(listener.on_partitions_assigned([partition]))

    assert consumer.seeks == [(partition, 1578)]
    assert listener.diagnostics()["assigned_partitions"] == [0]
    assert listener.diagnostics()["callback_errors"] == 0


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



def test_create_consumer_without_epoch_rejects_unsafe_false_flag_before_client(
    monkeypatch,
    tmp_path,
):
    config = _config(tmp_path, **_canonical_submit_updates(
        HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="false",
    ))
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
    config = _config(tmp_path, **_canonical_submit_updates(
        HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="false",
    ))
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


def test_submit_ingest_without_steady_epoch_fails_before_commit(tmp_path):
    config = _config(tmp_path, **_canonical_submit_updates(
        HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="true",
    ))
    store = RcaControlStore(config.control_db_path)
    consumer = FakeConsumer([{"partition-0": [_message(offset=20)]}])

    with pytest.raises(consumer_module.ConsumerLoopError, match="ingest failed"):
        consumer_module.run_poll_loop(
            consumer,
            store,
            config,
            max_polls=1,
            commit_payload=lambda item: {("tp", item.partition): item.offset + 1},
        )

    assert consumer.commits == []
    assert store.list_rows("rca_outbox") == []


def test_steady_activation_ingest_commits_and_binds_exact_ledger(tmp_path):
    config = _config(tmp_path, **_canonical_submit_updates(
        HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="true",
    ))
    store = RcaControlStore(config.control_db_path)
    epoch = _activate_direct_steady(store, start_offset=20)
    message = _message(
        offset=30,
        value=_g1q3_snapshot_value(work_item_id=7041712816),
    )
    consumer = FakeConsumer([{"partition-0": [message]}])

    stats = consumer_module.run_poll_loop(
        consumer,
        store,
        config,
        max_polls=1,
        commit_payload=lambda item: {("tp", item.partition): item.offset + 1},
    )

    [outbox] = store.list_rows("rca_outbox")
    [trigger] = store.list_rows("business_triggers")
    ledger = next(
        row
        for row in store.list_rows("rca_activation_admission_ledger")
        if row["decision"] == "admit"
    )
    assert stats.records_committed == 1
    assert consumer.commits == [{"offsets": {("tp", 0): 31}}]
    assert outbox["status"] == "pending"
    assert outbox["activation_epoch_id"] == epoch["epoch_id"]
    assert outbox["activation_ledger_id"] == ledger["ledger_id"]
    assert trigger["activation_epoch_id"] == epoch["epoch_id"]
    assert trigger["activation_ledger_id"] == ledger["ledger_id"]
    assert ledger["bound_at"] is not None


def test_historical_shadow_remains_readable_but_never_dispatchable(tmp_path):
    config = _config(tmp_path)
    store = RcaControlStore(config.control_db_path)
    store.ingest_record(
        KafkaRecord(topic=TOPIC, partition=0, offset=1, value=_value()),
        policy=config.policy,
    )
    _activate_direct_steady(store)

    [historical] = store.list_rows("rca_outbox")
    assert historical["status"] == "shadow"
    assert historical["activation_epoch_id"] is None
    assert store.preview_dispatchable() == []
    assert store.claim_outbox(lease_owner="steady-consumer-test") is None
    assert store.health()["activation"]["dispatchable_backlog"] == 0


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
    assert body["schema_version"] == "pnc_rca_kafka_consumer_health_v3"
    assert body["healthy"] is False
    assert body["enabled"] is True
    assert body["activation_required"] is True
    assert body["config"]["activation_required"] is True
    assert "top-secret-password" not in text
    assert "raw-secret-payload" not in text
    assert body["config"]["external_dispatch_wired"] is False
    assert not list(tmp_path.glob(".health.json.*.tmp"))


def test_health_v3_binds_full_config_and_immutable_runtime_identity(tmp_path):
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
    assert first["schema_version"] == "pnc_rca_kafka_consumer_health_v3"
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


def test_activation_required_health_is_red_without_current_epoch(tmp_path):
    config = _config(
        tmp_path,
        HERMES_RCA_KAFKA_ACTIVATION_REQUIRED="true",
    )
    store = RcaControlStore(config.control_db_path)
    reporter = consumer_module.HealthReporter(config, store)
    reporter.set_assignment_reporter(
        lambda: {"assigned_partitions": [0], "callback_errors": 0}
    )

    reporter.write(state="idle", stats=consumer_module.PollStats(), force=True)

    body = json.loads(config.health_path.read_text(encoding="utf-8"))
    assert body["store"]["ok"] is True
    assert body["store"]["activation"]["configured"] is False
    assert body["store"]["activation"]["production_active"] is False
    assert body["ok"] is False
    assert body["healthy"] is False


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
