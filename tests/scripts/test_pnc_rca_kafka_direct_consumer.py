"""Focused tests for the additive, transport-neutral Kafka runner."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import NamedTuple

import pytest

from gateway.pnc_rca_kafka_contract import WorkflowEventPolicy, WorkflowTransition
from gateway.pnc_rca_mini_store import (
    MiniIngestResult,
    MiniKafkaRecord,
    MiniRecordConflictError,
    MiniStore,
)
from scripts import pnc_rca_kafka_direct_consumer as direct


TOPIC = "feishu-project-workflow-event"


def _policy() -> WorkflowEventPolicy:
    return WorkflowEventPolicy(
        topic=TOPIC,
        policy_version="issue-created-v1",
        project_keys=frozenset({"project-key"}),
        project_simple_names=frozenset({"g1q3"}),
        work_item_type_keys=frozenset({"problem-type"}),
        status_change_types=frozenset({"Reached"}),
        transitions=(WorkflowTransition("new-problem-state", 1, 2),),
    )


def _value(
    *, work_item_id: int = 7041712812, title: str = "ACC braking issue"
) -> bytes:
    return json.dumps(
        {
            "id": work_item_id,
            "name": title,
            "nodes": [
                {
                    "state_key": "new-problem-state",
                    "pre_status": 1,
                    "cur_status": 2,
                }
            ],
            "project_key": "project-key",
            "project_simple_name": "g1q3",
            "status_change_type": "Reached",
            "updated_at": 1783650000000,
            "work_item_type_key": "problem-type",
        },
        separators=(",", ":"),
    ).encode()


def _message(
    offset: int = 10,
    *,
    partition: int = 0,
    value: bytes | None = None,
    work_item_id: int = 7041712812,
):
    return SimpleNamespace(
        topic=TOPIC,
        partition=partition,
        offset=offset,
        value=value if value is not None else _value(work_item_id=work_item_id),
        key=b"source-key",
        timestamp=1783650000000,
        headers=(("trace", b"t-1"), ("empty", None)),
    )


def _config(tmp_path: Path, **updates) -> direct.DirectConsumerConfig:
    values = {
        "topic": TOPIC,
        "policy": _policy(),
        "poll_timeout_ms": 25,
        "max_poll_records": 10,
        "health_path": tmp_path / "health.json",
    }
    values.update(updates)
    return direct.DirectConsumerConfig(**values)


@pytest.fixture()
def store(tmp_path: Path) -> MiniStore:
    return MiniStore(tmp_path / "mini.sqlite3")


class FakeConsumer:
    def __init__(self, batches=(), *, assignment=()):
        self.batches = list(batches)
        self._assignment = tuple(assignment)
        self.poll_calls = []
        self.commits = []
        self.seek_calls = []
        self.positions = {}

    def poll(self, **kwargs):
        self.poll_calls.append(kwargs)
        return self.batches.pop(0) if self.batches else {}

    def commit(self, **kwargs):
        self.commits.append(kwargs)

    def assignment(self):
        return self._assignment

    def position(self, partition):
        return self.positions.get(partition)

    def seek(self, partition, offset):
        self.seek_calls.append((partition, offset))
        self.positions[partition] = offset


class TopicPartition(NamedTuple):
    topic: str
    partition: int


def _tp(partition: int = 0, topic: str = TOPIC):
    return TopicPartition(topic, partition)


def test_record_adapter_accepts_object_and_mapping():
    object_record = direct.record_from_message(_message(7))
    mapping_record = direct.record_from_message({
        "topic": TOPIC,
        "partition": 2,
        "offset": 8,
        "value": "text",
        "timestamp_ms": 9,
        "headers": (("name", "value"),),
    })

    assert object_record.event_uid == f"{TOPIC}:0:7"
    assert object_record.timestamp_ms == 1783650000000
    assert mapping_record.value == b"text"
    assert mapping_record.headers == (("name", b"value"),)


def test_config_is_non_secret_and_has_explicit_t0(tmp_path: Path):
    config = _config(tmp_path, initial_offsets={"2": 120, 0: 99})

    assert config.initial_offset_for(0) == 99
    assert config.initial_offset_for(2) == 120
    assert config.initial_offset_for(1) is None
    assert config.public_dict()["initial_offsets"] == {"0": 99, "2": 120}
    assert config.public_dict()["policy"] == _policy().to_dict()


def test_config_requires_policy_topic_match(tmp_path: Path):
    with pytest.raises(ValueError, match="policy.topic"):
        direct.DirectConsumerConfig(topic="other", policy=_policy())


def test_default_commit_payload_advances_exactly_one_offset():
    assert direct.default_commit_payload(_message(41, partition=2)) == {(TOPIC, 2): 42}


def test_raw_inbox_progress_and_outbox_exist_before_commit(
    tmp_path: Path, store: MiniStore
):
    message = _message(10)
    consumer = FakeConsumer([{_tp(): [message]}])
    observations = []

    def commit_callback(_consumer, record, result):
        inbox = store.get_inbox(result.event_uid)
        observations.append((
            inbox["decision"],
            bytes(inbox["raw_value"]),
            store.partition_progress(topic=TOPIC, partitions=[0]),
            len(store.list_rows("business_triggers")),
            len(store.list_rows("rca_outbox")),
            store.ack_safe(result.event_uid),
            record.offset,
        ))

    stats = direct.run_poll_loop(
        consumer,
        store,
        _config(tmp_path),
        max_polls=1,
        commit_callback=commit_callback,
    )

    assert observations == [("accepted", message.value, {0: 11}, 1, 1, True, 10)]
    assert stats.records_seen == stats.records_committed == 1


def test_ack_unsafe_stops_before_transport_commit(tmp_path: Path):
    class UnsafeStore:
        def process_pending(self, **_kwargs):
            return []

        def ingest_record(self, record, **_kwargs):
            return MiniIngestResult(
                event_uid=record.event_uid,
                decision="accepted",
                reason="test",
                ack_safe=False,
            )

        def ack_safe(self, _event_uid):
            return False

    consumer = FakeConsumer([{_tp(): [_message(10)]}])
    with pytest.raises(direct.AckSafetyError, match="durable_progress_missing"):
        direct.run_poll_loop(consumer, UnsafeStore(), _config(tmp_path), max_polls=1)

    assert consumer.commits == []


def test_second_ack_check_stops_before_transport_commit(tmp_path: Path):
    class StaleProgressStore:
        def process_pending(self, **_kwargs):
            return []

        def ingest_record(self, record, **_kwargs):
            return MiniIngestResult(
                event_uid=record.event_uid,
                decision="accepted",
                reason="test",
                ack_safe=True,
            )

        def ack_safe(self, _event_uid):
            return False

    consumer = FakeConsumer([{_tp(): [_message(10)]}])
    with pytest.raises(direct.AckSafetyError, match="check_failed"):
        direct.run_poll_loop(
            consumer, StaleProgressStore(), _config(tmp_path), max_polls=1
        )
    assert consumer.commits == []


def test_ingest_error_stops_before_later_partition_record(tmp_path: Path):
    class FailingStore:
        def process_pending(self, **_kwargs):
            return []

        def ingest_record(self, *_args, **_kwargs):
            raise RuntimeError("storage unavailable")

    consumer = FakeConsumer([{_tp(): [_message(1), _message(2)]}])
    stats = direct.DirectPollStats()
    with pytest.raises(RuntimeError, match="storage unavailable"):
        direct.run_poll_loop(
            consumer,
            FailingStore(),
            _config(tmp_path),
            max_polls=1,
            stats=stats,
        )
    assert stats.records_seen == stats.ingest_errors == 1
    assert stats.records_committed == 0
    assert consumer.commits == []


def test_pending_recovery_finishes_before_first_poll(tmp_path: Path, store: MiniStore):
    raw = store.persist_raw(_message(7), policy=_policy())

    class ObservingConsumer(FakeConsumer):
        def poll(self, **kwargs):
            assert store.get_inbox(raw.event_uid)["decision"] == "accepted"
            assert store.partition_progress(topic=TOPIC, partitions=[0]) == {0: 8}
            return super().poll(**kwargs)

    consumer = ObservingConsumer([{}])
    stats = direct.run_poll_loop(consumer, store, _config(tmp_path), max_polls=1)

    assert stats.recovered_pending == 1
    assert stats.accepted == 1
    assert store.pending_event_uids() == []


def test_pending_recovery_paginates(store: MiniStore):
    for offset in range(3):
        store.persist_raw(_message(offset, work_item_id=offset + 1), policy=_policy())

    stats = direct.DirectPollStats()
    recovered = direct.recover_pending(store, batch_size=2, stats=stats)

    assert len(recovered) == stats.recovered_pending == 3
    assert store.partition_progress(topic=TOPIC, partitions=[0]) == {0: 3}


def test_transport_duplicate_is_committed_without_new_business_row(
    tmp_path: Path, store: MiniStore
):
    message = _message(10)
    store.ingest_record(message, policy=_policy())
    consumer = FakeConsumer([{_tp(): [message]}])

    stats = direct.run_poll_loop(consumer, store, _config(tmp_path), max_polls=1)

    assert stats.transport_duplicates == 1
    assert stats.records_committed == 1
    assert len(store.list_rows("business_triggers")) == 1
    assert len(store.list_rows("rca_outbox")) == 1
    assert consumer.commits[0]["offsets"] == {(TOPIC, 0): 11}


def test_payload_hash_conflict_is_fatal_without_commit(
    tmp_path: Path, store: MiniStore
):
    store.persist_raw(_message(10), policy=_policy())
    consumer = FakeConsumer([
        {_tp(): [_message(10, value=_value(title="changed bytes"))]}
    ])
    stats = direct.DirectPollStats()

    with pytest.raises(MiniRecordConflictError, match="payload hash conflict"):
        direct.run_poll_loop(
            consumer,
            store,
            _config(tmp_path),
            max_polls=1,
            stats=stats,
            recover_on_start=False,
        )

    assert stats.conflicts == 1
    assert consumer.commits == []
    assert store.get_inbox(f"{TOPIC}:0:10")["decision"] == "pending"


@pytest.mark.parametrize(
    ("value", "field"),
    [
        (b"not-json", "invalid"),
        (json.dumps({"id": 1, "name": "x"}).encode(), "filtered"),
    ],
)
def test_terminal_non_business_decisions_are_ack_safe(
    tmp_path: Path, store: MiniStore, value: bytes, field: str
):
    consumer = FakeConsumer([{_tp(): [_message(10, value=value)]}])
    stats = direct.run_poll_loop(consumer, store, _config(tmp_path), max_polls=1)

    assert getattr(stats, field) == 1
    assert stats.records_committed == 1
    assert store.list_rows("business_triggers") == []
    assert store.list_rows("rca_outbox") == []


def test_business_duplicate_at_later_offset_is_deduped(
    tmp_path: Path, store: MiniStore
):
    consumer = FakeConsumer([{_tp(): [_message(10), _message(11)]}])
    stats = direct.run_poll_loop(consumer, store, _config(tmp_path), max_polls=1)

    assert stats.accepted == stats.deduped == 1
    assert stats.records_committed == 2
    assert store.partition_progress(topic=TOPIC, partitions=[0]) == {0: 12}
    assert len(store.list_rows("rca_outbox")) == 1


def test_commit_failure_leaves_durable_progress_for_replay(
    tmp_path: Path, store: MiniStore
):
    class CommitFailingConsumer(FakeConsumer):
        def commit(self, **_kwargs):
            raise RuntimeError("broker unavailable")

    consumer = CommitFailingConsumer([{_tp(): [_message(10)]}])
    stats = direct.DirectPollStats()
    with pytest.raises(RuntimeError, match="broker unavailable"):
        direct.run_poll_loop(
            consumer,
            store,
            _config(tmp_path),
            max_polls=1,
            stats=stats,
        )

    assert stats.commit_errors == 1
    assert stats.records_committed == 0
    assert store.partition_progress(topic=TOPIC, partitions=[0]) == {0: 11}
    assert store.get_inbox(f"{TOPIC}:0:10")["decision"] == "accepted"


def test_offset_resolution_uses_t0_without_other_evidence(
    store: MiniStore,
):
    resolution = direct.DirectOffsetCoordinator(
        store, topic=TOPIC, initial_offsets={0: 120}
    ).resolve(0)

    assert resolution.source == "t0"
    assert resolution.seek_offset == 120
    assert resolution.committed_offset is None


def test_offset_resolution_requires_explicit_start(store: MiniStore):
    with pytest.raises(direct.OffsetCoherenceError, match="initial_offset_missing:0"):
        direct.DirectOffsetCoordinator(store, topic=TOPIC).resolve(0)


def test_offset_resolution_uses_durable_progress_when_uncommitted(
    store: MiniStore,
):
    store.ingest_record(_message(10), policy=_policy())
    resolution = direct.DirectOffsetCoordinator(store, topic=TOPIC).resolve(0)

    assert resolution.source == "durable_progress"
    assert resolution.seek_offset == 11


def test_offset_resolution_accepts_coherent_committed_offset(store: MiniStore):
    store.ingest_record(_message(10), policy=_policy())
    provider = direct.MappingOffsetProvider(
        committed={(TOPIC, 0): SimpleNamespace(offset=11)},
        positions={(TOPIC, 0): 9},
    )
    resolution = direct.DirectOffsetCoordinator(
        store, topic=TOPIC, provider=provider
    ).resolve(0)

    assert resolution.source == "committed"
    assert resolution.seek_offset == 11
    assert resolution.broker_position == 9


def test_offset_resolution_replays_when_commit_trails_durable_progress(
    store: MiniStore,
):
    store.ingest_record(_message(10), policy=_policy())
    provider = direct.MappingOffsetProvider(committed={0: 10})
    resolution = direct.DirectOffsetCoordinator(
        store, topic=TOPIC, provider=provider
    ).resolve(0)

    assert resolution.source == "committed"
    assert resolution.seek_offset == 10
    assert resolution.durable_next_offset == 11


def test_offset_resolution_rejects_broker_ahead_of_durable_progress(
    store: MiniStore,
):
    store.ingest_record(_message(10), policy=_policy())
    provider = direct.MappingOffsetProvider(committed={0: 12})

    with pytest.raises(direct.OffsetCoherenceError, match="incoherent:0"):
        direct.DirectOffsetCoordinator(store, topic=TOPIC, provider=provider).resolve(0)


def test_offset_resolution_rejects_committed_before_explicit_t0(store: MiniStore):
    provider = direct.MappingOffsetProvider(committed={0: 9})

    with pytest.raises(direct.OffsetCoherenceError, match="incoherent:0"):
        direct.DirectOffsetCoordinator(
            store,
            topic=TOPIC,
            provider=provider,
            initial_offsets={0: 10},
        ).resolve(0)


def test_offset_provider_is_injected_and_assignment_is_sought_once(
    tmp_path: Path, store: MiniStore
):
    partition = _tp()
    consumer = FakeConsumer([{}, {}], assignment=(partition,))
    coordinator = direct.DirectOffsetCoordinator(
        store,
        topic=TOPIC,
        provider=direct.MappingOffsetProvider(committed={0: None}),
        initial_offsets={0: 120},
    )

    direct.run_poll_loop(
        consumer,
        store,
        _config(tmp_path),
        coordinator=coordinator,
        max_polls=2,
    )

    assert consumer.seek_calls == [(partition, 120)]


def test_assignment_rejects_unexpected_topic(store: MiniStore):
    coordinator = direct.DirectOffsetCoordinator(
        store, topic=TOPIC, initial_offsets={0: 10}
    )

    with pytest.raises(direct.OffsetCoherenceError, match="unexpected_topic"):
        coordinator.resolve_assignment((_tp(topic="other"),))


def test_partition_offsets_must_not_decrease_within_batch(
    tmp_path: Path, store: MiniStore
):
    consumer = FakeConsumer([{_tp(): [_message(11), _message(10)]}])

    with pytest.raises(direct.PollOrderError, match="offset_decreased"):
        direct.run_poll_loop(consumer, store, _config(tmp_path), max_polls=1)

    assert consumer.commits == []
    assert store.list_rows("kafka_inbox") == []


def test_idle_poll_is_bounded_and_observed(tmp_path: Path, store: MiniStore):
    consumer = FakeConsumer([{}, {}])
    stats = direct.run_poll_loop(consumer, store, _config(tmp_path), max_polls=2)

    assert stats.polls == stats.idle_polls == 2
    assert len(consumer.poll_calls) == 2
    body = json.loads((tmp_path / "health.json").read_text(encoding="utf-8"))
    assert body["state"] == "stopped"


def test_health_is_atomic_payload_free_and_has_no_legacy_gate_fields(
    tmp_path: Path,
):
    config = _config(tmp_path)
    reporter = direct.DirectHealthReporter(config.health_path, config=config)
    reporter.error("ingest", RuntimeError("secret raw payload"))
    body = reporter.write(state="error", stats=direct.DirectPollStats())
    text = config.health_path.read_text(encoding="utf-8")

    assert body["schema_version"] == direct.DIRECT_HEALTH_SCHEMA_VERSION
    assert body["healthy"] is False
    assert body["last_error"]["type"] == "RuntimeError"
    assert "secret raw payload" not in text
    assert not list(tmp_path.glob(".health.json.*.tmp"))
    lowered = text.lower()
    for forbidden in ("activation", "release", '"w3"'):
        assert forbidden not in lowered


def test_poll_failure_is_typed_in_health_without_error_detail(
    tmp_path: Path, store: MiniStore
):
    class FailingConsumer(FakeConsumer):
        def poll(self, **_kwargs):
            raise RuntimeError("password=secret raw payload")

    with pytest.raises(RuntimeError, match="password=secret"):
        direct.run_poll_loop(FailingConsumer(), store, _config(tmp_path), max_polls=1)

    text = (tmp_path / "health.json").read_text(encoding="utf-8")
    body = json.loads(text)
    assert body["state"] == "error"
    assert body["last_error"]["phase"] == "poll"
    assert body["last_error"]["type"] == "RuntimeError"
    assert "password=secret" not in text


def test_wrapper_exposes_same_bounded_runner(tmp_path: Path, store: MiniStore):
    runner = direct.DirectConsumer(FakeConsumer([{}]), store, _config(tmp_path))

    stats = runner.run(max_polls=1)

    assert stats.polls == stats.idle_polls == 1
    assert direct.run_direct_consumer is direct.run_poll_loop


def _direct_env(tmp_path: Path, **updates: str) -> dict[str, str]:
    values = {
        f"{direct.DIRECT_ENV_PREFIX}BOOTSTRAP_SERVERS": "direct-a:9092,direct-b:9092",
        f"{direct.DIRECT_ENV_PREFIX}TOPIC": TOPIC,
        f"{direct.DIRECT_ENV_PREFIX}GROUP_ID": "rca-direct-independent",
        f"{direct.DIRECT_ENV_PREFIX}SASL_USERNAME": "direct-user",
        f"{direct.DIRECT_ENV_PREFIX}SASL_PASSWORD": "direct-password",
        f"{direct.DIRECT_ENV_PREFIX}SECURITY_PROTOCOL": "SASL_PLAINTEXT",
        f"{direct.DIRECT_ENV_PREFIX}SASL_MECHANISM": "PLAIN",
        f"{direct.DIRECT_ENV_PREFIX}T0_OFFSETS_JSON": '{"0": 10}',
        f"{direct.DIRECT_ENV_PREFIX}DB_PATH": str(tmp_path / "direct-mini.sqlite3"),
        f"{direct.DIRECT_ENV_PREFIX}HEALTH_PATH": str(tmp_path / "direct-health.json"),
        f"{direct.DIRECT_ENV_PREFIX}POLICY_JSON": json.dumps(_policy().to_dict()),
        f"{direct.DIRECT_ENV_PREFIX}COMMIT_ENABLED": "true",
    }
    values.update(updates)
    return values


def test_direct_config_isolated_env_and_redacted_public_contract(tmp_path: Path):
    config = direct.DirectKafkaConfig.from_env(
        _direct_env(tmp_path), hermes_home=tmp_path
    )

    assert config.topic == TOPIC
    assert config.group_id == "rca-direct-independent"
    assert config.initial_offsets == {0: 10}
    assert config.db_path.name == "direct-mini.sqlite3"
    assert config.health_path.name == "direct-health.json"
    assert config.public_dict()["commit_enabled"] is True
    public = json.dumps(config.public_dict(), sort_keys=True)
    assert "direct-password" not in public
    assert "CONTROL_DB_PATH" not in public
    assert "activation" not in public.lower()


def test_direct_config_rejects_shadow_default_group(tmp_path: Path):
    env = _direct_env(
        tmp_path,
        **{f"{direct.DIRECT_ENV_PREFIX}COMMIT_ENABLED": "false"},
        **{f"{direct.DIRECT_ENV_PREFIX}GROUP_ID": direct.DIRECT_DEFAULT_GROUP_ID},
    )

    with pytest.raises(ValueError, match="isolated group"):
        direct.DirectKafkaConfig.from_env(env, hermes_home=tmp_path)


def test_direct_config_rejects_legacy_production_group(tmp_path: Path):
    env = _direct_env(
        tmp_path,
        **{f"{direct.DIRECT_ENV_PREFIX}GROUP_ID": "rca_root_cause_analysis_agent"},
    )

    with pytest.raises(ValueError, match="legacy Kafka group"):
        direct.DirectKafkaConfig.from_env(env, hermes_home=tmp_path)


@pytest.mark.parametrize("suffix", ["GROUP_ID", "COMMIT_ENABLED"])
def test_direct_cli_config_requires_explicit_group_and_commit_mode(
    tmp_path: Path, suffix: str
):
    env = _direct_env(tmp_path)
    env.pop(f"{direct.DIRECT_ENV_PREFIX}{suffix}")

    with pytest.raises(ValueError, match=suffix):
        direct.DirectKafkaConfig.from_env(env, hermes_home=tmp_path)


def test_direct_kafka_factory_builds_kwargs_and_exact_subscription(
    monkeypatch, tmp_path
):
    captured = {}
    ListenerBase = type("ConsumerRebalanceListener", (), {})

    class TopicPartition:
        def __init__(self, topic, partition):
            self.topic = topic
            self.partition = partition

        def __hash__(self):
            return hash((self.topic, self.partition))

        def __eq__(self, other):
            return (
                getattr(other, "topic", None),
                getattr(other, "partition", None),
            ) == (self.topic, self.partition)

    class OffsetAndMetadata:
        def __init__(self, offset, metadata="", *args):
            self.offset = offset
            self.metadata = metadata

    class FakeRaw:
        def __init__(self, **kwargs):
            captured["kwargs"] = kwargs
            captured["raw"] = self
            self.subscriptions = []
            self.commits = []

        def subscribe(self, *, topics, listener=None):
            assert isinstance(listener, ListenerBase)
            captured["listener"] = listener
            self.subscriptions.append(topics)

        def close(self):
            captured["closed"] = True

    module = SimpleNamespace(
        KafkaConsumer=FakeRaw,
        TopicPartition=TopicPartition,
        OffsetAndMetadata=OffsetAndMetadata,
        ConsumerRebalanceListener=ListenerBase,
    )
    monkeypatch.setattr(direct.importlib, "import_module", lambda name: module)
    config = direct.DirectKafkaConfig.from_env(
        _direct_env(tmp_path), hermes_home=tmp_path
    )

    consumer = direct.create_kafka_consumer(config)

    assert consumer.raw.subscriptions == [(TOPIC,)]
    assert captured["kwargs"]["bootstrap_servers"] == ["direct-a:9092", "direct-b:9092"]
    assert captured["kwargs"]["group_id"] == "rca-direct-independent"
    assert captured["kwargs"]["sasl_plain_username"] == "direct-user"
    assert captured["kwargs"]["sasl_plain_password"] == "direct-password"
    assert captured["kwargs"]["enable_auto_commit"] is False
    assert "consumer_timeout_ms" not in captured["kwargs"]
    assert "offset_lookup_timeout_ms" not in captured["kwargs"]
    assert "recovery_batch_size" not in captured["kwargs"]
    assert isinstance(captured["listener"], ListenerBase)


def test_factory_uses_timeout_ms_and_fetch_cap(monkeypatch, tmp_path):
    env = _direct_env(
        tmp_path,
        **{f"{direct.DIRECT_ENV_PREFIX}MAX_POLL_RECORDS": "10000"},
        **{f"{direct.DIRECT_ENV_PREFIX}OFFSET_LOOKUP_TIMEOUT_MS": "4321"},
    )
    config = direct.DirectKafkaConfig.from_env(env, hermes_home=tmp_path)
    calls = []
    ListenerBase = type("ConsumerRebalanceListener", (), {})

    class Raw:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def subscribe(self, *, topics, listener):
            assert isinstance(listener, ListenerBase)

        def committed(self, _partition, **kwargs):
            calls.append(kwargs)
            return None

    module = SimpleNamespace(KafkaConsumer=Raw, ConsumerRebalanceListener=ListenerBase)
    monkeypatch.setattr(direct.importlib, "import_module", lambda _name: module)
    consumer = direct.create_kafka_consumer(config)
    consumer.committed(TOPIC, 0)

    assert consumer.raw.kwargs["fetch_max_bytes"] == direct.DIRECT_MAX_FETCH_BYTES
    assert calls == [{"timeout_ms": 4321}]


def test_adapter_converts_broker_neutral_commit_payload(tmp_path: Path, monkeypatch):
    class TopicPartition:
        def __init__(self, topic, partition):
            self.topic = topic
            self.partition = partition

        def __hash__(self):
            return hash((self.topic, self.partition))

        def __eq__(self, other):
            return (self.topic, self.partition) == (
                getattr(other, "topic", None),
                getattr(other, "partition", None),
            )

    class OffsetAndMetadata:
        def __init__(self, offset, metadata="", *args):
            self.offset = offset
            self.metadata = metadata

    class Raw:
        def __init__(self):
            self.commits = []

        def commit(self, **kwargs):
            self.commits.append(kwargs)

    raw = Raw()
    adapter = direct.KafkaPythonConsumerAdapter(
        raw,
        SimpleNamespace(
            TopicPartition=TopicPartition, OffsetAndMetadata=OffsetAndMetadata
        ),
    )
    adapter.commit(offsets={(TOPIC, 0): 11})

    assert len(raw.commits) == 1
    key, value = next(iter(raw.commits[0]["offsets"].items()))
    assert (key.topic, key.partition) == (TOPIC, 0)
    assert value.offset == 11


def test_assignment_listener_seeks_explicit_t0_before_first_fetch(tmp_path: Path):
    class TopicPartition:
        def __init__(self, topic, partition):
            self.topic = topic
            self.partition = partition

        def __hash__(self):
            return hash((self.topic, self.partition))

        def __eq__(self, other):
            return (self.topic, self.partition) == (
                getattr(other, "topic", None),
                getattr(other, "partition", None),
            )

    class Raw:
        def __init__(self):
            self.seeks = []

        def committed(self, _partition, **_kwargs):
            raise AssertionError("rebalance callback must not query committed offset")

        def position(self, _partition):
            raise AssertionError("rebalance callback must not query position")

        def seek(self, partition, offset):
            self.seeks.append((partition, offset))

    raw = Raw()
    adapter = direct.KafkaPythonConsumerAdapter(
        raw,
        SimpleNamespace(
            TopicPartition=TopicPartition,
            ConsumerRebalanceListener=type("ConsumerRebalanceListener", (), {}),
        ),
    )
    store = MiniStore(tmp_path / "mini.sqlite3")
    coordinator = direct.DirectOffsetCoordinator(
        store,
        topic=TOPIC,
        provider=adapter,
        initial_offsets={0: 10},
    )
    adapter.set_coordinator(coordinator)
    partition = TopicPartition(TOPIC, 0)
    adapter.rebalance_listener().on_partitions_assigned((partition,))

    assert [(item.topic, item.partition, offset) for item, offset in raw.seeks] == [
        (TOPIC, 0, 10)
    ]


def test_assignment_listener_without_t0_defers_to_main_poll_loop(tmp_path: Path):
    class TopicPartition:
        def __init__(self, topic, partition):
            self.topic = topic
            self.partition = partition

    class Raw:
        def committed(self, _partition, **_kwargs):
            raise AssertionError("rebalance callback must not query committed offset")

        def position(self, _partition):
            raise AssertionError("rebalance callback must not query position")

        def seek(self, _partition, _offset):
            raise AssertionError("no explicit T0 means the callback must not seek")

    adapter = direct.KafkaPythonConsumerAdapter(
        Raw(),
        SimpleNamespace(
            TopicPartition=TopicPartition,
            ConsumerRebalanceListener=type("ConsumerRebalanceListener", (), {}),
        ),
    )
    coordinator = direct.DirectOffsetCoordinator(
        MiniStore(tmp_path / "mini.sqlite3"),
        topic=TOPIC,
        provider=adapter,
    )
    adapter.set_coordinator(coordinator)

    adapter.rebalance_listener().on_partitions_assigned((TopicPartition(TOPIC, 0),))


def test_shadow_runner_never_calls_commit(tmp_path: Path, store: MiniStore):
    config = _config(
        tmp_path,
        group_id="rca-direct-shadow",
        commit_enabled=False,
    )
    consumer = FakeConsumer([{_tp(): [_message(10)]}])

    stats = direct.run_poll_loop(consumer, store, config, max_polls=1)

    assert consumer.commits == []
    assert stats.records_seen == 1
    assert stats.records_committed == 0
    assert stats.commits_skipped == 1


def test_check_config_is_read_only_and_does_not_construct_store_or_kafka(
    monkeypatch, tmp_path: Path, capsys
):
    env = _direct_env(tmp_path)
    monkeypatch.setattr(direct.os, "environ", env)
    monkeypatch.setattr(
        direct,
        "MiniStore",
        lambda *_args, **_kwargs: pytest.fail("check-config opened MiniStore"),
    )
    monkeypatch.setattr(
        direct,
        "create_kafka_consumer",
        lambda *_args, **_kwargs: pytest.fail("check-config created Kafka"),
    )

    assert direct.main(["--check-config"]) == 0
    output = capsys.readouterr()
    payload = json.loads(output.out)
    assert payload["ok"] is True
    assert payload["mode"] == "check-config"
    assert payload["config"]["group_id"] == "rca-direct-independent"
    assert "direct-password" not in output.out
    assert output.err == ""


def test_dry_run_is_read_only(monkeypatch, tmp_path: Path, capsys):
    env = _direct_env(tmp_path)
    monkeypatch.setattr(direct.os, "environ", env)
    monkeypatch.setattr(
        direct,
        "MiniStore",
        lambda *_args, **_kwargs: pytest.fail("dry-run opened MiniStore"),
    )
    monkeypatch.setattr(
        direct,
        "create_kafka_consumer",
        lambda *_args, **_kwargs: pytest.fail("dry-run created Kafka"),
    )

    assert direct.main(["--dry-run"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert payload["mode"] == "dry-run"
    assert payload["validation_scope"] == "config_only_no_db_or_kafka"


def test_kafka_factory_fails_closed_when_listener_contract_is_missing(
    monkeypatch, tmp_path: Path
):
    created = {}

    class Raw:
        def __init__(self, **_kwargs):
            created["raw"] = self
            self.closed = False

        def subscribe(self, **_kwargs):
            raise AssertionError("subscribe must not run without listener base")

        def close(self):
            self.closed = True

    module = SimpleNamespace(KafkaConsumer=Raw)
    monkeypatch.setattr(direct.importlib, "import_module", lambda _name: module)
    config = direct.DirectKafkaConfig.from_env(
        _direct_env(tmp_path), hermes_home=tmp_path
    )

    with pytest.raises(direct.DirectConsumerError, match="listener_unavailable"):
        direct.create_kafka_consumer(config)

    assert created["raw"].closed is True


def test_main_builds_store_coordinator_and_runs_fake_kafka(monkeypatch, tmp_path: Path):
    env = _direct_env(tmp_path)
    monkeypatch.setattr(direct.os, "environ", env)
    captured = {}

    class TopicPartition:
        def __init__(self, topic, partition):
            self.topic = topic
            self.partition = partition

        def __hash__(self):
            return hash((self.topic, self.partition))

        def __eq__(self, other):
            return (self.topic, self.partition) == (
                getattr(other, "topic", None),
                getattr(other, "partition", None),
            )

    class OffsetAndMetadata:
        def __init__(self, offset, metadata="", *args):
            self.offset = offset
            self.metadata = metadata

    class Raw:
        def __init__(self, **kwargs):
            captured["kwargs"] = kwargs
            captured["raw"] = self
            self.partition = TopicPartition(TOPIC, 0)
            self.poll_count = 0
            self.commits = []
            self.closed = False

        def subscribe(self, *, topics, listener=None):
            captured["topics"] = topics
            captured["listener"] = listener

        def assignment(self):
            return (self.partition,)

        def committed(self, _partition, **_kwargs):
            return None

        def position(self, _partition):
            return 10

        def seek(self, _partition, _offset):
            raise AssertionError("position already matches T0")

        def poll(self, **_kwargs):
            self.poll_count += 1
            if self.poll_count == 1:
                return {self.partition: [_message(10)]}
            return {}

        def commit(self, **kwargs):
            self.commits.append(kwargs)

        def close(self):
            self.closed = True

    module = SimpleNamespace(
        KafkaConsumer=Raw,
        TopicPartition=TopicPartition,
        OffsetAndMetadata=OffsetAndMetadata,
        ConsumerRebalanceListener=type("ConsumerRebalanceListener", (), {}),
    )
    monkeypatch.setattr(direct.importlib, "import_module", lambda _name: module)

    assert direct.main(["--max-polls", "1"]) == 0
    assert captured["topics"] == (TOPIC,)
    assert captured["kwargs"]["group_id"] == "rca-direct-independent"
    assert captured["raw"].closed is True


def test_main_fail_closed_on_kafka_setup_without_secret_output(
    monkeypatch, tmp_path: Path, capsys
):
    env = _direct_env(tmp_path)
    monkeypatch.setattr(direct.os, "environ", env)
    monkeypatch.setattr(
        direct,
        "create_kafka_consumer",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            direct.DirectConsumerError("broker password=direct-password")
        ),
    )

    assert direct.main(["--max-polls", "1"]) == 2
    output = capsys.readouterr()
    assert json.loads(output.err)["ok"] is False
    assert "direct-password" not in output.err


def test_direct_source_has_no_legacy_gate_imports():
    source = Path(direct.__file__).read_text(encoding="utf-8").lower()
    for forbidden in (
        "pnc_rca_control_store",
        "pnc_rca_write_fence",
        "pnc_rca_runtime_identity",
        "hmac",
    ):
        assert forbidden not in source
