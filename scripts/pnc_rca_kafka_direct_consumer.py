"""Transport-neutral direct Kafka intake for the additive RCA path.

This module deliberately contains no Kafka client import.  A caller supplies a
consumer-like object with ``poll``, ``commit`` and (optionally) ``seek`` methods,
which keeps the durable intake semantics testable without a broker.  The
``MiniStore`` remains the only persistence boundary: records are durable before
an offset is committed, and pending rows are recovered before the first poll.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import tempfile
from typing import Any, Final, Protocol, TypeAlias

from gateway.pnc_rca_kafka_contract import FIXED_KAFKA_GROUP_ID, WorkflowEventPolicy
from gateway.pnc_rca_mini_store import (
    MiniIngestResult,
    MiniKafkaRecord,
    MiniRecordConflictError,
    MiniStore,
)


DIRECT_HEALTH_SCHEMA_VERSION: Final = "pnc_rca_kafka_direct_health_v1"
DEFAULT_POLL_TIMEOUT_MS: Final = 5_000
DEFAULT_MAX_POLL_RECORDS: Final = 100
DEFAULT_RECOVERY_BATCH_SIZE: Final = 1_000
MAX_HEALTH_ERROR_TYPE: Final = 120


class DirectConsumerError(RuntimeError):
    """Base error raised by the transport-neutral runner."""


class OffsetCoherenceError(DirectConsumerError):
    """Committed, local and initial offsets cannot be reconciled safely."""


class AckSafetyError(DirectConsumerError):
    """The store did not prove durable progress for a record."""


class PollOrderError(DirectConsumerError):
    """A batch violates the non-decreasing order of one partition."""


class OffsetProvider(Protocol):
    """Small provider contract used during assignment.

    ``committed`` is required.  ``position`` is optional and is used only for
    diagnostics/seek decisions when a provider implements it.
    """

    def committed(self, topic: str, partition: int) -> int | Any | None: ...

    def position(self, topic: str, partition: int) -> int | None: ...


OffsetProviderLike: TypeAlias = OffsetProvider | Mapping[Any, Any] | None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _valid_nonnegative(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return int(value)


def _field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _partition_id(partition: Any) -> int:
    value = _field(partition, "partition", partition)
    return _valid_nonnegative(value, "partition")


def _partition_topic(partition: Any, default: str) -> str:
    value = _field(partition, "topic", default)
    topic = str(value or default).strip()
    if not topic:
        raise ValueError("topic must not be empty")
    return topic


def _offset_value(value: Any, field_name: str) -> int | None:
    """Normalize Kafka OffsetAndMetadata-like values and nullable offsets."""

    if value is None:
        return None
    nested = getattr(value, "offset", value)
    return _valid_nonnegative(nested, field_name)


def _headers(value: Any) -> tuple[tuple[str, bytes | None], ...]:
    result: list[tuple[str, bytes | None]] = []
    for item in value or ():
        if not isinstance(item, Sequence) or isinstance(item, (str, bytes)):
            raise ValueError("record headers must contain name/value pairs")
        if len(item) != 2:
            raise ValueError("record headers must contain name/value pairs")
        name, header_value = item
        if header_value is None:
            result.append((str(name), None))
        elif isinstance(header_value, (bytes, bytearray, memoryview, str)):
            result.append((
                str(name),
                header_value.encode()
                if isinstance(header_value, str)
                else bytes(header_value),
            ))
        else:
            raise ValueError("record header value must be bytes or text")
    return tuple(result)


def record_from_message(message: Any) -> MiniKafkaRecord:
    """Convert a Kafka-like message or mapping to the store record contract."""

    if isinstance(message, MiniKafkaRecord):
        return message
    timestamp = _field(message, "timestamp_ms", None)
    if timestamp is None:
        timestamp = _field(message, "timestamp", None)
    if isinstance(timestamp, bool) or not isinstance(timestamp, int) or timestamp < 0:
        timestamp = None
    return MiniKafkaRecord(
        topic=str(_field(message, "topic", "")),
        partition=_valid_nonnegative(_field(message, "partition"), "partition"),
        offset=_valid_nonnegative(_field(message, "offset"), "offset"),
        value=_field(message, "value"),
        key=_field(message, "key", None),
        timestamp_ms=timestamp,
        headers=_headers(_field(message, "headers", ())),
    )


@dataclass(frozen=True, slots=True)
class DirectConsumerConfig:
    """Validated, non-secret settings for the direct intake loop."""

    topic: str
    policy: WorkflowEventPolicy
    group_id: str = FIXED_KAFKA_GROUP_ID
    poll_timeout_ms: int = DEFAULT_POLL_TIMEOUT_MS
    max_poll_records: int = DEFAULT_MAX_POLL_RECORDS
    recovery_batch_size: int = DEFAULT_RECOVERY_BATCH_SIZE
    initial_offsets: Mapping[int, int] = field(default_factory=dict)
    health_path: Path | None = None

    def __post_init__(self) -> None:
        topic = str(self.topic or "").strip()
        if not topic:
            raise ValueError("topic must not be empty")
        if topic != self.policy.topic:
            raise ValueError("topic must match policy.topic")
        group_id = str(self.group_id or "").strip()
        if not group_id:
            raise ValueError("group_id must not be empty")
        for name, value in (
            ("poll_timeout_ms", self.poll_timeout_ms),
            ("max_poll_records", self.max_poll_records),
            ("recovery_batch_size", self.recovery_batch_size),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        offsets: dict[int, int] = {}
        for partition, offset in dict(self.initial_offsets or {}).items():
            offsets[_valid_nonnegative(int(partition), "partition")] = (
                _valid_nonnegative(offset, "initial offset")
            )
        object.__setattr__(self, "topic", topic)
        object.__setattr__(self, "group_id", group_id)
        object.__setattr__(self, "initial_offsets", offsets)
        if self.health_path is not None:
            object.__setattr__(self, "health_path", Path(self.health_path).expanduser())

    def initial_offset_for(self, partition: int) -> int | None:
        return self.initial_offsets.get(_valid_nonnegative(partition, "partition"))

    def public_dict(self) -> dict[str, Any]:
        return {
            "topic": self.topic,
            "group_id": self.group_id,
            "poll_timeout_ms": self.poll_timeout_ms,
            "max_poll_records": self.max_poll_records,
            "recovery_batch_size": self.recovery_batch_size,
            "initial_offsets": {
                str(partition): offset
                for partition, offset in sorted(self.initial_offsets.items())
            },
            "policy": self.policy.to_dict(),
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "DirectConsumerConfig":
        policy_value = value.get("policy")
        policy = (
            policy_value
            if isinstance(policy_value, WorkflowEventPolicy)
            else WorkflowEventPolicy.from_mapping(policy_value or {})
        )
        offsets = value.get("initial_offsets", value.get("t0_offsets", {}))
        return cls(
            topic=value.get("topic", policy.topic),
            policy=policy,
            group_id=value.get("group_id", FIXED_KAFKA_GROUP_ID),
            poll_timeout_ms=value.get("poll_timeout_ms", DEFAULT_POLL_TIMEOUT_MS),
            max_poll_records=value.get("max_poll_records", DEFAULT_MAX_POLL_RECORDS),
            recovery_batch_size=value.get(
                "recovery_batch_size", DEFAULT_RECOVERY_BATCH_SIZE
            ),
            initial_offsets=offsets or {},
            health_path=value.get("health_path"),
        )


@dataclass(frozen=True, slots=True)
class OffsetResolution:
    topic: str
    partition: int
    committed_offset: int | None
    durable_next_offset: int | None
    t0_offset: int | None
    seek_offset: int
    source: str
    broker_position: int | None = None

    @property
    def offset(self) -> int:
        """Short alias useful to transport adapters."""

        return self.seek_offset


def _mapping_lookup(mapping: Mapping[Any, Any], topic: str, partition: int) -> Any:
    for key in ((topic, partition), f"{topic}:{partition}", partition, str(partition)):
        if key in mapping:
            return mapping[key]
    return None


def _provider_value(
    provider: OffsetProviderLike,
    name: str,
    topic: str,
    partition: int,
) -> Any:
    if provider is None:
        return None
    if isinstance(provider, Mapping):
        if name == "committed":
            value = provider.get("committed")
            if isinstance(value, Mapping):
                return _mapping_lookup(value, topic, partition)
            return _mapping_lookup(provider, topic, partition)
        value = provider.get(name)
        return (
            _mapping_lookup(value, topic, partition)
            if isinstance(value, Mapping)
            else None
        )
    method = getattr(provider, name, None)
    if not callable(method):
        if name == "committed":
            for alias in ("committed_offset", "get_committed"):
                method = getattr(provider, alias, None)
                if callable(method):
                    break
        if not callable(method):
            return None
    try:
        return method(topic, partition)
    except TypeError:
        # Some adapters naturally key by a partition integer only.
        return method(partition)


class DirectOffsetCoordinator:
    """Resolve assignment starts from broker, local progress and explicit T0."""

    def __init__(
        self,
        store: MiniStore,
        *,
        topic: str,
        provider: OffsetProviderLike = None,
        initial_offsets: Mapping[int, int] | None = None,
    ) -> None:
        self.store = store
        self.topic = str(topic or "").strip()
        if not self.topic:
            raise ValueError("topic must not be empty")
        self.provider = provider
        raw_offsets = dict(initial_offsets or {})
        self.initial_offsets = {
            _valid_nonnegative(int(partition), "partition"): _valid_nonnegative(
                offset, "initial offset"
            )
            for partition, offset in raw_offsets.items()
        }

    def _committed(self, partition: int) -> int | None:
        return _offset_value(
            _provider_value(self.provider, "committed", self.topic, partition),
            "committed offset",
        )

    def _position(self, partition: int) -> int | None:
        return _offset_value(
            _provider_value(self.provider, "position", self.topic, partition),
            "broker position",
        )

    def resolve(self, partition: int) -> OffsetResolution:
        partition = _valid_nonnegative(partition, "partition")
        durable = self.store.partition_progress(
            topic=self.topic, partitions=(partition,)
        ).get(partition)
        if durable is not None:
            durable = _valid_nonnegative(durable, "durable next offset")
        committed = self._committed(partition)
        t0 = self.initial_offsets.get(partition)
        if committed is not None:
            # A failed broker commit legitimately leaves the broker behind
            # local durable progress.  The inverse would skip unpersisted data.
            if (t0 is not None and committed < t0) or (
                durable is not None and committed > durable
            ):
                raise OffsetCoherenceError(
                    f"broker_local_offset_incoherent:{partition}"
                )
            start, source = committed, "committed"
        elif durable is not None:
            start, source = durable, "durable_progress"
        elif t0 is not None:
            start, source = t0, "t0"
        else:
            raise OffsetCoherenceError(f"initial_offset_missing:{partition}")
        return OffsetResolution(
            topic=self.topic,
            partition=partition,
            committed_offset=committed,
            durable_next_offset=durable,
            t0_offset=t0,
            seek_offset=start,
            source=source,
            broker_position=self._position(partition),
        )

    resolve_partition = resolve

    def resolve_assignment(
        self, partitions: Iterable[Any]
    ) -> tuple[OffsetResolution, ...]:
        resolutions = []
        for item in partitions:
            topic = _partition_topic(item, self.topic)
            if topic != self.topic:
                raise OffsetCoherenceError("assigned_unexpected_topic")
            resolutions.append(self.resolve(_partition_id(item)))
        return tuple(resolutions)

    def apply_assignment(
        self, consumer: Any, partitions: Iterable[Any]
    ) -> tuple[OffsetResolution, ...]:
        """Seek each assigned partition to its reconciled durable start."""

        items = tuple(partitions)
        resolutions = self.resolve_assignment(items)
        seek = getattr(consumer, "seek", None)
        if not callable(seek):
            raise OffsetCoherenceError("consumer_seek_unavailable")
        position_method = getattr(consumer, "position", None)
        for item, resolution in zip(items, resolutions):
            current = None
            if callable(position_method):
                try:
                    current = _offset_value(position_method(item), "consumer position")
                except TypeError:
                    current = _offset_value(
                        position_method(resolution.partition), "consumer position"
                    )
            if current != resolution.seek_offset:
                seek(item, resolution.seek_offset)
        return resolutions

    cohere_assignment = resolve_assignment


class MappingOffsetProvider:
    """Simple injectable provider for tests and adapters."""

    def __init__(
        self,
        committed: Mapping[Any, Any] | None = None,
        positions: Mapping[Any, Any] | None = None,
    ) -> None:
        self.committed_offsets = dict(committed or {})
        self.positions = dict(positions or {})

    def committed(self, topic: str, partition: int) -> Any:
        return _mapping_lookup(self.committed_offsets, topic, partition)

    def position(self, topic: str, partition: int) -> Any:
        return _mapping_lookup(self.positions, topic, partition)


@dataclass(slots=True)
class DirectPollStats:
    polls: int = 0
    idle_polls: int = 0
    records_seen: int = 0
    records_committed: int = 0
    recovered_pending: int = 0
    accepted: int = 0
    filtered: int = 0
    invalid: int = 0
    deduped: int = 0
    transport_duplicates: int = 0
    conflicts: int = 0
    ingest_errors: int = 0
    commit_errors: int = 0

    def to_dict(self) -> dict[str, int]:
        return {str(key): int(value) for key, value in asdict(self).items()}


def _count_result(stats: DirectPollStats, result: MiniIngestResult) -> None:
    if result.decision in {"accepted", "filtered", "invalid", "deduped"}:
        setattr(stats, result.decision, getattr(stats, result.decision) + 1)
    if result.transport_duplicate:
        stats.transport_duplicates += 1


def _authorize_ack(store: Any, result: MiniIngestResult) -> None:
    if not bool(getattr(result, "ack_safe", False)):
        raise AckSafetyError(f"durable_progress_missing:{result.event_uid}")
    checker = getattr(store, "ack_safe", None)
    if callable(checker) and not bool(checker(result.event_uid)):
        raise AckSafetyError(f"durable_progress_check_failed:{result.event_uid}")


def process_record(
    store: Any,
    message: Any,
    *,
    policy: WorkflowEventPolicy,
) -> MiniIngestResult:
    """Persist/process one record and require a second durable ACK check."""

    record = record_from_message(message)
    result = store.ingest_record(record, policy=policy)
    _authorize_ack(store, result)
    return result


def recover_pending(
    store: Any,
    *,
    batch_size: int = DEFAULT_RECOVERY_BATCH_SIZE,
    stats: DirectPollStats | None = None,
) -> tuple[MiniIngestResult, ...]:
    """Drain pending rows before reading new transport records."""

    if (
        isinstance(batch_size, bool)
        or not isinstance(batch_size, int)
        or batch_size < 1
    ):
        raise ValueError("batch_size must be a positive integer")
    collected: list[MiniIngestResult] = []
    # A finite bound prevents a broken test/provider from creating an endless
    # recovery loop while retaining the normal multi-page drain behavior.
    for _ in range(100):
        page = tuple(store.process_pending(limit=batch_size))
        if not page:
            break
        for result in page:
            _authorize_ack(store, result)
            collected.append(result)
            if stats is not None:
                stats.recovered_pending += 1
                _count_result(stats, result)
        if len(page) < batch_size:
            break
    else:
        raise DirectConsumerError("pending_recovery_bound_exceeded")
    return tuple(collected)


def default_commit_payload(message: Any) -> dict[tuple[str, int], int]:
    """Build a broker-neutral one-offset advancement payload."""

    record = record_from_message(message)
    return {(record.topic, record.partition): record.offset + 1}


def _commit_record(
    consumer: Any,
    message: Any,
    result: MiniIngestResult,
    *,
    commit_callback: Callable[[Any, Any, MiniIngestResult], Any] | None,
    commit_payload: Callable[[Any], Any] | None,
) -> None:
    if commit_callback is not None:
        commit_callback(consumer, message, result)
        return
    commit = getattr(consumer, "commit", None)
    if not callable(commit):
        raise DirectConsumerError("consumer_commit_unavailable")
    if commit_payload is None:
        commit()
        return
    payload = commit_payload(message)
    commit(offsets=payload)


class DirectHealthReporter:
    """Atomic, payload-free process observation writer."""

    def __init__(
        self,
        path: str | Path | None = None,
        *,
        config: DirectConsumerConfig | None = None,
    ) -> None:
        self.path = Path(path).expanduser() if path is not None else None
        self.config = config
        self.state = "starting"
        self.last_event_at: str | None = None
        self.last_commit_at: str | None = None
        self.last_error: dict[str, str] | None = None

    def event(self) -> None:
        self.last_event_at = _utc_now()

    def committed(self) -> None:
        self.last_commit_at = _utc_now()

    def error(self, phase: str, exc: Exception) -> None:
        self.last_error = {
            "phase": str(phase),
            "type": type(exc).__name__[:MAX_HEALTH_ERROR_TYPE],
            "at": _utc_now(),
        }

    def observation(
        self,
        *,
        state: str,
        stats: DirectPollStats,
        assignment: Iterable[Any] = (),
    ) -> dict[str, Any]:
        self.state = str(state)
        body: dict[str, Any] = {
            "schema_version": DIRECT_HEALTH_SCHEMA_VERSION,
            "state": self.state,
            "healthy": self.state != "error",
            "ok": self.state != "error",
            "observed_at": _utc_now(),
            "last_event_at": self.last_event_at,
            "last_commit_at": self.last_commit_at,
            "assignment": sorted({_partition_id(item) for item in assignment}),
            "stats": stats.to_dict(),
        }
        if self.config is not None:
            body["config"] = self.config.public_dict()
        if self.last_error is not None:
            body["last_error"] = dict(self.last_error)
        return body

    def write(
        self,
        *,
        state: str,
        stats: DirectPollStats,
        assignment: Iterable[Any] = (),
    ) -> dict[str, Any]:
        body = self.observation(state=state, stats=stats, assignment=assignment)
        if self.path is None:
            return body
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(
            prefix=f".{self.path.name}.", suffix=".tmp", dir=self.path.parent
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(body, handle, ensure_ascii=False, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
        return body


def _batch_items(batch: Any) -> tuple[tuple[Any, tuple[Any, ...]], ...]:
    if not batch:
        return ()
    if isinstance(batch, Mapping):
        return tuple(
            (partition, tuple(messages or ())) for partition, messages in batch.items()
        )
    return ((None, tuple(batch)),)


def _poll(consumer: Any, config: DirectConsumerConfig) -> Any:
    poll = getattr(consumer, "poll", None)
    if not callable(poll):
        raise DirectConsumerError("consumer_poll_unavailable")
    try:
        return poll(
            timeout_ms=config.poll_timeout_ms,
            max_records=config.max_poll_records,
        )
    except TypeError:
        return poll(config.poll_timeout_ms)


def _assignment(consumer: Any) -> tuple[Any, ...]:
    method = getattr(consumer, "assignment", None)
    if not callable(method):
        return ()
    value = method()
    return tuple(value or ())


def run_poll_loop(
    consumer: Any,
    store: MiniStore,
    config: DirectConsumerConfig,
    *,
    coordinator: DirectOffsetCoordinator | None = None,
    health: DirectHealthReporter | None = None,
    stop_requested: Callable[[], bool] | None = None,
    commit_callback: Callable[[Any, Any, MiniIngestResult], Any] | None = None,
    commit_payload: Callable[[Any], Any] | None = default_commit_payload,
    max_polls: int | None = None,
    stats: DirectPollStats | None = None,
    recover_on_start: bool = True,
) -> DirectPollStats:
    """Run a bounded or continuous intake loop over an injected consumer."""

    if max_polls is not None and (
        isinstance(max_polls, bool) or not isinstance(max_polls, int) or max_polls < 0
    ):
        raise ValueError("max_polls must be a non-negative integer or None")
    stats = stats or DirectPollStats()
    stop_requested = stop_requested or (lambda: False)
    health = health or DirectHealthReporter(config.health_path, config=config)
    assignment: tuple[Any, ...] = ()
    cohered_assignment: tuple[tuple[str, int], ...] | None = None
    health.write(state="starting", stats=stats, assignment=assignment)

    if recover_on_start:
        try:
            recovered = recover_pending(
                store, batch_size=config.recovery_batch_size, stats=stats
            )
            if recovered:
                health.write(
                    state="recovered_pending", stats=stats, assignment=assignment
                )
        except Exception as exc:
            health.error("recovery", exc)
            health.write(state="error", stats=stats, assignment=assignment)
            raise

    while not stop_requested():
        if max_polls is not None and stats.polls >= max_polls:
            break
        try:
            assignment = _assignment(consumer)
            assignment_key = tuple(
                sorted(
                    (_partition_topic(item, config.topic), _partition_id(item))
                    for item in assignment
                )
            )
            if coordinator is not None and assignment_key != cohered_assignment:
                if assignment:
                    coordinator.apply_assignment(consumer, assignment)
                cohered_assignment = assignment_key
            batch = _poll(consumer, config)
        except Exception as exc:
            health.error("poll", exc)
            health.write(state="error", stats=stats, assignment=assignment)
            raise
        stats.polls += 1
        if not batch:
            stats.idle_polls += 1
            health.write(state="idle", stats=stats, assignment=assignment)
            continue

        try:
            for partition, messages in _batch_items(batch):
                records = tuple(record_from_message(message) for message in messages)
                previous_offset: int | None = None
                for record in records:
                    if partition is not None:
                        expected_partition = _partition_id(partition)
                        if record.partition != expected_partition:
                            raise PollOrderError("message_partition_mismatch")
                    if previous_offset is not None and record.offset < previous_offset:
                        raise PollOrderError(
                            f"partition_offset_decreased:{record.partition}"
                        )
                    previous_offset = record.offset

                for record in records:
                    stats.records_seen += 1
                    health.event()
                    try:
                        result = process_record(store, record, policy=config.policy)
                    except Exception as exc:
                        stats.ingest_errors += 1
                        if isinstance(exc, MiniRecordConflictError):
                            stats.conflicts += 1
                        health.error("ingest", exc)
                        health.write(state="error", stats=stats, assignment=assignment)
                        raise
                    _count_result(stats, result)
                    try:
                        _commit_record(
                            consumer,
                            record,
                            result,
                            commit_callback=commit_callback,
                            commit_payload=commit_payload,
                        )
                    except Exception as exc:
                        stats.commit_errors += 1
                        health.error("commit", exc)
                        health.write(state="error", stats=stats, assignment=assignment)
                        raise
                    stats.records_committed += 1
                    health.committed()
                    health.write(state="running", stats=stats, assignment=assignment)
        except Exception:
            raise

    health.write(state="stopped", stats=stats, assignment=assignment)
    return stats


class DirectConsumer:
    """Convenience object for embedding the runner in a service entrypoint."""

    def __init__(
        self,
        consumer: Any,
        store: MiniStore,
        config: DirectConsumerConfig,
        *,
        coordinator: DirectOffsetCoordinator | None = None,
        health: DirectHealthReporter | None = None,
    ) -> None:
        self.consumer = consumer
        self.store = store
        self.config = config
        self.coordinator = coordinator
        self.health = health

    def run(self, **kwargs: Any) -> DirectPollStats:
        return run_poll_loop(
            self.consumer,
            self.store,
            self.config,
            coordinator=self.coordinator,
            health=self.health,
            **kwargs,
        )


run_direct_consumer = run_poll_loop


__all__ = [
    "AckSafetyError",
    "DirectConsumer",
    "DirectConsumerConfig",
    "DirectConsumerError",
    "DirectHealthReporter",
    "DirectOffsetCoordinator",
    "DirectPollStats",
    "MappingOffsetProvider",
    "OffsetCoherenceError",
    "OffsetProvider",
    "OffsetResolution",
    "PollOrderError",
    "default_commit_payload",
    "process_record",
    "record_from_message",
    "recover_pending",
    "run_direct_consumer",
    "run_poll_loop",
]
