"""Transport-neutral direct Kafka intake for the additive RCA path.

This module deliberately contains no Kafka client import.  A caller supplies a
consumer-like object with ``poll``, ``commit`` and (optionally) ``seek`` methods,
which keeps the durable intake semantics testable without a broker.  The
``MiniStore`` remains the only persistence boundary: records are durable before
an offset is committed, and pending rows are recovered before the first poll.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import importlib
import json
import os
from pathlib import Path
import signal
import sys
import tempfile
from typing import Any, Final, Protocol, TypeAlias

from dotenv import dotenv_values

from gateway.pnc_rca_kafka_contract import (
    FIXED_KAFKA_GROUP_ID,
    WorkflowEventPolicy,
    WorkflowTransition,
)
from gateway.pnc_rca_mini_store import (
    MiniIngestResult,
    MiniKafkaRecord,
    MiniRecordConflictError,
    MiniStore,
)


DIRECT_HEALTH_SCHEMA_VERSION: Final = "pnc_rca_kafka_direct_health_v1"
DIRECT_ENV_PREFIX: Final = "HERMES_RCA_DIRECT_KAFKA_"
DIRECT_ENV_PREFIX_ALIASES: Final = (
    DIRECT_ENV_PREFIX,
    "HERMES_RCA_DIRECT_",
)
DIRECT_DEFAULT_GROUP_ID: Final = "rca_direct_path"
DIRECT_DEFAULT_API_VERSION: Final = (3, 9, 0)
DIRECT_DEFAULT_SECURITY_PROTOCOL: Final = "SASL_PLAINTEXT"
DIRECT_DEFAULT_SASL_MECHANISM: Final = "PLAIN"
DIRECT_DEFAULT_ISOLATION_LEVEL: Final = "read_committed"
DEFAULT_POLL_TIMEOUT_MS: Final = 5_000
DEFAULT_MAX_POLL_RECORDS: Final = 100
DEFAULT_RECOVERY_BATCH_SIZE: Final = 1_000
MAX_HEALTH_ERROR_TYPE: Final = 120
DIRECT_MAX_REQUEST_TIMEOUT_MS: Final = 300_000
DIRECT_MAX_SESSION_TIMEOUT_MS: Final = 120_000
DIRECT_MAX_POLL_INTERVAL_MS: Final = 900_000
DIRECT_MAX_POLL_TIMEOUT_MS: Final = 60_000
DIRECT_MAX_T0_PARTITIONS: Final = 10_000
DIRECT_MAX_FETCH_BYTES: Final = 20 * 1024 * 1024


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
    group_id: str = DIRECT_DEFAULT_GROUP_ID
    poll_timeout_ms: int = DEFAULT_POLL_TIMEOUT_MS
    max_poll_records: int = DEFAULT_MAX_POLL_RECORDS
    recovery_batch_size: int = DEFAULT_RECOVERY_BATCH_SIZE
    initial_offsets: Mapping[int, int] = field(default_factory=dict)
    health_path: Path | None = None
    commit_enabled: bool = True
    enabled: bool = True

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
        if not isinstance(self.commit_enabled, bool):
            raise ValueError("commit_enabled must be a boolean")
        if not isinstance(self.enabled, bool):
            raise ValueError("enabled must be a boolean")
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
            "commit_enabled": self.commit_enabled,
            "enabled": self.enabled,
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
            group_id=value.get("group_id", DIRECT_DEFAULT_GROUP_ID),
            poll_timeout_ms=value.get("poll_timeout_ms", DEFAULT_POLL_TIMEOUT_MS),
            max_poll_records=value.get("max_poll_records", DEFAULT_MAX_POLL_RECORDS),
            recovery_batch_size=value.get(
                "recovery_batch_size", DEFAULT_RECOVERY_BATCH_SIZE
            ),
            initial_offsets=offsets or {},
            health_path=value.get("health_path"),
            commit_enabled=value.get("commit_enabled", True),
            enabled=value.get("enabled", True),
        )


def _direct_required(source: Mapping[str, Any], suffix: str) -> str:
    for prefix in DIRECT_ENV_PREFIX_ALIASES:
        name = f"{prefix}{suffix}"
        value = str(source.get(name, "")).strip()
        if value:
            return value
    raise ValueError(f"missing required direct setting: {DIRECT_ENV_PREFIX}{suffix}")


def _direct_pick(
    source: Mapping[str, Any],
    suffixes: Sequence[str],
    *,
    default: Any = None,
) -> Any:
    for suffix in suffixes:
        for prefix in DIRECT_ENV_PREFIX_ALIASES:
            name = f"{prefix}{suffix}"
            value = source.get(name)
            if value is not None and str(value).strip() != "":
                return value
    return default


def _direct_positive(value: Any, name: str, *, maximum: int | None = None) -> int:
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if parsed < 1 or (maximum is not None and parsed > maximum):
        if maximum is None:
            raise ValueError(f"{name} must be a positive integer")
        raise ValueError(f"{name} must be between 1 and {maximum}")
    return parsed


def _direct_api_version(value: Any) -> tuple[int, int, int]:
    raw = str(value).strip()
    parts = raw.split(".")
    if len(parts) != 3:
        raise ValueError("direct API version must be major.minor.patch")
    try:
        result = tuple(int(item) for item in parts)
    except ValueError as exc:
        raise ValueError("direct API version must be numeric") from exc
    if any(item < 0 for item in result):
        raise ValueError("direct API version parts must be non-negative")
    return result  # type: ignore[return-value]


def _direct_strict_json(raw: str, name: str) -> Any:
    def unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate_json_key")
            result[key] = value
        return result

    def reject_constant(_value: str) -> None:
        raise ValueError("invalid_json_constant")

    try:
        return json.loads(
            raw,
            object_pairs_hook=unique_pairs,
            parse_constant=reject_constant,
        )
    except (json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise ValueError(f"{name} must be strict JSON") from exc


def _direct_t0_offsets(source: Mapping[str, Any]) -> dict[int, int]:
    raw = _direct_pick(
        source,
        ("T0_OFFSETS_JSON", "INITIAL_OFFSETS_JSON", "START_OFFSETS_JSON"),
    )
    if raw is None:
        return {}
    value = _direct_strict_json(str(raw), f"{DIRECT_ENV_PREFIX}T0_OFFSETS_JSON")
    if not isinstance(value, dict) or not value:
        raise ValueError(
            f"{DIRECT_ENV_PREFIX}T0_OFFSETS_JSON must be a non-empty object"
        )
    if len(value) > DIRECT_MAX_T0_PARTITIONS:
        raise ValueError("direct T0 offset map is too large")
    result: dict[int, int] = {}
    for raw_partition, raw_offset in value.items():
        partition_text = str(raw_partition).strip()
        if not partition_text.isdigit():
            raise ValueError("direct T0 partition keys must be non-negative integers")
        partition = int(partition_text)
        if (
            isinstance(raw_offset, bool)
            or not isinstance(raw_offset, int)
            or raw_offset < 0
        ):
            raise ValueError("direct T0 offsets must be non-negative integers")
        if partition in result:
            raise ValueError("direct T0 offset map contains duplicate partitions")
        result[partition] = raw_offset
    return dict(sorted(result.items()))


def _direct_policy(source: Mapping[str, Any], topic: str) -> WorkflowEventPolicy:
    raw_policy = _direct_pick(source, ("POLICY_JSON",))
    if raw_policy is not None:
        value = _direct_strict_json(str(raw_policy), f"{DIRECT_ENV_PREFIX}POLICY_JSON")
        if not isinstance(value, Mapping):
            raise ValueError(f"{DIRECT_ENV_PREFIX}POLICY_JSON must be an object")
        policy = WorkflowEventPolicy.from_mapping(value)
        if policy.topic != topic:
            raise ValueError("direct policy topic must match direct topic")
        return policy

    policy_version = _direct_pick(source, ("POLICY_VERSION", "CREATION_RULE_VERSION"))
    if policy_version is None:
        raise ValueError(
            f"missing required direct setting: {DIRECT_ENV_PREFIX}POLICY_JSON"
        )
    project_keys = _direct_required(source, "PROJECT_KEYS")
    project_simple_names = _direct_required(source, "PROJECT_SIMPLE_NAMES")
    work_item_type_keys = _direct_required(source, "WORK_ITEM_TYPE_KEYS")
    status_change_types = str(
        _direct_pick(source, ("STATUS_CHANGE_TYPES",), default="")
    )
    transitions_raw = str(
        _direct_pick(
            source, ("STATE_TRANSITIONS_JSON", "TRANSITIONS_JSON"), default="[]"
        )
    )
    transitions = _direct_strict_json(
        transitions_raw, f"{DIRECT_ENV_PREFIX}STATE_TRANSITIONS_JSON"
    )
    if not isinstance(transitions, list) or not all(
        isinstance(item, Mapping) for item in transitions
    ):
        raise ValueError("direct transitions must be a JSON array of objects")
    snapshot_patterns = str(_direct_pick(source, ("SNAPSHOT_PATTERNS",), default=""))
    snapshot_sub_stages = str(
        _direct_pick(source, ("SNAPSHOT_SUB_STAGES",), default="")
    )
    policy = WorkflowEventPolicy(
        topic=topic,
        policy_version=str(policy_version),
        project_keys=frozenset(
            item.strip() for item in project_keys.split(",") if item.strip()
        ),
        project_simple_names=frozenset(
            item.strip() for item in project_simple_names.split(",") if item.strip()
        ),
        work_item_type_keys=frozenset(
            item.strip() for item in work_item_type_keys.split(",") if item.strip()
        ),
        status_change_types=frozenset(
            item.strip() for item in status_change_types.split(",") if item.strip()
        ),
        transitions=tuple(
            WorkflowTransition.from_mapping(item) for item in transitions
        ),
        snapshot_patterns=frozenset(
            item.strip() for item in snapshot_patterns.split(",") if item.strip()
        ),
        snapshot_sub_stages=frozenset(
            item.strip() for item in snapshot_sub_stages.split(",") if item.strip()
        ),
    )
    return policy


@dataclass(frozen=True, slots=True)
class DirectKafkaConfig:
    """Direct-only process settings used by the executable Kafka adapter."""

    bootstrap_servers: tuple[str, ...]
    topic: str
    group_id: str
    username: str
    password: str = field(repr=False)
    security_protocol: str
    sasl_mechanism: str
    isolation_level: str
    api_version: tuple[int, int, int]
    request_timeout_ms: int
    session_timeout_ms: int
    max_poll_interval_ms: int
    poll_timeout_ms: int
    max_poll_records: int
    offset_lookup_timeout_ms: int
    recovery_batch_size: int
    initial_offsets: Mapping[int, int]
    db_path: Path
    health_path: Path
    client_id: str
    policy: WorkflowEventPolicy
    commit_enabled: bool
    enabled: bool
    ssl_cafile: str | None = None
    ssl_certfile: str | None = None
    ssl_keyfile: str | None = None

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, Any] | None = None,
        *,
        hermes_home: str | Path | None = None,
    ) -> "DirectKafkaConfig":
        source = os.environ if env is None else env
        enabled_value = _direct_pick(source, ("ENABLED",), default="false")
        enabled_raw = str(enabled_value).strip().lower()
        if enabled_raw not in {"true", "false"}:
            raise ValueError(
                f"{DIRECT_ENV_PREFIX}ENABLED must be exactly true or false"
            )
        enabled = enabled_raw == "true"
        bootstrap = _direct_required(source, "BOOTSTRAP_SERVERS")
        bootstrap_servers = tuple(
            item.strip() for item in bootstrap.split(",") if item.strip()
        )
        if not bootstrap_servers:
            raise ValueError("direct bootstrap servers must not be empty")
        topic = _direct_required(source, "TOPIC")
        if any(char in topic for char in (",", "\n", "\r")):
            raise ValueError("direct topic must name one exact topic")
        group_value = _direct_pick(source, ("GROUP_ID", "GROUP"))
        if group_value is None:
            raise ValueError(
                f"missing required direct setting: {DIRECT_ENV_PREFIX}GROUP_ID"
            )
        group_id = str(group_value).strip()
        if not group_id:
            raise ValueError("direct group id must not be empty")
        if group_id.casefold() == FIXED_KAFKA_GROUP_ID.casefold():
            raise ValueError("direct group id must not reuse the legacy Kafka group")

        protocol = (
            str(
                _direct_pick(
                    source,
                    ("SECURITY_PROTOCOL",),
                    default=DIRECT_DEFAULT_SECURITY_PROTOCOL,
                )
            )
            .strip()
            .upper()
        )
        allowed_protocols = {"PLAINTEXT", "SASL_PLAINTEXT", "SSL", "SASL_SSL"}
        if protocol not in allowed_protocols:
            raise ValueError("unsupported direct security protocol")
        mechanism = str(
            _direct_pick(
                source,
                ("SASL_MECHANISM",),
                default=DIRECT_DEFAULT_SASL_MECHANISM,
            )
        ).strip()
        if not mechanism:
            raise ValueError("direct SASL mechanism must not be empty")
        username = str(
            _direct_pick(source, ("SASL_USERNAME", "USERNAME", "USER"), default="")
        ).strip()
        password = str(_direct_pick(source, ("SASL_PASSWORD", "PASSWORD"), default=""))
        if enabled and protocol.startswith("SASL_") and (not username or not password):
            raise ValueError("direct SASL credentials are required")
        if (
            enabled
            and protocol in {"SSL", "SASL_SSL"}
            and not _direct_pick(source, ("SSL_CAFILE",), default=None)
        ):
            raise ValueError("direct SSL requires SSL_CAFILE")
        isolation = str(
            _direct_pick(
                source,
                ("ISOLATION_LEVEL",),
                default=DIRECT_DEFAULT_ISOLATION_LEVEL,
            )
        ).strip()
        if isolation not in {"read_committed", "read_uncommitted"}:
            raise ValueError("invalid direct isolation level")

        home = Path(hermes_home or Path.home() / ".hermes").expanduser()
        db_default = (
            home
            / "runtime"
            / "pnc_agent"
            / "feishu_issue_kafka_rca_direct"
            / "mini.sqlite3"
        )
        health_default = (
            home
            / "runtime"
            / "pnc_agent"
            / "feishu_issue_kafka_rca_direct"
            / "health.json"
        )
        db_path = Path(
            _direct_pick(
                source, ("DB_PATH", "MINI_STORE_PATH", "STORE_PATH"), default=db_default
            )
        ).expanduser()
        health_path = Path(
            _direct_pick(source, ("HEALTH_PATH",), default=health_default)
        ).expanduser()
        if db_path == health_path:
            raise ValueError("direct DB and health paths must differ")

        poll_timeout_ms = _direct_positive(
            _direct_pick(source, ("POLL_TIMEOUT_MS",), default=DEFAULT_POLL_TIMEOUT_MS),
            f"{DIRECT_ENV_PREFIX}POLL_TIMEOUT_MS",
            maximum=DIRECT_MAX_POLL_TIMEOUT_MS,
        )
        max_poll_records = _direct_positive(
            _direct_pick(
                source, ("MAX_POLL_RECORDS",), default=DEFAULT_MAX_POLL_RECORDS
            ),
            f"{DIRECT_ENV_PREFIX}MAX_POLL_RECORDS",
            maximum=10_000,
        )
        request_timeout_ms = _direct_positive(
            _direct_pick(source, ("REQUEST_TIMEOUT_MS",), default=120_000),
            f"{DIRECT_ENV_PREFIX}REQUEST_TIMEOUT_MS",
            maximum=DIRECT_MAX_REQUEST_TIMEOUT_MS,
        )
        session_timeout_ms = _direct_positive(
            _direct_pick(source, ("SESSION_TIMEOUT_MS",), default=30_000),
            f"{DIRECT_ENV_PREFIX}SESSION_TIMEOUT_MS",
            maximum=DIRECT_MAX_SESSION_TIMEOUT_MS,
        )
        if request_timeout_ms <= session_timeout_ms:
            raise ValueError("direct request timeout must exceed session timeout")
        max_poll_interval_ms = _direct_positive(
            _direct_pick(source, ("MAX_POLL_INTERVAL_MS",), default=300_000),
            f"{DIRECT_ENV_PREFIX}MAX_POLL_INTERVAL_MS",
            maximum=DIRECT_MAX_POLL_INTERVAL_MS,
        )
        offset_lookup_timeout_ms = _direct_positive(
            _direct_pick(source, ("OFFSET_LOOKUP_TIMEOUT_MS",), default=5_000),
            f"{DIRECT_ENV_PREFIX}OFFSET_LOOKUP_TIMEOUT_MS",
            maximum=60_000,
        )
        recovery_batch_size = _direct_positive(
            _direct_pick(
                source, ("RECOVERY_BATCH_SIZE",), default=DEFAULT_RECOVERY_BATCH_SIZE
            ),
            f"{DIRECT_ENV_PREFIX}RECOVERY_BATCH_SIZE",
            maximum=100_000,
        )
        api_version = _direct_api_version(
            _direct_pick(
                source,
                ("API_VERSION",),
                default=".".join(str(item) for item in DIRECT_DEFAULT_API_VERSION),
            )
        )
        auto_offset_reset = str(
            _direct_pick(source, ("AUTO_OFFSET_RESET",), default="none")
        ).strip()
        if auto_offset_reset != "none":
            raise ValueError("direct AUTO_OFFSET_RESET must be none")

        commit_value = _direct_pick(source, ("COMMIT_ENABLED",))
        if commit_value is None:
            raise ValueError(
                f"missing required direct setting: {DIRECT_ENV_PREFIX}COMMIT_ENABLED"
            )
        commit_raw = str(commit_value).strip().lower()
        if commit_raw not in {"true", "false"}:
            raise ValueError(
                f"{DIRECT_ENV_PREFIX}COMMIT_ENABLED must be exactly true or false"
            )
        commit_enabled = commit_raw == "true"
        if not commit_enabled and group_id == DIRECT_DEFAULT_GROUP_ID:
            raise ValueError("direct shadow mode requires an isolated group id")

        policy = _direct_policy(source, topic)
        return cls(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            group_id=group_id,
            username=username,
            password=password,
            security_protocol=protocol,
            sasl_mechanism=mechanism,
            isolation_level=isolation,
            api_version=api_version,
            request_timeout_ms=request_timeout_ms,
            session_timeout_ms=session_timeout_ms,
            max_poll_interval_ms=max_poll_interval_ms,
            poll_timeout_ms=poll_timeout_ms,
            max_poll_records=max_poll_records,
            offset_lookup_timeout_ms=offset_lookup_timeout_ms,
            recovery_batch_size=recovery_batch_size,
            initial_offsets=_direct_t0_offsets(source),
            db_path=db_path,
            health_path=health_path,
            client_id=str(
                _direct_pick(source, ("CLIENT_ID",), default="hermes-rca-direct")
            ).strip()
            or "hermes-rca-direct",
            policy=policy,
            commit_enabled=commit_enabled,
            enabled=enabled,
            ssl_cafile=(
                str(_direct_pick(source, ("SSL_CAFILE",), default="")).strip() or None
            ),
            ssl_certfile=(
                str(_direct_pick(source, ("SSL_CERTFILE",), default="")).strip() or None
            ),
            ssl_keyfile=(
                str(_direct_pick(source, ("SSL_KEYFILE",), default="")).strip() or None
            ),
        )

    def runner_config(self, *, enabled: bool | None = None) -> DirectConsumerConfig:
        return DirectConsumerConfig(
            topic=self.topic,
            policy=self.policy,
            group_id=self.group_id,
            poll_timeout_ms=self.poll_timeout_ms,
            max_poll_records=self.max_poll_records,
            recovery_batch_size=self.recovery_batch_size,
            initial_offsets=self.initial_offsets,
            health_path=self.health_path,
            commit_enabled=self.commit_enabled,
            enabled=self.enabled if enabled is None else enabled,
        )

    def kafka_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "bootstrap_servers": list(self.bootstrap_servers),
            "group_id": self.group_id,
            "client_id": self.client_id,
            "security_protocol": self.security_protocol,
            "sasl_mechanism": self.sasl_mechanism,
            "isolation_level": self.isolation_level,
            "api_version": self.api_version,
            "request_timeout_ms": self.request_timeout_ms,
            "session_timeout_ms": self.session_timeout_ms,
            "max_poll_interval_ms": self.max_poll_interval_ms,
            "max_poll_records": self.max_poll_records,
            "max_partition_fetch_bytes": 2 * 1024 * 1024,
            "fetch_max_bytes": min(
                2 * 1024 * 1024 * self.max_poll_records,
                DIRECT_MAX_FETCH_BYTES,
            ),
            "auto_offset_reset": "none",
            "enable_auto_commit": False,
            "allow_auto_create_topics": False,
        }
        if self.username:
            kwargs["sasl_plain_username"] = self.username
        if self.password:
            kwargs["sasl_plain_password"] = self.password
        for key, value in (
            ("ssl_cafile", self.ssl_cafile),
            ("ssl_certfile", self.ssl_certfile),
            ("ssl_keyfile", self.ssl_keyfile),
        ):
            if value:
                kwargs[key] = value
        return kwargs

    def public_dict(self) -> dict[str, Any]:
        return {
            "bootstrap_servers": list(self.bootstrap_servers),
            "topic": self.topic,
            "group_id": self.group_id,
            "security_protocol": self.security_protocol,
            "sasl_mechanism": self.sasl_mechanism,
            "isolation_level": self.isolation_level,
            "api_version": ".".join(str(item) for item in self.api_version),
            "request_timeout_ms": self.request_timeout_ms,
            "session_timeout_ms": self.session_timeout_ms,
            "max_poll_interval_ms": self.max_poll_interval_ms,
            "poll_timeout_ms": self.poll_timeout_ms,
            "max_poll_records": self.max_poll_records,
            "db_path": str(self.db_path),
            "health_path": str(self.health_path),
            "t0_offsets": {
                str(key): value for key, value in self.initial_offsets.items()
            },
            "client_id": self.client_id,
            "commit_enabled": self.commit_enabled,
            "enabled": self.enabled,
            "policy": self.policy.to_dict(),
        }


def load_direct_environment(
    env_file: str | Path | None = None,
    *,
    environ: Mapping[str, Any] | None = None,
) -> tuple[dict[str, str], Path | None]:
    """Read an optional dotenv file without mutating the process environment."""

    source = {
        str(key): str(value)
        for key, value in (os.environ if environ is None else environ).items()
        if value is not None
    }
    raw_path = env_file or _direct_pick(source, ("ENV_FILE",))
    path = Path(raw_path).expanduser() if raw_path else None
    if path is not None:
        if not path.exists():
            raise ValueError(f"direct env file does not exist: {path}")
        values = dotenv_values(path)
        for key, value in values.items():
            if value is not None and key not in source:
                source[key] = str(value)
    return source, path


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
        try:
            return _offset_value(
                _provider_value(self.provider, "position", self.topic, partition),
                "broker position",
            )
        except Exception:
            # kafka-python reports no position before an explicit T0 seek.
            return None

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
                    try:
                        current = _offset_value(
                            position_method(resolution.partition), "consumer position"
                        )
                    except Exception:
                        # An uninitialized subscription position is expected
                        # when explicit T0 is the first safe start.
                        current = None
                except Exception:
                    current = None
            if current != resolution.seek_offset:
                seek(item, resolution.seek_offset)
        return resolutions

    def prime_assignment(
        self, consumer: Any, partitions: Iterable[Any]
    ) -> tuple[tuple[str, int, int], ...]:
        """Set explicit T0 without broker or store reads inside a callback.

        kafka-python invokes synchronous rebalance listeners from its network
        event loop. Calling ``committed()`` or ``position()`` there re-enters
        that loop and fails. A local ``seek`` is sufficient to prevent
        ``auto_offset_reset=none`` from rejecting a fresh group. The main poll
        loop subsequently performs the full committed/durable/T0 coherence
        check and discards any batch prefetched before that final seek.
        """

        seek = getattr(consumer, "seek", None)
        if not callable(seek):
            raise OffsetCoherenceError("consumer_seek_unavailable")
        primed: list[tuple[str, int, int]] = []
        for item in tuple(partitions):
            topic = _partition_topic(item, self.topic)
            if topic != self.topic:
                raise OffsetCoherenceError("assigned_unexpected_topic")
            partition = _partition_id(item)
            offset = self.initial_offsets.get(partition)
            if offset is None:
                continue
            seek(item, offset)
            primed.append((topic, partition, offset))
        return tuple(primed)

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


class KafkaPythonConsumerAdapter:
    """Small adapter from kafka-python objects to the runner contract."""

    def __init__(self, consumer: Any, kafka_module: Any) -> None:
        self.raw = consumer
        self.kafka_module = kafka_module
        self._topic_partition = getattr(kafka_module, "TopicPartition", None)
        self._offset_metadata = getattr(kafka_module, "OffsetAndMetadata", None)
        self._coordinator: DirectOffsetCoordinator | None = None
        self._lookup_timeout_seconds = 5.0
        structs = getattr(kafka_module, "structs", None)
        if structs is None:
            try:
                structs = importlib.import_module("kafka.structs")
            except ImportError:
                structs = None
        if structs is not None:
            self._topic_partition = self._topic_partition or getattr(
                structs, "TopicPartition", None
            )
            self._offset_metadata = self._offset_metadata or getattr(
                structs, "OffsetAndMetadata", None
            )

    def _tp(self, topic: str, partition: int) -> Any:
        if self._topic_partition is None:
            return (topic, partition)
        return self._topic_partition(topic, partition)

    def _offset(self, offset: int) -> Any:
        if self._offset_metadata is None:
            return offset
        try:
            return self._offset_metadata(offset, "", -1)
        except TypeError:
            return self._offset_metadata(offset, "")

    def set_coordinator(
        self,
        coordinator: DirectOffsetCoordinator,
        *,
        lookup_timeout_ms: int | None = None,
    ) -> None:
        self._coordinator = coordinator
        if lookup_timeout_ms is not None:
            self._lookup_timeout_seconds = max(0.001, lookup_timeout_ms / 1000.0)

    def set_lookup_timeout(self, lookup_timeout_ms: int) -> None:
        self._lookup_timeout_seconds = max(0.001, lookup_timeout_ms / 1000.0)

    def rebalance_listener(self) -> Any:
        adapter = self
        base = getattr(self.kafka_module, "ConsumerRebalanceListener", None)
        if base is None:
            raise DirectConsumerError("kafka_rebalance_listener_unavailable")

        class Listener(base):
            def on_partitions_revoked(self, _partitions: Iterable[Any]) -> None:
                return None

            def on_partitions_assigned(self, partitions: Iterable[Any]) -> None:
                if adapter._coordinator is not None and partitions:
                    adapter._coordinator.prime_assignment(adapter, partitions)

        return Listener()

    def subscribe(self, *, topics: Sequence[str], listener: Any = None) -> Any:
        subscribe = getattr(self.raw, "subscribe", None)
        if not callable(subscribe):
            raise DirectConsumerError("consumer_subscribe_unavailable")
        if listener is None:
            listener = self.rebalance_listener()
        try:
            return subscribe(topics=tuple(topics), listener=listener)
        except TypeError as exc:
            raise DirectConsumerError(
                "consumer_listener_subscription_unsupported"
            ) from exc

    def poll(self, **kwargs: Any) -> Any:
        return self.raw.poll(**kwargs)

    def assignment(self) -> tuple[Any, ...]:
        method = getattr(self.raw, "assignment", None)
        return tuple(method() or ()) if callable(method) else ()

    def position(self, partition: Any) -> Any:
        return self.raw.position(partition)

    def seek(self, partition: Any, offset: int) -> Any:
        return self.raw.seek(partition, offset)

    def committed(self, topic: str, partition: int) -> Any:
        tp = self._tp(topic, partition)
        try:
            return self.raw.committed(
                tp, timeout_ms=int(self._lookup_timeout_seconds * 1000)
            )
        except TypeError:
            return self.raw.committed(tp)

    def commit(self, *, offsets: Mapping[Any, Any] | None = None) -> Any:
        commit = getattr(self.raw, "commit", None)
        if not callable(commit):
            raise DirectConsumerError("consumer_commit_unavailable")
        if offsets is None:
            return commit()
        converted: dict[Any, Any] = {}
        for key, value in offsets.items():
            if isinstance(key, tuple) and len(key) == 2:
                native_key = self._tp(
                    str(key[0]), _valid_nonnegative(key[1], "partition")
                )
            else:
                native_key = key
            if isinstance(value, int) and not isinstance(value, bool):
                native_value = self._offset(value)
            else:
                native_value = value
            converted[native_key] = native_value
        return commit(offsets=converted)

    def close(self) -> Any:
        close = getattr(self.raw, "close", None)
        return close() if callable(close) else None

    def __getattr__(self, name: str) -> Any:
        return getattr(self.raw, name)


def create_kafka_consumer(config: DirectKafkaConfig) -> KafkaPythonConsumerAdapter:
    """Create and subscribe one direct Kafka client; no durable side effects."""

    try:
        kafka_module = importlib.import_module("kafka")
    except ImportError as exc:
        raise DirectConsumerError("kafka_client_unavailable") from exc
    constructor = getattr(kafka_module, "KafkaConsumer", None)
    if not callable(constructor):
        raise DirectConsumerError("kafka_consumer_constructor_unavailable")
    try:
        raw_consumer = constructor(**config.kafka_kwargs())
        adapter = KafkaPythonConsumerAdapter(raw_consumer, kafka_module)
        adapter.set_lookup_timeout(
            int(getattr(config, "offset_lookup_timeout_ms", 5_000))
        )
        adapter.subscribe(topics=(config.topic,), listener=adapter.rebalance_listener())
    except Exception as exc:
        try:
            close = getattr(locals().get("raw_consumer"), "close", None)
            if callable(close):
                close()
        except Exception:
            pass
        if isinstance(exc, DirectConsumerError):
            raise
        raise DirectConsumerError("kafka_consumer_setup_failed") from exc
    return adapter


# Short alias for embedding code and test doubles.
create_consumer = create_kafka_consumer


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
    commits_skipped: int = 0

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
                os.fchmod(handle.fileno(), 0o600)
                json.dump(body, handle, ensure_ascii=False, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
            os.chmod(self.path, 0o600)
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
            # kafka-python discovers subscription assignment during poll.  If
            # that first poll also prefetched records, seek before processing
            # them and discard the prefetched batch so the durable start wins.
            post_poll_assignment = _assignment(consumer)
            post_poll_key = tuple(
                sorted(
                    (_partition_topic(item, config.topic), _partition_id(item))
                    for item in post_poll_assignment
                )
            )
            if (
                coordinator is not None
                and post_poll_assignment
                and post_poll_key != cohered_assignment
            ):
                coordinator.apply_assignment(consumer, post_poll_assignment)
                assignment = post_poll_assignment
                cohered_assignment = post_poll_key
                batch = {}
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
                    if config.commit_enabled:
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
                            health.write(
                                state="error", stats=stats, assignment=assignment
                            )
                            raise
                        stats.records_committed += 1
                        health.committed()
                    else:
                        # Explicit shadow mode never invokes any broker commit API.
                        stats.commits_skipped += 1
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


def build_direct_config(
    env: Mapping[str, Any] | None = None,
    *,
    hermes_home: str | Path | None = None,
) -> DirectKafkaConfig:
    return DirectKafkaConfig.from_env(env, hermes_home=hermes_home)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env-file",
        help="optional direct-only dotenv file; it is read without mutation",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--check-config",
        action="store_true",
        help="validate and print redacted direct config without opening DB/Kafka",
    )
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="validate direct startup prerequisites without side effects",
    )
    mode.add_argument(
        "--safe-off",
        action="store_true",
        help="force disabled mode without opening MiniStore or Kafka",
    )
    parser.add_argument(
        "--max-polls",
        type=int,
        help="bounded poll count for a local probe; omitted means resident loop",
    )
    return parser


def _print_json(payload: Mapping[str, Any], *, stream: Any = None) -> None:
    print(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        file=stream or sys.stdout,
    )


def main(argv: list[str] | None = None) -> int:
    """Run the independent direct Kafka path or a read-only config probe."""

    args = build_arg_parser().parse_args(argv)
    if args.max_polls is not None and args.max_polls < 0:
        _print_json(
            {"ok": False, "error": "max_polls_must_be_non_negative"},
            stream=sys.stderr,
        )
        return 2
    consumer: KafkaPythonConsumerAdapter | None = None
    try:
        source, env_path = load_direct_environment(args.env_file)
        if args.safe_off:
            # The CLI flag is an explicit operator override.  Apply it to a
            # private copy so dotenv/process environments are never mutated.
            source = dict(source)
            source[f"{DIRECT_ENV_PREFIX}ENABLED"] = "false"
        config = build_direct_config(source)
        if args.check_config or args.dry_run:
            payload: dict[str, Any] = {
                "ok": True,
                "mode": "check-config" if args.check_config else "dry-run",
                "validation_scope": "config_only_no_db_or_kafka",
                "config": config.public_dict(),
            }
            if env_path is not None:
                payload["env_file"] = str(env_path)
            _print_json(payload)
            return 0

        effective_enabled = config.enabled and not args.safe_off
        runner_config = config.runner_config(enabled=effective_enabled)
        stats = DirectPollStats()
        health = DirectHealthReporter(config.health_path, config=runner_config)
        if not effective_enabled:
            health.write(state="disabled", stats=stats)
            return 0

        store = MiniStore(config.db_path)
        health.write(state="starting", stats=stats)
        recover_pending(
            store,
            batch_size=runner_config.recovery_batch_size,
            stats=stats,
        )
        consumer = create_kafka_consumer(config)
        coordinator = DirectOffsetCoordinator(
            store,
            topic=config.topic,
            provider=consumer,
            initial_offsets=config.initial_offsets,
        )
        consumer.set_coordinator(
            coordinator,
            lookup_timeout_ms=config.offset_lookup_timeout_ms,
        )
        stopping = False

        def request_stop(_signum: int, _frame: Any) -> None:
            nonlocal stopping
            stopping = True

        signal.signal(signal.SIGTERM, request_stop)
        signal.signal(signal.SIGINT, request_stop)
        run_poll_loop(
            consumer,
            store,
            runner_config,
            coordinator=coordinator,
            health=health,
            stop_requested=lambda: stopping,
            max_polls=args.max_polls,
            stats=stats,
            recover_on_start=False,
        )
        return 0
    except Exception as exc:
        # Error messages are intentionally type-only so credentials/raw payloads
        # cannot leak through a startup or transport exception.
        _print_json(
            {"ok": False, "error": type(exc).__name__},
            stream=sys.stderr,
        )
        return 2
    finally:
        if consumer is not None:
            try:
                consumer.close()
            except Exception:
                pass


__all__ = [
    "AckSafetyError",
    "DirectConsumer",
    "DirectConsumerConfig",
    "DirectConsumerError",
    "DirectKafkaConfig",
    "DirectHealthReporter",
    "DirectOffsetCoordinator",
    "DirectPollStats",
    "DIRECT_ENV_PREFIX",
    "DIRECT_HEALTH_SCHEMA_VERSION",
    "KafkaPythonConsumerAdapter",
    "MappingOffsetProvider",
    "OffsetCoherenceError",
    "OffsetProvider",
    "OffsetResolution",
    "PollOrderError",
    "default_commit_payload",
    "build_arg_parser",
    "build_direct_config",
    "create_consumer",
    "create_kafka_consumer",
    "load_direct_environment",
    "main",
    "process_record",
    "record_from_message",
    "recover_pending",
    "run_direct_consumer",
    "run_poll_loop",
]


if __name__ == "__main__":
    raise SystemExit(main())
