#!/usr/bin/env python3
"""Standalone durable shadow consumer for Feishu workflow-event RCA intake."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import importlib
import json
import os
from pathlib import Path
import signal
import sys
import time
from typing import Any, Callable, Mapping

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv

from gateway.pnc_rca_control_store import (
    KafkaRecord,
    RcaControlStore,
    RecordProcessingBlockedError,
)
from gateway.pnc_rca_kafka_contract import (
    MAX_WORKFLOW_EVENT_BYTES,
    WorkflowEventPolicy,
)
from gateway.pnc_rca_policy_config import (
    W3SnapshotAuthority,
    w3_snapshot_authority_from_env,
    workflow_policy_from_env,
)
from gateway.pnc_rca_runtime_identity import (
    MAX_HEALTH_FUTURE_SKEW_SECONDS,
    RCA_KAFKA_CONSUMER_LOADED_DEPENDENCIES,
    build_runtime_identity,
    canonical_json_sha256,
    runtime_identity_is_valid,
)
from gateway.pnc_rca_write_fence import (
    RESIDENT_INGRESS_OPEN_STATES,
    require_resident_activation_epoch,
)
from hermes_constants import get_hermes_home


ENV_PREFIX = "HERMES_RCA_KAFKA_"
CONSUMER_HEALTH_SCHEMA_VERSION = "pnc_rca_kafka_consumer_health_v3"
SERVICE_LABEL = "local.pnc.rca-kafka-consumer"
MAX_CONFIG_JSON_NESTING = 32
FIXED_SERVICE_ID = "root_cause_analysis_agent"
FIXED_KAFKA_PRINCIPAL = "rca"
FIXED_KAFKA_GROUP_ID = "rca_root_cause_analysis_agent"
FIXED_API_VERSION = (3, 9, 0)
FIXED_REQUEST_TIMEOUT_MS = 120_000
MIN_SESSION_TIMEOUT_MS = 10_000
MAX_SESSION_TIMEOUT_MS = 60_000
MIN_MAX_POLL_INTERVAL_MS = 60_000
MAX_MAX_POLL_INTERVAL_MS = 600_000
MAX_POLL_TIMEOUT_MS = 5_000
MAX_POLL_RECORDS = 10
MAX_OFFSET_LOOKUP_TIMEOUT_MS = 10_000
MAX_OUTBOX_HIGH_WATERMARK = 1_000


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _required(env: Mapping[str, str], name: str) -> str:
    value = str(env.get(name, "")).strip()
    if not value:
        raise ValueError(f"missing required environment variable: {name}")
    return value


def kafka_principal_is_valid(value: str) -> bool:
    return value == FIXED_KAFKA_PRINCIPAL


def _required_kafka_principal(env: Mapping[str, str], name: str) -> str:
    raw = str(env.get(name, ""))
    value = _required(env, name)
    if raw != value or not kafka_principal_is_valid(value):
        raise ValueError(f"{name} must be exactly {FIXED_KAFKA_PRINCIPAL}")
    return value


def _integer(
    env: Mapping[str, str],
    name: str,
    default: int,
    *,
    minimum: int = 1,
    maximum: int | None = None,
) -> int:
    text = str(env.get(name, default)).strip()
    try:
        value = int(text)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    if maximum is not None and value > maximum:
        raise ValueError(f"{name} must be at most {maximum}")
    return value


def _boolean(env: Mapping[str, str], name: str, default: bool = False) -> bool:
    text = str(env.get(name, "true" if default else "false")).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be true or false")


def _strict_boolean(
    env: Mapping[str, str], name: str, default: bool = False
) -> bool:
    text = str(env.get(name, "true" if default else "false")).strip().lower()
    if text == "true":
        return True
    if text == "false":
        return False
    raise ValueError(f"{name} must be exactly true or false")


def _api_version(value: str) -> tuple[int, int, int]:
    parts = value.strip().split(".")
    if len(parts) != 3:
        raise ValueError("HERMES_RCA_KAFKA_API_VERSION must have major.minor.patch")
    try:
        result = tuple(int(part) for part in parts)
    except ValueError as exc:
        raise ValueError("HERMES_RCA_KAFKA_API_VERSION must be numeric") from exc
    if any(part < 0 for part in result):
        raise ValueError("HERMES_RCA_KAFKA_API_VERSION parts must be non-negative")
    return result  # type: ignore[return-value]


def _strict_json(raw: str, name: str) -> Any:
    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate_json_key")
            result[key] = value
        return result

    def reject_constant(_value: str) -> None:
        raise ValueError("invalid_json_constant")

    try:
        value = json.loads(
            raw,
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except (json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise ValueError(f"{name} must be strict JSON") from exc
    stack: list[tuple[Any, int]] = [(value, 1)]
    while stack:
        item, depth = stack.pop()
        if depth > MAX_CONFIG_JSON_NESTING:
            raise ValueError(f"{name} exceeds maximum JSON nesting")
        if isinstance(item, dict):
            stack.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            stack.extend((child, depth + 1) for child in item)
    return value


def _initial_offsets(env: Mapping[str, str]) -> tuple[tuple[int, int], ...]:
    """Parse an optional, explicit T0 partition offset baseline.

    The baseline is used only when the consumer group has no committed offset
    for an assigned partition.  An empty baseline is valid for an established
    group, but a new group will then fail closed during partition assignment.
    """
    name = f"{ENV_PREFIX}START_OFFSETS_JSON"
    raw = str(env.get(name, "")).strip()
    if not raw:
        return ()
    values = _strict_json(raw, name)
    if not isinstance(values, dict) or not values:
        raise ValueError(f"{name} must be a non-empty JSON object")
    parsed: dict[int, int] = {}
    for raw_partition, raw_offset in values.items():
        partition_text = str(raw_partition).strip()
        if not partition_text.isdigit():
            raise ValueError(f"{name} partition keys must be non-negative integers")
        partition = int(partition_text)
        if (
            isinstance(raw_offset, bool)
            or not isinstance(raw_offset, int)
            or raw_offset < 0
        ):
            raise ValueError(f"{name} offsets must be non-negative integers")
        if partition in parsed:
            raise ValueError(f"{name} contains a duplicate partition")
        parsed[partition] = raw_offset
    return tuple(sorted(parsed.items()))


@dataclass(frozen=True)
class ConsumerConfig:
    bootstrap_servers: tuple[str, ...]
    topic: str
    username: str
    password: str = field(repr=False)
    group_id: str
    api_version: tuple[int, int, int]
    security_protocol: str
    sasl_mechanism: str
    isolation_level: str
    request_timeout_ms: int
    session_timeout_ms: int
    max_poll_interval_ms: int
    poll_timeout_ms: int
    max_poll_records: int
    offset_lookup_timeout_ms: int
    auto_offset_reset: str
    initial_offsets: tuple[tuple[int, int], ...]
    client_id: str
    control_db_path: Path
    health_path: Path
    submit_enabled: bool
    activation_required: bool
    outbox_high_watermark: int
    outbox_resume_watermark: int
    policy: WorkflowEventPolicy
    w3_snapshot_authority: W3SnapshotAuthority | None = None

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
        *,
        hermes_home: str | Path | None = None,
    ) -> "ConsumerConfig":
        source = os.environ if env is None else env
        forbidden = f"{ENV_PREFIX}CONSUMER_TIMEOUT_MS"
        if str(source.get(forbidden, "")).strip():
            raise ValueError(
                f"{forbidden} is forbidden: the production consumer must stay resident"
            )

        home = Path(hermes_home or get_hermes_home()).expanduser()
        topic = _required(source, f"{ENV_PREFIX}TOPIC")
        if "," in topic or "\n" in topic or "\r" in topic:
            raise ValueError(f"{ENV_PREFIX}TOPIC must name one exact topic")
        bootstrap_servers = tuple(
            item.strip()
            for item in _required(source, f"{ENV_PREFIX}BOOTSTRAP_SERVERS").split(",")
            if item.strip()
        )
        if not bootstrap_servers:
            raise ValueError(f"{ENV_PREFIX}BOOTSTRAP_SERVERS must not be empty")

        username = _required_kafka_principal(source, f"{ENV_PREFIX}USER")
        group_id = _required(source, f"{ENV_PREFIX}GROUP")
        if group_id != FIXED_KAFKA_GROUP_ID:
            raise ValueError(
                f"{ENV_PREFIX}GROUP must be exactly {FIXED_KAFKA_GROUP_ID}"
            )
        api_version = _api_version(
            str(source.get(f"{ENV_PREFIX}API_VERSION", "3.9.0"))
        )
        if api_version != FIXED_API_VERSION:
            raise ValueError(f"{ENV_PREFIX}API_VERSION must be exactly 3.9.0")
        request_timeout_ms = _integer(
            source, f"{ENV_PREFIX}REQUEST_TIMEOUT_MS", 120_000
        )
        if request_timeout_ms != FIXED_REQUEST_TIMEOUT_MS:
            raise ValueError(
                f"{ENV_PREFIX}REQUEST_TIMEOUT_MS must be exactly "
                f"{FIXED_REQUEST_TIMEOUT_MS}"
            )
        session_timeout_ms = _integer(
            source,
            f"{ENV_PREFIX}SESSION_TIMEOUT_MS",
            30_000,
            minimum=MIN_SESSION_TIMEOUT_MS,
            maximum=MAX_SESSION_TIMEOUT_MS,
        )
        if request_timeout_ms <= session_timeout_ms:
            raise ValueError("request timeout must be greater than session timeout")
        offset_lookup_timeout_ms = _integer(
            source,
            f"{ENV_PREFIX}OFFSET_LOOKUP_TIMEOUT_MS",
            3_000,
            maximum=MAX_OFFSET_LOOKUP_TIMEOUT_MS,
        )
        if offset_lookup_timeout_ms >= session_timeout_ms:
            raise ValueError("offset lookup timeout must be less than session timeout")
        auto_offset_reset = str(
            source.get(f"{ENV_PREFIX}AUTO_OFFSET_RESET", "none")
        ).strip()
        if auto_offset_reset != "none":
            raise ValueError("auto offset reset must be exactly none")
        security_protocol = str(
            source.get(f"{ENV_PREFIX}SECURITY_PROTOCOL", "SASL_PLAINTEXT")
        ).strip()
        sasl_mechanism = str(source.get(f"{ENV_PREFIX}SASL_MECHANISM", "PLAIN")).strip()
        if security_protocol != "SASL_PLAINTEXT":
            raise ValueError("security protocol must be exactly SASL_PLAINTEXT")
        if sasl_mechanism != "PLAIN":
            raise ValueError("SASL mechanism must be exactly PLAIN")
        isolation_level = str(
            source.get(f"{ENV_PREFIX}ISOLATION_LEVEL", "read_committed")
        ).strip()
        if isolation_level != "read_committed":
            raise ValueError("isolation level must be exactly read_committed")
        client_id = str(
            source.get(f"{ENV_PREFIX}CLIENT_ID", FIXED_SERVICE_ID)
        ).strip()
        if client_id != FIXED_SERVICE_ID:
            raise ValueError(f"{ENV_PREFIX}CLIENT_ID must be exactly {FIXED_SERVICE_ID}")
        outbox_high_watermark = _integer(
            source,
            f"{ENV_PREFIX}OUTBOX_HIGH_WATERMARK",
            100,
            maximum=MAX_OUTBOX_HIGH_WATERMARK,
        )
        outbox_resume_watermark = _integer(
            source,
            f"{ENV_PREFIX}OUTBOX_RESUME_WATERMARK",
            50,
            minimum=0,
        )
        if outbox_resume_watermark >= outbox_high_watermark:
            raise ValueError("outbox resume watermark must be below high watermark")

        policy = workflow_policy_from_env(source)
        if policy.topic != topic:
            raise ValueError("workflow policy topic must match consumer topic")
        w3_snapshot_authority = w3_snapshot_authority_from_env(
            source,
            active_policy=policy,
        )
        return cls(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            username=username,
            password=_required(source, f"{ENV_PREFIX}PASSWORD"),
            group_id=group_id,
            api_version=api_version,
            security_protocol=security_protocol,
            sasl_mechanism=sasl_mechanism,
            isolation_level=isolation_level,
            request_timeout_ms=request_timeout_ms,
            session_timeout_ms=session_timeout_ms,
            max_poll_interval_ms=_integer(
                source,
                f"{ENV_PREFIX}MAX_POLL_INTERVAL_MS",
                300_000,
                minimum=MIN_MAX_POLL_INTERVAL_MS,
                maximum=MAX_MAX_POLL_INTERVAL_MS,
            ),
            poll_timeout_ms=_integer(
                source,
                f"{ENV_PREFIX}POLL_TIMEOUT_MS",
                1_000,
                maximum=MAX_POLL_TIMEOUT_MS,
            ),
            max_poll_records=_integer(
                source,
                f"{ENV_PREFIX}MAX_POLL_RECORDS",
                MAX_POLL_RECORDS,
                maximum=MAX_POLL_RECORDS,
            ),
            offset_lookup_timeout_ms=offset_lookup_timeout_ms,
            auto_offset_reset=auto_offset_reset,
            initial_offsets=_initial_offsets(source),
            client_id=client_id,
            control_db_path=Path(
                source.get(
                    f"{ENV_PREFIX}CONTROL_DB_PATH",
                    home
                    / "runtime"
                    / "pnc_agent"
                    / "feishu_issue_kafka_rca"
                    / "control.sqlite3",
                )
            ).expanduser(),
            health_path=Path(
                source.get(
                    f"{ENV_PREFIX}HEALTH_PATH",
                    home
                    / "runtime"
                    / "pnc_agent"
                    / "feishu_issue_kafka_rca"
                    / "health.json",
                )
            ).expanduser(),
            submit_enabled=_boolean(source, f"{ENV_PREFIX}SUBMIT_ENABLED", False),
            activation_required=_strict_boolean(
                source,
                f"{ENV_PREFIX}ACTIVATION_REQUIRED",
                False,
            ),
            outbox_high_watermark=outbox_high_watermark,
            outbox_resume_watermark=outbox_resume_watermark,
            policy=policy,
            w3_snapshot_authority=w3_snapshot_authority,
        )

    def public_dict(self) -> dict[str, Any]:
        """Return health-safe configuration without credentials."""
        return {
            "bootstrap_servers": list(self.bootstrap_servers),
            "topic": self.topic,
            "group_id": self.group_id,
            "api_version": ".".join(str(part) for part in self.api_version),
            "security_protocol": self.security_protocol,
            "sasl_mechanism": self.sasl_mechanism,
            "isolation_level": self.isolation_level,
            "request_timeout_ms": self.request_timeout_ms,
            "session_timeout_ms": self.session_timeout_ms,
            "max_poll_interval_ms": self.max_poll_interval_ms,
            "poll_timeout_ms": self.poll_timeout_ms,
            "max_poll_records": self.max_poll_records,
            "max_record_bytes": MAX_WORKFLOW_EVENT_BYTES,
            "fetch_max_bytes": MAX_WORKFLOW_EVENT_BYTES * self.max_poll_records,
            "allow_auto_create_topics": False,
            "offset_lookup_timeout_ms": self.offset_lookup_timeout_ms,
            "auto_offset_reset": self.auto_offset_reset,
            "initial_offsets": {
                str(partition): offset
                for partition, offset in self.initial_offsets
            },
            "initial_offset_policy": "committed_else_explicit_t0_else_fail",
            "client_id": self.client_id,
            "control_db_path": str(self.control_db_path),
            "health_path": str(self.health_path),
            "submit_enabled": self.submit_enabled,
            "activation_required": self.activation_required,
            "activation_mode": "steady_only",
            "outbox_high_watermark": self.outbox_high_watermark,
            "outbox_resume_watermark": self.outbox_resume_watermark,
            "external_dispatch_wired": False,
            "creation_rule_version": self.policy.policy_version,
            "w3_snapshot_shadow": (
                self.w3_snapshot_authority.to_public_dict()
                if self.w3_snapshot_authority is not None
                else {"enabled": False}
            ),
        }

    def runtime_public_dict(self) -> dict[str, Any]:
        """Return the complete public contract bound into resident health."""
        result = self.public_dict()
        result["policy"] = self.policy.to_dict()
        return result

    def initial_offset_for(self, partition: int) -> int | None:
        return dict(self.initial_offsets).get(int(partition))

    def kafka_kwargs(self) -> dict[str, Any]:
        """Build kafka-python arguments; intentionally omits consumer_timeout_ms."""
        return {
            "bootstrap_servers": list(self.bootstrap_servers),
            "security_protocol": self.security_protocol,
            "sasl_mechanism": self.sasl_mechanism,
            "isolation_level": self.isolation_level,
            "sasl_plain_username": self.username,
            "sasl_plain_password": self.password,
            "group_id": self.group_id,
            "api_version": self.api_version,
            "request_timeout_ms": self.request_timeout_ms,
            "session_timeout_ms": self.session_timeout_ms,
            "max_poll_interval_ms": self.max_poll_interval_ms,
            "max_poll_records": self.max_poll_records,
            "max_partition_fetch_bytes": MAX_WORKFLOW_EVENT_BYTES,
            "fetch_max_bytes": MAX_WORKFLOW_EVENT_BYTES * self.max_poll_records,
            "auto_offset_reset": self.auto_offset_reset,
            "enable_auto_commit": False,
            "allow_auto_create_topics": False,
            "client_id": self.client_id,
        }


@dataclass
class PollStats:
    polls: int = 0
    idle_polls: int = 0
    records_seen: int = 0
    records_committed: int = 0
    ingest_errors: int = 0
    commit_errors: int = 0
    accepted: int = 0
    filtered: int = 0
    invalid: int = 0
    deduped: int = 0
    recovered_pending: int = 0
    backpressure_pauses: int = 0
    backpressure_resumes: int = 0
    max_dispatch_backlog: int = 0
    record_processing_blocks: int = 0
    blocked_partitions: int = 0


class ConsumerLoopError(RuntimeError):
    """Fatal poll-loop error; restart is required before offsets may advance."""

    def __init__(self, phase: str, stats: PollStats):
        super().__init__(f"Kafka consumer {phase} failed; restart required")
        self.phase = phase
        self.stats = stats


class HealthReporter:
    def __init__(self, config: ConsumerConfig, store: RcaControlStore):
        self.config = config
        self.store = store
        self.started_at = _utc_now()
        self.public_config = config.runtime_public_dict()
        self.runtime_identity = build_runtime_identity(
            service_label=SERVICE_LABEL,
            script_path=Path(__file__),
            public_config=self.public_config,
            loaded_dependencies=RCA_KAFKA_CONSUMER_LOADED_DEPENDENCIES,
        )
        self.last_event_at: str | None = None
        self.last_commit_at: str | None = None
        self.last_error: dict[str, str] | None = None
        self.assignment_reporter: Callable[[], Mapping[str, Any]] | None = None
        self._last_write_monotonic = 0.0

    def set_assignment_reporter(
        self, reporter: Callable[[], Mapping[str, Any]]
    ) -> None:
        self.assignment_reporter = reporter

    def write(
        self,
        *,
        state: str,
        stats: PollStats,
        force: bool = False,
    ) -> None:
        now_monotonic = time.monotonic()
        if not force and now_monotonic - self._last_write_monotonic < 10:
            return
        self._last_write_monotonic = now_monotonic
        store_health = self.store.health()
        assignment = (
            dict(self.assignment_reporter())
            if self.assignment_reporter is not None
            else {
                "assignment_count": 0,
                "revocation_count": 0,
                "callback_errors": 0,
                "assigned_partitions": [],
                "last_assignment_at": None,
                "last_error_type": "assignment_reporter_unavailable",
            }
        )
        assignment_ok = (
            bool(assignment.get("assigned_partitions"))
            and assignment.get("callback_errors") == 0
        )
        healthy = (
            state
            not in {
                "error",
                "partition_blocked",
                "stopped",
                "stopped_with_error",
            }
            and stats.blocked_partitions == 0
            and store_health.get("ok") is True
            and assignment_ok
        )
        heartbeat_at = _utc_now()
        body = {
            "schema_version": CONSUMER_HEALTH_SCHEMA_VERSION,
            "ok": healthy,
            "healthy": healthy,
            "enabled": True,
            "state": state,
            "mode": "outbox_pending" if self.config.submit_enabled else "shadow",
            "activation_required": self.config.activation_required,
            "external_dispatch_wired": False,
            "started_at": self.started_at,
            "heartbeat_at": heartbeat_at,
            "runtime_identity": self.runtime_identity.to_dict(),
            "last_event_at": self.last_event_at,
            "last_commit_at": self.last_commit_at,
            "last_error": self.last_error,
            "stats": asdict(stats),
            "config": self.public_config,
            "store": store_health,
            "assignment": assignment,
        }
        path = self.config.health_path
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        temporary.write_text(
            json.dumps(body, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)

    def event(self) -> None:
        self.last_event_at = _utc_now()

    def committed(self, *, clear_error: bool = True) -> None:
        self.last_commit_at = _utc_now()
        if clear_error:
            self.last_error = None

    def error(self, phase: str, exc: Exception) -> None:
        self.last_error = {
            "phase": phase,
            "type": type(exc).__name__,
            "at": _utc_now(),
        }


class ExplicitInitialOffsetListener:
    """Apply T0 only to assigned partitions that have no group offset."""

    def __init__(
        self,
        consumer: Any,
        config: ConsumerConfig,
        store: RcaControlStore,
    ):
        self.consumer = consumer
        self.config = config
        self.store = store
        self.applied: dict[int, int] = {}
        self.assignment_count = 0
        self.revocation_count = 0
        self.callback_errors = 0
        self.assigned_partitions: tuple[int, ...] = ()
        self.last_assignment_at: str | None = None
        self.last_error_type = ""

    async def on_partitions_revoked(self, _revoked: Any) -> None:
        self.revocation_count += 1
        self.applied = {}
        self.assigned_partitions = ()

    async def on_partitions_assigned(self, assigned: Any) -> None:
        assigned_partitions = tuple(assigned)
        self.assignment_count += 1
        try:
            await self._apply_partitions_assigned(assigned_partitions)
        except Exception as exc:
            self.callback_errors += 1
            self.assigned_partitions = ()
            self.last_error_type = type(exc).__name__
            raise
        self.assigned_partitions = tuple(
            sorted(int(topic_partition.partition) for topic_partition in assigned_partitions)
        )
        self.last_assignment_at = _utc_now()
        self.last_error_type = ""

    async def _apply_partitions_assigned(
        self, assigned_partitions: tuple[Any, ...]
    ) -> None:
        coordinator = getattr(self.consumer, "_coordinator", None)
        fetch_offsets = getattr(
            coordinator, "fetch_committed_offsets_async", None
        )
        if not callable(fetch_offsets):
            raise RuntimeError("async_committed_offset_fetch_unavailable")
        committed_offsets = await fetch_offsets(
            assigned_partitions,
            timeout_ms=self.config.offset_lookup_timeout_ms,
        )
        if not isinstance(committed_offsets, Mapping):
            raise RuntimeError("async_committed_offset_fetch_invalid")
        # kafka-python drives AsyncConsumerRebalanceListener callbacks with its
        # own coroutine runner, not an asyncio event loop.  ``asyncio.to_thread``
        # therefore fails here with ``RuntimeError: no running event loop``.
        # This is one bounded SQLite read during assignment, so keep it inline.
        local_progress = self.store.partition_progress(
            topic=self.config.topic,
            partitions=(
                int(topic_partition.partition)
                for topic_partition in assigned_partitions
            ),
        )

        activation_start_fence_reader = getattr(
            self.store, "activation_partition_start_fence", None
        )
        if callable(activation_start_fence_reader):
            try:
                activation_start_fence = activation_start_fence_reader(
                    topic=self.config.topic,
                    partitions=(
                        int(topic_partition.partition)
                        for topic_partition in assigned_partitions
                    ),
                )
            except Exception as exc:
                raise RuntimeError(
                    "activation_start_fence_unavailable"
                ) from exc
            if not isinstance(activation_start_fence, Mapping):
                raise RuntimeError("activation_start_fence_invalid")
        else:
            # Isolated test doubles and pre-activation stores retain the
            # explicit-T0 path; the live v13 store exposes the epoch fence.
            activation_start_fence = {}

        missing: list[int] = []
        incoherent: list[int] = []
        to_seek: list[tuple[Any, int]] = []
        for topic_partition in assigned_partitions:
            if str(topic_partition.topic) != self.config.topic:
                raise RuntimeError("assigned_unexpected_topic")
            committed = committed_offsets.get(topic_partition)
            committed_offset = getattr(committed, "offset", committed)
            if (
                isinstance(committed_offset, int)
                and not isinstance(committed_offset, bool)
                and committed_offset >= 0
            ):
                local_next = local_progress.get(int(topic_partition.partition))
                activation_t0 = activation_start_fence.get(
                    int(topic_partition.partition)
                )
                if (
                    activation_t0 is not None
                    and (
                        isinstance(activation_t0, bool)
                        or not isinstance(activation_t0, int)
                        or activation_t0 < 0
                    )
                ):
                    raise RuntimeError("activation_start_fence_invalid")
                if (
                    activation_t0 is not None
                    and committed_offset < activation_t0
                ):
                    to_seek.append((topic_partition, activation_t0))
                    continue
                t0 = (
                    int(activation_t0)
                    if activation_t0 is not None
                    else self.config.initial_offset_for(topic_partition.partition)
                )
                intentional_activation_skip = (
                    activation_t0 is not None
                    and committed_offset == int(activation_t0)
                    and local_next is not None
                    and committed_offset > local_next
                )
                if (
                    local_next is None
                    or (
                        committed_offset > local_next
                        and not intentional_activation_skip
                    )
                    or (t0 is not None and committed_offset < t0)
                ):
                    incoherent.append(int(topic_partition.partition))
                continue
            offset = activation_start_fence.get(
                int(topic_partition.partition),
                self.config.initial_offset_for(topic_partition.partition),
            )
            if (
                isinstance(offset, bool)
                or (offset is not None and not isinstance(offset, int))
                or (isinstance(offset, int) and offset < 0)
            ):
                raise RuntimeError("activation_start_fence_invalid")
            if offset is None:
                missing.append(int(topic_partition.partition))
            else:
                to_seek.append((topic_partition, offset))
        if incoherent:
            joined = ",".join(str(value) for value in sorted(incoherent))
            raise RuntimeError(f"broker_local_offset_incoherent:{joined}")
        if missing:
            joined = ",".join(str(value) for value in sorted(missing))
            raise RuntimeError(f"initial_offset_missing_for_partitions:{joined}")
        for topic_partition, offset in to_seek:
            self.consumer.seek(topic_partition, offset)
            self.applied[int(topic_partition.partition)] = offset

    def diagnostics(self) -> dict[str, Any]:
        return {
            "assignment_count": self.assignment_count,
            "revocation_count": self.revocation_count,
            "callback_errors": self.callback_errors,
            "assigned_partitions": list(self.assigned_partitions),
            "last_assignment_at": self.last_assignment_at,
            "last_error_type": self.last_error_type,
            "applied_t0_offsets": {
                str(partition): offset
                for partition, offset in sorted(self.applied.items())
            },
        }


def create_consumer(
    config: ConsumerConfig,
    *,
    store: RcaControlStore | None = None,
) -> Any:
    """Import kafka-python only when a real consumer is explicitly started."""
    try:
        kafka_module = importlib.import_module("kafka")
        KafkaConsumer = kafka_module.KafkaConsumer
        AsyncConsumerRebalanceListener = (
            kafka_module.AsyncConsumerRebalanceListener
        )
    except ImportError as exc:
        raise RuntimeError(
            "kafka-python is not installed in this runtime; install a pinned dependency first"
        ) from exc
    control_store = store or RcaControlStore(
        config.control_db_path,
        require_current=True,
    )
    _require_activation_ingress_open(control_store, config)
    consumer = KafkaConsumer(**config.kafka_kwargs())
    listener_type = type(
        "HermesRcaInitialOffsetListener",
        (ExplicitInitialOffsetListener, AsyncConsumerRebalanceListener),
        {},
    )
    listener = listener_type(consumer, config, control_store)
    consumer.subscribe(topics=(config.topic,), listener=listener)
    # Keep the listener reachable for diagnostics and tests. KafkaConsumer also
    # retains it through subscription state, but that is not a public contract.
    consumer._hermes_rca_initial_offset_listener = listener
    return consumer


def _require_activation_ingress_open(
    store: RcaControlStore,
    config: ConsumerConfig,
) -> None:
    if not config.submit_enabled:
        return
    require_resident_activation_epoch(
        store,
        allowed_states=RESIDENT_INGRESS_OPEN_STATES,
    )


def _default_commit_payload(message: Any) -> dict[Any, Any]:
    structs = importlib.import_module("kafka.structs")
    OffsetAndMetadata = structs.OffsetAndMetadata
    TopicPartition = structs.TopicPartition

    topic_partition = TopicPartition(message.topic, message.partition)
    try:
        offset = OffsetAndMetadata(message.offset + 1, "", -1)
    except TypeError:
        offset = OffsetAndMetadata(message.offset + 1, "")
    return {topic_partition: offset}


def _record_from_message(message: Any) -> KafkaRecord:
    return KafkaRecord(
        topic=message.topic,
        partition=message.partition,
        offset=message.offset,
        value=message.value,
        key=getattr(message, "key", None),
        timestamp_ms=getattr(message, "timestamp", None),
        headers=tuple(getattr(message, "headers", ()) or ()),
    )


def _count_decision(stats: PollStats, decision: str) -> None:
    if decision in {"accepted", "filtered", "invalid", "deduped"}:
        setattr(stats, decision, getattr(stats, decision) + 1)


def recover_pending(
    store: RcaControlStore,
    stats: PollStats,
    *,
    health: HealthReporter | None = None,
    snapshot_authority: W3SnapshotAuthority | None = None,
) -> None:
    """Drain raw-first crash remnants without requiring a Kafka connection."""
    while True:
        try:
            runtime_identity = getattr(health, "runtime_identity", None)
            # Activation lineage is frozen with the raw row at ingress. Recovery
            # must never reinterpret it from the current process configuration.
            pending_kwargs: dict[str, Any] = {"limit": 1000}
            if runtime_identity is not None:
                pending_kwargs["runtime_identity"] = runtime_identity.to_dict()
            if snapshot_authority is not None:
                pending_kwargs["snapshot_authority"] = snapshot_authority
            recovered = store.process_pending(**pending_kwargs)
        except RecordProcessingBlockedError as exc:
            stats.record_processing_blocks += 1
            stats.blocked_partitions = max(1, stats.blocked_partitions)
            if health:
                health.error("recovery_record_blocked", exc)
                health.write(state="partition_blocked", stats=stats, force=True)
            return
        except Exception as exc:
            if health:
                health.error("recovery", exc)
                health.write(state="error", stats=stats, force=True)
            raise ConsumerLoopError("recovery", stats) from exc
        for result in recovered:
            stats.recovered_pending += 1
            _count_decision(stats, result.decision)
        if len(recovered) < 1000:
            break
    if health and stats.recovered_pending:
        health.write(state="recovered_pending", stats=stats, force=True)


def run_poll_loop(
    consumer: Any,
    store: RcaControlStore,
    config: ConsumerConfig,
    *,
    health: HealthReporter | None = None,
    stop_requested: Callable[[], bool] | None = None,
    commit_payload: Callable[[Any], dict[Any, Any]] | None = None,
    max_polls: int | None = None,
    stats: PollStats | None = None,
    recover_on_start: bool = True,
) -> PollStats:
    """Run the steady-only resident Kafka intake loop."""
    stats = stats or PollStats()
    stop_requested = stop_requested or (lambda: False)
    commit_payload = commit_payload or _default_commit_payload
    if health:
        health.write(state="starting", stats=stats, force=True)

    if recover_on_start:
        recover_pending(
            store,
            stats,
            health=health,
            snapshot_authority=config.w3_snapshot_authority,
        )

    backpressure_active = False
    blocked_partitions: set[Any] = set()

    while not stop_requested():
        if max_polls is not None and stats.polls >= max_polls:
            break

        try:
            backlog_reader = getattr(store, "dispatch_backlog_count", lambda: 0)
            backlog = int(backlog_reader())
            stats.max_dispatch_backlog = max(stats.max_dispatch_backlog, backlog)
            assignment = (
                tuple(consumer.assignment())
                if hasattr(consumer, "assignment")
                else ()
            )
            if assignment:
                blocked_partitions.intersection_update(assignment)
                stats.blocked_partitions = len(blocked_partitions)
            if backlog >= config.outbox_high_watermark:
                if assignment:
                    consumer.pause(*assignment)
                if not backpressure_active:
                    stats.backpressure_pauses += 1
                backpressure_active = True
            elif backpressure_active and backlog <= config.outbox_resume_watermark:
                resumable = tuple(
                    partition
                    for partition in assignment
                    if partition not in blocked_partitions
                )
                if resumable:
                    consumer.resume(*resumable)
                stats.backpressure_resumes += 1
                backpressure_active = False
            elif backpressure_active and assignment:
                consumer.pause(*assignment)
        except Exception as exc:
            if health:
                health.error("backpressure", exc)
                health.write(state="error", stats=stats, force=True)
            raise ConsumerLoopError("backpressure", stats) from exc

        try:
            batch = consumer.poll(
                timeout_ms=config.poll_timeout_ms,
                max_records=config.max_poll_records,
            )
        except Exception as exc:
            if health:
                health.error("poll", exc)
                health.write(state="error", stats=stats, force=True)
            raise ConsumerLoopError("poll", stats) from exc
        stats.polls += 1

        if not batch:
            stats.idle_polls += 1
            if health:
                health.write(
                    state=(
                        "partition_blocked"
                        if blocked_partitions
                        else "backpressure"
                        if backpressure_active
                        else "idle"
                    ),
                    stats=stats,
                )
            continue

        for partition, messages in batch.items():
            if partition in blocked_partitions:
                continue
            for message in tuple(messages):
                stats.records_seen += 1
                if health:
                    health.event()
                try:
                    result = store.ingest_record(
                        _record_from_message(message),
                        policy=config.policy,
                        submit_enabled=config.submit_enabled,
                        activation_required=config.activation_required,
                        runtime_identity=(
                            health.runtime_identity.to_dict()
                            if getattr(health, "runtime_identity", None) is not None
                            else None
                        ),
                        **(
                            {"snapshot_authority": config.w3_snapshot_authority}
                            if config.w3_snapshot_authority is not None
                            else {}
                        ),
                    )
                except RecordProcessingBlockedError as exc:
                    stats.ingest_errors += 1
                    stats.record_processing_blocks += 1
                    blocked_partitions.add(partition)
                    stats.blocked_partitions = len(blocked_partitions)
                    try:
                        consumer.pause(partition)
                    except Exception as pause_exc:
                        if health:
                            health.error("partition_pause", pause_exc)
                            health.write(state="error", stats=stats, force=True)
                        raise ConsumerLoopError("partition_pause", stats) from pause_exc
                    if health:
                        health.error("record_processing", exc)
                        health.write(
                            state="partition_blocked", stats=stats, force=True
                        )
                    break
                except Exception as exc:
                    stats.ingest_errors += 1
                    if health:
                        health.error("ingest", exc)
                        health.write(state="error", stats=stats, force=True)
                    raise ConsumerLoopError("ingest", stats) from exc

                _count_decision(stats, result.decision)
                if not result.ack_safe:
                    error = RuntimeError(
                        "durable ingest did not authorize offset commit"
                    )
                    if health:
                        health.error("ack_safety", error)
                        health.write(state="error", stats=stats, force=True)
                    raise ConsumerLoopError("ack_safety", stats) from error
                try:
                    consumer.commit(offsets=commit_payload(message))
                except Exception as exc:
                    stats.commit_errors += 1
                    if health:
                        health.error("commit", exc)
                        health.write(state="error", stats=stats, force=True)
                    raise ConsumerLoopError("commit", stats) from exc
                stats.records_committed += 1
                if health:
                    health.committed(clear_error=not blocked_partitions)
                    health.write(
                        state=(
                            "partition_blocked"
                            if blocked_partitions
                            else "running"
                        ),
                        stats=stats,
                    )

    if health:
        health.write(state="stopped", stats=stats, force=True)
    return stats


def load_consumer_environment(env_file: str | Path | None = None) -> Path:
    path = Path(
        env_file
        or os.environ.get(f"{ENV_PREFIX}ENV_FILE")
        or get_hermes_home() / ".env"
    ).expanduser()
    load_dotenv(path, override=False, interpolate=False)
    return path


def _health_runtime_identity_ok(payload: Mapping[str, Any]) -> bool:
    config = payload.get("config")
    return isinstance(config, Mapping) and runtime_identity_is_valid(
        payload.get("runtime_identity"),
        service_label=SERVICE_LABEL,
        public_config=config,
    )


def read_health_status(
    config: ConsumerConfig,
    *,
    max_age_seconds: int = 60,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Read and qualify the consumer heartbeat without connecting to Kafka."""
    if max_age_seconds < 1:
        raise ValueError("health max age must be at least 1 second")
    try:
        payload = json.loads(config.health_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {"ok": False, "error": "health_file_missing"}
    except (OSError, json.JSONDecodeError):
        return {"ok": False, "error": "health_file_unreadable"}
    if not isinstance(payload, dict):
        return {"ok": False, "error": "health_payload_invalid"}

    heartbeat_text = str(payload.get("heartbeat_at") or "").strip()
    observation_valid = True
    try:
        heartbeat = datetime.fromisoformat(heartbeat_text.replace("Z", "+00:00"))
    except ValueError:
        heartbeat = None
    if heartbeat is None or heartbeat.tzinfo is None:
        age_seconds = None
        fresh = False
    else:
        observed_at = now or datetime.now(timezone.utc)
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            observation_valid = False
            age_seconds = None
            fresh = False
        else:
            age_seconds = (
                observed_at.astimezone(timezone.utc)
                - heartbeat.astimezone(timezone.utc)
            ).total_seconds()
            fresh = -MAX_HEALTH_FUTURE_SKEW_SECONDS <= age_seconds <= max_age_seconds

    active_states = {
        "starting",
        "recovered_pending",
        "idle",
        "running",
    }
    assignment = payload.get("assignment")
    assignment_ok = (
        isinstance(assignment, Mapping)
        and bool(assignment.get("assigned_partitions"))
        and assignment.get("callback_errors") == 0
    )
    store = payload.get("store")
    store_ok = isinstance(store, Mapping) and store.get("ok") is True
    producer_ok = (
        payload.get("schema_version") == CONSUMER_HEALTH_SCHEMA_VERSION
        and payload.get("ok") is True
        and payload.get("healthy") is True
        and payload.get("enabled") is True
        and str(payload.get("state") or "") in active_states
        and assignment_ok
        and store_ok
        and _health_runtime_identity_ok(payload)
    )
    result = dict(payload)
    result["ok"] = bool(producer_ok and fresh)
    result["health_check"] = {
        "fresh": fresh,
        "heartbeat_age_seconds": age_seconds,
        "max_age_seconds": max_age_seconds,
        "checked_at": (now or datetime.now(timezone.utc)).isoformat(),
    }
    if not producer_ok:
        result["health_check"]["reason"] = "consumer_reported_unhealthy"
    elif not observation_valid:
        result["health_check"]["reason"] = "health_observation_invalid"
    elif heartbeat is None or heartbeat.tzinfo is None:
        result["health_check"]["reason"] = "heartbeat_invalid"
    elif age_seconds is not None and age_seconds < -MAX_HEALTH_FUTURE_SKEW_SECONDS:
        result["health_check"]["reason"] = "heartbeat_from_future"
    elif not fresh:
        result["health_check"]["reason"] = "heartbeat_stale"
    return result


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", help="dotenv path; defaults to HERMES_HOME/.env")
    parser.add_argument(
        "--check-config",
        action="store_true",
        help="validate and print redacted config without importing Kafka or connecting",
    )
    parser.add_argument(
        "--health",
        action="store_true",
        help="validate the last health heartbeat without connecting to Kafka",
    )
    parser.add_argument("--health-max-age-seconds", type=int)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    try:
        load_consumer_environment(args.env_file)
        config = ConsumerConfig.from_env()
        if args.check_config:
            print(
                json.dumps(
                    {"ok": True, "config": config.runtime_public_dict()},
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0
        if args.health:
            max_age = args.health_max_age_seconds
            if max_age is None:
                max_age = _integer(
                    os.environ,
                    f"{ENV_PREFIX}HEALTH_MAX_AGE_SECONDS",
                    60,
                )
            status = read_health_status(config, max_age_seconds=max_age)
            print(json.dumps(status, ensure_ascii=False, indent=2, sort_keys=True))
            return 0 if status.get("ok") is True else 2

        store = RcaControlStore(config.control_db_path, require_current=True)
        _require_activation_ingress_open(store, config)
        health = HealthReporter(config, store)
        stats = PollStats()
        recover_pending(
            store,
            stats,
            health=health,
            snapshot_authority=config.w3_snapshot_authority,
        )
        health.write(state="starting", stats=stats, force=True)
        stopping = False

        def request_stop(_signum: int, _frame: Any) -> None:
            nonlocal stopping
            stopping = True

        signal.signal(signal.SIGTERM, request_stop)
        signal.signal(signal.SIGINT, request_stop)
        try:
            consumer = create_consumer(config, store=store)
            health.set_assignment_reporter(
                consumer._hermes_rca_initial_offset_listener.diagnostics
            )
        except Exception as exc:
            health.error("consumer_create", exc)
            health.write(state="error", stats=stats, force=True)
            raise
        try:
            run_poll_loop(
                consumer,
                store,
                config,
                health=health,
                stop_requested=lambda: stopping,
                stats=stats,
                recover_on_start=False,
            )
        finally:
            consumer.close()
        return 0
    except Exception as exc:
        print(
            json.dumps(
                {"ok": False, "error": type(exc).__name__, "message": str(exc)},
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
