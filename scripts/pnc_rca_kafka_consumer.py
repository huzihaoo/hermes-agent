#!/usr/bin/env python3
"""Standalone durable shadow consumer for Feishu workflow-event RCA intake."""

from __future__ import annotations

import argparse
import asyncio
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
    ActivationIngressDeferredError,
    KafkaRecord,
    RcaControlStore,
    RecordProcessingBlockedError,
)
from gateway.pnc_rca_kafka_contract import (
    MAX_WORKFLOW_EVENT_BYTES,
    WorkflowEventPolicy,
)
from gateway.pnc_rca_policy_config import workflow_policy_from_env
from gateway.pnc_rca_runtime_identity import (
    MAX_HEALTH_FUTURE_SKEW_SECONDS,
    RCA_KAFKA_CONSUMER_LOADED_DEPENDENCIES,
    build_runtime_identity,
    canonical_json_sha256,
    runtime_identity_is_valid,
)
from hermes_constants import get_hermes_home


ENV_PREFIX = "HERMES_RCA_KAFKA_"
CONSUMER_HEALTH_SCHEMA_VERSION = "pnc_rca_kafka_consumer_health_v2"
ACTIVATION_FREEZE_SCHEMA_VERSION = "pnc_rca_activation_ingress_freeze_v1"
ACTIVATION_FREEZE_RELEASE_SCHEMA_VERSION = (
    "pnc_rca_activation_ingress_freeze_release_v1"
)
ACTIVATION_FREEZE_REQUIRED_SLOT_COUNT = 3
SERVICE_LABEL = "local.pnc.rca-kafka-consumer"
MAX_CONFIG_JSON_NESTING = 32
FIXED_SERVICE_ID = "root_cause_analysis_agent"
FIXED_KAFKA_GROUP_ID = "rca_root_cause_analysis_agent"
KAFKA_PRINCIPAL_PREFIX = "rca_"
MAX_KAFKA_PRINCIPAL_LENGTH = 128
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
    return (
        len(KAFKA_PRINCIPAL_PREFIX) < len(value) <= MAX_KAFKA_PRINCIPAL_LENGTH
        and value.startswith(KAFKA_PRINCIPAL_PREFIX)
        and all(
            character.isascii() and (character.isalnum() or character in "_.-")
            for character in value
        )
    )


def _required_kafka_principal(env: Mapping[str, str], name: str) -> str:
    raw = str(env.get(name, ""))
    value = _required(env, name)
    if raw != value or not kafka_principal_is_valid(value):
        raise ValueError(
            f"{name} must start with {KAFKA_PRINCIPAL_PREFIX} and contain only "
            "ASCII letters, digits, underscore, dot, or hyphen"
        )
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
            "activation_ingress_freeze_mode": "automatic_bounded_completion",
            "activation_ingress_freeze_restart_required": False,
            "outbox_high_watermark": self.outbox_high_watermark,
            "outbox_resume_watermark": self.outbox_resume_watermark,
            "external_dispatch_wired": False,
            "creation_rule_version": self.policy.policy_version,
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
    activation_deferred: int = 0
    activation_resumed: int = 0
    activation_freezes: int = 0
    activation_freeze_rebuilds: int = 0
    activation_freeze_releases: int = 0


class ConsumerLoopError(RuntimeError):
    """Fatal poll-loop error; restart is required before offsets may advance."""

    def __init__(self, phase: str, stats: PollStats):
        super().__init__(f"Kafka consumer {phase} failed; restart required")
        self.phase = phase
        self.stats = stats


class ActivationFreezeProtocolError(RuntimeError):
    """The atomic store readiness or Kafka pause evidence is not trustworthy."""


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
        self.activation_freeze: dict[str, Any] | None = None
        self.activation_freeze_release: dict[str, Any] | None = None
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
                "activation_freeze_error",
            }
            and stats.blocked_partitions == 0
            and store_health.get("ok") is True
            and assignment_ok
        )
        heartbeat_at = _utc_now()
        if self.activation_freeze is not None:
            self.activation_freeze = {
                **self.activation_freeze,
                "observed_at": heartbeat_at,
            }
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
            "activation_freeze": self.activation_freeze,
            "activation_freeze_release": self.activation_freeze_release,
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

    def set_activation_freeze(self, receipt: Mapping[str, Any]) -> None:
        self.activation_freeze = dict(receipt)
        self.activation_freeze_release = None
        self.last_error = None

    def invalidate_activation_freeze(self) -> None:
        self.activation_freeze = None

    def release_activation_freeze(
        self,
        *,
        epoch_id: str,
        freeze_token: str,
        reason: str,
    ) -> None:
        self.activation_freeze = None
        self.activation_freeze_release = {
            "schema_version": ACTIVATION_FREEZE_RELEASE_SCHEMA_VERSION,
            "epoch_id": epoch_id,
            "state": "released",
            "freeze_token": freeze_token,
            "reason": reason,
            "released_at": _utc_now(),
            "restart_required": False,
        }
        self.last_error = None


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
        local_progress = await asyncio.to_thread(
            self.store.partition_progress,
            topic=self.config.topic,
            partitions=(
                int(topic_partition.partition)
                for topic_partition in assigned_partitions
            ),
        )

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
                t0 = self.config.initial_offset_for(topic_partition.partition)
                if (
                    local_next is None
                    or committed_offset > local_next
                    or (t0 is not None and committed_offset < t0)
                ):
                    incoherent.append(int(topic_partition.partition))
                continue
            offset = self.config.initial_offset_for(topic_partition.partition)
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
    if not config.activation_required:
        return
    epoch = store.activation_epoch()
    state = str((epoch or {}).get("state") or "unconfigured")
    if state not in {"bounded_active", "confirmed", "steady_active"}:
        raise RuntimeError(f"rca_activation_ingress_not_open:{state}")


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


_ACTIVATION_FREEZE_READINESS_FIELDS = frozenset({
    "epoch_id",
    "state",
    "ready",
    "reason",
    "required_slot_count",
    "consumed_slot_count",
    "completed_bound_slot_count",
    "pending_inbox",
    "unbound_ledger",
    "inflight_writes",
})


def _activation_freeze_readiness(store: RcaControlStore) -> dict[str, Any]:
    """Read the control store's activation verdict from one SQLite snapshot."""
    readiness = store.activation_ingress_freeze_readiness()
    if not isinstance(readiness, Mapping) or not _ACTIVATION_FREEZE_READINESS_FIELDS.issubset(
        readiness
    ):
        raise ActivationFreezeProtocolError("activation_freeze_readiness_invalid")

    epoch_id = readiness.get("epoch_id")
    state = readiness.get("state")
    reason = readiness.get("reason")
    ready = readiness.get("ready")
    if (
        not isinstance(epoch_id, str)
        or not isinstance(state, str)
        or not state
        or not isinstance(reason, str)
        or not reason
        or not isinstance(ready, bool)
    ):
        raise ActivationFreezeProtocolError("activation_freeze_readiness_invalid")
    counts: dict[str, int] = {}
    for field_name in (
        "required_slot_count",
        "consumed_slot_count",
        "completed_bound_slot_count",
        "pending_inbox",
        "unbound_ledger",
        "inflight_writes",
    ):
        value = readiness.get(field_name)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ActivationFreezeProtocolError(
                "activation_freeze_readiness_invalid"
            )
        counts[field_name] = value
    if counts["required_slot_count"] < 1:
        raise ActivationFreezeProtocolError("activation_freeze_readiness_invalid")
    allowed_states = {
        "unconfigured",
        "safe_off",
        "preauthorized",
        "bounded_active",
        "confirmed",
        "steady_active",
        "aborted",
    }
    if (
        state not in allowed_states
        or counts["required_slot_count"] != ACTIVATION_FREEZE_REQUIRED_SLOT_COUNT
        or counts["consumed_slot_count"] > counts["required_slot_count"]
        or counts["completed_bound_slot_count"] > counts["required_slot_count"]
        or (state == "unconfigured") is bool(epoch_id)
    ):
        raise ActivationFreezeProtocolError("activation_freeze_readiness_invalid")
    expected_ready = (
        state == "bounded_active"
        and bool(epoch_id)
        and reason == "ready"
        and counts["consumed_slot_count"] == counts["required_slot_count"]
        and counts["completed_bound_slot_count"] == counts["required_slot_count"]
        and counts["pending_inbox"] == 0
        and counts["unbound_ledger"] == 0
        and counts["inflight_writes"] == 0
    )
    if ready is not expected_ready:
        raise ActivationFreezeProtocolError(
            "activation_freeze_readiness_inconsistent"
        )
    if (reason == "ready") is not ready:
        raise ActivationFreezeProtocolError(
            "activation_freeze_readiness_inconsistent"
        )
    return dict(readiness)


def _partition_key(partition: Any, *, expected_topic: str) -> tuple[str, int]:
    topic = getattr(partition, "topic", None)
    partition_id = getattr(partition, "partition", None)
    if (
        topic != expected_topic
        or isinstance(partition_id, bool)
        or not isinstance(partition_id, int)
        or partition_id < 0
    ):
        raise ActivationFreezeProtocolError(
            "activation_freeze_assignment_invalid"
        )
    return str(topic), partition_id


def _offset_value(value: Any, *, phase: str) -> int:
    offset = getattr(value, "offset", value)
    if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
        raise ActivationFreezeProtocolError(
            f"activation_freeze_{phase}_offset_invalid"
        )
    return offset


class ActivationIngressFreezeController:
    """Keep Kafka ingress paused while a bounded activation fence is collected."""

    def __init__(
        self,
        config: ConsumerConfig,
        health: HealthReporter | None,
    ) -> None:
        self.config = config
        self.health = health
        self.active = False
        self.epoch_id = ""
        self.freeze_token = ""
        self.partition_positions: dict[str, dict[str, int]] = {}
        self.assignment_signature = ""
        runtime_identity = getattr(health, "runtime_identity", None)
        self.runtime_identity_sha256 = (
            canonical_json_sha256(runtime_identity.to_dict())
            if runtime_identity is not None
            else ""
        )

    def _assignment(self, consumer: Any) -> tuple[Any, ...]:
        if not hasattr(consumer, "assignment"):
            return ()
        assignment = tuple(consumer.assignment())
        return tuple(
            sorted(
                assignment,
                key=lambda item: _partition_key(
                    item, expected_topic=self.config.topic
                ),
            )
        )

    def _assignment_signature(
        self,
        consumer: Any,
        assignment: tuple[Any, ...],
    ) -> str:
        diagnostics = self._listener_diagnostics(consumer)
        return canonical_json_sha256({
            "assignment": [
                {"topic": topic, "partition": partition_id}
                for topic, partition_id in (
                    _partition_key(item, expected_topic=self.config.topic)
                    for item in assignment
                )
            ],
            "assignment_count": diagnostics.get("assignment_count"),
            "revocation_count": diagnostics.get("revocation_count"),
            "last_assignment_at": diagnostics.get("last_assignment_at"),
            "assigned_partitions": diagnostics.get("assigned_partitions"),
            "callback_errors": diagnostics.get("callback_errors"),
            "applied_t0_offsets": diagnostics.get("applied_t0_offsets"),
        })

    @staticmethod
    def _listener_diagnostics(consumer: Any) -> Mapping[str, Any]:
        listener = getattr(
            consumer, "_hermes_rca_initial_offset_listener", None
        )
        diagnostics_reader = getattr(listener, "diagnostics", None)
        diagnostics: Mapping[str, Any] = {}
        if callable(diagnostics_reader):
            value = diagnostics_reader()
            if not isinstance(value, Mapping):
                raise ActivationFreezeProtocolError(
                    "activation_freeze_assignment_diagnostics_invalid"
                )
            return value
        return diagnostics

    def _validated_applied_t0_offsets(
        self,
        consumer: Any,
        assignment: tuple[Any, ...],
    ) -> dict[int, int]:
        diagnostics = self._listener_diagnostics(consumer)
        assigned = diagnostics.get("assigned_partitions")
        expected_assigned = [
            partition_id
            for _topic, partition_id in (
                _partition_key(item, expected_topic=self.config.topic)
                for item in assignment
            )
        ]
        assignment_count = diagnostics.get("assignment_count")
        revocation_count = diagnostics.get("revocation_count")
        if (
            not isinstance(assigned, list)
            or any(
                isinstance(value, bool) or not isinstance(value, int)
                for value in assigned
            )
            or sorted(assigned) != expected_assigned
            or diagnostics.get("callback_errors") != 0
            or isinstance(assignment_count, bool)
            or not isinstance(assignment_count, int)
            or assignment_count < 1
            or isinstance(revocation_count, bool)
            or not isinstance(revocation_count, int)
            or revocation_count < 0
            or not str(diagnostics.get("last_assignment_at") or "").strip()
        ):
            raise ActivationFreezeProtocolError(
                "activation_freeze_t0_diagnostics_invalid"
            )
        raw_offsets = diagnostics.get("applied_t0_offsets")
        if not isinstance(raw_offsets, Mapping):
            raise ActivationFreezeProtocolError(
                "activation_freeze_t0_diagnostics_invalid"
            )
        offsets: dict[int, int] = {}
        for raw_partition, raw_offset in raw_offsets.items():
            partition_text = str(raw_partition)
            if (
                not partition_text.isdigit()
                or str(int(partition_text)) != partition_text
                or isinstance(raw_offset, bool)
                or not isinstance(raw_offset, int)
                or raw_offset < 0
            ):
                raise ActivationFreezeProtocolError(
                    "activation_freeze_t0_diagnostics_invalid"
                )
            offsets[int(partition_text)] = raw_offset
        if not set(offsets).issubset(expected_assigned):
            raise ActivationFreezeProtocolError(
                "activation_freeze_t0_diagnostics_invalid"
            )
        return offsets

    def _position(
        self,
        consumer: Any,
        partition: Any,
    ) -> int:
        reader = getattr(consumer, "position", None)
        if not callable(reader):
            raise ActivationFreezeProtocolError(
                "activation_freeze_position_reader_unavailable"
            )
        return _offset_value(
            reader(partition, timeout_ms=self.config.offset_lookup_timeout_ms),
            phase="position",
        )

    def _positions(
        self,
        consumer: Any,
        assignment: tuple[Any, ...],
    ) -> dict[str, dict[str, int]]:
        positions: dict[str, dict[str, int]] = {self.config.topic: {}}
        for partition in assignment:
            topic, partition_id = _partition_key(
                partition, expected_topic=self.config.topic
            )
            positions[topic][str(partition_id)] = self._position(
                consumer, partition
            )
        return positions

    def _rewind_to_committed(
        self,
        consumer: Any,
        assignment: tuple[Any, ...],
    ) -> dict[str, dict[str, int]]:
        committed_reader = getattr(consumer, "committed", None)
        seek = getattr(consumer, "seek", None)
        if not callable(committed_reader) or not callable(seek):
            raise ActivationFreezeProtocolError(
                "activation_freeze_offset_reader_unavailable"
            )
        positions: dict[str, dict[str, int]] = {self.config.topic: {}}
        applied_t0_offsets: dict[int, int] | None = None
        for partition in assignment:
            topic, partition_id = _partition_key(
                partition, expected_topic=self.config.topic
            )
            raw_committed = committed_reader(
                partition,
                timeout_ms=self.config.offset_lookup_timeout_ms,
            )
            if raw_committed is None:
                if applied_t0_offsets is None:
                    applied_t0_offsets = self._validated_applied_t0_offsets(
                        consumer, assignment
                    )
                if partition_id not in applied_t0_offsets:
                    raise ActivationFreezeProtocolError(
                        "activation_freeze_uncommitted_t0_missing"
                    )
                committed = applied_t0_offsets[partition_id]
                if self._position(consumer, partition) < committed:
                    raise ActivationFreezeProtocolError(
                        "activation_freeze_uncommitted_t0_position_before_start"
                    )
            else:
                committed = _offset_value(
                    raw_committed,
                    phase="committed",
                )
            seek(partition, committed)
            position = self._position(consumer, partition)
            if position != committed:
                raise ActivationFreezeProtocolError(
                    "activation_freeze_seek_not_effective"
                )
            positions[topic][str(partition_id)] = position
        return positions

    def _publish(
        self,
        *,
        epoch_id: str,
        positions: Mapping[str, Mapping[str, int]],
        assignment_signature: str,
        stats: PollStats,
        rebuild: bool,
    ) -> None:
        if self.health is not None and not self.runtime_identity_sha256:
            raise ActivationFreezeProtocolError(
                "activation_freeze_runtime_identity_unavailable"
            )
        paused_at = _utc_now()
        token = canonical_json_sha256({
            "schema_version": ACTIVATION_FREEZE_SCHEMA_VERSION,
            "epoch_id": epoch_id,
            "paused_at": paused_at,
            "consumer_runtime_identity_sha256": self.runtime_identity_sha256,
            "partition_positions": positions,
            "assignment_signature": assignment_signature,
            "nonce": os.urandom(32).hex(),
        })
        self.active = True
        self.epoch_id = epoch_id
        self.freeze_token = token
        self.partition_positions = {
            topic: dict(partitions) for topic, partitions in positions.items()
        }
        self.assignment_signature = assignment_signature
        if rebuild:
            stats.activation_freeze_rebuilds += 1
        else:
            stats.activation_freezes += 1
        if self.health is not None:
            self.health.set_activation_freeze({
                "schema_version": ACTIVATION_FREEZE_SCHEMA_VERSION,
                "epoch_id": epoch_id,
                "state": "partitions_paused",
                "freeze_token": token,
                "paused_at": paused_at,
                "observed_at": paused_at,
                "consumer_runtime_identity_sha256": (
                    self.runtime_identity_sha256
                ),
                "partition_positions": self.partition_positions,
                "restart_required": False,
            })
            self.health.write(
                state="activation_frozen", stats=stats, force=True
            )

    def ensure_frozen(
        self,
        consumer: Any,
        *,
        epoch_id: str,
        stats: PollStats,
    ) -> None:
        assignment = self._assignment(consumer)
        was_active = self.active
        if not assignment:
            if was_active:
                self.epoch_id = epoch_id
                self.assignment_signature = ""
                self.partition_positions = {}
            if self.health is not None:
                self.health.invalidate_activation_freeze()
                self.health.write(
                    state="starting",
                    stats=stats,
                    force=True,
                )
            return

        self.active = True
        self.epoch_id = epoch_id
        signature = self._assignment_signature(consumer, assignment)
        positions_stable = False
        if was_active and signature == self.assignment_signature:
            positions_stable = (
                self._positions(consumer, assignment)
                == self.partition_positions
            )
        if positions_stable and self.freeze_token:
            return

        consumer.pause(*assignment)
        positions = self._rewind_to_committed(consumer, assignment)
        if (
            self._assignment(consumer) != assignment
            or self._assignment_signature(consumer, assignment) != signature
        ):
            raise ActivationFreezeProtocolError(
                "activation_freeze_assignment_changed"
            )
        self._publish(
            epoch_id=epoch_id,
            positions=positions,
            assignment_signature=signature,
            stats=stats,
            rebuild=bool(self.freeze_token),
        )

    def release(
        self,
        consumer: Any,
        *,
        reason: str,
        backpressure_active: bool,
        blocked_partitions: set[Any],
        stats: PollStats,
    ) -> None:
        if not self.active:
            return
        old_epoch_id = self.epoch_id
        old_token = self.freeze_token
        assignment = self._assignment(consumer)
        if not backpressure_active:
            resumable = tuple(
                partition
                for partition in assignment
                if partition not in blocked_partitions
            )
            if resumable:
                consumer.resume(*resumable)
        stats.activation_freeze_releases += 1
        self.active = False
        self.epoch_id = ""
        self.freeze_token = ""
        self.partition_positions = {}
        self.assignment_signature = ""
        if self.health is not None:
            self.health.release_activation_freeze(
                epoch_id=old_epoch_id,
                freeze_token=old_token,
                reason=reason,
            )
            self.health.write(state="idle", stats=stats, force=True)

    def reconcile(
        self,
        consumer: Any,
        readiness: Mapping[str, Any],
        *,
        backpressure_active: bool,
        blocked_partitions: set[Any],
        stats: PollStats,
        verify_active: bool = True,
    ) -> None:
        epoch_id = str(readiness["epoch_id"])
        state = str(readiness["state"])
        ready = readiness["ready"] is True
        if self.active and (
            epoch_id != self.epoch_id
            or state in {"steady_active", "aborted", "unconfigured"}
        ):
            self.release(
                consumer,
                reason=(
                    "activation_epoch_changed"
                    if epoch_id != self.epoch_id
                    else f"activation_{state}"
                ),
                backpressure_active=backpressure_active,
                blocked_partitions=blocked_partitions,
                stats=stats,
            )

        if state == "confirmed" or ready:
            if self.active and not verify_active:
                return
            self.ensure_frozen(
                consumer,
                epoch_id=epoch_id,
                stats=stats,
            )
            return
        if self.active:
            if verify_active:
                self.ensure_frozen(
                    consumer,
                    epoch_id=epoch_id,
                    stats=stats,
                )
            if self.health is not None:
                error = ActivationFreezeProtocolError(
                    "activation_freeze_readiness_regressed"
                )
                self.health.error("activation_freeze_readiness", error)
                self.health.write(
                    state="activation_freeze_error", stats=stats, force=True
                )


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
    """Poll indefinitely; an empty poll is a heartbeat, never an exit signal."""
    stats = stats or PollStats()
    stop_requested = stop_requested or (lambda: False)
    commit_payload = commit_payload or _default_commit_payload
    if health:
        health.write(state="starting", stats=stats, force=True)

    if recover_on_start:
        recover_pending(store, stats, health=health)

    backpressure_active = False
    blocked_partitions: set[Any] = set()
    deferred_messages: dict[Any, list[tuple[Any, bool]]] = {}
    freeze = ActivationIngressFreezeController(config, health)

    def reconcile_activation(
        *, verify_active: bool = True
    ) -> dict[str, Any] | None:
        if not config.activation_required:
            return None
        try:
            readiness = _activation_freeze_readiness(store)
            freeze.reconcile(
                consumer,
                readiness,
                backpressure_active=backpressure_active,
                blocked_partitions=blocked_partitions,
                stats=stats,
                verify_active=verify_active,
            )
            return readiness
        except Exception as exc:
            stats.ingest_errors += 1
            try:
                assignment = (
                    tuple(consumer.assignment())
                    if hasattr(consumer, "assignment")
                    else ()
                )
                if assignment:
                    consumer.pause(*assignment)
            except Exception:
                pass
            if health:
                health.error("activation_freeze", exc)
                health.write(
                    state="activation_freeze_error", stats=stats, force=True
                )
            raise ConsumerLoopError("activation_freeze", stats) from exc

    while not stop_requested():
        if max_polls is not None and stats.polls >= max_polls:
            break
        readiness = reconcile_activation(verify_active=False)
        for partition, queue in (
            () if freeze.active else tuple(deferred_messages.items())
        ):
            try:
                assignment = (
                    set(consumer.assignment())
                    if hasattr(consumer, "assignment")
                    else set()
                )
                if assignment and partition not in assignment:
                    deferred_messages.pop(partition, None)
                    blocked_partitions.discard(partition)
                    stats.blocked_partitions = len(blocked_partitions)
                    continue
            except Exception as exc:
                stats.ingest_errors += 1
                if health:
                    health.error("activation_retry", exc)
                    health.write(state="error", stats=stats, force=True)
                raise ConsumerLoopError("activation_retry", stats) from exc
            retry_disabled = False
            while queue:
                backlog_reader = getattr(
                    store, "dispatch_backlog_count", lambda: 0
                )
                if int(backlog_reader()) >= config.outbox_high_watermark:
                    break
                message, seen_counted = queue[0]
                if not seen_counted:
                    stats.records_seen += 1
                    queue[0] = (message, True)
                    if health:
                        health.event()
                try:
                    result = store.ingest_record(
                        _record_from_message(message),
                        policy=config.policy,
                        submit_enabled=config.submit_enabled,
                        activation_required=config.activation_required,
                        activation_slot_kind=(
                            "kafka_success" if config.activation_required else ""
                        ),
                        runtime_identity=(
                            health.runtime_identity.to_dict()
                            if getattr(health, "runtime_identity", None) is not None
                            else None
                        ),
                    )
                except ActivationIngressDeferredError:
                    break
                except RecordProcessingBlockedError as exc:
                    deferred_messages.pop(partition, None)
                    retry_disabled = True
                    if health:
                        health.error("record_processing", exc)
                        health.write(
                            state="partition_blocked", stats=stats, force=True
                        )
                    break
                except Exception as exc:
                    stats.ingest_errors += 1
                    if health:
                        health.error("activation_retry", exc)
                        health.write(state="error", stats=stats, force=True)
                    raise ConsumerLoopError("activation_retry", stats) from exc
                _count_decision(stats, result.decision)
                if not result.ack_safe:
                    error = RuntimeError(
                        "activation retry did not authorize offset commit"
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
                queue.pop(0)
                if health:
                    health.committed(clear_error=False)
            if retry_disabled or queue:
                continue
            stats.activation_resumed += 1
            deferred_messages.pop(partition, None)
            blocked_partitions.discard(partition)
            stats.blocked_partitions = len(blocked_partitions)
            if hasattr(consumer, "resume"):
                consumer.resume(partition)
            if health:
                health.committed()
                health.write(state="activation_resumed", stats=stats, force=True)
        try:
            backlog_reader = getattr(store, "dispatch_backlog_count", lambda: 0)
            backlog = int(backlog_reader())
            stats.max_dispatch_backlog = max(stats.max_dispatch_backlog, backlog)
            assignment = tuple(consumer.assignment()) if hasattr(consumer, "assignment") else ()
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
                if resumable and not freeze.active:
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
        was_frozen = freeze.active
        if (
            readiness is not None
            and (freeze.active or readiness["state"] != "steady_active")
        ):
            readiness = reconcile_activation()
        if freeze.active:
            if not batch:
                stats.idle_polls += 1
            if health:
                health.write(
                    state=(
                        "activation_frozen"
                        if freeze.freeze_token
                        else "activation_freeze_rebalancing"
                    ),
                    stats=stats,
                )
            continue
        if was_frozen and not batch:
            stats.idle_polls += 1
            if health:
                health.write(state="idle", stats=stats)
            continue
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

        batch_frozen = False
        for _partition, messages in batch.items():
            message_batch = tuple(messages)
            for message_index, message in enumerate(message_batch):
                if (
                    readiness is not None
                    and readiness["state"] == "bounded_active"
                ):
                    readiness = reconcile_activation()
                    if freeze.active:
                        batch_frozen = True
                        break
                stats.records_seen += 1
                if health:
                    health.event()
                try:
                    result = store.ingest_record(
                        _record_from_message(message),
                        policy=config.policy,
                        submit_enabled=config.submit_enabled,
                        activation_required=config.activation_required,
                        activation_slot_kind=(
                            "kafka_success" if config.activation_required else ""
                        ),
                        runtime_identity=(
                            health.runtime_identity.to_dict()
                            if getattr(health, "runtime_identity", None) is not None
                            else None
                        ),
                    )
                except ActivationIngressDeferredError as exc:
                    stats.ingest_errors += 1
                    stats.record_processing_blocks += 1
                    stats.activation_deferred += 1
                    blocked_partitions.add(_partition)
                    deferred_messages[_partition] = [
                        (item, index == 0)
                        for index, item in enumerate(
                            message_batch[message_index:]
                        )
                    ]
                    stats.blocked_partitions = len(blocked_partitions)
                    try:
                        consumer.pause(_partition)
                    except Exception as pause_exc:
                        if health:
                            health.error("partition_pause", pause_exc)
                            health.write(state="error", stats=stats, force=True)
                        raise ConsumerLoopError("partition_pause", stats) from pause_exc
                    if health:
                        health.error("activation_deferred", exc)
                        health.write(
                            state="activation_deferred", stats=stats, force=True
                        )
                    break
                except RecordProcessingBlockedError as exc:
                    stats.ingest_errors += 1
                    stats.record_processing_blocks += 1
                    blocked_partitions.add(_partition)
                    stats.blocked_partitions = len(blocked_partitions)
                    try:
                        consumer.pause(_partition)
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
                    error = RuntimeError("durable ingest did not authorize offset commit")
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
            if batch_frozen:
                break

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
        "activation_frozen",
        "activation_freeze_rebalancing",
        "activation_freeze_released",
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
        health = HealthReporter(config, store)
        stats = PollStats()
        _require_activation_ingress_open(store, config)
        recover_pending(store, stats, health=health)
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
