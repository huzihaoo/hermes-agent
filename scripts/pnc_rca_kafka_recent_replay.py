#!/usr/bin/env python3
"""Replay a bounded recent Kafka window through the RCA shadow intake chain."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
import hashlib
from importlib import metadata
import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys
import tempfile
import time
from typing import Any, Callable, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gateway.pnc_rca_control_store import KafkaRecord, RcaControlStore
from gateway.pnc_rca_kafka_contract import (
    classify_workflow_event,
    decode_json_object,
)
from gateway.pnc_rca_runtime_identity import canonical_json_sha256
from scripts.pnc_rca_kafka_consumer import ConsumerConfig
from scripts.pnc_rca_kafka_preflight import load_environment


SCHEMA_VERSION = "pnc_rca_kafka_recent_replay_v1"
POLICY_OBSERVATION_SCHEMA_VERSION = "pnc_rca_kafka_policy_observation_v1"
E2E_CANARY_MANIFEST_SCHEMA_VERSION = "pnc_rca_kafka_e2e_canary_manifest_v1"
COMPONENT = "pnc_rca_kafka_recent_replay"
MODULE_PATH = "scripts/pnc_rca_kafka_recent_replay.py"
DEFAULT_WINDOW_DAYS = 7
MAX_WINDOW_DAYS = 7
DEFAULT_MAX_MESSAGES = 5_000
MAX_MESSAGES = 10_000
DEFAULT_MAX_BYTES = 64 * 1024 * 1024
MAX_BYTES = 128 * 1024 * 1024
DEFAULT_MAX_SECONDS = 120
MAX_SECONDS = 300
MAX_OUTPUT_BYTES = 8 * 1024 * 1024
MAX_REPLAY_REQUEST_TIMEOUT_MS = 5_000
MAX_E2E_WORK_ITEMS = 200
MAX_OBSERVED_IDENTITIES = 100
MAX_OBSERVED_IDENTITY_LENGTH = 256
SHA256_HEX_LENGTH = 64


class ReplayError(RuntimeError):
    """A stable recent-replay failure without secret-bearing detail."""


class _HardDeadline:
    def __init__(self, seconds: int) -> None:
        self.seconds = seconds
        self.previous_handler: Any = None
        self.enabled = hasattr(signal, "SIGALRM") and hasattr(signal, "setitimer")

    def __enter__(self) -> "_HardDeadline":
        if self.enabled:
            self.previous_handler = signal.getsignal(signal.SIGALRM)

            def expire(_signum: int, _frame: Any) -> None:
                raise ReplayError("kafka_recent_total_timeout")

            signal.signal(signal.SIGALRM, expire)
            signal.setitimer(signal.ITIMER_REAL, self.seconds)
        return self

    def __exit__(self, _type: Any, _value: Any, _traceback: Any) -> None:
        if self.enabled:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, self.previous_handler)


def _owner_only_json(path_value: str | Path) -> tuple[dict[str, Any], dict[str, Any]]:
    path = Path(path_value).expanduser()
    if not path.is_absolute() or path.is_symlink():
        raise ReplayError("kafka_recent_e2e_manifest_path_unsafe")
    try:
        info = path.lstat()
    except OSError as exc:
        raise ReplayError("kafka_recent_e2e_manifest_unreadable") from exc
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.geteuid()
        or info.st_nlink != 1
        or stat.S_IMODE(info.st_mode) != 0o600
        or info.st_size > MAX_OUTPUT_BYTES
    ):
        raise ReplayError("kafka_recent_e2e_manifest_file_unsafe")
    raw = path.read_bytes()
    final = path.lstat()
    if (
        len(raw) != info.st_size
        or (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)
        != (final.st_dev, final.st_ino, final.st_size, final.st_mtime_ns)
    ):
        raise ReplayError("kafka_recent_e2e_manifest_changed_while_reading")
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ReplayError("kafka_recent_e2e_manifest_invalid_json") from exc
    if not isinstance(payload, dict):
        raise ReplayError("kafka_recent_e2e_manifest_invalid")
    return payload, {
        "path": str(path),
        "sha256": _sha256(raw),
        "mode": "0600",
        "size": len(raw),
    }


def load_e2e_canary_manifest(
    path_value: str | Path,
) -> tuple[frozenset[str], dict[str, Any]]:
    payload, observation = _owner_only_json(path_value)
    if payload.get("schema_version") != E2E_CANARY_MANIFEST_SCHEMA_VERSION:
        raise ReplayError("kafka_recent_e2e_manifest_schema_mismatch")
    raw_ids = payload.get("work_item_ids")
    if (
        not isinstance(raw_ids, list)
        or not raw_ids
        or len(raw_ids) > MAX_E2E_WORK_ITEMS
    ):
        raise ReplayError("kafka_recent_e2e_manifest_work_items_invalid")
    work_item_ids: list[str] = []
    for raw_id in raw_ids:
        value = str(raw_id).strip()
        if not value.isdigit() or int(value) <= 0 or len(value) > 32:
            raise ReplayError("kafka_recent_e2e_manifest_work_item_invalid")
        work_item_ids.append(value)
    if len(work_item_ids) != len(set(work_item_ids)):
        raise ReplayError("kafka_recent_e2e_manifest_work_item_duplicate")
    source = payload.get("source")
    if not isinstance(source, dict):
        raise ReplayError("kafka_recent_e2e_manifest_source_invalid")
    for name in (
        "project_key",
        "project_simple_name",
        "work_item_type_key",
        "feishu_plugin_source",
        "feishu_receipt_path",
    ):
        value = source.get(name)
        if (
            not isinstance(value, str)
            or not value.strip()
            or value != value.strip()
            or len(value) > MAX_OBSERVED_IDENTITY_LENGTH
        ):
            raise ReplayError("kafka_recent_e2e_manifest_source_invalid")
    receipt_path = str(source["feishu_receipt_path"])
    if (
        not Path(receipt_path).expanduser().is_absolute()
        or "\x00" in receipt_path
        or "\r" in receipt_path
        or "\n" in receipt_path
    ):
        raise ReplayError("kafka_recent_e2e_manifest_source_invalid")
    for name in ("screenshot_sha256", "feishu_receipt_sha256"):
        value = source.get(name)
        if (
            not isinstance(value, str)
            or len(value) != SHA256_HEX_LENGTH
            or any(character not in "0123456789abcdef" for character in value)
        ):
            raise ReplayError("kafka_recent_e2e_manifest_source_invalid")
    observation.update(
        work_item_count=len(work_item_ids),
        source={
            name: source[name]
            for name in (
                "project_key",
                "project_simple_name",
                "work_item_type_key",
                "feishu_plugin_source",
                "feishu_receipt_path",
                "screenshot_sha256",
                "feishu_receipt_sha256",
            )
        },
    )
    return frozenset(work_item_ids), observation


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _timestamp(value: datetime) -> str:
    return _utc(value).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _component_binding(repo_root: Path) -> dict[str, Any]:
    module = repo_root / MODULE_PATH
    raw = module.read_bytes()

    def git(*args: str) -> subprocess.CompletedProcess[bytes]:
        return subprocess.run(
            ["git", *args], cwd=repo_root, capture_output=True, check=False
        )

    head = git("rev-parse", "HEAD")
    commit = head.stdout.decode("ascii", "ignore").strip()
    committed = git("show", f"HEAD:{MODULE_PATH}")
    status = git("status", "--porcelain", "--", MODULE_PATH)
    return {
        "component": COMPONENT,
        "component_commit": commit,
        "module": MODULE_PATH,
        "module_sha256": _sha256(raw),
        "committed_match": committed.returncode == 0
        and _sha256(committed.stdout) == _sha256(raw),
        "module_clean": status.returncode == 0 and not status.stdout.strip(),
    }


def _consumer_kwargs(
    config: ConsumerConfig,
    *,
    max_seconds: int,
) -> dict[str, Any]:
    kwargs = config.kafka_kwargs()
    bounded_request_timeout_ms = max(
        1_000,
        min(MAX_REPLAY_REQUEST_TIMEOUT_MS, max_seconds * 1_000),
    )
    bounded_session_timeout_ms = max(
        100,
        min(config.session_timeout_ms, bounded_request_timeout_ms - 100),
    )
    kwargs.update(
        group_id=None,
        enable_auto_commit=False,
        auto_offset_reset="none",
        client_id="root_cause_analysis_agent_recent_replay",
        request_timeout_ms=bounded_request_timeout_ms,
        session_timeout_ms=bounded_session_timeout_ms,
        heartbeat_interval_ms=max(1, bounded_session_timeout_ms // 3),
        bootstrap_timeout_ms=bounded_request_timeout_ms,
    )
    return kwargs


def _message_value(message: Any) -> bytes:
    value = getattr(message, "value", b"")
    if isinstance(value, bytes):
        return value
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, str):
        return value.encode("utf-8")
    raise ReplayError("kafka_recent_message_value_invalid")


def _safe_store_summary(store: RcaControlStore) -> dict[str, Any]:
    health = store.health()
    return {
        "inbox": dict(sorted((health.get("inbox") or {}).items())),
        "outbox": dict(sorted((health.get("outbox") or {}).items())),
        "replay_raw_retained": dict(health.get("replay_raw") or {}),
    }


def _policy_observation_config(
    env: Mapping[str, str],
    *,
    hermes_home: str | Path | None = None,
) -> ConsumerConfig:
    source = dict(env)
    source.update({
        "HERMES_RCA_KAFKA_CREATION_RULE_VERSION": "observation-only-not-approved",
        "HERMES_RCA_KAFKA_PROJECT_KEYS": "__observation_only__",
        "HERMES_RCA_KAFKA_PROJECT_SIMPLE_NAMES": "__observation_only__",
        "HERMES_RCA_KAFKA_WORK_ITEM_TYPE_KEYS": "__observation_only__",
        "HERMES_RCA_KAFKA_STATUS_CHANGE_TYPES": "__observation_only__",
        "HERMES_RCA_KAFKA_STATE_TRANSITIONS_JSON": json.dumps(
            [
                {
                    "state_key": "__observation_only__",
                    "pre_status": 0,
                    "cur_status": 1,
                }
            ],
            separators=(",", ":"),
        ),
    })
    return ConsumerConfig.from_env(source, hermes_home=hermes_home)


class _PolicyObservation:
    def __init__(self, expected_work_item_ids: frozenset[str]) -> None:
        self.expected_work_item_ids = expected_work_item_ids
        self.expected_observed: set[str] = set()
        self.expected_records = 0
        self.records_seen = 0
        self.records_decoded = 0
        self.invalid_records = 0
        self.invalid_fields: Counter[str] = Counter()
        self.overflows: Counter[str] = Counter()
        self.fields: dict[str, Counter[str]] = {
            name: Counter()
            for name in (
                "project_key",
                "project_simple_name",
                "work_item_type_key",
                "status_change_type",
            )
        }
        self.expected_fields: dict[str, Counter[str]] = {
            name: Counter() for name in self.fields
        }
        self.expected_schema: dict[str, Counter[str]] = {}
        self.transitions: Counter[tuple[str, str, str]] = Counter()
        self.expected_transitions: Counter[tuple[str, str, str]] = Counter()

    def _identity(self, payload: Mapping[str, Any], name: str) -> str | None:
        value = payload.get(name)
        if (
            not isinstance(value, str)
            or not value
            or value != value.strip()
            or len(value) > MAX_OBSERVED_IDENTITY_LENGTH
            or "\r" in value
            or "\n" in value
        ):
            self.invalid_fields[name] += 1
            return None
        return value

    def _bounded_add(self, counter: Counter[Any], key: Any, name: str) -> None:
        if key not in counter and len(counter) >= MAX_OBSERVED_IDENTITIES:
            self.overflows[name] += 1
            return
        counter[key] += 1

    @staticmethod
    def _type_name(value: Any) -> str:
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "boolean"
        if isinstance(value, int):
            return "integer"
        if isinstance(value, float):
            return "number"
        if isinstance(value, str):
            return "string"
        if isinstance(value, list):
            return "array"
        if isinstance(value, dict):
            return "object"
        return "unknown"

    def observe(self, raw: bytes) -> None:
        self.records_seen += 1
        try:
            payload = decode_json_object(raw)
        except ValueError:
            self.invalid_records += 1
            return
        self.records_decoded += 1

        raw_work_item_id = payload.get("id")
        work_item_id = ""
        if not isinstance(raw_work_item_id, bool):
            work_item_id = str(raw_work_item_id or "").strip()
        is_expected = work_item_id in self.expected_work_item_ids
        if is_expected:
            self.expected_observed.add(work_item_id)
            self.expected_records += 1
            for field, value in payload.items():
                if (
                    not field
                    or field != field.strip()
                    or len(field) > MAX_OBSERVED_IDENTITY_LENGTH
                    or "\r" in field
                    or "\n" in field
                ):
                    self.overflows["expected_schema_field"] += 1
                    continue
                if (
                    field not in self.expected_schema
                    and len(self.expected_schema) >= MAX_OBSERVED_IDENTITIES
                ):
                    self.overflows["expected_schema_fields"] += 1
                    continue
                self.expected_schema.setdefault(field, Counter())[
                    self._type_name(value)
                ] += 1

        for name, counter in self.fields.items():
            value = self._identity(payload, name)
            if value is not None:
                self._bounded_add(counter, value, name)
                if is_expected:
                    self._bounded_add(
                        self.expected_fields[name],
                        value,
                        f"expected_{name}",
                    )

        nodes = payload.get("nodes")
        if not isinstance(nodes, list) or len(nodes) > 100:
            self.invalid_fields["nodes"] += 1
            return
        for node in nodes:
            if not isinstance(node, dict):
                self.invalid_fields["node"] += 1
                continue
            state_key = self._identity(node, "state_key")
            pre_status = node.get("pre_status")
            cur_status = node.get("cur_status")
            if (
                state_key is None
                or isinstance(pre_status, bool)
                or not isinstance(pre_status, (str, int))
                or isinstance(cur_status, bool)
                or not isinstance(cur_status, (str, int))
            ):
                self.invalid_fields["transition"] += 1
                continue
            pre_text = str(pre_status)
            cur_text = str(cur_status)
            if (
                not pre_text
                or not cur_text
                or len(pre_text) > MAX_OBSERVED_IDENTITY_LENGTH
                or len(cur_text) > MAX_OBSERVED_IDENTITY_LENGTH
            ):
                self.invalid_fields["transition"] += 1
                continue
            self._bounded_add(
                self.transitions,
                (state_key, pre_text, cur_text),
                "transitions",
            )
            if is_expected:
                self._bounded_add(
                    self.expected_transitions,
                    (state_key, pre_text, cur_text),
                    "expected_transitions",
                )

    @staticmethod
    def _fields_receipt(
        fields: Mapping[str, Counter[str]],
    ) -> dict[str, list[dict[str, Any]]]:
        return {
            name: [
                {"value": value, "count": count}
                for value, count in sorted(counter.items())
            ]
            for name, counter in fields.items()
        }

    @staticmethod
    def _transitions_receipt(
        transitions: Counter[tuple[str, str, str]],
    ) -> list[dict[str, Any]]:
        return [
            {
                "state_key": key[0],
                "pre_status": key[1],
                "cur_status": key[2],
                "count": count,
            }
            for key, count in sorted(transitions.items())
        ]

    def receipt(self) -> dict[str, Any]:
        expected = sorted(self.expected_work_item_ids, key=int)
        observed = sorted(self.expected_observed, key=int)
        return {
            "records_seen": self.records_seen,
            "records_decoded": self.records_decoded,
            "invalid_records": self.invalid_records,
            "invalid_field_counts": dict(sorted(self.invalid_fields.items())),
            "overflow_counts": dict(sorted(self.overflows.items())),
            "fields": self._fields_receipt(self.fields),
            "transitions": self._transitions_receipt(self.transitions),
            "e2e_canary": {
                "required": bool(expected),
                "expected_work_item_ids": expected,
                "observed_work_item_ids": observed,
                "missing_work_item_ids": sorted(
                    self.expected_work_item_ids - self.expected_observed,
                    key=int,
                ),
                "complete": bool(expected)
                and self.expected_work_item_ids == self.expected_observed,
                "observed_policy": {
                    "record_count": self.expected_records,
                    "fields": self._fields_receipt(self.expected_fields),
                    "schema": [
                        {
                            "field": field,
                            "present_count": sum(types.values()),
                            "types": [
                                {"type": type_name, "count": count}
                                for type_name, count in sorted(types.items())
                            ],
                        }
                        for field, types in sorted(self.expected_schema.items())
                    ],
                    "transitions": self._transitions_receipt(
                        self.expected_transitions
                    ),
                },
            },
        }


def collect_recent_replay(
    config: ConsumerConfig,
    *,
    repo_root: Path = REPO_ROOT,
    consumer_factory: Callable[..., Any] | None = None,
    topic_partition_factory: Callable[[str, int], Any] | None = None,
    now: datetime | None = None,
    window_days: int = DEFAULT_WINDOW_DAYS,
    max_messages: int = DEFAULT_MAX_MESSAGES,
    max_bytes: int = DEFAULT_MAX_BYTES,
    max_seconds: int = DEFAULT_MAX_SECONDS,
    env_file_observation: Mapping[str, Any] | None = None,
    expected_work_item_ids: frozenset[str] = frozenset(),
    e2e_manifest_observation: Mapping[str, Any] | None = None,
    raw_observer: Callable[[bytes], None] | None = None,
) -> dict[str, Any]:
    if metadata.version("kafka-python") != "3.0.7":
        raise ReplayError("kafka_recent_dependency_version_mismatch")
    if not 1 <= window_days <= MAX_WINDOW_DAYS:
        raise ReplayError("kafka_recent_window_invalid")
    if not 1 <= max_messages <= MAX_MESSAGES:
        raise ReplayError("kafka_recent_message_limit_invalid")
    if not 1 <= max_bytes <= MAX_BYTES:
        raise ReplayError("kafka_recent_byte_limit_invalid")
    if not 1 <= max_seconds <= MAX_SECONDS:
        raise ReplayError("kafka_recent_time_limit_invalid")

    if consumer_factory is None or topic_partition_factory is None:
        kafka = __import__("kafka")
        consumer_factory = consumer_factory or kafka.KafkaConsumer
        topic_partition_factory = topic_partition_factory or kafka.TopicPartition

    observed_at = _utc(now or datetime.now(timezone.utc))
    window_start = observed_at - timedelta(days=window_days)
    start_ms = int(window_start.timestamp() * 1000)
    started = time.monotonic()
    consumer_kwargs = _consumer_kwargs(config, max_seconds=max_seconds)
    consumer = consumer_factory(**consumer_kwargs)
    records: list[dict[str, Any]] = []
    decisions: Counter[str] = Counter()
    reasons: Counter[str] = Counter()
    matched_expected_work_items: set[str] = set()
    unexpected_accepted_work_items = 0
    total_bytes = 0
    stop_reason = "partition_end_offsets_reached"
    partition_receipts: list[dict[str, Any]] = []
    try:
        raw_partitions = consumer.partitions_for_topic(config.topic)
        if not isinstance(raw_partitions, set) or not raw_partitions:
            raise ReplayError("kafka_recent_partitions_missing")
        partitions = sorted(raw_partitions)
        if partitions != list(range(len(partitions))):
            raise ReplayError("kafka_recent_partitions_noncontiguous")
        topic_partitions = [
            topic_partition_factory(config.topic, partition)
            for partition in partitions
        ]
        beginning = consumer.beginning_offsets(topic_partitions)
        ending = consumer.end_offsets(topic_partitions)
        located = consumer.offsets_for_times(
            {topic_partition: start_ms for topic_partition in topic_partitions}
        )
        start_offsets: dict[Any, int] = {}
        next_offsets: dict[Any, int] = {}
        for topic_partition in topic_partitions:
            begin = int(beginning[topic_partition])
            end = int(ending[topic_partition])
            located_value = located.get(topic_partition)
            located_offset = (
                end if located_value is None else int(located_value.offset)
            )
            start = max(begin, min(located_offset, end))
            start_offsets[topic_partition] = start
            next_offsets[topic_partition] = start
            partition_receipts.append(
                {
                    "partition": int(topic_partition.partition),
                    "beginning_offset": begin,
                    "window_start_offset": start,
                    "fixed_end_offset": end,
                    "records_scanned": 0,
                }
            )
        consumer.assign(topic_partitions)
        for topic_partition in topic_partitions:
            consumer.seek(topic_partition, start_offsets[topic_partition])

        with tempfile.TemporaryDirectory(prefix="rca-kafka-recent-shadow-") as temporary:
            store = RcaControlStore(Path(temporary) / "control.sqlite3")
            while True:
                if all(
                    next_offsets[item] >= int(ending[item])
                    for item in topic_partitions
                ):
                    break
                if len(records) >= max_messages:
                    stop_reason = "message_limit_reached"
                    break
                if total_bytes >= max_bytes:
                    stop_reason = "byte_limit_reached"
                    break
                if time.monotonic() - started >= max_seconds:
                    stop_reason = "time_limit_reached"
                    break
                polled = consumer.poll(
                    timeout_ms=min(config.poll_timeout_ms, 1_000),
                    max_records=min(10, max_messages - len(records)),
                )
                if not isinstance(polled, Mapping):
                    raise ReplayError("kafka_recent_poll_invalid")
                for topic_partition in sorted(
                    polled,
                    key=lambda item: (str(item.topic), int(item.partition)),
                ):
                    if topic_partition not in next_offsets:
                        raise ReplayError("kafka_recent_unassigned_partition")
                    for message in polled[topic_partition]:
                        offset = int(message.offset)
                        end = int(ending[topic_partition])
                        if offset < next_offsets[topic_partition] or offset >= end:
                            raise ReplayError("kafka_recent_offset_out_of_window")
                        raw = _message_value(message)
                        if total_bytes + len(raw) > max_bytes:
                            stop_reason = "byte_limit_reached"
                            break
                        if raw_observer is not None:
                            raw_observer(raw)
                        kafka_record = KafkaRecord(
                            topic=str(message.topic),
                            partition=int(message.partition),
                            offset=offset,
                            value=raw,
                            key=getattr(message, "key", None),
                            timestamp_ms=getattr(message, "timestamp", None),
                            headers=tuple(getattr(message, "headers", ()) or ()),
                        )
                        result = store.ingest_record(
                            kafka_record,
                            policy=config.policy,
                            submit_enabled=False,
                            activation_required=False,
                        )
                        next_offsets[topic_partition] = offset + 1
                        partition_receipts[int(topic_partition.partition)][
                            "records_scanned"
                        ] += 1
                        total_bytes += len(raw)
                        decisions[result.decision] += 1
                        reasons[result.reason] += 1
                        accepted_work_item_id: str | None = None
                        if result.decision == "accepted":
                            classified = classify_workflow_event(
                                topic=config.topic,
                                value=raw,
                                policy=config.policy,
                            )
                            if (
                                classified.decision != "accepted"
                                or classified.normalized is None
                            ):
                                raise ReplayError(
                                    "kafka_recent_accepted_classification_drift"
                                )
                            accepted_work_item_id = (
                                classified.normalized.work_item_id
                            )
                            if accepted_work_item_id in expected_work_item_ids:
                                matched_expected_work_items.add(
                                    accepted_work_item_id
                                )
                            elif expected_work_item_ids:
                                unexpected_accepted_work_items += 1
                        records.append(
                            {
                                "partition": int(message.partition),
                                "offset": offset,
                                "timestamp_ms": getattr(message, "timestamp", None),
                                "value_bytes": len(raw),
                                "value_sha256": _sha256(raw),
                                "decision": result.decision,
                                "reason": result.reason,
                                "event_uid_sha256": _sha256(
                                    result.event_uid.encode("utf-8")
                                ),
                                "business_key_sha256": _sha256(
                                    result.business_key.encode("utf-8")
                                )
                                if result.business_key
                                else None,
                                "submission_key_sha256": _sha256(
                                    result.submission_key.encode("utf-8")
                                )
                                if result.submission_key
                                else None,
                                "trigger_created": result.trigger_created,
                                "outbox_created": result.outbox_created,
                                "expected_e2e_work_item": (
                                    accepted_work_item_id in expected_work_item_ids
                                    if accepted_work_item_id is not None
                                    else False
                                ),
                            }
                        )
                    if stop_reason == "byte_limit_reached":
                        break
            shadow_store = _safe_store_summary(store)
    finally:
        consumer.close(autocommit=False)

    component = _component_binding(repo_root)
    policy = config.policy.to_dict()
    receipt = {
        "schema_version": SCHEMA_VERSION,
        "observed_at": _timestamp(observed_at),
        "source": component,
        "config": {
            "topic": config.topic,
            "cluster_binding": {
                "bootstrap_servers_sha256": canonical_json_sha256(
                    list(config.bootstrap_servers)
                ),
                "principal_sha256": canonical_json_sha256(config.username),
                "env_file": dict(env_file_observation or {}),
            },
            "policy_sha256": canonical_json_sha256(policy),
        },
        "window": {
            "days": window_days,
            "started_at": _timestamp(window_start),
            "ended_at": _timestamp(observed_at),
            "start_timestamp_ms": start_ms,
            "partitions": partition_receipts,
        },
        "limits": {
            "max_messages": max_messages,
            "max_bytes": max_bytes,
            "max_seconds": max_seconds,
        },
        "transport": {
            "assignment": "explicit",
            "group_id": None,
            "subscribed": False,
            "group_joined": False,
            "enable_auto_commit": False,
            "commit_performed": False,
            "allow_auto_create_topics": False,
            "isolation_level": "read_committed",
            "request_timeout_ms": consumer_kwargs["request_timeout_ms"],
            "bootstrap_timeout_ms": consumer_kwargs["bootstrap_timeout_ms"],
        },
        "result": {
            "stop_reason": stop_reason,
            "records_scanned": len(records),
            "raw_bytes_scanned": total_bytes,
            "decision_counts": dict(sorted(decisions.items())),
            "reason_counts": dict(sorted(reasons.items())),
            "records": records,
            "shadow_store": shadow_store,
            "production_mutation_performed": False,
            "raw_payload_persisted_to_output": False,
            "temporary_store_destroyed": True,
            "e2e_canary": {
                "required": bool(expected_work_item_ids),
                "manifest": dict(e2e_manifest_observation or {}),
                "expected_work_item_ids": sorted(
                    expected_work_item_ids, key=int
                ),
                "matched_work_item_ids": sorted(
                    matched_expected_work_items, key=int
                ),
                "missing_work_item_ids": sorted(
                    expected_work_item_ids - matched_expected_work_items,
                    key=int,
                ),
                "unexpected_accepted_work_items": (
                    unexpected_accepted_work_items
                ),
                "complete": bool(expected_work_item_ids)
                and expected_work_item_ids == matched_expected_work_items,
            },
        },
    }
    return receipt


def collect_recent_policy_observation(
    config: ConsumerConfig,
    *,
    repo_root: Path = REPO_ROOT,
    consumer_factory: Callable[..., Any] | None = None,
    topic_partition_factory: Callable[[str, int], Any] | None = None,
    now: datetime | None = None,
    window_days: int = DEFAULT_WINDOW_DAYS,
    max_messages: int = DEFAULT_MAX_MESSAGES,
    max_bytes: int = DEFAULT_MAX_BYTES,
    max_seconds: int = DEFAULT_MAX_SECONDS,
    env_file_observation: Mapping[str, Any] | None = None,
    expected_work_item_ids: frozenset[str] = frozenset(),
    e2e_manifest_observation: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    observer = _PolicyObservation(expected_work_item_ids)
    replay = collect_recent_replay(
        config,
        repo_root=repo_root,
        consumer_factory=consumer_factory,
        topic_partition_factory=topic_partition_factory,
        now=now,
        window_days=window_days,
        max_messages=max_messages,
        max_bytes=max_bytes,
        max_seconds=max_seconds,
        env_file_observation=env_file_observation,
        raw_observer=observer.observe,
    )
    return {
        "schema_version": POLICY_OBSERVATION_SCHEMA_VERSION,
        "observed_at": replay["observed_at"],
        "source": replay["source"],
        "production_eligible": False,
        "owner_approval_required": [
            "creation_rule_version",
            "project_keys",
            "project_simple_names",
            "work_item_type_keys",
            "status_change_types",
            "state_transitions",
        ],
        "config": {
            "topic": config.topic,
            "cluster_binding": replay["config"]["cluster_binding"],
            "e2e_manifest": dict(e2e_manifest_observation or {}),
            "synthetic_transport_policy_used_for_admission": True,
        },
        "window": replay["window"],
        "limits": replay["limits"],
        "transport": replay["transport"],
        "result": {
            **observer.receipt(),
            "stop_reason": replay["result"]["stop_reason"],
            "records_scanned": replay["result"]["records_scanned"],
            "raw_bytes_scanned": replay["result"]["raw_bytes_scanned"],
            "production_mutation_performed": False,
            "raw_payload_persisted_to_output": False,
            "temporary_store_destroyed": True,
        },
    }


def _positive(value: str, *, name: str, maximum: int) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{name} must be an integer") from exc
    if not 1 <= parsed <= maximum:
        raise argparse.ArgumentTypeError(f"{name} must be between 1 and {maximum}")
    return parsed


def _output_path(value: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute() or "\x00" in value or "\n" in value or "\r" in value:
        raise ValueError("output path must be one absolute path")
    return path


def _write_owner_only(path: Path, payload: Mapping[str, Any]) -> str:
    raw = _canonical_bytes(payload) + b"\n"
    if len(raw) > MAX_OUTPUT_BYTES:
        raise ReplayError("kafka_recent_output_too_large")
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    parent = path.parent.resolve(strict=True)
    parent_info = parent.lstat()
    if (
        parent != path.parent
        or not stat.S_ISDIR(parent_info.st_mode)
        or stat.S_ISLNK(parent_info.st_mode)
        or parent_info.st_uid != os.geteuid()
        or path.is_symlink()
    ):
        raise ReplayError("kafka_recent_output_path_unsafe")
    if path.exists():
        existing = path.lstat()
        if (
            not stat.S_ISREG(existing.st_mode)
            or existing.st_uid != os.geteuid()
            or existing.st_nlink != 1
        ):
            raise ReplayError("kafka_recent_output_file_unsafe")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=parent
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    observed = path.lstat()
    if (
        not stat.S_ISREG(observed.st_mode)
        or observed.st_uid != os.geteuid()
        or observed.st_nlink != 1
        or stat.S_IMODE(observed.st_mode) != 0o600
    ):
        raise ReplayError("kafka_recent_output_file_unsafe")
    return _sha256(raw)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file")
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--e2e-canary-manifest",
        help="owner-only manifest of real Feishu work items expected in Kafka",
    )
    parser.add_argument(
        "--observe-policy-only",
        action="store_true",
        help=(
            "observe recent workflow identity/transition counts without an "
            "approved policy; output is never production-eligible"
        ),
    )
    parser.add_argument(
        "--window-days",
        type=lambda value: _positive(
            value, name="window-days", maximum=MAX_WINDOW_DAYS
        ),
        default=DEFAULT_WINDOW_DAYS,
    )
    parser.add_argument(
        "--max-messages",
        type=lambda value: _positive(value, name="max-messages", maximum=MAX_MESSAGES),
        default=DEFAULT_MAX_MESSAGES,
    )
    parser.add_argument(
        "--max-bytes",
        type=lambda value: _positive(value, name="max-bytes", maximum=MAX_BYTES),
        default=DEFAULT_MAX_BYTES,
    )
    parser.add_argument(
        "--max-seconds",
        type=lambda value: _positive(value, name="max-seconds", maximum=MAX_SECONDS),
        default=DEFAULT_MAX_SECONDS,
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config: ConsumerConfig | None = None
    try:
        with _HardDeadline(args.max_seconds):
            env, env_observation = load_environment(args.env_file)
            expected_work_item_ids: frozenset[str] = frozenset()
            e2e_manifest_observation: Mapping[str, Any] | None = None
            if args.e2e_canary_manifest:
                (
                    expected_work_item_ids,
                    e2e_manifest_observation,
                ) = load_e2e_canary_manifest(args.e2e_canary_manifest)
            if args.observe_policy_only:
                config = _policy_observation_config(env)
                receipt = collect_recent_policy_observation(
                    config,
                    repo_root=REPO_ROOT,
                    window_days=args.window_days,
                    max_messages=args.max_messages,
                    max_bytes=args.max_bytes,
                    max_seconds=args.max_seconds,
                    env_file_observation=env_observation,
                    expected_work_item_ids=expected_work_item_ids,
                    e2e_manifest_observation=e2e_manifest_observation,
                )
            else:
                config = ConsumerConfig.from_env(env)
                receipt = collect_recent_replay(
                    config,
                    repo_root=REPO_ROOT,
                    window_days=args.window_days,
                    max_messages=args.max_messages,
                    max_bytes=args.max_bytes,
                    max_seconds=args.max_seconds,
                    env_file_observation=env_observation,
                    expected_work_item_ids=expected_work_item_ids,
                    e2e_manifest_observation=e2e_manifest_observation,
                )
        output = _output_path(args.output)
        raw_sha256 = _write_owner_only(output, receipt)
        e2e = receipt["result"]["e2e_canary"]
        complete = not e2e["required"] or e2e["complete"]
        print(
            json.dumps(
                {
                    "ok": complete,
                    "output": str(output),
                    "raw_sha256": raw_sha256,
                    "records_scanned": receipt["result"]["records_scanned"],
                    "schema_version": receipt["schema_version"],
                    "e2e_canary_complete": e2e["complete"],
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 0 if complete else 3
    except Exception as exc:
        message = str(exc)
        if config is not None:
            message = message.replace(config.password, "[REDACTED]")
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": type(exc).__name__,
                    "message_sha256": _sha256(message.encode("utf-8")),
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
