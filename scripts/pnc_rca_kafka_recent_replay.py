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
from gateway.pnc_rca_runtime_identity import canonical_json_sha256
from scripts.pnc_rca_kafka_consumer import ConsumerConfig
from scripts.pnc_rca_kafka_preflight import load_environment


SCHEMA_VERSION = "pnc_rca_kafka_recent_replay_v1"
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


class ReplayError(RuntimeError):
    """A stable recent-replay failure without secret-bearing detail."""


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


def _consumer_kwargs(config: ConsumerConfig) -> dict[str, Any]:
    kwargs = config.kafka_kwargs()
    kwargs.update(
        group_id=None,
        enable_auto_commit=False,
        auto_offset_reset="none",
        client_id="root_cause_analysis_agent_recent_replay",
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
    consumer = consumer_factory(**_consumer_kwargs(config))
    started = time.monotonic()
    records: list[dict[str, Any]] = []
    decisions: Counter[str] = Counter()
    reasons: Counter[str] = Counter()
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
        },
    }
    return receipt


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
        env, env_observation = load_environment(args.env_file)
        config = ConsumerConfig.from_env(env)
        receipt = collect_recent_replay(
            config,
            repo_root=REPO_ROOT,
            window_days=args.window_days,
            max_messages=args.max_messages,
            max_bytes=args.max_bytes,
            max_seconds=args.max_seconds,
            env_file_observation=env_observation,
        )
        output = _output_path(args.output)
        raw_sha256 = _write_owner_only(output, receipt)
        print(
            json.dumps(
                {
                    "ok": True,
                    "output": str(output),
                    "raw_sha256": raw_sha256,
                    "records_scanned": receipt["result"]["records_scanned"],
                    "decision_counts": receipt["result"]["decision_counts"],
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 0
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
