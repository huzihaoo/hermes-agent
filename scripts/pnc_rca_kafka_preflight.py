#!/usr/bin/env python3
"""Collect read-only Kafka metadata evidence for the RCA release gate."""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import importlib
from importlib import metadata
import io
import json
import logging
import os
from pathlib import Path
import stat
import sys
import tempfile
from typing import Any, Callable, Mapping

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dotenv import dotenv_values

from hermes_constants import get_hermes_home


ENV_PREFIX = "HERMES_RCA_KAFKA_"
FIXED_SERVICE_ID = "root_cause_analysis_agent"
KAFKA_PRINCIPAL_PREFIX = "rca_"
MAX_KAFKA_PRINCIPAL_LENGTH = 128
MAX_ENV_FILE_BYTES = 1024 * 1024
BROKER_METADATA_SCHEMA_VERSION = "pnc_rca_broker_metadata_v3"
BROKER_OBSERVATION_SCHEMA_VERSION = "pnc_rca_broker_observation_v1"
COLLECTOR_SCHEMA_VERSION = "pnc_rca_kafka_preflight_v2"
REQUIRED_AUTHORIZED_OPERATIONS = frozenset({"DESCRIBE", "READ"})
KNOWN_AUTHORIZED_OPERATIONS = frozenset({
    "ALL",
    "ALTER",
    "ALTER_CONFIGS",
    "ANY",
    "CLUSTER_ACTION",
    "CREATE",
    "CREATE_TOKENS",
    "DELETE",
    "DESCRIBE",
    "DESCRIBE_CONFIGS",
    "DESCRIBE_TOKENS",
    "IDEMPOTENT_WRITE",
    "READ",
    "WRITE",
})
MUTATING_AUTHORIZED_OPERATIONS = frozenset({
    "ALL",
    "ALTER",
    "ALTER_CONFIGS",
    "ANY",
    "CLUSTER_ACTION",
    "CREATE",
    "CREATE_TOKENS",
    "DELETE",
    "IDEMPOTENT_WRITE",
    "WRITE",
})


def _required(env: Mapping[str, str], name: str) -> str:
    value = str(env.get(name, "")).strip()
    if not value:
        raise ValueError(f"missing required environment variable: {name}")
    return value


def _required_kafka_principal(env: Mapping[str, str], name: str) -> str:
    raw = str(env.get(name, ""))
    value = _required(env, name)
    valid_characters = all(
        character.isascii() and (character.isalnum() or character in "_.-")
        for character in value
    )
    if (
        raw != value
        or len(value) <= len(KAFKA_PRINCIPAL_PREFIX)
        or len(value) > MAX_KAFKA_PRINCIPAL_LENGTH
        or not value.startswith(KAFKA_PRINCIPAL_PREFIX)
        or not valid_characters
    ):
        raise ValueError(
            f"{name} must start with {KAFKA_PRINCIPAL_PREFIX} and contain only "
            "ASCII letters, digits, underscore, dot, or hyphen"
        )
    return value


def _required_single_line(env: Mapping[str, str], name: str) -> str:
    raw = str(env.get(name, ""))
    value = _required(env, name)
    if "\r" in raw or "\n" in raw:
        raise ValueError(f"{name} must be one line")
    if raw != value:
        raise ValueError(f"{name} must not have surrounding whitespace")
    return value


def _positive_integer(env: Mapping[str, str], name: str, default: int) -> int:
    raw = str(env.get(name, default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value < 1:
        raise ValueError(f"{name} must be positive")
    return value


def _required_positive_integer(env: Mapping[str, str], name: str) -> int:
    raw = _required(env, name)
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value < 1:
        raise ValueError(f"{name} must be positive")
    return value


def _api_version(raw: str) -> tuple[int, int, int]:
    parts = raw.strip().split(".")
    if len(parts) != 3:
        raise ValueError(f"{ENV_PREFIX}API_VERSION must have major.minor.patch")
    try:
        version = tuple(int(part) for part in parts)
    except ValueError as exc:
        raise ValueError(f"{ENV_PREFIX}API_VERSION must be numeric") from exc
    if any(part < 0 for part in version):
        raise ValueError(f"{ENV_PREFIX}API_VERSION parts must be non-negative")
    return version  # type: ignore[return-value]


def _canonical_sha256(value: Any) -> str:
    raw = json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _authorized_operations(
    raw_operations: Any,
    *,
    resource: str,
) -> list[str]:
    if not isinstance(raw_operations, list) or any(
        not isinstance(operation, str) for operation in raw_operations
    ):
        raise RuntimeError(f"broker_{resource}_authorized_operations_missing")
    authorized_operations = sorted(set(raw_operations))
    if not set(authorized_operations).issubset(KNOWN_AUTHORIZED_OPERATIONS):
        raise RuntimeError(f"broker_{resource}_authorized_operations_unknown")
    if set(authorized_operations) & MUTATING_AUTHORIZED_OPERATIONS:
        raise RuntimeError(f"broker_{resource}_mutation_operations_authorized")
    if not REQUIRED_AUTHORIZED_OPERATIONS.issubset(authorized_operations):
        raise RuntimeError(f"broker_{resource}_read_describe_not_authorized")
    return authorized_operations


@dataclass(frozen=True)
class BrokerProbeConfig:
    bootstrap_servers: tuple[str, ...]
    topic: str
    expected_cluster_id: str | None
    username: str
    password: str = field(repr=False)
    configured_group_id: str = FIXED_SERVICE_ID
    api_version: tuple[int, int, int] = (3, 9, 0)
    security_protocol: str = "SASL_PLAINTEXT"
    sasl_mechanism: str = "PLAIN"
    request_timeout_ms: int = 120_000
    minimum_replication_factor: int | None = None
    client_id: str = "root_cause_analysis_agent_metadata_preflight"

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
        *,
        observe_only: bool = False,
    ) -> "BrokerProbeConfig":
        source = os.environ if env is None else env
        bootstrap_servers = tuple(
            item.strip()
            for item in _required(source, f"{ENV_PREFIX}BOOTSTRAP_SERVERS").split(",")
            if item.strip()
        )
        if not bootstrap_servers:
            raise ValueError(f"{ENV_PREFIX}BOOTSTRAP_SERVERS must not be empty")
        topic = _required(source, f"{ENV_PREFIX}TOPIC")
        if any(character in topic for character in ",\r\n"):
            raise ValueError(f"{ENV_PREFIX}TOPIC must name one exact topic")
        expected_cluster_id = None
        if not observe_only:
            expected_cluster_id = _required_single_line(
                source, f"{ENV_PREFIX}EXPECTED_CLUSTER_ID"
            )
        username = _required_kafka_principal(source, f"{ENV_PREFIX}USER")
        group_id = _required(source, f"{ENV_PREFIX}GROUP")
        if group_id != FIXED_SERVICE_ID:
            raise ValueError(f"{ENV_PREFIX}GROUP must be exactly {FIXED_SERVICE_ID}")
        security_protocol = str(
            source.get(f"{ENV_PREFIX}SECURITY_PROTOCOL", "SASL_PLAINTEXT")
        ).strip()
        sasl_mechanism = str(source.get(f"{ENV_PREFIX}SASL_MECHANISM", "PLAIN")).strip()
        if security_protocol != "SASL_PLAINTEXT":
            raise ValueError("security protocol must be exactly SASL_PLAINTEXT")
        if sasl_mechanism != "PLAIN":
            raise ValueError("SASL mechanism must be exactly PLAIN")
        return cls(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            expected_cluster_id=expected_cluster_id,
            username=username,
            password=_required(source, f"{ENV_PREFIX}PASSWORD"),
            configured_group_id=group_id,
            api_version=_api_version(
                str(source.get(f"{ENV_PREFIX}API_VERSION", "3.9.0"))
            ),
            security_protocol=security_protocol,
            sasl_mechanism=sasl_mechanism,
            request_timeout_ms=_positive_integer(
                source, f"{ENV_PREFIX}REQUEST_TIMEOUT_MS", 120_000
            ),
            minimum_replication_factor=(
                None
                if observe_only
                else _required_positive_integer(
                    source, f"{ENV_PREFIX}MIN_REPLICATION_FACTOR"
                )
            ),
        )

    def admin_kwargs(self) -> dict[str, Any]:
        return {
            "bootstrap_servers": list(self.bootstrap_servers),
            "security_protocol": self.security_protocol,
            "sasl_mechanism": self.sasl_mechanism,
            "sasl_plain_username": self.username,
            "sasl_plain_password": self.password,
            "api_version": self.api_version,
            "request_timeout_ms": self.request_timeout_ms,
            "client_id": self.client_id,
        }

    def public_dict(self) -> dict[str, Any]:
        return {
            "bootstrap_servers_sha256": _canonical_sha256(list(self.bootstrap_servers)),
            "topic": self.topic,
            "expected_cluster_id": self.expected_cluster_id,
            "principal_sha256": _canonical_sha256(self.username),
            "configured_group_id": self.configured_group_id,
            "api_version": ".".join(str(part) for part in self.api_version),
            "security_protocol": self.security_protocol,
            "sasl_mechanism": self.sasl_mechanism,
            "request_timeout_ms": self.request_timeout_ms,
            "minimum_replication_factor": self.minimum_replication_factor,
            "client_id": self.client_id,
            "metadata_request": {
                "allow_auto_topic_creation": False,
                "include_topic_authorized_operations": True,
            },
            "group_request": {
                "api": "DescribeGroups",
                "group_id": self.configured_group_id,
                "include_authorized_operations": True,
            },
        }


def collect_broker_metadata(
    config: BrokerProbeConfig,
    *,
    admin_factory: Callable[..., Any] | None = None,
    now: datetime | None = None,
    env_file_observation: Mapping[str, Any] | None = None,
    observe_only: bool = False,
) -> dict[str, Any]:
    """Query exact topic metadata/ACLs without joining a group or consuming."""
    kafka_version = metadata.version("kafka-python")
    if kafka_version != "3.0.7":
        raise RuntimeError("kafka-python must be exactly 3.0.7")
    if admin_factory is None:
        kafka_module = importlib.import_module("kafka")
        admin_factory = kafka_module.KafkaAdminClient

    admin = None
    kafka_admin_logger = logging.getLogger("kafka.admin.client")
    logger_was_disabled = kafka_admin_logger.disabled
    kafka_admin_logger.disabled = True
    try:
        admin = admin_factory(**config.admin_kwargs())
        # Pinned 3.0.7 exposes cluster_id and topic ACLs in this exact-topic
        # Metadata v12 response. Avoid DescribeCluster, which could require an
        # unrelated cluster-level ACL for the least-privilege consumer principal.
        cluster = admin._manager.run(
            admin._get_cluster_metadata,
            [config.topic],
        )
        if not isinstance(cluster, Mapping):
            raise RuntimeError("broker_cluster_metadata_invalid")
        cluster_id = cluster.get("cluster_id")
        if not isinstance(cluster_id, str) or not cluster_id:
            raise RuntimeError("broker_cluster_id_missing")
        if not observe_only and cluster_id != config.expected_cluster_id:
            raise RuntimeError("broker_cluster_id_mismatch")
        topics = cluster.get("topics")
        if not isinstance(topics, list) or len(topics) != 1:
            raise RuntimeError("broker_topic_missing_or_unauthorized")
        topic = topics[0]
        if not isinstance(topic, Mapping):
            raise RuntimeError("broker_topic_identity_mismatch")
        topic_name = topic.get("name", topic.get("topic"))
        if topic_name != config.topic:
            raise RuntimeError("broker_topic_identity_mismatch")
        if topic.get("error_code") != 0:
            raise RuntimeError("broker_topic_missing_or_unauthorized")
        authorized_operations = _authorized_operations(
            topic.get("authorized_operations"),
            resource="topic",
        )
        raw_partitions = topic.get("partitions")
        if not isinstance(raw_partitions, list) or not raw_partitions:
            raise RuntimeError("broker_partitions_missing")
        partition_topology: list[dict[str, Any]] = []
        for raw_partition in raw_partitions:
            if not isinstance(raw_partition, Mapping):
                raise RuntimeError("broker_partitions_invalid")
            partition = raw_partition.get(
                "partition_index", raw_partition.get("partition")
            )
            leader_id = raw_partition.get("leader_id", raw_partition.get("leader"))
            leader_epoch = raw_partition.get("leader_epoch")
            replicas = raw_partition.get("replica_nodes", raw_partition.get("replicas"))
            isr = raw_partition.get("isr_nodes", raw_partition.get("isr"))
            offline = raw_partition.get(
                "offline_replicas", raw_partition.get("offline", [])
            )
            if (
                isinstance(partition, bool)
                or not isinstance(partition, int)
                or partition < 0
                or raw_partition.get("error_code") != 0
                or isinstance(leader_id, bool)
                or not isinstance(leader_id, int)
                or leader_id < 0
                or isinstance(leader_epoch, bool)
                or not isinstance(leader_epoch, int)
                or leader_epoch < 0
            ):
                raise RuntimeError("broker_partitions_invalid")
            normalized_sets: dict[str, list[int]] = {}
            for field, values in (
                ("replicas", replicas),
                ("isr", isr),
                ("offline_replicas", offline),
            ):
                if not isinstance(values, list) or any(
                    isinstance(value, bool) or not isinstance(value, int) or value < 0
                    for value in values
                ):
                    raise RuntimeError("broker_partition_topology_invalid")
                normalized = sorted(set(values))
                if len(normalized) != len(values):
                    raise RuntimeError("broker_partition_topology_invalid")
                normalized_sets[field] = normalized
            if (
                not normalized_sets["replicas"]
                or not normalized_sets["isr"]
                or normalized_sets["isr"] != normalized_sets["replicas"]
                or normalized_sets["offline_replicas"]
                or leader_id not in normalized_sets["isr"]
            ):
                raise RuntimeError("broker_partition_topology_unhealthy")
            partition_topology.append({
                "partition": partition,
                "leader_id": leader_id,
                "leader_epoch": leader_epoch,
                **normalized_sets,
            })
        partitions = [item["partition"] for item in partition_topology]
        if len(partitions) != len(set(partitions)):
            raise RuntimeError("broker_partitions_invalid")
        partition_topology.sort(key=lambda item: item["partition"])
        partitions.sort()
        if partitions != list(range(len(partitions))):
            raise RuntimeError("broker_partition_ids_not_contiguous")
        replication_factors = {len(item["replicas"]) for item in partition_topology}
        if len(replication_factors) != 1:
            raise RuntimeError("broker_replication_factor_inconsistent")
        replication_factor = replication_factors.pop()
        if (
            not observe_only
            and config.minimum_replication_factor is not None
            and replication_factor < config.minimum_replication_factor
        ):
            raise RuntimeError("broker_replication_factor_below_policy")

        group_descriptions = admin.describe_groups(
            [config.configured_group_id],
            include_authorized_operations=True,
        )
        if not isinstance(group_descriptions, Mapping) or set(group_descriptions) != {
            config.configured_group_id
        }:
            raise RuntimeError("broker_group_identity_mismatch")
        group_description = group_descriptions[config.configured_group_id]
        if not isinstance(group_description, Mapping):
            raise RuntimeError("broker_group_description_invalid")
        returned_group_id = group_description.get(
            "group_id", config.configured_group_id
        )
        if returned_group_id != config.configured_group_id:
            raise RuntimeError("broker_group_identity_mismatch")
        if "error" not in group_description or group_description["error"] not in (
            None,
            "",
        ):
            raise RuntimeError("broker_group_missing_or_unauthorized")
        group_authorized_operations = _authorized_operations(
            group_description.get("authorized_operations"),
            resource="group",
        )
    finally:
        try:
            if admin is not None:
                admin.close()
        finally:
            kafka_admin_logger.disabled = logger_was_disabled

    observed_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    source_sha256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    public_config = config.public_dict()
    return {
        "schema_version": (
            BROKER_OBSERVATION_SCHEMA_VERSION
            if observe_only
            else BROKER_METADATA_SCHEMA_VERSION
        ),
        "observed_at": observed_at.isoformat(),
        "production_eligible": not observe_only,
        "owner_approval_required": (
            ["cluster_id", "minimum_replication_factor"]
            if observe_only
            else []
        ),
        "topic_authorized": True,
        "topic_healthy": True,
        "group_authorized": True,
        "cluster_id": cluster_id,
        "expected_cluster_id": config.expected_cluster_id,
        "topic": config.topic,
        "group_id": config.configured_group_id,
        "partitions": partitions,
        "partition_topology": partition_topology,
        "replication_factor": replication_factor,
        "topic_authorized_operations": authorized_operations,
        "group_authorized_operations": group_authorized_operations,
        "collector": {
            "schema_version": COLLECTOR_SCHEMA_VERSION,
            "source_sha256": source_sha256,
            "dependency_versions": {"kafka-python": kafka_version},
            "connection_config_sha256": _canonical_sha256(public_config),
            "config": public_config,
            "mode": "observe_only" if observe_only else "release_gate",
            "env_file": dict(env_file_observation or {}),
            "side_effect_contract": {
                "exact_topic_metadata": True,
                "group_coordinator_lookup": True,
                "describe_groups": True,
                "additional_authorization_reads": ["DescribeGroups"],
                "subscribe": False,
                "assign": False,
                "poll": False,
                "commit": False,
                "offset_fetch": False,
                "list_offsets": False,
                "consumer_group_join": False,
                "topic_auto_create": False,
            },
        },
    }


def load_environment(
    env_file: str | Path | None = None,
) -> tuple[dict[str, str], dict[str, Any]]:
    path = Path(
        env_file
        or os.environ.get(f"{ENV_PREFIX}ENV_FILE")
        or get_hermes_home() / ".env"
    ).expanduser()
    try:
        if path.is_symlink():
            raise ValueError("Kafka preflight env file must not be a symlink")
        fd = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
    except ValueError:
        raise
    except OSError as exc:
        raise ValueError(
            "Kafka preflight env file must be one readable regular file"
        ) from exc
    try:
        file_stat = os.fstat(fd)
        if not stat.S_ISREG(file_stat.st_mode):
            raise ValueError("Kafka preflight env file must be one regular file")
        mode = stat.S_IMODE(file_stat.st_mode)
        if mode & 0o077:
            raise ValueError("Kafka preflight env file must be owner-only")
        if file_stat.st_size > MAX_ENV_FILE_BYTES:
            raise ValueError("Kafka preflight env file exceeds size limit")
        raw = bytearray()
        while len(raw) <= MAX_ENV_FILE_BYTES:
            chunk = os.read(fd, min(64 * 1024, MAX_ENV_FILE_BYTES + 1 - len(raw)))
            if not chunk:
                break
            raw.extend(chunk)
        final_stat = os.fstat(fd)
        identity = ("st_dev", "st_ino", "st_size", "st_mtime_ns")
        if (
            len(raw) > MAX_ENV_FILE_BYTES
            or len(raw) != file_stat.st_size
            or any(
                getattr(file_stat, field) != getattr(final_stat, field)
                for field in identity
            )
        ):
            raise ValueError("Kafka preflight env file changed while reading")
    finally:
        os.close(fd)
    try:
        contents = bytes(raw).decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("Kafka preflight env file must be UTF-8") from exc
    parsed = dotenv_values(stream=io.StringIO(contents), interpolate=False)
    source = {
        str(key): "" if value is None else str(value) for key, value in parsed.items()
    }
    public_kafka_env = {
        key: value
        for key, value in source.items()
        if key.startswith(ENV_PREFIX) and key != f"{ENV_PREFIX}PASSWORD"
    }
    observation = {
        "path": str(path.absolute()),
        "device": file_stat.st_dev,
        "inode": file_stat.st_ino,
        "mode": format(mode, "04o"),
        "size": file_stat.st_size,
        "mtime_ns": file_stat.st_mtime_ns,
        "public_kafka_env_sha256": _canonical_sha256(public_kafka_env),
        "password_set": bool(source.get(f"{ENV_PREFIX}PASSWORD", "")),
    }
    return source, observation


def _atomic_write_evidence(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink():
        raise ValueError("--output must not be a symlink")
    fd, temporary_name = tempfile.mkstemp(
        dir=str(path.parent),
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    temporary_path = Path(temporary_name)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            fd = -1
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
        directory_fd = os.open(
            str(path.parent), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        )
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if fd >= 0:
            os.close(fd)
        try:
            temporary_path.unlink()
        except FileNotFoundError:
            pass


def _output_path(raw: str | None) -> Path | None:
    if raw is None:
        return None
    value = raw.strip()
    path = Path(value).expanduser()
    if (
        not value
        or not path.is_absolute()
        or "\x00" in value
        or "\r" in value
        or "\n" in value
    ):
        raise ValueError("--output must be one safe absolute path")
    if path.is_symlink():
        raise ValueError("--output must not be a symlink")
    return path


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", help="dotenv path; defaults to HERMES_HOME/.env")
    parser.add_argument(
        "--output",
        help="absolute broker_metadata.json path; stdout only when omitted",
    )
    parser.add_argument(
        "--observe-only",
        action="store_true",
        help=(
            "observe cluster identity and replication without owner policy; "
            "the receipt is never production-eligible"
        ),
    )
    return parser


def main(
    argv: list[str] | None = None,
    *,
    admin_factory: Callable[..., Any] | None = None,
    env: Mapping[str, str] | None = None,
    now: datetime | None = None,
) -> int:
    args = build_arg_parser().parse_args(argv)
    config: BrokerProbeConfig | None = None
    try:
        output_path = _output_path(args.output)
        if args.observe_only and output_path is None:
            raise ValueError("--observe-only requires --output")
        env_observation: Mapping[str, Any] | None = None
        if env is None:
            env, env_observation = load_environment(args.env_file)
        config = BrokerProbeConfig.from_env(env, observe_only=args.observe_only)
        payload = collect_broker_metadata(
            config,
            admin_factory=admin_factory,
            now=now,
            env_file_observation=env_observation,
            observe_only=args.observe_only,
        )
        if output_path is not None:
            _atomic_write_evidence(output_path, payload)
        result = {
            "ok": True,
            "output": str(output_path) if output_path is not None else None,
            "canonical_payload_sha256": _canonical_sha256(payload),
            "broker_metadata": payload,
        }
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0
    except Exception as exc:
        message = str(exc)
        if config is not None and config.password:
            message = message.replace(config.password, "[REDACTED]")
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": type(exc).__name__,
                    "message": message,
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
