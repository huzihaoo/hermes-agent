#!/usr/bin/env python3
"""Collect one governed RCA canary receipt from read-only execution facts.

The collector is deliberately not a canary runner.  It cannot admit an event,
consume Kafka, submit a VM task, or write to Feishu.  The only optional write is
an atomic, local evidence projection selected with ``--write``.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import secrets
import sqlite3
import stat
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping, Protocol, Sequence
from urllib.parse import quote

from dotenv import dotenv_values

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gateway.pnc_rca_admission import (
    RcaAdmissionError,
    build_rca_admission,
    validate_rca_admission,
    validate_rca_trigger_context,
)
from gateway import pnc_rca_prod_admission as prod_admission
from gateway import pnc_rca_prod_bootstrap as prod_bootstrap
from gateway.pnc_rca_data_access import (
    RemoteDataAccessError,
    validate_remote_data_access,
)
from gateway.pnc_rca_delivery_contract import (
    DELIVERY_CONTRACT_SCHEMA_VERSION,
    DELIVERY_MANIFEST_SCHEMA_VERSION,
    DeliveryContractError,
    TERMINAL_DELIVERY_OUTCOMES,
    build_terminal_delivery,
    build_terminal_thread_reply_effect,
    build_report_url,
    compute_artifact_set_id,
    verify_persisted_artifact_inventory,
)
from gateway.pnc_group_binding import (
    G1Q3_RCA_GROUP_ID,
    GROUP_BINDING_RECEIPT_FILENAME_RE,
    pnc_group_binding_receipt_filename,
)
from gateway.pnc_rca_kafka_contract import WorkflowEventPolicy
from gateway.pnc_rca_runtime_transition import (
    HOST_RUNTIME_TRANSITION_KIND_BY_SERVICE,
    HOST_RUNTIME_TRANSITION_SERVICES,
    RUNTIME_IDENTITY_FIELDS,
    canonical_host_runtime_identity,
)
from gateway.pnc_rca_stage_lineage import (
    RCA_STAGE_NAME_BY_SHORT,
    StageLineageError,
    stage_lineage_relative_path,
    validate_stage_lineage_chain,
    validate_stage_lineage_receipt,
)
from hermes_constants import get_hermes_home


CANARY_RECEIPT_SCHEMA_VERSION = "pnc_rca_canary_receipt_v8"
SOURCE_MANIFEST_SCHEMA_VERSION = "pnc_rca_canary_source_provenance_v1"
CANARY_EVIDENCE_COMMIT_SCHEMA_VERSION = "pnc_rca_canary_evidence_commit_v1"
CANARY_EVIDENCE_PUBLISH_LOCK_SCHEMA_VERSION = (
    "pnc_rca_canary_evidence_publish_lock_v1"
)
TERMINAL_CANARY_RECEIPT_SCHEMA_VERSION = (
    "pnc_rca_terminal_delivery_canary_v2"
)
TERMINAL_CANARY_SOURCE_SCHEMA_VERSION = (
    "pnc_rca_terminal_delivery_canary_source_v1"
)
REMOTE_READER_HEALTH_SCHEMA_VERSION = "pnc_rca_remote_reader_health_v1"
BROWSER_SMOKE_SCHEMA_VERSION = "pnc_rca_html_browser_smoke_v2"
HTML_ARTIFACT_POLICY_VERSION = "passive_static_html_v1"
REMOTE_READ_RECEIPT_SCHEMA_VERSION = "g1q3_rca_remote_read_receipt_v1"
CAPACITY_LIFECYCLE_SCHEMA_VERSION = "g1q3_rca_capacity_lifecycle_artifact_v2"
CAPACITY_METER_SCHEMA_VERSION = "g1q3_rca_stage_capacity_meter_v2"
CAPACITY_METER_ACCOUNTING_MODE = "exclusive_tmp_hfs_total_v2"
SERVICE_RESULT_SCHEMA_VERSION = "g1q3_rca_service_result_v2"
WORKER_RESULT_SCHEMA_VERSION = "g1q3_rca_worker_result_v1"
WORKER_ATTESTATION_SCHEMA_VERSION = "g1q3_rca_worker_execution_attestation_v2"
WORKER_DISPATCH_RECEIPT_SCHEMA_VERSION = "g1q3_rca_worker_dispatch_receipt_v1"
STORAGE_FILE_RECEIPT_SCHEMA_VERSION = "g1q3_rca_storage_file_receipt_v1"

DEFAULT_VM_REPO_ROOT = "/home/mini/data3/yj-evaluation-server"
DEFAULT_VM_WORKER_ROOT = "/home/mini/.hermes/worker-state"
FIXED_SERVICE_ENTRYPOINT = (
    f"{DEFAULT_VM_REPO_ROOT}/api/g1q3_rca/scripts/run_rca_service_request.py"
)
FIXED_SERVICE_RELATIVE_ENTRYPOINT = "./api/g1q3_rca/scripts/run_rca_service_request.py"
FIXED_WORKER_ENTRYPOINT = f"{DEFAULT_VM_WORKER_ROOT}/vm_coding_worker_v2.py"
DEFAULT_SSH_MINI_AGENT = Path.home() / ".local" / "bin" / "ssh-mini-agent"
DEFAULT_CONTROL_DB = (
    Path.home()
    / ".hermes"
    / "runtime"
    / "pnc_agent"
    / "feishu_issue_kafka_rca"
    / "control.sqlite3"
)
DEFAULT_EVIDENCE_DIR = (
    Path.home()
    / ".hermes"
    / "runtime"
    / "pnc_agent"
    / "feishu_issue_kafka_rca"
    / "release_evidence"
)
DEFAULT_GROUP_BINDING_RECEIPT_DIR = (
    Path.home() / ".hermes" / "pnc_agent" / "receipts" / "g1q3_rca"
)
MAX_ENV_FILE_BYTES = 1024 * 1024
CANARY_EVIDENCE_DIR_ENV = "HERMES_RCA_CANARY_EVIDENCE_DIR"
CANARY_GROUP_BINDING_RECEIPT_DIR_ENV = (
    "HERMES_RCA_CANARY_GROUP_BINDING_RECEIPT_DIR"
)
KAFKA_CONTROL_DB_ENV = "HERMES_RCA_KAFKA_CONTROL_DB_PATH"
OUTBOX_CONTROL_DB_ENV = "HERMES_RCA_OUTBOX_CONTROL_DB_PATH"
OUTBOX_DELIVERY_DB_ENV = "HERMES_RCA_OUTBOX_DELIVERY_DB_PATH"
MANUAL_CHAT_IDS_ENV = "HERMES_RCA_MANUAL_CHAT_IDS"
FIXED_MANUAL_CHAT_IDS = frozenset(
    {
        "oc_6cfc782212009ff4cd815349909dd423",
        "oc_16614f4ba25b8c88b69c0b8e9ebc2fb5",
    }
)
GATEWAY_RUNTIME_IDENTITY_KEYS = {
    "service_label",
    "pid",
    "process_create_time",
    "boot_time",
    "executable",
    "script",
    "cwd",
    "script_sha256",
    "runtime_files_sha256",
    "public_config_sha256",
    "loaded_runtime_sha256",
}
CONTROL_RUNTIME_TRANSITION_SERVICES = frozenset({
    "local.pnc.rca-kafka-consumer",
    "local.pnc.rca-outbox-dispatcher",
})
DELIVERY_RUNTIME_TRANSITION_SERVICES = frozenset({
    "local.pnc.rca-delivery-collector",
    "local.pnc.rca-delivery-dispatcher",
})

MAX_JSON_BYTES = 8 * 1024 * 1024
CANARY_EVIDENCE_LOCK_WAIT_SECONDS = 10.0
CANARY_EVIDENCE_LOCK_MAX_AGE_SECONDS = 30.0
CANARY_EVIDENCE_LOCK_POLL_SECONDS = 0.01
MAX_GOAL_BYTES = 2 * 1024 * 1024
MAX_JSON_DEPTH = 64
MAX_JSON_NODES = 100_000
MAX_REMOTE_DIGEST_BYTES = 750_000_000
MAX_REMOTE_AGGREGATE_BYTES = 1_600_000_000
MAX_REMOTE_FILES = 32
REMOTE_TIMEOUT_SECONDS = 30
REMOTE_HASH_BYTES_PER_SECOND = 16 * 1024 * 1024
REMOTE_HASH_FIXED_OVERHEAD_SECONDS = 10
REMOTE_HASH_DEADLINE_MAX_SECONDS = 110
REMOTE_WRAPPER_GRACE_SECONDS = 5
REMOTE_HOST_GRACE_SECONDS = 5
GATE_VALIDATION_MAX_AGE_SECONDS = 900
SAFE_SUBMISSION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,191}$")
SOURCE_ID_RE = re.compile(r"^g1q3-rca-source-v1-[0-9a-f]{64}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
REMOTE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,511}$")
REQUIRED_READER_CLASSES = {"RemoteClipReader", "RemoteEventReader"}
DOWNSTREAM_STAGE_NAMES = RCA_STAGE_NAME_BY_SHORT
FIXED_CIFS_STORAGE_BASE = {
    "storage_mode": "cifs_mount_fixed",
    "observed_file_mode": "0755",
    "requested_file_mode": "0600",
    "mode_enforced_by_mount": True,
    "credentials_present": False,
    "secret_scan_passed": True,
}
FIXED_CIFS_MOUNT_SOURCE = (
    "//hfs.minieye.tech/department-pnc_team-planning_algo-driving-tmp"
)
FIXED_CIFS_STORAGE = {
    **FIXED_CIFS_STORAGE_BASE,
    "mount_evidence": {
        "mount_point": "/mnt/tmp",
        "mount_source": FIXED_CIFS_MOUNT_SOURCE,
        "fstype": "cifs",
        "file_mode": "0755",
        "dir_mode": "0755",
        "rw": True,
        "device_id": 1,
        "mount_namespace": "mnt:[1]",
    },
}
BROWSER_ZERO_FIELDS = {
    "unmanifested_request_count",
    "executable_script_count",
    "inline_event_handler_count",
    "external_active_document_count",
    "console_error_count",
    "runtime_exception_count",
    "log_error_count",
    "network_error_count",
}
BROWSER_SMOKE_KEYS = {
    "schema_version",
    "observed_at",
    "ok",
    "machine_generated",
    "source",
    "engine",
    "artifact_policy",
    "artifact_set_id",
    "report_url",
    "index_html_sha256",
    "manifest_sha256",
    "delivery_contract_sha256",
    "manifest_url_count",
    "manifest_url_set_sha256",
    "requested_urls",
    "request_list_sha256",
    "network_closure",
    "desktop_nonblank",
    "mobile_nonblank",
    "request_count",
    "browser",
    "viewports",
    "blockers",
    "evidence_sha256",
    *BROWSER_ZERO_FIELDS,
}
WORKER_ATTESTATION_KEYS = {
    "schema_version",
    "available",
    "executor_type",
    "agent_backend",
    "codex_backend_enabled",
    "coding_agent_fallback_enabled",
    "openclaw_invocation_count",
    "codex_invocation_count",
    "fallback_invocation_count",
    "worker_source_commit",
    "worker_tree_clean",
    "worker_entrypoint_path",
    "worker_entrypoint_sha256",
    "argv",
    "cwd",
    "dispatched_at",
    "process_started_at",
    "task_id",
    "run_id",
    "worker_pid",
    "dispatch_receipt_sha256",
}
WORKER_RESULT_REQUIRED_KEYS = {
    "schema_version",
    "task_id",
    "run_id",
    "repo_root",
    "canonical_task_dir",
    "goal_path",
    "host_inbox_root",
    "command",
    "runner_log",
    "artifact_root",
    "artifacts",
    "result_mode",
    "report_contract",
    "allowed_model_chain",
    "execution_route",
    "execution_attestation",
    "rca_submission_key",
    "rca_business_key",
    "rca_generation",
    "rca_contract_sha256",
    "rca_source_refs",
}
WORKER_RESULT_OPTIONAL_KEYS = {"external_artifacts", "resolved_snapshot"}
SERVICE_RESULT_KEYS = {
    "schema_version",
    "task_id",
    "status",
    "success",
    "goal_sha256",
    "request_sha256",
    "request_path",
    "output_dir",
    "artifact_cifs_root",
    "pipeline_result_path",
    "pipeline_status",
    "pipeline_stage",
    "blocker",
    "generated_at",
    "service_provenance",
    "request_storage",
    "worker_run_id",
    "worker_pid",
    "dispatch_receipt_sha256",
    *FIXED_CIFS_STORAGE,
}


class CanaryCollectionError(RuntimeError):
    """Fail-closed collector error with a non-sensitive public code."""

    def __init__(self, code: str):
        self.code = str(code or "canary_collection_failed")[:120]
        super().__init__(self.code)


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_json(value: Any) -> str:
    return _sha256_bytes(_canonical_json(value).encode("utf-8"))


def _sha256_execution_request(value: Mapping[str, Any]) -> str:
    """Hash the cross-repo execution ABI exactly as the VM service does."""
    try:
        encoded = json.dumps(
            dict(value),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise CanaryCollectionError("execution_request_json_invalid") from exc
    return _sha256_bytes(encoded)


def _canonical_rca_contract_sha256(
    admission: Mapping[str, Any], execution_request: Mapping[str, Any]
) -> str:
    work_item = execution_request.get("work_item")
    work_item = work_item if isinstance(work_item, dict) else {}
    data = execution_request.get("data")
    data = data if isinstance(data, dict) else {}
    source_refs = execution_request.get("source_refs")
    source_refs = source_refs if isinstance(source_refs, dict) else {}
    toolchain = execution_request.get("toolchain")
    toolchain = toolchain if isinstance(toolchain, dict) else {}
    stable_source_refs = {
        key: source_refs.get(key)
        for key in (
            "task_id",
            "source_kind",
            "origin_source_id",
            "rule_version",
            "generation",
            "business_key",
            "submission_key",
        )
    }
    if source_refs.get("source_kind") == "kafka_workflow_event":
        stable_source_refs.update(
            {
                key: source_refs.get(key)
                for key in ("source_event_id", "topic", "partition", "offset")
            }
        )
    stable_request = {
        "schema_version": execution_request.get("schema_version"),
        "request_kind": execution_request.get("request_kind"),
        "work_item": {
            key: work_item.get(key)
            for key in ("project_key", "work_item_type", "work_item_id")
        },
        "data_paths": {
            key: data.get(key) for key in ("artifact_root", "artifact_cifs_root")
        },
        "data_access": data.get("data_access"),
        "execution_policy": execution_request.get("execution_policy"),
        "source_refs": stable_source_refs,
        "intake_dispatcher": toolchain.get("intake_dispatcher"),
    }
    return _sha256_json({
        "admission": dict(admission),
        "execution_request": stable_request,
    })


def _utc_iso(value: datetime | None = None) -> str:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None or current.utcoffset() is None:
        raise CanaryCollectionError("collector_clock_not_timezone_aware")
    return current.astimezone(timezone.utc).isoformat()


def _validate_json_shape(value: Any) -> None:
    nodes = 0
    stack = [(value, 1)]
    while stack:
        item, depth = stack.pop()
        nodes += 1
        if nodes > MAX_JSON_NODES or depth > MAX_JSON_DEPTH:
            raise CanaryCollectionError("source_json_shape_exceeded")
        if isinstance(item, dict):
            stack.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            stack.extend((child, depth + 1) for child in item)


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def _decode_json(raw: bytes, *, code: str) -> dict[str, Any]:
    try:
        body = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_unique_json_object,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                ValueError("non-finite JSON value")
            ),
        )
    except (UnicodeError, ValueError, json.JSONDecodeError) as exc:
        raise CanaryCollectionError(code) from exc
    if not isinstance(body, dict):
        raise CanaryCollectionError(code)
    _validate_json_shape(body)
    return body


def _db_json(value: Any, *, code: str) -> dict[str, Any]:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("utf-8")) > MAX_JSON_BYTES
    ):
        raise CanaryCollectionError(code)
    return _decode_json(value.encode("utf-8"), code=code)


def _db_json_list(value: Any, *, code: str) -> list[dict[str, Any]]:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("utf-8")) > MAX_JSON_BYTES
    ):
        raise CanaryCollectionError(code)
    try:
        body = json.loads(
            value,
            object_pairs_hook=_unique_json_object,
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
        )
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise CanaryCollectionError(code) from exc
    _validate_json_shape(body)
    if not isinstance(body, list) or not all(isinstance(item, dict) for item in body):
        raise CanaryCollectionError(code)
    return [dict(item) for item in body]


def _parse_event_uid(event_uid: str) -> tuple[str, int, int]:
    value = str(event_uid or "").strip()
    if not value or len(value) > 500 or "\n" in value or "\r" in value:
        raise CanaryCollectionError("event_uid_invalid")
    parts = value.rsplit(":", 2)
    if (
        len(parts) != 3
        or not parts[0]
        or not parts[1].isdigit()
        or not parts[2].isdigit()
        or str(int(parts[1])) != parts[1]
        or str(int(parts[2])) != parts[2]
    ):
        raise CanaryCollectionError("event_uid_invalid")
    return parts[0], int(parts[1]), int(parts[2])


def _safe_submission(value: Any) -> str:
    submission = str(value or "").strip()
    if not SAFE_SUBMISSION_RE.fullmatch(submission) or submission in {".", ".."}:
        raise CanaryCollectionError("submission_key_invalid")
    return submission


def _safe_source_id(value: Any) -> str:
    source_id = str(value or "").strip()
    if not SOURCE_ID_RE.fullmatch(source_id):
        raise CanaryCollectionError("source_id_invalid")
    return source_id


def _stable_trigger_source_id(storage_source_kind: str, dedupe_key: str) -> str:
    return "g1q3-rca-source-v1-" + _sha256_json(
        {"source_kind": storage_source_kind, "dedupe": dedupe_key}
    )


def _manual_chat_ids(value: str | Sequence[str]) -> tuple[str, ...]:
    raw_items = value.split(",") if isinstance(value, str) else value
    normalized = tuple(sorted({str(item or "").strip() for item in raw_items if str(item or "").strip()}))
    if normalized and not set(normalized).issubset(FIXED_MANUAL_CHAT_IDS):
        raise CanaryCollectionError("manual_chat_allowlist_invalid")
    return normalized


def _require_sha256(value: Any, code: str) -> str:
    digest = str(value or "").strip().lower()
    if not SHA256_RE.fullmatch(digest):
        raise CanaryCollectionError(code)
    return digest


def _path_under_artifact_root(value: Any, artifact_root: str, *, code: str) -> str:
    path = str(value or "").strip()
    if not path.startswith("/") or "\\" in path or "\x00" in path:
        raise CanaryCollectionError(code)
    pure = PurePosixPath(path)
    if ".." in pure.parts:
        raise CanaryCollectionError(code)
    normalized_root = artifact_root.rstrip("/") + "/"
    if not path.startswith(normalized_root) or path == normalized_root.rstrip("/"):
        raise CanaryCollectionError(code)
    return path


def _validate_cifs_storage(value: Mapping[str, Any], *, code: str) -> dict[str, Any]:
    if any(
        value.get(key) != expected for key, expected in FIXED_CIFS_STORAGE_BASE.items()
    ):
        raise CanaryCollectionError(code)
    mount = value.get("mount_evidence")
    expected_keys = {
        "mount_point",
        "mount_source",
        "fstype",
        "file_mode",
        "dir_mode",
        "rw",
        "device_id",
        "mount_namespace",
    }
    if not isinstance(mount, dict) or set(mount) != expected_keys:
        raise CanaryCollectionError(code)
    device_id = mount.get("device_id")
    if (
        mount.get("mount_point") != "/mnt/tmp"
        or mount.get("mount_source") != FIXED_CIFS_MOUNT_SOURCE
        or mount.get("fstype") != "cifs"
        or mount.get("file_mode") != "0755"
        or mount.get("dir_mode") != "0755"
        or mount.get("rw") is not True
        or isinstance(device_id, bool)
        or not isinstance(device_id, int)
        or device_id < 1
        or re.fullmatch(r"mnt:\[[0-9]+\]", str(mount.get("mount_namespace") or ""))
        is None
    ):
        raise CanaryCollectionError(code)
    return dict(mount)


@dataclass(frozen=True)
class SourceRecord:
    path: str
    size_bytes: int
    raw_sha256: str
    canonical_sha256: str | None = None
    body: dict[str, Any] | None = None

    @classmethod
    def json_record(
        cls,
        path: str,
        body: Mapping[str, Any],
        *,
        raw: bytes | None = None,
    ) -> "SourceRecord":
        value = dict(body)
        encoded = raw if raw is not None else _canonical_json(value).encode("utf-8")
        return cls(
            path=path,
            size_bytes=len(encoded),
            raw_sha256=_sha256_bytes(encoded),
            canonical_sha256=_sha256_json(value),
            body=value,
        )


class RemoteSourceReader(Protocol):
    def read_sources(
        self,
        *,
        json_paths: Mapping[str, str],
        digest_paths: Mapping[str, tuple[str, int]] | None = None,
    ) -> dict[str, SourceRecord]: ...


class SshMiniAgentReader:
    """Bounded, read-only VM reader implemented through ssh-mini-agent."""

    def __init__(
        self,
        executable: str | Path = DEFAULT_SSH_MINI_AGENT,
        *,
        timeout_seconds: int = REMOTE_TIMEOUT_SECONDS,
    ) -> None:
        self.executable = str(Path(executable).expanduser())
        self.timeout_seconds = max(
            1, min(int(timeout_seconds), REMOTE_HASH_DEADLINE_MAX_SECONDS)
        )

    def read_sources(
        self,
        *,
        json_paths: Mapping[str, str],
        digest_paths: Mapping[str, tuple[str, int]] | None = None,
    ) -> dict[str, SourceRecord]:
        digests = dict(digest_paths or {})
        if not json_paths and not digests:
            return {}
        if len(json_paths) + len(digests) > MAX_REMOTE_FILES:
            raise CanaryCollectionError("remote_source_count_exceeded")
        if set(json_paths) & set(digests):
            raise CanaryCollectionError("remote_source_name_collision")
        requested_bytes = len(json_paths) * MAX_JSON_BYTES
        digest_request: dict[str, dict[str, Any]] = {}
        for name, value in digests.items():
            if (
                not isinstance(value, tuple)
                or len(value) != 2
                or isinstance(value[1], bool)
                or not isinstance(value[1], int)
                or value[1] <= 0
                or value[1] > MAX_REMOTE_DIGEST_BYTES
            ):
                raise CanaryCollectionError("remote_source_digest_limit_invalid")
            path, limit = value
            requested_bytes += limit
            digest_request[name] = {"path": path, "max_bytes": limit}
        if requested_bytes > MAX_REMOTE_AGGREGATE_BYTES:
            raise CanaryCollectionError("remote_source_byte_budget_exceeded")
        derived_deadline = REMOTE_HASH_FIXED_OVERHEAD_SECONDS + (
            requested_bytes + REMOTE_HASH_BYTES_PER_SECOND - 1
        ) // REMOTE_HASH_BYTES_PER_SECOND
        deadline_seconds = min(
            REMOTE_HASH_DEADLINE_MAX_SECONDS,
            max(self.timeout_seconds, derived_deadline),
        )
        wrapper_timeout_seconds = deadline_seconds + REMOTE_WRAPPER_GRACE_SECONDS
        host_timeout_seconds = wrapper_timeout_seconds + REMOTE_HOST_GRACE_SECONDS
        request = {
            "json": dict(json_paths),
            "digest": digest_request,
            "aggregate_byte_budget": MAX_REMOTE_AGGREGATE_BYTES,
            "deadline_seconds": deadline_seconds,
        }
        script = self._remote_script(request)
        try:
            process = subprocess.run(
                [self.executable, "run_py_json"],
                input=script,
                text=True,
                capture_output=True,
                check=False,
                timeout=host_timeout_seconds,
                env={
                    **os.environ,
                    "SSH_MINI_AGENT_TIMEOUT": str(wrapper_timeout_seconds),
                },
            )
        except subprocess.TimeoutExpired as exc:
            raise CanaryCollectionError("remote_source_reader_timeout") from exc
        except (OSError, subprocess.SubprocessError) as exc:
            raise CanaryCollectionError("remote_source_reader_unavailable") from exc
        if process.returncode == 124:
            raise CanaryCollectionError("remote_source_reader_timeout")
        if process.returncode != 0:
            raise CanaryCollectionError("remote_source_reader_failed")
        try:
            payload = json.loads(
                process.stdout,
                object_pairs_hook=_unique_json_object,
                parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
            )
        except ValueError as exc:
            raise CanaryCollectionError(
                "remote_source_reader_contract_invalid"
            ) from exc
        expected_names = set(json_paths) | set(digests)
        if not isinstance(payload, dict):
            raise CanaryCollectionError("remote_source_reader_contract_invalid")
        files = payload.get("files")
        if (
            payload.get("ok") is not True
            or not isinstance(files, dict)
            or set(files) != expected_names
        ):
            raise CanaryCollectionError("remote_source_reader_contract_invalid")
        records: dict[str, SourceRecord] = {}
        for name in sorted(expected_names):
            item = files.get(name)
            if not isinstance(item, dict):
                raise CanaryCollectionError("remote_source_reader_contract_invalid")
            expected_path = json_paths.get(name) or digests[name][0]
            if item.get("path") != expected_path:
                raise CanaryCollectionError("remote_source_path_mismatch")
            size = item.get("size_bytes")
            if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
                raise CanaryCollectionError("remote_source_size_invalid")
            raw_sha = _require_sha256(
                item.get("raw_sha256"), "remote_source_hash_invalid"
            )
            if name in json_paths:
                body = item.get("body")
                if not isinstance(body, dict):
                    raise CanaryCollectionError("remote_source_json_invalid")
                _validate_json_shape(body)
                canonical = _sha256_json(body)
                if item.get("canonical_sha256") != canonical:
                    raise CanaryCollectionError("remote_source_canonical_hash_mismatch")
                records[name] = SourceRecord(
                    path=expected_path,
                    size_bytes=size,
                    raw_sha256=raw_sha,
                    canonical_sha256=canonical,
                    body=dict(body),
                )
            else:
                records[name] = SourceRecord(
                    path=expected_path,
                    size_bytes=size,
                    raw_sha256=raw_sha,
                )
        return records

    @staticmethod
    def _remote_script(request: Mapping[str, Any]) -> str:
        # The remote program only performs lstat/open/hash/JSON parsing.  It has
        # no imports from the business runtime and no write-capable operations.
        encoded = json.dumps(dict(request), ensure_ascii=True, sort_keys=True)
        return f"""import hashlib, json, os, pathlib, stat, time
REQUEST = json.loads({encoded!r})
MAX_JSON_BYTES = {MAX_JSON_BYTES}
MAX_DEPTH = {MAX_JSON_DEPTH}
MAX_NODES = {MAX_JSON_NODES}
MAX_FILES = {MAX_REMOTE_FILES}
MAX_TOTAL_BYTES = {MAX_REMOTE_AGGREGATE_BYTES}
MAX_DEADLINE_SECONDS = {REMOTE_HASH_DEADLINE_MAX_SECONDS}
ALLOWED_ROOTS = ("/mnt/tmp/", "/home/mini/.hermes/worker-state/tasks/", "/home/mini/.hermes/shared-state/tasks/")

def fail(code):
    print(json.dumps({{"ok": False, "error": code}}, sort_keys=True))
    raise SystemExit(2)

deadline_seconds = REQUEST.get("deadline_seconds")
if (isinstance(deadline_seconds, bool) or not isinstance(deadline_seconds, int)
        or deadline_seconds <= 0 or deadline_seconds > MAX_DEADLINE_SECONDS):
    fail("deadline_invalid")
if REQUEST.get("aggregate_byte_budget") != MAX_TOTAL_BYTES:
    fail("aggregate_budget_invalid")
deadline = time.monotonic() + deadline_seconds

def check_deadline():
    if time.monotonic() >= deadline:
        raise SystemExit(124)

def checked(path_text, max_bytes):
    check_deadline()
    if not isinstance(path_text, str) or not path_text.startswith("/") or "\\x00" in path_text or "\\\\" in path_text:
        fail("path_invalid")
    pure = pathlib.PurePosixPath(path_text)
    if ".." in pure.parts or not any(path_text.startswith(root) for root in ALLOWED_ROOTS):
        fail("path_outside_allowed_roots")
    path = pathlib.Path(path_text)
    try:
        info = os.lstat(path)
    except OSError:
        fail("source_unreadable")
    check_deadline()
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        fail("source_not_regular")
    if info.st_size <= 0 or info.st_size > max_bytes:
        fail("source_size_invalid")
    cursor = path.parent
    while str(cursor) not in ("/mnt/tmp", "/home/mini/.hermes/worker-state/tasks", "/home/mini/.hermes/shared-state/tasks", "/"):
        try:
            parent_info = os.lstat(cursor)
        except OSError:
            fail("source_parent_unreadable")
        if stat.S_ISLNK(parent_info.st_mode) or not stat.S_ISDIR(parent_info.st_mode):
            fail("source_parent_symlink")
        cursor = cursor.parent
        check_deadline()
    try:
        if path.resolve(strict=True) != path:
            fail("source_resolve_mismatch")
    except OSError:
        fail("source_resolve_failed")
    check_deadline()
    return path, info.st_size

def unique(pairs):
    result = {{}}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result

def shape(value):
    nodes = 0
    stack = [(value, 1)]
    while stack:
        current, depth = stack.pop()
        nodes += 1
        if nodes > MAX_NODES or depth > MAX_DEPTH:
            raise ValueError("shape exceeded")
        if isinstance(current, dict):
            stack.extend((item, depth + 1) for item in current.values())
        elif isinstance(current, list):
            stack.extend((item, depth + 1) for item in current)

def read_checked(path, expected_size):
    check_deadline()
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or opened.st_size != expected_size:
            fail("source_changed_during_read")
        chunks = []
        remaining = expected_size
        while remaining:
            check_deadline()
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                fail("source_short_read")
            chunks.append(chunk)
            remaining -= len(chunk)
        check_deadline()
        if os.read(descriptor, 1):
            fail("source_changed_during_read")
        return b"".join(chunks)
    finally:
        os.close(descriptor)

def digest(path, expected_size):
    result = hashlib.sha256()
    check_deadline()
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or opened.st_size != expected_size:
            fail("source_changed_during_read")
        remaining = expected_size
        while remaining:
            check_deadline()
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                fail("source_short_read")
            result.update(chunk)
            remaining -= len(chunk)
        check_deadline()
        if os.read(descriptor, 1):
            fail("source_changed_during_read")
    finally:
        os.close(descriptor)
    return result.hexdigest()

json_specs = REQUEST.get("json")
digest_specs = REQUEST.get("digest")
if not isinstance(json_specs, dict) or not isinstance(digest_specs, dict):
    fail("request_invalid")
if set(json_specs) & set(digest_specs) or len(json_specs) + len(digest_specs) > MAX_FILES:
    fail("request_invalid")

plan = []
total_bytes = 0
for name, path_text in json_specs.items():
    path, size = checked(path_text, MAX_JSON_BYTES)
    total_bytes += size
    plan.append(("json", name, path_text, path, size))
for name, spec in digest_specs.items():
    if not isinstance(spec, dict):
        fail("digest_limit_invalid")
    limit = spec.get("max_bytes")
    if (isinstance(limit, bool) or not isinstance(limit, int)
            or limit <= 0 or limit > {MAX_REMOTE_DIGEST_BYTES}):
        fail("digest_limit_invalid")
    path_text = spec.get("path")
    path, size = checked(path_text, limit)
    total_bytes += size
    plan.append(("digest", name, path_text, path, size))
if total_bytes > MAX_TOTAL_BYTES:
    fail("aggregate_size_exceeded")

files = {{}}
for kind, name, path_text, path, size in plan:
    check_deadline()
    if kind == "digest":
        files[name] = {{"path": path_text, "size_bytes": size, "raw_sha256": digest(path, size)}}
        continue
    raw = read_checked(path, size)
    try:
        body = json.loads(raw.decode("utf-8"), object_pairs_hook=unique, parse_constant=lambda _value: (_ for _ in ()).throw(ValueError("non-finite")))
        if not isinstance(body, dict):
            raise ValueError("not object")
        shape(body)
        canonical = json.dumps(body, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    except Exception:
        fail("source_json_invalid")
    check_deadline()
    files[name] = {{"path": path_text, "size_bytes": size, "raw_sha256": hashlib.sha256(raw).hexdigest(), "canonical_sha256": hashlib.sha256(canonical).hexdigest(), "body": body}}
print(json.dumps({{"ok": True, "read_only": True, "files": files}}, ensure_ascii=False, sort_keys=True))
"""


def load_canary_collector_environment(env_file: str | Path) -> dict[str, str]:
    """Read one owner-only dotenv literally from a stable file descriptor."""
    path = Path(env_file).expanduser().absolute()
    descriptor = -1
    try:
        initial = os.lstat(path)
        if (
            stat.S_ISLNK(initial.st_mode)
            or not stat.S_ISREG(initial.st_mode)
            or initial.st_uid != os.getuid()
            or initial.st_nlink != 1
            or stat.S_IMODE(initial.st_mode) & 0o077
            or initial.st_size > MAX_ENV_FILE_BYTES
            or path.resolve(strict=True) != path
        ):
            raise ValueError(
                "canary collector env file must be one owner-only regular file "
                "without symlinks or hardlinks"
            )
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        opened = os.fstat(descriptor)
        identity_fields = (
            "st_dev",
            "st_ino",
            "st_size",
            "st_mtime_ns",
            "st_mode",
            "st_uid",
            "st_nlink",
        )
        if any(
            getattr(initial, field) != getattr(opened, field)
            for field in identity_fields
        ):
            raise ValueError("canary collector env file changed while opening")
        raw = bytearray()
        while len(raw) <= MAX_ENV_FILE_BYTES:
            chunk = os.read(
                descriptor,
                min(64 * 1024, MAX_ENV_FILE_BYTES + 1 - len(raw)),
            )
            if not chunk:
                break
            raw.extend(chunk)
        final = os.fstat(descriptor)
        if (
            len(raw) > MAX_ENV_FILE_BYTES
            or len(raw) != opened.st_size
            or any(
                getattr(opened, field) != getattr(final, field)
                for field in identity_fields
            )
        ):
            raise ValueError("canary collector env file changed while reading")
    except ValueError:
        raise
    except OSError as exc:
        raise ValueError(
            "canary collector env file must be one readable owner-only file"
        ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    try:
        contents = bytes(raw).decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("canary collector env file must be UTF-8") from exc
    parsed = dotenv_values(stream=io.StringIO(contents), interpolate=False)
    return {
        str(key): "" if value is None else str(value)
        for key, value in parsed.items()
    }


def _safe_config_path(value: Any, *, name: str) -> Path:
    raw = str(value or "").strip()
    if not raw or any(character in raw for character in ("\x00", "\r", "\n")):
        raise ValueError(f"{name} must be one non-empty path")
    return Path(raw).expanduser()


def _collector_env_defaults(source: Mapping[str, str]) -> dict[str, Any]:
    home_raw = str(source.get("HERMES_HOME", "") or "").strip()
    home = (
        _safe_config_path(home_raw, name="HERMES_HOME")
        if home_raw
        else Path(get_hermes_home()).expanduser()
    )
    if not home.is_absolute():
        raise ValueError("HERMES_HOME must be an absolute path")
    runtime_root = home / "runtime" / "pnc_agent" / "feishu_issue_kafka_rca"
    default_control = runtime_root / "control.sqlite3"
    kafka_control_raw = str(source.get(KAFKA_CONTROL_DB_ENV, "") or "").strip()
    outbox_control_raw = str(source.get(OUTBOX_CONTROL_DB_ENV, "") or "").strip()
    kafka_control = (
        _safe_config_path(kafka_control_raw, name=KAFKA_CONTROL_DB_ENV)
        if kafka_control_raw
        else None
    )
    outbox_control = (
        _safe_config_path(outbox_control_raw, name=OUTBOX_CONTROL_DB_ENV)
        if outbox_control_raw
        else None
    )
    if (
        kafka_control is not None
        and outbox_control is not None
        and kafka_control.absolute().resolve(strict=False)
        != outbox_control.absolute().resolve(strict=False)
    ):
        raise ValueError(
            f"{KAFKA_CONTROL_DB_ENV} and {OUTBOX_CONTROL_DB_ENV} must match"
        )
    control = kafka_control or outbox_control or default_control
    delivery_raw = str(source.get(OUTBOX_DELIVERY_DB_ENV, "") or "").strip()
    delivery = (
        _safe_config_path(delivery_raw, name=OUTBOX_DELIVERY_DB_ENV)
        if delivery_raw
        else control
    )
    evidence_raw = str(source.get(CANARY_EVIDENCE_DIR_ENV, "") or "").strip()
    receipt_raw = str(
        source.get(CANARY_GROUP_BINDING_RECEIPT_DIR_ENV, "") or ""
    ).strip()
    return {
        "control_db": control,
        "delivery_db": delivery,
        "delivery_db_explicit": bool(delivery_raw),
        "evidence_dir": (
            _safe_config_path(evidence_raw, name=CANARY_EVIDENCE_DIR_ENV)
            if evidence_raw
            else runtime_root / "release_evidence"
        ),
        "group_binding_receipt_dir": (
            _safe_config_path(
                receipt_raw,
                name=CANARY_GROUP_BINDING_RECEIPT_DIR_ENV,
            )
            if receipt_raw
            else home / "pnc_agent" / "receipts" / "g1q3_rca"
        ),
        "manual_chat_ids": str(source.get(MANUAL_CHAT_IDS_ENV, "") or ""),
        "prod_admission_hmac_key": str(
            source.get(prod_admission.HMAC_ENV, "") or ""
        ),
    }


@dataclass(frozen=True)
class CollectorConfig:
    control_db_path: Path
    delivery_db_path: Path
    evidence_dir: Path
    group_binding_receipt_dir: Path = DEFAULT_GROUP_BINDING_RECEIPT_DIR
    manual_chat_ids: tuple[str, ...] = ()
    ssh_mini_agent: Path = DEFAULT_SSH_MINI_AGENT
    remote_timeout_seconds: int = REMOTE_TIMEOUT_SECONDS
    prod_admission_hmac_key: str | bytes = field(default="", repr=False)

    @classmethod
    def defaults(cls) -> "CollectorConfig":
        return cls(
            control_db_path=DEFAULT_CONTROL_DB,
            delivery_db_path=DEFAULT_CONTROL_DB,
            evidence_dir=DEFAULT_EVIDENCE_DIR,
            manual_chat_ids=_manual_chat_ids(
                os.environ.get("HERMES_RCA_MANUAL_CHAT_IDS", "")
            ),
            prod_admission_hmac_key=os.environ.get(prod_admission.HMAC_ENV, ""),
        )


@dataclass(frozen=True)
class ControlFacts:
    admission: dict[str, Any]
    workflow_policy: dict[str, Any]
    submission_key: str
    business_key: str
    generation: int
    outbox_id: int
    outbox_result: dict[str, Any]
    outbox_payload: dict[str, Any]
    execution_origin: dict[str, Any]
    observed_trigger_source: dict[str, Any]
    delivery_subscriptions: list[dict[str, Any]]
    host_runtime_transitions: list[dict[str, Any]]
    authorization_sources: dict[str, SourceRecord]
    snapshot_sha256: str


@dataclass(frozen=True)
class DeliveryFacts:
    manifest: dict[str, Any]
    contract: dict[str, Any]
    artifacts: list[dict[str, Any]]
    report: dict[str, Any]
    delivery: dict[str, Any]
    delivery_obligations: list[dict[str, Any]]
    host_runtime_transitions: list[dict[str, Any]]
    snapshot_sha256: str


@dataclass(frozen=True)
class TerminalDeliveryFacts:
    watch: dict[str, Any]
    job: dict[str, Any]
    delivery_obligations: list[dict[str, Any]]
    host_runtime_transitions: list[dict[str, Any]]
    snapshot_sha256: str


@dataclass(frozen=True)
class CollectionResult:
    receipt: dict[str, Any]
    provenance: dict[str, Any]
    evidence_role: str = "primary"


class ReadOnlyDatabase:
    def __init__(self, path: Path):
        self.path = path.expanduser()
        self.connection: sqlite3.Connection | None = None
        self.snapshot_file_info: os.stat_result | None = None

    def __enter__(self) -> "ReadOnlyDatabase":
        initial_info = _secure_regular_file(
            self.path, code="control_database_invalid"
        )
        uri = f"file:{quote(str(self.path.resolve(strict=True)), safe='/')}?mode=ro"
        connection: sqlite3.Connection | None = None
        try:
            connection = sqlite3.connect(uri, uri=True, isolation_level=None, timeout=5)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only=ON")
            connection.execute("PRAGMA trusted_schema=OFF")
            connection.execute("PRAGMA busy_timeout=5000")
            connection.execute("BEGIN")
            opened_info = _secure_regular_file(
                self.path, code="control_database_invalid"
            )
            if not _same_database_identity(initial_info, opened_info):
                raise CanaryCollectionError("control_database_changed_during_read")
        except CanaryCollectionError:
            if connection is not None:
                connection.close()
            raise
        except sqlite3.Error as exc:
            if connection is not None:
                connection.close()
            raise CanaryCollectionError("control_database_unavailable") from exc
        self.connection = connection
        self.snapshot_file_info = opened_info
        return self

    def __exit__(self, *_args: Any) -> None:
        identity_error: CanaryCollectionError | None = None
        if self.connection is not None:
            try:
                final_info = _secure_regular_file(
                    self.path, code="control_database_invalid"
                )
                if self.snapshot_file_info is None or not _same_database_identity(
                    self.snapshot_file_info, final_info
                ):
                    identity_error = CanaryCollectionError(
                        "control_database_changed_during_read"
                    )
                else:
                    self.snapshot_file_info = final_info
            finally:
                try:
                    self.connection.rollback()
                finally:
                    self.connection.close()
        self.connection = None
        if identity_error is not None:
            raise identity_error

    def rows(self, sql: str, parameters: Sequence[Any] = ()) -> list[dict[str, Any]]:
        if self.connection is None:
            raise CanaryCollectionError("control_database_unavailable")
        try:
            return [
                dict(row) for row in self.connection.execute(sql, parameters).fetchall()
            ]
        except sqlite3.Error as exc:
            raise CanaryCollectionError("control_database_schema_invalid") from exc


def _secure_regular_file(
    path: Path,
    *,
    code: str,
    max_bytes: int | None = None,
    private: bool = False,
) -> os.stat_result:
    candidate = path.expanduser()
    try:
        info = os.lstat(candidate)
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            raise CanaryCollectionError(code)
        if info.st_size <= 0 or (max_bytes is not None and info.st_size > max_bytes):
            raise CanaryCollectionError(code)
        if candidate.resolve(strict=True) != candidate.absolute():
            raise CanaryCollectionError(code)
        if private and stat.S_IMODE(info.st_mode) & 0o077:
            raise CanaryCollectionError(code)
    except CanaryCollectionError:
        raise
    except OSError as exc:
        raise CanaryCollectionError(code) from exc
    return info


def _same_database_identity(left: os.stat_result, right: os.stat_result) -> bool:
    return left.st_dev == right.st_dev and left.st_ino == right.st_ino


def _read_local_json(
    path: Path, *, code: str, private: bool = True
) -> tuple[dict[str, Any], SourceRecord]:
    info = _secure_regular_file(
        path,
        code=code,
        max_bytes=MAX_JSON_BYTES,
        private=private,
    )
    descriptor = -1
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
        )
        opened = os.fstat(descriptor)
        if (
            opened.st_dev != info.st_dev
            or opened.st_ino != info.st_ino
            or opened.st_size != info.st_size
            or not stat.S_ISREG(opened.st_mode)
        ):
            raise CanaryCollectionError(code)
        chunks: list[bytes] = []
        remaining = MAX_JSON_BYTES + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        if len(raw) != info.st_size or len(raw) > MAX_JSON_BYTES:
            raise CanaryCollectionError(code)
    except CanaryCollectionError:
        raise
    except OSError as exc:
        raise CanaryCollectionError(code) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    body = _decode_json(raw, code=code)
    return body, SourceRecord(
        path=str(path),
        size_bytes=info.st_size,
        raw_sha256=_sha256_bytes(raw),
        canonical_sha256=_sha256_json(body),
        body=body,
    )


def _one(rows: Sequence[dict[str, Any]], code: str) -> dict[str, Any]:
    if len(rows) != 1:
        raise CanaryCollectionError(code)
    return dict(rows[0])


def _parse_timestamp(value: Any, *, code: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError as exc:
        raise CanaryCollectionError(code) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CanaryCollectionError(code)
    return parsed.astimezone(timezone.utc)


def _read_host_runtime_transitions(
    db: ReadOnlyDatabase,
    *,
    submission_key: str,
    business_key: str,
    generation: int,
    services: frozenset[str],
) -> list[dict[str, Any]]:
    if not services or not services.issubset(HOST_RUNTIME_TRANSITION_SERVICES):
        raise CanaryCollectionError("host_runtime_transition_services_invalid")
    table = db.rows(
        "SELECT 1 AS present FROM sqlite_master "
        "WHERE type = 'table' AND name = 'rca_host_runtime_transitions'"
    )
    if not table:
        return []
    rows = db.rows(
        """
        SELECT submission_key, business_key, generation, service_label,
               transition_kind, entity_key, runtime_identity_json,
               runtime_identity_sha256, transitioned_at
          FROM rca_host_runtime_transitions
         WHERE submission_key = ?
         ORDER BY service_label, transition_kind, entity_key
        """,
        (submission_key,),
    )
    projected: list[dict[str, Any]] = []
    for row in rows:
        label = str(row.get("service_label") or "")
        kind = str(row.get("transition_kind") or "")
        if (
            label not in HOST_RUNTIME_TRANSITION_SERVICES
            or HOST_RUNTIME_TRANSITION_KIND_BY_SERVICE.get(label) != kind
            or row.get("submission_key") != submission_key
            or row.get("business_key") != business_key
            or row.get("generation") != generation
        ):
            raise CanaryCollectionError("host_runtime_transition_identity_invalid")
        entity_key = str(row.get("entity_key") or "")
        if not entity_key or len(entity_key) > 512 or "\x00" in entity_key:
            raise CanaryCollectionError("host_runtime_transition_entity_invalid")
        runtime_identity = _db_json(
            row.get("runtime_identity_json"),
            code="host_runtime_transition_runtime_identity_invalid",
        )
        if set(runtime_identity) != RUNTIME_IDENTITY_FIELDS:
            raise CanaryCollectionError(
                "host_runtime_transition_runtime_identity_invalid"
            )
        try:
            identity_json, identity_sha256 = canonical_host_runtime_identity(
                runtime_identity,
                service_label=label,
            )
        except (TypeError, ValueError) as exc:
            raise CanaryCollectionError(
                "host_runtime_transition_runtime_identity_invalid"
            ) from exc
        if (
            row.get("runtime_identity_json") != identity_json
            or row.get("runtime_identity_sha256") != identity_sha256
        ):
            raise CanaryCollectionError("host_runtime_transition_digest_invalid")
        transitioned_at = _parse_timestamp(
            row.get("transitioned_at"),
            code="host_runtime_transition_timestamp_invalid",
        )
        if float(runtime_identity["process_create_time"]) > transitioned_at.timestamp():
            raise CanaryCollectionError("host_runtime_transition_timeline_invalid")
        if label not in services:
            continue
        projected.append({
            "submission_key": submission_key,
            "business_key": business_key,
            "generation": generation,
            "service_label": label,
            "transition_kind": kind,
            "entity_key": entity_key,
            "runtime_identity": runtime_identity,
            "runtime_identity_sha256": identity_sha256,
            "transitioned_at": transitioned_at.isoformat(),
        })
    return projected


def _validate_delivery_runtime_transition_bindings(
    transitions: Sequence[Mapping[str, Any]],
    *,
    delivery_id: str,
    delivery_created_at: str,
    obligations: Sequence[Mapping[str, Any]],
) -> None:
    completed_by_effect = {
        str(item.get("effect_key") or ""): str(item.get("completed_at") or "")
        for item in obligations
    }
    for transition in transitions:
        label = transition.get("service_label")
        if label == "local.pnc.rca-delivery-collector":
            valid = (
                transition.get("entity_key") == delivery_id
                and transition.get("transitioned_at") == delivery_created_at
            )
        else:
            entity_key = str(transition.get("entity_key") or "")
            valid = (
                label == "local.pnc.rca-delivery-dispatcher"
                and entity_key in completed_by_effect
                and transition.get("transitioned_at")
                == completed_by_effect[entity_key]
            )
        if not valid:
            raise CanaryCollectionError(
                "host_runtime_transition_delivery_binding_invalid"
            )


def _require_delivery_runtime_transition_chain(
    transitions: Sequence[Mapping[str, Any]],
    *,
    delivery_id: str,
    obligations: Sequence[Mapping[str, Any]],
) -> None:
    expected = {
        (
            "local.pnc.rca-delivery-collector",
            "delivery_created",
            delivery_id,
        ),
        *{
            (
                "local.pnc.rca-delivery-dispatcher",
                "effect_succeeded",
                str(item.get("effect_key") or ""),
            )
            for item in obligations
        },
    }
    observed = {
        (
            str(item.get("service_label") or ""),
            str(item.get("transition_kind") or ""),
            str(item.get("entity_key") or ""),
        )
        for item in transitions
    }
    if observed != expected or len(transitions) != len(expected):
        raise CanaryCollectionError("host_runtime_transition_chain_incomplete")


def _same_private_receipt_stat(left: os.stat_result, right: os.stat_result) -> bool:
    return (
        left.st_dev == right.st_dev
        and left.st_ino == right.st_ino
        and left.st_mode == right.st_mode
        and left.st_uid == right.st_uid
        and left.st_nlink == right.st_nlink
        and left.st_size == right.st_size
        and left.st_mtime_ns == right.st_mtime_ns
        and left.st_ctime_ns == right.st_ctime_ns
    )


def _open_group_binding_receipt_root(
    receipt_dir: Path, *, code: str
) -> tuple[Path, int, os.stat_result]:
    candidate = receipt_dir.expanduser()
    descriptor = -1
    try:
        resolved = candidate.resolve(strict=True)
        if resolved != Path(os.path.abspath(candidate)):
            raise CanaryCollectionError(code)
        initial = os.lstat(resolved)
        if (
            not stat.S_ISDIR(initial.st_mode)
            or initial.st_uid != os.getuid()
            or stat.S_IMODE(initial.st_mode) != 0o700
        ):
            raise CanaryCollectionError(code)
        descriptor = os.open(
            resolved,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(opened.st_mode)
            or opened.st_dev != initial.st_dev
            or opened.st_ino != initial.st_ino
            or opened.st_uid != os.getuid()
            or stat.S_IMODE(opened.st_mode) != 0o700
        ):
            raise CanaryCollectionError(code)
        return resolved, descriptor, opened
    except CanaryCollectionError:
        if descriptor >= 0:
            os.close(descriptor)
        raise
    except OSError as exc:
        if descriptor >= 0:
            os.close(descriptor)
        raise CanaryCollectionError(code) from exc


def _read_local_jsonl(
    receipt_root: Path,
    root_descriptor: int,
    root_info: os.stat_result,
    filename: str,
    *,
    code: str,
) -> tuple[list[dict[str, Any]], SourceRecord]:
    if (
        not GROUP_BINDING_RECEIPT_FILENAME_RE.fullmatch(filename)
        or Path(filename).name != filename
    ):
        raise CanaryCollectionError(code)
    descriptor = -1
    try:
        info = os.stat(
            filename,
            dir_fd=root_descriptor,
            follow_symlinks=False,
        )
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or info.st_nlink != 1
            or info.st_size <= 0
            or info.st_size > MAX_JSON_BYTES
            or stat.S_IMODE(info.st_mode) != 0o600
        ):
            raise CanaryCollectionError(code)
        descriptor = os.open(
            filename,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=root_descriptor,
        )
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or not _same_private_receipt_stat(info, opened)
            or opened.st_uid != os.getuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o600
        ):
            raise CanaryCollectionError(code)
        remaining = opened.st_size
        chunks: list[bytes] = []
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise CanaryCollectionError(code)
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise CanaryCollectionError(code)
        final_opened = os.fstat(descriptor)
        final_path = os.stat(
            filename,
            dir_fd=root_descriptor,
            follow_symlinks=False,
        )
        final_root = os.fstat(root_descriptor)
        final_root_path = os.lstat(receipt_root)
        if (
            not _same_private_receipt_stat(info, final_opened)
            or not _same_private_receipt_stat(info, final_path)
            or final_root.st_dev != root_info.st_dev
            or final_root.st_ino != root_info.st_ino
            or final_root.st_uid != os.getuid()
            or stat.S_IMODE(final_root.st_mode) != 0o700
            or final_root_path.st_dev != root_info.st_dev
            or final_root_path.st_ino != root_info.st_ino
            or final_root_path.st_uid != os.getuid()
            or stat.S_IMODE(final_root_path.st_mode) != 0o700
        ):
            raise CanaryCollectionError(code)
        raw = b"".join(chunks)
    except FileNotFoundError:
        raise
    except CanaryCollectionError:
        raise
    except OSError as exc:
        raise CanaryCollectionError(code) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if len(raw) != info.st_size or len(raw) > MAX_JSON_BYTES:
        raise CanaryCollectionError(code)
    if not raw.endswith(b"\n"):
        raise CanaryCollectionError(code)
    raw_lines = raw.splitlines()
    if len(raw_lines) != 1 or not raw_lines[0].strip():
        raise CanaryCollectionError(code)
    records = [_decode_json(raw_lines[0], code=code)]
    body = {"records": records}
    return records, SourceRecord(
        path=str(receipt_root / filename),
        size_bytes=len(raw),
        raw_sha256=_sha256_bytes(raw),
        canonical_sha256=_sha256_json(body),
        body=body,
    )


def _gateway_runtime_identity(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != GATEWAY_RUNTIME_IDENTITY_KEYS:
        raise CanaryCollectionError("manual_gateway_runtime_identity_not_proven")
    for field in ("pid",):
        field_value = value.get(field)
        if (
            isinstance(field_value, bool)
            or not isinstance(field_value, int)
            or field_value < 1
        ):
            raise CanaryCollectionError("manual_gateway_runtime_identity_not_proven")
    for field in ("process_create_time", "boot_time"):
        field_value = value.get(field)
        if (
            isinstance(field_value, bool)
            or not isinstance(field_value, (int, float))
            or field_value <= 0
        ):
            raise CanaryCollectionError("manual_gateway_runtime_identity_not_proven")
    if float(value["process_create_time"]) < float(value["boot_time"]):
        raise CanaryCollectionError("manual_gateway_runtime_identity_not_proven")
    if value.get("service_label") != "ai.hermes.gateway":
        raise CanaryCollectionError("manual_gateway_runtime_identity_not_proven")
    for field in ("executable", "script", "cwd"):
        path = str(value.get(field) or "")
        if not path or not Path(path).is_absolute() or "\x00" in path:
            raise CanaryCollectionError("manual_gateway_runtime_identity_not_proven")
    if not str(value["script"]).endswith("/gateway/run.py"):
        raise CanaryCollectionError("manual_gateway_runtime_identity_not_proven")
    for field in (
        "script_sha256",
        "runtime_files_sha256",
        "public_config_sha256",
        "loaded_runtime_sha256",
    ):
        if not SHA256_RE.fullmatch(str(value.get(field) or "")):
            raise CanaryCollectionError("manual_gateway_runtime_identity_not_proven")
    return dict(value)


def _manual_authorization_evidence(
    source: Mapping[str, Any],
    *,
    receipt_dir: Path,
    manual_chat_ids: Sequence[str],
) -> tuple[dict[str, Any], SourceRecord]:
    created_at = _parse_timestamp(
        source.get("created_at"), code="manual_authorization_source_time_invalid"
    )
    candidate_filenames = [
        pnc_group_binding_receipt_filename(
            receipt_date=day.date(),
            platform=source.get("platform"),
            chat_id=source.get("chat_id"),
            user_id=source.get("requester_id"),
            message_id=source.get("message_id"),
        )
        for day in (created_at, created_at - timedelta(days=1))
    ]
    matches: list[tuple[dict[str, Any], SourceRecord, int]] = []
    receipt_root, root_descriptor, root_info = _open_group_binding_receipt_root(
        receipt_dir, code="manual_authorization_receipt_invalid"
    )
    try:
        for filename in candidate_filenames:
            try:
                records, record_source = _read_local_jsonl(
                    receipt_root,
                    root_descriptor,
                    root_info,
                    filename,
                    code="manual_authorization_receipt_invalid",
                )
            except FileNotFoundError:
                continue
            record = records[0]
            if (
                record.get("event_type") == "group_binding_decision"
                and record.get("platform") == source.get("platform")
                and record.get("group_id") == source.get("chat_id")
                and record.get("requester") == source.get("requester_id")
                and record.get("message_id") == source.get("message_id")
                and record.get("decision") == "accepted"
                and record.get("route_surface") == "rca_manual_intake"
                and record.get("risk_gate") == "manual_intake_control_store"
            ):
                matches.append((record, record_source, 1))
    finally:
        os.close(root_descriptor)
    if len(matches) != 1:
        raise CanaryCollectionError("manual_authorization_receipt_not_unique")
    record, record_source, line_number = matches[0]
    authorization = record.get("manual_authorization")
    expected_auth_keys = {
        "schema_version",
        "manual_intake_enabled",
        "manual_chat_allowlist_valid",
        "manual_chat_allowlist_sha256",
        "chat_allowed",
        "mention_verified",
        "debug_requested",
        "debug_enabled",
        "requester_allowed",
        "debug_user_allowlist_sha256",
        "manual_operator_rate_limit",
        "manual_operator_rate_window_seconds",
        "authorized",
    }
    debug_requested = source.get("mode") == "debug"
    active_chat_ids = _manual_chat_ids(manual_chat_ids)
    if (
        not isinstance(authorization, dict)
        or set(authorization) != expected_auth_keys
        or authorization.get("schema_version") != "pnc_rca_manual_authorization_v2"
        or authorization.get("manual_intake_enabled") is not True
        or authorization.get("manual_chat_allowlist_valid") is not True
        or source.get("chat_id") not in active_chat_ids
        or authorization.get("manual_chat_allowlist_sha256")
        != _sha256_json(list(active_chat_ids))
        or authorization.get("chat_allowed") is not True
        or authorization.get("mention_verified") is not True
        or authorization.get("debug_requested") is not debug_requested
        or authorization.get("requester_allowed") is not True
        or authorization.get("authorized") is not True
        or (debug_requested and authorization.get("debug_enabled") is not True)
        or not SHA256_RE.fullmatch(
            str(authorization.get("debug_user_allowlist_sha256") or "")
        )
        or isinstance(authorization.get("manual_operator_rate_limit"), bool)
        or not isinstance(authorization.get("manual_operator_rate_limit"), int)
        or authorization.get("manual_operator_rate_limit", 0) < 1
        or isinstance(
            authorization.get("manual_operator_rate_window_seconds"), bool
        )
        or not isinstance(
            authorization.get("manual_operator_rate_window_seconds"), int
        )
        or authorization.get("manual_operator_rate_window_seconds", 0) < 1
    ):
        raise CanaryCollectionError("manual_authorization_not_proven")
    decision_at = _parse_timestamp(
        record.get("timestamp"), code="manual_authorization_timestamp_invalid"
    )
    if decision_at > created_at:
        raise CanaryCollectionError("manual_authorization_timestamp_invalid")
    snapshot = record.get("decision_snapshot")
    handoff = snapshot.get("handoff_contract") if isinstance(snapshot, dict) else None
    if (
        not isinstance(handoff, dict)
        or handoff.get("mode") != source.get("mode")
        or handoff.get("source_kind") != "feishu_group_manual"
    ):
        raise CanaryCollectionError("manual_authorization_route_contract_mismatch")
    gateway_runtime_identity = _gateway_runtime_identity(
        record.get("gateway_runtime_identity")
    )
    return (
        {
            "schema_version": "pnc_rca_manual_authorization_evidence_v2",
            "receipt_path": record_source.path,
            "line_number": line_number,
            "file_raw_sha256": record_source.raw_sha256,
            "record_sha256": _sha256_json(record),
            "timestamp": str(record["timestamp"]),
            "platform": str(record["platform"]),
            "chat_id": str(record["group_id"]),
            "message_id": str(record["message_id"]),
            "requester_id": str(record["requester"]),
            "decision": "accepted",
            "route_surface": "rca_manual_intake",
            "risk_gate": "manual_intake_control_store",
            "manual_authorization": dict(authorization),
            "gateway_runtime_identity": gateway_runtime_identity,
        },
        record_source,
    )


def _source_projection(
    row: Mapping[str, Any],
    *,
    authorization: Mapping[str, Any] | None,
) -> dict[str, Any]:
    storage_kind = str(row.get("source_kind") or "")
    mode = str(row.get("mode") or "")
    if storage_kind == "kafka_workflow_event":
        normalized_kind = "kafka_issue_created"
    elif storage_kind == "feishu_group_manual":
        normalized_kind = (
            "manual_issue_request" if mode == "run_or_join" else "manual_retrigger"
        )
    else:
        raise CanaryCollectionError("trigger_source_kind_invalid")
    projection = {
        "source_id": _safe_source_id(row.get("source_id")),
        "source_kind": normalized_kind,
        "storage_source_kind": storage_kind,
        "source_dedupe_key": str(row.get("source_dedupe_key") or ""),
        "payload_sha256": _require_sha256(
            row.get("payload_sha256"), "trigger_source_payload_hash_invalid"
        ),
        "platform": str(row.get("platform") or ""),
        "chat_id": str(row.get("chat_id") or ""),
        "thread_id": str(row.get("thread_id") or ""),
        "message_id": str(row.get("message_id") or ""),
        "requester_id": str(row.get("requester_id") or ""),
        "kafka_event_uid": row.get("kafka_event_uid"),
        "mode": mode,
        "outcome": str(row.get("outcome") or ""),
        "created_at": str(row.get("created_at") or ""),
        "binding_role": str(row.get("binding_role") or ""),
        "bound_at": str(row.get("bound_at") or ""),
        "business_key": str(row.get("binding_business_key") or ""),
        "generation": row.get("binding_generation"),
        "authorization": dict(authorization) if authorization is not None else None,
    }
    if projection["source_id"] != _stable_trigger_source_id(
        storage_kind, str(projection["source_dedupe_key"])
    ):
        raise CanaryCollectionError("trigger_source_id_invalid")
    _parse_timestamp(projection["created_at"], code="trigger_source_timestamp_invalid")
    _parse_timestamp(projection["bound_at"], code="trigger_binding_timestamp_invalid")
    return projection


def _read_control_facts(
    db: ReadOnlyDatabase,
    source_id: str,
    *,
    group_binding_receipt_dir: Path,
    manual_chat_ids: Sequence[str],
    terminal_failure: bool = False,
) -> ControlFacts:
    observed_id = _safe_source_id(source_id)
    source_sql = """
        SELECT s.*, b.business_key AS binding_business_key,
               b.generation AS binding_generation,
               b.role AS binding_role, b.bound_at
          FROM rca_trigger_sources AS s
          JOIN rca_trigger_bindings AS b ON b.source_id = s.source_id
         WHERE s.source_id = ?
    """
    observed_row = _one(
        db.rows(source_sql, (observed_id,)), "control_trigger_source_not_unique"
    )
    business_key = str(observed_row.get("binding_business_key") or "")
    generation = observed_row.get("binding_generation")
    if (
        not business_key
        or isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation < 1
    ):
        raise CanaryCollectionError("control_trigger_binding_invalid")
    row = _one(
        db.rows(
            """
            SELECT o.*, t.state AS trigger_state,
                   t.origin_source_id AS trigger_origin_source_id,
                   t.source_event_id AS trigger_source_event_id,
                   t.source_topic AS trigger_source_topic,
                   t.source_partition AS trigger_source_partition,
                   t.source_offset AS trigger_source_offset
              FROM rca_outbox AS o
              JOIN business_triggers AS t
                ON t.business_key = o.business_key AND t.generation = o.generation
             WHERE o.business_key = ? AND o.generation = ?
            """,
            (business_key, generation),
        ),
        "control_generation_outbox_not_unique",
    )
    origin_id = _safe_source_id(row.get("origin_source_id"))
    if row.get("trigger_origin_source_id") != origin_id:
        raise CanaryCollectionError("control_origin_source_mismatch")
    origin_row = _one(
        db.rows(source_sql, (origin_id,)), "control_origin_source_not_unique"
    )
    if (
        origin_row.get("binding_role") != "origin"
        or origin_row.get("binding_business_key") != business_key
        or origin_row.get("binding_generation") != generation
        or observed_row.get("binding_role")
        != ("origin" if observed_id == origin_id else "observer")
    ):
        raise CanaryCollectionError("control_origin_binding_invalid")

    payload = _db_json(row.get("payload_json"), code="control_outbox_payload_invalid")
    result = (
        {}
        if row.get("status") == "quarantined" and row.get("result_json") is None
        else _db_json(row.get("result_json"), code="control_outbox_result_invalid")
    )
    try:
        admission_value = validate_rca_admission(payload.get("admission") or {})
        trigger_context = validate_rca_trigger_context(
            payload.get("trigger_context") or {}
        )
    except RcaAdmissionError as exc:
        raise CanaryCollectionError("control_admission_invalid") from exc
    admission = admission_value.to_dict()
    submission = _safe_submission(admission.get("submission_key"))
    base_payload_keys = {
        "schema_version",
        "business_key",
        "submission_key",
        "creation_rule_version",
        "generation",
        "origin_source_id",
        "admission",
        "trigger_context",
    }
    if trigger_context.source_kind == "kafka_workflow_event":
        expected_payload_keys = base_payload_keys | {
            "source_event_id",
            "topic",
            "partition",
            "offset",
            "normalized_event",
        }
        expected_admission = build_rca_admission(
            project_key=trigger_context.project_key,
            project_simple_name=trigger_context.project_simple_name,
            work_item_type_key=trigger_context.work_item_type_key,
            work_item_id=trigger_context.work_item_id,
            rule_version=trigger_context.creation_rule_version,
            trigger_kind="issue_created",
            topic=payload.get("topic", ""),
            partition=payload.get("partition"),
            offset=payload.get("offset"),
        )
        event_uid = (
            f"{payload.get('topic')}:{payload.get('partition')}:"
            f"{payload.get('offset')}"
        )
        normalized = payload.get("normalized_event")
        if (
            origin_row.get("kafka_event_uid") != event_uid
            or payload.get("source_event_id") != event_uid
            or not isinstance(normalized, dict)
            or normalized.get("project_key") != trigger_context.project_key
            or normalized.get("project_simple_name")
            != trigger_context.project_simple_name
            or normalized.get("work_item_type_key")
            != trigger_context.work_item_type_key
            or normalized.get("work_item_id") != trigger_context.work_item_id
            or normalized.get("creation_rule_version")
            != trigger_context.creation_rule_version
            or normalized.get("issue_url") != trigger_context.issue_url
            or normalized.get("title") != trigger_context.title
        ):
            raise CanaryCollectionError("control_kafka_origin_invalid")
    elif trigger_context.source_kind == "feishu_group_manual":
        expected_payload_keys = base_payload_keys
        expected_admission = build_rca_admission(
            project_key=trigger_context.project_key,
            project_simple_name=trigger_context.project_simple_name,
            work_item_type_key=trigger_context.work_item_type_key,
            work_item_id=trigger_context.work_item_id,
            rule_version=trigger_context.creation_rule_version,
            trigger_kind=(
                "manual_issue_request" if generation == 1 else "manual_retrigger"
            ),
            generation=generation,
        )
    else:
        raise CanaryCollectionError("control_origin_source_kind_invalid")
    refs = admission_value.source_refs
    policy_row = _one(
        db.rows(
            "SELECT policy_sha256, policy_version, policy_json, active "
            "FROM rca_policy_snapshots WHERE active = 1"
        ),
        "control_active_policy_not_unique",
    )
    policy_body = _db_json(
        policy_row.get("policy_json"), code="control_active_policy_invalid"
    )
    try:
        workflow_policy = WorkflowEventPolicy.from_mapping(policy_body).to_dict()
    except (TypeError, ValueError) as exc:
        raise CanaryCollectionError("control_active_policy_invalid") from exc
    policy_sha256 = _sha256_bytes(
        json.dumps(
            workflow_policy,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    )
    if (
        policy_body != workflow_policy
        or policy_row.get("active") != 1
        or policy_row.get("policy_version") != workflow_policy["policy_version"]
        or policy_row.get("policy_sha256") != policy_sha256
        or workflow_policy["policy_version"] != refs.rule_version
    ):
        raise CanaryCollectionError("control_active_policy_invalid")
    outbox_status = str(row.get("status") or "")
    terminal_outbox_valid = terminal_failure and (
        (
            outbox_status == "completed"
            and bool(row.get("completed_at"))
            and row.get("trigger_state") == "submitted"
        )
        or (
            outbox_status == "quarantined"
            and bool(row.get("quarantined_at"))
            and row.get("trigger_state") == "quarantined"
        )
    )
    regular_outbox_valid = (
        not terminal_failure
        and outbox_status == "completed"
        and bool(row.get("completed_at"))
        and row.get("trigger_state") == "submitted"
    )
    if (
        payload.get("schema_version") != "pnc_rca_submission_outbox_v2"
        or set(payload) != expected_payload_keys
        or admission_value != expected_admission
        or trigger_context.source_kind != origin_row.get("source_kind")
        or payload.get("origin_source_id") != origin_id
        or payload.get("submission_key") != submission
        or payload.get("business_key") != business_key
        or payload.get("generation") != generation
        or payload.get("creation_rule_version") != refs.rule_version
        or row.get("submission_key") != submission
        or row.get("business_key") != business_key
        or row.get("generation") != generation
        or row.get("action") != "submit_rca_issue_intake"
        or not (terminal_outbox_valid or regular_outbox_valid)
    ):
        raise CanaryCollectionError("control_admission_binding_mismatch")
    if outbox_status == "completed" and (
        result.get("success") is not True
        or result.get("submission_key") != submission
        or result.get("task_id") != submission
    ):
        raise CanaryCollectionError("control_submission_result_invalid")
    if outbox_status == "quarantined" and result != {}:
        raise CanaryCollectionError("control_quarantined_result_invalid")

    authorization_sources: dict[str, SourceRecord] = {}

    def projected(source_row: Mapping[str, Any]) -> dict[str, Any]:
        authorization = None
        if source_row.get("source_kind") == "feishu_group_manual":
            authorization, source_record = _manual_authorization_evidence(
                source_row,
                receipt_dir=group_binding_receipt_dir,
                manual_chat_ids=manual_chat_ids,
            )
            authorization_sources[str(source_row["source_id"])] = source_record
        return _source_projection(source_row, authorization=authorization)

    execution_origin = projected(origin_row)
    observed_source = (
        execution_origin if observed_id == origin_id else projected(observed_row)
    )

    promotion_rows: list[dict[str, Any]] = []
    kafka_admission_proof: dict[str, Any] | None = None
    if observed_row.get("source_kind") == "kafka_workflow_event":
        event_uid = str(observed_row.get("kafka_event_uid") or "")
        topic, partition, offset = _parse_event_uid(event_uid)
        inbox = _one(
            db.rows("SELECT * FROM kafka_inbox WHERE event_uid = ?", (event_uid,)),
            "control_kafka_event_not_unique",
        )
        if (
            inbox.get("topic") != topic
            or inbox.get("partition_id") != partition
            or inbox.get("offset_id") != offset
            or inbox.get("decision") != "accepted"
            or inbox.get("submission_mode") != "pending"
            or inbox.get("business_key") != business_key
            or inbox.get("submission_key") != submission
            or inbox.get("generation") != generation
            or inbox.get("raw_sha256") != observed_row.get("payload_sha256")
        ):
            raise CanaryCollectionError("control_kafka_event_invalid")
        promotion_rows = db.rows(
            """
            SELECT audit_id, outbox_id, submission_key, operator, reason,
                   outcome, from_status, to_status
              FROM rca_shadow_promotion_audit
             WHERE event_uid = ? ORDER BY audit_id
            """,
            (event_uid,),
        )
        activation_epoch_id = str(row.get("activation_epoch_id") or "")
        activation_ledger_id = row.get("activation_ledger_id")
        direct_rows = []
        if activation_epoch_id and isinstance(activation_ledger_id, int):
            direct_rows = db.rows(
                """
                SELECT al.ledger_id, al.epoch_id, al.entrypoint, al.source_kind,
                       al.source_identity_sha256, al.slot_kind, al.decision,
                       al.business_key, al.submission_key, al.generation, al.bound_at,
                       ae.state AS epoch_state, ae.is_current,
                       s.authorized_source_kind, s.authorized_identity_sha256,
                       s.consumed_ledger_id, s.consumed_at,
                       t.activation_epoch_id AS trigger_epoch_id,
                       t.activation_ledger_id AS trigger_ledger_id
                  FROM rca_activation_admission_ledger AS al
                  JOIN rca_activation_epochs AS ae
                    ON ae.epoch_id = al.epoch_id
                  JOIN rca_activation_budget_slots AS s
                    ON s.epoch_id = al.epoch_id AND s.slot_kind = al.slot_kind
                  JOIN business_triggers AS t
                    ON t.business_key = al.business_key
                   AND t.generation = al.generation
                 WHERE al.ledger_id = ? AND al.epoch_id = ?
                """,
                (activation_ledger_id, activation_epoch_id),
            )
        if direct_rows:
            direct = _one(direct_rows, "control_activation_admission_not_unique")
            source_identity_sha256 = _sha256_json(
                {
                    "event_uid": event_uid,
                    "offset": offset,
                    "partition": partition,
                    "topic": topic,
                }
            )
            if (
                promotion_rows
                or direct.get("entrypoint") != "kafka_ingest"
                or direct.get("source_kind") != "kafka"
                or direct.get("source_identity_sha256") != source_identity_sha256
                or direct.get("slot_kind") != "kafka_success"
                or direct.get("decision") != "admit"
                or direct.get("business_key") != business_key
                or direct.get("submission_key") != submission
                or direct.get("generation") != generation
                or not str(direct.get("bound_at") or "")
                or direct.get("epoch_state") != "bounded_active"
                or direct.get("is_current") != 1
                or direct.get("authorized_source_kind") != "kafka"
                or direct.get("authorized_identity_sha256")
                != source_identity_sha256
                or direct.get("consumed_ledger_id") != activation_ledger_id
                or not str(direct.get("consumed_at") or "")
                or direct.get("trigger_epoch_id") != activation_epoch_id
                or direct.get("trigger_ledger_id") != activation_ledger_id
                or inbox.get("activation_epoch_id") != activation_epoch_id
                or inbox.get("activation_ingress_state") != "bounded_active"
                or inbox.get("activation_required") != 1
                or inbox.get("activation_slot_kind") != "kafka_success"
                or inbox.get("activation_source_identity_sha256")
                != source_identity_sha256
            ):
                raise CanaryCollectionError("control_activation_admission_unproven")
            kafka_admission_proof = {
                "path": "direct_bounded",
                "epoch_id": activation_epoch_id,
                "ledger_id": activation_ledger_id,
                "slot_kind": "kafka_success",
                "source_identity_sha256": source_identity_sha256,
            }
        else:
            promoted = [
                item for item in promotion_rows if item.get("outcome") == "promoted"
            ]
            if (
                len(promoted) != 1
                or any(
                    item.get("outcome") not in {"promoted", "already_promoted"}
                    for item in promotion_rows
                )
                or any(
                    item.get("outbox_id") != row.get("outbox_id")
                    or item.get("submission_key") != submission
                    or not str(item.get("operator") or "").strip()
                    or not str(item.get("reason") or "").strip()
                    for item in promotion_rows
                )
                or promoted[0].get("from_status") != "shadow"
                or promoted[0].get("to_status") != "pending"
            ):
                raise CanaryCollectionError("control_shadow_promotion_unproven")
            kafka_admission_proof = {
                "path": "shadow_promotion",
                "promotion_audit_sha256": _sha256_json(promotion_rows),
            }

    subscriptions = db.rows(
        """
        SELECT s.*, b.bound_at AS source_delivery_bound_at
          FROM rca_trigger_delivery_bindings AS b
          JOIN rca_delivery_subscriptions AS s
            ON s.subscription_key = b.subscription_key
         WHERE b.source_id = ?
         ORDER BY s.effect_kind, s.subscription_key
        """,
        (observed_id,),
    )
    expected_effects = {"feishu_issue_comment"}
    if observed_row.get("source_kind") == "feishu_group_manual":
        expected_effects.add("feishu_thread_reply")
    if {item.get("effect_kind") for item in subscriptions} != expected_effects:
        raise CanaryCollectionError("control_delivery_subscriptions_incomplete")
    for subscription in subscriptions:
        target = _db_json(
            subscription.get("target_json"), code="control_delivery_target_invalid"
        )
        if (
            subscription.get("business_key") != business_key
            or subscription.get("generation") != generation
            or subscription.get("required") != 1
            or subscription.get("status") != "materialized"
            or not subscription.get("delivery_id")
            or not subscription.get("effect_key")
            or not subscription.get("materialized_at")
            or not subscription.get("source_delivery_bound_at")
        ):
            raise CanaryCollectionError("control_delivery_subscription_invalid")
        if subscription.get("effect_kind") == "feishu_issue_comment":
            expected_target = {
                "schema_version": "pnc_rca_delivery_target_v1",
                "platform": "feishu_project",
                "project_key": refs.project_key,
                "work_item_type_key": refs.work_item_type_key,
                "work_item_id": refs.work_item_id,
                "output_cap": "L1",
            }
        else:
            root = str(observed_row.get("thread_id") or "").removeprefix("topic:")
            expected_target = {
                "schema_version": "pnc_rca_delivery_target_v1",
                "platform": "feishu",
                "chat_id": observed_row.get("chat_id"),
                "thread_id": observed_row.get("thread_id"),
                "reply_anchor_message_id": root,
                "source_message_id": observed_row.get("message_id"),
                "requester_id": observed_row.get("requester_id"),
                "reply_in_thread": True,
                "output_cap": "L1",
            }
            if subscription.get("target_key") != (
                f"feishu_thread:{observed_row.get('chat_id')}:{root}"
            ):
                raise CanaryCollectionError("control_thread_target_invalid")
        if target != expected_target:
            raise CanaryCollectionError("control_delivery_target_invalid")
        subscription["target"] = target
        subscription.pop("target_json", None)

    host_runtime_transitions = _read_host_runtime_transitions(
        db,
        submission_key=submission,
        business_key=business_key,
        generation=generation,
        services=CONTROL_RUNTIME_TRANSITION_SERVICES,
    )
    for transition in host_runtime_transitions:
        if transition["service_label"] == "local.pnc.rca-outbox-dispatcher":
            if (
                transition["entity_key"] != str(row["outbox_id"])
                or transition["transitioned_at"]
                != str(row.get("completed_at") or "")
            ):
                raise CanaryCollectionError(
                    "host_runtime_transition_outbox_binding_invalid"
                )
        elif (
            origin_row.get("source_kind") != "kafka_workflow_event"
            or transition["entity_key"]
            != str(origin_row.get("kafka_event_uid") or "")
        ):
            raise CanaryCollectionError(
                "host_runtime_transition_kafka_binding_invalid"
            )
        else:
            kafka_timeline = _one(
                db.rows(
                    "SELECT processed_at FROM kafka_inbox WHERE event_uid = ?",
                    (transition["entity_key"],),
                ),
                "host_runtime_transition_kafka_event_not_unique",
            )
            if transition["transitioned_at"] != str(
                kafka_timeline.get("processed_at") or ""
            ):
                raise CanaryCollectionError(
                    "host_runtime_transition_kafka_timeline_invalid"
                )
    expected_control_transitions: set[tuple[str, str, str]] = set()
    if row.get("status") == "completed":
        expected_control_transitions.add((
            "local.pnc.rca-outbox-dispatcher",
            "outbox_completed",
            str(row["outbox_id"]),
        ))
    if origin_row.get("source_kind") == "kafka_workflow_event":
        expected_control_transitions.add((
            "local.pnc.rca-kafka-consumer",
            "kafka_ingested",
            str(origin_row.get("kafka_event_uid") or ""),
        ))
    observed_control_transitions = {
        (
            str(item["service_label"]),
            str(item["transition_kind"]),
            str(item["entity_key"]),
        )
        for item in host_runtime_transitions
    }
    diagnostic_pre_submit_quarantine = (
        terminal_failure and row.get("status") == "quarantined"
    )
    if not diagnostic_pre_submit_quarantine and (
        observed_control_transitions != expected_control_transitions
        or len(host_runtime_transitions) != len(expected_control_transitions)
    ):
        raise CanaryCollectionError("host_runtime_transition_chain_incomplete")

    snapshot_material = {
        "execution_origin": execution_origin,
        "observed_trigger_source": observed_source,
        "outbox": {
            key: row.get(key)
            for key in (
                "outbox_id",
                "action",
                "business_key",
                "submission_key",
                "generation",
                "origin_source_id",
                "status",
                "completed_at",
                "quarantined_at",
                "last_error_code",
            )
        },
        "payload_sha256": _sha256_json(payload),
        "result_sha256": _sha256_json(result),
        "workflow_policy_sha256": _sha256_json(workflow_policy),
        "kafka_admission_proof": kafka_admission_proof,
        "subscriptions_sha256": _sha256_json(subscriptions),
        "host_runtime_transitions": host_runtime_transitions,
    }
    return ControlFacts(
        admission=admission,
        workflow_policy=workflow_policy,
        submission_key=submission,
        business_key=business_key,
        generation=generation,
        outbox_id=int(row["outbox_id"]),
        outbox_result=result,
        outbox_payload=payload,
        execution_origin=execution_origin,
        observed_trigger_source=observed_source,
        delivery_subscriptions=subscriptions,
        host_runtime_transitions=host_runtime_transitions,
        authorization_sources=authorization_sources,
        snapshot_sha256=_sha256_json(snapshot_material),
    )


def _read_terminal_delivery_facts(
    db: ReadOnlyDatabase,
    *,
    control: ControlFacts,
) -> TerminalDeliveryFacts:
    submission = control.submission_key
    watch = _one(
        db.rows(
            "SELECT * FROM rca_execution_watch WHERE submission_key = ?",
            (submission,),
        ),
        "terminal_delivery_watch_not_unique",
    )
    job = _one(
        db.rows(
            "SELECT * FROM rca_delivery_jobs WHERE submission_key = ?",
            (submission,),
        ),
        "terminal_delivery_job_not_unique",
    )
    outcome = str(job.get("outcome") or "")
    terminal_state = str(job.get("terminal_state") or "")
    error_code = str(job.get("terminal_error_code") or "")
    outcome_key = str(job.get("outcome_key") or "")
    expected_task_id = None if outcome == "quarantined" else submission
    if (
        watch.get("submission_outbox_id") != control.outbox_id
        or watch.get("task_id") != expected_task_id
        or watch.get("state") != "delivery_created"
        or watch.get("delivery_id") != job.get("delivery_id")
        or job.get("business_key") != control.business_key
        or job.get("generation") != control.generation
        or job.get("status") != "delivered"
        or outcome not in TERMINAL_DELIVERY_OUTCOMES
        or not re.fullmatch(r"[a-z][a-z0-9_]{0,119}", terminal_state)
        or not re.fullmatch(r"[a-z][a-z0-9_]{0,119}", error_code)
        or not re.fullmatch(r"g1q3-rca-terminal-v1-[0-9a-f]{64}", outcome_key)
        or job.get("artifact_set_id") != outcome_key
        or job.get("issue_url") != ""
        or job.get("report_url") != ""
        or _db_json(job.get("manifest_json"), code="terminal_manifest_invalid") != {}
        or _db_json(job.get("contract_json"), code="terminal_contract_invalid") != {}
        or _db_json_list(
            job.get("artifacts_json"), code="terminal_artifacts_invalid"
        )
        != []
    ):
        raise CanaryCollectionError("terminal_delivery_job_invalid")
    try:
        primary = build_terminal_delivery(
            business_key=control.business_key,
            submission_key=submission,
            generation=control.generation,
            project_key=str(job.get("project_key") or ""),
            work_item_type_key=str(job.get("work_item_type_key") or ""),
            work_item_id=str(job.get("work_item_id") or ""),
            outcome=outcome,
            terminal_state=terminal_state,
            error_code=error_code,
        )
    except DeliveryContractError as exc:
        raise CanaryCollectionError("terminal_delivery_identity_invalid") from exc
    if (
        job.get("delivery_id") != primary.delivery_id
        or outcome_key != primary.outcome_key
        or job.get("target_key") != primary.target_key
    ):
        raise CanaryCollectionError("terminal_delivery_identity_invalid")
    effects = db.rows(
        "SELECT * FROM rca_delivery_effects WHERE delivery_id = ?",
        (primary.delivery_id,),
    )
    effects_by_key = {str(item.get("effect_key") or ""): item for item in effects}
    if len(effects_by_key) != len(effects):
        raise CanaryCollectionError("terminal_delivery_effect_not_unique")
    obligations: list[dict[str, Any]] = []
    for subscription in control.delivery_subscriptions:
        effect_key = str(subscription.get("effect_key") or "")
        effect = effects_by_key.get(effect_key)
        target = dict(subscription.get("target") or {})
        if subscription.get("effect_kind") == "feishu_issue_comment":
            expected_key = primary.effect_key
            expected_sha = primary.semantic_payload_sha256
            expected_payload = primary.effect_payload
        else:
            try:
                expected_key, expected_sha, expected_payload = (
                    build_terminal_thread_reply_effect(
                        issue_effect_payload=primary.effect_payload,
                        target_key=str(subscription.get("target_key") or ""),
                        target=target,
                    )
                )
            except DeliveryContractError as exc:
                raise CanaryCollectionError(
                    "terminal_delivery_effect_invalid"
                ) from exc
        payload = _db_json(
            effect.get("payload_json") if effect else None,
            code="terminal_delivery_effect_payload_invalid",
        )
        receipt = _db_json(
            effect.get("remote_receipt_json") if effect else None,
            code="terminal_delivery_remote_receipt_invalid",
        )
        if (
            effect is None
            or effect.get("delivery_id") != primary.delivery_id
            or effect.get("effect_kind") != subscription.get("effect_kind")
            or effect.get("target_key") != subscription.get("target_key")
            or effect.get("required") != 1
            or effect.get("outcome") != outcome
            or effect.get("status") != "succeeded"
            or not effect.get("completed_at")
            or effect_key != expected_key
            or effect.get("payload_sha256") != expected_sha
            or payload != expected_payload
            or receipt.get("marker") != expected_payload["marker"]
        ):
            raise CanaryCollectionError("terminal_delivery_effect_invalid")
        remote_id = str(receipt.get("remote_id") or "")
        if not REMOTE_ID_RE.fullmatch(remote_id):
            raise CanaryCollectionError("terminal_delivery_remote_id_invalid")
        attempts = db.rows(
            """
            SELECT outcome, remote_id FROM rca_delivery_attempts
             WHERE effect_key = ? AND outcome IN ('ack', 'reconciled')
            """,
            (effect_key,),
        )
        if (
            len(attempts) != 1
            or attempts[0].get("remote_id") != remote_id
        ):
            raise CanaryCollectionError(
                "terminal_delivery_attempt_confirmation_invalid"
            )
        obligations.append(
            {
                "subscription_key": str(subscription["subscription_key"]),
                "effect_kind": str(subscription["effect_kind"]),
                "target_key": str(subscription["target_key"]),
                "target": target,
                "required": True,
                "subscription_status": "materialized",
                "delivery_id": primary.delivery_id,
                "effect_key": effect_key,
                "effect_status": "succeeded",
                "effect_outcome": outcome,
                "materialized_at": str(subscription["materialized_at"]),
                "completed_at": str(effect["completed_at"]),
                "remote_id": remote_id,
                "attempt_outcome": str(attempts[0]["outcome"]),
                "payload": payload,
            }
        )
    host_runtime_transitions = _read_host_runtime_transitions(
        db,
        submission_key=submission,
        business_key=control.business_key,
        generation=control.generation,
        services=DELIVERY_RUNTIME_TRANSITION_SERVICES,
    )
    _validate_delivery_runtime_transition_bindings(
        host_runtime_transitions,
        delivery_id=primary.delivery_id,
        delivery_created_at=str(watch.get("terminal_at") or ""),
        obligations=obligations,
    )
    if outcome != "quarantined":
        _require_delivery_runtime_transition_chain(
            host_runtime_transitions,
            delivery_id=primary.delivery_id,
            obligations=obligations,
        )
    projection_watch = {
        key: watch.get(key)
        for key in (
            "submission_outbox_id",
            "submission_key",
            "task_id",
            "state",
            "delivery_id",
            "terminal_at",
            "last_error_code",
        )
    }
    projection_job = {
        "delivery_id": primary.delivery_id,
        "status": "delivered",
        "outcome": outcome,
        "outcome_key": outcome_key,
        "terminal_state": terminal_state,
        "terminal_error_code": error_code,
        "artifact_boundary": {
            "manifest": {},
            "contract": {},
            "artifacts": [],
            "issue_url": "",
            "report_url": "",
        },
    }
    return TerminalDeliveryFacts(
        watch=projection_watch,
        job=projection_job,
        delivery_obligations=obligations,
        host_runtime_transitions=host_runtime_transitions,
        snapshot_sha256=_sha256_json(
            {
                "watch": projection_watch,
                "job": projection_job,
                "delivery_obligations": obligations,
                "host_runtime_transitions": host_runtime_transitions,
            }
        ),
    )


def _read_delivery_facts(
    db: ReadOnlyDatabase,
    *,
    control: ControlFacts,
) -> DeliveryFacts:
    submission = control.submission_key
    watch = _one(
        db.rows(
            "SELECT * FROM rca_execution_watch WHERE submission_key = ?", (submission,)
        ),
        "delivery_watch_not_unique",
    )
    job = _one(
        db.rows(
            "SELECT * FROM rca_delivery_jobs WHERE submission_key = ?", (submission,)
        ),
        "delivery_job_not_unique",
    )
    effects = db.rows(
        "SELECT * FROM rca_delivery_effects WHERE delivery_id = ?",
        (job.get("delivery_id"),),
    )
    if (
        watch.get("submission_outbox_id") != control.outbox_id
        or watch.get("task_id") != submission
        or watch.get("state") != "delivery_created"
        or watch.get("delivery_id") != job.get("delivery_id")
        or job.get("status") != "delivered"
    ):
        raise CanaryCollectionError("delivery_not_succeeded")
    manifest = _db_json(job.get("manifest_json"), code="delivery_manifest_invalid")
    contract = _db_json(job.get("contract_json"), code="delivery_contract_invalid")
    artifacts = _db_json_list(
        job.get("artifacts_json"), code="delivery_artifacts_invalid"
    )
    if (
        manifest.get("schema_version") != DELIVERY_MANIFEST_SCHEMA_VERSION
        or contract.get("schema_version") != DELIVERY_CONTRACT_SCHEMA_VERSION
        or manifest.get("submission_key") != submission
        or manifest.get("business_key") != control.business_key
        or manifest.get("generation") != control.generation
    ):
        raise CanaryCollectionError("delivery_identity_mismatch")
    try:
        artifact_set_id = compute_artifact_set_id(manifest)
        persisted = verify_persisted_artifact_inventory(
            manifest=manifest,
            stored_artifacts=artifacts,
            expected_artifact_set_id=artifact_set_id,
        )
        expected_report_url = build_report_url(submission, artifact_set_id)
    except DeliveryContractError as exc:
        raise CanaryCollectionError("delivery_artifact_inventory_invalid") from exc
    if (
        manifest.get("artifact_set_id") != artifact_set_id
        or job.get("artifact_set_id") != artifact_set_id
        or job.get("report_url") != expected_report_url
    ):
        raise CanaryCollectionError("delivery_effect_binding_mismatch")
    effects_by_key = {str(item.get("effect_key") or ""): item for item in effects}
    if len(effects_by_key) != len(effects):
        raise CanaryCollectionError("delivery_effect_not_unique")
    delivery_obligations: list[dict[str, Any]] = []
    primary_effect: dict[str, Any] | None = None
    primary_payload: dict[str, Any] | None = None
    primary_remote_receipt: dict[str, Any] | None = None
    for subscription in control.delivery_subscriptions:
        effect_key = str(subscription.get("effect_key") or "")
        effect = effects_by_key.get(effect_key)
        if (
            effect is None
            or effect.get("delivery_id") != job.get("delivery_id")
            or effect.get("effect_kind") != subscription.get("effect_kind")
            or effect.get("target_key") != subscription.get("target_key")
            or effect.get("required") != 1
            or effect.get("status") != "succeeded"
            or not effect.get("completed_at")
        ):
            raise CanaryCollectionError("delivery_not_succeeded")
        effect_payload = _db_json(
            effect.get("payload_json"), code="delivery_effect_payload_invalid"
        )
        remote_receipt = _db_json(
            effect.get("remote_receipt_json"),
            code="delivery_remote_receipt_invalid",
        )
        marker = f"[RCA_DELIVERY:{effect_key}:{artifact_set_id[-12:]}]"
        if (
            effect_payload.get("effect_key") != effect_key
            or effect_payload.get("artifact_set_id") != artifact_set_id
            or effect_payload.get("report_url") != expected_report_url
            or effect_payload.get("target_key") != effect.get("target_key")
            or effect_payload.get("marker") != marker
            or effect_payload.get("semantic_payload_sha256")
            != effect.get("payload_sha256")
            or remote_receipt.get("marker") != marker
        ):
            raise CanaryCollectionError("delivery_effect_binding_mismatch")
        remote_id = str(remote_receipt.get("remote_id") or "").strip()
        if not REMOTE_ID_RE.fullmatch(remote_id):
            raise CanaryCollectionError("delivery_remote_id_invalid")
        terminal_attempts = db.rows(
            """
            SELECT outcome, remote_id FROM rca_delivery_attempts
             WHERE effect_key = ? AND outcome IN ('ack', 'reconciled')
            """,
            (effect_key,),
        )
        if (
            len(terminal_attempts) != 1
            or terminal_attempts[0].get("remote_id") != remote_id
        ):
            raise CanaryCollectionError("delivery_attempt_confirmation_invalid")
        delivery_obligations.append(
            {
                "subscription_key": str(subscription["subscription_key"]),
                "effect_kind": str(subscription["effect_kind"]),
                "target_key": str(subscription["target_key"]),
                "target": dict(subscription["target"]),
                "required": True,
                "subscription_status": "materialized",
                "delivery_id": str(subscription["delivery_id"]),
                "effect_key": effect_key,
                "effect_status": "succeeded",
                "materialized_at": str(subscription["materialized_at"]),
                "completed_at": str(effect["completed_at"]),
                "remote_id": remote_id,
            }
        )
        if subscription.get("effect_kind") == "feishu_issue_comment":
            primary_effect = effect
            primary_payload = effect_payload
            primary_remote_receipt = remote_receipt
    if (
        primary_effect is None
        or primary_payload is None
        or primary_remote_receipt is None
    ):
        raise CanaryCollectionError("delivery_issue_effect_missing")
    by_role = {item.role: item for item in persisted}
    if set(by_role).isdisjoint({"index_html", "report_data"}):
        raise CanaryCollectionError("delivery_report_artifacts_missing")
    try:
        index = by_role["index_html"]
        report_data = by_role["report_data"]
    except KeyError as exc:
        raise CanaryCollectionError("delivery_report_artifacts_missing") from exc
    report = {
        "ok": True,
        "submission_key": submission,
        "artifact_set_id": artifact_set_id,
        "report_url": expected_report_url,
        "index_html": {"size_bytes": index.size, "sha256": index.sha256},
        "report_data_json": {
            "size_bytes": report_data.size,
            "sha256": report_data.sha256,
        },
        "html_validation": "html_delivery_ready",
        "artifact_policy": HTML_ARTIFACT_POLICY_VERSION,
    }
    delivery = {
        "ok": True,
        "submission_key": submission,
        "artifact_set_id": artifact_set_id,
        "report_url": expected_report_url,
        "effect_key": str(primary_effect.get("effect_key") or ""),
        "target_key": str(primary_effect.get("target_key") or ""),
        "marker": primary_payload["marker"],
        "remote_receipt": primary_remote_receipt,
    }
    host_runtime_transitions = _read_host_runtime_transitions(
        db,
        submission_key=submission,
        business_key=control.business_key,
        generation=control.generation,
        services=DELIVERY_RUNTIME_TRANSITION_SERVICES,
    )
    _validate_delivery_runtime_transition_bindings(
        host_runtime_transitions,
        delivery_id=str(job.get("delivery_id") or ""),
        delivery_created_at=str(watch.get("terminal_at") or ""),
        obligations=delivery_obligations,
    )
    _require_delivery_runtime_transition_chain(
        host_runtime_transitions,
        delivery_id=str(job.get("delivery_id") or ""),
        obligations=delivery_obligations,
    )
    snapshot = {
        "watch": {
            key: watch.get(key)
            for key in (
                "submission_outbox_id",
                "submission_key",
                "task_id",
                "state",
                "delivery_id",
            )
        },
        "job": {
            key: job.get(key)
            for key in (
                "delivery_id",
                "submission_key",
                "artifact_set_id",
                "status",
                "report_url",
            )
        },
        "effects": delivery_obligations,
        "manifest_sha256": _sha256_json(manifest),
        "contract_sha256": _sha256_json(contract),
        "artifacts_sha256": _sha256_json(artifacts),
        "remote_receipt_sha256": _sha256_json(primary_remote_receipt),
        "host_runtime_transitions": host_runtime_transitions,
    }
    return DeliveryFacts(
        manifest=manifest,
        contract=contract,
        artifacts=artifacts,
        report=report,
        delivery=delivery,
        delivery_obligations=delivery_obligations,
        host_runtime_transitions=host_runtime_transitions,
        snapshot_sha256=_sha256_json(snapshot),
    )


def read_local_canary_database_facts(
    config: CollectorConfig,
    source_id: str,
    terminal_failure: bool = False,
) -> dict[str, Any]:
    """Return a stable, JSON-safe projection from the two read-only stores."""
    if not isinstance(config, CollectorConfig):
        raise CanaryCollectionError("collector_config_invalid")
    if not isinstance(terminal_failure, bool):
        raise CanaryCollectionError("terminal_failure_flag_invalid")
    observed_source_id = _safe_source_id(source_id)
    same_db = config.control_db_path.expanduser().resolve(
        strict=False
    ) == config.delivery_db_path.expanduser().resolve(strict=False)
    control_database = ReadOnlyDatabase(config.control_db_path)
    with control_database as control_db:
        control = _read_control_facts(
            control_db,
            observed_source_id,
            group_binding_receipt_dir=config.group_binding_receipt_dir,
            manual_chat_ids=config.manual_chat_ids,
            terminal_failure=terminal_failure,
        )
        if same_db:
            delivery: DeliveryFacts | TerminalDeliveryFacts = (
                _read_terminal_delivery_facts(control_db, control=control)
                if terminal_failure
                else _read_delivery_facts(control_db, control=control)
            )
        else:
            with ReadOnlyDatabase(config.delivery_db_path) as delivery_db:
                delivery = (
                    _read_terminal_delivery_facts(delivery_db, control=control)
                    if terminal_failure
                    else _read_delivery_facts(delivery_db, control=control)
                )

    result: dict[str, Any] = {
        "control_snapshot_sha256": control.snapshot_sha256,
        "delivery_snapshot_sha256": delivery.snapshot_sha256,
        "admission": dict(control.admission),
        "workflow_policy": dict(control.workflow_policy),
        "submission_key": control.submission_key,
        "business_key": control.business_key,
        "generation": control.generation,
        "outbox_id": control.outbox_id,
        "execution_origin": dict(control.execution_origin),
        "observed_trigger_source": dict(control.observed_trigger_source),
        "host_runtime_transitions": sorted(
            [
                *[dict(item) for item in control.host_runtime_transitions],
                *[dict(item) for item in delivery.host_runtime_transitions],
            ],
            key=lambda item: (
                str(item.get("service_label") or ""),
                str(item.get("transition_kind") or ""),
                str(item.get("entity_key") or ""),
            ),
        ),
    }
    if terminal_failure:
        if not isinstance(delivery, TerminalDeliveryFacts):
            raise CanaryCollectionError("terminal_delivery_facts_invalid")
        result.update(
            {
                "watch": dict(delivery.watch),
                "delivery_job": dict(delivery.job),
                "delivery_obligations": [
                    dict(item) for item in delivery.delivery_obligations
                ],
            }
        )
    else:
        if not isinstance(delivery, DeliveryFacts):
            raise CanaryCollectionError("success_delivery_facts_invalid")
        delivery_projection = dict(delivery.delivery)
        remote_receipt = delivery_projection.get("remote_receipt")
        if not isinstance(remote_receipt, dict):
            raise CanaryCollectionError("delivery_remote_receipt_invalid")
        delivery_projection["remote_receipt"] = {
            "remote_id": str(remote_receipt.get("remote_id") or "")
        }
        result.update(
            {
                "report": dict(delivery.report),
                "delivery": delivery_projection,
                "delivery_obligations": [
                    dict(item) for item in delivery.delivery_obligations
                ],
            }
        )
    _canonical_json(result)
    return result


def _reader_fingerprint(health: Mapping[str, Any]) -> str:
    classes = health.get("reader_classes")
    runtime = health.get("runtime")
    api = health.get("api_contract")
    if (
        health.get("schema_version") != REMOTE_READER_HEALTH_SCHEMA_VERSION
        or health.get("ok") is not True
        or not isinstance(runtime, dict)
        or not isinstance(classes, dict)
        or set(classes) != REQUIRED_READER_CLASSES
        or not isinstance(api, dict)
    ):
        raise CanaryCollectionError("reader_health_invalid")
    source = health.get("source")
    if (
        not isinstance(source, dict)
        or source.get("generation_mode") != "machine_generated"
    ):
        raise CanaryCollectionError("reader_health_not_machine_generated")
    normalized_classes: dict[str, Any] = {}
    for name in sorted(REQUIRED_READER_CLASSES):
        item = classes.get(name)
        if not isinstance(item, dict):
            raise CanaryCollectionError("reader_health_invalid")
        normalized_classes[name] = {
            "importable": item.get("importable"),
            "module": item.get("module"),
            "iter_messages_parameters": item.get("iter_messages_parameters"),
            "features": item.get("features"),
        }
    material = {
        "runtime": runtime,
        "reader_classes": normalized_classes,
        "api_contract": api,
    }
    fingerprint = _sha256_json(material)
    if health.get("reader_fingerprint") != fingerprint:
        raise CanaryCollectionError("reader_health_fingerprint_invalid")
    return fingerprint


def _validate_browser_smoke(
    smoke: Mapping[str, Any],
    *,
    report: Mapping[str, Any],
    manifest_raw_sha256: str,
    contract_raw_sha256: str,
) -> None:
    if (
        set(smoke) != BROWSER_SMOKE_KEYS
        or smoke.get("schema_version") != BROWSER_SMOKE_SCHEMA_VERSION
        or smoke.get("ok") is not True
        or smoke.get("machine_generated") is not True
        or smoke.get("source") != "chromium_cdp_network_runtime_log"
        or smoke.get("engine") != "chromium"
        or smoke.get("artifact_set_id") != report.get("artifact_set_id")
        or smoke.get("report_url") != report.get("report_url")
        or smoke.get("index_html_sha256") != report.get("index_html", {}).get("sha256")
        or smoke.get("manifest_sha256") != manifest_raw_sha256
        or smoke.get("delivery_contract_sha256") != contract_raw_sha256
        or smoke.get("artifact_policy") != HTML_ARTIFACT_POLICY_VERSION
        or smoke.get("network_closure") != "manifest_allowlist"
        or smoke.get("desktop_nonblank") is not True
        or smoke.get("mobile_nonblank") is not True
        or smoke.get("blockers") != []
        or any(smoke.get(field) != 0 for field in BROWSER_ZERO_FIELDS)
    ):
        raise CanaryCollectionError("browser_smoke_binding_invalid")
    evidence_sha = _require_sha256(
        smoke.get("evidence_sha256"), "browser_smoke_hash_invalid"
    )
    material = dict(smoke)
    material.pop("evidence_sha256", None)
    if _sha256_json(material) != evidence_sha:
        raise CanaryCollectionError("browser_smoke_hash_invalid")


def _requested_scope_from_remote_read(
    remote_read: Mapping[str, Any],
) -> dict[str, Any]:
    """Project the machine-observed scope fields consumed by receipt validation."""
    requirements = {
        "schema_version": "g1q3_rca_remote_evidence_requirements_v1",
        "requirements_contract_version": remote_read.get(
            "requirements_contract_version"
        ),
        "requirements_contract_hash": remote_read.get("requirements_contract_hash"),
        "function_domain": remote_read.get("function_domain"),
        "requested_topics": remote_read.get("requested_topics"),
        "channel_allowlist": remote_read.get("requested_channels"),
        "requested_window": remote_read.get("requested_window"),
        "requirements_hash": remote_read.get("requirements_hash"),
    }
    return {
        "source": "vm_remote_read_receipt",
        "requirements": requirements,
    }


def _initial_remote_paths(submission: str) -> dict[str, str]:
    root = f"/mnt/tmp/{submission}/"
    return {
        "task_meta": f"/home/mini/.hermes/shared-state/tasks/{submission}/meta.json",
        "worker_result": f"{DEFAULT_VM_WORKER_ROOT}/tasks/{submission}/local-result.json",
        "execution_request": root + "rca_execution_request.json",
        "remote_read": root + "s2_remote_read/remote_read_receipt.json",
        "capacity_lifecycle": root + "derived_capacity_reservation_receipt.json",
        "capacity_meter": root + "derived_capacity_usage_receipt.json",
        "pipeline": root + "pipeline_result.json",
        "service_result": root + "rca_service_result.json",
        "delivery_manifest": root + "delivery_manifest.json",
        "delivery_contract": root + "delivery_contract.json",
    }


def _initial_remote_digest_paths(submission: str) -> dict[str, tuple[str, int]]:
    return {
        "goal": (
            f"/home/mini/.hermes/shared-state/tasks/{submission}/goal.md",
            MAX_GOAL_BYTES,
        )
    }


def _body(records: Mapping[str, SourceRecord], name: str) -> dict[str, Any]:
    record = records.get(name)
    if record is None or record.body is None or record.canonical_sha256 is None:
        raise CanaryCollectionError("remote_source_missing")
    if record.canonical_sha256 != _sha256_json(record.body):
        raise CanaryCollectionError("remote_source_canonical_hash_mismatch")
    return dict(record.body)


def _capacity_admission_projection(
    task_meta: Mapping[str, Any],
    task_meta_record: SourceRecord,
    *,
    submission_key: str,
    goal_sha256: str,
    contract_sha256: str,
    hmac_key: str | bytes,
) -> dict[str, Any]:
    receipt = task_meta.get("rca_prod_admission_receipt")
    if not isinstance(receipt, dict):
        raise CanaryCollectionError("rca_prod_task_meta_receipt_missing")
    schema_version = receipt.get("schema_version")
    if schema_version == prod_admission.SCHEMA_VERSION:
        capacity_mode = "steady"
        expected_receipt_fields = prod_admission.RECEIPT_FIELDS
        authorization_name = "capacity_authorization"
    elif schema_version == prod_admission.BOOTSTRAP_SCHEMA_VERSION:
        capacity_mode = "bootstrap"
        expected_receipt_fields = prod_admission.BOOTSTRAP_RECEIPT_FIELDS
        authorization_name = "bootstrap_authorization"
    else:
        raise CanaryCollectionError("rca_prod_task_meta_receipt_schema_invalid")
    bindings = receipt.get("bindings")
    authorization = receipt.get(authorization_name)
    snapshot = receipt.get("resource_snapshot")
    if (
        set(receipt) != expected_receipt_fields
        or not isinstance(bindings, dict)
        or set(bindings) != prod_admission.BINDING_FIELDS
        or not isinstance(authorization, dict)
        or not isinstance(snapshot, dict)
        or receipt.get("decision") != "allow"
        or receipt.get("resource_class") != "rca_prod"
        or receipt.get("trust_scope") != prod_admission.TRUST_SCOPE
        or receipt.get("single_task") is not True
        or receipt.get("queue_if_blocked") is not False
        or receipt.get("bypass_requested") is not False
        or task_meta.get("resource_class") != "rca_prod"
        or task_meta.get("lane") != "heavy"
        or task_meta.get("queue_if_blocked") is not False
        or task_meta.get("resource_gate_bypass") is not False
    ):
        raise CanaryCollectionError("rca_prod_task_meta_policy_invalid")
    receipt_body = {
        key: value
        for key, value in receipt.items()
        if key not in {"receipt_fingerprint", "hmac_sha256"}
    }
    receipt_fingerprint = _sha256_bytes(
        prod_admission.canonical_bytes(receipt_body)
    )
    key_fingerprint = str(
        task_meta.get("rca_prod_admission_key_fingerprint") or ""
    ).strip().lower()
    if (
        receipt.get("receipt_fingerprint") != receipt_fingerprint
        or not SHA256_RE.fullmatch(str(receipt.get("hmac_sha256") or ""))
        or not SHA256_RE.fullmatch(key_fingerprint)
        or receipt.get("resource_snapshot_sha256") != _sha256_json(snapshot)
    ):
        raise CanaryCollectionError("rca_prod_task_meta_fingerprint_invalid")
    expected_bindings = {
        "task_id": submission_key,
        "work_dir": f"/mnt/tmp/{submission_key}",
        "reservation_id": str(task_meta.get("reservation_id") or ""),
        "reservation_fence": str(task_meta.get("reservation_fence") or ""),
        "reservation_contract_sha256": str(
            task_meta.get("reservation_contract_sha256") or ""
        ),
        "goal_sha256": goal_sha256,
        "command_sha256": str(task_meta.get("rca_prod_command_sha256") or ""),
        "contract_sha256": contract_sha256,
    }
    if (
        any(bindings.get(key) != value for key, value in expected_bindings.items())
        or not str(bindings.get("attempt_id") or "").strip()
        or task_meta.get("rca_prod_goal_sha256") != goal_sha256
        or task_meta.get("rca_prod_contract_sha256") != contract_sha256
        or task_meta.get("rca_prod_attempt_id") != bindings.get("attempt_id")
        or task_meta.get("rca_prod_command_sha256")
        != prod_admission.command_sha256(
            prod_admission.build_rca_prod_command_argv(submission_key)
        )
    ):
        raise CanaryCollectionError("rca_prod_task_meta_binding_invalid")
    try:
        if capacity_mode == "steady":
            prod_admission.validate_rca_prod_receipt(
                receipt,
                expected_bindings=bindings,
                hmac_key=hmac_key,
                allow_historical=True,
            )
        else:
            prod_admission.validate_rca_prod_bootstrap_receipt(
                receipt,
                expected_bindings=bindings,
                expected_epoch_id=str(authorization.get("bootstrap_epoch_id") or ""),
                expected_release_bom_sha256=str(
                    authorization.get("release_bom_sha256") or ""
                ),
                expected_active_release_binding_sha256=str(
                    authorization.get("active_release_binding_sha256") or ""
                ),
                hmac_key=hmac_key,
                allow_historical=True,
            )
    except prod_admission.RcaProdAdmissionError as exc:
        raise CanaryCollectionError("rca_prod_task_meta_signature_invalid") from exc
    projection = {
        "resource_class": "rca_prod",
        "capacity_mode": capacity_mode,
        "task_meta_sha256": task_meta_record.raw_sha256,
        "admission_receipt_sha256": _sha256_json(receipt),
        "admission_schema_version": schema_version,
        "admission_key_fingerprint": key_fingerprint,
        "queue_if_blocked": False,
        "resource_gate_bypass": False,
    }
    bootstrap_meta_fields = {
        "rca_prod_capacity_mode",
        "rca_prod_bootstrap_epoch_id",
        "rca_prod_bootstrap_started_at",
        "rca_prod_bootstrap_deadline",
        "rca_prod_bootstrap_authorization_fingerprint",
        "rca_prod_bootstrap_release_approval_id",
        "rca_prod_bootstrap_max_concurrency",
        "rca_prod_bootstrap_daily_started_attempt_quota",
        "rca_prod_bootstrap_quota_timezone",
        "rca_prod_bootstrap_root_required_available_bytes",
        "rca_prod_bootstrap_delivery_required_available_bytes",
        "rca_prod_release_bom_sha256",
        "rca_prod_active_release_binding_sha256",
    }
    if capacity_mode == "steady":
        successful_samples = authorization.get("successful_sample_count")
        materialized_samples = authorization.get("input_materialized_sample_count")
        steady_root = authorization.get("root_required_available_bytes")
        steady_delivery = authorization.get("delivery_required_available_bytes")
        if (
            set(authorization) != prod_admission.CAPACITY_FIELDS
            or any(field in task_meta for field in bootstrap_meta_fields)
            or isinstance(successful_samples, bool)
            or not isinstance(successful_samples, int)
            or successful_samples < 20
            or materialized_samples != 0
            or isinstance(steady_root, bool)
            or not isinstance(steady_root, int)
            or steady_root < prod_admission.MIN_ROOT_AVAILABLE_BYTES
            or isinstance(steady_delivery, bool)
            or not isinstance(steady_delivery, int)
            or steady_delivery < prod_admission.MIN_DELIVERY_AVAILABLE_BYTES
            or any(
                not SHA256_RE.fullmatch(str(authorization.get(field) or ""))
                for field in (
                    "receipt_fingerprint",
                    "approval_evidence_sha256",
                    "authorization_receipt_sha256",
                )
            )
        ):
            raise CanaryCollectionError("rca_prod_steady_capacity_invalid")
        return projection
    if (
        set(authorization) != prod_admission.BOOTSTRAP_SIGNED_AUTHORIZATION_FIELDS
        or receipt.get("capacity_mode") != "bootstrap"
    ):
        raise CanaryCollectionError("rca_prod_bootstrap_capacity_invalid")
    bootstrap_projection = {
        "bootstrap_epoch_id": authorization.get("bootstrap_epoch_id"),
        "bootstrap_started_at": authorization.get("started_at"),
        "bootstrap_deadline": authorization.get("deadline"),
        "bootstrap_authorization_fingerprint": authorization.get(
            "receipt_fingerprint"
        ),
        "release_bom_sha256": authorization.get("release_bom_sha256"),
        "active_release_binding_sha256": authorization.get(
            "active_release_binding_sha256"
        ),
        "release_approval_id": authorization.get("release_approval_id"),
        "max_concurrency": authorization.get("max_concurrency"),
        "daily_started_attempt_quota": authorization.get(
            "daily_started_attempt_quota"
        ),
        "quota_timezone": authorization.get("quota_timezone"),
        "root_required_available_bytes": authorization.get(
            "root_required_available_bytes"
        ),
        "delivery_required_available_bytes": authorization.get(
            "delivery_required_available_bytes"
        ),
    }
    fixed_bootstrap_authorization = {
        "schema_version": prod_bootstrap.SCHEMA_VERSION,
        "capacity_mode": "bootstrap",
        "max_concurrency": prod_bootstrap.MAX_CONCURRENCY,
        "daily_started_attempt_quota": prod_bootstrap.DAILY_STARTED_ATTEMPT_QUOTA,
        "quota_timezone": prod_bootstrap.QUOTA_TIMEZONE,
        "root_reserve_bytes": prod_bootstrap.ROOT_RESERVE_BYTES,
        "root_per_task_bytes": prod_bootstrap.ROOT_PER_TASK_BYTES,
        "root_required_available_bytes": (
            prod_bootstrap.ROOT_REQUIRED_AVAILABLE_BYTES
        ),
        "delivery_reserve_bytes": prod_bootstrap.DELIVERY_RESERVE_BYTES,
        "delivery_per_task_bytes": prod_bootstrap.DELIVERY_PER_TASK_BYTES,
        "delivery_required_available_bytes": (
            prod_bootstrap.DELIVERY_REQUIRED_AVAILABLE_BYTES
        ),
        "queue_if_blocked": False,
        "bypass_requested": False,
        "input_materialization": "forbidden",
    }
    expected_bootstrap_meta = {
        "rca_prod_capacity_mode": "bootstrap",
        "rca_prod_bootstrap_epoch_id": bootstrap_projection["bootstrap_epoch_id"],
        "rca_prod_bootstrap_started_at": bootstrap_projection[
            "bootstrap_started_at"
        ],
        "rca_prod_bootstrap_deadline": bootstrap_projection["bootstrap_deadline"],
        "rca_prod_bootstrap_authorization_fingerprint": bootstrap_projection[
            "bootstrap_authorization_fingerprint"
        ],
        "rca_prod_bootstrap_release_approval_id": bootstrap_projection[
            "release_approval_id"
        ],
        "rca_prod_bootstrap_max_concurrency": prod_bootstrap.MAX_CONCURRENCY,
        "rca_prod_bootstrap_daily_started_attempt_quota": (
            prod_bootstrap.DAILY_STARTED_ATTEMPT_QUOTA
        ),
        "rca_prod_bootstrap_quota_timezone": prod_bootstrap.QUOTA_TIMEZONE,
        "rca_prod_bootstrap_root_required_available_bytes": (
            prod_bootstrap.ROOT_REQUIRED_AVAILABLE_BYTES
        ),
        "rca_prod_bootstrap_delivery_required_available_bytes": (
            prod_bootstrap.DELIVERY_REQUIRED_AVAILABLE_BYTES
        ),
        "rca_prod_release_bom_sha256": bootstrap_projection[
            "release_bom_sha256"
        ],
        "rca_prod_active_release_binding_sha256": bootstrap_projection[
            "active_release_binding_sha256"
        ],
    }
    if (
        any(task_meta.get(key) != value for key, value in expected_bootstrap_meta.items())
        or any(
            authorization.get(key) != value
            for key, value in fixed_bootstrap_authorization.items()
        )
        or bootstrap_projection["max_concurrency"] != prod_bootstrap.MAX_CONCURRENCY
        or bootstrap_projection["daily_started_attempt_quota"]
        != prod_bootstrap.DAILY_STARTED_ATTEMPT_QUOTA
        or bootstrap_projection["quota_timezone"] != prod_bootstrap.QUOTA_TIMEZONE
        or bootstrap_projection["root_required_available_bytes"]
        != prod_bootstrap.ROOT_REQUIRED_AVAILABLE_BYTES
        or bootstrap_projection["delivery_required_available_bytes"]
        != prod_bootstrap.DELIVERY_REQUIRED_AVAILABLE_BYTES
        or any(
            not SHA256_RE.fullmatch(str(bootstrap_projection[field] or ""))
            for field in ("bootstrap_authorization_fingerprint", "release_bom_sha256")
        )
        or any(
            not SHA256_RE.fullmatch(str(authorization.get(field) or ""))
            for field in (
                "authorization_receipt_sha256",
                "approval_evidence_sha256",
            )
        )
        or not str(bootstrap_projection["release_approval_id"] or "").strip()
    ):
        raise CanaryCollectionError("rca_prod_bootstrap_capacity_invalid")
    return {**projection, **bootstrap_projection}


class CanaryReceiptCollector:
    def __init__(
        self,
        config: CollectorConfig,
        *,
        remote_reader: RemoteSourceReader | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.config = config
        self.remote_reader = remote_reader or SshMiniAgentReader(
            config.ssh_mini_agent,
            timeout_seconds=config.remote_timeout_seconds,
        )
        self.now = now or (lambda: datetime.now(timezone.utc))

    def collect_terminal_failure(self, source_id: str) -> CollectionResult:
        observed_source_id = _safe_source_id(source_id)
        same_db = self.config.control_db_path.expanduser().resolve(
            strict=False
        ) == self.config.delivery_db_path.expanduser().resolve(strict=False)
        control_database = ReadOnlyDatabase(self.config.control_db_path)
        delivery_database = control_database
        with control_database as control_db:
            control = _read_control_facts(
                control_db,
                observed_source_id,
                group_binding_receipt_dir=self.config.group_binding_receipt_dir,
                manual_chat_ids=self.config.manual_chat_ids,
                terminal_failure=True,
            )
            if same_db:
                terminal = _read_terminal_delivery_facts(control_db, control=control)
            else:
                delivery_database = ReadOnlyDatabase(self.config.delivery_db_path)
                with delivery_database as delivery_db:
                    terminal = _read_terminal_delivery_facts(
                        delivery_db, control=control
                    )
        if (
            control.observed_trigger_source.get("storage_source_kind")
            != "feishu_group_manual"
        ):
            raise CanaryCollectionError(
                "terminal_delivery_canary_manual_source_required"
            )
        collected_at = self.now().astimezone(timezone.utc)
        receipt = {
            "schema_version": TERMINAL_CANARY_RECEIPT_SCHEMA_VERSION,
            "observed_at": _utc_iso(collected_at),
            "ok": True,
            "admission": control.admission,
            "observed_trigger_source": control.observed_trigger_source,
            "submission_key": control.submission_key,
            "business_key": control.business_key,
            "generation": control.generation,
            "outcome": terminal.job["outcome"],
            "terminal_state": terminal.job["terminal_state"],
            "error_code": terminal.job["terminal_error_code"],
            "watch": terminal.watch,
            "delivery_job": terminal.job,
            "delivery_obligations": terminal.delivery_obligations,
        }
        try:
            from scripts.pnc_rca_release_gate import (
                EvidenceError,
                validate_terminal_delivery_canary,
            )

            validate_terminal_delivery_canary(
                receipt,
                expected_manual_chat_ids=self.config.manual_chat_ids,
                expected_rule_version=str(
                    control.admission["source_refs"]["rule_version"]
                ),
                expected_workflow_policy=control.workflow_policy,
                now=collected_at,
                max_age_seconds=GATE_VALIDATION_MAX_AGE_SECONDS,
            )
        except (ImportError, EvidenceError) as exc:
            raise CanaryCollectionError(
                "terminal_delivery_canary_gate_incompatible"
            ) from exc
        provenance = {
            "schema_version": TERMINAL_CANARY_SOURCE_SCHEMA_VERSION,
            "collected_at": receipt["observed_at"],
            "read_only": True,
            "external_side_effects": False,
            "collector": {
                "path": str(Path(__file__).resolve()),
                "sha256": _sha256_bytes(Path(__file__).read_bytes()),
            },
            "observed_trigger_source_sha256": _sha256_json(
                control.observed_trigger_source
            ),
            "submission_key_sha256": _sha256_bytes(
                control.submission_key.encode("utf-8")
            ),
            "control_database": _database_provenance(
                self.config.control_db_path,
                control.snapshot_sha256,
                control_database.snapshot_file_info,
            ),
            "delivery_database": _database_provenance(
                self.config.delivery_db_path,
                terminal.snapshot_sha256,
                delivery_database.snapshot_file_info,
            ),
            "local_machine_sources": {
                "group_binding_authorizations": {
                    source_key: _source_provenance(source)
                    for source_key, source in sorted(
                        control.authorization_sources.items()
                    )
                }
            },
            "receipt_sha256": _sha256_json(receipt),
        }
        return CollectionResult(receipt=receipt, provenance=provenance)

    def collect_manual_success(self, source_id: str) -> CollectionResult:
        result = self.collect(source_id)
        execution_origin = result.receipt.get("execution_origin")
        observed_source = result.receipt.get("observed_trigger_source")
        admission = result.receipt.get("admission")
        execution_request = result.receipt.get("execution_request")
        if not all(
            isinstance(value, dict)
            for value in (
                execution_origin,
                observed_source,
                admission,
                execution_request,
            )
        ):
            raise CanaryCollectionError("manual_success_projection_invalid")
        source_refs = admission.get("source_refs")
        request_refs = execution_request.get("source_refs")
        if not isinstance(source_refs, dict) or not isinstance(request_refs, dict):
            raise CanaryCollectionError("manual_success_projection_invalid")
        if (
            execution_origin != observed_source
            or execution_origin.get("storage_source_kind") != "feishu_group_manual"
            or execution_origin.get("source_kind") != "manual_issue_request"
            or execution_origin.get("mode") != "run_or_join"
            or execution_origin.get("outcome") != "created"
            or execution_origin.get("binding_role") != "origin"
            or execution_origin.get("generation") != 1
            or execution_origin.get("chat_id") != G1Q3_RCA_GROUP_ID
            or admission.get("trigger_kind") != "manual_issue_request"
            or admission.get("generation") != 1
            or source_refs.get("topic") != ""
            or source_refs.get("partition") is not None
            or source_refs.get("offset") is not None
            or request_refs.get("source_kind") != "feishu_group_manual"
            or request_refs.get("origin_source_id")
            != execution_origin.get("source_id")
            or any(
                key in request_refs
                for key in ("source_event_id", "topic", "partition", "offset")
            )
        ):
            raise CanaryCollectionError("manual_success_origin_required")
        if {
            item.get("effect_kind")
            for item in result.receipt.get("delivery_obligations", [])
            if isinstance(item, dict)
        } != {"feishu_issue_comment", "feishu_thread_reply"}:
            raise CanaryCollectionError("manual_success_delivery_incomplete")
        return CollectionResult(
            receipt=result.receipt,
            provenance=result.provenance,
            evidence_role="manual_success",
        )

    def collect(self, source_id: str) -> CollectionResult:
        observed_source_id = _safe_source_id(source_id)
        same_db = self.config.control_db_path.expanduser().resolve(
            strict=False
        ) == self.config.delivery_db_path.expanduser().resolve(strict=False)
        control_database = ReadOnlyDatabase(self.config.control_db_path)
        delivery_database = control_database
        with control_database as control_db:
            control = _read_control_facts(
                control_db,
                observed_source_id,
                group_binding_receipt_dir=self.config.group_binding_receipt_dir,
                manual_chat_ids=self.config.manual_chat_ids,
            )
            if same_db:
                delivery = _read_delivery_facts(control_db, control=control)
            else:
                delivery_database = ReadOnlyDatabase(self.config.delivery_db_path)
                with delivery_database as delivery_db:
                    delivery = _read_delivery_facts(delivery_db, control=control)

        records = self.remote_reader.read_sources(
            json_paths=_initial_remote_paths(control.submission_key),
            digest_paths=_initial_remote_digest_paths(control.submission_key),
        )
        task_meta = _body(records, "task_meta")
        request = _body(records, "execution_request")
        remote_read = _body(records, "remote_read")
        lifecycle = _body(records, "capacity_lifecycle")
        meter = _body(records, "capacity_meter")
        pipeline = _body(records, "pipeline")
        service = _body(records, "service_result")
        worker = _body(records, "worker_result")
        remote_manifest = _body(records, "delivery_manifest")
        remote_contract = _body(records, "delivery_contract")
        artifact_root = f"/mnt/tmp/{control.submission_key}/"
        artifact_cifs_root = (
            "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/"
            f"{control.submission_key}/"
        )
        refs = request.get("source_refs")
        data = request.get("data")
        if not isinstance(refs, dict) or not isinstance(data, dict):
            raise CanaryCollectionError("execution_request_invalid")
        execution_origin = control.execution_origin
        expected_source_refs = {
            "task_id": control.submission_key,
            "source_kind": execution_origin["storage_source_kind"],
            "origin_source_id": execution_origin["source_id"],
            "rule_version": control.admission["source_refs"]["rule_version"],
            "generation": control.generation,
            "business_key": control.business_key,
            "submission_key": control.submission_key,
        }
        admission_refs = control.admission["source_refs"]
        if execution_origin["storage_source_kind"] == "kafka_workflow_event":
            expected_source_refs.update(
                {
                    "source_event_id": execution_origin["kafka_event_uid"],
                    "topic": admission_refs["topic"],
                    "partition": admission_refs["partition"],
                    "offset": admission_refs["offset"],
                }
            )
        if (
            request.get("schema_version") != "g1q3_rca_execution_request_v2"
            or refs != expected_source_refs
            or data.get("artifact_root") != artifact_root
            or data.get("artifact_cifs_root") != artifact_cifs_root
        ):
            raise CanaryCollectionError("execution_request_binding_mismatch")
        try:
            validate_remote_data_access(data.get("data_access"))
        except RemoteDataAccessError as exc:
            raise CanaryCollectionError(
                "execution_request_remote_read_invalid"
            ) from exc
        request_sha = _sha256_execution_request(request)
        request_record = records["execution_request"]

        remote_record = records["remote_read"]
        meter_record = records["capacity_meter"]
        meter_accounting = (
            meter.get("accounting")
            if isinstance(meter.get("accounting"), dict)
            else {}
        )
        expected_tmp_root = artifact_root.rstrip("/")
        raw_hfs_root = str(meter_accounting.get("hfs_root") or "")
        hfs_path = PurePosixPath(raw_hfs_root)
        cases_root = PurePosixPath(expected_tmp_root) / "cases"
        try:
            case_relative = hfs_path.relative_to(cases_root)
        except ValueError:
            case_relative = PurePosixPath()
        if (
            set(meter_accounting)
            != {"mode", "tmp_root", "hfs_root", "relationship"}
            or meter_accounting.get("mode") != CAPACITY_METER_ACCOUNTING_MODE
            or meter_accounting.get("tmp_root") != expected_tmp_root
            or meter_accounting.get("relationship") != "hfs_nested_in_tmp"
            or not hfs_path.is_absolute()
            or str(hfs_path) != raw_hfs_root
            or len(case_relative.parts) != 1
            or case_relative.name in {"", ".", ".."}
        ):
            raise CanaryCollectionError("capacity_meter_accounting_invalid")
        pipeline_remote_ref = pipeline.get("remote_read_receipt")
        pipeline_capacity = pipeline.get("capacity_usage")
        remote_cache = remote_read.get("derived_stream_cache")
        if not isinstance(remote_cache, dict):
            raise CanaryCollectionError("remote_stream_cache_invalid")
        cache_mount = _validate_cifs_storage(
            remote_cache, code="remote_stream_cache_storage_invalid"
        )
        if (
            remote_read.get("schema_version") != REMOTE_READ_RECEIPT_SCHEMA_VERSION
            or remote_read.get("status") != "completed"
            or not isinstance(pipeline_remote_ref, dict)
            or pipeline_remote_ref.get("path") != remote_record.path
            or pipeline_remote_ref.get("sha256") != remote_record.raw_sha256
            or meter.get("schema_version") != CAPACITY_METER_SCHEMA_VERSION
            or not isinstance(pipeline_capacity, dict)
            or pipeline_capacity.get("path") != meter_record.path
            or pipeline_capacity.get("sha256") != meter_record.raw_sha256
        ):
            raise CanaryCollectionError("pipeline_receipt_hash_binding_invalid")
        if (
            pipeline.get("status") != "report_generated_need_review"
            or pipeline.get("stage") != "s6_report"
            or pipeline.get("blocker") is not None
            or pipeline.get("remote_stream_cache") != remote_cache
            or set(pipeline_capacity)
            != {
                "path",
                "sha256",
                "status",
                "within_budget",
                "limits",
                "peaks",
                "terminal",
            }
            or pipeline_capacity.get("status") != "completed"
            or pipeline_capacity.get("within_budget") is not True
            or pipeline_capacity.get("limits") != meter.get("limits")
            or pipeline_capacity.get("peaks") != meter.get("peaks")
            or pipeline_capacity.get("terminal") != meter.get("terminal")
        ):
            raise CanaryCollectionError("pipeline_not_completed")

        full_receipts = lifecycle.get("full_receipts")
        reservation = request.get("toolchain", {}).get("derived_capacity_reservation")
        result_reservation = control.outbox_result.get("derived_capacity_reservation")
        if (
            lifecycle.get("schema_version") != CAPACITY_LIFECYCLE_SCHEMA_VERSION
            or not isinstance(full_receipts, dict)
            or set(full_receipts) != {"reserved", "activate", "release"}
            or full_receipts.get("reserved") != reservation
            or pipeline.get("derived_capacity_reservation") != lifecycle
            or not isinstance(result_reservation, dict)
            or result_reservation.get("receipt_sha256")
            != _sha256_json(full_receipts.get("reserved"))
            or not isinstance(lifecycle.get("audit"), dict)
            or lifecycle["audit"].get("terminal_proven") is not True
        ):
            raise CanaryCollectionError("capacity_lifecycle_binding_invalid")
        if (
            meter.get("status") != "completed"
            or meter.get("within_budget") is not True
            or not isinstance(meter.get("identity"), dict)
            or meter["identity"].get("submission_key") != control.submission_key
        ):
            raise CanaryCollectionError("capacity_meter_invalid")

        downstream = pipeline.get("downstream_stage_receipts")
        meter_stages = meter.get("stages")
        if (
            not isinstance(downstream, dict)
            or set(downstream) != set(DOWNSTREAM_STAGE_NAMES)
            or not isinstance(meter_stages, dict)
        ):
            raise CanaryCollectionError("downstream_receipts_incomplete")
        downstream_paths: dict[str, str] = {}
        stage_lineages: dict[str, dict[str, Any]] = {}
        for short, full in DOWNSTREAM_STAGE_NAMES.items():
            projected = downstream.get(short)
            metered = meter_stages.get(full)
            if not isinstance(projected, dict) or not isinstance(metered, dict):
                raise CanaryCollectionError("downstream_receipts_incomplete")
            if set(projected) != {
                "status",
                "finished_at",
                "artifact_receipt_path",
                "artifact_receipt_sha256",
            }:
                raise CanaryCollectionError("downstream_receipt_shape_invalid")
            path = _path_under_artifact_root(
                projected.get("artifact_receipt_path"),
                artifact_root,
                code="downstream_receipt_path_invalid",
            )
            expected_path = f"{artifact_root}{stage_lineage_relative_path(short)}"
            if (
                projected.get("status") != "completed"
                or projected.get("finished_at") != metered.get("finished_at")
                or path != metered.get("artifact_receipt_path")
                or path != expected_path
            ):
                raise CanaryCollectionError("downstream_receipt_binding_invalid")
            _require_sha256(
                projected.get("artifact_receipt_sha256"),
                "downstream_receipt_hash_invalid",
            )
            downstream_paths[f"stage_{short}"] = path
        stage_records = self.remote_reader.read_sources(json_paths=downstream_paths)
        for short in DOWNSTREAM_STAGE_NAMES:
            source = stage_records[f"stage_{short}"]
            if source.raw_sha256 != downstream[short].get("artifact_receipt_sha256"):
                raise CanaryCollectionError("downstream_receipt_hash_mismatch")
            stage_body = _body(stage_records, f"stage_{short}")
            try:
                lineage = validate_stage_lineage_receipt(
                    stage_body,
                    expected_stage=DOWNSTREAM_STAGE_NAMES[short],
                    artifact_root=artifact_root,
                )
            except StageLineageError as exc:
                raise CanaryCollectionError(exc.code) from exc
            if lineage["finished_at"] != downstream[short].get("finished_at"):
                raise CanaryCollectionError("stage_lineage_finished_at_mismatch")
            stage_lineages[short] = lineage

        if remote_manifest != delivery.manifest or remote_contract != delivery.contract:
            raise CanaryCollectionError("delivery_remote_store_mismatch")
        manifest_record = records["delivery_manifest"]
        artifact_digests: dict[str, tuple[str, int]] = {}
        cache_bytes = remote_cache.get("bytes")
        if (
            isinstance(cache_bytes, bool)
            or not isinstance(cache_bytes, int)
            or cache_bytes < 1
            or cache_bytes > MAX_REMOTE_DIGEST_BYTES
        ):
            raise CanaryCollectionError("remote_stream_cache_size_invalid")
        cache_path = _path_under_artifact_root(
            remote_cache.get("path"),
            artifact_root,
            code="remote_stream_cache_path_invalid",
        )
        cache_sha256 = _require_sha256(
            remote_cache.get("sha256"), "remote_stream_cache_hash_invalid"
        )
        artifact_digests["artifact_remote_stream_cache"] = (
            cache_path,
            cache_bytes,
        )
        for item in delivery.artifacts:
            role = str(item.get("role") or "")
            if role not in {"index_html", "report_data"}:
                continue
            path = _path_under_artifact_root(
                item.get("path"), artifact_root, code="report_artifact_path_invalid"
            )
            artifact_digests[f"artifact_{role}"] = (path, int(item["size"]))
        artifact_records = self.remote_reader.read_sources(
            json_paths={}, digest_paths=artifact_digests
        )
        cache_source = artifact_records.get("artifact_remote_stream_cache")
        if (
            cache_source is None
            or cache_source.size_bytes != cache_bytes
            or cache_source.raw_sha256 != cache_sha256
        ):
            raise CanaryCollectionError("remote_stream_cache_hash_mismatch")
        for role in ("index_html", "report_data"):
            expected = next(
                (item for item in delivery.artifacts if item.get("role") == role), None
            )
            source = artifact_records.get(f"artifact_{role}")
            if (
                not isinstance(expected, dict)
                or source is None
                or source.size_bytes != expected.get("size")
                or source.raw_sha256 != expected.get("sha256")
            ):
                raise CanaryCollectionError("report_artifact_hash_mismatch")

        health_path = self.config.evidence_dir / "remote_reader_health.json"
        health, health_record = _read_local_json(
            health_path, code="reader_health_source_invalid"
        )
        reader_fingerprint = _reader_fingerprint(health)
        runtime = remote_read.get("execution_runtime")
        health_runtime = health.get("runtime")
        if (
            not isinstance(runtime, dict)
            or not isinstance(health_runtime, dict)
            or any(health_runtime.get(key) != value for key, value in runtime.items())
        ):
            raise CanaryCollectionError("remote_reader_runtime_binding_invalid")

        smoke_path = (
            self.config.evidence_dir
            / "machine_sources"
            / control.submission_key
            / "browser_smoke.json"
        )
        smoke, smoke_record = _read_local_json(
            smoke_path, code="browser_smoke_source_invalid"
        )
        report = dict(delivery.report)
        report["manifest_sha256"] = manifest_record.raw_sha256
        report["delivery_manifest"] = {
            "size_bytes": manifest_record.size_bytes,
            "sha256": manifest_record.raw_sha256,
            "body": remote_manifest,
        }
        _validate_browser_smoke(
            smoke,
            report=report,
            manifest_raw_sha256=manifest_record.raw_sha256,
            contract_raw_sha256=records["delivery_contract"].raw_sha256,
        )
        report["browser_smoke"] = smoke

        worker_result = worker.get("result")
        if (
            set(worker)
            != {
                "task_id",
                "state",
                "completed_at",
                "exit_code",
                "summary",
                "result",
            }
            or worker.get("task_id") != control.submission_key
            or worker.get("state") != "completed"
            or type(worker.get("exit_code")) is not int
            or worker.get("exit_code") != 0
            or not str(worker.get("summary") or "").strip()
            or not isinstance(worker_result, dict)
            or worker_result.get("schema_version") != WORKER_RESULT_SCHEMA_VERSION
            or worker_result.get("task_id") != control.submission_key
        ):
            raise CanaryCollectionError("worker_result_invalid")
        attestation = worker_result.get("execution_attestation")
        if (
            not isinstance(attestation, dict)
            or set(attestation) != WORKER_ATTESTATION_KEYS
            or attestation.get("schema_version") != WORKER_ATTESTATION_SCHEMA_VERSION
            or attestation.get("available") is not True
            or attestation.get("executor_type") != "direct_cli"
            or attestation.get("agent_backend") != "none"
            or attestation.get("codex_backend_enabled") is not False
            or attestation.get("coding_agent_fallback_enabled") is not False
            or attestation.get("openclaw_invocation_count") != 0
            or attestation.get("codex_invocation_count") != 0
            or attestation.get("fallback_invocation_count") != 0
            or attestation.get("worker_tree_clean") is not True
            or attestation.get("worker_entrypoint_path") != FIXED_WORKER_ENTRYPOINT
            or attestation.get("task_id") != control.submission_key
        ):
            raise CanaryCollectionError("worker_attestation_invalid")
        run_id = str(attestation.get("run_id") or "").strip()
        worker_pid = attestation.get("worker_pid")
        dispatch_sha = _require_sha256(
            attestation.get("dispatch_receipt_sha256"),
            "worker_dispatch_receipt_invalid",
        )
        command = worker_result.get("command")
        expected_command = [
            FIXED_SERVICE_RELATIVE_ENTRYPOINT,
            "--task-id",
            control.submission_key,
            "--goal-path",
            f"/home/mini/.hermes/shared-state/tasks/{control.submission_key}/goal.md",
        ]
        dispatch_receipt = {
            "schema_version": WORKER_DISPATCH_RECEIPT_SCHEMA_VERSION,
            "task_id": control.submission_key,
            "run_id": run_id,
            "argv": expected_command,
            "cwd": DEFAULT_VM_REPO_ROOT,
            "dispatched_at": attestation.get("dispatched_at"),
            "process_started_at": attestation.get("process_started_at"),
            "worker_pid": worker_pid,
        }
        try:
            expected_contract_sha = _canonical_rca_contract_sha256(
                control.admission, request
            )
        except (TypeError, ValueError) as exc:
            raise CanaryCollectionError("fixed_service_contract_unverifiable") from exc
        expected_goal_sha = records["goal"].raw_sha256
        capacity_admission = _capacity_admission_projection(
            task_meta,
            records["task_meta"],
            submission_key=control.submission_key,
            goal_sha256=expected_goal_sha,
            contract_sha256=expected_contract_sha,
            hmac_key=self.config.prod_admission_hmac_key,
        )
        goal_path = (
            f"/home/mini/.hermes/shared-state/tasks/{control.submission_key}/goal.md"
        )
        service_mount = _validate_cifs_storage(
            service, code="fixed_service_storage_invalid"
        )
        if (
            not run_id
            or isinstance(worker_pid, bool)
            or not isinstance(worker_pid, int)
            or worker_pid < 2
            or dispatch_sha != _sha256_json(dispatch_receipt)
            or attestation.get("argv") != expected_command
            or attestation.get("cwd") != DEFAULT_VM_REPO_ROOT
            or command != expected_command
            or not WORKER_RESULT_REQUIRED_KEYS.issubset(worker_result)
            or set(worker_result)
            - WORKER_RESULT_REQUIRED_KEYS
            - WORKER_RESULT_OPTIONAL_KEYS
            or worker_result.get("run_id") != run_id
            or worker_result.get("repo_root") != DEFAULT_VM_REPO_ROOT
            or worker_result.get("canonical_task_dir")
            != f"/home/mini/.hermes/shared-state/tasks/{control.submission_key}"
            or worker_result.get("goal_path") != goal_path
            or worker_result.get("host_inbox_root")
            != "/home/mini/.hermes/shared-state/inbox"
            or worker_result.get("runner_log")
            != (
                f"{DEFAULT_VM_WORKER_ROOT}/tasks/{control.submission_key}/"
                "artifacts/runner.log"
            )
            or worker_result.get("artifact_root") != artifact_root.rstrip("/")
            or not isinstance(worker_result.get("artifacts"), list)
            or worker_result.get("result_mode") != "structured-result-artifact-only"
            or worker_result.get("report_contract") != "V4-V8"
            or worker_result.get("allowed_model_chain")
            != ["sub2api/gpt-5.5", "vtok/claude-opus-4-6"]
            or worker_result.get("execution_route") != "g1q3_rca_direct_cli"
            or worker_result.get("execution_attestation") != attestation
            or worker_result.get("rca_submission_key") != control.submission_key
            or worker_result.get("rca_business_key") != control.business_key
            or worker_result.get("rca_generation") != control.generation
            or worker_result.get("rca_contract_sha256") != expected_contract_sha
            or worker_result.get("rca_source_refs")
            != control.admission.get("source_refs")
            or set(service) != SERVICE_RESULT_KEYS
            or service.get("schema_version") != SERVICE_RESULT_SCHEMA_VERSION
            or service.get("task_id") != control.submission_key
            or service.get("status") != "completed"
            or service.get("success") is not True
            or service.get("goal_sha256") != expected_goal_sha
            or service.get("request_sha256") != request_sha
            or service.get("request_path") != request_record.path
            or service.get("output_dir") != artifact_root.rstrip("/")
            or service.get("artifact_cifs_root") != artifact_cifs_root
            or service.get("pipeline_result_path") != records["pipeline"].path
            or service.get("pipeline_status") != "report_generated_need_review"
            or service.get("pipeline_stage") != "s6_report"
            or service.get("blocker") is not None
            or service.get("worker_run_id") != run_id
            or service.get("worker_pid") != worker_pid
            or service.get("dispatch_receipt_sha256") != dispatch_sha
        ):
            raise CanaryCollectionError("fixed_service_binding_invalid")
        request_storage = service.get("request_storage")
        if (
            not isinstance(request_storage, dict)
            or set(request_storage)
            != {
                "schema_version",
                "path",
                "sha256",
                "bytes",
                *FIXED_CIFS_STORAGE,
            }
            or request_storage.get("schema_version")
            != STORAGE_FILE_RECEIPT_SCHEMA_VERSION
            or request_storage.get("path") != request_record.path
            or request_storage.get("sha256") != request_record.raw_sha256
            or request_storage.get("bytes") != request_record.size_bytes
        ):
            raise CanaryCollectionError("fixed_service_request_storage_invalid")
        request_mount = _validate_cifs_storage(
            request_storage,
            code="fixed_service_request_storage_invalid",
        )
        if not (cache_mount == service_mount == request_mount):
            raise CanaryCollectionError("cifs_mount_evidence_binding_mismatch")
        service_provenance = service.get("service_provenance")
        if (
            not isinstance(service_provenance, dict)
            or set(service_provenance)
            != {
                "schema_version",
                "available",
                "vm_source_commit",
                "vm_tree_clean",
                "service_entrypoint_path",
                "service_entrypoint_sha256",
            }
            or service_provenance.get("schema_version")
            != "g1q3_rca_service_provenance_v1"
            or service_provenance.get("available") is not True
            or service_provenance.get("vm_tree_clean") is not True
            or service_provenance.get("service_entrypoint_path")
            != FIXED_SERVICE_ENTRYPOINT
            or not re.fullmatch(
                r"[0-9a-f]{40}",
                str(service_provenance.get("vm_source_commit") or ""),
            )
            or not SHA256_RE.fullmatch(
                str(service_provenance.get("service_entrypoint_sha256") or "")
            )
        ):
            raise CanaryCollectionError("fixed_service_provenance_invalid")

        try:
            stage_lineages = validate_stage_lineage_chain(
                stage_lineages,
                artifact_root=artifact_root,
                expected_identity={
                    "task_id": control.submission_key,
                    "submission_key": control.submission_key,
                    "run_id": run_id,
                    "artifact_set_id": delivery.report["artifact_set_id"],
                    "request_sha256": request_sha,
                    "rca_contract_sha256": expected_contract_sha,
                },
                expected_finished_at={
                    short: str(downstream[short]["finished_at"])
                    for short in DOWNSTREAM_STAGE_NAMES
                },
                remote_stream_cache={
                    "kind": "derived_remote_stream_cache",
                    "path": cache_source.path,
                    "bytes": cache_source.size_bytes,
                    "sha256": cache_source.raw_sha256,
                },
                required_final_outputs=[
                    {
                        "kind": "delivery_manifest",
                        "path": manifest_record.path,
                        "bytes": manifest_record.size_bytes,
                        "sha256": manifest_record.raw_sha256,
                    },
                    {
                        "kind": "index_html",
                        "path": artifact_records["artifact_index_html"].path,
                        "bytes": artifact_records["artifact_index_html"].size_bytes,
                        "sha256": artifact_records["artifact_index_html"].raw_sha256,
                    },
                    {
                        "kind": "report_data",
                        "path": artifact_records["artifact_report_data"].path,
                        "bytes": artifact_records["artifact_report_data"].size_bytes,
                        "sha256": artifact_records["artifact_report_data"].raw_sha256,
                    },
                ],
            )
        except StageLineageError as exc:
            raise CanaryCollectionError(exc.code) from exc

        execution_request_sha = request_sha
        lifecycle_sha = _sha256_json(lifecycle)
        reserved_sha = _sha256_json(full_receipts["reserved"])
        pipeline_projection = {
            "status": "report_generated_need_review",
            "stage": "s6_report",
            "blocker": None,
            "remote_read_receipt": {
                "path": remote_record.path,
                "sha256": remote_record.canonical_sha256,
            },
            "remote_stream_cache": remote_read["derived_stream_cache"],
            "downstream_stage_receipts": {
                short: {
                    "status": "completed",
                    "finished_at": downstream[short]["finished_at"],
                    "artifact_receipt_path": downstream[short]["artifact_receipt_path"],
                    "artifact_receipt_sha256": downstream[short][
                        "artifact_receipt_sha256"
                    ],
                    "lineage": stage_lineages[short],
                }
                for short in DOWNSTREAM_STAGE_NAMES
            },
            "capacity_usage": {
                **pipeline_capacity,
                "sha256": meter_record.canonical_sha256,
            },
        }
        delivery_projection = {
            **delivery.delivery,
            "remote_receipt": {
                "remote_id": delivery.delivery["remote_receipt"]["remote_id"]
            },
        }
        vm = {
            "ok": True,
            "submission_key": control.submission_key,
            "task_id": control.submission_key,
            "terminal_state": "completed",
            "execution_request_sha256": execution_request_sha,
            "capacity_lifecycle_sha256": lifecycle_sha,
            "run_id": run_id,
            "dispatch_receipt_sha256": dispatch_sha,
            "capacity_admission": capacity_admission,
            "execution_plane": {
                "lane": "heavy",
                "resource_class": "rca_prod",
                "risk_class": "high",
                "executor_type": "direct_cli",
                "agent_backend": "none",
                "codex_backend_enabled": False,
                "coding_agent_fallback_enabled": False,
                "fixed_cli_entrypoint": FIXED_SERVICE_ENTRYPOINT,
                "vm_worker_commit": attestation.get("worker_source_commit"),
                "cwd": DEFAULT_VM_REPO_ROOT,
                "argv": expected_command,
                "agent_invocation_count": 0,
                "fallback_invocation_count": 0,
            },
            "execution_attestation": attestation,
            "worker_result": {
                "path": records["worker_result"].path,
                "sha256": records["worker_result"].canonical_sha256,
                "receipt": worker,
            },
            "service_result": {
                "path": records["service_result"].path,
                "sha256": records["service_result"].canonical_sha256,
                "receipt": service,
            },
        }
        collected_at = self.now()
        receipt = {
            "schema_version": CANARY_RECEIPT_SCHEMA_VERSION,
            "observed_at": _utc_iso(collected_at),
            "ok": True,
            "execution_origin": control.execution_origin,
            "observed_trigger_source": control.observed_trigger_source,
            "admission": control.admission,
            "execution_request": request,
            "remote_read": {
                "reader_fingerprint": reader_fingerprint,
                "receipt_sha256": remote_record.canonical_sha256,
                "receipt": remote_read,
            },
            "derived_capacity_lifecycle": lifecycle,
            "capacity_meter": {
                "path": meter_record.path,
                "sha256": meter_record.canonical_sha256,
                "receipt": meter,
            },
            "pipeline": pipeline_projection,
            "submission_count": 1,
            "submission_key": control.submission_key,
            "outbox": {
                "ok": True,
                "submission_key": control.submission_key,
                "origin_source_id": control.execution_origin["source_id"],
                "status": "completed",
                "execution_request_sha256": execution_request_sha,
                "reserved_receipt_sha256": reserved_sha,
            },
            "vm": vm,
            "report": report,
            "delivery": delivery_projection,
            "delivery_obligations": delivery.delivery_obligations,
        }
        expected_root_keys = {
            "schema_version",
            "observed_at",
            "ok",
            "execution_origin",
            "observed_trigger_source",
            "admission",
            "execution_request",
            "remote_read",
            "derived_capacity_lifecycle",
            "capacity_meter",
            "pipeline",
            "submission_count",
            "submission_key",
            "outbox",
            "vm",
            "report",
            "delivery",
            "delivery_obligations",
        }
        if set(receipt) != expected_root_keys:
            raise CanaryCollectionError("canary_receipt_projection_invalid")

        service_provenance = service["service_provenance"]
        try:
            from scripts.pnc_rca_release_gate import (
                EvidenceError,
                validate_canary_receipt,
            )
        except ImportError as exc:
            raise CanaryCollectionError("canary_receipt_gate_incompatible") from exc
        try:
            validate_canary_receipt(
                receipt,
                expected_execution_origin_id=control.execution_origin["source_id"],
                expected_execution_origin_kind=control.execution_origin["source_kind"],
                expected_observed_source_id=control.observed_trigger_source["source_id"],
                expected_observed_source_kind=control.observed_trigger_source[
                    "source_kind"
                ],
                expected_manual_chat_ids=self.config.manual_chat_ids,
                expected_request_sha256=request_sha,
                expected_admission=control.admission,
                expected_reader_fingerprint=reader_fingerprint,
                expected_requested_scope=_requested_scope_from_remote_read(remote_read),
                expected_vm_commit=str(service_provenance["vm_source_commit"]),
                expected_vm_worker_commit=str(attestation["worker_source_commit"]),
                expected_vm_service_entrypoint_sha256=str(
                    service_provenance["service_entrypoint_sha256"]
                ),
                expected_vm_worker_entrypoint_sha256=str(
                    attestation["worker_entrypoint_sha256"]
                ),
                now=collected_at.astimezone(timezone.utc),
                max_age_seconds=GATE_VALIDATION_MAX_AGE_SECONDS,
            )
        except (KeyError, EvidenceError) as exc:
            raise CanaryCollectionError("canary_receipt_gate_incompatible") from exc

        all_remote = {**records, **stage_records, **artifact_records}
        provenance = {
            "schema_version": SOURCE_MANIFEST_SCHEMA_VERSION,
            "collected_at": receipt["observed_at"],
            "read_only": True,
            "external_side_effects": False,
            "collector": {
                "path": str(Path(__file__).resolve()),
                "sha256": _sha256_bytes(Path(__file__).read_bytes()),
            },
            "execution_origin_sha256": _sha256_json(control.execution_origin),
            "observed_trigger_source_sha256": _sha256_json(
                control.observed_trigger_source
            ),
            "submission_key_sha256": _sha256_bytes(
                control.submission_key.encode("utf-8")
            ),
            "control_database": _database_provenance(
                self.config.control_db_path,
                control.snapshot_sha256,
                control_database.snapshot_file_info,
            ),
            "delivery_database": _database_provenance(
                self.config.delivery_db_path,
                delivery.snapshot_sha256,
                delivery_database.snapshot_file_info,
            ),
            "remote_transport": {
                "kind": "ssh-mini-agent",
                "operation": "bounded_read_only",
                "execution_request_abi": {
                    "canonicalization": (
                        "json_ensure_ascii_false_sort_keys_compact_v1"
                    ),
                    "sha256": request_sha,
                },
                "files": {
                    name: _source_provenance(source)
                    for name, source in sorted(all_remote.items())
                },
            },
            "local_machine_sources": {
                "remote_reader_health": _source_provenance(health_record),
                "browser_smoke": _source_provenance(smoke_record),
                "group_binding_authorizations": {
                    source_id: _source_provenance(source)
                    for source_id, source in sorted(
                        control.authorization_sources.items()
                    )
                },
            },
            "receipt_sha256": _sha256_json(receipt),
        }
        return CollectionResult(receipt=receipt, provenance=provenance)


def _source_provenance(source: SourceRecord) -> dict[str, Any]:
    result: dict[str, Any] = {
        "path": source.path,
        "size_bytes": source.size_bytes,
        "raw_sha256": source.raw_sha256,
    }
    if source.canonical_sha256:
        result["canonical_sha256"] = source.canonical_sha256
    return result


def _database_provenance(
    path: Path,
    snapshot_sha256: str,
    snapshot_file_info: os.stat_result | None,
) -> dict[str, Any]:
    info = _secure_regular_file(path, code="control_database_invalid")
    if snapshot_file_info is None or not _same_database_identity(
        snapshot_file_info, info
    ):
        raise CanaryCollectionError("control_database_changed_during_read")
    return {
        "path": str(path.expanduser().resolve(strict=True)),
        "device": info.st_dev,
        "inode": info.st_ino,
        "size_bytes": info.st_size,
        "mtime_ns": info.st_mtime_ns,
        "query_mode": "sqlite_mode_ro_query_only_transaction",
        "snapshot_sha256": snapshot_sha256,
    }


def _prepare_evidence_directory(directory: Path) -> Path:
    candidate = directory.expanduser().absolute()
    cursor = candidate
    missing: list[Path] = []
    while not cursor.exists():
        missing.append(cursor)
        parent = cursor.parent
        if parent == cursor:
            raise CanaryCollectionError("evidence_directory_invalid")
        cursor = parent
    try:
        existing = os.lstat(cursor)
        if stat.S_ISLNK(existing.st_mode) or not stat.S_ISDIR(existing.st_mode):
            raise CanaryCollectionError("evidence_directory_invalid")
        for path in reversed(missing):
            path.mkdir(mode=0o700)
        info = os.lstat(candidate)
        if (
            stat.S_ISLNK(info.st_mode)
            or not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) & 0o022
            or candidate.resolve(strict=True) != candidate
        ):
            raise CanaryCollectionError("evidence_directory_invalid")
    except CanaryCollectionError:
        raise
    except OSError as exc:
        raise CanaryCollectionError("evidence_directory_invalid") from exc
    return candidate


@dataclass(frozen=True)
class _EvidencePublication:
    evidence_role: str
    receipt_stem: str
    sources_stem: str
    receipt_raw: bytes
    sources_raw: bytes
    receipt_canonical_sha256: str
    commit_id: str
    files: dict[str, dict[str, Any]]

    @property
    def receipt_filename(self) -> str:
        return f"{self.receipt_stem}.{self.commit_id}.json"

    @property
    def sources_filename(self) -> str:
        return f"{self.sources_stem}.{self.commit_id}.json"

    @property
    def manifest_filename(self) -> str:
        return f"{self.receipt_stem}_commit.json"

    @property
    def lock_filename(self) -> str:
        return f".{self.manifest_filename}.lock"


def _evidence_publication(result: CollectionResult) -> _EvidencePublication:
    if result.evidence_role == "manual_success":
        if result.receipt.get("schema_version") != CANARY_RECEIPT_SCHEMA_VERSION:
            raise CanaryCollectionError("manual_success_evidence_invalid")
        evidence_role = "manual_success"
        receipt_stem = "manual_success_canary"
        sources_stem = "manual_success_canary_sources"
        expected_sources_schema = SOURCE_MANIFEST_SCHEMA_VERSION
    elif result.evidence_role != "primary":
        raise CanaryCollectionError("canary_evidence_role_invalid")
    elif (
        result.receipt.get("schema_version")
        == TERMINAL_CANARY_RECEIPT_SCHEMA_VERSION
    ):
        evidence_role = "manual_terminal_failure"
        receipt_stem = "manual_terminal_failure_canary"
        sources_stem = "manual_terminal_failure_canary_sources"
        expected_sources_schema = TERMINAL_CANARY_SOURCE_SCHEMA_VERSION
    else:
        if result.receipt.get("schema_version") != CANARY_RECEIPT_SCHEMA_VERSION:
            raise CanaryCollectionError("canary_evidence_schema_invalid")
        evidence_role = "primary"
        receipt_stem = "canary_receipt"
        sources_stem = "canary_receipt_sources"
        expected_sources_schema = SOURCE_MANIFEST_SCHEMA_VERSION

    receipt_schema = result.receipt.get("schema_version")
    sources_schema = result.provenance.get("schema_version")
    if not isinstance(receipt_schema, str) or not receipt_schema:
        raise CanaryCollectionError("canary_evidence_schema_invalid")
    if sources_schema != expected_sources_schema:
        raise CanaryCollectionError("canary_evidence_schema_invalid")
    receipt_canonical_sha256 = _sha256_json(result.receipt)
    if result.provenance.get("receipt_sha256") != receipt_canonical_sha256:
        raise CanaryCollectionError("canary_evidence_receipt_binding_invalid")
    try:
        receipt_raw = (_canonical_json(result.receipt) + "\n").encode("ascii")
        sources_raw = (_canonical_json(result.provenance) + "\n").encode("ascii")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise CanaryCollectionError("canary_evidence_json_invalid") from exc
    if (
        not receipt_raw
        or len(receipt_raw) > MAX_JSON_BYTES
        or not sources_raw
        or len(sources_raw) > MAX_JSON_BYTES
    ):
        raise CanaryCollectionError("canary_evidence_json_invalid")
    files = {
        "receipt": {
            "schema_version": receipt_schema,
            "size_bytes": len(receipt_raw),
            "raw_sha256": _sha256_bytes(receipt_raw),
        },
        "sources": {
            "schema_version": sources_schema,
            "size_bytes": len(sources_raw),
            "raw_sha256": _sha256_bytes(sources_raw),
        },
    }
    commit_id = _sha256_json(
        {
            "schema_version": CANARY_EVIDENCE_COMMIT_SCHEMA_VERSION,
            "evidence_role": evidence_role,
            "receipt_canonical_sha256": receipt_canonical_sha256,
            "files": files,
        }
    )
    return _EvidencePublication(
        evidence_role=evidence_role,
        receipt_stem=receipt_stem,
        sources_stem=sources_stem,
        receipt_raw=receipt_raw,
        sources_raw=sources_raw,
        receipt_canonical_sha256=receipt_canonical_sha256,
        commit_id=commit_id,
        files=files,
    )


def _same_evidence_stat(left: os.stat_result, right: os.stat_result) -> bool:
    return (
        left.st_dev == right.st_dev
        and left.st_ino == right.st_ino
        and left.st_mode == right.st_mode
        and left.st_uid == right.st_uid
        and left.st_nlink == right.st_nlink
        and left.st_size == right.st_size
        and left.st_mtime_ns == right.st_mtime_ns
        and left.st_ctime_ns == right.st_ctime_ns
    )


def _private_evidence_file_valid(
    info: os.stat_result, *, allow_empty: bool = False
) -> bool:
    return (
        stat.S_ISREG(info.st_mode)
        and info.st_uid == os.getuid()
        and info.st_nlink == 1
        and stat.S_IMODE(info.st_mode) == 0o600
        and (0 <= info.st_size if allow_empty else 0 < info.st_size)
        and info.st_size <= MAX_JSON_BYTES
    )


def _secure_evidence_open_flags(*, directory: bool = False) -> int:
    nofollow = getattr(os, "O_NOFOLLOW", None)
    directory_flag = getattr(os, "O_DIRECTORY", None) if directory else 0
    if nofollow is None or (directory and directory_flag is None):
        raise CanaryCollectionError("canary_evidence_secure_open_unavailable")
    return nofollow | int(directory_flag or 0) | getattr(os, "O_CLOEXEC", 0)


def _open_evidence_directory(directory: Path) -> int:
    descriptor = -1
    try:
        initial = os.lstat(directory)
        if (
            not stat.S_ISDIR(initial.st_mode)
            or stat.S_ISLNK(initial.st_mode)
            or initial.st_uid != os.getuid()
            or stat.S_IMODE(initial.st_mode) & 0o022
        ):
            raise CanaryCollectionError("evidence_directory_invalid")
        descriptor = os.open(
            directory,
            os.O_RDONLY | _secure_evidence_open_flags(directory=True),
        )
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(opened.st_mode)
            or opened.st_dev != initial.st_dev
            or opened.st_ino != initial.st_ino
            or opened.st_uid != os.getuid()
            or stat.S_IMODE(opened.st_mode) & 0o022
        ):
            raise CanaryCollectionError("evidence_directory_invalid")
        return descriptor
    except CanaryCollectionError:
        if descriptor >= 0:
            os.close(descriptor)
        raise
    except OSError as exc:
        if descriptor >= 0:
            os.close(descriptor)
        raise CanaryCollectionError("evidence_directory_invalid") from exc


def _read_evidence_entry(
    directory_fd: int,
    filename: str,
    *,
    missing_ok: bool = False,
) -> tuple[bytes, os.stat_result] | None:
    if Path(filename).name != filename or not filename:
        raise CanaryCollectionError("canary_evidence_target_invalid")
    descriptor = -1
    try:
        try:
            initial = os.stat(filename, dir_fd=directory_fd, follow_symlinks=False)
        except FileNotFoundError:
            if missing_ok:
                return None
            raise CanaryCollectionError("canary_evidence_target_invalid")
        if not _private_evidence_file_valid(initial):
            raise CanaryCollectionError("canary_evidence_target_invalid")
        descriptor = os.open(
            filename,
            os.O_RDONLY | _secure_evidence_open_flags(),
            dir_fd=directory_fd,
        )
        opened = os.fstat(descriptor)
        if (
            not _private_evidence_file_valid(opened)
            or not _same_evidence_stat(initial, opened)
        ):
            raise CanaryCollectionError("canary_evidence_target_invalid")
        remaining = opened.st_size
        chunks: list[bytes] = []
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise CanaryCollectionError("canary_evidence_target_invalid")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise CanaryCollectionError("canary_evidence_target_invalid")
        final = os.fstat(descriptor)
        current = os.stat(filename, dir_fd=directory_fd, follow_symlinks=False)
        if (
            not _same_evidence_stat(opened, final)
            or not _same_evidence_stat(final, current)
        ):
            raise CanaryCollectionError("canary_evidence_target_invalid")
        return b"".join(chunks), final
    except CanaryCollectionError:
        raise
    except OSError as exc:
        raise CanaryCollectionError("canary_evidence_target_invalid") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _unlink_created_entry(
    directory_fd: int, filename: str, created: os.stat_result | None
) -> None:
    if created is None:
        return
    try:
        current = os.stat(filename, dir_fd=directory_fd, follow_symlinks=False)
        if current.st_dev == created.st_dev and current.st_ino == created.st_ino:
            os.unlink(filename, dir_fd=directory_fd)
    except FileNotFoundError:
        pass
    except OSError:
        pass


def _write_all(descriptor: int, raw: bytes) -> None:
    offset = 0
    while offset < len(raw):
        written = os.write(descriptor, raw[offset:])
        if written <= 0:
            raise OSError("short evidence write")
        offset += written


def _publish_immutable_generation(
    directory_fd: int, filename: str, raw: bytes
) -> None:
    descriptor = -1
    created: os.stat_result | None = None
    completed = False
    try:
        try:
            descriptor = os.open(
                filename,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | _secure_evidence_open_flags(),
                0o600,
                dir_fd=directory_fd,
            )
        except FileExistsError:
            existing = _read_evidence_entry(directory_fd, filename)
            if existing is None or existing[0] != raw:
                raise CanaryCollectionError("canary_evidence_generation_conflict")
            return
        os.fchmod(descriptor, 0o600)
        created = os.fstat(descriptor)
        if not _private_evidence_file_valid(created, allow_empty=True):
            raise CanaryCollectionError("canary_evidence_target_invalid")
        _write_all(descriptor, raw)
        os.fsync(descriptor)
        final = os.fstat(descriptor)
        if (
            not _private_evidence_file_valid(final)
            or final.st_size != len(raw)
            or final.st_dev != created.st_dev
            or final.st_ino != created.st_ino
        ):
            raise CanaryCollectionError("canary_evidence_target_invalid")
        completed = True
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if not completed:
            _unlink_created_entry(directory_fd, filename, created)
    published = _read_evidence_entry(directory_fd, filename)
    if published is None or published[0] != raw:
        raise CanaryCollectionError("canary_evidence_generation_conflict")


def _load_evidence_publish_lock(
    directory_fd: int,
    publication: _EvidencePublication,
) -> dict[str, Any] | None:
    entry = _read_evidence_entry(
        directory_fd, publication.lock_filename, missing_ok=True
    )
    if entry is None:
        return None
    raw, _info = entry
    body = _decode_json(raw, code="canary_evidence_publish_lock_invalid")
    if (
        raw != (_canonical_json(body) + "\n").encode("ascii")
        or set(body)
        != {
            "schema_version",
            "evidence_role",
            "commit_id",
            "created_at",
            "pid",
        }
        or body.get("schema_version")
        != CANARY_EVIDENCE_PUBLISH_LOCK_SCHEMA_VERSION
        or body.get("evidence_role") != publication.evidence_role
        or not isinstance(body.get("commit_id"), str)
        or SHA256_RE.fullmatch(body["commit_id"]) is None
        or not isinstance(body.get("pid"), int)
        or isinstance(body["pid"], bool)
        or body["pid"] <= 0
    ):
        raise CanaryCollectionError("canary_evidence_publish_lock_invalid")
    created_at = _parse_timestamp(
        body.get("created_at"), code="canary_evidence_publish_lock_invalid"
    )
    age_seconds = (datetime.now(timezone.utc) - created_at).total_seconds()
    if age_seconds < -5.0 or age_seconds > CANARY_EVIDENCE_LOCK_MAX_AGE_SECONDS:
        raise CanaryCollectionError("canary_evidence_publish_lock_stale")
    return body


def _try_create_evidence_publish_lock(
    directory_fd: int,
    publication: _EvidencePublication,
) -> os.stat_result | None:
    raw = (
        _canonical_json(
            {
                "schema_version": CANARY_EVIDENCE_PUBLISH_LOCK_SCHEMA_VERSION,
                "evidence_role": publication.evidence_role,
                "commit_id": publication.commit_id,
                "created_at": _utc_iso(),
                "pid": os.getpid(),
            }
        )
        + "\n"
    ).encode("ascii")
    descriptor = -1
    created: os.stat_result | None = None
    completed = False
    try:
        try:
            descriptor = os.open(
                publication.lock_filename,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | _secure_evidence_open_flags(),
                0o600,
                dir_fd=directory_fd,
            )
        except FileExistsError:
            return None
        os.fchmod(descriptor, 0o600)
        created = os.fstat(descriptor)
        if not _private_evidence_file_valid(created, allow_empty=True):
            raise CanaryCollectionError("canary_evidence_publish_lock_invalid")
        _write_all(descriptor, raw)
        os.fsync(descriptor)
        final = os.fstat(descriptor)
        if (
            not _private_evidence_file_valid(final)
            or final.st_size != len(raw)
            or final.st_dev != created.st_dev
            or final.st_ino != created.st_ino
        ):
            raise CanaryCollectionError("canary_evidence_publish_lock_invalid")
        os.fsync(directory_fd)
        completed = True
        return final
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if not completed:
            _unlink_created_entry(
                directory_fd, publication.lock_filename, created
            )


def _acquire_evidence_publish_lock(
    directory_fd: int,
    publication: _EvidencePublication,
) -> os.stat_result | None:
    deadline = time.monotonic() + CANARY_EVIDENCE_LOCK_WAIT_SECONDS
    while True:
        created = _try_create_evidence_publish_lock(directory_fd, publication)
        if created is not None:
            return created
        try:
            observed = _load_evidence_publish_lock(directory_fd, publication)
        except CanaryCollectionError as exc:
            if exc.code not in {
                "canary_evidence_target_invalid",
                "canary_evidence_publish_lock_invalid",
            }:
                raise
            if time.monotonic() >= deadline:
                raise CanaryCollectionError(
                    "canary_evidence_publish_lock_invalid"
                ) from exc
            time.sleep(CANARY_EVIDENCE_LOCK_POLL_SECONDS)
            continue
        if observed is None:
            committed = _load_evidence_manifest(directory_fd, publication)
            if (
                committed is not None
                and committed["commit_id"] == publication.commit_id
            ):
                return None
            raise CanaryCollectionError("canary_evidence_publish_busy")
        if observed["commit_id"] != publication.commit_id:
            raise CanaryCollectionError("canary_evidence_publish_busy")
        if time.monotonic() >= deadline:
            raise CanaryCollectionError("canary_evidence_publish_lock_timeout")
        time.sleep(CANARY_EVIDENCE_LOCK_POLL_SECONDS)


def _release_evidence_publish_lock(
    directory_fd: int,
    publication: _EvidencePublication,
    acquired: os.stat_result,
) -> None:
    try:
        current = os.stat(
            publication.lock_filename,
            dir_fd=directory_fd,
            follow_symlinks=False,
        )
        if (
            not _private_evidence_file_valid(current)
            or current.st_dev != acquired.st_dev
            or current.st_ino != acquired.st_ino
        ):
            raise CanaryCollectionError(
                "canary_evidence_publish_lock_release_failed"
            )
        os.unlink(publication.lock_filename, dir_fd=directory_fd)
        os.fsync(directory_fd)
    except CanaryCollectionError:
        raise
    except OSError as exc:
        raise CanaryCollectionError(
            "canary_evidence_publish_lock_release_failed"
        ) from exc


def _evidence_commit_material(
    evidence_role: str,
    receipt_canonical_sha256: str,
    files: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": CANARY_EVIDENCE_COMMIT_SCHEMA_VERSION,
        "evidence_role": evidence_role,
        "receipt_canonical_sha256": receipt_canonical_sha256,
        "files": {
            kind: {
                "schema_version": files[kind]["schema_version"],
                "size_bytes": files[kind]["size_bytes"],
                "raw_sha256": files[kind]["raw_sha256"],
            }
            for kind in ("receipt", "sources")
        },
    }


def _load_evidence_manifest(
    directory_fd: int,
    publication: _EvidencePublication,
) -> dict[str, Any] | None:
    entry = _read_evidence_entry(
        directory_fd, publication.manifest_filename, missing_ok=True
    )
    if entry is None:
        return None
    raw, _info = entry
    body = _decode_json(raw, code="canary_evidence_manifest_invalid")
    if raw != (_canonical_json(body) + "\n").encode("ascii"):
        raise CanaryCollectionError("canary_evidence_manifest_invalid")
    if set(body) != {
        "schema_version",
        "evidence_role",
        "commit_id",
        "published_at",
        "receipt_canonical_sha256",
        "files",
    }:
        raise CanaryCollectionError("canary_evidence_manifest_invalid")
    if (
        body.get("schema_version") != CANARY_EVIDENCE_COMMIT_SCHEMA_VERSION
        or body.get("evidence_role") != publication.evidence_role
        or not isinstance(body.get("commit_id"), str)
        or SHA256_RE.fullmatch(body["commit_id"]) is None
        or not isinstance(body.get("receipt_canonical_sha256"), str)
        or SHA256_RE.fullmatch(body["receipt_canonical_sha256"]) is None
    ):
        raise CanaryCollectionError("canary_evidence_manifest_invalid")
    _parse_timestamp(
        body.get("published_at"), code="canary_evidence_manifest_invalid"
    )
    files = body.get("files")
    if not isinstance(files, dict) or set(files) != {"receipt", "sources"}:
        raise CanaryCollectionError("canary_evidence_manifest_invalid")
    for kind in ("receipt", "sources"):
        projection = files.get(kind)
        if (
            not isinstance(projection, dict)
            or set(projection)
            != {"filename", "schema_version", "size_bytes", "raw_sha256"}
            or not isinstance(projection.get("filename"), str)
            or not isinstance(projection.get("schema_version"), str)
            or not projection["schema_version"]
            or not isinstance(projection.get("size_bytes"), int)
            or isinstance(projection["size_bytes"], bool)
            or not 0 < projection["size_bytes"] <= MAX_JSON_BYTES
            or not isinstance(projection.get("raw_sha256"), str)
            or SHA256_RE.fullmatch(projection["raw_sha256"]) is None
        ):
            raise CanaryCollectionError("canary_evidence_manifest_invalid")
    expected_names = {
        "receipt": f"{publication.receipt_stem}.{body['commit_id']}.json",
        "sources": f"{publication.sources_stem}.{body['commit_id']}.json",
    }
    if any(files[kind]["filename"] != expected_names[kind] for kind in expected_names):
        raise CanaryCollectionError("canary_evidence_manifest_invalid")
    material = _evidence_commit_material(
        body["evidence_role"], body["receipt_canonical_sha256"], files
    )
    if _sha256_json(material) != body["commit_id"]:
        raise CanaryCollectionError("canary_evidence_manifest_invalid")

    loaded: dict[str, dict[str, Any]] = {}
    for kind in ("receipt", "sources"):
        generation = _read_evidence_entry(directory_fd, files[kind]["filename"])
        if generation is None:
            raise CanaryCollectionError("canary_evidence_manifest_invalid")
        generation_raw = generation[0]
        if (
            len(generation_raw) != files[kind]["size_bytes"]
            or _sha256_bytes(generation_raw) != files[kind]["raw_sha256"]
        ):
            raise CanaryCollectionError("canary_evidence_manifest_invalid")
        generation_body = _decode_json(
            generation_raw, code="canary_evidence_manifest_invalid"
        )
        if (
            generation_raw != (_canonical_json(generation_body) + "\n").encode("ascii")
            or generation_body.get("schema_version") != files[kind]["schema_version"]
        ):
            raise CanaryCollectionError("canary_evidence_manifest_invalid")
        loaded[kind] = generation_body
    receipt_sha256 = _sha256_json(loaded["receipt"])
    if (
        receipt_sha256 != body["receipt_canonical_sha256"]
        or loaded["sources"].get("receipt_sha256") != receipt_sha256
    ):
        raise CanaryCollectionError("canary_evidence_manifest_invalid")
    return body


def _stage_manifest(
    directory_fd: int, filename: str, body: Mapping[str, Any]
) -> str:
    raw = (_canonical_json(dict(body)) + "\n").encode("ascii")
    if not raw or len(raw) > MAX_JSON_BYTES:
        raise CanaryCollectionError("canary_evidence_manifest_invalid")
    for _attempt in range(32):
        temporary = f".{filename}.{secrets.token_hex(16)}.tmp"
        descriptor = -1
        try:
            descriptor = os.open(
                temporary,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | _secure_evidence_open_flags(),
                0o600,
                dir_fd=directory_fd,
            )
        except FileExistsError:
            continue
        try:
            os.fchmod(descriptor, 0o600)
            _write_all(descriptor, raw)
            os.fsync(descriptor)
            info = os.fstat(descriptor)
            if not _private_evidence_file_valid(info) or info.st_size != len(raw):
                raise CanaryCollectionError("canary_evidence_target_invalid")
            return temporary
        except Exception:
            try:
                os.unlink(temporary, dir_fd=directory_fd)
            except OSError:
                pass
            raise
        finally:
            if descriptor >= 0:
                os.close(descriptor)
    raise CanaryCollectionError("canary_evidence_write_failed")


def write_collection(result: CollectionResult, evidence_dir: Path) -> tuple[Path, Path]:
    publication = _evidence_publication(result)
    directory = _prepare_evidence_directory(evidence_dir)
    directory_fd = _open_evidence_directory(directory)
    temporary_manifest: str | None = None
    acquired_lock: os.stat_result | None = None
    try:
        acquired_lock = _acquire_evidence_publish_lock(directory_fd, publication)
        if acquired_lock is None:
            return (
                directory / publication.receipt_filename,
                directory / publication.sources_filename,
            )
        previous_manifest = _load_evidence_manifest(directory_fd, publication)
        if (
            previous_manifest is not None
            and previous_manifest["commit_id"] == publication.commit_id
        ):
            return (
                directory / publication.receipt_filename,
                directory / publication.sources_filename,
            )
        _publish_immutable_generation(
            directory_fd, publication.receipt_filename, publication.receipt_raw
        )
        _publish_immutable_generation(
            directory_fd, publication.sources_filename, publication.sources_raw
        )
        os.fsync(directory_fd)

        current_manifest = _load_evidence_manifest(directory_fd, publication)
        if current_manifest != previous_manifest:
            if (
                current_manifest is not None
                and current_manifest["commit_id"] == publication.commit_id
            ):
                return (
                    directory / publication.receipt_filename,
                    directory / publication.sources_filename,
                )
            raise CanaryCollectionError("canary_evidence_manifest_changed")
        manifest_files = {
            "receipt": {
                "filename": publication.receipt_filename,
                **publication.files["receipt"],
            },
            "sources": {
                "filename": publication.sources_filename,
                **publication.files["sources"],
            },
        }
        manifest = {
            "schema_version": CANARY_EVIDENCE_COMMIT_SCHEMA_VERSION,
            "evidence_role": publication.evidence_role,
            "commit_id": publication.commit_id,
            "published_at": _utc_iso(),
            "receipt_canonical_sha256": publication.receipt_canonical_sha256,
            "files": manifest_files,
        }
        temporary_manifest = _stage_manifest(
            directory_fd, publication.manifest_filename, manifest
        )
        os.replace(
            temporary_manifest,
            publication.manifest_filename,
            src_dir_fd=directory_fd,
            dst_dir_fd=directory_fd,
        )
        temporary_manifest = None
        os.fsync(directory_fd)
        published_manifest = _load_evidence_manifest(directory_fd, publication)
        if published_manifest != manifest:
            raise CanaryCollectionError("canary_evidence_manifest_invalid")
    except CanaryCollectionError:
        raise
    except OSError as exc:
        raise CanaryCollectionError("canary_evidence_write_failed") from exc
    finally:
        if temporary_manifest is not None:
            try:
                os.unlink(temporary_manifest, dir_fd=directory_fd)
            except OSError:
                pass
        try:
            if acquired_lock is not None:
                _release_evidence_publish_lock(
                    directory_fd, publication, acquired_lock
                )
        finally:
            os.close(directory_fd)
    return (
        directory / publication.receipt_filename,
        directory / publication.sources_filename,
    )


def _collector_config_from_args(args: argparse.Namespace) -> CollectorConfig:
    if args.env_file is None:
        defaults: dict[str, Any] = {
            "control_db": DEFAULT_CONTROL_DB,
            "delivery_db": DEFAULT_CONTROL_DB,
            "delivery_db_explicit": False,
            "evidence_dir": DEFAULT_EVIDENCE_DIR,
            "group_binding_receipt_dir": DEFAULT_GROUP_BINDING_RECEIPT_DIR,
            "manual_chat_ids": os.environ.get(MANUAL_CHAT_IDS_ENV, ""),
            "prod_admission_hmac_key": os.environ.get(
                prod_admission.HMAC_ENV, ""
            ),
        }
    else:
        defaults = _collector_env_defaults(
            load_canary_collector_environment(args.env_file)
        )
    control_db = (
        args.control_db
        if args.control_db is not None
        else defaults["control_db"]
    )
    if args.delivery_db is not None:
        delivery_db = args.delivery_db
    elif defaults["delivery_db_explicit"]:
        delivery_db = defaults["delivery_db"]
    else:
        delivery_db = control_db
    evidence_dir = (
        args.evidence_dir
        if args.evidence_dir is not None
        else defaults["evidence_dir"]
    )
    receipt_dir = (
        args.group_binding_receipt_dir
        if args.group_binding_receipt_dir is not None
        else defaults["group_binding_receipt_dir"]
    )
    manual_chat_ids = (
        args.manual_chat_ids
        if args.manual_chat_ids is not None
        else defaults["manual_chat_ids"]
    )
    return CollectorConfig(
        control_db_path=Path(control_db).expanduser(),
        delivery_db_path=Path(delivery_db).expanduser(),
        evidence_dir=Path(evidence_dir).expanduser(),
        group_binding_receipt_dir=Path(receipt_dir).expanduser(),
        manual_chat_ids=_manual_chat_ids(manual_chat_ids),
        ssh_mini_agent=args.ssh_mini_agent,
        prod_admission_hmac_key=defaults["prod_admission_hmac_key"],
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-id", required=True)
    parser.add_argument(
        "--env-file",
        type=Path,
        help="owner-only literal dotenv; omitted keeps collector defaults",
    )
    parser.add_argument("--control-db", type=Path)
    parser.add_argument("--delivery-db", type=Path)
    parser.add_argument("--evidence-dir", type=Path)
    parser.add_argument(
        "--group-binding-receipt-dir",
        type=Path,
    )
    parser.add_argument("--manual-chat-ids")
    parser.add_argument("--ssh-mini-agent", type=Path, default=DEFAULT_SSH_MINI_AGENT)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run", action="store_true", help="collect and validate only (default)"
    )
    mode.add_argument(
        "--write", action="store_true", help="atomically write local evidence files"
    )
    canary_kind = parser.add_mutually_exclusive_group()
    canary_kind.add_argument(
        "--terminal-failure",
        action="store_true",
        help="collect a source-bound manual terminal-delivery canary",
    )
    canary_kind.add_argument(
        "--manual-success",
        action="store_true",
        help="collect a production-group manual-origin success canary",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    try:
        config = _collector_config_from_args(args)
        collector = CanaryReceiptCollector(config)
        if args.terminal_failure:
            result = collector.collect_terminal_failure(args.source_id)
        elif args.manual_success:
            result = collector.collect_manual_success(args.source_id)
        else:
            result = collector.collect(args.source_id)
        publication = _evidence_publication(result)
        written: list[str] = []
        if args.write:
            paths = write_collection(result, config.evidence_dir)
            written = [
                *(path.name for path in paths),
                publication.manifest_filename,
            ]
        summary = {
            "ok": True,
            "mode": "write" if args.write else "dry_run",
            "read_only_collection": True,
            "external_side_effects": False,
            "evidence_role": publication.evidence_role,
            "evidence_commit_id": publication.commit_id,
            "evidence_manifest": publication.manifest_filename,
            "observed_source_id_sha256": _sha256_bytes(
                args.source_id.encode("utf-8")
            ),
            "submission_key_sha256": result.provenance["submission_key_sha256"],
            "receipt_sha256": result.provenance["receipt_sha256"],
            "written_files": written,
        }
        print(json.dumps(summary, ensure_ascii=True, sort_keys=True))
        return 0
    except (CanaryCollectionError, ValueError) as exc:
        error = (
            exc.code
            if isinstance(exc, CanaryCollectionError)
            else str(exc or "collector_configuration_invalid")[:240]
        )
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": error,
                    "external_side_effects": False,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
