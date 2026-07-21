#!/usr/bin/env python3
"""Audited adapter for the exact-issue and first-natural RCA production closeout."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import sys
from typing import Any, Callable, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import pnc_rca_prod_e2e_release as release
from scripts.pnc_rca_delivery_dispatcher import MeegleIssueCommentAdapter


EXACT_REQUEST_SCHEMA_VERSION = "pnc_rca_exact_kafka_recovery_request_v1"
EXACT_RECEIPT_SCHEMA_VERSION = "pnc_rca_exact_kafka_recovery_receipt_v1"
READBACK_EXPECTATION_SCHEMA_VERSION = "pnc_rca_official_readback_expectation_v1"
READBACK_RECEIPT_SCHEMA_VERSION = "pnc_rca_official_full_readback_v1"
NATURAL_SELECTOR_SCHEMA_VERSION = "pnc_rca_first_natural_kafka_selector_v1"
NATURAL_GATE_SCHEMA_VERSION = "pnc_rca_natural_kafka_canary_gate_v1"
NATURAL_RECEIPT_SCHEMA_VERSION = "pnc_rca_natural_kafka_canary_receipt_v1"
MAX_REQUEST_VALIDITY = timedelta(minutes=15)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_NONCE_RE = re.compile(r"^[A-Za-z0-9_-]{16,128}$")


class ExecutionAdapterError(RuntimeError):
    def __init__(self, code: str):
        self.code = str(code or "prod_execution_adapter_invalid")[:160]
        super().__init__(self.code)


def _now(value: datetime | None = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise ExecutionAdapterError("prod_execution_adapter_time_invalid")
    return current.astimezone(timezone.utc)


def _timestamp(value: Any, *, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError as exc:
        raise ExecutionAdapterError(f"prod_execution_adapter_{field}_invalid") from exc
    if parsed.tzinfo is None:
        raise ExecutionAdapterError(f"prod_execution_adapter_{field}_invalid")
    return parsed.astimezone(timezone.utc)


def _sha256(value: Any, *, field: str) -> str:
    text = str(value or "").strip().lower()
    if _SHA256_RE.fullmatch(text) is None:
        raise ExecutionAdapterError(f"prod_execution_adapter_{field}_invalid")
    return text


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _hash_text(value: str) -> dict[str, Any]:
    raw = value.encode("utf-8")
    return {"sha256": hashlib.sha256(raw).hexdigest(), "utf8_bytes": len(raw)}


def _validate_full_readback_receipt(
    value: Mapping[str, Any],
    *,
    expected_release_id: str,
) -> datetime:
    expected_fields = {
        "schema_version",
        "release_id",
        "adapter",
        "source",
        "scope",
        "observed_at",
        "fields",
        "comment_id",
        "comment_content_sha256",
        "comment_content_utf8_bytes",
        "marker_sha256",
        "marker_match_count",
        "pages_read",
        "comments",
        "terminal_receipt_sha256",
        "full_bodies_persisted",
    }
    fields = value.get("fields")
    comments = value.get("comments")
    comment_id = str(value.get("comment_id") or "").strip()
    comment_sha256 = _sha256(
        value.get("comment_content_sha256"), field="readback_comment_sha256"
    )
    comment_bytes = value.get("comment_content_utf8_bytes")
    designated_comments = (
        [
            item
            for item in comments
            if isinstance(item, Mapping) and item.get("comment_id") == comment_id
        ]
        if isinstance(comments, list)
        else []
    )
    if (
        set(value) != expected_fields
        or value.get("schema_version") != READBACK_RECEIPT_SCHEMA_VERSION
        or value.get("release_id") != expected_release_id
        or value.get("adapter") != "MeegleIssueCommentAdapter.get_fields_and_comments"
        or value.get("source") != "official_meegle_api"
        or value.get("scope")
        != {
            "project_key": release.TARGET_PROJECT_KEY,
            "work_item_id": release.TARGET_WORK_ITEM_ID,
        }
        or not isinstance(fields, Mapping)
        or set(fields) != set(release.TARGET_FIELD_KEYS)
        or any(
            not isinstance(fields.get(key), Mapping)
            or set(fields[key]) != {"sha256", "utf8_bytes"}
            or _sha256(fields[key].get("sha256"), field=f"readback_{key}_sha256")
            != fields[key].get("sha256")
            or isinstance(fields[key].get("utf8_bytes"), bool)
            or not isinstance(fields[key].get("utf8_bytes"), int)
            or fields[key].get("utf8_bytes", -1) < 0
            for key in release.TARGET_FIELD_KEYS
        )
        or not comment_id
        or isinstance(comment_bytes, bool)
        or not isinstance(comment_bytes, int)
        or comment_bytes < 1
        or _sha256(value.get("marker_sha256"), field="readback_marker_sha256")
        != value.get("marker_sha256")
        or value.get("marker_match_count") != 1
        or isinstance(value.get("pages_read"), bool)
        or not isinstance(value.get("pages_read"), int)
        or value.get("pages_read", 0) < 1
        or not isinstance(comments, list)
        or not comments
        or len(comments)
        != len({
            str(item.get("comment_id") or "")
            for item in comments
            if isinstance(item, Mapping)
        })
        or any(
            not isinstance(item, Mapping)
            or set(item)
            != {
                "comment_id",
                "content_sha256",
                "content_utf8_bytes",
                "marker_match_count",
            }
            or not str(item.get("comment_id") or "").strip()
            or _sha256(item.get("content_sha256"), field="readback_body_sha256")
            != item.get("content_sha256")
            or isinstance(item.get("content_utf8_bytes"), bool)
            or not isinstance(item.get("content_utf8_bytes"), int)
            or item.get("content_utf8_bytes", -1) < 0
            or isinstance(item.get("marker_match_count"), bool)
            or not isinstance(item.get("marker_match_count"), int)
            or item.get("marker_match_count", -1) < 0
            for item in comments
        )
        or sum(int(item["marker_match_count"]) for item in comments) != 1
        or len(designated_comments) != 1
        or designated_comments[0].get("content_sha256") != comment_sha256
        or designated_comments[0].get("content_utf8_bytes") != comment_bytes
        or _sha256(
            value.get("terminal_receipt_sha256"),
            field="readback_terminal_receipt_sha256",
        )
        != value.get("terminal_receipt_sha256")
        or value.get("full_bodies_persisted") is not False
    ):
        raise ExecutionAdapterError("prod_execution_adapter_target_readback_invalid")
    return _timestamp(value.get("observed_at"), field="target_readback_at")


def _validate_final_authority(
    *,
    request_path: Path,
    final_validation_path: Path,
    now: datetime,
) -> tuple[Mapping[str, Any], release.OwnedJson, Mapping[str, Any]]:
    request_owned = release._read_owned_json(
        request_path, artifact="execution_adapter_request"
    )
    request = release._validate_request(request_owned)
    final_owned = release._read_owned_json(
        final_validation_path, artifact="execution_adapter_final_validation"
    )
    verified = release._validate_final_validation_receipt(
        final_owned,
        request=request,
        execution_started_at=now,
        now=now,
    )
    scope = verified.get("authorized_scope")
    exact = scope.get("target_exact_recovery") if isinstance(scope, Mapping) else None
    natural = scope.get("post_cutover_canary") if isinstance(scope, Mapping) else None
    if (
        verified.get("production_ready") is not True
        or verified.get("blockers") != []
        or not isinstance(exact, Mapping)
        or exact.get("resident_consumer_only") is not True
        or exact.get("activation_slot_kind") != "kafka_success"
        or exact.get("commit_called") is not False
        or not isinstance(natural, Mapping)
        or natural.get("resident_natural_gate_required") is not True
        or natural.get("max_poll_records_during_gate") != 1
        or natural.get("pause_after_first_accepted") is not True
        or natural.get("failure_auto_stop") is not True
        or natural.get("activation_slot_kind") != ""
        or natural.get("activation_reason") != "activation_steady_active"
    ):
        raise ExecutionAdapterError("prod_execution_adapter_authorized_scope_invalid")
    return request, final_owned, verified


def build_exact_request(
    *,
    approval_request_path: Path,
    final_validation_path: Path,
    output_path: Path,
    nonce: str,
    now: datetime | None = None,
) -> Mapping[str, Any]:
    current = _now(now)
    if _NONCE_RE.fullmatch(str(nonce or "")) is None:
        raise ExecutionAdapterError("prod_execution_adapter_nonce_invalid")
    request, final_owned, verified = _validate_final_authority(
        request_path=approval_request_path,
        final_validation_path=final_validation_path,
        now=current,
    )
    execute_before = _timestamp(verified["execute_before"], field="execute_before")
    expires_at = min(execute_before, current + MAX_REQUEST_VALIDITY)
    if expires_at <= current:
        raise ExecutionAdapterError("prod_execution_adapter_window_expired")
    epoch_id = request["release_bom"]["bootstrap_authorization"]["bootstrap_epoch_id"]
    body = {
        "schema_version": EXACT_REQUEST_SCHEMA_VERSION,
        "release_id": request["release_id"],
        "epoch_id": epoch_id,
        "created_at": current.isoformat(),
        "expires_at": expires_at.isoformat(),
        "topic": release.TOPIC,
        "partition": release.PARTITION,
        "offset": release.TARGET_OFFSET,
        "event_uid": release.TARGET_EVENT_UID,
        "raw_sha256": release.TARGET_RAW_SHA256,
        "project_key": release.TARGET_PROJECT_KEY,
        "work_item_type_key": release.TARGET_WORK_ITEM_TYPE_KEY,
        "work_item_id": release.TARGET_WORK_ITEM_ID,
        "business_key": release.TARGET_BUSINESS_KEY,
        "submission_key": release.TARGET_SUBMISSION_KEY,
        "generation": 1,
        "final_validation_sha256": final_owned.sha256,
        "nonce": nonce,
    }
    body["request_sha256"] = hashlib.sha256(_canonical_bytes(body)).hexdigest()
    file_sha256 = release._publish_no_clobber(output_path, body)
    return {
        **body,
        "path": str(output_path.expanduser().absolute()),
        "file_sha256": file_sha256,
        "production_effects": {
            "control_db_writes": 0,
            "feishu_writes": 0,
            "kafka_offset_commits": 0,
        },
    }


def _validate_exact_request_owned(owned: release.OwnedJson) -> Mapping[str, Any]:
    body = dict(owned.body)
    claimed = str(body.pop("request_sha256", ""))
    expected_fields = {
        "schema_version",
        "release_id",
        "epoch_id",
        "created_at",
        "expires_at",
        "topic",
        "partition",
        "offset",
        "event_uid",
        "raw_sha256",
        "project_key",
        "work_item_type_key",
        "work_item_id",
        "business_key",
        "submission_key",
        "generation",
        "final_validation_sha256",
        "nonce",
        "request_sha256",
    }
    if (
        set(owned.body) != expected_fields
        or body.get("schema_version") != EXACT_REQUEST_SCHEMA_VERSION
        or body.get("topic") != release.TOPIC
        or body.get("partition") != release.PARTITION
        or body.get("offset") != release.TARGET_OFFSET
        or body.get("event_uid") != release.TARGET_EVENT_UID
        or body.get("raw_sha256") != release.TARGET_RAW_SHA256
        or body.get("project_key") != release.TARGET_PROJECT_KEY
        or body.get("work_item_type_key") != release.TARGET_WORK_ITEM_TYPE_KEY
        or body.get("work_item_id") != release.TARGET_WORK_ITEM_ID
        or body.get("business_key") != release.TARGET_BUSINESS_KEY
        or body.get("submission_key") != release.TARGET_SUBMISSION_KEY
        or body.get("generation") != 1
        or _SHA256_RE.fullmatch(str(body.get("final_validation_sha256") or "")) is None
        or _NONCE_RE.fullmatch(str(body.get("nonce") or "")) is None
        or _SHA256_RE.fullmatch(claimed) is None
        or hashlib.sha256(_canonical_bytes(body)).hexdigest() != claimed
    ):
        raise ExecutionAdapterError("prod_execution_adapter_exact_request_invalid")
    return {**body, "request_sha256": claimed}


def validate_exact_receipt(
    *,
    exact_request_path: Path,
    exact_receipt_path: Path,
    consumer_health_path: Path,
) -> Mapping[str, Any]:
    request_owned = release._read_owned_json(
        exact_request_path, artifact="execution_adapter_exact_request"
    )
    request = _validate_exact_request_owned(request_owned)
    receipt_owned = release._read_owned_json(
        exact_receipt_path, artifact="execution_adapter_exact_receipt"
    )
    receipt = receipt_owned.body
    expected_fields = {
        "schema_version",
        "release_id",
        "epoch_id",
        "request_sha256",
        "final_validation_sha256",
        "processed_at",
        "event_uid",
        "raw_sha256",
        "business_key",
        "submission_key",
        "generation",
        "outcome",
        "activation_slot_kind",
        "resident_runtime_identity_sha256",
        "kafka_observation",
        "raw_payload_persisted",
        "kafka_offset_committed",
    }
    observation = receipt.get("kafka_observation")
    retained_start = (
        observation.get("retained_start") if isinstance(observation, Mapping) else None
    )
    retained_end = (
        observation.get("retained_end") if isinstance(observation, Mapping) else None
    )
    health = release._read_owned_json(
        consumer_health_path, artifact="execution_adapter_consumer_health"
    ).body
    runtime_identity = health.get("runtime_identity")
    processed_at = _timestamp(receipt.get("processed_at"), field="processed_at")
    if (
        set(receipt) != expected_fields
        or receipt.get("schema_version") != EXACT_RECEIPT_SCHEMA_VERSION
        or receipt.get("release_id") != request["release_id"]
        or receipt.get("epoch_id") != request["epoch_id"]
        or receipt.get("request_sha256") != request["request_sha256"]
        or receipt.get("final_validation_sha256") != request["final_validation_sha256"]
        or receipt.get("event_uid") != release.TARGET_EVENT_UID
        or receipt.get("raw_sha256") != release.TARGET_RAW_SHA256
        or receipt.get("business_key") != release.TARGET_BUSINESS_KEY
        or receipt.get("submission_key") != release.TARGET_SUBMISSION_KEY
        or receipt.get("generation") != 1
        or receipt.get("outcome") != "ingested"
        or receipt.get("activation_slot_kind") != "kafka_success"
        or receipt.get("raw_payload_persisted") is not True
        or receipt.get("kafka_offset_committed") is not False
        or not isinstance(observation, Mapping)
        or observation.get("assignment_mode") != "explicit_single_partition"
        or observation.get("assigned_partitions") != [release.PARTITION]
        or observation.get("group_id") is not None
        or observation.get("enable_auto_commit") is not False
        or observation.get("commit_called") is not False
        or isinstance(retained_start, bool)
        or not isinstance(retained_start, int)
        or isinstance(retained_end, bool)
        or not isinstance(retained_end, int)
        or not retained_start <= release.TARGET_OFFSET < retained_end
        or not isinstance(runtime_identity, Mapping)
        or runtime_identity.get("service_label") != "local.pnc.rca-kafka-consumer"
        or receipt.get("resident_runtime_identity_sha256")
        != release._sha256_value(dict(runtime_identity))
        or processed_at < _timestamp(request["created_at"], field="created_at")
        or processed_at > _timestamp(request["expires_at"], field="expires_at")
    ):
        raise ExecutionAdapterError("prod_execution_adapter_exact_receipt_invalid")
    return {
        "schema_version": "pnc_rca_exact_kafka_recovery_validation_v1",
        "ok": True,
        "request_path": str(request_owned.path),
        "request_sha256": request_owned.sha256,
        "receipt_path": str(receipt_owned.path),
        "receipt_sha256": receipt_owned.sha256,
        "event_uid": release.TARGET_EVENT_UID,
        "release_id": request["release_id"],
        "epoch_id": request["epoch_id"],
        "final_validation_sha256": request["final_validation_sha256"],
        "processed_at": processed_at.isoformat(),
        "resident_runtime_identity_sha256": receipt["resident_runtime_identity_sha256"],
        "kafka_offset_committed": False,
    }


def official_full_readback(
    *,
    expectation_path: Path,
    output_path: Path,
    adapter_factory: Callable[
        [], MeegleIssueCommentAdapter
    ] = MeegleIssueCommentAdapter,
    now: datetime | None = None,
) -> Mapping[str, Any]:
    current = _now(now)
    owned = release._read_owned_json(
        expectation_path, artifact="execution_adapter_readback_expectation"
    )
    value = owned.body
    expected_fields = {
        "schema_version",
        "release_id",
        "project_key",
        "work_item_id",
        "field_values",
        "comment_id",
        "comment_content",
        "marker",
        "terminal_receipt_sha256",
        "not_before",
    }
    field_values = value.get("field_values")
    expected_comment = value.get("comment_content")
    marker = str(value.get("marker") or "")
    if (
        set(value) != expected_fields
        or value.get("schema_version") != READBACK_EXPECTATION_SCHEMA_VERSION
        or not isinstance(field_values, Mapping)
        or set(field_values) != set(release.TARGET_FIELD_KEYS)
        or any(
            not isinstance(field_values.get(key), Mapping)
            or _sha256(field_values[key].get("sha256"), field=f"{key}_sha256")
            != field_values[key].get("sha256")
            or not isinstance(field_values[key].get("utf8_bytes"), int)
            or field_values[key].get("utf8_bytes", -1) < 0
            for key in release.TARGET_FIELD_KEYS
        )
        or not isinstance(expected_comment, Mapping)
        or _sha256(expected_comment.get("sha256"), field="comment_sha256")
        != expected_comment.get("sha256")
        or not isinstance(expected_comment.get("utf8_bytes"), int)
        or expected_comment.get("utf8_bytes", -1) < 1
        or not str(value.get("comment_id") or "").strip()
        or not marker
        or _sha256(
            value.get("terminal_receipt_sha256"), field="terminal_receipt_sha256"
        )
        != value.get("terminal_receipt_sha256")
        or current < _timestamp(value.get("not_before"), field="not_before")
    ):
        raise ExecutionAdapterError(
            "prod_execution_adapter_readback_expectation_invalid"
        )
    result = adapter_factory().get_fields_and_comments(
        str(value["project_key"]),
        str(value["work_item_id"]),
        release.TARGET_FIELD_KEYS,
    )
    if result.get("success") is not True:
        raise ExecutionAdapterError("prod_execution_adapter_official_read_failed")
    observed_fields = {
        key: _hash_text(str(result["fields"].get(key, "")))
        for key in release.TARGET_FIELD_KEYS
    }
    comments = result.get("comments")
    if not isinstance(comments, list):
        raise ExecutionAdapterError("prod_execution_adapter_comments_invalid")
    comment_receipts = []
    marker_match_count = 0
    expected_matches = []
    for comment in comments:
        if not isinstance(comment, Mapping):
            raise ExecutionAdapterError("prod_execution_adapter_comments_invalid")
        remote_id = str(comment.get("remote_id") or "").strip()
        content = str(comment.get("content") or "")
        if not remote_id:
            raise ExecutionAdapterError("prod_execution_adapter_comments_invalid")
        marker_count = content.count(marker)
        marker_match_count += marker_count
        content_identity = _hash_text(content)
        comment_receipts.append({
            "comment_id": remote_id,
            "content_sha256": content_identity["sha256"],
            "content_utf8_bytes": content_identity["utf8_bytes"],
            "marker_match_count": marker_count,
        })
        if remote_id == value["comment_id"]:
            expected_matches.append(content_identity)
    if (
        observed_fields != field_values
        or expected_matches != [dict(expected_comment)]
        or marker_match_count != 1
        or len(comment_receipts)
        != len({item["comment_id"] for item in comment_receipts})
    ):
        raise ExecutionAdapterError("prod_execution_adapter_official_readback_mismatch")
    receipt = {
        "schema_version": READBACK_RECEIPT_SCHEMA_VERSION,
        "release_id": value["release_id"],
        "adapter": "MeegleIssueCommentAdapter.get_fields_and_comments",
        "source": "official_meegle_api",
        "scope": {
            "project_key": value["project_key"],
            "work_item_id": value["work_item_id"],
        },
        "observed_at": current.isoformat(),
        "fields": observed_fields,
        "comment_id": value["comment_id"],
        "comment_content_sha256": expected_comment["sha256"],
        "comment_content_utf8_bytes": expected_comment["utf8_bytes"],
        "marker_sha256": hashlib.sha256(marker.encode("utf-8")).hexdigest(),
        "marker_match_count": marker_match_count,
        "pages_read": result["pages_read"],
        "comments": comment_receipts,
        "terminal_receipt_sha256": value["terminal_receipt_sha256"],
        "full_bodies_persisted": False,
    }
    file_sha256 = release._publish_no_clobber(output_path, receipt)
    return {**receipt, "path": str(output_path.absolute()), "sha256": file_sha256}


def build_natural_gate(
    *,
    approval_request_path: Path,
    final_validation_path: Path,
    exact_request_path: Path,
    exact_receipt_path: Path,
    consumer_health_path: Path,
    target_readback_path: Path,
    minimum_offset: int,
    output_path: Path,
    now: datetime | None = None,
) -> Mapping[str, Any]:
    current = _now(now)
    if isinstance(minimum_offset, bool) or minimum_offset < 0:
        raise ExecutionAdapterError("prod_execution_adapter_minimum_offset_invalid")
    request, final_owned, verified = _validate_final_authority(
        request_path=approval_request_path,
        final_validation_path=final_validation_path,
        now=current,
    )
    exact_validation = validate_exact_receipt(
        exact_request_path=exact_request_path,
        exact_receipt_path=exact_receipt_path,
        consumer_health_path=consumer_health_path,
    )
    epoch_id = request["release_bom"]["bootstrap_authorization"]["bootstrap_epoch_id"]
    if (
        exact_validation.get("release_id") != request["release_id"]
        or exact_validation.get("epoch_id") != epoch_id
        or exact_validation.get("final_validation_sha256") != final_owned.sha256
    ):
        raise ExecutionAdapterError(
            "prod_execution_adapter_exact_release_binding_invalid"
        )
    readback_owned = release._read_owned_json(
        target_readback_path, artifact="execution_adapter_target_readback"
    )
    readback = readback_owned.body
    readback_at = _validate_full_readback_receipt(
        readback,
        expected_release_id=request["release_id"],
    )
    if (
        readback_at < _timestamp(exact_validation["processed_at"], field="processed_at")
        or readback_at > current + release.MAX_FUTURE_SKEW
    ):
        raise ExecutionAdapterError("prod_execution_adapter_target_readback_invalid")
    execute_before = _timestamp(verified["execute_before"], field="execute_before")
    expires_at = min(execute_before, current + MAX_REQUEST_VALIDITY)
    if expires_at <= current:
        raise ExecutionAdapterError("prod_execution_adapter_window_expired")
    exact_receipt_owned = release._read_owned_json(
        exact_receipt_path, artifact="execution_adapter_exact_receipt_binding"
    )
    body = {
        "schema_version": NATURAL_GATE_SCHEMA_VERSION,
        "release_id": request["release_id"],
        "epoch_id": epoch_id,
        "created_at": current.isoformat(),
        "expires_at": expires_at.isoformat(),
        "exact_readback_sha256": readback_owned.sha256,
        "exact_recovery_receipt_sha256": exact_receipt_owned.sha256,
        "minimum_offset": minimum_offset,
    }
    body["request_sha256"] = hashlib.sha256(_canonical_bytes(body)).hexdigest()
    file_sha256 = release._publish_no_clobber(output_path, body)
    return {**body, "path": str(output_path.absolute()), "sha256": file_sha256}


def _owner_only_database(path: Path) -> Path:
    selected = path.expanduser().absolute()
    try:
        info = selected.lstat()
    except OSError as exc:
        raise ExecutionAdapterError(
            "prod_execution_adapter_database_unavailable"
        ) from exc
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.geteuid()
        or stat.S_IMODE(info.st_mode) & 0o077
        or info.st_nlink != 1
    ):
        raise ExecutionAdapterError("prod_execution_adapter_database_not_owner_only")
    return selected


def select_first_natural(
    *,
    control_db_path: Path,
    target_readback_path: Path,
    natural_canary_receipt_path: Path,
    minimum_offset: int,
    output_path: Path,
    now: datetime | None = None,
) -> Mapping[str, Any]:
    current = _now(now)
    if isinstance(minimum_offset, bool) or minimum_offset < 0:
        raise ExecutionAdapterError("prod_execution_adapter_minimum_offset_invalid")
    readback_owned = release._read_owned_json(
        target_readback_path, artifact="execution_adapter_target_readback"
    )
    readback = readback_owned.body
    not_before = _validate_full_readback_receipt(
        readback,
        expected_release_id=str(readback.get("release_id") or ""),
    )
    resident_owned = release._read_owned_json(
        natural_canary_receipt_path,
        artifact="execution_adapter_natural_canary_receipt",
    )
    resident = resident_owned.body
    expected_resident_fields = {
        "schema_version",
        "release_id",
        "epoch_id",
        "request_sha256",
        "selected_at",
        "topic",
        "partition",
        "offset",
        "event_uid",
        "business_key",
        "submission_key",
        "generation",
        "decision",
        "activation_reason",
        "consumer_group_id",
        "kafka_offset_committed",
        "resident_runtime_identity_sha256",
        "next_ordinary_record_held",
    }
    resident_partition = resident.get("partition")
    resident_offset = resident.get("offset")
    selected_at = _timestamp(resident.get("selected_at"), field="natural_selected_at")
    if (
        set(resident) != expected_resident_fields
        or resident.get("schema_version") != NATURAL_RECEIPT_SCHEMA_VERSION
        or resident.get("release_id") != readback.get("release_id")
        or not str(resident.get("epoch_id") or "").strip()
        or _sha256(resident.get("request_sha256"), field="natural_request_sha256")
        != resident.get("request_sha256")
        or resident.get("topic") != release.TOPIC
        or isinstance(resident_partition, bool)
        or resident_partition != release.PARTITION
        or isinstance(resident_offset, bool)
        or not isinstance(resident_offset, int)
        or resident_offset < minimum_offset
        or resident.get("event_uid")
        != f"{release.TOPIC}:{release.PARTITION}:{resident_offset}"
        or not str(resident.get("business_key") or "").strip()
        or not str(resident.get("submission_key") or "").strip()
        or resident.get("generation") != 1
        or resident.get("decision") != "accepted"
        or resident.get("activation_reason") != "activation_steady_active"
        or resident.get("consumer_group_id") != "rca_root_cause_analysis_agent"
        or resident.get("kafka_offset_committed") is not True
        or _sha256(
            resident.get("resident_runtime_identity_sha256"),
            field="natural_runtime_identity_sha256",
        )
        != resident.get("resident_runtime_identity_sha256")
        or resident.get("next_ordinary_record_held") is not True
        or selected_at < not_before
        or selected_at > current + release.MAX_FUTURE_SKEW
    ):
        raise ExecutionAdapterError(
            "prod_execution_adapter_natural_resident_receipt_invalid"
        )
    database = _owner_only_database(control_db_path)
    uri = database.as_uri() + "?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True, timeout=5)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        conn.execute("BEGIN")
        rows = conn.execute(
            """
            SELECT i.event_uid,i.topic,i.partition_id,i.offset_id,i.raw_sha256,
                   i.business_key,i.submission_key,i.generation,i.processed_at,
                   t.project_key,t.work_item_type_key,t.work_item_id,
                   o.status AS outbox_status,o.completed_at,
                   al.entrypoint,al.slot_kind,al.decision AS activation_decision,
                   al.reason AS activation_reason,
                   j.delivery_id,j.status AS job_status,j.outcome AS job_outcome,
                   SUM(CASE WHEN e.required=1 THEN 1 ELSE 0 END) AS required_effects,
                   SUM(CASE WHEN e.required=1 AND e.status='succeeded' THEN 1 ELSE 0 END)
                       AS succeeded_required_effects,
                   (SELECT COUNT(*) FROM rca_shadow_promotion_audit AS spa
                     WHERE spa.event_uid=i.event_uid) AS recovery_write_count
              FROM kafka_inbox AS i
              LEFT JOIN business_triggers AS t ON t.submission_key=i.submission_key
              LEFT JOIN rca_outbox AS o ON o.submission_key=i.submission_key
              LEFT JOIN rca_activation_admission_ledger AS al
                ON al.epoch_id=o.activation_epoch_id
               AND al.ledger_id=o.activation_ledger_id
              LEFT JOIN rca_delivery_jobs AS j ON j.submission_key=i.submission_key
              LEFT JOIN rca_delivery_effects AS e ON e.delivery_id=j.delivery_id
             WHERE i.topic=? AND i.partition_id=? AND i.offset_id>=?
               AND i.event_uid!=? AND i.decision='accepted' AND i.processed_at>=?
             GROUP BY i.event_uid
             ORDER BY i.offset_id
             LIMIT 2
            """,
            (
                release.TOPIC,
                release.PARTITION,
                minimum_offset,
                release.TARGET_EVENT_UID,
                not_before.isoformat(),
            ),
        ).fetchall()
        conn.rollback()
    except sqlite3.Error as exc:
        raise ExecutionAdapterError(
            "prod_execution_adapter_natural_query_failed"
        ) from exc
    finally:
        if "conn" in locals():
            conn.close()
    if not rows:
        raise ExecutionAdapterError("prod_execution_adapter_natural_not_observed")
    row = dict(rows[0])
    if (
        row.get("entrypoint") != "kafka_ingest"
        or str(row.get("slot_kind") or "") != ""
        or row.get("activation_decision") != "admit"
        or row.get("activation_reason") != "activation_steady_active"
        or row.get("outbox_status") != "completed"
        or not row.get("completed_at")
        or row.get("job_status") != "delivered"
        or row.get("job_outcome") != "success"
        or int(row.get("required_effects") or 0) < 1
        or row.get("required_effects") != row.get("succeeded_required_effects")
        or row.get("recovery_write_count") != 0
        or row.get("project_key") != release.TARGET_PROJECT_KEY
        or row.get("work_item_id") == release.TARGET_WORK_ITEM_ID
        or row.get("generation") != 1
        or row.get("event_uid") != resident.get("event_uid")
        or row.get("offset_id") != resident.get("offset")
        or row.get("business_key") != resident.get("business_key")
        or row.get("submission_key") != resident.get("submission_key")
    ):
        raise ExecutionAdapterError("prod_execution_adapter_natural_not_closed")
    receipt = {
        "schema_version": NATURAL_SELECTOR_SCHEMA_VERSION,
        "release_id": readback["release_id"],
        "observed_at": current.isoformat(),
        "target_readback_sha256": readback_owned.sha256,
        "resident_canary_receipt_sha256": resident_owned.sha256,
        "minimum_offset": minimum_offset,
        "selection_order": "lowest_accepted_offset_after_exact_readback",
        "topic": row["topic"],
        "partition": row["partition_id"],
        "offset": row["offset_id"],
        "event_uid": row["event_uid"],
        "raw_sha256": row["raw_sha256"],
        "project_key": row["project_key"],
        "work_item_type_key": row["work_item_type_key"],
        "work_item_id": row["work_item_id"],
        "business_key": row["business_key"],
        "submission_key": row["submission_key"],
        "generation": row["generation"],
        "delivery_id": row["delivery_id"],
        "activation": {
            "entrypoint": row["entrypoint"],
            "slot_kind": str(row["slot_kind"] or ""),
            "decision": row["activation_decision"],
            "reason": row["activation_reason"],
        },
        "delivery_source": "ordinary_kafka_ingest",
        "recovery_write_count": 0,
        "operator_recovery_provenance": [],
    }
    file_sha256 = release._publish_no_clobber(output_path, receipt)
    return {**receipt, "path": str(output_path.absolute()), "sha256": file_sha256}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("build-exact-request")
    build.add_argument("--approval-request", type=Path, required=True)
    build.add_argument("--final-validation", type=Path, required=True)
    build.add_argument("--output", type=Path, required=True)
    build.add_argument("--nonce", required=True)
    validate = commands.add_parser("validate-exact-receipt")
    validate.add_argument("--exact-request", type=Path, required=True)
    validate.add_argument("--exact-receipt", type=Path, required=True)
    validate.add_argument("--consumer-health", type=Path, required=True)
    readback = commands.add_parser("official-readback")
    readback.add_argument("--expectation", type=Path, required=True)
    readback.add_argument("--output", type=Path, required=True)
    natural = commands.add_parser("select-first-natural")
    natural.add_argument("--control-db", type=Path, required=True)
    natural.add_argument("--target-readback", type=Path, required=True)
    natural.add_argument("--natural-canary-receipt", type=Path, required=True)
    natural.add_argument("--minimum-offset", type=int, required=True)
    natural.add_argument("--output", type=Path, required=True)
    gate = commands.add_parser("build-natural-gate")
    gate.add_argument("--approval-request", type=Path, required=True)
    gate.add_argument("--final-validation", type=Path, required=True)
    gate.add_argument("--exact-request", type=Path, required=True)
    gate.add_argument("--exact-receipt", type=Path, required=True)
    gate.add_argument("--consumer-health", type=Path, required=True)
    gate.add_argument("--target-readback", type=Path, required=True)
    gate.add_argument("--minimum-offset", type=int, required=True)
    gate.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "build-exact-request":
            result = build_exact_request(
                approval_request_path=args.approval_request,
                final_validation_path=args.final_validation,
                output_path=args.output,
                nonce=args.nonce,
            )
        elif args.command == "validate-exact-receipt":
            result = validate_exact_receipt(
                exact_request_path=args.exact_request,
                exact_receipt_path=args.exact_receipt,
                consumer_health_path=args.consumer_health,
            )
        elif args.command == "official-readback":
            result = official_full_readback(
                expectation_path=args.expectation,
                output_path=args.output,
            )
        elif args.command == "select-first-natural":
            result = select_first_natural(
                control_db_path=args.control_db,
                target_readback_path=args.target_readback,
                natural_canary_receipt_path=args.natural_canary_receipt,
                minimum_offset=args.minimum_offset,
                output_path=args.output,
            )
        else:
            result = build_natural_gate(
                approval_request_path=args.approval_request,
                final_validation_path=args.final_validation,
                exact_request_path=args.exact_request,
                exact_receipt_path=args.exact_receipt,
                consumer_health_path=args.consumer_health,
                target_readback_path=args.target_readback,
                minimum_offset=args.minimum_offset,
                output_path=args.output,
            )
    except Exception as exc:
        code = (
            exc.code
            if isinstance(exc, ExecutionAdapterError)
            else "prod_execution_adapter_internal_error"
        )
        print(json.dumps({"ok": False, "code": code}, sort_keys=True))
        return 2
    print(json.dumps({"ok": True, "result": result}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
