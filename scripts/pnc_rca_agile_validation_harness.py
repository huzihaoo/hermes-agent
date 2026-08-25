#!/usr/bin/env python3
"""Build the privacy-safe V286/K286/S16 validation contract and mode receipt.

The harness is deliberately not a production writer.  ``canary_write`` and
``write`` validate an exact owner authority and stop at a canonical-executor
handoff.  Offline and shadow modes only read sealed inputs and an optional
SQLite database opened with ``mode=ro``.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import sys
from typing import Any, Iterable, Mapping, Sequence

from gateway.pnc_rca_admission import build_rca_admission

SCHEMA_VERSION = "pnc_rca_validation_manifest_v2"
MODE_RECEIPT_SCHEMA_VERSION = "pnc_rca_agile_validation_mode_receipt_v1"
AUTHORITY_SCHEMA_VERSION = "pnc_rca_agile_write_authority_v1"
ATTRIBUTION_UPGRADE_SCHEMA_VERSION = "g1q3_v286_attribution_upgrade_contract_v1"
CREATOR_KEY = "7649830284321508335"
CREATOR_NAME = "\u9ece\u6d9b\u534e"
PROJECT_FIELD_KEY = "field_052f23"
PROJECT_NAME_PREFIX = "G1Q3"
ALLOWED_PROJECT_OPTION_IDS = ("6670325063",)
EXPECTED_CASE_COUNT = 286
EXPECTED_SOURCE_SHA256 = (
    "62cbf88a7430da9615e46260ec9cbbfb363b0849f15cef1c934ecd03fd888502"
)
FRAME_FIELD_KEY = "field_1fda45"
PDCL_FIELD_KEY = "field_527bc7"
REPORT_FIELD_KEY = "field_8c912e"
RESULT_FIELD_KEY = "field_9193cb"
MODES = ("offline", "shadow", "canary_write", "write")
WRITE_MODES = frozenset({"canary_write", "write"})


class ValidationHarnessError(ValueError):
    def __init__(self, code: str, detail: str = ""):
        self.code = str(code or "validation_harness_invalid")[:120]
        self.detail = str(detail or self.code)[:1000]
        super().__init__(self.detail)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode("ascii")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _field_map(data: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    fields = data.get("work_item_fields")
    if not isinstance(fields, list):
        raise ValidationHarnessError("source_work_item_fields_invalid")
    result: dict[str, Mapping[str, Any]] = {}
    for field in fields:
        if not isinstance(field, Mapping):
            raise ValidationHarnessError("source_work_item_field_invalid")
        key = str(field.get("key") or "").strip()
        if not key:
            raise ValidationHarnessError("source_work_item_field_key_missing")
        if key in result:
            raise ValidationHarnessError("source_work_item_field_duplicate", key)
        result[key] = field
    return result


def _nonempty(value: Any) -> bool:
    return value not in (None, "", [], {})


def _category(title: str) -> str:
    upper = f" {title.upper()} "
    for name in ("LCC", "ACC", "HMI"):
        start = 0
        while True:
            index = upper.find(name, start)
            if index < 0:
                break
            before = upper[index - 1] if index else " "
            after_index = index + len(name)
            after = upper[after_index] if after_index < len(upper) else " "
            if not before.isalpha() and not after.isalpha():
                return name
            start = index + 1
    return "OTHER"


def _project_values(field: Mapping[str, Any]) -> list[tuple[str, str]]:
    values = field.get("value")
    if not isinstance(values, list):
        raise ValidationHarnessError("source_project_value_invalid")
    result: list[tuple[str, str]] = []
    for value in values:
        if not isinstance(value, Mapping):
            raise ValidationHarnessError("source_project_option_invalid")
        option_id = str(value.get("id") or "").strip()
        name = str(value.get("name") or "").strip()
        if option_id:
            result.append((option_id, name))
    return result


def load_v286_cases(
    source_path: Path,
    *,
    expected_source_sha256: str = EXPECTED_SOURCE_SHA256,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    source_sha256 = sha256_file(source_path)
    if source_sha256 != expected_source_sha256:
        raise ValidationHarnessError(
            "source_snapshot_sha256_mismatch",
            f"expected={expected_source_sha256}:observed={source_sha256}",
        )
    selected: list[dict[str, Any]] = []
    source_rows = 0
    summary_rows = 0
    seen_ids: set[str] = set()
    with source_path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            try:
                row = json.loads(raw_line)
            except json.JSONDecodeError as exc:
                raise ValidationHarnessError(
                    "source_snapshot_json_invalid", str(line_number)
                ) from exc
            if not isinstance(row, Mapping):
                raise ValidationHarnessError("source_snapshot_row_invalid")
            data = row.get("data")
            if not isinstance(data, Mapping):
                if isinstance(row.get("summary"), Mapping):
                    summary_rows += 1
                    continue
                raise ValidationHarnessError("source_snapshot_row_shape_invalid")
            source_rows += 1
            work_item_id = str(row.get("work_item_id") or "").strip()
            if not work_item_id or work_item_id in seen_ids:
                raise ValidationHarnessError(
                    "source_work_item_identity_invalid", work_item_id
                )
            seen_ids.add(work_item_id)
            attribute = data.get("work_item_attribute")
            if not isinstance(attribute, Mapping):
                raise ValidationHarnessError("source_work_item_attribute_invalid")
            creator = attribute.get("create_by")
            if not isinstance(creator, Mapping):
                raise ValidationHarnessError("source_creator_invalid")
            creator_key = str(creator.get("key") or "").strip()
            creator_name = str(creator.get("name") or "").strip()
            fields = _field_map(data)
            project_field = fields.get(PROJECT_FIELD_KEY)
            if not isinstance(project_field, Mapping):
                continue
            project_values = _project_values(project_field)
            project_match = any(
                option_id in ALLOWED_PROJECT_OPTION_IDS
                and name.startswith(PROJECT_NAME_PREFIX)
                for option_id, name in project_values
            )
            if creator_key != CREATOR_KEY or not project_match:
                continue
            if creator_name != CREATOR_NAME:
                raise ValidationHarnessError("source_creator_name_mismatch")
            option_ids = sorted({value[0] for value in project_values})
            if option_ids != list(ALLOWED_PROJECT_OPTION_IDS):
                raise ValidationHarnessError(
                    "source_project_option_not_exact", work_item_id
                )
            title = str(attribute.get("work_item_name") or "")
            if not title:
                raise ValidationHarnessError("source_title_missing", work_item_id)
            frame = fields.get(FRAME_FIELD_KEY, {})
            pdcl = fields.get(PDCL_FIELD_KEY, {})
            report = fields.get(REPORT_FIELD_KEY, {})
            selected.append(
                {
                    "work_item_id": work_item_id,
                    "creator": {"key": creator_key, "name": creator_name},
                    "project": {
                        "field_key": PROJECT_FIELD_KEY,
                        "option_id": option_ids[0],
                        "name_prefix": PROJECT_NAME_PREFIX,
                    },
                    "title_sha256": sha256_bytes(title.encode("utf-8")),
                    "category": _category(title),
                    "frame_status": (
                        "present"
                        if isinstance(frame, Mapping) and _nonempty(frame.get("value"))
                        else "missing"
                    ),
                    "pdcl_status": (
                        "true"
                        if isinstance(pdcl, Mapping)
                        and str(pdcl.get("value") or "").strip().lower() == "true"
                        else "not_true"
                    ),
                    "existing_report_present": bool(
                        isinstance(report, Mapping) and _nonempty(report.get("value"))
                    ),
                }
            )
    if len(selected) != EXPECTED_CASE_COUNT:
        raise ValidationHarnessError(
            "v286_count_mismatch",
            f"expected={EXPECTED_CASE_COUNT}:observed={len(selected)}",
        )
    selected.sort(key=lambda item: item["work_item_id"])
    return selected, {
        "path": str(source_path),
        "sha256": source_sha256,
        "source_rows": source_rows,
        "summary_rows": summary_rows,
    }


def _file_identity(path: Path) -> dict[str, Any]:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return {"path": str(path), "present": False}
    return {
        "path": str(path),
        "present": True,
        "inode": info.st_ino,
        "device": info.st_dev,
        "size": info.st_size,
        "mtime_ns": info.st_mtime_ns,
        "mode": stat.S_IMODE(info.st_mode),
        "regular": stat.S_ISREG(info.st_mode),
        "symlink": stat.S_ISLNK(info.st_mode),
    }


def _db_identity(db_path: Path) -> list[dict[str, Any]]:
    return [_file_identity(Path(f"{db_path}{suffix}")) for suffix in ("", "-wal", "-shm")]


def _physical_identity(value: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        value.get("present"),
        value.get("device"),
        value.get("inode"),
        value.get("regular"),
        value.get("symlink"),
        value.get("mode"),
    )


def _chunks(values: Sequence[str], size: int = 250) -> Iterable[Sequence[str]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def _query_in(
    connection: sqlite3.Connection,
    query_prefix: str,
    ids: Sequence[str],
) -> list[sqlite3.Row]:
    rows: list[sqlite3.Row] = []
    for chunk in _chunks(ids):
        placeholders = ",".join("?" for _ in chunk)
        rows.extend(connection.execute(f"{query_prefix} ({placeholders})", chunk))
    return rows


def read_control_projection(
    db_path: Path | None,
    work_item_ids: Sequence[str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    if db_path is None:
        return {}, {
            "configured": False,
            "mode": "not_read",
            "source_stable": None,
        }
    before = _db_identity(db_path)
    if not before[0].get("regular") or before[0].get("symlink"):
        raise ValidationHarnessError("control_db_identity_invalid")
    uri = f"file:{db_path}?mode=ro"
    projection: dict[str, Any] = {
        work_item_id: {"events": [], "triggers": [], "effects": []}
        for work_item_id in work_item_ids
    }
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        events = _query_in(
            connection,
            """
            SELECT CAST(json_extract(normalized_json, '$.work_item_id') AS TEXT)
                       AS work_item_id,
                   event_uid, topic, partition_id, offset_id, raw_sha256,
                   business_key, submission_key, generation
              FROM kafka_inbox
             WHERE normalized_json IS NOT NULL
               AND CAST(json_extract(normalized_json, '$.work_item_id') AS TEXT) IN
            """,
            work_item_ids,
        )
        triggers = _query_in(
            connection,
            """
            SELECT work_item_id, business_key, generation, submission_key, state,
                   creation_rule_version, project_key, work_item_type_key,
                   normalized_json, source_event_id, source_topic,
                   source_partition, source_offset
              FROM business_triggers
             WHERE work_item_id IN
            """,
            work_item_ids,
        )
        effects = _query_in(
            connection,
            """
            SELECT j.work_item_id, j.delivery_id, j.submission_key,
                   j.generation, j.outcome AS job_outcome,
                   j.status AS job_status, j.created_at AS job_created_at,
                   j.contract_json, e.effect_key, e.effect_kind, e.outcome,
                   e.status, e.completed_at, e.payload_json,
                   e.payload_sha256, e.remote_receipt_json
              FROM rca_delivery_jobs AS j
              JOIN rca_delivery_effects AS e ON e.delivery_id = j.delivery_id
             WHERE j.work_item_id IN
            """,
            work_item_ids,
        )
        connection.rollback()
        connection.close()
    except sqlite3.Error as exc:
        raise ValidationHarnessError("control_db_read_failed", str(exc)) from exc
    after = _db_identity(db_path)
    if _physical_identity(before[0]) != _physical_identity(after[0]):
        raise ValidationHarnessError("control_db_physical_identity_changed_during_read")
    if any(
        value.get("present")
        and (not value.get("regular") or value.get("symlink"))
        for value in (*before, *after)
    ):
        raise ValidationHarnessError("control_db_sidecar_identity_invalid")
    for row in events:
        work_item_id = str(row["work_item_id"] or "")
        if work_item_id not in projection:
            continue
        projection[work_item_id]["events"].append(
            {
                "event_uid": str(row["event_uid"] or ""),
                "topic": str(row["topic"] or ""),
                "partition": row["partition_id"],
                "offset": row["offset_id"],
                "raw_sha256": str(row["raw_sha256"] or ""),
                "business_key": str(row["business_key"] or "") or None,
                "submission_key": str(row["submission_key"] or "") or None,
                "generation": row["generation"],
            }
        )
    for row in triggers:
        trigger = dict(row)
        normalized = trigger.get("normalized_json")
        if isinstance(normalized, str):
            try:
                context = json.loads(normalized)
            except json.JSONDecodeError:
                context = {}
        else:
            context = {}
        trigger["project_simple_name"] = str(
            context.get("project_simple_name") or ""
        )
        trigger.pop("normalized_json", None)
        projection[str(row["work_item_id"])]["triggers"].append(trigger)
    for row in effects:
        contract_raw = str(row["contract_json"] or "")
        payload_raw = str(row["payload_json"] or "")
        remote_raw = str(row["remote_receipt_json"] or "")
        try:
            contract = json.loads(contract_raw) if contract_raw else {}
            payload = json.loads(payload_raw) if payload_raw else {}
            remote = json.loads(remote_raw) if remote_raw else None
        except json.JSONDecodeError as exc:
            raise ValidationHarnessError(
                "control_db_delivery_json_invalid",
                str(row["effect_key"] or ""),
            ) from exc
        capability = (
            contract.get("consumer_capability")
            if isinstance(contract, Mapping)
            else None
        )
        integrated = (
            capability.get("integrated_sources")
            if isinstance(capability, Mapping)
            else None
        )
        communication = (
            integrated.get("conclusion_communication")
            if isinstance(integrated, Mapping)
            else None
        )
        communication_projection = (
            {
                "schema_version": str(communication.get("schema_version") or "")
                or None,
                "style_profile": str(communication.get("style_profile") or "")
                or None,
                "mode": str(communication.get("mode") or "") or None,
                "sha256": sha256_bytes(canonical_bytes(communication)),
            }
            if isinstance(communication, Mapping)
            else {
                "schema_version": None,
                "style_profile": None,
                "mode": None,
                "sha256": None,
            }
        )
        projection[str(row["work_item_id"])]["effects"].append(
            {
                "delivery_id": str(row["delivery_id"] or ""),
                "submission_key": str(row["submission_key"] or ""),
                "generation": row["generation"],
                "job_outcome": str(row["job_outcome"] or ""),
                "job_status": str(row["job_status"] or ""),
                "job_created_at": str(row["job_created_at"] or ""),
                "effect_key": str(row["effect_key"] or ""),
                "effect_kind": str(row["effect_kind"] or ""),
                "outcome": str(row["outcome"] or ""),
                "status": str(row["status"] or ""),
                "completed_at": str(row["completed_at"] or "") or None,
                "payload_sha256": str(row["payload_sha256"] or ""),
                "payload_schema_version": (
                    str(payload.get("schema_version") or "")
                    if isinstance(payload, Mapping)
                    else ""
                ),
                "marker": (
                    str(payload.get("marker") or "")
                    if isinstance(payload, Mapping)
                    else ""
                ),
                "remote_receipt_present": isinstance(remote, Mapping),
                "contract_sha256": (
                    sha256_bytes(canonical_bytes(contract))
                    if isinstance(contract, Mapping)
                    else None
                ),
                "attribution": communication_projection,
            }
        )
    for item in projection.values():
        item["events"].sort(
            key=lambda value: (
                value["topic"], value["partition"], value["offset"], value["event_uid"]
            )
        )
        item["triggers"].sort(key=lambda value: value["generation"])
        item["effects"].sort(
            key=lambda value: (
                value["generation"], value["effect_kind"], value["status"]
            )
        )
    return projection, {
        "configured": True,
        "path": str(db_path),
        "mode": "ro",
        "query_only": True,
        "source_stable": True,
        "snapshot_consistency": "sqlite_read_transaction",
        "main_physical_identity_stable": True,
        "sidecar_metadata_changed": before[1:] != after[1:],
        "identities_before": before,
        "identities_after": after,
    }


def load_official_readback(path: Path | None) -> tuple[dict[str, Any], dict[str, Any]]:
    if path is None:
        return {}, {"configured": False}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValidationHarnessError("official_readback_invalid", str(exc)) from exc
    if not isinstance(payload, Mapping) or not isinstance(payload.get("results"), list):
        raise ValidationHarnessError("official_readback_shape_invalid")
    result: dict[str, Any] = {}
    for raw in payload["results"]:
        if not isinstance(raw, Mapping):
            raise ValidationHarnessError("official_readback_row_invalid")
        work_item_id = str(raw.get("work_item_id") or "").strip()
        if not work_item_id or work_item_id in result:
            raise ValidationHarnessError("official_readback_identity_invalid")
        comments = raw.get("comments")
        report = raw.get("report_field")
        result_field = raw.get("result_field")
        result[work_item_id] = {
            "classification": str(raw.get("classification") or "unknown"),
            "comments_complete": bool(
                isinstance(comments, Mapping) and comments.get("complete") is True
            ),
            "comment_count": (
                int(comments.get("total") or 0) if isinstance(comments, Mapping) else 0
            ),
            "report_present": bool(
                isinstance(report, Mapping) and report.get("nonempty") is True
            ),
            "result_present": bool(
                isinstance(result_field, Mapping)
                and result_field.get("nonempty") is True
            ),
            "report_sha256": (
                str(report.get("sha256") or "")
                if isinstance(report, Mapping)
                else ""
            ),
            "result_sha256": (
                str(result_field.get("sha256") or "")
                if isinstance(result_field, Mapping)
                else ""
            ),
            "delivery_effect_keys": sorted({
                str(value)
                for value in raw.get("delivery_effect_keys") or []
                if str(value or "").strip()
            }),
            "terminal_effect_keys": sorted({
                str(value)
                for value in raw.get("terminal_effect_keys") or []
                if str(value or "").strip()
            }),
            "attribution_markers": [
                {
                    "version": str(marker.get("version") or ""),
                    "contract_sha256": str(marker.get("contract_sha256") or ""),
                    "effect_key": str(marker.get("effect_key") or ""),
                }
                for marker in raw.get("attribution_markers") or []
                if isinstance(marker, Mapping)
            ],
        }
    return result, {
        "configured": True,
        "path": str(path),
        "sha256": sha256_file(path),
        "schema_version": str(payload.get("schema_version") or ""),
        "observed_at": str(payload.get("observed_at") or ""),
        "result_count": len(result),
    }


def load_attribution_upgrade_contract(
    path: Path | None,
    *,
    work_item_ids: Sequence[str],
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    if path is None:
        return None, {"configured": False}
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValidationHarnessError(
            "attribution_upgrade_contract_invalid", str(exc)
        ) from exc
    expected_ids_sha256 = sha256_bytes(
        ("\n".join(sorted(work_item_ids)) + "\n").encode("ascii")
    )
    scope = value.get("scope") if isinstance(value, Mapping) else None
    if (
        not isinstance(value, Mapping)
        or value.get("schema_version") != ATTRIBUTION_UPGRADE_SCHEMA_VERSION
        or not str(value.get("version_id") or "").strip()
        or not isinstance(scope, Mapping)
        or scope.get("count") != EXPECTED_CASE_COUNT
        or scope.get("ids_sha256") != expected_ids_sha256
    ):
        raise ValidationHarnessError("attribution_upgrade_contract_invalid")
    contract_sha256 = sha256_bytes(raw)
    return dict(value), {
        "configured": True,
        "path": str(path),
        "sha256": contract_sha256,
        "version_id": str(value["version_id"]),
    }


def _current_trigger(rows: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    return max(rows, key=lambda row: int(row.get("generation") or 0)) if rows else None


def _effect_projection(
    effects: Sequence[Mapping[str, Any]],
    *,
    snapshot_report_present: bool,
) -> dict[str, Any]:
    statuses = Counter(str(row.get("status") or "unknown") for row in effects)
    succeeded = sum(1 for row in effects if row.get("status") == "succeeded")
    if succeeded:
        state = "succeeded_effect_present"
    elif effects:
        state = "non_succeeded_effect_present"
    elif snapshot_report_present:
        state = "snapshot_report_present_without_control_effect"
    else:
        state = "no_effect_observed_offline_readback_required"
    return {
        "state": state,
        "effect_count": len(effects),
        "succeeded_count": succeeded,
        "statuses": dict(sorted(statuses.items())),
        "latest_generation": max(
            (int(row.get("generation") or 0) for row in effects), default=None
        ),
        "effect_keys": sorted({
            str(row.get("effect_key") or "")
            for row in effects
            if str(row.get("effect_key") or "")
        }),
    }


def _current_attribution_projection(
    effects: Sequence[Mapping[str, Any]],
    official: Mapping[str, Any] | None,
) -> dict[str, Any]:
    ordered = sorted(
        effects,
        key=lambda row: (
            int(row.get("generation") or 0),
            str(row.get("completed_at") or ""),
            str(row.get("effect_key") or ""),
        ),
    )
    latest = ordered[-1] if ordered else None
    official_keys = set(official.get("delivery_effect_keys") or []) if official else set()
    succeeded_official = [
        row
        for row in ordered
        if row.get("status") == "succeeded"
        and row.get("effect_key") in official_keys
        and row.get("remote_receipt_present") is True
    ]
    attribution = latest.get("attribution") if isinstance(latest, Mapping) else None
    return {
        "version": (
            attribution.get("schema_version")
            if isinstance(attribution, Mapping)
            else None
        ),
        "hash": (
            attribution.get("sha256")
            if isinstance(attribution, Mapping)
            else None
        ),
        "latest_effect_key": (
            str(latest.get("effect_key") or "") if isinstance(latest, Mapping) else None
        ),
        "latest_generation": (
            latest.get("generation") if isinstance(latest, Mapping) else None
        ),
        "official_succeeded_effect_keys": [
            str(row.get("effect_key") or "") for row in succeeded_official
        ],
    }


def _canonical_next_identity(
    trigger: Mapping[str, Any] | None,
    *,
    work_item_id: str,
) -> dict[str, Any]:
    if not isinstance(trigger, Mapping):
        return {
            "status": "blocked_missing_canonical_generation_chain",
            "generation": None,
            "business_key": None,
            "submission_key": None,
            "effect_key": {
                "status": "deferred_until_canonical_vm_delivery_payload",
                "value": None,
            },
        }
    current_generation = int(trigger.get("generation") or 0)
    if current_generation < 1:
        raise ValidationHarnessError("control_trigger_generation_invalid", work_item_id)
    admission = build_rca_admission(
        project_key=str(trigger.get("project_key") or ""),
        project_simple_name=str(trigger.get("project_simple_name") or ""),
        work_item_type_key=str(trigger.get("work_item_type_key") or ""),
        work_item_id=work_item_id,
        rule_version=str(trigger.get("creation_rule_version") or ""),
        trigger_kind="manual_retrigger",
        generation=current_generation + 1,
    )
    return {
        "status": "derived_canonical_pending_authority",
        "generation": admission.generation,
        "business_key": admission.business_key,
        "submission_key": admission.submission_key,
        "effect_key": {
            "status": "deferred_until_canonical_vm_delivery_payload",
            "value": None,
            "derivation": (
                "compute_delivery_effect_key(delivery_id,effect_kind,target_key,"
                "semantic_payload_sha256) after sealed VM delivery"
            ),
        },
    }


def _upgrade_classification(
    *,
    official: Mapping[str, Any] | None,
    effects: Sequence[Mapping[str, Any]],
    upgrade_contract: Mapping[str, Any] | None,
    upgrade_observation: Mapping[str, Any],
) -> tuple[str, dict[str, Any]]:
    report_present = bool(official and official.get("report_present") is True)
    base = "rewrite_update" if report_present else "create"
    if not isinstance(upgrade_contract, Mapping):
        return base, {"already_current": False, "proof": "target_contract_not_configured"}
    version = str(upgrade_contract.get("version_id") or "")
    contract_sha = str(upgrade_observation.get("sha256") or "")
    official_effects = set(official.get("delivery_effect_keys") or []) if official else set()
    succeeded_effects = {
        str(row.get("effect_key") or "")
        for row in effects
        if row.get("status") == "succeeded"
        and row.get("remote_receipt_present") is True
    }
    matching_markers = [
        marker
        for marker in (official.get("attribution_markers") or [] if official else [])
        if marker.get("version") == version
        and marker.get("contract_sha256") == contract_sha
        and marker.get("effect_key") in official_effects
        and marker.get("effect_key") in succeeded_effects
    ]
    if matching_markers:
        return "already_current", {
            "already_current": True,
            "proof": "official_marker+control_succeeded_effect+remote_receipt",
            "effect_key": str(matching_markers[0]["effect_key"]),
        }
    return base, {
        "already_current": False,
        "proof": "exact_target_version_effect_not_proven",
    }


def select_s16(cases: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for category in ("LCC", "ACC", "HMI", "OTHER"):
        category_cases = [case for case in cases if case.get("category") == category]
        present = sorted(
            (case for case in category_cases if case.get("existing_report_present")),
            key=lambda case: (case["title_sha256"], case["work_item_id"]),
        )
        absent = sorted(
            (case for case in category_cases if not case.get("existing_report_present")),
            key=lambda case: (case["title_sha256"], case["work_item_id"]),
        )
        if len(present) < 2 or len(absent) < 2:
            raise ValidationHarnessError("s16_stratum_insufficient", category)
        for case in (*present[:2], *absent[:2]):
            selected.append(
                {
                    "work_item_id": case["work_item_id"],
                    "category": category,
                    "existing_report_present": case["existing_report_present"],
                    "default_mode": "shadow",
                    "write_authorized": False,
                    "terminal_status": "not_executed",
                }
            )
    if len(selected) != 16 or len({item["work_item_id"] for item in selected}) != 16:
        raise ValidationHarnessError("s16_selection_invalid")
    return selected


def build_validation_manifest(
    source_path: Path,
    *,
    expected_source_sha256: str = EXPECTED_SOURCE_SHA256,
    control_db_path: Path | None = None,
    official_readback_path: Path | None = None,
    attribution_upgrade_path: Path | None = None,
) -> dict[str, Any]:
    cases, source = load_v286_cases(
        source_path, expected_source_sha256=expected_source_sha256
    )
    work_item_ids = [case["work_item_id"] for case in cases]
    control, control_observation = read_control_projection(
        control_db_path, work_item_ids
    )
    readback, readback_observation = load_official_readback(official_readback_path)
    upgrade_contract, upgrade_observation = load_attribution_upgrade_contract(
        attribution_upgrade_path,
        work_item_ids=work_item_ids,
    )
    k286: list[dict[str, Any]] = []
    enriched: list[dict[str, Any]] = []
    for case in cases:
        work_item_id = case["work_item_id"]
        control_case = control.get(work_item_id, {})
        events = list(control_case.get("events") or [])
        trigger = _current_trigger(control_case.get("triggers") or [])
        execution_identity = {
            "business_key": str(trigger.get("business_key") or "") or None,
            "submission_key": str(trigger.get("submission_key") or "") or None,
            "generation": trigger.get("generation"),
            "trigger_state": str(trigger.get("state") or "") or None,
        } if isinstance(trigger, Mapping) else {
            "business_key": None,
            "submission_key": None,
            "generation": None,
            "trigger_state": None,
        }
        readback_case = readback.get(work_item_id)
        official_case = readback_case if isinstance(readback_case, Mapping) else None
        effects = list(control_case.get("effects") or [])
        classification, classification_proof = _upgrade_classification(
            official=official_case,
            effects=effects,
            upgrade_contract=upgrade_contract,
            upgrade_observation=upgrade_observation,
        )
        fresh_report_present = (
            official_case.get("report_present") is True
            if official_case is not None
            else case["existing_report_present"]
        )
        enriched.append(
            {
                **case,
                "source_snapshot_report_present": case["existing_report_present"],
                "existing_report_present": fresh_report_present,
                "execution_identity": execution_identity,
                "existing_effect": _effect_projection(
                    effects,
                    snapshot_report_present=fresh_report_present,
                ),
                "current_attribution": _current_attribution_projection(
                    effects,
                    official_case,
                ),
                "attribution_upgrade": {
                    "classification": classification,
                    "target_version": (
                        str(upgrade_contract.get("version_id") or "")
                        if isinstance(upgrade_contract, Mapping)
                        else None
                    ),
                    "target_contract_sha256": upgrade_observation.get("sha256"),
                    "proof": classification_proof,
                    "next_identity": _canonical_next_identity(
                        trigger,
                        work_item_id=work_item_id,
                    ),
                },
                "official_readback": (
                    dict(readback_case)
                    if isinstance(readback_case, Mapping)
                    else {
                        "classification": "missing",
                        "comments_complete": False,
                        "comment_count": 0,
                        "report_present": case["existing_report_present"],
                        "result_present": False,
                    }
                ),
            }
        )
        k286.append(
            {
                "work_item_id": work_item_id,
                "linkage_status": "official_event_linked" if events else "kafka_event_missing",
                "official_event_count": len(events),
                "official_events": events,
            }
        )
    s16 = select_s16(enriched)
    report_present = sum(1 for case in enriched if case["existing_report_present"])
    linked = sum(1 for case in k286 if case["official_event_count"])
    category_counts = Counter(case["category"] for case in enriched)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": utc_now(),
        "contract": {
            "creator_key": CREATOR_KEY,
            "creator_name": CREATOR_NAME,
            "project_field_key": PROJECT_FIELD_KEY,
            "project_name_prefix": PROJECT_NAME_PREFIX,
            "operator_filter": None,
            "allowed_project_option_ids": list(ALLOWED_PROJECT_OPTION_IDS),
            "expected_count": EXPECTED_CASE_COUNT,
        },
        "source_snapshot": source,
        "control_observation": control_observation,
        "official_readback_observation": readback_observation,
        "attribution_upgrade_observation": upgrade_observation,
        "attribution_upgrade_contract": upgrade_contract,
        "sets": {
            "V286": {
                "count": len(enriched),
                "cases": enriched,
                "summary": {
                    "frame_present": sum(
                        case["frame_status"] == "present" for case in enriched
                    ),
                    "pdcl_true": sum(
                        case["pdcl_status"] == "true" for case in enriched
                    ),
                    "existing_report_present": report_present,
                    "existing_report_absent": len(enriched) - report_present,
                    "rewrite_update": sum(
                        case["attribution_upgrade"]["classification"]
                        == "rewrite_update"
                        for case in enriched
                    ),
                    "create": sum(
                        case["attribution_upgrade"]["classification"] == "create"
                        for case in enriched
                    ),
                    "already_current": sum(
                        case["attribution_upgrade"]["classification"]
                        == "already_current"
                        for case in enriched
                    ),
                    "categories": dict(sorted(category_counts.items())),
                },
            },
            "K286": {
                "count": len(k286),
                "linked_count": linked,
                "missing_count": len(k286) - linked,
                "cases": k286,
                "coverage_claim": "frozen_case_linkage_not_live_consumer_coverage",
            },
            "S16": {
                "count": len(s16),
                "cases": s16,
                "selection": "4 categories x (2 report-present + 2 report-absent)",
            },
        },
        "privacy": {
            "raw_titles_persisted": False,
            "pdcl_addresses_persisted": False,
            "raw_kafka_payloads_persisted": False,
            "comment_bodies_persisted": False,
            "credential_values_persisted": False,
        },
        "write_policy": {
            "default_mode": "offline",
            "external_writes_default": False,
            "read_before_write_required": True,
            "read_after_write_required": True,
            "generation_effect_key_required": True,
            "rollback_receipt_required": True,
            "official_readback_must_refresh_before_write": True,
            "effect_key_must_derive_from_sealed_vm_delivery_payload": True,
        },
        "validation": {
            "passed": (
                len(enriched) == EXPECTED_CASE_COUNT
                and len(k286) == EXPECTED_CASE_COUNT
                and len(s16) == 16
                and all(case["frame_status"] == "present" for case in enriched)
                and all(case["pdcl_status"] == "true" for case in enriched)
            ),
            "operator_filter_is_null": True,
            "source_sha256_matches": source["sha256"] == expected_source_sha256,
            "unique_v286_ids": len({case["work_item_id"] for case in enriched}),
            "fresh_official_readback_complete": (
                not readback_observation.get("configured")
                or (
                    readback_observation.get("result_count") == EXPECTED_CASE_COUNT
                    and all(
                        case["official_readback"]["comments_complete"] is True
                        for case in enriched
                    )
                )
            ),
            "canonical_next_submission_identity_count": sum(
                case["attribution_upgrade"]["next_identity"]["submission_key"]
                is not None
                for case in enriched
            ),
            "external_side_effects": {
                "feishu_writes": 0,
                "control_db_writes": 0,
                "kafka_commits": 0,
                "vm_submissions": 0,
            },
        },
    }
    if not manifest["validation"]["passed"]:
        raise ValidationHarnessError("validation_manifest_gate_failed")
    return manifest


def _authority_projection(
    authority: Mapping[str, Any] | None,
    *,
    mode: str,
    manifest_sha256: str,
    s16_ids: set[str],
) -> tuple[bool, dict[str, Any]]:
    if authority is None:
        return False, {"status": "missing", "reason": "owner_authority_required"}
    cases = authority.get("cases")
    case_ids = {
        str(case.get("work_item_id") or "")
        for case in cases
        if isinstance(case, Mapping)
    } if isinstance(cases, list) else set()
    valid_cases = bool(cases) and all(
        isinstance(case, Mapping)
        and str(case.get("work_item_id") or "") in s16_ids
        and isinstance(case.get("generation"), int)
        and not isinstance(case.get("generation"), bool)
        and case.get("generation") > 0
        and bool(str(case.get("effect_key") or "").strip())
        and bool(str(case.get("rollback_receipt_path") or "").strip())
        for case in cases or []
    )
    valid = (
        authority.get("schema_version") == AUTHORITY_SCHEMA_VERSION
        and authority.get("authorized") is True
        and authority.get("mode") == mode
        and authority.get("validation_manifest_sha256") == manifest_sha256
        and valid_cases
        and (mode != "canary_write" or len(case_ids) == 1)
    )
    return valid, {
        "status": "valid" if valid else "invalid",
        "schema_version": authority.get("schema_version"),
        "mode": authority.get("mode"),
        "case_count": len(case_ids),
        "authority_sha256": sha256_bytes(canonical_bytes(authority)),
    }


def build_mode_receipt(
    manifest: Mapping[str, Any],
    *,
    manifest_sha256: str,
    mode: str,
    authority: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if mode not in MODES:
        raise ValidationHarnessError("validation_mode_invalid", mode)
    sets = manifest.get("sets")
    s16 = sets.get("S16") if isinstance(sets, Mapping) else None
    s16_cases = s16.get("cases") if isinstance(s16, Mapping) else None
    if not isinstance(s16_cases, list) or len(s16_cases) != 16:
        raise ValidationHarnessError("validation_manifest_s16_invalid")
    s16_ids = {str(case.get("work_item_id") or "") for case in s16_cases}
    authority_ok, authority_receipt = _authority_projection(
        authority,
        mode=mode,
        manifest_sha256=manifest_sha256,
        s16_ids=s16_ids,
    ) if mode in WRITE_MODES else (False, {"status": "not_required"})
    if mode == "offline":
        status = "completed"
    elif mode == "shadow":
        status = "shadow_projected"
    elif authority_ok:
        status = "ready_for_canonical_executor"
    else:
        status = "pending_approval"
    stage_receipts = []
    k_cases = {
        case["work_item_id"]: case
        for case in manifest["sets"]["K286"]["cases"]
    }
    for case in s16_cases:
        work_item_id = case["work_item_id"]
        linkage = k_cases[work_item_id]
        ingress_status = (
            "official_event_fixture_ready"
            if linkage["linkage_status"] == "official_event_linked"
            else "blocked_kafka_event_missing"
        )
        stage_receipts.append(
            {
                "work_item_id": work_item_id,
                "ingress": ingress_status,
                "admission": (
                    "offline_validated" if mode == "offline" else "shadow_simulated"
                ),
                "vm": "not_submitted",
                "collector": "not_executed",
                "delivery": "not_executed",
                "readback": "required_before_any_write",
                "duplicate_check": "no_new_effect",
                "terminal_status": (
                    "offline_ready"
                    if ingress_status == "official_event_fixture_ready"
                    else "blocked_incomplete"
                ),
            }
        )
    return {
        "schema_version": MODE_RECEIPT_SCHEMA_VERSION,
        "generated_at": utc_now(),
        "mode": mode,
        "status": status,
        "validation_manifest_sha256": manifest_sha256,
        "authority": authority_receipt,
        "canonical_executor_invoked": False,
        "stage_receipts": stage_receipts,
        "external_side_effects": {
            "feishu_writes": 0,
            "control_db_writes": 0,
            "kafka_commits": 0,
            "vm_submissions": 0,
        },
        "next_action": (
            "review exact owner authority, refresh read-before-write, then use the "
            "governed canonical executor"
            if status == "pending_approval"
            else "hand this receipt to the governed canonical executor"
            if status == "ready_for_canonical_executor"
            else "retain as zero-side-effect validation evidence"
        ),
    }


def _write_json(path: Path, value: Mapping[str, Any]) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = canonical_bytes(value) + b"\n"
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    return {"path": str(path), "sha256": sha256_bytes(raw), "bytes": len(raw)}


def _load_authority(path: Path | None) -> Mapping[str, Any] | None:
    if path is None:
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValidationHarnessError("write_authority_unreadable", str(exc)) from exc
    if not isinstance(value, Mapping):
        raise ValidationHarnessError("write_authority_invalid")
    return value


def load_sealed_manifest(path: Path) -> tuple[dict[str, Any], str]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValidationHarnessError("sealed_manifest_unreadable", str(exc)) from exc
    if not isinstance(value, dict):
        raise ValidationHarnessError("sealed_manifest_invalid")
    contract = value.get("contract")
    sets = value.get("sets")
    expected_contract = {
        "creator_key": CREATOR_KEY,
        "creator_name": CREATOR_NAME,
        "project_field_key": PROJECT_FIELD_KEY,
        "project_name_prefix": PROJECT_NAME_PREFIX,
        "operator_filter": None,
        "allowed_project_option_ids": list(ALLOWED_PROJECT_OPTION_IDS),
        "expected_count": EXPECTED_CASE_COUNT,
    }
    if (
        value.get("schema_version") != SCHEMA_VERSION
        or contract != expected_contract
        or not isinstance(sets, Mapping)
    ):
        raise ValidationHarnessError("sealed_manifest_invalid")
    for name, count in (("V286", 286), ("K286", 286), ("S16", 16)):
        item = sets.get(name)
        if (
            not isinstance(item, Mapping)
            or item.get("count") != count
            or not isinstance(item.get("cases"), list)
            or len(item["cases"]) != count
        ):
            raise ValidationHarnessError("sealed_manifest_invalid", name)
    if canonical_bytes(value) + b"\n" != raw:
        raise ValidationHarnessError("sealed_manifest_not_canonical")
    return value, sha256_bytes(raw)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--source", type=Path)
    source.add_argument("--receipt-only-manifest", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mode", choices=MODES, default="offline")
    parser.add_argument("--control-db", type=Path)
    parser.add_argument("--official-readback", type=Path)
    parser.add_argument("--attribution-upgrade", type=Path)
    parser.add_argument("--authority", type=Path)
    parser.add_argument(
        "--expected-source-sha256", default=EXPECTED_SOURCE_SHA256
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.receipt_only_manifest is not None:
            if (
                args.control_db is not None
                or args.official_readback is not None
                or args.attribution_upgrade is not None
            ):
                raise ValidationHarnessError("receipt_only_readback_not_allowed")
            manifest, manifest_sha256 = load_sealed_manifest(
                args.receipt_only_manifest
            )
            manifest_artifact = {
                "path": str(args.receipt_only_manifest),
                "sha256": manifest_sha256,
                "bytes": args.receipt_only_manifest.stat().st_size,
                "rewritten": False,
            }
        else:
            manifest = build_validation_manifest(
                args.source,
                expected_source_sha256=args.expected_source_sha256,
                control_db_path=args.control_db,
                official_readback_path=args.official_readback,
                attribution_upgrade_path=args.attribution_upgrade,
            )
            manifest_artifact = _write_json(args.output, manifest)
        mode_receipt = build_mode_receipt(
            manifest,
            manifest_sha256=manifest_artifact["sha256"],
            mode=args.mode,
            authority=_load_authority(args.authority),
        )
        receipt_path = (
            args.output
            if args.receipt_only_manifest is not None
            else args.output.with_name(f"{args.output.stem}-{args.mode}-receipt.json")
        )
        receipt_artifact = _write_json(receipt_path, mode_receipt)
        print(
            json.dumps(
                {
                    "success": mode_receipt["status"] != "pending_approval",
                    "status": mode_receipt["status"],
                    "manifest": manifest_artifact,
                    "mode_receipt": receipt_artifact,
                },
                ensure_ascii=True,
                sort_keys=True,
            )
        )
        return 3 if mode_receipt["status"] == "pending_approval" else 0
    except ValidationHarnessError as exc:
        print(
            json.dumps(
                {"success": False, "error_code": exc.code, "detail": exc.detail},
                ensure_ascii=True,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
