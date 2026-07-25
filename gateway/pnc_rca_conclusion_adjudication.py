"""Durable recognition and retraction of published PNC RCA conclusions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
from typing import Any, Literal, Mapping

from gateway.pnc_rca_delivery_contract import (
    DELIVERY_EFFECT_KIND,
    MAX_FEISHU_COMMENT_BYTES,
    RCA_RESULT_FIELD_KEY,
    compute_delivery_effect_key,
    delivery_effect_marker,
)
from gateway.pnc_rca_quality_oracle import evaluate_structural_tier


ADJUDICATION_SCHEMA_VERSION = "pnc_rca_conclusion_adjudication_v1"
ADJUDICATION_EFFECT_SCHEMA_VERSION_V1 = "pnc_rca_conclusion_adjudication_effect_v1"
ADJUDICATION_EFFECT_SCHEMA_VERSION = "pnc_rca_conclusion_adjudication_effect_v2"
ADJUDICATION_EFFECT_TARGET_PREFIX = "g1q3-rca-adjudication-target-v1"
ADJUDICATION_ID_PREFIX = "g1q3-rca-adjudication-v1"
ADJUDICATION_ACTIONS = frozenset({"retract", "recognize"})
ADJUDICATION_STATES = {
    "retract": "invalidated",
    "recognize": "recognized",
}
_WORK_ITEM_ID_RE = re.compile(r"^[0-9]{1,32}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
ADJUDICATION_ARTIFACT_RECEIPT_SCHEMA_VERSION = (
    "g1q3_rca_owner_review_artifact_receipt_v1"
)
MAX_ADJUDICATION_RECEIPT_LINE_BYTES = 1024 * 1024
_ARTIFACT_RECEIPT_FIELDS = frozenset(
    {
        "schema_version",
        "path",
        "offset",
        "length",
        "sha256",
        "device",
        "inode",
        "review_event_id",
    }
)


class ConclusionAdjudicationError(RuntimeError):
    """A conclusion adjudication could not be recorded safely."""


@dataclass(frozen=True)
class ConclusionAdjudicationResult:
    adjudication_id: str
    action: str
    conclusion_state: str
    business_key: str
    generation: int
    work_item_id: str
    original_delivery_id: str
    original_effect_key: str
    correction_effect_key: str
    created: bool
    created_at: str
    impact_lineage: dict[str, Any]
    artifact_repair_status: str = "pending"


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _stable_key(prefix: str, material: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(_canonical_json(material).encode("utf-8")).hexdigest()
    return f"{prefix}-{digest}"


def _clean_text(value: Any, *, field: str, maximum: int, required: bool) -> str:
    text = str(value or "").strip()
    if required and not text:
        raise ConclusionAdjudicationError(f"conclusion_adjudication_{field}_required")
    if (
        len(text) > maximum
        or _CONTROL_RE.search(text)
        or "\n" in text
        or "\r" in text
    ):
        raise ConclusionAdjudicationError(f"conclusion_adjudication_{field}_invalid")
    return text


def _json_object(value: Any, *, field: str) -> dict[str, Any]:
    try:
        parsed = json.loads(str(value or "{}"))
    except (TypeError, json.JSONDecodeError) as exc:
        raise ConclusionAdjudicationError(
            f"conclusion_adjudication_{field}_invalid"
        ) from exc
    if not isinstance(parsed, dict):
        raise ConclusionAdjudicationError(f"conclusion_adjudication_{field}_invalid")
    return parsed


def _execute_schema_script(conn: sqlite3.Connection, script: str) -> None:
    pending = ""
    for line in script.splitlines(keepends=True):
        pending += line
        if sqlite3.complete_statement(pending):
            statement = pending.strip()
            if statement:
                conn.execute(statement)
            pending = ""
    if pending.strip():
        raise RuntimeError("incomplete_conclusion_adjudication_schema_script")


def _normalized_artifact_receipt(value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != _ARTIFACT_RECEIPT_FIELDS:
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_artifact_receipt_shape_invalid"
        )
    path = Path(str(value.get("path") or "")).expanduser()
    if not path.is_absolute() or str(path) != str(path.absolute()):
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_artifact_receipt_path_invalid"
        )
    offset = value.get("offset")
    length = value.get("length")
    device = value.get("device")
    inode = value.get("inode")
    if (
        value.get("schema_version")
        != ADJUDICATION_ARTIFACT_RECEIPT_SCHEMA_VERSION
        or isinstance(offset, bool)
        or not isinstance(offset, int)
        or offset < 0
        or isinstance(length, bool)
        or not isinstance(length, int)
        or length <= 1
        or length > MAX_ADJUDICATION_RECEIPT_LINE_BYTES
        or isinstance(device, bool)
        or not isinstance(device, int)
        or device <= 0
        or isinstance(inode, bool)
        or not isinstance(inode, int)
        or inode <= 0
        or _SHA256_RE.fullmatch(str(value.get("sha256") or "")) is None
        or not str(value.get("review_event_id") or "").strip()
    ):
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_artifact_receipt_shape_invalid"
        )
    return {
        "schema_version": ADJUDICATION_ARTIFACT_RECEIPT_SCHEMA_VERSION,
        "path": str(path),
        "offset": offset,
        "length": length,
        "sha256": str(value["sha256"]),
        "device": device,
        "inode": inode,
        "review_event_id": str(value["review_event_id"]),
    }


def validate_conclusion_adjudication_artifact_receipt(
    value: Mapping[str, Any], *, adjudication: Mapping[str, Any]
) -> dict[str, Any]:
    """Recompute one immutable owner-review JSONL line and its ledger binding."""

    normalized = _normalized_artifact_receipt(value)
    path = Path(normalized["path"])
    if not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "pread"):
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_artifact_receipt_nofollow_unavailable"
        )
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
        )
    except OSError as exc:
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_artifact_receipt_unavailable"
        ) from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_dev != normalized["device"]
            or before.st_ino != normalized["inode"]
            or before.st_size < normalized["offset"] + normalized["length"]
        ):
            raise ConclusionAdjudicationError(
                "conclusion_adjudication_artifact_receipt_identity_invalid"
            )
        raw = os.pread(descriptor, normalized["length"], normalized["offset"])
        after = os.fstat(descriptor)
        lexical = os.lstat(path)
        if (
            len(raw) != normalized["length"]
            or stat.S_ISLNK(lexical.st_mode)
            or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
            != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
            or (after.st_dev, after.st_ino) != (lexical.st_dev, lexical.st_ino)
        ):
            raise ConclusionAdjudicationError(
                "conclusion_adjudication_artifact_receipt_identity_invalid"
            )
    except ConclusionAdjudicationError:
        raise
    except OSError as exc:
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_artifact_receipt_unavailable"
        ) from exc
    finally:
        os.close(descriptor)
    if (
        not raw.endswith(b"\n")
        or raw.count(b"\n") != 1
        or hashlib.sha256(raw).hexdigest() != normalized["sha256"]
    ):
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_artifact_receipt_hash_invalid"
        )
    def unique_object(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, item in items:
            if key in result:
                raise ConclusionAdjudicationError(
                    "conclusion_adjudication_artifact_receipt_json_invalid"
                )
            result[key] = item
        return result

    def invalid_number(_value: str) -> None:
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_artifact_receipt_json_invalid"
        )

    try:
        receipt = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=unique_object,
            parse_constant=invalid_number,
        )
    except ConclusionAdjudicationError:
        raise
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_artifact_receipt_json_invalid"
        ) from exc
    try:
        source = json.loads(str(adjudication.get("source_json") or ""))
        lineage = json.loads(str(adjudication.get("lineage_json") or ""))
    except (TypeError, json.JSONDecodeError) as exc:
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_artifact_receipt_ledger_invalid"
        ) from exc
    expected_action = {"retract": "撤回", "recognize": "通过"}.get(
        str(adjudication.get("action") or ""), ""
    )
    expected_verdict = {"retract": "retracted", "recognize": "approved"}.get(
        str(adjudication.get("action") or ""), ""
    )
    if not isinstance(receipt, dict):
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_artifact_receipt_json_invalid"
        )
    event_material = {
        key: receipt.get(key)
        for key in (
            "issue_id",
            "action",
            "reason",
            "owner_id",
            "adjudication_id",
            "source",
        )
    }
    expected_event_id = "g1q3-rca-owner-review-v1-" + hashlib.sha256(
        _canonical_json(event_material).encode("utf-8")
    ).hexdigest()
    receipt_parent = path.parent
    ledger_path = Path(str(receipt.get("ledger_path") or ""))
    sidecar_path = Path(str(receipt.get("business_state_sidecar_path") or ""))
    if (
        receipt.get("schema_version") != "g1q3_rca_owner_review_v1"
        or receipt.get("event_type") != "owner_review"
        or receipt.get("review_event_id") != normalized["review_event_id"]
        or receipt.get("review_event_id") != expected_event_id
        or receipt.get("adjudication_id") != adjudication.get("adjudication_id")
        or receipt.get("issue_id") != adjudication.get("work_item_id")
        or receipt.get("action") != expected_action
        or receipt.get("verdict") != expected_verdict
        or receipt.get("reason") != adjudication.get("reason")
        or receipt.get("owner_id") != adjudication.get("actor_id")
        or receipt.get("owner_name") != adjudication.get("actor_name")
        or receipt.get("reviewed_at") != adjudication.get("created_at")
        or receipt.get("original_effect_key")
        != adjudication.get("original_effect_key")
        or receipt.get("correction_effect_key")
        != adjudication.get("correction_effect_key")
        or receipt.get("conclusion_state")
        != adjudication.get("conclusion_state")
        or receipt.get("source") != source
        or receipt.get("impact_lineage") != lineage
        or receipt.get("override") is not True
        or not ledger_path.is_absolute()
        or ledger_path != receipt_parent / "ledger.json"
        or not sidecar_path.is_absolute()
        or sidecar_path
        != receipt_parent
        / "business-states"
        / f"G1Q3-{adjudication.get('work_item_id')}.business-state.yaml"
    ):
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_artifact_receipt_ledger_mismatch"
        )
    return normalized


def ensure_conclusion_adjudication_schema(conn: sqlite3.Connection) -> None:
    """Install the immutable adjudication ledger in the delivery database."""
    _execute_schema_script(
        conn,
        """
        CREATE TABLE IF NOT EXISTS rca_conclusion_adjudications (
            adjudication_id TEXT PRIMARY KEY,
            schema_version TEXT NOT NULL,
            business_key TEXT NOT NULL UNIQUE,
            generation INTEGER NOT NULL CHECK (generation >= 1),
            project_key TEXT NOT NULL,
            work_item_type_key TEXT NOT NULL,
            work_item_id TEXT NOT NULL,
            action TEXT NOT NULL CHECK (action IN ('retract', 'recognize')),
            conclusion_state TEXT NOT NULL CHECK (
                conclusion_state IN ('invalidated', 'recognized')
            ),
            reason TEXT NOT NULL,
            replacement_conclusion TEXT NOT NULL DEFAULT '',
            actor_id TEXT NOT NULL,
            actor_name TEXT NOT NULL DEFAULT '',
            source_json TEXT NOT NULL,
            original_delivery_id TEXT NOT NULL,
            original_effect_key TEXT NOT NULL UNIQUE,
            correction_effect_key TEXT NOT NULL UNIQUE,
            activation_epoch_id TEXT NOT NULL DEFAULT '',
            evaluator_refs_json TEXT NOT NULL,
            responsibility_domain TEXT NOT NULL,
            impact_window_start TEXT NOT NULL,
            impact_window_end TEXT NOT NULL,
            lineage_json TEXT NOT NULL,
            lineage_sha256 TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(original_delivery_id)
                REFERENCES rca_delivery_jobs(delivery_id),
            FOREIGN KEY(original_effect_key)
                REFERENCES rca_delivery_effects(effect_key),
            FOREIGN KEY(correction_effect_key)
                REFERENCES rca_delivery_effects(effect_key)
        );

        CREATE INDEX IF NOT EXISTS idx_rca_conclusion_adjudication_impact
            ON rca_conclusion_adjudications(
                responsibility_domain, impact_window_start, impact_window_end
            );

        CREATE TRIGGER IF NOT EXISTS trg_rca_conclusion_adjudication_no_update
        BEFORE UPDATE ON rca_conclusion_adjudications
        BEGIN
            SELECT RAISE(ABORT, 'rca_conclusion_adjudication_immutable');
        END;

        CREATE TRIGGER IF NOT EXISTS trg_rca_conclusion_adjudication_no_delete
        BEFORE DELETE ON rca_conclusion_adjudications
        BEGIN
            SELECT RAISE(ABORT, 'rca_conclusion_adjudication_immutable');
        END;

        CREATE TABLE IF NOT EXISTS rca_conclusion_adjudication_repairs (
            adjudication_id TEXT PRIMARY KEY,
            status TEXT NOT NULL CHECK (status IN ('pending', 'succeeded')),
            attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
            last_error_code TEXT NOT NULL DEFAULT '',
            last_error_detail TEXT NOT NULL DEFAULT '',
            receipt_schema_version TEXT NOT NULL DEFAULT '',
            receipt_path TEXT NOT NULL DEFAULT '',
            receipt_offset INTEGER NOT NULL DEFAULT -1 CHECK (receipt_offset >= -1),
            receipt_length INTEGER NOT NULL DEFAULT 0 CHECK (receipt_length >= 0),
            receipt_sha256 TEXT NOT NULL DEFAULT '',
            receipt_device INTEGER NOT NULL DEFAULT 0 CHECK (receipt_device >= 0),
            receipt_inode INTEGER NOT NULL DEFAULT 0 CHECK (receipt_inode >= 0),
            receipt_event_id TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            completed_at TEXT,
            FOREIGN KEY(adjudication_id)
                REFERENCES rca_conclusion_adjudications(adjudication_id)
        );

        CREATE INDEX IF NOT EXISTS idx_rca_conclusion_adjudication_repairs_status
            ON rca_conclusion_adjudication_repairs(status, updated_at);
        """,
    )
    repair_columns = {
        str(row[1])
        for row in conn.execute(
            "PRAGMA table_info(rca_conclusion_adjudication_repairs)"
        ).fetchall()
    }
    for name, definition in {
        "receipt_schema_version": "TEXT NOT NULL DEFAULT ''",
        "receipt_path": "TEXT NOT NULL DEFAULT ''",
        "receipt_offset": (
            "INTEGER NOT NULL DEFAULT -1 CHECK (receipt_offset >= -1)"
        ),
        "receipt_length": "INTEGER NOT NULL DEFAULT 0 CHECK (receipt_length >= 0)",
        "receipt_sha256": "TEXT NOT NULL DEFAULT ''",
        "receipt_device": "INTEGER NOT NULL DEFAULT 0 CHECK (receipt_device >= 0)",
        "receipt_inode": "INTEGER NOT NULL DEFAULT 0 CHECK (receipt_inode >= 0)",
        "receipt_event_id": "TEXT NOT NULL DEFAULT ''",
    }.items():
        if name not in repair_columns:
            conn.execute(
                "ALTER TABLE rca_conclusion_adjudication_repairs "
                f"ADD COLUMN {name} {definition}"
            )
    current = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT OR IGNORE INTO rca_conclusion_adjudication_repairs(
            adjudication_id, status, created_at, updated_at
        )
        SELECT adjudication_id, 'pending', created_at, ?
          FROM rca_conclusion_adjudications
        """,
        (current,),
    )


def validate_conclusion_adjudication_schema(conn: sqlite3.Connection) -> None:
    effect_columns = {
        str(row[1])
        for row in conn.execute("PRAGMA table_info(rca_delivery_effects)").fetchall()
    }
    if not {
        "adjudication_comment_attempt_count",
        "adjudication_comment_attempted_at",
    }.issubset(effect_columns):
        raise RuntimeError("rca_conclusion_adjudication_schema_not_current")
    required_columns = {
        "adjudication_id",
        "schema_version",
        "business_key",
        "generation",
        "project_key",
        "work_item_type_key",
        "work_item_id",
        "action",
        "conclusion_state",
        "reason",
        "replacement_conclusion",
        "actor_id",
        "actor_name",
        "source_json",
        "original_delivery_id",
        "original_effect_key",
        "correction_effect_key",
        "activation_epoch_id",
        "evaluator_refs_json",
        "responsibility_domain",
        "impact_window_start",
        "impact_window_end",
        "lineage_json",
        "lineage_sha256",
        "created_at",
    }
    observed_columns = {
        str(row[1])
        for row in conn.execute(
            "PRAGMA table_info(rca_conclusion_adjudications)"
        ).fetchall()
    }
    if observed_columns != required_columns:
        raise RuntimeError("rca_conclusion_adjudication_schema_not_current")
    repair_columns = {
        "adjudication_id",
        "status",
        "attempt_count",
        "last_error_code",
        "last_error_detail",
        "receipt_schema_version",
        "receipt_path",
        "receipt_offset",
        "receipt_length",
        "receipt_sha256",
        "receipt_device",
        "receipt_inode",
        "receipt_event_id",
        "created_at",
        "updated_at",
        "completed_at",
    }
    observed_repair_columns = {
        str(row[1])
        for row in conn.execute(
            "PRAGMA table_info(rca_conclusion_adjudication_repairs)"
        ).fetchall()
    }
    if observed_repair_columns != repair_columns:
        raise RuntimeError("rca_conclusion_adjudication_schema_not_current")
    triggers = {
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'trigger' "
            "AND tbl_name = 'rca_conclusion_adjudications'"
        ).fetchall()
    }
    if not {
        "trg_rca_conclusion_adjudication_no_update",
        "trg_rca_conclusion_adjudication_no_delete",
    }.issubset(triggers):
        raise RuntimeError("rca_conclusion_adjudication_schema_not_current")

    unique_column_sets = {
        tuple(
            str(info[2])
            for info in conn.execute(f"PRAGMA index_info({row[1]})").fetchall()
        )
        for row in conn.execute(
            "PRAGMA index_list(rca_conclusion_adjudications)"
        ).fetchall()
        if int(row[2]) == 1
    }
    if not {
        ("business_key",),
        ("original_effect_key",),
        ("correction_effect_key",),
    }.issubset(unique_column_sets):
        raise RuntimeError("rca_conclusion_adjudication_schema_not_current")

    foreign_keys = {
        (str(row[3]), str(row[2]), str(row[4]))
        for row in conn.execute(
            "PRAGMA foreign_key_list(rca_conclusion_adjudications)"
        ).fetchall()
    }
    if foreign_keys != {
        ("original_delivery_id", "rca_delivery_jobs", "delivery_id"),
        ("original_effect_key", "rca_delivery_effects", "effect_key"),
        ("correction_effect_key", "rca_delivery_effects", "effect_key"),
    }:
        raise RuntimeError("rca_conclusion_adjudication_schema_not_current")
    repair_foreign_keys = {
        (str(row[3]), str(row[2]), str(row[4]))
        for row in conn.execute(
            "PRAGMA foreign_key_list(rca_conclusion_adjudication_repairs)"
        ).fetchall()
    }
    if repair_foreign_keys != {
        ("adjudication_id", "rca_conclusion_adjudications", "adjudication_id")
    }:
        raise RuntimeError("rca_conclusion_adjudication_schema_not_current")
    invalid = conn.execute(
        """
        SELECT 1
          FROM rca_conclusion_adjudications AS a
     LEFT JOIN rca_conclusion_adjudication_repairs AS r
            ON r.adjudication_id = a.adjudication_id
         WHERE a.schema_version != ?
            OR TRIM(a.activation_epoch_id) = ''
            OR r.adjudication_id IS NULL
            OR (
                r.status = 'succeeded'
                AND (
                    r.receipt_schema_version != ?
                    OR TRIM(r.receipt_path) = ''
                    OR r.receipt_offset < 0
                    OR r.receipt_length <= 0
                    OR LENGTH(r.receipt_sha256) != 64
                    OR r.receipt_device <= 0
                    OR r.receipt_inode <= 0
                    OR TRIM(r.receipt_event_id) = ''
                )
            )
         LIMIT 1
        """,
        (
            ADJUDICATION_SCHEMA_VERSION,
            ADJUDICATION_ARTIFACT_RECEIPT_SCHEMA_VERSION,
        ),
    ).fetchone()
    if invalid is not None:
        raise RuntimeError("rca_conclusion_adjudication_schema_not_current")


def is_adjudication_effect_payload(payload: Mapping[str, Any]) -> bool:
    return payload.get("schema_version") in {
        ADJUDICATION_EFFECT_SCHEMA_VERSION_V1,
        ADJUDICATION_EFFECT_SCHEMA_VERSION,
    }


def identifies_adjudication_effect(
    payload: Mapping[str, Any], *, target_key: str
) -> bool:
    """Recognize current, legacy, and schema-laundered correction effects."""

    return is_adjudication_effect_payload(payload) or str(target_key).startswith(
        f"{ADJUDICATION_EFFECT_TARGET_PREFIX}-"
    )


def _impact_lineage(row: sqlite3.Row) -> dict[str, Any]:
    contract = _json_object(row["contract_json"], field="contract")
    oracle = evaluate_structural_tier(contract)
    evaluator_refs = list(oracle.facts.supported_evaluator_keys)[:64]
    responsibility_domain = str(oracle.facts.responsibility or "").strip()
    start = str(row["effect_created_at"] or row["job_created_at"] or "").strip()
    end = str(row["effect_completed_at"] or start).strip()
    if not start or not end:
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_impact_window_unavailable"
        )
    if not evaluator_refs:
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_evaluator_refs_unresolved"
        )
    if not responsibility_domain or responsibility_domain.lower() == "unresolved":
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_responsibility_domain_unresolved"
        )
    return {
        "schema_version": "pnc_rca_conclusion_impact_lineage_v1",
        "business_key": str(row["business_key"]),
        "generation": int(row["generation"]),
        "project_key": str(row["project_key"]),
        "work_item_type_key": str(row["work_item_type_key"]),
        "work_item_id": str(row["work_item_id"]),
        "original_delivery_id": str(row["delivery_id"]),
        "original_effect_key": str(row["effect_key"]),
        "evaluator_refs": evaluator_refs,
        "evaluator_resolution": "resolved",
        "responsibility_domain": responsibility_domain,
        "impact_window": {"start": start, "end": end},
    }


def _adjudication_content(
    *,
    marker: str,
    action: str,
    work_item_id: str,
    original_effect_key: str,
    reason: str,
    replacement_conclusion: str,
) -> tuple[str, str]:
    # Free-form owner input is internal audit evidence, never publication text.
    del original_effect_key, reason, replacement_conclusion
    if action != "retract":
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_recognize_publication_contract_unavailable"
        )
    field_value = "原自动 RCA 结论已撤回并标记作废；不可作为定责依据。"
    lines = [
        marker,
        "【RCA 更正】原自动分析结论已撤回并标记作废。",
        f"问题：{work_item_id}",
        "原结论不可作为定责依据；后续以重新复核后的结论为准。",
        "更正依据已保留在内部不可变审计记录中。",
    ]
    content = "\n".join(lines)
    if len(content.encode("utf-8")) > MAX_FEISHU_COMMENT_BYTES:
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_comment_too_large"
        )
    return content, field_value


def _build_effect(
    *,
    row: sqlite3.Row,
    adjudication_id: str,
    action: str,
    reason: str,
    replacement_conclusion: str,
    actor_id: str,
    actor_name: str,
    source: Mapping[str, str],
    activation_epoch_id: str,
    lineage: Mapping[str, Any],
) -> tuple[str, str, dict[str, Any]]:
    target_key = _stable_key(
        ADJUDICATION_EFFECT_TARGET_PREFIX,
        {
            "adjudication_id": adjudication_id,
            "original_target_key": str(row["target_key"]),
        },
    )
    semantic = {
        "schema_version": ADJUDICATION_EFFECT_SCHEMA_VERSION,
        "delivery_id": str(row["delivery_id"]),
        "effect_kind": DELIVERY_EFFECT_KIND,
        "target_key": target_key,
        "project_key": str(row["project_key"]),
        "work_item_type_key": str(row["work_item_type_key"]),
        "work_item_id": str(row["work_item_id"]),
        "business_key": str(row["business_key"]),
        "generation": int(row["generation"]),
        "adjudication_id": adjudication_id,
        "action": action,
        "conclusion_state": ADJUDICATION_STATES[action],
        "original_delivery_id": str(row["delivery_id"]),
        "original_effect_key": str(row["effect_key"]),
        "original_target_key": str(row["target_key"]),
        "reason": reason,
        "replacement_conclusion": replacement_conclusion,
        "actor_id": actor_id,
        "actor_name": actor_name,
        "activation_epoch_id": activation_epoch_id,
        "source_sha256": hashlib.sha256(
            _canonical_json(source).encode("utf-8")
        ).hexdigest(),
        "lineage_sha256": hashlib.sha256(
            _canonical_json(lineage).encode("utf-8")
        ).hexdigest(),
    }
    payload_sha256 = hashlib.sha256(
        _canonical_json(semantic).encode("utf-8")
    ).hexdigest()
    effect_key = compute_delivery_effect_key(
        delivery_id=str(row["delivery_id"]),
        effect_kind=DELIVERY_EFFECT_KIND,
        target_key=target_key,
        semantic_payload_sha256=payload_sha256,
    )
    marker = delivery_effect_marker(effect_key, str(row["artifact_set_id"]))
    comment_content, field_value = _adjudication_content(
        marker=marker,
        action=action,
        work_item_id=str(row["work_item_id"]),
        original_effect_key=str(row["effect_key"]),
        reason=reason,
        replacement_conclusion=replacement_conclusion,
    )
    payload = {
        **semantic,
        "effect_key": effect_key,
        "semantic_payload_sha256": payload_sha256,
        "marker": marker,
        "comment_content": comment_content,
        "field_updates": [
            {"field_key": RCA_RESULT_FIELD_KEY, "field_value": field_value}
        ],
    }
    return effect_key, payload_sha256, payload


def _validate_source(source: Mapping[str, Any]) -> dict[str, str]:
    if not isinstance(source, Mapping):
        raise ConclusionAdjudicationError("conclusion_adjudication_source_invalid")
    if set(source) != {"platform", "chat_id", "thread_id", "message_id"}:
        raise ConclusionAdjudicationError("conclusion_adjudication_source_invalid")
    normalized = {
        key: _clean_text(source.get(key), field=f"source_{key}", maximum=256, required=False)
        for key in ("platform", "chat_id", "thread_id", "message_id")
    }
    if normalized["platform"] != "feishu" or not normalized["chat_id"]:
        raise ConclusionAdjudicationError("conclusion_adjudication_source_invalid")
    return normalized


def _require_current_activation_epoch(conn: sqlite3.Connection) -> str:
    columns = {
        str(row[1])
        for row in conn.execute("PRAGMA table_info(rca_activation_epochs)").fetchall()
    }
    if not {"epoch_id", "state", "is_current"}.issubset(columns):
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_activation_unavailable"
        )
    rows = conn.execute(
        "SELECT epoch_id, state FROM rca_activation_epochs "
        "WHERE is_current = 1 ORDER BY epoch_id LIMIT 2"
    ).fetchall()
    if not rows:
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_activation_unavailable"
        )
    if len(rows) != 1:
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_activation_ambiguous"
        )
    if str(rows[0]["state"] or "") not in {"bounded_active", "steady_active"}:
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_activation_inactive"
        )
    return _clean_text(
        rows[0]["epoch_id"],
        field="activation_epoch_id",
        maximum=160,
        required=True,
    )


def _candidate_row(
    conn: sqlite3.Connection,
    *,
    work_item_id: str,
    original_effect_key: str,
) -> sqlite3.Row:
    parameters: list[Any] = [work_item_id]
    effect_filter = ""
    if original_effect_key:
        effect_filter = "AND e.effect_key = ?"
        parameters.append(original_effect_key)
    rows = conn.execute(
        f"""
        SELECT j.*, e.effect_key, e.target_key, e.payload_json,
               e.created_at AS effect_created_at,
               e.completed_at AS effect_completed_at,
               j.created_at AS job_created_at
          FROM rca_delivery_jobs AS j
          JOIN rca_delivery_effects AS e
            ON e.delivery_id = j.delivery_id
           AND e.effect_kind = 'feishu_issue_comment'
           AND e.target_key = j.target_key
         WHERE j.work_item_id = ?
           AND j.outcome = 'success'
           AND e.status = 'succeeded'
           AND e.write_phase = 'settled'
           {effect_filter}
         ORDER BY j.generation DESC, e.completed_at DESC, e.effect_key
         LIMIT 2
        """,
        parameters,
    ).fetchall()
    if not rows:
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_published_conclusion_missing"
        )
    if len(rows) > 1 and int(rows[0]["generation"]) == int(rows[1]["generation"]):
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_published_conclusion_ambiguous"
        )
    return rows[0]


def record_conclusion_adjudication_tx(
    conn: sqlite3.Connection,
    *,
    work_item_id: str,
    action: Literal["retract", "recognize"],
    reason: str,
    actor_id: str,
    actor_name: str = "",
    source: Mapping[str, Any],
    replacement_conclusion: str = "",
    original_effect_key: str = "",
    now: datetime | None = None,
) -> ConclusionAdjudicationResult:
    """Invalidate or recognize one published conclusion in the caller transaction."""
    if not conn.in_transaction:
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_transaction_required"
        )
    issue_id = _clean_text(
        work_item_id, field="work_item_id", maximum=32, required=True
    )
    if _WORK_ITEM_ID_RE.fullmatch(issue_id) is None:
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_work_item_id_invalid"
        )
    normalized_action = str(action or "").strip()
    if normalized_action not in ADJUDICATION_ACTIONS:
        raise ConclusionAdjudicationError("conclusion_adjudication_action_invalid")
    if normalized_action == "recognize":
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_recognize_publication_contract_unavailable"
        )
    normalized_reason = _clean_text(
        reason, field="reason", maximum=300, required=True
    )
    normalized_actor_id = _clean_text(
        actor_id, field="actor_id", maximum=160, required=True
    )
    normalized_actor_name = _clean_text(
        actor_name, field="actor_name", maximum=160, required=False
    )
    normalized_replacement = _clean_text(
        replacement_conclusion,
        field="replacement_conclusion",
        maximum=500,
        required=normalized_action == "recognize",
    )
    if normalized_action == "retract" and normalized_replacement:
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_retraction_replacement_invalid"
        )
    normalized_original_effect = _clean_text(
        original_effect_key,
        field="original_effect_key",
        maximum=256,
        required=False,
    )
    normalized_source = _validate_source(source)
    row = _candidate_row(
        conn,
        work_item_id=issue_id,
        original_effect_key=normalized_original_effect,
    )
    lineage = _impact_lineage(row)
    adjudication_id = _stable_key(
        ADJUDICATION_ID_PREFIX,
        {
            "business_key": str(row["business_key"]),
            "original_effect_key": str(row["effect_key"]),
            "action": normalized_action,
        },
    )
    existing = conn.execute(
        "SELECT * FROM rca_conclusion_adjudications WHERE business_key = ?",
        (row["business_key"],),
    ).fetchone()
    if existing is not None:
        exact = (
            str(existing["adjudication_id"]) == adjudication_id
            and str(existing["action"]) == normalized_action
            and str(existing["reason"]) == normalized_reason
            and str(existing["replacement_conclusion"]) == normalized_replacement
            and str(existing["actor_id"]) == normalized_actor_id
            and str(existing["original_effect_key"]) == str(row["effect_key"])
        )
        if not exact:
            raise ConclusionAdjudicationError(
                "conclusion_adjudication_comment_budget_exhausted"
            )
        repair = conn.execute(
            "SELECT status FROM rca_conclusion_adjudication_repairs "
            "WHERE adjudication_id = ?",
            (adjudication_id,),
        ).fetchone()
        return ConclusionAdjudicationResult(
            adjudication_id=adjudication_id,
            action=normalized_action,
            conclusion_state=str(existing["conclusion_state"]),
            business_key=str(row["business_key"]),
            generation=int(row["generation"]),
            work_item_id=issue_id,
            original_delivery_id=str(row["delivery_id"]),
            original_effect_key=str(row["effect_key"]),
            correction_effect_key=str(existing["correction_effect_key"]),
            created=False,
            created_at=str(existing["created_at"]),
            impact_lineage=lineage,
            artifact_repair_status=(
                str(repair["status"]) if repair is not None else "pending"
            ),
        )
    legacy_effect = conn.execute(
        """
        SELECT e.effect_key
          FROM rca_delivery_effects AS e
          JOIN rca_delivery_jobs AS j ON j.delivery_id = e.delivery_id
         WHERE j.business_key = ?
           AND (
                e.payload_json LIKE ?
                OR e.payload_json LIKE ?
           )
         LIMIT 1
        """,
        (
            row["business_key"],
            f'%"schema_version":"{ADJUDICATION_EFFECT_SCHEMA_VERSION}"%',
            f'%"schema_version":"{ADJUDICATION_EFFECT_SCHEMA_VERSION_V1}"%',
        ),
    ).fetchone()
    if legacy_effect is not None:
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_comment_budget_exhausted"
        )
    activation_epoch_id = _require_current_activation_epoch(conn)
    effect_key, payload_sha256, payload = _build_effect(
        row=row,
        adjudication_id=adjudication_id,
        action=normalized_action,
        reason=normalized_reason,
        replacement_conclusion=normalized_replacement,
        actor_id=normalized_actor_id,
        actor_name=normalized_actor_name,
        source=normalized_source,
        activation_epoch_id=activation_epoch_id,
        lineage=lineage,
    )
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()
    lineage_json = _canonical_json(lineage)
    lineage_sha256 = hashlib.sha256(lineage_json.encode("utf-8")).hexdigest()
    conn.execute(
        """
        INSERT INTO rca_delivery_effects(
            effect_key, delivery_id, effect_kind, required, target_key,
            payload_json, payload_sha256, outcome, status, created_at, updated_at
        ) VALUES (?, ?, 'feishu_issue_comment', 1, ?, ?, ?, 'success',
                  'pending', ?, ?)
        """,
        (
            effect_key,
            row["delivery_id"],
            payload["target_key"],
            _canonical_json(payload),
            payload_sha256,
            current,
            current,
        ),
    )
    conn.execute(
        """
        INSERT INTO rca_conclusion_adjudications(
            adjudication_id, schema_version, business_key, generation,
            project_key, work_item_type_key, work_item_id, action,
            conclusion_state, reason, replacement_conclusion, actor_id,
            actor_name, source_json, original_delivery_id, original_effect_key,
            correction_effect_key, activation_epoch_id, evaluator_refs_json,
            responsibility_domain, impact_window_start, impact_window_end,
            lineage_json, lineage_sha256, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                  ?, ?, ?, ?, ?)
        """,
        (
            adjudication_id,
            ADJUDICATION_SCHEMA_VERSION,
            row["business_key"],
            int(row["generation"]),
            row["project_key"],
            row["work_item_type_key"],
            issue_id,
            normalized_action,
            ADJUDICATION_STATES[normalized_action],
            normalized_reason,
            normalized_replacement,
            normalized_actor_id,
            normalized_actor_name,
            _canonical_json(normalized_source),
            row["delivery_id"],
            row["effect_key"],
            effect_key,
            activation_epoch_id,
            _canonical_json(lineage["evaluator_refs"]),
            lineage["responsibility_domain"],
            lineage["impact_window"]["start"],
            lineage["impact_window"]["end"],
            lineage_json,
            lineage_sha256,
            current,
        ),
    )
    conn.execute(
        """
        INSERT INTO rca_conclusion_adjudication_repairs(
            adjudication_id, status, created_at, updated_at
        ) VALUES (?, 'pending', ?, ?)
        """,
        (adjudication_id, current, current),
    )
    conn.execute(
        "UPDATE rca_delivery_jobs SET status = 'ready', updated_at = ? "
        "WHERE delivery_id = ?",
        (current, row["delivery_id"]),
    )
    return ConclusionAdjudicationResult(
        adjudication_id=adjudication_id,
        action=normalized_action,
        conclusion_state=ADJUDICATION_STATES[normalized_action],
        business_key=str(row["business_key"]),
        generation=int(row["generation"]),
        work_item_id=issue_id,
        original_delivery_id=str(row["delivery_id"]),
        original_effect_key=str(row["effect_key"]),
        correction_effect_key=effect_key,
        created=True,
        created_at=current,
        impact_lineage=lineage,
        artifact_repair_status="pending",
    )


def validate_adjudication_effect_claim(
    claim: Any,
) -> tuple[str, str, tuple[tuple[str, str], ...]]:
    """Recompute an adjudication effect before the dispatcher touches Feishu."""
    payload = claim.payload
    if (
        not isinstance(payload, Mapping)
        or payload.get("schema_version") != ADJUDICATION_EFFECT_SCHEMA_VERSION
    ):
        raise ConclusionAdjudicationError("conclusion_adjudication_effect_schema_invalid")
    if claim.effect_kind != DELIVERY_EFFECT_KIND or claim.required != 1:
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_effect_identity_invalid"
        )
    exact_keys = {
        "schema_version", "delivery_id", "effect_kind", "target_key",
        "project_key", "work_item_type_key", "work_item_id", "business_key",
        "generation", "adjudication_id",
        "action", "conclusion_state", "original_delivery_id",
        "original_effect_key", "original_target_key", "reason",
        "replacement_conclusion", "actor_id", "actor_name",
        "activation_epoch_id", "source_sha256", "lineage_sha256",
        "effect_key", "semantic_payload_sha256", "marker", "comment_content",
        "field_updates",
    }
    if set(payload) != exact_keys:
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_effect_shape_invalid"
        )
    identity = {
        "delivery_id": claim.delivery_id,
        "effect_kind": DELIVERY_EFFECT_KIND,
        "target_key": claim.target_key,
        "project_key": claim.project_key,
        "work_item_type_key": claim.work_item_type_key,
        "work_item_id": claim.work_item_id,
        "business_key": claim.business_key,
        "generation": claim.generation,
        "original_delivery_id": claim.delivery_id,
    }
    if any(payload.get(key) != value for key, value in identity.items()):
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_effect_identity_invalid"
        )
    action = str(payload.get("action") or "")
    if action not in ADJUDICATION_ACTIONS or payload.get("conclusion_state") != ADJUDICATION_STATES[action]:
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_effect_action_invalid"
        )
    if action == "recognize":
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_recognize_publication_contract_unavailable"
        )
    reason = _clean_text(
        payload.get("reason"), field="reason", maximum=300, required=True
    )
    actor_id = _clean_text(
        payload.get("actor_id"), field="actor_id", maximum=160, required=True
    )
    actor_name = _clean_text(
        payload.get("actor_name"), field="actor_name", maximum=160, required=False
    )
    replacement = _clean_text(
        payload.get("replacement_conclusion"),
        field="replacement_conclusion",
        maximum=500,
        required=action == "recognize",
    )
    if action == "retract" and replacement:
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_retraction_replacement_invalid"
        )
    activation_epoch_id = _clean_text(
        payload.get("activation_epoch_id"),
        field="activation_epoch_id",
        maximum=160,
        required=True,
    )
    if (
        _SHA256_RE.fullmatch(str(payload.get("source_sha256") or "")) is None
        or _SHA256_RE.fullmatch(str(payload.get("lineage_sha256") or "")) is None
    ):
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_effect_lineage_invalid"
        )
    expected_target_key = _stable_key(
        ADJUDICATION_EFFECT_TARGET_PREFIX,
        {
            "adjudication_id": str(payload.get("adjudication_id") or ""),
            "original_target_key": str(payload.get("original_target_key") or ""),
        },
    )
    if claim.target_key != expected_target_key:
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_effect_identity_invalid"
        )
    semantic = {
        key: payload.get(key)
        for key in (
            "schema_version", "delivery_id", "effect_kind", "target_key",
            "project_key", "work_item_type_key", "work_item_id", "business_key",
            "generation",
            "adjudication_id", "action", "conclusion_state",
            "original_delivery_id", "original_effect_key", "original_target_key",
            "reason", "replacement_conclusion", "actor_id", "actor_name",
            "activation_epoch_id", "source_sha256", "lineage_sha256",
        )
    }
    payload_sha256 = hashlib.sha256(
        _canonical_json(semantic).encode("utf-8")
    ).hexdigest()
    expected_key = compute_delivery_effect_key(
        delivery_id=claim.delivery_id,
        effect_kind=DELIVERY_EFFECT_KIND,
        target_key=claim.target_key,
        semantic_payload_sha256=payload_sha256,
    )
    if (
        claim.payload_sha256 != payload_sha256
        or payload.get("semantic_payload_sha256") != payload_sha256
        or claim.effect_key != expected_key
        or payload.get("effect_key") != expected_key
    ):
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_effect_hash_invalid"
        )
    marker = delivery_effect_marker(expected_key, claim.artifact_set_id)
    content, field_value = _adjudication_content(
        marker=marker,
        action=action,
        work_item_id=claim.work_item_id,
        original_effect_key=str(payload.get("original_effect_key") or ""),
        reason=reason,
        replacement_conclusion=replacement,
    )
    expected_updates = [
        {"field_key": RCA_RESULT_FIELD_KEY, "field_value": field_value}
    ]
    if (
        payload.get("marker") != marker
        or payload.get("comment_content") != content
        or payload.get("field_updates") != expected_updates
    ):
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_effect_content_invalid"
        )
    return marker, content, ((RCA_RESULT_FIELD_KEY, field_value),)


def validate_adjudication_effect_ledger_binding(
    claim: Any,
    adjudication: Mapping[str, Any],
    *,
    require_current_activation: bool = True,
) -> None:
    """Bind one self-consistent effect to its immutable authorized ledger row."""

    row = dict(adjudication)
    payload = claim.payload
    if not isinstance(payload, Mapping):
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_effect_ledger_mismatch"
        )
    expected = {
        "adjudication_id": row.get("adjudication_id"),
        "business_key": row.get("business_key"),
        "generation": row.get("generation"),
        "project_key": row.get("project_key"),
        "work_item_type_key": row.get("work_item_type_key"),
        "work_item_id": row.get("work_item_id"),
        "action": row.get("action"),
        "conclusion_state": row.get("conclusion_state"),
        "reason": row.get("reason"),
        "replacement_conclusion": row.get("replacement_conclusion"),
        "actor_id": row.get("actor_id"),
        "actor_name": row.get("actor_name"),
        "original_delivery_id": row.get("original_delivery_id"),
        "original_effect_key": row.get("original_effect_key"),
        "activation_epoch_id": row.get("activation_epoch_id"),
        "lineage_sha256": row.get("lineage_sha256"),
    }
    if (
        row.get("schema_version") != ADJUDICATION_SCHEMA_VERSION
        or row.get("correction_effect_key") != claim.effect_key
        or any(payload.get(key) != value for key, value in expected.items())
        or row.get("original_effect_status") != "succeeded"
        or row.get("original_effect_write_phase") != "settled"
        or row.get("original_effect_delivery_id")
        != row.get("original_delivery_id")
        or row.get("original_effect_kind") != DELIVERY_EFFECT_KIND
        or row.get("original_effect_required") != 1
        or row.get("original_effect_outcome") != "success"
        or payload.get("original_target_key")
        != row.get("original_effect_target_key")
        or row.get("original_job_business_key") != row.get("business_key")
        or row.get("original_job_generation") != row.get("generation")
        or row.get("original_job_project_key") != row.get("project_key")
        or row.get("original_job_work_item_type_key")
        != row.get("work_item_type_key")
        or row.get("original_job_work_item_id") != row.get("work_item_id")
        or row.get("original_job_target_key")
        != row.get("original_effect_target_key")
        or row.get("original_job_outcome") != "success"
    ):
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_effect_ledger_mismatch"
        )
    if require_current_activation and (
        row.get("activation_is_current") != 1
        or row.get("activation_state") not in {"bounded_active", "steady_active"}
    ):
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_effect_activation_stale"
        )

    source = _json_object(row.get("source_json"), field="source")
    normalized_source = _validate_source(source)
    expected_source_sha256 = hashlib.sha256(
        _canonical_json(normalized_source).encode("utf-8")
    ).hexdigest()
    lineage = _json_object(row.get("lineage_json"), field="lineage")
    lineage_keys = {
        "schema_version",
        "business_key",
        "generation",
        "project_key",
        "work_item_type_key",
        "work_item_id",
        "original_delivery_id",
        "original_effect_key",
        "evaluator_refs",
        "evaluator_resolution",
        "responsibility_domain",
        "impact_window",
    }
    if (
        set(lineage) != lineage_keys
        or lineage.get("schema_version")
        != "pnc_rca_conclusion_impact_lineage_v1"
    ):
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_effect_lineage_invalid"
        )
    expected_lineage_sha256 = hashlib.sha256(
        _canonical_json(lineage).encode("utf-8")
    ).hexdigest()
    try:
        evaluator_refs = json.loads(str(row.get("evaluator_refs_json") or ""))
    except json.JSONDecodeError as exc:
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_effect_lineage_invalid"
        ) from exc
    if (
        not isinstance(evaluator_refs, list)
        or any(not isinstance(item, str) or not item for item in evaluator_refs)
        or evaluator_refs != sorted(set(evaluator_refs))
        or lineage.get("evaluator_resolution")
        != ("resolved" if evaluator_refs else "unresolved")
    ):
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_effect_lineage_invalid"
        )
    lineage_expected = {
        "business_key": row.get("business_key"),
        "generation": row.get("generation"),
        "project_key": row.get("project_key"),
        "work_item_type_key": row.get("work_item_type_key"),
        "work_item_id": row.get("work_item_id"),
        "original_delivery_id": row.get("original_delivery_id"),
        "original_effect_key": row.get("original_effect_key"),
        "evaluator_refs": evaluator_refs,
        "responsibility_domain": row.get("responsibility_domain"),
        "impact_window": {
            "start": row.get("impact_window_start"),
            "end": row.get("impact_window_end"),
        },
    }
    if (
        payload.get("source_sha256") != expected_source_sha256
        or row.get("lineage_sha256") != expected_lineage_sha256
        or any(lineage.get(key) != value for key, value in lineage_expected.items())
    ):
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_effect_lineage_invalid"
        )
