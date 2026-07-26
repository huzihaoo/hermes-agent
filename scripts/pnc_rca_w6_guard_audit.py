#!/usr/bin/env python3
"""Read-only W6 learning-lane, comment-budget, and conclusion audit."""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import stat
from typing import Any, Iterable, Mapping, Sequence


AUDIT_SCHEMA_VERSION = "g1q3_rca_w6_guard_audit_v1"
LEARNING_ADMISSION_SCHEMA_VERSION = "g1q3_rca_learning_lane_admission_v1"
LEGACY_BATCH_ID = "rca-legacy-report-48-20260725"
LEGACY_BATCH_REQUESTER_ID = "codex-production-coverage"
FEISHU_EFFECT_PREFIX = "feishu_"
ISSUE_COMMENT_EFFECT = "feishu_issue_comment"
ADJUDICATION_EFFECT_SCHEMA_VERSIONS = frozenset({
    "pnc_rca_conclusion_adjudication_effect_v1",
    "pnc_rca_conclusion_adjudication_effect_v2",
})
ADJUDICATION_TARGET_PREFIX = "g1q3-rca-adjudication-target-v1-"
TERMINAL_EFFECT_SCHEMA_VERSIONS = frozenset({
    "pnc_rca_terminal_delivery_effect_v1",
    "pnc_rca_terminal_delivery_effect_v2",
    "pnc_rca_terminal_delivery_effect_v3",
})
_OPEN_ID_RE = re.compile(r"^ou_[0-9a-f]{32}$")
_MAX_EVIDENCE_ROWS = 20

_BASE_SCHEMA: Mapping[str, frozenset[str]] = {
    "business_triggers": frozenset({
        "business_key",
        "generation",
        "work_item_id",
        "created_at",
    }),
    "rca_trigger_sources": frozenset({
        "source_id",
        "source_kind",
        "mode",
        "requester_id",
        "chat_id",
        "thread_id",
        "message_id",
        "created_at",
    }),
    "rca_trigger_bindings": frozenset({"source_id", "business_key", "generation"}),
    "rca_delivery_jobs": frozenset({
        "delivery_id",
        "business_key",
        "generation",
        "work_item_id",
        "target_key",
    }),
    "rca_delivery_effects": frozenset({
        "effect_key",
        "delivery_id",
        "effect_kind",
        "target_key",
        "payload_json",
        "status",
        "write_phase",
        "write_started_at",
        "remote_receipt_json",
        "created_at",
    }),
    "rca_delivery_subscriptions": frozenset({
        "business_key",
        "generation",
        "effect_kind",
    }),
}
_LEARNING_SCHEMA = frozenset({
    "business_key",
    "generation",
    "work_item_id",
    "schema_version",
    "lane",
    "reason",
    "external_write_allowed",
    "admitted_at",
})
_ADJUDICATION_SCHEMA = frozenset({
    "adjudication_id",
    "business_key",
    "generation",
    "work_item_id",
    "action",
    "conclusion_state",
    "reason",
    "replacement_conclusion",
    "actor_id",
    "original_effect_key",
    "correction_effect_key",
    "created_at",
})


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _identity(path: Path) -> dict[str, int | str]:
    observed = path.lstat()
    return {
        "path": str(path),
        "device": int(observed.st_dev),
        "inode": int(observed.st_ino),
        "size": int(observed.st_size),
        "mtime_ns": int(observed.st_mtime_ns),
        "links": int(observed.st_nlink),
    }


def _wal_identity(path: Path) -> dict[str, int | str | bool]:
    wal = Path(f"{path}-wal")
    try:
        observed = wal.lstat()
    except FileNotFoundError:
        return {"path": str(wal), "exists": False, "size": 0}
    return {
        "path": str(wal),
        "exists": True,
        "device": int(observed.st_dev),
        "inode": int(observed.st_ino),
        "size": int(observed.st_size),
        "mtime_ns": int(observed.st_mtime_ns),
    }


def _parse_time(value: Any, *, field: str) -> datetime:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{field}_invalid") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field}_timezone_missing")
    return parsed.astimezone(timezone.utc)


def _canonical_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _table_names(conn: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}


def _require_base_schema(conn: sqlite3.Connection, tables: set[str]) -> None:
    for table, required in _BASE_SCHEMA.items():
        if table not in tables:
            raise RuntimeError(f"w6_required_table_missing:{table}")
        missing = sorted(required - _columns(conn, table))
        if missing:
            raise RuntimeError(
                f"w6_required_columns_missing:{table}:{','.join(missing)}"
            )


def _json_object(value: Any) -> dict[str, Any] | None:
    try:
        parsed = json.loads(str(value or ""))
    except (TypeError, json.JSONDecodeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _normalized_conclusion(payload: Mapping[str, Any] | None) -> str:
    if payload is None:
        return ""
    return str(payload.get("conclusion") or "").replace("\r\n", "\n").strip()


def _result_identity(
    payload: Mapping[str, Any] | None,
) -> tuple[str, str, str]:
    """Return stable identity, adjudication replacement text, and result kind."""

    conclusion = _normalized_conclusion(payload)
    if conclusion:
        return conclusion, conclusion, "conclusion"
    if payload and payload.get("schema_version") in TERMINAL_EFFECT_SCHEMA_VERSIONS:
        terminal = {
            "error_code": str(payload.get("error_code") or "").strip(),
            "outcome": str(payload.get("outcome") or "").strip(),
            "terminal_state": str(payload.get("terminal_state") or "").strip(),
        }
        identity = "terminal:" + json.dumps(
            terminal, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        )
        replacement = (
            f"terminal:{terminal['terminal_state']}:{terminal['outcome']}:"
            f"{terminal['error_code']}"
        )
        return identity, replacement, "terminal"
    return "", "", "unknown"


def _is_adjudication_effect(effect: Mapping[str, Any]) -> bool:
    payload = effect.get("payload")
    schema_version = str(payload.get("schema_version") or "") if payload else ""
    return schema_version in ADJUDICATION_EFFECT_SCHEMA_VERSIONS or str(
        effect.get("effect_target_key") or ""
    ).startswith(ADJUDICATION_TARGET_PREFIX)


def _potentially_outward(effect: Mapping[str, Any]) -> bool:
    return bool(
        effect.get("status") == "succeeded"
        or str(effect.get("write_started_at") or "").strip()
        or str(effect.get("remote_receipt_json") or "").strip()
    )


def _explicit_user_rerun(source: Mapping[str, Any]) -> bool:
    return bool(
        source.get("source_kind") == "feishu_group_manual"
        and source.get("mode") == "rerun"
        and _OPEN_ID_RE.fullmatch(str(source.get("requester_id") or ""))
        and str(source.get("chat_id") or "").strip()
        and str(source.get("thread_id") or "").strip()
        and str(source.get("message_id") or "").strip()
    )


def _rows(conn: sqlite3.Connection, sql: str) -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute(sql).fetchall()]


def _meta_value(
    conn: sqlite3.Connection, tables: set[str], table: str, key: str
) -> str:
    if table not in tables or not {"key", "value"}.issubset(_columns(conn, table)):
        return ""
    row = conn.execute(f"SELECT value FROM {table} WHERE key = ?", (key,)).fetchone()
    return str(row[0] or "") if row else ""


def _group_by(
    rows: Iterable[Mapping[str, Any]], field: str
) -> dict[str, list[Mapping[str, Any]]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get(field) or "")].append(row)
    return grouped


def audit_w6_guard(
    control_db: str | Path,
    *,
    stock_cutoff: str,
    expected_stock_count: int,
    sample_size: int = 5,
) -> dict[str, Any]:
    """Audit one checkpointed control database without mutating it."""

    path = Path(control_db).expanduser()
    if not path.is_absolute():
        raise ValueError("control DB path must be absolute")
    if expected_stock_count <= 0:
        raise ValueError("expected stock count must be positive")
    if sample_size < 1 or sample_size > 20:
        raise ValueError("sample size must be between 1 and 20")
    cutoff = _parse_time(stock_cutoff, field="stock_cutoff")
    before = _identity(path)
    mode = path.lstat().st_mode
    if (
        stat.S_ISLNK(mode)
        or not stat.S_ISREG(mode)
        or before["links"] != 1
        or before["size"] <= 0
    ):
        raise ValueError("control DB must be a non-empty, single-link regular file")
    wal_before = _wal_identity(path)
    if int(wal_before["size"]) != 0:
        raise RuntimeError("immutable W6 audit requires a checkpointed database")
    before_sha256 = _sha256(path)

    uri = f"{path.resolve().as_uri()}?mode=ro&immutable=1"
    conn = sqlite3.connect(uri, uri=True, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA query_only=ON")
        conn.execute("BEGIN")
        tables = _table_names(conn)
        _require_base_schema(conn, tables)
        control_schema = _meta_value(conn, tables, "control_meta", "schema_version")
        delivery_schema = _meta_value(
            conn, tables, "rca_delivery_meta", "schema_version"
        )

        triggers = _rows(
            conn,
            """
            SELECT business_key, generation, work_item_id, created_at
              FROM business_triggers
             ORDER BY business_key, generation
            """,
        )
        sources = {
            str(row["source_id"]): row
            for row in _rows(
                conn,
                """
                SELECT source_id, source_kind, mode, requester_id, chat_id,
                       thread_id, message_id, created_at
                  FROM rca_trigger_sources
                """,
            )
        }
        bindings: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
        for binding in _rows(
            conn,
            "SELECT source_id, business_key, generation FROM rca_trigger_bindings",
        ):
            source = sources.get(str(binding["source_id"]))
            if source is not None:
                bindings[
                    (str(binding["business_key"]), int(binding["generation"]))
                ].append(source)

        data_errors: list[str] = []
        trigger_times: dict[tuple[str, int], datetime] = {}
        trigger_by_key: dict[tuple[str, int], dict[str, Any]] = {}
        for trigger in triggers:
            key = (str(trigger["business_key"]), int(trigger["generation"]))
            trigger_by_key[key] = trigger
            try:
                trigger_times[key] = _parse_time(
                    trigger["created_at"], field="business_trigger_created_at"
                )
            except ValueError as exc:
                data_errors.append(f"{exc}:{key[0]}:{key[1]}")

        explicit_rerun_keys = {
            key
            for key, bound_sources in bindings.items()
            if any(_explicit_user_rerun(source) for source in bound_sources)
        }
        generation_origin_violations = [
            {
                "business_key": str(trigger["business_key"]),
                "generation": int(trigger["generation"]),
                "work_item_id": str(trigger["work_item_id"]),
            }
            for trigger in triggers
            if int(trigger["generation"]) > 1
            and (str(trigger["business_key"]), int(trigger["generation"]))
            not in explicit_rerun_keys
        ]

        effects = _rows(
            conn,
            """
            SELECT e.effect_key, e.delivery_id, e.effect_kind,
                   e.target_key AS effect_target_key, e.payload_json, e.status,
                   e.write_phase, e.write_started_at, e.remote_receipt_json,
                   e.created_at, j.business_key, j.generation, j.work_item_id,
                   j.target_key AS job_target_key
              FROM rca_delivery_effects AS e
              JOIN rca_delivery_jobs AS j ON j.delivery_id = e.delivery_id
             ORDER BY e.created_at, e.effect_key
            """,
        )
        primary_comments: list[dict[str, Any]] = []
        correction_comments: list[dict[str, Any]] = []
        other_comments: list[dict[str, Any]] = []
        feishu_effects: list[dict[str, Any]] = []
        for effect in effects:
            effect["payload"] = _json_object(effect["payload_json"])
            try:
                effect["created_at_dt"] = _parse_time(
                    effect["created_at"], field="delivery_effect_created_at"
                )
            except ValueError as exc:
                effect["created_at_dt"] = None
                data_errors.append(f"{exc}:{effect['effect_key']}")
            if str(effect["effect_kind"]).startswith(FEISHU_EFFECT_PREFIX):
                feishu_effects.append(effect)
            if effect[
                "effect_kind"
            ] != ISSUE_COMMENT_EFFECT or not _potentially_outward(effect):
                continue
            if _is_adjudication_effect(effect):
                correction_comments.append(effect)
            elif effect["effect_target_key"] == effect["job_target_key"]:
                (
                    effect["result_identity"],
                    effect["replacement_value"],
                    effect["result_kind"],
                ) = _result_identity(effect["payload"])
                if not effect["result_identity"]:
                    data_errors.append(
                        f"published_result_identity_missing:{effect['effect_key']}"
                    )
                primary_comments.append(effect)
            else:
                other_comments.append(effect)

        stock_ids = {
            str(effect["work_item_id"])
            for effect in primary_comments
            if effect["status"] == "succeeded"
            and effect["created_at_dt"] is not None
            and effect["created_at_dt"] <= cutoff
        }
        stock_count_matches = len(stock_ids) == expected_stock_count
        post_cutoff_stock_effects = [
            effect
            for effect in feishu_effects
            if str(effect["work_item_id"]) in stock_ids
            and effect["created_at_dt"] is not None
            and effect["created_at_dt"] > cutoff
        ]

        learning_schema_errors: list[str] = []
        learning_rows: list[dict[str, Any]] = []
        if "rca_learning_lane_admissions" not in tables:
            learning_schema_errors.append("learning_admission_table_missing")
        else:
            missing = sorted(
                _LEARNING_SCHEMA - _columns(conn, "rca_learning_lane_admissions")
            )
            if missing:
                learning_schema_errors.append(
                    f"learning_admission_columns_missing:{','.join(missing)}"
                )
            else:
                learning_rows = _rows(
                    conn,
                    """
                    SELECT business_key, generation, work_item_id, schema_version,
                           lane, reason, external_write_allowed, admitted_at
                      FROM rca_learning_lane_admissions
                     ORDER BY business_key, generation
                    """,
                )
        learning_schema_ready = not learning_schema_errors
        valid_learning_keys: set[tuple[str, int]] = set()
        learning_row_violations: list[dict[str, Any]] = []
        for row in learning_rows:
            key = (str(row["business_key"]), int(row["generation"]))
            trigger = trigger_by_key.get(key)
            valid = bool(
                row["schema_version"] == LEARNING_ADMISSION_SCHEMA_VERSION
                and row["lane"] == "learning"
                and row["reason"] in {"stock", "legacy"}
                and row["external_write_allowed"] == 0
                and trigger is not None
                and str(row["work_item_id"]) == str(trigger["work_item_id"])
                and str(row["work_item_id"]) in stock_ids
            )
            try:
                admitted_at = _parse_time(
                    row["admitted_at"], field="learning_admitted_at"
                )
                valid = valid and admitted_at > cutoff
            except ValueError as exc:
                data_errors.append(f"{exc}:{key[0]}:{key[1]}")
                valid = False
            if valid:
                valid_learning_keys.add(key)
            else:
                learning_row_violations.append({
                    "business_key": key[0],
                    "generation": key[1],
                    "work_item_id": str(row["work_item_id"]),
                })

        post_cutoff_stock_triggers = {
            key
            for key, trigger in trigger_by_key.items()
            if str(trigger["work_item_id"]) in stock_ids
            and trigger_times.get(key) is not None
            and trigger_times[key] > cutoff
        }
        stock_triggers_without_learning = sorted(
            post_cutoff_stock_triggers - valid_learning_keys
        )
        learning_effects = [
            effect
            for effect in feishu_effects
            if (str(effect["business_key"]), int(effect["generation"]))
            in valid_learning_keys
        ]
        subscriptions = _rows(
            conn,
            """
            SELECT business_key, generation, effect_kind
              FROM rca_delivery_subscriptions
            """,
        )
        learning_subscriptions = [
            row
            for row in subscriptions
            if (str(row["business_key"]), int(row["generation"])) in valid_learning_keys
            and str(row["effect_kind"]).startswith(FEISHU_EFFECT_PREFIX)
        ]

        primary_by_item = _group_by(primary_comments, "work_item_id")
        correction_by_item = _group_by(correction_comments, "work_item_id")
        other_by_item = _group_by(other_comments, "work_item_id")
        explicit_by_item: dict[str, set[tuple[str, int]]] = defaultdict(set)
        for key in explicit_rerun_keys:
            trigger = trigger_by_key.get(key)
            if trigger is not None:
                explicit_by_item[str(trigger["work_item_id"])].add(key)
        budget_violations: list[dict[str, Any]] = []
        all_comment_items = (
            set(primary_by_item) | set(correction_by_item) | set(other_by_item)
        )
        for work_item_id in sorted(all_comment_items):
            primary = sorted(
                primary_by_item.get(work_item_id, []),
                key=lambda row: (
                    int(row["generation"]),
                    row.get("created_at_dt")
                    or datetime.min.replace(tzinfo=timezone.utc),
                    str(row["effect_key"]),
                ),
            )
            corrections = correction_by_item.get(work_item_id, [])
            others = other_by_item.get(work_item_id, [])
            reruns = explicit_by_item.get(work_item_id, set())
            reasons: list[str] = []
            per_generation: dict[tuple[str, int], int] = defaultdict(int)
            for row in primary:
                per_generation[(str(row["business_key"]), int(row["generation"]))] += 1
            if any(count > 1 for count in per_generation.values()):
                reasons.append("multiple_primary_comments_in_generation")
            for row in primary[1:]:
                key = (str(row["business_key"]), int(row["generation"]))
                if key not in reruns:
                    reasons.append("primary_comment_without_explicit_user_rerun")
                    break
            if len(primary) > 1 + len(reruns):
                reasons.append("primary_comment_budget_exceeded")
            if len(corrections) > 1:
                reasons.append("adjudication_comment_budget_exceeded")
            if others:
                reasons.append("unclassified_issue_comment_effect")
            allowed_total = 1 + len(reruns) + 1
            actual_total = len(primary) + len(corrections) + len(others)
            if actual_total > allowed_total:
                reasons.append("total_comment_budget_exceeded")
            if reasons:
                budget_violations.append({
                    "work_item_id": work_item_id,
                    "primary_comments": len(primary),
                    "adjudication_comments": len(corrections),
                    "unclassified_comments": len(others),
                    "explicit_user_reruns": len(reruns),
                    "allowed_total": allowed_total,
                    "reasons": sorted(set(reasons)),
                })

        latest_stock_rows: list[tuple[datetime, str, Mapping[str, Any]]] = []
        for work_item_id in stock_ids:
            rows = [
                row
                for row in primary_by_item.get(work_item_id, [])
                if row.get("created_at_dt") is not None
            ]
            if rows:
                latest = max(
                    rows,
                    key=lambda row: (
                        row["created_at_dt"],
                        int(row["generation"]),
                        str(row["effect_key"]),
                    ),
                )
                latest_stock_rows.append((
                    latest["created_at_dt"],
                    work_item_id,
                    latest,
                ))
        samples: list[dict[str, Any]] = []
        for _created_at, work_item_id, latest in sorted(
            latest_stock_rows, reverse=True
        )[:sample_size]:
            rerun_count = len(explicit_by_item.get(work_item_id, set()))
            primary_count = len(primary_by_item.get(work_item_id, []))
            correction_count = len(correction_by_item.get(work_item_id, []))
            other_count = len(other_by_item.get(work_item_id, []))
            actual = primary_count + correction_count + other_count
            allowed = 1 + rerun_count + 1
            samples.append({
                "work_item_id": work_item_id,
                "latest_generation": int(latest["generation"]),
                "primary_comments": primary_count,
                "adjudication_comments": correction_count,
                "unclassified_comments": other_count,
                "explicit_user_reruns": rerun_count,
                "allowed_total": allowed,
                "potentially_outward_comments": actual,
                "within_budget": actual <= allowed
                and not any(
                    row["work_item_id"] == work_item_id for row in budget_violations
                ),
            })

        transition_rows: list[dict[str, Any]] = []
        for work_item_id, rows in primary_by_item.items():
            published = sorted(
                (
                    row
                    for row in rows
                    if row["status"] == "succeeded"
                    and row.get("created_at_dt") is not None
                    and row.get("result_identity")
                ),
                key=lambda row: (
                    int(row["generation"]),
                    row["created_at_dt"],
                    str(row["effect_key"]),
                ),
            )
            if not published:
                continue
            previous = published[0]
            for current in published[1:]:
                if current["result_identity"] == previous["result_identity"]:
                    previous = current
                    continue
                transition_rows.append({
                    "work_item_id": work_item_id,
                    "business_key": str(current["business_key"]),
                    "from_effect_key": str(previous["effect_key"]),
                    "from_generation": int(previous["generation"]),
                    "from_created_at": previous["created_at_dt"],
                    "from_result_identity": str(previous["result_identity"]),
                    "to_effect_key": str(current["effect_key"]),
                    "to_generation": int(current["generation"]),
                    "to_created_at": current["created_at_dt"],
                    "to_result_identity": str(current["result_identity"]),
                    "to_replacement_value": str(current["replacement_value"]),
                })
                previous = current

        adjudication_schema_errors: list[str] = []
        adjudications: list[dict[str, Any]] = []
        if "rca_conclusion_adjudications" not in tables:
            adjudication_schema_errors.append("adjudication_table_missing")
        else:
            missing = sorted(
                _ADJUDICATION_SCHEMA - _columns(conn, "rca_conclusion_adjudications")
            )
            if missing:
                adjudication_schema_errors.append(
                    f"adjudication_columns_missing:{','.join(missing)}"
                )
            else:
                adjudications = _rows(
                    conn,
                    """
                    SELECT adjudication_id, business_key, generation, work_item_id,
                           action, conclusion_state, reason,
                           replacement_conclusion, actor_id, original_effect_key,
                           correction_effect_key, created_at
                      FROM rca_conclusion_adjudications
                     ORDER BY created_at, adjudication_id
                    """,
                )
        adjudication_schema_ready = not adjudication_schema_errors
        adjudications_by_original = _group_by(adjudications, "original_effect_key")
        missing_adjudications: list[dict[str, Any]] = []
        matched_adjudications: set[str] = set()
        for transition in transition_rows:
            match: dict[str, Any] | None = None
            for row in adjudications_by_original.get(transition["from_effect_key"], []):
                try:
                    adjudicated_at = _parse_time(
                        row["created_at"], field="adjudication_created_at"
                    )
                except ValueError:
                    continue
                if (
                    str(row["work_item_id"]) == transition["work_item_id"]
                    and str(row["business_key"]) == transition["business_key"]
                    and row["action"] == "retract"
                    and row["conclusion_state"] == "invalidated"
                    and str(row["reason"] or "").strip()
                    and str(row["actor_id"] or "").strip()
                    and _normalized_conclusion({
                        "conclusion": row["replacement_conclusion"]
                    })
                    == transition["to_replacement_value"]
                    and transition["from_created_at"] <= adjudicated_at
                ):
                    match = row
                    break
            if match is None:
                missing_adjudications.append({
                    "work_item_id": transition["work_item_id"],
                    "from_effect_key": transition["from_effect_key"],
                    "from_generation": transition["from_generation"],
                    "from_result_identity_sha256": hashlib.sha256(
                        transition["from_result_identity"].encode("utf-8")
                    ).hexdigest(),
                    "to_effect_key": transition["to_effect_key"],
                    "to_generation": transition["to_generation"],
                    "to_result_identity_sha256": hashlib.sha256(
                        transition["to_result_identity"].encode("utf-8")
                    ).hexdigest(),
                })
            else:
                matched_adjudications.add(str(match["adjudication_id"]))

        legacy_sources = [
            source
            for source in sources.values()
            if source["requester_id"] == LEGACY_BATCH_REQUESTER_ID
        ]
        legacy_items = {
            str(trigger_by_key[key]["work_item_id"])
            for key, bound_sources in bindings.items()
            if any(
                source["requester_id"] == LEGACY_BATCH_REQUESTER_ID
                for source in bound_sources
            )
            and key in trigger_by_key
        }
        conn.rollback()
    finally:
        conn.close()

    after = _identity(path)
    wal_after = _wal_identity(path)
    after_sha256 = _sha256(path)
    if before != after or wal_before != wal_after or before_sha256 != after_sha256:
        raise RuntimeError("control DB changed during immutable W6 audit")

    invariants = {
        "audited_rows_valid": not data_errors,
        "stock_count_matches_expected": stock_count_matches,
        "learning_lane_schema_ready": learning_schema_ready,
        "learning_lane_observed": bool(valid_learning_keys),
        "learning_rows_valid": not learning_row_violations,
        "stock_post_cutoff_all_learning": not stock_triggers_without_learning,
        "learning_lane_no_feishu_subscriptions": not learning_subscriptions,
        "learning_lane_no_feishu_effects": not learning_effects,
        "stock_post_cutoff_feishu_effect_delta_zero": not post_cutoff_stock_effects,
        "generation_only_on_explicit_user_rerun": not generation_origin_violations,
        "comment_budget_clean": not budget_violations,
        "adjudication_schema_ready": adjudication_schema_ready,
        "conclusion_flips_explicitly_adjudicated": not missing_adjudications,
    }
    return {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "observed_at": _canonical_time(datetime.now(timezone.utc)),
        "external_writes": False,
        "sqlite_mode": "ro+immutable",
        "control_db": {
            **before,
            "sha256": before_sha256,
            "wal": wal_before,
            "control_schema_version": control_schema,
            "delivery_schema_version": delivery_schema,
        },
        "scope": {
            "stock_cutoff": _canonical_time(cutoff),
            "expected_stock_count": expected_stock_count,
            "observed_stock_count": len(stock_ids),
            "stock_ids_sha256": hashlib.sha256(
                "\n".join(sorted(stock_ids)).encode("utf-8")
            ).hexdigest(),
        },
        "legacy_batch": {
            "batch_id": LEGACY_BATCH_ID,
            "requester_id": LEGACY_BATCH_REQUESTER_ID,
            "source_count": len(legacy_sources),
            "work_item_count": len(legacy_items),
            "termination_verified": False,
            "production_action": "blocked_not_performed",
        },
        "counts": {
            "business_triggers": len(triggers),
            "primary_issue_comments": len(primary_comments),
            "terminal_result_comments": sum(
                row.get("result_kind") == "terminal" for row in primary_comments
            ),
            "adjudication_issue_comments": len(correction_comments),
            "unclassified_issue_comments": len(other_comments),
            "post_cutoff_stock_triggers": len(post_cutoff_stock_triggers),
            "valid_learning_admissions": len(valid_learning_keys),
            "learning_feishu_subscriptions": len(learning_subscriptions),
            "learning_feishu_effects": len(learning_effects),
            "post_cutoff_stock_feishu_effects": len(post_cutoff_stock_effects),
            "generation_origin_violations": len(generation_origin_violations),
            "comment_budget_violations": len(budget_violations),
            "conclusion_flip_items": len({
                row["work_item_id"] for row in transition_rows
            }),
            "conclusion_transitions": len(transition_rows),
            "adjudicated_transitions": len(matched_adjudications),
            "missing_transition_adjudications": len(missing_adjudications),
        },
        "schema_errors": {
            "learning_lane": learning_schema_errors,
            "adjudication": adjudication_schema_errors,
        },
        "violations": {
            "data_errors": data_errors[:_MAX_EVIDENCE_ROWS],
            "learning_rows": learning_row_violations[:_MAX_EVIDENCE_ROWS],
            "stock_triggers_without_learning": [
                {"business_key": key[0], "generation": key[1]}
                for key in stock_triggers_without_learning[:_MAX_EVIDENCE_ROWS]
            ],
            "learning_feishu_effects": [
                {
                    "effect_key": str(row["effect_key"]),
                    "work_item_id": str(row["work_item_id"]),
                    "effect_kind": str(row["effect_kind"]),
                }
                for row in learning_effects[:_MAX_EVIDENCE_ROWS]
            ],
            "post_cutoff_stock_feishu_effects": [
                {
                    "effect_key": str(row["effect_key"]),
                    "work_item_id": str(row["work_item_id"]),
                    "effect_kind": str(row["effect_kind"]),
                }
                for row in post_cutoff_stock_effects[:_MAX_EVIDENCE_ROWS]
            ],
            "generation_origins": generation_origin_violations[:_MAX_EVIDENCE_ROWS],
            "comment_budget": budget_violations[:_MAX_EVIDENCE_ROWS],
            "missing_transition_adjudications": missing_adjudications[
                :_MAX_EVIDENCE_ROWS
            ],
        },
        "latest_stock_comment_samples": samples,
        "invariants": invariants,
        "ok": all(invariants.values()),
        "ga_acceptance_claimed": False,
        "production_actions_performed": False,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--control-db", type=Path, required=True)
    parser.add_argument("--stock-cutoff", required=True)
    parser.add_argument("--expected-stock-count", type=int, required=True)
    parser.add_argument("--sample-size", type=int, default=5)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        result = audit_w6_guard(
            args.control_db,
            stock_cutoff=args.stock_cutoff,
            expected_stock_count=args.expected_stock_count,
            sample_size=args.sample_size,
        )
    except (OSError, RuntimeError, ValueError, sqlite3.Error) as exc:
        result = {
            "schema_version": AUDIT_SCHEMA_VERSION,
            "observed_at": _canonical_time(datetime.now(timezone.utc)),
            "external_writes": False,
            "ok": False,
            "error": f"{type(exc).__name__}:{exc}",
            "ga_acceptance_claimed": False,
            "production_actions_performed": False,
        }
    encoded = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    return 0 if result.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
