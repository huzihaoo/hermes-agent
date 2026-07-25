"""Durable recognition and retraction of published PNC RCA conclusions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import re
import sqlite3
from typing import Any, Literal, Mapping

from gateway.pnc_rca_delivery_contract import (
    DELIVERY_EFFECT_KIND,
    MAX_FEISHU_COMMENT_BYTES,
    RCA_RESULT_FIELD_KEY,
    compute_delivery_effect_key,
    delivery_effect_marker,
)


ADJUDICATION_SCHEMA_VERSION = "pnc_rca_conclusion_adjudication_v1"
ADJUDICATION_EFFECT_SCHEMA_VERSION = "pnc_rca_conclusion_adjudication_effect_v1"
ADJUDICATION_EFFECT_TARGET_PREFIX = "g1q3-rca-adjudication-target-v1"
ADJUDICATION_ID_PREFIX = "g1q3-rca-adjudication-v1"
ADJUDICATION_ACTIONS = frozenset({"retract", "recognize"})
ADJUDICATION_STATES = {
    "retract": "invalidated",
    "recognize": "recognized",
}
_WORK_ITEM_ID_RE = re.compile(r"^[0-9]{1,32}$")
_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


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
    impact_lineage: dict[str, Any]


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


def ensure_conclusion_adjudication_schema(conn: sqlite3.Connection) -> None:
    """Install the immutable adjudication ledger in the delivery database."""
    conn.executescript(
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
        """
    )


def validate_conclusion_adjudication_schema(conn: sqlite3.Connection) -> None:
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
    if not required_columns.issubset(observed_columns):
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


def is_adjudication_effect_payload(payload: Mapping[str, Any]) -> bool:
    return payload.get("schema_version") == ADJUDICATION_EFFECT_SCHEMA_VERSION


def _collect_evaluator_refs(value: Any, *, depth: int = 0) -> list[str]:
    if depth > 6:
        return []
    refs: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized = str(key).strip().lower()
            if normalized in {
                "evaluator_id",
                "evaluator_ids",
                "evaluator_ref",
                "evaluator_refs",
                "matched_evaluators",
            }:
                candidates = item if isinstance(item, (list, tuple, set)) else [item]
                for candidate in candidates:
                    if isinstance(candidate, Mapping):
                        candidate = (
                            candidate.get("id")
                            or candidate.get("ref")
                            or candidate.get("name")
                        )
                    text = str(candidate or "").strip()
                    if text and len(text) <= 160:
                        refs.append(text)
            refs.extend(_collect_evaluator_refs(item, depth=depth + 1))
    elif isinstance(value, (list, tuple)):
        for item in value:
            refs.extend(_collect_evaluator_refs(item, depth=depth + 1))
    return refs


def _responsibility_domain(*values: Mapping[str, Any]) -> str:
    priority = (
        "responsibility_domain",
        "responsibility_domain_ref",
        "domain_id",
        "domain_ref",
        "domain",
    )

    def find(value: Any, depth: int = 0) -> str:
        if depth > 6:
            return ""
        if isinstance(value, Mapping):
            for key in priority:
                candidate = value.get(key)
                if isinstance(candidate, Mapping):
                    candidate = (
                        candidate.get("id")
                        or candidate.get("ref")
                        or candidate.get("name")
                    )
                text = str(candidate or "").strip()
                if text and len(text) <= 160:
                    return text
            for item in value.values():
                found = find(item, depth + 1)
                if found:
                    return found
        elif isinstance(value, (list, tuple)):
            for item in value:
                found = find(item, depth + 1)
                if found:
                    return found
        return ""

    return next((found for value in values if (found := find(value))), "unresolved")


def _impact_lineage(row: sqlite3.Row) -> dict[str, Any]:
    contract = _json_object(row["contract_json"], field="contract")
    manifest = _json_object(row["manifest_json"], field="manifest")
    original_payload = _json_object(row["payload_json"], field="original_payload")
    evaluator_refs = sorted(
        set(
            _collect_evaluator_refs(contract)
            + _collect_evaluator_refs(manifest)
            + _collect_evaluator_refs(original_payload)
        )
    )[:64]
    start = str(row["effect_created_at"] or row["job_created_at"] or "").strip()
    end = str(row["effect_completed_at"] or start).strip()
    if not start or not end:
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_impact_window_unavailable"
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
        "evaluator_resolution": "resolved" if evaluator_refs else "unresolved",
        "responsibility_domain": _responsibility_domain(
            contract, manifest, original_payload
        ),
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
    if action == "retract":
        field_value = "原自动 RCA 结论已撤回并标记作废；不可作为定责依据。"
        lines = [
            marker,
            "【RCA 更正】原自动分析结论已撤回并标记作废。",
            f"问题：{work_item_id}",
            "原结论不可作为定责依据；后续以重新复核后的结论为准。",
            f"失效结论标识：{original_effect_key}",
            f"更正原因：{reason}",
        ]
    else:
        field_value = f"人工追认：{replacement_conclusion}"
        lines = [
            marker,
            "【RCA 追认】候选结论已经人工复核。",
            f"问题：{work_item_id}",
            f"追认结论：{replacement_conclusion}",
            f"原结论标识：{original_effect_key}",
            f"追认依据：{reason}",
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
    normalized = {
        key: _clean_text(source.get(key), field=f"source_{key}", maximum=256, required=False)
        for key in ("platform", "chat_id", "thread_id", "message_id")
    }
    if normalized["platform"] != "feishu" or not normalized["chat_id"]:
        raise ConclusionAdjudicationError("conclusion_adjudication_source_invalid")
    return normalized


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
            impact_lineage=lineage,
        )
    legacy_effect = conn.execute(
        """
        SELECT e.effect_key
          FROM rca_delivery_effects AS e
          JOIN rca_delivery_jobs AS j ON j.delivery_id = e.delivery_id
         WHERE j.business_key = ?
           AND e.payload_json LIKE ?
         LIMIT 1
        """,
        (
            row["business_key"],
            f'%"schema_version":"{ADJUDICATION_EFFECT_SCHEMA_VERSION}"%',
        ),
    ).fetchone()
    if legacy_effect is not None:
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_comment_budget_exhausted"
        )
    current_epoch = conn.execute(
        """
        SELECT epoch_id, state FROM rca_activation_epochs
         WHERE is_current = 1 LIMIT 2
        """
    ).fetchall() if conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' "
        "AND name = 'rca_activation_epochs'"
    ).fetchone() else []
    if len(current_epoch) > 1:
        raise ConclusionAdjudicationError(
            "conclusion_adjudication_activation_ambiguous"
        )
    activation_epoch_id = ""
    if current_epoch:
        if str(current_epoch[0]["state"]) not in {"bounded_active", "steady_active"}:
            raise ConclusionAdjudicationError(
                "conclusion_adjudication_activation_inactive"
            )
        activation_epoch_id = str(current_epoch[0]["epoch_id"])
    effect_key, payload_sha256, payload = _build_effect(
        row=row,
        adjudication_id=adjudication_id,
        action=normalized_action,
        reason=normalized_reason,
        replacement_conclusion=normalized_replacement,
        actor_id=normalized_actor_id,
        actor_name=normalized_actor_name,
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
        impact_lineage=lineage,
    )


def validate_adjudication_effect_claim(claim: Any) -> tuple[str, str, tuple[tuple[str, str], ...]]:
    """Recompute an adjudication effect before the dispatcher touches Feishu."""
    payload = claim.payload
    if not isinstance(payload, Mapping) or not is_adjudication_effect_payload(payload):
        raise ConclusionAdjudicationError("conclusion_adjudication_effect_schema_invalid")
    exact_keys = {
        "schema_version", "delivery_id", "effect_kind", "target_key",
        "project_key", "work_item_type_key", "work_item_id", "adjudication_id",
        "action", "conclusion_state", "original_delivery_id",
        "original_effect_key", "original_target_key", "reason",
        "replacement_conclusion", "actor_id", "actor_name", "lineage_sha256",
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
    semantic = {
        key: payload.get(key)
        for key in (
            "schema_version", "delivery_id", "effect_kind", "target_key",
            "project_key", "work_item_type_key", "work_item_id",
            "adjudication_id", "action", "conclusion_state",
            "original_delivery_id", "original_effect_key", "original_target_key",
            "reason", "replacement_conclusion", "actor_id", "actor_name",
            "lineage_sha256",
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
        reason=str(payload.get("reason") or ""),
        replacement_conclusion=str(payload.get("replacement_conclusion") or ""),
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
