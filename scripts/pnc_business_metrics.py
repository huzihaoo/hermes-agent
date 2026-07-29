#!/usr/bin/env python3
"""Normalize offline PNC metric observations.

The resident Hermes ``pnc_business_metrics.py`` script is intentionally a
read-only runtime helper and is not part of this repository.  This module is
the repository-side normalization layer used by the W12 candidate.  It accepts
the field names emitted by W3/delivery contracts while requiring an explicit
denominator scope so business and system observations cannot silently share a
denominator.  The SQLite producer below only opens checkpointed immutable
snapshots and never writes a runtime database.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import stat
from typing import Any


SCHEMA_VERSION = "pnc_business_metrics_w12_v1"
UPSTREAM_DISPATCH_SCHEMA_VERSION = "pnc_upstream_dispatch_metrics_w12_v2"
DENOMINATOR_KINDS = frozenset({"business", "system"})
CONFIDENCE_TIERS = ("high", "medium", "low", "none")
ENTRYPOINTS = ("kafka", "feishu")
UPSTREAM_DISPATCH_TERMINAL_CLASSES = (
    "valid_dispatch",
    "abstain_no_hit",
    "abstain_cross_domain",
    "out_of_scope",
    "technical_failure",
)
UPSTREAM_DISPATCH_ABSTAIN_REASONS = (
    "no_hit",
    "cross_domain",
    "input_incomplete",
    "capability_degraded",
    "timeout_fallback",
)
UPSTREAM_DISPATCH_OWNER_BUCKETS = frozenset(
    {
        "acc_longitudinal_control",
        "aeb",
        "fctb_fcw",
        "hmi_sr",
        "lane_perception",
        "lcc_lateral_control",
        "ooi_spp",
        "tsr",
        "vision_perception",
    }
)
UPSTREAM_DISPATCH_REVIEW_COVERAGE_MIN_PCT = 30
UPSTREAM_DISPATCH_REVIEWED_COUNT_MIN = 10
MAX_INPUT_BYTES = 96 * 1024 * 1024
CONTROL_STORE_SCHEMA_VERSION = "pnc_rca_control_store_v11"
DELIVERY_STORE_SCHEMA_VERSION = "pnc_rca_delivery_store_v10"
SUPPORTED_CONTROL_STORE_SCHEMA_VERSIONS = frozenset({
    CONTROL_STORE_SCHEMA_VERSION,
    "pnc_rca_control_store_v12",
    "pnc_rca_control_store_v13",
})
SUPPORTED_DELIVERY_STORE_SCHEMA_VERSIONS = frozenset({
    DELIVERY_STORE_SCHEMA_VERSION,
    "pnc_rca_delivery_store_v9",
})
ADJUDICATION_SCHEMA_VERSION = "pnc_rca_conclusion_adjudication_v1"
GOLDEN_INPUT_SCHEMA_VERSION = "pnc_rca_w12_golden_observations_v1"
ORACLE_SCHEMA_VERSION = "pnc_rca_structural_tier_oracle_v2"
MAX_SQLITE_JOBS = 100_000
_DELIVERY_EFFECT_SCHEMA_VERSIONS = frozenset({
    "pnc_rca_delivery_effect_v1",
    "pnc_rca_delivery_effect_v2",
    "pnc_rca_delivery_effect_v3",
    "pnc_rca_terminal_delivery_effect_v1",
    "pnc_rca_terminal_delivery_effect_v2",
    "pnc_rca_terminal_delivery_effect_v3",
})
_ADJUDICATION_EFFECT_SCHEMA_VERSIONS = frozenset({
    "pnc_rca_conclusion_adjudication_effect_v1",
    "pnc_rca_conclusion_adjudication_effect_v2",
})

_SAFE_TEXT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/+@-]{0,255}$")
_SHA1_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MISSING = object()

_SQLITE_REQUIRED_COLUMNS = {
    "control_meta": {"key", "value"},
    "rca_delivery_meta": {"key", "value"},
    "rca_admission_snapshots": {
        "snapshot_sha256",
        "business_key",
        "submission_key",
        "generation",
        "execution_decision",
    },
    "rca_source_authority_receipts": {
        "authority_sha256",
        "source_id",
        "source_kind",
        "payload_sha256",
        "authorization_evidence_sha256",
        "binding_action",
        "decision",
    },
    "rca_snapshot_source_envelopes": {
        "source_envelope_sha256",
        "snapshot_sha256",
        "submission_key",
        "source_authority_sha256",
        "source_id",
        "source_kind",
        "payload_sha256",
        "authorization_evidence_sha256",
        "binding_action",
        "decision",
        "source_metadata_json",
    },
    "rca_delivery_jobs": {
        "delivery_id",
        "submission_key",
        "business_key",
        "generation",
        "project_key",
        "work_item_id",
        "outcome",
        "outcome_key",
        "terminal_state",
        "terminal_error_code",
        "status",
        "contract_json",
        "created_at",
        "updated_at",
    },
    "rca_delivery_effects": {
        "effect_key",
        "delivery_id",
        "effect_kind",
        "required",
        "payload_json",
        "status",
        "write_phase",
        "remote_receipt_json",
        "completed_at",
        "updated_at",
    },
    "rca_conclusion_adjudications": {
        "adjudication_id",
        "schema_version",
        "business_key",
        "generation",
        "work_item_id",
        "action",
        "conclusion_state",
        "actor_id",
        "original_delivery_id",
        "original_effect_key",
        "correction_effect_key",
        "activation_epoch_id",
        "created_at",
    },
    "rca_conclusion_adjudication_repairs": {"adjudication_id", "status"},
}

_IMMUTABLE_TRIGGER_NAMES = frozenset({
    "trg_rca_source_authority_no_update",
    "trg_rca_source_authority_no_delete",
    "trg_rca_admission_snapshot_no_update",
    "trg_rca_admission_snapshot_no_delete",
    "trg_rca_snapshot_envelope_no_update",
    "trg_rca_snapshot_envelope_no_delete",
    "trg_rca_conclusion_adjudication_no_update",
    "trg_rca_conclusion_adjudication_no_delete",
})

_TERMINAL_DIAGNOSTIC_CODES = frozenset({
    "business_route_unresolved",
    "business_route_unsupported",
    "business_route_conflict",
    "business_adapter_not_ready",
    "input_remote_data_required",
    "input_remote_data_invalid",
    "input_frame_required",
    "input_required",
    "issue_source_unavailable",
    "submission_failed",
    "analysis_failed",
})
_UNSUPPORTED_CODES = frozenset({
    "business_profile_unsupported",
    "business_route_unsupported",
    "unsupported_function_domain",
})
_EVENT_NOT_FOUND_CODES = frozenset({"event_not_found", "remote_event_not_found"})


class MetricsValidationError(ValueError):
    """A malformed observation that must not enter a daily denominator."""

    def __init__(self, code: str, detail: str, *, index: int | None = None):
        self.code = str(code or "metrics_invalid")
        self.detail = str(detail or self.code)
        self.index = index
        suffix = f" (record {index})" if index is not None else ""
        super().__init__(f"{self.code}{suffix}: {self.detail}")

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "ok": False,
            "code": self.code,
            "detail": self.detail,
        }
        if self.index is not None:
            payload["record_index"] = self.index
        return payload


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _pick(value: Any, *paths: str) -> Any:
    """Return the first present value from dotted paths in a mapping."""

    root = _mapping(value)
    for path in paths:
        current: Any = root
        for part in path.split("."):
            if not isinstance(current, Mapping) or part not in current:
                current = _MISSING
                break
            current = current[part]
        if current is not _MISSING and current is not None and current != "":
            return current
    return None


def _pick_sources(sources: Sequence[Mapping[str, Any]], *paths: str) -> Any:
    for source in sources:
        value = _pick(source, *paths)
        if value is not None:
            return value
    return None


def _required_text(value: Any, field: str, *, index: int) -> str:
    if not isinstance(value, str) or not value.strip():
        raise MetricsValidationError(
            "metrics_dimension_required", f"{field} is required", index=index
        )
    text = value.strip()
    if "\n" in text or "\r" in text or not _SAFE_TEXT_RE.fullmatch(text):
        raise MetricsValidationError(
            "metrics_dimension_invalid",
            f"{field} is not a safe identifier",
            index=index,
        )
    return text


def _slug(value: Any, field: str, *, index: int) -> str:
    text = _required_text(value, field, index=index).lower().replace(" ", "_")
    return text


def normalize_entry(value: Any, *, index: int) -> str:
    raw = _slug(value, "entry", index=index)
    aliases = {
        "kafka": "kafka",
        "kafka_ingest": "kafka",
        "kafka_workflow_event": "kafka",
        "kafka_durable_inbox": "kafka",
        "feishu": "feishu",
        "manual": "feishu",
        "feishu_group_manual": "feishu",
        "feishu_thread_reply": "feishu",
        "feishu_issue_comment": "feishu",
    }
    normalized = aliases.get(raw)
    if normalized is None:
        raise MetricsValidationError(
            "metrics_entry_invalid", f"unsupported entry {raw!r}", index=index
        )
    return normalized


def normalize_confidence_tier(value: Any, *, index: int) -> str:
    raw = _slug(value, "confidence_tier", index=index)
    aliases = {
        "high": "high",
        "supported": "high",
        "supported_attribution": "high",
        "high_confidence_supported_attribution": "high",
        "medium": "medium",
        "candidate": "medium",
        "candidate_hypothesis": "medium",
        "medium_confidence_candidate_hypothesis": "medium",
        "low": "low",
        "honest_non_attribution": "low",
        "low_confidence_honest_non_attribution": "low",
        "none": "none",
        "technical_failure": "none",
        "consumer_delivery_failure": "none",
    }
    normalized = aliases.get(raw)
    if normalized is None:
        raise MetricsValidationError(
            "metrics_confidence_tier_invalid", f"unsupported tier {raw!r}", index=index
        )
    return normalized


def normalize_denominator_kind(value: Any, *, index: int) -> str:
    raw = _slug(value, "denominator_kind", index=index)
    aliases = {
        "business": "business",
        "business_clean": "business",
        "business_side": "business",
        "system": "system",
        "system_load": "system",
        "system_side": "system",
    }
    normalized = aliases.get(raw)
    if normalized is None:
        raise MetricsValidationError(
            "metrics_denominator_kind_invalid",
            f"unsupported scope {raw!r}",
            index=index,
        )
    return normalized


def _optional_text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _optional_bool(value: Any, field: str, *, index: int) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip().lower() in {"true", "false"}:
        return value.strip().lower() == "true"
    raise MetricsValidationError(
        "metrics_boolean_invalid", f"{field} must be boolean", index=index
    )


def _optional_nonnegative_int(value: Any, field: str, *, index: int) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise MetricsValidationError(
            "metrics_count_invalid",
            f"{field} must be a non-negative integer",
            index=index,
        )
    return value


def normalize_status(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip().lower().replace("-", "_").replace(" ", "_")


def normalize_attribution_outcome(value: Any) -> str:
    raw = normalize_status(value)
    aliases = {
        "owner_accepted": "owner_accepted",
        "accepted": "owner_accepted",
        "recognized": "owner_accepted",
        "owner_recognized": "owner_accepted",
        "owner_rejected": "owner_rejected",
        "rejected": "owner_rejected",
        "unsupported": "unsupported",
        "business_route_unsupported": "unsupported",
        "event_not_found": "event_not_found",
        "event_notfound": "event_not_found",
        "not_found": "event_not_found",
        "candidate": "candidate",
        "candidate_hypothesis": "candidate",
        "not_attributable": "not_attributable",
        "honest_non_attribution": "not_attributable",
    }
    return aliases.get(raw, raw)


def normalize_decision(value: Any) -> str:
    raw = normalize_status(value)
    return {
        "allow": "allow",
        "allowed": "allow",
        "pass": "allow",
        "approved": "allow",
        "approve": "allow",
        "accepted": "allow",
        "owner_accepted": "allow",
        "recognize": "allow",
        "recognized": "allow",
        "通过": "allow",
        "block": "block",
        "blocked": "block",
        "deny": "block",
        "denied": "block",
        "reject": "block",
        "rejected": "block",
        "retract": "block",
        "invalidated": "block",
        "撤回": "block",
    }.get(raw, raw)


def _sources(raw: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    return (
        raw,
        _mapping(raw.get("contract")),
        _mapping(raw.get("result")),
        _mapping(raw.get("delivery")),
    )


def normalize_record(raw: Mapping[str, Any], *, index: int = 0) -> dict[str, Any]:
    """Normalize one observation without inferring a denominator scope."""

    if not isinstance(raw, Mapping):
        raise MetricsValidationError(
            "metrics_record_invalid", "record must be an object", index=index
        )
    sources = _sources(raw)

    record_id_value = _pick_sources(
        sources, "record_id", "observation_id", "delivery_id", "job_id"
    )
    pair_id_value = _pick_sources(
        sources,
        "pair_id",
        "request_id",
        "business_key",
        "submission_key",
        "work_item_id",
        "record_id",
    )
    if record_id_value is None and pair_id_value is None:
        raise MetricsValidationError(
            "metrics_pair_id_required", "record_id or pair_id is required", index=index
        )
    record_id = _required_text(
        record_id_value or f"offline-{index}", "record_id", index=index
    )
    pair_id = _required_text(pair_id_value or record_id, "pair_id", index=index)

    release_value = _pick_sources(
        sources,
        "release_id",
        "release",
        "release_fingerprint",
        "release_lineage.release_id",
        "pipeline_release_id",
    )
    release = _required_text(release_value, "release", index=index)

    business_value = _pick_sources(
        sources,
        "business",
        "business_line",
        "business_line_ref",
        "business_profile.business_line",
        "business_profile.value.business_line",
        "business_key",
    )
    business = _slug(business_value, "business", index=index)
    business = {
        "g1q3_rca": "g1q3-rca",
        "g1q3-rca": "g1q3-rca",
        "integration_tools": "integration-tools",
        "integration-tools": "integration-tools",
    }.get(business, business)

    entry_value = _pick_sources(
        sources,
        "entry",
        "entrypoint",
        "source_kind",
        "source_metadata.source_kind",
        "creator_source_envelope.source_kind",
    )
    entry = normalize_entry(entry_value, index=index)

    tier_value = _pick_sources(
        sources,
        "confidence_tier",
        "tier",
        "terminal_class",
        "quality_classification",
        "quality_oracle.confidence_tier",
    )
    confidence_tier = normalize_confidence_tier(tier_value, index=index)

    scope_value = _pick_sources(
        sources,
        "denominator_kind",
        "denominator_scope",
        "scope",
        "business_scope",
    )
    denominator_kind = normalize_denominator_kind(scope_value, index=index)

    e2e = _mapping(_pick(raw, "e2e", "e2e_result", "execution"))
    technical = _mapping(
        _pick(raw, "technical", "technical_delivery", "delivery_readback")
    )
    delivery = _mapping(_pick(raw, "delivery"))
    readback = _mapping(_pick(raw, "readback"))
    attribution = _mapping(_pick(raw, "attribution", "useful_attribution"))
    golden = _mapping(_pick(raw, "golden", "golden_result", "regression"))
    signals = _mapping(_pick(raw, "signals"))
    triage = _mapping(_pick(signals, "triage")) or _mapping(_pick(raw, "triage"))
    rca_signal = _mapping(_pick(signals, "rca", "attribution")) or attribution
    gate = _mapping(_pick(signals, "gate")) or _mapping(_pick(raw, "gate"))

    delivery_status = normalize_status(
        _pick(technical, "delivery_status", "delivery", "delivery_outcome")
        or _pick(delivery, "status", "outcome")
    )
    readback_status = normalize_status(
        _pick(technical, "readback_status", "readback", "readback_outcome")
        or _pick(readback, "status", "outcome")
    )
    e2e_status = normalize_status(
        _pick(e2e, "status", "outcome", "result") or _pick(raw, "e2e_status")
    )

    attribution_outcome = normalize_attribution_outcome(
        _pick(attribution, "outcome", "status", "result")
        or _pick(raw, "attribution_outcome")
    )
    owner_decision = normalize_decision(
        _pick(attribution, "owner_decision", "owner_review", "review_decision")
        or _pick(rca_signal, "owner_decision", "owner_review", "review_decision")
    )

    triage_kind = _optional_text(_pick(triage, "kind", "判定_kind", "classification"))
    triage_expected_kind = _optional_text(
        _pick(triage, "expected_kind", "golden_kind", "owner_kind")
    )
    triage_correct = _optional_bool(
        _pick(triage, "correct", "is_correct"), "signals.triage.correct", index=index
    )

    gate_decision = normalize_decision(_pick(gate, "decision", "allow_block", "result"))
    gate_review_decision = normalize_decision(
        _pick(
            gate,
            "review_decision",
            "human_decision",
            "owner_decision",
            "expected_decision",
        )
    )

    golden_evaluated = _optional_bool(
        _pick(golden, "evaluated", "available", "covered"),
        "golden.evaluated",
        index=index,
    )
    false_high_confidence = _optional_bool(
        _pick(
            golden,
            "false_high_confidence",
            "false_high",
            "high_confidence_false_positive",
        ),
        "golden.false_high_confidence",
        index=index,
    )
    golden_regression = _optional_bool(
        _pick(golden, "regression", "no_regression_failed", "failed"),
        "golden.regression",
        index=index,
    )

    auxiliary_raw = _mapping(_pick(raw, "auxiliary"))
    auxiliary_counts: dict[str, int] = {}
    for name, paths in {
        "coverage_count": ("coverage_count", "coverage.count"),
        "report_count": ("report_count", "reports.count"),
        "field_write_count": ("field_write_count", "field_writes.count"),
    }.items():
        count = _optional_nonnegative_int(
            _pick_sources(sources, *paths),
            name,
            index=index,
        )
        if count is not None:
            auxiliary_counts[name] = count

    return {
        "schema_version": SCHEMA_VERSION,
        "record_id": record_id,
        "pair_id": pair_id,
        "release": release,
        "business": business,
        "entry": entry,
        "confidence_tier": confidence_tier,
        "denominator_kind": denominator_kind,
        "e2e_status": e2e_status,
        "delivery_status": delivery_status,
        "readback_status": readback_status,
        "attribution_outcome": attribution_outcome,
        "owner_decision": owner_decision,
        "triage_kind": triage_kind,
        "triage_expected_kind": triage_expected_kind,
        "triage_correct": triage_correct,
        "gate_decision": gate_decision,
        "gate_review_decision": gate_review_decision,
        "golden_evaluated": golden_evaluated,
        "false_high_confidence": false_high_confidence,
        "golden_regression": golden_regression,
        # These fields are deliberately auxiliary and are never used as a
        # numerator or denominator for the four W12 metrics.
        "auxiliary": {
            **dict(auxiliary_raw),
            **auxiliary_counts,
        },
    }


def normalize_records(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    if not isinstance(records, Sequence) or isinstance(
        records, (str, bytes, bytearray)
    ):
        raise MetricsValidationError(
            "metrics_records_invalid", "records must be an array"
        )
    return [normalize_record(item, index=index) for index, item in enumerate(records)]


def _dispatch_rate(numerator: int, denominator: int) -> dict[str, int | float | None]:
    return {
        "numerator": numerator,
        "denominator": denominator,
        "rate_pct": (
            round(100.0 * numerator / denominator, 1) if denominator > 0 else None
        ),
    }


def _dispatch_choice(
    value: Any,
    *,
    field: str,
    allowed: Sequence[str] | frozenset[str],
    index: int,
) -> str:
    normalized = normalize_status(value)
    if normalized not in allowed:
        raise MetricsValidationError(
            "metrics_upstream_dispatch_value_invalid",
            f"{field} must be one of {sorted(allowed)}, got {normalized!r}",
            index=index,
        )
    return normalized


def _dispatch_cohort(counts: Mapping[str, int]) -> dict[str, Any]:
    valid_dispatches = int(counts.get("valid_dispatch", 0))
    reviewed = int(counts.get("reviewed", 0))
    correct = int(counts.get("reviewed_correct", 0))
    return {
        "valid_dispatch_count": valid_dispatches,
        "reviewed_count": reviewed,
        "reviewed_correct_count": correct,
        "dispatch_accuracy": _dispatch_rate(correct, reviewed),
        "review_coverage": _dispatch_rate(reviewed, valid_dispatches),
    }


def reduce_upstream_dispatch_metrics(
    records: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Reduce terminal issue receipts into the W12 upstream-dispatch readout.

    Each issue must have exactly one terminal class.  A technical failure whose
    30-minute fallback has completed is normalized to ``abstain_no_hit`` with
    ``abstain_reason=timeout_fallback``; only unreduced technical failures stay
    outside ``denominator_base``.  Review validity is a sampling-quality gate,
    not an accuracy threshold.
    """

    if not isinstance(records, Sequence) or isinstance(
        records, (str, bytes, bytearray)
    ):
        raise MetricsValidationError(
            "metrics_upstream_dispatch_records_invalid",
            "records must be an array",
        )

    terminal_counts = {
        terminal_class: 0 for terminal_class in UPSTREAM_DISPATCH_TERMINAL_CLASSES
    }
    abstain_counts = {
        reason: 0 for reason in UPSTREAM_DISPATCH_ABSTAIN_REASONS
    }
    cohort_counts: dict[str, dict[str, int]] = {
        tier: {"valid_dispatch": 0, "reviewed": 0, "reviewed_correct": 0}
        for tier in ("high", "medium", "unspecified")
    }
    issue_ids: set[str] = set()
    reviewed_count = 0
    reviewed_correct_count = 0
    timeout_fallback_count = 0
    dispatch_bucket_counts = {
        bucket: 0 for bucket in sorted(UPSTREAM_DISPATCH_OWNER_BUCKETS)
    }

    for index, raw in enumerate(records):
        if not isinstance(raw, Mapping):
            raise MetricsValidationError(
                "metrics_upstream_dispatch_record_invalid",
                "record must be an object",
                index=index,
            )
        issue_id = _required_text(
            _pick(raw, "issue_id", "work_item_id", "record_id"),
            "issue_id",
            index=index,
        )
        if issue_id in issue_ids:
            raise MetricsValidationError(
                "metrics_upstream_dispatch_duplicate_issue",
                f"issue_id is repeated: {issue_id}",
                index=index,
            )
        issue_ids.add(issue_id)

        source_terminal_class = _dispatch_choice(
            raw.get("terminal_class"),
            field="terminal_class",
            allowed=UPSTREAM_DISPATCH_TERMINAL_CLASSES,
            index=index,
        )
        timeout_fallback_applied = _optional_bool(
            _pick(
                raw,
                "timeout_fallback_applied",
                "technical_failure.timeout_fallback_applied",
            ),
            "timeout_fallback_applied",
            index=index,
        )
        abstain_reason_raw = _pick(raw, "abstain_reason")

        terminal_class = source_terminal_class
        if source_terminal_class == "technical_failure" and timeout_fallback_applied:
            if abstain_reason_raw is not None:
                reduced_reason = _dispatch_choice(
                    abstain_reason_raw,
                    field="abstain_reason",
                    allowed=UPSTREAM_DISPATCH_ABSTAIN_REASONS,
                    index=index,
                )
                if reduced_reason != "timeout_fallback":
                    raise MetricsValidationError(
                        "metrics_upstream_dispatch_timeout_fallback_mismatch",
                        "a reduced technical failure must use timeout_fallback",
                        index=index,
                    )
            terminal_class = "abstain_no_hit"
            abstain_reason = "timeout_fallback"
            timeout_fallback_count += 1
        elif source_terminal_class == "abstain_no_hit":
            abstain_reason = (
                _dispatch_choice(
                    abstain_reason_raw,
                    field="abstain_reason",
                    allowed=UPSTREAM_DISPATCH_ABSTAIN_REASONS,
                    index=index,
                )
                if abstain_reason_raw is not None
                else "no_hit"
            )
            if abstain_reason not in {
                "no_hit",
                "input_incomplete",
                "capability_degraded",
                "timeout_fallback",
            }:
                raise MetricsValidationError(
                    "metrics_upstream_dispatch_abstain_reason_mismatch",
                    f"{abstain_reason} cannot use abstain_no_hit",
                    index=index,
                )
            if abstain_reason == "timeout_fallback":
                timeout_fallback_count += 1
        elif source_terminal_class == "abstain_cross_domain":
            abstain_reason = (
                _dispatch_choice(
                    abstain_reason_raw,
                    field="abstain_reason",
                    allowed=UPSTREAM_DISPATCH_ABSTAIN_REASONS,
                    index=index,
                )
                if abstain_reason_raw is not None
                else "cross_domain"
            )
            if abstain_reason != "cross_domain":
                raise MetricsValidationError(
                    "metrics_upstream_dispatch_abstain_reason_mismatch",
                    f"{abstain_reason} cannot use abstain_cross_domain",
                    index=index,
                )
        else:
            abstain_reason = ""
            if abstain_reason_raw is not None:
                raise MetricsValidationError(
                    "metrics_upstream_dispatch_abstain_reason_mismatch",
                    f"{source_terminal_class} cannot have abstain_reason",
                    index=index,
                )

        if (
            timeout_fallback_applied
            and source_terminal_class != "technical_failure"
            and abstain_reason != "timeout_fallback"
        ):
            raise MetricsValidationError(
                "metrics_upstream_dispatch_timeout_fallback_mismatch",
                "timeout fallback must reduce to abstain_reason=timeout_fallback",
                index=index,
            )

        review = _mapping(raw.get("review"))
        reviewed_value = (
            raw.get("reviewed") if "reviewed" in raw else review.get("reviewed")
        )
        correct_value = (
            raw.get("is_correct")
            if "is_correct" in raw
            else review.get("is_correct")
        )
        reviewed = _optional_bool(reviewed_value, "reviewed", index=index)
        is_correct = _optional_bool(correct_value, "is_correct", index=index)
        if reviewed is None:
            reviewed = is_correct is not None
        if reviewed and is_correct is None:
            raise MetricsValidationError(
                "metrics_upstream_dispatch_review_incomplete",
                "a reviewed dispatch requires is_correct",
                index=index,
            )
        if not reviewed and is_correct is not None:
            raise MetricsValidationError(
                "metrics_upstream_dispatch_review_mismatch",
                "an unreviewed dispatch cannot have is_correct",
                index=index,
            )
        system_bucket_raw = _pick(raw, "system_bucket")
        corrected_bucket_raw = _pick(raw, "corrected_bucket")

        terminal_counts[terminal_class] += 1
        if terminal_class in {"abstain_no_hit", "abstain_cross_domain"}:
            if system_bucket_raw is not None or corrected_bucket_raw is not None:
                raise MetricsValidationError(
                    "metrics_upstream_dispatch_bucket_mismatch",
                    "an abstention cannot have owner buckets",
                    index=index,
                )
            if reviewed:
                raise MetricsValidationError(
                    "metrics_upstream_dispatch_review_mismatch",
                    "an abstention cannot enter the review sample",
                    index=index,
                )
            abstain_counts[abstain_reason] += 1
            continue

        if terminal_class != "valid_dispatch":
            if system_bucket_raw is not None or corrected_bucket_raw is not None:
                raise MetricsValidationError(
                    "metrics_upstream_dispatch_bucket_mismatch",
                    f"{terminal_class} cannot have owner buckets",
                    index=index,
                )
            if reviewed:
                raise MetricsValidationError(
                    "metrics_upstream_dispatch_review_mismatch",
                    f"{terminal_class} cannot enter the review sample",
                    index=index,
                )
            continue

        system_bucket = _dispatch_choice(
            system_bucket_raw,
            field="system_bucket",
            allowed=UPSTREAM_DISPATCH_OWNER_BUCKETS,
            index=index,
        )
        dispatch_bucket_counts[system_bucket] += 1
        if not reviewed and corrected_bucket_raw is not None:
            raise MetricsValidationError(
                "metrics_upstream_dispatch_review_mismatch",
                "an unreviewed dispatch cannot have corrected_bucket",
                index=index,
            )
        if reviewed and is_correct:
            if corrected_bucket_raw is not None:
                raise MetricsValidationError(
                    "metrics_upstream_dispatch_review_mismatch",
                    "a correct dispatch cannot have corrected_bucket",
                    index=index,
                )
        elif reviewed:
            corrected_bucket = _dispatch_choice(
                corrected_bucket_raw,
                field="corrected_bucket",
                allowed=UPSTREAM_DISPATCH_OWNER_BUCKETS,
                index=index,
            )
            if corrected_bucket == system_bucket:
                raise MetricsValidationError(
                    "metrics_upstream_dispatch_review_mismatch",
                    "an incorrect dispatch must select a different bucket",
                    index=index,
                )
        tier_raw = raw.get("confidence_tier")
        if tier_raw is None or tier_raw == "":
            tier = "unspecified"
        else:
            tier = normalize_confidence_tier(tier_raw, index=index)
            if tier not in {"high", "medium"}:
                raise MetricsValidationError(
                    "metrics_upstream_dispatch_tier_invalid",
                    "a valid dispatch tier must be high or medium",
                    index=index,
                )
        cohort_counts[tier]["valid_dispatch"] += 1
        if reviewed:
            reviewed_count += 1
            cohort_counts[tier]["reviewed"] += 1
            if is_correct:
                reviewed_correct_count += 1
                cohort_counts[tier]["reviewed_correct"] += 1

    terminal_total = len(records)
    unreduced_technical_failures = terminal_counts["technical_failure"]
    denominator_base = (
        terminal_total
        - terminal_counts["out_of_scope"]
        - unreduced_technical_failures
    )
    valid_dispatch_count = terminal_counts["valid_dispatch"]
    abstain_count = (
        terminal_counts["abstain_no_hit"]
        + terminal_counts["abstain_cross_domain"]
    )
    review_coverage_is_sufficient = (
        reviewed_count >= UPSTREAM_DISPATCH_REVIEWED_COUNT_MIN
        and valid_dispatch_count > 0
        and reviewed_count * 100
        >= valid_dispatch_count * UPSTREAM_DISPATCH_REVIEW_COVERAGE_MIN_PCT
    )

    return {
        "ok": True,
        "schema_version": UPSTREAM_DISPATCH_SCHEMA_VERSION,
        "terminal_total": terminal_total,
        "terminal_counts": terminal_counts,
        "denominator_base": denominator_base,
        "technical_failure_breakdown": {
            "unreduced": unreduced_technical_failures,
            "timeout_fallback_reduced_to_abstain": timeout_fallback_count,
        },
        "metrics": {
            "dispatch_accuracy": _dispatch_rate(
                reviewed_correct_count, reviewed_count
            ),
            "dispatch_coverage": _dispatch_rate(
                valid_dispatch_count, denominator_base
            ),
            "abstain_rate": _dispatch_rate(abstain_count, denominator_base),
            "review_coverage": _dispatch_rate(
                reviewed_count, valid_dispatch_count
            ),
        },
        "abstain_breakdown": {
            reason: {
                "count": count,
                "denominator": denominator_base,
                "rate_pct": (
                    round(100.0 * count / denominator_base, 1)
                    if denominator_base > 0
                    else None
                ),
            }
            for reason, count in abstain_counts.items()
        },
        "dispatch_bucket_counts": dispatch_bucket_counts,
        "by_confidence_tier": {
            tier: _dispatch_cohort(counts)
            for tier, counts in cohort_counts.items()
        },
        "readout": {
            "status": (
                "valid"
                if review_coverage_is_sufficient
                else "insufficient_review_coverage"
            ),
            "ga_first_reading_gate_satisfied": review_coverage_is_sufficient,
            "reviewed_count": reviewed_count,
            "review_coverage_min_pct": (
                UPSTREAM_DISPATCH_REVIEW_COVERAGE_MIN_PCT
            ),
            "reviewed_count_min": UPSTREAM_DISPATCH_REVIEWED_COUNT_MIN,
            "accuracy_threshold_applied": False,
        },
    }


def load_records(path: str | Path) -> list[dict[str, Any]]:
    """Load a bounded JSON array/object or JSONL fixture from disk."""

    source = Path(path).expanduser()
    try:
        raw = source.read_bytes()
    except OSError as exc:
        raise MetricsValidationError("metrics_input_unavailable", str(source)) from exc
    if len(raw) > MAX_INPUT_BYTES:
        raise MetricsValidationError("metrics_input_too_large", str(source))
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise MetricsValidationError(
            "metrics_input_encoding_invalid", str(source)
        ) from exc
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        rows: list[Mapping[str, Any]] = []
        for line_no, line in enumerate(text.splitlines(), start=1):
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError as exc:
                raise MetricsValidationError(
                    "metrics_jsonl_invalid", f"line {line_no}: {exc.msg}"
                ) from exc
            if not isinstance(item, Mapping):
                raise MetricsValidationError(
                    "metrics_record_invalid", f"line {line_no} is not an object"
                )
            rows.append(item)
        return normalize_records(rows)
    if isinstance(parsed, Mapping):
        parsed = parsed.get("records")
    if not isinstance(parsed, list):
        raise MetricsValidationError(
            "metrics_records_invalid", "input must be an array or {records: []}"
        )
    return normalize_records(parsed)


def _identity(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise MetricsValidationError(
            "metrics_observation_identity_invalid", f"{field} is required"
        )
    text = value.strip()
    if not _SAFE_TEXT_RE.fullmatch(text):
        raise MetricsValidationError(
            "metrics_observation_identity_invalid", f"{field} is invalid"
        )
    return text


def _timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise MetricsValidationError(
            "metrics_observation_window_invalid", f"{field} is required"
        )
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise MetricsValidationError(
            "metrics_observation_window_invalid", f"{field} must be ISO-8601"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise MetricsValidationError(
            "metrics_observation_window_invalid", f"{field} needs a timezone"
        )
    return parsed.astimezone(timezone.utc)


def _strict_json_object(value: Any, field: str) -> dict[str, Any]:
    def unique_object(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, item in items:
            if key in result:
                raise MetricsValidationError(
                    "metrics_observation_json_invalid", f"{field} has duplicate keys"
                )
            result[key] = item
        return result

    def invalid_number(_value: str) -> None:
        raise MetricsValidationError(
            "metrics_observation_json_invalid", f"{field} has a non-finite number"
        )

    try:
        parsed = json.loads(
            str(value),
            object_pairs_hook=unique_object,
            parse_constant=invalid_number,
        )
    except MetricsValidationError:
        raise
    except (TypeError, json.JSONDecodeError) as exc:
        raise MetricsValidationError(
            "metrics_observation_json_invalid", f"{field} is not valid JSON"
        ) from exc
    if not isinstance(parsed, dict):
        raise MetricsValidationError(
            "metrics_observation_json_invalid", f"{field} must be an object"
        )
    return parsed


def _load_golden_input(
    path: str | Path, *, release_id: str, pipeline_commit: str
) -> dict[tuple[str, int], dict[str, Any]]:
    source = Path(path).expanduser().absolute()
    try:
        before = source.stat()
        raw = source.read_bytes()
        after = source.stat()
    except OSError as exc:
        raise MetricsValidationError(
            "metrics_golden_input_unavailable", str(source)
        ) from exc
    if (
        not stat.S_ISREG(before.st_mode)
        or len(raw) > MAX_INPUT_BYTES
        or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    ):
        raise MetricsValidationError(
            "metrics_golden_input_invalid", "golden input is not a stable regular file"
        )
    try:
        payload = _strict_json_object(raw.decode("utf-8"), "golden_input")
    except UnicodeDecodeError as exc:
        raise MetricsValidationError(
            "metrics_golden_input_invalid", "golden input must be UTF-8"
        ) from exc
    records = payload.get("records")
    if (
        payload.get("schema_version") != GOLDEN_INPUT_SCHEMA_VERSION
        or not isinstance(records, list)
        or not records
    ):
        raise MetricsValidationError(
            "metrics_golden_input_invalid", "golden input schema or records are invalid"
        )
    indexed: dict[tuple[str, int], dict[str, Any]] = {}
    for index, item in enumerate(records):
        if not isinstance(item, Mapping):
            raise MetricsValidationError(
                "metrics_golden_input_invalid",
                "golden record must be an object",
                index=index,
            )
        business_key = _required_text(
            item.get("business_key"), "business_key", index=index
        )
        generation = item.get("generation")
        evaluated = item.get("evaluated")
        false_high = item.get("false_high_confidence")
        regression = item.get("regression")
        expected_terminal_class = _required_text(
            item.get("expected_terminal_class"),
            "expected_terminal_class",
            index=index,
        )
        expected_gate = normalize_decision(item.get("expected_gate_decision"))
        if (
            isinstance(generation, bool)
            or not isinstance(generation, int)
            or generation < 1
            or item.get("release_id") != release_id
            or item.get("pipeline_commit") != pipeline_commit
            or not isinstance(evaluated, bool)
            or expected_gate not in {"allow", "block"}
            or (
                evaluated
                and (
                    not isinstance(false_high, bool) or not isinstance(regression, bool)
                )
            )
            or (not evaluated and (false_high is not None or regression is not None))
        ):
            raise MetricsValidationError(
                "metrics_golden_binding_invalid",
                "golden row must match release/pipeline and contain valid results",
                index=index,
            )
        key = (business_key, generation)
        if key in indexed:
            raise MetricsValidationError(
                "metrics_golden_binding_duplicate",
                f"duplicate golden key {business_key}/{generation}",
                index=index,
            )
        indexed[key] = {
            "business_key": business_key,
            "generation": generation,
            "release_id": release_id,
            "pipeline_commit": pipeline_commit,
            "evaluated": evaluated,
            "false_high_confidence": false_high,
            "regression": regression,
            "expected_terminal_class": expected_terminal_class,
            "expected_gate_decision": expected_gate,
        }
    return indexed


def _validate_sqlite_schema(conn: sqlite3.Connection) -> None:
    tables = {
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    missing_tables = sorted(set(_SQLITE_REQUIRED_COLUMNS) - tables)
    if missing_tables:
        raise MetricsValidationError(
            "metrics_control_db_schema_missing",
            f"required tables missing: {missing_tables}",
        )
    for table, required in _SQLITE_REQUIRED_COLUMNS.items():
        columns = {
            str(row[1])
            for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
        missing = sorted(required - columns)
        if missing:
            raise MetricsValidationError(
                "metrics_control_db_schema_missing",
                f"{table} columns missing: {missing}",
            )
    markers = {
        "control": conn.execute(
            "SELECT value FROM control_meta WHERE key = 'schema_version'"
        ).fetchone(),
        "delivery": conn.execute(
            "SELECT value FROM rca_delivery_meta WHERE key = 'schema_version'"
        ).fetchone(),
    }
    if (
        markers["control"] is None
        or str(markers["control"][0]) not in SUPPORTED_CONTROL_STORE_SCHEMA_VERSIONS
        or markers["delivery"] is None
        or str(markers["delivery"][0]) not in SUPPORTED_DELIVERY_STORE_SCHEMA_VERSIONS
    ):
        raise MetricsValidationError(
            "metrics_control_db_schema_mismatch",
            "supported control v11/v12/v13 and delivery v9 markers are required",
        )
    triggers = {
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'trigger'"
        ).fetchall()
    }
    missing_triggers = sorted(_IMMUTABLE_TRIGGER_NAMES - triggers)
    if missing_triggers:
        raise MetricsValidationError(
            "metrics_control_db_schema_missing",
            f"immutable ledger triggers missing: {missing_triggers}",
        )


def _bounded_rows(
    conn: sqlite3.Connection, sql: str, *, limit: int = MAX_SQLITE_JOBS
) -> list[sqlite3.Row]:
    cursor = conn.execute(sql)
    rows = cursor.fetchmany(limit + 1)
    if len(rows) > limit:
        raise MetricsValidationError(
            "metrics_control_db_too_many_rows", f"query exceeds {limit} rows"
        )
    return rows


def _canonical_json_sha256(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _effect_observation(
    job: Mapping[str, Any], effects: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    required: list[Mapping[str, Any]] = []
    for row in effects:
        required_value = row.get("required")
        if isinstance(required_value, bool) or required_value not in (0, 1):
            raise MetricsValidationError(
                "metrics_delivery_effect_invalid",
                f"effect {row.get('effect_key')} required flag is invalid",
            )
        if required_value == 1:
            required.append(row)
    if not required:
        raise MetricsValidationError(
            "metrics_delivery_effect_missing",
            f"delivery {job['delivery_id']} has no required effect",
        )
    for row in effects:
        if row in required:
            continue
        effect_key = str(row.get("effect_key") or "")
        payload = _strict_json_object(
            row.get("payload_json"), f"effect[{effect_key}].payload_json"
        )
        if str(payload.get("schema_version") or "") not in (
            _DELIVERY_EFFECT_SCHEMA_VERSIONS | _ADJUDICATION_EFFECT_SCHEMA_VERSIONS
        ):
            raise MetricsValidationError(
                "metrics_delivery_effect_schema_invalid",
                f"effect {effect_key} has unsupported schema",
            )
    codes = {
        normalize_status(job.get(field))
        for field in ("outcome", "outcome_key", "terminal_state", "terminal_error_code")
        if normalize_status(job.get(field))
    }
    oracle_by_hash: dict[str, dict[str, Any]] = {}
    payloads: dict[str, dict[str, Any]] = {}
    receipts_valid = True
    required_succeeded = True
    for effect in required:
        effect_key = str(effect["effect_key"])
        payload = _strict_json_object(
            effect["payload_json"], f"effect[{effect_key}].payload_json"
        )
        payloads[effect_key] = payload
        schema_version = str(payload.get("schema_version") or "")
        if (
            schema_version not in _DELIVERY_EFFECT_SCHEMA_VERSIONS
            and schema_version not in _ADJUDICATION_EFFECT_SCHEMA_VERSIONS
        ):
            raise MetricsValidationError(
                "metrics_delivery_effect_schema_invalid",
                f"effect {effect_key} has unsupported schema",
            )
        for field in ("outcome", "error_code", "diagnostic_code", "terminal_state"):
            code = normalize_status(payload.get(field))
            if code:
                codes.add(code)
        fallback = payload.get("terminal_fallback")
        if isinstance(fallback, Mapping):
            for field in ("outcome", "error_code", "kind", "terminal_state"):
                code = normalize_status(fallback.get(field))
                if code:
                    codes.add(code)
        oracle = payload.get("quality_oracle")
        if oracle is not None:
            if not isinstance(oracle, Mapping):
                raise MetricsValidationError(
                    "metrics_oracle_invalid", f"effect {effect_key} oracle is invalid"
                )
            oracle = dict(oracle)
            digest = _canonical_json_sha256(oracle)
            if (
                oracle.get("schema_version") != ORACLE_SCHEMA_VERSION
                or payload.get("quality_oracle_sha256") != digest
                or payload.get("terminal_class") != oracle.get("terminal_class")
                or payload.get("confidence_tier") != oracle.get("confidence_tier")
                or oracle.get("confidence_tier") not in CONFIDENCE_TIERS
                or not isinstance(oracle.get("facts"), Mapping)
                or oracle.get("publication_allowed") is not True
                or oracle.get("classification_conflict") is not False
            ):
                raise MetricsValidationError(
                    "metrics_oracle_invalid",
                    f"effect {effect_key} oracle binding is invalid",
                )
            oracle_by_hash[digest] = oracle
        succeeded = (
            normalize_status(effect.get("status")) == "succeeded"
            and normalize_status(effect.get("write_phase")) == "settled"
        )
        required_succeeded = required_succeeded and succeeded
        if succeeded:
            try:
                receipt = _strict_json_object(
                    effect["remote_receipt_json"],
                    f"effect[{effect_key}].remote_receipt_json",
                )
            except MetricsValidationError:
                receipts_valid = False
            else:
                receipts_valid = (
                    receipts_valid
                    and bool(receipt)
                    and (
                        receipt.get("source")
                        in {"read_before_write", "read_after_write"}
                    )
                )
        else:
            receipts_valid = False
    if len(oracle_by_hash) > 1:
        raise MetricsValidationError(
            "metrics_oracle_conflict",
            f"delivery {job['delivery_id']} has conflicting W1 oracle results",
        )
    oracle_sha256 = next(iter(oracle_by_hash), "")
    oracle = oracle_by_hash.get(oracle_sha256)
    diagnostic_codes = sorted(codes & _TERMINAL_DIAGNOSTIC_CODES)
    if oracle is None and not diagnostic_codes:
        raise MetricsValidationError(
            "metrics_oracle_missing",
            f"delivery {job['delivery_id']} has neither W1 oracle nor terminal diagnostic",
        )
    terminal_class = (
        str(oracle["terminal_class"]) if oracle is not None else diagnostic_codes[0]
    )
    confidence_tier = str(oracle["confidence_tier"]) if oracle is not None else "none"
    delivery_succeeded = (
        normalize_status(job.get("status")) == "delivered" and required_succeeded
    )
    readback_succeeded = delivery_succeeded and receipts_valid
    return {
        "codes": codes,
        "terminal_class": terminal_class,
        "confidence_tier": confidence_tier,
        "oracle": oracle,
        "oracle_sha256": oracle_sha256,
        "delivery_status": "succeeded" if delivery_succeeded else "failed",
        "readback_status": "verified" if readback_succeeded else "failed",
        "e2e_status": "success" if readback_succeeded else "failed",
        "effect_keys": sorted(payloads),
    }


def _business_dimension(project_key: Any) -> str:
    project = str(project_key or "").strip().lower().replace("_", "-")
    if project in {"g1q3", "t03o4q"}:
        return "g1q3-rca"
    if not project or not _SAFE_TEXT_RE.fullmatch(project):
        raise MetricsValidationError(
            "metrics_business_dimension_invalid", "project_key is invalid"
        )
    return project


def load_sqlite_observations(
    control_db: str | Path,
    *,
    release_id: str,
    pipeline_commit: str,
    window_start: str,
    window_end: str,
    golden_input: str | Path,
) -> list[dict[str, Any]]:
    """Produce normalized W12 rows from a checkpointed, read-only control DB."""

    release = _identity(release_id, "release_id")
    pipeline = str(pipeline_commit or "").strip().lower()
    if _SHA1_RE.fullmatch(pipeline) is None:
        raise MetricsValidationError(
            "metrics_pipeline_identity_invalid", "pipeline_commit must be 40 hex"
        )
    start = _timestamp(window_start, "window_start")
    end = _timestamp(window_end, "window_end")
    if end <= start or (end - start).total_seconds() > 24 * 60 * 60:
        raise MetricsValidationError(
            "metrics_observation_window_invalid",
            "daily window must be positive and at most 24 hours",
        )
    golden = _load_golden_input(
        golden_input, release_id=release, pipeline_commit=pipeline
    )

    source = Path(control_db).expanduser().absolute().resolve()
    wal = Path(f"{source}-wal")
    try:
        before = source.stat()
        wal_before = wal.stat() if wal.exists() else None
    except OSError as exc:
        raise MetricsValidationError(
            "metrics_control_db_unavailable", str(source)
        ) from exc
    if not stat.S_ISREG(before.st_mode):
        raise MetricsValidationError(
            "metrics_control_db_unavailable", "control DB is not a regular file"
        )
    if wal_before is not None and wal_before.st_size > 0:
        raise MetricsValidationError(
            "metrics_control_db_wal_present",
            "immutable observation requires a checkpointed SQLite snapshot",
        )

    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(
            f"{source.as_uri()}?mode=ro&immutable=1",
            uri=True,
            isolation_level=None,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only = ON")
        if int(conn.execute("PRAGMA query_only").fetchone()[0]) != 1:
            raise MetricsValidationError(
                "metrics_control_db_not_read_only", "query_only could not be enabled"
            )
        _validate_sqlite_schema(conn)
        jobs = _bounded_rows(
            conn,
            """
            SELECT delivery_id, submission_key, business_key, generation,
                   project_key, work_item_id, outcome, outcome_key,
                   terminal_state, terminal_error_code, status, contract_json,
                   created_at, updated_at
              FROM rca_delivery_jobs
             ORDER BY created_at, delivery_id
            """,
        )
        selected: dict[tuple[str, int], dict[str, Any]] = {}
        for row in jobs:
            created = _timestamp(row["created_at"], "rca_delivery_jobs.created_at")
            if not (start <= created < end):
                continue
            item = dict(row)
            key = (str(item["business_key"]), int(item["generation"]))
            if key in selected:
                raise MetricsValidationError(
                    "metrics_delivery_identity_duplicate",
                    f"multiple deliveries for {key[0]}/{key[1]}",
                )
            selected[key] = item
        if not selected:
            raise MetricsValidationError(
                "metrics_no_records", "daily control DB window has no delivery jobs"
            )
        missing_golden = sorted(set(selected) - set(golden))
        extra_golden = sorted(set(golden) - set(selected))
        if missing_golden or extra_golden:
            raise MetricsValidationError(
                "metrics_golden_binding_incomplete",
                f"missing={missing_golden[:20]} extra={extra_golden[:20]}",
            )

        snapshots: dict[tuple[str, int], list[dict[str, Any]]] = {}
        for row in _bounded_rows(
            conn,
            """
            SELECT snapshot_sha256, business_key, submission_key, generation,
                   execution_decision
              FROM rca_admission_snapshots
             ORDER BY business_key, generation
            """,
        ):
            item = dict(row)
            snapshots.setdefault(
                (str(item["business_key"]), int(item["generation"])), []
            ).append(item)

        envelopes: dict[str, list[dict[str, Any]]] = {}
        for row in _bounded_rows(
            conn,
            """
            SELECT e.source_envelope_sha256, e.snapshot_sha256, e.submission_key,
                   e.source_authority_sha256, e.source_id, e.source_kind,
                   e.payload_sha256, e.authorization_evidence_sha256,
                   e.binding_action, e.decision, e.source_metadata_json,
                   a.authority_sha256 AS matched_authority_sha256
              FROM rca_snapshot_source_envelopes AS e
         LEFT JOIN rca_source_authority_receipts AS a
                ON a.authority_sha256 = e.source_authority_sha256
               AND a.source_id = e.source_id
               AND a.source_kind = e.source_kind
               AND a.payload_sha256 = e.payload_sha256
               AND a.authorization_evidence_sha256 =
                   e.authorization_evidence_sha256
               AND a.binding_action = e.binding_action
               AND a.decision = e.decision
             ORDER BY e.snapshot_sha256, e.source_envelope_sha256
            """,
            limit=MAX_SQLITE_JOBS * 8,
        ):
            item = dict(row)
            envelopes.setdefault(str(item["snapshot_sha256"]), []).append(item)

        effects: dict[str, list[dict[str, Any]]] = {}
        for row in _bounded_rows(
            conn,
            """
            SELECT effect_key, delivery_id, effect_kind, required, payload_json,
                   status, write_phase, remote_receipt_json, completed_at, updated_at
              FROM rca_delivery_effects
             ORDER BY delivery_id, effect_key
            """,
            limit=MAX_SQLITE_JOBS * 16,
        ):
            item = dict(row)
            effects.setdefault(str(item["delivery_id"]), []).append(item)

        adjudications: dict[tuple[str, int], list[dict[str, Any]]] = {}
        for row in _bounded_rows(
            conn,
            """
            SELECT adjudication_id, schema_version, business_key, generation,
                   work_item_id, action, conclusion_state, actor_id,
                   original_delivery_id, original_effect_key,
                   correction_effect_key, activation_epoch_id, created_at
              FROM rca_conclusion_adjudications
             ORDER BY created_at, adjudication_id
            """,
        ):
            item = dict(row)
            if _timestamp(item["created_at"], "adjudication.created_at") >= end:
                continue
            adjudications.setdefault(
                (str(item["business_key"]), int(item["generation"])), []
            ).append(item)
        repair_status = {
            str(row["adjudication_id"]): str(row["status"])
            for row in _bounded_rows(
                conn,
                "SELECT adjudication_id, status "
                "FROM rca_conclusion_adjudication_repairs",
            )
        }

        observations: list[dict[str, Any]] = []
        source_envelope_ids: set[str] = set()
        source_ids: set[str] = set()
        for key in sorted(selected):
            job = selected[key]
            snapshot_rows = snapshots.get(key, [])
            if len(snapshot_rows) != 1:
                raise MetricsValidationError(
                    "metrics_snapshot_binding_invalid",
                    f"expected one admission snapshot for {key[0]}/{key[1]}",
                )
            snapshot = snapshot_rows[0]
            if (
                snapshot["submission_key"] != job["submission_key"]
                or snapshot["execution_decision"] != "admit"
            ):
                raise MetricsValidationError(
                    "metrics_snapshot_binding_invalid",
                    f"delivery/snapshot binding is invalid for {key[0]}/{key[1]}",
                )
            source_rows = envelopes.get(str(snapshot["snapshot_sha256"]), [])
            if not source_rows:
                raise MetricsValidationError(
                    "metrics_source_identity_missing",
                    f"source envelopes missing for {key[0]}/{key[1]}",
                )
            effect_rows = effects.get(str(job["delivery_id"]), [])
            effect = _effect_observation(job, effect_rows)
            contract = _strict_json_object(
                job["contract_json"],
                f"delivery[{job['delivery_id']}].contract_json",
            )
            contract_oracle = contract.get("quality_oracle")
            if contract_oracle is not None:
                if (
                    not isinstance(contract_oracle, Mapping)
                    or effect["oracle"] is None
                    or _canonical_json_sha256(dict(contract_oracle))
                    != effect["oracle_sha256"]
                ):
                    raise MetricsValidationError(
                        "metrics_oracle_conflict",
                        f"delivery {job['delivery_id']} contract/oracle mismatch",
                    )
            codes = set(effect["codes"])
            for field in ("outcome", "error_code", "diagnostic_code", "terminal_state"):
                code = normalize_status(contract.get(field))
                if code:
                    codes.add(code)
            if codes & _UNSUPPORTED_CODES:
                attribution_outcome = "unsupported"
            elif codes & _EVENT_NOT_FOUND_CODES:
                attribution_outcome = "event_not_found"
            else:
                attribution_outcome = ""
            denominator_kind = (
                "business"
                if attribution_outcome
                or (
                    normalize_status(job.get("outcome")) == "success"
                    and effect["terminal_class"]
                    not in {"technical_failure", "consumer_delivery_failure"}
                )
                else "system"
            )

            ledger_rows = adjudications.get(key, [])
            if len(ledger_rows) > 1:
                raise MetricsValidationError(
                    "metrics_owner_adjudication_conflict",
                    f"multiple adjudications for {key[0]}/{key[1]}",
                )
            ledger = ledger_rows[0] if ledger_rows else None
            owner_decision = ""
            if ledger is not None:
                effect_by_key = {str(row["effect_key"]): row for row in effect_rows}
                expected_state = {
                    "recognize": "recognized",
                    "retract": "invalidated",
                }.get(str(ledger["action"]))
                if (
                    ledger["schema_version"] != ADJUDICATION_SCHEMA_VERSION
                    or ledger["conclusion_state"] != expected_state
                    or ledger["business_key"] != job["business_key"]
                    or int(ledger["generation"]) != int(job["generation"])
                    or ledger["work_item_id"] != job["work_item_id"]
                    or ledger["original_delivery_id"] != job["delivery_id"]
                    or ledger["original_effect_key"] not in effect_by_key
                    or ledger["correction_effect_key"] not in effect_by_key
                    or effect_by_key[ledger["original_effect_key"]]["required"] != 1
                    or effect_by_key[ledger["correction_effect_key"]]["required"] != 1
                    or not str(ledger["activation_epoch_id"] or "").strip()
                    or not str(ledger["actor_id"] or "").strip()
                    or repair_status.get(str(ledger["adjudication_id"]))
                    not in {"pending", "succeeded"}
                ):
                    raise MetricsValidationError(
                        "metrics_owner_adjudication_invalid",
                        f"adjudication binding invalid for {key[0]}/{key[1]}",
                    )
                owner_decision = "allow" if ledger["action"] == "recognize" else "block"
                if not attribution_outcome:
                    attribution_outcome = (
                        "owner_accepted"
                        if owner_decision == "allow"
                        else "owner_rejected"
                    )
            if denominator_kind == "business" and not attribution_outcome:
                # W13 is a post-publication review surface for medium-tier
                # candidate conclusions only.  High-tier supported results
                # and low-tier honest non-attribution are judged by their
                # bound W1 oracle/golden facts; requiring an owner row here
                # would make a valid high/low delivery impossible to report.
                # Keep the high result out of the owner-acceptance metric and
                # make low non-attribution an explicit excluded outcome.
                confidence_tier = str(effect["confidence_tier"])
                if confidence_tier == "medium":
                    raise MetricsValidationError(
                        "metrics_owner_adjudication_missing",
                        f"eligible medium business row lacks W13 adjudication for {key[0]}/{key[1]}",
                    )
                if confidence_tier == "high":
                    attribution_outcome = "supported_attribution"
                elif confidence_tier == "low":
                    attribution_outcome = "not_attributable"
                else:
                    raise MetricsValidationError(
                        "metrics_attribution_outcome_missing",
                        f"business row has no attribution outcome for tier {confidence_tier}: {key[0]}/{key[1]}",
                    )
            if denominator_kind == "system" and not attribution_outcome:
                attribution_outcome = "not_attributable"

            golden_row = golden[key]
            expected_class = str(golden_row["expected_terminal_class"])
            actual_gate = (
                "allow"
                if effect["oracle"] is not None
                and effect["oracle"].get("publication_allowed") is True
                else "block"
            )
            for row_index, source_row in enumerate(source_rows):
                if (
                    not source_row["matched_authority_sha256"]
                    or source_row["submission_key"] != job["submission_key"]
                    or source_row["decision"] != snapshot["execution_decision"]
                    or _SHA256_RE.fullmatch(
                        str(source_row["source_envelope_sha256"] or "")
                    )
                    is None
                    or _SHA256_RE.fullmatch(
                        str(source_row["source_authority_sha256"] or "")
                    )
                    is None
                    or _SHA256_RE.fullmatch(str(source_row["payload_sha256"] or ""))
                    is None
                    or _SHA256_RE.fullmatch(
                        str(source_row["authorization_evidence_sha256"] or "")
                    )
                    is None
                ):
                    raise MetricsValidationError(
                        "metrics_source_identity_invalid",
                        f"source identity binding invalid for {key[0]}/{key[1]}",
                    )
                entry = normalize_entry(source_row["source_kind"], index=row_index)
                source_identity = str(source_row["source_envelope_sha256"])
                if source_identity in source_envelope_ids:
                    raise MetricsValidationError(
                        "metrics_source_identity_duplicate",
                        f"source envelope reused: {source_identity}",
                    )
                source_id = str(source_row["source_id"] or "")
                if (
                    not source_id
                    or _SAFE_TEXT_RE.fullmatch(source_id) is None
                    or source_id in source_ids
                ):
                    raise MetricsValidationError(
                        "metrics_source_identity_duplicate",
                        f"source id reused: {source_id}",
                    )
                source_envelope_ids.add(source_identity)
                source_ids.add(source_id)
                metadata = _strict_json_object(
                    source_row["source_metadata_json"],
                    f"source[{source_identity}].source_metadata_json",
                )
                observations.append({
                    "schema_version": SCHEMA_VERSION,
                    "record_id": f"w12:{source_identity}",
                    "pair_id": str(job["delivery_id"]),
                    "release": release,
                    "release_id": release,
                    "pipeline_commit": pipeline,
                    "business": _business_dimension(job["project_key"]),
                    "entry": entry,
                    "confidence_tier": effect["confidence_tier"],
                    "denominator_kind": denominator_kind,
                    "e2e_status": effect["e2e_status"],
                    "delivery_status": effect["delivery_status"],
                    "readback_status": effect["readback_status"],
                    "attribution_outcome": attribution_outcome,
                    "owner_decision": owner_decision,
                    "triage_kind": effect["terminal_class"],
                    "triage_expected_kind": expected_class,
                    "triage_correct": effect["terminal_class"] == expected_class,
                    "gate_decision": actual_gate,
                    "gate_review_decision": golden_row["expected_gate_decision"],
                    "golden_evaluated": golden_row["evaluated"],
                    "false_high_confidence": golden_row["false_high_confidence"],
                    "golden_regression": golden_row["regression"],
                    "terminal_class": effect["terminal_class"],
                    "quality_oracle": effect["oracle"] or {},
                    "quality_oracle_sha256": effect["oracle_sha256"],
                    "auxiliary": {},
                    "identity_provenance": {
                        "source_envelope_sha256": source_identity,
                        "source_authority_sha256": str(
                            source_row["source_authority_sha256"]
                        ),
                        "source_id": source_id,
                        "source_kind": str(source_row["source_kind"]),
                        "payload_sha256": str(source_row["payload_sha256"]),
                        "authorization_evidence_sha256": str(
                            source_row["authorization_evidence_sha256"]
                        ),
                        "binding_action": str(source_row["binding_action"]),
                        "decision": str(source_row["decision"]),
                        "requester_id": str(metadata.get("requester_id") or ""),
                    },
                    "source_provenance": {
                        "source_envelope_sha256": source_identity,
                        "source_authority_sha256": str(
                            source_row["source_authority_sha256"]
                        ),
                        "source_id": source_id,
                        "source_kind": str(source_row["source_kind"]),
                    },
                    "entry_provenance": {
                        "entry": entry,
                        "source_kind": str(source_row["source_kind"]),
                        "source_id": source_id,
                    },
                    "delivery_provenance": {
                        "business_key": str(job["business_key"]),
                        "generation": int(job["generation"]),
                        "submission_key": str(job["submission_key"]),
                        "delivery_id": str(job["delivery_id"]),
                        "effect_keys": list(effect["effect_keys"]),
                        "adjudication_id": (
                            str(ledger["adjudication_id"]) if ledger else ""
                        ),
                    },
                    "oracle_provenance": {
                        "schema_version": (
                            str(effect["oracle"].get("schema_version"))
                            if effect["oracle"] is not None
                            else ""
                        ),
                        "sha256": str(effect["oracle_sha256"]),
                        "terminal_class": str(effect["terminal_class"]),
                    },
                    "golden_provenance": {
                        "schema_version": GOLDEN_INPUT_SCHEMA_VERSION,
                        "release_id": release,
                        "pipeline_commit": pipeline,
                        "business_key": key[0],
                        "generation": key[1],
                    },
                })
    except MetricsValidationError:
        raise
    except sqlite3.Error as exc:
        raise MetricsValidationError(
            "metrics_control_db_read_failed", str(exc)
        ) from exc
    except (
        AttributeError,
        IndexError,
        KeyError,
        TypeError,
        ValueError,
        OverflowError,
        UnicodeError,
    ) as exc:
        raise MetricsValidationError(
            "metrics_control_db_row_invalid", str(exc)
        ) from exc
    finally:
        if conn is not None:
            conn.close()

    try:
        after = source.stat()
        wal_after = wal.stat() if wal.exists() else None
    except OSError as exc:
        raise MetricsValidationError(
            "metrics_control_db_changed_during_read", str(source)
        ) from exc
    source_identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    after_identity = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    wal_identity = (
        (
            wal_before.st_dev,
            wal_before.st_ino,
            wal_before.st_size,
            wal_before.st_mtime_ns,
        )
        if wal_before is not None
        else None
    )
    wal_after_identity = (
        (wal_after.st_dev, wal_after.st_ino, wal_after.st_size, wal_after.st_mtime_ns)
        if wal_after is not None
        else None
    )
    if source_identity != after_identity or wal_identity != wal_after_identity:
        raise MetricsValidationError(
            "metrics_control_db_changed_during_read",
            "SQLite source or WAL identity changed during observation",
        )
    return observations


if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input", required=True, help="offline JSON/JSONL observations"
    )
    args = parser.parse_args()
    try:
        rows = load_records(args.input)
    except MetricsValidationError as exc:
        print(json.dumps(exc.as_dict(), ensure_ascii=False), file=sys.stderr)
        raise SystemExit(2)
    print(json.dumps(rows, ensure_ascii=False, indent=2, sort_keys=True))
