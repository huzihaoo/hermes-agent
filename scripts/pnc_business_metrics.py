#!/usr/bin/env python3
"""Normalize offline PNC metric observations.

The resident Hermes ``pnc_business_metrics.py`` script is intentionally a
read-only runtime helper and is not part of this repository.  This module is
the repository-side, offline-compatible normalization layer used by the W12
candidate.  It accepts the field names emitted by W3/delivery contracts while
requiring an explicit denominator scope so business and system observations
cannot silently share a denominator.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import json
from pathlib import Path
import re
from typing import Any


SCHEMA_VERSION = "pnc_business_metrics_w12_v1"
DENOMINATOR_KINDS = frozenset({"business", "system"})
CONFIDENCE_TIERS = ("high", "medium", "low", "none")
ENTRYPOINTS = ("kafka", "feishu")
MAX_INPUT_BYTES = 96 * 1024 * 1024

_SAFE_TEXT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/+@-]{0,255}$")
_MISSING = object()


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
