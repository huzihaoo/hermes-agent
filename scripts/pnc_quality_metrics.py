#!/usr/bin/env python3
"""Produce the W12 four-metric daily report from offline or DB observations.

The report is deliberately a pure aggregation surface.  It consumes bounded
JSON/JSONL observations, groups them by ``release x business x entry x
confidence_tier``, and keeps business/system denominators in separate buckets.
The SQLite mode accepts only a checkpointed immutable read-only snapshot.  No
runtime network endpoint, launchd service, or delivery writer is opened.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
import sys
from typing import Any, Callable

# ``python scripts/pnc_quality_metrics.py`` puts ``scripts/`` (rather than the
# repository root) on sys.path.  Add the root so the same import works from the
# CLI and from pytest.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.pnc_business_metrics import (
    CONFIDENCE_TIERS,
    DENOMINATOR_KINDS,
    ENTRYPOINTS,
    MetricsValidationError,
    SUPPORTED_CONTROL_STORE_SCHEMA_VERSIONS,
    SUPPORTED_DELIVERY_STORE_SCHEMA_VERSIONS,
    load_records,
    load_sqlite_observations,
    normalize_record,
)


SCHEMA_VERSION = "pnc_quality_metrics_w12_v1"
METRIC_NAMES = (
    "dual_entry_e2e_success",
    "technical_delivery_readback",
    "useful_attribution",
    "false_high_confidence_no_regression",
)
_SUCCESS_E2E = frozenset({"success", "succeeded", "completed", "delivered"})
_SUCCESS_DELIVERY = frozenset({"success", "succeeded", "delivered", "settled", "ack"})
_SUCCESS_READBACK = frozenset({
    "verified",
    "success",
    "succeeded",
    "readback_verified",
    "ack",
})
_ATTRIBUTION_EXCLUSIONS = frozenset({"unsupported", "event_not_found"})

# These are the three ``todo`` rows in the resident pnc_quality_metrics.py
# inventory.  They now have explicit fields and denominator semantics.  The
# labels are retained verbatim so operators can compare old/new reports.
SIGNAL_INVENTORY = (
    {
        "name": "triage_accuracy_kind_distribution",
        "label": "triage 准确率 / kind 分布",
        "status": "have",
        "clean_fields": (
            "signals.triage.kind",
            "signals.triage.expected_kind",
        ),
        "denominator_kind": "business",
        "numerator": "kind == expected_kind",
    },
    {
        "name": "rca_adoption_rate",
        "label": "RCA 采纳率",
        "status": "have",
        "clean_fields": (
            "attribution.outcome",
            "attribution.owner_decision",
        ),
        "denominator_kind": "business",
        "numerator": "owner_decision == accepted",
    },
    {
        "name": "gate_consistency_rate",
        "label": "门禁一致率",
        "status": "have",
        "clean_fields": (
            "signals.gate.decision",
            "signals.gate.review_decision",
        ),
        "denominator_kind": "system",
        "numerator": "decision == review_decision",
    },
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _rate(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(100.0 * numerator / denominator, 1)


def _bucket() -> dict[str, dict[str, int | float | None]]:
    return {
        kind: {"numerator": 0, "denominator": 0, "rate_pct": None}
        for kind in sorted(DENOMINATOR_KINDS)
    }


def _metric(*, denominator_kind: str = "separate") -> dict[str, Any]:
    return {
        "denominator_kind": denominator_kind,
        "by_denominator": _bucket(),
    }


def _metrics() -> dict[str, dict[str, Any]]:
    metrics = {name: _metric() for name in METRIC_NAMES}
    metrics["useful_attribution"]["excluded_outcomes"] = {
        kind: {name: 0 for name in sorted(_ATTRIBUTION_EXCLUSIONS)}
        for kind in sorted(DENOMINATOR_KINDS)
    }
    metrics["false_high_confidence_no_regression"]["failure_counts"] = {
        kind: {"false_high_confidence": 0, "regression": 0}
        for kind in sorted(DENOMINATOR_KINDS)
    }
    return metrics


def _finish_metric(metric: dict[str, Any]) -> None:
    for values in metric["by_denominator"].values():
        values["rate_pct"] = _rate(values["numerator"], values["denominator"])


def _add_observation(
    metric: dict[str, Any],
    *,
    kind: str,
    eligible: bool,
    success: bool,
) -> None:
    if kind not in DENOMINATOR_KINDS or not eligible:
        return
    values = metric["by_denominator"][kind]
    values["denominator"] += 1
    if success:
        values["numerator"] += 1


def _issue(
    issues: list[dict[str, Any]], code: str, detail: str, row: Mapping[str, Any]
) -> None:
    issues.append({"code": code, "detail": detail, "record_id": row.get("record_id")})


def _normalization(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    if not isinstance(records, Sequence) or isinstance(
        records, (str, bytes, bytearray)
    ):
        raise MetricsValidationError(
            "metrics_records_invalid", "records must be an array"
        )
    normalized: list[dict[str, Any]] = []
    for index, row in enumerate(records):
        if (
            isinstance(row, Mapping)
            and row.get("schema_version") == "pnc_business_metrics_w12_v1"
        ):
            normalized.append(dict(row))
        else:
            # Keep the index in normalization errors for negative-injection
            # diagnostics, while accepting both raw and already-normalized
            # fixtures.
            normalized.append(normalize_record(row, index=index))
    return normalized


def _pair_results(
    rows: Sequence[Mapping[str, Any]], issues: list[dict[str, Any]]
) -> dict[tuple[str, str, str, str], dict[str, Any]]:
    grouped: dict[tuple[str, str, str, str], list[Mapping[str, Any]]] = defaultdict(
        list
    )
    for row in rows:
        base = (
            str(row["release"]),
            str(row["business"]),
            str(row["confidence_tier"]),
            str(row["pair_id"]),
        )
        grouped[base].append(row)

    result: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for key, pair_rows in grouped.items():
        by_entry: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for row in pair_rows:
            by_entry[str(row["entry"])].append(row)
        scopes = {str(row["denominator_kind"]) for row in pair_rows}
        if len(scopes) != 1:
            _issue(
                issues,
                "metrics_pair_denominator_mixed",
                f"pair {key[-1]} mixes scopes {sorted(scopes)}",
                pair_rows[0],
            )
            continue
        scope = next(iter(scopes))
        complete = all(entry in by_entry for entry in ENTRYPOINTS)
        result[key] = {"scope": scope, "complete": complete}
    return result


def _signal_ratio(
    rows: Sequence[Mapping[str, Any]],
    *,
    eligible: Callable[[Mapping[str, Any]], bool],
    success: Callable[[Mapping[str, Any]], bool],
    skip: Callable[[Mapping[str, Any]], bool] | None = None,
    expected_scope: str | None = None,
    issues: list[dict[str, Any]],
    missing_code: str,
) -> dict[str, Any]:
    metric = _metric(denominator_kind=expected_scope or "separate")
    for row in rows:
        if expected_scope and row["denominator_kind"] != expected_scope:
            continue
        if skip is not None and skip(row):
            continue
        if not eligible(row):
            _issue(issues, missing_code, "clean signal fields are missing", row)
            continue
        _add_observation(
            metric,
            kind=str(row["denominator_kind"]),
            eligible=True,
            success=success(row),
        )
    _finish_metric(metric)
    return metric


def _group_skeleton(
    rows: Sequence[Mapping[str, Any]],
) -> dict[tuple[str, str, str, str], dict[str, Any]]:
    groups: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (
            str(row["release"]),
            str(row["business"]),
            str(row["entry"]),
            str(row["confidence_tier"]),
        )
        group = groups.setdefault(
            key,
            {
                "dimensions": {
                    "release": key[0],
                    "business": key[1],
                    "entry": key[2],
                    "confidence_tier": key[3],
                },
                "denominators": {kind: 0 for kind in sorted(DENOMINATOR_KINDS)},
                "metrics": _metrics(),
                "signals": {},
                "auxiliary": {
                    "attribution_exclusions": {
                        name: 0 for name in sorted(_ATTRIBUTION_EXCLUSIONS)
                    },
                    "incomplete_e2e_pairs": 0,
                    "coverage_count": 0,
                    "report_count": 0,
                    "field_write_count": 0,
                },
                "_rows": [],
            },
        )
        group["_rows"].append(row)
        group["denominators"][str(row["denominator_kind"])] += 1
        auxiliary = row.get("auxiliary") or {}
        for field in ("coverage_count", "report_count", "field_write_count"):
            value = auxiliary.get(field)
            if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                group["auxiliary"][field] += value
    return groups


def _compute_group_metrics(
    groups: dict[tuple[str, str, str, str], dict[str, Any]],
    pair_results: Mapping[tuple[str, str, str, str], Mapping[str, Any]],
    issues: list[dict[str, Any]],
) -> None:
    for key, group in groups.items():
        rows = group["_rows"]
        base = key[:2] + (key[3],)
        pair_ids = sorted({str(row["pair_id"]) for row in rows})
        e2e_metric = group["metrics"]["dual_entry_e2e_success"]
        for pair_id in pair_ids:
            pair = pair_results.get((base[0], base[1], base[2], pair_id))
            if pair is None:
                continue
            if not pair["complete"]:
                group["auxiliary"]["incomplete_e2e_pairs"] += 1

        technical = group["metrics"]["technical_delivery_readback"]
        attribution = group["metrics"]["useful_attribution"]
        golden = group["metrics"]["false_high_confidence_no_regression"]
        for row in rows:
            kind = str(row["denominator_kind"])
            e2e_status = str(row.get("e2e_status") or "")
            if not e2e_status:
                _issue(
                    issues,
                    "metrics_e2e_status_missing",
                    "E2E status missing",
                    row,
                )
            _add_observation(
                e2e_metric,
                kind=kind,
                eligible=True,
                success=e2e_status in _SUCCESS_E2E,
            )
            delivery_status = str(row.get("delivery_status") or "")
            readback_status = str(row.get("readback_status") or "")
            if not delivery_status or not readback_status:
                _issue(
                    issues,
                    "metrics_technical_status_missing",
                    "delivery/readback status missing",
                    row,
                )
            _add_observation(
                technical,
                kind=kind,
                eligible=True,
                success=(
                    delivery_status in _SUCCESS_DELIVERY
                    and readback_status in _SUCCESS_READBACK
                ),
            )

            outcome = str(row.get("attribution_outcome") or "")
            if outcome in _ATTRIBUTION_EXCLUSIONS:
                group["auxiliary"]["attribution_exclusions"][outcome] += 1
                attribution["excluded_outcomes"][kind][outcome] += 1
            elif kind == "business":
                if not outcome:
                    _issue(
                        issues,
                        "metrics_attribution_outcome_missing",
                        "attribution outcome missing",
                        row,
                    )
                else:
                    owner_decision = str(row.get("owner_decision") or "")
                    if not owner_decision:
                        _issue(
                            issues,
                            "metrics_owner_decision_missing",
                            "owner decision missing for attribution-eligible row",
                            row,
                        )
                    _add_observation(
                        attribution,
                        kind=kind,
                        eligible=True,
                        success=owner_decision == "allow",
                    )
            elif outcome:
                # System observations are intentionally visible as excluded,
                # never folded into the business attribution denominator.
                group["auxiliary"].setdefault("attribution_scope_excluded", 0)
                group["auxiliary"]["attribution_scope_excluded"] += 1

            evaluated = row.get("golden_evaluated")
            if evaluated is not True:
                if evaluated is None:
                    _issue(
                        issues,
                        "metrics_golden_evaluation_missing",
                        "golden evaluation status missing",
                        row,
                    )
                else:
                    group["auxiliary"].setdefault("golden_unassessed", 0)
                    group["auxiliary"]["golden_unassessed"] += 1
                continue
            false_high = row.get("false_high_confidence")
            regression = row.get("golden_regression")
            if not isinstance(false_high, bool) or not isinstance(regression, bool):
                _issue(
                    issues,
                    "metrics_golden_result_missing",
                    "golden false-high/regression result missing",
                    row,
                )
                # Count the evaluated case as a failure, never as a pass.
                _add_observation(golden, kind=kind, eligible=True, success=False)
            else:
                if false_high:
                    golden["failure_counts"][kind]["false_high_confidence"] += 1
                if regression:
                    golden["failure_counts"][kind]["regression"] += 1
                _add_observation(
                    golden,
                    kind=kind,
                    eligible=True,
                    success=(not false_high and not regression),
                )
        for metric in (e2e_metric, technical, attribution, golden):
            _finish_metric(metric)

        signal_rows = [
            row for row in rows if str(row["denominator_kind"]) == "business"
        ]
        group["signals"]["triage_accuracy_kind_distribution"] = _signal_ratio(
            signal_rows,
            eligible=lambda row: bool(
                row.get("triage_kind") and row.get("triage_expected_kind")
            ),
            success=lambda row: (
                bool(row.get("triage_correct"))
                if row.get("triage_correct") is not None
                else row.get("triage_kind") == row.get("triage_expected_kind")
            ),
            skip=lambda row: row.get("attribution_outcome") in _ATTRIBUTION_EXCLUSIONS,
            expected_scope="business",
            issues=issues,
            missing_code="metrics_triage_signal_missing",
        )
        group["signals"]["rca_adoption_rate"] = _signal_ratio(
            signal_rows,
            eligible=lambda row: (
                row.get("attribution_outcome") not in _ATTRIBUTION_EXCLUSIONS
                and bool(row.get("attribution_outcome"))
                and bool(row.get("owner_decision"))
            ),
            success=lambda row: row.get("owner_decision") == "allow",
            skip=lambda row: row.get("attribution_outcome") in _ATTRIBUTION_EXCLUSIONS,
            expected_scope="business",
            issues=issues,
            missing_code="metrics_rca_signal_missing",
        )
        group["signals"]["gate_consistency_rate"] = _signal_ratio(
            rows,
            eligible=lambda row: bool(
                row.get("gate_decision") and row.get("gate_review_decision")
            ),
            success=lambda row: (
                row.get("gate_decision") == row.get("gate_review_decision")
            ),
            expected_scope="system",
            issues=issues,
            missing_code="metrics_gate_signal_missing",
        )
        del group["_rows"]


def build_daily_report(
    records: Sequence[Mapping[str, Any]],
    *,
    observed_at: str | None = None,
    strict: bool = True,
) -> dict[str, Any]:
    """Aggregate records into a fail-closed W12 report.

    ``strict=True`` (the CLI default) raises on any missing clean signal or
    denominator-critical field.  ``strict=False`` is useful for an operator
    preview: the report is emitted with ``ok=false`` and an explicit errors
    list, but no invalid row is promoted to a numerator.
    """

    rows = _normalization(records)
    if not rows:
        raise MetricsValidationError(
            "metrics_no_records", "at least one observation is required"
        )
    timestamp = observed_at or _now_iso()
    try:
        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError as exc:
        raise MetricsValidationError(
            "metrics_observed_at_invalid", "observed_at must be ISO-8601"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise MetricsValidationError(
            "metrics_observed_at_timezone_missing",
            "observed_at must include a timezone",
        )

    issues: list[dict[str, Any]] = []
    observed_entries = {str(row["entry"]) for row in rows}
    missing_entries = sorted(set(ENTRYPOINTS) - observed_entries)
    if missing_entries:
        _issue(
            issues,
            "metrics_entry_coverage_incomplete",
            f"daily input is missing entries {missing_entries}",
            rows[0],
        )
    groups = _group_skeleton(rows)
    pair_results = _pair_results(rows, issues)
    _compute_group_metrics(groups, pair_results, issues)

    rendered_groups: list[dict[str, Any]] = []
    total_auxiliary = {
        "coverage_count": 0,
        "report_count": 0,
        "field_write_count": 0,
        "attribution_exclusions": {name: 0 for name in sorted(_ATTRIBUTION_EXCLUSIONS)},
        "incomplete_e2e_pairs": 0,
    }
    for key in sorted(groups):
        group = groups[key]
        rendered_groups.append(group)
        for field in (
            "coverage_count",
            "report_count",
            "field_write_count",
            "incomplete_e2e_pairs",
        ):
            total_auxiliary[field] += int(group["auxiliary"].get(field) or 0)
        for name in _ATTRIBUTION_EXCLUSIONS:
            total_auxiliary["attribution_exclusions"][name] += int(
                group["auxiliary"]["attribution_exclusions"].get(name) or 0
            )

    report = {
        "schema_version": SCHEMA_VERSION,
        "observed_at": parsed
        .astimezone(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z"),
        "ok": not issues,
        "grouping": ["release", "business", "entry", "confidence_tier"],
        "denominator_policy": {
            "kinds": sorted(DENOMINATOR_KINDS),
            "mixed_denominators_forbidden": True,
            "auxiliary_excluded": [
                "coverage_count",
                "report_count",
                "field_write_count",
            ],
        },
        "entry_coverage": {
            "required": list(ENTRYPOINTS),
            "observed": sorted(observed_entries),
            "complete": not missing_entries,
        },
        "confidence_tiers": list(CONFIDENCE_TIERS),
        "signal_inventory": [dict(item) for item in SIGNAL_INVENTORY],
        "groups": rendered_groups,
        "auxiliary": total_auxiliary,
        "errors": issues,
        "source": {
            "mode": "offline_fixture",
            "runtime_mutation_performed": False,
            "external_effects_triggered": False,
        },
    }
    if strict and issues:
        detail = json.dumps(
            {"errors": issues[:20], "error_count": len(issues)},
            ensure_ascii=False,
            sort_keys=True,
        )
        raise MetricsValidationError("metrics_report_not_clean", detail)
    return report


def render_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        f"# PNC W12 四指标日报 ({report.get('observed_at', '')})",
        "",
        f"- status: {'OK' if report.get('ok') else 'NOT_CLEAN'}",
        "- grouping: release × business × entry × confidence_tier",
        "- denominator policy: business and system are reported separately",
        "",
        "| release | business | entry | tier | metric | business n/d | system n/d |",
        "|---|---|---|---|---|---:|---:|",
    ]
    for group in report.get("groups", []):
        dims = group.get("dimensions", {})
        for metric_name in METRIC_NAMES:
            metric = (group.get("metrics") or {}).get(metric_name) or {}
            buckets = metric.get("by_denominator") or {}
            cells = []
            for kind in ("business", "system"):
                bucket = buckets.get(kind) or {}
                cells.append(
                    f"{bucket.get('numerator', 0)}/{bucket.get('denominator', 0)}"
                )
            lines.append(
                f"| {dims.get('release')} | {dims.get('business')} | {dims.get('entry')} | "
                f"{dims.get('confidence_tier')} | {metric_name} | {cells[0]} | {cells[1]} |"
            )
    auxiliary = report.get("auxiliary") or {}
    lines.extend([
        "",
        "## Auxiliary (not metric denominators)",
        "",
        f"- coverage_count: {auxiliary.get('coverage_count', 0)}",
        f"- report_count: {auxiliary.get('report_count', 0)}",
        f"- field_write_count: {auxiliary.get('field_write_count', 0)}",
        f"- attribution exclusions: {auxiliary.get('attribution_exclusions', {})}",
    ])
    if report.get("errors"):
        lines.extend(["", "## Errors", ""])
        lines.extend(
            f"- {item.get('code')}: {item.get('detail')}"
            for item in report["errors"][:20]
        )
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--input", help="offline JSON/JSONL observations")
    mode.add_argument(
        "--control-db",
        help="checkpointed control v11/v12 + delivery v9 SQLite snapshot",
    )
    parser.add_argument("--release-id")
    parser.add_argument("--pipeline-commit")
    parser.add_argument("--window-start")
    parser.add_argument("--window-end")
    parser.add_argument("--golden-input")
    parser.add_argument("--observed-at", default=None)
    parser.add_argument("--markdown", action="store_true")
    parser.add_argument(
        "--non-strict",
        action="store_true",
        help="emit a NOT_CLEAN report instead of exiting on issues",
    )
    args = parser.parse_args(argv)
    try:
        if args.control_db:
            required = {
                "release_id": args.release_id,
                "pipeline_commit": args.pipeline_commit,
                "window_start": args.window_start,
                "window_end": args.window_end,
                "golden_input": args.golden_input,
            }
            missing = sorted(name for name, value in required.items() if not value)
            if missing:
                raise MetricsValidationError(
                    "metrics_sqlite_cli_input_missing",
                    f"SQLite mode requires {missing}",
                )
            if args.observed_at and args.observed_at != args.window_end:
                raise MetricsValidationError(
                    "metrics_observed_at_window_mismatch",
                    "observed_at must equal window_end in SQLite mode",
                )
            rows = load_sqlite_observations(
                args.control_db,
                release_id=args.release_id,
                pipeline_commit=args.pipeline_commit,
                window_start=args.window_start,
                window_end=args.window_end,
                golden_input=args.golden_input,
            )
            observed_at = args.window_end
        else:
            sqlite_only = {
                "release_id": args.release_id,
                "pipeline_commit": args.pipeline_commit,
                "window_start": args.window_start,
                "window_end": args.window_end,
                "golden_input": args.golden_input,
            }
            unexpected = sorted(
                name for name, value in sqlite_only.items() if value is not None
            )
            if unexpected:
                raise MetricsValidationError(
                    "metrics_cli_mode_invalid",
                    f"--input cannot be combined with {unexpected}",
                )
            rows = load_records(args.input)
            observed_at = args.observed_at
        report = build_daily_report(
            rows, observed_at=observed_at, strict=not args.non_strict
        )
        if args.control_db:
            report["source"] = {
                "mode": "sqlite_uri_mode_ro_immutable",
                "control_db": str(Path(args.control_db).expanduser().absolute()),
                "control_schema_versions_supported": sorted(
                    SUPPORTED_CONTROL_STORE_SCHEMA_VERSIONS
                ),
                "delivery_schema_versions_supported": sorted(
                    SUPPORTED_DELIVERY_STORE_SCHEMA_VERSIONS
                ),
                "release_id": args.release_id,
                "pipeline_commit": args.pipeline_commit,
                "window_start": args.window_start,
                "window_end": args.window_end,
                "runtime_mutation_performed": False,
                "external_effects_triggered": False,
                "wal_created": False,
            }
    except MetricsValidationError as exc:
        print(
            json.dumps(exc.as_dict(), ensure_ascii=False, sort_keys=True),
            file=sys.stderr,
        )
        return 2
    if args.markdown:
        print(render_markdown(report))
    else:
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
