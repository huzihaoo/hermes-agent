#!/usr/bin/env python3
"""Read-only recomputation and migration table for RCA structural tiers."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
from typing import Any, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gateway.pnc_rca_delivery_contract import (  # noqa: E402
    RCA_RESULT_FIELD_KEY,
    render_public_rca_result,
)
from gateway.pnc_rca_quality_oracle import (  # noqa: E402
    CANDIDATE_HYPOTHESIS,
    HONEST_NON_ATTRIBUTION,
    SUPPORTED_ATTRIBUTION,
    evaluate_structural_tier,
    normalize_terminal_class,
)


SCHEMA_VERSION = "pnc_rca_tier_migration_v1"
RECEIPT_SCHEMA_VERSION = "pnc_rca_tier_migration_receipt_v1"
DEFAULT_DB = Path(
    "/Users/songying/.hermes/runtime/pnc_agent/feishu_issue_kafka_rca/control.sqlite3"
)
MAX_INPUT_BYTES = 16 * 1024 * 1024


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json_file(path: Path) -> tuple[dict[str, Any], str]:
    raw = path.read_bytes()
    if not raw or len(raw) > MAX_INPUT_BYTES:
        raise RuntimeError(f"input_size_invalid:{path}")
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"input_json_invalid:{path}") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"input_object_required:{path}")
    return value, _sha256_bytes(raw)


def _atomic_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary_path = Path(temporary)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if not isinstance(value, str) or not value:
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _load_scope(path: Path, expected_count: int) -> tuple[list[dict[str, Any]], str]:
    payload, digest = _read_json_file(path)
    tickets = payload.get("tickets")
    if not isinstance(tickets, list):
        raise RuntimeError("scope_tickets_missing")
    rows: dict[str, dict[str, Any]] = {}
    for value in tickets:
        if not isinstance(value, Mapping):
            raise RuntimeError("scope_ticket_invalid")
        work_item_id = str(value.get("work_item_id") or "").strip()
        if not work_item_id.isdigit() or work_item_id in rows:
            raise RuntimeError(f"scope_work_item_invalid:{work_item_id}")
        rows[work_item_id] = dict(value)
    if len(rows) != expected_count:
        raise RuntimeError(f"scope_count_invalid:{len(rows)}:{expected_count}")
    if not any(
        normalize_terminal_class(item.get("quality_classification"))
        == SUPPORTED_ATTRIBUTION
        for item in rows.values()
    ):
        raise RuntimeError("scope_has_no_previous_supported_claim")
    return [rows[key] for key in sorted(rows, key=int)], digest


def _latest_rows(
    db_path: Path, work_item_ids: Sequence[str]
) -> tuple[dict[str, dict[str, Any]], str, int]:
    uri = f"file:{db_path}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=20)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA query_only = ON")
        query_only = int(connection.execute("PRAGMA query_only").fetchone()[0])
        quick_check = str(connection.execute("PRAGMA quick_check").fetchone()[0])
        placeholders = ",".join("?" for _ in work_item_ids)
        rows = connection.execute(
            f"""
            WITH latest AS (
                SELECT business_triggers.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY work_item_id
                           ORDER BY generation DESC, created_at DESC
                       ) AS row_number
                  FROM business_triggers
                 WHERE work_item_id IN ({placeholders})
            )
            SELECT latest.work_item_id, latest.generation,
                   latest.submission_key, latest.source_topic,
                   latest.source_partition, latest.source_offset,
                   jobs.status AS job_status, jobs.outcome AS job_outcome,
                   jobs.terminal_state, jobs.terminal_error_code,
                   jobs.report_url, jobs.contract_json,
                   effects.status AS effect_status,
                   effects.payload_json AS effect_payload_json
              FROM latest
         LEFT JOIN rca_delivery_jobs AS jobs
                ON jobs.submission_key = latest.submission_key
         LEFT JOIN rca_delivery_effects AS effects
                ON effects.delivery_id = jobs.delivery_id
               AND effects.effect_kind = 'feishu_issue_comment'
             WHERE latest.row_number = 1
            """,
            tuple(work_item_ids),
        ).fetchall()
    finally:
        connection.close()
    if query_only != 1:
        raise RuntimeError("control_db_query_only_not_enforced")
    if quick_check != "ok":
        raise RuntimeError(f"control_db_quick_check_failed:{quick_check}")
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        work_item_id = str(row["work_item_id"] or "")
        if work_item_id in result:
            raise RuntimeError(f"latest_delivery_not_unique:{work_item_id}")
        result[work_item_id] = dict(row)
    return result, quick_check, query_only


def _result_field(payload: Mapping[str, Any]) -> str:
    for item in payload.get("field_updates") or []:
        if (
            isinstance(item, Mapping)
            and str(item.get("field_key") or "") == RCA_RESULT_FIELD_KEY
        ):
            return str(item.get("field_value") or "").strip()
    return ""


def _consumer_status(row: Mapping[str, Any]) -> str:
    if str(row.get("job_outcome") or "") != "success":
        return ""
    if str(row.get("job_status") or "") != "delivered":
        return "delivery_failed"
    if str(row.get("effect_status") or "") != "succeeded":
        return "readback_failed"
    return ""


def _migration_actions(
    previous_class: str,
    recomputed_class: str,
    previous_approval_ready: bool,
    human_decision: str,
    structural_violations: Sequence[str],
) -> list[str]:
    actions: list[str] = []
    if previous_class and previous_class != recomputed_class:
        actions.append(f"relabel:{previous_class}:{recomputed_class}")
    if previous_approval_ready and not human_decision:
        actions.append("clear_approval_ready")
    if any(
        value.startswith("banned_public_phrase:") for value in structural_violations
    ):
        actions.append("remove_banned_public_phrase")
    if "actual_evaluator_inventory_invalid" in structural_violations:
        actions.append("repair_actual_evaluator_inventory")
    return actions or ["retain_structural_class"]


def _render_markdown(payload: Mapping[str, Any]) -> str:
    summary = payload["summary"]
    lines = [
        "# RCA 71-ticket structural tier migration",
        "",
        f"- Observed: `{payload['observed_at']}`",
        f"- Rows: `{summary['total']}`",
        f"- Previous evidence_attribution: `{summary['previous_evidence_attribution']}`",
        f"- Recomputed supported_attribution: `{summary['recomputed_supported_attribution']}`",
        f"- Empty human decisions: `{summary['empty_human_decision']}`",
        f"- Recomputed approval-ready: `{summary['recomputed_approval_ready']}`",
        "",
        "| Ticket | Generation | Previous | Recomputed | Evaluator hits | Evidence refs | Causal closed | Legacy conflict | Migration |",
        "|---|---:|---|---|---:|---:|---|---|---|",
    ]
    for row in payload["tickets"]:
        facts = row["structural_oracle"]["facts"]
        actions = ", ".join(row["migration_actions"])
        lines.append(
            f"| {row['work_item_id']} | {row['generation']} | "
            f"`{row['previous_quality_classification'] or '-'}` | "
            f"`{row['recomputed_terminal_class']}` | "
            f"{facts['supported_evaluator_count']} | {facts['evidence_ref_count']} | "
            f"{str(facts['causal_chain_closed']).lower()} | "
            f"{str(row['legacy_classification_conflict']).lower()} | {actions} |"
        )
    lines.append("")
    return "\n".join(lines)


def _negative_contract(*, banned: bool = False) -> dict[str, Any]:
    conclusion = (
        "自动RCA未归因：请核对问题数据地址。"
        if banned
        else "自动RCA未归因：现有证据不足。"
    )
    return {
        "quality_classification": "supported_attribution",
        "consumer_capability": {
            "actual_evaluators": [],
            "evidence": {},
        },
        "public_result": {
            "summary": {"short_conclusion": conclusion},
            "responsibility": {},
            "causal_chain": {"narrative": []},
            "evidence_summary": {"refs": []},
            "user_action": {},
        },
    }


def _run_negative(scenario: str, receipt_path: Path | None) -> int:
    contract = _negative_contract(banned=scenario == "banned_phrase")
    result = evaluate_structural_tier(
        contract,
        claimed_terminal_class=SUPPORTED_ATTRIBUTION,
        publication_text=(
            "归因结论：自动RCA未归因。\n责任模块：暂无法判断。\n"
            "因果关系：现有证据不足。\n关键证据：现有证据不足。"
        ),
    )
    expected_violation = (
        "banned_public_phrase:请核对问题数据地址"
        if scenario == "banned_phrase"
        else "supported_attribution_evaluator_count_zero"
    )
    blocked = (
        result.classification_conflict
        and not result.publication_allowed
        and expected_violation in result.violations
    )
    payload = {
        "schema_version": "pnc_rca_tier_oracle_negative_injection_v1",
        "scenario": scenario,
        "blocked": blocked,
        "expected_violation": expected_violation,
        "exit_code": 2 if blocked else 0,
        "oracle": result.as_dict(),
    }
    encoded = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if receipt_path is not None:
        _atomic_text(receipt_path, encoded)
    print(_canonical_json(payload), file=sys.stderr)
    return payload["exit_code"]


def recompute(
    *,
    scope_ledger: Path,
    db_path: Path,
    output_json: Path,
    output_markdown: Path,
    receipt_path: Path,
    expected_count: int,
    negative_receipts: Sequence[Path],
) -> dict[str, Any]:
    scope, scope_sha = _load_scope(scope_ledger, expected_count)
    if not db_path.is_file():
        raise RuntimeError(f"control_db_missing:{db_path}")
    db_sha_before = _sha256_file(db_path)
    latest, quick_check, query_only = _latest_rows(
        db_path, [item["work_item_id"] for item in scope]
    )
    if len(latest) != expected_count:
        raise RuntimeError(f"latest_row_count_invalid:{len(latest)}:{expected_count}")

    rows: list[dict[str, Any]] = []
    for previous in scope:
        work_item_id = previous["work_item_id"]
        current = latest.get(work_item_id)
        if not current:
            raise RuntimeError(f"latest_row_missing:{work_item_id}")
        contract = _json_object(current.get("contract_json"))
        effect_payload = _json_object(current.get("effect_payload_json"))
        current_result = _result_field(effect_payload)
        current_comment = str(effect_payload.get("comment_content") or "").strip()
        if not current_result or not current_comment:
            raise RuntimeError(f"current_publication_empty:{work_item_id}")
        publication = f"{current_result}\n{current_comment}"
        execution_outcome = str(current.get("job_outcome") or "")
        terminal_error = str(current.get("terminal_error_code") or "")
        consumer_status = _consumer_status(current)
        structural = evaluate_structural_tier(
            contract,
            execution_outcome=execution_outcome,
            terminal_error_code=terminal_error,
            consumer_delivery_status=consumer_status,
        )
        observed = evaluate_structural_tier(
            contract,
            publication_text=publication,
            execution_outcome=execution_outcome,
            terminal_error_code=terminal_error,
            consumer_delivery_status=consumer_status,
        )
        previous_quality = str(previous.get("quality_classification") or "")
        previous_class = normalize_terminal_class(previous_quality)
        human_decision = str(previous.get("human_decision") or "").strip()
        previous_ready = previous.get("approval_ready") is True
        legacy = evaluate_structural_tier(
            contract,
            claimed_terminal_class=previous_class or None,
            human_decision=human_decision,
            approval_ready=previous_ready,
            publication_text=publication,
            execution_outcome=execution_outcome,
            terminal_error_code=terminal_error,
            consumer_delivery_status=consumer_status,
        )
        projected_text = render_public_rca_result(
            contract, terminal_class=structural.terminal_class
        )
        projected = evaluate_structural_tier(
            contract,
            claimed_terminal_class=structural.terminal_class,
            human_decision=human_decision,
            approval_ready=False,
            publication_text=projected_text,
            execution_outcome=execution_outcome,
            terminal_error_code=terminal_error,
            consumer_delivery_status=consumer_status,
        )
        rows.append({
            "work_item_id": work_item_id,
            "generation": current.get("generation"),
            "submission_key": str(current.get("submission_key") or ""),
            "previous_quality_classification": previous_quality,
            "previous_terminal_class": previous_class,
            "recomputed_terminal_class": structural.terminal_class,
            "recomputed_confidence_tier": structural.confidence_tier,
            "structural_oracle": structural.as_dict(),
            "observed_publication_sha256": _sha256_bytes(publication.encode("utf-8")),
            "observed_policy_violations": list(observed.violations),
            "legacy_classification_conflict": legacy.classification_conflict,
            "legacy_violations": list(legacy.violations),
            "projected_publication_allowed": projected.publication_allowed,
            "projected_violations": list(projected.violations),
            "previous_approval_ready": previous_ready,
            "human_decision": human_decision,
            "recomputed_approval_ready": bool(
                human_decision and not projected.classification_conflict
            ),
            "migration_actions": _migration_actions(
                previous_class,
                structural.terminal_class,
                previous_ready,
                human_decision,
                structural.violations,
            ),
        })

    if not rows or len(rows) != expected_count:
        raise RuntimeError("migration_output_empty_or_incomplete")
    classes = Counter(row["recomputed_terminal_class"] for row in rows)
    previous_classes = Counter(row["previous_quality_classification"] for row in rows)
    projection_blockers = Counter(
        violation for row in rows for violation in row["projected_violations"]
    )
    summary = {
        "total": len(rows),
        "previous_quality_classifications": dict(sorted(previous_classes.items())),
        "previous_evidence_attribution": previous_classes.get(
            "evidence_attribution", 0
        ),
        "recomputed_terminal_classes": dict(sorted(classes.items())),
        "recomputed_supported_attribution": classes.get(SUPPORTED_ATTRIBUTION, 0),
        "recomputed_candidate_hypothesis": classes.get(CANDIDATE_HYPOTHESIS, 0),
        "recomputed_honest_non_attribution": classes.get(HONEST_NON_ATTRIBUTION, 0),
        "legacy_classification_conflicts": sum(
            bool(row["legacy_classification_conflict"]) for row in rows
        ),
        "observed_policy_conflicts": sum(
            bool(row["observed_policy_violations"]) for row in rows
        ),
        "projected_publishable": sum(
            bool(row["projected_publication_allowed"]) for row in rows
        ),
        "projected_blockers": dict(sorted(projection_blockers.items())),
        "empty_human_decision": sum(not row["human_decision"] for row in rows),
        "recomputed_approval_ready": sum(
            row["recomputed_approval_ready"] for row in rows
        ),
    }
    output = {
        "schema_version": SCHEMA_VERSION,
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "read_only": True,
        "source": {
            "scope_ledger": str(scope_ledger),
            "scope_ledger_sha256": scope_sha,
            "control_db": str(db_path),
            "control_db_sha256": db_sha_before,
            "control_db_quick_check": quick_check,
            "control_db_query_only": bool(query_only),
        },
        "summary": summary,
        "tickets": rows,
    }
    encoded = json.dumps(output, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    markdown = _render_markdown(output)
    _atomic_text(output_json, encoded)
    _atomic_text(output_markdown, markdown)

    db_sha_after = _sha256_file(db_path)
    if db_sha_before != db_sha_after:
        raise RuntimeError("control_db_changed_during_read_only_recompute")
    expected_negatives = {
        "supported_without_evaluator": "supported_attribution_evaluator_count_zero",
        "banned_phrase": "banned_public_phrase:请核对问题数据地址",
    }
    verified_negatives: list[dict[str, Any]] = []
    seen_negative_scenarios: set[str] = set()
    for negative_path in negative_receipts:
        negative, negative_sha = _read_json_file(negative_path)
        scenario = str(negative.get("scenario") or "")
        expected_violation = expected_negatives.get(scenario)
        oracle = negative.get("oracle")
        oracle_violations = (
            oracle.get("violations") if isinstance(oracle, Mapping) else None
        )
        if (
            negative.get("schema_version")
            != "pnc_rca_tier_oracle_negative_injection_v1"
            or negative.get("blocked") is not True
            or int(negative.get("exit_code") or 0) == 0
            or expected_violation is None
            or negative.get("expected_violation") != expected_violation
            or not isinstance(oracle_violations, list)
            or expected_violation not in oracle_violations
            or scenario in seen_negative_scenarios
        ):
            raise RuntimeError(f"negative_injection_not_blocked:{negative_path}")
        seen_negative_scenarios.add(scenario)
        verified_negatives.append({
            "path": str(negative_path),
            "sha256": negative_sha,
            "scenario": scenario,
            "exit_code": int(negative["exit_code"]),
        })
    if seen_negative_scenarios != set(expected_negatives):
        raise RuntimeError("negative_injection_receipts_incomplete")
    receipt = {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "read_only": True,
        "nonempty_validation": {
            "expected_count": expected_count,
            "scope_count": len(scope),
            "latest_row_count": len(latest),
            "migration_row_count": len(rows),
            "all_current_publications_nonempty": True,
            "control_db_query_only": bool(query_only),
            "control_db_quick_check": quick_check,
            "control_db_unchanged": db_sha_before == db_sha_after,
        },
        "source": output["source"],
        "summary": summary,
        "artifacts": {
            "migration_json": str(output_json),
            "migration_json_sha256": _sha256_bytes(encoded.encode("utf-8")),
            "migration_markdown": str(output_markdown),
            "migration_markdown_sha256": _sha256_bytes(markdown.encode("utf-8")),
        },
        "negative_injections": verified_negatives,
        "production_mutations": [],
    }
    receipt_encoded = (
        json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    )
    _atomic_text(receipt_path, receipt_encoded)
    return receipt


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--inject-negative",
        choices=("supported_without_evaluator", "banned_phrase"),
    )
    parser.add_argument("--failure-receipt", type=Path)
    parser.add_argument("--scope-ledger", type=Path)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--output-json", type=Path)
    parser.add_argument("--output-markdown", type=Path)
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--expected-count", type=int, default=71)
    parser.add_argument("--negative-receipt", type=Path, action="append", default=[])
    args = parser.parse_args(argv)

    if args.inject_negative:
        return _run_negative(args.inject_negative, args.failure_receipt)
    required = {
        "scope_ledger": args.scope_ledger,
        "output_json": args.output_json,
        "output_markdown": args.output_markdown,
        "receipt": args.receipt,
    }
    missing = sorted(key for key, value in required.items() if value is None)
    if missing:
        parser.error("required arguments missing: " + ",".join(missing))
    if args.expected_count < 1:
        parser.error("--expected-count must be positive")
    try:
        receipt = recompute(
            scope_ledger=args.scope_ledger,
            db_path=args.db,
            output_json=args.output_json,
            output_markdown=args.output_markdown,
            receipt_path=args.receipt,
            expected_count=args.expected_count,
            negative_receipts=args.negative_receipt,
        )
    except (OSError, sqlite3.Error, RuntimeError, ValueError) as exc:
        print(_canonical_json({"ok": False, "error": str(exc)}), file=sys.stderr)
        return 2
    print(_canonical_json({"ok": True, "summary": receipt["summary"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
