#!/usr/bin/env python3
"""Collect human ticket-validity inputs through read-only Meegle APIs.

The historic file name is retained for compatibility.  ``field_b23cb8`` is
not an RCA adoption or quality signal: it only records whether a human treated
the ticket itself as valid, for fail-closed intake filtering.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
import json
from pathlib import Path
import re
import sys
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from gateway.pnc_issue_context import G1Q3_ADOPTION_FIELD_KEY
from scripts.pnc_rca_delivery_dispatcher import MeegleIssueCommentAdapter


MANIFEST_SCHEMA_VERSION = "pnc_rca_adoption_read_manifest_v1"
BATCH_SCHEMA_VERSION = "pnc_rca_ticket_validity_batch_v1"
TICKET_VALIDITY_PURPOSE = "human_ticket_validity_only_not_rca_quality_metric"
MAX_MANIFEST_BYTES = 2 * 1024 * 1024
MAX_RECORDS = 1000
_TOKEN_RE = re.compile(r"[A-Za-z0-9_.:@/-]{1,256}")


class AdoptionCollectorError(ValueError):
    def __init__(self, code: str, detail: str):
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}")


def _token(value: Any, field: str) -> str:
    text = str(value or "").strip()
    if not text or _TOKEN_RE.fullmatch(text) is None:
        raise AdoptionCollectorError(
            "adoption_manifest_identity_invalid",
            f"{field} is missing or invalid",
        )
    return text


def _nonnegative_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise AdoptionCollectorError(
            "adoption_manifest_time_invalid",
            f"{field} must be a non-negative integer",
        )
    return value


def load_manifest(path: str | Path) -> dict[str, Any]:
    source = Path(path).expanduser()
    try:
        raw = source.read_bytes()
    except OSError as exc:
        raise AdoptionCollectorError(
            "adoption_manifest_unavailable", str(source)
        ) from exc
    if len(raw) > MAX_MANIFEST_BYTES:
        raise AdoptionCollectorError("adoption_manifest_too_large", str(source))
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AdoptionCollectorError(
            "adoption_manifest_invalid", "manifest must be UTF-8 JSON"
        ) from exc
    if not isinstance(payload, Mapping):
        raise AdoptionCollectorError(
            "adoption_manifest_invalid", "manifest must be an object"
        )
    return dict(payload)


def normalize_manifest(payload: Mapping[str, Any]) -> dict[str, Any]:
    if payload.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        raise AdoptionCollectorError(
            "adoption_manifest_schema_invalid", "unsupported manifest schema"
        )
    observed_at_ms = _nonnegative_int(payload.get("observed_at_ms"), "observed_at_ms")
    records = payload.get("records")
    if (
        not isinstance(records, Sequence)
        or isinstance(records, (str, bytes, bytearray))
        or not records
        or len(records) > MAX_RECORDS
    ):
        raise AdoptionCollectorError(
            "adoption_manifest_records_invalid",
            f"records must contain 1..{MAX_RECORDS} objects",
        )

    normalized = []
    identities: set[tuple[str, int]] = set()
    issue_generations: set[tuple[str, int]] = set()
    for index, raw in enumerate(records):
        if not isinstance(raw, Mapping):
            raise AdoptionCollectorError(
                "adoption_manifest_record_invalid", f"record {index} is not an object"
            )
        business_key = _token(raw.get("business_key"), "business_key")
        project_key = _token(raw.get("project_key"), "project_key")
        work_item_id = _token(raw.get("work_item_id"), "work_item_id")
        generation = _nonnegative_int(raw.get("generation"), "generation")
        conclusion_time_ms = _nonnegative_int(
            raw.get("conclusion_time_ms"), "conclusion_time_ms"
        )
        if generation < 1 or conclusion_time_ms > observed_at_ms:
            raise AdoptionCollectorError(
                "adoption_manifest_generation_invalid",
                f"record {index} has an invalid generation boundary",
            )
        next_value = raw.get("next_conclusion_time_ms")
        next_conclusion_time_ms = (
            None
            if next_value is None
            else _nonnegative_int(next_value, "next_conclusion_time_ms")
        )
        if next_conclusion_time_ms is not None and (
            next_conclusion_time_ms <= conclusion_time_ms
            or next_conclusion_time_ms > observed_at_ms
        ):
            raise AdoptionCollectorError(
                "adoption_manifest_generation_invalid",
                f"record {index} has an invalid next conclusion boundary",
            )
        identity = (business_key, generation)
        issue_identity = (work_item_id, generation)
        if identity in identities or issue_identity in issue_generations:
            raise AdoptionCollectorError(
                "adoption_manifest_identity_duplicate",
                f"record {index} repeats a conclusion generation",
            )
        identities.add(identity)
        issue_generations.add(issue_identity)
        normalized.append({
            "business_key": business_key,
            "project_key": project_key,
            "work_item_id": work_item_id,
            "generation": generation,
            "conclusion_time_ms": conclusion_time_ms,
            "next_conclusion_time_ms": next_conclusion_time_ms,
        })
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "observed_at_ms": observed_at_ms,
        "records": normalized,
    }


def collect_adoption_signals(
    manifest: Mapping[str, Any],
    *,
    adapter: MeegleIssueCommentAdapter,
) -> dict[str, Any]:
    normalized = normalize_manifest(manifest)
    observed_at_ms = int(normalized["observed_at_ms"])
    output_records = []
    error_count = 0
    for row in normalized["records"]:
        result = dict(adapter.read_generation_adoption(
            row["project_key"],
            row["work_item_id"],
            generation=row["generation"],
            conclusion_time_ms=row["conclusion_time_ms"],
            next_conclusion_time_ms=row["next_conclusion_time_ms"],
            observed_at_ms=(
                observed_at_ms
                if row["next_conclusion_time_ms"] is None
                else None
            ),
        ))
        if result.get("success") is not True:
            error_count += 1
            end_ms = (
                row["next_conclusion_time_ms"] - 1
                if row["next_conclusion_time_ms"] is not None
                else observed_at_ms
            )
            result = {
                **result,
                "source": "official_meegle_api",
                "scope": {
                    "project_key": row["project_key"],
                    "work_item_id": row["work_item_id"],
                },
                "field_key": G1Q3_ADOPTION_FIELD_KEY,
                "generation": row["generation"],
                "start_ms": row["conclusion_time_ms"],
                "end_ms": end_ms,
                "window_semantics": (
                    "half_open_conclusion_to_next_conclusion"
                    if row["next_conclusion_time_ms"] is not None
                    else "closed_conclusion_to_observed_at"
                ),
                "status": "read_error",
                "error_code": str(
                    result.get("error_code") or "g1q3_adoption_read_failed"
                ),
            }
        raw_status = str(result.get("status") or "").strip()
        validity_state = {
            "adopted": "human_valid",
            "rejected": "human_invalid",
            "read_error": "read_error",
        }.get(raw_status, "unknown")
        output_records.append({
            "business_key": row["business_key"],
            "work_item_id": row["work_item_id"],
            "generation": row["generation"],
            "ticket_validity": {
                "field_key": G1Q3_ADOPTION_FIELD_KEY,
                "state": validity_state,
                "source": str(result.get("source") or "official_meegle_api"),
                "explicit_human_operation": result.get("explicit") is True,
                "error_code": str(result.get("error_code") or ""),
            },
        })
    return {
        "schema_version": BATCH_SCHEMA_VERSION,
        "ok": error_count == 0,
        "observed_at_ms": observed_at_ms,
        "source": "official_meegle_api",
        "purpose": TICKET_VALIDITY_PURPOSE,
        "quality_metric_eligible": False,
        "read_only": True,
        "write_commands_performed": 0,
        "error_count": error_count,
        "records": output_records,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    args = parser.parse_args(argv)
    try:
        batch = collect_adoption_signals(
            load_manifest(args.manifest),
            adapter=MeegleIssueCommentAdapter(),
        )
    except AdoptionCollectorError as exc:
        print(
            json.dumps(
                {"ok": False, "code": exc.code, "detail": exc.detail},
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    print(json.dumps(batch, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if batch["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
