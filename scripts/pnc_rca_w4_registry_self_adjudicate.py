#!/usr/bin/env python3
"""Build the delegated W4 blocker registry from immutable live evidence.

The tool applies design decision D38 without inventing locator-level counts:

* counts are exact blocker-code aggregates over distinct submissions;
* a zero code count becomes an observation only when the evidence scan is valid;
* a non-zero code remains a hard gate unless a separate 100% false-outcome
  evidence source exists (none is accepted implicitly by this tool);
* candidate-only sites and incomplete evidence fail closed;
* the three Publication shapes remain hard regardless of historical count.

The live SQLite database is opened with ``mode=ro&immutable=1``.  No table,
manifest, service, queue, or external system is mutated.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

try:
    from scripts import pnc_rca_w4_hard_gate_registry_audit as registry_audit
except ImportError:  # pragma: no cover - direct script execution
    import pnc_rca_w4_hard_gate_registry_audit as registry_audit


LIVE_COUNTS_SCHEMA_VERSION = "pnc_rca_w4_live_code_aggregate_counts_v1"
LEDGER_SCHEMA_VERSION = "pnc_rca_w4_self_adjudication_ledger_v1"
SUMMARY_SCHEMA_VERSION = "pnc_rca_w4_self_adjudication_summary_v1"

EXPECTED_BASE_COMMIT = "20ea44bdfaa862b2a366c33bc90552f1fcb13557"
EXPECTED_BASE_TREE = "5d009bc14e197694bc5f03af16c8c5e9ff60af37"
EXPECTED_PIPELINE_COMMIT = "1158a49140bd4459d5fbff4ca91cdea9875cd8b1"
EXPECTED_PIPELINE_TREE = "0cc6f8db18b9af8e60657cb662726113c4231fd7"
EXPECTED_HOST_COMMIT = "3acbdf9036370ba983557e0071f224ba93b9d118"
EXPECTED_HOST_TREE = "7536ed9eaccfa3f6c96e8c64b78781cce61d8bc8"
EXPECTED_ROW_COUNT = 154
LOOKBACK_DAYS = 30

AUTHORITY_PHRASE = registry_audit.DELEGATED_OWNER_PHRASE
AUTHORITY_MARKERS = (
    "委托规则自裁",
    "近 30 天 0 触发",
    "100% 假阴假阳",
    "重复拦截合并前移",
    "非空 + 可达 + 非 `.viz.mcap`",
)

_JSON_SURFACES = (
    (
        "watch.last_status_json",
        "SELECT submission_key, created_at, last_status_json FROM rca_execution_watch",
    ),
    (
        "outbox.result_json",
        "SELECT submission_key, created_at, result_json FROM rca_outbox "
        "WHERE result_json IS NOT NULL AND result_json != ''",
    ),
    (
        "job.manifest_json",
        "SELECT submission_key, created_at, manifest_json FROM rca_delivery_jobs",
    ),
    (
        "job.contract_json",
        "SELECT submission_key, created_at, contract_json FROM rca_delivery_jobs",
    ),
    (
        "job.artifacts_json",
        "SELECT submission_key, created_at, artifacts_json FROM rca_delivery_jobs",
    ),
    (
        "effect.payload_json",
        "SELECT j.submission_key, e.created_at, e.payload_json "
        "FROM rca_delivery_effects e "
        "JOIN rca_delivery_jobs j ON j.delivery_id = e.delivery_id",
    ),
    (
        "effect.remote_receipt_json",
        "SELECT j.submission_key, e.created_at, e.remote_receipt_json "
        "FROM rca_delivery_effects e "
        "JOIN rca_delivery_jobs j ON j.delivery_id = e.delivery_id "
        "WHERE e.remote_receipt_json IS NOT NULL "
        "AND e.remote_receipt_json != ''",
    ),
)

_SCALAR_SURFACES = (
    (
        "watch.last_error_code",
        "SELECT submission_key, created_at, last_error_code "
        "FROM rca_execution_watch WHERE last_error_code != ''",
    ),
    (
        "outbox.last_error_code",
        "SELECT submission_key, created_at, last_error_code "
        "FROM rca_outbox WHERE last_error_code != ''",
    ),
    (
        "job.terminal_error_code",
        "SELECT submission_key, created_at, terminal_error_code "
        "FROM rca_delivery_jobs WHERE terminal_error_code != ''",
    ),
    (
        "effect.last_error_code",
        "SELECT j.submission_key, e.created_at, e.last_error_code "
        "FROM rca_delivery_effects e "
        "JOIN rca_delivery_jobs j ON j.delivery_id = e.delivery_id "
        "WHERE e.last_error_code != ''",
    ),
    (
        "attempt.error_code",
        "SELECT j.submission_key, a.started_at, a.error_code "
        "FROM rca_delivery_attempts a "
        "JOIN rca_delivery_effects e ON e.effect_key = a.effect_key "
        "JOIN rca_delivery_jobs j ON j.delivery_id = e.delivery_id "
        "WHERE a.error_code != ''",
    ),
)

_GATE_ORDER = (
    "identity.issue_reference_contract",
    "identity.w3_execution_snapshot",
    "execution.remote_read_completeness",
    "execution.translate_workdir_permission",
    "execution.viz_mcap_build",
    "publication.report_present",
    "publication.report_reachable_exact_readback",
    "publication.report_not_viz_mcap",
    "execution.live_evidence_unresolved",
    "execution.live_nonzero_unadjudicated",
)

_NONZERO_GATE_SPECS: Mapping[str, Mapping[str, str]] = {
    "issue_field_missing_remote_data_reference": {
        "gate_id": "identity.issue_reference_contract",
        "category": "identity",
        "repo_face": "pipeline",
        "enforcement_path": "api/g1q3_rca/scripts/rca_request_contract.py",
        "enforcement_symbol": "validate_issue_context_fields",
        "test_path": "api/g1q3_rca/tests/test_rca_request_contract.py",
        "test_symbol": "validate_issue_context_fields",
    },
    "issue_field_invalid_frame_reference": {
        "gate_id": "identity.issue_reference_contract",
        "category": "identity",
        "repo_face": "pipeline",
        "enforcement_path": "api/g1q3_rca/scripts/rca_request_contract.py",
        "enforcement_symbol": "validate_issue_context_fields",
        "test_path": "api/g1q3_rca/tests/test_rca_request_contract.py",
        "test_symbol": "validate_issue_context_fields",
    },
    "remote_read_completeness_not_proven": {
        "gate_id": "execution.remote_read_completeness",
        "category": "execution",
        "repo_face": "pipeline",
        "enforcement_path": "api/g1q3_rca/scripts/run_rca_auto_pipeline.py",
        "enforcement_symbol": "run_s2_remote_read",
        "test_path": "api/g1q3_rca/tests/test_run_rca_remote_pipeline.py",
        "test_symbol": "remote_read_completeness_not_proven",
    },
    "translate_workdir_permission": {
        "gate_id": "execution.translate_workdir_permission",
        "category": "execution",
        "repo_face": "pipeline",
        "enforcement_path": "api/g1q3_rca/scripts/run_rca_auto_pipeline.py",
        "enforcement_symbol": "translate_workdir_permission",
        "test_path": "api/g1q3_rca/tests/test_cl_resilience_translate_workdir.py",
        "test_symbol": "translate_workdir_permission",
    },
    "viz_mcap_build_failed": {
        "gate_id": "execution.viz_mcap_build",
        "category": "execution",
        "repo_face": "pipeline",
        "enforcement_path": "api/g1q3_rca/scripts/run_rca_auto_pipeline.py",
        "enforcement_symbol": "viz_mcap_build_failed",
        "test_path": "api/g1q3_rca/tests/test_terminal_diagnostic_report.py",
        "test_symbol": "viz_mcap_build_failed",
    },
}

_CANDIDATE_ONLY_GATE_SPEC: Mapping[str, str] = {
    "gate_id": "identity.w3_execution_snapshot",
    "category": "identity",
    "repo_face": "pipeline",
    "enforcement_path": "api/g1q3_rca/scripts/run_rca_execution_request.py",
    "enforcement_symbol": "validate_request_source_refs",
    "test_path": "api/g1q3_rca/tests/test_w3_execution_snapshot.py",
    "test_symbol": "w3_execution_snapshot",
}

_PUBLICATION_GATE_SPECS: Mapping[str, Mapping[str, str]] = {
    "html_payload_missing": {
        "gate_id": "publication.report_present",
        "category": "publication",
        "shape": "empty",
        "repo_face": "host",
        "enforcement_path": "scripts/pnc_rca_delivery_dispatcher.py",
        "enforcement_symbol": "_validate_effect",
        "test_path": "tests/scripts/test_pnc_rca_delivery_dispatcher.py",
        "test_symbol": "test_publication_report_url_counterexamples_fail_closed_before_http",
    },
    "html_payload_mismatch": {
        "gate_id": "publication.report_reachable_exact_readback",
        "category": "publication",
        "shape": "reachability",
        "repo_face": "host",
        "enforcement_path": "scripts/pnc_rca_delivery_dispatcher.py",
        "enforcement_symbol": "default_report_verifier",
        "test_path": "tests/scripts/test_pnc_rca_delivery_dispatcher.py",
        "test_symbol": "test_primary_report_network_failure_blocks_external_comment",
    },
    "non_canonical_html": {
        "gate_id": "publication.report_not_viz_mcap",
        "category": "publication",
        "shape": "non-.viz.mcap",
        "repo_face": "host",
        "enforcement_path": "scripts/pnc_rca_delivery_dispatcher.py",
        "enforcement_symbol": "_validate_effect",
        "test_path": "tests/scripts/test_pnc_rca_delivery_dispatcher.py",
        "test_symbol": "test_publication_report_url_counterexamples_fail_closed_before_http",
    },
}

_UNRESOLVED_GATE_SPEC: Mapping[str, str] = {
    "gate_id": "execution.live_evidence_unresolved",
    "category": "execution",
    "repo_face": "registry_tool",
    "enforcement_path": "scripts/pnc_rca_w4_registry_self_adjudicate.py",
    "enforcement_symbol": "data_integrity_ok",
    "test_path": "tests/scripts/test_pnc_rca_w4_registry_self_adjudicate.py",
    "test_symbol": "test_malformed_live_payload_fails_zero_rows_closed",
}

_UNADJUDICATED_NONZERO_GATE_SPEC: Mapping[str, str] = {
    "gate_id": "execution.live_nonzero_unadjudicated",
    "category": "execution",
    "repo_face": "registry_tool",
    "enforcement_path": "scripts/pnc_rca_w4_registry_self_adjudicate.py",
    "enforcement_symbol": "live_nonzero_unadjudicated",
    "test_path": "tests/scripts/test_pnc_rca_w4_registry_self_adjudicate.py",
    "test_symbol": "test_unknown_nonzero_code_fails_closed",
}


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path) -> Mapping[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, Mapping):
        raise ValueError(f"JSON object required: {path}")
    return value


def _canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _write_json(path: Path, value: Any) -> str:
    raw = _canonical_bytes(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    return _sha256_bytes(raw)


def _parse_timestamp(value: str) -> datetime:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        raise ValueError(f"timezone missing from timestamp: {value}")
    return parsed.astimezone(timezone.utc)


def _iso_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def verify_git_face(
    repo: Path,
    *,
    expected_commit: str,
    expected_tree: str,
    label: str,
) -> dict[str, Any]:
    commit = _git(repo, "rev-parse", "HEAD")
    tree = _git(repo, "rev-parse", "HEAD^{tree}")
    status = _git(repo, "status", "--porcelain")
    if commit != expected_commit:
        raise ValueError(f"{label} commit mismatch: {commit}")
    if tree != expected_tree:
        raise ValueError(f"{label} tree mismatch: {tree}")
    if status:
        raise ValueError(f"{label} worktree is dirty")
    return {
        "repository": str(repo),
        "commit": commit,
        "tree": tree,
        "worktree_clean": True,
    }


def verify_authority(path: Path, expected_sha256: str) -> dict[str, Any]:
    raw = path.read_bytes()
    observed_sha256 = _sha256_bytes(raw)
    if observed_sha256 != expected_sha256:
        raise ValueError(
            "authority SHA-256 mismatch: "
            f"expected {expected_sha256}, observed {observed_sha256}"
        )
    text = raw.decode("utf-8")
    missing = [marker for marker in AUTHORITY_MARKERS if marker not in text]
    if missing:
        raise ValueError(f"authority markers missing: {missing}")
    return {
        "path": str(path),
        "sha256": observed_sha256,
        "decision": "D38",
        "section": "6 decision 38 + 9.3",
        "owner_phrase": AUTHORITY_PHRASE,
        "markers_verified": list(AUTHORITY_MARKERS),
    }


def _walk_strings(value: Any, path: str = "$") -> Iterable[tuple[str, str]]:
    if isinstance(value, Mapping):
        for key in sorted(value, key=str):
            yield from _walk_strings(value[key], f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from _walk_strings(item, f"{path}[{index}]")
    elif isinstance(value, str):
        yield path, value


def _site_identity(site: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(
        str(site.get(field) or "")
        for field in ("file", "scope", "call", "keyword", "value")
    )


def _sqlite_uri(path: Path) -> str:
    return f"file:{path.resolve().as_posix()}?mode=ro&immutable=1"


def extract_live_counts(
    database: Path,
    codes: Sequence[str],
    *,
    observed_at: datetime,
    lookback_days: int = LOOKBACK_DAYS,
) -> dict[str, Any]:
    """Read exact code aggregates from durable output surfaces."""

    code_set = set(codes)
    window_start = observed_at - timedelta(days=lookback_days)
    submissions: dict[str, set[str]] = {code: set() for code in code_set}
    leaf_occurrences: dict[str, int] = {code: 0 for code in code_set}
    surface_submissions: dict[str, dict[str, set[str]]] = {
        code: defaultdict(set) for code in code_set
    }
    first_seen: dict[str, datetime] = {}
    last_seen: dict[str, datetime] = {}
    path_samples: dict[str, set[str]] = {code: set() for code in code_set}
    surface_record_counts: dict[str, int] = {}
    malformed_json: list[dict[str, str]] = []
    timestamp_errors: list[dict[str, str]] = []

    def in_window(raw_timestamp: str, surface: str, key: str) -> datetime | None:
        try:
            timestamp = _parse_timestamp(raw_timestamp)
        except (TypeError, ValueError) as exc:
            timestamp_errors.append({
                "surface": surface,
                "record_key": key,
                "error": str(exc),
            })
            return None
        if timestamp < window_start or timestamp > observed_at:
            return None
        return timestamp

    def record_match(
        code: str,
        *,
        submission_key: str,
        timestamp: datetime,
        surface: str,
        value_path: str,
    ) -> None:
        submissions[code].add(submission_key)
        leaf_occurrences[code] += 1
        surface_submissions[code][surface].add(submission_key)
        first_seen[code] = min(timestamp, first_seen.get(code, timestamp))
        last_seen[code] = max(timestamp, last_seen.get(code, timestamp))
        if len(path_samples[code]) < 12:
            path_samples[code].add(f"{surface}:{value_path}")

    connection = sqlite3.connect(_sqlite_uri(database), uri=True)
    try:
        connection.execute("PRAGMA query_only = ON")
        table_names = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        required_tables = {
            "rca_execution_watch",
            "rca_outbox",
            "rca_delivery_jobs",
            "rca_delivery_effects",
            "rca_delivery_attempts",
        }
        missing_tables = sorted(required_tables - table_names)

        execution_rows = connection.execute(
            "SELECT submission_key, created_at FROM rca_execution_watch"
        ).fetchall()
        execution_times: list[datetime] = []
        window_submission_keys: set[str] = set()
        for submission_key, raw_timestamp in execution_rows:
            timestamp = in_window(
                str(raw_timestamp), "watch.created_at", str(submission_key)
            )
            if timestamp is not None:
                execution_times.append(timestamp)
                window_submission_keys.add(str(submission_key))

        for surface, statement in _JSON_SURFACES:
            if any(
                table in missing_tables
                for table in required_tables
                if table in statement
            ):
                continue
            count = 0
            for submission_key, raw_timestamp, body in connection.execute(statement):
                key = str(submission_key)
                timestamp = in_window(str(raw_timestamp), surface, key)
                if timestamp is None or body in (None, ""):
                    continue
                count += 1
                try:
                    payload = json.loads(str(body))
                except json.JSONDecodeError as exc:
                    malformed_json.append({
                        "surface": surface,
                        "record_key": key,
                        "error": f"{exc.msg}@{exc.pos}",
                    })
                    continue
                for value_path, value in _walk_strings(payload):
                    if value in code_set:
                        record_match(
                            value,
                            submission_key=key,
                            timestamp=timestamp,
                            surface=surface,
                            value_path=value_path,
                        )
            surface_record_counts[surface] = count

        for surface, statement in _SCALAR_SURFACES:
            if any(
                table in missing_tables
                for table in required_tables
                if table in statement
            ):
                continue
            count = 0
            for submission_key, raw_timestamp, value in connection.execute(statement):
                key = str(submission_key)
                timestamp = in_window(str(raw_timestamp), surface, key)
                if timestamp is None:
                    continue
                count += 1
                text = str(value or "")
                if text in code_set:
                    record_match(
                        text,
                        submission_key=key,
                        timestamp=timestamp,
                        surface=surface,
                        value_path="$",
                    )
            surface_record_counts[surface] = count

        control_schema = connection.execute(
            "SELECT value FROM control_meta WHERE key = 'schema_version'"
        ).fetchone()
        delivery_schema = connection.execute(
            "SELECT value FROM rca_delivery_meta WHERE key = 'schema_version'"
        ).fetchone()
    finally:
        connection.close()

    code_counts = []
    for code in sorted(code_set):
        code_counts.append({
            "code": code,
            "live_count": len(submissions[code]),
            "leaf_occurrences": leaf_occurrences[code],
            "first_seen_at": _iso_z(first_seen[code]) if code in first_seen else None,
            "last_seen_at": _iso_z(last_seen[code]) if code in last_seen else None,
            "surface_submission_counts": {
                surface: len(keys)
                for surface, keys in sorted(surface_submissions[code].items())
            },
            "path_samples": sorted(path_samples[code]),
        })

    data_integrity_ok = bool(execution_times) and not (
        missing_tables or malformed_json or timestamp_errors
    )
    database_stat = database.stat()
    return {
        "schema_version": LIVE_COUNTS_SCHEMA_VERSION,
        "read_only": True,
        "database": {
            "path": str(database),
            "sha256": _sha256_file(database),
            "bytes": database_stat.st_size,
            "open_mode": "sqlite mode=ro&immutable=1 + PRAGMA query_only",
            "control_schema": str(control_schema[0]) if control_schema else None,
            "delivery_schema": str(delivery_schema[0]) if delivery_schema else None,
        },
        "window": {
            "lookback_days": lookback_days,
            "start": _iso_z(window_start),
            "end": _iso_z(observed_at),
            "execution_count": len(window_submission_keys),
            "first_execution_at": (
                _iso_z(min(execution_times)) if execution_times else None
            ),
            "last_execution_at": (
                _iso_z(max(execution_times)) if execution_times else None
            ),
        },
        "method": {
            "count_scope": "code_aggregate",
            "unit": "distinct submission_key",
            "matching": "exact string leaf or exact scalar error code",
            "deduplication": "same code and submission counts once across all surfaces",
            "input_payloads_excluded": True,
            "false_outcome_evidence": "absent_not_inferred",
        },
        "surfaces": surface_record_counts,
        "integrity": {
            "ok": data_integrity_ok,
            "missing_tables": missing_tables,
            "malformed_json": malformed_json,
            "timestamp_errors": timestamp_errors,
        },
        "counts": code_counts,
        "summary": {
            "candidate_code_count": len(code_counts),
            "nonzero_code_count": sum(item["live_count"] > 0 for item in code_counts),
            "zero_code_count": sum(item["live_count"] == 0 for item in code_counts),
        },
        "production_actions": [],
    }


def _gate_ref(spec: Mapping[str, str], face: Mapping[str, Any]) -> tuple[str, str]:
    commit = str(face.get("commit") or "working-tree")
    enforcement = (
        f"{spec['repo_face']}@{commit}:{spec['enforcement_path']}"
        f"#{spec['enforcement_symbol']}"
    )
    negative = (
        f"{spec['repo_face']}@{commit}:{spec['test_path']}::{spec['test_symbol']}"
    )
    return enforcement, negative


def _verify_spec_refs(
    spec: Mapping[str, str],
    *,
    faces: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    face_name = spec["repo_face"]
    face = faces[face_name]
    repository = Path(str(face["repository"]))
    enforcement_path = repository / spec["enforcement_path"]
    test_path = repository / spec["test_path"]
    enforcement_text = enforcement_path.read_text(encoding="utf-8")
    test_text = test_path.read_text(encoding="utf-8")
    enforcement_symbol_found = spec["enforcement_symbol"] in enforcement_text
    test_symbol_found = spec["test_symbol"] in test_text
    if not enforcement_symbol_found or not test_symbol_found:
        raise ValueError(f"gate ref verification failed: {spec['gate_id']}")
    return {
        "gate_id": spec["gate_id"],
        "repo_face": face_name,
        "commit": face.get("commit"),
        "tree": face.get("tree"),
        "enforcement_path": spec["enforcement_path"],
        "enforcement_sha256": _sha256_file(enforcement_path),
        "enforcement_symbol": spec["enforcement_symbol"],
        "enforcement_symbol_found": True,
        "negative_test_path": spec["test_path"],
        "negative_test_sha256": _sha256_file(test_path),
        "negative_test_symbol": spec["test_symbol"],
        "negative_test_symbol_found": True,
    }


def adjudicate(
    inventory_payload: Mapping[str, Any],
    live_evidence: Mapping[str, Any],
    *,
    inventory_file_sha256: str,
    live_evidence_ref: str,
    live_evidence_sha256: str,
    authority: Mapping[str, Any],
    faces: Mapping[str, Mapping[str, Any]],
    observed_at: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    binding = registry_audit._inventory_binding(inventory_payload)
    expected = {
        "base_commit": EXPECTED_BASE_COMMIT,
        "base_tree": EXPECTED_BASE_TREE,
        "candidate_commit": EXPECTED_PIPELINE_COMMIT,
        "candidate_tree": EXPECTED_PIPELINE_TREE,
        "candidate_site_count": EXPECTED_ROW_COUNT,
    }
    for field, expected_value in expected.items():
        if binding[field] != expected_value:
            raise ValueError(
                f"inventory {field} mismatch: {binding[field]} != {expected_value}"
            )

    base_sites = inventory_payload["base"]["literal_emission_sites"]
    candidate_sites = inventory_payload["candidate"]["literal_emission_sites"]
    if not isinstance(base_sites, list) or not isinstance(candidate_sites, list):
        raise ValueError("inventory sites missing")
    base_identities = {
        _site_identity(site) for site in base_sites if isinstance(site, Mapping)
    }

    raw_counts = live_evidence.get("counts")
    if not isinstance(raw_counts, list):
        raise ValueError("live evidence counts missing")
    counts = {
        str(item.get("code") or ""): int(item.get("live_count", -1))
        for item in raw_counts
        if isinstance(item, Mapping)
    }
    data_integrity_ok = bool(
        isinstance(live_evidence.get("integrity"), Mapping)
        and live_evidence["integrity"].get("ok")
    )

    authority_ref = (
        f"{authority['path']}#{authority['section']};sha256={authority['sha256']}"
    )
    gate_specs: dict[str, Mapping[str, str]] = {}
    gate_sources: dict[str, list[str]] = defaultdict(list)
    ledger_rows: list[dict[str, Any]] = []
    registry_rows: list[dict[str, Any]] = []
    candidate_only_count = 0
    mandatory_publication_count = 0
    unresolved_count = 0

    for ordinal, raw_site in enumerate(candidate_sites):
        if not isinstance(raw_site, Mapping):
            raise ValueError(f"candidate site {ordinal} is invalid")
        site = dict(raw_site)
        locator = str(site.get("locator") or "")
        code = str(site.get("value") or "")
        if not locator or code not in counts:
            raise ValueError(f"candidate site evidence missing: {ordinal}")
        live_count = counts[code]
        baseline_identity_exposed = _site_identity(site) in base_identities
        hard_gate_ids: list[str] = []
        decision_basis: str
        evidence_status = "resolved"

        publication_spec = _PUBLICATION_GATE_SPECS.get(code)
        if publication_spec is not None:
            mandatory_publication_count += 1
            spec = publication_spec
            hard_gate_ids.append(spec["gate_id"])
            gate_specs[spec["gate_id"]] = spec
            decision_basis = f"D38_publication_{spec['shape']}_mandatory_hard"
            final_decision = "hard_gate"
        elif not baseline_identity_exposed:
            candidate_only_count += 1
            unresolved_count += 1
            evidence_status = "unresolved_candidate_only_site"
            spec = _CANDIDATE_ONLY_GATE_SPEC
            hard_gate_ids.append(spec["gate_id"])
            gate_specs[spec["gate_id"]] = spec
            decision_basis = "D38_candidate_unexposed_fail_closed"
            final_decision = "hard_gate"
        elif not data_integrity_ok:
            unresolved_count += 1
            evidence_status = "unresolved_live_evidence_integrity"
            spec = _UNRESOLVED_GATE_SPEC
            hard_gate_ids.append(spec["gate_id"])
            gate_specs[spec["gate_id"]] = spec
            decision_basis = "D38_unresolved_data_fail_closed"
            final_decision = "hard_gate"
        elif live_count > 0:
            spec = _NONZERO_GATE_SPECS.get(code)
            if spec is None:
                unresolved_count += 1
                evidence_status = "unresolved_nonzero_code_without_explicit_mapping"
                spec = _UNADJUDICATED_NONZERO_GATE_SPEC
                decision_basis = "D38_nonzero_unadjudicated_fail_closed"
            else:
                decision_basis = "D38_nonzero_without_100pct_false_evidence_hard"
            hard_gate_ids.append(spec["gate_id"])
            gate_specs[spec["gate_id"]] = spec
            final_decision = "hard_gate"
        else:
            decision_basis = "D38_30d_zero_trigger_observation"
            final_decision = "observation"

        for gate_id in hard_gate_ids:
            gate_sources[gate_id].append(locator)

        trigger_condition = (
            f"candidate blocker code {code!r} is emitted at the bound locator; "
            "live_count is the exact 30-day distinct-submission code aggregate"
        )
        registry_row = {
            "locator": locator,
            "final_decision": final_decision,
            "decision_ref": f"{authority_ref};rule={decision_basis}",
            "trigger_condition": trigger_condition,
            "count_scope": "code_aggregate",
            "live_count": live_count,
            "live_evidence_ref": f"{live_evidence_ref}#code={code}",
            "live_evidence_sha256": live_evidence_sha256,
            "hard_gate_ids": hard_gate_ids,
        }
        registry_rows.append(registry_row)
        ledger_rows.append({
            "ordinal": ordinal,
            "locator": locator,
            "file": site.get("file"),
            "line": site.get("line"),
            "scope": site.get("scope"),
            "call": site.get("call"),
            "keyword": site.get("keyword"),
            "code": code,
            "baseline_identity_exposed": baseline_identity_exposed,
            "live_count": live_count,
            "count_scope": "code_aggregate",
            "false_outcome_evidence": "absent_not_inferred",
            "evidence_status": evidence_status,
            "final_decision": final_decision,
            "decision_basis": decision_basis,
            "hard_gate_ids": hard_gate_ids,
        })

    face_for_spec = {
        "pipeline": faces["pipeline"],
        "host": faces["host"],
        "registry_tool": faces["registry_tool"],
    }
    hard_gates: list[dict[str, Any]] = []
    ref_checks: list[dict[str, Any]] = []
    gate_rank = {gate_id: index for index, gate_id in enumerate(_GATE_ORDER)}
    for gate_id in sorted(
        gate_specs, key=lambda item: (gate_rank.get(item, 999), item)
    ):
        spec = gate_specs[gate_id]
        face = face_for_spec[spec["repo_face"]]
        enforcement_ref, negative_test_ref = _gate_ref(spec, face)
        hard_gates.append({
            "gate_id": gate_id,
            "category": spec["category"],
            "source_locators": gate_sources[gate_id],
            "enforcement_ref": enforcement_ref,
            "negative_test_ref": negative_test_ref,
        })
        ref_checks.append(_verify_spec_refs(spec, faces=face_for_spec))

    registry = {
        "schema_version": registry_audit.REGISTRY_SCHEMA_VERSION,
        "inventory_binding": {
            "base_commit": binding["base_commit"],
            "base_tree": binding["base_tree"],
            "base_site_count": binding["base_site_count"],
            "candidate_commit": binding["candidate_commit"],
            "candidate_tree": binding["candidate_tree"],
            "candidate_site_count": binding["candidate_site_count"],
            "inventory_sha256": binding["inventory_sha256"],
        },
        "owner_approval": {
            "phrase": AUTHORITY_PHRASE,
            "approval_ref": authority_ref,
            "status": "approved",
        },
        "rows": registry_rows,
        "hard_gates": hard_gates,
    }

    hard_row_count = sum(row["final_decision"] == "hard_gate" for row in ledger_rows)
    observation_row_count = len(ledger_rows) - hard_row_count
    summary = {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "observed_at": observed_at,
        "status": "self_adjudicated_offline",
        "authority": dict(authority),
        "inventory": {
            "path_sha256": inventory_file_sha256,
            "binding_sha256": binding["inventory_sha256"],
            "base_commit": binding["base_commit"],
            "base_tree": binding["base_tree"],
            "candidate_commit": binding["candidate_commit"],
            "candidate_tree": binding["candidate_tree"],
            "row_count": binding["candidate_site_count"],
            "unique_code_count": len(counts),
        },
        "live_evidence": {
            "path": live_evidence_ref,
            "sha256": live_evidence_sha256,
            "integrity_ok": data_integrity_ok,
            "window": live_evidence.get("window"),
            "nonzero_codes": [
                item["code"]
                for item in raw_counts
                if isinstance(item, Mapping) and int(item.get("live_count", 0)) > 0
            ],
        },
        "decision_counts": {
            "total_rows": len(ledger_rows),
            "observation_rows": observation_row_count,
            "hard_gate_rows": hard_row_count,
            "candidate_only_fail_closed_rows": candidate_only_count,
            "mandatory_publication_rows": mandatory_publication_count,
            "unresolved_fail_closed_rows": unresolved_count,
            "hard_gate_count": len(hard_gates),
            "hard_gate_limit": registry_audit.MAX_HARD_GATE_COUNT,
            "within_hard_gate_limit": len(hard_gates)
            <= registry_audit.MAX_HARD_GATE_COUNT,
        },
        "hard_gate_ids": [gate["gate_id"] for gate in hard_gates],
        "reference_verification": {
            "ok": all(
                check["enforcement_symbol_found"]
                and check["negative_test_symbol_found"]
                for check in ref_checks
            ),
            "checks": ref_checks,
        },
        "policy": {
            "zero_trigger": "observation when exact 30-day code aggregate is zero",
            "nonzero_trigger": (
                "hard unless separately proven 100% false-positive/false-negative"
            ),
            "duplicate": "same failure code rows share the earliest explicit gate",
            "publication": "empty/reachability/non-.viz.mcap remain hard",
            "unresolved": "fail closed",
            "definition_id_inferred": False,
            "locator_count_guessed": False,
        },
        "faces": {key: dict(value) for key, value in faces.items()},
        "production_actions": [],
        "ga_claimed": False,
    }
    if not summary["decision_counts"]["within_hard_gate_limit"]:
        raise ValueError("hard gate limit not achieved")
    if unresolved_count and not all(
        row["final_decision"] == "hard_gate"
        for row in ledger_rows
        if str(row["evidence_status"]).startswith("unresolved")
    ):
        raise ValueError("unresolved row did not fail closed")

    ledger = {
        "schema_version": LEDGER_SCHEMA_VERSION,
        "observed_at": observed_at,
        "read_only": True,
        "authority": dict(authority),
        "inventory_binding": registry["inventory_binding"],
        "inventory_file_sha256": inventory_file_sha256,
        "live_evidence_ref": live_evidence_ref,
        "live_evidence_sha256": live_evidence_sha256,
        "rows": ledger_rows,
        "summary": summary["decision_counts"],
        "production_actions": [],
    }
    return ledger, summary, registry


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--live-db", type=Path, required=True)
    parser.add_argument("--pipeline-repo", type=Path, required=True)
    parser.add_argument("--host-repo", type=Path, required=True)
    parser.add_argument("--authority", type=Path, required=True)
    parser.add_argument("--authority-sha256", required=True)
    parser.add_argument("--observed-at", required=True)
    parser.add_argument("--live-output", type=Path, required=True)
    parser.add_argument("--ledger-output", type=Path, required=True)
    parser.add_argument("--summary-output", type=Path, required=True)
    parser.add_argument("--registry-output", type=Path, required=True)
    args = parser.parse_args(argv)

    observed_at = _parse_timestamp(args.observed_at)
    observed_at_text = _iso_z(observed_at)
    inventory = _load_json(args.inventory)
    authority = verify_authority(args.authority, args.authority_sha256)
    pipeline_face = verify_git_face(
        args.pipeline_repo,
        expected_commit=EXPECTED_PIPELINE_COMMIT,
        expected_tree=EXPECTED_PIPELINE_TREE,
        label="pipeline",
    )
    host_face = verify_git_face(
        args.host_repo,
        expected_commit=EXPECTED_HOST_COMMIT,
        expected_tree=EXPECTED_HOST_TREE,
        label="host",
    )
    registry_tool_face = {
        "repository": str(Path(__file__).resolve().parents[1]),
        "commit": _git(Path(__file__).resolve().parents[1], "rev-parse", "HEAD"),
        "tree": _git(Path(__file__).resolve().parents[1], "rev-parse", "HEAD^{tree}"),
        "worktree_clean": not bool(
            _git(Path(__file__).resolve().parents[1], "status", "--porcelain")
        ),
    }

    candidate_sites = inventory.get("candidate", {}).get("literal_emission_sites", [])
    if not isinstance(candidate_sites, list):
        raise ValueError("candidate inventory sites missing")
    codes = sorted({
        str(site.get("value") or "")
        for site in candidate_sites
        if isinstance(site, Mapping) and str(site.get("value") or "")
    })
    live_evidence = extract_live_counts(
        args.live_db,
        codes,
        observed_at=observed_at,
    )
    live_sha256 = _write_json(args.live_output, live_evidence)
    faces = {
        "pipeline": pipeline_face,
        "host": host_face,
        "registry_tool": registry_tool_face,
    }
    ledger, summary, registry = adjudicate(
        inventory,
        live_evidence,
        inventory_file_sha256=_sha256_file(args.inventory),
        live_evidence_ref=str(args.live_output),
        live_evidence_sha256=live_sha256,
        authority=authority,
        faces=faces,
        observed_at=observed_at_text,
    )
    output_hashes = {
        "live_counts": live_sha256,
        "ledger": _write_json(args.ledger_output, ledger),
        "summary": _write_json(args.summary_output, summary),
        "registry": _write_json(args.registry_output, registry),
    }
    print(
        json.dumps(
            {
                "ok": True,
                "read_only": True,
                "outputs": {
                    "live_counts": str(args.live_output),
                    "ledger": str(args.ledger_output),
                    "summary": str(args.summary_output),
                    "registry": str(args.registry_output),
                },
                "sha256": output_hashes,
                "decision_counts": summary["decision_counts"],
                "hard_gate_ids": summary["hard_gate_ids"],
                "production_actions": [],
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
