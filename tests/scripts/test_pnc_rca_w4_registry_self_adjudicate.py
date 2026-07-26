from __future__ import annotations

import copy
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from scripts import pnc_rca_w4_registry_self_adjudicate as adjudicator


def _site(index: int, code: str) -> dict[str, object]:
    return {
        "file": "api/g1q3_rca/scripts/run_rca_auto_pipeline.py",
        "scope": f"scope_{index}",
        "call": "<dict-literal>",
        "keyword": "kind",
        "value": code,
        "line": index + 10,
        "locator": f"blocker-site-v1-{index:064x}",
    }


def _inventory() -> dict[str, object]:
    base_sites = [
        _site(0, "remote_read_completeness_not_proven"),
        _site(1, "html_payload_missing"),
        _site(2, "html_payload_mismatch"),
        _site(3, "non_canonical_html"),
        _site(4, "zero_code"),
        _site(5, "issue_field_missing_remote_data_reference"),
        _site(6, "issue_field_invalid_frame_reference"),
    ]
    candidate_sites = [
        *copy.deepcopy(base_sites),
        _site(7, "w3_execution_snapshot_invalid"),
    ]

    def section(commit: str, tree: str, sites: list[dict[str, object]]):
        return {
            "commit": commit,
            "tree": tree,
            "literal_emission_sites": sites,
            "literal_emission_summary": {"site_count": len(sites)},
        }

    return {
        "schema_version": "pnc_rca_blocker_literal_inventory_v1",
        "base": section(
            adjudicator.EXPECTED_BASE_COMMIT,
            adjudicator.EXPECTED_BASE_TREE,
            base_sites,
        ),
        "candidate": section(
            adjudicator.EXPECTED_PIPELINE_COMMIT,
            adjudicator.EXPECTED_PIPELINE_TREE,
            candidate_sites,
        ),
        "delta": {
            "authoritative_literal_emissions": {
                "base_count": len(base_sites),
                "candidate_count": len(candidate_sites),
                "added_count": 1,
                "removed_count": 0,
                "added": [copy.deepcopy(candidate_sites[-1])],
                "removed": [],
            }
        },
    }


def _live_evidence(*, integrity_ok: bool = True, zero_count: int = 0):
    values = [
        "remote_read_completeness_not_proven",
        "html_payload_missing",
        "html_payload_mismatch",
        "non_canonical_html",
        "zero_code",
        "issue_field_missing_remote_data_reference",
        "issue_field_invalid_frame_reference",
        "w3_execution_snapshot_invalid",
    ]
    counts = []
    for value in values:
        live_count = 2 if value == "remote_read_completeness_not_proven" else 0
        if value == "zero_code":
            live_count = zero_count
        if value == "issue_field_missing_remote_data_reference":
            live_count = 1
        if value == "issue_field_invalid_frame_reference":
            live_count = 13
        counts.append({"code": value, "live_count": live_count})
    return {
        "integrity": {"ok": integrity_ok},
        "counts": counts,
        "window": {"lookback_days": 30, "execution_count": 2},
    }


def _faces(tmp_path: Path):
    face = {
        "repository": str(tmp_path),
        "commit": "a" * 40,
        "tree": "b" * 40,
        "worktree_clean": True,
    }
    return {"pipeline": face, "host": face, "registry_tool": face}


def _authority():
    return {
        "path": "/authority.md",
        "sha256": "a" * 64,
        "decision": "D38",
        "section": "6 decision 38 + 9.3",
        "owner_phrase": "委托规则自裁",
    }


def _adjudicate(monkeypatch, tmp_path: Path, live_evidence):
    monkeypatch.setattr(adjudicator, "EXPECTED_ROW_COUNT", 8)
    monkeypatch.setattr(
        adjudicator,
        "_verify_spec_refs",
        lambda spec, **_: {
            "gate_id": spec["gate_id"],
            "enforcement_symbol_found": True,
            "negative_test_symbol_found": True,
        },
    )
    return adjudicator.adjudicate(
        _inventory(),
        live_evidence,
        inventory_file_sha256="b" * 64,
        live_evidence_ref="live-counts.json",
        live_evidence_sha256="c" * 64,
        authority=_authority(),
        faces=_faces(tmp_path),
        observed_at="2026-07-26T14:30:00Z",
    )


def _create_live_db(path: Path, payload: str = '{"blocker":{"kind":"code_a"}}'):
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE control_meta(key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE rca_delivery_meta(key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE rca_execution_watch(
            submission_key TEXT, created_at TEXT, last_status_json TEXT,
            last_error_code TEXT
        );
        CREATE TABLE rca_outbox(
            submission_key TEXT, created_at TEXT, result_json TEXT,
            last_error_code TEXT
        );
        CREATE TABLE rca_delivery_jobs(
            delivery_id TEXT, submission_key TEXT, created_at TEXT,
            manifest_json TEXT, contract_json TEXT, artifacts_json TEXT,
            terminal_error_code TEXT
        );
        CREATE TABLE rca_delivery_effects(
            effect_key TEXT, delivery_id TEXT, created_at TEXT,
            payload_json TEXT, remote_receipt_json TEXT, last_error_code TEXT
        );
        CREATE TABLE rca_delivery_attempts(
            effect_key TEXT, started_at TEXT, error_code TEXT
        );
        """
    )
    connection.execute(
        "INSERT INTO control_meta VALUES('schema_version', 'control-v1')"
    )
    connection.execute(
        "INSERT INTO rca_delivery_meta VALUES('schema_version', 'delivery-v1')"
    )
    now = "2026-07-25T10:00:00+00:00"
    connection.execute(
        "INSERT INTO rca_execution_watch VALUES(?, ?, ?, '')",
        ("submission-1", now, payload),
    )
    connection.execute(
        "INSERT INTO rca_outbox VALUES(?, ?, '{}', '')",
        ("submission-1", now),
    )
    connection.execute(
        "INSERT INTO rca_delivery_jobs VALUES(?, ?, ?, '{}', ?, '{}', '')",
        ("delivery-1", "submission-1", now, payload),
    )
    connection.execute(
        "INSERT INTO rca_delivery_jobs VALUES(?, ?, ?, '{}', ?, '{}', '')",
        ("delivery-2", "submission-2", now, payload),
    )
    connection.commit()
    connection.close()


def test_live_counts_deduplicate_same_code_across_durable_surfaces(tmp_path: Path):
    database = tmp_path / "control.sqlite3"
    _create_live_db(database)

    evidence = adjudicator.extract_live_counts(
        database,
        ["code_a", "code_b"],
        observed_at=datetime(2026, 7, 26, 14, 30, tzinfo=timezone.utc),
    )

    counts = {row["code"]: row for row in evidence["counts"]}
    assert evidence["read_only"] is True
    assert evidence["integrity"]["ok"] is True
    assert counts["code_a"]["live_count"] == 2
    assert counts["code_a"]["leaf_occurrences"] == 3
    assert counts["code_b"]["live_count"] == 0


def test_d38_zero_nonzero_candidate_only_and_publication_rules(monkeypatch, tmp_path):
    ledger, summary, registry = _adjudicate(monkeypatch, tmp_path, _live_evidence())

    rows = {row["code"]: row for row in ledger["rows"]}
    assert rows["zero_code"]["final_decision"] == "observation"
    assert rows["remote_read_completeness_not_proven"]["final_decision"] == "hard_gate"
    assert rows["w3_execution_snapshot_invalid"]["evidence_status"] == (
        "unresolved_candidate_only_site"
    )
    assert rows["html_payload_missing"]["decision_basis"] == (
        "D38_publication_empty_mandatory_hard"
    )
    assert rows["issue_field_missing_remote_data_reference"]["hard_gate_ids"] == [
        "identity.issue_reference_contract"
    ]
    assert rows["issue_field_invalid_frame_reference"]["hard_gate_ids"] == [
        "identity.issue_reference_contract"
    ]
    assert summary["decision_counts"] == {
        "total_rows": 8,
        "observation_rows": 1,
        "hard_gate_rows": 7,
        "candidate_only_fail_closed_rows": 1,
        "mandatory_publication_rows": 3,
        "unresolved_fail_closed_rows": 1,
        "hard_gate_count": 6,
        "hard_gate_limit": 15,
        "within_hard_gate_limit": True,
    }
    assert registry["owner_approval"]["phrase"] == "委托规则自裁"


def test_malformed_live_payload_fails_zero_rows_closed(monkeypatch, tmp_path):
    ledger, summary, _ = _adjudicate(
        monkeypatch,
        tmp_path,
        _live_evidence(integrity_ok=False),
    )

    zero_row = next(row for row in ledger["rows"] if row["code"] == "zero_code")
    assert zero_row["final_decision"] == "hard_gate"
    assert zero_row["evidence_status"] == "unresolved_live_evidence_integrity"
    assert "execution.live_evidence_unresolved" in summary["hard_gate_ids"]


def test_unknown_nonzero_code_fails_closed(monkeypatch, tmp_path):
    ledger, summary, _ = _adjudicate(
        monkeypatch,
        tmp_path,
        _live_evidence(zero_count=1),
    )

    row = next(row for row in ledger["rows"] if row["code"] == "zero_code")
    assert row["final_decision"] == "hard_gate"
    assert row["evidence_status"] == "unresolved_nonzero_code_without_explicit_mapping"
    assert "execution.live_nonzero_unadjudicated" in summary["hard_gate_ids"]


def test_authority_requires_exact_hash_and_all_d38_markers(tmp_path: Path):
    authority = tmp_path / "authority.md"
    authority.write_text("委托规则自裁", encoding="utf-8")

    with pytest.raises(ValueError, match="markers missing"):
        adjudicator.verify_authority(
            authority,
            adjudicator._sha256_file(authority),
        )
