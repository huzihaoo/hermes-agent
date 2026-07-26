from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from scripts import pnc_rca_release_scorecard as scorecard


COMMIT = "1" * 40
TREE = "2" * 40
SHA256 = "3" * 64
OBSERVED_AT = "2026-07-25T11:20:00Z"


def _lineage() -> dict:
    return {
        "commit": COMMIT,
        "tree": TREE,
        "activated_at": OBSERVED_AT,
        "reason": "bounded test activation",
        "previous_commit": "4" * 40,
        "time_source": "activation_receipt",
        "evidence_path": "/evidence/activation.json",
    }


def _valid_scorecard() -> dict:
    counts = {tier: 0 for tier in scorecard.FOUR_TIERS}
    face = {"commit": COMMIT, "tree": TREE}
    return {
        "schema_version": scorecard.SCHEMA_VERSION,
        "observed_at": OBSERVED_AT,
        "release_status": "NOT_GA",
        "ga_claim_allowed": False,
        "live": {
            "release_id": "release-test",
            "activation": {"state": "legacy_unconfigured"},
            "fingerprints": {
                "host": dict(face),
                "pipeline": dict(face),
                "worker": dict(face),
                "mcap": {
                    **face,
                    "runtime_contract_sha256": SHA256,
                },
            },
            "profile_readiness": [
                {
                    "profile_id": "g1q3",
                    "execution_readiness": "ready",
                    "evaluator_scope": "g1q3_scope",
                },
                {
                    "profile_id": "mdrive4",
                    "execution_readiness": "input_adapter_pending",
                    "evaluator_scope": "mdrive4_scope",
                },
            ],
            "tier_counts": {
                "active_release": {"counts": dict(counts), "unclassified": 0},
                "seven_day": {"counts": dict(counts), "unclassified": 0},
            },
            "requester_identity_denominators": {
                "total_triggers": 4,
                "counts": {
                    "human": 1,
                    "automation": 1,
                    "legacy_automation": 1,
                    "unknown": 1,
                },
            },
            "canaries": {
                "natural_kafka": {"state": "not_observed_for_active_release"},
                "feishu_topic": {"state": "not_observed_for_active_release"},
            },
            "real_data": {
                "row_counts": {
                    "business_triggers": 1,
                    "rca_trigger_sources": 1,
                    "rca_delivery_jobs": 1,
                    "rca_delivery_effects": 1,
                }
            },
        },
        "reference": {
            "source_boundaries": {
                "live": "execution truth",
                "reference": "contract",
                "historical": "archived evidence",
            }
        },
        "historical": {
            "release_lineage": {
                "today": {
                    "host": [_lineage()],
                    "pipeline": [_lineage()],
                    "host_count": 1,
                    "pipeline_count": 1,
                },
                "seven_day": {
                    "host": [_lineage()],
                    "pipeline": [_lineage()],
                    "host_count": 1,
                    "pipeline_count": 1,
                },
            },
            "reported_tier_counts": {
                "scope_total": 1,
                "counts": {
                    **counts,
                    "low_confidence_honest_non_attribution": 1,
                },
            },
        },
        "read_only_attestation": {
            "production_mutation_performed": False,
            "external_effects_triggered": False,
        },
    }


def test_scorecard_validator_accepts_complete_not_ga_contract() -> None:
    scorecard.validate_scorecard(_valid_scorecard())


def test_scorecard_validator_accepts_quiet_day_with_seven_day_lineage() -> None:
    quiet_day = _valid_scorecard()
    quiet_day["historical"]["release_lineage"]["today"].update(
        {"host": [], "pipeline": [], "host_count": 0, "pipeline_count": 0}
    )

    scorecard.validate_scorecard(quiet_day)


def test_negative_injection_exits_nonzero(tmp_path: Path) -> None:
    injected = _valid_scorecard()
    injected["live"]["fingerprints"]["host"]["commit"] = ""
    path = tmp_path / "injected-scorecard.json"
    path.write_text(json.dumps(injected), encoding="utf-8")

    completed = subprocess.run(
        [
            sys.executable,
            str(Path(scorecard.__file__).resolve()),
            "--validate",
            str(path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode != 0
    failure = json.loads(completed.stderr)
    assert failure["ok"] is False
    assert failure["code"] == "required_field_empty"


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        (
            {
                "outcome": "success",
                "terminal_state": "",
                "terminal_error_code": "",
                "contract_json": json.dumps({
                    "public_result": {
                        "responsibility": {"status": "supported_attribution"}
                    }
                }),
            },
            "high_confidence_supported_attribution",
        ),
        (
            {
                "outcome": "success",
                "terminal_state": "",
                "terminal_error_code": "",
                "contract_json": json.dumps({
                    "public_result": {
                        "responsibility": {"status": "candidate_from_live_rca"}
                    }
                }),
            },
            "medium_confidence_candidate_hypothesis",
        ),
        (
            {
                "outcome": "success",
                "terminal_state": "",
                "terminal_error_code": "",
                "contract_json": json.dumps({
                    "public_result": {
                        "responsibility": {"status": "suppressed_no_decoded_backing"}
                    }
                }),
            },
            "low_confidence_honest_non_attribution",
        ),
        (
            {
                "outcome": "quarantined",
                "terminal_state": "terminal_failed",
                "terminal_error_code": "vm_failure",
                "contract_json": "{}",
            },
            "technical_failure",
        ),
    ],
)
def test_live_tier_projection_uses_explicit_contract_fields(
    row: dict, expected: str
) -> None:
    assert scorecard._job_tier(row) == expected


def test_requester_identity_denominators_match_w10_categories() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.execute(
        "CREATE TABLE rca_trigger_sources(source_kind TEXT, requester_id TEXT)"
    )
    connection.executemany(
        "INSERT INTO rca_trigger_sources VALUES (?, ?)",
        [
            ("feishu_group_manual", "ou_person"),
            ("feishu_group_manual", "automation:repair"),
            ("feishu_group_manual", "operator-songying"),
            ("feishu_group_manual", "operator_songying"),
            ("feishu_group_manual", "codex-production"),
            ("feishu_group_manual", "codex_production"),
            ("kafka_workflow_event", ""),
        ],
    )

    result = scorecard._requester_identity_denominators(connection)

    assert result["counts"] == {
        "human": 1,
        "automation": 1,
        "legacy_automation": 4,
        "unknown": 1,
    }


def test_markdown_keeps_live_reference_historical_and_not_ga_visible() -> None:
    rendered = scorecard.render_markdown(_valid_scorecard())

    assert rendered.startswith("# PNC RCA Release Scorecard - NOT GA")
    assert "## Live" in rendered
    assert "## Reference" in rendered
    assert "## Historical" in rendered
    assert "NOT GA" in rendered
