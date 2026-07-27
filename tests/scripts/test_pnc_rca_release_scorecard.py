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
            "network_requests_performed": False,
            "external_effects_triggered": False,
        },
    }


def test_scorecard_validator_accepts_complete_not_ga_contract() -> None:
    scorecard.validate_scorecard(_valid_scorecard())


def test_scorecard_validator_accepts_legacy_v1_read_only_artifact() -> None:
    legacy = _valid_scorecard()
    legacy["schema_version"] = scorecard.LEGACY_SCHEMA_VERSION
    legacy["read_only_attestation"].pop("network_requests_performed")

    scorecard.validate_scorecard(legacy)


def test_scorecard_v2_rejects_canary_without_explicit_reachability() -> None:
    incomplete = _valid_scorecard()
    incomplete["live"]["canaries"]["natural_kafka"] = {
        "state": "pass",
        "checks": {"formal_report_url": True},
    }

    with pytest.raises(scorecard.ScorecardError) as raised:
        scorecard.validate_scorecard(incomplete)

    assert raised.value.code == "canary_publication_check_invalid"


def test_scorecard_validator_accepts_quiet_day_with_seven_day_lineage() -> None:
    quiet_day = _valid_scorecard()
    quiet_day["historical"]["release_lineage"]["today"].update({
        "host": [],
        "pipeline": [],
        "host_count": 0,
        "pipeline_count": 0,
    })

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


def test_natural_kafka_canary_requires_human_requester_identity() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.executescript(
        """
        CREATE TABLE rca_trigger_sources(
            source_id TEXT,
            source_kind TEXT,
            mode TEXT,
            requester_id TEXT,
            chat_id TEXT,
            thread_id TEXT,
            message_id TEXT,
            kafka_event_uid TEXT,
            created_at TEXT
        );
        CREATE TABLE rca_trigger_bindings(
            source_id TEXT,
            business_key TEXT,
            generation INTEGER
        );
        CREATE TABLE business_triggers(
            business_key TEXT,
            generation INTEGER,
            submission_key TEXT
        );
        CREATE TABLE rca_delivery_jobs(
            submission_key TEXT,
            delivery_id TEXT,
            work_item_id TEXT,
            outcome TEXT,
            status TEXT,
            report_url TEXT,
            updated_at TEXT
        );
        """
    )
    connection.executemany(
        "INSERT INTO rca_trigger_sources VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "automation-newer",
                "kafka_workflow_event",
                "issue_created",
                "automation:kafka",
                "",
                "",
                "",
                "event-automation",
                "2026-07-27T02:00:00Z",
            ),
            (
                "human-older",
                "kafka_workflow_event",
                "issue_created",
                "ou_owner",
                "",
                "",
                "",
                "event-human",
                "2026-07-27T01:00:00Z",
            ),
        ],
    )

    result = scorecard._latest_canary_row(
        connection,
        kind="natural_kafka",
        since=None,
    )

    assert result is not None
    assert result["source_id"] == "human-older"
    assert result["requester_id"] == "ou_owner"


@pytest.mark.parametrize(
    ("report_url", "receipt_source", "confirmed_url", "expected"),
    [
        (
            "http://192.168.26.174:18081/G1Q3_RCA/cases/a/index.html",
            "read_after_write",
            "http://192.168.26.174:18081/G1Q3_RCA/cases/a/index.html",
            {
                "report_url_nonempty": True,
                "report_url_reachable": True,
                "report_url_not_viz_mcap": True,
            },
        ),
        (
            "http://192.168.26.174:18081/G1Q3_RCA/cases/a/index.html",
            "read_before_write",
            "http://192.168.26.174:18081/G1Q3_RCA/cases/a/index.html",
            {
                "report_url_nonempty": True,
                "report_url_reachable": False,
                "report_url_not_viz_mcap": True,
            },
        ),
        (
            "http://192.168.26.174:18081/G1Q3_RCA/cases/a/a.viz.mcap",
            "read_after_write",
            "http://192.168.26.174:18081/G1Q3_RCA/cases/a/a.viz.mcap",
            {
                "report_url_nonempty": True,
                "report_url_reachable": True,
                "report_url_not_viz_mcap": False,
            },
        ),
    ],
)
def test_publication_url_uses_dispatcher_reachability_receipt(
    report_url: str,
    receipt_source: str,
    confirmed_url: str,
    expected: dict[str, bool],
) -> None:
    checks, evidence = scorecard._publication_report_url_checks(
        report_url,
        issue_effect={
            "effect_kind": "feishu_issue_comment",
            "status": "succeeded",
            "remote_receipt": {
                "source": receipt_source,
                "confirmed_report_url": confirmed_url,
            },
        },
    )

    assert checks == expected
    assert evidence["confirmed_report_url"] == confirmed_url
    assert evidence["proof_basis"].endswith(
        "exact_size_sha256" if expected["report_url_reachable"] else "not_proven"
    )


def test_canary_requires_dispatcher_report_url_reachability_receipt() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.execute(
        """
        CREATE TABLE rca_delivery_effects(
            delivery_id TEXT,
            effect_kind TEXT,
            required INTEGER,
            status TEXT,
            payload_json TEXT,
            remote_receipt_json TEXT,
            completed_at TEXT,
            created_at TEXT
        )
        """
    )
    report_url = (
        "http://192.168.26.174:18081/G1Q3_RCA/cases/issue-1/artifact-1/index.html"
    )
    effect = (
        "delivery-1",
        "feishu_issue_comment",
        1,
        "succeeded",
        "{}",
        json.dumps({
            "source": "read_after_write",
            "confirmed_report_url": report_url,
        }),
        OBSERVED_AT,
        OBSERVED_AT,
    )
    connection.execute(
        "INSERT INTO rca_delivery_effects VALUES (?, ?, ?, ?, ?, ?, ?, ?)", effect
    )
    row = {
        "source_id": "source-1",
        "created_at": OBSERVED_AT,
        "business_key": "business-1",
        "submission_key": "submission-1",
        "delivery_id": "delivery-1",
        "work_item_id": "issue-1",
        "generation": 1,
        "job_outcome": "success",
        "job_status": "delivered",
        "report_url": report_url,
    }

    reachable = scorecard._evaluate_canary(connection, row, kind="natural_kafka")
    connection.execute(
        "UPDATE rca_delivery_effects SET remote_receipt_json = ?",
        (json.dumps({"source": "read_before_write"}),),
    )
    unreachable = scorecard._evaluate_canary(connection, row, kind="natural_kafka")

    assert reachable["state"] == "pass"
    assert reachable["checks"]["report_url_reachable"] is True
    assert (
        reachable["report_url_reachability_evidence"]["confirmed_report_url"]
        == report_url
    )
    assert unreachable["state"] == "fail"
    assert unreachable["checks"]["report_url_reachable"] is False


def test_markdown_keeps_live_reference_historical_and_not_ga_visible() -> None:
    rendered = scorecard.render_markdown(_valid_scorecard())

    assert rendered.startswith("# PNC RCA Release Scorecard - NOT GA")
    assert "## Live" in rendered
    assert "## Reference" in rendered
    assert "## Historical" in rendered
    assert "NOT GA" in rendered
