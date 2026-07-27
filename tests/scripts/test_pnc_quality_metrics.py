from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from scripts import pnc_business_metrics as business_metrics
from scripts import pnc_quality_metrics as quality_metrics


OBSERVED_AT = "2026-07-26T00:00:00Z"
SQLITE_WINDOW_START = "2026-07-26T00:00:00Z"
SQLITE_WINDOW_END = "2026-07-27T00:00:00Z"
SQLITE_RELEASE = "release-20260726"
SQLITE_PIPELINE_COMMIT = "a" * 40


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _oracle(terminal_class: str, tier: str) -> dict:
    return {
        "schema_version": "pnc_rca_structural_tier_oracle_v2",
        "terminal_class": terminal_class,
        "confidence_tier": tier,
        "publication_allowed": True,
        "classification_conflict": False,
        "violations": [],
        "facts": {"golden_coverage_complete": tier != "none"},
    }


@pytest.fixture
def sqlite_observation_fixture(tmp_path: Path) -> tuple[Path, Path]:
    db_path = tmp_path / "control.sqlite3"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE control_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE rca_delivery_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE rca_admission_snapshots(
            snapshot_sha256 TEXT PRIMARY KEY,
            business_key TEXT NOT NULL,
            submission_key TEXT NOT NULL,
            generation INTEGER NOT NULL,
            execution_decision TEXT NOT NULL
        );
        CREATE TABLE rca_source_authority_receipts(
            authority_sha256 TEXT PRIMARY KEY,
            source_id TEXT NOT NULL,
            source_kind TEXT NOT NULL,
            payload_sha256 TEXT NOT NULL,
            authorization_evidence_sha256 TEXT NOT NULL,
            binding_action TEXT NOT NULL,
            decision TEXT NOT NULL
        );
        CREATE TABLE rca_snapshot_source_envelopes(
            source_envelope_sha256 TEXT PRIMARY KEY,
            snapshot_sha256 TEXT NOT NULL,
            submission_key TEXT NOT NULL,
            source_authority_sha256 TEXT NOT NULL,
            source_id TEXT NOT NULL,
            source_kind TEXT NOT NULL,
            payload_sha256 TEXT NOT NULL,
            authorization_evidence_sha256 TEXT NOT NULL,
            binding_action TEXT NOT NULL,
            decision TEXT NOT NULL,
            source_metadata_json TEXT NOT NULL
        );
        CREATE TABLE rca_delivery_jobs(
            delivery_id TEXT PRIMARY KEY,
            submission_key TEXT NOT NULL,
            business_key TEXT NOT NULL,
            generation INTEGER NOT NULL,
            project_key TEXT NOT NULL,
            work_item_id TEXT NOT NULL,
            outcome TEXT NOT NULL,
            outcome_key TEXT NOT NULL,
            terminal_state TEXT NOT NULL,
            terminal_error_code TEXT NOT NULL,
            status TEXT NOT NULL,
            contract_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE rca_delivery_effects(
            effect_key TEXT PRIMARY KEY,
            delivery_id TEXT NOT NULL,
            effect_kind TEXT NOT NULL,
            required INTEGER NOT NULL,
            payload_json TEXT NOT NULL,
            status TEXT NOT NULL,
            write_phase TEXT NOT NULL,
            remote_receipt_json TEXT,
            completed_at TEXT,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE rca_conclusion_adjudications(
            adjudication_id TEXT PRIMARY KEY,
            schema_version TEXT NOT NULL,
            business_key TEXT NOT NULL,
            generation INTEGER NOT NULL,
            work_item_id TEXT NOT NULL,
            action TEXT NOT NULL,
            conclusion_state TEXT NOT NULL,
            actor_id TEXT NOT NULL,
            original_delivery_id TEXT NOT NULL,
            original_effect_key TEXT NOT NULL,
            correction_effect_key TEXT NOT NULL,
            activation_epoch_id TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE rca_conclusion_adjudication_repairs(
            adjudication_id TEXT PRIMARY KEY,
            status TEXT NOT NULL
        );
        """
    )
    conn.execute(
        "INSERT INTO control_meta VALUES('schema_version', ?)",
        (business_metrics.CONTROL_STORE_SCHEMA_VERSION,),
    )
    conn.execute(
        "INSERT INTO rca_delivery_meta VALUES('schema_version', ?)",
        (business_metrics.DELIVERY_STORE_SCHEMA_VERSION,),
    )
    immutable_tables = {
        "source_authority": "rca_source_authority_receipts",
        "admission_snapshot": "rca_admission_snapshots",
        "snapshot_envelope": "rca_snapshot_source_envelopes",
        "conclusion_adjudication": "rca_conclusion_adjudications",
    }
    for prefix, table in immutable_tables.items():
        for operation in ("UPDATE", "DELETE"):
            suffix = operation.lower()
            conn.execute(
                f"CREATE TRIGGER trg_rca_{prefix}_no_{suffix} "
                f"BEFORE {operation} ON {table} "
                "BEGIN SELECT RAISE(ABORT, 'immutable'); END"
            )

    cases = [
        {
            "name": "recognized",
            "generation": 1,
            "outcome": "success",
            "error": "",
            "diagnostic": "",
            "oracle": _oracle("candidate_hypothesis", "medium"),
            "golden": (True, False, False, "candidate_hypothesis", "allow"),
        },
        {
            "name": "unsupported",
            "generation": 1,
            "outcome": "terminal_failed",
            "error": "business_profile_unsupported",
            "diagnostic": "business_route_unsupported",
            "oracle": None,
            "golden": (False, None, None, "business_route_unsupported", "block"),
        },
        {
            "name": "event-not-found",
            "generation": 1,
            "outcome": "terminal_failed",
            "error": "remote_event_not_found",
            "diagnostic": "",
            "oracle": _oracle("honest_non_attribution", "low"),
            "golden": (False, None, None, "honest_non_attribution", "allow"),
        },
        {
            "name": "system-failure",
            "generation": 1,
            "outcome": "terminal_failed",
            "error": "analysis_failed",
            "diagnostic": "analysis_failed",
            "oracle": None,
            "golden": (True, False, True, "analysis_failed", "block"),
        },
    ]
    golden_records = []
    for ordinal, case in enumerate(cases, start=1):
        name = case["name"]
        business_key = f"business-{name}"
        submission_key = f"submission-{name}"
        delivery_id = f"delivery-{name}"
        snapshot_sha = _digest(f"snapshot:{name}")
        created_at = f"2026-07-26T0{ordinal}:00:00+00:00"
        conn.execute(
            "INSERT INTO rca_admission_snapshots VALUES(?, ?, ?, ?, 'admit')",
            (snapshot_sha, business_key, submission_key, case["generation"]),
        )
        for entry_index, source_kind in enumerate((
            "kafka_workflow_event",
            "feishu_group_manual",
        )):
            source_id = f"source-{name}-{entry_index}"
            envelope_sha = _digest(f"envelope:{source_id}")
            authority_sha = _digest(f"authority:{source_id}")
            payload_sha = _digest(f"payload:{source_id}")
            evidence_sha = _digest(f"evidence:{source_id}")
            binding_action = "create" if entry_index == 0 else "join"
            conn.execute(
                "INSERT INTO rca_source_authority_receipts "
                "VALUES(?, ?, ?, ?, ?, ?, 'admit')",
                (
                    authority_sha,
                    source_id,
                    source_kind,
                    payload_sha,
                    evidence_sha,
                    binding_action,
                ),
            )
            conn.execute(
                "INSERT INTO rca_snapshot_source_envelopes "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 'admit', ?)",
                (
                    envelope_sha,
                    snapshot_sha,
                    submission_key,
                    authority_sha,
                    source_id,
                    source_kind,
                    payload_sha,
                    evidence_sha,
                    binding_action,
                    json.dumps({"requester_id": f"requester-{source_id}"}),
                ),
            )
        conn.execute(
            "INSERT INTO rca_delivery_jobs VALUES(?, ?, ?, ?, 't03o4q', ?, ?, '', "
            "?, ?, 'delivered', '{}', ?, ?)",
            (
                delivery_id,
                submission_key,
                business_key,
                case["generation"],
                str(7000 + ordinal),
                case["outcome"],
                "completed",
                case["error"],
                created_at,
                created_at,
            ),
        )
        payload = {
            "schema_version": "pnc_rca_delivery_effect_v3",
            "outcome": case["outcome"],
        }
        if case["diagnostic"]:
            payload["diagnostic_code"] = case["diagnostic"]
        if case["oracle"] is not None:
            payload.update({
                "terminal_class": case["oracle"]["terminal_class"],
                "confidence_tier": case["oracle"]["confidence_tier"],
                "quality_oracle": case["oracle"],
                "quality_oracle_sha256": hashlib.sha256(
                    json.dumps(
                        case["oracle"],
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                ).hexdigest(),
            })
        effect_key = f"effect-{name}"
        conn.execute(
            "INSERT INTO rca_delivery_effects VALUES(?, ?, 'feishu_issue_comment', "
            "1, ?, 'succeeded', 'settled', ?, ?, ?)",
            (
                effect_key,
                delivery_id,
                json.dumps(payload, ensure_ascii=False, sort_keys=True),
                json.dumps({"source": "read_after_write", "remote_id": effect_key}),
                created_at,
                created_at,
            ),
        )
        if name == "recognized":
            correction_key = "effect-recognized-correction"
            conn.execute(
                "INSERT INTO rca_delivery_effects VALUES(?, ?, "
                "'feishu_issue_comment', 1, ?, 'succeeded', 'settled', ?, ?, ?)",
                (
                    correction_key,
                    delivery_id,
                    json.dumps({
                        "schema_version": ("pnc_rca_conclusion_adjudication_effect_v2"),
                        "action": "recognize",
                    }),
                    json.dumps({"source": "read_after_write", "remote_id": "owner"}),
                    created_at,
                    created_at,
                ),
            )
            conn.execute(
                "INSERT INTO rca_conclusion_adjudications VALUES(?, ?, ?, 1, ?, "
                "'recognize', 'recognized', 'owner-1', ?, ?, ?, 'epoch-1', ?)",
                (
                    "adjudication-recognized",
                    business_metrics.ADJUDICATION_SCHEMA_VERSION,
                    business_key,
                    str(7000 + ordinal),
                    delivery_id,
                    effect_key,
                    correction_key,
                    created_at,
                ),
            )
            conn.execute(
                "INSERT INTO rca_conclusion_adjudication_repairs "
                "VALUES('adjudication-recognized', 'succeeded')"
            )
        evaluated, false_high, regression, expected_class, expected_gate = case[
            "golden"
        ]
        golden_records.append({
            "business_key": business_key,
            "generation": case["generation"],
            "release_id": SQLITE_RELEASE,
            "pipeline_commit": SQLITE_PIPELINE_COMMIT,
            "evaluated": evaluated,
            "false_high_confidence": false_high,
            "regression": regression,
            "expected_terminal_class": expected_class,
            "expected_gate_decision": expected_gate,
        })
    conn.commit()
    conn.close()

    golden_path = tmp_path / "golden.json"
    golden_path.write_text(
        json.dumps({
            "schema_version": business_metrics.GOLDEN_INPUT_SCHEMA_VERSION,
            "records": golden_records,
        }),
        encoding="utf-8",
    )
    return db_path, golden_path


def _record(
    *,
    pair_id: str,
    entry: str,
    scope: str,
    tier: str,
    e2e: str = "success",
    delivery: str = "succeeded",
    readback: str = "verified",
    attribution: str = "owner_accepted",
    owner_decision: str = "accepted",
    false_high: bool = False,
    regression: bool = False,
    triage_kind: str = "lane",
    triage_expected_kind: str = "lane",
    gate_decision: str = "allow",
    gate_review_decision: str = "allow",
    coverage_count: int = 100,
    report_count: int = 200,
    field_write_count: int = 300,
) -> dict:
    source_kind = "kafka_workflow_event" if entry == "kafka" else "feishu_group_manual"
    return {
        "record_id": f"{pair_id}-{entry}",
        "pair_id": pair_id,
        "release_id": "release-20260726",
        "business_line": "g1q3_rca",
        "source_kind": source_kind,
        "confidence_tier": tier,
        "denominator_kind": scope,
        "e2e": {"status": e2e},
        "technical": {
            "delivery_status": delivery,
            "readback_status": readback,
        },
        "attribution": {
            "outcome": attribution,
            "owner_decision": owner_decision,
        },
        "golden": {
            "evaluated": True,
            "false_high_confidence": false_high,
            "regression": regression,
        },
        "signals": {
            "triage": {
                "kind": triage_kind,
                "expected_kind": triage_expected_kind,
            },
            "gate": {
                "decision": gate_decision,
                "review_decision": gate_review_decision,
            },
        },
        "coverage_count": coverage_count,
        "report_count": report_count,
        "field_write_count": field_write_count,
    }


def _clean_records() -> list[dict]:
    return [
        _record(
            pair_id="business-pair",
            entry="kafka",
            scope="business",
            tier="medium",
            attribution="candidate",
        ),
        _record(
            pair_id="business-pair",
            entry="feishu",
            scope="business",
            tier="medium",
            owner_decision="rejected",
            attribution="owner_rejected",
            triage_expected_kind="aeb",
        ),
        _record(
            pair_id="system-pair",
            entry="kafka",
            scope="system",
            tier="high",
            attribution="unsupported",
        ),
        _record(
            pair_id="system-pair",
            entry="feishu",
            scope="system",
            tier="high",
            e2e="failed",
            readback="failed",
            attribution="event_not_found",
            false_high=True,
            gate_review_decision="block",
        ),
    ]


def _rewrite_golden_record(path: Path, business_key: str, **updates: object) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    for record in payload["records"]:
        if record["business_key"] == business_key:
            record.update(updates)
            break
    else:
        raise AssertionError(f"golden record not found: {business_key}")
    path.write_text(json.dumps(payload), encoding="utf-8")


def _remove_adjudication(db_path: Path, business_key: str) -> None:
    """Remove one temporary-fixture adjudication while retaining immutability."""

    conn = sqlite3.connect(db_path)
    conn.execute("DROP TRIGGER trg_rca_conclusion_adjudication_no_delete")
    row = conn.execute(
        "SELECT adjudication_id, correction_effect_key "
        "FROM rca_conclusion_adjudications WHERE business_key = ?",
        (business_key,),
    ).fetchone()
    assert row is not None
    adjudication_id, correction_effect_key = row
    conn.execute(
        "DELETE FROM rca_conclusion_adjudication_repairs WHERE adjudication_id = ?",
        (adjudication_id,),
    )
    conn.execute(
        "DELETE FROM rca_conclusion_adjudications WHERE adjudication_id = ?",
        (adjudication_id,),
    )
    conn.execute(
        "DELETE FROM rca_delivery_effects WHERE effect_key = ?",
        (correction_effect_key,),
    )
    conn.execute(
        "CREATE TRIGGER trg_rca_conclusion_adjudication_no_delete "
        "BEFORE DELETE ON rca_conclusion_adjudications "
        "BEGIN SELECT RAISE(ABORT, 'immutable'); END"
    )
    conn.commit()
    conn.close()


def _replace_effect_oracle(
    db_path: Path,
    effect_key: str,
    *,
    terminal_class: str,
    confidence_tier: str,
) -> None:
    conn = sqlite3.connect(db_path)
    payload = json.loads(
        conn.execute(
            "SELECT payload_json FROM rca_delivery_effects WHERE effect_key = ?",
            (effect_key,),
        ).fetchone()[0]
    )
    oracle = {
        "schema_version": "pnc_rca_structural_tier_oracle_v2",
        "terminal_class": terminal_class,
        "confidence_tier": confidence_tier,
        "publication_allowed": True,
        "classification_conflict": False,
        "violations": [],
        "facts": {"golden_coverage_complete": confidence_tier != "low"},
    }
    digest = hashlib.sha256(
        json.dumps(
            oracle,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    payload.update({
        "terminal_class": terminal_class,
        "confidence_tier": confidence_tier,
        "quality_oracle": oracle,
        "quality_oracle_sha256": digest,
    })
    conn.execute(
        "UPDATE rca_delivery_effects SET payload_json = ? WHERE effect_key = ?",
        (json.dumps(payload, ensure_ascii=False, sort_keys=True), effect_key),
    )
    conn.commit()
    conn.close()


def _group(report: dict, *, entry: str, tier: str) -> dict:
    return next(
        group
        for group in report["groups"]
        if group["dimensions"]["entry"] == entry
        and group["dimensions"]["confidence_tier"] == tier
    )


def test_daily_report_groups_four_axes_and_never_mixes_denominators() -> None:
    report = quality_metrics.build_daily_report(
        _clean_records(), observed_at=OBSERVED_AT
    )

    assert report["ok"] is True
    assert report["grouping"] == [
        "release",
        "business",
        "entry",
        "confidence_tier",
    ]
    assert len(report["groups"]) == 4

    business = _group(report, entry="kafka", tier="medium")
    assert business["dimensions"]["business"] == "g1q3-rca"
    assert business["denominators"] == {"business": 1, "system": 0}
    assert business["metrics"]["dual_entry_e2e_success"]["by_denominator"][
        "business"
    ] == {
        "numerator": 1,
        "denominator": 1,
        "rate_pct": 100.0,
    }
    assert (
        business["metrics"]["useful_attribution"]["by_denominator"]["business"][
            "numerator"
        ]
        == 1
    )
    assert (
        business["metrics"]["useful_attribution"]["by_denominator"]["system"][
            "denominator"
        ]
        == 0
    )

    system = _group(report, entry="kafka", tier="high")
    assert system["denominators"] == {"business": 0, "system": 1}
    # Each entry has its own denominator; the Feishu failure cannot pollute
    # Kafka's success rate.
    assert system["metrics"]["dual_entry_e2e_success"]["by_denominator"]["system"] == {
        "numerator": 1,
        "denominator": 1,
        "rate_pct": 100.0,
    }
    assert (
        system["metrics"]["technical_delivery_readback"]["by_denominator"]["system"][
            "numerator"
        ]
        == 1
    )
    assert (
        system["metrics"]["useful_attribution"]["by_denominator"]["business"][
            "denominator"
        ]
        == 0
    )

    for group in report["groups"]:
        for metric in group["metrics"].values():
            assert set(metric["by_denominator"]) == {"business", "system"}
            assert "denominator" not in metric, (
                "a mixed top-level denominator is forbidden"
            )


def test_unsupported_and_event_not_found_are_auxiliary_not_attribution_success() -> (
    None
):
    records = [
        _record(
            pair_id="excluded",
            entry="kafka",
            scope="business",
            tier="low",
            attribution="unsupported",
            owner_decision="pending",
        ),
        _record(
            pair_id="excluded",
            entry="feishu",
            scope="business",
            tier="low",
            attribution="event-not-found",
            owner_decision="pending",
        ),
    ]

    report = quality_metrics.build_daily_report(records, observed_at=OBSERVED_AT)
    kafka = _group(report, entry="kafka", tier="low")
    feishu = _group(report, entry="feishu", tier="low")

    assert (
        kafka["metrics"]["useful_attribution"]["by_denominator"]["business"][
            "denominator"
        ]
        == 0
    )
    assert (
        feishu["metrics"]["useful_attribution"]["by_denominator"]["business"][
            "denominator"
        ]
        == 0
    )
    assert report["auxiliary"]["attribution_exclusions"] == {
        "event_not_found": 1,
        "not_attributable": 0,
        "unsupported": 1,
    }


def test_high_and_low_tiers_never_enter_owner_attribution_denominator() -> None:
    records = [
        _record(
            pair_id="high-no-owner",
            entry=entry,
            scope="business",
            tier="high",
            attribution="supported_attribution",
            owner_decision="",
        )
        for entry in ("kafka", "feishu")
    ] + [
        _record(
            pair_id="low-honest",
            entry=entry,
            scope="business",
            tier="low",
            attribution="not_attributable",
            owner_decision="",
        )
        for entry in ("kafka", "feishu")
    ]

    report = quality_metrics.build_daily_report(records, observed_at=OBSERVED_AT)

    assert report["ok"] is True
    high = _group(report, entry="kafka", tier="high")
    low = _group(report, entry="kafka", tier="low")
    for group in (high, low):
        assert group["metrics"]["useful_attribution"]["by_denominator"]["business"] == {
            "numerator": 0,
            "denominator": 0,
            "rate_pct": None,
        }
        assert group["signals"]["rca_adoption_rate"]["by_denominator"]["business"] == {
            "numerator": 0,
            "denominator": 0,
            "rate_pct": None,
        }
    assert report["auxiliary"]["attribution_exclusions"]["not_attributable"] == 2


def test_auxiliary_counts_cannot_inflate_any_metric_denominator() -> None:
    records = [
        _record(
            pair_id="auxiliary",
            entry=entry,
            scope="business",
            tier="medium",
            coverage_count=10_000,
            report_count=20_000,
            field_write_count=30_000,
        )
        for entry in ("kafka", "feishu")
    ]

    report = quality_metrics.build_daily_report(records, observed_at=OBSERVED_AT)

    assert report["auxiliary"]["coverage_count"] == 20_000
    assert report["auxiliary"]["report_count"] == 40_000
    assert report["auxiliary"]["field_write_count"] == 60_000
    for group in report["groups"]:
        for metric in group["metrics"].values():
            assert (
                sum(
                    bucket["denominator"]
                    for bucket in metric["by_denominator"].values()
                )
                <= 1
            )


def test_three_former_todo_signals_have_clean_fields_and_rates() -> None:
    report = quality_metrics.build_daily_report(
        _clean_records(), observed_at=OBSERVED_AT
    )

    inventory = {item["name"]: item for item in report["signal_inventory"]}
    assert set(inventory) == {
        "triage_accuracy_kind_distribution",
        "rca_adoption_rate",
        "gate_consistency_rate",
    }
    assert all(item["status"] == "have" for item in inventory.values())
    assert all(item["clean_fields"] for item in inventory.values())

    business = _group(report, entry="kafka", tier="medium")
    assert (
        business["signals"]["triage_accuracy_kind_distribution"]["by_denominator"][
            "business"
        ]["rate_pct"]
        == 100.0
    )
    assert (
        business["signals"]["rca_adoption_rate"]["by_denominator"]["business"][
            "rate_pct"
        ]
        == 100.0
    )

    system = _group(report, entry="feishu", tier="high")
    assert (
        system["signals"]["gate_consistency_rate"]["by_denominator"]["system"][
            "rate_pct"
        ]
        == 0.0
    )
    assert system["metrics"]["false_high_confidence_no_regression"]["failure_counts"][
        "system"
    ] == {"false_high_confidence": 1, "regression": 0}


def test_markdown_keeps_auxiliary_in_a_separate_section() -> None:
    report = quality_metrics.build_daily_report(
        _clean_records(), observed_at=OBSERVED_AT
    )

    rendered = quality_metrics.render_markdown(report)

    assert "release × business × entry × confidence_tier" in rendered
    assert "## Auxiliary (not metric denominators)" in rendered
    assert "coverage_count:" in rendered


def test_normalizer_rejects_an_implicit_denominator_scope() -> None:
    row = _record(
        pair_id="implicit-scope",
        entry="kafka",
        scope="business",
        tier="medium",
    )
    row.pop("denominator_kind")

    with pytest.raises(business_metrics.MetricsValidationError) as error:
        business_metrics.normalize_record(row)

    assert error.value.code == "metrics_dimension_required"


def test_daily_report_rejects_duplicate_record_ids() -> None:
    first = _record(
        pair_id="duplicate-record-id",
        entry="kafka",
        scope="business",
        tier="medium",
    )
    second = _record(
        pair_id="duplicate-record-id",
        entry="feishu",
        scope="business",
        tier="medium",
    )
    second["record_id"] = first["record_id"]

    with pytest.raises(business_metrics.MetricsValidationError) as error:
        quality_metrics.build_daily_report([first, second], observed_at=OBSERVED_AT)

    assert error.value.code == "metrics_duplicate_record_id"


def test_daily_report_rejects_duplicate_pair_entry_observations() -> None:
    first = _record(
        pair_id="duplicate-pair-entry",
        entry="kafka",
        scope="business",
        tier="medium",
    )
    second = dict(first)
    second["record_id"] = "duplicate-pair-entry-kafka-retry"

    with pytest.raises(business_metrics.MetricsValidationError) as error:
        quality_metrics.build_daily_report([first, second], observed_at=OBSERVED_AT)

    assert error.value.code == "metrics_duplicate_pair_entry"


def test_daily_report_rejects_malformed_already_normalized_row() -> None:
    with pytest.raises(business_metrics.MetricsValidationError) as error:
        quality_metrics.build_daily_report(
            [{"schema_version": business_metrics.SCHEMA_VERSION}],
            observed_at=OBSERVED_AT,
        )

    assert error.value.code == "metrics_normalized_dimensions_missing"


def test_negative_mixed_scope_pair_injection_exits_nonzero(tmp_path: Path) -> None:
    injected = [
        _record(
            pair_id="mixed-scope",
            entry="kafka",
            scope="business",
            tier="medium",
        ),
        _record(
            pair_id="mixed-scope",
            entry="feishu",
            scope="system",
            tier="medium",
        ),
    ]
    path = tmp_path / "mixed-scope-injection.json"
    path.write_text(json.dumps(injected), encoding="utf-8")

    completed = subprocess.run(
        [
            sys.executable,
            str(Path(quality_metrics.__file__).resolve()),
            "--input",
            str(path),
            "--observed-at",
            OBSERVED_AT,
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode != 0
    failure = json.loads(completed.stderr)
    assert failure["ok"] is False
    assert failure["code"] == "metrics_report_not_clean"
    assert "metrics_pair_denominator_mixed" in failure["detail"]


def test_sqlite_observation_producer_joins_identity_w13_oracle_and_golden(
    sqlite_observation_fixture: tuple[Path, Path],
) -> None:
    db_path, golden_path = sqlite_observation_fixture
    before = db_path.stat()

    rows = business_metrics.load_sqlite_observations(
        db_path,
        release_id=SQLITE_RELEASE,
        pipeline_commit=SQLITE_PIPELINE_COMMIT,
        window_start=SQLITE_WINDOW_START,
        window_end=SQLITE_WINDOW_END,
        golden_input=golden_path,
    )
    report = quality_metrics.build_daily_report(rows, observed_at=SQLITE_WINDOW_END)

    after = db_path.stat()
    assert (before.st_ino, before.st_size, before.st_mtime_ns) == (
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    )
    assert not Path(f"{db_path}-wal").exists()
    assert len(rows) == 8
    assert len({row["record_id"] for row in rows}) == 8
    assert {row["entry"] for row in rows} == {"kafka", "feishu"}
    assert all(row["identity_provenance"]["source_id"] for row in rows)
    assert all(row["entry_provenance"]["entry"] == row["entry"] for row in rows)
    assert all(
        row["quality_oracle_sha256"] for row in rows if row["confidence_tier"] != "none"
    )
    assert all(
        row["golden_provenance"]["pipeline_commit"] == SQLITE_PIPELINE_COMMIT
        for row in rows
    )

    recognized = _group(report, entry="kafka", tier="medium")
    assert recognized["metrics"]["useful_attribution"]["by_denominator"][
        "business"
    ] == {"numerator": 1, "denominator": 1, "rate_pct": 100.0}
    assert recognized["metrics"]["false_high_confidence_no_regression"][
        "by_denominator"
    ]["business"] == {"numerator": 1, "denominator": 1, "rate_pct": 100.0}

    shared_none_tier = _group(report, entry="kafka", tier="none")
    assert shared_none_tier["denominators"] == {"business": 1, "system": 1}
    assert shared_none_tier["metrics"]["false_high_confidence_no_regression"][
        "by_denominator"
    ]["system"] == {
        "numerator": 0,
        "denominator": 1,
        "rate_pct": 0.0,
    }
    assert report["auxiliary"]["attribution_exclusions"] == {
        "event_not_found": 2,
        "not_attributable": 2,
        "unsupported": 2,
    }
    assert report["ok"] is True


def test_sqlite_loader_accepts_high_and_low_without_w13_adjudication(
    sqlite_observation_fixture: tuple[Path, Path],
) -> None:
    db_path, golden_path = sqlite_observation_fixture

    # Recognized is changed from medium to high, then its owner correction is
    # removed.  High confidence is golden/oracle governed, not W13 governed.
    _remove_adjudication(db_path, "business-recognized")
    _replace_effect_oracle(
        db_path,
        "effect-recognized",
        terminal_class="supported_attribution",
        confidence_tier="high",
    )
    _rewrite_golden_record(
        golden_path,
        "business-recognized",
        expected_terminal_class="supported_attribution",
    )

    # Event-not-found is changed to a successful low honest non-attribution;
    # it has no owner review and must remain an explicit excluded outcome.
    conn = sqlite3.connect(db_path)
    conn.execute(
        "UPDATE rca_delivery_jobs SET outcome='success', outcome_key='completed', "
        "terminal_state='', terminal_error_code='' "
        "WHERE delivery_id='delivery-event-not-found'"
    )
    conn.commit()
    conn.close()
    _replace_effect_oracle(
        db_path,
        "effect-event-not-found",
        terminal_class="honest_non_attribution",
        confidence_tier="low",
    )

    rows = business_metrics.load_sqlite_observations(
        db_path,
        release_id=SQLITE_RELEASE,
        pipeline_commit=SQLITE_PIPELINE_COMMIT,
        window_start=SQLITE_WINDOW_START,
        window_end=SQLITE_WINDOW_END,
        golden_input=golden_path,
    )
    report = quality_metrics.build_daily_report(rows, observed_at=SQLITE_WINDOW_END)

    assert report["ok"] is True
    high_rows = [row for row in rows if row["pair_id"] == "delivery-recognized"]
    low_rows = [row for row in rows if row["pair_id"] == "delivery-event-not-found"]
    assert {row["confidence_tier"] for row in high_rows} == {"high"}
    assert {row["owner_decision"] for row in high_rows} == {""}
    assert {row["attribution_outcome"] for row in high_rows} == {
        "supported_attribution"
    }
    assert {row["confidence_tier"] for row in low_rows} == {"low"}
    assert {row["owner_decision"] for row in low_rows} == {""}
    assert {row["attribution_outcome"] for row in low_rows} == {"not_attributable"}


def test_sqlite_loader_requires_w13_for_medium_only(
    sqlite_observation_fixture: tuple[Path, Path],
) -> None:
    db_path, golden_path = sqlite_observation_fixture
    _remove_adjudication(db_path, "business-recognized")

    with pytest.raises(business_metrics.MetricsValidationError) as error:
        business_metrics.load_sqlite_observations(
            db_path,
            release_id=SQLITE_RELEASE,
            pipeline_commit=SQLITE_PIPELINE_COMMIT,
            window_start=SQLITE_WINDOW_START,
            window_end=SQLITE_WINDOW_END,
            golden_input=golden_path,
        )

    assert error.value.code == "metrics_owner_adjudication_missing"


@pytest.mark.parametrize(
    ("business_key", "updates"),
    [
        (
            "business-recognized",
            {"evaluated": True, "false_high_confidence": None},
        ),
        (
            "business-event-not-found",
            {"evaluated": False, "false_high_confidence": False},
        ),
    ],
)
def test_high_low_golden_fields_remain_fail_closed(
    sqlite_observation_fixture: tuple[Path, Path],
    business_key: str,
    updates: dict[str, object],
) -> None:
    db_path, golden_path = sqlite_observation_fixture
    _rewrite_golden_record(golden_path, business_key, **updates)

    with pytest.raises(business_metrics.MetricsValidationError) as error:
        business_metrics.load_sqlite_observations(
            db_path,
            release_id=SQLITE_RELEASE,
            pipeline_commit=SQLITE_PIPELINE_COMMIT,
            window_start=SQLITE_WINDOW_START,
            window_end=SQLITE_WINDOW_END,
            golden_input=golden_path,
        )

    assert error.value.code == "metrics_golden_binding_invalid"


def test_sqlite_cli_feeds_existing_daily_report(
    sqlite_observation_fixture: tuple[Path, Path],
) -> None:
    db_path, golden_path = sqlite_observation_fixture
    completed = subprocess.run(
        [
            sys.executable,
            str(Path(quality_metrics.__file__).resolve()),
            "--control-db",
            str(db_path),
            "--release-id",
            SQLITE_RELEASE,
            "--pipeline-commit",
            SQLITE_PIPELINE_COMMIT,
            "--window-start",
            SQLITE_WINDOW_START,
            "--window-end",
            SQLITE_WINDOW_END,
            "--golden-input",
            str(golden_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    report = json.loads(completed.stdout)
    assert report["ok"] is True
    assert report["source"]["mode"] == "sqlite_uri_mode_ro_immutable"
    assert report["source"]["runtime_mutation_performed"] is False
    assert report["source"]["wal_created"] is False


@pytest.mark.parametrize(
    "control_schema_version",
    ["pnc_rca_control_store_v12", "pnc_rca_control_store_v13"],
)
def test_sqlite_observation_accepts_integrated_control_schema(
    sqlite_observation_fixture: tuple[Path, Path],
    control_schema_version: str,
) -> None:
    db_path, golden_path = sqlite_observation_fixture
    conn = sqlite3.connect(db_path)
    conn.execute(
        "UPDATE control_meta SET value = ? WHERE key = 'schema_version'",
        (control_schema_version,),
    )
    conn.commit()
    conn.close()

    rows = business_metrics.load_sqlite_observations(
        db_path,
        release_id=SQLITE_RELEASE,
        pipeline_commit=SQLITE_PIPELINE_COMMIT,
        window_start=SQLITE_WINDOW_START,
        window_end=SQLITE_WINDOW_END,
        golden_input=golden_path,
    )
    assert len(rows) == 8


@pytest.mark.parametrize("corruption", ["missing_schema", "malformed_marker"])
def test_sqlite_cli_missing_or_malformed_schema_exits_two(
    sqlite_observation_fixture: tuple[Path, Path], corruption: str
) -> None:
    db_path, golden_path = sqlite_observation_fixture
    conn = sqlite3.connect(db_path)
    if corruption == "missing_schema":
        conn.execute("DROP TABLE rca_conclusion_adjudications")
    else:
        conn.execute(
            "UPDATE rca_delivery_meta SET value = 'pnc_rca_delivery_store_v7' "
            "WHERE key = 'schema_version'"
        )
    conn.commit()
    conn.close()

    completed = subprocess.run(
        [
            sys.executable,
            str(Path(quality_metrics.__file__).resolve()),
            "--control-db",
            str(db_path),
            "--release-id",
            SQLITE_RELEASE,
            "--pipeline-commit",
            SQLITE_PIPELINE_COMMIT,
            "--window-start",
            SQLITE_WINDOW_START,
            "--window-end",
            SQLITE_WINDOW_END,
            "--golden-input",
            str(golden_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 2
    failure = json.loads(completed.stderr)
    assert failure["code"] in {
        "metrics_control_db_schema_missing",
        "metrics_control_db_schema_mismatch",
    }
    assert not Path(f"{db_path}-wal").exists()
