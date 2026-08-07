import json
import sqlite3

import pytest

from scripts.pnc_rca_batch_rerun import QUEUE_SCHEMA_VERSION
from scripts.pnc_rca_exact_refresh_queue import (
    SOURCE_FILTER,
    ExactRefreshQueueError,
    build_queue,
)


def _source(path, *, logic="AND"):
    value = {
        "schema_version": "pnc_rca_batch_queue_v1",
        "source_inventory_sha256": "a" * 64,
        "filter": {**SOURCE_FILTER, "logic": logic},
        "items": [
            {
                "issue_id": "7048803418",
                "title": "ACC braking issue",
                "quality_classification": "missing",
                "current_submission_key": "",
                "priority": 1,
            },
            {
                "issue_id": "7048803419",
                "title": "LCC lane issue",
                "quality_classification": "legacy_or_other",
                "current_submission_key": "",
                "priority": 2,
            },
        ],
    }
    path.write_text(json.dumps(value), encoding="utf-8")
    return path


def _db(path, *, ambiguous=False):
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE business_triggers ("
        "work_item_id TEXT, generation INTEGER, submission_key TEXT, created_at TEXT)"
    )
    conn.execute(
        "INSERT INTO business_triggers VALUES (?, ?, ?, ?)",
        (
            "7048803418",
            2,
            "g1q3-rca-s1-" + "b" * 64,
            "2026-08-07T00:00:00+00:00",
        ),
    )
    if ambiguous:
        conn.execute(
            "INSERT INTO business_triggers VALUES (?, ?, ?, ?)",
            (
                "7048803418",
                2,
                "g1q3-rca-s1-" + "c" * 64,
                "2026-08-07T00:01:00+00:00",
            ),
        )
    conn.commit()
    conn.close()
    return path


def test_build_queue_binds_exact_scope_and_latest_control_preconditions(tmp_path):
    queue, receipt = build_queue(
        source_path=_source(tmp_path / "source.json"),
        control_db=_db(tmp_path / "control.sqlite3"),
        batch_id="g1q3-li-taohua-refresh",
    )

    assert queue["schema_version"] == QUEUE_SCHEMA_VERSION
    assert queue["scope"]["logic"] == "AND"
    assert queue["scope"]["project_relation_id"] == "6670325063"
    assert queue["scope"]["creator_key"] == "7649830284321508335"
    assert queue["items"][0]["current_generation"] == 2
    assert queue["items"][0]["current_submission_key"].endswith("b" * 64)
    assert queue["items"][1]["current_generation"] == 0
    assert queue["items"][1]["current_submission_key"] == ""
    assert receipt["counts"] == {"total": 2, "existing": 1, "absent": 1}
    assert all(value == 0 for value in receipt["production_effects"].values())


def test_build_queue_rejects_or_scope(tmp_path):
    with pytest.raises(
        ExactRefreshQueueError, match="exact_refresh_source_contract_invalid"
    ):
        build_queue(
            source_path=_source(tmp_path / "source.json", logic="OR"),
            control_db=_db(tmp_path / "control.sqlite3"),
            batch_id="g1q3-li-taohua-refresh",
        )


def test_build_queue_rejects_ambiguous_latest_generation(tmp_path):
    with pytest.raises(
        ExactRefreshQueueError, match="exact_refresh_control_scope_ambiguous"
    ):
        build_queue(
            source_path=_source(tmp_path / "source.json"),
            control_db=_db(tmp_path / "control.sqlite3", ambiguous=True),
            batch_id="g1q3-li-taohua-refresh",
        )
