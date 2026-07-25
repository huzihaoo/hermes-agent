from __future__ import annotations

import sqlite3
from pathlib import Path

from scripts.pnc_rca_requester_identity_report import build_report


def _db(path: Path) -> None:
    connection = sqlite3.connect(path)
    connection.execute(
        "CREATE TABLE rca_trigger_sources ("
        "source_id TEXT, source_kind TEXT, platform TEXT, requester_id TEXT, created_at TEXT)"
    )
    connection.executemany(
        "INSERT INTO rca_trigger_sources VALUES (?, ?, ?, ?, ?)",
        [
            (
                "old-operator",
                "feishu_group_manual",
                "operator",
                "operator-songying",
                "2026-07-25T09:00:00+00:00",
            ),
            (
                "old-human",
                "feishu_group_manual",
                "feishu",
                "ou_person123",
                "2026-07-25T09:01:00+00:00",
            ),
            (
                "new-automation",
                "feishu_group_manual",
                "operator",
                "automation:rca-ga",
                "2026-07-25T11:00:00+00:00",
            ),
            (
                "new-human",
                "feishu_group_manual",
                "feishu",
                "ou_person123",
                "2026-07-25T11:01:00+00:00",
            ),
        ],
    )
    connection.commit()
    connection.close()


def test_report_separates_historical_legacy_from_post_cutover_denominators(
    tmp_path: Path,
):
    path = tmp_path / "control.sqlite3"
    _db(path)
    report = build_report(
        control_db=path,
        enforce_after="2026-07-25T10:00:00+00:00",
    )
    assert report["ok"] is True
    assert report["actor_counts"] == {
        "human": 2,
        "automation": 1,
        "legacy_automation": 1,
        "unknown": 0,
    }
    assert report["denominators"] == {
        "human": 2,
        "automation": 1,
        "legacy_excluded": 1,
        "unknown_excluded": 0,
    }
    assert report["post_cutover_violation_count"] == 0
