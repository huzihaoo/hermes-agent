from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from scripts import pnc_rca_w6_guard_audit as audit


CUTOFF = "2026-07-25T10:15:43.473251+00:00"
STOCK_ITEM = "7000000001"
NEW_ITEM = "7000000002"
STOCK_BUSINESS = "business-stock"
NEW_BUSINESS = "business-new"
OPEN_ID = "ou_0123456789abcdef0123456789abcdef"


def _schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE business_triggers (
            business_key TEXT NOT NULL,
            generation INTEGER NOT NULL,
            work_item_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (business_key, generation)
        );
        CREATE TABLE rca_trigger_sources (
            source_id TEXT PRIMARY KEY,
            source_kind TEXT NOT NULL,
            mode TEXT NOT NULL,
            requester_id TEXT NOT NULL,
            chat_id TEXT NOT NULL,
            thread_id TEXT NOT NULL,
            message_id TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE rca_trigger_bindings (
            source_id TEXT NOT NULL,
            business_key TEXT NOT NULL,
            generation INTEGER NOT NULL
        );
        CREATE TABLE rca_delivery_jobs (
            delivery_id TEXT PRIMARY KEY,
            business_key TEXT NOT NULL,
            generation INTEGER NOT NULL,
            work_item_id TEXT NOT NULL,
            target_key TEXT NOT NULL
        );
        CREATE TABLE rca_delivery_effects (
            effect_key TEXT PRIMARY KEY,
            delivery_id TEXT NOT NULL,
            effect_kind TEXT NOT NULL,
            target_key TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            status TEXT NOT NULL,
            write_phase TEXT NOT NULL,
            write_started_at TEXT,
            remote_receipt_json TEXT,
            created_at TEXT NOT NULL
        );
        CREATE TABLE rca_delivery_subscriptions (
            business_key TEXT NOT NULL,
            generation INTEGER NOT NULL,
            effect_kind TEXT NOT NULL
        );
        CREATE TABLE rca_learning_lane_admissions (
            business_key TEXT NOT NULL,
            generation INTEGER NOT NULL,
            work_item_id TEXT NOT NULL,
            schema_version TEXT NOT NULL,
            lane TEXT NOT NULL,
            reason TEXT NOT NULL,
            external_write_allowed INTEGER NOT NULL,
            admitted_at TEXT NOT NULL,
            PRIMARY KEY (business_key, generation)
        );
        CREATE TABLE rca_conclusion_adjudications (
            adjudication_id TEXT PRIMARY KEY,
            business_key TEXT NOT NULL,
            generation INTEGER NOT NULL,
            work_item_id TEXT NOT NULL,
            action TEXT NOT NULL,
            conclusion_state TEXT NOT NULL,
            reason TEXT NOT NULL,
            replacement_conclusion TEXT NOT NULL,
            actor_id TEXT NOT NULL,
            original_effect_key TEXT NOT NULL,
            correction_effect_key TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        """
    )


def _trigger(
    conn: sqlite3.Connection,
    *,
    source_id: str,
    business_key: str,
    generation: int,
    work_item_id: str,
    created_at: str,
    rerun: bool = False,
) -> None:
    conn.execute(
        "INSERT INTO business_triggers VALUES (?, ?, ?, ?)",
        (business_key, generation, work_item_id, created_at),
    )
    conn.execute(
        "INSERT INTO rca_trigger_sources VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            source_id,
            "feishu_group_manual" if rerun else "kafka_workflow_event",
            "rerun" if rerun else "issue_created",
            OPEN_ID if rerun else "",
            "chat-1" if rerun else "",
            "thread-1" if rerun else "",
            f"message-{source_id}" if rerun else "",
            created_at,
        ),
    )
    conn.execute(
        "INSERT INTO rca_trigger_bindings VALUES (?, ?, ?)",
        (source_id, business_key, generation),
    )


def _effect(
    conn: sqlite3.Connection,
    *,
    effect_key: str,
    delivery_id: str,
    business_key: str,
    generation: int,
    work_item_id: str,
    created_at: str,
    conclusion: str = "",
    adjudication: bool = False,
) -> None:
    job_target = f"issue:{work_item_id}"
    conn.execute(
        "INSERT OR IGNORE INTO rca_delivery_jobs VALUES (?, ?, ?, ?, ?)",
        (delivery_id, business_key, generation, work_item_id, job_target),
    )
    payload = {
        "schema_version": (
            "pnc_rca_conclusion_adjudication_effect_v2"
            if adjudication
            else "pnc_rca_delivery_effect_v2"
        )
    }
    if conclusion:
        payload["conclusion"] = conclusion
    target = (
        f"{audit.ADJUDICATION_TARGET_PREFIX}{effect_key}"
        if adjudication
        else job_target
    )
    conn.execute(
        """
        INSERT INTO rca_delivery_effects(
            effect_key, delivery_id, effect_kind, target_key, payload_json,
            status, write_phase, write_started_at, remote_receipt_json, created_at
        ) VALUES (?, ?, 'feishu_issue_comment', ?, ?, 'succeeded', 'settled', ?, '{}', ?)
        """,
        (
            effect_key,
            delivery_id,
            target,
            json.dumps(payload, separators=(",", ":"), sort_keys=True),
            created_at,
            created_at,
        ),
    )


def _database(tmp_path: Path) -> Path:
    path = tmp_path / "control.sqlite3"
    with sqlite3.connect(path) as conn:
        _schema(conn)
        _trigger(
            conn,
            source_id="stock-initial",
            business_key=STOCK_BUSINESS,
            generation=1,
            work_item_id=STOCK_ITEM,
            created_at="2026-07-25T10:00:00+00:00",
        )
        _effect(
            conn,
            effect_key="stock-effect-1",
            delivery_id="stock-delivery-1",
            business_key=STOCK_BUSINESS,
            generation=1,
            work_item_id=STOCK_ITEM,
            created_at="2026-07-25T10:05:00+00:00",
            conclusion="stock conclusion",
        )
        _trigger(
            conn,
            source_id="stock-rerun",
            business_key=STOCK_BUSINESS,
            generation=2,
            work_item_id=STOCK_ITEM,
            created_at="2026-07-25T11:00:00+00:00",
            rerun=True,
        )
        conn.execute(
            "INSERT INTO rca_learning_lane_admissions VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                STOCK_BUSINESS,
                2,
                STOCK_ITEM,
                audit.LEARNING_ADMISSION_SCHEMA_VERSION,
                "learning",
                "stock",
                0,
                "2026-07-25T11:00:01+00:00",
            ),
        )

        _trigger(
            conn,
            source_id="new-initial",
            business_key=NEW_BUSINESS,
            generation=1,
            work_item_id=NEW_ITEM,
            created_at="2026-07-25T11:05:00+00:00",
        )
        _effect(
            conn,
            effect_key="new-effect-a",
            delivery_id="new-delivery-1",
            business_key=NEW_BUSINESS,
            generation=1,
            work_item_id=NEW_ITEM,
            created_at="2026-07-25T11:06:00+00:00",
            conclusion="conclusion A",
        )
        conn.execute(
            "INSERT INTO rca_conclusion_adjudications VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "adjudication-a-b",
                NEW_BUSINESS,
                1,
                NEW_ITEM,
                "retract",
                "invalidated",
                "new evidence invalidated the earlier conclusion",
                "conclusion B",
                OPEN_ID,
                "new-effect-a",
                "internal-ledger-only",
                "2026-07-25T11:30:00+00:00",
            ),
        )
        _trigger(
            conn,
            source_id="new-rerun",
            business_key=NEW_BUSINESS,
            generation=2,
            work_item_id=NEW_ITEM,
            created_at="2026-07-25T11:20:00+00:00",
            rerun=True,
        )
        _effect(
            conn,
            effect_key="new-effect-b",
            delivery_id="new-delivery-2",
            business_key=NEW_BUSINESS,
            generation=2,
            work_item_id=NEW_ITEM,
            created_at="2026-07-25T11:21:00+00:00",
            conclusion="conclusion B",
        )
    return path


def _audit(path: Path) -> dict:
    return audit.audit_w6_guard(
        path,
        stock_cutoff=CUTOFF,
        expected_stock_count=1,
    )


def _inject_learning_effect(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        _effect(
            conn,
            effect_key="stock-effect-injected",
            delivery_id="stock-delivery-2",
            business_key=STOCK_BUSINESS,
            generation=2,
            work_item_id=STOCK_ITEM,
            created_at="2026-07-25T11:01:00+00:00",
            conclusion="must never publish",
        )


@pytest.mark.parametrize(
    "schema_version", sorted(audit.TERMINAL_EFFECT_SCHEMA_VERSIONS)
)
def test_terminal_delivery_versions_have_stable_result_identity(
    schema_version: str,
) -> None:
    identity, replacement, kind = audit._result_identity({
        "schema_version": schema_version,
        "terminal_state": "failed",
        "outcome": "terminal_failed",
        "error_code": "vm_terminal_failed_unclassified",
    })

    assert kind == "terminal"
    assert identity.startswith("terminal:{")
    assert replacement == (
        "terminal:failed:terminal_failed:vm_terminal_failed_unclassified"
    )


def test_clean_snapshot_satisfies_offline_w6_guard(tmp_path: Path) -> None:
    path = _database(tmp_path)
    before = path.read_bytes()

    result = _audit(path)

    assert result["ok"] is True
    assert result["external_writes"] is False
    assert result["production_actions_performed"] is False
    assert result["scope"]["observed_stock_count"] == 1
    assert result["counts"]["valid_learning_admissions"] == 1
    assert result["counts"]["conclusion_flip_items"] == 1
    assert result["counts"]["conclusion_transitions"] == 1
    assert result["counts"]["adjudicated_transitions"] == 1
    assert result["legacy_batch"]["termination_verified"] is False
    assert path.read_bytes() == before


def test_learning_lane_rejects_any_feishu_effect(tmp_path: Path) -> None:
    path = _database(tmp_path)
    _inject_learning_effect(path)

    result = _audit(path)

    assert result["ok"] is False
    assert result["counts"]["learning_feishu_effects"] == 1
    assert result["counts"]["post_cutoff_stock_feishu_effects"] == 1
    assert result["invariants"]["learning_lane_no_feishu_effects"] is False


def test_automation_rerun_does_not_increase_comment_budget(tmp_path: Path) -> None:
    path = _database(tmp_path)
    with sqlite3.connect(path) as conn:
        conn.execute(
            "UPDATE rca_trigger_sources SET requester_id = 'automation:legacy' "
            "WHERE source_id = 'new-rerun'"
        )

    result = _audit(path)

    assert result["ok"] is False
    assert result["counts"]["generation_origin_violations"] == 1
    assert result["counts"]["comment_budget_violations"] == 1
    assert result["invariants"]["generation_only_on_explicit_user_rerun"] is False


def test_conclusion_flip_requires_explicit_replacement_and_basis(
    tmp_path: Path,
) -> None:
    path = _database(tmp_path)
    with sqlite3.connect(path) as conn:
        conn.execute("DELETE FROM rca_conclusion_adjudications")

    result = _audit(path)

    assert result["ok"] is False
    assert result["counts"]["conclusion_transitions"] == 1
    assert result["counts"]["missing_transition_adjudications"] == 1
    missing = result["violations"]["missing_transition_adjudications"][0]
    assert missing["from_effect_key"] == "new-effect-a"
    assert missing["to_effect_key"] == "new-effect-b"


def test_missing_learning_ledger_fails_closed(tmp_path: Path) -> None:
    path = _database(tmp_path)
    with sqlite3.connect(path) as conn:
        conn.execute("DROP TABLE rca_learning_lane_admissions")

    result = _audit(path)

    assert result["ok"] is False
    assert result["schema_errors"]["learning_lane"] == [
        "learning_admission_table_missing"
    ]
    assert result["invariants"]["learning_lane_schema_ready"] is False


def test_nonempty_wal_is_rejected_before_audit(tmp_path: Path) -> None:
    path = _database(tmp_path)
    Path(f"{path}-wal").write_bytes(b"not-checkpointed")

    with pytest.raises(RuntimeError, match="checkpointed"):
        _audit(path)


def test_cli_negative_injection_exits_nonzero(tmp_path: Path) -> None:
    path = _database(tmp_path)
    _inject_learning_effect(path)

    completed = subprocess.run(
        [
            sys.executable,
            str(Path(audit.__file__).resolve()),
            "--control-db",
            str(path),
            "--stock-cutoff",
            CUTOFF,
            "--expected-stock-count",
            "1",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 2
    payload = json.loads(completed.stdout)
    assert payload["ok"] is False
    assert payload["counts"]["learning_feishu_effects"] == 1
    assert payload["invariants"]["learning_lane_no_feishu_effects"] is False
