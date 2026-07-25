from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys

from scripts.pnc_rca_tier_recompute import main


SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "pnc_rca_tier_recompute.py"


def _candidate_contract():
    return {
        "consumer_capability": {
            "actual_evaluators": [
                {"evaluator_id": "lane_geometry_quality", "status": "supported"}
            ],
            "evidence": {
                "issue_frame_id": 12,
                "focus_window": {"start_ts": 0.0, "end_ts": 1.0},
                "field_lineage": {"fidelity_ok": True},
                "viz_lineage": {"ok": True, "status": "pass"},
            },
        },
        "report": {"candidate_owner_domain": "PERCEPTION_LANE"},
        "public_result": {
            "summary": {
                "short_conclusion": "车道线跳变导致车道保持不稳，候选方向需人工复核。"
            },
            "candidate": "PERCEPTION_LANE",
            "responsibility": {"status": "candidate"},
            "evidence_summary": {"refs": []},
            "causal_chain": {
                "narrative": [
                    {"role": "现象", "text": "车道保持不稳。"},
                    {"role": "证据", "text": "车道线横向跳变。"},
                    {"role": "因果判断", "text": "跳变经控制链传导。"},
                ]
            },
            "user_action": {},
        },
    }


def _honest_contract():
    return {
        "consumer_capability": {
            "actual_evaluators": [],
            "evidence": {},
        },
        "public_result": {
            "summary": {"short_conclusion": "自动RCA未归因：现有证据不足。"},
            "responsibility": {},
            "evidence_summary": {"refs": []},
            "causal_chain": {"narrative": []},
            "user_action": {},
        },
    }


def _build_db(path: Path):
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE business_triggers (
            work_item_id TEXT, generation INTEGER, created_at TEXT,
            submission_key TEXT, source_topic TEXT,
            source_partition INTEGER, source_offset INTEGER
        );
        CREATE TABLE rca_delivery_jobs (
            submission_key TEXT, delivery_id TEXT, status TEXT, outcome TEXT,
            terminal_state TEXT, terminal_error_code TEXT, report_url TEXT,
            contract_json TEXT
        );
        CREATE TABLE rca_delivery_effects (
            delivery_id TEXT, effect_kind TEXT, status TEXT, payload_json TEXT
        );
        """
    )
    rows = [
        ("7000000001", "submission-1", "delivery-1", _candidate_contract()),
        ("7000000002", "submission-2", "delivery-2", _honest_contract()),
    ]
    for index, (work_item_id, submission, delivery, contract) in enumerate(rows, 1):
        connection.execute(
            "INSERT INTO business_triggers VALUES (?, ?, ?, ?, '', NULL, NULL)",
            (work_item_id, index, f"2026-07-25T00:00:0{index}+00:00", submission),
        )
        connection.execute(
            "INSERT INTO rca_delivery_jobs VALUES (?, ?, 'delivered', 'success', '', '', ?, ?)",
            (
                submission,
                delivery,
                f"https://g1q3-rca.minieye.tech/{work_item_id}/index.html",
                json.dumps(contract, ensure_ascii=False),
            ),
        )
        effect = {
            "field_updates": [
                {
                    "field_key": "field_9193cb",
                    "field_value": (
                        "候选结论，需人工复核。"
                        if index == 1
                        else "自动RCA未归因：现有证据不足。"
                    ),
                }
            ],
            "comment_content": "历史 RCA 评论，待人工审批。",
        }
        connection.execute(
            "INSERT INTO rca_delivery_effects VALUES (?, 'feishu_issue_comment', 'succeeded', ?)",
            (delivery, json.dumps(effect, ensure_ascii=False)),
        )
    connection.commit()
    connection.close()


def _scope(path: Path):
    payload = {
        "tickets": [
            {
                "work_item_id": "7000000001",
                "quality_classification": "evidence_attribution",
                "approval_ready": True,
                "human_decision": "",
            },
            {
                "work_item_id": "7000000002",
                "quality_classification": "honest_non_attribution_insufficient_evidence",
                "approval_ready": True,
                "human_decision": "",
            },
        ]
    }
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def test_negative_injections_exit_nonzero_and_write_receipts(tmp_path):
    for scenario in ("supported_without_evaluator", "banned_phrase"):
        receipt = tmp_path / f"{scenario}.json"
        completed = subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--inject-negative",
                scenario,
                "--failure-receipt",
                str(receipt),
            ],
            text=True,
            capture_output=True,
            check=False,
        )

        assert completed.returncode == 2
        payload = json.loads(receipt.read_text(encoding="utf-8"))
        assert payload["blocked"] is True
        assert payload["exit_code"] == 2
        assert payload["expected_violation"] in payload["oracle"]["violations"]


def test_recompute_uses_read_only_db_and_requires_nonempty_rows(tmp_path):
    db = tmp_path / "control.sqlite3"
    scope = tmp_path / "scope.json"
    _build_db(db)
    _scope(scope)
    negative_receipts = []
    for scenario in ("supported_without_evaluator", "banned_phrase"):
        receipt = tmp_path / f"negative-{scenario}.json"
        assert (
            main([
                "--inject-negative",
                scenario,
                "--failure-receipt",
                str(receipt),
            ])
            == 2
        )
        negative_receipts.append(receipt)
    before = hashlib.sha256(db.read_bytes()).hexdigest()
    output_json = tmp_path / "migration.json"
    output_md = tmp_path / "migration.md"
    receipt = tmp_path / "receipt.json"

    rc = main([
        "--scope-ledger",
        str(scope),
        "--db",
        str(db),
        "--output-json",
        str(output_json),
        "--output-markdown",
        str(output_md),
        "--receipt",
        str(receipt),
        "--expected-count",
        "2",
        "--negative-receipt",
        str(negative_receipts[0]),
        "--negative-receipt",
        str(negative_receipts[1]),
    ])

    assert rc == 0
    assert hashlib.sha256(db.read_bytes()).hexdigest() == before
    migration = json.loads(output_json.read_text(encoding="utf-8"))
    assert migration["summary"]["total"] == 2
    assert migration["summary"]["previous_evidence_attribution"] == 1
    assert migration["summary"]["recomputed_supported_attribution"] == 0
    assert migration["summary"]["recomputed_candidate_hypothesis"] == 1
    assert migration["summary"]["recomputed_honest_non_attribution"] == 1
    assert migration["summary"]["recomputed_approval_ready"] == 0
    assert migration["summary"]["projected_blockers"] == {}
    proof = json.loads(receipt.read_text(encoding="utf-8"))
    assert proof["read_only"] is True
    assert proof["nonempty_validation"]["migration_row_count"] == 2
    assert proof["nonempty_validation"]["control_db_unchanged"] is True
    assert len(proof["negative_injections"]) == 2
