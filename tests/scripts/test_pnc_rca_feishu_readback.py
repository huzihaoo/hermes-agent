from __future__ import annotations

import json
from pathlib import Path
import subprocess

from scripts import pnc_rca_feishu_readback as readback


def _completed(value, *, returncode=0):
    return subprocess.CompletedProcess(
        args=[],
        returncode=returncode,
        stdout=json.dumps(value, ensure_ascii=False),
        stderr="",
    )


def test_comment_extract_is_privacy_safe_and_parses_exact_markers():
    effect = "g1q3-rca-effect-v1-" + "a" * 64
    terminal = "g1q3-rca-terminal-effect-v1-" + "b" * 64
    raw_content = (
        "private body\n"
        f"[RCA_DELIVERY:{effect}:0123456789ab]\n"
        f"[RCA_TERMINAL:{terminal}:terminal_failed:12]\n"
        f"[RCA_ATTRIBUTION:v286-v1:{'c' * 64}:{effect}]"
    )

    result = readback.extract_comment(
        {"comment_id": "123", "created_at": "now", "content": raw_content}
    )

    assert result["delivery_markers"] == [
        {"effect_key": effect, "artifact_suffix": "0123456789ab"}
    ]
    assert result["terminal_markers"] == [
        {"effect_key": terminal, "outcome": "terminal_failed", "generation": 12}
    ]
    assert result["attribution_markers"] == [
        {
            "version": "v286-v1",
            "contract_sha256": "c" * 64,
            "effect_key": effect,
        }
    ]
    assert raw_content not in json.dumps(result)


def test_fetch_comments_requires_complete_stable_pagination():
    effect = "g1q3-rca-effect-v1-" + "d" * 64
    responses = {
        "1": {
            "comments": [{"comment_id": "1", "content": f"RCA_DELIVERY:{effect}:123456789abc"}],
            "pagination": {"page_num": 1, "page_size": 1, "total": 2, "total_pages": 2},
        },
        "2": {
            "comments": [{"comment_id": "2", "content": "ordinary"}],
            "pagination": {"page_num": 2, "page_size": 1, "total": 2, "total_pages": 2},
        },
    }

    def runner(command):
        return _completed(responses[command[command.index("--page-num") + 1]])

    result = readback.fetch_comments(
        "7000000001",
        meegle=Path("/meegle"),
        project_key="project",
        runner=runner,
    )

    assert result["comments"]["complete"] is True
    assert result["comments"]["returned"] == 2
    assert result["delivery_effect_keys"] == [effect]
    assert len(result["comment_extracts"]) == 1


def test_fetch_comments_accepts_the_official_zero_comment_shape():
    result = readback.fetch_comments(
        "7000000001",
        meegle=Path("/meegle"),
        project_key="project",
        runner=lambda _command: _completed({
            "comments": [],
            "pagination": {
                "page_num": 1,
                "page_size": 20,
                "total": 0,
                "total_pages": 0,
            },
        }),
    )

    assert result["comments"]["complete"] is True
    assert result["comments"]["total"] == 0
    assert result["comment_extracts"] == []


def test_fetch_work_items_hashes_values_without_persisting_them():
    private_report = "https://private.invalid/report"
    payload = {
        "errors": [],
        "results": [
            {
                "work_item_id": 7000000001,
                "data": {
                    "work_item_attribute": {"work_item_id": "7000000001"},
                    "work_item_fields": [
                        {
                            "key": readback.PROJECT_FIELD_KEY,
                            "value": [{"id": int(readback.EXPECTED_PROJECT_OPTION_ID), "name": "G1Q3"}],
                        },
                        {"key": readback.REPORT_FIELD_KEY, "value": private_report},
                        {"key": readback.RESULT_FIELD_KEY, "value": "private result"},
                    ],
                },
            }
        ],
        "summary": {"total": 1, "succeeded": 1, "failed": 0},
    }

    projected, receipts = readback.fetch_work_items(
        ["7000000001"],
        meegle=Path("/meegle"),
        project_key="project",
        runner=lambda _command: _completed(payload),
    )

    assert projected["7000000001"]["routing"]["exact"] is True
    assert projected["7000000001"]["report_field"]["nonempty"] is True
    assert private_report not in json.dumps(projected)
    assert receipts[0]["returned"] == 1
