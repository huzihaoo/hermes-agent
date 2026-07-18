from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import pnc_rca_prerelease_learning as learning


def _comment(created_at: str, content: str, ref: str) -> dict:
    return {
        "comment_ref": ref,
        "created_at": created_at,
        "actor_hash": "a" * 16,
        "content": content,
        "content_sha256": learning.hashlib.sha256(content.encode()).hexdigest(),
    }


def _transition(occurred_at: str, new_key: str) -> dict:
    return {
        "occurred_at": occurred_at,
        "occurred_at_ms": 1,
        "old_status_key": "IN PROGRESS",
        "new_status_key": new_key,
        "new_status_label": learning.TARGET_STATUSES[new_key],
        "actor_hash": "b" * 16,
    }


def test_truth_prefers_last_causal_comment_before_validation_transition() -> None:
    comments = [
        _comment("2026-07-02 19:00:00", "先看一下数据", "c1"),
        _comment(
            "2026-07-02 20:58:25",
            "原因分析：模型漏检后重新检出，目标速度从0初始化，Vx收敛慢。",
            "c2",
        ),
    ]
    truth = learning.align_owner_truth(
        comments,
        [_transition("2026-07-02T12:59:36Z", "o93u2k3ri")],
    )

    assert truth["status"] == "resolved"
    assert truth["selection_reason"] == "last_causal_comment_before_validation_transition"
    assert truth["selected_comment"]["comment_ref"] == "c2"
    assert truth["validation_transition"]["new_status_label"] == "转验证（开发转测试）"


def test_later_explicit_correction_supersedes_validation_comment() -> None:
    comments = [
        _comment(
            "2026-07-02 20:58:25",
            "原因分析：OOI目标切换导致减速。",
            "c1",
        ),
        _comment(
            "2026-07-03 10:00:00",
            "更正：实际原因是感知目标Vx跳变，误触发AWB。",
            "c2",
        ),
    ]
    truth = learning.align_owner_truth(
        comments,
        [_transition("2026-07-02T12:59:36Z", "o93u2k3ri")],
    )

    assert truth["selection_reason"] == "later_explicit_owner_correction"
    assert truth["selected_comment"]["comment_ref"] == "c2"


def test_split_case_keeps_owner_material_out_of_blind_input() -> None:
    owner_text = "原因分析：目标速度从0初始化导致Vx收敛慢"
    root_cause = "最终根因：速度初始化错误"
    blind, truth = learning.split_case(
        {
            "work_item_id": "7035714261",
            "title": "G1Q3 FCW误触发",
            "status_key": "o93u2k3ri",
            "status_label": "转验证（开发转测试）",
            "updated_at": "2026-07-17",
        },
        {
            "description": "雨天直道行驶时误触发",
            "function_category": "FCW",
            "frame_reference": "7466",
            "pdcl_data": "",
            "root_cause_text": root_cause,
        },
        [_comment("2026-07-02 20:58:25", owner_text, "c1")],
        [_transition("2026-07-02T12:59:36Z", "o93u2k3ri")],
    )

    serialized = json.dumps(blind, ensure_ascii=False)
    assert owner_text not in serialized
    assert root_cause not in serialized
    assert truth["owner_truth"]["selected_comment"]["content"] == owner_text
    assert truth["owner_truth"]["root_cause_field_secondary"] == root_cause


def test_comment_normalization_removes_transient_file_urls() -> None:
    rows, pagination = learning.normalize_comments({
        "comments": [{
            "comment_id": 123,
            "created_at": "2026-07-02 20:58:25",
            "creator": "owner-1",
            "content": "原因分析：Vx异常。\n![](https://project.feishu.cn/goapi/v5/platform/file/stream/download/secret)",
        }],
        "pagination": {"page_num": 1, "total_pages": 1},
    })

    assert pagination == {"page_num": 1, "total_pages": 1}
    assert "secret" not in rows[0]["content"]
    assert "[image]" in rows[0]["content"]
    assert rows[0]["actor_hash"] != "owner-1"


def test_comment_normalization_accepts_explicit_empty_history() -> None:
    rows, pagination = learning.normalize_comments({
        "comments": [],
        "pagination": {"page_num": 1, "total_pages": 0},
    })

    assert rows == []
    assert pagination == {"page_num": 1, "total_pages": 0}


def test_ledger_requires_blind_result_before_reveal_and_is_write_once(tmp_path: Path) -> None:
    blind = [{"work_item_id": "7035714261", "title": "case"}]
    truth = [{"work_item_id": "7035714261", "owner_truth": {"status": "resolved"}}]
    learning.write_corpus(
        tmp_path,
        blind,
        truth,
        generated_at="2026-07-18T00:00:00Z",
    )

    with pytest.raises(learning.CorpusError, match="blind_result_required_before_truth"):
        learning.reveal_owner_truth(tmp_path, work_item_id="7035714261")

    first = learning.append_blind_result(
        tmp_path,
        work_item_id="7035714261",
        evaluator_version="7904680b",
        result={"status": "need_evidence"},
    )
    revealed = learning.reveal_owner_truth(tmp_path, work_item_id="7035714261")

    assert first["record_type"] == "blind_result"
    assert revealed == truth[0]
    records = learning._ledger_records(tmp_path / "learning-ledger.jsonl")
    assert [item["record_type"] for item in records] == [
        "header",
        "blind_result",
        "truth_reveal",
    ]
    learning.reveal_owner_truth(tmp_path, work_item_id="7035714261")
    assert len(learning._ledger_records(tmp_path / "learning-ledger.jsonl")) == 3
    with pytest.raises(learning.CorpusError, match="blind_result_already_recorded"):
        learning.append_blind_result(
            tmp_path,
            work_item_id="7035714261",
            evaluator_version="new-version",
            result={"status": "hypothesis_ready"},
        )


def test_post_reveal_regression_is_versioned_and_requires_reveal(tmp_path: Path) -> None:
    blind = [{"work_item_id": "7035714261", "title": "case"}]
    truth = [{"work_item_id": "7035714261", "owner_truth": {"status": "resolved"}}]
    learning.write_corpus(
        tmp_path,
        blind,
        truth,
        generated_at="2026-07-18T00:00:00Z",
    )
    learning.append_blind_result(
        tmp_path,
        work_item_id="7035714261",
        evaluator_version="baseline",
        result={"status": "need_evidence"},
    )

    with pytest.raises(
        learning.CorpusError, match="truth_reveal_required_before_regression"
    ):
        learning.append_post_reveal_regression(
            tmp_path,
            work_item_id="7035714261",
            evaluator_version="candidate-v1",
            result={"status": "hypothesis_ready"},
        )

    learning.reveal_owner_truth(tmp_path, work_item_id="7035714261")
    regression = learning.append_post_reveal_regression(
        tmp_path,
        work_item_id="7035714261",
        evaluator_version="candidate-v1",
        result={"status": "hypothesis_ready"},
    )

    assert regression["record_type"] == "post_reveal_regression"
    with pytest.raises(learning.CorpusError, match="regression_result_already_recorded"):
        learning.append_post_reveal_regression(
            tmp_path,
            work_item_id="7035714261",
            evaluator_version="candidate-v1",
            result={"status": "changed"},
        )


def test_write_corpus_refuses_identity_order_mismatch(tmp_path: Path) -> None:
    with pytest.raises(learning.CorpusError, match="corpus_identity_order_mismatch"):
        learning.write_corpus(
            tmp_path,
            [{"work_item_id": "1"}],
            [{"work_item_id": "2"}],
        )
