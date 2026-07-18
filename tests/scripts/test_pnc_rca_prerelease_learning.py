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


def test_version_question_is_not_an_explicit_correction() -> None:
    comments = [
        _comment(
            "2026-07-02 20:58:25",
            "原因分析：内八场景判断导致BLM不可用。",
            "c1",
        ),
        _comment(
            "2026-07-03 10:00:00",
            "哪个版本优化这些是不是要写清楚呀",
            "c2",
        ),
    ]

    truth = learning.align_owner_truth(
        comments,
        [_transition("2026-07-02T12:59:36Z", "o93u2k3ri")],
    )

    assert truth["selection_reason"] != "later_explicit_owner_correction"
    assert truth["selected_comment"]["comment_ref"] == "c1"


def test_fix_status_alone_is_not_owner_root_cause() -> None:
    truth = learning.align_owner_truth(
        [_comment("2026-07-02 20:58:25", "已优化回灌，待复测", "c1")],
        [_transition("2026-07-02T12:59:36Z", "o93u2k3ri")],
        root_cause_text="规划目标选择错误",
    )

    assert truth["selection_reason"] == "root_cause_field_only"
    assert truth["selected_comment"] is None
    assert truth["root_cause_field_secondary"] == "规划目标选择错误"


def test_domain_triage_request_alone_is_not_owner_root_cause() -> None:
    truth = learning.align_owner_truth(
        [_comment("2026-07-02 20:58:25", "@规划 看一下 OOI", "c1")],
        [_transition("2026-07-02T12:59:36Z", "o93u2k3ri")],
        root_cause_text="OOI目标选择错误",
    )

    assert truth["selection_reason"] == "root_cause_field_only"
    assert truth["selected_comment"] is None


def test_query_status_scopes_the_project_by_pdcl_not_title(monkeypatch) -> None:
    observed = {}
    client = learning.MeegleReadClient()

    def fake_json(args):
        observed["args"] = args
        return {"data": {}, "list": [{"count": 0}]}

    monkeypatch.setattr(client, "_json", fake_json)

    rows, total = client.query_status("关闭（CLOSED）", offset=50)

    assert rows == []
    assert total == 0
    mql = observed["args"][observed["args"].index("--mql") + 1]
    assert "`问题数据地址_PDCL` is not null" in mql
    assert "`状态` = '关闭（CLOSED）'" in mql
    assert "名称` like '%G1Q3%'" not in mql
    assert "LIMIT 50,50" in mql


def test_query_target_index_keeps_non_g1q3_titles() -> None:
    class Client:
        def query_status(self, status_label, *, offset):
            if offset:
                return [], 1
            work_item_id = {
                label: str(7000000001 + index)
                for index, label in enumerate(learning.TARGET_STATUS_LABELS)
            }[status_label]
            return [{
                "work_item_id": work_item_id,
                "title": "FCW误触发，无G1Q3标题前缀",
                "status_key": "CLOSED",
                "status_label": status_label,
                "updated_at": "2026-07-18",
            }], 1

    rows = learning.query_target_index(Client())

    assert len(rows) == 3


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


def test_capture_corpus_resumes_completed_case_cache(
    tmp_path: Path, monkeypatch
) -> None:
    index = [
        {"work_item_id": "7000000002", "updated_at": "2026-07-18"},
        {"work_item_id": "7000000001", "updated_at": "2026-07-17"},
    ]
    attempts = {item["work_item_id"]: 0 for item in index}

    monkeypatch.setattr(learning, "query_target_index", lambda _client: index)

    def capture(_client, item):
        work_item_id = item["work_item_id"]
        attempts[work_item_id] += 1
        if work_item_id == "7000000001" and attempts[work_item_id] == 1:
            raise learning.CorpusError("transient_read")
        return (
            {"work_item_id": work_item_id, "title": "blind"},
            {"work_item_id": work_item_id, "owner_truth": {"status": "resolved"}},
        )

    monkeypatch.setattr(learning, "capture_case", capture)
    output_dir = tmp_path / "corpus"

    with pytest.raises(learning.CorpusError) as caught:
        learning.capture_corpus(output_dir, workers=1)
    assert caught.value.code == "corpus_capture_incomplete"

    manifest = learning.capture_corpus(output_dir, workers=1)

    assert manifest["case_count"] == 2
    assert attempts == {"7000000002": 1, "7000000001": 2}
    progress = json.loads(
        (tmp_path / ".corpus.capture-cache" / "progress.json").read_text()
    )
    assert progress["completed"] == 2
    assert progress["remaining"] == 0
    assert progress["finished"] is True
    blind = json.loads((output_dir / "blind-cases.json").read_text())
    assert [item["work_item_id"] for item in blind["cases"]] == [
        "7000000002",
        "7000000001",
    ]


def test_capture_corpus_resumes_frozen_index_without_live_requery(
    tmp_path: Path, monkeypatch
) -> None:
    index = [{"work_item_id": "7000000001", "updated_at": "2026-07-18"}]
    queries = 0

    def query(_client):
        nonlocal queries
        queries += 1
        return index

    monkeypatch.setattr(learning, "query_target_index", query)
    monkeypatch.setattr(
        learning,
        "capture_case",
        lambda _client, _item: (_ for _ in ()).throw(
            learning.CorpusError("transient_read")
        ),
    )
    output_dir = tmp_path / "corpus"

    with pytest.raises(learning.CorpusError) as caught:
        learning.capture_corpus(output_dir, workers=1)
    assert caught.value.code == "corpus_capture_incomplete"

    index[0] = {"work_item_id": "7000000001", "updated_at": "2026-07-19"}
    with pytest.raises(learning.CorpusError) as caught:
        learning.capture_corpus(output_dir, workers=1)
    assert caught.value.code == "corpus_capture_incomplete"
    assert queries == 1


def test_blind_result_batch_is_idempotent_and_rejects_conflict(tmp_path: Path) -> None:
    corpus_dir = tmp_path / "corpus"
    learning.write_corpus(
        corpus_dir,
        [{"work_item_id": "7000000001"}, {"work_item_id": "7000000002"}],
        [{"work_item_id": "7000000001"}, {"work_item_id": "7000000002"}],
    )
    batch = {
        "schema_version": learning.BLIND_RESULT_BATCH_SCHEMA_VERSION,
        "evaluator_version": "git-a8402aa35",
        "results": [
            {"work_item_id": "7000000001", "result": {"status": "completed"}},
            {"work_item_id": "7000000002", "result": {"status": "not_evaluable"}},
        ],
    }

    first = learning.append_blind_results_batch(corpus_dir, batch=batch)
    second = learning.append_blind_results_batch(corpus_dir, batch=batch)

    assert first == {
        "schema_version": learning.BLIND_RESULT_BATCH_SCHEMA_VERSION,
        "evaluator_version": "git-a8402aa35",
        "input_count": 2,
        "appended": 2,
        "reused": 0,
    }
    assert second["appended"] == 0
    assert second["reused"] == 2

    conflicting = json.loads(json.dumps(batch))
    conflicting["results"][0]["result"]["status"] = "blocked"
    with pytest.raises(learning.CorpusError) as caught:
        learning.append_blind_results_batch(corpus_dir, batch=conflicting)
    assert caught.value.code == "blind_result_conflict"


def test_prepare_blind_request_batch_uses_production_frame_parser(
    tmp_path: Path,
) -> None:
    corpus_dir = tmp_path / "corpus"
    learning.write_corpus(
        corpus_dir,
        [
            {
                "work_item_id": "7000000001",
                "title": "FCW",
                "frame_reference": "123",
                "remote_data_access_status": "valid",
                "remote_data_access": {"mode": "remote_read"},
            },
            {
                "work_item_id": "7000000002",
                "title": "LCC",
                "frame_reference": "20260708, 20:05:00",
                "remote_data_access_status": "valid",
                "remote_data_access": {"mode": "remote_read"},
            },
            {
                "work_item_id": "7000000003",
                "title": "ACC",
                "frame_reference": "bad frame",
                "remote_data_access_status": "remote_data_reference_invalid",
                "remote_data_access": None,
            },
        ],
        [
            {"work_item_id": "7000000001", "owner_truth": "secret-1"},
            {"work_item_id": "7000000002", "owner_truth": "secret-2"},
            {"work_item_id": "7000000003", "owner_truth": "secret-3"},
        ],
    )
    output_path = tmp_path / "blind-request-batch.json"

    result = learning.prepare_blind_request_batch(corpus_dir, output_path)
    payload = json.loads(output_path.read_text())

    assert result["case_count"] == 3
    assert payload["owner_truth_included"] is False
    assert "secret" not in output_path.read_text()
    assert payload["requests"][0]["frame_id"] == "123"
    assert payload["requests"][1]["frame_lookup"] == {
        "kind": "front_camera_timestamp",
        "source_field": "问题发生frame_id",
        "marker_time": "2026-07-08T20:05:00+08:00",
        "timezone": "Asia/Shanghai",
        "management_timestamp": 1783512300000000,
        "management_timestamp_unit": "microseconds_since_unix_epoch",
        "camera_scope": "front_view",
        "selection": "nearest_timestamp",
        "max_delta_us": 1000000,
        "topic_priority": [
            "front_120", "camera1", "front_190", "camera4", "front_30", "avm_front"
        ],
    }
    assert payload["requests"][2]["frame_reference_error"] == (
        "frame_reference_format_invalid"
    )


def test_refine_owner_truth_corpus_removes_weak_domain_only_comment(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    output = tmp_path / "refined"
    weak = _comment("2026-07-02 20:58:25", "@规划 看一下 OOI", "c1")
    weak["causal_score"] = 4
    learning.write_corpus(
        source,
        [{"work_item_id": "7000000001"}],
        [{
            "work_item_id": "7000000001",
            "owner_truth": {
                "status": "resolved",
                "selection_reason": "last_causal_comment_before_validation_transition",
                "validation_transition": _transition(
                    "2026-07-02T12:59:36Z", "o93u2k3ri"
                ),
                "selected_comment": weak,
                "causal_comment_timeline": [weak],
                "root_cause_field_secondary": "OOI目标选择错误",
            },
        }],
    )

    result = learning.refine_owner_truth_corpus(source, output)
    refined = json.loads((output / "owner-truth.json").read_text())

    assert result["case_count"] == 1
    assert refined["cases"][0]["owner_truth"]["selected_comment"] is None
    assert refined["cases"][0]["owner_truth"]["selection_reason"] == (
        "root_cause_field_only"
    )
