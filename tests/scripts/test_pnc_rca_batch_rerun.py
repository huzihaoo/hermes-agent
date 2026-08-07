import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from gateway.pnc_rca_issue_focus import ANALYSIS_INSUFFICIENT_STATEMENT
from scripts import pnc_rca_batch_rerun as batch_rerun
from scripts.pnc_rca_batch_rerun import (
    OWNER_RECEIPT_EFFECT_SCOPE,
    OWNER_RECEIPT_NO_OTHER_TASK_BOUNDARY,
    OWNER_RECEIPT_SCHEMA_VERSION,
    QUEUE_AUTHORITY_FLAGS,
    QUEUE_SCHEMA_VERSION,
    QUEUE_SCOPE,
    SCHEMA_VERSION,
    BatchRerunError,
    _approval,
    _batch_terminal_authority,
    _issue_snapshot,
    _load_queue,
    _load_or_create_state,
    _owner_receipt_binding,
    _queue_precondition_matches,
    _request,
    _silent_terminal_authority,
    _terminal_failure,
    _write_state,
)


def _owner_receipt(
    *,
    batch_id="gray-20260724",
    queue_sha256="1" * 64,
    selected_issue_ids=None,
    requester_id="automation:rca-batch-rerun",
    **changes,
):
    value = {
        "schema_version": OWNER_RECEIPT_SCHEMA_VERSION,
        "approved": True,
        "batch_id": batch_id,
        "queue_sha256": queue_sha256,
        "selected_issue_ids": sorted(selected_issue_ids or ["7048803418"]),
        "production_effects": dict(OWNER_RECEIPT_EFFECT_SCOPE),
        "no_other_task_boundary": dict(OWNER_RECEIPT_NO_OTHER_TASK_BOUNDARY),
        "approved_by": "owner:胡子豪",
        "approved_at": datetime.now(timezone.utc).isoformat(),
        "requester_id": requester_id,
        "reason": f"production_gray_batch:{batch_id}",
        "activation_required": True,
        "runtime_commit": "a" * 40,
        "runtime_tree": "b" * 40,
    }
    value.update(changes)
    return value


def _write_owner_receipt(path, value=None):
    value = value or _owner_receipt()
    path.write_bytes(
        (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()
    )
    path.chmod(0o600)
    return path


def _snapshot(
    *,
    job_status="delivered",
    job_outcome="success",
    effect_status="succeeded",
    causal=True,
):
    contract = (
        {
            "report": {
                "candidate_owner": "ACC decoded 证据",
                "diagnostic_only": False,
            },
            "artifacts": {
                "attribution_causal_text": "ACC 退出判据命中，指向状态机抑制标志。"
            },
            "public_result": {
                "summary": {"status": "candidate"},
                "responsibility": {"status": "candidate"},
                "terminal_diagnostic": {},
            },
        }
        if causal
        else {
            "report": {"candidate_owner": "", "diagnostic_only": True},
            "artifacts": {"attribution_causal_text": ""},
            "public_result": {
                "summary": {"status": "diagnostic_report_ready"},
                "terminal_diagnostic": {"blocker_kind": "remote_event_not_found"},
            },
        }
    )
    return {
        "generation": 6,
        "submission_key": "submission-6",
        "delivery_id": "delivery-6",
        "job_status": job_status,
        "job_outcome": job_outcome,
        "outcome_key": "decoded_no_supported_causal_chain",
        "terminal_state": "",
        "terminal_error_code": "",
        "issue_url": "https://project.feishu.cn/t03o4q/issue/detail/7048803418",
        "report_url": "https://g1q3-rca.minieye.tech/report.html",
        "manifest_json": "{}",
        "contract_json": json.dumps(contract),
        "artifacts_json": "{}",
        "effects": [
            {
                "effect_kind": "feishu_issue_comment",
                "required": 1,
                "status": effect_status,
                "remote_receipt": {
                    "remote_id": "7665000000000000000",
                    "confirmed_field_keys": ["field_9193cb", "field_8c912e"],
                    "source": "read_after_write",
                },
                "completed_at": "2026-07-24T06:00:00+00:00",
                "last_error_code": "",
            }
        ],
    }


def test_approval_accepts_issue_only_official_readback():
    approval = _approval(_snapshot())

    assert approval is not None
    assert approval["generation"] == 6
    assert approval["official_comment_id"] == "7665000000000000000"
    assert approval["official_field_keys"] == ["field_8c912e", "field_9193cb"]
    assert approval["quality"]["status"] == "causal_candidate"
    assert approval["quality"]["responsibility"] == "ACC decoded 证据"


def test_approval_rejects_delivered_noncausal_result():
    snapshot = _snapshot(causal=False)

    assert _approval(snapshot) is None
    failure = _terminal_failure(snapshot)
    assert failure is not None
    assert failure["job_status"] == "delivered"


def test_approval_accepts_decoded_data_binding_conflict_as_a_cause():
    snapshot = _snapshot()
    contract = json.loads(snapshot["contract_json"])
    contract["report"]["candidate_owner"] = "问题数据/回灌链路"
    contract["artifacts"]["attribution_causal_text"] = (
        "问题描述目标与绑定数据不一致，责任指向问题数据/回灌链路。"
    )
    contract["public_result"]["summary"]["status"] = "blocked"
    contract["public_result"]["responsibility"]["status"] = (
        "candidate_data_integrity_conflict"
    )
    snapshot["contract_json"] = json.dumps(contract)

    approval = _approval(snapshot)

    assert approval is not None
    assert approval["quality"]["responsibility"] == "问题数据/回灌链路"


def test_approval_accepts_explicit_focus_stop_after_official_field_readback():
    from tests.gateway.test_pnc_rca_delivery_contract import _focus_payload

    snapshot = _snapshot(causal=False)
    snapshot["contract_json"] = json.dumps({
        "issue_focus": _focus_payload(
            "HMI-S弯",
            status=ANALYSIS_INSUFFICIENT_STATEMENT,
        )
    })

    approval = _approval(snapshot, issue_title="HMI-S弯")

    assert approval is not None
    assert approval["quality"] == {
        "status": "explicit_focus_stop",
        "analysis_status": ANALYSIS_INSUFFICIENT_STATEMENT,
        "responsibility": "暂无法判断",
        "causal_text_sha256": "",
    }


def test_approval_waits_for_required_effect_and_surfaces_terminal_failure():
    pending = _snapshot(job_status="partial", effect_status="retry_wait")
    failed = _snapshot(job_status="partial", job_outcome="terminal_failed")

    assert _approval(pending) is None
    assert _terminal_failure(pending) is None
    failure = _terminal_failure(failed)
    assert failure is not None
    assert failure["job_status"] == "partial"


def test_batch_request_is_operator_issue_only_and_deterministic():
    request = _request(
        batch_id="gray-20260724",
        issue_id="7048803418",
        request_index=1,
        requester_id="automation:rca-batch-rerun",
    )

    assert request.platform == "operator"
    assert request.chat_id == request.thread_id == ""
    assert request.message_id == "gray-20260724-7048803418-try-1"
    assert request.requester_id == "automation:rca-batch-rerun"


def test_silent_terminal_authority_requires_exact_deadline_no_delivery(tmp_path):
    batch_id = "gray-20260724"
    reason = f"production_gray_batch:{batch_id}"
    snapshot = {
        **_snapshot(),
        "generation": 1,
        "submission_key": "g1q3-rca-s1-" + "a" * 64,
        "watch_state": "terminal_failed",
        "watch_delivery_id": None,
        "watch_error_code": "rca_work_deadline_exceeded",
    }
    authority = _silent_terminal_authority(
        snapshot=snapshot,
        batch_id=batch_id,
        queue_sha256="1" * 64,
        issue_id="7048803418",
        owner_receipt_path=str(tmp_path / "owner.json"),
        owner_receipt_sha256="2" * 64,
        requester_id="automation:rca-batch-rerun",
        reason=reason,
    )

    assert authority is not None
    assert authority["prior_submission_key"] == snapshot["submission_key"]
    assert authority["owner_receipt_path"] == str(tmp_path / "owner.json")
    for changes in (
        {"watch_state": "delivery_created"},
        {"watch_delivery_id": "delivery-1"},
        {"watch_error_code": "vm_status_missing"},
    ):
        assert _silent_terminal_authority(
            snapshot={**snapshot, **changes},
            batch_id=batch_id,
            queue_sha256="1" * 64,
            issue_id="7048803418",
            owner_receipt_path=str(tmp_path / "owner.json"),
            owner_receipt_sha256="2" * 64,
            requester_id="automation:rca-batch-rerun",
            reason=reason,
        ) is None


def test_batch_terminal_authority_accepts_any_settled_owner_approved_delivery(
    tmp_path,
):
    base = {
        **_snapshot(),
        "submission_key": "g1q3-rca-s1-" + "a" * 64,
        "watch_state": "delivery_created",
        "watch_delivery_id": "delivery-6",
        "terminal_error_code": "",
    }
    authority = _batch_terminal_authority(
        snapshot=base,
        batch_id="gray-20260724",
        queue_sha256="1" * 64,
        issue_id="7048803418",
        owner_receipt_path=str(tmp_path / "owner.json"),
        owner_receipt_sha256="2" * 64,
        requester_id="automation:rca-batch-rerun",
        reason="production_gray_batch:gray-20260724",
    )
    assert authority is not None
    assert authority["terminal_mode"] == "settled_delivery_correction"
    assert _batch_terminal_authority(
        snapshot={
            **base,
            "effects": [{**base["effects"][0], "status": "retry_wait"}],
        },
        batch_id="gray-20260724",
        queue_sha256="1" * 64,
        issue_id="7048803418",
        owner_receipt_path=str(tmp_path / "owner.json"),
        owner_receipt_sha256="2" * 64,
        requester_id="automation:rca-batch-rerun",
        reason="production_gray_batch:gray-20260724",
    ) is None


def test_owner_receipt_binding_hashes_exact_owner_only_file(tmp_path):
    receipt = tmp_path / "owner-receipt.json"
    _write_owner_receipt(receipt)
    raw = receipt.read_bytes()

    path, sha256 = _owner_receipt_binding(
        receipt,
        expected_batch_id="gray-20260724",
        expected_queue_sha256="1" * 64,
        expected_issue_ids=["7048803418"],
        expected_requester_id="automation:rca-batch-rerun",
        expected_runtime_commit="a" * 40,
        expected_runtime_tree="b" * 40,
    )

    assert path == str(receipt)
    assert sha256 == hashlib.sha256(raw).hexdigest()

    receipt.chmod(0o644)
    with pytest.raises(BatchRerunError, match="batch_owner_receipt_identity_invalid"):
        _owner_receipt_binding(receipt)


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("approved", False, "batch_owner_receipt_not_approved"),
        ("selected_issue_ids", ["7048803419"], "batch_owner_receipt_selection_mismatch"),
        ("production_effects", {"other_task": True}, "batch_owner_receipt_effect_scope_invalid"),
        (
            "no_other_task_boundary",
            {"mode": "shared"},
            "batch_owner_receipt_task_boundary_invalid",
        ),
        ("activation_required", False, "batch_owner_receipt_activation_required"),
        ("runtime_commit", "0" * 40, "batch_owner_receipt_runtime_invalid"),
    ],
)
def test_owner_receipt_semantics_fail_closed(tmp_path, field, value, error):
    receipt = tmp_path / f"owner-{field}.json"
    _write_owner_receipt(receipt, _owner_receipt(**{field: value}))
    with pytest.raises(BatchRerunError, match=error):
        _owner_receipt_binding(
            receipt,
            expected_batch_id="gray-20260724",
            expected_queue_sha256="1" * 64,
            expected_issue_ids=["7048803418"],
            expected_requester_id="automation:rca-batch-rerun",
        )


def test_owner_receipt_rejects_noncanonical_and_minimal_marker(tmp_path):
    receipt = tmp_path / "owner-invalid.json"
    receipt.write_bytes(b'{"approved":true}\n')
    receipt.chmod(0o600)
    with pytest.raises(BatchRerunError, match="batch_owner_receipt_schema_invalid"):
        _owner_receipt_binding(receipt)

    valid = _owner_receipt()
    receipt.write_bytes(
        (json.dumps(valid, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()
    )
    receipt.chmod(0o600)
    with pytest.raises(BatchRerunError, match="batch_owner_receipt_noncanonical"):
        _owner_receipt_binding(receipt)


def test_batch_state_v4_binds_owner_receipt_path_hash_selection_and_gate(tmp_path):
    state_path = tmp_path / "batch-state.json"
    owner_path = str(tmp_path / "owner-receipt.json")
    values = {
        "batch_id": "gray-20260724",
        "queue_sha256": "1" * 64,
        "runtime_commit": "2" * 40,
        "runtime_tree": "4" * 40,
        "owner_receipt_path": owner_path,
        "owner_receipt_sha256": "3" * 64,
        "selected_issue_ids": ["7048803418"],
    }
    state = _load_or_create_state(state_path, **values)
    _write_state(state_path, state)

    assert state["schema_version"] == SCHEMA_VERSION
    assert state["owner_receipt_path"] == owner_path
    assert state["owner_receipt_sha256"] == "3" * 64
    assert state["selected_issue_ids"] == ["7048803418"]
    assert state["activation_required"] is True
    assert state["runtime_tree"] == "4" * 40
    with pytest.raises(BatchRerunError, match="batch_state_binding_mismatch"):
        _load_or_create_state(
            state_path,
            **{**values, "owner_receipt_sha256": "4" * 64},
        )
    with pytest.raises(BatchRerunError, match="batch_state_binding_mismatch"):
        _load_or_create_state(
            state_path,
            **{**values, "runtime_commit": "5" * 40},
        )


def _queue_value(*, current_submission_key=None, current_generation=1):
    issue_id = "7048803418"
    submission_key = (
        "g1q3-rca-s1-" + "a" * 64
        if current_submission_key is None
        else current_submission_key
    )
    return {
        "schema_version": QUEUE_SCHEMA_VERSION,
        "batch_id": "gray-20260724",
        "project_key": "t03o4q",
        "scope": {
            **QUEUE_SCOPE,
            "issue_count": 1,
            "issue_ids_sha256": hashlib.sha256(
                f"{issue_id}\n".encode("utf-8")
            ).hexdigest(),
        },
        "source_inventory_sha256": "c" * 64,
        "authority_flags": dict(QUEUE_AUTHORITY_FLAGS),
        "items": [
            {
                "issue_id": issue_id,
                "title": "ACC braking issue",
                "quality_classification": "missing",
                "current_submission_key": submission_key,
                "current_generation": current_generation,
                "priority": 1,
                "project_key": "t03o4q",
            }
        ],
    }


def test_queue_schema_binds_exact_and_scope_and_control_precondition(tmp_path):
    queue_path = tmp_path / "queue.json"
    value = _queue_value()
    queue_path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")
    items, digest = _load_queue(queue_path, expected_batch_id="gray-20260724")
    assert items[0]["queue_submission_key"].startswith("g1q3-rca-s1-")
    assert items[0]["queue_generation"] == 1
    assert len(digest) == 64
    for field, replacement in (
        ("schema_version", "wrong"),
        ("batch_id", "other-batch"),
        ("project_key", "other-project"),
    ):
        bad = dict(value)
        bad[field] = replacement
        queue_path.write_text(json.dumps(bad, sort_keys=True), encoding="utf-8")
        with pytest.raises(BatchRerunError, match="batch_queue_schema_invalid"):
            _load_queue(queue_path, expected_batch_id="gray-20260724")
    bad_item = dict(value)
    bad_item["items"] = [{**value["items"][0], "current_submission_key": ""}]
    queue_path.write_text(json.dumps(bad_item, sort_keys=True), encoding="utf-8")
    with pytest.raises(BatchRerunError, match="batch_queue_item_invalid"):
        _load_queue(queue_path, expected_batch_id="gray-20260724")

    bad_scope = dict(value)
    bad_scope["scope"] = {**value["scope"], "logic": "OR"}
    queue_path.write_text(json.dumps(bad_scope, sort_keys=True), encoding="utf-8")
    with pytest.raises(BatchRerunError, match="batch_queue_scope_invalid"):
        _load_queue(queue_path, expected_batch_id="gray-20260724")


def test_queue_accepts_explicit_absent_control_precondition(tmp_path):
    queue_path = tmp_path / "queue.json"
    queue_path.write_text(
        json.dumps(
            _queue_value(current_submission_key="", current_generation=0),
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    [item], _digest = _load_queue(
        queue_path, expected_batch_id="gray-20260724"
    )

    assert item["queue_submission_key"] == ""
    assert item["queue_generation"] == 0
    assert _queue_precondition_matches(item, None) is True
    assert _queue_precondition_matches(item, _snapshot()) is False


def _run_args(tmp_path, queue_path, owner_path):
    return SimpleNamespace(
        batch_id="gray-20260724",
        expected_runtime_commit="a" * 40,
        expected_runtime_tree="b" * 40,
        queue=str(queue_path),
        owner_receipt=str(owner_path),
        state=str(tmp_path / "state.json"),
        control_db=str(tmp_path / "control.sqlite3"),
        requester_id="automation:rca-batch-rerun",
        item_timeout_seconds=30,
        poll_seconds=1,
        retry_failed=False,
    )


def _write_run_inputs(tmp_path, queue):
    queue_path = tmp_path / "queue.json"
    queue_path.write_text(json.dumps(queue, sort_keys=True), encoding="utf-8")
    queue_sha256 = hashlib.sha256(queue_path.read_bytes()).hexdigest()
    owner_path = tmp_path / "owner.json"
    _write_owner_receipt(
        owner_path,
        _owner_receipt(queue_sha256=queue_sha256),
    )
    return queue_path, owner_path


def test_run_creates_initial_generation_for_exact_absent_item(tmp_path, monkeypatch):
    queue_path, owner_path = _write_run_inputs(
        tmp_path,
        _queue_value(current_submission_key="", current_generation=0),
    )
    admitted = False
    delivered = {
        **_snapshot(),
        "generation": 1,
        "submission_key": "g1q3-rca-s1-" + "c" * 64,
    }

    class FakeStore:
        def __init__(self, _path):
            pass

        def admit_manual_trigger(self, _request, **kwargs):
            nonlocal admitted
            assert "batch_terminal_rerun_authority" not in kwargs
            assert "silent_terminal_rerun_authority" not in kwargs
            admitted = True
            return SimpleNamespace(
                outcome="created",
                generation=1,
                submission_key=delivered["submission_key"],
                source_id="source-1",
            )

    def snapshot(_path, _issue_id, *, submission_key=""):
        if not admitted:
            return None
        if submission_key and submission_key != delivered["submission_key"]:
            return None
        return delivered

    monkeypatch.setattr(batch_rerun, "RcaControlStore", FakeStore)
    monkeypatch.setattr(batch_rerun, "_issue_snapshot", snapshot)
    monkeypatch.setattr(
        batch_rerun, "_runtime_identity", lambda: ("a" * 40, "b" * 40)
    )

    result = batch_rerun.run(_run_args(tmp_path, queue_path, owner_path))

    assert result["status"] == "completed"
    assert result["summary"] == {"accepted": 1, "total": 1}


def test_run_refreshes_existing_success_instead_of_skipping_it(tmp_path, monkeypatch):
    queue_path, owner_path = _write_run_inputs(tmp_path, _queue_value())
    prior = {
        **_snapshot(),
        "generation": 1,
        "submission_key": "g1q3-rca-s1-" + "a" * 64,
        "watch_state": "delivery_created",
        "watch_delivery_id": "delivery-1",
    }
    refreshed = {
        **_snapshot(),
        "generation": 2,
        "submission_key": "g1q3-rca-s1-" + "c" * 64,
    }
    admitted = False

    class FakeStore:
        def __init__(self, _path):
            pass

        def admit_manual_trigger(self, _request, **kwargs):
            nonlocal admitted
            authority = kwargs.get("batch_terminal_rerun_authority")
            assert authority is not None
            assert authority["prior_submission_key"] == prior["submission_key"]
            admitted = True
            return SimpleNamespace(
                outcome="created",
                generation=2,
                submission_key=refreshed["submission_key"],
                source_id="source-2",
            )

    def snapshot(_path, _issue_id, *, submission_key=""):
        value = refreshed if admitted else prior
        if submission_key and submission_key != value["submission_key"]:
            return None
        return value

    monkeypatch.setattr(batch_rerun, "RcaControlStore", FakeStore)
    monkeypatch.setattr(batch_rerun, "_issue_snapshot", snapshot)
    monkeypatch.setattr(
        batch_rerun, "_runtime_identity", lambda: ("a" * 40, "b" * 40)
    )

    result = batch_rerun.run(_run_args(tmp_path, queue_path, owner_path))

    assert admitted is True
    assert result["items"]["7048803418"]["generation"] == 2
    assert result["status"] == "completed"


def test_issue_snapshot_tracks_new_outbox_before_execution_watch_exists(tmp_path):
    db_path = tmp_path / "control.sqlite3"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE business_triggers (
            business_key TEXT,
            generation INTEGER,
            submission_key TEXT,
            work_item_id TEXT
        );
        CREATE TABLE rca_outbox (
            outbox_id INTEGER,
            business_key TEXT,
            generation INTEGER,
            status TEXT,
            last_error_code TEXT,
            last_error_detail TEXT,
            completed_at TEXT
        );
        CREATE TABLE rca_execution_watch (
            submission_outbox_id INTEGER,
            state TEXT,
            delivery_id TEXT,
            last_error_code TEXT
        );
        CREATE TABLE rca_delivery_jobs (
            submission_key TEXT,
            delivery_id TEXT,
            status TEXT,
            outcome TEXT,
            outcome_key TEXT,
            terminal_state TEXT,
            terminal_error_code TEXT,
            issue_url TEXT,
            report_url TEXT,
            manifest_json TEXT,
            contract_json TEXT,
            artifacts_json TEXT,
            updated_at TEXT
        );
        CREATE TABLE rca_delivery_effects (
            delivery_id TEXT,
            effect_key TEXT,
            effect_kind TEXT,
            required INTEGER,
            target_key TEXT,
            status TEXT,
            remote_receipt_json TEXT,
            last_error_code TEXT,
            completed_at TEXT,
            updated_at TEXT
        );
        INSERT INTO business_triggers VALUES (
            'business-1', 6, 'submission-6', '7048803418'
        );
        INSERT INTO rca_outbox VALUES (
            378, 'business-1', 6, 'pending', '', '', NULL
        );
        """
    )
    conn.commit()
    conn.close()
    before_sidecars = {
        path.name for path in tmp_path.iterdir() if path.name.endswith(("-wal", "-shm"))
    }

    snapshot = _issue_snapshot(db_path, "7048803418", submission_key="submission-6")

    assert snapshot is not None
    assert snapshot["generation"] == 6
    assert snapshot["submission_key"] == "submission-6"
    assert snapshot["outbox_status"] == "pending"
    after_sidecars = {
        path.name for path in tmp_path.iterdir() if path.name.endswith(("-wal", "-shm"))
    }
    assert after_sidecars == before_sidecars
