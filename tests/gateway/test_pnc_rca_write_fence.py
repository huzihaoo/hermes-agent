from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace

import pytest

from gateway.pnc_rca_write_fence import (
    ExternalWriteFenceError,
    build_issued_write_fence,
    canonical_write_fence_sha256,
    snapshot_core_sha256,
    validate_write_fence,
    validate_write_fence_source_binding,
)


NOW = datetime(2026, 7, 26, 12, 0, tzinfo=timezone.utc)


def _snapshot():
    return {
        "schema_version": "pnc_rca_admission_snapshot_v1",
        "request_sha256": "1" * 64,
        "canonical_request": {
            "schema_version": "pnc_rca_canonical_request_v1",
            "ticket": {
                "project_key": "t03o4q",
                "issue_url": "https://project.feishu.cn/g1q3/issue/detail/123",
            },
            "business_profile": {"value": {"profile_id": "g1q3"}},
        },
        "resolved_admission": {
            "business_key": "business-1",
            "submission_key": "submission-1",
            "generation": 1,
        },
        "execution_admission": {
            "activation_epoch_id": "epoch-1",
            "activation_ledger_id": 7,
            "decision": "admit",
            "legacy_unconfigured": False,
        },
    }


def _fence():
    snapshot = _snapshot()
    return build_issued_write_fence(
        snapshot=snapshot,
        activation_epoch_id="epoch-1",
        activation_ledger_id=7,
        admission_key="admission-1",
        target_set={
            "issue_target": snapshot["canonical_request"]["ticket"]["issue_url"],
            "thread_target": None,
        },
        now=NOW,
        expires_at=NOW + timedelta(hours=2),
    )


def _bound_snapshot_and_envelope(
    *, fence_now: datetime = NOW, submission_key: str = "submission-1"
):
    base = _snapshot()
    base["resolved_admission"]["submission_key"] = submission_key
    fence = build_issued_write_fence(
        snapshot=base,
        activation_epoch_id="epoch-1",
        activation_ledger_id=7,
        admission_key="admission-1",
        target_set={
            "issue_target": "https://project.feishu.cn/g1q3/issue/detail/123",
            "thread_target": "topic:abc",
            "chat_id": "oc_1",
        },
        now=fence_now,
        expires_at=fence_now + timedelta(hours=2),
    )
    snapshot_identity = {**base, "write_fence": fence}
    snapshot_sha256 = canonical_write_fence_sha256(snapshot_identity)
    snapshot = {
        **snapshot_identity,
        "snapshot_id": f"pnc-rca-snapshot-v1-{snapshot_sha256}",
        "snapshot_sha256": snapshot_sha256,
    }
    envelope_identity = {
        "schema_version": "pnc_rca_snapshot_source_envelope_v1",
        "source_authority_sha256": "2" * 64,
        "snapshot_id": snapshot["snapshot_id"],
        "snapshot_sha256": snapshot_sha256,
        "submission_key": submission_key,
        "source_id": "source-1",
        "source_kind": "feishu_group_manual",
        "ingress_decision": {},
        "source_metadata": {
            "platform": "feishu",
            "chat_id": "oc_1",
            "thread_id": "topic:abc",
        },
        "anchor": {
            "issue_target": "https://project.feishu.cn/g1q3/issue/detail/123",
            "thread_target": "topic:abc",
        },
    }
    envelope_sha256 = canonical_write_fence_sha256(envelope_identity)
    envelope = {
        **envelope_identity,
        "source_envelope_id": f"pnc-rca-source-envelope-v1-{envelope_sha256}",
        "source_envelope_sha256": envelope_sha256,
    }
    return snapshot, envelope, fence


def test_issued_fence_is_bound_to_core_and_operation():
    snapshot = _snapshot()
    fence = _fence()
    assert fence["state"] == "issued"
    assert fence["admission_snapshot_sha256"] == snapshot_core_sha256(snapshot)
    observed = validate_write_fence(
        fence,
        snapshot=snapshot,
        operation="feishu_issue_comment",
        target=snapshot["canonical_request"]["ticket"]["issue_url"],
        expected_epoch_id="epoch-1",
        expected_ledger_id=7,
        expected_business_key="business-1",
        expected_submission_key="submission-1",
        expected_generation=1,
        expected_issue_target=snapshot["canonical_request"]["ticket"]["issue_url"],
        now=NOW,
    )
    assert observed == fence


@pytest.mark.parametrize(
    ("mutation", "code"),
    [
        (lambda f: f.pop("write_fence", None), "external_write_fence_missing"),
        (lambda f: f.update({"activation_epoch_id": "stale"}), "external_write_fence_schema_invalid"),
        (lambda f: f.update({"allowed_write_kinds": ["vm_submit"]}), "external_write_fence_schema_invalid"),
    ],
)
def test_fence_mutations_fail_closed(mutation, code):
    fence = _fence()
    if code == "external_write_fence_missing":
        with pytest.raises(ExternalWriteFenceError) as exc:
            validate_write_fence({}, now=NOW)
    else:
        if code == "external_write_fence_schema_invalid":
            mutation(fence)
            with pytest.raises(ExternalWriteFenceError) as exc:
                validate_write_fence(fence, now=NOW, expected_epoch_id="epoch-1")
        else:
            with pytest.raises(ExternalWriteFenceError) as exc:
                validate_write_fence(fence, now=NOW, expected_epoch_id="stale")
    assert exc.value.code == code


def test_legacy_boolean_does_not_grant_without_fence():
    with pytest.raises(ExternalWriteFenceError) as exc:
        validate_write_fence({}, operation="feishu_issue_comment", target="issue", now=NOW)
    assert exc.value.code == "external_write_fence_missing"


def test_source_binding_rejects_target_and_envelope_hash_mutations():
    snapshot, envelope, fence = _bound_snapshot_and_envelope()
    targets = validate_write_fence_source_binding(
        fence,
        snapshot=snapshot,
        source_envelope=envelope,
    )
    assert targets["issue_target"].endswith("/123")
    assert targets["target_set_sha256"] == fence["target_set_sha256"]

    changed_anchor = dict(envelope)
    changed_anchor["anchor"] = {
        "issue_target": "https://project.feishu.cn/g1q3/issue/detail/999",
        "thread_target": "topic:abc",
    }
    with pytest.raises(ExternalWriteFenceError) as exc:
        validate_write_fence_source_binding(
            fence,
            snapshot=snapshot,
            source_envelope=changed_anchor,
        )
    assert exc.value.code == "external_write_fence_identity_mismatch"

    forged_envelope = dict(envelope)
    forged_envelope["source_envelope_sha256"] = "3" * 64
    with pytest.raises(ExternalWriteFenceError) as exc:
        validate_write_fence_source_binding(
            fence,
            snapshot=snapshot,
            source_envelope=forged_envelope,
        )
    assert exc.value.code == "external_write_fence_identity_mismatch"


def test_source_binding_rejects_self_consistent_epoch_ledger_mismatch():
    snapshot, envelope, fence = _bound_snapshot_and_envelope()
    forged = dict(fence)
    forged["activation_epoch_id"] = "epoch-forged"
    forged["activation_ledger_id"] = 99
    forged_payload = {
        key: forged[key] for key in forged if key not in {"fence_id", "state"}
    }
    forged["fence_id"] = (
        "pnc-rca-wf1-" + canonical_write_fence_sha256(forged_payload)
    )
    snapshot_identity = {
        key: value
        for key, value in snapshot.items()
        if key not in {"snapshot_id", "snapshot_sha256", "write_fence"}
    }
    snapshot_identity["write_fence"] = forged
    snapshot_sha256 = canonical_write_fence_sha256(snapshot_identity)
    forged_snapshot = {
        **snapshot_identity,
        "snapshot_id": f"pnc-rca-snapshot-v1-{snapshot_sha256}",
        "snapshot_sha256": snapshot_sha256,
    }
    envelope_identity = {
        key: envelope[key]
        for key in (
            "schema_version",
            "source_authority_sha256",
            "snapshot_id",
            "snapshot_sha256",
            "submission_key",
            "source_id",
            "source_kind",
            "ingress_decision",
            "source_metadata",
            "anchor",
        )
    }
    envelope_identity["snapshot_id"] = forged_snapshot["snapshot_id"]
    envelope_identity["snapshot_sha256"] = forged_snapshot["snapshot_sha256"]
    envelope_sha256 = canonical_write_fence_sha256(envelope_identity)
    forged_envelope = {
        **envelope_identity,
        "source_envelope_id": f"pnc-rca-source-envelope-v1-{envelope_sha256}",
        "source_envelope_sha256": envelope_sha256,
    }
    with pytest.raises(ExternalWriteFenceError) as exc:
        validate_write_fence_source_binding(
            forged,
            snapshot=forged_snapshot,
            source_envelope=forged_envelope,
        )
    assert exc.value.code == "external_write_fence_identity_mismatch"


def test_w13_shaped_claim_cannot_change_authoritative_issue_target():
    from scripts.pnc_rca_delivery_dispatcher import DeliveryDispatcher

    snapshot, envelope, fence = _bound_snapshot_and_envelope()
    source_targets = validate_write_fence_source_binding(
        fence,
        snapshot=snapshot,
        source_envelope=envelope,
    )

    class Store:
        def validate_learning_lane_external_operation(
            self, *, business_key, generation, operation
        ):
            assert business_key == "business-1"
            assert generation == 1
            assert operation == "feishu_issue_comment"

        def is_historical_external_write_effect(self, _created_at):
            return False

        def validate_external_write_fence_binding(self, _fence):
            return {
                "epoch_id": "epoch-1",
                "ledger_id": 7,
                "business_key": "business-1",
                "submission_key": "submission-1",
                "generation": 1,
                **source_targets,
            }

    claim = SimpleNamespace(
        contract={
            "w3_execution_snapshot": {
                "write_fence": fence,
                "snapshot_core_sha256": snapshot_core_sha256(snapshot),
            }
        },
        effect_created_at=NOW.isoformat(),
        effect_kind="feishu_issue_comment",
        payload={
            "schema_version": "pnc_rca_conclusion_adjudication_effect_v2"
        },
        business_key="business-1",
        submission_key="submission-1",
        generation=1,
        issue_url="https://project.feishu.cn/g1q3/issue/detail/123",
    )
    dispatcher = DeliveryDispatcher.__new__(DeliveryDispatcher)
    dispatcher.store = Store()
    dispatcher.now = lambda: NOW
    dispatcher._validate_external_write(
        claim,
        operation="feishu_issue_comment",
        target=claim.issue_url,
    )

    forged_claim = SimpleNamespace(**vars(claim))
    forged_claim.issue_url = "https://project.feishu.cn/g1q3/issue/detail/999"
    with pytest.raises(ExternalWriteFenceError) as exc:
        dispatcher._validate_external_write(
            forged_claim,
            operation="feishu_issue_comment",
            target=forged_claim.issue_url,
        )
    assert exc.value.code == "external_write_fence_target_mismatch"


def test_relay_boundary_uses_live_bound_thread_and_rejects_forged_target(
    monkeypatch,
):
    from scripts import pnc_completion_notice_relay as relay

    submission_key = "g1q3-rca-s1-submission-1"
    snapshot, envelope, fence = _bound_snapshot_and_envelope(
        fence_now=datetime.now(timezone.utc) - timedelta(minutes=1),
        submission_key=submission_key,
    )
    source_targets = validate_write_fence_source_binding(
        fence,
        snapshot=snapshot,
        source_envelope=envelope,
    )
    binding = {
        "snapshot": snapshot,
        "snapshot_core_sha256": snapshot_core_sha256(snapshot),
        "write_fence": fence,
        **source_targets,
    }
    live = {
        "epoch_id": "epoch-1",
        "ledger_id": 7,
        "business_key": "business-1",
        "submission_key": submission_key,
        "generation": 1,
        **source_targets,
    }
    monkeypatch.setattr(relay, "_load_task_write_fence", lambda _task: binding)
    monkeypatch.setattr(relay, "_relay_live_fence_binding", lambda _fence: live)
    sent: list[dict[str, str]] = []
    cards: list[str] = []
    send, send_card = relay._fenced_task_senders(
        "g1q3-rca-s1-submission-1",
        lambda args: sent.append(args) or "ok",
        lambda target, _payload, **_kwargs: cards.append(target)
        or {"success": True},
    )
    assert send({"target": "feishu:oc_1:abc", "message": "ok"}) == "ok"
    assert send_card(
        "feishu:oc_1:abc", {"title": "ok"}
    )["success"] is True
    with pytest.raises(ExternalWriteFenceError) as exc:
        send({"target": "feishu:oc_1:attacker", "message": "bad"})
    assert exc.value.code == "external_write_fence_target_mismatch"
    assert len(sent) == 1
    assert cards == ["feishu:oc_1:abc"]


def test_attachment_boundary_rejects_mutated_source_anchor(monkeypatch):
    from scripts import pnc_vm_task_sync as vm_sync

    submission_key = "g1q3-rca-s1-submission-1"
    fence_now = datetime.now(timezone.utc) - timedelta(minutes=1)
    snapshot, envelope, fence = _bound_snapshot_and_envelope(
        fence_now=fence_now,
        submission_key=submission_key,
    )
    snapshot["canonical_request"]["ticket"]["work_item_id"] = "123"
    # Rebuild the final snapshot/fence identity after adding the immutable item id.
    fence = build_issued_write_fence(
        snapshot={key: value for key, value in snapshot.items() if key not in {"snapshot_id", "snapshot_sha256", "write_fence"}},
        activation_epoch_id="epoch-1",
        activation_ledger_id=7,
        admission_key="admission-1",
        target_set={
            "issue_target": "https://project.feishu.cn/g1q3/issue/detail/123",
            "thread_target": "topic:abc",
            "chat_id": "oc_1",
        },
        now=fence_now,
        expires_at=fence_now + timedelta(hours=2),
    )
    snapshot_identity = {
        key: value
        for key, value in snapshot.items()
        if key not in {"snapshot_id", "snapshot_sha256", "write_fence"}
    }
    snapshot_identity["write_fence"] = fence
    snapshot_sha256 = canonical_write_fence_sha256(snapshot_identity)
    snapshot = {
        **snapshot_identity,
        "snapshot_id": f"pnc-rca-snapshot-v1-{snapshot_sha256}",
        "snapshot_sha256": snapshot_sha256,
    }
    envelope["snapshot_id"] = snapshot["snapshot_id"]
    envelope["snapshot_sha256"] = snapshot_sha256
    envelope_identity = {
        key: envelope[key]
        for key in (
            "schema_version",
            "source_authority_sha256",
            "snapshot_id",
            "snapshot_sha256",
            "submission_key",
            "source_id",
            "source_kind",
            "ingress_decision",
            "source_metadata",
            "anchor",
        )
    }
    envelope_sha256 = canonical_write_fence_sha256(envelope_identity)
    envelope["source_envelope_sha256"] = envelope_sha256
    envelope["source_envelope_id"] = f"pnc-rca-source-envelope-v1-{envelope_sha256}"

    class Row(dict):
        pass

    class Cursor:
        def __init__(self, row):
            self.row = row

        def fetchone(self):
            return self.row

    class Connection:
        row_factory = None

        def execute(self, sql, _params=()):
            if "admission_snapshot_json" in sql:
                return Cursor(Row(
                    {
                        "admission_snapshot_json": json.dumps(snapshot),
                        "source_envelope_json": json.dumps(envelope),
                    }
                ))
            return Cursor(Row(
                {
                    "epoch_id": "epoch-1",
                    "state": "bounded_active",
                    "is_current": 1,
                    "ledger_id": 7,
                    "business_key": "business-1",
                    "submission_key": submission_key,
                    "generation": 1,
                    "decision": "admit",
                    "bound_at": fence_now.isoformat(),
                }
            ))

        def close(self):
            return None

    monkeypatch.setenv("HERMES_RCA_CONTROL_DB_PATH", "/tmp/w5-fence-test.sqlite3")
    monkeypatch.setattr(vm_sync.sqlite3, "connect", lambda *_args, **_kwargs: Connection())
    vm_sync._validate_report_attachment_fence(
        vm_task_id="g1q3-rca-s1-submission-1",
        work_item_id="123",
    )
    envelope["anchor"] = {
        "issue_target": "https://project.feishu.cn/g1q3/issue/detail/999",
        "thread_target": "topic:abc",
    }
    with pytest.raises(ExternalWriteFenceError) as exc:
        vm_sync._validate_report_attachment_fence(
            vm_task_id="g1q3-rca-s1-submission-1",
            work_item_id="123",
        )
    assert exc.value.code == "external_write_fence_identity_mismatch"


def test_delivery_cutoff_is_durable_and_grandfathers_only_old_rows(tmp_path):
    pytest.importorskip("psutil")
    from gateway.pnc_rca_delivery_store import RcaDeliveryStore

    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    assert store.is_historical_external_write_effect("2026-07-24T23:59:59Z")
    assert not store.is_historical_external_write_effect("2026-07-25T00:00:00Z")
    with store._connect() as conn:
        marker = conn.execute(
            "SELECT value FROM rca_delivery_meta WHERE key = 'w5_external_write_fence_cutoff'"
        ).fetchone()
    assert marker is not None
    assert marker[0] == "2026-07-25T00:00:00+00:00"


def test_dispatcher_grandfather_is_immutable_but_new_missing_fence_blocks():
    pytest.importorskip("psutil")
    from scripts.pnc_rca_delivery_dispatcher import DeliveryDispatcher

    class Store:
        def __init__(self, historical):
            self.historical = historical

        def validate_learning_lane_external_operation(
            self, *, business_key, generation, operation
        ):
            assert business_key == "business-1"
            assert generation == 1
            assert operation == "feishu_issue_comment"

        def is_historical_external_write_effect(self, _created_at):
            return self.historical

    claim = SimpleNamespace(
        contract={},
        effect_created_at="2026-07-25T00:00:01+00:00",
        business_key="business-1",
        generation=1,
    )
    historical_dispatcher = DeliveryDispatcher.__new__(DeliveryDispatcher)
    historical_dispatcher.store = Store(True)
    historical_dispatcher.now = lambda: NOW
    historical_dispatcher._validate_external_write(
        claim,
        operation="feishu_issue_comment",
        target="issue",
    )

    new_dispatcher = DeliveryDispatcher.__new__(DeliveryDispatcher)
    new_dispatcher.store = Store(False)
    new_dispatcher.now = lambda: NOW
    with pytest.raises(ExternalWriteFenceError) as exc:
        new_dispatcher._validate_external_write(
            claim,
            operation="feishu_issue_comment",
            target="issue",
        )
    assert exc.value.code == "external_write_fence_missing"
