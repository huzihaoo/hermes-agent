from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from gateway.pnc_rca_write_fence import (
    ExternalWriteFenceError,
    build_issued_write_fence,
    snapshot_core_sha256,
    validate_write_fence,
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

        def is_historical_external_write_effect(self, _created_at):
            return self.historical

    claim = SimpleNamespace(
        contract={},
        effect_created_at="2026-07-25T00:00:01+00:00",
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
