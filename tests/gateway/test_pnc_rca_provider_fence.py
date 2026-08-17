from __future__ import annotations

import sqlite3
from types import SimpleNamespace

import pytest

from gateway import pnc_rca_provider_fence as provider_fence
from gateway.pnc_rca_control_store import RcaControlStore
from gateway.pnc_rca_delivery_store import RcaDeliveryStore
from gateway.pnc_rca_write_fence import ExternalWriteFenceError
from tests.gateway.test_pnc_rca_delivery_store import (
    _physical_v15_delivery_fixture,
    _sqlite_storage_identity,
)


ISSUE_URL = "https://project.feishu.cn/t03o4q/issue/detail/7041712812"
PROJECT_KEY = "project-key"
PROJECT_SIMPLE_NAME = "t03o4q"
WORK_ITEM_ID = "7041712812"


def _provider_binding(kind: str) -> tuple[dict[str, object], object, str]:
    common = {
        "epoch_id": "provider-epoch-v14",
        "activation_ledger_id": 17,
        "effect_key": "provider-effect-key",
        "delivery_id": "provider-delivery-id",
        "lease_token": "provider-lease-token",
        "lease_fence": 3,
        "operation": "feishu_issue_comment",
        "issue_url": ISSUE_URL,
        "target_key": f"feishu_project:{PROJECT_KEY}:problem:{WORK_ITEM_ID}",
        "business_key": f"{PROJECT_KEY}:problem:{WORK_ITEM_ID}",
        "submission_key": "g1q3-rca-s1-" + "a" * 64,
        "generation": 2,
        "project_key": PROJECT_KEY,
        "project_simple_name": PROJECT_SIMPLE_NAME,
    }
    if kind == "profile_terminal":
        live = {**common, "source_error_code": "business_profile_adapter_not_ready"}
        claim = provider_fence.build_profile_terminal_provider_claim(
            epoch_id=str(live["epoch_id"]),
            activation_ledger_id=int(live["activation_ledger_id"]),
            effect_key=str(live["effect_key"]),
            delivery_id=str(live["delivery_id"]),
            lease_token=str(live["lease_token"]),
            lease_fence=int(live["lease_fence"]),
            issue_target=str(live["issue_url"]),
            project_key=str(live["project_key"]),
            project_simple_name=str(live["project_simple_name"]),
            target_key=str(live["target_key"]),
            business_key=str(live["business_key"]),
            submission_key=str(live["submission_key"]),
            generation=int(live["generation"]),
            source_error_code=str(live["source_error_code"]),
        )
        return live, claim, "validate_profile_terminal_external_write_binding"
    live = {
        **common,
        "authority_sha256": "b" * 64,
        "outbox_id": 41,
        "work_item_type_key": "problem",
        "work_item_id": WORK_ITEM_ID,
    }
    claim = provider_fence.build_terminal_rerun_provider_claim(
        authority_sha256=str(live["authority_sha256"]),
        outbox_id=int(live["outbox_id"]),
        epoch_id=str(live["epoch_id"]),
        activation_ledger_id=int(live["activation_ledger_id"]),
        effect_key=str(live["effect_key"]),
        delivery_id=str(live["delivery_id"]),
        lease_token=str(live["lease_token"]),
        lease_fence=int(live["lease_fence"]),
        issue_target=str(live["issue_url"]),
        target_key=str(live["target_key"]),
        business_key=str(live["business_key"]),
        submission_key=str(live["submission_key"]),
        generation=int(live["generation"]),
        project_key=str(live["project_key"]),
        project_simple_name=str(live["project_simple_name"]),
        work_item_type_key=str(live["work_item_type_key"]),
        work_item_id=str(live["work_item_id"]),
    )
    return live, claim, "validate_terminal_rerun_external_write_binding"


@pytest.mark.parametrize("kind", ["profile_terminal", "terminal_rerun"])
def test_provider_revalidation_uses_live_current_store_with_active_wal(
    tmp_path,
    monkeypatch,
    kind,
):
    path, _migration = _physical_v15_delivery_fixture(tmp_path / kind)
    control = RcaControlStore(
        path,
        require_current=True,
        allow_successor_write=True,
    )
    live, claim, validation_method = _provider_binding(kind)
    validations = []

    def validate_binding(store, **kwargs):
        validations.append(kwargs)
        assert store.requested_read_only is False
        assert store.ensure_current_rows is False
        assert store.schema_runtime_capability()["mode"] == "current_write"
        return dict(live)

    monkeypatch.setattr(RcaDeliveryStore, validation_method, validate_binding)
    monkeypatch.setattr(provider_fence, "_canonical_store", lambda: control)
    monkeypatch.setattr(
        RcaControlStore,
        "create_schema_probe_snapshot",
        classmethod(
            lambda _cls, *_args, **_kwargs: pytest.fail(
                "provider revalidation must not raw-copy a current v15 store"
            )
        ),
    )

    wal_writer = sqlite3.connect(path)
    try:
        assert wal_writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
        wal_writer.execute("PRAGMA wal_autocheckpoint=0")
        wal_writer.execute(
            "INSERT INTO rca_delivery_meta(key, value) VALUES(?, ?)",
            (f"provider_active_wal_{kind}", "present"),
        )
        wal_writer.commit()
        before = _sqlite_storage_identity(path)

        result = provider_fence.revalidate_provider_write_claim(
            claim,
            operation="feishu_issue_comment",
            issue_project_key=PROJECT_KEY,
            issue_work_item_id=WORK_ITEM_ID,
        )

        after = _sqlite_storage_identity(path)
        assert after["db"] == before["db"]
        assert after["-wal"] == before["-wal"]
        assert (after["-shm"] is None) is (before["-shm"] is None)
    finally:
        wal_writer.close()

    assert result["authority_kind"] == kind
    assert len(validations) == 1


@pytest.mark.parametrize(
    ("kind", "error_code"),
    [
        ("profile_terminal", "external_write_fence_epoch_not_current"),
        ("terminal_rerun", "external_write_fence_identity_mismatch"),
    ],
)
def test_provider_revalidation_rejects_v14_before_validation_or_external_write(
    tmp_path,
    monkeypatch,
    kind,
    error_code,
):
    path = tmp_path / "control.sqlite3"
    RcaControlStore(path)
    RcaDeliveryStore(path)
    _live, claim, validation_method = _provider_binding(kind)
    monkeypatch.setattr(
        RcaDeliveryStore,
        validation_method,
        lambda *_args, **_kwargs: pytest.fail("v14 business binding was evaluated"),
    )
    monkeypatch.setattr(
        provider_fence,
        "_canonical_store",
        lambda: SimpleNamespace(db_path=path),
    )
    before = _sqlite_storage_identity(path)
    external_calls = []

    def physical_write_boundary():
        provider_fence.revalidate_provider_write_claim(
            claim,
            operation="feishu_issue_comment",
            issue_project_key=PROJECT_KEY,
            issue_work_item_id=WORK_ITEM_ID,
        )
        external_calls.append("provider_write")

    with pytest.raises(ExternalWriteFenceError) as exc:
        physical_write_boundary()

    assert exc.value.code == error_code
    assert external_calls == []
    after = _sqlite_storage_identity(path)
    assert after["db"] == before["db"]
    assert after["-wal"] == before["-wal"]
    assert (after["-shm"] is None) is (before["-shm"] is None)
