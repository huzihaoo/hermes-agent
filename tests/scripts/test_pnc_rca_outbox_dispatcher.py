from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from gateway.pnc_rca_control_store import RcaControlStore
from gateway.pnc_rca_runtime_identity import RCA_RUNTIME_RELATIVE_FILES
from gateway.pnc_rca_runtime_identity import canonical_json_sha256
from scripts import pnc_rca_outbox_dispatcher as dispatcher
from tests.gateway.test_pnc_rca_control_store import (
    _manual_request,
    _policy,
    _profile_snapshot_policy,
    _profile_snapshot_record,
    _record,
    _steady_control_store,
)


def test_storage_admission_uses_isolated_production_runtime():
    assert dispatcher.REMOTE_STORAGE_ADMISSION_MODULE == (
        "/home/mini/.hermes/rca-prod-runtime/releases/"
        "rca-platform-20260724/api/g1q3_rca/storage_admission.py"
    )


def test_stored_unsupported_profile_is_terminal_before_preread(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    result = store.ingest_record(
        _profile_snapshot_record(301, "6841983153"),
        policy=_profile_snapshot_policy(),
        submit_enabled=True,
    )
    assert result.decision == "accepted"
    claim = store.claim_outbox(lease_owner="profile-terminal-test")
    assert claim is not None
    admission, event = dispatcher._validated_claim_contract(claim)

    error = dispatcher._stored_profile_terminal_error(
        claim=claim,
        admission=admission,
        event=event,
    )

    assert error is not None
    assert error[0] == "business_profile_unsupported"
    assert "no G1Q3 evaluator" in error[1]


def test_stored_unresolved_profile_keeps_preread_path(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    result = store.ingest_record(
        _profile_snapshot_record(302, ""),
        policy=_profile_snapshot_policy(),
        submit_enabled=True,
    )
    assert result.decision == "accepted"
    claim = store.claim_outbox(lease_owner="profile-unresolved-test")
    assert claim is not None
    admission, event = dispatcher._validated_claim_contract(claim)

    assert (
        dispatcher._stored_profile_terminal_error(
            claim=claim,
            admission=admission,
            event=event,
        )
        is None
    )


def test_stored_adapter_pending_profile_is_terminal_before_preread(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    result = store.ingest_record(
        _profile_snapshot_record(303, "7019637554"),
        policy=_profile_snapshot_policy(),
        submit_enabled=True,
    )
    assert result.decision == "accepted"
    claim = store.claim_outbox(lease_owner="profile-adapter-terminal-test")
    assert claim is not None
    admission, event = dispatcher._validated_claim_contract(claim)

    error = dispatcher._stored_profile_terminal_error(
        claim=claim,
        admission=admission,
        event=event,
    )

    assert error is not None
    assert error[0] == "business_profile_adapter_not_ready"
    assert "input adapter is not ready" in error[1]


@pytest.mark.parametrize(
    ("option_ids", "expected_error"),
    (
        (("6841983153",), "business_profile_unsupported"),
        (("6841983153", "7019637554"), "business_profile_conflict"),
    ),
)
def test_unsupported_profile_dispatch_stops_before_all_execution_boundaries(
    tmp_path,
    option_ids,
    expected_error,
):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    record = _profile_snapshot_record(304, option_ids[0])
    if len(option_ids) > 1:
        payload = json.loads(record.value)
        payload["fields"][0]["field_value"] = list(option_ids)
        record = replace(
            record,
            value=json.dumps(payload, sort_keys=True).encode(),
        )
    result = store.ingest_record(
        record,
        policy=_profile_snapshot_policy(),
        submit_enabled=True,
    )
    assert result.decision == "accepted"
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path, enabled=True),
        hermes_home=tmp_path,
    )
    boundary_calls = []

    def forbidden_boundary(name):
        def invoke(*_args, **_kwargs):
            boundary_calls.append(name)
            raise AssertionError(f"{name} must not run for an outbox-only terminal")

        return invoke

    instance = dispatcher.OutboxDispatcher(
        store=store,
        config=config,
        enrich=forbidden_boundary("enrich"),
        storage_admission=forbidden_boundary("storage_admission"),
        submit=forbidden_boundary("vm_submit"),
        derived_capacity_reservation=forbidden_boundary("capacity_reservation"),
        lease_owner="profile-outbox-only-test",
    )
    instance._delivery_backpressure_outcome = lambda: None

    outcome = instance.dispatch_one()

    assert outcome.status == "quarantined"
    assert outcome.error_code == expected_error
    assert boundary_calls == []
    [outbox] = store.list_rows("rca_outbox")
    assert outbox["status"] == "quarantined"
    assert outbox["last_error_code"] == expected_error
    assert outbox["result_json"] is None


def _config_env(
    tmp_path, *, enabled: bool = False, canonical_layout: bool = False
) -> dict[str, str]:
    control_db_path = (
        tmp_path
        / "runtime"
        / "pnc_agent"
        / "feishu_issue_kafka_rca"
        / "control.sqlite3"
        if canonical_layout
        else tmp_path / "control.sqlite3"
    )
    return {
        "HERMES_RCA_OUTBOX_DISPATCH_ENABLED": str(enabled).lower(),
        "HERMES_RCA_OUTBOX_STORAGE_ADMISSION_ENABLED": str(enabled).lower(),
        "HERMES_RCA_OUTBOX_DERIVED_CAPACITY_RESERVATION_ENABLED": str(enabled).lower(),
        "HERMES_RCA_OUTBOX_DELIVERY_BACKPRESSURE_ENABLED": str(enabled).lower(),
        "HERMES_RCA_OUTBOX_CONTROL_DB_PATH": str(control_db_path),
        "HERMES_RCA_OUTBOX_HEALTH_PATH": str(tmp_path / "health.json"),
    }


def test_submission_receipt_binds_preread_work_item_title():
    submission_key = "g1q3-rca-s1-" + "a" * 64
    title_binding = dispatcher._submission_work_item_title_binding(
        SimpleNamespace(work_item={"title": "ACC-右车近距离切入ACC不减速"})
    )

    receipt = dispatcher._submission_receipt(
        {
            "task": {"task_id": submission_key, "state": "submitted"},
            "success": True,
            "deduped": False,
            "created": True,
            "returncode": 0,
        },
        submission_key=submission_key,
        work_item_title_binding=title_binding,
        capacity_admission_summary={},
        derived_capacity_reservation_receipt={},
    )

    assert receipt["work_item"] == {
        "title": "ACC-右车近距离切入ACC不减速",
        "title_sha256": dispatcher.issue_title_sha256("ACC-右车近距离切入ACC不减速"),
    }


def test_config_does_not_require_or_project_legacy_release_identity(tmp_path):
    env = _config_env(tmp_path)
    env["HERMES_RCA_OUTBOX_STORAGE_RESERVATION_ENABLED"] = "true"
    config = dispatcher.DispatcherConfig.from_env(env, hermes_home=tmp_path)

    public = config.public_dict()
    assert config.activation_required is False
    assert public["activation_required"] is False
    assert "capacity_mode" not in public
    assert "release_id" not in public
    assert "release_id" not in config.runtime_public_dict()
    assert "bootstrap_epoch_id" not in public
    assert not hasattr(config, "storage_reservation_enabled")
    assert "storage_reservation_enabled" not in public


def test_config_exposes_strict_activation_required(tmp_path):
    env = _config_env(tmp_path)
    env["HERMES_RCA_OUTBOX_ACTIVATION_REQUIRED"] = "true"

    config = dispatcher.DispatcherConfig.from_env(env, hermes_home=tmp_path)

    assert config.activation_required is True
    assert config.public_dict()["activation_required"] is True


def test_config_rejects_declarative_feishu_writeback_enablement(tmp_path):
    env = _config_env(tmp_path)
    env["HERMES_RCA_OUTBOX_ALLOW_FEISHU_WRITEBACK"] = "true"

    with pytest.raises(ValueError, match="declarative-only"):
        dispatcher.DispatcherConfig.from_env(env, hermes_home=tmp_path)


def test_config_keeps_legacy_feishu_writeback_projection_false(tmp_path):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )

    assert config.allow_feishu_writeback is False
    assert config.public_dict()["allow_feishu_writeback"] is False


@pytest.mark.parametrize("value", ["1", "0", "yes", "on", "off", ""])
def test_activation_required_rejects_boolean_aliases(tmp_path, value):
    env = _config_env(tmp_path)
    env["HERMES_RCA_OUTBOX_ACTIVATION_REQUIRED"] = value

    with pytest.raises(ValueError, match="exactly true or false"):
        dispatcher.DispatcherConfig.from_env(env, hermes_home=tmp_path)


def test_delivery_backpressure_snapshot_uses_strict_activation_scope(tmp_path):
    env = _config_env(tmp_path)
    env["HERMES_RCA_OUTBOX_ACTIVATION_REQUIRED"] = "true"
    config = dispatcher.DispatcherConfig.from_env(env, hermes_home=tmp_path)
    RcaControlStore(config.control_db_path)
    snapshot = dispatcher.RcaDeliveryStore(
        config.delivery_db_path
    ).backpressure_snapshot()
    calls = []

    class ProbeDeliveryStore:
        def backpressure_snapshot(self, *, now, activation_required):
            calls.append((now, activation_required))
            return snapshot

    instance = object.__new__(dispatcher.OutboxDispatcher)
    instance.config = config
    instance.delivery_store = ProbeDeliveryStore()
    instance.now = lambda: datetime(2026, 8, 8, tzinfo=timezone.utc)
    instance.stats = dispatcher.DispatchStats()
    instance._delivery_backpressure_active = False
    instance._last_delivery_snapshot = None
    instance._last_delivery_error = None

    assert instance._delivery_backpressure_outcome() is None
    assert calls == [(datetime(2026, 8, 8, tzinfo=timezone.utc), True)]


def test_delivery_circuit_does_not_block_upstream_rca_submission(tmp_path):
    env = _config_env(tmp_path)
    config = dispatcher.DispatcherConfig.from_env(env, hermes_home=tmp_path)
    RcaControlStore(config.control_db_path)
    delivery_store = dispatcher.RcaDeliveryStore(config.delivery_db_path)
    delivery_store.open_delivery_dispatcher_circuit(
        reason_code="report_public_origin_invalid",
        now=datetime(2026, 8, 8, tzinfo=timezone.utc),
    )
    instance = object.__new__(dispatcher.OutboxDispatcher)
    instance.config = config
    instance.delivery_store = delivery_store
    instance.now = lambda: datetime(2026, 8, 8, tzinfo=timezone.utc)
    instance.stats = dispatcher.DispatchStats()
    instance._delivery_backpressure_active = False
    instance._last_delivery_snapshot = None
    instance._last_delivery_error = None

    assert instance._delivery_backpressure_outcome() is None
    assert (
        instance._last_delivery_snapshot["delivery_dispatcher_circuit"]["state"]
        == "open"
    )
    assert instance.stats.delivery_circuit_blocked == 0


def test_dispatcher_renew_uses_current_steady_lease_contract(tmp_path):
    env = _config_env(tmp_path)
    env["HERMES_RCA_OUTBOX_ACTIVATION_REQUIRED"] = "true"
    calls = []
    instance = object.__new__(dispatcher.OutboxDispatcher)
    instance.config = dispatcher.DispatcherConfig.from_env(
        env,
        hermes_home=tmp_path,
    )
    instance.store = SimpleNamespace(
        extend_outbox_lease=lambda **kwargs: calls.append(kwargs)
    )
    instance.now = lambda: datetime.now(timezone.utc)
    claim = SimpleNamespace(
        outbox_id=17,
        lease_token="lease-token",
        lease_owner="lease-owner",
    )

    instance._renew(claim)

    assert calls[0]["outbox_id"] == 17
    assert calls[0]["lease_token"] == "lease-token"
    assert calls[0]["lease_owner"] == "lease-owner"
    assert calls[0]["lease_seconds"] == instance.config.lease_seconds
    assert "activation_required" not in calls[0]


def test_enabled_resident_without_epoch_exits_before_dispatcher_creation(
    tmp_path,
    monkeypatch,
    capsys,
):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path, enabled=True),
        hermes_home=tmp_path,
    )
    store = RcaControlStore(config.control_db_path)
    constructed = False

    def unexpected_dispatcher(*_args, **_kwargs):
        nonlocal constructed
        constructed = True
        raise AssertionError("dispatcher must not start without an active epoch")

    monkeypatch.setattr(dispatcher, "load_dispatcher_environment", lambda _path: None)
    monkeypatch.setattr(
        dispatcher.DispatcherConfig,
        "from_env",
        classmethod(lambda _cls: config),
    )
    monkeypatch.setattr(dispatcher, "OutboxDispatcher", unexpected_dispatcher)

    assert dispatcher.main(["--once"]) == 2
    assert constructed is False
    assert store.list_rows("kafka_inbox") == []
    assert store.list_rows("rca_outbox") == []
    assert "resident_activation_epoch_missing" in capsys.readouterr().err


def _patch_reset_cli(monkeypatch, config):
    env_path = config.control_db_path.parent / ".env"
    env_path.parent.mkdir(parents=True, exist_ok=True)
    env_path.write_text("HERMES_TEST=true\n", encoding="utf-8")
    monkeypatch.setattr(dispatcher, "get_hermes_home", lambda: env_path.parent)
    monkeypatch.setattr(
        dispatcher,
        "load_dispatcher_environment",
        lambda _path: env_path,
    )
    monkeypatch.setattr(
        dispatcher.DispatcherConfig,
        "from_env",
        classmethod(lambda _cls, *_args, **_kwargs: config),
    )


def test_clear_circuit_plan_does_not_mutate_or_create_receipt(
    tmp_path, monkeypatch, capsys
):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )
    store = RcaControlStore(config.control_db_path)
    store.open_dispatcher_circuit(
        reason_code="snapshot_stale", reason_detail="offline test"
    )
    receipt = tmp_path / "reset.json"
    _patch_reset_cli(monkeypatch, config)

    assert (
        dispatcher.main([
            "--clear-circuit",
            "--operator",
            "owner@example.com",
            "--reason",
            "verify snapshot before rearm",
            "--receipt",
            str(receipt),
        ])
        == 0
    )

    result = json.loads(capsys.readouterr().out)
    assert result["mode"] == "plan"
    assert result["applied"] is False
    assert result["pre_state"]["state"] == "open"
    assert store.dispatcher_circuit().state == "open"
    assert not receipt.exists()


def test_clear_circuit_apply_writes_receipt_and_db_audit(tmp_path, monkeypatch, capsys):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )
    store = RcaControlStore(config.control_db_path)
    store.open_dispatcher_circuit(
        reason_code="snapshot_stale", reason_detail="offline test"
    )
    receipt = tmp_path / "reset.json"
    _patch_reset_cli(monkeypatch, config)

    assert (
        dispatcher.main([
            "--clear-circuit",
            "--operator",
            "owner@example.com",
            "--reason",
            "verify snapshot before rearm",
            "--apply",
            "--receipt",
            str(receipt),
        ])
        == 0
    )

    result = json.loads(capsys.readouterr().out)
    body = json.loads(receipt.read_text(encoding="utf-8"))
    assert result["applied"] is True
    assert result["receipt"] == str(receipt.absolute())
    assert result["receipt_sha256"]
    assert body["operator"] == "owner@example.com"
    assert body["reason"] == "verify snapshot before rearm"
    assert body["pre_state"]["state"] == "open"
    assert body["post_state"]["state"] == "closed"
    assert body["effect_delta"]["external_writes"] == 0
    assert body["receipt_fingerprint"] == canonical_json_sha256({
        key: value for key, value in body.items() if key != "receipt_fingerprint"
    })
    assert receipt.stat().st_mode & 0o777 == 0o444
    sidecar = receipt.with_name(receipt.name + ".sha256")
    assert sidecar.read_text(encoding="ascii").startswith(result["receipt_sha256"])
    assert store.dispatcher_circuit().state == "closed"
    assert store.dispatcher_circuit_reset_audit(body["reset_id"]) == body


def test_clear_circuit_duplicate_receipt_is_rejected_before_mutation(
    tmp_path, monkeypatch, capsys
):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )
    store = RcaControlStore(config.control_db_path)
    store.open_dispatcher_circuit(reason_code="snapshot_stale")
    receipt = tmp_path / "reset.json"
    _patch_reset_cli(monkeypatch, config)
    args = [
        "--clear-circuit",
        "--operator",
        "owner",
        "--reason",
        "first reset",
        "--apply",
        "--receipt",
        str(receipt),
    ]
    assert dispatcher.main(args) == 0
    capsys.readouterr()

    # Re-open the circuit to prove the duplicate path is checked before the
    # second mutation attempt.
    store.open_dispatcher_circuit(reason_code="reopened-for-negative-test")
    assert dispatcher.main(args) == 2
    assert "already_exists" in capsys.readouterr().err
    assert store.dispatcher_circuit().state == "open"


def test_clear_circuit_closed_state_fails_closed_without_plan_success(
    tmp_path, monkeypatch, capsys
):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )
    store = RcaControlStore(config.control_db_path)
    _patch_reset_cli(monkeypatch, config)

    assert (
        dispatcher.main([
            "--clear-circuit",
            "--operator",
            "owner",
            "--reason",
            "do not reset a closed circuit",
        ])
        == 2
    )
    assert "requires_open_circuit" in capsys.readouterr().err
    assert store.dispatcher_circuit().state == "closed"


def test_clear_circuit_receipt_materialization_failure_reports_recovery(
    tmp_path, monkeypatch, capsys
):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )
    store = RcaControlStore(config.control_db_path)
    store.open_dispatcher_circuit(reason_code="snapshot_stale")
    receipt = tmp_path / "reset.json"
    _patch_reset_cli(monkeypatch, config)
    writer = dispatcher._write_immutable_receipt

    def fail_materialization(*_args, **_kwargs):
        raise OSError("simulated receipt filesystem failure")

    monkeypatch.setattr(dispatcher, "_write_immutable_receipt", fail_materialization)
    assert (
        dispatcher.main([
            "--clear-circuit",
            "--operator",
            "owner",
            "--reason",
            "persist database audit before filesystem copy",
            "--apply",
            "--receipt",
            str(receipt),
        ])
        == 2
    )
    error = json.loads(capsys.readouterr().err)
    assert error["recovery_required"] is True
    assert error["meta_key"].startswith("rca_dispatcher_circuit_reset:")
    assert store.dispatcher_circuit().state == "closed"
    assert store.dispatcher_circuit_reset_audit(error["reset_id"]) is not None
    monkeypatch.setattr(dispatcher, "_write_immutable_receipt", writer)
    recovered = tmp_path / "recovered-reset.json"
    assert (
        dispatcher.main([
            "--materialize-reset",
            error["reset_id"],
            "--receipt",
            str(recovered),
        ])
        == 0
    )
    recovery_result = json.loads(capsys.readouterr().out)
    assert recovery_result["recovered"] is True
    assert (
        json.loads(recovered.read_text(encoding="utf-8"))["reset_id"]
        == error["reset_id"]
    )


def test_clear_circuit_rejects_relative_receipt_before_mutation(
    tmp_path, monkeypatch, capsys
):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )
    store = RcaControlStore(config.control_db_path)
    store.open_dispatcher_circuit(reason_code="snapshot_stale")
    _patch_reset_cli(monkeypatch, config)

    assert (
        dispatcher.main([
            "--clear-circuit",
            "--operator",
            "owner",
            "--reason",
            "absolute path required",
            "--apply",
            "--receipt",
            "relative-reset.json",
        ])
        == 2
    )
    assert "path_invalid" in capsys.readouterr().err
    assert store.dispatcher_circuit().state == "open"


@pytest.mark.parametrize(
    "extra,expected",
    [
        (["--reason", "reason"], "operator_and_reason_required"),
        (["--operator", "operator"], "operator_and_reason_required"),
        (
            ["--operator", "operator", "--reason", "reason", "--apply"],
            "receipt_required",
        ),
    ],
)
def test_clear_circuit_apply_requires_bounded_audit_inputs(
    tmp_path, monkeypatch, capsys, extra, expected
):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )
    store = RcaControlStore(config.control_db_path)
    store.open_dispatcher_circuit(reason_code="snapshot_stale")
    _patch_reset_cli(monkeypatch, config)

    assert dispatcher.main(["--clear-circuit", *extra]) == 2
    assert expected in capsys.readouterr().err
    assert store.dispatcher_circuit().state == "open"


def test_config_has_no_production_capacity_mode_selector(tmp_path):
    env = _config_env(tmp_path, canonical_layout=True)
    env["HERMES_RCA_PROD_CAPACITY_MODE"] = "bootstrap"

    config = dispatcher.DispatcherConfig.from_env(env, hermes_home=tmp_path)

    assert "capacity_mode" not in config.public_dict()


def test_default_submit_has_no_bootstrap_capacity_contract(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(
        "tools.vm_task_tool.vm_task_submit_service",
        lambda **kwargs: calls.append(kwargs) or {"success": True},
    )
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )
    dispatcher.default_submit(object(), SimpleNamespace(toolchain={}), config=config)
    assert not any("bootstrap" in key or key == "capacity_mode" for key in calls[0])


def test_default_submit_preserves_reconcile_only_dedupe(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(
        "tools.vm_task_tool.vm_task_submit_service",
        lambda **kwargs: calls.append(kwargs) or {"success": True},
    )
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )
    request = SimpleNamespace(
        toolchain={"derived_capacity_reservation": {"status": "released"}}
    )

    dispatcher.default_submit(object(), request, config=config)

    assert calls[0]["reconcile_only"] is True


def test_default_submit_defers_live_store_check_to_service_create_guard(
    monkeypatch, tmp_path
):
    submit_calls = []
    fence_checks = []
    monkeypatch.setattr(
        "tools.vm_task_tool.vm_task_submit_service",
        lambda **kwargs: submit_calls.append(kwargs) or {"success": True},
    )
    monkeypatch.setattr(
        dispatcher,
        "validate_snapshot_execution_bundle",
        lambda _value: SimpleNamespace(snapshot="immutable-bundle"),
    )
    monkeypatch.setattr(
        dispatcher,
        "_validate_vm_submit_fence",
        lambda **kwargs: fence_checks.append(kwargs),
    )
    config = replace(
        dispatcher.DispatcherConfig.from_env(
            _config_env(tmp_path), hermes_home=tmp_path
        ),
        w3_snapshot_read_mode="snapshot_required",
    )
    live_binding = {"epoch_id": "epoch-live", "ledger_id": 17}
    store = SimpleNamespace(
        validate_external_write_fence_binding=lambda _fence: live_binding
    )

    dispatcher.default_submit(
        object(),
        SimpleNamespace(toolchain={"w3_execution_snapshot": {"fixture": True}}),
        config=config,
        control_store=store,
    )

    assert len(fence_checks) == 1
    assert "control_store" not in fence_checks[0]
    authority = submit_calls[0]["live_write_fence_authority"]
    assert authority({"state": "issued"}) == live_binding


def test_steady_health_requires_hmac_and_has_no_expiring_authorization(tmp_path):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path, enabled=True), hermes_home=tmp_path
    )
    reporter = object.__new__(dispatcher.HealthReporter)
    reporter.config = config
    reporter._admission_key_fingerprint_observer = lambda: "7" * 64

    status = reporter.capacity_admission_status()

    assert status["ready"] is True
    assert status["capacity_mode"] == "steady"
    assert "deadline" not in status["authorization"]
    assert "successful_sample_count" not in status["authorization"]
    assert dispatcher._health_capacity_admission_ok({
        "enabled": True,
        "config": config.public_dict(),
        "capacity_admission": status,
    })


def test_capacity_guard_fails_before_claim_when_admission_key_is_invalid(tmp_path):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path, enabled=True), hermes_home=tmp_path
    )
    opened = []
    reporter = object.__new__(dispatcher.HealthReporter)
    reporter.config = config
    reporter.store = SimpleNamespace(
        open_dispatcher_circuit=lambda **kwargs: opened.append(kwargs)
    )
    reporter.workspace_runtime_status = lambda: {"required": True, "ready": True}

    def invalid_key():
        raise dispatcher.RcaProdAdmissionError(
            "rca_prod_hmac_key_invalid", retryable=False
        )

    reporter._admission_key_fingerprint_observer = invalid_key

    outcome = reporter.dispatch_guard_outcome()

    assert outcome.status == "capacity_authorization_unavailable"
    assert outcome.error_code == "rca_prod_hmac_key_invalid"
    assert opened[0]["reason_code"] == "dispatcher_prod_admission_invalid"


def test_runtime_closure_includes_prod_admission_and_delivery_runtime():
    assert {
        "gateway/__init__.py",
        "gateway/pnc_rca_prod_admission.py",
        "gateway/record_only/runtime.py",
        "gateway/record_only/transport.py",
        "scripts/pnc_completion_notice_relay.py",
        "scripts/pnc_foxglove_delivery.py",
        "scripts/pnc_vm_task_sync.py",
        "tools/__init__.py",
    }.issubset(RCA_RUNTIME_RELATIVE_FILES)
    assert (
        "vm_task_service_rca_prod_admission_blocked" in dispatcher._SERVICE_ERROR_CODES
    )
    assert (
        "vm_task_service_rca_prod_admission_blocked"
        in dispatcher._GLOBAL_CIRCUIT_ERROR_CODES
    )


def test_vm_submit_fence_normalizes_stale_control_store_conflict(monkeypatch):
    fence = {"state": "issued", "activation_epoch_id": "epoch-old"}
    bundle = SimpleNamespace(
        snapshot=SimpleNamespace(write_fence=fence),
        creator_source_envelope=SimpleNamespace(),
    )
    monkeypatch.setattr(
        dispatcher,
        "validate_snapshot_execution_bundle",
        lambda value: value,
    )
    monkeypatch.setattr(
        dispatcher,
        "validate_write_fence_source_binding",
        lambda *_args, **_kwargs: {
            "issue_target": "issue",
            "thread_target": None,
            "chat_id": "chat",
            "target_set_sha256": "1" * 64,
        },
    )
    monkeypatch.setattr(
        dispatcher, "validate_write_fence", lambda *_args, **_kwargs: None
    )

    class StaleStore:
        def validate_external_write_fence_binding(self, _fence):
            raise dispatcher.RecordConflictError(
                "external_write_fence_epoch_not_current"
            )

    with pytest.raises(dispatcher.ExternalWriteFenceError) as raised:
        dispatcher._validate_vm_submit_fence(
            bundle=bundle,
            admission=SimpleNamespace(
                submission_key="submission",
                business_key="business",
                generation=1,
            ),
            now=datetime.now(timezone.utc),
            control_store=StaleStore(),
        )

    assert raised.value.code == "external_write_fence_epoch_not_current"


@pytest.mark.parametrize(
    "error_code",
    [
        "vm_task_service_rca_prod_admission_blocked",
        "vm_task_service_rca_prod_command_invalid",
    ],
)
def test_prod_admission_failure_routes_to_global_circuit(monkeypatch, error_code):
    assert error_code in dispatcher._SERVICE_ERROR_CODES
    assert error_code in dispatcher._GLOBAL_CIRCUIT_ERROR_CODES
    instance = object.__new__(dispatcher.OutboxDispatcher)
    claim = object()
    opened = []
    monkeypatch.setattr(
        instance,
        "_open_circuit",
        lambda actual_claim, code, detail: (
            opened.append((actual_claim, code, detail)) or "opened"
        ),
    )
    monkeypatch.setattr(
        instance,
        "_retry",
        lambda *_args, **_kwargs: pytest.fail("global admission failure was retried"),
    )
    monkeypatch.setattr(
        instance,
        "_quarantine",
        lambda *_args, **_kwargs: pytest.fail(
            "global admission failure quarantined one issue"
        ),
    )

    result = instance._handle_dispatch_error(
        claim,
        error_code,
        "rca_prod_hmac_key_invalid",
    )

    assert result == "opened"
    assert opened == [
        (
            claim,
            error_code,
            "rca_prod_hmac_key_invalid",
        )
    ]
