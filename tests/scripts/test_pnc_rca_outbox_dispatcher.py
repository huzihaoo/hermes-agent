from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

import pytest

from gateway.pnc_rca_prod_bootstrap import RcaBootstrapAuthorizationError
from gateway.pnc_rca_runtime_identity import RCA_RUNTIME_RELATIVE_FILES
from scripts import pnc_rca_outbox_dispatcher as dispatcher


RELEASE_ID = "rca-v0182-test-release"
EPOCH_ID = "rca-bootstrap-v0182-test-epoch"


def test_storage_admission_uses_isolated_production_runtime():
    assert dispatcher.REMOTE_STORAGE_ADMISSION_MODULE == (
        "/home/mini/.hermes/rca-prod-runtime/releases/"
        "rca-e2e-hotfix-20260721/api/g1q3_rca/storage_admission.py"
    )


def _config_env(tmp_path, *, enabled: bool = False) -> dict[str, str]:
    return {
        "HERMES_RCA_OUTBOX_DISPATCH_ENABLED": str(enabled).lower(),
        "HERMES_RCA_OUTBOX_STORAGE_ADMISSION_ENABLED": str(enabled).lower(),
        "HERMES_RCA_OUTBOX_DERIVED_CAPACITY_RESERVATION_ENABLED": str(
            enabled
        ).lower(),
        "HERMES_RCA_OUTBOX_DELIVERY_BACKPRESSURE_ENABLED": str(enabled).lower(),
        "HERMES_RCA_OUTBOX_CONTROL_DB_PATH": str(tmp_path / "control.sqlite3"),
        "HERMES_RCA_OUTBOX_HEALTH_PATH": str(tmp_path / "health.json"),
        "HERMES_RCA_PROD_CAPACITY_MODE": "bootstrap",
        "HERMES_RCA_PROD_RELEASE_ID": RELEASE_ID,
        "HERMES_RCA_PROD_BOOTSTRAP_EPOCH_ID": EPOCH_ID,
    }


def _authorization() -> dict[str, object]:
    return {
        "authorization_ready": True,
        "capacity_mode": "bootstrap",
        "bootstrap_epoch_id": EPOCH_ID,
        "release_approval_id": RELEASE_ID,
        "started_at": "2026-07-20T00:00:00+00:00",
        "deadline": "2026-07-24T00:00:00+00:00",
        "receipt_fingerprint": "1" * 64,
        "authorization_receipt_sha256": "2" * 64,
        "active_release_binding_sha256": "3" * 64,
        "candidate_env_sha256": "4" * 64,
        "release_bom_sha256": "5" * 64,
        "approval_evidence_sha256": "6" * 64,
    }


def test_config_requires_and_projects_production_capacity_binding(tmp_path):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )

    public = config.public_dict()
    assert public["capacity_mode"] == "bootstrap"
    assert public["release_id"] == RELEASE_ID
    assert public["bootstrap_epoch_id"] == EPOCH_ID
    assert config.active_release_binding_path == (
        tmp_path / "active-release-binding.json"
    )
    assert config.live_env_path == tmp_path / ".env"


def test_config_fails_closed_without_bootstrap_capacity_mode(tmp_path):
    env = _config_env(tmp_path)
    env.pop("HERMES_RCA_PROD_CAPACITY_MODE")

    with pytest.raises(ValueError, match="must be exactly bootstrap"):
        dispatcher.DispatcherConfig.from_env(env, hermes_home=tmp_path)


def test_steady_capacity_remains_disabled_until_health_is_implemented(tmp_path):
    env = _config_env(tmp_path)
    env["HERMES_RCA_PROD_CAPACITY_MODE"] = "steady"

    with pytest.raises(ValueError, match="must be exactly bootstrap"):
        dispatcher.DispatcherConfig.from_env(env, hermes_home=tmp_path)


def test_default_submit_uses_bound_bootstrap_contract(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(
        "tools.vm_task_tool.vm_task_submit_service",
        lambda **kwargs: calls.append(kwargs) or {"success": True},
    )
    monkeypatch.setattr(
        dispatcher, "_load_bound_bootstrap_authorization", lambda _config: _authorization()
    )
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )
    admission = object()
    request = SimpleNamespace(toolchain={})

    assert dispatcher.default_submit(admission, request, config=config) == {
        "success": True
    }
    assert calls == [
        {
            "service_id": dispatcher.DEFAULT_SERVICE_ID,
            "capability": dispatcher.SERVICE_CAPABILITY,
            "operation": dispatcher.SERVICE_OPERATION,
            "admission": admission,
            "execution_request": request,
            "reconcile_only": False,
            "capacity_mode": "bootstrap",
            "bootstrap_epoch_id": EPOCH_ID,
            "release_bom_sha256": "5" * 64,
            "bootstrap_started_at": "2026-07-20T00:00:00+00:00",
            "bootstrap_deadline": "2026-07-24T00:00:00+00:00",
            "bootstrap_authorization_fingerprint": "1" * 64,
            "active_release_binding_sha256": "3" * 64,
        }
    ]


def test_default_submit_fails_before_vm_boundary_on_binding_error(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(
        "tools.vm_task_tool.vm_task_submit_service",
        lambda **_kwargs: pytest.fail("invalid bootstrap binding reached VM boundary"),
    )

    def invalid(_config):
        raise RcaBootstrapAuthorizationError("rca_active_release_binding_invalid")

    monkeypatch.setattr(dispatcher, "_load_bound_bootstrap_authorization", invalid)
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )

    with pytest.raises(dispatcher.DispatchCircuitError) as raised:
        dispatcher.default_submit(
            object(), SimpleNamespace(toolchain={}), config=config
        )
    assert raised.value.code == "dispatcher_bootstrap_authorization_invalid"
    assert raised.value.detail == "rca_active_release_binding_invalid"


def test_default_submit_preserves_reconcile_only_dedupe(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(
        "tools.vm_task_tool.vm_task_submit_service",
        lambda **kwargs: calls.append(kwargs) or {"success": True},
    )
    monkeypatch.setattr(
        dispatcher, "_load_bound_bootstrap_authorization", lambda _config: _authorization()
    )
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )
    request = SimpleNamespace(
        toolchain={"derived_capacity_reservation": {"status": "released"}}
    )

    dispatcher.default_submit(object(), request, config=config)

    assert calls[0]["reconcile_only"] is True


def test_capacity_health_projection_is_release_bound(tmp_path):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path, enabled=True), hermes_home=tmp_path
    )
    reporter = object.__new__(dispatcher.HealthReporter)
    reporter.config = config
    reporter._bootstrap_authorization_observer = _authorization
    reporter._admission_key_fingerprint_observer = lambda: "7" * 64

    status = reporter.capacity_admission_status()

    assert status["required"] is True
    assert status["ready"] is True
    assert status["authorization"]["release_approval_id"] == RELEASE_ID
    assert dispatcher._health_capacity_admission_ok(
        {
            "enabled": True,
            "config": config.public_dict(),
            "capacity_admission": status,
        }
    )


def test_capacity_guard_opens_circuit_when_bootstrap_binding_is_unavailable(tmp_path):
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
    reporter._admission_key_fingerprint_observer = lambda: "7" * 64

    def unavailable():
        raise RcaBootstrapAuthorizationError("rca_bootstrap_expired_or_deadline_invalid")

    reporter._bootstrap_authorization_observer = unavailable

    outcome = reporter.dispatch_guard_outcome()

    assert outcome.status == "capacity_authorization_unavailable"
    assert outcome.error_code == "rca_bootstrap_expired_or_deadline_invalid"
    assert opened == [
        {
            "reason_code": "dispatcher_bootstrap_authorization_invalid",
            "reason_detail": "rca_bootstrap_expired_or_deadline_invalid",
        }
    ]


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
    reporter._bootstrap_authorization_observer = _authorization

    def invalid_key():
        raise dispatcher.RcaProdAdmissionError(
            "rca_prod_hmac_key_invalid", retryable=False
        )

    reporter._admission_key_fingerprint_observer = invalid_key

    outcome = reporter.dispatch_guard_outcome()

    assert outcome.status == "capacity_authorization_unavailable"
    assert outcome.error_code == "rca_prod_hmac_key_invalid"
    assert opened[0]["reason_code"] == "dispatcher_bootstrap_authorization_invalid"


def test_runtime_closure_includes_prod_admission_but_not_retired_transition():
    assert {
        "gateway/__init__.py",
        "gateway/pnc_rca_prod_admission.py",
        "gateway/pnc_rca_prod_bootstrap.py",
        "gateway/record_only/runtime.py",
        "gateway/record_only/transport.py",
        "scripts/pnc_completion_notice_relay.py",
        "scripts/pnc_foxglove_delivery.py",
        "scripts/pnc_vm_task_sync.py",
        "tools/__init__.py",
    }.issubset(RCA_RUNTIME_RELATIVE_FILES)
    assert {
        "gateway/pnc_rca_capacity_runtime.py",
        "gateway/pnc_rca_capacity_sample_evidence.py",
        "gateway/pnc_rca_capacity_transition.py",
    }.isdisjoint(RCA_RUNTIME_RELATIVE_FILES)
    assert (
        "vm_task_service_rca_prod_admission_blocked"
        in dispatcher._SERVICE_ERROR_CODES
    )
    assert (
        "vm_task_service_rca_prod_admission_blocked"
        in dispatcher._GLOBAL_CIRCUIT_ERROR_CODES
    )


@pytest.mark.parametrize(
    "error_code",
    [
        "vm_task_service_rca_prod_admission_blocked",
        "vm_task_service_capacity_mode_invalid",
        "vm_task_service_bootstrap_binding_invalid",
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
        lambda actual_claim, code, detail: opened.append(
            (actual_claim, code, detail)
        )
        or "opened",
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
