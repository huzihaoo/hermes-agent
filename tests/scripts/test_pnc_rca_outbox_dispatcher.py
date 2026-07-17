from __future__ import annotations

from types import SimpleNamespace

from gateway.pnc_rca_runtime_identity import RCA_RUNTIME_RELATIVE_FILES
from scripts import pnc_rca_outbox_dispatcher as dispatcher


def _config_env(tmp_path) -> dict[str, str]:
    return {
        "HERMES_RCA_OUTBOX_DISPATCH_ENABLED": "false",
        "HERMES_RCA_OUTBOX_CONTROL_DB_PATH": str(tmp_path / "control.sqlite3"),
        "HERMES_RCA_OUTBOX_HEALTH_PATH": str(tmp_path / "health.json"),
        "HERMES_RCA_PROD_CAPACITY_MODE": "bootstrap",
        "HERMES_RCA_PROD_RELEASE_ID": "retired-release",
        "HERMES_RCA_PROD_BOOTSTRAP_EPOCH_ID": "retired-epoch",
    }


def test_config_omits_retired_release_and_activation_gates(tmp_path):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )

    public = config.public_dict()
    assert "activation_required" not in public
    assert "capacity_mode" not in public
    assert "release_id" not in public
    assert "bootstrap_epoch_id" not in public


def test_default_submit_uses_fixed_functional_service_contract(monkeypatch):
    calls = []

    def fake_submit(**kwargs):
        calls.append(kwargs)
        return {"success": True}

    monkeypatch.setattr("tools.vm_task_tool.vm_task_submit_service", fake_submit)
    admission = object()
    request = SimpleNamespace(toolchain={})

    assert dispatcher.default_submit(admission, request) == {"success": True}
    assert calls == [
        {
            "service_id": dispatcher.DEFAULT_SERVICE_ID,
            "capability": dispatcher.SERVICE_CAPABILITY,
            "operation": dispatcher.SERVICE_OPERATION,
            "admission": admission,
            "execution_request": request,
            "reconcile_only": False,
        }
    ]


def test_default_submit_preserves_reconcile_only_dedupe(monkeypatch):
    calls = []
    monkeypatch.setattr(
        "tools.vm_task_tool.vm_task_submit_service",
        lambda **kwargs: calls.append(kwargs) or {"success": True},
    )

    request = SimpleNamespace(
        toolchain={"derived_capacity_reservation": {"status": "released"}}
    )
    dispatcher.default_submit(object(), request)

    assert calls[0]["reconcile_only"] is True


def test_runtime_closure_excludes_retired_release_modules():
    retired = {
        "gateway/pnc_rca_capacity_runtime.py",
        "gateway/pnc_rca_capacity_sample_evidence.py",
        "gateway/pnc_rca_capacity_transition.py",
        "gateway/pnc_rca_prod_admission.py",
        "gateway/pnc_rca_prod_bootstrap.py",
    }
    assert retired.isdisjoint(RCA_RUNTIME_RELATIVE_FILES)
