from __future__ import annotations

import json

from gateway.pnc_rca_delivery_quarantine_baseline import (
    BASELINE_PATH_ENV,
    BASELINE_SHA256_ENV,
    PROD_BOOTSTRAP_EPOCH_ID_ENV,
    PROD_RELEASE_ID_ENV,
)
from gateway.pnc_rca_delivery_store import RcaDeliveryStore
from gateway.pnc_rca_runtime_identity import RCA_RUNTIME_RELATIVE_FILES
from scripts import pnc_rca_delivery_collector as collector
from scripts import pnc_rca_delivery_dispatcher as dispatcher


def _legacy_baseline_env(tmp_path) -> dict[str, str]:
    return {
        BASELINE_PATH_ENV: str(tmp_path / "missing-baseline.json"),
        BASELINE_SHA256_ENV: "0" * 64,
        PROD_RELEASE_ID_ENV: "legacy-release-that-must-not-gate",
        PROD_BOOTSTRAP_EPOCH_ID_ENV: "legacy-bootstrap-that-must-not-gate",
    }


def test_delivery_quarantine_modules_are_not_in_runtime_identity_closure():
    assert (
        "gateway/pnc_rca_delivery_quarantine_baseline.py" not in RCA_RUNTIME_RELATIVE_FILES
    )
    assert (
        "gateway/pnc_rca_delivery_quarantine_migration.py" not in RCA_RUNTIME_RELATIVE_FILES
    )
    assert "gateway/pnc_rca_write_fence.py" in RCA_RUNTIME_RELATIVE_FILES


def test_disabled_check_config_ignores_legacy_quarantine_environment(
    tmp_path, monkeypatch, capsys
):
    legacy = _legacy_baseline_env(tmp_path)
    collector_config = collector.CollectorConfig.from_env(
        legacy,
        hermes_home=tmp_path,
    )
    monkeypatch.setattr(collector, "load_collector_environment", lambda: tmp_path)
    monkeypatch.setattr(
        collector.CollectorConfig,
        "from_env",
        staticmethod(lambda: collector_config),
    )
    monkeypatch.setattr(
        collector,
        "probe_remote_css_parser",
        lambda *_args, **_kwargs: collector.expected_remote_css_runtime_dependency(),
    )

    assert collector.main(["--check-config"]) == 0
    collector_payload = json.loads(capsys.readouterr().out)
    assert collector_payload["ok"] is True
    assert collector_payload["release"] == {}
    assert "quarantine_baseline" not in collector_payload
    assert not any(
        "quarantine" in key for key in collector_payload["config"]
    )

    dispatcher_config = dispatcher.DispatcherConfig.from_env(
        legacy,
        hermes_home=tmp_path,
    )
    monkeypatch.setattr(
        dispatcher, "load_delivery_dispatcher_environment", lambda: tmp_path
    )
    monkeypatch.setattr(
        dispatcher.DispatcherConfig,
        "from_env",
        staticmethod(lambda: dispatcher_config),
    )

    assert dispatcher.main(["--check-config"]) == 0
    dispatcher_payload = json.loads(capsys.readouterr().out)
    assert dispatcher_payload["ok"] is True
    assert dispatcher_payload["release"] == {}
    assert "quarantine_baseline" not in dispatcher_payload
    assert not any(
        "quarantine" in key for key in dispatcher_payload["config"]
    )


def test_delivery_health_keeps_legacy_quarantine_out_of_normal_gate(
    tmp_path, monkeypatch
):
    for key, value in _legacy_baseline_env(tmp_path).items():
        monkeypatch.setenv(key, value)
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")

    health = store.health()

    assert health["ok"] is True
    assert "delivery_quarantine" not in health
    assert "quarantine_baseline_invalid" not in health["production_blockers"]
