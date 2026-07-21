from __future__ import annotations

import json

import pytest

from gateway.pnc_rca_delivery_quarantine_baseline import (
    BASELINE_PATH_ENV,
    BASELINE_SHA256_ENV,
    PROD_BOOTSTRAP_EPOCH_ID_ENV,
    PROD_RELEASE_ID_ENV,
)
from gateway.pnc_rca_runtime_identity import (
    GATEWAY_LOADED_DEPENDENCIES,
    RCA_RUNTIME_RELATIVE_FILES,
)
from scripts import pnc_rca_delivery_collector as collector
from scripts import pnc_rca_delivery_dispatcher as dispatcher
from tests.gateway.test_pnc_rca_delivery_quarantine_baseline import _build_bundle


def test_delivery_quarantine_module_is_in_runtime_identity_closure():
    assert (
        "gateway/pnc_rca_delivery_quarantine_baseline.py" in RCA_RUNTIME_RELATIVE_FILES
    )
    assert (
        "gateway/pnc_rca_delivery_quarantine_migration.py" in RCA_RUNTIME_RELATIVE_FILES
    )


def test_disabled_fresh_check_config_remains_backward_compatible(
    tmp_path, monkeypatch, capsys
):
    collector_config = collector.CollectorConfig.from_env({}, hermes_home=tmp_path)
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
    assert json.loads(capsys.readouterr().out)["quarantine_baseline"]["state"] == (
        "disabled"
    )

    dispatcher_config = dispatcher.DispatcherConfig.from_env({}, hermes_home=tmp_path)
    monkeypatch.setattr(
        dispatcher, "load_delivery_dispatcher_environment", lambda: tmp_path
    )
    monkeypatch.setattr(
        dispatcher.DispatcherConfig,
        "from_env",
        staticmethod(lambda: dispatcher_config),
    )
    assert dispatcher.main(["--check-config"]) == 0
    assert json.loads(capsys.readouterr().out)["quarantine_baseline"]["state"] == (
        "disabled"
    )


def _collector_config(bundle, tmp_path, *, sha256, enabled=False):
    return collector.CollectorConfig.from_env(
        {
            "HERMES_RCA_DELIVERY_COLLECTOR_ENABLED": str(enabled).lower(),
            "HERMES_RCA_DELIVERY_COLLECTOR_CONTROL_DB_PATH": str(
                bundle["store"].db_path
            ),
            "HERMES_RCA_DELIVERY_COLLECTOR_HEALTH_PATH": str(
                tmp_path / "collector-health.json"
            ),
            BASELINE_PATH_ENV: str(bundle["baseline_path"]),
            BASELINE_SHA256_ENV: sha256,
            PROD_RELEASE_ID_ENV: bundle["core"]["release_id"],
            PROD_BOOTSTRAP_EPOCH_ID_ENV: bundle["bootstrap_epoch_id"],
        },
        hermes_home=tmp_path,
    )


def _dispatcher_config(bundle, tmp_path, *, sha256, enabled=False):
    return dispatcher.DispatcherConfig.from_env(
        {
            "HERMES_RCA_DELIVERY_DISPATCHER_ENABLED": str(enabled).lower(),
            "HERMES_RCA_DELIVERY_DISPATCHER_CONTROL_DB_PATH": str(
                bundle["store"].db_path
            ),
            "HERMES_RCA_DELIVERY_DISPATCHER_HEALTH_PATH": str(
                tmp_path / "dispatcher-health.json"
            ),
            BASELINE_PATH_ENV: str(bundle["baseline_path"]),
            BASELINE_SHA256_ENV: sha256,
            PROD_RELEASE_ID_ENV: bundle["core"]["release_id"],
            PROD_BOOTSTRAP_EPOCH_ID_ENV: bundle["bootstrap_epoch_id"],
        },
        hermes_home=tmp_path,
    )


@pytest.mark.parametrize("valid", [True, False])
def test_collector_check_config_returns_baseline_readiness_rc(
    tmp_path, monkeypatch, capsys, valid
):
    bundle = _build_bundle(tmp_path)
    sha256 = bundle["baseline_sha256"] if valid else "0" * 64
    config = _collector_config(bundle, tmp_path, sha256=sha256, enabled=True)
    monkeypatch.setattr(collector, "load_collector_environment", lambda: tmp_path)
    monkeypatch.setattr(
        collector.CollectorConfig,
        "from_env",
        staticmethod(lambda: config),
    )
    monkeypatch.setattr(
        collector,
        "probe_remote_css_parser",
        lambda *_args, **_kwargs: collector.expected_remote_css_runtime_dependency(),
    )

    rc = collector.main(["--check-config"])
    payload = json.loads(capsys.readouterr().out)

    assert rc == (0 if valid else 2)
    assert payload["ok"] is valid
    assert payload["quarantine_baseline"]["ready"] is valid


@pytest.mark.parametrize("valid", [True, False])
def test_dispatcher_check_config_returns_baseline_readiness_rc(
    tmp_path, monkeypatch, capsys, valid
):
    bundle = _build_bundle(tmp_path)
    sha256 = bundle["baseline_sha256"] if valid else "0" * 64
    config = _dispatcher_config(bundle, tmp_path, sha256=sha256, enabled=True)
    monkeypatch.setattr(
        dispatcher, "load_delivery_dispatcher_environment", lambda: tmp_path
    )
    monkeypatch.setattr(
        dispatcher.DispatcherConfig,
        "from_env",
        staticmethod(lambda: config),
    )

    rc = dispatcher.main(["--check-config"])
    payload = json.loads(capsys.readouterr().out)

    assert rc == (0 if valid else 2)
    assert payload["ok"] is valid
    assert payload["quarantine_baseline"]["ready"] is valid


@pytest.mark.parametrize("valid", [True, False])
def test_collector_and_dispatcher_health_validate_same_baseline(
    tmp_path, monkeypatch, valid
):
    bundle = _build_bundle(tmp_path)
    sha256 = bundle["baseline_sha256"] if valid else "0" * 64
    expected_dependency = collector.expected_remote_css_runtime_dependency()

    original_identity_builder = collector.build_runtime_identity

    def build_identity(**kwargs):
        kwargs["loaded_dependencies"] = GATEWAY_LOADED_DEPENDENCIES
        return original_identity_builder(**kwargs)

    monkeypatch.setattr(collector, "build_runtime_identity", build_identity)
    monkeypatch.setattr(dispatcher, "build_runtime_identity", build_identity)
    collector_config = _collector_config(bundle, tmp_path, sha256=sha256, enabled=True)
    collector.HealthReporter(
        collector_config,
        bundle["store"],
        remote_css_probe=lambda *_args, **_kwargs: expected_dependency,
    ).write(
        state="idle",
        stats=collector.CollectorStats(),
        refresh_dependencies=False,
    )
    dispatcher_config = _dispatcher_config(
        bundle, tmp_path, sha256=sha256, enabled=True
    )
    dispatcher.HealthReporter(dispatcher_config, bundle["store"]).write(
        state="idle",
        stats=dispatcher.DispatchStats(),
    )

    collector_health = json.loads(collector_config.health_path.read_text())
    dispatcher_health = json.loads(dispatcher_config.health_path.read_text())
    for payload in (collector_health, dispatcher_health):
        assert payload["healthy"] is valid
        assert payload["store"]["delivery_quarantine"]["ready"] is valid
