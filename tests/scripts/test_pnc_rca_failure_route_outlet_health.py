from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from scripts import pnc_rca_delivery_collector as collector
from scripts.pnc_rca_failure_route_outlet import (
    OUTLET_INSPECTION_SCHEMA_VERSION,
    OUTLET_SCHEMA_VERSION,
    FailureRouteOutlet,
    FailureRouteOutletSchemaError,
)
from tests.scripts.test_pnc_rca_failure_taxonomy_audit import _routed_db


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _config(
    tmp_path: Path,
    *,
    control_db_path: Path,
    outlet_root: Path,
) -> collector.CollectorConfig:
    return collector.CollectorConfig.from_env(
        {
            "HERMES_RCA_DELIVERY_COLLECTOR_ENABLED": "true",
            "HERMES_RCA_DELIVERY_COLLECTOR_CONTROL_DB_PATH": str(control_db_path),
            "HERMES_RCA_DELIVERY_COLLECTOR_HEALTH_PATH": str(tmp_path / "health.json"),
            "HERMES_RCA_DELIVERY_COLLECTOR_SSH_MINI_AGENT": ("/safe/ssh-mini-agent"),
            "HERMES_RCA_DELIVERY_COLLECTOR_ARTIFACT_READ_TIMEOUT_SECONDS": "30",
            "HERMES_RCA_DELIVERY_COLLECTOR_LEASE_SECONDS": "60",
            "HERMES_RCA_DELIVERY_COLLECTOR_FAILURE_ROUTE_OUTLET_ROOT": str(outlet_root),
        },
        hermes_home=tmp_path,
    )


def test_inspect_missing_outlet_is_ready_without_materializing(tmp_path):
    db_path = _routed_db(tmp_path / "source")
    outlet_root = tmp_path / "missing-outlet"
    source_sha256 = _sha256(db_path)

    inspection = FailureRouteOutlet.inspect(db_path, outlet_root)

    assert inspection["schema_version"] == OUTLET_INSPECTION_SCHEMA_VERSION
    assert inspection["status"] == "uninitialized"
    assert inspection["ready"] is True
    assert inspection["initialized"] is False
    assert inspection["outlet_schema_version"] == ""
    assert inspection["read_only"] is True
    assert inspection["external_writes"] is False
    assert inspection["error"] == ""
    assert not outlet_root.exists()
    assert _sha256(db_path) == source_sha256


def test_validate_initialized_outlet_is_immutable_and_schema_bound(tmp_path):
    db_path = _routed_db(tmp_path / "source")
    outlet_root = tmp_path / "outlet"
    FailureRouteOutlet(db_path, outlet_root)
    outlet_db = outlet_root / "outlet.sqlite3"
    files_before = sorted(path.name for path in outlet_root.iterdir())
    outlet_sha256 = _sha256(outlet_db)
    outlet_mtime_ns = outlet_db.stat().st_mtime_ns

    inspection = FailureRouteOutlet.validate(db_path, outlet_root)

    assert inspection["status"] == "ready"
    assert inspection["ready"] is True
    assert inspection["initialized"] is True
    assert inspection["outlet_schema_version"] == OUTLET_SCHEMA_VERSION
    assert sorted(path.name for path in outlet_root.iterdir()) == files_before
    assert _sha256(outlet_db) == outlet_sha256
    assert outlet_db.stat().st_mtime_ns == outlet_mtime_ns


@pytest.mark.parametrize(
    ("target", "error"),
    [
        ("root_file", "failure_route_outlet_root_invalid"),
        ("root_symlink", "failure_route_outlet_root_invalid"),
        ("db_symlink", "failure_route_outlet_db_invalid"),
    ],
)
def test_inspect_rejects_invalid_or_symlink_paths(tmp_path, target, error):
    db_path = _routed_db(tmp_path / "source")
    outlet_root = tmp_path / "outlet"
    if target == "root_file":
        outlet_root.write_text("not a directory", encoding="utf-8")
    elif target == "root_symlink":
        real_root = tmp_path / "real-outlet"
        real_root.mkdir()
        outlet_root.symlink_to(real_root, target_is_directory=True)
    else:
        outlet_root.mkdir()
        target_db = tmp_path / "target.sqlite3"
        target_db.write_bytes(b"not an outlet")
        (outlet_root / "outlet.sqlite3").symlink_to(target_db)

    inspection = FailureRouteOutlet.inspect(db_path, outlet_root)

    assert inspection["ready"] is False
    assert inspection["status"] == "invalid"
    assert inspection["error"] == error
    with pytest.raises(FailureRouteOutletSchemaError, match=error):
        FailureRouteOutlet.validate(db_path, outlet_root)


def test_inspect_rejects_schema_drift_without_changing_database(tmp_path):
    db_path = _routed_db(tmp_path / "source")
    outlet_root = tmp_path / "outlet"
    FailureRouteOutlet(db_path, outlet_root)
    outlet_db = outlet_root / "outlet.sqlite3"
    writer = sqlite3.connect(outlet_db)
    try:
        writer.execute(
            "UPDATE failure_route_outlet_meta SET value = 'future' "
            "WHERE key = 'schema_version'"
        )
        writer.commit()
        wal_path = outlet_db.with_name(outlet_db.name + "-wal")
        files_before = sorted(path.name for path in outlet_root.iterdir())
        before_sha256 = _sha256(outlet_db)
        wal_sha256 = _sha256(wal_path)

        inspection = FailureRouteOutlet.inspect(db_path, outlet_root)

        assert inspection["ready"] is False
        assert inspection["error"] == "failure_route_outlet_schema_not_current"
        assert sorted(path.name for path in outlet_root.iterdir()) == files_before
        assert _sha256(outlet_db) == before_sha256
        assert _sha256(wal_path) == wal_sha256
    finally:
        writer.close()


def test_inspect_rejects_incomplete_current_schema(tmp_path):
    db_path = _routed_db(tmp_path / "source")
    outlet_root = tmp_path / "outlet"
    outlet_root.mkdir(mode=0o700)
    outlet_db = outlet_root / "outlet.sqlite3"
    with sqlite3.connect(outlet_db) as conn:
        conn.executescript(
            "CREATE TABLE failure_route_outlet_meta ("
            "key TEXT PRIMARY KEY, value TEXT NOT NULL);"
            "INSERT INTO failure_route_outlet_meta(key, value) "
            f"VALUES('schema_version', '{OUTLET_SCHEMA_VERSION}');"
        )
    outlet_db.chmod(0o600)
    before_sha256 = _sha256(outlet_db)

    inspection = FailureRouteOutlet.inspect(db_path, outlet_root)

    assert inspection["ready"] is False
    assert inspection["error"] == "failure_route_outlet_schema_invalid"
    assert _sha256(outlet_db) == before_sha256


@pytest.mark.parametrize(
    ("target", "mode", "error"),
    [
        ("root", 0o500, "failure_route_outlet_root_permission_denied"),
        ("db", 0o400, "failure_route_outlet_db_permission_denied"),
    ],
)
def test_inspect_fails_closed_on_insufficient_permissions(
    tmp_path, target, mode, error
):
    db_path = _routed_db(tmp_path / "source")
    outlet_root = tmp_path / "outlet"
    FailureRouteOutlet(db_path, outlet_root)
    selected = outlet_root if target == "root" else outlet_root / "outlet.sqlite3"
    selected.chmod(mode)
    try:
        inspection = FailureRouteOutlet.inspect(db_path, outlet_root)
    finally:
        selected.chmod(0o700 if target == "root" else 0o600)

    assert inspection["ready"] is False
    assert inspection["error"] == error


@pytest.mark.parametrize("invalid", [False, True])
def test_health_reporter_binds_read_only_outlet_readiness(
    tmp_path, monkeypatch, invalid
):
    db_path = _routed_db(tmp_path / "source")
    outlet_root = tmp_path / "outlet"
    if invalid:
        outlet_root.write_text("invalid", encoding="utf-8")
    config = _config(
        tmp_path,
        control_db_path=db_path,
        outlet_root=outlet_root,
    )
    source_sha256 = _sha256(db_path)
    monkeypatch.setattr(
        collector,
        "build_runtime_identity",
        lambda **_kwargs: SimpleNamespace(to_dict=lambda: {}),
    )
    reporter = collector.HealthReporter(
        config,
        SimpleNamespace(health=lambda **_kwargs: {"ok": True}),
        remote_css_probe=lambda *_args, **_kwargs: (
            collector.expected_remote_css_runtime_dependency()
        ),
    )

    reporter.write(
        state="idle",
        stats=collector.CollectorStats(),
        refresh_dependencies=False,
    )

    health = json.loads(config.health_path.read_text(encoding="utf-8"))
    outlet = health["dependencies"]["failure_route_outlet"]
    assert health["healthy"] is (not invalid)
    assert reporter.dependencies_ready is (not invalid)
    assert outlet["ready"] is (not invalid)
    assert outlet["status"] == ("invalid" if invalid else "uninitialized")
    assert _sha256(db_path) == source_sha256
    if not invalid:
        assert not outlet_root.exists()


def test_collector_loop_blocks_work_when_outlet_preflight_fails(tmp_path, monkeypatch):
    db_path = _routed_db(tmp_path / "source")
    outlet_root = tmp_path / "outlet"
    outlet_root.write_text("invalid", encoding="utf-8")
    config = _config(
        tmp_path,
        control_db_path=db_path,
        outlet_root=outlet_root,
    )
    monkeypatch.setattr(
        collector,
        "build_runtime_identity",
        lambda **_kwargs: SimpleNamespace(to_dict=lambda: {}),
    )
    collect_calls = []
    instance = SimpleNamespace(
        config=config,
        store=SimpleNamespace(health=lambda **_kwargs: {"ok": True}),
        stats=collector.CollectorStats(),
        runtime_identity=None,
        collect_batch=lambda: collect_calls.append(True),
    )

    rc = collector.run_collector_loop(
        instance,
        once=True,
        remote_css_probe=lambda *_args, **_kwargs: (
            collector.expected_remote_css_runtime_dependency()
        ),
    )

    health = json.loads(config.health_path.read_text(encoding="utf-8"))
    assert rc == 2
    assert collect_calls == []
    assert health["healthy"] is False
    assert health["dependency_error"] == "failure_route_outlet_root_invalid"


@pytest.mark.parametrize("invalid", [False, True])
def test_check_config_outlet_preflight_is_read_only(
    tmp_path, monkeypatch, capsys, invalid
):
    db_path = tmp_path / "control.sqlite3"
    db_path.write_bytes(b"immutable-control-db")
    outlet_root = tmp_path / "outlet"
    if invalid:
        outlet_root.write_text("invalid", encoding="utf-8")
    config = _config(
        tmp_path,
        control_db_path=db_path,
        outlet_root=outlet_root,
    )
    source_sha256 = _sha256(db_path)
    monkeypatch.setattr(collector, "load_collector_environment", lambda: tmp_path)
    monkeypatch.setattr(
        collector.CollectorConfig,
        "from_env",
        staticmethod(lambda: config),
    )
    monkeypatch.setattr(
        collector,
        "RcaDeliveryStore",
        lambda *_args, **_kwargs: SimpleNamespace(
            schema_runtime_capability=lambda: {
                "observed_control_schema_version": "pnc_rca_control_store_v14",
                "binary_write_schema_version": "pnc_rca_control_store_v15",
                "mode": "current_write",
                "read_supported": True,
                "write_enabled": True,
                "work_admission_enabled": True,
                "lease_acquisition_enabled": True,
                "external_effect_enabled": True,
            },
            health=lambda **_kwargs: {
                "activation": {"production_ready": True},
            }
        ),
    )
    monkeypatch.setattr(
        collector,
        "validate_bound_resident_release",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        collector,
        "probe_remote_css_parser",
        lambda *_args, **_kwargs: collector.expected_remote_css_runtime_dependency(),
    )

    rc = collector.main(["--check-config"])
    payload = json.loads(capsys.readouterr().out)

    assert rc == (2 if invalid else 0)
    assert payload["ok"] is (not invalid)
    outlet = payload["dependencies"]["failure_route_outlet"]
    assert outlet["ready"] is (not invalid)
    assert outlet["read_only"] is True
    assert outlet["external_writes"] is False
    assert _sha256(db_path) == source_sha256
    if not invalid:
        assert not outlet_root.exists()
