from __future__ import annotations

from pathlib import PurePosixPath
from types import SimpleNamespace

from scripts import pnc_rca_delivery_collector as collector
from scripts.pnc_foxglove_delivery import canonical_viz_mcap_path


def _config_env(tmp_path) -> dict[str, str]:
    return {
        "HERMES_RCA_DELIVERY_COLLECTOR_ENABLED": "true",
        "HERMES_RCA_DELIVERY_COLLECTOR_CONTROL_DB_PATH": str(
            tmp_path / "control.sqlite3"
        ),
        "HERMES_RCA_DELIVERY_COLLECTOR_HEALTH_PATH": str(tmp_path / "health.json"),
        "HERMES_RCA_DELIVERY_COLLECTOR_SSH_MINI_AGENT": "/safe/ssh-mini-agent",
        "HERMES_RCA_DELIVERY_COLLECTOR_ARTIFACT_READ_TIMEOUT_SECONDS": "30",
        "HERMES_RCA_DELIVERY_COLLECTOR_LEASE_SECONDS": "60",
        "HERMES_RCA_DELIVERY_COLLECTOR_CAPACITY_SAMPLE_ENABLED": "true",
        "HERMES_RCA_DELIVERY_COLLECTOR_ACTIVATION_REQUIRED": "true",
    }


def test_remote_bundle_reader_uses_formal_viz_publication_root():
    submission_key = "g1q3-rca-s1-" + "a" * 64
    formal_root = str(PurePosixPath(canonical_viz_mcap_path(submission_key)).parent)
    script = collector._remote_bundle_script(submission_key)

    assert f"FORMAL_VIZ_ROOT = {formal_root!r}" in script
    assert "FORMAL_VIZ_ROOT = posixpath.normpath(ROOT)" not in script


def test_config_exposes_capacity_sampling_without_restoring_activation_gate(tmp_path):
    config = collector.CollectorConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )

    public = config.public_dict()
    assert "activation_required" not in public
    assert public["capacity_sample_enabled"] is True
    assert public["capacity_sample_batch_size"] == 20


def test_collect_batch_collects_delivery_then_capacity_samples():
    instance = collector.DeliveryCollector.__new__(collector.DeliveryCollector)
    instance.config = SimpleNamespace(batch_size=3)
    instance.stats = collector.CollectorStats()
    instance.backfill = lambda: 0
    outcomes = iter(
        [
            collector.CollectOutcome(status="running"),
            collector.CollectOutcome(status="idle"),
        ]
    )
    instance.collect_one = lambda: next(outcomes)
    capacity_calls = []
    instance.collect_capacity_samples = lambda: capacity_calls.append(True)

    result = instance.collect_batch()

    assert [item.status for item in result] == ["running", "idle"]
    assert capacity_calls == [True]


def test_collector_stats_expose_capacity_counters_without_activation_counter():
    public = collector.asdict(collector.CollectorStats())
    assert "activation_blocked" not in public
    assert public["capacity_scanned"] == 0
    assert public["capacity_eligible"] == 0
    assert public["capacity_appended"] == 0
    assert public["capacity_rejected"] == 0
    assert public["capacity_frozen"] == 0
    assert public["capacity_last_error"] == ""
    assert public["stale_lease"] == 0
