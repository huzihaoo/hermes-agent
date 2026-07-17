from __future__ import annotations

from types import SimpleNamespace

from scripts import pnc_rca_delivery_collector as collector


def _config_env(tmp_path) -> dict[str, str]:
    return {
        "HERMES_RCA_DELIVERY_COLLECTOR_ENABLED": "false",
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


def test_config_omits_retired_capacity_and_activation_gates(tmp_path):
    config = collector.CollectorConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )

    public = config.public_dict()
    assert "activation_required" not in public
    assert "capacity_sample_enabled" not in public
    assert "capacity_sample_batch_size" not in public


def test_collect_batch_only_collects_delivery_work():
    instance = collector.DeliveryCollector.__new__(collector.DeliveryCollector)
    instance.config = SimpleNamespace(batch_size=3)
    instance.backfill = lambda: 0
    outcomes = iter(
        [
            collector.CollectOutcome(status="running"),
            collector.CollectOutcome(status="idle"),
        ]
    )
    instance.collect_one = lambda: next(outcomes)

    result = instance.collect_batch()

    assert [item.status for item in result] == ["running", "idle"]


def test_collector_stats_have_no_capacity_or_activation_counters():
    public = collector.asdict(collector.CollectorStats())
    assert "activation_blocked" not in public
    assert all(not key.startswith("capacity_") for key in public)
    assert public["stale_lease"] == 0
