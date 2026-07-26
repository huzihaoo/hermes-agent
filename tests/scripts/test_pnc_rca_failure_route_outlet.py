from __future__ import annotations

from datetime import timedelta
from dataclasses import replace
import json
from pathlib import Path
import sqlite3

import pytest

from scripts.pnc_rca_failure_route_outlet import (
    FailureRouteOutlet,
    FailureRouteOutletPermanentError,
    StaleFailureRouteOutletLeaseError,
)
from tests.gateway.test_pnc_rca_delivery_store import NOW
from tests.scripts.test_pnc_rca_delivery_collector import (
    _real_terminal_collector,
)
from tests.scripts.test_pnc_rca_failure_taxonomy_audit import _routed_db


def _route_keys(db_path: Path) -> dict[str, str]:
    with sqlite3.connect(db_path) as conn:
        return {
            str(kind): str(route_key)
            for route_key, kind in conn.execute(
                "SELECT route_key, route_kind FROM rca_failure_routes "
                "WHERE route_kind IN ('internal_backlog', 'internal_alert') "
                "ORDER BY route_kind"
            )
        }


def test_internal_routes_settle_idempotently_without_main_effects(tmp_path):
    db_path = _routed_db(tmp_path / "source")
    keys = _route_keys(db_path)
    with sqlite3.connect(db_path) as conn:
        before = {
            "effects": conn.execute(
                "SELECT COUNT(*) FROM rca_delivery_effects"
            ).fetchone()[0],
            "jobs": conn.execute(
                "SELECT COUNT(*) FROM rca_delivery_jobs"
            ).fetchone()[0],
        }
    outlet = FailureRouteOutlet(
        db_path,
        tmp_path / "outlet",
        lease_owner="test-internal-outlet",
    )

    results = [
        outlet.process_route(
            keys["internal_backlog"],
            now=NOW,
        ),
        outlet.process_route(
            keys["internal_alert"],
            now=NOW + timedelta(seconds=1),
        ),
    ]
    assert {result["status"] for result in results} == {"settled"}
    assert all(result["external_effects"] == 0 for result in results)

    duplicate = outlet.process_route(keys["internal_backlog"], now=NOW)
    assert duplicate["status"] == "settled"
    assert duplicate["created"] is False
    assert duplicate["attempt"] == 1

    rows = outlet.list_rows()
    assert {row["route_key"] for row in rows} == set(keys.values())
    assert {row["status"] for row in rows} == {"settled"}
    for row in rows:
        receipt = Path(row["receipt_path"])
        assert receipt.is_file()
        payload = json.loads(receipt.read_text(encoding="utf-8"))
        assert payload["external_effects"] == 0
        assert payload["route_key"] == row["route_key"]

    with sqlite3.connect(db_path) as conn:
        after = {
            "effects": conn.execute(
                "SELECT COUNT(*) FROM rca_delivery_effects"
            ).fetchone()[0],
            "jobs": conn.execute(
                "SELECT COUNT(*) FROM rca_delivery_jobs"
            ).fetchone()[0],
        }
    assert after == before


def test_outlet_retries_then_quarantines_sink_failures(tmp_path):
    db_path = _routed_db(tmp_path / "source")
    route_key = _route_keys(db_path)["internal_backlog"]
    calls = []

    def failing_sink(_claim):
        calls.append(True)
        raise OSError("local sink unavailable")

    outlet = FailureRouteOutlet(
        db_path,
        tmp_path / "outlet",
        lease_owner="test-retry",
        max_attempts=2,
        retry_delays_seconds=(0,),
        receipt_sink=failing_sink,
    )
    first = outlet.process_route(route_key, now=NOW)
    second = outlet.process_route(route_key, now=NOW + timedelta(seconds=1))

    assert first["status"] == "retry_wait"
    assert second["status"] == "quarantined"
    assert len(calls) == 2
    assert outlet.row(route_key)["attempt"] == 2


def test_outlet_rejects_non_internal_route_before_sidecar_write(tmp_path):
    db_path = _routed_db(tmp_path / "source")
    with sqlite3.connect(db_path) as conn:
        route_key = conn.execute(
            "SELECT route_key FROM rca_failure_routes "
            "WHERE route_kind = 'infra_remediation_hold'"
        ).fetchone()[0]
    outlet = FailureRouteOutlet(db_path, tmp_path / "outlet")

    with pytest.raises(
        FailureRouteOutletPermanentError,
        match="failure_route_outlet_external_route_forbidden",
    ):
        outlet.process_route(route_key, now=NOW)
    assert outlet.list_rows() == []


def test_expired_lease_cannot_settle_after_reclaim(tmp_path):
    db_path = _routed_db(tmp_path / "source")
    route_key = _route_keys(db_path)["internal_alert"]
    first = FailureRouteOutlet(
        db_path,
        tmp_path / "outlet",
        lease_owner="owner-a",
        lease_seconds=1,
    )
    second = FailureRouteOutlet(
        db_path,
        tmp_path / "outlet",
        lease_owner="owner-b",
        lease_seconds=1,
    )
    first.sync_route(route_key, now=NOW)
    claim_a = first.claim(route_key=route_key, now=NOW)
    assert claim_a is not None
    claim_b = second.claim(route_key=route_key, now=NOW + timedelta(seconds=2))
    assert claim_b is not None
    with pytest.raises(StaleFailureRouteOutletLeaseError):
        first.settle(
            claim_a,
            first._write_local_receipt(claim_a),
            now=NOW + timedelta(seconds=2),
        )


def test_collector_wires_internal_route_to_local_outlet(tmp_path):
    clock = [NOW]
    instance = _real_terminal_collector(
        tmp_path,
        blocker={"kind": "html_capability_payload_mismatch", "retryable": False},
        clock=clock,
    )
    outcome = instance.collect_one()
    assert outcome.status == "failure_hold"
    outlet_root = instance.config.failure_route_outlet_root
    outlet_db = outlet_root / "outlet.sqlite3"
    assert outlet_db.is_file()
    with sqlite3.connect(outlet_db) as conn:
        row = conn.execute(
            "SELECT status, route_status FROM failure_route_outlets"
        ).fetchone()
    assert row == ("settled", "alert_pending")
    assert instance.stats.internal_outlet_settled == 1


def test_internal_outlet_failure_does_not_remove_bounded_user_fallback(tmp_path):
    clock = [NOW]
    instance = _real_terminal_collector(
        tmp_path,
        blocker={"kind": "html_capability_payload_mismatch", "retryable": False},
        clock=clock,
    )
    invalid_root = tmp_path / "outlet-file"
    invalid_root.write_text("not a directory", encoding="utf-8")
    instance.config = replace(
        instance.config,
        failure_route_outlet_root=invalid_root,
    )

    held = instance.collect_one()
    assert held.status == "failure_hold"
    clock[0] = NOW + timedelta(seconds=1800)
    fallback = instance.collect_one()

    assert fallback.status == "terminal_failed"
    assert instance.stats.internal_outlet_errors >= 1
