from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from scripts import pnc_rca_release_scorecard as scorecard


pytestmark = pytest.mark.integration


def test_live_scorecard_reads_real_sources_without_mocks_or_mutation() -> None:
    paths = scorecard._default_paths()
    required = (
        paths.live_manifest,
        paths.active_binding,
        paths.state_root / "control.sqlite3",
        paths.gateway_state,
    )
    if not all(path.exists() for path in required):
        pytest.skip("PNC RCA live sources are not installed")
    before = {
        path: (
            path.stat().st_dev,
            path.stat().st_ino,
            path.stat().st_size,
            path.stat().st_mtime_ns,
        )
        for path in (
            paths.live_manifest,
            paths.active_binding,
            paths.state_root / "control.sqlite3",
        )
    }

    result = scorecard.build_scorecard(paths, as_of=datetime.now(timezone.utc))

    after = {
        path: (
            path.stat().st_dev,
            path.stat().st_ino,
            path.stat().st_size,
            path.stat().st_mtime_ns,
        )
        for path in before
    }
    assert after == before
    assert result["release_status"] == "NOT_GA"
    assert result["ga_claim_allowed"] is False
    assert all(
        result["live"]["fingerprints"][face]["commit"]
        for face in ("host", "pipeline", "worker", "mcap")
    )
    assert {item["profile_id"] for item in result["live"]["profile_readiness"]} == {
        "g1q3",
        "mdrive4",
    }
    assert result["live"]["activation"]["state"]
    assert set(result["live"]["canaries"]) == {"natural_kafka", "feishu_topic"}
    assert result["live"]["real_data"]["row_counts"]["business_triggers"] > 0
    assert result["historical"]["release_lineage"]["today"]["host_count"] > 0
    assert result["historical"]["release_lineage"]["today"]["pipeline_count"] > 0
    assert result["read_only_attestation"]["production_mutation_performed"] is False
