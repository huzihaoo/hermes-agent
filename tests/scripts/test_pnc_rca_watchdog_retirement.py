from __future__ import annotations

import json
from pathlib import Path

from scripts.pnc_live_exec import SERVICE_TARGETS


REPO_ROOT = Path(__file__).resolve().parents[2]
RETIRED_LABEL = "local.pnc.watcher-staleness-watchdog"


def test_legacy_rca_watchdog_is_not_distributed() -> None:
    assert not (REPO_ROOT / "local.pnc.watcher-staleness-watchdog.plist").exists()
    assert not (REPO_ROOT / "scripts" / "watcher_staleness_watchdog.sh").exists()

    operational_sources = [
        *REPO_ROOT.glob("*.plist"),
        *(REPO_ROOT / "scripts").glob("*.py"),
        *(REPO_ROOT / "scripts").glob("*.sh"),
    ]
    for path in operational_sources:
        assert RETIRED_LABEL not in path.read_text(encoding="utf-8"), path


def test_generic_release_fingerprint_cli_remains_available() -> None:
    registry = json.loads(
        (
            REPO_ROOT / "gateway" / "assets" / "pnc_stable_target_registry_v1.json"
        ).read_text(encoding="utf-8")
    )
    expected = "hermes_release_fingerprint_check.py"

    assert SERVICE_TARGETS["local.pnc.release-fingerprint-check"] == (
        "governance_tool",
        expected,
    )
    assert registry["targets"]["local.pnc.release-fingerprint-check"][
        "relative_path"
    ] == expected
    assert (REPO_ROOT / "scripts" / "wrappers" / "hermes-release-fingerprint-check").is_file()
