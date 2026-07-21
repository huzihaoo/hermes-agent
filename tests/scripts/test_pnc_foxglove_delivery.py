from __future__ import annotations

from scripts.pnc_foxglove_delivery import (
    canonical_viz_mcap_cifs_path,
    canonical_viz_mcap_path,
    foxglove_url,
)


SUBMISSION_KEY = "g1q3-rca-s1-" + "a" * 64


def test_canonical_viz_paths_use_governed_task_landing():
    assert canonical_viz_mcap_path(SUBMISSION_KEY) == (
        f"/mnt/tmp/{SUBMISSION_KEY}/{SUBMISSION_KEY}.viz.mcap"
    )
    assert canonical_viz_mcap_cifs_path(SUBMISSION_KEY) == (
        "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/"
        f"{SUBMISSION_KEY}/{SUBMISSION_KEY}.viz.mcap"
    )


def test_foxglove_url_accepts_exact_task_landing_path():
    path = canonical_viz_mcap_path(SUBMISSION_KEY)
    assert foxglove_url(path) == (
        "https://192.168.21.217/?ds=foxglove-http&ds.mcapPath=" + path
    )


def test_foxglove_url_preserves_legacy_read_only_publications():
    legacy = (
        "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/"
        f"{SUBMISSION_KEY}/{SUBMISSION_KEY}.viz.mcap"
    )
    assert foxglove_url(legacy).endswith(f"ds.mcapPath={legacy}")


def test_foxglove_url_rejects_noncanonical_task_paths():
    assert foxglove_url(f"/mnt/tmp/other/{SUBMISSION_KEY}.viz.mcap") == ""
    assert foxglove_url(f"/mnt/tmp/{SUBMISSION_KEY}/nested/{SUBMISSION_KEY}.viz.mcap") == ""
    assert foxglove_url(f"/tmp/{SUBMISSION_KEY}/{SUBMISSION_KEY}.viz.mcap") == ""
