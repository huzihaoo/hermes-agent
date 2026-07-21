from __future__ import annotations

from scripts.pnc_foxglove_delivery import (
    canonical_publication_origin,
    canonical_viz_remote_file_url,
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


def test_foxglove_url_accepts_exact_task_landing_path(monkeypatch):
    monkeypatch.setenv("PNC_FOXGLOVE_RENDER_HOST", "https://192.168.21.217")
    path = canonical_viz_mcap_path(SUBMISSION_KEY)
    assert foxglove_url(path) == (
        "https://192.168.21.217/?ds=remote-file&ds.url="
        "https%3A%2F%2F192.168.21.217%2Fg1q3-rca-artifacts%2Fv1%2F"
        f"{SUBMISSION_KEY}%2F{SUBMISSION_KEY}.viz.mcap"
    )
    assert canonical_viz_remote_file_url(SUBMISSION_KEY) == (
        "https://192.168.21.217/g1q3-rca-artifacts/v1/"
        f"{SUBMISSION_KEY}/{SUBMISSION_KEY}.viz.mcap"
    )


def test_foxglove_url_preserves_legacy_read_only_publications():
    legacy = (
        "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/"
        f"{SUBMISSION_KEY}/{SUBMISSION_KEY}.viz.mcap"
    )
    assert foxglove_url(legacy).endswith(f"ds.mcapPath={legacy}")


def test_foxglove_url_preserves_unicode_legacy_case_keys():
    key = "6986500860_fcw_合肥-G1Q3_6028车-自车右转"
    legacy = (
        "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/"
        f"{key}/{key}.viz.mcap"
    )
    rendered = foxglove_url(legacy)

    assert rendered.startswith("https://192.168.21.217/?ds=foxglove-http&ds.mcapPath=")
    assert "%E5%90%88%E8%82%A5" in rendered


def test_foxglove_url_rejects_noncanonical_task_paths():
    assert foxglove_url(f"/mnt/tmp/other/{SUBMISSION_KEY}.viz.mcap") == ""
    assert foxglove_url(f"/mnt/tmp/{SUBMISSION_KEY}/nested/{SUBMISSION_KEY}.viz.mcap") == ""
    assert foxglove_url(f"/tmp/{SUBMISSION_KEY}/{SUBMISSION_KEY}.viz.mcap") == ""
    assert foxglove_url("/mnt/tmp/中文/中文.viz.mcap") == ""
    for key in ("case(1)", "case[1]", "case\ud800"):
        legacy = (
            "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/"
            f"{key}/{key}.viz.mcap"
        )
        assert foxglove_url(legacy) == ""


def test_task_remote_file_requires_https_viewer_origin(monkeypatch):
    monkeypatch.setenv("PNC_FOXGLOVE_RENDER_HOST", "http://viewer.internal")

    assert foxglove_url(canonical_viz_mcap_path(SUBMISSION_KEY)) == ""
    assert canonical_viz_remote_file_url(SUBMISSION_KEY) == ""
    monkeypatch.setenv("PNC_FOXGLOVE_RENDER_HOST", "")
    assert foxglove_url(canonical_viz_mcap_path(SUBMISSION_KEY)) == ""
    assert canonical_viz_remote_file_url(SUBMISSION_KEY) == ""


def test_task_remote_file_requires_explicit_viewer_origin(monkeypatch):
    monkeypatch.delenv("PNC_FOXGLOVE_RENDER_HOST", raising=False)

    assert foxglove_url(canonical_viz_mcap_path(SUBMISSION_KEY)) == ""
    assert canonical_viz_remote_file_url(SUBMISSION_KEY) == ""


def test_publication_origin_requires_explicit_https_dns(monkeypatch):
    for origin in (
        "https://viewer.internal",
        "https://viewer.internal:8443",
    ):
        monkeypatch.setenv("PNC_FOXGLOVE_RENDER_HOST", origin)
        assert canonical_publication_origin() == origin

    for origin in (
        "",
        "http://viewer.internal",
        "https://192.168.21.217",
        "https://localhost",
    ):
        monkeypatch.setenv("PNC_FOXGLOVE_RENDER_HOST", origin)
        assert canonical_publication_origin() == ""

    monkeypatch.delenv("PNC_FOXGLOVE_RENDER_HOST", raising=False)
    assert canonical_publication_origin() == ""


def test_malformed_viewer_origins_fail_closed(monkeypatch):
    path = canonical_viz_mcap_path(SUBMISSION_KEY)
    for origin in (
        "https://viewer.internal:bad",
        "https://viewer.internal:0",
        "https://viewer.internal:443",
        "https://viewer.internal:70000",
        "https://viewer.internal?",
        "https://viewer.internal#",
        "https://[::1",
        "https://[::1]",
        "https://[fe80::1%25en0]",
        "https://[::ffff:192.0.2.128]",
        "https://[::ffff:c000:280]",
        "https://viewer\\evil.internal",
        "https://Viewer.Internal",
        "https://viewer.internal\n",
        "https://\u89c6\u56fe.internal",
        "https://0x7f000001",
        "https://127.0.0.0x1",
        "https://0x7f.0x0.0x0.0x1",
        "https://xn--a.internal",
        "https://xn--fsq.internal",
        "https://foo.xn--a",
        "https://viewer.internal:",
    ):
        monkeypatch.setenv("PNC_FOXGLOVE_RENDER_HOST", origin)
        assert foxglove_url(path) == ""
        assert canonical_viz_remote_file_url(SUBMISSION_KEY) == ""


def test_nondefault_https_viewer_port_is_supported(monkeypatch):
    monkeypatch.setenv("PNC_FOXGLOVE_RENDER_HOST", "https://viewer.internal:8443")

    assert canonical_viz_remote_file_url(SUBMISSION_KEY) == (
        "https://viewer.internal:8443/g1q3-rca-artifacts/v1/"
        f"{SUBMISSION_KEY}/{SUBMISSION_KEY}.viz.mcap"
    )
