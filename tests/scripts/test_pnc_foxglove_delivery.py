from __future__ import annotations

import plistlib
from pathlib import Path

import pytest

from scripts.pnc_foxglove_delivery import (
    canonical_https_report_origin,
    canonical_publication_origin,
    canonical_report_origin,
    canonical_report_url_from_vm_path,
    canonical_viz_mcap_cifs_path,
    canonical_viz_mcap_path,
    foxglove_url,
    validate_canonical_report_url,
)


SUBMISSION_KEY = "g1q3-rca-s1-" + "a" * 64


def test_canonical_viz_paths_use_formal_perception_share():
    assert canonical_viz_mcap_path(SUBMISSION_KEY) == (
        "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/"
        f"{SUBMISSION_KEY}/{SUBMISSION_KEY}.viz.mcap"
    )
    assert canonical_viz_mcap_cifs_path(SUBMISSION_KEY) == (
        "//hfs.minieye.tech/department-perception_test_team/G1Q3_RCA/cases/"
        f"{SUBMISSION_KEY}/{SUBMISSION_KEY}.viz.mcap"
    )


def test_foxglove_url_accepts_exact_formal_path():
    path = canonical_viz_mcap_path(SUBMISSION_KEY)
    assert foxglove_url(path) == (
        "https://192.168.21.217/?ds=foxglove-http&ds.mcapPath="
        f"{path}"
    )


def test_foxglove_url_preserves_legacy_read_only_publications():
    legacy = (
        "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/"
        f"{SUBMISSION_KEY}/{SUBMISSION_KEY}.viz.mcap"
    )
    assert foxglove_url(legacy).endswith(f"ds.mcapPath={legacy}")


def test_foxglove_url_rejects_non_submission_case_keys():
    key = "6986500860_fcw_合肥-G1Q3_6028车-自车右转"
    legacy = (
        "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/"
        f"{key}/{key}.viz.mcap"
    )
    assert foxglove_url(legacy) == ""


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


def test_foxglove_url_uses_fixed_existing_surface(monkeypatch):
    path = canonical_viz_mcap_path(SUBMISSION_KEY)
    for configured in ("", "http://viewer.internal", "https://viewer.internal"):
        monkeypatch.setenv("PNC_FOXGLOVE_RENDER_HOST", configured)
        assert foxglove_url(path) == (
            "https://192.168.21.217/?ds=foxglove-http&ds.mcapPath=" + path
        )


def test_publication_origin_requires_explicit_approved_origin(monkeypatch):
    for origin in (
        "https://viewer.internal",
        "http://192.168.26.174:18081",
    ):
        monkeypatch.setenv("PNC_FOXGLOVE_RENDER_HOST", origin)
        assert canonical_publication_origin() == origin

    for origin in (
        "",
        "http://viewer.internal",
        "http://192.168.26.175:18081",
        "http://192.168.26.174:18082",
        "https://192.168.21.217",
        "https://localhost",
        "https://viewer.internal:8443",
    ):
        monkeypatch.setenv("PNC_FOXGLOVE_RENDER_HOST", origin)
        assert canonical_publication_origin() == ""

    monkeypatch.delenv("PNC_FOXGLOVE_RENDER_HOST", raising=False)
    assert canonical_publication_origin() == ""


def test_canonical_report_origin_rejects_private_http_and_accepts_explicit_https():
    assert canonical_https_report_origin("http://192.168.26.174:18081") == ""
    assert canonical_https_report_origin("https://192.168.21.217") == ""
    assert canonical_https_report_origin("https://g1q3-rca.minieye.tech") == (
        "https://g1q3-rca.minieye.tech"
    )


@pytest.mark.parametrize(
    "origin",
    [
        "",
        "http://192.168.26.175:18081",
        "http://192.168.26.174:18082",
        "http://user@192.168.26.174:18081",
        "http://192.168.26.174:18081/reports",
        "http://192.168.26.174:18081?query=1",
    ],
)
def test_report_origin_accepts_only_the_approved_internal_http_service(origin):
    assert canonical_report_origin(origin) == ""
    assert canonical_report_origin("http://192.168.26.174:18081") == (
        "http://192.168.26.174:18081"
    )


def test_canonical_report_url_requires_index_html_and_exact_origin():
    origin = "https://g1q3-rca.minieye.tech"
    vm_path = (
        "/mnt/minieye/pdcl/department/perception_test_team/"
        "G1Q3_RCA/cases/g1q3-rca-s1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        "aaaaaaaaaaaaaaaaaaaaaaaa/index.html"
    )
    expected = (
        "https://g1q3-rca.minieye.tech/G1Q3_RCA/cases/"
        "g1q3-rca-s1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/"
        "index.html"
    )

    assert canonical_report_url_from_vm_path(vm_path, origin) == expected
    assert validate_canonical_report_url(expected, origin) == expected
    assert validate_canonical_report_url(
        "http://192.168.26.174:18081/G1Q3_RCA/cases/x/index.html", origin
    ) == ""
    assert validate_canonical_report_url(
        "https://g1q3-rca.minieye.tech/G1Q3_RCA/cases/x/report.viz.mcap", origin
    ) == ""


def test_canonical_report_url_accepts_exact_internal_service_only():
    origin = "http://192.168.26.174:18081"
    vm_path = (
        "/mnt/minieye/pdcl/department/perception_test_team/"
        "G1Q3_RCA/cases/demo/index.html"
    )
    expected = f"{origin}/G1Q3_RCA/cases/demo/index.html"

    assert canonical_report_url_from_vm_path(vm_path, origin) == expected
    assert validate_canonical_report_url(expected, origin) == expected
    for rejected in (
        "",
        f"{origin}/G1Q3_RCA/cases/demo/demo.viz.mcap",
        "http://192.168.26.175:18081/G1Q3_RCA/cases/demo/index.html",
        "http://192.168.26.174:18082/G1Q3_RCA/cases/demo/index.html",
    ):
        assert validate_canonical_report_url(rejected, origin) == ""


@pytest.mark.parametrize(
    "plist_name",
    [
        "local.pnc.rca-delivery-collector.plist",
        "local.pnc.rca-delivery-dispatcher.plist",
        "local.pnc.completion-notice-relay.plist",
        "local.pnc.vm-task-sync.plist",
    ],
)
def test_publication_host_writers_pin_approved_internal_origin(plist_name):
    root = Path(__file__).resolve().parents[2]
    with (root / plist_name).open("rb") as handle:
        payload = plistlib.load(handle)

    assert payload["EnvironmentVariables"]["PNC_FOXGLOVE_RENDER_HOST"] == (
        "http://192.168.26.174:18081"
    )


def test_malformed_publication_origins_do_not_change_fixed_foxglove_url(monkeypatch):
    expected = foxglove_url(canonical_viz_mcap_path(SUBMISSION_KEY))
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
        assert foxglove_url(canonical_viz_mcap_path(SUBMISSION_KEY)) == expected
