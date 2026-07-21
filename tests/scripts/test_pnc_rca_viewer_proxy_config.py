from __future__ import annotations

from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "deploy/nginx/g1q3-rca-publication.conf"
SUBMISSION_A = "g1q3-rca-s1-" + "a" * 64
SUBMISSION_B = "g1q3-rca-s1-" + "b" * 64
ARTIFACT_SET = "g1q3-rca-artifact-v1-" + "c" * 64
REPORT_INDEX = (
    f"/G1Q3_RCA/cases/{SUBMISSION_A}/{ARTIFACT_SET}/index.html"
)
REPORT_ASSET = (
    f"/G1Q3_RCA/cases/{SUBMISSION_A}/{ARTIFACT_SET}/"
    "assets/media/%E8%AF%81%E6%8D%AE.mp4"
)
VIZ_ARTIFACT = (
    f"/g1q3-rca-artifacts/v1/{SUBMISSION_A}/{SUBMISSION_A}.viz.mcap"
)


def _source_and_guards() -> tuple[str, tuple[re.Pattern[str], re.Pattern[str]]]:
    source = CONFIG.read_text(encoding="ascii")
    patterns = re.findall(
        r'if \(\$request_uri !~ "([^"]+)"\) \{ return 404; \}',
        source,
    )
    assert len(patterns) == 2
    return source, (re.compile(patterns[0]), re.compile(patterns[1]))


def test_proxy_include_has_two_scoped_manifest_validated_routes():
    source, (report_guard, viz_guard) = _source_and_guards()

    assert source.startswith("# Include only inside the canonical viewer HTTPS server.")
    assert source.count("location ^~ ") == 2
    assert "location ^~ /G1Q3_RCA/cases/ {" in source
    assert "location ^~ /g1q3-rca-artifacts/ {" in source
    assert source.count("autoindex off;") == 2
    assert source.count("limit_except GET HEAD OPTIONS {") == 2
    assert source.count("proxy_pass http://192.168.26.174:18081;") == 2
    assert "proxy_pass http://$" not in source
    assert source.count("proxy_set_header Accept-Encoding \"\";") == 2
    assert source.count("proxy_set_header Range $http_range;") == 2
    assert source.count("proxy_set_header If-Range $http_if_range;") == 2
    assert source.count("proxy_set_header Origin $http_origin;") == 2
    assert source.count("proxy_pass_request_body off;") == 2
    assert source.count("proxy_buffering off;") == 2
    assert source.count("proxy_intercept_errors off;") == 2
    assert re.search(r"(?m)^\s*(rewrite|alias|root)\s", source) is None

    assert report_guard.fullmatch(REPORT_INDEX)
    assert report_guard.fullmatch(REPORT_ASSET)
    assert viz_guard.fullmatch(VIZ_ARTIFACT)


def test_proxy_guards_reject_directories_queries_traversal_and_identity_drift():
    _source, (report_guard, viz_guard) = _source_and_guards()

    rejected_reports = (
        REPORT_INDEX + "?raw=1",
        REPORT_INDEX.rsplit("/", 1)[0] + "/",
        REPORT_INDEX.replace("/index.html", "/../index.html"),
        REPORT_INDEX.replace(ARTIFACT_SET, "not-content-addressed"),
        REPORT_INDEX.replace(SUBMISSION_A, "case-a"),
        REPORT_INDEX.replace("/cases/", "/cases//"),
    )
    rejected_viz = (
        VIZ_ARTIFACT + "?raw=1",
        VIZ_ARTIFACT.rsplit("/", 1)[0] + "/",
        VIZ_ARTIFACT.replace(SUBMISSION_A + ".viz.mcap", SUBMISSION_B + ".viz.mcap"),
        VIZ_ARTIFACT.replace("/v1/", "/v2/"),
        VIZ_ARTIFACT.replace("/v1/", "/v1//"),
        VIZ_ARTIFACT + ".bak",
    )
    assert all(report_guard.fullmatch(path) is None for path in rejected_reports)
    assert all(viz_guard.fullmatch(path) is None for path in rejected_viz)
