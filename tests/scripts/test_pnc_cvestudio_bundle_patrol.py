from __future__ import annotations

import json
from typing import Any

import pytest

from scripts import pnc_cvestudio_bundle_patrol as patrol


RENDERER = "http://renderer.test/"
ROOT = (
    b"<!doctype html><html><head>"
    b'<script defer src="react-vendor.js"></script>'
    b'<script src="main.js"></script>'
    b"</head><body><div id='root'></div></body></html>"
)
REACT = b"react-vendor fixture\n"
MAIN = b"main fixture\n"


class _Response:
    def __init__(
        self,
        body: bytes = b"",
        *,
        status: int = 200,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.body = body
        self.status = status
        self.headers = headers or {}
        self.closed = False

    def getcode(self) -> int:
        return self.status

    def read(self, _size: int = -1) -> bytes:
        body, self.body = self.body, b""
        return body

    def close(self) -> None:
        self.closed = True


class _Opener:
    def __init__(self, routes: dict[str, _Response]) -> None:
        self.routes = routes
        self.calls: list[str] = []

    def open(self, request: Any, *, timeout: float) -> _Response:
        assert timeout > 0
        url = request.full_url
        self.calls.append(url)
        response = self.routes[url]
        return _Response(
            response.body,
            status=response.status,
            headers=dict(response.headers),
        )


def _opener(*, root: _Response | None = None) -> _Opener:
    return _Opener(
        {
            RENDERER: root or _Response(ROOT),
            RENDERER + "react-vendor.js": _Response(REACT),
            RENDERER + "main.js": _Response(MAIN),
        }
    )


def _expected() -> str:
    return patrol.bundle_fingerprint(
        [
            ("index.html", ROOT),
            ("react-vendor.js", REACT),
            ("main.js", MAIN),
        ]
    )


def test_patrol_match_is_read_only_and_labels_http_200_as_spa_liveness():
    opener = _opener()
    result = patrol.run_patrol(
        renderer_url=RENDERER,
        expected_bundle_sha256=_expected(),
        opener=opener,
    )

    assert result["status"] == "ok"
    assert result["alert"] is False
    assert result["bundle"]["hash_status"] == "match"
    assert result["renderer"]["status_code"] == 200
    assert result["renderer"]["liveness"] == "spa_endpoint_only"
    assert result["renderer"]["parse_verified"] is False
    assert "does not prove MCAP parse" in result["renderer"]["note"]
    assert result["production_actions"] == {
        "writes": 0,
        "restarts": 0,
        "external_effects": 0,
    }
    assert all("ds.mcapPath" not in url for url in opener.calls)


def test_hash_change_is_an_explicit_alert():
    result = patrol.run_patrol(
        renderer_url=RENDERER,
        expected_bundle_sha256="0" * 64,
        opener=_opener(),
    )

    assert result["status"] == "alert"
    assert result["alert"] is True
    assert result["error_code"] == "cvestudio_bundle_hash_changed"
    assert result["bundle"]["hash_status"] == "changed"
    assert result["bundle"]["observed_sha256"] != "0" * 64


def test_missing_baseline_is_fail_visible_and_reports_observed_hash():
    result = patrol.run_patrol(renderer_url=RENDERER, opener=_opener())

    assert result["status"] == "unconfigured"
    assert result["alert"] is True
    assert result["error_code"] == "cvestudio_bundle_hash_unconfigured"
    assert result["live_configuration_required"] == [
        "PNC_CVESTUDIO_BUNDLE_SHA256"
    ]
    assert result["bundle"]["observed_sha256"] == _expected()


def test_endpoint_failure_is_alert_and_never_parse_success():
    opener = _opener(root=_Response(status=503))
    result = patrol.run_patrol(
        renderer_url=RENDERER,
        expected_bundle_sha256=_expected(),
        opener=opener,
    )

    assert result["status"] == "alert"
    assert result["error_code"] == "cvestudio_renderer_http_status"
    assert result["renderer"]["liveness"] == "unavailable"
    assert result["renderer"]["parse_verified"] is False
    assert opener.calls == [RENDERER]


def test_redirect_is_not_followed():
    opener = _opener(root=_Response(status=302, headers={"Location": "/next"}))
    result = patrol.run_patrol(
        renderer_url=RENDERER,
        expected_bundle_sha256=_expected(),
        opener=opener,
    )

    assert result["status"] == "alert"
    assert result["error_code"] == "cvestudio_renderer_http_status"
    assert opener.calls == [RENDERER]


def test_cross_origin_script_is_rejected_before_fetch():
    root = _Response(
        b'<script src="https://evil.test/main.js"></script>'
    )
    opener = _opener(root=root)
    result = patrol.run_patrol(
        renderer_url=RENDERER,
        expected_bundle_sha256=_expected(),
        opener=opener,
    )

    assert result["status"] == "alert"
    assert result["error_code"] == "cvestudio_bundle_script_cross_origin"
    assert opener.calls == [RENDERER]


def test_invalid_expected_hash_is_configuration_error():
    result = patrol.run_patrol(
        renderer_url=RENDERER,
        expected_bundle_sha256="not-a-sha",
        opener=_opener(),
    )

    assert result["status"] == "alert"
    assert result["error_code"] == "cvestudio_bundle_expected_hash_invalid"
    assert result["bundle"]["hash_status"] == "invalid_configuration"


def test_cli_emits_json_and_nonzero_for_hash_drift(monkeypatch, capsys):
    opener = _opener()
    monkeypatch.setattr(
        patrol,
        "_build_default_opener",
        lambda *, insecure_tls: opener,
    )

    exit_code = patrol.main(
        [
            "--renderer-url",
            RENDERER,
            "--expected-bundle-sha256",
            "f" * 64,
            "--json",
        ]
    )

    assert exit_code != 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["error_code"] == "cvestudio_bundle_hash_changed"
    assert payload["renderer"]["parse_verified"] is False


@pytest.mark.parametrize(
    "value",
    [
        "https://renderer.test/?ds=foxglove-http",
        "https://renderer.test/path",
        "file:///tmp/cvestudio",
    ],
)
def test_renderer_url_must_be_an_origin_root(value: str):
    result = patrol.run_patrol(
        renderer_url=value,
        expected_bundle_sha256=_expected(),
        opener=_opener(),
    )

    assert result["error_code"] == "cvestudio_renderer_url_invalid"
