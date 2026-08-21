import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import feishu_credential_health as health


@pytest.fixture(autouse=True)
def _block_live_subprocess(monkeypatch):
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("live credential subprocess disabled in test")
        ),
    )


def test_release_local_keepwarm_import_closure():
    module = health.load_keepwarm_module()

    assert module.APP_ID == health.APP_ID
    assert health.KEEPWARM_PATH.resolve() == Path(module.__file__).resolve()


def test_doc_surface_reuses_keepwarm_success_without_file_fake_green():
    future_expiry = 4102444800000
    module = SimpleNamespace(keepwarm=lambda: (0, {"health": "OK", "after_expiresAt": future_expiry, "owner": "胡子豪"}))

    row = health.doc_surface("2026-06-22T00:00:00+08:00", keepwarm_module=module)

    assert row == {
        "surface": "doc",
        "owner": "胡子豪",
        "expires_at": future_expiry,
        "days_left": None,
        "health": "OK",
        "checked_at": "2026-06-22T00:00:00+08:00",
    }


def test_doc_surface_refresh_failure_is_reauth_required():
    module = SimpleNamespace(
        keepwarm=lambda: (
            2,
            {
                "health": "REAUTH_REQUIRED",
                "before_expiresAt": 1000,
                "after_expiresAt": 1000,
                "owner": "胡子豪",
                "error_class": "MCP_TOOL_ERROR",
            },
        )
    )

    row = health.doc_surface("now", keepwarm_module=module)

    assert row["surface"] == "doc"
    assert row["health"] == "REAUTH_REQUIRED"
    assert row["error_class"] == "MCP_TOOL_ERROR"


def test_project_cli_status_is_ok_and_exposes_owner_and_expiry(monkeypatch):
    monkeypatch.setattr(
        health,
        "call_project_auth_status",
        lambda: {
            "authenticated": True,
            "expires_in_minutes": 118,
            "host": "project.feishu.cn",
            "owner": "CLI Owner",
        },
    )

    row = health.project_surface("now")

    assert row["surface"] == "project"
    assert row["health"] == "OK"
    assert row["days_left"] is None
    assert row["owner"] == "CLI Owner"
    assert row["expires_at"] is not None


def test_project_surface_prefers_owner_reported_by_cli(monkeypatch):
    monkeypatch.setattr(
        health,
        "call_project_auth_status",
        lambda: {"authenticated": True, "expires_in_minutes": 118, "owner": "CLI Owner"},
    )

    row = health.project_surface("now")

    assert row["owner"] == "CLI Owner"


def test_project_surface_uses_explicit_owner_binding(monkeypatch):
    monkeypatch.setenv(health.PROJECT_OWNER_ENV, "Configured Owner")
    monkeypatch.setattr(
        health,
        "call_project_auth_status",
        lambda: {"authenticated": True, "expires_in_minutes": 118},
    )
    monkeypatch.setattr(health, "call_project_identity", lambda: {})

    row = health.project_surface("now")

    assert row["health"] == "OK"
    assert row["owner"] == "Configured Owner"


def test_project_surface_fails_closed_when_owner_is_not_bound(monkeypatch):
    monkeypatch.delenv(health.PROJECT_OWNER_ENV, raising=False)
    monkeypatch.setattr(
        health,
        "call_project_auth_status",
        lambda: {"authenticated": True, "expires_in_minutes": 118},
    )
    monkeypatch.setattr(health, "call_project_identity", lambda: {})

    row = health.project_surface("now")

    assert row["health"] == "PROBE_FAILED"
    assert row["error_class"] == "OWNER_UNAVAILABLE"
    assert row["expires_at"] is not None


def test_project_auth_errors_are_reauth_required(monkeypatch):
    samples = [
        "401 Unauthorized",
        "invalid token",
        "expired token",
        "MEEGLE_USER_ACCESS_TOKEN expired",
    ]
    for sample in samples:
        def fail(sample=sample):
            raise RuntimeError(sample)
        monkeypatch.setattr(health, "call_project_auth_status", fail)
        row = health.project_surface("now")
        assert row["health"] == "REAUTH_REQUIRED"
        assert row["surface"] == "project"


def test_project_other_errors_are_probe_failed(monkeypatch):
    monkeypatch.setattr(health, "call_project_auth_status", lambda: (_ for _ in ()).throw(RuntimeError("network timeout")))

    row = health.project_surface("now")

    assert row["health"] == "PROBE_FAILED"
    assert row["error_class"] == "RuntimeError"


def test_scrub_removes_secret_values_and_keys():
    payload = {
        "MEEGLE_USER_ACCESS_TOKEN": "m-" + "FAKEVALUE1234567890",
        "nested": {"appSecret": "fake-app-secret-value", "text": "Authz safe"},
        "ok": "胡子豪",
    }

    dumped = json.dumps(health.scrub(payload), ensure_ascii=False)

    assert "FAKEVALUE" not in dumped
    assert "fake-app-secret-value" not in dumped
    assert "胡子豪" in dumped


def test_safe_error_message_redacts_assignment_style_secrets():
    message = health.safe_error_message("request failed MEEGLE_USER_ACCESS_TOKEN=raw-secret-value")

    assert "raw-secret-value" not in message
    assert "<redacted>" in message


def test_safe_error_message_redacts_quoted_json_secrets():
    message = health.safe_error_message(
        '{"error":"denied","access_token":"raw-json-token","client_secret":"raw-json-secret"}'
    )

    assert "raw-json-token" not in message
    assert "raw-json-secret" not in message
    assert message.count("<redacted>") == 2


def test_project_probe_invokes_official_meegle_status_shape(monkeypatch):

    class Proc:
        returncode = 0
        stdout = json.dumps({"authenticated": True, "expires_in_minutes": 118, "host": "project.feishu.cn"})
        stderr = ""

    calls = []

    def fake_run(args, **kwargs):
        calls.append((args, kwargs))
        assert args[1:] == ["auth", "status", "--format", "json"]
        assert kwargs["env"]["MEEGLE_HOST"] == "project.feishu.cn"
        return Proc()

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = health.call_project_auth_status()

    assert result["authenticated"] is True
    assert calls


def test_project_identity_invokes_official_meegle_user_me_shape(monkeypatch):
    class Proc:
        returncode = 0
        stdout = json.dumps({"data": {"name": "CLI Owner"}})
        stderr = ""

    calls = []

    def fake_run(args, **kwargs):
        calls.append((args, kwargs))
        return Proc()

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = health.call_project_identity()

    assert result["data"]["name"] == "CLI Owner"
    assert calls[0][0][1:] == ["user", "me", "--format", "json"]


def test_project_surface_reads_owner_from_user_identity(monkeypatch):
    monkeypatch.delenv(health.PROJECT_OWNER_ENV, raising=False)
    monkeypatch.setattr(
        health,
        "call_project_auth_status",
        lambda: {"authenticated": True, "expires_in_minutes": 118},
    )
    monkeypatch.setattr(
        health, "call_project_identity", lambda: {"data": {"name": "CLI Owner"}}
    )

    row = health.project_surface("now")

    assert row["health"] == "OK"
    assert row["owner"] == "CLI Owner"


def test_project_surface_reads_meegle_name_cn_identity(monkeypatch):
    monkeypatch.delenv(health.PROJECT_OWNER_ENV, raising=False)
    monkeypatch.setattr(
        health,
        "call_project_auth_status",
        lambda: {"authenticated": True, "expires_in_minutes": 72},
    )
    monkeypatch.setattr(
        health,
        "call_project_identity",
        lambda: {"name_cn": "项目负责人", "name_en": "Project Owner", "user_key": "user-1"},
    )

    row = health.project_surface("now")

    assert row["health"] == "OK"
    assert row["owner"] == "项目负责人"


def test_meegle_host_environment_does_not_override_explicit_value(monkeypatch):
    monkeypatch.setenv("MEEGLE_HOST", "custom.example")

    assert health._meegle_environment()["MEEGLE_HOST"] == "custom.example"


def test_nonzero_unauthenticated_status_is_reauth_required(monkeypatch):
    class Proc:
        returncode = 1
        stdout = json.dumps({"authenticated": False, "reason": "signed out"})
        stderr = ""

    monkeypatch.setattr(subprocess, "run", lambda *args, **kwargs: Proc())

    row = health.project_surface("now")

    assert row["health"] == "REAUTH_REQUIRED"


def test_server_unreachable_status_is_probe_failed_not_reauth(monkeypatch):
    class Proc:
        returncode = 2
        stdout = json.dumps(
            {"authenticated": False, "reason": "server unreachable: timeout"}
        )
        stderr = ""

    monkeypatch.setattr(subprocess, "run", lambda *args, **kwargs: Proc())

    row = health.project_surface("now")

    assert row["health"] == "PROBE_FAILED"
    assert row["error_class"] == "RuntimeError"


def test_device_code_surface_checks_help_without_starting_login(monkeypatch):
    class Proc:
        returncode = 0
        stdout = "Usage: meegle auth login --device-code"
        stderr = ""

    calls = []

    def fake_run(args, **kwargs):
        calls.append(args)
        return Proc()

    monkeypatch.setattr(health, "_meegle_executable", lambda: "/usr/local/bin/meegle")
    monkeypatch.setattr(subprocess, "run", fake_run)

    row = health.device_code_surface("now")

    assert row["health"] == "OK"
    assert row["device_code_available"] is True
    assert calls == [["/usr/local/bin/meegle", "auth", "login", "--help"]]


def test_device_code_surface_reports_unavailable_help(monkeypatch):
    class Proc:
        returncode = 0
        stdout = "Usage: meegle auth login"
        stderr = ""

    monkeypatch.setattr(health, "_meegle_executable", lambda: "/usr/local/bin/meegle")
    monkeypatch.setattr(subprocess, "run", lambda *args, **kwargs: Proc())

    row = health.device_code_surface("now")

    assert row["health"] == "PROBE_FAILED"
    assert row["device_code_available"] is False
    assert row["error_class"] == "DEVICE_CODE_UNAVAILABLE"


def test_write_output_has_three_rows_and_no_secret(tmp_path):
    rows = [
        {"surface": "doc", "owner": "胡子豪", "expires_at": 1, "days_left": None, "health": "OK", "checked_at": "now"},
        {"surface": "project", "owner": None, "expires_at": None, "days_left": None, "health": "OK", "checked_at": "now"},
        {"surface": "meegle_cli", "owner": "胡子豪", "expires_at": None, "days_left": None, "health": "OK", "checked_at": "now", "device_code_available": True},
    ]

    path = health.write_output(rows, tmp_path)
    text = path.read_text(encoding="utf-8")

    assert path.exists()
    assert (tmp_path / "latest.json").exists()
    data = json.loads(text)
    assert [row["surface"] for row in data["rows"]] == ["doc", "project", "meegle_cli"]
    assert "token" not in text.lower()
    assert "secret" not in text.lower()


def test_main_fails_closed_when_health_producer_returns_no_rows(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(health, "run_health", lambda: [])

    assert health.main(["--json", "--output-dir", str(tmp_path)]) == 2


def test_main_fails_closed_when_health_surface_set_is_incomplete(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        health,
        "run_health",
        lambda: [{"surface": "doc", "health": "OK", "checked_at": "now"}],
    )

    assert health.main(["--json", "--output-dir", str(tmp_path)]) == 2
