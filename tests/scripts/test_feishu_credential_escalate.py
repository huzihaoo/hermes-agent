import contextlib
import io
import json
import os
import stat
from types import SimpleNamespace

import pytest

from scripts import feishu_credential_escalate as esc

_OAUTH_STATE = "s" * 43


@pytest.fixture(autouse=True)
def _block_live_auth_helpers(monkeypatch, tmp_path):
    """Keep focused tests off every live credential path and transport."""
    monkeypatch.setattr(esc, "STATE_PATH", tmp_path / "escalation-state.json")
    monkeypatch.setattr(esc, "FALLBACK_PATH", tmp_path / "fallback.json")
    monkeypatch.setattr(esc, "CALLBACK_LOG_PATH", tmp_path / "listener.log")
    monkeypatch.setattr(
        esc,
        "_call_feishu_doc_tool",
        lambda *_args, **_kwargs: {"structuredContent": {}},
    )
    monkeypatch.setattr(
        esc,
        "start_doc_oauth",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("direct OAuth disabled in test")),
    )
    monkeypatch.setattr(
        esc,
        "complete_doc_oauth",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("direct OAuth disabled in test")),
    )
    monkeypatch.setattr(
        esc,
        "send_message_tool",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("live send disabled in test")
        ),
    )
    monkeypatch.setattr(
        esc,
        "load_send_environment",
        lambda: (_ for _ in ()).throw(AssertionError("dotenv disabled in test")),
    )
    monkeypatch.setattr(
        esc.subprocess,
        "run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("live subprocess disabled in test")
        ),
    )
    monkeypatch.setattr(
        esc.subprocess,
        "Popen",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("live listener disabled in test")
        ),
    )
    monkeypatch.setattr(esc, "in_quiet_hours", lambda: False)


def _doc_row(health="REAUTH_REQUIRED", expires_at=1234, owner="胡子豪"):
    return {"surface": "doc", "owner": owner, "expires_at": expires_at, "days_left": None, "health": health, "checked_at": "now"}


def _project_row(health="PROBE_FAILED"):
    return {"surface": "project", "owner": None, "expires_at": None, "days_left": None, "health": health, "checked_at": "now"}


def _meegle_row(health="PROBE_FAILED"):
    return {"surface": "meegle_cli", "owner": "胡子豪", "expires_at": None, "days_left": None, "health": health, "checked_at": "now"}


def _all_rows(*, doc="REAUTH_REQUIRED", project="PROBE_FAILED", meegle="PROBE_FAILED"):
    return [_doc_row(doc), _project_row(project), _meegle_row(meegle)]


def test_owner_name_resolves_to_open_id_and_token_user_id_does_not(monkeypatch):
    mapping = {"ou_d1d3cfeba1be0a22faa36aaf4fb3907d": "胡子豪"}
    monkeypatch.setattr(esc, "_load_user_id_mapping", lambda: mapping)

    assert esc.open_id_for_owner("胡子豪") == "ou_d1d3cfeba1be0a22faa36aaf4fb3907d"
    assert esc.open_id_for_owner("fefb829e") == ""


@pytest.mark.parametrize("value", ["ou_", "ou_good:om_thread", "ou_good/bad", "ou_good\nnext"])
def test_open_id_validation_rejects_values_that_could_retarget_send(monkeypatch, value):
    monkeypatch.setattr(esc, "_load_user_id_mapping", lambda: {})

    assert esc.open_id_for_owner(value, default_open_id="") == ""
    assert esc.target_for_open_id(value) == ""


def test_duplicate_owner_name_refuses_ambiguous_target(monkeypatch):
    monkeypatch.setattr(
        esc,
        "_load_user_id_mapping",
        lambda: {"ou_first": "同名", "ou_second": "同名"},
    )

    assert esc.open_id_for_owner("同名") == ""


def test_dry_run_reauth_doc_renders_dm_at_and_runbook(monkeypatch):
    monkeypatch.setattr(esc, "_load_user_id_mapping", lambda: {"ou_d1d3cfeba1be0a22faa36aaf4fb3907d": "胡子豪"})
    monkeypatch.setattr(esc, "resolve_display_name", lambda open_id: "胡子豪")

    result = esc.row_escalation(_doc_row(), state={"sent": {}}, now=1000)

    assert result["dry_run"] is True
    assert result["target"] == "feishu:ou_d1d3cfeba1be0a22faa36aaf4fb3907d"
    assert result["open_id"] == "ou_d1d3cfeba1be0a22faa36aaf4fb3907d"
    assert result["has_mention"] is True
    assert '<at user_id="ou_d1d3cfeba1be0a22faa36aaf4fb3907d">胡子豪</at>' in result["preview"]
    assert "feishu-credential-runbook.md" in result["preview"]
    assert "§1 文档 OAuth 重新授权" in result["preview"]
    assert "当前授权入口未启用" in result["preview"]
    assert result["notify_key"] == "REAUTH_REQUIRED|doc||1234"


def test_project_null_owner_uses_default_open_id(monkeypatch):
    monkeypatch.setattr(esc, "resolve_display_name", lambda open_id: "胡子豪")

    result = esc.row_escalation(_project_row(), state={"sent": {}}, default_open_id="ou_d1d3cfeba1be0a22faa36aaf4fb3907d")

    assert result["target"] == "feishu:ou_d1d3cfeba1be0a22faa36aaf4fb3907d"
    assert "§3 Meegle 用户凭据处置" in result["preview"]


def test_dedup_suppresses_same_key_within_cooldown(monkeypatch):
    monkeypatch.setattr(esc, "_load_user_id_mapping", lambda: {"ou_d1d3cfeba1be0a22faa36aaf4fb3907d": "胡子豪"})
    key = esc.notify_key_for_row(_doc_row())
    state = {"sent": {key: 1000}}

    result = esc.row_escalation(_doc_row(), state=state, now=1001, cooldown_seconds=3600)

    assert result["suppressed"] is True
    assert result["dry_run"] is True


def test_health_change_gets_new_notify_key_and_not_suppressed(monkeypatch):
    monkeypatch.setattr(esc, "_load_user_id_mapping", lambda: {"ou_d1d3cfeba1be0a22faa36aaf4fb3907d": "胡子豪"})
    old_key = esc.notify_key_for_row(_doc_row("REAUTH_REQUIRED"))
    state = {"sent": {old_key: 1000}}

    result = esc.row_escalation(_doc_row("PROBE_FAILED"), state=state, now=1001, cooldown_seconds=3600)

    assert result["notify_key"] != old_key
    assert result["suppressed"] is False


def test_no_open_id_refuses_without_group_fallback(monkeypatch):
    monkeypatch.setattr(esc, "_load_user_id_mapping", lambda: {})

    result = esc.row_escalation(_doc_row(owner="不存在"), state={"sent": {}})

    assert result["refused"] is True
    assert result["reason"] == "owner_open_id_unresolved"
    assert "target" not in result


def test_no_target_refuses(monkeypatch):
    monkeypatch.setattr(esc, "_load_user_id_mapping", lambda: {"ou_d1d3cfeba1be0a22faa36aaf4fb3907d": "胡子豪"})

    result = esc.row_escalation(_doc_row(), state={"sent": {}}, target_mode="topic", topic_target="")

    assert result["refused"] is True
    assert result["reason"] == "no_explicit_target"


@pytest.mark.parametrize(
    "target",
    ["telegram", "feishu", "feishu:oc_group", "feishu:ou_user", "topic:om_thread"],
)
def test_topic_target_rejects_home_group_and_non_feishu_routes(monkeypatch, target):
    monkeypatch.setattr(esc, "_load_user_id_mapping", lambda: {"ou_d1d3cfeba1be0a22faa36aaf4fb3907d": "胡子豪"})

    result = esc.row_escalation(
        _doc_row(), state={"sent": {}}, target_mode="topic", topic_target=target
    )

    assert result["refused"] is True
    assert result["reason"] == "no_explicit_target"


def test_topic_target_requires_explicit_feishu_group_and_thread(monkeypatch):
    monkeypatch.setattr(esc, "_load_user_id_mapping", lambda: {"ou_d1d3cfeba1be0a22faa36aaf4fb3907d": "胡子豪"})

    result = esc.row_escalation(
        _doc_row(),
        state={"sent": {}},
        target_mode="topic",
        topic_target="feishu:oc_group:om_thread",
    )

    assert result["target"] == "feishu:oc_group:om_thread"


def test_markdownish_owner_text_does_not_break_plain_message(monkeypatch):
    monkeypatch.setattr(esc, "_load_user_id_mapping", lambda: {"ou_d1d3cfeba1be0a22faa36aaf4fb3907d": "胡子豪"})
    monkeypatch.setattr(esc, "resolve_display_name", lambda open_id: "胡子豪")
    row = _doc_row(owner="`胡子豪`")
    row["owner"] = "胡子豪"

    result = esc.row_escalation(row, state={"sent": {}})

    assert "```" not in result["preview"]
    assert "**" not in result["preview"]
    assert "<at user_id=" in result["preview"]


def test_send_success_persists_dedup_state(tmp_path, monkeypatch):
    monkeypatch.setattr(esc, "_load_user_id_mapping", lambda: {"ou_d1d3cfeba1be0a22faa36aaf4fb3907d": "胡子豪"})
    monkeypatch.setattr(esc, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(
        esc,
        "get_doc_auth_url",
        lambda: {
            "ok": True,
            "auth_url": f"https://open.feishu.cn/auth?state={_OAUTH_STATE}",
            "state": _OAUTH_STATE,
        },
    )
    monkeypatch.setattr(esc, "in_quiet_hours", lambda: False)
    monkeypatch.setattr(
        esc,
        "start_callback_listener",
        lambda state, **_kwargs: {"started": True, "pid": 123},
    )
    calls = []

    def fake_send(args):
        calls.append(args)
        return json.dumps({"success": True, "message_id": "om_1"})

    result = esc.row_escalation(_doc_row(), send=True, state={"sent": {}}, send_func=fake_send, now=1000)

    assert result["sent"] is True
    assert calls[0]["target"] == "feishu:ou_d1d3cfeba1be0a22faa36aaf4fb3907d"
    state = json.loads((tmp_path / "state.json").read_text())
    assert state["sent"][result["notify_key"]] == 1000


def test_unsupported_outbound_mode_fails_closed_and_does_not_send(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_OUTBOUND_MODE", "record-onyl")
    monkeypatch.setattr(esc, "FALLBACK_PATH", tmp_path / "fallback.json")
    monkeypatch.setattr(esc, "_load_user_id_mapping", lambda: {"ou_owner": "胡子豪"})
    monkeypatch.setattr(esc, "_notify_fallback", lambda _alert: {"attempted": True, "ok": False})
    calls = []

    result = esc.row_escalation(
        _doc_row(),
        send=True,
        state={"sent": {}},
        send_func=lambda args: calls.append(args) or {"success": True},
        now=1000,
    )

    assert result["sent"] is False
    assert "unsupported HERMES_OUTBOUND_MODE" in result["send_result"]["error"]
    assert calls == []
    assert os.environ["HERMES_OUTBOUND_MODE"] == "record-onyl"


def test_ok_health_skips():
    result = esc.row_escalation(_doc_row("OK"), state={"sent": {}})

    assert result["skipped"] is True
    assert result["reason"] == "health_ok_or_not_escalating"


def test_recovery_ok_clears_same_surface_escalation_ledger_when_sending(tmp_path, monkeypatch):
    monkeypatch.setattr(esc, "STATE_PATH", tmp_path / "state.json")
    state = {
        "sent": {
            "REAUTH_REQUIRED|doc||9999": 1000,
            "EXPIRED|doc||8888": 1100,
            "PROBE_FAILED|project||2026-06-24": 1200,
        }
    }

    result = esc.row_escalation(_doc_row("OK"), send=True, state=state)

    assert result["skipped"] is True
    assert result["recovery_cleared_keys"] == ["EXPIRED|doc||8888", "REAUTH_REQUIRED|doc||9999"]
    assert "REAUTH_REQUIRED|doc||9999" not in state["sent"]
    assert "EXPIRED|doc||8888" not in state["sent"]
    assert state["sent"]["PROBE_FAILED|project||2026-06-24"] == 1200
    persisted = json.loads((tmp_path / "state.json").read_text())
    assert persisted == state


def test_reauth_required_can_send_again_after_recovery_clears_ledger(tmp_path, monkeypatch):
    monkeypatch.setattr(esc, "_load_user_id_mapping", lambda: {"ou_d1d3cfeba1be0a22faa36aaf4fb3907d": "胡子豪"})
    monkeypatch.setattr(esc, "resolve_display_name", lambda open_id: "胡子豪")
    monkeypatch.setattr(esc, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(
        esc,
        "get_doc_auth_url",
        lambda: {
            "ok": True,
            "auth_url": f"https://open.feishu.cn/auth?state={_OAUTH_STATE}",
            "state": _OAUTH_STATE,
        },
    )
    monkeypatch.setattr(esc, "in_quiet_hours", lambda: False)
    monkeypatch.setattr(
        esc,
        "start_callback_listener",
        lambda state, **_kwargs: {"started": True, "pid": 456},
    )
    state = {"sent": {"REAUTH_REQUIRED|doc||9999": 1000}}

    recovered = esc.row_escalation(_doc_row("OK", expires_at=9999), send=True, state=state, now=2000)

    assert recovered["recovery_cleared_keys"] == ["REAUTH_REQUIRED|doc||9999"]
    assert state["sent"] == {}
    calls = []

    def fake_send(args):
        calls.append(args)
        return json.dumps({"success": True, "message_id": "om_again"})

    again = esc.row_escalation(_doc_row("REAUTH_REQUIRED", expires_at=9999), send=True, state=state, send_func=fake_send, now=2010, cooldown_seconds=3600)

    assert again["suppressed"] is False
    assert again["sent"] is True
    assert calls
    assert state["sent"]["REAUTH_REQUIRED|doc||9999"] == 2010


def test_main_loads_dotenv_before_send(monkeypatch, tmp_path, capsys):
    health = tmp_path / "health.json"
    health.write_text('{"rows":[]}', encoding="utf-8")
    calls = []
    monkeypatch.setattr(esc, "load_send_environment", lambda: calls.append("load") or [])
    monkeypatch.setattr(esc, "run", lambda rows, **kwargs: calls.append(("run", kwargs.get("send"))) or {"ok": True, "results": []})

    rc = esc.main(["--health-json", str(health), "--send", "--json"])

    assert rc == 0
    assert calls[0] == "load"
    assert calls[1] == ("run", True)


def test_main_dry_run_does_not_load_dotenv(monkeypatch, tmp_path):
    health = tmp_path / "health.json"
    health.write_text('{"rows":[]}', encoding="utf-8")
    monkeypatch.setattr(
        esc,
        "run",
        lambda rows, **kwargs: {"ok": True, "results": [], "send": kwargs["send"]},
    )

    assert esc.main(["--health-json", str(health), "--json"]) == 0


def test_main_returns_failure_when_escalation_is_not_ok(monkeypatch, tmp_path):
    health = tmp_path / "health.json"
    health.write_text('{"rows":[]}', encoding="utf-8")
    monkeypatch.setattr(esc, "load_send_environment", lambda: [])
    monkeypatch.setattr(
        esc,
        "run",
        lambda rows, **kwargs: {"ok": False, "results": []},
    )

    assert esc.main(["--health-json", str(health), "--json"]) == 2


@pytest.mark.parametrize(
    "body",
    [
        "{}",
        '{"rows":[]}',
        '{"rows":[null]}',
        '{"rows":[{}]}',
        '{"rows":[{"surface":"doc","health":"UNKNOWN"}]}',
        "not-json",
    ],
)
def test_main_fails_closed_when_health_rows_are_unusable(
    monkeypatch, tmp_path, body
):
    health = tmp_path / "health.json"
    health.write_text(body, encoding="utf-8")
    monkeypatch.setattr(esc, "load_send_environment", lambda: [])

    assert esc.main(["--health-json", str(health), "--json"]) == 2


def test_run_fails_closed_on_empty_health_rows():
    assert esc.run([]) == {
        "ok": False,
        "error_class": "HEALTH_ROWS_EMPTY",
        "results": [],
    }


@pytest.mark.parametrize(
    "rows",
    [
        [{}],
        [_doc_row("UNKNOWN"), _project_row("OK"), _meegle_row("OK")],
        [_doc_row("OK"), _project_row("OK")],
        [_doc_row("OK"), _project_row("OK"), _project_row("OK")],
    ],
)
def test_run_fails_closed_on_invalid_health_schema(rows):
    result = esc.run(rows)

    assert result["ok"] is False
    assert result["error_class"] == "HEALTH_ROWS_INVALID"
    assert result["results"] == []


def test_doc_dry_run_does_not_start_oauth_or_expose_callback_material(monkeypatch):
    monkeypatch.setattr(esc, "_load_user_id_mapping", lambda: {"ou_d1d3cfeba1be0a22faa36aaf4fb3907d": "胡子豪"})
    monkeypatch.setattr(esc, "resolve_display_name", lambda open_id: "胡子豪")
    monkeypatch.setattr(
        esc,
        "get_doc_auth_url",
        lambda: (_ for _ in ()).throw(AssertionError("dry-run must not start OAuth")),
    )
    monkeypatch.setattr(esc, "in_quiet_hours", lambda: False)

    result = esc.row_escalation(_doc_row(), state={"sent": {}})

    assert result["dry_run"] is True
    assert "auth_url" not in result
    assert "auth_state" not in result
    assert "https://" not in result["preview"]


def test_doc_send_starts_callback_listener(monkeypatch, tmp_path):
    monkeypatch.setattr(esc, "_load_user_id_mapping", lambda: {"ou_d1d3cfeba1be0a22faa36aaf4fb3907d": "胡子豪"})
    monkeypatch.setattr(esc, "STATE_PATH", tmp_path / "state.json")
    auth_url = f"https://open.feishu.cn/auth?state={_OAUTH_STATE}"
    monkeypatch.setattr(
        esc,
        "get_doc_auth_url",
        lambda: {"ok": True, "auth_url": auth_url, "state": _OAUTH_STATE},
    )
    monkeypatch.setattr(esc, "in_quiet_hours", lambda: False)
    events = []
    monkeypatch.setattr(
        esc,
        "start_callback_listener",
        lambda state, **_kwargs: events.append(("listener", state))
        or {"started": True, "pid": 123},
    )

    sent_payloads = []
    result = esc.row_escalation(
        _doc_row(expires_at=9999),
        send=True,
        state={"sent": {}},
        send_func=lambda args: events.append(("send", None))
        or sent_payloads.append(args)
        or json.dumps(
            {
                "success": True,
                "state": _OAUTH_STATE,
                "auth_url": auth_url,
                "echo": args["message"],
            }
        ),
        now=1000,
    )

    assert result["sent"] is True
    assert events == [("listener", _OAUTH_STATE), ("send", None)]
    assert result["callback_listener"]["started"] is True
    assert auth_url in sent_payloads[0]["message"]
    rendered = json.dumps(result, ensure_ascii=False)
    assert auth_url not in rendered
    assert _OAUTH_STATE not in rendered


def test_failed_send_stops_ready_callback_listener(tmp_path, monkeypatch):
    monkeypatch.setattr(esc, "_load_user_id_mapping", lambda: {"ou_d1d3cfeba1be0a22faa36aaf4fb3907d": "胡子豪"})
    monkeypatch.setattr(esc, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(esc, "FALLBACK_PATH", tmp_path / "fallback.json")
    monkeypatch.setattr(
        esc,
        "get_doc_auth_url",
        lambda: {
            "ok": True,
            "auth_url": f"https://open.feishu.cn/auth?state={_OAUTH_STATE}",
            "state": _OAUTH_STATE,
        },
    )
    events = []

    class Proc:
        def poll(self):
            return None

        def terminate(self):
            events.append("terminate")

        def wait(self, *, timeout):
            events.append(("wait", timeout))
            return 0

    monkeypatch.setattr(
        esc,
        "start_callback_listener",
        lambda _state, **_kwargs: events.append("listener")
        or {"started": True, "pid": 999, "_process": Proc()},
    )
    monkeypatch.setattr(esc, "_notify_fallback", lambda _alert: {"attempted": True, "ok": False})

    result = esc.row_escalation(
        _doc_row(),
        send=True,
        state={"sent": {}},
        send_func=lambda _args: events.append("send")
        or {"success": False, "error": "send failed"},
        now=1000,
    )

    assert result["sent"] is False
    assert events == ["listener", "send", "terminate", ("wait", 2)]


def test_listener_failure_sends_explicit_paste_code_fallback(monkeypatch, tmp_path):
    monkeypatch.setattr(
        esc,
        "_load_user_id_mapping",
        lambda: {"ou_d1d3cfeba1be0a22faa36aaf4fb3907d": "胡子豪"},
    )
    monkeypatch.setattr(esc, "STATE_PATH", tmp_path / "state.json")
    auth_url = f"https://open.feishu.cn/auth?state={_OAUTH_STATE}"
    monkeypatch.setattr(
        esc,
        "get_doc_auth_url",
        lambda: {"ok": True, "auth_url": auth_url, "state": _OAUTH_STATE},
    )
    monkeypatch.setattr(
        esc,
        "start_callback_listener",
        lambda _state, **_kwargs: {
            "started": False,
            "reason": "listener_not_ready",
            "returncode": 2,
        },
    )
    sent_payloads = []

    result = esc.row_escalation(
        _doc_row(),
        send=True,
        state={"sent": {}},
        send_func=lambda args: sent_payloads.append(args) or {"success": True},
        now=1000,
    )

    assert result["sent"] is True
    assert result["callback_listener"]["started"] is False
    assert "本机自动回调当前不可用" in sent_payloads[0]["message"]
    assert "callback URL" in sent_payloads[0]["message"]


def test_doc_quiet_hours_suppresses_send(monkeypatch):
    monkeypatch.setattr(esc, "_load_user_id_mapping", lambda: {"ou_d1d3cfeba1be0a22faa36aaf4fb3907d": "胡子豪"})
    monkeypatch.setattr(
        esc,
        "get_doc_auth_url",
        lambda: {
            "ok": True,
            "auth_url": f"https://open.feishu.cn/auth?state={_OAUTH_STATE}",
            "state": _OAUTH_STATE,
        },
    )
    monkeypatch.setattr(esc, "in_quiet_hours", lambda: True)
    calls = []

    result = esc.row_escalation(_doc_row(expires_at=8888), send=True, state={"sent": {}}, send_func=lambda args: calls.append(args) or json.dumps({"success": True}))

    assert result["quiet_hours_suppressed"] is True
    assert "sent" not in result
    assert calls == []


def test_callback_listener_waits_for_child_readiness_without_state_argv(monkeypatch):
    captured = {}
    secret_values = {
        "FEISHU_DEFAULT_APP_SECRET": "dummy-default-secret",
        "FEISHU_APP_SECRET": "dummy-app-secret",
        "API_TOKEN": "dummy-api-token",
        "SSH_AUTH_SOCK": "/tmp/dummy-agent.sock",
    }
    for key, value in secret_values.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("FEISHU_APP_ID", "dummy-app-id")
    monkeypatch.setenv("LISTENER_BENIGN_MARKER", "preserved")

    class Proc:
        pid = 321

        def poll(self):
            return None

    def fake_popen(command, **kwargs):
        captured["command"] = command
        captured["env"] = kwargs["env"]
        os.write(kwargs["pass_fds"][0], b"READY\n")
        return Proc()

    result = esc.start_callback_listener(
        _OAUTH_STATE,
        popen_factory=fake_popen,
        select_fn=lambda reads, _writes, _errors, _timeout: (reads, [], []),
    )

    assert result["started"] is True
    assert result["pid"] == 321
    command = " ".join(captured["command"])
    assert _OAUTH_STATE not in command
    assert "--state" not in command
    assert "--code" not in command
    assert secret_values.keys().isdisjoint(captured["env"])
    assert set(secret_values.values()).isdisjoint(captured["env"].values())
    assert captured["env"]["FEISHU_APP_ID"] == "dummy-app-id"
    assert captured["env"]["LISTENER_BENIGN_MARKER"] == "preserved"


def test_callback_listener_readiness_timeout_terminates_child_with_rc2():
    events = []

    class Proc:
        pid = 654

        def poll(self):
            return None

        def terminate(self):
            events.append("terminate")

        def wait(self, *, timeout):
            events.append(("wait", timeout))
            return 2

    result = esc.start_callback_listener(
        _OAUTH_STATE,
        popen_factory=lambda *_args, **_kwargs: Proc(),
        select_fn=lambda *_args, **_kwargs: ([], [], []),
    )

    assert result == {
        "started": False,
        "reason": "readiness_timeout",
        "returncode": 2,
    }
    assert events == ["terminate", ("wait", 2)]


def test_callback_listener_runtime_timeout_returns_rc2_and_closes(capsys):
    now = [0.0]
    servers = []
    ready_read, ready_write = os.pipe()

    class Server:
        timeout = None
        closed = False

        def handle_request(self):
            now[0] = 601.0

        def server_close(self):
            self.closed = True

    def server_factory(*_args):
        server = Server()
        servers.append(server)
        return server

    try:
        rc = esc.run_callback_listener(
            ttl_seconds=600,
            ready_fd=ready_write,
            server_factory=server_factory,
            clock=lambda: now[0],
        )
        ready_write = -1
        signal = os.read(ready_read, 64)
    finally:
        if ready_write >= 0:
            os.close(ready_write)
        os.close(ready_read)

    assert rc == 2
    assert signal == b"READY\n"
    assert servers[0].closed is True
    assert '"ok": false' in capsys.readouterr().out


def test_callback_handler_applies_bounded_request_socket_timeout(capsys):
    now = [0.0]
    captured = {}

    class Server:
        timeout = None

        def handle_request(self):
            now[0] = 601.0

        def server_close(self):
            return None

    def server_factory(_address, handler):
        captured["handler"] = handler
        return Server()

    assert esc.run_callback_listener(
        ttl_seconds=600,
        server_factory=server_factory,
        clock=lambda: now[0],
    ) == 2

    timeouts = []

    class Request:
        def settimeout(self, value):
            timeouts.append(value)

        def makefile(self, _mode, _buffering=None):
            return io.BytesIO()

        def sendall(self, _payload):
            return None

    handler = captured["handler"].__new__(captured["handler"])
    handler.request = Request()
    handler.setup()

    assert timeouts == [pytest.approx(0.1)]


def test_callback_listener_cli_does_not_load_dotenv(monkeypatch):
    calls = []
    monkeypatch.setattr(
        esc,
        "run_callback_listener",
        lambda **kwargs: calls.append(kwargs) or 2,
    )

    assert esc.main(["--callback-listener", "--ttl-seconds", "600"]) == 2
    assert calls == [{"ttl_seconds": 600, "ready_fd": None}]


@pytest.mark.parametrize(
    "payload",
    [
        {"success": True},
        {"success": True, "isError": False, "error": ""},
    ],
)
def test_strict_success_payload_accepts_only_explicit_clean_success(payload):
    assert esc._strict_success_payload(payload) is True


@pytest.mark.parametrize(
    "payload",
    [{"success": "true"}, {"success": True, "isError": "false"}, {"success": True, "error": "failed"}],
)
def test_strict_success_payload_rejects_truthy_or_contradictory_success(payload):
    assert esc._strict_success_payload(payload) is False


def test_auth_url_rejects_non_feishu_redirect(monkeypatch):
    result = esc.get_doc_auth_url(
        call_tool=lambda *_args, **_kwargs: {
            "structuredContent": {
                "authUrl": "https://evil.example/auth?state=s1",
                "state": _OAUTH_STATE,
            }
        }
    )

    assert result == {"ok": False, "error": "auth_url_invalid"}


def test_auth_url_rejects_non_cryptographic_state():
    result = esc.get_doc_auth_url(
        call_tool=lambda *_args, **_kwargs: {
            "structuredContent": {
                "authUrl": "https://open.feishu.cn/auth?state=short",
                "state": "short",
            }
        }
    )

    assert result == {"ok": False, "error": "auth_url_invalid"}


def test_default_doc_auth_path_uses_direct_oauth_without_legacy_mcp(monkeypatch):
    monkeypatch.setattr(
        esc,
        "_call_feishu_doc_tool",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("legacy MCP must remain disabled")
        ),
    )
    monkeypatch.setattr(
        esc,
        "start_doc_oauth",
        lambda **kwargs: {
            "ok": True,
            "auth_url": "https://open.feishu.cn/open-apis/authen/v1/authorize?state=" + "s" * 43,
            "state": "s" * 43,
            "appId": esc.APP_ID,
        },
    )
    monkeypatch.setattr(
        esc,
        "complete_doc_oauth",
        lambda **kwargs: {
            "success": True,
            "appId": esc.APP_ID,
            "expiresAt": 1234,
            "userInfo": {"name": "胡子豪", "userId": "fefb829e"},
        },
    )

    started = esc.get_doc_auth_url()
    completed = esc.call_doc_auth_callback(code="code", state="s" * 43)

    assert started["ok"] is True
    assert started["state"] == "s" * 43
    assert completed["isError"] is False
    assert completed["structuredContent"]["success"] is True


def test_oauth_start_cli_does_not_load_secret_environment(monkeypatch, capsys):
    monkeypatch.setattr(
        esc,
        "get_doc_auth_url",
        lambda: {"ok": True, "auth_url": "https://open.feishu.cn/auth", "state": "s" * 43},
    )
    monkeypatch.setattr(
        esc,
        "load_send_environment",
        lambda: (_ for _ in ()).throw(AssertionError("OAuth start must not load secrets")),
    )

    rc = esc.main(["--oauth-start", "--json"])

    assert rc == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True


def test_oauth_callback_cli_reads_stdin_without_loading_environment(monkeypatch, capsys):
    events = []
    monkeypatch.setattr(
        esc,
        "call_doc_auth_callback",
        lambda **kwargs: events.append(("callback", kwargs))
        or {"structuredContent": {"success": True}, "isError": False},
    )
    monkeypatch.setattr(
        esc.sys,
        "stdin",
        io.StringIO(
            json.dumps({"code": "valid-code", "state": _OAUTH_STATE}) + "\n"
        ),
    )

    rc = esc.main(["--oauth-callback", "--json"])

    assert rc == 0
    assert events == [("callback", {"code": "valid-code", "state": _OAUTH_STATE})]
    output = capsys.readouterr().out
    assert json.loads(output)["structuredContent"]["success"] is True
    assert "valid-code" not in output
    assert _OAUTH_STATE not in output


def test_oauth_callback_cli_rejects_code_and_state_argv(capsys):
    dummy_code = "DUMMY_AUTH_CODE_MUST_NOT_APPEAR"
    dummy_state = "z" * 43
    with pytest.raises(SystemExit) as raised:
        esc.main(
            [
                "--oauth-callback",
                "--code",
                dummy_code,
                "--state",
                dummy_state,
                "--json",
            ]
        )

    assert raised.value.code == 2
    error = capsys.readouterr().err
    assert "must be provided via stdin" in error
    assert dummy_code not in error
    assert dummy_state not in error


@pytest.mark.parametrize(
    "callback_input",
    [
        (
            "http://localhost:3010/oauth/feishu/callback?"
            "code=DUMMY_POSITIONAL_CODE&state=" + "z" * 43
        ),
        json.dumps(
            {"code": "DUMMY_POSITIONAL_CODE", "state": "z" * 43}
        ),
    ],
)
def test_oauth_callback_cli_rejects_positional_input_without_echo(
    callback_input,
    capsys,
):
    with pytest.raises(SystemExit) as raised:
        esc.main(["--oauth-callback", "--json", callback_input])

    assert raised.value.code == 2
    error = capsys.readouterr().err
    assert "must be provided via stdin" in error
    assert "DUMMY_POSITIONAL_CODE" not in error
    assert "z" * 43 not in error


def test_invalid_callback_input_fails_before_callback_or_dotenv(monkeypatch, capsys):
    monkeypatch.setattr(esc.sys, "stdin", io.StringIO("https://evil.example/callback\n"))
    monkeypatch.setattr(
        esc,
        "call_doc_auth_callback",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("invalid input must not reach callback")
        ),
    )

    assert esc.main(["--oauth-callback", "--json"]) == 2
    output = json.loads(capsys.readouterr().out)
    assert output["error_class"] == "OAUTH_INPUT_INVALID"


def test_callback_input_uses_hidden_tty_prompt():
    class TTY(io.StringIO):
        def isatty(self):
            return True

    prompts = []
    code, state = esc._read_oauth_callback_input(
        stream=TTY(),
        prompt_func=lambda prompt: prompts.append(prompt)
        or f"http://localhost:3010/oauth/feishu/callback?code=tty-code&state={_OAUTH_STATE}",
    )

    assert (code, state) == ("tty-code", _OAUTH_STATE)
    assert prompts and "callback" in prompts[0].lower()


@pytest.mark.parametrize(
    "value",
    [
        f"http://127.0.0.1:3010/oauth/feishu/callback?code=c&state={_OAUTH_STATE}",
        f"http://localhost:3010/oauth/feishu/callback?code=c&state={_OAUTH_STATE}&extra=1",
        f"http://localhost:3010/oauth/feishu/callback?code=c&code=d&state={_OAUTH_STATE}",
        f'{{"code":"c","state":"{_OAUTH_STATE}","extra":1}}',
    ],
)
def test_callback_input_parser_rejects_contract_drift(value):
    with pytest.raises(ValueError):
        esc._parse_oauth_callback_input(value)


def test_state_symlink_is_unreadable_fail_closed(tmp_path):
    target = tmp_path / "target.json"
    target.write_text('{"sent": {}}', encoding="utf-8")
    link = tmp_path / "state.json"
    link.symlink_to(target)

    state, error = esc.load_escalation_state(link)

    assert state == {"sent": {}}
    assert error == "state_unreadable"


def test_escalation_state_writer_and_reader_require_0600(tmp_path, monkeypatch):
    path = tmp_path / "state.json"
    monkeypatch.setattr(esc, "STATE_PATH", path)

    esc._write_escalation_state({"sent": {}})

    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert esc.load_escalation_state(path) == ({"sent": {}}, None)
    path.chmod(0o644)
    assert esc.load_escalation_state(path) == ({"sent": {}}, "state_unreadable")


def test_credential_send_scopes_live_transport_and_restores_record_only(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_OUTBOUND_MODE", "record-only")
    monkeypatch.setattr(esc, "_load_user_id_mapping", lambda: {"ou_d1d3cfeba1be0a22faa36aaf4fb3907d": "胡子豪"})
    monkeypatch.setattr(esc, "resolve_display_name", lambda open_id: "胡子豪")
    monkeypatch.setattr(esc, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(esc, "get_doc_auth_url", lambda: {"ok": False})
    monkeypatch.setattr(esc, "in_quiet_hours", lambda: False)
    seen = []

    def fake_send(args):
        seen.append(os.environ.get("HERMES_OUTBOUND_MODE"))
        return json.dumps({"success": True, "message_id": "om_test"})

    result = esc.row_escalation(
        _doc_row(),
        send=True,
        state={"sent": {}},
        send_func=fake_send,
        now=1000,
    )

    assert result["sent"] is True
    assert seen == ["live"]
    assert os.environ["HERMES_OUTBOUND_MODE"] == "record-only"


def test_credential_send_rejects_unknown_outbound_mode_before_sender(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_OUTBOUND_MODE", "record-onyl")
    monkeypatch.setattr(esc, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(esc, "FALLBACK_PATH", tmp_path / "fallback.json")
    monkeypatch.setattr(esc, "_load_user_id_mapping", lambda: {"ou_d1d3cfeba1be0a22faa36aaf4fb3907d": "胡子豪"})
    monkeypatch.setattr(esc, "resolve_display_name", lambda _open_id: "胡子豪")
    monkeypatch.setattr(esc, "in_quiet_hours", lambda: False)
    monkeypatch.setattr(esc, "_notify_fallback", lambda _alert: {"attempted": True, "ok": False, "error": "test"})
    calls = []

    result = esc.row_escalation(
        _doc_row(),
        send=True,
        state={"sent": {}},
        send_func=lambda args: calls.append(args) or {"success": True},
        now=1000,
    )

    assert result["sent"] is False
    assert calls == []
    assert os.environ["HERMES_OUTBOUND_MODE"] == "record-onyl"


def test_failed_send_persists_secret_free_fallback_and_notifies(tmp_path, monkeypatch):
    fallback_path = tmp_path / "shared" / "fallback.json"
    monkeypatch.setattr(esc, "FALLBACK_PATH", fallback_path)
    calls = []

    def fake_notify(args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(esc.subprocess, "run", fake_notify)
    row = _doc_row(owner="胡子豪")
    row["auth_url"] = "https://open.feishu.cn/?access_token=do-not-persist"
    result = esc.persist_fallback(row, reason="record-only: token=do-not-persist", now=1000)

    assert result["persisted"] is True
    assert fallback_path.exists()
    assert stat.S_IMODE(fallback_path.stat().st_mode) == 0o600
    body = json.loads(fallback_path.read_text())
    dumped = json.dumps(body, ensure_ascii=False)
    assert "do-not-persist" not in dumped
    assert "access_token" not in dumped
    assert calls and calls[0][0][0] == "/usr/bin/osascript"


def test_quoted_json_and_callback_material_are_redacted_from_safe_results():
    raw = json.dumps(
        {
            "client_secret": "quoted-client-secret",
            "access_token": "quoted-access-token",
            "code": "DUMMY_AUTH_CODE_ABC",
            "state": _OAUTH_STATE,
            "auth_url": f"https://open.feishu.cn/auth?state={_OAUTH_STATE}",
        }
    )

    rendered = esc._fallback_text(raw, 2_000)
    safe = esc._safe_send_value(json.loads(raw))
    combined = rendered + json.dumps(safe, ensure_ascii=False)

    assert "quoted-client-secret" not in combined
    assert "quoted-access-token" not in combined
    assert "DUMMY_AUTH_CODE_ABC" not in combined
    assert _OAUTH_STATE not in combined
    assert "https://open.feishu.cn" not in combined


def test_query_style_callback_code_and_state_are_redacted():
    rendered = esc._fallback_text(
        f"callback failed code=DUMMY_QUERY_CODE state={_OAUTH_STATE}",
        2_000,
    )

    assert "DUMMY_QUERY_CODE" not in rendered
    assert _OAUTH_STATE not in rendered


def test_fallback_notification_is_attempted_when_file_write_fails(monkeypatch):
    row = _doc_row()
    notifications = []
    monkeypatch.setattr(esc, "_write_fallback_alert", lambda alert: (_ for _ in ()).throw(OSError("disk full")))
    monkeypatch.setattr(
        esc,
        "_notify_fallback",
        lambda alert: notifications.append(alert) or {"attempted": True, "ok": False, "error": "test"},
    )

    result = esc.persist_fallback(row, reason="sender failed", now=1000)

    assert result["persisted"] is False
    assert result["notification"]["attempted"] is True
    assert notifications and notifications[0]["surface"] == "doc"


def test_notification_uses_argv_and_records_osascript_failure():
    calls = []

    def fake_runner(args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(returncode=1)

    result = esc._notify_fallback(
        {"surface": 'doc"; do shell script "bad', "health": "PROBE_FAILED"},
        runner=fake_runner,
    )

    assert result == {"attempted": True, "ok": False, "error": "osascript_failed"}
    assert calls[0][0][0] == "/usr/bin/osascript"
    assert calls[0][0][1] == "-e"
    script = calls[0][0][2]
    assert script.startswith("display notification \"")
    assert '\\"; do shell script \\\"bad' in script
    assert calls[0][1]["timeout"] == 5


def test_sender_exception_isolated_per_row_and_fallback_is_recorded(tmp_path, monkeypatch):
    monkeypatch.setattr(esc, "FALLBACK_PATH", tmp_path / "fallback.json")
    monkeypatch.setattr(esc, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(esc, "_load_user_id_mapping", lambda: {"ou_d1d3cfeba1be0a22faa36aaf4fb3907d": "胡子豪"})
    monkeypatch.setattr(esc, "resolve_display_name", lambda open_id: "胡子豪")
    monkeypatch.setattr(esc, "get_doc_auth_url", lambda: {"ok": False})
    monkeypatch.setattr(esc, "in_quiet_hours", lambda: False)
    monkeypatch.setattr(
        esc,
        "_notify_fallback",
        lambda alert: {"attempted": True, "ok": False, "error": "test"},
    )

    def broken_sender(_args):
        raise RuntimeError("record-only configuration refused outbound")

    result = esc.run(
        _all_rows(),
        send=True,
        send_func=broken_sender,
        now=1000,
    )

    assert len(result["results"]) == 3
    assert result["ok"] is False
    assert all(item["sent"] is False for item in result["results"])
    assert len(json.loads((tmp_path / "fallback.json").read_text())["alerts"]) == 3


def test_non_object_sender_result_is_failure_and_is_persisted(tmp_path, monkeypatch):
    monkeypatch.setattr(esc, "FALLBACK_PATH", tmp_path / "fallback.json")
    monkeypatch.setattr(esc, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(esc, "_load_user_id_mapping", lambda: {"ou_d1d3cfeba1be0a22faa36aaf4fb3907d": "胡子豪"})
    monkeypatch.setattr(esc, "resolve_display_name", lambda open_id: "胡子豪")
    monkeypatch.setattr(esc, "in_quiet_hours", lambda: False)
    monkeypatch.setattr(esc, "_notify_fallback", lambda alert: {"attempted": True, "ok": False, "error": "test"})

    result = esc.row_escalation(
        _doc_row(),
        send=True,
        state={"sent": {}},
        send_func=lambda _args: "[]",
        now=1000,
    )

    assert result["sent"] is False
    assert result["send_result"]["raw"] == []
    assert result["fallback"]["persisted"] is True


def test_non_boolean_sender_success_does_not_suppress_fallback(tmp_path, monkeypatch):
    monkeypatch.setattr(esc, "FALLBACK_PATH", tmp_path / "fallback.json")
    monkeypatch.setattr(esc, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(esc, "_load_user_id_mapping", lambda: {"ou_d1d3cfeba1be0a22faa36aaf4fb3907d": "胡子豪"})
    monkeypatch.setattr(esc, "resolve_display_name", lambda open_id: "胡子豪")
    monkeypatch.setattr(esc, "in_quiet_hours", lambda: False)
    monkeypatch.setattr(esc, "_notify_fallback", lambda alert: {"attempted": True, "ok": False, "error": "test"})

    result = esc.row_escalation(
        _doc_row(),
        send=True,
        state={"sent": {}},
        send_func=lambda _args: json.dumps({"success": "false"}),
        now=1000,
    )

    assert result["sent"] is False
    assert result["fallback"]["persisted"] is True


@pytest.mark.parametrize(
    "payload",
    [
        {"success": True, "isError": "true"},
        {"success": True, "isError": 1},
        {"success": True, "error": "failed"},
    ],
)
def test_contradictory_sender_success_is_failure(tmp_path, monkeypatch, payload):
    monkeypatch.setattr(esc, "FALLBACK_PATH", tmp_path / "fallback.json")
    monkeypatch.setattr(esc, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(esc, "_load_user_id_mapping", lambda: {"ou_d1d3cfeba1be0a22faa36aaf4fb3907d": "胡子豪"})
    monkeypatch.setattr(esc, "resolve_display_name", lambda open_id: "胡子豪")
    monkeypatch.setattr(esc, "in_quiet_hours", lambda: False)
    monkeypatch.setattr(esc, "_notify_fallback", lambda alert: {"attempted": True, "ok": False, "error": "test"})

    result = esc.row_escalation(
        _doc_row(),
        send=True,
        state={"sent": {}},
        send_func=lambda _args: payload,
        now=1000,
    )

    assert result["sent"] is False
    assert result["fallback"]["persisted"] is True


def test_malformed_sent_ledger_refuses_before_external_send(tmp_path, monkeypatch):
    monkeypatch.setattr(esc, "FALLBACK_PATH", tmp_path / "fallback.json")
    monkeypatch.setattr(esc, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(
        esc,
        "_notify_fallback",
        lambda alert: {"attempted": True, "ok": False, "error": "test"},
    )
    calls = []

    result = esc.row_escalation(
        _doc_row(),
        send=True,
        state={"sent": []},
        send_func=lambda args: calls.append(args) or json.dumps({"success": True}),
        now=1000,
    )

    assert result["refused"] is True
    assert result["state_error"] == "state_sent_ledger_invalid"
    assert result["fallback"]["persisted"] is True
    assert calls == []


@pytest.mark.parametrize("timestamp", ["bad", [], True, float("inf")])
def test_malformed_sent_entry_refuses_before_external_send(tmp_path, monkeypatch, timestamp):
    monkeypatch.setattr(esc, "FALLBACK_PATH", tmp_path / "fallback.json")
    monkeypatch.setattr(esc, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(esc, "_notify_fallback", lambda alert: {"attempted": True, "ok": False, "error": "test"})
    calls = []
    key = esc.notify_key_for_row(_doc_row())

    result = esc.row_escalation(
        _doc_row(),
        send=True,
        state={"sent": {key: timestamp}},
        send_func=lambda args: calls.append(args) or {"success": True},
        now=1000,
    )

    assert result["refused"] is True
    assert result["state_error"] == "state_sent_entry_invalid"
    assert calls == []


def test_suppressed_doc_does_not_generate_auth_url(monkeypatch):
    monkeypatch.setattr(esc, "_load_user_id_mapping", lambda: {"ou_d1d3cfeba1be0a22faa36aaf4fb3907d": "胡子豪"})
    key = esc.notify_key_for_row(_doc_row())
    calls = []
    monkeypatch.setattr(esc, "get_doc_auth_url", lambda: calls.append("auth") or {})

    result = esc.row_escalation(
        _doc_row(), state={"sent": {key: 1000}}, now=1001, cooldown_seconds=3600
    )

    assert result["suppressed"] is True
    assert calls == []


def test_quiet_doc_does_not_generate_auth_url(monkeypatch):
    monkeypatch.setattr(esc, "_load_user_id_mapping", lambda: {"ou_d1d3cfeba1be0a22faa36aaf4fb3907d": "胡子豪"})
    monkeypatch.setattr(esc, "in_quiet_hours", lambda: True)
    calls = []
    monkeypatch.setattr(esc, "get_doc_auth_url", lambda: calls.append("auth") or {})

    result = esc.row_escalation(_doc_row(), state={"sent": {}})

    assert result["quiet_hours_suppressed"] is True
    assert calls == []


def test_run_accepts_injected_state_without_duplicate_keyword(monkeypatch):
    monkeypatch.setattr(esc, "_load_user_id_mapping", lambda: {})

    result = esc.run(
        _all_rows(doc="OK", project="PROBE_FAILED", meegle="OK"),
        send=False,
        state={"sent": {}},
        default_open_id="",
    )

    assert result["ok"] is False
    assert result["results"][1]["reason"] == "owner_open_id_unresolved"


def test_run_uses_state_lock_for_send(monkeypatch):
    events = []

    @contextlib.contextmanager
    def fake_lock():
        events.append("locked")
        yield
        events.append("unlocked")

    monkeypatch.setattr(esc, "escalation_state_lock", fake_lock)
    monkeypatch.setattr(esc, "_load_user_id_mapping", lambda: {})
    monkeypatch.setattr(
        esc,
        "_persist_fallback_safely",
        lambda *args, **kwargs: {"persisted": False, "error": "test"},
    )

    esc.run(
        _all_rows(doc="OK", project="PROBE_FAILED", meegle="OK"),
        send=True,
        state={"sent": {}},
        default_open_id="",
    )

    assert events == ["locked", "unlocked"]


def test_run_does_not_persist_false_alert_for_healthy_row_exception(monkeypatch):
    monkeypatch.setattr(
        esc,
        "row_escalation",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("broken")),
    )
    fallbacks = []
    monkeypatch.setattr(
        esc,
        "_persist_fallback_safely",
        lambda *args, **kwargs: fallbacks.append((args, kwargs)) or {"persisted": True},
    )

    result = esc.run(
        _all_rows(doc="OK", project="OK", meegle="OK"),
        send=True,
        state={"sent": {}},
    )

    assert all(item["sent"] is False for item in result["results"])
    assert all("fallback" not in item for item in result["results"])
    assert fallbacks == []


def test_fallback_is_cleared_when_surface_recovers(tmp_path, monkeypatch):
    path = tmp_path / "fallback.json"
    monkeypatch.setattr(esc, "FALLBACK_PATH", path)
    row = _doc_row()
    monkeypatch.setattr(esc, "_notify_fallback", lambda alert: {"attempted": True, "ok": False, "error": "test"})
    esc.persist_fallback(row, reason="failed", now=1000)
    assert json.loads(path.read_text())["alerts"]

    monkeypatch.setattr(esc, "STATE_PATH", tmp_path / "state.json")
    result = esc.row_escalation({**row, "health": "OK"}, send=True, state={"sent": {}})

    assert result["skipped"] is True
    assert json.loads(path.read_text())["alerts"] == []


def test_recovery_clears_fallback_even_when_sent_state_write_fails(tmp_path, monkeypatch):
    path = tmp_path / "fallback.json"
    monkeypatch.setattr(esc, "FALLBACK_PATH", path)
    row = _doc_row()
    monkeypatch.setattr(
        esc,
        "_notify_fallback",
        lambda alert: {"attempted": True, "ok": False, "error": "test"},
    )
    esc.persist_fallback(row, reason="failed", now=1000)
    monkeypatch.setattr(
        esc,
        "_write_escalation_state",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("state locked")),
    )

    result = esc.row_escalation(
        {**row, "health": "OK"}, send=True, state={"sent": {"REAUTH_REQUIRED|doc||9999": 1000}}
    )

    assert result["skipped"] is True
    assert "state_error" in result
    assert json.loads(path.read_text())["alerts"] == []


def test_dry_run_does_not_write_fallback_or_call_notification(tmp_path, monkeypatch):
    monkeypatch.setattr(esc, "FALLBACK_PATH", tmp_path / "fallback.json")
    monkeypatch.setattr(esc, "_load_user_id_mapping", lambda: {"ou_d1d3cfeba1be0a22faa36aaf4fb3907d": "胡子豪"})
    monkeypatch.setattr(esc, "resolve_display_name", lambda open_id: "胡子豪")
    monkeypatch.setattr(esc, "get_doc_auth_url", lambda: {"ok": False})
    monkeypatch.setattr(esc, "in_quiet_hours", lambda: False)
    monkeypatch.setattr(esc, "_notify_fallback", lambda alert: (_ for _ in ()).throw(AssertionError("not called")))

    result = esc.row_escalation(_doc_row(), send=False, state={"sent": {}})

    assert result["dry_run"] is True
    assert not (tmp_path / "fallback.json").exists()
