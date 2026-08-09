from __future__ import annotations

import json
from pathlib import Path

import pytest

from gateway.record_only import runtime
from hermes_constants import reset_hermes_home_override, set_hermes_home_override
from scripts import pnc_meegle_auth_watchdog as wd


ROLLING_EXPIRES = [119, 102, 85, 68, 51, 34, 17, 0, 119, 102]


def cfg(tmp_path: Path, **kw):
    base = dict(
        warn_min=45,
        crit_min=20,
        re_alert_seconds=7200,
        expired_confirm_checks=2,
        unknown_confirm_checks=2,
        dry_run=False,
        send=True,
        try_assist=True,
        owner_open_id="ou_owner",
        owner_name="胡子豪",
        alert_target="feishu:ou_owner",
        state_path=tmp_path / "state.json",
        host_default="project.feishu.cn",
        quiet_start="00:00",
        quiet_end="00:00",
        proactive_reinit_hours=24,
        proactive_auto_roll_count=3,
    )
    base.update(kw)
    return wd.WatchdogConfig(**base)


def deps(statuses, *, sent=None, runner=None, now=1000.0):
    calls = {"i": 0}

    def status_func():
        i = min(calls["i"], len(statuses) - 1)
        calls["i"] += 1
        return dict(statuses[i])

    sent = sent if sent is not None else []

    def send_func(args):
        sent.append(args)
        return json.dumps({"success": True, "message_id": "om_x"})

    def default_runner(args):
        return 0, json.dumps({
            "verification_url": "https://verify.example",
            "user_code": "ABCD-EFGH",
            "device_code": "DEVICE-SECRET",
        }), ""

    return wd.WatchdogDeps(status_func=status_func, send_func=send_func, runner=runner or default_runner, now_func=lambda: now), sent


def auth(expires=100, authenticated=True, **kw):
    data = {"ok": authenticated is True, "authenticated": authenticated, "expires_in_minutes": expires, "host": "project.feishu.cn", "error": ""}
    data.update(kw)
    return data


def record_only_env(tmp_path: Path, monkeypatch):
    root = tmp_path / "records"
    root.mkdir(mode=0o700)
    key_file = tmp_path / "record.key"
    key_file.write_text("ab" * 32 + "\n", encoding="ascii")
    key_file.chmod(0o600)
    census_root = Path(__file__).resolve().parents[3] / "evidence" / "target-outbound-census"
    monkeypatch.setenv("HERMES_OUTBOUND_MODE", "record-only")
    monkeypatch.setenv("HERMES_OUTBOUND_RECORD_ROOT", str(root))
    monkeypatch.setenv("HERMES_OUTBOUND_RECORD_KEY_FILE", str(key_file))
    monkeypatch.setenv("HERMES_OUTBOUND_CENSUS_ROOT", str(census_root))
    runtime._reset_for_tests()
    return root, set_hermes_home_override(tmp_path)


def test_healthy_does_not_alert(tmp_path):
    d, sent = deps([auth(106)])
    result = wd.run_once(cfg(tmp_path), d)
    assert result["state"] == "healthy"
    assert result["alert_sent"] is False
    assert result["consecutive_expired"] == 0
    assert result["consecutive_unknown"] == 0
    assert sent == []


def test_no_assist_cli_flag_disables_watchdog_assist(monkeypatch, capsys):
    captured = []

    def fake_run_once(config):
        captured.append(config)
        return {"ok": True, "state": "healthy"}

    monkeypatch.setattr(wd, "run_once", fake_run_once)

    assert wd.main(["--once", "--json", "--no-assist"]) == 0
    assert captured[0].try_assist is False
    assert json.loads(capsys.readouterr().out)["state"] == "healthy"


def test_live_mode_keeps_injected_status_runner_and_sender_semantics(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_OUTBOUND_MODE", "live")
    sent = []
    d = deps([auth(None, authenticated=False), auth(None, authenticated=False)], sent=sent)[0]
    config = cfg(tmp_path, try_assist=False)
    first = wd.run_once(config, d)
    second = wd.run_once(config, d)

    assert first["alert_sent"] is False
    assert second["alert_sent"] is True
    assert second["assist"] is None
    assert len(sent) == 1


def test_record_only_mode_skips_reload_env_protected_root_read(monkeypatch):
    monkeypatch.setenv("HERMES_OUTBOUND_MODE", "record-only")

    def bomb_reload():
        raise AssertionError("reload_env attempted a protected-root read")

    monkeypatch.setattr(wd, "reload_env", bomb_reload)
    wd._reload_env_for_current_mode()


def test_partial_record_only_config_fails_before_original_watchdog_deps_or_state(tmp_path, monkeypatch):
    home_token = set_hermes_home_override(tmp_path)
    monkeypatch.setenv("HERMES_OUTBOUND_MODE", "record-only")
    records = tmp_path / "records"
    records.mkdir(mode=0o700)
    monkeypatch.setenv("HERMES_OUTBOUND_RECORD_ROOT", str(records))
    monkeypatch.delenv("HERMES_OUTBOUND_RECORD_KEY_FILE", raising=False)
    runtime._reset_for_tests()

    def bomb(*_args, **_kwargs):
        raise AssertionError("real watchdog dependency was touched")

    try:
        with pytest.raises(runtime.RecordOnlyConfigurationError, match="KEY_FILE"):
            wd.run_once(
                cfg(tmp_path),
                wd.WatchdogDeps(status_func=bomb, runner=bomb, send_func=bomb),
            )
        assert not (tmp_path / "state.json").exists()
    finally:
        runtime._reset_for_tests()
        reset_hermes_home_override(home_token)


def test_authenticated_expires_roll_119_to_0_to_119_never_alerts_or_assists(tmp_path):
    (tmp_path / "state.json").write_text(json.dumps({
        "last_state": "healthy",
        "last_expires": 0,
        "consecutive_auto_rolls": 3,
        "last_proactive_reinit_at": 0,
    }))
    runner_calls = []

    def runner(args):
        runner_calls.append(args)
        return 0, json.dumps({"verification_url": "https://verify.example", "user_code": "ABCD-EFGH"}), ""

    sent = []
    results = []
    for idx, expires in enumerate(ROLLING_EXPIRES):
        d, _ = deps([auth(expires)], sent=sent, runner=runner, now=1000 + idx * 1019)
        results.append(wd.run_once(cfg(tmp_path, try_assist=False), d))

    assert {r["state"] for r in results} == {"healthy"}
    assert all(r["alert_sent"] is False for r in results)
    assert all(r["confirmed_failure"] is False for r in results)
    assert all(r["proactive_reinit"] is False for r in results)
    assert sent == []
    assert runner_calls == []
    state = json.loads((tmp_path / "state.json").read_text())
    assert state["consecutive_expired"] == 0
    assert state["consecutive_unknown"] == 0


def test_single_expired_is_transient_no_alert_and_counts_one(tmp_path):
    d, sent = deps([auth(None, authenticated=False, error="expired")])
    result = wd.run_once(cfg(tmp_path), d)

    assert result["state"] == "expired"
    assert result["consecutive_expired"] == 1
    assert result["confirmed_failure"] is False
    assert result["alert_sent"] is False
    assert result["assist"] is None
    assert sent == []
    state = json.loads((tmp_path / "state.json").read_text())
    assert state["consecutive_expired"] == 1


def test_consecutive_expired_alerts_once_assists_and_rate_limits(tmp_path):
    runner_calls = []

    def runner(args):
        runner_calls.append(args)
        return 0, json.dumps({
            "verification_url": "https://verify.example",
            "user_code": "ABCD-EFGH",
            "device_code": "DEVICE-SECRET",
        }), ""

    sent = []
    r1 = wd.run_once(cfg(tmp_path), deps([auth(None, authenticated=False)], sent=sent, runner=runner, now=1000)[0])
    r2 = wd.run_once(cfg(tmp_path), deps([auth(None, authenticated=False)], sent=sent, runner=runner, now=1000 + 1019)[0])
    r3 = wd.run_once(cfg(tmp_path), deps([auth(None, authenticated=False)], sent=sent, runner=runner, now=1000 + 2 * 1019)[0])
    r4 = wd.run_once(cfg(tmp_path), deps([auth(None, authenticated=False)], sent=sent, runner=runner, now=1000 + 7200 + 1020)[0])

    assert r1["alert_sent"] is False
    assert r1["consecutive_expired"] == 1
    assert r2["confirmed_failure"] is True
    assert r2["alert_sent"] is True
    assert r2["consecutive_expired"] == 2
    assert r3["alert_sent"] is False
    assert r4["alert_sent"] is True
    assert len(sent) == 2
    assert runner_calls == [[
        "auth",
        "login",
        "--device-code",
        "--host",
        "project.feishu.cn",
        "--phase",
        "init",
        "--once",
    ]] * 2
    first_msg = sent[0]["message"]
    assert '<at user_id="ou_owner">' in first_msg
    assert "胡子豪" in first_msg
    assert "meegle auth login --device-code" in first_msg
    assert "verification_url=https://verify.example" in first_msg
    assert "user_code=ABCD-EFGH" in first_msg
    assert "DEVICE-SECRET" not in first_msg
    assert "device_code" not in first_msg


def test_expired_confirm_checks_env_three_requires_three(tmp_path, monkeypatch):
    monkeypatch.setenv("PNC_MEEGLE_EXPIRED_CONFIRM_CHECKS", "3")
    sent = []
    config = wd.config_from_env(state_path=tmp_path / "state.json")
    config.alert_target = "feishu:ou_owner"
    config.owner_open_id = "ou_owner"
    config.owner_name = "胡子豪"
    config.quiet_start = "00:00"
    config.quiet_end = "00:00"

    r1 = wd.run_once(config, deps([auth(None, authenticated=False)], sent=sent, now=1000)[0])
    r2 = wd.run_once(config, deps([auth(None, authenticated=False)], sent=sent, now=2000)[0])
    r3 = wd.run_once(config, deps([auth(None, authenticated=False)], sent=sent, now=3000)[0])

    assert config.expired_confirm_checks == 3
    assert r1["alert_sent"] is False
    assert r2["alert_sent"] is False
    assert r2["consecutive_expired"] == 2
    assert r3["alert_sent"] is True
    assert r3["consecutive_expired"] == 3
    assert len(sent) == 1




def test_single_expired_then_healthy_does_not_send_recovery_noise(tmp_path):
    sent = []
    wd.run_once(cfg(tmp_path), deps([auth(None, authenticated=False)], sent=sent, now=1000)[0])
    result = wd.run_once(cfg(tmp_path), deps([auth(119)], sent=sent, now=2000)[0])

    assert result["state"] == "healthy"
    assert result["alert_sent"] is False
    assert result["consecutive_expired"] == 0
    assert sent == []

def test_expired_recovery_sends_closing_alert_and_clears_counts(tmp_path):
    sent = []
    wd.run_once(cfg(tmp_path), deps([auth(None, authenticated=False)], sent=sent, now=1000)[0])
    wd.run_once(cfg(tmp_path), deps([auth(None, authenticated=False)], sent=sent, now=2000)[0])
    result = wd.run_once(cfg(tmp_path), deps([auth(88)], sent=sent, now=3000)[0])

    assert result["state"] == "healthy"
    assert result["alert_sent"] is True
    assert result["consecutive_expired"] == 0
    assert result["consecutive_unknown"] == 0
    assert "已恢复" in sent[-1]["message"]
    state = json.loads((tmp_path / "state.json").read_text())
    assert state["consecutive_expired"] == 0
    assert state["consecutive_unknown"] == 0


def test_unknown_single_no_alert_consecutive_n_alerts(tmp_path):
    sent = []
    config = cfg(tmp_path, try_assist=False)
    r1 = wd.run_once(config, deps([auth(None, authenticated=None, ok=False, error="meegle CLI not found")], sent=sent, now=1000)[0])
    r2 = wd.run_once(config, deps([auth(None, authenticated=None, ok=False, error="meegle CLI not found")], sent=sent, now=2000)[0])

    assert r1["state"] == "unknown"
    assert r1["consecutive_unknown"] == 1
    assert r1["alert_sent"] is False
    assert r2["confirmed_failure"] is True
    assert r2["alert_sent"] is True
    assert r2["assist"] is None
    assert len(sent) == 1
    assert "unknown，不等同过期" in sent[0]["message"]
    assert "已过期/未授权" not in sent[0]["message"]


def test_warn_critical_classification_no_longer_alerts_or_assists(tmp_path):
    runner_calls = []

    def runner(args):
        runner_calls.append(args)
        return 0, json.dumps({"verification_url": "https://verify.example", "user_code": "ABCD-EFGH"}), ""

    sent = []
    r1 = wd.run_once(cfg(tmp_path), deps([auth(40)], sent=sent, runner=runner, now=1000)[0])
    r2 = wd.run_once(cfg(tmp_path), deps([auth(0)], sent=sent, runner=runner, now=2000)[0])

    assert r1["state"] == "healthy"
    assert r2["state"] == "healthy"
    assert r1["alert_sent"] is False
    assert r2["alert_sent"] is False
    assert r2["silent_refresh_probe"] is None
    assert sent == []
    assert runner_calls == []


def test_confirm_guard_would_fail_if_single_expired_alerted(tmp_path):
    d, sent = deps([auth(None, authenticated=False, error="expired")])
    result = wd.run_once(cfg(tmp_path), d)

    assert result["consecutive_expired"] == 1
    assert result["confirmed_failure"] is False
    assert result["alert_sent"] is False
    assert sent == []


def test_redacts_token_fields_from_message_and_state(tmp_path):
    sent = []
    wd.run_once(cfg(tmp_path), deps([auth(None, authenticated=None, ok=False, error="access_token=abc123 token: zzz")], sent=sent, now=1000)[0])
    wd.run_once(cfg(tmp_path), deps([auth(None, authenticated=None, ok=False, error="access_token=abc123 token: zzz")], sent=sent, now=2000)[0])
    assert "abc123" not in sent[0]["message"]
    assert "zzz" not in sent[0]["message"]
    assert "abc123" not in (tmp_path / "state.json").read_text()


def test_redact_device_code_but_preserves_user_code_and_verification_url():
    assert wd.SECRET_KEY_RE.search("user_code") is None
    assert wd.SECRET_KEY_RE.search("verification_url") is None

    redacted = wd.redact({
        "device_code": "X",
        "device_code_value": "Y",
        "user_code": "WXYZ-1234",
        "verification_url": "https://verify.example",
        "nested": "device_code=FREEFORM-SECRET user_code=WXYZ-1234",
    })

    assert redacted["device_code"] == "[REDACTED]"
    assert redacted["device_code_value"] == "[REDACTED]"
    assert redacted["user_code"] == "WXYZ-1234"
    assert redacted["verification_url"] == "https://verify.example"
    assert "FREEFORM-SECRET" not in redacted["nested"]
    assert "user_code=WXYZ-1234" in redacted["nested"]


def test_alert_message_redacts_device_code_but_keeps_human_material(tmp_path):
    message = wd.build_alert_message(
        state="expired",
        status=auth(None, authenticated=False),
        config=cfg(tmp_path),
        assist={
            "verification_url": "https://verify.example",
            "user_code": "WXYZ-1234",
            "device_code": "SECRET",
        },
    )

    assert "verification_url=https://verify.example" in message
    assert "user_code=WXYZ-1234" in message
    assert "SECRET" not in message
    assert "device_code" not in message


def test_proactive_init_failure_keeps_manual_command_separate_from_error(tmp_path):
    message = wd.build_alert_message(
        state="healthy",
        status=auth(107),
        config=cfg(tmp_path),
        assist={
            "ok": False,
            "error": "ExternalWriteFenceError: command denied",
        },
        proactive=True,
    )

    lines = message.splitlines()
    assert "meegle auth login --device-code --host project.feishu.cn" in lines
    assert "error=ExternalWriteFenceError: command denied" in lines
    assert "project.feishu.cn。error=" not in message


def test_quiet_hours_suppresses_confirmed_meegle_alert(tmp_path):
    sent = []
    wd.run_once(cfg(tmp_path, quiet_start="22:00", quiet_end="08:00"), deps([auth(None, authenticated=False)], sent=sent, now=23 * 3600)[0])
    result = wd.run_once(cfg(tmp_path, quiet_start="22:00", quiet_end="08:00"), deps([auth(None, authenticated=False)], sent=sent, now=23 * 3600 + 1019)[0])

    assert result["state"] == "expired"
    assert result["confirmed_failure"] is True
    assert result["quiet_hours_suppressed"] is True
    assert result["alert_sent"] is False
    assert sent == []


def test_long_auto_roll_without_device_code_init_triggers_proactive_reinit(tmp_path):
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps({
        "last_state": "healthy",
        "last_expires": 0,
        "consecutive_auto_rolls": 2,
        "last_proactive_reinit_at": 1000.0,
    }))
    runner_calls = []

    def runner(args):
        runner_calls.append(args)
        return 0, json.dumps({"verification_url": "https://verify.example", "user_code": "ABCD-EFGH", "device_code": "SECRET"}), ""

    sent = []
    config = cfg(tmp_path, proactive_reinit_hours=1, proactive_auto_roll_count=3)
    result = wd.run_once(config, deps([auth(119)], sent=sent, runner=runner, now=1000.0 + 7200)[0])

    assert result["state"] == "healthy"
    assert result["proactive_reinit"] is True
    assert result["alert_sent"] is True
    assert runner_calls == [[
        "auth",
        "login",
        "--device-code",
        "--host",
        "project.feishu.cn",
        "--phase",
        "init",
        "--once",
    ]]
    assert '<at user_id="ou_owner">' in sent[0]["message"]
    assert "需扫码续期" in sent[0]["message"]
    assert "SECRET" not in sent[0]["message"]
    saved = json.loads(state_path.read_text())
    assert saved["last_auth_init_ok"] is True
    assert saved["auth_init_success_count"] == 1
    assert saved["last_proactive_reinit_at"] == 1000.0 + 7200
    assert isinstance(float(saved["last_proactive_reinit_at"]), float)
    assert "last_device_code_init_at" not in saved


def test_record_only_confirmed_expiry_records_status_device_and_alert_without_real_deps(tmp_path, monkeypatch):
    _records, home_token = record_only_env(tmp_path, monkeypatch)
    statuses = [auth(None, authenticated=False), auth(None, authenticated=False)]
    status_index = {"value": 0}

    def fixture_status():
        index = min(status_index["value"], len(statuses) - 1)
        status_index["value"] += 1
        return dict(statuses[index])

    def bomb(*_args, **_kwargs):
        raise AssertionError("real watchdog dependency was touched")

    deps_value = wd.WatchdogDeps(
        status_func=bomb,
        record_only_status_func=fixture_status,
        runner=bomb,
        send_func=bomb,
        now_func=lambda: 1000.0,
    )
    try:
        first = wd.run_once(cfg(tmp_path), deps_value)
        second = wd.run_once(cfg(tmp_path), deps_value)
        transport = runtime.get_record_only_transport("scripts.pnc_meegle_auth_watchdog")
        assert transport is not None
        rows = transport.read_all()
    finally:
        runtime._reset_for_tests()
        reset_hermes_home_override(home_token)

    assert first["confirmed_failure"] is False
    assert first["alert_sent"] is False
    assert second["confirmed_failure"] is True
    assert second["alert_sent"] is True
    assert [row["operation"] for row in rows] == [
        "auth_status_check",
        "auth_device_init",
        "text_send",
    ]
    assert rows[0]["attempt_count"] == 2
    assert rows[1]["external_delivery_attempted"] is False
    assert rows[2]["external_delivery_attempted"] is False
    assert second["alert_result"]["external_delivery_verified"] is False


def test_record_only_quiet_hours_preserve_confirm_without_device_or_alert(tmp_path, monkeypatch):
    _records, home_token = record_only_env(tmp_path, monkeypatch)

    def bomb(*_args, **_kwargs):
        raise AssertionError("real watchdog dependency was touched")

    deps_value = wd.WatchdogDeps(
        status_func=bomb,
        record_only_status_func=lambda: auth(None, authenticated=False),
        runner=bomb,
        send_func=bomb,
        now_func=lambda: 23 * 3600,
    )
    quiet_config = cfg(tmp_path, quiet_start="22:00", quiet_end="08:00")
    try:
        wd.run_once(quiet_config, deps_value)
        result = wd.run_once(quiet_config, deps_value)
        transport = runtime.get_record_only_transport("scripts.pnc_meegle_auth_watchdog")
        assert transport is not None
        rows = transport.read_all()
    finally:
        runtime._reset_for_tests()
        reset_hermes_home_override(home_token)

    assert result["confirmed_failure"] is True
    assert result["quiet_hours_suppressed"] is True
    assert result["alert_sent"] is False
    assert [row["operation"] for row in rows] == ["auth_status_check"]
    assert rows[0]["attempt_count"] == 2


def test_record_only_preserves_rate_limit_and_recovery_with_original_deps_bombed(tmp_path, monkeypatch):
    _records, home_token = record_only_env(tmp_path, monkeypatch)
    statuses = [
        auth(None, authenticated=False),
        auth(None, authenticated=False),
        auth(None, authenticated=False),
        auth(88),
    ]
    times = [1000.0, 2000.0, 2500.0, 3000.0]
    indexes = {"status": 0, "time": 0}

    def fixture_status():
        value = statuses[indexes["status"]]
        indexes["status"] += 1
        return dict(value)

    def next_time():
        value = times[indexes["time"]]
        indexes["time"] += 1
        return value

    def bomb(*_args, **_kwargs):
        raise AssertionError("real watchdog dependency was touched")

    deps_value = wd.WatchdogDeps(
        status_func=bomb,
        record_only_status_func=fixture_status,
        runner=bomb,
        send_func=bomb,
        now_func=next_time,
    )
    try:
        first = wd.run_once(cfg(tmp_path), deps_value)
        alerted = wd.run_once(cfg(tmp_path), deps_value)
        rate_limited = wd.run_once(cfg(tmp_path), deps_value)
        recovered = wd.run_once(cfg(tmp_path), deps_value)
        transport = runtime.get_record_only_transport("scripts.pnc_meegle_auth_watchdog")
        assert transport is not None
        rows = transport.read_all()
    finally:
        runtime._reset_for_tests()
        reset_hermes_home_override(home_token)

    assert first["alert_sent"] is False
    assert alerted["alert_sent"] is True
    assert rate_limited["alert_sent"] is False
    assert recovered["alert_sent"] is True
    assert recovered["state"] == "healthy"
    assert [row["operation"] for row in rows] == [
        "auth_status_check",
        "auth_device_init",
        "text_send",
        "text_send",
    ]
    assert rows[0]["attempt_count"] == 4


def test_proactive_reinit_cooldown_survives_save_load_roundtrip(tmp_path):
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps({
        "last_state": "healthy",
        "last_expires": 0,
        "consecutive_auto_rolls": 2,
        "last_proactive_reinit_at": 0,
    }))
    runner_calls = []

    def runner(args):
        runner_calls.append(args)
        return 0, json.dumps({"verification_url": "https://verify.example", "user_code": "ABCD-EFGH", "device_code": "SECRET"}), ""

    sent = []
    config = cfg(tmp_path, proactive_reinit_hours=24, proactive_auto_roll_count=3)
    first = wd.run_once(config, deps([auth(119)], sent=sent, runner=runner, now=5000.0)[0])
    saved = wd.load_state(state_path)

    assert first["proactive_reinit"] is True
    assert first["alert_sent"] is True
    assert len(runner_calls) == 1
    assert saved["last_proactive_reinit_at"] == 5000.0
    assert float(saved["last_proactive_reinit_at"]) == 5000.0
    assert "last_device_code_init_at" not in saved
    assert saved["auth_init_success_count"] == 1

    second = wd.run_once(config, deps([auth(119)], sent=sent, runner=runner, now=5060.0)[0])
    third = wd.run_once(config, deps([auth(119)], sent=sent, runner=runner, now=5120.0)[0])

    assert second["proactive_reinit"] is False
    assert third["proactive_reinit"] is False
    assert second["alert_sent"] is False
    assert third["alert_sent"] is False
    assert len(runner_calls) == 1
    assert len(sent) == 1
    saved_again = wd.load_state(state_path)
    assert float(saved_again["last_proactive_reinit_at"]) == 5000.0
    assert "last_device_code_init_at" not in saved_again


def test_legacy_redacted_device_code_init_key_migrates_after_one_trigger(tmp_path):
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps({
        "last_state": "healthy",
        "last_expires": 0,
        "consecutive_auto_rolls": 2,
        "last_device_code_init_at": "[REDACTED]",
        "auth_init_success_count": 4,
    }))
    runner_calls = []

    def runner(args):
        runner_calls.append(args)
        return 0, json.dumps({"verification_url": "https://verify.example", "user_code": "ABCD-EFGH", "device_code": "SECRET"}), ""

    sent = []
    config = cfg(tmp_path, proactive_reinit_hours=24, proactive_auto_roll_count=3)
    first = wd.run_once(config, deps([auth(119)], sent=sent, runner=runner, now=9000.0)[0])
    saved = wd.load_state(state_path)

    assert first["proactive_reinit"] is True
    assert len(runner_calls) == 1
    assert saved["last_proactive_reinit_at"] == 9000.0
    assert float(saved["last_proactive_reinit_at"]) == 9000.0
    assert "last_device_code_init_at" not in saved
    assert saved["auth_init_success_count"] == 5

    second = wd.run_once(config, deps([auth(119)], sent=sent, runner=runner, now=9060.0)[0])

    assert second["proactive_reinit"] is False
    assert len(runner_calls) == 1
    assert len(sent) == 1


def test_proactive_reinit_scan_required_state_mentions_owner(tmp_path):
    message = wd.build_alert_message(
        state="healthy",
        status=auth(119),
        config=cfg(tmp_path),
        assist={"verification_url": "https://verify.example", "user_code": "WXYZ-1234", "device_code": "SECRET"},
        proactive=True,
    )

    assert '<at user_id="ou_owner">' in message
    assert "胡子豪" in message
    assert "需扫码续期" in message
    assert "verification_url=https://verify.example" in message
    assert "user_code=WXYZ-1234" in message
    assert "SECRET" not in message
    assert "device_code" not in message


def test_healthy_without_auto_roll_threshold_does_not_proactive_reinit(tmp_path):
    runner_calls = []

    def runner(args):
        runner_calls.append(args)
        return 0, json.dumps({"verification_url": "https://verify.example", "user_code": "ABCD-EFGH"}), ""

    sent = []
    result = wd.run_once(cfg(tmp_path, proactive_reinit_hours=1, proactive_auto_roll_count=3), deps([auth(119)], sent=sent, runner=runner, now=1000)[0])

    assert result["state"] == "healthy"
    assert result["proactive_reinit"] is False
    assert result["alert_sent"] is False
    assert sent == []
    assert runner_calls == []
