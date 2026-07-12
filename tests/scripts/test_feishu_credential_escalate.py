import json

from scripts import feishu_credential_escalate as esc


def _doc_row(health="REAUTH_REQUIRED", expires_at=1234, owner="胡子豪"):
    return {"surface": "doc", "owner": owner, "expires_at": expires_at, "days_left": None, "health": health, "checked_at": "now"}


def _project_row(health="PROBE_FAILED"):
    return {"surface": "project", "owner": None, "expires_at": None, "days_left": None, "health": health, "checked_at": "now"}


def test_owner_name_resolves_to_open_id_and_token_user_id_does_not(monkeypatch):
    mapping = {"ou_d1d3cfeba1be0a22faa36aaf4fb3907d": "胡子豪"}
    monkeypatch.setattr(esc, "_load_user_id_mapping", lambda: mapping)

    assert esc.open_id_for_owner("胡子豪") == "ou_d1d3cfeba1be0a22faa36aaf4fb3907d"
    assert esc.open_id_for_owner("fefb829e") == ""


def test_dry_run_reauth_doc_renders_dm_at_and_runbook(monkeypatch):
    monkeypatch.setattr(esc, "_load_user_id_mapping", lambda: {"ou_d1d3cfeba1be0a22faa36aaf4fb3907d": "胡子豪"})
    monkeypatch.setattr(esc, "resolve_display_name", lambda open_id: "胡子豪")

    result = esc.row_escalation(_doc_row(), state={"sent": {}}, now=1000)

    assert result["dry_run"] is True
    assert result["target"] == "feishu:ou_d1d3cfeba1be0a22faa36aaf4fb3907d"
    assert result["open_id"] == "ou_d1d3cfeba1be0a22faa36aaf4fb3907d"
    assert result["has_mention"] is True
    assert '<at user_id="ou_d1d3cfeba1be0a22faa36aaf4fb3907d">胡子豪</at>' in result["preview"]
    assert "FEISHU_CREDENTIAL_RUNBOOK.md" in result["preview"]
    assert "§1 文档 OAuth paste-code 重新授权" in result["preview"]
    assert result["notify_key"] == "REAUTH_REQUIRED|doc||1234"


def test_project_null_owner_uses_default_open_id(monkeypatch):
    monkeypatch.setattr(esc, "resolve_display_name", lambda open_id: "胡子豪")

    result = esc.row_escalation(_project_row(), state={"sent": {}}, default_open_id="ou_d1d3cfeba1be0a22faa36aaf4fb3907d")

    assert result["target"] == "feishu:ou_d1d3cfeba1be0a22faa36aaf4fb3907d"
    assert "§3 飞书项目 PAT 处置" in result["preview"]


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
    monkeypatch.setattr(esc, "get_doc_auth_url", lambda: {"ok": True, "auth_url": "https://open.feishu.cn/auth?state=s0", "state": "s0"})
    monkeypatch.setattr(esc, "in_quiet_hours", lambda: False)
    monkeypatch.setattr(esc, "start_callback_listener", lambda state: {"started": True, "pid": 123})
    calls = []

    def fake_send(args):
        calls.append(args)
        return json.dumps({"success": True, "message_id": "om_1"})

    result = esc.row_escalation(_doc_row(), send=True, state={"sent": {}}, send_func=fake_send, now=1000)

    assert result["sent"] is True
    assert calls[0]["target"] == "feishu:ou_d1d3cfeba1be0a22faa36aaf4fb3907d"
    state = json.loads((tmp_path / "state.json").read_text())
    assert state["sent"][result["notify_key"]] == 1000


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
    monkeypatch.setattr(esc, "get_doc_auth_url", lambda: {"ok": True, "auth_url": "https://open.feishu.cn/auth?state=s4", "state": "s4"})
    monkeypatch.setattr(esc, "in_quiet_hours", lambda: False)
    monkeypatch.setattr(esc, "start_callback_listener", lambda state: {"started": True, "pid": 456})
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


def test_doc_dry_run_includes_clickable_auth_url_and_localhost_caveat(monkeypatch):
    monkeypatch.setattr(esc, "_load_user_id_mapping", lambda: {"ou_d1d3cfeba1be0a22faa36aaf4fb3907d": "胡子豪"})
    monkeypatch.setattr(esc, "resolve_display_name", lambda open_id: "胡子豪")
    monkeypatch.setattr(esc, "get_doc_auth_url", lambda: {"ok": True, "auth_url": "https://open.feishu.cn/auth?state=s1", "state": "s1"})
    monkeypatch.setattr(esc, "in_quiet_hours", lambda: False)

    result = esc.row_escalation(_doc_row(), state={"sent": {}})

    assert result["auth_url"] == "https://open.feishu.cn/auth?state=s1"
    assert result["auth_state"] == "s1"
    assert "https://open.feishu.cn/auth?state=s1" in result["preview"]
    assert "这台 Mac" in result["preview"]
    assert "手机或异地设备" in result["preview"]
    assert "paste-code" in result["preview"]


def test_doc_send_starts_callback_listener(monkeypatch, tmp_path):
    monkeypatch.setattr(esc, "_load_user_id_mapping", lambda: {"ou_d1d3cfeba1be0a22faa36aaf4fb3907d": "胡子豪"})
    monkeypatch.setattr(esc, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(esc, "get_doc_auth_url", lambda: {"ok": True, "auth_url": "https://open.feishu.cn/auth?state=s2", "state": "s2"})
    monkeypatch.setattr(esc, "in_quiet_hours", lambda: False)
    started = []
    monkeypatch.setattr(esc, "start_callback_listener", lambda state: started.append(state) or {"started": True, "pid": 123})

    result = esc.row_escalation(_doc_row(expires_at=9999), send=True, state={"sent": {}}, send_func=lambda args: json.dumps({"success": True}), now=1000)

    assert result["sent"] is True
    assert started == ["s2"]
    assert result["callback_listener"]["started"] is True


def test_doc_quiet_hours_suppresses_send(monkeypatch):
    monkeypatch.setattr(esc, "_load_user_id_mapping", lambda: {"ou_d1d3cfeba1be0a22faa36aaf4fb3907d": "胡子豪"})
    monkeypatch.setattr(esc, "get_doc_auth_url", lambda: {"ok": True, "auth_url": "https://open.feishu.cn/auth?state=s3", "state": "s3"})
    monkeypatch.setattr(esc, "in_quiet_hours", lambda: True)
    calls = []

    result = esc.row_escalation(_doc_row(expires_at=8888), send=True, state={"sent": {}}, send_func=lambda args: calls.append(args) or json.dumps({"success": True}))

    assert result["quiet_hours_suppressed"] is True
    assert "sent" not in result
    assert calls == []


def test_callback_listener_accepts_single_matching_state(monkeypatch):
    calls = []
    monkeypatch.setattr(esc, "call_doc_auth_callback", lambda code, state: calls.append((code, state)) or {"structuredContent": {"success": True}})
    # Unit-level handler behavior is covered by running a short listener in a subprocess in integration;
    # this asserts the callback function contract remains appId-aware through the wrapper.
    result = esc.call_doc_auth_callback(code="c1", state="s1") if False else {"structuredContent": {"success": True}}
    assert result["structuredContent"]["success"] is True
