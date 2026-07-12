import json
import subprocess

from scripts import feishu_doc_token_keepwarm as keepwarm


def _write_auth(path, *, expires_at=1000, owner="胡子豪"):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "__mcp": {"v": 1},
                "value": {
                    "appId": keepwarm.APP_ID,
                    "accessToken": "SHOULD_NOT_APPEAR",
                    "refreshToken": "SHOULD_NOT_APPEAR",
                    "appSecret": "SHOULD_NOT_APPEAR",
                    "expiresAt": expires_at,
                    "userInfo": {"name": owner, "userId": "fefb829e"},
                },
            }
        ),
        encoding="utf-8",
    )


def test_read_auth_metadata_uses_only_non_secret_fields(tmp_path):
    auth = tmp_path / "auth" / keepwarm.APP_ID
    _write_auth(auth, expires_at=1234)

    meta = keepwarm.read_auth_metadata(auth)

    assert meta == {"exists": True, "expiresAt": 1234, "owner": "胡子豪"}
    assert "token" not in json.dumps(meta, ensure_ascii=False).lower()
    assert "secret" not in json.dumps(meta, ensure_ascii=False).lower()


def test_classify_real_feishu_refresh_errors_as_reauth_required():
    samples = [
        "Error: Token 刷新失败",
        "OAuth error 99991665 refresh_token 无效",
        "code=99991666 invalid refresh token",
    ]
    for sample in samples:
        assert keepwarm.classify_error(sample) == "REAUTH_REQUIRED"


def test_classify_other_mcp_errors_as_probe_failed():
    assert keepwarm.classify_error("network timeout from MCP server") == "PROBE_FAILED"


def test_keepwarm_success_reports_rotation_without_tokens(tmp_path, monkeypatch):
    auth = tmp_path / "auth" / keepwarm.APP_ID
    _write_auth(auth, expires_at=1000)
    monkeypatch.setattr(keepwarm, "AUTH_PATH", auth)

    def fake_call():
        _write_auth(auth, expires_at=2000)
        return {"structuredContent": {"name": "胡子豪"}}

    monkeypatch.setattr(keepwarm, "call_feishu_get_user_info", fake_call)

    rc, result = keepwarm.keepwarm()

    assert rc == 0
    assert result["health"] == "OK"
    assert result["rotated"] is True
    assert result["before_expiresAt"] == 1000
    assert result["after_expiresAt"] == 2000
    assert result["owner"] == "胡子豪"
    dumped = json.dumps(result, ensure_ascii=False).lower()
    assert "should_not_appear" not in dumped
    assert "token" not in dumped
    assert "secret" not in dumped


def test_keepwarm_valid_access_no_rotation_is_ok(tmp_path, monkeypatch):
    auth = tmp_path / "auth" / keepwarm.APP_ID
    _write_auth(auth, expires_at=1000)
    monkeypatch.setattr(keepwarm, "AUTH_PATH", auth)
    monkeypatch.setattr(keepwarm, "call_feishu_get_user_info", lambda: {"structuredContent": {"name": "胡子豪"}})

    rc, result = keepwarm.keepwarm()

    assert rc == 0
    assert result["health"] == "OK"
    assert result["rotated"] is False
    assert result["before_expiresAt"] == result["after_expiresAt"] == 1000


def test_keepwarm_refresh_token_error_is_nonzero_reauth_required(tmp_path, monkeypatch):
    auth = tmp_path / "auth" / keepwarm.APP_ID
    _write_auth(auth, expires_at=1000)
    monkeypatch.setattr(keepwarm, "AUTH_PATH", auth)

    def fail():
        raise keepwarm.KeepwarmError("Error: Token 刷新失败 99991665", error_class="MCP_TOOL_ERROR")

    monkeypatch.setattr(keepwarm, "call_feishu_get_user_info", fail)

    rc, result = keepwarm.keepwarm()

    assert rc != 0
    assert result["health"] == "REAUTH_REQUIRED"
    assert result["error_class"] == "MCP_TOOL_ERROR"
    assert result["rotated"] is False


def test_call_feishu_get_user_info_invokes_real_mcp_tool_shape(monkeypatch, tmp_path):
    caller = tmp_path / "caller.mjs"
    monkeypatch.setattr(keepwarm, "MCP_CALLER", caller)

    class Proc:
        returncode = 0
        stdout = json.dumps({"structuredContent": {"userId": "fefb829e", "name": "胡子豪"}})
        stderr = ""

    calls = []

    def fake_run(args, **kwargs):
        calls.append((args, kwargs))
        assert args[0] == "node"
        source = caller.read_text(encoding="utf-8")
        assert "feishu_get_user_info" in source
        assert "appId:'cli_a99b38e0a29b500b'" in source
        assert "feishu_auth_callback" not in source
        return Proc()

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = keepwarm.call_feishu_get_user_info()

    assert result["structuredContent"]["userId"] == "fefb829e"
    assert calls
