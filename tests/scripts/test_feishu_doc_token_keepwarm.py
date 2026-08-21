import json
import stat
import urllib.parse

import pytest

from scripts import feishu_doc_token_keepwarm as keepwarm


@pytest.fixture(autouse=True)
def _isolate_live_auth_and_transport(monkeypatch, tmp_path):
    monkeypatch.setattr(keepwarm, "AUTH_PATH", tmp_path / "live-auth-blocked")
    monkeypatch.setattr(keepwarm, "OAUTH_STATE_PATH", tmp_path / "live-state-blocked")
    monkeypatch.setattr(
        keepwarm,
        "_http_json",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("live network disabled in test")
        ),
    )
    monkeypatch.setattr(
        keepwarm,
        "_load_oauth_app_secret",
        lambda: (_ for _ in ()).throw(AssertionError("live dotenv disabled in test")),
    )


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
    path.chmod(0o600)


def test_read_auth_metadata_uses_only_non_secret_fields(tmp_path):
    auth = tmp_path / "auth" / keepwarm.APP_ID
    _write_auth(auth, expires_at=1234)

    meta = keepwarm.read_auth_metadata(auth)

    assert meta == {"exists": True, "expiresAt": 1234, "owner": "胡子豪"}
    assert "token" not in json.dumps(meta, ensure_ascii=False).lower()
    assert "secret" not in json.dumps(meta, ensure_ascii=False).lower()


def test_auth_reader_rejects_non_0600_file_without_reading_it(tmp_path):
    auth = tmp_path / "auth" / keepwarm.APP_ID
    _write_auth(auth)
    auth.chmod(0o644)

    with pytest.raises(keepwarm.KeepwarmError) as raised:
        keepwarm.read_auth_metadata(auth)

    assert raised.value.error_class == "AUTH_STORAGE_ERROR"


def test_auth_reader_rejects_symlink(tmp_path):
    target = tmp_path / "target"
    _write_auth(target)
    auth = tmp_path / "auth-link"
    auth.symlink_to(target)

    with pytest.raises(keepwarm.KeepwarmError) as raised:
        keepwarm.read_auth_metadata(auth)

    assert raised.value.error_class == "AUTH_STORAGE_ERROR"


def test_quoted_json_error_values_are_redacted():
    raw = json.dumps(
        {
            "client_secret": "quoted-client-secret",
            "access_token": "quoted-access-token",
            "message": "request failed",
        }
    )

    rendered = keepwarm._safe_error_message(raw)

    assert "quoted-client-secret" not in rendered
    assert "quoted-access-token" not in rendered
    assert rendered.count("<redacted>") == 2


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


def test_direct_probe_refreshes_expired_access_token_without_mcp(tmp_path, monkeypatch):
    auth = tmp_path / "auth" / keepwarm.APP_ID
    _write_auth(auth, expires_at=1000)
    monkeypatch.setattr(keepwarm, "AUTH_PATH", auth)
    monkeypatch.setattr(
        keepwarm, "_load_oauth_app_secret", lambda: "app-secret-for-test"
    )
    monkeypatch.setattr(keepwarm, "_now_ms", lambda: 2_000)
    calls = []

    def fake_http(method, url, **kwargs):
        calls.append((method, url, kwargs))
        if method == "POST":
            return {"code": 0, "access_token": "new-access", "refresh_token": "new-refresh", "expires_in": 7200}
        return {"code": 0, "data": {"user_id": "fefb829e", "name": "胡子豪"}}

    monkeypatch.setattr(keepwarm, "_http_json", fake_http)

    result = keepwarm.call_feishu_get_user_info()

    assert result["structuredContent"]["user_id"] == "fefb829e"
    assert [call[0] for call in calls] == ["POST", "GET"]
    assert calls[0][2]["payload"]["grant_type"] == "refresh_token"
    assert "app-secret-for-test" == calls[0][2]["payload"]["client_secret"]
    assert stat.S_IMODE(auth.stat().st_mode) == 0o600
    persisted = json.loads(auth.read_text(encoding="utf-8"))["value"]
    assert persisted["accessToken"] == "new-access"
    assert "new-access" not in json.dumps(result)


def test_direct_probe_uses_valid_access_token_without_node_or_refresh(tmp_path, monkeypatch):
    auth = tmp_path / "auth" / keepwarm.APP_ID
    _write_auth(auth, expires_at=9_999_999_999_000)
    monkeypatch.setattr(keepwarm, "AUTH_PATH", auth)
    calls = []

    def fake_http(method, url, **kwargs):
        calls.append((method, url, kwargs))
        return {"code": 0, "data": {"open_id": "ou_test", "name": "胡子豪"}}

    monkeypatch.setattr(keepwarm, "_http_json", fake_http)

    result = keepwarm.call_feishu_get_user_info()

    assert result["structuredContent"]["open_id"] == "ou_test"
    assert [call[0] for call in calls] == ["GET"]
    assert calls[0][2]["headers"]["Authorization"].startswith("Bearer ")


def test_missing_access_token_keeps_explicit_reauth_classification(tmp_path, monkeypatch):
    auth = tmp_path / "auth" / keepwarm.APP_ID
    _write_auth(auth, expires_at=9_999_999_999_000)
    data = json.loads(auth.read_text(encoding="utf-8"))
    del data["value"]["accessToken"]
    auth.write_text(json.dumps(data), encoding="utf-8")
    monkeypatch.setattr(keepwarm, "AUTH_PATH", auth)

    rc, result = keepwarm.keepwarm()

    assert rc == 2
    assert result["health"] == "REAUTH_REQUIRED"
    assert result["error_class"] == "REAUTH_REQUIRED"


def test_direct_probe_rejects_empty_user_info_response(tmp_path, monkeypatch):
    auth = tmp_path / "auth" / keepwarm.APP_ID
    _write_auth(auth, expires_at=9_999_999_999_000)
    monkeypatch.setattr(keepwarm, "AUTH_PATH", auth)
    monkeypatch.setattr(keepwarm, "_http_json", lambda *args, **kwargs: {"code": 0, "data": {}})

    try:
        keepwarm.call_feishu_get_user_info()
    except keepwarm.KeepwarmError as exc:
        assert exc.error_class == "INVALID_RESPONSE"
    else:
        raise AssertionError("empty user_info response must fail closed")


def test_refresh_write_refuses_to_overwrite_newer_auth_snapshot(tmp_path):
    auth = tmp_path / "auth" / keepwarm.APP_ID
    _write_auth(auth, expires_at=1000)
    original = json.loads(auth.read_text(encoding="utf-8"))
    newer = dict(original)
    newer["value"] = dict(original["value"])
    newer["value"]["expiresAt"] = 2000
    auth.write_text(json.dumps(newer), encoding="utf-8")

    try:
        keepwarm._write_auth_value(original, {**original["value"], "expiresAt": 3000}, auth)
    except keepwarm.KeepwarmError as exc:
        assert exc.error_class == "AUTH_STORAGE_ERROR"
    else:
        raise AssertionError("refresh must not overwrite a newer auth snapshot")
    assert json.loads(auth.read_text(encoding="utf-8"))["value"]["expiresAt"] == 2000


def test_refresh_write_rechecks_noncooperative_writer_before_replace(
    tmp_path, monkeypatch
):
    auth = tmp_path / "auth" / keepwarm.APP_ID
    _write_auth(auth, expires_at=1000)
    original = json.loads(auth.read_text(encoding="utf-8"))
    real_reader = keepwarm._read_bounded_owned_file
    reads = []

    def racing_reader(path, **kwargs):
        reads.append(path)
        if len(reads) == 2:
            _write_auth(auth, expires_at=4000)
        return real_reader(path, **kwargs)

    monkeypatch.setattr(keepwarm, "_read_bounded_owned_file", racing_reader)

    with pytest.raises(keepwarm.KeepwarmError) as raised:
        keepwarm._write_auth_value(
            original,
            {**original["value"], "expiresAt": 3000},
            auth,
        )

    assert raised.value.error_class == "AUTH_STORAGE_ERROR"
    assert json.loads(auth.read_text(encoding="utf-8"))["value"]["expiresAt"] == 4000


def _start_oauth(tmp_path, auth, *, now=1_000):
    state_path = tmp_path / "state" / "oauth.json"
    state = "s" * 43
    result = keepwarm.start_doc_oauth(
        state_path=state_path,
        auth_path=auth,
        now=now,
        state_factory=lambda size: state if size == 32 else "",
    )
    return state_path, state, result


def _exchange_success(**kwargs):
    return {
        "code": 0,
        "access_token": "new-access-token",
        "refresh_token": "new-refresh-token",
        "expires_in": 7200,
    }


def _expected_user(**kwargs):
    return {
        "name": keepwarm.EXPECTED_OWNER_NAME,
        "user_id": keepwarm.EXPECTED_OWNER_USER_ID,
        "open_id": keepwarm.EXPECTED_OWNER_OPEN_ID,
    }


def test_direct_oauth_start_uses_fixed_contract_and_private_one_time_ledger(tmp_path, monkeypatch):
    auth = tmp_path / "auth" / keepwarm.APP_ID
    _write_auth(auth)
    monkeypatch.setattr(
        keepwarm,
        "_app_secret",
        lambda: (_ for _ in ()).throw(AssertionError("OAuth start must not read secrets")),
    )

    state_path, state, result = _start_oauth(tmp_path, auth)

    parsed = urllib.parse.urlparse(result["auth_url"])
    query = urllib.parse.parse_qs(parsed.query)
    assert (parsed.scheme, parsed.netloc, parsed.path) == (
        "https",
        "open.feishu.cn",
        "/open-apis/authen/v1/authorize",
    )
    assert query == {
        "client_id": [keepwarm.APP_ID],
        "redirect_uri": [keepwarm.OAUTH_REDIRECT_URI],
        "scope": [keepwarm.OAUTH_SCOPES],
        "state": [state],
        "response_type": ["code"],
    }
    assert result["expires_in"] == 900
    assert stat.S_IMODE(state_path.stat().st_mode) == 0o600
    ledger_text = state_path.read_text(encoding="utf-8")
    ledger = json.loads(ledger_text)
    assert ledger["status"] == "active"
    assert ledger["expires_at"] - ledger["created_at"] == 900
    assert ledger["auth_snapshot"]["sha256"]
    assert "SHOULD_NOT_APPEAR" not in ledger_text


def test_direct_oauth_start_rejects_symlinked_state_ledger(tmp_path):
    target = tmp_path / "target.json"
    target.write_text("unchanged", encoding="utf-8")
    target.chmod(0o600)
    state_path = tmp_path / "oauth.json"
    state_path.symlink_to(target)

    with pytest.raises(keepwarm.KeepwarmError) as raised:
        keepwarm.start_doc_oauth(
            state_path=state_path,
            auth_path=tmp_path / "missing-auth",
            now=1_000,
            state_factory=lambda _size: "s" * 43,
        )

    assert raised.value.error_class == "AUTH_STORAGE_ERROR"
    assert target.read_text(encoding="utf-8") == "unchanged"


def test_direct_oauth_start_reuses_the_single_unexpired_state(tmp_path):
    auth = tmp_path / "auth" / keepwarm.APP_ID
    _write_auth(auth)
    state_path, state, first = _start_oauth(tmp_path, auth)
    before = state_path.read_bytes()

    second = keepwarm.start_doc_oauth(
        state_path=state_path,
        auth_path=auth,
        now=1_100,
        state_factory=lambda _size: (_ for _ in ()).throw(
            AssertionError("an active state must be reused")
        ),
    )

    assert first["reused"] is False
    assert second["reused"] is True
    assert second["state"] == state
    assert second["expires_in"] == 800
    assert state_path.read_bytes() == before


def test_direct_oauth_start_rejects_non_0600_auth_snapshot(tmp_path):
    auth = tmp_path / "auth" / keepwarm.APP_ID
    _write_auth(auth)
    auth.chmod(0o644)
    state_path = tmp_path / "state.json"

    with pytest.raises(keepwarm.KeepwarmError) as raised:
        keepwarm.start_doc_oauth(
            state_path=state_path,
            auth_path=auth,
            now=1_000,
            state_factory=lambda _size: "s" * 43,
        )

    assert raised.value.error_class == "AUTH_STORAGE_ERROR"
    assert not state_path.exists()


@pytest.mark.parametrize(
    ("kwargs", "error_class"),
    [
        ({"app_id": "cli_wrong"}, "OAUTH_INPUT_INVALID"),
        ({"redirect_uri": "http://127.0.0.1:3010/oauth/feishu/callback"}, "OAUTH_INPUT_INVALID"),
        ({"ttl_seconds": 599}, "OAUTH_INPUT_INVALID"),
        ({"ttl_seconds": 901}, "OAUTH_INPUT_INVALID"),
    ],
)
def test_direct_oauth_start_rejects_contract_drift(tmp_path, kwargs, error_class):
    with pytest.raises(keepwarm.KeepwarmError) as raised:
        keepwarm.start_doc_oauth(
            state_path=tmp_path / "state.json",
            auth_path=tmp_path / "auth.json",
            now=1_000,
            state_factory=lambda _size: "s" * 43,
            **kwargs,
        )

    assert raised.value.error_class == error_class
    assert not (tmp_path / "state.json").exists()


def test_direct_oauth_callback_exchanges_validates_owner_and_atomically_replaces_auth(tmp_path):
    auth = tmp_path / "auth" / keepwarm.APP_ID
    _write_auth(auth)
    state_path, state, _ = _start_oauth(tmp_path, auth)
    calls = []

    def exchange(**kwargs):
        calls.append(("exchange", kwargs))
        return _exchange_success(**kwargs)

    def user_info(**kwargs):
        calls.append(("user_info", kwargs))
        return _expected_user(**kwargs)

    result = keepwarm.complete_doc_oauth(
        code="valid-code",
        state=state,
        state_path=state_path,
        auth_path=auth,
        now=1_100,
        secret_loader=lambda: "test-app-secret",
        exchange_code=exchange,
        fetch_user_info=user_info,
    )

    assert result["success"] is True
    assert result["userInfo"]["userId"] == keepwarm.EXPECTED_OWNER_USER_ID
    assert calls[0][1]["app_id"] == keepwarm.APP_ID
    assert calls[0][1]["redirect_uri"] == keepwarm.OAUTH_REDIRECT_URI
    assert calls[0][1]["app_secret"] == "test-app-secret"
    assert calls[1][1]["access_token"] == "new-access-token"
    assert stat.S_IMODE(auth.stat().st_mode) == 0o600
    assert stat.S_IMODE(
        state_path.with_name(f".{state_path.name}.lock").stat().st_mode
    ) == 0o600
    assert stat.S_IMODE(auth.with_name(f".{auth.name}.lock").stat().st_mode) == 0o600
    persisted = json.loads(auth.read_text(encoding="utf-8"))["value"]
    assert persisted["appId"] == keepwarm.APP_ID
    assert persisted["accessToken"] == "new-access-token"
    assert persisted["refreshToken"] == "new-refresh-token"
    assert persisted["expiresAt"] == 1_100_000 + 7_200_000
    assert "appSecret" not in persisted
    consumed_text = state_path.read_text(encoding="utf-8")
    assert json.loads(consumed_text)["status"] == "consumed"
    assert state not in consumed_text
    rendered = json.dumps(result, ensure_ascii=False)
    assert "new-access-token" not in rendered
    assert "new-refresh-token" not in rendered
    assert "test-app-secret" not in rendered


def test_direct_oauth_callback_state_mismatch_fails_before_secret_or_exchange(tmp_path):
    auth = tmp_path / "auth" / keepwarm.APP_ID
    _write_auth(auth)
    state_path, _state, _ = _start_oauth(tmp_path, auth)
    calls = []

    with pytest.raises(keepwarm.KeepwarmError) as raised:
        keepwarm.complete_doc_oauth(
            code="valid-code",
            state="x" * 43,
            state_path=state_path,
            auth_path=auth,
            now=1_100,
            secret_loader=lambda: calls.append("secret") or "secret",
            exchange_code=lambda **kwargs: calls.append("exchange") or {},
            fetch_user_info=lambda **kwargs: calls.append("user_info") or {},
        )

    assert raised.value.error_class == "OAUTH_STATE_MISMATCH"
    assert calls == []
    assert json.loads(state_path.read_text(encoding="utf-8"))["status"] == "active"


def test_direct_oauth_callback_expiry_does_not_consume_state(tmp_path):
    auth = tmp_path / "auth" / keepwarm.APP_ID
    _write_auth(auth)
    state_path, state, _ = _start_oauth(tmp_path, auth)

    with pytest.raises(keepwarm.KeepwarmError) as raised:
        keepwarm.complete_doc_oauth(
            code="valid-code",
            state=state,
            state_path=state_path,
            auth_path=auth,
            now=1_900,
            secret_loader=lambda: (_ for _ in ()).throw(AssertionError("must not read secret")),
        )

    assert raised.value.error_class == "OAUTH_STATE_EXPIRED"
    assert json.loads(state_path.read_text(encoding="utf-8"))["status"] == "active"


def test_direct_oauth_callback_rejects_non_0600_state_before_secret_read(tmp_path):
    auth = tmp_path / "auth" / keepwarm.APP_ID
    _write_auth(auth)
    state_path, state, _ = _start_oauth(tmp_path, auth)
    state_path.chmod(0o644)

    with pytest.raises(keepwarm.KeepwarmError) as raised:
        keepwarm.complete_doc_oauth(
            code="valid-code",
            state=state,
            state_path=state_path,
            auth_path=auth,
            now=1_100,
            secret_loader=lambda: (_ for _ in ()).throw(
                AssertionError("secret must remain unread")
            ),
        )

    assert raised.value.error_class == "AUTH_STORAGE_ERROR"


def test_direct_oauth_owner_mismatch_preserves_auth_and_active_state(tmp_path):
    auth = tmp_path / "auth" / keepwarm.APP_ID
    _write_auth(auth)
    original = auth.read_bytes()
    state_path, state, _ = _start_oauth(tmp_path, auth)

    with pytest.raises(keepwarm.KeepwarmError) as raised:
        keepwarm.complete_doc_oauth(
            code="valid-code",
            state=state,
            state_path=state_path,
            auth_path=auth,
            now=1_100,
            secret_loader=lambda: "test-app-secret",
            exchange_code=_exchange_success,
            fetch_user_info=lambda **kwargs: {
                "name": "另一位用户",
                "user_id": "other-user",
            },
        )

    assert raised.value.error_class == "OAUTH_OWNER_MISMATCH"
    assert auth.read_bytes() == original
    assert json.loads(state_path.read_text(encoding="utf-8"))["status"] == "active"


@pytest.mark.parametrize(
    "user",
    [
        {
            "name": keepwarm.EXPECTED_OWNER_NAME,
            "user_id": keepwarm.EXPECTED_OWNER_USER_ID,
        },
        {
            "name": keepwarm.EXPECTED_OWNER_NAME,
            "user_id": keepwarm.EXPECTED_OWNER_USER_ID,
            "open_id": "ou_other",
        },
    ],
)
def test_direct_oauth_requires_exact_owner_open_id(tmp_path, user):
    auth = tmp_path / "auth" / keepwarm.APP_ID
    _write_auth(auth)
    original = auth.read_bytes()
    state_path, state, _ = _start_oauth(tmp_path, auth)

    with pytest.raises(keepwarm.KeepwarmError) as raised:
        keepwarm.complete_doc_oauth(
            code="valid-code",
            state=state,
            state_path=state_path,
            auth_path=auth,
            now=1_100,
            secret_loader=lambda: "test-app-secret",
            exchange_code=_exchange_success,
            fetch_user_info=lambda **_kwargs: user,
        )

    assert raised.value.error_class == "OAUTH_OWNER_MISMATCH"
    assert auth.read_bytes() == original
    assert json.loads(state_path.read_text(encoding="utf-8"))["status"] == "active"


def test_direct_oauth_state_validation_precedes_default_secret_loader(tmp_path, monkeypatch):
    auth = tmp_path / "auth" / keepwarm.APP_ID
    _write_auth(auth)
    state_path, _state, _ = _start_oauth(tmp_path, auth)
    monkeypatch.setattr(
        keepwarm,
        "_load_oauth_app_secret",
        lambda: (_ for _ in ()).throw(AssertionError("dotenv must remain unloaded")),
    )

    with pytest.raises(keepwarm.KeepwarmError) as raised:
        keepwarm.complete_doc_oauth(
            code="valid-code",
            state="x" * 43,
            state_path=state_path,
            auth_path=auth,
            now=1_100,
        )

    assert raised.value.error_class == "OAUTH_STATE_MISMATCH"


def test_direct_oauth_refuses_auth_changed_since_start_before_secret_read(tmp_path):
    auth = tmp_path / "auth" / keepwarm.APP_ID
    _write_auth(auth, expires_at=1_000)
    state_path, state, _ = _start_oauth(tmp_path, auth)
    _write_auth(auth, expires_at=2_000)

    with pytest.raises(keepwarm.KeepwarmError) as raised:
        keepwarm.complete_doc_oauth(
            code="valid-code",
            state=state,
            state_path=state_path,
            auth_path=auth,
            now=1_100,
            secret_loader=lambda: (_ for _ in ()).throw(AssertionError("must not read secret")),
        )

    assert raised.value.error_class == "AUTH_STORAGE_ERROR"
    assert json.loads(auth.read_text(encoding="utf-8"))["value"]["expiresAt"] == 2_000
    assert json.loads(state_path.read_text(encoding="utf-8"))["status"] == "active"


def test_direct_oauth_refuses_auth_changed_during_exchange(tmp_path):
    auth = tmp_path / "auth" / keepwarm.APP_ID
    _write_auth(auth, expires_at=1_000)
    state_path, state, _ = _start_oauth(tmp_path, auth)
    user_calls = []

    def exchange(**kwargs):
        _write_auth(auth, expires_at=2_000)
        return _exchange_success(**kwargs)

    with pytest.raises(keepwarm.KeepwarmError) as raised:
        keepwarm.complete_doc_oauth(
            code="valid-code",
            state=state,
            state_path=state_path,
            auth_path=auth,
            now=1_100,
            secret_loader=lambda: "test-app-secret",
            exchange_code=exchange,
            fetch_user_info=lambda **kwargs: user_calls.append(kwargs) or _expected_user(**kwargs),
        )

    assert raised.value.error_class == "AUTH_STORAGE_ERROR"
    assert user_calls == []
    assert json.loads(auth.read_text(encoding="utf-8"))["value"]["expiresAt"] == 2_000
    assert json.loads(state_path.read_text(encoding="utf-8"))["status"] == "active"


def test_direct_oauth_consumed_state_cannot_be_replayed(tmp_path):
    auth = tmp_path / "auth" / keepwarm.APP_ID
    _write_auth(auth)
    state_path, state, _ = _start_oauth(tmp_path, auth)

    def complete():
        return keepwarm.complete_doc_oauth(
            code="valid-code",
            state=state,
            state_path=state_path,
            auth_path=auth,
            now=1_100,
            secret_loader=lambda: "test-app-secret",
            exchange_code=_exchange_success,
            fetch_user_info=_expected_user,
        )

    complete()

    with pytest.raises(keepwarm.KeepwarmError) as raised:
        complete()

    assert raised.value.error_class == "OAUTH_STATE_REPLAY"
