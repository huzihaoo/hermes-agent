import asyncio
import json
import os
import plistlib
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from gateway.record_only import runtime
from hermes_constants import reset_hermes_home_override, set_hermes_home_override
from scripts import pnc_completion_notice_relay
from scripts.pnc_foxglove_delivery import canonical_viz_mcap_path, foxglove_url


def _write_sidecar(tmp_path, task_id="task-1", *, send_status="pending"):
    sidecar = tmp_path / "task-state" / f"{task_id}.json"
    sidecar.parent.mkdir(parents=True)
    sidecar.write_text(json.dumps({
        "completion_notice": {
            "send_status": send_status,
            "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
            "thread_id": "topic:om_1",
            "message_id": "om_1",
            "vm_task_id": "vm-1",
            "text": "完成通知",
        }
    }), encoding="utf-8")
    return sidecar


def _set_record_only_env(tmp_path: Path, monkeypatch):
    records = tmp_path / "records"
    records.mkdir(mode=0o700)
    key_file = tmp_path / "record.key"
    key_file.write_text("ab" * 32 + "\n", encoding="ascii")
    key_file.chmod(0o600)
    census_root = Path(
        os.getenv("HERMES_OUTBOUND_CENSUS_ROOT")
        or Path(__file__).resolve().parents[3] / "evidence" / "target-outbound-census"
    )
    monkeypatch.setenv("HERMES_OUTBOUND_MODE", "record-only")
    monkeypatch.setenv("HERMES_OUTBOUND_RECORD_ROOT", str(records))
    monkeypatch.setenv("HERMES_OUTBOUND_RECORD_KEY_FILE", str(key_file))
    monkeypatch.setenv("HERMES_OUTBOUND_CENSUS_ROOT", str(census_root))
    runtime._reset_for_tests()
    return records


def _install_unit_test_active_relay_fence(monkeypatch):
    """Keep relay state-machine tests behind an explicit active fence stub."""

    def bind(_task_id, send_func, send_card_func):
        def send(args):
            if send_func is None:
                raise AssertionError("unit test did not provide a text sender")
            return send_func(dict(args))

        def send_card(target, payload, message_id=None):
            if send_card_func is None:
                raise AssertionError("unit test did not provide a card sender")
            return send_card_func(target, payload, message_id)

        return send, send_card

    monkeypatch.setattr(
        pnc_completion_notice_relay,
        "_fenced_task_senders",
        bind,
    )


def test_relay_dry_run_builds_feishu_topic_target(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        _write_sidecar(tmp_path)
        result = pnc_completion_notice_relay.relay_pending_notices(task_ids=["task-1"], send=False)
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True
    assert result["candidate_count"] == 1
    assert result["rows"][0]["target"] == f"feishu:{pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1]}:om_1"
    assert result["rows"][0]["preview"] == "完成通知"


def test_record_only_mode_skips_relay_reload_env_protected_root_read(monkeypatch):
    monkeypatch.setenv("HERMES_OUTBOUND_MODE", "record-only")

    def bomb_reload():
        raise AssertionError("relay reload_env attempted a protected-root read")

    monkeypatch.setattr(pnc_completion_notice_relay, "reload_env", bomb_reload)
    pnc_completion_notice_relay._reload_env_for_current_mode()


def test_live_mode_keeps_relay_reload_env_behavior(monkeypatch):
    monkeypatch.setenv("HERMES_OUTBOUND_MODE", "live")
    calls = []
    monkeypatch.setattr(pnc_completion_notice_relay, "reload_env", lambda: calls.append(True))

    pnc_completion_notice_relay._reload_env_for_current_mode()

    assert calls == [True]


def test_misspelled_outbound_mode_refuses_relay_daemon_before_env_reload(
    monkeypatch,
):
    monkeypatch.setenv("HERMES_OUTBOUND_MODE", "record-onyl")
    reload_calls = []
    monkeypatch.setattr(
        pnc_completion_notice_relay,
        "reload_env",
        lambda: reload_calls.append(True),
    )

    with pytest.raises(
        runtime.RecordOnlyConfigurationError,
        match="unsupported HERMES_OUTBOUND_MODE",
    ):
        pnc_completion_notice_relay._reload_env_for_current_mode()

    assert reload_calls == []


def test_hot_sender_auth_retry_revalidates_epoch_before_second_provider_call(
    monkeypatch,
):
    sender = object.__new__(pnc_completion_notice_relay.FeishuHotSender)
    sender._record_sender = None
    live = True
    guard_calls = 0
    provider_calls = 0
    rebuilds = []

    def provider_revalidate(_claim, **_kwargs):
        nonlocal guard_calls
        guard_calls += 1
        if not live:
            raise pnc_completion_notice_relay.ExternalWriteFenceError(
                "external_write_fence_epoch_not_current"
            )
        return {
            "epoch_id": "epoch-gray-1",
            "state": "bounded_active",
            "ledger_id": 1,
            "chat_id": pnc_completion_notice_relay.G1Q3_RCA_CHAT_ID,
            "thread_id": "topic:om_root",
        }

    class FakeAdapter:
        async def send(self, _chat_id, _message, *, metadata=None):
            nonlocal live, provider_calls
            pnc_completion_notice_relay.revalidate_provider_write_claim(
                metadata["_pnc_rca_external_write_guard"],
                operation=metadata["_pnc_rca_external_write_operation"],
                chat_id=_chat_id,
                thread_id=metadata.get("thread_id", ""),
            )
            provider_calls += 1
            live = False
            return SimpleNamespace(
                success=False,
                error="tenant access_token expired",
                message_id=None,
            )

    fake_adapter = FakeAdapter()

    def ensure_adapter(*, rebuild=False):
        rebuilds.append(rebuild)
        return fake_adapter

    monkeypatch.setattr(sender, "_ensure_adapter", ensure_adapter)
    monkeypatch.setattr(
        pnc_completion_notice_relay,
        "revalidate_provider_write_claim",
        provider_revalidate,
    )
    provider_claim = pnc_completion_notice_relay.build_write_fence_provider_claim(
        {"state": "issued"}
    )

    result = json.loads(
        sender.send(
            {
                "target": (
                    "feishu:"
                    f"{pnc_completion_notice_relay.G1Q3_RCA_CHAT_ID}:om_root"
                ),
                "message": "must stop before retry",
                    "_pnc_rca_external_write_guard": provider_claim,
            }
        )
    )

    assert guard_calls == 2
    assert provider_calls == 1
    assert True in rebuilds
    assert "external_write_fence_epoch_not_current" in result["error"]


def test_hot_sender_rejects_forged_callable_claim_before_adapter(monkeypatch):
    sender = object.__new__(pnc_completion_notice_relay.FeishuHotSender)
    sender._record_sender = None
    monkeypatch.setattr(
        sender,
        "_ensure_adapter",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("forged claim reached Feishu adapter")
        ),
    )

    with pytest.raises(
        pnc_completion_notice_relay.ExternalWriteFenceError,
        match="external_write_provider_claim_invalid",
    ):
        sender.send(
            {
                "target": (
                    "feishu:"
                    f"{pnc_completion_notice_relay.G1Q3_RCA_CHAT_ID}:om_root"
                ),
                "message": "must stay blocked",
                "_pnc_rca_external_write_guard": lambda: {"state": "forged"},
            }
        )


def test_card_patch_readback_rejects_wrong_message_binding():
    get_calls = []
    item = SimpleNamespace(
        message_id="om_wrong",
        chat_id=pnc_completion_notice_relay.G1Q3_RCA_CHAT_ID,
        thread_id="om_root",
        root_id="om_root",
        parent_id="",
        body=SimpleNamespace(content='{"task_id":"task-rca-1"}'),
    )
    response = SimpleNamespace(data=SimpleNamespace(items=[item]))
    adapter = SimpleNamespace(
        _build_get_message_request=lambda message_id: message_id,
        _response_succeeded=lambda _response: True,
        _client=SimpleNamespace(
            im=SimpleNamespace(
                v1=SimpleNamespace(
                    message=SimpleNamespace(
                        get=lambda request: get_calls.append(request) or response
                    )
                )
            )
        ),
    )

    with pytest.raises(
        pnc_completion_notice_relay.ExternalWriteFenceError,
        match="external_write_fence_target_mismatch",
    ):
        asyncio.run(
            pnc_completion_notice_relay.FeishuHotSender._verify_card_patch_target(
                adapter,
                message_id="om_expected",
                chat_id=pnc_completion_notice_relay.G1Q3_RCA_CHAT_ID,
                thread_id="topic:om_root",
                submission_key="task-rca-1",
            )
        )

    assert get_calls == ["om_expected"]


def test_fenced_one_shot_relay_never_uses_send_message_tool(monkeypatch):
    task_id = "task-rca-1"
    chat_id = pnc_completion_notice_relay.G1Q3_RCA_CHAT_ID
    thread_id = "topic:om_root"
    live = {
        "epoch_id": "epoch-gray-1",
        "state": "bounded_active",
        "ledger_id": 1,
        "business_key": "business-1",
        "submission_key": task_id,
        "generation": 1,
        "chat_id": chat_id,
        "thread_target": thread_id,
        "issue_target": "https://project.feishu.cn/example/issue/detail/7001",
        "target_set_sha256": "1" * 64,
    }
    fence = {"state": "issued"}
    monkeypatch.setattr(
        pnc_completion_notice_relay,
        "_load_task_write_fence",
        lambda _task_id: {
            "snapshot": {
                "resolved_admission": {
                    "business_key": "business-1",
                    "submission_key": task_id,
                    "generation": 1,
                }
            },
            "snapshot_core_sha256": "2" * 64,
            "write_fence": fence,
        },
    )
    monkeypatch.setattr(
        pnc_completion_notice_relay,
        "_relay_live_fence_binding",
        lambda _fence: dict(live),
    )
    monkeypatch.setattr(
        pnc_completion_notice_relay,
        "validate_write_fence",
        lambda *_args, **_kwargs: fence,
    )
    monkeypatch.setattr(
        pnc_completion_notice_relay,
        "send_message_tool",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("RCA one-shot relay reached send_message_tool")
        ),
    )
    calls = []

    class FakeHotSender:
        def send(self, args):
            calls.append(dict(args))
            return json.dumps({"success": True, "message_id": "om_sent"})

        def send_task_card(self, *_args, **_kwargs):
            raise AssertionError("unexpected card send")

    monkeypatch.setattr(
        pnc_completion_notice_relay, "FeishuHotSender", FakeHotSender
    )

    send_text, _send_card = pnc_completion_notice_relay._fenced_task_senders(
        task_id,
        None,
        None,
    )
    result = json.loads(
        send_text(
            {
                "action": "send",
                "target": f"feishu:{chat_id}:om_root",
                "message": "bounded notice",
            }
        )
    )

    assert result["success"] is True
    assert len(calls) == 1
    assert type(calls[0]["_pnc_rca_external_write_guard"]) is (
        pnc_completion_notice_relay.RcaProviderWriteClaim
    )


def test_l4_sealed_clock_and_business_timestamp_format_are_idempotent(monkeypatch):
    monkeypatch.setenv("HERMES_OUTBOUND_MODE", "record-only")
    monkeypatch.setenv("HERMES_L4_SANDBOX_ACTIVE", "1")
    monkeypatch.setenv("HERMES_L4_EVENT_EPOCH", "1783850400")

    assert pnc_completion_notice_relay._now_epoch() == 1783850400.0
    assert pnc_completion_notice_relay._now_iso() == "2026-07-12T18:00:00+08:00"
    assert pnc_completion_notice_relay._format_business_ts("2026-07-12 19:57:39") == "2026-07-12 19:57:39"
    assert pnc_completion_notice_relay._format_business_ts(
        pnc_completion_notice_relay._format_business_ts("2026-07-12 19:57:39")
    ) == "2026-07-12 19:57:39"


def test_l4_sealed_clock_fails_closed_when_epoch_is_missing(monkeypatch):
    monkeypatch.setenv("HERMES_OUTBOUND_MODE", "record-only")
    monkeypatch.setenv("HERMES_L4_SANDBOX_ACTIVE", "1")
    monkeypatch.delenv("HERMES_L4_EVENT_EPOCH", raising=False)

    with pytest.raises(RuntimeError, match="requires HERMES_L4_EVENT_EPOCH"):
        pnc_completion_notice_relay._now_epoch()


def test_relay_send_marks_notice_sent(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    try:
        sidecar = _write_sidecar(tmp_path)
        calls = []

        def fake_send(args):
            calls.append(args)
            return json.dumps({"success": True, "message_id": "om_sent"})

        monkeypatch.setattr(pnc_completion_notice_relay, "send_message_tool", fake_send)
        result = pnc_completion_notice_relay.relay_pending_notices(task_ids=["task-1"], send=True)
        body = json.loads(sidecar.read_text(encoding="utf-8"))
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True
    assert result["sent_count"] == 1
    assert body["completion_notice"]["sent_at"].endswith("+08:00")
    assert body["completion_notice"]["send_status"] == "sent"
    assert body["completion_notice"]["send_result"]["message_id"] == "om_sent"
    assert calls[0]["target"].endswith(":om_1")




def test_relay_replay_task_id_is_suppressed_by_real_iter(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        _write_sidecar(tmp_path, task_id="replay-g1q3-rca-case-20260618")
        pending = pnc_completion_notice_relay.iter_pending_notices(task_ids=["replay-g1q3-rca-case-20260618"])
        relayable = pnc_completion_notice_relay._notice_is_relayable(
            {"send_status": "pending"},
            task_id="replay-g1q3-rca-case-20260618",
        )
    finally:
        reset_hermes_home_override(token)

    assert pending == []
    assert relayable == (False, "replay_suppressed")


def test_relay_replay_sidecar_flag_is_suppressed_without_mocking_scan(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        sidecar = _write_sidecar(tmp_path, task_id="g1q3-rca-replay-sidecar")
        body = json.loads(sidecar.read_text(encoding="utf-8"))
        body["replay"] = True
        body["completion_notice"]["replay"] = True
        sidecar.write_text(json.dumps(body), encoding="utf-8")
        pending = pnc_completion_notice_relay.iter_pending_notices(task_ids=["g1q3-rca-replay-sidecar"])
        result = pnc_completion_notice_relay.relay_pending_notices(task_ids=["g1q3-rca-replay-sidecar"], send=False)
    finally:
        reset_hermes_home_override(token)

    assert pending == []
    assert result["candidate_count"] == 0
    assert result["sent_count"] == 0


def test_relay_replay_guard_does_not_block_normal_pending_notice(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        _write_sidecar(tmp_path, task_id="g1q3-rca-normal-production")
        pending = pnc_completion_notice_relay.iter_pending_notices(task_ids=["g1q3-rca-normal-production"])
        result = pnc_completion_notice_relay.relay_pending_notices(task_ids=["g1q3-rca-normal-production"], send=False)
    finally:
        reset_hermes_home_override(token)

    assert len(pending) == 1
    assert pending[0][0] == "g1q3-rca-normal-production"
    assert result["candidate_count"] == 1
    assert result["rows"][0]["preview"] == "完成通知"

def test_relay_ignores_already_sent_notice(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        _write_sidecar(tmp_path, send_status="sent")
        result = pnc_completion_notice_relay.relay_pending_notices(task_ids=["task-1"], send=True)
    finally:
        reset_hermes_home_override(token)

    assert result["candidate_count"] == 0
    assert result["sent_count"] == 0


def test_relay_rechecks_sidecar_before_send(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    try:
        sidecar = _write_sidecar(tmp_path)

        def fake_iter(**kwargs):
            body = json.loads(sidecar.read_text(encoding="utf-8"))
            # Simulate another worker winning the race after discovery but before send.
            updated = json.loads(sidecar.read_text(encoding="utf-8"))
            updated["completion_notice"]["send_status"] = "sent"
            sidecar.write_text(json.dumps(updated), encoding="utf-8")
            return [("task-1", sidecar, body, body["completion_notice"])]

        calls = []
        monkeypatch.setattr(pnc_completion_notice_relay, "iter_pending_notices", fake_iter)
        monkeypatch.setattr(pnc_completion_notice_relay, "send_message_tool", lambda args: calls.append(args) or json.dumps({"success": True}))
        result = pnc_completion_notice_relay.relay_pending_notices(task_ids=["task-1"], send=True)
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True
    assert result["sent_count"] == 0
    assert result["rows"][0]["skipped"] is True
    assert calls == []


def test_main_skips_when_lock_is_held(tmp_path, capsys):
    token = set_hermes_home_override(tmp_path)
    try:
        lock_path = tmp_path / "locks" / "pnc-completion-notice-relay.lock"
        with pnc_completion_notice_relay.SingleRunLock(lock_path) as lock:
            assert lock.acquired is True
            rc = pnc_completion_notice_relay.main(["--send", "--json"])
        out = json.loads(capsys.readouterr().out)
    finally:
        reset_hermes_home_override(token)

    assert rc == 0
    assert out["skipped"] is True
    assert out["reason"] == "another pnc_completion_notice_relay run is active"


def test_failed_notice_retry_policy_respects_cooldown_and_attempts(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        sidecar = _write_sidecar(tmp_path, send_status="failed")
        body = json.loads(sidecar.read_text(encoding="utf-8"))
        body["completion_notice"]["attempt_count"] = 1
        body["completion_notice"]["last_attempt_at"] = datetime.now(timezone.utc).isoformat()
        sidecar.write_text(json.dumps(body), encoding="utf-8")

        not_due = pnc_completion_notice_relay.iter_pending_notices(
            task_ids=["task-1"],
            retry_failed_after_seconds=600,
            max_attempts=3,
        )
        retry_notice = dict(body["completion_notice"])
        retry_notice["last_attempt_at"] = "2026-06-10T00:00:00+00:00"
        due = pnc_completion_notice_relay._notice_is_relayable(
            retry_notice,
            retry_failed_after_seconds=600,
            max_attempts=3,
            now_ts=pnc_completion_notice_relay._parse_iso_ts("2026-06-10T00:20:00+00:00"),
        )
        retry_notice["attempt_count"] = 3
        maxed = pnc_completion_notice_relay._notice_is_relayable(
            retry_notice,
            retry_failed_after_seconds=600,
            max_attempts=3,
            now_ts=pnc_completion_notice_relay._parse_iso_ts("2026-06-10T00:20:00+00:00"),
        )
    finally:
        reset_hermes_home_override(token)

    assert not_due == []
    assert due[0] is True
    assert due[1].startswith("failed_retry_due")
    assert maxed[0] is True
    retry_notice["alert_sent_at"] = "2026-06-10T00:30:00+00:00"
    alerted = pnc_completion_notice_relay._notice_is_relayable(
        retry_notice,
        retry_failed_after_seconds=600,
        max_attempts=3,
        now_ts=pnc_completion_notice_relay._parse_iso_ts("2026-06-10T00:40:00+00:00"),
    )
    assert alerted == (False, "max_attempts_reached:3")


def test_retry_failed_notice_can_succeed_and_clears_error(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    try:
        sidecar = _write_sidecar(tmp_path, send_status="failed")
        body = json.loads(sidecar.read_text(encoding="utf-8"))
        body["completion_notice"]["attempt_count"] = 1
        body["completion_notice"]["last_attempt_at"] = "2026-06-10T00:00:00+00:00"
        body["completion_notice"]["send_error"] = "temporary"
        sidecar.write_text(json.dumps(body), encoding="utf-8")
        monkeypatch.setattr(pnc_completion_notice_relay, "send_message_tool", lambda args: json.dumps({"success": True, "message_id": "om_retry"}))

        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=["task-1"],
            send=True,
            retry_failed_after_seconds=1,
            max_attempts=3,
        )
        updated = json.loads(sidecar.read_text(encoding="utf-8"))["completion_notice"]
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True
    assert result["sent_count"] == 1
    assert updated["send_status"] == "sent"
    assert updated["attempt_count"] == 2
    assert "send_error" not in updated


def test_completion_notice_relay_launchd_guard_requires_keepalive_watch(
    tmp_path, monkeypatch
):
    from scripts import hermes_live_drift_guard

    plist = tmp_path / "Library" / "LaunchAgents" / "local.pnc.completion-notice-relay.plist"
    plist.parent.mkdir(parents=True)
    plist.write_bytes(
        plistlib.dumps(
            {
                "Label": "local.pnc.completion-notice-relay",
                "ProgramArguments": [
                    "/usr/bin/python3",
                    str(tmp_path / ".hermes/runtime/governance-tools/pnc_live_exec.py"),
                    "local.pnc.completion-notice-relay",
                    "--send",
                    "--retry-failed-after",
                    "600",
                    "--max-attempts",
                    "3",
                ],
                "WorkingDirectory": str(tmp_path / ".hermes/runtime"),
                "EnvironmentVariables": {"HERMES_HOME": str(tmp_path / ".hermes")},
            }
        )
    )
    monkeypatch.setattr(hermes_live_drift_guard.Path, "home", staticmethod(lambda: tmp_path))
    raw_without_watch = """
    program = /usr/bin/python3
    /usr/bin/python3 PLACEHOLDER/.hermes/runtime/governance-tools/pnc_live_exec.py local.pnc.completion-notice-relay
    --send --retry-failed-after 600 --max-attempts 3
    """.replace("PLACEHOLDER", str(tmp_path))
    monkeypatch.setattr(
        hermes_live_drift_guard,
        "read_launchd_runtime",
        lambda label: {"found": "true", "raw": raw_without_watch},
    )

    result = hermes_live_drift_guard.validate_pnc_completion_notice_relay_launchd()

    assert result["ok"] is False
    assert any("must include --watch" in error for error in result["errors"])
    assert any("KeepAlive=true" in error for error in result["errors"])


def test_completion_notice_relay_launchd_guard_accepts_keepalive_watch(tmp_path, monkeypatch):
    from scripts import hermes_live_drift_guard

    plist = tmp_path / "Library" / "LaunchAgents" / "local.pnc.completion-notice-relay.plist"
    plist.parent.mkdir(parents=True)
    plist.write_bytes(
        plistlib.dumps(
            {
                "Label": "local.pnc.completion-notice-relay",
                "ProgramArguments": [
                    "/usr/bin/python3",
                    str(tmp_path / ".hermes/runtime/governance-tools/pnc_live_exec.py"),
                    "local.pnc.completion-notice-relay",
                    "--send",
                    "--watch",
                    "--retry-failed-after",
                    "600",
                    "--max-attempts",
                    "3",
                ],
                "WorkingDirectory": str(tmp_path / ".hermes/runtime"),
                "EnvironmentVariables": {"HERMES_HOME": str(tmp_path / ".hermes")},
                "KeepAlive": True,
            }
        )
    )
    monkeypatch.setattr(hermes_live_drift_guard.Path, "home", staticmethod(lambda: tmp_path))
    raw_with_watch = f"""
    program = /usr/bin/python3
    /usr/bin/python3 {tmp_path}/.hermes/runtime/governance-tools/pnc_live_exec.py local.pnc.completion-notice-relay
    --send --watch --retry-failed-after 600 --max-attempts 3
    KeepAlive => true
    """
    monkeypatch.setattr(
        hermes_live_drift_guard,
        "read_launchd_runtime",
        lambda label: {"found": "true", "raw": raw_with_watch},
    )

    result = hermes_live_drift_guard.validate_pnc_completion_notice_relay_launchd()

    assert result["ok"] is True
    assert result["errors"] == []


def test_completion_notice_relay_launchd_guard_accepts_release_safe_off_watch(
    tmp_path, monkeypatch
):
    from scripts import hermes_live_drift_guard

    plist = tmp_path / "Library" / "LaunchAgents" / "local.pnc.completion-notice-relay.plist"
    plist.parent.mkdir(parents=True)
    plist.write_bytes(
        plistlib.dumps(
            {
                "Label": "local.pnc.completion-notice-relay",
                "ProgramArguments": [
                    "/usr/bin/python3",
                    str(tmp_path / ".hermes/runtime/governance-tools/pnc_live_exec.py"),
                    "local.pnc.completion-notice-relay",
                    "--task-id",
                    "rca-r11-safe-off-no-task",
                    "--watch",
                    "--retry-failed-after",
                    "600",
                    "--max-attempts",
                    "3",
                ],
                "WorkingDirectory": str(tmp_path / ".hermes/runtime"),
                "EnvironmentVariables": {"HERMES_HOME": str(tmp_path / ".hermes")},
                "KeepAlive": {"SuccessfulExit": False},
            }
        )
    )
    monkeypatch.setattr(hermes_live_drift_guard.Path, "home", staticmethod(lambda: tmp_path))
    raw_with_safe_off = f"""
    program = /usr/bin/python3
    /usr/bin/python3 {tmp_path}/.hermes/runtime/governance-tools/pnc_live_exec.py local.pnc.completion-notice-relay
    --task-id rca-r11-safe-off-no-task --watch --retry-failed-after 600 --max-attempts 3
    KeepAlive => true
    """
    monkeypatch.setattr(
        hermes_live_drift_guard,
        "read_launchd_runtime",
        lambda label: {"found": "true", "raw": raw_with_safe_off},
    )

    result = hermes_live_drift_guard.validate_pnc_completion_notice_relay_launchd()

    assert result["ok"] is True
    assert result["errors"] == []


def test_repository_completion_notice_relay_defaults_to_release_safe_off():
    repository_root = Path(__file__).resolve().parents[2]
    body = plistlib.loads(
        (repository_root / "local.pnc.completion-notice-relay.plist").read_bytes()
    )
    arguments = body["ProgramArguments"]

    assert "--send" not in arguments
    task_id_index = arguments.index("--task-id")
    assert arguments[task_id_index + 1] == "rca-r11-safe-off-no-task"


def test_failed_notice_alerts_home_channel_once_at_max_attempts(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    try:
        sidecar = _write_sidecar(tmp_path, send_status="failed")
        body = json.loads(sidecar.read_text(encoding="utf-8"))
        body["completion_notice"]["attempt_count"] = 2
        body["completion_notice"]["last_attempt_at"] = "2026-06-10T00:00:00+00:00"
        body["completion_notice"]["send_error"] = "temporary"
        sidecar.write_text(json.dumps(body), encoding="utf-8")
        calls = []

        def fake_send(args):
            calls.append(args)
            if len(calls) == 1:
                return json.dumps({"success": False, "error": "bad chat"})
            return json.dumps({"success": True, "message_id": "om_alert"})

        monkeypatch.setenv("FEISHU_HOME_CHANNEL", "oc_home")
        monkeypatch.setattr(pnc_completion_notice_relay, "send_message_tool", fake_send)

        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=["task-1"],
            send=True,
            retry_failed_after_seconds=1,
            max_attempts=3,
        )
        updated = json.loads(sidecar.read_text(encoding="utf-8"))["completion_notice"]

        repeat = pnc_completion_notice_relay.maybe_alert_failed_notice(
            sidecar,
            json.loads(sidecar.read_text(encoding="utf-8")),
            task_id="task-1",
            notice=updated,
            error="bad chat",
            max_attempts=3,
        )
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is False
    assert updated["send_status"] == "failed"
    assert updated["attempt_count"] == 3
    assert updated["alert_sent_at"]
    assert updated["alert_result"]["message_id"] == "om_alert"
    assert calls[0]["target"].startswith(f"feishu:{pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1]}")
    assert calls[1]["target"] == "feishu:oc_home"
    assert "[PNC completion notice relay 告警]" in calls[1]["message"]
    assert repeat["skipped"] is True
    assert repeat["reason"] == "alert_already_sent"
    assert len(calls) == 2



def test_watch_mode_uses_hot_sender_and_relay_loop_once(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    try:
        _write_sidecar(tmp_path)
        calls = []

        class FakeHotSender:
            def __init__(self):
                calls.append({"init": True})

            def send(self, args):
                calls.append(args)
                return json.dumps({"success": True, "message_id": "om_hot"})

        sleeps = []

        def fake_sleep(seconds):
            sleeps.append(seconds)
            raise KeyboardInterrupt

        monkeypatch.setattr(pnc_completion_notice_relay, "FeishuHotSender", FakeHotSender)
        monkeypatch.setattr(pnc_completion_notice_relay.time, "sleep", fake_sleep)

        try:
            pnc_completion_notice_relay.watch_pending_notices(send=True, poll_seconds=0.1, full_scan_seconds=120)
        except KeyboardInterrupt:
            pass
    finally:
        reset_hermes_home_override(token)

    assert calls[0] == {"init": True}
    assert calls[1]["target"].startswith("feishu:")
    assert calls[1]["message"] == "完成通知"
    assert sleeps == [0.1]


def test_hot_sender_constructor_is_record_only_complete_and_never_builds_adapter(tmp_path, monkeypatch):
    _set_record_only_env(tmp_path, monkeypatch)

    def bomb_adapter(*_args, **_kwargs):
        raise AssertionError("live Feishu adapter was built")

    monkeypatch.setattr(pnc_completion_notice_relay.FeishuHotSender, "_ensure_adapter", bomb_adapter)
    try:
        sender = pnc_completion_notice_relay.FeishuHotSender()
        text = json.loads(sender.send({
            "action": "send",
            "target": "feishu:oc_fixture:om_topic",
            "message": "中文完成",
            "task_id": "task-hot-record-only",
        }))
        card = sender.send_task_card(
            "feishu:oc_fixture:om_topic",
            {"elements": [{"tag": "markdown", "content": "终态卡片"}]},
            message_id="om_card",
        )
        transport = runtime.get_record_only_transport("scripts.pnc_completion_notice_relay")
        assert transport is not None
        rows = transport.read_all()
    finally:
        runtime._reset_for_tests()

    assert text["success"] is True
    assert text["external_delivery_attempted"] is False
    assert card["success"] is True
    assert card["simulated_update_recorded"] is True
    assert [row["operation"] for row in rows] == ["text_reply", "card_update"]


def test_hot_sender_live_mode_still_builds_the_real_adapter_path(monkeypatch):
    monkeypatch.setenv("HERMES_OUTBOUND_MODE", "live")
    calls = []

    def fake_ensure(self, *, rebuild=False):
        calls.append(rebuild)
        self._adapter = object()
        return self._adapter

    monkeypatch.setattr(pnc_completion_notice_relay.FeishuHotSender, "_ensure_adapter", fake_ensure)
    sender = pnc_completion_notice_relay.FeishuHotSender()

    assert sender._record_sender is None
    assert calls == [False]


def test_record_only_task_senders_bind_context_dedupe_and_redact_tracking_id(tmp_path, monkeypatch):
    _set_record_only_env(tmp_path, monkeypatch)
    raw_task_id = "g1q3-rca-issue-intake-7017699515"
    try:
        from gateway.record_only.transport import RecordOnlyRelaySender

        transport = runtime.get_record_only_transport("scripts.pnc_completion_notice_relay")
        assert transport is not None
        sender = RecordOnlyRelaySender(transport)
        send_text, send_card = pnc_completion_notice_relay._record_only_task_senders(
            sender,
            task_id=raw_task_id,
            body={"task_card": {"user_state": "done"}},
            notice={"state": "completed"},
        )
        text_args = {
            "action": "send",
            "target": "feishu:oc_fixture:om_topic",
            "message": f"中文完成，追踪号 {raw_task_id}",
        }
        card_payload = {
            "elements": [
                {"tag": "markdown", "content": f"终态卡片，追踪号 {raw_task_id}"}
            ]
        }
        assert json.loads(send_text(text_args))["success"] is True
        assert json.loads(send_text(text_args))["duplicate"] is True
        assert send_card("feishu:oc_fixture:om_topic", card_payload, message_id="om_card")["success"] is True
        assert send_card("feishu:oc_fixture:om_topic", card_payload, message_id="om_card")["duplicate"] is True
        rows = transport.read_all()
    finally:
        runtime._reset_for_tests()

    assert [row["operation"] for row in rows] == ["text_reply", "card_update"]
    assert [row["attempt_count"] for row in rows] == [2, 2]
    assert all(row["task_id_hash"] for row in rows)
    assert all(row["caller_dedupe_key_hash"] for row in rows)
    assert all(row["terminal_state"] == "completed" for row in rows)
    assert all(row["external_delivery_attempted"] is False for row in rows)
    assert raw_task_id not in json.dumps(rows, ensure_ascii=False)


def test_watch_health_proves_three_fused_startup_loops(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    health_path = tmp_path / "health" / "relay.json"
    (tmp_path / "health").mkdir(mode=0o700)
    evidence = {
        "pid": 123,
        "process_create_time": 1783890000.0,
        "started_at": "2026-07-13T00:00:00+00:00",
        "runtime_identity": {
            "executable": "/runtime/.venv/bin/python",
            "script": "/runtime/scripts/pnc_completion_notice_relay.py",
            "cwd": "/runtime",
            "script_sha256": "1" * 64,
            "interpreter_sha256": "2" * 64,
            "plist_path": "/Users/test/Library/LaunchAgents/relay.plist",
            "plist_sha256": "3" * 64,
            "program_arguments_sha256": "4" * 64,
            "environment_sha256": "5" * 64,
        },
    }
    loops = []

    def stop_after_three(seconds):
        loops.append(seconds)
        if len(loops) == 3:
            raise KeyboardInterrupt

    monkeypatch.setattr(
        pnc_completion_notice_relay,
        "relay_pending_notices",
        lambda **_kwargs: {
            "ok": True,
            "candidate_count": 0,
            "sent_count": 0,
            "card_fallback_attempted_count": 0,
            "card_fallback_sent_count": 0,
            "errors": [],
        },
    )
    monkeypatch.setattr(pnc_completion_notice_relay.time, "sleep", stop_after_three)
    try:
        with pytest.raises(KeyboardInterrupt):
            pnc_completion_notice_relay.watch_pending_notices(
                send=False,
                poll_seconds=0.1,
                full_scan_seconds=1,
                canary_loops=3,
                max_card_fallbacks_per_loop=0,
                health_path=health_path,
                runtime_evidence_builder=lambda **_kwargs: evidence,
            )
    finally:
        reset_hermes_home_override(token)

    body = json.loads(health_path.read_text(encoding="utf-8"))
    assert health_path.stat().st_mode & 0o777 == 0o600
    assert body["schema_version"] == "pnc_completion_notice_relay_health_v1"
    assert body["loop_count"] == 3
    assert body["startup_canary_loops_required"] == 3
    assert body["startup_canary_loops_completed"] == 3
    assert body["startup_canary_completed_at"]
    assert body["configured_max_card_fallbacks_per_loop"] == 0
    assert body["effective_max_card_fallbacks_per_loop"] == 0
    assert body["card_fallback_attempted_count"] == 0
    assert body["card_fallback_sent_count"] == 0
    assert body["healthy"] is True
    assert body["errors"] == []


def test_relay_sends_task_card_once_and_marks_sent_hash(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    try:
        sidecar = _write_sidecar(tmp_path)
        body = json.loads(sidecar.read_text(encoding="utf-8"))
        body["task_card"] = {
            "schema_version": 1,
            "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
            "thread_id": "topic:om_1",
            "user_state": "running",
            "milestones": [],
            "pending_confirms": [],
            "delivery": {},
        }
        sidecar.write_text(json.dumps(body), encoding="utf-8")
        card_calls = []

        def fake_card(target, rendered, message_id):
            card_calls.append((target, rendered, message_id))
            return {"success": True, "message_id": "om_card"}

        monkeypatch.setattr(pnc_completion_notice_relay, "send_message_tool", lambda args: json.dumps({"success": True, "message_id": "om_text"}))
        result = pnc_completion_notice_relay.relay_pending_notices(task_ids=["task-1"], send=True, send_card_func=fake_card)
        updated = json.loads(sidecar.read_text(encoding="utf-8"))

        repeat = pnc_completion_notice_relay.relay_pending_notices(task_ids=["task-1"], send=True, send_card_func=fake_card)
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True
    assert len(card_calls) == 1
    assert card_calls[0][0].endswith(":om_1")
    assert card_calls[0][2] is None
    assert updated["task_card"]["card_message_id"] == "om_card"
    assert updated["task_card"]["last_sent_hash"]
    assert repeat["candidate_count"] == 0


def test_relay_updates_existing_task_card_when_hash_changes(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        sidecar = _write_sidecar(tmp_path, send_status="sent")
        body = json.loads(sidecar.read_text(encoding="utf-8"))
        body["task_card"] = {
            "schema_version": 1,
            "card_message_id": "om_card",
            "last_sent_hash": "old",
            "last_update_ts": "2026-06-10T00:00:00+00:00",
            "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
            "thread_id": "topic:om_1",
            "user_state": "done",
            "milestones": [],
            "pending_confirms": [],
            "delivery": {"conclusion": "done"},
        }
        sidecar.write_text(json.dumps(body), encoding="utf-8")
        calls = []
        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=["task-1"],
            send=True,
            send_card_func=lambda target, rendered, message_id: calls.append((target, message_id)) or {"success": True, "message_id": message_id, "updated": True},
        )
        updated = json.loads(sidecar.read_text(encoding="utf-8"))["task_card"]
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True
    assert result["candidate_count"] == 1
    assert calls == [(f"feishu:{pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1]}:om_1", "om_card")]
    assert updated["last_sent_hash"] != "old"
    assert updated["card_message_id"] == "om_card"


def test_task_card_sidecar_cas_suppresses_stale_repeat_disposition(tmp_path):
    sidecar = _write_sidecar(tmp_path, task_id="card-cas", send_status="sent")
    base = json.loads(sidecar.read_text(encoding="utf-8"))
    base["task_card"] = {
        "schema_version": 1,
        "task_id": "card-cas",
        "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
        "thread_id": "topic:om_card_cas",
        "user_state": "done",
        "milestones": [],
        "pending_confirms": [],
        "delivery": {"conclusion": "done"},
    }
    sidecar.write_text(json.dumps(base), encoding="utf-8")
    first_body = json.loads(sidecar.read_text(encoding="utf-8"))
    stale_body = json.loads(sidecar.read_text(encoding="utf-8"))
    calls = []

    def sender(target, rendered, message_id=None):
        calls.append((target, message_id))
        return {"success": True, "message_id": "om_card_cas"}

    first = pnc_completion_notice_relay.sync_task_card(
        task_id="card-cas",
        path=sidecar,
        body=first_body,
        send=True,
        send_card_func=sender,
        throttle_seconds=0,
    )
    repeat = pnc_completion_notice_relay.sync_task_card(
        task_id="card-cas",
        path=sidecar,
        body=stale_body,
        send=True,
        send_card_func=sender,
        throttle_seconds=0,
    )
    durable = json.loads(sidecar.read_text(encoding="utf-8"))["task_card"]

    assert first["disposition"] == "recorded"
    assert repeat["disposition"] == "duplicate_noop"
    assert repeat["reason"] == "hash_unchanged"
    assert len(calls) == 1
    assert durable["last_card_semantic_key"] == first["semantic_key"] == repeat["semantic_key"]


def test_task_card_sidecar_lock_rejects_hardlink(tmp_path):
    sidecar = _write_sidecar(tmp_path, task_id="card-hardlink", send_status="sent")
    body = json.loads(sidecar.read_text(encoding="utf-8"))
    body["task_card"] = {
        "task_id": "card-hardlink",
        "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
        "user_state": "running",
        "milestones": [],
        "delivery": {},
    }
    sidecar.write_text(json.dumps(body), encoding="utf-8")
    source = tmp_path / "lock-source"
    source.write_text("", encoding="utf-8")
    source.chmod(0o600)
    os.link(source, sidecar.parent / f".{sidecar.name}.card.lock")
    calls = []

    with pytest.raises(RuntimeError, match="link count mismatch"):
        pnc_completion_notice_relay.sync_task_card(
            task_id="card-hardlink",
            path=sidecar,
            body=body,
            send=True,
            send_card_func=lambda *args, **kwargs: calls.append(True) or {"success": True},
            throttle_seconds=0,
        )

    assert calls == []


def test_relay_calls_hot_sender_with_message_id_keyword(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        sidecar = _write_sidecar(tmp_path, send_status="sent")
        body = json.loads(sidecar.read_text(encoding="utf-8"))
        body["task_card"] = {
            "schema_version": 1,
            "card_message_id": "om_card",
            "last_sent_hash": "old",
            "last_update_ts": "2026-06-10T00:00:00+00:00",
            "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
            "thread_id": "topic:om_1",
            "user_state": "done",
            "milestones": [],
            "pending_confirms": [],
            "delivery": {"conclusion": "done"},
        }
        sidecar.write_text(json.dumps(body), encoding="utf-8")
        calls = []

        def fake_hot_sender_signature(target, rendered, message_id=None):
            calls.append((target, bool(rendered), message_id))
            return {"success": True, "message_id": message_id, "updated": True}

        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=["task-1"],
            send=True,
            send_card_func=fake_hot_sender_signature,
        )
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True
    assert calls == [(f"feishu:{pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1]}:om_1", True, "om_card")]


def test_relay_card_update_throttle_skips_recent_update(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        sidecar = _write_sidecar(tmp_path, send_status="sent")
        body = json.loads(sidecar.read_text(encoding="utf-8"))
        body["task_card"] = {
            "schema_version": 1,
            "card_message_id": "om_card",
            "last_sent_hash": "old",
            "last_update_ts": pnc_completion_notice_relay._now_iso(),
            "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
            "thread_id": "topic:om_1",
            "user_state": "running",
            "milestones": [],
            "pending_confirms": [],
            "delivery": {},
        }
        sidecar.write_text(json.dumps(body), encoding="utf-8")
        calls = []
        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=["task-1"],
            send=True,
            send_card_func=lambda target, rendered, message_id: calls.append(target) or {"success": True},
        )
    finally:
        reset_hermes_home_override(token)

    assert result["candidate_count"] == 1
    assert result["rows"][0]["task_card"]["reason"] == "throttled"
    assert calls == []


def test_relay_suppresses_process_text_but_still_updates_card(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        sidecar = _write_sidecar(tmp_path)
        body = json.loads(sidecar.read_text(encoding="utf-8"))
        body["completion_notice"]["state"] = "running"
        body["completion_notice"]["text"] = "过程消息不应追加"
        body["task_card"] = {
            "schema_version": 1,
            "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
            "thread_id": "topic:om_1",
            "user_state": "running",
            "milestones": [],
            "pending_confirms": [],
            "delivery": {},
        }
        sidecar.write_text(json.dumps(body), encoding="utf-8")
        text_calls = []
        card_calls = []

        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=["task-1"],
            send=True,
            send_func=lambda args: text_calls.append(args) or json.dumps({"success": True}),
            send_card_func=lambda target, rendered, message_id: card_calls.append(target) or {"success": True, "message_id": "om_card"},
        )
        updated = json.loads(sidecar.read_text(encoding="utf-8"))
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True
    assert result["rows"][0]["text_suppressed"] is True
    assert updated["completion_notice"]["send_status"] == "suppressed"
    assert updated["completion_notice"]["suppress_reason"] == "process_state=running"
    assert updated["task_card"]["card_message_id"] == "om_card"
    assert text_calls == []
    assert card_calls == [f"feishu:{pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1]}:om_1"]


def test_relay_isolates_poison_card_render_error_and_survives(tmp_path, monkeypatch):
    # A card whose render raises (e.g. sanitizer-bypassed stale positive milestone
    # tripping the fail-closed guard) must NOT crash the whole watch loop and
    # starve every other task of card sync.  The relay records the error and
    # keeps going.  Regression for the 2026-07-09 12k-crash relay crash-loop.
    token = set_hermes_home_override(tmp_path)
    try:
        sidecar = _write_sidecar(tmp_path)
        body = json.loads(sidecar.read_text(encoding="utf-8"))
        body["task_card"] = {
            "schema_version": 1,
            "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
            "thread_id": "topic:om_1",
            "user_state": "running",
            "milestones": [],
            "pending_confirms": [],
            "delivery": {},
        }
        sidecar.write_text(json.dumps(body), encoding="utf-8")

        def boom(*args, **kwargs):
            raise ValueError("task card contains forbidden fragment(s): RCA 报告已生成")

        monkeypatch.setattr(pnc_completion_notice_relay, "sync_task_card", boom)
        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=["task-1"],
            send=True,
            send_func=lambda args: json.dumps({"success": True}),
            send_card_func=lambda target, rendered, message_id: {"success": True, "message_id": "om_card"},
        )
    finally:
        reset_hermes_home_override(token)

    # Loop survived (did not raise); the failure is visible, not silent.
    assert result["ok"] is False
    assert any("card render/sync failed" in e for e in result["errors"])
    assert result["rows"][0]["task_card"]["reason"] == "render_error"


def test_m2_3_source_contract_is_declared():
    assert pnc_completion_notice_relay.M2_3_SOURCE_CONTRACT["card_failure_degrade"].startswith("card send/patch failure")
    assert pnc_completion_notice_relay.CARD_FALLBACK_PREFIX == "[PNC task card fallback]"



def test_card_failure_fallback_fuse_defaults_closed(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        sidecar = _write_sidecar(tmp_path)
        body = json.loads(sidecar.read_text(encoding="utf-8"))
        body["completion_notice"]["text"] = ""
        body["task_card"] = {
            "schema_version": 1,
            "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
            "thread_id": "topic:om_1",
            "user_state": "running",
            "milestones": [],
            "pending_confirms": [],
            "delivery": {},
        }
        sidecar.write_text(json.dumps(body), encoding="utf-8")
        text_calls = []

        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=["task-1"],
            send=True,
            send_func=lambda args: text_calls.append(args) or json.dumps({"success": True}),
            send_card_func=lambda target, rendered, message_id: {"success": False, "error": "patch failed"},
        )
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True
    assert result["card_fallback_budget"] == 0
    assert result["card_fallback_attempted_count"] == 0
    assert result["card_fallback_suppressed_count"] == 1
    assert result["rows"][0]["task_card_fallback"]["reason"] == "card_fallback_fuse_open"
    assert text_calls == []


def test_expired_card_patch_never_fallbacks_and_freezes_hash(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        sidecar = _write_sidecar(tmp_path)
        body = json.loads(sidecar.read_text(encoding="utf-8"))
        body["completion_notice"]["text"] = ""
        body["task_card"] = {
            "schema_version": 1,
            "card_message_id": "om_old_card",
            "last_sent_hash": "old_hash",
            "last_update_ts": "2026-06-10T00:00:00+00:00",
            "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
            "thread_id": "topic:om_1",
            "user_state": "running",
            "milestones": [],
            "pending_confirms": [],
            "delivery": {},
        }
        sidecar.write_text(json.dumps(body), encoding="utf-8")
        text_calls = []
        expired = "[230031] Message has expired when updating message, ext=Message can only be updated within fourteen days."

        first = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=["task-1"],
            send=True,
            send_func=lambda args: text_calls.append(args) or json.dumps({"success": True}),
            send_card_func=lambda target, rendered, message_id: {"success": False, "error": expired},
            max_card_fallbacks_per_loop=1,
        )
        after_first = json.loads(sidecar.read_text(encoding="utf-8"))["task_card"]
        second = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=["task-1"],
            send=True,
            send_func=lambda args: text_calls.append(args) or json.dumps({"success": True}),
            send_card_func=lambda target, rendered, message_id: {"success": False, "error": expired},
            max_card_fallbacks_per_loop=1,
        )
    finally:
        reset_hermes_home_override(token)

    assert first["rows"][0]["task_card"]["reason"] == "card_message_expired"
    assert "task_card_fallback" not in first["rows"][0]
    assert first["card_fallback_sent_count"] == 0
    assert text_calls == []
    assert after_first["last_sent_hash"] == after_first["last_render_hash"]
    assert after_first["card_message_expired_at"]
    assert second["candidate_count"] == 0 or second["rows"][0].get("task_card", {}).get("reason") == "hash_unchanged"


def test_m2_3_card_failure_without_completion_text_degrades_to_topic_text(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        sidecar = _write_sidecar(tmp_path)
        body = json.loads(sidecar.read_text(encoding="utf-8"))
        body["completion_notice"]["text"] = ""
        body["task_card"] = {
            "schema_version": 1,
            "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
            "thread_id": "topic:om_1",
            "user_state": "running",
            "status_line": "VM 已接手，正在跑",
            "milestones": [],
            "pending_confirms": [],
            "delivery": {},
        }
        sidecar.write_text(json.dumps(body), encoding="utf-8")
        text_calls = []

        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=["task-1"],
            send=True,
            send_func=lambda args: text_calls.append(args) or json.dumps({"success": True, "message_id": "om_fallback"}),
            send_card_func=lambda target, rendered, message_id: {"success": False, "error": "patch failed"},
            max_card_fallbacks_per_loop=1,
        )
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True
    row = result["rows"][0]
    assert row["task_card"]["success"] is False
    assert row["task_card_fallback"]["sent"] is True
    assert text_calls[0]["target"].endswith(":om_1")
    assert "[PNC task card fallback]" in text_calls[0]["message"]
    assert "task_id: task-1" in text_calls[0]["message"]
    assert "VM 已接手" in text_calls[0]["message"]


def test_rca_card_failure_fallback_never_exposes_internal_html(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        sidecar = _write_sidecar(tmp_path)
        body = json.loads(sidecar.read_text(encoding="utf-8"))
        body["completion_notice"]["text"] = ""
        body["task_card"] = {
            "schema_version": 1,
            "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
            "thread_id": "topic:om_1",
            "user_state": "completed",
            "status_line": "RCA 报告已生成",
            "milestones": [],
            "pending_confirms": [],
            "delivery": {
                "report_status": "report_ready",
                "conclusion": "自动分析已完成",
                "artifact_path": (
                    "http://192.168.26.174:18081/G1Q3_RCA/cases/demo/index.html"
                ),
            },
        }
        sidecar.write_text(json.dumps(body), encoding="utf-8")
        text_calls = []

        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=["task-1"],
            send=True,
            send_func=lambda args: text_calls.append(args)
            or json.dumps({"success": True, "message_id": "om_fallback"}),
            send_card_func=lambda target, rendered, message_id: {
                "success": False,
                "error": "patch failed",
            },
            max_card_fallbacks_per_loop=1,
        )
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True
    message = text_calls[0]["message"]
    assert "18081" not in message
    assert "index.html" not in message
    assert "内部 HTML 审计产物已隐藏" in message


def test_rca_completion_text_is_sanitized_after_card_failure(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        sidecar = _write_sidecar(tmp_path)
        body = json.loads(sidecar.read_text(encoding="utf-8"))
        body["completion_notice"]["text"] = (
            "artifact: http://192.168.26.174:18081/G1Q3_RCA/cases/demo/index.html\n"
            "结论：自动分析已完成"
        )
        body["task_card"] = {
            "schema_version": 1,
            "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
            "thread_id": "topic:om_1",
            "user_state": "completed",
            "milestones": [],
            "pending_confirms": [],
            "delivery": {
                "report_status": "report_ready",
                "rca_status": "report_ready",
            },
        }
        sidecar.write_text(json.dumps(body), encoding="utf-8")
        text_calls = []

        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=["task-1"],
            send=True,
            send_func=lambda args: text_calls.append(args)
            or json.dumps({"success": True, "message_id": "om_text"}),
            send_card_func=lambda target, rendered, message_id: {
                "success": False,
                "error": "patch failed",
            },
            max_card_fallbacks_per_loop=1,
        )
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True
    message = text_calls[0]["message"]
    assert "结论：自动分析已完成" in message
    assert "内部 HTML 审计产物已隐藏" in message
    assert "18081" not in message
    assert "index.html" not in message


@pytest.mark.parametrize(
    "internal_pointer",
    [
        "http://internal/G1Q3_RCA/demo/index%2Ehtml",
        "http://internal/G1Q3_RCA/demo/report.xhtml",
        "https://internal/report?file=index%252Ehtml",
        "<http://internal/G1Q3_RCA/demo/index.html>",
        "http://internal/G1Q3_RCA/demo/index.html]",
        "http://192.168.26.174:18081?case=demo",
        "http://192.168.26.174:18081#demo",
        "(http://192.168.26.174:18081)",
        "https://internal/report?file=index%2525252Ehtml",
        "https://internal/report?file=index&#46;html",
        "https://internal/report?file=index&amp;#46;html",
    ],
)
def test_rca_fallback_sanitizer_decodes_internal_html_references(
    internal_pointer,
):
    card = {"delivery": {"rca_status": "report_ready"}}

    rendered = pnc_completion_notice_relay._rca_public_text_without_internal_html(
        f"artifact: {internal_pointer}\n结论：保留",
        card,
    )

    assert internal_pointer not in rendered
    assert "内部 HTML 审计产物已隐藏" in rendered
    assert "结论：保留" in rendered


def test_rca_fallback_sanitizer_preserves_exact_validated_foxglove_with_dot_name():
    submission_key = "case.html"
    viz_mcap_vm = canonical_viz_mcap_path(submission_key)
    exact_foxglove = foxglove_url(viz_mcap_vm)
    card = {
        "delivery": {
            "rca_status": "report_ready",
            "foxglove_url": exact_foxglove,
            "viz_mcap_vm": viz_mcap_vm,
        }
    }

    rendered = pnc_completion_notice_relay._rca_public_text_without_internal_html(
        f"artifact: {exact_foxglove}",
        card,
    )

    assert rendered == f"artifact: {exact_foxglove}"


def test_non_rca_report_status_does_not_enable_rca_html_sanitizer():
    text = "public: https://docs.example/release/report.html\n完成"
    card = {"delivery": {"report_status": "published"}}

    assert (
        pnc_completion_notice_relay._rca_public_text_without_internal_html(
            text,
            card,
        )
        == text
    )


def test_m2_3_card_failure_fallback_send_error_is_visible(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        sidecar = _write_sidecar(tmp_path)
        body = json.loads(sidecar.read_text(encoding="utf-8"))
        body["completion_notice"]["text"] = ""
        body["task_card"] = {
            "schema_version": 1,
            "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
            "thread_id": "topic:om_1",
            "user_state": "running",
            "milestones": [],
            "pending_confirms": [],
            "delivery": {},
        }
        sidecar.write_text(json.dumps(body), encoding="utf-8")

        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=["task-1"],
            send=True,
            send_func=lambda args: json.dumps({"success": False, "error": "fallback bad"}),
            send_card_func=lambda target, rendered, message_id: {"success": False, "error": "patch failed"},
            max_card_fallbacks_per_loop=1,
        )
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is False
    assert result["errors"] == ["task-1: card_fallback: fallback bad"]
    assert result["rows"][0]["task_card_fallback"]["sent"] is False


def _write_integration_tools_shared_state(tmp_path, task_id="task-1", *, state="intake_checked", goal_text="我想用 logsim 回放一包 mcap，怎么安全发起？需要注意哪些路径和 flag？"):
    task_dir = tmp_path / "runtime" / "shared-state" / "tasks" / task_id
    task_dir.mkdir(parents=True)
    (task_dir / "meta.json").write_text(json.dumps({
        "task_id": task_id,
        "business_line": "integration_tools",
        "state": state,
        "updated_at": "2026-06-10T00:00:00+08:00",
    }), encoding="utf-8")
    (task_dir / "status.md").write_text(f"---\nstate: {state}\n---\nold\n", encoding="utf-8")
    (task_dir / "goal.md").write_text(goal_text, encoding="utf-8")
    return task_dir


def test_integration_tools_intake_checked_answer_only_auto_closes_and_updates_card(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    try:
        _write_integration_tools_shared_state(tmp_path)
        sidecar = tmp_path / "task-state" / "task-1.json"
        sidecar.parent.mkdir(parents=True)
        sidecar.write_text(json.dumps({
            "task_card": {
                "schema_version": 1,
                "task_id": "task-1",
                "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
                "thread_id": "topic:om_1",
                "card_message_id": "om_card",
                "last_sent_hash": "old",
                "last_update_ts": "2026-06-10T00:00:00+08:00",
                "user_state": "host-created",
                "status_line": "已接单。会先做完整性和风险检查。",
                "milestones": [{"ts": "2026-06-10T00:00:00+08:00", "label": "任务建好"}],
                "pending_confirms": [],
                "delivery": {"boundaries": []},
            }
        }), encoding="utf-8")
        updates = []
        monkeypatch.setattr(
            pnc_completion_notice_relay,
            "_update_shared_state_for_close_loop",
            lambda task_id, **kwargs: updates.append({"task_id": task_id, **kwargs}) or {"success": True},
        )
        card_calls = []

        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=["task-1"],
            send=True,
            send_card_func=lambda target, rendered, message_id: card_calls.append((target, rendered, message_id)) or {"success": True, "message_id": message_id, "updated": True},
        )
        updated = json.loads(sidecar.read_text(encoding="utf-8"))["task_card"]
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True
    assert result["candidate_count"] == 1
    assert result["rows"][0]["close_loop_guard"]["to_state"] == "closed"
    assert updates[0]["state"] == "closed"
    assert updated["user_state"] == "done"
    assert "已收口" in updated["status_line"]
    assert "已按工具知识页" in updated["delivery"]["conclusion"]
    assert any("答疑类误建任务已关闭" in item["label"] for item in updated["milestones"])
    assert card_calls and card_calls[0][2] == "om_card"


def test_integration_tools_intake_checked_foxglove_question_auto_closes_as_answer_only(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    try:
        _write_integration_tools_shared_state(
            tmp_path,
            goal_text="foxglove 打开后没有 planning topic，应该收集哪些信息？是不是可以直接跑 run_planning_visualization.sh 看看？",
        )
        sidecar = tmp_path / "task-state" / "task-1.json"
        sidecar.parent.mkdir(parents=True)
        sidecar.write_text(json.dumps({
            "task_card": {
                "schema_version": 1,
                "task_id": "task-1",
                "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
                "thread_id": "topic:om_1",
                "card_message_id": "om_card",
                "last_sent_hash": "old",
                "last_update_ts": "2026-06-10T00:00:00+08:00",
                "user_state": "host-created",
                "milestones": [],
                "pending_confirms": [],
                "delivery": {"boundaries": []},
            }
        }), encoding="utf-8")
        updates = []
        monkeypatch.setattr(
            pnc_completion_notice_relay,
            "_update_shared_state_for_close_loop",
            lambda task_id, **kwargs: updates.append({"task_id": task_id, **kwargs}) or {"success": True},
        )

        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=["task-1"],
            send=True,
            send_card_func=lambda target, rendered, message_id: {"success": True, "message_id": message_id, "updated": True},
        )
        updated = json.loads(sidecar.read_text(encoding="utf-8"))["task_card"]
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True
    assert result["rows"][0]["close_loop_guard"]["to_state"] == "closed"
    assert result["rows"][0]["close_loop_guard"]["answer_only"] is True
    assert updates[0]["state"] == "closed"
    assert updated["user_state"] == "done"


def test_integration_tools_intake_checked_non_answer_only_turns_need_input(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    try:
        _write_integration_tools_shared_state(tmp_path, goal_text="帮我处理这个工具问题")
        sidecar = tmp_path / "task-state" / "task-1.json"
        sidecar.parent.mkdir(parents=True)
        sidecar.write_text(json.dumps({
            "task_card": {
                "schema_version": 1,
                "task_id": "task-1",
                "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
                "thread_id": "topic:om_1",
                "card_message_id": "om_card",
                "last_sent_hash": "old",
                "last_update_ts": "2026-06-10T00:00:00+08:00",
                "user_state": "host-created",
                "milestones": [],
                "pending_confirms": [],
                "delivery": {"boundaries": []},
            }
        }), encoding="utf-8")
        updates = []
        monkeypatch.setattr(
            pnc_completion_notice_relay,
            "_update_shared_state_for_close_loop",
            lambda task_id, **kwargs: updates.append({"task_id": task_id, **kwargs}) or {"success": True},
        )

        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=["task-1"],
            send=True,
            send_card_func=lambda target, rendered, message_id: {"success": True, "message_id": message_id, "updated": True},
        )
        updated = json.loads(sidecar.read_text(encoding="utf-8"))["task_card"]
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True
    assert result["rows"][0]["close_loop_guard"]["to_state"] == "need_input"
    assert updates[0]["state"] == "need_input"
    assert updated["user_state"] == "awaiting_user"
    assert "需要补充" in updated["status_line"]


# --- @originator notification on human-action states -----------------------

def _write_roles(tmp_path, mapping):
    cfg_dir = tmp_path / "config"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    (cfg_dir / "user-roles.json").write_text(json.dumps({"user_id_mapping": mapping}), encoding="utf-8")


def _write_need_input_sidecar(tmp_path, task_id="task-1", *, user_state="awaiting_user", pending=None, last_notify_key=None):
    sidecar = tmp_path / "task-state" / f"{task_id}.json"
    sidecar.parent.mkdir(parents=True, exist_ok=True)
    card = {
        "schema_version": 1,
        "task_id": task_id,
        "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
        "thread_id": "topic:om_1",
        "message_id": "om_1",
        "card_message_id": "om_card",
        "last_sent_hash": "synced",  # card already in sync; only the ping is due
        "last_update_ts": "2026-06-10T00:00:00+08:00",
        "user_state": user_state,
        "status_line": "需要补充：输入路径、目标动作、验收人。",
        "milestones": [],
        "pending_confirms": pending or [],
        "delivery": {"boundaries": []},
    }
    if last_notify_key is not None:
        card["last_notify_key"] = last_notify_key
    sidecar.write_text(json.dumps({"task_card": card}), encoding="utf-8")
    # Pre-seed last_sent_hash with the real render hash so the card does not
    # also try to (re)send and we isolate the notify path.
    body = json.loads(sidecar.read_text(encoding="utf-8"))
    from gateway.feishu_task_card import render_task_card, stable_render_hash
    body["task_card"]["last_sent_hash"] = stable_render_hash(render_task_card(body["task_card"]))
    sidecar.write_text(json.dumps(body), encoding="utf-8")
    return sidecar




def test_v4_need_input_first_timeout_blocks_and_second_abandons(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    try:
        _write_roles(tmp_path, {"ou_liuxu": "刘旭"})
        _write_integration_tools_shared_state(tmp_path, state="need_input")
        meta_path = tmp_path / "runtime" / "shared-state" / "tasks" / "task-1" / "meta.json"
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        meta["requester"] = "ou_liuxu"
        meta_path.write_text(json.dumps(meta), encoding="utf-8")
        sidecar = _write_need_input_sidecar(tmp_path)
        updates = []
        monkeypatch.setattr(
            pnc_completion_notice_relay,
            "_update_shared_state_for_close_loop",
            lambda task_id, **kwargs: updates.append({"task_id": task_id, **kwargs}) or {"success": True},
        )
        text_calls = []
        send_func = lambda args: text_calls.append(args) or json.dumps({"success": True, "message_id": "om_ping"})
        card_func = lambda target, rendered, message_id: {"success": True, "message_id": message_id or "om_card", "updated": True}

        first = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=["task-1"],
            send=True,
            send_func=send_func,
            send_card_func=card_func,
        )
        first_card = json.loads(sidecar.read_text(encoding="utf-8"))["task_card"]
        # Simulate shared-state reflecting the first guard transition and aging again.
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        meta["state"] = "blocked"
        meta["latest_summary"] = "integration_tools close-loop guard: need_input -> blocked"
        meta["updated_at"] = "2026-06-10T00:00:00+08:00"
        meta_path.write_text(json.dumps(meta), encoding="utf-8")
        body = json.loads(sidecar.read_text(encoding="utf-8"))
        body["task_card"]["close_loop_guard_applied_at"] = "2026-06-10T00:00:00+08:00"
        body["task_card"]["need_input_first_timeout_at"] = "2026-06-10T00:00:00+08:00"
        sidecar.write_text(json.dumps(body), encoding="utf-8")
        second = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=["task-1"],
            send=True,
            send_func=send_func,
            send_card_func=card_func,
        )
    finally:
        reset_hermes_home_override(token)

    assert first["rows"][0]["close_loop_guard"]["to_state"] == "blocked"
    assert updates[0]["state"] == "blocked"
    assert first_card["user_state"] == "awaiting_user"
    assert '<at user_id="ou_liuxu">刘旭</at>' in text_calls[0]["message"]
    assert second["rows"][0]["close_loop_guard"]["to_state"] == "abandoned"
    assert updates[1]["state"] == "abandoned"

def test_need_input_pings_originator_with_at_mention(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    try:
        _write_roles(tmp_path, {"ou_liuxu": "刘旭"})
        _write_integration_tools_shared_state(tmp_path, state="need_input")
        # Add the originator to meta.requester.
        meta_path = tmp_path / "runtime" / "shared-state" / "tasks" / "task-1" / "meta.json"
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        meta["requester"] = "ou_liuxu"
        meta_path.write_text(json.dumps(meta), encoding="utf-8")
        sidecar = _write_need_input_sidecar(tmp_path)
        text_calls = []

        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=["task-1"],
            send=True,
            send_func=lambda args: text_calls.append(args) or json.dumps({"success": True, "message_id": "om_ping"}),
            send_card_func=lambda target, rendered, message_id: {"success": True, "message_id": message_id or "om_card", "updated": True},
        )
        updated = json.loads(sidecar.read_text(encoding="utf-8"))["task_card"]
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True
    assert len(text_calls) == 1
    assert '<at user_id="ou_liuxu">刘旭</at>' in text_calls[0]["message"]
    assert text_calls[0]["target"].endswith(":om_1")
    assert updated["last_notify_key"]
    assert result["rows"][0]["originator_notify"]["sent"] is True


def test_need_input_ping_is_idempotent_until_transition_changes(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    try:
        _write_roles(tmp_path, {"ou_liuxu": "刘旭"})
        _write_integration_tools_shared_state(tmp_path, state="need_input")
        meta_path = tmp_path / "runtime" / "shared-state" / "tasks" / "task-1" / "meta.json"
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        meta["requester"] = "ou_liuxu"
        meta_path.write_text(json.dumps(meta), encoding="utf-8")
        sidecar = _write_need_input_sidecar(tmp_path)
        text_calls = []
        send_func = lambda args: text_calls.append(args) or json.dumps({"success": True, "message_id": "om_ping"})
        card_func = lambda target, rendered, message_id: {"success": True, "message_id": message_id or "om_card", "updated": True}

        pnc_completion_notice_relay.relay_pending_notices(task_ids=["task-1"], send=True, send_func=send_func, send_card_func=card_func)
        # Second pass: nothing changed -> no re-ping.
        second = pnc_completion_notice_relay.relay_pending_notices(task_ids=["task-1"], send=True, send_func=send_func, send_card_func=card_func)

        # Passive write: updated_at advances but state+summary unchanged -> still NO re-ping.
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        meta["updated_at"] = "2026-06-10T00:30:00+08:00"
        meta_path.write_text(json.dumps(meta), encoding="utf-8")
        passive = pnc_completion_notice_relay.relay_pending_notices(task_ids=["task-1"], send=True, send_func=send_func, send_card_func=card_func)

        # Real transition: new latest_summary (re-triage) -> re-ping once.
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        meta["updated_at"] = "2026-06-10T01:00:00+08:00"
        meta["latest_summary"] = "re-triage: still missing acceptance owner"
        meta_path.write_text(json.dumps(meta), encoding="utf-8")
        third = pnc_completion_notice_relay.relay_pending_notices(task_ids=["task-1"], send=True, send_func=send_func, send_card_func=card_func)
    finally:
        reset_hermes_home_override(token)

    assert len(text_calls) == 2  # first + after-real-transition only (passive bump ignored)
    assert all(not r.get("originator_notify", {}).get("sent") for r in second["rows"])
    assert all(not r.get("originator_notify", {}).get("sent") for r in passive["rows"])  # the crux: passive bump => no spam
    assert any(r.get("originator_notify", {}).get("sent") for r in third["rows"])


def test_pending_confirm_pings_originator(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    try:
        _write_roles(tmp_path, {"ou_liuxu": "刘旭"})
        _write_integration_tools_shared_state(tmp_path, state="executing")
        meta_path = tmp_path / "runtime" / "shared-state" / "tasks" / "task-1" / "meta.json"
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        meta["requester"] = "ou_liuxu"
        meta_path.write_text(json.dumps(meta), encoding="utf-8")
        sidecar = _write_need_input_sidecar(
            tmp_path,
            user_state="awaiting_user",
            pending=[{"id": "c1", "question": "方案A还是方案B？", "preset": "plan_ab", "resolved": None}],
        )
        text_calls = []

        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=["task-1"],
            send=True,
            send_func=lambda args: text_calls.append(args) or json.dumps({"success": True, "message_id": "om_ping"}),
            send_card_func=lambda target, rendered, message_id: {"success": True, "message_id": message_id or "om_card", "updated": True},
        )
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True
    assert len(text_calls) == 1
    assert "确认" in text_calls[0]["message"]
    assert '<at user_id="ou_liuxu">刘旭</at>' in text_calls[0]["message"]
    assert result["rows"][0]["originator_notify"]["kind"] == "confirm"


def test_unknown_originator_skips_orphan_ping_instead_of_self_talking(tmp_path, monkeypatch):
    # Inverted 2026-06-26 (closed-loop-resilience): previously this emitted an
    # orphan "（未识别到发起人）" card with no <at> — a "机器人自言自语" degradation
    # (live: 7028467612). The guard now SKIPS the orphan ping and surfaces it to
    # ops instead, so nobody gets a card that pings no one.
    token = set_hermes_home_override(tmp_path)
    try:
        # No user-roles mapping and a session-key requester -> no open_id.
        _write_integration_tools_shared_state(tmp_path, state="need_input")
        meta_path = tmp_path / "runtime" / "shared-state" / "tasks" / "task-1" / "meta.json"
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        meta["requester"] = "agent:main:main"  # not an open_id
        meta_path.write_text(json.dumps(meta), encoding="utf-8")
        sidecar = _write_need_input_sidecar(tmp_path)
        text_calls = []

        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=["task-1"],
            send=True,
            send_func=lambda args: text_calls.append(args) or json.dumps({"success": True, "message_id": "om_ping"}),
            send_card_func=lambda target, rendered, message_id: {"success": True, "message_id": message_id or "om_card", "updated": True},
        )
    finally:
        reset_hermes_home_override(token)

    # No orphan @-ping is sent when the originator cannot be resolved.
    assert result["ok"] is True
    assert text_calls == []
    assert result["rows"][0]["originator_notify"]["skipped"] is True
    assert result["rows"][0]["originator_notify"]["reason"] == "originator_unresolved"


def test_non_integration_tools_line_is_not_pinged(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    try:
        _write_roles(tmp_path, {"ou_liuxu": "刘旭"})
        # g1q3-rca style task (business_line absent / not integration_tools).
        task_dir = tmp_path / "runtime" / "shared-state" / "tasks" / "task-1"
        task_dir.mkdir(parents=True)
        (task_dir / "meta.json").write_text(json.dumps({
            "task_id": "task-1", "state": "need_input", "user_id": "ou_liuxu",
            "updated_at": "2026-06-10T00:00:00+08:00",
        }), encoding="utf-8")
        sidecar = _write_need_input_sidecar(tmp_path)
        text_calls = []

        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=["task-1"],
            send=True,
            send_func=lambda args: text_calls.append(args) or json.dumps({"success": True, "message_id": "om_ping"}),
            send_card_func=lambda target, rendered, message_id: {"success": True, "message_id": message_id or "om_card", "updated": True},
        )
    finally:
        reset_hermes_home_override(token)

    assert text_calls == []  # excluded line: no @originator ping
    assert all(not r.get("originator_notify") for r in result["rows"])


def test_ping_retried_when_send_fails(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    try:
        _write_roles(tmp_path, {"ou_liuxu": "刘旭"})
        _write_integration_tools_shared_state(tmp_path, state="need_input")
        meta_path = tmp_path / "runtime" / "shared-state" / "tasks" / "task-1" / "meta.json"
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        meta["requester"] = "ou_liuxu"
        meta_path.write_text(json.dumps(meta), encoding="utf-8")
        sidecar = _write_need_input_sidecar(tmp_path)
        calls = {"n": 0}

        def flaky(args):
            calls["n"] += 1
            if calls["n"] == 1:
                return json.dumps({"success": False, "error": "transient"})
            return json.dumps({"success": True, "message_id": "om_ping"})

        card_func = lambda target, rendered, message_id: {"success": True, "message_id": message_id or "om_card", "updated": True}
        first = pnc_completion_notice_relay.relay_pending_notices(task_ids=["task-1"], send=True, send_func=flaky, send_card_func=card_func)
        # First send failed: last_notify_key must NOT be persisted -> still pending.
        mid = json.loads(sidecar.read_text(encoding="utf-8"))["task_card"]
        second = pnc_completion_notice_relay.relay_pending_notices(task_ids=["task-1"], send=True, send_func=flaky, send_card_func=card_func)
        after = json.loads(sidecar.read_text(encoding="utf-8"))["task_card"]
    finally:
        reset_hermes_home_override(token)

    assert "last_notify_key" not in mid  # not persisted on failure
    assert first["rows"][0]["originator_notify"]["sent"] is False
    assert calls["n"] == 2  # retried on the next pass
    assert after.get("last_notify_key")  # persisted after success


def test_backfill_stamps_existing_without_sending_and_is_idempotent(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    try:
        _write_roles(tmp_path, {"ou_liuxu": "刘旭"})
        # Terminal abandoned state: the close-loop guard does not touch it, so we
        # isolate backfill idempotence from guard-driven transitions.
        _write_integration_tools_shared_state(tmp_path, state="abandoned")
        meta_path = tmp_path / "runtime" / "shared-state" / "tasks" / "task-1" / "meta.json"
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        meta["requester"] = "ou_liuxu"
        meta_path.write_text(json.dumps(meta), encoding="utf-8")
        sidecar = _write_need_input_sidecar(tmp_path, user_state="abandoned")

        first = pnc_completion_notice_relay.backfill_originator_notify_keys()
        stamped_card = json.loads(sidecar.read_text(encoding="utf-8"))["task_card"]
        second = pnc_completion_notice_relay.backfill_originator_notify_keys()

        # After backfill, a real send pass must NOT ping (already stamped).
        text_calls = []
        pnc_completion_notice_relay.relay_pending_notices(
            task_ids=["task-1"],
            send=True,
            send_func=lambda args: text_calls.append(args) or json.dumps({"success": True}),
            send_card_func=lambda target, rendered, message_id: {"success": True, "message_id": "om_card", "updated": True},
        )
    finally:
        reset_hermes_home_override(token)

    assert first["stamped_count"] == 1
    assert stamped_card["last_notify_key"]
    assert stamped_card["last_notify_backfilled_at"]
    assert second["stamped_count"] == 0  # idempotent
    assert text_calls == []  # backfilled task is not retroactively pinged


def test_no_ping_when_target_lacks_thread_anchor(tmp_path, monkeypatch):
    # Hard rule: @ must go to the topic thread, never a bare main group.
    token = set_hermes_home_override(tmp_path)
    try:
        _write_roles(tmp_path, {"ou_liuxu": "刘旭"})
        _write_integration_tools_shared_state(tmp_path, state="need_input")
        meta_path = tmp_path / "runtime" / "shared-state" / "tasks" / "task-1" / "meta.json"
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        meta["requester"] = "ou_liuxu"
        meta_path.write_text(json.dumps(meta), encoding="utf-8")
        sidecar = _write_need_input_sidecar(tmp_path)
        # Strip every thread/message anchor so _card_target degrades to bare group.
        body = json.loads(sidecar.read_text(encoding="utf-8"))
        for k in ("thread_id", "message_id", "card_message_id"):
            body["task_card"].pop(k, None)
        from gateway.feishu_task_card import render_task_card, stable_render_hash
        body["task_card"]["last_sent_hash"] = stable_render_hash(render_task_card(body["task_card"]))
        sidecar.write_text(json.dumps(body), encoding="utf-8")
        text_calls = []

        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=["task-1"],
            send=True,
            send_func=lambda args: text_calls.append(args) or json.dumps({"success": True}),
            send_card_func=lambda target, rendered, message_id: {"success": True, "message_id": "om_card", "updated": True},
        )
    finally:
        reset_hermes_home_override(token)

    assert text_calls == []  # never floods the main group
    notify = result["rows"][0].get("originator_notify") or {}
    assert notify.get("reason") == "no_thread_anchor"



def _write_terminal_card_sidecar(tmp_path, task_id="task-1", *, with_contract=True, send_status="pending", delivery_sent=False, text=None, generated_at=None, suppressed_at=None, chat_id=None):
    sidecar = tmp_path / "task-state" / f"{task_id}.json"
    sidecar.parent.mkdir(parents=True, exist_ok=True)
    contract = {
        "required": True,
        "mode": "thread_reply",
        "must_carry": ["conclusion", "cause", "fixed_state", "html_url", "verification"],
        "at_originator": True,
        "max_attempts": 3,
    }
    notice = {
        "send_status": send_status,
        "state": "completed",
        "chat_id": chat_id or pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
        "thread_id": "topic:om_1",
        "message_id": "om_1",
        "vm_task_id": "vm-1",
        "text": text or "结论：完成\n原因：cause ok\n修复状态：fixed_state ok\n报告链接：https://example.invalid/report.html\n验证：verification ok\n<at user_id=\"ou_origin\">发起人</at>",
    }
    if generated_at is not None:
        notice["generated_at"] = generated_at
    if suppressed_at is not None:
        notice["suppressed_at"] = suppressed_at
    if with_contract:
        notice["completion_delivery"] = dict(contract)
    if delivery_sent:
        notice["delivery_sent"] = True
        notice["delivery_sent_marker"] = {"sent_at": "2026-06-16T00:00:00+08:00", "message_id": "om_sent"}
        notice["send_status"] = "sent"
    card = {
        "schema_version": 1,
        "task_id": task_id,
        "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
        "thread_id": "topic:om_1",
        "message_id": "om_1",
        "card_message_id": "om_card",
        "user_state": "completed",
        "status_line": "已完成",
        "milestones": [],
        "pending_confirms": [],
        "delivery": {"conclusion": "卡片摘要", "artifact_path": "https://example.invalid/report.html", "boundaries": [], "next_options": []},
    }
    if with_contract:
        card["completion_delivery"] = dict(contract)
    body = {"completion_notice": notice, "task_card": card}
    if with_contract:
        body["completion_delivery"] = dict(contract)
    sidecar.write_text(json.dumps(body, ensure_ascii=False), encoding="utf-8")
    body = json.loads(sidecar.read_text(encoding="utf-8"))
    from gateway.feishu_task_card import render_task_card, stable_render_hash
    body["task_card"]["last_sent_hash"] = stable_render_hash(render_task_card(body["task_card"]))
    sidecar.write_text(json.dumps(body, ensure_ascii=False), encoding="utf-8")
    return sidecar


def test_completion_delivery_required_bypasses_one_task_one_card_for_terminal_text(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        sidecar = _write_terminal_card_sidecar(tmp_path, with_contract=True)
        text_calls = []
        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=["task-1"],
            send=True,
            send_func=lambda args: text_calls.append(args) or json.dumps({"success": True, "message_id": "om_delivery"}),
            send_card_func=lambda target, rendered, message_id: {"success": True, "message_id": message_id or "om_card", "updated": True},
        )
        updated = json.loads(sidecar.read_text(encoding="utf-8"))
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True
    assert result["sent_count"] == 1
    assert len(text_calls) == 1
    msg = text_calls[0]["message"]
    for token_text in ["结论", "原因", "修复状态", "报告链接", "验证"]:
        assert token_text in msg
    assert '<at user_id="ou_origin">发起人</at>' in msg
    assert updated["completion_notice"]["send_status"] == "sent"
    assert updated["completion_notice"]["delivery_sent"] is True
    assert updated["completion_notice"]["delivery_sent_marker"]["message_id"] == "om_delivery"
    assert not result["rows"][0].get("text_suppressed")


def test_completion_delivery_sent_marker_prevents_duplicate_terminal_text(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        sidecar = _write_terminal_card_sidecar(tmp_path, with_contract=True)
        text_calls = []
        send_func = lambda args: text_calls.append(args) or json.dumps({"success": True, "message_id": "om_delivery"})
        card_func = lambda target, rendered, message_id: {"success": True, "message_id": message_id or "om_card", "updated": True}
        first = pnc_completion_notice_relay.relay_pending_notices(task_ids=["task-1"], send=True, send_func=send_func, send_card_func=card_func)
        second = pnc_completion_notice_relay.relay_pending_notices(task_ids=["task-1"], send=True, send_func=send_func, send_card_func=card_func)
    finally:
        reset_hermes_home_override(token)

    assert first["sent_count"] == 1
    assert second["sent_count"] == 0
    assert len(text_calls) == 1


def test_legacy_task_without_completion_delivery_keeps_one_card_suppression(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        sidecar = _write_terminal_card_sidecar(tmp_path, with_contract=False)
        text_calls = []
        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=["task-1"],
            send=True,
            send_func=lambda args: text_calls.append(args) or json.dumps({"success": True, "message_id": "om_delivery"}),
            send_card_func=lambda target, rendered, message_id: {"success": True, "message_id": message_id or "om_card", "updated": True},
        )
        updated = json.loads(sidecar.read_text(encoding="utf-8"))
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True
    assert result["sent_count"] == 0
    assert text_calls == []
    assert result["rows"][0]["text_suppressed"] is True
    assert result["rows"][0]["suppress_reason"] == "one_task_one_card"
    assert updated["completion_notice"]["send_status"] == "suppressed"


def test_120456_like_suppressed_completed_notice_is_relayed_when_contract_required(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        sidecar = _write_terminal_card_sidecar(tmp_path, task_id="20260616-120456", with_contract=True, send_status="suppressed")
        body = json.loads(sidecar.read_text(encoding="utf-8"))
        body["completion_notice"]["suppress_reason"] = "one_task_one_card"
        body["completion_notice"]["suppressed_at"] = "2026-06-16T12:30:00+08:00"
        sidecar.write_text(json.dumps(body, ensure_ascii=False), encoding="utf-8")
        text_calls = []
        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=["20260616-120456"],
            send=True,
            send_func=lambda args: text_calls.append(args) or json.dumps({"success": True, "message_id": "om_delivery_120456"}),
            send_card_func=lambda target, rendered, message_id: {"success": True, "message_id": message_id or "om_card", "updated": True},
        )
        updated = json.loads(sidecar.read_text(encoding="utf-8"))
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True
    assert result["sent_count"] == 1
    assert len(text_calls) == 1
    assert "结论" in text_calls[0]["message"]
    assert updated["completion_notice"]["send_status"] == "sent"
    assert updated["completion_notice"]["delivery_sent"] is True
    assert "suppress_reason" not in updated["completion_notice"]


def _iso_offset(seconds: int) -> str:
    return datetime.fromtimestamp(pnc_completion_notice_relay.RELAY_PROCESS_START_TS + seconds, tz=timezone.utc).isoformat()


def test_completion_delivery_new_generated_notice_full_scan_sends_and_marks(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        sidecar = _write_terminal_card_sidecar(tmp_path, with_contract=True, generated_at=_iso_offset(5))
        text_calls = []
        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=None,
            send=True,
            send_func=lambda args: text_calls.append(args) or json.dumps({"success": True, "message_id": "om_new"}),
            send_card_func=lambda target, rendered, message_id: {"success": True, "message_id": message_id or "om_card", "updated": True},
            explicit_completion_delivery=False,
        )
        updated = json.loads(sidecar.read_text(encoding="utf-8"))["completion_notice"]
    finally:
        reset_hermes_home_override(token)

    assert result["sent_count"] == 1
    assert len(text_calls) == 1
    assert '<at user_id="ou_origin">发起人</at>' in text_calls[0]["message"]
    assert updated["delivery_sent"] is True
    assert updated["delivery_sent_marker"]["message_id"] == "om_new"


def test_completion_delivery_explicit_historical_suppressed_task_is_allowed(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        old = _iso_offset(-3600)
        sidecar = _write_terminal_card_sidecar(tmp_path, task_id="20260616-120456", with_contract=True, send_status="suppressed", suppressed_at=old)
        body = json.loads(sidecar.read_text(encoding="utf-8"))
        body["completion_notice"]["suppress_reason"] = "one_task_one_card"
        sidecar.write_text(json.dumps(body, ensure_ascii=False), encoding="utf-8")
        text_calls = []
        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=["20260616-120456"],
            send=True,
            send_func=lambda args: text_calls.append(args) or json.dumps({"success": True, "message_id": "om_explicit"}),
            send_card_func=lambda target, rendered, message_id: {"success": True, "message_id": message_id or "om_card", "updated": True},
        )
        updated = json.loads(sidecar.read_text(encoding="utf-8"))["completion_notice"]
    finally:
        reset_hermes_home_override(token)

    assert result["sent_count"] == 1
    assert len(text_calls) == 1
    assert updated["delivery_sent_marker"]["message_id"] == "om_explicit"


def test_completion_delivery_full_scan_skips_historical_suppressed_contracts_including_business_group(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        old = _iso_offset(-3600)
        new = _iso_offset(5)
        business = "oc_6cfc782212009ff4cd815349909dd423"
        test_group = "oc_16614f4ba25b8c88b69c0b8e9ebc2fb5"
        for tid, chat in [("old-biz-1", business), ("old-biz-2", business), ("old-test", test_group)]:
            sidecar = _write_terminal_card_sidecar(tmp_path, task_id=tid, with_contract=True, send_status="suppressed", suppressed_at=old, chat_id=chat)
            body = json.loads(sidecar.read_text(encoding="utf-8"))
            body["completion_notice"]["suppress_reason"] = "one_task_one_card"
            sidecar.write_text(json.dumps(body, ensure_ascii=False), encoding="utf-8")
        _write_terminal_card_sidecar(tmp_path, task_id="new-test", with_contract=True, generated_at=new, chat_id=test_group)
        text_calls = []
        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=None,
            send=True,
            limit=20,
            send_func=lambda args: text_calls.append(args) or json.dumps({"success": True, "message_id": f"om_{len(text_calls)}"}),
            send_card_func=lambda target, rendered, message_id: {"success": True, "message_id": message_id or "om_card", "updated": True},
            explicit_completion_delivery=False,
        )
    finally:
        reset_hermes_home_override(token)

    sent_chat_ids = [row.get("chat_id") for row in result["rows"] if row.get("sent")]
    assert result["sent_count"] == 1
    assert sent_chat_ids == [test_group]
    assert sum(1 for call in text_calls if business in call["target"]) == 0


def test_completion_delivery_marker_is_written_on_success(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        sidecar = _write_terminal_card_sidecar(tmp_path, with_contract=True, generated_at=_iso_offset(5))
        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=None,
            send=True,
            send_func=lambda args: json.dumps({"success": True, "message_id": "om_marker"}),
            send_card_func=lambda target, rendered, message_id: {"success": True, "message_id": message_id or "om_card", "updated": True},
            explicit_completion_delivery=False,
        )
        notice = json.loads(sidecar.read_text(encoding="utf-8"))["completion_notice"]
    finally:
        reset_hermes_home_override(token)

    assert result["rows"][0]["delivery_sent"] is True
    assert notice["send_status"] == "sent"
    assert notice["delivery_sent"] is True
    assert notice["delivery_sent_at"]
    assert notice["delivery_sent_marker"]["message_id"] == "om_marker"
    assert notice["send_result"]["message_id"] == "om_marker"


def test_enriches_g1q3_card_delivery_from_shared_state_log(tmp_path):
    token = set_hermes_home_override(tmp_path)
    task_id = "20260617-164205-g1q3-rca-issue-intake-7015689036"
    try:
        shared = tmp_path / "runtime" / "shared-state" / "tasks" / task_id
        shared.mkdir(parents=True, exist_ok=True)
        artifact_root = tmp_path / "artifacts" / "g1q3"
        artifact_root.mkdir(parents=True, exist_ok=True)
        (artifact_root / "index.html").write_text("<html><body>ok</body></html>", encoding="utf-8")
        (shared / "meta.json").write_text(json.dumps({
            "state": "completed",
            "created_at": "2026-06-17T16:42:05+08:00",
            "updated_at": "2026-06-17T16:49:32+08:00",
            "import_source": "bridge-inbox",
            "artifact_root": str(artifact_root),
            "artifact_cifs_root": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/g1q3/",
        }), encoding="utf-8")
        (shared / "log.md").write_text('''gate=ready_to_download
Existing RCA HTML report was found and verified as delivery-ready
{
  "html_validation_state": "html_delivery_ready",
  "receipt_status": "hypothesis_ready",
  "candidate_cause": "OP 目标纵向位置波动/跳变超阈，建议核查感知融合/mono3d 测距链路。",
  "candidate_responsibility": "刘培瑞",
  "evidence_boundary": "原始 mcap 已落盘但 parsed/L2 assets 缺失"
}
''', encoding="utf-8")
        body = {
            "task_card": {"task_id": task_id, "user_state": "done", "delivery": {"conclusion": f"{task_id} completed"}, "milestones": []},
            "completion_notice": {"state": "completed", "generated_at": "2026-06-17T16:50:00+08:00"},
        }
        enriched = pnc_completion_notice_relay.enrich_g1q3_task_card_delivery(task_id, body)
    finally:
        reset_hermes_home_override(token)

    delivery = enriched["task_card"]["delivery"]
    assert "未生成 RCA 报告" in delivery["conclusion"]
    assert delivery["attribution_status"] == "hypothesis_ready"
    assert delivery["report_status"] == "need_user_data"
    assert delivery["candidate_cause"].startswith("低置信假设，待人工确认")
    assert "responsibility_candidate" not in delivery
    # CIFS (//hfs...) is a file share, not a web URL: it must NOT be rendered as
    # a clickable artifact_path (Feishu would mis-render it as a refused HTTPS
    # link).  The report is still surfaced via report_status + the CIFS pickup
    # directory in artifact_root.
    assert not str(delivery.get("artifact_path") or "").startswith("//")
    assert delivery["artifact_root"].startswith("//hfs1.minieye.tech")
    assert enriched["task_card"]["diagnostics"]["key_decision"] == "existing_report_draft_not_deliverable"
    labels = [item["label"] for item in enriched["task_card"]["milestones"]]
    assert any("本轮暂停" in label or "RCA 报告" in label for label in labels)
    assert any("本轮暂停" in label or "已接单" in label for label in labels)


def test_enrich_g1q3_card_uses_authoritative_business_result_over_log(tmp_path):
    # Regression (Feishu issue 7025381565, 2026-06-23): the card must read the
    # worker's authoritative business_result (gate_decision=skipped) instead of
    # regex-inferring gate/blocker/attribution from log text.  Here the log even
    # carries the misleading "ready_to_download" token and a stale
    # hypothesis_ready attribution — business_result must still win.
    token = set_hermes_home_override(tmp_path)
    task_id = "20260623-164448-g1q3-rca-issue-intake-7025381565"
    try:
        shared = tmp_path / "runtime" / "shared-state" / "tasks" / task_id
        shared.mkdir(parents=True, exist_ok=True)
        (shared / "meta.json").write_text(json.dumps({
            "state": "completed",
            "created_at": "2026-06-23T16:44:48+08:00",
            "updated_at": "2026-06-23T16:48:09+08:00",
            "import_source": "bridge-inbox",
        }), encoding="utf-8")
        (shared / "log.md").write_text(
            "status need_source_or_evidence ... ready_to_download token present here\n",
            encoding="utf-8",
        )
        (shared / "result.md").write_text(json.dumps({
            "status": "completed",
            "business_result": {
                "gate_decision": "skipped",
                "gate_skip_reason": "missing_or_invalid_pdcl_download_cmd",
                "status": "need_evidence",
                "terminal_state": "need_download",
                "work_item_id": "7025381565",
            },
        }), encoding="utf-8")
        body = {
            "task_card": {
                "task_id": task_id,
                "user_state": "done",
                "delivery": {"conclusion": f"{task_id} done", "attribution_status": "hypothesis_ready"},
                "milestones": [],
                "diagnostics": {"attribution_status": "hypothesis_ready"},
            },
            "completion_notice": {"state": "completed", "generated_at": "2026-06-23T16:50:00+08:00"},
        }
        enriched = pnc_completion_notice_relay.enrich_g1q3_task_card_delivery(task_id, body)
    finally:
        reset_hermes_home_override(token)

    card = enriched["task_card"]
    delivery = card["delivery"]
    diagnostics = card["diagnostics"]
    # Authoritative gate is "skipped", never the misleading ready_to_download.
    assert "event/clip 引用" in card["status_line"]
    assert "远程读取" in card["status_line"]
    assert "不执行 MDI 下载" in card["status_line"]
    assert "ready_to_download" not in card["status_line"]
    assert "继续下载/解析" not in card["status_line"]
    assert delivery["report_status"] == "need_user_data"
    # Zero evidence read -> no attribution surfaced (no hypothesis_ready).
    assert delivery.get("attribution_status", "") == ""
    assert diagnostics.get("attribution_status", "") == ""
    # Real blocker, never "无".
    assert diagnostics["blocker"] != "无"
    assert "PDCL" in diagnostics["blocker"] or "数据" in diagnostics["blocker"]
    labels = [item["label"] for item in card["milestones"]]
    assert any("本轮暂停" in label or "补充" in label for label in labels)
    assert all("gate=ready_to_download" not in label for label in labels)


def test_enrich_g1q3_card_stitches_latest_governance_report_contract(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    task_id = "20260627-142804-g1q3-rca-issue-intake-7029768863-68863_4a42ba"
    try:
        shared = tmp_path / "runtime" / "shared-state" / "tasks" / task_id
        shared.mkdir(parents=True, exist_ok=True)
        (shared / "meta.json").write_text(json.dumps({
            "state": "completed",
            "business_line": "g1q3_rca",
            "artifact_root": "/mnt/tmp/g1q3_rca_issue_intake_7029768863_4a42ba/",
            "updated_at": "2026-06-27T14:33:12+08:00",
        }), encoding="utf-8")
        (shared / "log.md").write_text("ready_to_download old blocked log\n", encoding="utf-8")
        gov = tmp_path / "pnc_agent" / "governance_rca"
        gov.mkdir(parents=True)
        (gov / "g1q3_rca_issue_intake_7029768863_8a6bed.json").write_text(json.dumps({
            "work_item_id": "7029768863",
            "artifact_root": "/mnt/tmp/g1q3_rca_issue_intake_7029768863_8a6bed/",
        }), encoding="utf-8")
        latest_contract = {
            "schema_version": "g1q3_delivery_contract_v1",
            "work_item_id": "7029768863",
            "business_state": "report_completed",
            "presentation_state": "report_ready_needs_review",
            "report": {
                "status": "html_delivery_ready",
                "is_deliverable": True,
                "is_candidate": True,
                "candidate_owner": "殷莉奇",
                "candidate_owner_domain": "ACC",
            },
            "summary": {"l0": "7029768863 RCA 报告已生成。", "short_conclusion": "候选因果判断：实际减速度偏重。"},
            "evidence_boundary": ["原始 mcap 已落盘；当前报告为候选 RCA，需人工复核。"],
            "artifacts": {
                "task_root_vm": "/mnt/tmp/g1q3_rca_issue_intake_7029768863_8a6bed",
                "task_root_cifs": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/g1q3_rca_issue_intake_7029768863_8a6bed",
                "case_dir_vm": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/7029768863_acc",
                "case_dir_cifs": "//hfs.minieye.tech/department-perception_test_team/G1Q3_RCA/cases/7029768863_acc",
                "primary_report_vm": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/7029768863_acc/index.html",
                "primary_report_cifs": "//hfs.minieye.tech/department-perception_test_team/G1Q3_RCA/cases/7029768863_acc/index.html",
                "report_data_vm": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/7029768863_acc/report_data.json",
            },
            "verification": {"terminal_state": "report_ready", "pipeline_status": "report_generated_need_review"},
        }
        monkeypatch.setattr(pnc_completion_notice_relay, "_read_vm_json_file", lambda path: latest_contract)
        body = {
            "task_card": {
                "task_id": task_id,
                "user_state": "in_progress",
                "delivery": {
                    "report_status": "need_download",
                    "conclusion": "需要发起人补充缺失字段后再继续 RCA",
                    "boundaries": [
                        "处理进展：需补充数据/证据",
                        "元数据门禁：skipped / out_of_scope",
                        "具体缺少：需要所属项目为 G1Q3_T1L_捷途，且发生时间不早于 2025-05-12",
                    ],
                },
                "milestones": [{"label": "闭环：转 need_input，已 @发起人补齐数据", "ts": "old"}],
                "close_loop_guard_reason": "g1q3_rca close-loop guard: completed -> blocked (intake 需补充数据/证据)",
            },
            "completion_notice": {"state": "completed", "generated_at": "2026-06-27T16:24:00+08:00"},
        }
        enriched = pnc_completion_notice_relay.enrich_g1q3_task_card_delivery(task_id, body)
    finally:
        reset_hermes_home_override(token)

    card = enriched["task_card"]
    assert card["user_state"] == "done"
    assert card["delivery"]["report_status"] == "html_delivery_ready"
    assert card["delivery"]["report_index_html_vm"].endswith("/7029768863_acc/index.html")
    assert "需要发起人补充" not in card["delivery"]["conclusion"]
    assert "close_loop_guard_reason" not in card
    assert all("need_input" not in item["label"] and "补齐数据" not in item["label"] for item in card["milestones"])
    boundary_text = "；".join(card["delivery"].get("boundaries") or [])
    assert "原始 mcap 已落盘" in boundary_text
    for stale in [
        "需补充数据",
        "out_of_scope",
        "元数据门禁",
        "具体缺少",
        "需要所属项目为 G1Q3_T1L_捷途",
        "need_input",
        "need_source_or_evidence",
    ]:
        assert stale not in boundary_text


def test_g1q3_close_loop_guard_flips_completed_need_download_to_blocked(tmp_path, monkeypatch):
    # P2 (issue 7025381565): a blocked intake imported from the VM as
    # state=completed must be re-asserted as `blocked` (active human-action) so
    # the @originator ping fires and the task stays resumable.
    token = set_hermes_home_override(tmp_path)
    task_id = "20260623-164448-g1q3-rca-issue-intake-7025381565"
    try:
        shared = tmp_path / "runtime" / "shared-state" / "tasks" / task_id
        shared.mkdir(parents=True, exist_ok=True)
        (shared / "meta.json").write_text(json.dumps({
            "state": "completed",
            "business_line": "g1q3_rca",
            "created_at": "2026-06-23T16:44:48+08:00",
            "updated_at": "2026-06-23T16:48:09+08:00",
        }), encoding="utf-8")
        calls = []

        def fake_update(task_id_arg, *, state, summary, status_text):
            calls.append({"task_id": task_id_arg, "state": state})
            return {"success": True, "state": state}

        monkeypatch.setattr(pnc_completion_notice_relay, "_update_shared_state_for_close_loop", fake_update)
        sidecar_path = tmp_path / "task-state" / f"{task_id}.json"
        sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        body = {
            "task_card": {
                "task_id": task_id,
                "user_state": "in_progress",
                "delivery": {"report_status": "need_download", "conclusion": "intake done", "boundaries": []},
                "milestones": [],
            },
        }
        sidecar_path.write_text(json.dumps(body), encoding="utf-8")
        out_body, action = pnc_completion_notice_relay.apply_g1q3_close_loop_guard(
            task_id, sidecar_path, body,
            now_ts=pnc_completion_notice_relay._parse_iso_ts("2026-06-23T18:00:00+08:00"),
        )
    finally:
        reset_hermes_home_override(token)

    assert action is not None and action["applied"] is True
    assert action["to_state"] == "blocked"
    assert calls and calls[0]["state"] == "blocked"
    card = out_body["task_card"]
    assert card["close_loop_guard_state"] == "blocked"
    # The @originator ping is driven by the shared-state being a human-action state.
    assert pnc_completion_notice_relay._human_action_kind("blocked", str(card.get("user_state") or ""), []) == "need_input"
    # The boundary tells the originator how to resume.
    boundary_text = "；".join(card["delivery"]["boundaries"])
    assert "问题数据地址_PDCL" in boundary_text
    assert "event/clip 引用" in boundary_text
    assert "不执行 MDI 下载" in boundary_text
    assert "Kafka 创建事件自动受理" in boundary_text
    assert "HERMES_RCA_MANUAL_CHAT_IDS 当前启用子集" in boundary_text
    assert "真实 @小助手" in boundary_text
    assert "分析/重跑 + 完整问题单 URL" in boundary_text
    assert "普通 URL、未 @ 或私聊仍只读" in boundary_text
    assert "统一受理、去重、代际控制和远程读取链路" in boundary_text
    assert "人工触发结果回到原任务话题" in boundary_text
    assert "RCA 新任务仅由 Kafka" not in boundary_text
    assert "重发问题链接，我会自动重跑" not in boundary_text


def _write_g1q3_infra_blocked(tmp_path, task_id, *, blocker):
    """g1q3-rca task: state=completed, report_status=need_download, with a
    structured business_result.blocker in result.md (the fault_class contract)."""
    shared = tmp_path / "runtime" / "shared-state" / "tasks" / task_id
    shared.mkdir(parents=True, exist_ok=True)
    (shared / "meta.json").write_text(json.dumps({
        "state": "completed",
        "business_line": "g1q3_rca",
        "created_at": "2026-06-26T11:21:55+08:00",
        "updated_at": "2026-06-26T11:27:48+08:00",
        "artifact_root": "/mnt/tmp/g1q3_rca_issue_intake_7028467612_57119c/",
    }), encoding="utf-8")
    (shared / "result.md").write_text(json.dumps({
        "business_result": {
            "gate_decision": "ready_to_download",
            "status": "need_evidence",
            "terminal_state": "need_download",
            "blocker": blocker,
        },
    }), encoding="utf-8")
    return shared


def test_infra_self_healable_task_does_not_ping_originator(tmp_path):
    # The 7028467612 fix: a retryable VM permission/infra fault must NOT @-ping
    # the issue originator (they cannot fix a VM ownership error).
    token = set_hermes_home_override(tmp_path)
    task_id = "20260626-112011-g1q3-rca-issue-intake-7028467612-67612_57119c"
    try:
        _write_g1q3_infra_blocked(tmp_path, task_id, blocker={
            "kind": "translate_service_unavailable",
            "fault_class": "infra_self_healable",
            "retryable": True,
        })
        body = {"task_card": {
            "task_id": task_id,
            "user_state": "in_progress",
            "delivery": {"report_status": "need_download"},
        }}
        pending = pnc_completion_notice_relay._originator_notify_pending(task_id, body)
        meta = pnc_completion_notice_relay._load_shared_state_meta(task_id)
        decision = pnc_completion_notice_relay.maybe_notify_originator(
            task_id=task_id, path=tmp_path / "x.json", body=body, meta=meta, send=False,
        )
    finally:
        reset_hermes_home_override(token)
    assert pending is False
    assert decision == {"skipped": True, "reason": "pipeline_fix_no_originator_ping", "kind": "need_input"}


def test_nested_fixture_pipeline_blocker_persists_and_routes_to_ops(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    task_id = "l4-infra-route-g1q3-rca"
    blocker = {
        "kind": "translate_workdir_permission",
        "fault_class": "infra_self_healable",
        "retryable": True,
        "message": "PermissionError in isolated fixture",
    }
    try:
        monkeypatch.setenv("HERMES_PNC_INFRA_ALERT", "1")
        shared = tmp_path / "runtime" / "shared-state" / "tasks" / task_id
        shared.mkdir(parents=True)
        meta = {
            "state": "blocked",
            "business_line": "g1q3_rca",
            "requester": "ou_l4_originator",
            "latest_summary": "infra self heal",
        }
        (shared / "meta.json").write_text(json.dumps(meta), encoding="utf-8")
        body = {
            "vm_delivery_proposal": {
                "schema_version": 1,
                "source": "pnc_vm_task_sync",
                "evidence_source": "fixture",
                "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
                "thread_id": "topic:om_l4_infra",
                "message_id": "om_l4_infra",
                "vm_task_id": "vm-l4-infra",
                "user_state": "in_progress",
                "delivery": {"report_status": "need_download"},
                "delivery_contract": {
                    "schema_version": "g1q3_delivery_contract_v1",
                    "business_state": "blocked_need_evidence",
                    "report": {"status": "need_download", "is_deliverable": False},
                    "pipeline_result": {
                        "status": "blocked",
                        "stage": "s3b_translate",
                        "blocker": blocker,
                    },
                },
            }
        }
        body = pnc_completion_notice_relay.reconcile_vm_delivery_proposal(task_id, body)
        body = pnc_completion_notice_relay.enrich_g1q3_task_card_delivery(task_id, body)
        card = body["task_card"]
        pending = pnc_completion_notice_relay._originator_notify_pending(task_id, body)
        originator = pnc_completion_notice_relay.maybe_notify_originator(
            task_id=task_id,
            path=tmp_path / "task-state" / f"{task_id}.json",
            body=body,
            meta=meta,
            send=False,
        )
        infra = pnc_completion_notice_relay.maybe_notify_infra_recovery(
            task_id=task_id,
            path=tmp_path / "task-state" / f"{task_id}.json",
            body=body,
            meta=meta,
            send=False,
        )
    finally:
        reset_hermes_home_override(token)

    assert card["delivery"]["report_status"] == "need_pipeline_fix"
    assert card["diagnostics"]["pipeline_blocker"] == blocker
    assert pending is False
    assert originator["reason"] == "pipeline_fix_no_originator_ping"
    assert infra["dry_run"] is True
    assert infra["kind"] == "infra_recovery"


def test_close_loop_guard_card_render_stable_no_fault_class(tmp_path, monkeypatch):
    # Regression for the 2026-06-26 card-flap flood: apply_g1q3_close_loop_guard
    # must NOT stamp a card-rendered field (e.g. the reverted fault_class) that the
    # second writer (vm_task_sync) strips — that flapped the render hash and
    # re-patched every full scan. Two consecutive guard passes must yield an
    # identical rendered card (idempotent, no flap) and carry no fault_class.
    from gateway.feishu_task_card import render_task_card, stable_render_hash
    token = set_hermes_home_override(tmp_path)
    task_id = "20260626-112011-g1q3-rca-issue-intake-7028467612-67612_57119c"
    try:
        _write_g1q3_infra_blocked(tmp_path, task_id, blocker={
            "kind": "translate_workdir_permission", "fault_class": "infra_self_healable", "retryable": True,
        })
        monkeypatch.setattr(
            pnc_completion_notice_relay, "_update_shared_state_for_close_loop",
            lambda *a, **k: {"success": True, "state": k.get("state")},
        )
        sidecar = tmp_path / "task-state" / f"{task_id}.json"
        sidecar.parent.mkdir(parents=True, exist_ok=True)
        body = {"task_card": {
            "task_id": task_id, "user_state": "in_progress",
            "delivery": {"report_status": "need_download", "conclusion": "intake done", "boundaries": []},
            "milestones": [], "chat_id": "oc_x", "message_id": "om_a",
        }}
        sidecar.write_text(json.dumps(body), encoding="utf-8")
        ts = pnc_completion_notice_relay._parse_iso_ts("2026-06-26T11:30:00+08:00")
        b1, a1 = pnc_completion_notice_relay.apply_g1q3_close_loop_guard(task_id, sidecar, json.loads(json.dumps(body)), now_ts=ts)
        h1 = stable_render_hash(render_task_card(b1["task_card"]))
        b2, _ = pnc_completion_notice_relay.apply_g1q3_close_loop_guard(task_id, sidecar, json.loads(json.dumps(b1)), now_ts=ts)
        h2 = stable_render_hash(render_task_card(b2["task_card"]))
    finally:
        reset_hermes_home_override(token)
    assert a1 is not None and a1["skipped"] == "pipeline_fix_not_user_data"
    assert a1["fault_class"] == "infra_self_healable"
    assert "fault_class" not in b1["task_card"], "fault_class must not be a card-rendered field (two-writer flap source)"
    assert h1 == h2, "close-loop guard card render flapped across passes (flood risk)"


def test_governance_rca_fallback_resolves_originator(tmp_path):
    # The VM-result path leaves meta without user_id; the governance_rca intake
    # record (written at admission) reliably has it. Verify the host recovers it.
    token = set_hermes_home_override(tmp_path)
    task_id = "20260626-112011-g1q3-rca-issue-intake-7028467612-67612_57119c"
    slug = "g1q3_rca_issue_intake_7028467612_57119c"
    try:
        gov_dir = tmp_path / "pnc_agent" / "governance_rca"
        gov_dir.mkdir(parents=True, exist_ok=True)
        (gov_dir / f"{slug}.json").write_text(json.dumps({
            "task_slug": slug,
            "work_item_id": "7028467612",
            "user_id": "ou_d1d3cfeba1be0a22faa36aaf4fb3907d",
        }), encoding="utf-8")
        meta = {  # no requester / user_id, only artifact_root carrying the slug
            "business_line": "g1q3_rca",
            "artifact_root": f"/mnt/tmp/{slug}/",
        }
        open_id = pnc_completion_notice_relay._resolve_originator_for_notify(task_id, meta)
    finally:
        reset_hermes_home_override(token)
    assert open_id == "ou_d1d3cfeba1be0a22faa36aaf4fb3907d"


def test_infra_recovery_ops_alert_is_env_gated_and_targets_ops_not_originator(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    task_id = "20260626-112011-g1q3-rca-issue-intake-7028467612-67612_57119c"
    try:
        _write_g1q3_infra_blocked(tmp_path, task_id, blocker={
            "kind": "translate_workdir_permission", "fault_class": "infra_self_healable", "retryable": True,
        })
        body = {"task_card": {
            "task_id": task_id, "user_state": "in_progress",
            "delivery": {"report_status": "need_download"},
            "chat_id": "oc_x", "message_id": "om_anchor",
        }}
        meta = pnc_completion_notice_relay._load_shared_state_meta(task_id)
        monkeypatch.delenv("HERMES_PNC_INFRA_ALERT", raising=False)
        off = pnc_completion_notice_relay.maybe_notify_infra_recovery(
            task_id=task_id, path=tmp_path / "x.json", body=body, meta=meta, send=False)
        monkeypatch.setenv("HERMES_PNC_INFRA_ALERT", "1")
        on = pnc_completion_notice_relay.maybe_notify_infra_recovery(
            task_id=task_id, path=tmp_path / "x.json", body=body, meta=meta, send=False)
    finally:
        reset_hermes_home_override(token)
    assert off == {"skipped": True, "reason": "infra_alert_disabled", "kind": "infra_recovery"}
    assert on["dry_run"] is True and on["kind"] == "infra_recovery"
    assert "建议恢复阶段：s3b_translate" in on["preview"]
    assert "统一 RCA 控制面执行受控重试" in on["preview"]
    assert "禁止直接运行旧阶段脚本或下载路径" in on["preview"]
    assert "--from-stage" not in on["preview"]
    assert "无需发起人补数据" in on["preview"]


def test_g1q3_close_loop_guard_skips_stale_task_to_avoid_retroactive_ping(tmp_path, monkeypatch):
    # Deploy-safety: a relay restart must NOT retroactively flip+ping a
    # long-settled need_download task (e.g. one from two weeks ago).
    token = set_hermes_home_override(tmp_path)
    task_id = "20260610-143325-g1q3-rca-issue-intake-6986500860"
    try:
        shared = tmp_path / "runtime" / "shared-state" / "tasks" / task_id
        shared.mkdir(parents=True, exist_ok=True)
        (shared / "meta.json").write_text(json.dumps({
            "state": "completed",
            "business_line": "g1q3_rca",
            "created_at": "2026-06-10T14:33:25+08:00",
        }), encoding="utf-8")
        calls = []
        monkeypatch.setattr(
            pnc_completion_notice_relay,
            "_update_shared_state_for_close_loop",
            lambda *a, **k: calls.append(k) or {"success": True},
        )
        sidecar_path = tmp_path / "task-state" / f"{task_id}.json"
        sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        body = {"task_card": {"task_id": task_id, "delivery": {"report_status": "need_download"}, "milestones": []}}
        sidecar_path.write_text(json.dumps(body), encoding="utf-8")
        # "now" is ~13 days after creation -> stale -> must NOT flip or ping.
        now_ts = pnc_completion_notice_relay._parse_iso_ts("2026-06-23T19:37:00+08:00")
        _out, action = pnc_completion_notice_relay.apply_g1q3_close_loop_guard(task_id, sidecar_path, body, now_ts=now_ts)
    finally:
        reset_hermes_home_override(token)

    assert action is None
    assert calls == []


def test_g1q3_close_loop_guard_idempotent_when_already_blocked(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    task_id = "20260623-164448-g1q3-rca-issue-intake-7025381565"
    try:
        shared = tmp_path / "runtime" / "shared-state" / "tasks" / task_id
        shared.mkdir(parents=True, exist_ok=True)
        (shared / "meta.json").write_text(json.dumps({
            "state": "blocked",
            "business_line": "g1q3_rca",
        }), encoding="utf-8")
        calls = []
        monkeypatch.setattr(
            pnc_completion_notice_relay,
            "_update_shared_state_for_close_loop",
            lambda *a, **k: calls.append(k) or {"success": True},
        )
        sidecar_path = tmp_path / "task-state" / f"{task_id}.json"
        sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        body = {"task_card": {"task_id": task_id, "delivery": {"report_status": "need_download"}, "milestones": []}}
        sidecar_path.write_text(json.dumps(body), encoding="utf-8")
        _out, action = pnc_completion_notice_relay.apply_g1q3_close_loop_guard(task_id, sidecar_path, body)
    finally:
        reset_hermes_home_override(token)

    # Already blocked -> no re-flip, no re-ping.
    assert action is None
    assert calls == []




def test_merge_task_card_preserves_download_notify_markers():
    current = {"task_id": "t", "status_line": "new"}
    previous = {
        "last_download_notify_key": "k",
        "last_download_notify_at": "2026-06-24T00:00:00+08:00",
        "last_download_notify_error": "old-error",
        "last_download_notify_skipped_reason": "no_thread_anchor",
    }

    merged = pnc_completion_notice_relay._merge_task_card_persistent_fields(current, previous)

    assert merged["last_download_notify_key"] == "k"
    assert merged["last_download_notify_at"] == "2026-06-24T00:00:00+08:00"
    assert merged["last_download_notify_error"] == "old-error"
    assert merged["last_download_notify_skipped_reason"] == "no_thread_anchor"


def test_g1q3_mechanical_download_failure_notifies_once(tmp_path, monkeypatch):
    _install_unit_test_active_relay_fence(monkeypatch)
    token = set_hermes_home_override(tmp_path)
    task_id = "20260623-220131-g1q3-rca-issue-intake-7025452822-52822_ffdab7"
    try:
        shared = tmp_path / "runtime" / "shared-state" / "tasks" / task_id
        shared.mkdir(parents=True, exist_ok=True)
        (shared / "meta.json").write_text(json.dumps({
            "state": "blocked",
            "business_line": "g1q3_rca",
            "requester": "ou_originator",
            "latest_summary": "g1q3_rca close-loop guard: completed -> blocked",
            "created_at": "2026-06-23T22:01:55+08:00",
        }), encoding="utf-8")
        (shared / "result.md").write_text(json.dumps({
            "verification": {
                "checked_original_pipeline_blocker": {
                    "kind": "invalid_schema_version",
                    "message": "expected g1q3_rca_execution_request_v1",
                }
            }
        }), encoding="utf-8")
        sidecar_path = tmp_path / "task-state" / f"{task_id}.json"
        sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        body = {
            "task_card": {
                "task_id": task_id,
                "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
                "message_id": "om_origin",
                "user_state": "in_progress",
                "delivery": {"report_status": "need_download", "conclusion": "intake done; waiting for data"},
                "milestones": [],
            }
        }
        sidecar_path.write_text(json.dumps(body), encoding="utf-8")
        sends = []
        def fake_send(args):
            sends.append(args)
            return json.dumps({"success": True, "message_id": "om_notify"})
        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=[task_id],
            send=True,
            send_func=fake_send,
            send_card_func=lambda *a, **k: {"success": True},
        )
        updated = json.loads(sidecar_path.read_text(encoding="utf-8"))
        second = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=[task_id],
            send=True,
            send_func=fake_send,
            send_card_func=lambda *a, **k: {"success": True},
        )
    finally:
        reset_hermes_home_override(token)

    row = result["rows"][0]
    assert row["close_loop_guard"]["skipped"] == "pipeline_fix_not_user_data"
    assert row["close_loop_guard"]["blocker_kind"] == "invalid_schema_version"
    assert row["originator_notify"]["reason"] == "pipeline_fix_no_originator_ping"
    assert "download_notify" not in row
    assert updated["task_card"]["delivery"]["report_status"] == "need_pipeline_fix"
    assert "last_download_notify_key" not in updated["task_card"]
    assert "download_notify" not in second["rows"][0]
    assert len(sends) == 0


def test_g1q3_mechanical_download_failure_notifies_dry_run_candidate_without_card_hash_change(tmp_path):
    token = set_hermes_home_override(tmp_path)
    task_id = "20260623-220131-g1q3-rca-issue-intake-7025452822-52822_ffdab7"
    try:
        shared = tmp_path / "runtime" / "shared-state" / "tasks" / task_id
        shared.mkdir(parents=True, exist_ok=True)
        (shared / "meta.json").write_text(json.dumps({
            "state": "blocked",
            "business_line": "g1q3_rca",
            "requester": "ou_originator",
            "latest_summary": "download blocked",
        }), encoding="utf-8")
        sidecar_path = tmp_path / "task-state" / f"{task_id}.json"
        sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        body = {
            "task_card": {
                "task_id": task_id,
                "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
                "message_id": "om_origin",
                "last_render_hash": "already-synced",
                "delivery": {"report_status": "need_download"},
                "diagnostics": {"download_blocker_kind": "missing_request"},
            }
        }
        sidecar_path.write_text(json.dumps(body), encoding="utf-8")
        result = pnc_completion_notice_relay.relay_pending_notices(task_ids=[task_id], send=False)
    finally:
        reset_hermes_home_override(token)

    assert result["candidate_count"] == 1
    row = result["rows"][0]
    assert row["originator_notify"]["dry_run"] is True
    assert row["originator_notify"]["kind"] == "need_data"
    assert row["originator_notify"]["open_id"] == "ou_originator"
    assert "download_notify" not in row

def test_g1q3_card_success_suppresses_duplicate_full_completion_text(
    tmp_path,
    monkeypatch,
):
    _install_unit_test_active_relay_fence(monkeypatch)
    os.environ["HERMES_G1Q3_ANOMALY_AUTO_NOTIFY"] = "1"
    token = set_hermes_home_override(tmp_path)
    task_id = "20260617-164205-g1q3-rca-issue-intake-7015689036"
    try:
        sidecar = _write_terminal_card_sidecar(tmp_path, task_id=task_id, with_contract=True, generated_at=_iso_offset(5))
        shared = tmp_path / "runtime" / "shared-state" / "tasks" / task_id
        shared.mkdir(parents=True, exist_ok=True)
        artifact_root = tmp_path / "artifacts" / "g1q3"
        artifact_root.mkdir(parents=True, exist_ok=True)
        (artifact_root / "index.html").write_text("<html><body>ok</body></html>", encoding="utf-8")
        (shared / "meta.json").write_text(json.dumps({
            "state": "completed",
            "artifact_root": str(artifact_root),
            "artifact_cifs_root": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/g1q3/",
        }), encoding="utf-8")
        (shared / "log.md").write_text('''Existing RCA HTML report was found
"html_validation_state": "html_delivery_ready"
"receipt_status": "hypothesis_ready"
"candidate_cause": "候选原因"
"candidate_responsibility": "刘培瑞"
''', encoding="utf-8")
        text_calls = []
        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=[task_id],
            send=True,
            send_func=lambda args: text_calls.append(args) or json.dumps({"success": True, "message_id": "om_text"}),
            send_card_func=lambda target, rendered, message_id: {"success": True, "message_id": message_id or "om_card", "updated": True},
        )
        updated = json.loads(sidecar.read_text(encoding="utf-8"))
    finally:
        os.environ.pop("HERMES_G1Q3_ANOMALY_AUTO_NOTIFY", None)
        reset_hermes_home_override(token)

    assert result["sent_count"] == 0
    assert len(text_calls) == 1
    assert "需人工确认" in text_calls[0]["message"]
    row = result["rows"][0]
    assert row["text_suppressed"] is True
    assert row["suppress_reason"] == "card_delivery_complete"
    assert updated["completion_notice"]["delivery_sent"] is True
    assert "未生成 RCA 报告" in updated["task_card"]["delivery"]["conclusion"] or "需补充" in updated["task_card"]["delivery"]["conclusion"]
    # CIFS share path is not a clickable web link; it is surfaced as the pickup
    # directory instead of a refused HTTPS artifact_path link.
    assert not str(updated["task_card"]["delivery"].get("artifact_path") or "").startswith("//")
    assert updated["task_card"]["delivery"]["artifact_root"].startswith("//hfs1.minieye.tech")


def test_v8_relay_consumes_vm_bridge_progress_into_milestones_and_heartbeat(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    try:
        sidecar = tmp_path / "task-state" / "task-progress.json"
        sidecar.parent.mkdir(parents=True)
        old_ts = "2026-06-10T00:00:00+00:00"
        sidecar.write_text(json.dumps({
            "updated_at": old_ts,
            "vm_bridge": {"state": "running", "progress": {"phase": "read_mcap", "message": "读取mcap", "ts": old_ts}},
            "recent_events": [{"phase": "sync_repo", "summary": "同步仓库", "ts": old_ts}],
            "task_card": {
                "schema_version": 1,
                "task_id": "task-progress",
                "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
                "thread_id": "topic:om_progress",
                "card_message_id": "om_card",
                "last_sent_hash": "old",
                "last_update_ts": old_ts,
                "user_state": "running",
                "milestones": [],
                "pending_confirms": [],
                "delivery": {},
            },
        }), encoding="utf-8")

        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=["task-progress"],
            send=False,
        )
        updated = json.loads(sidecar.read_text(encoding="utf-8"))["task_card"]
    finally:
        reset_hermes_home_override(token)

    labels = [item["label"] for item in updated["milestones"]]
    assert "执行阶段：读取mcap" in labels
    assert "执行阶段：同步仓库" in labels
    assert any("仍在执行，最近阶段 读取mcap" in item for item in labels)
    assert result["rows"][0]["task_card"]["dry_run"] is True


def test_v11_originator_notify_generalized_to_named_long_business_line(tmp_path, monkeypatch):
    _install_unit_test_active_relay_fence(monkeypatch)
    token = set_hermes_home_override(tmp_path)
    try:
        _write_roles(tmp_path, {"ou_liuxu": "刘旭"})
        task_dir = tmp_path / "runtime" / "shared-state" / "tasks" / "task-v11"
        task_dir.mkdir(parents=True)
        (task_dir / "meta.json").write_text(json.dumps({
            "task_id": "task-v11",
            "business_line": "g1q3_rca",
            "state": "need_input",
            "requester": "ou_liuxu",
            "updated_at": "2026-06-10T00:00:00+08:00",
        }), encoding="utf-8")
        _write_need_input_sidecar(tmp_path, task_id="task-v11")
        text_calls = []

        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=["task-v11"],
            send=True,
            send_func=lambda args: text_calls.append(args) or json.dumps({"success": True, "message_id": "om_ping"}),
            send_card_func=lambda target, rendered, message_id: {"success": True, "message_id": message_id or "om_card", "updated": True},
        )
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True
    assert len(text_calls) == 1
    assert '<at user_id="ou_liuxu">刘旭</at>' in text_calls[0]["message"]
    assert result["rows"][0]["originator_notify"]["sent"] is True


def test_v10_relay_backfills_delivery_contract_fields(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        sidecar = tmp_path / "task-state" / "task-v10-delivery.json"
        sidecar.parent.mkdir(parents=True)
        sidecar.write_text(json.dumps({
            "artifact_root": "/mnt/tmp/task-v10-delivery/",
            "artifact_cifs_root": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/task-v10-delivery/",
            "task_card": {
                "schema_version": 1,
                "task_id": "task-v10-delivery",
                "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
                "thread_id": "topic:om_v10",
                "card_message_id": "om_card",
                "last_sent_hash": "old",
                "user_state": "done",
                "delivery": {"conclusion": "已完成"},
            },
        }, ensure_ascii=False), encoding="utf-8")

        result = pnc_completion_notice_relay.relay_pending_notices(task_ids=["task-v10-delivery"], send=False)
        delivery = json.loads(sidecar.read_text(encoding="utf-8"))["task_card"]["delivery"]
    finally:
        reset_hermes_home_override(token)

    assert result["rows"][0]["task_card"]["dry_run"] is True
    assert delivery["input_original"] == "未落地/不适用"
    assert delivery["input_resolved"] == "未落地/不适用"
    assert delivery["artifact_vm"] == "/mnt/tmp/task-v10-delivery/"
    assert delivery["artifact_cifs"] == "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/task-v10-delivery/"
    assert delivery["cifs_status"] == "success"


# ---------------------------------------------------------------------------
# Regression: G1Q3-RCA intake card fixes (issue 7023754183, 2026-06-22)
# Three symptoms, one root cause: the relay rendered an unverified report
# button, a CIFS path as a clickable HTTPS link, and dropped per-phase
# milestones in the watch channel. These tests pin each fix on the real
# (non-mocked) code path.
# ---------------------------------------------------------------------------


def test_normalize_html_artifact_path_does_not_linkify_cifs_unc():
    # //hfs... is a CIFS share; it must NOT be returned as a clickable pointer.
    pointer, root = pnc_completion_notice_relay._normalize_html_artifact_path(
        "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/x/",
        report_status="html_delivery_ready",
    )
    assert pointer == ""  # no clickable button into a file share
    assert root.startswith("//hfs1.minieye.tech")


def test_normalize_html_artifact_path_does_not_linkify_vm_mnt_path():
    pointer, root = pnc_completion_notice_relay._normalize_html_artifact_path(
        "/mnt/tmp/g1q3_rca_issue_intake_7023754183_254d6d/",
        report_status="html_delivery_ready",
    )
    assert pointer == ""
    assert root.startswith("/mnt/tmp/")


def test_normalize_html_artifact_path_no_report_status_emits_no_pointer():
    # Without a confirmed report, never fabricate an index.html button.
    pointer, root = pnc_completion_notice_relay._normalize_html_artifact_path(
        "//hfs1.minieye.tech/department/tmp/x/",
        report_status="",
    )
    assert pointer == ""


def test_normalize_html_artifact_path_keeps_real_http_report_clickable():
    pointer, root = pnc_completion_notice_relay._normalize_html_artifact_path(
        "https://reports.example.com/g1q3/index.html",
    )
    assert pointer == "https://reports.example.com/g1q3/index.html"


def test_normalize_html_artifact_path_preserves_url_query_and_fragment():
    exact = "https://reports.example.com/g1q3/index.html?token=abc#section"
    pointer, root = pnc_completion_notice_relay._normalize_html_artifact_path(exact)
    assert pointer == exact
    assert root == "https://reports.example.com/g1q3/"


def test_normalize_html_artifact_path_rejects_malformed_urls():
    for malformed in ("http://[broken", "file://[broken"):
        assert pnc_completion_notice_relay._normalize_html_artifact_path(
            malformed,
            report_status="html_delivery_ready",
        ) == ("", "")

    pointer, root = pnc_completion_notice_relay._normalize_html_artifact_path(
        "https://reports.example.com/g1q3?token=abc#section",
        report_status="html_delivery_ready",
    )
    assert pointer == "https://reports.example.com/g1q3/index.html?token=abc#section"
    assert root == "https://reports.example.com/g1q3/"


def test_canonical_report_index_rejects_ambiguous_empty_path_segments():
    assert pnc_completion_notice_relay._canonical_user_visible_report_index(
        "/mnt/tmp/case-a//nested/",
    ) == ""


def test_is_cifs_unc_detects_shares_but_not_web_urls():
    assert pnc_completion_notice_relay._is_cifs_unc("//hfs1.minieye.tech/share/x")
    assert pnc_completion_notice_relay._is_cifs_unc("\\\\hfs1\\share")
    assert not pnc_completion_notice_relay._is_cifs_unc("https://x/y")
    assert not pnc_completion_notice_relay._is_cifs_unc("http://x/y")
    assert not pnc_completion_notice_relay._is_cifs_unc("/mnt/tmp/x")


def test_host_report_exists_false_for_empty_dir_true_for_real_index(tmp_path, monkeypatch):
    # Resolve a fake VM /mnt/tmp path through the real local-candidate mapping.
    home = tmp_path
    mount = home / "Mounts" / "mini_root" / "mnt" / "tmp" / "case_x"
    mount.mkdir(parents=True)
    monkeypatch.setattr(pnc_completion_notice_relay.Path, "home", staticmethod(lambda: home))
    # Empty dir -> no report.
    assert pnc_completion_notice_relay._host_report_exists("/mnt/tmp/case_x/") is False
    # Now materialize a real index.html.
    (mount / "index.html").write_text("<html></html>", encoding="utf-8")
    assert pnc_completion_notice_relay._host_report_exists("/mnt/tmp/case_x/") is True


def _write_g1q3_intake_sidecar(tmp_path, task_id):
    """Reproduce the 7023754183 shape: completed state, artifact_root set, but
    the gate only reached ready_to_download and no report was materialized."""
    home = tmp_path
    task_dir = home / "runtime" / "shared-state" / "tasks" / task_id
    task_dir.mkdir(parents=True)
    (task_dir / "meta.json").write_text(json.dumps({
        "task_id": task_id,
        "state": "completed",
        "business_line": "g1q3_rca",
        "import_source": "bridge-inbox",
        "artifact_root": f"/mnt/tmp/{task_id}/",
        "artifact_cifs_root": f"//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/{task_id}/",
        "created_at": "2026-06-22T11:01:37+08:00",
        "updated_at": "2026-06-22T11:05:32+08:00",
    }), encoding="utf-8")
    # Worker log tail carrying the real gate verdict.
    (task_dir / "log.md").write_text(
        'gate_result status need_evidence decision ready_to_download\n'
        '"status": "need_evidence", "decision": "ready_to_download"\n'
        'G4_data_structure: requires_download\n',
        encoding="utf-8",
    )
    chat_id = pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1]
    sidecar = home / "task-state" / f"{task_id}.json"
    sidecar.parent.mkdir(parents=True)
    sidecar.write_text(json.dumps({
        "task_card": {
            "task_id": task_id,
            "chat_id": chat_id,
            "thread_id": "topic:om_1",
            # No card_message_id -> card needs sync (relayable candidate).
            "milestones": [{"ts": "2026-06-22T11:01:41+08:00", "label": "任务建好"}],
            "delivery": {
                "artifact_path": f"//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/{task_id}/",
                "artifact_root": f"//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/{task_id}/",
            },
        },
        "completion_notice": {
            "state": "completed",
            "send_status": "sent",
            "chat_id": chat_id,
            "thread_id": "topic:om_1",
            "message_id": "om_1",
            "vm_task_id": task_id,
        },
    }), encoding="utf-8")
    return sidecar


def test_g1q3_intake_without_report_is_not_rendered_as_completed(tmp_path):
    task_id = "20260622-110137-g1q3-rca-issue-intake-7023754183"
    token = set_hermes_home_override(tmp_path)
    try:
        sidecar = _write_g1q3_intake_sidecar(tmp_path, task_id)
        body = json.loads(sidecar.read_text(encoding="utf-8"))
        out = pnc_completion_notice_relay.enrich_g1q3_task_card_delivery(task_id, body)
    finally:
        reset_hermes_home_override(token)
    card = out["task_card"]
    delivery = card["delivery"]
    # F4: not done; honest need-download state.
    assert card["user_state"] != "done"
    assert "ready_to_download" in delivery["conclusion"] or "未生成 RCA 报告" in delivery["conclusion"]
    # F1: no fabricated html report / clickable pointer.
    assert delivery.get("report_status") != "html_delivery_ready"
    assert not str(delivery.get("artifact_path") or "").endswith("index.html")
    # F3: per-phase milestones present beyond the seed (watch channel parity).
    labels = [m["label"] for m in card["milestones"]]
    assert "任务建好" in labels or any("已接单" in l for l in labels)
    assert any("已接单" in l for l in labels)
    assert len(labels) >= 2


def test_g1q3_enrichment_runs_in_watch_channel_without_explicit_filter(
    tmp_path,
    monkeypatch,
):
    """F3: relay watch loop (no task_ids filter) must still enrich g1q3 cards."""
    task_id = "20260622-110137-g1q3-rca-issue-intake-7023754183"
    monkeypatch.setattr(
        pnc_completion_notice_relay,
        "_automatic_g1q3_write_fence_ready",
        lambda _task_id: True,
    )
    _install_unit_test_active_relay_fence(monkeypatch)
    token = set_hermes_home_override(tmp_path)
    try:
        sidecar = _write_g1q3_intake_sidecar(tmp_path, task_id)
        # No task_ids -> explicit_task_filter is False (the watch channel).
        pnc_completion_notice_relay.relay_pending_notices(
            task_ids=None,
            send=True,
            send_func=lambda args: json.dumps({"success": True, "message_id": "om_x"}),
            send_card_func=lambda target, rendered, message_id: {"success": True, "message_id": message_id or "om_card", "updated": True},
        )
        updated = json.loads(sidecar.read_text(encoding="utf-8"))
    finally:
        reset_hermes_home_override(token)
    labels = [m["label"] for m in updated["task_card"]["milestones"]]
    # Enrichment fired in the auto channel: more than just the seed milestone.
    assert any("已接单" in l for l in labels), labels
    assert updated["task_card"].get("user_state") != "done"


def test_g1q3_automatic_scan_skips_unfenced_history_without_sidecar_mutation(
    tmp_path,
    monkeypatch,
):
    task_id = "20260622-110137-g1q3-rca-issue-intake-7023754183"
    token = set_hermes_home_override(tmp_path)
    try:
        sidecar = _write_g1q3_intake_sidecar(tmp_path, task_id)
        before = sidecar.read_bytes()
        monkeypatch.setattr(
            pnc_completion_notice_relay,
            "_automatic_g1q3_write_fence_ready",
            lambda _task_id: False,
        )
        sends = []
        cards = []

        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=None,
            send=True,
            send_func=lambda args: sends.append(args) or "{}",
            send_card_func=lambda *args, **kwargs: cards.append((args, kwargs)) or {},
            explicit_completion_delivery=False,
        )
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True
    assert result["candidate_count"] == 0
    assert result["errors"] == []
    assert sidecar.read_bytes() == before
    assert sends == []
    assert cards == []


def test_g1q3_name_only_automatic_scan_requires_fence_before_mutation_or_send(
    tmp_path,
    monkeypatch,
):
    task_id = "20260617-190920-g1q3-rca-status-check"
    token = set_hermes_home_override(tmp_path)
    try:
        sidecar = _write_g1q3_intake_sidecar(tmp_path, task_id)
        meta_path = (
            tmp_path / "runtime" / "shared-state" / "tasks" / task_id / "meta.json"
        )
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        meta.pop("business_line")
        meta_path.write_text(json.dumps(meta), encoding="utf-8")
        before = sidecar.read_bytes()
        body = json.loads(before)
        assert pnc_completion_notice_relay._is_g1q3_rca_origin_task(task_id, body)
        monkeypatch.setattr(
            pnc_completion_notice_relay,
            "_automatic_g1q3_write_fence_ready",
            lambda _task_id: False,
        )
        sends = []
        cards = []

        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=None,
            send=True,
            send_func=lambda args: sends.append(args) or "{}",
            send_card_func=lambda *args, **kwargs: cards.append((args, kwargs)) or {},
            explicit_completion_delivery=False,
        )
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True
    assert result["candidate_count"] == 0
    assert sidecar.read_bytes() == before
    assert sends == []
    assert cards == []

# ---------------------------------------------------------------------------
# Regression: G1Q3-RCA honest broadcast redesign (2026-06-22)
# ---------------------------------------------------------------------------

def test_g1q3_false_green_existing_index_is_downgraded_and_pings_anomaly(
    tmp_path,
    monkeypatch,
):
    _install_unit_test_active_relay_fence(monkeypatch)
    os.environ["HERMES_G1Q3_ANOMALY_AUTO_NOTIFY"] = "1"
    task_id = "20260622-201049-g1q3-rca-status-check-g1q3-rca"
    token = set_hermes_home_override(tmp_path)
    try:
        _write_roles(tmp_path, {"ou_lijinxia": "李锦霞", "ou_lilinxuan": "林丽旋"})
        shared = tmp_path / "runtime" / "shared-state" / "tasks" / task_id
        shared.mkdir(parents=True, exist_ok=True)
        artifact_root = tmp_path / "artifacts" / task_id
        artifact_root.mkdir(parents=True, exist_ok=True)
        (artifact_root / "index.html").write_text("<html>draft</html>", encoding="utf-8")
        (artifact_root / "gate_result.json").write_text(json.dumps({
            "decision": "ready_to_download",
            "G4_data_structure": "requires_download",
            "G5_time_alignment": "requires_download",
            "evidence_boundary": "原始 mcap 已落盘但 parsed/L2 assets 缺失，仅确认 ACC raw topic 证据入口",
        }, ensure_ascii=False), encoding="utf-8")
        (artifact_root / "report_data.json").write_text(json.dumps({
            "html_validation_state": "html_delivery_ready",
            "receipt_status": "hypothesis_ready",
            "candidate_cause": "车道线 lane_near_y_jump",
            "candidate_responsibility": "张再兹",
        }, ensure_ascii=False), encoding="utf-8")
        (shared / "meta.json").write_text(json.dumps({
            "task_id": task_id,
            "state": "completed",
            "business_line": "g1q3_rca",
            "requester": "ou_lijinxia",
            "issue_owner_open_id": "ou_lilinxuan",
            "issue_owner_name": "林丽旋",
            "artifact_root": str(artifact_root),
            "artifact_cifs_root": f"//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/{task_id}/",
            "updated_at": "2026-06-22T20:18:00+08:00",
        }, ensure_ascii=False), encoding="utf-8")
        (shared / "log.md").write_text("""terminal_state says report_ready
\"html_validation_state\": \"html_delivery_ready\"
\"receipt_status\": \"hypothesis_ready\"
gate_result.decision = ready_to_download
G4_data_structure requires_download
G5_time_alignment requires_download
候选原因：车道线 lane_near_y_jump
责任候选：张再兹
""", encoding="utf-8")
        sidecar = tmp_path / "task-state" / f"{task_id}.json"
        sidecar.parent.mkdir(parents=True)
        sidecar.write_text(json.dumps({
            "task_card": {
                "task_id": task_id,
                "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
                "thread_id": "topic:om_g1q3",
                "message_id": "om_g1q3",
                "card_message_id": "om_card",
                "last_sent_hash": "old",
                "user_state": "done",
                "delivery": {"artifact_root": str(artifact_root)},
                "milestones": [],
            },
            "completion_notice": {"state": "completed", "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1], "thread_id": "topic:om_g1q3", "message_id": "om_g1q3", "send_status": "sent"},
        }, ensure_ascii=False), encoding="utf-8")
        text_calls = []
        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=[task_id],
            send=True,
            send_func=lambda args: text_calls.append(args) or json.dumps({"success": True, "message_id": "om_ping"}),
            send_card_func=lambda target, rendered, message_id: {"success": True, "message_id": message_id or "om_card", "updated": True},
        )
        updated = json.loads(sidecar.read_text(encoding="utf-8"))
    finally:
        os.environ.pop("HERMES_G1Q3_ANOMALY_AUTO_NOTIFY", None)
        reset_hermes_home_override(token)

    delivery = updated["task_card"]["delivery"]
    rendered_text = json.dumps(pnc_completion_notice_relay.render_task_card(updated["task_card"]), ensure_ascii=False)
    assert delivery["report_status"] == "need_user_data"
    assert "未生成 RCA 报告" in delivery["conclusion"]
    assert "html_delivery_ready" not in rendered_text
    assert "报告可交付" not in rendered_text
    assert "报告已生成" not in rendered_text
    assert "张再兹" not in rendered_text
    assert updated["task_card"]["diagnostics"]["anomaly"] is True
    assert result["rows"][0]["anomaly_notify"]["sent"] is True
    assert len(text_calls) == 1
    assert '<at user_id="ou_lijinxia">李锦霞</at>' in text_calls[0]["message"]
    assert '<at user_id="ou_lilinxuan">林丽旋</at>' in text_calls[0]["message"]
    assert "需人工确认" in text_calls[0]["message"]


def test_g1q3_green_gate_with_parsed_l2_can_be_delivery_ready(tmp_path, monkeypatch):
    task_id = "20260622-210000-g1q3-rca-issue-intake-green"
    token = set_hermes_home_override(tmp_path)
    try:
        shared = tmp_path / "runtime" / "shared-state" / "tasks" / task_id
        shared.mkdir(parents=True, exist_ok=True)
        artifact_root = tmp_path / "artifacts" / task_id
        artifact_root.mkdir(parents=True, exist_ok=True)
        (artifact_root / "index.html").write_text("<html>ok</html>", encoding="utf-8")
        (artifact_root / "gate_result.json").write_text(json.dumps({"decision": "green", "parsed_l2_assets_present": True}), encoding="utf-8")
        (artifact_root / "report_data.json").write_text(json.dumps({"html_validation_state": "html_delivery_ready", "parsed_l2_assets_present": True}), encoding="utf-8")
        artifact_vm_root = f"/mnt/tmp/{task_id}/"
        artifact_cifs_root = f"//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/{task_id}/"
        monkeypatch.setattr(
            pnc_completion_notice_relay,
            "local_candidates_for_vm_path",
            lambda path: [artifact_root] if str(path).rstrip("/") == artifact_vm_root.rstrip("/") else [],
        )
        (shared / "meta.json").write_text(json.dumps({
            "state": "completed",
            "business_line": "g1q3_rca",
            "artifact_root": artifact_vm_root,
            "artifact_cifs_root": artifact_cifs_root,
            "updated_at": "2026-06-22T21:00:00+08:00",
        }), encoding="utf-8")
        body = {"task_card": {"task_id": task_id, "delivery": {}, "milestones": []}, "completion_notice": {"state": "completed"}}
        out = pnc_completion_notice_relay.enrich_g1q3_task_card_delivery(task_id, body)
    finally:
        reset_hermes_home_override(token)
    delivery = out["task_card"]["delivery"]
    assert delivery["report_status"] == "html_delivery_ready"
    assert delivery["artifact_root"] == artifact_cifs_root
    assert delivery["artifact_cifs"] == artifact_cifs_root
    assert delivery["artifact_path"] == artifact_cifs_root + "index.html"
    assert delivery["cifs_status"] == "success"
    assert delivery["conclusion"] == "RCA 报告已生成"
    assert out["task_card"]["user_state"] == "done"


@pytest.mark.parametrize(
    "artifact_cifs_root",
    ["", "//nonexistent.invalid/share/review/", "http://[broken"],
)
def test_g1q3_host_local_report_without_bound_pickup_surface_fails_closed(
    tmp_path,
    artifact_cifs_root,
):
    task_id = "20260622-210001-g1q3-rca-issue-intake-host-only"
    token = set_hermes_home_override(tmp_path)
    try:
        shared = tmp_path / "runtime" / "shared-state" / "tasks" / task_id
        shared.mkdir(parents=True, exist_ok=True)
        artifact_root = tmp_path / "artifacts" / task_id
        artifact_root.mkdir(parents=True, exist_ok=True)
        (artifact_root / "index.html").write_text("<html>ok</html>", encoding="utf-8")
        (artifact_root / "gate_result.json").write_text(json.dumps({
            "decision": "green",
            "parsed_l2_assets_present": True,
        }), encoding="utf-8")
        (artifact_root / "report_data.json").write_text(json.dumps({
            "html_validation_state": "html_delivery_ready",
            "parsed_l2_assets_present": True,
        }), encoding="utf-8")
        meta = {
            "state": "completed",
            "business_line": "g1q3_rca",
            "artifact_root": str(artifact_root),
        }
        if artifact_cifs_root:
            meta["artifact_cifs_root"] = artifact_cifs_root
        (shared / "meta.json").write_text(json.dumps(meta), encoding="utf-8")
        body = {
            "task_card": {"task_id": task_id, "delivery": {}, "milestones": []},
            "completion_notice": {"state": "completed"},
        }

        out = pnc_completion_notice_relay.enrich_g1q3_task_card_delivery(task_id, body)
    finally:
        reset_hermes_home_override(token)

    delivery = out["task_card"]["delivery"]
    rendered = json.dumps(
        pnc_completion_notice_relay.render_task_card(out["task_card"]),
        ensure_ascii=False,
    )
    assert out["task_card"]["user_state"] != "done"
    assert out["task_card"]["presentation"]["has_deliverable_report"] is False
    assert delivery["cifs_status"] != "success"
    assert delivery["artifact_label"] != "打开 HTML 报告"
    assert "成功，可从取件路径获取" not in rendered


def test_milestones_are_business_tz_sorted_and_semantic_deduped():
    raw = [
        {"ts": "2026-06-22T20:14:00+08:00", "label": "任务状态更新：pending"},
        {"ts": "2026-06-22T12:13:00+00:00", "label": "报告状态确认：need_download"},
        {"ts": "2026-06-22T20:15:00+08:00", "label": "任务状态更新：in_progress"},
        {"ts": "2026-06-22T20:16:00+08:00", "label": "报告状态确认：html_delivery_ready"},
        {"ts": "", "label": "字段/准入校验完成，gate=ready_to_download"},
    ]
    out = pnc_completion_notice_relay._trim_milestones(raw, limit=8)
    labels = [x["label"] for x in out]
    assert "任务状态更新：in_progress" in labels
    assert "任务状态更新：pending" not in labels
    assert "报告状态确认：need_download" not in labels
    parsed = [pnc_completion_notice_relay._parse_iso_ts(x["ts"]) for x in out if x["ts"]]
    assert parsed == sorted(parsed)
    assert all("T" not in x["ts"] and "+08:00" not in x["ts"] for x in out if x["ts"])
    assert any(x["ts"] == "2026-06-22 20:16:00" for x in out)


def test_g1q3_probe_structured_notice_uses_gate_truth_and_removes_report_link():
    from tools import vm_task_completion_probe

    text = vm_task_completion_probe._notice_from_rca_execution_result("G1Q3-7023754183", {
        "schema_version": "g1q3_rca_execution_result_v1",
        "status": "completed",
        "html_validation_state": "html_delivery_ready",
        "receipt_status": "hypothesis_ready",
        "verification": {"gate_result": {"decision": "ready_to_download", "pending": ["G4_data_structure: requires_download"]}},
        "readback": {"safe_for_group": True, "text": "报告可交付\n报告链接：file://x/index.html\n责任候选：张再兹\n候选原因：lane_near_y_jump"},
    })
    assert text is not None
    assert "gate=ready_to_download" in text
    assert "报告可交付" not in text
    assert "html_delivery_ready" not in text
    assert "报告已生成" not in text
    assert "报告链接" not in text
    assert "张再兹" not in text




def test_backfill_g1q3_anomaly_enriches_historical_sidecar_before_stamping(tmp_path):
    task_id = "20260622-201049-g1q3-rca-status-check-g1q3-rca"
    token = set_hermes_home_override(tmp_path)
    try:
        shared = tmp_path / "runtime" / "shared-state" / "tasks" / task_id
        shared.mkdir(parents=True, exist_ok=True)
        artifact_root = tmp_path / "artifacts" / task_id
        artifact_root.mkdir(parents=True, exist_ok=True)
        (artifact_root / "index.html").write_text("<html>draft</html>", encoding="utf-8")
        (artifact_root / "gate_result.json").write_text(json.dumps({
            "decision": "ready_to_download",
            "G4_data_structure": "requires_download",
            "G5_time_alignment": "requires_download",
        }, ensure_ascii=False), encoding="utf-8")
        (artifact_root / "report_data.json").write_text(json.dumps({
            "html_validation_state": "html_delivery_ready",
            "receipt_status": "hypothesis_ready",
            "candidate_cause": "low confidence",
        }, ensure_ascii=False), encoding="utf-8")
        (shared / "meta.json").write_text(json.dumps({
            "task_id": task_id,
            "state": "completed",
            "artifact_root": str(artifact_root),
            "updated_at": "2026-06-22T21:39:00+08:00",
        }, ensure_ascii=False), encoding="utf-8")
        sidecar = tmp_path / "task-state" / f"{task_id}.json"
        sidecar.parent.mkdir(parents=True)
        sidecar.write_text(json.dumps({
            "task_card": {
                "task_id": task_id,
                "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
                "thread_id": "topic:om_g1q3",
                "message_id": "om_g1q3",
                "card_message_id": "om_card",
                "user_state": "done",
                "delivery": {"artifact_root": str(artifact_root)},
            },
            "completion_notice": {"state": "completed", "send_status": "sent"},
        }, ensure_ascii=False), encoding="utf-8")

        result = pnc_completion_notice_relay.backfill_g1q3_anomaly_notify_keys()
        updated = json.loads(sidecar.read_text(encoding="utf-8"))
    finally:
        reset_hermes_home_override(token)

    assert result["stamped_count"] == 1
    assert updated["task_card"]["diagnostics"]["anomaly"] is True
    assert updated["task_card"]["last_anomaly_notify_key"].endswith("|g1q3_anomaly")
    assert updated["task_card"]["last_anomaly_notify_backfilled_at"]

def test_g1q3_anomaly_auto_notify_disabled_by_default(tmp_path, monkeypatch):
    _install_unit_test_active_relay_fence(monkeypatch)
    task_id = "20260622-201049-g1q3-rca-status-check-g1q3-rca"
    token = set_hermes_home_override(tmp_path)
    try:
        os.environ.pop("HERMES_G1Q3_ANOMALY_AUTO_NOTIFY", None)
        shared = tmp_path / "runtime" / "shared-state" / "tasks" / task_id
        shared.mkdir(parents=True, exist_ok=True)
        artifact_root = tmp_path / "artifacts" / task_id
        artifact_root.mkdir(parents=True, exist_ok=True)
        (artifact_root / "index.html").write_text("<html>draft</html>", encoding="utf-8")
        (artifact_root / "gate_result.json").write_text(json.dumps({"decision": "ready_to_download"}), encoding="utf-8")
        (shared / "meta.json").write_text(json.dumps({"state": "completed", "artifact_root": str(artifact_root)}), encoding="utf-8")
        sidecar = tmp_path / "task-state" / f"{task_id}.json"
        sidecar.parent.mkdir(parents=True)
        sidecar.write_text(json.dumps({
            "task_card": {
                "task_id": task_id,
                "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
                "thread_id": "topic:om_g1q3",
                "message_id": "om_g1q3",
                "card_message_id": "om_card",
                "last_sent_hash": "old",
                "user_state": "done",
                "delivery": {"artifact_root": str(artifact_root)},
                "diagnostics": {"anomaly": True},
            },
            "completion_notice": {"state": "completed", "send_status": "sent", "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1], "thread_id": "topic:om_g1q3", "message_id": "om_g1q3"},
        }, ensure_ascii=False), encoding="utf-8")
        text_calls = []
        result = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=[task_id],
            send=True,
            send_func=lambda args: text_calls.append(args) or json.dumps({"success": True, "message_id": "om_ping"}),
            send_card_func=lambda target, rendered, message_id: {"success": True, "message_id": message_id or "om_card", "updated": True},
        )
    finally:
        reset_hermes_home_override(token)

    assert text_calls == []
    assert result["rows"][0]["anomaly_notify"] == {"skipped": True, "reason": "auto_notify_disabled", "kind": "g1q3_anomaly"}

def test_g1q3_anomaly_notify_key_survives_later_delivery_mark_write(
    tmp_path,
    monkeypatch,
):
    _install_unit_test_active_relay_fence(monkeypatch)
    os.environ["HERMES_G1Q3_ANOMALY_AUTO_NOTIFY"] = "1"
    task_id = "20260622-110137-g1q3-rca-issue-intake-7023754183"
    token = set_hermes_home_override(tmp_path)
    try:
        _write_roles(tmp_path, {"ou_huzihao": "胡子豪", "ou_lilinxuan": "林丽旋"})
        shared = tmp_path / "runtime" / "shared-state" / "tasks" / task_id
        shared.mkdir(parents=True, exist_ok=True)
        artifact_root = tmp_path / "artifacts" / task_id
        artifact_root.mkdir(parents=True, exist_ok=True)
        (artifact_root / "index.html").write_text("<html>draft</html>", encoding="utf-8")
        (artifact_root / "gate_result.json").write_text(json.dumps({"decision": "reuse"}), encoding="utf-8")
        (artifact_root / "report_data.json").write_text(json.dumps({"html_validation_state": "html_delivery_ready"}), encoding="utf-8")
        (shared / "meta.json").write_text(json.dumps({
            "state": "completed",
            "latest_summary": f"{task_id} completed",
            "business_line": "g1q3_rca",
            "requester": "ou_huzihao",
            "issue_owner_open_id": "ou_lilinxuan",
            "artifact_root": str(artifact_root),
            "updated_at": "2026-06-22T21:00:00+08:00",
        }), encoding="utf-8")
        (shared / "log.md").write_text('"html_validation_state": "html_delivery_ready"\ngate=reuse\n', encoding="utf-8")
        sidecar = tmp_path / "task-state" / f"{task_id}.json"
        sidecar.parent.mkdir(parents=True)
        sidecar.write_text(json.dumps({
            "task_card": {
                "task_id": task_id,
                "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
                "thread_id": "topic:om_g1q3",
                "message_id": "om_g1q3",
                "card_message_id": "om_card",
                "last_sent_hash": "old",
                "user_state": "done",
                "delivery": {"artifact_root": str(artifact_root)},
                "milestones": [],
            },
            "completion_notice": {
                "state": "completed",
                "send_status": "suppressed",
                "suppress_reason": "one_task_one_card",
                "completion_delivery": {"required": True},
                "generated_at": "2026-06-22T21:00:01+08:00",
                "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
                "thread_id": "topic:om_g1q3",
                "message_id": "om_g1q3",
            },
        }, ensure_ascii=False), encoding="utf-8")
        text_calls = []
        first = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=[task_id],
            send=True,
            send_func=lambda args: text_calls.append(args) or json.dumps({"success": True, "message_id": "om_ping"}),
            send_card_func=lambda target, rendered, message_id: {"success": True, "message_id": message_id or "om_card", "updated": True},
            explicit_completion_delivery=True,
        )
        after_first = json.loads(sidecar.read_text(encoding="utf-8"))
        second = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=[task_id],
            send=True,
            send_func=lambda args: text_calls.append(args) or json.dumps({"success": True, "message_id": "om_ping2"}),
            send_card_func=lambda target, rendered, message_id: {"success": True, "message_id": message_id or "om_card", "updated": True},
            explicit_completion_delivery=True,
        )
    finally:
        os.environ.pop("HERMES_G1Q3_ANOMALY_AUTO_NOTIFY", None)
        reset_hermes_home_override(token)

    assert first["rows"][0]["anomaly_notify"]["sent"] is True
    assert after_first["task_card"].get("last_anomaly_notify_key")
    assert len(text_calls) == 1
    if second["rows"]:
        assert second["rows"][0].get("anomaly_notify", {}).get("sent") is not True
    assert len(text_calls) == 1


def test_g1q3_mechanical_download_failure_ledger_blocks_repeat_when_card_marker_missing(
    tmp_path,
    monkeypatch,
):
    _install_unit_test_active_relay_fence(monkeypatch)
    token = set_hermes_home_override(tmp_path)
    task_id = "20260623-220131-g1q3-rca-issue-intake-7025452822-52822_ffdab7"
    try:
        shared = tmp_path / "runtime" / "shared-state" / "tasks" / task_id
        shared.mkdir(parents=True, exist_ok=True)
        (shared / "meta.json").write_text(json.dumps({
            "state": "blocked",
            "business_line": "g1q3_rca",
            "requester": "ou_originator",
            "latest_summary": "download blocked",
        }), encoding="utf-8")
        (shared / "result.md").write_text(json.dumps({
            "verification": {"checked_original_pipeline_blocker": {"kind": "invalid_schema_version"}}
        }), encoding="utf-8")
        sidecar_path = tmp_path / "task-state" / f"{task_id}.json"
        sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        body = {
            "task_card": {
                "task_id": task_id,
                "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
                "message_id": "om_origin",
                "user_state": "in_progress",
                "delivery": {"report_status": "need_download", "conclusion": "waiting for data"},
            }
        }
        sidecar_path.write_text(json.dumps(body), encoding="utf-8")
        sends = []

        def fake_send(args):
            sends.append(args)
            return json.dumps({"success": True, "message_id": "om_notify"})

        first = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=[task_id],
            send=True,
            send_func=fake_send,
            send_card_func=lambda *a, **k: {"success": True},
        )
        overwritten = json.loads(sidecar_path.read_text(encoding="utf-8"))
        overwritten["task_card"].pop("last_download_notify_key", None)
        overwritten["task_card"].pop("last_download_notify_at", None)
        sidecar_path.write_text(json.dumps(overwritten), encoding="utf-8")
        second = pnc_completion_notice_relay.relay_pending_notices(
            task_ids=[task_id],
            send=True,
            send_func=fake_send,
            send_card_func=lambda *a, **k: {"success": True},
        )
    finally:
        reset_hermes_home_override(token)

    assert first["rows"][0]["close_loop_guard"]["skipped"] == "pipeline_fix_not_user_data"
    assert first["rows"][0]["originator_notify"]["reason"] == "pipeline_fix_no_originator_ping"
    assert "download_notify" not in first["rows"][0]
    assert "download_notify" not in second["rows"][0]
    assert len(sends) == 0


def test_g1q3_mechanical_download_failure_ledger_claim_blocks_duplicate_attempt(tmp_path):
    token = set_hermes_home_override(tmp_path)
    task_id = "task-g1q3-rca-ledger-claim"
    try:
        shared = tmp_path / "runtime" / "shared-state" / "tasks" / task_id
        shared.mkdir(parents=True, exist_ok=True)
        meta = {"state": "blocked", "business_line": "g1q3_rca"}
        (shared / "meta.json").write_text(json.dumps(meta), encoding="utf-8")
        body = {
            "task_card": {
                "task_id": task_id,
                "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
                "message_id": "om_origin",
                "delivery": {"report_status": "need_download"},
                "diagnostics": {"download_blocker_kind": "missing_request"},
            }
        }
        sidecar_path = tmp_path / "task-state" / f"{task_id}.json"
        sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        sidecar_path.write_text(json.dumps(body), encoding="utf-8")
        sends = []

        def fake_send(args):
            sends.append(args)
            return json.dumps({"success": True, "message_id": "om_notify"})

        first = pnc_completion_notice_relay.maybe_notify_mechanical_download_failure(
            task_id=task_id,
            path=sidecar_path,
            body=json.loads(sidecar_path.read_text(encoding="utf-8")),
            meta=meta,
            send=True,
            send_func=fake_send,
        )
        stale_body = {
            "task_card": {
                "task_id": task_id,
                "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
                "message_id": "om_origin",
                "delivery": {"report_status": "need_download"},
                "diagnostics": {"download_blocker_kind": "missing_request"},
            }
        }
        second = pnc_completion_notice_relay.maybe_notify_mechanical_download_failure(
            task_id=task_id,
            path=sidecar_path,
            body=stale_body,
            meta=meta,
            send=True,
            send_func=fake_send,
        )
    finally:
        reset_hermes_home_override(token)

    assert first["sent"] is True
    assert second["reason"] == "already_notified_ledger"
    assert len(sends) == 1
    assert "远程读取/数据处理链路" in sends[0]["message"]
    assert "不会回退到 MDI 下载" in sends[0]["message"]
    assert "自动下载/数据管线" not in sends[0]["message"]


def test_g1q3_report_ready_result_wins_over_stale_ready_to_download_log_and_preserves_case_dir(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    task_id = "20260624-165958-g1q3-rca-issue-intake-7026726390-26390_bc7e1d"
    try:
        shared = tmp_path / "runtime" / "shared-state" / "tasks" / task_id
        shared.mkdir(parents=True, exist_ok=True)
        shared.joinpath("meta.json").write_text(json.dumps({
            "state": "completed",
            "business_line": "g1q3_rca",
            "artifact_root": "/mnt/tmp/g1q3_rca_issue_intake_7026726390_bc7e1d/",
            "artifact_cifs_root": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/g1q3_rca_issue_intake_7026726390_bc7e1d/",
            "created_at": "2026-06-24T17:01:41+08:00",
            "updated_at": "2026-06-24T17:07:49+08:00",
        }), encoding="utf-8")
        shared.joinpath("log.md").write_text("gate=ready_to_download\nneed_source_or_evidence\n", encoding="utf-8")
        shared.joinpath("result.md").write_text(json.dumps({
            "schema_version": "shared_state_worker_result_v1",
            "work_item_id": "7026726390",
            "summary": {
                "terminal_state": "report_ready",
                "pipeline_status": "report_generated_need_review",
                "attribution_status": "hypothesis_ready",
            },
            "rca_observation": {
                "short_conclusion": "候选因果判断：实际减速度相对 OOI 加速度偏重。",
                "high_confidence_boundary": "非高置信自动归因；当前候选仍需人工 review",
            },
            "verification": {"checks": [
                {"name": "index_html_exists_nonempty", "ok": True},
                {"name": "report_data_exists_nonempty", "ok": True},
            ]},
            "artifacts": {
                "artifact_root_vm": "/mnt/tmp/g1q3_rca_issue_intake_7026726390_bc7e1d/",
                "artifact_root_cifs": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/g1q3_rca_issue_intake_7026726390_bc7e1d/",
                "case_dir_vm": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/7026726390_acc",
                "index_html_vm": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/7026726390_acc/index.html",
                "report_data_vm": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/7026726390_acc/report_data.json",
            },
        }, ensure_ascii=False), encoding="utf-8")
        body = {
            "task_card": {
                "task_id": task_id,
                "user_state": "done",
                "delivery": {"report_status": "need_download", "conclusion": "stale intake"},
                "milestones": [],
            },
            "completion_notice": {"state": "completed", "generated_at": "2026-06-24T17:07:50+08:00"},
        }
        enriched = pnc_completion_notice_relay.enrich_g1q3_task_card_delivery(task_id, body)
        sidecar = tmp_path / "task-state" / f"{task_id}.json"
        sidecar.parent.mkdir(parents=True)
        sidecar.write_text(json.dumps(enriched, ensure_ascii=False), encoding="utf-8")
        calls = []
        monkeypatch.setattr(pnc_completion_notice_relay, "_update_shared_state_for_close_loop", lambda *a, **k: calls.append(k) or {"success": True})
        out, action = pnc_completion_notice_relay.apply_g1q3_close_loop_guard(
            task_id, sidecar, enriched,
            now_ts=pnc_completion_notice_relay._parse_iso_ts("2026-06-24T17:09:05+08:00"),
        )
    finally:
        reset_hermes_home_override(token)

    delivery = out["task_card"]["delivery"]
    assert action is None
    assert calls == []
    assert delivery["report_status"] == "html_delivery_ready"
    assert out["task_card"]["user_state"] == "done"
    assert "需补齐数据" not in out["task_card"]["status_line"]
    assert delivery["agent_artifact_root_vm"].startswith("/mnt/tmp/")
    assert delivery["agent_artifact_root_cifs"].startswith("//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/")
    assert delivery["business_case_dir_vm"].startswith("/mnt/minieye/pdcl/department/perception_test_team/")
    assert delivery["business_case_dir_cifs"].startswith("//hfs.minieye.tech/department-perception_test_team/")
    assert delivery["report_index_html_cifs"].endswith("/index.html")


def test_g1q3_7026690721_strict_closeout_log_blocks_false_need_input_when_artifacts_unmounted(tmp_path, monkeypatch):
    """Real 2026-06-24 shape: host cannot read /mnt/tmp artifacts, result.md is generic.

    The only host-visible report-ready evidence is the VM closeout text copied
    into shared log.md.  This must still win over stale card need_download and
    must not let the G1Q3 close-loop guard mutate the task to blocked/need_input.
    """
    token = set_hermes_home_override(tmp_path)
    task_id = "20260624-185722-g1q3-rca-issue-intake-7026690721-90721_13e562"
    try:
        shared = tmp_path / "runtime" / "shared-state" / "tasks" / task_id
        shared.mkdir(parents=True, exist_ok=True)
        artifact_root = "/mnt/tmp/g1q3_rca_issue_intake_7026690721_13e562/"
        shared.joinpath("meta.json").write_text(json.dumps({
            "state": "completed",
            "business_line": "g1q3_rca",
            "artifact_root": artifact_root,
            "artifact_cifs_root": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/g1q3_rca_issue_intake_7026690721_13e562/",
            "created_at": "2026-06-24T18:58:46+08:00",
            "updated_at": "2026-06-24T19:06:05+08:00",
        }), encoding="utf-8")
        shared.joinpath("result.md").write_text(json.dumps({
            "task_id": task_id,
            "run_id": f"worker-{task_id}",
            "artifact_root": artifact_root,
            "artifacts": [
                {"path": f"/home/mini/.hermes/worker-state/tasks/{task_id}/artifacts/codex-last-message.txt"},
                {"path": f"/home/mini/.hermes/worker-state/tasks/{task_id}/artifacts/runner.log"},
            ],
            "result_mode": "structured-result-artifact-only",
        }, ensure_ascii=False), encoding="utf-8")
        shared.joinpath("log.md").write_text("""
gate=ready_to_download
need_source_or_evidence
# VM readonly closeout
```json
{
  "schema_version": "openclaw_vm_closeout_v4_v8",
  "v4_result": {
    "status": "completed",
    "terminal_state": "report_ready",
    "pipeline_status": "report_generated_need_review",
    "rca_business_status": "completed"
  },
  "v5_artifacts": {
    "artifact_root": "/mnt/tmp/g1q3_rca_issue_intake_7026690721_13e562/",
    "artifact_cifs_root": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/g1q3_rca_issue_intake_7026690721_13e562/",
    "case_dir": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/7026690721_acc",
    "index_html": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/7026690721_acc/index.html",
    "report_data": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/7026690721_acc/report_data.json"
  },
  "v6_verification": {"verified": true},
  "v7_gates_and_risks": {
    "confidence_boundary": "非高置信自动归因；当前候选仍需人工 review"
  }
}
```
""", encoding="utf-8")
        body = {
            "task_card": {
                "task_id": task_id,
                "user_state": "done",
                "delivery": {
                    "report_status": "need_download",
                    "conclusion": "intake 与准入校验完成；待下载/解析数据后再出 RCA 结论",
                    "boundaries": ["请在飞书问题卡片补充 问题数据地址_PDCL"],
                },
                "milestones": [
                    {"ts": "2026-06-24 19:06:05", "label": "闭环：转 need_input，已 @发起人补齐数据"},
                    {"ts": "", "label": "字段/准入校验完成，gate=report"},
                ],
            },
            "completion_notice": {"state": "completed", "generated_at": "2026-06-24T19:05:51+08:00"},
        }
        enriched = pnc_completion_notice_relay.enrich_g1q3_task_card_delivery(task_id, body)
        sidecar = tmp_path / "task-state" / f"{task_id}.json"
        sidecar.parent.mkdir(parents=True)
        sidecar.write_text(json.dumps(enriched, ensure_ascii=False), encoding="utf-8")
        calls = []
        monkeypatch.setattr(pnc_completion_notice_relay, "_update_shared_state_for_close_loop", lambda *a, **k: calls.append(k) or {"success": True})
        notify_pending = pnc_completion_notice_relay._originator_notify_pending(task_id, enriched)
        notify_result = pnc_completion_notice_relay.maybe_notify_originator(
            task_id=task_id,
            path=sidecar,
            body=enriched,
            meta=json.loads(shared.joinpath("meta.json").read_text(encoding="utf-8")),
            send=False,
        )
        out, action = pnc_completion_notice_relay.apply_g1q3_close_loop_guard(
            task_id,
            sidecar,
            enriched,
            now_ts=pnc_completion_notice_relay._parse_iso_ts("2026-06-24T19:06:05+08:00"),
        )
    finally:
        reset_hermes_home_override(token)

    delivery = out["task_card"]["delivery"]
    assert action is None
    assert calls == []
    assert notify_pending is False
    assert notify_result == {"skipped": True, "reason": "report_ready_no_need_input", "kind": "need_input"}
    assert delivery["report_status"] == "html_delivery_ready"
    assert out["task_card"]["user_state"] == "done"
    assert "需补齐数据" not in out["task_card"]["status_line"]
    assert not any("问题数据地址_PDCL" in str(item) for item in delivery.get("boundaries", []))
    assert not any("need_input" in str(item.get("label", "")) or "gate=" in str(item.get("label", "")) for item in out["task_card"].get("milestones", []))
    assert delivery["artifact_path"] == delivery["report_index_html_cifs"]
    assert not str(delivery["artifact_path"]).startswith("http://")
    assert delivery["publication_url_status"] == "blocked_missing_canonical_https"
    assert delivery["artifact_root"] == "//hfs.minieye.tech/department-perception_test_team/G1Q3_RCA/cases/7026690721_acc"
    assert delivery["business_case_dir_cifs"] == "//hfs.minieye.tech/department-perception_test_team/G1Q3_RCA/cases/7026690721_acc"
    assert delivery["agent_artifact_root_vm"] == "/mnt/tmp/g1q3_rca_issue_intake_7026690721_13e562/"
    assert delivery["agent_artifact_root_cifs"].rstrip("/") == "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/g1q3_rca_issue_intake_7026690721_13e562"


def test_g1q3_delivery_contract_v1_report_completed_wins_over_stale_need_download(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    task_id = "20260624-200000-g1q3-rca-issue-intake-contract-report"
    try:
        shared = tmp_path / "runtime" / "shared-state" / "tasks" / task_id
        shared.mkdir(parents=True, exist_ok=True)
        shared.joinpath("meta.json").write_text(json.dumps({
            "state": "blocked",
            "business_line": "g1q3_rca",
            "artifact_root": "/mnt/tmp/contract-report/",
        }), encoding="utf-8")
        shared.joinpath("log.md").write_text("old gate=ready_to_download\n", encoding="utf-8")
        body = {
            "delivery_contract": {
                "schema_version": "g1q3_delivery_contract_v1",
                "task_id": task_id,
                "run_id": f"worker-{task_id}",
                "execution_state": "completed",
                "business_state": "report_completed",
                "presentation_state": "report_ready_needs_review",
                "report": {
                    "status": "report_generated_need_review",
                    "is_deliverable": True,
                    "is_candidate": True,
                    "requires_human_review": True,
                    "candidate_owner": "殷莉奇",
                    "candidate_owner_domain": "ACC",
                },
                "summary": {"l0": "RCA 候选报告已生成，当前需候选 owner 人工复核。"},
                "evidence_boundary": ["parsed/L2 assets 缺失", "视频不可用"],
                "artifacts": {
                    "task_root_vm": "/mnt/tmp/contract-report/",
                    "task_root_cifs": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/contract-report/",
                    "case_dir_vm": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/contract_acc",
                    "primary_report_vm": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/contract_acc/index.html",
                    "report_data_vm": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/contract_acc/report_data.json",
                    "viz_mcap_vm": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/contract_acc/contract_acc.viz.mcap",
                    "attribution_causal_text": "目标输入异常 -> ACC 纵向请求波动",
                },
                "verification": {"terminal_state": "report_ready", "pipeline_status": "report_generated_need_review"},
            },
            "task_card": {
                "task_id": task_id,
                "user_state": "done",
                "delivery": {"report_status": "need_download", "conclusion": "stale intake"},
                "milestones": [{"label": "闭环：转 need_input，已 @发起人补齐数据"}],
            },
            "completion_notice": {"state": "completed"},
        }
        out = pnc_completion_notice_relay.enrich_g1q3_task_card_delivery(task_id, body)
        sidecar = tmp_path / "task-state" / f"{task_id}.json"
        sidecar.parent.mkdir(parents=True)
        sidecar.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
        calls = []
        monkeypatch.setattr(pnc_completion_notice_relay, "_update_shared_state_for_close_loop", lambda *a, **k: calls.append(k) or {"success": True})
        notify_pending = pnc_completion_notice_relay._originator_notify_pending(task_id, out)
        _body, action = pnc_completion_notice_relay.apply_g1q3_close_loop_guard(task_id, sidecar, out)
    finally:
        reset_hermes_home_override(token)

    delivery = out["task_card"]["delivery"]
    assert action is None
    assert calls == []
    assert notify_pending is False
    assert delivery["report_status"] == "report_ready"
    assert delivery["responsibility_candidate"] == "殷莉奇"
    assert delivery["artifact_path"] == delivery["foxglove_url"]
    assert delivery["artifact_label"] == "打开 foxglove 可视化"
    assert delivery["foxglove_url"].endswith("/contract_acc/contract_acc.viz.mcap")
    assert delivery["attribution_causal_text"] == "目标输入异常 -> ACC 纵向请求波动"
    assert delivery["artifact_cifs"] == "//hfs.minieye.tech/department-perception_test_team/G1Q3_RCA/cases/contract_acc"
    assert "parsed/L2 assets 缺失" in "；".join(delivery["boundaries"])
    assert not any("need_input" in str(item.get("label", "")) for item in out["task_card"].get("milestones", []))


def test_shared_result_viz_only_requires_verified_nonempty_check(monkeypatch):
    case_dir = "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/viz_only"
    payload = {
        "schema_version": "shared_state_worker_result_v1",
        "summary": {"terminal_state": "report_ready", "pipeline_status": "report_generated_need_review"},
        "rca_observation": {"short_conclusion": "目标输入异常 -> 纵向请求波动"},
        "verification": {"checks": [{"name": "viz_mcap_exists_nonempty", "ok": True}]},
        "artifacts": {
            "case_dir_vm": case_dir,
            "viz_mcap_vm": f"{case_dir}/viz_only.viz.mcap",
            "attribution_causal_text": "目标输入异常 -> 纵向请求波动",
        },
    }
    monkeypatch.setattr(pnc_completion_notice_relay, "_load_shared_result_payload", lambda task_id: payload)
    monkeypatch.setattr(pnc_completion_notice_relay, "_load_execution_request_from_goal", lambda task_id: {})

    truth = pnc_completion_notice_relay._shared_result_report_ready("viz-only")

    assert truth["index_html_vm"] == ""
    assert truth["foxglove_url"].endswith("/viz_only/viz_only.viz.mcap")
    assert truth["attribution_causal_text"] == "目标输入异常 -> 纵向请求波动"


def test_g1q3_delivery_contract_v1_missing_user_input_does_not_false_green(tmp_path):
    token = set_hermes_home_override(tmp_path)
    task_id = "20260624-200000-g1q3-rca-issue-intake-contract-missing"
    try:
        shared = tmp_path / "runtime" / "shared-state" / "tasks" / task_id
        shared.mkdir(parents=True, exist_ok=True)
        shared.joinpath("meta.json").write_text(json.dumps({
            "state": "completed",
            "business_line": "g1q3_rca",
            "artifact_root": "/mnt/tmp/contract-missing/",
        }), encoding="utf-8")
        shared.joinpath("log.md").write_text("html_delivery_ready old draft\n", encoding="utf-8")
        body = {
            "delivery_contract": {
                "schema_version": "g1q3_delivery_contract_v1",
                "task_id": task_id,
                "execution_state": "completed",
                "business_state": "missing_user_input",
                "presentation_state": "need_user_input",
                "report": {"status": "need_download", "is_deliverable": False},
                "user_action": {"requires_user_input": True, "next_action_text": "请补充问题数据地址_PDCL"},
                "artifacts": {
                    "task_root_vm": "/mnt/tmp/contract-missing/",
                    "task_root_cifs": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/contract-missing/",
                },
            },
            "task_card": {
                "task_id": task_id,
                "user_state": "done",
                "delivery": {"report_status": "html_delivery_ready", "conclusion": "stale false green"},
                "milestones": [],
            },
            "completion_notice": {"state": "completed"},
        }
        out = pnc_completion_notice_relay.enrich_g1q3_task_card_delivery(task_id, body)
    finally:
        reset_hermes_home_override(token)

    delivery = out["task_card"]["delivery"]
    assert delivery["report_status"] == "need_user_data"
    assert "未生成 RCA 报告" in delivery["conclusion"]
    assert out["task_card"]["user_state"] == "in_progress"
    assert "event/clip 引用" in out["task_card"]["status_line"]
    assert "不执行 MDI 下载" in out["task_card"]["status_line"]
    assert "responsibility_candidate" not in delivery


def _write_goal_for_request_parser(tmp_path, monkeypatch, content) -> None:
    task_dir = tmp_path / "task"
    task_dir.mkdir(parents=True, exist_ok=True)
    goal_path = task_dir / "goal.md"
    if isinstance(content, bytes):
        goal_path.write_bytes(content)
    else:
        goal_path.write_text(content, encoding="utf-8")
    monkeypatch.setattr(
        pnc_completion_notice_relay,
        "_shared_state_task_dir",
        lambda _task_id: task_dir,
    )


def _fixed_request_goal(request: object) -> str:
    canonical = json.dumps(
        request,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return (
        "<!-- G1Q3_RCA_EXECUTION_REQUEST_JSON:BEGIN -->\n"
        f"{canonical}\n"
        "<!-- G1Q3_RCA_EXECUTION_REQUEST_JSON:END -->"
    )


def _legacy_request_goal(request_json: str) -> str:
    return (
        "## RcaExecutionRequest JSON\n"
        "```json\n"
        f"{request_json}\n"
        "```\n"
    )


def test_execution_request_goal_parser_prefers_unique_canonical_fixed_marker(
    tmp_path, monkeypatch
):
    fixed = {
        "schema_version": "g1q3_rca_execution_request_v2",
        "work_item": {"work_item_id": "fixed"},
    }
    legacy = {"work_item": {"work_item_id": "legacy"}}
    goal = (
        _fixed_request_goal(fixed)
        + "\n- cd /home/mini/data3/yj-evaluation-server\n"
        + _legacy_request_goal(json.dumps(legacy))
    )
    _write_goal_for_request_parser(tmp_path, monkeypatch, goal)

    assert pnc_completion_notice_relay._load_execution_request_from_goal("task") == (
        fixed
    )


@pytest.mark.parametrize(
    "corruption",
    [
        "duplicate_block",
        "missing_end",
        "end_before_begin",
        "multiline",
        "noncanonical",
        "duplicate_key",
        "non_object",
        "marker_injection",
        "deep_nesting",
    ],
)
def test_execution_request_goal_parser_rejects_ambiguous_or_malicious_fixed_marker(
    tmp_path, monkeypatch, corruption
):
    begin = "<!-- G1Q3_RCA_EXECUTION_REQUEST_JSON:BEGIN -->"
    end = "<!-- G1Q3_RCA_EXECUTION_REQUEST_JSON:END -->"
    canonical = '{"a":1,"b":2}'
    if corruption == "duplicate_block":
        fixed = f"{begin}\n{canonical}\n{end}\n{begin}\n{canonical}\n{end}"
    elif corruption == "missing_end":
        fixed = f"{begin}\n{canonical}\n"
    elif corruption == "end_before_begin":
        fixed = f"{end}\n{begin}\n{canonical}"
    elif corruption == "multiline":
        fixed = f'{begin}\n{{\n"a":1\n}}\n{end}'
    elif corruption == "noncanonical":
        fixed = f'{begin}\n{{"b":2, "a":1}}\n{end}'
    elif corruption == "duplicate_key":
        fixed = f'{begin}\n{{"a":1,"a":2}}\n{end}'
    elif corruption == "non_object":
        fixed = f"{begin}\n[1,2]\n{end}"
    elif corruption == "deep_nesting":
        payload = '{"nested":' + "[" * 1_500 + "0" + "]" * 1_500 + "}"
        fixed = f"{begin}\n{payload}\n{end}"
    else:
        injected = json.dumps(
            {"text": "<!-- G1Q3_RCA_ADMISSION_JSON:BEGIN -->"},
            sort_keys=True,
            separators=(",", ":"),
        )
        fixed = f"{begin}\n{injected}\n{end}"
    valid_legacy = _legacy_request_goal('{"legacy":true}')
    _write_goal_for_request_parser(
        tmp_path,
        monkeypatch,
        fixed + "\n" + valid_legacy,
    )

    assert pnc_completion_notice_relay._load_execution_request_from_goal("task") == {}


def test_execution_request_goal_parser_keeps_bounded_legacy_object_compatibility(
    tmp_path, monkeypatch
):
    legacy = {
        "data": {"pdcl_download_cmd": "historical-shape"},
        "work_item": {"work_item_id": "legacy"},
    }
    pretty = json.dumps(legacy, ensure_ascii=False, indent=2)
    _write_goal_for_request_parser(
        tmp_path,
        monkeypatch,
        _legacy_request_goal(pretty),
    )

    assert pnc_completion_notice_relay._load_execution_request_from_goal("task") == (
        legacy
    )


@pytest.mark.parametrize(
    "legacy_json",
    [
        "[]",
        '{"a":1,"a":2}',
        "NaN",
    ],
)
def test_execution_request_goal_parser_rejects_invalid_legacy_shapes(
    tmp_path, monkeypatch, legacy_json
):
    _write_goal_for_request_parser(
        tmp_path,
        monkeypatch,
        _legacy_request_goal(legacy_json),
    )

    assert pnc_completion_notice_relay._load_execution_request_from_goal("task") == {}


def test_execution_request_goal_parser_rejects_oversize_and_invalid_utf8(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        pnc_completion_notice_relay,
        "RCA_EXECUTION_REQUEST_MAX_BYTES",
        32,
    )
    _write_goal_for_request_parser(
        tmp_path,
        monkeypatch,
        _legacy_request_goal(json.dumps({"value": "x" * 64})),
    )
    assert pnc_completion_notice_relay._load_execution_request_from_goal("task") == {}

    _write_goal_for_request_parser(tmp_path, monkeypatch, b"\xff\xfe\xfd")
    assert pnc_completion_notice_relay._load_execution_request_from_goal("task") == {}


def test_execution_request_goal_parser_rejects_oversize_goal(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(pnc_completion_notice_relay, "RCA_GOAL_MAX_BYTES", 32)
    _write_goal_for_request_parser(
        tmp_path,
        monkeypatch,
        _fixed_request_goal({"value": "bounded-request"}),
    )

    assert pnc_completion_notice_relay._load_execution_request_from_goal("task") == {}


def test_execution_request_goal_parser_rejects_duplicate_legacy_envelopes(
    tmp_path, monkeypatch
):
    block = _legacy_request_goal('{"legacy":true}')
    _write_goal_for_request_parser(tmp_path, monkeypatch, block + block)

    assert pnc_completion_notice_relay._load_execution_request_from_goal("task") == {}


def test_g1q3_report_ready_enrichment_uses_report_html_and_issue_input(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    task_id = "20260624-165958-g1q3-rca-issue-intake-7026726390-26390_bc7e1d"
    try:
        shared = tmp_path / "runtime" / "shared-state" / "tasks" / task_id
        shared.mkdir(parents=True, exist_ok=True)
        shared.joinpath("meta.json").write_text(json.dumps({
            "state": "blocked",
            "business_line": "g1q3_rca",
            "artifact_root": "/mnt/tmp/g1q3_rca_issue_intake_7026726390_bc7e1d/",
            "artifact_cifs_root": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/g1q3_rca_issue_intake_7026726390_bc7e1d/",
        }), encoding="utf-8")
        shared.joinpath("goal.md").write_text('''## RcaExecutionRequest JSON
```json
{"data":{"pdcl_download_cmd":"mdi download event -u 7445 -s ./"},"work_item":{"work_item_id":"7026726390","url":"https://project.feishu.cn/t03o4q/issue/detail/7026726390"}}
```
''', encoding="utf-8")
        shared.joinpath("log.md").write_text("gate=ready_to_download\n", encoding="utf-8")
        shared.joinpath("result.md").write_text(json.dumps({
            "summary": {"terminal_state": "report_ready", "pipeline_status": "report_generated_need_review", "attribution_status": "hypothesis_ready"},
            "rca_observation": {"short_conclusion": "候选因果判断：实际减速度相对 OOI 加速度偏重。；建议由 控制 继续核查。"},
            "verification": {"checks": [{"name": "index_html_exists_nonempty", "ok": True}, {"name": "report_data_exists_nonempty", "ok": True}]},
            "artifacts": {
                "artifact_root_vm": "/mnt/tmp/g1q3_rca_issue_intake_7026726390_bc7e1d/",
                "artifact_root_cifs": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/g1q3_rca_issue_intake_7026726390_bc7e1d/",
                "case_dir_vm": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/7026726390_acc",
                "index_html_vm": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/7026726390_acc/index.html",
                "report_data_vm": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/7026726390_acc/report_data.json",
            },
        }, ensure_ascii=False), encoding="utf-8")
        body = {"task_card": {"task_id": task_id, "user_state": "done", "delivery": {}, "milestones": []}, "completion_notice": {"state": "completed"}}
        out = pnc_completion_notice_relay.enrich_g1q3_task_card_delivery(task_id, body)
    finally:
        reset_hermes_home_override(token)
    delivery = out["task_card"]["delivery"]
    assert delivery["artifact_path"] == delivery["report_index_html_cifs"]
    assert not str(delivery["artifact_path"]).startswith("http://")
    assert delivery["publication_url_status"] == "blocked_missing_canonical_https"
    assert delivery["artifact_root"] == "//hfs.minieye.tech/department-perception_test_team/G1Q3_RCA/cases/7026726390_acc"
    assert delivery["report_index_html_http"] == "http://192.168.26.174:18081/G1Q3_RCA/cases/7026726390_acc/index.html"
    assert delivery["business_case_dir_http"] == "http://192.168.26.174:18081/G1Q3_RCA/cases/7026726390_acc"
    assert delivery["artifact_vm"] == "/mnt/tmp/g1q3_rca_issue_intake_7026726390_bc7e1d/"
    assert delivery["artifact_cifs"] == "//hfs.minieye.tech/department-perception_test_team/G1Q3_RCA/cases/7026726390_acc"
    assert delivery["input_original"] == "飞书问题 7026726390 + 远程直读 (历史 v1 event/clip 引用；不执行 MDI 下载)"
    assert delivery["input_resolved"] == "远程直读 (历史 v1 event/clip 引用；不执行 MDI 下载)"
    assert "7445" not in delivery["input_original"]
    assert "7445" not in delivery["input_resolved"]


def test_compact_input_label_uses_v2_remote_read_summary_without_identifiers():
    request = {
        "work_item": {"work_item_id": "7026726390"},
        "data": {
            "data_access": {
                "mode": "remote_read",
                "references": [
                    {"kind": "event", "event_uuid": "sensitive-event"},
                    {"kind": "clip", "clip_uuid": "sensitive-clip"},
                ],
            },
        },
    }

    label = pnc_completion_notice_relay._compact_input_label(request)

    assert label == "飞书问题 7026726390 + 远程直读 (clip x1, event x1；不执行 MDI 下载)"
    assert "sensitive-event" not in label
    assert "sensitive-clip" not in label


def test_legacy_mdi_evidence_is_neutralized_without_false_need_input():
    text = pnc_completion_notice_relay._remote_read_user_text(
        "历史证据来源：mdi download event -u opaque -s ./"
    )

    assert text == pnc_completion_notice_relay.REMOTE_REFERENCE_COMPATIBILITY_NOTE
    assert "event/clip 引用" in text
    assert "不执行 MDI 下载" in text
    assert "需在飞书问题单" not in text
    assert "opaque" not in text


@pytest.mark.parametrize(
    "legacy_text",
    [
        "mdi clip -u opaque -s ./",
        "mdi event -u opaque -s ./",
        "mdi refresh2 -u opaque -s ./",
        "mdi download event -u opaque -s ./；不执行 MDI 下载",
    ],
)
def test_every_legacy_mdi_form_is_neutralized_before_user_render(legacy_text):
    text = pnc_completion_notice_relay._remote_read_user_text(legacy_text)

    assert text == pnc_completion_notice_relay.REMOTE_REFERENCE_COMPATIBILITY_NOTE
    assert "opaque" not in text
    assert legacy_text not in text


def test_legacy_need_input_reason_requests_remote_reference_not_download():
    reason = pnc_completion_notice_relay._need_input_reason(
        {
            "delivery": {
                "missing_reason": "请补充问题数据地址_PDCL：mdi download event -u opaque -s ./",
            },
        },
        {},
    )

    assert reason == pnc_completion_notice_relay.REMOTE_REFERENCE_GUIDANCE
    assert "只提取引用并远程读取" in reason
    assert "不执行 MDI 下载" in reason
    assert "mdi refresh" not in reason.lower()
    assert "mdi download" not in reason.lower()
    assert "opaque" not in reason


def test_perception_test_team_http_maps_vm_report_path():
    url = pnc_completion_notice_relay._perception_test_team_http(
        "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/7026726390_acc/index.html"
    )
    assert url == "http://192.168.26.174:18081/G1Q3_RCA/cases/7026726390_acc/index.html"


def test_perception_test_team_http_rejects_path_traversal():
    assert pnc_completion_notice_relay._perception_test_team_http(
        "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/../secret/index.html"
    ) == ""


def test_publication_origin_requires_explicit_shared_origin(monkeypatch):
    monkeypatch.delenv("PNC_FOXGLOVE_RENDER_HOST", raising=False)
    monkeypatch.setattr(
        pnc_completion_notice_relay,
        "PERCEPTION_TEST_TEAM_HTTP_BASE",
        "http://192.168.26.174:18081/",
    )

    assert pnc_completion_notice_relay._canonical_publication_report_origin() == ""
    assert pnc_completion_notice_relay._canonical_publication_report_url(
        "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/demo/index.html"
    ) == ""
    assert pnc_completion_notice_relay._validated_canonical_report_link(
        "http://192.168.26.174:18081/G1Q3_RCA/cases/demo/index.html"
    ) == ""


def test_publication_origin_accepts_explicit_internal_service(monkeypatch):
    monkeypatch.setenv(
        "PNC_FOXGLOVE_RENDER_HOST",
        "http://192.168.26.174:18081",
    )
    vm_path = (
        "/mnt/minieye/pdcl/department/perception_test_team/"
        "G1Q3_RCA/cases/demo/index.html"
    )
    expected = (
        "http://192.168.26.174:18081/G1Q3_RCA/cases/demo/index.html"
    )

    assert pnc_completion_notice_relay._canonical_publication_report_origin() == (
        "http://192.168.26.174:18081"
    )
    assert pnc_completion_notice_relay._canonical_publication_report_url(vm_path) == (
        expected
    )
    assert pnc_completion_notice_relay._validated_canonical_report_link(expected) == (
        expected
    )
    assert pnc_completion_notice_relay._validated_canonical_report_link(
        "http://192.168.26.174:18081/G1Q3_RCA/cases/demo/demo.viz.mcap"
    ) == ""


def test_publication_origin_accepts_explicit_https_and_preserves_report_identity(monkeypatch):
    monkeypatch.setenv(
        "PNC_FOXGLOVE_RENDER_HOST",
        "https://g1q3-rca.minieye.tech",
    )
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

    assert pnc_completion_notice_relay._canonical_publication_report_origin() == (
        "https://g1q3-rca.minieye.tech"
    )
    assert pnc_completion_notice_relay._canonical_publication_report_url(vm_path) == expected
    assert pnc_completion_notice_relay._validated_canonical_report_link(expected) == expected


def test_private_or_unbound_foxglove_link_cannot_become_artifact_path():
    viz_path = (
        "/mnt/minieye/pdcl/department/perception_test_team/"
        "G1Q3_RCA/cases/demo/demo.viz.mcap"
    )

    assert pnc_completion_notice_relay._validated_foxglove_link(
        "https://192.168.21.217/?ds=foxglove-http&ds.mcapPath=/private.viz.mcap",
        viz_path,
    ) == ""


@pytest.mark.parametrize(
    "configured",
    [
        "https://192.168.21.217/",
        "https://g1q3-rca.minieye.tech:443/",
        "https://g1q3-rca.minieye.tech/reports/",
        "https://G1Q3-RCA.MINIEYE.TECH/",
    ],
)
def test_publication_origin_rejects_noncanonical_https_shapes(monkeypatch, configured):
    monkeypatch.setenv("PNC_FOXGLOVE_RENDER_HOST", configured)

    assert pnc_completion_notice_relay._canonical_publication_report_origin() == ""


def _write_blocked_keyframe_case(tmp_path, task_id="20260627-120000-g1q3-rca-issue-intake-7029488224-real"):
    artifact_root = tmp_path / "artifacts" / task_id
    artifact_root.mkdir(parents=True, exist_ok=True)
    contract = {
        "schema_version": "g1q3_delivery_contract_v1",
        "work_item_id": "7029488224",
        "business_state": "awaiting_download",
        "presentation_state": "processing",
        "report": {"status": "need_download", "is_deliverable": False},
        "user_action": {"requires_user_input": False, "next_action": "已受理；无需发起人补数据"},
        "summary": {"l0": "已受理；无需发起人补数据"},
        "artifacts": {
            "task_root_vm": "/mnt/tmp/g1q3_rca_issue_intake_7029488224_real/",
            "task_root_cifs": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/g1q3_rca_issue_intake_7029488224_real/",
        },
        "verification": {"pipeline_status": "awaiting_download", "terminal_state": "running"},
    }
    pipeline = {
        "status": "blocked",
        "stage": "s45_auto_keyframe",
        "blocker": {
            "kind": "missing_signal_keyframe",
            "message": "discover_acc_speed_unstable 缺少可定位关键帧信号，自动找帧无候选",
        },
    }
    (artifact_root / "delivery_contract.json").write_text(json.dumps(contract, ensure_ascii=False), encoding="utf-8")
    (artifact_root / "pipeline_result.json").write_text(json.dumps(pipeline, ensure_ascii=False), encoding="utf-8")
    shared = tmp_path / "runtime" / "shared-state" / "tasks" / task_id
    shared.mkdir(parents=True, exist_ok=True)
    (shared / "meta.json").write_text(json.dumps({
        "state": "completed",
        "business_line": "g1q3_rca",
        "work_item_id": "7029488224",
        "artifact_root": str(artifact_root),
        "created_at": "2026-06-27T12:00:00+08:00",
        "updated_at": "2026-06-27T12:10:00+08:00",
    }, ensure_ascii=False), encoding="utf-8")
    return artifact_root, contract, pipeline


def test_need_input_reason_surfaces_blocker(tmp_path):
    token = set_hermes_home_override(tmp_path)
    task_id = "20260627-120000-g1q3-rca-issue-intake-7029488224-real"
    try:
        artifact_root, _contract, _pipeline = _write_blocked_keyframe_case(tmp_path, task_id)
        card = {
            "task_id": task_id,
            "user_state": "awaiting_user",
            "delivery": {
                "report_status": "need_keyframe",
                "human_action_kind": "need_keyframe",
                "artifact_root": str(artifact_root),
                "conclusion": "已受理；无需发起人补数据",
            },
        }
        meta = json.loads((tmp_path / "runtime" / "shared-state" / "tasks" / task_id / "meta.json").read_text(encoding="utf-8"))
        reason = pnc_completion_notice_relay._need_input_reason(card, meta)
    finally:
        reset_hermes_home_override(token)

    assert "关键帧" in reason or "信号" in reason
    assert "discover_acc_speed_unstable" in reason
    assert "无需发起人补数据" not in reason
    assert "数据已就位，无需重传" in reason


def test_no_completed_then_need_input_flap(tmp_path):
    token = set_hermes_home_override(tmp_path)
    task_id = "20260627-120000-g1q3-rca-issue-intake-7029488224-real"
    try:
        artifact_root, contract, _pipeline = _write_blocked_keyframe_case(tmp_path, task_id)
        body = {
            "delivery_contract": contract,
            "task_card": {
                "task_id": task_id,
                "chat_id": pnc_completion_notice_relay.DEFAULT_CHAT_IDS[1],
                "thread_id": "topic:om_g1q3",
                "message_id": "om_g1q3",
                "user_state": "done",
                "delivery": {"artifact_root": str(artifact_root), "report_status": "need_download"},
                "milestones": [],
            },
            "completion_notice": {
                "state": "completed",
                "text": "结论：worker finished",
                "completion_delivery": {"required": True, "must_carry": ["fixed_state", "html_url"]},
            },
            "artifacts": [str(artifact_root / "delivery_contract.json")],
        }
        out = pnc_completion_notice_relay.enrich_g1q3_task_card_delivery(task_id, body)
        text = pnc_completion_notice_relay._text_with_completion_must_carry(
            out["completion_notice"]["text"], out, out["completion_notice"]
        )
    finally:
        reset_hermes_home_override(token)

    card = out["task_card"]
    delivery = card["delivery"]
    assert card["user_state"] != "done"
    assert card["user_state"] == "awaiting_user"
    assert delivery["presentation_state"] == "blocked"
    assert delivery["report_status"] == "need_keyframe"
    assert delivery.get("artifact_path") != str(artifact_root / "delivery_contract.json")
    assert "fixed_state：blocked/need_keyframe" in text
    assert "delivery_contract.json" not in text
    assert "本次未生成可交付报告" in text
