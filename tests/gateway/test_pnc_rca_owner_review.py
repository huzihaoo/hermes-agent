import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from gateway import run as gateway_run
from gateway.config import Platform
from gateway.platforms.base import MessageEvent, MessageType
from gateway.pnc_group_binding import G1Q3_RCA_GROUP_ID
from gateway.pnc_rca_owner_review import handle_owner_review_message
from gateway.session import SessionSource


def make_event(text: str, *, user_id="ou_owner", user_name="Owner A", chat_id=G1Q3_RCA_GROUP_ID) -> MessageEvent:
    return MessageEvent(
        text=text,
        message_type=MessageType.TEXT,
        source=SessionSource(
            platform=Platform.FEISHU,
            user_id=user_id,
            user_name=user_name,
            chat_id=chat_id,
            chat_name="G1Q3 RCA",
            chat_type="group",
        ),
        message_id="om_1",
    )


def review_dir(home):
    return home / "pnc_agent" / "reviews" / "g1q3_rca"


def ledger(home):
    return json.loads((review_dir(home) / "ledger.json").read_text(encoding="utf-8"))


def business_state_sidecar(home, issue_id="123"):
    return json.loads((review_dir(home) / "business-states" / f"G1Q3-{issue_id}.business-state.yaml").read_text(encoding="utf-8"))


def receipts(home):
    files = list(review_dir(home).glob("owner_review-*.jsonl"))
    assert len(files) == 1
    return [json.loads(line) for line in files[0].read_text(encoding="utf-8").splitlines()]


def test_unrelated_message_not_handled(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_G1Q3_REVIEW_OWNER_USER_IDS", "ou_owner")

    result = handle_owner_review_message(make_event("帮我看 G1Q3-123"), hermes_home=tmp_path)

    assert result.handled is False
    assert not review_dir(tmp_path).exists()


def test_non_bound_chat_is_ignored(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_G1Q3_REVIEW_OWNER_USER_IDS", "ou_owner")

    result = handle_owner_review_message(make_event("rca 通过 123", chat_id="oc_other"), hermes_home=tmp_path)

    assert result.handled is False
    assert result.response is None
    assert not review_dir(tmp_path).exists()


def test_explicit_rca_disabled_is_handled_without_files(tmp_path, monkeypatch):
    monkeypatch.delenv("HERMES_G1Q3_REVIEW_OWNERS", raising=False)
    monkeypatch.delenv("HERMES_G1Q3_REVIEW_OWNER_USER_IDS", raising=False)

    result = handle_owner_review_message(make_event("rca 通过 123"), hermes_home=tmp_path)

    assert result.handled is True
    assert result.response == (
        "owner review 未启用,请配置 HERMES_G1Q3_REVIEW_OWNER_USER_IDS"
    )
    assert not review_dir(tmp_path).exists()


def test_non_owner_rejected_without_ledger(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_G1Q3_REVIEW_OWNER_USER_IDS", "ou_owner")

    result = handle_owner_review_message(make_event("rca 通过 123", user_id="ou_other", user_name="Other"), hermes_home=tmp_path)

    assert result.handled is True
    assert "不在 G1Q3 RCA owner review allowlist" in result.response
    assert not review_dir(tmp_path).exists()


def test_typed_user_id_allowlist_is_accepted(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_G1Q3_REVIEW_OWNER_USER_IDS", "ou_owner")
    monkeypatch.delenv("HERMES_G1Q3_REVIEW_OWNERS", raising=False)

    result = handle_owner_review_message(make_event("RCA 通过 123", user_name="Renamed Owner"), hermes_home=tmp_path)

    assert result.handled is True
    assert "issue 123 / 通过 / owner Renamed Owner" in result.response
    assert ledger(tmp_path)["issues"]["123"]["current"]["owner_id"] == "ou_owner"


def test_bare_user_id_in_legacy_owner_env_is_not_accepted(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_G1Q3_REVIEW_OWNERS", "ou_owner")
    monkeypatch.delenv("HERMES_G1Q3_REVIEW_OWNER_USER_IDS", raising=False)

    result = handle_owner_review_message(make_event("rca 通过 123", user_name="Different Display Name"), hermes_home=tmp_path)

    assert result.handled is True
    assert "未启用" in result.response
    assert not review_dir(tmp_path).exists()


def test_owner_name_allowlist_is_not_authorization(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_G1Q3_REVIEW_OWNERS", "Owner A")
    monkeypatch.delenv("HERMES_G1Q3_REVIEW_OWNER_USER_IDS", raising=False)

    result = handle_owner_review_message(make_event("RCA 通过 123"), hermes_home=tmp_path)

    assert result.handled is True
    assert "未启用" in result.response
    assert not review_dir(tmp_path).exists()


def test_matching_display_name_cannot_override_stable_user_id(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("HERMES_G1Q3_REVIEW_OWNER_USER_IDS", "ou_real_owner")
    monkeypatch.setenv("HERMES_G1Q3_REVIEW_OWNERS", "Owner A")

    result = handle_owner_review_message(
        make_event("RCA 通过 123", user_id="ou_other", user_name="Owner A"),
        hermes_home=tmp_path,
    )

    assert "不在 G1Q3 RCA owner review allowlist" in result.response
    assert not review_dir(tmp_path).exists()


def test_malformed_command_returns_usage_after_feature_enabled(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_G1Q3_REVIEW_OWNER_USER_IDS", "ou_owner")

    result = handle_owner_review_message(make_event("rca 通过 abc"), hermes_home=tmp_path)

    assert result.handled is True
    assert "格式：rca" in result.response
    assert not review_dir(tmp_path).exists()


def test_reject_and_need_evidence_require_reason(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_G1Q3_REVIEW_OWNER_USER_IDS", "ou_owner")

    reject = handle_owner_review_message(make_event("rca 驳回 123"), hermes_home=tmp_path)
    need = handle_owner_review_message(make_event("rca 补证据 123 覆盖"), hermes_home=tmp_path)

    assert reject.handled is True and "必须填写理由" in reject.response
    assert need.handled is True and "必须填写理由" in need.response
    assert not review_dir(tmp_path).exists()


def test_approve_writes_ledger_and_receipt_with_latency_unavailable(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_G1Q3_REVIEW_OWNER_USER_IDS", "ou_owner")

    result = handle_owner_review_message(make_event("rca 通过 123"), hermes_home=tmp_path)

    assert result.handled is True
    assert "耗时不可算" in result.response
    current = ledger(tmp_path)["issues"]["123"]["current"]
    assert current["schema_version"] == "g1q3_rca_owner_review_v1"
    assert current["verdict"] == "approved"
    assert current["report_generated_at"] is None
    assert current["latency_seconds"] is None
    receipt = receipts(tmp_path)[0]
    assert receipt["latency_unavailable"] is True
    current_sidecar = business_state_sidecar(tmp_path)
    assert current_sidecar["business_state"] == "fix_verification"
    assert current_sidecar["owner_review"]["verdict"] == "approved"
    assert current_sidecar["transitions"][-1]["from"] == "rca_review"


def test_report_generated_at_prefers_rca_execution_artifact(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_G1Q3_REVIEW_OWNER_USER_IDS", "ou_owner")
    sidecar_dir = tmp_path / "task-state"
    sidecar_dir.mkdir()
    (sidecar_dir / "task-1.json").write_text(json.dumps({
        "issue_id": "123",
        "rca_execution_result": {
            "work_item_id": "123",
            "artifacts": {"report_generated_at": "2026-06-12T00:00:00+00:00"},
        },
        "completion_notice": {"generated_at": "2026-06-11T00:00:00+00:00"},
    }), encoding="utf-8")

    result = handle_owner_review_message(make_event("rca 通过 123"), hermes_home=tmp_path)

    assert "耗时不可算" not in result.response
    current = ledger(tmp_path)["issues"]["123"]["current"]
    assert current["report_generated_at"] == "2026-06-12T00:00:00+00:00"
    assert "rca_execution_result.artifacts.report_generated_at" in current["report_generated_at_source"]
    assert isinstance(current["latency_seconds"], int)
    assert receipts(tmp_path)[0]["latency_unavailable"] is False


def test_completion_notice_generated_at_is_fallback(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_G1Q3_REVIEW_OWNER_USER_IDS", "ou_owner")
    sidecar_dir = tmp_path / "task-state"
    sidecar_dir.mkdir()
    (sidecar_dir / "task-1.json").write_text(json.dumps({
        "work_item_id": "G1Q3-123",
        "completion_notice": {"generated_at": "2026-06-12T00:00:00+00:00"},
    }), encoding="utf-8")

    handle_owner_review_message(make_event("rca 补证据 123 需要补充日志"), hermes_home=tmp_path)

    current = ledger(tmp_path)["issues"]["123"]["current"]
    assert current["verdict"] == "need_evidence"
    assert current["reason"] == "需要补充日志"
    assert current["report_generated_at"] == "2026-06-12T00:00:00+00:00"
    assert "completion_notice.generated_at" in current["report_generated_at_source"]


def test_idempotent_current_requires_override_and_override_moves_history(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_G1Q3_REVIEW_OWNER_USER_IDS", "ou_owner")

    first = handle_owner_review_message(make_event("rca 通过 123"), hermes_home=tmp_path)
    second = handle_owner_review_message(make_event("rca 驳回 123 证据不足"), hermes_home=tmp_path)
    third = handle_owner_review_message(make_event("rca 驳回 123 证据不足 覆盖"), hermes_home=tmp_path)

    assert "已记录" in first.response
    assert "已有 current 结论" in second.response
    data = ledger(tmp_path)["issues"]["123"]
    assert data["current"]["verdict"] == "rejected"
    assert data["current"]["override"] is True
    assert data["history"][0]["verdict"] == "approved"
    assert "issue 123 / 驳回" in third.response
    assert len(receipts(tmp_path)) == 2
    current_sidecar = business_state_sidecar(tmp_path)
    assert current_sidecar["business_state"] == "need_input"
    assert current_sidecar["owner_review"]["verdict"] == "rejected"


def make_runner():
    runner = object.__new__(gateway_run.GatewayRunner)
    runner.adapters = {}
    runner.config = SimpleNamespace()
    runner.session_store = SimpleNamespace()
    runner.pairing_store = SimpleNamespace()
    runner._running_agents = {}
    runner._running_agents_ts = {}
    return runner


@pytest.mark.asyncio
async def test_gateway_connection_consumes_disabled_rca_before_agent(monkeypatch, tmp_path):
    runner = make_runner()
    event = make_event("rca 通过 123")
    monkeypatch.delenv("HERMES_G1Q3_REVIEW_OWNERS", raising=False)
    monkeypatch.delenv("HERMES_G1Q3_REVIEW_OWNER_USER_IDS", raising=False)
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)

    async def _raise(*args, **kwargs):
        raise AssertionError("owner review command must not reach agent")

    monkeypatch.setattr(gateway_run.GatewayRunner, "_handle_message_with_agent", _raise, raising=False)

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert response == (
        "owner review 未启用,请配置 HERMES_G1Q3_REVIEW_OWNER_USER_IDS"
    )


@pytest.mark.asyncio
async def test_gateway_connection_unrelated_message_falls_through(monkeypatch, tmp_path):
    runner = make_runner()
    event = make_event("普通消息", user_id="ou_user", user_name="User", chat_id="oc_other")
    monkeypatch.setenv("HERMES_G1Q3_REVIEW_OWNER_USER_IDS", "ou_owner")
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)
    monkeypatch.setattr(gateway_run.GatewayRunner, "_session_key_for_source", lambda self, source: "feishu:oc_g1q3")

    async def _agent(self, event, source, quick_key, run_generation):
        return f"agent:{event.text}:{quick_key}"

    monkeypatch.setattr(gateway_run.GatewayRunner, "_handle_message_with_agent", _agent, raising=False)
    monkeypatch.setattr(gateway_run.GatewayRunner, "_begin_session_run", lambda self, key: 1, raising=False)
    monkeypatch.setattr(gateway_run.GatewayRunner, "_release_running_agent_state", lambda self, key: None, raising=False)

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert response == "agent:普通消息:feishu:oc_g1q3"


def test_owner_review_accepts_all_business_test_group(monkeypatch, tmp_path):
    from gateway.pnc_rca_owner_review import handle_owner_review_message

    monkeypatch.setenv("HERMES_G1Q3_OWNER_REVIEW_ENABLED", "1")
    monkeypatch.setenv("HERMES_G1Q3_REVIEW_OWNER_USER_IDS", "ou_owner")
    event = make_event(
        "rca 通过 7013527412 测试群验收",
        chat_id="oc_16614f4ba25b8c88b69c0b8e9ebc2fb5",
    )

    result = handle_owner_review_message(event, hermes_home=tmp_path)

    assert result is not None
    assert result.handled is True
    assert "已记录" in result.response
