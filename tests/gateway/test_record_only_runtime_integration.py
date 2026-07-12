from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from gateway.config import PlatformConfig
from gateway.record_only import runtime


CANDIDATE_ROOT = Path(__file__).resolve().parents[3]
CENSUS_ROOT = CANDIDATE_ROOT / "evidence" / "target-outbound-census"


@pytest.fixture
def record_only_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "records"
    root.mkdir(mode=0o700)
    key_file = tmp_path / "record.key"
    key_file.write_text("ab" * 32 + "\n", encoding="ascii")
    key_file.chmod(0o600)
    monkeypatch.setenv("HERMES_OUTBOUND_MODE", "record-only")
    monkeypatch.setenv("HERMES_OUTBOUND_RECORD_ROOT", str(root))
    monkeypatch.setenv("HERMES_OUTBOUND_RECORD_KEY_FILE", str(key_file))
    monkeypatch.setenv("HERMES_OUTBOUND_CENSUS_ROOT", str(CENSUS_ROOT))
    runtime._reset_for_tests()
    yield root
    runtime._reset_for_tests()


def test_live_mode_is_lazy_and_does_not_load_census(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("HERMES_OUTBOUND_MODE", raising=False)
    sys.modules.pop("gateway.record_only.transport", None)
    sys.modules.pop("gateway.record_only.census_binding", None)
    assert runtime.get_record_only_transport("test.live") is None
    assert "gateway.record_only.transport" not in sys.modules
    assert "gateway.record_only.census_binding" not in sys.modules


def test_record_only_mode_fails_closed_without_key(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "records"
    root.mkdir(mode=0o700)
    monkeypatch.setenv("HERMES_OUTBOUND_MODE", "record-only")
    monkeypatch.setenv("HERMES_OUTBOUND_RECORD_ROOT", str(root))
    monkeypatch.delenv("HERMES_OUTBOUND_RECORD_KEY_FILE", raising=False)
    runtime._reset_for_tests()
    with pytest.raises(runtime.RecordOnlyConfigurationError, match="KEY_FILE.*required"):
        runtime.get_record_only_transport("test.missing-key")


def test_card_reply_is_recorded_with_no_delivery_authority(record_only_env: Path) -> None:
    transport = runtime.get_record_only_transport("test.card")
    assert transport is not None
    result = transport.record(
        operation="card_reply",
        platform="feishu",
        destination_kind="chat",
        destination_id="oc_fixture",
        message_id="om_fixture",
        payload_type="interactive_card",
        payload={"elements": [{"tag": "markdown", "content": "中文卡片"}]},
        reply_mode="message",
        update_mode="create",
    )
    row = transport.read_all()[0]
    assert result.success is True
    assert row["operation"] == "card_reply"
    assert row["external_delivery_attempted"] is False
    assert row["candidate_execution_authorized"] is False
    assert "oc_fixture" not in json.dumps(row, ensure_ascii=False)
    assert "om_fixture" not in json.dumps(row, ensure_ascii=False)


@pytest.mark.asyncio
async def test_feishu_public_send_records_before_client(record_only_env: Path) -> None:
    from gateway.platforms.feishu import FeishuAdapter

    class BombClient:
        def __getattribute__(self, name: str):
            raise AssertionError(f"real Feishu client was touched: {name}")

    adapter = FeishuAdapter(PlatformConfig(enabled=True))
    adapter._client = BombClient()
    result = await adapter.send(
        "oc_fixture",
        "中文完成 //hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/smoke/",
        metadata={
            "thread_id": "omt_fixture",
            "task_id": "task-fixture",
            "terminal_state": "completed",
            "dedupe_key": "task-fixture:completed",
        },
    )
    assert result.success is True
    assert result.message_id and result.message_id.startswith("rec_")
    transport = runtime.get_record_only_transport("gateway.feishu.adapter")
    assert transport is not None
    row = transport.read_all()[0]
    assert row["operation"] == "text_reply"
    assert row["terminal_state"] == "completed"
    assert row["links"] == [
        "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/smoke/"
    ]


@pytest.mark.asyncio
async def test_feishu_approval_card_records_without_client(record_only_env: Path) -> None:
    from gateway.platforms.feishu import FeishuAdapter

    adapter = FeishuAdapter(PlatformConfig(enabled=True))
    adapter._client = None
    result = await adapter.send_exec_approval(
        "oc_fixture",
        "echo safe",
        "session-fixture",
        metadata={"task_id": "task-card", "dedupe_key": "approval:task-card"},
    )
    assert result.success is True
    transport = runtime.get_record_only_transport("gateway.feishu.adapter")
    assert transport is not None
    rows = transport.read_all()
    assert rows[0]["operation"] == "card_send"
    assert rows[0]["payload_type"] == "interactive_card"


@pytest.mark.asyncio
async def test_feishu_remote_image_records_before_download(record_only_env: Path) -> None:
    from gateway.platforms.feishu import FeishuAdapter

    adapter = FeishuAdapter(PlatformConfig(enabled=True))

    async def bomb_download(*args, **kwargs):
        raise AssertionError("remote image download was attempted")

    adapter._download_remote_image = bomb_download
    result = await adapter.send_image(
        "oc_fixture",
        "https://example.invalid/image.png",
        caption="图片说明",
        metadata={"task_id": "task-image"},
    )
    assert result.success is True
    transport = runtime.get_record_only_transport("gateway.feishu.adapter")
    assert transport is not None
    row = transport.read_all()[0]
    assert row["operation"] == "file_send"
    assert row["payload_type"] == "image"
    assert row["links"] == ["https://example.invalid/image.png"]


@pytest.mark.asyncio
async def test_feishu_reactions_record_before_client(record_only_env: Path) -> None:
    from gateway.platforms.feishu import FeishuAdapter

    class BombClient:
        def __getattribute__(self, name: str):
            raise AssertionError(f"real Feishu client was touched: {name}")

    adapter = FeishuAdapter(PlatformConfig(enabled=True))
    adapter._client = BombClient()
    reaction_id = await adapter._add_reaction("om_fixture", "Typing")
    assert reaction_id and reaction_id.startswith("rec_")
    assert await adapter._remove_reaction("om_fixture", reaction_id) is True

    transport = runtime.get_record_only_transport("gateway.feishu.adapter")
    assert transport is not None
    rows = transport.read_all()
    assert [row["operation"] for row in rows] == ["reaction_add", "reaction_remove"]
    assert [row["update_mode"] for row in rows] == ["create", "delete"]
    serialized = json.dumps(rows, ensure_ascii=False)
    assert "om_fixture" not in serialized
    assert reaction_id not in json.dumps(rows[1]["payload"], ensure_ascii=False)


@pytest.mark.asyncio
async def test_relay_records_before_transport(record_only_env: Path) -> None:
    from gateway.relay.adapter import RelayAdapter
    from gateway.relay.descriptor import CapabilityDescriptor

    class BombTransport:
        async def send_outbound(self, *args, **kwargs):
            raise AssertionError("real relay transport was touched")

    descriptor = CapabilityDescriptor(
        contract_version=1,
        platform="discord",
        label="Discord",
        max_message_length=2000,
        supports_draft_streaming=False,
        supports_edit=True,
        supports_threads=True,
        markdown_dialect="discord",
        len_unit="chars",
    )
    adapter = RelayAdapter(PlatformConfig(enabled=True), descriptor, transport=BombTransport())
    result = await adapter.send(
        "channel-fixture",
        "relay payload",
        metadata={"task_id": "relay-task", "dedupe_key": "relay-task:done"},
    )
    assert result.success is True
    transport = runtime.get_record_only_transport("gateway.relay.adapter")
    assert transport is not None
    row = transport.read_all()[0]
    assert row["operation"] == "text_send"
    assert row["external_delivery_attempted"] is False


def test_pnc_relay_card_update_uses_record_sink(record_only_env: Path) -> None:
    from gateway.record_only.transport import RecordOnlyRelaySender

    transport = runtime.get_record_only_transport("scripts.pnc_completion_notice_relay")
    assert transport is not None
    sender = RecordOnlyRelaySender(transport)
    result = sender.send_task_card(
        "feishu:oc_fixture:omt_fixture",
        {"elements": [{"tag": "markdown", "content": "终态卡片"}]},
        message_id="om_fixture",
    )
    assert result["success"] is True
    assert result["updated"] is False
    assert result["simulated_update_recorded"] is True
    assert result["external_delivery_verified"] is False
    row = transport.read_all()[0]
    assert row["operation"] == "card_update"
    assert row["reply_mode"] == "thread"
