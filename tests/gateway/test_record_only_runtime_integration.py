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


@pytest.mark.parametrize(
    "misspelled_mode",
    ["record_only", "recordonly", "record-onyl", "record only"],
)
def test_misspelled_outbound_mode_fails_closed_before_transport_side_effects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    misspelled_mode: str,
) -> None:
    record_root = tmp_path / "records"
    monkeypatch.setenv("HERMES_OUTBOUND_MODE", misspelled_mode)
    monkeypatch.setenv("HERMES_OUTBOUND_RECORD_ROOT", str(record_root))
    sys.modules.pop("gateway.record_only.transport", None)
    sys.modules.pop("gateway.record_only.census_binding", None)

    with pytest.raises(
        runtime.RecordOnlyConfigurationError,
        match="unsupported HERMES_OUTBOUND_MODE",
    ):
        runtime.get_record_only_transport("test.misspelled-mode")

    assert not record_root.exists()
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


def test_record_key_reader_rejects_symlink_and_hardlink(tmp_path: Path) -> None:
    key = tmp_path / "record.key"
    key.write_text("ab" * 32 + "\n", encoding="ascii")
    key.chmod(0o600)
    symlink = tmp_path / "record-link.key"
    symlink.symlink_to(key)
    with pytest.raises(runtime.RecordOnlyConfigurationError):
        runtime._read_key_file(symlink)

    hardlink = tmp_path / "record-hardlink.key"
    hardlink.hardlink_to(key)
    with pytest.raises(runtime.RecordOnlyConfigurationError, match="single-link"):
        runtime._read_key_file(key)


def test_record_key_reader_rejects_path_swap_at_open(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    key = tmp_path / "record.key"
    replacement = tmp_path / "replacement.key"
    key.write_text("ab" * 32 + "\n", encoding="ascii")
    replacement.write_text("cd" * 32 + "\n", encoding="ascii")
    key.chmod(0o600)
    replacement.chmod(0o600)
    real_open = runtime.os.open
    swapped = False

    def swap_then_open(path, flags, *args, **kwargs):
        nonlocal swapped
        if not swapped and Path(path) == key:
            swapped = True
            key.rename(tmp_path / "original.key")
            replacement.rename(key)
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(runtime.os, "open", swap_then_open)
    with pytest.raises(runtime.RecordOnlyConfigurationError):
        runtime._read_key_file(key)


def test_record_key_reader_rejects_in_read_mutation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    key = tmp_path / "record.key"
    key.write_text("ab" * 128, encoding="ascii")
    key.chmod(0o600)
    real_read = runtime.os.read
    mutated = False

    def read_then_mutate(fd, size):
        nonlocal mutated
        chunk = real_read(fd, size)
        if chunk and not mutated:
            mutated = True
            key.write_text("cd" * 128, encoding="ascii")
            key.chmod(0o600)
        return chunk

    monkeypatch.setattr(runtime.os, "read", read_then_mutate)
    with pytest.raises(runtime.RecordOnlyConfigurationError, match="changed while being read"):
        runtime._read_key_file(key)


def test_live_mode_never_reads_record_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HERMES_OUTBOUND_MODE", "live")

    def bomb(_path):
        raise AssertionError("live mode read the record-only key")

    monkeypatch.setattr(runtime, "_read_key_file", bomb)
    assert runtime.get_record_only_transport("test.live-key-lazy") is None


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
async def test_feishu_plugin_all_ten_operation_classes_stop_before_real_io(record_only_env: Path) -> None:
    from gateway.platforms.feishu import FeishuAdapter

    class BombClient:
        def __getattribute__(self, name: str):
            raise AssertionError(f"real Feishu client was touched: {name}")

    adapter = FeishuAdapter(PlatformConfig(enabled=True))
    adapter._client = BombClient()

    async def bomb_download(*_args, **_kwargs):
        raise AssertionError("remote media download was attempted")

    adapter._download_remote_image = bomb_download

    assert (await adapter.send("oc_fixture", "plain send")).success is True
    assert (await adapter.send("oc_fixture", "reply", reply_to="om_source")).success is True
    assert (await adapter.edit_message("oc_fixture", "om_edit", "edited")).success is True
    assert (await adapter.send_exec_approval("oc_fixture", "echo safe", "session-fixture")).success is True
    card_reply = await adapter._send_raw_message(
        chat_id="oc_fixture",
        msg_type="interactive",
        payload=json.dumps({"elements": [{"tag": "markdown", "content": "card reply"}]}),
        reply_to="om_card_source",
        metadata=None,
    )
    assert card_reply.success is True
    await adapter._update_approval_card_to_expired("om_card_update", "oc_fixture")
    assert (
        await adapter.send_image("oc_fixture", "https://example.invalid/image.png")
    ).success is True
    assert (
        await adapter.send_document(
            "oc_fixture",
            "/definitely/not/read/document.txt",
            reply_to="om_file_source",
        )
    ).success is True
    reaction_id = await adapter._add_reaction("om_reaction", "Typing")
    assert reaction_id is not None
    assert await adapter._remove_reaction("om_reaction", reaction_id) is True

    transport = runtime.get_record_only_transport("gateway.feishu.adapter")
    assert transport is not None
    rows = transport.read_all()
    assert [row["operation"] for row in rows] == [
        "text_send",
        "text_reply",
        "text_update",
        "card_send",
        "card_reply",
        "card_update",
        "file_send",
        "file_reply",
        "reaction_add",
        "reaction_remove",
    ]
    assert all(row["external_delivery_attempted"] is False for row in rows)


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
