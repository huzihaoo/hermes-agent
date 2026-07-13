"""RCA-specific Feishu contracts adapted to the v0.18.2 plugin layout."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from gateway.config import PlatformConfig
from gateway.platforms.feishu import FeishuAdapter
from plugins.platforms.feishu.adapter import (
    FeishuMentionRef,
    _FeishuBotIdentity,
    _self_mention_is_command_directed,
)


def _adapter(tmp_path, *, extra=None):
    with patch.object(FeishuAdapter, "_load_seen_message_ids"):
        adapter = FeishuAdapter(PlatformConfig(extra=extra or {}))
    adapter._dedup_state_path = tmp_path / "feishu_seen_message_ids.json"
    adapter._api_poll_state_path = tmp_path / "feishu_api_poll_state_v1.json"
    return adapter


def _inbound_data(message_id="om_two_phase"):
    return SimpleNamespace(
        event=SimpleNamespace(
            message=SimpleNamespace(
                message_id=message_id,
                message_type="text",
                content='{"text":"hello"}',
                chat_id="oc_chat",
                chat_type="group",
                mentions=[],
            ),
            sender=SimpleNamespace(
                sender_type="user",
                sender_id=SimpleNamespace(
                    open_id="ou_sender",
                    user_id=None,
                    union_id=None,
                ),
            ),
        )
    )


def _api_item(message_id, create_time, *, chat_id="oc_g1q3"):
    return {
        "message_id": message_id,
        "msg_type": "text",
        "chat_id": chat_id,
        "create_time": str(create_time),
        "body": {"content": '{"text":"RCA"}'},
        "sender": {
            "id": "ou_user",
            "id_type": "open_id",
            "sender_type": "user",
        },
    }


def test_command_directedness_requires_the_bot_to_be_first_addressee():
    self_ref = FeishuMentionRef(
        name="PNC-Agent",
        open_id="ou_bot",
        is_self=True,
    )
    other_ref = FeishuMentionRef(name="OtherBot", open_id="ou_other")

    assert _self_mention_is_command_directed(
        "@PNC-Agent analyze @OtherBot",
        [self_ref, other_ref],
    )
    assert not _self_mention_is_command_directed(
        "@OtherBot analyze @PNC-Agent",
        [other_ref, self_ref],
    )
    assert not _self_mention_is_command_directed(
        "please ask @PNC-Agent",
        [self_ref],
    )


def test_mention_identity_uses_ids_authoritatively():
    hydrated = _FeishuBotIdentity(
        open_id="ou_bot",
        user_id="u_bot",
        name="PNC-Agent",
    )
    assert hydrated.matches(
        open_id="ou_bot",
        user_id="",
        name="Different Name",
    )
    assert not hydrated.matches(
        open_id="ou_other",
        user_id="u_other",
        name="PNC-Agent",
    )
    assert not hydrated.matches(open_id="", user_id="", name="PNC-Agent")

    name_only = _FeishuBotIdentity(name="PNC-Agent")
    assert not name_only.matches(
        open_id="ou_other",
        user_id="",
        name="PNC-Agent",
    )
    assert name_only.matches(open_id="", user_id="", name="PNC-Agent")


def test_lark_client_has_a_finite_http_timeout():
    from gateway.platforms import feishu

    built_client = object()
    builder = Mock()
    builder.app_id.return_value = builder
    builder.app_secret.return_value = builder
    builder.domain.return_value = builder
    builder.log_level.return_value = builder
    builder.timeout.return_value = builder
    builder.build.return_value = built_client
    fake_lark = SimpleNamespace(
        Client=SimpleNamespace(builder=Mock(return_value=builder)),
        LogLevel=SimpleNamespace(WARNING="warning"),
    )
    adapter = object.__new__(FeishuAdapter)
    adapter._app_id = "cli_test"
    adapter._app_secret = "secret_test"

    with patch.object(feishu, "lark", fake_lark):
        result = adapter._build_lark_client("feishu-domain")

    assert result is built_client
    builder.timeout.assert_called_once_with(12.0)


def test_reply_card_adds_only_the_canonical_issue_identity(tmp_path):
    issue_url = (
        "https://project.feishu.cn/t03o4q/issue/detail/7003183096?openScene=4"
    )
    parent = SimpleNamespace(
        msg_type="post",
        body=SimpleNamespace(
            content=json.dumps(
                {
                    "zh_cn": {
                        "content": [[
                            {"tag": "text", "text": "issue card"},
                            {"tag": "a", "text": "view", "href": issue_url},
                        ]]
                    }
                }
            )
        ),
        mentions=[],
    )
    adapter = _adapter(tmp_path)
    adapter._client = SimpleNamespace(
        im=SimpleNamespace(
            v1=SimpleNamespace(
                message=SimpleNamespace(
                    get=Mock(
                        return_value=SimpleNamespace(
                            success=lambda: True,
                            data=SimpleNamespace(items=[parent]),
                        )
                    )
                )
            )
        )
    )
    adapter._build_get_message_request = Mock(return_value=object())

    text = asyncio.run(adapter._fetch_message_text("om_parent"))

    assert text.splitlines()[-1] == (
        "https://project.feishu.cn/t03o4q/issue/detail/7003183096"
    )


@pytest.mark.asyncio
async def test_callback_failure_abandons_processing_and_redelivery_retries(tmp_path):
    adapter = _adapter(tmp_path)
    adapter._admit = Mock(return_value=None)
    adapter._process_inbound_message = AsyncMock(
        side_effect=[RuntimeError("callback failed"), True]
    )
    data = _inbound_data("om_callback_retry")

    with pytest.raises(RuntimeError, match="callback failed"):
        await adapter._handle_message_event_data(data)
    assert "om_callback_retry" not in adapter._processing_message_ids
    assert "om_callback_retry" not in adapter._seen_message_ids

    assert await adapter._handle_message_event_data(data) is True
    assert adapter._process_inbound_message.await_count == 2
    assert "om_callback_retry" in adapter._seen_message_ids


@pytest.mark.asyncio
async def test_durable_worker_disposition_keeps_inbox_processing(tmp_path):
    adapter = _adapter(tmp_path)
    adapter._admit = Mock(return_value=None)
    adapter._process_inbound_message = AsyncMock(return_value=False)
    message_id = "om_durable_owner"

    assert await adapter._handle_message_event_data(_inbound_data(message_id)) is False
    assert message_id in adapter._processing_message_ids
    assert message_id not in adapter._seen_message_ids

    adapter._complete_message_processing(message_id)
    assert message_id not in adapter._processing_message_ids
    assert message_id in adapter._seen_message_ids


def test_processing_entry_is_retryable_after_restart(tmp_path, monkeypatch):
    inbox_path = tmp_path / "feishu_seen_message_ids.json"
    inbox_path.write_text(
        json.dumps(
            {
                "schema_version": "feishu_message_inbox_v2",
                "messages": {
                    "om_crashed": {
                        "status": "processing",
                        "updated_at": 1,
                    }
                },
                "message_ids": {},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))

    adapter = FeishuAdapter(PlatformConfig())

    assert adapter._begin_message_processing("om_crashed") is True


@pytest.mark.asyncio
async def test_api_poll_callback_failure_keeps_pending_without_cursor_advance(
    tmp_path,
):
    chat_id = "oc_g1q3"
    item = _api_item("om_callback_retry", 2000, chat_id=chat_id)
    adapter = _adapter(
        tmp_path,
        extra={"api_poll_chat_ids": [chat_id]},
    )
    adapter._api_poll_started_at_ms = 1_500_000
    adapter._api_poll_start_cursor_ms = 1_500_000
    adapter._fetch_recent_chat_messages_via_api = Mock(
        side_effect=[[item], [item]]
    )
    adapter._handle_message_event_data = AsyncMock(
        side_effect=[RuntimeError("callback failed"), True]
    )

    with pytest.raises(RuntimeError, match="callback failed"):
        await adapter._poll_api_chat_once(chat_id)
    assert "om_callback_retry" not in adapter._api_poll_seen_message_ids
    assert chat_id not in adapter._api_poll_last_seen_create_time_ms
    assert adapter._api_poll_pending_items[chat_id][0]["message_id"] == (
        "om_callback_retry"
    )

    await adapter._poll_api_chat_once(chat_id)
    assert adapter._handle_message_event_data.await_count == 2
    assert "om_callback_retry" in adapter._api_poll_seen_message_ids
    assert adapter._api_poll_last_seen_create_time_ms[chat_id] == 2_000_000
    assert chat_id not in adapter._api_poll_pending_items


@pytest.mark.asyncio
async def test_api_poll_initial_baseline_replays_only_post_start_messages(tmp_path):
    chat_id = "oc_g1q3"
    adapter = _adapter(
        tmp_path,
        extra={"api_poll_chat_ids": [chat_id]},
    )
    adapter._api_poll_started_at_ms = 1_500_000
    adapter._api_poll_start_cursor_ms = 1_500_000
    adapter._fetch_recent_chat_messages_via_api = Mock(
        return_value=[
            _api_item("om_new_at_boot", 2000, chat_id=chat_id),
            _api_item("om_old_history", 1000, chat_id=chat_id),
        ]
    )
    handled = []

    async def handle(data):
        handled.append(data.event.message.message_id)
        return True

    adapter._handle_message_event_data = handle

    await adapter._poll_api_chat_once(chat_id)

    assert handled == ["om_new_at_boot"]
    assert "om_new_at_boot" in adapter._api_poll_seen_message_ids
    assert "om_old_history" in adapter._api_poll_seen_message_ids


def test_api_poll_event_preserves_thread_identity_and_ingress_source(tmp_path):
    adapter = _adapter(tmp_path)
    data = adapter._api_message_item_to_event_data(
        {
            **_api_item("om_msg", 123, chat_id="oc_chat"),
            "parent_id": "om_parent",
            "root_id": "om_root",
            "mentions": [
                {
                    "id": "ou_bot",
                    "id_type": "open_id",
                    "key": "@_user_1",
                    "name": "Bot",
                }
            ],
        }
    )

    assert data.event.message.parent_id == "om_parent"
    assert data.event.message.root_id == "om_root"
    assert data.event.message.mentions[0].id.open_id == "ou_bot"
    assert data._hermes_ingress_source == "api_poll"
