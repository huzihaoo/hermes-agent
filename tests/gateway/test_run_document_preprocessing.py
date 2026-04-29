"""Tests for gateway inbound document preprocessing."""

import asyncio
from types import SimpleNamespace

from gateway.config import GatewayConfig, Platform
from gateway.platforms.base import MessageEvent, MessageType
from gateway.session import SessionSource
from gateway.run import GatewayRunner


def test_prepare_message_for_agent_injects_document_saved_path():
    runner = object.__new__(GatewayRunner)
    runner.config = GatewayConfig()
    runner.adapters = {}
    runner._model = "test-model"
    runner._base_url = ""

    source = SessionSource(
        platform=Platform.FEISHU,
        user_id="ou_1",
        user_name="Alice",
        chat_id="oc_1",
        chat_type="group",
        thread_id="topic:om_1",
    )
    event = MessageEvent(
        text="please parse it",
        message_type=MessageType.DOCUMENT,
        source=source,
        media_urls=["/tmp/doc_123_bus.dbc"],
        media_types=["application/octet-stream"],
    )

    message_text = asyncio.run(
        GatewayRunner._prepare_inbound_message_text(
            runner,
            event=event,
            source=source,
            history=[],
        )
    )

    assert "The user sent a document: 'bus.dbc'" in message_text
    assert "The file is saved at: /tmp/doc_123_bus.dbc" in message_text
    assert "please parse it" in message_text
