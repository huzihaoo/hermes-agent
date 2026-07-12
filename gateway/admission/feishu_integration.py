"""Feishu integration for admission controller.

Bridges the admission controller with the existing FeishuAdapter.
"""

import asyncio
import logging
from pathlib import Path
from typing import Optional

from gateway.admission.controller import AdmissionController
from gateway.platforms.feishu import FeishuAdapter

logger = logging.getLogger(__name__)


class FeishuAdmissionBridge:
    """Bridge between FeishuAdapter and AdmissionController.

    Intercepts incoming Feishu messages, routes them through admission control,
    and coordinates response delivery.
    """

    def __init__(
        self,
        feishu_adapter: FeishuAdapter,
        admission_controller: AdmissionController,
    ):
        self.feishu = feishu_adapter
        self.admission = admission_controller
        self._original_handler = None

    async def start(self):
        """Start the bridge by hooking into Feishu message flow."""
        # Hook into Feishu's message handler
        self._original_handler = self.feishu._handle_message
        self.feishu._handle_message = self._intercept_message
        logger.info("FeishuAdmissionBridge started")

    async def stop(self):
        """Stop the bridge and restore original handler."""
        if self._original_handler:
            self.feishu._handle_message = self._original_handler
        logger.info("FeishuAdmissionBridge stopped")

    async def _intercept_message(self, event: dict):
        """Intercept incoming Feishu message and route through admission."""
        try:
            # Extract message metadata
            message_id = event.get("message_id", "")
            chat_id = event.get("chat_id", "")
            chat_type = event.get("chat_type", "")  # "p2p" or "group"
            thread_id = event.get("thread_id", "")
            user_id = event.get("sender", {}).get("sender_id", {}).get("open_id", "")
            message_text = event.get("message", {}).get("content", "")

            # Admit the message (returns tuple: admitted, feedback, item)
            admitted, feedback, queue_item = await self.admission.admit(
                user_id=user_id,
                message=message_text,
                chat_id=chat_id,
                chat_type=chat_type,
                thread_id=thread_id,
                request_message_id=message_id,
                platform="feishu",
            )

            if admitted and queue_item:
                logger.info(
                    f"Message admitted: queue_id={queue_item.id}, "
                    f"lane={queue_item.lane}, user={user_id}"
                )
            else:
                logger.warning(f"Message rejected: {feedback}")

        except Exception as e:
            logger.error(f"Failed to intercept message: {e}", exc_info=True)
            # Fall back to original handler
            if self._original_handler:
                await self._original_handler(event)
