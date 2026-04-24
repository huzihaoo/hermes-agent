"""Async queue worker for processing admission queue items."""

from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable

from .controller import AdmissionController
from .types import Lane, QueueItem

logger = logging.getLogger(__name__)


class QueueWorker:
    """Async worker that processes items from admission queue lanes."""

    def __init__(
        self,
        admission_controller: AdmissionController,
        process_fn: Callable[[QueueItem], Awaitable[dict]],
    ):
        """Initialize queue worker.

        Args:
            admission_controller: The admission controller managing the queue
            process_fn: Async function to process each queue item
        """
        self._admission = admission_controller
        self._process_fn = process_fn
        self._running = False
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        """Start worker loops for all lanes."""
        if self._running:
            logger.warning("[worker] Already running")
            return

        self._running = True
        logger.info("[worker] Starting queue workers for all lanes")

        # Start one worker per lane
        self._tasks = [
            asyncio.create_task(self._worker_loop("fast")),
            asyncio.create_task(self._worker_loop("standard")),
            asyncio.create_task(self._worker_loop("heavy")),
        ]

    async def stop(self) -> None:
        """Stop all worker loops gracefully."""
        if not self._running:
            return

        logger.info("[worker] Stopping queue workers")
        self._running = False

        for task in self._tasks:
            task.cancel()

        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def _worker_loop(self, lane: Lane) -> None:
        """Worker loop for a specific lane.

        Continuously dequeues and processes items from the given lane.
        """
        logger.info(f"[worker] Started {lane} lane worker")

        while self._running:
            try:
                # Try to dequeue next item
                item = self._admission.dequeue_next(lane)

                if item:
                    logger.info(f"[worker] Processing {item.id} from {lane} lane (user={item.user_id})")
                    
                    try:
                        # Process the item
                        result = await self._process_fn(item)
                        
                        # Mark as completed
                        self._admission.complete(item.id, result)
                        logger.info(f"[worker] Completed {item.id}")
                        
                    except Exception as e:
                        # Mark as failed
                        logger.error(f"[worker] Failed to process {item.id}: {e}", exc_info=True)
                        self._admission.fail(item.id, str(e))
                else:
                    # No items in queue, wait before checking again
                    await asyncio.sleep(1)

            except asyncio.CancelledError:
                logger.info(f"[worker] {lane} lane worker cancelled")
                break
            except Exception as e:
                logger.error(f"[worker] Unexpected error in {lane} lane: {e}", exc_info=True)
                await asyncio.sleep(5)  # Back off on errors

        logger.info(f"[worker] Stopped {lane} lane worker")
