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
        self._cleanup_interval_hours = 6  # Cleanup every 6 hours

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
            asyncio.create_task(self._cleanup_loop()),
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
                    
                    start_time = asyncio.get_event_loop().time()
                    try:
                        # Process the item
                        result = await self._process_fn(item)
                        
                        # Mark as completed
                        elapsed = asyncio.get_event_loop().time() - start_time
                        result["processing_time_seconds"] = round(elapsed, 2)
                        self._admission.complete(item.id, result)
                        logger.info(f"[worker] Completed {item.id} in {elapsed:.2f}s")
                        
                    except Exception as e:
                        # Mark as failed
                        elapsed = asyncio.get_event_loop().time() - start_time
                        logger.error(f"[worker] Failed to process {item.id} after {elapsed:.2f}s: {e}", exc_info=True)
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
    
    async def _cleanup_loop(self) -> None:
        """Periodic cleanup of old completed items."""
        logger.info("[worker] Started cleanup loop")
        
        while self._running:
            try:
                # Wait for cleanup interval
                await asyncio.sleep(self._cleanup_interval_hours * 3600)
                
                if not self._running:
                    break
                
                # Cleanup items older than 24 hours
                removed = self._admission.queue.cleanup_old_items(max_age_hours=24)
                if removed > 0:
                    logger.info(f"[worker] Cleaned up {removed} old items")
                    self._admission.queue.save()
                    
            except asyncio.CancelledError:
                logger.info("[worker] Cleanup loop cancelled")
                break
            except Exception as e:
                logger.error(f"[worker] Cleanup error: {e}", exc_info=True)
                await asyncio.sleep(3600)  # Retry in 1 hour on error
        
        logger.info("[worker] Stopped cleanup loop")
