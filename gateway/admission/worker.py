"""Async queue worker — concurrent processing across domain_ids within each domain.

Architecture:
  - 1 dispatcher per domain (user, group, vm)
  - Each dispatcher spawns concurrent tasks per active domain_id
  - Within a domain_id, items process serially (preserves message ordering)
  - Across domain_ids, items process in parallel (isolation)
  - max_concurrent_per_domain caps how many domain_ids run simultaneously
"""

from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable, Dict, Set

from .controller import AdmissionController
from .types import ALL_DOMAINS, ALL_LANES, Domain, QueueItem

logger = logging.getLogger(__name__)


class QueueWorker:
    """Async worker with per-domain_id concurrency and configurable limits."""

    def __init__(
        self,
        admission_controller: AdmissionController,
        process_fn: Callable[[QueueItem], Awaitable[dict]],
        max_concurrent_per_domain: int = 10,
    ):
        self._admission = admission_controller
        self._process_fn = process_fn
        if (
            not isinstance(max_concurrent_per_domain, int)
            or isinstance(max_concurrent_per_domain, bool)
            or max_concurrent_per_domain < 1
        ):
            raise ValueError("max_concurrent_per_domain must be a positive integer")
        self._running = False
        self._tasks: list[asyncio.Task] = []
        self._cleanup_interval_hours = 6
        self._max_concurrent = max_concurrent_per_domain
        # Track which domain_ids have an active processing task
        self._active_domain_ids: Dict[Domain, Set[str]] = {
            d: set() for d in ALL_DOMAINS
        }
        # Track available slots per domain without relying on private asyncio.Semaphore internals.
        self._available_domain_slots: Dict[Domain, int] = {d: 0 for d in ALL_DOMAINS}
        # In-flight tasks for graceful shutdown
        self._inflight: Set[asyncio.Task] = set()

    async def start(self) -> None:
        """Start one dispatcher per domain + cleanup loop."""
        if self._running:
            logger.warning("[worker] Already running")
            return

        self._running = True
        self._available_domain_slots = {d: self._max_concurrent for d in ALL_DOMAINS}
        logger.info(
            "[worker] Starting domain dispatchers (max_concurrent=%d)",
            self._max_concurrent,
        )

        self._tasks = [
            asyncio.create_task(self._domain_dispatcher(d))
            for d in ALL_DOMAINS
        ]
        self._tasks.append(asyncio.create_task(self._cleanup_loop()))

    async def stop(self, drain_timeout: float = 5.0) -> None:
        """Stop all workers gracefully, waiting for in-flight items."""
        if not self._running:
            return

        logger.info("[worker] Stopping domain dispatchers")
        self._running = False

        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

        # Drain in-flight processing tasks
        if self._inflight:
            logger.info("[worker] Draining %d in-flight tasks (timeout=%.1fs)",
                        len(self._inflight), drain_timeout)
            done, pending = await asyncio.wait(
                self._inflight, timeout=drain_timeout,
            )
            if pending:
                logger.warning("[worker] %d tasks still running after drain, cancelling",
                               len(pending))
                for t in pending:
                    t.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
            self._inflight.clear()

        logger.info("[worker] Stopped")

    async def _domain_dispatcher(self, domain: Domain) -> None:
        """Dispatcher for a domain. Scans for work and spawns per-domain_id tasks."""
        logger.info("[worker] Started %s dispatcher", domain)
        while self._running:
            try:
                spawned = False
                active = self._active_domain_ids[domain]
                ready_domain_ids = [
                    did for did in self._admission.queue.active_domain_ids(domain)
                    if did not in active
                ]
                ready_domain_ids.sort(
                    key=lambda did: (
                        -max(
                            (item.priority for item in self._admission.queue.list_pending(domain=domain, domain_id=did)),
                            default=-1,
                        ),
                        did,
                    )
                )
                for domain_id in ready_domain_ids:
                    if self._available_domain_slots[domain] <= 0:
                        break
                    for lane in ALL_LANES:
                        item = self._admission.dequeue_next(lane, domain=domain, domain_id=domain_id)
                        if item is None:
                            continue

                        spawned = True
                        active.add(item.domain_id)
                        self._available_domain_slots[domain] -= 1
                        task = asyncio.create_task(
                            self._process_item_with_reserved_slot(item)
                        )
                        self._inflight.add(task)
                        task.add_done_callback(self._inflight.discard)
                        break
                    else:
                        continue
                if not spawned:
                    await asyncio.sleep(0.5)

            except asyncio.CancelledError:
                logger.info("[worker] %s dispatcher cancelled", domain)
                break
            except Exception as e:
                logger.error(
                    "[worker] Unexpected error in %s dispatcher: %s",
                    domain, e, exc_info=True,
                )
                await asyncio.sleep(5)

        logger.info("[worker] Stopped %s dispatcher", domain)

    async def _process_item_with_reserved_slot(self, item: QueueItem) -> None:
        """Process an item after the dispatcher has reserved a public domain slot."""
        try:
            await self._process_item(item)
        finally:
            self._available_domain_slots[item.domain] += 1

    async def _process_item(self, item: QueueItem) -> None:
        """Process a single queue item."""
        logger.info(
            "[worker] Processing %s from %s/%s:%s (user=%s)",
            item.id, item.domain, item.domain_id, item.lane, item.user_id,
        )

        start_time = asyncio.get_event_loop().time()
        try:
            result = await self._process_fn(item)
            elapsed = asyncio.get_event_loop().time() - start_time
            result["processing_time_seconds"] = round(elapsed, 2)
            self._admission.complete(item.id, result)
            logger.info("[worker] Completed %s in %.2fs", item.id, elapsed)
        except Exception as e:
            elapsed = asyncio.get_event_loop().time() - start_time
            logger.error(
                "[worker] Failed %s after %.2fs: %s",
                item.id, elapsed, e, exc_info=True,
            )
            self._admission.fail(item.id, str(e))
        finally:
            self._active_domain_ids[item.domain].discard(item.domain_id)

    async def _cleanup_loop(self) -> None:
        """Periodic cleanup of old completed items."""
        logger.info("[worker] Started cleanup loop")

        while self._running:
            try:
                await asyncio.sleep(self._cleanup_interval_hours * 3600)
                if not self._running:
                    break
                removed = self._admission.queue.cleanup_old_items(max_age_hours=24)
                if removed > 0:
                    logger.info("[worker] Cleaned up %d old items", removed)
                    self._admission.queue.save()
            except asyncio.CancelledError:
                logger.info("[worker] Cleanup loop cancelled")
                break
            except Exception as e:
                logger.error("[worker] Cleanup error: %s", e, exc_info=True)
                await asyncio.sleep(3600)

        logger.info("[worker] Stopped cleanup loop")
