"""Admission controller — orchestrates permission, queue, audit, rate-limit, and retry."""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Tuple

from .alerts import AlertManager, ErrorRateAlert, QueueDepthAlert
from .audit import AuditEvent, log_audit
from .queue import AdmissionQueue
from .types import (
    ALL_DOMAINS, ALL_LANES, DEFAULT_MAX_RETRIES, DEFAULT_RETRY_BASE_DELAY,
    Domain, Lane, QueueItem,
)

logger = logging.getLogger(__name__)

_MAX_DURABLE_EVENT_CONTEXT_BYTES = 128 * 1024

# Priority by role
_ROLE_PRIORITY = {
    "owner": 100,
    "admin": 50,
    "senior": 30,
    "member": 10,
}


def _resolve_role(user_id: str) -> str:
    """Resolve user role from config. Falls back to 'member'."""
    try:
        from tools.permission_policy import get_user_role_by_id
        return get_user_role_by_id(user_id)
    except Exception:
        return "member"


def _classify_lane(message: str) -> Lane:
    """Simple heuristic to classify a message into a queue lane.

    - fast:     short greetings / tiny questions
    - heavy:    coding tasks, VM delegation, long-running work
    - standard: everything else
    """
    msg = message.strip().lower()
    length = len(msg)

    heavy_keywords = [
        "coding", "代码", "写代码", "编程", "重构", "refactor",
        "vm", "虚拟机", "部署", "deploy", "build", "编译",
        "长程", "long-running", "批量", "batch",
    ]
    for kw in heavy_keywords:
        if kw in msg:
            return "heavy"

    if length <= 8:
        return "fast"

    return "standard"


def _classify_domain(
    chat_type: str | None = None,
    platform: str | None = None,
    vm_id: str | None = None,
) -> Domain:
    """Route to a domain based on source signals.

    Rules:
        - vm_id present or platform == "vm"  → vm
        - chat_type == "group"               → group
        - everything else                    → user
    """
    if vm_id or platform == "vm":
        return "vm"
    if chat_type == "group":
        return "group"
    return "user"


def _resolve_domain_id(
    domain: Domain,
    user_id: str,
    chat_id: str | None = None,
    vm_id: str | None = None,
) -> str:
    """Derive the domain_id that scopes this queue item."""
    if domain == "group":
        return chat_id or user_id
    if domain == "vm":
        return vm_id or chat_id or user_id
    return user_id


def deterministic_transport_item_id(
    platform: str,
    request_message_id: str,
) -> str:
    """Return the stable queue ID for a transport message."""
    return str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"hermes-admission-v1:{platform}:{request_message_id}",
        )
    )


class AdmissionController:
    """Coordinates permission checks, queue admission, audit, rate-limit, and retry."""

    def __init__(
        self,
        db_path: Path | None = None,
        audit_dir: Path | None = None,
        rate_limit_per_user: int = 20,
        rate_limit_window_seconds: int = 60,
    ):
        self._db_path = db_path or (Path.home() / ".hermes" / "admission" / "queue.db")
        self._audit_dir = audit_dir or (Path.home() / ".hermes" / "audit")

        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._audit_dir.mkdir(parents=True, exist_ok=True)

        self.queue = AdmissionQueue(db_path=self._db_path)
        try:
            self.queue.load()
            if self.queue.persistence_healthy:
                logger.info("[admission] Loaded queue from %s", self._db_path)
            else:
                logger.error(
                    "[admission] Queue persistence is unhealthy after load: %s",
                    self.queue.persistence_errors,
                )
        except Exception as exc:
            logger.warning("[admission] Failed to load persisted queue: %s", exc)

        self._metrics = {
            "total_admitted": 0,
            "total_rejected": 0,
            "total_completed": 0,
            "total_failed": 0,
            "total_retried": 0,
            "total_dead": 0,
            "total_queued": 0,
        }
        self._metrics_lock = threading.Lock()
        # Restore persisted metrics
        try:
            from .persistence import load_metrics
            saved = load_metrics(self._db_path)
            for k, v in saved.items():
                if k in self._metrics:
                    self._metrics[k] = v
        except Exception as exc:
            logger.warning("[admission] Failed to load persisted metrics: %s", exc)

        self._depth_warning_threshold = 10
        self._depth_critical_threshold = 50

        # Alert system
        self._alert_manager = AlertManager(cooldown_seconds=300)
        self._alert_manager.register(QueueDepthAlert(
            warning=self._depth_warning_threshold,
            critical=self._depth_critical_threshold,
        ))
        self._alert_manager.register(ErrorRateAlert(
            threshold=0.2, critical_threshold=0.5, window_seconds=300,
        ))

        # Rate limiting: sliding window per user_id
        self._rate_limit = rate_limit_per_user
        self._rate_window = rate_limit_window_seconds
        self._user_timestamps: dict[str, list[float]] = defaultdict(list)

        logger.info("[admission] Controller initialized (db=%s, audit=%s)",
                     self._db_path, self._audit_dir)

    def validate_config(self) -> tuple[bool, list[str]]:
        """Validate admission control configuration."""
        errors = []
        try:
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            test_file = self._db_path.parent / ".write_test"
            test_file.touch()
            test_file.unlink()
        except Exception as e:
            errors.append(f"Database path not writable: {e}")

        try:
            self._audit_dir.mkdir(parents=True, exist_ok=True)
            test_file = self._audit_dir / ".write_test"
            test_file.touch()
            test_file.unlink()
        except Exception as e:
            errors.append(f"Audit directory not writable: {e}")

        try:
            from tools.permission_policy import get_user_role_by_id
            role = get_user_role_by_id("test_user")
            if role not in ("owner", "admin", "senior", "member"):
                errors.append(f"Invalid default role: {role}")
        except FileNotFoundError:
            errors.append("Permission policy config not found: ~/.hermes/config/user-roles.json")
        except Exception as e:
            errors.append(f"Permission policy error: {e}")

        return (len(errors) == 0, errors)

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------

    def _check_rate_limit(self, user_id: str) -> bool:
        """Return True if user is within rate limit, False if exceeded."""
        now = time.monotonic()
        cutoff = now - self._rate_window
        # Prune old timestamps
        timestamps = self._user_timestamps[user_id]
        self._user_timestamps[user_id] = [t for t in timestamps if t > cutoff]
        return len(self._user_timestamps[user_id]) < self._rate_limit

    def _record_request(self, user_id: str) -> None:
        """Record a request timestamp for rate limiting."""
        self._user_timestamps[user_id].append(time.monotonic())

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def admit(
        self,
        user_id: str,
        message: str,
        chat_id: str | None = None,
        chat_type: str | None = None,
        thread_id: str | None = None,
        request_message_id: str | None = None,
        platform: str | None = None,
        vm_id: str | None = None,
        event_context: dict[str, Any] | None = None,
        require_durable_persistence: bool = False,
    ) -> Tuple[bool, str, QueueItem | None]:
        """Check permission, rate-limit, and enqueue.

        Returns (admitted, feedback_message, queue_item).
        """
        # A controller that observed unreadable persisted state must not admit or
        # consume more work: a later snapshot save could otherwise delete rows
        # that were intentionally skipped during recovery.
        self.queue.require_persistence_healthy()

        if require_durable_persistence and event_context is not None:
            try:
                encoded_context = json.dumps(
                    event_context,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode("utf-8")
            except (TypeError, ValueError) as exc:
                raise ValueError("durable event_context must be JSON serializable") from exc
            if len(encoded_context) > _MAX_DURABLE_EVENT_CONTEXT_BYTES:
                raise ValueError("durable event_context exceeds 128 KiB")

        item_id = (
            deterministic_transport_item_id(platform, request_message_id)
            if platform and request_message_id
            else str(uuid.uuid4())
        )
        existing = self.queue.get_item(item_id)
        if existing and existing.status in {"queued", "processing", "completed"}:
            if (
                existing.user_id != user_id
                or existing.chat_id != chat_id
                or existing.platform != platform
            ):
                raise ValueError("admission request identity conflict")
            return (True, "请求已在队列中", existing)

        # A dead/failed deterministic item represents redelivery of the same
        # transport event. Do not rate-limit recovery of work we already accepted.
        is_redelivery = existing is not None
        if not is_redelivery and not self._check_rate_limit(user_id):
            with self._metrics_lock:
                self._metrics["total_rejected"] += 1
            self._audit("rate_limit", "", "denied", {
                "user_id": user_id,
                "limit": self._rate_limit,
                "window": self._rate_window,
            })
            return (False, f"请求过于频繁，请 {self._rate_window} 秒后再试", None)

        role = _resolve_role(user_id)
        lane = _classify_lane(message)
        priority = _ROLE_PRIORITY.get(role, 10)
        domain = _classify_domain(chat_type=chat_type, platform=platform, vm_id=vm_id)
        domain_id = _resolve_domain_id(domain, user_id, chat_id=chat_id, vm_id=vm_id)

        item = QueueItem(
            id=item_id,
            user_id=user_id,
            user_role=role,
            message=message,
            lane=lane,
            priority=priority,
            domain=domain,
            domain_id=domain_id,
            chat_id=chat_id,
            chat_type=chat_type,
            thread_id=thread_id,
            request_message_id=request_message_id,
            platform=platform,
            event_context=event_context,
        )

        if require_durable_persistence:
            self.queue.enqueue_persisted(item)
        else:
            self.queue.enqueue(item)
        with self._metrics_lock:
            self._metrics["total_admitted"] += 1
        if not is_redelivery:
            self._record_request(user_id)
        if require_durable_persistence:
            self._save_metrics_quiet()
        else:
            self._save_quiet()

        pos = self.queue.get_position(item.id)
        pos_text = f"，排队 {pos[1]} 位" if pos and pos[1] > 1 else ""

        self._check_queue_depth(lane, domain)

        self._audit("enqueue", item.id, "queued", {
            "role": role,
            "lane": lane,
            "domain": domain,
            "domain_id": domain_id,
            "priority": priority,
            "user_id": user_id,
        })

        domain_label = {"user": "私聊", "group": "群聊", "vm": "VM"}[domain]
        return (True, f"已加入 {domain_label}/{lane} 队列{pos_text}", item)

    def get_transport_item(
        self,
        platform: str,
        request_message_id: str,
    ) -> QueueItem | None:
        """Return the queue record that owns a transport message, if any."""
        if not platform or not request_message_id:
            return None
        self.queue.require_persistence_healthy()
        return self.queue.get_item(
            deterministic_transport_item_id(platform, request_message_id)
        )

    def dequeue_next(
        self,
        lane: Lane,
        domain: Domain | None = None,
        domain_id: str | None = None,
    ) -> QueueItem | None:
        item = self.queue.dequeue_persisted(
            lane,
            domain=domain,
            domain_id=domain_id,
        )
        if item:
            self._audit("dequeue", item.id, "allowed", {
                "lane": lane,
                "domain": item.domain,
                "user_id": item.user_id,
            })
        return item

    def complete(self, item_id: str, result: dict | None = None) -> None:
        def mark_completed(item: QueueItem) -> None:
            item.status = "completed"
            item.result = result
            item.completed_at = datetime.now()

        self.queue.update_persisted(item_id, mark_completed)
        self._audit("complete", item_id, "completed", result)
        with self._metrics_lock:
            self._metrics["total_completed"] += 1
        self._save_metrics_quiet()

    def fail(self, item_id: str, error: str | None = None) -> None:
        """Mark item as failed. If retries remain, re-enqueue with backoff."""
        transition: dict[str, Any] = {}

        def mark_retry_or_dead(item: QueueItem) -> None:
            if item.retry_count < item.max_retries:
                item.retry_count += 1
                item.last_error = error
                delay = DEFAULT_RETRY_BASE_DELAY * (2 ** (item.retry_count - 1))
                item.next_retry_at = datetime.now() + timedelta(seconds=delay)
                item.status = "queued"
                item.started_at = None
                item.completed_at = None
                transition.update(status="queued", delay=delay)
                return
            item.last_error = error
            item.status = "dead"
            item.result = {"error": error} if error else None
            item.completed_at = datetime.now()
            transition.update(status="dead")

        item = self.queue.update_persisted(item_id, mark_retry_or_dead)
        if item and transition.get("status") == "queued":
            delay = float(transition["delay"])
            with self._metrics_lock:
                self._metrics["total_retried"] += 1
            self._audit("retry", item_id, "queued", {
                "error": error,
                "retry_count": item.retry_count,
                "max_retries": item.max_retries,
                "next_retry_delay": delay,
                "user_id": item.user_id,
            })
            logger.info(
                "[admission] Retry %d/%d for %s (delay=%.1fs): %s",
                item.retry_count, item.max_retries, item_id, delay, error,
            )
        elif item and transition.get("status") == "dead":
            with self._metrics_lock:
                self._metrics["total_dead"] += 1
            self._audit("dead_letter", item_id, "failed", {
                "error": error,
                "retry_count": item.retry_count,
                "user_id": item.user_id,
            })
            logger.warning(
                "[admission] Dead-lettered %s after %d retries: %s",
                item_id, item.retry_count, error,
            )
        else:
            with self._metrics_lock:
                self._metrics["total_failed"] += 1
            self._audit("fail", item_id, "failed", {"error": error})

        self._save_metrics_quiet()

    # ------------------------------------------------------------------
    # Queue visibility
    # ------------------------------------------------------------------

    def get_status(self, domain: Domain | None = None,
                    domain_id: str | None = None) -> dict:
        """Get current queue status.

        Returns nested structure: domain -> domain_id -> lane -> {pending, items}.
        When domain_id is given, only that sub-queue is shown.
        """
        status: dict = {}
        domains = [domain] if domain else list(ALL_DOMAINS)

        for d in domains:
            active_dids = self.queue.active_domain_ids(d)
            if domain_id:
                active_dids = [domain_id] if domain_id in active_dids else []

            d_status: dict = {}
            for did in active_dids:
                did_status: dict = {}
                for lane in ALL_LANES:
                    items = self.queue.list_pending(lane=lane, domain=d, domain_id=did)
                    if not items:
                        continue
                    did_status[lane] = {
                        "pending": len(items),
                        "items": [
                            {
                                "id": item.id,
                                "user_id": item.user_id,
                                "user_role": item.user_role,
                                "priority": item.priority,
                                "message_preview": item.message[:50] + "..." if len(item.message) > 50 else item.message,
                                "created_at": item.created_at.isoformat(),
                            }
                            for item in items[:5]
                        ],
                    }
                if did_status:
                    d_status[did] = did_status

            if d_status:
                status[d] = d_status

        status["metrics"] = self._metrics.copy()
        status["persistence"] = self.queue.persistence_status()
        return status

    def format_status_text(self, domain: Domain | None = None,
                            domain_id: str | None = None) -> str:
        """Format queue status as human-readable text for chat display."""
        status = self.get_status(domain=domain, domain_id=domain_id)
        lines = ["📊 队列状态", ""]

        domain_emoji = {"user": "👤", "group": "👥", "vm": "🖥️"}
        lane_emoji = {"fast": "⚡", "standard": "📝", "heavy": "🔨"}

        for d in ALL_DOMAINS:
            if d not in status:
                continue
            d_data = status[d]
            d_total = sum(
                lane_info["pending"]
                for did_data in d_data.values()
                for lane_info in did_data.values()
            )
            lines.append(f"{domain_emoji[d]} {d.upper()} (共 {d_total} 排队)")

            for did, did_data in sorted(d_data.items()):
                did_total = sum(v["pending"] for v in did_data.values())
                lines.append(f"  📌 {did} ({did_total})")
                for l in ALL_LANES:
                    if l not in did_data:
                        continue
                    l_data = did_data[l]
                    lines.append(f"    {lane_emoji[l]} {l}: {l_data['pending']}")
                    for item in l_data["items"][:3]:
                        lines.append(f"      • {item['user_id']} (优先级 {item['priority']})")
            lines.append("")

        m = status["metrics"]
        lines.append("📈 统计")
        lines.append(f"  已处理: {m['total_completed']}")
        lines.append(f"  失败: {m['total_failed']}")
        lines.append(f"  重试: {m['total_retried']}")
        lines.append(f"  死信: {m['total_dead']}")
        lines.append(f"  总准入: {m['total_admitted']}")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _check_queue_depth(self, lane: Lane, domain: Domain | None = None) -> None:
        """Check queue depth and log warnings if thresholds exceeded."""
        depth = self.queue.pending_count(lane=lane, domain=domain)
        label = f"{domain}:{lane}" if domain else lane

        alerts = self._alert_manager.check_all({
            "pending_count": depth,
            **self._metrics,
        })
        for alert in alerts:
            if alert.rule_name.startswith("queue_depth"):
                if alert.level.value == "critical":
                    logger.error("[admission] %s", alert.message)
                else:
                    logger.warning("[admission] %s", alert.message)

    def get_alert_history(self, limit: int = 100) -> list:
        return self._alert_manager.get_history(limit=limit)

    def apply_template(self, template) -> None:
        """Apply a PolicyTemplate to reconfigure this controller."""
        self._rate_limit = template.rate_limit_per_user
        self._rate_window = template.rate_limit_window_seconds
        self._depth_warning_threshold = template.depth_warning
        self._depth_critical_threshold = template.depth_critical

        # Rebuild alert manager with new thresholds
        self._alert_manager = AlertManager(cooldown_seconds=template.alert_cooldown_seconds)
        self._alert_manager.register(QueueDepthAlert(
            warning=template.depth_warning,
            critical=template.depth_critical,
        ))
        self._alert_manager.register(ErrorRateAlert(
            threshold=template.error_rate_threshold,
            critical_threshold=template.error_rate_critical,
            window_seconds=300,
        ))
        logger.info("[admission] Applied template '%s'", template.name)

    def _audit(self, action: str, resource: str, result: str, metadata: dict | None = None) -> None:
        try:
            log_audit(
                AuditEvent(
                    user_id=metadata.get("user_id", "") if metadata else "",
                    action=action,
                    resource=resource,
                    result=result,
                    metadata=metadata,
                ),
                audit_dir=self._audit_dir,
            )
        except Exception as exc:
            logger.warning("[admission] Audit log failed: %s", exc)

    def _save_quiet(self) -> None:
        try:
            self.queue.save()
        except Exception as exc:
            logger.error("[admission] Queue save failed: %s", exc)
            return
        self._save_metrics_quiet()

    def _save_metrics_quiet(self) -> None:
        try:
            from .persistence import save_metrics

            save_metrics(self._db_path, self._metrics)
        except Exception as exc:
            logger.warning("[admission] Metrics save failed: %s", exc)
