"""Admission controller — orchestrates permission, queue, and audit."""

from __future__ import annotations

import logging
import uuid
from pathlib import Path
from typing import Tuple

from .audit import AuditEvent, log_audit
from .queue import AdmissionQueue
from .types import Lane, QueueItem

logger = logging.getLogger(__name__)

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

    # Heavy lane first: coding / VM / long-running keywords win even if short.
    heavy_keywords = [
        "coding", "代码", "写代码", "编程", "重构", "refactor",
        "vm", "虚拟机", "部署", "deploy", "build", "编译",
        "长程", "long-running", "批量", "batch",
    ]
    for kw in heavy_keywords:
        if kw in msg:
            return "heavy"

    # Fast lane: genuinely tiny messages only.
    if length <= 8:
        return "fast"

    return "standard"


class AdmissionController:
    """Coordinates permission checks, queue admission, and audit logging."""

    def __init__(
        self,
        db_path: Path | None = None,
        audit_dir: Path | None = None,
    ):
        self._db_path = db_path or (Path.home() / ".hermes" / "admission" / "queue.db")
        self._audit_dir = audit_dir or (Path.home() / ".hermes" / "audit")
        
        # Ensure directories exist
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._audit_dir.mkdir(parents=True, exist_ok=True)
        
        self.queue = AdmissionQueue(db_path=self._db_path)
        try:
            self.queue.load()
            logger.info("[admission] Loaded queue from %s", self._db_path)
        except Exception as exc:
            logger.warning("[admission] Failed to load persisted queue: %s", exc)
        
        # Metrics
        self._metrics = {
            "total_admitted": 0,
            "total_rejected": 0,
            "total_completed": 0,
            "total_failed": 0,
        }
        
        # Queue depth thresholds for warnings
        self._depth_warning_threshold = 10
        self._depth_critical_threshold = 50
        
        logger.info("[admission] Controller initialized (db=%s, audit=%s)", 
                   self._db_path, self._audit_dir)
    
    def validate_config(self) -> tuple[bool, list[str]]:
        """Validate admission control configuration.
        
        Returns:
            (is_valid, error_messages)
        """
        errors = []
        
        # Check database path is writable
        try:
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            test_file = self._db_path.parent / ".write_test"
            test_file.touch()
            test_file.unlink()
        except Exception as e:
            errors.append(f"Database path not writable: {e}")
        
        # Check audit directory is writable
        try:
            self._audit_dir.mkdir(parents=True, exist_ok=True)
            test_file = self._audit_dir / ".write_test"
            test_file.touch()
            test_file.unlink()
        except Exception as e:
            errors.append(f"Audit directory not writable: {e}")
        
        # Check permission policy is loadable
        try:
            from tools.permission_policy import get_user_role_by_id
            # Try to get default role
            role = get_user_role_by_id("test_user")
            if role not in ("owner", "admin", "senior", "member"):
                errors.append(f"Invalid default role: {role}")
        except FileNotFoundError:
            errors.append("Permission policy config not found: ~/.hermes/config/user-roles.json")
        except Exception as e:
            errors.append(f"Permission policy error: {e}")
        
        return (len(errors) == 0, errors)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def admit(
        self,
        user_id: str,
        message: str,
        chat_id: str | None = None,
        thread_id: str | None = None,
        platform: str | None = None,
    ) -> Tuple[bool, str, QueueItem | None]:
        """Check permission and enqueue.

        Returns (admitted, feedback_message, queue_item).
        """
        role = _resolve_role(user_id)
        lane = _classify_lane(message)
        priority = _ROLE_PRIORITY.get(role, 10)

        item = QueueItem(
            id=str(uuid.uuid4()),
            user_id=user_id,
            user_role=role,
            message=message,
            lane=lane,
            priority=priority,
            chat_id=chat_id,
            thread_id=thread_id,
            platform=platform,
        )

        self.queue.enqueue(item)
        self._save_quiet()
        self._metrics["total_admitted"] += 1

        pos = self.queue.get_position(item.id)
        pos_text = f"，排队 {pos[1]} 位" if pos and pos[1] > 1 else ""
        
        # Check queue depth and log warnings
        self._check_queue_depth(lane)

        self._audit("enqueue", item.id, "queued", {
            "role": role,
            "lane": lane,
            "priority": priority,
            "user_id": user_id,
        })

        return (True, f"已加入 {lane} 队列{pos_text}", item)

    def dequeue_next(self, lane: Lane) -> QueueItem | None:
        item = self.queue.dequeue(lane)
        if item:
            self._audit("dequeue", item.id, "allowed", {
                "lane": lane,
                "user_id": item.user_id,
            })
            self._save_quiet()
        return item

    def complete(self, item_id: str, result: dict | None = None) -> None:
        self.queue.mark_completed(item_id, result)
        self._audit("complete", item_id, "completed", result)
        self._save_quiet()
        self._metrics["total_completed"] += 1

    def fail(self, item_id: str, error: str | None = None) -> None:
        self.queue.mark_failed(item_id, error)
        self._audit("fail", item_id, "failed", {"error": error})
        self._save_quiet()
        self._metrics["total_failed"] += 1

    # ------------------------------------------------------------------
    # Queue visibility
    # ------------------------------------------------------------------

    def get_status(self) -> dict:
        """Get current queue status for all lanes.
        
        Returns:
            {
                "fast": {"pending": N, "items": [...]},
                "standard": {"pending": N, "items": [...]},
                "heavy": {"pending": N, "items": [...]},
                "metrics": {"total_admitted": N, ...}
            }
        """
        status = {}
        for lane in ["fast", "standard", "heavy"]:
            items = self.queue.list_pending(lane)
            status[lane] = {
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
                    for item in items[:5]  # Only show first 5 per lane
                ]
            }
        status["metrics"] = self._metrics.copy()
        return status
    
    def format_status_text(self) -> str:
        """Format queue status as human-readable text for chat display."""
        status = self.get_status()
        
        lines = ["📊 队列状态", ""]
        
        # Lane status
        for lane in ["fast", "standard", "heavy"]:
            lane_data = status[lane]
            emoji = {"fast": "⚡", "standard": "📝", "heavy": "🔨"}[lane]
            lines.append(f"{emoji} {lane.upper()}: {lane_data['pending']} 排队")
            
            if lane_data["items"]:
                for item in lane_data["items"][:3]:  # Show top 3
                    lines.append(f"  • {item['user_id']} (优先级 {item['priority']})")
        
        # Metrics
        lines.append("")
        lines.append("📈 统计")
        m = status["metrics"]
        lines.append(f"  已处理: {m['total_completed']}")
        lines.append(f"  失败: {m['total_failed']}")
        lines.append(f"  总准入: {m['total_admitted']}")
        
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _check_queue_depth(self, lane: Lane) -> None:
        """Check queue depth and log warnings if thresholds exceeded."""
        depth = self.queue.pending_count(lane)
        
        if depth >= self._depth_critical_threshold:
            logger.error(
                "[admission] CRITICAL: %s lane depth=%d (threshold=%d)",
                lane, depth, self._depth_critical_threshold
            )
        elif depth >= self._depth_warning_threshold:
            logger.warning(
                "[admission] WARNING: %s lane depth=%d (threshold=%d)",
                lane, depth, self._depth_warning_threshold
            )

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
            logger.warning("[admission] Queue save failed: %s", exc)
