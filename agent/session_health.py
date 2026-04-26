"""
Session health monitoring and memory protection.

Prevents context window explosions by tracking session metrics and
enforcing limits on message count, runtime, and file size.

Based on incident: Session 20260425_124550_f9ed45
- 872 messages over 11 hours
- 2.3MB session file
- Context compression failure

Ref: knowledge/wiki/systems/session-memory-protection.md
"""
import time
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List

logger = logging.getLogger(__name__)


class HealthLevel(Enum):
    GREEN = "green"
    YELLOW = "yellow"
    RED = "red"
    BLOCKED = "blocked"


@dataclass
class HealthCheck:
    level: HealthLevel
    message_count: int = 0
    runtime_seconds: float = 0
    large_return_count: int = 0
    warnings: List[str] = field(default_factory=list)
    
    @property
    def should_warn(self) -> bool:
        return self.level in (HealthLevel.YELLOW, HealthLevel.RED)
    
    @property
    def should_block(self) -> bool:
        return self.level == HealthLevel.BLOCKED
    
    @property
    def status_emoji(self) -> str:
        return {
            HealthLevel.GREEN: "🟢",
            HealthLevel.YELLOW: "🟡",
            HealthLevel.RED: "🔴",
            HealthLevel.BLOCKED: "⛔",
        }[self.level]
    
    def format_warning(self) -> Optional[str]:
        if not self.warnings:
            return None
        header = f"{self.status_emoji} Session Health: {self.level.value.upper()}"
        body = "\n".join(f"  • {w}" for w in self.warnings)
        return f"{header}\n{body}"


class SessionHealthMonitor:
    """Monitor session health and enforce memory protection limits.
    
    Thresholds:
      Messages:  400 → yellow, 450 → red, 500 → blocked
      Runtime:   4h → yellow, 5h → red, 6h → blocked
      Tool returns > 40KB: 5 cumulative → yellow checkpoint hint
    """
    
    # Message limits
    MSG_WARN = 400
    MSG_CRITICAL = 450
    MSG_BLOCK = 500
    
    # Runtime limits (seconds)
    RT_WARN = 4 * 3600
    RT_CRITICAL = 5 * 3600
    RT_BLOCK = 6 * 3600
    
    # Tool return size
    TOOL_WARN_BYTES = 50 * 1024
    TOOL_TRUNCATE_BYTES = 100 * 1024
    LARGE_RETURN_BYTES = 40 * 1024
    LARGE_RETURN_MAX = 5
    
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.start_time = time.time()
        self.large_return_count = 0
        self._last_warned_at_count = 0
        self._last_warned_at_time = 0
    
    def check(self, messages: list) -> HealthCheck:
        """Evaluate session health against all thresholds."""
        msg_count = len(messages)
        runtime = time.time() - self.start_time
        warnings = []
        level = HealthLevel.GREEN
        
        # --- Message count ---
        if msg_count >= self.MSG_BLOCK:
            level = HealthLevel.BLOCKED
            warnings.append(
                f"Messages: {msg_count} ≥ {self.MSG_BLOCK} (LIMIT). "
                f"Start a new session."
            )
        elif msg_count >= self.MSG_CRITICAL:
            level = max(level, HealthLevel.RED, key=lambda l: list(HealthLevel).index(l))
            warnings.append(
                f"Messages: {msg_count} ≥ {self.MSG_CRITICAL}. "
                f"Checkpoint now and consider a new session."
            )
        elif msg_count >= self.MSG_WARN:
            level = max(level, HealthLevel.YELLOW, key=lambda l: list(HealthLevel).index(l))
            warnings.append(
                f"Messages: {msg_count} ≥ {self.MSG_WARN}. "
                f"Consider checkpointing soon."
            )
        
        # --- Runtime ---
        hours = runtime / 3600
        if runtime >= self.RT_BLOCK:
            level = HealthLevel.BLOCKED
            warnings.append(
                f"Runtime: {hours:.1f}h ≥ 6h (LIMIT). "
                f"Start a new session."
            )
        elif runtime >= self.RT_CRITICAL:
            level = max(level, HealthLevel.RED, key=lambda l: list(HealthLevel).index(l))
            warnings.append(
                f"Runtime: {hours:.1f}h ≥ 5h. "
                f"Checkpoint now and consider a new session."
            )
        elif runtime >= self.RT_WARN:
            level = max(level, HealthLevel.YELLOW, key=lambda l: list(HealthLevel).index(l))
            warnings.append(
                f"Runtime: {hours:.1f}h ≥ 4h. "
                f"Consider checkpointing soon."
            )
        
        # --- Large tool returns ---
        if self.large_return_count >= self.LARGE_RETURN_MAX:
            level = max(level, HealthLevel.YELLOW, key=lambda l: list(HealthLevel).index(l))
            warnings.append(
                f"Large tool returns (>40KB): {self.large_return_count} ≥ {self.LARGE_RETURN_MAX}. "
                f"Consider checkpointing."
            )
        
        return HealthCheck(
            level=level,
            message_count=msg_count,
            runtime_seconds=runtime,
            large_return_count=self.large_return_count,
            warnings=warnings,
        )
    
    def track_tool_return(self, result: str) -> Optional[str]:
        """Track tool return size. Returns a warning string if threshold exceeded."""
        if not isinstance(result, str):
            return None
        size = len(result.encode("utf-8", errors="replace"))
        
        if size > self.LARGE_RETURN_BYTES:
            self.large_return_count += 1
        
        if size > self.TOOL_TRUNCATE_BYTES:
            logger.warning(
                "session_health: tool return %d bytes (>%dKB) in session %s",
                size, self.TOOL_TRUNCATE_BYTES // 1024, self.session_id,
            )
            return (
                f"⚠️ Tool return size: {size // 1024}KB (>{self.TOOL_TRUNCATE_BYTES // 1024}KB). "
                f"Consider using head/tail/grep instead of full reads."
            )
        elif size > self.TOOL_WARN_BYTES:
            logger.info(
                "session_health: tool return %d bytes (>%dKB) in session %s",
                size, self.TOOL_WARN_BYTES // 1024, self.session_id,
            )
        return None
    
    def should_emit_warning(self, health: HealthCheck) -> bool:
        """Deduplicate warnings — only emit if state changed."""
        if not health.should_warn and not health.should_block:
            return False
        # Emit at most once per 50-message increment
        if health.message_count - self._last_warned_at_count < 50:
            return False
        self._last_warned_at_count = health.message_count
        return True
