"""Admission control — queue, audit, and permission integration.

Version History:
  v1.0.0 (2026-04-24) — Initial release
    - Multi-lane queue (fast/standard/heavy)
    - Domain isolation (user/group/vm)
    - Rate limiting + retry with exponential backoff
    - SQLite persistence with WAL mode
    - Audit logging
    - 62/62 tests passing

  v1.0.1 (2026-04-25) — Concurrency hardening
    - Fixed _gc_empty_domain_id race condition (pop with None default)
    - Added _metrics_lock for thread-safe counter updates
    - Persistence switched to upsert (INSERT OR REPLACE) instead of DELETE+rewrite
    - All 3 concurrency stress tests passing (500 items, 10 threads)
"""

__version__ = "1.0.1"

from .audit import AuditEvent, log_audit
from .controller import AdmissionController
from .feishu_integration import FeishuAdmissionBridge
from .queue import AdmissionQueue
from .types import Lane, QueueItem, QueueStatus

__all__ = [
    "AdmissionController",
    "AdmissionQueue",
    "AuditEvent",
    "FeishuAdmissionBridge",
    "Lane",
    "QueueItem",
    "QueueStatus",
    "log_audit",
    "__version__",
]
