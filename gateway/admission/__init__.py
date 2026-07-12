"""Admission control — queue, audit, and permission integration.

Version History:
  v1.2.0 (2026-04-26) — Policy templates
    - Added PolicyTemplate and TemplateStore
    - Built-in templates: strict / relaxed / vip-priority
    - Import/export/seed/list support via CLI
    - AdmissionController.apply_template for runtime reconfiguration
    - 95/95 admission tests passing

  v1.1.0 (2026-04-26) — Alert rules
    - Added alerts.py with AlertManager / QueueDepthAlert / ErrorRateAlert
    - Queue depth warning/critical thresholds via unified alert pipeline
    - Error-rate monitoring with cooldown suppression and history
    - 79/79 admission tests passing at release

  v1.0.1 (2026-04-25) — Concurrency hardening
    - Fixed _gc_empty_domain_id race condition (pop with None default)
    - Added _metrics_lock for thread-safe counter updates
    - Persistence switched to upsert (INSERT OR REPLACE) instead of DELETE+rewrite
    - All 3 concurrency stress tests passing (500 items, 10 threads)
"""

__version__ = "1.11.3"

from .alerts import AlertLevel, AlertManager, AlertRecord, ErrorRateAlert, QueueDepthAlert
from .audit import AuditEvent, log_audit
from .controller import AdmissionController
from .feishu_integration import FeishuAdmissionBridge
from .queue import AdmissionQueue
from .templates import PolicyTemplate, TemplateStore, builtin_templates
from .types import Lane, QueueItem, QueueStatus

__all__ = [
    "AdmissionController",
    "AdmissionQueue",
    "AlertLevel",
    "AlertManager",
    "AlertRecord",
    "AuditEvent",
    "ErrorRateAlert",
    "FeishuAdmissionBridge",
    "Lane",
    "PolicyTemplate",
    "QueueDepthAlert",
    "QueueItem",
    "QueueStatus",
    "TemplateStore",
    "builtin_templates",
    "__version__",
]
