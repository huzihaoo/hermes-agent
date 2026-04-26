"""Observability module for Hermes Agent.

Version History:
  v1.1.1 (2026-04-26) — Retention policy
    - TraceStore.cleanup_old_data(): delete old traces/spans by retention window
    - TaskStore.cleanup_old_tasks(): delete old tasks by retention window
    - 6/6 retention policy tests passing

  v1.1.0 (2026-04-26) — Dashboard UI MVP
    - Standalone FastAPI dashboard server (port 8765)
    - Dark-themed UI: Overview / Traces / Cost tabs
    - Real-time stats with auto-refresh

  v1.0.0 (2026-04-24) — Initial release (Observability MVP)
    - Trace/Span model with hierarchical structure
    - TraceStore with SQLite persistence
    - Pricing calculator for token cost tracking
    - Dashboard API (FastAPI mount)
    - Gateway commands: /trace, /cost
    - CLI commands: hermes trace, hermes cost
    - Middleware integration for auto-tracing
    - Phase A-E complete, 100% test coverage
"""

__version__ = "1.1.1"

from .trace import Trace, Span
from .store import TraceStore

__all__ = [
    "__version__",
    "Trace",
    "Span",
    "TraceStore",
]
