"""Observability module for Hermes Agent.

Version History:
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

__version__ = "1.0.0"

from .trace import Trace, Span
from .store import TraceStore

__all__ = [
    "__version__",
    "Trace",
    "Span",
    "TraceStore",
]
