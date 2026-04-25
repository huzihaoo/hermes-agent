"""REST API for observability dashboard."""

from typing import Optional, List, Dict, Any
from pathlib import Path

from .store import TraceStore


_store_instance: Optional[TraceStore] = None


def get_api_store() -> TraceStore:
    """Get TraceStore instance for API."""
    global _store_instance
    if _store_instance is None:
        from hermes_constants import get_hermes_home
        db_path = get_hermes_home() / "analytics" / "traces.db"
        _store_instance = TraceStore(db_path)
    return _store_instance


def reset_api_store():
    """Reset the cached store (for testing)."""
    global _store_instance
    _store_instance = None


def api_list_traces(
    limit: int = 20,
    user_id: Optional[str] = None,
    status: Optional[str] = None
) -> Dict[str, Any]:
    """List recent traces with optional filters."""
    store = get_api_store()
    traces = store.list_recent(limit=limit, user_id=user_id)
    
    if status:
        traces = [t for t in traces if t["status"] == status]
    
    return {"traces": traces, "count": len(traces)}


def api_get_trace(trace_id: str) -> Optional[Dict[str, Any]]:
    """Get trace details with spans."""
    store = get_api_store()
    trace = store.get(trace_id)
    
    if not trace:
        return None
    
    spans = store.get_spans(trace_id)
    return {"trace": trace, "spans": spans}


def api_stats_daily(days: int = 7) -> Dict[str, Any]:
    """Get daily statistics."""
    store = get_api_store()
    return store.stats_daily(days=days)


def api_stats_cost(days: int = 30, group_by: Optional[str] = None) -> Dict[str, Any]:
    """Get cost statistics, optionally grouped by user."""
    store = get_api_store()
    
    if group_by == "user":
        by_user = store.stats_by_user(days=days)
        total = sum(u["total_cost_usd"] or 0.0 for u in by_user)
        return {
            "by_user": by_user,
            "total_cost_usd": total,
            "days": days
        }
    
    return store.stats_daily(days=days)
