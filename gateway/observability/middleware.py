"""Observability middleware for gateway - wraps agent execution with tracing."""

import functools
from pathlib import Path
from typing import Optional, Callable, Any

from .trace import Trace, Span
from .store import TraceStore
from .pricing import calculate_cost


# Global store instance
_store: Optional[TraceStore] = None


def init_observability(db_path: Optional[Path] = None):
    """Initialize the global trace store."""
    global _store
    if db_path is None:
        db_path = Path.home() / ".hermes" / "analytics" / "traces.db"
    _store = TraceStore(db_path)


def get_store() -> TraceStore:
    """Get the global trace store, initializing if needed."""
    global _store
    if _store is None:
        init_observability()
    return _store


class TraceContext:
    """Thread-local trace context for the current request."""
    
    def __init__(self):
        self.trace: Optional[Trace] = None
        self.current_span: Optional[Span] = None
    
    def start_trace(self, user_id: str, platform: str, request_summary: str, task_type: str = "") -> Trace:
        """Start a new trace."""
        self.trace = Trace(
            user_id=user_id,
            platform=platform,
            request_summary=request_summary,
            task_type=task_type
        )
        return self.trace
    
    def start_span(self, name: str, kind: str = "internal") -> Optional[Span]:
        """Start a new span in the current trace."""
        if self.trace is None:
            return None
        parent_id = self.current_span.span_id if self.current_span else None
        span = self.trace.start_span(name, kind=kind, parent_span_id=parent_id)
        self.current_span = span
        return span
    
    def finish_span(self, status: str = "ok"):
        """Finish the current span."""
        if self.current_span:
            self.current_span.finish(status=status)
            self.current_span = None
    
    def finish_trace(self, status: str = "completed", error: str = ""):
        """Finish the trace and save it."""
        if self.trace:
            self.trace.finish(status=status, error=error)
            get_store().save(self.trace)
            self.trace = None


# Global context (will be replaced with thread-local in production)
_context = TraceContext()


def get_context() -> TraceContext:
    """Get the current trace context."""
    return _context


def trace_llm_call(model: str, input_tokens: int, output_tokens: int) -> None:
    """Record an LLM call in the current trace."""
    ctx = get_context()
    if ctx.trace is None:
        return
    
    span = ctx.start_span(f"llm:{model}", kind="llm")
    if span:
        span.model = model
        span.input_tokens = input_tokens
        span.output_tokens = output_tokens
        span.cost_usd = calculate_cost(input_tokens, output_tokens, model)
        ctx.finish_span()


def trace_tool_call(tool_name: str, args_preview: str = "") -> Callable:
    """Decorator to trace a tool call."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            ctx = get_context()
            if ctx.trace:
                span = ctx.start_span(f"tool:{tool_name}", kind="tool")
                if span:
                    span.tool_name = tool_name
                    span.tool_args_preview = args_preview[:200]
            
            try:
                result = func(*args, **kwargs)
                if ctx.trace:
                    ctx.finish_span(status="ok")
                return result
            except Exception as e:
                if ctx.trace:
                    ctx.finish_span(status="error")
                raise
        
        return wrapper
    return decorator
