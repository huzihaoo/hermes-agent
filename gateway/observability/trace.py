"""Trace and Span data models for observability."""

import time
import uuid
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any


@dataclass
class Span:
    """A single step in a Trace (LLM call, tool call, etc)."""
    
    span_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    trace_id: str = ""
    parent_span_id: Optional[str] = None
    name: str = ""                    # "llm:call", "tool:read_file"
    kind: str = "internal"            # "llm", "tool", "internal"
    start_time: float = field(default_factory=time.time)
    end_time: float = 0.0
    duration_ms: float = 0.0
    status: str = "ok"                # "ok", "error", "timeout"
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    # LLM-specific
    input_tokens: int = 0
    output_tokens: int = 0
    model: str = ""
    cost_usd: float = 0.0
    
    # Tool-specific
    tool_name: str = ""
    tool_args_preview: str = ""
    
    def finish(self, status: str = "ok"):
        """Mark span as finished and calculate duration."""
        self.end_time = time.time()
        self.duration_ms = (self.end_time - self.start_time) * 1000
        self.status = status


@dataclass
class Trace:
    """A complete user request with all its spans."""
    
    trace_id: str = field(default_factory=lambda: uuid.uuid4().hex[:16])
    user_id: str = ""
    platform: str = ""
    request_summary: str = ""
    start_time: float = field(default_factory=time.time)
    end_time: float = 0.0
    status: str = "running"           # "running", "completed", "failed", "timeout"
    error: str = ""
    spans: List[Span] = field(default_factory=list)
    total_tokens: int = 0
    total_cost_usd: float = 0.0
    task_type: str = ""
    
    def start_span(self, name: str, kind: str = "internal", parent_span_id: Optional[str] = None) -> Span:
        """Create and attach a new span."""
        span = Span(trace_id=self.trace_id, name=name, kind=kind, parent_span_id=parent_span_id)
        self.spans.append(span)
        return span
    
    def finish(self, status: str = "completed", error: str = ""):
        """Mark trace as finished and aggregate metrics."""
        self.end_time = time.time()
        self.status = status
        self.error = error
        
        # Aggregate tokens and cost from all spans
        self.total_tokens = sum(s.input_tokens + s.output_tokens for s in self.spans)
        self.total_cost_usd = sum(s.cost_usd for s in self.spans)
