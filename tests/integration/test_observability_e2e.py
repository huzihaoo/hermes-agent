"""End-to-end integration test for the full observability + memory + concurrency stack."""

import asyncio
import time
import pytest

from gateway.observability.trace import Trace
from gateway.observability.store import TraceStore
from gateway.observability.pricing import calculate_cost
from gateway.observability.middleware import TraceContext
from agent.memory_v2.store import MemoryStore
from gateway.concurrency import UserConcurrencyLimiter, TokenQuotaManager


def test_full_agent_simulation(tmp_path):
    """Simulate a complete agent run with all systems active."""
    
    # 1. Initialize all stores
    trace_store = TraceStore(db_path=tmp_path / "traces.db")
    memory_store = MemoryStore(db_path=tmp_path / "memory.db")
    quota_mgr = TokenQuotaManager(db_path=tmp_path / "quota.db", default_monthly_limit=100_000)
    
    user_id = "alice"
    
    # 2. Check quota before starting
    assert quota_mgr.check_quota(user_id) is True
    
    # 3. Start trace
    trace = Trace(user_id=user_id, platform="feishu", request_summary="分析 QCon 方案差距")
    
    # 4. Simulate LLM call
    span1 = trace.start_span("llm:understand", kind="llm")
    span1.model = "claude-opus-4-6"
    span1.input_tokens = 2500
    span1.output_tokens = 800
    span1.cost_usd = calculate_cost(2500, 800, "claude-opus-4-6")
    span1.finish()
    
    # 5. Simulate tool call
    span2 = trace.start_span("tool:search_files", kind="tool")
    span2.tool_name = "search_files"
    time.sleep(0.01)
    span2.finish()
    
    # 6. Simulate second LLM call
    span3 = trace.start_span("llm:analyze", kind="llm")
    span3.model = "claude-opus-4-6"
    span3.input_tokens = 8000
    span3.output_tokens = 3000
    span3.cost_usd = calculate_cost(8000, 3000, "claude-opus-4-6")
    span3.finish()
    
    # 7. Finish trace
    trace.finish()
    trace_store.save(trace)
    
    # 8. Record token consumption
    total_tokens = trace.total_tokens
    quota_mgr.consume(user_id, total_tokens, cost_usd=trace.total_cost_usd)
    
    # 9. Store memory from this task
    memory_store.add(user_id, "QCon 方案差距分析完成，主要差距在可观测性和记忆系统", category="task_result")
    
    # --- Verify everything ---
    
    # Trace stored correctly
    loaded = trace_store.get(trace.trace_id)
    assert loaded is not None
    assert loaded["user_id"] == user_id
    assert loaded["total_tokens"] == total_tokens
    assert loaded["total_cost_usd"] > 0
    assert loaded["status"] == "completed"
    
    # Spans stored correctly
    spans = trace_store.get_spans(trace.trace_id)
    assert len(spans) == 3
    llm_spans = [s for s in spans if s["kind"] == "llm"]
    tool_spans = [s for s in spans if s["kind"] == "tool"]
    assert len(llm_spans) == 2
    assert len(tool_spans) == 1
    
    # Cost calculated correctly
    assert loaded["total_cost_usd"] == pytest.approx(
        calculate_cost(2500, 800, "claude-opus-4-6") + calculate_cost(8000, 3000, "claude-opus-4-6"),
        rel=1e-6
    )
    
    # Quota tracked
    usage = quota_mgr.get_usage(user_id)
    assert usage["tokens_used"] == total_tokens
    assert usage["over_limit"] is False
    
    # Memory stored and searchable
    results = memory_store.search("QCon", user_id=user_id)
    assert len(results) >= 1
    assert "QCon" in results[0]["content"]
    
    # Stats work
    stats = trace_store.stats_daily(days=1)
    assert stats["total_traces"] == 1
    assert stats["total_tokens"] == total_tokens


@pytest.mark.asyncio
async def test_concurrent_users_simulation(tmp_path):
    """Simulate multiple users hitting the system concurrently."""
    
    limiter = UserConcurrencyLimiter(max_concurrent=2)
    trace_store = TraceStore(db_path=tmp_path / "traces.db")
    quota_mgr = TokenQuotaManager(db_path=tmp_path / "quota.db", default_monthly_limit=50_000)
    
    async def simulate_user(user_id: str, task_name: str):
        # Acquire concurrency slot
        acquired = await limiter.acquire(user_id)
        if not acquired:
            return {"user_id": user_id, "status": "queued"}
        
        try:
            # Check quota
            if not quota_mgr.check_quota(user_id):
                return {"user_id": user_id, "status": "over_quota"}
            
            # Create trace
            trace = Trace(user_id=user_id, platform="feishu", request_summary=task_name)
            span = trace.start_span("llm:call", kind="llm")
            span.input_tokens = 1000
            span.output_tokens = 500
            span.cost_usd = calculate_cost(1000, 500, "claude-sonnet-4")
            span.finish()
            trace.finish()
            trace_store.save(trace)
            
            # Consume quota
            quota_mgr.consume(user_id, 1500, cost_usd=trace.total_cost_usd)
            
            return {"user_id": user_id, "status": "completed", "trace_id": trace.trace_id}
        finally:
            await limiter.release(user_id)
    
    # Run 5 tasks for 2 users
    tasks = [
        simulate_user("alice", "task 1"),
        simulate_user("alice", "task 2"),
        simulate_user("alice", "task 3"),  # This should be queued (max 2)
        simulate_user("bob", "task 4"),
        simulate_user("bob", "task 5"),
    ]
    
    results = await asyncio.gather(*tasks)
    
    # At least some should complete
    completed = [r for r in results if r["status"] == "completed"]
    assert len(completed) >= 3  # At least alice(2) + bob(2) - 1 queued
    
    # Verify traces stored
    all_traces = trace_store.list_recent(limit=10)
    assert len(all_traces) >= 3
