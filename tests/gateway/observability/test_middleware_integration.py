"""Integration test for observability middleware."""

import time

from gateway.observability.middleware import (
    init_observability,
    get_context,
    trace_llm_call,
    trace_tool_call,
    get_store
)


def test_full_trace_flow(tmp_path):
    """Test a complete trace with LLM and tool calls."""
    init_observability(tmp_path / "traces.db")
    ctx = get_context()
    
    # Start a trace
    trace = ctx.start_trace(
        user_id="alice",
        platform="feishu",
        request_summary="test task",
        task_type="test"
    )
    assert trace is not None
    
    # Simulate LLM call
    trace_llm_call("claude-opus-4-6", input_tokens=1000, output_tokens=500)
    
    # Simulate tool call
    @trace_tool_call("read_file", args_preview="/tmp/test.txt")
    def mock_read_file():
        time.sleep(0.01)
        return "file content"
    
    result = mock_read_file()
    assert result == "file content"
    
    # Another LLM call
    trace_llm_call("claude-sonnet-4", input_tokens=500, output_tokens=200)
    
    # Finish trace
    ctx.finish_trace(status="completed")
    
    # Verify stored
    store = get_store()
    loaded = store.get(trace.trace_id)
    assert loaded is not None
    assert loaded["user_id"] == "alice"
    assert loaded["total_tokens"] == 2200  # 1000+500+500+200
    assert loaded["total_cost_usd"] > 0
    
    # Verify spans
    spans = store.get_spans(trace.trace_id)
    assert len(spans) == 3  # 2 LLM + 1 tool
    llm_spans = [s for s in spans if s["kind"] == "llm"]
    tool_spans = [s for s in spans if s["kind"] == "tool"]
    assert len(llm_spans) == 2
    assert len(tool_spans) == 1
    assert tool_spans[0]["tool_name"] == "read_file"
