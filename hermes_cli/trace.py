"""CLI commands for trace and cost management."""

import time
from pathlib import Path
from typing import Optional

from hermes_cli.colors import Colors, color
from hermes_constants import get_hermes_home


def _get_store():
    from gateway.observability.store import TraceStore
    return TraceStore(db_path=get_hermes_home() / "analytics" / "traces.db")


def trace_list(limit: int = 20, user_id: Optional[str] = None, status: Optional[str] = None):
    """List recent traces."""
    store = _get_store()
    traces = store.list_recent(limit=limit, user_id=user_id)
    
    if status:
        traces = [t for t in traces if t["status"] == status]
    
    if not traces:
        print("暂无 Trace 记录。")
        return
    
    print(color("📊 最近 Trace\n", Colors.BOLD))
    
    for t in traces:
        # Status icon
        icon = {"completed": "✅", "failed": "❌", "running": "⏳", "timeout": "⏰"}.get(t["status"], "❓")
        
        # Duration
        duration = ""
        if t["end_time"] and t["start_time"]:
            dur_s = t["end_time"] - t["start_time"]
            duration = f"{dur_s:.1f}s"
        
        # Cost
        cost = f"${t['total_cost_usd']:.4f}" if t["total_cost_usd"] else ""
        
        # Tokens
        tokens = f"{t['total_tokens']:,}" if t["total_tokens"] else ""
        
        # Time
        ts = time.strftime("%m-%d %H:%M", time.localtime(t["start_time"]))
        
        trace_id = color(t["trace_id"][:12], Colors.YELLOW)
        summary = (t["request_summary"] or "")[:50]
        
        print(f"  {icon} {trace_id}  {ts}  {duration:>6}  {tokens:>8} tok  {cost:>8}  {summary}")
    
    print()
    print(color("💡 使用 'hermes trace show <id>' 查看详情", Colors.DIM))


def trace_show(trace_id: str):
    """Show trace details with span tree."""
    store = _get_store()
    
    # Try exact match first, then prefix
    trace = store.get(trace_id)
    if not trace:
        traces = store.list_recent(limit=100)
        matches = [t for t in traces if t["trace_id"].startswith(trace_id)]
        if len(matches) == 1:
            trace = matches[0]
        elif len(matches) > 1:
            print(color(f"❌ Trace ID '{trace_id}' 匹配到多个，请使用更长的 ID。", Colors.RED))
            return
    
    if not trace:
        print(color(f"❌ Trace '{trace_id}' 未找到。", Colors.RED))
        return
    
    # Header
    icon = {"completed": "✅", "failed": "❌", "running": "⏳"}.get(trace["status"], "❓")
    print(f"\n{icon} {color('Trace 详情', Colors.BOLD)}\n")
    print(f"  ID:       {color(trace['trace_id'], Colors.YELLOW)}")
    print(f"  用户:     {trace['user_id'] or '未知'}")
    print(f"  平台:     {trace['platform'] or '未知'}")
    print(f"  状态:     {trace['status']}")
    print(f"  摘要:     {trace['request_summary'] or '无'}")
    
    if trace["start_time"]:
        print(f"  开始:     {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(trace['start_time']))}")
    if trace["end_time"]:
        dur = trace["end_time"] - trace["start_time"]
        print(f"  耗时:     {dur:.1f}s")
    
    print(f"  Token:    {trace['total_tokens']:,}")
    print(f"  成本:     ${trace['total_cost_usd']:.4f}")
    
    if trace.get("error"):
        print(f"  错误:     {color(trace['error'], Colors.RED)}")
    
    # Spans
    spans = store.get_spans(trace["trace_id"])
    if spans:
        print(f"\n  {color('Span 列表:', Colors.BOLD)}")
        for i, s in enumerate(spans, 1):
            kind_icon = {"llm": "🤖", "tool": "🔧", "internal": "⚙️"}.get(s["kind"], "❓")
            dur = f"{s['duration_ms']:.0f}ms" if s["duration_ms"] else ""
            tokens = ""
            if s["input_tokens"] or s["output_tokens"]:
                tokens = f"{s['input_tokens']}+{s['output_tokens']} tok"
            cost = f"${s['cost_usd']:.4f}" if s["cost_usd"] else ""
            
            print(f"    {i}. {kind_icon} {s['name']:30} {dur:>8}  {tokens:>15}  {cost:>8}")
    
    print()


def cost_summary(days: int = 30, user_id: Optional[str] = None, group_by: Optional[str] = None):
    """Show cost summary."""
    store = _get_store()
    
    if group_by == "user":
        by_user = store.stats_by_user(days=days)
        if not by_user:
            print("暂无成本数据。")
            return
        
        print(color(f"💰 成本统计 (最近 {days} 天, 按用户)\n", Colors.BOLD))
        total_cost = 0.0
        for u in by_user:
            cost = u["total_cost_usd"] or 0.0
            total_cost += cost
            tokens = u["total_tokens"] or 0
            count = u["trace_count"] or 0
            print(f"  {u['user_id']:20} {count:>4} 次  {tokens:>10,} tok  ${cost:>8.4f}")
        
        print(f"\n  {'总计':20} {'':>4}     {'':>10}     ${total_cost:>8.4f}")
    else:
        stats = store.stats_daily(days=days)
        print(color(f"💰 成本统计 (最近 {days} 天)\n", Colors.BOLD))
        print(f"  任务数:   {stats['total_traces']}")
        print(f"  Token:    {stats['total_tokens']:,}")
        print(f"  成本:     ${stats['total_cost_usd']:.4f}")
    
    print()
