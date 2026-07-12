"""In-process context-governance metrics.

This module intentionally has no external dependencies.  It provides aggregate
counters that can be exported by future gateway/admission metrics surfaces while
keeping private message content out of observability data.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from threading import Lock
from typing import Any


@dataclass
class ContextGovernanceCounters:
    prepared_history_runs: int = 0
    input_messages_total: int = 0
    output_messages_total: int = 0
    input_chars_total: int = 0
    output_chars_total: int = 0
    dropped_self_history_total: int = 0
    compacted_tool_outputs_total: int = 0
    feishu_topic_sanitized_total: int = 0
    feishu_topic_sanitized_chars_total: int = 0
    feishu_topic_new_message_extracted_total: int = 0
    feishu_topic_fallback_to_regex_total: int = 0
    feishu_topic_fingerprint_hits_total: int = 0
    feishu_topic_fingerprint_misses_total: int = 0
    context_assembly_runs_total: int = 0
    context_assembly_input_chars_total: int = 0
    context_assembly_output_chars_total: int = 0
    context_assembly_layers_seen_total: int = 0
    context_assembly_layers_included_total: int = 0
    context_assembly_layer_overflow_total: int = 0
    feishu_thread_fetch_success_total: int = 0
    feishu_thread_fetch_error_total: int = 0


_LOCK = Lock()
_COUNTERS = ContextGovernanceCounters()


def record_context_governance_stats(stats: Any) -> None:
    with _LOCK:
        _COUNTERS.prepared_history_runs += 1
        _COUNTERS.input_messages_total += int(getattr(stats, "input_messages", 0) or 0)
        _COUNTERS.output_messages_total += int(getattr(stats, "output_messages", 0) or 0)
        _COUNTERS.input_chars_total += int(getattr(stats, "input_chars", 0) or 0)
        _COUNTERS.output_chars_total += int(getattr(stats, "output_chars", 0) or 0)
        _COUNTERS.dropped_self_history_total += int(getattr(stats, "dropped_self_history", 0) or 0)
        _COUNTERS.compacted_tool_outputs_total += int(getattr(stats, "compacted_tool_outputs", 0) or 0)


def record_context_assembly_stats(stats: Any) -> None:
    layers_seen = getattr(stats, "layers_seen", {}) or {}
    layers_included = getattr(stats, "layers_included", {}) or {}
    layer_overflow = getattr(stats, "layer_overflow", {}) or {}
    with _LOCK:
        _COUNTERS.context_assembly_runs_total += 1
        _COUNTERS.context_assembly_input_chars_total += int(getattr(stats, "total_input_chars", 0) or 0)
        _COUNTERS.context_assembly_output_chars_total += int(getattr(stats, "total_output_chars", 0) or 0)
        _COUNTERS.context_assembly_layers_seen_total += sum(int(value or 0) for value in layers_seen.values())
        _COUNTERS.context_assembly_layers_included_total += sum(int(value or 0) for value in layers_included.values())
        _COUNTERS.context_assembly_layer_overflow_total += sum(int(value or 0) for value in layer_overflow.values())


def record_feishu_thread_fetch(*, success: bool) -> None:
    with _LOCK:
        if success:
            _COUNTERS.feishu_thread_fetch_success_total += 1
        else:
            _COUNTERS.feishu_thread_fetch_error_total += 1


def record_feishu_topic_sanitized(*, removed_chars: int, new_message_extracted: bool, fingerprint_filtered: bool) -> None:
    removed = max(0, int(removed_chars or 0))
    with _LOCK:
        _COUNTERS.feishu_topic_sanitized_total += 1
        _COUNTERS.feishu_topic_sanitized_chars_total += removed
        if new_message_extracted:
            _COUNTERS.feishu_topic_new_message_extracted_total += 1
        else:
            _COUNTERS.feishu_topic_fallback_to_regex_total += 1
        if fingerprint_filtered:
            _COUNTERS.feishu_topic_fingerprint_hits_total += 1
        else:
            _COUNTERS.feishu_topic_fingerprint_misses_total += 1


def get_context_governance_counters() -> dict[str, int]:
    with _LOCK:
        return dict(asdict(_COUNTERS))


def reset_context_governance_counters() -> None:
    global _COUNTERS
    with _LOCK:
        _COUNTERS = ContextGovernanceCounters()


def export_context_governance_metrics() -> str:
    counters = get_context_governance_counters()
    help_text = {
        "prepared_history_runs": "Total transcript-to-agent history preparation runs",
        "input_messages_total": "Total transcript messages seen before context governance",
        "output_messages_total": "Total transcript messages kept after context governance",
        "input_chars_total": "Total transcript chars seen before context governance",
        "output_chars_total": "Total transcript chars kept after context governance",
        "dropped_self_history_total": "Total bot/platform self-history messages dropped",
        "compacted_tool_outputs_total": "Total oversized tool outputs compacted",
        "feishu_topic_sanitized_total": "Total Feishu synthetic topic messages sanitized",
        "feishu_topic_sanitized_chars_total": "Total Feishu topic chars removed before agent input",
        "feishu_topic_new_message_extracted_total": "Total Feishu topic sanitizations using newest-human-message extraction",
        "feishu_topic_fallback_to_regex_total": "Total Feishu topic sanitizations using pre-marker fallback",
        "feishu_topic_fingerprint_hits_total": "Total Feishu topic sanitizations matching bot fingerprint registry",
        "feishu_topic_fingerprint_misses_total": "Total Feishu topic sanitizations without bot fingerprint registry match",
        "context_assembly_runs_total": "Total ContextAssembler runs",
        "context_assembly_input_chars_total": "Total chars received by ContextAssembler",
        "context_assembly_output_chars_total": "Total chars emitted by ContextAssembler",
        "context_assembly_layers_seen_total": "Total context layers evaluated by ContextAssembler",
        "context_assembly_layers_included_total": "Total context layers included by ContextAssembler",
        "context_assembly_layer_overflow_total": "Total context layer overflows in ContextAssembler",
        "feishu_thread_fetch_success_total": "Total successful Feishu thread-fetch seam calls",
        "feishu_thread_fetch_error_total": "Total failed Feishu thread-fetch seam calls",
    }
    lines: list[str] = []
    for key, value in counters.items():
        metric = f"gateway_context_governance_{key}"
        lines.append(f"# HELP {metric} {help_text[key]}")
        lines.append(f"# TYPE {metric} counter")
        lines.append(f"{metric} {value}")
    return "\n".join(lines) + "\n"


class ContextGovernanceMetricsExporter:
    def export(self) -> str:
        return export_context_governance_metrics()


class _CompatContextGovernanceMetrics:
    """Backward-compatible object API used by the live sync worktree."""

    def record_prepared_history(self, stats: Any) -> None:
        record_context_governance_stats(stats)

    def record_feishu_topic_history_compacted(self, count: int = 1) -> None:
        for _ in range(max(0, int(count or 0))):
            record_feishu_topic_sanitized(
                removed_chars=0,
                new_message_extracted=False,
                fingerprint_filtered=False,
            )

    def snapshot(self) -> dict[str, int]:
        return get_context_governance_counters()


context_governance_metrics = _CompatContextGovernanceMetrics()
