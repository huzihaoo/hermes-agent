"""Configurable budget constants for tool result persistence.

Overridable at the RL environment level via HermesAgentEnvConfig fields.
Per-tool resolution: pinned > config overrides > registry > default.
"""

from dataclasses import dataclass, field
from typing import Dict

# Tools whose thresholds must be bounded for stable long-running sessions.
# read_file used to be pinned to infinity to avoid persist->read->persist loops,
# but that allowed a single large read to bloat the conversation and force
# fallback-time compaction.  Keep it high enough for useful file inspection,
# but below the fallback compaction trigger (~96K tokens in the stable config).
PINNED_THRESHOLDS: Dict[str, float] = {
    "read_file": 80_000,
}

# Stable-write defaults.  These intentionally keep tool output written into the
# conversation below the 128K fallback context's compaction trigger.  Full raw
# outputs are persisted to temp files with a preview/path pointer instead.
DEFAULT_RESULT_SIZE_CHARS: int = 80_000
DEFAULT_TURN_BUDGET_CHARS: int = 80_000
DEFAULT_PREVIEW_SIZE_CHARS: int = 1_500


@dataclass(frozen=True)
class BudgetConfig:
    """Immutable budget constants for the 3-layer tool result persistence system.

    Layer 2 (per-result): resolve_threshold(tool_name) -> threshold in chars.
    Layer 3 (per-turn):   turn_budget -> aggregate char budget across all tool
                          results in a single assistant turn.
    Preview:              preview_size -> inline snippet size after persistence.
    """

    default_result_size: int = DEFAULT_RESULT_SIZE_CHARS
    turn_budget: int = DEFAULT_TURN_BUDGET_CHARS
    preview_size: int = DEFAULT_PREVIEW_SIZE_CHARS
    tool_overrides: Dict[str, int] = field(default_factory=dict)

    def resolve_threshold(self, tool_name: str) -> int | float:
        """Resolve the persistence threshold for a tool.

        Priority: pinned -> tool_overrides -> registry per-tool -> default.
        """
        if tool_name in PINNED_THRESHOLDS:
            return PINNED_THRESHOLDS[tool_name]
        if tool_name in self.tool_overrides:
            return self.tool_overrides[tool_name]
        from tools.registry import registry
        return registry.get_max_result_size(tool_name, default=self.default_result_size)


# Default config -- matches current hardcoded behavior exactly.
DEFAULT_BUDGET = BudgetConfig()
