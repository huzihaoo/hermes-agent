"""Gateway context governance policy.

The gateway should assemble model context from explicit, budgeted layers rather
than replaying arbitrary platform transcripts.  This module holds the default
budgets so platform-specific sanitizers and future context assemblers share one
contract surface.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ContextLayerBudget:
    """Budget for a named context layer."""

    name: str
    max_chars: int
    include_by_default: bool = True


@dataclass(frozen=True)
class GatewayContextPolicy:
    """Default character budgets for gateway model-context assembly."""

    latest_user_max_chars: int = 100_000
    reply_context_max_chars: int = 500
    root_summary_max_chars: int = 800
    recent_human_max_messages: int = 5
    recent_human_max_chars: int = 1200
    task_state_max_chars: int = 1200
    artifact_refs_max_chars: int = 800
    total_injected_aux_max_chars: int = 4000
    drop_bot_self_history: bool = True

    def layer_budgets(self) -> tuple[ContextLayerBudget, ...]:
        """Return the standard context-layer budgets in injection order."""
        return (
            ContextLayerBudget("latest_user", self.latest_user_max_chars),
            ContextLayerBudget("reply_context", self.reply_context_max_chars),
            ContextLayerBudget("root_summary", self.root_summary_max_chars),
            ContextLayerBudget("recent_human", self.recent_human_max_chars),
            ContextLayerBudget("task_state", self.task_state_max_chars),
            ContextLayerBudget("artifact_refs", self.artifact_refs_max_chars),
        )
