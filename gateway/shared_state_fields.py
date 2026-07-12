"""Tenant field governance for shared-state tasks: business_line + owner.

Fixes the pollution where ~97% of tasks have NULL business_line and ~67% have an
empty/role-account owner (design: shared-state-tenant-field-pollution-fix-20260618).

Pure/deterministic helpers only — no DB writes here. The create-task path calls
``enforce_task_fields`` (gated by SHARED_STATE_FIELD_ENFORCE_ENABLED) so NEW tasks
never carry a NULL business_line; the backfill script reuses the inference helpers.

Principles:
  * controlled vocabulary: business_line is an enum (5+1 responsibility structure)
    plus an explicit ``unassigned`` terminal — an HONEST empty, distinct from NULL.
  * human vs system: ``coding`` / ``integration-tools-owner`` and the like are
    role/system accounts, not people; tagged via actor_kind so metrics can exclude
    them from the human-owner denominator.
  * never guess: inference only fires on reliable signals; otherwise -> unassigned.
"""
from __future__ import annotations

import re

# 5+1 responsibility structure + honest "unassigned" terminal (NOT NULL).
BUSINESS_LINES = (
    "integration_tools",
    "g1q3_rca",
    "rca_general",
    "evaluation_gate",
    "toolchain",
    "l4_orin",
    "knowledge_governance",
    "unassigned",
)

# Known non-human owners observed live in the owner column (2026-06-19).
ROLE_ACCOUNTS = frozenset({"integration-tools-owner", "g1q3-rca-owner", "rca-owner"})
SYSTEM_ACCOUNTS = frozenset({"coding", "create_task_v2.py", "agent", "system", "main"})


def normalize_business_line(value: str | None, *, inferred: str | None = None) -> str:
    """Return a valid enum value, never NULL/empty.

    Order: explicit valid value -> reliably inferred value -> ``unassigned``.
    Unknown/garbage values collapse to ``unassigned`` (honest), not silently kept.
    """
    v = str(value or "").strip()
    if v in BUSINESS_LINES and v != "unassigned":
        return v
    inf = str(inferred or "").strip()
    if inf in BUSINESS_LINES and inf != "unassigned":
        return inf
    return "unassigned"


def classify_actor_kind(owner: str | None) -> str:
    """Return 'human' | 'role_account' | 'system' | 'unknown'."""
    o = str(owner or "").strip()
    if not o:
        return "unknown"
    if o in SYSTEM_ACCOUNTS:
        return "system"
    if o in ROLE_ACCOUNTS or o.endswith("-owner"):
        return "role_account"
    return "human"


def infer_business_line_from_context(*, title: str = "", goal_text: str = "", meta_hint: str = "") -> str | None:
    """Reliable-signal inference only; returns None when not confident.

    Title pattern is the most reliable signal we have today (e.g. G1Q3 in title).
    """
    blob = " ".join((str(title or ""), str(goal_text or ""), str(meta_hint or ""))).lower()
    if not blob.strip():
        return None
    if "g1q3" in blob:
        return "g1q3_rca"
    if any(k in blob for k in ("mdrive4", "integration_tools", "mcap-clean", "mcap-translate")):
        return "integration_tools"
    if any(k in blob for k in ("门禁", "evaluation gate", "evaluation_gate", "准出")):
        return "evaluation_gate"
    if any(k in blob for k in ("l4 orin", "l4_orin", "orin")):
        return "l4_orin"
    if any(k in blob for k in ("知识治理", "knowledge governance", "knowledge_governance")):
        return "knowledge_governance"
    if "rca" in blob:
        return "rca_general"
    return None


def enforce_task_fields(
    *,
    business_line: str | None,
    owner: str | None,
    title: str = "",
    goal_text: str = "",
    requester: str = "",
) -> dict:
    """Return enforced fields for a NEW task. business_line is NEVER NULL.

    - business_line: explicit -> inferred(title/goal) -> unassigned.
    - owner: kept; actor_kind classifies human/role/system; bl_source/owner_source
      record provenance for auditability.
    """
    inferred = infer_business_line_from_context(title=title, goal_text=goal_text)
    explicit = str(business_line or "").strip()
    bl = normalize_business_line(business_line, inferred=inferred)
    if explicit in BUSINESS_LINES and explicit != "unassigned":
        bl_source = "explicit"
    elif inferred:
        bl_source = "inferred"
    else:
        bl_source = "unassigned"

    owner_val = str(owner or "").strip() or str(requester or "").strip()
    actor_kind = classify_actor_kind(owner_val)
    owner_source = "explicit" if str(owner or "").strip() else ("requester" if requester else "none")

    return {
        "business_line": bl,
        "bl_source": bl_source,
        "owner": owner_val,
        "actor_kind": actor_kind,
        "owner_source": owner_source,
    }


def is_human_owner(owner: str | None) -> bool:
    """For metric denominators: count only real people."""
    return classify_actor_kind(owner) == "human"
