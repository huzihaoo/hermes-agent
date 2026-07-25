"""Requester identity contract for RCA human and automation entrypoints."""

from __future__ import annotations

import re
from typing import Literal


RequesterActorKind = Literal["human", "automation", "legacy_automation", "unknown"]

_HUMAN_OPEN_ID_RE = re.compile(r"^ou_[A-Za-z0-9_-]{3,200}$")
_AUTOMATION_ID_RE = re.compile(r"^automation:[a-z0-9][a-z0-9._-]{2,63}$")


def classify_rca_requester(requester_id: str | None) -> RequesterActorKind:
    value = str(requester_id or "").strip()
    if _HUMAN_OPEN_ID_RE.fullmatch(value):
        return "human"
    if _AUTOMATION_ID_RE.fullmatch(value):
        return "automation"
    if re.match(r"^(?:operator|codex)[-_]", value):
        return "legacy_automation"
    return "unknown"


def validate_rca_requester(*, platform: str, requester_id: str) -> str:
    actor_kind = classify_rca_requester(requester_id)
    if platform == "operator" and actor_kind != "automation":
        raise ValueError("manual_operator_requester_identity_invalid")
    if platform == "feishu" and actor_kind != "human":
        raise ValueError("manual_feishu_requester_identity_invalid")
    return actor_kind
