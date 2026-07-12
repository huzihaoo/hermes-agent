"""Append-only state machine receipts for host-side G1Q3 RCA intake."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from gateway.pnc_rca_schema import RcaIntakeState, to_dict


RCA_INTAKE_STAGES = {
    "admitted",
    "issue_enrichment_started",
    "issue_enriched",
    "issue_enrichment_blocked",
    "issue_preread_blocked",
    "issue_fields_extracted",
    "issue_field_validation_blocked",
    "case_resolved",
    "case_resolution_blocked",
    "metadata_gated",
    "metadata_blocked",
    "vm_submitted",
    "vm_running",
    "vm_completed",
    "vm_failed",
    "readback_ready",
    "readback_delivered",
    "readback_blocked",
}


_SENSITIVE_SOURCE_KEYS = {"token", "secret", "raw", "raw_payload", "authorization", "cookie"}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _privacy_light(value: Any, *, string_limit: int = 1200) -> Any:
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key)
            if key_text.lower() in _SENSITIVE_SOURCE_KEYS:
                continue
            out[key_text] = _privacy_light(item, string_limit=string_limit)
        return out
    if isinstance(value, list):
        return [_privacy_light(item, string_limit=string_limit) for item in value[:20]]
    if isinstance(value, str):
        text = value.replace("\r\n", "\n").strip()
        if len(text) > string_limit:
            return text[:string_limit].rstrip() + "..."
        return text
    return value


def transition(previous: RcaIntakeState, stage: str, **updates: Any) -> RcaIntakeState:
    """Return a new intake state for a named stage transition."""
    if stage not in RCA_INTAKE_STAGES:
        raise ValueError(f"unknown RCA intake stage: {stage}")
    allowed = set(RcaIntakeState.__dataclass_fields__) - {"schema_version"}
    unknown = sorted(set(updates) - allowed)
    if unknown:
        raise ValueError(f"unknown RCA intake state field(s): {', '.join(unknown)}")
    return replace(previous, stage=stage, **updates)


def write_rca_intake_state(receipt_dir: str | Path, state: RcaIntakeState) -> Path:
    """Append one privacy-light RCA intake state receipt as JSONL."""
    out_dir = Path(receipt_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    path = out_dir / f"{now.date().isoformat()}-rca-intake.jsonl"
    record = to_dict(state)
    record["event_type"] = "rca_intake_state"
    record["receipt_timestamp"] = now.isoformat()
    record["source"] = _privacy_light(record.get("source") or {})
    record["request_text_excerpt"] = _privacy_light(record.get("request_text_excerpt") or "", string_limit=1200)
    record["issue_context"] = _privacy_light(record.get("issue_context") or {}, string_limit=2000)
    if record.get("blocker"):
        record["blocker"] = _privacy_light(record.get("blocker"), string_limit=1000)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    return path


def new_intake_state(
    *,
    task_id: str,
    stage: str = "admitted",
    group_binding_id: str = "",
    source: dict[str, Any] | None = None,
    request_text_excerpt: str = "",
    issue_context: Any = None,
    blocker: dict[str, Any] | None = None,
    retryable: bool = False,
) -> RcaIntakeState:
    """Create an initial intake state with bounded request/source data."""
    if stage not in RCA_INTAKE_STAGES:
        raise ValueError(f"unknown RCA intake stage: {stage}")
    return RcaIntakeState(
        task_id=task_id,
        stage=stage,
        group_binding_id=group_binding_id,
        source=_privacy_light(source or {}),
        request_text_excerpt=_privacy_light(request_text_excerpt, string_limit=1200),
        issue_context=issue_context,
        blocker=blocker,
        retryable=retryable,
        created_at=_now_iso(),
    )
