"""Owner review ledger for G1Q3 RCA completion notices.

This module is intentionally gateway-local and side-effect-light: it only
handles explicit ``rca ...`` owner commands, writes an audited JSON ledger plus
JSONL receipts, and otherwise lets normal intake routing continue.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

OWNER_ENV = "HERMES_G1Q3_REVIEW_OWNERS"
OWNER_USER_ID_ENV = "HERMES_G1Q3_REVIEW_OWNER_USER_IDS"
BUSINESS_STATE_SCHEMA_VERSION = "pnc_business_state_sidecar_from_owner_review_v1"
LEDGER_SCHEMA_VERSION = "g1q3_rca_owner_review_ledger_v1"
REVIEW_SCHEMA_VERSION = "g1q3_rca_owner_review_v1"

_COMMAND_RE = re.compile(
    r"^(?:rca|RCA)\s+(通过|驳回|补证据|撤回)\s+(\d+)(?:\s+(.*))?$",
    re.DOTALL,
)


@dataclass(frozen=True)
class OwnerAllowlist:
    user_ids: set[str]
    names: set[str]
    legacy: set[str]


@dataclass(frozen=True)
class OwnerReviewResult:
    handled: bool
    response: str | None = None


def handle_owner_review_message(
    event: Any, *, hermes_home: str | Path
) -> OwnerReviewResult:
    """Consume explicit owner commands even when their audited handler fails."""
    try:
        return _handle_owner_review_message(event, hermes_home=hermes_home)
    except Exception as exc:
        parsed = _parse_command(getattr(event, "text", "") or "")
        if parsed is not None and _is_g1q3_bound_group_source(
            getattr(event, "source", None)
        ):
            return OwnerReviewResult(
                handled=True,
                response=(
                    "RCA owner review 处理失败，提交状态需核验："
                    f"{type(exc).__name__}"
                ),
            )
        raise


def _handle_owner_review_message(
    event: Any, *, hermes_home: str | Path
) -> OwnerReviewResult:
    """Handle an explicit owner-review command if ``event.text`` is one.

    Returns ``handled=False`` for unrelated messages.  All explicit ``rca``
    commands are consumed, including disabled/malformed/unauthorized cases, so
    they never fall through to generic G1Q3 intake.
    """
    parsed = _parse_command(getattr(event, "text", "") or "")
    if parsed is None:
        return OwnerReviewResult(handled=False)
    if not _is_g1q3_bound_group_source(getattr(event, "source", None)):
        return OwnerReviewResult(handled=False)

    owners = _owner_allowlist()
    if not owners.user_ids:
        return OwnerReviewResult(
            handled=True,
            response=(
                "owner review 未启用,请配置 "
                "HERMES_G1Q3_REVIEW_OWNER_USER_IDS"
            ),
        )
    if not parsed.get("valid"):
        return OwnerReviewResult(handled=True, response=_usage_message())

    source = getattr(event, "source", None)
    owner_id = str(getattr(source, "user_id", "") or "").strip()
    owner_name = str(getattr(source, "user_name", "") or "").strip()
    if not _is_allowed_owner(owner_id=owner_id, owner_name=owner_name, owners=owners):
        return OwnerReviewResult(handled=True, response="你不在 G1Q3 RCA owner review allowlist 中，本次不写入 ledger。")

    action = str(parsed["action"])
    issue_id = str(parsed["issue_id"])
    reason = str(parsed.get("reason") or "").strip()
    override = bool(parsed.get("override"))
    if action in {"驳回", "补证据", "撤回"} and not reason:
        return OwnerReviewResult(handled=True, response=f"{action} 必须填写理由。格式：rca {action} {issue_id} <理由>")

    review_dir = Path(hermes_home) / "pnc_agent" / "reviews" / "g1q3_rca"
    now = datetime.now(timezone.utc)
    adjudication_result = None
    adjudication_store = None
    if action == "撤回":
        from gateway.pnc_rca_delivery_store import RcaDeliveryStore

        control_db_path = (
            Path(hermes_home)
            / "runtime"
            / "pnc_agent"
            / "feishu_issue_kafka_rca"
            / "control.sqlite3"
        )
        try:
            adjudication_store = RcaDeliveryStore(
                control_db_path, require_current=True
            )
            adjudication_result = adjudication_store.record_conclusion_adjudication(
                work_item_id=issue_id,
                action="retract",
                reason=reason,
                actor_id=owner_id,
                actor_name=owner_name,
                source=_source_record(event),
                now=now,
            )
        except Exception as exc:
            return OwnerReviewResult(
                handled=True,
                response=f"RCA 撤回未执行：{exc}",
            )
        override = True
    try:
        persisted = _persist_owner_review_artifacts(
            event=event,
            hermes_home=Path(hermes_home),
            review_dir=review_dir,
            issue_id=issue_id,
            action=action,
            reason=reason,
            owner_id=owner_id,
            owner_name=owner_name,
            override=override,
            adjudication_result=adjudication_result,
            now=now,
        )
    except Exception as exc:
        if adjudication_result is None or adjudication_store is None:
            raise
        try:
            adjudication_store.mark_conclusion_adjudication_artifact_repair(
                adjudication_id=adjudication_result.adjudication_id,
                succeeded=False,
                error_code=type(exc).__name__,
                error_detail=str(exc),
                now=now,
            )
        except Exception:
            pass
        return OwnerReviewResult(
            handled=True,
            response=(
                f"RCA 撤回已提交：issue {issue_id}；更正已入队，"
                f"审计材料待修复：{type(exc).__name__}"
            ),
        )
    if persisted["idempotent"] and adjudication_result is None:
        current = persisted["record"]
        return OwnerReviewResult(
            handled=True,
            response=(
                f"issue {issue_id} 已有 current 结论："
                f"{current.get('action') or current.get('verdict')}；"
                "如需改判请在指令中加入 覆盖。"
            ),
        )
    if adjudication_result is not None and adjudication_store is not None:
        try:
            adjudication_store.mark_conclusion_adjudication_artifact_repair(
                adjudication_id=adjudication_result.adjudication_id,
                succeeded=True,
                now=now,
            )
        except Exception as exc:
            return OwnerReviewResult(
                handled=True,
                response=(
                    f"RCA 撤回已提交：issue {issue_id}；更正已入队，"
                    f"审计状态待修复：{type(exc).__name__}"
                ),
            )

    latency_seconds = persisted["record"].get("latency_seconds")
    latency_text = (
        "耗时不可算"
        if latency_seconds is None
        else _format_latency(int(latency_seconds))
    )
    return OwnerReviewResult(
        handled=True,
        response=(
            f"RCA owner review 已记录：issue {issue_id} / {action} / owner "
            f"{owner_name or owner_id or 'unknown'} / {latency_text}"
        ),
    )


def _is_g1q3_bound_group_source(source: Any) -> bool:
    from gateway.pnc_group_binding import is_g1q3_rca_bound_chat, _is_feishu

    return _is_feishu(getattr(source, "platform", None)) and is_g1q3_rca_bound_chat(getattr(source, "chat_id", ""))


def _parse_command(text: str) -> dict[str, Any] | None:
    if not (text.startswith("rca ") or text.startswith("RCA ")):
        return None
    match = _COMMAND_RE.match(text.strip())
    if not match:
        return {"valid": False}
    action, issue_id, tail = match.groups()
    reason, override = _extract_reason_and_override(tail or "")
    return {"valid": True, "action": action, "issue_id": issue_id, "reason": reason, "override": override}


def _extract_reason_and_override(tail: str) -> tuple[str, bool]:
    parts = [part for part in re.split(r"\s+", str(tail or "").strip()) if part]
    override = False
    kept: list[str] = []
    for part in parts:
        if part == "覆盖":
            override = True
        else:
            kept.append(part)
    return " ".join(kept).strip(), override


def _owner_allowlist() -> OwnerAllowlist:
    user_ids: set[str] = {item.strip() for item in os.getenv(OWNER_USER_ID_ENV, "").split(",") if item.strip()}
    names: set[str] = set()
    legacy: set[str] = set()
    for raw in os.getenv(OWNER_ENV, "").split(","):
        item = raw.strip()
        if not item:
            continue
        if item.startswith(("user_id:", "uid:")):
            user_ids.add(item.split(":", 1)[1].strip())
        elif item.startswith(("name:", "user_name:")):
            names.add(item.split(":", 1)[1].strip())
        else:
            legacy.add(item)
    return OwnerAllowlist(user_ids={x for x in user_ids if x}, names={x for x in names if x}, legacy={x for x in legacy if x})


def _is_allowed_owner(*, owner_id: str, owner_name: str, owners: OwnerAllowlist) -> bool:
    del owner_name
    return bool(owner_id and owner_id in owners.user_ids)


def _verdict_for_action(action: str) -> str:
    return {
        "通过": "approved",
        "驳回": "rejected",
        "补证据": "need_evidence",
        "撤回": "retracted",
    }.get(action, "unknown")


def _source_record(event: Any) -> dict[str, Any]:
    source = getattr(event, "source", None)
    platform = getattr(getattr(source, "platform", None), "value", getattr(source, "platform", None))
    return {
        "platform": str(platform or ""),
        "chat_id": str(getattr(source, "chat_id", "") or ""),
        "thread_id": str(getattr(source, "thread_id", "") or ""),
        "message_id": str(getattr(event, "message_id", "") or ""),
    }


def _review_event_id(record: dict[str, Any]) -> str:
    material = {
        key: record.get(key)
        for key in (
            "issue_id",
            "action",
            "reason",
            "owner_id",
            "adjudication_id",
            "source",
        )
    }
    encoded = json.dumps(
        material,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return f"g1q3-rca-owner-review-v1-{hashlib.sha256(encoded).hexdigest()}"


def _persist_owner_review_artifacts(
    *,
    event: Any,
    hermes_home: Path,
    review_dir: Path,
    issue_id: str,
    action: str,
    reason: str,
    owner_id: str,
    owner_name: str,
    override: bool,
    adjudication_result: Any,
    now: datetime,
) -> dict[str, Any]:
    artifact_info = _find_report_generated_at(hermes_home, issue_id)
    report_generated_at = artifact_info.get("report_generated_at")
    latency_seconds = (
        _latency_seconds(report_generated_at, now)
        if report_generated_at
        else None
    )
    record: dict[str, Any] = {
        "schema_version": REVIEW_SCHEMA_VERSION,
        "issue_id": issue_id,
        "verdict": _verdict_for_action(action),
        "action": action,
        "reason": reason,
        "owner_id": owner_id,
        "owner_name": owner_name,
        "reviewed_at": now.isoformat(),
        "report_generated_at": report_generated_at,
        "report_generated_at_source": artifact_info.get("source"),
        "latency_seconds": latency_seconds,
        "override": override,
        "source": _source_record(event),
    }
    if adjudication_result is not None:
        record.update(
            adjudication_id=adjudication_result.adjudication_id,
            original_effect_key=adjudication_result.original_effect_key,
            correction_effect_key=adjudication_result.correction_effect_key,
            conclusion_state=adjudication_result.conclusion_state,
            impact_lineage=adjudication_result.impact_lineage,
        )
    sidecar_path = _business_state_sidecar_path(
        review_dir=review_dir,
        issue_id=issue_id,
    )
    record["business_state_sidecar_path"] = str(sidecar_path)
    record["review_event_id"] = _review_event_id(record)
    write_result = _write_ledger(
        review_dir=review_dir,
        issue_id=issue_id,
        record=record,
        override=override,
    )
    idempotent = bool(write_result.get("idempotent"))
    if idempotent:
        current = write_result.get("current")
        if isinstance(current, dict):
            record = dict(current)
        if adjudication_result is None:
            return {"idempotent": True, "record": record}

    sidecar_path = _business_state_sidecar_path(
        review_dir=review_dir,
        issue_id=issue_id,
    )
    _write_business_state_sidecar(
        review_dir=review_dir,
        issue_id=issue_id,
        record=record,
    )
    receipt = {
        "schema_version": REVIEW_SCHEMA_VERSION,
        "event_type": "owner_review",
        **record,
        "latency_unavailable": record.get("latency_seconds") is None,
        "ledger_path": str(review_dir / "ledger.json"),
        "business_state_sidecar_path": str(sidecar_path),
    }
    try:
        receipt_time = datetime.fromisoformat(str(record["reviewed_at"]))
    except (KeyError, TypeError, ValueError):
        receipt_time = now
    _append_receipt(review_dir, receipt, now=receipt_time)
    return {"idempotent": idempotent, "record": record}


def _usage_message() -> str:
    return "格式：rca <通过|驳回|补证据|撤回> <issue_id数字> [理由] [覆盖]；驳回/补证据/撤回理由必填。"


def _write_ledger(*, review_dir: Path, issue_id: str, record: dict[str, Any], override: bool) -> dict[str, Any]:
    review_dir.mkdir(parents=True, exist_ok=True)
    ledger_path = review_dir / "ledger.json"
    with open(ledger_path, "a+", encoding="utf-8") as fh:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        fh.seek(0)
        try:
            ledger = json.loads(fh.read() or "{}")
        except json.JSONDecodeError:
            ledger = {}
        if not isinstance(ledger, dict) or ledger.get("schema_version") != LEDGER_SCHEMA_VERSION:
            ledger = {"schema_version": LEDGER_SCHEMA_VERSION, "issues": {}}
        issues = ledger.setdefault("issues", {})
        entry = issues.setdefault(issue_id, {"history": []})
        current = entry.get("current") if isinstance(entry, dict) else None
        if current and all(
            current.get(key) == record.get(key)
            for key in ("action", "reason", "owner_id", "adjudication_id")
        ):
            return {"idempotent": True, "current": current}
        if current and not override:
            return {"idempotent": True, "current": current}
        if current and override:
            history = entry.get("history") if isinstance(entry.get("history"), list) else []
            history.append(current)
            entry["history"] = history[-200:]
        entry["current"] = record
        entry.setdefault("history", [])
        ledger["updated_at"] = record["reviewed_at"]
        fh.seek(0)
        fh.truncate()
        fh.write(json.dumps(ledger, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
        return {"idempotent": False, "current": record}


def _append_receipt(
    review_dir: Path, receipt: dict[str, Any], *, now: datetime
) -> bool:
    review_dir.mkdir(parents=True, exist_ok=True)
    path = review_dir / f"owner_review-{now.date().isoformat()}.jsonl"
    with open(path, "a+", encoding="utf-8") as fh:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        fh.seek(0)
        event_id = str(receipt.get("review_event_id") or "")
        for line in fh:
            try:
                existing = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event_id and existing.get("review_event_id") == event_id:
                return False
        fh.seek(0, os.SEEK_END)
        fh.write(json.dumps(receipt, ensure_ascii=False, sort_keys=True) + "\n")
        return True


def _business_state_sidecar_path(*, review_dir: Path, issue_id: str) -> Path:
    return review_dir / "business-states" / f"G1Q3-{issue_id}.business-state.yaml"


def _write_business_state_sidecar(*, review_dir: Path, issue_id: str, record: dict[str, Any]) -> Path:
    path = _business_state_sidecar_path(review_dir=review_dir, issue_id=issue_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    state, next_action = _business_state_for_verdict(str(record.get("verdict") or ""))
    reviewed_at = str(record.get("reviewed_at") or "")
    owner = str(record.get("owner_name") or record.get("owner_id") or "rca_owner")
    reason = str(record.get("reason") or record.get("action") or record.get("verdict") or "owner review")
    doc = {
        "schema_version": BUSINESS_STATE_SCHEMA_VERSION,
        "case_id": f"G1Q3-{issue_id}",
        "task_id": f"g1q3_rca_issue_review_{issue_id}",
        "business_state": state,
        "owner": owner,
        "transitions": [
            {"from": "new", "to": "owner_confirmed", "at": reviewed_at, "by": owner, "reason": "owner review command identifies accountable owner"},
            {"from": "owner_confirmed", "to": "evidence_ready", "at": reviewed_at, "by": "rca_agent", "reason": "RCA report completion notice reached owner review"},
            {"from": "evidence_ready", "to": "rca_review", "at": reviewed_at, "by": "rca_agent", "reason": "RCA result requires owner review"},
            {"from": "rca_review", "to": state, "at": reviewed_at, "by": owner, "reason": reason},
        ],
        "next_action": next_action,
        "reopened_count": 0,
        "source_class": "task_state",
        "truth_status": "observed_owner_review",
        "owner_review": {
            "schema_version": str(record.get("schema_version") or REVIEW_SCHEMA_VERSION),
            "issue_id": issue_id,
            "verdict": str(record.get("verdict") or ""),
            "action": str(record.get("action") or ""),
            "reviewed_at": reviewed_at,
            "ledger_schema_version": LEDGER_SCHEMA_VERSION,
        },
    }
    with open(path, "w", encoding="utf-8") as fh:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        fh.write(json.dumps(doc, ensure_ascii=False, indent=2, sort_keys=False) + "\n")
    return path


def _business_state_for_verdict(verdict: str) -> tuple[str, str]:
    if verdict == "approved":
        return "fix_verification", "进入修复验证/交付门禁；必要时补 gate decision 与 delivery receipt"
    if verdict == "need_evidence":
        return "need_input", "owner 要求补证据；补齐后重新提交 RCA review"
    if verdict == "rejected":
        return "need_input", "owner 驳回当前 RCA；补充/重跑证据后重新提交 RCA review"
    if verdict == "retracted":
        return "need_input", "当前 RCA 结论已作废；重新复核证据后再提交 RCA review"
    return "rca_review", "复核 owner review verdict 后再推进"


def _find_report_generated_at(hermes_home: Path, issue_id: str) -> dict[str, Any]:
    for payload, label in _candidate_payloads(hermes_home):
        if not _payload_matches_issue(payload, issue_id):
            continue
        result = payload.get("rca_execution_result") if isinstance(payload.get("rca_execution_result"), dict) else payload
        artifacts = result.get("artifacts") if isinstance(result.get("artifacts"), dict) else {}
        report_generated_at = _clean_iso(artifacts.get("report_generated_at"))
        if report_generated_at:
            return {"report_generated_at": report_generated_at, "source": f"{label}:rca_execution_result.artifacts.report_generated_at"}
        notice = payload.get("completion_notice") if isinstance(payload.get("completion_notice"), dict) else {}
        generated_at = _clean_iso(notice.get("generated_at"))
        if generated_at:
            return {"report_generated_at": generated_at, "source": f"{label}:completion_notice.generated_at"}
    return {"report_generated_at": None, "source": None}


def _candidate_payloads(hermes_home: Path) -> list[tuple[dict[str, Any], str]]:
    roots = [
        hermes_home / "task-state",
        hermes_home / "shared-state" / "tasks",
        hermes_home / "pnc_agent" / "reviews" / "g1q3_rca" / "artifacts",
    ]
    rows: list[tuple[dict[str, Any], str]] = []
    for root in roots:
        if not root.exists():
            continue
        for path in list(root.glob("*.json")) + list(root.glob("*/*.json")):
            if len(rows) >= 500:
                return rows
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if isinstance(payload, dict):
                rows.append((payload, str(path)))
    return rows


def _payload_matches_issue(payload: dict[str, Any], issue_id: str) -> bool:
    needles = {issue_id, f"G1Q3-{issue_id}"}
    keys = ("issue_id", "work_item_id", "case_id")
    containers = [payload]
    for key in ("rca_execution_result", "completion_notice", "work_item", "case"):
        value = payload.get(key)
        if isinstance(value, dict):
            containers.append(value)
    for container in containers:
        for key in keys:
            value = str(container.get(key) or "").strip()
            if value in needles or value.endswith(f"-{issue_id}"):
                return True
    return False


def _clean_iso(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _latency_seconds(report_generated_at: str, now: datetime) -> int | None:
    try:
        base = datetime.fromisoformat(report_generated_at.replace("Z", "+00:00"))
        if base.tzinfo is None:
            base = base.replace(tzinfo=timezone.utc)
        return max(0, int((now - base.astimezone(timezone.utc)).total_seconds()))
    except ValueError:
        return None


def _format_latency(seconds: int) -> str:
    if seconds < 60:
        return f"耗时{seconds}秒"
    minutes = seconds // 60
    if minutes < 60:
        return f"耗时{minutes}分钟"
    return f"耗时{minutes // 60}小时{minutes % 60}分钟"
