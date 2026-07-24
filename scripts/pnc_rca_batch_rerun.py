#!/usr/bin/env python3
"""Run an issue-only RCA rerun queue through the resident production path."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
import time
from typing import Any, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gateway.pnc_rca_control_store import (  # noqa: E402
    MANUAL_TRIGGER_SCHEMA_VERSION,
    ManualRcaTriggerRequest,
    RcaControlStore,
)


SCHEMA_VERSION = "pnc_rca_batch_rerun_state_v1"
MAX_INPUT_BYTES = 2 * 1024 * 1024
ISSUE_ID_RE = re.compile(r"^[0-9]{6,24}$")
BATCH_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{2,63}$")
TERMINAL_JOB_STATUSES = frozenset({"delivered", "partial", "quarantined"})
ACTIVE_EFFECT_STATUSES = frozenset({"pending", "claimed", "retry_wait", "uncertain"})


class BatchRerunError(RuntimeError):
    def __init__(self, code: str):
        self.code = str(code or "batch_rerun_failed")[:120]
        super().__init__(self.code)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _read_json(path: Path) -> tuple[Any, str]:
    raw = path.read_bytes()
    if not raw or len(raw) > MAX_INPUT_BYTES:
        raise BatchRerunError("batch_input_size_invalid")
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BatchRerunError("batch_input_json_invalid") from exc
    return value, _sha256_bytes(raw)


def _write_state(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = (_canonical_json(value) + "\n").encode("utf-8")
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    fd = os.open(tmp, flags, 0o600)
    try:
        with os.fdopen(fd, "wb", closefd=True) as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
    except Exception:
        try:
            tmp.unlink()
        except OSError:
            pass
        raise


def _runtime_commit() -> str:
    try:
        return subprocess.check_output(
            ["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"],
            text=True,
            stderr=subprocess.DEVNULL,
            timeout=5,
        ).strip()
    except (OSError, subprocess.SubprocessError) as exc:
        raise BatchRerunError("batch_runtime_commit_unavailable") from exc


def _load_queue(path: Path) -> tuple[list[dict[str, Any]], str]:
    value, raw_sha = _read_json(path)
    if not isinstance(value, Mapping) or not isinstance(value.get("items"), list):
        raise BatchRerunError("batch_queue_schema_invalid")
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in value["items"]:
        if not isinstance(raw, Mapping):
            raise BatchRerunError("batch_queue_item_invalid")
        issue_id = str(raw.get("issue_id") or "").strip()
        if ISSUE_ID_RE.fullmatch(issue_id) is None or issue_id in seen:
            raise BatchRerunError("batch_queue_issue_invalid")
        seen.add(issue_id)
        items.append({
            "issue_id": issue_id,
            "title": str(raw.get("title") or "").strip(),
            "quality_classification": str(
                raw.get("quality_classification") or ""
            ).strip(),
            "queue_submission_key": str(
                raw.get("current_submission_key") or ""
            ).strip(),
            "priority": int(raw.get("priority") or 0),
        })
    return sorted(items, key=lambda row: (row["priority"], row["issue_id"])), raw_sha


def _load_or_create_state(
    path: Path,
    *,
    batch_id: str,
    queue_sha256: str,
    runtime_commit: str,
) -> dict[str, Any]:
    if path.exists():
        value, _raw_sha = _read_json(path)
        if not isinstance(value, Mapping):
            raise BatchRerunError("batch_state_schema_invalid")
        state = dict(value)
        if (
            state.get("schema_version") != SCHEMA_VERSION
            or state.get("batch_id") != batch_id
            or state.get("queue_sha256") != queue_sha256
        ):
            raise BatchRerunError("batch_state_binding_mismatch")
        if not isinstance(state.get("items"), Mapping):
            raise BatchRerunError("batch_state_schema_invalid")
        state["items"] = dict(state["items"])
        return state
    current = _now()
    return {
        "schema_version": SCHEMA_VERSION,
        "batch_id": batch_id,
        "queue_sha256": queue_sha256,
        "runtime_commit": runtime_commit,
        "created_at": current,
        "updated_at": current,
        "status": "running",
        "items": {},
    }


def _json_object(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, str) or not raw:
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return dict(value) if isinstance(value, Mapping) else {}


def _issue_snapshot(
    db_path: Path, issue_id: str, *, submission_key: str = ""
) -> dict[str, Any] | None:
    conn = sqlite3.connect(db_path, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        submission_filter = ""
        params: tuple[str, ...] = (issue_id,)
        if submission_key:
            submission_filter = " AND b.submission_key = ?"
            params = (issue_id, submission_key)
        row = conn.execute(
            f"""
            SELECT b.generation, b.submission_key,
                   o.status AS outbox_status,
                   o.last_error_code AS outbox_error_code,
                   o.last_error_detail AS outbox_error_detail,
                   o.completed_at AS outbox_completed_at,
                   j.delivery_id, j.status AS job_status,
                   j.outcome AS job_outcome, j.outcome_key,
                   j.terminal_state, j.terminal_error_code,
                   j.issue_url, j.report_url, j.manifest_json,
                   j.contract_json, j.artifacts_json,
                   j.updated_at AS job_updated_at
              FROM business_triggers AS b
              JOIN rca_outbox AS o
                ON o.business_key = b.business_key
               AND o.generation = b.generation
         LEFT JOIN rca_execution_watch AS w
                ON w.submission_outbox_id = o.outbox_id
         LEFT JOIN rca_delivery_jobs AS j
                ON j.submission_key = b.submission_key
             WHERE b.work_item_id = ?{submission_filter}
             ORDER BY b.generation DESC
             LIMIT 1
            """,
            params,
        ).fetchone()
        if row is None:
            return None
        snapshot = {key: row[key] for key in row.keys()}
        effects: list[dict[str, Any]] = []
        if row["delivery_id"]:
            for effect in conn.execute(
                """
                SELECT effect_key, effect_kind, required, target_key, status,
                       remote_receipt_json, last_error_code, completed_at,
                       updated_at
                  FROM rca_delivery_effects
                 WHERE delivery_id = ?
                 ORDER BY effect_kind, effect_key
                """,
                (row["delivery_id"],),
            ).fetchall():
                item = {key: effect[key] for key in effect.keys()}
                item["remote_receipt"] = _json_object(item.pop("remote_receipt_json"))
                effects.append(item)
        snapshot["effects"] = effects
        return snapshot
    finally:
        conn.close()


def _approval(snapshot: Mapping[str, Any]) -> dict[str, Any] | None:
    if snapshot.get("job_status") != "delivered":
        return None
    if snapshot.get("job_outcome") != "success":
        return None
    required = [
        effect
        for effect in snapshot.get("effects", [])
        if isinstance(effect, Mapping) and int(effect.get("required") or 0) == 1
    ]
    issue_effects = [
        effect
        for effect in required
        if effect.get("effect_kind") == "feishu_issue_comment"
    ]
    if len(issue_effects) != 1 or any(
        effect.get("status") != "succeeded" for effect in required
    ):
        return None
    receipt = dict(issue_effects[0].get("remote_receipt") or {})
    field_keys = sorted(str(value) for value in receipt.get("confirmed_field_keys", []))
    if not receipt.get("remote_id") or field_keys != ["field_8c912e", "field_9193cb"]:
        return None
    return {
        "generation": int(snapshot["generation"]),
        "submission_key": str(snapshot["submission_key"]),
        "delivery_id": str(snapshot["delivery_id"]),
        "outcome_key": str(snapshot.get("outcome_key") or ""),
        "issue_url": str(snapshot.get("issue_url") or ""),
        "report_url": str(snapshot.get("report_url") or ""),
        "official_comment_id": str(receipt["remote_id"]),
        "official_field_keys": field_keys,
        "official_readback_source": str(receipt.get("source") or ""),
        "manifest": _json_object(snapshot.get("manifest_json")),
        "artifacts": _json_object(snapshot.get("artifacts_json")),
        "completed_at": str(issue_effects[0].get("completed_at") or ""),
    }


def _terminal_failure(snapshot: Mapping[str, Any]) -> dict[str, Any] | None:
    effects = [
        effect
        for effect in snapshot.get("effects", [])
        if isinstance(effect, Mapping) and int(effect.get("required") or 0) == 1
    ]
    if any(effect.get("status") in ACTIVE_EFFECT_STATUSES for effect in effects):
        return None
    job_status = str(snapshot.get("job_status") or "")
    if job_status in TERMINAL_JOB_STATUSES and _approval(snapshot) is None:
        return {
            "job_status": job_status,
            "job_outcome": str(snapshot.get("job_outcome") or ""),
            "outcome_key": str(snapshot.get("outcome_key") or ""),
            "terminal_state": str(snapshot.get("terminal_state") or ""),
            "terminal_error_code": str(snapshot.get("terminal_error_code") or ""),
            "effects": [
                {
                    "effect_kind": str(effect.get("effect_kind") or ""),
                    "status": str(effect.get("status") or ""),
                    "error_code": str(effect.get("last_error_code") or ""),
                }
                for effect in effects
            ],
        }
    if snapshot.get("outbox_status") == "quarantined" and not job_status:
        return {
            "job_status": "",
            "job_outcome": "",
            "outcome_key": "",
            "terminal_state": "outbox_quarantined",
            "terminal_error_code": str(
                snapshot.get("outbox_error_code") or "outbox_quarantined"
            ),
            "effects": [],
        }
    return None


def _request(
    *, batch_id: str, issue_id: str, request_index: int, requester_id: str
) -> ManualRcaTriggerRequest:
    return ManualRcaTriggerRequest(
        schema_version=MANUAL_TRIGGER_SCHEMA_VERSION,
        issue_url=f"https://project.feishu.cn/t03o4q/issue/detail/{issue_id}",
        mode="rerun",
        reason=f"production_gray_batch:{batch_id}",
        platform="operator",
        chat_id="",
        thread_id="",
        message_id=f"{batch_id}-{issue_id}-try-{request_index}",
        requester_id=requester_id,
    )


def run(args: argparse.Namespace) -> dict[str, Any]:
    batch_id = str(args.batch_id or "").strip()
    if BATCH_ID_RE.fullmatch(batch_id) is None:
        raise BatchRerunError("batch_id_invalid")
    runtime_commit = _runtime_commit()
    if runtime_commit != str(args.expected_runtime_commit or "").strip():
        raise BatchRerunError("batch_runtime_commit_mismatch")
    queue, queue_sha = _load_queue(Path(args.queue))
    state_path = Path(args.state)
    state = _load_or_create_state(
        state_path,
        batch_id=batch_id,
        queue_sha256=queue_sha,
        runtime_commit=runtime_commit,
    )
    store = RcaControlStore(Path(args.control_db))
    completed = 0
    for queue_item in queue:
        issue_id = queue_item["issue_id"]
        item = dict(state["items"].get(issue_id) or {})
        latest = _issue_snapshot(Path(args.control_db), issue_id)
        if latest is None:
            raise BatchRerunError("batch_issue_scope_missing")
        accepted = _approval(latest)
        queue_submission = queue_item["queue_submission_key"]
        if accepted is not None and (
            item.get("status") == "accepted"
            or str(latest["submission_key"]) != queue_submission
        ):
            item.update({
                **queue_item,
                "status": "accepted",
                "approval": accepted,
                "updated_at": _now(),
            })
            state["items"][issue_id] = item
            state["updated_at"] = _now()
            _write_state(state_path, state)
            completed += 1
            continue
        if item.get("status") == "failed" and not args.retry_failed:
            raise BatchRerunError("batch_failed_item_requires_retry_flag")
        request_index = int(item.get("request_index") or 0)
        if item.get("status") not in {"submitted", "running"}:
            request_index += 1
            admitted = store.admit_manual_trigger(
                _request(
                    batch_id=batch_id,
                    issue_id=issue_id,
                    request_index=request_index,
                    requester_id=args.requester_id,
                ),
                allowed_chat_ids=set(),
                submit_enabled=True,
                operator_authorized=True,
                activation_required=False,
            )
            item.update({
                **queue_item,
                "status": "submitted",
                "request_index": request_index,
                "generation": admitted.generation,
                "submission_key": admitted.submission_key,
                "source_id": admitted.source_id,
                "submitted_at": _now(),
                "updated_at": _now(),
            })
            state["items"][issue_id] = item
            state["updated_at"] = _now()
            _write_state(state_path, state)
        deadline = time.monotonic() + args.item_timeout_seconds
        while True:
            current = _issue_snapshot(Path(args.control_db), issue_id)
            if current is None or int(current["generation"]) > int(item["generation"]):
                raise BatchRerunError("batch_issue_generation_drift")
            latest = _issue_snapshot(
                Path(args.control_db),
                issue_id,
                submission_key=str(item["submission_key"]),
            )
            if latest is None:
                raise BatchRerunError("batch_issue_generation_drift")
            accepted = _approval(latest)
            if accepted is not None:
                item.update({
                    "status": "accepted",
                    "approval": accepted,
                    "updated_at": _now(),
                })
                state["items"][issue_id] = item
                state["updated_at"] = _now()
                _write_state(state_path, state)
                completed += 1
                break
            failure = _terminal_failure(latest)
            if failure is not None:
                item.update({
                    "status": "failed",
                    "failure": failure,
                    "updated_at": _now(),
                })
                state["items"][issue_id] = item
                state["status"] = "blocked_on_item_failure"
                state["updated_at"] = _now()
                _write_state(state_path, state)
                raise BatchRerunError("batch_item_terminal_failure")
            if time.monotonic() >= deadline:
                item.update({"status": "running", "updated_at": _now()})
                state["items"][issue_id] = item
                state["status"] = "running"
                state["updated_at"] = _now()
                _write_state(state_path, state)
                raise BatchRerunError("batch_item_timeout")
            item["status"] = "running"
            item["updated_at"] = _now()
            state["items"][issue_id] = item
            state["updated_at"] = _now()
            _write_state(state_path, state)
            time.sleep(args.poll_seconds)
    state["status"] = "completed"
    state["completed_at"] = _now()
    state["updated_at"] = state["completed_at"]
    state["summary"] = {"accepted": completed, "total": len(queue)}
    _write_state(state_path, state)
    return state


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--control-db", required=True)
    parser.add_argument("--queue", required=True)
    parser.add_argument("--state", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--expected-runtime-commit", required=True)
    parser.add_argument("--requester-id", default="operator-songying")
    parser.add_argument("--poll-seconds", type=int, default=5)
    parser.add_argument("--item-timeout-seconds", type=int, default=7200)
    parser.add_argument("--retry-failed", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
        if args.poll_seconds < 1 or args.item_timeout_seconds < 30:
            raise BatchRerunError("batch_poll_config_invalid")
        result = run(args)
        print(_canonical_json(result))
        return 0
    except BatchRerunError as exc:
        print(_canonical_json({"ok": False, "error_code": exc.code}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
