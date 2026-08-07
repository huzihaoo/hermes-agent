#!/usr/bin/env python3
"""Build an exact G1Q3/creator refresh queue from a read-only control snapshot."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
from typing import Any, Mapping, Sequence
from urllib.parse import quote

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.pnc_rca_batch_rerun import (  # noqa: E402
    BATCH_ID_RE,
    ISSUE_ID_RE,
    QUEUE_AUTHORITY_FLAGS,
    QUEUE_QUALITY_CLASSIFICATIONS,
    QUEUE_SCHEMA_VERSION,
    QUEUE_SCOPE,
)


SOURCE_SCHEMA_VERSION = "pnc_rca_batch_queue_v1"
RECEIPT_SCHEMA_VERSION = "g1q3_rca_exact_refresh_queue_receipt_v1"
MAX_INPUT_BYTES = 4 * 1024 * 1024
SOURCE_FIELDS = frozenset(
    {"schema_version", "source_inventory_sha256", "filter", "items"}
)
SOURCE_ITEM_FIELDS = frozenset(
    {
        "issue_id",
        "title",
        "quality_classification",
        "current_submission_key",
        "priority",
    }
)
SOURCE_FILTER = {
    "logic": "AND",
    "created_by": QUEUE_SCOPE["creator_key"],
    "creator_name": QUEUE_SCOPE["creator_name"],
    "project_field_key": "field_052f23",
    "project_id": int(QUEUE_SCOPE["project_relation_id"]),
    "project_name": QUEUE_SCOPE["project_name"],
}
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SUBMISSION_RE = re.compile(r"^g1q3-rca-s1-[0-9a-f]{64}$")


class ExactRefreshQueueError(RuntimeError):
    def __init__(self, code: str):
        self.code = str(code or "exact_refresh_queue_failed")[:160]
        super().__init__(self.code)


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


def _read_source(path: Path) -> tuple[list[dict[str, Any]], str]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExactRefreshQueueError("exact_refresh_source_unreadable") from exc
    if not raw or len(raw) > MAX_INPUT_BYTES or not isinstance(value, Mapping):
        raise ExactRefreshQueueError("exact_refresh_source_invalid")
    if (
        set(value) != SOURCE_FIELDS
        or value.get("schema_version") != SOURCE_SCHEMA_VERSION
        or value.get("filter") != SOURCE_FILTER
        or _SHA256_RE.fullmatch(
            str(value.get("source_inventory_sha256") or "").strip().lower()
        )
        is None
        or value.get("source_inventory_sha256") == "0" * 64
        or not isinstance(value.get("items"), list)
    ):
        raise ExactRefreshQueueError("exact_refresh_source_contract_invalid")
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in value["items"]:
        if not isinstance(item, Mapping) or set(item) != SOURCE_ITEM_FIELDS:
            raise ExactRefreshQueueError("exact_refresh_source_item_invalid")
        if not all(isinstance(item.get(field), str) for field in (
            "issue_id",
            "title",
            "quality_classification",
            "current_submission_key",
        )):
            raise ExactRefreshQueueError("exact_refresh_source_item_invalid")
        issue_id = str(item.get("issue_id") or "").strip()
        title = str(item.get("title") or "").strip()
        classification = str(item.get("quality_classification") or "").strip()
        priority = item.get("priority")
        if (
            ISSUE_ID_RE.fullmatch(issue_id) is None
            or issue_id in seen
            or not title
            or classification not in QUEUE_QUALITY_CLASSIFICATIONS
            or str(item.get("current_submission_key") or "").strip()
            or isinstance(priority, bool)
            or not isinstance(priority, int)
            or priority < 0
        ):
            raise ExactRefreshQueueError("exact_refresh_source_item_invalid")
        seen.add(issue_id)
        items.append({
            "issue_id": issue_id,
            "title": title,
            "quality_classification": classification,
            "priority": priority,
        })
    if not items:
        raise ExactRefreshQueueError("exact_refresh_source_empty")
    return items, str(value["source_inventory_sha256"])


def _latest_control_rows(
    db_path: Path, issue_ids: Sequence[str]
) -> tuple[dict[str, tuple[int, str]], dict[str, Any]]:
    selected = db_path.expanduser().absolute()
    try:
        before = selected.stat()
    except OSError as exc:
        raise ExactRefreshQueueError("exact_refresh_control_db_unavailable") from exc
    uri = f"file:{quote(str(selected), safe='/')}?mode=ro"
    rows: dict[str, tuple[int, str]] = {}
    try:
        conn = sqlite3.connect(uri, uri=True, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only = ON")
        conn.execute("BEGIN")
        for offset in range(0, len(issue_ids), 200):
            chunk = list(issue_ids[offset : offset + 200])
            placeholders = ",".join("?" for _ in chunk)
            observed = conn.execute(
                "SELECT work_item_id, generation, submission_key, created_at "
                "FROM business_triggers WHERE work_item_id IN ("
                + placeholders
                + ") ORDER BY work_item_id, generation DESC, created_at DESC",
                chunk,
            ).fetchall()
            for row in observed:
                issue_id = str(row["work_item_id"] or "")
                generation = int(row["generation"] or 0)
                submission_key = str(row["submission_key"] or "")
                current = rows.get(issue_id)
                if current is None:
                    if generation < 1 or _SUBMISSION_RE.fullmatch(submission_key) is None:
                        raise ExactRefreshQueueError(
                            "exact_refresh_control_identity_invalid"
                        )
                    rows[issue_id] = (generation, submission_key)
                elif current[0] == generation and current[1] != submission_key:
                    raise ExactRefreshQueueError("exact_refresh_control_scope_ambiguous")
        conn.rollback()
    except sqlite3.Error as exc:
        raise ExactRefreshQueueError("exact_refresh_control_query_failed") from exc
    finally:
        if "conn" in locals():
            conn.close()
    return rows, {
        "path": str(selected),
        "device": int(before.st_dev),
        "inode": int(before.st_ino),
        "size": int(before.st_size),
        "mtime_ns": int(before.st_mtime_ns),
    }


def build_queue(
    *, source_path: Path, control_db: Path, batch_id: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    if BATCH_ID_RE.fullmatch(str(batch_id or "").strip()) is None:
        raise ExactRefreshQueueError("exact_refresh_batch_id_invalid")
    source, source_inventory_sha256 = _read_source(source_path)
    issue_ids = [item["issue_id"] for item in source]
    latest, db_identity = _latest_control_rows(control_db, issue_ids)
    output_items = []
    for item in source:
        generation, submission_key = latest.get(item["issue_id"], (0, ""))
        output_items.append({
            **item,
            "current_submission_key": submission_key,
            "current_generation": generation,
            "project_key": "t03o4q",
        })
    issue_ids_sha256 = _sha256_bytes(
        ("\n".join(sorted(issue_ids)) + "\n").encode("utf-8")
    )
    queue = {
        "schema_version": QUEUE_SCHEMA_VERSION,
        "batch_id": batch_id,
        "project_key": "t03o4q",
        "scope": {
            **QUEUE_SCOPE,
            "issue_count": len(output_items),
            "issue_ids_sha256": issue_ids_sha256,
        },
        "source_inventory_sha256": source_inventory_sha256,
        "authority_flags": dict(QUEUE_AUTHORITY_FLAGS),
        "items": output_items,
    }
    receipt = {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "batch_id": batch_id,
        "source_path": str(source_path.expanduser().absolute()),
        "source_inventory_sha256": source_inventory_sha256,
        "scope": dict(queue["scope"]),
        "control_db": db_identity,
        "counts": {
            "total": len(output_items),
            "existing": len(latest),
            "absent": len(output_items) - len(latest),
        },
        "production_effects": {
            "control_db_writes": 0,
            "feishu_writes": 0,
            "kafka_commits": 0,
            "production_submissions": 0,
        },
    }
    return queue, receipt


def _write_new(path: Path, value: Mapping[str, Any]) -> str:
    selected = path.expanduser().absolute()
    selected.parent.mkdir(parents=True, exist_ok=True)
    raw = (_canonical_json(value) + "\n").encode("utf-8")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    try:
        fd = os.open(selected, flags, 0o600)
    except OSError as exc:
        raise ExactRefreshQueueError("exact_refresh_output_exists_or_unwritable") from exc
    with os.fdopen(fd, "wb", closefd=True) as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    return _sha256_bytes(raw)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True)
    parser.add_argument("--control-db", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--apply", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
        queue, receipt = build_queue(
            source_path=Path(args.source),
            control_db=Path(args.control_db),
            batch_id=args.batch_id,
        )
        if not args.apply:
            raise ExactRefreshQueueError("exact_refresh_apply_required")
        output = Path(args.output).expanduser().absolute()
        queue_sha256 = _write_new(output, queue)
        receipt = {
            **receipt,
            "queue_path": str(output),
            "queue_sha256": queue_sha256,
        }
        receipt_path = output.with_suffix(output.suffix + ".receipt.json")
        receipt_sha256 = _write_new(receipt_path, receipt)
        print(_canonical_json({
            "ok": True,
            "queue_path": str(output),
            "queue_sha256": queue_sha256,
            "receipt_path": str(receipt_path),
            "receipt_sha256": receipt_sha256,
            "counts": receipt["counts"],
        }))
        return 0
    except ExactRefreshQueueError as exc:
        print(_canonical_json({"ok": False, "error_code": exc.code}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
