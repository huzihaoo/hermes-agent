#!/usr/bin/env python3
"""Build and operate the pre-release G1Q3 RCA learning corpus.

The corpus is intentionally split into blind inputs and owner truth. Production
RCA never imports this module or reads the truth artifact. The append-only
ledger enforces predict-before-reveal during offline evaluator development.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from gateway.pnc_issue_context import sanitize_issue_evidence_text
from gateway.pnc_rca_data_access import (
    RemoteDataAccessError,
    build_remote_data_access,
)


SCHEMA_VERSION = "g1q3_rca_prerelease_learning_v1"
LEDGER_SCHEMA_VERSION = "g1q3_rca_prerelease_ledger_v1"
PROJECT_KEY = "t03o4q"
WORK_ITEM_TYPE = "【08】问题管理"
PAGE_SIZE = 50
MAX_COMMENT_PAGES = 50
MAX_OPERATION_PAGES = 50
MAX_TEXT_CHARS = 6000
SHANGHAI = ZoneInfo("Asia/Shanghai")

TARGET_STATUSES = {
    "o93u2k3ri": "转验证（开发转测试）",
    "1OhRnCBmH": "验证中（In Testing）",
    "CLOSED": "关闭（CLOSED）",
}
VALIDATION_STATUS_KEYS = frozenset({"o93u2k3ri", "1OhRnCBmH"})
TARGET_STATUS_LABELS = tuple(TARGET_STATUSES.values())

DETAIL_FIELD_KEYS = (
    "description",
    "field_1fda45",  # 问题发生 frame_id
    "field_93aa63",  # 问题数据地址_PDCL
    "field_e776bb",  # 问题所属功能类别
    "field_842fc8",  # 问题根本原因分析 (truth only)
)

_CAUSAL_TERMS = (
    "原因分析", "根因", "原因为", "问题在", "问题是", "导致", "异常",
    "误检", "漏检", "跳变", "抖动", "收敛", "初始化", "选错", "误选",
    "晚选", "未选中", "触发", "控制", "感知", "规划", "spp", "ooi",
    "cipv", "ttc", "vx", "vy", "置信度", "车道线", "回灌", "修复",
    "优化", "符合预期", "正触发", "works as designed",
)
_CORRECTION_TERMS = (
    "更正", "修正", "实际原因", "最终原因", "确认原因", "根因确认", "经确认",
    "不是", "而是", "更新结论",
)
_AUTOMATION_MARKERS = (
    "[RCA_DELIVERY:", "自动RCA未归因", "自动 RCA 未归因",
    "本评论由 RCA", "诊断终态报告",
)
_SAFE_ID_RE = re.compile(r"^[0-9]{6,24}$")
_ISO_DATE_RE = re.compile(r"^20\d{2}-\d{2}-\d{2}$")


class CorpusError(RuntimeError):
    """Stable pre-release corpus failure."""

    def __init__(self, code: str, detail: str = ""):
        super().__init__(detail or code)
        self.code = code
        self.detail = detail or code


def canonical_json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise CorpusError("canonical_json_invalid") from exc


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(canonical_json_bytes(value) + b"\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def _append_jsonl(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
    fd = os.open(path, flags, 0o600)
    try:
        with os.fdopen(fd, "ab", closefd=False) as handle:
            handle.write(canonical_json_bytes(value) + b"\n")
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        os.close(fd)


def _unwrap_envelope(value: Any) -> Any:
    if (
        isinstance(value, Mapping)
        and "error" in value
        and "meta" in value
        and "data" in value
    ):
        if value.get("error"):
            raise CorpusError("meegle_response_error", sha256_json(value["error"]))
        return value.get("data")
    return value


def _field_value(field: Mapping[str, Any]) -> Any:
    raw = field.get("value")
    if not isinstance(raw, Mapping):
        return None
    for key in ("string_value", "long_value", "double_value", "bool_value"):
        if key in raw:
            return raw.get(key)
    if "key_label_value_list" in raw:
        rows = raw.get("key_label_value_list")
        return [
            {"key": str(row.get("key") or ""), "label": str(row.get("label") or "")}
            for row in rows or []
            if isinstance(row, Mapping)
        ]
    if "key_label_value" in raw:
        row = raw.get("key_label_value") or {}
        return {
            "key": str(row.get("key") or ""),
            "label": str(row.get("label") or ""),
        }
    if "cascade_key_label_value" in raw:
        labels: list[str] = []
        row = raw.get("cascade_key_label_value")
        while isinstance(row, Mapping) and row:
            label = str(row.get("label") or row.get("key") or "").strip()
            if label:
                labels.append(label)
            children = row.get("children")
            row = children[0] if isinstance(children, list) and children else None
        return "/".join(labels)
    return None


def normalize_query_rows(payload: Any) -> tuple[list[dict[str, Any]], int]:
    body = _unwrap_envelope(payload)
    if not isinstance(body, Mapping):
        raise CorpusError("meegle_query_invalid")
    data = body.get("data")
    rows: list[dict[str, Any]] = []
    if isinstance(data, Mapping):
        for group in data.values():
            for item in group if isinstance(group, list) else []:
                fields = item.get("moql_field_list") if isinstance(item, Mapping) else None
                if not isinstance(fields, list):
                    continue
                normalized = {
                    str(field.get("key") or ""): _field_value(field)
                    for field in fields
                    if isinstance(field, Mapping) and field.get("key")
                }
                work_item_id = str(normalized.get("work_item_id") or "").strip()
                status_rows = normalized.get("work_item_status") or []
                status = status_rows[0] if isinstance(status_rows, list) and status_rows else {}
                if not _SAFE_ID_RE.fullmatch(work_item_id) or not isinstance(status, Mapping):
                    continue
                rows.append({
                    "work_item_id": work_item_id,
                    "title": sanitize_issue_evidence_text(normalized.get("name"))[:1000],
                    "status_key": str(status.get("key") or ""),
                    "status_label": str(status.get("label") or ""),
                    "updated_at": str(normalized.get("updated_at") or ""),
                })
    total = 0
    for item in body.get("list") or []:
        if isinstance(item, Mapping) and isinstance(item.get("count"), int):
            total = max(total, int(item["count"]))
    return rows, total


def _compact_comment(value: Any) -> str:
    text = sanitize_issue_evidence_text(value)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text[:MAX_TEXT_CHARS]


def _actor_hash(value: Any) -> str:
    text = str(value or "").strip()
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16] if text else ""


def normalize_comments(payload: Any) -> tuple[list[dict[str, Any]], dict[str, int]]:
    body = _unwrap_envelope(payload)
    if not isinstance(body, Mapping):
        raise CorpusError("meegle_comments_invalid")
    raw: Any = None
    for key in ("comments", "items", "list"):
        if isinstance(body.get(key), list):
            raw = body[key]
            break
    if not isinstance(raw, list):
        raise CorpusError("meegle_comments_invalid")
    comments: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, Mapping):
            continue
        text = _compact_comment(
            item.get("content") or item.get("text") or item.get("body") or ""
        )
        if not text:
            continue
        comment_id = str(item.get("comment_id") or item.get("id") or "").strip()
        comments.append({
            "comment_ref": hashlib.sha256(comment_id.encode("utf-8")).hexdigest()[:16]
            if comment_id else sha256_json([item.get("created_at"), text])[:16],
            "created_at": str(
                item.get("created_at") or item.get("create_time") or item.get("time") or ""
            ),
            "actor_hash": _actor_hash(item.get("creator") or item.get("author")),
            "content": text,
            "content_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
        })
    pagination = body.get("pagination") if isinstance(body.get("pagination"), Mapping) else {}
    page_num = pagination.get("page_num")
    total_pages = pagination.get("total_pages")
    return comments, {
        "page_num": int(page_num) if isinstance(page_num, int) and page_num > 0 else 1,
        "total_pages": (
            int(total_pages)
            if isinstance(total_pages, int) and total_pages >= 0
            else 1
        ),
    }


def normalize_status_transitions(payload: Any) -> tuple[list[dict[str, Any]], str]:
    body = _unwrap_envelope(payload)
    if not isinstance(body, Mapping):
        raise CorpusError("meegle_operations_invalid")
    transitions: list[dict[str, Any]] = []
    for record in body.get("op_records") or []:
        if not isinstance(record, Mapping):
            continue
        timestamp_ms = record.get("operation_time")
        if isinstance(timestamp_ms, bool) or not isinstance(timestamp_ms, int):
            continue
        for content in record.get("record_contents") or []:
            if not isinstance(content, Mapping):
                continue
            obj = content.get("object") if isinstance(content.get("object"), Mapping) else {}
            if (
                obj.get("object_value") != "work_item_status"
                and content.get("object_property") != "workitem_status"
            ):
                continue
            old_values = [str(item) for item in content.get("old") or []]
            new_values = [str(item) for item in content.get("new") or []]
            if not new_values:
                continue
            new_key = new_values[-1]
            old_key = old_values[-1] if old_values else ""
            observed = datetime.fromtimestamp(timestamp_ms / 1000, timezone.utc)
            transitions.append({
                "occurred_at": observed.isoformat().replace("+00:00", "Z"),
                "occurred_at_ms": timestamp_ms,
                "old_status_key": old_key,
                "new_status_key": new_key,
                "new_status_label": TARGET_STATUSES.get(new_key, ""),
                "actor_hash": _actor_hash(record.get("operator")),
            })
    return transitions, str(body.get("start_from") or "") if body.get("has_more") else ""


def _parse_comment_time(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    candidates = (text, text.replace("Z", "+00:00"))
    for candidate in candidates:
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=SHANGHAI)
        return parsed.astimezone(timezone.utc)
    return None


def causal_comment_score(text: str) -> int:
    normalized = str(text or "").strip()
    if not normalized or any(marker in normalized for marker in _AUTOMATION_MARKERS):
        return -100
    lowered = normalized.lower()
    score = sum(2 for term in _CAUSAL_TERMS if term.lower() in lowered)
    if re.search(r"(?:frame|帧)\s*(?:约|[:：=])?\s*\d+", lowered):
        score += 2
    if re.search(r"-?\d+(?:\.\d+)?\s*(?:m/s|mps|km/h|kph|s|秒|帧|%)", lowered):
        score += 2
    if "原因" in normalized:
        score += 3
    if any(term in normalized for term in _CORRECTION_TERMS):
        score += 3
    return score


def align_owner_truth(
    comments: Sequence[Mapping[str, Any]],
    transitions: Sequence[Mapping[str, Any]],
    *,
    root_cause_text: str = "",
) -> dict[str, Any]:
    timeline = []
    for raw in comments:
        parsed = _parse_comment_time(raw.get("created_at"))
        text = str(raw.get("content") or "").strip()
        score = causal_comment_score(text)
        if parsed is None or score < 2:
            continue
        timeline.append({**dict(raw), "_time": parsed, "causal_score": score})
    timeline.sort(key=lambda item: item["_time"])

    normalized_transitions = []
    for raw in transitions:
        parsed = _parse_comment_time(raw.get("occurred_at"))
        if parsed is not None:
            normalized_transitions.append({**dict(raw), "_time": parsed})
    normalized_transitions.sort(key=lambda item: item["_time"])
    validation = [
        item for item in normalized_transitions
        if item.get("new_status_key") in VALIDATION_STATUS_KEYS
    ]
    closure = [
        item for item in normalized_transitions
        if item.get("new_status_key") == "CLOSED"
    ]
    anchor = validation[-1] if validation else closure[-1] if closure else None

    selected: Mapping[str, Any] | None = None
    selection_reason = ""
    if anchor and timeline:
        before = [
            item for item in timeline
            if timedelta(0) <= anchor["_time"] - item["_time"] <= timedelta(days=14)
        ]
        if before:
            selected = before[-1]
            selection_reason = "last_causal_comment_before_validation_transition"
        else:
            after = [
                item for item in timeline
                if timedelta(0) <= item["_time"] - anchor["_time"] <= timedelta(hours=4)
            ]
            if after:
                selected = after[0]
                selection_reason = "first_causal_comment_after_validation_transition"
    if selected is None and timeline:
        selected = timeline[-1]
        selection_reason = "latest_causal_comment_fallback"

    if selected is not None:
        later_corrections = [
            item for item in timeline
            if item["_time"] > selected["_time"]
            and any(term in str(item.get("content") or "") for term in _CORRECTION_TERMS)
        ]
        if later_corrections:
            selected = later_corrections[-1]
            selection_reason = "later_explicit_owner_correction"

    public_timeline = [
        {
            key: value for key, value in item.items()
            if key != "_time"
        }
        for item in timeline
    ]
    selected_public = None
    if selected is not None:
        selected_public = {
            key: value for key, value in selected.items()
            if key != "_time"
        }
    anchor_public = None
    if anchor is not None:
        anchor_public = {
            key: value for key, value in anchor.items()
            if key != "_time"
        }
    root_cause = _compact_comment(root_cause_text)
    return {
        "status": "resolved" if selected_public or root_cause else "unresolved",
        "selection_reason": selection_reason or (
            "root_cause_field_only" if root_cause else "no_causal_owner_text"
        ),
        "validation_transition": anchor_public,
        "selected_comment": selected_public,
        "causal_comment_timeline": public_timeline,
        "root_cause_field_secondary": root_cause,
        "policy": (
            "owner comments are offline supervision only; they must never drive "
            "production RCA inference"
        ),
    }


def _safe_data_access(raw: Any) -> tuple[dict[str, Any] | None, str]:
    text = str(raw or "").strip()
    if not text:
        return None, "missing"
    try:
        access = build_remote_data_access(text)
    except RemoteDataAccessError as exc:
        return None, str(exc.code)
    return access, "valid"


def split_case(
    index: Mapping[str, Any],
    detail: Mapping[str, Any],
    comments: Sequence[Mapping[str, Any]],
    transitions: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    work_item_id = str(index.get("work_item_id") or "")
    if not _SAFE_ID_RE.fullmatch(work_item_id):
        raise CorpusError("work_item_id_invalid")
    data_access, data_status = _safe_data_access(detail.get("pdcl_data"))
    blind = {
        "work_item_id": work_item_id,
        "title": sanitize_issue_evidence_text(index.get("title"))[:1000],
        "status_at_capture": {
            "key": str(index.get("status_key") or ""),
            "label": str(index.get("status_label") or ""),
        },
        "updated_at": str(index.get("updated_at") or ""),
        "description": _compact_comment(detail.get("description")),
        "function_category": sanitize_issue_evidence_text(
            detail.get("function_category")
        )[:500],
        "frame_reference": sanitize_issue_evidence_text(
            detail.get("frame_reference")
        )[:200],
        "remote_data_access": data_access,
        "remote_data_access_status": data_status,
        "source": "official_meegle_read_only",
    }
    truth = {
        "work_item_id": work_item_id,
        "owner_truth": align_owner_truth(
            comments,
            transitions,
            root_cause_text=str(detail.get("root_cause_text") or ""),
        ),
    }
    if "owner_truth" in blind or "root_cause_text" in blind or "comments" in blind:
        raise CorpusError("blind_truth_leak")
    return blind, truth


@dataclass
class MeegleReadClient:
    timeout_seconds: float = 30.0
    retries: int = 3

    def _json(self, args: Sequence[str]) -> dict[str, Any]:
        executable = shutil.which("meegle")
        if not executable:
            raise CorpusError("meegle_cli_missing")
        last_error = ""
        for attempt in range(self.retries):
            try:
                completed = subprocess.run(
                    [executable, *args],
                    capture_output=True,
                    check=False,
                    text=True,
                    timeout=self.timeout_seconds,
                    env={**os.environ, "MEEGLE_HOST": "project.feishu.cn"},
                )
            except subprocess.TimeoutExpired:
                last_error = "timeout"
            else:
                if completed.returncode == 0 and completed.stdout.strip():
                    try:
                        body = json.loads(completed.stdout)
                    except json.JSONDecodeError:
                        last_error = "invalid_json"
                    else:
                        if isinstance(body, dict):
                            _unwrap_envelope(body)
                            return body
                        last_error = "non_object_json"
                else:
                    last_error = (
                        f"rc={completed.returncode}:"
                        + hashlib.sha256(
                            str(completed.stderr or completed.stdout).encode("utf-8")
                        ).hexdigest()
                    )
            if attempt + 1 < self.retries:
                time.sleep(0.5 * (2 ** attempt))
        raise CorpusError("meegle_read_failed", last_error)

    def query_status(self, status_label: str, *, offset: int) -> tuple[list[dict[str, Any]], int]:
        if status_label not in TARGET_STATUS_LABELS:
            raise CorpusError("target_status_invalid")
        mql = (
            "SELECT `名称`, `工作项id`, `状态`, `更新时间` "
            f"FROM `{PROJECT_KEY}`.`{WORK_ITEM_TYPE}` "
            "WHERE `名称` like '%G1Q3%' "
            f"AND `状态` = '{status_label}' "
            "ORDER BY `更新时间` DESC, `工作项id` DESC "
            f"LIMIT {offset},{PAGE_SIZE}"
        )
        body = self._json((
            "workitem", "query", "--project-key", PROJECT_KEY,
            "--mql", mql, "--envelope", "--format", "json",
        ))
        return normalize_query_rows(body)

    def detail(self, work_item_id: str) -> dict[str, Any]:
        args = [
            "workitem", "get", "--project-key", PROJECT_KEY,
            "--work-item-id", work_item_id,
        ]
        for field in DETAIL_FIELD_KEYS:
            args.extend(("--fields", field))
        args.extend(("--format", "json"))
        body = _unwrap_envelope(self._json(tuple(args)))
        if not isinstance(body, Mapping):
            raise CorpusError("meegle_detail_invalid")
        fields = {
            str(item.get("key") or ""): item.get("value")
            for item in body.get("work_item_fields") or []
            if isinstance(item, Mapping)
        }
        return {
            "description": fields.get("description") or "",
            "frame_reference": fields.get("field_1fda45") or "",
            "pdcl_data": fields.get("field_93aa63") or "",
            "function_category": fields.get("field_e776bb") or "",
            "root_cause_text": fields.get("field_842fc8") or "",
        }

    def comments(self, work_item_id: str) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        seen: set[str] = set()
        for page in range(1, MAX_COMMENT_PAGES + 1):
            body = self._json((
                "comment", "list", "--project-key", PROJECT_KEY,
                "--work-item-id", work_item_id, "--page-num", str(page),
                "--format", "json",
            ))
            rows, pagination = normalize_comments(body)
            for row in rows:
                identity = row["comment_ref"] + ":" + row["content_sha256"]
                if identity not in seen:
                    seen.add(identity)
                    output.append(row)
            if page >= pagination["total_pages"]:
                return sorted(output, key=lambda item: item["created_at"])
        raise CorpusError("meegle_comment_page_limit")

    def transitions_near_comments(
        self,
        work_item_id: str,
        comments: Sequence[Mapping[str, Any]],
        updated_at: str,
    ) -> list[dict[str, Any]]:
        dates = {
            parsed.astimezone(SHANGHAI).date()
            for parsed in (_parse_comment_time(item.get("created_at")) for item in comments)
            if parsed is not None
        }
        if _ISO_DATE_RE.fullmatch(str(updated_at or "")):
            dates.add(datetime.fromisoformat(str(updated_at)).date())
        transitions: list[dict[str, Any]] = []
        seen: set[tuple[Any, ...]] = set()
        for observed_date in sorted(dates):
            start = datetime.combine(
                observed_date - timedelta(days=1), datetime.min.time(), SHANGHAI
            )
            end = start + timedelta(days=3) - timedelta(milliseconds=1)
            start_from = ""
            for _page in range(MAX_OPERATION_PAGES):
                args = [
                    "workitem", "list-op-records", "--project-key", PROJECT_KEY,
                    "--work-item-id", work_item_id,
                    "--op-record-module", "field_mod",
                    "--operation-type", "modify",
                    "--start", str(int(start.timestamp() * 1000)),
                    "--end", str(int(end.timestamp() * 1000)),
                ]
                if start_from:
                    args.extend(("--start-from", start_from))
                args.extend(("--format", "json"))
                rows, next_page = normalize_status_transitions(self._json(tuple(args)))
                for row in rows:
                    identity = (
                        row["occurred_at_ms"], row["old_status_key"], row["new_status_key"]
                    )
                    if identity not in seen:
                        seen.add(identity)
                        transitions.append(row)
                if not next_page:
                    break
                start_from = next_page
            else:
                raise CorpusError("meegle_operation_page_limit")
        return sorted(transitions, key=lambda item: item["occurred_at_ms"])


def capture_case(client: MeegleReadClient, index: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    work_item_id = str(index.get("work_item_id") or "")
    detail = client.detail(work_item_id)
    comments = client.comments(work_item_id)
    transitions = client.transitions_near_comments(
        work_item_id, comments, str(index.get("updated_at") or "")
    )
    return split_case(index, detail, comments, transitions)


def query_target_index(client: MeegleReadClient) -> list[dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for status_label in TARGET_STATUS_LABELS:
        offset = 0
        while True:
            rows, total = client.query_status(status_label, offset=offset)
            for row in rows:
                if "G1Q3" in row["title"].upper():
                    output[row["work_item_id"]] = row
            offset += len(rows)
            if not rows or offset >= total:
                break
    return sorted(
        output.values(),
        key=lambda item: (item.get("updated_at") or "", item["work_item_id"]),
        reverse=True,
    )


def write_corpus(
    output_dir: Path,
    blind_cases: Sequence[Mapping[str, Any]],
    owner_truth: Sequence[Mapping[str, Any]],
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if [item.get("work_item_id") for item in blind_cases] != [
        item.get("work_item_id") for item in owner_truth
    ]:
        raise CorpusError("corpus_identity_order_mismatch")
    generated = generated_at or _utc_now()
    blind_payload = {
        "schema_version": SCHEMA_VERSION,
        "kind": "blind_inputs",
        "generated_at": generated,
        "production_inference_allowed": False,
        "cases": list(blind_cases),
    }
    truth_payload = {
        "schema_version": SCHEMA_VERSION,
        "kind": "owner_truth",
        "generated_at": generated,
        "production_inference_allowed": False,
        "cases": list(owner_truth),
    }
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "kind": "corpus_manifest",
        "generated_at": generated,
        "case_count": len(blind_cases),
        "order": "updated_at_desc_work_item_id_desc",
        "blind_cases_sha256": sha256_json(blind_payload),
        "owner_truth_sha256": sha256_json(truth_payload),
        "policy": {
            "predict_before_reveal": True,
            "online_learning": False,
            "production_truth_access": False,
        },
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(output_dir, 0o700)
    _atomic_json(output_dir / "blind-cases.json", blind_payload)
    _atomic_json(output_dir / "owner-truth.json", truth_payload)
    _atomic_json(output_dir / "manifest.json", manifest)
    ledger_header = {
        "schema_version": LEDGER_SCHEMA_VERSION,
        "record_type": "header",
        "created_at": generated,
        "corpus_manifest_sha256": sha256_json(manifest),
        "previous_record_sha256": "0" * 64,
    }
    ledger_header["record_sha256"] = sha256_json(ledger_header)
    ledger = output_dir / "learning-ledger.jsonl"
    if ledger.exists():
        raise CorpusError("learning_ledger_already_exists")
    _append_jsonl(ledger, ledger_header)
    return manifest


def _ledger_records(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        item = json.loads(line)
        if not isinstance(item, dict):
            raise CorpusError("learning_ledger_invalid")
        records.append(item)
    if not records or records[0].get("record_type") != "header":
        raise CorpusError("learning_ledger_invalid")
    previous = "0" * 64
    for item in records:
        claimed = str(item.get("record_sha256") or "")
        material = {key: value for key, value in item.items() if key != "record_sha256"}
        if item.get("previous_record_sha256") != previous or sha256_json(material) != claimed:
            raise CorpusError("learning_ledger_hash_chain_invalid")
        previous = claimed
    return records


def append_blind_result(
    corpus_dir: Path,
    *,
    work_item_id: str,
    result: Mapping[str, Any],
    evaluator_version: str,
) -> dict[str, Any]:
    blind = json.loads((corpus_dir / "blind-cases.json").read_text(encoding="utf-8"))
    known = {str(item.get("work_item_id")) for item in blind.get("cases") or []}
    if work_item_id not in known:
        raise CorpusError("blind_case_unknown")
    ledger_path = corpus_dir / "learning-ledger.jsonl"
    records = _ledger_records(ledger_path)
    if any(
        item.get("record_type") == "blind_result"
        and item.get("work_item_id") == work_item_id
        for item in records
    ):
        raise CorpusError("blind_result_already_recorded")
    if not evaluator_version.strip():
        raise CorpusError("evaluator_version_missing")
    record = {
        "schema_version": LEDGER_SCHEMA_VERSION,
        "record_type": "blind_result",
        "recorded_at": _utc_now(),
        "work_item_id": work_item_id,
        "evaluator_version": evaluator_version.strip(),
        "result": dict(result),
        "result_sha256": sha256_json(result),
        "previous_record_sha256": records[-1]["record_sha256"],
    }
    record["record_sha256"] = sha256_json(record)
    _append_jsonl(ledger_path, record)
    return record


def reveal_owner_truth(corpus_dir: Path, *, work_item_id: str) -> dict[str, Any]:
    ledger_path = corpus_dir / "learning-ledger.jsonl"
    records = _ledger_records(ledger_path)
    if not any(
        item.get("record_type") == "blind_result"
        and item.get("work_item_id") == work_item_id
        for item in records
    ):
        raise CorpusError("blind_result_required_before_truth")
    truth = json.loads((corpus_dir / "owner-truth.json").read_text(encoding="utf-8"))
    match = next(
        (
            item for item in truth.get("cases") or []
            if str(item.get("work_item_id")) == work_item_id
        ),
        None,
    )
    if not isinstance(match, dict):
        raise CorpusError("owner_truth_missing")
    if not any(
        item.get("record_type") == "truth_reveal"
        and item.get("work_item_id") == work_item_id
        for item in records
    ):
        record = {
            "schema_version": LEDGER_SCHEMA_VERSION,
            "record_type": "truth_reveal",
            "recorded_at": _utc_now(),
            "work_item_id": work_item_id,
            "owner_truth_sha256": sha256_json(match),
            "previous_record_sha256": records[-1]["record_sha256"],
        }
        record["record_sha256"] = sha256_json(record)
        _append_jsonl(ledger_path, record)
    return match


def append_post_reveal_regression(
    corpus_dir: Path,
    *,
    work_item_id: str,
    result: Mapping[str, Any],
    evaluator_version: str,
) -> dict[str, Any]:
    ledger_path = corpus_dir / "learning-ledger.jsonl"
    records = _ledger_records(ledger_path)
    if not any(
        item.get("record_type") == "truth_reveal"
        and item.get("work_item_id") == work_item_id
        for item in records
    ):
        raise CorpusError("truth_reveal_required_before_regression")
    version = evaluator_version.strip()
    if not version:
        raise CorpusError("evaluator_version_missing")
    if any(
        item.get("record_type") == "post_reveal_regression"
        and item.get("work_item_id") == work_item_id
        and item.get("evaluator_version") == version
        for item in records
    ):
        raise CorpusError("regression_result_already_recorded")
    record = {
        "schema_version": LEDGER_SCHEMA_VERSION,
        "record_type": "post_reveal_regression",
        "recorded_at": _utc_now(),
        "work_item_id": work_item_id,
        "evaluator_version": version,
        "result": dict(result),
        "result_sha256": sha256_json(result),
        "previous_record_sha256": records[-1]["record_sha256"],
    }
    record["record_sha256"] = sha256_json(record)
    _append_jsonl(ledger_path, record)
    return record


def capture_corpus(
    output_dir: Path,
    *,
    workers: int = 4,
    limit: int = 0,
) -> dict[str, Any]:
    if workers < 1 or workers > 8:
        raise CorpusError("workers_invalid")
    client = MeegleReadClient()
    index = query_target_index(client)
    if limit > 0:
        index = index[:limit]
    captured: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
    failures: list[dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(capture_case, client, item): item for item in index}
        for future in as_completed(futures):
            item = futures[future]
            try:
                captured[item["work_item_id"]] = future.result()
            except Exception as exc:
                failures.append({
                    "work_item_id": item["work_item_id"],
                    "error_code": getattr(exc, "code", type(exc).__name__),
                })
    if failures:
        raise CorpusError("corpus_capture_incomplete", json.dumps(failures, sort_keys=True))
    ordered = [captured[item["work_item_id"]] for item in index]
    return write_corpus(
        output_dir,
        [item[0] for item in ordered],
        [item[1] for item in ordered],
    )


def _load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise CorpusError("json_object_required")
    return value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    capture_parser = subparsers.add_parser("capture")
    capture_parser.add_argument("--output-dir", required=True, type=Path)
    capture_parser.add_argument("--workers", type=int, default=4)
    capture_parser.add_argument("--limit", type=int, default=0)

    record_parser = subparsers.add_parser("record-blind")
    record_parser.add_argument("--corpus-dir", required=True, type=Path)
    record_parser.add_argument("--work-item-id", required=True)
    record_parser.add_argument("--result", required=True, type=Path)
    record_parser.add_argument("--evaluator-version", required=True)

    reveal_parser = subparsers.add_parser("reveal-truth")
    reveal_parser.add_argument("--corpus-dir", required=True, type=Path)
    reveal_parser.add_argument("--work-item-id", required=True)

    regression_parser = subparsers.add_parser("record-regression")
    regression_parser.add_argument("--corpus-dir", required=True, type=Path)
    regression_parser.add_argument("--work-item-id", required=True)
    regression_parser.add_argument("--result", required=True, type=Path)
    regression_parser.add_argument("--evaluator-version", required=True)

    args = parser.parse_args(argv)
    try:
        if args.command == "capture":
            result = capture_corpus(
                args.output_dir, workers=args.workers, limit=args.limit
            )
        elif args.command == "record-blind":
            result = append_blind_result(
                args.corpus_dir,
                work_item_id=args.work_item_id,
                result=_load_object(args.result),
                evaluator_version=args.evaluator_version,
            )
        elif args.command == "reveal-truth":
            result = reveal_owner_truth(
                args.corpus_dir, work_item_id=args.work_item_id
            )
        else:
            result = append_post_reveal_regression(
                args.corpus_dir,
                work_item_id=args.work_item_id,
                result=_load_object(args.result),
                evaluator_version=args.evaluator_version,
            )
    except CorpusError as exc:
        print(json.dumps({"ok": False, "error_code": exc.code}, ensure_ascii=False))
        return 2
    print(json.dumps({"ok": True, "result": result}, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
