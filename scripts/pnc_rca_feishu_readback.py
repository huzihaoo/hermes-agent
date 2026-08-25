#!/usr/bin/env python3
"""Capture privacy-safe Feishu field and comment readback for a sealed set."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Any, Callable, Mapping, Sequence


SCHEMA_VERSION = "g1q3_feishu_attribution_readback_v2"
PROJECT_FIELD_KEY = "field_052f23"
REPORT_FIELD_KEY = "field_8c912e"
RESULT_FIELD_KEY = "field_9193cb"
REQUESTED_FIELDS = (PROJECT_FIELD_KEY, REPORT_FIELD_KEY, RESULT_FIELD_KEY)
EXPECTED_PROJECT_OPTION_ID = "6670325063"
MAX_BATCH_SIZE = 200
MAX_COMMENT_PAGES = 100
_DELIVERY_RE = re.compile(
    r"RCA_DELIVERY:(g1q3-rca-effect-v1-[0-9a-f]{64}):([0-9a-f]{12})"
)
_TERMINAL_RE = re.compile(
    r"RCA_TERMINAL:(g1q3-rca-terminal-effect-v1-[0-9a-f]{64})"
    r":([a-z0-9_]+):([1-9][0-9]*)"
)
_ATTRIBUTION_RE = re.compile(
    r"RCA_ATTRIBUTION:([A-Za-z0-9._-]{3,120}):([0-9a-f]{64})"
    r":(g1q3-rca-effect-v1-[0-9a-f]{64})"
)


class FeishuReadbackError(RuntimeError):
    def __init__(self, code: str, detail: str = ""):
        self.code = str(code or "feishu_readback_failed")[:120]
        self.detail = str(detail or self.code)[:1000]
        super().__init__(self.detail)


Runner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode("ascii")


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _value_projection(field_key: str, value: Any) -> dict[str, Any]:
    raw = _canonical_bytes(value)
    return {
        "field_key": field_key,
        "present": value is not None,
        "nonempty": value not in (None, "", [], {}),
        "bytes": len(raw),
        "sha256": _sha256(raw),
    }


def _default_runner(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(command),
        text=True,
        capture_output=True,
        timeout=60,
        check=False,
    )


def _run_json(runner: Runner, command: Sequence[str], error_code: str) -> tuple[Any, dict[str, Any]]:
    completed = runner(command)
    raw = completed.stdout.encode("utf-8")
    receipt = {
        "returncode": completed.returncode,
        "bytes": len(raw),
        "sha256": _sha256(raw),
    }
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or error_code).strip()
        raise FeishuReadbackError(error_code, detail)
    try:
        return json.loads(completed.stdout), receipt
    except json.JSONDecodeError as exc:
        raise FeishuReadbackError(error_code, "response_json_invalid") from exc


def _load_ids(path: Path) -> tuple[list[str], str]:
    raw = path.read_bytes()
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FeishuReadbackError("ids_manifest_invalid") from exc
    cases = None
    if isinstance(value, Mapping):
        sets = value.get("sets")
        v286 = sets.get("V286") if isinstance(sets, Mapping) else None
        if isinstance(v286, Mapping):
            cases = v286.get("cases")
        if cases is None:
            cases = value.get("cases")
    if not isinstance(cases, list):
        raise FeishuReadbackError("ids_manifest_invalid")
    ids = [
        str(case.get("work_item_id") or "").strip()
        for case in cases
        if isinstance(case, Mapping)
    ]
    if (
        not ids
        or len(ids) != len(cases)
        or len(set(ids)) != len(ids)
        or any(re.fullmatch(r"[0-9]{6,24}", item) is None for item in ids)
    ):
        raise FeishuReadbackError("ids_manifest_identity_invalid")
    return sorted(ids), _sha256(raw)


def _chunks(values: Sequence[str], size: int) -> list[list[str]]:
    return [list(values[start : start + size]) for start in range(0, len(values), size)]


def _field_map(data: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    fields = data.get("work_item_fields")
    if not isinstance(fields, list):
        raise FeishuReadbackError("work_item_fields_invalid")
    result: dict[str, Mapping[str, Any]] = {}
    for field in fields:
        if not isinstance(field, Mapping):
            raise FeishuReadbackError("work_item_field_invalid")
        key = str(field.get("key") or "").strip()
        if key in result or key not in REQUESTED_FIELDS:
            raise FeishuReadbackError("work_item_field_identity_invalid", key)
        result[key] = field
    return result


def _routing_projection(work_item_id: str, data: Mapping[str, Any]) -> dict[str, Any]:
    attribute = data.get("work_item_attribute")
    fields = _field_map(data)
    project = fields.get(PROJECT_FIELD_KEY)
    values = project.get("value") if isinstance(project, Mapping) else None
    option_ids = sorted({
        str(value.get("id") or "")
        for value in values or []
        if isinstance(value, Mapping) and str(value.get("id") or "")
    }) if isinstance(values, list) else []
    observed_id = (
        str(attribute.get("work_item_id") or "")
        if isinstance(attribute, Mapping)
        else ""
    )
    return {
        "field_key": PROJECT_FIELD_KEY,
        "expected_option_id": EXPECTED_PROJECT_OPTION_ID,
        "option_ids": option_ids,
        "work_item_identity": {
            "observed": observed_id,
            "matches": observed_id == work_item_id,
        },
        "exact": option_ids == [EXPECTED_PROJECT_OPTION_ID] and observed_id == work_item_id,
    }


def _work_item_projection(work_item_id: str, data: Mapping[str, Any]) -> dict[str, Any]:
    fields = _field_map(data)
    return {
        "work_item_id": work_item_id,
        "routing": _routing_projection(work_item_id, data),
        "report_field": _value_projection(
            REPORT_FIELD_KEY,
            fields.get(REPORT_FIELD_KEY, {}).get("value"),
        ),
        "result_field": _value_projection(
            RESULT_FIELD_KEY,
            fields.get(RESULT_FIELD_KEY, {}).get("value"),
        ),
    }


def fetch_work_items(
    ids: Sequence[str],
    *,
    meegle: Path,
    project_key: str,
    runner: Runner = _default_runner,
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    projected: dict[str, dict[str, Any]] = {}
    receipts: list[dict[str, Any]] = []
    for batch_number, batch in enumerate(_chunks(ids, MAX_BATCH_SIZE), start=1):
        command = [
            str(meegle), "workitem", "+batch-get",
            "--project-key", project_key,
            "--work-item-ids", ",".join(batch),
        ]
        for field in REQUESTED_FIELDS:
            command.extend(("--fields", field))
        command.extend(("--format", "json"))
        payload, receipt = _run_json(runner, command, "workitem_batch_get_failed")
        if not isinstance(payload, Mapping):
            raise FeishuReadbackError("workitem_batch_get_shape_invalid")
        results = payload.get("results")
        errors = payload.get("errors")
        if not isinstance(results, list) or errors not in (None, []):
            raise FeishuReadbackError("workitem_batch_get_incomplete")
        for raw in results:
            if not isinstance(raw, Mapping) or not isinstance(raw.get("data"), Mapping):
                raise FeishuReadbackError("workitem_batch_get_row_invalid")
            work_item_id = str(raw.get("work_item_id") or "").strip()
            if work_item_id not in batch or work_item_id in projected:
                raise FeishuReadbackError("workitem_batch_get_identity_invalid")
            projected[work_item_id] = _work_item_projection(work_item_id, raw["data"])
        receipt.update({"batch_number": batch_number, "requested": len(batch), "returned": len(results)})
        receipts.append(receipt)
    if set(projected) != set(ids):
        raise FeishuReadbackError("workitem_batch_get_coverage_invalid")
    return projected, receipts


def extract_comment(comment: Mapping[str, Any]) -> dict[str, Any]:
    comment_id = str(comment.get("comment_id") or "").strip()
    content = str(comment.get("content") or "")
    raw = content.encode("utf-8")
    delivery = [
        {"effect_key": match.group(1), "artifact_suffix": match.group(2)}
        for match in _DELIVERY_RE.finditer(content)
    ]
    terminal = [
        {
            "effect_key": match.group(1),
            "outcome": match.group(2),
            "generation": int(match.group(3)),
        }
        for match in _TERMINAL_RE.finditer(content)
    ]
    attribution = [
        {
            "version": match.group(1),
            "contract_sha256": match.group(2),
            "effect_key": match.group(3),
        }
        for match in _ATTRIBUTION_RE.finditer(content)
    ]
    return {
        "comment_id": comment_id,
        "created_at": str(comment.get("created_at") or ""),
        "content_bytes": len(raw),
        "content_sha256": _sha256(raw),
        "delivery_markers": delivery,
        "terminal_markers": terminal,
        "attribution_markers": attribution,
        "relevant": bool(delivery or terminal or attribution),
    }


def fetch_comments(
    work_item_id: str,
    *,
    meegle: Path,
    project_key: str,
    runner: Runner = _default_runner,
) -> dict[str, Any]:
    pages: list[dict[str, Any]] = []
    extracts: list[dict[str, Any]] = []
    page_num = 1
    expected_total_pages = 1
    expected_total: int | None = None
    while page_num <= expected_total_pages:
        if page_num > MAX_COMMENT_PAGES:
            raise FeishuReadbackError("comment_page_limit_exceeded", work_item_id)
        command = [
            str(meegle), "comment", "list",
            "--project-key", project_key,
            "--work-item-id", work_item_id,
            "--page-num", str(page_num),
            "--format", "json",
        ]
        payload, receipt = _run_json(runner, command, "comment_list_failed")
        if not isinstance(payload, Mapping):
            raise FeishuReadbackError("comment_list_shape_invalid")
        comments = payload.get("comments")
        pagination = payload.get("pagination")
        if not isinstance(comments, list) or not isinstance(pagination, Mapping):
            raise FeishuReadbackError("comment_list_shape_invalid")
        observed_page = pagination.get("page_num")
        total_pages = pagination.get("total_pages")
        total = pagination.get("total")
        if (
            isinstance(observed_page, bool)
            or not isinstance(observed_page, int)
            or observed_page != page_num
            or isinstance(total_pages, bool)
            or not isinstance(total_pages, int)
            or total_pages < 0
            or isinstance(total, bool)
            or not isinstance(total, int)
            or total < 0
            or (total_pages == 0 and (total != 0 or comments))
            or (total_pages > 0 and total == 0)
        ):
            raise FeishuReadbackError("comment_pagination_invalid")
        if page_num == 1:
            expected_total_pages = total_pages
            expected_total = total
        elif total_pages != expected_total_pages or total != expected_total:
            raise FeishuReadbackError("comment_pagination_drift")
        projected = [extract_comment(value) for value in comments if isinstance(value, Mapping)]
        if len(projected) != len(comments):
            raise FeishuReadbackError("comment_row_invalid")
        extracts.extend(value for value in projected if value["relevant"])
        pages.append({
            "page_num": page_num,
            "returned": len(comments),
            "total": total,
            "total_pages": total_pages,
            "response": receipt,
        })
        page_num += 1
    returned = sum(page["returned"] for page in pages)
    return {
        "comments": {
            "complete": expected_total == returned,
            "returned": returned,
            "total": expected_total,
            "pages": pages,
        },
        "comment_extracts": extracts,
        "delivery_effect_keys": sorted({
            marker["effect_key"]
            for value in extracts
            for marker in value["delivery_markers"]
        }),
        "terminal_effect_keys": sorted({
            marker["effect_key"]
            for value in extracts
            for marker in value["terminal_markers"]
        }),
        "attribution_markers": sorted(
            {
                (
                    marker["version"],
                    marker["contract_sha256"],
                    marker["effect_key"],
                )
                for value in extracts
                for marker in value["attribution_markers"]
            }
        ),
    }


def _load_resume_readback(
    path: Path,
    *,
    ids: Sequence[str],
    project_key: str,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], dict[str, Any]]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("ascii"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FeishuReadbackError("resume_readback_invalid") from exc
    results = value.get("results") if isinstance(value, Mapping) else None
    inventory = value.get("inventory") if isinstance(value, Mapping) else None
    if (
        not isinstance(value, Mapping)
        or value.get("schema_version") != SCHEMA_VERSION
        or value.get("project_key") != project_key
        or not isinstance(results, list)
        or not isinstance(inventory, Mapping)
        or inventory.get("count") != len(ids)
        or inventory.get("ids_sha256")
        != _sha256(("\n".join(ids) + "\n").encode("ascii"))
    ):
        raise FeishuReadbackError("resume_readback_invalid")
    work_items: dict[str, dict[str, Any]] = {}
    comments: dict[str, dict[str, Any]] = {}
    for row in results:
        if not isinstance(row, Mapping):
            raise FeishuReadbackError("resume_readback_invalid")
        work_item_id = str(row.get("work_item_id") or "")
        if work_item_id not in ids or work_item_id in work_items:
            raise FeishuReadbackError("resume_readback_invalid")
        routing = row.get("routing")
        report = row.get("report_field")
        result = row.get("result_field")
        if not all(isinstance(item, Mapping) for item in (routing, report, result)):
            raise FeishuReadbackError("resume_readback_invalid")
        work_items[work_item_id] = {
            "work_item_id": work_item_id,
            "routing": dict(routing),
            "report_field": dict(report),
            "result_field": dict(result),
        }
        comment_projection = row.get("comments")
        if isinstance(comment_projection, Mapping) and comment_projection.get("complete") is True:
            comments[work_item_id] = {
                "comments": dict(comment_projection),
                "comment_extracts": list(row.get("comment_extracts") or []),
                "delivery_effect_keys": list(row.get("delivery_effect_keys") or []),
                "terminal_effect_keys": list(row.get("terminal_effect_keys") or []),
                "attribution_markers": [
                    (
                        str(marker.get("version") or ""),
                        str(marker.get("contract_sha256") or ""),
                        str(marker.get("effect_key") or ""),
                    )
                    for marker in row.get("attribution_markers") or []
                    if isinstance(marker, Mapping)
                ],
            }
    if set(work_items) != set(ids):
        raise FeishuReadbackError("resume_readback_invalid")
    return work_items, comments, {
        "path": str(path),
        "sha256": _sha256(raw),
        "observed_at": str(value.get("observed_at") or ""),
        "reused_work_items": len(work_items),
        "reused_complete_comments": len(comments),
    }


def build_readback(
    ids_manifest: Path,
    *,
    meegle: Path,
    project_key: str,
    max_workers: int = 3,
    resume_readback: Path | None = None,
    runner: Runner = _default_runner,
) -> dict[str, Any]:
    if max_workers < 1 or max_workers > 3:
        raise FeishuReadbackError("max_workers_invalid")
    ids, source_sha256 = _load_ids(ids_manifest)
    auth, auth_receipt = _run_json(
        runner,
        [str(meegle), "auth", "status", "--format", "json"],
        "meegle_auth_status_failed",
    )
    resume_observation: dict[str, Any] = {"configured": False}
    if resume_readback is None:
        work_items, batch_receipts = fetch_work_items(
            ids, meegle=meegle, project_key=project_key, runner=runner
        )
        comment_results: dict[str, dict[str, Any]] = {}
    else:
        work_items, comment_results, resume_observation = _load_resume_readback(
            resume_readback,
            ids=ids,
            project_key=project_key,
        )
        resume_observation["configured"] = True
        batch_receipts = []
    pending_comment_ids = [item for item in ids if item not in comment_results]
    errors: list[dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(
                fetch_comments,
                work_item_id,
                meegle=meegle,
                project_key=project_key,
                runner=runner,
            ): work_item_id
            for work_item_id in pending_comment_ids
        }
        for future in as_completed(futures):
            work_item_id = futures[future]
            try:
                comment_results[work_item_id] = future.result()
            except FeishuReadbackError as exc:
                errors.append({
                    "work_item_id": work_item_id,
                    "error_code": exc.code,
                    "detail_sha256": _sha256(exc.detail.encode("utf-8")),
                })
    results = []
    for work_item_id in ids:
        comments = comment_results.get(work_item_id)
        result = dict(work_items[work_item_id])
        if comments is None:
            result.update({
                "comments": {"complete": False, "returned": 0, "total": None, "pages": []},
                "comment_extracts": [],
                "delivery_effect_keys": [],
                "terminal_effect_keys": [],
                "attribution_markers": [],
                "errors": ["comment_read_failed"],
            })
        else:
            result.update(comments)
            result["attribution_markers"] = [
                {
                    "version": version,
                    "contract_sha256": contract_sha256,
                    "effect_key": effect_key,
                }
                for version, contract_sha256, effect_key in result["attribution_markers"]
            ]
            result["errors"] = []
        results.append(result)
    validation_errors = list(errors)
    validation_errors.extend(
        {"work_item_id": result["work_item_id"], "error_code": "routing_not_exact"}
        for result in results
        if result["routing"]["exact"] is not True
    )
    validation_errors.extend(
        {"work_item_id": result["work_item_id"], "error_code": "comments_incomplete"}
        for result in results
        if result["comments"]["complete"] is not True
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "observed_at": _utc_now(),
        "project_key": project_key,
        "requested_fields": list(REQUESTED_FIELDS),
        "inventory": {
            "path": str(ids_manifest),
            "sha256": source_sha256,
            "count": len(ids),
            "ids_sha256": _sha256(("\n".join(ids) + "\n").encode("ascii")),
        },
        "auth": {
            "authenticated": isinstance(auth, Mapping),
            "response": auth_receipt,
        },
        "concurrency": {"max_workers": max_workers},
        "resume_observation": resume_observation,
        "batch_receipts": batch_receipts,
        "results": results,
        "validation": {
            "ok": not validation_errors and len(results) == len(ids),
            "errors": validation_errors,
            "expected_count": len(ids),
            "result_count": len(results),
            "unique_work_item_ids": len({row["work_item_id"] for row in results}),
        },
        "privacy": {
            "raw_field_values_persisted": False,
            "raw_comment_bodies_persisted": False,
            "raw_titles_persisted": False,
        },
        "external_side_effects": {
            "control_store_writes": 0,
            "feishu_writes": 0,
            "kafka_commits": 0,
            "vm_submissions": 0,
        },
    }


def _write_json(path: Path, value: Mapping[str, Any]) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = _canonical_bytes(value) + b"\n"
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    return {"path": str(path), "sha256": _sha256(raw), "bytes": len(raw)}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ids-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--meegle", type=Path, default=Path("/usr/local/bin/meegle"))
    parser.add_argument("--project-key", default="68ef617fb371dc80a10641f7")
    parser.add_argument("--max-workers", type=int, default=3)
    parser.add_argument("--resume-readback", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        readback = build_readback(
            args.ids_manifest,
            meegle=args.meegle,
            project_key=args.project_key,
            max_workers=args.max_workers,
            resume_readback=args.resume_readback,
        )
        artifact = _write_json(args.output, readback)
        print(json.dumps({"success": readback["validation"]["ok"], "artifact": artifact}, sort_keys=True))
        return 0 if readback["validation"]["ok"] else 2
    except FeishuReadbackError as exc:
        print(
            json.dumps(
                {"success": False, "error_code": exc.code, "detail": exc.detail},
                ensure_ascii=True,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
