#!/usr/bin/env python3
"""Read-only, provenance-bound Feishu workload exporter for G1Q3 RCA."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

_IMPORT_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_IMPORT_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_IMPORT_REPO_ROOT))

from gateway.pnc_rca_data_access import (
    RemoteDataAccessError,
    build_remote_data_access,
    validate_remote_data_access,
)


COMPONENT = "rca_issue_workload_export"
MODULE_PATH = "scripts/rca_issue_workload_export.py"
CENSUS_SCHEMA_VERSION = "rca_issue_workload_export_census_v1"
RECEIPT_SCHEMA_VERSION = "rca_issue_workload_export_receipt_v1"
MAPPING_REQUEST_SCHEMA_VERSION = "rca_issue_domain_mapping_request_v1"
MAPPING_SCHEMA_VERSION = "rca_issue_domain_mapping_v1"
MAPPING_APPROVAL_SCHEMA_VERSION = "rca_issue_domain_mapping_approval_v1"
MANIFEST_SCHEMA_VERSION = "pnc_rca_remote_reader_soak_manifest_v1"
PROJECT_KEY = "t03o4q"
WORK_ITEM_TYPE = "issue"
WORK_ITEM_ID_FIELD = "work_item_id"
FUNCTION_CATEGORY_FIELD = "field_e776bb"
PDCL_DATA_FIELD = "field_93aa63"
PDCL_DATA_FIELD_NAME = "问题数据地址_PDCL"
DOMAIN_QUOTAS = {"ACC": 50, "AEB_FCW": 50, "DNP": 50, "LCC": 50}
READER_CLASS_QUOTAS = {"RemoteClipReader": 25, "RemoteEventReader": 25}
ALLOWED_FUNCTION_DOMAINS = {"ACC", "AEB", "DNP", "FCW", "LCC"}
MEEGLE_HOST = "project.feishu.cn"
MAX_MEEGLE_RESPONSE_BYTES = 32 * 1024 * 1024
MAX_MAPPING_BYTES = 256 * 1024
MAX_PAGE_SIZE = 50
_HEX40_RE = re.compile(r"^[0-9a-f]{40}$")
_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_FIELD_KEY_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")
_PROJECT_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")


class ExportError(RuntimeError):
    """A stable, non-sensitive workload export failure."""

    def __init__(self, code: str, detail: str):
        super().__init__(detail)
        self.code = code
        self.detail = detail


@dataclass(frozen=True)
class TaxonomyOption:
    option_ids: tuple[str, ...]
    option_path: tuple[str, ...]


@dataclass(frozen=True)
class WorkloadCandidate:
    work_item_id: str
    option_ids: tuple[str, ...]
    option_path: tuple[str, ...]
    data_access: dict[str, Any]

    @property
    def reference(self) -> dict[str, str]:
        return dict(self.data_access["references"][0])

    @property
    def reference_identity(self) -> tuple[str, str]:
        reference = self.reference
        kind = reference["kind"]
        locator_key = "clip_uuid" if kind == "clip" else "event_uuid"
        return kind, reference[locator_key]

    @property
    def reader_class(self) -> str:
        return str(self.reference["reader_class"])


@dataclass(frozen=True)
class DomainMapping:
    rules: dict[tuple[tuple[str, ...], tuple[str, ...]], str]
    artifact_sha256: str
    approval: dict[str, str]


@dataclass
class ScanResult:
    census: dict[str, Any]
    candidates: list[WorkloadCandidate]
    taxonomy_options: list[TaxonomyOption]
    taxonomy_sha256: str


Runner = Callable[[Sequence[str], float], subprocess.CompletedProcess[str]]


def _canonical_json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ExportError("export_json_invalid", "artifact is not canonical JSON") from exc


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_json(value: Any) -> str:
    return _sha256_bytes(_canonical_json_bytes(value))


def _timestamp(value: datetime | None = None) -> str:
    observed = value or datetime.now(timezone.utc)
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=timezone.utc)
    return observed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _parse_timestamp(value: Any, *, field: str) -> str:
    text = str(value or "")
    try:
        parsed = datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as exc:
        raise ExportError("domain_mapping_invalid", f"{field} must be UTC microseconds") from exc
    return _timestamp(parsed)


def _absolute_no_resolve(path: Path) -> Path:
    return Path(os.path.abspath(os.path.expanduser(str(path))))


def _default_runner(
    args: Sequence[str], timeout_seconds: float
) -> subprocess.CompletedProcess[str]:
    executable = shutil.which("meegle")
    if not executable:
        raise ExportError("meegle_cli_missing", "official meegle CLI is unavailable")
    environment = os.environ.copy()
    environment.setdefault("MEEGLE_HOST", MEEGLE_HOST)
    try:
        return subprocess.run(
            [executable, *args],
            capture_output=True,
            check=False,
            env=environment,
            text=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        raise ExportError("meegle_timeout", "bounded meegle read timed out") from exc


class MeegleClient:
    def __init__(self, *, runner: Runner = _default_runner, timeout_seconds: float = 30.0):
        if timeout_seconds <= 0:
            raise ExportError("meegle_timeout_invalid", "timeout must be positive")
        self._runner = runner
        self._timeout_seconds = float(timeout_seconds)

    def _json(self, args: Sequence[str]) -> dict[str, Any]:
        completed = self._runner(tuple(args), self._timeout_seconds)
        stdout = str(completed.stdout or "")
        stderr = str(completed.stderr or "")
        if completed.returncode != 0:
            digest = _sha256_bytes(stderr.encode("utf-8"))
            raise ExportError(
                "meegle_read_failed",
                f"meegle read failed rc={completed.returncode} stderr_sha256={digest}",
            )
        if len(stdout.encode("utf-8")) > MAX_MEEGLE_RESPONSE_BYTES:
            raise ExportError("meegle_response_too_large", "meegle response exceeded limit")
        try:
            body = json.loads(stdout)
        except json.JSONDecodeError as exc:
            raise ExportError("meegle_response_invalid", "meegle response was not JSON") from exc
        if not isinstance(body, dict):
            raise ExportError("meegle_response_invalid", "meegle response must be an object")
        return body

    def auth_status(self) -> dict[str, Any]:
        body = self._json(("auth", "status", "--format", "json"))
        if body.get("authenticated") is not True or body.get("host") != MEEGLE_HOST:
            raise ExportError("meegle_auth_unavailable", "authenticated official Feishu source required")
        return {
            "authenticated": True,
            "host": MEEGLE_HOST,
            "expires_in_minutes": int(body.get("expires_in_minutes") or 0),
        }

    def field_metadata(
        self, *, project_key: str, work_item_type: str, field_key: str
    ) -> dict[str, Any]:
        return self._json(
            (
                "workitem",
                "meta-fields",
                "--project-key",
                project_key,
                "--work-item-type",
                work_item_type,
                "--page-num",
                "1",
                "--field-keys",
                field_key,
                "--format",
                "json",
            )
        )

    def query(self, *, project_key: str, mql: str) -> dict[str, Any]:
        return self._json(
            (
                "workitem",
                "query",
                "--project-key",
                project_key,
                "--mql",
                mql,
                "--format",
                "json",
            )
        )


def _validate_source_identifiers(project_key: str, work_item_type: str) -> None:
    if not _PROJECT_KEY_RE.fullmatch(project_key):
        raise ExportError("project_key_invalid", "project key is invalid")
    if not _FIELD_KEY_RE.fullmatch(work_item_type):
        raise ExportError("work_item_type_invalid", "work item type is invalid")


def _taxonomy_options(field: Mapping[str, Any]) -> list[TaxonomyOption]:
    options: list[TaxonomyOption] = []

    def walk(raw: Any, ids: tuple[str, ...], labels: tuple[str, ...]) -> None:
        if not isinstance(raw, list):
            return
        for item in raw:
            if not isinstance(item, Mapping):
                raise ExportError("taxonomy_invalid", "taxonomy option must be an object")
            option_id = str(item.get("option_id") or "").strip()
            label = str(item.get("option_name") or "").strip()
            if not option_id or not label:
                raise ExportError("taxonomy_invalid", "taxonomy option identity is missing")
            next_ids = ids + (option_id,)
            next_labels = labels + (label,)
            children = item.get("children")
            if children:
                walk(children, next_ids, next_labels)
            else:
                options.append(TaxonomyOption(next_ids, next_labels))

    walk(field.get("option"), (), ())
    if not options:
        raise ExportError("taxonomy_invalid", "function taxonomy has no leaf options")
    return sorted(options, key=lambda option: (option.option_path, option.option_ids))


def _load_taxonomy(
    body: Mapping[str, Any], *, expected_field_key: str
) -> tuple[dict[str, Any], list[TaxonomyOption], str]:
    raw_fields = body.get("list")
    if not isinstance(raw_fields, list) or len(raw_fields) != 1:
        raise ExportError("taxonomy_field_missing", "exact function taxonomy field required")
    raw_field = raw_fields[0]
    if not isinstance(raw_field, Mapping):
        raise ExportError("taxonomy_invalid", "function taxonomy field is invalid")
    if (
        raw_field.get("field_key") != expected_field_key
        or raw_field.get("field_type") != "tree-select"
    ):
        raise ExportError("taxonomy_field_mismatch", "function taxonomy field mismatch")
    options = _taxonomy_options(raw_field)
    material = {
        "field_key": expected_field_key,
        "field_name": str(raw_field.get("field_name") or ""),
        "field_type": "tree-select",
        "options": [
            {"option_ids": list(option.option_ids), "option_path": list(option.option_path)}
            for option in options
        ],
    }
    return material, options, _sha256_json(material)


def _cascade_paths(value: Any) -> list[tuple[tuple[str, ...], tuple[str, ...]]]:
    if not isinstance(value, Mapping):
        return []
    option_id = str(value.get("key") or "").strip()
    label = str(value.get("label") or "").strip()
    if not option_id or not label:
        return []
    children = value.get("children")
    if not children:
        return [((option_id,), (label,))]
    paths: list[tuple[tuple[str, ...], tuple[str, ...]]] = []
    if not isinstance(children, list):
        return []
    for child in children:
        for child_ids, child_labels in _cascade_paths(child):
            paths.append(((option_id,) + child_ids, (label,) + child_labels))
    return paths


def _field_value(field: Mapping[str, Any]) -> Any:
    raw = field.get("value")
    if not isinstance(raw, Mapping):
        return None
    if "long_value" in raw:
        return str(raw.get("long_value") or "")
    if "string_value" in raw:
        return str(raw.get("string_value") or "")
    if "cascade_key_label_value" in raw:
        paths = _cascade_paths(raw.get("cascade_key_label_value"))
        return paths[0] if len(paths) == 1 else None
    return None


def _query_rows(body: Mapping[str, Any]) -> list[dict[str, Any]]:
    data = body.get("data")
    if not isinstance(data, Mapping):
        return []
    rows: list[dict[str, Any]] = []
    for group_key in sorted(data, key=str):
        raw_items = data[group_key]
        if not isinstance(raw_items, list):
            continue
        for raw_item in raw_items:
            if not isinstance(raw_item, Mapping):
                continue
            raw_fields = raw_item.get("moql_field_list")
            if not isinstance(raw_fields, list):
                continue
            row: dict[str, Any] = {}
            for raw_field in raw_fields:
                if not isinstance(raw_field, Mapping):
                    continue
                key = str(raw_field.get("key") or "")
                if key:
                    row[key] = _field_value(raw_field)
            rows.append(row)
    return rows


def _query_count(body: Mapping[str, Any]) -> int:
    groups = body.get("list")
    if not isinstance(groups, list) or not groups or not isinstance(groups[0], Mapping):
        raise ExportError("meegle_count_missing", "query count was not returned")
    count = groups[0].get("count")
    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        raise ExportError("meegle_count_invalid", "query count was invalid")
    return count


def _query_mql(
    *, project_key: str, work_item_type: str, offset: int, page_size: int
) -> str:
    _validate_source_identifiers(project_key, work_item_type)
    return (
        f"SELECT `{WORK_ITEM_ID_FIELD}`, `{FUNCTION_CATEGORY_FIELD}`, `{PDCL_DATA_FIELD}` "
        f"FROM `{project_key}`.`{work_item_type}` "
        f"WHERE `{PDCL_DATA_FIELD}` is not null "
        f"ORDER BY `{WORK_ITEM_ID_FIELD}` ASC LIMIT {offset},{page_size}"
    )


def _single_reference_contract(
    access: Mapping[str, Any], reference: Mapping[str, str]
) -> dict[str, Any]:
    return validate_remote_data_access({**dict(access), "references": [dict(reference)]})


def _component_binding(repo_root: Path) -> dict[str, Any]:
    module = repo_root / MODULE_PATH
    try:
        module_bytes = module.read_bytes()
    except OSError as exc:
        raise ExportError("component_module_missing", "exporter module is unavailable") from exc
    module_sha256 = _sha256_bytes(module_bytes)

    def git(*args: str) -> subprocess.CompletedProcess[bytes]:
        return subprocess.run(
            ["git", *args], cwd=repo_root, capture_output=True, check=False
        )

    head = git("rev-parse", "HEAD")
    commit = head.stdout.decode("ascii", "ignore").strip() if head.returncode == 0 else ""
    committed = git("show", f"HEAD:{MODULE_PATH}")
    committed_match = (
        committed.returncode == 0 and _sha256_bytes(committed.stdout) == module_sha256
    )
    status = git("status", "--porcelain", "--", MODULE_PATH)
    module_clean = status.returncode == 0 and not status.stdout.strip()
    return {
        "component": COMPONENT,
        "component_commit": commit if _HEX40_RE.fullmatch(commit) else "",
        "module": MODULE_PATH,
        "module_sha256": module_sha256,
        "committed_match": committed_match,
        "module_clean": module_clean,
    }


def scan_workloads(
    client: MeegleClient,
    *,
    repo_root: Path,
    project_key: str = PROJECT_KEY,
    work_item_type: str = WORK_ITEM_TYPE,
    page_size: int = MAX_PAGE_SIZE,
    max_records: int | None = None,
    observed_at: str | None = None,
) -> ScanResult:
    _validate_source_identifiers(project_key, work_item_type)
    if page_size < 1 or page_size > MAX_PAGE_SIZE:
        raise ExportError("page_size_invalid", "page size must be between 1 and 50")
    if max_records is not None and max_records < 1:
        raise ExportError("max_records_invalid", "max records must be positive")
    generated_at = _parse_timestamp(observed_at, field="observed_at") if observed_at else _timestamp()
    auth = client.auth_status()
    taxonomy_body = client.field_metadata(
        project_key=project_key,
        work_item_type=work_item_type,
        field_key=FUNCTION_CATEGORY_FIELD,
    )
    taxonomy, taxonomy_options, taxonomy_sha256 = _load_taxonomy(
        taxonomy_body, expected_field_key=FUNCTION_CATEGORY_FIELD
    )
    component = _component_binding(repo_root)

    candidates: list[WorkloadCandidate] = []
    rejections: Counter[str] = Counter()
    category_records: Counter[tuple[str, ...]] = Counter()
    category_valid_work_items: Counter[tuple[str, ...]] = Counter()
    category_reference_candidates: Counter[tuple[str, ...]] = Counter()
    category_reference_identities: dict[
        tuple[str, ...], set[tuple[str, str]]
    ] = defaultdict(set)
    category_reader_counts: dict[tuple[str, ...], Counter[str]] = defaultdict(Counter)
    reader_counts: Counter[str] = Counter()
    reference_kind_counts: Counter[str] = Counter()
    work_items_with_valid_reference: set[str] = set()
    seen_work_items: set[str] = set()
    session_provenance: list[dict[str, Any]] = []
    observed_counts: list[int] = []
    records_seen = 0
    offset = 0
    target_records: int | None = None
    early_empty_page = False

    while target_records is None or offset < target_records:
        request_size = page_size
        if target_records is not None:
            request_size = min(request_size, target_records - offset)
        mql = _query_mql(
            project_key=project_key,
            work_item_type=work_item_type,
            offset=offset,
            page_size=request_size,
        )
        body = client.query(project_key=project_key, mql=mql)
        count = _query_count(body)
        observed_counts.append(count)
        if target_records is None:
            target_records = min(count, max_records) if max_records is not None else count
            if target_records == 0:
                break
        rows = _query_rows(body)
        session_id = str(body.get("session_id") or "")
        session_provenance.append(
            {
                "offset": offset,
                "requested": request_size,
                "returned": len(rows),
                "observed_source_count": count,
                "query_sha256": _sha256_bytes(mql.encode("utf-8")),
                "session_id_sha256": _sha256_bytes(session_id.encode("utf-8"))
                if session_id
                else None,
            }
        )
        if not rows:
            early_empty_page = offset < (target_records or 0)
            break
        for row in rows:
            records_seen += 1
            work_item_id = str(row.get(WORK_ITEM_ID_FIELD) or "").strip()
            category = row.get(FUNCTION_CATEGORY_FIELD)
            source_value = str(row.get(PDCL_DATA_FIELD) or "").strip()
            if not _IDENTIFIER_RE.fullmatch(work_item_id):
                rejections["work_item_id_invalid"] += 1
                continue
            if work_item_id in seen_work_items:
                rejections["duplicate_work_item_page_drift"] += 1
                continue
            seen_work_items.add(work_item_id)
            if (
                not isinstance(category, tuple)
                or len(category) != 2
                or not all(isinstance(part, tuple) and part for part in category)
            ):
                rejections["function_category_missing_or_invalid"] += 1
                continue
            option_ids, option_path = category
            category_records[option_path] += 1
            try:
                access = build_remote_data_access(source_value)
            except RemoteDataAccessError as exc:
                rejections[exc.code] += 1
                continue
            work_items_with_valid_reference.add(work_item_id)
            category_valid_work_items[option_path] += 1
            for reference in access["references"]:
                detached = _single_reference_contract(access, reference)
                candidate = WorkloadCandidate(
                    work_item_id=work_item_id,
                    option_ids=option_ids,
                    option_path=option_path,
                    data_access=detached,
                )
                candidates.append(candidate)
                category_reference_candidates[option_path] += 1
                category_reference_identities[option_path].add(
                    candidate.reference_identity
                )
                category_reader_counts[option_path][candidate.reader_class] += 1
                reader_counts[candidate.reader_class] += 1
                reference_kind_counts[candidate.reference["kind"]] += 1
        offset += len(rows)
        if len(rows) < request_size:
            early_empty_page = offset < (target_records or 0)
            break

    reference_identities = Counter(candidate.reference_identity for candidate in candidates)
    duplicate_reference_candidates = sum(
        count - 1 for count in reference_identities.values() if count > 1
    )
    snapshot_stable = (
        not early_empty_page
        and bool(observed_counts or target_records == 0)
        and len(set(observed_counts)) <= 1
        and rejections["duplicate_work_item_page_drift"] == 0
        and records_seen == (target_records or 0)
    )
    categories = []
    for option_path in sorted(category_records):
        categories.append(
            {
                "option_path": list(option_path),
                "record_count": category_records[option_path],
                "valid_work_item_count": category_valid_work_items[option_path],
                "valid_reference_candidate_count": category_reference_candidates[
                    option_path
                ],
                "unique_reference_count": len(
                    category_reference_identities[option_path]
                ),
                "duplicate_reference_candidate_count": (
                    category_reference_candidates[option_path]
                    - len(category_reference_identities[option_path])
                ),
                "reader_class_counts": dict(
                    sorted(category_reader_counts[option_path].items())
                ),
            }
        )
    census = {
        "schema_version": CENSUS_SCHEMA_VERSION,
        "observed_at": generated_at,
        "source": component,
        "feishu": {
            "host": auth["host"],
            "authenticated": auth["authenticated"],
            "project_key": project_key,
            "work_item_type": work_item_type,
            "selected_fields": [
                WORK_ITEM_ID_FIELD,
                FUNCTION_CATEGORY_FIELD,
                PDCL_DATA_FIELD,
            ],
            "mutation_performed": False,
            "attachment_read_performed": False,
            "page_size": page_size,
            "page_count": len(session_provenance),
            "session_provenance": session_provenance,
        },
        "taxonomy": {
            **taxonomy,
            "sha256": taxonomy_sha256,
            "leaf_count": len(taxonomy_options),
        },
        "statistics": {
            "initial_source_count": observed_counts[0] if observed_counts else 0,
            "minimum_observed_source_count": min(observed_counts) if observed_counts else 0,
            "maximum_observed_source_count": max(observed_counts) if observed_counts else 0,
            "target_records": target_records or 0,
            "records_seen": records_seen,
            "source_scan_complete": bool(observed_counts)
            and (target_records or 0) == observed_counts[0],
            "unique_work_items_seen": len(seen_work_items),
            "valid_work_item_count": len(work_items_with_valid_reference),
            "valid_reference_candidate_count": len(candidates),
            "unique_reference_count": len(reference_identities),
            "duplicate_reference_candidate_count": duplicate_reference_candidates,
            "snapshot_stable": snapshot_stable,
            "categories": categories,
            "reader_class_counts": dict(sorted(reader_counts.items())),
            "reference_kind_counts": dict(sorted(reference_kind_counts.items())),
            "rejection_reasons": dict(sorted(rejections.items())),
        },
        "security": {
            "raw_issue_payload_persisted": False,
            "raw_pdcl_field_persisted": False,
            "description_or_attachment_persisted": False,
            "credential_or_token_persisted": False,
            "input_materialized": False,
            "input_materialized_bytes": 0,
        },
    }
    return ScanResult(census, candidates, taxonomy_options, taxonomy_sha256)


def _secure_read_json(
    path: Path, *, max_bytes: int, artifact: str
) -> tuple[dict[str, Any], bytes]:
    label = artifact.replace("_", " ")
    try:
        initial = path.lstat()
    except OSError as exc:
        raise ExportError(f"{artifact}_unavailable", f"{label} is unavailable") from exc
    if (
        not stat.S_ISREG(initial.st_mode)
        or stat.S_ISLNK(initial.st_mode)
        or path.resolve(strict=True) != path
        or initial.st_uid != os.geteuid()
        or initial.st_nlink != 1
        or stat.S_IMODE(initial.st_mode) & 0o077
        or initial.st_size < 2
        or initial.st_size > max_bytes
    ):
        raise ExportError(f"{artifact}_unsafe", f"{label} must be owner-only")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if (
            opened.st_dev != initial.st_dev
            or opened.st_ino != initial.st_ino
            or opened.st_mode != initial.st_mode
            or opened.st_uid != initial.st_uid
            or opened.st_nlink != initial.st_nlink
        ):
            raise ExportError(f"{artifact}_changed", f"{label} changed while reading")
        raw = b""
        while len(raw) <= max_bytes:
            chunk = os.read(descriptor, min(64 * 1024, max_bytes + 1 - len(raw)))
            if not chunk:
                break
            raw += chunk
    finally:
        os.close(descriptor)
    if len(raw) > max_bytes:
        raise ExportError(f"{artifact}_too_large", f"{label} exceeded limit")
    try:
        body = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExportError(f"{artifact}_invalid", f"{label} is not JSON") from exc
    if not isinstance(body, dict):
        raise ExportError(f"{artifact}_invalid", f"{label} must be an object")
    if raw != _canonical_json_bytes(body) + b"\n":
        raise ExportError(
            f"{artifact}_not_canonical", f"{label} must be canonical JSON"
        )
    return body, raw


def load_domain_mapping(
    path: Path,
    *,
    approval_receipt_path: Path,
    project_key: str,
    work_item_type: str,
    taxonomy_options: Sequence[TaxonomyOption],
    taxonomy_sha256: str,
) -> DomainMapping:
    body, raw = _secure_read_json(
        path, max_bytes=MAX_MAPPING_BYTES, artifact="domain_mapping"
    )
    if set(body) != {
        "schema_version",
        "project_key",
        "work_item_type",
        "field_key",
        "taxonomy_sha256",
        "approval",
        "rules",
    }:
        raise ExportError("domain_mapping_invalid", "domain mapping fields mismatch")
    if (
        body.get("schema_version") != MAPPING_SCHEMA_VERSION
        or body.get("project_key") != project_key
        or body.get("work_item_type") != work_item_type
        or body.get("field_key") != FUNCTION_CATEGORY_FIELD
        or body.get("taxonomy_sha256") != taxonomy_sha256
    ):
        raise ExportError("domain_mapping_binding_mismatch", "domain mapping source binding mismatch")
    approval = body.get("approval")
    if not isinstance(approval, Mapping) or set(approval) != {
        "authority",
        "approved_by",
        "approved_at",
        "receipt_sha256",
    }:
        raise ExportError("domain_mapping_approval_invalid", "owner approval fields mismatch")
    normalized_approval = {
        "authority": str(approval.get("authority") or "").strip(),
        "approved_by": str(approval.get("approved_by") or "").strip(),
        "approved_at": _parse_timestamp(approval.get("approved_at"), field="approved_at"),
        "receipt_sha256": str(approval.get("receipt_sha256") or ""),
    }
    if (
        normalized_approval["authority"] != "PDCL/data owner"
        or not normalized_approval["approved_by"]
        or not _HEX64_RE.fullmatch(normalized_approval["receipt_sha256"])
    ):
        raise ExportError("domain_mapping_approval_invalid", "PDCL/data owner approval required")
    known_options = {(option.option_ids, option.option_path) for option in taxonomy_options}
    raw_rules = body.get("rules")
    if not isinstance(raw_rules, list) or not raw_rules:
        raise ExportError("domain_mapping_rules_invalid", "domain mapping rules are required")
    rules: dict[tuple[tuple[str, ...], tuple[str, ...]], str] = {}
    domains: set[str] = set()
    for raw_rule in raw_rules:
        if not isinstance(raw_rule, Mapping) or set(raw_rule) != {
            "function_domain",
            "option_ids",
            "option_path",
        }:
            raise ExportError("domain_mapping_rules_invalid", "domain mapping rule fields mismatch")
        domain = str(raw_rule.get("function_domain") or "").strip().upper()
        option_ids = tuple(str(value or "").strip() for value in raw_rule.get("option_ids") or [])
        option_path = tuple(str(value or "").strip() for value in raw_rule.get("option_path") or [])
        identity = (option_ids, option_path)
        if (
            domain not in ALLOWED_FUNCTION_DOMAINS
            or not option_ids
            or not option_path
            or any(not value for value in option_ids + option_path)
            or identity not in known_options
            or identity in rules
        ):
            raise ExportError("domain_mapping_rules_invalid", "domain mapping rule is invalid")
        rules[identity] = domain
        domains.add(domain)
    if not {"ACC", "DNP", "LCC"}.issubset(domains) or not domains.intersection({"AEB", "FCW"}):
        raise ExportError("domain_mapping_quota_coverage_missing", "mapping does not cover all quotas")
    rules_material = {
        "schema_version": body["schema_version"],
        "project_key": body["project_key"],
        "work_item_type": body["work_item_type"],
        "field_key": body["field_key"],
        "taxonomy_sha256": body["taxonomy_sha256"],
        "rules": body["rules"],
    }
    approval_receipt, approval_receipt_raw = _secure_read_json(
        approval_receipt_path,
        max_bytes=MAX_MAPPING_BYTES,
        artifact="domain_mapping_approval_receipt",
    )
    if set(approval_receipt) != {
        "schema_version",
        "authority",
        "approved_by",
        "approved_at",
        "mapping_rules_sha256",
    }:
        raise ExportError(
            "domain_mapping_approval_receipt_invalid",
            "domain mapping approval receipt fields mismatch",
        )
    normalized_receipt = {
        "schema_version": approval_receipt.get("schema_version"),
        "authority": str(approval_receipt.get("authority") or "").strip(),
        "approved_by": str(approval_receipt.get("approved_by") or "").strip(),
        "approved_at": _parse_timestamp(
            approval_receipt.get("approved_at"), field="approval_receipt.approved_at"
        ),
        "mapping_rules_sha256": str(
            approval_receipt.get("mapping_rules_sha256") or ""
        ),
    }
    if (
        normalized_receipt["schema_version"] != MAPPING_APPROVAL_SCHEMA_VERSION
        or normalized_receipt["authority"] != normalized_approval["authority"]
        or normalized_receipt["approved_by"] != normalized_approval["approved_by"]
        or normalized_receipt["approved_at"] != normalized_approval["approved_at"]
        or normalized_receipt["mapping_rules_sha256"] != _sha256_json(rules_material)
        or normalized_approval["receipt_sha256"]
        != _sha256_bytes(approval_receipt_raw)
    ):
        raise ExportError(
            "domain_mapping_approval_receipt_mismatch",
            "domain mapping approval receipt binding mismatch",
        )
    return DomainMapping(
        rules=rules,
        artifact_sha256=_sha256_bytes(raw),
        approval=normalized_approval,
    )


def _quota_domain(function_domain: str) -> str:
    return "AEB_FCW" if function_domain in {"AEB", "FCW"} else function_domain


def _candidate_sort_key(
    item: tuple[WorkloadCandidate, str]
) -> tuple[str, str, str, str]:
    candidate, function_domain = item
    kind, locator = candidate.reference_identity
    return (_quota_domain(function_domain), candidate.work_item_id, kind, locator)


def _attempt_selection(
    eligible: Sequence[tuple[WorkloadCandidate, str]],
    *,
    reader_order: Sequence[str],
) -> list[tuple[WorkloadCandidate, str]] | None:
    by_work_item: dict[str, set[str]] = defaultdict(set)
    for candidate, _domain in eligible:
        by_work_item[candidate.work_item_id].add(candidate.reader_class)
    selected: list[tuple[WorkloadCandidate, str]] = []
    work_items: set[str] = set()
    references: set[tuple[str, str]] = set()
    domain_counts: Counter[str] = Counter()
    reader_counts: Counter[str] = Counter()

    def take(item: tuple[WorkloadCandidate, str]) -> bool:
        candidate, function_domain = item
        quota_domain = _quota_domain(function_domain)
        if (
            domain_counts[quota_domain] >= DOMAIN_QUOTAS[quota_domain]
            or candidate.work_item_id in work_items
            or candidate.reference_identity in references
        ):
            return False
        selected.append(item)
        work_items.add(candidate.work_item_id)
        references.add(candidate.reference_identity)
        domain_counts[quota_domain] += 1
        reader_counts[candidate.reader_class] += 1
        return True

    for reader_class in reader_order:
        choices = sorted(
            (item for item in eligible if item[0].reader_class == reader_class),
            key=lambda item: (
                len(by_work_item[item[0].work_item_id]),
                *_candidate_sort_key(item),
            ),
        )
        for item in choices:
            if reader_counts[reader_class] >= READER_CLASS_QUOTAS[reader_class]:
                break
            take(item)
        if reader_counts[reader_class] < READER_CLASS_QUOTAS[reader_class]:
            return None

    for quota_domain in sorted(DOMAIN_QUOTAS):
        choices = sorted(
            (
                item
                for item in eligible
                if _quota_domain(item[1]) == quota_domain
            ),
            key=lambda item: (
                reader_counts[item[0].reader_class],
                *_candidate_sort_key(item),
            ),
        )
        for item in choices:
            if domain_counts[quota_domain] >= DOMAIN_QUOTAS[quota_domain]:
                break
            take(item)
        if domain_counts[quota_domain] < DOMAIN_QUOTAS[quota_domain]:
            return None
    return sorted(selected, key=lambda item: f"issue-{item[0].work_item_id}")


def build_manifest(
    scan: ScanResult,
    mapping: DomainMapping,
    *,
    generated_at: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    generated_at = _parse_timestamp(generated_at, field="generated_at")
    source = scan.census["source"]
    if (
        not source.get("committed_match")
        or not source.get("module_clean")
        or not _HEX40_RE.fullmatch(str(source.get("component_commit") or ""))
    ):
        raise ExportError("component_binding_unsealed", "committed clean exporter required")
    if scan.census["statistics"].get("snapshot_stable") is not True:
        raise ExportError("source_snapshot_unstable", "stable complete source census required")
    if scan.census["statistics"].get("source_scan_complete") is not True:
        raise ExportError("source_scan_incomplete", "complete source census required")
    eligible: list[tuple[WorkloadCandidate, str]] = []
    unmapped: Counter[tuple[str, ...]] = Counter()
    for candidate in scan.candidates:
        domain = mapping.rules.get((candidate.option_ids, candidate.option_path))
        if domain is None:
            unmapped[candidate.option_path] += 1
            continue
        eligible.append((candidate, domain))
    attempts = []
    for reader_order in (
        ("RemoteClipReader", "RemoteEventReader"),
        ("RemoteEventReader", "RemoteClipReader"),
    ):
        selected = _attempt_selection(eligible, reader_order=reader_order)
        if selected is not None:
            attempts.append(selected)
    if not attempts:
        availability: Counter[str] = Counter(
            _quota_domain(domain) for _candidate, domain in eligible
        )
        raise ExportError(
            "workload_quota_insufficient",
            "eligible unique workload cannot satisfy domain and reader quotas: "
            + json.dumps(dict(sorted(availability.items())), sort_keys=True),
        )
    selected = min(
        attempts,
        key=lambda result: _canonical_json_bytes(
            [
                [candidate.work_item_id, candidate.reference_identity, domain]
                for candidate, domain in result
            ]
        ),
    )
    cases = [
        {
            "case_id": f"issue-{candidate.work_item_id}",
            "work_item_id": candidate.work_item_id,
            "function_domain": domain,
            "data_access": candidate.data_access,
        }
        for candidate, domain in selected
    ]
    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at": generated_at,
        "source": {
            "generation_mode": "machine_generated",
            "component": COMPONENT,
            "component_commit": source["component_commit"],
            "artifact_sha256": source["module_sha256"],
        },
        "domain_quotas": dict(sorted(DOMAIN_QUOTAS.items())),
        "reader_class_quotas": dict(sorted(READER_CLASS_QUOTAS.items())),
        "cases": cases,
    }
    quota_counts = Counter(_quota_domain(case["function_domain"]) for case in cases)
    reader_counts = Counter(
        case["data_access"]["references"][0]["reader_class"] for case in cases
    )
    if quota_counts != Counter(DOMAIN_QUOTAS) or any(
        reader_counts[name] < count for name, count in READER_CLASS_QUOTAS.items()
    ):
        raise ExportError("manifest_internal_invariant_failed", "manifest quota invariant failed")
    receipt = {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "generated_at": generated_at,
        "source": dict(source),
        "census_sha256": _sha256_json(scan.census),
        "taxonomy_sha256": scan.taxonomy_sha256,
        "mapping": {
            "artifact_sha256": mapping.artifact_sha256,
            "approval": mapping.approval,
        },
        "selection": {
            "eligible_reference_candidates": len(eligible),
            "unmapped_category_counts": [
                {"option_path": list(path), "reference_count": count}
                for path, count in sorted(unmapped.items())
            ],
            "case_count": len(cases),
            "unique_work_items": len({case["work_item_id"] for case in cases}),
            "unique_references": len(
                {
                    WorkloadCandidate(
                        case["work_item_id"], (), (), case["data_access"]
                    ).reference_identity
                    for case in cases
                }
            ),
            "domain_counts": dict(sorted(quota_counts.items())),
            "reader_class_counts": dict(sorted(reader_counts.items())),
        },
        "manifest": {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "sha256": _sha256_json(manifest),
            "case_count": len(cases),
        },
        "security": dict(scan.census["security"]),
    }
    return manifest, receipt


def build_mapping_request(
    scan: ScanResult,
    *,
    repo_root: Path,
    census_file_sha256: str,
) -> dict[str, Any]:
    if not _HEX64_RE.fullmatch(census_file_sha256):
        raise ExportError("census_hash_invalid", "census file hash is invalid")
    taxonomy_options = {
        tuple(option["option_path"]): tuple(option["option_ids"])
        for option in scan.census["taxonomy"]["options"]
    }
    eligible_options = []
    for category in scan.census["statistics"]["categories"]:
        if category.get("unique_reference_count", 0) < 1:
            continue
        option_path = tuple(category["option_path"])
        option_ids = taxonomy_options.get(option_path)
        if option_ids is None:
            raise ExportError("taxonomy_category_mismatch", "census taxonomy mismatch")
        eligible_options.append(
            {
                "option_ids": list(option_ids),
                "option_path": list(option_path),
                "record_count": category["record_count"],
                "valid_work_item_count": category["valid_work_item_count"],
                "unique_reference_count": category["unique_reference_count"],
                "duplicate_reference_candidate_count": category[
                    "duplicate_reference_candidate_count"
                ],
                "reader_class_counts": category["reader_class_counts"],
            }
        )
    schema_entries = []
    for schema_path in (
        "docs/pnc/schemas/rca_issue_domain_mapping_v1.schema.json",
        "docs/pnc/schemas/rca_issue_domain_mapping_approval_v1.schema.json",
    ):
        try:
            schema_bytes = (repo_root / schema_path).read_bytes()
        except OSError as exc:
            raise ExportError("mapping_schema_missing", "mapping schema is unavailable") from exc
        schema_entries.append(
            {"path": schema_path, "sha256": _sha256_bytes(schema_bytes)}
        )
    return {
        "schema_version": MAPPING_REQUEST_SCHEMA_VERSION,
        "generated_at": scan.census["observed_at"],
        "source": {
            "component": COMPONENT,
            "component_commit": scan.census["source"]["component_commit"],
            "module_sha256": scan.census["source"]["module_sha256"],
            "census_file_sha256": census_file_sha256,
            "census_body_sha256": _sha256_json(scan.census),
        },
        "feishu_binding": {
            "host": scan.census["feishu"]["host"],
            "project_key": scan.census["feishu"]["project_key"],
            "work_item_type": scan.census["feishu"]["work_item_type"],
            "field_key": FUNCTION_CATEGORY_FIELD,
            "taxonomy_sha256": scan.taxonomy_sha256,
            "source_scan_complete": scan.census["statistics"][
                "source_scan_complete"
            ],
            "snapshot_stable": scan.census["statistics"]["snapshot_stable"],
        },
        "required_authority": "PDCL/data owner",
        "required_decision": {
            "function_domains": ["ACC", "LCC", "AEB", "FCW", "DNP"],
            "quota_domains": dict(sorted(DOMAIN_QUOTAS.items())),
            "rule_identity": ["option_ids", "option_path"],
            "instruction": (
                "Approve exact live taxonomy leaves for every function domain; "
                "DNP must not be inferred from parent labels or record volume."
            ),
        },
        "eligible_taxonomy_options": sorted(
            eligible_options, key=lambda item: item["option_path"]
        ),
        "required_artifacts": {
            "schemas": schema_entries,
            "mapping_mode": "canonical_json_owner_only_0600",
            "approval_receipt_mode": "canonical_json_owner_only_0600",
            "approval_binding": (
                "approval receipt mapping_rules_sha256 must bind schema/project/type/"
                "field/taxonomy/rules; mapping approval.receipt_sha256 must bind the "
                "exact receipt file"
            ),
        },
        "security": {
            "issue_identifiers_included": False,
            "remote_references_included": False,
            "raw_pdcl_fields_included": False,
            "credentials_or_tokens_included": False,
        },
    }


def _secure_atomic_write(path: Path, value: Mapping[str, Any]) -> str:
    payload = _canonical_json_bytes(value) + b"\n"
    parent = path.parent
    parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        parent_info = parent.lstat()
    except OSError as exc:
        raise ExportError("output_directory_invalid", "output directory unavailable") from exc
    if not stat.S_ISDIR(parent_info.st_mode) or stat.S_ISLNK(parent_info.st_mode):
        raise ExportError("output_directory_invalid", "output parent must be a directory")
    if parent.resolve(strict=True) != parent:
        raise ExportError("output_directory_invalid", "output parent must not traverse symlinks")
    if parent_info.st_uid != os.geteuid():
        raise ExportError("output_directory_invalid", "output parent must be owner controlled")
    if path.exists() or path.is_symlink():
        try:
            destination = path.lstat()
        except OSError as exc:
            raise ExportError("output_path_invalid", "output path is unavailable") from exc
        if (
            not stat.S_ISREG(destination.st_mode)
            or stat.S_ISLNK(destination.st_mode)
            or destination.st_uid != os.geteuid()
            or destination.st_nlink != 1
        ):
            raise ExportError("output_path_invalid", "output path is unsafe")
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        info = temporary.lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_nlink != 1
            or stat.S_IMODE(info.st_mode) != 0o600
        ):
            raise ExportError("output_temporary_invalid", "temporary output is unsafe")
        os.replace(temporary, path)
        os.chmod(path, 0o600)
        directory_fd = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temporary.exists():
            temporary.unlink()
    return _sha256_bytes(payload)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--project-key", default=PROJECT_KEY)
    parser.add_argument("--work-item-type", default=WORK_ITEM_TYPE)
    parser.add_argument("--page-size", type=int, default=MAX_PAGE_SIZE)
    parser.add_argument("--max-records", type=int)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--observed-at")
    parser.add_argument("--census-output", type=Path, required=True)
    parser.add_argument("--mapping-request-output", type=Path)
    parser.add_argument("--domain-mapping", type=Path)
    parser.add_argument("--domain-mapping-approval-receipt", type=Path)
    parser.add_argument("--manifest-output", type=Path)
    parser.add_argument("--receipt-output", type=Path)
    parser.add_argument("--generated-at")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    manifest_requested = any(
        value is not None
        for value in (
            args.domain_mapping,
            args.domain_mapping_approval_receipt,
            args.manifest_output,
            args.receipt_output,
        )
    )
    if manifest_requested and not all(
        value is not None
        for value in (
            args.domain_mapping,
            args.domain_mapping_approval_receipt,
            args.manifest_output,
            args.receipt_output,
        )
    ):
        raise ExportError(
            "manifest_arguments_incomplete",
            "domain mapping, manifest output, and receipt output are required together",
        )
    client = MeegleClient(timeout_seconds=args.timeout_seconds)
    scan = scan_workloads(
        client,
        repo_root=args.repo_root.resolve(),
        project_key=args.project_key,
        work_item_type=args.work_item_type,
        page_size=args.page_size,
        max_records=args.max_records,
        observed_at=args.observed_at,
    )
    census_sha256 = _secure_atomic_write(
        _absolute_no_resolve(args.census_output), scan.census
    )
    summary: dict[str, Any] = {
        "ok": True,
        "mode": "census",
        "census_sha256": census_sha256,
        "records_seen": scan.census["statistics"]["records_seen"],
        "valid_work_items": scan.census["statistics"]["valid_work_item_count"],
        "snapshot_stable": scan.census["statistics"]["snapshot_stable"],
    }
    if args.mapping_request_output is not None:
        mapping_request = build_mapping_request(
            scan,
            repo_root=args.repo_root.resolve(),
            census_file_sha256=census_sha256,
        )
        mapping_request_sha256 = _secure_atomic_write(
            _absolute_no_resolve(args.mapping_request_output), mapping_request
        )
        summary["mapping_request_sha256"] = mapping_request_sha256
    if manifest_requested:
        mapping = load_domain_mapping(
            _absolute_no_resolve(args.domain_mapping),
            approval_receipt_path=_absolute_no_resolve(
                args.domain_mapping_approval_receipt
            ),
            project_key=args.project_key,
            work_item_type=args.work_item_type,
            taxonomy_options=scan.taxonomy_options,
            taxonomy_sha256=scan.taxonomy_sha256,
        )
        manifest, receipt = build_manifest(
            scan,
            mapping,
            generated_at=args.generated_at or _timestamp(),
        )
        manifest_sha256 = _secure_atomic_write(
            _absolute_no_resolve(args.manifest_output), manifest
        )
        receipt_sha256 = _secure_atomic_write(
            _absolute_no_resolve(args.receipt_output), receipt
        )
        summary.update(
            {
                "mode": "manifest",
                "manifest_sha256": manifest_sha256,
                "receipt_sha256": receipt_sha256,
                "case_count": len(manifest["cases"]),
            }
        )
    print(json.dumps(summary, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ExportError as error:
        print(
            json.dumps(
                {"ok": False, "error": error.code, "detail": error.detail},
                sort_keys=True,
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        raise SystemExit(2) from error
