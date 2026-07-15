"""Schemas for host-side PNC/G1Q3 RCA intake and VM execution handoff."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass, replace as dataclass_replace
from datetime import datetime, timezone
import json
import re
from typing import Any, Literal

from gateway.pnc_rca_data_access import (
    RemoteDataAccessError,
    build_blocked_remote_data_access,
    build_remote_data_access,
)


# === RCA_REQUEST_CONTRACT:BEGIN (do not edit between markers without updating host copy) ===
RCA_ISSUE_CONTEXT_SCHEMA_VERSION = "pnc_rca_issue_context_v1"
RCA_INTAKE_STATE_SCHEMA_VERSION = "pnc_rca_intake_state_v1"
RCA_EXECUTION_REQUEST_SCHEMA_VERSION = "g1q3_rca_execution_request_v2"
RCA_EXECUTION_RESULT_SCHEMA_VERSION = "g1q3_rca_execution_result_v1"
RCA_TOOLCHAIN_FINGERPRINT_SCHEMA_VERSION = "g1q3_rca_toolchain_v1"

SourceQuality = Literal["full", "partial", "fallback_raw_text", "unavailable"]
RequestKind = Literal["issue_intake", "status_check"]


@dataclass(frozen=True)
class RcaIssueContext:
    project_key: str = ""
    work_item_type: str = "issue"
    work_item_id: str = ""
    url: str = ""
    title: str = ""
    status: str = ""
    owners: list[str] = field(default_factory=list)
    project_label: str = ""
    case_id: str = ""
    frame_id: str = ""
    frame_lookup: dict[str, Any] = field(default_factory=dict)
    frame_reference_error: str = ""
    function_category: str = ""
    function_domain: str = ""
    pdcl_download_cmd: str = ""
    is_pdcl_format: bool | None = None
    root_cause_text: str = ""
    description_markdown: str = ""
    comments_timeline: list[dict[str, Any]] = field(default_factory=list)
    media_refs: list[dict[str, Any]] = field(default_factory=list)
    source_quality: SourceQuality = "unavailable"
    blockers: list[dict[str, Any]] = field(default_factory=list)



def validate_issue_context_fields(issue_context: RcaIssueContext) -> tuple[RcaIssueContext, dict[str, Any] | None]:
    """Validate host-extracted issue fields before VM handoff.

    This is intentionally host-side: VM receives normalized fields and blockers;
    it must not infer whether Feishu was unreadable vs a field was missing.
    """
    if issue_context.source_quality == "unavailable":
        return issue_context, None
    source_value = _sanitize_string(issue_context.pdcl_download_cmd)
    if not source_value:
        return dataclasses_replace(issue_context, is_pdcl_format=False), {
            "kind": "issue_field_missing_remote_data_reference",
            "sub_kind": "empty",
            "field": "问题数据地址_PDCL",
            "message": "主控已读取问题卡片，但未提取到可远程读取的 event/clip 数据引用",
            "retryable": True,
        }
    try:
        build_remote_data_access(source_value)
    except RemoteDataAccessError as exc:
        return dataclasses_replace(issue_context, is_pdcl_format=False), {
            "kind": "issue_field_invalid_remote_data_reference",
            "sub_kind": exc.code,
            "field": "问题数据地址_PDCL",
            "message": "主控已读取问题卡片，但 问题数据地址_PDCL 无法解析为 RemoteEventReader/RemoteClipReader 引用",
            "retryable": True,
        }
    if issue_context.frame_reference_error:
        return dataclasses_replace(issue_context, is_pdcl_format=True), {
            "kind": "issue_field_invalid_frame_reference",
            "sub_kind": issue_context.frame_reference_error,
            "field": "问题发生frame_id",
            "message": "主控已读取问题卡片，但 问题发生frame_id 既不是正整数帧号，也不是支持的精确时间格式",
            "retryable": True,
        }
    return dataclasses_replace(issue_context, is_pdcl_format=True), None


def dataclasses_replace(value: RcaIssueContext, **updates: Any) -> RcaIssueContext:
    return dataclass_replace(value, **updates)

@dataclass(frozen=True)
class RcaIntakeState:
    schema_version: str = RCA_INTAKE_STATE_SCHEMA_VERSION
    task_id: str = ""
    stage: str = "admitted"
    group_binding_id: str = ""
    source: dict[str, Any] = field(default_factory=dict)
    request_text_excerpt: str = ""
    issue_context: RcaIssueContext | dict[str, Any] | None = None
    execution_request_path: str = ""
    vm_task_id: str = ""
    blocker: dict[str, Any] | None = None
    retryable: bool = False
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


@dataclass(frozen=True)
class RcaExecutionRequest:
    schema_version: str = RCA_EXECUTION_REQUEST_SCHEMA_VERSION
    request_kind: RequestKind = "issue_intake"
    work_item: dict[str, Any] = field(default_factory=dict)
    case: dict[str, Any] = field(default_factory=dict)
    data: dict[str, Any] = field(default_factory=dict)
    evidence: dict[str, Any] = field(default_factory=dict)
    execution_policy: dict[str, Any] = field(default_factory=dict)
    source_refs: dict[str, Any] = field(default_factory=dict)
    toolchain: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RcaExecutionResult:
    schema_version: str = RCA_EXECUTION_RESULT_SCHEMA_VERSION
    task_id: str = ""
    status: Literal["completed", "blocked", "need_evidence", "need_review", "failed"] = "blocked"
    work_item_id: str = ""
    case_id: str = ""
    l0: str = ""
    l1: str = ""
    gates: dict[str, Any] = field(default_factory=dict)
    artifacts: dict[str, Any] = field(default_factory=dict)
    blocker: dict[str, Any] | None = None
    readback: dict[str, Any] = field(default_factory=lambda: {"safe_for_group": False, "text": ""})


def _sanitize_string(value: str, *, limit: int | None = None) -> str:
    text = str(value or "").replace("\r\n", "\n").strip()
    text = re.sub(r"<!--.*?-->", "", text, flags=re.DOTALL)
    text = re.sub(
        r"https?://(?:[a-z0-9-]+\.)*(?:feishu\.cn|larksuite\.com)"
        r"/[^\s<>\"'\])]*?(?:/file/(?:stream/)?(?:download|preview)|"
        r"/attachment/(?:stream/)?(?:download|preview))"
        r"[^\s<>\"'\])]*",
        "[attachment]",
        text,
        flags=re.IGNORECASE,
    )
    if limit is not None and len(text) > limit:
        return text[:limit].rstrip() + "..."
    return text


def _sanitize_mapping(value: Any) -> Any:
    if is_dataclass(value):
        return _sanitize_mapping(asdict(value))
    if isinstance(value, dict):
        sanitized: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key)
            if key_text.lower() in {"raw", "raw_payload", "raw_feishu_payload", "full_payload", "secret", "token"}:
                continue
            sanitized[key_text] = _sanitize_mapping(item)
        return sanitized
    if isinstance(value, list):
        return [_sanitize_mapping(item) for item in value]
    if isinstance(value, str):
        return _sanitize_string(value)
    return value


def _redact_remote_source_value(value: Any, source_value: str) -> Any:
    """Remove the legacy address envelope from every VM-bound evidence field."""
    source = str(source_value or "").strip()
    if not source:
        return value
    if isinstance(value, str):
        return value.replace(source, "[remote data reference redacted]")
    if isinstance(value, dict):
        return {
            str(key): _redact_remote_source_value(item, source)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact_remote_source_value(item, source) for item in value]
    return value


def to_dict(value: Any) -> dict[str, Any]:
    """Convert schema dataclasses to a deterministic, privacy-light dict."""
    data = _sanitize_mapping(value)
    return data if isinstance(data, dict) else {}


def to_json(value: Any) -> str:
    """Serialize schema dataclasses with stable ordering for receipts/tests."""
    return json.dumps(to_dict(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def issue_context_from_compact_text(
    *,
    project_key: str = "",
    work_item_id: str = "",
    url: str = "",
    compact_text: str = "",
    source_quality: SourceQuality = "unavailable",
    blockers: list[dict[str, Any]] | None = None,
) -> RcaIssueContext:
    """Build a safe issue context from an existing compact text block.

    This intentionally does not retain raw Feishu payloads.  It opportunistically
    lifts common compact lines into typed fields while preserving a bounded
    description mirror for VM weak parsing.
    """
    text = _sanitize_string(compact_text, limit=6000)

    def line_value(label: str) -> str:
        prefix = f"- {label}:"
        for line in text.splitlines():
            if line.startswith(prefix):
                return line[len(prefix):].strip()
        return ""

    frame_lookup: dict[str, Any] = {}
    frame_lookup_text = line_value("frame_lookup")
    if frame_lookup_text:
        try:
            parsed_frame_lookup = json.loads(frame_lookup_text)
        except json.JSONDecodeError:
            parsed_frame_lookup = None
        if isinstance(parsed_frame_lookup, dict):
            frame_lookup = parsed_frame_lookup

    blockers_value = list(blockers or [])
    if not text and not blockers_value and source_quality == "unavailable":
        blockers_value.append({"kind": "host_preread_unavailable", "message": "Feishu issue preread unavailable"})
    return RcaIssueContext(
        project_key=project_key,
        work_item_id=work_item_id,
        url=url,
        title=line_value("title"),
        status=line_value("当前状态"),
        owners=[part.strip() for part in line_value("当前负责人").split(",") if part.strip()],
        project_label=line_value("所属项目"),
        frame_id=line_value("frame_id"),
        frame_lookup=frame_lookup,
        frame_reference_error=line_value("frame_reference_error"),
        pdcl_download_cmd=line_value("数据地址"),
        root_cause_text=line_value("根因分析字段"),
        description_markdown=text,
        source_quality=source_quality,
        blockers=blockers_value,
    )


def build_execution_request(
    *,
    request_kind: RequestKind,
    task_id: str,
    issue_context: RcaIssueContext,
    request_text_excerpt: str = "",
    source_group_id: str = "",
    source_message_id: str = "",
    artifact_root: str = "",
    artifact_cifs_root: str = "",
    allow_download: bool = False,
    allow_feishu_writeback: bool = False,
    group_response_cap: str = "L1",
    translate_baseline: str = "production",
    translate_contract_path: str = "",
    toolchain: dict[str, Any] | None = None,
) -> RcaExecutionRequest:
    """Build the fixed VM execution request contract from host intake context."""
    if allow_download:
        raise ValueError("MDI download is forbidden by the RCA remote-read contract")
    try:
        data_access = build_remote_data_access(issue_context.pdcl_download_cmd)
    except RemoteDataAccessError as exc:
        if not issue_context.blockers:
            raise
        data_access = build_blocked_remote_data_access(
            issue_context.pdcl_download_cmd, exc
        )
    case_id = issue_context.case_id or ""
    return RcaExecutionRequest(
        request_kind=request_kind,
        work_item={
            "project_key": issue_context.project_key,
            "work_item_type": issue_context.work_item_type,
            "work_item_id": issue_context.work_item_id,
            "url": issue_context.url,
            "title": issue_context.title,
            "status": issue_context.status,
            "owners": issue_context.owners,
        },
        case={
            "case_id": case_id,
            "frame_id": issue_context.frame_id,
            "frame_lookup": issue_context.frame_lookup,
            "function_category": issue_context.function_category,
            "function_domain": issue_context.function_domain,
            "project_label": issue_context.project_label,
        },
        data={
            "data_access": data_access,
            "artifact_root": artifact_root,
            "artifact_cifs_root": artifact_cifs_root,
        },
        evidence=_redact_remote_source_value({
            "source_quality": issue_context.source_quality,
            "root_cause_text": issue_context.root_cause_text,
            "description_markdown": issue_context.description_markdown,
            "comments_timeline": issue_context.comments_timeline,
            "media_refs": issue_context.media_refs,
            "blockers": issue_context.blockers,
        }, issue_context.pdcl_download_cmd),
        execution_policy={
            "mode": (
                "remote_read"
                if data_access.get("status") != "blocked"
                else "remote_read_blocked"
            ),
            "data_access_mode": "remote_read",
            "allow_download": False,
            "input_materialization": "forbidden",
            "derived_artifacts_allowed": True,
            "allow_feishu_writeback": bool(allow_feishu_writeback),
            "group_response_cap": group_response_cap,
            "artifact_root": artifact_root,
            "translate_baseline": _sanitize_string(translate_baseline or "production", limit=80),
            "translate_contract_path": _sanitize_string(translate_contract_path, limit=500),
        },
        source_refs={
            "task_id": task_id,
            "source_group_id": source_group_id,
            "source_message_id": source_message_id,
            "request_text_excerpt": _sanitize_string(request_text_excerpt, limit=1200),
        },
        toolchain=_sanitize_mapping(toolchain or {}),
    )
# === RCA_REQUEST_CONTRACT:END ===
