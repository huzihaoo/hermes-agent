"""Strict delivery identity and artifact verification for Kafka-triggered RCA."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import hashlib
import json
import posixpath
import re
import uuid
from pathlib import PurePosixPath
from typing import Any, Mapping, Sequence
from urllib.parse import quote, unquote, urlparse

from gateway.pnc_rca_admission import RcaAdmission, validate_rca_admission
from scripts.pnc_foxglove_delivery import (
    canonical_viz_mcap_path,
    foxglove_url,
    validate_foxglove_url,
)


DELIVERY_CONTRACT_SCHEMA_VERSION = "g1q3_delivery_contract_v1"
DELIVERY_MANIFEST_SCHEMA_VERSION = "delivery_manifest_v2"
DELIVERY_EFFECT_SCHEMA_VERSION_V1 = "pnc_rca_delivery_effect_v1"
DELIVERY_EFFECT_SCHEMA_VERSION = "pnc_rca_delivery_effect_v2"
TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_V1 = "pnc_rca_terminal_delivery_effect_v1"
TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION = "pnc_rca_terminal_delivery_effect_v2"
TERMINAL_DIAGNOSTIC_CONTRACT_SCHEMA_VERSION = "pnc_rca_terminal_diagnostic_v1"
DELIVERY_KEY_VERSION = "v1"
DELIVERY_EFFECT_KIND = "feishu_issue_comment"
DELIVERY_THREAD_EFFECT_KIND = "feishu_thread_reply"
DELIVERY_EFFECT_KINDS = frozenset(
    {DELIVERY_EFFECT_KIND, DELIVERY_THREAD_EFFECT_KIND}
)
DELIVERY_TARGET_SCHEMA_VERSION = "pnc_rca_delivery_target_v1"
TERMINAL_DELIVERY_OUTCOMES = frozenset({"terminal_failed", "quarantined"})
_VM_TMP_PREFIX = "/mnt/tmp/"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,191}$")
_FEISHU_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{1,255}$")
_TERMINAL_CODE_RE = re.compile(r"^[a-z][a-z0-9_]{0,119}$")
_ARTIFACT_SET_ID_RE = re.compile(
    r"^g1q3-rca-artifact-v1-[0-9a-f]{64}$"
)
_FORMAL_REPORT_SEGMENT_RE = re.compile(r"^(?:[A-Za-z0-9._~-]|%[0-9A-Fa-f]{2})+$")
_FORMAL_REPORT_HOST = "192.168.26.174"
_FORMAL_REPORT_PORT = 18081
_FORMAL_REPORT_CIFS_HOST = "hfs1.minieye.tech"
_FORMAL_REPORT_SHARE = "department-pnc_team-planning_algo-driving"
_FORMAL_REPORT_CIFS_PREFIX = (
    f"//{_FORMAL_REPORT_CIFS_HOST}/{_FORMAL_REPORT_SHARE}/tmp/"
)
_DELIVERY_MANIFEST_V2_FIELDS = frozenset(
    {
        "schema_version",
        "sealed",
        "submission_key",
        "business_key",
        "generation",
        "project_key",
        "work_item_type_key",
        "work_item_id",
        "artifact_revision",
        "sealed_at",
        "deliverable_kind",
        "dependencies_complete",
        "artifact_root",
        "html_validation",
        "artifacts",
        "artifact_set_id",
        "report_vm_path",
        "report_cifs_path",
        "report_url",
    }
)
_DELIVERY_MANIFEST_ARTIFACT_FIELDS = frozenset(
    {"role", "path", "size", "sha256", "media_type", "required"}
)
_DELIVERY_HTML_VALIDATION_FIELDS = frozenset(
    {"state", "report_data_sha256", "blockers", "fidelity_ok"}
)
MAX_DELIVERY_ARTIFACTS = 512
MAX_DELIVERY_ARTIFACT_BYTES = 256 * 1024 * 1024
MAX_DELIVERY_ARTIFACT_TOTAL_BYTES = 512 * 1024 * 1024
MAX_DELIVERY_INDEX_HTML_BYTES = 32 * 1024 * 1024
MAX_FEISHU_COMMENT_BYTES = 8 * 1024
MAX_CONCLUSION_BYTES = 2 * 1024
RCA_RESULT_FIELD_KEY = "field_9193cb"
RCA_REPORT_FIELD_KEY = "field_8c912e"
DELIVERY_REPORT_LINK_KIND = "manifest_html"
_HTML_REPORT_STATUSES = frozenset(
    {"html_delivery_ready", "report_generated_need_review", "report_ready"}
)
_VIZ_REPORT_STATUSES = frozenset({"report_ready"})
_VIZ_PUBLICATION_SCHEMA_VERSION = "g1q3_rca_viz_publication_v1"


class DeliveryContractError(ValueError):
    """A permanent artifact or identity error that must fail closed."""

    def __init__(self, code: str, detail: str = ""):
        self.code = str(code or "delivery_contract_invalid")[:120]
        self.detail = str(detail or self.code)[:1000]
        super().__init__(self.detail)


@dataclass(frozen=True)
class VerifiedArtifact:
    role: str
    path: str
    relative_path: str
    size: int
    sha256: str
    media_type: str
    required: bool


@dataclass(frozen=True)
class VerifiedDelivery:
    delivery_id: str
    effect_key: str
    semantic_payload_sha256: str
    artifact_set_id: str
    business_key: str
    submission_key: str
    generation: int
    project_key: str
    work_item_type_key: str
    work_item_id: str
    target_key: str
    issue_url: str
    report_url: str
    viz_mcap_vm: str
    foxglove_url: str
    conclusion: str
    marker: str
    manifest: dict[str, Any]
    contract: dict[str, Any]
    artifacts: tuple[VerifiedArtifact, ...]
    effect_payload: dict[str, Any]

    def job_payload(self) -> dict[str, Any]:
        return {
            "delivery_id": self.delivery_id,
            "artifact_set_id": self.artifact_set_id,
            "business_key": self.business_key,
            "submission_key": self.submission_key,
            "generation": self.generation,
            "project_key": self.project_key,
            "work_item_type_key": self.work_item_type_key,
            "work_item_id": self.work_item_id,
            "target_key": self.target_key,
            "issue_url": self.issue_url,
            "report_url": self.report_url,
            "viz_mcap_vm": self.viz_mcap_vm,
            "foxglove_url": self.foxglove_url,
            "manifest": self.manifest,
            "contract": self.contract,
            "artifacts": [asdict(item) for item in self.artifacts],
        }


@dataclass(frozen=True)
class VerifiedTerminalDelivery:
    delivery_id: str
    effect_key: str
    semantic_payload_sha256: str
    outcome_key: str
    outcome: str
    terminal_state: str
    error_code: str
    business_key: str
    submission_key: str
    generation: int
    project_key: str
    work_item_type_key: str
    work_item_id: str
    target_key: str
    diagnostic_code: str
    diagnostic_result: str
    marker: str
    contract: dict[str, Any]
    effect_payload: dict[str, Any]

    def job_payload(self) -> dict[str, Any]:
        return {
            "delivery_id": self.delivery_id,
            "outcome_key": self.outcome_key,
            "outcome": self.outcome,
            "terminal_state": self.terminal_state,
            "error_code": self.error_code,
            "business_key": self.business_key,
            "submission_key": self.submission_key,
            "generation": self.generation,
            "project_key": self.project_key,
            "work_item_type_key": self.work_item_type_key,
            "work_item_id": self.work_item_id,
            "target_key": self.target_key,
            "issue_url": "",
            "report_url": "",
            "manifest": {},
            "contract": self.contract,
            "artifacts": [],
        }


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _stable_key(prefix: str, material: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(_canonical_json(material).encode("utf-8")).hexdigest()
    return f"{prefix}-{digest}"


_V1_BASE_EFFECT_SEMANTIC_FIELDS = (
    "schema_version",
    "delivery_id",
    "effect_kind",
    "target_key",
    "project_key",
    "work_item_type_key",
    "work_item_id",
    "issue_url",
    "artifact_set_id",
    "report_url",
    "report_cifs_path",
    "viz_mcap_vm",
    "foxglove_url",
    "report_status",
    "requires_human_review",
    "conclusion",
    "field_updates",
)
_BASE_EFFECT_SEMANTIC_FIELDS = (
    *_V1_BASE_EFFECT_SEMANTIC_FIELDS,
    "report_link_kind",
)
_THREAD_EFFECT_SEMANTIC_FIELDS = (
    "platform",
    "chat_id",
    "thread_id",
    "reply_anchor_message_id",
    "source_message_id",
    "requester_id",
    "reply_in_thread",
    "output_cap",
)
_TERMINAL_V1_BASE_EFFECT_SEMANTIC_FIELDS = (
    "schema_version",
    "delivery_id",
    "effect_kind",
    "target_key",
    "project_key",
    "work_item_type_key",
    "work_item_id",
    "outcome",
    "terminal_state",
    "error_code",
    "submission_key",
    "generation",
)
_TERMINAL_BASE_EFFECT_SEMANTIC_FIELDS = (
    *_TERMINAL_V1_BASE_EFFECT_SEMANTIC_FIELDS,
    "diagnostic_code",
    "diagnostic_result",
    "field_updates",
)

_TERMINAL_DIAGNOSTIC_RESULTS = {
    "input_remote_data_required": (
        "自动归因未完成（非归因结论）：问题单缺少问题数据地址，请补充有效的 event/clip 引用后重试。"
    ),
    "input_remote_data_invalid": (
        "自动归因未完成（非归因结论）：问题数据地址无法解析，请修正 event/clip 引用后重试。"
    ),
    "input_frame_required": (
        "自动归因未完成（非归因结论）：缺少或无法解析问题发生 frame_id/时间，请修正后重试。"
    ),
    "input_required": (
        "自动归因未完成（非归因结论）：问题单缺少自动分析所需输入，请补齐后重试。"
    ),
    "issue_source_unavailable": (
        "自动归因未完成（非归因结论）：本次未能可靠读取问题单输入，请恢复读取链路后重试。"
    ),
    "submission_failed": (
        "自动归因未完成（非归因结论）：任务在提交前终止，请由系统维护者排查并显式重试。"
    ),
    "analysis_failed": (
        "自动归因未完成（非归因结论）：自动分析任务异常终止，请由系统维护者排查并显式重试。"
    ),
}
_TERMINAL_INPUT_DIAGNOSTIC_CODES = {
    "issue_field_missing_remote_data_reference": "input_remote_data_required",
    "issue_field_invalid_remote_data_reference": "input_remote_data_invalid",
    "issue_field_invalid_frame_reference": "input_frame_required",
    "issue_fields_not_ready": "input_required",
}
_TERMINAL_SOURCE_DIAGNOSTIC_CODES = frozenset(
    {
        "host_issue_preread_empty",
        "host_issue_preread_failed",
        "host_issue_preread_timeout",
        "host_issue_preread_unavailable",
        "host_mcp_preread_empty",
        "host_mcp_preread_failed",
        "host_mcp_preread_timeout",
        "host_meegle_preread_empty",
        "host_meegle_preread_failed",
        "host_meegle_preread_timeout",
        "host_meegle_preread_unauthenticated",
        "issue_enrichment_not_ready",
        "issue_not_visible",
    }
)


def _terminal_semantic_fields(schema_version: Any) -> tuple[str, ...]:
    if schema_version == TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_V1:
        return _TERMINAL_V1_BASE_EFFECT_SEMANTIC_FIELDS
    if schema_version == TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION:
        return _TERMINAL_BASE_EFFECT_SEMANTIC_FIELDS
    raise DeliveryContractError("terminal_delivery_schema_unsupported")


def terminal_diagnostic_code(
    error_code: Any,
    *,
    source_error_code: Any = "",
) -> str:
    """Project private terminal causes into a small, non-sensitive public taxonomy."""

    public_code = str(error_code or "").strip().lower()
    source_code = str(source_error_code or "").strip().lower()
    for candidate in (source_code, public_code):
        mapped = _TERMINAL_INPUT_DIAGNOSTIC_CODES.get(candidate)
        if mapped:
            return mapped
        if candidate in _TERMINAL_SOURCE_DIAGNOSTIC_CODES:
            return "issue_source_unavailable"
        if candidate.endswith("_need_keyframe"):
            return "input_frame_required"
        if candidate.endswith("_required_input"):
            return "input_required"
    if public_code == "outbox_submission_quarantined":
        return "submission_failed"
    return "analysis_failed"


def delivery_effect_semantic_payload(
    payload: Mapping[str, Any], effect_kind: str
) -> dict[str, Any]:
    if effect_kind not in DELIVERY_EFFECT_KINDS:
        raise DeliveryContractError("delivery_effect_kind_unsupported")
    schema_version = payload.get("schema_version")
    if schema_version == DELIVERY_EFFECT_SCHEMA_VERSION_V1:
        fields = list(_V1_BASE_EFFECT_SEMANTIC_FIELDS)
    elif schema_version == DELIVERY_EFFECT_SCHEMA_VERSION:
        fields = list(_BASE_EFFECT_SEMANTIC_FIELDS)
    else:
        raise DeliveryContractError("delivery_effect_schema_unsupported")
    if effect_kind == DELIVERY_THREAD_EFFECT_KIND:
        fields.extend(_THREAD_EFFECT_SEMANTIC_FIELDS)
    return {key: payload.get(key) for key in fields}


def compute_delivery_effect_payload_sha256(
    payload: Mapping[str, Any], effect_kind: str
) -> str:
    semantic = delivery_effect_semantic_payload(payload, effect_kind)
    return hashlib.sha256(_canonical_json(semantic).encode("utf-8")).hexdigest()


def compute_delivery_effect_key(
    *,
    delivery_id: str,
    effect_kind: str,
    target_key: str,
    semantic_payload_sha256: str,
) -> str:
    if effect_kind not in DELIVERY_EFFECT_KINDS:
        raise DeliveryContractError("delivery_effect_kind_unsupported")
    return _stable_key(
        "g1q3-rca-effect-v1",
        {
            "key_version": DELIVERY_KEY_VERSION,
            "delivery_id": delivery_id,
            "effect_kind": effect_kind,
            "target_key": target_key,
            "semantic_payload_sha256": semantic_payload_sha256,
        },
    )


def delivery_effect_marker(effect_key: str, artifact_set_id: str) -> str:
    return f"[RCA_DELIVERY:{effect_key}:{artifact_set_id[-12:]}]"


def delivery_effect_idempotency_uuid(effect_key: str) -> str:
    digest = hashlib.sha256(str(effect_key).encode("utf-8")).hexdigest()
    return str(uuid.UUID(digest[:32]))


def build_issue_comment_content(
    *,
    marker: str,
    work_item_id: str,
    report_status: str,
    conclusion: str,
    report_url: str,
    report_cifs_path: str,
) -> str:
    lines = [
        marker,
        "【G1Q3 RCA 机器人报告】RCA 报告已生成，需人工复核后结案。",
        f"问题：{work_item_id}",
        f"报告状态：{report_status}",
    ]
    if conclusion:
        lines.append(f"候选结论：{conclusion}")
    lines.extend(
        [
            f"归因报告：{report_url}",
            f"报告文件（CIFS）：{report_cifs_path}",
            "说明：以上为自动 RCA 候选结论，需人工复核确认后再结案。",
        ]
    )
    content = "\n".join(lines)
    if len(content.encode("utf-8")) > MAX_FEISHU_COMMENT_BYTES:
        raise DeliveryContractError("delivery_comment_too_large")
    return content


def build_thread_reply_content(
    *,
    marker: str,
    work_item_id: str,
    report_status: str,
    conclusion: str,
    report_url: str,
    issue_url: str,
) -> str:
    lines = [
        marker,
        "【G1Q3 RCA 任务话题交付】RCA 报告已生成，需人工复核后结案。",
        f"问题：{work_item_id}",
        f"报告状态：{report_status}",
    ]
    if conclusion:
        lines.append(f"候选结论：{conclusion}")
    lines.extend(
        [
            f"归因报告：{report_url}",
            f"问题单：{issue_url}",
            "说明：以上为自动 RCA 候选结论，需人工复核确认后再结案。",
        ]
    )
    content = "\n".join(lines)
    if len(content.encode("utf-8")) > MAX_FEISHU_COMMENT_BYTES:
        raise DeliveryContractError("delivery_thread_reply_too_large")
    return content


def terminal_delivery_effect_semantic_payload(
    payload: Mapping[str, Any], effect_kind: str
) -> dict[str, Any]:
    if effect_kind not in DELIVERY_EFFECT_KINDS:
        raise DeliveryContractError("delivery_effect_kind_unsupported")
    fields = list(_terminal_semantic_fields(payload.get("schema_version")))
    if effect_kind == DELIVERY_THREAD_EFFECT_KIND:
        fields.extend(_THREAD_EFFECT_SEMANTIC_FIELDS)
    return {key: payload.get(key) for key in fields}


def compute_terminal_delivery_effect_payload_sha256(
    payload: Mapping[str, Any], effect_kind: str
) -> str:
    semantic = terminal_delivery_effect_semantic_payload(payload, effect_kind)
    return hashlib.sha256(_canonical_json(semantic).encode("utf-8")).hexdigest()


def compute_terminal_delivery_effect_key(
    *,
    delivery_id: str,
    effect_kind: str,
    target_key: str,
    semantic_payload_sha256: str,
) -> str:
    if effect_kind not in DELIVERY_EFFECT_KINDS:
        raise DeliveryContractError("delivery_effect_kind_unsupported")
    return _stable_key(
        "g1q3-rca-terminal-effect-v1",
        {
            "key_version": DELIVERY_KEY_VERSION,
            "delivery_id": delivery_id,
            "effect_kind": effect_kind,
            "target_key": target_key,
            "semantic_payload_sha256": semantic_payload_sha256,
        },
    )


def terminal_delivery_effect_marker(
    effect_key: str, outcome: str, generation: int
) -> str:
    if outcome not in TERMINAL_DELIVERY_OUTCOMES:
        raise DeliveryContractError("terminal_delivery_outcome_invalid")
    if isinstance(generation, bool) or not isinstance(generation, int) or generation < 1:
        raise DeliveryContractError("terminal_delivery_generation_invalid")
    return f"[RCA_TERMINAL:{effect_key}:{outcome}:{generation}]"


def _terminal_code(value: Any, field: str) -> str:
    text = str(value or "").strip().lower()
    if not _TERMINAL_CODE_RE.fullmatch(text):
        raise DeliveryContractError(f"terminal_delivery_{field}_invalid")
    return text


def _terminal_content(
    *,
    marker: str,
    outcome: str,
    terminal_state: str,
    error_code: str,
    submission_key: str,
    generation: int,
    thread: bool,
    diagnostic_result: str = "",
) -> str:
    heading = "【G1Q3 RCA 任务话题终态】" if thread else "【G1Q3 RCA 机器人终态】"
    if diagnostic_result:
        lines = [
            marker,
            f"{heading}诊断报告",
            f"诊断结论：{diagnostic_result}",
            f"任务：{submission_key}",
            f"代次：{generation}",
            f"终态：{terminal_state}",
            f"结果：{outcome}",
            f"错误码：{error_code}",
            "本代归因报告：未生成（不存在已验证的 Foxglove 发布产物）。",
            (
                "报告字段：本终态不改写；若问题单现有值非空，可能属于其他代次，"
                f"不代表第 {generation} 代结论。"
            ),
            "说明：本终态诊断不包含自动归因结论；修复输入或系统故障后需显式重试。",
        ]
    else:
        lines = [
            marker,
            f"{heading}本次自动分析未生成可交付报告。",
            f"任务：{submission_key}",
            f"代次：{generation}",
            f"终态：{terminal_state}",
            f"结果：{outcome}",
            f"错误码：{error_code}",
            "说明：未生成或发布 HTML 报告；请根据错误码排查后显式重试。",
        ]
    content = "\n".join(lines)
    if len(content.encode("utf-8")) > MAX_FEISHU_COMMENT_BYTES:
        raise DeliveryContractError("terminal_delivery_content_too_large")
    return content


def build_terminal_delivery(
    *,
    business_key: str,
    submission_key: str,
    generation: int,
    project_key: str,
    work_item_type_key: str,
    work_item_id: str,
    outcome: str,
    terminal_state: str,
    error_code: str,
    source_error_code: str = "",
    diagnostic_code: str = "",
    schema_version: str = TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION,
) -> VerifiedTerminalDelivery:
    values = {
        "business_key": _required_text(business_key, "business_key"),
        "submission_key": _required_text(submission_key, "submission_key"),
        "project_key": _required_text(project_key, "project_key"),
        "work_item_type_key": _required_text(
            work_item_type_key, "work_item_type_key"
        ),
        "work_item_id": _required_text(work_item_id, "work_item_id"),
    }
    if not all(_SAFE_KEY_RE.fullmatch(value) for value in values.values()):
        raise DeliveryContractError("terminal_delivery_identity_invalid")
    if isinstance(generation, bool) or not isinstance(generation, int) or generation < 1:
        raise DeliveryContractError("terminal_delivery_generation_invalid")
    normalized_outcome = _terminal_code(outcome, "outcome")
    if normalized_outcome not in TERMINAL_DELIVERY_OUTCOMES:
        raise DeliveryContractError("terminal_delivery_outcome_invalid")
    normalized_state = _terminal_code(terminal_state, "state")
    normalized_error = _terminal_code(error_code, "error_code")
    if schema_version not in {
        TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_V1,
        TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION,
    }:
        raise DeliveryContractError("terminal_delivery_schema_unsupported")
    target_key = (
        f"feishu_project:{values['project_key']}:{values['work_item_type_key']}:"
        f"{values['work_item_id']}"
    )
    outcome_key = _stable_key(
        "g1q3-rca-terminal-v1",
        {
            "key_version": DELIVERY_KEY_VERSION,
            "submission_key": values["submission_key"],
            "generation": generation,
            "outcome": normalized_outcome,
            "terminal_state": normalized_state,
            "error_code": normalized_error,
        },
    )
    delivery_id = _stable_key(
        "g1q3-rca-terminal-delivery-v1",
        {
            "key_version": DELIVERY_KEY_VERSION,
            "outcome_key": outcome_key,
            "target_key": target_key,
        },
    )
    semantic: dict[str, Any] = {
        "schema_version": schema_version,
        "delivery_id": delivery_id,
        "effect_kind": DELIVERY_EFFECT_KIND,
        "target_key": target_key,
        "project_key": values["project_key"],
        "work_item_type_key": values["work_item_type_key"],
        "work_item_id": values["work_item_id"],
        "outcome": normalized_outcome,
        "terminal_state": normalized_state,
        "error_code": normalized_error,
        "submission_key": values["submission_key"],
        "generation": generation,
    }
    diagnostic_result = ""
    diagnostic_contract: dict[str, Any] = {}
    if schema_version == TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION:
        derived_diagnostic_code = terminal_diagnostic_code(
            normalized_error,
            source_error_code=source_error_code,
        )
        normalized_diagnostic_code = str(diagnostic_code or derived_diagnostic_code).strip()
        if normalized_diagnostic_code not in _TERMINAL_DIAGNOSTIC_RESULTS:
            raise DeliveryContractError("terminal_delivery_diagnostic_code_invalid")
        if (
            normalized_error != "outbox_submission_quarantined"
            and normalized_diagnostic_code != derived_diagnostic_code
        ):
            raise DeliveryContractError("terminal_delivery_diagnostic_code_mismatch")
        diagnostic_result = (
            f"RCA 第 {generation} 代："
            f"{_TERMINAL_DIAGNOSTIC_RESULTS[normalized_diagnostic_code]}"
            "本代未生成已验证归因报告；问题单归因报告字段若非空，"
            f"可能保留自其他代次，不代表第 {generation} 代结论。"
        )
        field_updates = [
            {
                "field_key": RCA_RESULT_FIELD_KEY,
                "field_value": diagnostic_result,
            },
        ]
        semantic.update(
            {
                "diagnostic_code": normalized_diagnostic_code,
                "diagnostic_result": diagnostic_result,
                "field_updates": field_updates,
            }
        )
        diagnostic_contract = {
            "schema_version": TERMINAL_DIAGNOSTIC_CONTRACT_SCHEMA_VERSION,
            "generation": generation,
            "diagnostic_code": normalized_diagnostic_code,
            "diagnostic_result": diagnostic_result,
            "diagnostic_report_status": "not_generated",
            "report_field_write_policy": "preserve_existing",
            "preserved_report_semantics": "other_generation_not_current",
        }
    else:
        normalized_diagnostic_code = ""
    semantic_sha = compute_terminal_delivery_effect_payload_sha256(
        semantic, DELIVERY_EFFECT_KIND
    )
    effect_key = compute_terminal_delivery_effect_key(
        delivery_id=delivery_id,
        effect_kind=DELIVERY_EFFECT_KIND,
        target_key=target_key,
        semantic_payload_sha256=semantic_sha,
    )
    marker = terminal_delivery_effect_marker(
        effect_key, normalized_outcome, generation
    )
    payload = {
        **semantic,
        "effect_key": effect_key,
        "semantic_payload_sha256": semantic_sha,
        "marker": marker,
        "comment_content": _terminal_content(
            marker=marker,
            outcome=normalized_outcome,
            terminal_state=normalized_state,
            error_code=normalized_error,
            submission_key=values["submission_key"],
            generation=generation,
            thread=False,
            diagnostic_result=diagnostic_result,
        ),
    }
    return VerifiedTerminalDelivery(
        delivery_id=delivery_id,
        effect_key=effect_key,
        semantic_payload_sha256=semantic_sha,
        outcome_key=outcome_key,
        outcome=normalized_outcome,
        terminal_state=normalized_state,
        error_code=normalized_error,
        business_key=values["business_key"],
        submission_key=values["submission_key"],
        generation=generation,
        project_key=values["project_key"],
        work_item_type_key=values["work_item_type_key"],
        work_item_id=values["work_item_id"],
        target_key=target_key,
        diagnostic_code=normalized_diagnostic_code,
        diagnostic_result=diagnostic_result,
        marker=marker,
        contract=diagnostic_contract,
        effect_payload=payload,
    )


def build_terminal_thread_reply_effect(
    *,
    issue_effect_payload: Mapping[str, Any],
    target_key: str,
    target: Mapping[str, Any],
) -> tuple[str, str, dict[str, Any]]:
    issue = dict(issue_effect_payload or {})
    if (
        issue.get("schema_version")
        not in {
            TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_V1,
            TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION,
        }
        or issue.get("effect_kind") != DELIVERY_EFFECT_KIND
    ):
        raise DeliveryContractError("terminal_delivery_primary_effect_invalid")
    expected_issue_sha = compute_terminal_delivery_effect_payload_sha256(
        issue, DELIVERY_EFFECT_KIND
    )
    if issue.get("semantic_payload_sha256") != expected_issue_sha:
        raise DeliveryContractError("terminal_delivery_primary_effect_invalid")
    validated_target = validate_delivery_subscription_target(
        effect_kind=DELIVERY_THREAD_EFFECT_KIND,
        target_key=target_key,
        target=target,
        project_key=str(issue.get("project_key") or ""),
        work_item_type_key=str(issue.get("work_item_type_key") or ""),
        work_item_id=str(issue.get("work_item_id") or ""),
    )
    semantic = {
        key: issue.get(key)
        for key in _terminal_semantic_fields(issue.get("schema_version"))
        if key not in {"effect_kind", "target_key"}
    }
    semantic.update(
        {
            "effect_kind": DELIVERY_THREAD_EFFECT_KIND,
            "target_key": target_key,
            **{key: validated_target[key] for key in _THREAD_EFFECT_SEMANTIC_FIELDS},
        }
    )
    semantic_sha = compute_terminal_delivery_effect_payload_sha256(
        semantic, DELIVERY_THREAD_EFFECT_KIND
    )
    effect_key = compute_terminal_delivery_effect_key(
        delivery_id=str(semantic.get("delivery_id") or ""),
        effect_kind=DELIVERY_THREAD_EFFECT_KIND,
        target_key=target_key,
        semantic_payload_sha256=semantic_sha,
    )
    outcome = str(semantic.get("outcome") or "")
    generation = semantic.get("generation")
    marker = terminal_delivery_effect_marker(effect_key, outcome, generation)
    payload = {
        **semantic,
        "effect_key": effect_key,
        "semantic_payload_sha256": semantic_sha,
        "marker": marker,
        "idempotency_uuid": delivery_effect_idempotency_uuid(effect_key),
        "message_content": _terminal_content(
            marker=marker,
            outcome=outcome,
            terminal_state=str(semantic.get("terminal_state") or ""),
            error_code=str(semantic.get("error_code") or ""),
            submission_key=str(semantic.get("submission_key") or ""),
            generation=generation,
            thread=True,
            diagnostic_result=str(semantic.get("diagnostic_result") or ""),
        ),
    }
    return effect_key, semantic_sha, payload


def validate_delivery_subscription_target(
    *,
    effect_kind: str,
    target_key: str,
    target: Mapping[str, Any],
    project_key: str,
    work_item_type_key: str,
    work_item_id: str,
) -> dict[str, Any]:
    value = dict(target or {})
    if effect_kind == DELIVERY_EFFECT_KIND:
        expected = {
            "schema_version": DELIVERY_TARGET_SCHEMA_VERSION,
            "platform": "feishu_project",
            "project_key": project_key,
            "work_item_type_key": work_item_type_key,
            "work_item_id": work_item_id,
            "output_cap": "L1",
        }
        expected_key = (
            f"feishu_project:{project_key}:{work_item_type_key}:{work_item_id}"
        )
    elif effect_kind == DELIVERY_THREAD_EFFECT_KIND:
        expected_keys = {
            "schema_version",
            "platform",
            "chat_id",
            "thread_id",
            "reply_anchor_message_id",
            "source_message_id",
            "requester_id",
            "reply_in_thread",
            "output_cap",
        }
        if set(value) != expected_keys:
            raise DeliveryContractError("delivery_subscription_target_invalid")
        chat_id = str(value.get("chat_id") or "").strip()
        anchor = str(value.get("reply_anchor_message_id") or "").strip()
        source_message_id = str(value.get("source_message_id") or "").strip()
        requester_id = str(value.get("requester_id") or "").strip()
        if not all(
            _FEISHU_ID_RE.fullmatch(item)
            for item in (chat_id, anchor, source_message_id, requester_id)
        ) or not (
            chat_id.startswith("oc_")
            and anchor.startswith("om_")
            and source_message_id.startswith("om_")
            and requester_id.startswith("ou_")
        ):
            raise DeliveryContractError("delivery_subscription_target_invalid")
        expected = {
            "schema_version": DELIVERY_TARGET_SCHEMA_VERSION,
            "platform": "feishu",
            "chat_id": chat_id,
            "thread_id": f"topic:{anchor}",
            "reply_anchor_message_id": anchor,
            "source_message_id": source_message_id,
            "requester_id": requester_id,
            "reply_in_thread": True,
            "output_cap": "L1",
        }
        expected_key = f"feishu_thread:{chat_id}:{anchor}"
    else:
        raise DeliveryContractError("delivery_effect_kind_unsupported")
    if value != expected or target_key != expected_key:
        raise DeliveryContractError("delivery_subscription_target_invalid")
    return expected


def build_thread_reply_effect(
    *,
    issue_effect_payload: Mapping[str, Any],
    target_key: str,
    target: Mapping[str, Any],
) -> tuple[str, str, dict[str, Any]]:
    issue = dict(issue_effect_payload or {})
    if issue.get("effect_kind") != DELIVERY_EFFECT_KIND:
        raise DeliveryContractError("delivery_primary_effect_invalid")
    expected_issue_sha = compute_delivery_effect_payload_sha256(
        issue, DELIVERY_EFFECT_KIND
    )
    if issue.get("semantic_payload_sha256") != expected_issue_sha:
        raise DeliveryContractError("delivery_primary_effect_invalid")
    validated_target = validate_delivery_subscription_target(
        effect_kind=DELIVERY_THREAD_EFFECT_KIND,
        target_key=target_key,
        target=target,
        project_key=str(issue.get("project_key") or ""),
        work_item_type_key=str(issue.get("work_item_type_key") or ""),
        work_item_id=str(issue.get("work_item_id") or ""),
    )
    schema_version = issue.get("schema_version")
    if schema_version != DELIVERY_EFFECT_SCHEMA_VERSION:
        raise DeliveryContractError("delivery_effect_schema_unsupported")
    semantic = {
        key: issue.get(key)
        for key in _BASE_EFFECT_SEMANTIC_FIELDS
        if key not in {"effect_kind", "target_key"}
    }
    semantic.update(
        {
            "effect_kind": DELIVERY_THREAD_EFFECT_KIND,
            "target_key": target_key,
            **{
                key: validated_target[key]
                for key in _THREAD_EFFECT_SEMANTIC_FIELDS
            },
        }
    )
    semantic_sha = compute_delivery_effect_payload_sha256(
        semantic, DELIVERY_THREAD_EFFECT_KIND
    )
    effect_key = compute_delivery_effect_key(
        delivery_id=str(semantic.get("delivery_id") or ""),
        effect_kind=DELIVERY_THREAD_EFFECT_KIND,
        target_key=target_key,
        semantic_payload_sha256=semantic_sha,
    )
    artifact_set_id = str(semantic.get("artifact_set_id") or "")
    marker = delivery_effect_marker(effect_key, artifact_set_id)
    conclusion = str(semantic.get("conclusion") or "").strip()
    message_content = build_thread_reply_content(
        marker=marker,
        work_item_id=str(semantic.get("work_item_id") or ""),
        report_status=str(semantic.get("report_status") or ""),
        conclusion=conclusion,
        report_url=str(semantic.get("report_url") or ""),
        issue_url=str(semantic.get("issue_url") or ""),
    )
    payload = {
        **semantic,
        "effect_key": effect_key,
        "semantic_payload_sha256": semantic_sha,
        "marker": marker,
        "idempotency_uuid": delivery_effect_idempotency_uuid(effect_key),
        "message_content": message_content,
    }
    return effect_key, semantic_sha, payload


def _required_text(value: Any, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise DeliveryContractError("delivery_field_missing", f"{field} is required")
    return text


def _truncate_utf8(value: str, limit: int) -> str:
    encoded = value.encode("utf-8")
    if len(encoded) <= limit:
        return value
    suffix = "..."
    body = encoded[: max(0, limit - len(suffix))].decode("utf-8", errors="ignore")
    return body.rstrip() + suffix


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise DeliveryContractError("delivery_field_invalid", f"{field} must be positive")
    if value <= 0:
        raise DeliveryContractError("delivery_field_invalid", f"{field} must be positive")
    return value


def _sha256(value: Any, field: str) -> str:
    text = str(value or "").strip().lower()
    if not _SHA256_RE.fullmatch(text):
        raise DeliveryContractError("artifact_hash_invalid", f"{field} is not SHA-256")
    return text


def canonical_artifact_root(submission_key: str) -> str:
    key = str(submission_key or "").strip()
    if not _SAFE_KEY_RE.fullmatch(key):
        raise DeliveryContractError(
            "delivery_identity_invalid", "submission_key is not a safe path segment"
        )
    return f"{_VM_TMP_PREFIX}{key}/"


def _normalize_root(value: Any, submission_key: str) -> str:
    expected = canonical_artifact_root(submission_key)
    raw = _required_text(value, "delivery_manifest.artifact_root")
    if not raw.startswith("/") or ".." in PurePosixPath(raw).parts or "\x00" in raw:
        raise DeliveryContractError("artifact_root_invalid", f"invalid artifact_root: {raw}")
    normalized = posixpath.normpath(raw).rstrip("/") + "/"
    if normalized != expected:
        raise DeliveryContractError(
            "artifact_root_identity_mismatch",
            f"artifact_root must be exactly {expected}",
        )
    return normalized


def _artifact_path(root: str, value: Any) -> tuple[str, str]:
    raw = _required_text(value, "artifact.path")
    if "\x00" in raw or ".." in PurePosixPath(raw).parts or "\\" in raw:
        raise DeliveryContractError("artifact_path_invalid", f"unsafe artifact path: {raw}")
    absolute = posixpath.normpath(raw if raw.startswith("/") else posixpath.join(root, raw))
    root_no_slash = root.rstrip("/")
    try:
        common = posixpath.commonpath((root_no_slash, absolute))
    except ValueError as exc:
        raise DeliveryContractError("artifact_path_invalid", raw) from exc
    if common != root_no_slash or absolute == root_no_slash:
        raise DeliveryContractError(
            "artifact_path_outside_root", f"artifact path escapes {root}: {raw}"
        )
    relative = posixpath.relpath(absolute, root_no_slash)
    return absolute, relative


def _manifest_artifact_material(manifest: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = manifest.get("artifacts")
    if not isinstance(rows, list) or not rows:
        raise DeliveryContractError(
            "delivery_manifest_artifacts_invalid", "manifest artifacts must be non-empty"
        )
    if len(rows) > MAX_DELIVERY_ARTIFACTS:
        raise DeliveryContractError(
            "delivery_manifest_artifacts_invalid", "manifest contains too many artifacts"
        )
    material: list[dict[str, Any]] = []
    total_size = 0
    for index, item in enumerate(rows):
        if not isinstance(item, Mapping):
            raise DeliveryContractError(
                "delivery_manifest_artifacts_invalid", f"artifact[{index}] must be an object"
            )
        if set(item) != _DELIVERY_MANIFEST_ARTIFACT_FIELDS:
            raise DeliveryContractError(
                "delivery_manifest_artifact_shape_invalid",
                f"artifact[{index}] fields do not match delivery_manifest_v2",
            )
        role = _required_text(item.get("role"), f"artifact[{index}].role")
        path = _required_text(item.get("path"), f"artifact[{index}].path")
        media_type = _required_text(
            item.get("media_type"), f"artifact[{index}].media_type"
        )
        if (
            path.lower().endswith(".mcap")
            or role.lower() in {"mcap", "viz_mcap", "visualization_mcap"}
            or "mcap" in media_type.lower()
        ):
            raise DeliveryContractError(
                "html_delivery_mcap_forbidden",
                "MCAP is not an HTML delivery dependency",
            )
        if not isinstance(item.get("required"), bool):
            raise DeliveryContractError(
                "delivery_field_invalid", f"artifact[{index}].required must be boolean"
            )
        size = _positive_int(item.get("size"), f"artifact[{index}].size")
        if size > MAX_DELIVERY_ARTIFACT_BYTES:
            raise DeliveryContractError(
                "delivery_artifact_file_too_large", path
            )
        total_size += size
        if total_size > MAX_DELIVERY_ARTIFACT_TOTAL_BYTES:
            raise DeliveryContractError("delivery_artifact_bundle_too_large")
        material.append(
            {
                "role": role,
                "path": path,
                "size": size,
                "sha256": _sha256(item.get("sha256"), f"artifact[{index}].sha256"),
                "media_type": media_type,
                "required": item["required"],
            }
        )
    return sorted(material, key=lambda row: (row["role"], row["path"]))


def _sealed_at(value: Any) -> str:
    text = _required_text(value, "delivery_manifest.sealed_at")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DeliveryContractError("delivery_manifest_sealed_at_invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise DeliveryContractError("delivery_manifest_sealed_at_invalid")
    return text


def _html_validation_material(manifest: Mapping[str, Any]) -> dict[str, Any]:
    value = manifest.get("html_validation")
    if not isinstance(value, Mapping):
        raise DeliveryContractError("html_validation_missing")
    if set(value) != _DELIVERY_HTML_VALIDATION_FIELDS:
        raise DeliveryContractError("html_validation_shape_invalid")
    if value.get("state") != "html_delivery_ready":
        raise DeliveryContractError("html_validation_state_invalid")
    blockers = value.get("blockers")
    if not isinstance(blockers, list) or blockers:
        raise DeliveryContractError("html_validation_blocked")
    if value.get("fidelity_ok") is not True:
        raise DeliveryContractError("html_validation_fidelity_failed")
    return {
        "state": "html_delivery_ready",
        "report_data_sha256": _sha256(
            value.get("report_data_sha256"),
            "delivery_manifest.html_validation.report_data_sha256",
        ),
        "blockers": [],
        "fidelity_ok": True,
    }


def _validate_manifest_v2_shape(manifest: Mapping[str, Any]) -> None:
    if set(manifest) != _DELIVERY_MANIFEST_V2_FIELDS:
        raise DeliveryContractError("delivery_manifest_shape_invalid")


def compute_artifact_set_id(manifest: Mapping[str, Any]) -> str:
    """Recompute immutable artifact identity without volatile timestamps."""
    material = {
        "key_version": DELIVERY_KEY_VERSION,
        "schema_version": manifest.get("schema_version"),
        "submission_key": manifest.get("submission_key"),
        "business_key": manifest.get("business_key"),
        "generation": manifest.get("generation"),
        "project_key": manifest.get("project_key"),
        "work_item_type_key": manifest.get("work_item_type_key"),
        "work_item_id": manifest.get("work_item_id"),
        "artifact_revision": _positive_int(
            manifest.get("artifact_revision"),
            "delivery_manifest.artifact_revision",
        ),
        "sealed_at": _sealed_at(manifest.get("sealed_at")),
        "deliverable_kind": manifest.get("deliverable_kind"),
        "dependencies_complete": manifest.get("dependencies_complete"),
        "html_validation": _html_validation_material(manifest),
        "artifacts": _manifest_artifact_material(manifest),
    }
    return _stable_key("g1q3-rca-artifact-v1", material)


def _store_mismatch(role: str, detail: str) -> DeliveryContractError:
    code = (
        f"delivery_{role}_store_mismatch"
        if role in {"index_html", "report_data"}
        else "delivery_artifact_store_mismatch"
    )
    return DeliveryContractError(code, detail)


def verify_persisted_artifact_inventory(
    *,
    manifest: Mapping[str, Any],
    stored_artifacts: Sequence[Mapping[str, Any]],
    expected_artifact_set_id: str,
) -> tuple[VerifiedArtifact, ...]:
    """Rebuild a sealed manifest inventory and match every persisted row exactly."""
    if manifest.get("schema_version") != DELIVERY_MANIFEST_SCHEMA_VERSION:
        raise DeliveryContractError("delivery_manifest_schema_unsupported")
    if manifest.get("sealed") is not True:
        raise DeliveryContractError("delivery_manifest_not_sealed")
    if manifest.get("deliverable_kind") != "html":
        raise DeliveryContractError("delivery_kind_unsupported")
    if manifest.get("dependencies_complete") is not True:
        raise DeliveryContractError("delivery_dependencies_incomplete")
    computed_artifact_set_id = compute_artifact_set_id(manifest)
    _validate_manifest_v2_shape(manifest)
    if (
        manifest.get("artifact_set_id") != expected_artifact_set_id
        or computed_artifact_set_id != expected_artifact_set_id
    ):
        raise DeliveryContractError("delivery_manifest_store_hash_mismatch")

    submission_key = _required_text(
        manifest.get("submission_key"), "delivery_manifest.submission_key"
    )
    _validate_report_url(
        manifest.get("report_url"),
        submission_key=submission_key,
        artifact_set_id=expected_artifact_set_id,
    )
    _validate_report_identity_paths(
        manifest,
        submission_key=submission_key,
        artifact_set_id=expected_artifact_set_id,
    )
    root = _normalize_root(manifest.get("artifact_root"), submission_key)
    material = _manifest_artifact_material(manifest)
    expected: list[VerifiedArtifact] = []
    expected_roles: set[str] = set()
    expected_paths: set[str] = set()
    expected_relative_paths: set[str] = set()
    expected_total = 0
    for item in material:
        absolute, relative = _artifact_path(root, item["path"])
        role = item["role"]
        if (
            role in expected_roles
            or absolute in expected_paths
            or relative in expected_relative_paths
        ):
            raise DeliveryContractError(
                "delivery_manifest_duplicate_artifact", role
            )
        size = item["size"]
        if size > MAX_DELIVERY_ARTIFACT_BYTES:
            raise DeliveryContractError(
                "delivery_artifact_file_too_large", absolute
            )
        expected_total += size
        if expected_total > MAX_DELIVERY_ARTIFACT_TOTAL_BYTES:
            raise DeliveryContractError("delivery_artifact_bundle_too_large")
        expected_roles.add(role)
        expected_paths.add(absolute)
        expected_relative_paths.add(relative)
        expected.append(
            VerifiedArtifact(
                role=role,
                path=absolute,
                relative_path=relative,
                size=size,
                sha256=item["sha256"],
                media_type=item["media_type"],
                required=item["required"],
            )
        )

    if (
        not isinstance(stored_artifacts, Sequence)
        or isinstance(stored_artifacts, (str, bytes, bytearray))
        or not stored_artifacts
        or len(stored_artifacts) > MAX_DELIVERY_ARTIFACTS
    ):
        raise DeliveryContractError("delivery_artifact_inventory_invalid")
    stored_by_role: dict[str, VerifiedArtifact] = {}
    stored_paths: set[str] = set()
    stored_relative_paths: set[str] = set()
    stored_total = 0
    for index, row in enumerate(stored_artifacts):
        if not isinstance(row, Mapping):
            raise DeliveryContractError(
                "delivery_artifact_inventory_invalid",
                f"stored artifact[{index}] must be an object",
            )
        role = _required_text(row.get("role"), f"stored artifact[{index}].role")
        raw_path = _required_text(
            row.get("path"), f"stored artifact[{index}].path"
        )
        absolute, path_relative = _artifact_path(root, raw_path)
        relative = _required_text(
            row.get("relative_path"),
            f"stored artifact[{index}].relative_path",
        )
        relative_path = PurePosixPath(relative)
        if (
            relative_path.is_absolute()
            or ".." in relative_path.parts
            or "\\" in relative
            or "\x00" in relative
            or posixpath.normpath(relative) != relative
        ):
            raise DeliveryContractError("artifact_path_invalid", relative)
        if raw_path != absolute or relative != path_relative:
            raise _store_mismatch(role, "stored artifact path is not canonical")
        media_type = _required_text(
            row.get("media_type"), f"stored artifact[{index}].media_type"
        )
        if (
            raw_path.lower().endswith(".mcap")
            or role.lower() in {"mcap", "viz_mcap", "visualization_mcap"}
            or "mcap" in media_type.lower()
        ):
            raise DeliveryContractError("html_delivery_mcap_forbidden")
        size = _positive_int(row.get("size"), f"stored artifact[{index}].size")
        if size > MAX_DELIVERY_ARTIFACT_BYTES:
            raise DeliveryContractError(
                "delivery_artifact_file_too_large", absolute
            )
        sha256 = _sha256(
            row.get("sha256"), f"stored artifact[{index}].sha256"
        )
        required = row.get("required")
        if not isinstance(required, bool):
            raise DeliveryContractError(
                "delivery_field_invalid",
                f"stored artifact[{index}].required must be boolean",
            )
        if (
            role in stored_by_role
            or absolute in stored_paths
            or relative in stored_relative_paths
        ):
            raise DeliveryContractError(
                "delivery_artifact_inventory_duplicate", role
            )
        stored_total += size
        if stored_total > MAX_DELIVERY_ARTIFACT_TOTAL_BYTES:
            raise DeliveryContractError("delivery_artifact_bundle_too_large")
        stored_paths.add(absolute)
        stored_relative_paths.add(relative)
        stored_by_role[role] = VerifiedArtifact(
            role=role,
            path=absolute,
            relative_path=relative,
            size=size,
            sha256=sha256,
            media_type=media_type,
            required=required,
        )

    if len(stored_by_role) != len(expected) or set(stored_by_role) != expected_roles:
        raise DeliveryContractError("delivery_artifact_inventory_mismatch")
    for artifact in expected:
        if stored_by_role[artifact.role] != artifact:
            raise _store_mismatch(
                artifact.role,
                f"stored artifact does not match sealed role {artifact.role}",
            )
    return tuple(expected)


def _validate_report_asset_url(
    value: Any,
    *,
    submission_key: str | None = None,
    artifact_set_id: str | None = None,
) -> str:
    url = _required_text(value, "delivery_manifest.report_url")
    parsed = urlparse(url)
    if parsed.scheme != "http" or parsed.netloc != (
        f"{_FORMAL_REPORT_HOST}:{_FORMAL_REPORT_PORT}"
    ):
        raise DeliveryContractError("report_url_invalid", f"unsafe report URL: {url}")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise DeliveryContractError("report_url_invalid", f"unsafe report URL: {url}")
    parts = parsed.path.split("/")
    if len(parts) < 6 or parts[:3] != ["", "G1Q3_RCA", "cases"]:
        raise DeliveryContractError("report_url_invalid", f"unsafe report URL: {url}")
    route_submission_key = parts[3]
    route_artifact_set_id = parts[4]
    if (
        not _SAFE_KEY_RE.fullmatch(route_submission_key)
        or not _ARTIFACT_SET_ID_RE.fullmatch(route_artifact_set_id)
    ):
        raise DeliveryContractError("report_url_invalid", f"unsafe report URL: {url}")
    if (
        submission_key is not None
        and route_submission_key != submission_key
    ) or (
        artifact_set_id is not None
        and route_artifact_set_id != artifact_set_id
    ):
        raise DeliveryContractError(
            "report_url_identity_mismatch",
            "report URL is not bound to the sealed submission and artifact set",
        )
    for segment in parts[5:]:
        if not _FORMAL_REPORT_SEGMENT_RE.fullmatch(segment):
            raise DeliveryContractError("report_url_invalid", f"unsafe report URL: {url}")
        decoded = unquote(segment)
        if (
            decoded in {"", ".", ".."}
            or "/" in decoded
            or "\\" in decoded
            or "%" in decoded
            or any(ord(char) < 32 for char in decoded)
        ):
            raise DeliveryContractError("report_url_invalid", f"unsafe report URL: {url}")
    return url


def _validate_report_url(
    value: Any,
    *,
    submission_key: str | None = None,
    artifact_set_id: str | None = None,
) -> str:
    url = _validate_report_asset_url(value)
    parts = urlparse(url).path.split("/")
    if parts[5:] != ["index.html"]:
        raise DeliveryContractError("report_url_invalid", f"unsafe report URL: {url}")
    if (submission_key is not None and parts[3] != submission_key) or (
        artifact_set_id is not None and parts[4] != artifact_set_id
    ):
        raise DeliveryContractError(
            "report_url_identity_mismatch",
            "report URL is not bound to the sealed submission and artifact set",
        )
    return url


def build_report_url(submission_key: Any, artifact_set_id: Any) -> str:
    """Build the one immutable publication URL for a sealed artifact set."""
    submission = _required_text(submission_key, "delivery_manifest.submission_key")
    artifact_set = _required_text(
        artifact_set_id, "delivery_manifest.artifact_set_id"
    )
    if not _SAFE_KEY_RE.fullmatch(submission) or not _ARTIFACT_SET_ID_RE.fullmatch(
        artifact_set
    ):
        raise DeliveryContractError("report_url_identity_invalid")
    return _validate_report_url(
        f"http://{_FORMAL_REPORT_HOST}:{_FORMAL_REPORT_PORT}/"
        f"G1Q3_RCA/cases/{submission}/{artifact_set}/index.html",
        submission_key=submission,
        artifact_set_id=artifact_set,
    )


def build_report_vm_path(submission_key: Any, artifact_set_id: Any) -> str:
    submission = _required_text(submission_key, "delivery_manifest.submission_key")
    artifact_set = _required_text(
        artifact_set_id, "delivery_manifest.artifact_set_id"
    )
    if not _SAFE_KEY_RE.fullmatch(submission) or not _ARTIFACT_SET_ID_RE.fullmatch(
        artifact_set
    ):
        raise DeliveryContractError("report_url_identity_invalid")
    return f"{_VM_TMP_PREFIX}{submission}/{artifact_set}/index.html"


def build_report_cifs_path(submission_key: Any, artifact_set_id: Any) -> str:
    vm_path = build_report_vm_path(submission_key, artifact_set_id)
    return _FORMAL_REPORT_CIFS_PREFIX + vm_path.removeprefix(_VM_TMP_PREFIX)


def _validate_report_identity_paths(
    manifest: Mapping[str, Any],
    *,
    submission_key: str,
    artifact_set_id: str,
) -> None:
    if (
        manifest.get("report_vm_path")
        != build_report_vm_path(submission_key, artifact_set_id)
        or manifest.get("report_cifs_path")
        != build_report_cifs_path(submission_key, artifact_set_id)
    ):
        raise DeliveryContractError("report_path_identity_mismatch")


def validate_report_url(
    value: Any,
    *,
    submission_key: str | None = None,
    artifact_set_id: str | None = None,
) -> str:
    """Validate the single production HTTP route allowed in delivery effects."""
    return _validate_report_url(
        value,
        submission_key=submission_key,
        artifact_set_id=artifact_set_id,
    )


def validate_report_asset_url(
    value: Any,
    *,
    submission_key: str | None = None,
    artifact_set_id: str | None = None,
) -> str:
    """Validate one static asset below the single formal internal report route."""
    return _validate_report_asset_url(
        value,
        submission_key=submission_key,
        artifact_set_id=artifact_set_id,
    )


def build_report_artifact_url(report_url: Any, relative_path: Any) -> str:
    primary = _validate_report_url(report_url)
    relative = _required_text(relative_path, "artifact.relative_path")
    path = PurePosixPath(relative)
    if path.is_absolute() or ".." in path.parts or not path.parts:
        raise DeliveryContractError("artifact_path_invalid", relative)
    encoded = "/".join(quote(part, safe="-._~") for part in path.parts)
    base = primary.rsplit("/", 1)[0]
    return _validate_report_asset_url(f"{base}/{encoded}")


def _contract_artifact_path(artifacts: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        text = str(artifacts.get(key) or "").strip()
        if text:
            return text
    return ""


def _observations_by_path(
    observed_files: Sequence[Mapping[str, Any]],
) -> dict[str, Mapping[str, Any]]:
    result: dict[str, Mapping[str, Any]] = {}
    for item in observed_files:
        if not isinstance(item, Mapping):
            raise DeliveryContractError(
                "artifact_observation_invalid", "observed file must be an object"
            )
        path = str(item.get("path") or "").strip()
        if not path or path in result:
            raise DeliveryContractError(
                "artifact_observation_invalid", f"duplicate or missing observed path: {path}"
            )
        result[path] = item
    return result


def _verify_viz_publication(
    *,
    contract_artifacts: Mapping[str, Any],
    observations: Mapping[str, Mapping[str, Any]],
    submission_key: str,
) -> tuple[str, str]:
    publication = contract_artifacts.get("viz_publication")
    if not isinstance(publication, Mapping):
        raise DeliveryContractError("viz_publication_missing")
    expected_fields = {
        "schema_version",
        "status",
        "submission_key",
        "path",
        "size",
        "sha256",
        "manifest_path",
        "manifest_size",
        "manifest_sha256",
        "source_path",
        "source_sha256",
        "published_at",
    }
    if set(publication) != expected_fields:
        raise DeliveryContractError("viz_publication_shape_invalid")
    if (
        publication.get("schema_version") != _VIZ_PUBLICATION_SCHEMA_VERSION
        or publication.get("status") != "published"
        or publication.get("submission_key") != submission_key
    ):
        raise DeliveryContractError("viz_publication_identity_mismatch")

    expected_path = canonical_viz_mcap_path(submission_key)
    path = _required_text(publication.get("path"), "viz_publication.path")
    if not expected_path or path != expected_path:
        raise DeliveryContractError("viz_publication_path_invalid")
    expected_manifest_path = expected_path.removesuffix(".viz.mcap") + ".viz.manifest.json"
    manifest_path = _required_text(
        publication.get("manifest_path"), "viz_publication.manifest_path"
    )
    if manifest_path != expected_manifest_path:
        raise DeliveryContractError("viz_publication_manifest_path_invalid")

    source_path = _required_text(publication.get("source_path"), "viz_publication.source_path")
    source_root = canonical_artifact_root(submission_key)
    if (
        not source_path.startswith(source_root + "cases/")
        or not source_path.endswith(".viz.mcap")
        or ".." in PurePosixPath(source_path).parts
    ):
        raise DeliveryContractError("viz_publication_source_invalid")
    published_sha = _sha256(publication.get("sha256"), "viz_publication.sha256")
    if _sha256(publication.get("source_sha256"), "viz_publication.source_sha256") != published_sha:
        raise DeliveryContractError("viz_publication_source_hash_mismatch")
    published_size = _positive_int(publication.get("size"), "viz_publication.size")
    manifest_size = _positive_int(
        publication.get("manifest_size"), "viz_publication.manifest_size"
    )
    manifest_sha = _sha256(
        publication.get("manifest_sha256"), "viz_publication.manifest_sha256"
    )

    observed_viz = observations.get(path)
    observed_manifest = observations.get(manifest_path)
    for label, observed in (("viz", observed_viz), ("manifest", observed_manifest)):
        if (
            not isinstance(observed, Mapping)
            or observed.get("is_file") is not True
            or observed.get("is_symlink") is True
            or observed.get("parents_symlink_free") is not True
        ):
            raise DeliveryContractError(f"viz_publication_{label}_not_observed")
    if (
        _positive_int(observed_viz.get("size"), "observed_viz.size") != published_size
        or _sha256(observed_viz.get("sha256"), "observed_viz.sha256") != published_sha
        or observed_viz.get("sha256_attested_by_manifest") is not True
    ):
        raise DeliveryContractError("viz_publication_observation_mismatch")
    if (
        _positive_int(observed_manifest.get("size"), "observed_manifest.size")
        != manifest_size
        or _sha256(observed_manifest.get("sha256"), "observed_manifest.sha256")
        != manifest_sha
    ):
        raise DeliveryContractError("viz_publication_manifest_observation_mismatch")

    rendered = foxglove_url(path)
    if not rendered or not validate_foxglove_url(rendered, path):
        raise DeliveryContractError("foxglove_url_invalid")
    return path, rendered


def _verify_identity(
    admission: RcaAdmission,
    contract: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> None:
    refs = admission.source_refs
    expected = {
        "submission_key": admission.submission_key,
        "business_key": admission.business_key,
        "generation": admission.generation,
        "project_key": refs.project_key,
        "work_item_type_key": refs.work_item_type_key,
        "work_item_id": refs.work_item_id,
    }
    for key, value in expected.items():
        if manifest.get(key) != value:
            raise DeliveryContractError(
                "delivery_identity_mismatch",
                f"manifest {key} does not match admission",
            )
    if str(contract.get("task_id") or "").strip() != admission.submission_key:
        raise DeliveryContractError(
            "delivery_identity_mismatch", "contract task_id does not match submission_key"
        )
    run_id = str(contract.get("run_id") or "").strip()
    if run_id and run_id != admission.submission_key:
        raise DeliveryContractError(
            "delivery_identity_mismatch", "contract run_id does not match submission_key"
        )
    if str(contract.get("work_item_id") or "").strip() != refs.work_item_id:
        raise DeliveryContractError(
            "delivery_identity_mismatch", "contract work_item_id does not match admission"
        )


def verify_delivery_bundle(
    *,
    admission: RcaAdmission | Mapping[str, Any],
    delivery_contract: Mapping[str, Any],
    delivery_manifest: Mapping[str, Any],
    observed_files: Sequence[Mapping[str, Any]],
    html_dependencies: Sequence[str],
) -> VerifiedDelivery:
    """Verify sealed evidence plus one published viz and build a send-free effect."""
    validated_admission = validate_rca_admission(admission)
    contract = dict(delivery_contract or {})
    manifest = dict(delivery_manifest or {})
    if not contract:
        raise DeliveryContractError("delivery_contract_missing")
    if not manifest:
        raise DeliveryContractError("delivery_manifest_missing")
    if contract.get("schema_version") != DELIVERY_CONTRACT_SCHEMA_VERSION:
        raise DeliveryContractError("delivery_contract_schema_unsupported")
    if manifest.get("schema_version") != DELIVERY_MANIFEST_SCHEMA_VERSION:
        raise DeliveryContractError("delivery_manifest_schema_unsupported")
    if manifest.get("sealed") is not True:
        raise DeliveryContractError("delivery_manifest_not_sealed")
    if manifest.get("deliverable_kind") != "html":
        raise DeliveryContractError(
            "delivery_kind_unsupported", "only sealed HTML delivery is supported"
        )
    if manifest.get("dependencies_complete") is not True:
        raise DeliveryContractError(
            "delivery_dependencies_incomplete",
            "manifest must attest a complete HTML dependency inventory",
    )
    _verify_identity(validated_admission, contract, manifest)

    if str(contract.get("business_state") or "").strip() != "report_completed":
        raise DeliveryContractError("delivery_business_state_not_ready")
    report = contract.get("report") if isinstance(contract.get("report"), Mapping) else {}
    if report.get("is_deliverable") is not True:
        raise DeliveryContractError("delivery_report_not_deliverable")
    explicit_kind = str(
        report.get("deliverable_kind") or contract.get("deliverable_kind") or ""
    ).strip()
    if explicit_kind != "foxglove_viz":
        raise DeliveryContractError("delivery_kind_unsupported")
    report_status = str(report.get("status") or "").strip()
    if report_status not in _VIZ_REPORT_STATUSES:
        raise DeliveryContractError(
            "delivery_report_status_not_viz", f"unsupported report status: {report_status}"
        )
    if report.get("requires_human_review") is not True:
        raise DeliveryContractError(
            "delivery_review_boundary_missing", "RCA delivery must require human review"
        )

    root = _normalize_root(manifest.get("artifact_root"), validated_admission.submission_key)
    artifact_material = _manifest_artifact_material(manifest)
    expected_artifact_set_id = compute_artifact_set_id(manifest)
    _validate_manifest_v2_shape(manifest)
    if manifest.get("artifact_set_id") != expected_artifact_set_id:
        raise DeliveryContractError("artifact_set_id_mismatch")

    contract_artifacts = (
        contract.get("artifacts") if isinstance(contract.get("artifacts"), Mapping) else {}
    )
    manifest_vm = _contract_artifact_path(
        contract_artifacts, "delivery_manifest_vm", "manifest_vm"
    )
    if manifest_vm != f"{root}delivery_manifest.json":
        raise DeliveryContractError(
            "delivery_manifest_reference_mismatch",
            "contract must reference the canonical delivery_manifest.json",
        )
    if contract_artifacts.get("artifact_set_id") != expected_artifact_set_id:
        raise DeliveryContractError("artifact_set_reference_mismatch")

    observations = _observations_by_path(observed_files)
    viz_mcap_vm, rendered_foxglove_url = _verify_viz_publication(
        contract_artifacts=contract_artifacts,
        observations=observations,
        submission_key=validated_admission.submission_key,
    )
    roles: dict[str, VerifiedArtifact] = {}
    verified: list[VerifiedArtifact] = []
    seen_paths: set[str] = set()
    for item in artifact_material:
        absolute, relative = _artifact_path(root, item["path"])
        if absolute in seen_paths or item["role"] in roles:
            raise DeliveryContractError(
                "delivery_manifest_duplicate_artifact", item["role"]
            )
        seen_paths.add(absolute)
        observed = observations.get(absolute)
        if observed is None:
            raise DeliveryContractError("artifact_missing", absolute)
        if (
            observed.get("is_file") is not True
            or observed.get("is_symlink") is True
            or observed.get("parents_symlink_free") is not True
        ):
            raise DeliveryContractError("artifact_not_regular_file", absolute)
        if _positive_int(observed.get("size"), f"observed[{absolute}].size") != item["size"]:
            raise DeliveryContractError("artifact_size_mismatch", absolute)
        if _sha256(observed.get("sha256"), f"observed[{absolute}].sha256") != item["sha256"]:
            raise DeliveryContractError("artifact_hash_mismatch", absolute)
        artifact = VerifiedArtifact(
            role=item["role"],
            path=absolute,
            relative_path=relative,
            size=item["size"],
            sha256=item["sha256"],
            media_type=item["media_type"],
            required=item["required"],
        )
        roles[artifact.role] = artifact
        verified.append(artifact)

    for role in ("index_html", "report_data"):
        artifact = roles.get(role)
        if artifact is None or not artifact.required:
            raise DeliveryContractError("required_html_artifact_missing", role)
    if not roles["index_html"].relative_path.lower().endswith(".html"):
        raise DeliveryContractError("required_html_artifact_invalid")
    if not roles["report_data"].relative_path.lower().endswith(".json"):
        raise DeliveryContractError("required_report_data_artifact_invalid")
    html_validation = _html_validation_material(manifest)
    if html_validation["report_data_sha256"] != roles["report_data"].sha256:
        raise DeliveryContractError(
            "html_validation_report_data_hash_mismatch",
            "html validation is not bound to the sealed report_data artifact",
        )

    html_dependency_paths: set[str] = set()
    for dependency in html_dependencies:
        absolute, _relative = _artifact_path(root, dependency)
        html_dependency_paths.add(absolute)
    missing_dependencies = sorted(html_dependency_paths - seen_paths)
    if missing_dependencies:
        raise DeliveryContractError(
            "html_dependency_not_manifested", missing_dependencies[0]
        )

    primary_report = _contract_artifact_path(
        contract_artifacts, "index_html_vm", "primary_report_vm"
    )
    report_data = _contract_artifact_path(contract_artifacts, "report_data_vm")
    if primary_report != roles["index_html"].path or report_data != roles["report_data"].path:
        raise DeliveryContractError(
            "delivery_artifact_reference_mismatch",
            "contract HTML/JSON paths do not match the sealed manifest",
        )

    report_url = _validate_report_url(
        manifest.get("report_url"),
        submission_key=validated_admission.submission_key,
        artifact_set_id=expected_artifact_set_id,
    )
    _validate_report_identity_paths(
        manifest,
        submission_key=validated_admission.submission_key,
        artifact_set_id=expected_artifact_set_id,
    )
    report_cifs_path = build_report_cifs_path(
        validated_admission.submission_key,
        expected_artifact_set_id,
    )
    refs = validated_admission.source_refs
    target_key = (
        f"feishu_project:{refs.project_key}:{refs.work_item_type_key}:"
        f"{refs.work_item_id}"
    )
    project_simple_name = str(refs.project_simple_name or "").strip()
    if validated_admission.schema_version != "pnc_rca_admission_v1" and not project_simple_name:
        raise DeliveryContractError("delivery_project_simple_name_missing")
    issue_url = (
        f"https://project.feishu.cn/{refs.project_key}"
        f"/issue/detail/{refs.work_item_id}"
    )
    delivery_id = _stable_key(
        "g1q3-rca-delivery-v1",
        {
            "key_version": DELIVERY_KEY_VERSION,
            "submission_key": validated_admission.submission_key,
            "artifact_set_id": expected_artifact_set_id,
            "target_key": target_key,
        },
    )
    summary = contract.get("summary") if isinstance(contract.get("summary"), Mapping) else {}
    conclusion = _truncate_utf8(
        str(summary.get("short_conclusion") or summary.get("l0") or "").strip(),
        MAX_CONCLUSION_BYTES,
    )
    if not conclusion:
        raise DeliveryContractError(
            "delivery_conclusion_missing",
            "a non-empty RCA conclusion is required for the result field",
        )
    semantic_payload = {
        "schema_version": DELIVERY_EFFECT_SCHEMA_VERSION,
        "delivery_id": delivery_id,
        "effect_kind": DELIVERY_EFFECT_KIND,
        "target_key": target_key,
        "project_key": refs.project_key,
        "work_item_type_key": refs.work_item_type_key,
        "work_item_id": refs.work_item_id,
        "issue_url": issue_url,
        "artifact_set_id": expected_artifact_set_id,
        "report_url": report_url,
        "report_cifs_path": report_cifs_path,
        "viz_mcap_vm": viz_mcap_vm,
        "foxglove_url": rendered_foxglove_url,
        "report_link_kind": DELIVERY_REPORT_LINK_KIND,
        "report_status": report_status,
        "requires_human_review": True,
        "conclusion": conclusion,
        "field_updates": [
            {
                "field_key": RCA_RESULT_FIELD_KEY,
                "field_value": conclusion,
            },
            {
                "field_key": RCA_REPORT_FIELD_KEY,
                "field_value": report_url,
            },
        ],
    }
    semantic_payload_sha256 = compute_delivery_effect_payload_sha256(
        semantic_payload, DELIVERY_EFFECT_KIND
    )
    effect_key = compute_delivery_effect_key(
        delivery_id=delivery_id,
        effect_kind=DELIVERY_EFFECT_KIND,
        target_key=target_key,
        semantic_payload_sha256=semantic_payload_sha256,
    )
    marker = delivery_effect_marker(effect_key, expected_artifact_set_id)
    comment_content = build_issue_comment_content(
        marker=marker,
        work_item_id=refs.work_item_id,
        report_status=report_status,
        conclusion=conclusion,
        report_url=report_url,
        report_cifs_path=report_cifs_path,
    )
    effect_payload = {
        **semantic_payload,
        "effect_key": effect_key,
        "semantic_payload_sha256": semantic_payload_sha256,
        "marker": marker,
        "comment_content": comment_content,
    }
    return VerifiedDelivery(
        delivery_id=delivery_id,
        effect_key=effect_key,
        semantic_payload_sha256=semantic_payload_sha256,
        artifact_set_id=expected_artifact_set_id,
        business_key=validated_admission.business_key,
        submission_key=validated_admission.submission_key,
        generation=validated_admission.generation,
        project_key=refs.project_key,
        work_item_type_key=refs.work_item_type_key,
        work_item_id=refs.work_item_id,
        target_key=target_key,
        issue_url=issue_url,
        report_url=report_url,
        viz_mcap_vm=viz_mcap_vm,
        foxglove_url=rendered_foxglove_url,
        conclusion=conclusion,
        marker=marker,
        manifest=manifest,
        contract=contract,
        artifacts=tuple(verified),
        effect_payload=effect_payload,
    )
