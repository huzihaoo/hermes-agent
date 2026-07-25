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
    canonical_publication_origin,
    canonical_viz_mcap_cifs_path,
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
_PROJECT_SIMPLE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_FEISHU_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{1,255}$")
_TERMINAL_CODE_RE = re.compile(r"^[a-z][a-z0-9_]{0,119}$")
_ARTIFACT_SET_ID_RE = re.compile(
    r"^g1q3-rca-artifact-v1-[0-9a-f]{64}$"
)
_FORMAL_REPORT_SEGMENT_RE = re.compile(r"^(?:[A-Za-z0-9._~-]|%[0-9A-Fa-f]{2})+$")
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
CONSUMER_CAPABILITY_SCHEMA_VERSION = "rca_consumer_capability_publication_v1"
RCA_RESULT_FIELD_KEY = "field_9193cb"
RCA_REPORT_FIELD_KEY = "field_8c912e"
DELIVERY_REPORT_LINK_KIND = "html_report"
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
    "project_simple_name",
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
    "business_route_unresolved": (
        "自动归因未完成（非归因结论）：官方所属项目字段缺失，无法唯一选择业务归因能力；未按标题、负责人或群聊猜测。"
    ),
    "business_route_unsupported": (
        "自动归因未完成（非归因结论）：官方所属项目尚未注册对应归因能力；未进入 G1Q3 或其他项目评测器。"
    ),
    "business_route_conflict": (
        "自动归因未完成（非归因结论）：官方所属项目字段存在冲突，无法唯一选择业务归因能力。"
    ),
    "business_adapter_not_ready": (
        "自动归因未完成（非归因结论）：已按官方字段路由到对应业务，但该项目输入适配尚未就绪；未跨项目回退或伪造归因。"
    ),
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
_BUSINESS_ROUTE_DIAGNOSTIC_CODES = frozenset(
    {
        "business_route_unresolved",
        "business_route_unsupported",
        "business_route_conflict",
        "business_adapter_not_ready",
    }
)
_TERMINAL_INPUT_DIAGNOSTIC_CODES = {
    "business_profile_unresolved": "business_route_unresolved",
    "business_profile_unsupported": "business_route_unsupported",
    "business_profile_conflict": "business_route_conflict",
    "business_profile_adapter_not_ready": "business_adapter_not_ready",
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
    foxglove_url: str,
    report_cifs_path: str,
) -> str:
    lines = [
        marker,
        "【RCA 结果】自动分析已完成，待人工审批。",
        f"问题：{work_item_id}",
    ]
    if conclusion:
        lines.append(conclusion)
    lines.extend(
        [
            f"详细证据报告：{report_url}",
            "请人工审批归因结论；报告页用于查看证据和完整过程。",
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
    foxglove_url: str,
    issue_url: str,
) -> str:
    lines = [
        marker,
        "【RCA 结果】自动分析已完成，待人工审批。",
        f"问题：{work_item_id}",
    ]
    if conclusion:
        lines.append(conclusion)
    lines.extend(
        [
            f"详细证据报告：{report_url}",
            f"问题单：{issue_url}",
            "请人工审批归因结论；报告页用于查看证据和完整过程。",
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
    if diagnostic_result and "\n" in diagnostic_result:
        lines = [marker, "【RCA 结果】"] + diagnostic_result.splitlines()
    else:
        lines = [
            marker,
            "【RCA 结果】本次未形成可确认的自动归因。",
            "责任候选：暂无法判断。",
            "因果链：暂无足够证据建立可确认的因果链。",
            f"当前卡点：{diagnostic_result or '自动分析未生成可交付证据。'}",
            "关键证据：本次未生成可供审批的归因证据。",
            "下一步：请补齐问题数据或修复输入后重新发起 RCA；人工可先行分流。",
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
    diagnostic_detail: str = "",
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
        public_detail = str(diagnostic_detail or "").strip()
        if public_detail:
            if (
                normalized_diagnostic_code not in _BUSINESS_ROUTE_DIAGNOSTIC_CODES
                or "\n" in public_detail
                or "\r" in public_detail
                or len(public_detail.encode("utf-8")) > 1500
            ):
                raise DeliveryContractError(
                    "terminal_delivery_diagnostic_detail_invalid"
                )
        diagnostic_result = _render_terminal_user_result(
            normalized_diagnostic_code,
            public_detail,
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
        if public_detail:
            diagnostic_contract["diagnostic_detail"] = public_detail
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
        foxglove_url=str(semantic.get("foxglove_url") or ""),
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


def _consumer_capability_summary(contract: Mapping[str, Any]) -> str:
    capability = contract.get("consumer_capability")
    if capability is None:
        summary = contract.get("summary")
        capability = (
            summary.get("consumer_capability")
            if isinstance(summary, Mapping)
            else None
        )
    if not capability:
        return ""
    if not isinstance(capability, Mapping):
        raise DeliveryContractError("consumer_capability_invalid")
    if capability.get("schema_version") != CONSUMER_CAPABILITY_SCHEMA_VERSION:
        raise DeliveryContractError("consumer_capability_schema_unsupported")
    profile = _required_text(
        capability.get("capability_profile"), "consumer_capability.capability_profile"
    )
    version = _required_text(
        capability.get("capability_version"), "consumer_capability.capability_version"
    )
    evaluator_scope = _required_text(
        capability.get("evaluator_scope"), "consumer_capability.evaluator_scope"
    )
    applicability = str(capability.get("applicability") or "").strip()
    if applicability not in {"applied", "not_applied"}:
        raise DeliveryContractError("consumer_capability_applicability_invalid")
    signals = capability.get("actual_signals")
    fields = capability.get("actual_fields")
    evaluators = capability.get("actual_evaluators")
    unused = capability.get("unused_capabilities")
    if not all(isinstance(value, list) for value in (signals, fields, evaluators, unused)):
        raise DeliveryContractError("consumer_capability_inventory_invalid")
    for item in evaluators:
        if (
            not isinstance(item, Mapping)
            or not str(item.get("evaluator_id") or "").strip()
            or not str(item.get("status") or "").strip()
        ):
            raise DeliveryContractError("consumer_capability_evaluator_invalid")
    for item in unused:
        if (
            not isinstance(item, Mapping)
            or not str(item.get("evaluator_id") or "").strip()
            or not str(item.get("status") or "").strip()
            or not str(item.get("reason") or "").strip()
        ):
            raise DeliveryContractError("consumer_capability_unused_reason_missing")
    actual_count = len(signals) + len(fields) + len(evaluators)
    if applicability == "applied" and actual_count < 1:
        raise DeliveryContractError("consumer_capability_false_applied")
    reason = str(capability.get("not_applied_reason") or "").strip()
    if applicability == "not_applied" and not reason:
        raise DeliveryContractError("consumer_capability_reason_missing")
    evidence = capability.get("evidence")
    if not isinstance(evidence, Mapping):
        raise DeliveryContractError("consumer_capability_evidence_invalid")
    field_lineage = evidence.get("field_lineage")
    if not isinstance(field_lineage, Mapping):
        raise DeliveryContractError("consumer_capability_field_lineage_invalid")
    viz = evidence.get("viz_lineage")
    if not isinstance(viz, Mapping):
        raise DeliveryContractError("consumer_capability_viz_lineage_invalid")
    frame = evidence.get("issue_frame_id")
    frame_text = str(frame) if frame not in {None, ""} else "未指定"
    if applicability == "not_applied":
        return (
            f"能力出版：{profile}@{version}（{evaluator_scope}）未调用；"
            f"原因：{reason}；证据帧：{frame_text}；viz：{str(viz.get('status') or '未生成')}"
        )
    return (
        f"能力出版：{profile}@{version}（{evaluator_scope}）；"
        f"实际 signals/fields/evaluators={len(signals)}/{len(fields)}/{len(evaluators)}；"
        f"未调用={len(unused)}；证据帧：{frame_text}；"
        f"viz：{str(viz.get('status') or '未生成')}"
    )


_PUBLIC_INTERNAL_FRAGMENTS = (
    "能力出版",
    "evaluator_scope",
    "signals/fields/evaluators",
    "未调用=",
    "执行代次",
    "代次：",
    "错误码",
    "terminal_diagnostic",
    "RCA 第 ",
)

_PUBLIC_TEXT_REPLACEMENTS = (
    ("decoded evaluator", "已解码证据"),
    ("decoded 证据", "已解码证据"),
    ("OOI 槽位", "目标跟踪记录"),
    ("活动槽位", "有效记录位置"),
    ("OOI", "前方目标"),
    ("原始 mcap 已解码出函数级证据，可直接进入 RCA", "生产数据已成功读取，并提取到可核验的功能证据"),
)

_PUBLIC_RESPONSIBILITY_LABELS = {
    "ACC": "ACC 功能链",
    "AEB_FCW": "AEB/FCW 功能链",
    "CONTROL_LONGITUDINAL": "纵向控制",
    "DNP_SPP": "规划功能链",
    "LANE_PERCEPTION": "车道线感知",
    "LCC": "LCC 功能链",
    "PERCEPTION_LANE": "车道线感知",
    "PERCEPTION_OBJECT": "目标感知/融合",
    "PLANNING": "规划",
    "问题数据/回灌链路": "问题数据/回灌链路",
}


def _public_text(value: Any, *, limit: int = 900) -> str:
    """Keep user-facing RCA text free of execution metadata and debug noise."""
    raw = str(value or "").strip()
    if not raw:
        return ""
    kept = [
        line.strip()
        for line in raw.splitlines()
        if line.strip() and not any(fragment in line for fragment in _PUBLIC_INTERNAL_FRAGMENTS)
    ]
    text = "；".join(kept)
    for source, replacement in _PUBLIC_TEXT_REPLACEMENTS:
        text = re.sub(re.escape(source), replacement, text, flags=re.IGNORECASE)
    text = re.sub(r"(?<=[\u4e00-\u9fff])\s+(?=[\u4e00-\u9fff])", "", text)
    for prefix in ("候选因果判断：", "候选原因：", "诊断结论："):
        if text.startswith(prefix):
            text = text[len(prefix):].strip()
    return _truncate_utf8(text.rstrip("。； ") + ("。" if text else ""), limit)


def _public_responsibility(value: Any) -> str:
    text = _public_text(value, limit=260)
    if not text:
        return ""
    normalized = text.rstrip("。 ").upper()
    return _PUBLIC_RESPONSIBILITY_LABELS.get(normalized, text)


def _public_action(value: Any) -> str:
    text = _public_text(value, limit=500)
    if any(
        marker in text
        for marker in ("已受理", "待受控远程读取", "自动管线", "无需发起人补数据")
    ):
        return ""
    return text


def _public_attribution_text(value: Any) -> tuple[str, str]:
    """Split a legacy evaluator sentence into conclusion and review boundary."""
    text = _public_text(value, limit=1200)
    if not text:
        return "", ""
    boundary = ""
    boundary_match = re.search(r"(?:；|。)(当前缺少[^。；]*[。]?)", text)
    if boundary_match:
        boundary = boundary_match.group(1).rstrip("。； ") + "。"
        text = text[: boundary_match.start()].rstrip("。； ") + "。"
    text = re.sub(r"^当前工况下，[^。]{1,120}-无描述。", "", text)
    text = re.sub(r"已解码证据\s*已支持候选归因方向：", "", text)
    text = re.sub(r"\b[A-Z][A-Z0-9_/ -]{0,40}\s+已解码证据：", "", text)
    text = re.sub(r"责任候选：[^。；]+(?:，需人工复核)?[。]?$", "", text)
    text = re.sub(r"(?<=[\u4e00-\u9fff])\s+(?=[\u4e00-\u9fff])", "", text)
    text = text.strip("。； ")
    if text and not text.endswith("..."):
        text += "。"
    return (text, boundary)


def _public_first_text(*values: Any, limit: int = 900) -> str:
    for value in values:
        text = _public_text(value, limit=limit)
        if text:
            return text
    return ""


def _public_terminal_blocker(code: str) -> tuple[str, str, str]:
    """Map internal terminal categories to a stable human explanation."""
    mapping = {
        "remote_event_not_found": (
            "问题数据事件在当前生产数据源中不存在。",
            "无法读取对应证据，因此不能判断责任方或因果链。",
            "请核对问题单中的 PDCL 事件地址，修正后重新发起 RCA。",
        ),
        "unsupported_function_domain": (
            "本次任务未进入功能证据分析，不能作为归因结果。",
            "问题数据尚未经过通用信号扫描和因果评测。",
            "请由系统重新执行完整证据分析。",
        ),
        "business_adapter_not_ready": (
            "该问题已识别到所属业务，但对应数据适配尚未就绪。",
            "当前没有可验证的业务证据，不能跨项目借用其他归因能力。",
            "请人工分流；完成该业务的数据适配后重新发起 RCA。",
        ),
        "business_route_unresolved": (
            "问题单缺少可确认的业务归属，当前不能自动归因。",
            "无法唯一选择对应业务的数据和归因能力，系统未按标题、负责人或群聊猜测。",
            "请补齐或修正问题单的业务归属字段后重新发起 RCA。",
        ),
        "business_route_unsupported": (
            "问题所属业务尚未接入自动 RCA，当前不能自动归因。",
            "系统已停止在业务边界，未借用其他项目的数据或归因能力。",
            "请人工分流；该业务接入后重新发起 RCA。",
        ),
        "business_route_conflict": (
            "问题单的业务归属字段相互冲突，当前不能自动归因。",
            "无法唯一选择对应业务的数据和归因能力。",
            "请修正冲突的业务归属字段后重新发起 RCA。",
        ),
    }
    return mapping.get(
        code,
        (
            "本次未生成可确认的自动归因。",
            "当前没有足够的可验证证据建立责任和因果链。",
            "请补齐问题数据或输入后重新发起 RCA；人工可先行分流。",
        ),
    )


def build_public_rca_result(contract: Mapping[str, Any]) -> dict[str, Any]:
    """Project a delivery contract into the small result a Feishu user reviews.

    The report keeps its full machine-readable lineage.  This projection is
    deliberately limited to decision-bearing fields and never exposes model,
    evaluator, generation, or terminal implementation details.
    """
    contract = contract if isinstance(contract, Mapping) else {}
    public = contract.get("public_result")
    public = public if isinstance(public, Mapping) else {}
    summary = public.get("summary") if isinstance(public.get("summary"), Mapping) else {}
    if not summary:
        raw_summary = contract.get("summary")
        summary = raw_summary if isinstance(raw_summary, Mapping) else {}
    report = contract.get("report") if isinstance(contract.get("report"), Mapping) else {}
    responsibility = public.get("responsibility") if isinstance(public.get("responsibility"), Mapping) else {}
    causal = public.get("causal_chain") if isinstance(public.get("causal_chain"), Mapping) else {}
    evidence = public.get("evidence_summary") if isinstance(public.get("evidence_summary"), Mapping) else {}
    terminal = public.get("terminal_diagnostic") if isinstance(public.get("terminal_diagnostic"), Mapping) else {}

    terminal_code = str(terminal.get("blocker_kind") or "").strip()
    raw_short = _public_first_text(
        summary.get("short_conclusion"),
        summary.get("l0"),
        limit=1200,
    )
    short, derived_boundary = _public_attribution_text(raw_short)
    no_attribution = bool(terminal_code) or any(
        marker in short
        for marker in (
            "不能自动归因",
            "未形成归因",
            "未找到",
            "不存在，请核对",
            "暂不在自动",
            "自动RCA未归因",
            "当前问题域不在已验证",
            "已生成诊断报告",
        )
    )
    responsibility_candidate = _public_responsibility(
        report.get("candidate_owner_domain")
        or responsibility.get("candidate")
        or responsibility.get("owner")
        or public.get("candidate")
        or report.get("candidate_owner")
    )

    artifacts = contract.get("artifacts") if isinstance(contract.get("artifacts"), Mapping) else {}
    fallback_causal_text, fallback_boundary = _public_attribution_text(_public_first_text(
        artifacts.get("attribution_causal_text"),
        summary.get("short_conclusion"),
        limit=1000,
    ))
    if not derived_boundary:
        derived_boundary = fallback_boundary

    responsibility_fallback = _public_first_text(
        responsibility.get("candidate"),
        responsibility.get("owner"),
        public.get("candidate"),
        report.get("candidate_owner"),
        limit=260,
    )
    if not responsibility_candidate:
        responsibility_candidate = responsibility_fallback

    narrative = causal.get("narrative")
    narrative = narrative if isinstance(narrative, list) else []
    evidence_text = ""
    causal_text = ""
    for item in narrative:
        if not isinstance(item, Mapping):
            continue
        role = str(item.get("role") or "").strip()
        text = _public_text(item.get("text"), limit=1000)
        if not text:
            continue
        if role == "因果判断" and not causal_text:
            causal_text = text
        elif role == "证据" and not evidence_text:
            evidence_text = text
    hypotheses = causal.get("hypotheses")
    hypotheses = hypotheses if isinstance(hypotheses, list) else []
    structured_evidence: list[str] = []
    hypothesis_claim = ""
    for item in hypotheses:
        if not isinstance(item, Mapping):
            continue
        if not hypothesis_claim:
            hypothesis_claim = _public_first_text(
                item.get("claim"), item.get("narrative"), item.get("text"), item.get("summary"),
                limit=700,
            )
        supporting = item.get("supporting_evidence")
        if not isinstance(supporting, list):
            continue
        for evidence_item in supporting[:4]:
            if not isinstance(evidence_item, Mapping):
                continue
            detail = _public_first_text(
                evidence_item.get("evidence"), evidence_item.get("summary"),
                limit=420,
            )
            if not detail:
                continue
            name = _public_text(evidence_item.get("name"), limit=100).rstrip("。")
            rendered = f"{name}：{detail}" if name else detail
            if rendered not in structured_evidence:
                structured_evidence.append(rendered)
    if structured_evidence:
        evidence_text = "；".join(structured_evidence)
        if hypothesis_claim:
            causal_text = f"{structured_evidence[0].rstrip('。')}，因此{hypothesis_claim}"
    elif not causal_text and hypothesis_claim:
        causal_text = hypothesis_claim
    if not causal_text:
        causal_text = fallback_causal_text
    if not evidence_text:
        refs = evidence.get("refs")
        if isinstance(refs, list):
            compact_refs: list[str] = []
            for item in refs[:6]:
                if isinstance(item, Mapping):
                    compact = item.get("summary") or item.get("field") or item.get("check") or item.get("fit_source")
                else:
                    compact = item
                text = _public_text(compact, limit=220)
                if text and text not in compact_refs:
                    compact_refs.append(text)
            evidence_text = "；".join(compact_refs)
    evidence_boundary = public.get("evidence_boundary")
    if not isinstance(evidence_boundary, list):
        evidence_boundary = contract.get("evidence_boundary")
    evidence_boundary = evidence_boundary if isinstance(evidence_boundary, list) else []
    if not evidence_text:
        evidence_text = _public_first_text(*evidence_boundary, limit=1000)
    boundary = _public_first_text(
        responsibility.get("boundary"),
        summary.get("high_confidence_boundary"),
        derived_boundary,
        *evidence_boundary,
        limit=800,
    )
    action = public.get("user_action") if isinstance(public.get("user_action"), Mapping) else {}
    if not action:
        raw_action = contract.get("user_action")
        action = raw_action if isinstance(raw_action, Mapping) else {}
    next_action = _public_action(action.get("next_action_text") or action.get("next_action"))

    if no_attribution:
        conclusion, impact, default_action = _public_terminal_blocker(terminal_code)
        specific = evidence_text or fallback_causal_text or short
        causal_boundary = "暂无足够证据建立可确认的因果链。"
        if not terminal_code and any(marker in short for marker in ("数据源不一致", "证据冲突", "未找到该目标")):
            conclusion = "问题单描述的目标与生产数据不一致，当前不能确认责任归因。"
            impact = "问题单中的目标信息无法在绑定的生产数据中匹配。"
            causal_boundary = "问题单目标描述 → 生产数据核验 → 关键目标不匹配 → 无法验证责任因果链。"
            default_action = "请核对绑定的 PDCL 事件及目标信息；修正数据地址、时间或目标后重新发起 RCA。"
        elif not terminal_code and "未提供可核验的现象描述" in short:
            conclusion = "生产数据已读取，但问题现象描述不足，当前不能确认归因。"
            impact = "缺少可核验的异常现象，无法选择并验证对应因果机制。"
            causal_boundary = "生产数据读取 → 问题现象不明确 → 无法选择归因机制 → 转人工补充。"
            default_action = "请补充发生了什么、预期行为及实际异常后重新发起 RCA。"
        elif not terminal_code and any(
            marker in short
            for marker in ("自动RCA未归因", "当前问题域不在已验证", "已生成诊断报告")
        ):
            conclusion = "本次旧任务未进入功能证据分析，不能作为归因结果。"
            impact = "问题数据尚未经过通用信号扫描和因果评测。"
            causal_boundary = "任务在功能证据分析前结束，尚无可审核的缺陷因果链。"
            default_action = "请由系统重新执行完整证据分析。"
            specific = impact
        elif not terminal_code and short:
            conclusion = short
            impact = boundary or "问题现象、证据或因果链尚未达到可确认标准。"
        return {
            "conclusion": conclusion,
            "responsibility": "暂无法判断。",
            "causal_chain": causal_boundary,
            "evidence": specific or "未取得可用于责任判断的充分证据。",
            "boundary": impact,
            "next_action": next_action or default_action,
            "attribution_ready": False,
        }

    if not causal_text:
        causal_text = "暂无结构化因果链；请在详细证据报告中复核候选判断。"
    if not evidence_text:
        evidence_text = boundary or "详细证据已写入报告页，待人工复核。"
    return {
        "conclusion": short or "已生成候选归因，待人工审批。",
        "responsibility": responsibility_candidate or "待人工确认。",
        "causal_chain": causal_text,
        "evidence": evidence_text,
        "boundary": boundary or "无额外自动处理卡点；需人工确认责任边界。",
        "next_action": next_action or "请人工确认责任候选、因果链和证据边界。",
        "attribution_ready": True,
    }


def render_public_rca_result(contract: Mapping[str, Any]) -> str:
    result = build_public_rca_result(contract)
    lines = [
        f"归因结论：{result['conclusion']}",
        f"责任模块：{result['responsibility']}",
        f"因果关系：{result['causal_chain']}",
        f"关键证据：{result['evidence']}",
    ]
    return "\n".join(_truncate_utf8(line, 1800) for line in lines)


def _render_terminal_user_result(code: str, detail: str = "") -> str:
    conclusion, impact, action = _public_terminal_blocker(code)
    detail_text = _public_text(detail, limit=500)
    if detail_text and code == "business_adapter_not_ready":
        # Keep the route decision useful without leaking resolver/evaluator
        # implementation names into the issue field.
        route = detail_text.split("（", 1)[0].rstrip("。； ")
        if route:
            conclusion = f"未形成归因：{route}。"
    return "\n".join(
        (
            f"归因结论：{conclusion}",
            "责任模块：暂无法判断。",
            "因果关系：暂无足够证据建立可确认的因果链。",
            f"关键证据：{impact}",
        )
    )


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
    public_origin = canonical_publication_origin()
    if not public_origin:
        raise DeliveryContractError("report_public_origin_invalid")
    expected = urlparse(public_origin)
    if parsed.scheme != expected.scheme or parsed.netloc != expected.netloc:
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
    public_origin = canonical_publication_origin()
    if not public_origin:
        raise DeliveryContractError("report_public_origin_invalid")
    return _validate_report_url(
        f"{public_origin}/G1Q3_RCA/cases/"
        f"{submission}/{artifact_set}/index.html",
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
    """Validate the canonical public HTTPS report route used in delivery effects."""
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
    """Validate one static asset below the canonical public report route."""
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
    """Verify one sealed report and build a send-free delivery effect.

    A published Foxglove surface is verified when claimed, but it is not required
    when the sealed HTML report already carries the causal result and evidence.
    """
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
    if explicit_kind not in {"html", "foxglove_viz"}:
        raise DeliveryContractError("delivery_kind_unsupported")
    report_status = str(report.get("status") or "").strip()
    allowed_report_statuses = (
        _VIZ_REPORT_STATUSES
        if explicit_kind == "foxglove_viz"
        else _HTML_REPORT_STATUSES
    )
    if report_status not in allowed_report_statuses:
        raise DeliveryContractError(
            "delivery_report_status_invalid", f"unsupported report status: {report_status}"
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
    if explicit_kind == "foxglove_viz":
        viz_mcap_vm, rendered_foxglove_url = _verify_viz_publication(
            contract_artifacts=contract_artifacts,
            observations=observations,
            submission_key=validated_admission.submission_key,
        )
    else:
        if contract_artifacts.get("viz_publication") or contract_artifacts.get(
            "viz_mcap_vm"
        ):
            raise DeliveryContractError("html_delivery_must_not_claim_viz")
        viz_mcap_vm, rendered_foxglove_url = "", ""
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

    html_report_url = _validate_report_url(
        manifest.get("report_url"),
        submission_key=validated_admission.submission_key,
        artifact_set_id=expected_artifact_set_id,
    )
    _validate_report_identity_paths(
        manifest,
        submission_key=validated_admission.submission_key,
        artifact_set_id=expected_artifact_set_id,
    )
    report_cifs_path = (
        canonical_viz_mcap_cifs_path(validated_admission.submission_key)
        if explicit_kind == "foxglove_viz"
        else str(manifest.get("report_cifs_path") or "").strip()
    )
    if not report_cifs_path:
        raise DeliveryContractError("delivery_report_cifs_path_invalid")
    refs = validated_admission.source_refs
    target_key = (
        f"feishu_project:{refs.project_key}:{refs.work_item_type_key}:"
        f"{refs.work_item_id}"
    )
    project_simple_name = str(refs.project_simple_name or "").strip()
    if not project_simple_name:
        raise DeliveryContractError("delivery_project_simple_name_missing")
    if not _PROJECT_SIMPLE_NAME_RE.fullmatch(project_simple_name):
        raise DeliveryContractError("delivery_project_simple_name_invalid")
    issue_url = (
        f"https://project.feishu.cn/{project_simple_name}"
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
    # Keep the complete capability contract in the sealed report, but project
    # only user-decision fields into Feishu.  Validation still runs so a false
    # or malformed publication cannot pass through silently.
    _consumer_capability_summary(contract)
    raw_public = contract.get("public_result") if isinstance(contract.get("public_result"), Mapping) else {}
    raw_public_summary = raw_public.get("summary") if isinstance(raw_public.get("summary"), Mapping) else {}
    raw_summary = contract.get("summary") if isinstance(contract.get("summary"), Mapping) else {}
    if not str(
        raw_public_summary.get("short_conclusion")
        or raw_public_summary.get("l0")
        or raw_summary.get("short_conclusion")
        or raw_summary.get("l0")
    ).strip():
        raise DeliveryContractError(
            "delivery_conclusion_missing",
            "a non-empty RCA conclusion is required for the result field",
        )
    conclusion_text = render_public_rca_result(contract)
    conclusion = _truncate_utf8(conclusion_text, MAX_CONCLUSION_BYTES)
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
        "project_simple_name": project_simple_name,
        "work_item_type_key": refs.work_item_type_key,
        "work_item_id": refs.work_item_id,
        "issue_url": issue_url,
        "artifact_set_id": expected_artifact_set_id,
        "report_url": html_report_url,
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
                "field_value": html_report_url,
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
        report_url=html_report_url,
        foxglove_url=rendered_foxglove_url,
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
        report_url=html_report_url,
        viz_mcap_vm=viz_mcap_vm,
        foxglove_url=rendered_foxglove_url,
        conclusion=conclusion,
        marker=marker,
        manifest=manifest,
        contract=contract,
        artifacts=tuple(verified),
        effect_payload=effect_payload,
    )
