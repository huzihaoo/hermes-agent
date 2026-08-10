"""Strict delivery identity and artifact verification for Kafka-triggered RCA."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import hashlib
import json
import math
import posixpath
import re
import uuid
from pathlib import PurePosixPath
from typing import Any, Mapping, Sequence
from urllib.parse import parse_qs, quote, unquote, urlparse

from gateway.pnc_rca_admission import RcaAdmission, validate_rca_admission
from gateway.pnc_rca_abstention_projection import (
    RcaEvidenceProjectionError,
    build_gate_a_identifier_binding,
    build_gate_a_public_result,
    validate_gate_a_projection,
)
from gateway.feishu_mention import build_at_mention
from gateway.feishu_task_card import contains_internal_rca_html_reference
from gateway.pnc_rca_quality_oracle import (
    CANDIDATE_HYPOTHESIS,
    HONEST_NON_ATTRIBUTION,
    MEDIUM_TIER_DISCLAIMER,
    SUPPORTED_ATTRIBUTION,
    TierOracleConflict,
    evaluate_structural_tier,
    public_tier_from_rendered_text,
    require_publishable,
)
from gateway.pnc_rca_issue_focus import (
    ANALYSIS_CAPABILITY_UNSUPPORTED,
    ANALYSIS_COMPLETE,
    ANALYSIS_EVIDENCE_CONFLICT,
    ANALYSIS_EVIDENCE_INCOMPLETE,
    ANALYSIS_INSUFFICIENT_STATEMENT,
    ISSUE_FOCUS_PLAN_SCHEMA_VERSION,
    ISSUE_FOCUS_PLAN_STATUS_PLANNED,
    IssueFocusContractError,
    IssueFocusValidation,
    issue_title_sha256,
    normalized_issue_title,
    resolve_issue_intent,
    validate_issue_focus_evidence,
)
from scripts.pnc_foxglove_delivery import (
    canonical_publication_origin,
    canonical_viz_mcap_cifs_path,
    canonical_viz_mcap_path,
    foxglove_url,
    validate_foxglove_url,
)


DELIVERY_CONTRACT_SCHEMA_VERSION_V1 = "g1q3_delivery_contract_v1"
# v2 binds a sealed delivery to the immutable issue title and its focus-evidence
# contract.  v1 remains readable for historical artifacts and legacy adapters.
DELIVERY_CONTRACT_SCHEMA_VERSION = "g1q3_delivery_contract_v2"
DELIVERY_CONTRACT_SCHEMA_VERSIONS = frozenset({
    DELIVERY_CONTRACT_SCHEMA_VERSION_V1,
    DELIVERY_CONTRACT_SCHEMA_VERSION,
})
DELIVERY_MANIFEST_SCHEMA_VERSION = "delivery_manifest_v2"
DELIVERY_EFFECT_SCHEMA_VERSION_V1 = "pnc_rca_delivery_effect_v1"
DELIVERY_EFFECT_SCHEMA_VERSION_V2 = "pnc_rca_delivery_effect_v2"
DELIVERY_EFFECT_SCHEMA_VERSION_V3 = "pnc_rca_delivery_effect_v3"
# v4 separates the Feishu field projection from the issue-comment body.  The
# old v3 payload remains a first-class read/dispatch format so an already
# sealed effect is never reinterpreted under the new projection contract.
DELIVERY_EFFECT_SCHEMA_VERSION = "pnc_rca_delivery_effect_v4"
TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_V1 = "pnc_rca_terminal_delivery_effect_v1"
TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION = "pnc_rca_terminal_delivery_effect_v2"
TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION = "pnc_rca_terminal_delivery_effect_v3"
# New terminal effects are comment-only.  The historical v2/v3 schemas remain
# readable with their original field-update semantics.
TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY = (
    "pnc_rca_terminal_delivery_effect_v4"
)
TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY = (
    "pnc_rca_terminal_delivery_effect_v5"
)
TERMINAL_DIAGNOSTIC_CONTRACT_SCHEMA_VERSION = "pnc_rca_terminal_diagnostic_v1"
TERMINAL_FALLBACK_CONTRACT_SCHEMA_VERSION = "pnc_rca_bounded_terminal_fallback_v1"
TERMINAL_FALLBACK_PUBLIC_CONTRACT_SCHEMA_VERSION = (
    "pnc_rca_bounded_terminal_public_result_v1"
)
V19_CONCLUSION_COMMUNICATION_SCHEMA_VERSION = (
    "g1q3_rca_conclusion_communication_v2"
)
V19_ISSUE_FOCUS_BINDING_SCHEMA_VERSION = "g1q3_rca_v19_issue_focus_binding_v1"
V19_PRIMARY_CONCLUSION_MODES = frozenset(
    {"high_confidence_candidate", "works_as_designed", "symptom_refuted"}
)
V19_REQUIRED_CONCLUSION_CHECKS = frozenset(
    {
        "evidence_only",
        "no_new_causal_claim",
        "routing_hints_separated_from_evidence",
        "evidence_points_present",
        "decoded_evidence_backed",
        "causality_closed",
        "expression_quality_pass",
        "professional_structure_complete",
        "professional_wording",
        "no_substantial_sentence_repetition",
    }
)
_TERMINAL_FALLBACK_UNBACKED_EVIDENCE_RE = re.compile(
    r"已取得的证据|关键证据|证据包|evidence[_ -]?ref",
    re.IGNORECASE,
)
DELIVERY_KEY_VERSION = "v1"
DELIVERY_EFFECT_KIND = "feishu_issue_comment"
DELIVERY_THREAD_EFFECT_KIND = "feishu_thread_reply"
DELIVERY_CARD_PATCH_EFFECT_KIND = "feishu_card_patch"
DELIVERY_CARD_PATCH_EFFECT_SCHEMA_VERSION = "pnc_rca_card_patch_effect_v1"
DELIVERY_EFFECT_KINDS = frozenset({
    DELIVERY_EFFECT_KIND,
    DELIVERY_THREAD_EFFECT_KIND,
    DELIVERY_CARD_PATCH_EFFECT_KIND,
})
TERMINAL_DELIVERY_EFFECT_KINDS = frozenset({
    DELIVERY_EFFECT_KIND,
    DELIVERY_THREAD_EFFECT_KIND,
})
DELIVERY_TARGET_SCHEMA_VERSION = "pnc_rca_delivery_target_v1"
TERMINAL_DELIVERY_OUTCOMES = frozenset({"terminal_failed", "quarantined"})
_VM_TMP_PREFIX = "/mnt/tmp/"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,191}$")
_PROJECT_SIMPLE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_FEISHU_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{1,255}$")
_TERMINAL_CODE_RE = re.compile(r"^[a-z][a-z0-9_]{0,119}$")
_TERMINAL_ERROR_CODE_RE = re.compile(
    r"^(?:[a-z][a-z0-9_]{0,119}|taxonomy_gap:[a-z0-9_.-]{1,96})$"
)
_ARTIFACT_SET_ID_RE = re.compile(r"^g1q3-rca-artifact-v1-[0-9a-f]{64}$")
_FORMAL_REPORT_SEGMENT_RE = re.compile(r"^(?:[A-Za-z0-9._~-]|%[0-9A-Fa-f]{2})+$")
_FORMAL_REPORT_CIFS_HOST = "hfs1.minieye.tech"
_FORMAL_REPORT_SHARE = "department-pnc_team-planning_algo-driving"
_FORMAL_REPORT_CIFS_PREFIX = f"//{_FORMAL_REPORT_CIFS_HOST}/{_FORMAL_REPORT_SHARE}/tmp/"
_DELIVERY_MANIFEST_V2_FIELDS = frozenset({
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
})
_DELIVERY_MANIFEST_ARTIFACT_FIELDS = frozenset({
    "role",
    "path",
    "size",
    "sha256",
    "media_type",
    "required",
})
_DELIVERY_HTML_VALIDATION_FIELDS = frozenset({
    "state",
    "report_data_sha256",
    "blockers",
    "fidelity_ok",
})
MAX_DELIVERY_ARTIFACTS = 512
MAX_DELIVERY_ARTIFACT_BYTES = 256 * 1024 * 1024
MAX_DELIVERY_ARTIFACT_TOTAL_BYTES = 512 * 1024 * 1024
MAX_DELIVERY_INDEX_HTML_BYTES = 32 * 1024 * 1024
MAX_FEISHU_COMMENT_BYTES = 8 * 1024
MAX_FEISHU_CARD_BYTES = 64 * 1024
MAX_CONCLUSION_BYTES = 2 * 1024
CONSUMER_CAPABILITY_SCHEMA_VERSION = "rca_consumer_capability_publication_v1"
RCA_RESULT_FIELD_KEY = "field_9193cb"
RCA_REPORT_FIELD_KEY = "field_8c912e"
DELIVERY_REPORT_LINK_KIND = "foxglove_viz"
RERUN_PROMPT_PREFIX = (
    "如需重新分析，请到 PNC-Agent 群 @小助手 并发送：重新分析"
)
ADOPTION_PROMPT_LINE = (
    "责任模块研发请在「是否采纳」字段选择“采纳”或“不采纳”；"
    "如选择“不采纳”，请填写不采纳原因。提单测试同学无需额外操作。"
)
_PROJECT_URL_SLUGS = {
    "t03o4q": "t03o4q",
    "68ef617fb371dc80a10641f7": "t03o4q",
}
_HTML_REPORT_STATUSES = frozenset({
    "html_delivery_ready",
    "report_generated_need_review",
    "report_ready",
})
_VIZ_REPORT_STATUSES = frozenset({"report_ready"})
_VIZ_PUBLICATION_SCHEMA_VERSION = "g1q3_rca_viz_publication_v1"


class DeliveryContractError(ValueError):
    """A permanent artifact or identity error that must fail closed."""

    def __init__(self, code: str, detail: str = ""):
        self.code = str(code or "delivery_contract_invalid")[:120]
        self.detail = str(detail or self.code)[:1000]
        super().__init__(self.detail)


def canonical_issue_url(project_key: Any, work_item_id: Any) -> str:
    project = str(project_key or "").strip()
    issue = str(work_item_id or "").strip()
    slug = _PROJECT_URL_SLUGS.get(project, project)
    if (
        not _PROJECT_SIMPLE_NAME_RE.fullmatch(slug)
        or not issue.isdigit()
        or len(issue) > 32
    ):
        raise DeliveryContractError("delivery_issue_url_invalid")
    return f"https://project.feishu.cn/{slug}/issue/detail/{issue}"


def rerun_prompt_line(issue_url: Any) -> str:
    text = str(issue_url or "").strip()
    parsed = urlparse(text)
    parts = tuple(part for part in parsed.path.split("/") if part)
    if (
        parsed.scheme != "https"
        or parsed.netloc != "project.feishu.cn"
        or parsed.params
        or parsed.query
        or parsed.fragment
        or len(parts) != 4
        or parts[1:3] != ("issue", "detail")
        or not _PROJECT_SIMPLE_NAME_RE.fullmatch(parts[0])
        or not parts[3].isdigit()
        or len(parts[3]) > 32
    ):
        raise DeliveryContractError("delivery_issue_url_invalid")
    return f"{RERUN_PROMPT_PREFIX} {text}"


_UPSTREAM_DISPATCH_SCHEMA_VERSION = "g1q3_upstream_dispatch_v2"
_UPSTREAM_OWNER_BUCKET_LABELS = {
    "acc_longitudinal_control": "纵向控制",
    "aeb": "AEB",
    "fctb_fcw": "AEB_FCW",
    "hmi_sr": "HMI_SR",
    "lane_perception": "车道线感知",
    "lcc_lateral_control": "横向控制",
    "ooi_spp": "目标选择 / SPP",
    "tsr": "TSR",
    "vision_perception": "视觉感知",
}
_UPSTREAM_DISPATCH_REASONS = frozenset(
    {
        "single_owner_bucket_hit",
        "abstain_no_hit",
        "abstain_cross_domain",
        "out_of_scope",
    }
)


def _legacy_abstain_no_hit() -> dict[str, Any]:
    return {
        "hit_evaluator_keys": [],
        "hit_window_envelope": None,
        "hit_windows": [],
        "owner_bucket": None,
        "owner_bucket_label": None,
        "reason": "abstain_no_hit",
        "schema_version": _UPSTREAM_DISPATCH_SCHEMA_VERSION,
        "terminal_classification": "abstain",
    }


def _gate_a_projection(contract: Mapping[str, Any]) -> dict[str, Any] | None:
    raw = contract.get("gate_a_projection")
    if raw is None:
        return None
    try:
        projection = validate_gate_a_projection(raw)
        if projection["level"] == "L1_observation":
            projection = validate_gate_a_projection(
                raw,
                identifier_binding=build_gate_a_identifier_binding(
                    contract.get("consumer_capability")
                ),
            )
        return projection
    except RcaEvidenceProjectionError as exc:
        raise DeliveryContractError(
            "gate_a_projection_invalid", f"gate_a_projection_invalid: {exc}"
        ) from exc


def _sanitize_gate_a_contract(contract: Mapping[str, Any]) -> dict[str, Any]:
    """Replace all decision-bearing fields with the L0/L1 projection."""
    projection = _gate_a_projection(contract)
    if projection is None:
        return dict(contract)
    public = build_gate_a_public_result(projection)
    sanitized = dict(contract)
    sanitized["public_result"] = public
    sanitized["summary"] = dict(public["summary"])
    report = dict(contract.get("report")) if isinstance(
        contract.get("report"), Mapping
    ) else {}
    for field in (
        "candidate_owner_domain",
        "candidate_owner",
        "candidate_responsibility",
        "responsibility_candidate",
        "is_candidate",
    ):
        report.pop(field, None)
    sanitized["report"] = report
    for field in (
        "quality_classification",
        "terminal_class",
        "confidence_tier",
        "approval_ready",
        "human_decision",
    ):
        sanitized.pop(field, None)
    artifacts = contract.get("artifacts")
    if isinstance(artifacts, Mapping):
        artifacts = dict(artifacts)
        artifacts.pop("attribution_causal_text", None)
        sanitized["artifacts"] = artifacts
    sanitized["evidence_boundary"] = list(public.get("evidence_boundary") or [])
    return sanitized


def is_trusted_v19_conclusion_contract(contract: Mapping[str, Any]) -> bool:
    """Return whether the VM sealed a v19 primary conclusion for delivery.

    Gate A remains the default host projection.  This narrow compatibility
    path preserves only the VM pipeline's already-validated primary projection;
    low-confidence, blocked, and observation-only contracts still fail closed
    to the Gate A fact surface.
    """
    if not isinstance(contract, Mapping):
        return False
    capability = contract.get("consumer_capability")
    if not isinstance(capability, Mapping):
        return False
    integrated = capability.get("integrated_sources")
    if not isinstance(integrated, Mapping):
        return False
    communication = integrated.get("conclusion_communication")
    if not isinstance(communication, Mapping):
        return False
    publication = communication.get("publication")
    quality = communication.get("quality_checks")
    if not isinstance(publication, Mapping) or not isinstance(quality, Mapping):
        return False
    if (
        communication.get("schema_version")
        != V19_CONCLUSION_COMMUNICATION_SCHEMA_VERSION
        or communication.get("style_profile") != "human_professional_dense_v2"
        or communication.get("mode") not in V19_PRIMARY_CONCLUSION_MODES
        or publication.get("primary_candidate_eligible") is not True
        or publication.get("reason_codes") != []
        or publication.get("requires_receipt_decoded_backing") is not True
        or any(quality.get(key) is not True for key in V19_REQUIRED_CONCLUSION_CHECKS)
    ):
        return False
    headline = str(communication.get("headline") or "").strip()
    summary = str(communication.get("summary") or "").strip()
    boundary = str(communication.get("evidence_boundary") or "").strip()
    next_action = str(communication.get("next_action") or "").strip()
    evidence_points = communication.get("evidence_points")
    if (
        not headline
        or not summary
        or not boundary
        or not next_action
        or not isinstance(evidence_points, list)
        or not evidence_points
        or not all(str(item or "").strip() for item in evidence_points)
    ):
        return False
    report = contract.get("report")
    artifacts = contract.get("artifacts")
    contract_summary = contract.get("summary")
    public = contract.get("public_result")
    public_summary = public.get("summary") if isinstance(public, Mapping) else None
    if not all(
        isinstance(value, Mapping)
        for value in (report, artifacts, contract_summary, public, public_summary)
    ):
        return False
    if (
        report.get("is_deliverable") is not True
        or str(report.get("status") or "").strip() not in _VIZ_REPORT_STATUSES
        or contract.get("business_state") != "report_completed"
        or contract_summary.get("professional_conclusion_selection_status")
        != "trusted_professional"
        or contract_summary.get("professional_conclusion") != summary
        or public_summary.get("short_conclusion") != summary
        or not str(artifacts.get("attribution_causal_text") or "").strip()
    ):
        return False
    if communication.get("mode") == "high_confidence_candidate":
        if not str(report.get("candidate_owner") or "").strip():
            return False
    return True


def _uses_trusted_v19_primary_projection(contract: Mapping[str, Any]) -> bool:
    """Use the sealed v19 publication when the legacy W1b field is absent."""
    return (
        contract.get("upstream_dispatch") is None
        and is_trusted_v19_conclusion_contract(contract)
    )


def _validated_upstream_dispatch(contract: Mapping[str, Any]) -> dict[str, Any]:
    raw = contract.get("upstream_dispatch")
    if raw is None:
        # Old sealed contracts cannot be reinterpreted as a direction. Treat
        # their missing W1b output as an honest no-hit abstention.
        return _legacy_abstain_no_hit()
    if not isinstance(raw, Mapping) or set(raw) != {
        "hit_evaluator_keys",
        "hit_window_envelope",
        "hit_windows",
        "owner_bucket",
        "owner_bucket_label",
        "reason",
        "schema_version",
        "terminal_classification",
    }:
        raise DeliveryContractError("upstream_dispatch_contract_invalid")
    owner_bucket = raw.get("owner_bucket")
    owner_bucket_label = raw.get("owner_bucket_label")
    reason = raw.get("reason")
    terminal = raw.get("terminal_classification")
    keys = raw.get("hit_evaluator_keys")
    envelope = raw.get("hit_window_envelope")
    windows = raw.get("hit_windows")
    if (
        raw.get("schema_version") != _UPSTREAM_DISPATCH_SCHEMA_VERSION
        or reason not in _UPSTREAM_DISPATCH_REASONS
        or not isinstance(keys, list)
        or len(keys) > MAX_DELIVERY_ARTIFACTS
        or any(not isinstance(key, str) or not _SAFE_KEY_RE.fullmatch(key) for key in keys)
        or keys != sorted(set(keys))
        or not isinstance(windows, list)
        or len(windows) > MAX_DELIVERY_ARTIFACTS
    ):
        raise DeliveryContractError("upstream_dispatch_contract_invalid")
    if reason == "single_owner_bucket_hit":
        if (
            terminal != "valid_dispatch"
            or owner_bucket not in _UPSTREAM_OWNER_BUCKET_LABELS
            or owner_bucket_label
            != _UPSTREAM_OWNER_BUCKET_LABELS.get(owner_bucket)
            or not keys
        ):
            raise DeliveryContractError("upstream_dispatch_contract_invalid")
    elif reason == "out_of_scope":
        if (
            terminal != "out_of_scope"
            or owner_bucket is not None
            or owner_bucket_label is not None
            or keys
            or windows
            or envelope is not None
        ):
            raise DeliveryContractError("upstream_dispatch_contract_invalid")
    elif (
        terminal != "abstain"
        or owner_bucket is not None
        or owner_bucket_label is not None
        or (reason == "abstain_no_hit" and (keys or windows or envelope is not None))
        or (reason == "abstain_cross_domain" and not keys)
    ):
        raise DeliveryContractError("upstream_dispatch_contract_invalid")
    normalized_windows = []
    for window in windows:
        if not isinstance(window, Mapping) or set(window) != {
            "key",
            "t_end",
            "t_start",
        }:
            raise DeliveryContractError("upstream_dispatch_window_invalid")
        evaluator_key = window.get("key")
        start = window.get("t_start")
        end = window.get("t_end")
        if (
            evaluator_key not in keys
            or isinstance(start, bool)
            or isinstance(end, bool)
            or not isinstance(start, (int, float))
            or not isinstance(end, (int, float))
            or not math.isfinite(float(start))
            or not math.isfinite(float(end))
            or float(start) > float(end)
        ):
            raise DeliveryContractError("upstream_dispatch_window_invalid")
        normalized_windows.append(
            {
                "key": str(evaluator_key),
                "t_end": float(end),
                "t_start": float(start),
            }
        )
    if normalized_windows != sorted(
        normalized_windows,
        key=lambda item: (
            item["t_start"],
            item["t_end"],
            item["key"],
        ),
    ):
        raise DeliveryContractError("upstream_dispatch_window_invalid")
    if normalized_windows:
        if not isinstance(envelope, Mapping) or set(envelope) != {
            "participating_key_count",
            "t_end",
            "t_start",
        }:
            raise DeliveryContractError("upstream_dispatch_window_envelope_invalid")
        expected_envelope = {
            "participating_key_count": len(
                {item["key"] for item in normalized_windows}
            ),
            "t_end": max(item["t_end"] for item in normalized_windows),
            "t_start": min(item["t_start"] for item in normalized_windows),
        }
        if dict(envelope) != expected_envelope:
            raise DeliveryContractError("upstream_dispatch_window_envelope_invalid")
        normalized_envelope = expected_envelope
    elif envelope is not None:
        raise DeliveryContractError("upstream_dispatch_window_envelope_invalid")
    else:
        normalized_envelope = None
    return {
        "hit_evaluator_keys": list(keys),
        "hit_window_envelope": normalized_envelope,
        "hit_windows": normalized_windows,
        "owner_bucket": owner_bucket,
        "owner_bucket_label": owner_bucket_label,
        "reason": reason,
        "schema_version": _UPSTREAM_DISPATCH_SCHEMA_VERSION,
        "terminal_classification": terminal,
    }


def _dispatch_first_line(
    dispatch: Mapping[str, Any], *, gate_a_projection: bool = False
) -> str:
    if gate_a_projection:
        return "本单未能定向"
    if dispatch.get("terminal_classification") == "out_of_scope":
        return "本单不在自动分析范围"
    owner_bucket_label = dispatch.get("owner_bucket_label")
    if isinstance(owner_bucket_label, str) and owner_bucket_label:
        return f"建议责任方：{owner_bucket_label}"
    return "本单未能定向"


def _dispatch_window_line(dispatch: Mapping[str, Any]) -> str:
    envelope = dispatch.get("hit_window_envelope")
    if not isinstance(envelope, Mapping):
        return ""
    return (
        "异常时间窗：相对事件起点 "
        f"{float(envelope['t_start']):.3f}s 至 "
        f"{float(envelope['t_end']):.3f}s"
        f"（参与判据 {int(envelope['participating_key_count'])} 个）"
    )


def _abstain_analysis_handoff_lines(
    contract: Mapping[str, Any],
) -> list[str]:
    capability = contract.get("consumer_capability")
    if capability is None and isinstance(contract.get("summary"), Mapping):
        capability = contract["summary"].get("consumer_capability")
    if not capability:
        return []
    if not isinstance(capability, Mapping):
        raise DeliveryContractError("consumer_capability_invalid")
    evidence = capability.get("evidence")
    focus = evidence.get("focus_window") if isinstance(evidence, Mapping) else None
    if (
        not isinstance(focus, Mapping)
        or not {"start_ts", "end_ts"}.issubset(focus)
        or not set(focus).issubset(
            {"start_ts", "end_ts", "start_frame", "end_frame"}
        )
    ):
        raise DeliveryContractError("consumer_capability_focus_window_invalid")
    start = focus.get("start_ts")
    end = focus.get("end_ts")
    if (
        isinstance(start, bool)
        or isinstance(end, bool)
        or not isinstance(start, (int, float))
        or not isinstance(end, (int, float))
        or not math.isfinite(float(start))
        or not math.isfinite(float(end))
        or float(start) > float(end)
    ):
        raise DeliveryContractError("consumer_capability_focus_window_invalid")
    for frame_key in ("start_frame", "end_frame"):
        frame = focus.get(frame_key)
        if frame is not None and (
            isinstance(frame, bool) or not isinstance(frame, int) or frame < 0
        ):
            raise DeliveryContractError("consumer_capability_focus_window_invalid")
    raw_names = [
        *(capability.get("actual_signals") or []),
        *(capability.get("actual_fields") or []),
    ]
    if any(
        not isinstance(name, str)
        or not name.strip()
        or len(name.encode("utf-8")) > 128
        or "\n" in name
        or "\r" in name
        or "`" in name
        for name in raw_names
    ):
        raise DeliveryContractError("consumer_capability_signal_name_invalid")
    names = list(dict.fromkeys(name.strip() for name in raw_names))
    if not names:
        raise DeliveryContractError("consumer_capability_signal_inventory_empty")
    displayed = names[:10]
    suffix = f" 等共 {len(names)} 个" if len(names) > len(displayed) else ""
    return [
        (
            "建议研发重点复核相对问题时刻 "
            f"{float(start):.3f}s-{float(end):.3f}s。"
        ),
        "已读取的关键信号包括 "
        + "、".join(f"`{name}`" for name in displayed)
        + suffix
        + "。",
    ]


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


def issue_focus_payload_sha256(value: Mapping[str, Any]) -> str:
    """Hash the exact issue-focus payload carried by a sealed contract."""

    if not isinstance(value, Mapping):
        raise DeliveryContractError("issue_focus_payload_invalid")
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def normalize_v19_issue_focus_binding(value: Mapping[str, Any]) -> dict[str, Any]:
    """Wrap the sealed v19 receipt focus for the Host delivery contract."""
    if not isinstance(value, Mapping) or set(value) != {"plan", "gate", "hard_stop"}:
        raise DeliveryContractError("issue_focus_v19_binding_invalid")
    plan = value.get("plan")
    gate = value.get("gate")
    if not isinstance(plan, Mapping) or not isinstance(gate, Mapping):
        raise DeliveryContractError("issue_focus_v19_binding_invalid")
    return {
        "schema_version": V19_ISSUE_FOCUS_BINDING_SCHEMA_VERSION,
        "plan": dict(plan),
        "gate": dict(gate),
        "hard_stop": value.get("hard_stop"),
    }


def _validate_v19_issue_focus_binding(
    *, issue_title: str, value: Mapping[str, Any]
) -> IssueFocusValidation:
    if set(value) != {"schema_version", "plan", "gate", "hard_stop"} or (
        value.get("schema_version") != V19_ISSUE_FOCUS_BINDING_SCHEMA_VERSION
        or value.get("hard_stop") is not False
        or value.get("gate") != {}
    ):
        raise DeliveryContractError("issue_focus_v19_binding_invalid")
    plan = value.get("plan")
    expected_plan_fields = {
        "schema_version",
        "analysis_status",
        "issue_intent",
        "title_sha256",
        "intent_sha256",
        "required_capabilities",
        "required_segments",
        "required_entities",
        "required_measurements",
        "required_checks",
        "required_calculations",
        "missing_requirements",
        "unsupported_capabilities",
        "capability_review",
        "stop_reason",
        "source",
        "plan_sha256",
    }
    if not isinstance(plan, Mapping) or set(plan) != expected_plan_fields:
        raise DeliveryContractError("issue_focus_v19_plan_invalid")
    intent = resolve_issue_intent(issue_title)
    expected_requirements = {
        "required_capabilities": list(intent.required_capabilities),
        "required_segments": list(intent.required_segments),
        "required_entities": list(intent.required_entities),
        "required_measurements": list(intent.required_measurements),
        "required_checks": list(intent.required_checks),
        "required_calculations": list(intent.required_calculations),
    }
    if (
        plan.get("schema_version") != ISSUE_FOCUS_PLAN_SCHEMA_VERSION
        or plan.get("analysis_status") != ISSUE_FOCUS_PLAN_STATUS_PLANNED
        or plan.get("issue_intent") != intent.to_dict()
        or plan.get("title_sha256") != issue_title_sha256(issue_title)
        or plan.get("intent_sha256") != intent.intent_sha256
        or any(plan.get(key) != expected for key, expected in expected_requirements.items())
        or plan.get("missing_requirements") != []
        or plan.get("unsupported_capabilities") != []
        or plan.get("stop_reason") != ""
    ):
        raise DeliveryContractError("issue_focus_v19_plan_invalid")
    review = plan.get("capability_review")
    source = plan.get("source")
    if (
        not isinstance(review, list)
        or any(not isinstance(item, str) or not item for item in review)
        or review != sorted(set(review))
        or not isinstance(source, Mapping)
        or set(source)
        != {"kind", "description_sha256", "comments_sha256", "comment_count"}
        or source.get("kind") != "title_plus_bounded_issue_context"
        or not _SHA256_RE.fullmatch(str(source.get("description_sha256") or ""))
        or not _SHA256_RE.fullmatch(str(source.get("comments_sha256") or ""))
        or isinstance(source.get("comment_count"), bool)
        or not isinstance(source.get("comment_count"), int)
        or source.get("comment_count") < 0
    ):
        raise DeliveryContractError("issue_focus_v19_plan_invalid")
    plan_material = dict(plan)
    observed_plan_sha256 = str(plan_material.pop("plan_sha256") or "")
    expected_plan_sha256 = hashlib.sha256(
        _canonical_json(plan_material).encode("utf-8")
    ).hexdigest()
    if observed_plan_sha256 != expected_plan_sha256:
        raise DeliveryContractError("issue_focus_v19_plan_hash_mismatch")
    return IssueFocusValidation(
        intent_sha256=intent.intent_sha256,
        analysis_status=ANALYSIS_COMPLETE,
        attribution_allowed=True,
        missing_requirements=(),
        unsupported_capabilities=(),
    )


def _issue_focus_stop_message(status: str) -> str:
    return {
        ANALYSIS_INSUFFICIENT_STATEMENT: "问题陈述不足，未输出责任归因。",
        ANALYSIS_CAPABILITY_UNSUPPORTED: "问题焦点所需能力未接入，未输出责任归因。",
        ANALYSIS_EVIDENCE_INCOMPLETE: "问题焦点证据不完整，未输出责任归因。",
        ANALYSIS_EVIDENCE_CONFLICT: "问题焦点证据存在冲突，未输出责任归因。",
    }.get(str(status or "").strip(), "问题焦点未通过归因门禁，未输出责任归因。")


def validate_delivery_issue_focus(
    contract: Mapping[str, Any], issue_title: Any = ""
) -> tuple[str, IssueFocusValidation | None]:
    """Bind the immutable submission title to the sealed focus-evidence payload.

    Legacy v1 contracts remain readable when no title/focus is supplied.  Once a
    title is bound (or a v2 contract is presented), focus evidence is mandatory;
    generic evaluator output cannot substitute for it.
    """

    if not isinstance(contract, Mapping):
        raise DeliveryContractError("delivery_contract_missing")
    schema = str(contract.get("schema_version") or "").strip()
    if schema not in DELIVERY_CONTRACT_SCHEMA_VERSIONS:
        raise DeliveryContractError("delivery_contract_schema_unsupported")
    title = normalized_issue_title(issue_title)
    raw_focus = contract.get("issue_focus")
    if (
        isinstance(raw_focus, Mapping)
        and raw_focus.get("schema_version")
        == V19_ISSUE_FOCUS_BINDING_SCHEMA_VERSION
    ):
        if not title:
            raise DeliveryContractError("issue_focus_title_missing")
        if not is_trusted_v19_conclusion_contract(contract):
            raise DeliveryContractError("issue_focus_v19_conclusion_untrusted")
        return title, _validate_v19_issue_focus_binding(
            issue_title=title,
            value=raw_focus,
        )
    if schema == DELIVERY_CONTRACT_SCHEMA_VERSION_V1 and raw_focus is None:
        # v1 is a historical adapter contract.  It remains readable with the
        # generic quality oracle, but a newly bound title may only publish an
        # honest non-attribution until the producer emits v2 focus evidence.
        return title, None
    if schema == DELIVERY_CONTRACT_SCHEMA_VERSION and not title:
        raise DeliveryContractError("issue_focus_title_missing")
    if not title:
        if raw_focus is not None:
            raise DeliveryContractError("issue_focus_title_missing")
        return "", None
    if not raw_focus or not isinstance(raw_focus, Mapping):
        raise DeliveryContractError("issue_focus_evidence_missing")
    if len(title.encode("utf-8")) > 1200:
        raise DeliveryContractError("issue_focus_title_invalid")
    try:
        validation = validate_issue_focus_evidence(
            issue_title=title,
            value=raw_focus,
        )
    except IssueFocusContractError as exc:
        raise DeliveryContractError(exc.code, exc.detail) from exc
    return title, validation


def issue_focus_oracle_contract(
    contract: Mapping[str, Any],
    validation: IssueFocusValidation | None,
) -> dict[str, Any]:
    """Return the contract view used by the generic quality oracle.

    An incomplete focus is an explicit stop.  Candidate-bearing public fields
    are removed from the oracle input so a broad evaluator cannot promote the
    stop to a generic attribution.
    """

    result = dict(contract)
    if validation is None or validation.attribution_allowed:
        return result
    public = (
        dict(result.get("public_result"))
        if isinstance(result.get("public_result"), Mapping)
        else {}
    )
    public["summary"] = {
        "short_conclusion": _issue_focus_stop_message(validation.analysis_status)
    }
    public.pop("candidate", None)
    public["responsibility"] = {"status": "unresolved"}
    public["evidence_summary"] = {"refs": []}
    public["causal_chain"] = {"narrative": []}
    public["user_action"] = {}
    result["public_result"] = public
    result["summary"] = dict(public["summary"])
    report = dict(result.get("report")) if isinstance(result.get("report"), Mapping) else {}
    for field in (
        "candidate_owner_domain",
        "candidate_owner",
        "candidate_responsibility",
        "responsibility_candidate",
        "is_candidate",
    ):
        report.pop(field, None)
    result["report"] = report
    return result


def delivery_oracle_contract(
    contract: Mapping[str, Any],
    validation: IssueFocusValidation | None,
) -> dict[str, Any]:
    """Build the exact oracle input used at both Host and dispatch replay."""

    result = dict(contract)
    # v19 seals an equivalent primary publication under its conclusion
    # communication contract.  Do not reinterpret that sealed result as the
    # legacy W1b no-hit abstention; doing so erases its responsibility before
    # the structural oracle runs.  Older and untrusted contracts retain the
    # historical fail-closed dispatch validation.
    if not _uses_trusted_v19_primary_projection(contract):
        result["upstream_dispatch"] = _validated_upstream_dispatch(contract)
    return issue_focus_oracle_contract(result, validation)


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
_V2_BASE_EFFECT_SEMANTIC_FIELDS = (
    *_V1_BASE_EFFECT_SEMANTIC_FIELDS,
    "project_simple_name",
    "report_link_kind",
)
_V3_BASE_EFFECT_SEMANTIC_FIELDS = (
    *_V2_BASE_EFFECT_SEMANTIC_FIELDS,
    "terminal_class",
    "confidence_tier",
    "quality_oracle",
    "quality_oracle_sha256",
)
_BASE_EFFECT_SEMANTIC_FIELDS = (
    *_V3_BASE_EFFECT_SEMANTIC_FIELDS,
    "result_field_value",
)
_FOCUS_EFFECT_SEMANTIC_FIELDS = (
    "issue_title",
    "issue_focus_sha256",
    "issue_focus_validation",
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
_CARD_PATCH_EFFECT_SEMANTIC_FIELDS = (
    "schema_version",
    "delivery_id",
    "effect_kind",
    "target_key",
    "project_key",
    "work_item_type_key",
    "work_item_id",
    "business_key",
    "submission_key",
    "generation",
    "platform",
    "chat_id",
    "thread_id",
    "message_id",
    "output_cap",
    "adjudication_id",
    "action",
    "conclusion_state",
    "original_effect_key",
    "correction_effect_key",
    "render_hash",
    "card_payload",
)
_CARD_PATCH_EFFECT_FINAL_FIELDS = frozenset({
    *_CARD_PATCH_EFFECT_SEMANTIC_FIELDS,
    "effect_key",
    "semantic_payload_sha256",
    "idempotency_uuid",
})
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
_TERMINAL_FALLBACK_BASE_EFFECT_SEMANTIC_FIELDS = (
    *_TERMINAL_V1_BASE_EFFECT_SEMANTIC_FIELDS,
    "conclusion",
    "terminal_class",
    "confidence_tier",
    "quality_oracle",
    "quality_oracle_sha256",
    "field_updates",
    "terminal_fallback",
)

_TERMINAL_DIAGNOSTIC_RESULTS = {
    "out_of_scope": "本单不在自动分析范围。",
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
        "自动归因未完成（非归因结论）：当前输入中没有可读取的问题数据地址；已转内部待办。"
    ),
    "input_remote_data_invalid": (
        "自动归因未完成（非归因结论）：当前问题数据地址无法解析；已转内部待办。"
    ),
    "input_frame_required": (
        "自动归因未完成（非归因结论）：缺少或无法解析问题发生 frame_id/时间；已转内部待办。"
    ),
    "input_required": (
        "自动归因未完成（非归因结论）：当前输入不足以完成自动分析；已转内部待办。"
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
_TERMINAL_SELF_PROOF = {
    "out_of_scope": (
        "适用性检查 -> 业务范围判定",
        "问题单官方字段与已注册业务能力",
        "当前问题不在已验证的自动分析范围，未运行信号归因判据。",
    ),
    "business_route_unresolved": (
        "官方所属项目字段 -> 业务 profile 解析",
        "飞书问题单官方字段",
        "未解析出唯一业务，未按标题、负责人或群聊猜测。",
    ),
    "business_route_unsupported": (
        "官方所属项目字段 -> 已注册业务能力清单",
        "飞书问题单官方字段与运行时能力注册表",
        "业务已识别但未接入自动 RCA，未借用其他项目评测器。",
    ),
    "business_route_conflict": (
        "官方所属项目字段 -> 业务 profile 一致性检查",
        "飞书问题单官方字段",
        "多个业务身份相互冲突，未选择任一项目评测器。",
    ),
    "business_adapter_not_ready": (
        "官方所属项目字段 -> 业务 profile -> 输入 adapter",
        "飞书问题单官方字段与对应业务适配状态",
        "业务已识别，但输入适配尚未就绪；不能跨项目借用其他归因能力。",
    ),
    "input_remote_data_required": (
        "问题数据地址_PDCL -> RemoteEventReader / RemoteClipReader 引用解析",
        "飞书问题单数据地址字段",
        "未获得可读取的 event 或 clip 引用，未进入信号解析。",
    ),
    "input_remote_data_invalid": (
        "问题数据地址_PDCL -> RemoteEventReader / RemoteClipReader 引用解析",
        "飞书问题单数据地址字段",
        "字段存在但无法形成规范 event 或 clip 引用，未进入信号解析。",
    ),
    "input_frame_required": (
        "问题发生 frame_id / 时间 -> 帧定位解析",
        "飞书问题单 frame 字段",
        "未获得可用帧号或时间锚点，未运行窗口判据。",
    ),
    "input_required": (
        "问题单必需字段 -> 数据引用与帧定位联合检查",
        "飞书问题单官方字段",
        "至少一项必要输入缺失或无效，未运行信号归因判据。",
    ),
    "issue_source_unavailable": (
        "Meegle workitem get / comment list -> MCP 只读降级",
        "飞书问题单字段与评论",
        "只读来源未返回可靠输入，未把读取失败解释为业务缺陷。",
    ),
    "submission_failed": (
        "业务准入 -> 受限任务提交 -> 提交回执",
        "准入快照与任务提交回执",
        "任务在分析执行前终止，未产生信号或判据证据。",
    ),
    "analysis_failed": (
        "输入解析 -> 信号扫描 -> 因果评测 -> 任务终态回执",
        "任务状态与已封存执行回执",
        "执行未形成可复核的判据命中和异常时间窗。",
    ),
}
_BUSINESS_ROUTE_DIAGNOSTIC_CODES = frozenset({
    "business_route_unresolved",
    "business_route_unsupported",
    "business_route_conflict",
    "business_adapter_not_ready",
})
_TERMINAL_INPUT_DIAGNOSTIC_CODES = {
    "out_of_scope": "out_of_scope",
    "not_admissible": "out_of_scope",
    "business_profile_unresolved": "business_route_unresolved",
    "business_profile_unsupported": "business_route_unsupported",
    "business_profile_conflict": "business_route_conflict",
    "business_profile_adapter_not_ready": "business_adapter_not_ready",
    "issue_field_missing_remote_data_reference": "input_remote_data_required",
    "issue_field_invalid_remote_data_reference": "input_remote_data_invalid",
    "issue_field_invalid_frame_reference": "input_frame_required",
    "issue_fields_not_ready": "input_required",
}
_TERMINAL_SOURCE_DIAGNOSTIC_CODES = frozenset({
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
})


def _terminal_semantic_fields(schema_version: Any) -> tuple[str, ...]:
    if schema_version == TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_V1:
        return _TERMINAL_V1_BASE_EFFECT_SEMANTIC_FIELDS
    if schema_version == TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION:
        return _TERMINAL_BASE_EFFECT_SEMANTIC_FIELDS
    if schema_version == TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION:
        return _TERMINAL_FALLBACK_BASE_EFFECT_SEMANTIC_FIELDS
    if schema_version == TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY:
        return _TERMINAL_BASE_EFFECT_SEMANTIC_FIELDS
    if schema_version == TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY:
        return _TERMINAL_FALLBACK_BASE_EFFECT_SEMANTIC_FIELDS
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
    if effect_kind == DELIVERY_CARD_PATCH_EFFECT_KIND:
        if schema_version != DELIVERY_CARD_PATCH_EFFECT_SCHEMA_VERSION:
            raise DeliveryContractError("delivery_effect_schema_unsupported")
        fields = list(_CARD_PATCH_EFFECT_SEMANTIC_FIELDS)
    elif schema_version == DELIVERY_EFFECT_SCHEMA_VERSION_V1:
        fields = list(_V1_BASE_EFFECT_SEMANTIC_FIELDS)
    elif schema_version == DELIVERY_EFFECT_SCHEMA_VERSION_V2:
        fields = list(_V2_BASE_EFFECT_SEMANTIC_FIELDS)
    elif schema_version == DELIVERY_EFFECT_SCHEMA_VERSION_V3:
        fields = list(_V3_BASE_EFFECT_SEMANTIC_FIELDS)
    elif schema_version == DELIVERY_EFFECT_SCHEMA_VERSION:
        fields = list(_BASE_EFFECT_SEMANTIC_FIELDS)
        if any(key in payload for key in _FOCUS_EFFECT_SEMANTIC_FIELDS):
            fields.extend(_FOCUS_EFFECT_SEMANTIC_FIELDS)
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


def _validated_public_foxglove_url(value: Any) -> str:
    text = str(value or "").strip()
    try:
        parsed = urlparse(text)
        query = parse_qs(parsed.query, keep_blank_values=True)
    except ValueError as exc:
        raise DeliveryContractError("foxglove_url_invalid") from exc
    paths = query.get("ds.mcapPath", [])
    if (
        query.get("ds") != ["foxglove-http"]
        or set(query) != {"ds", "ds.mcapPath"}
        or len(paths) != 1
        or not validate_foxglove_url(text, paths[0])
    ):
        raise DeliveryContractError("foxglove_url_invalid")
    return text


def _reject_public_internal_html(
    value: Any,
    *,
    protected_foxglove_url: str = "",
) -> None:
    if contains_internal_rca_html_reference(
        value,
        protected_foxglove_url=protected_foxglove_url,
    ):
        raise DeliveryContractError("delivery_public_html_reference_forbidden")


def build_issue_comment_content(
    *,
    marker: str,
    work_item_id: str,
    report_status: str,
    conclusion: str,
    report_url: str,
    foxglove_url: str,
    report_cifs_path: str,
    issue_url: str,
    terminal_class: str = "",
) -> str:
    public_url = _validated_public_foxglove_url(foxglove_url)
    lines = conclusion.splitlines() if conclusion else []
    if not lines or not (
        lines[0].startswith("建议责任方：")
        or lines[0] in {"本单未能定向", "本单不在自动分析范围"}
    ):
        raise DeliveryContractError("delivery_dispatch_first_line_invalid")
    lines.extend([
        f"Foxglove 证据：{public_url}",
    ])
    if (
        terminal_class == HONEST_NON_ATTRIBUTION
        and "建议研发重点复核相对问题时刻" in conclusion
    ):
        lines.append(
            "请在上述 Foxglove 证据中自行查看该区间；本轮不承诺自动跳转或预置 layout。"
        )
    lines.extend([marker, rerun_prompt_line(issue_url)])
    if terminal_class in {SUPPORTED_ATTRIBUTION, CANDIDATE_HYPOTHESIS}:
        lines.append(ADOPTION_PROMPT_LINE)
    content = "\n".join(lines)
    _reject_public_internal_html(
        content,
        protected_foxglove_url=public_url,
    )
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
    requester_id: str,
    terminal_class: str = "",
) -> str:
    public_url = _validated_public_foxglove_url(foxglove_url)
    lines = conclusion.splitlines() if conclusion else []
    if not lines or not (
        lines[0].startswith("建议责任方：")
        or lines[0] in {"本单未能定向", "本单不在自动分析范围"}
    ):
        raise DeliveryContractError("delivery_dispatch_first_line_invalid")
    lines.extend([
        f"Foxglove 证据：{public_url}",
        f"问题单：{issue_url}",
        f"发起人：{build_at_mention(requester_id)}",
    ])
    if (
        terminal_class == HONEST_NON_ATTRIBUTION
        and "建议研发重点复核相对问题时刻" in conclusion
    ):
        lines.append(
            "请在上述 Foxglove 证据中自行查看该区间；本轮不承诺自动跳转或预置 layout。"
        )
    lines.extend([marker, rerun_prompt_line(issue_url)])
    if terminal_class in {SUPPORTED_ATTRIBUTION, CANDIDATE_HYPOTHESIS}:
        lines.append(ADOPTION_PROMPT_LINE)
    content = "\n".join(lines)
    _reject_public_internal_html(
        content,
        protected_foxglove_url=public_url,
    )
    if len(content.encode("utf-8")) > MAX_FEISHU_COMMENT_BYTES:
        raise DeliveryContractError("delivery_thread_reply_too_large")
    return content


def terminal_delivery_effect_semantic_payload(
    payload: Mapping[str, Any], effect_kind: str
) -> dict[str, Any]:
    if effect_kind not in TERMINAL_DELIVERY_EFFECT_KINDS:
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
    if effect_kind not in TERMINAL_DELIVERY_EFFECT_KINDS:
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
    if (
        isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation < 1
    ):
        raise DeliveryContractError("terminal_delivery_generation_invalid")
    return f"[RCA_TERMINAL:{effect_key}:{outcome}:{generation}]"


def _terminal_code(value: Any, field: str) -> str:
    text = str(value or "").strip().lower()
    pattern = _TERMINAL_ERROR_CODE_RE if field == "error_code" else _TERMINAL_CODE_RE
    if not pattern.fullmatch(text):
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
    issue_url: str,
    thread: bool,
    diagnostic_result: str = "",
    requester_id: str = "",
) -> str:
    lines = (
        diagnostic_result.splitlines()
        if diagnostic_result
        else [
            "本单未能定向",
            MEDIUM_TIER_DISCLAIMER,
            "未发现已知异常模式",
            "本次没有判据命中，无法提供异常时间窗。",
        ]
    )
    if thread:
        lines.append(f"发起人：{build_at_mention(requester_id)}")
    lines.extend([marker, rerun_prompt_line(issue_url)])
    content = "\n".join(lines)
    _reject_public_internal_html(content)
    if len(content.encode("utf-8")) > MAX_FEISHU_COMMENT_BYTES:
        raise DeliveryContractError("terminal_delivery_content_too_large")
    return content


def _validated_terminal_fallback(value: Any) -> dict[str, Any]:
    fallback = dict(value) if isinstance(value, Mapping) else {}
    expected_keys = {
        "schema_version",
        "work_started_at",
        "deadline_at",
        "elapsed_seconds",
        "confidence_tier",
        "terminal_class",
        "route_key",
        "route_kind",
        "route_owner",
    }
    if set(fallback) != expected_keys:
        raise DeliveryContractError("terminal_fallback_contract_invalid")
    try:
        started = datetime.fromisoformat(
            str(fallback["work_started_at"]).replace("Z", "+00:00")
        )
        deadline = datetime.fromisoformat(
            str(fallback["deadline_at"]).replace("Z", "+00:00")
        )
    except (TypeError, ValueError) as exc:
        raise DeliveryContractError("terminal_fallback_contract_invalid") from exc
    if (
        started.tzinfo is None
        or started.utcoffset() is None
        or deadline.tzinfo is None
        or deadline.utcoffset() is None
        or int((deadline - started).total_seconds()) != 30 * 60
        or type(fallback.get("elapsed_seconds")) is not int
        or int(fallback["elapsed_seconds"]) < 30 * 60
        or fallback.get("schema_version") != TERMINAL_FALLBACK_CONTRACT_SCHEMA_VERSION
        or fallback.get("confidence_tier") != "low"
        or fallback.get("terminal_class") != HONEST_NON_ATTRIBUTION
        or re.fullmatch(
            r"rca-failure-route-[0-9a-f]{64}", str(fallback.get("route_key") or "")
        )
        is None
        or fallback.get("route_kind")
        not in {"infra_remediation_hold", "internal_backlog", "internal_alert"}
        or _SAFE_KEY_RE.fullmatch(str(fallback.get("route_owner") or "")) is None
    ):
        raise DeliveryContractError("terminal_fallback_contract_invalid")
    return fallback


def _terminal_fallback_public_contract() -> dict[str, Any]:
    return {
        "schema_version": TERMINAL_FALLBACK_PUBLIC_CONTRACT_SCHEMA_VERSION,
        "terminal_class": HONEST_NON_ATTRIBUTION,
        "public_result": {
            "terminal_class": HONEST_NON_ATTRIBUTION,
            "summary": {
                "short_conclusion": "自动RCA未归因：本次自动处理未形成可确认的归因结论。",
            },
            "responsibility": {
                "status": "not_attributed",
                "candidate": "暂无法判断",
            },
            "causal_chain": {"narrative": []},
        },
    }


def _terminal_fallback_public_result() -> str:
    return "\n".join((
        "本单未能定向",
        MEDIUM_TIER_DISCLAIMER,
        "检查路径：任务终态 -> 失败路由回执 -> 已封存证据清单。",
        "数据来源：任务状态与失败路由回执。",
        "检查结果：未获得可复核的信号或判据证据，无法提供异常时间窗。",
    ))


def _validate_terminal_fallback_publication(
    public_contract: Mapping[str, Any], publication_text: str
) -> None:
    inspected = f"{_canonical_json(dict(public_contract))}\n{publication_text}"
    if _TERMINAL_FALLBACK_UNBACKED_EVIDENCE_RE.search(inspected):
        raise DeliveryContractError("terminal_fallback_unbacked_evidence_claim")


def _terminal_fallback_content(
    *, marker: str, conclusion: str, issue_url: str, requester_id: str = ""
) -> str:
    lines = [*conclusion.splitlines()]
    if requester_id:
        lines.append(f"发起人：{build_at_mention(requester_id)}")
    lines.extend([marker, rerun_prompt_line(issue_url)])
    content = "\n".join(lines)
    _reject_public_internal_html(content)
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
    terminal_fallback: Mapping[str, Any] | None = None,
    schema_version: str = TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION,
) -> VerifiedTerminalDelivery:
    values = {
        "business_key": _required_text(business_key, "business_key"),
        "submission_key": _required_text(submission_key, "submission_key"),
        "project_key": _required_text(project_key, "project_key"),
        "work_item_type_key": _required_text(work_item_type_key, "work_item_type_key"),
        "work_item_id": _required_text(work_item_id, "work_item_id"),
    }
    if not all(_SAFE_KEY_RE.fullmatch(value) for value in values.values()):
        raise DeliveryContractError("terminal_delivery_identity_invalid")
    if (
        isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation < 1
    ):
        raise DeliveryContractError("terminal_delivery_generation_invalid")
    normalized_outcome = _terminal_code(outcome, "outcome")
    if normalized_outcome not in TERMINAL_DELIVERY_OUTCOMES:
        raise DeliveryContractError("terminal_delivery_outcome_invalid")
    normalized_state = _terminal_code(terminal_state, "state")
    normalized_error = _terminal_code(error_code, "error_code")
    if schema_version not in {
        TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_V1,
        TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION,
        TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION,
        TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY,
        TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY,
    }:
        raise DeliveryContractError("terminal_delivery_schema_unsupported")
    target_key = (
        f"feishu_project:{values['project_key']}:{values['work_item_type_key']}:"
        f"{values['work_item_id']}"
    )
    issue_url = canonical_issue_url(
        values["project_key"], values["work_item_id"]
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
    fallback_conclusion = ""
    fallback_contract: dict[str, Any] = {}
    if schema_version in {
        TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION,
        TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY,
    }:
        fallback = _validated_terminal_fallback(terminal_fallback)
        public_contract = _terminal_fallback_public_contract()
        fallback_conclusion = _terminal_fallback_public_result()
        _validate_terminal_fallback_publication(
            public_contract,
            fallback_conclusion,
        )
        oracle = evaluate_structural_tier(
            public_contract,
            publication_text=fallback_conclusion,
        )
        try:
            require_publishable(oracle)
        except TierOracleConflict as exc:
            raise DeliveryContractError(
                "classification_conflict",
                ",".join(exc.result.violations)
                or f"{exc.result.terminal_class}_not_publishable",
            ) from exc
        if (
            oracle.schema_version != "pnc_rca_structural_tier_oracle_v2"
            or oracle.terminal_class != HONEST_NON_ATTRIBUTION
            or oracle.confidence_tier != "low"
        ):
            raise DeliveryContractError("terminal_fallback_oracle_invalid")
        semantic.update({
            "conclusion": fallback_conclusion,
            "terminal_class": oracle.terminal_class,
            "confidence_tier": oracle.confidence_tier,
            "quality_oracle": oracle.as_dict(),
            "quality_oracle_sha256": oracle.sha256(),
            "field_updates": (
                []
                if schema_version
                == TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY
                else [
                    {
                        "field_key": RCA_RESULT_FIELD_KEY,
                        "field_value": fallback_conclusion,
                    },
                ]
            ),
            "terminal_fallback": fallback,
        })
        fallback_contract = {
            "schema_version": TERMINAL_FALLBACK_CONTRACT_SCHEMA_VERSION,
            "generation": generation,
            "terminal_fallback": fallback,
            "public_contract": public_contract,
            "terminal_class": oracle.terminal_class,
            "confidence_tier": oracle.confidence_tier,
            "quality_oracle": oracle.as_dict(),
            "quality_oracle_sha256": oracle.sha256(),
            "report_field_write_policy": "preserve_existing",
        }
        normalized_diagnostic_code = ""
    elif schema_version in {
        TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION,
        TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY,
    }:
        if terminal_fallback is not None:
            raise DeliveryContractError("terminal_fallback_schema_invalid")
        derived_diagnostic_code = terminal_diagnostic_code(
            normalized_error,
            source_error_code=source_error_code,
        )
        normalized_diagnostic_code = str(
            diagnostic_code or derived_diagnostic_code
        ).strip()
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
        field_updates = (
            []
            if schema_version == TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY
            else [
                {
                    "field_key": RCA_RESULT_FIELD_KEY,
                    "field_value": diagnostic_result,
                },
            ]
        )
        semantic.update({
            "diagnostic_code": normalized_diagnostic_code,
            "diagnostic_result": diagnostic_result,
            "field_updates": field_updates,
        })
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
        if terminal_fallback is not None:
            raise DeliveryContractError("terminal_fallback_schema_invalid")
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
    marker = terminal_delivery_effect_marker(effect_key, normalized_outcome, generation)
    if schema_version in {
        TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION,
        TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY,
    }:
        comment_content = _terminal_fallback_content(
            marker=marker,
            conclusion=fallback_conclusion,
            issue_url=issue_url,
        )
        _validate_terminal_fallback_publication(
            fallback_contract["public_contract"],
            comment_content,
        )
        replayed_oracle = evaluate_structural_tier(
            fallback_contract["public_contract"],
            publication_text=f"{fallback_conclusion}\n{comment_content}",
        )
        try:
            require_publishable(replayed_oracle)
        except TierOracleConflict as exc:
            raise DeliveryContractError(
                "classification_conflict",
                ",".join(exc.result.violations)
                or f"{exc.result.terminal_class}_not_publishable",
            ) from exc
        if replayed_oracle.as_dict() != semantic["quality_oracle"]:
            raise DeliveryContractError(
                "classification_conflict",
                "terminal fallback oracle changed after final rendering",
            )
    else:
        comment_content = _terminal_content(
            marker=marker,
            outcome=normalized_outcome,
            terminal_state=normalized_state,
            error_code=normalized_error,
            submission_key=values["submission_key"],
            generation=generation,
            issue_url=issue_url,
            thread=False,
            diagnostic_result=diagnostic_result,
        )
    payload = {
        **semantic,
        "effect_key": effect_key,
        "semantic_payload_sha256": semantic_sha,
        "marker": marker,
        "comment_content": comment_content,
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
        contract=fallback_contract or diagnostic_contract,
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
            TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION,
            TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY,
            TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY,
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
    semantic.update({
        "effect_kind": DELIVERY_THREAD_EFFECT_KIND,
        "target_key": target_key,
        **{key: validated_target[key] for key in _THREAD_EFFECT_SEMANTIC_FIELDS},
    })
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
    if issue.get("schema_version") in {
        TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION,
        TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY,
    }:
        message_content = _terminal_fallback_content(
            marker=marker,
            conclusion=str(semantic.get("conclusion") or ""),
            issue_url=canonical_issue_url(
                semantic.get("project_key"), semantic.get("work_item_id")
            ),
            requester_id=str(semantic.get("requester_id") or ""),
        )
    else:
        message_content = _terminal_content(
            marker=marker,
            outcome=outcome,
            terminal_state=str(semantic.get("terminal_state") or ""),
            error_code=str(semantic.get("error_code") or ""),
            submission_key=str(semantic.get("submission_key") or ""),
            generation=generation,
            issue_url=canonical_issue_url(
                semantic.get("project_key"), semantic.get("work_item_id")
            ),
            thread=True,
            diagnostic_result=str(semantic.get("diagnostic_result") or ""),
            requester_id=str(semantic.get("requester_id") or ""),
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
    elif effect_kind == DELIVERY_CARD_PATCH_EFFECT_KIND:
        expected_keys = {
            "schema_version",
            "platform",
            "chat_id",
            "thread_id",
            "message_id",
            "submission_key",
            "output_cap",
        }
        if set(value) != expected_keys:
            raise DeliveryContractError("delivery_subscription_target_invalid")
        chat_id = str(value.get("chat_id") or "").strip()
        thread_id = str(value.get("thread_id") or "").strip()
        message_id = str(value.get("message_id") or "").strip()
        submission_key = str(value.get("submission_key") or "").strip()
        thread_anchor = thread_id.removeprefix("topic:")
        if (
            _FEISHU_ID_RE.fullmatch(chat_id) is None
            or _FEISHU_ID_RE.fullmatch(message_id) is None
            or not chat_id.startswith("oc_")
            or not message_id.startswith("om_")
            or _SAFE_KEY_RE.fullmatch(submission_key) is None
            or (
                thread_id
                and (
                    not thread_id.startswith("topic:om_")
                    or _FEISHU_ID_RE.fullmatch(thread_anchor) is None
                )
            )
        ):
            raise DeliveryContractError("delivery_subscription_target_invalid")
        expected = {
            "schema_version": DELIVERY_TARGET_SCHEMA_VERSION,
            "platform": "feishu",
            "chat_id": chat_id,
            "thread_id": thread_id,
            "message_id": message_id,
            "submission_key": submission_key,
            "output_cap": "L1",
        }
        expected_key = f"feishu_card:{chat_id}:{message_id}"
    else:
        raise DeliveryContractError("delivery_effect_kind_unsupported")
    if value != expected or target_key != expected_key:
        raise DeliveryContractError("delivery_subscription_target_invalid")
    return expected


def card_patch_payload_has_exact_submission_marker(
    value: Any,
    *,
    submission_key: str,
) -> bool:
    """Bind a rendered task card to one exact, parseable tracking marker."""

    key = str(submission_key or "").strip()
    if _SAFE_KEY_RE.fullmatch(key) is None or not isinstance(value, Mapping):
        return False
    elements = value.get("elements")
    if not isinstance(elements, list):
        return False
    expected = f"追踪号：`{key}`"
    return any(
        isinstance(element, Mapping)
        and element.get("tag") == "markdown"
        and str(element.get("content") or "").strip() == expected
        for element in elements
    )


def _validated_card_payload(value: Any, *, submission_key: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise DeliveryContractError("delivery_card_patch_payload_invalid")
    card_payload = dict(value)
    if (
        not isinstance(card_payload.get("header"), Mapping)
        or not isinstance(card_payload.get("elements"), list)
        or not card_payload["elements"]
    ):
        raise DeliveryContractError("delivery_card_patch_payload_invalid")
    try:
        encoded = _canonical_json(card_payload).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise DeliveryContractError("delivery_card_patch_payload_invalid") from exc
    if len(encoded) > MAX_FEISHU_CARD_BYTES or not (
        card_patch_payload_has_exact_submission_marker(
            card_payload,
            submission_key=submission_key,
        )
    ):
        raise DeliveryContractError("delivery_card_patch_payload_invalid")
    return card_payload


def card_patch_render_hash(card_payload: Mapping[str, Any]) -> str:
    try:
        encoded = _canonical_json(dict(card_payload)).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise DeliveryContractError("delivery_card_patch_payload_invalid") from exc
    if len(encoded) > MAX_FEISHU_CARD_BYTES:
        raise DeliveryContractError("delivery_card_patch_payload_invalid")
    return hashlib.sha256(encoded).hexdigest()


def validate_card_patch_effect_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise DeliveryContractError("delivery_card_patch_effect_shape_invalid")
    value = dict(payload or {})
    if set(value) != _CARD_PATCH_EFFECT_FINAL_FIELDS:
        raise DeliveryContractError("delivery_card_patch_effect_shape_invalid")
    if (
        value.get("schema_version") != DELIVERY_CARD_PATCH_EFFECT_SCHEMA_VERSION
        or value.get("effect_kind") != DELIVERY_CARD_PATCH_EFFECT_KIND
    ):
        raise DeliveryContractError("delivery_card_patch_effect_schema_invalid")
    generation = value.get("generation")
    if isinstance(generation, bool) or not isinstance(generation, int) or generation < 1:
        raise DeliveryContractError("delivery_card_patch_effect_identity_invalid")
    for key in (
        "delivery_id",
        "project_key",
        "work_item_type_key",
        "business_key",
        "submission_key",
        "adjudication_id",
        "original_effect_key",
        "correction_effect_key",
    ):
        if _SAFE_KEY_RE.fullmatch(str(value.get(key) or "")) is None:
            raise DeliveryContractError("delivery_card_patch_effect_identity_invalid")
    work_item_id = str(value.get("work_item_id") or "").strip()
    if not work_item_id.isdigit():
        raise DeliveryContractError("delivery_card_patch_effect_identity_invalid")
    action = str(value.get("action") or "")
    conclusion_state = str(value.get("conclusion_state") or "")
    if {"recognize": "recognized", "retract": "invalidated"}.get(action) != conclusion_state:
        raise DeliveryContractError("delivery_card_patch_effect_state_invalid")
    validate_delivery_subscription_target(
        effect_kind=DELIVERY_CARD_PATCH_EFFECT_KIND,
        target_key=str(value.get("target_key") or ""),
        target={
            "schema_version": DELIVERY_TARGET_SCHEMA_VERSION,
            **{
                key: value.get(key)
                for key in (
                    "platform",
                    "chat_id",
                    "thread_id",
                    "message_id",
                    "submission_key",
                    "output_cap",
                )
            },
        },
        project_key=str(value.get("project_key") or ""),
        work_item_type_key=str(value.get("work_item_type_key") or ""),
        work_item_id=work_item_id,
    )
    card_payload = _validated_card_payload(
        value.get("card_payload"),
        submission_key=str(value.get("submission_key") or ""),
    )
    expected_render_hash = card_patch_render_hash(card_payload)
    if value.get("render_hash") != expected_render_hash:
        raise DeliveryContractError("delivery_card_patch_render_hash_invalid")
    semantic_sha = compute_delivery_effect_payload_sha256(
        value, DELIVERY_CARD_PATCH_EFFECT_KIND
    )
    expected_effect_key = compute_delivery_effect_key(
        delivery_id=str(value.get("delivery_id") or ""),
        effect_kind=DELIVERY_CARD_PATCH_EFFECT_KIND,
        target_key=str(value.get("target_key") or ""),
        semantic_payload_sha256=semantic_sha,
    )
    if (
        value.get("semantic_payload_sha256") != semantic_sha
        or value.get("effect_key") != expected_effect_key
        or value.get("idempotency_uuid")
        != delivery_effect_idempotency_uuid(expected_effect_key)
    ):
        raise DeliveryContractError("delivery_card_patch_effect_hash_invalid")
    return {
        **value,
        "card_payload": card_payload,
    }


def build_card_patch_effect(
    *,
    delivery_id: str,
    project_key: str,
    work_item_type_key: str,
    work_item_id: str,
    business_key: str,
    submission_key: str,
    generation: int,
    adjudication_id: str,
    action: str,
    conclusion_state: str,
    original_effect_key: str,
    correction_effect_key: str,
    target_key: str,
    target: Mapping[str, Any],
    card_payload: Mapping[str, Any],
) -> tuple[str, str, dict[str, Any]]:
    validated_target = validate_delivery_subscription_target(
        effect_kind=DELIVERY_CARD_PATCH_EFFECT_KIND,
        target_key=target_key,
        target=target,
        project_key=project_key,
        work_item_type_key=work_item_type_key,
        work_item_id=work_item_id,
    )
    normalized_submission_key = str(submission_key or "").strip()
    if validated_target["submission_key"] != normalized_submission_key:
        raise DeliveryContractError("delivery_card_patch_effect_identity_invalid")
    validated_card = _validated_card_payload(
        card_payload,
        submission_key=normalized_submission_key,
    )
    semantic = {
        "schema_version": DELIVERY_CARD_PATCH_EFFECT_SCHEMA_VERSION,
        "delivery_id": str(delivery_id or "").strip(),
        "effect_kind": DELIVERY_CARD_PATCH_EFFECT_KIND,
        "target_key": str(target_key or "").strip(),
        "project_key": str(project_key or "").strip(),
        "work_item_type_key": str(work_item_type_key or "").strip(),
        "work_item_id": str(work_item_id or "").strip(),
        "business_key": str(business_key or "").strip(),
        "submission_key": normalized_submission_key,
        "generation": generation,
        "platform": validated_target["platform"],
        "chat_id": validated_target["chat_id"],
        "thread_id": validated_target["thread_id"],
        "message_id": validated_target["message_id"],
        "output_cap": validated_target["output_cap"],
        "adjudication_id": str(adjudication_id or "").strip(),
        "action": str(action or "").strip(),
        "conclusion_state": str(conclusion_state or "").strip(),
        "original_effect_key": str(original_effect_key or "").strip(),
        "correction_effect_key": str(correction_effect_key or "").strip(),
        "render_hash": card_patch_render_hash(validated_card),
        "card_payload": validated_card,
    }
    semantic_sha = compute_delivery_effect_payload_sha256(
        semantic, DELIVERY_CARD_PATCH_EFFECT_KIND
    )
    effect_key = compute_delivery_effect_key(
        delivery_id=semantic["delivery_id"],
        effect_kind=DELIVERY_CARD_PATCH_EFFECT_KIND,
        target_key=semantic["target_key"],
        semantic_payload_sha256=semantic_sha,
    )
    payload = {
        **semantic,
        "effect_key": effect_key,
        "semantic_payload_sha256": semantic_sha,
        "idempotency_uuid": delivery_effect_idempotency_uuid(effect_key),
    }
    return effect_key, semantic_sha, validate_card_patch_effect_payload(payload)


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
    if schema_version == DELIVERY_EFFECT_SCHEMA_VERSION_V3:
        base_fields = _V3_BASE_EFFECT_SEMANTIC_FIELDS
    elif schema_version == DELIVERY_EFFECT_SCHEMA_VERSION:
        base_fields = list(_BASE_EFFECT_SEMANTIC_FIELDS)
        if any(key in issue for key in _FOCUS_EFFECT_SEMANTIC_FIELDS):
            base_fields.extend(_FOCUS_EFFECT_SEMANTIC_FIELDS)
    else:
        raise DeliveryContractError("delivery_effect_schema_unsupported")
    semantic = {
        key: issue.get(key)
        for key in base_fields
        if key not in {"effect_kind", "target_key"}
    }
    semantic.update({
        "effect_kind": DELIVERY_THREAD_EFFECT_KIND,
        "target_key": target_key,
        **{key: validated_target[key] for key in _THREAD_EFFECT_SEMANTIC_FIELDS},
    })
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
        requester_id=str(semantic.get("requester_id") or ""),
        terminal_class=str(semantic.get("terminal_class") or ""),
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
            summary.get("consumer_capability") if isinstance(summary, Mapping) else None
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
    if not all(
        isinstance(value, list) for value in (signals, fields, evaluators, unused)
    ):
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
    (
        "原始 mcap 已解码出函数级证据，可直接进入 RCA",
        "生产数据已成功读取，并提取到可核验的功能证据",
    ),
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
        if line.strip()
        and not any(fragment in line for fragment in _PUBLIC_INTERNAL_FRAGMENTS)
    ]
    text = "；".join(kept)
    for source, replacement in _PUBLIC_TEXT_REPLACEMENTS:
        text = re.sub(re.escape(source), replacement, text, flags=re.IGNORECASE)
    text = re.sub(r"(?<=[\u4e00-\u9fff])\s+(?=[\u4e00-\u9fff])", "", text)
    for prefix in ("候选因果判断：", "候选原因：", "诊断结论："):
        if text.startswith(prefix):
            text = text[len(prefix) :].strip()
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
            "系统已转入内部证据待办，不要求问题发起人执行操作。",
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
            "系统已记录内部处理路径，不要求问题发起人执行维护操作。",
        ),
    )


def build_public_rca_result(contract: Mapping[str, Any]) -> dict[str, Any]:
    """Project a delivery contract into the small result a Feishu user reviews.

    The report keeps its full machine-readable lineage.  This projection is
    deliberately limited to decision-bearing fields and never exposes model,
    evaluator, generation, or terminal implementation details.
    """
    contract = contract if isinstance(contract, Mapping) else {}
    gate_a_projection = _gate_a_projection(contract)
    if gate_a_projection is not None:
        contract = _sanitize_gate_a_contract(contract)
    public = contract.get("public_result")
    public = public if isinstance(public, Mapping) else {}
    summary = (
        public.get("summary") if isinstance(public.get("summary"), Mapping) else {}
    )
    if not summary:
        raw_summary = contract.get("summary")
        summary = raw_summary if isinstance(raw_summary, Mapping) else {}
    report = (
        contract.get("report") if isinstance(contract.get("report"), Mapping) else {}
    )
    responsibility = (
        public.get("responsibility")
        if isinstance(public.get("responsibility"), Mapping)
        else {}
    )
    causal = (
        public.get("causal_chain")
        if isinstance(public.get("causal_chain"), Mapping)
        else {}
    )
    evidence = (
        public.get("evidence_summary")
        if isinstance(public.get("evidence_summary"), Mapping)
        else {}
    )
    terminal = (
        public.get("terminal_diagnostic")
        if isinstance(public.get("terminal_diagnostic"), Mapping)
        else {}
    )

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

    artifacts = (
        contract.get("artifacts")
        if isinstance(contract.get("artifacts"), Mapping)
        else {}
    )
    fallback_causal_text, fallback_boundary = _public_attribution_text(
        _public_first_text(
            artifacts.get("attribution_causal_text"),
            summary.get("short_conclusion"),
            limit=1000,
        )
    )
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
                item.get("claim"),
                item.get("narrative"),
                item.get("text"),
                item.get("summary"),
                limit=700,
            )
        supporting = item.get("supporting_evidence")
        if not isinstance(supporting, list):
            continue
        for evidence_item in supporting[:4]:
            if not isinstance(evidence_item, Mapping):
                continue
            detail = _public_first_text(
                evidence_item.get("evidence"),
                evidence_item.get("summary"),
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
            causal_text = (
                f"{structured_evidence[0].rstrip('。')}，因此{hypothesis_claim}"
            )
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
                    compact = (
                        item.get("summary")
                        or item.get("field")
                        or item.get("check")
                        or item.get("fit_source")
                    )
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
    action = (
        public.get("user_action")
        if isinstance(public.get("user_action"), Mapping)
        else {}
    )
    if not action:
        raw_action = contract.get("user_action")
        action = raw_action if isinstance(raw_action, Mapping) else {}
    next_action = _public_action(
        action.get("next_action_text") or action.get("next_action")
    )

    if no_attribution:
        conclusion, impact, default_action = _public_terminal_blocker(terminal_code)
        specific = evidence_text or fallback_causal_text or short
        causal_boundary = "暂无足够证据建立可确认的因果链。"
        if not terminal_code and any(
            marker in short for marker in ("数据源不一致", "证据冲突", "未找到该目标")
        ):
            conclusion = "问题单描述的目标与生产数据不一致，当前不能确认责任归因。"
            impact = "问题单中的目标信息无法在绑定的生产数据中匹配。"
            causal_boundary = (
                "问题单目标描述 → 生产数据核验 → 关键目标不匹配 → 无法验证责任因果链。"
            )
            default_action = "请核对绑定的 PDCL 事件及目标信息；修正数据地址、时间或目标后重新发起 RCA。"
        elif not terminal_code and "未提供可核验的现象描述" in short:
            conclusion = "生产数据已读取，但问题现象描述不足，当前不能确认归因。"
            impact = "缺少可核验的异常现象，无法选择并验证对应因果机制。"
            causal_boundary = (
                "生产数据读取 → 问题现象不明确 → 无法选择归因机制 → 转人工补充。"
            )
            default_action = "请补充发生了什么、预期行为及实际异常后重新发起 RCA。"
        elif gate_a_projection is not None:
            if gate_a_projection["level"] == "L1_observation":
                conclusion = "本次仅发布已观测事实，未输出责任归因。"
                impact = boundary or "域 golden 未绑定，当前不输出责任归因。"
                causal_boundary = (
                    "已读取评测器观测事实 → 域 golden 未绑定 → 不输出责任归因。"
                )
                default_action = "域 golden 完成 Gate B 准入后再评估责任归因。"
                specific = evidence_text or short
            else:
                conclusion = short or "本次未取得可用于归因的分析数据。"
                impact = boundary or conclusion
                causal_boundary = "分析输入未物化 → 不生成证据判断 → 不输出责任归因。"
                default_action = "系统保留弃权结果；待输入可用后再执行分析。"
                specific = short or evidence_text
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


def render_public_rca_result(
    contract: Mapping[str, Any], *, terminal_class: str = ""
) -> str:
    result = build_public_rca_result(contract)
    focus = contract.get("issue_focus") if isinstance(contract, Mapping) else None
    if isinstance(focus, Mapping):
        focus_status = str(focus.get("analysis_status") or "").strip()
        if focus_status and focus_status != ANALYSIS_COMPLETE:
            # Focus stops are deliberately independent of the generic dispatch
            # evaluator.  Keep the public first line in the existing abstention
            # grammar so the comment renderer cannot turn a stop into blame.
            return "\n".join(
                ["本单未能定向", _issue_focus_stop_message(focus_status)]
            )
    if _uses_trusted_v19_primary_projection(contract):
        # The v19 conclusion communication is already receipt-backed and
        # quality-checked.  Render that public projection directly rather than
        # manufacturing a legacy upstream-dispatch hit from an owner label.
        communication = contract["consumer_capability"]["integrated_sources"][
            "conclusion_communication"
        ]
        mode = communication["mode"]
        if mode == "symptom_refuted":
            first_line = "归因判断：原问题现象被证据反证"
        elif mode == "works_as_designed":
            first_line = "责任结论：当前行为符合设计预期"
        else:
            responsibility = str(
                result.get("responsibility") or "待人工确认"
            ).strip()
            first_line = f"建议责任方：{responsibility}"
        lines = [first_line]
        lines.append(
            "可直接参考"
            if terminal_class == SUPPORTED_ATTRIBUTION
            else MEDIUM_TIER_DISCLAIMER
        )
        lines.extend(
            [
                f"依据：{result['evidence']}",
                f"归因判断：{result['conclusion']}",
            ]
        )
        return "\n".join(_truncate_utf8(line, 1800) for line in lines)
    dispatch = _validated_upstream_dispatch(contract)
    gate_a_projection = _gate_a_projection(contract) is not None
    lines = [_dispatch_first_line(dispatch, gate_a_projection=gate_a_projection)]
    if gate_a_projection:
        # Gate A is an observation surface even when the legacy dispatch
        # envelope says abstain_no_hit.  Do not route it through the legacy
        # "未发现已知异常模式" branch, which would discard materialized
        # signal/window facts before they reach the user.
        result = build_public_rca_result(contract)
        if result.get("evidence"):
            lines.append("观测事实：" + str(result["evidence"]))
        projection = _gate_a_projection(contract)
        if isinstance(projection, Mapping) and projection.get("level") == "L0_abstain":
            # L0's fixed abstention message is both the observed boundary and
            # the short conclusion; add a distinct close instead of repeating it.
            lines.append("本次仅发布弃权事实，未输出责任归因。")
        else:
            lines.append(str(result["conclusion"]))
        return "\n".join(_truncate_utf8(line, 1800) for line in lines)
    lines.append(
        "可直接参考"
        if terminal_class == SUPPORTED_ATTRIBUTION
        else MEDIUM_TIER_DISCLAIMER
    )
    if (
        terminal_class == HONEST_NON_ATTRIBUTION
        and dispatch["reason"] in {"abstain_no_hit", "abstain_cross_domain"}
    ):
        handoff = _abstain_analysis_handoff_lines(contract)
        if not handoff:
            raise DeliveryContractError(
                "consumer_capability_required_for_abstain"
            )
        lines.extend(["自动分析未形成可派单结论。", *handoff])
        return "\n".join(_truncate_utf8(line, 1800) for line in lines)
    if dispatch["reason"] == "out_of_scope":
        pass
    elif dispatch["reason"] == "abstain_no_hit":
        lines.extend(
            [
                "未发现已知异常模式",
                "本次没有判据命中，无法提供异常时间窗。",
            ]
        )
    elif dispatch["reason"] == "abstain_cross_domain":
        lines.append("多个环节均有异常信号")
        lines.append(
            _dispatch_window_line(dispatch)
            or "命中判据未提供可复算时间窗，本次不展示可疑段。"
        )
    else:
        lines.extend(
            [
                f"依据：{result['evidence']}",
                f"归因判断：{result['conclusion']}",
            ]
        )
        window_line = _dispatch_window_line(dispatch)
        if window_line:
            lines.append(window_line)
    return "\n".join(_truncate_utf8(line, 1800) for line in lines)


_RESULT_FIELD_CLAUSE_PREFIXES = (
    "归因结论：",
    "归因判断：",
    "因果关系：",
    "关键证据：",
    "依据：",
)


def _result_field_clause(value: Any) -> str:
    text = _public_text(value, limit=1200).strip()
    changed = True
    while text and changed:
        changed = False
        for prefix in _RESULT_FIELD_CLAUSE_PREFIXES:
            if text.startswith(prefix):
                text = text[len(prefix) :].strip()
                changed = True
    return text.rstrip("。； ")


def _result_field_clause_key(value: str) -> str:
    return re.sub(
        r"[\s，。；：、,.!?！？:（）()\[\]【】'\"`]+",
        "",
        value,
    ).lower()


def _deduplicated_result_field_conclusion(result: Mapping[str, Any]) -> str:
    clauses: list[str] = []
    keys: list[str] = []
    for value in (
        result.get("conclusion"),
        result.get("causal_chain"),
        result.get("evidence"),
    ):
        clause = _result_field_clause(value)
        key = _result_field_clause_key(clause)
        if not clause or not key:
            continue
        if any(key in existing for existing in keys):
            continue
        replaced = False
        for index, existing in enumerate(keys):
            if existing and existing in key:
                clauses[index] = clause
                keys[index] = key
                replaced = True
                break
        if not replaced:
            clauses.append(clause)
            keys.append(key)
    return "；".join(clauses) or "本次未生成可确认的自动归因"


def render_public_rca_result_field(
    contract: Mapping[str, Any], *, terminal_class: str = ""
) -> str:
    """Render the versioned two-line Feishu attribution-result field.

    The full report contract remains structured.  Only this bounded projection
    folds causal and evidence text into the conclusion line; the existing issue
    and thread comment renderer remains independent.
    """

    result = build_public_rca_result(contract)
    if terminal_class == HONEST_NON_ATTRIBUTION:
        # The issue comment remains the detailed operational handoff.  The
        # attribution field must stay a bounded, non-causal projection for a
        # low-tier terminal: do not let legacy candidates, blame wording, or
        # evidence snippets turn an abstention into a responsibility claim.
        result = {
            **result,
            "conclusion": "未形成可确认的归因结论",
            "responsibility": "",
            "causal_chain": "",
            "evidence": "",
        }
    responsibility = _public_responsibility(result.get("responsibility"))
    responsibility = responsibility.rstrip("。； ") or "暂无法判断"
    responsibility_line = f"责任模块：{responsibility}"
    conclusion_prefix = "归因结论："
    available = MAX_CONCLUSION_BYTES - len(
        f"{conclusion_prefix}\n{responsibility_line}".encode("utf-8")
    )
    suffix = (
        f"；{MEDIUM_TIER_DISCLAIMER}。"
        if terminal_class == CANDIDATE_HYPOTHESIS
        else "。"
    )
    conclusion = _deduplicated_result_field_conclusion(result)
    conclusion = conclusion.replace(MEDIUM_TIER_DISCLAIMER, "").rstrip("。； ")
    conclusion = _truncate_utf8(
        conclusion,
        max(0, available - len(suffix.encode("utf-8"))),
    ).rstrip("。； ")
    return (
        f"{conclusion_prefix}{conclusion}{suffix}\n"
        f"{responsibility_line}"
    )


def _render_terminal_user_result(code: str, detail: str = "") -> str:
    heading = "本单不在自动分析范围" if code == "out_of_scope" else "本单未能定向"
    path, source, result = _TERMINAL_SELF_PROOF.get(
        code,
        _TERMINAL_SELF_PROOF["analysis_failed"],
    )
    lines = [
        heading,
        MEDIUM_TIER_DISCLAIMER,
        f"检查路径：{path}。",
        f"数据来源：{source}。",
        f"检查结果：{result}",
    ]
    return "\n".join(lines)


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise DeliveryContractError(
            "delivery_field_invalid", f"{field} must be positive"
        )
    if value <= 0:
        raise DeliveryContractError(
            "delivery_field_invalid", f"{field} must be positive"
        )
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
        raise DeliveryContractError(
            "artifact_root_invalid", f"invalid artifact_root: {raw}"
        )
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
        raise DeliveryContractError(
            "artifact_path_invalid", f"unsafe artifact path: {raw}"
        )
    absolute = posixpath.normpath(
        raw if raw.startswith("/") else posixpath.join(root, raw)
    )
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
            "delivery_manifest_artifacts_invalid",
            "manifest artifacts must be non-empty",
        )
    if len(rows) > MAX_DELIVERY_ARTIFACTS:
        raise DeliveryContractError(
            "delivery_manifest_artifacts_invalid",
            "manifest contains too many artifacts",
        )
    material: list[dict[str, Any]] = []
    total_size = 0
    for index, item in enumerate(rows):
        if not isinstance(item, Mapping):
            raise DeliveryContractError(
                "delivery_manifest_artifacts_invalid",
                f"artifact[{index}] must be an object",
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
            raise DeliveryContractError("delivery_artifact_file_too_large", path)
        total_size += size
        if total_size > MAX_DELIVERY_ARTIFACT_TOTAL_BYTES:
            raise DeliveryContractError("delivery_artifact_bundle_too_large")
        material.append({
            "role": role,
            "path": path,
            "size": size,
            "sha256": _sha256(item.get("sha256"), f"artifact[{index}].sha256"),
            "media_type": media_type,
            "required": item["required"],
        })
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
            raise DeliveryContractError("delivery_manifest_duplicate_artifact", role)
        size = item["size"]
        if size > MAX_DELIVERY_ARTIFACT_BYTES:
            raise DeliveryContractError("delivery_artifact_file_too_large", absolute)
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
        raw_path = _required_text(row.get("path"), f"stored artifact[{index}].path")
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
            raise DeliveryContractError("delivery_artifact_file_too_large", absolute)
        sha256 = _sha256(row.get("sha256"), f"stored artifact[{index}].sha256")
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
            raise DeliveryContractError("delivery_artifact_inventory_duplicate", role)
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
    if not _SAFE_KEY_RE.fullmatch(
        route_submission_key
    ) or not _ARTIFACT_SET_ID_RE.fullmatch(route_artifact_set_id):
        raise DeliveryContractError("report_url_invalid", f"unsafe report URL: {url}")
    if (submission_key is not None and route_submission_key != submission_key) or (
        artifact_set_id is not None and route_artifact_set_id != artifact_set_id
    ):
        raise DeliveryContractError(
            "report_url_identity_mismatch",
            "report URL is not bound to the sealed submission and artifact set",
        )
    for segment in parts[5:]:
        if not _FORMAL_REPORT_SEGMENT_RE.fullmatch(segment):
            raise DeliveryContractError(
                "report_url_invalid", f"unsafe report URL: {url}"
            )
        decoded = unquote(segment)
        if (
            decoded in {"", ".", ".."}
            or "/" in decoded
            or "\\" in decoded
            or "%" in decoded
            or any(ord(char) < 32 for char in decoded)
        ):
            raise DeliveryContractError(
                "report_url_invalid", f"unsafe report URL: {url}"
            )
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
    artifact_set = _required_text(artifact_set_id, "delivery_manifest.artifact_set_id")
    if not _SAFE_KEY_RE.fullmatch(submission) or not _ARTIFACT_SET_ID_RE.fullmatch(
        artifact_set
    ):
        raise DeliveryContractError("report_url_identity_invalid")
    public_origin = canonical_publication_origin()
    if not public_origin:
        raise DeliveryContractError("report_public_origin_invalid")
    return _validate_report_url(
        f"{public_origin}/G1Q3_RCA/cases/{submission}/{artifact_set}/index.html",
        submission_key=submission,
        artifact_set_id=artifact_set,
    )


def build_report_vm_path(submission_key: Any, artifact_set_id: Any) -> str:
    submission = _required_text(submission_key, "delivery_manifest.submission_key")
    artifact_set = _required_text(artifact_set_id, "delivery_manifest.artifact_set_id")
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
    if manifest.get("report_vm_path") != build_report_vm_path(
        submission_key, artifact_set_id
    ) or manifest.get("report_cifs_path") != build_report_cifs_path(
        submission_key, artifact_set_id
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
                "artifact_observation_invalid",
                f"duplicate or missing observed path: {path}",
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
    expected_manifest_path = (
        expected_path.removesuffix(".viz.mcap") + ".viz.manifest.json"
    )
    manifest_path = _required_text(
        publication.get("manifest_path"), "viz_publication.manifest_path"
    )
    if manifest_path != expected_manifest_path:
        raise DeliveryContractError("viz_publication_manifest_path_invalid")

    source_path = _required_text(
        publication.get("source_path"), "viz_publication.source_path"
    )
    source_root = canonical_artifact_root(submission_key)
    if (
        not source_path.startswith(source_root + "cases/")
        or not source_path.endswith(".viz.mcap")
        or ".." in PurePosixPath(source_path).parts
    ):
        raise DeliveryContractError("viz_publication_source_invalid")
    published_sha = _sha256(publication.get("sha256"), "viz_publication.sha256")
    if (
        _sha256(publication.get("source_sha256"), "viz_publication.source_sha256")
        != published_sha
    ):
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
            "delivery_identity_mismatch",
            "contract task_id does not match submission_key",
        )
    run_id = str(contract.get("run_id") or "").strip()
    if run_id and run_id != admission.submission_key:
        raise DeliveryContractError(
            "delivery_identity_mismatch",
            "contract run_id does not match submission_key",
        )
    if str(contract.get("work_item_id") or "").strip() != refs.work_item_id:
        raise DeliveryContractError(
            "delivery_identity_mismatch",
            "contract work_item_id does not match admission",
        )


def verify_delivery_bundle(
    *,
    admission: RcaAdmission | Mapping[str, Any],
    delivery_contract: Mapping[str, Any],
    delivery_manifest: Mapping[str, Any],
    observed_files: Sequence[Mapping[str, Any]],
    html_dependencies: Sequence[str],
    w3_execution_binding: Mapping[str, Any] | None = None,
    issue_title: Any = "",
    report_issue_focus: Mapping[str, Any] | None = None,
) -> VerifiedDelivery:
    """Verify one sealed report and build a send-free delivery effect.

    A published Foxglove surface is verified when claimed, but it is not required
    when the sealed HTML report already carries the causal result and evidence.
    """
    validated_admission = validate_rca_admission(admission)
    contract = dict(delivery_contract or {})
    if contract.get("gate_a_projection") is not None:
        contract = _sanitize_gate_a_contract(contract)
    manifest = dict(delivery_manifest or {})
    if w3_execution_binding is not None:
        if not isinstance(w3_execution_binding, Mapping):
            raise DeliveryContractError("w3_execution_snapshot_invalid")
        contract["w3_execution_snapshot"] = dict(w3_execution_binding)
        manifest["w3_execution_snapshot"] = dict(w3_execution_binding)
    if not contract:
        raise DeliveryContractError("delivery_contract_missing")
    if not manifest:
        raise DeliveryContractError("delivery_manifest_missing")
    if contract.get("schema_version") not in DELIVERY_CONTRACT_SCHEMA_VERSIONS:
        raise DeliveryContractError("delivery_contract_schema_unsupported")
    if manifest.get("schema_version") != DELIVERY_MANIFEST_SCHEMA_VERSION:
        raise DeliveryContractError("delivery_manifest_schema_unsupported")
    if manifest.get("sealed") is not True:
        raise DeliveryContractError("delivery_manifest_not_sealed")
    if manifest.get("deliverable_kind") != "html":
        raise DeliveryContractError(
            "delivery_kind_unsupported",
            "manifest must describe the sealed HTML audit bundle",
        )
    if manifest.get("dependencies_complete") is not True:
        raise DeliveryContractError(
            "delivery_dependencies_incomplete",
            "manifest must attest a complete HTML dependency inventory",
        )
    _verify_identity(validated_admission, contract, manifest)
    bound_issue_title, issue_focus_validation = validate_delivery_issue_focus(
        contract,
        issue_title,
    )
    if issue_focus_validation is not None:
        raw_focus = contract.get("issue_focus")
        if not isinstance(report_issue_focus, Mapping):
            raise DeliveryContractError("issue_focus_report_binding_missing")
        if not isinstance(raw_focus, Mapping) or dict(report_issue_focus) != dict(raw_focus):
            raise DeliveryContractError("issue_focus_report_binding_mismatch")

    if str(contract.get("business_state") or "").strip() != "report_completed":
        raise DeliveryContractError("delivery_business_state_not_ready")
    report = (
        contract.get("report") if isinstance(contract.get("report"), Mapping) else {}
    )
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
            "delivery_report_status_invalid",
            f"unsupported report status: {report_status}",
        )
    if not isinstance(report.get("requires_human_review"), bool):
        raise DeliveryContractError(
            "delivery_review_boundary_invalid",
            "RCA review metadata must be an explicit boolean",
        )

    root = _normalize_root(
        manifest.get("artifact_root"), validated_admission.submission_key
    )
    artifact_material = _manifest_artifact_material(manifest)
    expected_artifact_set_id = compute_artifact_set_id(manifest)
    _validate_manifest_v2_shape(manifest)
    if manifest.get("artifact_set_id") != expected_artifact_set_id:
        raise DeliveryContractError("artifact_set_id_mismatch")

    contract_artifacts = (
        contract.get("artifacts")
        if isinstance(contract.get("artifacts"), Mapping)
        else {}
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
        if (
            _positive_int(observed.get("size"), f"observed[{absolute}].size")
            != item["size"]
        ):
            raise DeliveryContractError("artifact_size_mismatch", absolute)
        if (
            _sha256(observed.get("sha256"), f"observed[{absolute}].sha256")
            != item["sha256"]
        ):
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
    if (
        primary_report != roles["index_html"].path
        or report_data != roles["report_data"].path
    ):
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
    report_cifs_path = canonical_viz_mcap_cifs_path(
        validated_admission.submission_key
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
    issue_url = canonical_issue_url(project_simple_name, refs.work_item_id)
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
    raw_public = (
        contract.get("public_result")
        if isinstance(contract.get("public_result"), Mapping)
        else {}
    )
    raw_public_summary = (
        raw_public.get("summary")
        if isinstance(raw_public.get("summary"), Mapping)
        else {}
    )
    raw_summary = (
        contract.get("summary") if isinstance(contract.get("summary"), Mapping) else {}
    )
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
    oracle_contract = delivery_oracle_contract(
        contract,
        issue_focus_validation,
    )
    structural_tier = evaluate_structural_tier(oracle_contract)
    conclusion_text = render_public_rca_result(
        contract, terminal_class=structural_tier.terminal_class
    )
    conclusion = _truncate_utf8(conclusion_text, MAX_CONCLUSION_BYTES)
    if not conclusion:
        raise DeliveryContractError(
            "delivery_conclusion_missing",
            "a non-empty RCA conclusion is required for the result field",
        )
    publication_tier = evaluate_structural_tier(
        oracle_contract,
        publication_text=conclusion,
    )
    try:
        require_publishable(publication_tier)
    except TierOracleConflict as exc:
        raise DeliveryContractError(
            "classification_conflict",
            ",".join(exc.result.violations)
            or f"{exc.result.terminal_class}_not_publishable",
        ) from exc
    if (
        issue_focus_validation is not None
        and not issue_focus_validation.attribution_allowed
        and publication_tier.terminal_class != HONEST_NON_ATTRIBUTION
    ):
        raise DeliveryContractError("issue_focus_attribution_forbidden")
    if (
        bound_issue_title
        and issue_focus_validation is None
        and publication_tier.terminal_class != HONEST_NON_ATTRIBUTION
    ):
        raise DeliveryContractError("issue_focus_evidence_missing")
    result_field_value = render_public_rca_result_field(
        contract,
        terminal_class=publication_tier.terminal_class,
    )
    if (
        len(result_field_value.encode("utf-8")) > MAX_CONCLUSION_BYTES
        or len(result_field_value.splitlines()) != 2
        or not result_field_value.startswith("归因结论：")
        or not result_field_value.splitlines()[1].startswith("责任模块：")
    ):
        raise DeliveryContractError("delivery_result_field_projection_invalid")
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
        "report_url": rendered_foxglove_url,
        "report_cifs_path": report_cifs_path,
        "viz_mcap_vm": viz_mcap_vm,
        "foxglove_url": rendered_foxglove_url,
        "report_link_kind": DELIVERY_REPORT_LINK_KIND,
        "report_status": report_status,
        "requires_human_review": (
            publication_tier.terminal_class == CANDIDATE_HYPOTHESIS
        ),
        "conclusion": conclusion,
        "terminal_class": publication_tier.terminal_class,
        "confidence_tier": publication_tier.confidence_tier,
        "quality_oracle": publication_tier.as_dict(),
        "quality_oracle_sha256": publication_tier.sha256(),
        "result_field_value": result_field_value,
        "field_updates": [
            {
                "field_key": RCA_RESULT_FIELD_KEY,
                "field_value": result_field_value,
            },
            {
                "field_key": RCA_REPORT_FIELD_KEY,
                "field_value": rendered_foxglove_url,
            },
        ],
    }
    if issue_focus_validation is not None:
        raw_focus = contract.get("issue_focus")
        if not isinstance(raw_focus, Mapping):
            raise DeliveryContractError("issue_focus_evidence_missing")
        semantic_payload.update(
            {
                "issue_title": bound_issue_title,
                "issue_focus_sha256": issue_focus_payload_sha256(raw_focus),
                "issue_focus_validation": issue_focus_validation.to_dict(),
            }
        )
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
        report_url=rendered_foxglove_url,
        foxglove_url=rendered_foxglove_url,
        report_cifs_path=report_cifs_path,
        issue_url=issue_url,
        terminal_class=publication_tier.terminal_class,
    )
    final_publication_tier = evaluate_structural_tier(
        oracle_contract,
        publication_text=(
            f"{conclusion}\n{result_field_value}\n{comment_content}"
        ),
    )
    try:
        require_publishable(final_publication_tier)
    except TierOracleConflict as exc:
        raise DeliveryContractError(
            "classification_conflict",
            ",".join(exc.result.violations)
            or f"{exc.result.terminal_class}_not_publishable",
        ) from exc
    if final_publication_tier.as_dict() != publication_tier.as_dict():
        raise DeliveryContractError(
            "classification_conflict",
            "quality oracle changed after final comment rendering",
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
        report_url=rendered_foxglove_url,
        viz_mcap_vm=viz_mcap_vm,
        foxglove_url=rendered_foxglove_url,
        conclusion=conclusion,
        marker=marker,
        manifest=manifest,
        contract=contract,
        artifacts=tuple(verified),
        effect_payload=effect_payload,
    )
