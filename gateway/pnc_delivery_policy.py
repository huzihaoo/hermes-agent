"""PNC-Agent delivery/redaction policy helpers.

Pure helpers for deciding whether a worker/result payload may be delivered to a
bound Feishu business group.  They do not send messages and do not inspect live
Feishu, VM, or Gateway state.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


DeliveryDecisionKind = Literal["allow", "block", "defer"]
OutputLevel = Literal["L0", "L1", "L2", "L3"]


_LEVEL_RANK = {"L0": 0, "L1": 1, "L2": 2, "L3": 3}
_GROUP_SUMMARY_PROFILE = "dp_g1q3_rca_group_summary"
_OPERATOR_TOPIC_PROFILE = "dp_g1q3_rca_operator_topic"


@dataclass(frozen=True)
class PncDeliveryDecision:
    decision: DeliveryDecisionKind
    delivery_profile_ref: str
    requested_output_level: str
    max_output_level: str
    reason: str | None = None
    user_message: str | None = None


@dataclass(frozen=True)
class G1Q3RcaTaskIntent:
    template_id: str
    case_id: str
    requester: str
    source_group_id: str
    work_item_id: str = ""
    business_line_ref: str = "rca"
    project_space_ref: str = "g1q3_rca"
    delivery_profile_ref: str = _GROUP_SUMMARY_PROFILE
    output_cap: str = "L1"


@dataclass(frozen=True)
class IntentValidationDecision:
    decision: Literal["valid", "reject"]
    reason: str | None = None
    intent: G1Q3RcaTaskIntent | None = None


def _normalize(value: object) -> str:
    return str(value or "").strip()


def _contains_forbidden_detail(text: str) -> str | None:
    body = text.lower()
    if any(token in text for token in ("原始日志", "完整日志")) or "raw log" in body:
        return "raw_log"
    if any(token in text for token in ("源码 diff", "源码", "仓库路径")) or "source diff" in body:
        return "source_or_diff"
    if any(token in text for token in ("/home/", "/Users/", "/mnt/", "ssh-mini", "docker run")):
        return "operator_or_path_detail"
    return None


def evaluate_pnc_delivery(
    *,
    delivery_profile_ref: str,
    requested_output_level: str,
    content_preview: str = "",
    operator_topic_enabled: bool = False,
) -> PncDeliveryDecision:
    """Evaluate delivery safety for a PNC-Agent output surface."""
    profile = _normalize(delivery_profile_ref) or _GROUP_SUMMARY_PROFILE
    requested = _normalize(requested_output_level) or "L1"
    max_level = "L1" if profile == _GROUP_SUMMARY_PROFILE else "L3"

    forbidden = _contains_forbidden_detail(content_preview or "")
    if forbidden:
        return PncDeliveryDecision(
            decision="block",
            delivery_profile_ref=profile,
            requested_output_level=requested,
            max_output_level=max_level,
            reason=forbidden,
            user_message="默认业务群只接收 L0/L1 摘要；原始日志、源码 diff、路径和执行细节需要受控面审阅。",
        )

    if profile == _GROUP_SUMMARY_PROFILE:
        if _LEVEL_RANK.get(requested, 99) <= _LEVEL_RANK["L1"]:
            return PncDeliveryDecision(
                decision="allow",
                delivery_profile_ref=profile,
                requested_output_level=requested,
                max_output_level=max_level,
            )
        return PncDeliveryDecision(
            decision="defer",
            delivery_profile_ref=profile,
            requested_output_level=requested,
            max_output_level=max_level,
            reason="output_level_above_group_cap",
            user_message="默认业务群只允许 L0/L1；更深证据需走受控 operator topic 或 Dashboard。",
        )

    if profile == _OPERATOR_TOPIC_PROFILE and not operator_topic_enabled:
        return PncDeliveryDecision(
            decision="defer",
            delivery_profile_ref=profile,
            requested_output_level=requested,
            max_output_level=max_level,
            reason="operator_topic_disabled",
            user_message="operator topic 尚未配置真实 topic id，详细证据暂不投递。",
        )

    return PncDeliveryDecision(
        decision="allow",
        delivery_profile_ref=profile,
        requested_output_level=requested,
        max_output_level=max_level,
    )


def validate_g1q3_rca_task_intent(intent: G1Q3RcaTaskIntent) -> IntentValidationDecision:
    """Validate the minimal handoff contract before any worker is allowed."""
    if not (_normalize(intent.case_id) or _normalize(intent.work_item_id)):
        return IntentValidationDecision(decision="reject", reason="missing_issue_identifier")
    if intent.business_line_ref != "rca":
        return IntentValidationDecision(decision="reject", reason="cross_business_line")
    if intent.project_space_ref != "g1q3_rca":
        return IntentValidationDecision(decision="reject", reason="cross_project_space")
    if intent.output_cap not in {"L0", "L1"}:
        return IntentValidationDecision(decision="reject", reason="output_cap_above_group_profile")
    joined = " ".join(
        [
            intent.template_id,
            intent.case_id,
            intent.work_item_id,
            intent.requester,
            intent.source_group_id,
            intent.delivery_profile_ref,
        ]
    )
    forbidden = _contains_forbidden_detail(joined)
    if forbidden:
        return IntentValidationDecision(decision="reject", reason=f"forbidden_{forbidden}")
    if intent.template_id not in {
        "rca_case_status_check",
        "rca_case_evidence_summary",
        "rca_report_generate",
        "rca_issue_intake",
    }:
        return IntentValidationDecision(decision="reject", reason="unsupported_template")
    return IntentValidationDecision(decision="valid", intent=intent)
