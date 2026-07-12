"""PNC-Agent fixed Feishu group binding policy helpers.

This module is intentionally pure and side-effect-light for the first G1Q3 RCA
slice: no live Feishu calls, no worker execution, no outbound delivery.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Literal


G1Q3_RCA_GROUP_ID = "oc_6cfc782212009ff4cd815349909dd423"
PNC_ALL_BUSINESS_TEST_GROUP_ID = "oc_16614f4ba25b8c88b69c0b8e9ebc2fb5"
G1Q3_RCA_GROUP_BINDING_ID = "gb_g1q3_rca_feishu_group"

_GIT_INTENT = re.compile(
    r"(git\s+(clone|pull|push|fetch|checkout|commit|merge|rebase|reset|branch|stash)|"
    r"切(到|换)?\s*[^\s，。]*\s*分支|新建分支|提交\s*(代码|改动|commit|pr|mr)|拉取?\s*代码)",
    re.IGNORECASE,
)
_REPO_READ_INTENT = re.compile(
    r"((读取|访问|打开|拉取|克隆|clone|浏览)\s*[^\n]{0,12}(仓库|源码|repo)|任意仓库|任意源码)",
    re.IGNORECASE,
)
_SHELL_INTENT = re.compile(
    r"((执行|跑|运行)\s*(一下|这条|下面|如下)?\s*(命令|shell|脚本|bash)|跑这个命令|执行一下)",
    re.IGNORECASE,
)
_SELF_REF_FOLLOWUP = re.compile(
    r"(结论|报告|进展|状态|做到哪|哪一步|卡在|卡哪|为什么|为啥|跑完|跑过|怎么样了|同步|这个不就是|哪里不满足|失败)",
    re.IGNORECASE,
)


def _is_self_referential_followup(text: str) -> bool:
    body = text or ""
    if not _SELF_REF_FOLLOWUP.search(body):
        return False
    if re.search(r"(生成|出|做)\s*(一版|一下|个)?\s*(rca\s*)?(报告|report)", body, re.IGNORECASE):
        return False
    # True privileged imperatives are still rejected by the safety gates.
    return not (
        _GIT_INTENT.search(body)
        or _REPO_READ_INTENT.search(body)
        or _SHELL_INTENT.search(body)
    )


DecisionKind = Literal["allow", "reject", "dry_run", "accepted", "clarify"]


@dataclass(frozen=True)
class PncGroupBindingDecision:
    decision: DecisionKind
    group_binding_id: str | None = None
    business_line_ref: str | None = None
    project_space_ref: str | None = None
    template_id: str | None = None
    output_cap: str | None = None
    reason: str | None = None
    user_message: str | None = None
    handoff_contract: dict | None = None
    gray_delivery_phase: str = "g1q3_rca_business_delivery_gray"
    route_surface: str | None = None
    risk_gate: str | None = None
    # (choice_id, label) rows for a decision="clarify" button card. Transport
    # (Feishu card) is rendered by the gateway; this stays platform-agnostic.
    clarify_options: tuple[tuple[str, str], ...] | None = None


def _normalize(value: object) -> str:
    return str(value or "").strip()


def _is_feishu(platform: object) -> bool:
    value = getattr(platform, "value", platform)
    return str(value or "").strip().lower() == "feishu"


def _classify_g1q3_rca_template(text: str) -> str | None:
    body = text.lower()
    # Gray delivery principle: accept business natural language at the entrance;
    # risk is enforced later by execution/output gates.
    if any(token in body for token in ("证据", "evidence", "缺什么", "缺少", "缺失", "缺啥", "还缺", "补什么")):
        return "rca_case_evidence_summary"
    if any(token in text for token in ("报告在哪", "报告在哪里", "报告链接", "链接在哪", "产物在哪", "结论是什么", "结论呢", "跑过没", "跑过了吗")):
        return "rca_case_status_check"
    if _extract_issue_work_item_id(text) and any(token in body for token in ("再看", "看下", "查下", "查一下", "状态", "进展", "结论", "报告", "跑过")):
        return "rca_case_status_check"
    # Natural Feishu issue-card analysis should win over field labels inside the
    # forwarded card (for example the card itself contains "状态"), otherwise
    # "分析这个问题 [【08】问题管理] ... 状态 OPEN" is misrouted as a plain
    # status check instead of issue intake.  Evidence/missing-field follow-ups
    # are checked first so "飞书问题 7013527412 缺少什么" does not become a new
    # generic intake.
    if _looks_like_feishu_issue_card(text) or (_mentions_g1q3_case(text) and any(
        token in text
        for token in (
            "分析这个问题",
            "分析下这个问题",
            "看这个问题",
            "看下这个问题",
            "处理这个问题",
            "问题管理",
            "缺陷描述",
            "缺陷",
        )
    )):
        return "rca_issue_intake"
    if any(token in body for token in ("做到哪一步", "进展", "状态", "status", "再看下", "看下", "查一下", "查下")):
        return "rca_case_status_check"
    if any(token in body for token in ("生成报告", "生成一版", "生成 rca", "rca report")):
        return "rca_report_generate"
    if "报告" in body or "摘要" in body:
        return "rca_case_status_check"
    return None


def _bound_decision(
    *,
    decision: DecisionKind,
    template_id: str | None = None,
    reason: str | None = None,
    user_message: str | None = None,
    handoff_contract: dict | None = None,
    route_surface: str | None = None,
    risk_gate: str | None = None,
    clarify_options: tuple[tuple[str, str], ...] | None = None,
) -> PncGroupBindingDecision:
    return PncGroupBindingDecision(
        decision=decision,
        group_binding_id=G1Q3_RCA_GROUP_BINDING_ID,
        business_line_ref="rca",
        project_space_ref="g1q3_rca",
        template_id=template_id,
        output_cap="L1",
        reason=reason,
        user_message=user_message,
        handoff_contract=handoff_contract,
        route_surface=route_surface,
        risk_gate=risk_gate,
        clarify_options=clarify_options,
    )


def _reject(reason: str, message: str) -> PncGroupBindingDecision:
    return _bound_decision(decision="reject", reason=reason, user_message=message, risk_gate=reason)


# Conservative clarify (owner-approved): a template-less message in the bound
# G1Q3 group becomes a button-clarify instead of a blunt reject; pure chit-chat
# never reaches here (already allow'd upstream in the all-business test group).
_RCA_CLARIFY_OPTIONS: tuple[tuple[str, str], ...] = (
    ("rca_case_status_check", "查 case 状态"),
    ("rca_issue_intake", "提交问题分析"),
    ("rca_case_evidence_summary", "证据 / 缺项查询"),
)


def _clarify(reason: str, message: str) -> PncGroupBindingDecision:
    return _bound_decision(
        decision="clarify", reason=reason, user_message=message,
        risk_gate=reason, clarify_options=_RCA_CLARIFY_OPTIONS,
    )


def _mentions_g1q3_case(text: str) -> bool:
    return bool(re.search(r"(?<![A-Za-z0-9])G1Q3[-_ ]?[A-Za-z0-9]+", text, re.IGNORECASE))


def _extract_issue_work_item_id(text: str) -> str:
    body = text or ""
    match = re.search(r"project\.feishu\.cn/[^\s)]+/issue/detail/(\d+)", body, re.IGNORECASE)
    if match:
        return match.group(1)
    # Follow-up messages in the bound Feishu group often say only
    # "飞书问题 7013527412 缺少什么" instead of forwarding the full issue URL.
    # Treat that as an issue-scoped RCA request rather than falling through to
    # the generic unsupported-template rejection.
    match = re.search(r"(?:飞书问题|问题|issue|work[_ -]?item)\s*[:：#]?\s*(\d{6,})", body, re.IGNORECASE)
    if match:
        return match.group(1)
    if any(token in body for token in ("再看", "看下", "查下", "查一下", "缺", "结论", "报告", "跑过")):
        match = re.search(r"(?<!\d)(\d{9,})(?!\d)", body)
        if match:
            return match.group(1)
    return ""


def _looks_like_feishu_issue_card(text: str) -> bool:
    body = text or ""
    return bool(
        _extract_issue_work_item_id(body)
        or ("【08】问题管理" in body and any(token in body for token in ("分析这个问题", "看这个问题", "处理这个问题")))
    )


def is_g1q3_rca_bound_chat(chat_id: object) -> bool:
    """Return True for chats where G1Q3-RCA control-plane commands are valid."""
    return _normalize(chat_id) in {G1Q3_RCA_GROUP_ID, PNC_ALL_BUSINESS_TEST_GROUP_ID}


def _is_all_business_test_group(chat_id: object) -> bool:
    return _normalize(chat_id) == PNC_ALL_BUSINESS_TEST_GROUP_ID


def _looks_like_g1q3_rca_request(text: str) -> bool:
    """Gate G1Q3 on the shared test group without hijacking other businesses."""
    body = text or ""
    lower = body.lower()
    if _mentions_g1q3_case(body) or _looks_like_feishu_issue_card(body):
        return True
    if "rca" in lower or "g1q3" in lower:
        return True
    if re.search(r"(?:飞书问题|问题|issue|work[_ -]?item)\s*[:：#]?\s*\d{6,}", body, re.IGNORECASE):
        return True
    return False


def _specific_rejection_reason(text: str) -> tuple[str, str] | None:
    body = text.lower()
    if any(token in text for token in ("评测门禁", "集成发版")):
        return ("cross_business_line", "这个群里的 PNC-Agent 只处理 G1Q3 RCA；跨业务线请求请到对应绑定入口发起。")
    if any(token in body for token in ("g1q4", "g2q1", "别的项目", "其他项目", "另一个项目")):
        return ("cross_project_space", "这个群只绑定 g1q3_rca，不能处理其他项目空间。")
    is_self_followup = _is_self_referential_followup(text)
    if is_self_followup:
        return None
    if _REPO_READ_INTENT.search(text):
        return ("arbitrary_repo_read", "这个群不开放任意仓库或源码读取。")
    if _SHELL_INTENT.search(text):
        return ("arbitrary_shell", "这个群不开放任意 shell/命令执行。")
    if _GIT_INTENT.search(text):
        return ("arbitrary_git", "这个群不开放任意 git 操作。")
    if any(token in text for token in ("原始日志", "完整日志", "raw log")):
        return ("output_level_too_high", "业务群只允许 L0/L1 摘要，原始日志不能直接群发。")
    if any(token in text for token in ("源码 diff", "source diff", "diff")):
        return ("source_related_output", "源码 diff 不能直接进入默认业务群输出面。")
    if "operator topic" in body or "详细证据" in text:
        return ("delivery_surface_not_enabled", "operator topic 尚未启用，不能投递详细证据。")
    if ("报告" in text or "rca" in body) and not (_mentions_g1q3_case(text) or _extract_issue_work_item_id(text)):
        return ("missing_required_input", "缺少 case_id 或等价问题编号，不能生成 RCA 报告。")
    return None


def write_pnc_group_binding_receipt(
    *,
    receipt_dir: str | Path,
    decision: PncGroupBindingDecision,
    platform: object,
    chat_id: object,
    user_id: object,
    message_id: object,
) -> Path:
    """Append a privacy-light JSONL receipt for a PNC group-binding decision."""
    out_dir = Path(receipt_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    path = out_dir / f"{now.date().isoformat()}.jsonl"
    record = {
        "event_type": "group_binding_decision",
        "timestamp": now.isoformat(),
        "platform": str(getattr(platform, "value", platform) or ""),
        "group_binding_id": decision.group_binding_id,
        "group_id": _normalize(chat_id),
        "business_line_ref": decision.business_line_ref,
        "project_space_ref": decision.project_space_ref,
        "template_id": decision.template_id,
        "decision": decision.decision,
        "reason": decision.reason,
        "output_cap": decision.output_cap,
        "gray_delivery_phase": decision.gray_delivery_phase,
        "route_surface": decision.route_surface,
        "risk_gate": decision.risk_gate,
        "requester": _normalize(user_id),
        "message_id": _normalize(message_id),
        "decision_snapshot": asdict(decision),
    }
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    return path


def write_pnc_group_handoff_receipt(
    *,
    receipt_dir: str | Path,
    decision: PncGroupBindingDecision,
    submit_result: dict,
    platform: object,
    chat_id: object,
    user_id: object,
    message_id: object,
) -> Path:
    """Append a privacy-light JSONL receipt for shared-state handoff submission."""
    out_dir = Path(receipt_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    path = out_dir / f"{now.date().isoformat()}.jsonl"
    task = submit_result.get("task") if isinstance(submit_result, dict) else {}
    if not isinstance(task, dict):
        task = {}
    notify = submit_result.get("notify_process") if isinstance(submit_result, dict) else {}
    if not isinstance(notify, dict):
        notify = {}
    handoff = decision.handoff_contract if isinstance(decision.handoff_contract, dict) else {}
    record = {
        "event_type": "handoff_submission",
        "timestamp": now.isoformat(),
        "platform": str(getattr(platform, "value", platform) or ""),
        "group_binding_id": decision.group_binding_id,
        "group_id": _normalize(chat_id),
        "business_line_ref": decision.business_line_ref,
        "project_space_ref": decision.project_space_ref,
        "template_id": decision.template_id,
        "output_cap": decision.output_cap,
        "gray_delivery_phase": decision.gray_delivery_phase,
        "route_surface": decision.route_surface,
        "risk_gate": decision.risk_gate,
        "contract_version": _normalize(handoff.get("contract_version")),
        "case_id": _normalize(handoff.get("case_id")),
        "work_item_id": _normalize(handoff.get("work_item_id")),
        "requester": _normalize(user_id),
        "message_id": _normalize(message_id),
        "handoff_success": bool(isinstance(submit_result, dict) and submit_result.get("success")),
        "task_id": _normalize(task.get("task_id") or task.get("id")),
        "handoff_error": _normalize(submit_result.get("error") if isinstance(submit_result, dict) else ""),
        "completion_probe_started": bool(notify.get("started")),
    }
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    return path


def evaluate_pnc_group_request(
    *,
    platform: object,
    chat_id: object,
    text: str,
    active_thread_case: dict | None = None,
) -> PncGroupBindingDecision:
    """Evaluate a request against G1Q3 RCA Feishu business surfaces.

    The dedicated G1Q3-RCA group remains strictly bound to this business.  The
    shared PNC test group can exercise all business lines, so it enters the
    G1Q3 policy only when the message carries a clear G1Q3/RCA/issue signal;
    unrelated business prompts are allowed to continue to their own adapters.
    """
    if not _is_feishu(platform):
        return PncGroupBindingDecision(decision="allow")
    normalized_chat_id = _normalize(chat_id)
    if normalized_chat_id == G1Q3_RCA_GROUP_ID:
        pass
    elif _is_all_business_test_group(normalized_chat_id):
        if not _looks_like_g1q3_rca_request(text or ""):
            return PncGroupBindingDecision(decision="allow")
    else:
        return PncGroupBindingDecision(decision="allow")

    specific_rejection = _specific_rejection_reason(text or "")
    if specific_rejection is not None:
        reason, message = specific_rejection
        return _reject(reason, message)

    template_id = _classify_g1q3_rca_template(text or "")
    if template_id is None and _is_self_referential_followup(text or ""):
        template_id = "rca_case_status_check"
    active_case = active_thread_case if isinstance(active_thread_case, dict) else None
    has_explicit_identifier = bool(_mentions_g1q3_case(text or "") or _extract_issue_work_item_id(text or ""))
    if active_case and not has_explicit_identifier and (
        _is_self_referential_followup(text or "")
        or template_id in {"rca_case_status_check", "rca_case_evidence_summary"}
    ):
        work_item_id = str(active_case.get("work_item_id") or "").strip()
        case_id = str(active_case.get("case_id") or "").strip()
        task_id = str(active_case.get("task_id") or active_case.get("id") or "").strip()
        return _bound_decision(
            decision="accepted",
            template_id="rca_case_status_check",
            reason=None,
            user_message="G1Q3 RCA thread follow-up accepted for governed shared-state status handoff.",
            route_surface="rca_case_status_check",
            risk_gate="execution_layer",
            handoff_contract={
                "contract_version": "g1q3_rca_group_handoff_v2",
                "case_id": case_id,
                "work_item_id": work_item_id,
                "source_task_id": task_id,
                "resource_class": "pnc_data",
                "lane": "standard",
                "artifact_root_policy": "/mnt/tmp/<task_id>/",
                "group_response_cap": "L1",
            },
        )
    if template_id is None:
        return _clarify(
            "unsupported_task_template",
            "这个群里的 PNC-Agent 处理 G1Q3 RCA 标准任务。你想做哪一类？点一下，我告诉你需要补什么（也可以直接打字补上 G1Q3 case）：",
        )

    case_match = re.search(r"(?<![A-Za-z0-9])G1Q3[-_ ]?([A-Za-z0-9]+)", text or "", re.IGNORECASE)
    case_id = case_match.group(1) if case_match else ""
    work_item_id = _extract_issue_work_item_id(text or "")
    if case_id or work_item_id:
        if template_id not in {"rca_case_status_check", "rca_issue_intake", "rca_case_evidence_summary"}:
            return _bound_decision(
                decision="dry_run",
                template_id=template_id,
                reason="template_not_enabled_for_real_handoff",
                user_message="这个模板已识别，但第一版真实交付只开放状态查询、问题 intake 和缺项查询；报告生成仍保持 dry-run。",
                route_surface="report_generation_deferred",
                risk_gate="template_not_enabled_for_real_handoff",
            )
        return _bound_decision(
            decision="accepted",
            template_id=template_id,
            reason=None,
            user_message="G1Q3 RCA intake request accepted for governed shared-state handoff.",
            route_surface=template_id,
            risk_gate="execution_layer",
            handoff_contract={
                "contract_version": "g1q3_rca_group_handoff_v2",
                "case_id": case_id,
                "work_item_id": work_item_id,
                "resource_class": "pnc_data",
                "lane": "standard",
                "artifact_root_policy": "/mnt/tmp/<task_id>/",
                "group_response_cap": "L1",
            },
        )
    if template_id == "rca_issue_intake":
        return _reject(
            "missing_issue_identifier",
            "已收到问题分析请求，但没有解析到飞书 issue 链接、work_item_id 或 G1Q3 case。请转发完整问题卡片或粘贴问题链接。",
        )
    return _bound_decision(decision="dry_run", template_id=template_id, reason="missing_case_id", route_surface=template_id, risk_gate="missing_identifier")
