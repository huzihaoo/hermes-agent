"""PNC-Agent fixed Feishu group binding policy helpers.

This module is intentionally pure and side-effect-light for the first G1Q3 RCA
slice: no live Feishu calls, no worker execution, no outbound delivery.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from typing import Literal, Sequence


G1Q3_RCA_GROUP_ID = "oc_6cfc782212009ff4cd815349909dd423"
PNC_ALL_BUSINESS_TEST_GROUP_ID = "oc_16614f4ba25b8c88b69c0b8e9ebc2fb5"
INTEGRATION_TOOLS_INTAKE_GROUP_ID = "oc_35039b74ffb63ab8100343dc32218c57"
G1Q3_RCA_MANUAL_GROUP_IDS = frozenset(
    {
        G1Q3_RCA_GROUP_ID,
        PNC_ALL_BUSINESS_TEST_GROUP_ID,
        INTEGRATION_TOOLS_INTAKE_GROUP_ID,
    }
)
G1Q3_RCA_GROUP_BINDING_ID = "gb_g1q3_rca_feishu_group"
GROUP_BINDING_RECEIPT_FILENAME_RE = re.compile(
    r"\A\d{4}-\d{2}-\d{2}-[0-9a-f]{64}\.jsonl\Z"
)
_MAX_GROUP_BINDING_RECEIPT_BYTES = 256 * 1024

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
_FEISHU_ISSUE_URL = re.compile(
    r"https?://project\.feishu\.cn/[^\s/?#)]+/issue/detail/(\d+)",
    re.IGNORECASE,
)
_MANUAL_NEGATED_INTENT = re.compile(
    r"^(?:(?:请|麻烦|帮我|帮忙)\s*)?"
    r"(?:不要|先别|无需|不用|别|暂不|暂时不要|不需要)\s*"
    r"(?:帮我|帮忙|再)?\s*(?:分析|重跑|重新分析|再跑|debug|调试|做\s*(?:g1q3\s*)?rca)",
    re.IGNORECASE,
)
_MANUAL_READ_ONLY_INTENT = re.compile(
    r"(?:不要|请勿|无需|不用|别|不)\s*(?:执行|运行|触发|创建|建单|重跑|重新分析|分析|debug|调试)"
    r"|(?:只|仅)\s*(?:查|查询|看)\s*(?:一下)?\s*(?:状态|进展|结果|报告)",
    re.IGNORECASE,
)
_MANUAL_QUERY_INTENT = re.compile(
    r"(?:报告|结果|结论)(?:链接)?\s*(?:在)?(?:哪|哪里|是什么)"
    r"|(?:分析)?任务.{0,8}(?:状态|进展).{0,8}(?:如何|怎样|怎么样|到哪|是什么|呢)"
    r"|(?:谁|由谁|是谁).{0,8}触发"
    r"|触发.{0,8}(?:谁|何人|人员)"
    r"|(?:状态|进展).{0,8}(?:如何|怎样|怎么样|到哪|呢)",
    re.IGNORECASE,
)
_MANUAL_CONDITIONAL_INTENT = re.compile(
    r"(?:能否|是否|可否|可不可以|要不要|需不需要)"
    r"|(?:可以|能|需要).{0,12}(?:吗|么|？|\?)",
    re.IGNORECASE,
)
_MANUAL_RERUN_INTENT = re.compile(
    r"(?:(?:请|麻烦|帮我|帮忙|立即|尽快|赶紧)\s*)*"
    r"(?:重跑|重新分析|再跑(?:一遍|一次)?|rerun)"
    r"(?:\s*(?:一下)?\s*(?:(?:这个|该)\s*)?(?:问题|(?:g1q3\s*)?rca))?"
    r"(?:\s*[，,:：;；。.!！]?\s*(?:辛苦(?:了|一下)?|谢谢(?:你)?|感谢))?",
    re.IGNORECASE,
)
_MANUAL_DEBUG_INTENT = re.compile(
    r"(?:(?:请|麻烦|帮我|帮忙|立即|尽快|赶紧)\s*)*(?:debug|调试)"
    r"(?:\s*(?:一下)?\s*(?:(?:这个|该)\s*)?(?:问题|(?:g1q3\s*)?rca))?"
    r"(?:\s*[，,:：;；。.!！]?\s*(?:辛苦(?:了|一下)?|谢谢(?:你)?|感谢))?",
    re.IGNORECASE,
)
_MANUAL_ANALYZE_INTENT = re.compile(
    r"(?:"
    r"(?:(?:请|麻烦|帮我|帮忙|立即|尽快|赶紧|紧急)\s*)*"
    r"分析(?:一下|下)?\s*(?:(?:这个|该)\s*)?问题"
    r"|(?:(?:请|麻烦|帮我|帮忙|立即|尽快|赶紧|紧急)\s*)*分析(?:一下|下)?"
    r"|(?:有个|有一个)?\s*紧急问题[，,:：\s]+"
    r"(?:(?:请|麻烦|帮我|帮忙|立即|尽快|赶紧)\s*)*分析(?:一下|下)?"
    r"|(?:这个|该)\s*问题(?:很|比较|非常)?\s*紧急[，,:：\s]+"
    r"(?:(?:请|麻烦|帮我|帮忙|立即|尽快|赶紧)\s*)*"
    r"分析(?:一下|下)?"
    r"|给\s*(?:(?:这个|该)\s*)?问题\s*(?:做|跑|执行)(?:一下)?\s*(?:g1q3\s*)?rca"
    r"|开始(?:做|跑)\s*(?:g1q3\s*)?rca"
    r"|开始分析(?:一下|下)?(?:\s*(?:(?:这个|该)\s*)?问题)?"
    r"|(?:做|跑|执行)(?:一下)?\s*(?:g1q3\s*)?rca"
    r"|紧急处理\s*(?:(?:这个|该)\s*)?问题"
    r")"
    r"(?:\s*[，,:：;；。.!！]?\s*(?:辛苦(?:了|一下)?|谢谢(?:你)?|感谢))?",
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
    ("rca_issue_intake", "查飞书问题状态（Kafka 自动创建）"),
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
    match = _FEISHU_ISSUE_URL.search(body)
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


@dataclass(frozen=True)
class _FeishuIssueIdentity:
    project_simple_name: str
    work_item_id: str
    canonical_url: str


def _extract_feishu_issue_identities(
    text: str,
    supplemental_urls: Sequence[str] | None = None,
) -> tuple[_FeishuIssueIdentity, ...]:
    """Return distinct canonical issue identities in encounter order."""
    values = [str(text or "")]
    if supplemental_urls is not None and not isinstance(supplemental_urls, (str, bytes)):
        values.extend(str(value or "") for value in supplemental_urls)

    identities: list[_FeishuIssueIdentity] = []
    seen: set[tuple[str, str]] = set()
    for value in values:
        for match in _FEISHU_ISSUE_URL.finditer(value):
            matched_url = match.group(0).rstrip("/")
            path = re.sub(
                r"^https?://project\.feishu\.cn/",
                "",
                matched_url,
                flags=re.IGNORECASE,
            )
            project_simple_name = path.split("/", 1)[0].strip().lower()
            work_item_id = match.group(1)
            identity_key = (project_simple_name, work_item_id)
            if not project_simple_name or identity_key in seen:
                continue
            seen.add(identity_key)
            identities.append(
                _FeishuIssueIdentity(
                    project_simple_name=project_simple_name,
                    work_item_id=work_item_id,
                    canonical_url=(
                        "https://project.feishu.cn/"
                        f"{project_simple_name}/issue/detail/{work_item_id}"
                    ),
                )
            )
    return tuple(identities)


def _extract_feishu_issue_url_work_item_id(text: str) -> str:
    """Extract only an explicit Feishu Project issue URL identity.

    This intentionally excludes bare issue numbers. Explicit links are routed
    into RCA policy on any Feishu surface; only a command-prefix request in a
    fixed group may later enter manual admission. Legacy number-only handling
    remains scoped to the two existing G1Q3 chats.
    """
    identities = _extract_feishu_issue_identities(text or "")
    return identities[0].work_item_id if len(identities) == 1 else ""


def _extract_feishu_issue_url(text: str) -> str:
    identities = _extract_feishu_issue_identities(text or "")
    return identities[0].canonical_url if len(identities) == 1 else ""


def _manual_command_scope(
    text: str,
    *,
    external_identity: bool = False,
    strip_directed_mention: bool = False,
) -> tuple[str, ...]:
    body = text or ""
    issue_match = _FEISHU_ISSUE_URL.search(body)
    if issue_match is None and not external_identity:
        return ()
    # The whole current-message text is authored command scope. Rich-card and
    # reply identities arrive separately through supplemental metadata; only
    # their identity is consumed, never their body or action words.
    scope = re.sub(
        r"https?://project\.feishu\.cn/[^\s/?#)]+/issue/detail/\d+(?:[?#][^\s]*)?",
        "",
        body,
        flags=re.IGNORECASE,
    )
    scope = re.sub(
        r"\[Mentioned:[^\]]*\]",
        "",
        scope,
        flags=re.IGNORECASE,
    )
    command_lines: list[str] = []
    directed_mention_consumed = False
    for line in scope.splitlines() or (scope,):
        command_text = line.strip()
        if strip_directed_mention and not directed_mention_consumed and command_text:
            if command_text.startswith("@"):
                command_text = re.sub(
                    r"^\s*@[^\s，,:：]+[\s，,:：]*",
                    "",
                    command_text,
                    count=1,
                ).strip()
                directed_mention_consumed = True
        command_text = command_text.strip("，,:：;；。.!！?？ ")
        if command_text:
            command_lines.append(command_text)
    return tuple(command_lines)


def _manual_command_clause(text: str) -> str:
    """Backward-compatible flattened view used by older policy callers."""
    return "，".join(_manual_command_scope(text))


def _manual_trigger_mode(
    text: str,
    *,
    external_identity: bool = False,
    strip_directed_mention: bool = False,
) -> str:
    command_lines = _manual_command_scope(
        text,
        external_identity=external_identity,
        strip_directed_mention=strip_directed_mention,
    )
    if not command_lines:
        return ""
    command_text = "，".join(command_lines)
    if (
        _MANUAL_NEGATED_INTENT.match(command_text)
        or _MANUAL_READ_ONLY_INTENT.search(command_text)
        or _MANUAL_QUERY_INTENT.search(command_text)
        or _MANUAL_CONDITIONAL_INTENT.search(command_text)
    ):
        return ""
    modes: set[str] = set()
    for candidate in (command_text, *command_lines):
        if _MANUAL_DEBUG_INTENT.fullmatch(candidate):
            modes.add("debug")
        if _MANUAL_RERUN_INTENT.fullmatch(candidate):
            modes.add("rerun")
        if _MANUAL_ANALYZE_INTENT.fullmatch(candidate):
            modes.add("run_or_join")
    return next(iter(modes)) if len(modes) == 1 else ""


def _looks_like_feishu_issue_card(text: str) -> bool:
    body = text or ""
    return bool(
        _extract_issue_work_item_id(body)
        or ("【08】问题管理" in body and any(token in body for token in ("分析这个问题", "看这个问题", "处理这个问题")))
    )


def is_g1q3_rca_bound_chat(chat_id: object) -> bool:
    """Return True for chats where G1Q3-RCA control-plane commands are valid."""
    return _normalize(chat_id) in G1Q3_RCA_MANUAL_GROUP_IDS


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


def _canonical_json_sha256(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _pnc_group_binding_receipt_identity(
    *,
    platform: object,
    chat_id: object,
    user_id: object,
    message_id: object,
    manual_authorization: dict | None = None,
    gateway_runtime_identity: dict | None = None,
) -> dict[str, str]:
    identity = {
        "chat_id": _normalize(chat_id),
        "message_id": _normalize(message_id),
        "platform": str(getattr(platform, "value", platform) or ""),
        "requester_id": _normalize(user_id),
        "schema_version": "pnc_group_binding_receipt_identity_v1",
    }
    manual_context_supplied = manual_authorization is not None
    runtime_context_supplied = gateway_runtime_identity is not None
    if manual_context_supplied != runtime_context_supplied:
        raise ValueError(
            "manual receipt identity requires authorization and runtime together"
        )
    if manual_context_supplied:
        if not isinstance(manual_authorization, dict) or not isinstance(
            gateway_runtime_identity, dict
        ):
            raise ValueError("manual receipt identity contexts must be dictionaries")
        identity.update(
            {
                "schema_version": "pnc_group_binding_receipt_identity_v2",
                "manual_authorization_sha256": _canonical_json_sha256(
                    manual_authorization
                ),
                "gateway_runtime_identity_sha256": _canonical_json_sha256(
                    gateway_runtime_identity
                ),
            }
        )
    return identity


def pnc_group_binding_receipt_filename(
    *,
    receipt_date: date,
    platform: object,
    chat_id: object,
    user_id: object,
    message_id: object,
    manual_authorization: dict | None = None,
    gateway_runtime_identity: dict | None = None,
) -> str:
    """Return the immutable receipt filename for one source-event attempt."""
    if isinstance(receipt_date, datetime) or not isinstance(receipt_date, date):
        raise ValueError("receipt_date must be a date")
    identity = _pnc_group_binding_receipt_identity(
        platform=platform,
        chat_id=chat_id,
        user_id=user_id,
        message_id=message_id,
        manual_authorization=manual_authorization,
        gateway_runtime_identity=gateway_runtime_identity,
    )
    digest = _canonical_json_sha256(identity)
    return f"{receipt_date.isoformat()}-{digest}.jsonl"


def _read_group_binding_receipt_record(
    *,
    directory_descriptor: int,
    filename: str,
) -> dict | None:
    """Read one immutable receipt through a verified owner-controlled dir fd."""
    descriptor = -1
    try:
        try:
            descriptor = os.open(
                filename,
                os.O_RDONLY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=directory_descriptor,
            )
        except FileNotFoundError:
            return
        file_info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(file_info.st_mode)
            or file_info.st_uid != os.getuid()
            or file_info.st_nlink != 1
            or stat.S_IMODE(file_info.st_mode) != 0o600
            or file_info.st_size < 1
            or file_info.st_size > _MAX_GROUP_BINDING_RECEIPT_BYTES
        ):
            raise OSError("legacy group binding receipt is not owner-controlled")
        payload = bytearray()
        while len(payload) < file_info.st_size:
            chunk = os.read(descriptor, file_info.st_size - len(payload))
            if not chunk:
                raise OSError("legacy group binding receipt was truncated")
            payload.extend(chunk)
        if os.read(descriptor, 1):
            raise OSError("legacy group binding receipt grew while reading")
        final_info = os.fstat(descriptor)
        final_path_info = os.stat(
            filename,
            dir_fd=directory_descriptor,
            follow_symlinks=False,
        )
        if (
            final_info.st_dev != file_info.st_dev
            or final_info.st_ino != file_info.st_ino
            or final_info.st_size != file_info.st_size
            or final_path_info.st_dev != file_info.st_dev
            or final_path_info.st_ino != file_info.st_ino
            or final_path_info.st_size != file_info.st_size
        ):
            raise OSError("legacy group binding receipt changed while reading")
        lines = bytes(payload).splitlines()
        if len(lines) != 1:
            raise OSError("legacy group binding receipt has invalid record count")
        try:
            record = json.loads(lines[0])
        except (TypeError, ValueError) as exc:
            raise OSError("group binding receipt is invalid JSON") from exc
        if not isinstance(record, dict):
            raise OSError("group binding receipt record must be an object")
        return record
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _manual_receipt_matches_attempt(
    *,
    record: dict,
    receipt_identity: dict[str, str],
    decision: PncGroupBindingDecision,
    platform: object,
    chat_id: object,
    user_id: object,
    message_id: object,
    manual_authorization: dict,
    gateway_runtime_identity: dict,
) -> bool:
    """Return whether an immutable v1/v2 record proves this exact attempt."""
    expected_source_fields = {
        "platform": str(getattr(platform, "value", platform) or ""),
        "group_id": _normalize(chat_id),
        "requester": _normalize(user_id),
        "message_id": _normalize(message_id),
    }
    if any(record.get(key) != value for key, value in expected_source_fields.items()):
        raise OSError("group binding receipt source identity mismatch")
    existing_identity = record.get("receipt_identity")
    if existing_identity is not None:
        if (
            existing_identity != receipt_identity
            or record.get("receipt_identity_sha256")
            != _canonical_json_sha256(receipt_identity)
        ):
            raise OSError("group binding receipt identity mismatch")
    existing_authorization = record.get("manual_authorization")
    existing_runtime = record.get("gateway_runtime_identity")
    if not isinstance(existing_authorization, dict) or not isinstance(
        existing_runtime, dict
    ):
        raise OSError("manual group binding receipt lacks attempt identity")
    return bool(
        _canonical_json_sha256(record.get("decision_snapshot"))
        == _canonical_json_sha256(asdict(decision))
        and _canonical_json_sha256(existing_authorization)
        == _canonical_json_sha256(manual_authorization)
        and _canonical_json_sha256(existing_runtime)
        == _canonical_json_sha256(gateway_runtime_identity)
    )


def pnc_group_binding_receipt_path(
    *,
    receipt_dir: str | Path,
    receipt_date: date,
    platform: object,
    chat_id: object,
    user_id: object,
    message_id: object,
    manual_authorization: dict | None = None,
    gateway_runtime_identity: dict | None = None,
) -> Path:
    """Return the canonical per-event path without touching the filesystem."""
    return Path(receipt_dir).expanduser() / pnc_group_binding_receipt_filename(
        receipt_date=receipt_date,
        platform=platform,
        chat_id=chat_id,
        user_id=user_id,
        message_id=message_id,
        manual_authorization=manual_authorization,
        gateway_runtime_identity=gateway_runtime_identity,
    )


def write_pnc_group_binding_receipt(
    *,
    receipt_dir: str | Path,
    decision: PncGroupBindingDecision,
    platform: object,
    chat_id: object,
    user_id: object,
    message_id: object,
    manual_authorization: dict | None = None,
    gateway_runtime_identity: dict | None = None,
    allow_existing_matching_attempt: bool = False,
) -> Path:
    """Create one immutable privacy-light JSONL receipt for a source event."""
    if not isinstance(allow_existing_matching_attempt, bool):
        raise ValueError("allow_existing_matching_attempt must be a boolean")
    if allow_existing_matching_attempt and (
        not isinstance(manual_authorization, dict)
        or not isinstance(gateway_runtime_identity, dict)
    ):
        raise ValueError("matching receipt reuse requires manual attempt identity")
    out_dir = Path(receipt_dir).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    resolved_dir = out_dir.resolve(strict=True)
    if resolved_dir != Path(os.path.abspath(out_dir)):
        raise OSError("group binding receipt directory must not traverse symlinks")
    directory_info = os.lstat(resolved_dir)
    if (
        not stat.S_ISDIR(directory_info.st_mode)
        or directory_info.st_uid != os.getuid()
    ):
        raise OSError("group binding receipt directory is not owner-controlled")
    now = datetime.now(timezone.utc)
    receipt_identity = _pnc_group_binding_receipt_identity(
        platform=platform,
        chat_id=chat_id,
        user_id=user_id,
        message_id=message_id,
        manual_authorization=manual_authorization,
        gateway_runtime_identity=gateway_runtime_identity,
    )
    filename = pnc_group_binding_receipt_filename(
        receipt_date=now.date(),
        platform=platform,
        chat_id=chat_id,
        user_id=user_id,
        message_id=message_id,
        manual_authorization=manual_authorization,
        gateway_runtime_identity=gateway_runtime_identity,
    )
    legacy_filename = (
        pnc_group_binding_receipt_filename(
            receipt_date=now.date(),
            platform=platform,
            chat_id=chat_id,
            user_id=user_id,
            message_id=message_id,
        )
        if isinstance(manual_authorization, dict)
        and isinstance(gateway_runtime_identity, dict)
        else None
    )
    path = resolved_dir / filename
    record = {
        "event_type": "group_binding_decision",
        "receipt_identity": receipt_identity,
        "receipt_identity_sha256": _canonical_json_sha256(receipt_identity),
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
        "manual_authorization": (
            dict(manual_authorization)
            if isinstance(manual_authorization, dict)
            else None
        ),
        "gateway_runtime_identity": (
            dict(gateway_runtime_identity)
            if isinstance(gateway_runtime_identity, dict)
            else None
        ),
    }
    payload = (
        json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n"
    ).encode("utf-8")
    directory_descriptor = os.open(
        resolved_dir,
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0),
    )
    descriptor = -1
    try:
        opened_directory = os.fstat(directory_descriptor)
        if (
            not stat.S_ISDIR(opened_directory.st_mode)
            or opened_directory.st_dev != directory_info.st_dev
            or opened_directory.st_ino != directory_info.st_ino
            or opened_directory.st_uid != os.getuid()
        ):
            raise OSError("group binding receipt directory changed during open")
        os.fchmod(directory_descriptor, 0o700)
        opened_directory = os.fstat(directory_descriptor)
        if stat.S_IMODE(opened_directory.st_mode) != 0o700:
            raise OSError("group binding receipt directory is not private")
        if legacy_filename is not None:
            legacy_record = _read_group_binding_receipt_record(
                directory_descriptor=directory_descriptor,
                filename=legacy_filename,
            )
            if legacy_record is not None and _manual_receipt_matches_attempt(
                record=legacy_record,
                receipt_identity=receipt_identity,
                decision=decision,
                platform=platform,
                chat_id=chat_id,
                user_id=user_id,
                message_id=message_id,
                manual_authorization=manual_authorization,
                gateway_runtime_identity=gateway_runtime_identity,
            ):
                if allow_existing_matching_attempt:
                    return resolved_dir / legacy_filename
                raise FileExistsError(
                    "manual receipt already exists for this attempt"
                )
        try:
            descriptor = os.open(
                filename,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=directory_descriptor,
            )
        except FileExistsError:
            if not allow_existing_matching_attempt:
                raise
            existing_record = _read_group_binding_receipt_record(
                directory_descriptor=directory_descriptor,
                filename=filename,
            )
            if existing_record is None or not _manual_receipt_matches_attempt(
                record=existing_record,
                receipt_identity=receipt_identity,
                decision=decision,
                platform=platform,
                chat_id=chat_id,
                user_id=user_id,
                message_id=message_id,
                manual_authorization=manual_authorization,
                gateway_runtime_identity=gateway_runtime_identity,
            ):
                raise OSError(
                    "existing group binding receipt does not match this attempt"
                )
            return path
        file_info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(file_info.st_mode)
            or file_info.st_uid != os.getuid()
            or file_info.st_nlink != 1
        ):
            raise OSError("group binding receipt file is not owner-controlled")
        os.fchmod(descriptor, 0o600)
        written = 0
        while written < len(payload):
            count = os.write(descriptor, payload[written:])
            if count <= 0:
                raise OSError("group binding receipt write was incomplete")
            written += count
        final_file_info = os.fstat(descriptor)
        if (
            final_file_info.st_dev != file_info.st_dev
            or final_file_info.st_ino != file_info.st_ino
            or final_file_info.st_uid != os.getuid()
            or final_file_info.st_nlink != 1
            or final_file_info.st_size != len(payload)
            or stat.S_IMODE(final_file_info.st_mode) != 0o600
        ):
            raise OSError("group binding receipt file changed during write")
        os.fsync(descriptor)
        os.fsync(directory_descriptor)
        final_directory = os.lstat(resolved_dir)
        if (
            final_directory.st_dev != opened_directory.st_dev
            or final_directory.st_ino != opened_directory.st_ino
            or final_directory.st_uid != os.getuid()
            or stat.S_IMODE(final_directory.st_mode) != 0o700
        ):
            raise OSError("group binding receipt directory changed during write")
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        os.close(directory_descriptor)
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
    issue_link_urls: Sequence[str] | None = None,
    reply_to_text: str | None = None,
    manual_mention_directed: bool | None = None,
) -> PncGroupBindingDecision:
    """Evaluate a request against G1Q3 RCA Feishu business surfaces.

    An explicit Feishu Project issue URL is read-only by default. In the three
    fixed RCA groups only, a command-prefix action can enter the durable manual
    control plane; authorization and real-mention checks happen at the gateway
    boundary. Number-only and case-only handling remains scoped below.
    """
    if not _is_feishu(platform):
        return PncGroupBindingDecision(decision="allow")
    normalized_chat_id = _normalize(chat_id)
    bound_chat = normalized_chat_id in G1Q3_RCA_MANUAL_GROUP_IDS
    current_identities = _extract_feishu_issue_identities(
        text or "",
        supplemental_urls=issue_link_urls,
    )
    reply_identities = (
        _extract_feishu_issue_identities(reply_to_text)
        if reply_to_text
        else ()
    )
    identities_by_key: dict[tuple[str, str], _FeishuIssueIdentity] = {}
    for identity in (*current_identities, *reply_identities):
        identities_by_key.setdefault(
            (identity.project_simple_name, identity.work_item_id), identity
        )
    identities = tuple(identities_by_key.values())
    identity_source = "message" if current_identities else "reply"
    if len(identities) > 1:
        return _bound_decision(
            decision="clarify",
            template_id="rca_issue_intake",
            reason="ambiguous_issue_identity",
            user_message=(
                "检测到多个不同的飞书问题单，本次不会创建、重跑或查询任务。"
                "请仅保留一个完整问题链接后重新 @ 小助手。"
            ),
            route_surface="rca_issue_identity_clarify",
            risk_gate="exactly_one_issue_identity",
        )
    issue_identity = identities[0] if identities else None
    issue_url = issue_identity.canonical_url if issue_identity else ""
    issue_url_work_item_id = issue_identity.work_item_id if issue_identity else ""
    manual_mode = (
        _manual_trigger_mode(
            text or "",
            external_identity=bool(issue_identity and not _FEISHU_ISSUE_URL.search(text or "")),
            strip_directed_mention=manual_mention_directed is True,
        )
        if bound_chat and issue_url
        else ""
    )
    if manual_mode and manual_mention_directed is not True:
        manual_mode = ""
    if manual_mode:
        specific_rejection = _specific_rejection_reason(text or "")
        if specific_rejection is not None:
            reason, message = specific_rejection
            return _reject(reason, message)
        return _bound_decision(
            decision="accepted",
            template_id="rca_issue_intake",
            reason="manual_explicit_issue_action",
            user_message="G1Q3 RCA manual request accepted for durable admission.",
            route_surface="rca_manual_intake",
            risk_gate="manual_intake_control_store",
            handoff_contract={
                "contract_version": "g1q3_rca_manual_trigger_v1",
                "case_id": "",
                "work_item_id": issue_url_work_item_id,
                "issue_url": issue_url,
                "project_simple_name": issue_identity.project_simple_name,
                "issue_identity_source": identity_source,
                "mode": manual_mode,
                "source_kind": "feishu_group_manual",
                "group_response_cap": "L1",
            },
        )
    if issue_url_work_item_id:
        return _bound_decision(
            decision="accepted",
            template_id="rca_case_status_check",
            reason="kafka_only_issue_lookup",
            user_message=(
                "当前消息仅查询已有 RCA 任务状态，不会创建或重跑。"
                "手工运行只接受固定 RCA 群内真实 @ 机器人、明确动作和完整问题链接。"
            ),
            route_surface="rca_kafka_issue_status",
            risk_gate="kafka_only_read_only",
            handoff_contract={
                "contract_version": "g1q3_rca_kafka_issue_status_v1",
                "case_id": "",
                "work_item_id": issue_url_work_item_id,
                "issue_url": issue_url,
                "project_simple_name": issue_identity.project_simple_name,
                "issue_identity_source": identity_source,
                "source_kind": "feishu_issue_url",
                "intake": "kafka_workflow_event",
                "read_only": True,
                "group_response_cap": "L1",
            },
        )
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
            reason="kafka_only_read_only_status",
            user_message="G1Q3 RCA thread follow-up accepted for read-only status lookup.",
            route_surface="rca_kafka_read_only_status",
            risk_gate="kafka_only_read_only",
            handoff_contract={
                "contract_version": "g1q3_rca_kafka_status_v1",
                "case_id": case_id,
                "work_item_id": work_item_id,
                "source_task_id": task_id,
                "read_only": True,
                "intake": "kafka_workflow_event",
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
        read_only_status = template_id in {
            "rca_case_status_check",
            "rca_case_evidence_summary",
        }
        return _bound_decision(
            decision="accepted",
            template_id=template_id,
            reason=("kafka_only_read_only_status" if read_only_status else None),
            user_message=(
                "G1Q3 RCA request accepted for read-only Kafka status lookup."
                if read_only_status
                else (
                    "Legacy chat intake will not create a task; use Kafka automatic "
                    "intake or the authorized fixed-group manual control plane."
                )
            ),
            route_surface=("rca_kafka_read_only_status" if read_only_status else template_id),
            risk_gate=("kafka_only_read_only" if read_only_status else "execution_layer"),
            handoff_contract={
                "contract_version": (
                    "g1q3_rca_kafka_status_v1"
                    if read_only_status
                    else "g1q3_rca_group_handoff_v2"
                ),
                "case_id": case_id,
                "work_item_id": work_item_id,
                **(
                    {"read_only": True, "intake": "kafka_workflow_event"}
                    if read_only_status
                    else {
                        "resource_class": "pnc_data",
                        "lane": "standard",
                        "artifact_root_policy": "/mnt/tmp/<task_id>/",
                    }
                ),
                "group_response_cap": "L1",
            },
        )
    if template_id == "rca_issue_intake":
        return _reject(
            "missing_issue_identifier",
            "已收到问题分析请求，但没有解析到飞书 issue 链接、work_item_id 或 G1Q3 case。请转发完整问题卡片或粘贴问题链接。",
        )
    return _bound_decision(decision="dry_run", template_id=template_id, reason="missing_case_id", route_surface=template_id, risk_gate="missing_identifier")
