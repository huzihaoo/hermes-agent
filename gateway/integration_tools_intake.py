"""Shared field extraction for integration_tools intake.

Single source of truth for "what fields did the user already give us".  Both the
intake triage in ``gateway/run.py`` (``_classify_mdrive4_intake_request``) and the
intake clarification dimension generator (``gateway/feishu_interaction_policy.py``)
consume this so that:

1. triage no longer short-circuits to ``general`` just because the literal token
   ``mdrive4`` is absent (root cause R1, see design
   ``integration-tools-intake-triage-fix-20260618``);
2. the clarification card never re-asks for something the user already provided
   (``skip_if`` on extracted fields — root cause P3 in the clarification design).

Intentionally pure/deterministic: no network, no filesystem, no LLM.  Extraction is
conservative — when in doubt it leaves a field None (so the agent may ask once)
rather than guessing wrong (which would wrongly suppress a needed question).
"""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass

# --- signal vocabularies (kept narrow & deterministic) ----------------------

_CLEAN_TERMS = ("清洗", "mcap-clean", "mcapclean", "clean后", "clean路径", "clean-only")
_TRANSLATE_TERMS = (
    "转成foxglove", "foxglove可用", "mcap-translate", "mcaptranslate",
    "转换foxglove", "可用格式", "转成可用",
)
_DIAGNOSTIC_TERMS = (
    "topic缺失", "缺失topic", "planningtopic", "planning topic", "topicmissing",
    "utc_tick", "utctick", "dnptick", "dnp tick", "tick流转", "诊断", "排查",
    "检查", "解析检查", "没有topic", "打不开",
)
_ARTIFACT_TERMS = ("产物", "drs", "artifact", "拉取", "issue_ref")
_BUILD_TERMS = (
    "decision_and_planning", "decisionandplanning", "编译", "build", "cmake",
    "gflags", "find_package", "快速上手", "开发", "configure",
)
_ISSUE_LIST_TERMS = ("飞书问题", "问题清单", "问题整理", "项目规划组", "问题列表")

# any of these means the message carries a real integration_tools signal, so triage
# must NOT short-circuit to general even without the literal "mdrive4".
_FOXGLOVE_TERMS = ("foxglove", "run_planning_visualization", "planning可视化")
_TOOL_DOMAIN_TERMS = (
    "mcap", "logsim", "回放", "replay", "pnc_specs", "mdrive4", "mdrive4-cli",
)

_MCAP_PATH_RE = re.compile(r"(/mnt/[^\s，。；;、]+\.mcap)")
_GENERIC_ABS_MCAP_RE = re.compile(r"([~/][^\s，。；;、]*\.mcap)")
_OWNER_FIELD_RE = re.compile(r"owner\s*[:：]\s*(\S+)", re.IGNORECASE)
_BRANCH_RE = re.compile(
    r"(?:分支|branch|commit)\s*[:：]?\s*([A-Za-z0-9._/\-]{3,})", re.IGNORECASE
)
_OUTPUT_REQ_TERMS = (
    "输出", "产物文件", "报告", "结论", "脚本", "图", "可复跑", "html",
)
_TOPIC_TICK_TERMS = ("topic", "tick", "utc_tick", "utctick", "dnp", "planning")


@dataclass(frozen=True)
class IntakeFields:
    """Structured view of fields the user already supplied.

    None means "not provided / not confidently extractable" — the agent may ask.
    A non-None value means "user already gave this" — do not re-ask (skip_if).
    """

    mcap_path: str | None = None
    owner: str | None = None
    project: str | None = None
    action: str | None = None  # clean | translate | diagnostic | artifact_pull | build | issue_list
    branch: str | None = None
    output_req: str | None = None
    topic_or_tick: str | None = None

    def signals_any(self) -> bool:
        """True when the message carries at least one integration_tools signal.

        Used by triage to decide whether it may continue classifying instead of
        short-circuiting to ``general``.  ``owner`` alone (e.g. a bare originator id)
        is intentionally NOT a signal — otherwise every message would qualify.
        """
        return any(
            value is not None
            for value in (
                self.mcap_path,
                self.action,
                self.project,
                self.branch,
                self.topic_or_tick,
            )
        )

    def as_dict(self) -> dict:
        return asdict(self)


def _first(*values: str | None) -> str | None:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return None


def extract_intake_fields(
    text: str,
    *,
    originator: str | None = None,
) -> IntakeFields:
    """Extract already-provided fields from a raw integration_tools message.

    Deterministic and conservative.  ``originator`` (the Feishu sender) seeds
    ``owner`` because the originator is the default acceptor for these flows.
    """
    raw = str(text or "")
    compact = raw.lower().replace(" ", "")
    originator = str(originator or "").strip()

    # --- mcap path ---
    mcap_match = _MCAP_PATH_RE.search(raw) or _GENERIC_ABS_MCAP_RE.search(raw)
    mcap_path = mcap_match.group(1).rstrip("。；;，,") if mcap_match else None

    # --- owner ---
    owner_field = _OWNER_FIELD_RE.search(raw)
    if owner_field:
        owner = owner_field.group(1).strip()
    elif "胡子豪" in raw:
        owner = "胡子豪"
    elif originator:
        owner = originator
    else:
        owner = None

    # --- project ---
    project = "mdrive4" if ("mdrive4" in compact or "项目" in compact) else None

    # --- action (most specific wins) ---
    wants_clean = any(k in compact for k in _CLEAN_TERMS)
    wants_translate = any(k in compact for k in _TRANSLATE_TERMS)
    wants_diagnostic = any(k in compact for k in _DIAGNOSTIC_TERMS) and any(
        k in compact for k in ("mcap", "topic", "tick", "dnp", "foxglove")
    )
    wants_artifact = any(k in compact for k in _ARTIFACT_TERMS)
    wants_build = any(k in compact for k in _BUILD_TERMS)
    wants_issue_list = any(k in compact for k in _ISSUE_LIST_TERMS)
    if wants_translate:
        action = "translate"
    elif wants_clean:
        action = "clean"
    elif wants_diagnostic:
        action = "diagnostic"
    elif wants_issue_list:
        action = "issue_list"
    elif wants_artifact:
        action = "artifact_pull"
    elif wants_build:
        action = "build"
    else:
        action = None

    # --- branch / commit ---
    branch_match = _BRANCH_RE.search(raw)
    branch = branch_match.group(1) if branch_match else None

    # --- output requirement (only when explicitly mentioned) ---
    output_req = None
    for term in _OUTPUT_REQ_TERMS:
        if term in compact:
            output_req = term
            break

    # --- topic / tick phenomenon for diagnostics ---
    topic_or_tick = None
    if any(k in compact for k in _TOPIC_TICK_TERMS):
        topic_or_tick = _first(
            *(term for term in _TOPIC_TICK_TERMS if term in compact)
        )

    # foxglove / logsim alone is enough of a domain signal to seed project so
    # signals_any() returns True even without a literal "mdrive4".
    if project is None and (
        any(k in compact for k in _FOXGLOVE_TERMS)
        or any(k in compact for k in _TOOL_DOMAIN_TERMS)
    ):
        project = "integration_tools"

    return IntakeFields(
        mcap_path=mcap_path,
        owner=owner,
        project=project,
        action=action,
        branch=branch,
        output_req=output_req,
        topic_or_tick=topic_or_tick,
    )


# --- v2 triage classifier --------------------------------------------------
#
# Same output shape as run.py ``_classify_mdrive4_intake_request`` (kind / status /
# missing_fields / reply_hint / auto_dispatch / close_loop_policy) plus a new
# ``extracted_fields`` key.  The behavioural change vs v1 is ONLY:
#   - it does not short-circuit to ``general`` when ``mdrive4`` is absent but other
#     signals exist (R1);
#   - every branch carries ``extracted_fields`` and derives ``missing_fields`` from
#     them instead of a hardcoded list (P3).
# Pure: callers pass the close-loop policy dict and the announcement/question
# predicates so this module stays free of run.py imports.

_ANNOUNCEMENT_MARKERS = ("参考这个文档", "上手的时候可以参考", "from_copylink规控同学", "文档哈")


def _looks_like_clarification_question(raw: str, compact: str) -> bool:
    text = str(raw or "").strip()
    if not text:
        return False
    has_question_shape = (
        text.endswith(("?", "？"))
        or re.search(r"能.+吗", text) is not None
        or any(k in compact for k in ("是不是", "有没有", "是什么", "看得到吗"))
        or "嘛" in text
    )
    if not has_question_shape:
        return False
    execution_terms = (
        "清洗", "转换", "转成", "排查", "诊断", "检查", "作图", "画图", "返回",
        "跑一下", "执行", "处理", "mcap-clean", "mcapclean", "mcap-translate",
        "mcaptranslate", "/mnt/", ".mcap",
    )
    if any(k in compact for k in execution_terms):
        return False
    domain_terms = ("mdrive4", "mdrive4-cli", "mdrive4cli", "mcap", "foxglove", "pnc_specs", "工具", "仓库")
    return any(k in compact for k in domain_terms)


def _missing_for_diagnostic(fields: IntakeFields) -> list[str]:
    missing: list[str] = []
    if not fields.mcap_path:
        missing.append("mcap/转换产物绝对路径")
    if not fields.topic_or_tick:
        missing.append("期望检查的 topic 名或 tick 现象")
    return missing


def classify_integration_tools_intake_v2(
    text: str,
    *,
    originator: str | None = None,
    close_loop_policy: dict | None = None,
) -> dict:
    """Field-first triage. Mirrors v1 output, adds ``extracted_fields``, no R1 short-circuit."""
    raw = str(text or "")
    compact = raw.lower().replace(" ", "")
    fields = extract_intake_fields(raw, originator=originator)
    policy = close_loop_policy or {}
    base = {
        "kind": "general",
        "status": "intake_checked",
        "missing_fields": [],
        "reply_hint": "",
        "auto_dispatch": None,
        "close_loop_policy": policy,
        "extracted_fields": fields.as_dict(),
    }

    if any(k in compact for k in _ANNOUNCEMENT_MARKERS):
        return {**base, "kind": "announcement_reference", "status": "closed",
                "reply_hint": "识别为群内文档/公告分享，不创建执行任务。"}

    if _looks_like_clarification_question(raw, compact):
        return {**base, "kind": "question", "status": "closed",
                "reply_hint": "这是澄清问句：我会按群内答疑直接回答，不创建执行任务、不进入 intake/超时闭环。",
                "direct_reply": "是，mdrive4 相关问题我会按仓库里的 mdrive4-cli / 受治理工具口径答疑；这条是澄清问句，不创建执行任务、不进入 intake/超时闭环。"}

    # ported v1 special case: parser verification pending ("待小助手...验证下")
    if any(k in compact for k in ("待小助手可以用的时候验证", "待小助手", "验证下")) and any(
        k in compact for k in ("mcap", "pnc_specs", "解析")
    ):
        return {**base, "kind": "parser_verification_pending", "status": "need_input",
                "missing_fields": ["是否现在执行验证", "输入路径是否可读", "期望验证命令/输出"],
                "reply_hint": "需要确认是否现在执行验证、输入路径是否可读、期望验证命令/输出。"}

    # R1 fix: only fall through to general when there is NO signal at all.
    if not fields.signals_any():
        return base

    if fields.action == "diagnostic":
        missing = _missing_for_diagnostic(fields)
        return {**base, "kind": "mcap_diagnostic_request",
                "status": "need_input" if missing else "intake_checked",
                "missing_fields": missing,
                "reply_hint": (
                    "识别为 mcap/topic/tick 诊断类请求；已保留原始路径/动作，将按具体 topic/tick 现象继续收口。"
                    if not missing else
                    "识别为 mcap/topic/tick 诊断类请求；请只补齐缺失的路径、topic/tick 现象或验收人，不需要重复已提供的路径和动作。"
                )}

    if fields.action in ("clean", "translate") and fields.mcap_path and fields.owner and fields.project:
        cli = "mcap-translate" if fields.action == "translate" else "mcap-clean"
        return {**base, "kind": f"{cli}_execution", "status": "intake_checked",
                "reply_hint": f"识别为 {cli} 执行请求，输入/owner/project 已具备；将自动派发受治理 fixed-CLI。",
                "auto_dispatch": {"cli": cli, "input": fields.mcap_path, "project": "mdrive4"}}

    if fields.action in ("clean", "translate"):
        # signal present but inputs incomplete -> ask only for what's missing
        missing = []
        if not fields.mcap_path:
            missing.append("mcap 绝对路径")
        if not fields.owner:
            missing.append("验收人/owner")
        return {**base, "kind": "mcap_execution_incomplete", "status": "need_input",
                "missing_fields": missing,
                "reply_hint": "识别为 mcap 清洗/转换请求；只需补齐缺失项，已提供的路径/动作不再重复询问。"}

    if fields.action == "artifact_pull":
        return {**base, "kind": "mdrive4_artifact_pull", "status": "need_input",
                "missing_fields": ["issue_ref/DRS ticket/clip ref", "software_version/build", "产物类型", "用途", "是否已有 PDCL/DRS/MDI 授权"],
                "reply_hint": "需要补充 issue_ref/DRS ticket、版本、产物类型、用途和授权情况；未授权时只能先做离线 plan。"}

    if fields.action == "issue_list":
        return {**base, "kind": "mdrive4_feishu_issue_list", "status": "need_input",
                "missing_fields": ["飞书项目/问题清单链接或项目 key", "筛选范围", "期望输出字段", "排序方式"],
                "reply_hint": "需要先补充飞书项目/问题清单链接、筛选范围、输出字段和排序方式。"}

    if fields.action == "build":
        missing = ["目标分支/commit", "编译目标范围", "运行环境", "期望输出", "是否允许 VM 实际编译"]
        if fields.branch:
            missing = [m for m in missing if m != "目标分支/commit"]
        return {**base, "kind": "mdrive4_decision_and_planning_build", "status": "need_input",
                "missing_fields": missing,
                "reply_hint": "这是代码/模块类任务，需要补充分支、编译目标、环境、输出形式和是否允许 VM 编译；命令必须先按 VM 事实源核对。"}

    # signal present (e.g. bare mcap path / domain term) but no specific action
    has_concrete_input = bool(fields.mcap_path or fields.topic_or_tick or fields.branch)
    if not has_concrete_input:
        # only a bare project/domain mention with no concrete input -> benign,
        # same as v1 ``general`` (do not over-trigger need_input on a mere name).
        return base
    return {**base, "kind": "integration_tools_underspecified", "status": "need_input",
            "missing_fields": ["目标动作（清洗/转换/诊断/拉取产物等）"],
            "reply_hint": "识别为 integration_tools 相关请求，但未明确目标动作；请补充要做什么，已提供的输入不再重复询问。"}
