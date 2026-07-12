#!/usr/bin/env python3
"""Evidence-first G1Q3 Feishu presentation projection.

Pure host-side projection only.  It does not read/write shared-state or touch VM
execution; callers pass the live evidence they already loaded.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
import re
from typing import Any, Iterable
from urllib.parse import urlsplit

REPORT_READY_STATUSES = {"html_delivery_ready", "report_ready", "report_generated_need_review"}
FAILED_STATES = {"failed", "timeout", "cancelled", "canceled", "abandoned", "error"}
PENDING_STATES = {"pending", "queued", "created", "delivered", "host-created", "delivered-to-vm"}
RUNNING_STATES = {"running", "executing", "in_progress", "picked-up", "started"}
NEED_EVIDENCE_RE = re.compile(r"need_evidence|need_source_or_evidence|need_download|requires_download|ready_to_download|missing_or_invalid_pdcl|invalid[_ -]?pdcl", re.I)
FRAME_RE = re.compile(r"invalid[_ -]?frame[_ -]?id|frame_id\s*(?:=|:)?\s*0|触发帧|关键帧", re.I)
DECLINED_RE = re.compile(r"not_admissible|out_of_scope|unauthori[sz]ed|越权|超范围|不予受理|受理未通过", re.I)
STATUS_CHECK_RE = re.compile(r"status[_ -]?check|readonly_status|read[-_ ]only status|状态查询|状态检查", re.I)
TIMEOUT_RE = re.compile(r"task_timeout|timeout|timed out|超时", re.I)
IN_FLIGHT_RE = re.compile(r"download(?:ing)?|解析|running|executing|progress|heartbeat|已派发|下载中|执行中", re.I)
PIPELINE_FIX_KINDS = {"reader_topic_mismatch", "alignment_failed", "schema_mismatch", "request_contract_drift", "translate_tool_missing"}
PIPELINE_FIX_CLASSES = {"hard_defect", "infra_self_healable"}
PIPELINE_STAGE_DEFINITIONS = {
    "s1_gate": {"stage": "gate", "label": "受理门禁核验中"},
    "gate": {"stage": "gate", "label": "受理门禁核验中"},
    "download": {"stage": "s2_download", "label": "数据下载执行中"},
    "downloading": {"stage": "s2_download", "label": "数据下载执行中"},
    "s2_download": {"stage": "s2_download", "label": "数据下载执行中"},
    "parse": {"stage": "s3_parse", "label": "数据落地中"},
    "parsing": {"stage": "s3_parse", "label": "数据落地中"},
    "s3_parse": {"stage": "s3_parse", "label": "数据落地中"},
    "s3a_materialize": {"stage": "s3_parse", "label": "数据落地中"},
    "s3b_translate": {"stage": "s3b_translate", "label": "MCAP 解析/translate 中"},
    "s5_alignment": {"stage": "s5_alignment", "label": "帧对齐中"},
    "s6_report": {"stage": "s6_report", "label": "RCA 报告生成中"},
}
PIPELINE_RUNNING_STAGES = {item["stage"] for item in PIPELINE_STAGE_DEFINITIONS.values()}
PIPELINE_STAGE_LABELS = {item["stage"]: item["label"] for item in PIPELINE_STAGE_DEFINITIONS.values()}


@dataclass(frozen=True)
class Presentation:
    lane: str
    user_state: str
    status_line: str
    conclusion: str
    diagnostic_state: str
    human_action_kind: str = "none"  # need_data / need_keyframe / need_frame / need_triage / confirm_review / none
    action_category: str = "none"  # hard / soft / none
    requires_user_input: bool = False
    missing_reason: str = ""
    report_status: str = ""
    cifs_status: str = ""
    artifact_label: str = ""
    blocker: str = ""
    milestones: list[dict[str, str]] = field(default_factory=list)
    has_deliverable_report: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple)):
        return str(value)
    return str(value).strip()


def _lower(value: Any) -> str:
    return _text(value).lower()


def _first(*values: Any, limit: int = 300) -> str:
    for value in values:
        text = _text(value)
        if text:
            return text[:limit]
    return ""


def _nested(data: dict[str, Any], *paths: str) -> str:
    for path in paths:
        cur: Any = data
        ok = True
        for part in path.split("."):
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                ok = False
                break
        if ok:
            text = _text(cur)
            if text:
                return text
    return ""


def _joined(*parts: Any) -> str:
    out: list[str] = []
    for part in parts:
        if isinstance(part, dict):
            out.extend(_text(v) for v in part.values())
        elif isinstance(part, (list, tuple)):
            out.extend(_text(v) for v in part)
        else:
            out.append(_text(part))
    return "\n".join(x for x in out if x)


def _action_category(kind: str) -> str:
    kind = _text(kind)
    if kind.startswith("need_"):
        return "hard"
    if kind == "confirm_review":
        return "soft"
    return "none"


def _surface_path(value: str) -> str:
    if value.startswith(("http://", "https://", "file://")):
        try:
            return urlsplit(value).path
        except ValueError:
            return ""
    return value.split("?", 1)[0].split("#", 1)[0]


def _delivery_surface_flags(report_truth: dict[str, Any], contract: dict[str, Any]) -> tuple[bool, bool]:
    ready = report_truth.get("report_ready_truth") if isinstance(report_truth.get("report_ready_truth"), dict) else {}
    html_candidates = (
        _nested(report_truth, "index_html_vm", "report_index_html_vm", "artifact_path"),
        _nested(ready, "index_html_vm", "report_index_html_vm", "artifact_path"),
        _nested(contract, "report_index_html_vm", "rca_status.html_link", "artifacts.index_html_vm", "artifacts.primary_report_vm"),
        _text(contract.get("artifact_path")),
    )
    foxglove_candidates = (
        _nested(report_truth, "foxglove_url", "viz_mcap_vm"),
        _nested(ready, "foxglove_url", "viz_mcap_vm"),
        _nested(contract, "foxglove_url", "viz_mcap_vm", "rca_status.foxglove_url", "artifacts.viz_mcap_vm"),
    )
    has_html = any(
        _surface_path(value).lower().endswith(".html")
        for value in html_candidates
        if value
    )
    has_foxglove = any(
        value.startswith(("http://", "https://")) or value.endswith(".viz.mcap")
        for value in foxglove_candidates
        if value
    )
    return has_html, has_foxglove


def _has_report(report_truth: dict[str, Any], contract: dict[str, Any]) -> bool:
    """Only trust an already-reconciled report truth, never bare index/status.

    The caller must pass positive, verified evidence (for example
    pnc_g1q3_truth/reconcile_report_truth + parsed report_data/gate checks, or a
    VM delivery contract whose report.is_deliverable was already validated).
    A raw html_delivery_ready token or index.html path can be a stale draft and
    must not by itself mark the lane done.
    """
    has_html, has_foxglove = _delivery_surface_flags(report_truth, contract)
    if not (has_html or has_foxglove):
        return False
    if report_truth.get("has_deliverable_report") is True or report_truth.get("real_report") is True:
        return True
    ready = report_truth.get("report_ready_truth")
    if isinstance(ready, dict) and ready:
        return True
    if ready is True:
        return True
    report = contract.get("report") if isinstance(contract.get("report"), dict) else {}
    if report.get("is_deliverable") is True and _lower(report.get("status")) in REPORT_READY_STATUSES:
        return True
    return False


def _missing_reason(contract: dict[str, Any], report_truth: dict[str, Any], log_text: str, *, kind: str = "") -> str:
    user_action = contract.get("user_action") if isinstance(contract.get("user_action"), dict) else {}
    blocker = contract.get("blocker") if isinstance(contract.get("blocker"), dict) else {}
    pipeline_blocker = report_truth.get("pipeline_blocker") if isinstance(report_truth.get("pipeline_blocker"), dict) else {}
    candidates = [
        contract.get("missing_reason"),
        user_action.get("reason"),
        user_action.get("message"),
        user_action.get("next_action_text"),
        user_action.get("next_action"),
        blocker.get("message"),
        pipeline_blocker.get("message"),
        report_truth.get("gate_skip_reason"),
        report_truth.get("skip_reason"),
        report_truth.get("blocker_message"),
    ]
    for candidate in candidates:
        text = _text(candidate)
        if text:
            return _humanize_reason(text, kind=kind)
    if kind == "frame":
        return "需补正向触发帧 frame_id（必须大于 0）或人工指定关键帧后继续 RCA。"
    if kind == "keyframe":
        return "自动找帧无候选；需人工指定关键帧或确认采集信号后继续 RCA。"
    if "pdcl" in log_text.lower() or "ready_to_download" in log_text.lower() or "need_evidence" in log_text.lower():
        return "需补充有效的问题数据地址/PDCL 下载命令或等价证据，补齐后再继续 RCA。"
    return "需补充问题数据/证据后继续 RCA。"


def _humanize_reason(text: str, *, kind: str = "") -> str:
    low = text.lower()
    if kind == "frame" or "invalid_frame_id" in low or "frame_id" in low:
        return "需补正向触发帧 frame_id（必须大于 0）或人工指定关键帧后继续 RCA。"
    if "missing_or_invalid_pdcl" in low or "invalid pdcl" in low or "pdcl" in low:
        return "需补充有效的问题数据地址/PDCL 下载命令或等价证据，补齐后再继续 RCA。"
    if "need_evidence" in low or "need_source_or_evidence" in low:
        return "需补充问题数据/证据后继续 RCA。"
    if "task_timeout" in low or "timeout" in low:
        return "执行超时，需工程侧检查后重试或转人工处理。"
    return text[:300]




def _blocker_dict(contract: dict[str, Any], report_truth: dict[str, Any]) -> dict[str, Any]:
    for data in (report_truth, contract):
        pipeline = None
        if isinstance(data, dict):
            pipeline = data.get("pipeline_result") or data.get("pipeline")
        if isinstance(pipeline, dict):
            blocker = pipeline.get("blocker")
            if isinstance(blocker, dict) and (blocker.get("kind") or blocker.get("fault_class") or blocker.get("message")):
                return blocker
        for key in ("pipeline_blocker", "blocker", "download_blocker"):
            value = data.get(key) if isinstance(data, dict) else None
            if isinstance(value, dict) and (value.get("kind") or value.get("fault_class") or value.get("message")):
                return value
    return {}


def _pipeline_fix_reason(contract: dict[str, Any], report_truth: dict[str, Any], log_all: str) -> str:
    blocker = _blocker_dict(contract, report_truth)
    kind = _lower(blocker.get("kind"))
    fault_class = _lower(blocker.get("fault_class"))
    retryable = _lower(blocker.get("retryable"))
    if fault_class in PIPELINE_FIX_CLASSES or kind in PIPELINE_FIX_KINDS:
        msg = _first(blocker.get("message"), report_truth.get("blocker_message"), report_truth.get("failure_reason"), contract.get("failure_reason"), limit=220)
        label = kind or fault_class or "pipeline_fix"
        return f"工程侧解析/对齐链路异常（{label}）" + (f"：{msg}" if msg else "；数据已就位，无需发起人补数据。")
    if kind and retryable == "true" and not NEED_EVIDENCE_RE.search(kind):
        msg = _first(blocker.get("message"), limit=220)
        return f"工程侧可重试链路异常（{kind}）" + (f"：{msg}" if msg else "；数据已就位，无需发起人补数据。")
    if "reader_topic_mismatch" in log_all or "alignment_failed" in log_all:
        return "工程侧解析/对齐链路异常；数据已就位，无需发起人补数据。"
    return ""

def _pipeline_status(data: Any) -> tuple[str, str]:
    if not isinstance(data, dict):
        return "", ""
    pipeline = data.get("pipeline_result") if isinstance(data.get("pipeline_result"), dict) else data.get("pipeline") if isinstance(data.get("pipeline"), dict) else data
    if not isinstance(pipeline, dict):
        return "", ""
    stage = _lower(pipeline.get("stage") or pipeline.get("pipeline_stage"))
    return _lower(pipeline.get("status") or pipeline.get("pipeline_status")), _normalize_pipeline_stage(stage)


def _normalize_pipeline_stage(stage: Any) -> str:
    text = _lower(stage)
    if not text:
        return ""
    definition = PIPELINE_STAGE_DEFINITIONS.get(text)
    if isinstance(definition, dict):
        return _text(definition.get("stage"))
    return text


def _pipeline_stage_label(contract: dict[str, Any], report_truth: dict[str, Any], vm_progress: dict[str, Any]) -> str:
    for data in (vm_progress, report_truth, contract):
        if not isinstance(data, dict):
            continue
        pipeline = data.get("pipeline_result") if isinstance(data.get("pipeline_result"), dict) else data.get("pipeline") if isinstance(data.get("pipeline"), dict) else data
        _status, stage = _pipeline_status(data)
        explicit_candidates = [pipeline.get("stage_label") if isinstance(pipeline, dict) else ""]
        if data is vm_progress:
            if stage in PIPELINE_STAGE_LABELS:
                explicit_candidates.extend([
                    pipeline.get("message") if isinstance(pipeline, dict) else "",
                    pipeline.get("summary") if isinstance(pipeline, dict) else "",
                ])
        explicit = _first(*explicit_candidates, limit=80)
        if explicit:
            return explicit
        if stage in PIPELINE_STAGE_LABELS:
            return PIPELINE_STAGE_LABELS[stage]
    verification = contract.get("verification") if isinstance(contract.get("verification"), dict) else {}
    stage = _normalize_pipeline_stage(verification.get("pipeline_stage") or verification.get("stage"))
    return PIPELINE_STAGE_LABELS.get(stage, "")


def _is_pipeline_running(contract: dict[str, Any], report_truth: dict[str, Any], vm_progress: dict[str, Any], log_all: str) -> bool:
    for data in (contract, report_truth, vm_progress):
        status, stage = _pipeline_status(data)
        if status in {"running", "executing", "in_progress"} and stage in PIPELINE_RUNNING_STAGES:
            return True
    verification = contract.get("verification") if isinstance(contract.get("verification"), dict) else {}
    status = _lower(verification.get("pipeline_status") or verification.get("terminal_state"))
    stage = _normalize_pipeline_stage(verification.get("pipeline_stage") or verification.get("stage"))
    if status in {"running", "awaiting_download", "executing", "in_progress"} and stage in PIPELINE_RUNNING_STAGES:
        return True
    return bool(re.search(r"pipeline_result[^\n]*(?:running|download)|stage[^\n]*download[^\n]*status[^\n]*running", log_all, re.I))


def _semantic_milestones(lane: str, *, created_at: str = "", updated_at: str = "", report_status: str = "") -> list[dict[str, str]]:
    labels: list[tuple[str, str]] = []
    if created_at:
        labels.append((created_at, "已接单，开始读取飞书问题"))
    if lane == "report_ready":
        labels.append((updated_at, "RCA 报告已生成，等待人工确认候选归因"))
    elif lane in {"need_evidence", "blocked_keyframe", "blocked_frame"}:
        labels.append((updated_at, "本轮暂停，待补充信息后继续 RCA"))
    elif lane == "not_admissible":
        labels.append((updated_at, "受理未通过，已转人工/ops 处理"))
    elif lane == "failed_timeout":
        labels.append((updated_at, "执行失败/超时，待工程侧处理"))
    elif lane == "pending":
        labels.append((updated_at or created_at, "已接单，等待执行资源接手"))
    elif lane == "status_check_done":
        labels.append((updated_at, "状态检查完成，无新增 RCA 报告"))
    elif lane == "in_progress":
        labels.append((updated_at, "任务正在执行，等待下一步证据"))
    elif lane == "pipeline_fix":
        labels.append((updated_at, "工程侧解析/对齐链路修复中，数据无需补传"))
    elif report_status:
        safe_status = "工程侧待修复" if report_status == "need_pipeline_fix" else report_status
        labels.append((updated_at, f"报告状态确认：{safe_status}"))
    seen: set[str] = set()
    out: list[dict[str, str]] = []
    for ts, label in labels:
        if label and label not in seen:
            seen.add(label)
            out.append({"ts": _text(ts), "label": label})
    return out[-8:]


def derive_presentation(
    state: str,
    contract: dict[str, Any] | None = None,
    report_truth: dict[str, Any] | None = None,
    log_text: str = "",
    vm_progress: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return the single user-facing projection for a G1Q3 task.

    Evidence ladder: declined > report > blockers/need evidence > failed >
    pending > status-check-done > in-progress > fail-closed.  ``state`` is only a
    signal and never by itself means done except for status-check tasks with
    explicit status-query evidence.
    """
    contract = contract or {}
    report_truth = report_truth or {}
    vm_progress = vm_progress or {}
    state_l = _lower(state)
    log_all = _joined(log_text, contract, report_truth, vm_progress)
    report_status = _text(report_truth.get("report_status") or report_truth.get("honest_report_status") or contract.get("report_status"))
    created_at = _first(contract.get("created_at"), report_truth.get("created_at"), vm_progress.get("created_at"), limit=80)
    updated_at = _first(contract.get("updated_at"), report_truth.get("updated_at"), vm_progress.get("updated_at"), vm_progress.get("ts"), limit=80)
    has_report = _has_report(report_truth, contract)

    declined = (
        _lower(contract.get("business_state")) in {"not_admissible", "out_of_scope", "declined", "unauthorized"}
        or _lower(report_truth.get("report_status")) in {"not_admissible", "out_of_scope", "declined"}
        or bool(DECLINED_RE.search(log_all))
    )
    if declined and not has_report:
        reason = _first(contract.get("missing_reason"), report_truth.get("blocker_message"), report_truth.get("honest_conclusion"), "受理未通过或超出当前 G1Q3-RCA 自动处理范围，已转人工/ops 处理。")
        p = Presentation(
            lane="not_admissible", user_state="blocked", status_line="受理未通过/超出范围，已转人工处理。",
            conclusion=reason, diagnostic_state="受理未通过/转人工", human_action_kind="none", action_category="none",
            requires_user_input=False, missing_reason=reason, report_status="not_admissible", cifs_status="未落地/不适用",
            artifact_label="产物目录(无报告)", blocker=reason,
            milestones=_semantic_milestones("not_admissible", created_at=created_at, updated_at=updated_at),
        )
        return p.to_dict()

    if has_report:
        partial = bool(re.search(r"partial|缺证|证据未齐|need_review|needs_review|hypothesis_ready", log_all, re.I))
        status = "RCA 报告已生成，候选归因待人工确认。" if partial else "RCA 报告已生成。"
        conclusion = _first(report_truth.get("honest_conclusion"), contract.get("conclusion"), "RCA 报告已生成；请按报告边界复核候选归因。")
        has_html_surface, has_foxglove_surface = _delivery_surface_flags(report_truth, contract)
        surface_status = "report_ready" if has_foxglove_surface else "html_delivery_ready"
        artifact_label = "打开 foxglove 可视化" if has_foxglove_surface else "打开 HTML 报告"
        p = Presentation(
            lane="report_ready", user_state="done", status_line=status, conclusion=conclusion,
            diagnostic_state="报告已生成", human_action_kind="confirm_review" if partial else "none", action_category="soft" if partial else "none",
            requires_user_input=False, missing_reason="请确认候选归因与证据边界。" if partial else "",
            report_status=surface_status, cifs_status="success", artifact_label=artifact_label,
            blocker="请确认候选归因（软确认，非阻塞）" if partial else "无", has_deliverable_report=True,
            milestones=_semantic_milestones("report_ready", created_at=created_at, updated_at=updated_at, report_status=surface_status),
        )
        return p.to_dict()

    pipeline_fix_reason = _pipeline_fix_reason(contract, report_truth, log_all)
    if pipeline_fix_reason:
        p = Presentation(
            lane="pipeline_fix", user_state="in_progress", status_line=pipeline_fix_reason,
            conclusion=pipeline_fix_reason, diagnostic_state="工程侧修复中", human_action_kind="none", action_category="none",
            requires_user_input=False, missing_reason="", report_status="need_pipeline_fix",
            cifs_status="暂无报告；数据已下载，工程侧修复解析/对齐链路后继续 RCA", artifact_label="产物目录(暂无报告)",
            blocker=pipeline_fix_reason,
            milestones=_semantic_milestones("pipeline_fix", created_at=created_at, updated_at=updated_at, report_status="need_pipeline_fix"),
        )
        return p.to_dict()

    keyframe = _lower(contract.get("report_status")) == "need_keyframe" or _lower(report_truth.get("report_status")) == "need_keyframe" or "blocked_need_keyframe" in _lower(contract.get("business_state"))
    frame = bool(FRAME_RE.search(log_all))
    if keyframe or frame:
        kind = "frame" if frame else "keyframe"
        reason = _missing_reason(contract, report_truth, log_all, kind=kind)
        action_kind = "need_frame" if frame else "need_keyframe"
        p = Presentation(
            lane="blocked_frame" if frame else "blocked_keyframe", user_state="awaiting_user",
            status_line=reason, conclusion=reason, diagnostic_state="已阻塞，需补充/确认后继续",
            human_action_kind=action_kind, action_category="hard", requires_user_input=True, missing_reason=reason,
            report_status="need_keyframe", cifs_status="暂无报告；需补充信息后生成 RCA 报告", artifact_label="产物目录(暂无报告)", blocker=reason,
            milestones=_semantic_milestones("blocked_frame" if frame else "blocked_keyframe", created_at=created_at, updated_at=updated_at),
        )
        return p.to_dict()

    status_query = bool(STATUS_CHECK_RE.search(log_all)) or _lower(contract.get("mode")) in {"status_check", "execution_only_readonly_status_query"}

    if _is_pipeline_running(contract, report_truth, vm_progress, log_all):
        stage_label = _pipeline_stage_label(contract, report_truth, vm_progress)
        status_line = "数据下载执行中（pipeline running）；等待解析完成后更新 RCA 状态。"
        if stage_label:
            status_line = f"{stage_label}（pipeline running）；当前尚无可交付 RCA 报告。"
        p = Presentation(
            lane="in_progress", user_state="in_progress", status_line=status_line,
            conclusion=status_line, diagnostic_state="执行中",
            human_action_kind="none", action_category="none", requires_user_input=False, report_status="need_download",
            cifs_status="未落地/不适用", artifact_label="产物目录(暂无报告)", blocker="无",
            milestones=_semantic_milestones("in_progress", created_at=created_at, updated_at=updated_at),
        )
        return p.to_dict()

    need_evidence = (
        _lower(contract.get("business_state")) in {"missing_user_input", "data_required", "intake_validated", "blocked_need_evidence"}
        or _lower(report_truth.get("report_status")) in {"need_download", "need_user_data", "need_evidence"}
        or _lower(report_truth.get("honest_report_status")) in {"need_download", "need_user_data", "need_evidence"}
        or bool(NEED_EVIDENCE_RE.search(log_all))
        or (state_l == "blocked" and not has_report)
    )
    if need_evidence:
        reason = _missing_reason(contract, report_truth, log_all)
        p = Presentation(
            lane="need_evidence", user_state="in_progress", status_line=f"已阻塞，需补齐数据/证据：{reason}",
            conclusion=f"未生成 RCA 报告；{reason}待补齐后继续下载/解析与 RCA。", diagnostic_state="已阻塞，需补充/确认后继续",
            human_action_kind="need_data", action_category="hard", requires_user_input=True, missing_reason=reason,
            report_status="need_user_data", cifs_status="暂无报告；需补充数据/证据后生成 RCA 报告", artifact_label="产物目录(暂无报告)", blocker=reason,
            milestones=_semantic_milestones("need_evidence", created_at=created_at, updated_at=updated_at),
        )
        return p.to_dict()

    failed = state_l in FAILED_STATES or bool(TIMEOUT_RE.search(log_all))
    if failed:
        reason = _first(report_truth.get("failure_reason"), contract.get("failure_reason"), contract.get("reason"), "执行失败/超时，需工程侧检查后重试或转人工处理。")
        reason = _humanize_reason(reason)
        p = Presentation(
            lane="failed_timeout", user_state="failed", status_line=f"执行失败/超时：{reason}", conclusion=f"本次未生成可交付 RCA 报告；{reason}",
            diagnostic_state="执行失败/超时", human_action_kind="none", action_category="none", requires_user_input=False, missing_reason="",
            report_status="failed", cifs_status="未落地/不适用", artifact_label="产物目录(无报告)", blocker=reason,
            milestones=_semantic_milestones("failed_timeout", created_at=created_at, updated_at=updated_at),
        )
        return p.to_dict()

    if state_l in PENDING_STATES:
        p = Presentation(
            lane="pending", user_state="pending", status_line="已接单，排队/等待执行资源接手。", conclusion="任务已创建，尚未开始真实执行；暂无 RCA 报告。",
            diagnostic_state="排队等待执行", report_status="pending", cifs_status="未落地/不适用", artifact_label="产物目录(暂无报告)",
            milestones=_semantic_milestones("pending", created_at=created_at, updated_at=updated_at),
        )
        return p.to_dict()

    if state_l in {"completed", "done"} and status_query:
        p = Presentation(
            lane="status_check_done", user_state="done", status_line="状态检查完成，当前无新增可交付 RCA 报告。",
            conclusion="状态查询已完成；未发现本轮新增可交付 RCA 报告。", diagnostic_state="状态检查完成（无新增报告）",
            report_status="status_check_done", cifs_status="未落地/不适用", artifact_label="产物目录(无新增报告)",
            milestones=_semantic_milestones("status_check_done", created_at=created_at, updated_at=updated_at),
        )
        return p.to_dict()

    # Fail closed for structured pipeline progress: a bare
    # {"status": "running"} without a recognized stage must not be upgraded to
    # a user-visible pipeline lane merely because "running" appears in the
    # joined evidence blob.  Free-text logs can still signal generic in-flight
    # work, while structured pipeline objects must pass _is_pipeline_running().
    in_flight = state_l in RUNNING_STATES or bool(IN_FLIGHT_RE.search(_text(log_text)))
    if in_flight:
        p = Presentation(
            lane="in_progress", user_state="in_progress", status_line="已受理，正在执行；等待下一步证据或报告产物。",
            conclusion="任务正在执行中；尚无可交付 RCA 报告。", diagnostic_state="执行中",
            report_status="in_progress", cifs_status="未落地/不适用", artifact_label="产物目录(暂无报告)",
            milestones=_semantic_milestones("in_progress", created_at=created_at, updated_at=updated_at),
        )
        return p.to_dict()

    p = Presentation(
        lane="confirming", user_state="running", status_line="已受理，状态确认中；尚无可交付 RCA 报告。",
        conclusion="状态确认中；尚无可交付 RCA 报告。", diagnostic_state="状态确认中",
        report_status="unknown", cifs_status="未落地/不适用", artifact_label="产物目录(暂无报告)",
        milestones=_semantic_milestones("confirming", created_at=created_at, updated_at=updated_at),
    )
    return p.to_dict()


def no_deliverable_forbidden_hits(rendered: Any, *, has_deliverable_report: bool = False) -> list[str]:
    """Forbidden fragments.  The old optimistic download phrase is banned in every lane."""
    text = json.dumps(rendered, ensure_ascii=False, sort_keys=True) if not isinstance(rendered, str) else rendered
    hits = ["正在自动下载/解析"] if "正在自动下载/解析" in text else []
    if has_deliverable_report:
        return hits
    forbidden = ("完成后出 RCA 结论", "成功，可从取件路径获取", "RCA 报告已生成")
    hits.extend(frag for frag in forbidden if frag in text)
    return hits


def sanitize_milestones(milestones: Iterable[Any], projection: dict[str, Any] | None = None) -> list[dict[str, str]]:
    """Semantic whitelist + report-ready no-regression milestone hygiene."""
    projection = projection or {}
    lane = _text(projection.get("lane"))
    has_report = bool(projection.get("has_deliverable_report")) or lane == "report_ready"
    banned = re.compile(r"No VM status summary yet|<task_id>|\brunning\b|gate=|need_download|need_evidence|raw", re.I)
    regress = re.compile(r"执行中|running|等待自动下载|待补|need_input|need_download|准入校验", re.I)
    allowed = re.compile(r"已接单|飞书问题已读取|报告已生成|人工确认|本轮暂停|补充|失败|超时|排队|等待执行|任务状态更新|状态检查完成|受理未通过|状态确认|执行阶段|工程侧|无需补传", re.I)
    by_label: dict[str, dict[str, str]] = {}
    for item in milestones:
        if not isinstance(item, dict):
            continue
        label = _text(item.get("label"))[:120]
        if not label or banned.search(label):
            continue
        if has_report and regress.search(label):
            continue
        # A card that later loses its deliverable report (e.g. report_ready →
        # need_download re-classification) must not keep a stale positive
        # milestone like "RCA 报告已生成…".  Reuse the render-time guard as the
        # single source of truth so the sanitizer can never emit a milestone the
        # render-time assert_no_forbidden_fragments would then reject and crash on.
        if not has_report and no_deliverable_forbidden_hits(label, has_deliverable_report=False):
            continue
        if not allowed.search(label):
            continue
        by_label[label] = {"ts": _text(item.get("ts")), "label": label}
    out = list(by_label.values())
    out.sort(key=lambda x: x.get("ts") or "9999")
    return out[-8:]
