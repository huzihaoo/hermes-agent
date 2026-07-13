"""Feishu task delivery card rendering helpers.

Pure rendering only: no network, no filesystem, no state mutation.
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from scripts.pnc_status_projection import no_deliverable_forbidden_hits, sanitize_milestones

TASK_STATUS_TEMPLATES = {
    "host-created": "我这边先把任务建好了，还在确认有没有真正送到 VM。",
    "delivered-to-VM": "任务已经送到 VM 这边了，等 worker 接手。",
    "picked-up": "VM 已经接手开始跑了，我继续盯结果。",
    "completed": "这边已经跑完了，结论给你收好了：{conclusion}",
    "blocked": "现在卡在 {blocker}，先按这个点继续排。",
    "failed": "这条任务失败了，最直接的失败点在 {reason}。",
    "abandoned": "这条任务按当前方案放弃了，原因是 {reason}。",
}

USER_STATE_TO_TEMPLATE = {
    "created": "host-created",
    "host-created": "host-created",
    "pending": "delivered-to-VM",
    "delivered": "delivered-to-VM",
    "delivered-to-VM": "delivered-to-VM",
    "running": "picked-up",
    "in_progress": "picked-up",
    "picked-up": "picked-up",
    "awaiting_user": "blocked",
    "done": "completed",
    "completed": "completed",
    "blocked": "blocked",
    "failed": "failed",
    "abandoned": "abandoned",
}

CONFIRM_OPTION_PRESETS = {
    "default": ["确认", "调整", "取消"],
    "plan_ab": ["方案A", "方案B"],
    "continue_stop": ["继续", "中止"],
    "boundary": ["接受边界", "要求补全"],
}

FORBIDDEN_FRAGMENTS = (
    "经验证",
    "当前判定为",
    "基于证据",
    "如下所示",
    "状态同步如下",
    "现阶段结论为",
)

HTML_SOURCE_FRAGMENTS = (
    "<!doctype",
    "<html",
    "<head",
    "<style",
    "</style",
    ".header h1",
    "justify-content:",
    "background:",
)

STATUS_DISPLAY = {
    "hypothesis_ready": "已有候选归因，待人工确认",
    "needs_review": "待人工复核",
    "need_review": "待人工复核",
    "html_delivery_ready": "HTML 报告已生成",
    "report_ready": "报告已生成",
    "need_download": "待补充数据/证据",
    "out_of_scope": "不予受理/转人工",
    "not_admissible": "不予受理/转人工",
    "not_applicable": "不适用",
    "blocked": "已阻塞，需补充/确认后继续",
    "completed": "已完成",
    "done": "已完成",
    "running": "执行中",
    "in_progress": "执行中",
    "reused_existing_report": "命中既有报告，已复用",
    "existing_report_draft_not_deliverable": "命中报告草稿，但暂不可交付",
}


def _display_status(value: Any) -> str:
    text = _safe_card_text(value)
    return STATUS_DISPLAY.get(text, text)


def _clean_candidate_cause(value: Any) -> str:
    text = _safe_card_text(value)
    if not text:
        return ""
    prefixes = ("低置信假设，待人工确认：", "候选因果判断：", "候选原因：")
    changed = True
    while changed:
        changed = False
        for prefix in prefixes:
            if text.startswith(prefix):
                text = text[len(prefix):].strip()
                changed = True
    text = text.replace("。；", "；").replace("；；", "；")
    text = text.replace("建议由 ", "建议由").replace(" 继续", "继续")
    return text.strip(" ；。")


def _display_label(value: Any) -> str:
    text = _safe_card_text(value)
    if not text:
        return ""
    replacements = {
        "报告状态确认：html_delivery_ready": "报告状态确认：HTML 报告已生成",
        "归因状态确认：hypothesis_ready": "归因状态确认：已有候选归因，待人工确认",
        "report_ready": "报告已生成",
        "html_delivery_ready": "HTML 报告已生成",
        "hypothesis_ready": "已有候选归因，待人工确认",
        "ready_to_download": "数据准入通过，等待处理",
        "out_of_scope": "不予受理/转人工",
        "not_admissible": "不予受理/转人工",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    return text


def _same_artifact_location(left: str, right: str) -> bool:
    l = str(left or "").strip().rstrip("/")
    r = str(right or "").strip().rstrip("/")
    if not l or not r:
        return False
    return l == r


def _display_artifact_label(label: Any, artifact_path: str) -> str:
    text = _safe_card_text(label, "打开 HTML 报告")
    path = str(artifact_path or "").strip()
    if text == "打开 HTML 报告" and path and not path.startswith(("http://", "https://", "file://")):
        return "HTML 报告路径" if path.lower().endswith(".html") else "报告目录"
    return text


def _text(value: Any, default: str = "") -> str:
    text = str(value or "").strip()
    return text or default


def _safe_card_text(value: Any, default: str = "") -> str:
    text = _text(value, default)
    lowered = text.lower()
    if any(fragment in lowered for fragment in HTML_SOURCE_FRAGMENTS):
        return "[已隐藏疑似 HTML/CSS 源码；请通过报告链接打开]"
    if re.search(r"\bmdi\s+(?:download|refresh2?|clip|event)\b", text, re.I):
        return "历史数据地址已转换为远程读取引用（不执行 MDI 下载）"
    return text


def _safe_input_text(value: Any) -> str:
    """Never render a historical MDI command as an executable instruction."""
    text = _safe_card_text(value)
    if text == "历史数据地址已转换为远程读取引用（不执行 MDI 下载）":
        return "远程读取引用（不执行 MDI 下载）"
    return text


def _format_milestone_ts(value: Any) -> str:
    text = _text(value)
    if not text:
        return ""
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return text
    if dt.tzinfo is None:
        # Producers (relay _format_business_ts, shared-state meta) emit naive
        # timestamps in the +08:00 business timezone, NOT UTC.  Treating a naive
        # business-tz string as UTC double-added +8h (header 16:44 rendered as
        # milestone 00:44 next day).  Interpret naive as business-tz so the
        # conversion below is idempotent.
        dt = dt.replace(tzinfo=timezone(timedelta(hours=8)))
    return dt.astimezone(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")


def _template_key(task_card: dict[str, Any]) -> str:
    state = _text(task_card.get("user_state"), "running")
    return USER_STATE_TO_TEMPLATE.get(state, state if state in TASK_STATUS_TEMPLATES else "picked-up")


def render_status_line(task_card: dict[str, Any]) -> str:
    presentation = task_card.get("presentation") if isinstance(task_card.get("presentation"), dict) else {}
    projected = _text(presentation.get("status_line"))
    if projected:
        return _safe_card_text(projected)
    explicit = _text(task_card.get("status_line"))
    if explicit:
        return _safe_card_text(explicit)
    delivery = task_card.get("delivery") if isinstance(task_card.get("delivery"), dict) else {}
    key = _template_key(task_card)
    template = TASK_STATUS_TEMPLATES[key]
    return template.format(
        conclusion=_safe_card_text(delivery.get("conclusion"), "结果已收口"),
        blocker=_safe_card_text(task_card.get("blocker") or delivery.get("blocker"), "待确认点"),
        reason=_safe_card_text(task_card.get("reason") or delivery.get("reason"), "执行失败"),
    )


def assert_no_forbidden_fragments(rendered: Any, *, has_deliverable_report: bool = True) -> None:
    text = json.dumps(rendered, ensure_ascii=False, sort_keys=True)
    hits = [fragment for fragment in FORBIDDEN_FRAGMENTS if fragment in text]
    hits.extend(no_deliverable_forbidden_hits(text, has_deliverable_report=has_deliverable_report))
    if hits:
        raise ValueError(f"task card contains forbidden fragment(s): {', '.join(hits)}")


def stable_render_hash(rendered: dict[str, Any]) -> str:
    body = json.dumps(rendered, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _markdown(content: str) -> dict[str, str]:
    return {"tag": "markdown", "content": content}


def _confirm_options(item: dict[str, Any]) -> list[str]:
    raw_options = item.get("options")
    if isinstance(raw_options, list) and raw_options:
        return [_text(option) for option in raw_options if _text(option)]
    preset = _text(item.get("preset"), "default")
    return list(CONFIRM_OPTION_PRESETS.get(preset, CONFIRM_OPTION_PRESETS["default"]))


def _milestone_from_progress(progress: dict[str, Any]) -> dict[str, str] | None:
    if not isinstance(progress, dict):
        return None
    phase = _text(progress.get("phase"))
    message = _safe_card_text(progress.get("message") or progress.get("summary") or progress.get("raw"), phase)
    if not message and not phase:
        return None
    label = message if not phase else f"执行阶段：{message}"
    return {"ts": _text(progress.get("ts") or progress.get("time")), "label": label}


def _milestones_from_vm_bridge(task_card: dict[str, Any]) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    vm_bridge = task_card.get("vm_bridge") if isinstance(task_card.get("vm_bridge"), dict) else {}
    progress_item = _milestone_from_progress(vm_bridge.get("progress") if isinstance(vm_bridge, dict) else {})
    if progress_item:
        items.append(progress_item)
    recent_events = task_card.get("recent_events") if isinstance(task_card.get("recent_events"), list) else []
    for event in recent_events:
        if not isinstance(event, dict):
            continue
        phase = _text(event.get("phase"))
        summary = _safe_card_text(event.get("summary") or event.get("event") or event.get("message"), phase)
        if summary:
            items.append({"ts": _text(event.get("ts") or event.get("time")), "label": f"执行阶段：{summary}"})
    seen: set[str] = set()
    deduped: list[dict[str, str]] = []
    for item in items:
        label = _text(item.get("label"))
        if label and label not in seen:
            seen.add(label)
            deduped.append(item)
    return deduped[-8:]


def _confirm_actions(confirms: list[dict[str, Any]], *, task_id: str = "") -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    for item in confirms:
        if item.get("resolved") is not None:
            continue
        confirm_id = _text(item.get("id"), "confirm")
        for option in _confirm_options(item):
            label = _text(option)
            if not label:
                continue
            actions.append({
                "tag": "button",
                "text": {"tag": "plain_text", "content": label},
                "type": "primary" if label in {"确认", "继续", "接受边界", "方案A"} else "default",
                "value": {"hermes_action": "task_confirm", "task_id": task_id, "confirm_id": confirm_id, "choice": label},
            })
    return actions


def render_task_card(task_card: dict[str, Any]) -> dict[str, Any]:
    """Render a task_card sidecar object into Feishu interactive-card JSON."""
    if not isinstance(task_card, dict):
        task_card = {}
    status_line = render_status_line(task_card)
    task_id = _text(task_card.get("task_id"))
    presentation = task_card.get("presentation") if isinstance(task_card.get("presentation"), dict) else {}
    milestones = task_card.get("milestones") if isinstance(task_card.get("milestones"), list) else []
    if not milestones:
        milestones = _milestones_from_vm_bridge(task_card)
    milestones = sanitize_milestones(milestones, presentation)
    pending_confirms = task_card.get("pending_confirms") if isinstance(task_card.get("pending_confirms"), list) else []
    delivery = task_card.get("delivery") if isinstance(task_card.get("delivery"), dict) else {}

    elements: list[dict[str, Any]] = [_markdown(f"**状态**  {status_line}")]
    if task_id:
        elements.append(_markdown(f"追踪号：`{task_id}`"))
    if milestones:
        items = []
        for item in milestones[:8]:
            if not isinstance(item, dict):
                continue
            label = _display_label(item.get("label"))
            ts = _format_milestone_ts(item.get("ts"))
            if label:
                items.append(f"- {ts + ' · ' if ts else ''}{label}")
        if items:
            elements.append(_markdown("**里程碑**\n" + "\n".join(items)))

    open_confirms = [item for item in pending_confirms if isinstance(item, dict) and item.get("resolved") is None]
    if open_confirms:
        lines = [f"- {_text(item.get('question'), '请确认下一步')}" for item in open_confirms]
        elements.append(_markdown("**待确认**\n" + "\n".join(lines)))
        actions = _confirm_actions(open_confirms, task_id=task_id)
        if actions:
            elements.append({"tag": "action", "actions": actions[:6]})

    conclusion = _safe_card_text(delivery.get("conclusion"))
    attribution_status = _display_status(delivery.get("attribution_status"))
    report_status = _display_status(delivery.get("report_status"))
    candidate_cause = _clean_candidate_cause(delivery.get("candidate_cause"))
    attribution_causal_text = _clean_candidate_cause(delivery.get("attribution_causal_text"))
    responsibility_candidate = _safe_card_text(delivery.get("responsibility_candidate"))
    artifact_path = _safe_card_text(delivery.get("artifact_path"))
    foxglove_url = _safe_card_text(delivery.get("foxglove_url"))
    if foxglove_url.startswith(("http://", "https://")):
        artifact_path = foxglove_url
    diagnostics_for_report = task_card.get("diagnostics") if isinstance(task_card.get("diagnostics"), dict) else {}
    has_deliverable_report = (
        bool(presentation.get("has_deliverable_report"))
        or _text(delivery.get("report_status")) in {"html_delivery_ready", "report_ready", "report_generated_need_review"}
        or _text(diagnostics_for_report.get("report_status")) in {"html_delivery_ready", "report_ready", "report_generated_need_review"}
        or (artifact_path.startswith(("http://", "https://", "file://")) and artifact_path.lower().endswith(".html"))
        or foxglove_url.startswith(("http://", "https://"))
        or (not presentation and _text(task_card.get("user_state")) in {"done", "completed"} and _text(delivery.get("cifs_status")).lower() in {"success", "ok", "succeeded", "done", "ready"})
    )
    artifact_label = "打开 foxglove 可视化" if artifact_path == foxglove_url and foxglove_url else _display_artifact_label(delivery.get("artifact_label"), artifact_path)
    artifact_root = _safe_card_text(delivery.get("artifact_root"))
    input_original = _safe_input_text(delivery.get("input_original") or delivery.get("input"))
    input_resolved = _safe_input_text(delivery.get("input_resolved"))
    artifact_vm = _safe_card_text(delivery.get("artifact_vm"))
    artifact_cifs = _safe_card_text(delivery.get("artifact_cifs"))
    cifs_status = _safe_card_text(delivery.get("cifs_status"))
    verification = _safe_card_text(delivery.get("verification"))
    translate_baseline = _safe_card_text(delivery.get("translate_baseline"))
    translate_contract_path = _safe_card_text(delivery.get("translate_contract_path"))
    translate_blockers = delivery.get("translate_blockers") if isinstance(delivery.get("translate_blockers"), list) else []
    boundaries = delivery.get("boundaries") if isinstance(delivery.get("boundaries"), list) else []
    has_delivery_contract = bool(delivery)
    if has_delivery_contract and not artifact_vm:
        for candidate in (artifact_root, artifact_path):
            if candidate and candidate.startswith("/"):
                artifact_vm = candidate
                break
    if has_delivery_contract and not artifact_cifs:
        for candidate in (artifact_path, artifact_root):
            if candidate and candidate.startswith("//"):
                artifact_cifs = candidate
                break
    if has_delivery_contract and not cifs_status:
        cifs_status = "success" if has_deliverable_report and artifact_cifs and artifact_cifs != "未落地/不适用" else "未落地/不适用"
    if has_delivery_contract:
        parts = []
        if conclusion:
            parts.append(conclusion if conclusion.startswith("RCA ") else f"结论：{conclusion}")
        if attribution_status:
            parts.append(f"归因状态：{attribution_status}")
        if report_status:
            parts.append(f"报告状态：{report_status}")
        if attribution_causal_text:
            parts.append(f"归因因果：{attribution_causal_text}")
        if candidate_cause and candidate_cause != attribution_causal_text:
            parts.append(f"候选原因：{candidate_cause}")
        if responsibility_candidate:
            parts.append(f"责任候选：{responsibility_candidate}")
        missing = "未落地/不适用"
        input_original_display = input_original or missing
        input_resolved_display = input_resolved or missing
        artifact_vm_display = artifact_vm or missing
        artifact_cifs_display = artifact_cifs or missing
        cifs_status_norm = cifs_status.strip().lower()
        if cifs_status_norm in {"success", "ok", "succeeded", "done", "ready"} and has_deliverable_report:
            cifs_status_display = "成功，可从取件路径获取"
        elif cifs_status_norm in {"success", "ok", "succeeded", "done", "ready"}:
            cifs_status_display = "暂无报告；仅有中间产物目录"
        elif cifs_status_norm in {"failed", "failure", "error", "readonly_failed", "not_synced"}:
            cifs_status_display = (
                f"产物在 VM {artifact_vm_display}，本次未落 CIFS，已直接发图；"
                "需文件回复『拉取到CIFS』"
            )
        else:
            cifs_status_display = cifs_status or missing
        if input_original_display == missing and input_resolved_display == missing:
            pass
        elif input_original_display == input_resolved_display:
            parts.append(f"📥 输入：{input_resolved_display}")
        else:
            parts.append(f"📥 输入：{input_original_display} → 实际读取：{input_resolved_display}")
        if _same_artifact_location(artifact_vm_display, artifact_cifs_display):
            parts.append(f"📦 取件：{artifact_cifs_display}")
        else:
            parts.append(f"📤 产物(VM)：{artifact_vm_display}")
            parts.append(f"📦 取件(CIFS)：{artifact_cifs_display}")
        parts.append(f"CIFS 状态：{cifs_status_display}")
        if artifact_path:
            # Only true web/file URLs are clickable.  A leading "//" is a
            # CIFS/UNC file share (e.g. //hfs1.minieye.tech/...); Feishu renders
            # it as a protocol-relative HTTPS link that the file server refuses
            # ("拒绝了我们的连接请求"), so it must stay plain text.
            if artifact_path.startswith(("http://", "https://", "file://")):
                parts.append(f"产物：[{artifact_label}]({artifact_path})")
            else:
                parts.append(f"产物：{artifact_label}：{artifact_path}")
        if artifact_root and artifact_root != artifact_path:
            parts.append(f"报告目录：{artifact_root}")
        if boundaries:
            parts.append("边界/未尽事项：" + "；".join(_safe_card_text(item) for item in boundaries if _safe_card_text(item)))
        if translate_baseline:
            baseline_line = f"解析基线：{translate_baseline}"
            if translate_contract_path:
                baseline_line += f"（{translate_contract_path}）"
            parts.append(baseline_line)
        if translate_blockers:
            parts.append("解析基线阻塞：" + "；".join(_safe_card_text((item or {}).get("kind") if isinstance(item, dict) else item) for item in translate_blockers if _safe_card_text((item or {}).get("kind") if isinstance(item, dict) else item)))
        if verification:
            parts.append(f"验证：{verification}")
        elements.append(_markdown("**交付区**\n" + "\n".join(parts)))

    diagnostics = task_card.get("diagnostics") if isinstance(task_card.get("diagnostics"), dict) else {}
    if presentation:
        diagnostics = dict(diagnostics)
        if presentation.get("diagnostic_state"):
            diagnostics["shared_state"] = presentation.get("diagnostic_state")
        if presentation.get("blocker"):
            diagnostics["blocker"] = presentation.get("blocker")
    if diagnostics:
        labels = {
            "shared_state": "任务状态",
            "attribution_status": "归因状态",
            "report_status": "报告状态",
            "key_decision": "关键决策",
            "blocker": "需处理",
        }
        lines = []
        for key in ("shared_state", "attribution_status", "report_status", "key_decision", "blocker"):
            value = _display_status(diagnostics.get(key)) if key in {"shared_state", "attribution_status", "report_status", "key_decision"} else _safe_card_text(diagnostics.get(key))
            if value:
                lines.append(f"- {labels[key]}：{value}")
        if lines:
            elements.append(_markdown("**诊断**\n" + "\n".join(lines[:5])))

    card = {
        "config": {"wide_screen_mode": True},
        "header": {"title": {"tag": "plain_text", "content": "长程编码任务"}, "template": "blue"},
        "elements": elements,
    }
    assert_no_forbidden_fragments(card, has_deliverable_report=has_deliverable_report)
    return card
