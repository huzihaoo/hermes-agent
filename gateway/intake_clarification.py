"""Intake-stage structured multi-dimension clarification.

When a user's request at the Feishu entry point is ambiguous (input uncertain) or
its boundary is uncertain, instead of free-text back-and-forth we ask a Claude
AskUserQuestion-style **structured multi-dimension choice**: 1..N question
dimensions, each a single-select button row (2-4 options), with text fallback and
originator @-mention.

Design: ``feishu-intake-clarification-multidim-choice-20260618``.

This module has three layers:
  * dimension generator: pure function (message + triage extracted_fields) -> dims;
    ``skip_if`` suppresses any dimension whose field the user already provided (P3).
  * sidecar store: per-request JSON under ``admission-clarification/<request_id>.json``
    (independent of shared-state ``tasks/`` to keep the governance boundary clean).
  * resolver: mirrors ``feishu_task_confirm.resolve_task_confirm`` — atomic, locked,
    idempotent; button clicks and text replies share one write path.

Everything is gated by INTAKE_CLARIFY_ENABLED at the call sites; this module is
import-safe and side-effect free until ``resolve_*`` / ``ensure_*`` is called.
"""
from __future__ import annotations

import fcntl
import json
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from hermes_cli.config import get_hermes_home

# --- dimension library (declarative; mirror config/intake-clarification-dimensions.v1.yaml)
#
# ask_if / skip_if receive the triage ``extracted_fields`` dict (see
# gateway.integration_tools_intake.IntakeFields.as_dict()) and the triage ``kind``.
# A dimension is emitted iff ask_if(...) and not skip_if(...).

MAX_DIMENSIONS = 4  # aligns with AskUserQuestion; overflow -> text fallback


@dataclass(frozen=True)
class Dimension:
    id: str
    question: str
    options: list[str]
    preset: str = ""
    priority: int = 50  # lower = asked first when trimming to MAX_DIMENSIONS
    ask_if: Callable[[dict, str], bool] = lambda f, k: True
    skip_if: Callable[[dict, str], bool] = lambda f, k: False


def _f(fields: dict, key: str) -> Any:
    return (fields or {}).get(key)


DIMENSION_LIBRARY: list[Dimension] = [
    Dimension(
        id="intent",
        question="这是执行任务还是知识问答？",
        options=["执行任务", "知识问答"],
        priority=10,
        # ask when triage could not pin a concrete action
        ask_if=lambda f, k: k in {"general", "integration_tools_underspecified", "", None},
        skip_if=lambda f, k: bool(_f(f, "action")),
    ),
    Dimension(
        id="boundary",
        question="这次按什么边界来做？",
        options=["标准受治理", "快速原型"],
        preset="boundary",
        priority=20,
        # boundary cannot be inferred from fields -> ask whenever we're clarifying
        ask_if=lambda f, k: True,
        skip_if=lambda f, k: False,
    ),
    Dimension(
        id="output",
        question="你要什么形式的产出？",
        options=["结论即可", "要产物文件", "要可复跑脚本"],
        priority=40,
        ask_if=lambda f, k: True,
        skip_if=lambda f, k: bool(_f(f, "output_req")),
    ),
    Dimension(
        id="target_path",
        question="处理哪个输入？",
        options=["按我给的路径", "我再补路径"],
        priority=30,
        # only relevant for path-driven actions
        ask_if=lambda f, k: _f(f, "action") in {"clean", "translate", "diagnostic"},
        skip_if=lambda f, k: bool(_f(f, "mcap_path")),
    ),
    Dimension(
        id="owner",
        question="谁是验收人？",
        options=["我自己", "指定他人"],
        priority=50,
        ask_if=lambda f, k: True,
        skip_if=lambda f, k: bool(_f(f, "owner")),
    ),
]


@dataclass
class ClarificationSpec:
    request_id: str
    originator_open_id: str = ""
    chat_id: str = ""
    thread_id: str = ""
    raw_text: str = ""
    dimensions: list[dict[str, Any]] = field(default_factory=list)
    truncated: list[str] = field(default_factory=list)  # dim ids dropped past MAX

    def open_dimensions(self) -> list[dict[str, Any]]:
        return [d for d in self.dimensions if d.get("resolved") is None]

    def all_resolved(self) -> bool:
        return bool(self.dimensions) and all(d.get("resolved") is not None for d in self.dimensions)

    def as_dict(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "originator_open_id": self.originator_open_id,
            "chat_id": self.chat_id,
            "thread_id": self.thread_id,
            "raw_text": self.raw_text,
            "dimensions": self.dimensions,
            "truncated": self.truncated,
        }

    @classmethod
    def from_dict(cls, body: dict[str, Any]) -> "ClarificationSpec":
        return cls(
            request_id=str(body.get("request_id") or ""),
            originator_open_id=str(body.get("originator_open_id") or ""),
            chat_id=str(body.get("chat_id") or ""),
            thread_id=str(body.get("thread_id") or ""),
            raw_text=str(body.get("raw_text") or ""),
            dimensions=list(body.get("dimensions") or []),
            truncated=list(body.get("truncated") or []),
        )


def build_dimensions(extracted_fields: dict | None, triage_kind: str | None) -> tuple[list[dict[str, Any]], list[str]]:
    """Return (dimensions, truncated_ids).

    A dimension is emitted iff ask_if and not skip_if. Result is sorted by priority
    and trimmed to MAX_DIMENSIONS; trimmed dimension ids are returned separately so
    callers can route them to text fallback (never silently dropped).
    """
    fields = extracted_fields or {}
    kind = triage_kind or ""
    selected = [
        d for d in DIMENSION_LIBRARY
        if d.ask_if(fields, kind) and not d.skip_if(fields, kind)
    ]
    selected.sort(key=lambda d: d.priority)
    kept = selected[:MAX_DIMENSIONS]
    truncated = [d.id for d in selected[MAX_DIMENSIONS:]]
    dims = [
        {"id": d.id, "question": d.question, "options": list(d.options),
         "preset": d.preset, "resolved": None}
        for d in kept
    ]
    return dims, truncated


def needs_clarification(
    *,
    request_id: str,
    raw_text: str,
    extracted_fields: dict | None,
    triage_kind: str | None,
    originator_open_id: str = "",
    chat_id: str = "",
    thread_id: str = "",
    triage_status: str | None = None,
    triage_has_auto_dispatch: bool = False,
    triage_missing: list | None = None,
) -> ClarificationSpec | None:
    """Build a ClarificationSpec, or None when no dimension needs asking.

    Returning None means "information is sufficient / not ambiguous" -> caller
    proceeds on the normal path (zero extra friction).

    Pre-gate (added after real-traffic dry-run showed over-triggering): if triage
    already decided the request can proceed directly — it has an auto_dispatch, or
    it's intake_checked with no missing_fields, or it's already closed (question/
    announcement) — we do NOT pop a clarification card. Clarification is for
    ambiguous / under-specified requests only, not a tax on well-formed ones.
    """
    status = str(triage_status or "").strip()
    missing = list(triage_missing or [])
    if triage_has_auto_dispatch:
        return None
    if status == "closed":
        return None
    if status == "intake_checked" and not missing:
        return None

    dims, truncated = build_dimensions(extracted_fields, triage_kind)
    if not dims:
        return None
    return ClarificationSpec(
        request_id=str(request_id or "").strip(),
        originator_open_id=str(originator_open_id or "").strip(),
        chat_id=str(chat_id or "").strip(),
        thread_id=str(thread_id or "").strip(),
        raw_text=str(raw_text or ""),
        dimensions=dims,
        truncated=truncated,
    )


# --- sidecar store ----------------------------------------------------------

def _safe_request_id(request_id: str) -> str:
    rid = str(request_id or "").strip()
    # keep it filesystem-safe; mirror safe_task_id conservatism
    return "".join(c if (c.isalnum() or c in "-_.") else "_" for c in rid) or "unknown"


def clarification_dir() -> Path:
    return get_hermes_home() / "runtime" / "admission-clarification"


def sidecar_path(request_id: str) -> Path:
    return clarification_dir() / f"{_safe_request_id(request_id)}.json"


def _now_iso() -> str:
    return datetime.now(timezone(timedelta(hours=8))).isoformat()


@contextmanager
def _clarify_lock(request_id: str):
    lock_dir = get_hermes_home() / "locks" / "intake-clarification"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / f"{_safe_request_id(request_id)}.lock"
    with lock_path.open("w", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _atomic_write(path: Path, body: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(body, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _load(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8")) or {}
    except (ValueError, OSError):
        return {}


def save_spec(spec: ClarificationSpec) -> Path:
    path = sidecar_path(spec.request_id)
    with _clarify_lock(spec.request_id):
        body = _load(path)
        body.update(spec.as_dict())
        body.setdefault("created_at", _now_iso())
        body["updated_at"] = _now_iso()
        _atomic_write(path, body)
    return path


def load_spec(request_id: str) -> ClarificationSpec | None:
    body = _load(sidecar_path(request_id))
    if not body:
        return None
    return ClarificationSpec.from_dict(body)


def summarize_clarified_choices(request_id: str) -> dict[str, Any] | None:
    """Return the resolved choices once every dimension is answered, else None.

    Pure read: maps dimension_id -> chosen option, plus the original intake
    context (chat/thread/originator/raw_text) so a continuation can create the
    task with the clarified intent. None until all_resolved.
    """
    spec = load_spec(request_id)
    if spec is None or not spec.all_resolved():
        return None
    choices = {
        str(d.get("id")): str((d.get("resolved") or {}).get("choice") or "")
        for d in spec.dimensions
    }
    return {
        "request_id": spec.request_id,
        "chat_id": spec.chat_id,
        "thread_id": spec.thread_id,
        "originator_open_id": spec.originator_open_id,
        "raw_text": spec.raw_text,
        "choices": choices,
        # convenience flags derived from common dimensions
        "is_execution": choices.get("intent") == "执行任务",
        "is_qa": choices.get("intent") == "知识问答",
        "boundary": choices.get("boundary", ""),
    }


def _parse_iso(ts: str) -> datetime | None:
    try:
        return datetime.fromisoformat(str(ts))
    except (TypeError, ValueError):
        return None


def find_stale_clarifications(*, now_iso: str, threshold_minutes: int = 30) -> list[dict[str, Any]]:
    """Return specs with open dimensions older than threshold and not yet nudged.

    Pure scan over the clarification dir (now injected for testability). Caller is
    responsible for actually sending the @-originator nudge (see build_stale_nudge_text)
    and then calling mark_nudged. fail-closed: unparseable timestamps are skipped.
    """
    now = _parse_iso(now_iso)
    if now is None:
        return []
    out: list[dict[str, Any]] = []
    d = clarification_dir()
    if not d.exists():
        return []
    for path in sorted(d.glob("*.json")):
        body = _load(path)
        if not body or body.get("all_resolved") or body.get("nudged"):
            continue
        dims = body.get("dimensions") if isinstance(body.get("dimensions"), list) else []
        if not any(isinstance(x, dict) and x.get("resolved") is None for x in dims):
            continue
        created = _parse_iso(str(body.get("created_at") or body.get("updated_at") or ""))
        if created is None:
            continue
        age_min = (now - created).total_seconds() / 60.0
        if age_min >= threshold_minutes:
            out.append({"request_id": str(body.get("request_id") or path.stem),
                        "age_minutes": round(age_min, 1)})
    return out


def mark_nudged(request_id: str) -> bool:
    """Mark a clarification as nudged once (idempotent; True if THIS call set it)."""
    with _clarify_lock(request_id):
        path = sidecar_path(request_id)
        body = _load(path)
        if not body or body.get("nudged"):
            return False
        body["nudged"] = True
        body["nudged_at"] = _now_iso()
        _atomic_write(path, body)
        return True


def mark_continued(request_id: str) -> bool:
    """Idempotency guard for the continuation: set continued=true once.

    Returns True if THIS call set it (caller should proceed), False if it was
    already set (another click already triggered the continuation).
    """
    with _clarify_lock(request_id):
        path = sidecar_path(request_id)
        body = _load(path)
        if not body or body.get("continued"):
            return False
        body["continued"] = True
        body["continued_at"] = _now_iso()
        _atomic_write(path, body)
        return True



# --- resolver (mirror feishu_task_confirm.resolve_task_confirm) --------------

def _option_aliases(option: str) -> set[str]:
    base = {option, option.lower()}
    extra = {
        "执行任务": {"执行", "执行任务", "1", "跑", "去做"},
        "知识问答": {"问答", "知识问答", "答疑", "2", "解释"},
        "标准受治理": {"标准", "受治理", "标准受治理", "1", "正式"},
        "快速原型": {"快速", "原型", "快速原型", "2", "先试"},
        "结论即可": {"结论", "结论即可", "1"},
        "要产物文件": {"产物", "文件", "要产物文件", "2"},
        "要可复跑脚本": {"脚本", "可复跑", "要可复跑脚本", "3"},
        "按我给的路径": {"按我给的路径", "用这个", "就这个", "1"},
        "我再补路径": {"我再补路径", "补路径", "再补", "2"},
        "我自己": {"我自己", "我", "自己", "1"},
        "指定他人": {"指定他人", "别人", "他人", "2"},
    }.get(option, set())
    return base | extra


def _canonical_choice(raw_choice: str, options: list[str]) -> str:
    normalized = str(raw_choice or "").strip()
    if not normalized:
        return ""
    for option in options:
        if normalized == option:
            return option
    lower = normalized.lower()
    for option in options:
        aliases = _option_aliases(option)
        if normalized in aliases or lower in {a.lower() for a in aliases}:
            return option
    return ""


def resolve_intake_clarify(
    *,
    request_id: str,
    dimension_id: str,
    choice: str,
    actor_id: str = "",
    actor_name: str = "",
    source: str = "button",
    event_id: str = "",
) -> dict[str, Any]:
    """Resolve one dimension. Idempotent; button + text share this path."""
    request_id = str(request_id or "").strip()
    dimension_id = str(dimension_id or "").strip()
    if not request_id or not dimension_id:
        return {"ok": False, "changed": False, "error": "missing request_id or dimension_id"}
    with _clarify_lock(request_id):
        path = sidecar_path(request_id)
        body = _load(path)
        if not body:
            return {"ok": False, "changed": False, "error": "no clarification for request"}
        dims = body.get("dimensions") if isinstance(body.get("dimensions"), list) else []
        for item in dims:
            if not isinstance(item, dict) or str(item.get("id") or "").strip() != dimension_id:
                continue
            if item.get("resolved") is not None:
                return {"ok": True, "changed": False, "duplicate": True,
                        "request_id": request_id, "dimension_id": dimension_id}
            options = [str(o) for o in (item.get("options") or [])]
            canonical = _canonical_choice(choice, options)
            if not canonical:
                return {"ok": False, "changed": False, "error": "choice not allowed", "options": options}
            item["resolved"] = {
                "choice": canonical,
                "raw_choice": str(choice or "").strip(),
                "resolved_at": _now_iso(),
                "source": source,
                "actor_id": str(actor_id or "").strip(),
                "actor_name": str(actor_name or "").strip(),
                **({"event_id": str(event_id)} if event_id else {}),
            }
            body["dimensions"] = dims
            body["updated_at"] = item["resolved"]["resolved_at"]
            all_resolved = all(d.get("resolved") is not None for d in dims)
            body["all_resolved"] = all_resolved
            _atomic_write(path, body)
            return {"ok": True, "changed": True, "duplicate": False,
                    "request_id": request_id, "dimension_id": dimension_id,
                    "choice": canonical, "all_resolved": all_resolved}
    return {"ok": False, "changed": False, "error": "dimension not found",
            "request_id": request_id, "dimension_id": dimension_id}


def _clarify_actions(spec: ClarificationSpec) -> list[dict[str, Any]]:
    """Build single-select button rows for each OPEN dimension.

    Mirrors feishu_task_card._confirm_actions but carries hermes_action=intake_clarify
    and request_id (task is not created yet at intake time).
    """
    actions: list[dict[str, Any]] = []
    primary = {"执行任务", "标准受治理", "结论即可", "按我给的路径", "我自己"}
    for dim in spec.open_dimensions():
        dim_id = str(dim.get("id") or "dim")
        for option in (dim.get("options") or []):
            label = str(option or "").strip()
            if not label:
                continue
            actions.append({
                "tag": "button",
                "text": {"tag": "plain_text", "content": label},
                "type": "primary" if label in primary else "default",
                "value": {
                    "hermes_action": "intake_clarify",
                    "request_id": spec.request_id,
                    "dimension_id": dim_id,
                    "choice": label,
                },
            })
    return actions


def render_clarification_card(spec: ClarificationSpec, *, originator_name: str = "") -> dict[str, Any]:
    """Render a Feishu interactive card for the open clarification dimensions.

    Pure rendering: no network/filesystem. @-mention is built via feishu_mention so
    the originator is actually notified (the card itself does not push — callers must
    send the card as a fresh interactive message, and for stale nudges send a plain
    text @ per feishu_mention rules).
    """
    from gateway.feishu_mention import build_at_mention

    open_dims = spec.open_dimensions()
    elements: list[dict[str, Any]] = []
    if spec.originator_open_id:
        at = build_at_mention(spec.originator_open_id, originator_name)
        elements.append({"tag": "markdown", "content": f"{at} 这条需要你点一下，我就能准确接手："})
    for dim in open_dims:
        elements.append({"tag": "markdown", "content": f"**{str(dim.get('question') or '请选择')}**"})
    actions = _clarify_actions(spec)
    if actions:
        # Feishu caps actions per element; chunk into rows of <=4 to stay tidy.
        for i in range(0, len(actions), 4):
            elements.append({"tag": "action", "actions": actions[i:i + 4]})
    if spec.truncated:
        elements.append({"tag": "markdown",
                         "content": "（另有维度较多，未尽项可直接打字补充：" + "、".join(spec.truncated) + "）"})
    return {
        "config": {"wide_screen_mode": True},
        "header": {"title": {"tag": "plain_text", "content": "需要你确认一下"}, "template": "turquoise"},
        "elements": elements,
    }


def build_stale_nudge_text(spec: ClarificationSpec, *, originator_name: str = "") -> str:
    """Plain-text @-mention nudge when the card goes unanswered.

    Card patches do NOT push a Feishu notification, so the stale path must send a
    fresh PLAIN TEXT message carrying <at> (feishu_mention rule). Lists the still-open
    dimensions with their options so the user can click or type.
    """
    from gateway.feishu_mention import build_at_mention

    at = build_at_mention(spec.originator_open_id, originator_name) if spec.originator_open_id else ""
    lines = [f"{at} 这条任务还等你确认下面几点，点卡片按钮或直接回复都行："] if at else ["这条任务还等你确认："]
    for idx, dim in enumerate(spec.open_dimensions(), 1):
        opts = " / ".join(str(o) for o in (dim.get("options") or []))
        lines.append(f"{idx}. {dim.get('question')}：{opts}")
    return "\n".join(lines)


def resolve_intake_clarify_by_text(
    *,
    request_id: str,
    text: str,
    actor_id: str = "",
    actor_name: str = "",
    event_id: str = "",
) -> dict[str, Any]:
    """Text fallback: match a typed reply against the single open dimension's options.

    Conservative: only resolves when exactly one open dimension matches the text,
    to avoid ambiguous cross-dimension resolution.
    """
    spec = load_spec(request_id)
    if not spec:
        return {"ok": False, "changed": False, "error": "no clarification for request"}
    matches: list[tuple[str, str]] = []
    for item in spec.open_dimensions():
        options = [str(o) for o in (item.get("options") or [])]
        canonical = _canonical_choice(text, options)
        if canonical:
            matches.append((str(item.get("id") or ""), canonical))
    if not matches:
        return {"ok": False, "changed": False, "error": "text does not match any open dimension"}
    if len(matches) > 1:
        return {"ok": False, "changed": False, "error": "ambiguous clarification text"}
    dimension_id, canonical = matches[0]
    return resolve_intake_clarify(
        request_id=request_id, dimension_id=dimension_id, choice=canonical,
        actor_id=actor_id, actor_name=actor_name, source="text", event_id=event_id,
    )
