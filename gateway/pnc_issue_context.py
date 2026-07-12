"""Host-side Feishu issue context helpers for PNC/G1Q3 RCA intake.

This module keeps Feishu Project field/comment reads out of gateway.run so the
G1Q3 RCA intake path has a fixed, testable host-side scaffold.  It is
best-effort by design: failed or unavailable Feishu reads produce an empty
context block rather than pretending business evidence is missing.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import time
from dataclasses import dataclass
import logging
from typing import Any, Callable, Literal

GatewayToolCaller = Callable[[str, dict[str, Any]], Any]
IssueReadStatus = Literal["not_requested", "read_failed", "read_empty", "fields_extracted"]
MeegleRunner = Callable[[list[str]], tuple[int, str, str]]


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class G1Q3IssueReadResult:
    """Structured host-side Feishu issue read result for RCA state transitions."""

    context_text: str = ""
    status: IssueReadStatus = "not_requested"
    blocker: dict[str, Any] | None = None
    errors: list[dict[str, str]] | None = None
    # Which read source produced context_text: "meegle", "mcp", or
    # "mcp_auto_degraded" (Meegle source down, MCP picked up automatically).
    source: str = ""

    @property
    def source_quality(self) -> str:
        return "partial" if self.context_text else "unavailable"




_FEISHU_ISSUE_PROJECT_RE = re.compile(
    r"project\.feishu\.cn/([^/\s)]+)/issue/detail/(\d+)",
    re.IGNORECASE,
)


G1Q3_RCA_FEISHU_PROJECT_KEY = "t03o4q"
G1Q3_RCA_GROUP_ID = "oc_6cfc782212009ff4cd815349909dd423"
PNC_ALL_BUSINESS_TEST_GROUP_ID = "oc_16614f4ba25b8c88b69c0b8e9ebc2fb5"
MEEGLE_CLI_TIMEOUT_SECONDS = 12


def extract_feishu_issue_project_key(text: str, *, work_item_id: str = "") -> str:
    """Extract the Feishu Project space key from an issue URL.

    If ``work_item_id`` is provided, only a URL for that work item is accepted.
    """
    issue_id = str(work_item_id or "").strip()
    for match in _FEISHU_ISSUE_PROJECT_RE.finditer(text or ""):
        if not issue_id or match.group(2) == issue_id:
            return match.group(1)
    return ""


def resolve_feishu_issue_project_key(
    text: str,
    *,
    work_item_id: str = "",
    source_group_id: str = "",
) -> str:
    """Resolve the host-side Feishu project key for issue preread.

    URL evidence wins.  For the fixed G1Q3 RCA group, fall back to the known
    Feishu Project key so VM workers never need to guess the project space.
    """
    project_key = extract_feishu_issue_project_key(text or "", work_item_id=work_item_id)
    if project_key:
        return project_key
    if str(source_group_id or "").strip() in {G1Q3_RCA_GROUP_ID, PNC_ALL_BUSINESS_TEST_GROUP_ID} and str(work_item_id or "").strip():
        return G1Q3_RCA_FEISHU_PROJECT_KEY
    return ""


def call_gateway_tool(function_name: str, args: dict[str, Any]) -> dict[str, Any]:
    """Call an already-registered Hermes tool from gateway-side policy code.

    MCP discovery can fail transiently during gateway startup.  For G1Q3 host
    preread we retry once after lazy MCP discovery when the registry reports
    the Feishu Project tool as unknown.
    """
    from model_tools import handle_function_call

    def _call_once() -> dict[str, Any]:
        raw = handle_function_call(function_name, args)
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                return {"result": raw}
            return parsed if isinstance(parsed, dict) else {"result": parsed}
        return raw if isinstance(raw, dict) else {"result": raw}

    parsed = _call_once()
    if function_name.startswith("mcp_feishu_project_") and _is_unknown_tool_payload(parsed, function_name):
        _discover_mcp_tools_once()
        parsed = _call_once()
    return parsed


def mcp_result_payload(raw: Any) -> Any:
    """Normalize common MCP wrapper shapes to the underlying payload.

    Some Feishu Project MCP responses are JSON text followed by diagnostic
    suffixes such as ``log_id: ...``.  Parse the leading JSON document instead
    of downgrading the whole payload to a string.
    """
    payload = raw.get("result") if isinstance(raw, dict) and "result" in raw else raw
    if isinstance(payload, str) and payload.strip():
        text = payload.strip()
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            try:
                parsed, _idx = json.JSONDecoder().raw_decode(text)
                return parsed
            except json.JSONDecodeError:
                return payload
    return payload


def _payload_error(payload: Any) -> str:
    if isinstance(payload, dict) and payload.get("error"):
        return str(payload.get("error") or "").strip()
    return ""


def _is_unknown_tool_payload(payload: Any, function_name: str) -> bool:
    error = _payload_error(payload)
    return bool(error and error.startswith("Unknown tool:") and function_name in error)


def _discover_mcp_tools_once() -> None:
    """Best-effort lazy MCP discovery for policy-side host preread calls."""
    try:
        from tools.mcp_tool import discover_mcp_tools
        discover_mcp_tools()
    except Exception as exc:
        logger.warning("G1Q3 issue preread lazy MCP discovery failed: %s", exc)


def check_meegle_auth_status(runner: MeegleRunner | None = None) -> dict[str, Any]:
    """Structured Meegle CLI auth preflight for gateway startup/observability.

    Best-effort and never raises.  ``ok`` means the CLI answered and reports an
    authenticated session; ``authenticated is None`` means the check itself
    failed (CLI missing, timeout), which callers must not conflate with an
    expired login.
    """
    active_runner = runner or default_meegle_runner
    try:
        rc, out, err = active_runner(["auth", "status", "--format", "json"])
    except subprocess.TimeoutExpired:
        return {"ok": False, "authenticated": None, "expires_in_minutes": None, "host": "", "error": "meegle auth status timeout"}
    except Exception as exc:
        return {"ok": False, "authenticated": None, "expires_in_minutes": None, "host": "", "error": f"{type(exc).__name__}: {exc}"[:200]}

    payload = _json_from_cli_stdout(out)
    payload = payload if isinstance(payload, dict) else {}
    authenticated = payload.get("authenticated")
    expires = payload.get("expires_in_minutes")
    try:
        expires = int(expires) if expires is not None else None
    except (TypeError, ValueError):
        expires = None
    host = str(payload.get("host") or "")
    if rc != 0:
        return {
            "ok": False,
            "authenticated": bool(authenticated) if isinstance(authenticated, bool) else None,
            "expires_in_minutes": expires,
            "host": host,
            "error": str(err or out or "meegle auth status failed").strip()[:200],
        }
    is_auth = bool(authenticated)
    return {
        "ok": is_auth,
        "authenticated": is_auth,
        "expires_in_minutes": expires,
        "host": host,
        "error": "" if is_auth else str(payload.get("reason") or "meegle unauthenticated")[:200],
    }


def default_meegle_runner(args: list[str]) -> tuple[int, str, str]:
    """Run the official Meegle CLI with bounded timeout and JSON-friendly env.

    This is a diagnostic/fallback source for Feishu Project reads.  Secrets are
    not passed explicitly; the CLI uses its own keychain/config or sanctioned
    MEEGLE_* environment variables when present.
    """
    exe = shutil.which("meegle")
    if not exe:
        return 127, "", "meegle CLI not found"
    env = os.environ.copy()
    env.setdefault("MEEGLE_HOST", "project.feishu.cn")
    completed = subprocess.run(
        [exe, *args],
        text=True,
        capture_output=True,
        timeout=MEEGLE_CLI_TIMEOUT_SECONDS,
        env=env,
        check=False,
    )
    return completed.returncode, completed.stdout, completed.stderr


def _json_from_cli_stdout(stdout: str) -> Any:
    text = str(stdout or "").strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        try:
            parsed, _idx = json.JSONDecoder().raw_decode(text)
            return parsed
        except json.JSONDecodeError:
            return {"raw_stdout": text[:1000]}


def _unwrap_data_payload(payload: Any) -> Any:
    current = payload
    for _ in range(3):
        if isinstance(current, dict) and isinstance(current.get("data"), (dict, list)):
            current = current.get("data")
            continue
        return current
    return current


def _first_list_payload(payload: Any, keys: tuple[str, ...]) -> list[dict[str, Any]]:
    current = _unwrap_data_payload(payload)
    if isinstance(current, list):
        return [item for item in current if isinstance(item, dict)]
    if isinstance(current, dict):
        for key in keys:
            value = current.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        for key in ("result", "results"):
            value = current.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def _normalize_meegle_fields(raw_fields: Any) -> list[dict[str, Any]]:
    if isinstance(raw_fields, list):
        normalized: list[dict[str, Any]] = []
        for field in raw_fields:
            if not isinstance(field, dict):
                continue
            normalized.append({
                "key": field.get("key") or field.get("field_key") or field.get("fieldKey") or field.get("id") or "",
                "name": field.get("name") or field.get("field_name") or field.get("fieldName") or field.get("label") or "",
                "value": field.get("value") if "value" in field else field.get("field_value", field.get("fieldValue")),
            })
        return normalized
    if isinstance(raw_fields, dict):
        return [{"key": str(key), "name": str(key), "value": value} for key, value in raw_fields.items()]
    return []


def _capture_g1q3_issue_context(
    *,
    project_key: str,
    work_item_id: str,
    read_source: str,
    context_text: str,
    read_status: str,
    blocker: dict[str, Any] | None = None,
    errors: list[dict[str, Any]] | None = None,
) -> None:
    if not context_text and read_status == "fields_extracted":
        return
    try:
        from gateway.pnc_issue_capture import maybe_capture_issue_context
        maybe_capture_issue_context(
            project_key=project_key,
            work_item_id=work_item_id,
            read_source=read_source,
            context_text=context_text,
            read_status=read_status,
            blocker=blocker,
            errors=errors,
        )
    except Exception as exc:  # pragma: no cover - capture must never affect preread
        logger.warning("G1Q3 issue capture sidecar write failed: %s", exc)


def _normalize_meegle_workitem_payload(payload: Any, *, work_item_id: str) -> dict[str, Any]:
    item = _unwrap_data_payload(payload)
    if not isinstance(item, dict):
        return {}
    if isinstance(item.get("work_item_attribute"), dict) or isinstance(item.get("work_item_fields"), list):
        return item

    info = item.get("work_item_info") if isinstance(item.get("work_item_info"), dict) else {}
    status = item.get("status") or item.get("state") or item.get("state_info") or info.get("status") or info.get("state")
    if isinstance(status, str):
        status_obj: Any = {"name": status}
    elif isinstance(status, dict):
        status_obj = {"name": status.get("name") or status.get("label") or status.get("state_key_name") or status.get("end_state_key_name") or status.get("key") or ""}
    else:
        status_obj = {}
    attrs = {
        "work_item_id": str(item.get("work_item_id") or item.get("id") or info.get("work_item_id") or info.get("id") or work_item_id),
        "work_item_name": str(item.get("work_item_name") or item.get("name") or item.get("title") or info.get("work_item_name") or info.get("name") or ""),
        "work_item_status": status_obj,
    }
    fields = _normalize_meegle_fields(item.get("fields") or item.get("work_item_fields") or item.get("field_values") or {})
    return {"work_item_attribute": attrs, "work_item_fields": fields}


def _normalize_meegle_comments_payload(payload: Any) -> list[dict[str, Any]]:
    comments = _first_list_payload(payload, ("comments", "list", "items"))
    normalized: list[dict[str, Any]] = []
    for item in comments:
        content = item.get("content") or item.get("text") or item.get("body") or item.get("comment") or ""
        created = item.get("created_at") or item.get("create_time") or item.get("created_time") or item.get("time") or ""
        normalized.append({"created_at": str(created or ""), "content": compact_value(content)})
    return normalized


def fetch_g1q3_issue_context_result_via_meegle(
    *,
    project_key: str,
    work_item_id: str,
    runner: MeegleRunner = default_meegle_runner,
) -> G1Q3IssueReadResult:
    """Best-effort read through the official @lark-project/meegle CLI."""
    issue_id = str(work_item_id or "").strip()
    if not issue_id:
        return G1Q3IssueReadResult(status="not_requested")
    project = str(project_key or "").strip()
    if not project:
        return G1Q3IssueReadResult(
            status="read_failed",
            blocker={"kind": "host_meegle_preread_missing_project_key", "message": "Meegle CLI 兜底读取缺少 project_key", "retryable": False},
            errors=[{"tool": "meegle", "error_class": "MissingProjectKey"}],
        )

    errors: list[dict[str, str]] = []
    try:
        rc, out, err = runner(["auth", "status", "--format", "json"])
        auth_payload = _json_from_cli_stdout(out)
        if rc != 0 or (isinstance(auth_payload, dict) and auth_payload.get("authenticated") is False):
            reason = auth_payload.get("reason") if isinstance(auth_payload, dict) else (err or out)
            return G1Q3IssueReadResult(
                status="read_failed",
                blocker={
                    "kind": "host_meegle_preread_unauthenticated",
                    "message": "Meegle 未登录或授权已过期，主控暂时无法读取飞书 issue 字段/评论；请在主控机重新执行 meegle auth login。这不代表 问题数据地址_PDCL 缺失",
                    "retryable": True,
                },
                errors=[{"tool": "meegle auth status", "error_class": "Unauthenticated", "message": str(reason or "")[:200]}],
            )
    except subprocess.TimeoutExpired:
        return G1Q3IssueReadResult(
            status="read_failed",
            blocker={"kind": "host_meegle_preread_timeout", "message": "Meegle CLI auth status 超时", "retryable": True},
            errors=[{"tool": "meegle auth status", "error_class": "TimeoutExpired"}],
        )
    except Exception as exc:
        return G1Q3IssueReadResult(
            status="read_failed",
            blocker={"kind": "host_meegle_preread_failed", "message": "Meegle CLI 兜底读取启动失败", "retryable": True},
            errors=[{"tool": "meegle auth status", "error_class": type(exc).__name__, "message": str(exc)[:200]}],
        )

    workitem_payload: Any = {}
    comments_payload: Any = {}
    try:
        rc, out, err = runner(["workitem", "get", "--project-key", project, "--work-item-id", issue_id, "--fields", "_all", "--format", "json"])
        if rc != 0:
            errors.append({"tool": "meegle workitem get", "error_class": "CLIError", "message": (err or out)[:200]})
        else:
            workitem_payload = _json_from_cli_stdout(out)
    except subprocess.TimeoutExpired:
        errors.append({"tool": "meegle workitem get", "error_class": "TimeoutExpired"})
    except Exception as exc:
        errors.append({"tool": "meegle workitem get", "error_class": type(exc).__name__, "message": str(exc)[:200]})

    try:
        rc, out, err = runner(["comment", "list", "--project-key", project, "--work-item-id", issue_id, "--format", "json"])
        if rc != 0:
            errors.append({"tool": "meegle comment list", "error_class": "CLIError", "message": (err or out)[:200]})
        else:
            comments_payload = _json_from_cli_stdout(out)
    except subprocess.TimeoutExpired:
        errors.append({"tool": "meegle comment list", "error_class": "TimeoutExpired"})
    except Exception as exc:
        errors.append({"tool": "meegle comment list", "error_class": type(exc).__name__, "message": str(exc)[:200]})

    context_text = compact_g1q3_issue_context(
        work_item_brief=_normalize_meegle_workitem_payload(workitem_payload, work_item_id=issue_id),
        comments=_normalize_meegle_comments_payload(comments_payload),
    )
    if context_text:
        _capture_g1q3_issue_context(project_key=project, work_item_id=issue_id, read_source="meegle", context_text=context_text, read_status="fields_extracted", errors=errors or None)
        return G1Q3IssueReadResult(context_text=context_text, status="fields_extracted", errors=errors or None, source="meegle")
    if errors:
        return G1Q3IssueReadResult(
            status="read_failed",
            blocker={
                "kind": "host_meegle_preread_failed",
                "message": "Meegle CLI 兜底读取飞书 issue 字段/评论失败，不能据此判定字段缺失",
                "retryable": True,
                "failed_tools": [item["tool"] for item in errors],
            },
            errors=errors,
        )
    return G1Q3IssueReadResult(
        status="read_empty",
        blocker={"kind": "host_meegle_preread_empty", "message": "Meegle CLI 兜底读取返回空结果，不能据此判定字段缺失", "retryable": True},
    )


def _field_maps(work_item_brief: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    attrs = work_item_brief.get("work_item_attribute") if isinstance(work_item_brief, dict) else {}
    attrs = attrs if isinstance(attrs, dict) else {}
    fields = work_item_brief.get("work_item_fields") if isinstance(work_item_brief, dict) else []
    fields = fields if isinstance(fields, list) else []

    by_key: dict[str, Any] = {}
    by_name: dict[str, Any] = {}
    for field in fields:
        if not isinstance(field, dict):
            continue
        key = str(field.get("key") or "").strip()
        name = str(field.get("name") or "").strip()
        value = field.get("value")
        if key:
            by_key[key] = value
        if name:
            by_name[name] = value
    return attrs, by_key, by_name


def compact_value(value: Any) -> str:
    """Format Feishu Project field values into compact, recipient-safe text."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        if "label" in value:
            return str(value.get("label") or "").strip()
        if "name" in value:
            return str(value.get("name") or "").strip()
        if "iso_time" in value:
            return str(value.get("iso_time") or "").strip()
        if "timestamp" in value and not value.get("iso_time"):
            return str(value.get("timestamp") or "").strip()
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        parts = [compact_value(item) for item in value]
        parts = [part for part in parts if part]
        return ", ".join(parts)
    return str(value).strip()


def compact_g1q3_issue_context(*, work_item_brief: dict[str, Any], comments: list[dict[str, Any]]) -> str:
    """Build a compact, recipient-safe issue context block for VM intake."""
    attrs, by_key, by_name = _field_maps(work_item_brief)

    title = str(attrs.get("work_item_name") or "").strip()
    project_name = compact_value(by_key.get("field_052f23") or by_name.get("所属项目"))
    frame_id = compact_value(by_key.get("field_1fda45") or by_name.get("问题发生frameid"))
    happened_at = compact_value(by_name.get("发生时间"))
    data_addr = compact_value(by_key.get("field_93aa63") or by_name.get("问题数据地址_PDCL"))
    root_cause = compact_value(by_key.get("field_842fc8") or by_name.get("问题根本原因分析"))
    owner = compact_value(by_name.get("当前负责人") or by_name.get("责任人"))
    vehicle = compact_value(by_key.get("field_9e1bd0") or by_name.get("车辆编号/台架编号"))
    status_name = compact_value((attrs.get("work_item_status") or {}).get("name"))
    description = compact_value(by_name.get("描述"))[:1200]

    lines = ["## Feishu issue 已解析字段（主控侧读取）"]
    if title:
        lines.append(f"- title: {title}")
    work_item_id = str(attrs.get("work_item_id") or "").strip()
    if work_item_id:
        lines.append(f"- work_item_id: {work_item_id}")
    if project_name:
        lines.append(f"- 所属项目: {project_name}")
    if status_name:
        lines.append(f"- 当前状态: {status_name}")
    if owner:
        lines.append(f"- 当前负责人: {owner}")
    if vehicle:
        lines.append(f"- 车辆编号: {vehicle}")
    if frame_id:
        lines.append(f"- frame_id: {frame_id}")
    if happened_at:
        lines.append(f"- 发生时间: {happened_at}")
    if data_addr:
        lines.append(f"- 数据地址: {data_addr}")
    if root_cause:
        lines.append(f"- 根因分析字段: {root_cause}")
    if description:
        lines.append("- 描述摘录:")
        lines.append(description)

    compact_comments: list[str] = []
    for raw in comments[:3]:
        if not isinstance(raw, dict):
            continue
        created = str(raw.get("created_at") or "").strip()
        content = str(raw.get("content") or "").strip().replace("\r\n", "\n")
        if not content:
            continue
        content = re.sub(r"!\[[^\]]*\]\([^\)]*\)(?:<!--.*?-->)?", "[image]", content)
        content = re.sub(r"\n{3,}", "\n\n", content).strip()
        if len(content) > 500:
            content = content[:500].rstrip() + "..."
        compact_comments.append(f"- {created}: {content}" if created else f"- {content}")
    if compact_comments:
        lines.append("\n## 最近评论摘录")
        lines.extend(compact_comments)
    # A successful field read must surface at least one substantive field or
    # comment.  work_item_id alone is NOT evidence of a successful read: it is
    # derived from the request URL and is present even when the Feishu/Meegle
    # API returns an empty/odd payload (stale login, missing scope, partial
    # token).  If only the header (+ work_item_id) survived, treat the read as
    # empty so the caller classifies it read_empty/read_failed instead of
    # mislabeling it fields_extracted and then mis-diagnosing a missing
    # 问题数据地址_PDCL.  Regression: Feishu issue 7025381565 (2026-06-23).
    has_substantive_field = any(
        (
            title,
            project_name,
            status_name,
            owner,
            vehicle,
            frame_id,
            happened_at,
            data_addr,
            root_cause,
            description,
        )
    )
    if not has_substantive_field and not compact_comments:
        return ""
    return "\n".join(line for line in lines if line).strip()


def fetch_g1q3_issue_context_result_via_mcp(
    *,
    project_key: str,
    work_item_id: str,
    tool_caller: GatewayToolCaller = call_gateway_tool,
    now_ms: int | None = None,
) -> G1Q3IssueReadResult:
    """Best-effort read through Hermes MCP Feishu Project tools.

    This is no longer the default G1Q3 issue preread path.  It is retained as
    an explicit fallback/diagnostic path so Feishu Project issue intake does not
    load or discover MCP tools unless requested.
    """
    issue_id = str(work_item_id or "").strip()
    if not issue_id:
        return G1Q3IssueReadResult(status="not_requested")

    errors: list[dict[str, str]] = []
    brief_payload: Any = {}
    comments_payload: Any = {}
    try:
        raw_brief = tool_caller(
            "mcp_feishu_project_get_workitem_brief",
            {
                "fields": ["_all"],
                "name": "",
                "page_size": 100,
                "page_token": "",
                "project_key": project_key,
                "url": "",
                "work_item_id": issue_id,
            },
        )
        brief_payload = mcp_result_payload(raw_brief)
        if _payload_error(brief_payload):
            errors.append({"tool": "mcp_feishu_project_get_workitem_brief", "error_class": "ToolError", "message": _payload_error(brief_payload)[:200]})
            brief_payload = {}
    except Exception as exc:
        logger.warning("G1Q3 issue preread MCP failed at workitem brief: %s", exc)
        errors.append({"tool": "mcp_feishu_project_get_workitem_brief", "error_class": type(exc).__name__})
    try:
        raw_comments = tool_caller(
            "mcp_feishu_project_list_workitem_comments",
            {
                "end_time": now_ms if now_ms is not None else int(time.time() * 1000),
                "page_num": 1,
                "project_key": project_key,
                "start_time": 0,
                "work_item_id": issue_id,
            },
        )
        comments_payload = mcp_result_payload(raw_comments)
        if _payload_error(comments_payload):
            errors.append({"tool": "mcp_feishu_project_list_workitem_comments", "error_class": "ToolError", "message": _payload_error(comments_payload)[:200]})
            comments_payload = {}
    except Exception as exc:
        logger.warning("G1Q3 issue preread MCP failed at comments: %s", exc)
        errors.append({"tool": "mcp_feishu_project_list_workitem_comments", "error_class": type(exc).__name__})

    comments = comments_payload.get("comments") if isinstance(comments_payload, dict) else comments_payload
    context_text = compact_g1q3_issue_context(
        work_item_brief=brief_payload if isinstance(brief_payload, dict) else {},
        comments=comments if isinstance(comments, list) else [],
    )
    if context_text:
        _capture_g1q3_issue_context(project_key=project_key, work_item_id=issue_id, read_source="mcp", context_text=context_text, read_status="fields_extracted", errors=errors or None)
        return G1Q3IssueReadResult(context_text=context_text, status="fields_extracted", errors=errors or None, source="mcp")
    if errors:
        return G1Q3IssueReadResult(
            status="read_failed",
            blocker={
                "kind": "host_mcp_preread_failed",
                "message": "MCP 飞书 Project issue 字段/评论读取失败，不能据此判定字段缺失",
                "retryable": True,
                "failed_tools": [item["tool"] for item in errors],
            },
            errors=errors,
        )
    return G1Q3IssueReadResult(
        status="read_empty",
        blocker={
            "kind": "host_mcp_preread_empty",
            "message": "MCP 飞书 Project issue 读取返回空结果，不能据此判定字段缺失",
            "retryable": True,
        },
    )


def fetch_g1q3_issue_context_result(
    *,
    project_key: str,
    work_item_id: str,
    tool_caller: GatewayToolCaller = call_gateway_tool,
    now_ms: int | None = None,
    use_meegle_fallback: bool | None = None,
    use_mcp_fallback: bool | None = None,
    meegle_runner: MeegleRunner = default_meegle_runner,
) -> G1Q3IssueReadResult:
    """Read Feishu Project issue fields/comments with Meegle as primary.

    Scope: G1Q3/Feishu Project issue preread only.  The default path uses the
    official Meegle CLI and does not call MCP tools, so MCP discovery/tool
    schemas are not loaded for this intake path.  MCP runs as fallback in two
    cases: explicitly via ``HERMES_G1Q3_MCP_FALLBACK=1`` / ``use_mcp_fallback=
    True``, or automatically when the Meegle source itself is down (expired
    login, missing CLI, timeouts) so an expired token degrades availability
    instead of blocking intake.  Auto-degrade can be disabled with
    ``HERMES_G1Q3_MCP_AUTODEGRADE=0``.
    """
    issue_id = str(work_item_id or "").strip()
    if not issue_id:
        return G1Q3IssueReadResult(status="not_requested")
    if use_mcp_fallback is None:
        use_mcp_fallback = os.getenv("HERMES_G1Q3_MCP_FALLBACK", "").strip().lower() in {"1", "true", "yes", "on"}

    meegle_result = fetch_g1q3_issue_context_result_via_meegle(
        project_key=project_key,
        work_item_id=issue_id,
        runner=meegle_runner,
    )
    if meegle_result.context_text:
        return meegle_result

    auto_degrade_enabled = os.getenv("HERMES_G1Q3_MCP_AUTODEGRADE", "1").strip().lower() not in {"0", "false", "no", "off"}
    meegle_source_down = meegle_result.status == "read_failed"
    auto_degrade = auto_degrade_enabled and meegle_source_down and not use_mcp_fallback

    if use_mcp_fallback or auto_degrade:
        if auto_degrade:
            logger.warning(
                "G1Q3 issue preread: Meegle source down (%s); auto-degrading to MCP fallback for work_item %s",
                (meegle_result.blocker or {}).get("kind") or "unknown",
                issue_id,
            )
        mcp_result = fetch_g1q3_issue_context_result_via_mcp(
            project_key=project_key,
            work_item_id=issue_id,
            tool_caller=tool_caller,
            now_ms=now_ms,
        )
        if mcp_result.context_text:
            combined_errors = [*(meegle_result.errors or []), *(mcp_result.errors or [])] or None
            if auto_degrade:
                _capture_g1q3_issue_context(
                    project_key=project_key,
                    work_item_id=issue_id,
                    read_source="mcp_auto_degraded",
                    context_text=mcp_result.context_text,
                    read_status="fields_extracted",
                    errors=combined_errors,
                )
            return G1Q3IssueReadResult(
                context_text=mcp_result.context_text,
                status="fields_extracted",
                errors=combined_errors,
                source="mcp_auto_degraded" if auto_degrade else "mcp",
            )
        errors = [*(meegle_result.errors or []), *(mcp_result.errors or [])]
        blocker = {
            "kind": "host_issue_preread_failed",
            "message": "Meegle 主链路和 MCP 兜底均未成功读取飞书 issue 字段/评论，不能据此判定字段缺失",
            "retryable": True,
            "failed_tools": [item.get("tool", "unknown") for item in errors],
        } if errors else {
            "kind": "host_issue_preread_empty",
            "message": "Meegle 主链路和 MCP 兜底均返回空结果，不能据此判定字段缺失",
            "retryable": True,
        }
        meegle_kind = str((meegle_result.blocker or {}).get("kind") or "")
        if errors and "unauthenticated" in meegle_kind:
            # Keep the unauthenticated signal: it drives the group-side
            # "请重新授权 Meegle" notice and operator alerting.
            blocker = {
                "kind": meegle_kind,
                "message": "Meegle 未登录或授权已过期，且 MCP 兜底也未读取成功；请重新执行 meegle auth login。这不代表 问题数据地址_PDCL 缺失",
                "retryable": True,
                "failed_tools": [item.get("tool", "unknown") for item in errors],
            }
        return G1Q3IssueReadResult(status="read_failed" if errors else "read_empty", blocker=blocker, errors=errors or None)

    return meegle_result

def fetch_g1q3_issue_context(
    *,
    project_key: str,
    work_item_id: str,
    tool_caller: GatewayToolCaller = call_gateway_tool,
    now_ms: int | None = None,
    use_meegle_fallback: bool | None = None,
    meegle_runner: MeegleRunner = default_meegle_runner,
) -> str:
    """Backward-compatible compact text helper for existing callers/tests."""
    return fetch_g1q3_issue_context_result(
        project_key=project_key,
        work_item_id=work_item_id,
        tool_caller=tool_caller,
        now_ms=now_ms,
        use_meegle_fallback=use_meegle_fallback,
        meegle_runner=meegle_runner,
    ).context_text
