"""Shared-state v2 task submission helper for VM worker execution."""

from __future__ import annotations

import hashlib
import json
import re
import shlex
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from dataclasses import fields, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from gateway.pnc_rca_data_access import (
    RemoteDataAccessError,
    validate_remote_data_access,
)
from gateway.pnc_rca_prod_admission import (
    RcaProdAdmissionError,
    build_rca_prod_command_argv,
    command_sha256 as rca_prod_command_sha256,
    goal_sha256 as rca_prod_goal_sha256,
    issue_rca_prod_admission,
    validate_existing_rca_prod_meta,
)
from gateway.pnc_rca_workspace_runtime import (
    WorkspaceRuntimeError,
    WorkspaceRuntimeIdentity,
    validate_workspace_runtime,
)
from hermes_constants import get_hermes_home
from tools.registry import registry


def _session_value(name: str) -> str:
    try:
        from gateway.session_context import get_session_env

        return (get_session_env(name, "") or "").strip()
    except Exception:
        return ""


def _resolve_submitter(user_id: str = "", owner: str = "") -> tuple[str, str]:
    resolved_user_id = str(user_id or _session_value("HERMES_SESSION_USER_ID")).strip()
    explicit_owner = str(owner or "").strip()
    user_name = _session_value("HERMES_SESSION_USER_NAME") or explicit_owner
    if resolved_user_id:
        try:
            from tools.permission_policy import _load_config

            mapped = _load_config().get("user_id_mapping", {}).get(resolved_user_id)
            if mapped:
                user_name = str(mapped).strip()
            elif explicit_owner and resolved_user_id == explicit_owner:
                # Local tool fallbacks sometimes pass a display name through the
                # user_id parameter because no platform open_id is available.
                # Treat an exact owner/display-name match as a name, not as an
                # unknown platform id that would fail closed to member.
                resolved_user_id = ""
                user_name = explicit_owner
        except Exception:
            if explicit_owner and resolved_user_id == explicit_owner:
                resolved_user_id = ""
                user_name = explicit_owner
    return user_name, resolved_user_id


def _integration_tools_session_vm_permission_open() -> bool:
    """Return True when current session is the integration_tools intake group.

    Business policy: members of that group have VM system/tool business
    execution permission by default. This bypasses only the coarse role gate;
    risk_class=high and governed MCAP/destructive guards remain outside this
    helper and must still fail closed.
    """
    chat_id = _session_value("HERMES_SESSION_CHAT_ID")
    if not chat_id:
        return False
    try:
        from hermes_cli.config import cfg_get, load_config

        cfg = load_config() or {}
        block = cfg_get(cfg, "business_lines", "integration_tools", default={}) or {}
        if not isinstance(block, dict) or not bool(block.get("enabled", False)):
            return False
        vm_policy = block.get("vm_tool_business_permission") or {}
        if (
            isinstance(vm_policy, dict)
            and vm_policy.get("group_default_allowed") is False
        ):
            return False
        raw_ids: list[Any] = []
        for key in (
            "intake_chat_ids",
            "intake_chat_id",
            "intake_group_ids",
            "intake_group_id",
        ):
            value = block.get(key)
            if isinstance(value, (list, tuple, set)):
                raw_ids.extend(value)
            elif value:
                raw_ids.append(value)
        return chat_id in {str(v).strip() for v in raw_ids if str(v or "").strip()}
    except Exception:
        return False


def _vm_task_permission_denied_payload(
    user_name: str = "", role: str = ""
) -> dict[str, Any]:
    requester = str(user_name or "当前账号").strip() or "当前账号"
    return {
        "success": False,
        "error_code": "vm_task_permission_denied",
        "error": (
            f"{requester}当前账号没有 VM 编译/执行任务权限，本次未执行。"
            "请管理员授权 VM worker task 权限，或由 owner/有权限的人在同一线程重新发起。"
            "授权后会按审计规则在对应 worktree 执行，并回传 commit SHA、CI 链接和产物路径或失败摘要。"
        ),
        "retryable": False,
        "returncode": None,
    }


def _vm_task_permission_policy_unavailable_payload() -> dict[str, Any]:
    return {
        "success": False,
        "error_code": "vm_task_permission_policy_unavailable",
        "error": "暂时无法确认 VM 编译/执行任务权限，本次未执行。请稍后重试，或让管理员在同一线程重新发起。",
        "retryable": True,
        "returncode": None,
    }


def _check_vm_task_permission(
    user_name: str, user_id: str = "", risk_class: str = ""
) -> dict[str, Any] | None:
    # integration_tools business group defaults VM system/tool execution open.
    # Keep explicit high-risk tasks blocked for manual_review/escalation.
    if (
        _integration_tools_session_vm_permission_open()
        and str(risk_class or "").strip().lower() != "high"
    ):
        return None
    try:
        from tools.permission_policy import get_user_role, get_user_role_by_id

        role = get_user_role_by_id(user_id) if user_id else get_user_role(user_name)
    except Exception:
        return _vm_task_permission_policy_unavailable_payload()
    if role not in {"owner", "admin", "senior"}:
        return _vm_task_permission_denied_payload(user_name, role)
    return None


_RCA_SERVICE_CAPABILITY = "submit_g1q3_rca_issue_intake"
_RCA_SERVICE_OPERATION = "g1q3_rca_issue_intake"
_RCA_RESERVATION_MAX_OBSERVED_AGE_SECONDS = 120
_RCA_RESERVATION_MIN_REMAINING_LEASE_SECONDS = 150
_RCA_MAX_EXPECTED_ARTIFACT_CACHE_BYTES = 1_000_000_000_000
_RCA_ADMISSION_JSON_BEGIN = "<!-- G1Q3_RCA_ADMISSION_JSON:BEGIN -->"
_RCA_ADMISSION_JSON_END = "<!-- G1Q3_RCA_ADMISSION_JSON:END -->"
_RCA_EXECUTION_REQUEST_JSON_BEGIN = "<!-- G1Q3_RCA_EXECUTION_REQUEST_JSON:BEGIN -->"
_RCA_EXECUTION_REQUEST_JSON_END = "<!-- G1Q3_RCA_EXECUTION_REQUEST_JSON:END -->"
_RCA_VM_REPO_ROOT = "/home/mini/data3/yj-evaluation-server"
_RCA_FIXED_CLI_RELATIVE_PATH = "./api/g1q3_rca/scripts/run_rca_service_request.py"
_RCA_VM_TASK_ROOT = "/home/mini/.hermes/shared-state/tasks"
_RCA_STORAGE_ADMISSION_SCHEMA_VERSION = "pnc_rca_derived_capacity_admission_v2"
_RCA_FORBIDDEN_LEGACY_DOWNLOAD_FIELDS = frozenset({
    "pdcl_download_cmd",
    "pdcl_download_command",
    "mdi_download_cmd",
    "mdi_download_command",
    "mdi_refresh_cmd",
    "mdi_refresh_command",
    "is_pdcl_format",
})
_RCA_DOWNLOAD_FALSE_ONLY_FIELDS = frozenset({
    "allow_download",
    "download_enabled",
    "legacy_governance_download_enabled",
    "mdi_download_allowed",
    "mdi_download_attempted",
    "mdi_download_enabled",
    "pdcl_download_enabled",
})
_RCA_DOWNLOAD_ZERO_ONLY_FIELDS = frozenset({
    "auto_download_daily_quota",
    "download_daily_quota",
    "input_materialization_bytes_per_case",
    "legacy_daily_quota",
    "mdi_invocation_count",
})
_RCA_FORBIDDEN_LEGACY_ACCESS_MODE_VALUES = frozenset({
    "minimal_download",
    "full_download",
    "mdi_download",
})
_RCA_POLICY_INVARIANT_FIELDS = frozenset({
    "historical_policy_invariant",
    "historical_policy_invariants",
    "policy_invariant",
    "policy_invariants",
})
_RCA_LEGACY_DOWNLOAD_COMMAND_RE = re.compile(
    r"\b(?:mdi|pdcl)(?:\s+|[-_])(?:download|refresh(?:2)?)\b",
    re.IGNORECASE,
)
_RCA_LEGACY_DOWNLOAD_KEY_RE = re.compile(
    r"(?:^|_)(?:mdi|pdcl)(?:_|$).*?(?:download|refresh(?:2)?)(?:_|$)",
    re.IGNORECASE,
)
_RCA_DISABLED_POLICY_TEXT_RE = re.compile(
    r"\b(?:disabled|disallowed|forbidden|never|no\s+longer|prohibited|retired|must\s+not)\b",
    re.IGNORECASE,
)
_RCA_COMMAND_ARGUMENT_RE = re.compile(
    r"(?:^|\s)-{1,2}[A-Za-z][\w-]*|&&|\|\||\$\(|`|(?:^|\s)(?:\./|/[A-Za-z0-9])"
)
_RCA_FEISHU_ISSUE_URL_RE = re.compile(
    r"https?://project\.feishu\.cn/[A-Za-z0-9_-]+/issue/detail/[0-9]+"
    r"(?:[/?#\s]|$)",
    re.IGNORECASE,
)


def _normalized_rca_control_key(value: Any) -> str:
    key = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", str(value or "").strip())
    return re.sub(r"[^a-z0-9]+", "_", key.lower()).strip("_")


def _rca_policy_invariant_text(path: tuple[Any, ...], value: str) -> bool:
    keys = {
        _normalized_rca_control_key(component)
        for component in path
        if isinstance(component, str)
    }
    return bool(
        keys & _RCA_POLICY_INVARIANT_FIELDS
        and _RCA_DISABLED_POLICY_TEXT_RE.search(value)
        and not _RCA_COMMAND_ARGUMENT_RE.search(value)
    )


def _rca_json_path(path: tuple[Any, ...]) -> str:
    rendered = "$"
    for component in path:
        if isinstance(component, int):
            rendered += f"[{component}]"
        else:
            rendered += f".{component}"
    return rendered


def find_rca_legacy_download_violation(value: Any) -> str | None:
    """Return a path-scoped violation for any nested legacy input-download control."""
    stack: list[tuple[tuple[Any, ...], Any]] = [((), value)]
    while stack:
        path, item = stack.pop()
        if is_dataclass(item) and not isinstance(item, type):
            item = {field.name: getattr(item, field.name) for field in fields(item)}
        if isinstance(item, dict):
            for raw_key, child in reversed(tuple(item.items())):
                key = _normalized_rca_control_key(raw_key)
                child_path = (*path, str(raw_key))
                rendered_path = _rca_json_path(child_path)
                if key in _RCA_FORBIDDEN_LEGACY_DOWNLOAD_FIELDS:
                    return f"legacy_field:{rendered_path}"
                if _RCA_LEGACY_DOWNLOAD_COMMAND_RE.search(str(raw_key)):
                    return f"legacy_command_key:{rendered_path}"
                if key in _RCA_DOWNLOAD_FALSE_ONLY_FIELDS and child is not False:
                    return f"download_not_disabled:{rendered_path}"
                if (
                    key not in _RCA_DOWNLOAD_FALSE_ONLY_FIELDS
                    and _RCA_LEGACY_DOWNLOAD_KEY_RE.search(key)
                ):
                    return f"legacy_field:{rendered_path}"
                if key in _RCA_DOWNLOAD_ZERO_ONLY_FIELDS and (
                    type(child) is not int or child != 0
                ):
                    return f"download_not_zero:{rendered_path}"
                if key == "input_materialization" and child != "forbidden":
                    return f"input_materialization_not_forbidden:{rendered_path}"
                if key == "data_access_mode" and child != "remote_read":
                    return f"data_access_mode_not_remote_read:{rendered_path}"
                stack.append((child_path, child))
        elif isinstance(item, list):
            stack.extend(
                ((*path, index), child)
                for index, child in reversed(tuple(enumerate(item)))
            )
        elif isinstance(item, str):
            normalized_value = _normalized_rca_control_key(item)
            if normalized_value in _RCA_FORBIDDEN_LEGACY_ACCESS_MODE_VALUES:
                return f"legacy_access_mode:{_rca_json_path(path)}"
            if _RCA_LEGACY_DOWNLOAD_COMMAND_RE.search(item):
                if not _rca_policy_invariant_text(path, item):
                    return f"legacy_command:{_rca_json_path(path)}"
    return None


def _contains_rca_reserved_goal_marker(value: Any) -> bool:
    markers = (
        _RCA_ADMISSION_JSON_BEGIN,
        _RCA_ADMISSION_JSON_END,
        _RCA_EXECUTION_REQUEST_JSON_BEGIN,
        _RCA_EXECUTION_REQUEST_JSON_END,
    )
    stack = [value]
    while stack:
        item = stack.pop()
        if is_dataclass(item) and not isinstance(item, type):
            item = {field.name: getattr(item, field.name) for field in fields(item)}
        if isinstance(item, dict):
            stack.extend(item.keys())
            stack.extend(item.values())
        elif isinstance(item, (list, tuple)):
            stack.extend(item)
        elif isinstance(item, str) and any(marker in item for marker in markers):
            return True
    return False


def _is_reserved_rca_service_submission(
    *,
    title: str,
    goal: str,
    task_id: str,
    artifact_root: str,
    artifact_cifs_root: str,
) -> bool:
    """Keep issue-intake execution behind the capability-scoped service API."""
    normalized_title = str(title or "").strip().lower()
    normalized_goal = str(goal or "")
    normalized_goal_lower = normalized_goal.lower()
    normalized_task_id = str(task_id or "").strip().lower()
    normalized_artifact_root = str(artifact_root or "").strip().lower()
    normalized_artifact_cifs_root = str(artifact_cifs_root or "").strip().lower()
    issue_url_present = bool(
        _RCA_FEISHU_ISSUE_URL_RE.search(
            "\n".join((
                str(title or ""),
                normalized_goal,
                str(artifact_root or ""),
                str(artifact_cifs_root or ""),
            ))
        )
    )
    return any((
        issue_url_present,
        normalized_title.startswith("g1q3 rca issue intake"),
        normalized_title.startswith("g1q3-rca issue intake"),
        normalized_task_id.startswith("g1q3-rca-s1-"),
        normalized_task_id.startswith("g1q3-rca-issue-intake-"),
        normalized_task_id.startswith("g1q3_rca_issue_intake_"),
        normalized_task_id.startswith("g1q3-datapipe-"),
        "template_id: rca_issue_intake" in normalized_goal_lower,
        "g1q3_rca_issue_intake" in normalized_goal_lower,
        _RCA_ADMISSION_JSON_BEGIN.lower() in normalized_goal_lower,
        _RCA_ADMISSION_JSON_END.lower() in normalized_goal_lower,
        _RCA_EXECUTION_REQUEST_JSON_BEGIN.lower() in normalized_goal_lower,
        _RCA_EXECUTION_REQUEST_JSON_END.lower() in normalized_goal_lower,
        _RCA_FIXED_CLI_RELATIVE_PATH.lower() in normalized_goal_lower,
        "/api/g1q3_rca/scripts/run_rca_service_request.py" in normalized_goal_lower,
        "run_rca_auto_pipeline.py" in normalized_goal_lower,
        "/g1q3-rca-s1-" in normalized_artifact_root,
        "/g1q3_rca_issue_intake_" in normalized_artifact_root,
        "/g1q3-rca-s1-" in normalized_artifact_cifs_root,
        "/g1q3_rca_issue_intake_" in normalized_artifact_cifs_root,
    ))


def _reserved_rca_service_payload() -> dict[str, Any]:
    return {
        "success": False,
        "error_code": "g1q3_rca_service_boundary_required",
        "error": (
            "G1Q3 RCA issue intake is reserved for the capability-scoped "
            "vm_task_submit_service API"
        ),
        "retryable": False,
        "returncode": None,
    }


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _rca_reservation_freshness_error(
    receipt: dict[str, Any],
    *,
    admitted: bool,
    now: datetime | None = None,
) -> str | None:
    """Require monotonic, fresh reservation evidence before task creation."""

    def timestamp(value: Any) -> datetime | None:
        if value is None:
            return None
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("timezone-aware timestamp required")
        return parsed.astimezone(timezone.utc)

    try:
        current = (now or _utc_now()).astimezone(timezone.utc)
        observed = timestamp(receipt.get("observed_at"))
        reservation = receipt.get("reservation")
        if observed is None or not isinstance(reservation, dict):
            return "reservation timing evidence is missing"
        created = timestamp(reservation.get("created_at"))
        updated = timestamp(reservation.get("updated_at"))
        activated = timestamp(reservation.get("activated_at"))
        released = timestamp(reservation.get("released_at"))
        lease = timestamp(reservation.get("lease_expires_at"))
    except (TypeError, ValueError, OverflowError):
        return "reservation timing evidence is invalid"
    if created is None or updated is None or created > updated or updated > observed:
        return "reservation timestamps are not monotonic"
    if activated is not None and not (created <= activated <= updated):
        return "reservation activation timestamp is not monotonic"
    if released is not None and not (created <= released <= updated):
        return "reservation release timestamp is not monotonic"
    if admitted:
        age_seconds = (current - observed).total_seconds()
        if age_seconds < -5 or age_seconds > _RCA_RESERVATION_MAX_OBSERVED_AGE_SECONDS:
            return "reservation observation is stale"
        if (
            lease is None
            or (lease - current).total_seconds()
            < _RCA_RESERVATION_MIN_REMAINING_LEASE_SECONDS
        ):
            return "reservation lease does not cover the submit boundary"
    return None


def canonical_rca_contract_sha256(
    admission: dict[str, Any], execution_request: dict[str, Any]
) -> str:
    work_item = execution_request.get("work_item")
    work_item = work_item if isinstance(work_item, dict) else {}
    data = execution_request.get("data")
    data = data if isinstance(data, dict) else {}
    source_refs = execution_request.get("source_refs")
    source_refs = source_refs if isinstance(source_refs, dict) else {}
    toolchain = execution_request.get("toolchain")
    toolchain = toolchain if isinstance(toolchain, dict) else {}
    stable_source_refs = {
        key: source_refs.get(key)
        for key in (
            "task_id",
            "source_kind",
            "origin_source_id",
            "rule_version",
            "generation",
            "business_key",
            "submission_key",
        )
    }
    if source_refs.get("source_kind") == "kafka_workflow_event":
        stable_source_refs.update(
            {
                key: source_refs.get(key)
                for key in ("source_event_id", "topic", "partition", "offset")
            }
        )
    stable_request = {
        "schema_version": execution_request.get("schema_version"),
        "request_kind": execution_request.get("request_kind"),
        "work_item": {
            key: work_item.get(key)
            for key in ("project_key", "work_item_type", "work_item_id")
        },
        "data_paths": {
            key: data.get(key) for key in ("artifact_root", "artifact_cifs_root")
        },
        "data_access": data.get("data_access"),
        "execution_policy": execution_request.get("execution_policy"),
        "source_refs": stable_source_refs,
        "intake_dispatcher": toolchain.get("intake_dispatcher"),
    }
    canonical = json.dumps(
        {"admission": admission, "execution_request": stable_request},
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _rca_contract_sha256(
    admission: dict[str, Any], execution_request: dict[str, Any]
) -> str:
    """Compatibility alias for callers predating the public canonical helper."""
    return canonical_rca_contract_sha256(admission, execution_request)


def _validate_rca_remote_data_access(
    data: dict[str, Any], policy: dict[str, Any], request: dict[str, Any]
) -> str | None:
    """Validate the fixed no-MDI request boundary before creating a VM task."""
    legacy_violation = find_rca_legacy_download_violation(request)
    if legacy_violation:
        return (
            "legacy MDI/download controls are forbidden in the RCA execution "
            f"request ({legacy_violation})"
        )
    if policy.get("allow_download") is not False:
        return "RCA execution policy must explicitly forbid downloads"
    expected_policy = {
        "mode": "remote_read",
        "data_access_mode": "remote_read",
        "input_materialization": "forbidden",
        "derived_artifacts_allowed": True,
    }
    for key, expected in expected_policy.items():
        if policy.get(key) != expected:
            return f"RCA execution policy {key} must be {expected!r}"

    access = data.get("data_access")
    try:
        validate_remote_data_access(access)
    except RemoteDataAccessError as exc:
        return f"RCA data_access contract rejected: {exc.code}"
    return None


def _rca_existing_identity_error(
    existing: dict[str, Any],
    *,
    task_id: str,
    title: str,
    owner: str,
    expected_meta: dict[str, Any],
) -> str:
    meta = existing.get("meta") if isinstance(existing.get("meta"), dict) else {}
    mismatches: list[str] = []
    actual_task_id = str(existing.get("task_id") or meta.get("task_id") or "").strip()
    actual_title = str(existing.get("title") or meta.get("title") or "").strip()
    actual_owner = str(existing.get("owner") or meta.get("owner") or "").strip()
    if actual_task_id != task_id:
        mismatches.append("task_id")
    if actual_title != title:
        mismatches.append("title")
    if actual_owner != owner:
        mismatches.append("owner")
    for field, expected in expected_meta.items():
        if meta.get(field) != expected:
            mismatches.append(field)
    return ",".join(mismatches)


def _vm_task_service_denied_payload(error_code: str, error: str) -> dict[str, Any]:
    return {
        "success": False,
        "error_code": error_code,
        "error": error,
        "retryable": False,
        "returncode": None,
    }


def _check_vm_task_service_permission(
    service_id: str, capability: str
) -> dict[str, Any] | None:
    if capability != _RCA_SERVICE_CAPABILITY:
        return _vm_task_service_denied_payload(
            "vm_task_service_capability_denied",
            "service capability is not authorized for G1Q3 RCA issue intake",
        )
    try:
        from tools.permission_policy import service_capability_allows

        allowed = service_capability_allows(service_id, capability)
    except Exception:
        return _vm_task_permission_policy_unavailable_payload()
    if not allowed:
        return _vm_task_service_denied_payload(
            "vm_task_service_permission_denied",
            "service identity is not explicitly authorized for G1Q3 RCA issue intake",
        )
    return None


_DEFAULT_BRIDGE_ROOT = (
    Path.home() / "Mounts" / "mini_root" / "tmp" / "openclaw-shared-state"
)
_DEFAULT_HOST_CANONICAL_ROOT = get_hermes_home() / "runtime" / "shared-state"
_DEFAULT_VM_CANONICAL_ROOT = (
    Path.home() / "Mounts" / "mini_root" / ".hermes" / "shared-state"
)
_TASK_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_RCA_SOURCE_ID_RE = re.compile(r"^g1q3-rca-source-v1-[0-9a-f]{64}$")
_ALLOWED_LANES = {"fast", "standard", "heavy"}
_ALLOWED_RESOURCE_CLASSES = {"cpu", "io", "repo", "pnc_data", "network", "mixed"}
_ALLOWED_WORKSPACE_SCOPES = {
    "owner_main_repo",
    "user_worktree",
    "shared_nested_repo",
    "none",
    "unknown",
}
_ALLOWED_RISK_CLASSES = {"low", "normal", "high"}
_ALLOWED_EXECUTOR_TYPES = {"coding_agent", "direct_cli", "governed_tool"}
_ALLOWED_AGENT_BACKENDS = {"codex", "openclaw", "none"}
_SAFE_PATH_SEGMENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_REPO_SCOPE_RE = _SAFE_PATH_SEGMENT_RE
_VM_ARTIFACT_PREFIX = "/mnt/tmp/"
_CIFS_ARTIFACT_PREFIX = (
    "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/"
)
_VM_PATH_CONTRACT = """

---
VM path contract:
- Code/repos live under `/home/mini/<repo>` for owner/admin main repositories, or `/home/mini/worktrees/<repo>/<user>` for member/senior isolated worktrees.
- Shared-state execution truth lives under `/home/mini/.hermes/shared-state`; legacy/OpenClaw bridge state may appear under `/home/mini/tmp/openclaw-shared-state`.
- Default task data, downloads, conversion intermediates, caches, raw packages, and generated artifacts must go under `/mnt/tmp/<task_id>/` and `/mnt/tmp/<task_id>/downloads`.
- For integration_tools mdrive4 work, generated artifacts also go under `/mnt/tmp/<task_id>/` (owner decision 2026-06-12); `/mnt/minieye/mdrive4` source data stays read-only by default.
- User-visible CIFS path for `/mnt/tmp/<task_id>/` is `//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/<task_id>/`.
- Mounted data/tool roots include `/mnt/evaluation_data`, `/mnt/pnc_tools`, and mdrive4 data at `/mnt/minieye/mdrive4`.
- Do not default task artifacts to `~/Downloads`, `/tmp`, repo source dirs, `~/.cache`, or old `/home/mini/nas/miniPan/tmp/...` unless explicitly requested.
- If the user asks where a download/output/path is, or says a path cannot be found, answer with both the VM-internal path and the user-visible/shareable path.
""".strip()


def _goal_with_vm_path_contract(goal: str) -> str:
    if "VM path contract:" in goal:
        return goal
    return f"{goal.rstrip()}\n\n{_VM_PATH_CONTRACT}\n"


def _rca_fixed_cli_goal(
    *,
    task_id: str,
    admission: dict[str, Any],
    execution_request: dict[str, Any],
) -> str:
    """Build the non-extensible goal contract consumed by the VM worker."""
    safe_task_id = str(task_id or "").strip()
    if not _TASK_ID_RE.fullmatch(safe_task_id):
        raise ValueError("RCA task id is not a safe shared-state path segment")
    admission_json = json.dumps(
        admission,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    request_json = json.dumps(
        execution_request,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    markers = (
        _RCA_ADMISSION_JSON_BEGIN,
        _RCA_ADMISSION_JSON_END,
        _RCA_EXECUTION_REQUEST_JSON_BEGIN,
        _RCA_EXECUTION_REQUEST_JSON_END,
    )
    if any(marker in admission_json or marker in request_json for marker in markers):
        raise ValueError("RCA contract data contains a reserved goal marker")
    goal_path = f"{_RCA_VM_TASK_ROOT}/{safe_task_id}/goal.md"
    return "\n".join([
        "# Governed G1Q3 RCA service request",
        "",
        "Execution contract:",
        "- Host/main is control plane only; VM worker owns execution truth.",
        "- executor: fixed-cli under VM worker",
        "- Coding-agent fallback is forbidden for this service task.",
        "- The fixed CLI must validate both canonical JSON contracts before work.",
        "",
        _VM_PATH_CONTRACT,
        "",
        _RCA_ADMISSION_JSON_BEGIN,
        admission_json,
        _RCA_ADMISSION_JSON_END,
        _RCA_EXECUTION_REQUEST_JSON_BEGIN,
        request_json,
        _RCA_EXECUTION_REQUEST_JSON_END,
        f"- cd {_RCA_VM_REPO_ROOT}",
        (
            f"- {_RCA_FIXED_CLI_RELATIVE_PATH} --task-id {safe_task_id} "
            f"--goal-path {goal_path}"
        ),
    ])


def build_rca_fixed_cli_goal(
    *,
    task_id: str,
    admission: dict[str, Any],
    execution_request: dict[str, Any],
) -> str:
    """Public deterministic builder shared by submission and release evidence."""
    return _rca_fixed_cli_goal(
        task_id=task_id,
        admission=admission,
        execution_request=execution_request,
    )


def _rca_expected_artifact_cache_bytes(toolchain: dict[str, Any]) -> int:
    """Read the capacity unit already admitted by the dispatcher."""
    storage_admission = toolchain.get("storage_admission")
    if not isinstance(storage_admission, dict):
        raise ValueError("storage_admission must be an object")
    if (
        storage_admission.get("schema_version") != _RCA_STORAGE_ADMISSION_SCHEMA_VERSION
        or storage_admission.get("status") != "pass"
    ):
        raise ValueError("storage_admission must be an admitted production summary")
    policy = storage_admission.get("policy")
    if not isinstance(policy, dict):
        raise ValueError("storage_admission.policy must be an object")
    value = policy.get("expected_derived_artifact_bytes_per_case")
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value <= 0
        or value > _RCA_MAX_EXPECTED_ARTIFACT_CACHE_BYTES
    ):
        raise ValueError(
            "storage_admission.policy.expected_derived_artifact_bytes_per_case "
            "must be a bounded positive integer"
        )
    return value


VM_TASK_SUBMIT_SCHEMA = {
    "name": "vm_task_submit",
    "description": (
        "Submit a long-running VM/business task to shared-state v2 so the VM worker executes it. "
        "Use this instead of direct ssh-mini-run / ssh-mini-agent write execution for Feishu VM tasks. "
        "The submitted goal is automatically appended with the VM path contract: code under /home/mini, "
        "artifacts under /mnt/tmp/<task_id>/, and user-visible CIFS paths under "
        "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/<task_id>/."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "title": {
                "type": "string",
                "description": "Short human-readable task title.",
            },
            "goal": {
                "type": "string",
                "description": (
                    "Full self-contained VM-visible task brief. Include repo, branch, user/worktree, "
                    "expected verification, output requirements, and any known user-facing artifact/download path. "
                    "The tool appends the canonical VM path contract automatically."
                ),
            },
            "task_id": {
                "type": "string",
                "description": "Optional explicit shared-state task id. Leave empty unless a coordinator must pre-reserve a completion-probe-visible id.",
            },
            "owner": {
                "type": "string",
                "description": "Optional requester label. Ignored in gateway sessions; trusted session identity is used instead.",
            },
            "lane": {
                "type": "string",
                "enum": ["fast", "standard", "heavy"],
                "description": "Optional VM scheduler lane metadata. Use heavy for PNC data/conversion/evaluation and long-running VM tasks.",
            },
            "resource_class": {
                "type": "string",
                "enum": ["cpu", "io", "repo", "pnc_data", "network", "mixed"],
                "description": "Optional VM scheduler resource-class metadata for slot/resource isolation.",
            },
            "repo_scope": {
                "type": "string",
                "description": "Optional VM scheduler repository scope, e.g. pnc_specs, minieye_dnp_nop, none, or unknown.",
            },
            "workspace_scope": {
                "type": "string",
                "enum": [
                    "owner_main_repo",
                    "user_worktree",
                    "shared_nested_repo",
                    "none",
                    "unknown",
                ],
                "description": "Optional VM scheduler workspace isolation metadata.",
            },
            "risk_class": {
                "type": "string",
                "enum": ["low", "normal", "high"],
                "description": "Optional VM scheduler risk metadata.",
            },
            "artifact_root": {
                "type": "string",
                "description": "Optional expected VM artifact root, preferably /mnt/tmp/<task_id>/.",
            },
            "artifact_cifs_root": {
                "type": "string",
                "description": "Optional user-visible CIFS artifact root corresponding to artifact_root.",
            },
            "executor_type": {
                "type": "string",
                "enum": ["coding_agent", "direct_cli", "governed_tool"],
                "description": (
                    "VM execution plane. Use coding_agent for repository/code understanding, edits, refactors, "
                    "debugging, and test-fix tasks; use direct_cli/governed_tool only for deterministic bounded "
                    "tool or wrapper tasks. Defaults to coding_agent."
                ),
            },
            "agent_backend": {
                "type": "string",
                "enum": ["codex", "openclaw", "none"],
                "description": "Coding-agent backend when executor_type=coding_agent. Defaults to codex.",
            },
            "codex_backend_enabled": {
                "type": "boolean",
                "description": "Enable VM-side Codex backend for coding_agent tasks. Defaults to true when agent_backend=codex.",
            },
        },
        "required": ["title", "goal"],
    },
}

VM_TASK_STATUS_SCHEMA = {
    "name": "vm_task_status",
    "description": (
        "Read shared-state v2 canonical status/result for a VM task by task_id. "
        "Use after vm_task_submit before telling the user a VM task was picked up or completed."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "task_id": {
                "type": "string",
                "description": "Shared-state v2 task id returned by vm_task_submit.",
            },
            "include_markdown": {
                "type": "boolean",
                "description": "Include status.md and result.md snippets when present. Defaults to true.",
            },
        },
        "required": ["task_id"],
    },
}


def _create_task_script() -> Path:
    return get_hermes_home() / "workspace-work" / "bin" / "create_task_v2.py"


def _host_completion_probe_script() -> Path:
    return Path(__file__).resolve().with_name("vm_task_completion_probe.py")


def _python_executable() -> str:
    return shutil.which("python3.11") or shutil.which("python3") or "python3"


def _validate_optional_enum(name: str, value: str, allowed: set[str]) -> str | None:
    if not value:
        return None
    if value not in allowed:
        return f"invalid {name}: {value!r}; expected one of {sorted(allowed)}"
    return None


def _validate_artifact_root(
    name: str, value: str, prefixes: str | tuple[str, ...]
) -> str | None:
    if not value:
        return None
    if isinstance(prefixes, str):
        allowed_prefixes = (prefixes,)
    else:
        allowed_prefixes = tuple(prefixes)
    matched_prefix = next(
        (prefix for prefix in allowed_prefixes if value.startswith(prefix)), ""
    )
    if not matched_prefix:
        allowed = " or ".join(f"{prefix}<task_id>/" for prefix in allowed_prefixes)
        return f"invalid {name}: must be under {allowed}"
    relative = value[len(matched_prefix) :]
    if not relative:
        return f"invalid {name}: must include task id under {matched_prefix}"
    parts = [part for part in relative.split("/") if part]
    if not parts:
        return f"invalid {name}: must include task id under {matched_prefix}"
    if any(
        part in {".", ".."} or not _SAFE_PATH_SEGMENT_RE.match(part) for part in parts
    ):
        return f"invalid {name}: path segments must be safe labels without traversal"
    return None


def _build_scheduler_meta(
    *,
    lane: str = "",
    resource_class: str = "",
    repo_scope: str = "",
    workspace_scope: str = "",
    risk_class: str = "",
    artifact_root: str = "",
    artifact_cifs_root: str = "",
    executor_type: str = "",
    agent_backend: str = "",
    codex_backend_enabled: bool | None = None,
    allow_rca_prod_service: bool = False,
) -> tuple[dict[str, Any], str | None]:
    values = {
        "lane": str(lane or "").strip(),
        "resource_class": str(resource_class or "").strip(),
        "repo_scope": str(repo_scope or "").strip(),
        "workspace_scope": str(workspace_scope or "").strip(),
        "risk_class": str(risk_class or "").strip(),
        "artifact_root": str(artifact_root or "").strip(),
        "artifact_cifs_root": str(artifact_cifs_root or "").strip(),
        "executor_type": str(executor_type or "coding_agent").strip(),
        "agent_backend": str(agent_backend or "codex").strip(),
    }
    resource_classes = set(_ALLOWED_RESOURCE_CLASSES)
    if allow_rca_prod_service:
        resource_classes.add("rca_prod")
    checks = (
        ("lane", values["lane"], _ALLOWED_LANES),
        ("resource_class", values["resource_class"], resource_classes),
        ("workspace_scope", values["workspace_scope"], _ALLOWED_WORKSPACE_SCOPES),
        ("risk_class", values["risk_class"], _ALLOWED_RISK_CLASSES),
        ("executor_type", values["executor_type"], _ALLOWED_EXECUTOR_TYPES),
        ("agent_backend", values["agent_backend"], _ALLOWED_AGENT_BACKENDS),
    )
    for name, value, allowed in checks:
        error = _validate_optional_enum(name, value, allowed)
        if error:
            return {}, error
    if values["agent_backend"] == "none" and values["executor_type"] != "direct_cli":
        return (
            {},
            "invalid agent_backend: none is only valid for executor_type=direct_cli",
        )
    if values["repo_scope"] and not _REPO_SCOPE_RE.match(values["repo_scope"]):
        return (
            {},
            "invalid repo_scope: must be a safe repository label, not a filesystem path",
        )
    artifact_root_error = _validate_artifact_root(
        "artifact_root", values["artifact_root"], (_VM_ARTIFACT_PREFIX,)
    )
    if artifact_root_error:
        return {}, artifact_root_error
    cifs_root_error = _validate_artifact_root(
        "artifact_cifs_root", values["artifact_cifs_root"], _CIFS_ARTIFACT_PREFIX
    )
    if cifs_root_error:
        return {}, cifs_root_error
    meta: dict[str, Any] = {k: v for k, v in values.items() if v}
    if values["executor_type"] == "coding_agent" and values["agent_backend"] == "codex":
        meta["codex_backend_enabled"] = (
            True if codex_backend_enabled is None else bool(codex_backend_enabled)
        )
    elif codex_backend_enabled is not None:
        meta["codex_backend_enabled"] = bool(codex_backend_enabled)
    return meta, None


def vm_task_submit(
    title: str,
    goal: str,
    task_id: str = "",
    owner: str = "",
    user_id: str = "",
    lane: str = "",
    resource_class: str = "",
    repo_scope: str = "",
    workspace_scope: str = "",
    risk_class: str = "",
    artifact_root: str = "",
    artifact_cifs_root: str = "",
    executor_type: str = "",
    agent_backend: str = "",
    codex_backend_enabled: bool | None = None,
) -> Dict[str, Any]:
    """Create and bridge-deliver a shared-state v2 task for VM worker pickup."""
    title = str(title or "").strip()
    goal = str(goal or "").strip()
    if _is_reserved_rca_service_submission(
        title=title,
        goal=goal,
        task_id=task_id,
        artifact_root=artifact_root,
        artifact_cifs_root=artifact_cifs_root,
    ):
        return _reserved_rca_service_payload()
    trusted_user_name, trusted_user_id = _resolve_submitter(user_id, owner)
    if trusted_user_name or trusted_user_id:
        permission_error = _check_vm_task_permission(
            trusted_user_name, trusted_user_id, risk_class
        )
        if permission_error:
            return permission_error
        owner = trusted_user_name or trusted_user_id
    else:
        owner = str(owner or "").strip()
    return _vm_task_submit_trusted(
        title=title,
        goal=goal,
        task_id=task_id,
        owner=owner,
        lane=lane,
        resource_class=resource_class,
        repo_scope=repo_scope,
        workspace_scope=workspace_scope,
        risk_class=risk_class,
        artifact_root=artifact_root,
        artifact_cifs_root=artifact_cifs_root,
        executor_type=executor_type,
        agent_backend=agent_backend,
        codex_backend_enabled=codex_backend_enabled,
    )


def _vm_task_submit_trusted(
    *,
    title: str,
    goal: str,
    task_id: str = "",
    owner: str = "",
    lane: str = "",
    resource_class: str = "",
    repo_scope: str = "",
    workspace_scope: str = "",
    risk_class: str = "",
    artifact_root: str = "",
    artifact_cifs_root: str = "",
    executor_type: str = "",
    agent_backend: str = "",
    codex_backend_enabled: bool | None = None,
    routing_meta_extra: dict[str, Any] | None = None,
    create_once: bool = False,
    create_task_script: Path | None = None,
    rca_prod_service_receipt: dict[str, Any] | None = None,
    rca_prod_workspace_runtime: Any | None = None,
) -> Dict[str, Any]:
    """Create a task after a public-human or dedicated-service gate allowed it."""

    title = str(title or "").strip()
    goal = str(goal or "").strip()
    owner = str(owner or "").strip()
    if not title:
        return {"success": False, "error": "title is required"}
    if not goal:
        return {"success": False, "error": "goal is required"}
    task_id = str(task_id or "").strip()
    if task_id and not _TASK_ID_RE.match(task_id):
        return {
            "success": False,
            "error": "invalid task_id: must be a safe shared-state task id",
        }
    is_rca_prod = str(resource_class or "").strip() == "rca_prod"
    extra_receipt = (
        routing_meta_extra.get("rca_prod_admission_receipt")
        if isinstance(routing_meta_extra, dict)
        else None
    )
    if is_rca_prod and (
        not isinstance(rca_prod_service_receipt, dict)
        or extra_receipt != rca_prod_service_receipt
        or not isinstance(rca_prod_workspace_runtime, WorkspaceRuntimeIdentity)
    ):
        return _reserved_rca_service_payload()
    if not is_rca_prod and (
        rca_prod_service_receipt is not None or rca_prod_workspace_runtime is not None
    ):
        return _reserved_rca_service_payload()
    scheduler_meta, scheduler_meta_error = _build_scheduler_meta(
        lane=lane,
        resource_class=resource_class,
        repo_scope=repo_scope,
        workspace_scope=workspace_scope,
        risk_class=risk_class,
        artifact_root=artifact_root,
        artifact_cifs_root=artifact_cifs_root,
        executor_type=executor_type,
        agent_backend=agent_backend,
        codex_backend_enabled=codex_backend_enabled,
        allow_rca_prod_service=is_rca_prod and rca_prod_service_receipt is not None,
    )
    if scheduler_meta_error:
        return {"success": False, "error": scheduler_meta_error}
    goal = _goal_with_vm_path_contract(goal)

    create_task = create_task_script or _create_task_script()
    if not create_task.exists():
        return {
            "success": False,
            "error": f"create_task_v2.py not found: {create_task}",
        }

    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", suffix=".md", delete=False
    ) as f:
        f.write(goal)
        goal_file = f.name

    # Capture session routing context for Feishu delivery
    routing_meta = {}
    routing_env_map = {
        "platform": "HERMES_SESSION_PLATFORM",
        "chat_id": "HERMES_SESSION_CHAT_ID",
        "chat_name": "HERMES_SESSION_CHAT_NAME",
        "thread_id": "HERMES_SESSION_THREAD_ID",
        # Feishu topic completion/progress relays need the original message id as
        # a reply anchor.  thread_id alone can identify the topic, but carrying
        # message_id through shared-state metadata makes the route auditable and
        # lets future relays fail closed instead of falling back to the parent
        # group when a topic anchor is lost.
        "message_id": "HERMES_SESSION_MESSAGE_ID",
        "user_id": "HERMES_SESSION_USER_ID",
        "user_name": "HERMES_SESSION_USER_NAME",
        "session_key": "HERMES_SESSION_KEY",
    }
    for meta_key, env_key in routing_env_map.items():
        value = _session_value(env_key)
        if value:
            routing_meta[meta_key] = value
    if routing_meta_extra:
        routing_meta.update(routing_meta_extra)
    routing_meta.update(scheduler_meta)

    cmd = [
        _python_executable(),
        str(create_task),
        "--title",
        title,
        "--goal-file",
        goal_file,
        "--bridge-root",
        str(_DEFAULT_BRIDGE_ROOT),
        "--deliver-bridge",
        "--json",
    ]
    if owner:
        cmd.extend(["--owner", owner])
    if task_id:
        cmd.extend(["--task-id", task_id])
    if routing_meta:
        cmd.extend(["--meta", json.dumps(routing_meta, ensure_ascii=False)])
    if create_once:
        cmd.append("--create-once")

    try:
        if is_rca_prod:
            try:
                final_workspace_runtime = validate_workspace_runtime()
            except WorkspaceRuntimeError as exc:
                return {
                    **_vm_task_service_denied_payload(
                        "vm_task_service_workspace_runtime_invalid",
                        f"fixed RCA workspace runtime changed at create boundary: {exc.code}",
                    ),
                    "retryable": True,
                    "create_suppressed": True,
                }
            if (
                final_workspace_runtime != rca_prod_workspace_runtime
                or create_task != final_workspace_runtime.creator_path
            ):
                return {
                    **_vm_task_service_denied_payload(
                        "vm_task_service_workspace_runtime_drift",
                        "fixed RCA workspace runtime identity drifted at create boundary",
                    ),
                    "retryable": True,
                    "create_suppressed": True,
                    "expected_workspace_runtime": rca_prod_workspace_runtime.to_dict(),
                    "observed_workspace_runtime": final_workspace_runtime.to_dict(),
                }
        proc = subprocess.run(cmd, text=True, capture_output=True, timeout=120)
    except FileNotFoundError as exc:
        return {
            "success": False,
            "error": f"failed to launch task creator: {exc}",
            "returncode": None,
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "success": False,
            "error_code": "vm_task_creation_timeout",
            "error": "task creation timed out",
            "retryable": True,
            "returncode": None,
            "stdout": (exc.stdout or "").strip() if isinstance(exc.stdout, str) else "",
            "stderr": (exc.stderr or "").strip() if isinstance(exc.stderr, str) else "",
        }
    except Exception as exc:
        return {
            "success": False,
            "error": f"task creation failed: {type(exc).__name__}: {exc}",
            "returncode": None,
        }
    finally:
        try:
            Path(goal_file).unlink(missing_ok=True)
        except Exception:
            pass

    raw = (proc.stdout or "").strip()
    try:
        parsed = json.loads(raw) if raw else {}
    except Exception:
        parsed = {"raw_stdout": raw}

    notify_process: dict[str, Any] = {}
    if proc.returncode == 0:
        task_id = str(parsed.get("task_id") or parsed.get("id") or "").strip()
        notify_process = _spawn_completion_probe_background(task_id)

    return {
        "success": proc.returncode == 0,
        "returncode": proc.returncode,
        "task": parsed,
        "stderr": (proc.stderr or "").strip(),
        "notify_process": notify_process,
        "routing": {
            "host_state": "host-created" if proc.returncode == 0 else "failed",
            "delivery_attempted": True,
            "bridge_root": str(_DEFAULT_BRIDGE_ROOT),
            "next_truth_checks": [
                "confirm task appears in VM canonical queue before saying delivered-to-VM",
                "confirm VM worker claim before saying picked-up",
                "use canonical status reader/result import for completion truth",
            ],
        },
    }


def vm_task_submit_service(
    *,
    service_id: str,
    capability: str,
    operation: str,
    admission: Any,
    execution_request: Any,
    reconcile_only: bool = False,
    capacity_mode: str = "",
    bootstrap_epoch_id: str = "",
    release_bom_sha256: str = "",
    bootstrap_started_at: str = "",
    bootstrap_deadline: str = "",
    bootstrap_authorization_fingerprint: str = "",
    active_release_binding_sha256: str = "",
) -> Dict[str, Any]:
    """Submit one capability-scoped RCA intake without exposing general VM execution.

    This function is intentionally not registered as an agent tool.  It accepts
    no caller-controlled goal, scheduler lane, repository scope, risk class, or
    executor.  A trusted adapter must provide contracts whose issue identities
    agree, and the admission key becomes the stable shared-state task id.
    """

    normalized_service = str(service_id or "").strip()
    normalized_capability = str(capability or "").strip()
    normalized_operation = str(operation or "").strip()
    normalized_capacity_mode = str(capacity_mode or "").strip()
    normalized_bootstrap_epoch = str(bootstrap_epoch_id or "").strip()
    normalized_release_bom = str(release_bom_sha256 or "").strip().lower()
    normalized_bootstrap_started = str(bootstrap_started_at or "").strip()
    normalized_bootstrap_deadline = str(bootstrap_deadline or "").strip()
    normalized_bootstrap_fingerprint = str(
        bootstrap_authorization_fingerprint or ""
    ).strip().lower()
    normalized_active_release_binding = str(
        active_release_binding_sha256 or ""
    ).strip().lower()
    if normalized_capacity_mode not in {"steady", "bootstrap"}:
        return _vm_task_service_denied_payload(
            "vm_task_service_capacity_mode_invalid",
            "capacity_mode must be explicitly steady or bootstrap",
        )
    if normalized_capacity_mode == "steady" and (
        normalized_bootstrap_epoch
        or normalized_release_bom
        or normalized_bootstrap_started
        or normalized_bootstrap_deadline
        or normalized_bootstrap_fingerprint
        or normalized_active_release_binding
    ):
        return _vm_task_service_denied_payload(
            "vm_task_service_capacity_mode_invalid",
            "steady capacity mode cannot carry bootstrap release bindings",
        )
    if normalized_capacity_mode == "bootstrap" and (
        not normalized_bootstrap_epoch
        or not re.fullmatch(r"[0-9a-f]{64}", normalized_release_bom)
        or not normalized_bootstrap_started
        or not normalized_bootstrap_deadline
        or not re.fullmatch(r"[0-9a-f]{64}", normalized_bootstrap_fingerprint)
        or not re.fullmatch(r"[0-9a-f]{64}", normalized_active_release_binding)
    ):
        return _vm_task_service_denied_payload(
            "vm_task_service_bootstrap_binding_invalid",
            "bootstrap capacity mode requires an epoch id and release BOM SHA",
        )
    if not isinstance(reconcile_only, bool):
        return _vm_task_service_denied_payload(
            "vm_task_service_request_invalid",
            "reconcile_only must be boolean",
        )
    if normalized_operation != _RCA_SERVICE_OPERATION:
        return _vm_task_service_denied_payload(
            "vm_task_service_operation_denied",
            "service operation is not the fixed G1Q3 RCA issue intake operation",
        )
    permission_error = _check_vm_task_service_permission(
        normalized_service, normalized_capability
    )
    if permission_error:
        return permission_error

    raw_legacy_violation = find_rca_legacy_download_violation(execution_request)
    if raw_legacy_violation:
        return _vm_task_service_denied_payload(
            "vm_task_service_request_invalid",
            (
                "legacy MDI/download controls are forbidden in the RCA execution "
                f"request ({raw_legacy_violation})"
            ),
        )
    if _contains_rca_reserved_goal_marker(execution_request):
        return _vm_task_service_denied_payload(
            "vm_task_service_request_invalid",
            "execution request contains a reserved goal marker",
        )

    try:
        from gateway.pnc_rca_admission import validate_rca_admission

        validated_admission = validate_rca_admission(admission)
    except Exception as exc:
        return _vm_task_service_denied_payload(
            "vm_task_service_admission_invalid",
            f"invalid RCA admission: {exc}",
        )

    try:
        from gateway.pnc_rca_schema import (
            RCA_EXECUTION_REQUEST_SCHEMA_VERSION,
            to_dict as rca_to_dict,
        )

        request_payload = rca_to_dict(execution_request)
    except Exception as exc:
        return _vm_task_service_denied_payload(
            "vm_task_service_request_invalid",
            f"invalid RCA execution request: {exc}",
        )
    if (
        request_payload.get("schema_version") != RCA_EXECUTION_REQUEST_SCHEMA_VERSION
        or request_payload.get("request_kind") != "issue_intake"
    ):
        return _vm_task_service_denied_payload(
            "vm_task_service_request_invalid",
            "service submission requires a G1Q3 RCA issue_intake execution request",
        )
    allowed_request_fields = {
        "schema_version",
        "request_kind",
        "work_item",
        "case",
        "data",
        "evidence",
        "execution_policy",
        "source_refs",
        "toolchain",
    }
    if set(request_payload) - allowed_request_fields:
        return _vm_task_service_denied_payload(
            "vm_task_service_request_invalid",
            "execution request contains fields outside the fixed RCA contract",
        )

    work_item = request_payload.get("work_item")
    if not isinstance(work_item, dict):
        work_item = {}
    refs = validated_admission.source_refs
    request_type = str(
        work_item.get("work_item_type_key") or work_item.get("work_item_type") or ""
    ).strip()
    if (
        str(work_item.get("project_key") or "").strip() != refs.project_key
        or str(work_item.get("work_item_id") or "").strip() != refs.work_item_id
        or not request_type
        or request_type != refs.work_item_type_key
    ):
        return _vm_task_service_denied_payload(
            "vm_task_service_request_identity_mismatch",
            "execution request work item does not match the validated admission",
        )

    data = (
        request_payload.get("data")
        if isinstance(request_payload.get("data"), dict)
        else {}
    )
    policy = (
        request_payload.get("execution_policy")
        if isinstance(request_payload.get("execution_policy"), dict)
        else {}
    )
    data_access_error = _validate_rca_remote_data_access(data, policy, request_payload)
    if data_access_error:
        return _vm_task_service_denied_payload(
            "vm_task_service_request_invalid",
            data_access_error,
        )
    data_artifact_root = str(data.get("artifact_root") or "").strip()
    policy_artifact_root = str(policy.get("artifact_root") or "").strip()
    if (
        data_artifact_root
        and policy_artifact_root
        and data_artifact_root != policy_artifact_root
    ):
        return _vm_task_service_denied_payload(
            "vm_task_service_request_invalid",
            "execution request contains conflicting artifact roots",
        )
    artifact_root = data_artifact_root or policy_artifact_root
    artifact_cifs_root = str(data.get("artifact_cifs_root") or "").strip()
    source_refs = (
        request_payload.get("source_refs")
        if isinstance(request_payload.get("source_refs"), dict)
        else {}
    )
    origin_source_id = str(source_refs.get("origin_source_id") or "").strip()
    expected_source_refs = {
        "task_id": validated_admission.submission_key,
        "source_kind": (
            "kafka_workflow_event"
            if validated_admission.trigger_kind == "issue_created"
            else "feishu_group_manual"
        ),
        "origin_source_id": origin_source_id,
        "rule_version": refs.rule_version,
        "generation": validated_admission.generation,
        "business_key": validated_admission.business_key,
        "submission_key": validated_admission.submission_key,
    }
    if validated_admission.trigger_kind == "issue_created":
        expected_source_refs.update(
            {
                "source_event_id": (f"{refs.topic}:{refs.partition}:{refs.offset}"),
                "topic": refs.topic,
                "partition": refs.partition,
                "offset": refs.offset,
            }
        )
    elif (
        validated_admission.trigger_kind
        not in {"manual_issue_request", "manual_retrigger"}
        or refs.topic != ""
        or refs.partition is not None
        or refs.offset is not None
    ):
        return _vm_task_service_denied_payload(
            "vm_task_service_admission_invalid",
            "manual execution admission contains Kafka source coordinates",
        )
    if (
        not _RCA_SOURCE_ID_RE.fullmatch(origin_source_id)
        or source_refs != expected_source_refs
    ):
        return _vm_task_service_denied_payload(
            "vm_task_service_request_identity_mismatch",
            "execution request origin source identity does not match the validated admission",
        )
    expected_artifact_root = (
        f"{_VM_ARTIFACT_PREFIX}{validated_admission.submission_key}/"
    )
    expected_cifs_root = f"{_CIFS_ARTIFACT_PREFIX}{validated_admission.submission_key}/"
    if (
        artifact_root != expected_artifact_root
        or artifact_cifs_root != expected_cifs_root
    ):
        return _vm_task_service_denied_payload(
            "vm_task_service_request_identity_mismatch",
            "execution request task/artifact identity does not match the validated admission",
        )

    admission_payload = validated_admission.to_dict()
    toolchain = (
        request_payload.get("toolchain")
        if isinstance(request_payload.get("toolchain"), dict)
        else {}
    )
    reservation_receipt = toolchain.get("derived_capacity_reservation")
    if not isinstance(reservation_receipt, dict):
        return _vm_task_service_denied_payload(
            "vm_task_service_reservation_invalid",
            "execution request is missing the atomic derived-capacity reservation receipt",
        )
    try:
        from gateway.pnc_rca_derived_capacity_reservation import (
            DerivedCapacityReservationError,
            DerivedCapacityReservationRequest,
            canonical_data_access_sha256,
            validate_derived_capacity_reservation_receipt,
        )
    except Exception as exc:
        return _vm_task_service_denied_payload(
            "vm_task_service_reservation_invalid",
            f"atomic derived-capacity reservation validator is unavailable: {type(exc).__name__}",
        )
    try:
        expected_artifact_cache_bytes = _rca_expected_artifact_cache_bytes(toolchain)
    except ValueError as exc:
        return _vm_task_service_denied_payload(
            "vm_task_service_reservation_invalid",
            f"storage admission capacity contract is invalid: {exc}",
        )
    try:
        reservation_request = DerivedCapacityReservationRequest(
            submission_key=validated_admission.submission_key,
            task_id=validated_admission.submission_key,
            business_key=validated_admission.business_key,
            data_access_sha256=canonical_data_access_sha256(data["data_access"]),
            artifact_root=artifact_root,
            expected_artifact_cache_bytes=expected_artifact_cache_bytes,
        )
        reservation_decision = validate_derived_capacity_reservation_receipt(
            reservation_receipt,
            reservation_request,
        )
    except DerivedCapacityReservationError as exc:
        return _vm_task_service_denied_payload(
            "vm_task_service_reservation_invalid",
            f"atomic derived-capacity reservation failed validation: {exc.code}",
        )
    except Exception as exc:
        return _vm_task_service_denied_payload(
            "vm_task_service_reservation_invalid",
            f"atomic derived-capacity reservation validation failed: {type(exc).__name__}",
        )
    if reservation_decision.reconcile_only is not reconcile_only:
        return _vm_task_service_denied_payload(
            "vm_task_service_reservation_reconcile_mismatch",
            "reconcile_only does not match the derived-capacity reservation lifecycle",
        )
    freshness_error = _rca_reservation_freshness_error(
        reservation_decision.receipt,
        admitted=reservation_decision.admitted,
    )
    if freshness_error:
        return {
            **_vm_task_service_denied_payload(
                "vm_task_service_reservation_stale",
                freshness_error,
            ),
            "retryable": reservation_decision.admitted,
        }
    if not reconcile_only and not reservation_decision.admitted:
        return _vm_task_service_denied_payload(
            "vm_task_service_reservation_not_admitted",
            "derived-capacity reservation is not admitted for task creation",
        )

    try:
        workspace_runtime = validate_workspace_runtime()
    except WorkspaceRuntimeError as exc:
        return {
            **_vm_task_service_denied_payload(
                "vm_task_service_workspace_runtime_invalid",
                f"fixed RCA workspace runtime is unavailable: {exc.code}",
            ),
            "retryable": True,
        }

    title = f"G1Q3 RCA issue intake: {refs.work_item_id}"
    contract_sha256 = _rca_contract_sha256(admission_payload, request_payload)
    fixed_execution_meta = {
        "lane": "heavy",
        "resource_class": "rca_prod",
        "risk_class": "high",
        "executor_type": "direct_cli",
        "agent_backend": "none",
        "codex_backend_enabled": False,
        "coding_agent_fallback_enabled": False,
        "fixed_cli_entrypoint": (
            f"{_RCA_VM_REPO_ROOT}/{_RCA_FIXED_CLI_RELATIVE_PATH.removeprefix('./')}"
        ),
    }
    base_identity_meta = {
        "actor_kind": "service",
        "business_line": "g1q3_rca",
        "service_capability": normalized_capability,
        "service_operation": normalized_operation,
        "rca_business_key": validated_admission.business_key,
        "rca_submission_key": validated_admission.submission_key,
        "rca_generation": validated_admission.generation,
        "rca_trigger_kind": validated_admission.trigger_kind,
        "rca_create_once": True,
        "rca_contract_sha256": contract_sha256,
        "rca_data_access_mode": "remote_read",
        "rca_source_refs": admission_payload["source_refs"],
        "artifact_root": artifact_root,
        "artifact_cifs_root": artifact_cifs_root,
        **workspace_runtime.task_meta(),
        **fixed_execution_meta,
    }
    try:
        goal = _rca_fixed_cli_goal(
            task_id=validated_admission.submission_key,
            admission=admission_payload,
            execution_request=request_payload,
        )
    except (TypeError, ValueError) as exc:
        return _vm_task_service_denied_payload(
            "vm_task_service_request_invalid",
            f"fixed CLI goal contract could not be built: {type(exc).__name__}",
        )
    reservation_identity = reservation_decision.receipt.get("reservation")
    reservation_identity = (
        reservation_identity if isinstance(reservation_identity, dict) else {}
    )
    reservation_id = str(
        reservation_decision.receipt.get("reservation_id")
        or reservation_identity.get("reservation_id")
        or ""
    ).strip()
    reservation_fence = str(
        reservation_decision.receipt.get("fence")
        if reservation_decision.receipt.get("fence") is not None
        else reservation_identity.get("fence")
    ).strip()
    reservation_contract_sha256 = str(
        reservation_decision.receipt.get("contract_sha256")
        or reservation_identity.get("contract_sha256")
        or ""
    ).strip()
    try:
        stable_rca_prod_meta = {
            "resource_class": "rca_prod",
            "lane": "heavy",
            "queue_if_blocked": False,
            "resource_gate_bypass": False,
            "reservation_id": reservation_id,
            "reservation_fence": reservation_fence,
            "reservation_contract_sha256": reservation_contract_sha256,
            "rca_prod_goal_sha256": rca_prod_goal_sha256(goal),
            "rca_prod_command_sha256": rca_prod_command_sha256(
                build_rca_prod_command_argv(validated_admission.submission_key)
            ),
            "rca_prod_contract_sha256": contract_sha256,
        }
    except RcaProdAdmissionError as exc:
        return _vm_task_service_denied_payload(
            "vm_task_service_rca_prod_command_invalid",
            f"fixed RCA VM command contract is invalid: {exc.code}",
        )
    if normalized_capacity_mode == "bootstrap":
        stable_rca_prod_meta.update(
            {
                "rca_prod_capacity_mode": "bootstrap",
                "rca_prod_bootstrap_epoch_id": normalized_bootstrap_epoch,
                "rca_prod_bootstrap_started_at": normalized_bootstrap_started,
                "rca_prod_bootstrap_deadline": normalized_bootstrap_deadline,
                "rca_prod_bootstrap_authorization_fingerprint": (
                    normalized_bootstrap_fingerprint
                ),
                "rca_prod_release_bom_sha256": normalized_release_bom,
                "rca_prod_active_release_binding_sha256": (
                    normalized_active_release_binding
                ),
            }
        )
    identity_meta = {**base_identity_meta, **stable_rca_prod_meta}
    try:
        existing = vm_task_status(
            validated_admission.submission_key, include_markdown=False
        )
    except Exception as exc:
        return {
            "success": False,
            "error_code": "vm_task_service_dedupe_status_unavailable",
            "error": f"cannot reconcile existing RCA submission before create: {exc}",
            "retryable": True,
            "returncode": None,
            "admission": admission_payload,
        }
    if existing.get("success") is True:
        identity_error = _rca_existing_identity_error(
            existing,
            task_id=validated_admission.submission_key,
            title=title,
            owner=normalized_service,
            expected_meta=identity_meta,
        )
        if identity_error:
            return {
                **_vm_task_service_denied_payload(
                    "vm_task_service_existing_identity_conflict",
                    f"existing stable task id has conflicting RCA identity fields: {identity_error}",
                ),
                "existing_status": existing,
                "admission": admission_payload,
            }
        try:
            validate_existing_rca_prod_meta(
                existing.get("meta"),
                task_id=validated_admission.submission_key,
                goal=goal,
                contract_sha256=contract_sha256,
                reservation_id=reservation_id,
                reservation_fence=reservation_fence,
                reservation_contract_sha256=reservation_contract_sha256,
                capacity_mode=normalized_capacity_mode,
                bootstrap_epoch_id=normalized_bootstrap_epoch,
                release_bom_sha256=normalized_release_bom,
                bootstrap_started_at=normalized_bootstrap_started,
                bootstrap_deadline=normalized_bootstrap_deadline,
                bootstrap_authorization_fingerprint=(
                    normalized_bootstrap_fingerprint
                ),
                active_release_binding_sha256=(normalized_active_release_binding),
            )
        except RcaProdAdmissionError as exc:
            return {
                **_vm_task_service_denied_payload(
                    "vm_task_service_existing_identity_conflict",
                    f"existing stable task id has invalid RCA production admission: {exc.code}",
                ),
                "existing_status": existing,
                "admission": admission_payload,
            }
        return {
            "success": True,
            "deduped": True,
            "created": False,
            "task": {
                "task_id": validated_admission.submission_key,
                "state": existing.get("state", "unknown"),
            },
            "existing_status": existing,
            "admission": admission_payload,
            "workspace_runtime": workspace_runtime.to_dict(),
        }
    if existing.get("state") != "missing":
        return {
            "success": False,
            "error_code": "vm_task_service_dedupe_status_unavailable",
            "error": "cannot prove RCA submission is missing; create is suppressed",
            "retryable": True,
            "returncode": None,
            "existing_status": existing,
            "admission": admission_payload,
        }
    if reconcile_only:
        return {
            "success": False,
            "error_code": "vm_task_service_reconcile_missing",
            "error": (
                "released derived-capacity reservation permits reconciliation only; "
                "the stable RCA task is missing and create is suppressed"
            ),
            "retryable": False,
            "returncode": None,
            "reconcile_only": True,
            "create_suppressed": True,
            "existing_status": existing,
            "admission": admission_payload,
            "workspace_runtime": workspace_runtime.to_dict(),
        }
    try:
        create_workspace_runtime = validate_workspace_runtime()
    except WorkspaceRuntimeError as exc:
        return {
            **_vm_task_service_denied_payload(
                "vm_task_service_workspace_runtime_invalid",
                f"fixed RCA workspace runtime changed before create: {exc.code}",
            ),
            "retryable": True,
            "workspace_runtime": workspace_runtime.to_dict(),
        }
    if create_workspace_runtime != workspace_runtime:
        return {
            **_vm_task_service_denied_payload(
                "vm_task_service_workspace_runtime_drift",
                "fixed RCA workspace runtime identity drifted before create",
            ),
            "retryable": True,
            "workspace_runtime": workspace_runtime.to_dict(),
            "observed_workspace_runtime": create_workspace_runtime.to_dict(),
        }
    try:
        prod_admission = issue_rca_prod_admission(
            task_id=validated_admission.submission_key,
            submission_key=validated_admission.submission_key,
            goal=goal,
            contract_sha256=contract_sha256,
            reservation_id=reservation_id,
            reservation_fence=reservation_fence,
            reservation_contract_sha256=reservation_contract_sha256,
            capacity_mode=normalized_capacity_mode,
            bootstrap_epoch_id=normalized_bootstrap_epoch,
            release_bom_sha256=normalized_release_bom,
            bootstrap_started_at=normalized_bootstrap_started,
            bootstrap_deadline=normalized_bootstrap_deadline,
            bootstrap_authorization_fingerprint=normalized_bootstrap_fingerprint,
            active_release_binding_sha256=normalized_active_release_binding,
        )
    except RcaProdAdmissionError as exc:
        return {
            **_vm_task_service_denied_payload(
                "vm_task_service_rca_prod_admission_blocked",
                f"RCA production admission rejected: {exc.code}",
            ),
            "retryable": exc.retryable,
            "admission_reason": exc.code,
            "create_suppressed": True,
        }
    try:
        post_admission_workspace_runtime = validate_workspace_runtime()
    except WorkspaceRuntimeError as exc:
        return {
            **_vm_task_service_denied_payload(
                "vm_task_service_workspace_runtime_invalid",
                f"fixed RCA workspace runtime changed after production admission: {exc.code}",
            ),
            "retryable": True,
            "create_suppressed": True,
            "workspace_runtime": workspace_runtime.to_dict(),
        }
    if post_admission_workspace_runtime != workspace_runtime:
        return {
            **_vm_task_service_denied_payload(
                "vm_task_service_workspace_runtime_drift",
                "fixed RCA workspace runtime identity drifted during production admission",
            ),
            "retryable": True,
            "create_suppressed": True,
            "workspace_runtime": workspace_runtime.to_dict(),
            "observed_workspace_runtime": post_admission_workspace_runtime.to_dict(),
        }
    create_identity_meta = {**identity_meta, **prod_admission.meta}
    result = _vm_task_submit_trusted(
        title=title,
        goal=goal,
        task_id=validated_admission.submission_key,
        owner=normalized_service,
        lane="heavy",
        resource_class="rca_prod",
        repo_scope="unknown",
        workspace_scope="none",
        risk_class="high",
        artifact_root=artifact_root,
        artifact_cifs_root=artifact_cifs_root,
        executor_type="direct_cli",
        agent_backend="none",
        codex_backend_enabled=False,
        routing_meta_extra=create_identity_meta,
        create_once=True,
        create_task_script=post_admission_workspace_runtime.creator_path,
        rca_prod_service_receipt=prod_admission.receipt,
        rca_prod_workspace_runtime=post_admission_workspace_runtime,
    )
    try:
        reconciled = vm_task_status(
            validated_admission.submission_key, include_markdown=False
        )
    except Exception as exc:
        reconciled = {
            "success": False,
            "state": "unknown",
            "error": f"post-submit status reconciliation failed: {type(exc).__name__}: {exc}",
        }
    if reconciled.get("success") is True:
        identity_error = _rca_existing_identity_error(
            reconciled,
            task_id=validated_admission.submission_key,
            title=title,
            owner=normalized_service,
            expected_meta=identity_meta,
        )
        if identity_error:
            return {
                **_vm_task_service_denied_payload(
                    "vm_task_service_existing_identity_conflict",
                    f"reconciled stable task id has conflicting RCA identity fields: {identity_error}",
                ),
                "existing_status": reconciled,
                "submit_result": result,
                "admission": admission_payload,
            }
        try:
            validate_existing_rca_prod_meta(
                reconciled.get("meta"),
                task_id=validated_admission.submission_key,
                goal=goal,
                contract_sha256=contract_sha256,
                reservation_id=reservation_id,
                reservation_fence=reservation_fence,
                reservation_contract_sha256=reservation_contract_sha256,
                capacity_mode=normalized_capacity_mode,
                bootstrap_epoch_id=normalized_bootstrap_epoch,
                release_bom_sha256=normalized_release_bom,
                bootstrap_started_at=normalized_bootstrap_started,
                bootstrap_deadline=normalized_bootstrap_deadline,
                bootstrap_authorization_fingerprint=(
                    normalized_bootstrap_fingerprint
                ),
            )
        except RcaProdAdmissionError as exc:
            return {
                **_vm_task_service_denied_payload(
                    "vm_task_service_existing_identity_conflict",
                    f"reconciled stable task id has invalid RCA production admission: {exc.code}",
                ),
                "existing_status": reconciled,
                "submit_result": result,
                "admission": admission_payload,
            }
        creator_task = (
            result.get("task") if isinstance(result.get("task"), dict) else {}
        )
        creator_status = str(creator_task.get("status") or "").strip()
        created = result.get("success") is True and creator_status == "created"
        result.update({
            "success": True,
            "created": created,
            "deduped": not created,
            "reconciled": result.get("success") is not True,
            "task": {
                **creator_task,
                "task_id": validated_admission.submission_key,
                "state": reconciled.get("state", "unknown"),
            },
            "existing_status": reconciled,
            "admission": admission_payload,
            "workspace_runtime": workspace_runtime.to_dict(),
        })
        return result
    return {
        **result,
        "success": False,
        "error_code": "vm_task_service_submit_uncertain",
        "error": (
            "RCA task creation outcome is uncertain and no matching stable task is currently visible; "
            "retry must reconcile the same submission key"
        ),
        "retryable": True,
        "created": False,
        "deduped": False,
        "reconciled_status": reconciled,
        "admission": admission_payload,
        "workspace_runtime": workspace_runtime.to_dict(),
    }


def _is_safe_completion_probe_task_id(task_id: str) -> bool:
    """Fail closed for production notify probes; test/mock ids must not create real Feishu callbacks."""
    if not task_id or not _TASK_ID_RE.match(task_id):
        return False
    # Real shared-state tasks are timestamp-prefixed (YYYYMMDD-HHMMSS-...).
    # Short fixture ids such as "t1" previously leaked from tests into the
    # live process watcher and caused repeated Feishu topic notifications.
    return bool(re.match(r"^\d{8}-\d{6}-[A-Za-z0-9][A-Za-z0-9_.-]*$", task_id))


def _spawn_completion_probe_background(task_id: str) -> dict[str, Any]:
    """Start a host-side completion probe so VM task terminal state returns to the origin topic.

    VM/shared-state tasks are not Hermes-managed background processes, so the
    gateway process watcher cannot see them by itself.  Start the existing host
    relay script as a managed background process with notify_on_complete=True;
    when it exits, the gateway injects a synthetic internal event into the
    original Feishu topic and the agent can summarize the result there.
    """
    task_id = str(task_id or "").strip()
    if not _is_safe_completion_probe_task_id(task_id):
        return {"started": False, "reason": "missing_or_invalid_or_test_task_id"}
    script = _host_completion_probe_script()
    if not script.exists():
        return {
            "started": False,
            "reason": f"completion probe script not found: {script}",
        }
    try:
        from tools.terminal_tool import terminal_tool

        command = " ".join([
            shlex.quote(sys.executable or _python_executable()),
            shlex.quote(str(script)),
            "--task-id",
            shlex.quote(task_id),
        ])
        try:
            has_gateway_route = bool(
                _session_value("HERMES_SESSION_PLATFORM")
                and _session_value("HERMES_SESSION_CHAT_ID")
            )
        except Exception:
            has_gateway_route = False
        raw = terminal_tool(
            command=command,
            background=True,
            notify_on_complete=has_gateway_route,
            timeout=600,
        )
        try:
            payload = json.loads(raw) if raw else {}
        except Exception:
            payload = {"raw": raw}
        return {
            "started": bool(payload.get("session_id")),
            "session_id": payload.get("session_id", ""),
            "notify_on_complete": bool(payload.get("notify_on_complete")),
            "error": payload.get("error"),
        }
    except Exception as exc:
        return {
            "started": False,
            "reason": f"failed_to_start_completion_probe: {type(exc).__name__}: {exc}",
        }


def vm_task_submit_json(
    title: str,
    goal: str,
    task_id: str = "",
    owner: str = "",
    user_id: str = "",
    lane: str = "",
    resource_class: str = "",
    repo_scope: str = "",
    workspace_scope: str = "",
    risk_class: str = "",
    artifact_root: str = "",
    artifact_cifs_root: str = "",
    executor_type: str = "",
    agent_backend: str = "",
    codex_backend_enabled: bool | None = None,
) -> str:
    return json.dumps(
        vm_task_submit(
            title=title,
            goal=goal,
            task_id=task_id,
            owner=owner,
            user_id=user_id,
            lane=lane,
            resource_class=resource_class,
            repo_scope=repo_scope,
            workspace_scope=workspace_scope,
            risk_class=risk_class,
            artifact_root=artifact_root,
            artifact_cifs_root=artifact_cifs_root,
            executor_type=executor_type,
            agent_backend=agent_backend,
            codex_backend_enabled=codex_backend_enabled,
        ),
        ensure_ascii=False,
    )


def _read_text_if_present(path: Path, *, limit_chars: int = 12000) -> str:
    if not path.is_file():
        return ""
    text = path.read_text(encoding="utf-8", errors="replace")
    if len(text) <= limit_chars:
        return text
    return text[:limit_chars] + "\n...[truncated]"


def _read_json_if_present(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_task_db_row(root: Path, task_id: str) -> dict[str, Any]:
    db_path = root / "state.db"
    if not db_path.is_file():
        return {}
    conn = None
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=1)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
        ).fetchone()
    except sqlite3.Error:
        return {}
    finally:
        if conn is not None:
            conn.close()
    return dict(row) if row is not None else {}


def _first_non_empty(*values: Any) -> Any:
    for value in values:
        if value not in (None, ""):
            return value
    return ""


def vm_task_status(task_id: str, include_markdown: bool = True) -> Dict[str, Any]:
    task_id = str(task_id or "").strip()
    if not task_id or not _TASK_ID_RE.match(task_id):
        return {"success": False, "error": f"invalid task_id: {task_id!r}"}

    roots = []
    for candidate_root in (_DEFAULT_HOST_CANONICAL_ROOT, _DEFAULT_VM_CANONICAL_ROOT):
        root_path = Path(candidate_root)
        if root_path not in roots:
            roots.append(root_path)

    checked_roots: list[str] = []
    for root in roots:
        checked_roots.append(str(root))
        task_dir = root / "tasks" / task_id
        dispatch_queue = ""
        dispatch_payload: dict[str, Any] = {}
        for queue in ("pending", "claimed", "completed", "failed"):
            candidate = root / "dispatch" / queue / f"{task_id}.json"
            if candidate.is_file():
                dispatch_queue = queue
                dispatch_payload = _read_json_if_present(candidate)
                break

        if not task_dir.exists() and not dispatch_queue:
            continue

        status_path = task_dir / "status.md"
        result_path = task_dir / "result.md"
        meta_path = task_dir / "meta.json"
        meta = _read_json_if_present(meta_path)
        db_row = _read_task_db_row(root, task_id)
        db_meta: dict[str, Any] = {}
        raw_db_meta = db_row.get("meta_json")
        if isinstance(raw_db_meta, str) and raw_db_meta.strip():
            try:
                parsed_db_meta = json.loads(raw_db_meta)
                if isinstance(parsed_db_meta, dict):
                    db_meta = parsed_db_meta
            except (TypeError, ValueError):
                db_meta = {}
        task_meta = dict(db_meta)
        task_meta.update(meta)
        dispatch_meta = dispatch_payload.get("meta")
        if isinstance(dispatch_meta, dict):
            task_meta.update(dispatch_meta)
        state = str(
            _first_non_empty(
                dispatch_payload.get("state"),
                db_row.get("state"),
                meta.get("state"),
                dispatch_queue,
                "unknown",
            )
        )
        payload: Dict[str, Any] = {
            "success": True,
            "task_id": task_id,
            "state": state,
            "dispatch_queue": dispatch_queue,
            "summary": _first_non_empty(
                dispatch_payload.get("summary"),
                dispatch_payload.get("latest_summary"),
                db_row.get("latest_summary"),
                db_row.get("summary"),
                meta.get("summary"),
            ),
            "title": _first_non_empty(
                dispatch_payload.get("title"), db_row.get("title"), meta.get("title")
            ),
            "owner": _first_non_empty(
                dispatch_payload.get("owner"), db_row.get("owner"), meta.get("owner")
            ),
            "updated_at": _first_non_empty(
                dispatch_payload.get("updated_at"),
                db_row.get("updated_at"),
                meta.get("updated_at"),
            ),
            "run_id": _first_non_empty(
                dispatch_payload.get("run_id"), db_row.get("run_id"), meta.get("run_id")
            ),
            "agent_host": _first_non_empty(
                dispatch_payload.get("agent_host"),
                db_row.get("agent_host"),
                meta.get("agent_host"),
            ),
            "meta": task_meta,
            "paths": {
                "root": str(root),
                "checked_roots": checked_roots,
                "task_dir": str(task_dir),
                "status_md": str(status_path),
                "result_md": str(result_path),
            },
        }
        if include_markdown:
            payload["status_md"] = _read_text_if_present(status_path)
            payload["result_md"] = _read_text_if_present(result_path)
        return payload

    return {
        "success": False,
        "task_id": task_id,
        "state": "missing",
        "error": f"task not found in shared-state: {task_id}",
        "paths": {"root": str(roots[0]), "checked_roots": checked_roots},
    }


def vm_task_status_json(task_id: str, include_markdown: bool = True) -> str:
    return json.dumps(
        vm_task_status(task_id=task_id, include_markdown=include_markdown),
        ensure_ascii=False,
    )


registry.register(
    name="vm_task_submit",
    toolset="vm_tasks",
    schema=VM_TASK_SUBMIT_SCHEMA,
    handler=lambda args, **kw: vm_task_submit_json(
        title=args.get("title", ""),
        goal=args.get("goal", ""),
        task_id=args.get("task_id", ""),
        owner=args.get("owner", ""),
        user_id=kw.get("user_id", ""),
        lane=args.get("lane", ""),
        resource_class=args.get("resource_class", ""),
        repo_scope=args.get("repo_scope", ""),
        workspace_scope=args.get("workspace_scope", ""),
        risk_class=args.get("risk_class", ""),
        artifact_root=args.get("artifact_root", ""),
        artifact_cifs_root=args.get("artifact_cifs_root", ""),
        executor_type=args.get("executor_type", ""),
        agent_backend=args.get("agent_backend", ""),
        codex_backend_enabled=args.get("codex_backend_enabled")
        if "codex_backend_enabled" in args
        else None,
    ),
    emoji="🛰️",
)

registry.register(
    name="vm_task_status",
    toolset="vm_tasks",
    schema=VM_TASK_STATUS_SCHEMA,
    handler=lambda args, **kw: vm_task_status_json(
        task_id=args.get("task_id", ""),
        include_markdown=args.get("include_markdown", True),
    ),
    emoji="📡",
)
