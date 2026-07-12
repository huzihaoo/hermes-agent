"""Repo ACL approval request reservation helpers.

This module deliberately stops before live mutation: it creates durable pending
requests and reviewable Feishu card payloads, but does not grant repo ACLs.
Approval handlers can later consume these request IDs and call the safe pairing
repo grant entrypoint after admin confirmation.
"""

from __future__ import annotations

import json
import os
import re
import secrets
import shlex
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hermes_constants import get_hermes_dir
from gateway.pairing import _secure_write

DEFAULT_REPO_ACL_APPROVAL_DIR = get_hermes_dir("platforms/repo-acl-approvals", "repo-acl-approvals")
_REQUESTABLE_GRANTS = {"read", "write", "push"}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_nonempty(value: str, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field} is required")
    return normalized


def _validate_repo_scope(repo: str) -> str:
    normalized = _normalize_nonempty(repo, "repo").strip("/")
    if normalized == "*":
        raise ValueError("wildcard repo requests are not allowed")
    parts = normalized.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise ValueError(f"invalid repo scope: {repo}")
    if parts[-1] == "*":
        if len(parts) < 2:
            raise ValueError(f"invalid repo scope: {repo}")
        parts_to_validate = parts[:-1]
    elif any(part == "*" for part in parts):
        raise ValueError(f"invalid repo scope: {repo}")
    else:
        parts_to_validate = parts
    if not all(re.fullmatch(r"[A-Za-z0-9._-]+", part) for part in parts_to_validate):
        raise ValueError(f"invalid repo scope: {repo}")
    return normalized


def _validate_requested_grant(grant: str) -> str:
    normalized = str(grant or "").strip().lower()
    if normalized not in _REQUESTABLE_GRANTS:
        raise ValueError(f"invalid requested grant: {grant}")
    return normalized


def _new_request_id() -> str:
    return f"repoacl_{int(time.time())}_{secrets.token_hex(4)}"


def _looks_like_feishu_user_id(value: str) -> bool:
    normalized = str(value or "").strip()
    return normalized.startswith(("ou_", "on_", "ou-", "on-"))


def _safe_optional_display_name(value: str) -> str:
    normalized = str(value or "").strip()
    if not normalized or _looks_like_feishu_user_id(normalized):
        return ""
    return normalized


class RepoAclApprovalStore:
    """Small JSON-backed store for pending repo ACL approval requests."""

    def __init__(self, base_dir: str | Path | None = None):
        override_dir = os.getenv("HERMES_REPO_ACL_APPROVAL_DIR", "").strip()
        if base_dir is not None:
            self.base_dir = Path(base_dir)
        elif override_dir:
            self.base_dir = Path(override_dir)
        else:
            self.base_dir = DEFAULT_REPO_ACL_APPROVAL_DIR
        self.base_dir.mkdir(parents=True, exist_ok=True)

    @property
    def requests_path(self) -> Path:
        return self.base_dir / "repo-acl-requests.json"

    @property
    def outbox_path(self) -> Path:
        return self.base_dir / "repo-acl-approval-outbox.json"

    def _load_requests(self) -> dict[str, dict[str, Any]]:
        if not self.requests_path.exists():
            return {}
        try:
            data = json.loads(self.requests_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return data if isinstance(data, dict) else {}

    def _save_requests(self, requests: dict[str, dict[str, Any]]) -> None:
        _secure_write(self.requests_path, json.dumps(requests, ensure_ascii=False, indent=2) + "\n")

    def _load_outbox(self) -> dict[str, dict[str, Any]]:
        if not self.outbox_path.exists():
            return {}
        try:
            data = json.loads(self.outbox_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return data if isinstance(data, dict) else {}

    def _save_outbox(self, outbox: dict[str, dict[str, Any]]) -> None:
        _secure_write(self.outbox_path, json.dumps(outbox, ensure_ascii=False, indent=2) + "\n")

    def update_request_resolution(
        self,
        request_id: str,
        *,
        action: str,
        approver_name: str,
        status: str,
    ) -> dict[str, Any]:
        requests = self._load_requests()
        request = requests.get(_normalize_nonempty(request_id, "request_id"))
        if not request:
            raise ValueError(f"repo_acl request not found: {request_id}")
        request["status"] = _normalize_nonempty(status, "status")
        request["resolved_at"] = _utc_now_iso()
        request["resolution"] = {
            "action": _normalize_nonempty(action, "action"),
            "approver_name": _safe_optional_display_name(approver_name) or "审批人",
            "auto_apply": False,
        }
        requests[request_id] = request
        self._save_requests(requests)
        return request

    def create_request(
        self,
        *,
        requester_display_name: str,
        requester_user_id: str,
        repo: str,
        requested_grant: str,
        requested_action: str,
        reason: str,
        gitlab_evidence: dict[str, Any] | None = None,
        chat_id: str = "",
        thread_id: str = "",
        request_context: dict[str, Any] | None = None,
        approver_display_name: str = "",
        approver_user_id: str = "",
        dedupe: bool = True,
    ) -> dict[str, Any]:
        display_name = _normalize_nonempty(requester_display_name, "requester_display_name")
        user_id = _normalize_nonempty(requester_user_id, "requester_user_id")
        normalized_repo = _validate_repo_scope(repo)
        grant = _validate_requested_grant(requested_grant)
        action = _normalize_nonempty(requested_action, "requested_action")
        normalized_reason = _normalize_nonempty(reason, "reason")
        approver_name = _safe_optional_display_name(approver_display_name)
        approver_id = str(approver_user_id or "").strip()
        if approver_id and not approver_name:
            raise ValueError("approver_display_name must be a display name when approver_user_id is provided")
        delivery = {
            "platform": "feishu",
            "chat_id": str(chat_id or "").strip(),
            "thread_id": str(thread_id or "").strip(),
        }
        normalized_context = request_context or {}
        requests = self._load_requests()
        if dedupe:
            for existing in requests.values():
                if existing.get("status") != "pending":
                    continue
                if existing.get("requester", {}).get("display_name") != display_name:
                    continue
                if existing.get("requester", {}).get("feishu_user_id") != user_id:
                    continue
                if existing.get("repo") != normalized_repo:
                    continue
                if existing.get("requested_grant") != grant:
                    continue
                if existing.get("requested_action") != action:
                    continue
                if existing.get("delivery") != delivery:
                    continue
                if existing.get("request_context") != normalized_context:
                    continue
                if existing.get("approver", {}) != ({"display_name": approver_name, "feishu_user_id": approver_id} if approver_name else {}):
                    continue
                reused = dict(existing)
                reused["deduped"] = True
                return reused
        request_id = _new_request_id()
        now = _utc_now_iso()
        request = {
            "type": "repo_acl_request",
            "request_id": request_id,
            "status": "pending",
            "created_at": now,
            "requester": {
                "display_name": display_name,
                "feishu_user_id": user_id,
            },
            "repo": normalized_repo,
            "requested_grant": grant,
            "requested_action": action,
            "reason": normalized_reason,
            "gitlab_evidence": gitlab_evidence or {},
            "risk": {
                "source_access_if_approved": "S3",
                "group_reply_cap": "L1",
                "owner_dm_detail_cap": "L3",
            },
            "delivery": delivery,
            "apply": {
                "auto_apply": False,
                "safe_command_template": f"hermes pairing grant-repo {json.dumps(display_name, ensure_ascii=False)} {json.dumps(normalized_repo)} {grant}",
            },
            "review_options": [
                "approve_read_once",
                "approve_read_30d",
                "approve_write_30d",
                "reject",
                "request_more_info",
            ],
            "request_context": normalized_context,
        }
        if approver_name:
            request["approver"] = {
                "display_name": approver_name,
                "feishu_user_id": approver_id,
            }
        requests[request_id] = request
        self._save_requests(requests)
        return request

    def list_pending(self) -> list[dict[str, Any]]:
        requests = self._load_requests()
        return [req for req in requests.values() if req.get("status") == "pending"]


def _repo_user_from_vm_path(path: str) -> tuple[str, str | None] | None:
    worktree = re.match(r"^/home/mini/worktrees/([A-Za-z0-9._-]+)/([A-Za-z0-9._\-\u4e00-\u9fff]+)(?:/.*)?$", path)
    if worktree:
        return worktree.group(1), worktree.group(2)
    main = re.match(r"^/home/mini/([A-Za-z0-9._-]+)(?:/.*)?$", path)
    if main and main.group(1) != "worktrees":
        return main.group(1), None
    return None


def _required_grant_for_git_op(op: str) -> str:
    if op == "push":
        return "push"
    if op in {"add", "commit", "restore", "merge", "rebase", "checkout", "switch"}:
        return "write"
    return "read"


def _extract_repo_request_from_command(command: str) -> tuple[str, str, str] | None:
    try:
        argv = shlex.split(command)
    except ValueError:
        return None
    if not argv:
        return None

    agent_names = {"ssh-mini-agent", "~/.local/bin/ssh-mini-agent", "/Users/songying/.local/bin/ssh-mini-agent"}
    if argv[0] in agent_names and len(argv) >= 3 and argv[1] in {"list_files", "read_file", "grep", "head", "tail"}:
        for token in argv[2:]:
            parsed = _repo_user_from_vm_path(token)
            if parsed:
                repo, _user = parsed
                return repo, "read", argv[1]
        return None

    runner_names = {"ssh-mini-run", "~/.local/bin/ssh-mini-run", "/Users/songying/.local/bin/ssh-mini-run"}
    if argv[0] in runner_names and len(argv) == 2:
        remote = argv[1]
        segments = [segment.strip() for segment in remote.split("&&")]
        repo = ""
        strongest_grant = "read"
        action = "git"
        for segment in segments:
            if segment.startswith("cd "):
                try:
                    cd_argv = shlex.split(segment)
                except ValueError:
                    return None
                if len(cd_argv) == 2:
                    parsed = _repo_user_from_vm_path(cd_argv[1])
                    if parsed:
                        repo = parsed[0]
                continue
            git_match = re.match(r"^git\s+([a-zA-Z0-9_-]+)\b", segment)
            if git_match:
                op = git_match.group(1)
                action = f"git {op}"
                grant = _required_grant_for_git_op(op)
                if grant == "push":
                    strongest_grant = "push"
                elif grant == "write" and strongest_grant == "read":
                    strongest_grant = "write"
        if repo:
            return repo, strongest_grant, action
    return None


def create_repo_acl_request_from_command(
    command: str,
    *,
    requester_display_name: str,
    requester_user_id: str,
    store: RepoAclApprovalStore | None = None,
    chat_id: str = "",
    thread_id: str = "",
    gitlab_evidence: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Create a non-applying approval request for a repo command missing ACL.

    This is intentionally extraction-only: callers still decide when a command
    was denied by repo ACL policy. The helper does not grant permissions or send
    Feishu cards.
    """
    extracted = _extract_repo_request_from_command(command)
    if not extracted:
        return None
    repo, requested_grant, requested_action = extracted
    approval_store = store or RepoAclApprovalStore()
    reason = f"Missing repo_acl {requested_grant} grant for {repo} while attempting {requested_action}"
    return approval_store.create_request(
        requester_display_name=requester_display_name,
        requester_user_id=requester_user_id,
        repo=repo,
        requested_grant=requested_grant,
        requested_action=requested_action,
        reason=reason,
        gitlab_evidence=gitlab_evidence or {},
        chat_id=chat_id,
        thread_id=thread_id,
        request_context={"command": command},
    )


def reserve_repo_acl_approval_outbox(
    request: dict[str, Any],
    card: dict[str, Any],
    *,
    store: RepoAclApprovalStore | None = None,
) -> dict[str, Any]:
    """Reserve a dry-run outbox envelope for a repo ACL approval card.

    This deliberately does not send a Feishu card.  It persists the exact card
    payload and delivery target so the later live-delivery slice can be tested
    against a stable, reviewable artifact.
    """
    request_id = _normalize_nonempty(request.get("request_id", ""), "request_id")
    approval_store = store or RepoAclApprovalStore()
    delivery = request.get("delivery") or {}
    envelope = {
        "type": "repo_acl_approval_card_outbox",
        "status": "reserved",
        "send_mode": "dry_run",
        "sent": False,
        "created_at": _utc_now_iso(),
        "request_id": request_id,
        "delivery": {
            "platform": "feishu",
            "chat_id": str(delivery.get("chat_id", "")).strip(),
            "thread_id": str(delivery.get("thread_id", "")).strip(),
        },
        "card": card,
        "safety": {
            "auto_apply": bool((request.get("apply") or {}).get("auto_apply", False)),
            "live_send": False,
            "required_feishu_scopes": ["im:message", "im:message.send_as_user"],
            "scope_preflight": {
                "command": 'lark-cli auth check --scope "im:message im:message.send_as_user"',
                "status": "not_checked",
                "required_before_live_send": True,
            },
            "delivery_stage": "dry_run_only",
        },
    }
    outbox = approval_store._load_outbox()
    outbox[request_id] = envelope
    approval_store._save_outbox(outbox)
    return envelope


def prepare_repo_acl_approval_live_send(
    envelope: dict[str, Any],
    *,
    allow_chat_ids: set[str] | list[str] | tuple[str, ...],
    store: RepoAclApprovalStore | None = None,
) -> dict[str, Any]:
    """Prepare a reserved outbox envelope for test-chat live delivery.

    This is still non-sending and non-applying. It only flips a reviewed dry-run
    envelope into a live-ready envelope after route allowlist and im:message
    preflight checks have passed. The actual sender is a separate explicit step.
    """
    request_id = _normalize_nonempty(envelope.get("request_id", ""), "request_id")
    delivery = envelope.get("delivery") or {}
    chat_id = _normalize_nonempty(delivery.get("chat_id", ""), "delivery.chat_id")
    allowed = sorted(str(chat or "").strip() for chat in allow_chat_ids if str(chat or "").strip())
    if chat_id not in allowed:
        raise ValueError(f"delivery chat_id is not allowlisted: {chat_id}")
    safety = envelope.get("safety") or {}
    if bool(safety.get("auto_apply", False)):
        raise ValueError("auto_apply envelopes cannot be prepared for live send")
    if not {"im:message", "im:message.send_as_user"}.issubset(set(safety.get("required_feishu_scopes") or [])):
        raise ValueError("im:message and im:message.send_as_user are required before live send")
    preflight = safety.get("scope_preflight") or {}
    if preflight.get("status") != "granted":
        raise ValueError("im:message scope preflight is not granted")
    prepared = dict(envelope)
    prepared["send_mode"] = "live"
    prepared["sent"] = False
    prepared["prepared_at"] = _utc_now_iso()
    prepared["safety"] = {
        **safety,
        "auto_apply": False,
        "live_send": True,
        "delivery_stage": "test_chat_live_send_ready",
        "allowed_chat_ids": allowed,
    }
    approval_store = store or RepoAclApprovalStore()
    outbox = approval_store._load_outbox()
    outbox[request_id] = prepared
    approval_store._save_outbox(outbox)
    return prepared


def build_repo_acl_apply_plan(request: dict[str, Any], *, operator: str = "") -> dict[str, Any]:
    """Build a non-mutating manual apply plan for an approved repo ACL request.

    This deliberately does not call grant_repo_acl or write live config.  It only
    turns a recorded approval into an auditable command plan for the safe
    operator path.
    """
    request_id = _normalize_nonempty(request.get("request_id", ""), "request_id")
    if request.get("status") != "approved_pending_apply":
        raise ValueError("repo_acl apply plan requires status=approved_pending_apply")
    apply = request.get("apply") or {}
    resolution = request.get("resolution") or {}
    if bool(apply.get("auto_apply", False)) or bool(resolution.get("auto_apply", False)):
        raise ValueError("repo_acl apply plan refuses auto_apply requests")
    requester = request.get("requester") or {}
    user_name = _normalize_nonempty(requester.get("display_name", ""), "requester.display_name")
    repo = _validate_repo_scope(request.get("repo", ""))
    grant = _validate_requested_grant(request.get("requested_grant", ""))
    operator_name = _safe_optional_display_name(operator) or _safe_optional_display_name(resolution.get("approver_name", "")) or "审批人"
    local_cmd = ["hermes", "pairing", "grant-repo", user_name, repo, grant]
    audit_summary = f"repo_acl grant {grant} approved_by {operator_name} request {request_id}"
    return {
        "type": "repo_acl_apply_plan",
        "request_id": request_id,
        "status": "ready_for_manual_apply",
        "created_at": _utc_now_iso(),
        "operator": operator_name,
        "grant": {
            "user_name": user_name,
            "repo": repo,
            "grant": grant,
        },
        "source_request": {
            "status": request.get("status"),
            "requested_action": request.get("requested_action", ""),
            "resolution_action": resolution.get("action", ""),
            "resolved_at": request.get("resolved_at", ""),
        },
        "commands": {
            "local": local_cmd,
            "local_shell": " ".join(shlex.quote(part) for part in local_cmd),
            "vm_audit": ["/home/mini/worktrees/audit-logger.sh", user_name, repo, audit_summary],
            "vm_audit_shell": " ".join(shlex.quote(part) for part in ["/home/mini/worktrees/audit-logger.sh", user_name, repo, audit_summary]),
        },
        "mutation": {
            "live_config_mutated": False,
            "auto_apply": False,
        },
        "safety": {
            "source": "approved Feishu repo_acl card callback",
            "requires_backup": True,
            "requires_post_apply_smoke": True,
            "requires_operator_execution": True,
            "callback_did_not_grant_repo_acl": True,
        },
    }


def resolve_repo_acl_card_action(request_id: str, action: str, approver_name: str, *, store: RepoAclApprovalStore | None = None) -> dict[str, Any]:
    """Record a repo ACL card click without applying repo permissions."""
    normalized_action = _normalize_nonempty(action, "action")
    status_by_action = {
        "approve_read_30d": "approved_pending_apply",
        "reject": "rejected",
        "request_more_info": "more_info_requested",
    }
    status = status_by_action.get(normalized_action)
    if not status:
        raise ValueError(f"unsupported repo_acl card action: {action}")
    approval_store = store or RepoAclApprovalStore()
    return approval_store.update_request_resolution(
        request_id,
        action=normalized_action,
        approver_name=approver_name,
        status=status,
    )


def build_repo_acl_resolved_card(request: dict[str, Any]) -> dict[str, Any]:
    """Build the inline callback card for a resolved repo ACL card click."""
    resolution = request.get("resolution") or {}
    action = str(resolution.get("action") or "").strip()
    status = str(request.get("status") or "").strip()
    approver = str(resolution.get("approver_name") or "审批人").strip() or "审批人"
    request_id = str(request.get("request_id") or "").strip()
    repo = str(request.get("repo") or "").strip()
    grant = str(request.get("requested_grant") or "").strip()
    template = "red" if status == "rejected" else "blue" if status == "more_info_requested" else "green"
    title = "Repo ACL 审批已记录"
    if status == "rejected":
        title = "Repo ACL 审批已拒绝"
    elif status == "more_info_requested":
        title = "Repo ACL 审批需补充信息"
    action_label = {
        "approve_read_30d": "批准 read 30 天",
        "reject": "拒绝",
        "request_more_info": "补充信息",
    }.get(action, action or "unknown")
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": template,
            "title": {"tag": "plain_text", "content": title},
        },
        "elements": [
            _text_block(
                f"**请求 ID**: `{request_id}`\n"
                f"**仓库**: `{repo}`\n"
                f"**请求权限**: `{grant}`\n"
                f"**处理动作**: `{action_label}`\n"
                f"**审批人**: {approver}\n"
                "**安全边界**: 已记录审批点击，但不会自动写入 live repo_acl；后续仍需走安全 grant-repo 入口和审计。"
            )
        ],
    }


def build_lark_cli_repo_acl_card_send_command(envelope: dict[str, Any], *, dry_run: bool = True) -> list[str]:
    """Build a lark-cli command for sending a prepared repo ACL approval card.

    The default command includes lark-cli's own --dry-run flag. Callers must pass
    dry_run=False only after route, scope, and owner approval checks have passed.
    """
    if envelope.get("send_mode") != "live":
        raise ValueError("only live-prepared envelopes can build send commands")
    safety = envelope.get("safety") or {}
    if safety.get("delivery_stage") != "test_chat_live_send_ready" or safety.get("live_send") is not True:
        raise ValueError("envelope is not live-send ready")
    if bool(safety.get("auto_apply", False)):
        raise ValueError("auto_apply envelopes cannot be sent")
    if not {"im:message", "im:message.send_as_user"}.issubset(set(safety.get("required_feishu_scopes") or [])):
        raise ValueError("im:message and im:message.send_as_user are required before send")
    preflight = safety.get("scope_preflight") or {}
    if preflight.get("status") != "granted":
        raise ValueError("im:message scope preflight is not granted")
    request_id = _normalize_nonempty(envelope.get("request_id", ""), "request_id")
    delivery = envelope.get("delivery") or {}
    chat_id = _normalize_nonempty(delivery.get("chat_id", ""), "delivery.chat_id")
    allowed_chat_ids = set(safety.get("allowed_chat_ids") or [])
    if chat_id not in allowed_chat_ids:
        raise ValueError(f"delivery chat_id is not allowlisted: {chat_id}")
    card = envelope.get("card") or {}
    command = [
        "lark-cli",
        "im",
        "+messages-send",
        "--as",
        "user",
        "--chat-id",
        chat_id,
        "--msg-type",
        "interactive",
        "--content",
        json.dumps(card, ensure_ascii=False, separators=(",", ":")),
        "--idempotency-key",
        request_id,
    ]
    if dry_run:
        command.append("--dry-run")
    return command


def _text_block(content: str) -> dict[str, Any]:
    return {
        "tag": "div",
        "text": {"tag": "lark_md", "content": content},
    }


def build_repo_acl_approval_card(request: dict[str, Any]) -> dict[str, Any]:
    """Build a Feishu interactive-card-shaped payload for human approval."""
    requester = request.get("requester", {})
    approver = request.get("approver", {}) or {}
    evidence = request.get("gitlab_evidence", {}) or {}
    request_id = request.get("request_id", "")
    repo = request.get("repo", "")
    grant = request.get("requested_grant", "")
    action = request.get("requested_action", "")
    display_name = requester.get("display_name", "")
    approver_name = _safe_optional_display_name(approver.get("display_name", ""))
    evidence_lines = "无 GitLab snapshot 证据"
    if evidence:
        evidence_lines = "\n".join(f"- {key}: {value}" for key, value in evidence.items())
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": "orange",
            "title": {"tag": "plain_text", "content": "Repo 权限审批"},
        },
        "elements": [
            _text_block(f"**请求 ID**: `{request_id}`"),
            *([_text_block(f"**审批人**: {approver_name}")] if approver_name else []),
            _text_block(f"**请求人**: {display_name} (`{requester.get('feishu_user_id', '')}`)"),
            _text_block(f"**仓库**: `{repo}`\n**请求权限**: `{grant}`\n**触发动作**: `{action}`"),
            _text_block(f"**原因**: {request.get('reason', '')}"),
            _text_block(f"**GitLab 参考证据**:\n{evidence_lines}"),
            _text_block("**安全边界**: 这张卡片只创建审批预留，不会自动写入 live repo_acl；通过后仍应走安全 grant-repo 入口和审计。"),
            {
                "tag": "action",
                "actions": [
                    {
                        "tag": "button",
                        "text": {"tag": "plain_text", "content": "批准 read 30 天"},
                        "type": "primary",
                        "value": {
                            "hermes_action": "repo_acl_approve_read_30d",
                            "action": "approve_read_30d",
                            "request_id": request_id,
                        },
                    },
                    {
                        "tag": "button",
                        "text": {"tag": "plain_text", "content": "拒绝"},
                        "type": "danger",
                        "value": {
                            "hermes_action": "repo_acl_reject",
                            "action": "reject",
                            "request_id": request_id,
                        },
                    },
                    {
                        "tag": "button",
                        "text": {"tag": "plain_text", "content": "补充信息"},
                        "value": {
                            "hermes_action": "repo_acl_request_more_info",
                            "action": "request_more_info",
                            "request_id": request_id,
                        },
                    },
                ],
            },
        ],
    }
