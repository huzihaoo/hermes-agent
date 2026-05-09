"""Tests for repo ACL approval request reservation flow."""

import json

from tools.repo_acl_approval import (
    RepoAclApprovalStore,
    build_lark_cli_repo_acl_card_send_command,
    build_repo_acl_apply_plan,
    build_repo_acl_approval_card,
    create_repo_acl_request_from_command,
    prepare_repo_acl_approval_live_send,
    reserve_repo_acl_approval_outbox,
)


def test_create_repo_acl_request_persists_minimal_fail_closed_payload(tmp_path):
    store = RepoAclApprovalStore(tmp_path)

    request = store.create_request(
        requester_display_name="陈玉",
        requester_user_id="ou_chenyu",
        repo="planning_algo/nop/planning",
        requested_grant="read",
        requested_action="grep",
        reason="用户请求排查 planning 模块问题",
        gitlab_evidence={
            "gitlab_username_candidates": ["chenyu"],
            "snapshot_grant": "Developer/write",
        },
        chat_id="oc_test",
        thread_id="omt_test",
    )

    assert request["type"] == "repo_acl_request"
    assert request["status"] == "pending"
    assert request["request_id"].startswith("repoacl_")
    assert request["requester"]["display_name"] == "陈玉"
    assert request["requester"]["feishu_user_id"] == "ou_chenyu"
    assert request["repo"] == "planning_algo/nop/planning"
    assert request["requested_grant"] == "read"
    assert request["requested_action"] == "grep"
    assert request["gitlab_evidence"]["snapshot_grant"] == "Developer/write"
    assert request["delivery"]["chat_id"] == "oc_test"
    assert request["delivery"]["thread_id"] == "omt_test"
    assert request["risk"]["group_reply_cap"] == "L1"
    assert request["risk"]["source_access_if_approved"] == "S3"
    assert request["apply"]["auto_apply"] is False

    saved = json.loads((tmp_path / "repo-acl-requests.json").read_text(encoding="utf-8"))
    assert saved[request["request_id"]]["status"] == "pending"


def test_repo_acl_request_rejects_invalid_grant_and_broad_wildcard(tmp_path):
    store = RepoAclApprovalStore(tmp_path)

    try:
        store.create_request(
            requester_display_name="陈玉",
            requester_user_id="ou_chenyu",
            repo="planning_algo/nop/planning",
            requested_grant="admin",
            requested_action="grep",
            reason="bad",
        )
    except ValueError as exc:
        assert "invalid requested grant" in str(exc)
    else:
        raise AssertionError("admin grant should not be requestable through approval card reservation")

    try:
        store.create_request(
            requester_display_name="陈玉",
            requester_user_id="ou_chenyu",
            repo="*",
            requested_grant="read",
            requested_action="grep",
            reason="too broad",
        )
    except ValueError as exc:
        assert "wildcard repo requests are not allowed" in str(exc)
    else:
        raise AssertionError("global wildcard should be rejected")



def test_repo_acl_request_rejects_ambiguous_path_scopes(tmp_path):
    store = RepoAclApprovalStore(tmp_path)

    for repo in [
        "planning_algo//nop",
        "planning_algo/./nop",
        "planning_algo/*/nop",
        "../planning_algo/nop",
    ]:
        try:
            store.create_request(
                requester_display_name="陈玉",
                requester_user_id="ou_chenyu",
                repo=repo,
                requested_grant="read",
                requested_action="grep",
                reason="bad scope",
            )
        except ValueError as exc:
            assert "invalid repo scope" in str(exc) or "wildcard repo requests" in str(exc)
        else:
            raise AssertionError(f"ambiguous repo scope should be rejected: {repo}")

def test_build_repo_acl_approval_card_displays_approver_as_name_not_user_id(tmp_path):
    store = RepoAclApprovalStore(tmp_path)
    request = store.create_request(
        requester_display_name="陈玉",
        requester_user_id="ou_chenyu",
        repo="planning_algo/nop/planning",
        requested_grant="read",
        requested_action="read_file",
        reason="查看模块接口",
        gitlab_evidence={"snapshot_grant": "Reporter/read"},
        approver_display_name="胡子豪",
        approver_user_id="ou_d1d3cfeba1be0a22faa36aaf4fb3907d",
    )

    card = build_repo_acl_approval_card(request)
    body = json.dumps(card, ensure_ascii=False)

    assert "审批人" in body
    assert "审批人**: 胡子豪" in body
    assert "审批人**: ou_" not in body
    assert "ou_d1d3cfeba1be0a22faa36aaf4fb3907d" not in body


def test_repo_acl_approval_rejects_id_like_approver_display_name(tmp_path):
    store = RepoAclApprovalStore(tmp_path)

    try:
        store.create_request(
            requester_display_name="陈玉",
            requester_user_id="ou_chenyu",
            repo="planning_algo/nop/planning",
            requested_grant="read",
            requested_action="read_file",
            reason="查看模块接口",
            approver_display_name="ou_d1d3cfeba1be0a22faa36aaf4fb3907d",
            approver_user_id="ou_d1d3cfeba1be0a22faa36aaf4fb3907d",
        )
    except ValueError as exc:
        assert "display name" in str(exc)
    else:
        raise AssertionError("id-like approver display name should be rejected")


def test_build_repo_acl_approval_card_suppresses_id_like_stored_approver_name(tmp_path):
    store = RepoAclApprovalStore(tmp_path)
    request = store.create_request(
        requester_display_name="陈玉",
        requester_user_id="ou_chenyu",
        repo="planning_algo/nop/planning",
        requested_grant="read",
        requested_action="read_file",
        reason="查看模块接口",
        gitlab_evidence={"snapshot_grant": "Reporter/read"},
        approver_display_name="胡子豪",
        approver_user_id="ou_d1d3cfeba1be0a22faa36aaf4fb3907d",
    )
    request["approver"]["display_name"] = "ou_d1d3cfeba1be0a22faa36aaf4fb3907d"

    card = build_repo_acl_approval_card(request)
    body = json.dumps(card, ensure_ascii=False)

    assert "审批人**: ou_" not in body
    assert "ou_d1d3cfeba1be0a22faa36aaf4fb3907d" not in body


def test_build_repo_acl_approval_card_is_human_reviewable_and_non_applying(tmp_path):
    store = RepoAclApprovalStore(tmp_path)
    request = store.create_request(
        requester_display_name="陈玉",
        requester_user_id="ou_chenyu",
        repo="planning_algo/nop/planning",
        requested_grant="read",
        requested_action="read_file",
        reason="查看模块接口",
        gitlab_evidence={"snapshot_grant": "Reporter/read"},
    )

    card = build_repo_acl_approval_card(request)

    assert card["config"]["wide_screen_mode"] is True
    assert "Repo 权限审批" in card["header"]["title"]["content"]
    body = json.dumps(card, ensure_ascii=False)
    assert request["request_id"] in body
    assert "陈玉" in body
    assert "planning_algo/nop/planning" in body
    assert "不会自动写入 live repo_acl" in body
    assert "approve_read_30d" in body
    assert "reject" in body


def test_build_repo_acl_approval_card_buttons_are_feishu_callback_addressable(tmp_path):
    store = RepoAclApprovalStore(tmp_path)
    request = store.create_request(
        requester_display_name="陈玉",
        requester_user_id="ou_chenyu",
        repo="planning_algo/nop/planning",
        requested_grant="read",
        requested_action="read_file",
        reason="查看模块接口",
    )

    card = build_repo_acl_approval_card(request)
    actions = card["elements"][-1]["actions"]

    assert [action["value"]["request_id"] for action in actions] == [request["request_id"]] * 3
    assert [action["value"]["hermes_action"] for action in actions] == [
        "repo_acl_approve_read_30d",
        "repo_acl_reject",
        "repo_acl_request_more_info",
    ]
    assert [action["value"]["action"] for action in actions] == [
        "approve_read_30d",
        "reject",
        "request_more_info",
    ]


def test_reserve_repo_acl_approval_outbox_persists_card_without_sending(tmp_path):
    store = RepoAclApprovalStore(tmp_path)
    request = store.create_request(
        requester_display_name="陈玉",
        requester_user_id="ou_chenyu",
        repo="planning_algo/nop/planning",
        requested_grant="read",
        requested_action="read_file",
        reason="查看模块接口",
        chat_id="oc_test",
        thread_id="omt_test",
    )
    card = build_repo_acl_approval_card(request)

    envelope = reserve_repo_acl_approval_outbox(request, card, store=store)

    assert envelope["type"] == "repo_acl_approval_card_outbox"
    assert envelope["status"] == "reserved"
    assert envelope["send_mode"] == "dry_run"
    assert envelope["sent"] is False
    assert envelope["request_id"] == request["request_id"]
    assert envelope["delivery"] == {"platform": "feishu", "chat_id": "oc_test", "thread_id": "omt_test"}
    assert envelope["card"] == card
    assert envelope["safety"]["auto_apply"] is False
    assert envelope["safety"]["live_send"] is False
    assert envelope["safety"]["required_feishu_scopes"] == ["im:message", "im:message.send_as_user"]
    assert envelope["safety"]["scope_preflight"] == {
        "command": 'lark-cli auth check --scope "im:message im:message.send_as_user"',
        "status": "not_checked",
        "required_before_live_send": True,
    }
    assert envelope["safety"]["delivery_stage"] == "dry_run_only"
    saved = json.loads((tmp_path / "repo-acl-approval-outbox.json").read_text(encoding="utf-8"))
    assert saved[request["request_id"]]["card"] == card
    assert saved[request["request_id"]]["sent"] is False


def test_prepare_repo_acl_approval_live_send_requires_allowlisted_chat_and_scope():
    store = RepoAclApprovalStore()
    envelope = {
        "request_id": "repoacl_test",
        "send_mode": "dry_run",
        "sent": False,
        "delivery": {"platform": "feishu", "chat_id": "oc_16614f4ba25b8c88b69c0b8e9ebc2fb5", "thread_id": ""},
        "card": {"config": {"wide_screen_mode": True}, "elements": []},
        "safety": {
            "auto_apply": False,
            "live_send": False,
            "required_feishu_scopes": ["im:message", "im:message.send_as_user"],
            "scope_preflight": {"status": "granted"},
            "delivery_stage": "dry_run_only",
        },
    }

    prepared = prepare_repo_acl_approval_live_send(
        envelope,
        allow_chat_ids={"oc_16614f4ba25b8c88b69c0b8e9ebc2fb5"},
        store=store,
    )

    assert prepared["send_mode"] == "live"
    assert prepared["sent"] is False
    assert prepared["safety"]["live_send"] is True
    assert prepared["safety"]["auto_apply"] is False
    assert prepared["safety"]["delivery_stage"] == "test_chat_live_send_ready"
    assert prepared["safety"]["allowed_chat_ids"] == ["oc_16614f4ba25b8c88b69c0b8e9ebc2fb5"]


def test_prepare_repo_acl_approval_live_send_rejects_unallowlisted_chat():
    envelope = {
        "request_id": "repoacl_test",
        "send_mode": "dry_run",
        "sent": False,
        "delivery": {"platform": "feishu", "chat_id": "oc_prod", "thread_id": ""},
        "card": {"config": {"wide_screen_mode": True}, "elements": []},
        "safety": {
            "auto_apply": False,
            "live_send": False,
            "required_feishu_scopes": ["im:message", "im:message.send_as_user"],
            "scope_preflight": {"status": "granted"},
            "delivery_stage": "dry_run_only",
        },
    }

    try:
        prepare_repo_acl_approval_live_send(
            envelope,
            allow_chat_ids={"oc_16614f4ba25b8c88b69c0b8e9ebc2fb5"},
        )
    except ValueError as exc:
        assert "not allowlisted" in str(exc)
    else:
        raise AssertionError("production/non-allowlisted chat must not be prepared for live send")


def test_prepare_repo_acl_approval_live_send_rejects_missing_im_scope_preflight():
    envelope = {
        "request_id": "repoacl_test",
        "send_mode": "dry_run",
        "sent": False,
        "delivery": {"platform": "feishu", "chat_id": "oc_16614f4ba25b8c88b69c0b8e9ebc2fb5", "thread_id": ""},
        "card": {"config": {"wide_screen_mode": True}, "elements": []},
        "safety": {
            "auto_apply": False,
            "live_send": False,
            "required_feishu_scopes": ["im:message", "im:message.send_as_user"],
            "scope_preflight": {"status": "missing"},
            "delivery_stage": "dry_run_only",
        },
    }

    try:
        prepare_repo_acl_approval_live_send(
            envelope,
            allow_chat_ids={"oc_16614f4ba25b8c88b69c0b8e9ebc2fb5"},
        )
    except ValueError as exc:
        assert "im:message scope preflight is not granted" in str(exc)
    else:
        raise AssertionError("live send must require granted im:message preflight")


def test_build_lark_cli_send_command_is_dry_run_by_default_and_uses_idempotency():
    safety = {
        "auto_apply": False,
        "live_send": True,
        "delivery_stage": "test_chat_live_send_ready",
        "required_feishu_scopes": ["im:message", "im:message.send_as_user"],
        "scope_preflight": {"status": "granted"},
        "allowed_chat_ids": ["oc_16614f4ba25b8c88b69c0b8e9ebc2fb5"],
    }
    envelope = {
        "request_id": "repoacl_test",
        "send_mode": "live",
        "sent": False,
        "delivery": {"platform": "feishu", "chat_id": "oc_16614f4ba25b8c88b69c0b8e9ebc2fb5", "thread_id": ""},
        "card": {"config": {"wide_screen_mode": True}, "elements": []},
        "safety": safety,
    }

    command = build_lark_cli_repo_acl_card_send_command(envelope)

    assert command[0:3] == ["lark-cli", "im", "+messages-send"]
    assert "--as" in command and command[command.index("--as") + 1] == "user"
    assert "--chat-id" in command and command[command.index("--chat-id") + 1] == "oc_16614f4ba25b8c88b69c0b8e9ebc2fb5"
    assert "--msg-type" in command and command[command.index("--msg-type") + 1] == "interactive"
    assert "--dry-run" in command
    assert "--idempotency-key" in command and command[command.index("--idempotency-key") + 1] == "repoacl_test"
    assert "--content" in command and json.loads(command[command.index("--content") + 1]) == envelope["card"]


def test_build_lark_cli_send_command_rejects_unallowlisted_chat_even_for_dry_run():
    envelope = {
        "request_id": "repoacl_test",
        "send_mode": "live",
        "sent": False,
        "delivery": {"platform": "feishu", "chat_id": "oc_prod", "thread_id": ""},
        "card": {"config": {"wide_screen_mode": True}, "elements": []},
        "safety": {
            "auto_apply": False,
            "live_send": True,
            "delivery_stage": "test_chat_live_send_ready",
            "required_feishu_scopes": ["im:message", "im:message.send_as_user"],
            "scope_preflight": {"status": "granted"},
            "allowed_chat_ids": ["oc_16614f4ba25b8c88b69c0b8e9ebc2fb5"],
        },
    }

    try:
        build_lark_cli_repo_acl_card_send_command(envelope)
    except ValueError as exc:
        assert "not allowlisted" in str(exc)
    else:
        raise AssertionError("send command must reject non-allowlisted chat_id")


def test_build_lark_cli_send_command_rejects_missing_scope_preflight():
    envelope = {
        "request_id": "repoacl_test",
        "send_mode": "live",
        "sent": False,
        "delivery": {"platform": "feishu", "chat_id": "oc_16614f4ba25b8c88b69c0b8e9ebc2fb5", "thread_id": ""},
        "card": {"config": {"wide_screen_mode": True}, "elements": []},
        "safety": {
            "auto_apply": False,
            "live_send": True,
            "delivery_stage": "test_chat_live_send_ready",
            "required_feishu_scopes": ["im:message", "im:message.send_as_user"],
            "scope_preflight": {"status": "missing"},
            "allowed_chat_ids": ["oc_16614f4ba25b8c88b69c0b8e9ebc2fb5"],
        },
    }

    try:
        build_lark_cli_repo_acl_card_send_command(envelope)
    except ValueError as exc:
        assert "scope preflight" in str(exc)
    else:
        raise AssertionError("send command must reject missing im:message preflight")

def test_build_repo_acl_apply_plan_requires_approved_pending_apply_request():
    request = {
        "type": "repo_acl_request",
        "request_id": "repoacl_test",
        "status": "pending",
        "requester": {"display_name": "陈玉", "feishu_user_id": "ou_chenyu"},
        "repo": "minieye_dnp_nop",
        "requested_grant": "read",
        "apply": {"auto_apply": False},
    }

    try:
        build_repo_acl_apply_plan(request)
    except ValueError as exc:
        assert "approved_pending_apply" in str(exc)
    else:
        raise AssertionError("apply plan must require an approved_pending_apply request")


def test_build_repo_acl_apply_plan_is_non_mutating_and_audited():
    request = {
        "type": "repo_acl_request",
        "request_id": "repoacl_test",
        "status": "approved_pending_apply",
        "requester": {"display_name": "陈玉", "feishu_user_id": "ou_chenyu"},
        "repo": "minieye_dnp_nop",
        "requested_grant": "read",
        "requested_action": "read_file",
        "apply": {"auto_apply": False},
        "resolution": {"action": "approve_read_30d", "approver_name": "胡子豪", "auto_apply": False},
    }

    plan = build_repo_acl_apply_plan(request, operator="胡子豪")

    assert plan["type"] == "repo_acl_apply_plan"
    assert plan["request_id"] == "repoacl_test"
    assert plan["status"] == "ready_for_manual_apply"
    assert plan["mutation"]["live_config_mutated"] is False
    assert plan["mutation"]["auto_apply"] is False
    assert plan["grant"] == {"user_name": "陈玉", "repo": "minieye_dnp_nop", "grant": "read"}
    assert plan["operator"] == "胡子豪"
    assert plan["commands"]["local"] == ["hermes", "pairing", "grant-repo", "陈玉", "minieye_dnp_nop", "read"]
    assert plan["commands"]["vm_audit"] == [
        "/home/mini/worktrees/audit-logger.sh",
        "陈玉",
        "minieye_dnp_nop",
        "repo_acl grant read approved_by 胡子豪 request repoacl_test",
    ]
    assert plan["safety"]["requires_backup"] is True
    assert plan["safety"]["requires_post_apply_smoke"] is True
    assert plan["safety"]["source"] == "approved Feishu repo_acl card callback"


def test_build_repo_acl_apply_plan_rejects_auto_apply_request():
    request = {
        "type": "repo_acl_request",
        "request_id": "repoacl_test",
        "status": "approved_pending_apply",
        "requester": {"display_name": "陈玉", "feishu_user_id": "ou_chenyu"},
        "repo": "minieye_dnp_nop",
        "requested_grant": "read",
        "apply": {"auto_apply": True},
        "resolution": {"action": "approve_read_30d", "approver_name": "胡子豪", "auto_apply": False},
    }

    try:
        build_repo_acl_apply_plan(request)
    except ValueError as exc:
        assert "auto_apply" in str(exc)
    else:
        raise AssertionError("apply plan must reject auto_apply requests")


def test_create_request_from_missing_acl_read_command_extracts_session_and_repo(tmp_path):
    store = RepoAclApprovalStore(tmp_path)
    command = "ssh-mini-agent read_file /home/mini/worktrees/minieye_dnp_nop/陈玉/src/main.py --start 1 --lines 20"

    request = create_repo_acl_request_from_command(
        command,
        requester_display_name="陈玉",
        requester_user_id="ou_chenyu",
        store=store,
        chat_id="oc_test",
        thread_id="omt_test",
    )

    assert request["repo"] == "minieye_dnp_nop"
    assert request["requested_grant"] == "read"
    assert request["requested_action"] == "read_file"
    assert request["reason"] == "Missing repo_acl read grant for minieye_dnp_nop while attempting read_file"
    assert request["request_context"]["command"] == command
    assert request["delivery"] == {"platform": "feishu", "chat_id": "oc_test", "thread_id": "omt_test"}
    saved = json.loads((tmp_path / "repo-acl-requests.json").read_text(encoding="utf-8"))
    assert request["request_id"] in saved


def test_duplicate_pending_repo_acl_request_reuses_existing_request(tmp_path):
    store = RepoAclApprovalStore(tmp_path)
    command = "ssh-mini-agent read_file /home/mini/worktrees/minieye_dnp_nop/陈玉/src/main.py --start 1 --lines 20"

    first = create_repo_acl_request_from_command(
        command,
        requester_display_name="陈玉",
        requester_user_id="ou_chenyu",
        store=store,
        chat_id="oc_test",
        thread_id="omt_test",
    )
    second = create_repo_acl_request_from_command(
        command,
        requester_display_name="陈玉",
        requester_user_id="ou_chenyu",
        store=store,
        chat_id="oc_test",
        thread_id="omt_test",
    )

    assert second["request_id"] == first["request_id"]
    assert second["deduped"] is True
    saved = json.loads((tmp_path / "repo-acl-requests.json").read_text(encoding="utf-8"))
    assert list(saved) == [first["request_id"]]


def test_create_request_from_missing_acl_write_git_command_uses_worktree_repo(tmp_path):
    store = RepoAclApprovalStore(tmp_path)
    command = "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/陈玉 && git checkout feature/x'"

    request = create_repo_acl_request_from_command(
        command,
        requester_display_name="陈玉",
        requester_user_id="ou_chenyu",
        store=store,
    )

    assert request["repo"] == "minieye_dnp_nop"
    assert request["requested_grant"] == "write"
    assert request["requested_action"] == "git checkout"


def test_create_request_from_non_repo_command_returns_none(tmp_path):
    store = RepoAclApprovalStore(tmp_path)

    request = create_repo_acl_request_from_command(
        "python -m pytest tests/tools/test_repo_acl_approval.py -q",
        requester_display_name="陈玉",
        requester_user_id="ou_chenyu",
        store=store,
    )

    assert request is None
    assert not (tmp_path / "repo-acl-requests.json").exists()
