"""Tests for repo ACL approval request reservation flow."""

import json

from tools.repo_acl_approval import (
    RepoAclApprovalStore,
    build_repo_acl_approval_card,
    create_repo_acl_request_from_command,
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
    saved = json.loads((tmp_path / "repo-acl-approval-outbox.json").read_text(encoding="utf-8"))
    assert saved[request["request_id"]]["card"] == card
    assert saved[request["request_id"]]["sent"] is False


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
