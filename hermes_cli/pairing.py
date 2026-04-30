"""
CLI commands for the DM pairing system.

Usage:
    hermes pairing list                             # Show all pending + approved users
    hermes pairing approve <platform> <code>       # Approve a pairing code
    hermes pairing approve-user <platform> <user_id> [user_name]
                                                   # Directly approve a known user
    hermes pairing grant-senior <platform> <user_id> <user_name>
                                                   # Set senior role + map ID + approve user
    hermes pairing grant-senior-by-name <platform> <user_name>
                                                   # Reuse stored mapping to grant senior again
    hermes pairing grant-repo <user_name> <repo> <grant>
                                                   # Grant repo ACL: read/write/push/admin
    hermes pairing revoke-repo <user_name> <repo>  # Revoke repo ACL
    hermes pairing list-repo-acl [user_name]       # List repo ACL grants
    hermes pairing revoke <platform> <user_id>     # Revoke user access
    hermes pairing clear-pending                    # Clear all expired/pending codes
"""

def pairing_command(args):
    """Handle hermes pairing subcommands."""
    from gateway.pairing import PairingStore

    store = PairingStore()
    action = getattr(args, "pairing_action", None)

    if action == "list":
        _cmd_list(store)
    elif action == "approve":
        _cmd_approve(store, args.platform, args.code)
    elif action == "approve-user":
        _cmd_approve_user(store, args.platform, args.user_id, getattr(args, "user_name", ""))
    elif action == "grant-senior":
        _cmd_grant_senior(store, args.platform, args.user_id, args.user_name)
    elif action == "grant-senior-by-name":
        _cmd_grant_senior_by_name(store, args.platform, args.user_name)
    elif action == "grant-repo":
        _cmd_grant_repo(args.user_name, args.repo, args.grant)
    elif action == "revoke-repo":
        _cmd_revoke_repo(args.user_name, args.repo)
    elif action == "list-repo-acl":
        _cmd_list_repo_acl(getattr(args, "user_name", None))
    elif action == "revoke":
        _cmd_revoke(store, args.platform, args.user_id)
    elif action == "clear-pending":
        _cmd_clear_pending(store)
    else:
        print("Usage: hermes pairing {list|approve|approve-user|grant-senior|grant-senior-by-name|grant-repo|revoke-repo|list-repo-acl|revoke|clear-pending}")
        print("Run 'hermes pairing --help' for details.")


def _cmd_list(store):
    """List all pending and approved users."""
    pending = store.list_pending()
    approved = store.list_approved()

    if not pending and not approved:
        print("No pairing data found. No one has tried to pair yet~")
        return

    if pending:
        print(f"\n  Pending Pairing Requests ({len(pending)}):")
        print(f"  {'Platform':<12} {'Code':<10} {'User ID':<20} {'Name':<20} {'Age'}")
        print(f"  {'--------':<12} {'----':<10} {'-------':<20} {'----':<20} {'---'}")
        for p in pending:
            print(
                f"  {p['platform']:<12} {p['code']:<10} {p['user_id']:<20} "
                f"{p.get('user_name', ''):<20} {p['age_minutes']}m ago"
            )
    else:
        print("\n  No pending pairing requests.")

    if approved:
        print(f"\n  Approved Users ({len(approved)}):")
        print(f"  {'Platform':<12} {'User ID':<20} {'Name':<20}")
        print(f"  {'--------':<12} {'-------':<20} {'----':<20}")
        for a in approved:
            print(f"  {a['platform']:<12} {a['user_id']:<20} {a.get('user_name', ''):<20}")
    else:
        print("\n  No approved users.")

    print()


def _cmd_approve(store, platform: str, code: str):
    """Approve a pairing code."""
    platform = platform.lower().strip()
    code = code.upper().strip()

    result = store.approve_code(platform, code)
    if result:
        uid = result["user_id"]
        name = result.get("user_name", "")
        display = f"{name} ({uid})" if name else uid
        print(f"\n  Approved! User {display} on {platform} can now use the bot~")
        print("  They'll be recognized automatically on their next message.\n")
    else:
        print(f"\n  Code '{code}' not found or expired for platform '{platform}'.")
        print("  Run 'hermes pairing list' to see pending codes.\n")


def _cmd_approve_user(store, platform: str, user_id: str, user_name: str = ""):
    """Directly approve a known user without waiting for a pairing code."""
    platform = platform.lower().strip()
    user_id = str(user_id or "").strip()
    user_name = str(user_name or "").strip()

    if not platform or not user_id:
        print("\n  Usage: hermes pairing approve-user <platform> <user_id> [user_name]\n")
        return

    result = store.approve_user(platform, user_id, user_name)
    display = f"{result['user_name']} ({result['user_id']})" if result.get("user_name") else result["user_id"]
    print(f"\n  Approved! User {display} on {platform} can now use the bot~")
    print("  Any stale pending pairing request for this user was cleared.\n")


def _cmd_grant_senior(store, platform: str, user_id: str, user_name: str):
    """Set senior role, map the user ID, and approve the user in one shot."""
    from tools.permission_policy import map_user_id, set_user_role

    platform = platform.lower().strip()
    user_id = str(user_id or "").strip()
    user_name = str(user_name or "").strip()

    if not platform or not user_id or not user_name:
        print("\n  Usage: hermes pairing grant-senior <platform> <user_id> <user_name>\n")
        return

    set_user_role(user_name, "senior")
    map_user_id(user_name, user_id)
    result = store.approve_user(platform, user_id, user_name)
    display = f"{result['user_name']} ({result['user_id']})"
    print(f"\n  Granted senior role and approved {display} on {platform}.\n")


def _cmd_grant_senior_by_name(store, platform: str, user_name: str):
    """Grant senior using an already-known name->user_id mapping."""
    from tools.permission_policy import find_user_id_by_name

    platform = platform.lower().strip()
    user_name = str(user_name or "").strip()

    if not platform or not user_name:
        print("\n  Usage: hermes pairing grant-senior-by-name <platform> <user_name>\n")
        return

    user_id = find_user_id_by_name(user_name)
    if not user_id:
        print(f"\n  No stored user_id mapping found for {user_name}.\n")
        print("  Use 'hermes pairing grant-senior <platform> <user_id> <user_name>' first.\n")
        return

    _cmd_grant_senior(store, platform, user_id, user_name)


def _cmd_grant_repo(user_name: str, repo: str, grant: str):
    """Grant repo ACL to a display name."""
    from tools.permission_policy import grant_repo_acl

    try:
        saved_grant = grant_repo_acl(user_name, repo, grant)
    except ValueError as exc:
        print(f"\n  Could not grant repo ACL: {exc}\n")
        return
    print(f"\n  Granted repo ACL: {user_name} -> {repo} = {saved_grant}.\n")


def _cmd_revoke_repo(user_name: str, repo: str):
    """Revoke repo ACL from a display name."""
    from tools.permission_policy import revoke_repo_acl

    try:
        removed = revoke_repo_acl(user_name, repo)
    except ValueError as exc:
        print(f"\n  Could not revoke repo ACL: {exc}\n")
        return
    if removed:
        print(f"\n  Revoked repo ACL: {user_name} -> {repo}.\n")
    else:
        print(f"\n  No repo ACL found for {user_name} -> {repo}.\n")


def _cmd_list_repo_acl(user_name: str | None = None):
    """List repo ACL grants."""
    import json
    from tools.permission_policy import list_repo_acl

    try:
        grants = list_repo_acl(user_name)
    except ValueError as exc:
        print(f"\n  Could not list repo ACL: {exc}\n")
        return
    print(json.dumps(grants, ensure_ascii=False, indent=2))


def _cmd_revoke(store, platform: str, user_id: str):
    """Revoke a user's access."""
    platform = platform.lower().strip()

    if store.revoke(platform, user_id):
        print(f"\n  Revoked access for user {user_id} on {platform}.\n")
    else:
        print(f"\n  User {user_id} not found in approved list for {platform}.\n")


def _cmd_clear_pending(store):
    """Clear all pending pairing codes."""
    count = store.clear_pending()
    if count:
        print(f"\n  Cleared {count} pending pairing request(s).\n")
    else:
        print("\n  No pending requests to clear.\n")
