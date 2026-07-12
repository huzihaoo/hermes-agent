#!/usr/bin/env python3
"""Offline G1Q3-RCA release-gate smoke checks.

``--fixture-mode`` verifies a frozen, digest-bound local evidence bundle and
``--no-dispatch`` inspects routing source structure without executing candidate
code, VM access, or writes. These offline checks remain non-authorizing and
return No-Go until an external release authority binds the fixture to a frozen
release. Real execution is deliberately unavailable: ``--execute`` always emits
a machine-readable blocker until all governed controls exist.

Exit codes: 2 = a valid offline observation completed but release remains No-Go;
3 = an invalid invocation/evidence observation or the real-execution blocker.
Machine readers distinguish invalid offline input via ``error.code`` and the
disabled execute surface via ``blocker.code``. No mode performs VM or
production I/O.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import re
import stat
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

REPO = Path(__file__).resolve().parent.parent
G1Q3_RCA_GROUP_ID = "oc_6cfc782212009ff4cd815349909dd423"
FIXTURE_SCHEMA = "g1q3-smoke-fixture/v2"
FIXTURE_EVIDENCE = {
    "case/gate_result.json": ("case", "gate_result.json"),
    "case/report_data.json": ("case", "report_data.json"),
    "case/index.html": ("case", "index.html"),
    "artifact/exit.code": ("artifact", "exit.code"),
    "shared-state/task_card.json": ("shared_state", "task_card.json"),
}
REQUIRED_GATES = {f"G{i}" for i in range(7)}
RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{5,63}$")
RELEASE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{5,127}$")
TASK_SLUG_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{5,127}$")
WORK_ITEM_RE = re.compile(r"^[0-9]{1,32}$")
EVIDENCE_IDENTITY_FIELDS = (
    "run_id",
    "task_slug",
    "work_item_id",
    "issue_url",
    "group_id",
)
NON_AUTHORIZING_RESULT = {
    "ok": False,
    "production_ready": False,
    "cutover_go": False,
    "l6_gate_passed": False,
    "execution_authorized": False,
    "dispatch_attempted": False,
    "external_release_binding_verified": False,
}


class InvalidInvocation(ValueError):
    pass


class SmokeArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise InvalidInvocation(message)


def execute_blocker() -> dict:
    """Return the stable blocker contract for intentionally disabled execution."""
    return {
        "code": "G1Q3_REAL_EXECUTION_NOT_IMPLEMENTED",
        "reason": "real dispatch is disabled in this offline smoke gate",
        "missing_controls": [
            "trusted_signed_owner_approval",
            "release_run_nonce_durable_single_use",
            "frozen_repo_head_tree_policy_binding",
            "issue_handoff_requester_group_identity_envelope",
            "atomic_quota_reservation",
            "governed_ssh_mini_mcap_run_dispatch",
            "bounded_resource_timeout_and_task_owned_cleanup",
        ],
        "dispatch_attempted": False,
        "execution_authorized": False,
    }


def _decode_json_object(raw: bytes, *, label: str) -> dict:
    def reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict:
        result: dict = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"{label} contains duplicate JSON key: {key}")
            result[key] = value
        return result

    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=reject_duplicate_keys)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{label} is not valid UTF-8 JSON") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} JSON root must be an object")
    return value


def _quota_has_headroom(hermes_home: Path) -> tuple[bool, str]:
    """Safely observe quota, but never treat an observation as a reservation."""
    day = datetime.now(timezone.utc).date().isoformat()
    ledger = hermes_home / "pnc_agent" / "quota" / f"g1q3_auto_download-{day}.json"
    quota_env = os.environ.get("HERMES_G1Q3_AUTO_DOWNLOAD_DAILY_QUOTA", "").strip()
    try:
        quota = int(quota_env)
    except ValueError:
        return False, "quota configuration is malformed -> BLOCK"
    if quota <= 0:
        return False, "HERMES_G1Q3_AUTO_DOWNLOAD_DAILY_QUOTA<=0 -> auto_download_disabled -> BLOCK"
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(ledger, flags)
    except OSError as exc:
        return False, f"quota ledger unavailable or unsafe ({type(exc).__name__}) -> BLOCK"
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode):
            return False, "quota ledger is not a regular file -> BLOCK"
        if before.st_uid != os.getuid() or before.st_nlink != 1 or before.st_mode & 0o022:
            return False, "quota ledger ownership/link/mode is unsafe -> BLOCK"
        if before.st_size > 64 * 1024:
            return False, "quota ledger exceeds size limit -> BLOCK"
        raw = bytearray()
        while True:
            block = os.read(fd, 8192)
            if not block:
                break
            raw.extend(block)
            if len(raw) > 64 * 1024:
                return False, "quota ledger exceeds size limit -> BLOCK"
        after = os.fstat(fd)
        before_id = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        after_id = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        if before_id != after_id or len(raw) != before.st_size:
            return False, "quota ledger changed while observed -> BLOCK"
    finally:
        os.close(fd)
    try:
        body = _decode_json_object(bytes(raw), label="quota ledger")
        used = body["used"]
        if isinstance(used, bool) or not isinstance(used, int) or used < 0:
            raise ValueError("used must be a non-negative integer")
    except (KeyError, TypeError, ValueError):
        return False, "quota ledger is malformed -> BLOCK"
    if used >= quota:
        return False, f"exhausted used={used}/{quota} -> BLOCK"
    return False, (
        f"observed headroom used={used}/{quota}, but no atomic reservation was made -> BLOCK"
    )


def trigger(
    issue_url: str, work_item: str, requester: str, run_id: str, group_id: str
) -> tuple[str, str, dict]:
    """Compatibility symbol that can no longer reach a dispatcher."""
    del issue_url, work_item, requester, run_id, group_id
    raise RuntimeError(json.dumps(execute_blocker(), sort_keys=True))


def judge_green_checks(
    *,
    gate: dict | None,
    report: dict | None,
    index_html_bytes: int | None,
    report_data_bytes: int | None,
    exit_code: str | None,
    card_delivery: dict | None = None,
) -> dict:
    """Judge frozen evidence without performing I/O."""
    checks: dict = {}
    # 1) G0-G6 all pass
    gates = (gate or {}).get("gates", gate)
    if isinstance(gates, list):
        entries = [g for g in gates if isinstance(g, dict)]
        statuses = {g.get("gate"): g.get("status") for g in entries}
        gates_unique = len(entries) == len(gates) == len(statuses)
    elif isinstance(gates, dict):
        statuses = {k: (v.get("status") if isinstance(v, dict) else v) for k, v in gates.items()}
        gates_unique = True
    else:
        statuses = {}
        gates_unique = False
    g_pass = gates_unique and set(statuses) == REQUIRED_GATES and all(
        str(statuses[name]).lower() == "pass" for name in REQUIRED_GATES
    )
    checks["1_gates_all_pass"] = {"ok": g_pass, "detail": statuses}

    summary = (report or {}).get("summary") or {}
    status = summary.get("status")
    # 2) status honest
    status_ok = status in {"hypothesis_ready", "report_ready"}
    banner = str(summary.get("ui_banner_title") or "") + str(summary.get("high_confidence_boundary") or "")
    # honesty: hypothesis_ready must NOT be washed to a completed/定责 banner
    honest = True
    if status == "hypothesis_ready" and re.search(r"已完成|completed|已定责", banner, re.IGNORECASE):
        honest = False
    checks["2_status_honest"] = {"ok": bool(status_ok and honest), "status": status,
                                 "boundary": summary.get("high_confidence_boundary")}

    # 3) field lineage: no dropped decoded fields
    fl = (report or {}).get("field_lineage") or {}
    dropped = fl.get("manifest_decoded_dropped")
    checks["3_manifest_decoded_dropped_empty"] = {
        "ok": dropped == [] and fl.get("fidelity_ok") is True,
        "dropped": dropped,
        "fidelity_ok": fl.get("fidelity_ok"),
    }

    # 4) artifacts non-empty + exit0
    checks["4_artifacts_exit0"] = {
        "ok": bool(
            index_html_bytes
            and index_html_bytes > 0
            and report_data_bytes
            and report_data_bytes > 0
            and exit_code == "0"
        ),
        "index_html_bytes": index_html_bytes,
        "report_data_bytes": report_data_bytes,
        "exit_code": exit_code,
    }

    # 5) Card delivery is only scored when an isolated shared-state fixture is
    # supplied. Real headless execution keeps the boundary explicit.
    if card_delivery is None:
        checks["5_card_delivery"] = {
            "ok": None,
            "note": "NOT-COVERED (headless boundary): requires isolated shared-state replay or owner-gated A3 canary.",
        }
    else:
        report_status = str(card_delivery.get("report_status") or "")
        has_report = card_delivery.get("has_deliverable_report") is True
        user_state = str(card_delivery.get("user_state") or "")
        card_ok = (
            report_status in {"report_ready", "html_delivery_ready", "hypothesis_ready"}
            and has_report
            and user_state in {"done", "completed", "review"}
        )
        checks["5_card_delivery"] = {
            "ok": card_ok,
            "report_status": report_status,
            "has_deliverable_report": has_report,
            "user_state": user_state,
        }
    return checks


def _canonical_isolated_dir(path: Path, isolation_root: Path, *, label: str) -> Path:
    if not path.is_absolute():
        raise ValueError(f"{label} must be absolute")
    resolved = Path(os.path.normpath(str(path)))
    if resolved != path:
        raise ValueError(f"{label} must be a canonical real directory")
    try:
        relative = resolved.relative_to(isolation_root)
    except ValueError as exc:
        raise ValueError(f"{label} escapes isolation root") from exc
    if relative.parent != Path("."):
        raise ValueError(f"{label} must be a direct child of isolation root")
    try:
        info = resolved.lstat()
    except OSError as exc:
        raise ValueError(f"{label} is unavailable") from exc
    if not stat.S_ISDIR(info.st_mode):
        raise ValueError(f"{label} must be a canonical real directory")
    if info.st_uid != os.getuid() or info.st_mode & 0o222:
        raise ValueError(f"{label} must be user-owned and frozen read-only")
    return resolved


def _reject_symlink_components(path: Path, *, label: str) -> None:
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        try:
            info = current.lstat()
        except OSError as exc:
            raise ValueError(f"{label} path component is unavailable: {current}") from exc
        if stat.S_ISLNK(info.st_mode):
            raise ValueError(f"{label} cannot contain symlink components: {current}")


def validate_fixture_roots(
    *, isolation_root: Path, case_root: Path, artifact_root: Path, shared_state_root: Path
) -> tuple[Path, Path, Path, Path]:
    if not isolation_root.is_absolute():
        raise ValueError("isolation root must be absolute")
    lexical = Path(os.path.normpath(str(isolation_root)))
    if lexical != isolation_root:
        raise ValueError("isolation root must be lexically canonical")
    forbidden = (
        Path.home() / ".hermes",
        Path.home() / "Mounts",
        Path("/mnt"),
        Path("/Volumes"),
    )
    for blocked in forbidden:
        try:
            lexical.relative_to(blocked)
        except ValueError:
            continue
        raise ValueError(f"isolation root is under forbidden production root: {blocked}")
    _reject_symlink_components(lexical, label="isolation root")
    isolation = lexical
    try:
        isolation_info = isolation.lstat()
    except OSError as exc:
        raise ValueError("isolation root is unavailable") from exc
    if not stat.S_ISDIR(isolation_info.st_mode):
        raise ValueError("isolation root must be a canonical real directory")
    if isolation_info.st_uid != os.getuid() or isolation_info.st_mode & 0o222:
        raise ValueError("isolation root must be user-owned and frozen read-only")
    for blocked in forbidden:
        try:
            isolation.relative_to(blocked)
        except ValueError:
            continue
        raise ValueError(f"isolation root is under forbidden production root: {blocked}")
    roots = (
        _canonical_isolated_dir(case_root, isolation, label="case root"),
        _canonical_isolated_dir(artifact_root, isolation, label="artifact root"),
        _canonical_isolated_dir(shared_state_root, isolation, label="shared-state root"),
    )
    if len(set(roots)) != len(roots):
        raise ValueError("case/artifact/shared-state roots must be distinct")
    return isolation, roots[0], roots[1], roots[2]


def _open_frozen_directory(
    path: str | Path, *, label: str, dir_fd: int | None = None
) -> int:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_DIRECTORY", 0)
    )
    try:
        fd = os.open(path, flags, dir_fd=dir_fd)
    except OSError as exc:
        raise ValueError(f"{label} cannot be safely opened: {exc}") from exc
    info = os.fstat(fd)
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o222:
        os.close(fd)
        raise ValueError(f"{label} must be a user-owned frozen directory")
    return fd


def _open_absolute_directory(
    path: Path, *, label: str, require_frozen: bool = True
) -> int:
    """Open every absolute path component without following symlinks."""
    if not path.is_absolute() or Path(os.path.normpath(str(path))) != path:
        raise ValueError(f"{label} must be an absolute canonical path")
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_DIRECTORY", 0)
    )
    fd = os.open(path.anchor, flags)
    try:
        for part in path.parts[1:]:
            next_fd = os.open(part, flags, dir_fd=fd)
            os.close(fd)
            fd = next_fd
        info = os.fstat(fd)
        if not stat.S_ISDIR(info.st_mode):
            raise ValueError(f"{label} must be a directory")
        if require_frozen and (info.st_uid != os.getuid() or info.st_mode & 0o222):
            raise ValueError(f"{label} must be a user-owned frozen directory")
        return fd
    except Exception:
        os.close(fd)
        raise


def _directory_identity(fd: int) -> tuple[int, ...]:
    info = os.fstat(fd)
    if info.st_mode & 0o222:
        raise ValueError("fixture directory became writable during verification")
    return (info.st_dev, info.st_ino, info.st_mode, info.st_mtime_ns, info.st_ctime_ns)


def _assert_path_still_matches_fd(path: Path, fd: int, *, label: str) -> None:
    try:
        path_info = path.lstat()
    except OSError as exc:
        raise ValueError(f"{label} path was replaced during verification") from exc
    fd_info = os.fstat(fd)
    if stat.S_ISLNK(path_info.st_mode) or (
        path_info.st_dev,
        path_info.st_ino,
    ) != (fd_info.st_dev, fd_info.st_ino):
        raise ValueError(f"{label} path was replaced during verification")


def _stat_identity(info: os.stat_result) -> tuple[int, ...]:
    return (
        info.st_dev,
        info.st_ino,
        info.st_mode,
        info.st_uid,
        info.st_gid,
        info.st_nlink,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )


def _read_fixture_entry(
    dir_fd: int, name: str, *, label: str, max_bytes: int = 16 * 1024 * 1024
) -> tuple[bytes, tuple[int, ...]]:
    if not name or Path(name).name != name:
        raise ValueError(f"fixture entry name is not canonical: {label}")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(name, flags, dir_fd=dir_fd)
    except OSError as exc:
        raise ValueError(f"fixture file cannot be safely opened: {label}: {exc}") from exc
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode):
            raise ValueError(f"fixture is not a regular file: {label}")
        if before.st_uid != os.getuid() or before.st_nlink != 1:
            raise ValueError(f"fixture ownership/hardlink check failed: {label}")
        if before.st_mode & 0o222:
            raise ValueError(f"fixture file must be frozen read-only: {label}")
        if before.st_size > max_bytes:
            raise ValueError(f"fixture file too large: {label}")
        data = bytearray()
        while True:
            block = os.read(fd, 65536)
            if not block:
                break
            data.extend(block)
            if len(data) > max_bytes:
                raise ValueError(f"fixture file too large: {label}")
        after = os.fstat(fd)
        identity_before = (
            before.st_dev,
            before.st_ino,
            before.st_mode,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        )
        identity_after = (
            after.st_dev,
            after.st_ino,
            after.st_mode,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        )
        if identity_before != identity_after or len(data) != before.st_size:
            raise ValueError(f"fixture changed while being read: {label}")
        return bytes(data), identity_after
    finally:
        os.close(fd)


def _parse_issue_url(issue_url: str) -> tuple[str, str]:
    parsed = urlsplit(issue_url)
    if (
        parsed.scheme != "https"
        or parsed.netloc != "project.feishu.cn"
        or parsed.query
        or parsed.fragment
        or "%" in parsed.path
    ):
        raise ValueError("issue URL must be canonical project.feishu.cn HTTPS without query/fragment")
    parts = parsed.path.split("/")
    if len(parts) != 5 or parts[0] or parts[2:4] != ["issue", "detail"]:
        raise ValueError("issue URL path must be /<project>/issue/detail/<work-item>")
    project_key, work_item = parts[1], parts[4]
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", project_key) or not WORK_ITEM_RE.fullmatch(work_item):
        raise ValueError("issue URL project/work-item identity is invalid")
    canonical = f"https://project.feishu.cn/{project_key}/issue/detail/{work_item}"
    if issue_url != canonical:
        raise ValueError("issue URL is not canonical")
    return project_key, work_item


def _validate_identity(*, issue_url: str, work_item: str, group_id: str) -> dict:
    project_key, url_work_item = _parse_issue_url(issue_url)
    if not WORK_ITEM_RE.fullmatch(work_item) or work_item != url_work_item:
        raise ValueError("explicit work-item does not match issue URL")
    if group_id != G1Q3_RCA_GROUP_ID:
        raise ValueError("group-id does not match the fixed G1Q3-RCA release gate")
    return {
        "project_key": project_key,
        "work_item_id": work_item,
        "issue_url": issue_url,
        "group_id": group_id,
    }


def _read_local_routing_policy() -> tuple[bytes, str, bool]:
    """Read policy bytes stably without importing or executing them."""
    policy_path = REPO / "gateway" / "pnc_group_binding.py"
    parent_fd = _open_absolute_directory(
        policy_path.parent, label="routing policy parent", require_frozen=False
    )
    try:
        parent_identity = _stat_identity(os.fstat(parent_fd))
        snapshots = []
        for _pass in range(2):
            flags = (
                os.O_RDONLY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0)
            )
            try:
                fd = os.open(policy_path.name, flags, dir_fd=parent_fd)
            except OSError as exc:
                raise ValueError(f"routing policy cannot be safely opened: {exc}") from exc
            try:
                before = os.fstat(fd)
                if (
                    not stat.S_ISREG(before.st_mode)
                    or before.st_uid != os.getuid()
                    or before.st_nlink != 1
                    or before.st_size > 1024 * 1024
                ):
                    raise ValueError("routing policy ownership/type/link/size is unsafe")
                source = bytearray()
                while True:
                    block = os.read(fd, 65536)
                    if not block:
                        break
                    source.extend(block)
                    if len(source) > 1024 * 1024:
                        raise ValueError("routing policy exceeds size limit")
                after = os.fstat(fd)
                identity = _stat_identity(after)
                if identity != _stat_identity(before) or len(source) != before.st_size:
                    raise ValueError("routing policy changed while being read")
                snapshots.append((bytes(source), identity))
            finally:
                os.close(fd)
        _assert_path_still_matches_fd(
            policy_path.parent, parent_fd, label="routing policy parent"
        )
        if _stat_identity(os.fstat(parent_fd)) != parent_identity:
            raise ValueError("routing policy parent changed during verification")
    finally:
        os.close(parent_fd)
    if snapshots[0] != snapshots[1]:
        raise ValueError("routing policy changed between verification passes")
    try:
        final_info = policy_path.lstat()
    except OSError as exc:
        raise ValueError("routing policy path was replaced during verification") from exc
    final_identity = snapshots[1][1]
    if stat.S_ISLNK(final_info.st_mode) or _stat_identity(final_info) != final_identity:
        raise ValueError("routing policy path changed or was replaced during verification")
    source = snapshots[1][0]
    digest = hashlib.sha256(source).hexdigest()
    try:
        parent_info = policy_path.parent.lstat()
    except OSError as exc:
        raise ValueError("routing policy parent was replaced during verification") from exc
    if stat.S_ISLNK(parent_info.st_mode) or _stat_identity(parent_info) != parent_identity:
        raise ValueError("routing policy parent changed or was replaced during verification")
    frozen = bool(
        final_info.st_uid == os.getuid()
        and final_info.st_nlink == 1
        and not final_info.st_mode & 0o222
        and stat.S_ISDIR(parent_info.st_mode)
        and parent_info.st_uid == os.getuid()
        and not parent_info.st_mode & 0o222
    )
    return source, digest, frozen


def _inspect_local_routing_policy() -> dict:
    """Inspect the policy contract with AST only; never execute candidate code."""
    source, digest, frozen = _read_local_routing_policy()
    try:
        text = source.decode("utf-8")
        tree = ast.parse(text, filename=str(REPO / "gateway" / "pnc_group_binding.py"))
    except (UnicodeError, SyntaxError) as exc:
        raise ValueError("routing policy is not valid UTF-8 Python") from exc

    group_constant = None
    entrypoint_args: list[str] | None = None
    unsafe_top_level_expression_lines = [
        node.lineno
        for node in tree.body
        if isinstance(node, ast.Expr)
        and not isinstance(node.value, ast.Constant)
    ]
    for node in tree.body:
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if any(isinstance(target, ast.Name) and target.id == "G1Q3_RCA_GROUP_ID" for target in targets):
                try:
                    group_constant = ast.literal_eval(node.value)
                except (ValueError, TypeError):
                    group_constant = None
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "evaluate_pnc_group_request":
            entrypoint_args = [arg.arg for arg in (*node.args.args, *node.args.kwonlyargs)]

    required_args = {"platform", "chat_id", "text"}
    return {
        "routing_policy_sha256": digest,
        "policy_source_frozen": frozen,
        "policy_execution_performed": False,
        "ast_parse_ok": True,
        "group_constant_matches": group_constant == G1Q3_RCA_GROUP_ID,
        "entrypoint_signature_matches": (
            entrypoint_args is not None and required_args.issubset(entrypoint_args)
        ),
        "top_level_expression_safe": not unsafe_top_level_expression_lines,
        "unsafe_top_level_expression_lines": unsafe_top_level_expression_lines,
        "semantic_route_evaluation_performed": False,
    }


def _validate_fixture_manifest(
    manifest: dict,
    *,
    roots: dict[str, str],
    evidence: dict[str, bytes],
) -> dict:
    required_top = {"schema_version", "identity", "roots", "authorization", "evidence"}
    if set(manifest) != required_top or manifest.get("schema_version") != FIXTURE_SCHEMA:
        raise ValueError("fixture manifest schema/top-level fields are invalid")
    if manifest.get("roots") != roots:
        raise ValueError("fixture manifest roots do not match opened roots")
    authorization = manifest.get("authorization")
    if authorization != {
        "execution_authorized": False,
        "dispatch_attempted": False,
        "approval_receipt_id": None,
    }:
        raise ValueError("fixture manifest must explicitly be non-authorizing and non-dispatching")
    identity = manifest.get("identity")
    identity_fields = {
        "release_id",
        "run_id",
        "task_slug",
        "work_item_id",
        "issue_url",
        "group_id",
        "source_commit",
        "source_tree",
        "policy_sha256",
    }
    if not isinstance(identity, dict) or set(identity) != identity_fields:
        raise ValueError("fixture identity envelope is incomplete")
    release_id = identity.get("release_id")
    run_id = identity.get("run_id")
    task_slug = identity.get("task_slug")
    work_item = identity.get("work_item_id")
    if not isinstance(release_id, str) or not RELEASE_ID_RE.fullmatch(release_id):
        raise ValueError("fixture release_id is invalid")
    if not isinstance(run_id, str) or not RUN_ID_RE.fullmatch(run_id):
        raise ValueError("fixture run_id is invalid")
    if not isinstance(task_slug, str) or not TASK_SLUG_RE.fullmatch(task_slug):
        raise ValueError("fixture task_slug is invalid")
    for field, length in (("source_commit", 40), ("source_tree", 40), ("policy_sha256", 64)):
        value = identity.get(field)
        if not isinstance(value, str) or not re.fullmatch(rf"[0-9a-f]{{{length}}}", value):
            raise ValueError(f"fixture {field} is invalid")
    bound = _validate_identity(
        issue_url=str(identity.get("issue_url") or ""),
        work_item=str(work_item or ""),
        group_id=str(identity.get("group_id") or ""),
    )
    expected_slug = f"g1q3_rca_issue_intake_{work_item}_{run_id}"
    if task_slug != expected_slug:
        raise ValueError("fixture task_slug is not bound to work-item and run_id")
    digests = manifest.get("evidence")
    if not isinstance(digests, dict) or set(digests) != set(FIXTURE_EVIDENCE):
        raise ValueError("fixture evidence digest set is incomplete")
    for logical_path, data in evidence.items():
        expected = digests.get(logical_path)
        actual = hashlib.sha256(data).hexdigest()
        if not isinstance(expected, str) or not re.fullmatch(r"[0-9a-f]{64}", expected):
            raise ValueError(f"fixture digest is invalid: {logical_path}")
        if expected != actual:
            raise ValueError(f"fixture digest mismatch: {logical_path}")
    return {
        "release_id": release_id,
        "run_id": run_id,
        "task_slug": task_slug,
        "source_commit": identity["source_commit"],
        "source_tree": identity["source_tree"],
        "policy_sha256": identity["policy_sha256"],
        **bound,
    }


def _validate_evidence_identity(*, body: dict, identity: dict, label: str) -> None:
    expected = {field: identity[field] for field in EVIDENCE_IDENTITY_FIELDS}
    if body.get("fixture_identity") != expected:
        raise ValueError(f"{label} fixture identity does not match manifest")


def _validate_task_card_handoff(*, body: dict, identity: dict) -> None:
    expected = {
        "contract_version": "g1q3_rca_group_handoff_v2",
        "run_id": identity["run_id"],
        "task_slug": identity["task_slug"],
        "work_item_id": identity["work_item_id"],
        "issue_url": identity["issue_url"],
        "source_group_id": identity["group_id"],
    }
    if body.get("fixture_handoff_contract") != expected:
        raise ValueError("task card handoff identity does not match manifest")
    task_card = body.get("task_card")
    expected_card_identity = {
        "task_id": identity["task_slug"],
        "run_id": identity["run_id"],
        "work_item_id": identity["work_item_id"],
        "issue_url": identity["issue_url"],
        "chat_id": identity["group_id"],
    }
    if not isinstance(task_card, dict) or any(
        task_card.get(field) != value
        for field, value in expected_card_identity.items()
    ):
        raise ValueError("task card native identity does not match manifest")


def fixture_checks(
    *, isolation_root: Path, case_root: Path, artifact_root: Path, shared_state_root: Path
) -> dict:
    isolation_fd = _open_absolute_directory(isolation_root, label="isolation root")
    fds: dict[str, int] = {"isolation": isolation_fd}
    paths = {
        "isolation": isolation_root,
        "case": case_root,
        "artifact": artifact_root,
        "shared_state": shared_state_root,
    }
    try:
        for key, root in (
            ("case", case_root),
            ("artifact", artifact_root),
            ("shared_state", shared_state_root),
        ):
            fds[key] = _open_frozen_directory(
                root.name, label=f"{key} root", dir_fd=isolation_fd
            )
        directories_before = {key: _directory_identity(fd) for key, fd in fds.items()}
        snapshots: list[dict[str, tuple[bytes, tuple[int, ...]]]] = []
        locations = {
            "fixture_manifest.json": ("isolation", "fixture_manifest.json"),
            **FIXTURE_EVIDENCE,
        }
        for _pass in range(2):
            snapshots.append({
                logical_path: _read_fixture_entry(
                    fds[root_key], name, label=logical_path
                )
                for logical_path, (root_key, name) in locations.items()
            })
        directories_after = {key: _directory_identity(fd) for key, fd in fds.items()}
        if directories_before != directories_after or snapshots[0] != snapshots[1]:
            raise ValueError("fixture set changed between frozen verification passes")
        entry_inodes = {
            (identity[0], identity[1])
            for _data, identity in snapshots[1].values()
        }
        if len(entry_inodes) != len(snapshots[1]):
            raise ValueError("fixture entries must have distinct file identities")
        for key, fd in fds.items():
            _assert_path_still_matches_fd(paths[key], fd, label=f"{key} root")
        frozen = {key: value[0] for key, value in snapshots[1].items()}
    finally:
        for fd in reversed(list(fds.values())):
            os.close(fd)

    manifest = _decode_json_object(
        frozen.pop("fixture_manifest.json"), label="fixture manifest"
    )
    roots = {
        "case": case_root.name,
        "artifact": artifact_root.name,
        "shared_state": shared_state_root.name,
    }
    identity = _validate_fixture_manifest(manifest, roots=roots, evidence=frozen)
    gate = _decode_json_object(frozen["case/gate_result.json"], label="gate result")
    report_bytes = frozen["case/report_data.json"]
    report = _decode_json_object(report_bytes, label="report data")
    card_body = _decode_json_object(
        frozen["shared-state/task_card.json"], label="task card"
    )
    _validate_evidence_identity(body=gate, identity=identity, label="gate result")
    _validate_evidence_identity(body=report, identity=identity, label="report data")
    _validate_evidence_identity(body=card_body, identity=identity, label="task card")
    _validate_task_card_handoff(body=card_body, identity=identity)
    task_card = card_body.get("task_card") if isinstance(card_body.get("task_card"), dict) else {}
    card_delivery = task_card.get("delivery") if isinstance(task_card.get("delivery"), dict) else None
    if card_delivery is None:
        card_delivery = (
            card_body.get("delivery") if isinstance(card_body.get("delivery"), dict) else card_body
        )
    checks = judge_green_checks(
        gate=gate,
        report=report,
        index_html_bytes=len(frozen["case/index.html"]),
        report_data_bytes=len(report_bytes),
        exit_code=frozen["artifact/exit.code"].decode("utf-8").strip(),
        card_delivery=card_delivery,
    )
    return {
        "0_fixture_manifest_self_consistent": {
            "ok": True,
            **identity,
            "execution_authorized": False,
            "dispatch_attempted": False,
        },
        "0_external_release_binding": {
            "ok": False,
            "external_release_binding_verified": False,
            "production_ready": False,
            "l6_gate_passed": False,
            "cutover_go": False,
            "reason": "fixture manifest is self-declared; no external release authority was supplied",
        },
        **checks,
    }


def no_dispatch_decision(*, issue_url: str, work_item: str, group_id: str) -> dict:
    identity = _validate_identity(
        issue_url=issue_url, work_item=work_item, group_id=group_id
    )
    policy = _inspect_local_routing_policy()
    structure_observed = bool(
        policy["ast_parse_ok"]
        and policy["group_constant_matches"]
        and policy["entrypoint_signature_matches"]
        and policy["top_level_expression_safe"]
    )
    frozen_contract_observed = bool(
        structure_observed and policy["policy_source_frozen"]
    )
    return {
        "ok": False,
        "offline_check_passed": frozen_contract_observed,
        "offline_policy_contract_observed": frozen_contract_observed,
        "policy_structure_observed": structure_observed,
        "decision": "not_executed",
        "identity": identity,
        "handoff_work_item_id": None,
        "handoff_identity_verified": False,
        **policy,
        "execution_authorized": False,
        "dispatch_attempted": False,
        "external_release_binding_verified": False,
        "production_ready": False,
        "cutover_go": False,
        "l6_gate_passed": False,
        "authorization_note": (
            "Only a frozen AST contract can pass this offline observation; it is not semantic "
            "routing, handoff, release, or execution authorization"
        ),
    }


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------
def _build_parser() -> argparse.ArgumentParser:
    p = SmokeArgumentParser(
        prog="g1q3_rca_e2e_smoke",
        description="Offline G1Q3-RCA frozen-fixture and no-dispatch release gate.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    modes = p.add_mutually_exclusive_group(required=True)
    modes.add_argument("--fixture-mode", action="store_true",
                       help="read only explicitly isolated local fixture roots")
    modes.add_argument("--no-dispatch", "--dry-run", dest="no_dispatch", action="store_true",
                       help="routing AST contract only; no code execution, VM access, write, or dispatch")
    modes.add_argument("--execute", action="store_true",
                       help="disabled; always returns a machine-readable release blocker")
    p.add_argument("--hermes-home")
    p.add_argument("--issue-url")
    p.add_argument("--work-item")
    p.add_argument("--requester")
    p.add_argument("--group-id")
    p.add_argument("--run-id")
    p.add_argument("--confirm-execute", help="legacy argument; ignored because execution is disabled")
    p.add_argument("--isolation-root", type=Path)
    p.add_argument("--case-root", type=Path)
    p.add_argument("--artifact-root", type=Path)
    p.add_argument("--shared-state-root", type=Path)
    p.add_argument("--timeout", type=float, default=1200.0, help="legacy execute argument; ignored")
    p.add_argument("--json", action="store_true")
    return p


def main(argv: list[str] | None = None) -> int:
    raw_argv = list(argv) if argv is not None else sys.argv[1:]
    try:
        args = _build_parser().parse_args(raw_argv)
    except InvalidInvocation as exc:
        result = {
            "mode": "invalid",
            **NON_AUTHORIZING_RESULT,
            "offline_check_passed": False,
            "gate_scope": "offline_non_authorizing_only",
            "error": {
                "code": "G1Q3_INVALID_INVOCATION",
                "reason": str(exc),
            },
        }
        if "--json" in raw_argv:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            print(f"g1q3_rca_e2e_smoke: error: {exc}", file=sys.stderr)
        return 3
    mode = "fixture" if args.fixture_mode else ("no-dispatch" if args.no_dispatch else "execute")
    result: dict = {
        "mode": mode,
        **NON_AUTHORIZING_RESULT,
        "offline_check_passed": False,
        "gate_scope": "offline_non_authorizing_only",
    }

    if args.fixture_mode:
        incompatible = sorted(
            name
            for name in ("hermes_home", "issue_url", "work_item", "requester", "group_id", "run_id", "confirm_execute")
            if getattr(args, name) is not None
        )
        if incompatible:
            result["error"] = {
                "code": "G1Q3_INVALID_INVOCATION",
                "reason": "fixture mode received incompatible arguments",
                "arguments": incompatible,
            }
            if args.json:
                print(json.dumps(result, ensure_ascii=False, indent=2))
            return 3
        roots = (args.isolation_root, args.case_root, args.artifact_root, args.shared_state_root)
        if any(path is None for path in roots):
            result["error"] = {
                "code": "G1Q3_INVALID_INVOCATION",
                "reason": "fixture mode requires isolation/case/artifact/shared-state roots",
            }
            if args.json:
                print(json.dumps(result, ensure_ascii=False, indent=2))
            return 3
        try:
            isolation, case_root, artifact_root, shared_state_root = validate_fixture_roots(
                isolation_root=args.isolation_root,
                case_root=args.case_root,
                artifact_root=args.artifact_root,
                shared_state_root=args.shared_state_root,
            )
            checks = fixture_checks(
                isolation_root=isolation,
                case_root=case_root,
                artifact_root=artifact_root,
                shared_state_root=shared_state_root,
            )
            result["roots"] = {
                "isolation": str(isolation),
                "case": str(case_root),
                "artifact": str(artifact_root),
                "shared_state": str(shared_state_root),
            }
            result["green_checks"] = checks
            scored = [
                item.get("ok")
                for name, item in checks.items()
                if name != "0_external_release_binding" and item.get("ok") is not None
            ]
            result["offline_check_passed"] = bool(scored) and all(scored)
        except Exception as exc:
            result["error"] = {
                "code": "G1Q3_INVALID_FIXTURE_EVIDENCE",
                "reason": str(exc),
                "exception_type": type(exc).__name__,
            }
            if args.json:
                print(json.dumps(result, ensure_ascii=False, indent=2))
            return 3
        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        return 2

    if args.no_dispatch:
        incompatible = sorted(
            name
            for name in ("hermes_home", "requester", "run_id", "confirm_execute")
            if getattr(args, name) is not None
        )
        if any(
            value is not None
            for value in (
                args.isolation_root,
                args.case_root,
                args.artifact_root,
                args.shared_state_root,
            )
        ):
            incompatible.append("fixture_roots")
        if incompatible:
            result["error"] = {
                "code": "G1Q3_INVALID_INVOCATION",
                "reason": "no-dispatch mode received incompatible arguments",
                "arguments": sorted(incompatible),
            }
            if args.json:
                print(json.dumps(result, ensure_ascii=False, indent=2))
            return 3
        if not args.issue_url or not args.work_item or not args.group_id:
            result["error"] = {
                "code": "G1Q3_INVALID_INVOCATION",
                "reason": (
                    "no-dispatch mode requires explicit --issue-url, --work-item, and --group-id"
                ),
            }
            if args.json:
                print(json.dumps(result, ensure_ascii=False, indent=2))
            return 3
        try:
            result["decision"] = no_dispatch_decision(
                issue_url=args.issue_url,
                work_item=args.work_item,
                group_id=args.group_id,
            )
            result["offline_check_passed"] = bool(
                result["decision"].get("offline_check_passed")
            )
        except Exception as exc:
            result["error"] = {
                "code": "G1Q3_INVALID_OFFLINE_EVIDENCE",
                "reason": str(exc),
                "exception_type": type(exc).__name__,
            }
            result["decision"] = {
                "ok": False,
                "offline_check_passed": False,
                "error": result["error"],
            }
            if args.json:
                print(json.dumps(result, ensure_ascii=False, indent=2))
            return 3
        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        return 2

    # --execute is a compatibility surface only. It deliberately does not
    # inspect its arguments, environment, filesystem, VM, or production state.
    result["blocker"] = execute_blocker()
    print(json.dumps(result, ensure_ascii=False, indent=2 if args.json else None))
    return 3


if __name__ == "__main__":
    raise SystemExit(main())
