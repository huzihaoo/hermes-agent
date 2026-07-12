#!/usr/bin/env python3
"""G1Q3-RCA end-to-end delivery smoke gate.

Fixed release-gate for the G1Q3-RCA intake -> pipeline -> report chain.  This is
the executable form of runbook PNC_BUSINESS_OVERLAY_RELEASE_RUNBOOK.md §S5.5
"headless back-half smoke": it triggers the REAL datapipe coordinator (not a
mock), watches the real VM pipeline to completion, then verifies the §S5.5 five
green-check fields against the real case_dir.

The existing no-flag/full behavior still dispatches the detached coordinator,
downloads (~7.5G), runs s1->s6, and verifies.  Candidate pre-cutover checks use
the separate ``--no-dispatch`` record-only path with six explicit isolated
roots; that path never enters preflight, dotenv, gateway, VM, or Feishu code.

Coverage boundary (honest, printed): this headless path drives the execution
chain (s1_gate..s6_report + green checks) but does NOT exercise the long-lived
gateway websocket ingress nor the card-delivery/relay path — those require a
real human @-mention (A3), a physical constraint (the gateway drops bot-sent
messages via self_echo/bots_disabled by design).  See §S5.5.

Exit codes: 0 = real/dry-run PASS; 2 = smoke FAIL/timeout or a valid candidate
no-dispatch observation that remains No-Go; 3 = preflight/isolation/invocation
gate failed — smoke not attempted.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

REPO = Path(__file__).resolve().parent.parent
SSH_MINI_AGENT = str(Path.home() / ".local" / "bin" / "ssh-mini-agent")

# Fixed smoke case (owner-designated, 2026-07-10): ACC follow-stop, PDCL event.
DEFAULT_ISSUE_URL = "https://project.feishu.cn/t03o4q/issue/detail/7041712812"
DEFAULT_WORK_ITEM = "7041712812"
DEFAULT_REQUESTER = "ou_d1d3cfeba1be0a22faa36aaf4fb3907d"  # owner (胡子豪)
G1Q3_RCA_GROUP_ID = "oc_6cfc782212009ff4cd815349909dd423"
CASE_ROOT = "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases"

# Candidate-only record path.  The local candidate sandbox and the governed VM
# smoke landing are the only namespaces accepted by --no-dispatch.
NO_DISPATCH_ALLOWED_BASES = (
    Path.home() / "hermes-candidate-sandboxes",
    Path("/mnt/tmp/hermes-v0182-smoke-20260710"),
)
NO_DISPATCH_RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{5,63}$")
NO_DISPATCH_ROOT_FIELDS = (
    "case_root",
    "artifact_root",
    "shared_state_root",
    "output_root",
    "work_root",
    "download_root",
)

STAGE_ORDER = [
    "s1_gate", "s2_download", "s3a_materialize", "s3b_translate",
    "s5_alignment", "s6_report",
]


# --------------------------------------------------------------------------
# small helpers
# --------------------------------------------------------------------------
def _log(msg: str) -> None:
    print(msg, flush=True)


def _run_vm_py(script: str, timeout: float = 45.0) -> tuple[int, str, str]:
    """Run a python snippet on the VM via ssh-mini-agent run_py_json."""
    proc = subprocess.run(
        [SSH_MINI_AGENT, "run_py_json"],
        input=script, text=True, capture_output=True, timeout=timeout,
    )
    return proc.returncode, proc.stdout, proc.stderr


def _vm_read_json(path: str, lines: int = 800, timeout: float = 45.0) -> dict | None:
    """Read a JSON file off the VM; return parsed dict or None."""
    proc = subprocess.run(
        [SSH_MINI_AGENT, "read_file", path, "--start", "1", "--lines", str(lines)],
        text=True, capture_output=True, timeout=timeout,
    )
    if proc.returncode != 0:
        return None
    try:
        return json.loads(proc.stdout)
    except Exception:
        return None


# --------------------------------------------------------------------------
# candidate --no-dispatch record-only path (local filesystem only)
# --------------------------------------------------------------------------
class NoDispatchIsolationError(ValueError):
    """Raised before any candidate record is written when isolation is unsafe."""


def _canonical_existing_directory(path: Path, *, label: str) -> Path:
    if not path.is_absolute() or Path(os.path.normpath(str(path))) != path:
        raise NoDispatchIsolationError(f"{label} must be an absolute canonical path")
    try:
        resolved = path.resolve(strict=True)
        info = path.lstat()
    except OSError as exc:
        raise NoDispatchIsolationError(f"{label} is unavailable: {exc}") from exc
    if resolved != path or stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise NoDispatchIsolationError(f"{label} must be a real directory without symlinks")
    if info.st_uid != os.getuid():
        raise NoDispatchIsolationError(f"{label} must be owned by the current user")
    if info.st_mode & 0o022:
        raise NoDispatchIsolationError(f"{label} must not be group/world writable")
    return path


def _path_is_within(path: Path, base: Path) -> bool:
    try:
        path.relative_to(base)
    except ValueError:
        return False
    return True


def validate_no_dispatch_roots(
    *,
    run_id: str,
    case_root: Path,
    artifact_root: Path,
    shared_state_root: Path,
    output_root: Path,
    work_root: Path,
    download_root: Path,
) -> dict[str, Path]:
    """Bind all candidate paths to one isolated run namespace.

    All six roots must already exist as distinct direct children of a directory
    named after ``run_id``.  That run directory must be below the candidate
    sandbox base or the fixed governed VM smoke landing.  This makes an
    accidental live ``~/.hermes``/production shared-state path fail closed.
    """
    if not NO_DISPATCH_RUN_ID_RE.fullmatch(str(run_id or "")):
        raise NoDispatchIsolationError(
            "run-id must be 6-64 characters using only letters, digits, '_' or '-'"
        )
    raw = {
        "case_root": case_root,
        "artifact_root": artifact_root,
        "shared_state_root": shared_state_root,
        "output_root": output_root,
        "work_root": work_root,
        "download_root": download_root,
    }
    roots = {
        name: _canonical_existing_directory(Path(value), label=name.replace("_", "-"))
        for name, value in raw.items()
    }
    if len(set(roots.values())) != len(roots):
        raise NoDispatchIsolationError("candidate roots must be distinct")

    parents = {path.parent for path in roots.values()}
    if len(parents) != 1:
        raise NoDispatchIsolationError("candidate roots must share one direct run parent")
    run_root = _canonical_existing_directory(parents.pop(), label="run-root")
    if run_root.name != run_id:
        raise NoDispatchIsolationError("run-root basename must exactly match --run-id")
    if any(path.parent != run_root for path in roots.values()):
        raise NoDispatchIsolationError("candidate roots must be direct children of run-root")

    allowed = False
    for candidate in NO_DISPATCH_ALLOWED_BASES:
        try:
            base = _canonical_existing_directory(candidate, label="allowed candidate base")
        except NoDispatchIsolationError:
            continue
        if run_root != base and _path_is_within(run_root, base):
            allowed = True
            break
    if not allowed:
        raise NoDispatchIsolationError(
            "run-root must be below ~/hermes-candidate-sandboxes or "
            "/mnt/tmp/hermes-v0182-smoke-20260710"
        )
    return {"run_root": run_root, **roots}


def _open_isolated_directory(
    path: Path | str, *, label: str, dir_fd: int | None = None
) -> int:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_DIRECTORY", 0)
    )
    try:
        if dir_fd is not None:
            name = str(path)
            if not name or Path(name).name != name:
                raise NoDispatchIsolationError(f"{label} child name is not canonical")
            fd = os.open(name, flags, dir_fd=dir_fd)
        else:
            absolute = Path(path)
            if not absolute.is_absolute() or Path(os.path.normpath(str(absolute))) != absolute:
                raise NoDispatchIsolationError(f"{label} must be an absolute canonical path")
            fd = os.open(absolute.anchor, flags)
            try:
                for part in absolute.parts[1:]:
                    next_fd = os.open(part, flags, dir_fd=fd)
                    os.close(fd)
                    fd = next_fd
            except Exception:
                os.close(fd)
                raise
    except OSError as exc:
        raise NoDispatchIsolationError(f"cannot safely open {label}: {exc}") from exc
    info = os.fstat(fd)
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.getuid()
        or info.st_mode & 0o022
    ):
        os.close(fd)
        raise NoDispatchIsolationError(f"{label} changed or became unsafe")
    return fd


def _directory_identity(fd: int) -> tuple[int, int, int, int, int]:
    info = os.fstat(fd)
    return (info.st_dev, info.st_ino, info.st_mode, info.st_mtime_ns, info.st_ctime_ns)


def _assert_path_matches_fd(path: Path, fd: int, *, label: str) -> None:
    try:
        path_info = path.lstat()
    except OSError as exc:
        raise NoDispatchIsolationError(f"{label} disappeared during recording") from exc
    fd_info = os.fstat(fd)
    if stat.S_ISLNK(path_info.st_mode) or (
        path_info.st_dev,
        path_info.st_ino,
    ) != (fd_info.st_dev, fd_info.st_ino):
        raise NoDispatchIsolationError(f"{label} was replaced during recording")


def _write_exclusive_json(dir_fd: int, name: str, body: dict) -> tuple[bytes, str]:
    if not name or Path(name).name != name:
        raise NoDispatchIsolationError("record filename is not canonical")
    data = (json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode(
        "utf-8"
    )
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        fd = os.open(name, flags, 0o600, dir_fd=dir_fd)
    except OSError as exc:
        raise NoDispatchIsolationError(f"cannot create isolated record {name}: {exc}") from exc
    try:
        offset = 0
        while offset < len(data):
            offset += os.write(fd, data[offset:])
        os.fsync(fd)
    finally:
        os.close(fd)
    os.fsync(dir_fd)
    return data, hashlib.sha256(data).hexdigest()


def _validate_no_dispatch_identity(issue_url: str, work_item: str) -> str:
    parsed = urlsplit(str(issue_url or ""))
    parts = parsed.path.split("/")
    if (
        parsed.scheme != "https"
        or parsed.netloc != "project.feishu.cn"
        or parsed.query
        or parsed.fragment
        or "%" in parsed.path
        or len(parts) != 5
        or parts[0]
        or parts[2:4] != ["issue", "detail"]
        or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", parts[1])
        or not re.fullmatch(r"[0-9]{1,32}", parts[4])
        or parts[4] != str(work_item or "")
    ):
        raise NoDispatchIsolationError("issue-url and work-item identity are invalid or mismatched")
    canonical = f"https://project.feishu.cn/{parts[1]}/issue/detail/{parts[4]}"
    if canonical != issue_url:
        raise NoDispatchIsolationError("issue-url must use canonical project.feishu.cn spelling")
    return parts[1]


def record_no_dispatch(
    *,
    issue_url: str,
    work_item: str,
    requester: str,
    run_id: str,
    case_root: Path,
    artifact_root: Path,
    shared_state_root: Path,
    output_root: Path,
    work_root: Path,
    download_root: Path,
) -> dict:
    """Record a candidate-only execution intent without importing dispatch code."""
    project_key = _validate_no_dispatch_identity(issue_url, work_item)
    roots = validate_no_dispatch_roots(
        run_id=run_id,
        case_root=case_root,
        artifact_root=artifact_root,
        shared_state_root=shared_state_root,
        output_root=output_root,
        work_root=work_root,
        download_root=download_root,
    )
    run_fd = _open_isolated_directory(roots["run_root"], label="run_root")
    fds = {"run_root": run_fd}
    try:
        for name, path in roots.items():
            if name != "run_root":
                fds[name] = _open_isolated_directory(
                    path.name, label=name, dir_fd=run_fd
                )
    except Exception:
        for fd in fds.values():
            os.close(fd)
        raise
    readonly_before = {
        name: _directory_identity(fds[name])
        for name in ("case_root", "work_root", "download_root")
    }
    task_slug = f"g1q3_rca_issue_intake_{work_item}_{run_id}"
    observed_at = datetime.now(timezone.utc).isoformat()
    root_strings = {name: str(path) for name, path in roots.items()}
    requester_hash = hashlib.sha256(str(requester or "").encode("utf-8")).hexdigest()
    policy = {
        "dispatch_allowed": False,
        "download_allowed": False,
        "feishu_read_allowed": False,
        "feishu_write_allowed": False,
        "network_allowed": False,
        "vm_access_allowed": False,
        "production_shared_state_write_allowed": False,
    }
    request = {
        "schema_version": "g1q3_rca_no_dispatch_request_v1",
        "mode": "candidate_no_dispatch",
        "observed_at": observed_at,
        "identity": {
            "run_id": run_id,
            "task_slug": task_slug,
            "project_key": project_key,
            "work_item_id": work_item,
            "issue_url": issue_url,
            "source_group_id": G1Q3_RCA_GROUP_ID,
            "requester_sha256": requester_hash,
        },
        "roots": root_strings,
        "execution_policy": policy,
        "decision": {
            "route_evaluation_performed": False,
            "dispatch_attempted": False,
            "download_attempted": False,
            "external_delivery_attempted": False,
        },
    }
    state = {
        "schema_version": "g1q3_rca_no_dispatch_state_v1",
        "task_id": task_slug,
        "run_id": run_id,
        "state": "recorded_not_dispatched",
        "candidate_only": True,
        "production_state": False,
        "terminal": True,
        "observed_at": observed_at,
        "execution_policy": policy,
    }
    request_name = f"rca_execution_request.{run_id}.json"
    state_name = f"no_dispatch_state.{run_id}.json"
    audit_name = f"no_dispatch_audit.{run_id}.json"
    written: dict[str, dict[str, str]] = {}
    try:
        request_bytes, request_sha = _write_exclusive_json(
            fds["artifact_root"], request_name, request
        )
        written["execution_request"] = {
            "path": str(roots["artifact_root"] / request_name),
            "sha256": request_sha,
        }
        state_bytes, state_sha = _write_exclusive_json(
            fds["shared_state_root"], state_name, state
        )
        written["isolated_shared_state"] = {
            "path": str(roots["shared_state_root"] / state_name),
            "sha256": state_sha,
        }
        readonly_after = {
            name: _directory_identity(fds[name])
            for name in ("case_root", "work_root", "download_root")
        }
        untouched = {
            name: readonly_before[name] == readonly_after[name]
            for name in readonly_before
        }
        if not all(untouched.values()):
            raise NoDispatchIsolationError("read-only candidate roots changed during recording")
        audit = {
            "schema_version": "g1q3_rca_no_dispatch_audit_v1",
            "mode": "candidate_no_dispatch",
            "observed_at": observed_at,
            "identity": request["identity"],
            "roots": root_strings,
            "execution_policy": policy,
            "enforcement": {
                "scope": "g1q3_rca_e2e_smoke script control flow",
                "preflight_called": False,
                "dotenv_loaded": False,
                "gateway_run_imported": False,
                "dispatch_attempted": False,
                "download_attempted": False,
                "feishu_contact_attempted": False,
                "vm_or_ssh_attempted": False,
                "network_attempted": False,
                "production_shared_state_write_attempted": False,
                "read_only_roots_unchanged": untouched,
            },
            "records": written,
            "authorization": {
                "candidate_execution_authorized": False,
                "production_write_authorized": False,
                "dispatch_authorized": False,
                "download_authorized": False,
                "external_delivery_authorized": False,
                "cutover_authorized": False,
                "gate_decision": "NO_GO",
            },
        }
        audit_bytes, audit_sha = _write_exclusive_json(
            fds["output_root"], audit_name, audit
        )
        written["audit"] = {
            "path": str(roots["output_root"] / audit_name),
            "sha256": audit_sha,
        }
        for name, fd in fds.items():
            _assert_path_matches_fd(roots[name], fd, label=name)
        return {
            "mode": "no-dispatch",
            "ok": False,
            "record_only_completed": True,
            "offline_check_passed": True,
            "gate_decision": "NO_GO",
            "task_slug": task_slug,
            "roots": root_strings,
            "records": written,
            "dispatch_attempted": False,
            "download_attempted": False,
            "feishu_contact_attempted": False,
            "vm_or_ssh_attempted": False,
            "network_attempted": False,
            "production_shared_state_write_attempted": False,
            "execution_authorized": False,
            "cutover_authorized": False,
            "audit_payload_bytes": len(audit_bytes),
            "request_payload_bytes": len(request_bytes),
            "state_payload_bytes": len(state_bytes),
        }
    finally:
        for fd in fds.values():
            os.close(fd)


# --------------------------------------------------------------------------
# preflight gate (exit 3 on failure — smoke not attempted)
# --------------------------------------------------------------------------
def preflight(hermes_home: Path) -> tuple[bool, list[str]]:
    findings: list[str] = []
    ok = True

    # 1) governance download flag (set in-process to match gateway plist).
    os.environ["G1Q3_GOVERNANCE_DOWNLOAD_ENABLED"] = "1"
    try:
        sys.path.insert(0, str(REPO)); sys.path.insert(0, str(REPO / "scripts"))
        from pnc_g1q3_governance_rca import governance_download_enabled
        if governance_download_enabled():
            findings.append("flag: G1Q3_GOVERNANCE_DOWNLOAD_ENABLED=1 OK")
        else:
            ok = False; findings.append("flag: governance_download_enabled False -> BLOCK")
    except Exception as exc:
        ok = False; findings.append(f"flag: import failed: {type(exc).__name__}: {exc}")

    # 2) relay not crash-looping (poison-card fail-closed loop kills card sync).
    relay_ok, relay_note = _relay_not_crashlooping()
    findings.append(f"relay: {relay_note}")
    ok = ok and relay_ok

    # 3) VM reachable.
    try:
        proc = subprocess.run([SSH_MINI_AGENT, "doctor", "--json"],
                              text=True, capture_output=True, timeout=30)
        doc = json.loads(proc.stdout) if proc.stdout.strip() else {}
        if doc.get("ok") and doc.get("remote_rc") == 0:
            findings.append("vm: ssh-mini doctor ok")
        else:
            ok = False; findings.append(f"vm: doctor not ok: {str(doc)[:120]}")
    except Exception as exc:
        ok = False; findings.append(f"vm: doctor failed: {type(exc).__name__}: {exc}")

    # 4) daily download quota has headroom.
    q_ok, q_note = _quota_has_headroom(hermes_home)
    findings.append(f"quota: {q_note}")
    ok = ok and q_ok

    return ok, findings


def _relay_not_crashlooping() -> tuple[bool, str]:
    label = "local.pnc.completion-notice-relay"
    def _query() -> tuple[str, str]:
        proc = subprocess.run(["launchctl", "list"], text=True, capture_output=True, timeout=15)
        for line in proc.stdout.splitlines():
            if label in line:
                parts = line.split()
                return parts[0], parts[1]  # pid, status
        return "", ""
    pid1, status1 = _query()
    if not pid1:
        return True, f"{label} not loaded (skip — no relay to crash)"
    # A crash-loop shows status=1 AND a churning pid across a short window.
    time.sleep(3)
    pid2, status2 = _query()
    if status1 == "1" and pid1 != pid2:
        return False, f"CRASH-LOOP: status=1 pid churned {pid1}->{pid2} -> BLOCK (see §S5.5 poison-card)"
    return True, f"stable pid={pid2} status={status2}"


def _quota_has_headroom(hermes_home: Path) -> tuple[bool, str]:
    day = datetime.now(timezone.utc).date().isoformat()
    ledger = hermes_home / "pnc_agent" / "quota" / f"g1q3_auto_download-{day}.json"
    quota_env = os.environ.get("HERMES_G1Q3_AUTO_DOWNLOAD_DAILY_QUOTA", "").strip()
    try:
        quota = int(quota_env) if quota_env else 0
    except ValueError:
        quota = 0
    if quota <= 0:
        return False, "HERMES_G1Q3_AUTO_DOWNLOAD_DAILY_QUOTA<=0 -> auto_download_disabled -> BLOCK"
    used = 0
    if ledger.exists():
        try:
            used = int(json.loads(ledger.read_text()).get("used") or 0)
        except Exception:
            used = 0
    if used >= quota:
        return False, f"exhausted used={used}/{quota} -> BLOCK"
    return True, f"headroom used={used}/{quota}"


# --------------------------------------------------------------------------
# trigger the REAL detached datapipe coordinator
# --------------------------------------------------------------------------
def trigger(issue_url: str, work_item: str, requester: str) -> tuple[str, str, dict]:
    """Returns (task_slug, artifact_root, dispatch_result)."""
    sys.path.insert(0, str(REPO)); sys.path.insert(0, str(REPO / "scripts"))
    from gateway.config import Platform
    from gateway.pnc_group_binding import evaluate_pnc_group_request
    import gateway.run as gr
    from gateway.pnc_rca_artifacts import write_vm_tmp_text

    text = f"分析这个问题 {issue_url}"
    dec = evaluate_pnc_group_request(platform=Platform.FEISHU, chat_id=G1Q3_RCA_GROUP_ID, text=text)
    if dec.decision != "accepted":
        raise RuntimeError(f"decision != accepted: {dec.decision}")
    handoff = dec.handoff_contract or {}
    work_item_id = str(handoff.get("work_item_id") or work_item)

    # Fresh message_id -> fresh /mnt/tmp/..._<hash>/ namespace (real download, no reuse).
    uniq = hashlib.sha1(os.urandom(16)).hexdigest()[:10]
    message_id = f"om_smoke_{uniq}"
    trig = hashlib.sha1(message_id.encode()).hexdigest()[:6]
    safe_case = re.sub(r"[^A-Za-z0-9_-]+", "_", handoff.get("case_id") or work_item_id).strip("_") or "unknown"
    task_slug = f"g1q3_rca_issue_intake_{safe_case}_{trig}"
    artifact_root = f"/mnt/tmp/{task_slug}/"
    artifact_cifs = ("//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/"
                     f"{task_slug}/")
    full_case_id = f"G1Q3-{handoff.get('case_id')}" if handoff.get("case_id") else "待从飞书问题字段解析"
    translate_config = gr._g1q3_translate_baseline_config()

    from gateway.pnc_rca_schema import build_execution_request, to_json as rca_to_json
    from gateway.pnc_issue_context import issue_context_from_compact_text
    issue_ctx = issue_context_from_compact_text(
        project_key="t03o4q", work_item_id=work_item_id, compact_text="", issue_url=issue_url,
    )
    exec_req = build_execution_request(
        request_kind="issue_intake", task_id=task_slug, issue_context=issue_ctx,
        request_text_excerpt=text[:1200], source_group_id=G1Q3_RCA_GROUP_ID,
        source_message_id=message_id, artifact_root=artifact_root,
        artifact_cifs_root=artifact_cifs, allow_download=True,
        translate_baseline=translate_config.get("translate_baseline", "production"),
        translate_contract_path=translate_config.get("translate_contract_path", ""),
    )
    request_json = rca_to_json(exec_req)
    execution_request_path = f"{artifact_root}rca_execution_request.json"
    write_vm_tmp_text(execution_request_path, request_json + "\n")

    # CRITICAL: dispatch the REAL pipeline entrypoint (start_new_session detached).
    # _submit_g1q3_rca_status_handoff is a card/status handoff that does NOT start
    # the datapipe — calling it strands the run at s1_gate (2026-07-10 lesson).
    result = gr._dispatch_governance_datapipe_coordinator(
        task_slug=task_slug, artifact_root=artifact_root, artifact_cifs_root=artifact_cifs,
        execution_request_path=execution_request_path, request_json=request_json,
        template_id="rca_issue_intake", full_case_id=full_case_id, work_item_id=work_item_id,
        source_group_id=G1Q3_RCA_GROUP_ID, message_id=message_id, requester=requester,
        translate_config=translate_config,
    )
    return task_slug, artifact_root, result


# --------------------------------------------------------------------------
# watch the pipeline to completion
# --------------------------------------------------------------------------
def watch(artifact_root: str, timeout: float, poll: float = 20.0) -> tuple[bool, dict]:
    root = artifact_root.rstrip("/")
    deadline = time.monotonic() + timeout
    last_stages: dict = {}
    while time.monotonic() < deadline:
        ps = _vm_read_json(f"{root}/pipeline_state.json")
        if ps:
            stages = ps.get("stages")
            norm = ({k: (v.get("status") if isinstance(v, dict) else v) for k, v in stages.items()}
                    if isinstance(stages, dict) else {})
            if norm != last_stages:
                _log(f"  stages: {json.dumps(norm, ensure_ascii=False)}")
                last_stages = norm
            s6 = (stages or {}).get("s6_report") if isinstance(stages, dict) else None
            s6_status = s6.get("status") if isinstance(s6, dict) else s6
            # completion: exit.code file == 0 AND s6 completed
            rc, out, _ = _run_vm_py(
                f"import os,json;p={json.dumps(root)}+'/exit.code';"
                f"print(json.dumps({{'exit':open(p).read().strip() if os.path.exists(p) else None}}))")
            exit_code = None
            try:
                exit_code = json.loads(out).get("exit")
            except Exception:
                pass
            if s6_status == "completed" and exit_code == "0":
                return True, {"stages": last_stages, "exit_code": exit_code}
            if exit_code not in (None, "0"):
                return False, {"stages": last_stages, "exit_code": exit_code, "reason": "nonzero_exit"}
        time.sleep(poll)
    return False, {"stages": last_stages, "reason": "timeout"}


# --------------------------------------------------------------------------
# §S5.5 five green checks against the real case_dir
# --------------------------------------------------------------------------
def find_case_dir(work_item: str) -> str | None:
    rc, out, _ = _run_vm_py(
        "import os,json,glob;"
        f"c=sorted(glob.glob({json.dumps(CASE_ROOT + '/' + work_item + '*')}));"
        "print(json.dumps({'dirs':c}))")
    try:
        dirs = json.loads(out).get("dirs") or []
    except Exception:
        dirs = []
    # prefer the most specific (e.g. <id>_acc) with a report_data.json
    for d in sorted(dirs, key=len, reverse=True):
        rd = _vm_read_json(f"{d}/report_data.json", lines=1)
        if rd is not None or _vm_file_exists(f"{d}/report_data.json"):
            return d
    return dirs[-1] if dirs else None


def _vm_file_exists(path: str) -> bool:
    rc, out, _ = _run_vm_py(f"import os,json;print(json.dumps({{'e':os.path.exists({json.dumps(path)})}}))")
    try:
        return bool(json.loads(out).get("e"))
    except Exception:
        return False


def green_checks(case_dir: str, artifact_root: str) -> dict:
    checks: dict = {}
    gate = _vm_read_json(f"{case_dir}/gate_result.json")
    report = _vm_read_json(f"{case_dir}/report_data.json", lines=4000)

    # 1) G0-G6 all pass
    gates = (gate or {}).get("gates", gate)
    if isinstance(gates, list):
        statuses = {g.get("gate"): g.get("status") for g in gates if isinstance(g, dict)}
    elif isinstance(gates, dict):
        statuses = {k: (v.get("status") if isinstance(v, dict) else v) for k, v in gates.items()}
    else:
        statuses = {}
    g_pass = bool(statuses) and all(str(v).lower() == "pass" for v in statuses.values())
    checks["1_gates_all_pass"] = {"ok": g_pass, "detail": statuses}

    summary = (report or {}).get("summary") or {}
    status = summary.get("status")
    # 2) status honest
    status_ok = status in {"hypothesis_ready", "report_ready"}
    banner = str(summary.get("ui_banner_title") or "") + str(summary.get("high_confidence_boundary") or "")
    # honesty: hypothesis_ready must NOT be washed to a completed/定责 banner
    honest = True
    if status == "hypothesis_ready" and re.search(r"已完成|completed|已定责", banner):
        honest = False
    checks["2_status_honest"] = {"ok": bool(status_ok and honest), "status": status,
                                 "boundary": summary.get("high_confidence_boundary")}

    # 3) field lineage: no dropped decoded fields
    fl = (report or {}).get("field_lineage") or {}
    dropped = fl.get("manifest_decoded_dropped")
    checks["3_manifest_decoded_dropped_empty"] = {"ok": dropped == [], "dropped": dropped,
                                                  "fidelity_ok": fl.get("fidelity_ok")}

    # 4) artifacts non-empty + exit0
    idx = _vm_stat_size(f"{case_dir}/index.html")
    rd = _vm_stat_size(f"{case_dir}/report_data.json")
    exit_code = _vm_read_text(f"{artifact_root.rstrip('/')}/exit.code")
    checks["4_artifacts_exit0"] = {"ok": bool(idx and idx > 0 and rd and rd > 0 and exit_code == "0"),
                                   "index_html_bytes": idx, "report_data_bytes": rd, "exit_code": exit_code}

    # 5) card delivery — headless boundary, NOT covered (not pass/fail)
    checks["5_card_delivery"] = {"ok": None, "note":
        "NOT-COVERED (headless boundary): direct coordinator dispatch skips gateway "
        "early-card/relay delivery. Requires A3 real @-mention to verify card terminal."}
    return checks


def _vm_stat_size(path: str) -> int | None:
    rc, out, _ = _run_vm_py(
        f"import os,json;p={json.dumps(path)};"
        "print(json.dumps({'s':os.path.getsize(p) if os.path.exists(p) else None}))")
    try:
        return json.loads(out).get("s")
    except Exception:
        return None


def _vm_read_text(path: str) -> str | None:
    rc, out, _ = _run_vm_py(
        f"import os,json;p={json.dumps(path)};"
        "print(json.dumps({'t':open(p).read().strip() if os.path.exists(p) else None}))")
    try:
        return json.loads(out).get("t")
    except Exception:
        return None


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------
def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="g1q3_rca_e2e_smoke",
        description="G1Q3-RCA end-to-end delivery smoke gate (runbook §S5.5 headless back-half).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--hermes-home", default=os.environ.get("HERMES_HOME", str(Path.home() / ".hermes")))
    p.add_argument("--issue-url", default=DEFAULT_ISSUE_URL)
    p.add_argument("--work-item", default=DEFAULT_WORK_ITEM)
    p.add_argument("--requester", default=DEFAULT_REQUESTER)
    p.add_argument("--timeout", type=float, default=1200.0, help="max seconds to watch the pipeline")
    p.add_argument("--json", action="store_true")
    modes = p.add_mutually_exclusive_group()
    modes.add_argument("--dry-run", action="store_true",
                       help="legacy preflight + decision build; queries VM but does not dispatch")
    modes.add_argument(
        "--no-dispatch",
        action="store_true",
        help="candidate-only local record; no preflight, VM, network, download, Feishu, or dispatch",
    )
    p.add_argument("--run-id", help="required candidate isolation namespace for --no-dispatch")
    p.add_argument("--case-root", type=Path)
    p.add_argument("--artifact-root", type=Path)
    p.add_argument("--shared-state-root", type=Path)
    p.add_argument("--output-root", type=Path)
    p.add_argument("--work-root", type=Path)
    p.add_argument("--download-root", type=Path)
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    isolation_values = {name: getattr(args, name) for name in NO_DISPATCH_ROOT_FIELDS}
    if args.no_dispatch:
        missing = [name for name, value in isolation_values.items() if value is None]
        if not args.run_id:
            missing.append("run_id")
        if missing:
            result = {
                "mode": "no-dispatch",
                "ok": False,
                "record_only_completed": False,
                "dispatch_attempted": False,
                "download_attempted": False,
                "feishu_contact_attempted": False,
                "vm_or_ssh_attempted": False,
                "network_attempted": False,
                "production_shared_state_write_attempted": False,
                "gate_decision": "NO_GO",
                "error": {
                    "code": "G1Q3_NO_DISPATCH_ROOTS_REQUIRED",
                    "missing": sorted(missing),
                },
            }
            if args.json:
                print(json.dumps(result, ensure_ascii=False, indent=2))
            else:
                _log(f"NO-DISPATCH BLOCKED: missing {', '.join(sorted(missing))}")
            return 3
        try:
            result = record_no_dispatch(
                issue_url=args.issue_url,
                work_item=args.work_item,
                requester=args.requester,
                run_id=args.run_id,
                **isolation_values,
            )
        except Exception as exc:
            result = {
                "mode": "no-dispatch",
                "ok": False,
                "record_only_completed": False,
                "dispatch_attempted": False,
                "download_attempted": False,
                "feishu_contact_attempted": False,
                "vm_or_ssh_attempted": False,
                "network_attempted": False,
                "production_shared_state_write_attempted": False,
                "gate_decision": "NO_GO",
                "error": {
                    "code": "G1Q3_NO_DISPATCH_ISOLATION_FAILED",
                    "reason": str(exc),
                    "exception_type": type(exc).__name__,
                },
            }
            if args.json:
                print(json.dumps(result, ensure_ascii=False, indent=2))
            else:
                _log(f"NO-DISPATCH BLOCKED: {type(exc).__name__}: {exc}")
            return 3
        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            _log(f"NO-DISPATCH RECORDED: {result['records']['audit']['path']}")
            _log("NO-GO: dispatch/download/Feishu/VM/production shared-state were not attempted")
        return 2

    unexpected_isolation = [name for name, value in isolation_values.items() if value is not None]
    if args.run_id is not None:
        unexpected_isolation.append("run_id")
    if unexpected_isolation:
        result = {
            "mode": "invalid",
            "ok": False,
            "gate_decision": "NO_GO",
            "dispatch_attempted": False,
            "error": {
                "code": "G1Q3_ISOLATION_REQUIRES_NO_DISPATCH",
                "arguments": sorted(unexpected_isolation),
            },
        }
        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            _log("ISOLATION ARGUMENTS REQUIRE --no-dispatch")
        return 3

    hermes_home = Path(args.hermes_home)
    os.environ.setdefault("HERMES_HOME", str(hermes_home))
    # Load .env so Feishu/quota config matches the live gateway.
    try:
        sys.path.insert(0, str(REPO))
        from hermes_cli.env_loader import load_hermes_dotenv
        load_hermes_dotenv(project_env=REPO / ".env")
    except Exception:
        pass

    result: dict = {"mode": "dry-run" if args.dry_run else "full", "ok": False}

    _log("== preflight ==")
    pf_ok, pf = preflight(hermes_home)
    for f in pf:
        _log(f"  {f}")
    result["preflight"] = {"ok": pf_ok, "findings": pf}
    if not pf_ok:
        _log("PREFLIGHT FAILED -> exit 3 (smoke not attempted)")
        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        return 3

    if args.dry_run:
        # Validate decision + execution_request build without dispatching.
        try:
            sys.path.insert(0, str(REPO)); sys.path.insert(0, str(REPO / "scripts"))
            from gateway.config import Platform
            from gateway.pnc_group_binding import evaluate_pnc_group_request
            dec = evaluate_pnc_group_request(platform=Platform.FEISHU, chat_id=G1Q3_RCA_GROUP_ID,
                                             text=f"分析这个问题 {args.issue_url}")
            ok = dec.decision == "accepted"
            result["decision"] = {"ok": ok, "decision": dec.decision,
                                  "work_item": (dec.handoff_contract or {}).get("work_item_id")}
            result["ok"] = ok
            _log(f"== dry-run decision: {dec.decision} ==")
        except Exception as exc:
            result["decision"] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if result["ok"] else 2

    _log("== trigger detached datapipe coordinator ==")
    try:
        task_slug, artifact_root, dispatch = trigger(args.issue_url, args.work_item, args.requester)
    except Exception as exc:
        _log(f"TRIGGER FAILED: {type(exc).__name__}: {exc}")
        result["trigger"] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        return 2
    result["trigger"] = {"ok": bool(dispatch.get("dispatched")), "task_slug": task_slug,
                         "artifact_root": artifact_root, "followup_task_id": dispatch.get("followup_task_id")}
    _log(f"  dispatched={dispatch.get('dispatched')} slug={task_slug}")
    if not dispatch.get("dispatched"):
        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        return 2

    _log(f"== watch pipeline (timeout={args.timeout:.0f}s) ==")
    done, watch_info = watch(artifact_root, args.timeout)
    result["watch"] = watch_info
    if not done:
        _log(f"PIPELINE DID NOT COMPLETE: {watch_info.get('reason')}")
        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        return 2

    _log("== §S5.5 green checks ==")
    case_dir = find_case_dir(args.work_item)
    result["case_dir"] = case_dir
    if not case_dir:
        _log("NO CASE_DIR FOUND -> FAIL")
        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        return 2
    checks = green_checks(case_dir, artifact_root)
    result["green_checks"] = checks
    for k, v in checks.items():
        mark = "SKIP" if v.get("ok") is None else ("PASS" if v.get("ok") else "FAIL")
        _log(f"  [{mark}] {k}: {json.dumps({kk: vv for kk, vv in v.items() if kk != 'ok'}, ensure_ascii=False)[:160]}")

    # PASS = all pass/fail checks (1-4) pass; check 5 is an honest NOT-COVERED note.
    scored = [v.get("ok") for v in checks.values() if v.get("ok") is not None]
    ok = bool(scored) and all(scored)
    result["ok"] = ok
    _log(f"\n== {'PASS' if ok else 'FAIL'} == (card-delivery NOT covered: headless boundary, needs A3)")
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
