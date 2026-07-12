#!/usr/bin/env python3
"""Isolated L4 zero-impact acceptance harness.

The driver is intentionally fail closed. It launches production VM-sync,
relay, and task-card entry functions only in fresh per-phase homes under one
run root, through ``env -i`` and a hashed default-deny macOS sandbox profile.
Every receipt starts NO_GO and can become PASS only when all required gates
carry executed, machine-verifiable PASS evidence.
"""
from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import io
import json
import os
import pwd
import re
import shutil
import signal
import stat
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable


REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

SCHEMA_VERSION = "hermes_l4_zero_impact_harness_receipt_v1"
WORKER_SCHEMA_VERSION = "hermes_l4_zero_impact_worker_result_v1"
FIXTURE_SCHEMA_VERSION = "pnc_vm_task_sync_fixture_v1"
SCENARIO_IDS = tuple(f"S{index}" for index in range(1, 11))
CRASH_BOUNDARIES = (
    "before_sender",
    "after_record_before_mark",
    "before_mark_persist",
    "after_mark_before_ack",
)
REQUIRED_GATES = (
    "source_binding",
    "focused_tests",
    "s1_s10_combined",
    "business_crash_matrix",
    "concurrency_restart",
    "os_sandbox",
    "python_audit",
    "whole_process_containment",
    "ledger_integrity",
    "process_cleanup",
    "artifact_manifest",
)
SANDBOX_EXEC = Path("/usr/bin/sandbox-exec")
ENV_EXEC = Path("/usr/bin/env")
AUDIT_BOOTSTRAP = REPO_ROOT / "scripts" / "l4_audit_bootstrap"
SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
GIT_ID_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SECRET_ENV_RE = re.compile(
    r"(?:TOKEN|SECRET|PASSWORD|PASSWD|COOKIE|CREDENTIAL|AUTHORIZATION|API_KEY|PRIVATE_KEY)",
    re.I,
)
TEST_KEY_HEX = hashlib.sha256(b"hermes-l4-non-secret-record-key-v1").hexdigest()


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    payload = _canonical_json(value) + b"\n"
    temp = path.parent / f".{path.name}.tmp.{os.getpid()}"
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    fd = os.open(temp, flags, 0o600)
    try:
        offset = 0
        while offset < len(payload):
            offset += os.write(fd, payload[offset:])
        os.fsync(fd)
        info = os.fstat(fd)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or info.st_nlink != 1
            or stat.S_IMODE(info.st_mode) != 0o600
        ):
            raise RuntimeError(f"unsafe temporary receipt file: {temp}")
        os.replace(temp, path)
        os.chmod(path, 0o600)
        directory_fd = os.open(
            path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        )
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        os.close(fd)
        try:
            temp.unlink()
        except FileNotFoundError:
            pass


def _paths_overlap(left: Path, right: Path) -> bool:
    for candidate, ancestor in ((left, right), (right, left)):
        try:
            candidate.relative_to(ancestor)
        except ValueError:
            continue
        return True
    return False


def _account_home() -> Path:
    try:
        return Path(pwd.getpwuid(os.getuid()).pw_dir).resolve()
    except (KeyError, OSError):
        return Path.home().resolve()


def _protected_roots() -> tuple[Path, ...]:
    home = _account_home()
    return tuple(
        (home / relative).resolve(strict=False)
        for relative in (
            ".hermes",
            ".openclaw",
            ".codex",
            ".claude",
            ".ssh",
            ".aws",
            ".config/gcloud",
            "Library/Keychains",
        )
    )


def _verified_directory(path: Path) -> Path:
    raw = Path(path).expanduser()
    if not raw.is_absolute():
        raise ValueError(f"path must be absolute: {raw}")
    resolved = raw.resolve(strict=True)
    if raw.absolute() != resolved:
        raise ValueError(f"path must be canonical: {raw}")
    info = resolved.stat()
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.getuid()
        or info.st_mode & 0o022
    ):
        raise ValueError(f"unsafe directory owner/type/mode: {resolved}")
    return resolved


def _prepare_run_root(path: Path, *, source_root: Path) -> Path:
    raw = Path(path).expanduser()
    if not raw.is_absolute():
        raise ValueError("--run-root must be absolute")
    if raw.exists():
        resolved = _verified_directory(raw)
        if any(resolved.iterdir()):
            raise ValueError("--run-root must be a new empty directory")
    else:
        parent = _verified_directory(raw.parent)
        parent_fd = os.open(
            parent,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            os.mkdir(raw.name, 0o700, dir_fd=parent_fd)
        finally:
            os.close(parent_fd)
        resolved = _verified_directory(raw)
    if _paths_overlap(resolved, source_root):
        raise ValueError("run root must not overlap the source tree")
    for blocked in _protected_roots():
        if _paths_overlap(resolved, blocked):
            raise ValueError(f"run root overlaps protected root: {blocked}")
    return resolved


def _safe_leaf(value: str, *, field: str) -> str:
    if not SAFE_NAME_RE.fullmatch(str(value or "")):
        raise ValueError(f"invalid {field}: {value!r}")
    return str(value)


def _copy_census(source: Path, destination: Path) -> dict[str, Any]:
    source = _verified_directory(source)
    destination.mkdir(mode=0o700)
    expected = {
        "INDEX.json": "b6bcfb3a597da616bec2acc8e57eea18695b0bb20e29446926cf2eb2e3f81914",
        "census-v4.json": "d2c17c7b03642074d301259437f17cc879e8adfbd91d07029c2dda775a563e63",
    }
    rows = []
    for name, expected_sha in expected.items():
        src = source / name
        if _sha256_file(src) != expected_sha:
            raise ValueError(f"frozen census hash mismatch: {src}")
        dst = destination / name
        shutil.copyfile(src, dst)
        dst.chmod(0o600)
        if _sha256_file(dst) != expected_sha:
            raise ValueError(f"copied census hash mismatch: {dst}")
        rows.append({"path": str(dst), "sha256": expected_sha})
    return {"source": str(source), "files": rows}


def _sandbox_quote(value: Path) -> str:
    return json.dumps(str(value), ensure_ascii=True)


def _python_exec_allowlist(python_executable: Path) -> tuple[Path, ...]:
    """Return only the concrete executables used by this Python launcher.

    Homebrew's macOS launcher re-execs the framework Python.app binary after
    sandbox-exec starts it. Both paths must therefore be named explicitly;
    allowing process execution by directory or glob would weaken containment.
    """
    candidates = {python_executable.resolve(strict=True)}
    framework_launcher = (
        Path(sys.base_prefix).resolve(strict=True)
        / "Resources"
        / "Python.app"
        / "Contents"
        / "MacOS"
        / "Python"
    )
    if framework_launcher.is_file():
        candidates.add(framework_launcher.resolve(strict=True))
    verified = []
    for candidate in sorted(candidates, key=str):
        info = candidate.stat()
        if not stat.S_ISREG(info.st_mode) or not os.access(candidate, os.X_OK):
            raise ValueError(f"unsafe Python executable allowlist entry: {candidate}")
        verified.append(candidate)
    return tuple(verified)


def _sandbox_profile(
    *, run_root: Path, source_root: Path, python_executables: tuple[Path, ...]
) -> str:
    lines = [
        "(version 1)",
        "(deny default)",
        "(allow file-read*)",
        f"(allow file-write* (subpath {_sandbox_quote(run_root)}) (literal \"/dev/null\"))",
        "(allow process-info*)",
        "(allow sysctl-read)",
        "(allow mach-lookup)",
        "(allow ipc-posix*)",
        "(deny network*)",
        f"(deny file-write* (subpath {_sandbox_quote(source_root)}))",
    ]
    for executable in python_executables:
        lines.append(f"(allow process-exec (literal {_sandbox_quote(executable)}))")
    for blocked in _protected_roots():
        lines.append(f"(deny file-read* file-write* (subpath {_sandbox_quote(blocked)}))")
    return "\n".join(lines) + "\n"


def _prepare_phase(run_root: Path, name: str) -> dict[str, Path]:
    name = _safe_leaf(name, field="phase name")
    phase = run_root / "phases" / name
    os_home = phase / "os-home"
    hermes_home = os_home / ".hermes"
    records = phase / "outbound-records"
    temp = phase / "tmp"
    for path in (phase, os_home, hermes_home, records, temp):
        path.mkdir(parents=True, mode=0o700, exist_ok=False if path == phase else True)
        path.chmod(0o700)
    key = phase / "record.key"
    key.write_text(TEST_KEY_HEX + "\n", encoding="ascii")
    key.chmod(0o600)
    return {
        "phase": phase,
        "os_home": os_home,
        "hermes_home": hermes_home,
        "records": records,
        "key": key,
        "tmp": temp,
    }


@dataclass
class WorkerProcess:
    name: str
    command: list[str]
    process: subprocess.Popen[bytes]
    stdout_path: Path
    stderr_path: Path
    audit_path: Path
    result_path: Path
    started_at: float
    timeout_seconds: int
    stdout_handle: Any
    stderr_handle: Any


def _worker_environment(
    *, run_root: Path, phase_paths: dict[str, Path], audit_path: Path
) -> dict[str, str]:
    python_path = os.pathsep.join((str(AUDIT_BOOTSTRAP), str(REPO_ROOT)))
    env = {
        "HOME": str(phase_paths["os_home"]),
        "HERMES_HOME": str(phase_paths["hermes_home"]),
        "HERMES_L4_ALLOWED_WRITE_ROOT": str(run_root),
        "HERMES_L4_AUDIT_LOG": str(audit_path),
        "HERMES_L4_EVENT_EPOCH": "1783850400",
        "HERMES_L4_ISOLATION_ROOT": str(run_root),
        "HERMES_L4_PHASE_ROOT": str(phase_paths["phase"]),
        "HERMES_L4_PROTECTED_ROOTS": os.pathsep.join(str(path) for path in _protected_roots()),
        "HERMES_L4_RUN_ROOT": str(run_root),
        "HERMES_L4_SANDBOX_ACTIVE": "1",
        "HERMES_OUTBOUND_CENSUS_ROOT": str(run_root / "outbound-census"),
        "HERMES_OUTBOUND_MODE": "record-only",
        "HERMES_OUTBOUND_RECORD_KEY_FILE": str(phase_paths["key"]),
        "HERMES_OUTBOUND_RECORD_ROOT": str(phase_paths["records"]),
        "HERMES_G1Q3_REPORT_COMMENT": "1",
        "HERMES_PNC_INFRA_ALERT": "1",
        "HERMES_PNC_INFRA_ALERT_OPS_NAME": "L4-OPS",
        "HERMES_PNC_INFRA_ALERT_OPS_OPEN_ID": "ou_l4_ops_fixture",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": "/usr/bin:/bin",
        "PNC_RELAY_SEND_CONCURRENCY": "1",
        "PNC_TASK_CARD_UPDATE_THROTTLE_SECONDS": "0",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONHASHSEED": "0",
        "PYTHONNOUSERSITE": "1",
        "PYTHONPATH": python_path,
        "PYTHONPYCACHEPREFIX": str(phase_paths["phase"] / "pycache"),
        "TMPDIR": str(phase_paths["tmp"]),
        "TZ": "UTC",
    }
    leaked = sorted(key for key in env if SECRET_ENV_RE.search(key))
    if leaked:
        raise ValueError(f"sanitized worker environment contains secret-like keys: {leaked}")
    return env


def _spawn_worker(
    *,
    run_root: Path,
    profile_path: Path,
    python_executable: Path,
    phase_paths: dict[str, Path],
    name: str,
    worker_args: list[str],
    timeout_seconds: int,
) -> WorkerProcess:
    name = _safe_leaf(name, field="worker name")
    audit_path = run_root / "audit" / f"{name}.jsonl"
    result_path = run_root / "results" / f"{name}.json"
    stdout_path = run_root / "logs" / f"{name}.stdout.log"
    stderr_path = run_root / "logs" / f"{name}.stderr.log"
    for parent in (audit_path.parent, result_path.parent, stdout_path.parent):
        parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    audit_path.write_bytes(b"")
    audit_path.chmod(0o600)
    env = _worker_environment(
        run_root=run_root, phase_paths=phase_paths, audit_path=audit_path
    )
    command = [
        str(ENV_EXEC),
        "-i",
        *(f"{key}={value}" for key, value in sorted(env.items())),
        str(SANDBOX_EXEC),
        "-f",
        str(profile_path),
        str(python_executable),
        str(Path(__file__).resolve()),
        "--worker",
        *worker_args,
        "--worker-result",
        str(result_path),
    ]
    stdout_handle = stdout_path.open("wb")
    stderr_handle = stderr_path.open("wb")
    process = subprocess.Popen(
        command,
        cwd=run_root,
        env={},
        stdin=subprocess.DEVNULL,
        stdout=stdout_handle,
        stderr=stderr_handle,
        start_new_session=True,
    )
    return WorkerProcess(
        name=name,
        command=command,
        process=process,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        audit_path=audit_path,
        result_path=result_path,
        started_at=time.monotonic(),
        timeout_seconds=timeout_seconds,
        stdout_handle=stdout_handle,
        stderr_handle=stderr_handle,
    )


def _wait_worker(worker: WorkerProcess) -> dict[str, Any]:
    timed_out = False
    try:
        returncode = worker.process.wait(timeout=worker.timeout_seconds)
    except subprocess.TimeoutExpired:
        timed_out = True
        try:
            os.killpg(worker.process.pid, signal.SIGTERM)
            worker.process.wait(timeout=3)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            try:
                os.killpg(worker.process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            worker.process.wait(timeout=3)
        returncode = worker.process.returncode
    finally:
        worker.stdout_handle.close()
        worker.stderr_handle.close()
    result = None
    if worker.result_path.is_file():
        try:
            result = json.loads(worker.result_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            result = None
    return {
        "name": worker.name,
        "command": worker.command,
        "returncode": returncode,
        "timed_out": timed_out,
        "elapsed_seconds": round(time.monotonic() - worker.started_at, 6),
        "stdout_path": str(worker.stdout_path),
        "stderr_path": str(worker.stderr_path),
        "audit_path": str(worker.audit_path),
        "result_path": str(worker.result_path),
        "result": result,
        "reaped": worker.process.poll() is not None,
    }


def _wait_for_path(path: Path, *, timeout_seconds: int) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if path.exists():
            return True
        time.sleep(0.02)
    return False


class Checks:
    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []

    def add(self, name: str, condition: Any, detail: Any = None) -> bool:
        ok = bool(condition)
        self.rows.append({"name": name, "ok": ok, "detail": detail})
        return ok

    @property
    def ok(self) -> bool:
        return bool(self.rows) and all(row["ok"] for row in self.rows)


def _normalized_terminal_transition(value: Any) -> str:
    state = str(value or "unknown").strip().lower()
    return {
        "done": "completed",
        "awaiting_user": "blocked",
        "in_progress": "running",
    }.get(state, state)


def _semantic_operation_descriptor(row: dict[str, Any]) -> dict[str, Any]:
    operation = str(row.get("operation") or "")
    destination = row.get("destination") if isinstance(row.get("destination"), dict) else {}
    card_family = operation in {"card_send", "card_reply", "card_update"}
    descriptor = {
        "source_component": row.get("source_component"),
        "operation_family": "task_card_delivery" if card_family else operation,
        "task_id_hash": row.get("task_id_hash"),
        "terminal_transition": _normalized_terminal_transition(row.get("terminal_state")),
        "route_id_hash": destination.get("id_hash"),
        "route_thread_hash": destination.get("thread_id_hash"),
    }
    if not card_family:
        descriptor.update(
            {
                "route_message_hash": destination.get("message_id_hash"),
                "payload_hmac_sha256": row.get("payload_hmac_sha256"),
                "metadata_hmac_sha256": row.get("metadata_hmac_sha256"),
            }
        )
    return descriptor


def _semantic_operation_key(row: dict[str, Any]) -> str:
    return "semantic:" + _sha256_bytes(
        _canonical_json(_semantic_operation_descriptor(row))
    )


def _semantic_snapshot(rows: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    snapshot: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = _semantic_operation_key(row)
        entry = snapshot.setdefault(
            key,
            {
                "descriptor": _semantic_operation_descriptor(row),
                "record_ids": [],
                "attempt_count": 0,
            },
        )
        record_id = str(row.get("record_id") or "")
        if record_id and record_id not in entry["record_ids"]:
            entry["record_ids"].append(record_id)
        attempt = row.get("attempt_count")
        if isinstance(attempt, int) and not isinstance(attempt, bool):
            entry["attempt_count"] = max(entry["attempt_count"], attempt)
    for entry in snapshot.values():
        entry["record_ids"].sort()
    return snapshot


def _worker_preflight(result_path: Path) -> tuple[Path, Path]:
    if os.environ.get("HERMES_L4_SANDBOX_ACTIVE") != "1":
        raise RuntimeError("worker refuses to run without the sandbox marker")
    sitecustomize = sys.modules.get("sitecustomize")
    loaded_from = Path(str(getattr(sitecustomize, "__file__", ""))).resolve(strict=True)
    expected = (AUDIT_BOOTSTRAP / "sitecustomize.py").resolve(strict=True)
    if loaded_from != expected:
        raise RuntimeError("worker audit bootstrap was not loaded from the tracked source")
    run_root = _verified_directory(Path(os.environ["HERMES_L4_RUN_ROOT"]))
    phase_root = _verified_directory(Path(os.environ["HERMES_L4_PHASE_ROOT"]))
    try:
        phase_root.relative_to(run_root)
        result_path.parent.resolve(strict=True).relative_to(run_root)
    except ValueError as exc:
        raise RuntimeError("worker paths escape the isolated run root") from exc
    os.chdir(run_root)
    return run_root, phase_root


def _scenario_payload(scenario_id: str) -> dict[str, Any]:
    sid = scenario_id.upper()
    state: dict[str, Any] = {
        "value": "completed",
        "summary": f"{sid} 离线场景完成",
        "terminal": True,
        "updated_at": "2026-07-12T10:00:00+00:00",
    }
    payload: dict[str, Any] = {
        "updated_at": "2026-07-12T10:00:00+00:00",
        "state": state,
        "artifacts": [],
        "errors": [],
        "vm_bridge": {"state": state["value"], "summary": state["summary"]},
    }
    if sid == "S1":
        payload["artifacts"] = [
            "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/l4-s1/result.json"
        ]
    elif sid == "S2":
        state.update(value="running", summary="数据下载执行中", terminal=False)
        payload["vm_bridge"] = {
            "state": "running",
            "summary": "数据下载执行中",
            "progress": {
                "phase": "s2_download",
                "message": "pipeline running",
                "ts": "2026-07-12T10:00:00+00:00",
            },
        }
        payload["delivery_contract"] = {
            "schema_version": "g1q3_delivery_contract_v1",
            "work_item_id": "7100000002",
            "business_state": "awaiting_download",
            "presentation_state": "processing",
            "report": {"status": "need_download", "is_deliverable": False},
            "summary": {"l0": "已受理；等待自动下载/解析"},
            "user_action": {"requires_user_input": False},
            "artifacts": {
                "task_root_vm": "/mnt/tmp/l4-s2/",
                "task_root_cifs": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/l4-s2/",
            },
        }
    elif sid in {"S3", "S4"}:
        case = sid.lower()
        artifacts = {
            "index_html_vm": (
                "/mnt/minieye/pdcl/department/perception_test_team/"
                f"G1Q3_RCA/cases/l4_{case}/index.html"
            ),
            "report_data_vm": (
                "/mnt/minieye/pdcl/department/perception_test_team/"
                f"G1Q3_RCA/cases/l4_{case}/report_data.json"
            ),
            "case_dir_vm": (
                "/mnt/minieye/pdcl/department/perception_test_team/"
                f"G1Q3_RCA/cases/l4_{case}"
            ),
            "case_dir_cifs": (
                "//hfs.minieye.tech/department-perception_test_team/"
                f"G1Q3_RCA/cases/l4_{case}/"
            ),
        }
        if sid == "S4":
            artifacts["viz_mcap_vm"] = (
                "/mnt/minieye/pdcl/department/perception_test_team/"
                "G1Q3_RCA/cases/l4_s4/l4_s4.viz.mcap"
            )
            artifacts["attribution_causal_text"] = "纵向控制请求波动"
        payload["delivery_contract"] = {
            "schema_version": "g1q3_delivery_contract_v1",
            "work_item_id": "7100000003" if sid == "S3" else "7100000004",
            "business_state": "report_completed",
            "presentation_state": "report_ready_needs_review",
            "report": {
                "status": "report_generated_need_review",
                "is_deliverable": True,
                "is_candidate": True,
            },
            "summary": {
                "l0": "报告已生成",
                "short_conclusion": "纵向控制请求波动",
            },
            "evidence_boundary": ["离线固定 fixture；结论需人工复核"],
            "artifacts": artifacts,
        }
    elif sid == "S5":
        state["summary"] = "poison card isolation fixture"
    elif sid == "S6":
        state["summary"] = "expired card freeze fixture"
    elif sid == "S7":
        state.update(value="blocked", summary="translate workdir permission", terminal=True)
        payload["vm_bridge"] = {"state": "blocked", "summary": state["summary"]}
        payload["delivery_contract"] = {
            "schema_version": "g1q3_delivery_contract_v1",
            "work_item_id": "7100000007",
            "business_state": "blocked_need_evidence",
            "presentation_state": "blocked",
            "report": {"status": "need_download", "is_deliverable": False},
            "pipeline_result": {
                "status": "blocked",
                "stage": "s3b_translate",
                "blocker": {
                    "kind": "translate_workdir_permission",
                    "fault_class": "infra_self_healable",
                    "retryable": True,
                    "message": "PermissionError in isolated fixture",
                },
            },
            "summary": {"l0": "工程侧修复中，数据无需补传"},
            "artifacts": {"task_root_vm": "/mnt/tmp/l4-s7/"},
        }
    elif sid in {"S8", "S9"}:
        state.update(value="blocked", summary="需要补充源数据", terminal=True)
        payload["vm_bridge"] = {"state": "blocked", "summary": state["summary"]}
        payload["delivery_contract"] = {
            "schema_version": "g1q3_delivery_contract_v1",
            "work_item_id": "7100000008" if sid == "S8" else "7100000009",
            "business_state": "missing_user_input",
            "presentation_state": "blocked",
            "report": {"status": "need_user_data", "is_deliverable": False},
            "blocker": {
                "kind": "need_data",
                "fault_class": "needs_human_input",
                "message": "请补充中文源数据与关键帧说明" if sid == "S9" else "请补充源数据",
            },
            "user_action": {
                "requires_user_input": True,
                "next_action": "请补充中文源数据与关键帧说明" if sid == "S9" else "请补充源数据",
            },
            "summary": {"l0": "待发起人补充输入"},
            "artifacts": {},
        }
    elif sid == "S10":
        cifs = "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/l4-s10/中文 报告/"
        state["summary"] = "CIFS 路径保持可复制"
        payload["artifacts"] = [cifs]
        payload["vm_bridge"] = {
            "state": "completed",
            "summary": state["summary"],
            "user_visible_path": cifs,
        }
    return payload


def _scenario_specs(sync_module: Any) -> list[dict[str, Any]]:
    specs = []
    g1q3 = {"S2", "S3", "S4", "S7", "S8", "S9"}
    for index, sid in enumerate(SCENARIO_IDS, 1):
        slug = {
            "S1": "ordinary",
            "S2": "need-download",
            "S3": "html-only",
            "S4": "foxglove-primary",
            "S5": "poison-card",
            "S6": "expired-card",
            "S7": "infra-route",
            "S8": "originator-route",
            "S9": "utf8-mention",
            "S10": "cifs-plain",
        }[sid]
        task_id = f"l4-{sid.lower()}-{slug}"
        if sid in g1q3:
            task_id += "-g1q3-rca"
        specs.append(
            {
                "scenario_id": sid,
                "task_id": task_id,
                "vm_task_id": f"l4-vm-{sid.lower()}-{slug}",
                "chat_id": sync_module.G1Q3_RCA_CHAT_ID if sid in g1q3 else sync_module.PNC_CHAT_ID,
                "message_id": f"om_l4_{sid.lower()}",
                "thread_id": f"topic:om_l4_{sid.lower()}",
                "summary": "中文任务：提及与哈希稳定" if sid == "S9" else f"L4 {sid} {slug}",
                "started_at": float(1000 + index),
            }
        )
    return specs


def _create_fixture_corpus() -> tuple[list[dict[str, Any]], Path]:
    from gateway.tasks.store import TaskStore
    from gateway.tasks.types import Task, TaskStatus, TaskType
    from hermes_cli.config import get_hermes_home
    from scripts import pnc_vm_task_sync as sync

    home = Path(get_hermes_home())
    fixture_root = home / sync.FIXTURE_DIR_NAME / "combined"
    fixture_root.mkdir(parents=True, mode=0o700, exist_ok=True)
    store = TaskStore(home / "analytics" / "tasks.db")
    specs = _scenario_specs(sync)
    for spec in specs:
        store.upsert(
            Task(
                task_id=spec["task_id"],
                status=TaskStatus.RUNNING,
                task_type=TaskType.CHAT,
                user_id="ou_l4_originator_fixture",
                platform="feishu",
                request_summary=spec["summary"],
                started_at=spec["started_at"],
                agent_route="g1q3-rca" if "g1q3-rca" in spec["task_id"] else "pnc-vm",
                chat_id=spec["chat_id"],
                chat_type="group",
                thread_id=spec["thread_id"],
                message_id=spec["message_id"],
                vm_task_id=spec["vm_task_id"],
            )
        )
        fixture = fixture_root / f"{spec['vm_task_id']}.json"
        _atomic_json(
            fixture,
            {
                "schema_version": FIXTURE_SCHEMA_VERSION,
                "vm_task_id": spec["vm_task_id"],
                "scenario_id": spec["scenario_id"],
                "payload": _scenario_payload(spec["scenario_id"]),
            },
        )
    return specs, fixture_root


def _run_vm_sync(fixture_root: Path, *, include_terminal: bool) -> dict[str, Any]:
    from scripts import pnc_vm_task_sync as sync

    args = ["--fixture-root", str(fixture_root), "--json"]
    if include_terminal:
        args.append("--include-terminal")
    for scenario_id in SCENARIO_IDS:
        args.extend(("--require-fixture-scenario", scenario_id))
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        rc = sync.main(args)
    result = json.loads(output.getvalue())
    result["returncode"] = rc
    return result


def _sidecar_path(task_id: str) -> Path:
    from hermes_cli.config import get_hermes_home

    return Path(get_hermes_home()) / "task-state" / f"{task_id}.json"


def _write_shared_meta(task_id: str, payload: dict[str, Any]) -> None:
    from hermes_cli.config import get_hermes_home

    path = Path(get_hermes_home()) / "runtime" / "shared-state" / "tasks" / task_id / "meta.json"
    _atomic_json(path, payload)


def _prime_and_mutate_combined(
    specs: list[dict[str, Any]],
) -> dict[str, Any]:
    from scripts import pnc_completion_notice_relay as relay
    from scripts.vm_task_state_bridge import _atomic_write_json

    task_ids = [spec["task_id"] for spec in specs]
    # This is the production scan/reconcile path, but not a send.
    relay.iter_pending_notices(task_ids=task_ids)
    by_scenario = {spec["scenario_id"]: spec for spec in specs}

    poison_path = _sidecar_path(by_scenario["S5"]["task_id"])
    poison = json.loads(poison_path.read_text(encoding="utf-8"))
    poison_card = poison["task_card"]
    poison_card.setdefault("delivery", {})["conclusion"] = "经验证 poison fixture 不得渲染"
    poison["task_card"] = poison_card
    # Model a stale standalone poison card. A fresh VM proposal would correctly
    # overwrite this field before render and would not exercise isolation.
    poison.pop("vm_delivery_proposal", None)
    _atomic_write_json(poison_path, poison)

    expired_path = _sidecar_path(by_scenario["S6"]["task_id"])
    expired = json.loads(expired_path.read_text(encoding="utf-8"))
    expired["task_card"]["card_message_id"] = "om_l4_expired_card"
    _atomic_write_json(expired_path, expired)
    expired_calls: list[dict[str, Any]] = []

    def expired_sender(target: str, card: dict[str, Any], message_id: str | None = None) -> dict[str, Any]:
        expired_calls.append({"target": target, "message_id": message_id, "card_hash": _sha256_bytes(_canonical_json(card))})
        return {"success": False, "error": "230031 message has expired"}

    first_expired = relay.sync_task_card(
        task_id=by_scenario["S6"]["task_id"],
        path=expired_path,
        body=json.loads(expired_path.read_text(encoding="utf-8")),
        send=True,
        send_card_func=expired_sender,
        throttle_seconds=0,
    )
    second_expired = relay.sync_task_card(
        task_id=by_scenario["S6"]["task_id"],
        path=expired_path,
        body=json.loads(expired_path.read_text(encoding="utf-8")),
        send=True,
        send_card_func=expired_sender,
        throttle_seconds=0,
    )

    route_payloads = {
        "S7": {
            "blocker": {
                "kind": "translate_workdir_permission",
                "fault_class": "infra_self_healable",
                "retryable": True,
                "message": "PermissionError in isolated fixture",
            },
            "user_state": "in_progress",
            "report_status": "need_pipeline_fix",
            "human_action_kind": "none",
            "action_category": "none",
            "requester": "ou_l4_originator_s7",
            "summary": "工程侧自愈；不得要求发起人补数据",
        },
        "S8": {
            "blocker": {
                "kind": "need_data",
                "fault_class": "needs_human_input",
                "message": "请补充源数据",
            },
            "user_state": "awaiting_user",
            "report_status": "need_user_data",
            "human_action_kind": "need_data",
            "action_category": "hard",
            "requester": "ou_l4_originator_s8",
            "summary": "请补充源数据",
        },
        "S9": {
            "blocker": {
                "kind": "need_data",
                "fault_class": "needs_human_input",
                "message": "请补充中文源数据与关键帧说明",
            },
            "user_state": "awaiting_user",
            "report_status": "need_user_data",
            "human_action_kind": "need_data",
            "action_category": "hard",
            "requester": "ou_l4_utf8_originator",
            "summary": "中文任务需要补充：方向盘角度与关键帧说明",
        },
    }
    for sid, route in route_payloads.items():
        spec = by_scenario[sid]
        path = _sidecar_path(spec["task_id"])
        body = json.loads(path.read_text(encoding="utf-8"))
        card = body["task_card"]
        card["user_state"] = route["user_state"]
        card["diagnostics"] = {"blocker": route["blocker"]}
        delivery = card.setdefault("delivery", {})
        delivery.update(
            {
                "conclusion": route["summary"],
                "missing_reason": route["summary"],
                "report_status": route["report_status"],
                "human_action_kind": route["human_action_kind"],
                "action_category": route["action_category"],
            }
        )
        card["delivery"] = delivery
        body["task_card"] = card
        _atomic_write_json(path, body)
        _write_shared_meta(
            spec["task_id"],
            {
                "schema_version": 1,
                "state": "blocked",
                "business_line": "g1q3_rca",
                "requester": route["requester"],
                "latest_summary": route["summary"],
                "updated_at": "2026-07-12T10:00:00+00:00",
                "work_item_id": {
                    "S7": "7100000007",
                    "S8": "7100000008",
                    "S9": "7100000009",
                }[sid],
            },
        )
    return {
        "expired_first": first_expired,
        "expired_second": second_expired,
        "expired_sender_calls": expired_calls,
    }


def _read_ledger() -> tuple[Any, list[dict[str, Any]], dict[str, Any], Path]:
    from gateway.record_only import runtime

    transport = runtime.get_record_only_transport("scripts.pnc_completion_notice_relay")
    if transport is None:
        raise RuntimeError("record-only transport unavailable")
    rows = transport.read_all()
    ledger = transport.ledger
    raw_lines = ledger.read_bytes().splitlines()
    header = json.loads(raw_lines[0])
    return transport, rows, header, ledger


def _ledger_contract_checks(
    checks: Checks,
    *,
    transport: Any,
    rows: list[dict[str, Any]],
    header: dict[str, Any],
    task_ids: Iterable[str],
) -> None:
    payload_types = {
        "card_send": "interactive_card",
        "card_update": "interactive_card",
        "file_send": "file",
        "project_comment_add": "text",
        "project_comment_list": "query",
        "text_reply": "text",
        "text_send": "text",
    }
    allowed_sources = {
        "gateway.pnc_report_comment",
        "scripts.pnc_completion_notice_relay",
        "scripts.pnc_vm_task_sync.report_attachment",
    }
    checks.add("ledger_has_rows", bool(rows), len(rows))
    checks.add("ledger_header_record_count", header.get("record_count") == len(rows), header.get("record_count"))
    checks.add("ledger_header_generation_positive", isinstance(header.get("generation"), int) and header["generation"] >= 1, header.get("generation"))
    checks.add("ledger_header_chain_head", header.get("chain_head_hmac_sha256") == rows[-1].get("integrity_hmac_sha256"), header.get("chain_head_hmac_sha256"))
    checks.add("ledger_sources_exact", all(row.get("source_component") in allowed_sources for row in rows), sorted({row.get("source_component") for row in rows}))
    checks.add("ledger_operations_typed", all(payload_types.get(row.get("operation")) == row.get("payload_type") for row in rows), [(row.get("operation"), row.get("payload_type")) for row in rows])
    checks.add("ledger_destination_hashes", all((row.get("destination") or {}).get("id_hash") for row in rows))
    checks.add("ledger_task_hashes", all(row.get("task_id_hash") for row in rows))
    checks.add("ledger_caller_dedupe_hashes", all(row.get("caller_dedupe_key_hash") for row in rows))
    checks.add("ledger_content_hmacs", all(SHA256_RE.fullmatch(str(row.get("payload_hmac_sha256") or "")) and SHA256_RE.fullmatch(str(row.get("metadata_hmac_sha256") or "")) for row in rows))
    checks.add("ledger_attempt_counts", all(isinstance(row.get("attempt_count"), int) and row["attempt_count"] >= 1 for row in rows))
    checks.add("ledger_external_delivery_false", all(row.get("external_delivery_attempted") is False and row.get("external_delivery_verified") is False for row in rows))
    checks.add("ledger_authority_false", all(row.get("candidate_execution_authorized") is False and row.get("promotion_authorized") is False and row.get("cutover_authorized") is False for row in rows))
    serialized = json.dumps(rows, ensure_ascii=False, sort_keys=True)
    checks.add("ledger_raw_task_ids_absent", all(task_id not in serialized for task_id in task_ids))
    checks.add("ledger_chain_reverified_by_transport", len(transport.read_all()) == len(rows))


def _combined_worker() -> dict[str, Any]:
    from gateway.record_only import runtime
    from scripts import pnc_completion_notice_relay as relay
    from scripts import pnc_vm_task_sync as sync

    checks = Checks()
    runtime._reset_for_tests()
    specs, fixture_root = _create_fixture_corpus()
    task_ids = [spec["task_id"] for spec in specs]
    all_scoped_ids = task_ids + [spec["vm_task_id"] for spec in specs]
    initial_sync = _run_vm_sync(fixture_root, include_terminal=False)
    checks.add("initial_sync_returncode", initial_sync.get("returncode") == 0, initial_sync)
    checks.add("initial_sync_all_scenarios", set(initial_sync.get("executed_fixture_scenarios") or []) == set(SCENARIO_IDS), initial_sync.get("executed_fixture_scenarios"))
    checks.add("initial_sync_ten", initial_sync.get("synced_count") == 10, initial_sync.get("synced_count"))
    checks.add(
        "initial_sync_fixture_evidence_only",
        initial_sync.get("pipeline_evidence_source") == "fixture"
        and all(
            row.get("pipeline_evidence_source") == "fixture"
            for row in initial_sync.get("rows", [])
        ),
        initial_sync.get("pipeline_evidence_source"),
    )
    mutation = _prime_and_mutate_combined(specs)

    s3 = next(spec for spec in specs if spec["scenario_id"] == "S3")
    s3_index = _scenario_payload("S3")["delivery_contract"]["artifacts"]["index_html_vm"]
    attachment_first = sync._feishu_report_attachment_link(
        work_item_id="7100000003",
        vm_task_id=s3["vm_task_id"],
        index_html=s3_index,
    )
    attachment_second = sync._feishu_report_attachment_link(
        work_item_id="7100000003",
        vm_task_id=s3["vm_task_id"],
        index_html=s3_index,
    )
    checks.add("attachment_guard_returns_no_live_url", attachment_first == attachment_second == "")

    first_relay = relay.relay_pending_notices(
        task_ids=task_ids,
        send=True,
        limit=20,
        max_card_fallbacks_per_loop=0,
    )
    # S5 proves a malformed card cannot starve following tasks. A fresh proposal
    # then heals it exactly once. Complete that expected initial convergence
    # before taking the replay baseline.
    stabilization_sync = _run_vm_sync(fixture_root, include_terminal=True)
    checks.add("stabilization_sync_returncode", stabilization_sync.get("returncode") == 0, stabilization_sync)
    checks.add(
        "stabilization_sync_fixture_evidence_only",
        stabilization_sync.get("pipeline_evidence_source") == "fixture"
        and all(
            row.get("pipeline_evidence_source") == "fixture"
            for row in stabilization_sync.get("rows", [])
        ),
        stabilization_sync.get("pipeline_evidence_source"),
    )
    stabilization_relay = relay.relay_pending_notices(
        task_ids=task_ids,
        send=True,
        limit=20,
        max_card_fallbacks_per_loop=0,
    )
    transport, before_rows, before_header, ledger = _read_ledger()
    before_ids = {row["record_id"] for row in before_rows}
    before_attempts = {row["record_id"]: row["attempt_count"] for row in before_rows}
    before_semantics = _semantic_snapshot(before_rows)

    replay_sync = _run_vm_sync(fixture_root, include_terminal=True)
    checks.add("replay_sync_returncode", replay_sync.get("returncode") == 0, replay_sync)
    checks.add("replay_sync_all_scenarios", set(replay_sync.get("executed_fixture_scenarios") or []) == set(SCENARIO_IDS), replay_sync.get("executed_fixture_scenarios"))
    checks.add(
        "replay_sync_fixture_evidence_only",
        replay_sync.get("pipeline_evidence_source") == "fixture"
        and all(
            row.get("pipeline_evidence_source") == "fixture"
            for row in replay_sync.get("rows", [])
        ),
        replay_sync.get("pipeline_evidence_source"),
    )
    sync._feishu_report_attachment_link(
        work_item_id="7100000003",
        vm_task_id=s3["vm_task_id"],
        index_html=s3_index,
    )
    second_relay = relay.relay_pending_notices(
        task_ids=task_ids,
        send=True,
        limit=20,
        max_card_fallbacks_per_loop=0,
    )
    transport, after_rows, after_header, ledger = _read_ledger()
    after_ids = {row["record_id"] for row in after_rows}
    after_semantics = _semantic_snapshot(after_rows)
    checks.add("replay_no_new_logical_intent", before_ids == after_ids, {"before": len(before_ids), "after": len(after_ids)})
    checks.add(
        "replay_semantic_keys_stable",
        set(before_semantics) == set(after_semantics),
        {"before": len(before_semantics), "after": len(after_semantics)},
    )
    checks.add(
        "replay_single_disposition_per_semantic_transition",
        all(len(entry["record_ids"]) == 1 for entry in after_semantics.values()),
        {
            key: entry["record_ids"]
            for key, entry in after_semantics.items()
            if len(entry["record_ids"]) != 1
        },
    )
    checks.add(
        "replay_attempts_monotonic",
        not (after_ids - before_ids)
        and all(
            row["attempt_count"]
            >= before_attempts.get(row["record_id"], row["attempt_count"] + 1)
            for row in after_rows
        ),
        {row["record_id"]: row["attempt_count"] for row in after_rows},
    )
    checks.add(
        "replay_exercised_dedupe",
        any(
            row["record_id"] in before_attempts
            and row["attempt_count"] > before_attempts[row["record_id"]]
            for row in after_rows
        ),
    )
    _ledger_contract_checks(
        checks,
        transport=transport,
        rows=after_rows,
        header=after_header,
        task_ids=all_scoped_ids,
    )

    relay_rows = {row.get("task_id"): row for row in first_relay.get("rows", [])}
    specs_by_sid = {spec["scenario_id"]: spec for spec in specs}
    scenario_results: dict[str, dict[str, Any]] = {}

    def body_for(sid: str) -> dict[str, Any]:
        return json.loads(_sidecar_path(specs_by_sid[sid]["task_id"]).read_text(encoding="utf-8"))

    def task_rows(sid: str) -> list[dict[str, Any]]:
        task_hash = transport._hash_id(specs_by_sid[sid]["task_id"])
        return [row for row in after_rows if row.get("task_id_hash") == task_hash]

    s1_rows = task_rows("S1")
    s1_thread_hash = transport._hash_id(specs_by_sid["S1"]["message_id"])
    s1_ok = bool(s1_rows) and any(row["operation"] == "card_send" for row in s1_rows) and any(row["operation"] == "text_reply" for row in s1_rows) and all((row["destination"] or {}).get("thread_id_hash") == s1_thread_hash for row in s1_rows)
    scenario_results["S1"] = {"ok": s1_ok, "operations": [row["operation"] for row in s1_rows]}

    s2_body = body_for("S2")
    s2_delivery = (s2_body.get("task_card") or {}).get("delivery") or {}
    s2_text = json.dumps(s2_delivery, ensure_ascii=False)
    s2_sync_rows = [
        row
        for row in replay_sync.get("rows", [])
        if row.get("fixture_scenario_id") == "S2"
    ]
    s2_evidence_source = (
        s2_sync_rows[0].get("pipeline_evidence_source")
        if len(s2_sync_rows) == 1
        else None
    )
    s2_ok = (
        s2_delivery.get("report_status") in {"need_download", "need_user_data"}
        and not any(
            value in s2_text
            for value in ("report_ready", "html_delivery_ready", "foxglove-http")
        )
        and s2_evidence_source == "fixture"
    )
    scenario_results["S2"] = {
        "ok": s2_ok,
        "delivery": s2_delivery,
        "pipeline_evidence_source": s2_evidence_source,
    }

    s3_delivery = (body_for("S3").get("task_card") or {}).get("delivery") or {}
    s3_ok = s3_delivery.get("report_status") == "html_delivery_ready" and str(s3_delivery.get("artifact_path") or "").endswith("index.html") and not s3_delivery.get("foxglove_url")
    scenario_results["S3"] = {"ok": s3_ok, "delivery": s3_delivery}

    s4_delivery = (body_for("S4").get("task_card") or {}).get("delivery") or {}
    s4_ok = s4_delivery.get("report_status") == "report_ready" and "ds=foxglove-http" in str(s4_delivery.get("artifact_path") or "") and str(s4_delivery.get("report_index_html_vm") or "").endswith("index.html") and str(s4_delivery.get("report_data_vm") or "").endswith("report_data.json")
    scenario_results["S4"] = {"ok": s4_ok, "delivery": s4_delivery}

    s5_row = relay_rows.get(specs_by_sid["S5"]["task_id"], {})
    following_present = specs_by_sid["S6"]["task_id"] in relay_rows
    s5_ok = (s5_row.get("task_card") or {}).get("reason") == "render_error" and following_present
    scenario_results["S5"] = {"ok": s5_ok, "relay_row": s5_row, "following_present": following_present}

    s6_body = body_for("S6")
    s6_ok = (mutation.get("expired_first") or {}).get("reason") == "card_message_expired" and (mutation.get("expired_second") or {}).get("reason") == "hash_unchanged" and len(mutation.get("expired_sender_calls") or []) == 1 and bool((s6_body.get("task_card") or {}).get("card_message_expired_at")) and "task_card_fallback" not in (relay_rows.get(specs_by_sid["S6"]["task_id"], {}) or {})
    scenario_results["S6"] = {"ok": s6_ok, **mutation}

    s7_row = relay_rows.get(specs_by_sid["S7"]["task_id"], {})
    s7_infra = s7_row.get("infra_notify") or {}
    s7_origin = s7_row.get("originator_notify") or {}
    s7_ok = s7_infra.get("sent") is True and s7_origin.get("sent") is not True and s7_origin.get("reason") in {"infra_self_healable_no_originator_ping", "pipeline_fix_no_originator_ping"}
    scenario_results["S7"] = {"ok": s7_ok, "infra": s7_infra, "originator": s7_origin}

    s8_row = relay_rows.get(specs_by_sid["S8"]["task_id"], {})
    s8_origin = s8_row.get("originator_notify") or {}
    s8_infra = s8_row.get("infra_notify") or {}
    s8_ok = s8_origin.get("sent") is True and s8_origin.get("has_mention") is True and s8_infra.get("sent") is not True
    scenario_results["S8"] = {"ok": s8_ok, "originator": s8_origin, "infra": s8_infra}

    s9_rows = task_rows("S9")
    s9_serialized = json.dumps(s9_rows, ensure_ascii=False)
    s9_ok = "中文" in s9_serialized and any(row.get("mentions") for row in s9_rows) and all(row.get("recorded_payload_hash") for row in s9_rows)
    scenario_results["S9"] = {"ok": s9_ok, "operations": [row["operation"] for row in s9_rows]}

    s10_rows = task_rows("S10")
    s10_serialized = json.dumps(s10_rows, ensure_ascii=False)
    s10_ok = "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/l4-s10/中文 报告/" in s10_serialized and "https://hfs1.minieye.tech" not in s10_serialized
    scenario_results["S10"] = {"ok": s10_ok, "operations": [row["operation"] for row in s10_rows]}

    for sid in SCENARIO_IDS:
        checks.add(f"scenario_{sid.lower()}", scenario_results[sid]["ok"], scenario_results[sid])
    runtime._reset_for_tests()
    return {
        "schema_version": WORKER_SCHEMA_VERSION,
        "worker": "combined",
        "ok": checks.ok,
        "checks": checks.rows,
        "scenarios": scenario_results,
        "initial_sync": initial_sync,
        "first_relay": first_relay,
        "stabilization_sync": stabilization_sync,
        "stabilization_relay": stabilization_relay,
        "replay_sync": replay_sync,
        "second_relay": second_relay,
        "ledger": {
            "path": str(ledger),
            "sha256": _sha256_file(ledger),
            "record_count": len(after_rows),
            "generation": after_header.get("generation"),
            "chain_head_hmac_sha256": after_header.get("chain_head_hmac_sha256"),
            "external_delivery_attempted": False,
        },
        "replay_disposition": {
            "before_record_count": len(before_rows),
            "after_record_count": len(after_rows),
            "before_semantic_count": len(before_semantics),
            "after_semantic_count": len(after_semantics),
            "new_record_ids": sorted(after_ids - before_ids),
            "removed_record_ids": sorted(before_ids - after_ids),
            "semantic_keys_stable": set(before_semantics) == set(after_semantics),
        },
    }


class L4ProcessTermination(BaseException):
    """Intentional abrupt worker termination used only by the crash matrix."""


def _crash_sidecar(boundary: str) -> tuple[str, Path]:
    from scripts import pnc_vm_task_sync as sync

    task_id = f"l4-crash-{_safe_leaf(boundary, field='crash boundary')}"
    path = _sidecar_path(task_id)
    if not path.exists():
        _atomic_json(
            path,
            {
                "completion_notice": {
                    "schema_version": 1,
                    "generated_at": "2026-07-12T10:00:00+00:00",
                    "source": "l4_crash_fixture",
                    "state": "completed",
                    "text": f"L4 crash boundary {boundary}",
                    "send_status": "pending",
                    "chat_id": sync.PNC_CHAT_ID,
                    "thread_id": f"topic:om_l4_crash_{boundary}",
                    "message_id": f"om_l4_crash_{boundary}",
                    "vm_task_id": f"vm-{task_id}",
                }
            },
        )
    return task_id, path


def _crash_inject_worker(boundary: str) -> None:
    from gateway.record_only import runtime
    from scripts import pnc_completion_notice_relay as relay

    if boundary not in CRASH_BOUNDARIES:
        raise ValueError(f"unknown crash boundary: {boundary}")
    runtime._reset_for_tests()
    task_id, _path = _crash_sidecar(boundary)

    def terminate(stage: str, context: dict[str, Any]) -> None:
        if stage == boundary:
            raise L4ProcessTermination(
                f"L4_PROCESS_TERMINATION:{boundary}:{context.get('task_id')}"
            )

    relay.relay_pending_notices(
        task_ids=[task_id],
        send=True,
        limit=1,
        crash_hook=terminate,
    )
    raise RuntimeError(f"crash checkpoint was not reached: {boundary}")


def _optional_ledger_rows() -> list[dict[str, Any]]:
    from gateway.record_only import runtime

    transport = runtime.get_record_only_transport("scripts.pnc_completion_notice_relay")
    if transport is None or not transport.ledger.exists():
        return []
    return transport.read_all()


def _crash_restart_worker(boundary: str) -> dict[str, Any]:
    from gateway.record_only import runtime
    from scripts import pnc_completion_notice_relay as relay

    runtime._reset_for_tests()
    task_id, path = _crash_sidecar(boundary)
    before_body = json.loads(path.read_text(encoding="utf-8"))
    before_rows = _optional_ledger_rows()
    replay = relay.relay_pending_notices(task_ids=[task_id], send=True, limit=1)
    after_body = json.loads(path.read_text(encoding="utf-8"))
    after_rows = _optional_ledger_rows()
    before_status = str((before_body.get("completion_notice") or {}).get("send_status") or "")
    after_status = str((after_body.get("completion_notice") or {}).get("send_status") or "")
    expected_before_count = 0 if boundary == "before_sender" else 1
    expected_before_attempt = 0 if boundary == "before_sender" else 1
    expected_before_status = "sent" if boundary == "after_mark_before_ack" else "pending"
    expected_after_attempt = {
        "before_sender": 1,
        "after_record_before_mark": 2,
        "before_mark_persist": 2,
        "after_mark_before_ack": 1,
    }[boundary]
    checks = Checks()
    checks.add("pre_restart_record_count", len(before_rows) == expected_before_count, len(before_rows))
    checks.add("pre_restart_attempt_count", (before_rows[0]["attempt_count"] if before_rows else 0) == expected_before_attempt, before_rows)
    checks.add("pre_restart_sidecar_status", before_status == expected_before_status, before_status)
    checks.add("post_restart_one_logical_record", len(after_rows) == 1, len(after_rows))
    checks.add("post_restart_attempt_count", bool(after_rows) and after_rows[0]["attempt_count"] == expected_after_attempt, after_rows)
    checks.add("post_restart_sidecar_sent", after_status == "sent", after_status)
    checks.add("post_restart_no_external_delivery", bool(after_rows) and after_rows[0]["external_delivery_attempted"] is False)
    runtime._reset_for_tests()
    return {
        "schema_version": WORKER_SCHEMA_VERSION,
        "worker": "crash-restart",
        "boundary": boundary,
        "ok": checks.ok,
        "checks": checks.rows,
        "before": {"sidecar_status": before_status, "rows": before_rows},
        "after": {"sidecar_status": after_status, "rows": after_rows},
        "replay": replay,
    }


def _concurrency_setup_worker() -> dict[str, Any]:
    from gateway.record_only import runtime
    from scripts import pnc_completion_notice_relay as relay

    runtime._reset_for_tests()
    specs, fixture_root = _create_fixture_corpus()
    sync_result = _run_vm_sync(fixture_root, include_terminal=False)
    task_ids = [spec["task_id"] for spec in specs]
    candidates = relay.iter_pending_notices(task_ids=task_ids)
    first_card_hashes = {
        task_id: _sha256_bytes(
            _canonical_json(
                json.loads(_sidecar_path(task_id).read_text(encoding="utf-8")).get("task_card") or {}
            )
        )
        for task_id in task_ids
    }
    relay.iter_pending_notices(task_ids=task_ids)
    second_card_hashes = {
        task_id: _sha256_bytes(
            _canonical_json(
                json.loads(_sidecar_path(task_id).read_text(encoding="utf-8")).get("task_card") or {}
            )
        )
        for task_id in task_ids
    }
    checks = Checks()
    checks.add("setup_sync", sync_result.get("returncode") == 0 and sync_result.get("synced_count") == 10, sync_result)
    checks.add("setup_cards_reconciled", all((_sidecar_path(task_id).is_file()) for task_id in task_ids), len(candidates))
    checks.add(
        "setup_cards_at_fixed_point",
        first_card_hashes == second_card_hashes,
        {
            task_id: {"first": first_card_hashes[task_id], "second": second_card_hashes[task_id]}
            for task_id in task_ids
            if first_card_hashes[task_id] != second_card_hashes[task_id]
        },
    )
    runtime._reset_for_tests()
    return {
        "schema_version": WORKER_SCHEMA_VERSION,
        "worker": "concurrency-setup",
        "ok": checks.ok,
        "checks": checks.rows,
        "fixture_root": str(fixture_root),
        "task_ids": task_ids,
        "sync": sync_result,
    }


def _concurrency_lock_holder(ready: Path, release: Path) -> dict[str, Any]:
    from hermes_cli.config import get_hermes_home
    from scripts import pnc_vm_task_sync as sync

    home = sync._validate_isolated_hermes_home(Path(get_hermes_home()), prepare_outputs=True)
    sync._validate_fixture_record_paths(home)
    binding = sync._capture_fixture_output_binding(home)
    acquired = False
    released = False
    with sync.FixtureSingleRunLock(binding) as lock:
        acquired = bool(lock.acquired)
        _atomic_json(ready, {"acquired": acquired, "pid": os.getpid()})
        deadline = time.monotonic() + 20
        while acquired and time.monotonic() < deadline:
            if release.exists():
                released = True
                break
            time.sleep(0.02)
    return {
        "schema_version": WORKER_SCHEMA_VERSION,
        "worker": "concurrency-lock-holder",
        "ok": acquired and released,
        "acquired": acquired,
        "release_observed": released,
    }


def _fixture_root_from_home() -> Path:
    from hermes_cli.config import get_hermes_home
    from scripts import pnc_vm_task_sync as sync

    return Path(get_hermes_home()) / sync.FIXTURE_DIR_NAME / "combined"


def _wait_start(start: Path) -> None:
    if not _wait_for_path(start, timeout_seconds=15):
        raise TimeoutError(f"concurrency start barrier not released: {start}")


def _concurrent_sync_worker(start: Path) -> dict[str, Any]:
    _wait_start(start)
    result = _run_vm_sync(_fixture_root_from_home(), include_terminal=True)
    return {
        "schema_version": WORKER_SCHEMA_VERSION,
        "worker": "concurrent-sync",
        "ok": result.get("returncode") == 0,
        "sync": result,
    }


def _lock_contender_worker() -> dict[str, Any]:
    result = _run_vm_sync(_fixture_root_from_home(), include_terminal=True)
    skipped = result.get("skipped") is True and result.get("reason") == "another pnc_vm_task_sync run is active"
    return {
        "schema_version": WORKER_SCHEMA_VERSION,
        "worker": "lock-contender",
        "ok": skipped and result.get("returncode") == 0,
        "sync": result,
    }


def _concurrent_relay_worker(start: Path) -> dict[str, Any]:
    from scripts import pnc_completion_notice_relay as relay
    from scripts import pnc_vm_task_sync as sync

    _wait_start(start)
    task_ids = [spec["task_id"] for spec in _scenario_specs(sync)]
    result = relay.relay_pending_notices(
        task_ids=task_ids,
        send=True,
        limit=20,
        max_card_fallbacks_per_loop=0,
    )
    return {
        "schema_version": WORKER_SCHEMA_VERSION,
        "worker": "concurrent-relay",
        "ok": isinstance(result, dict),
        "relay": result,
    }


def _concurrent_card_worker(start: Path) -> dict[str, Any]:
    from gateway.record_only import runtime
    from gateway.record_only.transport import RecordOnlyRelaySender
    from scripts import pnc_completion_notice_relay as relay
    from scripts import pnc_vm_task_sync as sync

    _wait_start(start)
    transport = runtime.get_record_only_transport("scripts.pnc_completion_notice_relay")
    if transport is None:
        raise RuntimeError("record-only card transport unavailable")
    sender = RecordOnlyRelaySender(transport)
    rows = []
    for spec in _scenario_specs(sync):
        path = _sidecar_path(spec["task_id"])
        body = json.loads(path.read_text(encoding="utf-8"))
        text_sender, card_sender = relay._record_only_task_senders(
            sender,
            task_id=spec["task_id"],
            body=body,
            notice=body.get("completion_notice") if isinstance(body.get("completion_notice"), dict) else {},
        )
        del text_sender
        try:
            result = relay.sync_task_card(
                task_id=spec["task_id"],
                path=path,
                body=body,
                send=True,
                send_card_func=card_sender,
                throttle_seconds=0,
            )
        except Exception as exc:
            result = {"error": f"{type(exc).__name__}: {exc}"}
        rows.append({"task_id": spec["task_id"], "result": result})
    runtime._reset_for_tests()
    return {
        "schema_version": WORKER_SCHEMA_VERSION,
        "worker": "concurrent-card",
        "ok": all("error" not in (row.get("result") or {}) for row in rows),
        "rows": rows,
    }


def _concurrency_restart_check_worker() -> dict[str, Any]:
    from gateway.record_only import runtime
    from gateway.tasks.store import TaskStore
    from hermes_cli.config import get_hermes_home
    from scripts import pnc_completion_notice_relay as relay
    from scripts import pnc_vm_task_sync as sync

    specs = _scenario_specs(sync)
    task_ids = [spec["task_id"] for spec in specs]
    checks = Checks()
    parsed: dict[str, dict[str, Any]] = {}
    for spec in specs:
        try:
            body = json.loads(_sidecar_path(spec["task_id"]).read_text(encoding="utf-8"))
        except Exception as exc:
            body = {"parse_error": f"{type(exc).__name__}: {exc}"}
        parsed[spec["task_id"]] = body
        checks.add(f"json_{spec['scenario_id'].lower()}", "parse_error" not in body, body.get("parse_error"))
        card = body.get("task_card") if isinstance(body.get("task_card"), dict) else {}
        checks.add(f"task_binding_{spec['scenario_id'].lower()}", not card or card.get("task_id") == spec["task_id"], card.get("task_id"))
        if spec["scenario_id"] != "S2":
            notice = body.get("completion_notice") if isinstance(body.get("completion_notice"), dict) else {}
            checks.add(f"terminal_notice_{spec['scenario_id'].lower()}", bool(notice.get("state")), notice)
    store = TaskStore(Path(get_hermes_home()) / "analytics" / "tasks.db")
    tasks = {task.task_id: task for task in store.list_recent(limit=50)}
    checks.add("taskstore_all_tasks", set(task_ids).issubset(tasks), sorted(tasks))

    sync_result = _run_vm_sync(_fixture_root_from_home(), include_terminal=True)
    relay_one = relay.relay_pending_notices(task_ids=task_ids, send=True, limit=20, max_card_fallbacks_per_loop=0)
    transport, rows_one, _header_one, _ledger = _read_ledger()
    ids_one = {row["record_id"] for row in rows_one}
    semantics_one = _semantic_snapshot(rows_one)
    relay_two = relay.relay_pending_notices(task_ids=task_ids, send=True, limit=20, max_card_fallbacks_per_loop=0)
    transport, rows_two, header_two, ledger = _read_ledger()
    ids_two = {row["record_id"] for row in rows_two}
    semantics_two = _semantic_snapshot(rows_two)
    checks.add("restart_sync_ok", sync_result.get("returncode") == 0, sync_result)
    checks.add(
        "restart_fixture_evidence_only",
        sync_result.get("pipeline_evidence_source") == "fixture"
        and all(
            row.get("pipeline_evidence_source") == "fixture"
            for row in sync_result.get("rows", [])
        ),
        sync_result.get("pipeline_evidence_source"),
    )
    checks.add("restart_no_duplicate_logical_intent", ids_one == ids_two, {"one": len(ids_one), "two": len(ids_two)})
    checks.add(
        "restart_semantic_keys_stable",
        set(semantics_one) == set(semantics_two),
        {"one": len(semantics_one), "two": len(semantics_two)},
    )
    checks.add(
        "restart_single_disposition_per_semantic_transition",
        all(len(entry["record_ids"]) == 1 for entry in semantics_two.values()),
        {
            key: entry["record_ids"]
            for key, entry in semantics_two.items()
            if len(entry["record_ids"]) != 1
        },
    )
    checks.add("restart_ledger_valid", len(transport.read_all()) == len(rows_two), len(rows_two))
    checks.add("restart_external_delivery_false", all(row["external_delivery_attempted"] is False for row in rows_two))
    runtime._reset_for_tests()
    return {
        "schema_version": WORKER_SCHEMA_VERSION,
        "worker": "concurrency-restart-check",
        "ok": checks.ok,
        "checks": checks.rows,
        "sync": sync_result,
        "relay_one": relay_one,
        "relay_two": relay_two,
        "ledger": {
            "path": str(ledger),
            "sha256": _sha256_file(ledger),
            "record_count": len(rows_two),
            "generation": header_two.get("generation"),
        },
        "restart_disposition": {
            "first_record_count": len(rows_one),
            "second_record_count": len(rows_two),
            "first_semantic_count": len(semantics_one),
            "second_semantic_count": len(semantics_two),
            "new_record_ids": sorted(ids_two - ids_one),
            "semantic_keys_stable": set(semantics_one) == set(semantics_two),
        },
    }


def _git_output(*args: str) -> bytes:
    proc = subprocess.run(
        ["/usr/bin/git", *args],
        cwd=REPO_ROOT,
        env={"PATH": "/usr/bin:/bin", "LC_ALL": "C"},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=15,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"git {' '.join(args)} failed: {proc.stderr.decode('utf-8', errors='replace')[:500]}"
        )
    return proc.stdout


def _source_binding(expected_commit: str, expected_tree: str) -> dict[str, Any]:
    commit = _git_output("rev-parse", "HEAD^{commit}").decode().strip()
    tree = _git_output("rev-parse", "HEAD^{tree}").decode().strip()
    status_raw = _git_output("status", "--porcelain=v1", "-uall")
    diff_raw = _git_output("diff", "--binary")
    untracked = [
        value.decode("utf-8")
        for value in _git_output("ls-files", "--others", "--exclude-standard", "-z").split(b"\0")
        if value
    ]
    overlay = hashlib.sha256()
    overlay.update(status_raw)
    overlay.update(diff_raw)
    for relative in sorted(untracked):
        path = REPO_ROOT / relative
        overlay.update(relative.encode("utf-8") + b"\0")
        if path.is_file():
            overlay.update(path.read_bytes())
        elif path.is_symlink():
            overlay.update(os.readlink(path).encode("utf-8"))
    clean = not status_raw.strip()
    expected_present = bool(expected_commit and expected_tree)
    expected_valid = bool(GIT_ID_RE.fullmatch(expected_commit) and GIT_ID_RE.fullmatch(expected_tree))
    ok = clean and expected_present and expected_valid and commit == expected_commit and tree == expected_tree
    return {
        "status": "PASS" if ok else "FAIL",
        "ok": ok,
        "commit": commit,
        "tree": tree,
        "expected_commit": expected_commit or None,
        "expected_tree": expected_tree or None,
        "clean": clean,
        "status_lines": status_raw.decode("utf-8", errors="replace").splitlines(),
        "overlay_sha256": overlay.hexdigest(),
        "tracked_harness_sha256": _sha256_file(Path(__file__).resolve()),
        "audit_bootstrap_sha256": _sha256_file(AUDIT_BOOTSTRAP / "sitecustomize.py"),
    }


def _focused_tests_gate(path: Path | None, source: dict[str, Any]) -> dict[str, Any]:
    if path is None:
        return {
            "status": "NOT_RUN",
            "ok": False,
            "reason": "--focused-tests-receipt is required for L4 PASS",
        }
    try:
        raw = path.read_bytes()
        payload = json.loads(raw)
    except (OSError, json.JSONDecodeError) as exc:
        return {"status": "FAIL", "ok": False, "reason": f"invalid focused test receipt: {exc}"}
    ok = (
        isinstance(payload, dict)
        and payload.get("schema_version") == "hermes_l4_focused_tests_receipt_v1"
        and payload.get("ok") is True
        and payload.get("failed") == 0
        and isinstance(payload.get("passed"), int)
        and payload.get("passed") == payload.get("node_count")
        and payload.get("passed", 0) > 0
        and payload.get("candidate_commit") == source.get("commit")
        and payload.get("candidate_tree") == source.get("tree")
        and SHA256_RE.fullmatch(str(payload.get("test_node_manifest_sha256") or ""))
    )
    return {
        "status": "PASS" if ok else "FAIL",
        "ok": ok,
        "path": str(path.resolve()),
        "sha256": _sha256_bytes(raw),
        "summary": payload if isinstance(payload, dict) else None,
    }


def _read_audit(path: Path) -> dict[str, Any]:
    rows = []
    errors = []
    try:
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                errors.append(f"line {line_number}: {exc}")
                continue
            if isinstance(row, dict):
                rows.append(row)
            else:
                errors.append(f"line {line_number}: not an object")
    except OSError as exc:
        errors.append(str(exc))
    denies = [row for row in rows if row.get("decision") == "deny"]
    bootstraps = [row for row in rows if row.get("event") == "audit.bootstrap"]
    sockets = [row for row in rows if str(row.get("event") or "").startswith("socket.")]
    return {
        "path": str(path),
        "sha256": _sha256_file(path) if path.is_file() else None,
        "rows": len(rows),
        "errors": errors,
        "deny_count": len(denies),
        "denials": denies,
        "bootstrap_count": len(bootstraps),
        "socket_observation_count": len(sockets),
        "ok": not errors and len(bootstraps) == 1 and not denies,
    }


def _sanitize_command(command: list[str]) -> list[str]:
    output = []
    assignment_count = 0
    for item in command:
        if "=" in item and not item.startswith("--") and output[:2] == [str(ENV_EXEC), "-i"]:
            assignment_count += 1
            continue
        if assignment_count:
            output.append(f"<sanitized-env-assignments:{assignment_count}>")
            assignment_count = 0
        output.append(item)
    if assignment_count:
        output.append(f"<sanitized-env-assignments:{assignment_count}>")
    return output


def _artifact_manifest(run_root: Path, receipt_path: Path) -> dict[str, Any]:
    manifest_path = run_root / "artifact-manifest.json"
    excluded = {receipt_path.resolve(strict=False), manifest_path.resolve(strict=False)}
    rows = []
    violations = []
    for path in sorted(run_root.rglob("*")):
        if path.resolve(strict=False) in excluded or path.is_dir():
            continue
        try:
            before = path.lstat()
            if not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid() or before.st_nlink != 1:
                violations.append(f"unsafe artifact: {path}")
                continue
            digest = _sha256_file(path)
            after = path.lstat()
            if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_mtime_ns,
            ):
                violations.append(f"artifact changed while hashing: {path}")
                continue
            rows.append(
                {
                    "path": str(path.relative_to(run_root)),
                    "size": before.st_size,
                    "mode": oct(stat.S_IMODE(before.st_mode)),
                    "sha256": digest,
                }
            )
        except OSError as exc:
            violations.append(f"{path}: {exc}")
    manifest = {
        "schema_version": "hermes_l4_artifact_manifest_v1",
        "run_root": str(run_root),
        "excluded_self_referential_paths": [
            str(receipt_path.relative_to(run_root)),
            str(manifest_path.relative_to(run_root)),
        ],
        "files": rows,
        "file_count": len(rows),
        "violations": violations,
        "ok": not violations and bool(rows),
    }
    _atomic_json(manifest_path, manifest)
    return {
        "status": "PASS" if manifest["ok"] else "FAIL",
        "ok": manifest["ok"],
        "path": str(manifest_path),
        "sha256": _sha256_file(manifest_path),
        "file_count": len(rows),
        "violations": violations,
        "exclusions": manifest["excluded_self_referential_paths"],
    }


def _worker_dispatch(args: argparse.Namespace) -> dict[str, Any] | None:
    result_path = Path(args.worker_result)
    _worker_preflight(result_path)
    if args.worker == "combined":
        return _combined_worker()
    if args.worker == "crash-inject":
        _crash_inject_worker(args.boundary)
        return None
    if args.worker == "crash-restart":
        return _crash_restart_worker(args.boundary)
    if args.worker == "concurrency-setup":
        return _concurrency_setup_worker()
    if args.worker == "lock-holder":
        return _concurrency_lock_holder(Path(args.ready_path), Path(args.release_path))
    if args.worker == "lock-contender":
        return _lock_contender_worker()
    if args.worker == "concurrent-sync":
        return _concurrent_sync_worker(Path(args.start_path))
    if args.worker == "concurrent-relay":
        return _concurrent_relay_worker(Path(args.start_path))
    if args.worker == "concurrent-card":
        return _concurrent_card_worker(Path(args.start_path))
    if args.worker == "concurrency-restart-check":
        return _concurrency_restart_check_worker()
    raise ValueError(f"unknown worker: {args.worker}")


def _run_worker_main(args: argparse.Namespace) -> int:
    result_path = Path(args.worker_result)
    try:
        result = _worker_dispatch(args)
    except Exception as exc:
        result = {
            "schema_version": WORKER_SCHEMA_VERSION,
            "worker": args.worker,
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "traceback": traceback.format_exc(limit=20),
        }
    if result is None:
        return 3
    _atomic_json(result_path, result)
    return 0 if result.get("ok") is True else 2


def _driver(args: argparse.Namespace) -> int:
    source_root = REPO_ROOT.resolve(strict=True)
    run_root = _prepare_run_root(Path(args.run_root), source_root=source_root)
    receipt_path = run_root / "receipt.json"
    census = _copy_census(Path(args.census_root), run_root / "outbound-census")
    python_executable = Path(sys.executable).resolve(strict=True)
    python_exec_allowlist = _python_exec_allowlist(python_executable)
    if not (SANDBOX_EXEC.is_file() and ENV_EXEC.is_file() and AUDIT_BOOTSTRAP.is_dir()):
        raise RuntimeError("required sandbox/env/audit bootstrap entry is unavailable")
    profile_path = run_root / "l4-default-deny.sb"
    profile_text = _sandbox_profile(
        run_root=run_root,
        source_root=source_root,
        python_executables=python_exec_allowlist,
    )
    profile_path.write_text(profile_text, encoding="utf-8")
    profile_path.chmod(0o600)
    profile_sha = _sha256_file(profile_path)
    source = _source_binding(args.candidate_commit or "", args.candidate_tree or "")
    focused = _focused_tests_gate(
        Path(args.focused_tests_receipt).resolve(strict=True)
        if args.focused_tests_receipt
        else None,
        source,
    )
    gates: dict[str, dict[str, Any]] = {
        gate: {"status": "NOT_RUN", "ok": False} for gate in REQUIRED_GATES
    }
    gates["source_binding"] = source
    gates["focused_tests"] = focused
    worker_reports: list[dict[str, Any]] = []
    crash_results: dict[str, Any] = {}
    concurrency_result: dict[str, Any] = {}

    if args.execute:
        combined_phase = _prepare_phase(run_root, "combined")
        combined = _spawn_worker(
            run_root=run_root,
            profile_path=profile_path,
            python_executable=python_executable,
            phase_paths=combined_phase,
            name="combined",
            worker_args=["combined"],
            timeout_seconds=args.timeout_seconds,
        )
        combined_report = _wait_worker(combined)
        worker_reports.append(combined_report)
        combined_ok = (
            not combined_report["timed_out"]
            and combined_report["returncode"] == 0
            and isinstance(combined_report.get("result"), dict)
            and combined_report["result"].get("ok") is True
        )
        gates["s1_s10_combined"] = {
            "status": "PASS" if combined_ok else "FAIL",
            "ok": combined_ok,
            "worker": combined_report,
        }

        for boundary in CRASH_BOUNDARIES:
            phase = _prepare_phase(run_root, f"crash-{boundary}")
            inject = _spawn_worker(
                run_root=run_root,
                profile_path=profile_path,
                python_executable=python_executable,
                phase_paths=phase,
                name=f"crash-{boundary}-inject",
                worker_args=["crash-inject", "--boundary", boundary],
                timeout_seconds=args.timeout_seconds,
            )
            inject_report = _wait_worker(inject)
            worker_reports.append(inject_report)
            try:
                inject_stderr = Path(inject_report["stderr_path"]).read_text(
                    encoding="utf-8", errors="replace"
                )
            except OSError:
                inject_stderr = ""
            terminated_at_boundary = (
                not inject_report["timed_out"]
                and inject_report["returncode"] not in {0, None}
                and f"L4_PROCESS_TERMINATION:{boundary}:" in inject_stderr
            )
            restart = _spawn_worker(
                run_root=run_root,
                profile_path=profile_path,
                python_executable=python_executable,
                phase_paths=phase,
                name=f"crash-{boundary}-restart",
                worker_args=["crash-restart", "--boundary", boundary],
                timeout_seconds=args.timeout_seconds,
            )
            restart_report = _wait_worker(restart)
            worker_reports.append(restart_report)
            restart_ok = (
                restart_report["returncode"] == 0
                and isinstance(restart_report.get("result"), dict)
                and restart_report["result"].get("ok") is True
            )
            crash_results[boundary] = {
                "ok": terminated_at_boundary and restart_ok,
                "terminated_at_boundary": terminated_at_boundary,
                "inject": inject_report,
                "restart": restart_report,
            }
        crash_ok = all(result["ok"] for result in crash_results.values())
        gates["business_crash_matrix"] = {
            "status": "PASS" if crash_ok else "FAIL",
            "ok": crash_ok,
            "boundaries": crash_results,
        }

        concurrency_phase = _prepare_phase(run_root, "concurrency")
        setup = _spawn_worker(
            run_root=run_root,
            profile_path=profile_path,
            python_executable=python_executable,
            phase_paths=concurrency_phase,
            name="concurrency-setup",
            worker_args=["concurrency-setup"],
            timeout_seconds=args.timeout_seconds,
        )
        setup_report = _wait_worker(setup)
        worker_reports.append(setup_report)
        setup_ok = setup_report["returncode"] == 0 and bool((setup_report.get("result") or {}).get("ok"))

        barrier_root = run_root / "barriers"
        barrier_root.mkdir(mode=0o700, exist_ok=True)
        lock_ready = barrier_root / "lock-ready.json"
        lock_release = barrier_root / "lock-release.json"
        holder = _spawn_worker(
            run_root=run_root,
            profile_path=profile_path,
            python_executable=python_executable,
            phase_paths=concurrency_phase,
            name="lock-holder",
            worker_args=[
                "lock-holder",
                "--ready-path",
                str(lock_ready),
                "--release-path",
                str(lock_release),
            ],
            timeout_seconds=args.timeout_seconds,
        )
        holder_ready = _wait_for_path(lock_ready, timeout_seconds=10)
        contender = _spawn_worker(
            run_root=run_root,
            profile_path=profile_path,
            python_executable=python_executable,
            phase_paths=concurrency_phase,
            name="lock-contender",
            worker_args=["lock-contender"],
            timeout_seconds=args.timeout_seconds,
        )
        contender_report = _wait_worker(contender)
        worker_reports.append(contender_report)
        _atomic_json(lock_release, {"release": True, "observed_ready": holder_ready})
        holder_report = _wait_worker(holder)
        worker_reports.append(holder_report)
        lock_ok = (
            holder_ready
            and holder_report["returncode"] == 0
            and bool((holder_report.get("result") or {}).get("ok"))
            and contender_report["returncode"] == 0
            and bool((contender_report.get("result") or {}).get("ok"))
        )

        concurrent_start = barrier_root / "concurrent-start.json"
        concurrent_workers = [
            _spawn_worker(
                run_root=run_root,
                profile_path=profile_path,
                python_executable=python_executable,
                phase_paths=concurrency_phase,
                name=f"concurrent-{role}",
                worker_args=[f"concurrent-{role}", "--start-path", str(concurrent_start)],
                timeout_seconds=args.timeout_seconds,
            )
            for role in ("sync", "relay", "card")
        ]
        _atomic_json(concurrent_start, {"start": True})
        concurrent_reports = [_wait_worker(worker) for worker in concurrent_workers]
        worker_reports.extend(concurrent_reports)
        concurrent_ok = all(
            report["returncode"] == 0
            and isinstance(report.get("result"), dict)
            and report["result"].get("ok") is True
            for report in concurrent_reports
        )
        restart = _spawn_worker(
            run_root=run_root,
            profile_path=profile_path,
            python_executable=python_executable,
            phase_paths=concurrency_phase,
            name="concurrency-restart-check",
            worker_args=["concurrency-restart-check"],
            timeout_seconds=args.timeout_seconds,
        )
        restart_report = _wait_worker(restart)
        worker_reports.append(restart_report)
        restart_ok = (
            restart_report["returncode"] == 0
            and isinstance(restart_report.get("result"), dict)
            and restart_report["result"].get("ok") is True
        )
        concurrency_ok = setup_ok and lock_ok and concurrent_ok and restart_ok
        concurrency_result = {
            "setup": setup_report,
            "lock_holder_ready": holder_ready,
            "lock_holder": holder_report,
            "lock_contender": contender_report,
            "concurrent_workers": concurrent_reports,
            "restart": restart_report,
            "ok": concurrency_ok,
        }
        gates["concurrency_restart"] = {
            "status": "PASS" if concurrency_ok else "FAIL",
            "ok": concurrency_ok,
            **concurrency_result,
        }

    audit_results = [_read_audit(Path(report["audit_path"])) for report in worker_reports]
    audit_ok = bool(audit_results) and all(result["ok"] for result in audit_results)
    gates["python_audit"] = {
        "status": "PASS" if audit_ok else ("NOT_RUN" if not args.execute else "FAIL"),
        "ok": audit_ok,
        "logs": audit_results,
        "limitations": [
            "Python audit hooks do not fully observe dir_fd target resolution",
            "native syscalls and descriptor-backed mmap require the OS sandbox",
            "zero audit denials alone is not containment evidence",
        ],
    }
    profile_rules_ok = all(
        fragment in profile_text
        for fragment in (
            "(deny default)",
            "(deny network*)",
            f"(allow file-write* (subpath {_sandbox_quote(run_root)})",
            f"(deny file-write* (subpath {_sandbox_quote(source_root)})",
        )
    )
    sandbox_invocations = bool(worker_reports) and all(
        str(SANDBOX_EXEC) in report["command"]
        and str(profile_path) in report["command"]
        and str(ENV_EXEC) == report["command"][0]
        and report["command"][1] == "-i"
        for report in worker_reports
    )
    sandbox_start_errors = []
    for report, audit_result in zip(worker_reports, audit_results, strict=True):
        try:
            stderr = Path(report["stderr_path"]).read_text(encoding="utf-8", errors="replace")
        except OSError:
            stderr = ""
        startup_marker = any(
            marker in stderr.lower()
            for marker in (
                "sandbox_apply",
                "invalid sandbox profile",
                "posix_spawn:",
            )
        )
        if startup_marker or audit_result.get("bootstrap_count") != 1:
            sandbox_start_errors.append(
                {
                    "worker": report["name"],
                    "audit_bootstrap_count": audit_result.get("bootstrap_count"),
                    "stderr": stderr[:500],
                }
            )
    sandbox_ok = args.execute and profile_rules_ok and sandbox_invocations and not sandbox_start_errors
    gates["os_sandbox"] = {
        "status": "PASS" if sandbox_ok else ("NOT_RUN" if not args.execute else "FAIL"),
        "ok": sandbox_ok,
        "profile": str(profile_path),
        "profile_sha256": profile_sha,
        "deny_default": "(deny default)" in profile_text,
        "deny_network": "(deny network*)" in profile_text,
        "source_read_only": f"(deny file-write* (subpath {_sandbox_quote(source_root)})" in profile_text,
        "write_root_only": f"(allow file-write* (subpath {_sandbox_quote(run_root)})" in profile_text,
        "startup_errors": sandbox_start_errors,
    }
    environment_keys = []
    if args.execute:
        sample_phase = combined_phase
        sample_audit = run_root / "audit" / "sample.jsonl"
        environment_keys = sorted(
            _worker_environment(
                run_root=run_root,
                phase_paths=sample_phase,
                audit_path=sample_audit,
            )
        )
    environment_ok = args.execute and not any(SECRET_ENV_RE.search(key) for key in environment_keys)
    containment_ok = sandbox_ok and audit_ok and environment_ok
    gates["whole_process_containment"] = {
        "status": "PASS" if containment_ok else ("NOT_RUN" if not args.execute else "FAIL"),
        "ok": containment_ok,
        "requires_os_sandbox_and_python_audit": True,
        "os_sandbox_ok": sandbox_ok,
        "python_audit_ok": audit_ok,
        "sanitized_environment_ok": environment_ok,
        "sanitized_environment_key_names": environment_keys,
        "network_attempts_are_failures": True,
        "protected_root_attempts_are_failures": True,
        "writes_allowed_only_below": str(run_root),
    }
    reaped = bool(worker_reports) and all(report["reaped"] and not report["timed_out"] for report in worker_reports)
    gates["process_cleanup"] = {
        "status": "PASS" if reaped else ("NOT_RUN" if not args.execute else "FAIL"),
        "ok": reaped,
        "all_children_reaped": reaped,
        "worker_count": len(worker_reports),
        "timeouts": [report["name"] for report in worker_reports if report["timed_out"]],
        "listeners_expected": 0,
        "subprocesses_allowed_inside_workers": 0,
    }
    combined_result = (
        gates.get("s1_s10_combined", {}).get("worker", {}).get("result")
        if args.execute
        else None
    )
    crash_ledgers_ok = args.execute and all(
        bool((entry.get("restart", {}).get("result") or {}).get("ok"))
        for entry in crash_results.values()
    )
    concurrency_ledger_ok = bool(
        (concurrency_result.get("restart", {}).get("result") or {}).get("ledger")
    ) if args.execute else False
    ledger_ok = (
        args.execute
        and isinstance(combined_result, dict)
        and combined_result.get("ok") is True
        and bool(combined_result.get("ledger"))
        and crash_ledgers_ok
        and concurrency_ledger_ok
    )
    gates["ledger_integrity"] = {
        "status": "PASS" if ledger_ok else ("NOT_RUN" if not args.execute else "FAIL"),
        "ok": ledger_ok,
        "combined": combined_result.get("ledger") if isinstance(combined_result, dict) else None,
        "crash_restart_ledgers_verified": crash_ledgers_ok,
        "concurrency_restart_ledger": (
            concurrency_result.get("restart", {}).get("result") or {}
        ).get("ledger") if args.execute else None,
    }

    for report in worker_reports:
        report["command"] = _sanitize_command(report["command"])
    manifest_gate = _artifact_manifest(run_root, receipt_path)
    gates["artifact_manifest"] = manifest_gate
    phase2_gate_names = (
        "s1_s10_combined",
        "business_crash_matrix",
        "concurrency_restart",
        "os_sandbox",
        "python_audit",
        "whole_process_containment",
        "ledger_integrity",
        "process_cleanup",
        "artifact_manifest",
    )
    phase2_pass = all(gates[name].get("ok") is True for name in phase2_gate_names)
    l4_pass = all(gates[name].get("ok") is True for name in REQUIRED_GATES)
    receipt = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_epoch": time.time(),
        "run_root": str(run_root),
        "execution_requested": bool(args.execute),
        "l4_decision": "PASS" if l4_pass else "NO_GO",
        "phase2_decision": "PASS" if phase2_pass else "NO_GO",
        "candidate_execution_authorized": False,
        "promotion_authorized": False,
        "cutover_authorized": False,
        "zero_business_impact_claimed": False,
        "external_delivery_attempted": False,
        "external_delivery_verified": False,
        "source_binding": source,
        "census": census,
        "python": {
            "executable": str(python_executable),
            "sha256": _sha256_file(python_executable),
            "process_exec_allowlist": [
                {"path": str(path), "sha256": _sha256_file(path)}
                for path in python_exec_allowlist
            ],
        },
        "command_allowlist": {
            "env": {"path": str(ENV_EXEC), "sha256": _sha256_file(ENV_EXEC)},
            "sandbox_exec": {"path": str(SANDBOX_EXEC), "sha256": _sha256_file(SANDBOX_EXEC)},
            "python": {"path": str(python_executable), "sha256": _sha256_file(python_executable)},
            "worker_script": {"path": str(Path(__file__).resolve()), "sha256": _sha256_file(Path(__file__).resolve())},
            "worker_subprocess_allowlist": [],
        },
        "gates": gates,
        "workers": worker_reports,
        "blockers": [name for name in REQUIRED_GATES if gates[name].get("ok") is not True],
        "residual_risks": [
            "TaskStore, sidecar, and watchdog path pre/post checks are defense in depth; the OS sandbox is the containment boundary",
            "record-only intent replay is at-least-once and does not claim external exactly-once delivery",
            "L4 PASS does not authorize promotion, candidate execution, cutover, or a zero-business-impact claim",
        ],
    }
    _atomic_json(receipt_path, receipt)
    print(
        json.dumps(
            {
                "receipt": str(receipt_path),
                "l4_decision": receipt["l4_decision"],
                "phase2_decision": receipt["phase2_decision"],
                "blockers": receipt["blockers"],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0 if l4_pass else 2


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-root")
    parser.add_argument("--census-root")
    parser.add_argument("--candidate-commit", default="")
    parser.add_argument("--candidate-tree", default="")
    parser.add_argument("--focused-tests-receipt")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--timeout-seconds", type=int, default=90)
    parser.add_argument(
        "--worker",
        choices=(
            "combined",
            "crash-inject",
            "crash-restart",
            "concurrency-setup",
            "lock-holder",
            "lock-contender",
            "concurrent-sync",
            "concurrent-relay",
            "concurrent-card",
            "concurrency-restart-check",
        ),
        help=argparse.SUPPRESS,
    )
    parser.add_argument("--worker-result", help=argparse.SUPPRESS)
    parser.add_argument("--boundary", choices=CRASH_BOUNDARIES, help=argparse.SUPPRESS)
    parser.add_argument("--ready-path", help=argparse.SUPPRESS)
    parser.add_argument("--release-path", help=argparse.SUPPRESS)
    parser.add_argument("--start-path", help=argparse.SUPPRESS)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    args.timeout_seconds = max(10, min(int(args.timeout_seconds), 300))
    if args.worker:
        if not args.worker_result:
            raise SystemExit("worker mode requires --worker-result")
        return _run_worker_main(args)
    if not args.run_root or not args.census_root:
        raise SystemExit("driver mode requires --run-root and --census-root")
    return _driver(args)


if __name__ == "__main__":
    raise SystemExit(main())
