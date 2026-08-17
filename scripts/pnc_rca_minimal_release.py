#!/usr/bin/env python3
"""Plan, apply, or verify one minimal GitLab-backed RCA Host release."""

from __future__ import annotations

import argparse
import base64
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import signal
import stat
import subprocess
import sys
import tempfile
import time
from typing import Any

import psutil

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gateway.pnc_rca_control_store import ActivationEpochError, RcaControlStore
from gateway.pnc_rca_delivery_store import RcaDeliveryStore
from gateway.pnc_rca_runtime_identity import runtime_identity_is_valid
from gateway.pnc_rca_write_fence import (
    MINIMAL_RELEASE_HOST_REMOTE,
    MINIMAL_RELEASE_NOTE_SCHEMA_VERSION,
    MINIMAL_RELEASE_PRODUCTION_DEFINITION,
    MinimalReleaseNoteIdentityError,
    validate_minimal_release_note_identity,
)


SCHEMA = "pnc_rca_minimal_release_driver_v1"
NOTE_SCHEMA = MINIMAL_RELEASE_NOTE_SCHEMA_VERSION
RECEIPT_SCHEMA = "pnc_rca_minimal_release_apply_receipt_v1"
TERMINAL_FAILURE_SCHEMA = "pnc_rca_minimal_release_terminal_failure_v1"
EXECUTION_READBACK_SCHEMA = "pnc_rca_execution_identity_readback_v1"
PRODUCTION_DEFINITION = MINIMAL_RELEASE_PRODUCTION_DEFINITION
HOST_REMOTE = MINIMAL_RELEASE_HOST_REMOTE
HEX40 = re.compile(r"^[0-9a-f]{40}$")
HEX64 = re.compile(r"^[0-9a-f]{64}$")
IDENTIFIER = re.compile(r"^[a-z0-9][a-z0-9._-]{5,127}$")
MAX_FILE_BYTES = 4 * 1024 * 1024
MAX_HEALTH_AGE_SECONDS = 120
RELEASE_LOCK_NAME = ".pnc-rca-minimal-release.lock"
SSH_MINI_AGENT = "/Users/songying/.local/bin/ssh-mini-agent"
REPORT_MANIFEST_ROOT = PurePosixPath("/home/mini/.config/g1q3-rca")
REPORT_MANIFEST_PATH = REPORT_MANIFEST_ROOT / "report-runtime-manifest.json"
CONTROL_DB_RELATIVE_PATH = Path(
    "runtime/pnc_agent/feishu_issue_kafka_rca/control.sqlite3"
)
CONTROL_DB_ENV_KEYS = (
    "HERMES_RCA_KAFKA_CONTROL_DB_PATH",
    "HERMES_RCA_OUTBOX_CONTROL_DB_PATH",
    "HERMES_RCA_OUTBOX_DELIVERY_DB_PATH",
    "HERMES_RCA_DELIVERY_COLLECTOR_CONTROL_DB_PATH",
    "HERMES_RCA_DELIVERY_DISPATCHER_CONTROL_DB_PATH",
    "HERMES_RCA_CONTROL_DB_PATH",
)

# label, release-relative script, HERMES_HOME-relative health, freshness field,
# health schema. Gateway status has no schema and updated_at is a transition,
# not a continuously advancing heartbeat.
REQUIRED_RESIDENTS = (
    ("ai.hermes.gateway", "hermes_cli/main.py", "gateway_state.json", "updated_at", ""),
    (
        "local.pnc.rca-outbox-dispatcher",
        "scripts/pnc_rca_outbox_dispatcher.py",
        "runtime/pnc_agent/feishu_issue_kafka_rca/outbox_dispatcher_health.json",
        "heartbeat_at",
        "pnc_rca_outbox_dispatcher_health_v2",
    ),
    (
        "local.pnc.rca-delivery-collector",
        "scripts/pnc_rca_delivery_collector.py",
        "runtime/pnc_agent/feishu_issue_kafka_rca/delivery_collector_health.json",
        "updated_at",
        "pnc_rca_delivery_collector_health_v2",
    ),
    (
        "local.pnc.rca-delivery-dispatcher",
        "scripts/pnc_rca_delivery_dispatcher.py",
        "runtime/pnc_agent/feishu_issue_kafka_rca/delivery_dispatcher_health.json",
        "updated_at",
        "pnc_rca_delivery_dispatcher_health_v2",
    ),
)
DISABLED_RESIDENTS = (
    "local.pnc.rca-kafka-consumer",
    "local.pnc.completion-notice-relay",
)
RELEASE_HEALTH_RESIDENTS = {
    "local.pnc.rca-delivery-collector",
    "local.pnc.rca-delivery-dispatcher",
}
LEGACY_NOTE_MARKERS = (
    "authority",
    "baseline",
    "capsule",
    "facade",
    "pointer",
    "scorecard",
    "transaction",
)
LEGACY_ENV = {
    "HERMES_RCA_DELIVERY_QUARANTINE_BASELINE_PATH",
    "HERMES_RCA_DELIVERY_QUARANTINE_BASELINE_SHA256",
    "HERMES_RCA_PROD_BOOTSTRAP_EPOCH_ID",
    "HERMES_RCA_PROD_CAPACITY_MODE",
    "HERMES_RCA_PROD_RELEASE_ID",
    "HERMES_RCA_OUTBOX_STORAGE_RESERVATION_ENABLED",
    "HERMES_RCA_RELEASE_AUTHORITY_PATH",
    "HERMES_RCA_RELEASE_POINTER_PATH",
}

Runner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]
VmRunner = Callable[[Sequence[str], str | None], subprocess.CompletedProcess[str]]
ReportReader = Callable[[Path], tuple[bytes, dict]]
ProcessFactory = Callable[[int], Any]
StoreFactory = Callable[[Path, bool], Any]
DeliveryStoreFactory = Callable[[Path], Any]


class ReleaseError(RuntimeError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode()
    except (TypeError, ValueError) as exc:
        raise ReleaseError("json_value_invalid") from exc


def _absolute(value: str | Path, code: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute() or ".." in path.parts:
        raise ReleaseError(code)
    return path


def _identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
    )


def _read(path: Path, code: str, *, owner_only: bool = False) -> bytes:
    path = _absolute(path, f"{code}_path_invalid")
    descriptor = -1
    try:
        before = path.lstat()
        mode = stat.S_IMODE(before.st_mode)
        if (
            stat.S_ISLNK(before.st_mode)
            or not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
            or mode & 0o022
            or (owner_only and mode & 0o077)
            or not 0 < before.st_size <= MAX_FILE_BYTES
        ):
            raise ReleaseError(f"{code}_identity_invalid")
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        opened = os.fstat(descriptor)
        if _identity(opened) != _identity(before):
            raise ReleaseError(f"{code}_changed")
        chunks: list[bytes] = []
        remaining = opened.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise ReleaseError(f"{code}_changed")
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        if os.read(descriptor, 1) or _identity(os.fstat(descriptor)) != _identity(
            before
        ):
            raise ReleaseError(f"{code}_changed")
        if _identity(path.lstat()) != _identity(before):
            raise ReleaseError(f"{code}_changed")
        return raw
    except ReleaseError:
        raise
    except OSError as exc:
        raise ReleaseError(f"{code}_unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _decode_json(raw: bytes, code: str) -> dict:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise ReleaseError(f"{code}_invalid")
            result[key] = value
        return result

    try:
        value = json.loads(
            raw,
            object_pairs_hook=pairs,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                ReleaseError(f"{code}_invalid")
            ),
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReleaseError(f"{code}_invalid") from exc
    if not isinstance(value, dict):
        raise ReleaseError(f"{code}_invalid")
    return value


def _json(path: Path, code: str, *, owner_only: bool = False) -> tuple[bytes, dict]:
    raw = _read(path, code, owner_only=owner_only)
    return raw, _decode_json(raw, code)


def _run(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    for key in tuple(env):
        if key.startswith("GIT_CONFIG_") or key in {
            "GIT_CONFIG",
            "GIT_CONFIG_PARAMETERS",
            "GIT_DIR",
            "GIT_WORK_TREE",
            "GIT_COMMON_DIR",
            "GIT_OBJECT_DIRECTORY",
            "GIT_ALTERNATE_OBJECT_DIRECTORIES",
            "GIT_EXEC_PATH",
            "GIT_TEMPLATE_DIR",
            "GIT_SSH",
            "GIT_SSH_COMMAND",
            "GIT_SSH_VARIANT",
            "GIT_PROXY_COMMAND",
        }:
            env.pop(key, None)
    env.update({
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_SYSTEM": "/dev/null",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_COUNT": "0",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_SSH": "/usr/bin/ssh",
        "GIT_SSH_VARIANT": "ssh",
    })
    return subprocess.run(
        list(command), capture_output=True, text=True, timeout=60, env=env, check=False
    )


def _run_vm_agent(
    command: Sequence[str], input_text: str | None = None
) -> subprocess.CompletedProcess[str]:
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("SSH_MINI_") and key != "HOME"
    }
    env.update({
        "HOME": "/Users/songying",
        "PATH": "/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin",
    })
    return subprocess.run(
        list(command),
        input=input_text,
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
        check=False,
    )


def _call(runner: Runner, command: Sequence[str], code: str) -> str:
    try:
        result = runner(tuple(command))
    except (OSError, subprocess.SubprocessError) as exc:
        raise ReleaseError(f"{code}_unavailable") from exc
    if result.returncode:
        raise ReleaseError(f"{code}_failed")
    return result.stdout


def _has_legacy_key(value: object) -> bool:
    if isinstance(value, Mapping):
        return any(
            any(marker in str(key).lower() for marker in LEGACY_NOTE_MARKERS)
            or _has_legacy_key(child)
            for key, child in value.items()
        )
    return isinstance(value, list) and any(_has_legacy_key(item) for item in value)


def _load_note(path: Path, home: Path) -> tuple[bytes, dict]:
    raw, note = _json(path, "release_note", owner_only=True)
    if _has_legacy_key(note):
        raise ReleaseError("release_note_contract_invalid")
    try:
        validated_identity = validate_minimal_release_note_identity(note)
    except MinimalReleaseNoteIdentityError as exc:
        code = {
            "minimal_release_note_host_invalid": "release_note_host_invalid",
            "minimal_release_note_identity_invalid": "release_note_identity_invalid",
        }.get(exc.code, "release_note_contract_invalid")
        raise ReleaseError(code) from exc
    identity = validated_identity["release_identity"]
    host = validated_identity["host"]
    root = _absolute(str(host.get("runtime_root") or ""), "runtime_root_invalid")
    if (
        root.parent != home / "runtime/releases"
        or not HEX64.fullmatch(str(note.get("release_fingerprint_sha256") or ""))
        or note["release_fingerprint_sha256"] != _sha(_canonical(identity))
    ):
        raise ReleaseError("release_note_host_invalid")
    worker = validated_identity["worker"]
    pipeline = validated_identity["pipeline"]
    report = identity.get("report_service")
    _absolute(str(worker.get("runtime_root") or ""), "release_note_identity_invalid")
    _absolute(str(pipeline.get("runtime_root") or ""), "release_note_identity_invalid")
    if (
        not isinstance(report, Mapping)
        or not HEX64.fullmatch(str(report.get("manifest_sha256") or ""))
        or report.get("pipeline_commit") != pipeline.get("commit")
        or report.get("pipeline_tree") != pipeline.get("tree")
    ):
        raise ReleaseError("release_note_identity_invalid")
    _absolute(str(report.get("manifest_path") or ""), "release_note_identity_invalid")
    projection = note.get("runtime_projection")
    if not isinstance(projection, Mapping) or not all(
        HEX64.fullmatch(str(projection.get(key) or ""))
        for key in ("env_sha256", "live_manifest_sha256")
    ):
        raise ReleaseError("release_note_projection_invalid")
    activation = note.get("activation")
    activation_fields = {
        "epoch_id",
        "control_db_path",
        "operator",
        "reason",
        "expected_predecessor_epoch_id",
        "expected_predecessor_state",
        "expected_predecessor_binding_fingerprint",
        "db_logical_identity",
        "db_logical_identity_sha256",
        "partition_start_fence",
        "partition_start_fence_sha256",
    }
    if (
        not isinstance(activation, Mapping)
        or set(activation) != activation_fields
        or not IDENTIFIER.fullmatch(str(activation.get("epoch_id") or ""))
    ):
        raise ReleaseError("release_note_activation_invalid")
    control_db = _absolute(
        str(activation.get("control_db_path") or ""),
        "release_note_activation_invalid",
    )
    if control_db != home / CONTROL_DB_RELATIVE_PATH:
        raise ReleaseError("release_note_control_db_invalid")
    if (
        not str(activation.get("operator") or "").strip()
        or not str(activation.get("reason") or "").strip()
    ):
        raise ReleaseError("release_note_activation_invalid")
    predecessor = str(activation.get("expected_predecessor_epoch_id") or "")
    predecessor_state = str(activation.get("expected_predecessor_state") or "")
    predecessor_hash = str(
        activation.get("expected_predecessor_binding_fingerprint") or ""
    )
    if bool(predecessor) != bool(predecessor_state) or bool(predecessor) != bool(
        predecessor_hash
    ):
        raise ReleaseError("release_note_predecessor_invalid")
    if predecessor and (
        not IDENTIFIER.fullmatch(predecessor)
        or predecessor_state not in {"aborted", "steady_active"}
        or not HEX64.fullmatch(predecessor_hash)
    ):
        raise ReleaseError("release_note_predecessor_invalid")
    _bound_binding(raw, note)
    profile = note.get("resident_profile")
    if profile != {
        "name": "operator_issue_only_v1",
        "required": [row[0] for row in REQUIRED_RESIDENTS],
        "disabled": list(DISABLED_RESIDENTS),
    }:
        raise ReleaseError("release_note_profile_invalid")
    canary = note.get("canary")
    if (
        not isinstance(canary, Mapping)
        or not IDENTIFIER.fullmatch(str(canary.get("batch_id") or ""))
        or not str(canary.get("issue_id") or "").isdigit()
    ):
        raise ReleaseError("release_note_canary_invalid")
    _absolute(str(canary.get("state_path") or ""), "release_note_canary_invalid")
    return raw, note


def _gitlab_resolve_faces(
    faces: Mapping[str, Mapping[str, str]], runner: Runner = _run
) -> dict[str, dict[str, str]]:
    if set(faces) != {"host", "worker", "pipeline"}:
        raise ReleaseError("gitlab_face_set_invalid")
    result: dict[str, dict[str, str]] = {}
    with tempfile.TemporaryDirectory(prefix="pnc-rca-gitlab-readback-") as temp:
        repository = str(Path(temp) / "readback.git")
        _call(
            runner,
            ("/usr/bin/git", "init", "--bare", repository),
            "gitlab_tree_readback",
        )
        for name in ("host", "worker", "pipeline"):
            face = faces[name]
            remote = str(face.get("remote") or "")
            branch = str(face.get("remote_branch") or "")
            remote_tag = str(face.get("remote_tag") or "")
            if (
                not remote.startswith("git@git.minieye.tech:")
                or (name == "host" and remote != HOST_REMOTE)
                or re.fullmatch(r"refs/heads/[A-Za-z0-9._/-]+", branch) is None
                or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,127}", remote_tag)
                is None
            ):
                raise ReleaseError("gitlab_face_input_invalid")
            tag = f"refs/tags/{remote_tag}"
            local_root = f"refs/pnc-rca-readback/{name}"
            local_branch = f"{local_root}/branch"
            local_tag = f"{local_root}/tag"
            _call(
                runner,
                (
                    "/usr/bin/git",
                    "-C",
                    repository,
                    "fetch",
                    "--quiet",
                    "--depth=1",
                    "--filter=blob:none",
                    "--no-tags",
                    remote,
                    f"+{branch}:{local_branch}",
                    f"+{tag}:{local_tag}",
                ),
                "gitlab_readback",
            )
            tag_type = _call(
                runner,
                ("/usr/bin/git", "-C", repository, "cat-file", "-t", local_tag),
                "gitlab_readback",
            ).strip()
            values = _call(
                runner,
                (
                    "/usr/bin/git",
                    "-C",
                    repository,
                    "rev-parse",
                    f"{local_branch}^{{commit}}",
                    f"{local_branch}^{{tree}}",
                    local_tag,
                    f"{local_tag}^{{commit}}",
                ),
                "gitlab_readback",
            ).splitlines()
            if len(values) != 4:
                raise ReleaseError("gitlab_identity_mismatch")
            commit, tree, tag_object, peeled = values
            if (
                tag_type != "tag"
                or peeled != commit
                or not all(
                    HEX40.fullmatch(value) for value in (commit, tree, tag_object)
                )
            ):
                raise ReleaseError("gitlab_identity_mismatch")
            result[name] = {
                "remote": remote,
                "remote_branch": branch,
                "remote_tag": remote_tag,
                "remote_tag_object": tag_object,
                "commit": commit,
                "tree": tree,
            }
    return result


def gitlab_readback(note: Mapping[str, Any], runner: Runner = _run) -> dict:
    expected = note["release_identity"]
    resolved = _gitlab_resolve_faces(
        {name: expected[name] for name in ("host", "worker", "pipeline")}, runner
    )
    result = {}
    for name in ("host", "worker", "pipeline"):
        face = expected[name]
        actual = resolved[name]
        if any(
            actual[key] != face[key]
            for key in (
                "remote",
                "remote_branch",
                "remote_tag",
                "remote_tag_object",
                "commit",
            )
        ):
            raise ReleaseError("gitlab_identity_mismatch")
        if actual["tree"] != face["tree"]:
            raise ReleaseError("gitlab_tree_identity_mismatch")
        tag = f"refs/tags/{face['remote_tag']}"
        refs = {
            face["remote_branch"]: actual["commit"],
            tag: actual["remote_tag_object"],
            f"{tag}^{{}}": actual["commit"],
        }
        result[name] = {
            "remote": face["remote"],
            "refs": refs,
            "commit": actual["commit"],
            "tree": actual["tree"],
            "tree_source": "gitlab_fetched_commit",
        }
    return result


def runtime_readback(note: Mapping[str, Any], runner: Runner = _run) -> dict:
    host = note["release_identity"]["host"]
    root = host["runtime_root"]
    values = _call(
        runner,
        ("/usr/bin/git", "-C", root, "rev-parse", "HEAD", "HEAD^{tree}"),
        "runtime_readback",
    ).splitlines()
    dirty = _call(
        runner,
        (
            "/usr/bin/git",
            "-C",
            root,
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
        ),
        "runtime_readback",
    )
    if values != [host["commit"], host["tree"]] or dirty:
        raise ReleaseError("runtime_identity_mismatch")
    return {"runtime_root": root, "commit": values[0], "tree": values[1], "clean": True}


def _vm_call(
    runner: VmRunner,
    command: Sequence[str],
    code: str,
    input_text: str | None = None,
) -> str:
    try:
        result = runner(tuple(command), input_text)
    except (OSError, subprocess.SubprocessError) as exc:
        raise ReleaseError(f"{code}_unavailable") from exc
    if result.returncode:
        raise ReleaseError(f"{code}_failed")
    return result.stdout


def _read_vm_report_manifest(
    path: Path, vm_runner: VmRunner = _run_vm_agent
) -> tuple[bytes, dict]:
    remote = PurePosixPath(str(path))
    if (
        not remote.is_absolute()
        or ".." in remote.parts
        or remote != REPORT_MANIFEST_PATH
    ):
        raise ReleaseError("report_manifest_path_invalid")
    doctor_raw = _vm_call(
        vm_runner,
        (SSH_MINI_AGENT, "doctor", "--json"),
        "ssh_mini_doctor",
    ).encode()
    doctor = _decode_json(doctor_raw, "ssh_mini_doctor")
    if doctor.get("ok") is not True:
        raise ReleaseError("ssh_mini_doctor_failed")
    script = f"""import base64
import hashlib
import json
import os
import stat

path = {json.dumps(str(remote))}
root = {json.dumps(str(REPORT_MANIFEST_ROOT))}
if os.path.dirname(path) != root or os.path.realpath(path) != path:
    raise RuntimeError('report_manifest_path_invalid')
before = os.lstat(path)
if (not stat.S_ISREG(before.st_mode) or stat.S_ISLNK(before.st_mode)
        or before.st_nlink != 1 or before.st_size <= 0
        or before.st_size > {MAX_FILE_BYTES}):
    raise RuntimeError('report_manifest_identity_invalid')
fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
try:
    opened = os.fstat(fd)
    identity = lambda item: (
        item.st_dev, item.st_ino, item.st_mode, item.st_nlink,
        item.st_size, item.st_mtime_ns, item.st_ctime_ns,
    )
    if identity(opened) != identity(before):
        raise RuntimeError('report_manifest_changed')
    raw = bytearray()
    while len(raw) <= {MAX_FILE_BYTES}:
        chunk = os.read(fd, min(1024 * 1024, {MAX_FILE_BYTES} + 1 - len(raw)))
        if not chunk:
            break
        raw.extend(chunk)
    if len(raw) != opened.st_size or identity(os.fstat(fd)) != identity(before):
        raise RuntimeError('report_manifest_changed')
    if identity(os.lstat(path)) != identity(before):
        raise RuntimeError('report_manifest_changed')
finally:
    os.close(fd)
raw = bytes(raw)
print(json.dumps({{
    'raw_base64': base64.b64encode(raw).decode('ascii'),
    'sha256': hashlib.sha256(raw).hexdigest(),
}}, sort_keys=True, separators=(',', ':')))
"""
    response_raw = _vm_call(
        vm_runner,
        (SSH_MINI_AGENT, "run_py_json"),
        "report_manifest_readback",
        script,
    ).encode()
    response = _decode_json(response_raw, "report_manifest_readback")
    try:
        raw = base64.b64decode(str(response.get("raw_base64") or ""), validate=True)
    except (ValueError, TypeError) as exc:
        raise ReleaseError("report_manifest_readback_invalid") from exc
    if not raw or len(raw) > MAX_FILE_BYTES or response.get("sha256") != _sha(raw):
        raise ReleaseError("report_manifest_readback_invalid")
    return raw, _decode_json(raw, "report_manifest")


def _parse_env(raw: bytes) -> dict[str, str]:
    try:
        lines = raw.decode().splitlines()
    except UnicodeDecodeError as exc:
        raise ReleaseError("candidate_env_invalid") from exc
    result: dict[str, str] = {}
    for line in lines:
        if not line or line.lstrip().startswith("#"):
            continue
        if "=" not in line:
            raise ReleaseError("candidate_env_invalid")
        key, value = line.split("=", 1)
        if not key or key in result:
            raise ReleaseError("candidate_env_invalid")
        result[key] = value
    return result


def _normalize_fence(value: object) -> dict[str, dict[str, int]]:
    if not isinstance(value, Mapping):
        raise ReleaseError("release_note_activation_fence_invalid")
    result: dict[str, dict[str, int]] = {}
    for topic, raw_partitions in value.items():
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,248}", str(topic)):
            raise ReleaseError("release_note_activation_fence_invalid")
        if not isinstance(raw_partitions, Mapping) or not raw_partitions:
            raise ReleaseError("release_note_activation_fence_invalid")
        partitions: dict[str, int] = {}
        for partition, offset in raw_partitions.items():
            key = str(partition)
            if (
                not key.isdigit()
                or str(int(key)) != key
                or isinstance(offset, bool)
                or not isinstance(offset, int)
                or offset < 0
            ):
                raise ReleaseError("release_note_activation_fence_invalid")
            partitions[key] = offset
        result[str(topic)] = partitions
    return result


def _bound_binding(note_raw: bytes, note: Mapping[str, Any]) -> dict:
    activation = note["activation"]
    db_identity = activation.get("db_logical_identity")
    fence = _normalize_fence(activation.get("partition_start_fence"))
    if (
        not isinstance(db_identity, Mapping)
        or not db_identity
        or len(_canonical(db_identity)) > 4096
        or not HEX64.fullmatch(str(activation.get("db_logical_identity_sha256") or ""))
        or activation["db_logical_identity_sha256"] != _sha(_canonical(db_identity))
        or not HEX64.fullmatch(
            str(activation.get("partition_start_fence_sha256") or "")
        )
        or activation["partition_start_fence_sha256"] != _sha(_canonical(fence))
    ):
        raise ReleaseError("release_note_activation_binding_invalid")
    return {
        "epoch_id": activation["epoch_id"],
        "release_fingerprint": note["release_fingerprint_sha256"],
        "release_binding_sha256": _sha(note_raw),
        "config_sha256": note["runtime_projection"]["env_sha256"],
        "db_logical_identity": dict(db_identity),
        "db_logical_identity_sha256": activation["db_logical_identity_sha256"],
        "partition_start_fence": fence,
        "partition_start_fence_sha256": activation["partition_start_fence_sha256"],
    }


def _candidate_inputs(
    note_path: Path,
    note_raw: bytes,
    note: Mapping[str, Any],
    manifest_path: Path,
    env_path: Path,
) -> dict:
    manifest_raw, manifest = _json(manifest_path, "candidate_manifest")
    env_raw = _read(env_path, "candidate_env")
    projection = note["runtime_projection"]
    host = note["release_identity"]["host"]
    face = manifest.get("face_git_bindings", {}).get("runtime_engine", {})
    note_binding = manifest.get("rca_release_note", {})
    if (
        _sha(manifest_raw) != projection["live_manifest_sha256"]
        or _sha(env_raw) != projection["env_sha256"]
        or manifest.get("runtime_root") != host["runtime_root"]
        or manifest.get("promotion_source_head") != host["commit"]
        or manifest.get("env_sha256") != projection["env_sha256"]
        or (face.get("commit"), face.get("tree"), face.get("repo"))
        != (host["commit"], host["tree"], host["runtime_root"])
        or note_binding.get("path") != str(note_path)
        or note_binding.get("release_id") != note["release_id"]
        or note_binding.get("release_fingerprint_sha256")
        != note["release_fingerprint_sha256"]
        or "rca_release_authority" in manifest
    ):
        raise ReleaseError("candidate_manifest_binding_invalid")
    env = _parse_env(env_raw)
    expected = {
        "HERMES_RCA_KAFKA_SUBMIT_ENABLED": "false",
        "HERMES_RCA_OUTBOX_DISPATCH_ENABLED": "true",
        "HERMES_RCA_DELIVERY_COLLECTOR_ENABLED": "true",
        "HERMES_RCA_DELIVERY_DISPATCHER_ENABLED": "true",
        "HERMES_RCA_ACTIVATION_REQUIRED": "true",
        "HERMES_RCA_RELEASE_NOTE_PATH": str(note_path),
        "HERMES_OUTBOUND_MODE": "record-only",
        **{
            key: str(note["activation"]["control_db_path"])
            for key in CONTROL_DB_ENV_KEYS
        },
    }
    if LEGACY_ENV & env.keys() or any(
        env.get(key) != value for key, value in expected.items()
    ):
        raise ReleaseError("candidate_env_binding_invalid")
    return _bound_binding(note_raw, note)


def _open_store(path: Path, read_only: bool) -> RcaControlStore:
    return RcaControlStore(path, require_current=True, read_only=read_only)


def _open_delivery_store(path: Path) -> RcaDeliveryStore:
    return RcaDeliveryStore(
        path,
        require_current=True,
        read_only=True,
        ensure_current_rows=False,
    )


def _activation_expected(binding: Mapping[str, Any]) -> dict[str, str]:
    # The two capsule-named keys are historical ControlStore schema columns.
    # They are checked for compatibility but never exposed as release concepts.
    fingerprint = binding["release_fingerprint"]
    note_sha = binding["release_binding_sha256"]
    fence_sha = binding["partition_start_fence_sha256"]
    return {
        "state": "steady_active",
        "preauthorization_fingerprint": fingerprint,
        "preauthorization_gate_receipt_sha256": note_sha,
        "preauthorization_capsule_sha256": note_sha,
        "preproduction_fingerprint": fingerprint,
        "preproduction_gate_receipt_sha256": note_sha,
        "preproduction_capsule_sha256": note_sha,
        "config_sha256": binding["config_sha256"],
        "db_logical_identity_sha256": binding["db_logical_identity_sha256"],
        "partition_start_fence_sha256": fence_sha,
        "partition_end_fence_sha256": fence_sha,
        "production_fingerprint": fingerprint,
        "production_gate_receipt_sha256": note_sha,
    }


def _public_epoch(epoch: Mapping[str, Any]) -> dict:
    keys = (
        "epoch_id",
        "state",
        "config_sha256",
        "db_logical_identity_sha256",
        "partition_start_fence_sha256",
        "partition_end_fence_sha256",
        "production_fingerprint",
        "production_gate_receipt_sha256",
        "updated_at",
    )
    return {key: epoch.get(key) for key in keys}


def _store_error(exc: Exception) -> ReleaseError:
    code = str(exc)
    if not re.fullmatch(r"[a-z][a-z0-9_]{2,127}", code):
        code = "activation_store_invalid"
    return ReleaseError(code)


def _activation_plan(
    note: Mapping[str, Any],
    binding: Mapping[str, Any],
    store_factory: StoreFactory,
) -> dict:
    activation = note["activation"]
    zero = {
        "dispatchable_outbox": 0,
        "execution_delivery": 0,
        "pending_inbox": 0,
        "total": 0,
    }
    try:
        store = store_factory(Path(activation["control_db_path"]), True)
        current = store.activation_epoch()
        predecessor_id = str(activation.get("expected_predecessor_epoch_id") or "")
        inflight = zero
        if current and current.get("epoch_id") == binding["epoch_id"]:
            if any(
                str(current.get(key) or "") != str(value)
                for key, value in _activation_expected(binding).items()
            ):
                raise ReleaseError("activation_direct_steady_binding_conflict")
            change = False
        else:
            if current and current.get("state") not in {"aborted", "steady_active"}:
                raise ReleaseError("activation_current_epoch_exists")
            if (
                current
                and current.get("state") == "steady_active"
                and not predecessor_id
            ):
                raise ReleaseError("activation_current_epoch_exists")
            if not current and predecessor_id:
                raise ReleaseError("activation_predecessor_binding_changed")
            if predecessor_id:
                predecessor = store.direct_steady_predecessor()
                expected = (
                    predecessor_id,
                    activation["expected_predecessor_state"],
                    activation["expected_predecessor_binding_fingerprint"],
                )
                observed = (
                    predecessor.get("epoch_id") if predecessor else "",
                    predecessor.get("state") if predecessor else "",
                    predecessor.get("binding_fingerprint") if predecessor else "",
                )
                if observed != expected:
                    raise ReleaseError("activation_predecessor_binding_changed")
                inflight = dict(predecessor["inflight"])
            if (
                current
                and current.get("state") == "steady_active"
                and inflight["total"]
            ):
                raise ReleaseError("activation_predecessor_inflight_not_drained")
            change = True
    except ReleaseError:
        raise
    except (ActivationEpochError, RuntimeError, ValueError) as exc:
        raise _store_error(exc) from exc
    return {
        "epoch_id": binding["epoch_id"],
        "would_change": change,
        "predecessor_inflight": inflight,
        "release_fingerprint": binding["release_fingerprint"],
        "release_note_sha256": binding["release_binding_sha256"],
        "config_sha256": binding["config_sha256"],
    }


def _activation_apply(
    note: Mapping[str, Any],
    binding: Mapping[str, Any],
    plan: Mapping[str, Any],
    store_factory: StoreFactory,
) -> dict:
    activation = note["activation"]
    try:
        current = store_factory(
            Path(activation["control_db_path"]), False
        ).activate_direct_steady_epoch(
            epoch_id=binding["epoch_id"],
            release_fingerprint=binding["release_fingerprint"],
            release_binding_sha256=binding["release_binding_sha256"],
            config_sha256=binding["config_sha256"],
            db_logical_identity=binding["db_logical_identity"],
            partition_start_fence=binding["partition_start_fence"],
            operator=activation["operator"],
            reason=activation["reason"],
            expected_predecessor_epoch_id=str(
                activation.get("expected_predecessor_epoch_id") or ""
            ),
            expected_predecessor_state=str(
                activation.get("expected_predecessor_state") or ""
            ),
            expected_predecessor_binding_fingerprint=str(
                activation.get("expected_predecessor_binding_fingerprint") or ""
            ),
        )
    except (ActivationEpochError, RuntimeError, ValueError) as exc:
        raise _store_error(exc) from exc
    return {
        "changed": bool(plan["would_change"]),
        "current_epoch": _public_epoch(current),
    }


def _activation_status(
    note: Mapping[str, Any], binding: Mapping[str, Any], store_factory: StoreFactory
) -> dict:
    try:
        current = store_factory(
            Path(note["activation"]["control_db_path"]), True
        ).activation_epoch()
    except (ActivationEpochError, RuntimeError, ValueError) as exc:
        raise _store_error(exc) from exc
    if (
        not current
        or current.get("epoch_id") != binding["epoch_id"]
        or any(
            str(current.get(key) or "") != str(value)
            for key, value in _activation_expected(binding).items()
        )
    ):
        raise ReleaseError("activation_not_steady")
    return _public_epoch(current)


def _artifact(source: Path, target: Path, before_sha: str, after_sha: str) -> dict:
    if before_sha != "ABSENT" and not HEX64.fullmatch(before_sha):
        raise ReleaseError("artifact_expected_sha_invalid")
    desired = _sha(_read(source, "artifact_source"))
    current = (
        _sha(_read(target, "artifact_target"))
        if target.exists() or target.is_symlink()
        else "ABSENT"
    )
    if desired != after_sha or current not in {before_sha, desired}:
        raise ReleaseError("artifact_cas_mismatch")
    return {
        "source": str(source),
        "target": str(target),
        "before_sha256": current,
        "expected_before_sha256": before_sha,
        "after_sha256": desired,
        "would_change": current != desired,
    }


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_staged(path: Path, prefix: str, raw: bytes) -> str:
    descriptor, temporary = tempfile.mkstemp(prefix=prefix, dir=path)
    try:
        os.fchmod(descriptor, 0o600)
        view = memoryview(raw)
        while view:
            view = view[os.write(descriptor, view) :]
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        return temporary
    except Exception:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def _target_bytes(target: Path, code: str) -> bytes | None:
    return _read(target, code) if target.exists() or target.is_symlink() else None


def _stage_artifacts(items: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    if len(items) != 2:
        raise ReleaseError("install_artifact_set_invalid")
    staged: list[dict[str, Any]] = []
    try:
        for item in items:
            source, target = Path(item["source"]), Path(item["target"])
            if not target.parent.is_dir():
                raise ReleaseError("install_parent_invalid")
            raw = _read(source, "install_source")
            desired = _sha(raw)
            current_raw = _target_bytes(target, "install_target")
            current = _sha(current_raw) if current_raw is not None else "ABSENT"
            if (
                desired != item["after_sha256"]
                or current != item["before_sha256"]
                or current not in {item["expected_before_sha256"], desired}
            ):
                raise ReleaseError("install_cas_mismatch")
            candidate = ""
            preimage = ""
            try:
                if current != desired:
                    candidate = _write_staged(
                        target.parent, f".{target.name}.minimal-candidate-", raw
                    )
                    if current_raw is not None:
                        preimage = _write_staged(
                            target.parent,
                            f".{target.name}.minimal-preimage-",
                            current_raw,
                        )
            except Exception:
                for path in (candidate, preimage):
                    if path:
                        try:
                            os.unlink(path)
                        except FileNotFoundError:
                            pass
                raise
            staged.append({
                "target": target,
                "before_sha256": current,
                "after_sha256": desired,
                "candidate": candidate,
                "preimage": preimage,
                "changed": current != desired,
                "installed": False,
            })
        return staged
    except Exception:
        _cleanup_staged(staged)
        raise


def _cleanup_staged(staged: Sequence[dict[str, Any]]) -> None:
    for item in staged:
        for key in ("candidate", "preimage"):
            path = str(item.get(key) or "")
            if path:
                try:
                    os.unlink(path)
                except FileNotFoundError:
                    pass
                item[key] = ""


def _cleanup_staged_candidates(staged: Sequence[dict[str, Any]]) -> None:
    for item in staged:
        path = str(item.get("candidate") or "")
        if not path:
            continue
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
        item["candidate"] = ""


def _restore_preimage(item: dict[str, Any]) -> None:
    target = Path(item["target"])
    preimage = str(item.get("preimage") or "")
    try:
        current_raw = _target_bytes(target, "install_rollback_cas")
        current = _sha(current_raw) if current_raw is not None else "ABSENT"
        if current != item["after_sha256"]:
            raise ReleaseError("install_rollback_cas_mismatch")
        if preimage:
            os.replace(preimage, target)
            item["preimage"] = ""
        else:
            target.unlink()
        _fsync_directory(target.parent)
        current_raw = _target_bytes(target, "install_rollback_readback")
        current = _sha(current_raw) if current_raw is not None else "ABSENT"
        if current != item["before_sha256"]:
            raise ReleaseError("install_rollback_readback_mismatch")
        item["installed"] = False
    except (OSError, ReleaseError) as exc:
        raise ReleaseError("install_rollback_failed") from exc


def _install_staged(staged: list[dict[str, Any]]) -> list[dict[str, Any]]:
    try:
        for item in staged:
            target = Path(item["target"])
            current_raw = _target_bytes(target, "install_cas_target")
            current = _sha(current_raw) if current_raw is not None else "ABSENT"
            if current != item["before_sha256"]:
                raise ReleaseError("install_cas_mismatch")
        for item in staged:
            if not item["changed"]:
                continue
            target = Path(item["target"])
            os.replace(str(item["candidate"]), target)
            item["candidate"] = ""
            item["installed"] = True
            _fsync_directory(target.parent)
        result = []
        for item in staged:
            target = Path(item["target"])
            if _sha(_read(target, "install_readback")) != item["after_sha256"]:
                raise ReleaseError("install_readback_mismatch")
            result.append({
                "target": str(target),
                "before_sha256": item["before_sha256"],
                "after_sha256": item["after_sha256"],
                "changed": item["changed"],
            })
        return result
    except ReleaseError:
        raise
    except OSError as exc:
        raise ReleaseError("install_pair_failed") from exc


def _pid_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except (PermissionError, OSError):
        return True
    return True


def _remove_stale_release_lock(path: Path) -> None:
    descriptor = -1
    try:
        before = path.lstat()
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
            or not 0 < before.st_size <= MAX_FILE_BYTES
        ):
            raise ReleaseError("release_apply_locked")
        descriptor = os.open(
            path,
            os.O_RDWR
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        opened = os.fstat(descriptor)
        if _identity(opened) != _identity(before):
            raise ReleaseError("release_apply_locked")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (BlockingIOError, OSError) as exc:
            raise ReleaseError("release_apply_locked") from exc
        if _identity(path.lstat()) != _identity(opened):
            raise ReleaseError("release_apply_locked")
        os.lseek(descriptor, 0, os.SEEK_SET)
        raw = os.read(descriptor, opened.st_size)
        if (
            len(raw) != opened.st_size
            or os.read(descriptor, 1)
            or _identity(os.fstat(descriptor)) != _identity(opened)
        ):
            raise ReleaseError("release_apply_locked")
        value = _decode_json(raw, "release_lock")
        pid = value.get("pid")
        if (
            set(value) != {"pid", "release_id"}
            or isinstance(pid, bool)
            or not isinstance(pid, int)
            or pid <= 0
            or not IDENTIFIER.fullmatch(str(value.get("release_id") or ""))
            or _pid_is_alive(pid)
        ):
            raise ReleaseError("release_apply_locked")
        observed = path.lstat()
        if _identity(observed) != _identity(opened):
            raise ReleaseError("release_apply_locked")
        path.unlink()
        _fsync_directory(path.parent)
    except ReleaseError as exc:
        raise ReleaseError("release_apply_locked") from exc
    except OSError as exc:
        raise ReleaseError("release_apply_locked") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _acquire_release_lock(
    home: Path, release_id: str
) -> tuple[Path, int, tuple[int, int]]:
    path = home / "runtime" / RELEASE_LOCK_NAME
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = -1
    for attempt in range(2):
        try:
            descriptor = os.open(path, flags, 0o600)
            os.fchmod(descriptor, 0o600)
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except FileExistsError as exc:
            if attempt:
                raise ReleaseError("release_apply_locked") from exc
            _remove_stale_release_lock(path)
        except OSError as exc:
            if descriptor >= 0:
                opened = os.fstat(descriptor)
                os.close(descriptor)
                descriptor = -1
                try:
                    current = path.lstat()
                    if (current.st_dev, current.st_ino) == (
                        opened.st_dev,
                        opened.st_ino,
                    ):
                        path.unlink()
                        _fsync_directory(path.parent)
                except OSError:
                    pass
            raise ReleaseError("release_lock_unavailable") from exc
    if descriptor < 0:
        raise ReleaseError("release_lock_unavailable")
    observed = os.fstat(descriptor)
    identity = (observed.st_dev, observed.st_ino)
    try:
        raw = _canonical({"pid": os.getpid(), "release_id": release_id}) + b"\n"
        view = memoryview(raw)
        while view:
            view = view[os.write(descriptor, view) :]
        os.fsync(descriptor)
        _fsync_directory(path.parent)
        return path, descriptor, identity
    except Exception:
        os.close(descriptor)
        try:
            current = path.lstat()
            if (current.st_dev, current.st_ino) == identity:
                path.unlink()
        except OSError:
            pass
        raise


def _release_release_lock(lock: tuple[Path, int, tuple[int, int]]) -> None:
    path, descriptor, identity = lock
    try:
        try:
            observed = path.lstat()
        except FileNotFoundError:
            return
        if (observed.st_dev, observed.st_ino) == identity:
            path.unlink()
            _fsync_directory(path.parent)
    except OSError:
        pass
    finally:
        try:
            os.close(descriptor)
        except OSError:
            pass


def _write_reserved_receipt(
    receipt: tuple[Path, int, tuple[int, int]], value: Mapping[str, Any]
) -> None:
    path, descriptor, identity = receipt
    try:
        before = path.lstat()
        opened = os.fstat(descriptor)
        if (
            (before.st_dev, before.st_ino) != identity
            or (opened.st_dev, opened.st_ino) != identity
            or not stat.S_ISREG(before.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or stat.S_IMODE(before.st_mode) != 0o600
            or stat.S_IMODE(opened.st_mode) != 0o600
            or before.st_uid != os.geteuid()
            or opened.st_uid != os.geteuid()
            or before.st_nlink != 1
            or opened.st_nlink != 1
        ):
            raise ReleaseError("receipt_identity_invalid")
        raw = _pretty_json(value)
        os.lseek(descriptor, 0, os.SEEK_SET)
        os.ftruncate(descriptor, 0)
        view = memoryview(raw)
        while view:
            view = view[os.write(descriptor, view) :]
        os.fsync(descriptor)
        after = path.lstat()
        if (after.st_dev, after.st_ino) != identity:
            raise ReleaseError("receipt_changed")
        _fsync_directory(path.parent)
    except ReleaseError:
        raise
    except OSError as exc:
        raise ReleaseError("receipt_write_failed") from exc


def _reserve_apply_receipt(
    path: Path, started: Mapping[str, Any]
) -> tuple[Path, int, tuple[int, int]]:
    if not path.parent.is_dir():
        raise ReleaseError("receipt_parent_invalid")
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = -1
    try:
        descriptor = os.open(path, flags, 0o600)
        os.fchmod(descriptor, 0o600)
    except FileExistsError as exc:
        raise ReleaseError("receipt_exists") from exc
    except OSError as exc:
        if descriptor >= 0:
            opened = os.fstat(descriptor)
            os.close(descriptor)
            try:
                current = path.lstat()
                if (current.st_dev, current.st_ino) == (
                    opened.st_dev,
                    opened.st_ino,
                ):
                    path.unlink()
                    _fsync_directory(path.parent)
            except OSError:
                pass
        raise ReleaseError("receipt_unavailable") from exc
    opened = os.fstat(descriptor)
    receipt = (path, descriptor, (opened.st_dev, opened.st_ino))
    try:
        _write_reserved_receipt(receipt, started)
        return receipt
    except Exception:
        os.close(descriptor)
        try:
            current = path.lstat()
            if (current.st_dev, current.st_ino) == receipt[2]:
                path.unlink()
                _fsync_directory(path.parent)
        except OSError:
            pass
        raise


def _close_apply_receipt(receipt: tuple[Path, int, tuple[int, int]] | None) -> None:
    if receipt is not None:
        try:
            os.close(receipt[1])
        except OSError:
            pass


def _read_apply_receipt(path: Path) -> tuple[bytes, dict]:
    try:
        observed = path.lstat()
    except OSError as exc:
        raise ReleaseError("apply_receipt_unavailable") from exc
    if stat.S_IMODE(observed.st_mode) != 0o600:
        raise ReleaseError("apply_receipt_identity_invalid")
    try:
        return _json(path, "apply_receipt", owner_only=True)
    except ReleaseError as exc:
        if exc.code.startswith("apply_receipt_"):
            raise
        raise ReleaseError("apply_receipt_invalid") from exc


def _write_terminal_failure_marker(
    receipt_path: Path,
    failed: Mapping[str, Any],
    receipt_error: ReleaseError,
) -> Path:
    path = receipt_path.with_name(f"{receipt_path.name}.terminal-failure.json")
    value = {
        "schema_version": TERMINAL_FAILURE_SCHEMA,
        "transaction_state": "failed",
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "primary_receipt_path": str(receipt_path),
        "primary_receipt_error": receipt_error.code,
        "failure": dict(failed),
    }
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = -1
    identity: tuple[int, int] | None = None
    try:
        descriptor = os.open(path, flags, 0o600)
        os.fchmod(descriptor, 0o600)
        opened = os.fstat(descriptor)
        identity = (opened.st_dev, opened.st_ino)
        raw = _pretty_json(value)
        view = memoryview(raw)
        while view:
            view = view[os.write(descriptor, view) :]
        os.fsync(descriptor)
        observed = path.lstat()
        if (
            (observed.st_dev, observed.st_ino) != identity
            or stat.S_IMODE(observed.st_mode) != 0o600
            or observed.st_uid != os.geteuid()
            or observed.st_nlink != 1
        ):
            raise ReleaseError("terminal_failure_marker_identity_invalid")
        _fsync_directory(path.parent)
        return path
    except ReleaseError:
        raise
    except OSError as exc:
        raise ReleaseError("terminal_failure_marker_unavailable") from exc
    finally:
        if descriptor >= 0:
            try:
                os.close(descriptor)
            except OSError:
                pass
        if identity is not None:
            try:
                observed = path.lstat()
                if observed.st_size == 0 and (
                    observed.st_dev,
                    observed.st_ino,
                ) == identity:
                    path.unlink()
                    _fsync_directory(path.parent)
            except OSError:
                pass


def _install_apply_signal_handlers() -> dict[int, Any]:
    previous: dict[int, Any] = {}

    def interrupted(signum: int, _frame: object) -> None:
        name = signal.Signals(signum).name.lower()
        raise ReleaseError(f"apply_{name}")

    try:
        for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            previous[signum] = signal.getsignal(signum)
            signal.signal(signum, interrupted)
    except (OSError, ValueError):
        for signum, handler in previous.items():
            try:
                signal.signal(signum, handler)
            except (OSError, ValueError):
                pass
        return {}
    return previous


def _restore_apply_signal_handlers(previous: Mapping[int, Any]) -> None:
    for signum, handler in previous.items():
        try:
            signal.signal(signum, handler)
        except (OSError, ValueError):
            pass


def _ignore_apply_signals(previous: Mapping[int, Any]) -> None:
    for signum in previous:
        try:
            signal.signal(signum, signal.SIG_IGN)
        except (OSError, ValueError):
            pass


def _launch(label: str, runner: Runner) -> dict:
    try:
        result = runner(("/bin/launchctl", "print", f"gui/{os.getuid()}/{label}"))
    except (OSError, subprocess.SubprocessError) as exc:
        raise ReleaseError("launchctl_unavailable") from exc
    raw = result.stdout if not result.returncode else result.stderr
    if result.returncode not in {0, 113}:
        raise ReleaseError("launchctl_unavailable")
    pids = re.findall(r"(?m)^\s*pid\s*=\s*([0-9]+)\s*$", raw)
    if not result.returncode and len(pids) > 1:
        raise ReleaseError("launchctl_identity_invalid")
    if pids and int(pids[0]) <= 0:
        raise ReleaseError("launchctl_identity_invalid")
    return {
        "label": label,
        "loaded": not result.returncode,
        "pid": int(pids[0]) if pids else None,
    }


def _freshness(raw: object, now: datetime) -> tuple[datetime, float]:
    try:
        parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError
        age = (now - parsed.astimezone(timezone.utc)).total_seconds()
    except (TypeError, ValueError) as exc:
        raise ReleaseError("resident_freshness_invalid") from exc
    if age < -5:
        raise ReleaseError("resident_freshness_invalid")
    return parsed, age


def resident_readback(
    note: Mapping[str, Any],
    note_sha: str,
    home: Path,
    *,
    runner: Runner = _run,
    process_factory: ProcessFactory = psutil.Process,
    now: datetime | None = None,
    previous: Mapping[str, int | None] | None = None,
) -> dict:
    now = now or datetime.now(timezone.utc)
    host = note["release_identity"]["host"]
    projection = note["runtime_projection"]
    required = []
    for (
        label,
        relative_script,
        relative_health,
        freshness_field,
        schema,
    ) in REQUIRED_RESIDENTS:
        launch = _launch(label, runner)
        pid = launch["pid"]
        if (
            not launch["loaded"]
            or not pid
            or (previous is not None and previous.get(label) == pid)
        ):
            raise ReleaseError("resident_not_restarted")
        try:
            process = process_factory(pid)
            cwd = process.cwd()
            command = process.cmdline()
            created = float(process.create_time())
        except (OSError, psutil.Error, ValueError) as exc:
            raise ReleaseError("resident_process_unreadable") from exc
        script = str(Path(host["runtime_root"]) / relative_script)
        if cwd != host["runtime_root"] or script not in command:
            raise ReleaseError("resident_identity_mismatch")
        _raw, health = _json(home / relative_health, "resident_health")
        fresh, age = _freshness(health.get(freshness_field), now)
        freshness_kind = "restart_transition" if not schema else "health_heartbeat"
        if (schema or previous is not None) and age > MAX_HEALTH_AGE_SECONDS:
            raise ReleaseError("resident_stale")
        if not schema:
            valid = (
                health.get("pid") == pid and health.get("gateway_state") == "running"
            )
        else:
            identity = health.get("runtime_identity")
            valid = bool(
                health.get("schema_version") == schema
                and health.get("healthy") is True
                and str(health.get("state") or "")
                not in {"", "starting", "stopped", "disabled", "error", "circuit_open"}
                and runtime_identity_is_valid(identity, service_label=label)
                and identity.get("pid") == pid
                and identity.get("cwd") == host["runtime_root"]
                and identity.get("script") == script
                and abs(float(identity.get("process_create_time") or 0) - created)
                <= 0.01
            )
            if label in RELEASE_HEALTH_RESIDENTS:
                expected_release = {
                    "epoch_id": note["activation"]["epoch_id"],
                    "release_id": note["release_id"],
                    "release_fingerprint_sha256": note["release_fingerprint_sha256"],
                    "release_note_path": str(note["_path"]),
                    "release_note_sha256": note_sha,
                    "runtime_root": host["runtime_root"],
                    "runtime_commit": host["commit"],
                    "runtime_tree": host["tree"],
                    "live_manifest_sha256": projection["live_manifest_sha256"],
                    "live_env_sha256": projection["env_sha256"],
                }
                valid = valid and health.get("release") == expected_release
        if not valid:
            raise ReleaseError("resident_health_invalid")
        required.append({
            "label": label,
            "pid": pid,
            "cwd": cwd,
            "script": script,
            "process_create_time": created,
            "freshness_kind": freshness_kind,
            "freshness_at": fresh.isoformat(),
            "freshness_age_seconds": age,
        })
    disabled = [_launch(label, runner) for label in DISABLED_RESIDENTS]
    if any(item["loaded"] for item in disabled):
        raise ReleaseError("disabled_resident_loaded")
    return {
        "name": "operator_issue_only_v1",
        "required": required,
        "disabled": disabled,
    }


def _profile_snapshot(runner: Runner) -> dict:
    return {
        "name": "operator_issue_only_v1",
        "required": [_launch(row[0], runner) for row in REQUIRED_RESIDENTS],
        "disabled": [_launch(label, runner) for label in DISABLED_RESIDENTS],
    }


def _all_resident_labels() -> tuple[str, ...]:
    return tuple(row[0] for row in REQUIRED_RESIDENTS) + tuple(DISABLED_RESIDENTS)


def _persistent_disabled_readback(runner: Runner) -> dict[str, bool]:
    output = _call(
        runner,
        ("/bin/launchctl", "print-disabled", f"gui/{os.getuid()}"),
        "resident_persistent_readback",
    )
    result: dict[str, bool] = {}
    for label, raw in re.findall(
        r'(?m)^\s*"([^"]+)"\s*=>\s*(true|false|enabled|disabled)\s*$', output
    ):
        if label in result:
            raise ReleaseError("resident_persistent_readback_invalid")
        result[label] = raw in {"true", "disabled"}
    return result


def _persistent_profile_readback(runner: Runner) -> dict:
    disabled = _persistent_disabled_readback(runner)
    required_rows = [
        {"label": row[0], "disabled": bool(disabled.get(row[0], False))}
        for row in REQUIRED_RESIDENTS
    ]
    disabled_rows = [
        {"label": label, "disabled": disabled.get(label)}
        for label in DISABLED_RESIDENTS
    ]
    if any(row["disabled"] for row in required_rows) or any(
        row["disabled"] is not True for row in disabled_rows
    ):
        raise ReleaseError("resident_persistent_profile_mismatch")
    return {"required": required_rows, "disabled": disabled_rows}


def _assert_all_residents_stopped(runner: Runner) -> list[dict]:
    observed = [_launch(label, runner) for label in _all_resident_labels()]
    if any(item["loaded"] for item in observed):
        raise ReleaseError("resident_quiesce_readback_failed")
    return observed


def _stop_all_residents(runner: Runner) -> dict:
    errors: list[str] = []
    for label in _all_resident_labels():
        try:
            if _launch(label, runner)["loaded"]:
                _call(
                    runner,
                    ("/bin/launchctl", "bootout", f"gui/{os.getuid()}/{label}"),
                    "resident_stop",
                )
        except ReleaseError as exc:
            errors.append(f"{label}:{exc.code}")
    observed: list[dict] = []
    for label in _all_resident_labels():
        try:
            observed.append(_launch(label, runner))
        except ReleaseError as exc:
            errors.append(f"{label}:{exc.code}")
    return {
        "all_stopped": len(observed) == len(_all_resident_labels())
        and not any(item["loaded"] for item in observed),
        "observed": observed,
        "errors": errors,
    }


def _quiesce_residents(runner: Runner) -> dict:
    previous = {label: _launch(label, runner) for label in _all_resident_labels()}
    for label in _all_resident_labels():
        if previous[label]["loaded"]:
            _call(
                runner,
                ("/bin/launchctl", "bootout", f"gui/{os.getuid()}/{label}"),
                "resident_quiesce",
            )
    stopped = _assert_all_residents_stopped(runner)
    for label, *_rest in REQUIRED_RESIDENTS:
        _call(
            runner,
            ("/bin/launchctl", "enable", f"gui/{os.getuid()}/{label}"),
            "resident_persistent_enable",
        )
    for label in DISABLED_RESIDENTS:
        _call(
            runner,
            ("/bin/launchctl", "disable", f"gui/{os.getuid()}/{label}"),
            "resident_persistent_disable",
        )
    persistent = _persistent_profile_readback(runner)
    _assert_all_residents_stopped(runner)
    return {
        "previous": previous,
        "stopped": stopped,
        "persistent": persistent,
    }


def _resident_identity_binding(profile: Mapping[str, Any]) -> dict:
    required = profile.get("required")
    disabled = profile.get("disabled")
    persistent = profile.get("persistent")
    if (
        not isinstance(required, list)
        or not isinstance(disabled, list)
        or not isinstance(persistent, Mapping)
        or len(required) != len(REQUIRED_RESIDENTS)
        or len(disabled) != len(DISABLED_RESIDENTS)
    ):
        raise ReleaseError("resident_binding_invalid")
    required_by_label = {
        str(item.get("label") or ""): item
        for item in required
        if isinstance(item, Mapping)
    }
    disabled_by_label = {
        str(item.get("label") or ""): item
        for item in disabled
        if isinstance(item, Mapping)
    }
    if set(required_by_label) != {row[0] for row in REQUIRED_RESIDENTS} or set(
        disabled_by_label
    ) != set(DISABLED_RESIDENTS):
        raise ReleaseError("resident_binding_invalid")
    required_result = []
    for label, *_rest in REQUIRED_RESIDENTS:
        item = required_by_label[label]
        pid = item.get("pid")
        created = item.get("process_create_time")
        if (
            isinstance(pid, bool)
            or not isinstance(pid, int)
            or pid <= 0
            or isinstance(created, bool)
            or not isinstance(created, (int, float))
            or float(created) <= 0
            or not str(item.get("cwd") or "")
            or not str(item.get("script") or "")
        ):
            raise ReleaseError("resident_binding_invalid")
        required_result.append({
            "label": label,
            "pid": pid,
            "cwd": item["cwd"],
            "script": item["script"],
            "process_create_time": float(created),
        })
    disabled_result = []
    for label in DISABLED_RESIDENTS:
        item = disabled_by_label[label]
        if item.get("loaded") is not False or item.get("pid") is not None:
            raise ReleaseError("resident_binding_invalid")
        disabled_result.append({"label": label, "loaded": False, "pid": None})
    return {
        "required": required_result,
        "disabled": disabled_result,
        "persistent": persistent,
    }


def _resident_transition(
    quiesce: Mapping[str, Any], profile: Mapping[str, Any]
) -> dict:
    current = _resident_identity_binding(profile)
    previous = quiesce.get("previous")
    if not isinstance(previous, Mapping):
        raise ReleaseError("resident_transition_invalid")
    required = []
    for item in current["required"]:
        prior = previous.get(item["label"])
        if not isinstance(prior, Mapping):
            raise ReleaseError("resident_transition_invalid")
        previous_pid = prior.get("pid")
        if previous_pid is not None and previous_pid == item["pid"]:
            raise ReleaseError("resident_not_restarted")
        required.append({
            **item,
            "previous_loaded": prior.get("loaded") is True,
            "previous_pid": previous_pid,
            "new_pid": item["pid"],
        })
    disabled = []
    for label in DISABLED_RESIDENTS:
        prior = previous.get(label)
        if not isinstance(prior, Mapping):
            raise ReleaseError("resident_transition_invalid")
        disabled.append({
            "label": label,
            "previous_loaded": prior.get("loaded") is True,
            "previous_pid": prior.get("pid"),
            "new_pid": None,
        })
    return {
        "required": required,
        "disabled": disabled,
        "persistent": current["persistent"],
    }


def _resident_profile_readback(
    note: Mapping[str, Any],
    note_sha: str,
    home: Path,
    *,
    runner: Runner,
    process_factory: ProcessFactory,
    now: datetime | None = None,
    previous: Mapping[str, int | None] | None = None,
) -> dict:
    result = resident_readback(
        note,
        note_sha,
        home,
        runner=runner,
        process_factory=process_factory,
        now=now,
        previous=previous,
    )
    return {**result, "persistent": _persistent_profile_readback(runner)}


def _restart(
    note: Mapping[str, Any],
    note_sha: str,
    home: Path,
    runner: Runner,
    process_factory: ProcessFactory,
    timeout: float,
    quiesce: Mapping[str, Any],
) -> dict:
    previous_raw = quiesce.get("previous")
    if not isinstance(previous_raw, Mapping):
        raise ReleaseError("resident_transition_invalid")
    previous = {
        row[0]: (
            previous_raw[row[0]].get("pid")
            if isinstance(previous_raw.get(row[0]), Mapping)
            else None
        )
        for row in REQUIRED_RESIDENTS
    }
    _assert_all_residents_stopped(runner)
    launch_dir = home.parent / "Library/LaunchAgents"
    for label, *_rest in REQUIRED_RESIDENTS:
        _call(
            runner,
            (
                "/bin/launchctl",
                "bootstrap",
                f"gui/{os.getuid()}",
                str(launch_dir / f"{label}.plist"),
            ),
            "resident_bootstrap",
        )
    deadline = time.monotonic() + timeout
    while True:
        try:
            return _resident_profile_readback(
                note,
                note_sha,
                home,
                runner=runner,
                process_factory=process_factory,
                previous=previous,
            )
        except ReleaseError as exc:
            if time.monotonic() >= deadline:
                raise ReleaseError("restart_readback_timeout") from exc
            time.sleep(0.25)


def canary_readback(
    note: Mapping[str, Any],
    note_sha256: str,
    *,
    delivery_store_factory: DeliveryStoreFactory = _open_delivery_store,
) -> dict:
    canary = note["canary"]
    path = Path(canary["state_path"])
    raw, state = _json(path, "canary_state", owner_only=True)
    issue = str(canary["issue_id"])
    items = state.get("items")
    item = items.get(issue) if isinstance(items, Mapping) else None
    approval = item.get("approval") if isinstance(item, Mapping) else None
    acceptance = approval.get("acceptance") if isinstance(approval, Mapping) else None
    transport = acceptance.get("transport") if isinstance(acceptance, Mapping) else None
    causal = (
        acceptance.get("causal_attribution")
        if isinstance(acceptance, Mapping)
        else None
    )
    source = (
        approval.get("official_readback_source")
        if isinstance(approval, Mapping)
        else None
    )
    submission_key = (
        str(item.get("submission_key") or "") if isinstance(item, Mapping) else ""
    )
    host = note["release_identity"]["host"]
    if (
        state.get("batch_id") != canary["batch_id"]
        or state.get("acceptance_axis") != "transport"
        or state.get("status") != "completed"
        or state.get("selected_issue_ids") != [issue]
        or not isinstance(items, Mapping)
        or set(items) != {issue}
        or not isinstance(item, Mapping)
        or item.get("status") not in {"accepted", "completed"}
        or not re.fullmatch(r"g1q3-rca-s1-[0-9a-f]{64}", submission_key)
        or not isinstance(approval, Mapping)
        or approval.get("acceptance_axis") != "transport"
        or not isinstance(transport, Mapping)
        or transport.get("status") != "pass"
        or not str(transport.get("official_comment_id") or "").strip()
        or transport.get("official_field_keys") != ["field_8c912e", "field_9193cb"]
        or source not in {"read_after_write", "read_after_recovery_write"}
        or transport.get("official_readback_source") != source
        or state.get("runtime_commit") != host["commit"]
        or state.get("runtime_tree") != host["tree"]
    ):
        raise ReleaseError("canary_reference_invalid")
    try:
        db_readback = delivery_store_factory(
            Path(note["activation"]["control_db_path"])
        ).canonical_canary_readback(
            batch_id=str(canary["batch_id"]),
            issue_id=issue,
            submission_key=submission_key,
            activation_epoch_id=str(note["activation"]["epoch_id"]),
        )
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        raise ReleaseError("canary_db_readback_invalid") from exc
    if not isinstance(db_readback, Mapping):
        raise ReleaseError("canary_db_projection_mismatch")
    db_transport = db_readback.get("transport")
    db_execution_readback = db_readback.get("execution_identity_readback")
    if (
        db_readback.get("batch_id") != canary["batch_id"]
        or db_readback.get("issue_id") != issue
        or db_readback.get("submission_key") != submission_key
        or db_readback.get("activation_epoch_id") != note["activation"]["epoch_id"]
        or not isinstance(db_transport, Mapping)
        or not isinstance(db_execution_readback, Mapping)
        or _canonical(dict(transport)) != _canonical(dict(db_transport))
        or _canonical(approval.get("execution_identity_readback"))
        != _canonical(dict(db_execution_readback))
    ):
        raise ReleaseError("canary_db_projection_mismatch")
    execution_readback = validate_execution_identity_readback(
        note,
        note_sha256,
        db_execution_readback,
        expected_task_id=submission_key,
    )
    return {
        "issue_id": issue,
        "state_path": str(path),
        "state_sha256": _sha(raw),
        "status": state["status"],
        "item_status": item["status"],
        "transport": dict(transport),
        "causal_attribution": dict(causal) if isinstance(causal, Mapping) else {},
        "causal_attribution_required": False,
        "execution_identity_readback": execution_readback,
        "canonical_db_readback": dict(db_readback),
    }


def validate_execution_identity_readback(
    note: Mapping[str, Any],
    note_sha: str,
    value: object,
    *,
    expected_task_id: str,
) -> dict:
    """Validate the collector projection from canonical VM execution receipts."""

    identity = note["release_identity"]
    if (
        not HEX64.fullmatch(str(note_sha or ""))
        or not re.fullmatch(r"g1q3-rca-s1-[0-9a-f]{64}", expected_task_id)
        or not isinstance(value, Mapping)
    ):
        raise ReleaseError("execution_identity_readback_invalid")
    readback = dict(value)
    if set(readback) != {
        "schema_version",
        "source",
        "release_id",
        "activation_epoch_id",
        "release_fingerprint_sha256",
        "release_note_sha256",
        "task_id",
        "submission_key",
        "worker",
        "pipeline",
        "report_service",
        "delivery_manifest",
    }:
        raise ReleaseError("execution_identity_readback_invalid")
    if (
        readback.get("schema_version") != EXECUTION_READBACK_SCHEMA
        or readback.get("source") != "host_collector_canonical_vm_receipts_v1"
        or readback.get("release_id") != note["release_id"]
        or readback.get("activation_epoch_id") != note["activation"]["epoch_id"]
        or readback.get("release_fingerprint_sha256")
        != note["release_fingerprint_sha256"]
        or readback.get("release_note_sha256") != note_sha
        or readback.get("task_id") != expected_task_id
        or readback.get("submission_key") != expected_task_id
    ):
        raise ReleaseError("execution_identity_readback_mismatch")

    worker = readback.get("worker")
    pipeline = readback.get("pipeline")
    report = readback.get("report_service")
    delivery_manifest = readback.get("delivery_manifest")
    execution_keys = {
        "commit",
        "tree",
        "runtime_root",
        "clean",
        "entrypoint_path",
        "entrypoint_sha256",
        "receipt_path",
        "receipt_sha256",
    }
    if (
        not isinstance(worker, Mapping)
        or set(worker) != execution_keys
        or not isinstance(pipeline, Mapping)
        or set(pipeline) != execution_keys
        or not isinstance(report, Mapping)
        or set(report)
        != {
            "manifest_path",
            "manifest_sha256",
            "pipeline_commit",
            "pipeline_tree",
            "runtime_root",
            "report_script_sha256",
        }
        or not isinstance(delivery_manifest, Mapping)
        or set(delivery_manifest) != {"path", "sha256"}
    ):
        raise ReleaseError("execution_identity_readback_invalid")

    def nonzero_sha256(raw: object) -> bool:
        value = str(raw or "")
        return bool(HEX64.fullmatch(value)) and value != "0" * 64

    def absolute_child(raw_path: object, raw_root: object) -> bool:
        path_text = str(raw_path or "")
        root_text = str(raw_root or "")
        path = PurePosixPath(path_text)
        root = PurePosixPath(root_text)
        return (
            path.is_absolute()
            and root.is_absolute()
            and ".." not in path.parts
            and ".." not in root.parts
            and str(path) == path_text
            and str(root) == root_text
            and root in path.parents
        )

    worker_identity = identity["worker"]
    pipeline_identity = identity["pipeline"]
    report_identity = identity["report_service"]
    expected_worker_receipt = (
        f"/home/mini/.hermes/shared-state/tasks/{expected_task_id}/result.md"
    )
    expected_service_receipt = f"/mnt/tmp/{expected_task_id}/rca_service_result.json"
    expected_delivery_manifest = f"/mnt/tmp/{expected_task_id}/delivery_manifest.json"
    if (
        worker.get("clean") is not True
        or pipeline.get("clean") is not True
        or any(
            section.get(field) != expected.get(field)
            for section, expected in (
                (worker, worker_identity),
                (pipeline, pipeline_identity),
            )
            for field in ("commit", "tree", "runtime_root")
        )
        or PurePosixPath(str(worker.get("entrypoint_path") or ""))
        != PurePosixPath(str(worker_identity["runtime_root"]))
        / "vm_coding_worker_v2.py"
        or not absolute_child(
            pipeline.get("entrypoint_path"), pipeline_identity["runtime_root"]
        )
        or worker.get("receipt_path") != expected_worker_receipt
        or pipeline.get("receipt_path") != expected_service_receipt
        or not all(
            nonzero_sha256(section.get(field))
            for section in (worker, pipeline)
            for field in ("entrypoint_sha256", "receipt_sha256")
        )
        or report.get("manifest_path") != report_identity["manifest_path"]
        or report.get("manifest_sha256") != report_identity["manifest_sha256"]
        or report.get("pipeline_commit") != pipeline_identity["commit"]
        or report.get("pipeline_tree") != pipeline_identity["tree"]
        or report.get("runtime_root") != pipeline_identity["runtime_root"]
        or not nonzero_sha256(report.get("report_script_sha256"))
        or delivery_manifest.get("path") != expected_delivery_manifest
        or not nonzero_sha256(delivery_manifest.get("sha256"))
    ):
        raise ReleaseError("execution_identity_readback_mismatch")
    return readback


def _inputs(
    note_path: Path, manifest_path: Path, env_path: Path, home: Path
) -> tuple[bytes, dict, dict]:
    note_raw, note = _load_note(note_path, home)
    note["_path"] = str(note_path)
    candidates = _candidate_inputs(note_path, note_raw, note, manifest_path, env_path)
    return note_raw, note, candidates


def _pretty_json(value: object) -> bytes:
    try:
        return (
            json.dumps(
                value,
                sort_keys=True,
                indent=2,
                ensure_ascii=False,
                allow_nan=False,
            ).encode()
            + b"\n"
        )
    except (TypeError, ValueError) as exc:
        raise ReleaseError("json_value_invalid") from exc


def _normalize_partition_topics(
    value: Mapping[str, Sequence[int]],
) -> dict[str, tuple[int, ...]]:
    if not isinstance(value, Mapping):
        raise ReleaseError("prepare_partition_topics_invalid")
    result: dict[str, tuple[int, ...]] = {}
    for raw_topic, raw_partitions in value.items():
        topic = str(raw_topic or "").strip()
        if re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._-]{0,248}", topic
        ) is None or isinstance(raw_partitions, (str, bytes)):
            raise ReleaseError("prepare_partition_topics_invalid")
        try:
            raw_items = tuple(raw_partitions)
            if any(isinstance(item, bool) for item in raw_items):
                raise ValueError("boolean partition")
            partitions = tuple(sorted({int(item) for item in raw_items}))
        except (TypeError, ValueError) as exc:
            raise ReleaseError("prepare_partition_topics_invalid") from exc
        if (
            not partitions
            or any(item < 0 for item in partitions)
            or len(partitions) != len(raw_items)
        ):
            raise ReleaseError("prepare_partition_topics_invalid")
        result[topic] = partitions
    return dict(sorted(result.items()))


def _prepare_control_binding(
    control_db: Path,
    partition_topics: Mapping[str, Sequence[int]],
    store_factory: StoreFactory,
) -> dict[str, Any]:
    topics = _normalize_partition_topics(partition_topics)
    try:
        store = store_factory(control_db, True)
        source = store.control_db_source_snapshot_identity()
        predecessor = store.direct_steady_predecessor()
        fence: dict[str, dict[str, int]] = {}
        for topic, partitions in topics.items():
            progress = store.partition_progress(topic=topic, partitions=partitions)
            if set(progress) != set(partitions) or any(
                isinstance(offset, bool) or not isinstance(offset, int) or offset < 0
                for offset in progress.values()
            ):
                raise ReleaseError("prepare_partition_progress_missing")
            fence[topic] = {
                str(partition): progress[partition] for partition in partitions
            }
    except ReleaseError:
        raise
    except (ActivationEpochError, RuntimeError, TypeError, ValueError) as exc:
        raise _store_error(exc) from exc
    expected_path = str(control_db.expanduser().absolute())
    logical = source.get("logical_db_identity") if isinstance(source, Mapping) else None
    if (
        not isinstance(source, Mapping)
        or source.get("schema_version") != "pnc_rca_control_store_source_snapshot_v1"
        or source.get("present") is not True
        or source.get("path") != expected_path
        or not isinstance(logical, Mapping)
        or not logical
    ):
        raise ReleaseError("prepare_control_snapshot_invalid")
    predecessor_fields = {
        "expected_predecessor_epoch_id": "",
        "expected_predecessor_state": "",
        "expected_predecessor_binding_fingerprint": "",
    }
    if predecessor is not None:
        if (
            not isinstance(predecessor, Mapping)
            or not IDENTIFIER.fullmatch(str(predecessor.get("epoch_id") or ""))
            or predecessor.get("state") not in {"aborted", "steady_active"}
            or not HEX64.fullmatch(str(predecessor.get("binding_fingerprint") or ""))
        ):
            raise ReleaseError("prepare_predecessor_invalid")
        predecessor_fields = {
            "expected_predecessor_epoch_id": predecessor["epoch_id"],
            "expected_predecessor_state": predecessor["state"],
            "expected_predecessor_binding_fingerprint": predecessor[
                "binding_fingerprint"
            ],
        }
    return {
        "db_logical_identity": dict(logical),
        "partition_start_fence": fence,
        **predecessor_fields,
    }


def _prepare_env(live_raw: bytes, note_path: Path, control_db: Path) -> bytes:
    env = _parse_env(live_raw)
    if any(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key) is None for key in env):
        raise ReleaseError("live_env_template_invalid")
    for key in LEGACY_ENV:
        env.pop(key, None)
    env.update({
        "HERMES_RCA_KAFKA_SUBMIT_ENABLED": "false",
        "HERMES_RCA_OUTBOX_DISPATCH_ENABLED": "true",
        "HERMES_RCA_DELIVERY_COLLECTOR_ENABLED": "true",
        "HERMES_RCA_DELIVERY_DISPATCHER_ENABLED": "true",
        "HERMES_RCA_ACTIVATION_REQUIRED": "true",
        "HERMES_RCA_RELEASE_NOTE_PATH": str(note_path),
        "HERMES_OUTBOUND_MODE": "record-only",
        **{key: str(control_db) for key in CONTROL_DB_ENV_KEYS},
    })
    return "".join(f"{key}={env[key]}\n" for key in sorted(env)).encode()


def _prepare_manifest(
    live: Mapping[str, Any],
    *,
    host: Mapping[str, str],
    env_sha256: str,
    note_path: Path,
    release_id: str,
    release_fingerprint: str,
) -> bytes:
    manifest = _decode_json(_canonical(live), "live_manifest_template")
    raw_bindings = manifest.get("face_git_bindings")
    bindings = dict(raw_bindings) if isinstance(raw_bindings, Mapping) else {}
    raw_engine = bindings.get("runtime_engine")
    engine = dict(raw_engine) if isinstance(raw_engine, Mapping) else {}
    engine.update({
        "commit": host["commit"],
        "tree": host["tree"],
        "repo": host["runtime_root"],
    })
    bindings["runtime_engine"] = engine
    manifest.update({
        "runtime_root": host["runtime_root"],
        "promotion_source_head": host["commit"],
        "env_sha256": env_sha256,
        "face_git_bindings": bindings,
        "rca_release_note": {
            "path": str(note_path),
            "release_id": release_id,
            "release_fingerprint_sha256": release_fingerprint,
        },
    })
    manifest.pop("rca_release_authority", None)
    return _pretty_json(manifest)


def _cleanup_prepared_outputs(created: Sequence[Mapping[str, Any]]) -> None:
    failed = False
    parents: set[Path] = set()
    for item in reversed(created):
        path = Path(item["path"])
        parents.add(path.parent)
        try:
            observed = path.lstat()
            if (observed.st_dev, observed.st_ino) != item["identity"]:
                failed = True
                continue
            path.unlink()
        except FileNotFoundError:
            continue
        except OSError:
            failed = True
    for parent in parents:
        try:
            _fsync_directory(parent)
        except OSError:
            failed = True
    if failed:
        raise ReleaseError("prepare_output_cleanup_failed")


def _write_prepared_outputs(
    outputs: Sequence[tuple[Path, bytes]],
) -> list[dict[str, Any]]:
    paths = [path for path, _raw in outputs]
    if len(paths) != 3 or len(set(paths)) != 3:
        raise ReleaseError("prepare_output_paths_invalid")
    descriptors: list[tuple[int, Path, bytes]] = []
    created: list[dict[str, Any]] = []
    failure: Exception | None = None
    try:
        for path, raw in outputs:
            if not path.parent.is_dir():
                raise ReleaseError("prepare_output_parent_invalid")
            flags = (
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0)
            )
            try:
                descriptor = os.open(path, flags, 0o600)
            except FileExistsError as exc:
                raise ReleaseError("prepare_output_exists") from exc
            except OSError as exc:
                raise ReleaseError("prepare_output_unavailable") from exc
            descriptors.append((descriptor, path, raw))
            opened = os.fstat(descriptor)
            created.append({
                "path": str(path),
                "identity": (opened.st_dev, opened.st_ino),
            })
            os.fchmod(descriptor, 0o600)
        for descriptor, _path, raw in descriptors:
            view = memoryview(raw)
            while view:
                view = view[os.write(descriptor, view) :]
            os.fsync(descriptor)
            observed = os.fstat(descriptor)
            if stat.S_IMODE(observed.st_mode) != 0o600 or observed.st_size != len(raw):
                raise ReleaseError("prepare_output_readback_invalid")
    except (OSError, ReleaseError) as exc:
        failure = exc
    finally:
        for descriptor, _path, _raw in descriptors:
            try:
                os.close(descriptor)
            except OSError as exc:
                failure = failure or exc
    if failure is not None:
        _cleanup_prepared_outputs(created)
        if isinstance(failure, ReleaseError):
            raise failure
        raise ReleaseError("prepare_output_write_failed") from failure
    try:
        for parent in sorted({path.parent for path in paths}, key=str):
            _fsync_directory(parent)
    except OSError as exc:
        _cleanup_prepared_outputs(created)
        raise ReleaseError("prepare_output_write_failed") from exc
    return created


def prepare_release(
    *,
    release_id: str,
    epoch_id: str,
    operator: str,
    reason: str,
    canary_batch_id: str,
    canary_issue_id: str,
    canary_state_path: Path,
    host_branch: str,
    host_tag: str,
    host_runtime_root: Path,
    worker_remote: str,
    worker_branch: str,
    worker_tag: str,
    worker_runtime_root: Path,
    pipeline_remote: str,
    pipeline_branch: str,
    pipeline_tag: str,
    pipeline_runtime_root: Path,
    report_manifest_path: Path,
    partition_topics: Mapping[str, Sequence[int]],
    control_db: Path,
    release_note: Path,
    manifest_output: Path,
    env_output: Path,
    home: Path,
    runner: Runner = _run,
    vm_runner: VmRunner = _run_vm_agent,
    report_reader: ReportReader | None = None,
    store_factory: StoreFactory = _open_store,
) -> dict:
    home = _absolute(home, "hermes_home_invalid")
    release_note = _absolute(release_note, "release_note_path_invalid")
    manifest_output = _absolute(manifest_output, "manifest_output_path_invalid")
    env_output = _absolute(env_output, "env_output_path_invalid")
    control_db = _absolute(control_db, "control_db_path_invalid")
    canary_state_path = _absolute(canary_state_path, "canary_state_path_invalid")
    host_runtime_root = _absolute(host_runtime_root, "host_runtime_root_invalid")
    worker_runtime_root = _absolute(worker_runtime_root, "worker_runtime_root_invalid")
    pipeline_runtime_root = _absolute(
        pipeline_runtime_root, "pipeline_runtime_root_invalid"
    )
    report_manifest_path = _absolute(
        report_manifest_path, "report_manifest_path_invalid"
    )
    if control_db != home / CONTROL_DB_RELATIVE_PATH:
        raise ReleaseError("control_db_path_invalid")
    if PurePosixPath(str(report_manifest_path)) != REPORT_MANIFEST_PATH:
        raise ReleaseError("report_manifest_path_invalid")
    live_env_path = home / ".env"
    live_manifest_path = home / "runtime/LIVE_MANIFEST.json"
    output_paths = (release_note, env_output, manifest_output)
    if len(set(output_paths)) != 3:
        raise ReleaseError("prepare_output_paths_invalid")
    if set(output_paths) & {
        live_env_path,
        live_manifest_path,
    }:
        raise ReleaseError("prepare_output_is_live_projection")
    if any(path.exists() or path.is_symlink() for path in output_paths):
        raise ReleaseError("prepare_output_exists")
    specs = {
        "host": {
            "remote": HOST_REMOTE,
            "remote_branch": host_branch,
            "remote_tag": host_tag,
        },
        "worker": {
            "remote": worker_remote,
            "remote_branch": worker_branch,
            "remote_tag": worker_tag,
        },
        "pipeline": {
            "remote": pipeline_remote,
            "remote_branch": pipeline_branch,
            "remote_tag": pipeline_tag,
        },
    }
    resolved = _gitlab_resolve_faces(specs, runner)
    identity: dict[str, dict[str, Any]] = {
        "host": {**resolved["host"], "runtime_root": str(host_runtime_root)},
        "worker": {**resolved["worker"], "runtime_root": str(worker_runtime_root)},
        "pipeline": {
            **resolved["pipeline"],
            "runtime_root": str(pipeline_runtime_root),
        },
    }
    runtime_note = {"release_identity": {"host": identity["host"]}}
    runtime = runtime_readback(runtime_note, runner)
    reader = report_reader or (
        lambda path: _read_vm_report_manifest(path, vm_runner=vm_runner)
    )
    report_raw, report_manifest = reader(report_manifest_path)
    pipeline = identity["pipeline"]
    if (
        report_manifest.get("schema_version") != "pnc_rca_report_manifest_v1"
        or report_manifest.get("runtime_root") != pipeline["runtime_root"]
        or report_manifest.get("pipeline_commit") != pipeline["commit"]
        or report_manifest.get("pipeline_tree") != pipeline["tree"]
        or not HEX64.fullmatch(str(report_manifest.get("report_script_sha256") or ""))
    ):
        raise ReleaseError("report_manifest_pipeline_mismatch")
    identity["report_service"] = {
        "manifest_path": str(report_manifest_path),
        "manifest_sha256": _sha(report_raw),
        "pipeline_commit": pipeline["commit"],
        "pipeline_tree": pipeline["tree"],
    }
    release_fingerprint = _sha(_canonical(identity))
    live_env_raw = _read(live_env_path, "live_env_template")
    live_manifest_raw, live_manifest = _json(
        live_manifest_path, "live_manifest_template"
    )
    env_raw = _prepare_env(live_env_raw, release_note, control_db)
    manifest_raw = _prepare_manifest(
        live_manifest,
        host=identity["host"],
        env_sha256=_sha(env_raw),
        note_path=release_note,
        release_id=release_id,
        release_fingerprint=release_fingerprint,
    )
    activation = _prepare_control_binding(control_db, partition_topics, store_factory)
    note = {
        "schema_version": NOTE_SCHEMA,
        "production_definition": PRODUCTION_DEFINITION,
        "release_id": release_id,
        "release_fingerprint_sha256": release_fingerprint,
        "release_identity": identity,
        "runtime_projection": {
            "env_sha256": _sha(env_raw),
            "live_manifest_sha256": _sha(manifest_raw),
        },
        "activation": {
            "epoch_id": epoch_id,
            "control_db_path": str(control_db),
            "operator": operator,
            "reason": reason,
            "db_logical_identity": activation["db_logical_identity"],
            "db_logical_identity_sha256": _sha(
                _canonical(activation["db_logical_identity"])
            ),
            "partition_start_fence": activation["partition_start_fence"],
            "partition_start_fence_sha256": _sha(
                _canonical(activation["partition_start_fence"])
            ),
            "expected_predecessor_epoch_id": activation[
                "expected_predecessor_epoch_id"
            ],
            "expected_predecessor_state": activation["expected_predecessor_state"],
            "expected_predecessor_binding_fingerprint": activation[
                "expected_predecessor_binding_fingerprint"
            ],
        },
        "resident_profile": {
            "name": "operator_issue_only_v1",
            "required": [row[0] for row in REQUIRED_RESIDENTS],
            "disabled": list(DISABLED_RESIDENTS),
        },
        "canary": {
            "batch_id": canary_batch_id,
            "issue_id": canary_issue_id,
            "state_path": str(canary_state_path),
        },
    }
    note_raw = _pretty_json(note)
    if _gitlab_resolve_faces(specs, runner) != resolved:
        raise ReleaseError("gitlab_changed_during_prepare")
    if runtime_readback(runtime_note, runner) != runtime:
        raise ReleaseError("runtime_changed_during_prepare")
    report_closeout_raw, report_closeout = reader(report_manifest_path)
    if report_closeout_raw != report_raw or report_closeout != report_manifest:
        raise ReleaseError("report_manifest_changed_during_prepare")
    if (
        _read(live_env_path, "live_env_template") != live_env_raw
        or _read(live_manifest_path, "live_manifest_template") != live_manifest_raw
    ):
        raise ReleaseError("live_template_changed_during_prepare")
    created = _write_prepared_outputs((
        (release_note, note_raw),
        (env_output, env_raw),
        (manifest_output, manifest_raw),
    ))
    try:
        _inputs(release_note, manifest_output, env_output, home)
    except Exception:
        _cleanup_prepared_outputs(created)
        raise
    return {
        "schema_version": SCHEMA,
        "ok": True,
        "mode": "prepare",
        "applied": False,
        "release_id": release_id,
        "gitlab": resolved,
        "runtime": runtime,
        "templates": {
            "env": {"path": str(live_env_path), "sha256": _sha(live_env_raw)},
            "manifest": {
                "path": str(live_manifest_path),
                "sha256": _sha(live_manifest_raw),
            },
        },
        "outputs": {
            "release_note": {"path": str(release_note), "sha256": _sha(note_raw)},
            "env": {"path": str(env_output), "sha256": _sha(env_raw)},
            "manifest": {"path": str(manifest_output), "sha256": _sha(manifest_raw)},
        },
    }


def build_plan(
    *,
    release_note: Path,
    manifest_source: Path,
    env_source: Path,
    expected_manifest_sha256: str,
    expected_env_sha256: str,
    home: Path,
    runner: Runner = _run,
    store_factory: StoreFactory = _open_store,
) -> dict:
    home = _absolute(home, "hermes_home_invalid")
    release_note = _absolute(release_note, "release_note_path_invalid")
    manifest_source = _absolute(manifest_source, "manifest_source_path_invalid")
    env_source = _absolute(env_source, "env_source_path_invalid")
    note_raw, note, binding = _inputs(release_note, manifest_source, env_source, home)
    projection = note["runtime_projection"]
    artifacts = [
        _artifact(
            env_source, home / ".env", expected_env_sha256, projection["env_sha256"]
        ),
        _artifact(
            manifest_source,
            home / "runtime/LIVE_MANIFEST.json",
            expected_manifest_sha256,
            projection["live_manifest_sha256"],
        ),
    ]
    return {
        "schema_version": SCHEMA,
        "ok": True,
        "mode": "plan",
        "applied": False,
        "release_id": note["release_id"],
        "release_note": {"path": str(release_note), "sha256": _sha(note_raw)},
        "gitlab": gitlab_readback(note, runner),
        "runtime": runtime_readback(note, runner),
        "artifacts": artifacts,
        "activation": _activation_plan(note, binding, store_factory),
        "resident_profile": _profile_snapshot(runner),
        "single_canary": {
            "issue_id": note["canary"]["issue_id"],
            "state_path": note["canary"]["state_path"],
            "submitted_by_driver": False,
        },
    }


def _require_release_note_sha(path: Path, expected: str) -> None:
    try:
        observed = _sha(_read(path, "release_note", owner_only=True))
    except ReleaseError as exc:
        raise ReleaseError("release_note_changed") from exc
    if observed != expected:
        raise ReleaseError("release_note_changed")


def _live_projection_readback(home: Path) -> dict[str, str]:
    return {
        "env_sha256": _sha(_read(home / ".env", "live_env")),
        "live_manifest_sha256": _sha(
            _read(home / "runtime/LIVE_MANIFEST.json", "live_manifest")
        ),
    }


def _restore_installed_artifacts(staged: Sequence[dict[str, Any]]) -> None:
    for item in reversed(staged):
        if item.get("installed"):
            _restore_preimage(item)


def _validate_receipt_transition(
    transition: object, current_profile: Mapping[str, Any]
) -> dict:
    if not isinstance(transition, Mapping):
        raise ReleaseError("apply_receipt_resident_mismatch")
    current = _resident_identity_binding(current_profile)
    required = transition.get("required")
    disabled = transition.get("disabled")
    if (
        not isinstance(required, list)
        or not isinstance(disabled, list)
        or len(required) != len(REQUIRED_RESIDENTS)
        or len(disabled) != len(DISABLED_RESIDENTS)
    ):
        raise ReleaseError("apply_receipt_resident_mismatch")
    required_by_label = {
        str(item.get("label") or ""): item
        for item in required
        if isinstance(item, Mapping)
    }
    disabled_by_label = {
        str(item.get("label") or ""): item
        for item in disabled
        if isinstance(item, Mapping)
    }
    if set(required_by_label) != {row[0] for row in REQUIRED_RESIDENTS} or set(
        disabled_by_label
    ) != set(DISABLED_RESIDENTS):
        raise ReleaseError("apply_receipt_resident_mismatch")
    current_required = {item["label"]: item for item in current["required"]}
    for label, item in required_by_label.items():
        expected_keys = {
            "label",
            "pid",
            "cwd",
            "script",
            "process_create_time",
            "previous_loaded",
            "previous_pid",
            "new_pid",
        }
        previous_pid = item.get("previous_pid")
        if (
            set(item) != expected_keys
            or not isinstance(item.get("previous_loaded"), bool)
            or (
                previous_pid is not None
                and (
                    isinstance(previous_pid, bool)
                    or not isinstance(previous_pid, int)
                    or previous_pid <= 0
                )
            )
            or (item.get("previous_loaded") is False and previous_pid is not None)
            or item.get("new_pid") != current_required[label]["pid"]
            or item.get("pid") != item.get("new_pid")
            or previous_pid == item.get("new_pid")
            or {
                key: item.get(key)
                for key in (
                    "label",
                    "pid",
                    "cwd",
                    "script",
                    "process_create_time",
                )
            }
            != current_required[label]
        ):
            raise ReleaseError("apply_receipt_resident_mismatch")
    for label, item in disabled_by_label.items():
        if (
            set(item)
            != {
                "label",
                "previous_loaded",
                "previous_pid",
                "new_pid",
            }
            or not isinstance(item.get("previous_loaded"), bool)
            or (
                item.get("previous_pid") is not None
                and (
                    isinstance(item.get("previous_pid"), bool)
                    or not isinstance(item.get("previous_pid"), int)
                    or item.get("previous_pid") <= 0
                )
            )
            or (
                item.get("previous_loaded") is False
                and item.get("previous_pid") is not None
            )
            or item.get("new_pid") is not None
        ):
            raise ReleaseError("apply_receipt_resident_mismatch")
    if transition.get("persistent") != current["persistent"]:
        raise ReleaseError("apply_receipt_resident_mismatch")
    return dict(transition)


def _validate_receipt_profile(
    profile: object, current_profile: Mapping[str, Any]
) -> dict:
    if (
        not isinstance(profile, Mapping)
        or set(profile) != {"name", "required", "disabled", "persistent"}
        or profile.get("name") != "operator_issue_only_v1"
    ):
        raise ReleaseError("apply_receipt_resident_mismatch")
    try:
        recorded_binding = _resident_identity_binding(profile)
        current_binding = _resident_identity_binding(current_profile)
    except ReleaseError as exc:
        raise ReleaseError("apply_receipt_resident_mismatch") from exc
    if recorded_binding != current_binding:
        raise ReleaseError("apply_receipt_resident_mismatch")
    required = profile.get("required")
    disabled = profile.get("disabled")
    if not isinstance(required, list) or not isinstance(disabled, list):
        raise ReleaseError("apply_receipt_resident_mismatch")
    expected_freshness = {
        label: "restart_transition" if not schema else "health_heartbeat"
        for label, _script, _health, _field, schema in REQUIRED_RESIDENTS
    }
    for item in required:
        if not isinstance(item, Mapping):
            raise ReleaseError("apply_receipt_resident_mismatch")
        age = item.get("freshness_age_seconds")
        if (
            set(item)
            != {
                "label",
                "pid",
                "cwd",
                "script",
                "process_create_time",
                "freshness_kind",
                "freshness_at",
                "freshness_age_seconds",
            }
            or item.get("freshness_kind")
            != expected_freshness.get(str(item.get("label") or ""))
            or isinstance(age, bool)
            or not isinstance(age, (int, float))
            or not math.isfinite(float(age))
            or float(age) < -5
        ):
            raise ReleaseError("apply_receipt_resident_mismatch")
        try:
            fresh = datetime.fromisoformat(
                str(item.get("freshness_at") or "").replace("Z", "+00:00")
            )
            if fresh.tzinfo is None or fresh.utcoffset() is None:
                raise ValueError
        except (TypeError, ValueError) as exc:
            raise ReleaseError("apply_receipt_resident_mismatch") from exc
    if any(
        not isinstance(item, Mapping)
        or set(item) != {"label", "loaded", "pid"}
        for item in disabled
    ):
        raise ReleaseError("apply_receipt_resident_mismatch")
    return dict(profile)


def _validate_completed_apply_receipt(
    receipt_path: Path,
    receipt: Mapping[str, Any],
    *,
    release_note: Path,
    note: Mapping[str, Any],
    note_sha256: str,
    home: Path,
    gitlab: Mapping[str, Any],
    runtime: Mapping[str, Any],
    live_projection: Mapping[str, Any],
    activation: Mapping[str, Any],
    residents: Mapping[str, Any],
) -> dict:
    apply_pid = receipt.get("apply_pid")
    expected_keys = {
        "schema_version",
        "transaction_state",
        "ok",
        "mode",
        "applied",
        "apply_pid",
        "started_at",
        "completed_at",
        "applied_at",
        "release_id",
        "receipt_path",
        "release_note",
        "gitlab",
        "runtime",
        "live_projection",
        "artifacts",
        "activation",
        "activation_changed",
        "resident_profile",
        "resident_transition",
        "single_canary",
    }
    expected_canary = {
        "issue_id": note["canary"]["issue_id"],
        "state_path": note["canary"]["state_path"],
        "submitted_by_driver": False,
    }
    if (
        set(receipt) != expected_keys
        or receipt.get("schema_version") != RECEIPT_SCHEMA
        or receipt.get("transaction_state") != "completed"
        or receipt.get("ok") is not True
        or receipt.get("mode") != "apply"
        or receipt.get("applied") is not True
        or receipt.get("release_id") != note["release_id"]
        or receipt.get("receipt_path") != str(receipt_path)
        or receipt.get("release_note")
        != {"path": str(release_note), "sha256": note_sha256}
        or receipt.get("gitlab") != gitlab
        or receipt.get("runtime") != runtime
        or receipt.get("live_projection") != live_projection
        or receipt.get("activation") != activation
        or not isinstance(receipt.get("activation_changed"), bool)
        or receipt.get("single_canary") != expected_canary
        or isinstance(apply_pid, bool)
        or not isinstance(apply_pid, int)
        or apply_pid <= 0
        or not str(receipt.get("started_at") or "")
        or not str(receipt.get("completed_at") or "")
        or receipt.get("applied_at") != receipt.get("completed_at")
    ):
        raise ReleaseError("apply_receipt_binding_mismatch")
    try:
        started = datetime.fromisoformat(
            str(receipt["started_at"]).replace("Z", "+00:00")
        )
        completed = datetime.fromisoformat(
            str(receipt["completed_at"]).replace("Z", "+00:00")
        )
        if (
            started.tzinfo is None
            or started.utcoffset() is None
            or completed.tzinfo is None
            or completed.utcoffset() is None
            or completed < started
        ):
            raise ValueError
    except (TypeError, ValueError) as exc:
        raise ReleaseError("apply_receipt_binding_mismatch") from exc
    artifacts = receipt.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) != 2:
        raise ReleaseError("apply_receipt_binding_mismatch")
    artifact_by_target = {
        str(item.get("target") or ""): item
        for item in artifacts
        if isinstance(item, Mapping)
    }
    expected_artifacts = {
        str(home / ".env"): live_projection["env_sha256"],
        str(home / "runtime/LIVE_MANIFEST.json"): live_projection[
            "live_manifest_sha256"
        ],
    }
    if set(artifact_by_target) != set(expected_artifacts):
        raise ReleaseError("apply_receipt_binding_mismatch")
    for target, expected_after in expected_artifacts.items():
        item = artifact_by_target[target]
        before = item.get("before_sha256")
        changed = item.get("changed")
        if (
            set(item)
            != {"target", "before_sha256", "after_sha256", "changed"}
            or (before != "ABSENT" and not isinstance(before, str))
            or (
                isinstance(before, str)
                and before != "ABSENT"
                and HEX64.fullmatch(before) is None
            )
            or item.get("after_sha256") != expected_after
            or not isinstance(changed, bool)
            or changed != (before != expected_after)
        ):
            raise ReleaseError("apply_receipt_binding_mismatch")
    _validate_receipt_profile(receipt.get("resident_profile"), residents)
    _validate_receipt_transition(receipt.get("resident_transition"), residents)
    return dict(receipt)


def apply_release(
    *,
    release_note: Path,
    manifest_source: Path,
    env_source: Path,
    expected_manifest_sha256: str,
    expected_env_sha256: str,
    home: Path,
    confirm_release_id: str,
    receipt: Path | None = None,
    runner: Runner = _run,
    process_factory: ProcessFactory = psutil.Process,
    store_factory: StoreFactory = _open_store,
    restart_timeout: float = 60,
) -> dict:
    release_note = _absolute(release_note, "release_note_path_invalid")
    home = _absolute(home, "hermes_home_invalid")
    note_raw, note = _load_note(release_note, home)
    note["_path"] = str(release_note)
    if confirm_release_id != note["release_id"]:
        raise ReleaseError("apply_confirmation_mismatch")
    if isinstance(restart_timeout, bool):
        raise ReleaseError("restart_timeout_invalid")
    try:
        restart_timeout = float(restart_timeout)
    except (TypeError, ValueError) as exc:
        raise ReleaseError("restart_timeout_invalid") from exc
    if not math.isfinite(restart_timeout) or restart_timeout <= 0:
        raise ReleaseError("restart_timeout_invalid")
    receipt = _absolute(
        receipt
        or release_note.with_name(f"{note['release_id']}.minimal-release-apply.json"),
        "receipt_path_invalid",
    )
    if receipt.parent != release_note.parent:
        raise ReleaseError("receipt_path_invalid")
    note_sha = _sha(note_raw)
    lock = _acquire_release_lock(home, note["release_id"])
    receipt_handle: tuple[Path, int, tuple[int, int]] | None = None
    staged: list[dict[str, Any]] = []
    installs: list[dict[str, Any]] = []
    quiesce: dict[str, Any] | None = None
    artifacts_installed = False
    activation_attempted = False
    activation_committed = False
    activation_outcome_known = True
    artifacts_restored = False
    retain_staged = False
    staged_cleaned = False
    effect_started = False
    signal_handlers: dict[int, Any] = {}
    started_at = datetime.now(timezone.utc).isoformat()
    try:
        started = {
            "schema_version": RECEIPT_SCHEMA,
            "transaction_state": "started",
            "ok": False,
            "mode": "apply",
            "applied": False,
            "apply_pid": os.getpid(),
            "started_at": started_at,
            "release_id": note["release_id"],
            "receipt_path": str(receipt),
            "release_note": {
                "path": str(release_note),
                "sha256": note_sha,
            },
        }
        receipt_handle = _reserve_apply_receipt(receipt, started)
        signal_handlers = _install_apply_signal_handlers()
        plan = build_plan(
            release_note=release_note,
            manifest_source=manifest_source,
            env_source=env_source,
            expected_manifest_sha256=expected_manifest_sha256,
            expected_env_sha256=expected_env_sha256,
            home=home,
            runner=runner,
            store_factory=store_factory,
        )
        if plan["release_note"]["sha256"] != note_sha:
            raise ReleaseError("release_note_changed")
        _require_release_note_sha(release_note, note_sha)
        staged = _stage_artifacts(plan["artifacts"])
        before_effect_gitlab = gitlab_readback(note, runner)
        before_effect_runtime = runtime_readback(note, runner)
        if before_effect_gitlab != plan["gitlab"]:
            raise ReleaseError("gitlab_changed_during_apply")
        if before_effect_runtime != plan["runtime"]:
            raise ReleaseError("runtime_changed_during_apply")
        _require_release_note_sha(release_note, note_sha)
        _write_reserved_receipt(
            receipt_handle,
            {
                **started,
                "gitlab": before_effect_gitlab,
                "runtime": before_effect_runtime,
            },
        )
        effect_started = True
        quiesce = _quiesce_residents(runner)
        _require_release_note_sha(release_note, note_sha)
        installs = _install_staged(staged)
        artifacts_installed = True
        _require_release_note_sha(release_note, note_sha)
        _assert_all_residents_stopped(runner)
        binding = _bound_binding(note_raw, note)
        activation_attempted = True
        activation_outcome_known = False
        try:
            activation_apply = _activation_apply(
                note, binding, plan["activation"], store_factory
            )
        except BaseException:
            try:
                _activation_status(note, binding, store_factory)
            except ReleaseError as status_exc:
                if status_exc.code == "activation_not_steady":
                    activation_outcome_known = True
            else:
                activation_committed = True
                activation_outcome_known = True
            raise
        activation_committed = True
        activation_outcome_known = True
        activation = _activation_status(note, binding, store_factory)
        if activation_apply["current_epoch"] != activation:
            raise ReleaseError("activation_changed_during_apply")
        residents = _restart(
            note,
            note_sha,
            home,
            runner,
            process_factory,
            restart_timeout,
            quiesce,
        )
        first_resident_binding = _resident_identity_binding(residents)
        _require_release_note_sha(release_note, note_sha)
        closeout_live = _live_projection_readback(home)
        if closeout_live != note["runtime_projection"]:
            raise ReleaseError("live_projection_changed_during_apply")
        closeout_gitlab = gitlab_readback(note, runner)
        closeout_runtime = runtime_readback(note, runner)
        if closeout_gitlab != plan["gitlab"]:
            raise ReleaseError("gitlab_changed_during_apply")
        if closeout_runtime != plan["runtime"]:
            raise ReleaseError("runtime_changed_during_apply")
        closeout_activation = _activation_status(note, binding, store_factory)
        if closeout_activation != activation:
            raise ReleaseError("activation_changed_during_apply")
        closeout_residents = _resident_profile_readback(
            note,
            note_sha,
            home,
            runner=runner,
            process_factory=process_factory,
        )
        if _resident_identity_binding(closeout_residents) != first_resident_binding:
            raise ReleaseError("resident_changed_during_apply")
        _require_release_note_sha(release_note, note_sha)
        resident_transition = _resident_transition(quiesce, closeout_residents)
        _cleanup_staged(staged)
        staged_cleaned = True
        completed_at = datetime.now(timezone.utc).isoformat()
        result = {
            "schema_version": RECEIPT_SCHEMA,
            "transaction_state": "completed",
            "ok": True,
            "mode": "apply",
            "applied": True,
            "apply_pid": os.getpid(),
            "started_at": started_at,
            "completed_at": completed_at,
            "applied_at": completed_at,
            "release_id": note["release_id"],
            "receipt_path": str(receipt),
            "release_note": plan["release_note"],
            "gitlab": closeout_gitlab,
            "runtime": closeout_runtime,
            "live_projection": closeout_live,
            "artifacts": installs,
            "activation": closeout_activation,
            "activation_changed": activation_apply["changed"],
            "resident_profile": closeout_residents,
            "resident_transition": resident_transition,
            "single_canary": plan["single_canary"],
        }
        _write_reserved_receipt(receipt_handle, result)
        return result
    except BaseException as exc:
        _ignore_apply_signals(signal_handlers)
        if receipt_handle is not None:
            stop = (
                {"attempted": True, **_stop_all_residents(runner)}
                if effect_started
                else {"attempted": False, "all_stopped": None, "observed": [], "errors": []}
            )
            rollback_error = ""
            artifacts_mutated = any(item.get("installed") for item in staged)
            rollback_safe = not activation_attempted or (
                activation_outcome_known and not activation_committed
            )
            if artifacts_mutated and rollback_safe:
                if stop["all_stopped"] is True:
                    try:
                        _restore_installed_artifacts(staged)
                        artifacts_restored = True
                    except ReleaseError as rollback_exc:
                        rollback_error = rollback_exc.code
                        retain_staged = True
                else:
                    rollback_error = "resident_stop_incomplete"
                    retain_staged = True
            elif artifacts_mutated and not activation_committed:
                rollback_error = "activation_outcome_unknown"
                retain_staged = True
            if retain_staged:
                try:
                    _cleanup_staged_candidates(staged)
                except (OSError, ReleaseError):
                    pass
            failed = {
                "schema_version": RECEIPT_SCHEMA,
                "transaction_state": "failed",
                "ok": False,
                "mode": "apply",
                "applied": False,
                "apply_pid": os.getpid(),
                "started_at": started_at,
                "failed_at": datetime.now(timezone.utc).isoformat(),
                "release_id": note["release_id"],
                "receipt_path": str(receipt),
                "release_note": {
                    "path": str(release_note),
                    "sha256": _sha(note_raw),
                },
                "error_code": (
                    exc.code if isinstance(exc, ReleaseError) else "apply_internal_error"
                ),
                "activation_attempted": activation_attempted,
                "activation_committed": activation_committed,
                "activation_outcome_known": activation_outcome_known,
                "effect_started": effect_started,
                "artifacts_installed": artifacts_installed,
                "artifacts_mutated": artifacts_mutated,
                "artifacts_restored": artifacts_restored,
                "rollback_error": rollback_error,
                "rollback_recovery_paths": [
                    str(item["preimage"])
                    for item in staged
                    if retain_staged and str(item.get("preimage") or "")
                ],
                "resident_stop": stop,
            }
            try:
                _write_reserved_receipt(receipt_handle, failed)
            except ReleaseError as receipt_exc:
                try:
                    _write_terminal_failure_marker(receipt, failed, receipt_exc)
                except ReleaseError as marker_exc:
                    raise ReleaseError(
                        "terminal_receipt_and_marker_write_failed"
                    ) from marker_exc
                raise ReleaseError("terminal_receipt_write_failed") from receipt_exc
        raise
    finally:
        try:
            _restore_apply_signal_handlers(signal_handlers)
        except BaseException:
            pass
        if not retain_staged and not staged_cleaned:
            try:
                _cleanup_staged(staged)
            except BaseException:
                pass
        elif retain_staged:
            try:
                _cleanup_staged_candidates(staged)
            except BaseException:
                pass
        try:
            _close_apply_receipt(receipt_handle)
        except BaseException:
            pass
        try:
            _release_release_lock(lock)
        except BaseException:
            pass


def verify_release(
    *,
    release_note: Path,
    apply_receipt: Path,
    home: Path,
    runner: Runner = _run,
    process_factory: ProcessFactory = psutil.Process,
    store_factory: StoreFactory = _open_store,
    delivery_store_factory: DeliveryStoreFactory = _open_delivery_store,
    now: datetime | None = None,
) -> dict:
    release_note = _absolute(release_note, "release_note_path_invalid")
    apply_receipt = _absolute(apply_receipt, "apply_receipt_path_invalid")
    if apply_receipt.parent != release_note.parent:
        raise ReleaseError("apply_receipt_path_invalid")
    home = _absolute(home, "hermes_home_invalid")
    note_raw, note = _load_note(release_note, home)
    note["_path"] = str(release_note)
    note_sha256 = _sha(note_raw)
    projection = note["runtime_projection"]
    lock = _acquire_release_lock(home, note["release_id"])
    try:
        _require_release_note_sha(release_note, note_sha256)
        live = _live_projection_readback(home)
        if live != projection:
            raise ReleaseError("live_projection_mismatch")
        binding = _bound_binding(note_raw, note)
        gitlab = gitlab_readback(note, runner)
        runtime = runtime_readback(note, runner)
        activation = _activation_status(note, binding, store_factory)
        residents = _resident_profile_readback(
            note,
            note_sha256,
            home,
            runner=runner,
            process_factory=process_factory,
            now=now,
        )
        receipt_raw, receipt = _read_apply_receipt(apply_receipt)
        _validate_completed_apply_receipt(
            apply_receipt,
            receipt,
            release_note=release_note,
            note=note,
            note_sha256=note_sha256,
            home=home,
            gitlab=gitlab,
            runtime=runtime,
            live_projection=live,
            activation=activation,
            residents=residents,
        )
        canary = canary_readback(
            note,
            note_sha256,
            delivery_store_factory=delivery_store_factory,
        )

        _require_release_note_sha(release_note, note_sha256)
        closeout_live = _live_projection_readback(home)
        if closeout_live != live or closeout_live != projection:
            raise ReleaseError("live_projection_changed_during_verify")
        closeout_gitlab = gitlab_readback(note, runner)
        if closeout_gitlab != gitlab:
            raise ReleaseError("gitlab_changed_during_verify")
        closeout_runtime = runtime_readback(note, runner)
        if closeout_runtime != runtime:
            raise ReleaseError("runtime_changed_during_verify")
        closeout_activation = _activation_status(note, binding, store_factory)
        if closeout_activation != activation:
            raise ReleaseError("activation_changed_during_verify")
        closeout_residents = _resident_profile_readback(
            note,
            note_sha256,
            home,
            runner=runner,
            process_factory=process_factory,
            now=now,
        )
        if _resident_identity_binding(closeout_residents) != _resident_identity_binding(
            residents
        ):
            raise ReleaseError("resident_changed_during_verify")
        closeout_receipt_raw, closeout_receipt = _read_apply_receipt(apply_receipt)
        if closeout_receipt_raw != receipt_raw or closeout_receipt != receipt:
            raise ReleaseError("apply_receipt_changed_during_verify")
        _validate_completed_apply_receipt(
            apply_receipt,
            closeout_receipt,
            release_note=release_note,
            note=note,
            note_sha256=note_sha256,
            home=home,
            gitlab=closeout_gitlab,
            runtime=closeout_runtime,
            live_projection=closeout_live,
            activation=closeout_activation,
            residents=closeout_residents,
        )
        _require_release_note_sha(release_note, note_sha256)
        return {
            "schema_version": SCHEMA,
            "ok": True,
            "mode": "verify",
            "applied": False,
            "release_id": note["release_id"],
            "release_note": {"path": str(release_note), "sha256": note_sha256},
            "apply_receipt": {
                "path": str(apply_receipt),
                "sha256": _sha(closeout_receipt_raw),
            },
            "gitlab": closeout_gitlab,
            "runtime": closeout_runtime,
            "live_projection": closeout_live,
            "activation": closeout_activation,
            "resident_profile": closeout_residents,
            "single_canary": canary,
        }
    finally:
        _release_release_lock(lock)


def _partition_topic_arguments(values: Sequence[str]) -> dict[str, tuple[int, ...]]:
    result: dict[str, tuple[int, ...]] = {}
    for value in values:
        topic, separator, raw_partitions = str(value).partition("=")
        parts = raw_partitions.split(",") if separator else []
        if (
            not topic
            or topic in result
            or not parts
            or any(not part.isdigit() for part in parts)
        ):
            raise ReleaseError("prepare_partition_topics_invalid")
        result[topic] = tuple(int(part) for part in parts)
    return _normalize_partition_topics(result)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    def common(target: argparse.ArgumentParser) -> None:
        target.add_argument("--release-note", type=Path, required=True)
        target.add_argument("--hermes-home", type=Path, default=Path.home() / ".hermes")

    prepare = commands.add_parser("prepare")
    common(prepare)
    prepare.add_argument("--release-id", required=True)
    prepare.add_argument("--epoch-id", required=True)
    prepare.add_argument("--operator", required=True)
    prepare.add_argument("--reason", required=True)
    prepare.add_argument("--canary-batch-id", required=True)
    prepare.add_argument("--canary-issue-id", required=True)
    prepare.add_argument("--canary-state-path", type=Path, required=True)
    prepare.add_argument("--host-branch", required=True)
    prepare.add_argument("--host-tag", required=True)
    prepare.add_argument("--host-runtime-root", type=Path, required=True)
    prepare.add_argument("--worker-remote", required=True)
    prepare.add_argument("--worker-branch", required=True)
    prepare.add_argument("--worker-tag", required=True)
    prepare.add_argument("--worker-runtime-root", type=Path, required=True)
    prepare.add_argument("--pipeline-remote", required=True)
    prepare.add_argument("--pipeline-branch", required=True)
    prepare.add_argument("--pipeline-tag", required=True)
    prepare.add_argument("--pipeline-runtime-root", type=Path, required=True)
    prepare.add_argument("--report-manifest-path", type=Path, required=True)
    prepare.add_argument(
        "--partition-topic",
        action="append",
        default=[],
        metavar="TOPIC=PARTITION[,PARTITION...]",
    )
    prepare.add_argument("--control-db", type=Path, required=True)
    prepare.add_argument("--manifest-output", type=Path, required=True)
    prepare.add_argument("--env-output", type=Path, required=True)

    for name in ("plan", "apply"):
        target = commands.add_parser(name)
        common(target)
        target.add_argument("--manifest-source", type=Path, required=True)
        target.add_argument("--env-source", type=Path, required=True)
        target.add_argument("--expected-manifest-sha256", required=True)
        target.add_argument("--expected-env-sha256", required=True)
        if name == "apply":
            target.add_argument("--confirm-release-id", required=True)
            target.add_argument("--receipt", type=Path)
            target.add_argument("--restart-timeout", type=float, default=60)
    verify = commands.add_parser("verify")
    common(verify)
    verify.add_argument("--apply-receipt", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    command = ""
    try:
        args = _parser().parse_args(argv)
        command = args.command
        common = {"release_note": args.release_note, "home": args.hermes_home}
        if command == "prepare":
            result = prepare_release(
                **common,
                release_id=args.release_id,
                epoch_id=args.epoch_id,
                operator=args.operator,
                reason=args.reason,
                canary_batch_id=args.canary_batch_id,
                canary_issue_id=args.canary_issue_id,
                canary_state_path=args.canary_state_path,
                host_branch=args.host_branch,
                host_tag=args.host_tag,
                host_runtime_root=args.host_runtime_root,
                worker_remote=args.worker_remote,
                worker_branch=args.worker_branch,
                worker_tag=args.worker_tag,
                worker_runtime_root=args.worker_runtime_root,
                pipeline_remote=args.pipeline_remote,
                pipeline_branch=args.pipeline_branch,
                pipeline_tag=args.pipeline_tag,
                pipeline_runtime_root=args.pipeline_runtime_root,
                report_manifest_path=args.report_manifest_path,
                partition_topics=_partition_topic_arguments(args.partition_topic),
                control_db=args.control_db,
                manifest_output=args.manifest_output,
                env_output=args.env_output,
            )
        elif command == "verify":
            result = verify_release(**common, apply_receipt=args.apply_receipt)
        else:
            inputs = {
                **common,
                "manifest_source": args.manifest_source,
                "env_source": args.env_source,
                "expected_manifest_sha256": args.expected_manifest_sha256,
                "expected_env_sha256": args.expected_env_sha256,
            }
            result = (
                build_plan(**inputs)
                if command == "plan"
                else apply_release(
                    **inputs,
                    confirm_release_id=args.confirm_release_id,
                    receipt=args.receipt,
                    restart_timeout=args.restart_timeout,
                )
            )
    except ReleaseError as exc:
        result = {
            "schema_version": SCHEMA,
            "ok": False,
            "command": command,
            "code": exc.code,
        }
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 2
    print(json.dumps(result, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
