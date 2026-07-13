"""Immutable process identity evidence for resident RCA services."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import importlib
from importlib import metadata
import json
import os
from pathlib import Path
from pathlib import PurePosixPath
import math
import re
import stat
import sys
from typing import Any, Mapping, Sequence

import psutil


RCA_RUNTIME_RELATIVE_FILES = (
    "gateway/feishu_task_card.py",
    "gateway/pnc_issue_capture.py",
    "gateway/pnc_issue_context.py",
    "gateway/pnc_pdcl_contract.py",
    "gateway/pnc_rca_admission.py",
    "gateway/pnc_rca_capacity_runtime.py",
    "gateway/pnc_rca_capacity_sample_evidence.py",
    "gateway/pnc_rca_capacity_transition.py",
    "gateway/pnc_rca_control_store.py",
    "gateway/pnc_rca_data_access.py",
    "gateway/pnc_rca_derived_capacity_reservation.py",
    "gateway/pnc_rca_delivery_contract.py",
    "gateway/pnc_rca_delivery_store.py",
    "gateway/pnc_rca_kafka_contract.py",
    "gateway/pnc_rca_prod_admission.py",
    "gateway/pnc_rca_prod_bootstrap.py",
    "gateway/pnc_rca_runtime_identity.py",
    "gateway/pnc_rca_runtime_transition.py",
    "gateway/pnc_rca_schema.py",
    "gateway/pnc_rca_stage_lineage.py",
    "gateway/pnc_rca_workspace_runtime.py",
    "gateway/session_context.py",
    "hermes_constants.py",
    "scripts/pnc_g1q3_truth.py",
    "scripts/pnc_rca_activation.py",
    "scripts/pnc_rca_capacity_transition_executor.py",
    "scripts/pnc_rca_delivery_collector.py",
    "scripts/pnc_rca_delivery_dispatcher.py",
    "scripts/pnc_rca_kafka_consumer.py",
    "scripts/pnc_rca_outbox_dispatcher.py",
    "tools/permission_policy.py",
    "tools/registry.py",
    "tools/vm_task_tool.py",
)
DELIVERY_RUNTIME_RELATIVE_FILES = RCA_RUNTIME_RELATIVE_FILES
GATEWAY_RCA_RUNTIME_RELATIVE_FILES = (
    "hermes_cli/main.py",
    "gateway/run.py",
    "gateway/pnc_group_binding.py",
    "gateway/platforms/feishu.py",
    "gateway/pnc_rca_runtime_identity.py",
    "gateway/pnc_rca_runtime_transition.py",
    "gateway/pnc_rca_control_store.py",
    "gateway/pnc_rca_admission.py",
    "gateway/pnc_rca_kafka_contract.py",
    "gateway/pnc_rca_policy_config.py",
    "gateway/pnc_issue_context.py",
    "gateway/pnc_rca_schema.py",
    "gateway/pnc_rca_data_access.py",
    "gateway/pnc_pdcl_contract.py",
    "gateway/pnc_rca_derived_capacity_reservation.py",
)
MAX_HEALTH_FUTURE_SKEW_SECONDS = 30
MAX_RUNTIME_FILE_BYTES = 32 * 1024 * 1024
GATEWAY_LOADED_DEPENDENCIES = {
    "psutil": "psutil",
    "python-dotenv": "dotenv",
}
RCA_KAFKA_CONSUMER_LOADED_DEPENDENCIES = {
    **GATEWAY_LOADED_DEPENDENCIES,
    "kafka-python": "kafka",
    "python-snappy": "snappy",
}
RCA_OUTBOX_DISPATCHER_LOADED_DEPENDENCIES = dict(GATEWAY_LOADED_DEPENDENCIES)
RCA_DELIVERY_COLLECTOR_LOADED_DEPENDENCIES = {
    **GATEWAY_LOADED_DEPENDENCIES,
    "tinycss2": "tinycss2",
}
RCA_DELIVERY_DISPATCHER_LOADED_DEPENDENCIES = {
    **GATEWAY_LOADED_DEPENDENCIES,
    "lark-oapi": "lark_oapi",
}
RCA_LOADED_DEPENDENCIES = {
    **RCA_KAFKA_CONSUMER_LOADED_DEPENDENCIES,
    **RCA_DELIVERY_COLLECTOR_LOADED_DEPENDENCIES,
    **RCA_DELIVERY_DISPATCHER_LOADED_DEPENDENCIES,
}
RCA_LOADED_DEPENDENCIES_BY_SERVICE = {
    "local.pnc.rca-kafka-consumer": RCA_KAFKA_CONSUMER_LOADED_DEPENDENCIES,
    "local.pnc.rca-outbox-dispatcher": RCA_OUTBOX_DISPATCHER_LOADED_DEPENDENCIES,
    "local.pnc.rca-delivery-collector": RCA_DELIVERY_COLLECTOR_LOADED_DEPENDENCIES,
    "local.pnc.rca-delivery-dispatcher": RCA_DELIVERY_DISPATCHER_LOADED_DEPENDENCIES,
}
RUNTIME_IDENTITY_FIELDS = frozenset({
    "service_label",
    "pid",
    "process_create_time",
    "boot_time",
    "executable",
    "script",
    "cwd",
    "script_sha256",
    "runtime_files_sha256",
    "public_config_sha256",
    "loaded_runtime_sha256",
})
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def canonical_json_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def runtime_identity_is_valid(
    value: Any,
    *,
    service_label: str | None = None,
    public_config: Mapping[str, Any] | None = None,
) -> bool:
    """Validate the exact immutable identity contract emitted by RCA residents."""
    if not isinstance(value, Mapping) or set(value) != RUNTIME_IDENTITY_FIELDS:
        return False
    if (
        not isinstance(value.get("service_label"), str)
        or not value["service_label"]
        or (service_label is not None and value["service_label"] != service_label)
    ):
        return False
    pid = value.get("pid")
    if isinstance(pid, bool) or not isinstance(pid, int) or pid < 1:
        return False
    timeline: dict[str, float] = {}
    for field in ("process_create_time", "boot_time"):
        raw = value.get(field)
        if (
            isinstance(raw, bool)
            or not isinstance(raw, (int, float))
            or not math.isfinite(float(raw))
            or float(raw) <= 0
        ):
            return False
        timeline[field] = float(raw)
    if timeline["process_create_time"] < timeline["boot_time"]:
        return False
    for field in ("executable", "script", "cwd"):
        raw = value.get(field)
        if not isinstance(raw, str) or not raw.startswith("/") or "\x00" in raw:
            return False
    for field in (
        "script_sha256",
        "runtime_files_sha256",
        "public_config_sha256",
        "loaded_runtime_sha256",
    ):
        if _SHA256_RE.fullmatch(str(value.get(field) or "")) is None:
            return False
    return public_config is None or value.get(
        "public_config_sha256"
    ) == canonical_json_sha256(dict(public_config))


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    descriptor = -1
    try:
        initial = os.lstat(path)
        if (
            stat.S_ISLNK(initial.st_mode)
            or not stat.S_ISREG(initial.st_mode)
            or initial.st_size > MAX_RUNTIME_FILE_BYTES
        ):
            raise OSError("runtime file is not a bounded regular file")
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns)
            != (initial.st_dev, initial.st_ino, initial.st_size, initial.st_mtime_ns)
        ):
            raise OSError("runtime file changed before hashing")
        remaining = opened.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise OSError("runtime file was truncated while hashing")
            digest.update(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise OSError("runtime file grew while hashing")
        final = os.fstat(descriptor)
        final_path = os.lstat(path)
        expected = (initial.st_dev, initial.st_ino, initial.st_size, initial.st_mtime_ns)
        if (
            (final.st_dev, final.st_ino, final.st_size, final.st_mtime_ns) != expected
            or (
                final_path.st_dev,
                final_path.st_ino,
                final_path.st_size,
                final_path.st_mtime_ns,
            )
            != expected
        ):
            raise OSError("runtime file changed while hashing")
        return digest.hexdigest()
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _runtime_relative_parts(relative: str) -> tuple[str, ...]:
    value = PurePosixPath(str(relative or ""))
    if (
        value.is_absolute()
        or not value.parts
        or any(part in {"", ".", ".."} for part in value.parts)
    ):
        raise FileNotFoundError(f"unsafe RCA runtime file path: {relative!r}")
    return tuple(value.parts)


def _runtime_file_sha256_at(root_fd: int, relative: str) -> str:
    parts = _runtime_relative_parts(relative)
    opened_directories: list[int] = []
    parent_fd = root_fd
    descriptor = -1
    try:
        for part in parts[:-1]:
            parent_fd = os.open(
                part,
                os.O_RDONLY
                | getattr(os, "O_DIRECTORY", 0)
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=parent_fd,
            )
            opened_directories.append(parent_fd)
        descriptor = os.open(
            parts[-1],
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=parent_fd,
        )
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_size > MAX_RUNTIME_FILE_BYTES
        ):
            raise FileNotFoundError(f"RCA runtime file is not regular: {relative}")
        digest = hashlib.sha256()
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise OSError("runtime file was truncated while hashing")
            digest.update(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise OSError("runtime file grew while hashing")
        after = os.fstat(descriptor)
        if (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ) != (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        ):
            raise OSError("runtime file changed while hashing")
        return digest.hexdigest()
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        for directory_fd in reversed(opened_directories):
            os.close(directory_fd)


def runtime_file_snapshot(
    repo_root: str | Path,
    relative_files: Sequence[str],
) -> tuple[dict[str, str], str]:
    configured_root = Path(repo_root).expanduser().absolute()
    root_info = os.lstat(configured_root)
    if (
        stat.S_ISLNK(root_info.st_mode)
        or not stat.S_ISDIR(root_info.st_mode)
        or configured_root.resolve(strict=True) != configured_root
    ):
        raise FileNotFoundError("RCA runtime root is not canonical")
    root_fd = os.open(
        configured_root,
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        file_hashes = {
            relative: _runtime_file_sha256_at(root_fd, relative)
            for relative in relative_files
        }
    finally:
        os.close(root_fd)
    return file_hashes, canonical_json_sha256(file_hashes)


def runtime_file_hashes(
    repo_root: str | Path,
    relative_files: Sequence[str],
) -> dict[str, str]:
    return runtime_file_snapshot(repo_root, relative_files)[0]


def runtime_files_sha256(
    repo_root: str | Path,
    relative_files: Sequence[str],
) -> str:
    return runtime_file_snapshot(repo_root, relative_files)[1]


def rca_runtime_file_hashes(repo_root: str | Path) -> dict[str, str]:
    return runtime_file_hashes(repo_root, RCA_RUNTIME_RELATIVE_FILES)


def rca_runtime_files_sha256(repo_root: str | Path) -> str:
    return runtime_files_sha256(repo_root, RCA_RUNTIME_RELATIVE_FILES)


def gateway_rca_runtime_file_hashes(repo_root: str | Path) -> dict[str, str]:
    return runtime_file_hashes(repo_root, GATEWAY_RCA_RUNTIME_RELATIVE_FILES)


def gateway_rca_runtime_files_sha256(repo_root: str | Path) -> str:
    return runtime_files_sha256(repo_root, GATEWAY_RCA_RUNTIME_RELATIVE_FILES)


def loaded_runtime_snapshot(
    dependencies: Mapping[str, str],
) -> dict[str, Any]:
    process_executable = Path(psutil.Process().exe()).resolve(strict=True)
    sys_executable = Path(sys.executable).resolve(strict=True)
    loaded_dependencies: dict[str, dict[str, str]] = {}
    for distribution, module_name in sorted(dependencies.items()):
        if not distribution or not module_name:
            raise ValueError("loaded dependency names must be non-empty")
        module = importlib.import_module(module_name)
        origin_value = getattr(module, "__file__", None)
        if not origin_value:
            raise RuntimeError(f"loaded dependency origin missing: {module_name}")
        origin = Path(origin_value).resolve(strict=True)
        loaded_dependencies[distribution] = {
            "module": module_name,
            "origin": str(origin),
            "sha256": file_sha256(origin),
            "version": metadata.version(distribution),
        }
    return {
        "sys_executable": str(sys_executable),
        "sys_executable_sha256": file_sha256(sys_executable),
        "process_executable": str(process_executable),
        "process_executable_sha256": file_sha256(process_executable),
        "dependencies": loaded_dependencies,
    }


def loaded_runtime_sha256(dependencies: Mapping[str, str]) -> str:
    return canonical_json_sha256(loaded_runtime_snapshot(dependencies))


def gateway_loaded_runtime_sha256() -> str:
    return loaded_runtime_sha256(GATEWAY_LOADED_DEPENDENCIES)


def delivery_runtime_files_sha256(repo_root: str | Path) -> str:
    return rca_runtime_files_sha256(repo_root)


@dataclass(frozen=True)
class RuntimeIdentity:
    service_label: str
    pid: int
    process_create_time: float
    boot_time: float
    executable: str
    script: str
    cwd: str
    script_sha256: str
    runtime_files_sha256: str
    public_config_sha256: str
    loaded_runtime_sha256: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_runtime_identity(
    *,
    service_label: str,
    script_path: str | Path,
    public_config: Mapping[str, Any],
    runtime_relative_files: Sequence[str] = RCA_RUNTIME_RELATIVE_FILES,
    loaded_dependencies: Mapping[str, str] = RCA_LOADED_DEPENDENCIES,
) -> RuntimeIdentity:
    """Capture identity once so a heartbeat cannot drift across processes/builds."""
    process = psutil.Process(os.getpid())
    script = Path(script_path).expanduser().resolve(strict=True)
    repo_root = script.parent.parent
    executable = Path(process.exe()).expanduser().resolve(strict=True)
    cwd = Path(process.cwd()).expanduser().resolve(strict=True)
    return RuntimeIdentity(
        service_label=service_label,
        pid=process.pid,
        process_create_time=float(process.create_time()),
        boot_time=float(psutil.boot_time()),
        executable=str(executable),
        script=str(script),
        cwd=str(cwd),
        script_sha256=file_sha256(script),
        runtime_files_sha256=runtime_files_sha256(repo_root, runtime_relative_files),
        public_config_sha256=canonical_json_sha256(dict(public_config)),
        loaded_runtime_sha256=loaded_runtime_sha256(loaded_dependencies),
    )
