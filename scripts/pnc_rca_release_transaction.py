#!/usr/bin/env python3
"""Plan, apply, and roll back one authority-bound RCA host projection."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import plistlib
import re
import stat
import subprocess
import tempfile
from typing import Any, Mapping, Sequence
import uuid

from gateway import pnc_rca_prod_bootstrap as bootstrap
from gateway import pnc_rca_release_authority as authority


PLAN_SCHEMA_VERSION = "pnc_rca_release_transaction_plan_v1"
RECEIPT_SCHEMA_VERSION = "pnc_rca_release_transaction_receipt_v1"
ROLLBACK_SCHEMA_VERSION = "pnc_rca_release_transaction_rollback_v1"
CLI_SCHEMA_VERSION = "pnc_rca_release_transaction_cli_v1"
PLIST_NAMES = (
    "local.pnc.vm-task-sync.plist",
    "local.pnc.completion-notice-relay.plist",
    "local.pnc.rca-delivery-collector.plist",
    "local.pnc.rca-delivery-dispatcher.plist",
)
RELEASE_SCOPED_PLIST_KEYS = frozenset(
    {
        "HERMES_RCA_DELIVERY_QUARANTINE_BASELINE_SHA256",
        "HERMES_RCA_DELIVERY_DISPATCHER_INVENTORY_PIN",
        "HERMES_RCA_DELIVERY_DISPATCHER_OBSERVABILITY_ENABLED",
        "HERMES_RCA_DELIVERY_DISPATCHER_OBSERVABILITY_PATH",
        "HERMES_RCA_DELIVERY_DISPATCHER_OBSERVATION_RELEASE_ID",
    }
)
HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
RELEASE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
MAX_FILE_BYTES = 8 * 1024 * 1024
MAX_JSON_BYTES = 8 * 1024 * 1024
TARGET_MODE = {
    "authority": 0o400,
    "pointer": 0o600,
    "manifest": 0o600,
    "binding": 0o600,
    "env": 0o600,
    "bootstrap_authorization": 0o600,
    "plist": 0o644,
}


class ReleaseTransactionError(RuntimeError):
    def __init__(self, code: str, detail: str = "") -> None:
        self.code = str(code or "pnc_rca_release_transaction_invalid")[:160]
        self.detail = str(detail or self.code)[:1000]
        super().__init__(self.code)


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _absolute(path: Path, code: str) -> Path:
    selected = path.expanduser()
    if not selected.is_absolute() or selected.absolute() != selected:
        raise ReleaseTransactionError(code)
    return selected


def _directory(path: Path, *, create: bool = False) -> Path:
    selected = _absolute(path, "pnc_release_transaction_directory_invalid")
    if create:
        try:
            selected.mkdir(mode=0o700, parents=True, exist_ok=True)
        except OSError as exc:
            raise ReleaseTransactionError(
                "pnc_release_transaction_directory_invalid"
            ) from exc
    try:
        metadata = selected.lstat()
        resolved = selected.resolve(strict=True)
    except OSError as exc:
        raise ReleaseTransactionError(
            "pnc_release_transaction_directory_invalid"
        ) from exc
    if (
        resolved != selected
        or stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) & 0o022
    ):
        raise ReleaseTransactionError("pnc_release_transaction_directory_invalid")
    return selected


def _identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _read_file(
    path: Path,
    *,
    code: str,
    maximum: int = MAX_FILE_BYTES,
    required_mode: int | None = None,
) -> tuple[bytes, dict[str, Any]]:
    selected = _absolute(path, code)
    descriptor = -1
    try:
        before = selected.lstat()
        if (
            stat.S_ISLNK(before.st_mode)
            or not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
            or before.st_size < 1
            or before.st_size > maximum
            or (
                required_mode is not None
                and stat.S_IMODE(before.st_mode) != required_mode
            )
        ):
            raise ReleaseTransactionError(code)
        descriptor = os.open(
            selected,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        opened = os.fstat(descriptor)
        if _identity(opened) != _identity(before):
            raise ReleaseTransactionError(f"{code}_changed")
        chunks: list[bytes] = []
        remaining = opened.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise ReleaseTransactionError(f"{code}_changed")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1) or _identity(os.fstat(descriptor)) != _identity(before):
            raise ReleaseTransactionError(f"{code}_changed")
        if _identity(selected.lstat()) != _identity(before):
            raise ReleaseTransactionError(f"{code}_changed")
        raw = b"".join(chunks)
        return raw, {
            "sha256": _sha(raw),
            "mode": format(stat.S_IMODE(before.st_mode), "04o"),
            "size_bytes": len(raw),
            "device": before.st_dev,
            "inode": before.st_ino,
            "mtime_ns": before.st_mtime_ns,
            "ctime_ns": before.st_ctime_ns,
        }
    except FileNotFoundError as exc:
        raise ReleaseTransactionError(code) from exc
    except OSError as exc:
        raise ReleaseTransactionError(code) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _observe(path: Path, *, required: bool) -> dict[str, Any]:
    try:
        _raw, observation = _read_file(
            path,
            code="pnc_release_transaction_target_invalid",
            maximum=MAX_FILE_BYTES,
        )
        observation["exists"] = True
        return observation
    except ReleaseTransactionError:
        if not required:
            try:
                path.lstat()
            except FileNotFoundError:
                return {
                    "exists": False,
                    "sha256": None,
                    "mode": None,
                    "size_bytes": 0,
                    "device": None,
                    "inode": None,
                    "mtime_ns": None,
                    "ctime_ns": None,
                }
        raise


def _same_observation(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    return dict(left) == dict(right)


def _json(raw: bytes, *, code: str) -> dict[str, Any]:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise ReleaseTransactionError(f"{code}_duplicate_key")
            result[key] = value
        return result

    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs)
    except ReleaseTransactionError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ReleaseTransactionError(code) from exc
    if not isinstance(value, dict):
        raise ReleaseTransactionError(code)
    return value


def _pretty(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(dict(value), ensure_ascii=True, indent=2, sort_keys=True, allow_nan=False)
        + "\n"
    ).encode("utf-8")


def _write_new(path: Path, raw: bytes, *, mode: int = 0o600) -> None:
    parent = _directory(path.parent, create=True)
    descriptor = -1
    temporary: Path | None = None
    try:
        descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=parent)
        temporary = Path(name)
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "wb", closefd=True) as stream:
            descriptor = -1
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path, follow_symlinks=False)
        os.unlink(temporary)
        temporary = None
        directory_fd = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except FileExistsError as exc:
        raise ReleaseTransactionError("pnc_release_transaction_output_exists") from exc
    except OSError as exc:
        raise ReleaseTransactionError("pnc_release_transaction_output_invalid") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _replace(path: Path, raw: bytes, *, mode: int) -> None:
    parent = _directory(path.parent, create=True)
    descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=parent)
    temporary = Path(name)
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "wb", closefd=True) as stream:
            descriptor = -1
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        temporary = None
        os.chmod(path, mode)
        directory_fd = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except OSError as exc:
        raise ReleaseTransactionError("pnc_release_transaction_replace_failed") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _git(source_root: Path, argument: str) -> str:
    try:
        result = subprocess.run(
            ("git", "-C", str(source_root), "rev-parse", argument),
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ReleaseTransactionError("pnc_release_transaction_source_git_invalid") from exc
    value = result.stdout.strip()
    if result.returncode != 0 or not re.fullmatch(r"[0-9a-f]{40}", value):
        raise ReleaseTransactionError("pnc_release_transaction_source_git_invalid")
    return value


def _source_provenance(source_root: Path) -> dict[str, str]:
    status = subprocess.run(
        ("git", "-C", str(source_root), "status", "--porcelain", "--untracked-files=all"),
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    if status.returncode != 0 or status.stdout:
        raise ReleaseTransactionError("pnc_release_transaction_source_dirty")
    return {"commit": _git(source_root, "HEAD"), "tree": _git(source_root, "HEAD^{tree}")}


def _env_map(raw: bytes) -> dict[str, str]:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ReleaseTransactionError("pnc_release_transaction_env_invalid") from exc
    values: dict[str, str] = {}
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" not in line:
            raise ReleaseTransactionError("pnc_release_transaction_env_invalid")
        key, value = line.split("=", 1)
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key) or key in values:
            raise ReleaseTransactionError("pnc_release_transaction_env_invalid")
        values[key] = value
    return values


def _validate_binding(
    binding: Mapping[str, Any],
    *,
    release_id: str,
    authority_sha256: str,
    authority_epoch_id: str,
    env_sha256: str,
    binding_path: Path,
    env_path: Path,
) -> None:
    if (
        set(binding)
        != bootstrap.ACTIVE_RELEASE_BINDING_FIELDS
        or binding.get("schema_version") != bootstrap.ACTIVE_RELEASE_BINDING_SCHEMA_VERSION
        or binding.get("release_id") != release_id
        or binding.get("authority_sha256") != authority_sha256
        or binding.get("authority_epoch_id") != authority_epoch_id
        or binding.get("complete") is not True
        or binding.get("live_write_performed") is not False
    ):
        raise ReleaseTransactionError("pnc_release_transaction_binding_invalid")
    side_effect = binding.get("side_effect_contract")
    nested = binding.get("bindings")
    policy = binding.get("policy")
    capacity = policy.get("capacity_admission") if isinstance(policy, Mapping) else None
    candidate_env = nested.get("candidate_env") if isinstance(nested, Mapping) else None
    if (
        not isinstance(side_effect, Mapping)
        or side_effect.get("canonical_active_release_binding") != str(binding_path)
        or side_effect.get("canonical_live_env") != str(env_path)
        or not isinstance(candidate_env, Mapping)
        or candidate_env.get("sha256") != env_sha256
        or not isinstance(capacity, Mapping)
        or capacity.get("release_approval_id") != release_id
        or capacity.get("bootstrap_epoch_id") == ""
        or capacity.get("capacity_mode") != "bootstrap"
    ):
        raise ReleaseTransactionError("pnc_release_transaction_binding_invalid")
    for value in (
        binding.get("authority_sha256"),
        capacity.get("release_bom_sha256"),
        capacity.get("approval_evidence_sha256"),
        capacity.get("bootstrap_authorization_sha256"),
        capacity.get("bootstrap_authorization_fingerprint"),
    ):
        if not isinstance(value, str) or HEX64_RE.fullmatch(value) is None:
            raise ReleaseTransactionError("pnc_release_transaction_binding_invalid")


def _validate_plist(raw: bytes, *, label: str, hermes_home: Path) -> None:
    try:
        value = plistlib.loads(raw)
    except (ValueError, plistlib.InvalidFileException) as exc:
        raise ReleaseTransactionError("pnc_release_transaction_plist_invalid") from exc
    if not isinstance(value, Mapping) or value.get("Label") != label:
        raise ReleaseTransactionError("pnc_release_transaction_plist_invalid")
    args = value.get("ProgramArguments")
    launcher = str(hermes_home / "runtime" / "governance-tools" / "pnc_live_exec.py")
    if not isinstance(args, list) or args[:3] != ["/usr/bin/python3", launcher, label]:
        raise ReleaseTransactionError("pnc_release_transaction_plist_runtime_pinned")
    serialized = json.dumps(value, ensure_ascii=True, sort_keys=True)
    if any(marker in serialized for marker in ("/runtime/releases/", "/runtime/venvs/")):
        raise ReleaseTransactionError("pnc_release_transaction_plist_runtime_pinned")
    environment = value.get("EnvironmentVariables")
    if not isinstance(environment, Mapping):
        raise ReleaseTransactionError("pnc_release_transaction_plist_environment_invalid")
    if label in {"local.pnc.rca-delivery-collector", "local.pnc.rca-delivery-dispatcher"} and any(
        key in environment for key in RELEASE_SCOPED_PLIST_KEYS
    ):
        raise ReleaseTransactionError("pnc_release_transaction_plist_release_pin")


def _candidate_paths(candidate_root: Path) -> dict[str, Path]:
    return {
        "authority": candidate_root / "authority.json",
        "pointer": candidate_root / "ACTIVE_RCA_RELEASE.json",
        "manifest": candidate_root / "LIVE_MANIFEST.json",
        "binding": candidate_root / "active-release-binding.json",
        "env": candidate_root / "candidate.env",
        "bootstrap_authorization": candidate_root / "bootstrap-authorization.json",
        **{name: candidate_root / name for name in PLIST_NAMES},
    }


def build_plan(
    *,
    candidate_root: Path,
    source_root: Path,
    home: Path,
    hermes_home: Path,
    control_db: Path,
    evidence_root: Path,
    transaction_id: str | None = None,
) -> tuple[dict[str, Any], Path]:
    candidate_root = _directory(candidate_root)
    source_root = _directory(source_root)
    home = _directory(home)
    hermes_home = _directory(hermes_home)
    state_root = _directory(
        hermes_home / "runtime" / "pnc_agent" / "feishu_issue_kafka_rca"
    )
    evidence_root = _directory(evidence_root, create=True)
    provenance = _source_provenance(source_root)
    paths = _candidate_paths(candidate_root)
    authority_raw, authority_value = _read_file(
        paths["authority"], code="pnc_release_transaction_authority_invalid", required_mode=0o600
    )
    authority_value = _json(authority_raw, code="pnc_release_transaction_authority_invalid")
    try:
        authority.validate_release_authority(authority_value)
    except authority.ReleaseAuthorityError as exc:
        raise ReleaseTransactionError("pnc_release_transaction_authority_invalid") from exc
    release_id = str(authority_value.get("release_id") or "")
    authority_sha256 = authority.canonical_json_sha256(authority_value)
    authority_epoch_id = str(authority_value.get("authority_epoch_id") or "")
    if (
        RELEASE_ID_RE.fullmatch(release_id) is None
        or authority_value.get("status") != "approved_for_activation"
        or authority_value.get("faces", {}).get("host_runtime", {}).get("commit")
        != provenance["commit"]
        or authority_value.get("faces", {}).get("host_runtime", {}).get("tree")
        != provenance["tree"]
    ):
        raise ReleaseTransactionError("pnc_release_transaction_authority_invalid")
    authority_target = state_root / f"{release_id}.authority.json"
    pointer_raw, pointer = _read_file(
        paths["pointer"], code="pnc_release_transaction_pointer_invalid", required_mode=0o600
    )
    pointer = _json(pointer_raw, code="pnc_release_transaction_pointer_invalid")
    try:
        authority.validate_active_pointer(
            pointer,
            authority_value,
            expected_authority_path=authority_target,
        )
    except authority.ReleaseAuthorityError as exc:
        raise ReleaseTransactionError("pnc_release_transaction_pointer_invalid") from exc
    if pointer.get("state") != "active":
        raise ReleaseTransactionError("pnc_release_transaction_pointer_invalid")

    manifest_raw, manifest = _read_file(
        paths["manifest"], code="pnc_release_transaction_manifest_invalid", required_mode=0o600
    )
    manifest = _json(manifest_raw, code="pnc_release_transaction_manifest_invalid")
    env_raw, env_observation = _read_file(
        paths["env"], code="pnc_release_transaction_env_invalid", required_mode=0o600
    )
    env = _env_map(env_raw)
    if (
        env.get("HERMES_RCA_PROD_RELEASE_ID") != release_id
        or env.get("HERMES_RCA_PROD_CAPACITY_MODE") != "bootstrap"
        or env.get("HERMES_RCA_OUTBOX_ALLOW_FEISHU_WRITEBACK") != "false"
        or env.get("HERMES_RCA_DELIVERY_DISPATCHER_ENABLED") != "false"
        or manifest.get("env_sha256") != env_observation["sha256"]
    ):
        raise ReleaseTransactionError("pnc_release_transaction_env_invalid")
    binding_raw, binding = _read_file(
        paths["binding"], code="pnc_release_transaction_binding_invalid", required_mode=0o600
    )
    binding = _json(binding_raw, code="pnc_release_transaction_binding_invalid")
    binding_target = state_root / bootstrap.ACTIVE_RELEASE_BINDING_NAME
    env_target = hermes_home / ".env"
    _validate_binding(
        binding,
        release_id=release_id,
        authority_sha256=authority_sha256,
        authority_epoch_id=authority_epoch_id,
        env_sha256=env_observation["sha256"],
        binding_path=binding_target,
        env_path=env_target,
    )
    authorization_raw, authorization_value = _read_file(
        paths["bootstrap_authorization"],
        code="pnc_release_transaction_bootstrap_authorization_invalid",
        required_mode=0o600,
    )
    authorization_value = _json(
        authorization_raw,
        code="pnc_release_transaction_bootstrap_authorization_invalid",
    )
    if authorization_raw != bootstrap.canonical_bytes(authorization_value):
        raise ReleaseTransactionError(
            "pnc_release_transaction_bootstrap_authorization_noncanonical"
        )
    capacity = binding["policy"]["capacity_admission"]
    try:
        bootstrap.validate_bootstrap_authorization(
            authorization_value,
            expected_epoch_id=capacity["bootstrap_epoch_id"],
            expected_release_bom_sha256=capacity["release_bom_sha256"],
            expected_release_approval_id=release_id,
            expected_approval_evidence_sha256=capacity["approval_evidence_sha256"],
            authorization_receipt_sha256=_sha(authorization_raw),
        )
    except bootstrap.RcaBootstrapAuthorizationError as exc:
        raise ReleaseTransactionError(
            "pnc_release_transaction_bootstrap_authorization_invalid"
        ) from exc
    try:
        audit = authority.audit_release_projections(
            authority_value,
            pointer=pointer,
            authority_path=authority_target,
            live_manifest=manifest,
            active_binding=binding,
            control_store_path=control_db,
        )
    except (authority.ReleaseAuthorityError, OSError, ValueError) as exc:
        raise ReleaseTransactionError("pnc_release_transaction_projection_invalid") from exc
    if not audit.get("ok"):
        raise ReleaseTransactionError("pnc_release_transaction_projection_invalid")

    source_specs: list[tuple[str, Path, Path, int, str]] = [
        (
            "authority",
            paths["authority"],
            authority_target,
            TARGET_MODE["authority"],
            "authority",
        ),
        (
            "pointer",
            paths["pointer"],
            state_root / "ACTIVE_RCA_RELEASE.json",
            TARGET_MODE["pointer"],
            "pointer",
        ),
        (
            "manifest",
            paths["manifest"],
            hermes_home / "runtime" / "LIVE_MANIFEST.json",
            TARGET_MODE["manifest"],
            "manifest",
        ),
        ("binding", paths["binding"], binding_target, TARGET_MODE["binding"], "binding"),
        ("env", paths["env"], env_target, TARGET_MODE["env"], "env"),
        (
            "bootstrap_authorization",
            paths["bootstrap_authorization"],
            bootstrap.BOOTSTRAP_AUTHORIZATION_PATH.expanduser().absolute(),
            TARGET_MODE["bootstrap_authorization"],
            "bootstrap_authorization",
        ),
    ]
    launch_dir = home / "Library" / "LaunchAgents"
    for name in PLIST_NAMES:
        raw, _obs = _read_file(
            paths[name], code="pnc_release_transaction_plist_invalid", required_mode=0o600
        )
        source_raw, _source_observation = _read_file(
            source_root / name,
            code="pnc_release_transaction_plist_source_invalid",
        )
        if raw != source_raw:
            raise ReleaseTransactionError(
                "pnc_release_transaction_plist_source_mismatch"
            )
        _validate_plist(raw, label=name[:-6], hermes_home=hermes_home)
        source_specs.append((name, paths[name], launch_dir / name, TARGET_MODE["plist"], "plist"))

    selected_id = transaction_id or (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        + "-"
        + uuid.uuid4().hex[:12]
    )
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", selected_id) is None:
        raise ReleaseTransactionError("pnc_release_transaction_id_invalid")
    transaction_dir = evidence_root / selected_id
    rollback_dir = transaction_dir / "rollback"
    staged_dir = transaction_dir / "staged"
    _directory(transaction_dir, create=True)
    _directory(rollback_dir, create=True)
    _directory(staged_dir, create=True)
    entries: list[dict[str, Any]] = []
    for index, (name, source, target, mode, kind) in enumerate(source_specs):
        raw, source_observation = _read_file(
            source,
            code="pnc_release_transaction_candidate_invalid",
            required_mode=0o600 if kind != "plist" else 0o600,
        )
        staged = staged_dir / f"{index:02d}-{name}.blob"
        _write_new(staged, raw, mode=mode)
        before = _observe(target, required=False)
        if name == "authority" and before["exists"]:
            raise ReleaseTransactionError("pnc_release_transaction_authority_target_exists")
        entries.append(
            {
                "name": name,
                "kind": kind,
                "source_path": str(source),
                "source": source_observation,
                "staged_path": str(staged),
                "target_path": str(target),
                "target_mode": format(mode, "04o"),
                "before": before,
                "rollback_path": str(rollback_dir / f"{index:02d}-{name}.before"),
            }
        )
    plan = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "cli_schema_version": CLI_SCHEMA_VERSION,
        "transaction_id": selected_id,
        "planned_at": datetime.now(timezone.utc).isoformat(),
        "release_id": release_id,
        "authority_sha256": authority_sha256,
        "authority_epoch_id": authority_epoch_id,
        "source_root": str(source_root),
        "source_commit": provenance["commit"],
        "source_tree": provenance["tree"],
        "candidate_root": str(candidate_root),
        "state_root": str(state_root),
        "transaction_dir": str(transaction_dir),
        "rollback_dir": str(rollback_dir),
        "entries": entries,
        "mutation_performed": False,
    }
    plan_path = transaction_dir / "plan.json"
    _write_new(plan_path, _pretty(plan), mode=0o600)
    return plan, plan_path


def _backup(plan: Mapping[str, Any]) -> None:
    for entry in plan["entries"]:
        before = entry["before"]
        if before["exists"] is not True:
            continue
        raw, _observation = _read_file(
            Path(entry["target_path"]), code="pnc_release_transaction_target_changed"
        )
        _write_new(
            Path(entry["rollback_path"]),
            raw,
            mode=int(entry["before"]["mode"], 8),
        )


def _restore(plan: Mapping[str, Any]) -> None:
    for entry in reversed(plan["entries"]):
        target = Path(entry["target_path"])
        before = entry["before"]
        if before["exists"]:
            raw, _obs = _read_file(
                Path(entry["rollback_path"]), code="pnc_release_transaction_rollback_invalid"
            )
            _replace(target, raw, mode=int(before["mode"], 8))
        else:
            target.unlink(missing_ok=True)


def apply_plan(
    plan: Mapping[str, Any],
    *,
    plan_path: Path,
    replace_func=os.replace,
) -> dict[str, Any]:
    state_root = _directory(Path(plan["state_root"]))
    lock = state_root / ".pnc-rca-release-transaction.lock"
    lock_fd = -1
    staged: list[Path] = []
    mutation_started = False
    rollback_performed = False
    try:
        try:
            lock_fd = os.open(
                lock,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_CLOEXEC", 0),
                0o600,
            )
        except FileExistsError as exc:
            raise ReleaseTransactionError("pnc_release_transaction_lock_held") from exc
        os.write(lock_fd, str(plan["transaction_id"]).encode("ascii"))
        os.fsync(lock_fd)
        provenance = _source_provenance(Path(plan["source_root"]))
        if provenance != {
            "commit": plan["source_commit"],
            "tree": plan["source_tree"],
        }:
            raise ReleaseTransactionError("pnc_release_transaction_source_changed")
        _backup(plan)
        for entry in plan["entries"]:
            current = _observe(Path(entry["target_path"]), required=False)
            if not _same_observation(current, entry["before"]):
                raise ReleaseTransactionError("pnc_release_transaction_target_changed")
            staged_path = Path(entry["staged_path"])
            raw, observation = _read_file(
                staged_path, code="pnc_release_transaction_staged_invalid"
            )
            if observation["sha256"] != entry["source"]["sha256"]:
                raise ReleaseTransactionError("pnc_release_transaction_staged_changed")
            staged.append(staged_path)
        mutation_started = True
        # The manifest is published last; readers see the old authority until all
        # supporting files are ready, and automatic rollback handles any failure.
        ordered = sorted(
            plan["entries"], key=lambda item: 1 if item["kind"] == "manifest" else 0
        )
        for entry in ordered:
            raw, _obs = _read_file(
                Path(entry["staged_path"]), code="pnc_release_transaction_staged_invalid"
            )
            temporary = Path(entry["target_path"]).parent / (
                f".{Path(entry['target_path']).name}.release-{plan['transaction_id']}.tmp"
            )
            _write_new(temporary, raw, mode=int(entry["target_mode"], 8))
            try:
                replace_func(temporary, Path(entry["target_path"]))
            finally:
                temporary.unlink(missing_ok=True)
        after = []
        for entry in plan["entries"]:
            observed = _observe(Path(entry["target_path"]), required=True)
            if (
                observed["sha256"] != entry["source"]["sha256"]
                or observed["mode"] != entry["target_mode"]
            ):
                raise ReleaseTransactionError("pnc_release_transaction_verify_failed")
            after.append({"name": entry["name"], "observed": observed})
        receipt = {
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "transaction_id": plan["transaction_id"],
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "release_id": plan["release_id"],
            "authority_sha256": plan["authority_sha256"],
            "authority_epoch_id": plan["authority_epoch_id"],
            "source_commit": plan["source_commit"],
            "source_tree": plan["source_tree"],
            "plan_path": str(plan_path),
            "plan_raw_sha256": _sha(plan_path.read_bytes()),
            "entries": after,
            "mutation_performed": True,
            "rollback_performed": False,
            "production_effects": {
                "resident_restart": False,
                "task_submission": False,
                "kafka_consume": False,
                "feishu_write": False,
            },
            "verification": "pass",
        }
        receipt_path = Path(plan["transaction_dir"]) / "receipt.json"
        _write_new(receipt_path, _pretty(receipt), mode=0o600)
        receipt["receipt_path"] = str(receipt_path)
        receipt["receipt_raw_sha256"] = _sha(receipt_path.read_bytes())
        return receipt
    except Exception:
        if mutation_started:
            try:
                _restore(plan)
                rollback_performed = True
            except Exception:
                raise
        raise
    finally:
        if lock_fd >= 0:
            os.close(lock_fd)
        lock.unlink(missing_ok=True)
        if rollback_performed:
            rollback_path = Path(plan["transaction_dir"]) / "automatic-rollback.json"
            try:
                _write_new(
                    rollback_path,
                    _pretty(
                        {
                            "schema_version": ROLLBACK_SCHEMA_VERSION,
                            "transaction_id": plan["transaction_id"],
                            "rolled_back_at": datetime.now(timezone.utc).isoformat(),
                            "restored_to_pre_transaction": True,
                        }
                    ),
                    mode=0o600,
                )
            except ReleaseTransactionError:
                pass


def _read_plan(path: Path) -> tuple[bytes, dict[str, Any]]:
    raw, _obs = _read_file(path, code="pnc_release_transaction_plan_invalid", required_mode=0o600)
    value = _json(raw, code="pnc_release_transaction_plan_invalid")
    if value.get("schema_version") != PLAN_SCHEMA_VERSION:
        raise ReleaseTransactionError("pnc_release_transaction_plan_invalid")
    return raw, value


def rollback_transaction(receipt_path: Path, *, output_path: Path) -> dict[str, Any]:
    receipt_raw, receipt = _read_file(
        receipt_path, code="pnc_release_transaction_receipt_invalid", required_mode=0o600
    )
    value = _json(receipt_raw, code="pnc_release_transaction_receipt_invalid")
    if value.get("schema_version") != RECEIPT_SCHEMA_VERSION:
        raise ReleaseTransactionError("pnc_release_transaction_receipt_invalid")
    plan_raw, plan = _read_plan(Path(value["plan_path"]))
    if _sha(plan_raw) != value.get("plan_raw_sha256"):
        raise ReleaseTransactionError("pnc_release_transaction_receipt_binding_invalid")
    state_root = _directory(Path(plan["state_root"]))
    lock = state_root / ".pnc-rca-release-transaction.lock"
    try:
        fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as exc:
        raise ReleaseTransactionError("pnc_release_transaction_lock_held") from exc
    try:
        for item in value.get("entries", []):
            entry = next(
                (candidate for candidate in plan["entries"] if candidate["name"] == item["name"]),
                None,
            )
            if entry is None:
                raise ReleaseTransactionError("pnc_release_transaction_receipt_binding_invalid")
            current = _observe(Path(entry["target_path"]), required=True)
            if not _same_observation(current, item["observed"]):
                raise ReleaseTransactionError("pnc_release_transaction_rollback_target_changed")
        _restore(plan)
        result = {
            "schema_version": ROLLBACK_SCHEMA_VERSION,
            "transaction_id": plan["transaction_id"],
            "rolled_back_at": datetime.now(timezone.utc).isoformat(),
            "restored_to_pre_transaction": True,
            "production_effects": {
                "resident_restart": False,
                "task_submission": False,
                "kafka_consume": False,
                "feishu_write": False,
            },
        }
        _write_new(output_path, _pretty(result), mode=0o600)
        return result
    finally:
        os.close(fd)
        lock.unlink(missing_ok=True)


def _arguments(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    plan = commands.add_parser("plan")
    plan.add_argument("--candidate-root", type=Path, required=True)
    plan.add_argument("--source-root", type=Path, required=True)
    plan.add_argument("--home", type=Path, default=Path.home())
    plan.add_argument("--hermes-home", type=Path, default=Path.home() / ".hermes")
    plan.add_argument("--control-db", type=Path, required=True)
    plan.add_argument("--evidence-root", type=Path, required=True)
    plan.add_argument("--transaction-id")
    apply = commands.add_parser("apply")
    apply.add_argument("--plan", type=Path, required=True)
    rollback = commands.add_parser("rollback")
    rollback.add_argument("--receipt", type=Path, required=True)
    rollback.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = _arguments(argv)
        if args.command == "plan":
            result, path = build_plan(
                candidate_root=args.candidate_root,
                source_root=args.source_root,
                home=args.home.expanduser().absolute(),
                hermes_home=args.hermes_home.expanduser().absolute(),
                control_db=args.control_db.expanduser().absolute(),
                evidence_root=args.evidence_root.expanduser().absolute(),
                transaction_id=args.transaction_id,
            )
            print(json.dumps({"ok": True, "plan_path": str(path), **result}, sort_keys=True))
            return 0
        if args.command == "apply":
            raw, plan = _read_plan(args.plan)
            result = apply_plan(plan, plan_path=args.plan)
            print(json.dumps({"ok": True, **result}, sort_keys=True))
            return 0
        result = rollback_transaction(args.receipt, output_path=args.output)
        print(json.dumps({"ok": True, **result}, sort_keys=True))
        return 0
    except (ReleaseTransactionError, OSError, ValueError) as exc:
        print(json.dumps({"ok": False, "code": getattr(exc, "code", "pnc_rca_release_transaction_invalid")}, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
