#!/usr/bin/env python3
"""Stage a short-lived, release-bound RCA bootstrap admission.

The command deliberately stages only the capacity authority required for
bounded, no-materialization RCA validation.  It never starts a resident,
submits a task, consumes Kafka, or writes to Feishu.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from gateway import pnc_rca_prod_bootstrap as bootstrap
from gateway import pnc_rca_release_authority as release_authority


SCHEMA_VERSION = "pnc_rca_bootstrap_stage_receipt_v1"
MAX_INPUT_BYTES = 1024 * 1024
MAX_AUTHORIZATION_DURATION = timedelta(days=8)
RELEASE_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,200}$")


class BootstrapStageError(ValueError):
    def __init__(self, code: str) -> None:
        self.code = str(code or "rca_bootstrap_stage_invalid")[:160]
        super().__init__(self.code)


@dataclass(frozen=True)
class StagePaths:
    control_db: Path
    live_env: Path
    active_binding: Path
    authorization: Path
    receipt: Path


def _canonical_bytes(value: Any) -> bytes:
    try:
        return (
            json.dumps(
                value,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise BootstrapStageError("rca_bootstrap_stage_json_invalid") from exc


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _absolute(path: Path, code: str) -> Path:
    selected = path.expanduser().absolute()
    if not selected.is_absolute():
        raise BootstrapStageError(code)
    return selected


def _read_regular(path: Path, code: str, *, required_mode: int | None = None) -> bytes:
    selected = _absolute(path, code)
    descriptor = -1
    try:
        before = selected.lstat()
        if (
            stat.S_ISLNK(before.st_mode)
            or not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_size < 1
            or before.st_size > MAX_INPUT_BYTES
            or (required_mode is not None and stat.S_IMODE(before.st_mode) != required_mode)
        ):
            raise BootstrapStageError(code)
        descriptor = os.open(
            selected,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
        )
        opened = os.fstat(descriptor)
        identity = (
            opened.st_dev,
            opened.st_ino,
            opened.st_mode,
            opened.st_nlink,
            opened.st_size,
            opened.st_mtime_ns,
            opened.st_ctime_ns,
        )
        if identity != (
            before.st_dev,
            before.st_ino,
            before.st_mode,
            before.st_nlink,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        ):
            raise BootstrapStageError(code)
        chunks: list[bytes] = []
        remaining = opened.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise BootstrapStageError(code)
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise BootstrapStageError(code)
        after = os.fstat(descriptor)
        after_path = selected.lstat()
        if identity != (
            after.st_dev,
            after.st_ino,
            after.st_mode,
            after.st_nlink,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ) or identity != (
            after_path.st_dev,
            after_path.st_ino,
            after_path.st_mode,
            after_path.st_nlink,
            after_path.st_size,
            after_path.st_mtime_ns,
            after_path.st_ctime_ns,
        ):
            raise BootstrapStageError(code)
        return b"".join(chunks)
    except OSError as exc:
        raise BootstrapStageError(code) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _read_json(path: Path, code: str, *, required_mode: int | None = None) -> tuple[bytes, dict[str, Any]]:
    raw = _read_regular(path, code, required_mode=required_mode)
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise BootstrapStageError(code) from exc
    if not isinstance(value, dict):
        raise BootstrapStageError(code)
    return raw, value


def _git_value(root: Path, argument: str) -> str:
    try:
        result = subprocess.run(
            ("git", "-C", str(root), "rev-parse", argument),
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise BootstrapStageError("rca_bootstrap_stage_host_identity_invalid") from exc
    value = result.stdout.strip()
    if result.returncode != 0 or not re.fullmatch(r"[0-9a-f]{40}", value):
        raise BootstrapStageError("rca_bootstrap_stage_host_identity_invalid")
    return value


def _validate_host_seal(
    seal: Mapping[str, Any], *, host_source: Path, release_id: str
) -> None:
    candidate = seal.get("candidate")
    if (
        seal.get("schema_version") != "pnc_rca_gray_host_seal_v1"
        or seal.get("status") not in {"candidate_sealed_unreleased", "gateway_only_released"}
        or seal.get("release_id") != release_id
        or not isinstance(candidate, Mapping)
        or candidate.get("clean") is not True
    ):
        raise BootstrapStageError("rca_bootstrap_stage_host_seal_invalid")
    source = _absolute(host_source, "rca_bootstrap_stage_host_source_invalid")
    if candidate.get("worktree") != str(source):
        raise BootstrapStageError("rca_bootstrap_stage_host_seal_invalid")
    if candidate.get("commit") != _git_value(source, "HEAD") or candidate.get(
        "tree"
    ) != _git_value(source, "HEAD^{tree}"):
        raise BootstrapStageError("rca_bootstrap_stage_host_seal_invalid")


def _validate_approval(record: Mapping[str, Any], *, owner: str) -> None:
    scope = record.get("scope")
    if (
        record.get("schema_version") != "pnc_rca_r8_full_completion_authorization_v1"
        or record.get("state") != "ACTIVE_FULL_COMPLETION_SCOPE"
        or record.get("owner") != owner
        or not isinstance(scope, list)
        or not any("canonical rca_prod" in str(item) for item in scope)
    ):
        raise BootstrapStageError("rca_bootstrap_stage_owner_authorization_invalid")


def _parse_timestamp(value: str, code: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise BootstrapStageError(code) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise BootstrapStageError(code)
    return parsed.astimezone(timezone.utc)


def _validate_resource_snapshot(snapshot: Mapping[str, Any], *, now: datetime) -> None:
    runtime = snapshot.get("rca_prod_snapshot")
    if snapshot.get("ok_for_rca_prod_submit") is not True or not isinstance(runtime, Mapping):
        raise BootstrapStageError("rca_bootstrap_stage_resource_not_ready")
    observed = _parse_timestamp(str(runtime.get("observed_at") or ""), "rca_bootstrap_stage_resource_snapshot_invalid")
    if now - observed > timedelta(minutes=10) or observed - now > timedelta(minutes=1):
        raise BootstrapStageError("rca_bootstrap_stage_resource_snapshot_stale")
    for field, minimum in (
        ("root_available_bytes", bootstrap.ROOT_REQUIRED_AVAILABLE_BYTES),
        ("delivery_available_bytes", bootstrap.DELIVERY_REQUIRED_AVAILABLE_BYTES),
    ):
        value = runtime.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
            raise BootstrapStageError("rca_bootstrap_stage_resource_not_ready")


def _replace_env(raw: bytes, *, release_id: str, epoch_id: str) -> bytes:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise BootstrapStageError("rca_bootstrap_stage_live_env_invalid") from exc
    replacements = {
        "HERMES_RCA_PROD_CAPACITY_MODE": "bootstrap",
        "HERMES_RCA_PROD_RELEASE_ID": release_id,
        "HERMES_RCA_PROD_BOOTSTRAP_EPOCH_ID": epoch_id,
    }
    found: set[str] = set()
    lines: list[str] = []
    for line in text.splitlines():
        key = line.split("=", 1)[0]
        if key in replacements:
            lines.append(f"{key}={replacements[key]}")
            found.add(key)
        else:
            lines.append(line)
    if found != set(replacements):
        raise BootstrapStageError("rca_bootstrap_stage_live_env_keys_missing")
    return ("\n".join(lines) + "\n").encode("utf-8")


def _prepare_stage(
    *,
    paths: StagePaths,
    host_source: Path,
    host_seal_path: Path,
    authority_path: Path,
    owner_authorization_path: Path,
    resource_snapshot_path: Path,
    release_id: str,
    bootstrap_epoch_id: str,
    owner: str,
    deadline: datetime,
    now: datetime,
) -> dict[str, Any]:
    if not RELEASE_ID_RE.fullmatch(release_id) or not bootstrap.EPOCH_ID_RE.fullmatch(
        bootstrap_epoch_id
    ):
        raise BootstrapStageError("rca_bootstrap_stage_release_identity_invalid")
    if not owner or len(owner) > 128 or owner != owner.strip():
        raise BootstrapStageError("rca_bootstrap_stage_owner_invalid")
    if deadline <= now or deadline - now > MAX_AUTHORIZATION_DURATION:
        raise BootstrapStageError("rca_bootstrap_stage_deadline_invalid")
    live_env_raw = _read_regular(paths.live_env, "rca_bootstrap_stage_live_env_invalid", required_mode=0o600)
    host_seal_raw, host_seal = _read_json(host_seal_path, "rca_bootstrap_stage_host_seal_invalid")
    authority_raw, authority = _read_json(
        authority_path,
        "rca_bootstrap_stage_release_authority_invalid",
        required_mode=0o600,
    )
    try:
        release_authority.validate_release_authority(authority)
    except release_authority.ReleaseAuthorityError as exc:
        raise BootstrapStageError(
            "rca_bootstrap_stage_release_authority_invalid"
        ) from exc
    host_face = authority.get("faces", {}).get("host_runtime", {})
    authority_sha256 = release_authority.canonical_json_sha256(authority)
    authority_epoch_id = str(authority.get("authority_epoch_id") or "")
    if (
        authority.get("status") != "approved_for_activation"
        or authority.get("release_id") != release_id
        or host_face.get("commit") != _git_value(host_source, "HEAD")
        or host_face.get("tree") != _git_value(host_source, "HEAD^{tree}")
    ):
        raise BootstrapStageError("rca_bootstrap_stage_release_authority_invalid")
    approval_raw, approval = _read_json(owner_authorization_path, "rca_bootstrap_stage_owner_authorization_invalid")
    snapshot_raw, snapshot = _read_json(resource_snapshot_path, "rca_bootstrap_stage_resource_snapshot_invalid")
    _validate_host_seal(host_seal, host_source=host_source, release_id=release_id)
    _validate_approval(approval, owner=owner)
    _validate_resource_snapshot(snapshot, now=now)
    candidate_env_raw = _replace_env(
        live_env_raw, release_id=release_id, epoch_id=bootstrap_epoch_id
    )
    release_bom_sha256 = _sha256(host_seal_raw)
    approval_sha256 = _sha256(approval_raw)
    authorization = bootstrap.issue_bootstrap_authorization(
        bootstrap_epoch_id=bootstrap_epoch_id,
        started_at=now,
        deadline=deadline,
        release_approval_id=release_id,
        release_bom_sha256=release_bom_sha256,
        approval_evidence_sha256=approval_sha256,
        authorized_by=owner,
        authorized_role="owner",
        now=now,
        receipt_id=f"bootstrap-auth-{bootstrap_epoch_id}",
    )
    validated_auth = bootstrap.validate_bootstrap_authorization(
        authorization,
        now=now,
        expected_epoch_id=bootstrap_epoch_id,
        expected_release_bom_sha256=release_bom_sha256,
        expected_release_approval_id=release_id,
        expected_approval_evidence_sha256=approval_sha256,
    )
    authorization_raw = bootstrap.canonical_bytes(authorization)
    authorization_sha256 = _sha256(authorization_raw)
    binding = {
        "schema_version": bootstrap.ACTIVE_RELEASE_BINDING_SCHEMA_VERSION,
        "release_id": release_id,
        "authority_sha256": authority_sha256,
        "authority_epoch_id": authority_epoch_id,
        "complete": True,
        "live_write_performed": False,
        "bindings": {
            "release_bom_sha256": release_bom_sha256,
            "release_approval": {"sha256": approval_sha256},
            "candidate_env": {"sha256": _sha256(candidate_env_raw)},
            "bootstrap_authorization": {
                "sha256": authorization_sha256,
                "receipt_fingerprint": validated_auth["receipt_fingerprint"],
            },
        },
        "policy": {
            "capacity_admission": {
                "capacity_mode": "bootstrap",
                "bootstrap_epoch_id": bootstrap_epoch_id,
                "bootstrap_authorization_fingerprint": validated_auth["receipt_fingerprint"],
                "bootstrap_authorization_sha256": authorization_sha256,
                "release_bom_sha256": release_bom_sha256,
                "release_approval_id": release_id,
                "approval_evidence_sha256": approval_sha256,
            }
        },
        "side_effect_contract": {
            "canonical_active_release_binding": str(paths.active_binding),
            "canonical_live_env": str(paths.live_env),
        },
    }
    binding_raw = _canonical_bytes(binding)
    return {
        "live_env_raw": live_env_raw,
        "candidate_env_raw": candidate_env_raw,
        "authorization_raw": authorization_raw,
        "binding_raw": binding_raw,
        "release_bom_sha256": release_bom_sha256,
        "approval_sha256": approval_sha256,
        "host_seal_sha256": _sha256(host_seal_raw),
        "authority_sha256": authority_sha256,
        "authority_epoch_id": authority_epoch_id,
        "authority_raw_sha256": _sha256(authority_raw),
        "resource_snapshot_sha256": _sha256(snapshot_raw),
        "authorization": validated_auth,
    }


def _target_backup(path: Path) -> bytes | None:
    if not path.exists():
        return None
    return _read_regular(path, "rca_bootstrap_stage_target_invalid", required_mode=0o600)


def _atomic_write(path: Path, raw: bytes, *, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, mode)
    finally:
        temporary.unlink(missing_ok=True)


def _restore(path: Path, previous: bytes | None) -> None:
    if previous is None:
        path.unlink(missing_ok=True)
    else:
        _atomic_write(path, previous)


def stage_bootstrap(
    *,
    paths: StagePaths,
    host_source: Path,
    host_seal_path: Path,
    authority_path: Path,
    owner_authorization_path: Path,
    resource_snapshot_path: Path,
    release_id: str,
    bootstrap_epoch_id: str,
    owner: str,
    deadline: datetime,
    now: datetime | None = None,
    apply: bool = False,
) -> dict[str, Any]:
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    expected_binding = paths.control_db.parent / bootstrap.ACTIVE_RELEASE_BINDING_NAME
    if paths.active_binding != expected_binding:
        raise BootstrapStageError("rca_bootstrap_stage_active_binding_path_invalid")
    if paths.authorization != bootstrap.BOOTSTRAP_AUTHORIZATION_PATH.expanduser().absolute():
        raise BootstrapStageError("rca_bootstrap_stage_authorization_path_invalid")
    prepared = _prepare_stage(
        paths=paths,
        host_source=_absolute(host_source, "rca_bootstrap_stage_host_source_invalid"),
        host_seal_path=_absolute(host_seal_path, "rca_bootstrap_stage_host_seal_invalid"),
        authority_path=_absolute(
            authority_path, "rca_bootstrap_stage_release_authority_invalid"
        ),
        owner_authorization_path=_absolute(owner_authorization_path, "rca_bootstrap_stage_owner_authorization_invalid"),
        resource_snapshot_path=_absolute(resource_snapshot_path, "rca_bootstrap_stage_resource_snapshot_invalid"),
        release_id=release_id,
        bootstrap_epoch_id=bootstrap_epoch_id,
        owner=owner,
        deadline=deadline.astimezone(timezone.utc),
        now=current,
    )
    summary = {
        "schema_version": SCHEMA_VERSION,
        "status": "PLAN" if not apply else "APPLIED",
        "release_id": release_id,
        "bootstrap_epoch_id": bootstrap_epoch_id,
        "observed_at": current.replace(microsecond=0).isoformat(),
        "deadline": deadline.astimezone(timezone.utc).replace(microsecond=0).isoformat(),
        "release_bom_sha256": prepared["release_bom_sha256"],
        "owner_authorization_sha256": prepared["approval_sha256"],
        "host_seal_sha256": prepared["host_seal_sha256"],
        "authority_sha256": prepared["authority_sha256"],
        "authority_epoch_id": prepared["authority_epoch_id"],
        "authority_raw_sha256": prepared["authority_raw_sha256"],
        "resource_snapshot_sha256": prepared["resource_snapshot_sha256"],
        "authorization": {
            "receipt_fingerprint": prepared["authorization"]["receipt_fingerprint"],
            "authorization_receipt_sha256": prepared["authorization"]["authorization_receipt_sha256"],
            "policy": {
                key: prepared["authorization"][key]
                for key in (
                    "max_concurrency",
                    "daily_started_attempt_quota",
                    "input_materialization",
                    "queue_if_blocked",
                    "bypass_requested",
                )
            },
        },
        "targets": {
            "live_env": str(paths.live_env),
            "active_binding": str(paths.active_binding),
            "bootstrap_authorization": str(paths.authorization),
        },
        "production_effects": {
            "resident_start": False,
            "task_submission": False,
            "kafka_consume": False,
            "feishu_write": False,
        },
    }
    if not apply:
        return summary
    if paths.receipt.exists():
        raise BootstrapStageError("rca_bootstrap_stage_receipt_exists")
    backups = {
        paths.live_env: _target_backup(paths.live_env),
        paths.authorization: _target_backup(paths.authorization),
        paths.active_binding: _target_backup(paths.active_binding),
    }
    written: list[Path] = []
    try:
        for path, raw in (
            (paths.live_env, prepared["candidate_env_raw"]),
            (paths.authorization, prepared["authorization_raw"]),
            (paths.active_binding, prepared["binding_raw"]),
        ):
            _atomic_write(path, raw)
            written.append(path)
        binding = bootstrap.load_active_release_binding(
            path=paths.active_binding,
            live_env_path=paths.live_env,
            expected_release_id=release_id,
            expected_epoch_id=bootstrap_epoch_id,
            expected_authority_sha256=prepared["authority_sha256"],
            expected_authority_epoch_id=prepared["authority_epoch_id"],
        )
        authorization = bootstrap.load_bootstrap_authorization(
            now=current,
            expected_epoch_id=bootstrap_epoch_id,
            expected_release_bom_sha256=prepared["release_bom_sha256"],
            expected_release_approval_id=release_id,
            expected_approval_evidence_sha256=prepared["approval_sha256"],
        )
        summary["status"] = "APPLIED_VERIFIED"
        summary["readback"] = {
            "active_release_binding_sha256": binding["binding_receipt_sha256"],
            "bootstrap_authorization_sha256": authorization["authorization_receipt_sha256"],
            "bootstrap_authorization_fingerprint": authorization["receipt_fingerprint"],
        }
        _atomic_write(paths.receipt, _canonical_bytes(summary))
        return summary
    except Exception:
        for path in reversed(written):
            try:
                _restore(path, backups[path])
            except OSError:
                pass
        raise


def _arguments(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host-source", type=Path, required=True)
    parser.add_argument("--host-seal", type=Path, required=True)
    parser.add_argument("--authority", type=Path, required=True)
    parser.add_argument("--owner-authorization", type=Path, required=True)
    parser.add_argument("--resource-snapshot", type=Path, required=True)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--bootstrap-epoch-id", required=True)
    parser.add_argument("--owner", required=True)
    parser.add_argument("--deadline", required=True)
    parser.add_argument("--live-env", type=Path, required=True)
    parser.add_argument("--control-db", type=Path, required=True)
    parser.add_argument("--active-binding", type=Path, required=True)
    parser.add_argument("--authorization", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = _arguments(argv)
        deadline = _parse_timestamp(args.deadline, "rca_bootstrap_stage_deadline_invalid")
        payload = stage_bootstrap(
            paths=StagePaths(
                control_db=_absolute(args.control_db, "rca_bootstrap_stage_control_db_invalid"),
                live_env=_absolute(args.live_env, "rca_bootstrap_stage_live_env_invalid"),
                active_binding=_absolute(args.active_binding, "rca_bootstrap_stage_active_binding_invalid"),
                authorization=_absolute(args.authorization, "rca_bootstrap_stage_authorization_invalid"),
                receipt=_absolute(args.receipt, "rca_bootstrap_stage_receipt_invalid"),
            ),
            host_source=args.host_source,
            host_seal_path=args.host_seal,
            authority_path=args.authority,
            owner_authorization_path=args.owner_authorization,
            resource_snapshot_path=args.resource_snapshot,
            release_id=args.release_id,
            bootstrap_epoch_id=args.bootstrap_epoch_id,
            owner=args.owner,
            deadline=deadline,
            apply=args.apply,
        )
    except BootstrapStageError as exc:
        print(json.dumps({"ok": False, "code": exc.code}, sort_keys=True))
        return 2
    except bootstrap.RcaBootstrapAuthorizationError as exc:
        print(json.dumps({"ok": False, "code": exc.code}, sort_keys=True))
        return 2
    print(json.dumps({"ok": True, **payload}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
