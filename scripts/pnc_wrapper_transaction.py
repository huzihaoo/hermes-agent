#!/usr/bin/env python3
"""Install or roll back the governed Hermes runtime wrapper set atomically."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile
from typing import Any, Callable, Mapping, Sequence
import uuid


SOURCE_NAMES = (
    "hermes-context-budget-check",
    "hermes-governance-check",
    "hermes-live-drift-guard",
    "hermes-provider-failure-audit",
    "hermes-release-fingerprint-check",
    "hermes-safe-worktree-remove",
    "hermes-worktree-hygiene",
    "hermes.current",
)
RETIRED_NAMES = ("hermes-g1q3-e2e-smoke",)
PLAN_SCHEMA_VERSION = "pnc_wrapper_transaction_plan_v1"
RECEIPT_SCHEMA_VERSION = "pnc_wrapper_transaction_receipt_v1"
ROLLBACK_RECEIPT_SCHEMA_VERSION = "pnc_wrapper_rollback_receipt_v1"
CLI_SCHEMA_VERSION = "pnc_wrapper_transaction_cli_v1"
MAX_WRAPPER_BYTES = 1024 * 1024
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")


class WrapperTransactionError(RuntimeError):
    """Stable wrapper lifecycle failure."""

    def __init__(self, code: str, detail: str = ""):
        self.code = str(code or "pnc_wrapper_transaction_invalid")[:160]
        self.detail = str(detail or self.code)[:1000]
        super().__init__(self.code)


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _absolute(path: Path, code: str) -> Path:
    selected = path.expanduser()
    if not selected.is_absolute() or selected.absolute() != selected:
        raise WrapperTransactionError(code)
    return selected


def _directory(path: Path, *, create: bool = False) -> Path:
    selected = _absolute(path, "pnc_wrapper_transaction_directory_invalid")
    if create:
        try:
            selected.mkdir(mode=0o700, parents=True, exist_ok=False)
        except OSError as exc:
            raise WrapperTransactionError(
                "pnc_wrapper_transaction_directory_invalid", str(selected)
            ) from exc
    try:
        observed = selected.lstat()
        resolved = selected.resolve(strict=True)
    except OSError as exc:
        raise WrapperTransactionError(
            "pnc_wrapper_transaction_directory_invalid", str(selected)
        ) from exc
    if (
        resolved != selected
        or stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISDIR(observed.st_mode)
        or observed.st_uid != os.geteuid()
        or stat.S_IMODE(observed.st_mode) & 0o022
    ):
        raise WrapperTransactionError(
            "pnc_wrapper_transaction_directory_invalid", str(selected)
        )
    return selected


def _file_observation(path: Path, *, required: bool) -> dict[str, Any]:
    try:
        observed = path.lstat()
    except FileNotFoundError:
        if required:
            raise WrapperTransactionError(
                "pnc_wrapper_transaction_source_missing", str(path)
            )
        return {"exists": False, "sha256": None, "mode": None, "size_bytes": 0}
    except OSError as exc:
        raise WrapperTransactionError(
            "pnc_wrapper_transaction_file_unavailable", str(path)
        ) from exc
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISREG(observed.st_mode)
        or observed.st_uid != os.geteuid()
        or observed.st_nlink != 1
        or observed.st_size <= 0
        or observed.st_size > MAX_WRAPPER_BYTES
    ):
        raise WrapperTransactionError(
            "pnc_wrapper_transaction_file_invalid", str(path)
        )
    raw = path.read_bytes()
    if len(raw) != observed.st_size:
        raise WrapperTransactionError(
            "pnc_wrapper_transaction_file_changed", str(path)
        )
    return {
        "exists": True,
        "sha256": _sha256(raw),
        "mode": format(stat.S_IMODE(observed.st_mode), "04o"),
        "size_bytes": observed.st_size,
    }


def _source_provenance(root: Path) -> dict[str, str]:
    try:
        commit = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"],
            text=True,
            capture_output=True,
            timeout=20,
            check=False,
        )
        tree = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD^{tree}"],
            text=True,
            capture_output=True,
            timeout=20,
            check=False,
        )
        dirty = subprocess.run(
            ["git", "-C", str(root), "status", "--porcelain", "--untracked-files=all"],
            text=True,
            capture_output=True,
            timeout=20,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise WrapperTransactionError(
            "pnc_wrapper_transaction_source_git_unavailable"
        ) from exc
    if any(item.returncode != 0 for item in (commit, tree, dirty)):
        raise WrapperTransactionError("pnc_wrapper_transaction_source_git_unavailable")
    commit_text = commit.stdout.strip()
    tree_text = tree.stdout.strip()
    if (
        _COMMIT_RE.fullmatch(commit_text) is None
        or _COMMIT_RE.fullmatch(tree_text) is None
        or dirty.stdout
    ):
        raise WrapperTransactionError("pnc_wrapper_transaction_source_git_dirty")
    return {"commit": commit_text, "tree": tree_text}


def _write_new(path: Path, raw: bytes, *, mode: int = 0o600) -> None:
    descriptor = -1
    try:
        descriptor = os.open(
            path,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_CLOEXEC", 0),
            mode,
        )
        written = 0
        while written < len(raw):
            count = os.write(descriptor, raw[written:])
            if count <= 0:
                raise OSError("short write")
            written += count
        os.fsync(descriptor)
    except FileExistsError as exc:
        raise WrapperTransactionError(
            "pnc_wrapper_transaction_output_exists", str(path)
        ) from exc
    except OSError as exc:
        raise WrapperTransactionError(
            "pnc_wrapper_transaction_output_invalid", str(path)
        ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _pretty(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(
            dict(value),
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _copy_new(source: Path, destination: Path, *, mode: int) -> None:
    raw = source.read_bytes()
    _write_new(destination, raw, mode=mode)
    if _sha256(destination.read_bytes()) != _sha256(raw):
        raise WrapperTransactionError("pnc_wrapper_transaction_backup_mismatch")


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def build_plan(
    *,
    source_root: Path,
    target_bin: Path,
    evidence_root: Path,
    authority_sha256: str,
    transaction_id: str | None = None,
) -> tuple[dict[str, Any], Path]:
    source = _directory(source_root)
    target = _directory(target_bin)
    evidence = _directory(evidence_root)
    authority = str(authority_sha256 or "").lower()
    if _SHA256_RE.fullmatch(authority) is None:
        raise WrapperTransactionError("pnc_wrapper_transaction_authority_invalid")
    selected_id = transaction_id or (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        + "-"
        + uuid.uuid4().hex[:12]
    )
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", selected_id) is None:
        raise WrapperTransactionError("pnc_wrapper_transaction_id_invalid")
    transaction_dir = evidence / selected_id
    backup_dir = transaction_dir / "rollback"
    provenance = _source_provenance(source)
    entries: list[dict[str, Any]] = []
    wrappers_root = source / "scripts" / "wrappers"
    for name in SOURCE_NAMES:
        source_path = wrappers_root / name
        source_observation = _file_observation(source_path, required=True)
        raw = source_path.read_bytes()
        if (
            b"pnc_live_exec.py" not in raw
            or b"runtime/releases/" in raw
            or source_observation["mode"] != "0755"
        ):
            raise WrapperTransactionError(
                "pnc_wrapper_transaction_source_not_dynamic", name
            )
        target_path = target / name
        before = _file_observation(target_path, required=False)
        entries.append({
            "name": name,
            "action": "install",
            "source_path": str(source_path),
            "source": source_observation,
            "target_path": str(target_path),
            "before": before,
            "backup_path": str(backup_dir / f"install-{name}.before"),
        })
    for name in RETIRED_NAMES:
        target_path = target / name
        before = _file_observation(target_path, required=False)
        entries.append({
            "name": name,
            "action": "retire",
            "source_path": None,
            "source": None,
            "target_path": str(target_path),
            "before": before,
            "backup_path": str(backup_dir / f"retire-{name}.before"),
        })
    _directory(transaction_dir, create=True)
    _directory(backup_dir, create=True)
    plan = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "transaction_id": selected_id,
        "planned_at": datetime.now(timezone.utc).isoformat(),
        "authority_sha256": authority,
        "source_root": str(source),
        "source_commit": provenance["commit"],
        "source_tree": provenance["tree"],
        "target_bin": str(target),
        "transaction_dir": str(transaction_dir),
        "rollback_dir": str(backup_dir),
        "entries": entries,
        "mutation_performed": False,
    }
    plan_path = transaction_dir / "plan.json"
    _write_new(plan_path, _pretty(plan))
    return plan, plan_path


def _stage_sources(plan: Mapping[str, Any]) -> dict[str, Path]:
    target_bin = Path(plan["target_bin"])
    staged: dict[str, Path] = {}
    try:
        for entry in plan["entries"]:
            if entry["action"] != "install":
                continue
            descriptor, temporary = tempfile.mkstemp(
                prefix=f".{entry['name']}.pnc-wrapper-",
                suffix=".tmp",
                dir=target_bin,
            )
            path = Path(temporary)
            try:
                os.fchmod(descriptor, 0o755)
                raw = Path(entry["source_path"]).read_bytes()
                written = 0
                while written < len(raw):
                    count = os.write(descriptor, raw[written:])
                    if count <= 0:
                        raise OSError("short write")
                    written += count
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            if _sha256(path.read_bytes()) != entry["source"]["sha256"]:
                raise WrapperTransactionError(
                    "pnc_wrapper_transaction_stage_mismatch", entry["name"]
                )
            staged[entry["name"]] = path
        return staged
    except Exception:
        for path in staged.values():
            try:
                path.unlink()
            except OSError:
                pass
        raise


def _backup_targets(plan: Mapping[str, Any]) -> None:
    for entry in plan["entries"]:
        if entry["before"]["exists"] is not True:
            continue
        target = Path(entry["target_path"])
        backup = Path(entry["backup_path"])
        _copy_new(
            target,
            backup,
            mode=int(str(entry["before"]["mode"]), 8),
        )
        if _file_observation(backup, required=True) != entry["before"]:
            raise WrapperTransactionError(
                "pnc_wrapper_transaction_backup_mismatch", entry["name"]
            )


def _restore_from_plan(
    plan: Mapping[str, Any],
    *,
    replace_func: Callable[[str | Path, str | Path], None] = os.replace,
) -> None:
    target_bin = Path(plan["target_bin"])
    for entry in reversed(plan["entries"]):
        target = Path(entry["target_path"])
        backup = Path(entry["backup_path"])
        before = entry["before"]
        if before["exists"] is True:
            if not backup.exists():
                raise WrapperTransactionError(
                    "pnc_wrapper_transaction_rollback_backup_missing", entry["name"]
                )
            descriptor, temporary = tempfile.mkstemp(
                prefix=f".{entry['name']}.rollback-", suffix=".tmp", dir=target_bin
            )
            temp_path = Path(temporary)
            try:
                os.fchmod(descriptor, int(str(before["mode"]), 8))
                raw = backup.read_bytes()
                written = 0
                while written < len(raw):
                    count = os.write(descriptor, raw[written:])
                    if count <= 0:
                        raise OSError("short write")
                    written += count
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            replace_func(temp_path, target)
        elif target.exists():
            target.unlink()
    _fsync_directory(target_bin)


def apply_plan(
    plan: Mapping[str, Any],
    *,
    plan_path: Path,
    replace_func: Callable[[str | Path, str | Path], None] = os.replace,
) -> dict[str, Any]:
    target_bin = Path(plan["target_bin"])
    transaction_dir = Path(plan["transaction_dir"])
    lock_path = target_bin / ".pnc-wrapper-transaction.lock"
    lock_fd = -1
    lock_owned = False
    staged: dict[str, Path] = {}
    mutation_started = False
    rollback_performed = False
    try:
        try:
            lock_fd = os.open(
                lock_path,
                os.O_CREAT
                | os.O_EXCL
                | os.O_WRONLY
                | getattr(os, "O_CLOEXEC", 0),
                0o600,
            )
        except FileExistsError as exc:
            raise WrapperTransactionError(
                "pnc_wrapper_transaction_lock_held"
            ) from exc
        lock_owned = True
        os.write(lock_fd, str(plan["transaction_id"]).encode("ascii"))
        os.fsync(lock_fd)
        staged = _stage_sources(plan)
        _backup_targets(plan)
        for entry in plan["entries"]:
            target = Path(entry["target_path"])
            if _file_observation(target, required=False) != entry["before"]:
                raise WrapperTransactionError(
                    "pnc_wrapper_transaction_target_changed", entry["name"]
                )
        mutation_started = True
        for entry in plan["entries"]:
            target = Path(entry["target_path"])
            if entry["action"] == "install":
                replace_func(staged.pop(entry["name"]), target)
            elif entry["before"]["exists"] is True:
                target.unlink()
        _fsync_directory(target_bin)
        after: list[dict[str, Any]] = []
        for entry in plan["entries"]:
            observed = _file_observation(Path(entry["target_path"]), required=False)
            if entry["action"] == "install":
                if (
                    observed["sha256"] != entry["source"]["sha256"]
                    or observed["mode"] != "0755"
                ):
                    raise WrapperTransactionError(
                        "pnc_wrapper_transaction_install_verify_failed", entry["name"]
                    )
            elif observed["exists"] is not False:
                raise WrapperTransactionError(
                    "pnc_wrapper_transaction_retire_verify_failed", entry["name"]
                )
            after.append({"name": entry["name"], "observed": observed})
        receipt = {
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "transaction_id": plan["transaction_id"],
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "authority_sha256": plan["authority_sha256"],
            "source_commit": plan["source_commit"],
            "source_tree": plan["source_tree"],
            "plan_path": str(plan_path),
            "plan_raw_sha256": _sha256(plan_path.read_bytes()),
            "target_bin": str(target_bin),
            "rollback_dir": plan["rollback_dir"],
            "installed": list(SOURCE_NAMES),
            "retired": list(RETIRED_NAMES),
            "after": after,
            "mutation_performed": True,
            "rollback_performed": False,
            "verification": "pass",
        }
        receipt_path = transaction_dir / "receipt.json"
        _write_new(receipt_path, _pretty(receipt))
        receipt["receipt_path"] = str(receipt_path)
        receipt["receipt_raw_sha256"] = _sha256(receipt_path.read_bytes())
        return receipt
    except Exception:
        if mutation_started:
            _restore_from_plan(plan, replace_func=replace_func)
            rollback_performed = True
        raise
    finally:
        for path in staged.values():
            try:
                path.unlink()
            except OSError:
                pass
        if lock_fd >= 0:
            os.close(lock_fd)
        if lock_owned:
            try:
                lock_path.unlink()
            except FileNotFoundError:
                pass
        if rollback_performed:
            rollback = {
                "schema_version": ROLLBACK_RECEIPT_SCHEMA_VERSION,
                "transaction_id": plan["transaction_id"],
                "rolled_back_at": datetime.now(timezone.utc).isoformat(),
                "reason": "apply_failed",
                "restored_to_pre_transaction": True,
            }
            try:
                _write_new(transaction_dir / "automatic-rollback.json", _pretty(rollback))
            except WrapperTransactionError:
                pass


def _read_json(path: Path, *, artifact: str) -> tuple[bytes, dict[str, Any]]:
    selected = _absolute(path, "pnc_wrapper_transaction_receipt_path_invalid")
    observed = _file_observation(selected, required=True)
    if observed["mode"] != "0600":
        raise WrapperTransactionError(
            "pnc_wrapper_transaction_receipt_file_invalid", artifact
        )
    raw = selected.read_bytes()
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise WrapperTransactionError(
            "pnc_wrapper_transaction_receipt_json_invalid", artifact
        ) from exc
    if not isinstance(value, dict):
        raise WrapperTransactionError(
            "pnc_wrapper_transaction_receipt_shape_invalid", artifact
        )
    return raw, value


def rollback_transaction(
    receipt_path: Path,
    *,
    output_path: Path,
) -> dict[str, Any]:
    selected_output = _absolute(
        output_path, "pnc_wrapper_transaction_rollback_output_invalid"
    )
    _directory(selected_output.parent)
    receipt_raw, receipt = _read_json(receipt_path, artifact="receipt")
    if receipt.get("schema_version") != RECEIPT_SCHEMA_VERSION:
        raise WrapperTransactionError("pnc_wrapper_transaction_receipt_shape_invalid")
    plan_path = Path(str(receipt.get("plan_path") or ""))
    plan_raw, plan = _read_json(plan_path, artifact="plan")
    if (
        plan.get("schema_version") != PLAN_SCHEMA_VERSION
        or _sha256(plan_raw) != receipt.get("plan_raw_sha256")
        or plan.get("transaction_id") != receipt.get("transaction_id")
    ):
        raise WrapperTransactionError("pnc_wrapper_transaction_receipt_binding_invalid")
    for entry in plan["entries"]:
        observed = _file_observation(Path(entry["target_path"]), required=False)
        if entry["action"] == "install":
            if observed.get("sha256") != entry["source"]["sha256"]:
                raise WrapperTransactionError(
                    "pnc_wrapper_transaction_rollback_target_changed", entry["name"]
                )
        elif observed.get("exists") is not False:
            raise WrapperTransactionError(
                "pnc_wrapper_transaction_rollback_target_changed", entry["name"]
            )
    _restore_from_plan(plan)
    result = {
        "schema_version": ROLLBACK_RECEIPT_SCHEMA_VERSION,
        "transaction_id": receipt["transaction_id"],
        "rolled_back_at": datetime.now(timezone.utc).isoformat(),
        "source_receipt_raw_sha256": _sha256(receipt_raw),
        "restored_to_pre_transaction": True,
        "target_bin": plan["target_bin"],
    }
    _write_new(selected_output, _pretty(result))
    return result


class _SafeParser(argparse.ArgumentParser):
    def error(self, _message: str) -> None:
        raise WrapperTransactionError("pnc_wrapper_transaction_cli_arguments_invalid")


def _arguments(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = _SafeParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("plan", "apply"):
        command = commands.add_parser(name)
        command.add_argument("--source-root", type=Path, required=True)
        command.add_argument("--target-bin", type=Path, required=True)
        command.add_argument("--evidence-root", type=Path, required=True)
        command.add_argument("--authority-sha256", required=True)
        command.add_argument("--transaction-id")
    rollback = commands.add_parser("rollback")
    rollback.add_argument("--receipt", type=Path, required=True)
    rollback.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    command = "unknown"
    try:
        args = _arguments(argv)
        command = str(args.command)
        if command in {"plan", "apply"}:
            plan, plan_path = build_plan(
                source_root=args.source_root,
                target_bin=args.target_bin,
                evidence_root=args.evidence_root,
                authority_sha256=args.authority_sha256,
                transaction_id=args.transaction_id,
            )
            if command == "apply":
                receipt = apply_plan(plan, plan_path=plan_path)
                payload = {
                    "schema_version": CLI_SCHEMA_VERSION,
                    "command": command,
                    "ok": True,
                    "transaction_id": plan["transaction_id"],
                    "plan_path": str(plan_path),
                    "receipt_path": receipt["receipt_path"],
                    "receipt_raw_sha256": receipt["receipt_raw_sha256"],
                    "mutation_performed": True,
                    "rollback_performed": False,
                }
            else:
                payload = {
                    "schema_version": CLI_SCHEMA_VERSION,
                    "command": command,
                    "ok": True,
                    "transaction_id": plan["transaction_id"],
                    "plan_path": str(plan_path),
                    "plan_raw_sha256": _sha256(plan_path.read_bytes()),
                    "mutation_performed": False,
                }
        elif command == "rollback":
            result = rollback_transaction(args.receipt, output_path=args.output)
            payload = {
                "schema_version": CLI_SCHEMA_VERSION,
                "command": command,
                "ok": True,
                **result,
            }
        else:
            raise WrapperTransactionError(
                "pnc_wrapper_transaction_cli_arguments_invalid"
            )
        print(json.dumps(payload, ensure_ascii=True, sort_keys=True))
        return 0
    except WrapperTransactionError as exc:
        print(
            json.dumps(
                {
                    "schema_version": CLI_SCHEMA_VERSION,
                    "command": command,
                    "ok": False,
                    "code": exc.code,
                    "detail": exc.detail,
                },
                ensure_ascii=True,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
