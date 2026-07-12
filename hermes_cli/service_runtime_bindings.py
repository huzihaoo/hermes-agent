"""Validated config/environment bindings for persistent gateway processes.

Persistent service managers must not infer versioned config paths from the
shell that happens to run ``start`` or ``restart``.  The only authority is the
target Hermes home's ``runtime/LIVE_MANIFEST.json``.

Stage B must write the following fields before selecting ``external`` mode::

    {
      "config_env_binding_mode": "external",
      "release": "hermes-agent-v0.18.2",
      "tag": "v2026.7.7.2",
      "commit": "<full git commit>",
      "release_identity": "<release_identity_for(release, tag, commit)>",
      "config_env_binding": {
        "hermes_home": "<absolute target HERMES_HOME>",
        "release": "<exact top-level release>",
        "tag": "<exact top-level tag>",
        "commit": "<exact top-level commit>",
        "release_identity": "<exact top-level release_identity>",
        "config_path": "<absolute path>",
        "config_sha256": "<sha256 of exact file bytes>",
        "env_path": "<absolute, distinct path>",
        "env_sha256": "<sha256 of exact file bytes>"
      }
    }

Missing manifests and genuinely legacy manifests without
``config_env_binding_mode`` retain the legacy behavior (no external pair).
Removing only the mode while external-only fields remain is a fatal downgrade
attempt. Once a manifest opts into ``external``, every field and live filesystem
invariant is mandatory and failures are fatal.
The target home must be owner-stable and no-follow. The manifest must be
regular/no-follow/single-linked and may not be group/world writable. Bound config and
env files additionally require exact mode ``0400`` on POSIX. Windows, whose
``chmod/stat`` surface collapses owner-read-only to read-only semantics, accepts
the corresponding regular file only when no write bit is reported.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import sys
from pathlib import Path
from typing import Mapping, MutableMapping


CONFIG_PATH_ENV = "HERMES_CONFIG_PATH"
ENV_PATH_ENV = "HERMES_ENV_PATH"
SERVICE_BINDING_ENV_KEYS = (CONFIG_PATH_ENV, ENV_PATH_ENV)

EXTERNAL_BINDING_MODE = "external"
_MAX_MANIFEST_BYTES = 1024 * 1024
_MAX_BOUND_FILE_BYTES = 16 * 1024 * 1024
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_COMMIT_RE = re.compile(r"^(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})$")
_EXTERNAL_ONLY_MANIFEST_KEYS = frozenset({
    "config_env_binding",
    "release",
    "tag",
    "commit",
    "release_identity",
})

STAGE_B_EXTERNAL_BINDING_REQUIRED_FIELDS = {
    "manifest": (
        "config_env_binding_mode",
        "release",
        "tag",
        "commit",
        "release_identity",
        "config_env_binding",
    ),
    "config_env_binding": (
        "hermes_home",
        "release",
        "tag",
        "commit",
        "release_identity",
        "config_path",
        "config_sha256",
        "env_path",
        "env_sha256",
    ),
}


class ServiceRuntimeBindingError(RuntimeError):
    """The live manifest requested external bindings but failed validation."""


def release_identity_for(release: str, tag: str, commit: str) -> str:
    """Return the canonical identity digest binding release, tag, and commit."""
    payload = json.dumps(
        {"commit": commit, "release": release, "tag": tag},
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def is_named_profile_home(target_home: str | Path) -> bool:
    """Return whether *target_home* has the canonical profiles/name shape."""
    try:
        return Path(target_home).expanduser().parent.name == "profiles"
    except (OSError, TypeError, ValueError):
        return False


def _absolute_normalized(path: str | Path, *, field: str) -> Path:
    value = str(path).strip()
    if not value:
        raise ServiceRuntimeBindingError(f"missing external binding field: {field}")
    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        raise ServiceRuntimeBindingError(f"{field} must be an absolute path: {value!r}")
    return Path(os.path.abspath(os.path.normpath(str(candidate))))


def _same_path(left: str | Path, right: str | Path) -> bool:
    left_norm = os.path.normcase(os.path.abspath(os.path.normpath(str(left))))
    right_norm = os.path.normcase(os.path.abspath(os.path.normpath(str(right))))
    return left_norm == right_norm


def _same_identity(left: os.stat_result, right: os.stat_result) -> bool:
    return left.st_dev == right.st_dev and left.st_ino == right.st_ino


def _open_verified_bytes(
    path: Path,
    *,
    owner_uid: int,
    required_mode: int | None,
    max_bytes: int,
    description: str,
    forbid_group_world_write: bool = False,
    forbid_any_write: bool = False,
) -> bytes:
    """Read bytes from one descriptor while enforcing live file invariants."""
    try:
        before = path.lstat()
    except OSError as exc:
        raise ServiceRuntimeBindingError(
            f"cannot stat {description} {path}: {exc}"
        ) from exc
    if stat.S_ISLNK(before.st_mode):
        raise ServiceRuntimeBindingError(f"{description} must not be a symlink: {path}")
    if not stat.S_ISREG(before.st_mode):
        raise ServiceRuntimeBindingError(
            f"{description} must be a regular file: {path}"
        )
    if before.st_nlink != 1:
        raise ServiceRuntimeBindingError(
            f"{description} must have exactly one hard link: {path} (nlink={before.st_nlink})"
        )
    if before.st_uid != owner_uid:
        raise ServiceRuntimeBindingError(
            f"{description} owner mismatch: {path} (uid={before.st_uid}, expected={owner_uid})"
        )
    observed_mode = stat.S_IMODE(before.st_mode)
    if required_mode is not None and observed_mode != required_mode:
        raise ServiceRuntimeBindingError(
            f"{description} mode must be {required_mode:04o}: {path} (mode={observed_mode:04o})"
        )
    if forbid_group_world_write and observed_mode & 0o022:
        raise ServiceRuntimeBindingError(
            f"{description} must not be group/world writable: {path} "
            f"(mode={observed_mode:04o})"
        )
    if forbid_any_write and observed_mode & 0o222:
        raise ServiceRuntimeBindingError(
            f"{description} must be non-writable on Windows: {path} "
            f"(mode={observed_mode:04o})"
        )

    flags = os.O_RDONLY
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise ServiceRuntimeBindingError(
            f"cannot open {description} {path}: {exc}"
        ) from exc

    chunks: list[bytes] = []
    total = 0
    try:
        opened = os.fstat(fd)
        if not _same_identity(before, opened):
            raise ServiceRuntimeBindingError(
                f"{description} changed while opening: {path}"
            )
        while True:
            chunk = os.read(fd, min(1024 * 1024, max_bytes + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > max_bytes:
                raise ServiceRuntimeBindingError(
                    f"{description} exceeds {max_bytes} bytes: {path}"
                )
        opened_after = os.fstat(fd)
        if not _same_identity(opened, opened_after):
            raise ServiceRuntimeBindingError(
                f"{description} changed while reading: {path}"
            )
    finally:
        os.close(fd)

    try:
        after = path.lstat()
    except OSError as exc:
        raise ServiceRuntimeBindingError(
            f"cannot re-stat {description} {path}: {exc}"
        ) from exc
    if not _same_identity(before, after):
        raise ServiceRuntimeBindingError(f"{description} changed after reading: {path}")
    if (
        after.st_size != before.st_size
        or after.st_mtime_ns != before.st_mtime_ns
        or after.st_ctime_ns != before.st_ctime_ns
    ):
        raise ServiceRuntimeBindingError(
            f"{description} metadata changed while reading: {path}"
        )
    return b"".join(chunks)


def _load_manifest(target_home: Path, owner_uid: int) -> dict:
    manifest_path = target_home / "runtime" / "LIVE_MANIFEST.json"
    if not manifest_path.exists():
        if manifest_path.is_symlink():
            raise ServiceRuntimeBindingError(
                f"live manifest must not be a broken symlink: {manifest_path}"
            )
        return {}
    raw = _open_verified_bytes(
        manifest_path,
        owner_uid=owner_uid,
        required_mode=None,
        forbid_group_world_write=True,
        max_bytes=_MAX_MANIFEST_BYTES,
        description="live manifest",
    )
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ServiceRuntimeBindingError(
            f"invalid live manifest {manifest_path}: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise ServiceRuntimeBindingError(
            f"live manifest must contain a JSON object: {manifest_path}"
        )
    return payload


def _required_text(mapping: Mapping[str, object], field: str, *, scope: str) -> str:
    value = mapping.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ServiceRuntimeBindingError(f"missing {scope} field: {field}")
    if "\x00" in value or "\r" in value or "\n" in value:
        raise ServiceRuntimeBindingError(
            f"invalid control character in {scope} field: {field}"
        )
    return value.strip()


def resolve_service_runtime_bindings(target_home: str | Path) -> dict[str, str]:
    """Return a validated config/env pair for a persistent gateway process.

    Named profiles always return an empty pair without consulting a manifest.
    Missing/legacy manifests also return an empty pair.  An explicit external
    mode is all-or-nothing and raises :class:`ServiceRuntimeBindingError` on
    any schema or live-file mismatch.
    """
    home = _absolute_normalized(target_home, field="target_home")
    if is_named_profile_home(home):
        return {}

    manifest_path = home / "runtime" / "LIVE_MANIFEST.json"
    try:
        home_stat = home.lstat()
    except FileNotFoundError:
        return {}
    except OSError as exc:
        raise ServiceRuntimeBindingError(
            f"cannot stat target HERMES_HOME {home}: {exc}"
        ) from exc
    if stat.S_ISLNK(home_stat.st_mode):
        raise ServiceRuntimeBindingError(
            f"target HERMES_HOME must not be a symlink: {home}"
        )
    if not stat.S_ISDIR(home_stat.st_mode):
        raise ServiceRuntimeBindingError(
            f"target HERMES_HOME is not a directory: {home}"
        )
    if not manifest_path.exists() and not manifest_path.is_symlink():
        return {}

    manifest = _load_manifest(home, home_stat.st_uid)
    if "config_env_binding_mode" not in manifest:
        leaked_external_keys = sorted(
            _EXTERNAL_ONLY_MANIFEST_KEYS.intersection(manifest)
        )
        if leaked_external_keys:
            raise ServiceRuntimeBindingError(
                "live manifest omits config_env_binding_mode but retains "
                f"external-only fields: {', '.join(leaked_external_keys)}"
            )
        return {}
    mode = manifest["config_env_binding_mode"]
    if mode != EXTERNAL_BINDING_MODE:
        raise ServiceRuntimeBindingError(
            f"unsupported config_env_binding_mode {mode!r}; expected 'external' or no mode"
        )

    release = _required_text(manifest, "release", scope="manifest")
    tag = _required_text(manifest, "tag", scope="manifest")
    commit = _required_text(manifest, "commit", scope="manifest")
    if not _COMMIT_RE.fullmatch(commit):
        raise ServiceRuntimeBindingError(
            "manifest commit must be a full 40-64 character hexadecimal identity"
        )
    release_identity = _required_text(manifest, "release_identity", scope="manifest")
    expected_identity = release_identity_for(release, tag, commit)
    if release_identity != expected_identity:
        raise ServiceRuntimeBindingError(
            "manifest release_identity does not bind its release/tag/commit"
        )

    binding = manifest.get("config_env_binding")
    if not isinstance(binding, dict):
        raise ServiceRuntimeBindingError("missing manifest object: config_env_binding")
    for field, expected in (
        ("release", release),
        ("tag", tag),
        ("commit", commit),
        ("release_identity", release_identity),
    ):
        observed = _required_text(binding, field, scope="config_env_binding")
        if observed != expected:
            raise ServiceRuntimeBindingError(
                f"config_env_binding.{field} does not match manifest {field}"
            )

    binding_home = _absolute_normalized(
        _required_text(binding, "hermes_home", scope="config_env_binding"),
        field="config_env_binding.hermes_home",
    )
    if not _same_path(binding_home, home):
        raise ServiceRuntimeBindingError(
            f"config_env_binding.hermes_home mismatch: {binding_home} != {home}"
        )

    config_path = _absolute_normalized(
        _required_text(binding, "config_path", scope="config_env_binding"),
        field="config_env_binding.config_path",
    )
    env_path = _absolute_normalized(
        _required_text(binding, "env_path", scope="config_env_binding"),
        field="config_env_binding.env_path",
    )
    if _same_path(config_path, env_path):
        raise ServiceRuntimeBindingError("config_path and env_path must be distinct")

    config_sha = _required_text(binding, "config_sha256", scope="config_env_binding")
    env_sha = _required_text(binding, "env_sha256", scope="config_env_binding")
    if not _SHA256_RE.fullmatch(config_sha):
        raise ServiceRuntimeBindingError(
            "config_env_binding.config_sha256 must be 64 lowercase hex"
        )
    if not _SHA256_RE.fullmatch(env_sha):
        raise ServiceRuntimeBindingError(
            "config_env_binding.env_sha256 must be 64 lowercase hex"
        )

    config_bytes = _open_verified_bytes(
        config_path,
        owner_uid=home_stat.st_uid,
        required_mode=None if sys.platform == "win32" else 0o400,
        forbid_group_world_write=False,
        forbid_any_write=sys.platform == "win32",
        max_bytes=_MAX_BOUND_FILE_BYTES,
        description="external config",
    )
    env_bytes = _open_verified_bytes(
        env_path,
        owner_uid=home_stat.st_uid,
        required_mode=None if sys.platform == "win32" else 0o400,
        forbid_group_world_write=False,
        forbid_any_write=sys.platform == "win32",
        max_bytes=_MAX_BOUND_FILE_BYTES,
        description="external env",
    )
    if hashlib.sha256(config_bytes).hexdigest() != config_sha:
        raise ServiceRuntimeBindingError("external config SHA-256 mismatch")
    if hashlib.sha256(env_bytes).hexdigest() != env_sha:
        raise ServiceRuntimeBindingError("external env SHA-256 mismatch")

    try:
        home_after = home.lstat()
    except OSError as exc:
        raise ServiceRuntimeBindingError(
            f"cannot re-stat target HERMES_HOME {home}: {exc}"
        ) from exc
    if (
        stat.S_ISLNK(home_after.st_mode)
        or not _same_identity(home_stat, home_after)
        or home_after.st_uid != home_stat.st_uid
        or home_after.st_ctime_ns != home_stat.st_ctime_ns
    ):
        raise ServiceRuntimeBindingError(
            f"target HERMES_HOME changed during binding validation: {home}"
        )

    return {
        CONFIG_PATH_ENV: str(config_path),
        ENV_PATH_ENV: str(env_path),
    }


def apply_service_runtime_bindings(
    env: MutableMapping[str, str], target_home: str | Path
) -> None:
    """Replace ambient service bindings with the target manifest's authority."""
    home = _absolute_normalized(target_home, field="target_home")
    resolved = resolve_service_runtime_bindings(home)
    env["HERMES_HOME"] = str(home)
    for key in SERVICE_BINDING_ENV_KEYS:
        env.pop(key, None)
    env.update(resolved)


def service_runtime_environment(
    target_home: str | Path, base_env: Mapping[str, str] | None = None
) -> dict[str, str]:
    """Build a child environment with no ambient config/env authority."""
    env = dict(os.environ if base_env is None else base_env)
    apply_service_runtime_bindings(env, target_home)
    return env
