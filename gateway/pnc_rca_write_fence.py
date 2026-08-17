"""One fail-closed authorization fence for RCA external writes.

The W3 admission snapshot is the source of identity.  This module deliberately
does not read environment switches and never turns a boolean into permission.
An issued fence is immutable, content addressed, and checked again at the
provider boundary so epoch revocation closes the validation/write TOCTOU gap.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from typing import Any, Mapping


WRITE_FENCE_SCHEMA_VERSION = "pnc_rca_write_fence_v1"
WRITE_FENCE_ID_PREFIX = "pnc-rca-wf1-"
WRITE_FENCE_MAX_LIFETIME = timedelta(days=30)
WRITE_FENCE_MAX_FUTURE_SKEW = timedelta(minutes=5)
WRITE_FENCE_ALLOWED_KINDS = frozenset({
    "vm_submit",
    "feishu_issue_comment",
    "feishu_issue_field_update",
    "feishu_thread_reply",
    "feishu_card_create",
    "feishu_card_patch",
    "feishu_attachment_upload",
    "internal_alert",
})
RESIDENT_ACTIVATION_EPOCH_STATES = frozenset({"steady_active"})
RESIDENT_INGRESS_OPEN_STATES = RESIDENT_ACTIVATION_EPOCH_STATES
RESIDENT_EXTERNAL_WRITE_STATES = RESIDENT_ACTIVATION_EPOCH_STATES
MINIMAL_RELEASE_NOTE_SCHEMA_VERSION = "pnc_rca_minimal_release_note_v1"
MINIMAL_RELEASE_PRODUCTION_DEFINITION = (
    "gitlab_ref+release_note+exact_commit_tree_tag+immutable_runtime+"
    "restart_readback+single_canary"
)
MINIMAL_RELEASE_HOST_REMOTE = "git@git.minieye.tech:planning_algo/hermes.git"
MINIMAL_RELEASE_CONTROL_DB_RELATIVE_PATH = Path(
    "runtime/pnc_agent/feishu_issue_kafka_rca/control.sqlite3"
)
MAX_MINIMAL_RELEASE_NOTE_BYTES = 1024 * 1024
WRITE_FENCE_FIELDS = frozenset({
    "schema_version",
    "fence_id",
    "state",
    "admission_snapshot_sha256",
    "activation_epoch_id",
    "activation_ledger_id",
    "admission_key",
    "tenant_id",
    "business_key",
    "submission_key",
    "generation",
    "target_set_sha256",
    "allowed_write_kinds",
    "issued_at",
    "expires_at",
})
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_GIT_OBJECT_RE = re.compile(r"^[0-9a-f]{40}$")
_MINIMAL_RELEASE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{5,127}$")
_MINIMAL_RELEASE_BRANCH_RE = re.compile(r"^refs/heads/[A-Za-z0-9._/-]+$")
_MINIMAL_RELEASE_TAG_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,127}$")
_MINIMAL_RELEASE_GITLAB_REMOTE_RE = re.compile(
    r"^git@git\.minieye\.tech:[A-Za-z0-9._/-]+\.git$"
)
_MINIMAL_RELEASE_NOTE_FIELDS = frozenset({
    "schema_version",
    "production_definition",
    "release_id",
    "release_fingerprint_sha256",
    "release_identity",
    "runtime_projection",
    "activation",
    "resident_profile",
    "canary",
})
_MINIMAL_RELEASE_IDENTITY_FIELDS = frozenset({
    "host",
    "worker",
    "pipeline",
    "report_service",
})
_MINIMAL_RELEASE_GIT_FACE_FIELDS = frozenset({
    "remote",
    "remote_branch",
    "remote_tag",
    "remote_tag_object",
    "commit",
    "tree",
    "runtime_root",
})
_MINIMAL_RELEASE_REPORT_FACE_FIELDS = frozenset({
    "manifest_path",
    "manifest_sha256",
    "pipeline_commit",
    "pipeline_tree",
})


class MinimalReleaseNoteIdentityError(ValueError):
    """A stable structural failure in the shared minimal release identity."""

    def __init__(self, code: str) -> None:
        self.code = str(code or "minimal_release_note_contract_invalid")[:120]
        super().__init__(self.code)


class ExternalWriteFenceError(ValueError):
    """A named fail-closed fence validation failure."""

    def __init__(self, code: str, detail: str = "") -> None:
        self.code = str(code or "external_write_fence_invalid")[:120]
        self.detail = str(detail or self.code)[:1000]
        super().__init__(self.detail)


def _minimal_release_gitlab_remote_valid(value: object) -> bool:
    remote = str(value or "")
    if _MINIMAL_RELEASE_GITLAB_REMOTE_RE.fullmatch(remote) is None:
        return False
    path = remote.removeprefix("git@git.minieye.tech:").removesuffix(".git")
    return all(part not in {"", ".", ".."} for part in path.split("/"))


def _minimal_release_gitlab_face_valid(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != _MINIMAL_RELEASE_GIT_FACE_FIELDS:
        return False
    objects = tuple(
        str(value.get(key) or "") for key in ("commit", "tree", "remote_tag_object")
    )
    return bool(
        _minimal_release_gitlab_remote_valid(value.get("remote"))
        and _MINIMAL_RELEASE_BRANCH_RE.fullmatch(str(value.get("remote_branch") or ""))
        and _MINIMAL_RELEASE_TAG_RE.fullmatch(str(value.get("remote_tag") or ""))
        and all(_GIT_OBJECT_RE.fullmatch(item) and item != "0" * 40 for item in objects)
    )


def _minimal_release_has_github_key(value: object) -> bool:
    if isinstance(value, Mapping):
        return any(
            str(key).lower().startswith("github_")
            or _minimal_release_has_github_key(child)
            for key, child in value.items()
        )
    if isinstance(value, (list, tuple)):
        return any(_minimal_release_has_github_key(item) for item in value)
    return False


def validate_minimal_release_note_identity(note: object) -> dict[str, Any]:
    """Validate the GitLab-only identity shared by release and resident paths."""

    if (
        not isinstance(note, Mapping)
        or set(note) != _MINIMAL_RELEASE_NOTE_FIELDS
        or note.get("schema_version") != MINIMAL_RELEASE_NOTE_SCHEMA_VERSION
        or note.get("production_definition") != MINIMAL_RELEASE_PRODUCTION_DEFINITION
        or _MINIMAL_RELEASE_ID_RE.fullmatch(str(note.get("release_id") or "")) is None
    ):
        raise MinimalReleaseNoteIdentityError("minimal_release_note_contract_invalid")
    identity = note.get("release_identity")
    if (
        not isinstance(identity, Mapping)
        or set(identity) != _MINIMAL_RELEASE_IDENTITY_FIELDS
    ):
        raise MinimalReleaseNoteIdentityError("minimal_release_note_identity_invalid")
    host = identity.get("host")
    worker = identity.get("worker")
    pipeline = identity.get("pipeline")
    if (
        not _minimal_release_gitlab_face_valid(host)
        or str(host.get("remote") or "") != MINIMAL_RELEASE_HOST_REMOTE
    ):
        raise MinimalReleaseNoteIdentityError("minimal_release_note_host_invalid")
    if not _minimal_release_gitlab_face_valid(
        worker
    ) or not _minimal_release_gitlab_face_valid(pipeline):
        raise MinimalReleaseNoteIdentityError("minimal_release_note_identity_invalid")
    report = identity.get("report_service")
    if (
        not isinstance(report, Mapping)
        or set(report) != _MINIMAL_RELEASE_REPORT_FACE_FIELDS
    ):
        raise MinimalReleaseNoteIdentityError("minimal_release_note_identity_invalid")
    if _minimal_release_has_github_key(note):
        raise MinimalReleaseNoteIdentityError("minimal_release_note_contract_invalid")
    return {
        "release_id": str(note["release_id"]),
        "release_identity": identity,
        "host": host,
        "worker": worker,
        "pipeline": pipeline,
    }


def _resident_control_db_path(value: object) -> Path:
    try:
        path = Path(value).expanduser()
    except (TypeError, ValueError) as exc:
        raise ExternalWriteFenceError("resident_release_control_db_path_invalid") from exc
    if not path.is_absolute() or ".." in path.parts:
        raise ExternalWriteFenceError("resident_release_control_db_path_invalid")
    return path


def require_resident_activation_epoch(
    store: Any,
    *,
    allowed_states: Any = RESIDENT_ACTIVATION_EPOCH_STATES,
) -> dict[str, Any]:
    """Require a current activation epoch without consulting environment flags."""

    try:
        epoch = store.activation_epoch()
    except Exception as exc:
        raise ExternalWriteFenceError(
            "resident_activation_epoch_unavailable", type(exc).__name__
        ) from exc
    if not isinstance(epoch, Mapping):
        raise ExternalWriteFenceError("resident_activation_epoch_missing")
    epoch_id = str(epoch.get("epoch_id") or "").strip()
    state = str(epoch.get("state") or "").strip()
    states = frozenset(str(item or "").strip() for item in allowed_states)
    if not states or any(not item for item in states):
        raise ExternalWriteFenceError("resident_activation_epoch_policy_invalid")
    if not epoch_id:
        raise ExternalWriteFenceError("resident_activation_epoch_missing")
    if state not in states:
        raise ExternalWriteFenceError(
            "resident_activation_epoch_state_invalid", state or "unconfigured"
        )
    return dict(epoch)


def validate_resident_release_note(
    epoch: Mapping[str, Any],
    *,
    release_note_path: str | Path,
    runtime_root: str | Path,
    runtime_commit: str,
    runtime_tree: str,
    live_manifest_sha256: str,
    live_env_path: str | Path,
    control_db_path: str | Path,
) -> dict[str, Any]:
    """Bind one resident process to the current epoch's minimal release note."""

    path = Path(release_note_path).expanduser()
    if not path.is_absolute() or path != path.absolute():
        raise ExternalWriteFenceError("resident_release_note_path_invalid")
    try:
        lexical = path.lstat()
    except OSError as exc:
        raise ExternalWriteFenceError(
            "resident_release_note_unavailable", type(exc).__name__
        ) from exc
    if (
        stat.S_ISLNK(lexical.st_mode)
        or not stat.S_ISREG(lexical.st_mode)
        or lexical.st_uid != os.getuid()
        or lexical.st_nlink != 1
        or stat.S_IMODE(lexical.st_mode) & 0o077
        or lexical.st_size <= 0
        or lexical.st_size > MAX_MINIMAL_RELEASE_NOTE_BYTES
    ):
        raise ExternalWriteFenceError("resident_release_note_file_invalid")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise ExternalWriteFenceError(
            "resident_release_note_unavailable", type(exc).__name__
        ) from exc
    try:
        before = os.fstat(descriptor)
        raw = b""
        while len(raw) <= MAX_MINIMAL_RELEASE_NOTE_BYTES:
            chunk = os.read(
                descriptor,
                min(1024 * 1024, MAX_MINIMAL_RELEASE_NOTE_BYTES + 1 - len(raw)),
            )
            if not chunk:
                break
            raw += chunk
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if (
        len(raw) > MAX_MINIMAL_RELEASE_NOTE_BYTES
        or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        or before.st_dev != lexical.st_dev
        or before.st_ino != lexical.st_ino
        or not stat.S_ISREG(before.st_mode)
        or before.st_uid != os.getuid()
        or before.st_nlink != 1
        or stat.S_IMODE(before.st_mode) & 0o077
    ):
        raise ExternalWriteFenceError("resident_release_note_changed")
    try:
        note = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExternalWriteFenceError("resident_release_note_schema_invalid") from exc
    if not isinstance(note, Mapping):
        raise ExternalWriteFenceError("resident_release_note_schema_invalid")
    note = dict(note)
    try:
        validated_identity = validate_minimal_release_note_identity(note)
    except MinimalReleaseNoteIdentityError as exc:
        code = {
            "minimal_release_note_host_invalid": (
                "resident_release_runtime_commit_mismatch"
            ),
            "minimal_release_note_identity_invalid": (
                "resident_release_identity_invalid"
            ),
        }.get(exc.code, "resident_release_note_schema_invalid")
        raise ExternalWriteFenceError(code) from exc
    expected_control_db = _resident_control_db_path(control_db_path)
    env_path = Path(live_env_path).expanduser()
    if not env_path.is_absolute() or ".." in env_path.parts:
        raise ExternalWriteFenceError("resident_release_env_unavailable")
    canonical_control_db = env_path.parent / MINIMAL_RELEASE_CONTROL_DB_RELATIVE_PATH
    if expected_control_db != canonical_control_db:
        raise ExternalWriteFenceError("resident_release_control_db_mismatch")
    activation = note.get("activation")
    if not isinstance(activation, Mapping):
        raise ExternalWriteFenceError("resident_release_note_schema_invalid")
    note_control_db_raw = activation.get("control_db_path")
    try:
        note_control_db = _resident_control_db_path(note_control_db_raw)
    except ExternalWriteFenceError as exc:
        raise ExternalWriteFenceError("resident_release_control_db_mismatch") from exc
    if (
        not isinstance(note_control_db_raw, str)
        or note_control_db != expected_control_db
        or note_control_db_raw != str(expected_control_db)
    ):
        raise ExternalWriteFenceError("resident_release_control_db_mismatch")
    note_sha256 = hashlib.sha256(raw).hexdigest()
    fingerprint = str(note.get("release_fingerprint_sha256") or "").strip()
    # This is a one-way v1 contract: legacy activation receipts are not release notes.
    epoch_fingerprint = str(
        epoch.get("release_fingerprint_sha256") or ""
    ).strip()
    epoch_receipt = str(epoch.get("release_note_sha256") or "").strip()
    identity = validated_identity["release_identity"]
    if (
        _SHA256_RE.fullmatch(fingerprint) is None
        or fingerprint == "0" * 64
        or hashlib.sha256(_canonical(identity)).hexdigest() != fingerprint
        or fingerprint != epoch_fingerprint
    ):
        raise ExternalWriteFenceError("resident_release_fingerprint_mismatch")
    if _SHA256_RE.fullmatch(epoch_receipt) is None or note_sha256 != epoch_receipt:
        raise ExternalWriteFenceError("resident_release_note_sha256_mismatch")
    host = identity.get("host")
    worker = identity.get("worker")
    pipeline = identity.get("pipeline")
    report = identity.get("report_service")
    projection = note.get("runtime_projection")
    runtime_path = Path(runtime_root).expanduser()
    if not runtime_path.is_absolute():
        raise ExternalWriteFenceError("resident_release_runtime_commit_mismatch")
    expected_root = str(runtime_path)
    expected_commit = str(runtime_commit or "").strip()
    expected_tree = str(runtime_tree or "").strip()
    expected_manifest = str(live_manifest_sha256 or "").strip()
    if (
        not isinstance(host, Mapping)
        or str(host.get("runtime_root") or "").strip() != expected_root
        or str(host.get("commit") or "").strip() != expected_commit
        or re.fullmatch(r"[0-9a-f]{40}", expected_commit) is None
        or str(host.get("tree") or "").strip() != expected_tree
        or re.fullmatch(r"[0-9a-f]{40}", expected_tree) is None
    ):
        raise ExternalWriteFenceError("resident_release_runtime_commit_mismatch")
    if (
        not isinstance(worker, Mapping)
        or not Path(str(worker.get("runtime_root") or "")).is_absolute()
        or not Path(str(pipeline.get("runtime_root") or "")).is_absolute()
        or not isinstance(report, Mapping)
        or _SHA256_RE.fullmatch(str(report.get("manifest_sha256") or "")) is None
        or str(report.get("pipeline_commit") or "") != str(pipeline.get("commit"))
        or str(report.get("pipeline_tree") or "") != str(pipeline.get("tree"))
    ):
        raise ExternalWriteFenceError("resident_release_identity_invalid")
    if (
        not isinstance(projection, Mapping)
        or str(projection.get("live_manifest_sha256") or "").strip()
        != expected_manifest
        or _SHA256_RE.fullmatch(expected_manifest) is None
    ):
        raise ExternalWriteFenceError("resident_release_manifest_mismatch")
    expected_env = str(projection.get("env_sha256") or "").strip()
    try:
        env_lexical = env_path.lstat()
        env_descriptor = os.open(env_path, flags)
    except OSError as exc:
        raise ExternalWriteFenceError(
            "resident_release_env_unavailable", type(exc).__name__
        ) from exc
    try:
        env_before = os.fstat(env_descriptor)
        env_raw = b""
        while len(env_raw) <= MAX_MINIMAL_RELEASE_NOTE_BYTES:
            chunk = os.read(
                env_descriptor,
                min(1024 * 1024, MAX_MINIMAL_RELEASE_NOTE_BYTES + 1 - len(env_raw)),
            )
            if not chunk:
                break
            env_raw += chunk
        env_after = os.fstat(env_descriptor)
    finally:
        os.close(env_descriptor)
    if (
        not env_path.is_absolute()
        or stat.S_ISLNK(env_lexical.st_mode)
        or not stat.S_ISREG(env_lexical.st_mode)
        or env_lexical.st_uid != os.getuid()
        or env_lexical.st_nlink != 1
        or stat.S_IMODE(env_lexical.st_mode) & 0o077
        or len(env_raw) > MAX_MINIMAL_RELEASE_NOTE_BYTES
        or (
            env_lexical.st_dev,
            env_lexical.st_ino,
            env_lexical.st_size,
            env_lexical.st_mtime_ns,
        )
        != (
            env_before.st_dev,
            env_before.st_ino,
            env_before.st_size,
            env_before.st_mtime_ns,
        )
        or (
            env_before.st_dev,
            env_before.st_ino,
            env_before.st_size,
            env_before.st_mtime_ns,
        )
        != (
            env_after.st_dev,
            env_after.st_ino,
            env_after.st_size,
            env_after.st_mtime_ns,
        )
        or not stat.S_ISREG(env_before.st_mode)
        or env_before.st_uid != os.getuid()
        or env_before.st_nlink != 1
        or stat.S_IMODE(env_before.st_mode) & 0o077
        or _SHA256_RE.fullmatch(expected_env) is None
        or hashlib.sha256(env_raw).hexdigest() != expected_env
    ):
        raise ExternalWriteFenceError("resident_release_env_mismatch")
    return {
        "epoch_id": str(epoch.get("epoch_id") or ""),
        "release_id": validated_identity["release_id"],
        "release_fingerprint_sha256": fingerprint,
        "release_note_path": str(path),
        "release_note_sha256": note_sha256,
        "runtime_root": expected_root,
        "runtime_commit": expected_commit,
        "runtime_tree": expected_tree,
        "live_manifest_sha256": expected_manifest,
        "live_env_sha256": expected_env,
    }


def validate_bound_resident_release(
    store: Any,
    *,
    release_note_path: str | Path,
    runtime_root: str | Path,
    runtime_commit: str,
    runtime_tree: str,
    live_manifest_sha256: str,
    live_env_path: str | Path,
    expected_epoch_id: str = "",
    expected_fingerprint: str = "",
    expected_note_sha256: str = "",
) -> dict[str, Any]:
    """Revalidate the current steady epoch against one immutable release note."""

    try:
        control_db_path = store.db_path
    except (AttributeError, TypeError, ValueError) as exc:
        raise ExternalWriteFenceError(
            "resident_release_control_db_path_unavailable"
        ) from exc
    control_db_path = _resident_control_db_path(control_db_path)
    epoch = require_resident_activation_epoch(store, allowed_states={"steady_active"})
    binding = validate_resident_release_note(
        epoch,
        release_note_path=release_note_path,
        runtime_root=runtime_root,
        runtime_commit=runtime_commit,
        runtime_tree=runtime_tree,
        live_manifest_sha256=live_manifest_sha256,
        live_env_path=live_env_path,
        control_db_path=control_db_path,
    )
    if expected_epoch_id and binding["epoch_id"] != expected_epoch_id:
        raise ExternalWriteFenceError("resident_release_binding_changed")
    if (
        expected_fingerprint
        and binding["release_fingerprint_sha256"] != expected_fingerprint
    ):
        raise ExternalWriteFenceError("resident_release_binding_changed")
    if expected_note_sha256 and binding["release_note_sha256"] != expected_note_sha256:
        raise ExternalWriteFenceError("resident_release_binding_changed")
    return binding


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise ExternalWriteFenceError("external_write_fence_schema_invalid") from exc


def canonical_write_fence_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _text(name: str, value: Any, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise ExternalWriteFenceError("external_write_fence_schema_invalid", name)
    result = value.strip()
    if not result and not allow_empty:
        raise ExternalWriteFenceError("external_write_fence_schema_invalid", name)
    return result


def _sha(name: str, value: Any) -> str:
    result = _text(name, value)
    if _SHA256_RE.fullmatch(result) is None:
        raise ExternalWriteFenceError("external_write_fence_schema_invalid", name)
    return result


def _utc(value: Any, name: str) -> datetime:
    raw = _text(name, value)
    try:
        parsed = datetime.fromisoformat(
            raw[:-1] + "+00:00" if raw.endswith("Z") else raw
        )
    except (TypeError, ValueError, OverflowError) as exc:
        raise ExternalWriteFenceError(
            "external_write_fence_schema_invalid", name
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ExternalWriteFenceError("external_write_fence_schema_invalid", name)
    return parsed.astimezone(timezone.utc)


def canonical_utc(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _mapping_to_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if hasattr(value, "to_dict") and callable(value.to_dict):
        result = value.to_dict()
        if isinstance(result, Mapping):
            return dict(result)
    raise ExternalWriteFenceError("external_write_fence_schema_invalid")


def snapshot_core_payload(snapshot: Any) -> dict[str, Any]:
    """Return the snapshot identity without its write-fence slot.

    The original W3 snapshot hash includes ``write_fence``.  Binding the issued
    fence to that final hash would be circular, so W5 binds to this explicitly
    named core digest and then recomputes the final W3 hash over the issued slot.
    """

    if hasattr(snapshot, "canonical_request"):
        request = snapshot.canonical_request
        request = request.to_dict() if hasattr(request, "to_dict") else request
        resolved = snapshot.resolved_admission
        execution = snapshot.execution_admission
        schema_version = snapshot.schema_version
        request_sha256 = snapshot.request_sha256
    else:
        mapping = _mapping_to_dict(snapshot)
        request = mapping.get("canonical_request")
        resolved = mapping.get("resolved_admission")
        execution = mapping.get("execution_admission")
        schema_version = mapping.get("schema_version")
        request_sha256 = mapping.get("request_sha256")
    return {
        "schema_version": schema_version,
        "request_sha256": request_sha256,
        "canonical_request": _mapping_to_dict(request),
        "resolved_admission": _mapping_to_dict(resolved),
        "execution_admission": _mapping_to_dict(execution),
    }


def snapshot_core_sha256(snapshot: Any) -> str:
    return canonical_write_fence_sha256(snapshot_core_payload(snapshot))


def _snapshot_value(snapshot: Any, name: str, default: Any = None) -> Any:
    if hasattr(snapshot, name):
        return getattr(snapshot, name)
    return _mapping_to_dict(snapshot).get(name, default)


def _request_ticket(snapshot: Any) -> Mapping[str, Any]:
    request = _snapshot_value(snapshot, "canonical_request", {})
    request = request.to_dict() if hasattr(request, "to_dict") else request
    if not isinstance(request, Mapping) or not isinstance(
        request.get("ticket"), Mapping
    ):
        raise ExternalWriteFenceError("external_write_fence_identity_mismatch")
    return request["ticket"]


def target_set_sha256(target_set: Mapping[str, Any]) -> str:
    if not isinstance(target_set, Mapping):
        raise ExternalWriteFenceError("external_write_fence_schema_invalid")
    normalized = dict(target_set)
    allowed_fields = {"issue_target", "thread_target"}
    if "chat_id" in normalized:
        allowed_fields.add("chat_id")
    if set(normalized) != allowed_fields:
        raise ExternalWriteFenceError("external_write_fence_schema_invalid")
    normalized["issue_target"] = _text(
        "target_set.issue_target", normalized.get("issue_target")
    )
    thread_target = normalized.get("thread_target")
    if thread_target is not None:
        normalized["thread_target"] = _text("target_set.thread_target", thread_target)
    if "chat_id" in normalized:
        normalized["chat_id"] = _text(
            "target_set.chat_id", normalized.get("chat_id"), allow_empty=True
        )
    return canonical_write_fence_sha256(normalized)


def write_target_set_from_source_envelope(
    source_envelope: Any,
) -> dict[str, Any]:
    """Rebuild the only target set authorized by the immutable W3 source."""

    envelope = _mapping_to_dict(source_envelope)
    source_kind = _text("source_kind", envelope.get("source_kind"))
    anchor = _mapping_to_dict(envelope.get("anchor"))
    metadata = _mapping_to_dict(envelope.get("source_metadata"))
    if set(anchor) != {"issue_target", "thread_target"}:
        raise ExternalWriteFenceError("external_write_fence_schema_invalid")
    issue_target = _text("anchor.issue_target", anchor.get("issue_target"))
    raw_thread_target = anchor.get("thread_target")
    thread_target = (
        None
        if raw_thread_target is None
        else _text("anchor.thread_target", raw_thread_target)
    )
    target_set: dict[str, Any] = {
        "issue_target": issue_target,
        "thread_target": thread_target,
    }
    if source_kind == "kafka_workflow_event":
        if thread_target is not None:
            raise ExternalWriteFenceError("external_write_fence_target_mismatch")
    elif source_kind == "feishu_group_manual":
        platform = _text("source_metadata.platform", metadata.get("platform"))
        chat_id = _text(
            "source_metadata.chat_id",
            metadata.get("chat_id"),
            allow_empty=platform == "operator",
        )
        metadata_thread = _text(
            "source_metadata.thread_id",
            metadata.get("thread_id"),
            allow_empty=platform == "operator",
        )
        if platform == "feishu":
            if not chat_id or thread_target != metadata_thread:
                raise ExternalWriteFenceError("external_write_fence_target_mismatch")
        elif platform == "operator":
            if chat_id or metadata_thread or thread_target is not None:
                raise ExternalWriteFenceError("external_write_fence_target_mismatch")
        else:
            raise ExternalWriteFenceError("external_write_fence_schema_invalid")
        target_set["chat_id"] = chat_id
    else:
        raise ExternalWriteFenceError("external_write_fence_schema_invalid")
    # Normalize and reject any shape drift before returning the raw targets.
    target_set_sha256(target_set)
    return target_set


def validate_write_fence_source_binding(
    fence: Any,
    *,
    snapshot: Any,
    source_envelope: Any,
) -> dict[str, Any]:
    """Bind a fence to its exact immutable W3 snapshot and source targets."""

    snapshot_value = _mapping_to_dict(snapshot)
    envelope_value = _mapping_to_dict(source_envelope)
    snapshot_fields = {
        "schema_version",
        "snapshot_id",
        "snapshot_sha256",
        "request_sha256",
        "canonical_request",
        "resolved_admission",
        "execution_admission",
        "write_fence",
    }
    if set(snapshot_value) != snapshot_fields:
        raise ExternalWriteFenceError("external_write_fence_schema_invalid")
    snapshot_identity = {
        key: snapshot_value[key]
        for key in (
            "schema_version",
            "request_sha256",
            "canonical_request",
            "resolved_admission",
            "execution_admission",
            "write_fence",
        )
    }
    final_snapshot_sha256 = canonical_write_fence_sha256(snapshot_identity)
    if (
        snapshot_value.get("snapshot_sha256") != final_snapshot_sha256
        or snapshot_value.get("snapshot_id")
        != f"pnc-rca-snapshot-v1-{final_snapshot_sha256}"
    ):
        raise ExternalWriteFenceError("external_write_fence_identity_mismatch")

    envelope_fields = {
        "schema_version",
        "source_envelope_id",
        "source_envelope_sha256",
        "source_authority_sha256",
        "snapshot_id",
        "snapshot_sha256",
        "submission_key",
        "source_id",
        "source_kind",
        "ingress_decision",
        "source_metadata",
        "anchor",
    }
    if (
        set(envelope_value) != envelope_fields
        or envelope_value.get("schema_version") != "pnc_rca_snapshot_source_envelope_v1"
    ):
        raise ExternalWriteFenceError("external_write_fence_schema_invalid")
    envelope_identity = {
        key: envelope_value[key]
        for key in (
            "schema_version",
            "source_authority_sha256",
            "snapshot_id",
            "snapshot_sha256",
            "submission_key",
            "source_id",
            "source_kind",
            "ingress_decision",
            "source_metadata",
            "anchor",
        )
    }
    envelope_sha256 = canonical_write_fence_sha256(envelope_identity)
    if (
        envelope_value.get("source_envelope_sha256") != envelope_sha256
        or envelope_value.get("source_envelope_id")
        != f"pnc-rca-source-envelope-v1-{envelope_sha256}"
    ):
        raise ExternalWriteFenceError("external_write_fence_identity_mismatch")
    try:
        fence_value = _mapping_to_dict(fence)
    except ExternalWriteFenceError:
        raise
    observed_fence = snapshot_value.get("write_fence")
    if not isinstance(observed_fence, Mapping) or canonical_write_fence_sha256(
        observed_fence
    ) != canonical_write_fence_sha256(fence_value):
        raise ExternalWriteFenceError("external_write_fence_identity_mismatch")
    resolved = _mapping_to_dict(snapshot_value.get("resolved_admission"))
    if (
        envelope_value.get("snapshot_id") != snapshot_value.get("snapshot_id")
        or envelope_value.get("snapshot_sha256")
        != snapshot_value.get("snapshot_sha256")
        or envelope_value.get("submission_key") != resolved.get("submission_key")
        or envelope_value.get("submission_key") != fence_value.get("submission_key")
    ):
        raise ExternalWriteFenceError("external_write_fence_identity_mismatch")
    targets = write_target_set_from_source_envelope(envelope_value)
    request = _mapping_to_dict(snapshot_value.get("canonical_request"))
    ticket = _mapping_to_dict(request.get("ticket"))
    if str(targets["issue_target"]).rstrip("/") != str(
        ticket.get("issue_url") or ""
    ).strip().rstrip("/"):
        raise ExternalWriteFenceError("external_write_fence_target_mismatch")
    issued_at = _utc(fence_value.get("issued_at"), "issued_at")
    validate_write_fence(
        fence,
        snapshot=snapshot_value,
        expected_target_set_sha256=target_set_sha256(targets),
        # Validate immutable shape at its issuance instant. Live callers still
        # perform the expiry and current-epoch check at the provider boundary.
        now=issued_at,
    )
    return {
        **targets,
        "target_set_sha256": target_set_sha256(targets),
    }


def write_fence_binding(snapshot: Any) -> dict[str, Any]:
    """Small immutable binding carried by VM/delivery contracts."""
    fence = _snapshot_value(snapshot, "write_fence", {})
    fence = dict(fence) if isinstance(fence, Mapping) else {}
    if fence.get("state") != "issued":
        return {}
    return {
        "write_fence": fence,
        "snapshot_core_sha256": snapshot_core_sha256(snapshot),
    }


def build_issued_write_fence(
    *,
    snapshot: Any,
    activation_epoch_id: str,
    activation_ledger_id: int,
    admission_key: str,
    target_set: Mapping[str, Any],
    now: datetime | None = None,
    expires_at: datetime | None = None,
    allowed_write_kinds: Any = None,
    tenant_id: str | None = None,
) -> dict[str, Any]:
    """Build the canonical fence payload; no external side effect occurs here."""

    core_sha = snapshot_core_sha256(snapshot)
    epoch = _text("activation_epoch_id", activation_epoch_id)
    if (
        isinstance(activation_ledger_id, bool)
        or not isinstance(activation_ledger_id, int)
        or activation_ledger_id < 1
    ):
        raise ExternalWriteFenceError(
            "external_write_fence_schema_invalid", "activation_ledger_id"
        )
    key = _text("admission_key", admission_key)
    resolved = _snapshot_value(snapshot, "resolved_admission", {})
    business = _text("business_key", resolved.get("business_key"))
    submission = _text("submission_key", resolved.get("submission_key"))
    generation = resolved.get("generation")
    if (
        isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation < 1
    ):
        raise ExternalWriteFenceError(
            "external_write_fence_schema_invalid", "generation"
        )
    execution = _snapshot_value(snapshot, "execution_admission", {})
    if (
        execution.get("decision") != "admit"
        or execution.get("legacy_unconfigured") is True
    ):
        raise ExternalWriteFenceError(
            "external_write_fence_operation_denied",
            "snapshot is not an admitted active execution",
        )
    if (
        str(execution.get("activation_epoch_id") or "") != epoch
        or int(execution.get("activation_ledger_id") or 0) != activation_ledger_id
    ):
        raise ExternalWriteFenceError(
            "external_write_fence_identity_mismatch",
            "execution admission binding differs",
        )
    profile = {}
    request = _snapshot_value(snapshot, "canonical_request", {})
    if isinstance(request, Mapping) and isinstance(
        request.get("business_profile"), Mapping
    ):
        profile_value = request["business_profile"].get("value")
        if isinstance(profile_value, Mapping):
            profile = dict(profile_value)
    tenant = _text(
        "tenant_id",
        tenant_id
        or profile.get("profile_id")
        or _request_ticket(snapshot).get("project_key"),
    )
    point = now or datetime.now(timezone.utc)
    if point.tzinfo is None or point.utcoffset() is None:
        raise ExternalWriteFenceError("external_write_fence_schema_invalid", "now")
    expiry = expires_at or (point.astimezone(timezone.utc) + WRITE_FENCE_MAX_LIFETIME)
    if expiry.tzinfo is None or expiry.utcoffset() is None:
        raise ExternalWriteFenceError(
            "external_write_fence_schema_invalid", "expires_at"
        )
    if expiry <= point or expiry - point > WRITE_FENCE_MAX_LIFETIME:
        raise ExternalWriteFenceError(
            "external_write_fence_schema_invalid", "expires_at"
        )
    kinds = sorted(set(allowed_write_kinds or WRITE_FENCE_ALLOWED_KINDS))
    if not kinds or any(kind not in WRITE_FENCE_ALLOWED_KINDS for kind in kinds):
        raise ExternalWriteFenceError(
            "external_write_fence_schema_invalid", "allowed_write_kinds"
        )
    payload = {
        "schema_version": WRITE_FENCE_SCHEMA_VERSION,
        "admission_snapshot_sha256": core_sha,
        "activation_epoch_id": epoch,
        "activation_ledger_id": activation_ledger_id,
        "admission_key": key,
        "tenant_id": tenant,
        "business_key": business,
        "submission_key": submission,
        "generation": generation,
        "target_set_sha256": target_set_sha256(target_set),
        "allowed_write_kinds": kinds,
        "issued_at": canonical_utc(point),
        "expires_at": canonical_utc(expiry),
    }
    return {
        **payload,
        "schema_version": WRITE_FENCE_SCHEMA_VERSION,
        "fence_id": WRITE_FENCE_ID_PREFIX + canonical_write_fence_sha256(payload),
        "state": "issued",
    }


def validate_write_fence(
    fence: Any,
    *,
    snapshot: Any | None = None,
    snapshot_core_sha256_value: str | None = None,
    operation: str | None = None,
    target: str | None = None,
    expected_epoch_id: str | None = None,
    expected_ledger_id: int | None = None,
    expected_business_key: str | None = None,
    expected_submission_key: str | None = None,
    expected_generation: int | None = None,
    expected_tenant_id: str | None = None,
    expected_issue_target: str | None = None,
    expected_thread_target: str | None = None,
    expected_target_set_sha256: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Validate an issued fence and optionally its live operation binding."""

    if fence is None or fence == {}:
        raise ExternalWriteFenceError("external_write_fence_missing")
    if not isinstance(fence, Mapping) or set(fence) != WRITE_FENCE_FIELDS:
        raise ExternalWriteFenceError("external_write_fence_schema_invalid")
    value = dict(fence)
    if (
        value["schema_version"] != WRITE_FENCE_SCHEMA_VERSION
        or value["state"] != "issued"
    ):
        raise ExternalWriteFenceError("external_write_fence_schema_invalid")
    fence_id = _text("fence_id", value.get("fence_id"))
    if not fence_id.startswith(WRITE_FENCE_ID_PREFIX):
        raise ExternalWriteFenceError("external_write_fence_schema_invalid")
    payload = {key: value[key] for key in value if key not in {"fence_id", "state"}}
    if fence_id != WRITE_FENCE_ID_PREFIX + canonical_write_fence_sha256(payload):
        raise ExternalWriteFenceError("external_write_fence_schema_invalid")
    core_sha = snapshot_core_sha256_value or (
        snapshot_core_sha256(snapshot) if snapshot is not None else None
    )
    if core_sha is not None and value["admission_snapshot_sha256"] != _sha(
        "admission_snapshot_sha256", core_sha
    ):
        raise ExternalWriteFenceError("external_write_fence_identity_mismatch")
    epoch = _text("activation_epoch_id", value.get("activation_epoch_id"))
    ledger = value.get("activation_ledger_id")
    if isinstance(ledger, bool) or not isinstance(ledger, int) or ledger < 1:
        raise ExternalWriteFenceError("external_write_fence_schema_invalid")
    if expected_epoch_id is not None and epoch != _text(
        "expected_epoch_id", expected_epoch_id
    ):
        raise ExternalWriteFenceError("external_write_fence_epoch_not_current")
    if expected_ledger_id is not None and ledger != expected_ledger_id:
        raise ExternalWriteFenceError("external_write_fence_epoch_not_current")
    if expected_business_key is not None and value["business_key"] != _text(
        "expected_business_key", expected_business_key
    ):
        raise ExternalWriteFenceError("external_write_fence_identity_mismatch")
    if expected_submission_key is not None and value["submission_key"] != _text(
        "expected_submission_key", expected_submission_key
    ):
        raise ExternalWriteFenceError("external_write_fence_identity_mismatch")
    if expected_generation is not None and value["generation"] != expected_generation:
        raise ExternalWriteFenceError("external_write_fence_identity_mismatch")
    if expected_tenant_id is not None and value["tenant_id"] != _text(
        "expected_tenant_id", expected_tenant_id
    ):
        raise ExternalWriteFenceError("external_write_fence_identity_mismatch")
    if snapshot is not None:
        resolved = _snapshot_value(snapshot, "resolved_admission", {})
        execution = _snapshot_value(snapshot, "execution_admission", {})
        if not isinstance(execution, Mapping):
            raise ExternalWriteFenceError("external_write_fence_schema_invalid")
        if (
            value["business_key"] != resolved.get("business_key")
            or value["submission_key"] != resolved.get("submission_key")
            or value["generation"] != resolved.get("generation")
        ):
            raise ExternalWriteFenceError("external_write_fence_identity_mismatch")
        if (
            execution.get("decision") != "admit"
            or execution.get("legacy_unconfigured") is True
        ):
            raise ExternalWriteFenceError("external_write_fence_operation_denied")
        execution_epoch = execution.get("activation_epoch_id")
        execution_ledger = execution.get("activation_ledger_id")
        if (
            not isinstance(execution_epoch, str)
            or not execution_epoch.strip()
            or isinstance(execution_ledger, bool)
            or not isinstance(execution_ledger, int)
            or execution_ledger < 1
        ):
            raise ExternalWriteFenceError("external_write_fence_schema_invalid")
        if (
            value["activation_epoch_id"] != execution_epoch
            or value["activation_ledger_id"] != execution_ledger
        ):
            raise ExternalWriteFenceError("external_write_fence_identity_mismatch")
    target_hash = value["target_set_sha256"]
    _sha("target_set_sha256", target_hash)
    if expected_target_set_sha256 is not None and target_hash != _sha(
        "expected_target_set_sha256", expected_target_set_sha256
    ):
        raise ExternalWriteFenceError("external_write_fence_target_mismatch")
    kinds = value["allowed_write_kinds"]
    if (
        not isinstance(kinds, (list, tuple))
        or list(kinds) != sorted(set(kinds))
        or not kinds
        or any(kind not in WRITE_FENCE_ALLOWED_KINDS for kind in kinds)
    ):
        raise ExternalWriteFenceError("external_write_fence_schema_invalid")
    issued = _utc(value["issued_at"], "issued_at")
    expires = _utc(value["expires_at"], "expires_at")
    observed = now or datetime.now(timezone.utc)
    if observed.tzinfo is None or observed.utcoffset() is None:
        raise ExternalWriteFenceError("external_write_fence_schema_invalid", "now")
    observed = observed.astimezone(timezone.utc)
    if expires <= issued or expires - issued > WRITE_FENCE_MAX_LIFETIME:
        raise ExternalWriteFenceError("external_write_fence_schema_invalid")
    if issued - observed > WRITE_FENCE_MAX_FUTURE_SKEW:
        raise ExternalWriteFenceError(
            "external_write_fence_schema_invalid", "issued_at_future"
        )
    if observed >= expires:
        raise ExternalWriteFenceError("external_write_fence_expired")
    if operation is not None:
        op = str(operation or "").strip()
        if op == "feishu_field_update":
            op = "feishu_issue_field_update"
        if op not in kinds:
            raise ExternalWriteFenceError("external_write_fence_operation_denied")
        observed_target = str(target or "").strip()
        if op in {"vm_submit", "internal_alert"}:
            expected = value["submission_key"]
            if observed_target and observed_target != expected:
                raise ExternalWriteFenceError("external_write_fence_target_mismatch")
        elif op in {
            "feishu_issue_comment",
            "feishu_issue_field_update",
            "feishu_card_create",
            "feishu_card_patch",
            "feishu_attachment_upload",
        }:
            if expected_issue_target is not None and observed_target.rstrip("/") != str(
                expected_issue_target
            ).strip().rstrip("/"):
                raise ExternalWriteFenceError("external_write_fence_target_mismatch")
        elif op == "feishu_thread_reply":
            expected = str(expected_thread_target or "").strip()
            if expected and observed_target and observed_target != expected:
                raise ExternalWriteFenceError("external_write_fence_target_mismatch")
    return value


def issue_snapshot_write_fence(
    snapshot: Any,
    *,
    activation_epoch_id: str,
    activation_ledger_id: int,
    admission_key: str,
    target_set: Mapping[str, Any],
    now: datetime | None = None,
    expires_at: datetime | None = None,
    allowed_write_kinds: Any = None,
    tenant_id: str | None = None,
) -> Any:
    """Return a new W3 snapshot carrying an issued fence and final hash."""

    from gateway.pnc_rca_snapshot import AdmissionSnapshot, canonical_json_sha256

    if not isinstance(snapshot, AdmissionSnapshot):
        raise ExternalWriteFenceError("external_write_fence_identity_mismatch")
    current_fence = dict(snapshot.write_fence)
    if current_fence.get("state") == "issued":
        validate_write_fence(current_fence, snapshot=snapshot)
        return snapshot
    if current_fence != {
        "schema_version": "pnc_rca_write_fence_slot_v1",
        "state": "unissued",
    }:
        raise ExternalWriteFenceError("external_write_fence_schema_invalid")
    issued = build_issued_write_fence(
        snapshot=snapshot,
        activation_epoch_id=activation_epoch_id,
        activation_ledger_id=activation_ledger_id,
        admission_key=admission_key,
        target_set=target_set,
        now=now,
        expires_at=expires_at,
        allowed_write_kinds=allowed_write_kinds,
        tenant_id=tenant_id,
    )
    identity = {
        "schema_version": snapshot.schema_version,
        "request_sha256": snapshot.request_sha256,
        "canonical_request": snapshot.canonical_request.to_dict(),
        "resolved_admission": dict(snapshot.resolved_admission),
        "execution_admission": dict(snapshot.execution_admission),
        "write_fence": issued,
    }
    digest = canonical_json_sha256(identity)
    return AdmissionSnapshot(
        schema_version=snapshot.schema_version,
        snapshot_id=f"pnc-rca-snapshot-v1-{digest}",
        snapshot_sha256=digest,
        request_sha256=snapshot.request_sha256,
        canonical_request=snapshot.canonical_request,
        resolved_admission=dict(snapshot.resolved_admission),
        execution_admission=dict(snapshot.execution_admission),
        write_fence=issued,
    )


__all__ = [
    "ExternalWriteFenceError",
    "MINIMAL_RELEASE_HOST_REMOTE",
    "MINIMAL_RELEASE_NOTE_SCHEMA_VERSION",
    "MINIMAL_RELEASE_PRODUCTION_DEFINITION",
    "MinimalReleaseNoteIdentityError",
    "RESIDENT_ACTIVATION_EPOCH_STATES",
    "RESIDENT_EXTERNAL_WRITE_STATES",
    "RESIDENT_INGRESS_OPEN_STATES",
    "WRITE_FENCE_ALLOWED_KINDS",
    "WRITE_FENCE_FIELDS",
    "WRITE_FENCE_ID_PREFIX",
    "WRITE_FENCE_MAX_LIFETIME",
    "WRITE_FENCE_SCHEMA_VERSION",
    "build_issued_write_fence",
    "canonical_utc",
    "canonical_write_fence_sha256",
    "issue_snapshot_write_fence",
    "require_resident_activation_epoch",
    "snapshot_core_payload",
    "snapshot_core_sha256",
    "target_set_sha256",
    "validate_write_fence",
    "validate_write_fence_source_binding",
    "validate_bound_resident_release",
    "validate_minimal_release_note_identity",
    "validate_resident_release_note",
    "write_target_set_from_source_envelope",
    "write_fence_binding",
]
