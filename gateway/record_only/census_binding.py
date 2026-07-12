#!/usr/bin/env python3
"""Fail-closed binding to the authoritative target-only outbound census."""

from __future__ import annotations

import errno
import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


INDEX_NAME = "INDEX.json"
EXPECTED_INDEX_SHA256 = "b6bcfb3a597da616bec2acc8e57eea18695b0bb20e29446926cf2eb2e3f81914"
EXPECTED_INDEX_SCHEMA = "hermes_target_only_evidence_index_v1"
EXPECTED_ARTIFACT_NAME = "census-v4.json"
EXPECTED_ARTIFACT_SHA256 = "d2c17c7b03642074d301259437f17cc879e8adfbd91d07029c2dda775a563e63"
EXPECTED_REPRODUCTION_NAME = "census-v4.repro.json"
EXPECTED_CENSUS_SCHEMA = "hermes_static_outbound_census_v1"
EXPECTED_STATUS = "PROVISIONAL_STATIC_OUTBOUND_CENSUS_NO_GO"
EXPECTED_GATE_DECISION = "NO_GO"
EXPECTED_SOURCE_COMMIT = "9de9c25f620ff7f1ce0fd5457d596052d5159596"
EXPECTED_SOURCE_TREE = "1624297419fab639f57302244f6bb28b161bd014"
EXPECTED_SOURCE_SHA256_MANIFEST_SHA256 = (
    "8df101b0f85845864b3956266b3c4a07412ad44b4cac05acea9d8a265a4c6dbe"
)
EXPECTED_SOURCE_TREE_MANIFEST_SHA256 = (
    "320341a68141ee65be7e25463ef27370d83ba2a0f006fafe5c19239ef69c6c0f"
)
EXPECTED_SUPERSEDED = (
    "census-v1.json",
    "census-v2.json",
    "census-v3.json",
    "census-v3.repro.json",
)
EXPECTED_MODE_ANOMALIES = (
    ".github/pr-screenshots/39327/providers-collapsed.png",
    ".github/pr-screenshots/39327/providers-expanded.png",
    ".github/pr-screenshots/39327/tools-collapsed.png",
    ".github/pr-screenshots/39327/tools-expanded.png",
    "optional-skills/devops/docker-management/SKILL.md",
)
EXPECTED_SCAN_SUFFIXES = (
    ".bash",
    ".cjs",
    ".cts",
    ".go",
    ".js",
    ".jsx",
    ".mjs",
    ".mts",
    ".ps1",
    ".py",
    ".rb",
    ".rs",
    ".sh",
    ".ts",
    ".tsx",
    ".zsh",
)
EXPECTED_BY_LANGUAGE = {
    "javascript": 211,
    "powershell": 12,
    "python": 6052,
    "rust": 19,
    "shell": 44,
}
EXPECTED_RUNTIME_BY_CATEGORY = {
    "cloud_sdk": 5,
    "cloud_sdk_import": 3,
    "generic_connect_candidate": 2,
    "generic_http_candidate": 801,
    "generic_send_candidate": 19,
    "http_cli": 49,
    "http_fetch": 39,
    "http_sdk": 666,
    "http_sdk_import": 210,
    "http_stdlib": 473,
    "http_stdlib_import": 230,
    "network_or_subprocess_import": 29,
    "network_protocol": 1,
    "network_protocol_import": 3,
    "platform_sdk": 45,
    "platform_sdk_import": 32,
    "platform_send_candidate": 33,
    "provider_sdk": 18,
    "provider_sdk_import": 27,
    "remote_exec": 2,
    "socket": 39,
    "socket_cli": 1,
    "socket_import": 29,
    "subprocess": 531,
    "subprocess_candidate": 118,
    "subprocess_import": 149,
    "websocket": 38,
    "websocket_import": 20,
}
EXPECTED_COUNTS = {
    "rows": 6338,
    "runtime_rows": 3612,
    "test_rows": 2726,
    "by_language": EXPECTED_BY_LANGUAGE,
    "runtime_by_category": EXPECTED_RUNTIME_BY_CATEGORY,
}
EXPECTED_BLOCKERS = (
    "every runtime census row must be classified and bound to record-only, disabled, or explicitly isolated behavior",
    "dynamic imports, reflection, native binaries, generated bundles, plugins, skills, and tool-driven subprocesses require runtime egress tracing",
    "credential stripping and deny-network containment remain independent mandatory gates",
    "this static census does not authorize candidate execution or production outbound",
    "all parse errors, oversized exclusions, and unclassified executable-mode files must be resolved",
)

_INDEX_FIELDS = {
    "authoritative_scope",
    "candidate_execution_authorized",
    "canonical",
    "gate_decision",
    "not_authoritative_for",
    "production_ready",
    "promotion_authorized",
    "provisional_target_only",
    "schema_version",
    "status",
    "superseded",
    "superseded_classification",
}
_CENSUS_FIELDS = {
    "all_runtime_rows_classified",
    "blockers",
    "candidate_execution_authorized",
    "counts",
    "gate_decision",
    "production_ready",
    "promotion_authorized",
    "provisional_target_only",
    "record_only_coverage_complete",
    "rows",
    "runtime_egress_trace_complete",
    "scanner",
    "schema_version",
    "source_commit",
    "source_sha256_manifest",
    "source_sha256_manifest_sha256",
    "source_tree",
    "source_tree_manifest",
    "source_tree_manifest_sha256",
    "status",
}
_ROW_FIELDS = {
    "category",
    "confidence",
    "is_test",
    "language",
    "line",
    "path",
    "record_only_coverage",
    "review_status",
    "symbol",
}
_SAFE_LEAF_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_HEX_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_HEX_GIT_RE = re.compile(r"^[0-9a-f]{40}$")


class CensusBindingError(RuntimeError):
    """Raised when target outbound evidence does not match the frozen binding."""


@dataclass(frozen=True)
class VerifiedCensusBinding:
    index_name: str
    index_sha256: str
    artifact_name: str
    artifact_sha256: str
    status: str
    gate_decision: str
    source_commit: str
    source_tree: str
    source_sha256_manifest_sha256: str
    source_tree_manifest_sha256: str
    manifest_files: int
    scanned_files: int
    total_rows: int
    runtime_rows: int
    test_rows: int
    pending_rows: int
    unverified_rows: int
    unclassified_executable_modes: tuple[str, ...]
    superseded: tuple[str, ...]

    def as_ledger_binding(self) -> dict[str, Any]:
        return {
            "index_sha256": self.index_sha256,
            "canonical_artifact": self.artifact_name,
            "canonical_artifact_sha256": self.artifact_sha256,
            "status": self.status,
            "gate_decision": self.gate_decision,
            "source_commit": self.source_commit,
            "source_tree": self.source_tree,
            "source_sha256_manifest_sha256": self.source_sha256_manifest_sha256,
            "source_tree_manifest_sha256": self.source_tree_manifest_sha256,
            "manifest_files": self.manifest_files,
            "scanned_files": self.scanned_files,
            "total_rows": self.total_rows,
            "runtime_rows": self.runtime_rows,
            "test_rows": self.test_rows,
            "pending_rows": self.pending_rows,
            "unverified_rows": self.unverified_rows,
            "unclassified_executable_mode_count": len(
                self.unclassified_executable_modes
            ),
            "all_runtime_rows_classified": False,
            "runtime_egress_trace_complete": False,
            "dynamic_import_trace_complete": False,
            "skill_trace_complete": False,
            "subprocess_descendant_trace_complete": False,
            "record_only_coverage_complete": False,
            "production_ready": False,
            "promotion_authorized": False,
            "candidate_execution_authorized": False,
            "cutover_authorized": False,
            "external_delivery_attempted": False,
            "external_delivery_verified": False,
        }

    def as_status(self) -> dict[str, Any]:
        return {
            "index": {
                "artifact": self.index_name,
                "sha256": self.index_sha256,
                "schema_version": EXPECTED_INDEX_SCHEMA,
            },
            "canonical_artifact": {
                "artifact": self.artifact_name,
                "sha256": self.artifact_sha256,
                "schema_version": EXPECTED_CENSUS_SCHEMA,
            },
            "status": self.status,
            "gate_decision": self.gate_decision,
            "source_commit": self.source_commit,
            "source_tree": self.source_tree,
            "source_sha256_manifest_sha256": self.source_sha256_manifest_sha256,
            "source_tree_manifest_sha256": self.source_tree_manifest_sha256,
            "manifest_files": self.manifest_files,
            "scanned_files": self.scanned_files,
            "total_rows": self.total_rows,
            "runtime_rows": self.runtime_rows,
            "test_rows": self.test_rows,
            "pending_rows": self.pending_rows,
            "unverified_rows": self.unverified_rows,
            "all_runtime_rows_classified": False,
            "record_only_coverage_complete": False,
            "runtime_egress_trace_complete": False,
            "dynamic_import_trace_complete": False,
            "skill_trace_complete": False,
            "subprocess_descendant_trace_complete": False,
            "unclassified_executable_mode_count": len(
                self.unclassified_executable_modes
            ),
            "unclassified_executables": list(self.unclassified_executable_modes),
            "superseded": list(self.superseded),
            "production_ready": False,
            "promotion_authorized": False,
            "candidate_execution_authorized": False,
            "cutover_authorized": False,
            "external_delivery_attempted": False,
            "external_delivery_verified": False,
        }


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _strict_json_loads(raw: bytes, *, context: str) -> Any:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise CensusBindingError(f"duplicate JSON key in {context}: {key}")
            value[key] = item
        return value

    def reject_constant(value: str) -> None:
        raise CensusBindingError(f"non-finite JSON value in {context}: {value}")

    try:
        return json.loads(
            raw,
            object_pairs_hook=reject_duplicates,
            parse_constant=reject_constant,
        )
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise CensusBindingError(f"invalid JSON in {context}: {exc}") from exc


def _safe_leaf_name(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not _SAFE_LEAF_RE.fullmatch(value):
        raise CensusBindingError(f"unsafe {field}")
    if Path(value).name != value or value in {".", ".."} or "/" in value or "\\" in value:
        raise CensusBindingError(f"path traversal in {field}")
    return value


def _validate_evidence_dir(path: Path) -> tuple[Path, int, tuple[int, int]]:
    if not isinstance(path, Path) or not path.is_absolute():
        raise CensusBindingError("evidence root must be an absolute Path")
    try:
        before = path.lstat()
    except OSError as exc:
        raise CensusBindingError(f"evidence root unavailable: {exc}") from exc
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISDIR(before.st_mode):
        raise CensusBindingError("evidence root must be a real directory")
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise CensusBindingError(f"evidence root cannot be resolved: {exc}") from exc
    if resolved != path:
        raise CensusBindingError("evidence root must be canonical and traversal-free")
    if before.st_uid != os.getuid() or before.st_mode & 0o022:
        raise CensusBindingError("evidence root ownership or write mode is unsafe")
    flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise CensusBindingError(f"evidence root cannot be safely opened: {exc}") from exc
    opened = os.fstat(fd)
    identity = (opened.st_dev, opened.st_ino)
    if identity != (before.st_dev, before.st_ino):
        os.close(fd)
        raise CensusBindingError("evidence root changed while it was opened")
    return resolved, fd, identity


def _assert_dir_identity(path: Path, fd: int, identity: tuple[int, int]) -> None:
    opened = os.fstat(fd)
    try:
        visible = path.lstat()
    except OSError as exc:
        raise CensusBindingError(f"evidence root disappeared: {exc}") from exc
    if (
        not stat.S_ISDIR(opened.st_mode)
        or stat.S_ISLNK(visible.st_mode)
        or not stat.S_ISDIR(visible.st_mode)
        or opened.st_uid != os.getuid()
        or visible.st_uid != os.getuid()
        or opened.st_mode & 0o022
        or visible.st_mode & 0o022
        or (opened.st_dev, opened.st_ino) != identity
        or (visible.st_dev, visible.st_ino) != identity
    ):
        raise CensusBindingError("evidence root identity changed during verification")


def _secure_read_at(
    directory_fd: int,
    name: str,
    *,
    max_bytes: int,
    context: str,
    after_open_hook: Callable[[str], None] | None = None,
) -> bytes:
    safe_name = _safe_leaf_name(name, field=f"{context} filename")
    try:
        before = os.stat(safe_name, dir_fd=directory_fd, follow_symlinks=False)
    except OSError as exc:
        raise CensusBindingError(f"{context} unavailable: {exc}") from exc
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_uid != os.getuid()
        or before.st_mode & 0o022
        or before.st_nlink != 1
        or before.st_size < 1
        or before.st_size > max_bytes
    ):
        raise CensusBindingError(f"{context} ownership/type/mode/link/size is unsafe")
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        fd = os.open(safe_name, flags, dir_fd=directory_fd)
    except OSError as exc:
        raise CensusBindingError(f"{context} cannot be safely opened: {exc}") from exc
    try:
        opened = os.fstat(fd)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.getuid()
            or opened.st_mode & 0o022
            or opened.st_nlink != 1
            or opened.st_size < 1
            or opened.st_size > max_bytes
            or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
        ):
            raise CensusBindingError(f"{context} changed while it was opened")
        if after_open_hook is not None:
            after_open_hook(safe_name)
        data = bytearray()
        while True:
            block = os.read(fd, 65536)
            if not block:
                break
            data.extend(block)
            if len(data) > max_bytes:
                raise CensusBindingError(f"{context} exceeds bounded verification size")
        after_fd = os.fstat(fd)
        try:
            after_path = os.stat(
                safe_name, dir_fd=directory_fd, follow_symlinks=False
            )
        except OSError as exc:
            raise CensusBindingError(f"{context} path changed during read: {exc}") from exc
        stable_fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")
        if any(getattr(opened, field) != getattr(after_fd, field) for field in stable_fields):
            raise CensusBindingError(f"{context} was modified while it was read")
        if (
            (after_path.st_dev, after_path.st_ino)
            != (after_fd.st_dev, after_fd.st_ino)
            or after_fd.st_uid != os.getuid()
            or after_path.st_uid != os.getuid()
            or after_fd.st_mode & 0o022
            or after_path.st_mode & 0o022
            or after_fd.st_nlink != 1
            or after_path.st_nlink != 1
            or after_fd.st_size < 1
            or after_fd.st_size > max_bytes
            or not stat.S_ISREG(after_path.st_mode)
        ):
            raise CensusBindingError(f"{context} path identity changed during read")
        return bytes(data)
    finally:
        os.close(fd)


def _require_exact_false(mapping: dict[str, Any], fields: tuple[str, ...], *, context: str) -> None:
    for field in fields:
        if mapping.get(field) is not False:
            raise CensusBindingError(f"{context}.{field} must remain exactly false")


def _validate_manifest_reference(value: Any, *, expected_name: str, field: str) -> None:
    if not isinstance(value, str):
        raise CensusBindingError(f"{field} must be a string")
    path = Path(value)
    if not path.is_absolute() or ".." in path.parts or path.name != expected_name:
        raise CensusBindingError(f"{field} is not an absolute expected manifest reference")


def verify_target_outbound_census(
    evidence_root: Path,
    *,
    _after_open_hook: Callable[[str], None] | None = None,
) -> VerifiedCensusBinding:
    """Verify INDEX plus canonical artifact without following mutable path aliases."""

    root, root_fd, root_identity = _validate_evidence_dir(evidence_root)
    try:
        index_raw = _secure_read_at(
            root_fd,
            INDEX_NAME,
            max_bytes=64 * 1024,
            context="outbound census INDEX",
            after_open_hook=_after_open_hook,
        )
        index_sha256 = _sha256(index_raw)
        if index_sha256 != EXPECTED_INDEX_SHA256:
            raise CensusBindingError("outbound census INDEX SHA-256 mismatch")
        index = _strict_json_loads(index_raw, context="outbound census INDEX")
        if not isinstance(index, dict) or set(index) != _INDEX_FIELDS:
            raise CensusBindingError("outbound census INDEX fields do not match schema")
        _require_exact_false(
            index,
            (
                "candidate_execution_authorized",
                "production_ready",
                "promotion_authorized",
            ),
            context="INDEX",
        )
        if (
            index.get("schema_version") != EXPECTED_INDEX_SCHEMA
            or index.get("authoritative_scope")
            != "target-only static outbound review work queue"
            or index.get("provisional_target_only") is not True
            or index.get("status") != EXPECTED_STATUS
            or index.get("gate_decision") != EXPECTED_GATE_DECISION
            or index.get("not_authoritative_for")
            != ["zero-outbound proof", "candidate execution", "promotion", "cutover"]
            or index.get("superseded_classification")
            != "SUPERSEDED_P1_FALSE_GREEN_DO_NOT_INDEX"
            or index.get("superseded") != list(EXPECTED_SUPERSEDED)
        ):
            raise CensusBindingError("outbound census INDEX safety binding mismatch")
        canonical = index.get("canonical")
        if not isinstance(canonical, dict) or set(canonical) != {
            "artifact",
            "reproduction",
            "sha256",
        }:
            raise CensusBindingError("outbound census INDEX canonical fields mismatch")
        artifact_name = _safe_leaf_name(
            canonical.get("artifact"), field="canonical artifact"
        )
        reproduction_name = _safe_leaf_name(
            canonical.get("reproduction"), field="canonical reproduction"
        )
        artifact_sha256 = canonical.get("sha256")
        if (
            artifact_name != EXPECTED_ARTIFACT_NAME
            or artifact_name in EXPECTED_SUPERSEDED
            or reproduction_name != EXPECTED_REPRODUCTION_NAME
            or not isinstance(artifact_sha256, str)
            or not _HEX_SHA256_RE.fullmatch(artifact_sha256)
            or artifact_sha256 != EXPECTED_ARTIFACT_SHA256
        ):
            raise CensusBindingError("outbound census canonical artifact binding mismatch")

        artifact_raw = _secure_read_at(
            root_fd,
            artifact_name,
            max_bytes=16 * 1024 * 1024,
            context="outbound census canonical artifact",
            after_open_hook=_after_open_hook,
        )
        if _sha256(artifact_raw) != artifact_sha256:
            raise CensusBindingError("outbound census canonical artifact SHA-256 mismatch")
        census = _strict_json_loads(
            artifact_raw, context="outbound census canonical artifact"
        )
        if not isinstance(census, dict) or set(census) != _CENSUS_FIELDS:
            raise CensusBindingError("outbound census fields do not match schema")
        _require_exact_false(
            census,
            (
                "all_runtime_rows_classified",
                "candidate_execution_authorized",
                "production_ready",
                "promotion_authorized",
                "record_only_coverage_complete",
                "runtime_egress_trace_complete",
            ),
            context="census",
        )
        if (
            census.get("schema_version") != EXPECTED_CENSUS_SCHEMA
            or census.get("provisional_target_only") is not True
            or census.get("status") != EXPECTED_STATUS
            or census.get("gate_decision") != EXPECTED_GATE_DECISION
            or census.get("blockers") != list(EXPECTED_BLOCKERS)
            or census.get("source_commit") != EXPECTED_SOURCE_COMMIT
            or census.get("source_tree") != EXPECTED_SOURCE_TREE
            or census.get("source_sha256_manifest_sha256")
            != EXPECTED_SOURCE_SHA256_MANIFEST_SHA256
            or census.get("source_tree_manifest_sha256")
            != EXPECTED_SOURCE_TREE_MANIFEST_SHA256
        ):
            raise CensusBindingError("outbound census provenance or safety fields mismatch")
        for value, pattern, field in (
            (census.get("source_commit"), _HEX_GIT_RE, "source_commit"),
            (census.get("source_tree"), _HEX_GIT_RE, "source_tree"),
            (
                census.get("source_sha256_manifest_sha256"),
                _HEX_SHA256_RE,
                "source_sha256_manifest_sha256",
            ),
            (
                census.get("source_tree_manifest_sha256"),
                _HEX_SHA256_RE,
                "source_tree_manifest_sha256",
            ),
        ):
            if not isinstance(value, str) or not pattern.fullmatch(value):
                raise CensusBindingError(f"invalid {field}")
        _validate_manifest_reference(
            census.get("source_sha256_manifest"),
            expected_name="source-file-sha256.tsv",
            field="source_sha256_manifest",
        )
        _validate_manifest_reference(
            census.get("source_tree_manifest"),
            expected_name="tree-inventory.tsv",
            field="source_tree_manifest",
        )

        counts = census.get("counts")
        if counts != EXPECTED_COUNTS:
            raise CensusBindingError("outbound census scanner counts mismatch")
        scanner = census.get("scanner")
        if not isinstance(scanner, dict) or set(scanner) != {
            "manifest_files",
            "parse_errors",
            "scan_suffixes",
            "scanned_files",
            "skipped_large",
            "unclassified_executables",
        }:
            raise CensusBindingError("outbound census scanner fields mismatch")
        if (
            scanner.get("manifest_files") != 6171
            or scanner.get("scanned_files") != 4279
            or scanner.get("parse_errors") != []
            or scanner.get("skipped_large") != []
            or scanner.get("scan_suffixes") != list(EXPECTED_SCAN_SUFFIXES)
            or scanner.get("unclassified_executables")
            != list(EXPECTED_MODE_ANOMALIES)
        ):
            raise CensusBindingError("outbound census scanner status mismatch")

        rows = census.get("rows")
        if not isinstance(rows, list) or len(rows) != EXPECTED_COUNTS["rows"]:
            raise CensusBindingError("outbound census row count mismatch")
        language_counts: dict[str, int] = {}
        runtime_category_counts: dict[str, int] = {}
        runtime_rows = 0
        test_rows = 0
        pending_rows = 0
        unverified_rows = 0
        for number, row in enumerate(rows, 1):
            if not isinstance(row, dict) or set(row) != _ROW_FIELDS:
                raise CensusBindingError(f"outbound census row {number} fields mismatch")
            if row.get("review_status") == "pending":
                pending_rows += 1
            if row.get("record_only_coverage") == "unverified":
                unverified_rows += 1
            if row.get("review_status") != "pending" or row.get("record_only_coverage") != "unverified":
                raise CensusBindingError(
                    f"outbound census row {number} is not pending/unverified"
                )
            is_test = row.get("is_test")
            language = row.get("language")
            category = row.get("category")
            path_value = row.get("path")
            line = row.get("line")
            confidence = row.get("confidence")
            symbol = row.get("symbol")
            if (
                type(is_test) is not bool
                or language not in EXPECTED_BY_LANGUAGE
                or not isinstance(category, str)
                or not category
                or not isinstance(path_value, str)
                or not path_value
                or Path(path_value).is_absolute()
                or ".." in Path(path_value).parts
                or isinstance(line, bool)
                or not isinstance(line, int)
                or line < 1
                or confidence not in {"high", "medium", "low"}
                or not isinstance(symbol, str)
                or not symbol
            ):
                raise CensusBindingError(f"outbound census row {number} has invalid types")
            language_counts[language] = language_counts.get(language, 0) + 1
            if is_test:
                test_rows += 1
            else:
                runtime_rows += 1
                runtime_category_counts[category] = (
                    runtime_category_counts.get(category, 0) + 1
                )
        if (
            runtime_rows != EXPECTED_COUNTS["runtime_rows"]
            or test_rows != EXPECTED_COUNTS["test_rows"]
            or pending_rows != EXPECTED_COUNTS["rows"]
            or unverified_rows != EXPECTED_COUNTS["rows"]
            or language_counts != EXPECTED_BY_LANGUAGE
            or runtime_category_counts != EXPECTED_RUNTIME_BY_CATEGORY
        ):
            raise CensusBindingError("outbound census rows do not reproduce scanner counts")
        _assert_dir_identity(root, root_fd, root_identity)
        return VerifiedCensusBinding(
            index_name=INDEX_NAME,
            index_sha256=index_sha256,
            artifact_name=artifact_name,
            artifact_sha256=artifact_sha256,
            status=EXPECTED_STATUS,
            gate_decision=EXPECTED_GATE_DECISION,
            source_commit=EXPECTED_SOURCE_COMMIT,
            source_tree=EXPECTED_SOURCE_TREE,
            source_sha256_manifest_sha256=EXPECTED_SOURCE_SHA256_MANIFEST_SHA256,
            source_tree_manifest_sha256=EXPECTED_SOURCE_TREE_MANIFEST_SHA256,
            manifest_files=6171,
            scanned_files=4279,
            total_rows=6338,
            runtime_rows=runtime_rows,
            test_rows=test_rows,
            pending_rows=pending_rows,
            unverified_rows=unverified_rows,
            unclassified_executable_modes=EXPECTED_MODE_ANOMALIES,
            superseded=EXPECTED_SUPERSEDED,
        )
    finally:
        os.close(root_fd)


DEFAULT_EVIDENCE_ROOT = Path(
    os.environ.get(
        "HERMES_OUTBOUND_CENSUS_ROOT",
        str(Path(__file__).resolve().parents[3] / "evidence" / "target-outbound-census"),
    )
)


def _load_authoritative_census_binding():
    from gateway.record_only.external_census_binding import (
        EXTERNAL_CENSUS_BINDING_FD,
        consume_external_census_binding,
    )

    try:
        os.fstat(EXTERNAL_CENSUS_BINDING_FD)
    except OSError as exc:
        if exc.errno != errno.EBADF:
            raise CensusBindingError(
                f"cannot inspect inherited external binding FD: {exc}"
            ) from exc
        return verify_target_outbound_census(DEFAULT_EVIDENCE_ROOT)
    return consume_external_census_binding()


AUTHORITATIVE_CENSUS_BINDING = _load_authoritative_census_binding()
