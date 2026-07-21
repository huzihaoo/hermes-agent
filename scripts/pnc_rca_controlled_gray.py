#!/usr/bin/env python3
"""Build a fail-closed, validation-only plan for one controlled RCA gray run."""

from __future__ import annotations

import argparse
import ast
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import subprocess
import sys
from typing import Any, Callable, Mapping, Sequence


REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gateway import pnc_rca_prod_admission as prod_admission


SPEC_SCHEMA_VERSION = "pnc_rca_controlled_gray_spec_v1"
BOM_SCHEMA_VERSION = "pnc_rca_controlled_gray_bom_v1"
PLAN_SCHEMA_VERSION = "pnc_rca_controlled_gray_plan_v1"
HOST_RUNTIME_MANIFEST_SCHEMA_VERSION = (
    "pnc_rca_controlled_gray_host_runtime_manifest_v1"
)
ADMISSION_CONTRACT_SCHEMA_VERSION = "pnc_rca_controlled_gray_admission_contract_v1"

TARGET_API_PROJECT_KEY = "68ef617fb371dc80a10641f7"
TARGET_PROJECT_SIMPLE_NAME = "t03o4q"
TARGET_WORK_ITEM_TYPE_KEY = "issue"
TARGET_WORK_ITEM_ID = "7051585084"
TARGET_TOPIC = "feishu-project-workflow-event"
TARGET_PARTITION = 0
TARGET_OFFSET = 650
TARGET_ISSUE_URL = (
    "https://project.feishu.cn/t03o4q/issue/detail/7051585084"
)
RESULT_FIELD_KEY = "field_9193cb"
REPORT_FIELD_KEY = "field_8c912e"
TARGET_FIELD_KEYS = (RESULT_FIELD_KEY, REPORT_FIELD_KEY)
REPORT_MANIFEST_SCHEMA_VERSION = "delivery_manifest_v2"
REPORT_ROUTE_PREFIX = "/G1Q3_RCA/cases/"
OFFICIAL_FIELD_READBACK_ADAPTER = "MeegleIssueCommentAdapter.get_fields"
OFFICIAL_COMMENT_READBACK_ADAPTER = "MeegleIssueCommentAdapter.list_comments"
OFFICIAL_COMBINED_READBACK_ADAPTER = (
    "MeegleIssueCommentAdapter.get_fields_and_comments"
)

CANONICAL_RESOURCE_PATH = Path.home() / ".local/bin/ssh-mini-resource"
CANONICAL_SUBMIT_PATH = Path.home() / ".local/bin/ssh-mini-submit"
CANONICAL_CAPACITY_VALIDATOR_PATH = (
    Path.home()
    / ".hermes/workspace-work/bin/context_rca_capacity_authorization.py"
)
CANONICAL_CAPACITY_AUTHORIZATION_PATH = (
    Path.home() / ".ssh-mini/rca-capacity-authorization.json"
)
GOVERNED_EXECUTION_ADAPTER_PATH = (
    REPO_ROOT / "scripts/pnc_rca_prod_execution_adapter.py"
)
GOVERNED_EXECUTION_ADAPTER_COMMANDS = (
    "build-exact-request",
    "validate-exact-receipt",
    "official-readback",
    "build-natural-gate",
    "select-first-natural",
)
RESOURCE_CLASS = "rca_prod"
CAPACITY_MODE = "steady"

EXPECTED_HOST_ROOT = "/Users/songying/.codex/tmp/rca-host-70c432-zero-cache"
EXPECTED_HOST_COMMIT = "92f60f4da5df335b756da6b2e970b7096cc10d45"
EXPECTED_HOST_TREE = "05bdbda2923841095e0f11a3e983a487dcd4593a"
EXPECTED_HOST_GO_RECEIPT_PATH = (
    "/Users/songying/.codex/tmp/rca-prod-e2e-release-20260721/evidence/"
    "controlled-gray/host-independent-go-92f60f4d.json"
)
EXPECTED_HOST_GO_RECEIPT_SHA256 = (
    "82fd1391256e983206ce941d98850718c65e52f6d3bf0afd6a7e90ba88c4bd7b"
)
EXPECTED_VM_ROOT = (
    "/home/mini/.hermes/rca-prod-runtime/releases/"
    "rca-e2e-hotfix-pathsafe-20260721"
)
EXPECTED_VM_COMMIT = "00599fa5cd8718df3c31cd177f606a9e32b2419b"
EXPECTED_VM_TREE = "27cb14f0cef85de51e32dca5da572ca318ebcb91"
EXPECTED_VM_GO_RECEIPT_SHA256 = (
    "6dd776db67ff8a0859e050a433a613c3ff1fe17a547a56326ca523b9cdfb405a"
)
EXPECTED_VM_GO_AUTHORITATIVE_PATH = (
    "/home/mini/.hermes/rca-prod-runtime/audits/"
    "00599fa5cd8718df3c31cd177f606a9e32b2419b/independent-go-receipt.json"
)
EXPECTED_VM_GO_REPLICA_PATH = (
    "/mnt/tmp/g1q3-rca-00599fa-independent-audit-20260722/"
    "receipt-go-00599fa5.json"
)
EXPECTED_VM_GO_CIFS_PATH = (
    "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/"
    "g1q3-rca-00599fa-independent-audit-20260722/receipt-go-00599fa5.json"
)
EXPECTED_VM_CLOSURE_SHA256 = (
    "2a10e84f97ce20f0b1dd46586c7d12659f56626ad55c27e29518f64afd593499"
)
EXPECTED_VM_CLOSURE_CORE_SHA256 = (
    "85d2211188a98a637e266fcb3623efdc0c5837cdd6b2e96ef3ea006217d37b08"
)

MAX_JSON_BYTES = 2 * 1024 * 1024
MAX_SOURCE_BYTES = 8 * 1024 * 1024
MAX_GIT_OUTPUT_BYTES = 32 * 1024 * 1024
MAX_RESOURCE_OUTPUT_BYTES = prod_admission.MAX_RESOURCE_OUTPUT_BYTES
MAX_CAPACITY_SNAPSHOT_AGE = timedelta(seconds=120)
MAX_HOST_FILESYSTEM_ENTRIES = 200_000
HEX40_RE = re.compile(r"[0-9a-f]{40}\Z")
HEX64_RE = re.compile(r"[0-9a-f]{64}\Z")
IDENTITY_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")

SPEC_FIELDS = {"schema_version", "release_id", "host_candidate", "vm_candidate"}
HOST_SPEC_FIELDS = {"root", "commit", "tree"}
VM_SPEC_FIELDS = {
    "root",
    "commit",
    "tree",
    "independent_go_receipt_path",
    "independent_go_receipt_sha256",
}
VM_RECEIPT_FIELDS = {
    "candidate",
    "candidate_lineage_after_inherited_audit",
    "changed_files_from_inherited_audit_candidate",
    "current_verification",
    "deployment_authorization",
    "inherited_independent_audit",
    "nonblocking_notes",
    "observed_at",
    "open_blockers",
    "production_actions",
    "production_mutation",
    "public_report_contract",
    "release_recommendation",
    "schema_version",
    "scope",
    "source_files",
    "source_remote_readback",
    "verdict",
}
VM_CANDIDATE_FIELDS = {
    "branch",
    "cache_paths",
    "candidate_edited_by_auditor",
    "commit",
    "git_clean",
    "git_status",
    "parent",
    "repo",
    "symlinks",
    "tree",
}
HOST_GO_RECEIPT_FIELDS = {
    "candidate",
    "deployment_authorization",
    "live_evidence",
    "observed_at",
    "open_blockers",
    "production_actions",
    "production_mutation",
    "receipt_storage",
    "release_recommendation",
    "schema_version",
    "scope",
    "verdict",
    "verification",
}
HOST_GO_EXPECTED_BLOCKERS = (
    "regular_rca_prod_capacity_authorization_absent",
    "canonical_capacity_policy_requires_at_least_20_zero_materialized_successful_samples_over_at_least_7_days",
    "canonical_public_dns_tls_same_origin_route_absent",
    "exact_7051585084_has_no_trigger_outbox_watch_delivery_effect_or_official_postback",
    "first_natural_kafka_controlled_gray_not_completed",
    "host_92f60f4d_candidate_not_deployed_to_live_runtime",
)
VM_GO_EXPECTED_BLOCKERS = (
    "regular rca_prod capacity authorization absent",
    "canonical public DNS/TLS route not installed",
    "maintainer deployment approval absent",
)
RUNTIME_SCOPE_NAMES = (
    "RCA_RUNTIME_RELATIVE_FILES",
    "GATEWAY_RCA_RUNTIME_RELATIVE_FILES",
)
REQUIRED_HOST_RUNTIME_FILES = frozenset(
    {
        "gateway/pnc_rca_delivery_contract.py",
        "gateway/pnc_rca_prod_admission.py",
        "gateway/pnc_rca_runtime_identity.py",
        "scripts/pnc_rca_delivery_collector.py",
        "scripts/pnc_rca_delivery_dispatcher.py",
        "scripts/pnc_rca_kafka_consumer.py",
        "scripts/pnc_rca_outbox_dispatcher.py",
    }
)

ResourceProbe = Callable[[], Mapping[str, Any]]


class ControlledGrayError(ValueError):
    """Stable failure raised before any production-capable action is available."""

    def __init__(self, code: str, detail: str = ""):
        self.code = str(code or "controlled_gray_invalid")[:160]
        self.detail = str(detail or self.code)[:500]
        super().__init__(self.code)


def _canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise ControlledGrayError("controlled_gray_not_canonical") from exc


def _json_document(value: Any) -> bytes:
    return _canonical_bytes(value) + b"\n"


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_value(value: Any) -> str:
    return _sha256_bytes(_canonical_bytes(value))


def _hex40(value: Any, *, field: str) -> str:
    normalized = str(value or "").strip().lower()
    if HEX40_RE.fullmatch(normalized) is None:
        raise ControlledGrayError(f"controlled_gray_{field}_invalid")
    return normalized


def _hex64(value: Any, *, field: str) -> str:
    normalized = str(value or "").strip().lower()
    if HEX64_RE.fullmatch(normalized) is None:
        raise ControlledGrayError(f"controlled_gray_{field}_invalid")
    return normalized


def _timestamp(value: Any, *, field: str) -> datetime:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ControlledGrayError(f"controlled_gray_{field}_invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ControlledGrayError(f"controlled_gray_{field}_invalid")
    return parsed.astimezone(timezone.utc)


def _now(value: datetime | None = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None or current.utcoffset() is None:
        raise ControlledGrayError("controlled_gray_validation_time_invalid")
    return current.astimezone(timezone.utc)


def _absolute_path(value: Any, *, field: str) -> Path:
    text = str(value or "").strip()
    selected = Path(text).expanduser()
    if (
        not text
        or not selected.is_absolute()
        or "\x00" in text
        or ".." in selected.parts
    ):
        raise ControlledGrayError(f"controlled_gray_{field}_invalid")
    return selected.absolute()


def _stat_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_gid,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _read_stable_file(
    path: Path,
    *,
    artifact: str,
    maximum: int,
    expected_sha256: str | None = None,
) -> tuple[bytes, str]:
    selected = _absolute_path(path, field=f"{artifact}_path")
    if not hasattr(os, "O_NOFOLLOW"):
        raise ControlledGrayError(f"controlled_gray_{artifact}_no_follow_unavailable")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW
    descriptor = -1
    try:
        before_path = os.lstat(selected)
        descriptor = os.open(selected, flags)
        before = os.fstat(descriptor)
        if (
            stat.S_ISLNK(before_path.st_mode)
            or not stat.S_ISREG(before.st_mode)
            or _stat_identity(before_path) != _stat_identity(before)
            or before.st_size < 1
            or before.st_size > maximum
        ):
            raise ControlledGrayError(f"controlled_gray_{artifact}_identity_invalid")
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise ControlledGrayError(f"controlled_gray_{artifact}_unstable")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise ControlledGrayError(f"controlled_gray_{artifact}_unstable")
        after = os.fstat(descriptor)
        after_path = os.lstat(selected)
        if (
            _stat_identity(before) != _stat_identity(after)
            or _stat_identity(before) != _stat_identity(after_path)
        ):
            raise ControlledGrayError(f"controlled_gray_{artifact}_unstable")
    except ControlledGrayError:
        raise
    except OSError as exc:
        raise ControlledGrayError(f"controlled_gray_{artifact}_unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    raw = b"".join(chunks)
    digest = _sha256_bytes(raw)
    if expected_sha256 is not None and digest != _hex64(
        expected_sha256, field=f"{artifact}_sha256"
    ):
        raise ControlledGrayError(f"controlled_gray_{artifact}_sha256_mismatch")
    return raw, digest


def _strict_json(raw: bytes, *, artifact: str) -> Mapping[str, Any]:
    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise ControlledGrayError(
                    f"controlled_gray_{artifact}_duplicate_key"
                )
            value[key] = item
        return value

    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=unique,
            parse_constant=lambda item: (_ for _ in ()).throw(
                ControlledGrayError(
                    f"controlled_gray_{artifact}_number_invalid", str(item)
                )
            ),
        )
    except ControlledGrayError:
        raise
    except (UnicodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ControlledGrayError(f"controlled_gray_{artifact}_json_invalid") from exc
    if not isinstance(value, Mapping):
        raise ControlledGrayError(f"controlled_gray_{artifact}_shape_invalid")
    return value


def _normalize_spec(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != SPEC_FIELDS:
        raise ControlledGrayError("controlled_gray_spec_shape_invalid")
    release_id = str(value.get("release_id") or "").strip()
    host = value.get("host_candidate")
    vm = value.get("vm_candidate")
    if IDENTITY_RE.fullmatch(release_id) is None:
        raise ControlledGrayError("controlled_gray_release_id_invalid")
    if not isinstance(host, Mapping) or set(host) != HOST_SPEC_FIELDS:
        raise ControlledGrayError("controlled_gray_host_spec_invalid")
    if not isinstance(vm, Mapping) or set(vm) != VM_SPEC_FIELDS:
        raise ControlledGrayError("controlled_gray_vm_spec_invalid")
    if value.get("schema_version") != SPEC_SCHEMA_VERSION:
        raise ControlledGrayError("controlled_gray_spec_schema_invalid")
    normalized = {
        "schema_version": SPEC_SCHEMA_VERSION,
        "release_id": release_id,
        "host_candidate": {
            "root": str(_absolute_path(host.get("root"), field="host_root")),
            "commit": _hex40(host.get("commit"), field="host_commit"),
            "tree": _hex40(host.get("tree"), field="host_tree"),
        },
        "vm_candidate": {
            "root": str(_absolute_path(vm.get("root"), field="vm_root")),
            "commit": _hex40(vm.get("commit"), field="vm_commit"),
            "tree": _hex40(vm.get("tree"), field="vm_tree"),
            "independent_go_receipt_path": str(
                _absolute_path(
                    vm.get("independent_go_receipt_path"),
                    field="vm_go_receipt_path",
                )
            ),
            "independent_go_receipt_sha256": _hex64(
                vm.get("independent_go_receipt_sha256"),
                field="vm_go_receipt_sha256",
            ),
        },
    }
    expected = {
        "host_candidate": {
            "root": EXPECTED_HOST_ROOT,
            "commit": EXPECTED_HOST_COMMIT,
            "tree": EXPECTED_HOST_TREE,
        },
        "vm_candidate": {
            "root": EXPECTED_VM_ROOT,
            "commit": EXPECTED_VM_COMMIT,
            "tree": EXPECTED_VM_TREE,
            "independent_go_receipt_sha256": EXPECTED_VM_GO_RECEIPT_SHA256,
        },
    }
    for component, fields in expected.items():
        if any(
            normalized[component].get(field) != expected_value
            for field, expected_value in fields.items()
        ):
            raise ControlledGrayError(
                f"controlled_gray_{component}_production_binding_mismatch"
            )
    return normalized


def _git(root: Path, *arguments: str, maximum: int = MAX_GIT_OUTPUT_BYTES) -> bytes:
    env = {
        **os.environ,
        "GIT_OPTIONAL_LOCKS": "0",
        "LC_ALL": "C",
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    try:
        completed = subprocess.run(
            ["git", "-C", str(root), *arguments],
            check=False,
            capture_output=True,
            timeout=30,
            env=env,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ControlledGrayError("controlled_gray_host_git_unavailable") from exc
    if (
        completed.returncode != 0
        or len(completed.stdout) > maximum
        or len(completed.stderr) > MAX_JSON_BYTES
    ):
        raise ControlledGrayError("controlled_gray_host_git_failed")
    return completed.stdout


def _safe_relative(value: Any, *, field: str) -> str:
    text = str(value or "")
    selected = PurePosixPath(text)
    if (
        not text
        or selected.is_absolute()
        or any(part in {"", ".", ".."} for part in selected.parts)
        or "\x00" in text
    ):
        raise ControlledGrayError(f"controlled_gray_{field}_invalid")
    return text


def _runtime_scopes(source: bytes) -> dict[str, list[str]]:
    try:
        tree = ast.parse(source.decode("utf-8"))
    except (UnicodeError, SyntaxError) as exc:
        raise ControlledGrayError(
            "controlled_gray_host_runtime_identity_ast_invalid"
        ) from exc
    discovered: dict[str, list[str]] = {}
    for node in tree.body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        names = [target.id for target in targets if isinstance(target, ast.Name)]
        selected = set(names) & set(RUNTIME_SCOPE_NAMES)
        if not selected or node.value is None:
            continue
        try:
            raw = ast.literal_eval(node.value)
        except (ValueError, TypeError, SyntaxError) as exc:
            raise ControlledGrayError(
                "controlled_gray_host_runtime_allowlist_not_literal"
            ) from exc
        if not isinstance(raw, (tuple, list)):
            raise ControlledGrayError(
                "controlled_gray_host_runtime_allowlist_invalid"
            )
        normalized = [
            _safe_relative(item, field="host_runtime_relative_path") for item in raw
        ]
        if not normalized or len(normalized) != len(set(normalized)):
            raise ControlledGrayError(
                "controlled_gray_host_runtime_allowlist_invalid"
            )
        for name in selected:
            discovered[name] = normalized
    if set(discovered) != set(RUNTIME_SCOPE_NAMES):
        raise ControlledGrayError("controlled_gray_host_runtime_allowlist_missing")
    return discovered


def _host_file_descriptor(
    *, root: Path, commit: str, relative: str
) -> dict[str, Any]:
    selected = root.joinpath(*PurePosixPath(relative).parts)
    raw, digest = _read_stable_file(
        selected, artifact="host_runtime_file", maximum=MAX_SOURCE_BYTES
    )
    committed = _git(root, "show", f"{commit}:{relative}", maximum=MAX_SOURCE_BYTES)
    if committed != raw:
        raise ControlledGrayError("controlled_gray_host_runtime_worktree_drift")
    line = _git(root, "ls-tree", commit, "--", relative).decode(
        "utf-8", errors="strict"
    )
    match = re.fullmatch(
        r"(100644|100755) blob ([0-9a-f]{40})\t([^\n]+)\n?", line
    )
    if match is None or match.group(3) != relative:
        raise ControlledGrayError("controlled_gray_host_runtime_git_entry_invalid")
    return {
        "path": relative,
        "sha256": digest,
        "size_bytes": len(raw),
        "git_mode": match.group(1),
        "git_blob_oid": match.group(2),
    }


def _validate_host_hygiene(root: Path) -> dict[str, int]:
    entries = 0
    for current, directories, files in os.walk(root, followlinks=False):
        current_path = Path(current)
        if current_path == root and ".git" in directories:
            directories.remove(".git")
        for name in [*directories, *files]:
            entries += 1
            if entries > MAX_HOST_FILESYSTEM_ENTRIES:
                raise ControlledGrayError("controlled_gray_host_tree_too_large")
            selected = current_path / name
            try:
                info = os.lstat(selected)
            except OSError as exc:
                raise ControlledGrayError("controlled_gray_host_hygiene_unavailable") from exc
            if stat.S_ISLNK(info.st_mode):
                raise ControlledGrayError("controlled_gray_host_symlink_forbidden")
            if (
                name
                in {"__pycache__", ".pytest_cache", ".ruff_cache", ".mypy_cache"}
                or (stat.S_ISREG(info.st_mode) and selected.suffix in {".pyc", ".pyo"})
            ):
                raise ControlledGrayError("controlled_gray_host_cache_forbidden")
    return {"entries_scanned": entries, "cache_artifacts": 0, "symlinks": 0}


def _source_tree(raw: bytes, *, artifact: str) -> ast.Module:
    try:
        return ast.parse(raw.decode("utf-8"))
    except (UnicodeError, SyntaxError) as exc:
        raise ControlledGrayError(
            f"controlled_gray_{artifact}_ast_invalid"
        ) from exc


def _definition(
    tree: ast.Module,
    name: str,
    *,
    class_name: str | None = None,
) -> ast.FunctionDef | ast.AsyncFunctionDef:
    body: Sequence[ast.stmt] = tree.body
    if class_name is not None:
        selected_class = next(
            (
                node
                for node in tree.body
                if isinstance(node, ast.ClassDef) and node.name == class_name
            ),
            None,
        )
        if selected_class is None:
            raise ControlledGrayError(
                "controlled_gray_host_delivery_capability_invalid"
            )
        body = selected_class.body
    selected = next(
        (
            node
            for node in body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == name
        ),
        None,
    )
    if selected is None:
        raise ControlledGrayError("controlled_gray_host_delivery_capability_invalid")
    return selected


def _literal_assignment(tree: ast.Module, name: str) -> Any:
    for node in tree.body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        if any(isinstance(target, ast.Name) and target.id == name for target in targets):
            try:
                return ast.literal_eval(node.value)
            except (ValueError, TypeError, SyntaxError) as exc:
                raise ControlledGrayError(
                    "controlled_gray_host_delivery_capability_invalid"
                ) from exc
    raise ControlledGrayError("controlled_gray_host_delivery_capability_invalid")


def _assignment_node(tree: ast.Module, name: str) -> ast.Assign | ast.AnnAssign:
    for node in tree.body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        if any(isinstance(target, ast.Name) and target.id == name for target in targets):
            return node
    raise ControlledGrayError("controlled_gray_host_delivery_capability_invalid")


def _node_names(node: ast.AST) -> set[str]:
    return {item.id for item in ast.walk(node) if isinstance(item, ast.Name)}


def _node_strings(node: ast.AST) -> set[str]:
    return {
        item.value
        for item in ast.walk(node)
        if isinstance(item, ast.Constant) and isinstance(item.value, str)
    }


def _node_attribute_names(node: ast.AST) -> set[str]:
    return {
        item.attr for item in ast.walk(node) if isinstance(item, ast.Attribute)
    }


def _call_name(node: ast.Call) -> str:
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return ""


def _call_lines(node: ast.AST, name: str) -> list[int]:
    return sorted(
        int(item.lineno)
        for item in ast.walk(node)
        if isinstance(item, ast.Call) and _call_name(item) == name
    )


def _function_arguments(node: ast.FunctionDef | ast.AsyncFunctionDef) -> set[str]:
    return {
        argument.arg
        for argument in [
            *node.args.posonlyargs,
            *node.args.args,
            *node.args.kwonlyargs,
        ]
    }


def _validate_host_delivery_capabilities(
    *, contract_raw: bytes, dispatcher_raw: bytes
) -> dict[str, Any]:
    contract_tree = _source_tree(contract_raw, artifact="host_delivery_contract")
    dispatcher_tree = _source_tree(
        dispatcher_raw, artifact="host_delivery_dispatcher"
    )
    constants = {
        name: _literal_assignment(contract_tree, name)
        for name in (
            "DELIVERY_MANIFEST_SCHEMA_VERSION",
            "DELIVERY_EFFECT_SCHEMA_VERSION",
            "DELIVERY_REPORT_LINK_KIND",
            "RCA_RESULT_FIELD_KEY",
            "RCA_REPORT_FIELD_KEY",
        )
    }
    if constants != {
        "DELIVERY_MANIFEST_SCHEMA_VERSION": REPORT_MANIFEST_SCHEMA_VERSION,
        "DELIVERY_EFFECT_SCHEMA_VERSION": "pnc_rca_delivery_effect_v2",
        "DELIVERY_REPORT_LINK_KIND": "manifest_html",
        "RCA_RESULT_FIELD_KEY": RESULT_FIELD_KEY,
        "RCA_REPORT_FIELD_KEY": REPORT_FIELD_KEY,
    }:
        raise ControlledGrayError("controlled_gray_host_delivery_capability_invalid")

    issue_builder = _definition(contract_tree, "build_issue_comment_content")
    thread_builder = _definition(contract_tree, "build_thread_reply_content")
    verify_bundle = _definition(contract_tree, "verify_delivery_bundle")
    if not {
        "marker",
        "work_item_id",
        "report_status",
        "conclusion",
        "report_url",
        "report_cifs_path",
    }.issubset(_function_arguments(issue_builder)) or not {
        "marker",
        "work_item_id",
        "report_status",
        "conclusion",
        "report_url",
        "issue_url",
    }.issubset(_function_arguments(thread_builder)):
        raise ControlledGrayError("controlled_gray_host_delivery_capability_invalid")
    verify_calls = {
        name: len(_call_lines(verify_bundle, name))
        for name in (
            "build_issue_comment_content",
            "compute_delivery_effect_payload_sha256",
            "compute_delivery_effect_key",
            "delivery_effect_marker",
        )
    }
    if any(count < 1 for count in verify_calls.values()):
        raise ControlledGrayError("controlled_gray_host_delivery_capability_invalid")
    base_semantic_fields = _assignment_node(
        contract_tree, "_BASE_EFFECT_SEMANTIC_FIELDS"
    )
    if "project_simple_name" not in _node_strings(base_semantic_fields):
        raise ControlledGrayError("controlled_gray_host_delivery_capability_invalid")

    for method in ("get_fields", "list_comments", "get_fields_and_comments"):
        _definition(
            dispatcher_tree,
            method,
            class_name="MeegleIssueCommentAdapter",
        )
    validate_effect = _definition(dispatcher_tree, "_validate_effect")
    validate_names = _node_names(validate_effect)
    validate_strings = _node_strings(validate_effect)
    if (
        "DELIVERY_EFFECT_SCHEMA_VERSION" not in validate_names
        or "DELIVERY_EFFECT_SCHEMA_VERSION_V1" in validate_names
        or "DELIVERY_REPORT_LINK_KIND" not in validate_names
        or "_PROJECT_SIMPLE_NAME_RE" not in validate_names
        or "project_simple_name" not in validate_strings
        or "delivery_effect_schema_unsupported" not in validate_strings
        or "delivery_effect_content_invalid" not in validate_strings
        or len(_call_lines(validate_effect, "build_issue_comment_content")) < 1
        or len(_call_lines(validate_effect, "build_thread_reply_content")) < 1
        or len(_call_lines(validate_effect, "verify_persisted_artifact_inventory"))
        < 1
    ):
        raise ControlledGrayError("controlled_gray_host_delivery_capability_invalid")

    for method in (
        "_list_remote_effect",
        "_read_field_updates",
        "_write_field_updates",
        "_add_remote_effect",
    ):
        boundary = _definition(
            dispatcher_tree, method, class_name="DeliveryDispatcher"
        )
        attributes = _node_attribute_names(boundary)
        if "project_key" not in attributes or "project_simple_name" in attributes:
            raise ControlledGrayError(
                "controlled_gray_host_delivery_capability_invalid"
            )

    confirmed = _definition(dispatcher_tree, "_confirmed_content_matches")
    if (
        "expected_content" not in _node_names(confirmed)
        or len(_call_lines(confirmed, "_marker_matches")) < 1
        or len(_call_lines(confirmed, "_canonical_remote_content")) < 1
        or not any(isinstance(item, ast.Eq) for item in ast.walk(confirmed))
    ):
        raise ControlledGrayError("controlled_gray_host_delivery_capability_invalid")

    report_verifier = _definition(dispatcher_tree, "default_report_verifier")
    report_method = _definition(
        dispatcher_tree,
        "_verify_report_artifacts",
        class_name="DeliveryDispatcher",
    )
    report_strings = _node_strings(report_method) | _node_strings(report_verifier)
    if (
        not {"status_code", "content_length", "sha256"}.issubset(report_strings)
        or "report_http_verification_mismatch" not in report_strings
        or len(_call_lines(report_method, "report_verifier")) < 1
    ):
        raise ControlledGrayError("controlled_gray_host_delivery_capability_invalid")

    complete = _definition(
        dispatcher_tree,
        "_complete_from_marker",
        class_name="DeliveryDispatcher",
    )
    dispatch = _definition(
        dispatcher_tree,
        "_dispatch_claim",
        class_name="DeliveryDispatcher",
    )
    receipt_fields = {"confirmed_content_sha256", "confirmed_report_url"}
    if not receipt_fields.issubset(
        _node_strings(complete)
    ) or not receipt_fields.issubset(_node_strings(dispatch)):
        raise ControlledGrayError("controlled_gray_host_delivery_capability_invalid")
    verification_lines = _call_lines(dispatch, "_verify_report_artifacts")
    remote_boundary_names = (
        "_list_remote_effect",
        "_write_field_updates",
        "_add_remote_effect",
        "_complete_from_marker",
        "complete_effect",
    )
    remote_lines = {
        name: _call_lines(dispatch, name) for name in remote_boundary_names
    }
    confirmed_lines = _call_lines(dispatch, "_confirmed_content_matches")
    if (
        len(verification_lines) < 1
        or any(not lines for lines in remote_lines.values())
        or verification_lines[0]
        >= min(line for lines in remote_lines.values() for line in lines)
        or len(confirmed_lines) < 3
    ):
        raise ControlledGrayError("controlled_gray_host_delivery_capability_invalid")
    return {
        "delivery_manifest_schema_version": constants[
            "DELIVERY_MANIFEST_SCHEMA_VERSION"
        ],
        "delivery_effect_schema_version": constants[
            "DELIVERY_EFFECT_SCHEMA_VERSION"
        ],
        "report_link_kind": constants["DELIVERY_REPORT_LINK_KIND"],
        "field_keys": [
            constants["RCA_RESULT_FIELD_KEY"],
            constants["RCA_REPORT_FIELD_KEY"],
        ],
        "legacy_v1_success_effect_rejected": True,
        "canonical_content_reconstruction": True,
        "api_project_key_and_url_slug_separated": True,
        "official_field_adapter": OFFICIAL_FIELD_READBACK_ADAPTER,
        "official_comment_adapter": OFFICIAL_COMMENT_READBACK_ADAPTER,
        "official_combined_adapter": OFFICIAL_COMBINED_READBACK_ADAPTER,
        "full_content_match_call_count": len(confirmed_lines),
        "http_artifact_verification_precedes_remote_boundary": True,
        "http_artifact_verification_call_count": len(verification_lines),
        "receipt_fields": sorted(receipt_fields),
        "contract_verify_calls": verify_calls,
    }


def _validate_host_go_receipt(
    host: Mapping[str, Any], *, now: datetime
) -> dict[str, Any]:
    path = _absolute_path(
        EXPECTED_HOST_GO_RECEIPT_PATH, field="host_go_receipt_path"
    )
    try:
        info = os.lstat(path)
    except OSError as exc:
        raise ControlledGrayError("controlled_gray_host_go_receipt_unavailable") from exc
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.geteuid()
        or info.st_nlink != 1
        or stat.S_IMODE(info.st_mode) != 0o600
    ):
        raise ControlledGrayError("controlled_gray_host_go_receipt_identity_invalid")
    raw, digest = _read_stable_file(
        path,
        artifact="host_go_receipt",
        maximum=MAX_JSON_BYTES,
        expected_sha256=EXPECTED_HOST_GO_RECEIPT_SHA256,
    )
    body = _strict_json(raw, artifact="host_go_receipt")
    candidate = body.get("candidate")
    storage = body.get("receipt_storage")
    verification = body.get("verification")
    focused = (
        verification.get("focused_suite")
        if isinstance(verification, Mapping)
        else None
    )
    code_checks = (
        verification.get("code_checks")
        if isinstance(verification, Mapping)
        else None
    )
    publication = (
        verification.get("publication_origin_suite")
        if isinstance(verification, Mapping)
        else None
    )
    hygiene = (
        verification.get("worktree_hygiene")
        if isinstance(verification, Mapping)
        else None
    )
    shaped = (
        verification.get("production_shape_probe")
        if isinstance(verification, Mapping)
        else None
    )
    blockers = (
        verification.get("blocker_reproductions")
        if isinstance(verification, Mapping)
        else None
    )
    live = body.get("live_evidence")
    capacity = live.get("capacity") if isinstance(live, Mapping) else None
    target = live.get("exact_target") if isinstance(live, Mapping) else None
    expected_root = str(_absolute_path(host.get("root"), field="host_root"))
    expected_commit = _hex40(host.get("commit"), field="host_commit")
    expected_tree = _hex40(host.get("tree"), field="host_tree")
    if (
        set(body) != HOST_GO_RECEIPT_FIELDS
        or body.get("schema_version")
        != "pnc_rca_host_controlled_gray_independent_audit_v2"
        or body.get("scope") != "controlled-gray BOM binding only"
        or body.get("verdict") != "GO"
        or body.get("release_recommendation")
        != "eligible_for_controlled_gray_bom_binding_only"
        or body.get("deployment_authorization") is not False
        or body.get("production_mutation") is not False
        or body.get("production_actions") != []
        or body.get("open_blockers") != list(HOST_GO_EXPECTED_BLOCKERS)
        or not isinstance(candidate, Mapping)
        or candidate.get("repo") != expected_root
        or candidate.get("commit") != expected_commit
        or candidate.get("tree") != expected_tree
        or candidate.get("git_clean") is not True
        or candidate.get("git_status") != ""
        or candidate.get("cache_dirs") != []
        or candidate.get("pyc_files") != []
        or not isinstance(storage, Mapping)
        or storage.get("authoritative_owner_only_path") != str(path)
        or storage.get("required_mode") != "0600"
        or storage.get("create_once") is not True
        or storage.get("integrity_algorithm") != "sha256"
        or not isinstance(focused, Mapping)
        or focused.get("result") != "PASS"
        or int(focused.get("passed") or 0) < 173
        or not isinstance(publication, Mapping)
        or publication.get("result") != "PASS"
        or int(publication.get("passed") or 0) < 10
        or publication.get("canonical_https_dns_only") is not True
        or publication.get("explicit_port_rejected") is not True
        or publication.get("ip_literal_rejected") is not True
        or not isinstance(code_checks, Mapping)
        or any(code_checks.get(key) != "PASS" for key in ("ruff", "diff_check"))
        or not isinstance(hygiene, Mapping)
        or hygiene.get("git_clean") is not True
        or any(hygiene.get(key) != 0 for key in ("cache_dirs", "pyc_files"))
        or not isinstance(shaped, Mapping)
        or shaped.get("internal_project_key_bound_to_target") is not True
        or shaped.get("project_simple_name") != TARGET_PROJECT_SIMPLE_NAME
        or shaped.get("browser_issue_url") != TARGET_ISSUE_URL
        or shaped.get("semantic_payload_sha256_valid") is not True
        or not isinstance(blockers, Mapping)
        or any(
            not isinstance(item, Mapping) or item.get("result") != "PASS"
            for item in blockers.values()
        )
        or not isinstance(capacity, Mapping)
        or capacity.get("regular_capacity_authorization_present") is not False
        or not isinstance(target, Mapping)
        or target.get("work_item_id") != TARGET_WORK_ITEM_ID
        or target.get("issue_url") != TARGET_ISSUE_URL
        or target.get("result_field_nonempty") is not False
        or target.get("report_field_nonempty") is not False
        or target.get("rca_marker_comment_count") != 0
    ):
        raise ControlledGrayError("controlled_gray_host_go_receipt_invalid")
    observed_at = _timestamp(body.get("observed_at"), field="host_go_observed_at")
    if observed_at > now + timedelta(minutes=5):
        raise ControlledGrayError("controlled_gray_host_go_receipt_future_dated")
    return {
        "observed_path": str(path),
        "sha256": digest,
        "schema_version": body["schema_version"],
        "verdict": "GO",
        "scope": body["scope"],
        "deployment_authorization": False,
    }


def _observe_host_candidate(
    host: Mapping[str, Any], *, now: datetime
) -> dict[str, Any]:
    root = _absolute_path(host.get("root"), field="host_root")
    try:
        root_stat = os.lstat(root)
    except OSError as exc:
        raise ControlledGrayError("controlled_gray_host_root_unavailable") from exc
    if (
        stat.S_ISLNK(root_stat.st_mode)
        or not stat.S_ISDIR(root_stat.st_mode)
        or root_stat.st_uid != os.geteuid()
    ):
        raise ControlledGrayError("controlled_gray_host_root_identity_invalid")
    expected_commit = _hex40(host.get("commit"), field="host_commit")
    expected_tree = _hex40(host.get("tree"), field="host_tree")
    commit = _git(root, "rev-parse", "HEAD^{commit}").decode().strip()
    tree = _git(root, "rev-parse", "HEAD^{tree}").decode().strip()
    status = _git(root, "status", "--porcelain=v1", "--untracked-files=all")
    if commit != expected_commit or tree != expected_tree:
        raise ControlledGrayError("controlled_gray_host_commit_tree_mismatch")
    if status:
        raise ControlledGrayError("controlled_gray_host_worktree_dirty")
    hygiene = _validate_host_hygiene(root)
    identity_relative = "gateway/pnc_rca_runtime_identity.py"
    identity_source = _git(
        root, "show", f"{commit}:{identity_relative}", maximum=MAX_SOURCE_BYTES
    )
    scopes = _runtime_scopes(identity_source)
    union = sorted(set(scopes[RUNTIME_SCOPE_NAMES[0]]) | set(scopes[RUNTIME_SCOPE_NAMES[1]]))
    if not REQUIRED_HOST_RUNTIME_FILES.issubset(union):
        raise ControlledGrayError("controlled_gray_host_required_runtime_files_missing")
    files = {
        relative: _host_file_descriptor(
            root=root, commit=expected_commit, relative=relative
        )
        for relative in union
    }
    capabilities = _validate_host_delivery_capabilities(
        contract_raw=_git(
            root,
            "show",
            f"{expected_commit}:gateway/pnc_rca_delivery_contract.py",
            maximum=MAX_SOURCE_BYTES,
        ),
        dispatcher_raw=_git(
            root,
            "show",
            f"{expected_commit}:scripts/pnc_rca_delivery_dispatcher.py",
            maximum=MAX_SOURCE_BYTES,
        ),
    )
    manifest: dict[str, Any] = {
        "schema_version": HOST_RUNTIME_MANIFEST_SCHEMA_VERSION,
        "candidate_root": str(root),
        "candidate_commit": expected_commit,
        "candidate_tree": expected_tree,
        "runtime_scopes": {
            "rca": scopes[RUNTIME_SCOPE_NAMES[0]],
            "gateway": scopes[RUNTIME_SCOPE_NAMES[1]],
            "union": union,
        },
        "files": files,
    }
    manifest["closure_sha256"] = _sha256_value(manifest)
    independent_go_receipt = _validate_host_go_receipt(host, now=now)
    return {
        "root": str(root),
        "commit": expected_commit,
        "tree": expected_tree,
        "git_status_sha256": _sha256_bytes(status),
        "hygiene": hygiene,
        "delivery_capabilities": capabilities,
        "independent_go_receipt": independent_go_receipt,
        "runtime_manifest": manifest,
        "runtime_manifest_sha256": _sha256_value(manifest),
    }


def _validate_vm_go_receipt(
    vm: Mapping[str, Any], *, now: datetime
) -> dict[str, Any]:
    path = _absolute_path(
        vm.get("independent_go_receipt_path"), field="vm_go_receipt_path"
    )
    expected_sha = _hex64(
        vm.get("independent_go_receipt_sha256"), field="vm_go_receipt_sha256"
    )
    try:
        receipt_info = os.lstat(path)
    except OSError as exc:
        raise ControlledGrayError("controlled_gray_vm_go_receipt_unavailable") from exc
    if (
        stat.S_ISLNK(receipt_info.st_mode)
        or not stat.S_ISREG(receipt_info.st_mode)
        or receipt_info.st_uid != os.geteuid()
        or receipt_info.st_nlink != 1
        or stat.S_IMODE(receipt_info.st_mode) != 0o600
    ):
        raise ControlledGrayError("controlled_gray_vm_go_receipt_identity_invalid")
    raw, receipt_sha = _read_stable_file(
        path,
        artifact="vm_go_receipt",
        maximum=MAX_JSON_BYTES,
        expected_sha256=expected_sha,
    )
    body = _strict_json(raw, artifact="vm_go_receipt")
    candidate = body.get("candidate")
    verification = body.get("current_verification")
    focused = (
        verification.get("expanded_five_file_suite")
        if isinstance(verification, Mapping)
        else None
    )
    owner_probe = (
        verification.get("owner_only_environment_probe")
        if isinstance(verification, Mapping)
        else None
    )
    closure = (
        verification.get("fixed_cli_closure")
        if isinstance(verification, Mapping)
        else None
    )
    expected_commit = _hex40(vm.get("commit"), field="vm_commit")
    expected_tree = _hex40(vm.get("tree"), field="vm_tree")
    expected_root = str(_absolute_path(vm.get("root"), field="vm_root"))
    lineage = body.get("candidate_lineage_after_inherited_audit")
    inherited = body.get("inherited_independent_audit")
    remote = body.get("source_remote_readback")
    report = body.get("public_report_contract")
    if (
        set(body) != VM_RECEIPT_FIELDS
        or body.get("schema_version")
        != "g1q3_rca_vm_candidate_independent_audit_v3"
        or body.get("scope")
        != "offline VM release candidate; controlled release tooling eligibility only"
        or body.get("verdict") != "GO"
        or body.get("release_recommendation")
        != "eligible_for_controlled_release_tooling"
        or body.get("deployment_authorization") is not False
        or body.get("production_mutation") is not False
        or body.get("open_blockers") != list(VM_GO_EXPECTED_BLOCKERS)
        or body.get("production_actions") != []
        or not isinstance(candidate, Mapping)
        or set(candidate) != VM_CANDIDATE_FIELDS
        or candidate.get("repo") != expected_root
        or candidate.get("commit") != expected_commit
        or candidate.get("tree") != expected_tree
        or candidate.get("git_clean") is not True
        or candidate.get("git_status") != ""
        or candidate.get("candidate_edited_by_auditor") is not False
        or any(candidate.get(field) != [] for field in ("cache_paths", "symlinks"))
        or not isinstance(focused, Mapping)
        or focused.get("returncode") != 0
        or int(focused.get("passed") or 0) < 149
        or focused.get("skipped") != 1
        or not isinstance(owner_probe, Mapping)
        or owner_probe.get("result") != "PASS"
        or owner_probe.get("mode") != "0600"
        or owner_probe.get("probe_removed") is not True
        or not str(owner_probe.get("derived_report_url") or "").startswith(
            "https://"
        )
        or not isinstance(closure, Mapping)
        or closure.get("schema_version")
        != "pnc_rca_fixed_cli_mcap_closure_audit_v5"
        or closure.get("sha256") != EXPECTED_VM_CLOSURE_SHA256
        or closure.get("evidence_core_sha256")
        != EXPECTED_VM_CLOSURE_CORE_SHA256
        or closure.get("candidate_commit") != expected_commit
        or closure.get("candidate_tree") != expected_tree
        or not isinstance(lineage, list)
        or not lineage
        or not isinstance(lineage[-1], Mapping)
        or lineage[-1].get("commit") != expected_commit
        or lineage[-1].get("tree") != expected_tree
        or not isinstance(inherited, Mapping)
        or inherited.get("candidate_commit")
        != "4b26cc7935eb4fa0910b42abde78d7f8d4efa0d1"
        or inherited.get("sha256")
        != "0765e0adfb3e74abe6a1daaea626901003b9b0cb94223a0b401d626d1a48d1bf"
        or inherited.get("authorizes_current_candidate") is not False
        or not isinstance(remote, Mapping)
        or remote.get("commit") != expected_commit
        or remote.get("matches_candidate") is not True
        or not isinstance(report, Mapping)
        or report.get("manifest_schema_version") != REPORT_MANIFEST_SCHEMA_VERSION
        or report.get("public_origin_scheme") != "https"
        or report.get("explicit_port_forbidden") is not True
        or report.get("ip_literal_forbidden") is not True
        or report.get("private_upstream_publication_forbidden") is not True
        or report.get("environment_variable") != "G1Q3_RCA_VIEWER_ORIGIN"
        or report.get("owner_only_environment_file")
        != "/home/mini/.config/g1q3-rca/report-http.env"
        or report.get("public_url_pattern")
        != (
            "<canonical_https_dns_origin>/G1Q3_RCA/cases/<submission_key>/"
            "<artifact_set_id>/index.html"
        )
    ):
        raise ControlledGrayError("controlled_gray_vm_go_receipt_invalid")
    observed_at = _timestamp(body.get("observed_at"), field="vm_go_observed_at")
    if observed_at > now + timedelta(minutes=5):
        raise ControlledGrayError("controlled_gray_vm_go_receipt_future_dated")
    return {
        "root": expected_root,
        "commit": expected_commit,
        "tree": expected_tree,
        "independent_go_receipt": {
            "observed_path": str(path),
            "sha256": receipt_sha,
            "schema_version": body["schema_version"],
            "verdict": "GO",
            "release_recommendation": body["release_recommendation"],
            "authoritative_owner_only_path": EXPECTED_VM_GO_AUTHORITATIVE_PATH,
            "replica_path": EXPECTED_VM_GO_REPLICA_PATH,
            "user_visible_cifs_path": EXPECTED_VM_GO_CIFS_PATH,
        },
    }


def _execution_contract() -> dict[str, Any]:
    return {
        "scope": {
            "ordered_targets": [
                {
                    "kind": "exact_issue",
                    "api_project_key": TARGET_API_PROJECT_KEY,
                    "project_simple_name": TARGET_PROJECT_SIMPLE_NAME,
                    "work_item_type_key": TARGET_WORK_ITEM_TYPE_KEY,
                    "work_item_id": TARGET_WORK_ITEM_ID,
                    "issue_url": TARGET_ISSUE_URL,
                },
                {
                    "kind": "first_natural_kafka_canary_after_target",
                    "count": 1,
                    "source_kind": "kafka_workflow_event",
                    "delivery_source": "ordinary_kafka_ingest",
                    "synthetic": False,
                    "manual_trigger": False,
                    "operator_recovery": False,
                },
            ],
            "additional_issues_forbidden": True,
        },
        "serial_failure_fence": {
            "max_concurrency": 1,
            "max_in_flight_write_sets": 1,
            "stop_on_first_failure": True,
            "suppress_all_later_writes_after_failure": True,
            "target_readback_must_pass_before_canary": True,
        },
        "kafka_observation": {
            "exact_target": {
                "mode": "resident_owner_only_exact_recovery",
                "assignment": "explicit_single_partition",
                "topic": TARGET_TOPIC,
                "partition": TARGET_PARTITION,
                "offset": TARGET_OFFSET,
                "group_id": None,
                "enable_auto_commit": False,
                "commit_api_allowed": False,
                "commit_called": False,
                "offset_store_mutation_allowed": False,
                "activation_slot_kind": "kafka_success",
            },
            "first_natural": {
                "mode": "resident_natural_canary_gate",
                "delivery_source": "ordinary_kafka_ingest",
                "group_id": "rca_root_cause_analysis_agent",
                "enable_auto_commit": False,
                "commit_api_allowed": True,
                "commit_after_durable_ingest": True,
                "max_poll_records": 1,
                "pause_after_first_accepted": True,
                "failure_auto_stop": True,
                "activation_slot_kind": "",
                "activation_reason": "activation_steady_active",
            },
        },
        "rca_execution": {
            "resource_class": RESOURCE_CLASS,
            "capacity_mode": CAPACITY_MODE,
            "real_rca_required": True,
            "fixed_service_entrypoint": (
                "api/g1q3_rca/scripts/run_rca_service_request.py"
            ),
            "fake_result_forbidden": True,
            "mock_result_forbidden": True,
            "manual_result_forbidden": True,
            "input_materialization_bytes_required": 0,
        },
        "idempotency": {
            "submission_key_create_once": True,
            "delivery_effect_create_once": True,
            "artifact_set_id_required": True,
            "delivery_id_required": True,
            "effect_key_required": True,
            "semantic_payload_sha256_required": True,
            "retry_requires_identical_semantic_payload_sha256": True,
            "artifact_set_id_binds_manifest_and_artifact_hashes": True,
            "effect_key_binds_semantic_payload_sha256": True,
            "duplicate_field_or_comment_effect_forbidden": True,
        },
        "delivery": {
            "effect_schema_version": "pnc_rca_delivery_effect_v2",
            "legacy_effect_schema_v1_forbidden": True,
            "report_link_kind": "manifest_html",
            "field_keys_in_order": list(TARGET_FIELD_KEYS),
            "result_field": {
                "field_key": RESULT_FIELD_KEY,
                "nonempty": True,
                "source": "real_rca_attribution_result",
            },
            "report_field": {
                "field_key": REPORT_FIELD_KEY,
                "source": "delivery_manifest_v2.report_url",
                "manifest_schema_version": REPORT_MANIFEST_SCHEMA_VERSION,
                "required_route_prefix": REPORT_ROUTE_PREFIX,
                "manifest_bound_html_required": True,
                "report_url_must_resolve_to_manifest_index_sha256": True,
                "foxglove_url_forbidden_for_gray": True,
                "legacy_perception_share_forbidden": True,
            },
            "evidence_comment": {
                "required": True,
                "exact_count": 1,
                "unique_marker_required": True,
                "terminal_attempt_outcome": "ack",
                "content_sha256_required": True,
                "must_bind": [
                    "effect_key_via_marker",
                    "artifact_set_id",
                    "attribution_result_text",
                    "manifest_html_report_url",
                ],
            },
            "official_readback": {
                "required": True,
                "field_adapter": OFFICIAL_FIELD_READBACK_ADAPTER,
                "comment_adapter": OFFICIAL_COMMENT_READBACK_ADAPTER,
                "combined_adapter": OFFICIAL_COMBINED_READBACK_ADAPTER,
                "source": "official_meegle_api",
                "api_project_key": TARGET_API_PROJECT_KEY,
                "project_simple_name_for_url_only": TARGET_PROJECT_SIMPLE_NAME,
                "exact_field_value_hashes_required": True,
                "exact_comment_hash_required": True,
                "result_field_hash_must_match_written_value": True,
                "report_field_hash_must_match_manifest_report_url": True,
                "comment_hash_must_match_full_evidence_comment": True,
                "full_comment_content_readback_required": True,
                "marker_only_readback_forbidden": True,
                "marker_match_count": 1,
            },
            "prewrite_http_revalidation": {
                "required": True,
                "must_match_manifest_report_url": True,
                "must_match_manifest_index_sha256": True,
                "required_before_paths": [
                    "initial_write",
                    "idempotent_existing_effect_completion",
                    "uncertain_write_recovery",
                    "field_repair_after_existing_comment",
                    "post_write_ack",
                ],
                "failure_stops_all_later_writes": True,
            },
        },
        "submission_boundary": {
            "transport": "ssh-mini-submit",
            "transport_path": str(CANONICAL_SUBMIT_PATH),
            "resource_class": RESOURCE_CLASS,
            "capacity_mode": CAPACITY_MODE,
            "regular_capacity_authorization_path": str(
                CANONICAL_CAPACITY_AUTHORIZATION_PATH
            ),
            "signed_rca_prod_admission_required_just_in_time": True,
            "revalidate_before_every_production_effect": True,
            "bootstrap_resource_class_forbidden": True,
            "bootstrap_authorization_forbidden": True,
            "resource_gate_bypass_forbidden": True,
            "queue_if_blocked": False,
        },
    }


def _build_bom(spec: Mapping[str, Any], *, now: datetime) -> dict[str, Any]:
    normalized = _normalize_spec(spec)
    host = _observe_host_candidate(normalized["host_candidate"], now=now)
    vm = _validate_vm_go_receipt(normalized["vm_candidate"], now=now)
    source_raw, source_sha = _read_stable_file(
        Path(__file__).resolve(), artifact="tool_source", maximum=MAX_SOURCE_BYTES
    )
    submit_raw, submit_sha = _read_stable_file(
        CANONICAL_SUBMIT_PATH,
        artifact="submit_tool",
        maximum=MAX_SOURCE_BYTES,
    )
    adapter_raw, adapter_sha = _read_stable_file(
        GOVERNED_EXECUTION_ADAPTER_PATH,
        artifact="execution_adapter",
        maximum=MAX_SOURCE_BYTES,
    )
    adapter_tree = _source_tree(adapter_raw, artifact="execution_adapter")
    for function_name in (
        "build_exact_request",
        "validate_exact_receipt",
        "official_full_readback",
        "build_natural_gate",
        "select_first_natural",
    ):
        _definition(adapter_tree, function_name)
    bom: dict[str, Any] = {
        "schema_version": BOM_SCHEMA_VERSION,
        "release_id": normalized["release_id"],
        "tooling": {
            "path": str(Path(__file__).resolve()),
            "sha256": source_sha,
            "size_bytes": len(source_raw),
            "mode": "validate_plan_only",
            "production_executor_present": True,
            "governed_execution_adapter": {
                "path": str(GOVERNED_EXECUTION_ADAPTER_PATH),
                "sha256": adapter_sha,
                "size_bytes": len(adapter_raw),
                "commands": list(GOVERNED_EXECUTION_ADAPTER_COMMANDS),
                "direct_control_db_writes": False,
                "direct_feishu_writes": False,
                "direct_kafka_offset_commits": False,
                "resident_consumer_required": True,
            },
            "submission_tool": {
                "path": str(CANONICAL_SUBMIT_PATH.absolute()),
                "sha256": submit_sha,
                "size_bytes": len(submit_raw),
                "required_resource_class": RESOURCE_CLASS,
            },
        },
        "components": {"host": host, "vm": vm},
        "execution_contract": _execution_contract(),
    }
    bom["bom_core_sha256"] = _sha256_value(bom)
    return bom


def _default_resource_probe() -> Mapping[str, Any]:
    expected = CANONICAL_RESOURCE_PATH.expanduser().absolute()
    configured = prod_admission.DEFAULT_RESOURCE_PATH.expanduser().absolute()
    if configured != expected:
        raise ControlledGrayError("controlled_gray_resource_path_not_canonical")
    try:
        info = os.lstat(expected)
    except OSError as exc:
        raise ControlledGrayError("controlled_gray_resource_tool_unavailable") from exc
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.geteuid()
        or stat.S_IMODE(info.st_mode) & 0o022
    ):
        raise ControlledGrayError("controlled_gray_resource_tool_identity_invalid")
    captured: dict[str, subprocess.CompletedProcess[str]] = {}

    def run(command: Sequence[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if list(command) != [str(expected), "--json", "--resource-class", RESOURCE_CLASS]:
            raise ControlledGrayError("controlled_gray_resource_command_invalid")
        completed = subprocess.run(command, **kwargs)
        captured["completed"] = completed
        return completed

    try:
        prod_admission.run_resource_preflight(
            resource_path=expected,
            run_func=run,
            capacity_mode=CAPACITY_MODE,
        )
    except prod_admission.RcaProdAdmissionError:
        if "completed" not in captured:
            raise
    completed = captured.get("completed")
    if completed is None or completed.returncode != 0:
        raise ControlledGrayError("controlled_gray_resource_probe_unavailable")
    raw = (completed.stdout or "").encode("utf-8")
    if not raw or len(raw) > MAX_RESOURCE_OUTPUT_BYTES:
        raise ControlledGrayError("controlled_gray_resource_probe_output_invalid")
    return _strict_json(raw, artifact="resource_probe")


def _validate_regular_capacity(
    report: Mapping[str, Any], *, now: datetime
) -> dict[str, Any]:
    try:
        snapshot, capacity = prod_admission.validate_resource_report(
            report, now=now, capacity_mode=CAPACITY_MODE
        )
    except prod_admission.RcaProdAdmissionError as exc:
        reasons = report.get("rca_prod_reasons")
        detail = ",".join(str(item) for item in reasons) if isinstance(reasons, list) else exc.code
        raise ControlledGrayError(
            "controlled_gray_regular_rca_prod_not_admitted", detail
        ) from exc
    authorization = report.get("rca_capacity_authorization")
    if not isinstance(authorization, Mapping):
        raise ControlledGrayError("controlled_gray_capacity_authorization_invalid")
    canonical_receipt = str(CANONICAL_CAPACITY_AUTHORIZATION_PATH.absolute())
    if (
        report.get("resource_class") != RESOURCE_CLASS
        or authorization.get("receipt_path") != canonical_receipt
        or authorization.get("authorization_ready") is not True
        or authorization.get("status") != "valid"
        or list(authorization.get("reason_codes") or [])
        or authorization.get("max_concurrency") != 1
        or capacity.get("successful_sample_count", 0) < 20
        or capacity.get("input_materialized_sample_count") != 0
    ):
        raise ControlledGrayError("controlled_gray_capacity_contract_invalid")
    observed_at = _timestamp(snapshot.get("observed_at"), field="snapshot_time")
    if now - observed_at > MAX_CAPACITY_SNAPSHOT_AGE or observed_at > now + timedelta(
        seconds=5
    ):
        raise ControlledGrayError("controlled_gray_capacity_snapshot_stale")
    resource_raw, resource_sha = _read_stable_file(
        CANONICAL_RESOURCE_PATH,
        artifact="resource_tool",
        maximum=MAX_SOURCE_BYTES,
    )
    validator_raw, validator_sha = _read_stable_file(
        CANONICAL_CAPACITY_VALIDATOR_PATH,
        artifact="capacity_validator",
        maximum=MAX_SOURCE_BYTES,
    )
    return {
        "status": "valid",
        "resource_class": RESOURCE_CLASS,
        "capacity_mode": CAPACITY_MODE,
        "canonical_resource_path": str(CANONICAL_RESOURCE_PATH.absolute()),
        "canonical_resource_sha256": resource_sha,
        "canonical_resource_size_bytes": len(resource_raw),
        "canonical_capacity_validator_path": str(
            CANONICAL_CAPACITY_VALIDATOR_PATH.absolute()
        ),
        "canonical_capacity_validator_sha256": validator_sha,
        "canonical_capacity_validator_size_bytes": len(validator_raw),
        "canonical_capacity_authorization_path": canonical_receipt,
        "validation_api": (
            "gateway.pnc_rca_prod_admission.validate_resource_report"
        ),
        "probe_command": [
            str(CANONICAL_RESOURCE_PATH.absolute()),
            "--json",
            "--resource-class",
            RESOURCE_CLASS,
        ],
        "authorization": {
            **capacity,
            "max_concurrency": 1,
        },
        "snapshot": snapshot,
        "snapshot_sha256": _sha256_value(snapshot),
        "bootstrap_used": False,
    }


def evaluate(
    spec: Mapping[str, Any],
    *,
    now: datetime | None = None,
    resource_probe: ResourceProbe = _default_resource_probe,
) -> dict[str, Any]:
    """Validate immutable candidates and the live regular rca_prod admission gate."""

    current = _now(now)
    effects = {
        "production_mutation": False,
        "production_write_attempts": 0,
        "kafka_offset_commits": 0,
        "feishu_writes": 0,
        "service_restarts": 0,
        "vm_tasks_submitted": 0,
    }
    try:
        bom = _build_bom(spec, now=current)
    except ControlledGrayError as exc:
        return {
            "schema_version": PLAN_SCHEMA_VERSION,
            "observed_at": current.isoformat(),
            "decision": "NO_GO",
            "status": "NO_GO_STATIC_VALIDATION",
            "tool_mode": "validate_plan_only",
            "bom": None,
            "bom_core_sha256": None,
            "capacity_gate": None,
            "admission_contract": None,
            "blockers": [{"code": exc.code, "detail": exc.detail}],
            "production_effects": effects,
        }
    try:
        report = resource_probe()
        capacity = _validate_regular_capacity(report, now=current)
    except (ControlledGrayError, prod_admission.RcaProdAdmissionError) as exc:
        code = getattr(exc, "code", "controlled_gray_resource_probe_failed")
        detail = getattr(exc, "detail", str(code))
        return {
            "schema_version": PLAN_SCHEMA_VERSION,
            "observed_at": current.isoformat(),
            "decision": "NO_GO",
            "status": "NO_GO_REGULAR_RCA_PROD_CAPACITY",
            "tool_mode": "validate_plan_only",
            "bom": bom,
            "bom_core_sha256": bom["bom_core_sha256"],
            "capacity_gate": {
                "status": "invalid",
                "resource_class": RESOURCE_CLASS,
                "capacity_mode": CAPACITY_MODE,
                "canonical_capacity_authorization_path": str(
                    CANONICAL_CAPACITY_AUTHORIZATION_PATH.absolute()
                ),
                "bootstrap_accepted": False,
            },
            "admission_contract": None,
            "blockers": [{"code": str(code), "detail": str(detail)}],
            "production_effects": effects,
        }
    admission: dict[str, Any] = {
        "schema_version": ADMISSION_CONTRACT_SCHEMA_VERSION,
        "bom_core_sha256": bom["bom_core_sha256"],
        "capacity_authorization_receipt_sha256": capacity["authorization"][
            "authorization_receipt_sha256"
        ],
        "capacity_authorization_fingerprint": capacity["authorization"][
            "receipt_fingerprint"
        ],
        "resource_snapshot_sha256": capacity["snapshot_sha256"],
        "resource_tool_sha256": capacity["canonical_resource_sha256"],
        "capacity_validator_sha256": capacity[
            "canonical_capacity_validator_sha256"
        ],
        "submission_tool_sha256": bom["tooling"]["submission_tool"]["sha256"],
        "transport": "ssh-mini-submit",
        "resource_class": RESOURCE_CLASS,
        "capacity_mode": CAPACITY_MODE,
        "queue_if_blocked": False,
        "bypass_allowed": False,
        "signed_rca_prod_admission_required_just_in_time": True,
        "production_effects_authorized_by_this_plan": False,
    }
    admission["contract_sha256"] = _sha256_value(admission)
    return {
        "schema_version": PLAN_SCHEMA_VERSION,
        "observed_at": current.isoformat(),
        "decision": "GO",
        "status": "GO_FOR_CONTROLLED_GRAY_SUBMISSION",
        "tool_mode": "validate_plan_only",
        "bom": bom,
        "bom_core_sha256": bom["bom_core_sha256"],
        "capacity_gate": capacity,
        "admission_contract": admission,
        "blockers": [],
        "production_effects": effects,
    }


def _write_create_once(path: Path, body: Mapping[str, Any]) -> None:
    selected = _absolute_path(path, field="output_path")
    parent = selected.parent
    try:
        parent_info = os.lstat(parent)
    except OSError as exc:
        raise ControlledGrayError("controlled_gray_output_parent_unavailable") from exc
    if (
        stat.S_ISLNK(parent_info.st_mode)
        or not stat.S_ISDIR(parent_info.st_mode)
        or parent_info.st_uid != os.geteuid()
        or selected.exists()
        or selected.is_symlink()
    ):
        raise ControlledGrayError("controlled_gray_output_identity_invalid")
    descriptor = -1
    created = False
    try:
        descriptor = os.open(
            selected,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        created = True
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(_json_document(body))
            handle.flush()
            os.fsync(handle.fileno())
        parent_descriptor = os.open(
            parent,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            os.fsync(parent_descriptor)
        finally:
            os.close(parent_descriptor)
    except ControlledGrayError:
        if created:
            try:
                selected.unlink()
            except OSError:
                pass
        raise
    except OSError as exc:
        if created:
            try:
                selected.unlink()
            except OSError:
                pass
        raise ControlledGrayError("controlled_gray_output_write_failed") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        raw, _digest = _read_stable_file(
            args.spec, artifact="spec", maximum=MAX_JSON_BYTES
        )
        spec = _strict_json(raw, artifact="spec")
        result = evaluate(spec)
        if args.output is not None:
            _write_create_once(args.output, result)
    except ControlledGrayError as exc:
        result = {
            "schema_version": PLAN_SCHEMA_VERSION,
            "decision": "NO_GO",
            "status": "NO_GO_TOOL_FAILURE",
            "tool_mode": "validate_plan_only",
            "blockers": [{"code": exc.code, "detail": exc.detail}],
            "production_effects": {
                "production_mutation": False,
                "production_write_attempts": 0,
            },
        }
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result.get("decision") == "GO" else 2


if __name__ == "__main__":
    raise SystemExit(main())
