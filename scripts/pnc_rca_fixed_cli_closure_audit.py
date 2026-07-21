#!/usr/bin/env python3
"""Build a read-only MCAP hard-rule audit for a fixed-CLI import closure."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import re
import subprocess
from collections import deque
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


SCHEMA_VERSION = "pnc_rca_fixed_cli_mcap_closure_audit_v4"
MAX_SOURCE_BYTES = 2 * 1024 * 1024
MAX_MODULES = 512
MAX_FILESYSTEM_ENTRIES = 200_000
_COMMIT_RE = re.compile(r"[0-9a-f]{40,64}\Z")
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_MCAP_TOKEN_RE = re.compile(
    r"mcap_service|mcap_data_translate|plugins_dev_nvidia|(?:^|[^a-z])mcap(?:[^a-z]|$)",
    re.IGNORECASE,
)
_RAW_SERVICE_RE = re.compile(r"/work/build/bin/mcap_service")
_RAW_DOCKER_RE = re.compile(
    r"docker(?:\s|[\"',\[\]()])+run[\s\S]{0,500}"
    r"(?:mcap_data_translate(?:-dev)?|plugins_dev_nvidia)",
    re.IGNORECASE,
)
_FORBIDDEN_OUTPUT_ROOT_RE = re.compile(
    r"(?:/mnt/minieye/pdcl/department/perception_test_team|perception_test_team)"
)
_CACHE_DIRECTORY_NAMES = frozenset({"__pycache__", ".pytest_cache"})
_CACHE_FILE_SUFFIXES = frozenset({".pyc", ".pyo"})
VM_TASK_ROOT_PATTERN = "/mnt/tmp/<submission_key>/"
CIFS_TASK_ROOT_PATTERN = (
    "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/"
    "<submission_key>/"
)
FORBIDDEN_OUTPUT_ROOTS = (
    "/mnt/minieye/pdcl/department/perception_test_team",
)
FIXED_SERVICE_ENTRYPOINT = "api/g1q3_rca/scripts/run_rca_service_request.py"
_CLASSIFIED_FORBIDDEN_ROOT_REFERENCES = {
    (
        "api/g1q3_rca/config.py",
        "cfa205fc5575bfad36f878224e15317a627ba9be5de002645554569a9b09bffd",
    ): "benchmark_input_root",
    (
        "api/g1q3_rca/config.py",
        "cce3cef3a238fd7ff98426be5d0c189bda0d57d4082bd12e1ccb0a055f5a7986",
    ): "legacy_cli_display_root",
    (
        "api/g1q3_rca/scripts/check_case_gate.py",
        "dcdde5d3f72f4181925a9056c6f45ba2a8bab8d91ee65d85f68bec7bb65cb108",
    ): "retired_case_input_root",
    (
        "api/g1q3_rca/scripts/check_case_gate.py",
        "1c963b9ceea2458780f64a48cd91c0733daae5929157dd887796bc7993b71e57",
    ): "isolated_replay_input_root",
    (
        "api/g1q3_rca/scripts/materialize_g1q3_cases.py",
        "dcdde5d3f72f4181925a9056c6f45ba2a8bab8d91ee65d85f68bec7bb65cb108",
    ): "retired_materialization_input_root",
    (
        "api/g1q3_rca/scripts/run_rca_auto_pipeline.py",
        "12b3ef606617ff017e775fad6907ebcc688a6a14789f28dfdf896d8c5ed5f2a2",
    ): "input_mount_boundary",
    (
        "api/g1q3_rca/scripts/run_rca_auto_pipeline.py",
        "115ba81cf9e6be4681a006784ae932c8c1fb851b6c42d6f2ef89ae5b662ec041",
    ): "production_input_guard",
    (
        "api/g1q3_rca/scripts/run_rca_auto_pipeline.py",
        "587e2f09d22868510ed86af72cb0a619597565ec4aca1fd424c6cc179e457b57",
    ): "comment_only",
    (
        "api/g1q3_rca/scripts/run_rca_auto_pipeline.py",
        "73b2e7e1ccd98d5cf36ae04bb8b9fb01ac3d9c815d072508ce9d8c1123693266",
    ): "isolated_replay_validation_message",
    (
        "api/g1q3_rca/scripts/run_rca_auto_pipeline.py",
        "d4f12f94f7c66599d2558963400bcf20ec21d9dfc333cd4d95abeb024dc0ff0d",
    ): "docstring_only",
    (
        "api/g1q3_rca/scripts/run_rca_execution_request.py",
        "949621863d2f8cdd907459ccc4b6a696a27ba6eb4a95d4d1f5b237f31af6bf39",
    ): "display_path_mapping",
    (
        "api/g1q3_rca/scripts/run_rca_execution_request.py",
        "826fab546ea9f6ef14bce24ced99c35f0aaf2edfee19d6ae92cc51014494f543",
    ): "display_path_mapping",
}
REPORT_SERVER_ENTRYPOINT = "api/g1q3_rca/scripts/serve_rca_reports.py"
REPORT_SERVICE_UNIT = "api/g1q3_rca/systemd/g1q3-rca-report-http.service"
REPORT_SERVICE_UNIT_NAME = "g1q3-rca-report-http.service"
REPORT_SERVICE_LIVE_UNIT_PATH = (
    "/home/mini/.config/systemd/user/g1q3-rca-report-http.service"
)
REPORT_ROOT = "/mnt/tmp"
REPORT_ROUTE_PREFIX = "/G1Q3_RCA/cases/"
REPORT_BIND = "0.0.0.0"
REPORT_PORT = 18081
REPORT_MAX_FILE_BYTES = 256 * 1024 * 1024
REPORT_MAX_BUNDLE_BYTES = 512 * 1024 * 1024
REPORT_MAX_PATH_BYTES = 4096
REPORT_MAX_FILE_DEPTH = 16
DELIVERY_LINEAGE_PATH = "api/g1q3_rca/rca_delivery_lineage.py"
REMOTE_FILE_ROUTE_PREFIX = "/g1q3-rca-artifacts/v1/"
REPORT_MAX_CONCURRENT_REQUESTS = 4
REPORT_REQUEST_QUEUE_SIZE = 16
REPORT_ENVIRONMENT_FILE = "/home/mini/.config/g1q3-rca/report-http.env"
REPORT_VIEWER_ORIGIN_VARIABLE = "G1Q3_RCA_VIEWER_ORIGIN"
_SUBPROCESS_NAMES = {
    "subprocess.call",
    "subprocess.check_call",
    "subprocess.check_output",
    "subprocess.Popen",
    "subprocess.run",
    "os.popen",
    "os.system",
}
_DIRECT_WRITE_METHODS = frozenset(
    {
        "mkdir",
        "rename",
        "replace",
        "symlink_to",
        "touch",
        "write_bytes",
        "write_text",
    }
)


class ClosureAuditError(ValueError):
    def __init__(self, code: str):
        self.code = str(code or "fixed_cli_closure_audit_invalid")[:120]
        super().__init__(self.code)


def _canonical_json(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _absolute(value: str | Path, *, field: str) -> Path:
    text = str(value or "").strip()
    path = Path(text).expanduser()
    if not text or not path.is_absolute() or ".." in path.parts or "\x00" in text:
        raise ClosureAuditError(f"fixed_cli_closure_{field}_invalid")
    return path.absolute()


def _relative(value: str | Path, *, field: str) -> str:
    text = str(value or "").strip()
    path = Path(text)
    if (
        not text
        or path.is_absolute()
        or ".." in path.parts
        or "." in path.parts
        or "\x00" in text
    ):
        raise ClosureAuditError(f"fixed_cli_closure_{field}_invalid")
    return path.as_posix()


def _git(root: Path, *arguments: str, text: bool = True) -> str | bytes:
    environment = os.environ.copy()
    environment["GIT_OPTIONAL_LOCKS"] = "0"
    completed = subprocess.run(
        ["git", "-C", str(root), *arguments],
        check=False,
        capture_output=True,
        text=text,
        shell=False,
        timeout=60,
        env=environment,
    )
    if completed.returncode != 0:
        raise ClosureAuditError("fixed_cli_closure_git_failed")
    return completed.stdout


def _tracked_paths(root: Path) -> list[str]:
    raw = _git(root, "ls-files", "-z", text=False)
    assert isinstance(raw, bytes)
    paths = sorted(
        item.decode("utf-8", errors="strict")
        for item in raw.split(b"\x00")
        if item
    )
    return paths


def _git_path_list(root: Path, *arguments: str) -> list[str]:
    raw = _git(root, *arguments, "-z", text=False)
    if not isinstance(raw, bytes):
        raise ClosureAuditError("fixed_cli_closure_git_failed")
    return sorted(
        item.decode("utf-8", errors="strict")
        for item in raw.split(b"\x00")
        if item
    )


def _is_cache_path(relative: str) -> bool:
    path = Path(relative)
    return (
        any(part in _CACHE_DIRECTORY_NAMES for part in path.parts)
        or path.suffix.lower() in _CACHE_FILE_SUFFIXES
    )


def _filesystem_cache_paths(root: Path) -> list[str]:
    rows: list[str] = []
    observed = 0
    for current, directory_names, file_names in os.walk(
        root, topdown=True, followlinks=False
    ):
        current_path = Path(current)
        if current_path == root:
            directory_names[:] = [name for name in directory_names if name != ".git"]
        for name in sorted(directory_names):
            observed += 1
            if name in _CACHE_DIRECTORY_NAMES:
                rows.append((current_path / name).relative_to(root).as_posix() + "/")
        for name in sorted(file_names):
            observed += 1
            relative = (current_path / name).relative_to(root).as_posix()
            if _is_cache_path(relative):
                rows.append(relative)
        if observed > MAX_FILESYSTEM_ENTRIES:
            raise ClosureAuditError("fixed_cli_closure_filesystem_limit")
    return sorted(set(rows))


def _filesystem_seal(root: Path, tracked: Sequence[str]) -> Mapping[str, Any]:
    tracked_cache_paths = sorted(path for path in tracked if _is_cache_path(path))
    ignored_paths = _git_path_list(
        root, "ls-files", "--others", "--ignored", "--exclude-standard"
    )
    untracked_paths = _git_path_list(
        root, "ls-files", "--others", "--exclude-standard"
    )
    filesystem_cache_paths = _filesystem_cache_paths(root)
    if tracked_cache_paths or ignored_paths or untracked_paths or filesystem_cache_paths:
        raise ClosureAuditError("fixed_cli_closure_filesystem_not_sealed")
    return {
        "tracked_cache_path_count": 0,
        "ignored_path_count": 0,
        "untracked_path_count": 0,
        "filesystem_cache_path_count": 0,
        "pyc_file_count": 0,
        "pycache_directory_count": 0,
        "pytest_cache_directory_count": 0,
        "exact_source_seal": True,
    }


def _git_mode(root: Path, relative: str) -> str:
    raw = str(_git(root, "ls-files", "--stage", "--", relative) or "")
    rows = [line for line in raw.splitlines() if line.strip()]
    if len(rows) != 1 or "\t" not in rows[0]:
        raise ClosureAuditError("fixed_cli_closure_report_service_invalid")
    metadata, observed_path = rows[0].split("\t", 1)
    fields = metadata.split()
    if len(fields) != 3 or observed_path != relative:
        raise ClosureAuditError("fixed_cli_closure_report_service_invalid")
    return fields[0]


def _report_service_binding(
    root: Path, tracked: Sequence[str]
) -> Mapping[str, Any]:
    required_paths = {REPORT_SERVER_ENTRYPOINT, REPORT_SERVICE_UNIT}
    if not required_paths.issubset(set(tracked)):
        raise ClosureAuditError("fixed_cli_closure_report_service_invalid")

    server_path = root / REPORT_SERVER_ENTRYPOINT
    unit_path = root / REPORT_SERVICE_UNIT
    if (
        server_path.is_symlink()
        or not server_path.is_file()
        or unit_path.is_symlink()
        or not unit_path.is_file()
        or server_path.stat().st_size > MAX_SOURCE_BYTES
        or unit_path.stat().st_size > MAX_SOURCE_BYTES
        or _git_mode(root, REPORT_SERVER_ENTRYPOINT) != "100755"
        or _git_mode(root, REPORT_SERVICE_UNIT) != "100644"
    ):
        raise ClosureAuditError("fixed_cli_closure_report_service_invalid")

    server_payload, server_text, _tree = _read_python(server_path)
    required_server_markers = (
        'REPORT_ROOT = Path("/mnt/tmp")',
        'REPORT_BIND = "0.0.0.0"',
        "REPORT_PORT = 18081",
        'REPORT_ROUTE_PREFIX = ("G1Q3_RCA", "cases")',
        'VIZ_ROUTE_PREFIX = ("g1q3-rca-artifacts", "v1")',
        'DELIVERY_MANIFEST_SCHEMA = "delivery_manifest_v2"',
        'VIZ_PUBLICATION_SCHEMA = "g1q3_rca_viz_publication_v1"',
        "MAX_REPORT_FILE_BYTES = 256 * 1024 * 1024",
        "MAX_REPORT_BUNDLE_BYTES = 512 * 1024 * 1024",
        "MAX_VIZ_FILE_BYTES = 8 * 1024 * 1024 * 1024",
        "MAX_CONCURRENT_REQUESTS = 4",
        "REQUEST_QUEUE_SIZE = 16",
        "MAX_PATH_BYTES = 4096",
        "MAX_FILE_DEPTH = 16",
        'SUBMISSION_RE = re.compile(r"g1q3-rca-s1-[0-9a-f]{64}\\Z")',
        'ARTIFACT_SET_RE = re.compile(r"g1q3-rca-artifact-v1-[0-9a-f]{64}\\Z")',
        'ENCODED_SEPARATOR_RE = re.compile(r"%(?:2f|5c)", re.IGNORECASE)',
        'BYTE_RANGE_RE = re.compile(r"bytes=([0-9]{0,20})-([0-9]{0,20})\\Z")',
        'len(decoded.encode("utf-8")) > MAX_PATH_BYTES',
        "len(file_parts) > MAX_FILE_DEPTH",
        'getattr(os, "O_DIRECTORY", 0)',
        'getattr(os, "O_NOFOLLOW", 0)',
        "not stat.S_ISREG(before.st_mode)",
        "def parse_byte_range(",
        "def do_HEAD(self)",
        "def do_GET(self)",
        "def do_OPTIONS(self)",
        "def do_POST(self)",
        "def list_directory(self, _path: str)",
        'self.headers.get("Range")',
        "def canonical_viewer_origin(value: str)",
        'self.send_header("Access-Control-Allow-Origin", viewer_origin)',
        "class BoundedHTTPServer(HTTPServer)",
        "ThreadPoolExecutor(",
        "threading.BoundedSemaphore(max_workers)",
        "self.viewer_origin = canonical_viewer_origin(viewer_origin)",
        "server.serve_forever(poll_interval=0.5)",
    )
    singleton_markers = required_server_markers[:14]
    if (
        any(marker not in server_text for marker in required_server_markers)
        or any(server_text.count(marker) != 1 for marker in singleton_markers)
        or "SimpleHTTPRequestHandler" in server_text
        or _FORBIDDEN_OUTPUT_ROOT_RE.search(server_text) is not None
    ):
        raise ClosureAuditError("fixed_cli_closure_report_service_invalid")

    try:
        unit_payload = unit_path.read_bytes()
        unit_text = unit_payload.decode("utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise ClosureAuditError("fixed_cli_closure_report_service_invalid") from exc
    expected_exec_start = (
        f"/usr/bin/python3 -I -B {root}/{REPORT_SERVER_ENTRYPOINT} "
        f"--root {REPORT_ROOT} --bind {REPORT_BIND} --port {REPORT_PORT} "
        f"--viewer-origin ${{{REPORT_VIEWER_ORIGIN_VARIABLE}}}"
    )
    expected_directives = {
        "Type": "simple",
        "Environment": "PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1",
        "EnvironmentFile": REPORT_ENVIRONMENT_FILE,
        "ExecStart": expected_exec_start,
        "WorkingDirectory": "/",
        "UMask": "0077",
        "NoNewPrivileges": "true",
        "PrivateDevices": "true",
        "PrivateTmp": "true",
        "ProtectSystem": "strict",
        "ProtectHome": "read-only",
        "ReadOnlyPaths": f"{REPORT_ROOT} {root} {REPORT_ENVIRONMENT_FILE}",
        "InaccessiblePaths": FORBIDDEN_OUTPUT_ROOTS[0],
        "RestrictSUIDSGID": "true",
        "LockPersonality": "true",
        "MemoryDenyWriteExecute": "true",
        "RestrictAddressFamilies": "AF_INET AF_INET6",
    }
    lines = [line.strip() for line in unit_text.splitlines()]
    for key, value in expected_directives.items():
        observed = [line for line in lines if line.startswith(f"{key}=")]
        if observed != [f"{key}={value}"]:
            raise ClosureAuditError("fixed_cli_closure_report_service_invalid")
    if " -m http.server" in unit_text or "SimpleHTTPServer" in unit_text:
        raise ClosureAuditError("fixed_cli_closure_report_service_invalid")

    return {
        "unit": REPORT_SERVICE_UNIT_NAME,
        "entrypoint_relative": REPORT_SERVER_ENTRYPOINT,
        "entrypoint_path": str(server_path),
        "entrypoint_sha256": _sha256_bytes(server_payload),
        "entrypoint_git_mode": "100755",
        "candidate_unit_relative": REPORT_SERVICE_UNIT,
        "candidate_unit_path": str(unit_path),
        "candidate_unit_sha256": _sha256_bytes(unit_payload),
        "candidate_unit_git_mode": "100644",
        "live_unit_path": REPORT_SERVICE_LIVE_UNIT_PATH,
        "exec_start": expected_exec_start,
        "root": REPORT_ROOT,
        "route_prefix": REPORT_ROUTE_PREFIX,
        "port": REPORT_PORT,
        "directory_listing": False,
        "path_traversal": False,
        "symlink_escape": False,
        "read_only": True,
        "old_broad_http_server_forbidden": True,
        "environment_file": REPORT_ENVIRONMENT_FILE,
        "viewer_origin_variable": REPORT_VIEWER_ORIGIN_VARIABLE,
        "delivery_manifest_schema": "delivery_manifest_v2",
        "viz_manifest_schema": "g1q3_rca_viz_publication_v1",
        "max_concurrent_requests": REPORT_MAX_CONCURRENT_REQUESTS,
        "request_queue_size": REPORT_REQUEST_QUEUE_SIZE,
    }


def _remote_file_transport_binding(
    report_service: Mapping[str, Any]
) -> Mapping[str, Any]:
    entrypoint_sha256 = str(report_service.get("entrypoint_sha256") or "")
    if _SHA256_RE.fullmatch(entrypoint_sha256) is None:
        raise ClosureAuditError("fixed_cli_closure_remote_file_transport_invalid")
    vm_pattern = (
        "http://192.168.26.174:18081/g1q3-rca-artifacts/v1/"
        "<submission_key>/<submission_key>.viz.mcap"
    )
    https_pattern = (
        "<canonical_https_dns_origin>/g1q3-rca-artifacts/v1/"
        "<submission_key>/<submission_key>.viz.mcap"
    )
    return {
        "source_id": "remote-file",
        "viewer_query_parameter": "ds.url",
        "vm_route_prefix": REMOTE_FILE_ROUTE_PREFIX,
        "vm_http_url_pattern": vm_pattern,
        "public_https_url_pattern": https_pattern,
        "viewer_origin": "runtime_environment:G1Q3_RCA_VIEWER_ORIGIN",
        "viewer_url_contract": (
            "<canonical_https_dns_origin>/?ds=remote-file&ds.url="
            "<strict_percent_encoded_public_https_url>"
        ),
        "entrypoint_sha256": entrypoint_sha256,
        "head": True,
        "get": True,
        "single_byte_range": True,
        "suffix_byte_range": True,
        "range_not_satisfiable_416": True,
        "cors_exact_origin": True,
        "cors_preflight": True,
        "regular_file_only": True,
        "symlink_escape": False,
        "viewer_same_origin_https_proxy_required": True,
        "viewer_proxy_live_observed": False,
        "direct_http_browser_viable": False,
        "release_blocked_until_viewer_proxy_proven": True,
    }


def _delivery_manifest_binding(
    root: Path, tracked: Sequence[str]
) -> Mapping[str, Any]:
    if DELIVERY_LINEAGE_PATH not in set(tracked):
        raise ClosureAuditError("fixed_cli_closure_delivery_contract_invalid")
    path = root / DELIVERY_LINEAGE_PATH
    if (
        path.is_symlink()
        or not path.is_file()
        or path.stat().st_size > MAX_SOURCE_BYTES
        or _git_mode(root, DELIVERY_LINEAGE_PATH) != "100644"
    ):
        raise ClosureAuditError("fixed_cli_closure_delivery_contract_invalid")
    payload, source, _tree = _read_python(path)
    markers = (
        'DELIVERY_MANIFEST_SCHEMA = "delivery_manifest_v2"',
        'TASK_ARTIFACT_ROOT = Path("/mnt/tmp")',
        "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp",
        "FORMAL_REPORT_ROOT = TASK_ARTIFACT_ROOT",
        "def build_report_vm_path(",
        "def build_report_cifs_path(",
        "def build_report_url(",
        'f"http://{FORMAL_REPORT_HOST}:{FORMAL_REPORT_PORT}/G1Q3_RCA/cases/"',
        'expected_root = Path("/mnt/tmp") / submission_key',
        "expected_artifact_root = TASK_ARTIFACT_ROOT / submission_key",
        "destination = parent / artifact_set_id",
        'destination = parent / f"{submission_key}.viz.mcap"',
        '"report_vm_path"',
        '"report_cifs_path"',
        '"report_url"',
    )
    if (
        any(marker not in source for marker in markers)
        or 'DELIVERY_MANIFEST_SCHEMA = "delivery_manifest_v1"' in source
        or _FORBIDDEN_OUTPUT_ROOT_RE.search(source) is not None
    ):
        raise ClosureAuditError("fixed_cli_closure_delivery_contract_invalid")
    return {
        "schema_version": "delivery_manifest_v2",
        "lineage_relative": DELIVERY_LINEAGE_PATH,
        "lineage_path": str(path),
        "lineage_sha256": _sha256_bytes(payload),
        "lineage_git_mode": "100644",
        "artifact_root_pattern": "/mnt/tmp/<submission_key>/",
        "report_vm_path_pattern": (
            "/mnt/tmp/<submission_key>/<artifact_set_id>/index.html"
        ),
        "report_cifs_path_pattern": (
            "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/"
            "tmp/<submission_key>/<artifact_set_id>/index.html"
        ),
        "report_url_pattern": (
            "http://192.168.26.174:18081/G1Q3_RCA/cases/"
            "<submission_key>/<artifact_set_id>/index.html"
        ),
        "viz_vm_path_pattern": (
            "/mnt/tmp/<submission_key>/<submission_key>.viz.mcap"
        ),
        "legacy_v1_deliverable": False,
        "perception_test_team_output": False,
    }


def _module_for_relative(relative: str) -> str:
    path = Path(relative)
    if path.suffix != ".py":
        raise ClosureAuditError("fixed_cli_closure_entrypoint_not_python")
    parts = list(path.with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    if not parts:
        raise ClosureAuditError("fixed_cli_closure_module_invalid")
    return ".".join(parts)


def _module_path(root: Path, module: str) -> Path | None:
    if not module or any(not part.isidentifier() for part in module.split(".")):
        return None
    relative = Path(*module.split("."))
    file_candidate = root / relative.with_suffix(".py")
    package_candidate = root / relative / "__init__.py"
    matches = [
        item
        for item in (file_candidate, package_candidate)
        if item.is_file() and not item.is_symlink()
    ]
    if len(matches) > 1:
        raise ClosureAuditError("fixed_cli_closure_module_ambiguous")
    return matches[0] if matches else None


def _parent_packages(root: Path, module: str) -> Iterable[str]:
    parts = module.split(".")
    for index in range(1, len(parts)):
        parent = ".".join(parts[:index])
        if (root / Path(*parts[:index]) / "__init__.py").is_file():
            yield parent


def _from_base(current_module: str, current_path: Path, node: ast.ImportFrom) -> str:
    package = (
        current_module
        if current_path.name == "__init__.py"
        else current_module.rpartition(".")[0]
    )
    if node.level:
        parts = package.split(".") if package else []
        remove = node.level - 1
        if remove > len(parts):
            return ""
        parts = parts[: len(parts) - remove] if remove else parts
        if node.module:
            parts.extend(node.module.split("."))
        return ".".join(parts)
    return str(node.module or "")


def _import_candidates(module: str, path: Path, tree: ast.AST) -> set[str]:
    candidates: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            candidates.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            base = _from_base(module, path, node)
            if base:
                candidates.add(base)
                candidates.update(
                    f"{base}.{alias.name}"
                    for alias in node.names
                    if alias.name != "*"
                )
    return candidates


def _read_python(path: Path) -> tuple[bytes, str, ast.AST]:
    payload = path.read_bytes()
    if len(payload) > MAX_SOURCE_BYTES:
        raise ClosureAuditError("fixed_cli_closure_source_too_large")
    try:
        text = payload.decode("utf-8")
        tree = ast.parse(text, filename=str(path))
    except (UnicodeDecodeError, SyntaxError) as exc:
        raise ClosureAuditError("fixed_cli_closure_source_invalid") from exc
    return payload, text, tree


def _reachable_modules(root: Path, entrypoint: str) -> list[Mapping[str, Any]]:
    entry_path = root / entrypoint
    if entry_path.is_symlink() or not entry_path.is_file():
        raise ClosureAuditError("fixed_cli_closure_entrypoint_missing")
    initial = _module_for_relative(entrypoint)
    queue = deque([initial])
    queued = {initial}
    rows: dict[str, Mapping[str, Any]] = {}
    while queue:
        module = queue.popleft()
        path = _module_path(root, module)
        if path is None:
            continue
        relative = path.relative_to(root).as_posix()
        payload, text, tree = _read_python(path)
        rows[module] = {
            "module": module,
            "relative_path": relative,
            "sha256": _sha256_bytes(payload),
            "text": text,
            "tree": tree,
        }
        candidates = set(_parent_packages(root, module))
        candidates.update(_import_candidates(module, path, tree))
        for candidate in sorted(candidates):
            if candidate not in queued and _module_path(root, candidate) is not None:
                queued.add(candidate)
                queue.append(candidate)
        if len(queued) > MAX_MODULES:
            raise ClosureAuditError("fixed_cli_closure_module_limit")
    return [rows[key] for key in sorted(rows)]


def _call_name(node: ast.AST) -> str:
    parts: list[str] = []
    current = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if isinstance(current, ast.Name):
        parts.append(current.id)
    return ".".join(reversed(parts))


def _literal_strings(node: ast.AST) -> list[str]:
    values: list[str] = []
    for item in ast.walk(node):
        if isinstance(item, ast.Constant) and isinstance(item.value, str):
            values.append(item.value)
    return values


def _line_hits(relative: str, text: str, pattern: re.Pattern[str], kind: str) -> list[dict[str, Any]]:
    rows = []
    for line_no, line in enumerate(text.splitlines(), 1):
        if pattern.search(line):
            rows.append(
                {
                    "kind": kind,
                    "relative_path": relative,
                    "line": line_no,
                    "line_sha256": _sha256_bytes(line.encode("utf-8")),
                }
            )
    return rows


def _classified_forbidden_root_references(
    relative: str, text: str
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    classified: list[dict[str, Any]] = []
    unclassified: list[dict[str, Any]] = []
    for item in _line_hits(
        relative,
        text,
        _FORBIDDEN_OUTPUT_ROOT_RE,
        "forbidden_output_root_reference",
    ):
        key = (relative, str(item["line_sha256"]))
        role = _CLASSIFIED_FORBIDDEN_ROOT_REFERENCES.get(key)
        if role is None:
            unclassified.append(
                {
                    **item,
                    "kind": "unclassified_forbidden_output_root_reference",
                }
            )
        else:
            classified.append({**item, "role": role})
    return classified, unclassified


def _forbidden_output_sink_hits(
    relative: str, text: str, tree: ast.AST
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = _call_name(node.func)
        path_nodes: list[ast.AST] = []
        if isinstance(node.func, ast.Attribute) and node.func.attr in _DIRECT_WRITE_METHODS:
            path_nodes.append(node.func.value)
        elif name in {"os.mkdir", "os.makedirs", "os.remove", "os.unlink"}:
            path_nodes.extend(node.args[:1])
        elif name in {"os.rename", "os.replace", "shutil.copy", "shutil.copy2", "shutil.move"}:
            path_nodes.extend(node.args[1:2])
        elif name in {"open", "builtins.open"} and node.args:
            modes = _literal_strings(node.args[1]) if len(node.args) > 1 else []
            modes.extend(
                value
                for keyword in node.keywords
                if keyword.arg == "mode"
                for value in _literal_strings(keyword.value)
            )
            if any(any(flag in mode for flag in "wax+") for mode in modes):
                path_nodes.append(node.args[0])
        if not path_nodes or not any(
            _FORBIDDEN_OUTPUT_ROOT_RE.search(value)
            for path_node in path_nodes
            for value in _literal_strings(path_node)
        ):
            continue
        segment = ast.get_source_segment(text, node) or ""
        rows.append(
            {
                "kind": "forbidden_output_sink",
                "relative_path": relative,
                "line": int(getattr(node, "lineno", 0) or 0),
                "call": name or (
                    node.func.attr if isinstance(node.func, ast.Attribute) else ""
                ),
                "segment_sha256": _sha256_bytes(segment.encode("utf-8")),
            }
        )
    return rows


def _fixed_service_output_binding(
    entrypoint: str, modules: Sequence[Mapping[str, Any]]
) -> Mapping[str, Any]:
    if entrypoint != FIXED_SERVICE_ENTRYPOINT:
        return {"applicable": False, "enforced": False}
    rows = [item for item in modules if item.get("relative_path") == entrypoint]
    if len(rows) != 1:
        raise ClosureAuditError("fixed_cli_closure_output_binding_invalid")
    item = rows[0]
    text = str(item.get("text") or "")
    markers = (
        'ARTIFACT_ROOT = Path("/mnt/tmp")',
        'ARTIFACT_CIFS_PREFIX = "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/"',
        'artifact_root = Path(str(data.get("artifact_root") or ""))',
        "expected_root = ARTIFACT_ROOT / task_id",
        "if not artifact_root.is_absolute() or artifact_root != expected_root:",
        'raise ServiceRequestError("service_artifact_root_invalid", exit_code=21)',
        'expected_cifs = f"{ARTIFACT_CIFS_PREFIX}{task_id}/"',
        'if str(data.get("artifact_cifs_root") or "") != expected_cifs:',
        'cases_root = artifact_root / "cases"',
        "artifact_root, artifact_cifs_root = _validate_identity(task_id, admission, request)",
        "pipeline_result = pipeline_runner(",
    )
    if any(text.count(marker) != 1 for marker in markers):
        raise ClosureAuditError("fixed_cli_closure_output_binding_invalid")
    return {
        "applicable": True,
        "enforced": True,
        "entrypoint": entrypoint,
        "entrypoint_sha256": str(item.get("sha256") or ""),
        "vm_task_root_pattern": VM_TASK_ROOT_PATTERN,
        "cifs_task_root_pattern": CIFS_TASK_ROOT_PATTERN,
        "identity_validation_precedes_pipeline": True,
        "pipeline_output_root_from_validated_identity": True,
    }


def _subprocess_hits(relative: str, text: str, tree: ast.AST) -> list[dict[str, Any]]:
    rows = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or _call_name(node.func) not in _SUBPROCESS_NAMES:
            continue
        segment = ast.get_source_segment(text, node) or ""
        material = " ".join([segment, *_literal_strings(node)])
        if _MCAP_TOKEN_RE.search(material) is None:
            continue
        command_lower = material.lower()
        timeout_kw = any(item.arg == "timeout" for item in node.keywords)
        governed_wrapper = "ssh-mini-mcap-run" in command_lower
        docker = "docker" in command_lower and "run" in command_lower
        bounded = (
            governed_wrapper
            or timeout_kw
            or re.search(r"(?:^|[\s\"'])timeout(?:[\s\"']|$)", command_lower)
            is not None
        )
        docker_limited = (
            not docker
            or (
                "--memory" in command_lower
                and "--cpus" in command_lower
                and bounded
            )
        )
        rows.append(
            {
                "kind": "mcap_subprocess",
                "relative_path": relative,
                "line": int(getattr(node, "lineno", 0) or 0),
                "call": _call_name(node.func),
                "segment_sha256": _sha256_bytes(segment.encode("utf-8")),
                "governed_wrapper": governed_wrapper,
                "bounded": bounded,
                "docker_limited": docker_limited,
                "blocker": not (bounded and docker_limited),
            }
        )
    return rows


def _source_hits(relative: str, text: str, tree: ast.AST) -> list[dict[str, Any]]:
    rows = []
    rows.extend(_line_hits(relative, text, _RAW_SERVICE_RE, "raw_mcap_service"))
    if _RAW_DOCKER_RE.search(text):
        rows.append(
            {
                "kind": "raw_mcap_docker",
                "relative_path": relative,
                "line": text[: _RAW_DOCKER_RE.search(text).start()].count("\n") + 1,
                "source_sha256": _sha256_bytes(text.encode("utf-8")),
            }
        )
    rows.extend(_subprocess_hits(relative, text, tree))
    rows.extend(_forbidden_output_sink_hits(relative, text, tree))
    rows.extend(
        _line_hits(
            relative,
            text,
            re.compile(r"SSH_MINI_(?:SUBMIT_BYPASS_RESOURCE_GATE|ALLOW_RAW_MCAP)"),
            "resource_gate_bypass",
        )
    )
    return rows


def _outside_hits(
    root: Path, tracked: Sequence[str], reachable_paths: set[str]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    production = []
    tests = []
    for relative in tracked:
        if relative in reachable_paths or Path(relative).suffix not in {".py", ".sh"}:
            continue
        path = root / relative
        if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_SOURCE_BYTES:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        kinds = []
        if _RAW_SERVICE_RE.search(text):
            kinds.append("raw_mcap_service")
        if _RAW_DOCKER_RE.search(text):
            kinds.append("raw_mcap_docker")
        if not kinds:
            continue
        row = {
            "relative_path": relative,
            "kinds": sorted(kinds),
            "sha256": _sha256_file(path),
        }
        if relative.startswith("tests/") or "/test" in relative:
            tests.append(row)
        else:
            production.append(row)
    return production, tests


def audit(
    *,
    repo_root: str | Path,
    entrypoint: str | Path,
    expected_commit: str,
    expected_tree: str,
    output_path: str | Path | None = None,
) -> Mapping[str, Any]:
    root = _absolute(repo_root, field="repo_root")
    relative_entrypoint = _relative(entrypoint, field="entrypoint")
    commit = str(_git(root, "rev-parse", "--verify", "HEAD") or "").strip().lower()
    tree = str(_git(root, "rev-parse", "HEAD^{tree}") or "").strip().lower()
    status = str(
        _git(root, "status", "--porcelain=v1", "--untracked-files=all") or ""
    )
    if (
        _COMMIT_RE.fullmatch(expected_commit) is None
        or _COMMIT_RE.fullmatch(expected_tree) is None
        or commit != expected_commit
        or tree != expected_tree
        or status
    ):
        raise ClosureAuditError("fixed_cli_closure_repo_identity_mismatch")
    tracked = _tracked_paths(root)
    filesystem_seal = _filesystem_seal(root, tracked)
    report_service = _report_service_binding(root, tracked)
    remote_file_transport = _remote_file_transport_binding(report_service)
    delivery_manifest_contract = _delivery_manifest_binding(root, tracked)
    modules = _reachable_modules(root, relative_entrypoint)
    fixed_service_output_binding = _fixed_service_output_binding(
        relative_entrypoint, modules
    )
    reachable_paths = {str(item["relative_path"]) for item in modules}
    reachable_hits = []
    classified_forbidden_root_references = []
    for item in modules:
        classified, unclassified = _classified_forbidden_root_references(
            str(item["relative_path"]), str(item["text"])
        )
        classified_forbidden_root_references.extend(classified)
        reachable_hits.extend(unclassified)
        reachable_hits.extend(
            _source_hits(
                str(item["relative_path"]),
                str(item["text"]),
                item["tree"],
            )
        )
    outside_production, outside_tests = _outside_hits(root, tracked, reachable_paths)
    blockers = [
        item
        for item in reachable_hits
        if item["kind"]
        in {
            "raw_mcap_service",
            "raw_mcap_docker",
            "resource_gate_bypass",
            "forbidden_output_sink",
            "unclassified_forbidden_output_root_reference",
        }
        or (item["kind"] == "mcap_subprocess" and item.get("blocker") is True)
    ]
    raw_execution_blockers = [
        item
        for item in blockers
        if item["kind"]
        in {"raw_mcap_service", "raw_mcap_docker", "mcap_subprocess"}
    ]
    forbidden_output_blockers = [
        item
        for item in blockers
        if item["kind"]
        in {
            "forbidden_output_sink",
            "unclassified_forbidden_output_root_reference",
        }
    ]
    remote_reader_modules = sorted(
        str(item["module"])
        for item in modules
        if "remote_reader" in str(item["module"])
        or "remote_data_access" in str(item["module"])
    )
    public_modules = [
        {
            "module": item["module"],
            "relative_path": item["relative_path"],
            "sha256": item["sha256"],
        }
        for item in modules
    ]
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "ok": not blockers,
        "repo": {
            "root": str(root),
            "commit": commit,
            "tree": tree,
            "tree_clean": True,
            "status_sha256": _sha256_bytes(status.encode("utf-8")),
        },
        "entrypoint": relative_entrypoint,
        "filesystem_seal": filesystem_seal,
        "report_service": report_service,
        "remote_file_transport": remote_file_transport,
        "delivery_manifest_contract": delivery_manifest_contract,
        "reachable": {
            "module_count": len(public_modules),
            "modules": public_modules,
            "hits": reachable_hits,
            "blockers": blockers,
            "classified_forbidden_root_references": sorted(
                classified_forbidden_root_references,
                key=lambda item: (
                    str(item["relative_path"]),
                    int(item["line"]),
                    str(item["line_sha256"]),
                ),
            ),
        },
        "unreachable": {
            "production_hits": outside_production,
            "test_hits": outside_tests,
        },
        "production_path": {
            "remote_reader_modules": remote_reader_modules,
            "remote_reader_contract_reachable": bool(remote_reader_modules),
            "rca_prod_admission_boundary": "worker_candidate_evidence_required",
            "raw_mcap_execution_reachable": bool(raw_execution_blockers),
            "forbidden_output_root_reachable": bool(forbidden_output_blockers),
            "perception_test_team_write_reachable": bool(
                [
                    item
                    for item in forbidden_output_blockers
                    if item["kind"] == "forbidden_output_sink"
                ]
            ),
            "classified_forbidden_root_reference_count": len(
                classified_forbidden_root_references
            ),
            "fixed_service_output_binding": fixed_service_output_binding,
            "output_root_contract": {
                "vm_task_root_pattern": VM_TASK_ROOT_PATTERN,
                "cifs_task_root_pattern": CIFS_TASK_ROOT_PATTERN,
                "generated_artifacts_must_remain_inside_task_root": True,
                "forbidden_output_roots": list(FORBIDDEN_OUTPUT_ROOTS),
                "perception_test_team_input_only": True,
            },
        },
        "side_effects": {
            "mcap_started": False,
            "docker_started": False,
            "production_mutation": False,
        },
    }
    result["evidence_core_sha256"] = _sha256_bytes(_canonical_json(result))
    if output_path is not None:
        output = _absolute(output_path, field="output_path")
        test_mode = os.environ.get("PNC_RCA_CLOSURE_AUDIT_TEST_MODE") == "1"
        if (
            (not test_mode and not str(output).startswith("/mnt/tmp/"))
            or output.exists()
            or output.is_symlink()
        ):
            raise ClosureAuditError("fixed_cli_closure_output_invalid")
        output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        temporary = output.with_name(f".{output.name}.tmp-{os.getpid()}")
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(_canonical_json(result))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, output)
        finally:
            if temporary.exists() or temporary.is_symlink():
                temporary.unlink()
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--entrypoint", required=True)
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--expected-tree", required=True)
    parser.add_argument("--output", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = audit(
        repo_root=args.repo_root,
        entrypoint=args.entrypoint,
        expected_commit=args.expected_commit,
        expected_tree=args.expected_tree,
        output_path=args.output,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result.get("ok") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
