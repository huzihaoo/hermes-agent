from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from scripts import pnc_rca_fixed_cli_closure_audit as audit


def _git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(root), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _repo(tmp_path: Path, *, bad: str = "") -> tuple[Path, str, str]:
    root = tmp_path / "repo"
    root.mkdir()
    _git(root, "init", "-q")
    _git(root, "config", "user.email", "audit@example.com")
    _git(root, "config", "user.name", "Audit Test")
    (root / "pkg").mkdir()
    (root / "pkg/__init__.py").write_text("", encoding="utf-8")
    (root / "pkg/remote_reader.py").write_text("VALUE = 1\n", encoding="utf-8")
    (root / "pkg/entry.py").write_text(
        "from pkg.remote_reader import VALUE\n" + bad,
        encoding="utf-8",
    )
    (root / "tests").mkdir()
    (root / "tests/test_legacy.py").write_text(
        "LEGACY = '/work/build/bin/mcap_service'\n", encoding="utf-8"
    )
    server = root / audit.REPORT_SERVER_ENTRYPOINT
    server.parent.mkdir(parents=True)
    server.write_text(
        '''#!/usr/bin/env python3
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from http.server import HTTPServer
import os
import re
import stat
import threading

REPORT_ROOT = Path("/mnt/tmp")
REPORT_BIND = "0.0.0.0"
REPORT_PORT = 18081
REPORT_ROUTE_PREFIX = ("G1Q3_RCA", "cases")
VIZ_ROUTE_PREFIX = ("g1q3-rca-artifacts", "v1")
DELIVERY_MANIFEST_SCHEMA = "delivery_manifest_v2"
VIZ_PUBLICATION_SCHEMA = "g1q3_rca_viz_publication_v1"
MAX_REPORT_FILE_BYTES = 256 * 1024 * 1024
MAX_REPORT_BUNDLE_BYTES = 512 * 1024 * 1024
MAX_VIZ_FILE_BYTES = 8 * 1024 * 1024 * 1024
MAX_CONCURRENT_REQUESTS = 4
REQUEST_QUEUE_SIZE = 16
MAX_PATH_BYTES = 4096
MAX_FILE_DEPTH = 16
SUBMISSION_RE = re.compile(r"g1q3-rca-s1-[0-9a-f]{64}\\Z")
ARTIFACT_SET_RE = re.compile(r"g1q3-rca-artifact-v1-[0-9a-f]{64}\\Z")
ENCODED_SEPARATOR_RE = re.compile(r"%(?:2f|5c)", re.IGNORECASE)
BYTE_RANGE_RE = re.compile(r"bytes=([0-9]{0,20})-([0-9]{0,20})\\Z")

def canonical_viewer_origin(value: str):
    return value

def contract(decoded, file_parts, before, self, viewer_origin, manifest_report_url, relative_report):
    assert len(decoded.encode("utf-8")) > MAX_PATH_BYTES
    assert len(file_parts) > MAX_FILE_DEPTH
    assert getattr(os, "O_DIRECTORY", 0)
    assert getattr(os, "O_NOFOLLOW", 0)
    assert not stat.S_ISREG(before.st_mode)
    assert self.headers.get("Range")
    self.send_header("Access-Control-Allow-Origin", viewer_origin)
    public_origin = canonical_viewer_origin(viewer_origin)
    assert manifest_report_url != f"{public_origin}/G1Q3_RCA/cases/{relative_report}"

def parse_byte_range(value, size): pass

class Handler:
    def do_HEAD(self): pass
    def do_GET(self): pass
    def do_OPTIONS(self): pass
    def do_POST(self): pass
    def list_directory(self, _path: str): pass

class BoundedHTTPServer(HTTPServer):
    def __init__(self, viewer_origin, max_workers=MAX_CONCURRENT_REQUESTS):
        ThreadPoolExecutor(max_workers=max_workers)
        threading.BoundedSemaphore(max_workers)
        self.viewer_origin = canonical_viewer_origin(viewer_origin)

def main(server):
    server.serve_forever(poll_interval=0.5)
''',
        encoding="utf-8",
    )
    server.chmod(0o755)
    unit = root / audit.REPORT_SERVICE_UNIT
    unit.parent.mkdir(parents=True)
    unit.write_text(
        f'''[Service]
Type=simple
Environment=PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1
EnvironmentFile={audit.REPORT_ENVIRONMENT_FILE}
ExecStart=/usr/bin/python3 -I -B {root}/{audit.REPORT_SERVER_ENTRYPOINT} --root /mnt/tmp --bind 0.0.0.0 --port 18081 --viewer-origin ${{{audit.REPORT_VIEWER_ORIGIN_VARIABLE}}}
WorkingDirectory=/
UMask=0077
NoNewPrivileges=true
PrivateDevices=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=read-only
ReadOnlyPaths=/mnt/tmp {root} {audit.REPORT_ENVIRONMENT_FILE}
InaccessiblePaths=/mnt/minieye/pdcl/department/perception_test_team
RestrictSUIDSGID=true
LockPersonality=true
MemoryDenyWriteExecute=true
RestrictAddressFamilies=AF_INET AF_INET6
''',
        encoding="utf-8",
    )
    lineage = root / audit.DELIVERY_LINEAGE_PATH
    lineage.parent.mkdir(parents=True, exist_ok=True)
    lineage.write_text(
        '''from pathlib import Path
DELIVERY_MANIFEST_SCHEMA = "delivery_manifest_v2"
TASK_ARTIFACT_ROOT = Path("/mnt/tmp")
TASK_ARTIFACT_CIFS_ROOT = "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp"
FORMAL_REPORT_ROOT = TASK_ARTIFACT_ROOT
REPORT_VIEWER_ORIGIN_ENV = "G1Q3_RCA_VIEWER_ORIGIN"
REPORT_VIEWER_ENV_PATH = Path("/home/mini/.config/g1q3-rca/report-http.env")
def build_report_vm_path(): pass
def build_report_cifs_path(): pass
def configured_publication_origin(): pass
def build_report_url():
    return f"{configured_publication_origin()}/G1Q3_RCA/cases/"
def contract(submission_key, parent, artifact_set_id):
    expected_root = Path("/mnt/tmp") / submission_key
    expected_artifact_root = TASK_ARTIFACT_ROOT / submission_key
    destination = parent / artifact_set_id
    destination = parent / f"{submission_key}.viz.mcap"
    return {
        "report_vm_path": "",
        "report_cifs_path": "",
        "report_url": "",
    }
''',
        encoding="utf-8",
    )
    _git(root, "add", ".")
    _git(root, "commit", "-qm", "fixture")
    return root, _git(root, "rev-parse", "HEAD"), _git(
        root, "rev-parse", "HEAD^{tree}"
    )


def test_audit_distinguishes_reachable_and_test_hits(tmp_path: Path):
    root, commit, tree = _repo(tmp_path)
    result = audit.audit(
        repo_root=root,
        entrypoint="pkg/entry.py",
        expected_commit=commit,
        expected_tree=tree,
    )

    assert result["ok"] is True
    assert result["reachable"]["module_count"] == 3
    assert result["reachable"]["blockers"] == []
    assert len(result["unreachable"]["test_hits"]) == 1
    assert result["production_path"]["remote_reader_contract_reachable"] is True


def test_audit_rejects_reachable_raw_mcap_service(tmp_path: Path):
    root, commit, tree = _repo(
        tmp_path, bad="RAW = '/work/build/bin/mcap_service'\n"
    )
    result = audit.audit(
        repo_root=root,
        entrypoint="pkg/entry.py",
        expected_commit=commit,
        expected_tree=tree,
    )

    assert result["ok"] is False
    assert result["reachable"]["blockers"][0]["kind"] == "raw_mcap_service"


def test_audit_rejects_unbounded_mcap_subprocess(tmp_path: Path):
    root, commit, tree = _repo(
        tmp_path,
        bad=(
            "import subprocess\n"
            "subprocess.run(['/work/bin/mcap_tool', '--read-only'])\n"
        ),
    )
    result = audit.audit(
        repo_root=root,
        entrypoint="pkg/entry.py",
        expected_commit=commit,
        expected_tree=tree,
    )

    kinds = [item["kind"] for item in result["reachable"]["blockers"]]
    assert "mcap_subprocess" in kinds


def test_audit_rejects_reachable_perception_output_root(tmp_path: Path):
    root, commit, tree = _repo(
        tmp_path,
        bad=(
            "from pathlib import Path\n"
            "Path('/mnt/minieye/pdcl/department/perception_test_team/"
            "G1Q3_RCA/cases/result.json').write_text('bad')\n"
        ),
    )
    result = audit.audit(
        repo_root=root,
        entrypoint="pkg/entry.py",
        expected_commit=commit,
        expected_tree=tree,
    )

    kinds = [item["kind"] for item in result["reachable"]["blockers"]]
    assert "forbidden_output_sink" in kinds
    assert result["production_path"]["forbidden_output_root_reachable"] is True
    assert result["production_path"]["perception_test_team_write_reachable"] is True


def test_audit_rejects_unclassified_perception_reference(tmp_path: Path):
    root, commit, tree = _repo(
        tmp_path,
        bad=(
            "LEGACY = '/mnt/minieye/pdcl/department/"
            "perception_test_team/G1Q3_RCA/cases'\n"
        ),
    )

    result = audit.audit(
        repo_root=root,
        entrypoint="pkg/entry.py",
        expected_commit=commit,
        expected_tree=tree,
    )

    kinds = [item["kind"] for item in result["reachable"]["blockers"]]
    assert "unclassified_forbidden_output_root_reference" in kinds
    assert result["production_path"]["perception_test_team_write_reachable"] is False


def test_main_returns_nonzero_when_audit_reports_blocker(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(audit, "audit", lambda **_kwargs: {"ok": False})

    assert audit.main(
        [
            "--repo-root",
            str(tmp_path),
            "--entrypoint",
            "pkg/entry.py",
            "--expected-commit",
            "a" * 40,
            "--expected-tree",
            "b" * 40,
        ]
    ) == 1


@pytest.mark.parametrize("cache_kind", ["tracked", "ignored"])
def test_audit_rejects_cache_files_even_when_git_status_is_clean(
    tmp_path: Path, cache_kind: str
):
    root, _commit, _tree = _repo(tmp_path)
    cache = root / "pkg/__pycache__/entry.cpython-38.pyc"
    cache.parent.mkdir()
    cache.write_bytes(b"cache")
    if cache_kind == "tracked":
        _git(root, "add", str(cache.relative_to(root)))
        _git(root, "commit", "-qm", "track cache")
    else:
        (root / ".gitignore").write_text("__pycache__/\n", encoding="utf-8")
        _git(root, "add", ".gitignore")
        _git(root, "commit", "-qm", "ignore cache")
    commit = _git(root, "rev-parse", "HEAD")
    tree = _git(root, "rev-parse", "HEAD^{tree}")

    with pytest.raises(
        audit.ClosureAuditError,
        match="fixed_cli_closure_filesystem_not_sealed",
    ):
        audit.audit(
            repo_root=root,
            entrypoint="pkg/entry.py",
            expected_commit=commit,
            expected_tree=tree,
        )


def test_audit_records_exact_task_output_and_zero_cache_seal(tmp_path: Path):
    root, commit, tree = _repo(tmp_path)
    result = audit.audit(
        repo_root=root,
        entrypoint="pkg/entry.py",
        expected_commit=commit,
        expected_tree=tree,
    )

    assert result["filesystem_seal"] == {
        "tracked_cache_path_count": 0,
        "ignored_path_count": 0,
        "untracked_path_count": 0,
        "filesystem_cache_path_count": 0,
        "pyc_file_count": 0,
        "pycache_directory_count": 0,
        "pytest_cache_directory_count": 0,
        "exact_source_seal": True,
    }
    assert result["production_path"]["output_root_contract"] == {
        "vm_task_root_pattern": "/mnt/tmp/<submission_key>/",
        "cifs_task_root_pattern": (
            "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/"
            "tmp/<submission_key>/"
        ),
        "generated_artifacts_must_remain_inside_task_root": True,
        "forbidden_output_roots": [
            "/mnt/minieye/pdcl/department/perception_test_team"
        ],
        "perception_test_team_input_only": True,
    }
    report_service = result["report_service"]
    assert report_service["root"] == "/mnt/tmp"
    assert report_service["route_prefix"] == "/G1Q3_RCA/cases/"
    assert report_service["entrypoint_git_mode"] == "100755"
    assert report_service["candidate_unit_git_mode"] == "100644"
    assert report_service["directory_listing"] is False
    assert report_service["path_traversal"] is False
    assert report_service["symlink_escape"] is False
    assert report_service["read_only"] is True
    remote_file = result["remote_file_transport"]
    assert remote_file["source_id"] == "remote-file"
    assert remote_file["viewer_query_parameter"] == "ds.url"
    assert remote_file["single_byte_range"] is True
    assert remote_file["suffix_byte_range"] is True
    assert remote_file["viewer_same_origin_https_proxy_required"] is True
    assert remote_file["manifest_html_same_origin_https_proxy_required"] is True
    assert remote_file["viewer_proxy_live_observed"] is False
    assert remote_file["release_blocked_until_viewer_proxy_proven"] is True
    delivery = result["delivery_manifest_contract"]
    assert delivery["schema_version"] == "delivery_manifest_v2"
    assert delivery["legacy_v1_deliverable"] is False
    assert delivery["perception_test_team_output"] is False
    assert delivery["report_vm_path_pattern"] == (
        "/mnt/tmp/<submission_key>/<artifact_set_id>/index.html"
    )
    assert delivery["report_url_pattern"] == (
        "<canonical_https_dns_origin>/G1Q3_RCA/cases/"
        "<submission_key>/<artifact_set_id>/index.html"
    )


def test_audit_rejects_broad_report_http_service(tmp_path: Path):
    root, _commit, _tree = _repo(tmp_path)
    unit = root / audit.REPORT_SERVICE_UNIT
    unit.write_text(
        unit.read_text(encoding="utf-8").replace(
            next(
                line
                for line in unit.read_text(encoding="utf-8").splitlines()
                if line.startswith("ExecStart=")
            ),
            "ExecStart=/usr/bin/python3 -m http.server 18081 --directory /mnt/tmp",
        ),
        encoding="utf-8",
    )
    _git(root, "add", str(unit.relative_to(root)))
    _git(root, "commit", "-qm", "weaken report service")

    with pytest.raises(
        audit.ClosureAuditError,
        match="fixed_cli_closure_report_service_invalid",
    ):
        audit.audit(
            repo_root=root,
            entrypoint="pkg/entry.py",
            expected_commit=_git(root, "rev-parse", "HEAD"),
            expected_tree=_git(root, "rev-parse", "HEAD^{tree}"),
        )


def test_output_is_no_clobber_and_mode_0600(tmp_path: Path, monkeypatch):
    root, commit, tree = _repo(tmp_path)
    monkeypatch.setenv("PNC_RCA_CLOSURE_AUDIT_TEST_MODE", "1")
    output = tmp_path / "mnt/tmp/result.json"
    result = audit.audit(
        repo_root=root,
        entrypoint="pkg/entry.py",
        expected_commit=commit,
        expected_tree=tree,
        output_path=output,
    )
    assert json.loads(output.read_text(encoding="utf-8"))["evidence_core_sha256"] == result[
        "evidence_core_sha256"
    ]
    assert output.stat().st_mode & 0o777 == 0o600
    with pytest.raises(audit.ClosureAuditError, match="fixed_cli_closure_output_invalid"):
        audit.audit(
            repo_root=root,
            entrypoint="pkg/entry.py",
            expected_commit=commit,
            expected_tree=tree,
            output_path=output,
        )
