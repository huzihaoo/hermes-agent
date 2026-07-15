from __future__ import annotations

import hashlib
import json
import os
import pwd
import stat
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import pnc_rca_release_gate as release_gate
from scripts import pnc_rca_release_prepare as prepare


NOW = datetime(2026, 7, 13, 2, 0, tzinfo=timezone.utc)
MACHINE_IDENTITY = {
    "source": "test_machine_identity",
    "sha256": "9" * 64,
}
VM_COMMIT = "2" * 40
VM_WORKER_COMMIT = "3" * 40
VM_TREE = "a" * 40
VM_WORKER_TREE = "b" * 40
LAUNCHD_SHA256 = "4" * 64
REAL_APPROVAL_BINDING_VALIDATOR = (
    release_gate.validate_release_prepare_approval_binding
)


def test_release_prepare_cli_help_runs_outside_repo(tmp_path):
    result = subprocess.run(
        [sys.executable, str(Path(prepare.__file__).resolve()), "--help"],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert "Prepare an immutable RCA production cutover plan" in result.stdout


class _Consumer:
    topic = "feishu-project-workfLow-event"
    group_id = "rca_root_cause_analysis_agent"
    initial_offsets = ((0, 101), (1, 202))
    policy = SimpleNamespace(policy_version="g1q3_issue_created_v1", to_dict=lambda: {})

    @staticmethod
    def public_dict():
        return {
            "topic": _Consumer.topic,
            "group_id": _Consumer.group_id,
            "initial_offsets": {"0": 101, "1": 202},
            "password_configured": True,
        }


class _Dispatcher:
    @staticmethod
    def public_dict():
        return {
            "dispatch_enabled": True,
            "allow_feishu_writeback": False,
            "data_access_mode": "remote_read",
        }


def _git(repo: Path, *arguments: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *arguments],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _init_repo(path: Path, files: dict[str, str]) -> str:
    path.mkdir()
    for relative, content in files.items():
        destination = path / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(content, encoding="utf-8")
    _git(path, "init", "-q")
    _git(path, "add", ".")
    _git(
        path,
        "-c",
        "user.name=RCA Release Prepare Test",
        "-c",
        "user.email=rca-release-prepare@example.invalid",
        "commit",
        "-q",
        "-m",
        "fixture",
    )
    return _git(path, "rev-parse", "HEAD")


def _write_owner_json(path: Path, body: dict) -> None:
    path.write_text(
        json.dumps(body, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        + "\n",
        encoding="utf-8",
    )
    path.chmod(0o600)


def _cutover() -> release_gate.CutoverConfig:
    return release_gate.CutoverConfig(
        legacy_auto_execution_disabled=True,
        legacy_daily_quota=0,
        legacy_governance_download_enabled=False,
        delivery_collector_enabled=True,
        delivery_dispatcher_enabled=True,
        issue_capture_enabled=False,
        issue_capture_root_configured=False,
        manual_intake_enabled=True,
        manual_chat_ids=(),
        manual_operator_enabled=False,
        manual_operator_user_ids=(),
        manual_operator_rate_limit=None,
        manual_operator_rate_window_seconds=None,
        activation_required=True,
    )


def _provenance(host: Path, workspace: Path) -> dict:
    workspace_provenance = release_gate._local_git_provenance(
        workspace, component="workspace"
    )
    return {
        "schema_version": release_gate.BUILD_PROVENANCE_SCHEMA_VERSION,
        "host": {
            "source": "local_git",
            "repo_root": str(host.resolve()),
            "commit": _git(host, "rev-parse", "HEAD"),
            "tree_clean": True,
            "status_sha256": release_gate.EMPTY_GIT_STATUS_SHA256,
            "stable": True,
        },
        "workspace": workspace_provenance,
        "vm": {
            "source": "ssh-mini-agent",
            "repo_root": "/srv/rca-vm-candidate",
            "commit": VM_COMMIT,
            "tree_clean": True,
            "status_sha256": release_gate.EMPTY_GIT_STATUS_SHA256,
            "stable": True,
            "tree": VM_TREE,
            "entrypoint_path": (
                "/srv/rca-vm-candidate/api/g1q3_rca/scripts/run_rca_service_request.py"
            ),
            "entrypoint_sha256": "5" * 64,
            "entrypoint_committed_sha256": "5" * 64,
            "entrypoint_git_mode": "100644",
            "entrypoint_blob": "c" * 40,
        },
        "vm_worker": {
            "source": "ssh-mini-agent",
            "repo_root": "/srv/rca-worker-candidate",
            "commit": VM_WORKER_COMMIT,
            "tree_clean": True,
            "status_sha256": release_gate.EMPTY_GIT_STATUS_SHA256,
            "stable": True,
            "tree": VM_WORKER_TREE,
            "entrypoint_path": "/srv/rca-worker-candidate/vm_coding_worker_v2.py",
            "entrypoint_sha256": "6" * 64,
            "entrypoint_committed_sha256": "6" * 64,
            "entrypoint_git_mode": "100755",
            "entrypoint_blob": "d" * 40,
        },
        "external_dependencies": {
            name: {
                "schema_version": (
                    release_gate.EXTERNAL_DEPENDENCY_PROVENANCE_SCHEMA_VERSION
                ),
                "source": "local_lstat",
                "path": str(contract["path"]),
                "realpath": str(contract["path"]),
                "owner_uid": os.getuid(),
                "mode": contract["mode"],
                "regular_file": True,
                "symlink": False,
                "link_count": 1,
                "sha256": ("7" if name == "ssh_mini_agent" else "8") * 64,
                "stable": True,
            }
            for name, contract in release_gate.EXTERNAL_RELEASE_DEPENDENCIES.items()
        },
    }


def _fixture_runtime_descriptors(host: Path) -> dict[str, dict]:
    excluded = set(prepare.EXPECTED_CANDIDATE_PLISTS) | {"pyproject.toml", "uv.lock"}
    descriptors = {}
    for relative in _git(host, "ls-tree", "-r", "--name-only", "HEAD").splitlines():
        if relative in excluded:
            continue
        raw = (host / relative).read_bytes()
        line = _git(host, "ls-tree", "HEAD", "--", relative)
        prefix, tracked = line.split("\t", 1)
        mode, kind, blob = prefix.split(" ", 2)
        assert tracked == relative and kind == "blob"
        descriptors[relative] = {
            "path": relative,
            "sha256": hashlib.sha256(raw).hexdigest(),
            "size_bytes": len(raw),
            "mode": "0755" if mode == "100755" else "0644",
            "source_kind": "regular",
            "git_blob": blob,
        }
    return descriptors


def _runtime_detail(host: Path, stage: Path, *, host_commit: str) -> dict:
    candidate_plists = {
        filename: {
            "label": prepare.runtime_stage.CANDIDATE_PLISTS[filename][0],
            "source_sha256": hashlib.sha256((host / filename).read_bytes()).hexdigest(),
            "staging_sha256": hashlib.sha256((stage / filename).read_bytes()).hexdigest(),
            "canonical_sha256": hashlib.sha256((host / filename).read_bytes()).hexdigest(),
            "canonical_body_sha256": "e" * 64,
        }
        for filename in prepare.EXPECTED_CANDIDATE_PLISTS
    }
    runtime_files = {
        relative: descriptor["sha256"]
        for relative, descriptor in _fixture_runtime_descriptors(host).items()
    }
    interpreter_path = stage.absolute() / ".venv" / "bin" / "python"
    stage_stat = stage.lstat()
    venv_stat = (stage / ".venv").lstat()
    interpreter_stat = interpreter_path.lstat()
    interpreter_sha256 = hashlib.sha256(interpreter_path.read_bytes()).hexdigest()
    canonical_interpreter = (
        release_gate.CANONICAL_FUTURE_RUNTIME_ROOT / ".venv" / "bin" / "python"
    )
    gateway_origins: dict[str, str] = {}
    render_manifest = {
        "schema_version": release_gate.FUTURE_RUNTIME_RENDER_MANIFEST_SCHEMA_VERSION,
        "source_repo_root": str(host.resolve()),
        "source_commit": host_commit,
        "staging_root": str(stage.absolute()),
        "canonical_live_root": str(release_gate.CANONICAL_FUTURE_RUNTIME_ROOT),
        "runtime_file_sha256": runtime_files,
        "runtime_files_sha256": prepare._sha256_json(runtime_files),
        "interpreter": {
            "staging_path": str(interpreter_path),
            "canonical_path": str(canonical_interpreter),
            "sha256": interpreter_sha256,
        },
        "dependencies": {},
        "candidate_plists": candidate_plists,
        "canonical_launchd_config_sha256": "e" * 64,
        "canonical_runtime_config_sha256": LAUNCHD_SHA256,
        "gateway_runtime": {
            "sys_executable": str(canonical_interpreter),
            "sys_executable_sha256": interpreter_sha256,
            "process_executable": str(canonical_interpreter),
            "process_executable_sha256": interpreter_sha256,
            "module_origins": gateway_origins,
            "module_origins_sha256": prepare._sha256_json(gateway_origins),
            "dependency_versions": dict(
                release_gate.EXPECTED_GATEWAY_RUNTIME_DEPENDENCY_VERSIONS
            ),
            "repo_module_count": 4,
            "venv_dependency_count": 2,
        },
    }
    render_manifest_sha256 = prepare._sha256_json(render_manifest)
    return {
        "launchd_config_sha256": LAUNCHD_SHA256,
        "candidate_plist_sha256": {
            release_gate.CANDIDATE_SERVICES[filename][0]: hashlib.sha256(
                (stage / filename).read_bytes()
            ).hexdigest()
            for filename in release_gate.CANDIDATE_SERVICES
        },
        "runtime_stage_identity": {
            "root": {
                "path": str(stage.absolute()),
                "device": stage_stat.st_dev,
                "inode": stage_stat.st_ino,
                "owner_uid": os.geteuid(),
                "mode": stat.S_IMODE(stage_stat.st_mode),
            },
            "venv": {
                "path": str(stage.absolute() / ".venv"),
                "device": venv_stat.st_dev,
                "inode": venv_stat.st_ino,
                "owner_uid": os.geteuid(),
                "mode": stat.S_IMODE(venv_stat.st_mode),
            },
            "interpreter": {
                "path": str(interpreter_path),
                "sha256": interpreter_sha256,
                "device": interpreter_stat.st_dev,
                "inode": interpreter_stat.st_ino,
                "owner_uid": os.geteuid(),
                "mode": stat.S_IMODE(interpreter_stat.st_mode),
            },
        },
        "future_runtime_projection": {
            "schema_version": release_gate.FUTURE_RUNTIME_PROJECTION_SCHEMA_VERSION,
            "ok": True,
            "source_commit": host_commit,
            "staging_root": str(stage.absolute()),
            "canonical_live_root": str(release_gate.CANONICAL_FUTURE_RUNTIME_ROOT),
            "render_manifest_sha256": render_manifest_sha256,
        },
        "render_manifest": render_manifest,
        "render_manifest_sha256": render_manifest_sha256,
    }


def _release_bom(
    *,
    provenance: dict,
    runtime_config_sha256: str,
    workspace_component: dict,
    workspace_runtime_binding: dict,
    future_runtime_binding: dict,
    critical_files: dict[str, str],
) -> dict:
    components = {}
    for component in ("host", "vm", "vm_worker"):
        observed = provenance[component]
        projection = {
            key: observed[key]
            for key in (
                "source",
                "repo_root",
                "commit",
                "tree_clean",
                "status_sha256",
            )
        }
        if component in {"vm", "vm_worker"}:
            projection.update({
                key: observed[key]
                for key in (
                    "tree",
                    "entrypoint_path",
                    "entrypoint_sha256",
                    "entrypoint_committed_sha256",
                    "entrypoint_git_mode",
                    "entrypoint_blob",
                )
            })
        components[component] = projection
    components["workspace"] = workspace_component
    return {
        "schema_version": release_gate.RELEASE_BOM_SCHEMA_VERSION,
        "components": components,
        "workspace_runtime": workspace_runtime_binding,
        "future_runtime": future_runtime_binding,
        "runtime_config_sha256": runtime_config_sha256,
        "launchd_config_sha256": LAUNCHD_SHA256,
        "critical_files_sha256": prepare._sha256_json(critical_files),
        "external_dependencies": provenance["external_dependencies"],
    }


def _approval(
    *,
    release_id: str,
    release_bom_sha256: str,
    workspace_runtime_sha256: str,
    future_runtime_sha256: str,
    runtime_config_sha256: str,
    t0_sha256: str,
    rollback_sha256: str,
    rollback_window_seconds: int,
) -> dict:
    return {
        "schema_version": prepare.RELEASE_APPROVAL_SCHEMA_VERSION,
        "release_id": release_id,
        "decision": prepare.APPROVAL_DECISION,
        "created_at": NOW.isoformat(),
        "expires_at": (NOW + timedelta(hours=1)).isoformat(),
        "nonce": "release-approval-nonce-0001",
        "action_set": list(prepare.PRODUCTION_ACTION_SET),
        "action_set_sha256": prepare._sha256_json(list(prepare.PRODUCTION_ACTION_SET)),
        "approval_request_sha256": "0" * 64,
        "release_bom_sha256": release_bom_sha256,
        "workspace_runtime_sha256": workspace_runtime_sha256,
        "future_runtime_sha256": future_runtime_sha256,
        "runtime_config_sha256": runtime_config_sha256,
        "t0_sha256": t0_sha256,
        "rollback_config_sha256": rollback_sha256,
        "rollback_window_seconds": rollback_window_seconds,
        "identity": {
            "schema_version": prepare.RELEASE_APPROVAL_IDENTITY_SCHEMA_VERSION,
            "method": prepare.APPROVAL_IDENTITY_METHOD,
            "uid": os.geteuid(),
            "username": pwd.getpwuid(os.geteuid()).pw_name,
            "machine_identity_source": MACHINE_IDENTITY["source"],
            "machine_identity_sha256": MACHINE_IDENTITY["sha256"],
        },
    }


@pytest.fixture
def prepared_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    plist_files = {
        filename: f"candidate plist: {filename}\n"
        for filename in prepare.EXPECTED_CANDIDATE_PLISTS
    }
    runtime_files = {
        relative: f"# runtime fixture: {relative}\n"
        for relative in release_gate.FUTURE_RUNTIME_RELATIVE_FILES
    }
    host = tmp_path / "host"
    host_commit = _init_repo(
        host,
        {
            **plist_files,
            **runtime_files,
            "critical.txt": "critical source\n",
            "scripts/pnc_rca_canary_collector.py": "# canary collector\n",
            "pyproject.toml": (
                "[project]\n"
                'name = "release-prepare-fixture"\n'
                'version = "0.0.0"\n'
                "[project.optional-dependencies]\n"
                'kafka = ["kafka-python==3.0.7", "python-snappy==0.7.3", '
                '"tinycss2==1.2.1"]\n'
                'feishu = ["lark-oapi==1.5.3"]\n'
            ),
        },
    )
    workspace = tmp_path / "workspace"
    workspace_commit = _init_repo(
        workspace,
        {
            "bin/create_task_v2.py": "print('create task')\n",
            "bin/shared_state_v2.py": "print('shared state')\n",
            "bin/shared_state_fields.py": "print('shared fields')\n",
            "notes/unscoped.txt": "unscoped but committed\n",
        },
    )
    runtime_stage_root = tmp_path / "runtime-stage"
    runtime_stage_root.mkdir(mode=0o700)
    runtime_stage_venv_bin = runtime_stage_root / ".venv" / "bin"
    runtime_stage_venv_bin.mkdir(parents=True, mode=0o755)
    runtime_stage_interpreter = runtime_stage_venv_bin / "python"
    runtime_stage_interpreter.write_bytes(b"fixture-python\n")
    runtime_stage_interpreter.chmod(0o755)
    for filename in prepare.EXPECTED_CANDIDATE_PLISTS:
        (runtime_stage_root / filename).write_bytes((host / filename).read_bytes())
    runtime_stage_manifest_path = (
        runtime_stage_root / prepare.runtime_stage.MANIFEST_FILENAME
    )
    runtime_file_descriptors = _fixture_runtime_descriptors(host)
    runtime_file_sha256 = {
        relative: descriptor["sha256"]
        for relative, descriptor in runtime_file_descriptors.items()
    }
    runtime_stage_content = {
        "source": {"runtime_files": runtime_file_descriptors},
    }
    runtime_stage_manifest = {
        "schema_version": prepare.runtime_stage.MANIFEST_SCHEMA_VERSION,
        "complete": True,
        "production_effects_executed": False,
        "live_install_performed": False,
        "staging_root": str(runtime_stage_root.absolute()),
        "plan_sha256": "1" * 64,
        "content": runtime_stage_content,
        "content_sha256": prepare._sha256_json(runtime_stage_content),
        "future_canonical_projection": {
            "canonical_live_root": str(release_gate.CANONICAL_FUTURE_RUNTIME_ROOT),
            "source_commit": host_commit,
            "source_tree": _git(host, "rev-parse", "HEAD^{tree}"),
            "candidate_plist_sha256": {
                filename: hashlib.sha256((host / filename).read_bytes()).hexdigest()
                for filename in prepare.EXPECTED_CANDIDATE_PLISTS
            },
            "runtime_files_sha256": prepare._sha256_json(runtime_file_sha256),
        },
    }
    _write_owner_json(runtime_stage_manifest_path, runtime_stage_manifest)
    workspace_runtime_root = tmp_path / "workspace-runtime-stage"
    workspace_runtime_root.mkdir(mode=0o700)
    workspace_runtime_bin = workspace_runtime_root / "bin"
    workspace_runtime_bin.mkdir(mode=0o700)
    workspace_runtime_descriptors = {}
    for relative in prepare.workspace_runtime.WORKSPACE_RUNTIME_FILES:
        raw = (workspace / relative).read_bytes()
        destination = workspace_runtime_root / relative
        destination.write_bytes(raw)
        destination.chmod(
            prepare.workspace_runtime.WORKSPACE_RUNTIME_FILE_MODES[relative]
        )
        line = _git(workspace, "ls-tree", "HEAD", "--", relative)
        prefix, tracked = line.split("\t", 1)
        _mode, kind, blob = prefix.split(" ", 2)
        assert tracked == relative and kind == "blob"
        workspace_runtime_descriptors[relative] = (
            prepare.workspace_runtime.workspace_runtime_descriptor(
                path=relative,
                raw=raw,
                git_blob_oid=blob,
            )
        )
    workspace_runtime_manifest_path = (
        workspace_runtime_root
        / prepare.workspace_runtime.WORKSPACE_RUNTIME_MANIFEST_NAME
    )
    _write_owner_json(
        workspace_runtime_manifest_path,
        prepare.workspace_runtime.build_workspace_runtime_manifest(
            source_commit=workspace_commit,
            files=workspace_runtime_descriptors,
        ),
    )
    host_contract = tmp_path / "host-contract.py"
    vm_contract = tmp_path / "vm-contract.py"
    host_contract.write_text("contract\n")
    vm_contract.write_text("contract\n")
    env_file = tmp_path / "candidate.env"
    env_file.write_text(
        "HERMES_RCA_KAFKA_PASSWORD=super-secret-value\n"
        "HERMES_RCA_KAFKA_TOPIC=feishu-project-workfLow-event\n"
    )
    env_file.chmod(0o600)
    rollback = {
        "schema_version": prepare.ROLLBACK_CONFIG_SCHEMA_VERSION,
        "owner": "release-owner",
        "procedure": "restore the reviewed predecessor configuration",
        "max_restore_seconds": 300,
        "rollback_window_seconds": 3600,
    }
    rollback_path = tmp_path / "rollback.json"
    _write_owner_json(rollback_path, rollback)

    consumer = _Consumer()
    dispatcher = _Dispatcher()
    cutover = _cutover()
    monkeypatch.setattr(
        release_gate,
        "load_redacted_configs",
        lambda *_args, **_kwargs: (consumer, dispatcher),
    )
    monkeypatch.setattr(
        release_gate,
        "load_cutover_config",
        lambda *_args, **_kwargs: cutover,
    )
    monkeypatch.setattr(
        release_gate,
        "_required_critical_files",
        lambda _root: {"critical.txt", "scripts/pnc_rca_canary_collector.py"},
    )
    monkeypatch.setattr(
        release_gate.runtime_stage,
        "validate_staged_runtime",
        lambda _root: json.loads(runtime_stage_manifest_path.read_text()),
    )

    def validate_approval_binding(**kwargs):
        return {
            "schema_version": (
                prepare.RELEASE_APPROVAL_BINDING_VALIDATION_SCHEMA_VERSION
            ),
            "ok": True,
            "approval_request_sha256": kwargs["approval_request_sha256"],
            "approval_receipt_sha256": kwargs["approval_receipt_sha256"],
            "cutover_plan_schema_version": release_gate.CUTOVER_PLAN_SCHEMA_VERSION,
            "final_manifest_schema_version": (
                prepare.RELEASE_PREPARE_MANIFEST_SCHEMA_VERSION
            ),
        }

    monkeypatch.setattr(
        release_gate,
        "validate_release_prepare_approval_binding",
        validate_approval_binding,
        raising=False,
    )
    provenance = _provenance(host, workspace)
    workspace_component = prepare._workspace_component_projection(provenance)
    workspace_runtime_identity = (
        prepare.workspace_runtime.validate_staged_workspace_runtime(
            workspace_runtime_root
        ).to_dict()
    )
    runtime = _runtime_detail(host, runtime_stage_root, host_commit=host_commit)
    public_config = release_gate._public_config(consumer, dispatcher, cutover)
    runtime_config_sha256 = release_gate._sha256_json(public_config)
    critical_files = {
        relative: hashlib.sha256((host / relative).read_bytes()).hexdigest()
        for relative in ("critical.txt", "scripts/pnc_rca_canary_collector.py")
    }
    release_bom = _release_bom(
        provenance=provenance,
        runtime_config_sha256=runtime_config_sha256,
        workspace_component=workspace_component,
        workspace_runtime_binding=dict(
            prepare._workspace_runtime_release_binding(workspace_runtime_identity)
        ),
        future_runtime_binding=dict(
            prepare._future_runtime_release_binding(
                runtime_stage_manifest_identity={
                    "schema_version": runtime_stage_manifest["schema_version"],
                    "staging_root": str(runtime_stage_root.absolute()),
                    "manifest_path": str(runtime_stage_manifest_path.absolute()),
                    "manifest_sha256": hashlib.sha256(
                        runtime_stage_manifest_path.read_bytes()
                    ).hexdigest(),
                    "plan_sha256": runtime_stage_manifest["plan_sha256"],
                    "content_sha256": runtime_stage_manifest["content_sha256"],
                    "source_commit": host_commit,
                    "source_tree": runtime_stage_manifest[
                        "future_canonical_projection"
                    ]["source_tree"],
                    "canonical_live_root": str(
                        release_gate.CANONICAL_FUTURE_RUNTIME_ROOT
                    ),
                    "candidate_plist_sha256": runtime_stage_manifest[
                        "future_canonical_projection"
                    ]["candidate_plist_sha256"],
                    "runtime_file_descriptors": runtime_file_descriptors,
                    "runtime_files_sha256": runtime_stage_manifest[
                        "future_canonical_projection"
                    ]["runtime_files_sha256"],
                },
                runtime_detail=runtime,
            )
        ),
        critical_files=critical_files,
    )
    t0 = {
        "schema_version": prepare.T0_BINDING_SCHEMA_VERSION,
        "topic": consumer.topic,
        "group_id": consumer.group_id,
        "initial_offsets": {"0": 101, "1": 202},
    }
    release_id = "rca-production-20260713-0001"
    approval_path = tmp_path / "approval.json"
    approval = _approval(
        release_id=release_id,
        release_bom_sha256=prepare._sha256_json(release_bom),
        workspace_runtime_sha256=prepare._sha256_json(
            release_bom["workspace_runtime"]
        ),
        future_runtime_sha256=prepare._sha256_json(release_bom["future_runtime"]),
        runtime_config_sha256=runtime_config_sha256,
        t0_sha256=prepare._sha256_json(t0),
        rollback_sha256=prepare._sha256_json(rollback),
        rollback_window_seconds=3600,
    )
    _write_owner_json(approval_path, approval)
    inputs = prepare.PrepareInputs(
        env_file=env_file,
        host_candidate=host,
        workspace_candidate=workspace,
        runtime_staging_root=runtime_stage_root,
        runtime_stage_manifest=runtime_stage_manifest_path,
        future_live_root=release_gate.CANONICAL_FUTURE_RUNTIME_ROOT,
        workspace_runtime_root=workspace_runtime_root,
        workspace_runtime_manifest=workspace_runtime_manifest_path,
        vm_candidate="/srv/rca-vm-candidate",
        vm_worker_candidate="/srv/rca-worker-candidate",
        candidate_plists=tuple(
            runtime_stage_root / name for name in prepare.EXPECTED_CANDIDATE_PLISTS
        ),
        release_id=release_id,
        approval_receipt=approval_path,
        rollback_config=rollback_path,
        run_root=tmp_path / "release-run",
        host_contract=host_contract,
        vm_contract=vm_contract,
    )
    return SimpleNamespace(
        inputs=inputs,
        host=host,
        workspace=workspace,
        host_commit=host_commit,
        workspace_commit=workspace_commit,
        provenance=provenance,
        runtime=runtime,
        approval_path=approval_path,
        approval=approval,
        rollback_path=rollback_path,
        workspace_component=workspace_component,
        runtime_stage_manifest=runtime_stage_manifest,
        workspace_runtime_identity=workspace_runtime_identity,
    )


def _call(fixture, *, phase: str, calls: dict[str, int]):
    def provenance(_settings):
        calls["provenance"] += 1
        return fixture.provenance

    def runtime_projector(_host, _stage, **_kwargs):
        calls["runtime"] += 1
        return fixture.runtime

    def stage_validator(_root):
        return fixture.runtime_stage_manifest

    def workspace_validator(_root):
        return SimpleNamespace(to_dict=lambda: fixture.workspace_runtime_identity)

    return prepare.prepare_release(
        fixture.inputs,
        phase=phase,
        now=NOW,
        machine_identity_observer=lambda: MACHINE_IDENTITY,
        provenance_verifier=provenance,
        runtime_projector=runtime_projector,
        runtime_stage_validator=stage_validator,
        workspace_runtime_validator=workspace_validator,
    )


def _fixture_stage_validator(fixture):
    return lambda _root: fixture.runtime_stage_manifest


def _fixture_workspace_validator(fixture):
    return lambda _root: SimpleNamespace(
        to_dict=lambda: fixture.workspace_runtime_identity
    )


def _bind_approval_request(fixture) -> str:
    request_path = fixture.inputs.run_root / prepare.APPROVAL_REQUEST_FILENAME
    request_sha256 = hashlib.sha256(request_path.read_bytes()).hexdigest()
    body = json.loads(fixture.approval_path.read_text())
    body["approval_request_sha256"] = request_sha256
    _write_owner_json(fixture.approval_path, body)
    return request_sha256


def _prepare(fixture):
    calls = {"provenance": 0, "runtime": 0}
    _call(fixture, phase="request", calls=calls)
    _bind_approval_request(fixture)
    result = _call(fixture, phase="finalize", calls=calls)
    return result, calls


def test_prepare_emits_gate_valid_plan_without_live_side_effects(prepared_fixture):
    result, calls = _prepare(prepared_fixture)

    assert result.resumed is True
    assert result.phase == "finalize"
    assert calls == {"provenance": 4, "runtime": 4}
    assert stat.S_IMODE(result.run_root.stat().st_mode) == 0o700
    assert set(path.name for path in result.run_root.iterdir()) == {
        prepare.RUN_LOCK_FILENAME,
        prepare.RUN_IDENTITY_FILENAME,
        prepare.APPROVAL_REQUEST_FILENAME,
        *prepare.RUN_ARTIFACT_ORDER,
        prepare.RUN_MANIFEST_FILENAME,
    }
    for filename in (
        prepare.RUN_IDENTITY_FILENAME,
        prepare.APPROVAL_REQUEST_FILENAME,
        *prepare.RUN_ARTIFACT_ORDER,
        prepare.RUN_MANIFEST_FILENAME,
    ):
        assert stat.S_IMODE((result.run_root / filename).stat().st_mode) == 0o600
    plan = json.loads((result.run_root / "release_plan.json").read_text())
    cutover = json.loads((result.run_root / "cutover_plan.json").read_text())
    manifest = json.loads((result.run_root / prepare.RUN_MANIFEST_FILENAME).read_text())
    assert plan["mode"] == "plan_only"
    assert plan["executed"] is False
    assert plan["action_set"] == list(prepare.PRODUCTION_ACTION_SET)
    assert plan["side_effect_contract"] == {
        "live_files_written": False,
        "launchctl_invoked": False,
        "kafka_consumer_created": False,
        "kafka_offsets_mutated": False,
        "feishu_writes": False,
        "vm_files_written": False,
        "output_scope": "unique_owner_only_run_root",
    }
    assert cutover["approved"] is True
    assert plan["approval"]["decision"] == prepare.APPROVAL_DECISION
    assert "approved" not in prepared_fixture.approval
    assert manifest["complete"] is True
    assert manifest["plan_only"] is True
    all_output = b"".join(
        (result.run_root / name).read_bytes()
        for name in (
            prepare.RUN_IDENTITY_FILENAME,
            prepare.APPROVAL_REQUEST_FILENAME,
            *prepare.RUN_ARTIFACT_ORDER,
            prepare.RUN_MANIFEST_FILENAME,
        )
    )
    assert b"super-secret-value" not in all_output


def test_prepare_resume_is_byte_stable_and_no_clobber(prepared_fixture):
    first, _calls = _prepare(prepared_fixture)
    before = {
        path.name: path.read_bytes()
        for path in first.run_root.iterdir()
        if path.name != prepare.RUN_LOCK_FILENAME
    }

    second, calls = _prepare(prepared_fixture)

    after = {
        path.name: path.read_bytes()
        for path in second.run_root.iterdir()
        if path.name != prepare.RUN_LOCK_FILENAME
    }
    assert second.resumed is True
    assert calls == {"provenance": 4, "runtime": 4}
    assert after == before


@pytest.mark.parametrize(
    ("mutation", "code"),
    [
        (
            lambda body: body.update(release_id="other-release"),
            "release_approval_release_id_mismatch",
        ),
        (
            lambda body: body.update(action_set=["confirm_rca_production"]),
            "release_approval_action_set_mismatch",
        ),
        (
            lambda body: body.update(release_bom_sha256="0" * 64),
            "release_approval_release_bom_sha256_mismatch",
        ),
        (
            lambda body: body.update(runtime_config_sha256="0" * 64),
            "release_approval_runtime_config_sha256_mismatch",
        ),
        (
            lambda body: body.update(workspace_runtime_sha256="0" * 64),
            "release_approval_workspace_runtime_sha256_mismatch",
        ),
        (
            lambda body: body.update(future_runtime_sha256="0" * 64),
            "release_approval_future_runtime_sha256_mismatch",
        ),
        (
            lambda body: body.update(t0_sha256="0" * 64),
            "release_approval_t0_sha256_mismatch",
        ),
        (
            lambda body: body.update(rollback_config_sha256="0" * 64),
            "release_approval_rollback_config_sha256_mismatch",
        ),
        (
            lambda body: body.update(rollback_window_seconds=7200),
            "release_approval_rollback_window_mismatch",
        ),
        (
            lambda body: body["identity"].update(machine_identity_sha256="8" * 64),
            "release_approval_identity_mismatch",
        ),
        (
            lambda body: body.update(approved=True),
            "release_approval_shape_invalid",
        ),
    ],
)
def test_approval_is_exact_bound_and_never_self_asserted(
    prepared_fixture, mutation, code
):
    body = json.loads(prepared_fixture.approval_path.read_text())
    mutation(body)
    _write_owner_json(prepared_fixture.approval_path, body)

    with pytest.raises(prepare.ReleasePrepareError) as error:
        _prepare(prepared_fixture)

    assert error.value.code == code
    assert not prepared_fixture.inputs.run_root.joinpath("cutover_plan.json").exists()


@pytest.mark.parametrize("mode", [0o644, 0o400])
def test_approval_requires_kernel_owned_owner_only_file(prepared_fixture, mode):
    calls = {"provenance": 0, "runtime": 0}
    _call(prepared_fixture, phase="request", calls=calls)
    _bind_approval_request(prepared_fixture)
    prepared_fixture.approval_path.chmod(mode)

    with pytest.raises(prepare.ReleasePrepareError) as error:
        _call(prepared_fixture, phase="finalize", calls=calls)

    assert error.value.code == "release_approval_not_owner_only"


def test_request_phase_needs_no_approval_and_is_explicitly_non_effecting(
    prepared_fixture,
):
    prepared_fixture.inputs = prepare.PrepareInputs(**{
        **prepared_fixture.inputs.__dict__,
        "approval_receipt": None,
    })
    calls = {"provenance": 0, "runtime": 0}

    result = _call(prepared_fixture, phase="request", calls=calls)

    assert result.phase == "request"
    assert result.resumed is False
    assert result.manifest["production_effects_executed"] is False
    assert result.manifest["approval_required_for_finalize"] is True
    assert result.manifest["approval_identity_requirement"] == {
        "schema_version": prepare.RELEASE_APPROVAL_IDENTITY_SCHEMA_VERSION,
        "method": prepare.APPROVAL_IDENTITY_METHOD,
        "uid": os.geteuid(),
        "username": pwd.getpwuid(os.geteuid()).pw_name,
        "machine_identity_source": MACHINE_IDENTITY["source"],
        "machine_identity_sha256": MACHINE_IDENTITY["sha256"],
    }
    assert "approved" not in result.manifest
    bindings = result.manifest["bindings"]
    assert bindings["workspace_runtime_sha256"] == prepare._sha256_json(
        bindings["release_bom"]["workspace_runtime"]
    )
    assert bindings["future_runtime_sha256"] == prepare._sha256_json(
        bindings["release_bom"]["future_runtime"]
    )
    assert set(result.manifest["candidate_plist_sha256"]) == set(
        release_gate.FUTURE_RUNTIME_PLIST_FILENAMES
    )
    assert calls == {"provenance": 2, "runtime": 2}
    assert set(path.name for path in result.run_root.iterdir()) == {
        prepare.RUN_LOCK_FILENAME,
        prepare.RUN_IDENTITY_FILENAME,
        prepare.APPROVAL_REQUEST_FILENAME,
    }


def test_approval_expiry_is_enforced(prepared_fixture):
    body = json.loads(prepared_fixture.approval_path.read_text())
    body["created_at"] = (NOW - timedelta(hours=1)).isoformat()
    body["expires_at"] = (NOW - timedelta(seconds=1)).isoformat()
    _write_owner_json(prepared_fixture.approval_path, body)

    with pytest.raises(prepare.ReleasePrepareError) as error:
        _prepare(prepared_fixture)

    assert error.value.code == "release_approval_expired"


def test_finalize_rejects_approval_request_hash_mismatch(prepared_fixture):
    calls = {"provenance": 0, "runtime": 0}
    _call(prepared_fixture, phase="request", calls=calls)
    _bind_approval_request(prepared_fixture)
    body = json.loads(prepared_fixture.approval_path.read_text())
    body["approval_request_sha256"] = "f" * 64
    _write_owner_json(prepared_fixture.approval_path, body)

    with pytest.raises(prepare.ReleasePrepareError) as error:
        _call(prepared_fixture, phase="finalize", calls=calls)

    assert error.value.code == "release_approval_request_hash_mismatch"


def test_finalize_rejects_tampered_approval_request(prepared_fixture):
    calls = {"provenance": 0, "runtime": 0}
    _call(prepared_fixture, phase="request", calls=calls)
    _bind_approval_request(prepared_fixture)
    request_path = prepared_fixture.inputs.run_root / prepare.APPROVAL_REQUEST_FILENAME
    body = json.loads(request_path.read_text())
    body["production_effects_executed"] = True
    _write_owner_json(request_path, body)

    with pytest.raises(prepare.ReleasePrepareError) as error:
        _call(prepared_fixture, phase="finalize", calls=calls)

    assert error.value.code == "release_approval_request_drift"


def test_finalize_requires_prior_request_without_creating_run_root(prepared_fixture):
    calls = {"provenance": 0, "runtime": 0}

    with pytest.raises(prepare.ReleasePrepareError) as error:
        _call(prepared_fixture, phase="finalize", calls=calls)

    assert error.value.code == "release_approval_request_required"
    assert not prepared_fixture.inputs.run_root.exists()
    assert calls == {"provenance": 0, "runtime": 0}


def test_finalize_requires_approval_receipt(prepared_fixture):
    calls = {"provenance": 0, "runtime": 0}
    prepared_fixture.inputs = prepare.PrepareInputs(**{
        **prepared_fixture.inputs.__dict__,
        "approval_receipt": None,
    })
    _call(prepared_fixture, phase="request", calls=calls)

    with pytest.raises(prepare.ReleasePrepareError) as error:
        _call(prepared_fixture, phase="finalize", calls=calls)

    assert error.value.code == "release_approval_receipt_required"


def test_finalize_is_locked_until_release_gate_supports_authenticated_binding(
    prepared_fixture, monkeypatch
):
    calls = {"provenance": 0, "runtime": 0}
    _call(prepared_fixture, phase="request", calls=calls)
    _bind_approval_request(prepared_fixture)
    monkeypatch.delattr(
        release_gate,
        "validate_release_prepare_approval_binding",
        raising=False,
    )

    with pytest.raises(prepare.ReleasePrepareError) as error:
        _call(prepared_fixture, phase="finalize", calls=calls)

    assert error.value.code == (
        "release_gate_authenticated_approval_binding_unsupported"
    )
    assert not prepared_fixture.inputs.run_root.joinpath("cutover_plan.json").exists()
    assert not prepared_fixture.inputs.run_root.joinpath("build_manifest.json").exists()
    assert not prepared_fixture.inputs.run_root.joinpath("release_plan.json").exists()


def test_finalize_uses_current_gate_approval_validator(
    prepared_fixture, monkeypatch
):
    monkeypatch.setattr(
        release_gate,
        "validate_release_prepare_approval_binding",
        REAL_APPROVAL_BINDING_VALIDATOR,
    )

    result, _calls = _prepare(prepared_fixture)

    assert result.phase == "finalize"
    plan = json.loads((result.run_root / "release_plan.json").read_text())
    assert plan["gate_validation"]["approval_binding"]["ok"] is True


@pytest.mark.parametrize("mode", [0o644, 0o400])
def test_env_file_must_be_owner_only(prepared_fixture, mode):
    prepared_fixture.inputs.env_file.chmod(mode)
    calls = {"provenance": 0, "runtime": 0}

    with pytest.raises(prepare.ReleasePrepareError) as error:
        _call(prepared_fixture, phase="request", calls=calls)

    assert error.value.code == "release_prepare_env_not_owner_only"


def test_provenance_is_recollected_and_must_stay_exact(prepared_fixture):
    calls = 0

    def changing(_settings):
        nonlocal calls
        calls += 1
        value = json.loads(json.dumps(prepared_fixture.provenance))
        if calls == 2:
            value["vm"]["commit"] = "e" * 40
        return value

    with pytest.raises(prepare.ReleasePrepareError) as error:
        prepare.prepare_release(
            prepared_fixture.inputs,
            phase="request",
            now=NOW,
            machine_identity_observer=lambda: MACHINE_IDENTITY,
            provenance_verifier=changing,
            runtime_projector=lambda _host, _stage, **_kwargs: (
                prepared_fixture.runtime
            ),
            runtime_stage_validator=_fixture_stage_validator(prepared_fixture),
            workspace_runtime_validator=_fixture_workspace_validator(
                prepared_fixture
            ),
        )

    assert error.value.code == "build_manifest_provenance_changed"
    assert calls == 2


def test_runtime_is_recollected_and_must_stay_exact(prepared_fixture):
    calls = 0

    def changing(_host, _stage, **_kwargs):
        nonlocal calls
        calls += 1
        value = json.loads(json.dumps(prepared_fixture.runtime))
        if calls == 2:
            value["launchd_config_sha256"] = "e" * 64
        return value

    with pytest.raises(prepare.ReleasePrepareError) as error:
        prepare.prepare_release(
            prepared_fixture.inputs,
            phase="request",
            now=NOW,
            machine_identity_observer=lambda: MACHINE_IDENTITY,
            provenance_verifier=lambda _settings: prepared_fixture.provenance,
            runtime_projector=changing,
            runtime_stage_validator=_fixture_stage_validator(prepared_fixture),
            workspace_runtime_validator=_fixture_workspace_validator(
                prepared_fixture
            ),
        )

    assert error.value.code == "candidate_runtime_changed_during_prepare"
    assert calls == 2


def test_default_runtime_path_uses_future_projector_twice(
    prepared_fixture, monkeypatch
):
    calls = 0

    def projector(host, stage, **kwargs):
        nonlocal calls
        calls += 1
        assert host == prepared_fixture.inputs.host_candidate
        assert stage == prepared_fixture.inputs.runtime_staging_root
        assert kwargs["candidate_plists"] == prepared_fixture.inputs.candidate_plists
        assert kwargs["canonical_live_root"] == (
            release_gate.CANONICAL_FUTURE_RUNTIME_ROOT
        )
        assert kwargs["runtime_config_environment"] == {
            "HERMES_RCA_KAFKA_PASSWORD": "super-secret-value",
            "HERMES_RCA_KAFKA_TOPIC": "feishu-project-workfLow-event",
        }
        assert (
            kwargs["vm_worker_candidate_root"]
            == prepared_fixture.inputs.vm_worker_candidate
        )
        return prepared_fixture.runtime

    monkeypatch.setattr(release_gate, "project_future_candidate_runtime", projector)
    monkeypatch.setattr(
        release_gate,
        "check_candidate_runtime_dependencies",
        lambda *_args, **_kwargs: pytest.fail("old one-root runtime path used"),
    )

    prepare.prepare_release(
        prepared_fixture.inputs,
        phase="request",
        now=NOW,
        machine_identity_observer=lambda: MACHINE_IDENTITY,
        provenance_verifier=lambda _settings: prepared_fixture.provenance,
        runtime_stage_validator=_fixture_stage_validator(prepared_fixture),
        workspace_runtime_validator=_fixture_workspace_validator(prepared_fixture),
    )

    assert calls == 2


def test_dirty_or_unstable_candidate_fails_before_publication(prepared_fixture):
    def dirty(_settings):
        raise release_gate.EvidenceError("build_manifest_host_tree_dirty")

    with pytest.raises(prepare.ReleasePrepareError) as error:
        prepare.prepare_release(
            prepared_fixture.inputs,
            now=NOW,
            machine_identity_observer=lambda: MACHINE_IDENTITY,
            provenance_verifier=dirty,
            runtime_projector=lambda _host, _stage, **_kwargs: (
                prepared_fixture.runtime
            ),
            runtime_stage_validator=_fixture_stage_validator(prepared_fixture),
            workspace_runtime_validator=_fixture_workspace_validator(
                prepared_fixture
            ),
        )

    assert error.value.code == "build_manifest_host_tree_dirty"
    assert not prepared_fixture.inputs.run_root.joinpath("build_manifest.json").exists()


def test_workspace_closure_failure_is_fail_closed(prepared_fixture):
    prepared_fixture.provenance["workspace"]["execution_closure"]["scoped_clean"] = (
        False
    )

    with pytest.raises(prepare.ReleasePrepareError) as error:
        _prepare(prepared_fixture)

    assert error.value.code == "build_manifest_workspace_closure_dirty"


def test_exact_current_candidate_plists_are_required(prepared_fixture):
    assert set(prepare.EXPECTED_CANDIDATE_PLISTS) == set(
        release_gate.FUTURE_RUNTIME_PLIST_FILENAMES
    )
    prepared_fixture.inputs = prepare.PrepareInputs(**{
        **prepared_fixture.inputs.__dict__,
        "candidate_plists": prepared_fixture.inputs.candidate_plists[:-1],
    })

    with pytest.raises(prepare.ReleasePrepareError) as error:
        _prepare(prepared_fixture)

    assert error.value.code == "candidate_plist_set_mismatch"


def test_candidate_plists_must_come_from_physical_stage(prepared_fixture):
    prepared_fixture.inputs = prepare.PrepareInputs(**{
        **prepared_fixture.inputs.__dict__,
        "candidate_plists": tuple(
            prepared_fixture.host / filename
            for filename in prepare.EXPECTED_CANDIDATE_PLISTS
        ),
    })

    with pytest.raises(prepare.ReleasePrepareError) as error:
        _prepare(prepared_fixture)

    assert error.value.code == "candidate_plist_path_mismatch"


def test_runtime_stage_reader_uses_canonical_manifest_size_limit(prepared_fixture):
    manifest = json.loads(json.dumps(prepared_fixture.runtime_stage_manifest))
    descriptor = next(
        iter(manifest["content"]["source"]["runtime_files"].values())
    )
    descriptor["test_padding"] = "x" * (prepare.MAX_JSON_BYTES + 1)
    manifest["content_sha256"] = prepare._sha256_json(manifest["content"])
    _write_owner_json(prepared_fixture.inputs.runtime_stage_manifest, manifest)
    assert prepared_fixture.inputs.runtime_stage_manifest.stat().st_size > (
        prepare.MAX_JSON_BYTES
    )
    assert prepared_fixture.inputs.runtime_stage_manifest.stat().st_size < (
        prepare.runtime_stage.MAX_JSON_BYTES
    )

    identity = prepare._validate_runtime_stage_identity(
        prepared_fixture.inputs,
        validator=lambda _root: manifest,
    )

    assert identity["manifest_sha256"] == hashlib.sha256(
        prepared_fixture.inputs.runtime_stage_manifest.read_bytes()
    ).hexdigest()


def test_runtime_stage_manifest_is_revalidated_and_drift_fails(prepared_fixture):
    calls = 0

    def changing_stage(_root):
        nonlocal calls
        calls += 1
        value = json.loads(json.dumps(prepared_fixture.runtime_stage_manifest))
        if calls == 2:
            value["plan_sha256"] = "f" * 64
        return value

    with pytest.raises(prepare.ReleasePrepareError) as error:
        prepare.prepare_release(
            prepared_fixture.inputs,
            phase="request",
            now=NOW,
            machine_identity_observer=lambda: MACHINE_IDENTITY,
            provenance_verifier=lambda _settings: prepared_fixture.provenance,
            runtime_projector=lambda _host, _stage, **_kwargs: (
                prepared_fixture.runtime
            ),
            runtime_stage_validator=changing_stage,
            workspace_runtime_validator=_fixture_workspace_validator(
                prepared_fixture
            ),
        )

    assert error.value.code == "runtime_stage_manifest_validation_mismatch"
    assert calls == 2


@pytest.mark.parametrize("mutation", ["source_commit", "file_sha256"])
def test_workspace_runtime_must_match_workspace_candidate(
    prepared_fixture, mutation
):
    if mutation == "source_commit":
        prepared_fixture.workspace_runtime_identity["source_commit"] = "f" * 40
    else:
        prepared_fixture.workspace_runtime_identity["file_sha256"][
            "bin/create_task_v2.py"
        ] = "f" * 64

    with pytest.raises(prepare.ReleasePrepareError) as error:
        _prepare(prepared_fixture)

    assert error.value.code in {
        "workspace_runtime_identity_mismatch",
        "workspace_runtime_workspace_file_mismatch",
    }


@pytest.mark.parametrize("mutation", ["missing", "unknown"])
def test_vm_provenance_six_field_projection_is_exact(prepared_fixture, mutation):
    if mutation == "missing":
        del prepared_fixture.provenance["vm"]["entrypoint_blob"]
    else:
        prepared_fixture.provenance["vm"]["unexpected"] = True

    with pytest.raises(prepare.ReleasePrepareError) as error:
        _prepare(prepared_fixture)

    assert error.value.code == "build_provenance_vm_component_shape_invalid"


def test_vm_entrypoint_cross_commit_hash_fails_closed(prepared_fixture):
    prepared_fixture.provenance["vm"]["entrypoint_committed_sha256"] = "f" * 64

    with pytest.raises(prepare.ReleasePrepareError) as error:
        _prepare(prepared_fixture)

    assert error.value.code == "build_manifest_vm_entrypoint_hash_mismatch"


def test_candidate_plist_runtime_hash_mismatch_fails(prepared_fixture):
    filename = prepare.EXPECTED_CANDIDATE_PLISTS[0]
    prepared_fixture.runtime["render_manifest"]["candidate_plists"][filename][
        "staging_sha256"
    ] = "0" * 64
    prepared_fixture.runtime["render_manifest_sha256"] = prepare._sha256_json(
        prepared_fixture.runtime["render_manifest"]
    )
    prepared_fixture.runtime["future_runtime_projection"][
        "render_manifest_sha256"
    ] = prepared_fixture.runtime["render_manifest_sha256"]

    with pytest.raises(prepare.ReleasePrepareError) as error:
        _prepare(prepared_fixture)

    assert error.value.code == "candidate_plist_runtime_hash_mismatch"


def test_rollback_config_is_strict_and_bound(prepared_fixture):
    body = json.loads(prepared_fixture.rollback_path.read_text())
    body["rollback_window_seconds"] = 60
    _write_owner_json(prepared_fixture.rollback_path, body)

    with pytest.raises(prepare.ReleasePrepareError) as error:
        _prepare(prepared_fixture)

    assert error.value.code == "rollback_config_window_invalid"


def test_run_root_must_be_outside_candidate(prepared_fixture):
    prepared_fixture.inputs = prepare.PrepareInputs(**{
        **prepared_fixture.inputs.__dict__,
        "run_root": prepared_fixture.host / "release-run",
    })

    with pytest.raises(prepare.ReleasePrepareError) as error:
        _prepare(prepared_fixture)

    assert error.value.code == "release_prepare_run_root_inside_candidate"


def test_future_live_root_is_fixed_canonical(prepared_fixture, tmp_path):
    prepared_fixture.inputs = prepare.PrepareInputs(**{
        **prepared_fixture.inputs.__dict__,
        "future_live_root": tmp_path / "other-live-root",
    })

    with pytest.raises(prepare.ReleasePrepareError) as error:
        _prepare(prepared_fixture)

    assert error.value.code == "future_runtime_canonical_root_invalid"


def test_no_clobber_recovers_crash_before_link(tmp_path, monkeypatch):
    root = tmp_path / "run"
    root.mkdir(mode=0o700)
    destination = root / "artifact.json"
    body = {"schema_version": "fixture_v1", "value": 1}
    original_link = os.link
    calls = 0

    def fail_once(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("simulated crash")
        return original_link(*args, **kwargs)

    monkeypatch.setattr(os, "link", fail_once)
    with pytest.raises(RuntimeError, match="simulated crash"):
        prepare._publish_no_clobber(destination, body)
    assert not destination.exists()
    assert len(list(root.glob(".*.tmp"))) == 1

    assert prepare._publish_no_clobber(destination, body) is False
    assert json.loads(destination.read_text()) == body
    assert not list(root.glob(".*.tmp"))


def test_no_clobber_recovers_crash_after_link(tmp_path, monkeypatch):
    root = tmp_path / "run"
    root.mkdir(mode=0o700)
    destination = root / "artifact.json"
    body = {"schema_version": "fixture_v1", "value": 1}
    original_fsync = prepare._fsync_directory
    calls = 0

    def fail_after_link(path):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("simulated crash")
        return original_fsync(path)

    monkeypatch.setattr(prepare, "_fsync_directory", fail_after_link)
    with pytest.raises(RuntimeError, match="simulated crash"):
        prepare._publish_no_clobber(destination, body)
    assert destination.exists()
    assert len(list(root.glob(".*.tmp"))) == 1

    monkeypatch.setattr(prepare, "_fsync_directory", original_fsync)
    assert prepare._publish_no_clobber(destination, body) is True
    assert not list(root.glob(".*.tmp"))


def test_no_clobber_rejects_conflicting_destination(tmp_path):
    root = tmp_path / "run"
    root.mkdir(mode=0o700)
    destination = root / "artifact.json"
    prepare._publish_no_clobber(destination, {"value": 1})

    with pytest.raises(prepare.ReleasePrepareError) as error:
        prepare._publish_no_clobber(destination, {"value": 2})

    assert error.value.code == "release_prepare_artifact_conflict"


def test_existing_run_root_with_foreign_content_is_rejected(prepared_fixture):
    prepared_fixture.inputs.run_root.mkdir(mode=0o700)
    (prepared_fixture.inputs.run_root / "foreign.txt").write_text("foreign")

    with pytest.raises(prepare.ReleasePrepareError) as error:
        _prepare(prepared_fixture)

    assert error.value.code == "release_prepare_run_root_not_empty"


def test_manifest_hashes_match_exact_published_bytes(prepared_fixture):
    result, _calls = _prepare(prepared_fixture)
    manifest = result.manifest

    for filename, descriptor in manifest["artifacts"].items():
        raw = (result.run_root / filename).read_bytes()
        assert descriptor["sha256"] == hashlib.sha256(raw).hexdigest()
        assert descriptor["size_bytes"] == len(raw)


def test_cli_has_no_apply_or_execute_switch():
    actions = {action.dest for action in prepare._parser()._actions}

    assert "apply" not in actions
    assert "execute" not in actions
    assert "env_file" in actions
    assert "approval_receipt" in actions
    assert "run_root" in actions
    assert {
        "runtime_staging_root",
        "runtime_stage_manifest",
        "future_live_root",
        "workspace_runtime_root",
        "workspace_runtime_manifest",
    }.issubset(actions)


def test_old_cli_without_staged_identity_inputs_is_rejected():
    with pytest.raises(SystemExit):
        prepare._parser().parse_args([
            "--env-file",
            "/tmp/candidate.env",
            "--host-candidate",
            "/tmp/host",
            "--workspace-candidate",
            "/tmp/workspace",
            "--vm-candidate",
            "/srv/vm",
            "--vm-worker-candidate",
            "/srv/worker",
            "--candidate-plist",
            "/tmp/candidate.plist",
            "--release-id",
            "rca-release-0001",
            "--rollback-config",
            "/tmp/rollback.json",
            "--run-root",
            "/tmp/run",
            "--host-contract",
            "/tmp/host-contract.py",
            "--vm-contract",
            "/tmp/vm-contract.py",
        ])


def test_real_isolated_candidate_runtime_is_fail_closed_without_projection():
    with pytest.raises(release_gate.EvidenceError) as error:
        release_gate.check_candidate_runtime_dependencies(release_gate.REPO_ROOT)

    assert error.value.code == "runtime_candidate_interpreter_mismatch"


def test_real_request_fails_closed_for_unsealed_actual_host_candidate(
    prepared_fixture,
):
    actual_root = release_gate.REPO_ROOT
    with tempfile.TemporaryDirectory(prefix="rca-release-prepare-") as directory:
        prepared_fixture.inputs = prepare.PrepareInputs(**{
            **prepared_fixture.inputs.__dict__,
            "host_candidate": actual_root,
            "run_root": Path(directory) / "release-run",
        })

        with pytest.raises(prepare.ReleasePrepareError) as error:
            prepare.prepare_release(
                prepared_fixture.inputs,
                phase="request",
                now=NOW,
                machine_identity_observer=lambda: MACHINE_IDENTITY,
                provenance_verifier=lambda _settings: prepared_fixture.provenance,
                runtime_stage_validator=_fixture_stage_validator(prepared_fixture),
                workspace_runtime_validator=_fixture_workspace_validator(
                    prepared_fixture
                ),
            )

        assert error.value.code in {
            "build_manifest_host_tree_dirty",
            "build_manifest_host_git_unavailable",
            "future_runtime_stage_file_invalid",
        }
        assert not prepared_fixture.inputs.run_root.joinpath(
            prepare.APPROVAL_REQUEST_FILENAME
        ).exists()
