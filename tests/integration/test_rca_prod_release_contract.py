"""Offline Host -> sealed creator -> VM admission release contract."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from gateway.pnc_rca_admission import build_rca_admission
from gateway.pnc_rca_workspace_runtime import validate_staged_workspace_runtime
from gateway import pnc_rca_prod_admission
from tools import vm_task_tool


pytestmark = pytest.mark.integration

WORKSPACE_RUNTIME_ENV = "HERMES_RCA_RELEASE_WORKSPACE_RUNTIME"
VM_ADMISSION_MODULE_ENV = "HERMES_RCA_RELEASE_VM_ADMISSION_MODULE"
WORKSPACE_SOURCE_COMMIT = "c7e911ddc4292c555977800212d93304f1f87f1c"
WORKSPACE_CLOSURE_SHA256 = (
    "26db9cf6b5070d1c9434f5632ec88f08290c1f1c92d78a244026395b522b898b"
)
VM_ADMISSION_SHA256 = (
    "a7df5bd4ee2c7d8232858dcc2837c006e2940b3097eda64b02becd30c536353b"
)
HMAC_KEY = "hex:" + ("42" * 32)
NOW = datetime(2026, 7, 21, 5, 0, tzinfo=timezone.utc)


def _required_release_path(name: str) -> Path:
    raw = os.environ.get(name, "").strip()
    if not raw:
        pytest.skip(f"set {name} to run the pinned RCA release contract")
    path = Path(raw).expanduser().absolute()
    if not path.exists():
        pytest.fail(f"{name} does not exist: {path}")
    return path


def _load_vm_admission(path: Path):
    assert hashlib.sha256(path.read_bytes()).hexdigest() == VM_ADMISSION_SHA256
    spec = importlib.util.spec_from_file_location(
        "pinned_e287b731_vm_rca_prod_admission", path
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _snapshot(now: datetime = NOW) -> dict:
    return {
        "schema_version": pnc_rca_prod_admission.SNAPSHOT_SCHEMA_VERSION,
        "observed_at": now.isoformat(),
        "root_available_bytes": 700 * 1024**3,
        "delivery_available_bytes": 1200 * 1024**3,
        "root_device": "2050",
        "delivery_device": "93",
        "delivery_filesystem": "cifs",
        "delivery_mount_rw": True,
        "delivery_writable": True,
        "memory_available_bytes": 64 * 1024**3,
        "swap_free_ratio": 0.9,
        "load1": 1.0,
        "cpu_count": 32,
        "dnp_real": 0,
        "dnp_like": 0,
        "mcap_rss_bytes": 0,
        "mcap_process_count": 0,
    }


def _resource_report() -> dict:
    snapshot = _snapshot()
    capacity = {
        "schema_version": "context-rca-capacity-authorization/v1",
        "policy_version": "context-rca-capacity-model/2026-07-12-v1",
        "receipt_path": "/offline/test-capacity-authorization.json",
        "authorization_ready": True,
        "status": "valid",
        "reason_codes": [],
        "receipt_id": "offline-capacity-receipt",
        "receipt_fingerprint": "11" * 32,
        "sample_set_fingerprint": "22" * 32,
        "issued_at": (NOW - timedelta(minutes=5)).isoformat(),
        "expires_at": (NOW + timedelta(hours=1)).isoformat(),
        "successful_sample_count": 20,
        "input_materialized_sample_count": 0,
        "max_concurrency": 1,
        "root_peak_declared_bytes": 12 * 1024**3,
        "root_peak_observed_bytes": 10 * 1024**3,
        "delivery_peak_p95_bytes": 2 * 1024**3,
        "delivery_peak_p99_bytes": 3 * 1024**3,
        "delivery_growth_7d_bytes": 4 * 1024**3,
        "active_delivery_commitment_bytes": 5 * 1024**3,
        "capacity_horizon_days": 7,
        "root_required_available_bytes": 500 * 1024**3,
        "delivery_required_available_bytes": 600 * 1024**3,
        "approval_evidence_sha256": "33" * 32,
        "authorization_receipt_sha256": "44" * 32,
    }
    return {
        "ok": True,
        "ok_for_submit": True,
        "ok_for_rca_prod_submit": True,
        "resource_class": "rca_prod",
        "reasons": [],
        "rca_prod_reasons": [],
        "rca_capacity_authorization": capacity,
        "rca_prod_snapshot": snapshot,
        "rca_prod_snapshot_sha256": pnc_rca_prod_admission.sha256_value(snapshot),
    }


def _resource_runner(command, **_kwargs):
    return subprocess.CompletedProcess(
        command,
        0,
        stdout=json.dumps(_resource_report()),
        stderr="",
    )


def _goal_variant(goal: str, variant: str) -> str:
    stripped = goal.strip()
    if variant == "no_trailing_newline":
        return stripped
    if variant == "multiple_trailing_newlines":
        return stripped + "\n\n\n"
    if variant == "boundary_whitespace":
        return " \t\n" + stripped + "\n \t\n"
    raise AssertionError(variant)


@pytest.mark.parametrize(
    "variant",
    [
        "no_trailing_newline",
        "multiple_trailing_newlines",
        "boundary_whitespace",
    ],
)
def test_pinned_release_goal_bytes_survive_creator_and_vm_admission(
    tmp_path: Path,
    variant: str,
):
    workspace_root = _required_release_path(WORKSPACE_RUNTIME_ENV)
    vm_admission_path = _required_release_path(VM_ADMISSION_MODULE_ENV)
    workspace = validate_staged_workspace_runtime(workspace_root)
    assert workspace.source_commit == WORKSPACE_SOURCE_COMMIT
    assert workspace.closure_sha256 == WORKSPACE_CLOSURE_SHA256
    vm_admission = _load_vm_admission(vm_admission_path)

    admission = build_rca_admission(
        project_key="t03o4q",
        project_simple_name="g1q3",
        work_item_type_key="issue",
        work_item_id="7041712812",
        rule_version="issue-created-v1",
        topic="feishu-project-workflow-event",
        partition=1,
        offset=99,
    )
    task_id = admission.submission_key
    admission_payload = admission.to_dict()
    request_payload = {
        "schema_version": "g1q3_rca_execution_request_v2",
        "request_kind": "issue_intake",
    }
    built_goal = vm_task_tool.build_rca_fixed_cli_goal(
        task_id=task_id,
        admission=admission_payload,
        execution_request=request_payload,
    )
    canonical_goal = vm_task_tool.canonicalize_rca_goal_text(
        _goal_variant(built_goal, variant)
    )
    assert canonical_goal == built_goal

    contract_sha256 = vm_task_tool.canonical_rca_contract_sha256(
        admission_payload,
        request_payload,
    )
    prod_admission = pnc_rca_prod_admission.issue_rca_prod_admission(
        task_id=task_id,
        submission_key=task_id,
        goal=canonical_goal,
        contract_sha256=contract_sha256,
        reservation_id="offline-reservation",
        reservation_fence="1",
        reservation_contract_sha256="cd" * 32,
        run_func=_resource_runner,
        hmac_key=HMAC_KEY,
        now=NOW,
        attempt_id="offline-attempt",
        receipt_id="offline-receipt",
        capacity_mode="steady",
    )
    refs = admission_payload["source_refs"]
    artifact_root = f"/mnt/tmp/{task_id}/"
    artifact_cifs_root = (
        "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/"
        f"{task_id}/"
    )
    meta = {
        "actor_kind": "service",
        "business_line": "g1q3_rca",
        "service_capability": "submit_g1q3_rca_issue_intake",
        "service_operation": "g1q3_rca_issue_intake",
        "rca_business_key": admission.business_key,
        "rca_submission_key": task_id,
        "rca_generation": admission.generation,
        "rca_trigger_kind": admission.trigger_kind,
        "rca_create_once": True,
        "rca_contract_sha256": contract_sha256,
        "rca_data_access_mode": "remote_read",
        "rca_source_refs": {
            key: refs.get(key)
            for key in (
                "project_key",
                "work_item_type_key",
                "work_item_id",
                "rule_version",
                "topic",
                "partition",
                "offset",
            )
        },
        "artifact_root": artifact_root,
        "artifact_cifs_root": artifact_cifs_root,
        "repo_scope": "unknown",
        "workspace_scope": "none",
        "risk_class": "high",
        "executor_type": "direct_cli",
        "agent_backend": "none",
        "codex_backend_enabled": False,
        "coding_agent_fallback_enabled": False,
        "fixed_cli_entrypoint": (
            "/home/mini/.hermes/rca-prod-runtime/releases/"
            "rca-e2e-hotfix-20260723-remote-viz/"
            "api/g1q3_rca/scripts/run_rca_service_request.py"
        ),
        **workspace.task_meta(),
        **prod_admission.meta,
    }
    title = f"G1Q3 RCA issue intake: {refs['work_item_id']}"
    goal_input = tmp_path / "goal-input.md"
    goal_input.write_text(canonical_goal, encoding="utf-8")
    shared_state = tmp_path / "shared-state"
    created = subprocess.run(
        [
            sys.executable,
            str(workspace.creator_path),
            "--root",
            str(shared_state),
            "--task-id",
            task_id,
            "--title",
            title,
            "--goal-file",
            str(goal_input),
            "--owner",
            "root_cause_analysis_agent",
            "--meta",
            json.dumps(meta, ensure_ascii=False),
            "--create-once",
            "--json",
        ],
        check=False,
        text=True,
        capture_output=True,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        timeout=30,
    )
    assert created.returncode == 0, created.stderr
    assert json.loads(created.stdout)["status"] == "created"

    materialized_goal = shared_state / "tasks" / task_id / "goal.md"
    materialized_bytes = materialized_goal.read_bytes()
    expected_bytes = canonical_goal.encode("utf-8")
    expected_goal_sha256 = hashlib.sha256(expected_bytes).hexdigest()
    assert materialized_bytes == expected_bytes
    assert prod_admission.meta["rca_prod_goal_sha256"] == expected_goal_sha256
    assert (
        prod_admission.receipt["bindings"]["goal_sha256"]
        == expected_goal_sha256
    )

    command = vm_task_tool.build_rca_prod_command_argv(task_id)
    worker_task = {
        **meta,
        "task_id": task_id,
        "goal_path": str(materialized_goal),
        "work_tmp_dir": f"/mnt/tmp/{task_id}",
    }
    verdict = vm_admission.validate_worker_admission(
        worker_task,
        command,
        _snapshot(),
        now=NOW,
        hmac_key=HMAC_KEY,
    )
    assert verdict["ok"] is True, verdict["reasons"]
    assert verdict["identity"]["goal_sha256"] == expected_goal_sha256
    assert verdict["identity"]["contract_sha256"] == contract_sha256
    assert verdict["identity"]["command_sha256"] == (
        pnc_rca_prod_admission.command_sha256(command)
    )
