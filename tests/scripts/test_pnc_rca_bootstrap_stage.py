from __future__ import annotations

import hashlib
import json
import stat
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from gateway import pnc_rca_prod_bootstrap as bootstrap
from gateway import pnc_rca_release_authority as release_authority
from scripts import pnc_rca_bootstrap_stage as stage


NOW = datetime(2026, 7, 30, 16, 30, tzinfo=timezone.utc)
RELEASE_ID = "rca-goal-v4-r9-bootstrap-stage-test"
EPOCH_ID = "rca-bootstrap-r9-bootstrap-stage-test"


def _write_json(path: Path, value: object, *, mode: int = 0o600) -> None:
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")
    path.chmod(mode)


def _git(root: Path, value: str) -> str:
    return subprocess.check_output(
        ("git", "-C", str(root), "rev-parse", value), text=True
    ).strip()


def _paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> stage.StagePaths:
    authorization = tmp_path / "rca-bootstrap-capacity-authorization.json"
    monkeypatch.setattr(bootstrap, "BOOTSTRAP_AUTHORIZATION_PATH", authorization)
    env = tmp_path / ".env"
    env.write_text(
        "HERMES_RCA_PROD_CAPACITY_MODE=steady\n"
        "HERMES_RCA_PROD_RELEASE_ID=old-release\n"
        "HERMES_RCA_PROD_BOOTSTRAP_EPOCH_ID=rca-bootstrap-old\n"
        "UNRELATED=value\n",
        encoding="utf-8",
    )
    env.chmod(0o600)
    control_db = tmp_path / "control.sqlite3"
    control_db.write_bytes(b"placeholder")
    return stage.StagePaths(
        control_db=control_db,
        live_env=env,
        active_binding=tmp_path / "active-release-binding.json",
        authorization=authorization,
        receipt=tmp_path / "stage-receipt.json",
    )


def _inputs(
    tmp_path: Path, host_source: Path, *, resource_ok: bool = True
) -> tuple[Path, Path, Path, Path]:
    seal = tmp_path / "host-seal.json"
    _write_json(
        seal,
        {
            "schema_version": "pnc_rca_gray_host_seal_v1",
            "status": "candidate_sealed_unreleased",
            "release_id": RELEASE_ID,
            "candidate": {
                "clean": True,
                "worktree": str(host_source),
                "commit": _git(host_source, "HEAD"),
                "tree": _git(host_source, "HEAD^{tree}"),
            },
        },
    )
    authority = tmp_path / "authority.json"
    measured_sha = "3" * 64
    host_face = {
        "commit": _git(host_source, "HEAD"),
        "tree": _git(host_source, "HEAD^{tree}"),
        "root": "/srv/rca/host",
    }
    _write_json(
        authority,
        {
            "schema_version": release_authority.AUTHORITY_SCHEMA_VERSION,
            "release_id": RELEASE_ID,
            "authority_epoch_id": "rca-authority-bootstrap-stage-test",
            "created_at": NOW.isoformat(),
            "status": "approved_for_activation",
            "supersedes_authority_sha256": None,
            "faces": {
                "host_runtime": host_face,
                "vm_worker_state": {**host_face, "root": "/srv/rca/worker"},
                "g1q3_rca_pipeline": {**host_face, "root": "/srv/rca/pipeline"},
                "mcap_data_translate": {
                    **host_face,
                    "root": "/srv/rca/mcap",
                    "contract_sha256": measured_sha,
                },
            },
            "control_store": {
                "schema_version": "pnc_rca_control_store_v13",
                "database_instance_id": "device1-inode1",
                "schema_fingerprint_sha256": measured_sha,
                "backup_receipt_sha256": measured_sha,
                "not_measured_reason": "",
            },
            "quarantine_baseline": {
                "state": "ready",
                "required": True,
                "schema_version": "pnc_rca_delivery_quarantine_baseline_v1",
                "baseline_sha256": measured_sha,
                "not_measured_reason": "",
            },
            "side_effect_policy": {
                "mode": "disabled",
                "single_active_writer": True,
                "allow_historical_requeue": False,
                "allowed_effect_kinds": ["feishu_issue_comment"],
            },
            "report_publication": {
                "canonical_base_url": "http://192.168.26.174:18081",
                "root": "/mnt/tmp",
                "manifest_schema_version": "pnc_rca_report_manifest_v1",
            },
            "feishu_capability": {
                "required_surfaces": ["issue_comment"],
                "capability_profile_sha256": measured_sha,
                "not_measured_reason": "",
            },
        },
    )
    approval = tmp_path / "owner-authorization.json"
    _write_json(
        approval,
        {
            "schema_version": "pnc_rca_r8_full_completion_authorization_v1",
            "state": "ACTIVE_FULL_COMPLETION_SCOPE",
            "owner": "songying",
            "scope": ["canonical rca_prod submissions"],
        },
    )
    snapshot = tmp_path / "resource.json"
    _write_json(
        snapshot,
        {
            "ok_for_rca_prod_submit": resource_ok,
            "rca_prod_snapshot": {
                "observed_at": NOW.isoformat(),
                "root_available_bytes": bootstrap.ROOT_REQUIRED_AVAILABLE_BYTES,
                "delivery_available_bytes": bootstrap.DELIVERY_REQUIRED_AVAILABLE_BYTES,
            },
        },
    )
    return seal, authority, approval, snapshot


def _stage_args(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    paths = _paths(tmp_path, monkeypatch)
    host_source = Path(__file__).resolve().parents[2]
    seal, authority, approval, snapshot = _inputs(tmp_path, host_source)
    return {
        "paths": paths,
        "host_source": host_source,
        "host_seal_path": seal,
        "authority_path": authority,
        "owner_authorization_path": approval,
        "resource_snapshot_path": snapshot,
        "release_id": RELEASE_ID,
        "bootstrap_epoch_id": EPOCH_ID,
        "owner": "songying",
        "deadline": NOW + timedelta(hours=1),
        "now": NOW,
    }


def test_stage_apply_installs_a_release_bound_bootstrap_authority(tmp_path, monkeypatch):
    args = _stage_args(tmp_path, monkeypatch)

    result = stage.stage_bootstrap(**args, apply=True)

    assert result["status"] == "APPLIED_VERIFIED"
    assert result["production_effects"] == {
        "resident_start": False,
        "task_submission": False,
        "kafka_consume": False,
        "feishu_write": False,
    }
    assert stat.S_IMODE(args["paths"].authorization.stat().st_mode) == 0o600
    assert stat.S_IMODE(args["paths"].active_binding.stat().st_mode) == 0o600
    assert stat.S_IMODE(args["paths"].receipt.stat().st_mode) == 0o600
    assert "HERMES_RCA_PROD_CAPACITY_MODE=bootstrap" in args["paths"].live_env.read_text()
    assert f"HERMES_RCA_PROD_RELEASE_ID={RELEASE_ID}" in args["paths"].live_env.read_text()
    assert f"HERMES_RCA_PROD_BOOTSTRAP_EPOCH_ID={EPOCH_ID}" in args["paths"].live_env.read_text()
    binding = bootstrap.load_active_release_binding(
        path=args["paths"].active_binding,
        live_env_path=args["paths"].live_env,
        expected_release_id=RELEASE_ID,
        expected_epoch_id=EPOCH_ID,
        expected_authority_sha256=result["authority_sha256"],
        expected_authority_epoch_id=result["authority_epoch_id"],
    )
    authority = bootstrap.load_bootstrap_authorization(
        now=NOW,
        expected_epoch_id=EPOCH_ID,
        expected_release_bom_sha256=result["release_bom_sha256"],
        expected_release_approval_id=RELEASE_ID,
        expected_approval_evidence_sha256=result["owner_authorization_sha256"],
    )
    assert binding["authorization_receipt_sha256"] == authority["authorization_receipt_sha256"]
    assert binding["authority_sha256"] == result["authority_sha256"]
    assert binding["authority_epoch_id"] == result["authority_epoch_id"]
    assert result["readback"]["active_release_binding_sha256"] == hashlib.sha256(
        args["paths"].active_binding.read_bytes()
    ).hexdigest()


def test_stage_accepts_exact_bootstrap_epoch_duration_and_rejects_overflow(
    tmp_path,
    monkeypatch,
):
    args = _stage_args(tmp_path, monkeypatch)
    args["deadline"] = NOW + bootstrap.MAX_EPOCH_DURATION

    result = stage.stage_bootstrap(**args)

    assert stage.MAX_AUTHORIZATION_DURATION == bootstrap.MAX_EPOCH_DURATION
    assert result["status"] == "PLAN"
    assert result["deadline"] == args["deadline"].isoformat()

    args["deadline"] += timedelta(microseconds=1)
    with pytest.raises(stage.BootstrapStageError, match="deadline_invalid"):
        stage.stage_bootstrap(**args)


def test_stage_rejects_unready_resource_without_writes(tmp_path, monkeypatch):
    args = _stage_args(tmp_path, monkeypatch)
    _seal, _authority, _approval, snapshot = _inputs(
        tmp_path, args["host_source"], resource_ok=False
    )
    args["resource_snapshot_path"] = snapshot
    before = args["paths"].live_env.read_bytes()

    with pytest.raises(stage.BootstrapStageError, match="resource_not_ready"):
        stage.stage_bootstrap(**args, apply=True)

    assert args["paths"].live_env.read_bytes() == before
    assert not args["paths"].authorization.exists()
    assert not args["paths"].active_binding.exists()
    assert not args["paths"].receipt.exists()
