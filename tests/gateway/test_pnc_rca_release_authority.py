from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3

import pytest

from gateway import pnc_rca_release_authority as authority


NOW = "2026-08-02T06:00:00Z"
COMMIT = "1" * 40
TREE = "2" * 40
SHA = "3" * 64


def _face(root: str = "/srv/rca") -> dict[str, str]:
    return {"commit": COMMIT, "tree": TREE, "root": root}


def _authority(*, status: str = "candidate_only") -> dict:
    candidate = status == "candidate_only"
    return {
        "schema_version": authority.AUTHORITY_SCHEMA_VERSION,
        "release_id": "rca-test-release",
        "authority_epoch_id": "rca-test-epoch",
        "created_at": NOW,
        "status": status,
        "supersedes_authority_sha256": None,
        "faces": {
            "host_runtime": _face("/srv/rca/host"),
            "vm_worker_state": _face("/home/mini/.hermes/worker-state"),
            "g1q3_rca_pipeline": _face("/home/mini/rca-pipeline"),
            "mcap_data_translate": {
                **_face("/home/mini/mcap"),
                "contract_sha256": SHA,
            },
        },
        "control_store": {
            "schema_version": "pnc_rca_control_store_v13",
            "database_instance_id": "device1-inode2",
            "schema_fingerprint_sha256": None if candidate else SHA,
            "backup_receipt_sha256": None if candidate else SHA,
            "not_measured_reason": "W2 backup not materialized" if candidate else "",
        },
        "quarantine_baseline": {
            "state": "not_measured" if candidate else "ready",
            "required": None if candidate else True,
            "schema_version": None if candidate else "pnc_rca_delivery_quarantine_baseline_v1",
            "baseline_sha256": None if candidate else SHA,
            "not_measured_reason": "W2 baseline not materialized" if candidate else "",
        },
        "side_effect_policy": {
            "mode": "disabled",
            "single_active_writer": True,
            "allow_historical_requeue": False,
            "allowed_effect_kinds": ["feishu_issue_comment", "feishu_thread_reply"],
        },
        "report_publication": {
            "canonical_base_url": "http://192.168.26.174:18081",
            "root": "/mnt/tmp",
            "manifest_schema_version": "pnc_rca_report_manifest_v1",
        },
        "feishu_capability": {
            "required_surfaces": ["issue_comment", "thread_reply"],
            "capability_profile_sha256": None if candidate else SHA,
            "not_measured_reason": "W4 capability probes not materialized" if candidate else "",
        },
    }


def _manifest(value: dict, *, authority_sha: str | None = None) -> dict:
    faces = value["faces"]
    return {
        "runtime_root": faces["host_runtime"]["root"],
        "rca_release_authority": {
            "release_id": value["release_id"],
            "authority_sha256": authority_sha,
        },
        "face_git_bindings": {
            "runtime_engine": {
                "commit": faces["host_runtime"]["commit"],
                "tree": faces["host_runtime"]["tree"],
                "repo": faces["host_runtime"]["root"],
            },
            "vm_worker_state": {
                "commit": faces["vm_worker_state"]["commit"],
                "tree": faces["vm_worker_state"]["tree"],
                "repo": faces["vm_worker_state"]["root"],
            },
            "g1q3_rca_pipeline": {
                "commit": faces["g1q3_rca_pipeline"]["commit"],
                "tree": faces["g1q3_rca_pipeline"]["tree"],
                "repo": faces["g1q3_rca_pipeline"]["root"],
            },
            "mcap_data_translate": {
                "commit": faces["mcap_data_translate"]["commit"],
                "tree": faces["mcap_data_translate"]["tree"],
                "repo": faces["mcap_data_translate"]["root"],
            },
        },
    }


def _binding(value: dict, authority_sha: str | None = None) -> dict:
    return {
        "release_id": value["release_id"],
        "authority_epoch_id": value["authority_epoch_id"],
        "authority_sha256": authority_sha,
    }


def test_candidate_authority_is_strict_and_hash_is_stable() -> None:
    value = _authority()

    authority.validate_release_authority(value)
    first = authority.canonical_json_sha256(value)
    second = authority.canonical_json_sha256(json.loads(json.dumps(value)))

    assert first == second
    assert len(first) == 64


def test_approved_authority_rejects_unmeasured_control_or_baseline() -> None:
    value = _authority(status="approved_for_activation")
    value["control_store"]["backup_receipt_sha256"] = None

    with pytest.raises(authority.ReleaseAuthorityError) as raised:
        authority.validate_release_authority(value)

    assert raised.value.code == "rca_release_authority_approval_incomplete"


def test_candidate_pointer_cannot_become_active() -> None:
    value = _authority()

    pointer = authority.build_active_pointer(
        value,
        authority_path="/tmp/rca-test-release.authority.json",
        state="candidate",
        activated_at=NOW,
    )
    authority.validate_active_pointer(pointer, value)

    with pytest.raises(authority.ReleaseAuthorityError) as raised:
        authority.build_active_pointer(
            value,
            authority_path="/tmp/rca-test-release.authority.json",
            state="active",
            activated_at=NOW,
        )

    assert raised.value.code == "rca_release_pointer_activation_invalid"


def test_projection_audit_passes_only_when_every_projection_binds_exact_authority(
    tmp_path: Path,
) -> None:
    value = _authority()
    digest = authority.canonical_json_sha256(value)
    pointer = authority.build_active_pointer(
        value,
        authority_path=tmp_path / "authority.json",
        state="candidate",
        activated_at=NOW,
    )
    database = tmp_path / "control.sqlite3"
    connection = sqlite3.connect(database)
    connection.execute("CREATE TABLE control_meta(key TEXT PRIMARY KEY, value TEXT)")
    connection.execute(
        "INSERT INTO control_meta(key, value) VALUES ('schema_version', ?)",
        ("pnc_rca_control_store_v13",),
    )
    connection.commit()
    connection.close()

    result = authority.audit_release_projections(
        value,
        pointer=pointer,
        authority_path=tmp_path / "authority.json",
        live_manifest=_manifest(value, authority_sha=digest),
        active_binding=_binding(value, authority_sha=digest),
        control_store_path=database,
        now=datetime(2026, 8, 2, 6, 0, tzinfo=timezone.utc),
    )

    assert result["ok"] is True
    assert result["errors"] == []
    assert result["checks"]["control_store"]["status"] == "pass"


def test_projection_audit_rejects_current_split_shape() -> None:
    value = _authority()
    result = authority.audit_release_projections(
        value,
        live_manifest={"runtime_root": "/different"},
        active_binding={"release_id": value["release_id"]},
    )

    assert result["ok"] is False
    codes = {item["code"] for item in result["errors"]}
    assert "rca_release_live_manifest_authority_missing" in codes
    assert "rca_release_active_binding_authority_mismatch" in codes


def _stage_status() -> dict:
    return {
        "schema_version": authority.STAGE_STATUS_SCHEMA_VERSION,
        "authority_sha256": SHA,
        "observed_at": NOW,
        "stages": {
            name: {
                "status": "pass",
                "observed_at": NOW,
                "evidence_sha256": SHA,
                "reason": "",
            }
            for name in authority.RELEASE_STAGES
        },
    }


def test_stage_status_requires_reason_for_not_measured_and_completion_is_derived() -> None:
    status = _stage_status()

    authority.validate_stage_status(status)
    assert authority.production_completion_proven(status) is True

    status["stages"]["remote_receipt_proven"] = {
        "status": "not_measured",
        "observed_at": NOW,
        "evidence_sha256": None,
        "reason": "remote write not authorized",
    }
    authority.validate_stage_status(status)
    assert authority.production_completion_proven(status) is False

    status["stages"]["remote_receipt_proven"]["reason"] = ""
    with pytest.raises(authority.ReleaseAuthorityError) as raised:
        authority.validate_stage_status(status)
    assert raised.value.code == "rca_release_stage_measurement_invalid"


def _health(*, mode: str = "shadow", observed_at: str = NOW) -> dict:
    dimension = {
        "status": "pass",
        "evidence_sha256": SHA,
        "reason": "",
    }
    return {
        "schema_version": authority.COMPONENT_HEALTH_SCHEMA_VERSION,
        "component": "rca-test-component",
        "authority_sha256": SHA,
        "observed_at": observed_at,
        "freshness_ttl_seconds": 60,
        "pid": 1234,
        "started_at": NOW,
        "executable": "/usr/bin/rca-test",
        "executable_sha256": SHA,
        "process_health": dict(dimension),
        "dependency_health": dict(dimension),
        "readiness": dict(dimension),
        "side_effect_mode": mode,
    }


def test_component_health_rejects_disabled_ready_and_stale() -> None:
    disabled = _health(mode="disabled")
    with pytest.raises(authority.ReleaseAuthorityError) as raised:
        authority.validate_component_health(
            disabled,
            now=datetime(2026, 8, 2, 6, 0, tzinfo=timezone.utc),
        )
    assert raised.value.code == "rca_component_health_readiness_invalid"

    stale = _health(observed_at="2026-08-02T05:00:00Z")
    with pytest.raises(authority.ReleaseAuthorityError) as raised:
        authority.validate_component_health(
            stale,
            now=datetime(2026, 8, 2, 6, 0, tzinfo=timezone.utc),
        )
    assert raised.value.code == "rca_component_health_stale"
