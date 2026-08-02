from __future__ import annotations

import json
from pathlib import Path

import pytest

from gateway import pnc_rca_release_authority as authority
from scripts import pnc_rca_release_authority as cli


def _candidate() -> dict:
    face = {"commit": "1" * 40, "tree": "2" * 40, "root": "/srv/rca"}
    return {
        "schema_version": authority.AUTHORITY_SCHEMA_VERSION,
        "release_id": "rca-cli-test",
        "authority_epoch_id": "rca-cli-epoch",
        "created_at": "2026-08-02T06:00:00Z",
        "status": "candidate_only",
        "supersedes_authority_sha256": None,
        "faces": {
            "host_runtime": {**face, "root": "/srv/rca/host"},
            "vm_worker_state": {**face, "root": "/srv/rca/worker"},
            "g1q3_rca_pipeline": {**face, "root": "/srv/rca/pipeline"},
            "mcap_data_translate": {
                **face,
                "root": "/srv/rca/mcap",
                "contract_sha256": "3" * 64,
            },
        },
        "control_store": {
            "schema_version": "pnc_rca_control_store_v13",
            "database_instance_id": "device1-inode1",
            "schema_fingerprint_sha256": None,
            "backup_receipt_sha256": None,
            "not_measured_reason": "W2 not materialized",
        },
        "quarantine_baseline": {
            "state": "not_measured",
            "required": None,
            "schema_version": None,
            "baseline_sha256": None,
            "not_measured_reason": "W2 not materialized",
        },
        "side_effect_policy": {
            "mode": "disabled",
            "single_active_writer": True,
            "allow_historical_requeue": False,
            "allowed_effect_kinds": ["feishu_issue_comment"],
        },
        "report_publication": {
            "canonical_base_url": "http://127.0.0.1:18081",
            "root": "/mnt/tmp",
            "manifest_schema_version": "pnc_rca_report_manifest_v1",
        },
        "feishu_capability": {
            "required_surfaces": ["issue_comment"],
            "capability_profile_sha256": None,
            "not_measured_reason": "W4 not materialized",
        },
    }


def test_compile_candidate_writes_only_offline_outputs(tmp_path: Path) -> None:
    value = _candidate()
    input_path = tmp_path / "input.json"
    input_path.write_text(json.dumps(value), encoding="utf-8")
    output_dir = tmp_path / "candidate"

    result = cli.compile_candidate(
        value,
        output_dir=output_dir,
        generated_at="2026-08-02T06:00:00Z",
    )

    authority_path = output_dir / "rca-cli-test.authority.json"
    pointer_path = output_dir / "ACTIVE_RCA_RELEASE.candidate.json"
    assert result["ok"] is True
    assert result["production_mutation_performed"] is False
    assert authority_path.exists()
    assert pointer_path.exists()
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    assert pointer["state"] == "candidate"
    assert pointer["authority_sha256"] == authority.canonical_json_sha256(value)


def test_compile_rejects_approved_authority(tmp_path: Path) -> None:
    value = _candidate()
    value["status"] = "approved_for_activation"
    value["control_store"].update({
        "schema_fingerprint_sha256": "3" * 64,
        "backup_receipt_sha256": "3" * 64,
        "not_measured_reason": "",
    })
    value["quarantine_baseline"] = {
        "state": "ready",
        "required": True,
        "schema_version": "pnc_rca_delivery_quarantine_baseline_v1",
        "baseline_sha256": "3" * 64,
        "not_measured_reason": "",
    }
    value["feishu_capability"].update({
        "capability_profile_sha256": "3" * 64,
        "not_measured_reason": "",
    })

    with pytest.raises(cli.AuthorityCliError) as raised:
        cli.compile_candidate(value, output_dir=tmp_path / "candidate", generated_at="2026-08-02T06:00:00Z")

    assert raised.value.code == "rca_release_authority_offline_status_required"


def test_cli_verify_returns_nonzero_for_current_projection_split(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    value = _candidate()
    authority_path = tmp_path / "authority.json"
    authority_path.write_text(json.dumps(value), encoding="utf-8")
    binding_path = tmp_path / "binding.json"
    binding_path.write_text(json.dumps({"release_id": value["release_id"]}), encoding="utf-8")
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps({"runtime_root": "/different"}), encoding="utf-8")

    result = cli.main([
        "verify",
        "--authority",
        str(authority_path),
        "--live-manifest",
        str(manifest_path),
        "--active-binding",
        str(binding_path),
    ])
    output = capsys.readouterr().out

    assert result == 2
    parsed = json.loads(output)
    assert parsed["ok"] is False
    assert any(
        item["code"] == "rca_release_live_manifest_authority_missing"
        for item in parsed["errors"]
    )
