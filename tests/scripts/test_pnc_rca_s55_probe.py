from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from gateway.pnc_rca_release_authority import canonical_json_sha256
from scripts import pnc_rca_s55_probe as s55


def _write_owner(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    path.chmod(0o600)


def _authority() -> dict[str, Any]:
    return {
        "schema_version": "pnc_rca_release_authority_v1",
        "release_id": "rca-r11-s55-test",
        "authority_epoch_id": "rca-r11-s55-test-epoch",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": "approved_for_activation",
        "supersedes_authority_sha256": None,
        "faces": {
            "host_runtime": {"commit": "a" * 40, "tree": "b" * 40, "root": "/host/runtime"},
            "vm_worker_state": {"commit": "c" * 40, "tree": "d" * 40, "root": "/vm/worker"},
            "g1q3_rca_pipeline": {"commit": "e" * 40, "tree": "f" * 40, "root": "/vm/pipeline"},
            "mcap_data_translate": {
                "commit": "1" * 40,
                "tree": "2" * 40,
                "root": "/vm/mcap",
                "contract_sha256": "3" * 64,
            },
        },
        "control_store": {
            "schema_version": "pnc_rca_control_store_v13",
            "database_instance_id": "9d9b8cfc-5a1e-4ddb-91c0-18145a5b0e53",
            "schema_fingerprint_sha256": "4" * 64,
            "backup_receipt_sha256": "5" * 64,
            "not_measured_reason": "",
        },
        "quarantine_baseline": {
            "state": "ready",
            "required": True,
            "schema_version": "pnc_rca_delivery_quarantine_baseline_v1",
            "baseline_sha256": "6" * 64,
            "not_measured_reason": "",
        },
        "side_effect_policy": {
            "mode": "canary",
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
            "capability_profile_sha256": "7" * 64,
            "not_measured_reason": "",
        },
    }


@pytest.fixture
def probe_case(tmp_path: Path) -> SimpleNamespace:
    tmp_path.chmod(0o700)
    authority = _authority()
    authority_path = tmp_path / "authority.json"
    _write_owner(authority_path, authority)
    placeholder = tmp_path / "placeholder.json"
    _write_owner(placeholder, {"placeholder": True})
    return SimpleNamespace(
        authority=authority_path.absolute(),
        pointer=placeholder.absolute(),
        live_manifest=placeholder.absolute(),
        active_binding=placeholder.absolute(),
        schema_receipt=placeholder.absolute(),
        preproduction_gate=placeholder.absolute(),
        control_db=(tmp_path / "control.sqlite3").absolute(),
        env_file=placeholder.absolute(),
        vm_observation=placeholder.absolute(),
        expected_epoch_id="rca-r11-s55-test-epoch",
        expected_activation_state="preauthorized",
        expected_historical_hold=85,
        report_timeout_seconds=10.0,
        run_id="s55-test-run",
        output=(tmp_path / "s55-receipt.json").absolute(),
        authority_value=authority,
    )


def _stage_payload(name: str, result: str = "passed") -> bytes:
    return (
        json.dumps({
            "schema_version": s55.STAGE_SCHEMA_VERSION,
            "stage": name,
            "result": result,
            "reason": "" if result == "passed" else f"{name}_{result}",
            "detail": {"stage_evidence": name},
            "production_mutation_performed": False,
            "external_effects_triggered": False,
        })
        + "\n"
    ).encode()


def test_parent_records_real_stage_exit_codes_and_hashes(
    probe_case: SimpleNamespace,
) -> None:
    calls: list[str] = []

    def runner(command: list[str], **_kwargs: Any) -> SimpleNamespace:
        name = command[command.index("--name") + 1]
        calls.append(name)
        return SimpleNamespace(returncode=0, stdout=_stage_payload(name), stderr=b"")

    payload, exit_code = s55.run_probe(probe_case, runner=runner)
    receipt = json.loads(probe_case.output.read_text(encoding="utf-8"))

    assert calls == list(s55.STAGE_NAMES)
    assert exit_code == 0
    assert payload["result"] == "passed"
    assert receipt["direct_exit_code"] == 0
    assert receipt["not_measured_reason"] == ""
    assert probe_case.output.stat().st_mode & 0o777 == 0o600
    assert [item["exit_code"] for item in receipt["stages"]] == [0, 0, 0, 0, 0]
    assert all(len(item["stdout_sha256"]) == 64 for item in receipt["stages"])
    assert receipt["scope_attestation"]["feishu_remote_write_proven"] is False
    assert receipt["scope_attestation"]["mcap_execution_performed"] is False


@pytest.mark.parametrize(
    ("stage_result", "expected_result"),
    [("failed", "failed"), ("not_measured", "not_measured")],
)
def test_parent_nonpass_is_direct_exit_two_with_reason(
    probe_case: SimpleNamespace,
    stage_result: str,
    expected_result: str,
) -> None:
    def runner(command: list[str], **_kwargs: Any) -> SimpleNamespace:
        name = command[command.index("--name") + 1]
        result = stage_result if name == "report" else "passed"
        return SimpleNamespace(
            returncode=0 if result == "passed" else 2,
            stdout=_stage_payload(name, result=result),
            stderr=b"report unavailable" if result != "passed" else b"",
        )

    _payload, exit_code = s55.run_probe(probe_case, runner=runner)
    receipt = json.loads(probe_case.output.read_text(encoding="utf-8"))

    assert exit_code == 2
    assert receipt["result"] == expected_result
    assert receipt["direct_exit_code"] == 2
    assert receipt["not_measured_reason"]
    report = next(item for item in receipt["stages"] if item["name"] == "report")
    assert report["exit_code"] == 2
    assert report["stderr_size_bytes"] > 0


def _vm_observation(authority: dict[str, Any], now: datetime) -> dict[str, Any]:
    faces = {
        name: {**authority["faces"][name], "dirty": False}
        for name in ("vm_worker_state", "g1q3_rca_pipeline", "mcap_data_translate")
    }
    return {
        "schema_version": s55.VM_SCHEMA_VERSION,
        "observed_at": now.isoformat(),
        "release_id": authority["release_id"],
        "authority_sha256": canonical_json_sha256(authority),
        "faces": faces,
        "worker_probe": {
            "fixed_cli_path": "/vm/worker/bin/rca-fixed-cli",
            "fixed_cli_sha256": "8" * 64,
            "fixed_cli_exit_code": 0,
            "resource_class": "rca_prod",
            "input_materialized_bytes": 0,
            "task_created": False,
        },
        "report_resident": {
            "pid": 42001,
            "process_create_time": 1_785_000_000.0,
            "script": "/vm/pipeline/api/g1q3_rca/scripts/serve_rca_reports.py",
            "port": 18081,
            "pipeline_commit": authority["faces"]["g1q3_rca_pipeline"]["commit"],
            "pipeline_tree": authority["faces"]["g1q3_rca_pipeline"]["tree"],
            "pipeline_root": authority["faces"]["g1q3_rca_pipeline"]["root"],
        },
        "read_only_attestation": {
            "remote_mutation_performed": False,
            "task_submission_performed": False,
            "mcap_execution_performed": False,
            "external_effects_triggered": False,
        },
    }


def test_vm_observation_requires_exact_current_faces(probe_case: SimpleNamespace) -> None:
    now = datetime.now(timezone.utc)
    authority = probe_case.authority_value
    value = _vm_observation(authority, now)

    validated = s55._validate_vm_observation(
        value,
        authority=authority,
        authority_digest=canonical_json_sha256(authority),
        now=now,
    )
    assert validated["worker_probe"]["fixed_cli_exit_code"] == 0

    value["faces"]["g1q3_rca_pipeline"]["commit"] = "0" * 40
    with pytest.raises(s55.S55Error, match="pnc_rca_s55_vm_face_mismatch"):
        s55._validate_vm_observation(
            value,
            authority=authority,
            authority_digest=canonical_json_sha256(authority),
            now=now,
        )


def test_report_health_binds_authority_and_freshness(probe_case: SimpleNamespace) -> None:
    now = datetime.now(timezone.utc)
    authority = probe_case.authority_value
    body = {
        "schema_version": s55.REPORT_HEALTH_SCHEMA_VERSION,
        "ok": True,
        "observed_at": now.isoformat(),
        "release_id": authority["release_id"],
        "authority_sha256": canonical_json_sha256(authority),
        "root": authority["report_publication"]["root"],
        "manifest_schema_version": authority["report_publication"][
            "manifest_schema_version"
        ],
        "manifest_sha256": "9" * 64,
        "pid": 42001,
        "process_create_time": 1_785_000_000.0,
    }
    assert s55._validate_report_health(
        body,
        authority=authority,
        authority_digest=canonical_json_sha256(authority),
        now=now,
    )["ok"] is True

    body["observed_at"] = (now - timedelta(hours=1)).isoformat()
    with pytest.raises(s55.S55Error, match="pnc_rca_s55_report_health_stale"):
        s55._validate_report_health(
            body,
            authority=authority,
            authority_digest=canonical_json_sha256(authority),
            now=now,
        )


def test_current_chain_probe_does_not_restore_retired_execution() -> None:
    source = (Path(__file__).parents[2] / "scripts" / "pnc_rca_s55_probe.py").read_text(
        encoding="utf-8"
    )
    assert "g1q3_rca_e2e_smoke.py" not in source
    assert "mcap_service" not in source
    assert "docker run" not in source
    assert "historical_replay_performed" in source


def test_cli_invalid_arguments_return_direct_exit_two(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert s55.main([]) == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["result"] == "failed"
    assert payload["reason"] == "pnc_rca_s55_cli_arguments_invalid"
