from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from scripts import pnc_rca_release_gate_suite as suite


COMMIT = "1" * 40
TREE = "2" * 40


def _write(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(value, str):
        path.write_text(value, encoding="utf-8")
    else:
        path.write_text(json.dumps(value), encoding="utf-8")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _matrix(task_root: Path) -> Path:
    acceptance = {}
    for criterion_id in suite.ALL_CRITERIA[:-1]:
        evidence = f"evidence/{criterion_id.replace('-', 'minus')}.json"
        _write(task_root / evidence, {"criterion_id": criterion_id})
        acceptance[criterion_id] = {
            "ga_status": "RED",
            "offline_status": "GREEN",
            "live_status": "NOT_EXECUTED",
            "evidence": [evidence],
        }
    acceptance["A-1"]["offline_status"] = "NOT_APPLICABLE"
    path = task_root / "matrix.json"
    _write(
        path,
        {
            "candidate": {
                "host": {"commit": COMMIT, "tree": TREE},
                "pipeline": {"commit": COMMIT, "tree": TREE},
            },
            "acceptance": acceptance,
        },
    )
    return path


@pytest.fixture
def gate_inputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Path]:
    task_root = tmp_path / "task"
    task_root.mkdir()
    matrix = _matrix(task_root)
    host = tmp_path / "host"
    pipeline = tmp_path / "pipeline"
    host.mkdir()
    pipeline.mkdir()
    registry = host / "registry.json"
    adapter = task_root / "adapter.py"
    _write(registry, {"registry": True})
    _write(adapter, "print('{}')\n")

    monkeypatch.setattr(
        suite,
        "_candidate_binding",
        lambda *_args, **_kwargs: {
            "status": "GREEN",
            "expected": {},
            "observed": {},
            "errors": [],
        },
    )
    monkeypatch.setattr(
        suite,
        "_scorecard_check",
        lambda *_args, **_kwargs: {"status": "GREEN", "errors": []},
    )
    monkeypatch.setattr(
        suite,
        "_freshness_registry_check",
        lambda *_args, **_kwargs: {
            "status": "GREEN",
            "high_confidence_ticket_enforced": True,
            "errors": [],
        },
    )
    monkeypatch.setattr(
        suite,
        "_cutover_plan_check",
        lambda *_args, **_kwargs: {
            "status": "GREEN",
            "production_mutation_performed": False,
            "errors": [],
        },
    )
    monkeypatch.setattr(
        suite,
        "_w17_checks",
        lambda *_args, **_kwargs: {
            "status": "GREEN",
            "active_evaluator_count": 67,
            "errors": [],
        },
    )
    monkeypatch.setattr(
        suite,
        "_w17_l_b_db_contract_check",
        lambda *_args, **_kwargs: {
            "status": "GREEN",
            "a17_l_b_status": "PARTIAL_285_OF_336",
            "active_evaluator_covered_count": 67,
            "accuracy_evidence": False,
            "errors": [],
        },
    )
    return {
        "task_root": task_root,
        "matrix": matrix,
        "host": host,
        "pipeline": pipeline,
        "registry": registry,
        "adapter": adapter,
    }


def _build(inputs: dict[str, Path], *, injection: str = "") -> dict:
    return suite.build_release_gate(
        matrix_path=inputs["matrix"],
        task_root=inputs["task_root"],
        host_source=inputs["host"],
        pipeline_source=inputs["pipeline"],
        cutover_adapter=inputs["adapter"],
        registry_path=inputs["registry"],
        pipeline_python=Path("/usr/bin/python3"),
        timeout_seconds=10,
        activation_intent=True,
        inject_auto_red=injection,
    )


def test_suite_emits_exact_automated_and_human_criteria(gate_inputs) -> None:
    report = _build(gate_inputs)

    assert [item["criterion_id"] for item in report["criteria"]] == list(
        suite.ALL_CRITERIA
    )
    statuses = {item["criterion_id"]: item for item in report["criteria"]}
    assert statuses["A7"]["status"] == "REQUIRES_HUMAN"
    assert statuses["A13"]["status"] == "REQUIRES_HUMAN"
    assert statuses["A7"]["mode"] == "requires_human"
    assert statuses["A13"]["mode"] == "requires_human"
    assert report["summary"]["automated_total"] == 13
    assert report["summary"]["automated_red"] == []
    assert report["activation_preflight"]["exit_code"] == 0
    assert report["activation_preflight"]["activation_dispatch_performed"] is False


def test_a_minus_one_uses_static_cutover_gate_not_not_applicable_offline_status(
    gate_inputs,
) -> None:
    report = _build(gate_inputs)
    a_minus_one = next(
        item for item in report["criteria"] if item["criterion_id"] == "A-1"
    )

    assert a_minus_one["matrix_offline_status"] == "NOT_APPLICABLE"
    assert a_minus_one["status"] == "GREEN"
    assert a_minus_one["checks"]["cutover_plan_only_adapter"]["status"] == "GREEN"
    assert "live_followup_required" in a_minus_one


def test_automatic_red_blocks_activation_intent(gate_inputs) -> None:
    report = _build(gate_inputs, injection="A4")

    assert report["summary"]["automated_red"] == ["A4"]
    assert report["activation_preflight"]["status"] == "BLOCKED_AUTOMATED_GATE_RED"
    assert report["activation_preflight"]["exit_code"] == 2
    assert (
        report["activation_preflight"][
            "external_cutover_may_proceed_to_human_capture"
        ]
        is False
    )
    assert report["activation_preflight"]["activation_dispatch_performed"] is False


def test_cli_negative_injection_exits_nonzero(
    gate_inputs, capsys: pytest.CaptureFixture[str]
) -> None:
    rc = suite.main(
        [
            "--matrix",
            str(gate_inputs["matrix"]),
            "--task-root",
            str(gate_inputs["task_root"]),
            "--host-source",
            str(gate_inputs["host"]),
            "--pipeline-source",
            str(gate_inputs["pipeline"]),
            "--cutover-adapter",
            str(gate_inputs["adapter"]),
            "--registry",
            str(gate_inputs["registry"]),
            "--activation-intent",
            "--inject-auto-red",
            "A17",
        ]
    )

    assert rc == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["summary"]["automated_red"] == ["A17"]
    assert payload["activation_preflight"]["activation_dispatch_performed"] is False


def test_missing_evidence_fails_closed(gate_inputs) -> None:
    matrix = json.loads(gate_inputs["matrix"].read_text(encoding="utf-8"))
    matrix["acceptance"]["A2"]["evidence"] = ["evidence/missing.json"]
    _write(gate_inputs["matrix"], matrix)

    report = _build(gate_inputs)
    a2 = next(item for item in report["criteria"] if item["criterion_id"] == "A2")

    assert a2["status"] == "RED"
    assert report["activation_preflight"]["exit_code"] == 2


def test_validator_rejects_red_report_that_allows_cutover(gate_inputs) -> None:
    report = _build(gate_inputs, injection="A0")
    report["activation_preflight"][
        "external_cutover_may_proceed_to_human_capture"
    ] = True

    with pytest.raises(suite.ReleaseGateError) as raised:
        suite.validate_release_gate(report)

    assert raised.value.code == "release_gate_red_not_blocking"


def test_v17_candidate_binding_separates_runtime_pipeline_from_w17_head(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    host = tmp_path / "host"
    pipeline = tmp_path / "pipeline"
    host.mkdir()
    pipeline.mkdir()
    runtime_commit = "3" * 40
    runtime_tree = "4" * 40
    w17_commit = "5" * 40
    w17_tree = "6" * 40
    worker_commit = "7" * 40
    worker_tree = "8" * 40
    matrix = {
        "candidate": {
            "host_runtime_cutover": {"commit": COMMIT, "tree": TREE},
            "pipeline_runtime_binding": {
                "commit": runtime_commit,
                "tree": runtime_tree,
            },
            "pipeline_w17_offline_evaluation": {
                "commit": w17_commit,
                "tree": w17_tree,
                "runtime_binding": False,
            },
            "worker_runtime": {"commit": worker_commit, "tree": worker_tree},
            "release_id": "rca-test-v17",
        }
    }

    def identity(path: Path, **_kwargs):
        if path == host:
            return {
                "path": str(host),
                "commit": COMMIT,
                "tree": TREE,
                "tracked_clean": True,
                "valid": True,
            }
        return {
            "path": str(pipeline),
            "commit": w17_commit,
            "tree": w17_tree,
            "tracked_clean": True,
            "valid": True,
        }

    monkeypatch.setattr(suite, "_git_identity", identity)
    monkeypatch.setattr(
        suite,
        "_run_command",
        lambda *_args, **_kwargs: {
            "returncode": 0,
            "stdout_tail": runtime_tree,
        },
    )

    result = suite._candidate_binding(
        matrix,
        host_source=host,
        pipeline_source=pipeline,
        timeout_seconds=10,
    )

    assert result["status"] == "GREEN"
    assert result["observed"]["pipeline_runtime_git_object"]["tree"] == runtime_tree
    assert result["observed"]["pipeline_w17_offline"]["commit"] == w17_commit


def test_candidate_reference_token_must_match_matrix_binding(tmp_path: Path) -> None:
    receipts, errors = suite._evidence_receipts(
        tmp_path,
        "A9",
        [f"pipeline offline commit {'5' * 40}"],
        allowed_reference_tokens=frozenset(
            {f"pipeline offline commit {'6' * 40}"}
        ),
    )

    assert receipts == []
    assert errors == ["matrix_candidate_reference_mismatch"]


def _w17_l_b_inputs(tmp_path: Path) -> tuple[dict, Path]:
    task_root = tmp_path / "task"
    task_root.mkdir()
    database = task_root / "source.sqlite3"
    database.write_bytes(b"bounded historical delivery database\n")
    pipeline_commit = "a" * 40
    pipeline_tree = "b" * 40
    inventory_sha = "c" * 64
    manifest = {
        "schema_version": "g1q3_w17_lb_db_contract_manifest_v1",
        "status": "PARTIAL",
        "candidate_binding": {
            "pipeline_commit": pipeline_commit,
            "pipeline_tree": pipeline_tree,
            "inventory_ledger_sha256": inventory_sha,
        },
        "source": {
            "db_path": str(database),
            "db_sha256": _sha256(database),
            "db_bytes": database.stat().st_size,
            "db_integrity": "ok",
            "db_open_mode": "read_only_immutable",
        },
        "accounting": {
            "active_evaluator_count": 67,
            "active_evaluator_covered_count": 67,
            "active_evaluator_missing_count": 0,
            "canonical_target_snapshot_count": 336,
            "contract_count": 286,
            "contract_failure_count": 0,
            "contract_missing_count": 1,
            "exact_match_count": 285,
        },
        "claims": {
            "l_b_active_key_coverage": True,
            "canonical_336_complete": False,
            "accuracy_evidence": False,
            "generalization_evidence": False,
            "high_confidence_dimension_evidence": False,
            "report_data_used_as_evaluator_input": False,
        },
        "policy": {
            "identity_persisted_in_public_manifest": False,
            "raw_report_payload_persisted": False,
            "report_file_locator_persisted": False,
        },
        "production_actions": [],
    }
    manifest_path = task_root / "manifest.json"
    _write(manifest_path, manifest)
    receipt = {
        "schema_version": "g1q3_w17_lb_db_contract_scan_receipt_v1",
        "status": "PARTIAL",
        "artifacts": {
            "manifest": {
                "path": str(manifest_path),
                "sha256": _sha256(manifest_path),
            }
        },
        "source_checks": {
            "db_sha256": _sha256(database),
            "inventory_ledger_sha256": inventory_sha,
            "report_contract_count": 286,
            "exact_match_count": 285,
            "contract_missing_count": 1,
            "contract_failure_count": 0,
        },
        "release_claim": {
            "a17_l_b": "PARTIAL_285_OF_336",
            "active_key_coverage": "GREEN_67_OF_67",
            "accuracy_evidence": False,
            "generalization_evidence": False,
            "high_confidence_dimension_evidence": False,
        },
        "verification": {
            "database_open_mode": "read_only_immutable",
            "mcap_read": False,
            "platform_or_kafka_required": False,
            "raw_report_payload_persisted": False,
            "negative_fingerprint_injection": {
                "exit_code": 2,
                "output_artifact_count": 0,
            },
        },
        "production_actions": [],
    }
    receipt_path = task_root / "receipt.json"
    _write(receipt_path, receipt)
    receipt_path.chmod(0o600)
    matrix = {
        "authority": {
            "w17_l_b_db_contract": {
                "manifest": {
                    "path": manifest_path.relative_to(task_root).as_posix(),
                    "sha256": _sha256(manifest_path),
                },
                "receipt": {
                    "path": receipt_path.relative_to(task_root).as_posix(),
                    "sha256": _sha256(receipt_path),
                },
            }
        },
        "candidate": {
            "pipeline_w17_offline_evaluation": {
                "commit": pipeline_commit,
                "tree": pipeline_tree,
            }
        },
    }
    return matrix, task_root


def test_w17_l_b_db_contract_accepts_hash_bound_partial_67_key_coverage(
    tmp_path: Path,
) -> None:
    matrix, task_root = _w17_l_b_inputs(tmp_path)

    result = suite._w17_l_b_db_contract_check(matrix, task_root=task_root)

    assert result["status"] == "GREEN"
    assert result["a17_l_b_status"] == "PARTIAL_285_OF_336"
    assert result["active_evaluator_covered_count"] == 67
    assert result["exact_match_count"] == 285
    assert result["accuracy_evidence"] is False


def test_w17_l_b_db_contract_rejects_accuracy_overclaim(tmp_path: Path) -> None:
    matrix, task_root = _w17_l_b_inputs(tmp_path)
    manifest_path = task_root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["claims"]["accuracy_evidence"] = True
    _write(manifest_path, manifest)
    receipt_path = task_root / "receipt.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["artifacts"]["manifest"]["sha256"] = _sha256(manifest_path)
    _write(receipt_path, receipt)
    receipt_path.chmod(0o600)
    matrix["authority"]["w17_l_b_db_contract"]["manifest"]["sha256"] = _sha256(
        manifest_path
    )
    matrix["authority"]["w17_l_b_db_contract"]["receipt"]["sha256"] = _sha256(
        receipt_path
    )

    result = suite._w17_l_b_db_contract_check(matrix, task_root=task_root)

    assert result["status"] == "RED"
    assert "w17_l_b_claim_boundary_invalid" in result["errors"]
