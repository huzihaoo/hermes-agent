from __future__ import annotations

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
