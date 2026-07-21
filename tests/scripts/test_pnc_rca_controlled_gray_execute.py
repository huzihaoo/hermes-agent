from __future__ import annotations

import ast
import copy
from datetime import timedelta
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from scripts import pnc_rca_controlled_gray as gray
from scripts import pnc_rca_controlled_gray_execute as execute
from tests.scripts import test_pnc_rca_controlled_gray as gray_fixture


NOW = gray_fixture.NOW


def _write_json(path: Path, body: dict, *, mode: int = 0o600) -> str:
    raw = json.dumps(body, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    path.write_bytes(raw)
    path.chmod(mode)
    return hashlib.sha256(raw).hexdigest()


def _authorization(plan: dict, plan_sha256: str) -> dict:
    body = {
        "schema_version": execute.AUTHORIZATION_SCHEMA_VERSION,
        "authorization_id": "gray-auth-fixture-1",
        "decision": execute.AUTHORIZATION_DECISION,
        "issued_at": (NOW - timedelta(seconds=30)).isoformat(),
        "expires_at": (NOW + timedelta(minutes=3)).isoformat(),
        "release_id": plan["bom"]["release_id"],
        "plan_sha256": plan_sha256,
        "bom_core_sha256": plan["bom_core_sha256"],
        "executor_sha256": execute._current_source_sha256(
            Path(execute.__file__), artifact="executor_source"
        ),
        "required_primitive_contract_sha256": (
            execute.PRODUCTION_PRIMITIVE_CONTRACT_SHA256
        ),
        "target": execute._expected_target(),
        "slots": execute._expected_slots(),
        "policy": execute._expected_policy(),
        "authorized_by": "owner-fixture",
        "authorized_role": "owner",
        "nonce": "a" * 64,
    }
    body["authorization_fingerprint"] = execute._authorization_fingerprint(body)
    return body


@pytest.fixture
def apply_inputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Path, Path, dict, dict]:
    spec = gray_fixture._spec(tmp_path, monkeypatch)
    plan = gray.evaluate(
        spec,
        now=NOW,
        resource_probe=lambda: gray_fixture._resource_report(),
    )
    assert plan["decision"] == "GO"
    plan_path = tmp_path / "gray-plan.json"
    plan_sha = _write_json(plan_path, plan)
    authorization = _authorization(plan, plan_sha)
    authorization_path = tmp_path / "gray-authorization.json"
    _write_json(authorization_path, authorization)
    return plan_path, authorization_path, plan, authorization


def _blocked_capacity() -> dict:
    report = gray_fixture._resource_report()
    report.update(
        ok_for_submit=False,
        ok_for_rca_prod_submit=False,
        reasons=["rca_capacity_model_not_ready"],
        rca_prod_reasons=["rca_capacity_model_not_ready"],
    )
    report["rca_capacity_authorization"] = {
        "schema_version": "context-rca-capacity-authorization/v1",
        "policy_version": "context-rca-capacity-model/2026-07-12-v1",
        "receipt_path": str(gray.CANONICAL_CAPACITY_AUTHORIZATION_PATH),
        "authorization_ready": False,
        "status": "missing",
        "reason_codes": ["rca_capacity_model_not_ready", "receipt_missing"],
    }
    return report


def test_apply_missing_regular_capacity_has_zero_effects_and_stops_first(
    apply_inputs, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan_path, authorization_path, _plan, _authorization_body = apply_inputs

    def primitive_probe_must_not_run():
        pytest.fail("primitive discovery must be after regular capacity admission")

    monkeypatch.setattr(execute, "_primitive_gaps", primitive_probe_must_not_run)
    result = execute.evaluate(
        mode="apply",
        plan_path=plan_path,
        authorization_path=authorization_path,
        now=NOW,
        resource_probe=_blocked_capacity,
    )

    assert result["decision"] == "NO_GO"
    assert result["status"] == "NO_GO_REGULAR_RCA_PROD_CAPACITY"
    assert result["production_closed"] is False
    assert result["completion_receipt"] is None
    assert result["authorization_claim"] is None
    assert result["production_effects"] == execute._effects()


def test_apply_revalidates_host_bom_before_capacity_probe(
    apply_inputs,
) -> None:
    plan_path, authorization_path, plan, _authorization_body = apply_inputs
    host_root = Path(plan["bom"]["components"]["host"]["root"])
    contract = host_root / "gateway/pnc_rca_delivery_contract.py"
    contract.write_text(
        contract.read_text(encoding="utf-8") + "\nDRIFT = True\n",
        encoding="utf-8",
    )

    result = execute.evaluate(
        mode="apply",
        plan_path=plan_path,
        authorization_path=authorization_path,
        now=NOW,
        resource_probe=lambda: pytest.fail("capacity probe must not run"),
    )

    assert result["decision"] == "NO_GO"
    assert result["status"] == "NO_GO_INPUT_OR_AUTHORIZATION"
    assert result["blockers"] == [
        "controlled_gray_execution_bom_revalidation_failed"
    ]
    assert result["production_effects"] == execute._effects()


def test_valid_capacity_stops_at_explicit_primitive_gap_without_claim(
    apply_inputs,
) -> None:
    plan_path, authorization_path, _plan, _authorization_body = apply_inputs

    result = execute.evaluate(
        mode="apply",
        plan_path=plan_path,
        authorization_path=authorization_path,
        now=NOW,
        resource_probe=lambda: gray_fixture._resource_report(),
    )

    assert result["decision"] == "NO_GO"
    assert result["status"] == "NO_GO_PRODUCTION_PRIMITIVE_GAP"
    assert result["blockers"] == list(execute.PRODUCTION_PRIMITIVE_GAPS)
    assert result["required_primitive_contract_sha256"] == (
        execute.PRODUCTION_PRIMITIVE_CONTRACT_SHA256
    )
    assert result["production_effects"] == execute._effects()
    assert result["authorization_claim"] is None
    assert result["completion_receipt"] is None


def test_validate_never_claims_production_closed(
    apply_inputs,
) -> None:
    plan_path, authorization_path, _plan, _authorization_body = apply_inputs

    result = execute.evaluate(
        mode="validate",
        plan_path=plan_path,
        authorization_path=authorization_path,
        now=NOW,
        resource_probe=lambda: gray_fixture._resource_report(),
    )

    assert result["decision"] == "NO_GO"
    assert result["production_closed"] is False
    assert result["completion_receipt"] is None
    assert result["production_effects"]["production_write_attempts"] == 0


def test_authorization_must_be_owner_only_before_capacity_probe(
    apply_inputs,
) -> None:
    plan_path, authorization_path, _plan, _authorization_body = apply_inputs
    authorization_path.chmod(0o644)

    result = execute.evaluate(
        mode="apply",
        plan_path=plan_path,
        authorization_path=authorization_path,
        now=NOW,
        resource_probe=lambda: pytest.fail("capacity probe must not run"),
    )

    assert result["status"] == "NO_GO_INPUT_OR_AUTHORIZATION"
    assert result["blockers"] == [
        "controlled_gray_execution_authorization_not_owner_only"
    ]
    assert result["production_effects"] == execute._effects()


@pytest.mark.parametrize(
    "mutation",
    [
        lambda body: body.update(bom_core_sha256="0" * 64),
        lambda body: body.update(plan_sha256="1" * 64),
        lambda body: body["slots"][1].update(manual_commit_allowed=True),
        lambda body: body["slots"][1].update(manual_seek_allowed=True),
        lambda body: body["policy"].update(max_concurrency=2),
        lambda body: body["policy"].update(direct_meegle_write_allowed=True),
        lambda body: body.update(expires_at=(NOW - timedelta(seconds=1)).isoformat()),
    ],
)
def test_authorization_binding_relaxation_is_no_go_before_capacity(
    apply_inputs,
    mutation,
) -> None:
    plan_path, authorization_path, _plan, authorization = apply_inputs
    mutated = copy.deepcopy(authorization)
    mutation(mutated)
    mutated["authorization_fingerprint"] = execute._authorization_fingerprint(
        mutated
    )
    _write_json(authorization_path, mutated)

    result = execute.evaluate(
        mode="apply",
        plan_path=plan_path,
        authorization_path=authorization_path,
        now=NOW,
        resource_probe=lambda: pytest.fail("capacity probe must not run"),
    )

    assert result["decision"] == "NO_GO"
    assert result["status"] == "NO_GO_INPUT_OR_AUTHORIZATION"
    assert result["production_effects"] == execute._effects()


def test_plan_no_go_can_never_be_upgraded_by_executor(
    apply_inputs,
) -> None:
    plan_path, authorization_path, plan, _authorization_body = apply_inputs
    blocked_plan = copy.deepcopy(plan)
    blocked_plan["decision"] = "NO_GO"
    blocked_plan["status"] = "NO_GO_REGULAR_RCA_PROD_CAPACITY"
    blocked_plan["blockers"] = [{"code": "missing_capacity"}]
    plan_sha = _write_json(plan_path, blocked_plan)
    authorization = _authorization(blocked_plan, plan_sha)
    _write_json(authorization_path, authorization)

    result = execute.evaluate(
        mode="apply",
        plan_path=plan_path,
        authorization_path=authorization_path,
        now=NOW,
        resource_probe=lambda: pytest.fail("capacity probe must not run"),
    )

    assert result["status"] == "NO_GO_INPUT_OR_AUTHORIZATION"
    assert result["production_closed"] is False


def test_parser_has_no_dry_run_or_go_override() -> None:
    parser = execute._parser()
    options = {
        option
        for action in parser._actions
        for option in action.option_strings
    }
    subcommands = parser._subparsers._group_actions[0].choices

    assert set(subcommands) == {"validate", "apply"}
    assert "--dry-run" not in options
    assert "--force" not in options
    assert "--capacity-authorization" not in options


def test_executor_direct_cli_help_and_missing_input_fail_closed(
    tmp_path: Path,
) -> None:
    script = Path(execute.__file__).resolve()
    environment = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
    help_result = subprocess.run(
        [sys.executable, str(script), "--help"],
        cwd=execute.REPO_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    blocked_result = subprocess.run(
        [
            sys.executable,
            str(script),
            "apply",
            "--plan",
            str(tmp_path / "missing-plan.json"),
            "--authorization",
            str(tmp_path / "missing-authorization.json"),
        ],
        cwd=execute.REPO_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert help_result.returncode == 0
    assert "{validate,apply}" in help_result.stdout
    assert blocked_result.returncode == 2
    blocked = json.loads(blocked_result.stdout)
    assert blocked["decision"] == "NO_GO"
    assert blocked["status"] == "NO_GO_INPUT_OR_AUTHORIZATION"
    assert blocked["production_effects"] == execute._effects()


def test_executor_source_has_no_direct_production_writer_or_kafka_cursor_calls() -> None:
    tree = ast.parse(Path(execute.__file__).read_text(encoding="utf-8"))
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }
    calls = {
        node.func.attr
        if isinstance(node.func, ast.Attribute)
        else node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, (ast.Attribute, ast.Name))
    }

    assert not any(name.startswith("sqlite3") for name in imported)
    assert not any(name.startswith("kafka") for name in imported)
    assert "pnc_rca_delivery_dispatcher" not in imported
    assert not hasattr(execute, "_claim_authorization")
    assert calls.isdisjoint(
        {
            "commit",
            "seek",
            "update_fields",
            "add_comment",
            "execute_sql",
            "mkdir",
            "fdopen",
            "fsync",
        }
    )
