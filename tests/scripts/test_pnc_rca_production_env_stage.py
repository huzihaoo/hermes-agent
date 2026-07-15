from __future__ import annotations

import base64
import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from dotenv import dotenv_values

from scripts import pnc_rca_production_env_stage as stage


NOW = datetime.now(timezone.utc).replace(microsecond=0)
TOPIC = "feishu-project-workfLow-event"
KAFKA_SECRET = "kafka secret ${literal}"
PROVIDER_SECRET = "provider secret value # literal"
ADMISSION_HMAC_SECRET = "hex:" + "ab" * 32
RELEASE_ID = "rca-prod-20260713-001"
RELEASE_BOM_SHA256 = "b" * 64
BOOTSTRAP_EPOCH_ID = "rca-bootstrap-release-20260713"


def _owner_write(path: Path, raw: bytes | str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw.encode("utf-8") if isinstance(raw, str) else raw)
    path.chmod(0o600)


def test_fixed_kafka_group_is_distinct_from_client_and_service_identity() -> None:
    assert stage.FIXED_PRODUCTION_VALUES["HERMES_RCA_KAFKA_GROUP"] == (
        "rca_root_cause_analysis_agent"
    )
    assert stage.FIXED_PRODUCTION_VALUES["HERMES_RCA_KAFKA_CLIENT_ID"] == (
        "root_cause_analysis_agent"
    )
    assert stage.FIXED_PRODUCTION_VALUES["HERMES_RCA_OUTBOX_SERVICE_ID"] == (
        "root_cause_analysis_agent"
    )


def _env_body(state_root: Path) -> str:
    values = {
        **stage.FIXED_PRODUCTION_VALUES,
        "HERMES_RCA_KAFKA_BOOTSTRAP_SERVERS": "broker-1:9092,broker-2:9092",
        "HERMES_RCA_KAFKA_USER": "rca_release_agent",
        "HERMES_RCA_KAFKA_TOPIC": TOPIC,
        "HERMES_RCA_KAFKA_EXPECTED_CLUSTER_ID": "cluster-production-1",
        "HERMES_RCA_KAFKA_PASSWORD": KAFKA_SECRET,
        "HERMES_RCA_PROD_ADMISSION_HMAC_KEY": ADMISSION_HMAC_SECRET,
        "HERMES_RCA_PROD_RELEASE_ID": RELEASE_ID,
        "HERMES_RCA_PROD_BOOTSTRAP_EPOCH_ID": BOOTSTRAP_EPOCH_ID,
        "HERMES_RCA_KAFKA_MIN_REPLICATION_FACTOR": "2",
        "HERMES_RCA_KAFKA_START_OFFSETS_JSON": '{"0":100}',
        "HERMES_RCA_KAFKA_CREATION_RULE_VERSION": "issue-created-v1",
        "HERMES_RCA_KAFKA_PROJECT_KEYS": "project-key",
        "HERMES_RCA_KAFKA_PROJECT_SIMPLE_NAMES": "t03o4q",
        "HERMES_RCA_KAFKA_WORK_ITEM_TYPE_KEYS": "problem-type",
        "HERMES_RCA_KAFKA_STATUS_CHANGE_TYPES": "Reached",
        "HERMES_RCA_KAFKA_STATE_TRANSITIONS_JSON": (
            '[{"state_key":"new-problem","pre_status":1,"cur_status":2}]'
        ),
        "HERMES_RCA_MANUAL_CHAT_IDS": (
            "oc_16614f4ba25b8c88b69c0b8e9ebc2fb5,"
            "oc_6cfc782212009ff4cd815349909dd423"
        ),
        "HERMES_RCA_MANUAL_OPERATOR_ENABLED": "true",
        "HERMES_RCA_MANUAL_OPERATOR_USER_IDS": "ou_debug_operator",
        "HERMES_RCA_MANUAL_OPERATOR_RATE_LIMIT": "3",
        "HERMES_RCA_MANUAL_OPERATOR_RATE_WINDOW_SECONDS": "600",
    }
    values.update(
        {
            key: str(state_root / filename)
            for key, filename in stage.STATE_PATH_NAMES.items()
        }
    )
    lines = [
        "# An unrelated provider credential and comment must survive byte-for-byte.\n",
        f"OPENROUTER_API_KEY='{PROVIDER_SECRET}'\n",
        f"HERMES_RCA_KAFKA_PASSWORD='{KAFKA_SECRET}'\n",
        f"HERMES_RCA_PROD_ADMISSION_HMAC_KEY='{ADMISSION_HMAC_SECRET}'\n",
    ]
    for key, value in values.items():
        if key not in {
            "HERMES_RCA_KAFKA_PASSWORD",
            "HERMES_RCA_PROD_ADMISSION_HMAC_KEY",
        }:
            lines.append(f"{key}={value}\n")
    lines.append("UNRELATED_FUTURE_SETTING='keep this too'\n")
    return "".join(lines)


def _json(path: Path, body: dict) -> bytes:
    raw = stage._canonical_json(body)
    _owner_write(path, raw)
    return raw


@pytest.fixture
def fixture(tmp_path: Path, monkeypatch) -> SimpleNamespace:
    secure = tmp_path / "secure"
    secure.mkdir(mode=0o700)
    state_root = tmp_path / "runtime" / "pnc_agent" / "feishu_issue_kafka_rca"
    release_dir = tmp_path / "release"
    release_dir.mkdir(mode=0o700)
    approval_request_sha256 = "a" * 64
    workspace_runtime_sha256 = "c" * 64
    future_runtime_sha256 = "d" * 64
    action_set_sha256 = stage._sha256_json(list(stage.RELEASE_ACTION_SET))
    approval = {
        "schema_version": stage.RELEASE_APPROVAL_SCHEMA_VERSION,
        "release_id": RELEASE_ID,
        "decision": stage.RELEASE_APPROVAL_DECISION,
        "created_at": (NOW - timedelta(minutes=5)).isoformat(),
        "expires_at": (NOW + timedelta(hours=1)).isoformat(),
        "nonce": "release-approval-nonce-001",
        "action_set": list(stage.RELEASE_ACTION_SET),
        "action_set_sha256": action_set_sha256,
        "approval_request_sha256": approval_request_sha256,
        "release_bom_sha256": RELEASE_BOM_SHA256,
        "workspace_runtime_sha256": workspace_runtime_sha256,
        "future_runtime_sha256": future_runtime_sha256,
        "runtime_config_sha256": "e" * 64,
        "t0_sha256": "f" * 64,
        "rollback_config_sha256": "1" * 64,
        "rollback_window_seconds": 3600,
        "identity": {
            "schema_version": stage.RELEASE_APPROVAL_IDENTITY_SCHEMA_VERSION,
            "method": stage.RELEASE_APPROVAL_IDENTITY_METHOD,
            "uid": os.geteuid(),
            "username": "release-owner",
            "machine_identity_source": "test_machine_identity",
            "machine_identity_sha256": "2" * 64,
        },
    }
    approval_path = release_dir / "release-approval.json"
    approval_raw = _json(approval_path, approval)
    approval_sha256 = hashlib.sha256(approval_raw).hexdigest()

    input_env = secure / "approved-input.env"
    _owner_write(
        input_env,
        _env_body(state_root),
    )

    bootstrap_path = secure / "rca-bootstrap-capacity-authorization.json"
    bootstrap_authorization = stage.prod_bootstrap.issue_bootstrap_authorization(
        bootstrap_epoch_id=BOOTSTRAP_EPOCH_ID,
        started_at=NOW - timedelta(minutes=5),
        deadline=NOW + timedelta(days=7),
        release_approval_id=RELEASE_ID,
        release_bom_sha256=RELEASE_BOM_SHA256,
        approval_evidence_sha256=approval_sha256,
        authorized_by="release-owner",
        authorized_role="owner",
        now=NOW,
        receipt_id="bootstrap-authorization-001",
    )
    _owner_write(
        bootstrap_path,
        json.dumps(
            bootstrap_authorization,
            sort_keys=True,
            separators=(",", ":"),
        ),
    )
    monkeypatch.setattr(
        stage.prod_bootstrap, "BOOTSTRAP_AUTHORIZATION_PATH", bootstrap_path
    )

    run_identity = {
        "schema_version": stage.RELEASE_PREPARE_RUN_IDENTITY_SCHEMA_VERSION,
        "release_id": RELEASE_ID,
        "plan_only": True,
        "inputs": {
            "env_file": {
                "path": str(input_env),
                "sha256": hashlib.sha256(input_env.read_bytes()).hexdigest(),
            }
        },
    }
    run_path = release_dir / stage.RELEASE_PREPARE_RUN_IDENTITY_NAME
    run_raw = _json(run_path, run_identity)
    manifest = {
        "schema_version": stage.RELEASE_PREPARE_MANIFEST_SCHEMA_VERSION,
        "release_id": RELEASE_ID,
        "complete": True,
        "plan_only": True,
        "run_identity": {
            "filename": stage.RELEASE_PREPARE_RUN_IDENTITY_NAME,
            "sha256": hashlib.sha256(run_raw).hexdigest(),
        },
        "approval_receipt_sha256": approval_sha256,
        "approval_request_sha256": approval_request_sha256,
        "release_bom_sha256": RELEASE_BOM_SHA256,
        "workspace_runtime_sha256": workspace_runtime_sha256,
        "future_runtime_sha256": future_runtime_sha256,
        "action_set_sha256": action_set_sha256,
    }
    manifest_path = release_dir / stage.RELEASE_PREPARE_MANIFEST_NAME
    _json(manifest_path, manifest)

    output_dir = tmp_path / "staged"
    output_dir.mkdir(mode=0o700)
    inputs = stage.StageInputs(
        input_env=input_env,
        release_prepare_manifest=manifest_path,
        approval_receipt=approval_path,
        output_env=output_dir / "candidate.env",
        receipt=output_dir / "candidate-env-receipt.json",
        runtime_state_root=state_root,
        expected_topic=TOPIC,
    )
    return SimpleNamespace(
        inputs=inputs,
        input_env=input_env,
        manifest_path=manifest_path,
        approval_path=approval_path,
        run_path=run_path,
        output_dir=output_dir,
        state_root=state_root,
        bootstrap_path=bootstrap_path,
    )


def _run(fixture: SimpleNamespace, phase: str = "plan") -> stage.StageResult:
    return stage.run_production_env_stage(
        phase=phase,
        inputs=fixture.inputs,
        now=NOW,
    )


def _rewrite_env(fixture: SimpleNamespace, transform) -> None:
    raw = fixture.input_env.read_text(encoding="utf-8")
    _owner_write(fixture.input_env, transform(raw))


def _rebind_input_env(fixture: SimpleNamespace) -> None:
    run = json.loads(fixture.run_path.read_text(encoding="utf-8"))
    run["inputs"]["env_file"]["sha256"] = hashlib.sha256(
        fixture.input_env.read_bytes()
    ).hexdigest()
    run_raw = _json(fixture.run_path, run)
    manifest = json.loads(fixture.manifest_path.read_text(encoding="utf-8"))
    manifest["run_identity"]["sha256"] = hashlib.sha256(run_raw).hexdigest()
    _json(fixture.manifest_path, manifest)


def test_plan_is_read_only_and_redacted(fixture: SimpleNamespace) -> None:
    result = _run(fixture)
    serialized = json.dumps(result.body, sort_keys=True)

    assert result.phase == "plan"
    assert result.body["ok"] is True
    assert result.body["side_effect_contract"]["canonical_live_write_supported"] is False
    assert not fixture.inputs.output_env.exists()
    assert not fixture.inputs.receipt.exists()
    assert KAFKA_SECRET not in serialized
    assert PROVIDER_SECRET not in serialized
    assert "broker-1" not in serialized


def test_stage_preserves_credentials_unknown_keys_and_comments(
    fixture: SimpleNamespace,
) -> None:
    result = _run(fixture, "stage")
    raw = fixture.inputs.output_env.read_text(encoding="utf-8")
    values = dotenv_values(fixture.inputs.output_env, interpolate=False)
    receipt = json.loads(fixture.inputs.receipt.read_text(encoding="utf-8"))

    assert result.output_written is True
    assert result.receipt_written is True
    assert values["HERMES_RCA_KAFKA_PASSWORD"] == KAFKA_SECRET
    assert values["HERMES_RCA_PROD_ADMISSION_HMAC_KEY"] == ADMISSION_HMAC_SECRET
    assert values["HERMES_RCA_PROD_CAPACITY_MODE"] == "bootstrap"
    assert values["HERMES_RCA_PROD_RELEASE_ID"] == RELEASE_ID
    assert values["HERMES_RCA_PROD_BOOTSTRAP_EPOCH_ID"] == BOOTSTRAP_EPOCH_ID
    assert "HERMES_RCA_PROD_RELEASE_BOM_SHA256" not in values
    assert "HERMES_RCA_PROD_RELEASE_APPROVAL_ID" not in values
    assert "HERMES_RCA_PROD_APPROVAL_EVIDENCE_SHA256" not in values
    assert RELEASE_BOM_SHA256 not in raw
    assert receipt["bindings"]["release_approval"]["sha256"] not in raw
    assert values["OPENROUTER_API_KEY"] == PROVIDER_SECRET
    assert values["UNRELATED_FUTURE_SETTING"] == "keep this too"
    assert "# An unrelated provider credential and comment" in raw
    assert values["HERMES_RCA_MANUAL_INTAKE_ENABLED"] == "true"
    assert values["HERMES_RCA_MANUAL_OPERATOR_ENABLED"] == "true"
    assert values["HERMES_RCA_OUTBOX_ALLOW_DOWNLOAD"] == "false"
    assert values["G1Q3_GOVERNANCE_DOWNLOAD_ENABLED"] == "false"
    assert values["HERMES_RCA_KAFKA_CONTROL_DB_PATH"] == str(
        fixture.state_root / "control.sqlite3"
    )
    assert stat_mode(fixture.inputs.output_env) == 0o600
    assert stat_mode(fixture.inputs.receipt) == 0o600
    assert fixture.inputs.output_env.stat().st_nlink == 1
    assert receipt["bindings"]["input_env"]["sha256"] == hashlib.sha256(
        fixture.input_env.read_bytes()
    ).hexdigest()
    assert receipt["bindings"]["release_prepare_manifest"]["sha256"] == (
        hashlib.sha256(fixture.manifest_path.read_bytes()).hexdigest()
    )
    assert receipt["bindings"]["release_approval"]["sha256"] == hashlib.sha256(
        fixture.approval_path.read_bytes()
    ).hexdigest()
    assert receipt["bindings"]["bootstrap_authorization"]["sha256"] == (
        hashlib.sha256(fixture.bootstrap_path.read_bytes()).hexdigest()
    )
    assert receipt["policy"]["capacity_admission"]["capacity_mode"] == "bootstrap"
    assert receipt["policy"]["capacity_admission"]["release_approval_id"] == (
        RELEASE_ID
    )
    assert receipt["side_effect_contract"]["canonical_active_release_binding"] == str(
        fixture.state_root / stage.prod_bootstrap.ACTIVE_RELEASE_BINDING_NAME
    )
    assert receipt["bindings"]["candidate_env"]["sha256"] == hashlib.sha256(
        fixture.inputs.output_env.read_bytes()
    ).hexdigest()
    assert KAFKA_SECRET not in fixture.inputs.receipt.read_text(encoding="utf-8")
    assert PROVIDER_SECRET not in fixture.inputs.receipt.read_text(encoding="utf-8")
    assert ADMISSION_HMAC_SECRET not in fixture.inputs.receipt.read_text(
        encoding="utf-8"
    )


def stat_mode(path: Path) -> int:
    return path.stat().st_mode & 0o777


def test_stage_is_idempotent_and_validate_recomputes_exact_bytes(
    fixture: SimpleNamespace,
) -> None:
    _run(fixture, "stage")
    second = _run(fixture, "stage")
    validated = _run(fixture, "validate")

    assert second.output_written is False
    assert second.receipt_written is False
    assert validated.phase == "validate"
    assert validated.body["receipt_sha256"] == hashlib.sha256(
        fixture.inputs.receipt.read_bytes()
    ).hexdigest()


def test_bootstrap_authorization_is_revalidated_and_bound_before_write(
    fixture: SimpleNamespace,
) -> None:
    authorization = json.loads(fixture.bootstrap_path.read_text(encoding="utf-8"))
    authorization["release_approval"]["approval_id"] = "other-release"
    _owner_write(
        fixture.bootstrap_path,
        json.dumps(authorization, sort_keys=True, separators=(",", ":")),
    )

    with pytest.raises(stage.ProductionEnvStageError) as error:
        _run(fixture)

    assert error.value.code.startswith("production_env_bootstrap_authorization_invalid:")
    assert not fixture.inputs.output_env.exists()
    assert not fixture.inputs.receipt.exists()


def test_bootstrap_mode_is_static_but_release_bindings_are_not_in_env(
    fixture: SimpleNamespace,
) -> None:
    _rewrite_env(
        fixture,
        lambda raw: raw.replace(
            "HERMES_RCA_PROD_CAPACITY_MODE=bootstrap",
            "HERMES_RCA_PROD_CAPACITY_MODE=steady",
        ),
    )
    _rebind_input_env(fixture)

    with pytest.raises(stage.ProductionEnvStageError) as error:
        _run(fixture)

    assert error.value.code == "production_env_approved_value_mismatch"
    assert not fixture.inputs.output_env.exists()


@pytest.mark.parametrize(
    ("transform", "code"),
    [
        (
            lambda raw: raw.replace(
                "HERMES_RCA_KAFKA_EXPECTED_CLUSTER_ID=cluster-production-1\n", ""
            ),
            "production_env_required_key_missing",
        ),
        (
            lambda raw: raw.replace(
                f"HERMES_RCA_PROD_ADMISSION_HMAC_KEY='{ADMISSION_HMAC_SECRET}'\n",
                "",
            ),
            "production_env_required_key_missing",
        ),
        (
            lambda raw: raw.replace(ADMISSION_HMAC_SECRET, "hex:" + "ab" * 31),
            "production_env_admission_hmac_key_too_short",
        ),
        (
            lambda raw: raw
            + "HERMES_RCA_KAFKA_TOPIC=feishu-project-workflow-event\n",
            "production_env_input_duplicate_key",
        ),
        (
            lambda raw: raw + "BAD-KEY=value\n",
            "production_env_input_key_invalid",
        ),
        (
            lambda raw: raw + "MISSING_VALUE\n",
            "production_env_input_value_missing",
        ),
        (
            lambda raw: raw.replace(
                f"HERMES_RCA_KAFKA_TOPIC={TOPIC}",
                "HERMES_RCA_KAFKA_TOPIC=wrong-topic",
            ),
            "production_env_topic_mismatch",
        ),
        (
            lambda raw: raw.replace(
                "HERMES_RCA_KAFKA_USER=rca_release_agent",
                "HERMES_RCA_KAFKA_USER=legacy-agent",
            ),
            "production_env_kafka_principal_invalid",
        ),
        (
            lambda raw: raw.replace(
                "HERMES_RCA_OUTBOX_ALLOW_DOWNLOAD=false",
                "HERMES_RCA_OUTBOX_ALLOW_DOWNLOAD=true",
            ),
            "production_env_approved_value_mismatch",
        ),
    ],
)
def test_dotenv_or_approved_value_failures_happen_before_write(
    fixture: SimpleNamespace, transform, code: str
) -> None:
    _rewrite_env(fixture, transform)

    with pytest.raises(stage.ProductionEnvStageError) as error:
        _run(fixture)

    # A changed input normally fails the release binding first. Rebind only for
    # parser/value tests so each intended boundary is exercised directly.
    if error.value.code == "production_env_input_sha_not_prepared":
        _rebind_input_env(fixture)
        with pytest.raises(stage.ProductionEnvStageError) as rebound_error:
            _run(fixture)
        assert rebound_error.value.code == code
    else:
        assert error.value.code == code
    assert not fixture.inputs.output_env.exists()
    assert not fixture.inputs.receipt.exists()


def test_base64_admission_hmac_key_is_accepted_and_preserved(
    fixture: SimpleNamespace,
) -> None:
    base64_secret = "base64:" + base64.b64encode(b"z" * 32).decode("ascii")
    _rewrite_env(
        fixture,
        lambda raw: raw.replace(ADMISSION_HMAC_SECRET, base64_secret),
    )
    _rebind_input_env(fixture)

    _run(fixture, "stage")
    values = dotenv_values(fixture.inputs.output_env, interpolate=False)

    assert values["HERMES_RCA_PROD_ADMISSION_HMAC_KEY"] == base64_secret
    assert base64_secret not in fixture.inputs.receipt.read_text(encoding="utf-8")


def test_input_env_must_match_release_prepare_identity(fixture: SimpleNamespace) -> None:
    _rewrite_env(fixture, lambda raw: raw + "NEW_UNKNOWN=value\n")

    with pytest.raises(stage.ProductionEnvStageError) as error:
        _run(fixture)

    assert error.value.code == "production_env_input_sha_not_prepared"


def test_approval_raw_sha_is_bound_by_manifest(fixture: SimpleNamespace) -> None:
    approval = json.loads(fixture.approval_path.read_text(encoding="utf-8"))
    approval["identity"]["username"] = "replacement-owner-name"
    _json(fixture.approval_path, approval)

    with pytest.raises(stage.ProductionEnvStageError) as error:
        _run(fixture)

    assert error.value.code == "production_env_approval_sha_mismatch"


def test_approval_bom_must_match_release_prepare_manifest(
    fixture: SimpleNamespace,
) -> None:
    approval = json.loads(fixture.approval_path.read_text(encoding="utf-8"))
    approval["release_bom_sha256"] = "9" * 64
    approval_raw = _json(fixture.approval_path, approval)
    manifest = json.loads(fixture.manifest_path.read_text(encoding="utf-8"))
    manifest["approval_receipt_sha256"] = hashlib.sha256(approval_raw).hexdigest()
    _json(fixture.manifest_path, manifest)

    with pytest.raises(stage.ProductionEnvStageError) as error:
        _run(fixture)

    assert error.value.code == "production_env_approval_binding_mismatch"


def test_expired_approval_fails_before_write(fixture: SimpleNamespace) -> None:
    approval = json.loads(fixture.approval_path.read_text(encoding="utf-8"))
    approval["created_at"] = (NOW - timedelta(hours=2)).isoformat()
    approval["expires_at"] = (NOW - timedelta(hours=1)).isoformat()
    approval_raw = _json(fixture.approval_path, approval)
    manifest = json.loads(fixture.manifest_path.read_text(encoding="utf-8"))
    manifest["approval_receipt_sha256"] = hashlib.sha256(approval_raw).hexdigest()
    _json(fixture.manifest_path, manifest)

    with pytest.raises(stage.ProductionEnvStageError) as error:
        _run(fixture)

    assert error.value.code == "production_env_approval_expired"


def test_canonical_live_env_is_forbidden_even_as_receipt(
    fixture: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(stage, "CANONICAL_LIVE_ENV", fixture.inputs.receipt)

    with pytest.raises(stage.ProductionEnvStageError) as error:
        _run(fixture)

    assert error.value.code == "production_env_stage_live_or_alias_path_forbidden"


def test_symlink_input_and_insecure_output_parent_fail_closed(
    fixture: SimpleNamespace,
) -> None:
    link = fixture.input_env.parent / "input-link.env"
    link.symlink_to(fixture.input_env)
    linked_inputs = stage.StageInputs(**{**vars(fixture.inputs), "input_env": link})
    with pytest.raises(stage.ProductionEnvStageError) as link_error:
        stage.run_production_env_stage(phase="plan", inputs=linked_inputs, now=NOW)
    assert link_error.value.code == "production_env_input_unavailable"

    fixture.output_dir.chmod(0o755)
    with pytest.raises(stage.ProductionEnvStageError) as parent_error:
        _run(fixture)
    assert parent_error.value.code == "production_env_output_parent_not_owner_only"


def test_hard_linked_input_is_rejected(fixture: SimpleNamespace) -> None:
    hard_link = fixture.input_env.parent / "second-name.env"
    os.link(fixture.input_env, hard_link)

    with pytest.raises(stage.ProductionEnvStageError) as error:
        _run(fixture)

    assert error.value.code == "production_env_input_not_owner_only"


def test_source_drift_is_rechecked_before_publication(
    fixture: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = stage._revalidate_source_bindings
    changed = False

    def drift(inputs, binding):
        nonlocal changed
        if not changed:
            changed = True
            _owner_write(fixture.input_env, fixture.input_env.read_bytes() + b"DRIFT=1\n")
        return original(inputs, binding)

    monkeypatch.setattr(stage, "_revalidate_source_bindings", drift)

    with pytest.raises(stage.ProductionEnvStageError) as error:
        _run(fixture, "stage")

    assert error.value.code == "production_env_stage_source_changed"
    assert not fixture.inputs.output_env.exists()
    assert not fixture.inputs.receipt.exists()


def test_existing_candidate_conflict_is_never_overwritten(fixture: SimpleNamespace) -> None:
    _owner_write(fixture.inputs.output_env, "DO_NOT_REPLACE=true\n")

    with pytest.raises(stage.ProductionEnvStageError) as error:
        _run(fixture, "stage")

    assert error.value.code == "production_env_candidate_conflict"
    assert fixture.inputs.output_env.read_text() == "DO_NOT_REPLACE=true\n"
    assert not fixture.inputs.receipt.exists()


def test_validate_detects_candidate_tamper(fixture: SimpleNamespace) -> None:
    _run(fixture, "stage")
    _owner_write(
        fixture.inputs.output_env,
        fixture.inputs.output_env.read_bytes() + b"TAMPERED=true\n",
    )

    with pytest.raises(stage.ProductionEnvStageError) as error:
        _run(fixture, "validate")

    assert error.value.code == "production_env_candidate_sha_mismatch"


def test_cli_never_prints_credentials(fixture: SimpleNamespace, capsys) -> None:
    args = [
        "plan",
        "--input-env",
        str(fixture.inputs.input_env),
        "--release-prepare-manifest",
        str(fixture.inputs.release_prepare_manifest),
        "--approval-receipt",
        str(fixture.inputs.approval_receipt),
        "--output-env",
        str(fixture.inputs.output_env),
        "--receipt",
        str(fixture.inputs.receipt),
        "--runtime-state-root",
        str(fixture.inputs.runtime_state_root),
        "--expected-topic",
        fixture.inputs.expected_topic,
    ]

    assert stage.main(args) == 0
    output = "".join(capsys.readouterr())
    assert KAFKA_SECRET not in output
    assert PROVIDER_SECRET not in output
    assert ADMISSION_HMAC_SECRET not in output
    assert "broker-1" not in output
