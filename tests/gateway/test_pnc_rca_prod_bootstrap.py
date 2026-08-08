from __future__ import annotations

import copy
import hashlib
import json
import subprocess
from datetime import datetime, timedelta, timezone

import pytest

from gateway import pnc_rca_prod_admission as admission
from gateway import pnc_rca_prod_bootstrap as bootstrap


NOW = datetime(2026, 7, 13, 1, 0, tzinfo=timezone.utc)
STARTED_AT = NOW - timedelta(hours=1)
DEADLINE = STARTED_AT + timedelta(days=8)
KEY = "hex:" + ("42" * 32)
TASK_ID = "g1q3-rca-s1-" + ("b" * 32)
EPOCH_ID = "rca-bootstrap-release-20260713"
RELEASE_BOM_SHA = "ab" * 32
ACTIVE_RELEASE_BINDING_SHA = "ac" * 32
CONTRACT_SHA = "bc" * 32
RESERVATION_SHA = "cd" * 32


def raw_authorization(**overrides) -> dict:
    values = {
        "bootstrap_epoch_id": EPOCH_ID,
        "started_at": STARTED_AT,
        "deadline": DEADLINE,
        "release_approval_id": "release-approval-20260713",
        "release_bom_sha256": RELEASE_BOM_SHA,
        "approval_evidence_sha256": "de" * 32,
        "authorized_by": "owner-user",
        "authorized_role": "owner",
        "now": NOW,
        "receipt_id": "bootstrap-authorization-1",
    }
    values.update(overrides)
    return bootstrap.issue_bootstrap_authorization(**values)


def normalized_authorization(**overrides) -> dict:
    raw = raw_authorization()
    normalized = bootstrap.validate_bootstrap_authorization(
        raw,
        now=NOW,
        expected_epoch_id=EPOCH_ID,
        expected_release_bom_sha256=RELEASE_BOM_SHA,
        authorization_receipt_sha256="ef" * 32,
    )
    normalized.update(overrides)
    return normalized


def snapshot(**overrides) -> dict:
    value = {
        "schema_version": admission.SNAPSHOT_SCHEMA_VERSION,
        "observed_at": NOW.isoformat(),
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
    value.update(overrides)
    return value


def resource_report(**authorization_overrides) -> dict:
    resource_snapshot = snapshot()
    return {
        "ok": True,
        "ok_for_submit": True,
        "ok_for_rca_prod_bootstrap_submit": True,
        "resource_class": "rca_prod_bootstrap",
        "reasons": [],
        "rca_prod_bootstrap_reasons": [],
        "rca_bootstrap_authorization": normalized_authorization(
            **authorization_overrides
        ),
        "rca_prod_snapshot": resource_snapshot,
        "rca_prod_snapshot_sha256": admission.sha256_value(resource_snapshot),
    }


def completed(value: dict) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        ["ssh-mini-resource"], 0, stdout=json.dumps(value), stderr="secret"
    )


def issue(**overrides):
    authorization = normalized_authorization()
    values = {
        "task_id": TASK_ID,
        "submission_key": TASK_ID,
        "goal": "# governed bootstrap RCA goal\n",
        "contract_sha256": CONTRACT_SHA,
        "reservation_id": "reservation-1",
        "reservation_fence": 7,
        "reservation_contract_sha256": RESERVATION_SHA,
        "hmac_key": KEY,
        "now": NOW,
        "attempt_id": "attempt-bootstrap-1",
        "receipt_id": "receipt-bootstrap-1",
        "capacity_mode": "bootstrap",
        "bootstrap_epoch_id": EPOCH_ID,
        "release_bom_sha256": RELEASE_BOM_SHA,
        "bootstrap_started_at": authorization["started_at"],
        "bootstrap_deadline": authorization["deadline"],
        "bootstrap_authorization_fingerprint": authorization[
            "receipt_fingerprint"
        ],
        "active_release_binding_sha256": ACTIVE_RELEASE_BINDING_SHA,
        "run_func": lambda *args, **kwargs: completed(resource_report()),
    }
    values.update(overrides)
    return admission.issue_rca_prod_admission(**values)


def test_authorization_is_exact_owner_only_fingerprinted_and_hard_deadlined():
    value = raw_authorization()
    assert set(value) == bootstrap.AUTHORIZATION_FIELDS
    assert set(value["release_approval"]) == bootstrap.RELEASE_APPROVAL_FIELDS
    assert set(value["policy"]) == bootstrap.POLICY_FIELDS
    assert value["expires_at"] == value["deadline"]
    assert value["policy"] == bootstrap._policy()
    assert value["policy"]["daily_started_attempt_quota"] is None
    assert value["receipt_fingerprint"] == bootstrap.authorization_fingerprint(value)

    legacy = copy.deepcopy(value)
    legacy["policy"]["daily_started_attempt_quota"] = 5
    legacy["receipt_fingerprint"] = bootstrap.authorization_fingerprint(legacy)
    with pytest.raises(
        bootstrap.RcaBootstrapAuthorizationError, match="policy_invalid"
    ):
        bootstrap.validate_bootstrap_authorization(legacy, now=NOW)

    with pytest.raises(
        bootstrap.RcaBootstrapAuthorizationError, match="owner_required"
    ):
        raw_authorization(authorized_role="admin")
    with pytest.raises(
        bootstrap.RcaBootstrapAuthorizationError, match="deadline_invalid"
    ):
        raw_authorization(deadline=STARTED_AT + timedelta(days=8, seconds=1))


def test_authorization_expiry_and_epoch_release_bindings_fail_closed():
    value = raw_authorization()
    with pytest.raises(bootstrap.RcaBootstrapAuthorizationError, match="expired"):
        bootstrap.validate_bootstrap_authorization(
            value,
            now=DEADLINE + timedelta(seconds=1),
            expected_epoch_id=EPOCH_ID,
            expected_release_bom_sha256=RELEASE_BOM_SHA,
        )
    with pytest.raises(bootstrap.RcaBootstrapAuthorizationError, match="epoch_binding"):
        bootstrap.validate_bootstrap_authorization(
            value,
            now=NOW,
            expected_epoch_id="rca-bootstrap-other",
            expected_release_bom_sha256=RELEASE_BOM_SHA,
        )
    with pytest.raises(
        bootstrap.RcaBootstrapAuthorizationError, match="release_bom_binding"
    ):
        bootstrap.validate_bootstrap_authorization(
            value,
            now=NOW,
            expected_epoch_id=EPOCH_ID,
            expected_release_bom_sha256="00" * 32,
        )
    with pytest.raises(
        bootstrap.RcaBootstrapAuthorizationError, match="release_approval_binding"
    ):
        bootstrap.validate_bootstrap_authorization(
            value,
            now=NOW,
            expected_epoch_id=EPOCH_ID,
            expected_release_bom_sha256=RELEASE_BOM_SHA,
            expected_release_approval_id="other-release",
        )
    with pytest.raises(
        bootstrap.RcaBootstrapAuthorizationError, match="approval_evidence_binding"
    ):
        bootstrap.validate_bootstrap_authorization(
            value,
            now=NOW,
            expected_epoch_id=EPOCH_ID,
            expected_release_bom_sha256=RELEASE_BOM_SHA,
            expected_approval_evidence_sha256="00" * 32,
        )
    with pytest.raises(bootstrap.RcaBootstrapAuthorizationError, match="expired"):
        bootstrap.validate_bootstrap_authorization(
            value,
            now=NOW - timedelta(seconds=6),
            expected_epoch_id=EPOCH_ID,
            expected_release_bom_sha256=RELEASE_BOM_SHA,
        )
    with pytest.raises(
        bootstrap.RcaBootstrapAuthorizationError, match="outside_epoch"
    ):
        raw_authorization(now=STARTED_AT - timedelta(seconds=1))


def test_canonical_authorization_loader_is_owner_only_and_raw_sha_bound(
    tmp_path, monkeypatch
):
    path = tmp_path / "rca-bootstrap-capacity-authorization.json"
    raw = json.dumps(
        raw_authorization(), sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    path.write_bytes(raw)
    path.chmod(0o600)
    monkeypatch.setattr(bootstrap, "BOOTSTRAP_AUTHORIZATION_PATH", path)

    value = bootstrap.load_bootstrap_authorization(
        now=NOW,
        expected_epoch_id=EPOCH_ID,
        expected_release_bom_sha256=RELEASE_BOM_SHA,
        expected_release_approval_id="release-approval-20260713",
        expected_approval_evidence_sha256="de" * 32,
    )

    assert value["authorization_receipt_sha256"] == hashlib.sha256(raw).hexdigest()
    assert value["receipt_fingerprint"] == raw_authorization()["receipt_fingerprint"]


def test_canonical_authorization_loader_rejects_permissions_links_and_duplicates(
    tmp_path, monkeypatch
):
    path = tmp_path / "rca-bootstrap-capacity-authorization.json"
    raw = json.dumps(raw_authorization(), separators=(",", ":")).encode("utf-8")
    path.write_bytes(raw)
    path.chmod(0o644)
    monkeypatch.setattr(bootstrap, "BOOTSTRAP_AUTHORIZATION_PATH", path)
    with pytest.raises(
        bootstrap.RcaBootstrapAuthorizationError, match="not_owner_only"
    ):
        bootstrap.load_bootstrap_authorization(
            now=NOW,
            expected_epoch_id=EPOCH_ID,
            expected_release_bom_sha256=RELEASE_BOM_SHA,
        )
    path.chmod(0o600)
    hardlink = tmp_path / "hardlink.json"
    hardlink.hardlink_to(path)
    with pytest.raises(
        bootstrap.RcaBootstrapAuthorizationError, match="not_owner_only"
    ):
        bootstrap.load_bootstrap_authorization(
            now=NOW,
            expected_epoch_id=EPOCH_ID,
            expected_release_bom_sha256=RELEASE_BOM_SHA,
        )
    hardlink.unlink()

    path.write_text('{"schema_version":"one","schema_version":"two"}')
    path.chmod(0o600)
    with pytest.raises(
        bootstrap.RcaBootstrapAuthorizationError, match="duplicate_key"
    ):
        bootstrap.load_bootstrap_authorization(
            now=NOW,
            expected_epoch_id=EPOCH_ID,
            expected_release_bom_sha256=RELEASE_BOM_SHA,
        )


def _active_release_binding_body(
    *, env_sha256: str, binding_path, live_env_path
) -> dict:
    return {
        "schema_version": bootstrap.ACTIVE_RELEASE_BINDING_SCHEMA_VERSION,
        "release_id": "release-approval-20260713",
        "authority_sha256": "34" * 32,
        "authority_epoch_id": "rca-authority-test-epoch",
        "complete": True,
        "live_write_performed": False,
        "bindings": {
            "release_bom_sha256": RELEASE_BOM_SHA,
            "release_approval": {"sha256": "de" * 32},
            "candidate_env": {"sha256": env_sha256},
            "bootstrap_authorization": {
                "sha256": "ef" * 32,
                "receipt_fingerprint": "12" * 32,
            },
        },
        "policy": {
            "capacity_admission": {
                "capacity_mode": "bootstrap",
                "bootstrap_epoch_id": EPOCH_ID,
                "bootstrap_authorization_fingerprint": "12" * 32,
                "bootstrap_authorization_sha256": "ef" * 32,
                "release_bom_sha256": RELEASE_BOM_SHA,
                "release_approval_id": "release-approval-20260713",
                "approval_evidence_sha256": "de" * 32,
            }
        },
        "side_effect_contract": {
            "canonical_active_release_binding": str(binding_path),
            "canonical_live_env": str(live_env_path),
        },
    }


def test_active_release_binding_pins_live_env_release_and_authorization(tmp_path):
    live_env = tmp_path / ".env"
    live_env.write_bytes(b"HERMES_RCA_PROD_CAPACITY_MODE=bootstrap\n")
    live_env.chmod(0o600)
    binding = tmp_path / bootstrap.ACTIVE_RELEASE_BINDING_NAME
    body = _active_release_binding_body(
        env_sha256=hashlib.sha256(live_env.read_bytes()).hexdigest(),
        binding_path=binding,
        live_env_path=live_env,
    )
    raw = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    binding.write_bytes(raw)
    binding.chmod(0o600)

    result = bootstrap.load_active_release_binding(
        path=binding,
        live_env_path=live_env,
        expected_release_id="release-approval-20260713",
        expected_epoch_id=EPOCH_ID,
    )

    assert result["binding_receipt_sha256"] == hashlib.sha256(raw).hexdigest()
    assert result["authorization_receipt_sha256"] == "ef" * 32
    assert result["authority_sha256"] == "34" * 32
    assert result["authority_epoch_id"] == "rca-authority-test-epoch"
    assert result["candidate_env_sha256"] == hashlib.sha256(
        live_env.read_bytes()
    ).hexdigest()


def test_active_release_binding_rejects_live_env_or_auth_identity_drift(tmp_path):
    live_env = tmp_path / ".env"
    live_env.write_bytes(b"HERMES_RCA_PROD_CAPACITY_MODE=bootstrap\n")
    live_env.chmod(0o600)
    binding = tmp_path / bootstrap.ACTIVE_RELEASE_BINDING_NAME
    body = _active_release_binding_body(
        env_sha256=hashlib.sha256(live_env.read_bytes()).hexdigest(),
        binding_path=binding,
        live_env_path=live_env,
    )
    binding.write_text(json.dumps(body, separators=(",", ":")))
    binding.chmod(0o600)
    live_env.write_bytes(b"HERMES_RCA_PROD_CAPACITY_MODE=steady\n")
    with pytest.raises(
        bootstrap.RcaBootstrapAuthorizationError, match="live_env_mismatch"
    ):
        bootstrap.load_active_release_binding(
            path=binding,
            live_env_path=live_env,
            expected_release_id="release-approval-20260713",
            expected_epoch_id=EPOCH_ID,
        )

    live_env.write_bytes(b"HERMES_RCA_PROD_CAPACITY_MODE=bootstrap\n")
    body["policy"]["capacity_admission"]["bootstrap_authorization_sha256"] = (
        "99" * 32
    )
    binding.write_text(json.dumps(body, separators=(",", ":")))
    binding.chmod(0o600)
    with pytest.raises(
        bootstrap.RcaBootstrapAuthorizationError, match="cross_binding"
    ):
        bootstrap.load_active_release_binding(
            path=binding,
            live_env_path=live_env,
            expected_release_id="release-approval-20260713",
            expected_epoch_id=EPOCH_ID,
        )


def test_functional_binding_can_ignore_unrelated_live_env_bytes_but_not_identity(
    tmp_path,
):
    live_env = tmp_path / ".env"
    live_env.write_bytes(b"HERMES_RCA_PROD_CAPACITY_MODE=bootstrap\n")
    live_env.chmod(0o600)
    binding = tmp_path / bootstrap.ACTIVE_RELEASE_BINDING_NAME
    body = _active_release_binding_body(
        env_sha256=hashlib.sha256(live_env.read_bytes()).hexdigest(),
        binding_path=binding,
        live_env_path=live_env,
    )
    binding.write_text(json.dumps(body, separators=(",", ":")))
    binding.chmod(0o600)
    live_env.write_bytes(b"UNRELATED_RUNTIME_SETTING=changed\n")

    result = bootstrap.load_active_release_binding(
        path=binding,
        live_env_path=live_env,
        expected_release_id="release-approval-20260713",
        expected_epoch_id=EPOCH_ID,
        verify_live_env=False,
    )

    assert result["release_bom_sha256"] == RELEASE_BOM_SHA
    body["policy"]["capacity_admission"][
        "bootstrap_authorization_sha256"
    ] = "99" * 32
    binding.write_text(json.dumps(body, separators=(",", ":")))
    with pytest.raises(
        bootstrap.RcaBootstrapAuthorizationError, match="cross_binding"
    ):
        bootstrap.load_active_release_binding(
            path=binding,
            live_env_path=live_env,
            expected_release_id="release-approval-20260713",
            expected_epoch_id=EPOCH_ID,
            verify_live_env=False,
        )


def test_issue_builds_distinct_signed_bootstrap_receipt_and_fixed_meta():
    captured = {}

    def runner(command, **kwargs):
        captured["command"] = command
        captured["env"] = kwargs["env"]
        return completed(resource_report())

    result = issue(run_func=runner)
    receipt = result.receipt
    assert set(receipt) == admission.BOOTSTRAP_RECEIPT_FIELDS
    assert set(receipt["bootstrap_authorization"]) == (
        admission.BOOTSTRAP_SIGNED_AUTHORIZATION_FIELDS
    )
    assert receipt["schema_version"] == admission.BOOTSTRAP_SCHEMA_VERSION
    assert receipt["capacity_mode"] == "bootstrap"
    assert receipt["bootstrap_authorization"]["daily_started_attempt_quota"] is None
    assert captured["command"][-2:] == ["--resource-class", "rca_prod_bootstrap"]
    assert admission.HMAC_ENV not in captured["env"]
    assert result.meta["rca_prod_capacity_mode"] == "bootstrap"
    assert result.meta["rca_prod_bootstrap_epoch_id"] == EPOCH_ID
    assert result.meta["rca_prod_bootstrap_daily_started_attempt_quota"] is None
    assert result.meta["rca_prod_release_bom_sha256"] == RELEASE_BOM_SHA
    assert (
        result.meta["rca_prod_active_release_binding_sha256"]
        == ACTIVE_RELEASE_BINDING_SHA
    )
    admission.validate_rca_prod_bootstrap_receipt(
        receipt,
        expected_bindings=receipt["bindings"],
        expected_epoch_id=EPOCH_ID,
        expected_release_bom_sha256=RELEASE_BOM_SHA,
        expected_active_release_binding_sha256=ACTIVE_RELEASE_BINDING_SHA,
        hmac_key=KEY,
        now=NOW,
    )


@pytest.mark.parametrize(
    "field,value,code",
    [
        ("max_concurrency", 2, "bootstrap_policy_invalid"),
        ("daily_started_attempt_quota", 5, "bootstrap_policy_invalid"),
        ("root_required_available_bytes", 400 * 1024**3, "bootstrap_policy_invalid"),
        ("delivery_required_available_bytes", 512 * 1024**3, "bootstrap_policy_invalid"),
        ("input_materialization", "allowed", "bootstrap_policy_invalid"),
    ],
)
def test_resource_authorization_policy_relaxation_fails_closed(field, value, code):
    report = resource_report(**{field: value})
    with pytest.raises(admission.RcaProdAdmissionError, match=code):
        issue(run_func=lambda *args, **kwargs: completed(report))


def test_snapshot_must_cover_bootstrap_task_budgets():
    for field, available in (
        ("root_available_bytes", bootstrap.ROOT_REQUIRED_AVAILABLE_BYTES - 1),
        (
            "delivery_available_bytes",
            bootstrap.DELIVERY_REQUIRED_AVAILABLE_BYTES - 1,
        ),
    ):
        report = resource_report()
        report["rca_prod_snapshot"][field] = available
        report["rca_prod_snapshot_sha256"] = admission.sha256_value(
            report["rca_prod_snapshot"]
        )
        with pytest.raises(admission.RcaProdAdmissionError, match="capacity_blocked"):
            issue(run_func=lambda *args, **kwargs: completed(report))


def test_control_state_blocks_same_epoch_authorization_window_refresh():
    replacement = resource_report(
        started_at=(STARTED_AT + timedelta(hours=1)).isoformat(),
        deadline=(DEADLINE + timedelta(hours=1)).isoformat(),
        receipt_fingerprint="99" * 32,
    )
    with pytest.raises(admission.RcaProdAdmissionError, match="authorization_invalid"):
        issue(run_func=lambda *args, **kwargs: completed(replacement))


def test_receipt_tamper_mode_confusion_and_old_shape_fail_closed():
    bootstrap_result = issue()
    steady_result = admission.issue_rca_prod_admission(
        task_id=TASK_ID,
        submission_key=TASK_ID,
        goal="# governed bootstrap RCA goal\n",
        contract_sha256=CONTRACT_SHA,
        reservation_id="reservation-1",
        reservation_fence=7,
        reservation_contract_sha256=RESERVATION_SHA,
        hmac_key=KEY,
        now=NOW,
        attempt_id="attempt-steady",
        receipt_id="receipt-steady",
        capacity_mode="steady",
        run_func=lambda *args, **kwargs: completed(
            {
                "ok": True,
                "ok_for_submit": True,
                "ok_for_rca_prod_submit": True,
                "resource_class": "rca_prod",
                "reasons": [],
                "rca_prod_reasons": [],
                "rca_capacity_authorization": {
                    "authorization_ready": True,
                    "status": "valid",
                    "reason_codes": [],
                    "receipt_id": "steady-1",
                    "receipt_fingerprint": "11" * 32,
                    "approval_evidence_sha256": "22" * 32,
                    "authorization_receipt_sha256": "33" * 32,
                    "expires_at": (NOW + timedelta(hours=1)).isoformat(),
                    "successful_sample_count": 20,
                    "input_materialized_sample_count": 0,
                    "root_required_available_bytes": 500 * 1024**3,
                    "delivery_required_available_bytes": 600 * 1024**3,
                },
                "rca_prod_snapshot": snapshot(),
                "rca_prod_snapshot_sha256": admission.sha256_value(snapshot()),
            }
        ),
    )
    with pytest.raises(admission.RcaProdAdmissionError, match="schema_invalid"):
        admission.validate_rca_prod_receipt(
            bootstrap_result.receipt,
            expected_bindings=bootstrap_result.receipt["bindings"],
            hmac_key=KEY,
            now=NOW,
        )
    with pytest.raises(admission.RcaProdAdmissionError, match="schema_invalid"):
        admission.validate_rca_prod_bootstrap_receipt(
            steady_result.receipt,
            expected_bindings=steady_result.receipt["bindings"],
            expected_epoch_id=EPOCH_ID,
            expected_release_bom_sha256=RELEASE_BOM_SHA,
            expected_active_release_binding_sha256=ACTIVE_RELEASE_BINDING_SHA,
            hmac_key=KEY,
            now=NOW,
        )
    old_shape = copy.deepcopy(bootstrap_result.receipt)
    old_shape.pop("capacity_mode")
    with pytest.raises(admission.RcaProdAdmissionError, match="schema_invalid"):
        admission.validate_rca_prod_bootstrap_receipt(
            old_shape,
            expected_bindings=bootstrap_result.receipt["bindings"],
            expected_epoch_id=EPOCH_ID,
            expected_release_bom_sha256=RELEASE_BOM_SHA,
            expected_active_release_binding_sha256=ACTIVE_RELEASE_BINDING_SHA,
            hmac_key=KEY,
            now=NOW,
        )
    tampered = copy.deepcopy(bootstrap_result.receipt)
    tampered["bootstrap_authorization"]["daily_started_attempt_quota"] = 99
    tampered = admission._sign_receipt(tampered, admission._load_hmac_key(KEY))
    with pytest.raises(admission.RcaProdAdmissionError, match="authorization_invalid"):
        admission.validate_rca_prod_bootstrap_receipt(
            tampered,
            expected_bindings=bootstrap_result.receipt["bindings"],
            expected_epoch_id=EPOCH_ID,
            expected_release_bom_sha256=RELEASE_BOM_SHA,
            expected_active_release_binding_sha256=ACTIVE_RELEASE_BINDING_SHA,
            hmac_key=KEY,
            now=NOW,
        )


def test_bootstrap_authorization_file_sha_is_bound_into_signed_receipt():
    result = issue()
    signed = result.receipt["bootstrap_authorization"]
    assert signed["authorization_receipt_sha256"] == "ef" * 32
    assert signed["active_release_binding_sha256"] == ACTIVE_RELEASE_BINDING_SHA
    body = admission.canonical_bytes(admission._receipt_body(result.receipt))
    assert result.receipt["receipt_fingerprint"] == hashlib.sha256(body).hexdigest()
    missing_file_sha = resource_report(authorization_receipt_sha256=None)
    with pytest.raises(admission.RcaProdAdmissionError, match="fingerprint_invalid"):
        issue(run_func=lambda *args, **kwargs: completed(missing_file_sha))


def test_active_release_binding_is_signed_and_tamper_fails_closed():
    result = issue()
    tampered = copy.deepcopy(result.receipt)
    tampered["bootstrap_authorization"]["active_release_binding_sha256"] = "ff" * 32
    tampered = admission._sign_receipt(tampered, admission._load_hmac_key(KEY))
    with pytest.raises(
        admission.RcaProdAdmissionError,
        match="authorization_invalid",
    ):
        admission.validate_rca_prod_bootstrap_receipt(
            tampered,
            expected_bindings=result.receipt["bindings"],
            expected_epoch_id=EPOCH_ID,
            expected_release_bom_sha256=RELEASE_BOM_SHA,
            expected_active_release_binding_sha256=ACTIVE_RELEASE_BINDING_SHA,
            hmac_key=KEY,
            now=NOW,
        )
