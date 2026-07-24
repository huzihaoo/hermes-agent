from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess

import pytest

from gateway import pnc_rca_prod_admission as admission


NOW = datetime(2026, 7, 13, 1, 0, tzinfo=timezone.utc)
KEY = "hex:" + ("42" * 32)
TASK_ID = "g1q3-rca-s1-" + ("a" * 32)
CONTRACT_SHA = "ab" * 32
RESERVATION_SHA = "cd" * 32


def snapshot(*, observed_at: datetime = NOW) -> dict:
    return {
        "schema_version": admission.SNAPSHOT_SCHEMA_VERSION,
        "observed_at": observed_at.isoformat(),
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


def report() -> dict:
    resource_snapshot = snapshot()
    return {
        "ok": True,
        "ok_for_submit": True,
        "ok_for_rca_prod_submit": True,
        "resource_class": "rca_prod",
        "reasons": [],
        "rca_prod_reasons": [],
        "rca_prod_snapshot": resource_snapshot,
        "rca_prod_snapshot_sha256": admission.sha256_value(resource_snapshot),
    }


def completed(value: dict, *, returncode: int = 0) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        ["ssh-mini-resource"],
        returncode,
        stdout=json.dumps(value),
        stderr="sensitive stderr must not be surfaced",
    )


def issue(**overrides):
    values = {
        "task_id": TASK_ID,
        "submission_key": TASK_ID,
        "goal": "# governed RCA goal\n",
        "contract_sha256": CONTRACT_SHA,
        "reservation_id": "reservation-1",
        "reservation_fence": 7,
        "reservation_contract_sha256": RESERVATION_SHA,
        "hmac_key": KEY,
        "now": NOW,
        "attempt_id": "attempt-1",
        "receipt_id": "receipt-1",
        "capacity_mode": "steady",
        "run_func": lambda *args, **kwargs: completed(report()),
    }
    values.update(overrides)
    return admission.issue_rca_prod_admission(**values)


def test_command_matches_vm_fixed_cli_contract():
    command = admission.build_rca_prod_command_argv(TASK_ID)
    assert command == [
        "./api/g1q3_rca/scripts/run_rca_service_request.py",
        "--task-id",
        TASK_ID,
        "--goal-path",
        f"/home/mini/.hermes/shared-state/tasks/{TASK_ID}/goal.md",
    ]


def test_issue_builds_exact_signed_receipt_and_strips_key_from_resource_env():
    captured = {}

    def runner(command, **kwargs):
        captured["command"] = command
        captured["env"] = kwargs["env"]
        return completed(report())

    result = issue(run_func=runner)
    receipt = result.receipt
    assert set(receipt) == admission.RECEIPT_FIELDS
    assert set(receipt["bindings"]) == admission.BINDING_FIELDS
    assert receipt["resource_policy"] == admission.live_resource_policy()
    assert set(receipt["resource_snapshot"]) == admission.SNAPSHOT_FIELDS
    assert captured["command"][-2:] == ["--resource-class", "rca_prod"]
    assert admission.HMAC_ENV not in captured["env"]
    assert result.meta["resource_class"] == "rca_prod"
    assert result.meta["queue_if_blocked"] is False
    assert result.meta["resource_gate_bypass"] is False
    assert result.meta["rca_prod_admission_receipt"] == receipt
    assert KEY not in json.dumps(result.meta)
    assert admission.validate_rca_prod_receipt(
        receipt,
        expected_bindings=receipt["bindings"],
        hmac_key=KEY,
        now=NOW,
    ) == receipt


@pytest.mark.parametrize(
    "runner,code",
    [
        (
            lambda *args, **kwargs: (_ for _ in ()).throw(
                subprocess.TimeoutExpired(args[0], 15, output="secret", stderr="secret")
            ),
            "rca_prod_resource_timeout",
        ),
        (
            lambda *args, **kwargs: subprocess.CompletedProcess(
                args[0], 0, stdout="not-json", stderr="secret"
            ),
            "rca_prod_resource_output_invalid",
        ),
        (
            lambda *args, **kwargs: completed({**report(), "resource_class": "vm_heavy"}),
            "rca_prod_resource_blocked",
        ),
        (
            lambda *args, **kwargs: completed({
                **report(),
                "ok_for_submit": False,
                "ok_for_rca_prod_submit": False,
                "reasons": ["blocked"],
                "rca_prod_reasons": ["blocked"],
            }),
            "rca_prod_resource_blocked",
        ),
    ],
)
def test_resource_wrapper_failures_are_stable_and_redacted(runner, code):
    with pytest.raises(admission.RcaProdAdmissionError) as raised:
        issue(run_func=runner)
    assert raised.value.code == code
    assert "secret" not in str(raised.value)


def test_stale_snapshot_and_snapshot_hash_tamper_fail_closed():
    stale = report()
    stale["rca_prod_snapshot"]["observed_at"] = (
        NOW - timedelta(seconds=121)
    ).isoformat()
    stale["rca_prod_snapshot_sha256"] = admission.sha256_value(
        stale["rca_prod_snapshot"]
    )
    with pytest.raises(admission.RcaProdAdmissionError, match="snapshot_stale"):
        issue(run_func=lambda *args, **kwargs: completed(stale))

    tampered = report()
    tampered["rca_prod_snapshot"]["root_available_bytes"] += 1
    with pytest.raises(admission.RcaProdAdmissionError, match="snapshot_hash_invalid"):
        issue(run_func=lambda *args, **kwargs: completed(tampered))


def test_live_resource_policy_tamper_fails_closed():
    result = issue()
    tampered = copy.deepcopy(result.receipt)
    tampered["resource_policy"]["max_concurrency"] = 2
    tampered = admission._sign_receipt(tampered, admission._load_hmac_key(KEY))
    with pytest.raises(admission.RcaProdAdmissionError, match="resource_policy_invalid"):
        admission.validate_rca_prod_receipt(
            tampered,
            expected_bindings=result.receipt["bindings"],
            hmac_key=KEY,
            now=NOW,
        )


def test_key_missing_bad_format_and_short_key_fail_before_resource(monkeypatch):
    calls = []
    runner = lambda *args, **kwargs: calls.append(args) or completed(report())
    monkeypatch.delenv(admission.HMAC_ENV, raising=False)
    for key in (None, "raw-secret", "hex:42"):
        with pytest.raises(admission.RcaProdAdmissionError, match="hmac_key_invalid"):
            issue(hmac_key=key, run_func=runner)
    assert calls == []


def test_receipt_binding_signature_and_unknown_field_tamper_fail_closed():
    result = issue()
    for mutated in (
        {**result.receipt, "unknown": True},
        {**result.receipt, "hmac_sha256": "00" * 32},
        {
            **result.receipt,
            "bindings": {**result.receipt["bindings"], "command_sha256": "00" * 32},
        },
    ):
        with pytest.raises(admission.RcaProdAdmissionError):
            admission.validate_rca_prod_receipt(
                mutated,
                expected_bindings=result.receipt["bindings"],
                hmac_key=KEY,
                now=NOW,
            )


def test_retry_uses_unique_attempt_and_receipt_and_existing_identity_is_historical():
    first = admission.issue_rca_prod_admission(
        task_id=TASK_ID,
        submission_key=TASK_ID,
        goal="# governed RCA goal\n",
        contract_sha256=CONTRACT_SHA,
        reservation_id="reservation-1",
        reservation_fence=7,
        reservation_contract_sha256=RESERVATION_SHA,
        hmac_key=KEY,
        now=NOW,
        capacity_mode="steady",
        run_func=lambda *args, **kwargs: completed(report()),
    )
    second = admission.issue_rca_prod_admission(
        task_id=TASK_ID,
        submission_key=TASK_ID,
        goal="# governed RCA goal\n",
        contract_sha256=CONTRACT_SHA,
        reservation_id="reservation-1",
        reservation_fence=7,
        reservation_contract_sha256=RESERVATION_SHA,
        hmac_key=KEY,
        now=NOW,
        capacity_mode="steady",
        run_func=lambda *args, **kwargs: completed(report()),
    )
    assert first.receipt["receipt_id"] != second.receipt["receipt_id"]
    assert first.meta["rca_prod_attempt_id"] != second.meta["rca_prod_attempt_id"]
    admission.validate_existing_rca_prod_meta(
        first.meta,
        task_id=TASK_ID,
        goal="# governed RCA goal\n",
        contract_sha256=CONTRACT_SHA,
        reservation_id="reservation-1",
        reservation_fence=7,
        reservation_contract_sha256=RESERVATION_SHA,
        hmac_key=KEY,
        now=NOW + timedelta(days=1),
        capacity_mode="steady",
    )
    tampered = copy.deepcopy(first.meta)
    tampered["rca_prod_attempt_id"] = "attempt-other"
    with pytest.raises(admission.RcaProdAdmissionError, match="binding_invalid"):
        admission.validate_existing_rca_prod_meta(
            tampered,
            task_id=TASK_ID,
            goal="# governed RCA goal\n",
            contract_sha256=CONTRACT_SHA,
            reservation_id="reservation-1",
            reservation_fence=7,
            reservation_contract_sha256=RESERVATION_SHA,
            hmac_key=KEY,
            now=NOW + timedelta(days=1),
            capacity_mode="steady",
        )


def test_command_drift_changes_bound_hash_and_old_receipt_is_rejected(monkeypatch):
    result = issue()
    original = admission.build_rca_prod_command_argv
    monkeypatch.setattr(
        admission,
        "build_rca_prod_command_argv",
        lambda task_id: [*original(task_id), "--drift"],
    )
    with pytest.raises(admission.RcaProdAdmissionError, match="existing_identity_invalid"):
        admission.validate_existing_rca_prod_meta(
            result.meta,
            task_id=TASK_ID,
            goal="# governed RCA goal\n",
            contract_sha256=CONTRACT_SHA,
            reservation_id="reservation-1",
            reservation_fence=7,
            reservation_contract_sha256=RESERVATION_SHA,
            hmac_key=KEY,
            now=NOW,
            capacity_mode="steady",
        )
