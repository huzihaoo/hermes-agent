from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import subprocess

import pytest

from gateway import pnc_rca_storage_reservation as reservation_module
from gateway.pnc_rca_storage_reservation import (
    ASSUMED_CASES_PER_DAY,
    DEFAULT_SSH_MINI_AGENT,
    EXPECTED_INPUT_BYTES_PER_CASE,
    HFS_PATH,
    REMOTE_STORAGE_RESERVATION_MODULE,
    REMOTE_VM_REPO_ROOT,
    REQUESTED_CASES,
    RESERVATION_TTL_SECONDS,
    STORAGE_RESERVATION_RECEIPT_SCHEMA_VERSION,
    STORAGE_RESERVATION_REQUEST_SCHEMA_VERSION,
    StorageReservationError,
    StorageReservationRequest,
    TMP_PATH,
    reserve_storage_capacity,
    validate_storage_reservation_receipt,
)


SECRET_COMMAND = "mdi download event -u secret-download-id -s ./"
SUBMISSION_KEY = "g1q3-rca-s1-" + "a" * 64
OBSERVED_AT = "2026-07-10T12:00:00Z"


def _request(**updates) -> StorageReservationRequest:
    values = {
        "submission_key": SUBMISSION_KEY,
        "task_id": SUBMISSION_KEY,
        "business_key": "7008267126",
        "pdcl_download_cmd": SECRET_COMMAND,
        "artifact_root": f"/mnt/tmp/{SUBMISSION_KEY}/",
        "timeout_seconds": 17,
    }
    values.update(updates)
    return StorageReservationRequest(**values)


def _canonical_sha(value) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _capacity_policy() -> dict:
    return {
        "requested_cases": 1,
        "concurrency_reserve_cases": 1,
        "requested_cases_scope": "this_admission_capacity_reservation_only",
        "assumed_cases_per_day": 200,
        "assumed_cases_per_day_scope": "days_horizon_calculation_only",
        "expected_input_bytes_per_case": 8_400_000_000,
        "input_unit": "bytes",
        "gb_definition_bytes": 1_000_000_000,
        "reserve_ratio": 0.3,
        "reserve_percent": 30.0,
        "tmp_multiplier": 1.0,
        "hfs_multiplier": 2.25,
        "total_multiplier": 3.25,
    }


def _target(
    name: str,
    *,
    admitted: bool,
    outstanding: int = 0,
) -> dict:
    if name == "tmp":
        path = TMP_PATH
        multiplier = 1.0
        bytes_per_case = 8_400_000_000
        effective = 40_000_000_000 if admitted else 0
        horizon = 0.023 if admitted else 0.0
    else:
        path = HFS_PATH
        multiplier = 2.25
        bytes_per_case = 18_900_000_000
        effective = 80_000_000_000 if admitted else 0
        horizon = 0.021 if admitted else 0.0
    blocker = None if admitted else f"{name}_capacity_reserved_by_other_submissions"
    return {
        "name": name,
        "path": path,
        "observed_at": OBSERVED_AT,
        "multiplier": multiplier,
        "bytes_per_case": bytes_per_case,
        "required_bytes": bytes_per_case,
        "outstanding_held_bytes": outstanding,
        "effective_admittable_bytes": effective,
        "max_additional_cases_after_reservations": effective // bytes_per_case,
        "days_horizon_after_reservations": horizon,
        "ok_after_reservations": admitted,
        "reservation_blocker": blocker,
    }


def _receipt(
    request: StorageReservationRequest,
    *,
    status: str = "reserved",
    released_run_id: str = "",
    released_activated: bool = False,
    released_capacity_admitted: bool = True,
) -> dict:
    admitted = status in {"reserved", "active"}
    active = status == "active"
    waiting = status == "waiting_capacity"
    released = status == "released"
    capacity_admitted = (
        released_capacity_admitted if released else admitted
    )
    contract = request.contract()
    contract_sha256 = _canonical_sha(contract)
    reservation_id = "85d6f03f-c50e-420f-9c46-b60f958a03b4"
    requested = {
        "tmp": 8_400_000_000,
        "hfs": 18_900_000_000,
        "total": 27_300_000_000,
    }
    held = requested if admitted else {"tmp": 0, "hfs": 0, "total": 0}
    outstanding = 0 if capacity_admitted else 90_000_000_000
    targets = [
        _target("tmp", admitted=capacity_admitted, outstanding=outstanding),
        _target("hfs", admitted=capacity_admitted, outstanding=outstanding),
    ]
    capacity_blockers = [
        item["reservation_blocker"]
        for item in targets
        if item["reservation_blocker"]
    ]
    state_counts = {
        "reserved": 0,
        "active": 0,
        "waiting_capacity": 0,
        "released": 0,
        "expired": 0,
    }
    state_counts[status] = 1
    return {
        "schema_version": STORAGE_RESERVATION_RECEIPT_SCHEMA_VERSION,
        "request_schema_version": STORAGE_RESERVATION_REQUEST_SCHEMA_VERSION,
        "ok": admitted,
        "status": status,
        "reservation_id": reservation_id,
        "submission_key": request.submission_key,
        "contract_sha256": contract_sha256,
        "fence": 1,
        "operation": "reserve",
        "idempotent": active or released,
        "observed_at": OBSERVED_AT,
        "contract": contract,
        "reservation": {
            "reservation_id": reservation_id,
            "submission_key": request.submission_key,
            "contract_sha256": contract_sha256,
            "requested_cases": 1,
            "assumed_cases_per_day": 200,
            "expected_input_bytes_per_case": 8_400_000_000,
            "paths": {"tmp": TMP_PATH, "hfs": HFS_PATH},
            "reserve_ratio": 0.3,
            "requested_bytes": requested,
            "held_bytes": held,
            "state": status,
            "fence": 1,
            "run_id": (
                request.task_id
                if active
                else released_run_id
                if released
                else ""
            ),
            "created_at": "2026-07-10T11:59:00Z",
            "updated_at": OBSERVED_AT,
            "lease_expires_at": None if released else "2026-07-10T12:30:00Z",
            "activated_at": (
                OBSERVED_AT if active or (released and released_activated) else None
            ),
            "released_at": OBSERVED_AT if released else None,
        },
        "capacity": {
            "schema_version": STORAGE_RESERVATION_RECEIPT_SCHEMA_VERSION,
            "observed_at": OBSERVED_AT,
            "ok": capacity_admitted,
            "status": "pass" if capacity_admitted else "blocked",
            "blockers": capacity_blockers,
            "policy": _capacity_policy(),
            "required_bytes_total": 27_300_000_000,
            "max_additional_cases": 4,
            "days_horizon_at_assumed_cases_per_day": 0.021,
            "max_additional_cases_after_reservations": (
                4 if capacity_admitted else 0
            ),
            "days_horizon_after_reservations": (
                0.021 if capacity_admitted else 0.0
            ),
            "targets": targets,
            "outstanding_held_bytes": {
                "tmp": outstanding,
                "hfs": outstanding,
            },
        },
        "blocker": (
            {
                "kind": "storage_capacity_waiting",
                "retryable": True,
                "capacity_blockers": capacity_blockers,
            }
            if waiting
            else {
                "kind": "reservation_released_reconcile_only",
                "retryable": False,
                "reconcile_only": True,
                "create_allowed": False,
            }
            if released
            else None
        ),
        "health": {
            "state_counts": state_counts,
            "held_bytes": {
                "tmp": outstanding + held["tmp"],
                "hfs": outstanding + held["hfs"],
                "total": (outstanding * 2) + held["total"],
            },
            "recovered_expired_count": 0,
        },
    }


def test_request_builds_fixed_production_v1_without_secret_repr():
    request = _request()

    payload = request.payload()

    assert SECRET_COMMAND not in repr(request)
    assert payload == {
        "schema_version": "g1q3_rca_capacity_reservation_request_v1",
        "execution_identity": {
            "submission_key": SUBMISSION_KEY,
            "task_id": SUBMISSION_KEY,
            "business_key": "7008267126",
            "pdcl_download_cmd": SECRET_COMMAND,
            "artifact_root": f"/mnt/tmp/{SUBMISSION_KEY}/",
        },
        "capacity_policy": {
            "requested_cases": REQUESTED_CASES,
            "assumed_cases_per_day": ASSUMED_CASES_PER_DAY,
            "expected_input_bytes_per_case": EXPECTED_INPUT_BYTES_PER_CASE,
            "tmp_path": TMP_PATH,
            "hfs_path": HFS_PATH,
            "reserve_ratio": "0.30",
        },
        "ttl_seconds": RESERVATION_TTL_SECONDS,
    }
    assert payload["capacity_policy"]["requested_cases"] == 1
    assert request.contract()["execution_identity"]["artifact_root"] == (
        f"/mnt/tmp/{SUBMISSION_KEY}"
    )


def test_contract_matches_dispatch_execution_artifact_root_canonicalization():
    from scripts.pnc_rca_outbox_dispatcher import canonical_artifact_paths

    artifact_root, _ = canonical_artifact_paths(SUBMISSION_KEY)
    request = _request(artifact_root=artifact_root)

    assert artifact_root.endswith("/")
    assert request.payload()["execution_identity"]["artifact_root"] == artifact_root
    # VM v1 normalizes the execution request's path through pathlib.Path when
    # building the stable contract, so only the contract drops the slash.
    assert request.contract()["execution_identity"]["artifact_root"] == (
        artifact_root.rstrip("/")
    )


@pytest.mark.parametrize("status", ["reserved", "active"])
def test_strict_receipt_validation_admits_and_preserves_complete_receipt(status):
    request = _request()
    receipt = _receipt(request, status=status)

    decision = validate_storage_reservation_receipt(receipt, request)

    assert decision.admitted is True
    assert decision.blocked is False
    assert decision.status == status
    assert decision.receipt == receipt
    assert SECRET_COMMAND not in json.dumps(decision.receipt, ensure_ascii=False)


def test_waiting_capacity_is_a_valid_retryable_blocked_decision():
    request = _request()
    receipt = _receipt(request, status="waiting_capacity")

    decision = validate_storage_reservation_receipt(receipt, request)

    assert decision.admitted is False
    assert decision.blocked is True
    assert decision.receipt["blocker"]["retryable"] is True


@pytest.mark.parametrize("status", ["expired", "unknown"])
def test_unsupported_terminal_and_unknown_statuses_fail_closed(status):
    request = _request()
    receipt = _receipt(request)
    receipt["status"] = status
    receipt["reservation"]["state"] = status
    receipt["ok"] = False

    with pytest.raises(StorageReservationError) as caught:
        validate_storage_reservation_receipt(receipt, request)

    assert caught.value.code == "storage_reservation_status_invalid"


@pytest.mark.parametrize(
    ("mode", "capacity_admitted"),
    [
        ("released_before_activation", True),
        ("released_after_activation", True),
        ("released_from_waiting", False),
    ],
)
def test_released_receipt_is_valid_only_for_reconciliation(
    mode, capacity_admitted
):
    request = _request()
    bound = mode != "released_before_activation"
    receipt = _receipt(
        request,
        status="released",
        released_run_id=request.task_id if bound else "",
        released_activated=mode == "released_after_activation",
        released_capacity_admitted=capacity_admitted,
    )

    decision = validate_storage_reservation_receipt(receipt, request)

    assert decision.admitted is False
    assert decision.blocked is False
    assert decision.reconcile_only is True
    assert decision.status == "released"
    assert decision.receipt == receipt


@pytest.mark.parametrize(
    "mutate",
    [
        lambda receipt: receipt.update(idempotent=False),
        lambda receipt: receipt["blocker"].update(create_allowed=True),
        lambda receipt: receipt["reservation"].update(
            lease_expires_at="2026-07-10T12:30:00Z"
        ),
        lambda receipt: receipt["reservation"].update(released_at=None),
        lambda receipt: receipt["reservation"].update(run_id="other-task"),
    ],
)
def test_released_receipt_tampering_fails_closed(mutate):
    request = _request()
    receipt = _receipt(request, status="released")
    mutate(receipt)

    with pytest.raises(StorageReservationError):
        validate_storage_reservation_receipt(receipt, request)


@pytest.mark.parametrize(
    ("mutate", "error_code"),
    [
        (
            lambda receipt: receipt.update(schema_version="wrong-v1"),
            "storage_reservation_schema_invalid",
        ),
        (
            lambda receipt: receipt.update(submission_key="other"),
            "storage_reservation_identity_mismatch",
        ),
        (
            lambda receipt: receipt["contract"]["execution_identity"].update(
                business_key="other"
            ),
            "storage_reservation_contract_invalid",
        ),
        (
            lambda receipt: receipt.update(contract_sha256="f" * 64),
            "storage_reservation_contract_invalid",
        ),
        (
            lambda receipt: receipt["reservation"].update(fence=2),
            "storage_reservation_identity_mismatch",
        ),
        (
            lambda receipt: receipt["capacity"].update(required_bytes_total=1),
            "storage_reservation_schema_invalid",
        ),
        (
            lambda receipt: receipt["health"]["state_counts"].pop("expired"),
            "storage_reservation_schema_invalid",
        ),
    ],
)
def test_receipt_contract_identity_and_structure_fail_closed(mutate, error_code):
    request = _request()
    receipt = _receipt(request)
    mutate(receipt)

    with pytest.raises(StorageReservationError) as caught:
        validate_storage_reservation_receipt(receipt, request)

    assert caught.value.code == error_code
    assert SECRET_COMMAND not in caught.value.detail


def test_boundary_uses_only_fixed_absolute_wrapper_module_and_timeout():
    request = _request()
    expected = _receipt(request)
    captured = {}

    def fake_run(command, **kwargs):
        captured["command"] = command
        captured.update(kwargs)
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps(expected),
            stderr="",
        )

    decision = reserve_storage_capacity(request, run=fake_run)

    assert decision.admitted is True
    assert captured["command"] == [
        str(Path.home() / ".local" / "bin" / "ssh-mini-agent"),
        "run_py_json",
    ]
    assert captured["command"][0] == DEFAULT_SSH_MINI_AGENT
    assert Path(captured["command"][0]).is_absolute()
    assert captured["timeout"] == 17
    assert captured["env"]["SSH_MINI_AGENT_TIMEOUT"] == "17"
    assert captured["check"] is False
    assert captured["text"] is True
    assert captured["capture_output"] is True
    assert REMOTE_VM_REPO_ROOT in captured["input"]
    assert REMOTE_STORAGE_RESERVATION_MODULE in captured["input"]
    assert "reserve_execution_capacity(REQUEST)" in captured["input"]
    assert "sys.path.insert(0, str(REPO_ROOT))" in captured["input"]
    assert "subprocess" not in captured["input"]


def test_boundary_returns_full_waiting_receipt_without_collapsing_evidence():
    request = _request()
    waiting = _receipt(request, status="waiting_capacity")

    decision = reserve_storage_capacity(
        request,
        run=lambda command, **kwargs: subprocess.CompletedProcess(
            command, 0, stdout=json.dumps(waiting), stderr=""
        ),
    )

    assert decision.blocked is True
    assert decision.receipt == waiting
    assert decision.receipt["capacity"]["targets"]


def test_boundary_timeout_has_stable_non_sensitive_error_code():
    request = _request()

    def timeout(command, **kwargs):
        raise subprocess.TimeoutExpired(command, kwargs["timeout"])

    with pytest.raises(StorageReservationError) as caught:
        reserve_storage_capacity(request, run=timeout)

    assert caught.value.code == "storage_reservation_timeout"
    assert SECRET_COMMAND not in caught.value.detail


def test_boundary_os_and_nonzero_failures_have_stable_error_code():
    request = _request()

    with pytest.raises(StorageReservationError) as os_error:
        reserve_storage_capacity(
            request,
            run=lambda *args, **kwargs: (_ for _ in ()).throw(
                OSError("secret transport detail")
            ),
        )
    assert os_error.value.code == "storage_reservation_call_failed"
    assert "secret transport detail" not in os_error.value.detail

    with pytest.raises(StorageReservationError) as rc_error:
        reserve_storage_capacity(
            request,
            run=lambda command, **kwargs: subprocess.CompletedProcess(
                command,
                65,
                stdout=SECRET_COMMAND,
                stderr="secret stderr",
            ),
        )
    assert rc_error.value.code == "storage_reservation_call_failed"
    assert SECRET_COMMAND not in rc_error.value.detail
    assert "secret stderr" not in rc_error.value.detail


@pytest.mark.parametrize(
    "stdout",
    [
        "not-json",
        "[]",
        '{"schema_version":"a","schema_version":"b"}',
        '{"ok":NaN}',
    ],
)
def test_boundary_rejects_non_json_non_object_duplicate_and_non_finite(stdout):
    request = _request()

    with pytest.raises(StorageReservationError) as caught:
        reserve_storage_capacity(
            request,
            run=lambda command, **kwargs: subprocess.CompletedProcess(
                command, 0, stdout=stdout, stderr=""
            ),
        )

    assert caught.value.code == "storage_reservation_response_invalid"


def test_boundary_rejects_oversized_response(monkeypatch):
    request = _request()
    monkeypatch.setattr(reservation_module, "MAX_RESPONSE_BYTES", 10)

    with pytest.raises(StorageReservationError) as caught:
        reserve_storage_capacity(
            request,
            run=lambda command, **kwargs: subprocess.CompletedProcess(
                command, 0, stdout="{}" * 10, stderr=""
            ),
        )

    assert caught.value.code == "storage_reservation_response_invalid"


def test_boundary_propagates_stable_schema_error_from_validator():
    request = _request()
    wrong = _receipt(request)
    wrong["request_schema_version"] = "wrong-v1"

    with pytest.raises(StorageReservationError) as caught:
        reserve_storage_capacity(
            request,
            run=lambda command, **kwargs: subprocess.CompletedProcess(
                command, 0, stdout=json.dumps(wrong), stderr=""
            ),
        )

    assert caught.value.code == "storage_reservation_schema_invalid"


@pytest.mark.parametrize(
    "updates",
    [
        {"submission_key": "../escape"},
        {"artifact_root": "/mnt/tmp/other/"},
        {"pdcl_download_cmd": "mdi download x\nprint(secret)"},
        {"timeout_seconds": 121},
    ],
)
def test_request_identity_and_policy_input_fail_closed(updates):
    with pytest.raises(StorageReservationError) as caught:
        _request(**updates)

    assert caught.value.code == "storage_reservation_request_invalid"
    assert SECRET_COMMAND not in caught.value.detail


def test_decision_receipt_is_detached_from_mutable_boundary_payload():
    request = _request()
    original = _receipt(request)
    boundary_payload = deepcopy(original)

    decision = validate_storage_reservation_receipt(boundary_payload, request)
    boundary_payload["reservation"]["state"] = "tampered"

    assert decision.receipt == original
