from __future__ import annotations

from dataclasses import replace
import hashlib
import json
from pathlib import Path
import subprocess

import pytest

from gateway import pnc_rca_derived_capacity_reservation as reservation_module
from gateway.pnc_rca_derived_capacity_reservation import (
    CAPACITY_SCOPE,
    DERIVED_PRECREATE_ABORT_SCHEMA_VERSION,
    DERIVED_RESERVATION_CONTRACT_SCHEMA_VERSION,
    DERIVED_RESERVATION_RECEIPT_SCHEMA_VERSION,
    DERIVED_RESERVATION_REQUEST_SCHEMA_VERSION,
    DerivedCapacityReservationError,
    DerivedCapacityReservationRequest,
    HFS_PATH,
    TMP_PATH,
    abort_precreate_derived_capacity,
    canonical_data_access_sha256,
    reserve_derived_capacity,
    validate_derived_capacity_precreate_abort_receipt,
    validate_derived_capacity_reservation_receipt,
)


OBSERVED_AT = "2026-07-11T04:00:00+00:00"


def _data_access() -> dict:
    return {
        "schema_version": "g1q3_rca_remote_data_access_v1",
        "mode": "remote_read",
        "transport": "pdcl_pyclip",
        "references": [
            {
                "kind": "event",
                "event_uuid": "0190abcd-1111-2222-3333-444455556666",
                "reader_class": "RemoteEventReader",
            }
        ],
        "source": {
            "field": "问题数据地址_PDCL",
            "value_sha256": "b" * 64,
        },
        "reader_contract": {
            "distribution": "pdcl_pyclip",
            "required_version": "0.1.6+rca.2",
            "mdi_download_allowed": False,
            "fallback": "forbidden",
            "completeness": "full_requested_scope",
        },
    }


def _request(*, expected_bytes: int = 1_000_000_000):
    submission_key = "g1q3-rca-s1-" + "a" * 64
    return DerivedCapacityReservationRequest(
        submission_key=submission_key,
        task_id=submission_key,
        business_key="g1q3:issue:7041712812",
        data_access_sha256=canonical_data_access_sha256(_data_access()),
        artifact_root=f"/mnt/tmp/{submission_key}/",
        expected_artifact_cache_bytes=expected_bytes,
        timeout_seconds=17,
    )


def _sha256_json(value) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _byte_totals(tmp: int, hfs: int):
    return {"tmp": tmp, "hfs": hfs, "total": tmp + hfs}


def _receipt(request, *, status: str = "reserved", idempotent: bool = False):
    admitted = status in {"reserved", "active"}
    waiting = status == "waiting_capacity"
    released = status == "released"
    requested = request.requested_bytes
    total = _byte_totals(40_000_000_000, 0)
    if waiting:
        available = _byte_totals(12_000_000_000, 0)
        reserve = _byte_totals(12_000_000_000, 0)
        effective = _byte_totals(0, 0)
        blockers = ["task_output_publisher_insufficient_derived_capacity"]
    else:
        available = _byte_totals(40_000_000_000, 0)
        reserve = _byte_totals(12_000_000_000, 0)
        effective = _byte_totals(28_000_000_000, 0)
        blockers = []
    capacity = {
        "scope": CAPACITY_SCOPE,
        "atomic_reservation": True,
        "observed_at": OBSERVED_AT,
        "paths": {"tmp": TMP_PATH, "hfs": HFS_PATH},
        "reserve_ratio": "0.30",
        "required_bytes": requested,
        "total_bytes": total,
        "available_bytes": available,
        "reserve_bytes": reserve,
        "outstanding_held_bytes": _byte_totals(0, 0),
        "effective_admittable_bytes": effective,
        "admitted": not blockers,
        "blockers": blockers,
    }
    contract = request.contract()
    contract_sha256 = _sha256_json(contract)
    if admitted:
        held = requested
    else:
        held = _byte_totals(0, 0)
    if waiting:
        blocker = {
            "kind": "derived_capacity_waiting",
            "retryable": True,
            "capacity_blockers": blockers,
        }
    elif released:
        blocker = {
            "kind": "derived_capacity_reservation_released_reconcile_only",
            "retryable": False,
            "reconcile_only": True,
            "create_allowed": False,
        }
    else:
        blocker = None
    return {
        "schema_version": DERIVED_RESERVATION_RECEIPT_SCHEMA_VERSION,
        "request_schema_version": DERIVED_RESERVATION_REQUEST_SCHEMA_VERSION,
        "ok": admitted,
        "status": status,
        "reservation_id": "2d13a73f-a91c-4738-a3ae-98df25d23d2f",
        "submission_key": request.submission_key,
        "contract_sha256": contract_sha256,
        "fence": 1,
        "operation": "reserve",
        "idempotent": idempotent or released,
        "observed_at": OBSERVED_AT,
        "contract": contract,
        "reservation": {
            "reservation_id": "2d13a73f-a91c-4738-a3ae-98df25d23d2f",
            "submission_key": request.submission_key,
            "contract_sha256": contract_sha256,
            "state": status,
            "fence": 1,
            "run_id": request.task_id if status in {"active", "released"} else "",
            "requested_bytes": requested,
            "held_bytes": held,
            "created_at": OBSERVED_AT,
            "updated_at": OBSERVED_AT,
            "lease_expires_at": None if released else "2026-07-11T04:30:00+00:00",
            "activated_at": OBSERVED_AT if status in {"active", "released"} else None,
            "released_at": OBSERVED_AT if released else None,
        },
        "capacity": capacity,
        "blocker": blocker,
    }


def _precreate_abort_receipt(request, reservation_receipt, *, idempotent=False):
    return {
        "schema_version": DERIVED_PRECREATE_ABORT_SCHEMA_VERSION,
        "operation": "abort_precreate",
        "released": True,
        "idempotent": idempotent,
        "observed_at": OBSERVED_AT,
        "reservation_id": reservation_receipt["reservation_id"],
        "submission_key": request.submission_key,
        "task_id": request.task_id,
        "contract_sha256": reservation_receipt["contract_sha256"],
        "fence": reservation_receipt["fence"],
        "prior_state": "expired" if idempotent else "reserved",
        "state": "expired",
        "held_bytes": _byte_totals(0, 0),
    }


def test_data_access_hash_requires_sanitized_requested_scope_contract():
    access = _data_access()
    digest = canonical_data_access_sha256(access)

    assert len(digest) == 64
    assert digest == _sha256_json(access)

    for field, value in (
        ("required_version", "0.1.6"),
        ("completeness", "full_resource"),
        ("mdi_download_allowed", True),
    ):
        tampered = _data_access()
        tampered["reader_contract"][field] = value
        with pytest.raises(DerivedCapacityReservationError):
            canonical_data_access_sha256(tampered)

    mdi = _data_access()
    mdi["pdcl_download_cmd"] = "mdi download event -u secret"
    with pytest.raises(DerivedCapacityReservationError):
        canonical_data_access_sha256(mdi)


def test_request_binds_identity_hash_paths_and_configurable_budget_without_mdi():
    request = _request(expected_bytes=2_000_000_000)

    assert request.contract()["schema_version"] == (
        DERIVED_RESERVATION_CONTRACT_SCHEMA_VERSION
    )
    assert request.contract()["execution_identity"]["data_access_sha256"] == (
        canonical_data_access_sha256(_data_access())
    )
    assert request.requested_bytes == {
        "tmp": 2_000_000_000,
        "hfs": 4_500_000_000,
        "total": 6_500_000_000,
    }
    assert request.contract()["capacity_policy"]["tmp_path"] == "/mnt/tmp"
    assert request.contract()["capacity_policy"]["hfs_path"] == "/mnt/tmp"
    assert "perception_test_team" not in json.dumps(request.payload())
    serialized = json.dumps(request.payload(), sort_keys=True)
    assert "data_access_sha256" in serialized
    assert "event_uuid" not in serialized
    assert "mdi" not in serialized.lower()


@pytest.mark.parametrize(
    ("status", "admitted", "blocked", "reconcile_only"),
    [
        ("reserved", True, False, False),
        ("active", True, False, False),
        ("waiting_capacity", False, True, False),
        ("released", False, False, True),
    ],
)
def test_strict_receipt_validation_preserves_state_machine(
    status, admitted, blocked, reconcile_only
):
    request = _request()
    receipt = _receipt(request, status=status)

    decision = validate_derived_capacity_reservation_receipt(receipt, request)

    assert decision.status == status
    assert decision.admitted is admitted
    assert decision.blocked is blocked
    assert decision.reconcile_only is reconcile_only
    assert decision.receipt == receipt
    assert "mdi" not in json.dumps(decision.receipt, sort_keys=True).lower()


@pytest.mark.parametrize(
    "mutate",
    [
        lambda receipt: receipt.update(schema_version="wrong"),
        lambda receipt: receipt.update(submission_key="other"),
        lambda receipt: receipt.update(contract_sha256="f" * 64),
        lambda receipt: receipt["capacity"].update(atomic_reservation=False),
        lambda receipt: receipt["capacity"]["required_bytes"].update(tmp=1),
        lambda receipt: receipt["capacity"]["reserve_bytes"].update(tmp=1),
        lambda receipt: receipt["reservation"].update(fence=2),
        lambda receipt: receipt["reservation"].update(
            held_bytes={"tmp": 0, "hfs": 0, "total": 0}
        ),
        lambda receipt: receipt.update(pdcl_download_cmd="mdi event secret"),
    ],
)
def test_receipt_tampering_fails_closed(mutate):
    request = _request()
    receipt = _receipt(request)
    mutate(receipt)

    with pytest.raises(DerivedCapacityReservationError):
        validate_derived_capacity_reservation_receipt(receipt, request)


def test_receipt_timestamp_and_total_size_limits_fail_closed():
    request = _request()
    receipt = _receipt(request)
    receipt["observed_at"] = "2026-07-11T04:00:00." + "1" * 70_000 + "+00:00"

    with pytest.raises(DerivedCapacityReservationError):
        validate_derived_capacity_reservation_receipt(receipt, request)


def test_boundary_uses_fixed_module_and_returns_detached_receipt():
    request = _request()
    receipt = _receipt(request)
    captured = {}

    def fake_run(command, **kwargs):
        captured["command"] = command
        captured.update(kwargs)
        return subprocess.CompletedProcess(
            command, 0, stdout=json.dumps(receipt), stderr=""
        )

    decision = reserve_derived_capacity(request, run=fake_run)

    assert decision.status == "reserved"
    assert captured["command"] == [
        str(Path.home() / ".local" / "bin" / "ssh-mini-agent"),
        "run_py_json",
    ]
    assert captured["timeout"] == 17
    assert captured["env"]["SSH_MINI_AGENT_TIMEOUT"] == "17"
    assert reservation_module.REMOTE_DERIVED_RESERVATION_MODULE in captured["input"]
    assert "reserve_derived_capacity(REQUEST)" in captured["input"]
    assert "mdi" not in captured["input"].lower()

    receipt["status"] = "waiting_capacity"
    assert decision.receipt["status"] == "reserved"


def test_boundary_timeout_and_duplicate_json_fail_closed():
    request = _request()

    def timeout(command, **kwargs):
        raise subprocess.TimeoutExpired(command, kwargs["timeout"])

    with pytest.raises(
        DerivedCapacityReservationError,
        match="timed out",
    ):
        reserve_derived_capacity(request, run=timeout)

    def duplicate(command, **kwargs):
        del kwargs
        return subprocess.CompletedProcess(
            command,
            0,
            stdout='{"schema_version":"a","schema_version":"b"}',
            stderr="",
        )

    with pytest.raises(DerivedCapacityReservationError):
        reserve_derived_capacity(request, run=duplicate)

    deeply_nested = (
        '{"schema_version":'
        + "[" * 1_500
        + "0"
        + "]" * 1_500
        + "}"
    )

    def deep_response(command, **kwargs):
        del kwargs
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=deeply_nested,
            stderr="",
        )

    with pytest.raises(DerivedCapacityReservationError) as caught:
        reserve_derived_capacity(request, run=deep_response)
    assert caught.value.code == "derived_capacity_reservation_response_invalid"


def test_cyclic_receipt_fails_closed_without_recursive_scan_crash():
    request = _request()
    receipt = _receipt(request)
    cyclic: dict = {}
    cyclic["self"] = cyclic
    receipt["blocker"] = cyclic

    with pytest.raises(DerivedCapacityReservationError):
        validate_derived_capacity_reservation_receipt(receipt, request)


def test_precreate_abort_boundary_calls_fixed_vm_api_and_validates_receipt():
    request = _request()
    reserved = _receipt(request)
    aborted = _precreate_abort_receipt(request, reserved)
    captured = {}

    def fake_run(command, **kwargs):
        captured["command"] = command
        captured.update(kwargs)
        return subprocess.CompletedProcess(
            command, 0, stdout=json.dumps(aborted), stderr=""
        )

    receipt = abort_precreate_derived_capacity(request, reserved, run=fake_run)

    assert receipt == aborted
    assert "module.abort_precreate_derived_capacity(" in captured["input"]
    assert f'task_id=REQUEST["task_id"]' in captured["input"]
    assert f'contract_sha256=REQUEST["contract_sha256"]' in captured["input"]
    assert "mdi" not in captured["input"].lower()


def test_precreate_abort_boundary_rejects_nonreserved_source_or_forged_result():
    request = _request()
    with pytest.raises(DerivedCapacityReservationError):
        abort_precreate_derived_capacity(
            request,
            _receipt(request, status="active"),
            run=lambda *_args, **_kwargs: pytest.fail("must not call VM"),
        )

    reserved = _receipt(request)
    forged = _precreate_abort_receipt(request, reserved)
    forged["fence"] = 2
    with pytest.raises(DerivedCapacityReservationError):
        abort_precreate_derived_capacity(
            request,
            reserved,
            run=lambda command, **kwargs: subprocess.CompletedProcess(
                command, 0, stdout=json.dumps(forged), stderr=""
            ),
        )

    forged_boolean_fence = _precreate_abort_receipt(request, reserved)
    forged_boolean_fence["fence"] = True
    with pytest.raises(DerivedCapacityReservationError):
        validate_derived_capacity_precreate_abort_receipt(
            forged_boolean_fence, request, reserved
        )

    idempotent = _precreate_abort_receipt(request, reserved, idempotent=True)
    assert validate_derived_capacity_precreate_abort_receipt(
        idempotent, request, reserved
    ) == idempotent


def test_request_rejects_wrong_task_and_noncanonical_hash():
    request = _request()
    with pytest.raises(DerivedCapacityReservationError):
        replace(request, task_id="different")
    with pytest.raises(DerivedCapacityReservationError):
        replace(request, data_access_sha256="A" * 64)
