from __future__ import annotations

import copy
import hashlib
import hmac
import json
import os
import stat
import threading
from datetime import datetime, timedelta, timezone

import pytest

from gateway import pnc_rca_capacity_transition as transition
from gateway.pnc_rca_control_store import RcaControlStore


START = datetime(2026, 7, 13, 0, 0, tzinfo=timezone.utc)
RELEASE_ID = "release-20260713"
EPOCH_ID = "rca-bootstrap-release-20260713"
HMAC_KEY = b"capacity-transition-test-key-32b!"
RELEASE_BOM_SHA256 = "1" * 64
ACTIVE_RELEASE_BINDING_SHA256 = "2" * 64
EVIDENCE_BUNDLE_SHA256 = "3" * 64
EVIDENCE_BUNDLE_FINGERPRINT = "4" * 64


def digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def test_producer_deadline_window_has_exact_seven_day_six_hour_boundary():
    exact_deadline = START + transition.MIN_PRODUCER_DEADLINE_REMAINING
    detail = transition.validate_producer_deadline_window(
        activated_at=START,
        deadline=exact_deadline,
    )

    assert detail["remaining_seconds"] == 7.25 * 24 * 60 * 60
    assert detail["remaining_seconds"] == detail["minimum_remaining_seconds"]

    with pytest.raises(
        transition.CapacityTransitionError,
        match="rca_capacity_producer_window_insufficient",
    ):
        transition.validate_producer_deadline_window(
            activated_at=START + timedelta(microseconds=1),
            deadline=exact_deadline,
        )


def test_bootstrap_epoch_can_contain_producer_window_and_prepare_margin():
    from gateway import pnc_rca_prod_bootstrap as bootstrap

    assert (
        bootstrap.MAX_EPOCH_DURATION
        >= transition.MIN_PRODUCER_DEADLINE_REMAINING
    )


def test_producer_live_horizon_has_exact_seven_day_boundary():
    deadline = START + timedelta(days=8)
    exact_observation = deadline - transition.MIN_SAMPLE_WINDOW
    detail = transition.validate_producer_live_horizon(
        observed_at=exact_observation,
        deadline=deadline,
    )
    assert detail["remaining_seconds"] == timedelta(days=7).total_seconds()

    with pytest.raises(
        transition.CapacityTransitionError,
        match="rca_capacity_producer_horizon_insufficient",
    ):
        transition.validate_producer_live_horizon(
            observed_at=exact_observation + timedelta(microseconds=1),
            deadline=deadline,
        )


def sample(index: int, *, observed_at: datetime | None = None) -> dict:
    return transition.issue_capacity_sample(
        sample_id=f"sample-{index:03d}",
        release_id=RELEASE_ID,
        bootstrap_epoch_id=EPOCH_ID,
        release_bom_sha256=RELEASE_BOM_SHA256,
        active_release_binding_sha256=ACTIVE_RELEASE_BINDING_SHA256,
        task_id=f"g1q3-rca-{index:03d}",
        attempt_id=f"g1q3-rca-attempt-{index:03d}",
        admission_receipt_sha256=digest(f"receipt-{index}"),
        admission_receipt_fingerprint=digest(f"receipt-fingerprint-{index}"),
        task_manifest_sha256=digest(f"manifest-{index}"),
        producer_activation_receipt_sha256=digest("producer-activation"),
        producer_activation_receipt_fingerprint=digest(
            "producer-activation-fingerprint"
        ),
        vm_terminal_receipt_sha256=digest(f"vm-terminal-receipt-{index}"),
        vm_terminal_receipt_fingerprint=digest(
            f"vm-terminal-receipt-fingerprint-{index}"
        ),
        host_success_receipt_sha256=digest(f"host-success-receipt-{index}"),
        host_success_receipt_fingerprint=digest(
            f"host-success-receipt-fingerprint-{index}"
        ),
        terminal_status="succeeded",
        root_peak_bytes=1000 + index,
        delivery_peak_bytes=2000 + index,
        delivery_used_bytes=500 + index,
        input_materialized_bytes=0,
        observed_at=observed_at or START + timedelta(hours=6 * index),
        hmac_key=HMAC_KEY,
    )


def samples(count: int, *, spacing: timedelta = timedelta(hours=9)) -> list[dict]:
    return [
        sample(index, observed_at=START + spacing * index) for index in range(count)
    ]


def bootstrap_state() -> dict:
    initialized_at = (START - timedelta(minutes=1)).isoformat().replace("+00:00", "Z")
    return {
        "singleton_id": 1,
        "release_id": RELEASE_ID,
        "bootstrap_epoch_id": EPOCH_ID,
        "state": transition.BOOTSTRAP_PRODUCTION,
        "generation": 1,
        "final_ledger_sha256": None,
        "transition_authorization_sha256": None,
        "transition_authorization_fingerprint": None,
        "transition_receipt_sha256": None,
        "transition_receipt_fingerprint": None,
        "commit_marker_sha256": None,
        "commit_marker_fingerprint": None,
        "evidence_bundle_sha256": None,
        "evidence_bundle_fingerprint": None,
        "authorization_issued_at": None,
        "authorization_expires_at": None,
        "receipt_created_at": None,
        "marker_committed_at": None,
        "bootstrap_initialized_at": initialized_at,
        "steady_activated_at": None,
        "updated_at": initialized_at,
    }


def steady_state(ledger, authorization, receipt, marker, issued_at) -> dict:
    state = bootstrap_state()
    activated_at = (issued_at + timedelta(minutes=3)).isoformat().replace("+00:00", "Z")
    state.update({
        "state": transition.STEADY_ACTIVE,
        "generation": 2,
        "final_ledger_sha256": ledger.ledger_sha256,
        "transition_authorization_sha256": transition.sha256_canonical(authorization),
        "transition_authorization_fingerprint": authorization[
            "authorization_fingerprint"
        ],
        "transition_receipt_sha256": transition.sha256_canonical(receipt),
        "transition_receipt_fingerprint": receipt["receipt_fingerprint"],
        "commit_marker_sha256": transition.sha256_canonical(marker),
        "commit_marker_fingerprint": marker["marker_fingerprint"],
        "evidence_bundle_sha256": EVIDENCE_BUNDLE_SHA256,
        "evidence_bundle_fingerprint": EVIDENCE_BUNDLE_FINGERPRINT,
        "authorization_issued_at": authorization["issued_at"],
        "authorization_expires_at": authorization["expires_at"],
        "receipt_created_at": receipt["created_at"],
        "marker_committed_at": marker["committed_at"],
        "steady_activated_at": activated_at,
        "updated_at": activated_at,
    })
    return state


def transition_chain(persisted_state: dict | None = None):
    ledger = transition.validate_sample_ledger(samples(20), hmac_key=HMAC_KEY)
    issued_at = START + timedelta(hours=9 * 19, minutes=1)
    authorization = transition.issue_transition_authorization(
        ledger=ledger,
        authorization_id="steady-auth-1",
        approval_id="steady-approval-1",
        approval_evidence_sha256=digest("approval-evidence"),
        authorized_by="owner-user",
        authorized_role="owner",
        issued_at=issued_at,
        expires_at=issued_at + timedelta(minutes=30),
        persisted_state=persisted_state or bootstrap_state(),
        hmac_key=HMAC_KEY,
    )
    receipt = transition.issue_transition_receipt(
        ledger=ledger,
        authorization=authorization,
        receipt_id="steady-receipt-1",
        created_at=issued_at + timedelta(minutes=1),
        hmac_key=HMAC_KEY,
    )
    marker = transition.issue_steady_commit_marker(
        ledger=ledger,
        authorization=authorization,
        receipt=receipt,
        marker_id="steady-marker-1",
        committed_at=issued_at + timedelta(minutes=2),
        hmac_key=HMAC_KEY,
    )
    return ledger, authorization, receipt, marker, issued_at


def steady_intent(ledger, authorization, receipt, marker, issued_at):
    return transition.issue_steady_transition_intent(
        ledger=ledger,
        authorization=authorization,
        receipt=receipt,
        marker=marker,
        intent_id="steady-intent-1",
        business_activation_epoch_id="business-epoch-1",
        operator="owner-user",
        reason="qualified capacity samples approved for steady production",
        created_at=issued_at + timedelta(minutes=2),
        hmac_key=HMAC_KEY,
    )


def resign_sample(value: dict, *, key: bytes = HMAC_KEY) -> dict:
    return transition._sign_evidence(
        value,
        fingerprint_field="sample_fingerprint",
        hmac_field="sample_hmac_sha256",
        domain=transition.SAMPLE_HMAC_DOMAIN,
        hmac_key=key,
    )


def resolve_bootstrap(ledger, **evidence):
    return transition.resolve_effective_capacity(
        ledger=ledger,
        now=START + timedelta(days=8),
        hmac_key=HMAC_KEY,
        persisted_state=bootstrap_state(),
        **evidence,
    )


def test_sample_is_exact_schema_hmac_bound_and_materialization_free():
    value = sample(0)
    assert set(value) == transition.SAMPLE_FIELDS
    assert value["schema_version"] == transition.SAMPLE_SCHEMA_VERSION
    assert value["terminal_status"] == "succeeded"
    assert value["input_materialized_bytes"] == 0
    assert transition.validate_capacity_sample(value, hmac_key=HMAC_KEY) == value
    assert value["sample_hmac_sha256"] != digest(
        transition.canonical_bytes(value).decode("utf-8")
    )


def test_sample_requires_key_and_rejects_wrong_key_or_tamper():
    value = sample(0)
    for key in (b"", b"x" * 32):
        with pytest.raises(transition.CapacityTransitionError):
            transition.validate_capacity_sample(value, hmac_key=key)
    value["delivery_used_bytes"] += 1
    with pytest.raises(transition.CapacityTransitionError, match="signature_invalid"):
        transition.validate_capacity_sample(value, hmac_key=HMAC_KEY)


def test_sample_hmac_is_domain_separated_and_ledger_never_trusts_no_key():
    value = sample(0)
    body = transition._signed_body(
        value,
        fingerprint_field="sample_fingerprint",
        hmac_field="sample_hmac_sha256",
    )
    value["sample_hmac_sha256"] = hmac.new(HMAC_KEY, body, hashlib.sha256).hexdigest()
    with pytest.raises(transition.CapacityTransitionError, match="signature_invalid"):
        transition.validate_sample_ledger([value], hmac_key=HMAC_KEY)
    with pytest.raises(transition.CapacityTransitionError, match="hmac_key_required"):
        transition.validate_sample_ledger([sample(0)], hmac_key=None)


@pytest.mark.parametrize(
    ("field", "replacement", "error"),
    [
        ("terminal_status", "failed", "terminal_not_successful"),
        ("input_materialized_bytes", 1, "input_materialized_nonzero"),
        ("delivery_used_bytes", 3000, "delivery_metrics_invalid"),
    ],
)
def test_sample_policy_is_checked_even_with_valid_hmac(field, replacement, error):
    value = sample(0)
    value[field] = replacement
    value = resign_sample(value)
    with pytest.raises(transition.CapacityTransitionError, match=error):
        transition.validate_capacity_sample(value, hmac_key=HMAC_KEY)


@pytest.mark.parametrize(
    "field",
    [
        "sample_id",
        "task_id",
        "attempt_id",
        "admission_receipt_sha256",
        "admission_receipt_fingerprint",
        "task_manifest_sha256",
        "vm_terminal_receipt_sha256",
        "vm_terminal_receipt_fingerprint",
        "host_success_receipt_sha256",
        "host_success_receipt_fingerprint",
    ],
)
def test_ledger_rejects_reused_underlying_evidence(field):
    first, second = sample(0), sample(1)
    second[field] = first[field]
    second = resign_sample(second)
    with pytest.raises(transition.CapacityTransitionError, match="duplicate"):
        transition.validate_sample_ledger([first, second], hmac_key=HMAC_KEY)


@pytest.mark.parametrize(
    "field",
    [
        "producer_activation_receipt_sha256",
        "producer_activation_receipt_fingerprint",
    ],
)
def test_ledger_requires_one_epoch_producer_activation_receipt(field):
    first, second = sample(0), sample(1)
    second[field] = digest(f"different-{field}")
    second = resign_sample(second)
    with pytest.raises(
        transition.CapacityTransitionError,
        match="producer_activation_binding_mismatch",
    ):
        transition.validate_sample_ledger([first, second], hmac_key=HMAC_KEY)


def test_nineteen_stays_bootstrap_and_twenty_becomes_ready():
    empty = transition.validate_sample_ledger([], hmac_key=HMAC_KEY)
    nineteen = transition.validate_sample_ledger(samples(19), hmac_key=HMAC_KEY)
    twenty = transition.validate_sample_ledger(samples(20), hmac_key=HMAC_KEY)
    assert not nineteen.steady_qualified
    assert twenty.steady_qualified
    assert resolve_bootstrap(empty)["state"] == transition.BOOTSTRAP_PRODUCTION
    assert resolve_bootstrap(nineteen)["state"] == transition.BOOTSTRAP_PRODUCTION
    ready = resolve_bootstrap(twenty)
    assert ready["state"] == transition.STEADY_READY
    assert ready["capacity_mode"] == "bootstrap"


def test_sample_count_and_window_boundaries_are_inclusive():
    exact_seven = transition.validate_sample_ledger(
        [
            sample(index, observed_at=START + timedelta(days=7) * index / 19)
            for index in range(20)
        ],
        hmac_key=HMAC_KEY,
    )
    exact_thirty_one = transition.validate_sample_ledger(
        samples(32, spacing=timedelta(days=1)), hmac_key=HMAC_KEY
    )
    two_hundred = transition.validate_sample_ledger(
        [
            sample(index, observed_at=START + timedelta(days=7) * index / 199)
            for index in range(200)
        ],
        hmac_key=HMAC_KEY,
    )
    assert exact_seven.steady_qualified
    assert exact_thirty_one.steady_qualified
    assert two_hundred.steady_qualified
    with pytest.raises(transition.CapacityTransitionError, match="too_many_samples"):
        transition.validate_sample_ledger(samples(201), hmac_key=HMAC_KEY)


def test_over_thirty_one_days_and_gap_over_twenty_four_hours_are_ineligible():
    above = transition.validate_sample_ledger(
        samples(33, spacing=timedelta(days=1)), hmac_key=HMAC_KEY
    )
    assert not above.steady_qualified
    values = samples(20)
    for index in range(10, 20):
        values[index] = sample(
            index, observed_at=START + timedelta(hours=9 * index + 25)
        )
    with pytest.raises(transition.CapacityTransitionError, match="gap_exceeded"):
        transition.validate_sample_ledger(values, hmac_key=HMAC_KEY)


def test_ledger_append_is_atomic_owner_only_and_idempotent(tmp_path):
    path = tmp_path / "capacity-samples.jsonl"
    first = transition.append_capacity_sample(
        path, sample(0), hmac_key=HMAC_KEY, persisted_state_loader=bootstrap_state
    )
    second = transition.append_capacity_sample(
        path, sample(1), hmac_key=HMAC_KEY, persisted_state_loader=bootstrap_state
    )
    assert (first.sample_count, second.sample_count) == (1, 2)
    assert path.stat().st_mode & 0o777 == 0o600
    assert transition.read_sample_ledger(path, hmac_key=HMAC_KEY) == second
    assert (
        transition.append_capacity_sample(
            path, sample(1), hmac_key=HMAC_KEY, persisted_state_loader=bootstrap_state
        )
        == second
    )


def test_failed_copy_on_write_leaves_previous_ledger_intact(tmp_path, monkeypatch):
    path = tmp_path / "capacity-samples.jsonl"
    first = transition.append_capacity_sample(
        path, sample(0), hmac_key=HMAC_KEY, persisted_state_loader=bootstrap_state
    )
    original_write = transition.os.write
    calls = 0

    def torn_write(descriptor, raw):
        nonlocal calls
        calls += 1
        if calls == 1:
            return original_write(descriptor, raw[: max(1, len(raw) // 2)])
        raise OSError(28, "simulated ENOSPC")

    monkeypatch.setattr(transition.os, "write", torn_write)
    with pytest.raises(transition.CapacityTransitionError, match="transaction_write"):
        transition.append_capacity_sample(
            path, sample(1), hmac_key=HMAC_KEY, persisted_state_loader=bootstrap_state
        )
    assert transition.read_sample_ledger(path, hmac_key=HMAC_KEY) == first


def test_failed_file_fsync_leaves_previous_ledger_intact(tmp_path, monkeypatch):
    path = tmp_path / "capacity-samples.jsonl"
    first = transition.append_capacity_sample(
        path, sample(0), hmac_key=HMAC_KEY, persisted_state_loader=bootstrap_state
    )

    def fail_fsync(_descriptor):
        raise OSError(5, "simulated fsync failure")

    monkeypatch.setattr(transition.os, "fsync", fail_fsync)
    with pytest.raises(transition.CapacityTransitionError, match="transaction_fsync"):
        transition.append_capacity_sample(
            path, sample(1), hmac_key=HMAC_KEY, persisted_state_loader=bootstrap_state
        )
    assert transition.read_sample_ledger(path, hmac_key=HMAC_KEY) == first


def test_failed_first_publish_link_leaves_no_ledger(tmp_path, monkeypatch):
    path = tmp_path / "capacity-samples.jsonl"

    def fail_link(*_args, **_kwargs):
        raise OSError(5, "simulated link failure")

    monkeypatch.setattr(transition.os, "link", fail_link)
    with pytest.raises(transition.CapacityTransitionError, match="transaction_publish"):
        transition.append_capacity_sample(
            path, sample(0), hmac_key=HMAC_KEY, persisted_state_loader=bootstrap_state
        )
    assert not path.exists()
    assert list(tmp_path.glob(".capacity-samples.jsonl.txn-*")) == []


def test_directory_fsync_failure_recovers_idempotently(tmp_path, monkeypatch):
    path = tmp_path / "capacity-samples.jsonl"
    transition.append_capacity_sample(
        path, sample(0), hmac_key=HMAC_KEY, persisted_state_loader=bootstrap_state
    )
    original_fsync = transition.os.fsync

    def fail_directory_fsync(descriptor):
        if stat.S_ISDIR(os.fstat(descriptor).st_mode):
            raise OSError(5, "simulated directory fsync failure")
        return original_fsync(descriptor)

    monkeypatch.setattr(transition.os, "fsync", fail_directory_fsync)
    with pytest.raises(
        transition.CapacityTransitionError, match="directory_fsync_failed"
    ):
        transition.append_capacity_sample(
            path, sample(1), hmac_key=HMAC_KEY, persisted_state_loader=bootstrap_state
        )
    monkeypatch.setattr(transition.os, "fsync", original_fsync)
    recovered = transition.append_capacity_sample(
        path, sample(1), hmac_key=HMAC_KEY, persisted_state_loader=bootstrap_state
    )
    assert recovered.sample_count == 2


def test_append_rejects_overage_before_poisoning_ledger(tmp_path):
    path = tmp_path / "capacity-samples.jsonl"
    current = None
    for index in range(32):
        current = transition.append_capacity_sample(
            path,
            sample(index, observed_at=START + timedelta(days=index)),
            hmac_key=HMAC_KEY,
            persisted_state_loader=bootstrap_state,
        )
    with pytest.raises(transition.CapacityTransitionError, match="window_exceeded"):
        transition.append_capacity_sample(
            path,
            sample(32, observed_at=START + timedelta(days=32)),
            hmac_key=HMAC_KEY,
            persisted_state_loader=bootstrap_state,
        )
    assert transition.read_sample_ledger(path, hmac_key=HMAC_KEY) == current


def test_steady_state_freezes_ledger_before_any_write(tmp_path):
    ledger, authorization, receipt, marker, issued_at = transition_chain()
    path = tmp_path / "capacity-samples.jsonl"
    with pytest.raises(transition.CapacityTransitionError, match="frozen_after_steady"):
        transition.append_capacity_sample(
            path,
            sample(20),
            hmac_key=HMAC_KEY,
            persisted_state_loader=lambda: steady_state(
                ledger, authorization, receipt, marker, issued_at
            ),
        )
    assert not path.exists()


def test_ledger_rejects_noncanonical_or_truncated_content(tmp_path):
    path = tmp_path / "capacity-samples.jsonl"
    path.write_text(json.dumps(sample(0)) + "\n")
    path.chmod(0o600)
    with pytest.raises(transition.CapacityTransitionError, match="line_not_canonical"):
        transition.read_sample_ledger(path, hmac_key=HMAC_KEY)
    path.write_bytes(transition.canonical_bytes(sample(0)))
    path.chmod(0o600)
    with pytest.raises(transition.CapacityTransitionError, match="truncated"):
        transition.read_sample_ledger(path, hmac_key=HMAC_KEY)


def test_transition_authorization_is_hmac_owner_generation_and_ledger_bound():
    ledger, authorization, _, _, issued_at = transition_chain()
    assert set(authorization) == transition.TRANSITION_AUTHORIZATION_FIELDS
    assert authorization["target_generation"] == 2
    assert authorization["release_bom_sha256"] == RELEASE_BOM_SHA256
    tampered = copy.deepcopy(authorization)
    tampered["sample_count"] = 21
    with pytest.raises(
        transition.CapacityTransitionError, match="ledger_binding_invalid"
    ):
        transition.validate_transition_authorization(
            tampered, ledger=ledger, now=issued_at, hmac_key=HMAC_KEY
        )
    with pytest.raises(transition.CapacityTransitionError, match="tampered"):
        transition.validate_transition_authorization(
            authorization, ledger=ledger, now=issued_at, hmac_key=b"x" * 32
        )


def test_transition_authorization_requires_owner_short_ttl_and_fresh_sample():
    ledger = transition.validate_sample_ledger(samples(20), hmac_key=HMAC_KEY)
    issued_at = START + timedelta(hours=9 * 19, minutes=1)
    base = {
        "ledger": ledger,
        "authorization_id": "steady-auth-1",
        "approval_id": "steady-approval-1",
        "approval_evidence_sha256": digest("approval-evidence"),
        "authorized_by": "owner-user",
        "issued_at": issued_at,
        "expires_at": issued_at + timedelta(minutes=30),
        "persisted_state": bootstrap_state(),
        "hmac_key": HMAC_KEY,
    }
    with pytest.raises(transition.CapacityTransitionError, match="owner_required"):
        transition.issue_transition_authorization(**base, authorized_role="admin")
    with pytest.raises(transition.CapacityTransitionError, match="time_invalid"):
        transition.issue_transition_authorization(
            **{**base, "expires_at": issued_at + timedelta(hours=1, seconds=1)},
            authorized_role="owner",
        )
    with pytest.raises(transition.CapacityTransitionError, match="time_invalid"):
        transition.issue_transition_authorization(
            **{
                **base,
                "issued_at": issued_at + timedelta(days=2),
                "expires_at": issued_at + timedelta(days=2, minutes=30),
            },
            authorized_role="owner",
        )


def test_only_exact_persisted_steady_chain_activates_steady():
    ledger, authorization, receipt, marker, issued_at = transition_chain()
    intent = steady_intent(ledger, authorization, receipt, marker, issued_at)
    result = transition.resolve_effective_capacity(
        ledger=ledger,
        now=issued_at + timedelta(days=365),
        hmac_key=HMAC_KEY,
        persisted_state=steady_state(ledger, authorization, receipt, marker, issued_at),
        transition_intent=intent,
        transition_authorization=authorization,
        transition_receipt=receipt,
        commit_marker=marker,
        evidence_bundle_sha256=EVIDENCE_BUNDLE_SHA256,
        evidence_bundle_fingerprint=EVIDENCE_BUNDLE_FINGERPRINT,
    )
    assert result["state"] == transition.STEADY_ACTIVE
    assert result["irreversible"]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("generation", 3),
        ("release_id", "other-release"),
        ("bootstrap_epoch_id", "other-epoch"),
        ("commit_marker_fingerprint", "f" * 64),
    ],
)
def test_persisted_steady_generation_identity_and_marker_replay_are_rejected(
    field, value
):
    ledger, authorization, receipt, marker, issued_at = transition_chain()
    intent = steady_intent(ledger, authorization, receipt, marker, issued_at)
    state = steady_state(ledger, authorization, receipt, marker, issued_at)
    state[field] = value
    result = transition.resolve_effective_capacity(
        ledger=ledger,
        now=issued_at + timedelta(minutes=3),
        hmac_key=HMAC_KEY,
        persisted_state=state,
        transition_intent=intent,
        transition_authorization=authorization,
        transition_receipt=receipt,
        commit_marker=marker,
        evidence_bundle_sha256=EVIDENCE_BUNDLE_SHA256,
        evidence_bundle_fingerprint=EVIDENCE_BUNDLE_FINGERPRINT,
    )
    assert result["state"] == transition.STEADY_BLOCKED
    assert result["irreversible"]


@pytest.mark.parametrize(
    "artifact", ["intent", "authorization", "receipt", "marker", "bundle"]
)
def test_bootstrap_state_blocks_any_transition_artifact_as_orphan(artifact):
    ledger, authorization, receipt, marker, issued_at = transition_chain()
    kwargs = {}
    if artifact == "intent":
        kwargs["transition_intent"] = steady_intent(
            ledger, authorization, receipt, marker, issued_at
        )
    elif artifact == "authorization":
        kwargs["transition_authorization"] = authorization
    elif artifact == "receipt":
        kwargs["transition_receipt"] = receipt
    elif artifact == "marker":
        kwargs["commit_marker"] = marker
    else:
        kwargs["evidence_bundle_sha256"] = EVIDENCE_BUNDLE_SHA256
    result = transition.resolve_effective_capacity(
        ledger=ledger,
        now=issued_at + timedelta(minutes=3),
        hmac_key=HMAC_KEY,
        persisted_state=bootstrap_state(),
        **kwargs,
    )
    assert result["state"] == transition.STEADY_BLOCKED
    assert result["reason_code"] == "rca_capacity_transition_in_progress_or_orphaned"
    assert not result["irreversible"]


def test_persisted_steady_missing_or_tampered_evidence_blocks_irreversibly():
    ledger, authorization, receipt, marker, issued_at = transition_chain()
    intent = steady_intent(ledger, authorization, receipt, marker, issued_at)
    state = steady_state(ledger, authorization, receipt, marker, issued_at)
    missing = transition.resolve_effective_capacity(
        ledger=ledger,
        now=issued_at + timedelta(minutes=3),
        hmac_key=HMAC_KEY,
        persisted_state=state,
        transition_intent=intent,
    )
    assert missing["reason_code"] == "rca_capacity_steady_evidence_missing"
    assert missing["irreversible"]
    marker["sample_ledger_sha256"] = digest("other-ledger")
    tampered = transition.resolve_effective_capacity(
        ledger=ledger,
        now=issued_at + timedelta(minutes=3),
        hmac_key=HMAC_KEY,
        persisted_state=state,
        transition_authorization=authorization,
        transition_receipt=receipt,
        commit_marker=marker,
        evidence_bundle_sha256=EVIDENCE_BUNDLE_SHA256,
        evidence_bundle_fingerprint=EVIDENCE_BUNDLE_FINGERPRINT,
    )
    assert tampered["state"] == transition.STEADY_BLOCKED
    assert tampered["irreversible"]


def test_no_persisted_latch_never_activates_or_falls_back():
    ledger, authorization, receipt, marker, issued_at = transition_chain()
    intent = steady_intent(ledger, authorization, receipt, marker, issued_at)
    result = transition.resolve_effective_capacity(
        ledger=ledger,
        now=issued_at + timedelta(minutes=3),
        hmac_key=HMAC_KEY,
        persisted_state=None,
        transition_intent=intent,
        transition_authorization=authorization,
        transition_receipt=receipt,
        commit_marker=marker,
        evidence_bundle_sha256=EVIDENCE_BUNDLE_SHA256,
        evidence_bundle_fingerprint=EVIDENCE_BUNDLE_FINGERPRINT,
    )
    assert result["state"] == transition.STEADY_BLOCKED
    assert result["reason_code"] == "rca_capacity_persisted_state_missing"


def test_real_control_store_latch_is_accepted_end_to_end(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    initialized = store.initialize_capacity_transition(
        release_id=RELEASE_ID,
        bootstrap_epoch_id=EPOCH_ID,
        now=START - timedelta(minutes=1),
    )
    ledger, authorization, receipt, marker, issued_at = transition_chain(initialized)
    intent = steady_intent(ledger, authorization, receipt, marker, issued_at)
    persisted = store.compare_and_set_capacity_steady(
        expected_generation=1,
        release_id=RELEASE_ID,
        bootstrap_epoch_id=EPOCH_ID,
        final_ledger_sha256=ledger.ledger_sha256,
        transition_authorization_sha256=transition.sha256_canonical(authorization),
        transition_authorization_fingerprint=authorization["authorization_fingerprint"],
        transition_receipt_sha256=transition.sha256_canonical(receipt),
        transition_receipt_fingerprint=receipt["receipt_fingerprint"],
        commit_marker_sha256=transition.sha256_canonical(marker),
        commit_marker_fingerprint=marker["marker_fingerprint"],
        evidence_bundle_sha256=EVIDENCE_BUNDLE_SHA256,
        evidence_bundle_fingerprint=EVIDENCE_BUNDLE_FINGERPRINT,
        authorization_issued_at=authorization["issued_at"],
        authorization_expires_at=authorization["expires_at"],
        receipt_created_at=receipt["created_at"],
        marker_committed_at=marker["committed_at"],
        now=issued_at + timedelta(minutes=3),
    )
    result = transition.resolve_effective_capacity(
        ledger=ledger,
        now=issued_at + timedelta(days=1),
        hmac_key=HMAC_KEY,
        persisted_state=persisted,
        transition_intent=intent,
        transition_authorization=authorization,
        transition_receipt=receipt,
        commit_marker=marker,
        evidence_bundle_sha256=EVIDENCE_BUNDLE_SHA256,
        evidence_bundle_fingerprint=EVIDENCE_BUNDLE_FINGERPRINT,
    )
    assert result["state"] == transition.STEADY_ACTIVE


def test_shared_lock_blocks_exclusive_lock_until_release(tmp_path):
    path = tmp_path / "capacity.lock"
    entered = threading.Event()
    release = threading.Event()

    def hold_shared() -> None:
        with transition.capacity_flock(path, exclusive=False):
            entered.set()
            release.wait(timeout=2)

    thread = threading.Thread(target=hold_shared)
    thread.start()
    assert entered.wait(timeout=1)
    try:
        with pytest.raises(transition.CapacityTransitionError, match="lock_timeout"):
            with transition.capacity_flock(path, exclusive=True, timeout_seconds=0.05):
                pass
    finally:
        release.set()
        thread.join(timeout=2)
    assert not thread.is_alive()
    with transition.capacity_flock(path, exclusive=True, timeout_seconds=0.2):
        pass


def test_owner_only_read_rejects_permissions_symlink_and_hardlink(tmp_path):
    source = tmp_path / "source.json"
    source.write_text('{"value":1}\n')
    source.chmod(0o644)
    with pytest.raises(transition.CapacityTransitionError, match="not_owner_only"):
        transition.read_owner_only_json(source)

    source.chmod(0o600)
    symlink = tmp_path / "symlink.json"
    symlink.symlink_to(source)
    with pytest.raises(transition.CapacityTransitionError, match="file_unavailable"):
        transition.read_owner_only_json(symlink)

    hardlink = tmp_path / "hardlink.json"
    os.link(source, hardlink)
    with pytest.raises(transition.CapacityTransitionError, match="not_owner_only"):
        transition.read_owner_only_json(hardlink)


def test_owner_only_operations_reject_group_or_world_writable_parent(tmp_path):
    path = tmp_path / "artifact.json"
    tmp_path.chmod(0o777)
    try:
        with pytest.raises(
            transition.CapacityTransitionError, match="artifact_parent_invalid"
        ):
            transition.publish_owner_only_no_clobber(path, {"value": 1})
    finally:
        tmp_path.chmod(0o700)


def test_no_clobber_artifact_publish_is_owner_only_and_preserves_first(tmp_path):
    path = tmp_path / "steady-marker.json"
    artifact = {"schema_version": "test-v1", "value": 1}
    result = transition.publish_owner_only_no_clobber(path, artifact)
    assert path.stat().st_mode & 0o777 == 0o600
    assert result["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert result["sha256"] == transition.sha256_canonical(artifact)
    assert transition.publish_owner_only_no_clobber(path, artifact) == result
    with pytest.raises(transition.CapacityTransitionError, match="artifact_exists"):
        transition.publish_owner_only_no_clobber(path, {"value": 2})
    assert transition.read_owner_only_json(path)[0] == artifact


def test_no_clobber_does_not_replace_existing_symlink(tmp_path):
    source = tmp_path / "source.json"
    source.write_text("sentinel")
    target = tmp_path / "target.json"
    target.symlink_to(source)
    with pytest.raises(transition.CapacityTransitionError, match="artifact_exists"):
        transition.publish_owner_only_no_clobber(target, {"value": 1})
    assert source.read_text() == "sentinel"
