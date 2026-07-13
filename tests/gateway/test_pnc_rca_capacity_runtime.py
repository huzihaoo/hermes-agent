from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
import hashlib
import threading

import pytest

from gateway import pnc_rca_capacity_runtime as runtime
from gateway import pnc_rca_capacity_sample_evidence as sample_evidence
from gateway import pnc_rca_capacity_transition as transition
from gateway.pnc_rca_control_store import RcaControlStore


NOW = datetime(2026, 7, 20, 1, 0, tzinfo=timezone.utc)
KEY = bytes.fromhex("42" * 32)
RELEASE_ID = "release-20260713"
EPOCH_ID = "rca-bootstrap-release-20260713"
RELEASE_BOM = "a1" * 32
ACTIVE_BINDING = "b2" * 32


def digest(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def write_private(path, raw: bytes) -> None:
    path.write_bytes(raw)
    path.chmod(0o600)


def producer_receipt(*, binding: str = ACTIVE_BINDING) -> tuple[dict, bytes]:
    receipt = sample_evidence.issue_producer_activation_receipt(
        release_id=RELEASE_ID,
        bootstrap_epoch_id=EPOCH_ID,
        release_bom_sha256=RELEASE_BOM,
        active_release_binding_sha256=binding,
        activated_at=NOW - timedelta(days=8),
        hmac_key=KEY,
        receipt_id="producer-release-20260713",
    )
    return receipt, sample_evidence.canonical_bytes(receipt)


def build_samples(
    *,
    producer_sha256: str | None = None,
    producer_fingerprint: str | None = None,
) -> list[dict]:
    receipt, raw = producer_receipt()
    producer_sha256 = producer_sha256 or hashlib.sha256(raw).hexdigest()
    producer_fingerprint = producer_fingerprint or receipt["receipt_fingerprint"]
    first = NOW - timedelta(days=7, hours=2)
    spacing = timedelta(days=7, hours=1) / 19
    return [
        transition.issue_capacity_sample(
            sample_id=f"sample-{index:03d}",
            release_id=RELEASE_ID,
            bootstrap_epoch_id=EPOCH_ID,
            release_bom_sha256=RELEASE_BOM,
            active_release_binding_sha256=ACTIVE_BINDING,
            task_id=f"g1q3-rca-{index:03d}",
            attempt_id=f"attempt-{index:03d}",
            admission_receipt_sha256=digest(f"admission-{index}"),
            admission_receipt_fingerprint=digest(f"admission-fp-{index}"),
            task_manifest_sha256=digest(f"manifest-{index}"),
            producer_activation_receipt_sha256=producer_sha256,
            producer_activation_receipt_fingerprint=producer_fingerprint,
            vm_terminal_receipt_sha256=digest(f"vm-terminal-{index}"),
            vm_terminal_receipt_fingerprint=digest(f"vm-terminal-fp-{index}"),
            host_success_receipt_sha256=digest(f"host-success-{index}"),
            host_success_receipt_fingerprint=digest(f"host-success-fp-{index}"),
            terminal_status="succeeded",
            root_peak_bytes=1000 + index,
            delivery_peak_bytes=2000 + index,
            delivery_used_bytes=500 + index,
            input_materialized_bytes=0,
            observed_at=first + spacing * index,
            hmac_key=KEY,
        )
        for index in range(20)
    ]


def bootstrap_runtime(tmp_path):
    db_path = tmp_path / "control.sqlite3"
    store = RcaControlStore(db_path)
    state = store.initialize_capacity_transition(
        release_id=RELEASE_ID,
        bootstrap_epoch_id=EPOCH_ID,
        now=NOW - timedelta(days=8),
    )
    resolver = runtime.CapacityRuntimeResolver(
        store=store,
        control_db_path=db_path,
        release_id=RELEASE_ID,
        bootstrap_epoch_id=EPOCH_ID,
        initial_policy="bootstrap",
        hmac_key=KEY,
        now=lambda: NOW,
        lock_timeout_seconds=0.2,
    )
    resolver.paths.state_root.mkdir(mode=0o700)
    receipt, receipt_raw = producer_receipt()
    write_private(resolver.paths.producer_activation, receipt_raw)
    sample_values = build_samples(
        producer_sha256=hashlib.sha256(receipt_raw).hexdigest(),
        producer_fingerprint=receipt["receipt_fingerprint"],
    )
    write_private(
        resolver.paths.sample_ledger,
        b"".join(transition.canonical_bytes(value) + b"\n" for value in sample_values),
    )
    ledger = transition.read_sample_ledger(resolver.paths.sample_ledger, hmac_key=KEY)
    return store, resolver, state, ledger


def empty_bootstrap_runtime(tmp_path, *, with_producer: bool):
    db_path = tmp_path / "control.sqlite3"
    store = RcaControlStore(db_path)
    store.initialize_capacity_transition(
        release_id=RELEASE_ID,
        bootstrap_epoch_id=EPOCH_ID,
        now=NOW - timedelta(days=1),
    )
    resolver = runtime.CapacityRuntimeResolver(
        store=store,
        control_db_path=db_path,
        release_id=RELEASE_ID,
        bootstrap_epoch_id=EPOCH_ID,
        initial_policy="bootstrap",
        hmac_key=KEY,
        now=lambda: NOW,
        lock_timeout_seconds=0.2,
    )
    if with_producer:
        resolver.paths.state_root.mkdir(mode=0o700)
        receipt, raw = producer_receipt()
        write_private(resolver.paths.producer_activation, raw)
    return store, resolver


def activate(store, resolver, state, ledger):
    authorization = transition.issue_transition_authorization(
        ledger=ledger,
        authorization_id="steady-auth-1",
        approval_id="steady-approval-1",
        approval_evidence_sha256=digest("approval"),
        authorized_by="owner-user",
        authorized_role="owner",
        issued_at=NOW - timedelta(minutes=30),
        expires_at=NOW + timedelta(minutes=30),
        persisted_state=state,
        hmac_key=KEY,
    )
    receipt = transition.issue_transition_receipt(
        ledger=ledger,
        authorization=authorization,
        receipt_id="steady-receipt-1",
        created_at=NOW - timedelta(minutes=20),
        hmac_key=KEY,
    )
    marker = transition.issue_steady_commit_marker(
        ledger=ledger,
        authorization=authorization,
        receipt=receipt,
        marker_id="steady-marker-1",
        committed_at=NOW - timedelta(minutes=10),
        hmac_key=KEY,
    )
    intent = transition.issue_steady_transition_intent(
        ledger=ledger,
        authorization=authorization,
        receipt=receipt,
        marker=marker,
        intent_id="steady-intent-1",
        business_activation_epoch_id="business-epoch-1",
        operator="owner-user",
        reason="qualified capacity samples approved for steady production",
        created_at=NOW - timedelta(minutes=5),
        hmac_key=KEY,
    )
    intent_raw = transition.canonical_bytes(intent)
    authorization_raw = transition.canonical_bytes(authorization)
    receipt_raw = transition.canonical_bytes(receipt)
    marker_raw = transition.canonical_bytes(marker)
    bundle = runtime.issue_evidence_bundle(
        release_id=RELEASE_ID,
        bootstrap_epoch_id=EPOCH_ID,
        target_generation=2,
        sample_ledger_sha256=ledger.ledger_sha256,
        transition_intent_sha256=hashlib.sha256(intent_raw).hexdigest(),
        transition_intent_fingerprint=intent["intent_fingerprint"],
        transition_authorization_sha256=hashlib.sha256(authorization_raw).hexdigest(),
        transition_authorization_fingerprint=authorization["authorization_fingerprint"],
        transition_receipt_sha256=hashlib.sha256(receipt_raw).hexdigest(),
        transition_receipt_fingerprint=receipt["receipt_fingerprint"],
        commit_marker_sha256=hashlib.sha256(marker_raw).hexdigest(),
        commit_marker_fingerprint=marker["marker_fingerprint"],
        created_at=NOW - timedelta(minutes=5),
        hmac_key=KEY,
    )
    bundle_raw = transition.canonical_bytes(bundle)
    write_private(resolver.paths.transition_intent, intent_raw)
    write_private(resolver.paths.transition_authorization, authorization_raw)
    write_private(resolver.paths.transition_receipt, receipt_raw)
    write_private(resolver.paths.commit_marker, marker_raw)
    write_private(resolver.paths.evidence_bundle, bundle_raw)
    steady = store.compare_and_set_capacity_steady(
        expected_generation=1,
        release_id=RELEASE_ID,
        bootstrap_epoch_id=EPOCH_ID,
        final_ledger_sha256=ledger.ledger_sha256,
        transition_authorization_sha256=hashlib.sha256(authorization_raw).hexdigest(),
        transition_authorization_fingerprint=authorization["authorization_fingerprint"],
        transition_receipt_sha256=hashlib.sha256(receipt_raw).hexdigest(),
        transition_receipt_fingerprint=receipt["receipt_fingerprint"],
        commit_marker_sha256=hashlib.sha256(marker_raw).hexdigest(),
        commit_marker_fingerprint=marker["marker_fingerprint"],
        evidence_bundle_sha256=hashlib.sha256(bundle_raw).hexdigest(),
        evidence_bundle_fingerprint=bundle["bundle_fingerprint"],
        authorization_issued_at=authorization["issued_at"],
        authorization_expires_at=authorization["expires_at"],
        receipt_created_at=receipt["created_at"],
        marker_committed_at=marker["committed_at"],
        now=NOW,
    )
    return steady, intent, authorization, receipt, marker, bundle


def test_legacy_steady_without_release_is_compatible_and_does_not_require_key(tmp_path):
    resolver = runtime.CapacityRuntimeResolver(
        store=object(),
        control_db_path=tmp_path / "control.sqlite3",
        release_id="",
        bootstrap_epoch_id="",
        initial_policy="steady",
    )
    decision = resolver.observe()
    assert decision["configured"] is False
    assert decision["effective_mode"] == "steady"
    assert decision["legacy_compatibility"] is True


def test_production_bootstrap_is_resolved_from_db_and_hmac_ledger(tmp_path):
    _, resolver, _, ledger = bootstrap_runtime(tmp_path)
    decision = resolver.observe()
    assert decision["effective_state"] == transition.STEADY_READY
    assert decision["effective_mode"] == "bootstrap"
    assert decision["generation"] == 1
    assert decision["ledger"]["sha256"] == ledger.ledger_sha256
    assert decision["active_release_binding_sha256"] == ACTIVE_BINDING
    assert decision["ready"] is True


def test_bootstrap_empty_ledger_requires_fixed_producer_receipt(tmp_path):
    _, blocked_resolver = empty_bootstrap_runtime(
        tmp_path / "blocked", with_producer=False
    )
    blocked = blocked_resolver.observe()
    assert blocked["effective_state"] == transition.STEADY_BLOCKED
    assert blocked["ready"] is False

    _, resolver = empty_bootstrap_runtime(tmp_path / "ready", with_producer=True)
    decision = resolver.observe()
    assert decision["effective_state"] == transition.BOOTSTRAP_PRODUCTION
    assert decision["effective_mode"] == "bootstrap"
    assert decision["ledger"]["sample_count"] == 0
    assert decision["ledger"]["sha256"] == hashlib.sha256(b"").hexdigest()
    assert decision["active_release_binding_sha256"] == ACTIVE_BINDING
    assert decision["ready"] is True


def test_first_sample_must_match_fixed_producer_receipt(tmp_path):
    _, resolver, _, _ = bootstrap_runtime(tmp_path)
    samples = build_samples(
        producer_sha256=digest("different-producer"),
        producer_fingerprint=digest("different-producer-fingerprint"),
    )
    write_private(
        resolver.paths.sample_ledger,
        b"".join(transition.canonical_bytes(value) + b"\n" for value in samples),
    )

    decision = resolver.observe()

    assert decision["effective_state"] == transition.STEADY_BLOCKED
    assert decision["reason_code"] == (
        "rca_capacity_runtime_producer_ledger_binding_invalid"
    )


def test_missing_db_latch_and_wrong_hmac_fail_closed(tmp_path):
    store, resolver, _, _ = bootstrap_runtime(tmp_path)
    with store._connect() as conn:
        conn.execute("DROP TRIGGER trg_rca_capacity_state_no_delete")
        conn.execute("DROP TRIGGER trg_rca_capacity_audit_no_delete")
        conn.execute("DELETE FROM rca_capacity_transition_state")
        conn.execute("DELETE FROM rca_capacity_transition_audit")
    missing = resolver.observe()
    assert missing["effective_state"] == transition.STEADY_BLOCKED
    assert missing["reason_code"] == "rca_capacity_persisted_state_missing"

    _, other, _, _ = bootstrap_runtime(tmp_path / "other")
    other.hmac_key = b"x" * 32
    wrong_key = other.observe()
    assert wrong_key["effective_state"] == transition.STEADY_BLOCKED
    assert "signature" in wrong_key["reason_code"]


def test_static_bootstrap_switches_to_dynamic_steady_without_restart(tmp_path):
    store, resolver, state, ledger = bootstrap_runtime(tmp_path)
    assert resolver.observe()["effective_mode"] == "bootstrap"
    activate(store, resolver, state, ledger)
    decision = resolver.observe()
    assert decision["effective_state"] == transition.STEADY_ACTIVE
    assert decision["effective_mode"] == "steady"
    assert decision["generation"] == 2
    assert decision["irreversible"] is True
    assert decision["artifacts"]["commit_marker_fingerprint"]


def test_steady_uses_ratchet_origin_across_future_software_release(tmp_path):
    store, resolver, state, ledger = bootstrap_runtime(tmp_path)
    activate(store, resolver, state, ledger)
    future = runtime.CapacityRuntimeResolver(
        store=store,
        control_db_path=resolver.control_db_path,
        release_id="release-20260801",
        bootstrap_epoch_id="rca-bootstrap-release-20260801",
        initial_policy="steady",
        hmac_key=KEY,
        now=lambda: NOW,
    )
    decision = future.observe()
    assert decision["effective_mode"] == "steady"
    assert decision["current_release_id"] == "release-20260801"
    assert decision["ratchet_origin_release_id"] == RELEASE_ID


def test_active_missing_or_tampered_evidence_never_falls_back(tmp_path):
    store, resolver, state, ledger = bootstrap_runtime(tmp_path)
    activate(store, resolver, state, ledger)
    resolver.paths.transition_receipt.unlink()
    missing = resolver.observe()
    assert missing["effective_state"] == transition.STEADY_BLOCKED
    assert missing["effective_mode"] == "blocked"
    assert missing["irreversible"] is True

    _, resolver2, state2, ledger2 = bootstrap_runtime(tmp_path / "tampered")
    activate(resolver2.store, resolver2, state2, ledger2)
    bundle, _ = transition.read_owner_only_json(resolver2.paths.evidence_bundle)
    bundle["commit_marker_fingerprint"] = "f" * 64
    write_private(resolver2.paths.evidence_bundle, transition.canonical_bytes(bundle))
    tampered = resolver2.observe()
    assert tampered["effective_state"] == transition.STEADY_BLOCKED
    assert tampered["irreversible"] is True


def test_steady_capacity_never_accepts_empty_ledger(tmp_path):
    store, resolver, state, ledger = bootstrap_runtime(tmp_path)
    activate(store, resolver, state, ledger)
    resolver.paths.sample_ledger.unlink()

    decision = resolver.observe()

    assert decision["effective_state"] == transition.STEADY_BLOCKED
    assert decision["ready"] is False


def test_marker_or_partial_transition_evidence_before_db_cas_blocks(tmp_path):
    _, resolver, _, _ = bootstrap_runtime(tmp_path)
    write_private(
        resolver.paths.commit_marker, transition.canonical_bytes({"orphan": True})
    )
    decision = resolver.observe()
    assert decision["effective_state"] == transition.STEADY_BLOCKED
    assert decision["reason_code"] == "rca_capacity_transition_in_progress_or_orphaned"


def test_shared_decision_holds_global_lock_until_context_exit(tmp_path):
    _, resolver, _, _ = bootstrap_runtime(tmp_path)
    outcome: list[str] = []

    def take_exclusive() -> None:
        try:
            with transition.capacity_flock(
                resolver.paths.global_lock,
                exclusive=True,
                timeout_seconds=0.05,
            ):
                outcome.append("acquired")
        except transition.CapacityTransitionError as exc:
            outcome.append(exc.code)

    with resolver.shared_decision() as decision:
        assert decision["ready"] is True
        thread = threading.Thread(target=take_exclusive)
        thread.start()
        thread.join(timeout=1)
    assert outcome == ["rca_capacity_lock_timeout"]
    with transition.capacity_flock(
        resolver.paths.global_lock, exclusive=True, timeout_seconds=0.2
    ):
        pass


def test_evidence_bundle_hmac_and_exact_bindings_reject_tamper():
    bundle = runtime.issue_evidence_bundle(
        release_id=RELEASE_ID,
        bootstrap_epoch_id=EPOCH_ID,
        target_generation=2,
        sample_ledger_sha256="1" * 64,
        transition_intent_sha256="8" * 64,
        transition_intent_fingerprint="9" * 64,
        transition_authorization_sha256="2" * 64,
        transition_authorization_fingerprint="3" * 64,
        transition_receipt_sha256="4" * 64,
        transition_receipt_fingerprint="5" * 64,
        commit_marker_sha256="6" * 64,
        commit_marker_fingerprint="7" * 64,
        created_at=NOW,
        hmac_key=KEY,
    )
    tampered = copy.deepcopy(bundle)
    tampered["target_generation"] = 3
    with pytest.raises(runtime.CapacityRuntimeError, match="binding_invalid"):
        runtime.validate_evidence_bundle(
            tampered,
            hmac_key=KEY,
            now=NOW,
            expected_release_id=RELEASE_ID,
            expected_bootstrap_epoch_id=EPOCH_ID,
            expected_target_generation=2,
            expected_sample_ledger_sha256="1" * 64,
            expected_transition_intent_sha256="8" * 64,
            expected_transition_intent_fingerprint="9" * 64,
            expected_transition_authorization_sha256="2" * 64,
            expected_transition_authorization_fingerprint="3" * 64,
            expected_transition_receipt_sha256="4" * 64,
            expected_transition_receipt_fingerprint="5" * 64,
            expected_commit_marker_sha256="6" * 64,
            expected_commit_marker_fingerprint="7" * 64,
        )
