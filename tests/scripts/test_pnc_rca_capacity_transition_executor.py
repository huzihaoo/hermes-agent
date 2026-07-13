from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import threading

import pytest

from gateway import pnc_rca_capacity_sample_evidence as sample_evidence
from gateway import pnc_rca_capacity_transition as transition
from gateway.pnc_rca_control_store import RcaControlStore
from scripts import pnc_rca_capacity_transition_executor as executor_module


NOW = datetime(2026, 7, 20, 1, 0, tzinfo=timezone.utc)
KEY = bytes.fromhex("42" * 32)
RELEASE_ID = "release-20260713"
BOOTSTRAP_EPOCH_ID = "rca-bootstrap-release-20260713"
BUSINESS_EPOCH_ID = "rca-business-release-20260713"
OPERATOR = "owner-user"
REASON = "qualified capacity samples approved for steady production"


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


class StoreHarness:
    def __init__(self, delegate: RcaControlStore):
        self.delegate = delegate
        self.business_state = "steady_active"
        self.business_audit_count = 7
        self.calls: list[str] = []

    def capacity_transition_state(self):
        return self.delegate.capacity_transition_state()

    def compare_and_set_capacity_steady(self, **kwargs):
        self.calls.append("capacity_cas")
        return self.delegate.compare_and_set_capacity_steady(**kwargs)

    def activation_epoch(self):
        return {"epoch_id": BUSINESS_EPOCH_ID, "state": self.business_state}

    def transition_activation_epoch(self, **kwargs):
        del kwargs
        raise AssertionError("capacity executor must not mutate business activation")


def build_samples(
    *, producer_receipt_sha256: str, producer_receipt_fingerprint: str
) -> list[dict]:
    first = NOW - timedelta(days=7, hours=2)
    spacing = timedelta(days=7, hours=1) / 19
    return [
        transition.issue_capacity_sample(
            sample_id=f"sample-{index:03d}",
            release_id=RELEASE_ID,
            bootstrap_epoch_id=BOOTSTRAP_EPOCH_ID,
            release_bom_sha256="a1" * 32,
            active_release_binding_sha256="b2" * 32,
            task_id=f"g1q3-rca-{index:03d}",
            attempt_id=f"attempt-{index:03d}",
            admission_receipt_sha256=digest(f"admission-{index}"),
            admission_receipt_fingerprint=digest(f"admission-fp-{index}"),
            task_manifest_sha256=digest(f"manifest-{index}"),
            producer_activation_receipt_sha256=producer_receipt_sha256,
            producer_activation_receipt_fingerprint=producer_receipt_fingerprint,
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


def write_private(path: Path, raw: bytes) -> None:
    path.write_bytes(raw)
    path.chmod(0o600)


@pytest.fixture
def scenario(tmp_path):
    db_path = tmp_path / "control.sqlite3"
    delegate = RcaControlStore(db_path)
    state = delegate.initialize_capacity_transition(
        release_id=RELEASE_ID,
        bootstrap_epoch_id=BOOTSTRAP_EPOCH_ID,
        now=NOW - timedelta(days=8),
    )
    store = StoreHarness(delegate)
    executor = executor_module.SteadyCapacityTransitionExecutor(
        store=store,
        control_db_path=db_path,
        hmac_key=KEY,
        now=lambda: NOW,
        lock_timeout_seconds=0.2,
    )
    executor.paths.state_root.mkdir(mode=0o700)
    producer_receipt = sample_evidence.issue_producer_activation_receipt(
        release_id=RELEASE_ID,
        bootstrap_epoch_id=BOOTSTRAP_EPOCH_ID,
        release_bom_sha256="a1" * 32,
        active_release_binding_sha256="b2" * 32,
        activated_at=NOW - timedelta(days=7, hours=3),
        hmac_key=KEY,
        receipt_id="sample-producer-activation-test",
    )
    producer_raw = transition.canonical_bytes(producer_receipt)
    write_private(executor.paths.producer_activation, producer_raw)
    ledger_raw = b"".join(
        transition.canonical_bytes(item) + b"\n"
        for item in build_samples(
            producer_receipt_sha256=hashlib.sha256(producer_raw).hexdigest(),
            producer_receipt_fingerprint=str(producer_receipt["receipt_fingerprint"]),
        )
    )
    write_private(executor.paths.sample_ledger, ledger_raw)
    ledger = transition.read_sample_ledger(executor.paths.sample_ledger, hmac_key=KEY)
    authorization = transition.issue_transition_authorization(
        ledger=ledger,
        authorization_id="owner-auth-1",
        approval_id="owner-approval-1",
        approval_evidence_sha256=digest("owner-approval-evidence"),
        authorized_by=OPERATOR,
        authorized_role="owner",
        issued_at=NOW - timedelta(minutes=10),
        expires_at=NOW + timedelta(minutes=10),
        persisted_state=state,
        hmac_key=KEY,
    )
    authorization_path = tmp_path / "external-owner-authorization.json"
    write_private(authorization_path, transition.canonical_bytes(authorization))
    return store, executor, authorization_path


def execute(executor, authorization_path, *, apply=True):
    return executor.execute(
        epoch_id=BUSINESS_EPOCH_ID,
        operator=OPERATOR,
        reason=REASON,
        authorization_path=authorization_path,
        apply=apply,
    )


@pytest.mark.parametrize(
    "flag", ["--business-activation-epoch-id", "--epoch-id"]
)
def test_cli_names_business_activation_epoch_and_keeps_legacy_alias(flag):
    args = executor_module._build_parser().parse_args(
        [
            "--control-db",
            "/tmp/control.sqlite3",
            "transition-steady",
            "--authorization",
            "/tmp/authorization.json",
            flag,
            BUSINESS_EPOCH_ID,
            "--operator",
            OPERATOR,
            "--reason",
            REASON,
        ]
    )

    assert args.business_activation_epoch_id == BUSINESS_EPOCH_ID


def recover(store, prior_executor, *, now):
    executor = executor_module.SteadyCapacityTransitionExecutor(
        store=store,
        control_db_path=prior_executor.control_db_path,
        hmac_key=KEY,
        now=lambda: now,
        lock_timeout_seconds=0.2,
    )
    return executor.recover(apply=True), executor


def test_plan_requires_external_owner_authorization_but_publishes_nothing(scenario):
    _store, executor, authorization_path = scenario
    plan = execute(executor, authorization_path, apply=False)
    assert plan["applied"] is False
    assert plan["recovery"] is False
    assert not any(plan["artifact_presence"].values())
    with pytest.raises(
        executor_module.CapacityTransitionExecutorError,
        match="authorization_required",
    ):
        execute(executor, None, apply=False)
    assert not executor.paths.transition_intent.exists()


def test_business_steady_with_capacity_bootstrap_can_ratchet_without_business_write(
    scenario,
):
    store, executor, authorization_path = scenario
    assert store.delegate.capacity_transition_state()["state"] == (
        transition.BOOTSTRAP_PRODUCTION
    )
    audit_count = store.business_audit_count
    result = execute(executor, authorization_path)
    assert result["capacity_state"] == transition.STEADY_ACTIVE
    assert result["runtime_effective_state"] == transition.STEADY_ACTIVE
    assert result["business_activation_state"] == "steady_active"
    assert store.calls == ["capacity_cas"]
    assert store.business_audit_count == audit_count
    assert all(result["artifact_presence"].values())


def test_business_confirmed_is_rejected_before_any_capacity_artifact(scenario):
    store, executor, authorization_path = scenario
    store.business_state = "confirmed"
    with pytest.raises(
        executor_module.CapacityTransitionExecutorError,
        match="business_activation_not_steady",
    ):
        execute(executor, authorization_path)
    assert store.delegate.capacity_transition_state()["state"] == (
        transition.BOOTSTRAP_PRODUCTION
    )
    assert not executor.paths.transition_intent.exists()


@pytest.mark.parametrize(
    "crash_prefix",
    ["intent", "authorization", "receipt", "marker", "bundle", "capacity_cas"],
)
def test_each_durable_prefix_recovers_exact_after_authorization_expiry(
    scenario, crash_prefix
):
    store, executor, authorization_path = scenario

    def crash(prefix: str) -> None:
        if prefix == crash_prefix:
            raise RuntimeError(f"crash:{prefix}")

    executor.fault_injector = crash
    with pytest.raises(RuntimeError, match=f"crash:{crash_prefix}"):
        execute(executor, authorization_path)
    assert executor.paths.transition_intent.exists()
    result, recovered = recover(store, executor, now=NOW + timedelta(hours=2))
    assert result["applied"] is True
    assert result["recovery"] is True
    assert result["capacity_state"] == transition.STEADY_ACTIVE
    assert result["business_activation_state"] == "steady_active"
    assert all(result["artifact_presence"].values())
    assert recovered.status()["runtime_effective_state"] == transition.STEADY_ACTIVE


def test_expired_authorization_without_intent_cannot_start(scenario):
    _store, executor, authorization_path = scenario
    executor.now = lambda: NOW + timedelta(hours=2)
    with pytest.raises(
        transition.CapacityTransitionError,
        match="authorization_time_invalid",
    ):
        execute(executor, authorization_path)
    assert not executor.paths.transition_intent.exists()


def test_recovery_rejects_new_authority_and_conflicting_prefix(scenario):
    store, executor, authorization_path = scenario
    executor.fault_injector = lambda prefix: (
        (_ for _ in ()).throw(RuntimeError("crash:intent"))
        if prefix == "intent"
        else None
    )
    with pytest.raises(RuntimeError, match="crash:intent"):
        execute(executor, authorization_path)
    executor.fault_injector = None
    with pytest.raises(
        executor_module.CapacityTransitionExecutorError,
        match="recover_command_required",
    ):
        execute(executor, authorization_path)
    write_private(
        executor.paths.transition_authorization,
        transition.canonical_bytes({"conflict": True}),
    )
    with pytest.raises(transition.CapacityTransitionError, match="artifact_exists"):
        recover(store, executor, now=NOW + timedelta(hours=2))


def test_tampered_intent_blocks_recovery(scenario):
    store, executor, authorization_path = scenario
    executor.fault_injector = lambda prefix: (
        (_ for _ in ()).throw(RuntimeError("crash:intent"))
        if prefix == "intent"
        else None
    )
    with pytest.raises(RuntimeError, match="crash:intent"):
        execute(executor, authorization_path)
    intent, _ = transition.read_owner_only_json(executor.paths.transition_intent)
    intent["reason"] = "tampered reason"
    write_private(executor.paths.transition_intent, transition.canonical_bytes(intent))
    with pytest.raises(transition.CapacityTransitionError, match="intent_tampered"):
        recover(store, executor, now=NOW + timedelta(hours=2))


def test_global_exclusive_lock_serializes_apply(scenario):
    _store, executor, authorization_path = scenario
    entered = threading.Event()
    release = threading.Event()

    def hold_lock() -> None:
        with transition.capacity_flock(
            executor.paths.global_lock, exclusive=True, timeout_seconds=0.2
        ):
            entered.set()
            release.wait(timeout=2)

    thread = threading.Thread(target=hold_lock)
    thread.start()
    assert entered.wait(timeout=1)
    try:
        executor.lock_timeout_seconds = 0.03
        with pytest.raises(transition.CapacityTransitionError, match="lock_timeout"):
            execute(executor, authorization_path)
    finally:
        release.set()
        thread.join(timeout=2)
    assert not executor.paths.transition_intent.exists()


def test_bootstrap_runtime_blocks_intent_only_prefix(scenario):
    _store, executor, authorization_path = scenario
    executor.fault_injector = lambda prefix: (
        (_ for _ in ()).throw(RuntimeError("crash:intent"))
        if prefix == "intent"
        else None
    )
    with pytest.raises(RuntimeError, match="crash:intent"):
        execute(executor, authorization_path)
    status = executor.status()
    assert status["runtime_effective_state"] == transition.STEADY_BLOCKED
    assert status["runtime_reason_code"] == (
        "rca_capacity_transition_in_progress_or_orphaned"
    )


def test_recover_cli_accepts_only_apply_and_uses_no_ambient_authority(
    tmp_path, monkeypatch, capsys
):
    calls: list[bool] = []

    class FakeExecutor:
        def __init__(self, **kwargs):
            del kwargs

        def recover(self, *, apply: bool):
            calls.append(apply)
            return {
                "schema_version": executor_module.EXECUTOR_SCHEMA_VERSION,
                "ok": True,
                "command": "recover",
                "applied": apply,
            }

    monkeypatch.setattr(executor_module, "RcaControlStore", lambda *a, **k: object())
    monkeypatch.setattr(
        executor_module.runtime, "load_capacity_hmac_key", lambda *a, **k: KEY
    )
    monkeypatch.setattr(
        executor_module, "SteadyCapacityTransitionExecutor", FakeExecutor
    )
    rc = executor_module.main([
        "--control-db",
        str(tmp_path / "control.sqlite3"),
        "recover",
        "--apply",
    ])
    assert rc == 0
    assert calls == [True]
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "schema_version": executor_module.EXECUTOR_SCHEMA_VERSION,
        "ok": True,
        "command": "recover",
        "applied": True,
    }
