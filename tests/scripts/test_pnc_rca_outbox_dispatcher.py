from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from gateway.pnc_rca_control_store import RcaControlStore
from gateway.pnc_rca_runtime_identity import RCA_RUNTIME_RELATIVE_FILES
from gateway.pnc_rca_runtime_identity import canonical_json_sha256
from gateway.pnc_rca_vm_release_binding import (
    RCA_PROD_VM_STORAGE_ADMISSION_MODULE,
)
from scripts import pnc_rca_outbox_dispatcher as dispatcher
from tests.gateway.test_pnc_rca_control_store import (
    _migrate_v14_fixture_to_v15,
    _manual_request,
    _policy,
    _profile_snapshot_policy,
    _profile_snapshot_record,
    _record,
    _steady_control_store,
)
from tests.gateway.test_pnc_rca_delivery_store import (
    _physical_v15_delivery_fixture,
    _sqlite_storage_identity,
)


def _v15_control_store(path: Path) -> RcaControlStore:
    _steady_control_store(path)
    _migrate_v14_fixture_to_v15(path)
    return RcaControlStore(
        path,
        require_current=True,
        allow_successor_write=True,
    )


def test_storage_admission_uses_isolated_production_runtime():
    assert (
        dispatcher.REMOTE_STORAGE_ADMISSION_MODULE
        == RCA_PROD_VM_STORAGE_ADMISSION_MODULE
    )


def test_outbox_retry_backoff_uses_five_second_steady_interval():
    assert dispatcher.RETRY_DELAYS_SECONDS == (
        0,
        2,
        5,
        5,
        5,
        5,
        5,
        5,
        5,
        5,
    )
    assert [dispatcher.retry_delay_seconds(attempt) for attempt in range(1, 11)] == [
        2,
        5,
        5,
        5,
        5,
        5,
        5,
        5,
        5,
        5,
    ]
    assert dispatcher.retry_delay_seconds(100) == 5


def _sqlite_error(message: str, code: int | None = None) -> sqlite3.OperationalError:
    exc = sqlite3.OperationalError(message)
    if code is not None:
        exc.sqlite_errorcode = code
    return exc


def test_sqlite_busy_classifier_uses_extended_code_and_wrapped_chain():
    inner = _sqlite_error("misleading", sqlite3.SQLITE_BUSY | (2 << 8))
    outer = RuntimeError("store construction failed")
    outer.__cause__ = inner

    assert dispatcher._is_sqlite_busy_or_locked(outer) is True
    assert (
        dispatcher._is_sqlite_busy_or_locked(_sqlite_error("database is locked"))
        is True
    )


@pytest.mark.parametrize(
    "exc",
    [
        _sqlite_error("database is locked", sqlite3.SQLITE_ERROR),
        _sqlite_error("no such table: rca_outbox", sqlite3.SQLITE_ERROR),
        RuntimeError("database is locked"),
    ],
)
def test_sqlite_busy_classifier_rejects_nonbusy_failures(exc):
    assert dispatcher._is_sqlite_busy_or_locked(exc) is False


def test_resident_store_startup_retries_wrapped_busy_with_bounded_delays(
    tmp_path, monkeypatch, capsys
):
    sentinel = object()
    calls = []
    busy = RuntimeError("wrapped")
    busy.__cause__ = _sqlite_error("database is locked")

    def store_factory(*args, **kwargs):
        calls.append((args, kwargs))
        if len(calls) < 3:
            raise busy
        return sentinel

    sleeps = []
    monkeypatch.setattr(dispatcher, "RcaControlStore", store_factory)

    result = dispatcher._open_resident_control_store(
        tmp_path / "control.sqlite3", sleep=sleeps.append
    )

    assert result is sentinel
    assert sleeps == [2, 5]
    assert len(calls) == 3
    logs = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    assert [(item["attempt"], item["delay_seconds"]) for item in logs] == [
        (1, 2),
        (2, 5),
    ]
    assert all(item["phase"] == "resident_store_startup" for item in logs)


def test_resident_store_startup_persistent_busy_is_bounded(tmp_path, monkeypatch):
    calls = []
    sleeps = []

    def store_factory(*_args, **_kwargs):
        calls.append(True)
        raise _sqlite_error("database is locked", sqlite3.SQLITE_LOCKED)

    monkeypatch.setattr(dispatcher, "RcaControlStore", store_factory)

    with pytest.raises(sqlite3.OperationalError):
        dispatcher._open_resident_control_store(
            tmp_path / "control.sqlite3", sleep=sleeps.append
        )

    assert len(calls) == 4
    assert sleeps == [2, 5, 5]


def test_resident_store_startup_does_not_retry_nonbusy(tmp_path, monkeypatch):
    sleeps = []

    def store_factory(*_args, **_kwargs):
        raise sqlite3.OperationalError("no such table: rca_outbox")

    monkeypatch.setattr(dispatcher, "RcaControlStore", store_factory)

    with pytest.raises(sqlite3.OperationalError, match="no such table"):
        dispatcher._open_resident_control_store(
            tmp_path / "control.sqlite3", sleep=sleeps.append
        )

    assert sleeps == []


def test_resident_startup_gate_retries_wrapped_busy():
    calls = []
    sleeps = []
    wrapped = dispatcher.ExternalWriteFenceError("external_write_fence_epoch_not_current")
    wrapped.__cause__ = _sqlite_error("database is locked")

    def gate():
        calls.append(True)
        if len(calls) == 1:
            raise wrapped
        return "ready"

    assert dispatcher._retry_resident_store_operation(
        gate, phase="resident_activation_gate", sleep=sleeps.append
    ) == "ready"
    assert sleeps == [2]


def test_resident_initial_dispatch_guard_retries_busy():
    calls = []
    sleeps = []
    busy = _sqlite_error("database is locked")

    def guard():
        calls.append(True)
        if len(calls) == 1:
            raise busy
        return None

    assert dispatcher._retry_resident_store_operation(
        guard,
        phase="resident_initial_dispatch_guard",
        sleep=sleeps.append,
    ) is None
    assert len(calls) == 2
    assert sleeps == [2]


def test_stored_unsupported_profile_is_terminal_before_preread(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    result = store.ingest_record(
        _profile_snapshot_record(301, "6841983153"),
        policy=_profile_snapshot_policy(),
        submit_enabled=True,
    )
    assert result.decision == "accepted"
    claim = store.claim_outbox(lease_owner="profile-terminal-test")
    assert claim is not None
    admission, event = dispatcher._validated_claim_contract(claim)

    error = dispatcher._stored_profile_terminal_error(
        claim=claim,
        admission=admission,
        event=event,
    )

    assert error is not None
    assert error[0] == "business_profile_unsupported"
    assert "no G1Q3 evaluator" in error[1]


def test_stored_unresolved_profile_keeps_preread_path(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    result = store.ingest_record(
        _profile_snapshot_record(302, ""),
        policy=_profile_snapshot_policy(),
        submit_enabled=True,
    )
    assert result.decision == "accepted"
    claim = store.claim_outbox(lease_owner="profile-unresolved-test")
    assert claim is not None
    admission, event = dispatcher._validated_claim_contract(claim)

    assert (
        dispatcher._stored_profile_terminal_error(
            claim=claim,
            admission=admission,
            event=event,
        )
        is None
    )


def test_stored_adapter_pending_profile_is_terminal_before_preread(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    result = store.ingest_record(
        _profile_snapshot_record(303, "7019637554"),
        policy=_profile_snapshot_policy(),
        submit_enabled=True,
    )
    assert result.decision == "accepted"
    claim = store.claim_outbox(lease_owner="profile-adapter-terminal-test")
    assert claim is not None
    admission, event = dispatcher._validated_claim_contract(claim)

    error = dispatcher._stored_profile_terminal_error(
        claim=claim,
        admission=admission,
        event=event,
    )

    assert error is not None
    assert error[0] == "business_profile_adapter_not_ready"
    assert "input adapter is not ready" in error[1]


@pytest.mark.parametrize(
    ("option_ids", "expected_error"),
    (
        (("6841983153",), "business_profile_unsupported"),
        (("6841983153", "7019637554"), "business_profile_conflict"),
    ),
)
def test_unsupported_profile_dispatch_stops_before_all_execution_boundaries(
    tmp_path,
    option_ids,
    expected_error,
):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    record = _profile_snapshot_record(304, option_ids[0])
    if len(option_ids) > 1:
        payload = json.loads(record.value)
        payload["fields"][0]["field_value"] = list(option_ids)
        record = replace(
            record,
            value=json.dumps(payload, sort_keys=True).encode(),
        )
    result = store.ingest_record(
        record,
        policy=_profile_snapshot_policy(),
        submit_enabled=True,
    )
    assert result.decision == "accepted"
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path, enabled=True),
        hermes_home=tmp_path,
    )
    boundary_calls = []

    def forbidden_boundary(name):
        def invoke(*_args, **_kwargs):
            boundary_calls.append(name)
            raise AssertionError(f"{name} must not run for an outbox-only terminal")

        return invoke

    instance = dispatcher.OutboxDispatcher(
        store=store,
        config=config,
        enrich=forbidden_boundary("enrich"),
        storage_admission=forbidden_boundary("storage_admission"),
        submit=forbidden_boundary("vm_submit"),
        derived_capacity_reservation=forbidden_boundary("capacity_reservation"),
        lease_owner="profile-outbox-only-test",
    )
    instance._delivery_backpressure_outcome = lambda: None

    outcome = instance.dispatch_one()

    assert outcome.status == "quarantined"
    assert outcome.error_code == expected_error
    assert boundary_calls == []
    [outbox] = store.list_rows("rca_outbox")
    assert outbox["status"] == "quarantined"
    assert outbox["last_error_code"] == expected_error
    assert outbox["result_json"] is None


def _config_env(
    tmp_path, *, enabled: bool = False, canonical_layout: bool = False
) -> dict[str, str]:
    control_db_path = (
        tmp_path
        / "runtime"
        / "pnc_agent"
        / "feishu_issue_kafka_rca"
        / "control.sqlite3"
        if canonical_layout
        else tmp_path / "control.sqlite3"
    )
    return {
        "HERMES_RCA_OUTBOX_DISPATCH_ENABLED": str(enabled).lower(),
        "HERMES_RCA_OUTBOX_STORAGE_ADMISSION_ENABLED": str(enabled).lower(),
        "HERMES_RCA_OUTBOX_DERIVED_CAPACITY_RESERVATION_ENABLED": str(enabled).lower(),
        "HERMES_RCA_OUTBOX_DELIVERY_BACKPRESSURE_ENABLED": str(enabled).lower(),
        "HERMES_RCA_OUTBOX_CONTROL_DB_PATH": str(control_db_path),
        "HERMES_RCA_OUTBOX_HEALTH_PATH": str(tmp_path / "health.json"),
    }


def _post_claim_dispatcher(tmp_path, monkeypatch, exc):
    claim = SimpleNamespace(outbox_id=1, submission_key="submission", attempt=1)
    store = SimpleNamespace(
        dispatcher_circuit=lambda: SimpleNamespace(is_open=False),
        claim_outbox=lambda **_kwargs: claim,
    )
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path, enabled=True), hermes_home=tmp_path
    )
    instance = dispatcher.OutboxDispatcher(
        store=store,
        config=config,
        enrich=lambda _event: None,
        storage_admission=lambda _request: {},
        submit=lambda *_args: {},
        derived_capacity_reservation=lambda _request: None,
    )
    instance._delivery_backpressure_outcome = lambda: None
    instance._retry = lambda *_args, **_kwargs: pytest.fail(
        "store failure must not append an outbox retry mutation"
    )
    monkeypatch.setattr(
        dispatcher,
        "_validated_claim_contract",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(exc),
    )
    return instance


def test_post_claim_busy_escapes_without_retry_mutation(tmp_path, monkeypatch):
    instance = _post_claim_dispatcher(
        tmp_path, monkeypatch, _sqlite_error("database is locked")
    )

    with pytest.raises(sqlite3.OperationalError, match="database is locked"):
        instance.dispatch_one()


def test_storage_renew_busy_wrapper_escapes_without_retry_mutation(
    tmp_path, monkeypatch
):
    busy = _sqlite_error("database is locked")
    instance = _post_claim_dispatcher(tmp_path, monkeypatch, busy)
    refs = SimpleNamespace(
        project_key="project",
        work_item_id="issue-1",
        work_item_type_key="issue",
    )
    monkeypatch.setattr(
        dispatcher,
        "_validated_claim_contract",
        lambda *_args, **_kwargs: (SimpleNamespace(source_refs=refs), {}),
    )
    monkeypatch.setattr(
        dispatcher, "_stored_profile_terminal_error", lambda **_kwargs: None
    )
    instance.enrich = lambda _event: dispatcher.RcaIssueContext(
        project_key=refs.project_key,
        work_item_id=refs.work_item_id,
        work_item_type=refs.work_item_type_key,
    )
    renew_calls = []

    def renew(_claim):
        renew_calls.append(True)
        if len(renew_calls) == 2:
            raise busy

    instance._renew = renew
    instance.storage_admission = lambda _request: pytest.fail(
        "storage boundary must not run after renew BUSY"
    )
    instance._handle_dispatch_error = lambda *_args: pytest.fail(
        "wrapped store BUSY must not be converted to durable retry"
    )

    with pytest.raises(dispatcher.DispatchCircuitError) as raised:
        instance.dispatch_one()

    assert raised.value.code == "storage_admission_call_failed"
    assert raised.value.__cause__ is busy


def test_dispatch_loop_continues_after_store_busy_in_same_process():
    attempts = []

    def dispatch_batch():
        attempts.append(True)
        if len(attempts) == 1:
            raise _sqlite_error("database is locked")
        return [dispatcher.DispatchOutcome(status="idle")]

    instance = SimpleNamespace(
        stats=dispatcher.DispatchStats(),
        config=SimpleNamespace(
            poll_interval_seconds=0.01,
            circuit_poll_interval_seconds=0.02,
        ),
        dispatch_batch=dispatch_batch,
    )
    health_writes = []

    def health_write(**kwargs):
        health_writes.append(kwargs)
        first_idle = kwargs["state"] == "idle" and sum(
            item["state"] == "idle" for item in health_writes
        ) == 1
        if kwargs["state"] == "starting" or first_idle:
            return dispatcher.DispatchOutcome(
                status="store_busy", error_code="control_store_busy"
            )
        return None

    health = SimpleNamespace(
        write=health_write,
        heartbeat=lambda **_kwargs: True,
        dispatch_guard_outcome=lambda: None,
    )

    dispatcher.run_dispatch_loop(
        instance,
        health,
        stop_requested=lambda: len(attempts) >= 3,
        sleep=lambda _seconds: None,
        heartbeat_interval_seconds=60,
    )

    assert len(attempts) == 3
    assert instance.stats.store_busy == 1
    assert [item["state"] for item in health_writes] == [
        "starting",
        "store_busy",
        "idle",
        "idle",
    ]


def test_dispatch_loop_raises_nonbusy_operational_error():
    instance = SimpleNamespace(
        stats=dispatcher.DispatchStats(),
        config=SimpleNamespace(
            poll_interval_seconds=0.01,
            circuit_poll_interval_seconds=0.02,
        ),
        dispatch_batch=lambda: (_ for _ in ()).throw(
            _sqlite_error("no such table: rca_outbox")
        ),
    )
    health = SimpleNamespace(
        write=lambda **_kwargs: None,
        heartbeat=lambda **_kwargs: True,
        dispatch_guard_outcome=lambda: None,
    )

    with pytest.raises(sqlite3.OperationalError, match="no such table"):
        dispatcher.run_dispatch_loop(
            instance,
            health,
            stop_requested=lambda: False,
            heartbeat_interval_seconds=60,
        )


def test_submission_receipt_binds_preread_work_item_title():
    submission_key = "g1q3-rca-s1-" + "a" * 64
    title_binding = dispatcher._submission_work_item_title_binding(
        SimpleNamespace(work_item={"title": "ACC-右车近距离切入ACC不减速"})
    )

    receipt = dispatcher._submission_receipt(
        {
            "task": {"task_id": submission_key, "state": "submitted"},
            "success": True,
            "deduped": False,
            "created": True,
            "returncode": 0,
        },
        submission_key=submission_key,
        work_item_title_binding=title_binding,
        capacity_admission_summary={},
        derived_capacity_reservation_receipt={},
    )

    assert receipt["work_item"] == {
        "title": "ACC-右车近距离切入ACC不减速",
        "title_sha256": dispatcher.issue_title_sha256("ACC-右车近距离切入ACC不减速"),
    }


def test_config_does_not_require_or_project_legacy_release_identity(tmp_path):
    env = _config_env(tmp_path)
    env["HERMES_RCA_OUTBOX_STORAGE_RESERVATION_ENABLED"] = "true"
    config = dispatcher.DispatcherConfig.from_env(env, hermes_home=tmp_path)

    public = config.public_dict()
    assert config.activation_required is False
    assert public["activation_required"] is False
    assert "capacity_mode" not in public
    assert "release_id" not in public
    assert "release_id" not in config.runtime_public_dict()
    assert "bootstrap_epoch_id" not in public
    assert not hasattr(config, "storage_reservation_enabled")
    assert "storage_reservation_enabled" not in public


def test_target_submission_key_is_exposed_only_as_an_explicit_config_binding(
    tmp_path,
):
    key = "g1q3-rca-s1-" + "a" * 64
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )
    targeted = replace(config, only_submission_key=key)

    assert targeted.public_dict()["only_submission_key"] == key
    assert config.public_dict()["only_submission_key"] is None


def test_target_submission_key_requires_once(monkeypatch, tmp_path, capsys):
    key = "g1q3-rca-s1-" + "b" * 64
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )
    monkeypatch.setattr(dispatcher, "load_dispatcher_environment", lambda _path: None)
    monkeypatch.setattr(
        dispatcher.DispatcherConfig,
        "from_env",
        classmethod(lambda _cls: config),
    )

    assert dispatcher.main(["--only-submission-key", key]) == 2
    error = json.loads(capsys.readouterr().err)
    assert error["message"] == "outbox_target_requires_once_and_valid_key"


def test_config_exposes_strict_activation_required(tmp_path):
    env = _config_env(tmp_path)
    env["HERMES_RCA_OUTBOX_ACTIVATION_REQUIRED"] = "true"

    config = dispatcher.DispatcherConfig.from_env(env, hermes_home=tmp_path)

    assert config.activation_required is True
    assert config.public_dict()["activation_required"] is True


def test_config_rejects_declarative_feishu_writeback_enablement(tmp_path):
    env = _config_env(tmp_path)
    env["HERMES_RCA_OUTBOX_ALLOW_FEISHU_WRITEBACK"] = "true"

    with pytest.raises(ValueError, match="declarative-only"):
        dispatcher.DispatcherConfig.from_env(env, hermes_home=tmp_path)


def test_config_keeps_legacy_feishu_writeback_projection_false(tmp_path):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )

    assert config.allow_feishu_writeback is False
    assert config.public_dict()["allow_feishu_writeback"] is False


@pytest.mark.parametrize("value", ["1", "0", "yes", "on", "off", ""])
def test_activation_required_rejects_boolean_aliases(tmp_path, value):
    env = _config_env(tmp_path)
    env["HERMES_RCA_OUTBOX_ACTIVATION_REQUIRED"] = value

    with pytest.raises(ValueError, match="exactly true or false"):
        dispatcher.DispatcherConfig.from_env(env, hermes_home=tmp_path)


def test_delivery_backpressure_snapshot_uses_strict_activation_scope(tmp_path):
    env = _config_env(tmp_path)
    env["HERMES_RCA_OUTBOX_ACTIVATION_REQUIRED"] = "true"
    config = dispatcher.DispatcherConfig.from_env(env, hermes_home=tmp_path)
    RcaControlStore(config.control_db_path)
    snapshot = dispatcher.RcaDeliveryStore(
        config.delivery_db_path
    ).backpressure_snapshot()
    calls = []

    class ProbeDeliveryStore:
        def backpressure_snapshot(self, *, now, activation_required):
            calls.append((now, activation_required))
            return snapshot

    instance = object.__new__(dispatcher.OutboxDispatcher)
    instance.config = config
    instance.delivery_store = ProbeDeliveryStore()
    instance.now = lambda: datetime(2026, 8, 8, tzinfo=timezone.utc)
    instance.stats = dispatcher.DispatchStats()
    instance._delivery_backpressure_active = False
    instance._last_delivery_snapshot = None
    instance._last_delivery_error = None

    assert instance._delivery_backpressure_outcome() is None
    assert calls == [(datetime(2026, 8, 8, tzinfo=timezone.utc), True)]


def test_delivery_circuit_does_not_block_upstream_rca_submission(tmp_path):
    env = _config_env(tmp_path)
    config = dispatcher.DispatcherConfig.from_env(env, hermes_home=tmp_path)
    RcaControlStore(config.control_db_path)
    delivery_store = dispatcher.RcaDeliveryStore(config.delivery_db_path)
    delivery_store.open_delivery_dispatcher_circuit(
        reason_code="report_public_origin_invalid",
        now=datetime(2026, 8, 8, tzinfo=timezone.utc),
    )
    instance = object.__new__(dispatcher.OutboxDispatcher)
    instance.config = config
    instance.delivery_store = delivery_store
    instance.now = lambda: datetime(2026, 8, 8, tzinfo=timezone.utc)
    instance.stats = dispatcher.DispatchStats()
    instance._delivery_backpressure_active = False
    instance._last_delivery_snapshot = None
    instance._last_delivery_error = None

    assert instance._delivery_backpressure_outcome() is None
    assert (
        instance._last_delivery_snapshot["delivery_dispatcher_circuit"]["state"]
        == "open"
    )
    assert instance.stats.delivery_circuit_blocked == 0


def test_dispatcher_renew_uses_current_steady_lease_contract(tmp_path):
    env = _config_env(tmp_path)
    env["HERMES_RCA_OUTBOX_ACTIVATION_REQUIRED"] = "true"
    calls = []
    instance = object.__new__(dispatcher.OutboxDispatcher)
    instance.config = dispatcher.DispatcherConfig.from_env(
        env,
        hermes_home=tmp_path,
    )
    instance.store = SimpleNamespace(
        extend_outbox_lease=lambda **kwargs: calls.append(kwargs)
    )
    instance.now = lambda: datetime.now(timezone.utc)
    claim = SimpleNamespace(
        outbox_id=17,
        lease_token="lease-token",
        lease_owner="lease-owner",
    )

    instance._renew(claim)

    assert calls[0]["outbox_id"] == 17
    assert calls[0]["lease_token"] == "lease-token"
    assert calls[0]["lease_owner"] == "lease-owner"
    assert calls[0]["lease_seconds"] == instance.config.lease_seconds
    assert "activation_required" not in calls[0]


def test_enabled_resident_without_epoch_exits_before_dispatcher_creation(
    tmp_path,
    monkeypatch,
    capsys,
):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path, enabled=True),
        hermes_home=tmp_path,
    )
    path, _migration = _physical_v15_delivery_fixture(tmp_path)
    assert path == config.control_db_path
    with sqlite3.connect(path) as conn:
        conn.execute(
            "UPDATE rca_activation_epochs "
            "SET state = 'retired', is_current = 0, "
            "retired_at = COALESCE(retired_at, updated_at) "
            "WHERE is_current = 1"
        )
    store = RcaControlStore(
        path,
        require_current=True,
        allow_successor_write=True,
    )
    constructed = False

    def unexpected_dispatcher(*_args, **_kwargs):
        nonlocal constructed
        constructed = True
        raise AssertionError("dispatcher must not start without an active epoch")

    monkeypatch.setattr(dispatcher, "load_dispatcher_environment", lambda _path: None)
    monkeypatch.setattr(
        dispatcher.DispatcherConfig,
        "from_env",
        classmethod(lambda _cls: config),
    )
    monkeypatch.setattr(dispatcher, "OutboxDispatcher", unexpected_dispatcher)

    assert dispatcher.main(["--once"]) == 2
    assert constructed is False
    assert store.list_rows("kafka_inbox") == []
    assert store.list_rows("rca_outbox") == []
    assert "resident_activation_epoch_missing" in capsys.readouterr().err


def test_enabled_startup_uses_live_current_store_with_active_wal(
    tmp_path,
    monkeypatch,
):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path, enabled=True),
        hermes_home=tmp_path,
    )
    _steady_control_store(config.control_db_path)
    _migrate_v14_fixture_to_v15(config.control_db_path)
    store_calls = []
    real_store = dispatcher.RcaControlStore

    def tracked_store(*args, **kwargs):
        store_calls.append(dict(kwargs))
        return real_store(*args, **kwargs)

    fake_dispatcher = SimpleNamespace(
        stats=dispatcher.DispatchStats(),
        delivery_backpressure_health=lambda: {},
        dispatch_batch=lambda: [dispatcher.DispatchOutcome(status="idle")],
    )
    fake_health = SimpleNamespace(
        runtime_identity=SimpleNamespace(to_dict=lambda: {}),
        dispatch_guard_outcome=lambda: None,
        write=lambda **_kwargs: None,
    )
    monkeypatch.setattr(dispatcher, "load_dispatcher_environment", lambda _path: None)
    monkeypatch.setattr(
        dispatcher.DispatcherConfig,
        "from_env",
        classmethod(lambda _cls: config),
    )
    monkeypatch.setattr(dispatcher, "RcaControlStore", tracked_store)
    monkeypatch.setattr(dispatcher, "OutboxDispatcher", lambda **_kwargs: fake_dispatcher)
    monkeypatch.setattr(dispatcher, "HealthReporter", lambda *_args, **_kwargs: fake_health)
    monkeypatch.setattr(
        RcaControlStore,
        "create_schema_probe_snapshot",
        classmethod(
            lambda _cls, *_args, **_kwargs: pytest.fail(
                "normal startup must not copy the control DB"
            )
        ),
    )

    wal_writer = sqlite3.connect(config.control_db_path)
    try:
        assert wal_writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
        wal_writer.execute("PRAGMA wal_autocheckpoint=0")
        wal_writer.execute(
            "INSERT INTO control_meta(key, value) VALUES(?, ?)",
            ("outbox_startup_active_wal", "present"),
        )
        wal_writer.commit()
        before = _sqlite_storage_identity(config.control_db_path)

        assert dispatcher.main(["--once"]) == 0

        after = _sqlite_storage_identity(config.control_db_path)
        assert after["db"] == before["db"]
        assert after["-wal"] == before["-wal"]
        assert (after["-shm"] is None) is (before["-shm"] is None)
    finally:
        wal_writer.close()

    assert store_calls == [
        {
            "require_current": True,
            "read_only": False,
            "allow_successor_read_only": False,
            "allow_successor_write": True,
        }
    ]


def test_once_returns_rc2_for_store_busy_outcome(tmp_path, monkeypatch, capsys):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path, enabled=True), hermes_home=tmp_path
    )
    store = _v15_control_store(config.control_db_path)
    store_busy = dispatcher.DispatchOutcome(
        status="store_busy", error_code="control_store_busy"
    )
    fake_dispatcher = SimpleNamespace(
        stats=dispatcher.DispatchStats(),
        delivery_backpressure_health=lambda: {},
        dispatch_batch=lambda: [dispatcher.DispatchOutcome(status="idle")],
    )
    fake_health = SimpleNamespace(
        runtime_identity=SimpleNamespace(to_dict=lambda: {}),
        dispatch_guard_outcome=lambda: None,
        write=lambda **_kwargs: store_busy,
    )
    monkeypatch.setattr(dispatcher, "load_dispatcher_environment", lambda _path: None)
    monkeypatch.setattr(
        dispatcher.DispatcherConfig,
        "from_env",
        classmethod(lambda _cls: config),
    )
    monkeypatch.setattr(dispatcher, "_open_resident_control_store", lambda _path: store)
    monkeypatch.setattr(
        dispatcher, "OutboxDispatcher", lambda **_kwargs: fake_dispatcher
    )
    monkeypatch.setattr(
        dispatcher, "HealthReporter", lambda *_args, **_kwargs: fake_health
    )

    assert dispatcher.main(["--once"]) == 2
    outcomes = json.loads(capsys.readouterr().out)
    assert [item["status"] for item in outcomes] == ["idle", "store_busy"]


def _successor_read_only_capability() -> dict[str, object]:
    return {
        "observed_control_schema_version": "pnc_rca_control_store_v15",
        "binary_write_schema_version": "pnc_rca_control_store_v15",
        "mode": "successor_read_only",
        "read_supported": True,
        "write_enabled": False,
        "work_admission_enabled": False,
        "lease_acquisition_enabled": False,
        "external_effect_enabled": False,
    }


def test_successor_read_only_resident_writes_fresh_quiescent_health_only(
    tmp_path,
    monkeypatch,
):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path, enabled=True),
        hermes_home=tmp_path,
    )
    calls = []

    class SuccessorStore:
        def schema_runtime_capability(self):
            return _successor_read_only_capability()

        def health(self):
            calls.append("health")
            return {
                "ok": False,
                "process_healthy": True,
                "schema_version": "pnc_rca_control_store_v15",
            }

    def store_factory(*_args, **kwargs):
        calls.append(("store", kwargs))
        return SuccessorStore()

    monkeypatch.setattr(dispatcher, "load_dispatcher_environment", lambda _path: None)
    monkeypatch.setattr(
        dispatcher.DispatcherConfig,
        "from_env",
        classmethod(lambda _cls: config),
    )
    monkeypatch.setattr(dispatcher, "RcaControlStore", store_factory)
    monkeypatch.setattr(
        dispatcher,
        "require_resident_activation_epoch",
        lambda *_args, **_kwargs: pytest.fail("activation writer gate must not run"),
    )
    monkeypatch.setattr(
        dispatcher,
        "OutboxDispatcher",
        lambda *_args, **_kwargs: pytest.fail("dispatcher must not be created"),
    )
    monkeypatch.setattr(dispatcher.signal, "signal", lambda *_args: None)

    assert dispatcher.main(["--once"]) == 0

    [store_call, health_call] = calls
    assert store_call[0] == "store"
    assert store_call[1]["allow_successor_read_only"] is False
    assert store_call[1]["allow_successor_write"] is True
    assert health_call == "health"
    payload = json.loads(config.health_path.read_text(encoding="utf-8"))
    assert payload["mode"] == "successor_read_only"
    assert payload["ready"] is False
    assert payload["processing"] is False
    assert payload["healthy"] is True
    assert payload["process_healthy"] is True
    assert payload["business_ready"] is False
    assert payload["ok"] is False
    assert payload["schema_runtime_capability"] == (
        _successor_read_only_capability()
    )
    assert payload["stats"] == dispatcher.asdict(dispatcher.DispatchStats())
    observed = dispatcher.read_health_status(config)
    assert observed["ok"] is False
    assert observed["liveness_ok"] is True


def test_real_v15_outbox_dry_run_preserves_db_wal_shm_and_does_no_work(
    tmp_path,
    monkeypatch,
):
    path, _migration = _physical_v15_delivery_fixture(tmp_path)
    wal_writer = sqlite3.connect(path)
    assert wal_writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
    wal_writer.execute("PRAGMA wal_autocheckpoint=0")
    wal_writer.execute(
        "INSERT INTO rca_delivery_meta(key, value) VALUES(?, ?)",
        ("outbox_resident_live_wal_fixture", "present"),
    )
    wal_writer.commit()
    assert Path(f"{path}-wal").is_file()
    assert Path(f"{path}-shm").is_file()
    before = _sqlite_storage_identity(path)
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path, enabled=True),
        hermes_home=tmp_path,
    )

    monkeypatch.setattr(dispatcher, "load_dispatcher_environment", lambda _path: None)
    monkeypatch.setattr(
        dispatcher.DispatcherConfig,
        "from_env",
        classmethod(lambda _cls: config),
    )
    monkeypatch.setattr(
        dispatcher,
        "require_resident_activation_epoch",
        lambda *_args, **_kwargs: pytest.fail("activation writer gate must not run"),
    )
    monkeypatch.setattr(
        dispatcher,
        "OutboxDispatcher",
        lambda *_args, **_kwargs: pytest.fail("dispatcher must not be created"),
    )
    monkeypatch.setattr(dispatcher.signal, "signal", lambda *_args: None)

    try:
        assert dispatcher.main(["--dry-run"]) == 2
        after = _sqlite_storage_identity(path)
        assert after["db"] == before["db"]
        assert after["-wal"] == before["-wal"]
        assert (after["-shm"] is None) is (before["-shm"] is None)
    finally:
        wal_writer.close()

@pytest.mark.parametrize("mode", ["check_config", "dry_run"])
def test_successor_read_only_operator_modes_are_structured_red_without_work(
    tmp_path,
    monkeypatch,
    capsys,
    mode,
):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path, enabled=True),
        hermes_home=tmp_path,
    )
    config.control_db_path.write_bytes(b"fixture")
    calls = []

    class SuccessorStore:
        def schema_runtime_capability(self):
            return _successor_read_only_capability()

        def preview_dispatchable(self, **_kwargs):
            calls.append("preview")
            return []

    def store_factory(*_args, **kwargs):
        calls.append(("store", kwargs))
        return SuccessorStore()

    monkeypatch.setattr(dispatcher, "load_dispatcher_environment", lambda _path: None)
    monkeypatch.setattr(
        dispatcher.DispatcherConfig,
        "from_env",
        classmethod(lambda _cls: config),
    )
    monkeypatch.setattr(dispatcher, "RcaControlStore", store_factory)
    monkeypatch.setattr(
        dispatcher,
        "_open_resident_control_store",
        lambda *_args, **_kwargs: pytest.fail("operator mode used resident retries"),
    )

    flag = "--check-config" if mode == "check_config" else "--dry-run"
    assert dispatcher.main([flag]) == 2

    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False
    assert payload["mode"] == "successor_read_only"
    assert payload["ready"] is False
    assert payload["processing"] is False
    assert payload["operation"] == mode
    assert "preview" not in calls
    [store_call] = calls
    assert store_call[0] == "store"
    if mode == "check_config":
        assert store_call[1].get("read_only", False) is False
        assert store_call[1].get("allow_successor_read_only", False) is False
        assert store_call[1]["allow_successor_write"] is True
    else:
        assert store_call[1]["read_only"] is True
        assert store_call[1]["allow_successor_read_only"] is True
        assert store_call[1]["allow_successor_write"] is False


def test_disabled_check_config_does_not_probe_existing_control_db(
    tmp_path,
    monkeypatch,
    capsys,
):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path, enabled=False),
        hermes_home=tmp_path,
    )
    config.control_db_path.write_bytes(b"not-a-database")
    monkeypatch.setattr(dispatcher, "load_dispatcher_environment", lambda _path: None)
    monkeypatch.setattr(
        dispatcher.DispatcherConfig,
        "from_env",
        classmethod(lambda _cls: config),
    )
    monkeypatch.setattr(
        dispatcher,
        "RcaControlStore",
        lambda *_args, **_kwargs: pytest.fail("disabled check-config probed DB"),
    )

    assert dispatcher.main(["--check-config"]) == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True


def _patch_reset_cli(monkeypatch, config):
    env_path = config.control_db_path.parent / ".env"
    env_path.parent.mkdir(parents=True, exist_ok=True)
    env_path.write_text("HERMES_TEST=true\n", encoding="utf-8")
    monkeypatch.setattr(dispatcher, "get_hermes_home", lambda: env_path.parent)
    monkeypatch.setattr(
        dispatcher,
        "load_dispatcher_environment",
        lambda _path: env_path,
    )
    monkeypatch.setattr(
        dispatcher.DispatcherConfig,
        "from_env",
        classmethod(lambda _cls, *_args, **_kwargs: config),
    )


def test_clear_circuit_plan_does_not_mutate_or_create_receipt(
    tmp_path, monkeypatch, capsys
):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )
    store = _v15_control_store(config.control_db_path)
    store.open_dispatcher_circuit(
        reason_code="snapshot_stale", reason_detail="offline test"
    )
    receipt = tmp_path / "reset.json"
    _patch_reset_cli(monkeypatch, config)

    assert (
        dispatcher.main([
            "--clear-circuit",
            "--operator",
            "owner@example.com",
            "--reason",
            "verify snapshot before rearm",
            "--receipt",
            str(receipt),
        ])
        == 0
    )

    result = json.loads(capsys.readouterr().out)
    assert result["mode"] == "plan"
    assert result["applied"] is False
    assert result["pre_state"]["state"] == "open"
    assert store.dispatcher_circuit().state == "open"
    assert not receipt.exists()


def test_clear_circuit_apply_writes_receipt_and_db_audit(tmp_path, monkeypatch, capsys):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )
    store = _v15_control_store(config.control_db_path)
    store.open_dispatcher_circuit(
        reason_code="snapshot_stale", reason_detail="offline test"
    )
    receipt = tmp_path / "reset.json"
    _patch_reset_cli(monkeypatch, config)

    assert (
        dispatcher.main([
            "--clear-circuit",
            "--operator",
            "owner@example.com",
            "--reason",
            "verify snapshot before rearm",
            "--apply",
            "--receipt",
            str(receipt),
        ])
        == 0
    )

    result = json.loads(capsys.readouterr().out)
    body = json.loads(receipt.read_text(encoding="utf-8"))
    assert result["applied"] is True
    assert result["receipt"] == str(receipt.absolute())
    assert result["receipt_sha256"]
    assert body["operator"] == "owner@example.com"
    assert body["reason"] == "verify snapshot before rearm"
    assert body["pre_state"]["state"] == "open"
    assert body["post_state"]["state"] == "closed"
    assert body["effect_delta"]["external_writes"] == 0
    assert body["receipt_fingerprint"] == canonical_json_sha256({
        key: value for key, value in body.items() if key != "receipt_fingerprint"
    })
    assert receipt.stat().st_mode & 0o777 == 0o444
    sidecar = receipt.with_name(receipt.name + ".sha256")
    assert sidecar.read_text(encoding="ascii").startswith(result["receipt_sha256"])
    assert store.dispatcher_circuit().state == "closed"
    assert store.dispatcher_circuit_reset_audit(body["reset_id"]) == body


def test_clear_circuit_duplicate_receipt_is_rejected_before_mutation(
    tmp_path, monkeypatch, capsys
):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )
    store = _v15_control_store(config.control_db_path)
    store.open_dispatcher_circuit(reason_code="snapshot_stale")
    receipt = tmp_path / "reset.json"
    _patch_reset_cli(monkeypatch, config)
    args = [
        "--clear-circuit",
        "--operator",
        "owner",
        "--reason",
        "first reset",
        "--apply",
        "--receipt",
        str(receipt),
    ]
    assert dispatcher.main(args) == 0
    capsys.readouterr()

    # Re-open the circuit to prove the duplicate path is checked before the
    # second mutation attempt.
    store.open_dispatcher_circuit(reason_code="reopened-for-negative-test")
    assert dispatcher.main(args) == 2
    assert "already_exists" in capsys.readouterr().err
    assert store.dispatcher_circuit().state == "open"


def test_clear_circuit_closed_state_fails_closed_without_plan_success(
    tmp_path, monkeypatch, capsys
):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )
    store = _v15_control_store(config.control_db_path)
    _patch_reset_cli(monkeypatch, config)

    assert (
        dispatcher.main([
            "--clear-circuit",
            "--operator",
            "owner",
            "--reason",
            "do not reset a closed circuit",
        ])
        == 2
    )
    assert "requires_open_circuit" in capsys.readouterr().err
    assert store.dispatcher_circuit().state == "closed"


def test_clear_circuit_receipt_materialization_failure_reports_recovery(
    tmp_path, monkeypatch, capsys
):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )
    store = _v15_control_store(config.control_db_path)
    store.open_dispatcher_circuit(reason_code="snapshot_stale")
    receipt = tmp_path / "reset.json"
    _patch_reset_cli(monkeypatch, config)
    writer = dispatcher._write_immutable_receipt

    def fail_materialization(*_args, **_kwargs):
        raise OSError("simulated receipt filesystem failure")

    monkeypatch.setattr(dispatcher, "_write_immutable_receipt", fail_materialization)
    assert (
        dispatcher.main([
            "--clear-circuit",
            "--operator",
            "owner",
            "--reason",
            "persist database audit before filesystem copy",
            "--apply",
            "--receipt",
            str(receipt),
        ])
        == 2
    )
    error = json.loads(capsys.readouterr().err)
    assert error["recovery_required"] is True
    assert error["meta_key"].startswith("rca_dispatcher_circuit_reset:")
    assert store.dispatcher_circuit().state == "closed"
    assert store.dispatcher_circuit_reset_audit(error["reset_id"]) is not None
    monkeypatch.setattr(dispatcher, "_write_immutable_receipt", writer)
    recovered = tmp_path / "recovered-reset.json"
    assert (
        dispatcher.main([
            "--materialize-reset",
            error["reset_id"],
            "--receipt",
            str(recovered),
        ])
        == 0
    )
    recovery_result = json.loads(capsys.readouterr().out)
    assert recovery_result["recovered"] is True
    assert (
        json.loads(recovered.read_text(encoding="utf-8"))["reset_id"]
        == error["reset_id"]
    )


def test_clear_circuit_rejects_relative_receipt_before_mutation(
    tmp_path, monkeypatch, capsys
):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )
    store = _v15_control_store(config.control_db_path)
    store.open_dispatcher_circuit(reason_code="snapshot_stale")
    _patch_reset_cli(monkeypatch, config)

    assert (
        dispatcher.main([
            "--clear-circuit",
            "--operator",
            "owner",
            "--reason",
            "absolute path required",
            "--apply",
            "--receipt",
            "relative-reset.json",
        ])
        == 2
    )
    assert "path_invalid" in capsys.readouterr().err
    assert store.dispatcher_circuit().state == "open"


@pytest.mark.parametrize(
    "extra,expected",
    [
        (["--reason", "reason"], "operator_and_reason_required"),
        (["--operator", "operator"], "operator_and_reason_required"),
        (
            ["--operator", "operator", "--reason", "reason", "--apply"],
            "receipt_required",
        ),
    ],
)
def test_clear_circuit_apply_requires_bounded_audit_inputs(
    tmp_path, monkeypatch, capsys, extra, expected
):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )
    store = RcaControlStore(config.control_db_path)
    store.open_dispatcher_circuit(reason_code="snapshot_stale")
    _patch_reset_cli(monkeypatch, config)

    assert dispatcher.main(["--clear-circuit", *extra]) == 2
    assert expected in capsys.readouterr().err
    assert store.dispatcher_circuit().state == "open"


def test_config_has_no_production_capacity_mode_selector(tmp_path):
    env = _config_env(tmp_path, canonical_layout=True)
    env["HERMES_RCA_PROD_CAPACITY_MODE"] = "bootstrap"

    config = dispatcher.DispatcherConfig.from_env(env, hermes_home=tmp_path)

    assert "capacity_mode" not in config.public_dict()


def test_default_submit_has_no_bootstrap_capacity_contract(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(
        "tools.vm_task_tool.vm_task_submit_service",
        lambda **kwargs: calls.append(kwargs) or {"success": True},
    )
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )
    dispatcher.default_submit(object(), SimpleNamespace(toolchain={}), config=config)
    assert not any("bootstrap" in key or key == "capacity_mode" for key in calls[0])


def test_default_submit_preserves_reconcile_only_dedupe(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(
        "tools.vm_task_tool.vm_task_submit_service",
        lambda **kwargs: calls.append(kwargs) or {"success": True},
    )
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )
    request = SimpleNamespace(
        toolchain={"derived_capacity_reservation": {"status": "released"}}
    )

    dispatcher.default_submit(object(), request, config=config)

    assert calls[0]["reconcile_only"] is True


def test_default_submit_defers_live_store_check_to_service_create_guard(
    monkeypatch, tmp_path
):
    submit_calls = []
    fence_checks = []
    monkeypatch.setattr(
        "tools.vm_task_tool.vm_task_submit_service",
        lambda **kwargs: submit_calls.append(kwargs) or {"success": True},
    )
    monkeypatch.setattr(
        dispatcher,
        "validate_snapshot_execution_bundle",
        lambda _value: SimpleNamespace(snapshot="immutable-bundle"),
    )
    monkeypatch.setattr(
        dispatcher,
        "_validate_vm_submit_fence",
        lambda **kwargs: fence_checks.append(kwargs),
    )
    config = replace(
        dispatcher.DispatcherConfig.from_env(
            _config_env(tmp_path), hermes_home=tmp_path
        ),
        w3_snapshot_read_mode="snapshot_required",
    )
    live_binding = {"epoch_id": "epoch-live", "ledger_id": 17}
    store = SimpleNamespace(
        validate_external_write_fence_binding=lambda _fence: live_binding
    )

    dispatcher.default_submit(
        object(),
        SimpleNamespace(toolchain={"w3_execution_snapshot": {"fixture": True}}),
        config=config,
        control_store=store,
    )

    assert len(fence_checks) == 1
    assert "control_store" not in fence_checks[0]
    authority = submit_calls[0]["live_write_fence_authority"]
    assert authority({"state": "issued"}) == live_binding


def test_default_submit_re_raises_live_fence_busy_after_guard_suppresses_create(
    monkeypatch, tmp_path
):
    busy = _sqlite_error("database is locked")

    def submit_service(**kwargs):
        try:
            kwargs["live_write_fence_authority"]({"state": "issued"})
        except Exception:
            return {
                "success": False,
                "created": False,
                "create_suppressed": True,
                "error_code": "vm_task_service_request_identity_mismatch",
            }
        raise AssertionError("authority BUSY was not raised")

    monkeypatch.setattr(
        "tools.vm_task_tool.vm_task_submit_service",
        submit_service,
    )
    monkeypatch.setattr(
        dispatcher,
        "validate_snapshot_execution_bundle",
        lambda _value: SimpleNamespace(snapshot="immutable-bundle"),
    )
    monkeypatch.setattr(
        dispatcher,
        "_validate_vm_submit_fence",
        lambda **_kwargs: None,
    )
    config = replace(
        dispatcher.DispatcherConfig.from_env(
            _config_env(tmp_path), hermes_home=tmp_path
        ),
        w3_snapshot_read_mode="snapshot_required",
    )
    store = SimpleNamespace(
        validate_external_write_fence_binding=lambda _fence: (_ for _ in ()).throw(
            busy
        )
    )

    with pytest.raises(sqlite3.OperationalError, match="database is locked"):
        dispatcher.default_submit(
            object(),
            SimpleNamespace(toolchain={"w3_execution_snapshot": {"fixture": True}}),
            config=config,
            control_store=store,
        )


def test_default_submit_fails_contract_if_fence_busy_did_not_suppress_create(
    monkeypatch, tmp_path
):
    busy = _sqlite_error("database is locked")

    def submit_service(**kwargs):
        try:
            kwargs["live_write_fence_authority"]({"state": "issued"})
        except Exception:
            return {"success": False, "created": False}
        raise AssertionError("authority BUSY was not raised")

    monkeypatch.setattr("tools.vm_task_tool.vm_task_submit_service", submit_service)
    monkeypatch.setattr(
        dispatcher,
        "validate_snapshot_execution_bundle",
        lambda _value: SimpleNamespace(snapshot="immutable-bundle"),
    )
    monkeypatch.setattr(dispatcher, "_validate_vm_submit_fence", lambda **_kwargs: None)
    config = replace(
        dispatcher.DispatcherConfig.from_env(
            _config_env(tmp_path), hermes_home=tmp_path
        ),
        w3_snapshot_read_mode="snapshot_required",
    )
    store = SimpleNamespace(
        validate_external_write_fence_binding=lambda _fence: (_ for _ in ()).throw(
            busy
        )
    )

    with pytest.raises(
        dispatcher.DispatchCircuitError, match="did not suppress create"
    ) as raised:
        dispatcher.default_submit(
            object(),
            SimpleNamespace(toolchain={"w3_execution_snapshot": {"fixture": True}}),
            config=config,
            control_store=store,
        )

    assert dispatcher._is_sqlite_busy_or_locked(raised.value) is False


def test_default_submit_rejects_contradictory_fence_busy_result(
    monkeypatch, tmp_path
):
    busy = _sqlite_error("database is locked")

    def submit_service(**kwargs):
        try:
            kwargs["live_write_fence_authority"]({"state": "issued"})
        except Exception:
            return {
                "success": True,
                "created": True,
                "create_suppressed": True,
            }
        raise AssertionError("authority BUSY was not raised")

    monkeypatch.setattr("tools.vm_task_tool.vm_task_submit_service", submit_service)
    monkeypatch.setattr(
        dispatcher,
        "validate_snapshot_execution_bundle",
        lambda _value: SimpleNamespace(snapshot="immutable-bundle"),
    )
    monkeypatch.setattr(dispatcher, "_validate_vm_submit_fence", lambda **_kwargs: None)
    config = replace(
        dispatcher.DispatcherConfig.from_env(
            _config_env(tmp_path), hermes_home=tmp_path
        ),
        w3_snapshot_read_mode="snapshot_required",
    )
    store = SimpleNamespace(
        validate_external_write_fence_binding=lambda _fence: (_ for _ in ()).throw(
            busy
        )
    )

    with pytest.raises(
        dispatcher.DispatchCircuitError, match="did not suppress create"
    ) as raised:
        dispatcher.default_submit(
            object(),
            SimpleNamespace(toolchain={"w3_execution_snapshot": {"fixture": True}}),
            config=config,
            control_store=store,
        )

    assert dispatcher._is_sqlite_busy_or_locked(raised.value) is False


def test_steady_health_requires_hmac_and_has_no_expiring_authorization(tmp_path):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path, enabled=True), hermes_home=tmp_path
    )
    reporter = object.__new__(dispatcher.HealthReporter)
    reporter.config = config
    reporter._admission_key_fingerprint_observer = lambda: "7" * 64

    status = reporter.capacity_admission_status()

    assert status["ready"] is True
    assert status["capacity_mode"] == "steady"
    assert "deadline" not in status["authorization"]
    assert "successful_sample_count" not in status["authorization"]
    assert dispatcher._health_capacity_admission_ok({
        "enabled": True,
        "config": config.public_dict(),
        "capacity_admission": status,
    })


def test_activation_required_health_is_red_without_current_epoch(tmp_path):
    env = _config_env(tmp_path, enabled=True)
    env["HERMES_RCA_OUTBOX_ACTIVATION_REQUIRED"] = "true"
    config = dispatcher.DispatcherConfig.from_env(env, hermes_home=tmp_path)
    store = RcaControlStore(config.control_db_path)
    workspace_runtime = dispatcher.WorkspaceRuntimeIdentity(
        root=tmp_path / "workspace-runtime",
        manifest_path=tmp_path / "workspace-runtime" / "manifest.json",
        creator_path=tmp_path / "workspace-runtime" / "bin" / "create_task_v2.py",
        manifest_sha256="a" * 64,
        closure_sha256="b" * 64,
        source_commit="c" * 40,
        file_sha256={path: "d" * 64 for path in dispatcher.WORKSPACE_RUNTIME_FILES},
    )
    reporter = dispatcher.HealthReporter(
        config,
        store,
        workspace_runtime_observer=lambda: workspace_runtime,
        admission_key_fingerprint_observer=lambda: "e" * 64,
    )

    reporter.write(state="idle", stats=dispatcher.DispatchStats())

    payload = json.loads(config.health_path.read_text(encoding="utf-8"))
    assert payload["store"]["ok"] is True
    assert payload["store"]["activation"]["configured"] is False
    assert payload["store"]["activation"]["production_active"] is False
    assert payload["ok"] is False
    assert payload["healthy"] is False
    assert payload["readiness"]["ready_for_dispatch"] is False


def test_health_write_store_busy_keeps_liveness_and_fails_readiness_closed(
    tmp_path,
):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path, enabled=True), hermes_home=tmp_path
    )
    reporter = object.__new__(dispatcher.HealthReporter)
    reporter.config = config
    reporter.store = SimpleNamespace(
        dispatcher_circuit=lambda: (_ for _ in ()).throw(
            _sqlite_error("database is locked")
        )
    )
    reporter.delivery_backpressure_status = None
    reporter.started_at = dispatcher._utc_iso()
    reporter.public_config = config.runtime_public_dict()
    reporter.runtime_identity = SimpleNamespace(to_dict=lambda: {})
    reporter.workspace_runtime_status = lambda: {"required": True, "ready": True}
    reporter.capacity_admission_status = lambda: {"required": True, "ready": True}
    published = []
    reporter._publish = published.append
    stats = dispatcher.DispatchStats()

    outcome = reporter.write(state="idle", stats=stats)

    assert outcome == dispatcher.DispatchOutcome(
        status="store_busy", error_code="control_store_busy"
    )
    assert stats.store_busy == 1
    [body] = published
    assert body["state"] == "store_busy"
    assert body["ok"] is False
    assert body["healthy"] is False
    assert body["readiness"]["ready_for_dispatch"] is False
    assert body["store"] == {"ok": False, "error": "control_store_busy"}


def test_health_write_nonbusy_error_remains_fail_closed(tmp_path):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path, enabled=True), hermes_home=tmp_path
    )
    reporter = object.__new__(dispatcher.HealthReporter)
    reporter.config = config
    reporter.store = SimpleNamespace(
        dispatcher_circuit=lambda: (_ for _ in ()).throw(
            sqlite3.OperationalError("no such table: control_meta")
        )
    )
    reporter.delivery_backpressure_status = None

    with pytest.raises(sqlite3.OperationalError, match="no such table"):
        reporter.write(state="idle", stats=dispatcher.DispatchStats())


def test_capacity_guard_fails_before_claim_when_admission_key_is_invalid(tmp_path):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path, enabled=True), hermes_home=tmp_path
    )
    opened = []
    reporter = object.__new__(dispatcher.HealthReporter)
    reporter.config = config
    reporter.store = SimpleNamespace(
        open_dispatcher_circuit=lambda **kwargs: opened.append(kwargs)
    )
    reporter.workspace_runtime_status = lambda: {"required": True, "ready": True}

    def invalid_key():
        raise dispatcher.RcaProdAdmissionError(
            "rca_prod_hmac_key_invalid", retryable=False
        )

    reporter._admission_key_fingerprint_observer = invalid_key

    outcome = reporter.dispatch_guard_outcome()

    assert outcome.status == "capacity_authorization_unavailable"
    assert outcome.error_code == "rca_prod_hmac_key_invalid"
    assert opened[0]["reason_code"] == "dispatcher_prod_admission_invalid"


def test_runtime_closure_includes_prod_admission_and_delivery_runtime():
    assert {
        "gateway/__init__.py",
        "gateway/pnc_rca_prod_admission.py",
        "gateway/record_only/runtime.py",
        "gateway/record_only/transport.py",
        "scripts/pnc_completion_notice_relay.py",
        "scripts/pnc_foxglove_delivery.py",
        "scripts/pnc_vm_task_sync.py",
        "tools/__init__.py",
    }.issubset(RCA_RUNTIME_RELATIVE_FILES)
    assert (
        "vm_task_service_rca_prod_admission_blocked" in dispatcher._SERVICE_ERROR_CODES
    )
    assert (
        "vm_task_service_rca_prod_admission_blocked"
        in dispatcher._GLOBAL_CIRCUIT_ERROR_CODES
    )


def test_vm_submit_fence_normalizes_stale_control_store_conflict(monkeypatch):
    fence = {"state": "issued", "activation_epoch_id": "epoch-old"}
    bundle = SimpleNamespace(
        snapshot=SimpleNamespace(write_fence=fence),
        creator_source_envelope=SimpleNamespace(),
    )
    monkeypatch.setattr(
        dispatcher,
        "validate_snapshot_execution_bundle",
        lambda value: value,
    )
    monkeypatch.setattr(
        dispatcher,
        "validate_write_fence_source_binding",
        lambda *_args, **_kwargs: {
            "issue_target": "issue",
            "thread_target": None,
            "chat_id": "chat",
            "target_set_sha256": "1" * 64,
        },
    )
    monkeypatch.setattr(
        dispatcher, "validate_write_fence", lambda *_args, **_kwargs: None
    )

    class StaleStore:
        def validate_external_write_fence_binding(self, _fence):
            raise dispatcher.RecordConflictError(
                "external_write_fence_epoch_not_current"
            )

    with pytest.raises(dispatcher.ExternalWriteFenceError) as raised:
        dispatcher._validate_vm_submit_fence(
            bundle=bundle,
            admission=SimpleNamespace(
                submission_key="submission",
                business_key="business",
                generation=1,
            ),
            now=datetime.now(timezone.utc),
            control_store=StaleStore(),
        )

    assert raised.value.code == "external_write_fence_epoch_not_current"


@pytest.mark.parametrize(
    "error_code",
    [
        "vm_task_service_rca_prod_admission_blocked",
        "vm_task_service_rca_prod_command_invalid",
    ],
)
def test_prod_admission_failure_routes_to_global_circuit(monkeypatch, error_code):
    assert error_code in dispatcher._SERVICE_ERROR_CODES
    assert error_code in dispatcher._GLOBAL_CIRCUIT_ERROR_CODES
    instance = object.__new__(dispatcher.OutboxDispatcher)
    claim = object()
    opened = []
    monkeypatch.setattr(
        instance,
        "_open_circuit",
        lambda actual_claim, code, detail: (
            opened.append((actual_claim, code, detail)) or "opened"
        ),
    )
    monkeypatch.setattr(
        instance,
        "_retry",
        lambda *_args, **_kwargs: pytest.fail("global admission failure was retried"),
    )
    monkeypatch.setattr(
        instance,
        "_quarantine",
        lambda *_args, **_kwargs: pytest.fail(
            "global admission failure quarantined one issue"
        ),
    )

    result = instance._handle_dispatch_error(
        claim,
        error_code,
        "rca_prod_hmac_key_invalid",
    )

    assert result == "opened"
    assert opened == [
        (
            claim,
            error_code,
            "rca_prod_hmac_key_invalid",
        )
    ]


def test_submit_failure_detail_keeps_bounded_creator_diagnostic():
    detail = dispatcher._submit_failure_detail(
        {
            "error": "creation outcome uncertain",
            "submit_diagnostic": {
                "returncode": 1,
                "stderr_last_line": (
                    "sqlite3.OperationalError: unable to open database file"
                ),
            },
        },
        "vm_task_service_submit_uncertain",
    )

    assert detail.startswith("creation outcome uncertain; submit_diagnostic=")
    assert '\"returncode\":1' in detail
    assert "sqlite3.OperationalError" in detail


def test_retryable_prod_admission_failure_does_not_require_global_circuit():
    assert dispatcher._is_retryable_prod_admission_failure(
        {"retryable": True}, "vm_task_service_rca_prod_admission_blocked"
    )
    assert not dispatcher._is_retryable_prod_admission_failure(
        {"retryable": False}, "vm_task_service_rca_prod_admission_blocked"
    )
    assert not dispatcher._is_retryable_prod_admission_failure(
        {"retryable": True}, "vm_task_service_rca_prod_command_invalid"
    )
