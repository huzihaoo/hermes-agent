from __future__ import annotations

import fcntl
import hashlib
import json
import os
import stat
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from scripts import pnc_rca_cutover_guard as guard


NOW = datetime(2026, 7, 13, 6, 0, tzinfo=timezone.utc)
MACHINE = {"source": "test_machine", "sha256": "9" * 64}
BOOT_ID = "boot-id-1234567890abcdef"


def _write_owner(path: Path, body: object) -> None:
    path.write_bytes(guard._canonical_json(body))
    path.chmod(0o600)


def _runtime_identity(
    *,
    pid: int = 41001,
    create_time: float = 1_783_650_000.0,
    root: Path = guard.CANONICAL_LIVE_ROOT,
) -> dict:
    live = {
        "schema_version": guard.RUNTIME_FILES_IDENTITY_SCHEMA_VERSION,
        "canonical_root": str(root),
        "root_identity": {
            "path": str(root),
            "device": 1,
            "inode": 2,
            "owner_uid": os.geteuid(),
            "mode": 0o700,
        },
        "files": {"gateway/run.py": {"sha256": "1" * 64}},
        "runtime_files_sha256": "2" * 64,
        "interpreter": {"sha256": "3" * 64},
    }
    return {
        "schema_version": guard.GATEWAY_RUNNING_OBSERVATION_SCHEMA_VERSION,
        "canonical_root": str(root),
        "launchd": {
            "label": guard.GATEWAY_LABEL,
            "loaded": True,
            "pid": pid,
            "state": "running",
        },
        "process": {
            "pid": pid,
            "process_create_time": create_time,
            "executable": str(root / ".venv/bin/python"),
            "cwd": str(root),
            "cmdline_sha256": "4" * 64,
            "loaded_runtime_closure_sha256": guard._sha256_json(live),
        },
        "live_runtime_identity": live,
    }


def _sidecar_identity() -> dict:
    return {
        "schema_version": "pnc_rca_feishu_sidecar_identity_v1",
        "state": "present",
        "path": "/Users/songying/.hermes/runtime/feishu_api_poll_state_v1.json",
        "sha256": "5" * 64,
        "revision": 7,
        "app_scope": "6" * 32,
    }


def _precutover_service_state(runtime: dict) -> dict:
    jobs = {}
    for label in guard.SERVICE_LABELS:
        loaded = label == guard.GATEWAY_LABEL
        jobs[label] = {
            "launchd": {
                "label": label,
                "loaded": loaded,
                "state": "running" if loaded else "absent",
                "pid": runtime["process"]["pid"] if loaded else None,
                "last_exit_status": None,
            },
            "plist": {
                "path": str(guard.CANONICAL_LAUNCH_AGENTS_ROOT / f"{label}.plist"),
                "state": "regular",
                "sha256": hashlib.sha256(label.encode()).hexdigest(),
                "size_bytes": len(label),
                "mode": "0644",
                "uid": os.geteuid(),
                "nlink": 1,
            },
        }
    return {
        "schema_version": guard.LIVE_SERVICE_STATE_SCHEMA_VERSION,
        "target_runtime_root": str(guard.CANONICAL_LIVE_ROOT),
        "labels": list(guard.SERVICE_LABELS),
        "jobs": jobs,
    }


def _stopped(runtime: dict, sidecar: dict) -> dict:
    return {
        "schema_version": guard.GATEWAY_WRITER_STOP_OBSERVATION_SCHEMA_VERSION,
        "canonical_root": str(guard.CANONICAL_LIVE_ROOT),
        "launchd": {
            "label": guard.GATEWAY_LABEL,
            "loaded": True,
            "pid": None,
            "state": "not_running",
        },
        "process_census": {
            "probe": "psutil_gateway_canonical_runtime_census_v1",
            "canonical_root": str(guard.CANONICAL_LIVE_ROOT),
            "matching_processes": [],
        },
        "live_runtime_identity": json.loads(
            json.dumps(runtime["live_runtime_identity"])
        ),
        "live_sidecar_identity": json.loads(json.dumps(sidecar)),
    }


@pytest.fixture
def lease_setup(tmp_path: Path):
    lock_dir = tmp_path / "locks"
    lock_dir.mkdir(mode=0o700)
    prepare = tmp_path / "release-prepare.json"
    approval = tmp_path / "approval.json"
    _write_owner(prepare, {"schema_version": "prepare_v1", "complete": True})
    _write_owner(approval, {"schema_version": "approval_v1", "decision": "approve"})
    runtime = _runtime_identity()
    inputs = guard.LeaseInputs(
        release_id="release-20260713-a",
        release_prepare_manifest=prepare,
        approval_receipt=approval,
        expected_live_runtime_identity=runtime,
        duration_seconds=3600,
    )
    return {
        "lock": lock_dir / "pnc-production-cutover.lock",
        "prepare": prepare,
        "approval": approval,
        "runtime": runtime,
        "inputs": inputs,
    }


def _acquire(
    setup: dict, *, runtime_observer=None, now: datetime = NOW, clock=None
):
    return guard.acquire_cutover_lease(
        setup["inputs"],
        lock_path=setup["lock"],
        runtime_observer=runtime_observer or (lambda: setup["runtime"]),
        machine_observer=lambda: MACHINE,
        boot_id_observer=lambda: BOOT_ID,
        holder_process_observer=lambda: {
            "pid": os.getpid(),
            "create_time": 1_783_650_010.0,
        },
        now=now,
        clock=clock or (lambda: now),
    )


def test_lease_binds_kernel_holder_inputs_and_runtime_after_lock(lease_setup) -> None:
    calls = 0

    def observer():
        nonlocal calls
        calls += 1
        return lease_setup["runtime"]

    with _acquire(lease_setup, runtime_observer=observer) as lease:
        assert calls == 1
        assert lease.path == lease_setup["lock"]
        assert lease.fingerprint == hashlib.sha256(lease.raw).hexdigest()
        assert len(lease.token) >= 16
        assert lease.body["lease_token_sha256"] == hashlib.sha256(
            lease.token.encode("utf-8")
        ).hexdigest()
        assert lease.body["holder"] == {
            "pid": os.getpid(),
            "process_create_time": 1_783_650_010.0,
            "boot_id": BOOT_ID,
            "machine_identity": MACHINE,
        }
        assert lease.body["release_prepare_manifest"]["sha256"] == hashlib.sha256(
            lease_setup["prepare"].read_bytes()
        ).hexdigest()
        assert stat.S_IMODE(lease_setup["lock"].stat().st_mode) == 0o600
        assert guard.doctor_cutover_lock(lock_path=lease_setup["lock"])[
            "kernel_lock_held"
        ] is True

    doctor = guard.doctor_cutover_lock(lock_path=lease_setup["lock"])
    assert doctor["state"] == "free"
    assert doctor["body_sha256"] == hashlib.sha256(
        lease_setup["lock"].read_bytes()
    ).hexdigest()


def test_lease_assert_active_rechecks_expiry_on_every_call(lease_setup) -> None:
    current = [NOW]
    with _acquire(lease_setup, clock=lambda: current[0]) as lease:
        lease.assert_active()
        current[0] = NOW + timedelta(seconds=3601)
        with pytest.raises(guard.CutoverGuardError) as error:
            lease.assert_active()

    assert error.value.code == "cutover_lease_expired"


def test_lease_assert_active_rejects_rename_replace_split_brain(lease_setup) -> None:
    lease = _acquire(lease_setup)
    replacement_descriptor = -1
    moved = lease_setup["lock"].with_name("moved-cutover.lock")
    try:
        os.rename(lease_setup["lock"], moved)
        lease_setup["lock"].write_text("{}\n")
        lease_setup["lock"].chmod(0o600)
        replacement_descriptor = os.open(lease_setup["lock"], os.O_RDWR)
        fcntl.flock(replacement_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)

        with pytest.raises(guard.CutoverGuardError) as error:
            lease.assert_active()
    finally:
        if replacement_descriptor >= 0:
            fcntl.flock(replacement_descriptor, fcntl.LOCK_UN)
            os.close(replacement_descriptor)
        lease.close()

    assert error.value.code == "cutover_lease_identity_changed"


def test_lock_contention_never_uses_body_staleness(lease_setup) -> None:
    with _acquire(lease_setup):
        body = json.loads(lease_setup["lock"].read_text())
        body["expires_at"] = (NOW - timedelta(days=1)).isoformat()
        raw = guard._canonical_json(body)
        descriptor = os.open(lease_setup["lock"], os.O_WRONLY)
        try:
            os.ftruncate(descriptor, 0)
            os.write(descriptor, raw)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

        with pytest.raises(guard.CutoverGuardError) as error:
            _acquire(lease_setup)

    assert error.value.code == "cutover_guard_lock_contended"


@pytest.mark.parametrize("attack", ["symlink", "hardlink", "permissions"])
def test_lock_identity_attacks_fail_closed(lease_setup, tmp_path: Path, attack: str) -> None:
    lock = lease_setup["lock"]
    if attack == "symlink":
        target = tmp_path / "target"
        target.write_text("")
        lock.symlink_to(target)
    else:
        lock.write_text("")
        lock.chmod(0o600)
        if attack == "hardlink":
            os.link(lock, tmp_path / "second-link")
        else:
            lock.chmod(0o644)

    with pytest.raises(guard.CutoverGuardError) as error:
        _acquire(lease_setup)

    assert error.value.code in {
        "cutover_guard_lock_unavailable",
        "cutover_guard_lock_identity_invalid",
    }


@pytest.mark.parametrize("attack", ["symlink", "hardlink", "permissions", "oversize"])
def test_bound_input_file_attacks_fail_before_lock(
    lease_setup, tmp_path: Path, attack: str
) -> None:
    approval = lease_setup["approval"]
    if attack == "symlink":
        target = tmp_path / "real-approval"
        _write_owner(target, {"schema_version": "approval_v1"})
        approval.unlink()
        approval.symlink_to(target)
    elif attack == "hardlink":
        os.link(approval, tmp_path / "approval-link")
    elif attack == "permissions":
        approval.chmod(0o644)
    else:
        approval.write_bytes(b"x" * (guard.MAX_JSON_BYTES + 1))
        approval.chmod(0o600)

    with pytest.raises(guard.CutoverGuardError) as error:
        _acquire(lease_setup)

    assert error.value.code.startswith("cutover_guard_approval_receipt_")
    assert not lease_setup["lock"].exists()


@pytest.mark.parametrize("drift", ["pid", "create_time", "runtime", "process"])
def test_after_lock_runtime_reobservation_rejects_competing_release(
    lease_setup, drift: str
) -> None:
    observed = json.loads(json.dumps(lease_setup["runtime"]))
    if drift == "pid":
        observed["launchd"]["pid"] += 1
        observed["process"]["pid"] += 1
    elif drift == "create_time":
        observed["process"]["process_create_time"] += 1
    elif drift == "runtime":
        observed["live_runtime_identity"]["runtime_files_sha256"] = "f" * 64
        observed["process"]["loaded_runtime_closure_sha256"] = guard._sha256_json(
            observed["live_runtime_identity"]
        )
    else:
        observed["process"]["cmdline_sha256"] = "f" * 64

    with pytest.raises(guard.CutoverGuardError) as error:
        _acquire(lease_setup, runtime_observer=lambda: observed)

    assert error.value.code == "cutover_guard_live_runtime_changed"


def test_lease_duration_is_bounded_to_two_hours(lease_setup) -> None:
    lease_setup["inputs"] = guard.LeaseInputs(
        **{**lease_setup["inputs"].__dict__, "duration_seconds": 7201}
    )
    with pytest.raises(guard.CutoverGuardError) as error:
        _acquire(lease_setup)
    assert error.value.code == "cutover_guard_lease_duration_invalid"


@pytest.mark.parametrize("drift", ["boot", "process_create_time", "machine"])
def test_active_lease_rejects_holder_boot_process_or_machine_drift(
    lease_setup, drift
) -> None:
    state = {
        "boot": BOOT_ID,
        "create_time": 1_783_650_010.0,
        "machine": MACHINE,
    }
    lease = guard.acquire_cutover_lease(
        lease_setup["inputs"],
        lock_path=lease_setup["lock"],
        runtime_observer=lambda: lease_setup["runtime"],
        machine_observer=lambda: state["machine"],
        boot_id_observer=lambda: state["boot"],
        holder_process_observer=lambda: {
            "pid": os.getpid(),
            "create_time": state["create_time"],
        },
        now=NOW,
        clock=lambda: NOW,
    )
    try:
        if drift == "boot":
            state["boot"] = "different-boot-1234567890"
        elif drift == "process_create_time":
            state["create_time"] += 1
        else:
            state["machine"] = {"source": "test_machine", "sha256": "f" * 64}
        with pytest.raises(guard.CutoverGuardError) as error:
            lease.assert_active()
    finally:
        lease.close()
    assert error.value.code == "cutover_lease_holder_changed"


def test_lease_binds_explicit_current_base_runtime_root(lease_setup, tmp_path) -> None:
    root = Path("/Users/songying/.hermes/runtime/releases/hermes-v0.18.2-fixture")
    runtime = _runtime_identity(root=root)
    lease_setup["inputs"] = guard.LeaseInputs(
        release_id=lease_setup["inputs"].release_id,
        release_prepare_manifest=lease_setup["prepare"],
        approval_receipt=lease_setup["approval"],
        expected_live_runtime_identity=runtime,
        canonical_live_root=root,
        allow_absent_rca_files=True,
        duration_seconds=3600,
    )
    lease_setup["runtime"] = runtime
    lease_setup["lock"] = tmp_path / "locks" / "base-runtime-cutover.lock"
    lease_setup["lock"].parent.mkdir(mode=0o700, exist_ok=True)

    with _acquire(lease_setup) as lease:
        lease.assert_active()
        assert lease.body["expected_live_runtime_identity"]["canonical_root"] == str(
            root
        )


def test_writer_stop_observation_binds_explicit_current_base_runtime_root() -> None:
    root = Path("/Users/songying/.hermes/runtime/releases/hermes-v0.18.2-fixture")
    runtime = _runtime_identity(root=root)
    sidecar = _sidecar_identity()
    observed = {
        "schema_version": guard.GATEWAY_WRITER_STOP_OBSERVATION_SCHEMA_VERSION,
        "canonical_root": str(root),
        "launchd": {
            "label": guard.GATEWAY_LABEL,
            "loaded": False,
            "pid": None,
            "state": "not_running",
        },
        "process_census": {
            "probe": "psutil_gateway_canonical_runtime_census_v1",
            "canonical_root": str(root),
            "matching_processes": [],
        },
        "live_runtime_identity": runtime["live_runtime_identity"],
        "live_sidecar_identity": sidecar,
    }

    assert guard.validate_writer_stop_observation(
        observed,
        expected_live_runtime_identity=runtime,
        expected_live_sidecar_identity=sidecar,
    ) == observed


def test_expired_lease_cannot_publish_writer_stop(lease_setup, tmp_path) -> None:
    sidecar = _sidecar_identity()
    stopped = _stopped(lease_setup["runtime"], sidecar)
    with _acquire(lease_setup) as lease:
        with pytest.raises(guard.CutoverGuardError) as error:
            guard.observe_writer_stop(
                lease,
                guard.WriterStopInputs(
                    hold_id="hold-20260713-a",
                    plan_sha256="7" * 64,
                    receipt_path=tmp_path / "writer-stop.json",
                    expected_live_sidecar_identity=sidecar,
                    precutover_service_state=_precutover_service_state(
                        lease_setup["runtime"]
                    ),
                ),
                writer_stop_observer=lambda: stopped,
                now=NOW + timedelta(seconds=3601),
            )
    assert error.value.code == "cutover_lease_expired"


def test_writer_stop_receipt_reobserves_around_atomic_publication(lease_setup, tmp_path):
    sidecar = _sidecar_identity()
    stopped = _stopped(lease_setup["runtime"], sidecar)
    calls = 0

    def observer():
        nonlocal calls
        calls += 1
        return stopped

    with _acquire(lease_setup) as lease:
        body = guard.observe_writer_stop(
            lease,
            guard.WriterStopInputs(
                hold_id="hold-20260713-a",
                plan_sha256="7" * 64,
                receipt_path=tmp_path / "receipts" / "writer-stop.json",
                expected_live_sidecar_identity=sidecar,
                precutover_service_state=_precutover_service_state(
                    lease_setup["runtime"]
                ),
            ),
            writer_stop_observer=observer,
            now=NOW,
        )

    assert calls == 3
    assert body["lease_fingerprint"] == lease.fingerprint
    assert body["old_gateway_process"] == lease_setup["runtime"]["process"]
    assert body["production_effects_executed"] is False
    receipt = tmp_path / "receipts" / "writer-stop.json"
    assert stat.S_IMODE(receipt.stat().st_mode) == 0o600
    owned, normalized = guard.read_writer_stop_receipt(receipt, now=NOW)
    assert owned.sha256 == hashlib.sha256(receipt.read_bytes()).hexdigest()
    assert normalized == body


def test_writer_stop_receipt_rejects_rehashed_precutover_gateway_pid(
    lease_setup, tmp_path
) -> None:
    sidecar = _sidecar_identity()
    stopped = _stopped(lease_setup["runtime"], sidecar)
    receipt = tmp_path / "writer-stop.json"
    with _acquire(lease_setup) as lease:
        guard.observe_writer_stop(
            lease,
            guard.WriterStopInputs(
                hold_id="hold-20260713-a",
                plan_sha256="7" * 64,
                receipt_path=receipt,
                expected_live_sidecar_identity=sidecar,
                precutover_service_state=_precutover_service_state(
                    lease_setup["runtime"]
                ),
            ),
            writer_stop_observer=lambda: stopped,
            now=NOW,
        )
    body = json.loads(receipt.read_text())
    body["precutover_service_state"]["jobs"][guard.GATEWAY_LABEL]["launchd"][
        "pid"
    ] += 1
    body["precutover_service_state_sha256"] = guard._sha256_json(
        body["precutover_service_state"]
    )
    _write_owner(receipt, body)

    with pytest.raises(guard.CutoverGuardError) as error:
        guard.read_writer_stop_receipt(receipt, now=NOW)

    assert error.value.code == "writer_stop_precutover_gateway_state_invalid"


@pytest.mark.parametrize(
    ("mutation", "code"),
    [
        (
            lambda value: value["launchd"].update(pid=51001, state="running"),
            "writer_stop_launchctl_state_invalid",
        ),
        (
            lambda value: value["process_census"]["matching_processes"].append({
                "pid": 51002,
                "process_create_time": 1_783_650_100.0,
                "cmdline_sha256": "8" * 64,
            }),
            "writer_stop_process_census_invalid",
        ),
        (
            lambda value: value["live_runtime_identity"].update(
                runtime_files_sha256="f" * 64
            ),
            "writer_stop_live_identity_changed",
        ),
        (
            lambda value: value["live_sidecar_identity"].update(revision=8),
            "writer_stop_live_identity_changed",
        ),
    ],
)
def test_writer_stop_hostile_runtime_states_fail_closed(
    lease_setup, tmp_path, mutation, code
) -> None:
    sidecar = _sidecar_identity()
    stopped = _stopped(lease_setup["runtime"], sidecar)
    mutation(stopped)
    with _acquire(lease_setup) as lease:
        with pytest.raises(guard.CutoverGuardError) as error:
            guard.observe_writer_stop(
                lease,
                guard.WriterStopInputs(
                    hold_id="hold-20260713-a",
                    plan_sha256="7" * 64,
                    receipt_path=tmp_path / "writer-stop.json",
                    expected_live_sidecar_identity=sidecar,
                    precutover_service_state=_precutover_service_state(
                        lease_setup["runtime"]
                    ),
                ),
                writer_stop_observer=lambda: stopped,
                now=NOW,
            )
    assert error.value.code == code
    assert not (tmp_path / "writer-stop.json").exists()


def test_writer_stop_detects_drift_before_and_after_publication(lease_setup, tmp_path):
    sidecar = _sidecar_identity()
    stable = _stopped(lease_setup["runtime"], sidecar)
    drifted = json.loads(json.dumps(stable))
    drifted["live_sidecar_identity"]["revision"] = 8
    values = iter((stable, stable, drifted))
    receipt = tmp_path / "writer-stop.json"

    with _acquire(lease_setup) as lease:
        with pytest.raises(guard.CutoverGuardError) as error:
            guard.observe_writer_stop(
                lease,
                guard.WriterStopInputs(
                    hold_id="hold-20260713-a",
                    plan_sha256="7" * 64,
                    receipt_path=receipt,
                    expected_live_sidecar_identity=sidecar,
                    precutover_service_state=_precutover_service_state(
                        lease_setup["runtime"]
                    ),
                ),
                writer_stop_observer=lambda: next(values),
                now=NOW,
            )
    assert error.value.code in {
        "writer_stop_live_identity_changed",
        "writer_stop_observation_drift",
    }
    assert receipt.exists()


@pytest.mark.parametrize("attack", ["symlink", "hardlink", "permissions", "oversize", "duplicate"])
def test_writer_stop_receipt_file_attacks_fail_closed(
    lease_setup, tmp_path, attack
) -> None:
    sidecar = _sidecar_identity()
    stopped = _stopped(lease_setup["runtime"], sidecar)
    receipt = tmp_path / "writer-stop.json"
    with _acquire(lease_setup) as lease:
        body = guard.observe_writer_stop(
            lease,
            guard.WriterStopInputs(
                hold_id="hold-20260713-a",
                plan_sha256="7" * 64,
                receipt_path=receipt,
                expected_live_sidecar_identity=sidecar,
                precutover_service_state=_precutover_service_state(
                    lease_setup["runtime"]
                ),
            ),
            writer_stop_observer=lambda: stopped,
            now=NOW,
        )
    if attack == "symlink":
        target = tmp_path / "target"
        target.write_bytes(receipt.read_bytes())
        target.chmod(0o600)
        receipt.unlink()
        receipt.symlink_to(target)
    elif attack == "hardlink":
        os.link(receipt, tmp_path / "receipt-link")
    elif attack == "permissions":
        receipt.chmod(0o644)
    elif attack == "oversize":
        receipt.write_bytes(b"x" * (guard.MAX_JSON_BYTES + 1))
        receipt.chmod(0o600)
    else:
        raw = receipt.read_text()
        receipt.write_text(raw[:-2] + ',"release_id":"duplicate"}\n')
        receipt.chmod(0o600)

    with pytest.raises(guard.CutoverGuardError):
        guard.read_writer_stop_receipt(receipt, now=NOW)
    assert body["schema_version"] == guard.WRITER_STOP_RECEIPT_SCHEMA_VERSION


def test_writer_stop_receipt_stale_or_swapped_fails(lease_setup, tmp_path) -> None:
    sidecar = _sidecar_identity()
    stopped = _stopped(lease_setup["runtime"], sidecar)
    receipt = tmp_path / "writer-stop.json"
    with _acquire(lease_setup) as lease:
        guard.observe_writer_stop(
            lease,
            guard.WriterStopInputs(
                hold_id="hold-20260713-a",
                plan_sha256="7" * 64,
                receipt_path=receipt,
                expected_live_sidecar_identity=sidecar,
                precutover_service_state=_precutover_service_state(
                    lease_setup["runtime"]
                ),
            ),
            writer_stop_observer=lambda: stopped,
            now=NOW,
        )
    with pytest.raises(guard.CutoverGuardError) as stale:
        guard.read_writer_stop_receipt(
            receipt,
            now=NOW + timedelta(seconds=guard.MAX_WRITER_STOP_AGE_SECONDS + 1),
        )
    assert stale.value.code == "writer_stop_receipt_stale"

    body = json.loads(receipt.read_text())
    body["plan_sha256"] = "f" * 64
    _write_owner(receipt, body)
    _owned, swapped = guard.read_writer_stop_receipt(receipt, now=NOW)
    assert swapped["plan_sha256"] == "f" * 64


def test_no_clobber_recovers_crash_and_rejects_conflict(
    lease_setup, tmp_path, monkeypatch
) -> None:
    sidecar = _sidecar_identity()
    stopped = _stopped(lease_setup["runtime"], sidecar)
    receipt = tmp_path / "writer-stop.json"
    original_link = os.link
    calls = 0

    def fail_once(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("crash before link")
        return original_link(*args, **kwargs)

    with _acquire(lease_setup) as lease:
        inputs = guard.WriterStopInputs(
            hold_id="hold-20260713-a",
            plan_sha256="7" * 64,
            receipt_path=receipt,
            expected_live_sidecar_identity=sidecar,
            precutover_service_state=_precutover_service_state(
                lease_setup["runtime"]
            ),
        )
        monkeypatch.setattr(os, "link", fail_once)
        with pytest.raises(RuntimeError, match="crash before link"):
            guard.observe_writer_stop(
                lease,
                inputs,
                writer_stop_observer=lambda: stopped,
                now=NOW,
            )
        monkeypatch.setattr(os, "link", original_link)
        guard.observe_writer_stop(
            lease,
            inputs,
            writer_stop_observer=lambda: stopped,
            now=NOW,
        )
    assert receipt.exists()

    conflict = json.loads(receipt.read_text())
    conflict["hold_id"] = "hold-conflict-20260713"
    with pytest.raises(guard.CutoverGuardError) as error:
        guard._publish_no_clobber(receipt, conflict)
    assert error.value.code == "writer_stop_receipt_conflict"


def test_no_clobber_recovers_crash_after_link_and_cleans_temp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    destination = tmp_path / "receipt.json"
    body = {"schema_version": "fixture_v1", "value": 1}
    original_fsync = guard._fsync_directory
    calls = 0

    def crash_after_link(path):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("crash after link")
        return original_fsync(path)

    monkeypatch.setattr(guard, "_fsync_directory", crash_after_link)
    with pytest.raises(RuntimeError, match="crash after link"):
        guard._publish_no_clobber(destination, body)
    assert destination.exists()
    assert len(list(tmp_path.glob(".*.tmp"))) == 1

    monkeypatch.setattr(guard, "_fsync_directory", original_fsync)
    assert guard._publish_no_clobber(destination, body) is True
    assert not list(tmp_path.glob(".*.tmp"))


def test_plan_and_doctor_expose_no_mutation_executor(tmp_path: Path) -> None:
    runtime = _runtime_identity()
    lock = tmp_path / "missing.lock"
    plan = guard.plan_cutover_guard(
        runtime_observer=lambda: runtime,
        lock_path=lock,
    )
    doctor = guard.doctor_cutover_lock(lock_path=lock)
    actions = {action.dest for action in guard._parser()._actions}

    assert plan["mutation_commands_available"] is False
    assert plan["production_effects_executed"] is False
    assert doctor["state"] == "absent"
    assert actions == {
        "help",
        "command",
        "canonical_live_root",
        "allow_absent_rca_files",
    }
    assert not lock.exists()


@pytest.mark.parametrize(
    ("stdout", "expected_pid", "expected_state"),
    [
        ("state = running\npid = 41001\n", 41001, "running"),
        ("state = exited\n", None, "exited"),
    ],
)
def test_launchctl_observer_is_read_only_print_and_parses_variants(
    stdout, expected_pid, expected_state
) -> None:
    calls = []

    def runner(command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0, stdout, "")

    observed = guard._launchctl_print(runner=runner)

    assert observed == {
        "label": guard.GATEWAY_LABEL,
        "loaded": True,
        "pid": expected_pid,
        "state": expected_state,
    }
    assert calls[0][0] == [
        "launchctl",
        "print",
        f"gui/{os.getuid()}/{guard.GATEWAY_LABEL}",
    ]
    assert all(
        mutation not in calls[0][0]
        for mutation in ("stop", "start", "kickstart", "kill", "bootout")
    )


def test_launchctl_observer_ignores_nested_service_states() -> None:
    stdout = (
        "\tstate = running\n"
        "\tpid = 41001\n"
        "\tspawn type = daemon (3)\n"
        "\tproperties = {\n"
        "\t\tstate = active\n"
        "\t}\n"
    )
    result = subprocess.CompletedProcess(["launchctl"], 0, stdout, "")

    observed = guard._launchctl_print(
        runner=lambda *_args, **_kwargs: result
    )

    assert observed["pid"] == 41001
    assert observed["state"] == "running"


def test_plan_binds_explicit_current_runtime_root(tmp_path: Path) -> None:
    root = tmp_path / "releases" / "hermes-v0.18.2-current"
    runtime = _runtime_identity()
    runtime["canonical_root"] = str(root)
    runtime["process"]["cwd"] = str(root)
    runtime["live_runtime_identity"]["canonical_root"] = str(root)
    runtime["live_runtime_identity"]["root_identity"]["path"] = str(root)
    runtime["process"]["loaded_runtime_closure_sha256"] = guard._sha256_json(
        runtime["live_runtime_identity"]
    )

    plan = guard.plan_cutover_guard(
        runtime_observer=lambda: runtime,
        canonical_live_root=root,
        lock_path=tmp_path / "lock",
    )

    assert plan["canonical_live_root"] == str(root)
    assert plan["expected_live_runtime_identity"] == runtime
    assert plan["absent_rca_files_allowed"] is False


def test_runtime_observer_binds_absent_overlay_files(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "base-runtime"
    interpreter = root / ".venv/bin/python"
    interpreter.parent.mkdir(parents=True)
    interpreter.write_bytes(b"python")
    interpreter.chmod(0o755)
    monkeypatch.setattr(
        guard,
        "GATEWAY_RCA_RUNTIME_RELATIVE_FILES",
        ("gateway/base.py", "gateway/overlay.py"),
    )
    base = root / "gateway/base.py"
    base.parent.mkdir(parents=True)
    base.write_bytes(b"base")

    observed = guard.observe_live_runtime_files(
        root,
        allow_absent_rca_files=True,
    )

    assert observed["files"]["gateway/base.py"]["sha256"]
    assert observed["files"]["gateway/overlay.py"] == {
        "path": str(root / "gateway/overlay.py"),
        "state": "absent",
    }
    with pytest.raises(guard.CutoverGuardError) as error:
        guard.observe_live_runtime_files(root)
    assert error.value.code == "cutover_guard_live_runtime_file_unavailable"


def test_runtime_observer_binds_external_current_interpreter(
    tmp_path: Path, monkeypatch
) -> None:
    root = tmp_path / "base-runtime"
    root.mkdir()
    external = tmp_path / "sealed-venv/bin/python"
    external.parent.mkdir(parents=True)
    external.write_bytes(b"python")
    external.chmod(0o755)
    monkeypatch.setattr(guard, "GATEWAY_RCA_RUNTIME_RELATIVE_FILES", ())

    observed = guard.observe_live_runtime_files(
        root,
        interpreter_path=external,
    )

    assert observed["interpreter"]["path"] == str(external)
    assert observed["interpreter"]["sha256"] == hashlib.sha256(b"python").hexdigest()


def _external_interpreter_writer_stop_fixture(
    tmp_path: Path, monkeypatch
) -> tuple[dict, Path, Path]:
    root = tmp_path / "base-runtime"
    root.mkdir()
    external = tmp_path / "sealed-venv/bin/python"
    external.parent.mkdir(parents=True)
    external.write_bytes(b"python")
    external.chmod(0o755)
    monkeypatch.setattr(guard, "GATEWAY_RCA_RUNTIME_RELATIVE_FILES", ())
    runtime = guard.observe_live_runtime_files(
        root,
        interpreter_path=external,
    )
    expected = {
        "schema_version": guard.GATEWAY_RUNNING_OBSERVATION_SCHEMA_VERSION,
        "canonical_root": str(root),
        "launchd": {
            "label": guard.GATEWAY_LABEL,
            "loaded": True,
            "pid": 41001,
            "state": "running",
        },
        "process": {
            "pid": 41001,
            "process_create_time": NOW.timestamp() - 60,
            "executable": str(external),
            "cwd": str(root),
            "cmdline_sha256": "1" * 64,
            "loaded_runtime_closure_sha256": guard._sha256_json(runtime),
        },
        "live_runtime_identity": runtime,
    }
    return expected, root, external


def test_writer_stop_reuses_bound_external_interpreter(
    tmp_path: Path, monkeypatch
) -> None:
    expected, root, external = _external_interpreter_writer_stop_fixture(
        tmp_path, monkeypatch
    )
    sidecar = {"state": "absent"}

    observed = guard.observe_gateway_writer_stopped(
        expected_live_runtime_identity=expected,
        expected_live_sidecar_identity=sidecar,
        launchctl_observer=lambda: {
            "label": guard.GATEWAY_LABEL,
            "loaded": False,
            "pid": None,
            "state": "absent",
        },
        census_observer=lambda: {
            "probe": "psutil_gateway_canonical_runtime_census_v1",
            "canonical_root": str(root),
            "matching_processes": [],
        },
        sidecar_observer=lambda: sidecar,
    )

    assert observed["live_runtime_identity"]["interpreter"]["path"] == str(
        external
    )


def test_writer_stop_fails_closed_when_bound_external_interpreter_disappears(
    tmp_path: Path, monkeypatch
) -> None:
    expected, root, external = _external_interpreter_writer_stop_fixture(
        tmp_path, monkeypatch
    )
    external.unlink()

    with pytest.raises(guard.CutoverGuardError) as error:
        guard.observe_gateway_writer_stopped(
            expected_live_runtime_identity=expected,
            expected_live_sidecar_identity={"state": "absent"},
            launchctl_observer=lambda: {
                "label": guard.GATEWAY_LABEL,
                "loaded": False,
                "pid": None,
                "state": "absent",
            },
            census_observer=lambda: {
                "probe": "psutil_gateway_canonical_runtime_census_v1",
                "canonical_root": str(root),
                "matching_processes": [],
            },
            sidecar_observer=lambda: {"state": "absent"},
        )

    assert error.value.code == "cutover_guard_live_runtime_interpreter_unavailable"


def test_launchctl_ambiguous_output_fails_closed() -> None:
    result = subprocess.CompletedProcess(
        ["launchctl"], 0, "pid = 1\npid = 2\n", ""
    )
    with pytest.raises(guard.CutoverGuardError) as error:
        guard._launchctl_print(runner=lambda *_args, **_kwargs: result)
    assert error.value.code == "cutover_guard_launchctl_output_ambiguous"


def test_launchctl_unloaded_is_valid_writer_stop_state() -> None:
    result = subprocess.CompletedProcess(
        ["launchctl"], 113, "", "Could not find service"
    )

    assert guard._launchctl_print(runner=lambda *_args, **_kwargs: result) == {
        "label": guard.GATEWAY_LABEL,
        "loaded": False,
        "pid": None,
        "state": "absent",
    }


def test_doctor_rejects_duplicate_json_without_rewriting_lock(tmp_path: Path) -> None:
    lock = tmp_path / "duplicate.lock"
    raw = b'{"schema_version":"one","schema_version":"two"}\n'
    lock.write_bytes(raw)
    lock.chmod(0o600)

    with pytest.raises(guard.CutoverGuardError) as error:
        guard.doctor_cutover_lock(lock_path=lock)

    assert error.value.code == "cutover_guard_lock_duplicate_key"
    assert lock.read_bytes() == raw
