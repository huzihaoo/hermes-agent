from __future__ import annotations

import json
from pathlib import Path
import sqlite3

import pytest

from gateway.pnc_rca_control_store import RcaControlStore
from gateway.pnc_rca_delivery_store import RcaDeliveryStore
from scripts import pnc_rca_activation as activation
from scripts import pnc_rca_cutover_adapter as adapter
from scripts import pnc_rca_cutover_live as live
from scripts import pnc_rca_postinstall_activation as postinstall
from scripts import pnc_rca_production_cutover as cutover


TOPIC = "feishu-project-workflow-event"
EPOCH_ID = "rca-activation-20260716"


def _write_json(path: Path, body: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.write_text(json.dumps(body, sort_keys=True) + "\n", encoding="utf-8")
    path.chmod(0o600)


def _combined_store(path: Path, *, journal_mode: str = "delete") -> None:
    RcaControlStore(path)
    RcaDeliveryStore(path)
    with sqlite3.connect(path) as connection:
        for table in ("control_meta", "rca_delivery_meta"):
            connection.executemany(
                f"INSERT OR REPLACE INTO {table}(key, value) VALUES (?, ?)",
                (
                    ("fresh_install_db_instance_id", "db-instance-test"),
                    ("fresh_install_genesis_intent_sha256", "a" * 64),
                    ("fresh_install_origin_commit", "b" * 40),
                ),
            )
        connection.commit()
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        connection.execute(f"PRAGMA journal_mode={journal_mode.upper()}")
    path.chmod(0o600)


class FakeRunner:
    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []

    def run(self, argv) -> adapter.CommandResult:
        command = tuple(str(item) for item in argv)
        self.calls.append(command)
        script = Path(command[2]).name
        if script == "pnc_rca_release_gate.py":
            mode = command[command.index("--mode") + 1]
            receipt = Path(command[command.index("--receipt") + 1])
            _write_json(receipt, {"mode": mode, "ok": True})
            _write_json(
                postinstall._capsule_path(receipt, mode),
                {"mode": mode, "ok": True},
            )
            body = {"mode": mode, "ok": True}
        elif script == "pnc_rca_activation.py":
            activation_command = command[5]
            applied = "--apply" in command
            result = {"epoch_id": EPOCH_ID}
            if activation_command == "create":
                result = {"current_epoch": {"epoch_id": EPOCH_ID}}
            elif activation_command == "prepare-bootstrap-production":
                result = {
                    "producer_activation_receipt_sha256": "a" * 64,
                    "producer_receipt_present": True,
                    "runtime_effective_state": "BOOTSTRAP_PRODUCTION",
                }
            body = {
                "applied": applied,
                "command": activation_command,
                "mode": "apply" if applied else "plan",
                "ok": True,
                "result": result,
                "schema_version": activation.ACTIVATION_CLI_SCHEMA_VERSION,
            }
        else:
            raise AssertionError(command)
        return adapter.CommandResult(command, 0, json.dumps(body), "")


class FakeServices:
    def __init__(self) -> None:
        self.loaded = {label: False for label in cutover.RESIDENT_LABELS}
        self.restore_calls = 0
        self.start_calls = 0
        self.verify_calls = 0
        self.fail_verify = False

    def _initial_state(self) -> dict:
        return {
            "schema_version": live.LIVE_SERVICE_STATE_SCHEMA_VERSION,
            "target_runtime_root": str(cutover.CANONICAL_RUNTIME_ROOT),
            "labels": list(cutover.RESIDENT_LABELS),
            "jobs": {
                label: {
                    "launchd": {
                        "label": label,
                        "last_exit_status": None,
                        "loaded": False,
                        "pid": None,
                        "state": "absent",
                    },
                    "plist": {
                        "kind": "regular_file",
                        "path": str(
                            cutover.CANONICAL_LAUNCH_AGENTS_ROOT / f"{label}.plist"
                        ),
                    },
                }
                for label in cutover.RESIDENT_LABELS
            },
        }

    def capture_state(self, labels):
        assert tuple(labels) == cutover.RESIDENT_LABELS
        assert not any(self.loaded.values())
        return self._initial_state()

    def start_residents(self, labels):
        assert tuple(labels) == cutover.RESIDENT_LABELS
        self.start_calls += 1
        for label in labels:
            self.loaded[label] = True
        return list(labels)

    def verify(self, labels, *, runtime_sha256):
        assert tuple(labels) == cutover.RESIDENT_LABELS
        self.verify_calls += 1
        if self.fail_verify:
            raise live.LiveBoundaryError("fixture_resident_unhealthy")
        assert all(self.loaded[label] for label in labels)
        return {
            label: {
                "health_ok": True,
                "runtime_sha256": runtime_sha256,
            }
            for label in labels
        }

    def restore_state(self, state):
        assert state == self._initial_state()
        self.restore_calls += 1
        for label in cutover.RESIDENT_LABELS:
            self.loaded[label] = False


@pytest.fixture
def scenario(tmp_path: Path):
    root = tmp_path / "postinstall"
    evidence = root / "evidence"
    journal = root / "journal"
    for directory in (root, evidence, journal):
        directory.mkdir(mode=0o700)
    inputs = postinstall.PostinstallInputs(
        candidate_python=cutover.CANONICAL_RUNTIME_ROOT / ".venv/bin/python",
        control_db=root / "control.sqlite3",
        evidence_dir=evidence,
        live_env=root / "live.env",
        active_release_binding=root / "active-release-binding.json",
        release_id="rca-release-20260716",
        bootstrap_epoch_id="rca-bootstrap-20260716",
        expected_topic=TOPIC,
        expected_rule_version="creation-snapshot-v1",
        preauthorization_receipt=root / "preauthorization.json",
        preauthorization_capsule=(
            root / "preauthorization.activation-preauthorization.json"
        ),
        preproduction_receipt=root / "preproduction.json",
        preproduction_capsule=root / "preproduction.activation-preproduction.json",
        kafka_event_uid=f"{TOPIC}:0:584",
        manual_success_identity=root / "manual-success.json",
        manual_terminal_failure_identity=root / "manual-terminal-failure.json",
        runtime_content_sha256="b" * 64,
        journal_root=journal,
        lock_path=root / "postinstall.lock",
        receipt_path=root / "postinstall-receipt.json",
    )
    _combined_store(inputs.control_db)
    return inputs, FakeRunner(), FakeServices()


def _run(inputs, runner, services):
    return postinstall.run_postinstall_activation(
        inputs,
        authorization_decision=postinstall.AUTHORIZATION_DECISION,
        operator="release-owner",
        reason="approved bounded canary bootstrap",
        runner=runner,
        service_controller=services,
    )


def test_postinstall_activation_is_resumable_and_starts_exact_residents(
    scenario,
) -> None:
    inputs, runner, services = scenario

    first = _run(inputs, runner, services)
    call_count = len(runner.calls)
    second = _run(inputs, runner, services)

    assert first == second
    assert first["ok"] is True
    assert first["activation_state"] == "bounded_active"
    assert first["real_canaries_completed"] is False
    assert first["next_phase"] == "execute_exact_kafka_and_manual_canaries"
    assert call_count == 16
    assert len(runner.calls) == call_count
    assert services.start_calls == 1
    assert services.verify_calls == 2
    assert services.restore_calls == 0
    assert all(services.loaded.values())
    assert len(first["step_receipts"]) == call_count + 1
    assert inputs.receipt_path.stat().st_mode & 0o777 == 0o600


def test_resident_health_failure_restores_initial_state_and_can_resume(scenario) -> None:
    inputs, runner, services = scenario
    services.fail_verify = True

    with pytest.raises(live.LiveBoundaryError, match="fixture_resident_unhealthy"):
        _run(inputs, runner, services)

    assert services.restore_calls == 1
    assert not any(services.loaded.values())
    assert not inputs.receipt_path.exists()
    assert not (inputs.journal_root / "resident-start.done.json").exists()
    call_count = len(runner.calls)

    services.fail_verify = False
    recovered = _run(inputs, runner, services)

    assert recovered["ok"] is True
    assert len(runner.calls) == call_count
    assert services.restore_calls == 1
    assert services.start_calls == 2
    assert all(services.loaded.values())


def test_resume_revalidates_activation_journal_contract(scenario) -> None:
    inputs, runner, services = scenario
    _run(inputs, runner, services)
    journal = inputs.journal_root / "04-activation-create-apply.json"
    body = json.loads(journal.read_text(encoding="utf-8"))
    body["result"]["applied"] = False
    _write_json(journal, body)
    call_count = len(runner.calls)

    with pytest.raises(
        postinstall.PostinstallActivationError,
        match="postinstall_activation_result_invalid",
    ):
        _run(inputs, runner, services)

    assert len(runner.calls) == call_count


def test_prepare_control_store_wal_transitions_once_and_is_resumable(
    tmp_path: Path,
) -> None:
    database = tmp_path / "control.sqlite3"
    receipt = tmp_path / "02-control-store-wal.json"
    _combined_store(database)

    first = postinstall._prepare_control_store_wal(database, receipt)
    second = postinstall._prepare_control_store_wal(database, receipt)

    assert first == second
    assert first["before_journal_mode"] == "delete"
    assert first["after_journal_mode"] == "wal"
    assert database.stat().st_ino == first["database_identity"]["inode"]
    with sqlite3.connect(database) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"


def test_prepare_control_store_wal_rejects_noncurrent_database(
    tmp_path: Path,
) -> None:
    database = tmp_path / "control.sqlite3"
    receipt = tmp_path / "02-control-store-wal.json"
    _combined_store(database)
    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE control_meta SET value='stale' WHERE key='schema_version'"
        )
        connection.commit()
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        connection.execute("PRAGMA journal_mode=DELETE")

    with pytest.raises(
        postinstall.PostinstallActivationError,
        match="postinstall_control_store_invalid",
    ):
        postinstall._prepare_control_store_wal(database, receipt)

    assert not receipt.exists()
    with sqlite3.connect(database) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "delete"


def test_manifest_validation_is_owner_only_and_read_only(
    tmp_path: Path, capsys
) -> None:
    root = tmp_path / "manifest"
    evidence = root / "evidence"
    journal = root / "journal"
    for directory in (root, evidence, journal):
        directory.mkdir(mode=0o700)
    preauthorization = root / "preauthorization.json"
    preproduction = root / "preproduction.json"
    manifest = root / "postinstall-manifest.json"
    _write_json(
        manifest,
        {
            "active_release_binding": str(root / "active-release-binding.json"),
            "bootstrap_epoch_id": "rca-bootstrap-20260716",
            "candidate_python": str(
                cutover.CANONICAL_RUNTIME_ROOT / ".venv/bin/python"
            ),
            "control_db": str(root / "control.sqlite3"),
            "evidence_dir": str(evidence),
            "expected_rule_version": "creation-snapshot-v1",
            "expected_topic": TOPIC,
            "journal_root": str(journal),
            "kafka_event_uid": f"{TOPIC}:0:584",
            "live_env": str(root / "live.env"),
            "lock_path": str(root / "postinstall.lock"),
            "manual_success_identity": str(root / "manual-success.json"),
            "manual_terminal_failure_identity": str(
                root / "manual-terminal-failure.json"
            ),
            "preauthorization_capsule": str(
                postinstall._capsule_path(preauthorization, "preauthorization")
            ),
            "preauthorization_receipt": str(preauthorization),
            "preproduction_capsule": str(
                postinstall._capsule_path(preproduction, "preproduction")
            ),
            "preproduction_receipt": str(preproduction),
            "receipt_path": str(root / "postinstall-receipt.json"),
            "release_id": "rca-release-20260716",
            "runtime_content_sha256": "b" * 64,
            "schema_version": postinstall.MANIFEST_SCHEMA_VERSION,
        },
    )

    assert postinstall.main(["validate-manifest", "--manifest", str(manifest)]) == 0
    output = json.loads(capsys.readouterr().out)

    assert output == {
        "ok": True,
        "production_effects_executed": False,
        "release_id": "rca-release-20260716",
        "schema_version": postinstall.MANIFEST_SCHEMA_VERSION,
    }
    assert not (root / "postinstall.lock").exists()
    assert not (root / "postinstall-receipt.json").exists()

    manifest.chmod(0o644)
    assert postinstall.main(["validate-manifest", "--manifest", str(manifest)]) == 2
    failure = json.loads(capsys.readouterr().out)
    assert failure["ok"] is False
