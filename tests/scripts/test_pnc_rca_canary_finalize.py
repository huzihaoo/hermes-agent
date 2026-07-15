from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import pnc_rca_activation as activation
from scripts import pnc_rca_canary_finalize as finalizer
from scripts import pnc_rca_cutover_adapter as adapter
from scripts import pnc_rca_postinstall_activation as postinstall
from scripts import pnc_rca_production_cutover as cutover
from scripts import pnc_rca_release_gate as release_gate


TOPIC = "feishu-project-workflow-event"
EPOCH_ID = "rca-activation-20260716"
RECONCILE_EVENT = f"{TOPIC}:0:585"
DEFER_EVENT = f"{TOPIC}:0:586"


def _write_json(path: Path, body: object) -> str:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.write_text(json.dumps(body, sort_keys=True) + "\n", encoding="utf-8")
    path.chmod(0o600)
    return finalizer._sha256(path.read_bytes())


class FakeReconciliation:
    def __init__(self) -> None:
        self.entries = {
            RECONCILE_EVENT: {
                "status": "shadow",
                "last_error_code": "",
                "activation_epoch_id": EPOCH_ID,
            },
            DEFER_EVENT: {
                "status": "shadow",
                "last_error_code": "",
                "activation_epoch_id": EPOCH_ID,
            },
        }
        self.extra_shadow = ""

    def __call__(self, _inputs):
        entries = json.loads(json.dumps(self.entries))
        if self.extra_shadow:
            entries[self.extra_shadow] = {
                "status": "shadow",
                "last_error_code": "",
                "activation_epoch_id": EPOCH_ID,
            }
        return {
            "entries": entries,
            "shadow_event_uids": sorted(
                event_uid
                for event_uid, state in entries.items()
                if state["status"] == "shadow"
            ),
        }


class FakeRunner:
    def __init__(self, reconciliation: FakeReconciliation) -> None:
        self.reconciliation = reconciliation
        self.calls: list[tuple[str, ...]] = []
        self.state = "bounded_active"
        self.fail_defer_once = False

    def _activation_result(self, command: tuple[str, ...]) -> dict:
        name = command[5]
        if name == "status":
            return {
                "applied": False,
                "command": "status",
                "mode": "read_only",
                "ok": True,
                "result": {
                    "activation": {
                        "current_epoch": {
                            "epoch_id": EPOCH_ID,
                            "state": self.state,
                        }
                    }
                },
                "schema_version": activation.ACTIVATION_CLI_SCHEMA_VERSION,
            }
        applied = "--apply" in command
        if name == "confirm" and applied:
            self.state = "confirmed"
        elif name == "reconcile-shadow" and applied:
            event_uid = command[command.index("--event-uid") + 1]
            self.reconciliation.entries[event_uid]["status"] = "pending"
        elif name == "defer-event" and applied:
            if self.fail_defer_once:
                self.fail_defer_once = False
                return {
                    "code": "fixture_defer_failed",
                    "command": name,
                    "ok": False,
                    "schema_version": activation.ACTIVATION_CLI_SCHEMA_VERSION,
                }
            event_uid = command[command.index("--event-uid") + 1]
            self.reconciliation.entries[event_uid].update(
                status="quarantined",
                last_error_code="activation_epoch_deferred",
            )
        elif name == "transition-steady" and applied:
            self.state = "steady_active"
        return {
            "applied": applied,
            "command": name,
            "mode": "apply" if applied else "plan",
            "ok": True,
            "result": {"current_epoch": {"epoch_id": EPOCH_ID, "state": self.state}},
            "schema_version": activation.ACTIVATION_CLI_SCHEMA_VERSION,
        }

    def run(self, argv) -> adapter.CommandResult:
        command = tuple(str(item) for item in argv)
        self.calls.append(command)
        script = Path(command[2]).name
        returncode = 0
        if script == "pnc_rca_activation.py":
            body = self._activation_result(command)
            returncode = 0 if body["ok"] else 2
        elif script == "pnc_rca_canary_collector.py":
            write = "--write" in command
            if "--manual-success" in command:
                role = "manual_success"
                manifest = "manual_success_canary_commit.json"
            elif "--terminal-failure" in command:
                role = "manual_terminal_failure"
                manifest = "manual_terminal_failure_canary_commit.json"
            else:
                role = "primary"
                manifest = "canary_receipt_commit.json"
            body = {
                "ok": True,
                "mode": "write" if write else "dry_run",
                "read_only_collection": True,
                "external_side_effects": False,
                "evidence_role": role,
                "evidence_commit_id": finalizer._sha256(role.encode()),
                "evidence_manifest": manifest,
                "observed_source_id_sha256": "1" * 64,
                "submission_key_sha256": "2" * 64,
                "receipt_sha256": "3" * 64,
                "written_files": (
                    [f"{role}-receipt.json", f"{role}-sources.json", manifest]
                    if write
                    else []
                ),
            }
        elif script == "pnc_rca_release_gate.py":
            mode = command[command.index("--mode") + 1]
            if "--collect-activation-production-candidate" in command:
                destination = Path(
                    command[
                        command.index("--collect-activation-production-candidate") + 1
                    ]
                )
                body = {
                    "schema_version": (
                        release_gate.ACTIVATION_PRODUCTION_CANDIDATE_SCHEMA_VERSION
                    ),
                    "observed_at": "2026-07-16T00:00:00+00:00",
                    "read_only": True,
                    "external_side_effects": False,
                    "epoch_id": EPOCH_ID,
                    "config_sha256": "4" * 64,
                    "partition_start_fence_sha256": "5" * 64,
                    "partition_end_fence": {TOPIC: {"0": 621}},
                    "collector": {"fixture": True},
                }
                _write_json(destination, body)
            else:
                receipt = Path(command[command.index("--receipt") + 1])
                body = {"mode": mode, "ok": True}
                _write_json(receipt, body)
                if mode == finalizer.PRODUCTION_MODE:
                    _write_json(
                        release_gate.activation_confirmation_capsule_path(receipt),
                        {"mode": mode, "ok": True},
                    )
        else:
            raise AssertionError(command)
        return adapter.CommandResult(command, returncode, json.dumps(body), "")


@pytest.fixture
def scenario(tmp_path: Path):
    root = tmp_path / "finalize"
    evidence = root / "evidence"
    journal = root / "journal"
    group_receipts = root / "group-receipts"
    for directory in (root, evidence, journal, group_receipts):
        directory.mkdir(mode=0o700)
    success_identity = root / "manual-success.json"
    failure_identity = root / "manual-failure.json"
    manual_base = {
        "chat_id": "oc_formal",
        "requester_id": "ou_requester",
        "thread_id": "omt_thread",
        "issue_url": "https://project.feishu.cn/t03o4q/issue/detail/7049071505",
        "mode": "run_or_join",
    }
    success_sha = _write_json(
        success_identity,
        {**manual_base, "message_id": "om_success"},
    )
    failure_sha = _write_json(
        failure_identity,
        {
            **manual_base,
            "thread_id": "omt_failure",
            "message_id": "om_failure",
            "issue_url": "https://project.feishu.cn/t03o4q/issue/detail/7048913278",
        },
    )
    preproduction = root / "preproduction.activation-preproduction.json"
    preproduction_sha = _write_json(preproduction, {"fixture": "preproduction"})
    reconciliation_plan = root / "reconciliation-plan.json"
    reconciliation_sha = _write_json(
        reconciliation_plan,
        {
            "schema_version": finalizer.RECONCILIATION_SCHEMA_VERSION,
            "activation_epoch_id": EPOCH_ID,
            "entries": [
                {"event_uid": RECONCILE_EVENT, "action": "reconcile"},
                {"event_uid": DEFER_EVENT, "action": "defer"},
            ],
        },
    )
    runtime_sha = "b" * 64
    postinstall_receipt = root / "postinstall-receipt.json"
    postinstall_sha = _write_json(
        postinstall_receipt,
        {
            "schema_version": postinstall.RECEIPT_SCHEMA_VERSION,
            "ok": True,
            "release_id": "rca-release-20260716",
            "bootstrap_epoch_id": "rca-bootstrap-20260716",
            "activation_epoch_id": EPOCH_ID,
            "activation_state": "bounded_active",
            "real_canaries_completed": False,
            "next_phase": "execute_exact_kafka_and_manual_canaries",
            "resident_health": {
                label: {"health_ok": True, "runtime_sha256": runtime_sha}
                for label in cutover.RESIDENT_LABELS
            },
        },
    )
    release_id = "rca-release-20260716"
    production_receipt = (
        evidence
        / f"release_gate_{finalizer.PRODUCTION_MODE}_{release_id}.json"
    )
    inputs = finalizer.FinalizationInputs(
        candidate_python=cutover.CANONICAL_RUNTIME_ROOT / ".venv/bin/python",
        control_db=root / "control.sqlite3",
        delivery_db=root / "control.sqlite3",
        evidence_dir=evidence,
        live_env=root / "live.env",
        group_binding_receipt_dir=group_receipts,
        preproduction_capsule=preproduction,
        preproduction_capsule_sha256=preproduction_sha,
        postinstall_receipt=postinstall_receipt,
        postinstall_receipt_sha256=postinstall_sha,
        release_id=release_id,
        bootstrap_epoch_id="rca-bootstrap-20260716",
        activation_epoch_id=EPOCH_ID,
        expected_topic=TOPIC,
        expected_rule_version="creation-snapshot-v1",
        kafka_event_uid=f"{TOPIC}:0:584",
        manual_success_identity=success_identity,
        manual_success_identity_sha256=success_sha,
        manual_terminal_failure_identity=failure_identity,
        manual_terminal_failure_identity_sha256=failure_sha,
        canary_gate_receipt=(
            evidence / f"release_gate_{finalizer.CANARY_MODE}_{release_id}.json"
        ),
        production_candidate=evidence / "activation_production_candidate.json",
        production_gate_receipt=production_receipt,
        production_confirmation_capsule=(
            release_gate.activation_confirmation_capsule_path(production_receipt)
        ),
        active_release_binding=root / "active-release-binding.json",
        reconciliation_plan=reconciliation_plan,
        reconciliation_plan_sha256=reconciliation_sha,
        reconciliation_entries=(
            finalizer.ReconciliationEntry(RECONCILE_EVENT, "reconcile"),
            finalizer.ReconciliationEntry(DEFER_EVENT, "defer"),
        ),
        runtime_content_sha256=runtime_sha,
        journal_root=journal,
        lock_path=root / "finalize.lock",
        receipt_path=root / "finalize-receipt.json",
    )
    reconciliation = FakeReconciliation()
    return inputs, FakeRunner(reconciliation), reconciliation


def _run(inputs, runner, reconciliation):
    return finalizer.run_canary_finalization(
        inputs,
        authorization_decision=finalizer.AUTHORIZATION_DECISION,
        operator="release-owner",
        reason="approved real canaries and exact reconciliation",
        runner=runner,
        reconciliation_observer=reconciliation,
    )


def test_canary_finalization_collects_confirms_reconciles_and_is_idempotent(
    scenario,
) -> None:
    inputs, runner, reconciliation = scenario

    first = _run(inputs, runner, reconciliation)
    call_count = len(runner.calls)
    second = _run(inputs, runner, reconciliation)

    assert first == second
    assert first["real_canaries_completed"] is True
    assert first["activation_state"] == "steady_active"
    assert set(first["collector_evidence"]) == {
        "manual_success",
        "manual_terminal_failure",
        "primary",
    }
    assert first["reconciliation_after"][RECONCILE_EVENT]["status"] == "pending"
    assert first["reconciliation_after"][DEFER_EVENT]["status"] == "quarantined"
    assert runner.state == "steady_active"
    assert call_count == 20
    assert len(runner.calls) == call_count + 1
    assert len(first["step_receipts"]) == 17


def test_canary_finalization_resumes_after_committed_defer_command(scenario) -> None:
    inputs, runner, reconciliation = scenario
    runner.fail_defer_once = True

    with pytest.raises(
        postinstall.PostinstallActivationError, match="fixture_defer_failed"
    ):
        _run(inputs, runner, reconciliation)

    assert runner.state == "confirmed"
    assert reconciliation.entries[RECONCILE_EVENT]["status"] == "pending"
    assert reconciliation.entries[DEFER_EVENT]["status"] == "shadow"
    assert not inputs.receipt_path.exists()

    recovered = _run(inputs, runner, reconciliation)

    assert recovered["activation_state"] == "steady_active"
    assert reconciliation.entries[DEFER_EVENT]["status"] == "quarantined"


def test_unplanned_shadow_blocks_before_production_confirmation(scenario) -> None:
    inputs, runner, reconciliation = scenario
    reconciliation.extra_shadow = f"{TOPIC}:0:587"

    with pytest.raises(
        finalizer.CanaryFinalizationError,
        match="canary_finalize_unplanned_shadow_backlog",
    ):
        _run(inputs, runner, reconciliation)

    assert runner.state == "bounded_active"
    assert not inputs.production_gate_receipt.exists()
    assert not inputs.receipt_path.exists()


def test_manifest_validation_is_owner_only_and_read_only(scenario, capsys) -> None:
    inputs, _runner, _reconciliation = scenario
    manifest = inputs.receipt_path.parent / "canary-finalization-manifest.json"
    _write_json(
        manifest,
        {
            "schema_version": finalizer.MANIFEST_SCHEMA_VERSION,
            "candidate_python": str(inputs.candidate_python),
            "control_db": str(inputs.control_db),
            "delivery_db": str(inputs.delivery_db),
            "evidence_dir": str(inputs.evidence_dir),
            "live_env": str(inputs.live_env),
            "group_binding_receipt_dir": str(inputs.group_binding_receipt_dir),
            "preproduction_capsule": str(inputs.preproduction_capsule),
            "preproduction_capsule_sha256": (
                inputs.preproduction_capsule_sha256
            ),
            "postinstall_receipt": str(inputs.postinstall_receipt),
            "postinstall_receipt_sha256": inputs.postinstall_receipt_sha256,
            "release_id": inputs.release_id,
            "bootstrap_epoch_id": inputs.bootstrap_epoch_id,
            "activation_epoch_id": inputs.activation_epoch_id,
            "expected_topic": inputs.expected_topic,
            "expected_rule_version": inputs.expected_rule_version,
            "kafka_event_uid": inputs.kafka_event_uid,
            "manual_success_identity": str(inputs.manual_success_identity),
            "manual_success_identity_sha256": (
                inputs.manual_success_identity_sha256
            ),
            "manual_terminal_failure_identity": str(
                inputs.manual_terminal_failure_identity
            ),
            "manual_terminal_failure_identity_sha256": (
                inputs.manual_terminal_failure_identity_sha256
            ),
            "canary_gate_receipt": str(inputs.canary_gate_receipt),
            "production_candidate": str(inputs.production_candidate),
            "production_gate_receipt": str(inputs.production_gate_receipt),
            "production_confirmation_capsule": str(
                inputs.production_confirmation_capsule
            ),
            "active_release_binding": str(inputs.active_release_binding),
            "reconciliation_plan": str(inputs.reconciliation_plan),
            "reconciliation_plan_sha256": inputs.reconciliation_plan_sha256,
            "runtime_content_sha256": inputs.runtime_content_sha256,
            "journal_root": str(inputs.journal_root),
            "lock_path": str(inputs.lock_path),
            "receipt_path": str(inputs.receipt_path),
        },
    )

    assert finalizer.main(["validate-manifest", "--manifest", str(manifest)]) == 0
    result = json.loads(capsys.readouterr().out)

    assert result == {
        "ok": True,
        "production_effects_executed": False,
        "reconciliation_entry_count": 2,
        "release_id": inputs.release_id,
        "schema_version": finalizer.MANIFEST_SCHEMA_VERSION,
    }
    assert not inputs.lock_path.exists()
    assert not inputs.receipt_path.exists()

    manifest.chmod(0o644)
    assert finalizer.main(["validate-manifest", "--manifest", str(manifest)]) == 2
    assert json.loads(capsys.readouterr().out)["ok"] is False
