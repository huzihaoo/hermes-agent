from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
from types import SimpleNamespace

import pytest

from scripts import pnc_rca_cutover_execute as executor
from scripts import pnc_rca_feishu_ingress_hold as feishu_hold
from scripts import pnc_rca_production_cutover as cutover
from scripts import pnc_rca_vm_promotion as vm_promotion


NOW = datetime(2026, 7, 16, 1, 0, tzinfo=timezone.utc)
LEASE_FINGERPRINT = "1" * 64
MACHINE_IDENTITY = "2" * 64


def _write_json(path: Path, body: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.write_text(json.dumps(body, sort_keys=True) + "\n", encoding="utf-8")
    path.chmod(0o600)


def _migration_candidate_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "migration-candidate"
    repo.mkdir()
    for relative in executor.store_drill.MIGRATION_SOURCE_RELATIVE_PATHS:
        destination = repo / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes((executor.store_drill.REPO_ROOT / relative).read_bytes())
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.email", "test@example.invalid"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.name", "Migration Test"],
        check=True,
    )
    subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "commit", "-qm", "fixture"],
        check=True,
    )
    return repo


def test_migration_provenance_accepts_clean_repo_matching_staged_sources(
    tmp_path: Path,
) -> None:
    repo = _migration_candidate_repo(tmp_path)

    provenance = executor.store_drill._candidate_provenance(repo)

    assert provenance["repo_root"] == str(repo)
    assert set(provenance["migration_sources"]) == set(
        executor.store_drill.MIGRATION_SOURCE_RELATIVE_PATHS
    )


def test_migration_provenance_rejects_committed_source_not_in_staged_runtime(
    tmp_path: Path,
) -> None:
    repo = _migration_candidate_repo(tmp_path)
    relative = executor.store_drill.MIGRATION_SOURCE_RELATIVE_PATHS[0]
    with (repo / relative).open("ab") as stream:
        stream.write(b"\n# drift\n")
    subprocess.run(["git", "-C", str(repo), "add", relative], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "commit", "-qm", "drift"],
        check=True,
    )

    with pytest.raises(executor.store_drill.MigrationDrillError) as error:
        executor.store_drill._candidate_provenance(repo)

    assert error.value.code == "migration_candidate_runtime_source_mismatch"


class FakeLease:
    def __init__(self) -> None:
        self.fingerprint = LEASE_FINGERPRINT
        self.token = "cutover-session-lease-token-0001"
        self.body = {
            "release_id": "rca-session-20260716",
            "expires_at": (NOW + timedelta(hours=1)).isoformat(),
            "holder": {"pid": 41001},
        }
        self.closed = False

    def assert_active(self) -> None:
        if self.closed:
            raise RuntimeError("lease closed")

    def close(self) -> None:
        self.closed = True


class FakeServices:
    def __init__(self, events: list[str], name: str) -> None:
        self.events = events
        self.name = name
        self.stopped = False
        self.restored = False
        self.precutover = {
            "schema_version": "pnc_rca_live_service_state_v1",
            "target_runtime_root": str(cutover.CANONICAL_RUNTIME_ROOT),
            "labels": list(cutover.SERVICE_LABELS),
            "jobs": {label: {"fixture": True} for label in cutover.SERVICE_LABELS},
        }

    def capture_state(self, labels):
        assert tuple(labels) == cutover.SERVICE_LABELS
        return self.precutover

    def stop_writers(self, labels, **_kwargs):
        assert tuple(labels) == cutover.RUNTIME_QUIESCE_LABELS
        self.events.append(f"stop:{self.name}")
        self.stopped = True
        return {"ok": True, "receipt_path": "/fixture/writer-evidence.json"}

    def quiesce_runtime(self, labels):
        assert tuple(labels) == cutover.RUNTIME_QUIESCE_LABELS
        self.events.append(f"quiesce:{self.name}")
        return list(labels)

    def restore_state(self, state):
        assert state == self.precutover
        self.events.append(f"restore:{self.name}")
        self.restored = True


@pytest.fixture
def session(tmp_path: Path, monkeypatch):
    root = tmp_path / "session"
    root.mkdir(mode=0o700)
    hold_root = root / "hold"
    hold_root.mkdir(mode=0o700)
    plan_path = hold_root / feishu_hold.PLAN_FILENAME
    approval = root / "hold-approval.json"
    _write_json(
        plan_path,
        {
            "schema_version": feishu_hold.PLAN_SCHEMA_VERSION,
            "hold_id": "hold-session-20260716",
            "phase": "plan",
            "production_effects_executed": False,
            "live_sidecar_identity": {"revision": 1},
            "app_scope": "a" * 32,
        },
    )
    _write_json(approval, {"fixture": "approval"})
    paths = {
        name: root / f"{name}.json"
        for name in (
            "release_prepare_manifest",
            "release_approval_receipt",
            "vm_promotion_manifest",
            "vm_promotion_plan",
            "vm_promotion_approval",
            "vm_promotion_receipt",
            "vm_promotion_rollback_receipt",
            "writer_stop_receipt",
            "feishu_hold_cutover_binding",
            "env_stage_receipt",
            "runtime_stage_manifest",
            "workspace_runtime_manifest",
            "cutover_authorization_receipt",
            "session_receipt",
        )
    }
    for name in (
        "release_prepare_manifest",
        "release_approval_receipt",
        "env_stage_receipt",
        "runtime_stage_manifest",
        "workspace_runtime_manifest",
    ):
        _write_json(paths[name], {"fixture": name})
    journal = root / "journal"
    journal.mkdir(mode=0o700)
    nonce = root / "nonce"
    evidence = root / "evidence"
    snapshots = root / "snapshots"
    store_work = root / "store-work"
    store_evidence = root / "store-evidence"
    store_work.mkdir(mode=0o700)
    store_evidence.mkdir(mode=0o700)
    control_db = root / "runtime-state" / "control.sqlite3"
    control_db.parent.mkdir(mode=0o700)
    greenfield_store = executor.GreenfieldStoreInputs(
        control_db=control_db,
        delivery_db=control_db,
        work_dir=store_work,
        evidence_dir=store_evidence,
        migration_receipt=store_evidence / "store_migration_receipt.json",
        config_sha256="7" * 64,
        bootstrap_epoch_id="rca-bootstrap-session-20260716",
    )
    hold_inputs = feishu_hold.HoldInputs(
        env_file=root / "feishu.env",
        host_candidate=root / "candidate",
        live_sidecar=root / "live-sidecar.json",
        chat_ids=("oc_aaaaaaaaaaaaaaaa",),
        hold_id="hold-session-20260716",
        run_root=hold_root,
        approval_receipt=approval,
        cutover_binding=paths["feishu_hold_cutover_binding"],
    )
    inputs = executor.CutoverSessionInputs(
        release_id="rca-session-20260716",
        release_prepare_manifest=paths["release_prepare_manifest"],
        release_approval_receipt=paths["release_approval_receipt"],
        vm_promotion_manifest=paths["vm_promotion_manifest"],
        feishu_hold_inputs=hold_inputs,
        feishu_hold_approval_receipt=approval,
        greenfield_store=greenfield_store,
        writer_stop_receipt=paths["writer_stop_receipt"],
        feishu_hold_cutover_binding=paths["feishu_hold_cutover_binding"],
        env_stage_receipt=paths["env_stage_receipt"],
        runtime_stage_manifest=paths["runtime_stage_manifest"],
        workspace_runtime_manifest=paths["workspace_runtime_manifest"],
        cutover_authorization_receipt=paths["cutover_authorization_receipt"],
        expected_live_runtime_identity={"fixture": "runtime"},
        current_runtime_root=Path("/candidate/base-runtime"),
        allow_absent_rca_files=True,
        journal_root=journal,
        evidence_root=evidence,
        snapshot_root=snapshots,
        nonce_ledger_root=nonce,
        session_receipt=paths["session_receipt"],
        authorization_nonce="cutover-session-authorization-0001",
    )
    promotion_inputs = vm_promotion.VmPromotionInputs(
        release_prepare_manifest=paths["release_prepare_manifest"],
        release_approval_receipt=paths["release_approval_receipt"],
        vm_candidate_root="/mnt/tmp/rca-vm-candidate/worktree",
        worker_candidate_root="/mnt/tmp/rca-worker-candidate/worktree",
        vm_topic_extractor_sha256="8" * 64,
        vm_topic_extractor_size=7_159_960,
        plan_path=paths["vm_promotion_plan"],
        promotion_approval_receipt=paths["vm_promotion_approval"],
        receipt_path=paths["vm_promotion_receipt"],
        rollback_receipt_path=paths["vm_promotion_rollback_receipt"],
        remote_work_root="/mnt/tmp/rca-session-20260716/vm-promotion",
        remote_lock_path="/home/mini/.hermes/locks/rca-vm-promotion.lock",
    )
    lease = FakeLease()
    events = []
    promotion_calls = []
    preparation = FakeServices(events, "preparation")
    engine = FakeServices(events, "engine")
    service_values = iter((preparation, engine))
    phases = []

    monkeypatch.setattr(
        executor.feishu_hold,
        "run_ingress_hold",
        lambda _inputs, *, phase, **_kwargs: (
            phases.append(phase),
            _write_json(
                hold_root / feishu_hold.APPLY_RECEIPT_FILENAME,
                {"fixture": "hold-receipt"},
            )
            if phase == "apply"
            else None,
        )[-1],
    )
    monkeypatch.setattr(executor.feishu_hold, "_validate_approval", lambda *_a, **_k: {})
    monkeypatch.setattr(executor.guard, "acquire_cutover_lease", lambda *_a, **_k: lease)
    monkeypatch.setattr(
        executor.vm_promotion,
        "load_manifest",
        lambda _path: promotion_inputs,
    )

    def apply_promotion(_inputs, **_kwargs):
        promotion_calls.append("apply")
        events.append("promotion:apply")
        _write_json(paths["vm_promotion_receipt"], {"fixture": "promotion"})
        return {"ok": True}

    def verify_promotion(_inputs, **_kwargs):
        promotion_calls.append("verify")
        events.append("promotion:verify")
        return {"ok": True}

    def rollback_promotion(_inputs, **_kwargs):
        promotion_calls.append("rollback")
        events.append("promotion:rollback")
        _write_json(
            paths["vm_promotion_rollback_receipt"],
            {"fixture": "promotion-rollback"},
        )
        return {"ok": True, "rollback_complete": True}

    monkeypatch.setattr(executor.vm_promotion, "apply_promotion", apply_promotion)
    monkeypatch.setattr(executor.vm_promotion, "verify_promotion", verify_promotion)
    monkeypatch.setattr(executor.vm_promotion, "rollback_promotion", rollback_promotion)
    monkeypatch.setattr(
        executor.live,
        "LaunchdServiceController",
        lambda **_kwargs: next(service_values),
    )

    def writer_stop(_lease, writer_inputs, **_kwargs):
        _write_json(writer_inputs.receipt_path, {"fixture": "writer-stop"})
        return {"ok": True}

    monkeypatch.setattr(executor.guard, "observe_writer_stop", writer_stop)
    monkeypatch.setattr(
        executor.store_drill,
        "_load_writer_stop_evidence",
        lambda _path: {"fixture": "writer-evidence"},
    )

    def migration_drill(**kwargs):
        assert kwargs["repo_root"] == hold_inputs.host_candidate
        _write_json(kwargs["receipt_path"], {"fixture": "migration"})
        return {"ok": True}

    materialization_calls = []

    def materialize(**kwargs):
        materialization_calls.append(kwargs["apply"])
        body = {
            "schema_version": (
                executor.store_drill.FRESH_INSTALL_MATERIALIZATION_RECEIPT_SCHEMA_VERSION
            ),
            "ok": True,
        }
        if kwargs["apply"]:
            _write_json(
                store_evidence / "fresh_install_materialization_receipt.json",
                body,
            )
        return body

    monkeypatch.setattr(executor.store_drill, "run_migration_drill", migration_drill)
    monkeypatch.setattr(executor.store_drill, "materialize_fresh_install", materialize)
    monkeypatch.setattr(
        executor.feishu_hold,
        "build_cutover_binding",
        lambda *_a, output_path, **_k: _write_json(
            output_path, {"fixture": "hold-binding"}
        ),
    )
    monkeypatch.setattr(
        executor,
        "observe_authorization_live_identity",
        lambda _inputs: {"live_identity_sha256": "3" * 64},
    )
    monkeypatch.setattr(
        executor.cutover,
        "build_cutover_authorization",
        lambda *_a, output_path, **_k: _write_json(
            output_path, {"fixture": "authorization"}
        ),
    )
    return SimpleNamespace(
        inputs=inputs,
        lease=lease,
        preparation=preparation,
        engine=engine,
        phases=phases,
        paths=paths,
        monkeypatch=monkeypatch,
        materialization_calls=materialization_calls,
        promotion_inputs=promotion_inputs,
        promotion_calls=promotion_calls,
        events=events,
    )


def test_authorized_session_hands_one_lease_and_dynamic_receipts_to_engine(session):
    observed = {}

    def execute(cutover_inputs, **kwargs):
        observed["inputs"] = cutover_inputs
        observed["kwargs"] = kwargs
        return cutover.CutoverResult("apply", {"ok": True})

    session.monkeypatch.setattr(executor, "execute_bound_cutover", execute)

    receipt = executor.run_authorized_cutover_session(
        session.inputs,
        authorization_decision=executor.SESSION_AUTHORIZATION_DECISION,
        operator="release-owner",
        reason="approved release window",
        clock=lambda: NOW,
        machine_identity_provider=lambda: MACHINE_IDENTITY,
        runner=object(),
    )

    assert receipt["ok"] is True
    assert session.phases == ["plan", "apply"]
    assert session.preparation.stopped is True
    assert session.materialization_calls == [False, True]
    assert session.preparation.restored is False
    assert session.promotion_calls == ["apply", "verify"]
    assert session.events.index("promotion:verify") < session.events.index(
        "stop:preparation"
    )
    assert observed["kwargs"]["lease"] is session.lease
    assert observed["kwargs"]["service_controller"] is session.engine
    assert observed["inputs"].cutover_lease_fingerprint == LEASE_FINGERPRINT
    assert session.paths["session_receipt"].is_file()


def test_authorized_session_restores_old_services_before_engine_intent(session):
    def reject(*_args, **_kwargs):
        raise RuntimeError("binding rejected")

    session.monkeypatch.setattr(executor.feishu_hold, "build_cutover_binding", reject)

    with pytest.raises(RuntimeError, match="binding rejected"):
        executor.run_authorized_cutover_session(
            session.inputs,
            authorization_decision=executor.SESSION_AUTHORIZATION_DECISION,
            operator="release-owner",
            reason="approved release window",
            clock=lambda: NOW,
            machine_identity_provider=lambda: MACHINE_IDENTITY,
            runner=object(),
        )

    assert session.preparation.stopped is True
    assert session.preparation.restored is True
    assert session.promotion_calls == ["apply", "verify", "rollback"]
    assert session.events.index("promotion:rollback") < session.events.index(
        "restore:preparation"
    )
    assert session.lease.closed is True


def test_authorized_session_restores_host_when_vm_rollback_receipt_fails(session):
    session.monkeypatch.setattr(
        executor.feishu_hold,
        "build_cutover_binding",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("binding rejected")
        ),
    )

    def rollback_failed(*_args, **_kwargs):
        session.events.append("promotion:rollback-failed")
        raise RuntimeError("remote receipt unavailable")

    session.monkeypatch.setattr(
        executor.vm_promotion,
        "rollback_promotion",
        rollback_failed,
    )

    with pytest.raises(executor.CutoverExecutorError) as error:
        executor.run_authorized_cutover_session(
            session.inputs,
            authorization_decision=executor.SESSION_AUTHORIZATION_DECISION,
            operator="release-owner",
            reason="approved release window",
            clock=lambda: NOW,
            machine_identity_provider=lambda: MACHINE_IDENTITY,
            runner=object(),
        )

    assert error.value.code == "cutover_session_vm_promotion_rollback_failed"
    assert session.preparation.restored is True
    assert session.events.index("promotion:rollback-failed") < session.events.index(
        "restore:preparation"
    )
    assert session.lease.closed is True


def test_promotion_binding_is_validated_before_cutover_lease(session):
    invalid = vm_promotion.VmPromotionInputs(
        **{
            **session.promotion_inputs.__dict__,
            "release_approval_receipt": session.paths["release_prepare_manifest"],
        }
    )
    session.monkeypatch.setattr(
        executor.vm_promotion,
        "load_manifest",
        lambda _path: invalid,
    )
    session.monkeypatch.setattr(
        executor.guard,
        "acquire_cutover_lease",
        lambda *_a, **_k: pytest.fail("lease acquired before promotion binding"),
    )

    with pytest.raises(
        executor.CutoverExecutorError,
        match="cutover_session_vm_promotion_binding_invalid",
    ):
        executor.run_authorized_cutover_session(
            session.inputs,
            authorization_decision=executor.SESSION_AUTHORIZATION_DECISION,
            operator="release-owner",
            reason="approved release window",
            clock=lambda: NOW,
            machine_identity_provider=lambda: MACHINE_IDENTITY,
            runner=object(),
        )


def test_authorization_parent_is_owner_only_before_any_production_action(session):
    session.inputs.cutover_authorization_receipt.parent.chmod(0o755)

    with pytest.raises(executor.CutoverExecutorError) as error:
        executor.run_authorized_cutover_session(
            session.inputs,
            authorization_decision=executor.SESSION_AUTHORIZATION_DECISION,
            operator="release-owner",
            reason="approved release window",
            clock=lambda: NOW,
            machine_identity_provider=lambda: MACHINE_IDENTITY,
            runner=object(),
        )

    assert (
        error.value.code
        == "cutover_session_authorization_parent_not_owner_only"
    )
    assert session.phases == []
    assert session.promotion_calls == []
    assert session.events == []
    assert session.lease.closed is False


def test_owner_only_manifest_drives_read_only_cli_validation(session, capsys):
    root = session.paths["session_receipt"].parent
    runtime_root = Path("/candidate/base-runtime")
    live_runtime = {
        "schema_version": executor.guard.RUNTIME_FILES_IDENTITY_SCHEMA_VERSION,
        "canonical_root": str(runtime_root),
        "root_identity": {"fixture": True},
        "files": {},
        "runtime_files_sha256": "4" * 64,
        "interpreter": {"sha256": "5" * 64},
    }
    expected_runtime = {
        "schema_version": executor.guard.GATEWAY_RUNNING_OBSERVATION_SCHEMA_VERSION,
        "canonical_root": str(runtime_root),
        "launchd": {
            "label": executor.guard.GATEWAY_LABEL,
            "loaded": True,
            "pid": 41001,
            "state": "running",
        },
        "process": {
            "pid": 41001,
            "process_create_time": NOW.timestamp() - 60,
            "executable": str(runtime_root / ".venv/bin/python"),
            "cwd": str(runtime_root),
            "cmdline_sha256": "6" * 64,
            "loaded_runtime_closure_sha256": executor.guard._sha256_json(
                live_runtime
            ),
        },
        "live_runtime_identity": live_runtime,
    }
    guard_plan = root / "guard-plan.json"
    _write_json(
        guard_plan,
        {
            "schema_version": executor.guard.PLAN_SCHEMA_VERSION,
            "production_effects_executed": False,
            "canonical_live_root": str(runtime_root),
            "absent_rca_files_allowed": True,
            "expected_live_runtime_identity": expected_runtime,
        },
    )
    hold = session.inputs.feishu_hold_inputs
    manifest = root / "session-manifest.json"
    _write_json(
        manifest,
        {
            "schema_version": executor.SESSION_MANIFEST_SCHEMA_VERSION,
            "release_id": session.inputs.release_id,
            "guard_plan": str(guard_plan),
            "release_prepare_manifest": str(
                session.inputs.release_prepare_manifest
            ),
            "release_approval_receipt": str(
                session.inputs.release_approval_receipt
            ),
            "vm_promotion_manifest": str(session.inputs.vm_promotion_manifest),
            "feishu_hold": {
                "env_file": str(hold.env_file),
                "host_candidate": str(hold.host_candidate),
                "live_sidecar": str(hold.live_sidecar),
                "chat_ids": list(hold.chat_ids),
                "hold_id": hold.hold_id,
                "run_root": str(hold.run_root),
                "approval_receipt": str(
                    session.inputs.feishu_hold_approval_receipt
                ),
                "cutover_binding": str(
                    session.inputs.feishu_hold_cutover_binding
                ),
                "page_size": hold.page_size,
                "max_pages": hold.max_pages,
            },
            "greenfield_store": {
                "control_db": str(session.inputs.greenfield_store.control_db),
                "delivery_db": str(session.inputs.greenfield_store.delivery_db),
                "work_dir": str(session.inputs.greenfield_store.work_dir),
                "evidence_dir": str(session.inputs.greenfield_store.evidence_dir),
                "migration_receipt": str(
                    session.inputs.greenfield_store.migration_receipt
                ),
                "config_sha256": session.inputs.greenfield_store.config_sha256,
                "bootstrap_epoch_id": (
                    session.inputs.greenfield_store.bootstrap_epoch_id
                ),
                "max_writer_stop_age_seconds": (
                    session.inputs.greenfield_store.max_writer_stop_age_seconds
                ),
            },
            "writer_stop_receipt": str(session.inputs.writer_stop_receipt),
            "env_stage_receipt": str(session.inputs.env_stage_receipt),
            "runtime_stage_manifest": str(session.inputs.runtime_stage_manifest),
            "workspace_runtime_manifest": str(
                session.inputs.workspace_runtime_manifest
            ),
            "cutover_authorization_receipt": str(
                session.inputs.cutover_authorization_receipt
            ),
            "journal_root": str(session.inputs.journal_root),
            "evidence_root": str(session.inputs.evidence_root),
            "snapshot_root": str(session.inputs.snapshot_root),
            "nonce_ledger_root": str(session.inputs.nonce_ledger_root),
            "session_receipt": str(session.inputs.session_receipt),
            "authorization_nonce": session.inputs.authorization_nonce,
            "lease_duration_seconds": session.inputs.lease_duration_seconds,
            "authorization_validity_seconds": (
                session.inputs.authorization_validity_seconds
            ),
        },
    )

    loaded = executor.load_session_manifest(manifest)
    assert loaded.expected_live_runtime_identity == expected_runtime
    assert loaded.current_runtime_root == runtime_root
    assert loaded.vm_promotion_manifest == session.inputs.vm_promotion_manifest

    assert executor.main([
        "validate-manifest",
        "--session-manifest",
        str(manifest),
    ]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["ok"] is True
    assert output["production_effects_executed"] is False
