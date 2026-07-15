#!/usr/bin/env python3
"""Execute one authority-bound RCA cutover through concrete live boundaries."""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from scripts import pnc_rca_cutover_adapter as adapter
from scripts import pnc_rca_cutover_guard as guard
from scripts import pnc_rca_cutover_live as live
from scripts import pnc_rca_feishu_ingress_hold as feishu_hold
from scripts import pnc_rca_production_cutover as cutover
from scripts import pnc_rca_store_migration_drill as store_drill
from scripts import pnc_rca_vm_promotion as vm_promotion


EXECUTOR_SCHEMA_VERSION = "pnc_rca_bound_cutover_executor_v1"
SESSION_SCHEMA_VERSION = "pnc_rca_authorized_cutover_session_v1"
SESSION_MANIFEST_SCHEMA_VERSION = "pnc_rca_cutover_session_manifest_v1"
SESSION_AUTHORIZATION_DECISION = (
    "authorize_exact_rca_greenfield_materialization_and_cutover"
)
SESSION_ACTION_SET = (
    "acquire_global_cutover_lease",
    "promote_and_verify_vm_and_worker_candidates",
    "capture_precutover_service_state",
    "stop_gateway_and_rca_writers",
    "materialize_greenfield_shared_control_delivery_store",
    "stage_feishu_ingress_hold",
    "publish_exact_short_lived_cutover_authorization",
    "execute_rollback_armed_gateway_aux_cutover",
    "leave_rca_residents_stopped_for_bounded_canaries",
)


class CutoverExecutorError(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class GreenfieldStoreInputs:
    control_db: Path
    delivery_db: Path
    work_dir: Path
    evidence_dir: Path
    migration_receipt: Path
    config_sha256: str
    bootstrap_epoch_id: str
    max_writer_stop_age_seconds: int = 900


@dataclass(frozen=True)
class CutoverSessionInputs:
    release_id: str
    release_prepare_manifest: Path
    release_approval_receipt: Path
    vm_promotion_manifest: Path
    feishu_hold_inputs: feishu_hold.HoldInputs
    feishu_hold_approval_receipt: Path
    greenfield_store: GreenfieldStoreInputs
    writer_stop_receipt: Path
    feishu_hold_cutover_binding: Path
    env_stage_receipt: Path
    runtime_stage_manifest: Path
    workspace_runtime_manifest: Path
    cutover_authorization_receipt: Path
    expected_live_runtime_identity: Mapping[str, Any]
    current_runtime_root: Path
    allow_absent_rca_files: bool
    journal_root: Path
    evidence_root: Path
    snapshot_root: Path
    nonce_ledger_root: Path
    session_receipt: Path
    authorization_nonce: str
    lease_duration_seconds: int = 2 * 60 * 60
    authorization_validity_seconds: int = 30 * 60


def observe_authorization_live_identity(
    inputs: cutover.CutoverAuthorizationInputs,
) -> Mapping[str, Any]:
    """Observe the exact current CAS before publishing cutover authorization."""
    prepared = cutover.prepare_cutover_authorization_projection(inputs)
    observer = live.ProjectedLiveIdentityObserver(
        plan=prepared.plan,
        payloads=prepared.payload_descriptors,
    )
    identity = observer()
    return {
        "schema_version": live.LIVE_IDENTITY_SCHEMA_VERSION,
        "live_identity": identity,
        "live_identity_sha256": cutover._sha256_json(identity),
        "production_effects_executed": False,
    }


def _absolute_directory(value: Path, *, code: str) -> Path:
    selected = value.expanduser()
    if not selected.is_absolute() or ".." in selected.parts:
        raise CutoverExecutorError(code)
    return selected.absolute()


def execute_bound_cutover(
    inputs: cutover.CutoverInputs,
    *,
    lease: cutover.CutoverLease,
    evidence_root: Path,
    snapshot_root: Path,
    gate_validator: cutover.GateValidator = cutover._default_gate_validator,
    clock: Callable[[], datetime] | None = None,
    machine_identity_provider: cutover.MachineIdentityProvider = (
        cutover._default_machine_identity_sha256
    ),
    nonce_ledger_root: Path = cutover.CANONICAL_NONCE_LEDGER_ROOT,
    runner: adapter.CommandRunner | None = None,
    service_controller: adapter.ServiceController | None = None,
) -> cutover.CutoverResult:
    """Bind a live adapter to one exact active lease and execute the transaction."""
    if lease is None:
        raise CutoverExecutorError("cutover_executor_lease_required")
    lease.assert_active()
    if lease.fingerprint != inputs.cutover_lease_fingerprint:
        raise CutoverExecutorError("cutover_executor_lease_fingerprint_mismatch")
    evidence = _absolute_directory(
        evidence_root, code="cutover_executor_evidence_root_invalid"
    )
    snapshots = _absolute_directory(
        snapshot_root, code="cutover_executor_snapshot_root_invalid"
    )
    current = (clock or (lambda: datetime.now(timezone.utc)))()
    if current.tzinfo is None or current.utcoffset() is None:
        raise CutoverExecutorError("cutover_executor_clock_invalid")
    current = current.astimezone(timezone.utc)
    prepared = cutover.prepare_cutover_execution(
        inputs,
        gate_validator=gate_validator,
        machine_identity_provider=machine_identity_provider,
        now=current,
    )
    identity_observer = live.ProjectedLiveIdentityObserver(
        plan=prepared.plan,
        payloads=prepared.payload_descriptors,
    )
    observed = identity_observer()
    observed_sha256 = cutover._sha256_json(observed)
    if observed_sha256 != prepared.plan["bindings"][
        "expected_live_identity_sha256"
    ]:
        raise CutoverExecutorError("cutover_executor_initial_live_identity_drift")
    authority = adapter.AdapterMutationAuthority.bind(
        plan=prepared.plan,
        gate_binding=prepared.plan["bindings"],
        validated_authorization=prepared.authorization,
        machine_identity_sha256=prepared.machine_identity_sha256,
        lease_fingerprint=lease.fingerprint,
        lease_token=lease.token,
    )
    command_runner = runner or adapter.SubprocessArgvRunner()
    services = service_controller or live.LaunchdServiceController(
        evidence_root=evidence,
        runner=command_runner,
        precutover_service_state=prepared.precutover_service_state,
    )
    system_adapter = adapter.build_production_adapter(
        authority=authority,
        identity_observer=identity_observer,
        snapshot_root=snapshots,
        runner=command_runner,
        service_controller=services,
    )
    cutover.validate_cutover_plan(
        inputs,
        prepared.plan,
        gate_validator=gate_validator,
        adapter=system_adapter,
        machine_identity_provider=machine_identity_provider,
        now=current,
    )
    lease.assert_active()
    return cutover.apply_cutover(
        inputs,
        lease=lease,
        adapter=system_adapter,
        gate_validator=gate_validator,
        clock=clock,
        machine_identity_provider=machine_identity_provider,
        nonce_ledger_root=nonce_ledger_root,
    )


def _session_hold_inputs(inputs: CutoverSessionInputs) -> feishu_hold.HoldInputs:
    hold = inputs.feishu_hold_inputs
    return feishu_hold.HoldInputs(
        env_file=hold.env_file,
        host_candidate=hold.host_candidate,
        live_sidecar=hold.live_sidecar,
        chat_ids=hold.chat_ids,
        hold_id=hold.hold_id,
        run_root=hold.run_root,
        canonical_gateway_root=hold.canonical_gateway_root,
        approval_receipt=inputs.feishu_hold_approval_receipt,
        cutover_binding=inputs.feishu_hold_cutover_binding,
        page_size=hold.page_size,
        max_pages=hold.max_pages,
    )


def _has_mutating_intent(journal_root: Path) -> bool:
    steps = journal_root.expanduser().absolute() / "steps"
    return any(
        (steps / f"{index:02d}-{step}.intent.json").is_file()
        for index, step in enumerate(cutover.STEP_NAMES, 1)
        if step in cutover.MUTATING_STEPS
    )


def run_authorized_cutover_session(
    inputs: CutoverSessionInputs,
    *,
    authorization_decision: str,
    operator: str,
    reason: str,
    gate_validator: cutover.GateValidator = cutover._default_gate_validator,
    clock: Callable[[], datetime] | None = None,
    machine_identity_provider: cutover.MachineIdentityProvider = (
        cutover._default_machine_identity_sha256
    ),
    runner: adapter.CommandRunner | None = None,
    feishu_reader: feishu_hold.ReadOnlyMessageApi | None = None,
    feishu_machine_identity_observer: Callable[[], Mapping[str, str]] = (
        feishu_hold._machine_identity
    ),
    writer_stop_observer: Callable[[], Mapping[str, Any]] | None = None,
) -> Mapping[str, Any]:
    """Run the lease-bound preparation and cutover as one rollback-owned session."""
    if authorization_decision != SESSION_AUTHORIZATION_DECISION:
        raise CutoverExecutorError("cutover_session_authorization_decision_invalid")
    if not isinstance(operator, str) or not operator.strip():
        raise CutoverExecutorError("cutover_session_operator_invalid")
    if not isinstance(reason, str) or not reason.strip():
        raise CutoverExecutorError("cutover_session_reason_invalid")
    current_time = clock or (lambda: datetime.now(timezone.utc))
    started_at = current_time()
    if started_at.tzinfo is None or started_at.utcoffset() is None:
        raise CutoverExecutorError("cutover_session_clock_invalid")
    started_at = started_at.astimezone(timezone.utc)
    command_runner = runner or adapter.SubprocessArgvRunner()
    hold_inputs = _session_hold_inputs(inputs)
    feishu_hold.run_ingress_hold(
        hold_inputs,
        phase="plan",
        reader=feishu_reader,
        now=started_at,
        machine_identity_observer=feishu_machine_identity_observer,
    )
    plan_owned = feishu_hold._read_owned_json(
        hold_inputs.run_root.expanduser().absolute() / feishu_hold.PLAN_FILENAME,
        artifact="feishu_ingress_hold_plan",
    )
    plan = plan_owned.body
    approval_owned = feishu_hold._read_owned_json(
        inputs.feishu_hold_approval_receipt,
        artifact="feishu_ingress_hold_approval",
    )
    feishu_hold._validate_approval(
        approval_owned,
        plan=plan,
        plan_sha256=plan_owned.sha256,
        machine=feishu_machine_identity_observer(),
        now=started_at,
    )
    promotion_inputs = vm_promotion.load_manifest(inputs.vm_promotion_manifest)
    if (
        promotion_inputs.release_prepare_manifest
        != inputs.release_prepare_manifest
        or promotion_inputs.release_approval_receipt
        != inputs.release_approval_receipt
    ):
        raise CutoverExecutorError("cutover_session_vm_promotion_binding_invalid")
    lease = guard.acquire_cutover_lease(
        guard.LeaseInputs(
            release_id=inputs.release_id,
            release_prepare_manifest=inputs.release_prepare_manifest,
            approval_receipt=inputs.feishu_hold_approval_receipt,
            expected_live_runtime_identity=inputs.expected_live_runtime_identity,
            canonical_live_root=inputs.current_runtime_root,
            allow_absent_rca_files=inputs.allow_absent_rca_files,
            duration_seconds=inputs.lease_duration_seconds,
        ),
        now=started_at,
        clock=current_time,
    )
    preparation_services = live.LaunchdServiceController(
        evidence_root=inputs.evidence_root / "preparation-writer-stop",
        runner=command_runner,
    )
    precutover_services: Mapping[str, Any] | None = None
    promotion_applied = False
    host_cutover_complete = False
    try:
        if not promotion_inputs.receipt_path.is_file():
            vm_promotion.apply_promotion(
                promotion_inputs,
                authorization_decision=vm_promotion.AUTHORIZATION_DECISION,
                now=current_time(),
            )
        promotion_applied = True
        vm_promotion.verify_promotion(promotion_inputs, now=current_time())
        promotion_receipt_sha256 = cutover._read_owned_json(
            promotion_inputs.receipt_path,
            artifact="cutover_session_vm_promotion_receipt",
        ).sha256
        precutover_services = preparation_services.capture_state(
            cutover.SERVICE_LABELS
        )
        writer_evidence_result = preparation_services.stop_writers(
            cutover.WRITER_LABELS,
            lease_fingerprint=lease.fingerprint,
            lease_token=lease.token,
        )
        writer_evidence_path = Path(
            str(writer_evidence_result.get("receipt_path") or "")
        )
        if not writer_evidence_path.is_absolute():
            raise CutoverExecutorError("cutover_session_writer_evidence_invalid")
        writer_evidence = store_drill._load_writer_stop_evidence(
            writer_evidence_path
        )
        store = inputs.greenfield_store
        if not store.migration_receipt.exists():
            store_drill.run_migration_drill(
                control_db_path=store.control_db,
                delivery_db_path=store.delivery_db,
                work_dir=store.work_dir,
                evidence_dir=store.evidence_dir,
                writer_stop_evidence=writer_evidence,
                receipt_path=store.migration_receipt,
                now=current_time(),
                max_writer_stop_age_seconds=store.max_writer_stop_age_seconds,
            )
        materialization_receipt = (
            store.evidence_dir.expanduser().absolute()
            / "fresh_install_materialization_receipt.json"
        )
        materialization_common = {
            "migration_receipt_path": store.migration_receipt,
            "control_db_path": store.control_db,
            "delivery_db_path": store.delivery_db,
            "config_sha256": store.config_sha256,
            "evidence_dir": store.evidence_dir,
            "writer_stop_evidence": writer_evidence,
            "now": current_time(),
            "max_writer_stop_age_seconds": store.max_writer_stop_age_seconds,
            "release_id": inputs.release_id,
            "bootstrap_epoch_id": store.bootstrap_epoch_id,
            "operator": operator.strip(),
            "reason": reason.strip(),
        }
        store_drill.materialize_fresh_install(
            **materialization_common,
            apply=False,
        )
        materialization = store_drill.materialize_fresh_install(
            **materialization_common,
            apply=True,
        )
        if (
            materialization.get("schema_version")
            != store_drill.FRESH_INSTALL_MATERIALIZATION_RECEIPT_SCHEMA_VERSION
            or materialization.get("ok") is not True
            or not materialization_receipt.is_file()
        ):
            raise CutoverExecutorError(
                "cutover_session_greenfield_materialization_invalid"
            )
        sidecar_identity = plan.get("live_sidecar_identity")
        if not isinstance(sidecar_identity, Mapping):
            raise CutoverExecutorError("cutover_session_hold_plan_invalid")

        def observe_sidecar() -> Mapping[str, Any]:
            identity, _payload = feishu_hold._sidecar_observation(
                hold_inputs.live_sidecar,
                app_scope=str(plan.get("app_scope") or ""),
            )
            return identity

        def observe_stopped() -> Mapping[str, Any]:
            if writer_stop_observer is not None:
                return writer_stop_observer()
            return guard.observe_gateway_writer_stopped(
                expected_live_runtime_identity=inputs.expected_live_runtime_identity,
                expected_live_sidecar_identity=sidecar_identity,
                sidecar_observer=observe_sidecar,
            )

        guard.observe_writer_stop(
            lease,
            guard.WriterStopInputs(
                hold_id=hold_inputs.hold_id,
                plan_sha256=plan_owned.sha256,
                receipt_path=inputs.writer_stop_receipt,
                expected_live_sidecar_identity=sidecar_identity,
                precutover_service_state=precutover_services,
            ),
            writer_stop_observer=observe_stopped,
            now=current_time(),
        )
        feishu_hold.build_cutover_binding(
            hold_inputs,
            release_id=inputs.release_id,
            writer_stop_receipt=inputs.writer_stop_receipt,
            lease=lease,
            output_path=inputs.feishu_hold_cutover_binding,
            now=current_time(),
        )
        feishu_hold.run_ingress_hold(
            hold_inputs,
            phase="apply",
            reader=feishu_reader,
            now=current_time(),
            machine_identity_observer=feishu_machine_identity_observer,
            writer_stop_observer=observe_stopped,
        )
        hold_receipt = (
            hold_inputs.run_root.expanduser().absolute()
            / feishu_hold.APPLY_RECEIPT_FILENAME
        )
        authorization_inputs = cutover.CutoverAuthorizationInputs(
            release_prepare_manifest=inputs.release_prepare_manifest,
            approval_receipt=inputs.release_approval_receipt,
            writer_stop_receipt=inputs.writer_stop_receipt,
            feishu_hold_plan=plan_owned.path,
            feishu_hold_approval_receipt=inputs.feishu_hold_approval_receipt,
            feishu_hold_cutover_binding=inputs.feishu_hold_cutover_binding,
            feishu_hold_receipt=hold_receipt,
            env_stage_receipt=inputs.env_stage_receipt,
            runtime_stage_manifest=inputs.runtime_stage_manifest,
            workspace_runtime_manifest=inputs.workspace_runtime_manifest,
            cutover_lease_fingerprint=lease.fingerprint,
        )
        live_identity = observe_authorization_live_identity(authorization_inputs)
        cutover.build_cutover_authorization(
            authorization_inputs,
            expected_live_identity_sha256=live_identity["live_identity_sha256"],
            nonce=inputs.authorization_nonce,
            output_path=inputs.cutover_authorization_receipt,
            validity_seconds=inputs.authorization_validity_seconds,
            machine_identity_provider=machine_identity_provider,
            now=current_time(),
        )
        cutover_inputs = cutover.CutoverInputs(
            **{
                field: getattr(authorization_inputs, field)
                for field in cutover.AUTHORIZATION_ARTIFACT_FIELDS
            },
            cutover_authorization_receipt=inputs.cutover_authorization_receipt,
            cutover_lease_fingerprint=lease.fingerprint,
            journal_root=inputs.journal_root,
        )
        execution_services = live.LaunchdServiceController(
            evidence_root=inputs.evidence_root / "engine-writer-stop",
            runner=command_runner,
            precutover_service_state=precutover_services,
        )
        result = execute_bound_cutover(
            cutover_inputs,
            lease=lease,
            evidence_root=inputs.evidence_root / "engine",
            snapshot_root=inputs.snapshot_root,
            gate_validator=gate_validator,
            clock=current_time,
            machine_identity_provider=machine_identity_provider,
            nonce_ledger_root=inputs.nonce_ledger_root,
            runner=command_runner,
            service_controller=execution_services,
        )
        host_cutover_complete = True
        receipt = {
            "schema_version": SESSION_SCHEMA_VERSION,
            "ok": True,
            "release_id": inputs.release_id,
            "operator": operator.strip(),
            "reason": reason.strip(),
            "authorization_decision": authorization_decision,
            "action_set": list(SESSION_ACTION_SET),
            "action_set_sha256": cutover._sha256_json(list(SESSION_ACTION_SET)),
            "started_at": started_at.isoformat(),
            "completed_at": current_time().astimezone(timezone.utc).isoformat(),
            "lease_fingerprint": lease.fingerprint,
            "vm_promotion_receipt_sha256": promotion_receipt_sha256,
            "writer_stop_receipt_sha256": cutover._read_owned_json(
                inputs.writer_stop_receipt,
                artifact="cutover_session_writer_stop",
            ).sha256,
            "feishu_hold_receipt_sha256": cutover._read_owned_json(
                hold_receipt,
                artifact="cutover_session_feishu_hold",
            ).sha256,
            "greenfield_materialization_receipt_sha256": cutover._read_owned_json(
                materialization_receipt,
                artifact="cutover_session_greenfield_materialization",
            ).sha256,
            "cutover_authorization_receipt_sha256": cutover._read_owned_json(
                inputs.cutover_authorization_receipt,
                artifact="cutover_session_authorization",
            ).sha256,
            "cutover_complete_sha256": cutover._sha256_json(result.body),
            "rca_residents_started": False,
            "next_phase": "preauthorization_and_bounded_canaries",
        }
        cutover._publish_no_clobber(inputs.session_receipt, receipt)
        return receipt
    except Exception:
        if promotion_applied and not host_cutover_complete:
            try:
                vm_promotion.rollback_promotion(
                    promotion_inputs,
                    authorization_decision=vm_promotion.AUTHORIZATION_DECISION,
                    now=current_time(),
                )
            except Exception as rollback_exc:
                raise CutoverExecutorError(
                    "cutover_session_vm_promotion_rollback_failed"
                ) from rollback_exc
        if precutover_services is not None and not _has_mutating_intent(
            inputs.journal_root
        ):
            preparation_services.restore_state(precutover_services)
        raise
    finally:
        lease.close()


def _manifest_path(value: Any, *, field: str) -> Path:
    if not isinstance(value, str) or not value:
        raise CutoverExecutorError(f"cutover_session_manifest_{field}_invalid")
    path = Path(value).expanduser()
    if not path.is_absolute() or ".." in path.parts:
        raise CutoverExecutorError(f"cutover_session_manifest_{field}_invalid")
    return path.absolute()


def load_session_manifest(path: Path) -> CutoverSessionInputs:
    owned = cutover._read_owned_json(path, artifact="cutover_session_manifest")
    body = owned.body
    expected_keys = {
        "schema_version",
        "release_id",
        "guard_plan",
        "release_prepare_manifest",
        "release_approval_receipt",
        "vm_promotion_manifest",
        "feishu_hold",
        "greenfield_store",
        "writer_stop_receipt",
        "env_stage_receipt",
        "runtime_stage_manifest",
        "workspace_runtime_manifest",
        "cutover_authorization_receipt",
        "journal_root",
        "evidence_root",
        "snapshot_root",
        "nonce_ledger_root",
        "session_receipt",
        "authorization_nonce",
        "lease_duration_seconds",
        "authorization_validity_seconds",
    }
    if set(body) != expected_keys or body.get("schema_version") != (
        SESSION_MANIFEST_SCHEMA_VERSION
    ):
        raise CutoverExecutorError("cutover_session_manifest_shape_invalid")
    guard_plan_path = _manifest_path(body.get("guard_plan"), field="guard_plan")
    guard_plan = cutover._read_owned_json(
        guard_plan_path,
        artifact="cutover_session_guard_plan",
    ).body
    if (
        guard_plan.get("schema_version") != guard.PLAN_SCHEMA_VERSION
        or guard_plan.get("production_effects_executed") is not False
        or not isinstance(guard_plan.get("expected_live_runtime_identity"), Mapping)
        or not isinstance(guard_plan.get("absent_rca_files_allowed"), bool)
    ):
        raise CutoverExecutorError("cutover_session_guard_plan_invalid")
    current_root = _manifest_path(
        guard_plan.get("canonical_live_root"),
        field="current_runtime_root",
    )
    expected_runtime = guard._normalize_running_observation(
        guard_plan["expected_live_runtime_identity"],
        canonical_root=current_root,
    )
    hold_body = body.get("feishu_hold")
    hold_keys = {
        "env_file",
        "host_candidate",
        "live_sidecar",
        "chat_ids",
        "hold_id",
        "run_root",
        "approval_receipt",
        "cutover_binding",
        "page_size",
        "max_pages",
    }
    if not isinstance(hold_body, Mapping) or set(hold_body) != hold_keys:
        raise CutoverExecutorError("cutover_session_manifest_feishu_hold_invalid")
    chat_ids = hold_body.get("chat_ids")
    if not isinstance(chat_ids, list) or not all(
        isinstance(item, str) for item in chat_ids
    ):
        raise CutoverExecutorError("cutover_session_manifest_feishu_hold_invalid")
    feishu_hold_approval = _manifest_path(
        hold_body.get("approval_receipt"), field="feishu_hold_approval_receipt"
    )
    feishu_cutover = _manifest_path(
        hold_body.get("cutover_binding"), field="feishu_hold_cutover_binding"
    )
    hold_inputs = feishu_hold._validate_inputs(feishu_hold.HoldInputs(
        env_file=_manifest_path(hold_body.get("env_file"), field="feishu_env_file"),
        host_candidate=_manifest_path(
            hold_body.get("host_candidate"), field="host_candidate"
        ),
        live_sidecar=_manifest_path(
            hold_body.get("live_sidecar"), field="live_sidecar"
        ),
        chat_ids=tuple(chat_ids),
        hold_id=str(hold_body.get("hold_id") or ""),
        run_root=_manifest_path(hold_body.get("run_root"), field="hold_run_root"),
        approval_receipt=feishu_hold_approval,
        cutover_binding=feishu_cutover,
        page_size=hold_body.get("page_size"),
        max_pages=hold_body.get("max_pages"),
    ), phase="plan")
    store_body = body.get("greenfield_store")
    store_keys = {
        "control_db",
        "delivery_db",
        "work_dir",
        "evidence_dir",
        "migration_receipt",
        "config_sha256",
        "bootstrap_epoch_id",
        "max_writer_stop_age_seconds",
    }
    if not isinstance(store_body, Mapping) or set(store_body) != store_keys:
        raise CutoverExecutorError("cutover_session_manifest_greenfield_store_invalid")
    control_db = _manifest_path(store_body.get("control_db"), field="control_db")
    delivery_db = _manifest_path(
        store_body.get("delivery_db"), field="delivery_db"
    )
    store_config_sha256 = str(store_body.get("config_sha256") or "")
    bootstrap_epoch_id = str(store_body.get("bootstrap_epoch_id") or "")
    max_writer_age = store_body.get("max_writer_stop_age_seconds")
    if (
        control_db != delivery_db
        or cutover.SHA256_RE.fullmatch(store_config_sha256) is None
        or not bootstrap_epoch_id.strip()
        or len(bootstrap_epoch_id) > 128
        or isinstance(max_writer_age, bool)
        or not isinstance(max_writer_age, int)
        or max_writer_age < 1
    ):
        raise CutoverExecutorError("cutover_session_manifest_greenfield_store_invalid")
    greenfield_store = GreenfieldStoreInputs(
        control_db=control_db,
        delivery_db=delivery_db,
        work_dir=_manifest_path(store_body.get("work_dir"), field="store_work_dir"),
        evidence_dir=_manifest_path(
            store_body.get("evidence_dir"), field="store_evidence_dir"
        ),
        migration_receipt=_manifest_path(
            store_body.get("migration_receipt"), field="store_migration_receipt"
        ),
        config_sha256=store_config_sha256,
        bootstrap_epoch_id=bootstrap_epoch_id,
        max_writer_stop_age_seconds=max_writer_age,
    )
    release_id = str(body.get("release_id") or "")
    authorization_nonce = str(body.get("authorization_nonce") or "")
    lease_duration = body.get("lease_duration_seconds")
    authorization_validity = body.get("authorization_validity_seconds")
    if (
        cutover.RELEASE_ID_RE.fullmatch(release_id) is None
        or cutover.NONCE_RE.fullmatch(authorization_nonce) is None
        or isinstance(lease_duration, bool)
        or not isinstance(lease_duration, int)
        or lease_duration < 1
        or lease_duration > guard.MAX_LEASE_SECONDS
        or isinstance(authorization_validity, bool)
        or not isinstance(authorization_validity, int)
        or authorization_validity < 1
        or authorization_validity > cutover.MAX_AUTHORIZATION_VALIDITY_SECONDS
    ):
        raise CutoverExecutorError("cutover_session_manifest_policy_invalid")
    return CutoverSessionInputs(
        release_id=release_id,
        release_prepare_manifest=_manifest_path(
            body.get("release_prepare_manifest"), field="release_prepare_manifest"
        ),
        release_approval_receipt=_manifest_path(
            body.get("release_approval_receipt"), field="release_approval_receipt"
        ),
        vm_promotion_manifest=_manifest_path(
            body.get("vm_promotion_manifest"), field="vm_promotion_manifest"
        ),
        feishu_hold_inputs=hold_inputs,
        feishu_hold_approval_receipt=feishu_hold_approval,
        greenfield_store=greenfield_store,
        writer_stop_receipt=_manifest_path(
            body.get("writer_stop_receipt"), field="writer_stop_receipt"
        ),
        feishu_hold_cutover_binding=feishu_cutover,
        env_stage_receipt=_manifest_path(
            body.get("env_stage_receipt"), field="env_stage_receipt"
        ),
        runtime_stage_manifest=_manifest_path(
            body.get("runtime_stage_manifest"), field="runtime_stage_manifest"
        ),
        workspace_runtime_manifest=_manifest_path(
            body.get("workspace_runtime_manifest"),
            field="workspace_runtime_manifest",
        ),
        cutover_authorization_receipt=_manifest_path(
            body.get("cutover_authorization_receipt"),
            field="cutover_authorization_receipt",
        ),
        expected_live_runtime_identity=expected_runtime,
        current_runtime_root=current_root,
        allow_absent_rca_files=guard_plan["absent_rca_files_allowed"],
        journal_root=_manifest_path(body.get("journal_root"), field="journal_root"),
        evidence_root=_manifest_path(
            body.get("evidence_root"), field="evidence_root"
        ),
        snapshot_root=_manifest_path(
            body.get("snapshot_root"), field="snapshot_root"
        ),
        nonce_ledger_root=_manifest_path(
            body.get("nonce_ledger_root"), field="nonce_ledger_root"
        ),
        session_receipt=_manifest_path(
            body.get("session_receipt"), field="session_receipt"
        ),
        authorization_nonce=authorization_nonce,
        lease_duration_seconds=lease_duration,
        authorization_validity_seconds=authorization_validity,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("schema")
    validate = commands.add_parser("validate-manifest")
    validate.add_argument("--session-manifest", type=Path, required=True)
    apply = commands.add_parser("apply")
    apply.add_argument("--session-manifest", type=Path, required=True)
    apply.add_argument("--authorization-decision", required=True)
    apply.add_argument("--operator", required=True)
    apply.add_argument("--reason", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    body: Mapping[str, Any] = {
        "schema_version": EXECUTOR_SCHEMA_VERSION,
        "cli_apply_supported": True,
        "owner_only_session_manifest_required": True,
        "authorization_decision": SESSION_AUTHORIZATION_DECISION,
        "action_set": list(SESSION_ACTION_SET),
        "production_projection_is_ambient": False,
        "precutover_service_state_required": True,
        "greenfield_materialization_required": True,
        "automatic_rollback": True,
        "rca_residents_started": False,
        "next_phase": "preauthorization_and_bounded_canaries",
    }
    try:
        if args.command == "schema":
            result = body
        else:
            session_inputs = load_session_manifest(args.session_manifest)
            if args.command == "validate-manifest":
                result = {
                    "schema_version": SESSION_MANIFEST_SCHEMA_VERSION,
                    "ok": True,
                    "release_id": session_inputs.release_id,
                    "production_effects_executed": False,
                }
            else:
                result = run_authorized_cutover_session(
                    session_inputs,
                    authorization_decision=args.authorization_decision,
                    operator=args.operator,
                    reason=args.reason,
                )
    except (OSError, ValueError) as exc:
        code = getattr(exc, "code", "cutover_session_failed")
        print(json.dumps({"ok": False, "code": code}, sort_keys=True))
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
