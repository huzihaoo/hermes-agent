from __future__ import annotations

import hashlib
import json
import os
import plistlib
import stat
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from gateway import pnc_rca_workspace_runtime as workspace_runtime
from scripts import pnc_rca_cutover_execute as cutover_execute
from scripts import pnc_rca_cutover_guard as cutover_guard
from scripts import pnc_rca_production_cutover as cutover


NOW = datetime.now(timezone.utc).replace(microsecond=0)
RELEASE_ID = "rca-prod-20260713-cutover"
LEASE_FINGERPRINT = "1" * 64
RECOVERY_LEASE_FINGERPRINT = "e" * 64
RECOVERY_LEASE_TOKEN = "recovery-lease-token-0002"
INITIAL_LIVE = {"schema_version": "fake_live_identity_v1", "generation": "old"}
TARGET_LIVE = {"schema_version": "fake_live_identity_v1", "generation": "target"}
INITIAL_SHA = cutover._sha256_json(INITIAL_LIVE)
TARGET_SHA = cutover._sha256_json(TARGET_LIVE)
ACTIVATION_CONTRACT_SHA = "5" * 64
MACHINE_IDENTITY_SHA = cutover._default_machine_identity_sha256()


def _write_json(path: Path, body: dict) -> bytes:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = cutover._canonical_json(body)
    path.write_bytes(raw)
    path.chmod(0o600)
    return raw


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _precutover_service_state(pid: int) -> dict:
    jobs = {}
    for label in cutover_guard.SERVICE_LABELS:
        loaded = label == cutover_guard.GATEWAY_LABEL
        jobs[label] = {
            "launchd": {
                "label": label,
                "loaded": loaded,
                "state": "running" if loaded else "absent",
                "pid": pid if loaded else None,
                "last_exit_status": None,
            },
            "plist": {
                "path": str(
                    cutover_guard.CANONICAL_LAUNCH_AGENTS_ROOT / f"{label}.plist"
                ),
                "state": "regular",
                "sha256": hashlib.sha256(label.encode()).hexdigest(),
                "size_bytes": len(label),
                "mode": "0644",
                "uid": os.geteuid(),
                "nlink": 1,
            },
        }
    return {
        "schema_version": cutover_guard.LIVE_SERVICE_STATE_SCHEMA_VERSION,
        "target_runtime_root": str(cutover_guard.CANONICAL_LIVE_ROOT),
        "labels": list(cutover_guard.SERVICE_LABELS),
        "jobs": jobs,
    }


def _write_payload(path: Path, raw: bytes, *, mode: int) -> dict:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    path.chmod(mode)
    return {
        "path": path.name,
        "sha256": hashlib.sha256(raw).hexdigest(),
        "size_bytes": len(raw),
        "mode": f"{mode:04o}",
        "source_kind": "test_payload",
    }


@pytest.fixture
def fixture(tmp_path: Path) -> SimpleNamespace:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    paths = {field: root / f"{field}.json" for field in cutover.ARTIFACT_FIELDS}
    runtime_root = root / "runtime-stage"
    runtime_root.mkdir(mode=0o700)
    workspace_root = root / "workspace-stage"
    workspace_root.mkdir(mode=0o700)
    (workspace_root / "bin").mkdir(mode=0o700)
    paths["runtime_stage_manifest"] = runtime_root / "runtime-stage-manifest.json"
    paths["workspace_runtime_manifest"] = workspace_root / "manifest.json"
    approval = {
        "schema_version": cutover.RELEASE_APPROVAL_SCHEMA_VERSION,
        "release_id": RELEASE_ID,
        "decision": "authorize_rca_production_cutover_plan",
    }
    _write_json(paths["approval_receipt"], approval)
    prepare = {
        "schema_version": cutover.RELEASE_PREPARE_SCHEMA_VERSION,
        "release_id": RELEASE_ID,
        "complete": True,
        "plan_only": True,
        "approval_receipt_sha256": _sha(paths["approval_receipt"]),
        "release_bom_sha256": "7" * 64,
    }
    _write_json(paths["release_prepare_manifest"], prepare)

    hold_plan = {
        "schema_version": cutover.FEISHU_HOLD_PLAN_SCHEMA_VERSION,
        "hold_id": "hold-20260713-cutover",
        "phase": "plan",
        "production_effects_executed": False,
    }
    _write_json(paths["feishu_hold_plan"], hold_plan)
    hold_approval = {
        "schema_version": cutover.FEISHU_HOLD_APPROVAL_SCHEMA_VERSION,
        "hold_id": hold_plan["hold_id"],
        "plan_sha256": _sha(paths["feishu_hold_plan"]),
        "decision": "authorize_feishu_ingress_hold_staging",
    }
    _write_json(paths["feishu_hold_approval_receipt"], hold_approval)

    old_gateway = {
        "schema_version": "fake_gateway_identity_v1",
        "process": {"pid": 40001},
    }
    precutover_services = _precutover_service_state(40001)
    writer_observation = {
        "schema_version": "pnc_rca_gateway_writer_stop_observation_v1",
        "launchd": {"loaded": True, "pid": None, "state": "not_running"},
        "process_census": {"matching_processes": []},
    }
    writer = {
        "schema_version": cutover.WRITER_STOP_SCHEMA_VERSION,
        "release_id": RELEASE_ID,
        "production_effects_executed": False,
        "lease_fingerprint": LEASE_FINGERPRINT,
        "release_prepare_manifest_sha256": _sha(paths["release_prepare_manifest"]),
        "approval_receipt_sha256": _sha(paths["feishu_hold_approval_receipt"]),
        "old_gateway_runtime_identity": old_gateway,
        "old_gateway_runtime_identity_sha256": cutover._sha256_json(old_gateway),
        "precutover_service_state": precutover_services,
        "precutover_service_state_sha256": cutover._sha256_json(
            precutover_services
        ),
        "writer_stop_observation": writer_observation,
    }
    _write_json(paths["writer_stop_receipt"], writer)

    hold_cutover = {
        "schema_version": cutover.FEISHU_HOLD_CUTOVER_SCHEMA_VERSION,
        "hold_id": hold_plan["hold_id"],
        "release_id": RELEASE_ID,
        "plan_sha256": _sha(paths["feishu_hold_plan"]),
        "writer_stop_receipt_sha256": _sha(paths["writer_stop_receipt"]),
        "cutover_lease_fingerprint": LEASE_FINGERPRINT,
        "release_prepare_manifest_sha256": _sha(paths["release_prepare_manifest"]),
        "release_approval_receipt_sha256": _sha(paths["feishu_hold_approval_receipt"]),
    }
    _write_json(paths["feishu_hold_cutover_binding"], hold_cutover)
    sidecar_path = root / "feishu-sidecar.staged.json"
    _write_json(sidecar_path, {"hold": True, "release_id": RELEASE_ID})
    hold_receipt = {
        "schema_version": cutover.FEISHU_HOLD_SCHEMA_VERSION,
        "hold_id": hold_plan["hold_id"],
        "ok": True,
        "production_effects_executed": False,
        "live_sidecar_written": False,
        "plan_sha256": _sha(paths["feishu_hold_plan"]),
        "approval": {"receipt_sha256": _sha(paths["feishu_hold_approval_receipt"])},
        "cutover": {"release_id": RELEASE_ID},
        "writer_stop": {
            "receipt_sha256": _sha(paths["writer_stop_receipt"]),
            "lease_fingerprint": LEASE_FINGERPRINT,
        },
        "gate_validation": {
            "cutover_binding_sha256": _sha(paths["feishu_hold_cutover_binding"])
        },
        "future_install": {
            "staged_source": str(sidecar_path),
            "staged_sha256": _sha(sidecar_path),
            "canonical_sidecar_path": str(root / "live-feishu-sidecar.json"),
        },
    }
    _write_json(paths["feishu_hold_receipt"], hold_receipt)

    candidate_env = root / "candidate.env"
    candidate_env.write_text("SECRET=not-read-by-cutover\n", encoding="utf-8")
    candidate_env.chmod(0o600)
    candidate_env_sha = _sha(candidate_env)
    runtime_state_root = root / "runtime-state"
    runtime_state_root.mkdir(mode=0o700)
    active_release_binding = runtime_state_root / cutover.ACTIVE_RELEASE_BINDING_NAME
    env_stage = {
        "schema_version": cutover.ENV_STAGE_SCHEMA_VERSION,
        "release_id": RELEASE_ID,
        "complete": True,
        "live_write_performed": False,
        "bindings": {
            "release_prepare_manifest": {
                "sha256": _sha(paths["release_prepare_manifest"])
            },
            "release_approval": {"sha256": _sha(paths["approval_receipt"])},
            "release_bom_sha256": prepare["release_bom_sha256"],
            "candidate_env": {
                "path": str(candidate_env),
                "sha256": candidate_env_sha,
            },
            "bootstrap_authorization": {
                "schema_version": "context-rca-bootstrap-capacity-authorization/v1",
                "sha256": "d" * 64,
            },
        },
        "policy": {
            "kafka": {"activation_required": True},
            "capacity_admission": {
                "mode": "bootstrap",
                "resource_class": "rca_prod",
                "bootstrap_epoch_id": "rca-bootstrap-20260713-cutover",
            }
        },
        "side_effect_contract": {
            "canonical_live_env": str(cutover.CANONICAL_ENV_PATH),
            "canonical_active_release_binding": str(active_release_binding),
        },
    }
    _write_json(paths["env_stage_receipt"], env_stage)

    runtime_file_raw = b"print('candidate runtime')\n"
    runtime_file = runtime_root / "gateway" / "candidate.py"
    runtime_descriptor = _write_payload(runtime_file, runtime_file_raw, mode=0o644)
    runtime_descriptor["path"] = "gateway/candidate.py"
    runtime_source_root = root / "runtime-source"
    runtime_source_root.mkdir(mode=0o700)
    plist_pairs = {}
    plist_hashes = {}
    for index, name in enumerate(cutover.CANDIDATE_PLISTS, 1):
        source = _write_payload(
            runtime_source_root / name,
            f"canonical-candidate-plist-{index}\n".encode(),
            mode=0o644,
        )
        source["path"] = name
        source["source_kind"] = "regular"
        staged = _write_payload(
            runtime_root / name,
            f"staged-probe-plist-{index}\n".encode(),
            mode=0o644,
        )
        staged["path"] = name
        plist_pairs[name] = {"source": source, "staged": staged}
        plist_hashes[name] = source["sha256"]
    python_raw = b"#!/bin/sh\nexit 0\n"
    python_descriptor = _write_payload(
        runtime_root / ".venv" / "bin" / "python",
        python_raw,
        mode=0o755,
    )
    python_descriptor["path"] = "bin/python"
    runtime_content = {
        "source": {
            "repo_root": str(runtime_source_root),
            "runtime_files": {"gateway/candidate.py": runtime_descriptor},
        },
        "candidate_plists": plist_pairs,
        "venv": {"files": {"bin/python": python_descriptor}},
    }
    runtime_content_sha = cutover._sha256_json(runtime_content)
    runtime_stage = {
        "schema_version": cutover.RUNTIME_STAGE_SCHEMA_VERSION,
        "complete": True,
        "production_effects_executed": False,
        "live_install_performed": False,
        "staging_root": str(runtime_root),
        "content": runtime_content,
        "content_sha256": runtime_content_sha,
        "future_canonical_projection": {
            "canonical_live_root": str(cutover.CANONICAL_RUNTIME_ROOT),
            "candidate_plist_sha256": plist_hashes,
        },
    }
    _write_json(paths["runtime_stage_manifest"], runtime_stage)
    workspace_descriptors = {}
    for index, relative in enumerate(workspace_runtime.WORKSPACE_RUNTIME_FILES, 1):
        raw = f"print('workspace-{index}')\n".encode()
        destination = workspace_root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(raw)
        destination.chmod(workspace_runtime.WORKSPACE_RUNTIME_FILE_MODES[relative])
        workspace_descriptors[relative] = (
            workspace_runtime.workspace_runtime_descriptor(
                path=relative,
                raw=raw,
                git_blob_oid=format(index, "x") * 40,
            )
        )
    workspace = workspace_runtime.build_workspace_runtime_manifest(
        source_commit="a" * 40,
        files=workspace_descriptors,
    )
    _write_json(paths["workspace_runtime_manifest"], workspace)

    auth_bindings = {
        "release_prepare_manifest_sha256": _sha(paths["release_prepare_manifest"]),
        "approval_receipt_sha256": _sha(paths["approval_receipt"]),
        "release_bom_sha256": prepare["release_bom_sha256"],
        "cutover_lease_fingerprint": LEASE_FINGERPRINT,
        "writer_stop_receipt_sha256": _sha(paths["writer_stop_receipt"]),
        "feishu_hold_plan_sha256": _sha(paths["feishu_hold_plan"]),
        "feishu_hold_approval_receipt_sha256": _sha(
            paths["feishu_hold_approval_receipt"]
        ),
        "feishu_hold_cutover_binding_sha256": _sha(
            paths["feishu_hold_cutover_binding"]
        ),
        "feishu_hold_receipt_sha256": _sha(paths["feishu_hold_receipt"]),
        "env_stage_receipt_sha256": _sha(paths["env_stage_receipt"]),
        "candidate_env_sha256": candidate_env_sha,
        "runtime_stage_manifest_sha256": _sha(paths["runtime_stage_manifest"]),
        "workspace_runtime_manifest_sha256": _sha(paths["workspace_runtime_manifest"]),
        "expected_live_identity_sha256": INITIAL_SHA,
    }
    authorization = {
        "schema_version": cutover.AUTHORIZATION_SCHEMA_VERSION,
        "release_id": RELEASE_ID,
        "decision": cutover.AUTHORIZATION_DECISION,
        "created_at": (NOW - timedelta(minutes=1)).isoformat(),
        "expires_at": (NOW + timedelta(hours=1)).isoformat(),
        "nonce": "cutover-authorization-nonce-001",
        "action_set": list(cutover.CUTOVER_ACTION_SET),
        "action_set_sha256": cutover._sha256_json(list(cutover.CUTOVER_ACTION_SET)),
        "bindings": auth_bindings,
        "identity": {
            "schema_version": cutover.AUTHORIZATION_IDENTITY_SCHEMA_VERSION,
            "method": "kernel_owner_and_machine_binding",
            "uid": os.geteuid(),
            "username": "release-owner",
            "machine_identity_sha256": MACHINE_IDENTITY_SHA,
        },
    }
    _write_json(paths["cutover_authorization_receipt"], authorization)

    journal = tmp_path / "journal"
    journal.mkdir(mode=0o700)
    inputs = cutover.CutoverInputs(
        **paths,
        cutover_lease_fingerprint=LEASE_FINGERPRINT,
        journal_root=journal,
    )
    return SimpleNamespace(
        inputs=inputs,
        paths=paths,
        journal=journal,
        root=root,
        nonce_ledger=tmp_path / "nonce-ledger",
        runtime_root=runtime_root,
        runtime_source_root=runtime_source_root,
        workspace_root=workspace_root,
        candidate_env=candidate_env,
        sidecar_path=sidecar_path,
        active_release_binding=active_release_binding,
    )


class FakeGate:
    def __init__(self, *, mutate_on_call: int | None = None, mutate=None):
        self.calls: list[dict] = []
        self.mutate_on_call = mutate_on_call
        self.mutate = mutate

    def __call__(self, **kwargs):
        self.calls.append(kwargs)
        if self.mutate_on_call == len(self.calls) and self.mutate is not None:
            self.mutate()
        sha = kwargs["artifact_sha256"]
        auth = kwargs["cutover_authorization"]
        artifacts = kwargs["artifacts"]
        runtime = artifacts["runtime_stage_manifest"]
        workspace = artifacts["workspace_runtime_manifest"]
        env_stage = artifacts["env_stage_receipt"]
        sidecar = artifacts["feishu_hold_receipt"]["future_install"]
        return {
            "schema_version": cutover.GATE_VALIDATION_SCHEMA_VERSION,
            "ok": True,
            "release_id": RELEASE_ID,
            "release_prepare_manifest_sha256": sha["release_prepare_manifest"],
            "release_approval_receipt_sha256": sha["approval_receipt"],
            "release_bom_sha256": auth["bindings"]["release_bom_sha256"],
            "cutover_lease_fingerprint": LEASE_FINGERPRINT,
            "writer_stop_receipt_sha256": sha["writer_stop_receipt"],
            "feishu_hold_plan_sha256": sha["feishu_hold_plan"],
            "feishu_hold_approval_receipt_sha256": sha["feishu_hold_approval_receipt"],
            "feishu_hold_cutover_binding_sha256": sha["feishu_hold_cutover_binding"],
            "feishu_hold_receipt_sha256": sha["feishu_hold_receipt"],
            "env_stage_receipt_sha256": sha["env_stage_receipt"],
            "active_release_binding_path": env_stage["side_effect_contract"][
                "canonical_active_release_binding"
            ],
            "runtime_stage_manifest_sha256": sha["runtime_stage_manifest"],
            "workspace_runtime_manifest_sha256": sha["workspace_runtime_manifest"],
            "cutover_authorization_receipt_sha256": sha[
                "cutover_authorization_receipt"
            ],
            "expected_live_identity_sha256": INITIAL_SHA,
            "rollback_live_identity_sha256": INITIAL_SHA,
            "target_live_identity_sha256": TARGET_SHA,
            "runtime_content_sha256": runtime["content_sha256"],
            "workspace_runtime_sha256": workspace["closure_sha256"],
            "candidate_env_sha256": env_stage["bindings"]["candidate_env"]["sha256"],
            "feishu_sidecar_sha256": sidecar["staged_sha256"],
            "candidate_plist_set_sha256": cutover._sha256_json(
                runtime["future_canonical_projection"]["candidate_plist_sha256"]
            ),
            "activation_contract_sha256": ACTIVATION_CONTRACT_SHA,
            "gateway_aux_start_order": list(cutover.GATEWAY_AUX_LABELS),
            "resident_start_order": list(reversed(cutover.RESIDENT_LABELS)),
            "allowed_next_step": kwargs["requested_step"],
            "authorization_expires_at": auth["expires_at"],
        }


class FakeLease:
    locked = False

    def __init__(
        self,
        fingerprint: str = LEASE_FINGERPRINT,
        *,
        token: str = "cutover-lease-token-0001",
        holder_pid: int = 40001,
    ):
        self.fingerprint = fingerprint
        self.token = token
        self.body = {
            "holder": {
                "pid": holder_pid,
                "process_create_time": float(holder_pid),
                "boot_id": "test-boot-identity-0001",
                "machine_identity": {"fixture": "host"},
            }
        }
        self.active = False
        self.assertions = 0

    def __enter__(self):
        if FakeLease.locked:
            raise cutover.ProductionCutoverError("production_cutover_lease_busy")
        FakeLease.locked = True
        self.active = True
        return self

    def __exit__(self, *_args):
        self.active = False
        FakeLease.locked = False

    def assert_active(self):
        self.assertions += 1
        if not self.active:
            raise cutover.ProductionCutoverError("production_cutover_lease_inactive")


class FakeAdapter:
    def __init__(
        self,
        *,
        crash_step: str | None = None,
        fail_step: str | None = None,
        unhealthy: bool = False,
        bad_commands_step: str | None = None,
        executed_command_drift_step: str | None = None,
        rollback_fails: bool = False,
        rollback_crash_phase: str | None = None,
    ):
        self.state = dict(INITIAL_LIVE)
        self.crash_step = crash_step
        self.fail_step = fail_step
        self.unhealthy = unhealthy
        self.bad_commands_step = bad_commands_step
        self.executed_command_drift_step = executed_command_drift_step
        self.rollback_fails = rollback_fails
        self.rollback_crash_phase = rollback_crash_phase
        self.executed: list[str] = []
        self.rollback_count = 0
        self.observe_count = 0
        self.preflighted: list[str] = []

    def observe_live_identity(self):
        self.observe_count += 1
        return dict(self.state)

    def _services(self, labels, plan):
        return {
            label: {
                "kind": (
                    "periodic"
                    if label in cutover.PERIODIC_SERVICE_LABELS
                    else "resident"
                ),
                "loaded": True,
                "pid": (
                    None
                    if label in cutover.PERIODIC_SERVICE_LABELS
                    else 50000 + index
                ),
                "process_create_time": (
                    None
                    if label in cutover.PERIODIC_SERVICE_LABELS
                    else NOW.timestamp() + index
                ),
                "runtime_sha256": plan["bindings"]["runtime_content_sha256"],
                "health_ok": not self.unhealthy,
            }
            for index, label in enumerate(labels, 1)
        }

    def _commands(self, step, plan):
        return cutover._expected_commands_for_step(step, plan)

    def preflight_step(
        self,
        step,
        *,
        expected_identity_sha256,
        plan,
        payload_descriptors,
        lease_fingerprint,
        lease_token,
    ):
        assert len(lease_fingerprint) == 64
        assert len(lease_token) >= 16
        self.preflighted.append(step)
        commands: object = self._commands(step, plan)
        if self.bad_commands_step == step:
            commands = ["launchctl bootstrap forbidden-shell-string"]
        return {
            "schema_version": cutover.COMMAND_PREFLIGHT_SCHEMA_VERSION,
            "step": step,
            "expected_identity_sha256": expected_identity_sha256,
            "commands": commands,
            "payload_descriptors": payload_descriptors,
            "lease_fingerprint": lease_fingerprint,
        }

    def execute_step(
        self,
        step,
        *,
        expected_identity_sha256,
        plan,
        planned_commands,
        payload_descriptors,
        lease_fingerprint,
        lease_token,
    ):
        assert cutover._sha256_json(self.state) == expected_identity_sha256
        assert len(lease_fingerprint) == 64
        assert len(lease_token) >= 16
        self.executed.append(step)
        before = expected_identity_sha256
        snapshot = None
        services = {}
        evidence = {}
        started_labels = []
        commands: object = []
        if step == "snapshot_live":
            snapshot = {
                "schema_version": cutover.SNAPSHOT_SCHEMA_VERSION,
                "snapshot_id": "snap-001",
                "before_live_identity_sha256": before,
                "rollback_target_identity_sha256": plan["bindings"][
                    "rollback_live_identity_sha256"
                ],
                "components": {
                    name: {
                        "sha256": format(index, "x") * 64,
                        "restore_ref": f"/fake/snapshots/snap-001/{name}",
                    }
                    for index, name in enumerate(
                        (
                            "runtime",
                            "workspace",
                            "environment",
                            "plists",
                            "services",
                            "feishu_sidecar",
                            "active_release_binding",
                        ),
                        1,
                    )
                },
                "old_runtime_retained": True,
            }
        elif step in cutover.MUTATING_STEPS:
            self.state = {"schema_version": "fake_live_identity_v1", "generation": step}
            commands = [list(item) for item in planned_commands]
        if step == "stop_writers":
            evidence = {
                "schema_version": "pnc_rca_writer_stop_evidence_v1",
                "writer_labels": list(cutover.WRITER_LABELS),
                "runtime_quiesce_labels": list(cutover.RUNTIME_QUIESCE_LABELS),
                "receipt_sha256": "b" * 64,
                "receipt_path": "/fake/evidence/writer-stop.json",
            }
        elif step in {
            "install_feishu_sidecar",
            "install_runtime",
            "install_workspace",
            "install_environment",
            "install_plists",
        }:
            evidence = {
                "installed_sha256": {
                    "install_feishu_sidecar": plan["bindings"]["feishu_sidecar_sha256"],
                    "install_runtime": plan["bindings"]["runtime_content_sha256"],
                    "install_workspace": plan["bindings"]["workspace_runtime_sha256"],
                    "install_environment": plan["bindings"]["candidate_env_sha256"],
                    "install_plists": plan["bindings"]["candidate_plist_set_sha256"],
                }[step],
                "post_install_verified": True,
            }
            if step == "install_environment":
                environment = plan["payload_bindings"]["candidate_environment"]
                active = plan["payload_bindings"]["active_release_binding"]
                evidence["live_environment"] = {
                    "canonical_path": environment["canonical_path"],
                    "installed_sha256": environment["sha256"],
                    "mode": "0600",
                    "uid": os.geteuid(),
                    "nlink": 1,
                    "post_install_verified": True,
                }
                evidence["active_release_binding"] = {
                    "canonical_path": active["canonical_path"],
                    "installed_sha256": active["sha256"],
                    "mode": "0600",
                    "uid": os.geteuid(),
                    "nlink": 1,
                    "post_install_verified": True,
                }
        elif step == "start_gateway_aux":
            started_labels = list(plan["gateway_aux_start_order"])
            commands = [list(item) for item in planned_commands]
            self.state = dict(TARGET_LIVE)
        elif step == "verify_gateway_aux":
            services = self._services(cutover.GATEWAY_AUX_LABELS, plan)
        if self.crash_step == step:
            raise cutover.CutoverCrash(step)
        if self.fail_step == step:
            raise RuntimeError(f"failed:{step}")
        if self.executed_command_drift_step == step:
            commands = [["fake-cutover", "drifted-command"]]
        return {
            "schema_version": cutover.STEP_RESULT_SCHEMA_VERSION,
            "step": step,
            "before_identity_sha256": before,
            "after_identity_sha256": cutover._sha256_json(self.state),
            "commands": commands,
            "old_runtime_retained": True,
            "snapshot": snapshot,
            "services": services,
            "evidence": evidence,
            "started_labels": started_labels,
        }

    def rollback(
        self,
        *,
        snapshot,
        expected_identity_sha256,
        plan,
        planned_commands,
        lease_fingerprint,
        lease_token,
    ):
        assert cutover._sha256_json(self.state) == expected_identity_sha256
        assert len(lease_fingerprint) == 64
        assert len(lease_token) >= 16
        self.rollback_count += 1
        if self.rollback_crash_phase == "before_effect":
            raise cutover.CutoverCrash("rollback-before-effect")
        if self.rollback_fails:
            raise RuntimeError("rollback failed")
        before = expected_identity_sha256
        self.state = dict(INITIAL_LIVE)
        if self.rollback_crash_phase == "after_effect":
            raise cutover.CutoverCrash("rollback-after-effect")
        return {
            "schema_version": cutover.STEP_RESULT_SCHEMA_VERSION,
            "step": "rollback",
            "before_identity_sha256": before,
            "after_identity_sha256": cutover._sha256_json(self.state),
            "commands": [list(item) for item in planned_commands],
            "old_runtime_retained": True,
            "snapshot": None,
            "services": {},
            "evidence": {},
            "started_labels": [],
        }


@pytest.fixture(autouse=True)
def reset_fake_lock():
    FakeLease.locked = False
    yield
    FakeLease.locked = False


def _apply(fixture, adapter=None, gate=None, lease=None):
    return cutover.apply_cutover(
        fixture.inputs,
        lease=lease or FakeLease(),
        adapter=adapter or FakeAdapter(),
        gate_validator=gate or FakeGate(),
        clock=lambda: NOW,
        nonce_ledger_root=fixture.nonce_ledger,
    )


def _recovery_lease() -> FakeLease:
    return FakeLease(
        RECOVERY_LEASE_FINGERPRINT,
        token=RECOVERY_LEASE_TOKEN,
        holder_pid=50002,
    )


def _write_recovery_authorization(
    fixture,
    lease: FakeLease,
    *,
    created_at: datetime | None = None,
    expires_at: datetime | None = None,
    nonce: str = "cutover-recovery-nonce-0001",
) -> Path:
    context = cutover._load_historical_recovery_context(fixture.journal)
    lease_identity = cutover._lease_execution_identity(lease)
    forward = context["run_identity"]["forward_lease"]
    path = fixture.root / f"recovery-authorization-{nonce}.json"
    body = {
        "schema_version": cutover.RECOVERY_AUTHORIZATION_SCHEMA_VERSION,
        "release_id": RELEASE_ID,
        "decision": cutover.RECOVERY_AUTHORIZATION_DECISION,
        "created_at": (created_at or (NOW - timedelta(minutes=1))).isoformat(),
        "expires_at": (expires_at or (NOW + timedelta(minutes=10))).isoformat(),
        "nonce": nonce,
        "bindings": {
            "original_plan_sha256": cutover._sha256_json(context["plan"]),
            "journal_root": str(fixture.journal.resolve()),
            "run_identity_sha256": context["run_owned"].sha256,
            "snapshot_sha256": cutover._sha256_json(context["snapshot"]),
            "rollback_target_identity_sha256": context["run_identity"][
                "rollback_live_identity_sha256"
            ],
            "forward_lease_fingerprint": forward["fingerprint"],
            "forward_lease_token_sha256": forward["token_sha256"],
            "forward_holder_sha256": forward["holder_sha256"],
            "recovery_lease_fingerprint": lease_identity["fingerprint"],
            "recovery_lease_token_sha256": lease_identity["token_sha256"],
            "recovery_holder_sha256": lease_identity["holder_sha256"],
            "recovery_pid": lease_identity["holder"]["pid"],
            "machine_identity_sha256": MACHINE_IDENTITY_SHA,
        },
        "identity": {
            "schema_version": cutover.AUTHORIZATION_IDENTITY_SCHEMA_VERSION,
            "uid": os.geteuid(),
            "username": "recovery-owner",
            "machine_identity_sha256": MACHINE_IDENTITY_SHA,
        },
    }
    _write_json(path, body)
    return path


def _recover(fixture, adapter, lease=None, authorization=None):
    selected_lease = lease or _recovery_lease()
    receipt = authorization or _write_recovery_authorization(fixture, selected_lease)
    return cutover.recover_cutover(
        fixture.journal,
        recovery_authorization_receipt=receipt,
        lease=selected_lease,
        adapter=adapter,
        clock=lambda: NOW,
        nonce_ledger_root=fixture.nonce_ledger,
    )


def test_plan_never_observes_live_or_creates_journal_artifacts(fixture) -> None:
    gate = FakeGate()
    plan = cutover.build_cutover_plan(fixture.inputs, gate_validator=gate, now=NOW)

    assert plan["production_effects_executed"] is False
    assert plan["execution_contract"]["cli_apply_supported"] is False
    assert plan["writers_stopped_before_install"] is True
    assert plan["resident_start_order"] == list(reversed(cutover.RESIDENT_LABELS))
    assert list(fixture.journal.iterdir()) == []
    assert len(gate.calls) == 1
    assert gate.calls[0]["requested_step"] == "plan"
    staged_env = gate.calls[0]["artifacts"]["env_stage_receipt"]
    assert staged_env["policy"]["capacity_admission"]["mode"] == "bootstrap"
    assert staged_env["bindings"]["bootstrap_authorization"]["sha256"] == "d" * 64
    assert plan["bindings"]["active_release_binding_path"] == str(
        fixture.active_release_binding
    )
    assert plan["payload_bindings"]["active_release_binding"] == {
        "source_path": str(fixture.paths["env_stage_receipt"]),
        "sha256": _sha(fixture.paths["env_stage_receipt"]),
        "canonical_path": str(fixture.active_release_binding),
    }
    assert not fixture.nonce_ledger.exists()


def test_prepare_execution_freezes_plan_authorization_and_payloads(fixture) -> None:
    gate = FakeGate()

    prepared = cutover.prepare_cutover_execution(
        fixture.inputs,
        gate_validator=gate,
        machine_identity_provider=lambda: MACHINE_IDENTITY_SHA,
        now=NOW,
    )

    assert prepared.plan["bindings"]["cutover_lease_fingerprint"] == (
        LEASE_FINGERPRINT
    )
    assert prepared.authorization["receipt_sha256"] == _sha(
        fixture.paths["cutover_authorization_receipt"]
    )
    assert prepared.authorization["machine_identity_sha256"] == MACHINE_IDENTITY_SHA
    assert set(prepared.payload_descriptors) == {
        "candidate_environment",
        "active_release_binding",
        "feishu_sidecar",
        "runtime",
        "workspace",
    }
    assert prepared.machine_identity_sha256 == MACHINE_IDENTITY_SHA
    assert [call["requested_step"] for call in gate.calls] == ["plan"]
    assert list(fixture.journal.iterdir()) == []


def test_authorization_builder_validates_before_no_clobber_publish(fixture) -> None:
    authorization_path = fixture.paths["cutover_authorization_receipt"]
    authorization_path.unlink()
    authorization_inputs = cutover.CutoverAuthorizationInputs(
        **{
            field: getattr(fixture.inputs, field)
            for field in cutover.AUTHORIZATION_ARTIFACT_FIELDS
        },
        cutover_lease_fingerprint=LEASE_FINGERPRINT,
    )

    projection = cutover.prepare_cutover_authorization_projection(
        authorization_inputs
    )
    body = cutover.build_cutover_authorization(
        authorization_inputs,
        expected_live_identity_sha256=INITIAL_SHA,
        nonce="cutover-authorization-builder-0001",
        output_path=authorization_path,
        machine_identity_provider=lambda: MACHINE_IDENTITY_SHA,
        now=NOW,
    )

    assert projection.plan["bindings"]["runtime_content_sha256"] == (
        json.loads(fixture.paths["runtime_stage_manifest"].read_text())[
            "content_sha256"
        ]
    )
    assert body["bindings"]["expected_live_identity_sha256"] == INITIAL_SHA
    assert stat.S_IMODE(authorization_path.stat().st_mode) == 0o600
    assert not list(authorization_path.parent.glob("*.validation"))
    prepared = cutover.prepare_cutover_execution(
        fixture.inputs,
        gate_validator=FakeGate(),
        machine_identity_provider=lambda: MACHINE_IDENTITY_SHA,
        now=NOW,
    )
    assert prepared.authorization["receipt_sha256"] == _sha(authorization_path)


def test_validate_is_read_only_and_only_observes_live(fixture) -> None:
    gate = FakeGate()
    adapter = FakeAdapter()
    plan = cutover.build_cutover_plan(fixture.inputs, gate_validator=gate, now=NOW)
    result = cutover.validate_cutover_plan(
        fixture.inputs,
        plan,
        gate_validator=gate,
        adapter=adapter,
        now=NOW,
    )

    assert result["ok"] is True
    assert result["production_effects_executed"] is False
    assert adapter.executed == []
    assert adapter.observe_count == 1
    assert list(fixture.journal.iterdir()) == []


def test_apply_runs_exact_phases_and_writes_no_clobber_journal(fixture) -> None:
    gate = FakeGate()
    lease = FakeLease()
    adapter = FakeAdapter()
    result = _apply(fixture, adapter=adapter, gate=gate, lease=lease)

    assert result.body["ok"] is True
    assert result.body["old_runtime_retained"] is True
    assert adapter.executed == list(cutover.STEP_NAMES)
    assert adapter.state == TARGET_LIVE
    assert lease.assertions >= len(cutover.STEP_NAMES) + 2
    assert adapter.preflighted[: len(cutover.STEP_NAMES) + 1] == [
        *cutover.STEP_NAMES,
        "rollback",
    ]
    assert (fixture.journal / "complete.json").is_file()
    assert len(list(fixture.nonce_ledger.glob("*.json"))) == 1
    for index, step in enumerate(cutover.STEP_NAMES, 1):
        assert (fixture.journal / "steps" / f"{index:02d}-{step}.intent.json").is_file()
        assert (fixture.journal / "steps" / f"{index:02d}-{step}.done.json").is_file()
    requested = [call["requested_step"] for call in gate.calls]
    assert requested.count("plan") == 2
    assert requested[-len(cutover.STEP_NAMES) :] == list(cutover.STEP_NAMES)
    env_intent = json.loads(
        (fixture.journal / "steps" / "06-install_environment.intent.json").read_text()
    )
    assert set(env_intent["payload_descriptors"]) == {
        "candidate_environment",
        "active_release_binding",
    }
    env_done = json.loads(
        (fixture.journal / "steps" / "06-install_environment.done.json").read_text()
    )
    assert env_done["result"]["evidence"]["active_release_binding"] == {
        "canonical_path": str(fixture.active_release_binding),
        "installed_sha256": _sha(fixture.paths["env_stage_receipt"]),
        "mode": "0600",
        "uid": os.geteuid(),
        "nlink": 1,
        "post_install_verified": True,
    }


def test_bound_executor_wires_one_lease_authority_and_transaction(
    fixture, tmp_path, monkeypatch
) -> None:
    gate = FakeGate()
    lease = FakeLease()
    lease.active = True
    fake_adapter = FakeAdapter()
    captured = {}

    class Observer:
        def __init__(self, *, plan, payloads):
            captured["plan"] = plan
            captured["payloads"] = payloads

        def __call__(self):
            return dict(INITIAL_LIVE)

    def build_adapter(**kwargs):
        captured["adapter_kwargs"] = kwargs
        return fake_adapter

    monkeypatch.setattr(
        cutover_execute.live,
        "ProjectedLiveIdentityObserver",
        Observer,
    )
    monkeypatch.setattr(
        cutover_execute.adapter,
        "build_production_adapter",
        build_adapter,
    )

    result = cutover_execute.execute_bound_cutover(
        fixture.inputs,
        lease=lease,
        evidence_root=tmp_path / "evidence",
        snapshot_root=tmp_path / "snapshots",
        gate_validator=gate,
        clock=lambda: NOW,
        machine_identity_provider=lambda: MACHINE_IDENTITY_SHA,
        nonce_ledger_root=fixture.nonce_ledger,
        runner=object(),
        service_controller=object(),
    )

    assert result.body["ok"] is True
    assert fake_adapter.executed == list(cutover.STEP_NAMES)
    assert captured["plan"]["bindings"]["cutover_lease_fingerprint"] == (
        lease.fingerprint
    )
    assert set(captured["payloads"]) == {
        "candidate_environment",
        "active_release_binding",
        "feishu_sidecar",
        "runtime",
        "workspace",
    }
    assert captured["adapter_kwargs"]["authority"].lease_fingerprint == (
        lease.fingerprint
    )


def test_completed_apply_is_idempotent(fixture) -> None:
    adapter = FakeAdapter()
    first = _apply(fixture, adapter=adapter)
    second = _apply(fixture, adapter=adapter)

    assert first.resumed is False
    assert second.resumed is True
    assert adapter.executed == list(cutover.STEP_NAMES)


def test_resume_continues_after_done_receipt_crash_without_replaying_step(
    fixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    adapter = FakeAdapter()
    original = cutover._publish_no_clobber
    crashed = False

    def crash_after_done(path, body):
        nonlocal crashed
        result = original(path, body)
        if path.name == "04-install_runtime.done.json" and not crashed:
            crashed = True
            raise cutover.CutoverCrash("after-done")
        return result

    monkeypatch.setattr(cutover, "_publish_no_clobber", crash_after_done)
    with pytest.raises(cutover.CutoverCrash):
        _apply(fixture, adapter=adapter)
    executed_before_resume = list(adapter.executed)

    result = _apply(fixture, adapter=adapter)

    assert result.resumed is True
    assert adapter.executed[: len(executed_before_resume)] == executed_before_resume
    assert len(adapter.executed) == len(cutover.STEP_NAMES)
    assert adapter.state == TARGET_LIVE


@pytest.mark.parametrize("step", cutover.STEP_NAMES)
def test_every_step_hard_crash_is_detected_on_resume(fixture, step: str) -> None:
    adapter = FakeAdapter(crash_step=step)
    with pytest.raises(cutover.CutoverCrash):
        _apply(fixture, adapter=adapter)
    adapter.crash_step = None

    if step == "snapshot_live":
        expected = "production_cutover_incomplete_snapshot_requires_new_authorization"
    else:
        expected = "production_cutover_recovery_authorization_required"
    with pytest.raises(cutover.ProductionCutoverError) as error:
        _apply(fixture, adapter=adapter)

    assert error.value.code == expected
    if step != "snapshot_live":
        assert adapter.rollback_count == 0
        result = _recover(fixture, adapter)
        assert result.phase == "recover"
        assert result.body["forward_steps_executed"] is False
        assert adapter.rollback_count == 1
        assert adapter.state == INITIAL_LIVE


@pytest.mark.parametrize(
    "step",
    ["stop_writers", "install_runtime", "start_gateway_aux"],
)
def test_external_command_failure_rolls_back_exact_snapshot(fixture, step: str) -> None:
    adapter = FakeAdapter(fail_step=step)

    with pytest.raises(cutover.ProductionCutoverError) as error:
        _apply(fixture, adapter=adapter)

    assert error.value.code == "production_cutover_apply_failed_rolled_back"
    assert adapter.rollback_count == 1
    assert adapter.state == INITIAL_LIVE
    assert (fixture.journal / "failure.json").is_file()
    assert (fixture.journal / "rollback.json").is_file()


def test_unhealthy_service_verification_rolls_back(fixture) -> None:
    adapter = FakeAdapter(unhealthy=True)

    with pytest.raises(cutover.ProductionCutoverError) as error:
        _apply(fixture, adapter=adapter)

    assert error.value.code == "production_cutover_apply_failed_rolled_back"
    assert adapter.state == INITIAL_LIVE


def test_shell_string_command_is_rejected_by_preflight_before_mutation(fixture) -> None:
    adapter = FakeAdapter(bad_commands_step="install_runtime")

    with pytest.raises(cutover.ProductionCutoverError) as error:
        _apply(fixture, adapter=adapter)

    assert error.value.code == "production_cutover_step_commands_invalid"
    assert adapter.executed == []
    assert not (fixture.journal / "failure.json").exists()


@pytest.mark.parametrize(
    ("step", "commands", "code"),
    [
        (
            "install_runtime",
            [["sh", "-c", "cp runtime && rm -rf old"]],
            "production_cutover_command_not_allowlisted",
        ),
        (
            "install_runtime",
            [["/usr/bin/env", "sh", "-c", "rm -rf old"]],
            "production_cutover_command_not_allowlisted",
        ),
        (
            "install_runtime",
            [["rm", "-rf", "/old-runtime"]],
            "production_cutover_command_not_allowlisted",
        ),
        (
            "install_plists",
            [["launchctl", "bootstrap", "candidate.plist"]],
            "production_cutover_command_not_allowlisted",
        ),
        (
            "install_runtime",
            [["python3", "-c", "import os; os.unlink('x')"]],
            "production_cutover_command_not_allowlisted",
        ),
        (
            "install_runtime",
            [["find", "/tmp", "-delete"]],
            "production_cutover_command_not_allowlisted",
        ),
        (
            "install_runtime",
            [["mv", "source", "destination"]],
            "production_cutover_command_not_allowlisted",
        ),
        (
            "install_runtime",
            [["dd", "if=source", "of=destination"]],
            "production_cutover_command_not_allowlisted",
        ),
        (
            "install_runtime",
            [["truncate", "-s", "0", "destination"]],
            "production_cutover_command_not_allowlisted",
        ),
        (
            "install_runtime",
            [["unlisted-copy", "source", "destination"]],
            "production_cutover_command_not_allowlisted",
        ),
        (
            "install_runtime",
            [],
            "production_cutover_command_not_allowlisted",
        ),
    ],
)
def test_shell_destructive_and_early_start_commands_are_rejected(
    fixture, step: str, commands: list[list[str]], code: str
) -> None:
    adapter = FakeAdapter()
    original = adapter.preflight_step

    def preflight(selected, **kwargs):
        result = original(selected, **kwargs)
        if selected == step:
            result["commands"] = commands
        return result

    adapter.preflight_step = preflight

    with pytest.raises(cutover.ProductionCutoverError) as error:
        _apply(fixture, adapter=adapter)

    assert error.value.code == code
    assert adapter.executed == []
    assert not (fixture.journal / "failure.json").exists()


def test_rollback_failure_is_fail_closed(fixture) -> None:
    adapter = FakeAdapter(fail_step="install_runtime", rollback_fails=True)

    with pytest.raises(cutover.ProductionCutoverError) as error:
        _apply(fixture, adapter=adapter)

    assert error.value.code == "production_cutover_rollback_failed"
    assert not (fixture.journal / "complete.json").exists()

    adapter.rollback_fails = False
    with pytest.raises(cutover.ProductionCutoverError) as recovered:
        _apply(fixture, adapter=adapter)

    assert recovered.value.code == "production_cutover_recovery_authorization_required"
    result = _recover(fixture, adapter)
    assert result.phase == "recover"
    assert adapter.state == INITIAL_LIVE
    assert (fixture.journal / "rollback.done.json").is_file()


def test_global_lease_blocks_concurrent_apply_before_mutation(fixture) -> None:
    held = FakeLease()
    contender = FakeLease()
    adapter = FakeAdapter()

    with held:
        with pytest.raises(cutover.ProductionCutoverError) as error:
            _apply(fixture, adapter=adapter, lease=contender)

    assert error.value.code == "production_cutover_lease_busy"
    assert adapter.executed == []


def test_input_drift_under_lease_fails_before_first_external_step(fixture) -> None:
    def mutate():
        body = json.loads(fixture.paths["runtime_stage_manifest"].read_text())
        body["content_sha256"] = "f" * 64
        _write_json(fixture.paths["runtime_stage_manifest"], body)

    gate = FakeGate(mutate_on_call=2, mutate=mutate)
    adapter = FakeAdapter()

    with pytest.raises(cutover.ProductionCutoverError) as error:
        _apply(fixture, adapter=adapter, gate=gate)

    assert error.value.code in {
        "production_cutover_authorization_binding_mismatch",
        "production_cutover_input_drift",
    }
    assert adapter.executed == []


def test_wrong_release_approval_binding_is_rejected(fixture) -> None:
    gate = FakeGate()
    original = gate.__call__

    def wrong(**kwargs):
        result = original(**kwargs)
        result["release_approval_receipt_sha256"] = "f" * 64
        return result

    with pytest.raises(cutover.ProductionCutoverError) as error:
        cutover.build_cutover_plan(fixture.inputs, gate_validator=wrong, now=NOW)

    assert error.value.code == "production_cutover_gate_binding_mismatch"


def test_authorization_expiry_is_rechecked_before_each_external_step(fixture) -> None:
    calls = 0

    def clock():
        nonlocal calls
        calls += 1
        return NOW if calls <= 3 else NOW + timedelta(hours=2)

    adapter = FakeAdapter()
    with pytest.raises(cutover.ProductionCutoverError) as error:
        cutover.apply_cutover(
            fixture.inputs,
            lease=FakeLease(),
            adapter=adapter,
            gate_validator=FakeGate(),
            clock=clock,
            nonce_ledger_root=fixture.nonce_ledger,
        )

    assert error.value.code == "production_cutover_authorization_expired"
    assert adapter.executed == []


def test_apply_rejects_fixed_now_and_requires_clock_for_determinism(fixture) -> None:
    with pytest.raises(cutover.ProductionCutoverError) as error:
        cutover.apply_cutover(
            fixture.inputs,
            lease=FakeLease(),
            adapter=FakeAdapter(),
            gate_validator=FakeGate(),
            now=NOW,
            nonce_ledger_root=fixture.nonce_ledger,
        )

    assert error.value.code == "production_cutover_fixed_now_forbidden"
    assert not fixture.nonce_ledger.exists()


def test_machine_identity_must_match_authorization(fixture) -> None:
    with pytest.raises(cutover.ProductionCutoverError) as error:
        cutover.build_cutover_plan(
            fixture.inputs,
            gate_validator=FakeGate(),
            machine_identity_provider=lambda: "f" * 64,
            now=NOW,
        )

    assert error.value.code == "production_cutover_authorization_machine_mismatch"


def test_machine_identity_drift_fails_before_external_execution(fixture) -> None:
    calls = 0

    def machine_identity():
        nonlocal calls
        calls += 1
        return MACHINE_IDENTITY_SHA if calls <= 2 else "f" * 64

    adapter = FakeAdapter()
    with pytest.raises(cutover.ProductionCutoverError) as error:
        cutover.apply_cutover(
            fixture.inputs,
            lease=FakeLease(),
            adapter=adapter,
            gate_validator=FakeGate(),
            clock=lambda: NOW,
            machine_identity_provider=machine_identity,
            nonce_ledger_root=fixture.nonce_ledger,
        )

    assert error.value.code == "production_cutover_machine_identity_drift"
    assert adapter.executed == []


def test_nonce_ledger_blocks_same_authorization_in_changed_journal(fixture) -> None:
    first_adapter = FakeAdapter()
    _apply(fixture, adapter=first_adapter)
    second_journal = fixture.journal.parent / "changed-journal"
    second_journal.mkdir(mode=0o700)
    changed = replace(fixture.inputs, journal_root=second_journal)
    second_adapter = FakeAdapter()

    with pytest.raises(cutover.ProductionCutoverError) as error:
        cutover.apply_cutover(
            changed,
            lease=FakeLease(),
            adapter=second_adapter,
            gate_validator=FakeGate(),
            clock=lambda: NOW,
            nonce_ledger_root=fixture.nonce_ledger,
        )

    assert error.value.code == "production_cutover_authorization_replay"
    assert second_adapter.executed == []


def test_preflight_binds_exact_start_commands_and_order(fixture) -> None:
    adapter = FakeAdapter()
    _apply(fixture, adapter=adapter)
    plan = cutover.build_cutover_plan(
        fixture.inputs, gate_validator=FakeGate(), now=NOW
    )

    assert cutover.WRITER_LABELS == (
        "ai.hermes.gateway",
        *cutover.RESIDENT_LABELS,
    )
    assert cutover.RUNTIME_QUIESCE_LABELS == cutover.SERVICE_LABELS
    assert cutover._expected_commands_for_step("stop_writers", plan) == [
        [
            cutover.CUTOVER_ADAPTER_EXECUTABLE,
            "stop-writers",
            *cutover.RUNTIME_QUIESCE_LABELS,
        ]
    ]
    assert "ai.hermes.gateway.candidate.plist" in cutover.CANDIDATE_PLISTS
    assert cutover._expected_start_commands("start_gateway_aux", plan) == [
        [
            "/bin/launchctl",
            "bootstrap",
            f"gui/{os.geteuid()}",
            str(cutover.CANONICAL_LAUNCH_AGENTS_ROOT / f"{label}.plist"),
        ]
        for label in cutover.GATEWAY_AUX_LABELS
    ]
    assert "start_residents" not in cutover.STEP_NAMES
    assert cutover._expected_start_commands("start_residents", plan) == []


def test_gateway_candidate_preserves_v0182_production_inheritance() -> None:
    candidate_path = (
        Path(__file__).resolve().parents[2] / "ai.hermes.gateway.candidate.plist"
    )
    candidate = plistlib.loads(candidate_path.read_bytes())
    environment = candidate["EnvironmentVariables"]

    assert environment["G1Q3_GOVERNANCE_DOWNLOAD_ENABLED"] == "1"
    assert environment["HERMES_DISABLE_LAZY_INSTALLS"] == "1"
    assert environment["HERMES_FEISHU_API_POLL_STARTUP_LOOKBACK_SECONDS"] == "120"
    assert environment["HERMES_LAZY_INSTALL_TARGET"] == (
        "/Users/songying/.hermes/runtime/lazy-packages/v0182-py311"
    )
    assert environment["PYTHONDONTWRITEBYTECODE"] == "1"
    assert candidate["ProgramArguments"][0] == (
        "/Users/songying/.hermes/runtime/hermes-live/.venv/bin/python"
    )
    assert candidate["WorkingDirectory"] == (
        "/Users/songying/.hermes/runtime/hermes-live"
    )


def test_install_plists_use_install_ready_source_root() -> None:
    plan = {
        "payload_bindings": {
            "runtime": {
                "staging_root": "/candidate/runtime-stage",
                "candidate_plist_root": "/candidate/install-plists",
                "candidate_plist_sha256": {
                    name: format(index, "x") * 64
                    for index, name in enumerate(cutover.CANDIDATE_PLISTS, 1)
                },
            }
        }
    }

    commands = cutover._expected_commands_for_step("install_plists", plan)

    assert [command[2] for command in commands] == [
        f"/candidate/install-plists/{name}" for name in cutover.CANDIDATE_PLISTS
    ]


def test_wrong_start_command_order_fails_in_preflight(fixture) -> None:
    class WrongOrderAdapter(FakeAdapter):
        def preflight_step(self, step, **kwargs):
            result = super().preflight_step(step, **kwargs)
            if step == "start_gateway_aux":
                result["commands"] = list(reversed(result["commands"]))
            return result

    adapter = WrongOrderAdapter()
    with pytest.raises(cutover.ProductionCutoverError) as error:
        _apply(fixture, adapter=adapter)

    assert error.value.code == "production_cutover_command_not_allowlisted"
    assert adapter.executed == []


def test_rollback_commands_are_preflighted_before_first_mutation(fixture) -> None:
    class UnsafeRollbackAdapter(FakeAdapter):
        def preflight_step(self, step, **kwargs):
            result = super().preflight_step(step, **kwargs)
            if step == "rollback":
                result["commands"] = [["python3", "-c", "print('unsafe')"]]
            return result

    adapter = UnsafeRollbackAdapter()
    with pytest.raises(cutover.ProductionCutoverError) as error:
        _apply(fixture, adapter=adapter)

    assert error.value.code == "production_cutover_command_not_allowlisted"
    assert adapter.executed == []


def test_environment_hardlink_command_is_rejected_in_preflight(fixture) -> None:
    class HardlinkAdapter(FakeAdapter):
        def preflight_step(self, step, **kwargs):
            result = super().preflight_step(step, **kwargs)
            if step == "install_environment":
                result["commands"] = [["ln", "source", "destination"]]
            return result

    adapter = HardlinkAdapter()
    with pytest.raises(cutover.ProductionCutoverError) as error:
        _apply(fixture, adapter=adapter)

    assert error.value.code == "production_cutover_command_not_allowlisted"
    assert adapter.executed == []


def test_executed_commands_must_exactly_match_preflight(fixture) -> None:
    adapter = FakeAdapter(executed_command_drift_step="install_runtime")
    with pytest.raises(cutover.ProductionCutoverError) as error:
        _apply(fixture, adapter=adapter)

    assert error.value.code == "production_cutover_apply_failed_rolled_back"
    failure = json.loads((fixture.journal / "failure.json").read_text())
    assert failure["code"] == "production_cutover_command_not_allowlisted"
    assert adapter.state == INITIAL_LIVE


def test_payload_hash_mismatch_fails_before_first_external_step(fixture) -> None:
    fixture.candidate_env.write_bytes(b"tampered-before-apply\n")
    fixture.candidate_env.chmod(0o600)
    adapter = FakeAdapter()

    with pytest.raises(cutover.ProductionCutoverError) as error:
        _apply(fixture, adapter=adapter)

    assert error.value.code == "production_cutover_payload_hash_mismatch"
    assert adapter.executed == []


@pytest.mark.parametrize(
    "payload_path",
    [
        "candidate_env",
        "sidecar_path",
        "runtime_root/gateway/candidate.py",
        f"runtime_source_root/{cutover.CANDIDATE_PLISTS[0]}",
        "workspace_root/bin/create_task_v2.py",
    ],
)
def test_payload_mutation_after_first_mutation_forces_rollback(
    fixture, payload_path: str
) -> None:
    if "/" in payload_path:
        owner, relative = payload_path.split("/", 1)
        path = getattr(fixture, owner) / relative
    else:
        path = getattr(fixture, payload_path)
    adapter = FakeAdapter()
    original = adapter.execute_step

    def execute(step, **kwargs):
        result = original(step, **kwargs)
        if step == "stop_writers":
            path.write_bytes(path.read_bytes() + b"drift")
        return result

    adapter.execute_step = execute
    with pytest.raises(cutover.ProductionCutoverError) as error:
        _apply(fixture, adapter=adapter)

    assert error.value.code == "production_cutover_apply_failed_rolled_back"
    assert adapter.state == INITIAL_LIVE


def test_same_bytes_new_inode_is_detected_as_payload_drift(fixture) -> None:
    adapter = FakeAdapter()
    original = adapter.execute_step

    def execute(step, **kwargs):
        result = original(step, **kwargs)
        if step == "stop_writers":
            replacement = fixture.candidate_env.with_suffix(".replacement")
            replacement.write_bytes(fixture.candidate_env.read_bytes())
            replacement.chmod(0o600)
            os.replace(replacement, fixture.candidate_env)
        return result

    adapter.execute_step = execute
    with pytest.raises(cutover.ProductionCutoverError) as error:
        _apply(fixture, adapter=adapter)

    assert error.value.code == "production_cutover_apply_failed_rolled_back"
    failure = json.loads((fixture.journal / "failure.json").read_text())
    assert failure["code"] == "production_cutover_payload_drift"


def test_new_recovery_authority_restores_after_forward_authorization_expires(
    fixture,
) -> None:
    adapter = FakeAdapter(crash_step="install_runtime")
    with pytest.raises(cutover.CutoverCrash):
        _apply(fixture, adapter=adapter)
    forward_steps = list(adapter.executed)
    adapter.crash_step = None

    forward = json.loads(fixture.paths["cutover_authorization_receipt"].read_text())
    forward["created_at"] = (NOW - timedelta(hours=2)).isoformat()
    forward["expires_at"] = (NOW - timedelta(hours=1)).isoformat()
    _write_json(fixture.paths["cutover_authorization_receipt"], forward)
    with pytest.raises(cutover.ProductionCutoverError) as expired:
        _apply(fixture, adapter=adapter)
    assert expired.value.code == "production_cutover_authorization_expired"

    recovery_lease = _recovery_lease()
    recovery_authorization = _write_recovery_authorization(fixture, recovery_lease)
    result = _recover(
        fixture,
        adapter,
        lease=recovery_lease,
        authorization=recovery_authorization,
    )

    assert result.phase == "recover"
    assert result.body["forward_steps_executed"] is False
    assert adapter.executed == forward_steps
    assert adapter.state == INITIAL_LIVE
    recovery_intent = json.loads(
        next((fixture.journal / "recovery").glob("*.intent.json")).read_text()
    )
    assert recovery_intent["recovery_lease"]["fingerprint"] == (
        RECOVERY_LEASE_FINGERPRINT
    )
    assert (
        recovery_intent["recovery_lease"]["token_sha256"]
        == hashlib.sha256(RECOVERY_LEASE_TOKEN.encode()).hexdigest()
    )
    assert recovery_intent["recovery_lease"]["holder"]["pid"] == 50002
    assert recovery_intent["forward_steps_forbidden"] is True
    rollback_count = adapter.rollback_count
    resumed = _recover(
        fixture,
        adapter,
        lease=recovery_lease,
        authorization=recovery_authorization,
    )
    assert resumed.resumed is True
    assert adapter.rollback_count == rollback_count


def test_recovery_rolls_back_final_done_before_complete_after_forward_auth_expires(
    fixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    adapter = FakeAdapter()
    original = cutover._publish_no_clobber
    crashed = False

    def crash_after_final_done(path, body):
        nonlocal crashed
        result = original(path, body)
        if path.name == "09-verify_gateway_aux.done.json" and not crashed:
            crashed = True
            raise cutover.CutoverCrash("after-final-done-before-complete")
        return result

    monkeypatch.setattr(cutover, "_publish_no_clobber", crash_after_final_done)
    with pytest.raises(cutover.CutoverCrash):
        _apply(fixture, adapter=adapter)
    assert adapter.state == TARGET_LIVE
    assert not (fixture.journal / "complete.json").exists()
    forward_steps = list(adapter.executed)

    forward = json.loads(fixture.paths["cutover_authorization_receipt"].read_text())
    forward["created_at"] = (NOW - timedelta(hours=2)).isoformat()
    forward["expires_at"] = (NOW - timedelta(hours=1)).isoformat()
    _write_json(fixture.paths["cutover_authorization_receipt"], forward)
    with pytest.raises(cutover.ProductionCutoverError) as expired:
        _apply(fixture, adapter=adapter)
    assert expired.value.code == "production_cutover_authorization_expired"

    result = _recover(fixture, adapter)

    assert result.phase == "recover"
    assert result.body["forward_steps_executed"] is False
    assert adapter.executed == forward_steps
    assert adapter.rollback_count == 1
    assert adapter.state == INITIAL_LIVE


def test_recovery_rejects_reused_forward_lease_identity(fixture) -> None:
    adapter = FakeAdapter(crash_step="install_runtime")
    with pytest.raises(cutover.CutoverCrash):
        _apply(fixture, adapter=adapter)
    adapter.crash_step = None
    reused = FakeLease()
    authorization = _write_recovery_authorization(fixture, reused)

    with pytest.raises(cutover.ProductionCutoverError) as error:
        _recover(
            fixture,
            adapter,
            lease=reused,
            authorization=authorization,
        )

    assert error.value.code == "production_recovery_lease_not_new"
    assert adapter.rollback_count == 0


def test_recovery_authorization_is_short_lived_and_exactly_bound(fixture) -> None:
    adapter = FakeAdapter(crash_step="install_runtime")
    with pytest.raises(cutover.CutoverCrash):
        _apply(fixture, adapter=adapter)
    adapter.crash_step = None
    lease = _recovery_lease()
    expired = _write_recovery_authorization(
        fixture,
        lease,
        created_at=NOW - timedelta(hours=1),
        expires_at=NOW - timedelta(minutes=1),
    )

    with pytest.raises(cutover.ProductionCutoverError) as error:
        _recover(fixture, adapter, lease=lease, authorization=expired)

    assert error.value.code == "production_recovery_authorization_expired"
    assert adapter.rollback_count == 0


def test_recovery_authorization_rejects_snapshot_rebinding(fixture) -> None:
    adapter = FakeAdapter(crash_step="install_runtime")
    with pytest.raises(cutover.CutoverCrash):
        _apply(fixture, adapter=adapter)
    adapter.crash_step = None
    lease = _recovery_lease()
    authorization = _write_recovery_authorization(fixture, lease)
    body = json.loads(authorization.read_text())
    body["bindings"]["snapshot_sha256"] = "f" * 64
    _write_json(authorization, body)

    with pytest.raises(cutover.ProductionCutoverError) as error:
        _recover(fixture, adapter, lease=lease, authorization=authorization)

    assert error.value.code == "production_recovery_authorization_binding_mismatch"
    assert adapter.rollback_count == 0


@pytest.mark.parametrize("phase", ["before_effect", "after_effect"])
def test_rollback_crash_resumes_to_exact_snapshot(fixture, phase: str) -> None:
    adapter = FakeAdapter(
        fail_step="install_runtime",
        rollback_crash_phase=phase,
    )
    with pytest.raises(cutover.CutoverCrash):
        _apply(fixture, adapter=adapter)

    adapter.rollback_crash_phase = None
    adapter.fail_step = None
    with pytest.raises(cutover.ProductionCutoverError) as recovered:
        _apply(fixture, adapter=adapter)

    assert recovered.value.code == "production_cutover_recovery_authorization_required"
    result = _recover(fixture, adapter)
    assert result.phase == "recover"
    assert adapter.state == INITIAL_LIVE
    assert (fixture.journal / "rollback.done.json").is_file()


def test_crash_after_rollback_intent_is_recoverable(
    fixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    adapter = FakeAdapter(fail_step="install_runtime")
    original = cutover._publish_no_clobber
    crashed = False

    def crash_after_intent(path, body):
        nonlocal crashed
        result = original(path, body)
        if path.name == "rollback.intent.json" and not crashed:
            crashed = True
            raise cutover.CutoverCrash("after-rollback-intent")
        return result

    monkeypatch.setattr(cutover, "_publish_no_clobber", crash_after_intent)
    with pytest.raises(cutover.CutoverCrash):
        _apply(fixture, adapter=adapter)
    adapter.fail_step = None

    with pytest.raises(cutover.ProductionCutoverError) as recovered:
        _apply(fixture, adapter=adapter)

    assert recovered.value.code == "production_cutover_recovery_authorization_required"
    result = _recover(fixture, adapter)
    assert result.phase == "recover"
    assert adapter.state == INITIAL_LIVE


def test_crash_before_rollback_intent_is_recoverable(
    fixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    adapter = FakeAdapter(fail_step="install_runtime")
    original = cutover._publish_no_clobber
    crashed = False

    def crash_before_intent(path, body):
        nonlocal crashed
        if path.name == "rollback.intent.json" and not crashed:
            crashed = True
            raise cutover.CutoverCrash("before-rollback-intent")
        return original(path, body)

    monkeypatch.setattr(cutover, "_publish_no_clobber", crash_before_intent)
    with pytest.raises(cutover.CutoverCrash):
        _apply(fixture, adapter=adapter)
    assert not (fixture.journal / "rollback.intent.json").exists()
    adapter.fail_step = None

    with pytest.raises(cutover.ProductionCutoverError) as recovered:
        _apply(fixture, adapter=adapter)

    assert recovered.value.code == "production_cutover_recovery_authorization_required"
    result = _recover(fixture, adapter)
    assert result.phase == "recover"
    assert adapter.state == INITIAL_LIVE


def test_crash_after_rollback_done_is_reconciled_without_reexecution(
    fixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    adapter = FakeAdapter(fail_step="install_runtime")
    original = cutover._publish_no_clobber
    crashed = False

    def crash_after_done(path, body):
        nonlocal crashed
        result = original(path, body)
        if path.name == "rollback.done.json" and not crashed:
            crashed = True
            raise cutover.CutoverCrash("after-rollback-done")
        return result

    monkeypatch.setattr(cutover, "_publish_no_clobber", crash_after_done)
    with pytest.raises(cutover.CutoverCrash):
        _apply(fixture, adapter=adapter)
    rollback_count = adapter.rollback_count
    adapter.fail_step = None

    with pytest.raises(cutover.ProductionCutoverError) as recovered:
        _apply(fixture, adapter=adapter)

    assert recovered.value.code == "production_cutover_recovery_authorization_required"
    result = _recover(fixture, adapter)
    assert result.phase == "recover"
    assert adapter.rollback_count == rollback_count
    assert (fixture.journal / "rollback.json").is_file()


def test_cli_has_no_apply_mode() -> None:
    action = next(item for item in cutover._parser()._actions if item.dest == "phase")
    assert tuple(action.choices) == ("plan", "validate")


def test_guard_lease_interface_exposes_recovery_identity_material() -> None:
    fields = cutover_guard.CutoverLease.__dataclass_fields__
    assert {"body", "fingerprint", "token", "holder_observer", "clock"} <= set(fields)
