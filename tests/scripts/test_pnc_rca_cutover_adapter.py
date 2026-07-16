from __future__ import annotations

import hashlib
import json
import os
import stat
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import pnc_rca_cutover_adapter as adapter
from scripts import pnc_rca_production_cutover as cutover


LEASE_FINGERPRINT = "1" * 64
LEASE_TOKEN = "fixture-cutover-lease-token-0001"
RECOVERY_LEASE_FINGERPRINT = "a" * 64
RECOVERY_LEASE_TOKEN = "fixture-recovery-lease-token-0001"
AUTHORIZATION_SHA256 = "8" * 64
MACHINE_IDENTITY_SHA256 = "9" * 64
RELEASE_ID = "rca-adapter-fixture-0001"


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _write(path: Path, raw: bytes, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.write_bytes(raw)
    path.chmod(mode)


def _stat_fields(path: Path) -> dict[str, int]:
    info = path.lstat()
    return {
        field: int(getattr(info, field))
        for field in (
            "st_dev",
            "st_ino",
            "st_mode",
            "st_uid",
            "st_nlink",
            "st_size",
            "st_mtime_ns",
            "st_ctime_ns",
        )
    }


def _validated_authorization(plan: dict) -> dict:
    return {
        "release_id": plan["release_id"],
        "receipt_sha256": AUTHORIZATION_SHA256,
        "bindings": {"cutover_lease_fingerprint": LEASE_FINGERPRINT},
        "expires_at": "2099-01-01T00:00:00+00:00",
        "nonce": "fixture-adapter-authorization-0001",
        "machine_identity_sha256": MACHINE_IDENTITY_SHA256,
    }


def _lease_identity(fingerprint: str, token: str, pid: int) -> dict:
    holder = {
        "pid": pid,
        "process_create_time": float(pid),
        "boot_id": f"fixture-boot-{pid}",
        "machine_identity": {"fixture": "host"},
    }
    return {
        "fingerprint": fingerprint,
        "token_sha256": hashlib.sha256(token.encode()).hexdigest(),
        "holder": holder,
        "holder_sha256": cutover._sha256_json(holder),
    }


def _recovery_context(plan: dict, snapshot: dict, journal_root: Path) -> dict:
    forward_lease = _lease_identity(LEASE_FINGERPRINT, LEASE_TOKEN, 40001)
    recovery_lease = _lease_identity(
        RECOVERY_LEASE_FINGERPRINT, RECOVERY_LEASE_TOKEN, 50002
    )
    run_identity = cutover._forward_run_identity(
        plan, lease_identity=forward_lease
    )
    run_raw_sha256 = hashlib.sha256(cutover._canonical_json(run_identity)).hexdigest()
    return {
        "forward_lease": forward_lease,
        "recovery_lease": recovery_lease,
        "run_identity": run_identity,
        "run_raw_sha256": run_raw_sha256,
        "snapshot": snapshot,
        "journal_root": journal_root,
    }


def _validated_recovery_authorization(plan: dict, context: dict) -> dict:
    forward_lease = context["forward_lease"]
    recovery_lease = context["recovery_lease"]
    return {
        "release_id": plan["release_id"],
        "receipt_sha256": "b" * 64,
        "nonce": "fixture-recovery-authorization-0001",
        "expires_at": "2099-01-01T00:00:00+00:00",
        "bindings": {
            "original_plan_sha256": cutover._sha256_json(plan),
            "journal_root": str(context["journal_root"].resolve()),
            "run_identity_sha256": context["run_raw_sha256"],
            "snapshot_sha256": cutover._sha256_json(context["snapshot"]),
            "rollback_target_identity_sha256": plan["bindings"][
                "rollback_live_identity_sha256"
            ],
            "forward_lease_fingerprint": forward_lease["fingerprint"],
            "forward_lease_token_sha256": forward_lease["token_sha256"],
            "forward_holder_sha256": forward_lease["holder_sha256"],
            "recovery_lease_fingerprint": recovery_lease["fingerprint"],
            "recovery_lease_token_sha256": recovery_lease["token_sha256"],
            "recovery_holder_sha256": recovery_lease["holder_sha256"],
            "recovery_pid": recovery_lease["holder"]["pid"],
            "machine_identity_sha256": MACHINE_IDENTITY_SHA256,
        },
        "machine_identity_sha256": MACHINE_IDENTITY_SHA256,
    }


class FakeRunner:
    def __init__(self, state: dict):
        self.calls: list[tuple[str, ...]] = []
        self.state = state

    def run(self, argv):
        normalized = tuple(argv)
        self.calls.append(normalized)
        self.state["runner_generation"] += 1
        return adapter.CommandResult(normalized, 0)


class FakeServices:
    def __init__(self, state: dict, evidence_root: Path):
        self.state = state
        self.evidence_root = evidence_root
        self.restored: dict | None = None
        self.waited_until_unloaded: list[str] = []

    def capture_state(self, labels):
        return {"labels": list(labels), "generation": self.state["services_generation"]}

    def stop_writers(self, labels, *, lease_fingerprint, lease_token):
        assert lease_fingerprint == LEASE_FINGERPRINT
        assert lease_token == LEASE_TOKEN
        self.state["services_generation"] += 1
        receipt = self.evidence_root / "writer-stop.json"
        _write(receipt, b'{"stopped":true}\n')
        return {
            "schema_version": "pnc_rca_writer_stop_evidence_v1",
            "writer_labels": list(cutover.WRITER_LABELS),
            "runtime_quiesce_labels": list(labels),
            "receipt_sha256": _sha(receipt.read_bytes()),
            "receipt_path": str(receipt),
        }

    def verify(self, labels, *, runtime_sha256):
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
                    else 1000.0 + index
                ),
                "runtime_sha256": runtime_sha256,
                "health_ok": True,
            }
            for index, label in enumerate(labels, 1)
        }

    def wait_until_unloaded(self, label):
        self.waited_until_unloaded.append(label)

    def restore_state(self, state):
        self.restored = dict(state)
        self.state["services_generation"] = int(state["generation"])


class FixtureObserver:
    def __init__(self, root: Path, state: dict, logical_paths: list[str]):
        self.root = root
        self.state = state
        self.logical_paths = logical_paths

    def __call__(self):
        files = {}
        for logical in self.logical_paths:
            physical = self.root.joinpath(*Path(logical).parts[1:])
            if physical.is_file() and not physical.is_symlink():
                files[logical] = _sha(physical.read_bytes())
            elif physical.exists() or physical.is_symlink():
                files[logical] = "non-file"
            else:
                files[logical] = "absent"
        return {
            "schema_version": "fake_cutover_live_identity_v1",
            "files": files,
            "state": dict(self.state),
        }


def _descriptor(logical: str, physical: Path) -> dict:
    raw = physical.read_bytes()
    return {
        "schema_version": cutover.PAYLOAD_DESCRIPTOR_SCHEMA_VERSION,
        "kind": "regular_file",
        "path": logical,
        "binding_sha256": _sha(raw),
        "physical_sha256": _sha(raw),
        "size_bytes": len(raw),
        "identity": _stat_fields(physical),
    }


@pytest.fixture
def candidate(tmp_path: Path) -> SimpleNamespace:
    fake = tmp_path / "fake-root"
    fake.mkdir(mode=0o700)
    logical = {
        "source_env": "/candidate/environment.env",
        "source_binding": "/candidate/active-release-binding.json",
        "source_sidecar": "/candidate/feishu-sidecar.json",
        "runtime_stage": "/candidate/runtime-stage",
        "plist_source_root": "/candidate/install-plists",
        "workspace_stage": "/candidate/workspace-stage",
        "active_binding": "/Users/songying/.hermes/runtime/pnc-rca/active-release-binding.json",
        "sidecar": "/Users/songying/.hermes/runtime/pnc-rca/feishu-sidecar.json",
    }

    def physical(value: str) -> Path:
        return fake.joinpath(*Path(value).parts[1:])

    for destination in (
        str(cutover.CANONICAL_ENV_PATH),
        logical["active_binding"],
        logical["sidecar"],
        str(cutover.CANONICAL_RUNTIME_ROOT),
        str(cutover.CANONICAL_WORKSPACE_ROOT),
        str(cutover.CANONICAL_LAUNCH_AGENTS_ROOT / "placeholder.plist"),
    ):
        physical(destination).parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    _write(physical(logical["source_env"]), b"MODE=bootstrap\n")
    _write(physical(logical["source_binding"]), b'{"active":true}\n')
    _write(physical(logical["source_sidecar"]), b'{"hold":true}\n')
    physical(logical["runtime_stage"]).mkdir(parents=True, mode=0o700)
    physical(logical["plist_source_root"]).mkdir(parents=True, mode=0o700)
    physical(logical["workspace_stage"]).mkdir(parents=True, mode=0o700)
    state = {
        "services_generation": 0,
        "runner_generation": 0,
        "activation": "preauthorized",
    }
    observed_paths = [
        str(cutover.CANONICAL_ENV_PATH),
        logical["active_binding"],
        logical["sidecar"],
    ]
    observer = FixtureObserver(fake, state, observed_paths)
    initial_sha = cutover._sha256_json(observer())
    env_sha = _sha(physical(logical["source_env"]).read_bytes())
    binding_sha = _sha(physical(logical["source_binding"]).read_bytes())
    sidecar_sha = _sha(physical(logical["source_sidecar"]).read_bytes())
    candidate_plist_sha256 = {}
    for index, name in enumerate(cutover.CANDIDATE_PLISTS, 1):
        source = physical(f"{logical['plist_source_root']}/{name}")
        _write(source, f"canonical-plist-{index}\n".encode(), 0o644)
        candidate_plist_sha256[name] = _sha(source.read_bytes())
    plan = {
        "schema_version": cutover.PLAN_SCHEMA_VERSION,
        "release_id": RELEASE_ID,
        "authorization_machine_identity_sha256": MACHINE_IDENTITY_SHA256,
        "bindings": {
            "cutover_authorization_receipt_sha256": AUTHORIZATION_SHA256,
            "cutover_lease_fingerprint": LEASE_FINGERPRINT,
            "expected_live_identity_sha256": initial_sha,
            "rollback_live_identity_sha256": initial_sha,
            "target_live_identity_sha256": "2" * 64,
            "runtime_content_sha256": "3" * 64,
            "workspace_runtime_sha256": "4" * 64,
            "candidate_env_sha256": env_sha,
            "feishu_sidecar_sha256": sidecar_sha,
            "candidate_plist_set_sha256": "6" * 64,
            "activation_contract_sha256": "5" * 64,
        },
        "payload_bindings": {
            "candidate_environment": {
                "source_path": logical["source_env"],
                "canonical_path": str(cutover.CANONICAL_ENV_PATH),
                "sha256": env_sha,
            },
            "active_release_binding": {
                "source_path": logical["source_binding"],
                "canonical_path": logical["active_binding"],
                "sha256": binding_sha,
            },
            "feishu_sidecar": {
                "source_path": logical["source_sidecar"],
                "canonical_path": logical["sidecar"],
                "sha256": sidecar_sha,
            },
            "runtime": {
                "staging_root": logical["runtime_stage"],
                "candidate_plist_root": logical["plist_source_root"],
                "canonical_path": str(cutover.CANONICAL_RUNTIME_ROOT),
                "content_sha256": "3" * 64,
                "candidate_plist_sha256": candidate_plist_sha256,
            },
            "workspace": {
                "staging_root": logical["workspace_stage"],
                "canonical_path": str(cutover.CANONICAL_WORKSPACE_ROOT),
                "closure_sha256": "4" * 64,
            },
        },
        "gateway_aux_start_order": list(cutover.GATEWAY_AUX_LABELS),
        "resident_start_order": list(cutover.RESIDENT_LABELS),
    }
    payloads = {
        "candidate_environment": _descriptor(
            logical["source_env"], physical(logical["source_env"])
        ),
        "active_release_binding": _descriptor(
            logical["source_binding"], physical(logical["source_binding"])
        ),
    }
    services = FakeServices(state, physical("/evidence"))
    runner = FakeRunner(state)
    authority = adapter.AdapterMutationAuthority.bind(
        plan=plan,
        gate_binding=plan["bindings"],
        validated_authorization=_validated_authorization(plan),
        machine_identity_sha256=MACHINE_IDENTITY_SHA256,
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )

    def build(*, authorized=True, io_hook=None):
        return adapter.ProductionCutoverAdapter(
            projection=adapter.PathProjection.fake(fake),
            identity_observer=observer,
            snapshot_root=Path("/snapshots"),
            runner=runner,
            service_controller=services,
            authority=authority if authorized else None,
            io_hook=io_hook,
        )

    return SimpleNamespace(
        fake=fake,
        logical=logical,
        physical=physical,
        plan=plan,
        payloads=payloads,
        observer=observer,
        state=state,
        services=services,
        runner=runner,
        authority=authority,
        build=build,
    )


def test_preflight_is_read_only_but_execute_requires_explicit_authority(candidate):
    instance = candidate.build(authorized=False)
    before = cutover._sha256_json(candidate.observer())
    preflight = instance.preflight_step(
        "install_environment",
        expected_identity_sha256=before,
        plan=candidate.plan,
        payload_descriptors=candidate.payloads,
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )
    assert preflight["commands"] == cutover._expected_commands_for_step(
        "install_environment", candidate.plan
    )
    with pytest.raises(
        adapter.CutoverAdapterError,
        match="cutover_adapter_mutation_authority_required",
    ):
        instance.execute_step(
            "install_environment",
            expected_identity_sha256=before,
            plan=candidate.plan,
            planned_commands=preflight["commands"],
            payload_descriptors=candidate.payloads,
            lease_fingerprint=LEASE_FINGERPRINT,
            lease_token=LEASE_TOKEN,
        )
    assert not candidate.physical(str(cutover.CANONICAL_ENV_PATH)).exists()


def test_environment_pair_is_installed_atomically_and_matches_executor_contract(
    candidate,
):
    instance = candidate.build()
    before = cutover._sha256_json(candidate.observer())
    commands = cutover._expected_commands_for_step("install_environment", candidate.plan)
    result = instance.execute_step(
        "install_environment",
        expected_identity_sha256=before,
        plan=candidate.plan,
        planned_commands=commands,
        payload_descriptors=candidate.payloads,
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )
    assert candidate.physical(str(cutover.CANONICAL_ENV_PATH)).read_bytes() == (
        b"MODE=bootstrap\n"
    )
    assert candidate.physical(candidate.logical["active_binding"]).read_bytes() == (
        b'{"active":true}\n'
    )
    cutover._validate_step_result(
        result,
        step="install_environment",
        expected_before=before,
        plan=candidate.plan,
        planned_commands=commands,
    )


def test_install_plists_uses_canonical_sources_not_runtime_probe_projection(
    candidate,
):
    runtime = candidate.physical(candidate.logical["runtime_stage"])
    files = {}
    for index, name in enumerate(cutover.CANDIDATE_PLISTS, 1):
        staged = runtime / name
        _write(staged, f"stage-projection-{index}\n".encode())
        files[name] = {
            "sha256": _sha(staged.read_bytes()),
            "size_bytes": staged.stat().st_size,
            "identity": _stat_fields(staged),
        }
    manifest = runtime / adapter.RUNTIME_STAGE_MANIFEST_NAME
    _write(manifest, b"{}\n", 0o600)
    install_plists = {}
    for name in cutover.CANDIDATE_PLISTS:
        logical = f"{candidate.logical['plist_source_root']}/{name}"
        source = candidate.physical(logical)
        install_plists[logical] = {
            "sha256": _sha(source.read_bytes()),
            "size_bytes": source.stat().st_size,
            "mode": f"{stat.S_IMODE(source.stat().st_mode):04o}",
            "identity": _stat_fields(source),
        }
    descriptor = {
        "schema_version": cutover.PAYLOAD_DESCRIPTOR_SCHEMA_VERSION,
        "kind": "runtime_tree",
        "path": candidate.logical["runtime_stage"],
        "binding_sha256": candidate.plan["bindings"]["runtime_content_sha256"],
        "physical_sha256": adapter._sha256_json(files),
        "files": files,
        "root_identity": _stat_fields(runtime),
        "install_plists": install_plists,
    }
    instance = candidate.build()
    before = cutover._sha256_json(candidate.observer())
    commands = cutover._expected_commands_for_step("install_plists", candidate.plan)
    result = instance.execute_step(
        "install_plists",
        expected_identity_sha256=before,
        plan=candidate.plan,
        planned_commands=commands,
        payload_descriptors={"runtime": descriptor},
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )

    assert result["evidence"]["post_install_verified"] is True
    for name in cutover.CANDIDATE_PLISTS:
        installed = candidate.physical(
            str(
                cutover.CANONICAL_LAUNCH_AGENTS_ROOT
                / name.replace(".candidate.plist", ".plist")
            )
        )
        source = candidate.physical(
            f"{candidate.logical['plist_source_root']}/{name}"
        )
        assert installed.read_bytes() == source.read_bytes()
        assert installed.read_bytes() != (runtime / name).read_bytes()


def test_second_file_commit_failure_restores_both_old_files(candidate):
    live_env = candidate.physical(str(cutover.CANONICAL_ENV_PATH))
    live_binding = candidate.physical(candidate.logical["active_binding"])
    _write(live_env, b"OLD=1\n")
    _write(live_binding, b'{"active":false}\n')

    def fail_second(event: str, _path: Path):
        if event == "before_file_commit_1":
            raise RuntimeError("injected commit failure")

    instance = candidate.build(io_hook=fail_second)
    before = cutover._sha256_json(candidate.observer())
    with pytest.raises(RuntimeError, match="injected commit failure"):
        instance.execute_step(
            "install_environment",
            expected_identity_sha256=before,
            plan=candidate.plan,
            planned_commands=cutover._expected_commands_for_step(
                "install_environment", candidate.plan
            ),
            payload_descriptors=candidate.payloads,
            lease_fingerprint=LEASE_FINGERPRINT,
            lease_token=LEASE_TOKEN,
        )
    assert live_env.read_bytes() == b"OLD=1\n"
    assert live_binding.read_bytes() == b'{"active":false}\n'


@pytest.mark.parametrize("phase", ["after_file_displace_0", "after_file_install_0"])
def test_environment_transaction_resumes_after_hard_crash_prefix(candidate, phase):
    live_env = candidate.physical(str(cutover.CANONICAL_ENV_PATH))
    live_binding = candidate.physical(candidate.logical["active_binding"])
    _write(live_env, b"OLD=1\n")
    _write(live_binding, b'{"active":false}\n')
    before = cutover._sha256_json(candidate.observer())

    def crash(event: str, _path: Path):
        if event == phase:
            raise cutover.CutoverCrash(phase)

    with pytest.raises(cutover.CutoverCrash):
        candidate.build(io_hook=crash).execute_step(
            "install_environment",
            expected_identity_sha256=before,
            plan=candidate.plan,
            planned_commands=cutover._expected_commands_for_step(
                "install_environment", candidate.plan
            ),
            payload_descriptors=candidate.payloads,
            lease_fingerprint=LEASE_FINGERPRINT,
            lease_token=LEASE_TOKEN,
        )
    restarted = candidate.build()
    result = restarted.execute_step(
        "install_environment",
        expected_identity_sha256=cutover._sha256_json(candidate.observer()),
        plan=candidate.plan,
        planned_commands=cutover._expected_commands_for_step(
            "install_environment", candidate.plan
        ),
        payload_descriptors=candidate.payloads,
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )
    assert live_env.read_bytes() == b"MODE=bootstrap\n"
    assert live_binding.read_bytes() == b'{"active":true}\n'
    assert result["after_identity_sha256"] == cutover._sha256_json(candidate.observer())


def test_hard_crash_recovery_never_overwrites_unknown_retained_file(candidate):
    live_env = candidate.physical(str(cutover.CANONICAL_ENV_PATH))
    live_binding = candidate.physical(candidate.logical["active_binding"])
    _write(live_env, b"OLD=1\n")
    _write(live_binding, b'{"active":false}\n')

    def crash(event: str, _path: Path):
        if event == "after_file_displace_0":
            raise cutover.CutoverCrash(event)

    with pytest.raises(cutover.CutoverCrash):
        candidate.build(io_hook=crash).execute_step(
            "install_environment",
            expected_identity_sha256=cutover._sha256_json(candidate.observer()),
            plan=candidate.plan,
            planned_commands=cutover._expected_commands_for_step(
                "install_environment", candidate.plan
            ),
            payload_descriptors=candidate.payloads,
            lease_fingerprint=LEASE_FINGERPRINT,
            lease_token=LEASE_TOKEN,
        )
    retained = next(live_env.parent.glob(f".{live_env.name}.precutover.*"))
    retained.write_bytes(b"UNKNOWN=1\n")
    retained.chmod(0o600)
    with pytest.raises(
        adapter.CutoverAdapterError,
        match="cutover_adapter_transaction_state_unknown",
    ):
        candidate.build().execute_step(
            "install_environment",
            expected_identity_sha256=cutover._sha256_json(candidate.observer()),
            plan=candidate.plan,
            planned_commands=cutover._expected_commands_for_step(
                "install_environment", candidate.plan
            ),
            payload_descriptors=candidate.payloads,
            lease_fingerprint=LEASE_FINGERPRINT,
            lease_token=LEASE_TOKEN,
        )
    assert retained.read_bytes() == b"UNKNOWN=1\n"
    assert not live_env.exists()


def test_command_injection_is_rejected_before_any_mutation(candidate):
    instance = candidate.build()
    before = cutover._sha256_json(candidate.observer())
    commands = cutover._expected_commands_for_step("install_environment", candidate.plan)
    commands[0] = ["/usr/bin/env", "sh", "-c", "touch /tmp/forbidden"]
    with pytest.raises(
        adapter.CutoverAdapterError, match="cutover_adapter_command_not_allowlisted"
    ):
        instance.execute_step(
            "install_environment",
            expected_identity_sha256=before,
            plan=candidate.plan,
            planned_commands=commands,
            payload_descriptors=candidate.payloads,
            lease_fingerprint=LEASE_FINGERPRINT,
            lease_token=LEASE_TOKEN,
        )
    assert not candidate.physical(str(cutover.CANONICAL_ENV_PATH)).exists()


def test_hardlink_and_symlink_payloads_are_rejected(candidate):
    source = candidate.physical(candidate.logical["source_env"])
    hardlink = source.parent / "hardlink.env"
    os.link(source, hardlink)
    with pytest.raises(
        adapter.CutoverAdapterError, match="cutover_adapter_owner_file_identity_invalid"
    ):
        candidate.build().preflight_step(
            "install_environment",
            expected_identity_sha256=cutover._sha256_json(candidate.observer()),
            plan=candidate.plan,
            payload_descriptors=candidate.payloads,
            lease_fingerprint=LEASE_FINGERPRINT,
            lease_token=LEASE_TOKEN,
        )
    hardlink.unlink()
    replacement = source.parent / "replacement.env"
    _write(replacement, source.read_bytes())
    source.unlink()
    source.symlink_to(replacement)
    with pytest.raises(adapter.CutoverAdapterError):
        candidate.build().preflight_step(
            "install_environment",
            expected_identity_sha256=cutover._sha256_json(candidate.observer()),
            plan=candidate.plan,
            payload_descriptors=candidate.payloads,
            lease_fingerprint=LEASE_FINGERPRINT,
            lease_token=LEASE_TOKEN,
        )


def test_same_fd_read_detects_lexical_toctou_swap(candidate):
    source = candidate.physical(candidate.logical["source_env"])
    swapped = False

    def swap(event: str, path: Path):
        nonlocal swapped
        if event == "after_same_fd_read" and path == source and not swapped:
            swapped = True
            old = path.parent / "original.env"
            path.rename(old)
            _write(path, b"ATTACK=1\n")

    instance = candidate.build(io_hook=swap)
    with pytest.raises(
        adapter.CutoverAdapterError, match="cutover_adapter_owner_file_unstable"
    ):
        instance.preflight_step(
            "install_environment",
            expected_identity_sha256=cutover._sha256_json(candidate.observer()),
            plan=candidate.plan,
            payload_descriptors=candidate.payloads,
            lease_fingerprint=LEASE_FINGERPRINT,
            lease_token=LEASE_TOKEN,
        )
    assert not candidate.physical(str(cutover.CANONICAL_ENV_PATH)).exists()


def test_symlinked_destination_parent_is_rejected_without_partial_install(candidate):
    parent = candidate.physical(candidate.logical["active_binding"]).parent
    parent.rmdir()
    escape = candidate.physical("/escape")
    escape.mkdir(mode=0o700)
    parent.symlink_to(escape, target_is_directory=True)
    instance = candidate.build()
    with pytest.raises(
        adapter.CutoverAdapterError,
        match="cutover_adapter_directory_identity_invalid",
    ):
        instance.execute_step(
            "install_environment",
            expected_identity_sha256=cutover._sha256_json(candidate.observer()),
            plan=candidate.plan,
            planned_commands=cutover._expected_commands_for_step(
                "install_environment", candidate.plan
            ),
            payload_descriptors=candidate.payloads,
            lease_fingerprint=LEASE_FINGERPRINT,
            lease_token=LEASE_TOKEN,
        )
    live_env = candidate.physical(str(cutover.CANONICAL_ENV_PATH))
    assert not live_env.exists()
    assert not list(live_env.parent.glob(f".{live_env.name}.*.install"))
    assert not (escape / "active-release-binding.json").exists()


def test_snapshot_install_and_rollback_restore_exact_fake_live_state(candidate):
    live_env = candidate.physical(str(cutover.CANONICAL_ENV_PATH))
    live_binding = candidate.physical(candidate.logical["active_binding"])
    _write(live_env, b"OLD=1\n")
    _write(live_binding, b'{"active":false}\n')
    before = cutover._sha256_json(candidate.observer())
    candidate.plan["bindings"]["rollback_live_identity_sha256"] = before
    candidate.plan["bindings"]["expected_live_identity_sha256"] = before
    authority = adapter.AdapterMutationAuthority.bind(
        plan=candidate.plan,
        gate_binding=candidate.plan["bindings"],
        validated_authorization=_validated_authorization(candidate.plan),
        machine_identity_sha256=MACHINE_IDENTITY_SHA256,
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )
    instance = adapter.ProductionCutoverAdapter(
        projection=adapter.PathProjection.fake(candidate.fake),
        identity_observer=candidate.observer,
        snapshot_root=Path("/snapshots"),
        runner=candidate.runner,
        service_controller=candidate.services,
        authority=authority,
    )
    snapshot_result = instance.execute_step(
        "snapshot_live",
        expected_identity_sha256=before,
        plan=candidate.plan,
        planned_commands=[],
        payload_descriptors={},
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )
    snapshot = snapshot_result["snapshot"]
    assert snapshot_result["after_identity_sha256"] == before
    repeated_snapshot = instance.execute_step(
        "snapshot_live",
        expected_identity_sha256=before,
        plan=candidate.plan,
        planned_commands=[],
        payload_descriptors={},
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )
    assert repeated_snapshot["snapshot"] == snapshot
    install_result = instance.execute_step(
        "install_environment",
        expected_identity_sha256=before,
        plan=candidate.plan,
        planned_commands=cutover._expected_commands_for_step(
            "install_environment", candidate.plan
        ),
        payload_descriptors=candidate.payloads,
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )
    rollback_result = instance.rollback(
        snapshot=snapshot,
        expected_identity_sha256=install_result["after_identity_sha256"],
        plan=candidate.plan,
        planned_commands=cutover._expected_commands_for_step("rollback", candidate.plan),
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )
    assert live_env.read_bytes() == b"OLD=1\n"
    assert live_binding.read_bytes() == b'{"active":false}\n'
    assert rollback_result["after_identity_sha256"] == before


def test_rollback_transaction_resumes_after_hard_crash(candidate):
    live_env = candidate.physical(str(cutover.CANONICAL_ENV_PATH))
    live_binding = candidate.physical(candidate.logical["active_binding"])
    _write(live_env, b"OLD=1\n")
    _write(live_binding, b'{"active":false}\n')
    before = cutover._sha256_json(candidate.observer())
    candidate.plan["bindings"]["rollback_live_identity_sha256"] = before
    candidate.plan["bindings"]["expected_live_identity_sha256"] = before
    authority = adapter.AdapterMutationAuthority.bind(
        plan=candidate.plan,
        gate_binding=candidate.plan["bindings"],
        validated_authorization=_validated_authorization(candidate.plan),
        machine_identity_sha256=MACHINE_IDENTITY_SHA256,
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )

    def build(io_hook=None):
        return adapter.ProductionCutoverAdapter(
            projection=adapter.PathProjection.fake(candidate.fake),
            identity_observer=candidate.observer,
            snapshot_root=Path("/snapshots"),
            runner=candidate.runner,
            service_controller=candidate.services,
            authority=authority,
            io_hook=io_hook,
        )

    initial = build()
    snapshot = initial.execute_step(
        "snapshot_live",
        expected_identity_sha256=before,
        plan=candidate.plan,
        planned_commands=[],
        payload_descriptors={},
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )["snapshot"]
    installed = initial.execute_step(
        "install_environment",
        expected_identity_sha256=before,
        plan=candidate.plan,
        planned_commands=cutover._expected_commands_for_step(
            "install_environment", candidate.plan
        ),
        payload_descriptors=candidate.payloads,
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )

    def crash(event: str, _path: Path):
        if event == "after_file_displace_0":
            raise cutover.CutoverCrash("rollback-after-displace")

    with pytest.raises(cutover.CutoverCrash):
        build(io_hook=crash).rollback(
            snapshot=snapshot,
            expected_identity_sha256=installed["after_identity_sha256"],
            plan=candidate.plan,
            planned_commands=cutover._expected_commands_for_step("rollback", candidate.plan),
            lease_fingerprint=LEASE_FINGERPRINT,
            lease_token=LEASE_TOKEN,
        )
    result = build().rollback(
        snapshot=snapshot,
        expected_identity_sha256=cutover._sha256_json(candidate.observer()),
        plan=candidate.plan,
        planned_commands=cutover._expected_commands_for_step("rollback", candidate.plan),
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )
    assert live_env.read_bytes() == b"OLD=1\n"
    assert live_binding.read_bytes() == b'{"active":false}\n'
    assert result["after_identity_sha256"] == before


def test_recovery_authority_new_lease_is_rollback_only(candidate):
    live_env = candidate.physical(str(cutover.CANONICAL_ENV_PATH))
    live_binding = candidate.physical(candidate.logical["active_binding"])
    _write(live_env, b"OLD=1\n")
    _write(live_binding, b'{"active":false}\n')
    before = cutover._sha256_json(candidate.observer())
    candidate.plan["bindings"]["rollback_live_identity_sha256"] = before
    candidate.plan["bindings"]["expected_live_identity_sha256"] = before
    forward_authority = adapter.AdapterMutationAuthority.bind(
        plan=candidate.plan,
        gate_binding=candidate.plan["bindings"],
        validated_authorization=_validated_authorization(candidate.plan),
        machine_identity_sha256=MACHINE_IDENTITY_SHA256,
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )

    def build(authority):
        return adapter.ProductionCutoverAdapter(
            projection=adapter.PathProjection.fake(candidate.fake),
            identity_observer=candidate.observer,
            snapshot_root=Path("/snapshots"),
            runner=candidate.runner,
            service_controller=candidate.services,
            authority=authority,
        )

    forward = build(forward_authority)
    snapshot = forward.execute_step(
        "snapshot_live",
        expected_identity_sha256=before,
        plan=candidate.plan,
        planned_commands=[],
        payload_descriptors={},
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )["snapshot"]
    installed = forward.execute_step(
        "install_environment",
        expected_identity_sha256=before,
        plan=candidate.plan,
        planned_commands=cutover._expected_commands_for_step(
            "install_environment", candidate.plan
        ),
        payload_descriptors=candidate.payloads,
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )
    recovery_context = _recovery_context(
        candidate.plan, snapshot, candidate.fake / "executor-recovery-journal"
    )
    recovery_summary = _validated_recovery_authorization(
        candidate.plan, recovery_context
    )
    recovery_authority = adapter.AdapterMutationAuthority.bind_recovery(
        historical_plan=candidate.plan,
        historical_gate_binding=candidate.plan["bindings"],
        historical_run_identity=recovery_context["run_identity"],
        historical_run_identity_raw_sha256=recovery_context["run_raw_sha256"],
        historical_snapshot=snapshot,
        journal_root=recovery_context["journal_root"],
        recovery_lease_identity=recovery_context["recovery_lease"],
        validated_recovery_authorization=recovery_summary,
        recovery_authorization_raw_sha256=recovery_summary["receipt_sha256"],
        validated_recovery_authorization_summary_sha256=cutover._sha256_json(
            recovery_summary
        ),
        machine_identity_sha256=MACHINE_IDENTITY_SHA256,
        recovery_lease_token=RECOVERY_LEASE_TOKEN,
    )
    recovery = build(recovery_authority)
    with pytest.raises(
        adapter.CutoverAdapterError,
        match="cutover_adapter_mutation_authority_mismatch",
    ):
        recovery.preflight_step(
            "install_environment",
            expected_identity_sha256=installed["after_identity_sha256"],
            plan=candidate.plan,
            payload_descriptors=candidate.payloads,
            lease_fingerprint=RECOVERY_LEASE_FINGERPRINT,
            lease_token=RECOVERY_LEASE_TOKEN,
        )
    with pytest.raises(
        adapter.CutoverAdapterError,
        match="cutover_adapter_mutation_authority_mismatch",
    ):
        recovery.execute_step(
            "install_environment",
            expected_identity_sha256=installed["after_identity_sha256"],
            plan=candidate.plan,
            planned_commands=cutover._expected_commands_for_step(
                "install_environment", candidate.plan
            ),
            payload_descriptors=candidate.payloads,
            lease_fingerprint=RECOVERY_LEASE_FINGERPRINT,
            lease_token=RECOVERY_LEASE_TOKEN,
        )
    with pytest.raises(
        adapter.CutoverAdapterError,
        match="cutover_adapter_mutation_authority_mismatch",
    ):
        forward.preflight_step(
            "rollback",
            expected_identity_sha256=installed["after_identity_sha256"],
            plan=candidate.plan,
            payload_descriptors={},
            lease_fingerprint=RECOVERY_LEASE_FINGERPRINT,
            lease_token=RECOVERY_LEASE_TOKEN,
        )
    with pytest.raises(
        adapter.CutoverAdapterError,
        match="cutover_adapter_mutation_authority_mismatch",
    ):
        forward.rollback(
            snapshot=snapshot,
            expected_identity_sha256=installed["after_identity_sha256"],
            plan=candidate.plan,
            planned_commands=cutover._expected_commands_for_step(
                "rollback", candidate.plan
            ),
            lease_fingerprint=RECOVERY_LEASE_FINGERPRINT,
            lease_token=RECOVERY_LEASE_TOKEN,
        )
    commands = recovery.preflight_step(
        "rollback",
        expected_identity_sha256=installed["after_identity_sha256"],
        plan=candidate.plan,
        payload_descriptors={},
        lease_fingerprint=RECOVERY_LEASE_FINGERPRINT,
        lease_token=RECOVERY_LEASE_TOKEN,
    )["commands"]
    result = recovery.rollback(
        snapshot=snapshot,
        expected_identity_sha256=installed["after_identity_sha256"],
        plan=candidate.plan,
        planned_commands=commands,
        lease_fingerprint=RECOVERY_LEASE_FINGERPRINT,
        lease_token=RECOVERY_LEASE_TOKEN,
    )
    assert live_env.read_bytes() == b"OLD=1\n"
    assert live_binding.read_bytes() == b'{"active":false}\n'
    assert result["after_identity_sha256"] == before


def test_real_executor_recovery_contract_accepts_new_process_authority(candidate):
    live_env = candidate.physical(str(cutover.CANONICAL_ENV_PATH))
    live_binding = candidate.physical(candidate.logical["active_binding"])
    _write(live_env, b"OLD=1\n")
    _write(live_binding, b'{"active":false}\n')
    before = cutover._sha256_json(candidate.observer())
    candidate.plan["bindings"]["rollback_live_identity_sha256"] = before
    candidate.plan["bindings"]["expected_live_identity_sha256"] = before
    forward_authority = adapter.AdapterMutationAuthority.bind(
        plan=candidate.plan,
        gate_binding=candidate.plan["bindings"],
        validated_authorization=_validated_authorization(candidate.plan),
        machine_identity_sha256=MACHINE_IDENTITY_SHA256,
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )

    def build(authority):
        return adapter.ProductionCutoverAdapter(
            projection=adapter.PathProjection.fake(candidate.fake),
            identity_observer=candidate.observer,
            snapshot_root=Path("/snapshots"),
            runner=candidate.runner,
            service_controller=candidate.services,
            authority=authority,
        )

    forward = build(forward_authority)
    snapshot_result = forward.execute_step(
        "snapshot_live",
        expected_identity_sha256=before,
        plan=candidate.plan,
        planned_commands=[],
        payload_descriptors={},
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )
    installed = forward.execute_step(
        "install_environment",
        expected_identity_sha256=before,
        plan=candidate.plan,
        planned_commands=cutover._expected_commands_for_step(
            "install_environment", candidate.plan
        ),
        payload_descriptors=candidate.payloads,
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )
    assert installed["after_identity_sha256"] != before

    journal = candidate.fake / "executor-recovery-journal"
    steps = journal / "steps"
    steps.mkdir(parents=True, mode=0o700)
    journal.chmod(0o700)
    steps.chmod(0o700)
    context_values = _recovery_context(
        candidate.plan, snapshot_result["snapshot"], journal
    )
    _write(journal / "plan.json", cutover._canonical_json(candidate.plan))
    _write(
        journal / "run-identity.json",
        cutover._canonical_json(context_values["run_identity"]),
    )
    snapshot_done = {
        "schema_version": cutover.STEP_RESULT_SCHEMA_VERSION,
        "plan_sha256": cutover._sha256_json(candidate.plan),
        "index": 1,
        "step": "snapshot_live",
        "result": snapshot_result,
    }
    _write(
        steps / "01-snapshot_live.done.json",
        cutover._canonical_json(snapshot_done),
    )
    _write(
        journal / "failure.json",
        cutover._canonical_json({
            "schema_version": cutover.FAILURE_SCHEMA_VERSION,
            "ok": False,
            "plan_sha256": cutover._sha256_json(candidate.plan),
            "code": "fixture_hard_crash",
            "rollback_required": True,
        }),
    )

    now = datetime(2026, 7, 13, 8, 0, tzinfo=timezone.utc)

    class RecoveryLease:
        def __init__(self):
            self.fingerprint = RECOVERY_LEASE_FINGERPRINT
            self.token = RECOVERY_LEASE_TOKEN
            self.body = {"holder": context_values["recovery_lease"]["holder"]}
            self.active = False

        def __enter__(self):
            self.active = True
            return self

        def __exit__(self, *_args):
            self.active = False

        def assert_active(self):
            if not self.active:
                raise RuntimeError("recovery lease inactive")

    recovery_lease = RecoveryLease()
    executor_context = cutover._load_historical_recovery_context(journal)
    lease_identity = cutover._lease_execution_identity(recovery_lease)
    forward_identity = executor_context["run_identity"]["forward_lease"]
    recovery_bindings = {
        "original_plan_sha256": cutover._sha256_json(candidate.plan),
        "journal_root": str(journal.resolve()),
        "run_identity_sha256": executor_context["run_owned"].sha256,
        "snapshot_sha256": cutover._sha256_json(snapshot_result["snapshot"]),
        "rollback_target_identity_sha256": before,
        "forward_lease_fingerprint": forward_identity["fingerprint"],
        "forward_lease_token_sha256": forward_identity["token_sha256"],
        "forward_holder_sha256": forward_identity["holder_sha256"],
        "recovery_lease_fingerprint": lease_identity["fingerprint"],
        "recovery_lease_token_sha256": lease_identity["token_sha256"],
        "recovery_holder_sha256": lease_identity["holder_sha256"],
        "recovery_pid": lease_identity["holder"]["pid"],
        "machine_identity_sha256": MACHINE_IDENTITY_SHA256,
    }
    recovery_receipt = candidate.fake / "recovery-authorization.json"
    _write(
        recovery_receipt,
        cutover._canonical_json({
            "schema_version": cutover.RECOVERY_AUTHORIZATION_SCHEMA_VERSION,
            "release_id": candidate.plan["release_id"],
            "decision": cutover.RECOVERY_AUTHORIZATION_DECISION,
            "created_at": (now - timedelta(minutes=1)).isoformat(),
            "expires_at": (now + timedelta(minutes=10)).isoformat(),
            "nonce": "adapter-real-recovery-nonce-0001",
            "bindings": recovery_bindings,
            "identity": {
                "schema_version": cutover.AUTHORIZATION_IDENTITY_SCHEMA_VERSION,
                "uid": os.geteuid(),
                "username": "adapter-recovery-owner",
                "machine_identity_sha256": MACHINE_IDENTITY_SHA256,
            },
        }),
    )
    validated = cutover._validate_recovery_authorization(
        recovery_receipt,
        context=executor_context,
        lease_identity=lease_identity,
        machine_identity_sha256=MACHINE_IDENTITY_SHA256,
        now=now,
    )
    recovery_authority = adapter.AdapterMutationAuthority.bind_recovery(
        historical_plan=candidate.plan,
        historical_gate_binding=candidate.plan["bindings"],
        historical_run_identity=executor_context["run_identity"],
        historical_run_identity_raw_sha256=executor_context["run_owned"].sha256,
        historical_snapshot=executor_context["snapshot"],
        journal_root=journal,
        recovery_lease_identity=lease_identity,
        validated_recovery_authorization=validated,
        recovery_authorization_raw_sha256=hashlib.sha256(
            recovery_receipt.read_bytes()
        ).hexdigest(),
        validated_recovery_authorization_summary_sha256=cutover._sha256_json(
            validated
        ),
        machine_identity_sha256=MACHINE_IDENTITY_SHA256,
        recovery_lease_token=RECOVERY_LEASE_TOKEN,
    )
    result = cutover.recover_cutover(
        journal,
        recovery_authorization_receipt=recovery_receipt,
        lease=recovery_lease,
        adapter=build(recovery_authority),
        clock=lambda: now,
        machine_identity_provider=lambda: MACHINE_IDENTITY_SHA256,
        nonce_ledger_root=candidate.fake / "recovery-nonce-ledger",
    )
    assert result.phase == "recover"
    assert result.body["forward_steps_executed"] is False
    assert live_env.read_bytes() == b"OLD=1\n"
    assert live_binding.read_bytes() == b'{"active":false}\n'


@pytest.mark.parametrize("phase", ["before_snapshot_publish", "after_snapshot_publish"])
def test_snapshot_publish_is_restart_safe_at_atomic_boundary(candidate, phase):
    before = cutover._sha256_json(candidate.observer())

    def crash(event: str, _path: Path):
        if event == phase:
            raise cutover.CutoverCrash(phase)

    with pytest.raises(cutover.CutoverCrash):
        candidate.build(io_hook=crash).execute_step(
            "snapshot_live",
            expected_identity_sha256=before,
            plan=candidate.plan,
            planned_commands=[],
            payload_descriptors={},
            lease_fingerprint=LEASE_FINGERPRINT,
            lease_token=LEASE_TOKEN,
        )
    result = candidate.build().execute_step(
        "snapshot_live",
        expected_identity_sha256=before,
        plan=candidate.plan,
        planned_commands=[],
        payload_descriptors={},
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )
    final = Path(result["snapshot"]["components"]["runtime"]["restore_ref"]).parent
    assert (final / "snapshot-manifest.json").is_file()


def test_tree_snapshot_restores_root_and_empty_directory_modes(candidate):
    runtime = candidate.physical(str(cutover.CANONICAL_RUNTIME_ROOT))
    runtime.mkdir(mode=0o750)
    runtime.chmod(0o750)
    empty = runtime / "empty-retained"
    empty.mkdir(mode=0o710)
    empty.chmod(0o710)
    before = cutover._sha256_json(candidate.observer())
    candidate.plan["bindings"]["rollback_live_identity_sha256"] = before
    candidate.plan["bindings"]["expected_live_identity_sha256"] = before
    authority = adapter.AdapterMutationAuthority.bind(
        plan=candidate.plan,
        gate_binding=candidate.plan["bindings"],
        validated_authorization=_validated_authorization(candidate.plan),
        machine_identity_sha256=MACHINE_IDENTITY_SHA256,
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )
    instance = adapter.ProductionCutoverAdapter(
        projection=adapter.PathProjection.fake(candidate.fake),
        identity_observer=candidate.observer,
        snapshot_root=Path("/snapshots"),
        runner=candidate.runner,
        service_controller=candidate.services,
        authority=authority,
    )
    snapshot = instance.execute_step(
        "snapshot_live",
        expected_identity_sha256=before,
        plan=candidate.plan,
        planned_commands=[],
        payload_descriptors={},
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )["snapshot"]
    empty.rmdir()
    runtime.chmod(0o700)
    instance.rollback(
        snapshot=snapshot,
        expected_identity_sha256=before,
        plan=candidate.plan,
        planned_commands=cutover._expected_commands_for_step("rollback", candidate.plan),
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )
    assert stat.S_IMODE(runtime.stat().st_mode) == 0o750
    assert empty.is_dir()
    assert stat.S_IMODE(empty.stat().st_mode) == 0o710


def _tree_snapshot_with_legacy_symlinks(candidate):
    runtime = candidate.physical(str(cutover.CANONICAL_RUNTIME_ROOT))
    runtime.mkdir(mode=0o750)
    external = candidate.physical("/external-venv")
    external.mkdir(mode=0o700)
    marker = external / "must-not-be-read"
    marker.write_bytes(b"external payload")
    (runtime / ".venv").symlink_to(external, target_is_directory=True)
    bin_dir = runtime / "web/node_modules/.bin"
    bin_dir.mkdir(parents=True)
    (bin_dir / "tool").symlink_to("../tool/cli.js")
    before = cutover._sha256_json(candidate.observer())
    candidate.plan["bindings"]["rollback_live_identity_sha256"] = before
    candidate.plan["bindings"]["expected_live_identity_sha256"] = before
    authority = adapter.AdapterMutationAuthority.bind(
        plan=candidate.plan,
        gate_binding=candidate.plan["bindings"],
        validated_authorization=_validated_authorization(candidate.plan),
        machine_identity_sha256=MACHINE_IDENTITY_SHA256,
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )
    instance = adapter.ProductionCutoverAdapter(
        projection=adapter.PathProjection.fake(candidate.fake),
        identity_observer=candidate.observer,
        snapshot_root=Path("/snapshots"),
        runner=candidate.runner,
        service_controller=candidate.services,
        authority=authority,
    )
    snapshot = instance.execute_step(
        "snapshot_live",
        expected_identity_sha256=before,
        plan=candidate.plan,
        planned_commands=[],
        payload_descriptors={},
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )["snapshot"]
    return instance, snapshot, runtime, external, marker, before


def test_tree_snapshot_restores_legacy_symlinks_without_following(candidate):
    instance, snapshot, runtime, external, marker, before = (
        _tree_snapshot_with_legacy_symlinks(candidate)
    )
    (runtime / ".venv").unlink()
    (runtime / "web/node_modules/.bin/tool").unlink()

    instance.rollback(
        snapshot=snapshot,
        expected_identity_sha256=before,
        plan=candidate.plan,
        planned_commands=cutover._expected_commands_for_step(
            "rollback", candidate.plan
        ),
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )

    assert os.readlink(runtime / ".venv") == str(external)
    assert os.readlink(runtime / "web/node_modules/.bin/tool") == "../tool/cli.js"
    assert marker.read_bytes() == b"external payload"


def test_tree_snapshot_rejects_symlink_target_tamper_before_restore(candidate):
    instance, snapshot, runtime, _external, _marker, before = (
        _tree_snapshot_with_legacy_symlinks(candidate)
    )
    restore = Path(snapshot["components"]["runtime"]["restore_ref"])
    retained = restore / "payload/.venv"
    retained.unlink()
    retained.symlink_to("/tampered-target", target_is_directory=True)

    with pytest.raises(
        adapter.CutoverAdapterError,
        match="cutover_adapter_tree_symlink_drift",
    ):
        instance.rollback(
            snapshot=snapshot,
            expected_identity_sha256=before,
            plan=candidate.plan,
            planned_commands=cutover._expected_commands_for_step(
                "rollback", candidate.plan
            ),
            lease_fingerprint=LEASE_FINGERPRINT,
            lease_token=LEASE_TOKEN,
        )

    assert os.readlink(runtime / ".venv") != "/tampered-target"


def test_tree_snapshot_restores_contained_hardlink_topology(candidate):
    runtime = candidate.physical(str(cutover.CANONICAL_RUNTIME_ROOT))
    runtime.mkdir(mode=0o750)
    binary = runtime / "web/node_modules/esbuild/bin/esbuild"
    _write(binary, b"legacy binary", 0o755)
    alias = runtime / "web/node_modules/@esbuild/darwin-x64/bin/esbuild"
    alias.parent.mkdir(parents=True)
    os.link(binary, alias)
    before = cutover._sha256_json(candidate.observer())
    candidate.plan["bindings"]["rollback_live_identity_sha256"] = before
    candidate.plan["bindings"]["expected_live_identity_sha256"] = before
    authority = adapter.AdapterMutationAuthority.bind(
        plan=candidate.plan,
        gate_binding=candidate.plan["bindings"],
        validated_authorization=_validated_authorization(candidate.plan),
        machine_identity_sha256=MACHINE_IDENTITY_SHA256,
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )
    instance = adapter.ProductionCutoverAdapter(
        projection=adapter.PathProjection.fake(candidate.fake),
        identity_observer=candidate.observer,
        snapshot_root=Path("/snapshots"),
        runner=candidate.runner,
        service_controller=candidate.services,
        authority=authority,
    )
    snapshot = instance.execute_step(
        "snapshot_live",
        expected_identity_sha256=before,
        plan=candidate.plan,
        planned_commands=[],
        payload_descriptors={},
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )["snapshot"]
    alias.unlink()

    instance.rollback(
        snapshot=snapshot,
        expected_identity_sha256=before,
        plan=candidate.plan,
        planned_commands=cutover._expected_commands_for_step(
            "rollback", candidate.plan
        ),
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )

    assert binary.stat().st_ino == alias.stat().st_ino
    assert binary.stat().st_nlink == 2


def test_tree_snapshot_rejects_external_hardlink(candidate):
    runtime = candidate.physical(str(cutover.CANONICAL_RUNTIME_ROOT))
    runtime.mkdir(mode=0o750)
    binary = runtime / "bin/tool"
    _write(binary, b"legacy binary", 0o755)
    external = candidate.physical("/external-hardlink")
    os.link(binary, external)
    before = cutover._sha256_json(candidate.observer())

    with pytest.raises(
        adapter.CutoverAdapterError,
        match="cutover_adapter_tree_external_hardlink_forbidden",
    ):
        candidate.build().execute_step(
            "snapshot_live",
            expected_identity_sha256=before,
            plan=candidate.plan,
            planned_commands=[],
            payload_descriptors={},
            lease_fingerprint=LEASE_FINGERPRINT,
            lease_token=LEASE_TOKEN,
        )


def test_snapshot_file_mode_drift_is_rejected_before_restore(candidate):
    live_env = candidate.physical(str(cutover.CANONICAL_ENV_PATH))
    _write(live_env, b"OLD=1\n", 0o600)
    before = cutover._sha256_json(candidate.observer())
    candidate.plan["bindings"]["rollback_live_identity_sha256"] = before
    candidate.plan["bindings"]["expected_live_identity_sha256"] = before
    authority = adapter.AdapterMutationAuthority.bind(
        plan=candidate.plan,
        gate_binding=candidate.plan["bindings"],
        validated_authorization=_validated_authorization(candidate.plan),
        machine_identity_sha256=MACHINE_IDENTITY_SHA256,
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )
    instance = adapter.ProductionCutoverAdapter(
        projection=adapter.PathProjection.fake(candidate.fake),
        identity_observer=candidate.observer,
        snapshot_root=Path("/snapshots"),
        runner=candidate.runner,
        service_controller=candidate.services,
        authority=authority,
    )
    snapshot = instance.execute_step(
        "snapshot_live",
        expected_identity_sha256=before,
        plan=candidate.plan,
        planned_commands=[],
        payload_descriptors={},
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )["snapshot"]
    payload = Path(snapshot["components"]["environment"]["restore_ref"]) / "payload"
    payload.chmod(0o644)
    with pytest.raises(
        adapter.CutoverAdapterError,
        match="cutover_adapter_snapshot_payload_drift",
    ):
        instance.rollback(
            snapshot=snapshot,
            expected_identity_sha256=before,
            plan=candidate.plan,
            planned_commands=cutover._expected_commands_for_step("rollback", candidate.plan),
            lease_fingerprint=LEASE_FINGERPRINT,
            lease_token=LEASE_TOKEN,
        )
    assert live_env.read_bytes() == b"OLD=1\n"
    assert stat.S_IMODE(live_env.stat().st_mode) == 0o600


def test_existing_snapshot_is_never_overwritten_when_manifest_drifts(candidate):
    instance = candidate.build()
    before = cutover._sha256_json(candidate.observer())
    result = instance.execute_step(
        "snapshot_live",
        expected_identity_sha256=before,
        plan=candidate.plan,
        planned_commands=[],
        payload_descriptors={},
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )
    component = Path(result["snapshot"]["components"]["runtime"]["restore_ref"])
    manifest = component.parent / "snapshot-manifest.json"
    manifest.write_bytes(b'{"tampered":true}\n')
    manifest.chmod(0o600)
    with pytest.raises(
        adapter.CutoverAdapterError, match="cutover_adapter_snapshot_no_clobber"
    ):
        instance.execute_step(
            "snapshot_live",
            expected_identity_sha256=before,
            plan=candidate.plan,
            planned_commands=[],
            payload_descriptors={},
            lease_fingerprint=LEASE_FINGERPRINT,
            lease_token=LEASE_TOKEN,
        )


def test_runtime_manifest_is_bound_but_not_installed_into_live_tree(candidate):
    runtime_logical = candidate.logical["runtime_stage"]
    runtime = candidate.physical(runtime_logical)
    payload = runtime / "gateway" / "run.py"
    manifest = runtime / adapter.RUNTIME_STAGE_MANIFEST_NAME
    _write(payload, b"print('candidate')\n", 0o644)
    _write(manifest, b'{"schema_version":"fixture"}\n')
    payload_identity = _stat_fields(payload)
    physical = {
        "gateway/run.py": {
            "sha256": _sha(payload.read_bytes()),
            "size_bytes": payload.stat().st_size,
            "identity": payload_identity,
        }
    }
    descriptor = {
        "schema_version": cutover.PAYLOAD_DESCRIPTOR_SCHEMA_VERSION,
        "kind": "runtime_tree",
        "path": runtime_logical,
        "binding_sha256": candidate.plan["bindings"]["runtime_content_sha256"],
        "physical_sha256": adapter._sha256_json(physical),
        "files": physical,
        "root_identity": _stat_fields(runtime),
    }
    candidate.plan["bindings"]["runtime_stage_manifest_sha256"] = _sha(
        manifest.read_bytes()
    )
    authority = adapter.AdapterMutationAuthority.bind(
        plan=candidate.plan,
        gate_binding=candidate.plan["bindings"],
        validated_authorization=_validated_authorization(candidate.plan),
        machine_identity_sha256=MACHINE_IDENTITY_SHA256,
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )
    instance = adapter.ProductionCutoverAdapter(
        projection=adapter.PathProjection.fake(candidate.fake),
        identity_observer=candidate.observer,
        snapshot_root=Path("/snapshots"),
        runner=candidate.runner,
        service_controller=candidate.services,
        authority=authority,
    )
    before = cutover._sha256_json(candidate.observer())
    instance.execute_step(
        "install_runtime",
        expected_identity_sha256=before,
        plan=candidate.plan,
        planned_commands=cutover._expected_commands_for_step(
            "install_runtime", candidate.plan
        ),
        payload_descriptors={"runtime": descriptor},
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )
    live = candidate.physical(str(cutover.CANONICAL_RUNTIME_ROOT))
    assert (live / "gateway" / "run.py").read_bytes() == b"print('candidate')\n"
    assert not (live / adapter.RUNTIME_STAGE_MANIFEST_NAME).exists()


def test_launchctl_start_uses_exact_argv_without_shell(candidate):
    instance = candidate.build()
    before = cutover._sha256_json(candidate.observer())
    commands = cutover._expected_commands_for_step("start_gateway_aux", candidate.plan)
    result = instance.execute_step(
        "start_gateway_aux",
        expected_identity_sha256=before,
        plan=candidate.plan,
        planned_commands=commands,
        payload_descriptors={},
        lease_fingerprint=LEASE_FINGERPRINT,
        lease_token=LEASE_TOKEN,
    )
    assert candidate.runner.calls == [tuple(command) for command in commands]
    assert all(call[0] == "/bin/launchctl" for call in candidate.runner.calls)
    assert candidate.services.waited_until_unloaded == []
    cutover._validate_step_result(
        result,
        step="start_gateway_aux",
        expected_before=before,
        plan=candidate.plan,
        planned_commands=commands,
    )


def test_subprocess_runner_never_uses_a_shell(monkeypatch):
    observed = {}

    def run(argv, **kwargs):
        observed["argv"] = argv
        observed.update(kwargs)
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr(adapter.subprocess, "run", run)
    command = ["/bin/launchctl", "kickstart", "-k", "gui/501/ai.hermes.gateway"]
    result = adapter.SubprocessArgvRunner().run(command)
    assert observed["argv"] == command
    assert observed["shell"] is False
    assert result.argv == tuple(command)


def test_cli_has_no_mutation_subcommand(candidate):
    with pytest.raises(SystemExit):
        adapter.main(["install-owner-file", "/a", "/b", "0" * 64])


def test_production_projection_requires_separate_explicit_authority(candidate):
    with pytest.raises(
        adapter.CutoverAdapterError,
        match="cutover_adapter_production_projection_not_explicit",
    ):
        adapter.PathProjection.production()
    production_projection = adapter.PathProjection.production(explicit=True)
    with pytest.raises(
        adapter.CutoverAdapterError,
        match="cutover_adapter_production_authority_required",
    ):
        adapter.ProductionCutoverAdapter(
            projection=production_projection,
            identity_observer=candidate.observer,
            snapshot_root=Path("/Users/songying/.hermes/runtime/rca-snapshots"),
        )


def test_mutation_authority_rejects_unvalidated_receipt_or_machine(candidate):
    wrong_receipt = _validated_authorization(candidate.plan)
    wrong_receipt["receipt_sha256"] = "f" * 64
    with pytest.raises(
        adapter.CutoverAdapterError,
        match="cutover_adapter_gate_plan_binding_mismatch",
    ):
        adapter.AdapterMutationAuthority.bind(
            plan=candidate.plan,
            gate_binding=candidate.plan["bindings"],
            validated_authorization=wrong_receipt,
            machine_identity_sha256=MACHINE_IDENTITY_SHA256,
            lease_fingerprint=LEASE_FINGERPRINT,
            lease_token=LEASE_TOKEN,
        )


@pytest.mark.parametrize(
    "field",
    [
        "original_plan_sha256",
        "journal_root",
        "run_identity_sha256",
        "snapshot_sha256",
        "rollback_target_identity_sha256",
        "forward_lease_fingerprint",
        "forward_lease_token_sha256",
        "forward_holder_sha256",
        "recovery_lease_fingerprint",
        "recovery_lease_token_sha256",
        "recovery_holder_sha256",
        "recovery_pid",
        "machine_identity_sha256",
    ],
)
def test_recovery_authority_rejects_every_binding_tamper(candidate, field):
    snapshot = {
        "schema_version": cutover.SNAPSHOT_SCHEMA_VERSION,
        "rollback_target_identity_sha256": candidate.plan["bindings"][
            "rollback_live_identity_sha256"
        ],
    }
    context = _recovery_context(
        candidate.plan, snapshot, candidate.fake / "recovery-journal"
    )
    summary = _validated_recovery_authorization(candidate.plan, context)
    expected_summary_sha256 = cutover._sha256_json(summary)
    tampered = json.loads(json.dumps(summary))
    if field == "journal_root":
        tampered["bindings"][field] = "/different/recovery-journal"
    elif field == "recovery_pid":
        tampered["bindings"][field] += 1
    else:
        tampered["bindings"][field] = (
            "0" * 64 if tampered["bindings"][field] != "0" * 64 else "1" * 64
        )
    with pytest.raises(
        adapter.CutoverAdapterError,
        match="cutover_adapter_recovery_authority_binding_mismatch",
    ):
        adapter.AdapterMutationAuthority.bind_recovery(
            historical_plan=candidate.plan,
            historical_gate_binding=candidate.plan["bindings"],
            historical_run_identity=context["run_identity"],
            historical_run_identity_raw_sha256=context["run_raw_sha256"],
            historical_snapshot=snapshot,
            journal_root=context["journal_root"],
            recovery_lease_identity=context["recovery_lease"],
            validated_recovery_authorization=tampered,
            recovery_authorization_raw_sha256=summary["receipt_sha256"],
            validated_recovery_authorization_summary_sha256=(
                expected_summary_sha256
            ),
            machine_identity_sha256=MACHINE_IDENTITY_SHA256,
            recovery_lease_token=RECOVERY_LEASE_TOKEN,
        )


@pytest.mark.parametrize("tamper", ["raw_receipt", "validated_summary"])
def test_recovery_authority_rejects_receipt_or_summary_rebinding(candidate, tamper):
    snapshot = {
        "schema_version": cutover.SNAPSHOT_SCHEMA_VERSION,
        "rollback_target_identity_sha256": candidate.plan["bindings"][
            "rollback_live_identity_sha256"
        ],
    }
    context = _recovery_context(
        candidate.plan, snapshot, candidate.fake / "recovery-journal"
    )
    summary = _validated_recovery_authorization(candidate.plan, context)
    raw_sha256 = summary["receipt_sha256"]
    summary_sha256 = cutover._sha256_json(summary)
    if tamper == "raw_receipt":
        raw_sha256 = "0" * 64
    else:
        summary_sha256 = "0" * 64
    with pytest.raises(
        adapter.CutoverAdapterError,
        match="cutover_adapter_recovery_authority_binding_mismatch",
    ):
        adapter.AdapterMutationAuthority.bind_recovery(
            historical_plan=candidate.plan,
            historical_gate_binding=candidate.plan["bindings"],
            historical_run_identity=context["run_identity"],
            historical_run_identity_raw_sha256=context["run_raw_sha256"],
            historical_snapshot=snapshot,
            journal_root=context["journal_root"],
            recovery_lease_identity=context["recovery_lease"],
            validated_recovery_authorization=summary,
            recovery_authorization_raw_sha256=raw_sha256,
            validated_recovery_authorization_summary_sha256=summary_sha256,
            machine_identity_sha256=MACHINE_IDENTITY_SHA256,
            recovery_lease_token=RECOVERY_LEASE_TOKEN,
        )
    wrong_machine = _validated_authorization(candidate.plan)
    wrong_machine["machine_identity_sha256"] = "e" * 64
    with pytest.raises(
        adapter.CutoverAdapterError,
        match="cutover_adapter_gate_plan_binding_mismatch",
    ):
        adapter.AdapterMutationAuthority.bind(
            plan=candidate.plan,
            gate_binding=candidate.plan["bindings"],
            validated_authorization=wrong_machine,
            machine_identity_sha256=MACHINE_IDENTITY_SHA256,
            lease_fingerprint=LEASE_FINGERPRINT,
            lease_token=LEASE_TOKEN,
        )


def test_production_factory_has_no_ambient_defaults(candidate):
    with pytest.raises(
        adapter.CutoverAdapterError,
        match="cutover_adapter_production_dependencies_unavailable",
    ):
        adapter.build_production_adapter(authority=candidate.authority)
