from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import plistlib

import pytest

from scripts import pnc_rca_cutover_adapter as adapter
from scripts import pnc_rca_cutover_live as live
from scripts import pnc_rca_production_cutover as cutover


def _write(path: Path, raw: bytes, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.write_bytes(raw)
    path.chmod(mode)


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _descriptor(raw: bytes, mode: int) -> dict:
    return {
        "sha256": _sha(raw),
        "size_bytes": len(raw),
        "mode": f"{mode:04o}",
    }


@pytest.fixture
def projected(tmp_path: Path):
    fake = tmp_path / "physical"

    def physical(logical: Path) -> Path:
        return fake.joinpath(*logical.parts[1:])

    runtime_files = {
        "gateway/run.py": _descriptor(b"candidate-gateway\n", 0o644),
        ".venv/bin/python": _descriptor(b"candidate-python\n", 0o755),
    }
    workspace_files = {
        "prompt.md": _descriptor(b"candidate-workspace\n", 0o600),
        "workspace-runtime-manifest.json": _descriptor(b"{}\n", 0o600),
    }
    candidate_plists = {
        name: _sha(f"plist:{name}\n".encode()) for name in cutover.CANDIDATE_PLISTS
    }
    env_raw = b"HERMES_RCA_ACTIVATION_REQUIRED=true\n"
    binding_raw = b'{"release":"bound"}\n'
    sidecar_raw = b'{"revision":1}\n'
    runtime_sha = "1" * 64
    workspace_sha = "2" * 64
    activation_sha = "3" * 64
    plist_set_sha = cutover._sha256_json(dict(sorted(candidate_plists.items())))
    bindings = {
        "runtime_content_sha256": runtime_sha,
        "workspace_runtime_sha256": workspace_sha,
        "candidate_env_sha256": _sha(env_raw),
        "feishu_sidecar_sha256": _sha(sidecar_raw),
        "candidate_plist_set_sha256": plist_set_sha,
        "activation_contract_sha256": activation_sha,
        "expected_live_identity_sha256": "4" * 64,
        "rollback_live_identity_sha256": "4" * 64,
        "target_live_identity_sha256": "5" * 64,
        "cutover_authorization_receipt_sha256": "6" * 64,
        "cutover_lease_fingerprint": "7" * 64,
    }
    logical = {
        "runtime": cutover.CANONICAL_RUNTIME_ROOT,
        "workspace": cutover.CANONICAL_WORKSPACE_ROOT,
        "env": cutover.CANONICAL_ENV_PATH,
        "binding": Path("/Users/songying/.hermes/runtime/state/active-release-binding.json"),
        "sidecar": Path("/Users/songying/.hermes/feishu_api_poll_state_v1.json"),
        "runtime_stage": Path("/candidate/runtime"),
        "workspace_stage": Path("/candidate/workspace"),
    }
    plan = {
        "schema_version": cutover.PLAN_SCHEMA_VERSION,
        "release_id": "rca-live-fixture-0001",
        "authorization_machine_identity_sha256": "8" * 64,
        "bindings": bindings,
        "payload_bindings": {
            "runtime": {
                "canonical_path": str(logical["runtime"]),
                "staging_root": str(logical["runtime_stage"]),
                "candidate_plist_sha256": candidate_plists,
            },
            "workspace": {
                "canonical_path": str(logical["workspace"]),
                "staging_root": str(logical["workspace_stage"]),
            },
            "candidate_environment": {
                "canonical_path": str(logical["env"]),
                "sha256": _sha(env_raw),
            },
            "active_release_binding": {
                "canonical_path": str(logical["binding"]),
                "sha256": _sha(binding_raw),
            },
            "feishu_sidecar": {
                "canonical_path": str(logical["sidecar"]),
                "sha256": _sha(sidecar_raw),
            },
        },
        "gateway_aux_start_order": list(cutover.GATEWAY_AUX_LABELS),
        "resident_start_order": list(cutover.RESIDENT_LABELS),
    }
    payloads = {
        "candidate_environment": {"kind": "regular_file"},
        "active_release_binding": {"kind": "regular_file"},
        "feishu_sidecar": {"kind": "regular_file"},
        "runtime": {
            "kind": "runtime_tree",
            "files": runtime_files,
            "binding_sha256": runtime_sha,
        },
        "workspace": {
            "kind": "workspace_tree",
            "path": str(logical["workspace_stage"]),
            "binding_sha256": workspace_sha,
            "identity": {
                "file_sha256": {"prompt.md": workspace_files["prompt.md"]["sha256"]},
                "manifest_path": str(
                    logical["workspace_stage"] / "workspace-runtime-manifest.json"
                ),
                "manifest_sha256": workspace_files[
                    "workspace-runtime-manifest.json"
                ]["sha256"],
            },
        },
    }
    observer = live.ProjectedLiveIdentityObserver(
        plan=plan,
        payloads=payloads,
        path_mapper=physical,
    )
    return {
        "physical": physical,
        "logical": logical,
        "plan": plan,
        "payloads": payloads,
        "observer": observer,
        "runtime_files": runtime_files,
        "workspace_files": workspace_files,
        "candidate_plists": candidate_plists,
        "env_raw": env_raw,
        "binding_raw": binding_raw,
        "sidecar_raw": sidecar_raw,
    }


def _install_projection(fixture: dict) -> None:
    physical = fixture["physical"]
    logical = fixture["logical"]
    for relative, descriptor in fixture["runtime_files"].items():
        raw = {
            "gateway/run.py": b"candidate-gateway\n",
            ".venv/bin/python": b"candidate-python\n",
        }[relative]
        _write(
            physical(logical["runtime"] / relative),
            raw,
            int(descriptor["mode"], 8),
        )
    for relative, descriptor in fixture["workspace_files"].items():
        raw = {
            "prompt.md": b"candidate-workspace\n",
            "workspace-runtime-manifest.json": b"{}\n",
        }[relative]
        _write(
            physical(logical["workspace"] / relative),
            raw,
            int(descriptor["mode"], 8),
        )
    _write(physical(logical["env"]), fixture["env_raw"])
    _write(physical(logical["binding"]), fixture["binding_raw"])
    _write(physical(logical["sidecar"]), fixture["sidecar_raw"])
    for candidate in cutover.CANDIDATE_PLISTS:
        canonical = cutover.CANONICAL_LAUNCH_AGENTS_ROOT / candidate.replace(
            ".candidate.plist", ".plist"
        )
        _write(physical(canonical), f"plist:{candidate}\n".encode(), 0o644)


def test_projected_live_identity_only_matches_after_exact_install(projected) -> None:
    initial = projected["observer"]()
    assert initial["runtime_content_sha256"] != projected["plan"]["bindings"][
        "runtime_content_sha256"
    ]

    _install_projection(projected)
    observed = projected["observer"]()
    expected = {
        "runtime_content_sha256": projected["plan"]["bindings"][
            "runtime_content_sha256"
        ],
        "workspace_runtime_sha256": projected["plan"]["bindings"][
            "workspace_runtime_sha256"
        ],
        "candidate_env_sha256": projected["plan"]["bindings"][
            "candidate_env_sha256"
        ],
        "active_release_binding_sha256": projected["plan"]["payload_bindings"][
            "active_release_binding"
        ]["sha256"],
        "feishu_sidecar_sha256": projected["plan"]["bindings"][
            "feishu_sidecar_sha256"
        ],
        "candidate_plist_set_sha256": projected["plan"]["bindings"][
            "candidate_plist_set_sha256"
        ],
        "activation_contract_sha256": projected["plan"]["bindings"][
            "activation_contract_sha256"
        ],
    }
    assert observed == expected
    projected["plan"]["bindings"]["target_live_identity_sha256"] = cutover._sha256_json(
        expected
    )
    assert cutover._sha256_json(projected["observer"]()) == projected["plan"][
        "bindings"
    ]["target_live_identity_sha256"]


def test_projected_live_identity_detects_same_path_tamper(projected) -> None:
    _install_projection(projected)
    before = projected["observer"]()
    gateway = projected["physical"](
        projected["logical"]["runtime"] / "gateway/run.py"
    )
    gateway.write_bytes(b"tampered\n")

    after = projected["observer"]()

    assert after["runtime_content_sha256"] != before["runtime_content_sha256"]
    assert after["candidate_env_sha256"] == before["candidate_env_sha256"]


class FakeRunner:
    def __init__(self, jobs: dict[str, dict]) -> None:
        self.jobs = jobs
        self.calls: list[tuple[str, ...]] = []

    def run(self, argv) -> adapter.CommandResult:
        command = tuple(argv)
        self.calls.append(command)
        if command[1] == "print":
            label = command[2].rsplit("/", 1)[-1]
            job = self.jobs[label]
            if not job["loaded"]:
                return adapter.CommandResult(command, 113, "", "missing")
            lines = [f"\tstate = {job['state']}"]
            if job.get("pid") is not None:
                lines.append(f"\tpid = {job['pid']}")
            if job.get("last_exit_status") is not None:
                lines.append(f"\tlast exit code = {job['last_exit_status']}")
            lines.extend(("\tproperties = {", "\t\tstate = active", "\t}"))
            return adapter.CommandResult(command, 0, "\n".join(lines) + "\n", "")
        if command[1] == "bootout":
            label = command[2].rsplit("/", 1)[-1]
            self.jobs[label].update(loaded=False, pid=None, state="absent")
            return adapter.CommandResult(command, 0)
        if command[1] == "bootstrap":
            label = Path(command[3]).stem
            self.jobs[label].update(
                loaded=True,
                state="running",
                pid=self.jobs[label].get("bootstrap_pid", 9001),
            )
            return adapter.CommandResult(command, 0)
        raise AssertionError(command)


class FakeProcess:
    def __init__(self, pid: int, runtime: Path) -> None:
        self.pid = pid
        self.runtime = runtime

    def create_time(self) -> float:
        return 1000.0 + self.pid

    def cwd(self) -> str:
        return str(self.runtime)

    def environ(self) -> dict[str, str]:
        return {"VIRTUAL_ENV": str(self.runtime / ".venv")}


def _plist(path: Path, label: str, runtime: Path) -> None:
    body = {
        "Label": label,
        "WorkingDirectory": str(runtime),
        "ProgramArguments": [str(runtime / ".venv/bin/python"), "-m", "fixture"],
    }
    _write(path, plistlib.dumps(body), 0o644)


def test_launchd_controller_verifies_residents_and_periodic_one_shot(tmp_path) -> None:
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    plists = tmp_path / "LaunchAgents"
    jobs = {
        "ai.hermes.gateway": {
            "loaded": True,
            "state": "running",
            "pid": 101,
            "last_exit_status": None,
        },
        "local.pnc.completion-notice-relay": {
            "loaded": True,
            "state": "running",
            "pid": 102,
            "last_exit_status": None,
        },
        "local.pnc.vm-task-sync": {
            "loaded": True,
            "state": "not running",
            "pid": None,
            "last_exit_status": 0,
        },
    }
    for label in jobs:
        _plist(plists / f"{label}.plist", label, runtime)
    controller = live.LaunchdServiceController(
        evidence_root=tmp_path / "evidence",
        target_runtime_root=runtime,
        launch_agents_root=plists,
        runner=FakeRunner(jobs),
        process_factory=lambda pid: FakeProcess(pid, runtime),
    )

    result = controller.verify(cutover.GATEWAY_AUX_LABELS, runtime_sha256="a" * 64)

    assert result["ai.hermes.gateway"]["kind"] == "resident"
    assert result["ai.hermes.gateway"]["pid"] == 101
    assert result["local.pnc.vm-task-sync"] == {
        "kind": "periodic",
        "loaded": True,
        "pid": None,
        "process_create_time": None,
        "runtime_sha256": "a" * 64,
        "health_ok": True,
    }


def test_launchd_controller_fails_closed_when_resident_has_no_pid(tmp_path) -> None:
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    plists = tmp_path / "LaunchAgents"
    label = "ai.hermes.gateway"
    _plist(plists / f"{label}.plist", label, runtime)
    jobs = {
        label: {
            "loaded": True,
            "state": "not running",
            "pid": None,
            "last_exit_status": 0,
        }
    }
    controller = live.LaunchdServiceController(
        evidence_root=tmp_path / "evidence",
        target_runtime_root=runtime,
        launch_agents_root=plists,
        runner=FakeRunner(jobs),
    )

    with pytest.raises(live.LiveBoundaryError) as error:
        controller.verify((label,), runtime_sha256="a" * 64)

    assert error.value.code == "cutover_live_resident_not_running"


def _service_jobs() -> dict[str, dict]:
    return {
        label: {
            "loaded": True,
            "state": "running",
            "pid": 200 + index,
            "last_exit_status": None,
        }
        for index, label in enumerate(cutover.SERVICE_LABELS)
    }


def test_launchd_controller_snapshots_and_restores_bound_precutover_state(
    tmp_path,
) -> None:
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    plists = tmp_path / "LaunchAgents"
    jobs = _service_jobs()
    for label in jobs:
        _plist(plists / f"{label}.plist", label, runtime)
    runner = FakeRunner(jobs)
    original = live.LaunchdServiceController(
        evidence_root=tmp_path / "evidence",
        target_runtime_root=runtime,
        launch_agents_root=plists,
        runner=runner,
    ).capture_state(cutover.SERVICE_LABELS)
    for label in cutover.WRITER_LABELS:
        jobs[label].update(loaded=False, state="absent", pid=None)
    controller = live.LaunchdServiceController(
        evidence_root=tmp_path / "evidence",
        target_runtime_root=runtime,
        launch_agents_root=plists,
        runner=runner,
        precutover_service_state=original,
    )

    assert controller.capture_state(cutover.SERVICE_LABELS) == original

    controller.restore_state(original)
    assert all(jobs[label]["loaded"] for label in cutover.SERVICE_LABELS)


def test_launchd_controller_rejects_precutover_snapshot_while_writer_loaded(
    tmp_path,
) -> None:
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    plists = tmp_path / "LaunchAgents"
    jobs = _service_jobs()
    for label in jobs:
        _plist(plists / f"{label}.plist", label, runtime)
    runner = FakeRunner(jobs)
    original = live.LaunchdServiceController(
        evidence_root=tmp_path / "evidence",
        target_runtime_root=runtime,
        launch_agents_root=plists,
        runner=runner,
    ).capture_state(cutover.SERVICE_LABELS)
    controller = live.LaunchdServiceController(
        evidence_root=tmp_path / "evidence",
        target_runtime_root=runtime,
        launch_agents_root=plists,
        runner=runner,
        precutover_service_state=original,
    )

    with pytest.raises(live.LiveBoundaryError) as error:
        controller.capture_state(cutover.SERVICE_LABELS)

    assert error.value.code == "cutover_live_writer_not_stopped_for_snapshot"


def test_launchd_controller_starts_verifies_and_restores_exact_resident_set(
    tmp_path,
) -> None:
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    plists = tmp_path / "LaunchAgents"
    jobs = {
        label: {
            "loaded": False,
            "state": "absent",
            "pid": None,
            "last_exit_status": None,
            "bootstrap_pid": 9100 + index,
        }
        for index, label in enumerate(cutover.RESIDENT_LABELS)
    }
    for label in jobs:
        _plist(plists / f"{label}.plist", label, runtime)
    controller = live.LaunchdServiceController(
        evidence_root=tmp_path / "evidence",
        target_runtime_root=runtime,
        launch_agents_root=plists,
        runner=FakeRunner(jobs),
        process_factory=lambda pid: FakeProcess(pid, runtime),
    )
    initial = controller.capture_state(cutover.RESIDENT_LABELS)

    started = controller.start_residents(cutover.RESIDENT_LABELS)
    health = controller.verify(cutover.RESIDENT_LABELS, runtime_sha256="a" * 64)

    assert started == list(cutover.RESIDENT_LABELS)
    assert set(health) == set(cutover.RESIDENT_LABELS)
    assert all(item["kind"] == "resident" for item in health.values())
    controller.restore_state(initial)
    assert all(not jobs[label]["loaded"] for label in cutover.RESIDENT_LABELS)


def test_launchd_controller_stops_exact_writer_set_and_writes_receipt(tmp_path) -> None:
    jobs = {
        label: {
            "loaded": True,
            "state": "running",
            "pid": 100 + index,
            "last_exit_status": None,
        }
        for index, label in enumerate(cutover.WRITER_LABELS)
    }
    runner = FakeRunner(jobs)
    evidence = {
        "schema_version": "pnc_rca_writer_stop_evidence_v1",
        "observed_at": "2026-07-16T00:00:00+00:00",
        "services": {label: {"health_state": "stopped"} for label in jobs},
    }

    def write_receipt(path: Path, body) -> None:
        _write(
            path,
            (json.dumps(body, sort_keys=True, separators=(",", ":")) + "\n").encode(),
        )

    controller = live.LaunchdServiceController(
        evidence_root=tmp_path / "evidence",
        target_runtime_root=tmp_path / "runtime",
        launch_agents_root=tmp_path / "LaunchAgents",
        runner=runner,
        writer_stop_collector=lambda: evidence,
        receipt_writer=write_receipt,
    )

    result = controller.stop_writers(
        cutover.WRITER_LABELS,
        lease_fingerprint="b" * 64,
        lease_token="fixture-lease-token-0001",
    )

    assert result["writer_labels"] == list(cutover.WRITER_LABELS)
    assert Path(result["receipt_path"]).is_file()
    assert all(not job["loaded"] for job in jobs.values())
    bootouts = [call for call in runner.calls if call[1] == "bootout"]
    assert [call[2].rsplit("/", 1)[-1] for call in bootouts] == list(
        cutover.WRITER_LABELS
    )
