from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys
from types import SimpleNamespace

import pytest

from scripts import pnc_rca_minimal_release as release


ZERO_INFLIGHT = {
    "dispatchable_outbox": 0,
    "execution_delivery": 0,
    "pending_inbox": 0,
    "total": 0,
}


def test_script_path_help_is_executable_from_repository_root():
    repo_root = Path(__file__).resolve().parents[2]
    completed = subprocess.run(
        [
            sys.executable,
            str(repo_root / "scripts" / "pnc_rca_minimal_release.py"),
            "--help",
        ],
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert "{prepare,plan,apply,verify}" in completed.stdout


def _write(path: Path, raw: bytes, mode: int = 0o600) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    path.chmod(mode)
    return path


def _write_json(path: Path, value: object, mode: int = 0o600) -> bytes:
    raw = json.dumps(value, sort_keys=True, indent=2).encode() + b"\n"
    _write(path, raw, mode)
    return raw


class FakeStore:
    def __init__(
        self,
        current=None,
        predecessor=None,
        source_snapshot=None,
        partition_progress=None,
    ):
        self.current = current
        self.predecessor = predecessor
        self.source_snapshot = source_snapshot
        self.progress = partition_progress or {}
        self.activation_calls = []
        self.partition_calls = []

    def activation_epoch(self):
        return self.current

    def direct_steady_predecessor(self):
        return self.predecessor

    def control_db_source_snapshot_identity(self):
        if self.source_snapshot is None:
            raise RuntimeError("test_source_snapshot_missing")
        return json.loads(json.dumps(self.source_snapshot))

    def partition_progress(self, *, topic, partitions):
        self.partition_calls.append((topic, tuple(partitions)))
        return {
            partition: self.progress[(topic, partition)]
            for partition in partitions
            if (topic, partition) in self.progress
        }

    def activate_direct_steady_epoch(self, **kwargs):
        self.activation_calls.append(kwargs)
        self.current = {
            "epoch_id": kwargs["epoch_id"],
            **release._activation_expected({
                "release_fingerprint": kwargs["release_fingerprint"],
                "release_binding_sha256": kwargs["release_binding_sha256"],
                "config_sha256": kwargs["config_sha256"],
                "db_logical_identity_sha256": release._sha(
                    release._canonical(kwargs["db_logical_identity"])
                ),
                "partition_start_fence_sha256": release._sha(
                    release._canonical(kwargs["partition_start_fence"])
                ),
            }),
            "updated_at": "2026-08-17T12:00:00+00:00",
        }
        return self.current


class FakeRunner:
    def __init__(
        self,
        data,
        pids=None,
        bad_ref_face="",
        bad_tree_face="",
        bad_ref_round=1,
        bad_tag_type_face="",
        loaded_without_pid=(),
    ):
        self.data = data
        self.pids = dict(pids or {
            row[0]: 1000 + index for index, row in enumerate(release.REQUIRED_RESIDENTS)
        })
        self.initial_pids = dict(self.pids)
        self.loaded = {
            label for label, pid in self.pids.items() if pid is not None
        } | set(loaded_without_pid)
        self.disabled = {}
        self.bad_ref_face = bad_ref_face
        self.bad_tree_face = bad_tree_face
        self.bad_ref_round = bad_ref_round
        self.bad_tag_type_face = bad_tag_type_face
        self.fetch_faces = {}
        self.fetch_rounds = {}
        self.calls = []

    def __call__(self, command):
        command = tuple(command)
        self.calls.append(command)
        if (
            len(command) > 4
            and command[:2] == ("/usr/bin/git", "-C")
            and command[3] == "fetch"
        ):
            name, _face = next(
                (name, face)
                for name, face in self.data["identity"].items()
                if isinstance(face, dict) and face.get("remote") in command
            )
            round_no = self.fetch_rounds.get(name, 0) + 1
            self.fetch_rounds[name] = round_no
            self.fetch_faces[command[2]] = (name, round_no)
            return subprocess.CompletedProcess(command, 0, "", "")
        if (
            len(command) > 3
            and command[:2] == ("/usr/bin/git", "-C")
            and command[3] == "cat-file"
        ):
            name, _round_no = self.fetch_faces[command[2]]
            kind = "commit" if name == self.bad_tag_type_face else "tag"
            return subprocess.CompletedProcess(command, 0, f"{kind}\n", "")
        if "rev-parse" in command and any(
            "refs/pnc-rca-readback/" in value for value in command
        ):
            name = next(
                value.split("refs/pnc-rca-readback/", 1)[1].split("/", 1)[0]
                for value in command
                if "refs/pnc-rca-readback/" in value
            )
            face = self.data["identity"][name]
            _fetched_name, round_no = self.fetch_faces[command[2]]
            commit = (
                "0" * 40
                if name == self.bad_ref_face and round_no >= self.bad_ref_round
                else face["commit"]
            )
            tree = "0" * 40 if name == self.bad_tree_face else face["tree"]
            return subprocess.CompletedProcess(
                command,
                0,
                f"{commit}\n{tree}\n{face['remote_tag_object']}\n{commit}\n",
                "",
            )
        if "rev-parse" in command:
            host = self.data["identity"]["host"]
            return subprocess.CompletedProcess(
                command, 0, f"{host['commit']}\n{host['tree']}\n", ""
            )
        if "status" in command:
            return subprocess.CompletedProcess(command, 0, "", "")
        if command[:2] == ("/bin/launchctl", "print"):
            label = command[-1].rsplit("/", 1)[-1]
            if label in self.loaded:
                pid = self.pids.get(label)
                return subprocess.CompletedProcess(
                    command, 0, f"\tpid = {pid}\n" if pid is not None else "", ""
                )
            return subprocess.CompletedProcess(command, 113, "", "not found")
        if command[:2] == ("/bin/launchctl", "bootout"):
            label = command[-1].rsplit("/", 1)[-1]
            self.loaded.discard(label)
            self.pids[label] = None
            return subprocess.CompletedProcess(command, 0, "", "")
        if command[:2] == ("/bin/launchctl", "enable"):
            label = command[-1].rsplit("/", 1)[-1]
            self.disabled[label] = False
            return subprocess.CompletedProcess(command, 0, "", "")
        if command[:2] == ("/bin/launchctl", "disable"):
            label = command[-1].rsplit("/", 1)[-1]
            self.disabled[label] = True
            return subprocess.CompletedProcess(command, 0, "", "")
        if command[:2] == ("/bin/launchctl", "print-disabled"):
            rows = "\n".join(
                f'\t"{label}" => {"disabled" if value else "enabled"}'
                for label, value in sorted(self.disabled.items())
            )
            return subprocess.CompletedProcess(command, 0, rows + "\n", "")
        if command[:2] == ("/bin/launchctl", "bootstrap"):
            label = Path(command[-1]).stem
            prior = self.initial_pids.get(label)
            self.pids[label] = (prior if isinstance(prior, int) else 1000) + 10000
            self.loaded.add(label)
            return subprocess.CompletedProcess(command, 0, "", "")
        return subprocess.CompletedProcess(command, 0, "", "")


@pytest.fixture
def release_files(tmp_path):
    home = tmp_path / "home/.hermes"
    runtime_root = home / "runtime/releases/hermes-v0.18.2-r15aw-host-git"
    runtime_root.mkdir(parents=True)
    note_path = home / "runtime/operator_receipts/rca-r15aw.release-note.json"
    canary_path = home / "runtime/operator_receipts/rca-r15aw.canary.json"
    control_db = home / release.CONTROL_DB_RELATIVE_PATH
    env_source = tmp_path / "candidate.env"
    manifest_source = tmp_path / "candidate-manifest.json"
    live_env = _write(home / ".env", b"OLD_ENV=1\n")
    live_manifest = _write(home / "runtime/LIVE_MANIFEST.json", b'{"old":true}\n')

    env_raw = (
        "HERMES_RCA_KAFKA_SUBMIT_ENABLED=false\n"
        "HERMES_RCA_OUTBOX_DISPATCH_ENABLED=true\n"
        "HERMES_RCA_DELIVERY_COLLECTOR_ENABLED=true\n"
        "HERMES_RCA_DELIVERY_DISPATCHER_ENABLED=true\n"
        "HERMES_RCA_ACTIVATION_REQUIRED=true\n"
        f"HERMES_RCA_RELEASE_NOTE_PATH={note_path}\n"
        "HERMES_OUTBOUND_MODE=record-only\n"
        + "".join(f"{key}={control_db}\n" for key in release.CONTROL_DB_ENV_KEYS)
    ).encode()
    _write(env_source, env_raw)
    identity = {
        "host": {
            "commit": "1" * 40,
            "tree": "2" * 40,
            "remote": release.HOST_REMOTE,
            "remote_branch": "refs/heads/production/rca",
            "remote_tag": "rca-host-r15aw-20260817",
            "remote_tag_object": "3" * 40,
            "runtime_root": str(runtime_root),
        },
        "worker": {
            "commit": "4" * 40,
            "tree": "5" * 40,
            "remote": "git@git.minieye.tech:planning_algo/vm-worker-state.git",
            "remote_branch": "refs/heads/release/rca",
            "remote_tag": "rca-worker-r15aw-20260817",
            "remote_tag_object": "6" * 40,
            "runtime_root": "/home/mini/.hermes/worker-state",
        },
        "pipeline": {
            "commit": "7" * 40,
            "tree": "8" * 40,
            "remote": "git@git.minieye.tech:pdcl/yj-evaluation-server.git",
            "remote_branch": "refs/heads/rca",
            "remote_tag": "rca-pipeline-r15aw-20260817",
            "remote_tag_object": "a" * 40,
            "runtime_root": "/home/mini/.hermes/rca-prod-runtime/releases/r15aw",
        },
        "report_service": {
            "manifest_path": "/home/mini/.config/g1q3-rca/report-runtime-manifest.json",
            "manifest_sha256": "9" * 64,
            "pipeline_commit": "7" * 40,
            "pipeline_tree": "8" * 40,
        },
    }
    fingerprint = release._sha(release._canonical(identity))
    manifest = {
        "runtime_root": str(runtime_root),
        "promotion_source_head": identity["host"]["commit"],
        "env_sha256": release._sha(env_raw),
        "face_git_bindings": {
            "runtime_engine": {
                "commit": identity["host"]["commit"],
                "tree": identity["host"]["tree"],
                "repo": str(runtime_root),
            }
        },
        "rca_release_note": {
            "path": str(note_path),
            "release_id": "rca-r15aw-20260817",
            "release_fingerprint_sha256": fingerprint,
        },
    }
    manifest_raw = _write_json(manifest_source, manifest)
    db_identity = {"schema_version": "test_db_v1", "path": str(control_db)}
    fence = {"feishu-project-workflow-event": {"0": 1984}}
    note = {
        "schema_version": release.NOTE_SCHEMA,
        "production_definition": release.PRODUCTION_DEFINITION,
        "release_id": "rca-r15aw-20260817",
        "release_fingerprint_sha256": fingerprint,
        "release_identity": identity,
        "runtime_projection": {
            "env_sha256": release._sha(env_raw),
            "live_manifest_sha256": release._sha(manifest_raw),
        },
        "activation": {
            "epoch_id": "rca-activation-r15aw-20260817",
            "control_db_path": str(control_db),
            "operator": "owner:test",
            "reason": "test minimal release",
            "expected_predecessor_epoch_id": "",
            "expected_predecessor_state": "",
            "expected_predecessor_binding_fingerprint": "",
            "db_logical_identity": db_identity,
            "db_logical_identity_sha256": release._sha(release._canonical(db_identity)),
            "partition_start_fence": fence,
            "partition_start_fence_sha256": release._sha(release._canonical(fence)),
        },
        "resident_profile": {
            "name": "operator_issue_only_v1",
            "required": [row[0] for row in release.REQUIRED_RESIDENTS],
            "disabled": list(release.DISABLED_RESIDENTS),
        },
        "canary": {
            "batch_id": "canary-r15aw-7049076163-20260817",
            "issue_id": "7049076163",
            "state_path": str(canary_path),
        },
    }
    note_raw = _write_json(note_path, note)
    return {
        "home": home,
        "runtime_root": runtime_root,
        "note_path": note_path,
        "note": note,
        "note_raw": note_raw,
        "canary_path": canary_path,
        "control_db": control_db,
        "env_source": env_source,
        "manifest_source": manifest_source,
        "live_env": live_env,
        "live_manifest": live_manifest,
        "identity": identity,
    }


def _factory(store, calls):
    def factory(path, read_only):
        calls.append((path, read_only))
        return store

    return factory


def _plan_args(data):
    return {
        "release_note": data["note_path"],
        "manifest_source": data["manifest_source"],
        "env_source": data["env_source"],
        "expected_manifest_sha256": release._sha(data["live_manifest"].read_bytes()),
        "expected_env_sha256": release._sha(data["live_env"].read_bytes()),
        "home": data["home"],
    }


def _prepare_fixture(data, *, store=None, partition_topics=None, report=None):
    note_path = data["note_path"].with_name("prepared-release-note.json")
    env_output = data["note_path"].with_name("prepared.env")
    manifest_output = data["note_path"].with_name("prepared-manifest.json")
    logical_identity = {
        "database": {"path": str(data["control_db"]), "sha256": "b" * 64},
        "wal": {"present": False},
    }
    source_snapshot = {
        "schema_version": "pnc_rca_control_store_source_snapshot_v1",
        "path": str(data["control_db"].absolute()),
        "present": True,
        "logical_db_identity": logical_identity,
    }
    topic_map = (
        {"feishu-project-workflow-event": (0,)}
        if partition_topics is None
        else partition_topics
    )
    if store is None:
        store = FakeStore(
            source_snapshot=source_snapshot,
            partition_progress={
                (topic, partition): 1984 + partition
                for topic, partitions in topic_map.items()
                for partition in partitions
            },
        )
    pipeline = data["identity"]["pipeline"]
    report_value = report or {
        "schema_version": "pnc_rca_report_manifest_v1",
        "runtime_root": pipeline["runtime_root"],
        "pipeline_commit": pipeline["commit"],
        "pipeline_tree": pipeline["tree"],
        "report_script_sha256": "c" * 64,
    }
    report_raw = release._pretty_json(report_value)
    store_calls = []

    def report_reader(path):
        assert str(path) == data["identity"]["report_service"]["manifest_path"]
        return report_raw, json.loads(report_raw)

    args = {
        "release_id": "rca-r15aw-prepared-20260817",
        "epoch_id": "rca-activation-r15aw-prepared-20260817",
        "operator": "owner:test",
        "reason": "prepare deterministic minimal release",
        "canary_batch_id": "canary-r15aw-prepared-7049076163",
        "canary_issue_id": "7049076163",
        "canary_state_path": data["canary_path"].with_name("prepared-canary.json"),
        "host_branch": data["identity"]["host"]["remote_branch"],
        "host_tag": data["identity"]["host"]["remote_tag"],
        "host_runtime_root": data["runtime_root"],
        "worker_remote": data["identity"]["worker"]["remote"],
        "worker_branch": data["identity"]["worker"]["remote_branch"],
        "worker_tag": data["identity"]["worker"]["remote_tag"],
        "worker_runtime_root": Path(data["identity"]["worker"]["runtime_root"]),
        "pipeline_remote": data["identity"]["pipeline"]["remote"],
        "pipeline_branch": data["identity"]["pipeline"]["remote_branch"],
        "pipeline_tag": data["identity"]["pipeline"]["remote_tag"],
        "pipeline_runtime_root": Path(data["identity"]["pipeline"]["runtime_root"]),
        "report_manifest_path": Path(
            data["identity"]["report_service"]["manifest_path"]
        ),
        "partition_topics": topic_map,
        "control_db": data["control_db"],
        "release_note": note_path,
        "manifest_output": manifest_output,
        "env_output": env_output,
        "home": data["home"],
        "report_reader": report_reader,
        "store_factory": _factory(store, store_calls),
    }
    return {
        "args": args,
        "store": store,
        "store_calls": store_calls,
        "logical_identity": logical_identity,
        "report_raw": report_raw,
        "note_path": note_path,
        "env_output": env_output,
        "manifest_output": manifest_output,
    }


def test_prepare_derives_and_writes_deterministic_owner_only_outputs(release_files):
    prepared = _prepare_fixture(release_files)
    runner = FakeRunner(release_files)

    result = release.prepare_release(**prepared["args"], runner=runner)

    assert result["mode"] == "prepare"
    assert result["applied"] is False
    assert runner.fetch_rounds == {"host": 2, "worker": 2, "pipeline": 2}
    assert prepared["store_calls"] == [(release_files["control_db"], True)]
    for path in (
        prepared["note_path"],
        prepared["env_output"],
        prepared["manifest_output"],
    ):
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
    note = json.loads(prepared["note_path"].read_bytes())
    assert note["release_identity"]["host"]["commit"] == "1" * 40
    assert note["release_identity"]["worker"]["tree"] == "5" * 40
    assert note["release_identity"]["pipeline"]["remote_tag_object"] == "a" * 40
    assert note["release_identity"]["report_service"][
        "manifest_sha256"
    ] == release._sha(prepared["report_raw"])
    assert note["activation"]["db_logical_identity"] == prepared["logical_identity"]
    assert note["activation"]["partition_start_fence"] == {
        "feishu-project-workflow-event": {"0": 1984}
    }
    env = release._parse_env(prepared["env_output"].read_bytes())
    assert {key: env[key] for key in release.CONTROL_DB_ENV_KEYS} == {
        key: str(release_files["control_db"]) for key in release.CONTROL_DB_ENV_KEYS
    }
    first = {
        path: path.read_bytes()
        for path in (
            prepared["note_path"],
            prepared["env_output"],
            prepared["manifest_output"],
        )
    }
    for path in first:
        path.unlink()
    release.prepare_release(**prepared["args"], runner=FakeRunner(release_files))
    assert {path: path.read_bytes() for path in first} == first


def test_prepare_rejects_existing_output_and_cleans_new_reservations(release_files):
    prepared = _prepare_fixture(release_files)
    prepared["env_output"].write_bytes(b"preexisting\n")
    runner = FakeRunner(release_files)

    with pytest.raises(release.ReleaseError, match="prepare_output_exists"):
        release.prepare_release(**prepared["args"], runner=runner)

    assert prepared["env_output"].read_bytes() == b"preexisting\n"
    assert not prepared["note_path"].exists()
    assert not prepared["manifest_output"].exists()
    assert not runner.calls
    assert not prepared["store_calls"]


def test_prepare_rejects_noncanonical_control_db(release_files):
    prepared = _prepare_fixture(release_files)
    prepared["args"]["control_db"] = release_files["home"] / "runtime/other.sqlite3"

    with pytest.raises(release.ReleaseError, match="control_db_path_invalid"):
        release.prepare_release(**prepared["args"], runner=FakeRunner(release_files))

    assert not prepared["note_path"].exists()
    assert not prepared["env_output"].exists()
    assert not prepared["manifest_output"].exists()


def test_prepare_rejects_gitlab_drift_before_creating_outputs(release_files):
    prepared = _prepare_fixture(release_files)

    with pytest.raises(release.ReleaseError, match="gitlab_changed_during_prepare"):
        release.prepare_release(
            **prepared["args"],
            runner=FakeRunner(release_files, bad_ref_face="pipeline", bad_ref_round=2),
        )

    assert not prepared["note_path"].exists()
    assert not prepared["env_output"].exists()
    assert not prepared["manifest_output"].exists()


def test_prepare_rejects_github_identity(release_files):
    prepared = _prepare_fixture(release_files)
    prepared["args"]["pipeline_remote"] = (
        "git@github.com:minieye/yj-evaluation-server.git"
    )

    with pytest.raises(release.ReleaseError, match="gitlab_face_input_invalid"):
        release.prepare_release(**prepared["args"], runner=FakeRunner(release_files))

    assert not prepared["note_path"].exists()


def test_prepare_rejects_report_manifest_pipeline_mismatch(release_files):
    report = {
        "schema_version": "pnc_rca_report_manifest_v1",
        "runtime_root": release_files["identity"]["pipeline"]["runtime_root"],
        "pipeline_commit": "0" * 40,
        "pipeline_tree": release_files["identity"]["pipeline"]["tree"],
        "report_script_sha256": "c" * 64,
    }
    prepared = _prepare_fixture(release_files, report=report)

    with pytest.raises(release.ReleaseError, match="report_manifest_pipeline_mismatch"):
        release.prepare_release(**prepared["args"], runner=FakeRunner(release_files))

    assert not prepared["note_path"].exists()


def test_prepare_derives_predecessor_and_all_partition_fences(release_files):
    predecessor = {
        "epoch_id": "rca-activation-r15av-20260817",
        "state": "steady_active",
        "binding_fingerprint": "d" * 64,
        "inflight": ZERO_INFLIGHT,
    }
    topics = {"topic-a": (0, 2), "topic-b": (1,)}
    source = {
        "schema_version": "pnc_rca_control_store_source_snapshot_v1",
        "path": str(release_files["control_db"].absolute()),
        "present": True,
        "logical_db_identity": {"database": "snapshot", "wal": "snapshot-wal"},
    }
    store = FakeStore(
        predecessor=predecessor,
        source_snapshot=source,
        partition_progress={
            ("topic-a", 0): 10,
            ("topic-a", 2): 22,
            ("topic-b", 1): 31,
        },
    )
    prepared = _prepare_fixture(release_files, store=store, partition_topics=topics)

    release.prepare_release(**prepared["args"], runner=FakeRunner(release_files))

    activation = json.loads(prepared["note_path"].read_bytes())["activation"]
    assert activation["expected_predecessor_epoch_id"] == predecessor["epoch_id"]
    assert activation["expected_predecessor_state"] == "steady_active"
    assert activation["expected_predecessor_binding_fingerprint"] == "d" * 64
    assert activation["partition_start_fence"] == {
        "topic-a": {"0": 10, "2": 22},
        "topic-b": {"1": 31},
    }
    assert store.partition_calls == [("topic-a", (0, 2)), ("topic-b", (1,))]


def test_prepare_accepts_kafka_disabled_empty_partition_fence(release_files):
    prepared = _prepare_fixture(release_files, partition_topics={})

    release.prepare_release(**prepared["args"], runner=FakeRunner(release_files))

    activation = json.loads(prepared["note_path"].read_bytes())["activation"]
    assert activation["partition_start_fence"] == {}
    assert prepared["store"].partition_calls == []
    assert release._partition_topic_arguments([]) == {}


def test_prepare_removes_legacy_release_env_keys(release_files):
    live = ["KEEP_ME=value"]
    live.extend(f"{key}=legacy" for key in sorted(release.LEGACY_ENV))
    release_files["live_env"].write_text("\n".join(live) + "\n")
    prepared = _prepare_fixture(release_files)

    release.prepare_release(**prepared["args"], runner=FakeRunner(release_files))

    env = release._parse_env(prepared["env_output"].read_bytes())
    assert env["KEEP_ME"] == "value"
    assert not release.LEGACY_ENV & env.keys()


def test_report_manifest_reader_uses_fixed_ssh_mini_doctor_and_read_protocol(
    release_files,
):
    pipeline = release_files["identity"]["pipeline"]
    report = {
        "schema_version": "pnc_rca_report_manifest_v1",
        "runtime_root": pipeline["runtime_root"],
        "pipeline_commit": pipeline["commit"],
        "pipeline_tree": pipeline["tree"],
        "report_script_sha256": "c" * 64,
    }
    raw = release._pretty_json(report)
    calls = []

    def vm_runner(command, input_text):
        calls.append((tuple(command), input_text))
        if command[1:] == ("doctor", "--json"):
            return subprocess.CompletedProcess(command, 0, '{"ok":true}\n', "")
        assert command[1:] == ("run_py_json",)
        assert str(release.REPORT_MANIFEST_ROOT) in input_text
        response = {
            "raw_base64": release.base64.b64encode(raw).decode(),
            "sha256": release._sha(raw),
        }
        return subprocess.CompletedProcess(command, 0, json.dumps(response), "")

    observed_raw, observed = release._read_vm_report_manifest(
        Path(release_files["identity"]["report_service"]["manifest_path"]),
        vm_runner=vm_runner,
    )

    assert observed_raw == raw
    assert observed == report
    assert [command[0][0] for command in calls] == [
        release.SSH_MINI_AGENT,
        release.SSH_MINI_AGENT,
    ]


def test_vm_agent_runner_rejects_environment_overrides(monkeypatch):
    monkeypatch.setenv("HOME", "/tmp/attacker-home")
    monkeypatch.setenv("SSH_MINI_DEFAULT_DIR", "/tmp/attacker-dir")
    monkeypatch.setenv("SSH_MINI_AGENT_ALLOW_LONG_GUARD_BYPASS", "1")
    captured = {}

    def fake_run(command, **kwargs):
        captured.update(kwargs)
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(release.subprocess, "run", fake_run)
    release._run_vm_agent((release.SSH_MINI_AGENT, "doctor", "--json"))

    assert captured["env"]["HOME"] == "/Users/songying"
    assert "SSH_MINI_DEFAULT_DIR" not in captured["env"]
    assert "SSH_MINI_AGENT_ALLOW_LONG_GUARD_BYPASS" not in captured["env"]


def test_plan_is_read_only_and_uses_exact_gitlab_and_profile(release_files):
    store = FakeStore()
    store_calls = []
    runner = FakeRunner(release_files)
    result = release.build_plan(
        **_plan_args(release_files),
        runner=runner,
        store_factory=_factory(store, store_calls),
    )

    assert result["mode"] == "plan"
    assert result["applied"] is False
    assert result["gitlab"]["host"]["remote"] == release.HOST_REMOTE
    assert result["gitlab"]["worker"]["remote"].startswith("git@git.minieye.tech:")
    assert result["gitlab"]["pipeline"]["remote"] == (
        "git@git.minieye.tech:pdcl/yj-evaluation-server.git"
    )
    assert [item["label"] for item in result["resident_profile"]["required"]] == [
        row[0] for row in release.REQUIRED_RESIDENTS
    ]
    assert [item["label"] for item in result["resident_profile"]["disabled"]] == list(
        release.DISABLED_RESIDENTS
    )
    assert store_calls == [(release_files["control_db"], True)]
    assert not store.activation_calls
    assert all("pnc_rca_activation.py" not in " ".join(call) for call in runner.calls)
    assert release_files["live_env"].read_bytes() == b"OLD_ENV=1\n"


@pytest.mark.parametrize(
    "change",
    [
        "host_remote",
        "worker_remote",
        "pipeline_remote",
        "worker_branch",
        "production_definition",
        "missing_worker",
        "legacy_bootstrap_env",
        "legacy_capacity_env",
        "outbound_live",
    ],
)
def test_release_contract_rejects_wrong_identity_or_legacy_env(release_files, change):
    data = release_files
    if change.startswith("legacy_") or change == "outbound_live":
        env_raw = data["env_source"].read_bytes()
        if change == "outbound_live":
            env_raw = env_raw.replace(
                b"HERMES_OUTBOUND_MODE=record-only",
                b"HERMES_OUTBOUND_MODE=live",
            )
        else:
            legacy = (
                b"HERMES_RCA_PROD_BOOTSTRAP_EPOCH_ID=legacy\n"
                if change == "legacy_bootstrap_env"
                else b"HERMES_RCA_PROD_CAPACITY_MODE=steady\n"
            )
            env_raw += legacy
        data["env_source"].write_bytes(env_raw)
        data["env_source"].chmod(0o600)
        env_sha = release._sha(data["env_source"].read_bytes())
        manifest = json.loads(data["manifest_source"].read_text())
        manifest["env_sha256"] = env_sha
        manifest_raw = _write_json(data["manifest_source"], manifest)
        note = json.loads(data["note_path"].read_text())
        note["runtime_projection"] = {
            "env_sha256": env_sha,
            "live_manifest_sha256": release._sha(manifest_raw),
        }
        _write_json(data["note_path"], note)
        expected = "candidate_env_binding_invalid"
    else:
        note = json.loads(data["note_path"].read_text())
        if change.endswith("_remote"):
            face = change.removesuffix("_remote")
            note["release_identity"][face]["remote"] = (
                "git@github.com:example/repository.git"
            )
        elif change == "worker_branch":
            note["release_identity"]["worker"]["remote_branch"] = "main"
        elif change == "production_definition":
            note["production_definition"] = "github_release"
        elif change == "missing_worker":
            del note["release_identity"]["worker"]
        note["release_fingerprint_sha256"] = release._sha(
            release._canonical(note["release_identity"])
        )
        _write_json(data["note_path"], note)
        expected = (
            "release_note_host_invalid"
            if change == "host_remote"
            else (
                "release_note_contract_invalid"
                if change == "production_definition"
                else "release_note_identity_invalid"
            )
        )
    with pytest.raises(release.ReleaseError) as caught:
        release.build_plan(
            **_plan_args(data),
            runner=FakeRunner(data),
            store_factory=_factory(FakeStore(), []),
        )
    assert caught.value.code == expected


@pytest.mark.parametrize("key", release.CONTROL_DB_ENV_KEYS)
def test_candidate_rejects_each_noncanonical_control_db_env_key(release_files, key):
    data = release_files
    expected = f"{key}={data['control_db']}".encode()
    env_raw = (
        data["env_source"]
        .read_bytes()
        .replace(
            expected,
            f"{key}={data['home'] / 'runtime/other.sqlite3'}".encode(),
        )
    )
    assert env_raw != data["env_source"].read_bytes()
    data["env_source"].write_bytes(env_raw)
    env_sha = release._sha(env_raw)
    manifest = json.loads(data["manifest_source"].read_text())
    manifest["env_sha256"] = env_sha
    manifest_raw = _write_json(data["manifest_source"], manifest)
    note = json.loads(data["note_path"].read_text())
    note["runtime_projection"] = {
        "env_sha256": env_sha,
        "live_manifest_sha256": release._sha(manifest_raw),
    }
    _write_json(data["note_path"], note)

    with pytest.raises(release.ReleaseError, match="candidate_env_binding_invalid"):
        release.build_plan(
            **_plan_args(data),
            runner=FakeRunner(data),
            store_factory=_factory(FakeStore(), []),
        )


def test_release_note_rejects_noncanonical_control_db(release_files):
    note = json.loads(release_files["note_path"].read_text())
    note["activation"]["control_db_path"] = str(
        release_files["home"] / "runtime/other.sqlite3"
    )
    _write_json(release_files["note_path"], note)

    with pytest.raises(release.ReleaseError, match="release_note_control_db_invalid"):
        release.build_plan(
            **_plan_args(release_files),
            runner=FakeRunner(release_files),
            store_factory=_factory(FakeStore(), []),
        )


def test_plan_rejects_pipeline_gitlab_ref_mismatch(release_files):
    with pytest.raises(release.ReleaseError, match="gitlab_identity_mismatch"):
        release.build_plan(
            **_plan_args(release_files),
            runner=FakeRunner(release_files, bad_ref_face="pipeline"),
            store_factory=_factory(FakeStore(), []),
        )


def test_plan_rejects_worker_gitlab_commit_tree_mismatch(release_files):
    with pytest.raises(release.ReleaseError, match="gitlab_tree_identity_mismatch"):
        release.build_plan(
            **_plan_args(release_files),
            runner=FakeRunner(release_files, bad_tree_face="worker"),
            store_factory=_factory(FakeStore(), []),
        )


def test_gitlab_readback_fetches_exact_branch_and_annotated_tag_in_one_repo(
    release_files,
):
    runner = FakeRunner(release_files)

    release.gitlab_readback(release_files["note"], runner)

    init_calls = [call for call in runner.calls if call[1:3] == ("init", "--bare")]
    fetch_calls = [
        call
        for call in runner.calls
        if len(call) > 3 and call[:2] == ("/usr/bin/git", "-C") and call[3] == "fetch"
    ]
    assert len(init_calls) == 1
    assert len(fetch_calls) == 3
    assert len({call[2] for call in fetch_calls}) == 1
    for name, face in release_files["identity"].items():
        if name == "report_service":
            continue
        call = next(item for item in fetch_calls if face["remote"] in item)
        assert f"+{face['remote_branch']}:refs/pnc-rca-readback/{name}/branch" in call
        assert (
            f"+refs/tags/{face['remote_tag']}:refs/pnc-rca-readback/{name}/tag" in call
        )


def test_gitlab_readback_rejects_lightweight_tag(release_files):
    with pytest.raises(release.ReleaseError, match="gitlab_identity_mismatch"):
        release.gitlab_readback(
            release_files["note"],
            FakeRunner(release_files, bad_tag_type_face="pipeline"),
        )


def test_git_runner_ignores_inherited_config_and_transport_overrides(monkeypatch):
    observed = {}

    def fake_run(command, **kwargs):
        observed.update(kwargs["env"])
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setenv("GIT_CONFIG_GLOBAL", "/tmp/operator.gitconfig")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", "url.file:///tmp/fake.insteadOf")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", "git@git.minieye.tech:")
    monkeypatch.setenv("GIT_SSH_COMMAND", "ssh -F /tmp/operator-ssh-config")
    monkeypatch.setenv("GIT_EXEC_PATH", "/tmp/operator-git-exec")
    monkeypatch.setattr(release.subprocess, "run", fake_run)

    release._run(("/usr/bin/git", "--version"))

    assert observed["GIT_CONFIG_GLOBAL"] == "/dev/null"
    assert observed["GIT_CONFIG_SYSTEM"] == "/dev/null"
    assert observed["GIT_CONFIG_NOSYSTEM"] == "1"
    assert observed["GIT_CONFIG_COUNT"] == "0"
    assert "GIT_CONFIG_KEY_0" not in observed
    assert "GIT_CONFIG_VALUE_0" not in observed
    assert "GIT_SSH_COMMAND" not in observed
    assert observed["GIT_SSH"] == "/usr/bin/ssh"
    assert observed["GIT_SSH_VARIANT"] == "ssh"
    assert "GIT_EXEC_PATH" not in observed


@pytest.mark.parametrize("change", ["owner_mode", "db_hash", "fence", "legacy_path"])
def test_release_note_embeds_exact_activation_binding(release_files, change):
    note = json.loads(release_files["note_path"].read_text())
    if change == "owner_mode":
        release_files["note_path"].chmod(0o640)
        expected = "release_note_identity_invalid"
    else:
        if change == "db_hash":
            note["activation"]["db_logical_identity_sha256"] = "f" * 64
            expected = "release_note_activation_binding_invalid"
        elif change == "fence":
            note["activation"]["partition_start_fence"] = {
                "feishu-project-workflow-event": {"00": 1984}
            }
            expected = "release_note_activation_fence_invalid"
        else:
            note["activation"]["direct_binding_path"] = "/tmp/legacy-binding.json"
            expected = "release_note_activation_invalid"
        _write_json(release_files["note_path"], note)
    with pytest.raises(release.ReleaseError) as caught:
        release._load_note(release_files["note_path"], release_files["home"])
    assert caught.value.code == expected


def test_activation_uses_control_store_directly_and_hides_legacy_columns(release_files):
    note = dict(release_files["note"])
    binding = release._bound_binding(release_files["note_raw"], note)
    store = FakeStore()
    calls = []
    factory = _factory(store, calls)

    plan = release._activation_plan(note, binding, factory)
    applied = release._activation_apply(note, binding, plan, factory)

    assert calls == [
        (release_files["control_db"], True),
        (release_files["control_db"], False),
    ]
    assert len(store.activation_calls) == 1
    assert applied["current_epoch"]["epoch_id"] == binding["epoch_id"]
    assert "capsule" not in json.dumps(applied)


def test_activation_rejects_inflight_steady_predecessor(release_files):
    note = json.loads(json.dumps(release_files["note"]))
    note["activation"].update({
        "expected_predecessor_epoch_id": "rca-activation-r15av-20260817",
        "expected_predecessor_state": "steady_active",
        "expected_predecessor_binding_fingerprint": "a" * 64,
    })
    binding = release._bound_binding(release_files["note_raw"], release_files["note"])
    predecessor = {
        "epoch_id": "rca-activation-r15av-20260817",
        "state": "steady_active",
        "binding_fingerprint": "a" * 64,
        "inflight": {**ZERO_INFLIGHT, "dispatchable_outbox": 1, "total": 1},
    }
    store = FakeStore(
        current={"epoch_id": predecessor["epoch_id"], "state": "steady_active"},
        predecessor=predecessor,
    )
    with pytest.raises(
        release.ReleaseError, match="activation_predecessor_inflight_not_drained"
    ):
        release._activation_plan(note, binding, _factory(store, []))


def _synthetic_resident_profile(data, *, base_pid=11000):
    required = []
    for index, (label, relative_script, *_rest) in enumerate(
        release.REQUIRED_RESIDENTS
    ):
        required.append({
            "label": label,
            "pid": base_pid + index,
            "cwd": str(data["runtime_root"]),
            "script": str(data["runtime_root"] / relative_script),
            "process_create_time": 100.0 + index,
            "freshness_kind": (
                "restart_transition"
                if not release.REQUIRED_RESIDENTS[index][4]
                else "health_heartbeat"
            ),
            "freshness_at": "2026-08-17T10:00:30+00:00",
            "freshness_age_seconds": 30.0,
        })
    return {
        "name": "operator_issue_only_v1",
        "required": required,
        "disabled": [
            {"label": label, "loaded": False, "pid": None}
            for label in release.DISABLED_RESIDENTS
        ],
        "persistent": {
            "required": [
                {"label": row[0], "disabled": False}
                for row in release.REQUIRED_RESIDENTS
            ],
            "disabled": [
                {"label": label, "disabled": True}
                for label in release.DISABLED_RESIDENTS
            ],
        },
    }


def test_apply_requires_confirmation_then_installs_and_calls_store(
    release_files, monkeypatch
):
    runner = FakeRunner(release_files)
    store = FakeStore()
    calls = []
    before = release_files["live_env"].read_bytes()
    with pytest.raises(release.ReleaseError, match="apply_confirmation_mismatch"):
        release.apply_release(
            **_plan_args(release_files),
            confirm_release_id="wrong-release",
            runner=runner,
            store_factory=_factory(store, calls),
        )
    assert release_files["live_env"].read_bytes() == before
    assert not runner.calls and not calls

    residents = _synthetic_resident_profile(release_files)
    monkeypatch.setattr(release, "_restart", lambda *_args, **_kwargs: residents)
    monkeypatch.setattr(
        release, "_resident_profile_readback", lambda *_args, **_kwargs: residents
    )
    receipt = release_files["note_path"].with_name("apply-receipt.json")
    result = release.apply_release(
        **_plan_args(release_files),
        confirm_release_id=release_files["note"]["release_id"],
        receipt=receipt,
        runner=runner,
        store_factory=_factory(store, calls),
    )

    assert result["applied"] is True
    assert (
        release_files["live_env"].read_bytes()
        == release_files["env_source"].read_bytes()
    )
    assert (
        release_files["live_manifest"].read_bytes()
        == release_files["manifest_source"].read_bytes()
    )
    assert store.activation_calls
    assert stat.S_IMODE(receipt.stat().st_mode) == 0o600
    assert json.loads(receipt.read_bytes())["transaction_state"] == "completed"
    assert runner.fetch_rounds == {"host": 3, "worker": 3, "pipeline": 3}
    assert [call[1] for call in runner.calls if call[:1] == ("/bin/launchctl",)].count(
        "enable"
    ) == len(release.REQUIRED_RESIDENTS)
    assert [call[1] for call in runner.calls if call[:1] == ("/bin/launchctl",)].count(
        "disable"
    ) == len(release.DISABLED_RESIDENTS)
    assert not (release_files["home"] / "runtime" / release.RELEASE_LOCK_NAME).exists()


def test_apply_lock_is_exclusive_and_removed_after_failure(release_files):
    lock_path = release_files["home"] / "runtime" / release.RELEASE_LOCK_NAME
    _write(lock_path, b'"held"\n')
    runner = FakeRunner(release_files)

    with pytest.raises(release.ReleaseError, match="release_apply_locked"):
        release.apply_release(
            **_plan_args(release_files),
            confirm_release_id=release_files["note"]["release_id"],
            runner=runner,
            store_factory=_factory(FakeStore(), []),
        )

    assert not runner.calls
    assert release_files["live_env"].read_bytes() == b"OLD_ENV=1\n"
    assert lock_path.exists()


def test_release_lock_recovers_only_owner_only_dead_pid_record(release_files):
    lock_path = release_files["home"] / "runtime" / release.RELEASE_LOCK_NAME
    _write_json(
        lock_path,
        {"pid": 999_999_999, "release_id": "rca-stale-release-20260817"},
    )
    lock = release._acquire_release_lock(
        release_files["home"], release_files["note"]["release_id"]
    )
    try:
        assert json.loads(lock_path.read_bytes()) == {
            "pid": os.getpid(),
            "release_id": release_files["note"]["release_id"],
        }
    finally:
        release._release_release_lock(lock)
    assert not lock_path.exists()


def test_release_lock_refuses_live_pid_and_non_owner_mode(release_files):
    lock_path = release_files["home"] / "runtime" / release.RELEASE_LOCK_NAME
    _write_json(
        lock_path,
        {"pid": os.getpid(), "release_id": "rca-live-release-20260817"},
    )
    with pytest.raises(release.ReleaseError, match="release_apply_locked"):
        release._acquire_release_lock(
            release_files["home"], release_files["note"]["release_id"]
        )
    assert lock_path.exists()

    lock_path.unlink()
    _write_json(
        lock_path,
        {"pid": 999_999_999, "release_id": "rca-stale-release-20260817"},
        mode=0o640,
    )
    with pytest.raises(release.ReleaseError, match="release_apply_locked"):
        release._acquire_release_lock(
            release_files["home"], release_files["note"]["release_id"]
        )
    assert lock_path.exists()


def test_release_lock_refuses_dead_pid_inode_held_by_another_recovery(release_files):
    lock_path = release_files["home"] / "runtime" / release.RELEASE_LOCK_NAME
    _write_json(
        lock_path,
        {"pid": 999_999_999, "release_id": "rca-stale-release-20260817"},
    )
    descriptor = os.open(lock_path, os.O_RDWR)
    release.fcntl.flock(descriptor, release.fcntl.LOCK_EX | release.fcntl.LOCK_NB)
    try:
        with pytest.raises(release.ReleaseError, match="release_apply_locked"):
            release._acquire_release_lock(
                release_files["home"], release_files["note"]["release_id"]
            )
        assert lock_path.exists()
    finally:
        os.close(descriptor)

    lock = release._acquire_release_lock(
        release_files["home"], release_files["note"]["release_id"]
    )
    release._release_release_lock(lock)
    assert not lock_path.exists()


def test_apply_rechecks_gitlab_before_install(release_files):
    runner = FakeRunner(release_files, bad_ref_face="host", bad_ref_round=2)
    store = FakeStore()

    with pytest.raises(release.ReleaseError, match="gitlab_identity_mismatch"):
        release.apply_release(
            **_plan_args(release_files),
            confirm_release_id=release_files["note"]["release_id"],
            runner=runner,
            store_factory=_factory(store, []),
        )

    assert release_files["live_env"].read_bytes() == b"OLD_ENV=1\n"
    assert release_files["live_manifest"].read_bytes() == b'{"old":true}\n'
    assert not store.activation_calls
    receipt = release_files["note_path"].with_name(
        f"{release_files['note']['release_id']}.minimal-release-apply.json"
    )
    failed = json.loads(receipt.read_bytes())
    assert failed["transaction_state"] == "failed"
    assert failed["effect_started"] is False
    assert failed["resident_stop"]["attempted"] is False
    assert runner.loaded == {row[0] for row in release.REQUIRED_RESIDENTS}
    assert not (release_files["home"] / "runtime" / release.RELEASE_LOCK_NAME).exists()


def test_apply_rechecks_runtime_before_install(release_files, monkeypatch):
    original = release.runtime_readback
    calls = 0

    def changing_runtime(*args, **kwargs):
        nonlocal calls
        calls += 1
        result = original(*args, **kwargs)
        if calls == 2:
            return {**result, "tree": "0" * 40}
        return result

    monkeypatch.setattr(release, "runtime_readback", changing_runtime)
    store = FakeStore()
    with pytest.raises(release.ReleaseError, match="runtime_changed_during_apply"):
        release.apply_release(
            **_plan_args(release_files),
            confirm_release_id=release_files["note"]["release_id"],
            runner=FakeRunner(release_files),
            store_factory=_factory(store, []),
        )

    assert release_files["live_env"].read_bytes() == b"OLD_ENV=1\n"
    assert release_files["live_manifest"].read_bytes() == b'{"old":true}\n'
    assert not store.activation_calls


def test_apply_restores_env_when_manifest_replace_fails(release_files, monkeypatch):
    real_replace = release.os.replace

    def fail_manifest_replace(source, target):
        if (
            Path(target) == release_files["live_manifest"]
            and ".minimal-candidate-" in Path(source).name
        ):
            raise OSError("injected manifest rename failure")
        return real_replace(source, target)

    monkeypatch.setattr(release.os, "replace", fail_manifest_replace)
    store = FakeStore()
    with pytest.raises(release.ReleaseError, match="install_pair_failed"):
        release.apply_release(
            **_plan_args(release_files),
            confirm_release_id=release_files["note"]["release_id"],
            runner=FakeRunner(release_files),
            store_factory=_factory(store, []),
        )

    assert release_files["live_env"].read_bytes() == b"OLD_ENV=1\n"
    assert release_files["live_manifest"].read_bytes() == b'{"old":true}\n'
    assert not store.activation_calls
    assert not list(release_files["home"].rglob("*.minimal-candidate-*"))
    assert not list(release_files["home"].rglob("*.minimal-preimage-*"))
    assert not (release_files["home"] / "runtime" / release.RELEASE_LOCK_NAME).exists()


def test_apply_restores_both_files_when_install_readback_mismatches(
    release_files, monkeypatch
):
    real_read = release._read

    def mismatch_manifest_readback(path, code, **kwargs):
        if code == "install_readback" and Path(path) == release_files["live_manifest"]:
            return b'{"unexpected":true}\n'
        return real_read(path, code, **kwargs)

    monkeypatch.setattr(release, "_read", mismatch_manifest_readback)
    store = FakeStore()
    with pytest.raises(release.ReleaseError, match="install_readback_mismatch"):
        release.apply_release(
            **_plan_args(release_files),
            confirm_release_id=release_files["note"]["release_id"],
            runner=FakeRunner(release_files),
            store_factory=_factory(store, []),
        )

    assert release_files["live_env"].read_bytes() == b"OLD_ENV=1\n"
    assert release_files["live_manifest"].read_bytes() == b'{"old":true}\n'
    assert not store.activation_calls
    assert not list(release_files["home"].rglob("*.minimal-candidate-*"))
    assert not list(release_files["home"].rglob("*.minimal-preimage-*"))
    assert not (release_files["home"] / "runtime" / release.RELEASE_LOCK_NAME).exists()


def test_apply_preserves_recovery_preimage_when_rollback_itself_fails(
    release_files, monkeypatch
):
    real_replace = release.os.replace

    def fail_manifest_and_env_rollback(source, target):
        source = Path(source)
        target = Path(target)
        if (
            target == release_files["live_manifest"]
            and ".minimal-candidate-" in source.name
        ):
            raise OSError("injected manifest install failure")
        if (
            target == release_files["live_env"]
            and ".minimal-preimage-" in source.name
        ):
            raise OSError("injected env rollback failure")
        return real_replace(source, target)

    monkeypatch.setattr(release.os, "replace", fail_manifest_and_env_rollback)
    receipt = release_files["note_path"].with_name("rollback-failed.json")
    runner = FakeRunner(release_files)
    with pytest.raises(release.ReleaseError, match="install_pair_failed"):
        release.apply_release(
            **_plan_args(release_files),
            confirm_release_id=release_files["note"]["release_id"],
            receipt=receipt,
            runner=runner,
            store_factory=_factory(FakeStore(), []),
        )

    failed = json.loads(receipt.read_bytes())
    recovery = [Path(path) for path in failed["rollback_recovery_paths"]]
    assert failed["transaction_state"] == "failed"
    assert failed["artifacts_mutated"] is True
    assert failed["artifacts_restored"] is False
    assert failed["rollback_error"] == "install_rollback_failed"
    assert recovery and all(path.exists() for path in recovery)
    assert not list(release_files["home"].rglob("*.minimal-candidate-*"))
    assert release_files["live_env"].read_bytes() == release_files["env_source"].read_bytes()
    assert release_files["live_manifest"].read_bytes() == b'{"old":true}\n'
    assert failed["resident_stop"]["all_stopped"] is True


def test_apply_rejects_release_note_change_before_install(release_files, monkeypatch):
    runner = FakeRunner(release_files)
    store = FakeStore()
    calls = []
    original_plan = release.build_plan

    def plan_then_change(**kwargs):
        result = original_plan(**kwargs)
        note = json.loads(release_files["note_path"].read_text())
        note["changed_after_plan"] = True
        _write_json(release_files["note_path"], note)
        return result

    monkeypatch.setattr(release, "build_plan", plan_then_change)
    with pytest.raises(release.ReleaseError, match="release_note_changed"):
        release.apply_release(
            **_plan_args(release_files),
            confirm_release_id=release_files["note"]["release_id"],
            runner=runner,
            store_factory=_factory(store, calls),
        )
    assert release_files["live_env"].read_bytes() == b"OLD_ENV=1\n"
    assert not store.activation_calls


def test_apply_rechecks_release_note_after_resident_readback(
    release_files, monkeypatch
):
    store = FakeStore()

    def restart_then_change(*_args, **_kwargs):
        note = json.loads(release_files["note_path"].read_text())
        note["changed_after_restart"] = True
        _write_json(release_files["note_path"], note)
        return _synthetic_resident_profile(release_files)

    monkeypatch.setattr(release, "_restart", restart_then_change)
    receipt = release_files["note_path"].with_name("changed-note-receipt.json")
    with pytest.raises(release.ReleaseError, match="release_note_changed"):
        release.apply_release(
            **_plan_args(release_files),
            confirm_release_id=release_files["note"]["release_id"],
            receipt=receipt,
            runner=FakeRunner(release_files),
            store_factory=_factory(store, []),
        )
    assert store.activation_calls
    failed = json.loads(receipt.read_bytes())
    assert failed["transaction_state"] == "failed"
    assert failed["activation_committed"] is True
    assert failed["resident_stop"]["all_stopped"] is True


def test_apply_reserves_started_receipt_and_rolls_back_preactivation_failure(
    release_files, monkeypatch
):
    receipt = release_files["note_path"].with_name("preactivation-failed.json")
    base_runner = FakeRunner(release_files)
    receipt_identity = []

    def runner(command):
        command = tuple(command)
        if command[:2] == ("/bin/launchctl", "bootout") and not receipt_identity:
            started = json.loads(receipt.read_bytes())
            assert started["transaction_state"] == "started"
            assert stat.S_IMODE(receipt.stat().st_mode) == 0o600
            receipt_identity.append((receipt.stat().st_dev, receipt.stat().st_ino))
        return base_runner(command)

    real_assert_stopped = release._assert_all_residents_stopped
    calls = 0

    def fail_before_activation(current_runner):
        nonlocal calls
        calls += 1
        if calls == 3:
            raise release.ReleaseError("preactivation_injected")
        return real_assert_stopped(current_runner)

    monkeypatch.setattr(release, "_assert_all_residents_stopped", fail_before_activation)
    store = FakeStore()
    with pytest.raises(release.ReleaseError, match="preactivation_injected"):
        release.apply_release(
            **_plan_args(release_files),
            confirm_release_id=release_files["note"]["release_id"],
            receipt=receipt,
            runner=runner,
            store_factory=_factory(store, []),
        )

    failed = json.loads(receipt.read_bytes())
    assert (receipt.stat().st_dev, receipt.stat().st_ino) == receipt_identity[0]
    assert failed["transaction_state"] == "failed"
    assert failed["error_code"] == "preactivation_injected"
    assert failed["artifacts_installed"] is True
    assert failed["artifacts_restored"] is True
    assert failed["activation_attempted"] is False
    assert failed["resident_stop"]["all_stopped"] is True
    assert release_files["live_env"].read_bytes() == b"OLD_ENV=1\n"
    assert release_files["live_manifest"].read_bytes() == b'{"old":true}\n'
    assert not store.activation_calls


def test_apply_postactivation_failure_keeps_epoch_and_leaves_every_service_stopped(
    release_files, monkeypatch
):
    receipt = release_files["note_path"].with_name("postactivation-failed.json")
    runner = FakeRunner(release_files)
    store = FakeStore()

    def partially_start_then_fail(*_args, **_kwargs):
        label = release.REQUIRED_RESIDENTS[0][0]
        runner((
            "/bin/launchctl",
            "bootstrap",
            f"gui/{os.getuid()}",
            str(release_files["home"].parent / f"Library/LaunchAgents/{label}.plist"),
        ))
        raise release.ReleaseError("postactivation_injected")

    monkeypatch.setattr(release, "_restart", partially_start_then_fail)
    with pytest.raises(release.ReleaseError, match="postactivation_injected"):
        release.apply_release(
            **_plan_args(release_files),
            confirm_release_id=release_files["note"]["release_id"],
            receipt=receipt,
            runner=runner,
            store_factory=_factory(store, []),
        )

    failed = json.loads(receipt.read_bytes())
    assert failed["transaction_state"] == "failed"
    assert failed["activation_attempted"] is True
    assert failed["activation_committed"] is True
    assert failed["artifacts_restored"] is False
    assert failed["resident_stop"]["all_stopped"] is True
    assert not runner.loaded
    assert store.current["epoch_id"] == release_files["note"]["activation"]["epoch_id"]
    assert release_files["live_env"].read_bytes() == release_files["env_source"].read_bytes()
    assert (
        release_files["live_manifest"].read_bytes()
        == release_files["manifest_source"].read_bytes()
    )


def test_apply_activation_store_rollback_restores_artifacts(release_files, monkeypatch):
    receipt = release_files["note_path"].with_name("activation-rollback.json")
    runner = FakeRunner(release_files)
    store = FakeStore()
    monkeypatch.setattr(
        release,
        "_activation_apply",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            release.ReleaseError("activation_predecessor_binding_changed")
        ),
    )

    with pytest.raises(
        release.ReleaseError, match="activation_predecessor_binding_changed"
    ):
        release.apply_release(
            **_plan_args(release_files),
            confirm_release_id=release_files["note"]["release_id"],
            receipt=receipt,
            runner=runner,
            store_factory=_factory(store, []),
        )

    failed = json.loads(receipt.read_bytes())
    assert failed["activation_attempted"] is True
    assert failed["activation_outcome_known"] is True
    assert failed["activation_committed"] is False
    assert failed["artifacts_restored"] is True
    assert failed["resident_stop"]["all_stopped"] is True
    assert release_files["live_env"].read_bytes() == b"OLD_ENV=1\n"
    assert release_files["live_manifest"].read_bytes() == b'{"old":true}\n'


def test_apply_keyboard_interrupt_writes_failed_receipt_and_stops_all(release_files):
    receipt = release_files["note_path"].with_name("keyboard-interrupt.json")
    base_runner = FakeRunner(release_files)
    interrupted = False

    def runner(command):
        nonlocal interrupted
        result = base_runner(command)
        if command[:2] == ("/bin/launchctl", "bootout") and not interrupted:
            interrupted = True
            raise KeyboardInterrupt
        return result

    with pytest.raises(KeyboardInterrupt):
        release.apply_release(
            **_plan_args(release_files),
            confirm_release_id=release_files["note"]["release_id"],
            receipt=receipt,
            runner=runner,
            store_factory=_factory(FakeStore(), []),
        )

    failed = json.loads(receipt.read_bytes())
    assert failed["transaction_state"] == "failed"
    assert failed["resident_stop"]["all_stopped"] is True
    assert not base_runner.loaded
    assert not (release_files["home"] / "runtime" / release.RELEASE_LOCK_NAME).exists()


def test_apply_sigterm_handler_converges_to_terminal_failed(release_files, monkeypatch):
    receipt = release_files["note_path"].with_name("sigterm.json")
    runner = FakeRunner(release_files)
    prior = signal.getsignal(signal.SIGTERM)

    def interrupt_from_installed_handler(*_args, **_kwargs):
        handler = signal.getsignal(signal.SIGTERM)
        assert callable(handler)
        handler(signal.SIGTERM, None)

    monkeypatch.setattr(release, "_quiesce_residents", interrupt_from_installed_handler)
    with pytest.raises(release.ReleaseError, match="apply_sigterm"):
        release.apply_release(
            **_plan_args(release_files),
            confirm_release_id=release_files["note"]["release_id"],
            receipt=receipt,
            runner=runner,
            store_factory=_factory(FakeStore(), []),
        )

    failed = json.loads(receipt.read_bytes())
    assert failed["error_code"] == "apply_sigterm"
    assert failed["resident_stop"]["all_stopped"] is True
    assert signal.getsignal(signal.SIGTERM) == prior


def test_apply_cleanup_failure_never_publishes_completed_receipt(
    release_files, monkeypatch
):
    receipt = release_files["note_path"].with_name("cleanup-failed.json")
    runner = FakeRunner(release_files)
    residents = _synthetic_resident_profile(release_files)
    monkeypatch.setattr(release, "_restart", lambda *_args, **_kwargs: residents)
    monkeypatch.setattr(
        release, "_resident_profile_readback", lambda *_args, **_kwargs: residents
    )
    real_cleanup = release._cleanup_staged
    calls = 0

    def fail_once(staged):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("injected cleanup failure")
        return real_cleanup(staged)

    monkeypatch.setattr(release, "_cleanup_staged", fail_once)
    with pytest.raises(OSError, match="injected cleanup failure"):
        release.apply_release(
            **_plan_args(release_files),
            confirm_release_id=release_files["note"]["release_id"],
            receipt=receipt,
            runner=runner,
            store_factory=_factory(FakeStore(), []),
        )

    failed = json.loads(receipt.read_bytes())
    assert failed["transaction_state"] == "failed"
    assert failed["resident_stop"]["all_stopped"] is True
    assert not runner.loaded
    assert not (release_files["home"] / "runtime" / release.RELEASE_LOCK_NAME).exists()


def test_apply_terminal_receipt_write_failure_publishes_fallback_marker(
    release_files, monkeypatch
):
    receipt = release_files["note_path"].with_name("terminal-write-failed.json")
    marker = receipt.with_name(f"{receipt.name}.terminal-failure.json")
    runner = FakeRunner(release_files)
    real_quiesce = release._quiesce_residents
    real_write = release._write_reserved_receipt

    def quiesce_then_fail(current_runner):
        real_quiesce(current_runner)
        raise release.ReleaseError("injected_after_effect")

    def fail_terminal_write(handle, value):
        if value.get("transaction_state") == "failed":
            raise release.ReleaseError("receipt_write_failed")
        return real_write(handle, value)

    monkeypatch.setattr(release, "_quiesce_residents", quiesce_then_fail)
    monkeypatch.setattr(release, "_write_reserved_receipt", fail_terminal_write)

    with pytest.raises(release.ReleaseError, match="terminal_receipt_write_failed"):
        release.apply_release(
            **_plan_args(release_files),
            confirm_release_id=release_files["note"]["release_id"],
            receipt=receipt,
            runner=runner,
            store_factory=_factory(FakeStore(), []),
        )

    primary = json.loads(receipt.read_bytes())
    fallback = json.loads(marker.read_bytes())
    assert primary["transaction_state"] == "started"
    assert stat.S_IMODE(marker.stat().st_mode) == 0o600
    assert fallback["schema_version"] == release.TERMINAL_FAILURE_SCHEMA
    assert fallback["transaction_state"] == "failed"
    assert fallback["primary_receipt_path"] == str(receipt)
    assert fallback["primary_receipt_error"] == "receipt_write_failed"
    assert fallback["failure"]["error_code"] == "injected_after_effect"
    assert fallback["failure"]["resident_stop"]["all_stopped"] is True
    assert not runner.loaded
    assert not (release_files["home"] / "runtime" / release.RELEASE_LOCK_NAME).exists()


def test_apply_reports_when_terminal_receipt_and_fallback_marker_both_fail(
    release_files, monkeypatch
):
    receipt = release_files["note_path"].with_name("all-terminal-writes-failed.json")
    marker = receipt.with_name(f"{receipt.name}.terminal-failure.json")
    runner = FakeRunner(release_files)
    real_quiesce = release._quiesce_residents
    real_write = release._write_reserved_receipt

    def quiesce_then_fail(current_runner):
        real_quiesce(current_runner)
        raise release.ReleaseError("injected_after_effect")

    def fail_terminal_write(handle, value):
        if value.get("transaction_state") == "failed":
            raise release.ReleaseError("receipt_write_failed")
        return real_write(handle, value)

    monkeypatch.setattr(release, "_quiesce_residents", quiesce_then_fail)
    monkeypatch.setattr(release, "_write_reserved_receipt", fail_terminal_write)
    monkeypatch.setattr(
        release,
        "_write_terminal_failure_marker",
        lambda *_args: (_ for _ in ()).throw(
            release.ReleaseError("terminal_failure_marker_unavailable")
        ),
    )

    with pytest.raises(
        release.ReleaseError, match="terminal_receipt_and_marker_write_failed"
    ):
        release.apply_release(
            **_plan_args(release_files),
            confirm_release_id=release_files["note"]["release_id"],
            receipt=receipt,
            runner=runner,
            store_factory=_factory(FakeStore(), []),
        )

    assert json.loads(receipt.read_bytes())["transaction_state"] == "started"
    assert not marker.exists()
    assert not runner.loaded
    assert not (release_files["home"] / "runtime" / release.RELEASE_LOCK_NAME).exists()


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf")])
def test_apply_rejects_nonpositive_or_nonfinite_restart_timeout(
    release_files, timeout
):
    runner = FakeRunner(release_files)
    with pytest.raises(release.ReleaseError, match="restart_timeout_invalid"):
        release.apply_release(
            **_plan_args(release_files),
            confirm_release_id=release_files["note"]["release_id"],
            restart_timeout=timeout,
            runner=runner,
            store_factory=_factory(FakeStore(), []),
        )
    assert not runner.calls


def test_quiesce_persists_profile_and_recovers_loaded_service_without_pid(
    release_files, monkeypatch
):
    first = release.REQUIRED_RESIDENTS[0][0]
    pids = {
        row[0]: (None if row[0] == first else 1000 + index)
        for index, row in enumerate(release.REQUIRED_RESIDENTS)
    }
    pids.update({
        label: 2000 + index
        for index, label in enumerate(release.DISABLED_RESIDENTS)
    })
    runner = FakeRunner(
        release_files,
        pids=pids,
        loaded_without_pid=(first,),
    )

    quiesce = release._quiesce_residents(runner)

    assert quiesce["previous"][first] == {
        "label": first,
        "loaded": True,
        "pid": None,
    }
    assert not runner.loaded
    assert all(runner.disabled[label] is False for label, *_ in release.REQUIRED_RESIDENTS)
    assert all(runner.disabled[label] is True for label in release.DISABLED_RESIDENTS)
    assert any(
        call[:2] == ("/bin/launchctl", "bootout") and call[-1].endswith(first)
        for call in runner.calls
    )

    residents = _synthetic_resident_profile(release_files)
    monkeypatch.setattr(
        release, "_resident_profile_readback", lambda *_args, **_kwargs: residents
    )
    note = json.loads(release_files["note_path"].read_bytes())
    note["_path"] = str(release_files["note_path"])
    assert (
        release._restart(
            note,
            release._sha(release_files["note_raw"]),
            release_files["home"],
            runner,
            lambda _pid: None,
            0,
            quiesce,
        )
        == residents
    )
    assert runner.loaded == {row[0] for row in release.REQUIRED_RESIDENTS}
    assert not runner.loaded & set(release.DISABLED_RESIDENTS)


class FakeProcess:
    def __init__(self, *, cwd, script, created):
        self._cwd = cwd
        self._script = script
        self._created = created

    def cwd(self):
        return self._cwd

    def cmdline(self):
        return ["/usr/bin/python3", self._script]

    def create_time(self):
        return self._created


def _runtime_identity(label, pid, root, script, created):
    return {
        "service_label": label,
        "pid": pid,
        "process_create_time": created,
        "boot_time": 1.0,
        "executable": "/usr/bin/python3",
        "script": script,
        "cwd": root,
        "script_sha256": "a" * 64,
        "runtime_files_sha256": "b" * 64,
        "public_config_sha256": "c" * 64,
        "loaded_runtime_sha256": "d" * 64,
    }


def _resident_fixture(data, *, missing_release_label=""):
    now = datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)
    stamp = now.isoformat()
    note = json.loads(data["note_path"].read_text())
    note["_path"] = str(data["note_path"])
    note_sha = release._sha(data["note_path"].read_bytes())
    pids = {}
    processes = {}
    for index, (label, relative_script, health_path, freshness, schema) in enumerate(
        release.REQUIRED_RESIDENTS
    ):
        pid = 2000 + index
        created = 100.0 + index
        script = str(data["runtime_root"] / relative_script)
        pids[label] = pid
        processes[pid] = FakeProcess(
            cwd=str(data["runtime_root"]), script=script, created=created
        )
        if not schema:
            health = {"pid": pid, "gateway_state": "running", freshness: stamp}
        else:
            health = {
                "schema_version": schema,
                "healthy": True,
                "state": "running",
                freshness: stamp,
                "runtime_identity": _runtime_identity(
                    label, pid, str(data["runtime_root"]), script, created
                ),
            }
            if (
                label in release.RELEASE_HEALTH_RESIDENTS
                and label != missing_release_label
            ):
                health["release"] = {
                    "epoch_id": note["activation"]["epoch_id"],
                    "release_id": note["release_id"],
                    "release_fingerprint_sha256": note["release_fingerprint_sha256"],
                    "release_note_path": str(data["note_path"]),
                    "release_note_sha256": note_sha,
                    "runtime_root": str(data["runtime_root"]),
                    "runtime_commit": note["release_identity"]["host"]["commit"],
                    "runtime_tree": note["release_identity"]["host"]["tree"],
                    "live_manifest_sha256": note["runtime_projection"][
                        "live_manifest_sha256"
                    ],
                    "live_env_sha256": note["runtime_projection"]["env_sha256"],
                }
        _write_json(data["home"] / health_path, health)
    return note, note_sha, now, pids, processes


def test_resident_readback_requires_exact_delivery_release_and_runtime_identity(
    release_files,
):
    note, note_sha, now, pids, processes = _resident_fixture(release_files)
    result = release.resident_readback(
        note,
        note_sha,
        release_files["home"],
        runner=FakeRunner(release_files, pids),
        process_factory=processes.__getitem__,
        now=now,
        previous={label: pid - 100 for label, pid in pids.items()},
    )
    assert result["required"][0]["freshness_kind"] == "restart_transition"
    assert result["required"][1]["freshness_kind"] == "health_heartbeat"

    label = "local.pnc.rca-delivery-collector"
    note, note_sha, now, pids, processes = _resident_fixture(
        release_files, missing_release_label=label
    )
    with pytest.raises(release.ReleaseError, match="resident_health_invalid"):
        release.resident_readback(
            note,
            note_sha,
            release_files["home"],
            runner=FakeRunner(release_files, pids),
            process_factory=processes.__getitem__,
            now=now,
        )


def _canary_state(data, **changes):
    issue = data["note"]["canary"]["issue_id"]
    submission_key = "g1q3-rca-s1-" + "e" * 64
    value = {
        "batch_id": data["note"]["canary"]["batch_id"],
        "acceptance_axis": "transport",
        "status": "completed",
        "selected_issue_ids": [issue],
        "runtime_commit": data["identity"]["host"]["commit"],
        "runtime_tree": data["identity"]["host"]["tree"],
        "items": {
            issue: {
                "status": "accepted",
                "submission_key": submission_key,
                "approval": {
                    "acceptance_axis": "transport",
                    "official_readback_source": "read_after_write",
                    "acceptance": {
                        "transport": {
                            "status": "pass",
                            "official_comment_id": "oc_comment_123",
                            "official_field_keys": ["field_8c912e", "field_9193cb"],
                            "official_readback_source": "read_after_write",
                        },
                        "causal_attribution": {
                            "status": "not_ready",
                            "reason": "causal_quality_not_satisfied",
                        },
                    },
                    "execution_identity_readback": _execution_readback(
                        data, submission_key
                    ),
                },
            }
        },
    }
    value.update(changes)
    _write_json(data["canary_path"], value)
    return value


class FakeDeliveryStore:
    def __init__(self, projection=None, error=None):
        self.projection = projection
        self.error = error
        self.calls = []

    def canonical_canary_readback(self, **kwargs):
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return json.loads(json.dumps(self.projection))


def _canary_db_projection(data, state):
    issue = data["note"]["canary"]["issue_id"]
    item = state["items"][issue]
    approval = item["approval"]
    return {
        "schema_version": "pnc_rca_canonical_canary_readback_v1",
        "batch_id": data["note"]["canary"]["batch_id"],
        "issue_id": issue,
        "submission_key": item["submission_key"],
        "activation_epoch_id": data["note"]["activation"]["epoch_id"],
        "trigger": {"state": "submitted"},
        "outbox": {"status": "completed"},
        "watch": {"state": "delivery_created"},
        "delivery_job": {"status": "delivered", "outcome": "success"},
        "required_effects": [{"status": "succeeded", "write_phase": "settled"}],
        "transport": json.loads(json.dumps(approval["acceptance"]["transport"])),
        "execution_identity_readback": json.loads(
            json.dumps(approval["execution_identity_readback"])
        ),
    }


def _delivery_factory(store, calls=None):
    def factory(path):
        if calls is not None:
            calls.append(path)
        return store

    return factory


def _configure_verify_fixture(
    data,
    monkeypatch,
    *,
    activation_status=None,
    resident_readback=None,
    receipt_activation=None,
    receipt_residents=None,
):
    state = _canary_state(data)
    store = FakeDeliveryStore(_canary_db_projection(data, state))
    data["live_env"].write_bytes(data["env_source"].read_bytes())
    data["live_manifest"].write_bytes(data["manifest_source"].read_bytes())
    monkeypatch.setattr(release, "gitlab_readback", lambda *_args: {})
    monkeypatch.setattr(release, "runtime_readback", lambda *_args: {})
    activation = receipt_activation or {
        "epoch_id": data["note"]["activation"]["epoch_id"]
    }
    residents = receipt_residents or _synthetic_resident_profile(data)
    monkeypatch.setattr(
        release,
        "_activation_status",
        activation_status or (lambda *_args: activation),
    )
    monkeypatch.setattr(
        release,
        "_resident_profile_readback",
        resident_readback or (lambda *_args, **_kwargs: residents),
    )
    receipt_path = data["note_path"].with_name("completed-apply-receipt.json")
    previous = {
        label: {
            "label": label,
            "loaded": label in {row[0] for row in release.REQUIRED_RESIDENTS},
            "pid": 1000 + index
            if label in {row[0] for row in release.REQUIRED_RESIDENTS}
            else None,
        }
        for index, label in enumerate(release._all_resident_labels())
    }
    transition = release._resident_transition({"previous": previous}, residents)
    _write_json(
        receipt_path,
        {
            "schema_version": release.RECEIPT_SCHEMA,
            "transaction_state": "completed",
            "ok": True,
            "mode": "apply",
            "applied": True,
            "apply_pid": os.getpid(),
            "started_at": "2026-08-17T10:00:00+00:00",
            "completed_at": "2026-08-17T10:01:00+00:00",
            "applied_at": "2026-08-17T10:01:00+00:00",
            "release_id": data["note"]["release_id"],
            "receipt_path": str(receipt_path),
            "release_note": {
                "path": str(data["note_path"]),
                "sha256": release._sha(data["note_path"].read_bytes()),
            },
            "gitlab": {},
            "runtime": {},
            "live_projection": data["note"]["runtime_projection"],
            "artifacts": [
                {
                    "target": str(data["live_env"]),
                    "before_sha256": "d" * 64,
                    "after_sha256": data["note"]["runtime_projection"][
                        "env_sha256"
                    ],
                    "changed": True,
                },
                {
                    "target": str(data["live_manifest"]),
                    "before_sha256": "e" * 64,
                    "after_sha256": data["note"]["runtime_projection"][
                        "live_manifest_sha256"
                    ],
                    "changed": True,
                },
            ],
            "activation": activation,
            "activation_changed": True,
            "resident_profile": residents,
            "resident_transition": transition,
            "single_canary": {
                "issue_id": data["note"]["canary"]["issue_id"],
                "state_path": data["note"]["canary"]["state_path"],
                "submitted_by_driver": False,
            },
        },
    )
    data["apply_receipt"] = receipt_path
    return store


def _verify_args(data):
    return {
        "release_note": data["note_path"],
        "apply_receipt": data["apply_receipt"],
        "home": data["home"],
    }


def test_canary_gates_transport_but_only_reports_causal_attribution(release_files):
    state = _canary_state(release_files)
    store = FakeDeliveryStore(_canary_db_projection(release_files, state))
    result = release.canary_readback(
        release_files["note"],
        release._sha(release_files["note_raw"]),
        delivery_store_factory=_delivery_factory(store),
    )
    assert result["transport"]["status"] == "pass"
    assert result["causal_attribution"]["status"] == "not_ready"
    assert result["causal_attribution_required"] is False
    assert result["execution_identity_readback"]["worker"]["receipt_path"].endswith(
        "/result.md"
    )
    assert result["canonical_db_readback"]["transport"] == result["transport"]
    assert store.calls == [
        {
            "batch_id": release_files["note"]["canary"]["batch_id"],
            "issue_id": release_files["note"]["canary"]["issue_id"],
            "submission_key": state["items"][
                release_files["note"]["canary"]["issue_id"]
            ]["submission_key"],
            "activation_epoch_id": release_files["note"]["activation"]["epoch_id"],
        }
    ]


@pytest.mark.parametrize(
    "case", ["batch", "axis", "source", "partial_readback", "two_items"]
)
def test_canary_rejects_non_transport_or_non_readback_acceptance(release_files, case):
    value = _canary_state(release_files)
    store = FakeDeliveryStore(_canary_db_projection(release_files, value))
    issue = release_files["note"]["canary"]["issue_id"]
    if case == "batch":
        value["batch_id"] = "canary-old-release-20260817"
    elif case == "axis":
        value["acceptance_axis"] = "causal_attribution"
    elif case == "source":
        value["items"][issue]["approval"]["official_readback_source"] = "write_response"
        value["items"][issue]["approval"]["acceptance"]["transport"][
            "official_readback_source"
        ] = "write_response"
    elif case == "partial_readback":
        value["items"][issue]["approval"]["acceptance"]["transport"][
            "official_field_keys"
        ] = ["field_8c912e"]
    else:
        value["selected_issue_ids"].append("7049076164")
        value["items"]["7049076164"] = value["items"][issue]
    _write_json(release_files["canary_path"], value)
    with pytest.raises(release.ReleaseError, match="canary_reference_invalid"):
        release.canary_readback(
            release_files["note"],
            release._sha(release_files["note_raw"]),
            delivery_store_factory=_delivery_factory(store),
        )


@pytest.mark.parametrize("case", ["transport", "execution_identity"])
def test_canary_rejects_owner_state_tamper_against_db_projection(release_files, case):
    state = _canary_state(release_files)
    projection = _canary_db_projection(release_files, state)
    issue = release_files["note"]["canary"]["issue_id"]
    if case == "transport":
        state["items"][issue]["approval"]["acceptance"]["transport"][
            "official_comment_id"
        ] = "oc_owner_tamper"
    else:
        state["items"][issue]["approval"]["execution_identity_readback"]["worker"][
            "commit"
        ] = "0" * 40
    _write_json(release_files["canary_path"], state)

    with pytest.raises(release.ReleaseError, match="canary_db_projection_mismatch"):
        release.canary_readback(
            release_files["note"],
            release._sha(release_files["note_raw"]),
            delivery_store_factory=_delivery_factory(FakeDeliveryStore(projection)),
        )


def test_canary_rejects_missing_canonical_db_evidence(release_files):
    _canary_state(release_files)

    with pytest.raises(release.ReleaseError, match="canary_db_readback_invalid"):
        release.canary_readback(
            release_files["note"],
            release._sha(release_files["note_raw"]),
            delivery_store_factory=_delivery_factory(
                FakeDeliveryStore(error=RuntimeError("missing canonical row"))
            ),
        )


def test_canary_validates_db_identity_after_exact_state_compare(release_files):
    state = _canary_state(release_files)
    issue = release_files["note"]["canary"]["issue_id"]
    identity = state["items"][issue]["approval"]["execution_identity_readback"]
    identity["pipeline"]["tree"] = "0" * 40
    _write_json(release_files["canary_path"], state)
    projection = _canary_db_projection(release_files, state)

    with pytest.raises(
        release.ReleaseError, match="execution_identity_readback_mismatch"
    ):
        release.canary_readback(
            release_files["note"],
            release._sha(release_files["note_raw"]),
            delivery_store_factory=_delivery_factory(FakeDeliveryStore(projection)),
        )


def _execution_readback(data, submission_key=None):
    identity = data["identity"]
    submission_key = submission_key or "g1q3-rca-s1-" + "e" * 64
    return {
        "schema_version": release.EXECUTION_READBACK_SCHEMA,
        "source": "host_collector_canonical_vm_receipts_v1",
        "release_id": data["note"]["release_id"],
        "activation_epoch_id": data["note"]["activation"]["epoch_id"],
        "release_fingerprint_sha256": data["note"]["release_fingerprint_sha256"],
        "release_note_sha256": release._sha(data["note_raw"]),
        "task_id": submission_key,
        "submission_key": submission_key,
        "worker": {
            "commit": identity["worker"]["commit"],
            "tree": identity["worker"]["tree"],
            "runtime_root": identity["worker"]["runtime_root"],
            "clean": True,
            "entrypoint_path": (
                identity["worker"]["runtime_root"] + "/vm_coding_worker_v2.py"
            ),
            "entrypoint_sha256": "b" * 64,
            "receipt_path": (
                f"/home/mini/.hermes/shared-state/tasks/{submission_key}/result.md"
            ),
            "receipt_sha256": "c" * 64,
        },
        "pipeline": {
            "commit": identity["pipeline"]["commit"],
            "tree": identity["pipeline"]["tree"],
            "runtime_root": identity["pipeline"]["runtime_root"],
            "clean": True,
            "entrypoint_path": identity["pipeline"]["runtime_root"] + "/rca_cli.py",
            "entrypoint_sha256": "d" * 64,
            "receipt_path": f"/mnt/tmp/{submission_key}/rca_service_result.json",
            "receipt_sha256": "e" * 64,
        },
        "report_service": {
            "manifest_path": identity["report_service"]["manifest_path"],
            "manifest_sha256": identity["report_service"]["manifest_sha256"],
            "pipeline_commit": identity["report_service"]["pipeline_commit"],
            "pipeline_tree": identity["report_service"]["pipeline_tree"],
            "runtime_root": identity["pipeline"]["runtime_root"],
            "report_script_sha256": "f" * 64,
        },
        "delivery_manifest": {
            "path": f"/mnt/tmp/{submission_key}/delivery_manifest.json",
            "sha256": "a" * 64,
        },
    }


@pytest.mark.parametrize(
    ("section", "field"),
    [("worker", "tree"), ("pipeline", "commit"), ("report_service", "manifest_sha256")],
)
def test_execution_identity_readback_validator_is_exact(release_files, section, field):
    value = _execution_readback(release_files)
    assert (
        release.validate_execution_identity_readback(
            release_files["note"],
            release._sha(release_files["note_raw"]),
            value,
            expected_task_id=value["task_id"],
        )
        == value
    )
    value[section][field] = "0" * len(value[section][field])
    with pytest.raises(
        release.ReleaseError, match="execution_identity_readback_mismatch"
    ):
        release.validate_execution_identity_readback(
            release_files["note"],
            release._sha(release_files["note_raw"]),
            value,
            expected_task_id=value["task_id"],
        )


def test_execution_identity_readback_rejects_entrypoint_traversal(release_files):
    value = _execution_readback(release_files)
    value["pipeline"]["entrypoint_path"] = (
        value["pipeline"]["runtime_root"] + "/../../tmp/operator-filled.py"
    )

    with pytest.raises(
        release.ReleaseError, match="execution_identity_readback_mismatch"
    ):
        release.validate_execution_identity_readback(
            release_files["note"],
            release._sha(release_files["note_raw"]),
            value,
            expected_task_id=value["task_id"],
        )


def test_verify_hard_fails_without_collector_execution_readback(
    release_files, monkeypatch
):
    store = _configure_verify_fixture(release_files, monkeypatch)
    calls = []
    state = json.loads(release_files["canary_path"].read_bytes())
    issue = release_files["note"]["canary"]["issue_id"]
    del state["items"][issue]["approval"]["execution_identity_readback"]
    _write_json(release_files["canary_path"], state)

    with pytest.raises(release.ReleaseError, match="canary_db_projection_mismatch"):
        release.verify_release(
            **_verify_args(release_files),
            delivery_store_factory=_delivery_factory(store, calls),
        )
    assert calls == [release_files["control_db"]]


def test_verify_uses_release_lock(release_files, monkeypatch):
    store = _configure_verify_fixture(release_files, monkeypatch)
    lock = release._acquire_release_lock(
        release_files["home"], release_files["note"]["release_id"]
    )
    try:
        with pytest.raises(release.ReleaseError, match="release_apply_locked"):
            release.verify_release(
                **_verify_args(release_files),
                delivery_store_factory=_delivery_factory(store),
            )
    finally:
        release._release_release_lock(lock)


def test_verify_requires_completed_owner_receipt_and_current_apply_pids(
    release_files, monkeypatch
):
    store = _configure_verify_fixture(release_files, monkeypatch)
    receipt = release_files["apply_receipt"]
    receipt.unlink()
    with pytest.raises(release.ReleaseError, match="apply_receipt_unavailable"):
        release.verify_release(
            **_verify_args(release_files),
            delivery_store_factory=_delivery_factory(store),
        )


@pytest.mark.parametrize(
    "tamper",
    [
        "unknown_key",
        "missing_artifacts",
        "artifact_hash",
        "activation_changed",
        "resident_profile",
        "single_canary",
        "applied_at",
    ],
)
def test_verify_requires_exact_completed_apply_receipt(
    release_files, monkeypatch, tamper
):
    store = _configure_verify_fixture(release_files, monkeypatch)
    receipt = json.loads(release_files["apply_receipt"].read_bytes())
    if tamper == "unknown_key":
        receipt["legacy_authority"] = "forbidden"
    elif tamper == "missing_artifacts":
        receipt.pop("artifacts")
    elif tamper == "artifact_hash":
        receipt["artifacts"][0]["after_sha256"] = "0" * 64
    elif tamper == "activation_changed":
        receipt["activation_changed"] = "true"
    elif tamper == "resident_profile":
        receipt["resident_profile"]["required"][0]["pid"] += 1
    elif tamper == "single_canary":
        receipt["single_canary"]["issue_id"] = "7040000000"
    else:
        receipt["applied_at"] = "2026-08-17T10:02:00+00:00"
    _write_json(release_files["apply_receipt"], receipt)

    expected = (
        "apply_receipt_resident_mismatch"
        if tamper == "resident_profile"
        else "apply_receipt_binding_mismatch"
    )
    with pytest.raises(release.ReleaseError, match=expected):
        release.verify_release(
            **_verify_args(release_files),
            delivery_store_factory=_delivery_factory(store),
        )


def test_verify_compares_stable_resident_identity_not_dynamic_freshness(
    release_files, monkeypatch
):
    receipt_residents = _synthetic_resident_profile(release_files)
    current_residents = json.loads(json.dumps(receipt_residents))
    for item in current_residents["required"]:
        item["freshness_at"] = "2026-08-17T10:05:00+00:00"
        item["freshness_age_seconds"] = 1.0
    store = _configure_verify_fixture(
        release_files,
        monkeypatch,
        receipt_residents=receipt_residents,
        resident_readback=lambda *_args, **_kwargs: current_residents,
    )

    result = release.verify_release(
        **_verify_args(release_files),
        delivery_store_factory=_delivery_factory(store),
    )

    assert result["ok"] is True
    assert result["resident_profile"] == current_residents

    store = _configure_verify_fixture(release_files, monkeypatch)
    value = json.loads(release_files["apply_receipt"].read_bytes())
    value["resident_transition"]["required"][0]["new_pid"] += 1
    _write_json(release_files["apply_receipt"], value)
    with pytest.raises(release.ReleaseError, match="apply_receipt_resident_mismatch"):
        release.verify_release(
            **_verify_args(release_files),
            delivery_store_factory=_delivery_factory(store),
        )

    store = _configure_verify_fixture(release_files, monkeypatch)
    release_files["apply_receipt"].chmod(0o640)
    with pytest.raises(release.ReleaseError, match="apply_receipt_identity_invalid"):
        release.verify_release(
            **_verify_args(release_files),
            delivery_store_factory=_delivery_factory(store),
        )


def test_verify_rechecks_all_release_surfaces_and_receipt_twice(
    release_files, monkeypatch
):
    store = _configure_verify_fixture(release_files, monkeypatch)
    residents = _synthetic_resident_profile(release_files)
    counts = {
        "gitlab": 0,
        "runtime": 0,
        "activation": 0,
        "resident": 0,
        "live": 0,
    }
    live_readback = release._live_projection_readback

    def observed(name, value):
        def call(*_args, **_kwargs):
            counts[name] += 1
            return value

        return call

    def live(*args, **kwargs):
        counts["live"] += 1
        return live_readback(*args, **kwargs)

    monkeypatch.setattr(release, "gitlab_readback", observed("gitlab", {}))
    monkeypatch.setattr(release, "runtime_readback", observed("runtime", {}))
    monkeypatch.setattr(
        release,
        "_activation_status",
        observed(
            "activation",
            {"epoch_id": release_files["note"]["activation"]["epoch_id"]},
        ),
    )
    monkeypatch.setattr(
        release,
        "_resident_profile_readback",
        observed("resident", residents),
    )
    monkeypatch.setattr(release, "_live_projection_readback", live)

    result = release.verify_release(
        **_verify_args(release_files),
        delivery_store_factory=_delivery_factory(store),
    )

    assert result["ok"] is True
    assert counts == {
        "gitlab": 2,
        "runtime": 2,
        "activation": 2,
        "resident": 2,
        "live": 2,
    }


def test_verify_rejects_apply_receipt_drift_during_canary_readback(
    release_files, monkeypatch
):
    store = _configure_verify_fixture(release_files, monkeypatch)

    def mutate_receipt(*_args, **_kwargs):
        value = json.loads(release_files["apply_receipt"].read_bytes())
        value["completed_at"] = "2026-08-17T10:02:00+00:00"
        _write_json(release_files["apply_receipt"], value)
        return {"transport": {"status": "pass"}}

    monkeypatch.setattr(release, "canary_readback", mutate_receipt)
    with pytest.raises(
        release.ReleaseError, match="apply_receipt_changed_during_verify"
    ):
        release.verify_release(
            **_verify_args(release_files),
            delivery_store_factory=_delivery_factory(store),
        )


@pytest.mark.parametrize(
    ("target", "error"),
    [
        ("note", "release_note_changed"),
        ("env", "live_projection_changed_during_verify"),
        ("manifest", "live_projection_changed_during_verify"),
    ],
)
def test_verify_rejects_closeout_projection_drift(
    release_files, monkeypatch, target, error
):
    def drift_then_return(*_args, **_kwargs):
        if target == "note":
            note = json.loads(release_files["note_path"].read_text())
            note["activation"]["reason"] = "changed during verification"
            _write_json(release_files["note_path"], note)
        elif target == "env":
            release_files["live_env"].write_bytes(
                release_files["live_env"].read_bytes() + b"DRIFT=1\n"
            )
        else:
            manifest = json.loads(release_files["live_manifest"].read_text())
            manifest["drift"] = True
            _write_json(release_files["live_manifest"], manifest)
        return _synthetic_resident_profile(release_files)

    store = _configure_verify_fixture(
        release_files,
        monkeypatch,
        resident_readback=drift_then_return,
    )

    with pytest.raises(release.ReleaseError, match=error):
        release.verify_release(
            **_verify_args(release_files),
            delivery_store_factory=_delivery_factory(store),
        )
    assert not (release_files["home"] / "runtime" / release.RELEASE_LOCK_NAME).exists()


def test_verify_rejects_activation_change_at_closeout(release_files, monkeypatch):
    initial = {
        "epoch_id": release_files["note"]["activation"]["epoch_id"],
        "updated_at": "2026-08-17T10:00:00+00:00",
    }
    changed = {**initial, "updated_at": "2026-08-17T10:00:01+00:00"}
    observations = iter((initial, changed))
    store = _configure_verify_fixture(
        release_files,
        monkeypatch,
        activation_status=lambda *_args: next(observations),
        receipt_activation=initial,
    )

    with pytest.raises(release.ReleaseError, match="activation_changed_during_verify"):
        release.verify_release(
            **_verify_args(release_files),
            delivery_store_factory=_delivery_factory(store),
        )
    assert not (release_files["home"] / "runtime" / release.RELEASE_LOCK_NAME).exists()
