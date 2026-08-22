from __future__ import annotations

import ast
from datetime import datetime, timezone
import csv
import json
import os
from pathlib import Path
import signal
import sqlite3
import stat
import subprocess
import sys
import tempfile
from types import SimpleNamespace

import pytest

from scripts import pnc_rca_minimal_release as release


ZERO_INFLIGHT = {
    "dispatchable_outbox": 0,
    "execution_delivery": 0,
    "pending_inbox": 0,
    "total": 0,
}
DEFAULT_PREDECESSOR_ID = "rca-activation-r15av-20260817"
DEFAULT_PREDECESSOR_FINGERPRINT = "a" * 64


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
    assert "preflight" in completed.stdout
    assert "prepare-preflight" in completed.stdout
    assert "activate" in completed.stdout


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
        observed_control_schema_version=release.CONTROL_SCHEMA_V14,
        binary_write_schema_version=release.CONTROL_SCHEMA_V15,
    ):
        if current is None and predecessor is None:
            current = {
                "epoch_id": DEFAULT_PREDECESSOR_ID,
                "state": "steady_active",
            }
            predecessor = {
                **current,
                "binding_fingerprint": DEFAULT_PREDECESSOR_FINGERPRINT,
                "inflight": dict(ZERO_INFLIGHT),
            }
        self.current = current
        self.predecessor = predecessor
        self.source_snapshot = source_snapshot
        self.progress = partition_progress or {}
        self.activation_calls = []
        self.migration_calls = []
        self.partition_calls = []
        self.read_only = True
        self.observed_control_schema_version = observed_control_schema_version
        self.binary_write_schema_version = binary_write_schema_version

    def schema_runtime_capability(self):
        writable = not self.read_only
        successor = self.observed_control_schema_version == release.CONTROL_SCHEMA_V15
        return {
            "observed_control_schema_version": self.observed_control_schema_version,
            "binary_write_schema_version": self.binary_write_schema_version,
            "mode": (
                "current_write"
                if writable
                else "successor_read_only"
                if successor
                else "explicit_read_only"
            ),
            "read_supported": True,
            "write_enabled": writable,
            "work_admission_enabled": writable,
            "lease_acquisition_enabled": writable,
            "external_effect_enabled": writable,
        }

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
        return self._record_activation(kwargs)

    def migrate_v14_to_v15_and_activate(self, _path, **kwargs):
        self.migration_calls.append(kwargs)
        self.observed_control_schema_version = release.CONTROL_SCHEMA_V15
        self.binary_write_schema_version = release.CONTROL_SCHEMA_V15
        return self._record_activation(kwargs)

    def _record_activation(self, kwargs):
        self.current = {
            "epoch_id": kwargs["epoch_id"],
            **release._activation_expected({
                "release_fingerprint_sha256": kwargs["release_fingerprint_sha256"],
                "release_note_sha256": kwargs["release_note_sha256"],
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

    def probe_activation_outcome(self, _path, **kwargs):
        if self.observed_control_schema_version == release.CONTROL_SCHEMA_V14:
            return "not_committed"
        if self.current is None:
            return "unknown"
        expected = release._activation_expected({
            "release_fingerprint_sha256": kwargs["release_fingerprint_sha256"],
            "release_note_sha256": kwargs["release_note_sha256"],
            "config_sha256": kwargs["config_sha256"],
            "db_logical_identity_sha256": release._sha(
                release._canonical(kwargs["db_logical_identity"])
            ),
            "partition_start_fence_sha256": release._sha(
                release._canonical(kwargs["partition_start_fence"])
            ),
        })
        if (
            self.current.get("epoch_id") == kwargs["epoch_id"]
            and all(self.current.get(key) == value for key, value in expected.items())
        ):
            return "committed"
        predecessor = self.predecessor or {}
        if (
            self.current.get("epoch_id")
            == kwargs["expected_predecessor_epoch_id"]
            and self.current.get("state") == kwargs["expected_predecessor_state"]
            and predecessor.get("binding_fingerprint")
            == kwargs["expected_predecessor_binding_fingerprint"]
            and predecessor.get("inflight", {}).get("total") == 0
        ):
            return "not_committed"
        return "unknown"


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
        bad_ls_remote_face="",
    ):
        self.data = data
        self.pids = dict(
            pids
            or {
                row[0]: 1000 + index
                for index, row in enumerate(release.REQUIRED_RESIDENTS)
            }
        )
        self.initial_pids = dict(self.pids)
        self.loaded = {
            label for label, pid in self.pids.items() if pid is not None
        } | set(loaded_without_pid)
        self.disabled = {}
        self.bad_ref_face = bad_ref_face
        self.bad_tree_face = bad_tree_face
        self.bad_ref_round = bad_ref_round
        self.bad_tag_type_face = bad_tag_type_face
        self.bad_ls_remote_face = bad_ls_remote_face
        self.fetch_faces = {}
        self.fetch_rounds = {}
        self.calls = []

    def __call__(self, command):
        command = tuple(command)
        self.calls.append(command)
        if len(command) > 3 and command[:2] == ("/usr/bin/git", "ls-remote"):
            remote = next(
                (value for value in command if value.startswith("git@git.minieye.tech:")),
                "",
            )
            name = next(
                (
                    name
                    for name, face in self.data["identity"].items()
                    if isinstance(face, dict) and face.get("remote") == remote
                ),
                "",
            )
            face = self.data["identity"].get(name, {})
            commit = face.get("commit", "0" * 40)
            peeled_commit = (
                "0" * 40 if name == self.bad_ls_remote_face else commit
            )
            rows = []
            for ref in command[4:]:
                if ref == face.get("remote_branch"):
                    rows.append(f"{commit}\t{ref}")
                elif ref == f"refs/tags/{face.get('remote_tag')}":
                    rows.append(f"{face.get('remote_tag_object', '0' * 40)}\t{ref}")
                elif ref == f"refs/tags/{face.get('remote_tag')}^{{}}":
                    rows.append(f"{peeled_commit}\t{ref}")
            return subprocess.CompletedProcess(command, 0, "\n".join(rows) + "\n", "")
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
        "gateway_release_binding": {
            "workspace_runtime_manifest_sha256": "b" * 64,
            "workspace_runtime_closure_sha256": "c" * 64,
            "workspace_runtime_source_commit": "d" * 40,
        },
        "production_branch_bindings": {
            "workspace_runtime": {
                "branch": "DETACHED",
                "commit": "d" * 40,
                "remote": "git@git.minieye.tech:planning_algo/pnc-agent-workspace-work.git",
                "tree": "e" * 40,
            }
        },
        "rca_release_note": {
            "path": str(note_path),
            "release_id": "rca-r15aw-20260817",
            "release_fingerprint_sha256": fingerprint,
        },
    }
    manifest_raw = _write_json(manifest_source, manifest)
    # The candidate is structurally complete; the live template remains the
    # minimal preimage so apply tests can verify exact artifact restoration.
    db_identity = {"schema_version": "test_db_v1", "path": str(control_db)}
    fence = {"feishu-project-workflow-event": {"0": 1984}}
    activation = {
        "epoch_id": "rca-activation-r15aw-20260817",
        "control_db_path": str(control_db),
        "operator": "owner:test",
        "reason": "test minimal release",
        "expected_control_schema_version": (release.CONTROL_SCHEMA_V14),
        "target_control_schema_version": (release.CONTROL_SCHEMA_V15),
        "expected_predecessor_epoch_id": DEFAULT_PREDECESSOR_ID,
        "expected_predecessor_state": "steady_active",
        "expected_predecessor_binding_fingerprint": (DEFAULT_PREDECESSOR_FINGERPRINT),
        "db_logical_identity": db_identity,
        "db_logical_identity_sha256": release._sha(release._canonical(db_identity)),
        "partition_start_fence": fence,
        "partition_start_fence_sha256": release._sha(release._canonical(fence)),
    }
    activation["epoch_contract_sha256"] = release.minimal_release_epoch_contract_sha256(
        activation
    )
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
        "activation": activation,
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
        store.read_only = read_only
        return store

    return factory


def _migration_args(store):
    return {
        "migration_apply": store.migrate_v14_to_v15_and_activate,
        "outcome_probe": store.probe_activation_outcome,
    }


def _atomic_activation_kwargs(note, binding):
    activation = note["activation"]
    return {
        "epoch_id": binding["epoch_id"],
        "release_fingerprint_sha256": binding["release_fingerprint_sha256"],
        "release_note_sha256": binding["release_note_sha256"],
        "config_sha256": binding["config_sha256"],
        "db_logical_identity": binding["db_logical_identity"],
        "partition_start_fence": binding["partition_start_fence"],
        "operator": activation["operator"],
        "reason": activation["reason"],
        "expected_predecessor_epoch_id": activation["expected_predecessor_epoch_id"],
        "expected_predecessor_state": activation["expected_predecessor_state"],
        "expected_predecessor_binding_fingerprint": activation[
            "expected_predecessor_binding_fingerprint"
        ],
        "expected_control_schema_version": activation[
            "expected_control_schema_version"
        ],
        "target_control_schema_version": activation["target_control_schema_version"],
        "epoch_contract_sha256": activation["epoch_contract_sha256"],
    }


def _seed_real_v14_predecessor_note(data):
    legacy = release.RcaControlStore(data["control_db"])
    with sqlite3.connect(data["control_db"]) as conn:
        conn.execute(
            "INSERT INTO kafka_inbox("
            "event_uid, topic, partition_id, offset_id, raw_value, raw_size_bytes, "
            "raw_sha256, headers_json, policy_json, creation_rule_version, "
            "submission_mode, received_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "test-v14-predecessor-event",
                "feishu-project-workflow-event",
                0,
                1983,
                b"",
                0,
                release._sha(b""),
                "[]",
                "{}",
                "test-v1",
                "shadow",
                "2026-08-17T10:00:00+00:00",
            ),
        )
        conn.execute(
            "INSERT INTO kafka_partition_progress("
            "topic, partition_id, first_offset, durable_next_offset, "
            "last_event_uid, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
            (
                "feishu-project-workflow-event",
                0,
                1983,
                1984,
                "test-v14-predecessor-event",
                "2026-08-17T10:00:00+00:00",
            ),
        )
    legacy.activate_direct_steady_epoch(
        epoch_id="rca-activation-r15ax-predecessor",
        release_fingerprint_sha256="b" * 64,
        release_note_sha256="c" * 64,
        config_sha256="d" * 64,
        db_logical_identity={"source": "v14-predecessor"},
        partition_start_fence={"feishu-project-workflow-event": {"0": 1984}},
        operator="owner:test",
        reason="seed v14 predecessor",
    )
    predecessor = legacy.direct_steady_predecessor()
    assert predecessor is not None
    note = json.loads(json.dumps(data["note"]))
    note["activation"].update({
        "expected_predecessor_epoch_id": predecessor["epoch_id"],
        "expected_predecessor_state": predecessor["state"],
        "expected_predecessor_binding_fingerprint": predecessor["binding_fingerprint"],
    })
    note["activation"]["epoch_contract_sha256"] = (
        release.minimal_release_epoch_contract_sha256(note["activation"])
    )
    note_raw = _write_json(data["note_path"], note)
    return note, note_raw, release._bound_binding(note_raw, note)


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
    # Prepare tests exercise projection updates against a stale but structurally
    # valid live template; apply/verify tests retain the minimal old sentinel.
    data["live_manifest"].write_bytes(data["manifest_source"].read_bytes())
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
        "workspace_runtime_resolver": lambda _home, _runner: {
            "manifest_sha256": "1" * 64,
            "closure_sha256": "2" * 64,
            "source_commit": "3" * 40,
            "tree": "4" * 40,
        },
        # Unit tests inject a deterministic read-only result. The CLI/default
        # path reads the candidate contract and installed VM bytes directly.
        "dependency_probe": lambda _pipeline_root, _pipeline_identity: {
            "ok": True,
            "status": "fixture_dependency_proof",
        },
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
    assert prepared["store_calls"]
    assert all(read_only is True for _path, read_only in prepared["store_calls"])
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
    assert note["activation"]["expected_control_schema_version"] == (
        release.CONTROL_SCHEMA_V14
    )
    assert note["activation"]["target_control_schema_version"] == (
        release.CONTROL_SCHEMA_V15
    )
    assert note["activation"]["epoch_contract_sha256"] == (
        release.minimal_release_epoch_contract_sha256(note["activation"])
    )
    manifest = json.loads(prepared["manifest_output"].read_bytes())
    assert manifest["gateway_release_binding"][
        "workspace_runtime_manifest_sha256"
    ] == "1" * 64
    assert manifest["gateway_release_binding"][
        "workspace_runtime_closure_sha256"
    ] == "2" * 64
    assert manifest["gateway_release_binding"][
        "workspace_runtime_source_commit"
    ] == "3" * 40
    assert manifest["production_branch_bindings"]["workspace_runtime"]["tree"] == (
        "4" * 40
    )
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


def test_preflight_is_read_only_non_short_circuit_and_structured(release_files):
    prepared = _prepare_fixture(release_files)
    runner = FakeRunner(release_files)
    paths = (
        prepared["note_path"],
        prepared["env_output"],
        prepared["manifest_output"],
        release_files["control_db"],
        Path(f"{release_files['control_db']}-wal"),
        Path(f"{release_files['control_db']}-shm"),
    )
    prepared["args"]["pipeline_remote"] = "git@github.com:wrong/repository.git"
    prepared["env_output"].write_bytes(b"existing\n")
    before = {
        path: (path.read_bytes() if path.exists() else None)
        for path in paths
    }
    result = release.preflight_release(
        **prepared["args"],
        runner=runner,
        require_canary_state=False,
    )

    assert result["preflight_ok"] is False
    failed = {row["gate"] for row in result["failed"]}
    assert "prepare_output_exists" in failed
    assert "gitlab_face_input_invalid" in failed
    assert "prepare_control_snapshot_invalid" not in failed
    assert result["total"] == len(result["checks"])
    assert result["checked"] + result["deferred"] == result["total"]
    assert result["checked"] > len(result["failed"])
    assert all(
        {"gate", "actual", "expected", "hint", "passed"} <= row.keys()
        for row in result["checks"]
    )
    with (Path(__file__).resolve().parents[2] / "preflight-gate-inventory.csv").open(
        newline="", encoding="utf-8"
    ) as handle:
        inventory_codes = {row["gate_code"] for row in csv.DictReader(handle)}
    assert {row["gate"] for row in result["checks"]} <= inventory_codes
    assert any(command[1:3] == ("ls-remote", "--exit-code") for command in runner.calls)
    assert any(
        "--no-optional-locks" in command and "status" in command
        for command in runner.calls
    )
    assert not any(
        command[1:] in {("init", "--bare"), ("fetch", "--quiet")} or "fetch" in command
        for command in runner.calls
    )
    after = {
        path: (path.read_bytes() if path.exists() else None)
        for path in paths
    }
    assert after == before


def test_preflight_real_store_preserves_db_wal_shm_and_cleans_snapshots(
    release_files,
):
    release.RcaControlStore(release_files["control_db"])
    prepared = _prepare_fixture(release_files)
    prepared["args"]["store_factory"] = release._open_store
    source_paths = (
        release_files["control_db"],
        Path(f"{release_files['control_db']}-wal"),
        Path(f"{release_files['control_db']}-shm"),
    )

    def identity(path):
        if not path.exists():
            return None
        observed = path.lstat()
        return (
            observed.st_dev,
            observed.st_ino,
            observed.st_mode,
            observed.st_size,
            observed.st_mtime_ns,
            release._sha(path.read_bytes()),
        )

    before = {path: identity(path) for path in source_paths}
    snapshot_parent = Path(tempfile.gettempdir())
    snapshot_roots_before = set(snapshot_parent.glob("pnc-rca-control-ro-*"))
    result = release.preflight_release(
        **prepared["args"],
        runner=FakeRunner(release_files),
        require_canary_state=False,
    )
    after = {path: identity(path) for path in source_paths}
    snapshot_roots_after = set(snapshot_parent.glob("pnc-rca-control-ro-*"))

    assert result["checked"] > len(result["failed"])
    assert after == before
    assert snapshot_roots_after == snapshot_roots_before
    assert not prepared["note_path"].exists()
    assert not prepared["env_output"].exists()
    assert not prepared["manifest_output"].exists()


def test_prepare_preflight_defers_canary_and_raw_control_snapshot(release_files):
    prepared = _prepare_fixture(release_files)
    runner = FakeRunner(release_files)
    result = release.prepare_preflight_release(**prepared["args"], runner=runner)

    assert result["mode"] == "prepare-preflight"
    assert result["preflight_ok"] is True
    assert result["deferred"] >= 7
    deferred = {
        row["gate"]: row
        for row in result["checks"]
        if row["status"] == "deferred"
    }
    assert deferred["prepare_control_snapshot_invalid"]["precheckable"] is False
    assert deferred["rca_control_store_snapshot_source_changed"]["precheckable"] is False
    assert deferred["canary_state_unavailable"]["precheckable"] is False
    assert not prepared["store_calls"]
    assert not prepared["note_path"].exists()
    assert not prepared["manifest_output"].exists()
    assert not prepared["env_output"].exists()


def test_prepare_failure_retains_full_preflight_report(release_files):
    prepared = _prepare_fixture(release_files)
    prepared["args"]["pipeline_remote"] = "git@github.com:wrong/repository.git"
    prepared["env_output"].write_bytes(b"existing\n")
    with pytest.raises(release.PreflightError) as caught:
        release.prepare_release(
            **prepared["args"],
            runner=FakeRunner(release_files),
        )
    report = caught.value.result
    assert report["preflight_ok"] is False
    assert len(report["failed"]) >= 2
    assert {row["gate"] for row in report["failed"]} >= {
        "prepare_output_exists",
        "gitlab_face_input_invalid",
    }
    assert not prepared["note_path"].exists()
    assert not prepared["manifest_output"].exists()


def test_preflight_subprocess_failure_is_one_structured_row(release_files):
    prepared = _prepare_fixture(release_files)
    base_runner = FakeRunner(release_files)

    def runner(command):
        if (
            len(command) > 4
            and command[0:2] == ("/usr/bin/git", "ls-remote")
            and release_files["identity"]["worker"]["remote"] in command
        ):
            return subprocess.CompletedProcess(command, 128, "", "network down")
        return base_runner(command)

    result = release.preflight_release(
        **prepared["args"],
        runner=runner,
        require_canary_state=False,
    )
    failed = {row["gate"] for row in result["failed"]}
    assert "gitlab_identity_mismatch" in failed
    assert any(
        row.get("failure_code") == "gitlab_readback_failed"
        for row in result["failed"]
    )
    assert result["checked"] > len(result["failed"])


def test_preflight_invalid_partition_input_is_aggregated(release_files):
    prepared = _prepare_fixture(release_files)
    prepared["args"]["partition_topics"] = ("missing-separator",)
    prepared["env_output"].write_bytes(b"existing\n")

    result = release.preflight_release(
        **prepared["args"],
        runner=FakeRunner(release_files),
        require_canary_state=False,
    )

    failed = {row["gate"]: row for row in result["failed"]}
    assert {"prepare_partition_topics_invalid", "prepare_output_exists"} <= set(
        failed
    )
    assert failed["prepare_partition_topics_invalid"]["actual"] == [
        "missing-separator"
    ]


def test_default_dependency_probe_reports_vm_fingerprint_without_install(release_files):
    prepared = _prepare_fixture(release_files)
    prepared["args"].pop("dependency_probe")
    calls = []
    expected_dependencies = {
        name: {
            "version": "1.0.0",
            "module": module,
            "distribution_root": "/home/mini/.local/lib/python3.8/site-packages",
            "module_source": (
                "/home/mini/.local/lib/python3.8/site-packages/"
                + module.replace(".", "/")
                + "/__init__.py"
            ),
            "record_sha256": "a" * 64,
            "critical_files_sha256": "d" * 64,
            "critical_file_count": 17,
        }
        for name, module in release.REMOTE_READER_DEPENDENCY_MODULES.items()
    }
    actual_dependencies = {
        name: {**expected, "distribution": name}
        for name, expected in expected_dependencies.items()
    }
    actual_dependencies["mcap"] = {
        **actual_dependencies["mcap"],
        "version": "2.0.0",
    }

    def vm_runner(command, input_text):
        calls.append((tuple(command), input_text))
        assert command[1:] == ("run_py_json",)
        assert input_text is not None
        assert "subprocess.run" in input_text
        assert "--no-optional-locks" in input_text
        assert "--porcelain=v1" in input_text
        assert "cat-file" in input_text
        assert release_files["identity"]["pipeline"]["commit"] in input_text
        assert release_files["identity"]["pipeline"]["tree"] in input_text
        assert "source-materialization.json" in input_text
        assert "frozen_runtime" in input_text
        assert "frozen_materialization_receipt_ambiguous" in input_text
        assert "bootstrap_remote_reader_runtime.py" not in input_text
        assert "--install-offline" not in input_text
        assert release.REMOTE_READER_RUNTIME_BUNDLE_SCHEMA in input_text
        assert release.REMOTE_READER_RUNTIME_CONTRACT_SCHEMA in input_text
        assert (
            'bundle_relative = "api/g1q3_rca/vendor/'
            'remote_reader_runtime_bundle.generated.json"'
        ) in input_text
        assert "remote_reader_runtime_contract.json" not in input_text
        assert 'bundle.get("runtime_contract_sha256") == canonical_sha256(contract)' in input_text
        assert 'bundle.get("vendor_manifest_sha256") == canonical_sha256(vendor_manifest)' in input_text
        for name in release.REMOTE_READER_SYSTEM_DEPENDENCIES:
            assert name in input_text
        response = {
            "ok": False,
            "expected": {
                "source": {
                    "runtime_root": str(
                        release_files["identity"]["pipeline"]["runtime_root"]
                    ),
                    "commit": release_files["identity"]["pipeline"]["commit"],
                    "clean": True,
                },
                "dependencies": expected_dependencies,
            },
            "actual": {
                "source": {"clean": True, "stable": True},
                "dependencies": actual_dependencies,
            },
            "source": {"ok": True},
            "contract": {"ok": True},
            "dependencies": {
                name: {
                    "ok": name != "mcap",
                    "expected": expected_dependencies[name],
                    "actual": actual_dependencies[name],
                    "mismatched_fields": ["version"] if name == "mcap" else [],
                }
                for name in release.REMOTE_READER_SYSTEM_DEPENDENCIES
            },
            "mismatches": [
                {
                    "scope": "dependency",
                    "dependency": "mcap",
                    "fields": ["version"],
                }
            ],
            "bootstrap": {"status": "deferred_execution_only"},
        }
        return subprocess.CompletedProcess(
            command, 0, json.dumps(response, sort_keys=True) + "\n", ""
        )

    result = release.preflight_release(
        **prepared["args"],
        runner=FakeRunner(release_files),
        vm_runner=vm_runner,
        require_canary_state=False,
    )
    dependency_rows = [
        row
        for row in result["checks"]
        if row["gate"] == "remote_reader_dependency_unavailable"
    ]
    assert len(dependency_rows) == 1
    row = dependency_rows[0]
    assert row["passed"] is False
    assert row["failure_code"] == "remote_reader_dependency_unavailable"
    assert set(row["expected"]["dependencies"]) == set(
        release.REMOTE_READER_SYSTEM_DEPENDENCIES
    )
    assert row["actual"]["dependencies"]["mcap"]["ok"] is False
    assert row["actual"]["dependencies"]["mcap"]["mismatched_fields"] == [
        "version"
    ]
    assert row["actual"]["dependencies"]["pdcl-dss"]["ok"] is True
    assert row["actual"]["bootstrap"]["status"] == "deferred_execution_only"
    assert len(calls) == 1


@pytest.mark.parametrize("source_failure", ["stale_commit", "dirty_worktree"])
def test_default_dependency_probe_fails_closed_on_unbound_source(
    release_files, source_failure
):
    prepared = _prepare_fixture(release_files)
    prepared["args"].pop("dependency_probe")
    expected_commit = release_files["identity"]["pipeline"]["commit"]

    def vm_runner(command, input_text):
        actual = {
            "commit": "0" * 40 if source_failure == "stale_commit" else expected_commit,
            "clean": source_failure != "dirty_worktree",
            "stable": True,
        }
        response = {
            "ok": False,
            "expected": {
                "source": {"commit": expected_commit, "clean": True},
                "dependencies": {},
            },
            "actual": {"source": actual, "dependencies": {}},
            "source": {
                "ok": False,
                "expected": {"commit": expected_commit, "clean": True},
                "actual": actual,
                "error": None,
            },
            "contract": {"ok": False},
            "dependencies": {},
            "mismatches": [
                {
                    "scope": "source",
                    "fields": ["commit", "clean", "stable"],
                }
            ],
            "bootstrap": {"status": "deferred_execution_only"},
        }
        return subprocess.CompletedProcess(command, 0, json.dumps(response) + "\n", "")

    result = release.preflight_release(
        **prepared["args"],
        runner=FakeRunner(release_files),
        vm_runner=vm_runner,
        require_canary_state=False,
    )

    row = next(
        row
        for row in result["checks"]
        if row["gate"] == "remote_reader_dependency_unavailable"
    )
    assert row["passed"] is False
    assert row["actual"]["source"]["ok"] is False
    assert row["actual"]["mismatches"][0]["scope"] == "source"


def test_r4_behavioral_regression_collects_all_precheckable_historical_gates(
    release_files,
):
    class ChangingStore(FakeStore):
        snapshot_calls = 0

        def control_db_source_snapshot_identity(self):
            self.snapshot_calls += 1
            value = super().control_db_source_snapshot_identity()
            value["observation"] = self.snapshot_calls
            return value

    report = {
        "schema_version": "pnc_rca_report_manifest_v1",
        "runtime_root": release_files["identity"]["pipeline"]["runtime_root"],
        "pipeline_commit": "0" * 40,
        "pipeline_tree": release_files["identity"]["pipeline"]["tree"],
        "report_script_sha256": "c" * 64,
    }
    store = ChangingStore(
        source_snapshot={
            "schema_version": "pnc_rca_control_store_source_snapshot_v1",
            "path": str(release_files["control_db"].absolute()),
            "present": True,
            "logical_db_identity": {"database": "snapshot", "wal": "snapshot-wal"},
        },
        partition_progress={("feishu-project-workflow-event", 0): 1984},
        observed_control_schema_version=release.CONTROL_SCHEMA_V15,
        binary_write_schema_version=release.CONTROL_SCHEMA_V15,
    )
    prepared = _prepare_fixture(release_files, store=store, report=report)
    prepared["args"]["worker_remote"] = "git@github.com:wrong/repository.git"
    prepared["args"]["dependency_probe"] = (
        lambda _root, _identity: (_ for _ in ()).throw(
            release.ReleaseError("remote_reader_dependency_unavailable")
        )
    )

    result = release.preflight_release(
        **prepared["args"],
        runner=FakeRunner(release_files, bad_ls_remote_face="host"),
        require_canary_state=True,
    )
    failed = {row["gate"] for row in result["failed"]}
    assert {
        "gitlab_face_input_invalid",
        "gitlab_identity_mismatch",
        "report_manifest_pipeline_mismatch",
        "rca_control_store_snapshot_source_changed",
        "remote_reader_dependency_unavailable",
        "canary_state_unavailable",
    } <= failed
    identity_failures = [
        row
        for row in result["failed"]
        if row["gate"] == "gitlab_identity_mismatch"
    ]
    assert any(
        row.get("failure_code") == "gitlab_identity_mismatch"
        and row["actual"].get("peeled_tag_commit") == "0" * 40
        for row in identity_failures
    )
    report_failure = next(
        row
        for row in result["failed"]
        if row["gate"] == "report_manifest_pipeline_mismatch"
    )
    assert report_failure["actual"]["pipeline_commit"] == "0" * 40
    assert report_failure["expected"]["pipeline_commit"] == (
        release_files["identity"]["pipeline"]["commit"]
    )
    supersession = [
        row
        for row in result["checks"]
        if row["gate"] == "prepare_control_schema_already_v15"
    ]
    assert supersession and supersession[0]["passed"] is True
    deferred = {
        row["gate"]: row
        for row in result["checks"]
        if row["gate"] in {"restart_readback_timeout", "bootstrap_install-offline_failed"}
    }
    assert set(deferred) == {"restart_readback_timeout", "bootstrap_install-offline_failed"}
    assert all(row["precheckable"] is False and row["passed"] for row in deferred.values())
    assert result["checked"] > len(result["failed"])


def test_main_preserves_preflight_error_aggregate(monkeypatch, capsys):
    aggregate = {
        "schema_version": release.PREFLIGHT_SCHEMA,
        "mode": "preflight",
        "preflight_ok": False,
        "ok": False,
        "checked": 2,
        "passed": 0,
        "failed": [
            {
                "gate": "gitlab_identity_mismatch",
                "actual": "bad",
                "expected": "good",
                "hint": "refresh identity",
                "passed": False,
                "failure_code": "gitlab_identity_mismatch",
            },
            {
                "gate": "report_manifest_pipeline_mismatch",
                "actual": "bad",
                "expected": "good",
                "hint": "rematerialize",
                "passed": False,
                "failure_code": "report_manifest_pipeline_mismatch",
            },
        ],
        "checks": [],
    }

    class FakeParser:
        def parse_args(self, _argv):
            return SimpleNamespace(
                command="prepare",
                release_note=Path("/tmp/release-note.json"),
                hermes_home=Path("/tmp/hermes"),
                release_id="release-id",
                epoch_id="epoch-id",
                operator="owner:test",
                reason="test",
                canary_batch_id="canary-id",
                canary_issue_id="123456",
                canary_state_path=Path("/tmp/canary.json"),
                host_branch="refs/heads/production/rca",
                host_tag="host-tag",
                host_runtime_root=Path("/tmp/host"),
                worker_remote="git@git.minieye.tech:planning_algo/worker.git",
                worker_branch="refs/heads/worker",
                worker_tag="worker-tag",
                worker_runtime_root=Path("/tmp/worker"),
                pipeline_remote="git@git.minieye.tech:planning_algo/pipeline.git",
                pipeline_branch="refs/heads/pipeline",
                pipeline_tag="pipeline-tag",
                pipeline_runtime_root=Path("/tmp/pipeline"),
                report_manifest_path=Path("/home/mini/.config/g1q3-rca/report-runtime-manifest.json"),
                partition_topic=[],
                control_db=Path("/tmp/hermes/runtime/pnc_agent/feishu_issue_kafka_rca/control.sqlite3"),
                manifest_output=Path("/tmp/manifest.json"),
                env_output=Path("/tmp/env"),
            )

    monkeypatch.setattr(release, "_parser", lambda: FakeParser())

    def blocked(**_kwargs):
        raise release.PreflightError(aggregate)

    monkeypatch.setattr(release, "prepare_release", blocked)
    assert release.main([]) == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["failed"] == aggregate["failed"]
    assert payload["command"] == "prepare"

    standalone_args = FakeParser().parse_args([])
    standalone_args.command = "preflight"
    standalone_args.partition_topic = ["missing-separator"]

    class StandaloneParser:
        def parse_args(self, _argv):
            return standalone_args

    monkeypatch.setattr(release, "_parser", lambda: StandaloneParser())

    def standalone_preflight(**kwargs):
        assert kwargs["partition_topics"] == ("missing-separator",)
        return aggregate

    monkeypatch.setattr(release, "preflight_release", standalone_preflight)
    assert release.main([]) == 2
    standalone_payload = json.loads(capsys.readouterr().out)
    assert standalone_payload["failed"] == aggregate["failed"]
    assert "command" not in standalone_payload

    green = {**aggregate, "preflight_ok": True, "ok": True, "passed": 2, "failed": []}
    monkeypatch.setattr(release, "preflight_release", lambda **_kwargs: green)
    assert release.main([]) == 0
    assert json.loads(capsys.readouterr().out)["preflight_ok"] is True


R4_HISTORICAL_GATE_ATTEMPTS = (
    ("r15bb", "apply", "restart_readback_timeout"),
    ("r15bd", "prepare", "gitlab_identity_mismatch"),
    ("r15bd-2", "prepare", "rca_control_store_snapshot_source_changed"),
    ("r15bd-3", "prepare", "prepare_control_schema_already_v15"),
    ("r15be", "prepare", "rca_control_store_snapshot_source_changed"),
    ("r15be-2", "verify", "canary_state_unavailable"),
    ("308", "execution", "remote_reader_dependency_unavailable"),
    ("r15bg", "prepare", "gitlab_face_input_invalid"),
    ("r15bg-b", "prepare", "gitlab_identity_mismatch"),
    ("r15bg-c", "prepare", "report_manifest_pipeline_mismatch"),
    ("materialize", "execution", "bootstrap_install-offline_failed"),
)


def test_r4_inventory_covers_all_historical_gate_attempts():
    inventory_path = Path(__file__).resolve().parents[2] / "preflight-gate-inventory.csv"
    with inventory_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    by_code = {row["gate_code"]: row for row in rows}
    assert {"gate_code", "stage", "precheckable", "check_method", "source_line"} <= set(
        rows[0]
    )
    for _attempt, _stage, code in R4_HISTORICAL_GATE_ATTEMPTS:
        assert code in by_code
    allowed_misses = {"restart_readback_timeout", "bootstrap_install-offline_failed"}
    for _attempt, _stage, code in R4_HISTORICAL_GATE_ATTEMPTS:
        if code not in allowed_misses:
            assert by_code[code]["precheckable"] == "true"


def test_inventory_contains_every_current_literal_release_error():
    repo_root = Path(__file__).resolve().parents[2]
    tree = ast.parse((repo_root / "scripts/pnc_rca_minimal_release.py").read_text())
    current_codes = {
        node.args[0].value
        for node in ast.walk(tree)
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "ReleaseError"
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        )
    }
    with (repo_root / "preflight-gate-inventory.csv").open(
        newline="", encoding="utf-8"
    ) as handle:
        inventory_codes = {row["gate_code"] for row in csv.DictReader(handle)}
    assert len(current_codes) >= 128
    assert current_codes <= inventory_codes


def test_inventory_precheckable_rows_are_reachable_from_preflight():
    repo_root = Path(__file__).resolve().parents[2]
    tree = ast.parse((repo_root / "scripts/pnc_rca_minimal_release.py").read_text())
    functions = {
        node.name: node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    reachable_names = set()
    pending = ["preflight_release"]
    while pending:
        name = pending.pop()
        if name in reachable_names or name not in functions:
            continue
        reachable_names.add(name)
        for node in ast.walk(functions[name]):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id in functions
            ):
                pending.append(node.func.id)

    reachable_codes = {
        node.args[0].value
        for name in reachable_names
        for node in ast.walk(functions[name])
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in {"ReleaseError", "_PreflightProbeFailure"}
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        )
    }
    preflight = functions["preflight_release"]
    reachable_codes.update(
        node.args[0].value
        for node in ast.walk(preflight)
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "check"
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
            and not any(
                keyword.arg == "precheckable"
                and isinstance(keyword.value, ast.Constant)
                and keyword.value.value is False
                for keyword in node.keywords
            )
        )
    )
    reachable_codes.add("preflight_probe_failed")
    registered_codes = {
        node.args[0].value
        for node in ast.walk(preflight)
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "check"
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        )
    }
    path_values = next(
        node
        for node in ast.walk(preflight)
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "path_values"
            for target in node.targets
        )
    )
    assert isinstance(path_values.value, ast.Tuple)
    dynamic_path_codes = {
        item.elts[0].value
        for item in path_values.value.elts
        if (
            isinstance(item, ast.Tuple)
            and item.elts
            and isinstance(item.elts[0], ast.Constant)
            and isinstance(item.elts[0].value, str)
        )
    }
    registered_codes.update(dynamic_path_codes)
    reachable_codes.update(dynamic_path_codes)

    with (repo_root / "preflight-gate-inventory.csv").open(
        newline="", encoding="utf-8"
    ) as handle:
        reader = csv.DictReader(handle)
        assert reader.fieldnames == [
            "gate_code",
            "stage",
            "precheckable",
            "check_method",
            "source_line",
        ]
        rows = list(reader)
    assert len({row["gate_code"] for row in rows}) == len(rows)
    inventory_precheckable = {
        row["gate_code"] for row in rows if row["precheckable"] == "true"
    }
    inventory_codes = {row["gate_code"] for row in rows}
    assert registered_codes <= inventory_codes
    assert inventory_precheckable <= reachable_codes


def test_prepare_current_v15_writes_successor_note(release_files):
    prepared = _prepare_fixture(release_files)
    prepared["store"].observed_control_schema_version = release.CONTROL_SCHEMA_V15
    prepared["store"].binary_write_schema_version = release.CONTROL_SCHEMA_V15

    release.prepare_release(**prepared["args"], runner=FakeRunner(release_files))

    note = json.loads(prepared["note_path"].read_bytes())
    assert note["activation"]["expected_control_schema_version"] == (
        release.CONTROL_SCHEMA_V15
    )
    assert note["activation"]["target_control_schema_version"] == (
        release.CONTROL_SCHEMA_V15
    )
    assert note["activation"]["expected_predecessor_epoch_id"] == (
        DEFAULT_PREDECESSOR_ID
    )
    assert note["activation"]["epoch_contract_sha256"] == (
        release.minimal_release_epoch_contract_sha256(note["activation"])
    )


def test_prepare_rejects_v14_without_steady_predecessor(release_files):
    prepared = _prepare_fixture(release_files)
    prepared["store"].current = None
    prepared["store"].predecessor = None

    with pytest.raises(release.ReleaseError, match="prepare_predecessor_required"):
        release.prepare_release(**prepared["args"], runner=FakeRunner(release_files))

    assert not prepared["note_path"].exists()
    assert not prepared["env_output"].exists()
    assert not prepared["manifest_output"].exists()


def test_prepare_rejects_existing_output_and_cleans_new_reservations(release_files):
    prepared = _prepare_fixture(release_files)
    prepared["env_output"].write_bytes(b"preexisting\n")
    runner = FakeRunner(release_files)

    with pytest.raises(release.ReleaseError, match="prepare_output_exists"):
        release.prepare_release(**prepared["args"], runner=runner)

    assert prepared["env_output"].read_bytes() == b"preexisting\n"
    assert not prepared["note_path"].exists()
    assert not prepared["manifest_output"].exists()
    assert runner.calls
    assert all("fetch" not in command for command in runner.calls)
    assert prepared["store_calls"]
    assert all(read_only is True for _path, read_only in prepared["store_calls"])


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
    assert store.partition_calls == [
        ("topic-a", (0, 2)),
        ("topic-b", (1,)),
        ("topic-a", (0, 2)),
        ("topic-b", (1,)),
    ]


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


def test_report_manifest_reader_uses_read_protocol_without_mutating_doctor_probe(
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
    assert [command[0][0] for command in calls] == [release.SSH_MINI_AGENT]


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


@pytest.mark.parametrize("change", ["schema", "predecessor", "db", "fence"])
def test_release_note_epoch_contract_hash_binds_activation_inputs(
    release_files, change
):
    note = json.loads(release_files["note_path"].read_text())
    activation = note["activation"]
    if change == "schema":
        activation["target_control_schema_version"] = release.CONTROL_SCHEMA_V14
    elif change == "predecessor":
        activation.update({
            "expected_predecessor_epoch_id": "rca-activation-r15av-20260817",
            "expected_predecessor_state": "steady_active",
            "expected_predecessor_binding_fingerprint": "d" * 64,
        })
    elif change == "db":
        activation["db_logical_identity"] = {"database": "changed"}
        activation["db_logical_identity_sha256"] = release._sha(
            release._canonical(activation["db_logical_identity"])
        )
    else:
        activation["partition_start_fence"] = {
            "feishu-project-workflow-event": {"0": 1985}
        }
        activation["partition_start_fence_sha256"] = release._sha(
            release._canonical(activation["partition_start_fence"])
        )
    _write_json(release_files["note_path"], note)

    with pytest.raises(
        release.ReleaseError, match="release_note_epoch_contract_invalid"
    ):
        release._load_note(release_files["note_path"], release_files["home"])


def test_activation_exact_v15_retry_is_a_noop_with_successor_write_readback(
    release_files,
):
    note = release_files["note"]
    binding = release._bound_binding(release_files["note_raw"], note)
    store = FakeStore(
        observed_control_schema_version=release.CONTROL_SCHEMA_V15,
        binary_write_schema_version=release.CONTROL_SCHEMA_V15,
    )
    store._record_activation({
        "epoch_id": binding["epoch_id"],
        "release_fingerprint_sha256": binding["release_fingerprint_sha256"],
        "release_note_sha256": binding["release_note_sha256"],
        "config_sha256": binding["config_sha256"],
        "db_logical_identity": binding["db_logical_identity"],
        "partition_start_fence": binding["partition_start_fence"],
    })
    calls = []
    factory = _factory(store, calls)

    plan = release._activation_plan(
        note,
        binding,
        factory,
        store.probe_activation_outcome,
    )
    applied = release._activation_apply(note, binding, plan, factory)
    status = release._activation_status(note, binding, factory)

    assert plan["transition"] == "v15_noop"
    assert plan["would_change"] is False
    assert applied == {"changed": False, "current_epoch": status}
    assert not store.activation_calls
    assert not store.migration_calls
    assert calls == [
        (release_files["control_db"], True),
        (release_files["control_db"], False),
        (release_files["control_db"], False),
    ]


def _v15_successor_note(data):
    note = json.loads(json.dumps(data["note"]))
    activation = note["activation"]
    activation.update({
        "epoch_id": "rca-activation-r15bd-successor-20260819",
        "expected_control_schema_version": release.CONTROL_SCHEMA_V15,
        "target_control_schema_version": release.CONTROL_SCHEMA_V15,
    })
    activation["epoch_contract_sha256"] = (
        release.minimal_release_epoch_contract_sha256(activation)
    )
    raw = _write_json(data["note_path"], note)
    return note, raw, release._bound_binding(raw, note)


def test_activation_v15_successor_calls_store_then_exact_retry_is_noop(release_files):
    note, _raw, binding = _v15_successor_note(release_files)
    store = FakeStore(
        observed_control_schema_version=release.CONTROL_SCHEMA_V15,
        binary_write_schema_version=release.CONTROL_SCHEMA_V15,
    )
    calls = []
    factory = _factory(store, calls)

    plan = release._activation_plan(
        note, binding, factory, store.probe_activation_outcome
    )
    applied = release._activation_apply(note, binding, plan, factory)
    retry = release._activation_plan(
        note, binding, factory, store.probe_activation_outcome
    )

    assert plan["transition"] == "v15_successor"
    assert plan["would_change"] is True
    assert applied["changed"] is True
    assert applied["current_epoch"]["epoch_id"] == binding["epoch_id"]
    assert len(store.activation_calls) == 1
    assert not store.migration_calls
    assert retry["transition"] == "v15_noop"
    assert retry["would_change"] is False
    assert calls == [
        (release_files["control_db"], True),
        (release_files["control_db"], False),
        (release_files["control_db"], False),
        (release_files["control_db"], True),
    ]


def test_activation_v15_successor_gates_writer_before_activate(release_files):
    note, _raw, binding = _v15_successor_note(release_files)
    store = FakeStore(
        observed_control_schema_version=release.CONTROL_SCHEMA_V15,
        binary_write_schema_version=release.CONTROL_SCHEMA_V15,
    )
    read_capability = store.schema_runtime_capability()
    invalid_write_capability = {
        **read_capability,
        "mode": "successor_read_only",
        "write_enabled": False,
        "work_admission_enabled": False,
        "lease_acquisition_enabled": False,
        "external_effect_enabled": False,
    }

    def capability():
        return dict(read_capability if store.read_only else invalid_write_capability)

    store.schema_runtime_capability = capability
    with pytest.raises(
        release.ReleaseError, match="control_schema_v15_write_unavailable"
    ):
        release._activation_apply(
            note,
            binding,
            {"would_change": True, "transition": "v15_successor"},
            _factory(store, []),
        )
    assert not store.activation_calls


@pytest.mark.parametrize("failure", ["predecessor", "inflight", "fence"])
def test_activation_v15_successor_fails_closed_before_apply(release_files, failure):
    note, _raw, binding = _v15_successor_note(release_files)
    store = FakeStore(
        observed_control_schema_version=release.CONTROL_SCHEMA_V15,
        binary_write_schema_version=release.CONTROL_SCHEMA_V15,
    )
    if failure == "predecessor":
        store.predecessor["binding_fingerprint"] = "f" * 64
        expected = "activation_predecessor_binding_changed"
    elif failure == "inflight":
        store.predecessor["inflight"]["total"] = 1
        expected = "activation_predecessor_inflight_not_drained"
    else:
        store.probe_activation_outcome = lambda *_args, **_kwargs: "unknown"
        expected = "activation_successor_outcome_unknown"

    with pytest.raises(release.ReleaseError, match=expected):
        release._activation_plan(
            note, binding, _factory(store, []), store.probe_activation_outcome
        )

    assert not store.activation_calls


def test_activation_v15_retry_with_different_epoch_is_unknown(release_files):
    note = release_files["note"]
    binding = release._bound_binding(release_files["note_raw"], note)
    store = FakeStore(
        current={"epoch_id": "other-epoch", "state": "steady_active"},
        observed_control_schema_version=release.CONTROL_SCHEMA_V15,
        binary_write_schema_version=release.CONTROL_SCHEMA_V15,
    )

    with pytest.raises(
        release.ReleaseError, match="activation_migration_outcome_unknown"
    ):
        release._activation_plan(note, binding, _factory(store, []))


def test_activation_plan_rejects_v14_without_predecessor(release_files):
    note = json.loads(json.dumps(release_files["note"]))
    note["activation"].update({
        "expected_predecessor_epoch_id": "",
        "expected_predecessor_state": "",
        "expected_predecessor_binding_fingerprint": "",
    })
    binding = release._bound_binding(release_files["note_raw"], release_files["note"])
    store = FakeStore(current={}, predecessor=None)

    with pytest.raises(release.ReleaseError, match="activation_predecessor_required"):
        release._activation_plan(note, binding, _factory(store, []))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("mode", "current_write"),
        ("read_supported", False),
        ("write_enabled", True),
        ("external_effect_enabled", True),
    ],
)
def test_activation_plan_rejects_forged_v14_read_capability(
    release_files, field, value
):
    note = release_files["note"]
    binding = release._bound_binding(release_files["note_raw"], note)
    store = FakeStore()
    capability = store.schema_runtime_capability()
    capability[field] = value
    store.schema_runtime_capability = lambda: dict(capability)

    with pytest.raises(
        release.ReleaseError, match="control_schema_v14_read_unavailable"
    ):
        release._activation_plan(note, binding, _factory(store, []))

    assert not store.activation_calls


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("mode", "explicit_read_only"),
        ("read_supported", False),
        ("write_enabled", False),
        ("external_effect_enabled", False),
    ],
)
def test_activation_apply_rejects_forged_v15_write_capability_before_restart(
    release_files, field, value
):
    note = release_files["note"]
    binding = release._bound_binding(release_files["note_raw"], note)
    store = FakeStore(
        observed_control_schema_version=release.CONTROL_SCHEMA_V15,
        binary_write_schema_version=release.CONTROL_SCHEMA_V15,
    )
    store._record_activation({
        "epoch_id": binding["epoch_id"],
        "release_fingerprint_sha256": binding["release_fingerprint_sha256"],
        "release_note_sha256": binding["release_note_sha256"],
        "config_sha256": binding["config_sha256"],
        "db_logical_identity": binding["db_logical_identity"],
        "partition_start_fence": binding["partition_start_fence"],
    })
    capability = {
        **store.schema_runtime_capability(),
        "mode": "current_write",
        "write_enabled": True,
        "work_admission_enabled": True,
        "lease_acquisition_enabled": True,
        "external_effect_enabled": True,
    }
    capability[field] = value
    store.schema_runtime_capability = lambda: dict(capability)

    with pytest.raises(
        release.ReleaseError, match="control_schema_v15_write_unavailable"
    ):
        release._activation_apply(
            note,
            binding,
            {"would_change": False, "transition": "v15_noop"},
            _factory(store, []),
        )

    assert not store.activation_calls


def test_activation_uses_atomic_migration_and_hides_legacy_columns(release_files):
    note = dict(release_files["note"])
    binding = release._bound_binding(release_files["note_raw"], note)
    store = FakeStore()
    calls = []
    factory = _factory(store, calls)

    plan = release._activation_plan(note, binding, factory)
    applied = release._activation_apply(
        note,
        binding,
        plan,
        factory,
        store.migrate_v14_to_v15_and_activate,
    )

    assert calls == [
        (release_files["control_db"], True),
        (release_files["control_db"], False),
    ]
    assert plan["transition"] == "v14_to_v15_atomic"
    assert len(store.migration_calls) == 1
    assert not store.activation_calls
    assert applied["current_epoch"]["epoch_id"] == binding["epoch_id"]
    assert "capsule" not in json.dumps(applied)


def test_activation_real_store_atomic_migration_and_exact_v15_retry(release_files):
    note, _note_raw, binding = _seed_real_v14_predecessor_note(release_files)
    strict_state = "AND epoch.state = 'steady_active'"
    legacy_state = "AND epoch.state IN ('bounded_active', 'steady_active')"
    with sqlite3.connect(release_files["control_db"]) as conn:
        strict = str(
            conn.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'trigger' "
                "AND name = "
                "'trg_terminal_rerun_delivery_authority_binding_guard'"
            ).fetchone()[0]
        )
        assert strict.count(strict_state) == 1
        conn.execute(
            "DROP TRIGGER trg_terminal_rerun_delivery_authority_binding_guard"
        )
        conn.execute(strict.replace(strict_state, legacy_state, 1))

    plan = release._activation_plan(note, binding, release._open_store)
    applied = release._activation_apply(note, binding, plan, release._open_store)
    status = release._activation_status(note, binding, release._open_store)
    retry = release._activation_plan(note, binding, release._open_store)
    retried = release._activation_apply(note, binding, retry, release._open_store)

    assert plan["transition"] == "v14_to_v15_atomic"
    assert applied == {"changed": True, "current_epoch": status}
    assert retry["transition"] == "v15_noop"
    assert retried == {"changed": False, "current_epoch": status}
    with sqlite3.connect(release_files["control_db"]) as conn:
        migrated_guard = str(
            conn.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'trigger' "
                "AND name = "
                "'trg_terminal_rerun_delivery_authority_binding_guard'"
            ).fetchone()[0]
        )
    assert strict_state in migrated_guard
    assert legacy_state not in migrated_guard
    assert release._open_store(
        release_files["control_db"], False
    ).schema_runtime_capability() == {
        "observed_control_schema_version": release.CONTROL_SCHEMA_V15,
        "binary_write_schema_version": release.CONTROL_SCHEMA_V15,
        "mode": "current_write",
        "read_supported": True,
        "write_enabled": True,
        "work_admission_enabled": True,
        "lease_acquisition_enabled": True,
        "external_effect_enabled": True,
    }


def test_activation_real_store_v15_successor_driver_and_exact_retry(release_files):
    note, _note_raw, binding = _seed_real_v14_predecessor_note(release_files)
    release.RcaControlStore.migrate_v14_to_v15_and_activate(
        release_files["control_db"],
        **_atomic_activation_kwargs(note, binding),
    )
    migrated = release._open_store(release_files["control_db"], False)
    predecessor = migrated.direct_steady_predecessor()
    assert predecessor is not None

    successor = json.loads(json.dumps(note))
    activation = successor["activation"]
    activation.update({
        "epoch_id": "rca-activation-r15bd-real-successor-20260819",
        "expected_control_schema_version": release.CONTROL_SCHEMA_V15,
        "target_control_schema_version": release.CONTROL_SCHEMA_V15,
        "expected_predecessor_epoch_id": predecessor["epoch_id"],
        "expected_predecessor_state": predecessor["state"],
        "expected_predecessor_binding_fingerprint": predecessor[
            "binding_fingerprint"
        ],
    })
    activation["epoch_contract_sha256"] = (
        release.minimal_release_epoch_contract_sha256(activation)
    )
    successor_raw = _write_json(release_files["note_path"], successor)
    successor_binding = release._bound_binding(successor_raw, successor)

    plan = release._activation_plan(
        successor,
        successor_binding,
        release._open_store,
    )
    applied = release._activation_apply(
        successor,
        successor_binding,
        plan,
        release._open_store,
    )
    retry = release._activation_plan(
        successor,
        successor_binding,
        release._open_store,
    )
    retried = release._activation_apply(
        successor,
        successor_binding,
        retry,
        release._open_store,
    )

    assert plan["transition"] == "v15_successor"
    assert applied["changed"] is True
    assert applied["current_epoch"]["epoch_id"] == successor_binding["epoch_id"]
    assert retry["transition"] == "v15_noop"
    assert retried == {"changed": False, "current_epoch": applied["current_epoch"]}
    current = release._open_store(release_files["control_db"], False).activation_epoch()
    assert current["epoch_id"] == successor_binding["epoch_id"]
    assert current["state"] == "steady_active"


def test_activation_plan_and_status_reject_hidden_v15_binding_tamper(release_files):
    note, _note_raw, binding = _seed_real_v14_predecessor_note(release_files)
    release.RcaControlStore.migrate_v14_to_v15_and_activate(
        release_files["control_db"],
        **_atomic_activation_kwargs(note, binding),
    )
    with sqlite3.connect(release_files["control_db"]) as conn:
        conn.execute(
            "UPDATE rca_activation_transition_audit "
            "SET binding_fingerprint = ? WHERE audit_id = "
            "(SELECT MAX(audit_id) FROM rca_activation_transition_audit)",
            ("f" * 64,),
        )

    with pytest.raises(
        release.ReleaseError,
        match="activation_predecessor_binding_invalid",
    ):
        release._activation_plan(note, binding, release._open_store)
    with pytest.raises(
        release.ReleaseError,
        match="activation_predecessor_binding_invalid",
    ):
        release._activation_status(note, binding, release._open_store)


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

    def migrate_after_quiesce(path, **kwargs):
        assert not runner.loaded
        assert (
            release_files["live_env"].read_bytes()
            == release_files["env_source"].read_bytes()
        )
        assert (
            release_files["live_manifest"].read_bytes()
            == release_files["manifest_source"].read_bytes()
        )
        return store.migrate_v14_to_v15_and_activate(path, **kwargs)

    result = release.apply_release(
        **_plan_args(release_files),
        confirm_release_id=release_files["note"]["release_id"],
        receipt=receipt,
        runner=runner,
        store_factory=_factory(store, calls),
        migration_apply=migrate_after_quiesce,
        outcome_probe=store.probe_activation_outcome,
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
    assert store.migration_calls
    assert result["activation_outcome"] == "committed"
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
        if target == release_files["live_env"] and ".minimal-preimage-" in source.name:
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
    assert (
        release_files["live_env"].read_bytes()
        == release_files["env_source"].read_bytes()
    )
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
            **_migration_args(store),
        )
    assert store.migration_calls
    failed = json.loads(receipt.read_bytes())
    assert failed["transaction_state"] == "failed"
    assert failed["activation_committed"] is True
    assert failed["activation_outcome"] == "committed"
    assert failed["rollback_ceiling"] == "successor_read_only_or_forward_fix"
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
            assert started["activation_attempted"] is False
            assert started["activation_committed"] is False
            assert started["activation_outcome_known"] is False
            assert started["activation_outcome"] == "unknown"
            assert started["rollback_ceiling"] == "operator_adjudication_required"
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

    monkeypatch.setattr(
        release, "_assert_all_residents_stopped", fail_before_activation
    )
    store = FakeStore()
    with pytest.raises(release.ReleaseError, match="preactivation_injected"):
        release.apply_release(
            **_plan_args(release_files),
            confirm_release_id=release_files["note"]["release_id"],
            receipt=receipt,
            runner=runner,
            store_factory=_factory(store, []),
            **_migration_args(store),
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
            **_migration_args(store),
        )

    failed = json.loads(receipt.read_bytes())
    assert failed["transaction_state"] == "failed"
    assert failed["activation_attempted"] is True
    assert failed["activation_committed"] is True
    assert failed["activation_outcome"] == "committed"
    assert failed["rollback_ceiling"] == "successor_read_only_or_forward_fix"
    assert failed["artifacts_restored"] is False
    assert failed["resident_stop"]["all_stopped"] is True
    assert not runner.loaded
    assert store.current["epoch_id"] == release_files["note"]["activation"]["epoch_id"]
    assert (
        release_files["live_env"].read_bytes()
        == release_files["env_source"].read_bytes()
    )
    assert (
        release_files["live_manifest"].read_bytes()
        == release_files["manifest_source"].read_bytes()
    )


def test_apply_activation_store_rollback_restores_artifacts(release_files, monkeypatch):
    receipt = release_files["note_path"].with_name("activation-rollback.json")
    runner = FakeRunner(release_files)
    store = FakeStore()
    observed_checkpoint = []
    real_stop = release._stop_all_residents

    def stop_after_outcome_checkpoint(current_runner):
        checkpoint = json.loads(receipt.read_bytes())
        observed_checkpoint.append(checkpoint)
        return real_stop(current_runner)

    monkeypatch.setattr(
        release,
        "_activation_apply",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            release.ReleaseError("activation_predecessor_binding_changed")
        ),
    )
    monkeypatch.setattr(release, "_stop_all_residents", stop_after_outcome_checkpoint)

    with pytest.raises(
        release.ReleaseError, match="activation_predecessor_binding_changed"
    ):
        release.apply_release(
            **_plan_args(release_files),
            confirm_release_id=release_files["note"]["release_id"],
            receipt=receipt,
            runner=runner,
            store_factory=_factory(store, []),
            outcome_probe=store.probe_activation_outcome,
        )

    failed = json.loads(receipt.read_bytes())
    assert failed["activation_attempted"] is True
    assert failed["activation_outcome_known"] is True
    assert failed["activation_committed"] is False
    assert failed["activation_outcome"] == "not_committed"
    assert failed["rollback_ceiling"] == "artifact_restore_permitted"
    assert failed["artifacts_restored"] is True
    assert failed["resident_stop"]["all_stopped"] is True
    assert len(observed_checkpoint) == 1
    checkpoint = observed_checkpoint[0]
    assert checkpoint["activation_attempted"] is True
    assert checkpoint["activation_committed"] is False
    assert checkpoint["activation_outcome_known"] is True
    assert checkpoint["activation_outcome"] == "not_committed"
    assert checkpoint["rollback_ceiling"] == "artifact_restore_permitted"
    assert release_files["live_env"].read_bytes() == b"OLD_ENV=1\n"
    assert release_files["live_manifest"].read_bytes() == b'{"old":true}\n'


def test_apply_unknown_activation_outcome_keeps_candidate_and_all_residents_stopped(
    release_files, monkeypatch
):
    receipt = release_files["note_path"].with_name("activation-unknown.json")
    runner = FakeRunner(release_files)
    store = FakeStore()

    def lose_commit_visibility(*_args, **_kwargs):
        checkpoint = json.loads(receipt.read_bytes())
        assert checkpoint["transaction_state"] == "started"
        assert checkpoint["activation_attempted"] is True
        assert checkpoint["activation_committed"] is False
        assert checkpoint["activation_outcome_known"] is False
        assert checkpoint["activation_outcome"] == "unknown"
        assert checkpoint["rollback_ceiling"] == "operator_adjudication_required"
        raise OSError("injected_commit_visibility_loss")

    monkeypatch.setattr(release, "_activation_apply", lose_commit_visibility)

    with pytest.raises(OSError, match="injected_commit_visibility_loss"):
        release.apply_release(
            **_plan_args(release_files),
            confirm_release_id=release_files["note"]["release_id"],
            receipt=receipt,
            runner=runner,
            store_factory=_factory(store, []),
            outcome_probe=lambda *_args, **_kwargs: "unknown",
        )

    failed = json.loads(receipt.read_bytes())
    assert failed["activation_attempted"] is True
    assert failed["activation_outcome"] == "unknown"
    assert failed["rollback_ceiling"] == "operator_adjudication_required"
    assert failed["activation_outcome_known"] is False
    assert failed["activation_committed"] is False
    assert failed["artifacts_restored"] is False
    assert failed["rollback_error"] == "activation_outcome_unknown"
    assert failed["resident_stop"]["all_stopped"] is True
    assert not runner.loaded
    assert (
        release_files["live_env"].read_bytes()
        == release_files["env_source"].read_bytes()
    )
    assert (
        release_files["live_manifest"].read_bytes()
        == release_files["manifest_source"].read_bytes()
    )
    assert all(Path(path).exists() for path in failed["rollback_recovery_paths"])


def test_apply_committed_checkpoint_survives_terminal_interrupt_and_blocks_retry(
    release_files, monkeypatch
):
    class SimulatedSigkill(BaseException):
        pass

    receipt = release_files["note_path"].with_name("committed-interrupted.json")
    runner = FakeRunner(release_files)
    store = FakeStore()
    receipt_identity = []
    real_quiesce = release._quiesce_residents

    def capture_reserved_identity(current_runner):
        observed = receipt.stat()
        receipt_identity.append((observed.st_dev, observed.st_ino))
        return real_quiesce(current_runner)

    def fail_after_commit(*_args, **_kwargs):
        raise release.ReleaseError("injected_before_terminal_receipt")

    def interrupt_terminal_handling(_runner):
        checkpoint = json.loads(receipt.read_bytes())
        assert checkpoint["transaction_state"] == "started"
        assert checkpoint["activation_attempted"] is True
        assert checkpoint["activation_committed"] is True
        assert checkpoint["activation_outcome_known"] is True
        assert checkpoint["activation_outcome"] == "committed"
        assert checkpoint["rollback_ceiling"] == (
            "successor_read_only_or_forward_fix"
        )
        raise SimulatedSigkill

    monkeypatch.setattr(release, "_quiesce_residents", capture_reserved_identity)
    monkeypatch.setattr(release, "_restart", fail_after_commit)
    monkeypatch.setattr(release, "_stop_all_residents", interrupt_terminal_handling)

    with pytest.raises(SimulatedSigkill):
        release.apply_release(
            **_plan_args(release_files),
            confirm_release_id=release_files["note"]["release_id"],
            receipt=receipt,
            runner=runner,
            store_factory=_factory(store, []),
            **_migration_args(store),
        )

    checkpoint_raw = receipt.read_bytes()
    checkpoint = json.loads(checkpoint_raw)
    assert checkpoint["activation_outcome"] == "committed"
    assert checkpoint["rollback_ceiling"] == "successor_read_only_or_forward_fix"
    observed = receipt.stat()
    assert (observed.st_dev, observed.st_ino) == receipt_identity[0]

    with pytest.raises(release.ReleaseError, match="receipt_exists"):
        release.apply_release(
            **_plan_args(release_files),
            confirm_release_id=release_files["note"]["release_id"],
            receipt=receipt,
            runner=runner,
            store_factory=_factory(store, []),
            **_migration_args(store),
        )
    assert receipt.read_bytes() == checkpoint_raw


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
    store = FakeStore()
    with pytest.raises(OSError, match="injected cleanup failure"):
        release.apply_release(
            **_plan_args(release_files),
            confirm_release_id=release_files["note"]["release_id"],
            receipt=receipt,
            runner=runner,
            store_factory=_factory(store, []),
            **_migration_args(store),
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
def test_apply_rejects_nonpositive_or_nonfinite_restart_timeout(release_files, timeout):
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


def test_bounded_quiesced_phase_restores_exact_resident_shape(release_files):
    runner = FakeRunner(release_files)
    for label, *_rest in release.REQUIRED_RESIDENTS:
        runner.disabled[label] = False
    for label in release.DISABLED_RESIDENTS:
        runner.disabled[label] = True
    original_loaded = set(runner.loaded)
    original_disabled = dict(runner.disabled)
    receipt = release_files["note_path"].with_name("bounded-window.json")

    def phase(token, lock, quiesce):
        assert not runner.loaded
        assert lock[0] == release_files["home"] / "runtime" / release.RELEASE_LOCK_NAME
        assert token["token_sha256"] == release._sha(
            release._canonical({key: value for key, value in token.items() if key != "token_sha256"})
        )
        assert quiesce["token"] == token
        return {"applied": False, "stage": "prepare-plan"}

    result = release._run_bounded_quiesced_phase(
        release_id="rca-bounded-window-20260822",
        home=release_files["home"],
        receipt=receipt,
        phase=phase,
        runner=runner,
    )

    assert result["transaction_state"] == "completed"
    assert result["restoration"]["attempted"] is True
    assert runner.loaded == original_loaded
    assert runner.disabled == original_disabled
    assert stat.S_IMODE(receipt.stat().st_mode) == 0o600
    assert json.loads(receipt.read_bytes())["token"]["token_sha256"]
    assert not (release_files["home"] / "runtime" / release.RELEASE_LOCK_NAME).exists()


def test_bounded_quiesced_phase_restores_prepare_failure(release_files):
    runner = FakeRunner(release_files)
    for label, *_rest in release.REQUIRED_RESIDENTS:
        runner.disabled[label] = False
    for label in release.DISABLED_RESIDENTS:
        runner.disabled[label] = True
    original_loaded = set(runner.loaded)
    original_disabled = dict(runner.disabled)
    receipt = release_files["note_path"].with_name("bounded-window-failed.json")

    def fail(*_args):
        assert not runner.loaded
        raise release.ReleaseError("prepare_control_snapshot_invalid")

    with pytest.raises(release.ReleaseError, match="prepare_control_snapshot_invalid"):
        release._run_bounded_quiesced_phase(
            release_id="rca-bounded-window-failed-20260822",
            home=release_files["home"],
            receipt=receipt,
            phase=fail,
            runner=runner,
            hold_for_apply=True,
        )

    failed = json.loads(receipt.read_bytes())
    assert failed["transaction_state"] == "failed"
    assert failed["restoration"]["attempted"] is True
    assert failed["token"]["token_sha256"]
    assert runner.loaded == original_loaded
    assert runner.disabled == original_disabled
    assert not (release_files["home"] / "runtime" / release.RELEASE_LOCK_NAME).exists()


def test_bounded_quiesced_phase_restores_loaded_but_persistently_disabled(
    release_files,
):
    runner = FakeRunner(release_files)
    first = release.REQUIRED_RESIDENTS[0][0]
    for label in release._all_resident_labels():
        runner.disabled[label] = label in release.DISABLED_RESIDENTS or label == first
    original_loaded = set(runner.loaded)
    original_disabled = dict(runner.disabled)
    receipt = release_files["note_path"].with_name("bounded-disabled-loaded.json")

    result = release._run_bounded_quiesced_phase(
        release_id="rca-bounded-disabled-loaded-20260822",
        home=release_files["home"],
        receipt=receipt,
        phase=lambda *_args: {"applied": False},
        runner=runner,
    )

    assert result["transaction_state"] == "completed"
    assert runner.loaded == original_loaded
    assert runner.disabled == original_disabled


def test_activate_runs_prepare_plan_apply_in_one_quiesced_window(
    release_files, monkeypatch
):
    prepared = _prepare_fixture(release_files)
    runner = FakeRunner(release_files)
    sequence = []
    apply_receipt = prepared["note_path"].with_name("activate-apply.json")
    window_receipt = prepared["note_path"].with_name("activate-window.json")

    def prepare(**kwargs):
        sequence.append("prepare")
        assert not runner.loaded
        assert kwargs["release_id"] == prepared["args"]["release_id"]
        return {
            "outputs": {
                "manifest": {"path": str(prepared["manifest_output"])},
                "env": {"path": str(prepared["env_output"])},
            },
            "templates": {
                "manifest": {"sha256": "a" * 64},
                "env": {"sha256": "b" * 64},
            },
        }

    def plan(**kwargs):
        sequence.append("plan")
        assert not runner.loaded
        assert kwargs["expected_manifest_sha256"] == "a" * 64
        assert kwargs["expected_env_sha256"] == "b" * 64
        return {"ok": True, "mode": "plan"}

    def apply(**kwargs):
        sequence.append("apply")
        assert not runner.loaded
        assert kwargs["_release_lock"][0] == (
            release_files["home"] / "runtime" / release.RELEASE_LOCK_NAME
        )
        assert kwargs["_prequiesce"]["token"]["token_sha256"]
        for label, *_rest in release.REQUIRED_RESIDENTS:
            runner(("/bin/launchctl", "enable", f"gui/{os.getuid()}/{label}"))
            runner((
                "/bin/launchctl",
                "bootstrap",
                f"gui/{os.getuid()}",
                str(release_files["home"].parent / "Library/LaunchAgents" / f"{label}.plist"),
            ))
        for label in release.DISABLED_RESIDENTS:
            runner(("/bin/launchctl", "disable", f"gui/{os.getuid()}/{label}"))
        return {"applied": True, "mode": "apply"}

    monkeypatch.setattr(release, "prepare_release", prepare)
    monkeypatch.setattr(release, "build_plan", plan)
    monkeypatch.setattr(release, "apply_release", apply)

    result = release.activate_release(
        prepare_inputs=prepared["args"],
        confirm_release_id=prepared["args"]["release_id"],
        receipt=apply_receipt,
        quiesce_receipt=window_receipt,
        runner=runner,
    )

    assert sequence == ["prepare", "plan", "apply"]
    assert result["mode"] == "activate"
    assert result["applied"] is True
    window = json.loads(window_receipt.read_bytes())
    assert window["transaction_state"] == "completed"
    assert window["restoration"] == {
        "attempted": False,
        "reason": "apply_completed",
    }
    assert runner.loaded == {row[0] for row in release.REQUIRED_RESIDENTS}
    assert not (release_files["home"] / "runtime" / release.RELEASE_LOCK_NAME).exists()


def test_activate_executes_real_prepare_plan_apply_with_fake_boundaries(
    release_files, monkeypatch
):
    prepared = _prepare_fixture(release_files)
    runner = FakeRunner(release_files)
    store = prepared["store"]
    residents = _synthetic_resident_profile(release_files)

    def restart(*_args, **_kwargs):
        for label, *_rest in release.REQUIRED_RESIDENTS:
            runner((
                "/bin/launchctl",
                "bootstrap",
                f"gui/{os.getuid()}",
                str(release_files["home"].parent / "Library/LaunchAgents" / f"{label}.plist"),
            ))
        return residents

    monkeypatch.setattr(release, "_restart", restart)
    monkeypatch.setattr(
        release, "_resident_profile_readback", lambda *_args, **_kwargs: residents
    )
    apply_receipt = prepared["note_path"].with_name("real-activate-apply.json")
    window_receipt = prepared["note_path"].with_name("real-activate-window.json")
    real_build_plan = release.build_plan
    plan_calls = []

    def counted_build_plan(**kwargs):
        plan_calls.append(1)
        return real_build_plan(**kwargs)

    monkeypatch.setattr(release, "build_plan", counted_build_plan)

    result = release.activate_release(
        prepare_inputs=prepared["args"],
        confirm_release_id=prepared["args"]["release_id"],
        receipt=apply_receipt,
        quiesce_receipt=window_receipt,
        runner=runner,
        migration_apply=store.migrate_v14_to_v15_and_activate,
        outcome_probe=store.probe_activation_outcome,
    )

    assert result["applied"] is True
    assert plan_calls == [1]
    assert json.loads(apply_receipt.read_bytes())["transaction_state"] == "completed"
    assert json.loads(window_receipt.read_bytes())["transaction_state"] == "completed"
    assert store.migration_calls
    assert runner.loaded == {row[0] for row in release.REQUIRED_RESIDENTS}
    assert not (release_files["home"] / "runtime" / release.RELEASE_LOCK_NAME).exists()


def test_activate_apply_failure_keeps_apply_fail_closed_resident_stop(
    release_files, monkeypatch
):
    prepared = _prepare_fixture(release_files)
    runner = FakeRunner(release_files)
    apply_receipt = prepared["note_path"].with_name("failed-activate-apply.json")
    window_receipt = prepared["note_path"].with_name("failed-activate-window.json")

    monkeypatch.setattr(
        release,
        "prepare_release",
        lambda **_kwargs: {
            "outputs": {
                "manifest": {"path": str(prepared["manifest_output"])},
                "env": {"path": str(prepared["env_output"])},
            },
            "templates": {
                "manifest": {"sha256": "a" * 64},
                "env": {"sha256": "b" * 64},
            },
        },
    )
    monkeypatch.setattr(release, "build_plan", lambda **_kwargs: {"ok": True})

    def fail_apply(**_kwargs):
        assert not runner.loaded
        raise release.ReleaseError("activation_migration_outcome_unknown")

    monkeypatch.setattr(release, "apply_release", fail_apply)

    with pytest.raises(
        release.ReleaseError, match="activation_migration_outcome_unknown"
    ):
        release.activate_release(
            prepare_inputs=prepared["args"],
            confirm_release_id=prepared["args"]["release_id"],
            receipt=apply_receipt,
            quiesce_receipt=window_receipt,
            runner=runner,
        )

    failed = json.loads(window_receipt.read_bytes())
    assert failed["transaction_state"] == "failed"
    assert failed["restoration"] == {
        "attempted": False,
        "reason": "apply_fail_closed",
    }
    assert not runner.loaded
    assert not (release_files["home"] / "runtime" / release.RELEASE_LOCK_NAME).exists()


def test_quiesce_persists_profile_and_recovers_loaded_service_without_pid(
    release_files, monkeypatch
):
    first = release.REQUIRED_RESIDENTS[0][0]
    pids = {
        row[0]: (None if row[0] == first else 1000 + index)
        for index, row in enumerate(release.REQUIRED_RESIDENTS)
    }
    pids.update({
        label: 2000 + index for index, label in enumerate(release.DISABLED_RESIDENTS)
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
    assert all(
        runner.disabled[label] is False for label, *_ in release.REQUIRED_RESIDENTS
    )
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


def test_quiesce_waits_for_asynchronous_launchd_bootout(release_files, monkeypatch):
    runner = FakeRunner(release_files)
    real_assert_stopped = release._assert_all_residents_stopped
    attempts = 0
    sleeps = []

    def delayed_readback(current_runner):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise release.ReleaseError("resident_quiesce_readback_failed")
        return real_assert_stopped(current_runner)

    monkeypatch.setattr(release, "_assert_all_residents_stopped", delayed_readback)
    monkeypatch.setattr(release.time, "sleep", sleeps.append)

    quiesce = release._quiesce_residents(runner)

    assert not runner.loaded
    assert quiesce["stopped"] == [
        {"label": label, "loaded": False, "pid": None}
        for label in release._all_resident_labels()
    ]
    assert sleeps == [0.25, 0.25]


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
                    "after_sha256": data["note"]["runtime_projection"]["env_sha256"],
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
            "activation_outcome": "committed",
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
    "old_schema",
    [
        "pnc_rca_minimal_release_apply_receipt_v1",
        "pnc_rca_minimal_release_apply_receipt_v2",
    ],
)
def test_verify_rejects_undeployed_apply_receipt(
    release_files, monkeypatch, old_schema
):
    store = _configure_verify_fixture(release_files, monkeypatch)
    receipt = json.loads(release_files["apply_receipt"].read_bytes())
    receipt["schema_version"] = old_schema
    _write_json(release_files["apply_receipt"], receipt)

    assert release.RECEIPT_SCHEMA == "pnc_rca_minimal_release_apply_receipt_v3"
    with pytest.raises(
        release.ReleaseError,
        match="apply_receipt_schema_unsupported",
    ):
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
        "activation_outcome",
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
    elif tamper == "activation_outcome":
        receipt["activation_outcome"] = "unknown"
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
