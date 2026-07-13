from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
from types import SimpleNamespace

import pytest

from gateway.pnc_rca_control_store import (
    CONTROL_STORE_SCHEMA_VERSION,
    RcaControlStore,
)
from gateway.pnc_rca_delivery_store import (
    DELIVERY_STORE_SCHEMA_VERSION,
    RcaDeliveryStore,
)
from scripts import pnc_rca_store_migration_drill as drill_module
from scripts.pnc_rca_store_migration_drill import (
    CONTROL_PREDECESSOR_SCHEMA_VERSION,
    DELIVERY_PREDECESSOR_SCHEMA_VERSION,
    PREDECESSOR_COMPATIBILITY_PROBE,
    STORE_WRITER_LABELS,
    MigrationDrillError,
    main,
    observe_regular_file,
    run_migration_drill,
    write_receipt_atomic,
)


NOW = datetime(2026, 7, 11, 3, 0, tzinfo=timezone.utc)
MATERIALIZATION_AUDIT = {
    "release_id": "rca-release-test-20260711",
    "bootstrap_epoch_id": "bootstrap-epoch-test-20260711",
    "operator": "migration-test",
    "reason": "exercise fresh install materialization",
}
QUARANTINE_AUDIT = {
    "release_id": "rca-release-quarantine-20260711",
    "operator": "rollback-test",
    "reason": "quarantine the fresh install without deleting it",
}
RESTORE_AUDIT = {
    "release_id": "rca-release-restore-20260711",
    "operator": "restore-test",
    "reason": "restore the exact quarantined fresh install",
}


@pytest.fixture(autouse=True)
def _isolate_candidate_provenance(monkeypatch):
    def candidate(repo_root):
        root = Path(repo_root).resolve()
        commit = "a" * 40
        if (root / ".git").is_dir():
            commit = subprocess.run(
                ["git", "-C", str(root), "rev-parse", "HEAD"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
        return {
            "repo_root": str(root),
            "commit": commit,
            "migration_sources": {
                relative: "b" * 64
                for relative in drill_module.MIGRATION_SOURCE_RELATIVE_PATHS
            },
        }

    monkeypatch.setattr(
        drill_module,
        "_candidate_provenance",
        candidate,
    )


def _writer_stop(now: datetime = NOW) -> dict:
    return {
        "schema_version": "pnc_rca_writer_stop_evidence_v1",
        "observed_at": now.isoformat(),
        "services": {
            label: {
                "observed_at": now.isoformat(),
                "pid_state": "pid_absent",
                "health_state": "stopped",
            }
            for label in STORE_WRITER_LABELS
        },
    }


def _current_store(path: Path, *, control: bool, delivery: bool) -> None:
    if control:
        RcaControlStore(path)
    if delivery:
        RcaDeliveryStore(path)
    drill_module._checkpoint_restore(path)


def _writer_probe(*, live_label: str = "") -> dict:
    return {
        label: {
            "launchd_job_state": "present" if label == live_label else "absent",
            "matching_pids": [43210] if label == live_label else [],
        }
        for label in STORE_WRITER_LABELS
    }


def _materialize_fresh(tmp_path: Path, source: Path) -> dict:
    _run(tmp_path, control=source, delivery=source)
    return _apply_fresh_materialization(tmp_path, source)


def _apply_fresh_materialization(tmp_path: Path, source: Path) -> dict:
    return drill_module.materialize_fresh_install(
        migration_receipt_path=(
            tmp_path / "evidence" / "store_migration_receipt.json"
        ),
        control_db_path=source,
        delivery_db_path=source,
        config_sha256="c" * 64,
        evidence_dir=tmp_path / "evidence",
        writer_stop_evidence=_writer_stop(),
        writer_process_probe=_writer_probe,
        apply=True,
        now=NOW,
        **MATERIALIZATION_AUDIT,
    )


class _SyntheticProcessCrash(BaseException):
    pass


def _leave_materialization_stage_linked(
    tmp_path: Path,
    source: Path,
    monkeypatch,
) -> Path:
    original_unlink = os.unlink

    def crash_before_stage_unlink(path, *args, **kwargs):
        name = str(path)
        if (
            kwargs.get("dir_fd") is not None
            and name.startswith(f".{source.name}.genesis-")
            and name.endswith(".sqlite3")
        ):
            raise _SyntheticProcessCrash
        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(os, "unlink", crash_before_stage_unlink)
    with pytest.raises(_SyntheticProcessCrash):
        _apply_fresh_materialization(tmp_path, source)
    monkeypatch.setattr(os, "unlink", original_unlink)
    [stage_path] = list(
        source.parent.glob(f".{source.name}.genesis-*.sqlite3")
    )
    assert (stage_path.stat().st_dev, stage_path.stat().st_ino) == (
        source.stat().st_dev,
        source.stat().st_ino,
    )
    assert source.stat().st_nlink == 2
    return stage_path


def _quarantine_fresh(
    tmp_path: Path,
    source: Path,
    *,
    apply: bool = True,
    writer_process_probe=_writer_probe,
    now: datetime = NOW,
    writer_stop: dict | None = None,
    max_writer_stop_age_seconds: int = 900,
) -> dict:
    quarantine_dir = tmp_path / "quarantine"
    quarantine_dir.mkdir(mode=0o700, exist_ok=True)
    return drill_module.quarantine_fresh_install(
        materialization_receipt_path=(
            tmp_path / "evidence" / "fresh_install_materialization_receipt.json"
        ),
        control_db_path=source,
        delivery_db_path=source,
        config_sha256="c" * 64,
        evidence_dir=tmp_path / "evidence",
        quarantine_dir=quarantine_dir,
        writer_stop_evidence=writer_stop or _writer_stop(now),
        writer_process_probe=writer_process_probe,
        apply=apply,
        now=now,
        max_writer_stop_age_seconds=max_writer_stop_age_seconds,
        **(QUARANTINE_AUDIT if apply else {}),
    )


def _restore_fresh(
    tmp_path: Path,
    source: Path,
    *,
    apply: bool = True,
    writer_process_probe=_writer_probe,
    now: datetime = NOW,
    writer_stop: dict | None = None,
    max_writer_stop_age_seconds: int = 900,
) -> dict:
    return drill_module.restore_fresh_install_from_quarantine(
        quarantine_receipt_path=(
            tmp_path / "evidence" / "fresh_install_quarantine_receipt.json"
        ),
        control_db_path=source,
        delivery_db_path=source,
        config_sha256="c" * 64,
        evidence_dir=tmp_path / "evidence",
        quarantine_dir=tmp_path / "quarantine",
        writer_stop_evidence=writer_stop or _writer_stop(now),
        writer_process_probe=writer_process_probe,
        apply=apply,
        now=now,
        max_writer_stop_age_seconds=max_writer_stop_age_seconds,
        **(RESTORE_AUDIT if apply else {}),
    )


def _leave_partial_restore_copy(
    tmp_path: Path,
    source: Path,
    monkeypatch,
) -> Path:
    original_write = os.write
    crashed = False

    def crash_mid_copy(descriptor, value):
        nonlocal crashed
        if not crashed and len(value) > 1:
            written = original_write(descriptor, value[: max(1, len(value) // 2)])
            assert written > 0
            crashed = True
            raise _SyntheticProcessCrash
        return original_write(descriptor, value)

    monkeypatch.setattr(os, "write", crash_mid_copy)
    with pytest.raises(_SyntheticProcessCrash):
        _restore_fresh(tmp_path, source)
    monkeypatch.setattr(os, "write", original_write)
    [temporary] = list(
        source.parent.glob(f".{source.name}.*.restore-copy.partial.sqlite3")
    )
    [quarantine_artifact] = list(
        (tmp_path / "quarantine").glob("*.quarantined.sqlite3")
    )
    assert temporary.stat().st_mode & 0o777 == 0o600
    assert temporary.stat().st_nlink == 1
    assert 0 < temporary.stat().st_size < quarantine_artifact.stat().st_size
    return temporary


@pytest.mark.parametrize(
    "cmdline",
    [
        ["python", "-m", "hermes_cli.main", "gateway", "run", "--replace"],
        ["python", "/repo/hermes_cli/main.py", "gateway", "run"],
        ["python", "hermes_cli/main.py", "gateway", "run"],
        ["/venv/bin/hermes", "gateway", "run"],
        ["python", "/repo/gateway/run.py"],
    ],
)
def test_writer_probe_recognizes_every_supported_gateway_argv(
    monkeypatch, cmdline
):
    monkeypatch.setattr(
        drill_module.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=113,
            stdout="",
            stderr="Could not find service",
        ),
    )
    monkeypatch.setattr(
        drill_module.psutil,
        "process_iter",
        lambda _attrs: [SimpleNamespace(info={"pid": 43210, "cmdline": cmdline})],
    )

    observed = drill_module._default_writer_process_probe()

    assert observed["ai.hermes.gateway"] == {
        "launchd_job_state": "absent",
        "matching_pids": [43210],
    }


def test_writer_probe_does_not_match_gateway_words_inside_shell_script(monkeypatch):
    monkeypatch.setattr(
        drill_module.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=113,
            stdout="",
            stderr="Could not find service",
        ),
    )
    monkeypatch.setattr(
        drill_module.psutil,
        "process_iter",
        lambda _attrs: [
            SimpleNamespace(
                info={
                    "pid": 43210,
                    "cmdline": ["zsh", "-c", "echo hermes gateway run"],
                }
            )
        ],
    )

    observed = drill_module._default_writer_process_probe()

    assert observed["ai.hermes.gateway"]["matching_pids"] == []


def _run(
    tmp_path: Path,
    *,
    control: Path,
    delivery: Path,
    writer_stop: dict | None = None,
    predecessor_validator: Path | None = None,
    repo_root: Path = drill_module.REPO_ROOT,
) -> dict:
    return run_migration_drill(
        control_db_path=control,
        delivery_db_path=delivery,
        work_dir=tmp_path / "work",
        evidence_dir=tmp_path / "evidence",
        writer_stop_evidence=writer_stop or _writer_stop(),
        now=NOW,
        writer_process_probe=_writer_probe,
        predecessor_validator_path=predecessor_validator,
        repo_root=repo_root,
    )


def _validator_repo(
    tmp_path: Path,
    *,
    writes_stderr: bool = False,
) -> tuple[Path, Path]:
    repo = tmp_path / "validator-repo"
    executable = repo / "artifacts" / "pnc_rca_predecessor_validator"
    executable.parent.mkdir(parents=True)
    stderr_statement = (
        'import sys\nprint("unexpected diagnostic", file=sys.stderr)\n'
        if writes_stderr
        else ""
    )
    executable.write_text(
        f"""#!{sys.executable}
import argparse
import hashlib
import json
from pathlib import Path
import sqlite3

parser = argparse.ArgumentParser()
parser.add_argument("--database", required=True)
parser.add_argument("--roles-json", required=True)
parser.add_argument("--expected-schemas-json", required=True)
args = parser.parse_args()
roles = json.loads(args.roles_json)
expected = json.loads(args.expected_schemas_json)
database = Path(args.database)
uri = database.as_uri() + "?mode=ro&immutable=1"
connection = sqlite3.connect(uri, uri=True)
connection.execute("PRAGMA query_only=ON")
schemas = {{}}
if "control" in roles:
    schemas["control"] = connection.execute(
        "SELECT value FROM control_meta WHERE key='schema_version'"
    ).fetchone()[0]
if "delivery" in roles:
    schemas["delivery"] = connection.execute(
        "SELECT value FROM rca_delivery_meta WHERE key='schema_version'"
    ).fetchone()[0]
quick = connection.execute("PRAGMA quick_check").fetchone()[0]
foreign_keys = len(connection.execute("PRAGMA foreign_key_check").fetchall())
try:
    connection.execute("CREATE TABLE forbidden_write(value INTEGER)")
except sqlite3.OperationalError:
    write_probe = "blocked_readonly"
else:
    write_probe = "unexpected_write"
connection.close()
body = {{
    "schema_version": "pnc_rca_predecessor_validator_result_v1",
    "ok": schemas == expected,
    "read_only": True,
    "side_effects": "none",
    "database_sha256": hashlib.sha256(database.read_bytes()).hexdigest(),
    "roles": roles,
    "schemas": schemas,
    "quick_check": quick,
    "foreign_key_check_rows": foreign_keys,
    "write_probe": write_probe,
}}
{stderr_statement}print(json.dumps(body, ensure_ascii=True, sort_keys=True, separators=(",", ":")))
""",
        encoding="utf-8",
    )
    executable.chmod(0o755)
    subprocess.run(["git", "-C", str(repo), "init", "-q"], check=True)
    subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "-c",
            "user.name=RCA Migration Test",
            "-c",
            "user.email=rca-migration-test@example.invalid",
            "commit",
            "-q",
            "-m",
            "validator fixture",
        ],
        check=True,
    )
    return repo, executable


def _expected_schema_transitions() -> dict[str, dict[str, str]]:
    return {
        "control": {
            "from": CONTROL_PREDECESSOR_SCHEMA_VERSION,
            "to": CONTROL_STORE_SCHEMA_VERSION,
        },
        "delivery": {
            "from": DELIVERY_PREDECESSOR_SCHEMA_VERSION,
            "to": DELIVERY_STORE_SCHEMA_VERSION,
        },
    }


def test_existing_v9_shared_database_preserves_activation_and_source(tmp_path):
    source = tmp_path / "control.sqlite3"
    _current_store(source, control=True, delivery=True)
    control = RcaControlStore(source)
    safe_off = control.create_activation_epoch(
        epoch_id="migration-existing-v9",
        preauthorization_fingerprint="1" * 64,
        preauthorization_gate_receipt_sha256="2" * 64,
        preauthorization_capsule_sha256="3" * 64,
        config_sha256="b" * 64,
        db_logical_identity={"database": "migration-test"},
        partition_start_fence={"feishu-project-workflow-event": {"0": 10}},
        operator="migration-test",
        reason="prove_existing_v9_activation_inheritance",
        now=NOW,
    )
    control.preauthorize_activation_epoch(
        epoch_id="migration-existing-v9",
        preproduction_fingerprint="a" * 64,
        preproduction_gate_receipt_sha256="4" * 64,
        preproduction_capsule_sha256="5" * 64,
        expected_preauthorization_fingerprint=safe_off[
            "preauthorization_fingerprint"
        ],
        expected_preauthorization_gate_receipt_sha256=safe_off[
            "preauthorization_gate_receipt_sha256"
        ],
        expected_preauthorization_capsule_sha256=safe_off[
            "preauthorization_capsule_sha256"
        ],
        expected_config_sha256=safe_off["config_sha256"],
        expected_db_logical_identity_sha256=safe_off[
            "db_logical_identity_sha256"
        ],
        expected_partition_start_fence_sha256=safe_off[
            "partition_start_fence_sha256"
        ],
        operator="migration-test",
        reason="preauthorize existing v9 migration fixture",
        now=NOW,
    )
    drill_module._checkpoint_restore(source)
    with sqlite3.connect(source) as connection:
        connection.execute("CREATE TABLE migration_sentinel(value TEXT)")
        connection.execute("INSERT INTO migration_sentinel VALUES('preserved')")
    connection.close()
    before = observe_regular_file(source)

    receipt = _run(tmp_path, control=source, delivery=source)

    assert observe_regular_file(source) == before
    assert receipt["schema_version"] == drill_module.STORE_MIGRATION_RECEIPT_SCHEMA_VERSION
    assert receipt["mode"] == "existing"
    assert receipt["migration_state"] == "already_current"
    assert receipt["rollback_ready"] is False
    assert receipt["blockers"] == ["migration_source_already_current"]
    assert receipt["configured_databases"]["same_database"] is True
    assert len(receipt["database_drills"]) == 1
    [database] = receipt["database_drills"]
    assert database["drill_id"] == "shared"
    assert database["roles"] == ["control", "delivery"]
    assert receipt["schema_transitions"] == _expected_schema_transitions()
    assert database["schema_transitions"] == _expected_schema_transitions()
    assert database["source"]["identity"] == before
    assert database["backup"]["validation"]["schemas"] == {
        "control": CONTROL_STORE_SCHEMA_VERSION,
        "delivery": DELIVERY_STORE_SCHEMA_VERSION,
    }
    assert database["restore"]["validation"]["schemas"] == {
        "control": CONTROL_STORE_SCHEMA_VERSION,
        "delivery": DELIVERY_STORE_SCHEMA_VERSION,
    }
    assert database["restore"]["validation"]["structure"][
        "control_v9_activation"
    ] == drill_module._expected_control_activation_structure(present=True)
    assert database["restore"]["data_inheritance"]["row_count"] >= 1
    with sqlite3.connect(
        database["restore"]["artifact"]["path"]
    ) as restored_connection:
        assert restored_connection.execute(
            "SELECT value FROM migration_sentinel"
        ).fetchone() == ("preserved",)
        assert restored_connection.execute(
            "SELECT state FROM rca_activation_epochs WHERE epoch_id=?",
            ("migration-existing-v9",),
        ).fetchone() == ("preauthorized",)
    assert database["migration_state"] == "already_current"
    assert database["rollback_ready"] is False
    assert database["rollback"]["read_only_restore_proven"] is False
    assert database["rollback"]["compatibility_probe"] is None
    assert database["rollback"]["predecessor_validator_execution"] is None
    assert database["rollback"]["validation"]["write_probe"] == "blocked_readonly"
    assert database["rollback"]["validation"] == database["backup"]["validation"]
    assert receipt["writer_stop_evidence"]["process_probe"] == (
        "launchctl_job_absence_psutil_process_absence_v2"
    )
    assert receipt["writer_stop_evidence"]["services"]["ai.hermes.gateway"] == {
        "observed_at": NOW.isoformat(),
        "pid_state": "pid_absent",
        "health_state": "stopped",
        "process_probe": "launchctl_job_absence_psutil_process_absence_v2",
        "launchd_job_state": "absent",
        "matching_pids": [],
    }


def test_split_databases_have_independent_backups_and_restores(tmp_path):
    control = tmp_path / "control.sqlite3"
    delivery = tmp_path / "delivery.sqlite3"
    _current_store(control, control=True, delivery=False)
    _current_store(delivery, control=False, delivery=True)
    control_before = observe_regular_file(control)
    delivery_before = observe_regular_file(delivery)

    receipt = _run(tmp_path, control=control, delivery=delivery)

    assert receipt["configured_databases"]["same_database"] is False
    assert len(receipt["database_drills"]) == 2
    assert {tuple(item["roles"]) for item in receipt["database_drills"]} == {
        ("control",),
        ("delivery",),
    }
    assert receipt["schema_transitions"] == _expected_schema_transitions()
    for database in receipt["database_drills"]:
        assert database["migration_state"] == "already_current"
        assert database["rollback_ready"] is False
        assert database["schema_transitions"] == {
            role: _expected_schema_transitions()[role]
            for role in database["roles"]
        }
        assert database["rollback"]["validation"]["write_probe"] == (
            "blocked_readonly"
        )
        assert database["rollback"]["validation"] == (
            database["backup"]["validation"]
        )
    assert observe_regular_file(control) == control_before
    assert observe_regular_file(delivery) == delivery_before
    artifact_paths = {
        section["artifact"]["path"]
        for database in receipt["database_drills"]
        for section in (
            database["backup"],
            database["restore"],
            database["rollback"],
        )
    }
    assert len(artifact_paths) == 6


def test_missing_same_database_runs_real_v8_v5_predecessor_drill(tmp_path):
    source = tmp_path / "not-created.sqlite3"

    receipt = _run(tmp_path, control=source, delivery=source)

    assert source.exists() is False
    assert receipt["mode"] == "fresh_create"
    assert receipt["migration_state"] == "fresh_install"
    assert receipt["rollback_strategy"] == (
        "disable_writers_preserve_current_store_v1"
    )
    assert receipt["materialization_required"] is True
    assert receipt["rollback_ready"] is False
    assert receipt["blockers"] == ["fresh_install_materialization_required"]
    [database] = receipt["database_drills"]
    assert database["source"]["exists"] is False
    assert database["source"]["identity"] is None
    assert receipt["schema_transitions"] == _expected_schema_transitions()
    assert database["schema_transitions"] == _expected_schema_transitions()
    assert database["predecessor_fixture"]["schema_transitions"] == (
        _expected_schema_transitions()
    )
    assert database["source"]["pre_validation"]["schemas"] == {
        "control": CONTROL_PREDECESSOR_SCHEMA_VERSION,
        "delivery": DELIVERY_PREDECESSOR_SCHEMA_VERSION,
    }
    assert database["source"]["pre_validation"]["structure"] == {
        "control_v8_indexes": [
            "idx_business_triggers_issue_scope",
            "idx_rca_manual_operator_rate",
        ],
        "control_v9_activation": (
            drill_module._expected_control_activation_structure(present=False)
        ),
        "delivery_task_id_notnull": 0,
        "host_runtime_transitions": {
            "present": True,
            "columns": sorted(drill_module.HOST_RUNTIME_TRANSITION_COLUMNS),
            "submission_index": ["submission_key", "transition_id"],
        },
    }
    predecessor_path = Path(database["predecessor_fixture"]["artifact"]["path"])
    with sqlite3.connect(predecessor_path) as predecessor:
        tables = {
            str(row[0])
            for row in predecessor.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        indexes = {
            str(row[0])
            for row in predecessor.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            ).fetchall()
        }
        assert tables.isdisjoint(drill_module.CONTROL_V9_ACTIVATION_TABLES)
        assert indexes.isdisjoint(drill_module.CONTROL_V9_ACTIVATION_INDEXES)
        assert "rca_host_runtime_transitions" in tables
        for table, columns in drill_module.CONTROL_V9_ACTIVATION_COLUMNS.items():
            observed = {
                str(row[1])
                for row in predecessor.execute(f"PRAGMA table_info({table})")
            }
            assert observed.isdisjoint(columns)
    assert database["restore"]["validation"]["schemas"] == {
        "control": CONTROL_STORE_SCHEMA_VERSION,
        "delivery": DELIVERY_STORE_SCHEMA_VERSION,
    }
    assert database["restore"]["validation"]["structure"] == {
        "control_v8_indexes": [
            "idx_business_triggers_issue_scope",
            "idx_rca_manual_operator_rate",
        ],
        "control_v9_activation": (
            drill_module._expected_control_activation_structure(present=True)
        ),
        "delivery_task_id_notnull": 0,
        "host_runtime_transitions": {
            "present": True,
            "columns": sorted(drill_module.HOST_RUNTIME_TRANSITION_COLUMNS),
            "submission_index": ["submission_key", "transition_id"],
        },
    }
    assert database["restore"]["migration_observations"]["control"]["mode"] == (
        "migration"
    )
    rollback = database["rollback"]["validation"]
    assert rollback["schemas"] == {
        "control": CONTROL_STORE_SCHEMA_VERSION,
        "delivery": DELIVERY_STORE_SCHEMA_VERSION,
    }
    assert rollback["structure"]["control_v9_activation"] == (
        drill_module._expected_control_activation_structure(present=True)
    )
    assert rollback["structure"]["host_runtime_transitions"]["present"] is True
    assert rollback["write_probe"] == "blocked_readonly"
    assert database["rollback"]["strategy"] == (
        "disable_writers_preserve_current_store_v1"
    )
    assert database["rollback"]["preserve_store_proven"] is False
    assert database["installation_seed"]["validation"] == (
        database["restore"]["validation"]
    )


def test_fresh_install_materializer_plan_apply_and_idempotent_receipt(tmp_path):
    source = tmp_path / "fresh.sqlite3"
    migration = _run(tmp_path, control=source, delivery=source)
    [database] = migration["database_drills"]
    seed = Path(database["installation_seed"]["artifact"]["path"])
    files_before_plan = {
        path.relative_to(tmp_path): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }

    plan = drill_module.materialize_fresh_install(
        migration_receipt_path=(
            tmp_path / "evidence" / "store_migration_receipt.json"
        ),
        control_db_path=source,
        delivery_db_path=source,
        config_sha256="c" * 64,
        evidence_dir=tmp_path / "evidence",
        writer_stop_evidence=_writer_stop(),
        writer_process_probe=_writer_probe,
        now=NOW,
    )
    assert plan["applied"] is False
    assert source.exists() is False
    assert not (tmp_path / "evidence" / "fresh_install_genesis_intent.json").exists()
    assert not (tmp_path / ".pnc-rca-fresh-install.lock").exists()
    assert {
        path.relative_to(tmp_path): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    } == files_before_plan

    receipt = drill_module.materialize_fresh_install(
        migration_receipt_path=(
            tmp_path / "evidence" / "store_migration_receipt.json"
        ),
        control_db_path=source,
        delivery_db_path=source,
        config_sha256="c" * 64,
        evidence_dir=tmp_path / "evidence",
        writer_stop_evidence=_writer_stop(),
        writer_process_probe=_writer_probe,
        apply=True,
        now=NOW,
        **MATERIALIZATION_AUDIT,
    )

    assert receipt["strategy"] == "fresh_install_preserve"
    assert receipt["applied"] is True
    assert receipt["rollback_contract"] == {
        "action": "disable_writers_and_preserve_store",
        "destructive_cleanup_allowed": False,
        "quarantine_before_replacement_required": True,
    }
    destination = receipt["destination"]
    assert destination["artifact"] == observe_regular_file(source)
    assert destination["seed_sha256"] == observe_regular_file(seed)["sha256"]
    assert all(
        destination["state"][name]["present"] is False
        for name in ("wal", "shm", "journal", "tombstone")
    )
    genesis = drill_module._read_genesis_meta(source)
    assert genesis["control.fresh_install_db_instance_id"] == receipt[
        "db_instance_id"
    ]
    assert genesis["delivery.fresh_install_genesis_intent_sha256"] == receipt[
        "genesis_intent_sha256"
    ]
    assert (
        tmp_path / "evidence" / "fresh_install_materialization_receipt.json"
    ).is_file()
    assert receipt["audit"] == {
        f"{name}_sha256": hashlib.sha256(value.encode("utf-8")).hexdigest()
        for name, value in MATERIALIZATION_AUDIT.items()
        if name != "bootstrap_epoch_id"
    }
    receipt_text = (
        tmp_path / "evidence" / "fresh_install_materialization_receipt.json"
    ).read_text(encoding="utf-8")
    assert all(
        value not in receipt_text
        for name, value in MATERIALIZATION_AUDIT.items()
        if name not in {"release_id", "bootstrap_epoch_id"}
    )
    assert receipt["capacity_identity"] == {
        "release_id": MATERIALIZATION_AUDIT["release_id"],
        "bootstrap_epoch_id": MATERIALIZATION_AUDIT["bootstrap_epoch_id"],
    }
    for identity in receipt["runtime_identities"].values():
        assert set(identity) == {
            "schema_version",
            "uid",
            "gid",
            "pid",
            "process_create_time",
            "executable_path",
            "executable_sha256",
            "argv_sha256",
        }
        assert identity["uid"] == os.getuid()
        assert identity["gid"] == os.getgid()
        assert identity["pid"] == os.getpid()
        assert identity["process_create_time"] > 0
        assert len(identity["executable_sha256"]) == 64
        assert len(identity["argv_sha256"]) == 64
    assert not Path(f"{source}.pnc-rca-maintenance").exists()
    journal_id = receipt["materialization_journal"]["journal_id"]
    assert (
        tmp_path
        / "evidence"
        / f"fresh_install_materialization_journal.{journal_id}.receipted.json"
    ).is_file()

    idempotent = drill_module.materialize_fresh_install(
        migration_receipt_path=(
            tmp_path / "evidence" / "store_migration_receipt.json"
        ),
        control_db_path=source,
        delivery_db_path=source,
        config_sha256="c" * 64,
        evidence_dir=tmp_path / "evidence",
        writer_stop_evidence=_writer_stop(),
        writer_process_probe=_writer_probe,
        apply=True,
        now=NOW,
        **MATERIALIZATION_AUDIT,
    )
    assert idempotent["idempotent"] is True
    assert idempotent["applied"] is False


def test_fresh_install_apply_requires_complete_audit_before_writing(tmp_path):
    source = tmp_path / "fresh.sqlite3"
    _run(tmp_path, control=source, delivery=source)

    with pytest.raises(MigrationDrillError) as error:
        drill_module.materialize_fresh_install(
            migration_receipt_path=(
                tmp_path / "evidence" / "store_migration_receipt.json"
            ),
            control_db_path=source,
            delivery_db_path=source,
            config_sha256="c" * 64,
            evidence_dir=tmp_path / "evidence",
            writer_stop_evidence=_writer_stop(),
            writer_process_probe=_writer_probe,
            apply=True,
            now=NOW,
        )

    assert error.value.code == "fresh_install_release_id_invalid"
    assert source.exists() is False
    assert not Path(f"{source}.pnc-rca-maintenance").exists()
    assert not (tmp_path / ".pnc-rca-fresh-install.lock").exists()


def test_fresh_install_apply_requires_bootstrap_epoch_before_writing(tmp_path):
    source = tmp_path / "fresh.sqlite3"
    _run(tmp_path, control=source, delivery=source)

    with pytest.raises(MigrationDrillError) as error:
        drill_module.materialize_fresh_install(
            migration_receipt_path=(
                tmp_path / "evidence" / "store_migration_receipt.json"
            ),
            control_db_path=source,
            delivery_db_path=source,
            config_sha256="c" * 64,
            evidence_dir=tmp_path / "evidence",
            writer_stop_evidence=_writer_stop(),
            writer_process_probe=_writer_probe,
            apply=True,
            now=NOW,
            release_id=MATERIALIZATION_AUDIT["release_id"],
            operator=MATERIALIZATION_AUDIT["operator"],
            reason=MATERIALIZATION_AUDIT["reason"],
        )

    assert error.value.code == "capacity_transition_bootstrap_epoch_id_invalid"
    assert source.exists() is False
    assert not Path(f"{source}.pnc-rca-maintenance").exists()
    assert not (tmp_path / ".pnc-rca-fresh-install.lock").exists()


def test_existing_predecessor_apply_migrates_both_stores_and_is_origin_idempotent(
    tmp_path,
):
    source = (tmp_path / "existing.sqlite3").resolve()
    drill_module._create_predecessor_fixture(source, ["control", "delivery"])
    migration = _run(tmp_path, control=source, delivery=source)
    assert migration["migration_state"] == "migration_required"
    receipt_path = tmp_path / "evidence" / "store_migration_receipt.json"

    initialized = drill_module.initialize_existing_capacity_transition(
        migration_receipt_path=receipt_path,
        control_db_path=source,
        delivery_db_path=source,
        evidence_dir=tmp_path / "evidence",
        writer_stop_evidence=_writer_stop(),
        release_id="ratchet-origin-release-20260713",
        bootstrap_epoch_id="ratchet-origin-epoch-20260713",
        apply=True,
        now=NOW,
        writer_process_probe=_writer_probe,
        operator="migration-test",
        reason="migrate existing predecessor and initialize capacity",
    )

    assert initialized["operation"] == "existing_migration"
    assert initialized["capacity_transition"]["state"] == "BOOTSTRAP_PRODUCTION"
    validation = drill_module.inspect_sqlite_read_only(
        source, ("control", "delivery")
    )
    assert validation["schemas"] == {
        "control": CONTROL_STORE_SCHEMA_VERSION,
        "delivery": DELIVERY_STORE_SCHEMA_VERSION,
    }
    assert RcaControlStore(source, require_current=True).capacity_transition_state()[
        "release_id"
    ] == "ratchet-origin-release-20260713"

    repeated = drill_module.initialize_existing_capacity_transition(
        migration_receipt_path=receipt_path,
        control_db_path=source,
        delivery_db_path=source,
        evidence_dir=tmp_path / "evidence",
        writer_stop_evidence=_writer_stop(),
        release_id="later-software-release-20260714",
        bootstrap_epoch_id="later-software-epoch-20260714",
        apply=True,
        now=NOW,
        writer_process_probe=_writer_probe,
        operator="migration-test",
        reason="verify existing ratchet origin without resetting it",
    )
    assert repeated["idempotent"] is True
    assert repeated["capacity_identity"] == {
        "release_id": "ratchet-origin-release-20260713",
        "bootstrap_epoch_id": "ratchet-origin-epoch-20260713",
    }
    assert repeated["current_release_identity"] == {
        "release_id": "later-software-release-20260714",
        "bootstrap_epoch_id": "later-software-epoch-20260714",
    }


def test_existing_steady_without_original_initialization_receipt_fails_closed(
    tmp_path,
):
    source = (tmp_path / "steady.sqlite3").resolve()
    _current_store(source, control=True, delivery=True)
    _run(tmp_path, control=source, delivery=source)
    store = RcaControlStore(source, require_current=True)
    store.initialize_capacity_transition(
        release_id="steady-origin-release-20260713",
        bootstrap_epoch_id="steady-origin-epoch-20260713",
        now=NOW - timedelta(minutes=5),
    )
    store.compare_and_set_capacity_steady(
        expected_generation=1,
        release_id="steady-origin-release-20260713",
        bootstrap_epoch_id="steady-origin-epoch-20260713",
        final_ledger_sha256="1" * 64,
        transition_authorization_sha256="2" * 64,
        transition_authorization_fingerprint="3" * 64,
        transition_receipt_sha256="4" * 64,
        transition_receipt_fingerprint="5" * 64,
        commit_marker_sha256="6" * 64,
        commit_marker_fingerprint="7" * 64,
        evidence_bundle_sha256="8" * 64,
        evidence_bundle_fingerprint="9" * 64,
        authorization_issued_at=(NOW - timedelta(minutes=4)).isoformat(),
        authorization_expires_at=(NOW + timedelta(minutes=5)).isoformat(),
        receipt_created_at=(NOW - timedelta(minutes=3)).isoformat(),
        marker_committed_at=(NOW - timedelta(minutes=2)).isoformat(),
        now=NOW,
    )
    drill_module._checkpoint_restore(source)

    with pytest.raises(MigrationDrillError) as error:
        drill_module.initialize_existing_capacity_transition(
            migration_receipt_path=(
                tmp_path / "evidence" / "store_migration_receipt.json"
            ),
            control_db_path=source,
            delivery_db_path=source,
            evidence_dir=tmp_path / "evidence",
            writer_stop_evidence=_writer_stop(),
            release_id="later-release-20260714",
            bootstrap_epoch_id="later-epoch-20260714",
            apply=True,
            now=NOW,
            writer_process_probe=_writer_probe,
            operator="migration-test",
            reason="must not reconstruct missing steady origin receipt",
        )

    assert error.value.code == "existing_capacity_original_receipt_missing"
    assert store.capacity_transition_state()["state"] == "STEADY_ACTIVE"
    assert not Path(f"{source}.pnc-rca-maintenance").exists()


def test_existing_steady_receipt_rejects_rewritten_genesis(tmp_path):
    source = (tmp_path / "rewritten-genesis.sqlite3").resolve()
    drill_module._create_predecessor_fixture(source, ["control", "delivery"])
    _run(tmp_path, control=source, delivery=source)
    receipt_path = tmp_path / "evidence" / "store_migration_receipt.json"
    release_id = "steady-origin-release-20260713"
    epoch_id = "steady-origin-epoch-20260713"
    drill_module.initialize_existing_capacity_transition(
        migration_receipt_path=receipt_path,
        control_db_path=source,
        delivery_db_path=source,
        evidence_dir=tmp_path / "evidence",
        writer_stop_evidence=_writer_stop(),
        release_id=release_id,
        bootstrap_epoch_id=epoch_id,
        apply=True,
        now=NOW,
        writer_process_probe=_writer_probe,
        operator="migration-test",
        reason="initialize the immutable capacity origin",
    )
    RcaControlStore(source, require_current=True).compare_and_set_capacity_steady(
        expected_generation=1,
        release_id=release_id,
        bootstrap_epoch_id=epoch_id,
        final_ledger_sha256="1" * 64,
        transition_authorization_sha256="2" * 64,
        transition_authorization_fingerprint="3" * 64,
        transition_receipt_sha256="4" * 64,
        transition_receipt_fingerprint="5" * 64,
        commit_marker_sha256="6" * 64,
        commit_marker_fingerprint="7" * 64,
        evidence_bundle_sha256="8" * 64,
        evidence_bundle_fingerprint="9" * 64,
        authorization_issued_at=(NOW + timedelta(minutes=1)).isoformat(),
        authorization_expires_at=(NOW + timedelta(minutes=10)).isoformat(),
        receipt_created_at=(NOW + timedelta(minutes=2)).isoformat(),
        marker_committed_at=(NOW + timedelta(minutes=3)).isoformat(),
        now=NOW + timedelta(minutes=4),
    )

    rewritten = (NOW - timedelta(minutes=1)).isoformat()
    with sqlite3.connect(source) as connection:
        connection.execute("DROP TRIGGER trg_rca_capacity_state_steady_immutable")
        connection.execute("DROP TRIGGER trg_rca_capacity_audit_no_update")
        connection.execute(
            "UPDATE rca_capacity_transition_state "
            "SET bootstrap_initialized_at = ? WHERE singleton_id = 1",
            (rewritten,),
        )
        connection.execute(
            "UPDATE rca_capacity_transition_audit "
            "SET transitioned_at = ? WHERE to_generation = 1",
            (rewritten,),
        )

    with pytest.raises(MigrationDrillError) as error:
        drill_module.initialize_existing_capacity_transition(
            migration_receipt_path=receipt_path,
            control_db_path=source,
            delivery_db_path=source,
            evidence_dir=tmp_path / "evidence",
            writer_stop_evidence=_writer_stop(),
            release_id="later-release-20260714",
            bootstrap_epoch_id="later-epoch-20260714",
            apply=True,
            now=NOW + timedelta(minutes=5),
            writer_process_probe=_writer_probe,
            operator="migration-test",
            reason="prove the original capacity genesis is immutable",
        )
    assert error.value.code == "capacity_initialization_latch_drift"


def test_capacity_snapshot_rejects_steady_timestamp_chain_tamper(tmp_path):
    source = (tmp_path / "steady-tampered.sqlite3").resolve()
    _current_store(source, control=True, delivery=True)
    store = RcaControlStore(source, require_current=True)
    release_id = "steady-origin-release-20260713"
    epoch_id = "steady-origin-epoch-20260713"
    store.initialize_capacity_transition(
        release_id=release_id,
        bootstrap_epoch_id=epoch_id,
        now=NOW - timedelta(minutes=5),
    )
    steady_kwargs = {
        "expected_generation": 1,
        "release_id": release_id,
        "bootstrap_epoch_id": epoch_id,
        "final_ledger_sha256": "1" * 64,
        "transition_authorization_sha256": "2" * 64,
        "transition_authorization_fingerprint": "3" * 64,
        "transition_receipt_sha256": "4" * 64,
        "transition_receipt_fingerprint": "5" * 64,
        "commit_marker_sha256": "6" * 64,
        "commit_marker_fingerprint": "7" * 64,
        "evidence_bundle_sha256": "8" * 64,
        "evidence_bundle_fingerprint": "9" * 64,
        "authorization_issued_at": (NOW - timedelta(minutes=4)).isoformat(),
        "authorization_expires_at": (NOW + timedelta(minutes=5)).isoformat(),
        "receipt_created_at": (NOW - timedelta(minutes=3)).isoformat(),
        "marker_committed_at": (NOW - timedelta(minutes=2)).isoformat(),
        "now": NOW,
    }
    store.compare_and_set_capacity_steady(**steady_kwargs)
    assert drill_module._capacity_transition_snapshot(
        source,
        expected_release_id=release_id,
        expected_bootstrap_epoch_id=epoch_id,
    )["state"] == "STEADY_ACTIVE"

    invalid_expiry = (NOW - timedelta(minutes=3)).isoformat()
    with sqlite3.connect(source) as connection:
        connection.execute("DROP TRIGGER trg_rca_capacity_state_steady_immutable")
        connection.execute("DROP TRIGGER trg_rca_capacity_audit_no_update")
        connection.execute(
            "UPDATE rca_capacity_transition_state "
            "SET authorization_expires_at = ? WHERE singleton_id = 1",
            (invalid_expiry,),
        )
        connection.execute(
            "UPDATE rca_capacity_transition_audit "
            "SET authorization_expires_at = ? WHERE to_generation = 2",
            (invalid_expiry,),
        )

    with pytest.raises(MigrationDrillError) as error:
        drill_module._capacity_transition_snapshot(
            source,
            expected_release_id=release_id,
            expected_bootstrap_epoch_id=epoch_id,
        )
    assert error.value.code == "capacity_transition_timestamp_invalid"


def test_fresh_install_recovers_after_database_link_before_receipt(
    tmp_path, monkeypatch
):
    source = tmp_path / "fresh.sqlite3"
    _run(tmp_path, control=source, delivery=source)
    receipt_path = tmp_path / "evidence" / "fresh_install_materialization_receipt.json"
    original_write = drill_module._write_json_no_clobber
    failed = False

    def fail_materialization_receipt_once(path, value):
        nonlocal failed
        if Path(path) == receipt_path and not failed:
            failed = True
            raise MigrationDrillError("synthetic_receipt_crash")
        return original_write(path, value)

    monkeypatch.setattr(
        drill_module,
        "_write_json_no_clobber",
        fail_materialization_receipt_once,
    )
    with pytest.raises(MigrationDrillError) as error:
        drill_module.materialize_fresh_install(
            migration_receipt_path=(
                tmp_path / "evidence" / "store_migration_receipt.json"
            ),
            control_db_path=source,
            delivery_db_path=source,
            config_sha256="c" * 64,
            evidence_dir=tmp_path / "evidence",
            writer_stop_evidence=_writer_stop(),
            writer_process_probe=_writer_probe,
            apply=True,
            now=NOW,
            **MATERIALIZATION_AUDIT,
        )

    assert error.value.code == "synthetic_receipt_crash"
    marker_path = Path(f"{source}.pnc-rca-maintenance")
    assert source.is_file()
    assert marker_path.is_file()
    assert marker_path.stat().st_mode & 0o777 == 0o600
    marker = json.loads(marker_path.read_text(encoding="utf-8"))
    journal_id = marker["journal_id"]
    assert not receipt_path.exists()
    assert (
        tmp_path
        / "evidence"
        / f"fresh_install_materialization_journal.{journal_id}.installed.json"
    ).is_file()

    monkeypatch.setattr(drill_module, "_write_json_no_clobber", original_write)
    recovered = drill_module.materialize_fresh_install(
        migration_receipt_path=(tmp_path / "evidence" / "store_migration_receipt.json"),
        control_db_path=source,
        delivery_db_path=source,
        config_sha256="c" * 64,
        evidence_dir=tmp_path / "evidence",
        writer_stop_evidence=_writer_stop(),
        writer_process_probe=_writer_probe,
        apply=True,
        now=NOW,
        **MATERIALIZATION_AUDIT,
    )

    assert recovered["applied"] is False
    assert recovered["idempotent"] is False
    assert recovered["recovered"] is True
    assert receipt_path.is_file()
    assert not marker_path.exists()
    assert (
        tmp_path
        / "evidence"
        / f"fresh_install_materialization_journal.{journal_id}.receipted.json"
    ).is_file()


def test_materialization_recovery_validates_full_receipt_before_marker_release(
    tmp_path, monkeypatch
):
    source = tmp_path / "fresh.sqlite3"
    _run(tmp_path, control=source, delivery=source)
    original_write = drill_module._write_json_no_clobber
    failed = False

    def fail_receipted_once(path, value):
        nonlocal failed
        if (
            Path(path).name.endswith(".receipted.json")
            and value.get("phase") == "receipted"
            and not failed
        ):
            failed = True
            raise MigrationDrillError("synthetic_before_receipted")
        return original_write(path, value)

    monkeypatch.setattr(
        drill_module,
        "_write_json_no_clobber",
        fail_receipted_once,
    )
    with pytest.raises(MigrationDrillError, match="synthetic_before_receipted"):
        _apply_fresh_materialization(tmp_path, source)
    monkeypatch.setattr(drill_module, "_write_json_no_clobber", original_write)

    marker_path = Path(f"{source}.pnc-rca-maintenance")
    receipt_path = tmp_path / "evidence" / "fresh_install_materialization_receipt.json"
    assert marker_path.is_file()
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["rollback_contract"]["action"] = "tampered-release-marker"
    receipt_path.write_text(
        json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    receipt_path.chmod(0o600)

    with pytest.raises(MigrationDrillError) as error:
        _apply_fresh_materialization(tmp_path, source)

    assert error.value.code == "fresh_install_materialization_receipt_invalid"
    assert marker_path.is_file()


def test_fresh_install_recovers_database_link_before_stage_unlink(
    tmp_path, monkeypatch
):
    source = (tmp_path / "fresh.sqlite3").resolve()
    _run(tmp_path, control=source, delivery=source)
    stage_path = _leave_materialization_stage_linked(
        tmp_path,
        source,
        monkeypatch,
    )

    recovered = _apply_fresh_materialization(tmp_path, source)

    assert recovered["recovered"] is True
    assert recovered["applied"] is False
    assert source.stat().st_nlink == 1
    assert not stage_path.exists()
    assert (
        tmp_path / "evidence" / "fresh_install_materialization_receipt.json"
    ).is_file()


@pytest.mark.parametrize(
    "mutation",
    ["extra_hardlink", "stage_content_drift", "foreign_stage_candidate"],
)
def test_fresh_install_stage_link_recovery_rejects_ambiguity_and_drift(
    tmp_path,
    monkeypatch,
    mutation,
):
    source = (tmp_path / "fresh.sqlite3").resolve()
    _run(tmp_path, control=source, delivery=source)
    stage_path = _leave_materialization_stage_linked(
        tmp_path,
        source,
        monkeypatch,
    )
    if mutation == "extra_hardlink":
        os.link(source, tmp_path / "foreign-live-hardlink.sqlite3")
    elif mutation == "stage_content_drift":
        stage_path.unlink()
        stage_path.write_bytes(b"foreign-stage-content")
        stage_path.chmod(0o600)
    else:
        foreign = source.parent / (
            f".{source.name}.genesis-{'f' * 64}.sqlite3"
        )
        foreign.write_bytes(b"foreign-stage-candidate")
        foreign.chmod(0o600)

    with pytest.raises(MigrationDrillError) as error:
        _apply_fresh_materialization(tmp_path, source)

    assert error.value.code == "fresh_install_stage_recovery_conflict"
    assert source.exists()
    assert stage_path.exists()
    assert not (
        tmp_path / "evidence" / "fresh_install_materialization_receipt.json"
    ).exists()


def test_fresh_install_rejects_non_private_migration_evidence(tmp_path):
    source = tmp_path / "fresh.sqlite3"
    _run(tmp_path, control=source, delivery=source)
    migration_receipt = tmp_path / "evidence" / "store_migration_receipt.json"
    migration_receipt.chmod(0o644)

    with pytest.raises(MigrationDrillError) as error:
        drill_module.materialize_fresh_install(
            migration_receipt_path=migration_receipt,
            control_db_path=source,
            delivery_db_path=source,
            config_sha256="c" * 64,
            evidence_dir=tmp_path / "evidence",
            writer_stop_evidence=_writer_stop(),
            writer_process_probe=_writer_probe,
            now=NOW,
        )

    assert error.value.code == "migration_evidence_file_invalid"


def test_fresh_install_materializer_never_overwrites_destination(tmp_path):
    source = tmp_path / "fresh.sqlite3"
    _run(tmp_path, control=source, delivery=source)
    source.write_bytes(b"existing-owner-data")
    before = source.read_bytes()

    with pytest.raises(MigrationDrillError) as error:
        drill_module.materialize_fresh_install(
            migration_receipt_path=(
                tmp_path / "evidence" / "store_migration_receipt.json"
            ),
            control_db_path=source,
            delivery_db_path=source,
            config_sha256="c" * 64,
            evidence_dir=tmp_path / "evidence",
            writer_stop_evidence=_writer_stop(),
            writer_process_probe=_writer_probe,
            apply=True,
            now=NOW,
            **MATERIALIZATION_AUDIT,
        )

    assert error.value.code == "fresh_install_destination_not_absent"
    assert source.read_bytes() == before


def test_fresh_install_materializer_rechecks_processes_under_lock(tmp_path):
    source = tmp_path / "fresh.sqlite3"
    _run(tmp_path, control=source, delivery=source)
    calls = 0

    def changing_probe():
        nonlocal calls
        calls += 1
        return _writer_probe(
            live_label=("ai.hermes.gateway" if calls >= 2 else "")
        )

    with pytest.raises(MigrationDrillError) as error:
        drill_module.materialize_fresh_install(
            migration_receipt_path=(
                tmp_path / "evidence" / "store_migration_receipt.json"
            ),
            control_db_path=source,
            delivery_db_path=source,
            config_sha256="c" * 64,
            evidence_dir=tmp_path / "evidence",
            writer_stop_evidence=_writer_stop(),
            writer_process_probe=changing_probe,
            apply=True,
            now=NOW,
            **MATERIALIZATION_AUDIT,
        )

    assert error.value.code == "writer_stop_process_still_running"
    assert source.exists() is False
    assert list(
        (tmp_path / "evidence").glob(
            "fresh_install_materialization_failure.*.json"
        )
    )


def test_materialization_cli_defaults_to_read_only_plan(tmp_path, monkeypatch, capsys):
    source = tmp_path / "fresh.sqlite3"
    _run(tmp_path, control=source, delivery=source)
    writer_stop = tmp_path / "writer-stop.json"
    writer_stop.write_text(
        json.dumps(_writer_stop(datetime.now(timezone.utc))), encoding="utf-8"
    )
    writer_stop.chmod(0o600)
    monkeypatch.setattr(
        drill_module,
        "_default_writer_process_probe",
        _writer_probe,
    )

    assert main([
        "--control-db",
        str(source),
        "--delivery-db",
        str(source),
        "--work-dir",
        str(tmp_path / "unused-work"),
        "--evidence-dir",
        str(tmp_path / "evidence"),
        "--writer-stop-evidence",
        str(writer_stop),
        "--materialize-fresh-from-receipt",
        str(tmp_path / "evidence" / "store_migration_receipt.json"),
        "--materialization-config-sha256",
        "c" * 64,
        "--max-writer-stop-age-seconds",
        "200000",
    ]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["applied"] is False
    assert source.exists() is False
    assert not (tmp_path / "evidence" / "fresh_install_genesis_intent.json").exists()


def test_fresh_install_quarantine_and_restore_happy_path_is_idempotent(tmp_path):
    source = (tmp_path / "fresh.sqlite3").resolve()
    materialization = _materialize_fresh(tmp_path, source)
    original = observe_regular_file(source)

    quarantine_plan_files = {
        path.relative_to(tmp_path): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }
    quarantine_plan = _quarantine_fresh(tmp_path, source, apply=False)
    assert quarantine_plan["applied"] is False
    assert source.is_file()
    assert {
        path.relative_to(tmp_path): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    } == quarantine_plan_files

    quarantined = _quarantine_fresh(tmp_path, source)
    quarantine_artifact = Path(quarantined["quarantine_artifact"]["path"])
    assert quarantined["applied"] is True
    assert not source.exists()
    assert quarantine_artifact.is_file()
    assert quarantined["quarantine_artifact"]["sha256"] == original["sha256"]
    assert Path(f"{source}.pnc-rca-tombstone").is_file()
    assert not Path(f"{source}.pnc-rca-maintenance").exists()
    assert quarantine_artifact.stat().st_nlink == 1
    assert quarantine_artifact.stat().st_mode & 0o777 == 0o600
    assert Path(
        tmp_path / "evidence" / "fresh_install_quarantine_receipt.json"
    ).stat().st_mode & 0o777 == 0o600

    repeated_quarantine = _quarantine_fresh(tmp_path, source)
    assert repeated_quarantine["idempotent"] is True
    assert repeated_quarantine["applied"] is False

    restore_plan_files = {
        path.relative_to(tmp_path): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }
    restore_plan = _restore_fresh(tmp_path, source, apply=False)
    assert restore_plan["applied"] is False
    assert not source.exists()
    assert {
        path.relative_to(tmp_path): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    } == restore_plan_files

    restored = _restore_fresh(tmp_path, source)
    assert restored["applied"] is True
    assert source.is_file()
    assert source.stat().st_nlink == 1
    assert quarantine_artifact.is_file()
    assert quarantine_artifact.stat().st_nlink == 1
    assert restored["restored_live_artifact"]["sha256"] == original["sha256"]
    assert restored["restored_live_artifact"]["inode"] != original["inode"]
    assert not Path(f"{source}.pnc-rca-tombstone").exists()
    assert not Path(f"{source}.pnc-rca-maintenance").exists()
    assert (
        tmp_path / "evidence" / "fresh_install_restore_receipt.json"
    ).stat().st_mode & 0o777 == 0o600
    assert drill_module._read_genesis_meta(source) == materialization[
        "destination"
    ]["genesis_meta"]
    RcaControlStore(source, require_current=True)
    RcaDeliveryStore(source, require_current=True)

    repeated_restore = _restore_fresh(tmp_path, source)
    assert repeated_restore["idempotent"] is True
    assert repeated_restore["applied"] is False


def test_materialization_idempotent_fast_path_rejects_replaced_live_inode(tmp_path):
    source = (tmp_path / "fresh.sqlite3").resolve()
    _materialize_fresh(tmp_path, source)
    original = source.read_bytes()
    source.unlink()
    source.write_bytes(original)
    source.chmod(0o600)

    with pytest.raises(MigrationDrillError) as error:
        drill_module.materialize_fresh_install(
            migration_receipt_path=(
                tmp_path / "evidence" / "store_migration_receipt.json"
            ),
            control_db_path=source,
            delivery_db_path=source,
            config_sha256="c" * 64,
            evidence_dir=tmp_path / "evidence",
            writer_stop_evidence=_writer_stop(),
            writer_process_probe=_writer_probe,
            apply=True,
            now=NOW,
            **MATERIALIZATION_AUDIT,
        )

    assert error.value.code in {
        "fresh_install_materialized_database_changed",
        "fresh_install_materialization_receipt_invalid",
    }
    assert source.read_bytes() == original


def test_materialization_idempotent_fast_path_rejects_tampered_receipt(tmp_path):
    source = (tmp_path / "fresh.sqlite3").resolve()
    _materialize_fresh(tmp_path, source)
    receipt_path = tmp_path / "evidence" / "fresh_install_materialization_receipt.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["destination"]["artifact"]["sha256"] = "0" * 64
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    receipt_path.chmod(0o600)

    with pytest.raises(MigrationDrillError) as error:
        drill_module.materialize_fresh_install(
            migration_receipt_path=(
                tmp_path / "evidence" / "store_migration_receipt.json"
            ),
            control_db_path=source,
            delivery_db_path=source,
            config_sha256="c" * 64,
            evidence_dir=tmp_path / "evidence",
            writer_stop_evidence=_writer_stop(),
            writer_process_probe=_writer_probe,
            apply=True,
            now=NOW,
            **MATERIALIZATION_AUDIT,
        )

    assert error.value.code == "fresh_install_materialization_receipt_invalid"


def test_quarantine_maintenance_and_tombstone_fence_both_current_stores(
    tmp_path, monkeypatch
):
    source = (tmp_path / "fresh.sqlite3").resolve()
    _materialize_fresh(tmp_path, source)
    original_write = drill_module._write_json_no_clobber
    failed = False

    def fail_prepared_once(path, value):
        nonlocal failed
        if (
            str(path).endswith(".prepared.json")
            and value.get("operation") == "quarantine"
            and not failed
        ):
            failed = True
            raise MigrationDrillError("synthetic_quarantine_crash")
        return original_write(path, value)

    monkeypatch.setattr(drill_module, "_write_json_no_clobber", fail_prepared_once)
    with pytest.raises(MigrationDrillError, match="synthetic_quarantine_crash"):
        _quarantine_fresh(tmp_path, source)
    marker = Path(f"{source}.pnc-rca-maintenance")
    assert marker.is_file()
    with pytest.raises(RuntimeError):
        RcaControlStore(source, require_current=True)
    with pytest.raises(RuntimeError):
        RcaDeliveryStore(source, require_current=True)

    monkeypatch.setattr(drill_module, "_write_json_no_clobber", original_write)
    recovered = _quarantine_fresh(tmp_path, source)
    assert recovered["recovered"] is True
    assert not marker.exists()
    assert Path(f"{source}.pnc-rca-tombstone").is_file()
    with pytest.raises(RuntimeError):
        RcaControlStore(source, require_current=True)
    with pytest.raises(RuntimeError):
        RcaDeliveryStore(source, require_current=True)


@pytest.mark.parametrize(
    "phase",
    [
        "prepared",
        "linked",
        "unlinked",
        "tombstone",
        "tombstoned",
        "receipt",
        "receipted",
    ],
)
def test_quarantine_recovers_every_durable_crash_window(
    tmp_path, monkeypatch, phase
):
    source = (tmp_path / "fresh.sqlite3").resolve()
    _materialize_fresh(tmp_path, source)
    original_write = drill_module._write_json_no_clobber
    failed = False

    def fail_phase_once(path, value):
        nonlocal failed
        candidate = Path(path)
        matches = (
            phase == "tombstone"
            and candidate == Path(f"{source}.pnc-rca-tombstone")
        ) or (
            phase == "receipt"
            and candidate.name == "fresh_install_quarantine_receipt.json"
        ) or (
            phase not in {"tombstone", "receipt"}
            and candidate.name.endswith(f".{phase}.json")
            and value.get("operation") == "quarantine"
        )
        if matches and not failed:
            failed = True
            raise MigrationDrillError(f"synthetic_quarantine_{phase}_crash")
        return original_write(path, value)

    monkeypatch.setattr(drill_module, "_write_json_no_clobber", fail_phase_once)
    with pytest.raises(MigrationDrillError) as error:
        _quarantine_fresh(tmp_path, source)
    assert error.value.code == f"synthetic_quarantine_{phase}_crash"
    assert Path(f"{source}.pnc-rca-maintenance").is_file()

    monkeypatch.setattr(drill_module, "_write_json_no_clobber", original_write)
    recovered = _quarantine_fresh(tmp_path, source)
    assert recovered["recovered"] is True
    assert not source.exists()
    assert Path(f"{source}.pnc-rca-tombstone").is_file()
    assert Path(recovered["quarantine_artifact"]["path"]).is_file()


def test_quarantine_rejects_sidecar_process_race_unknown_artifact_and_permissions(
    tmp_path
):
    sidecar_root = tmp_path / "sidecar"
    sidecar_root.mkdir()
    sidecar_source = (sidecar_root / "fresh.sqlite3").resolve()
    _materialize_fresh(sidecar_root, sidecar_source)
    Path(f"{sidecar_source}-wal").write_bytes(b"unknown-wal")
    with pytest.raises(MigrationDrillError):
        _quarantine_fresh(sidecar_root, sidecar_source)
    assert sidecar_source.is_file()
    assert Path(f"{sidecar_source}-wal").read_bytes() == b"unknown-wal"

    race_root = tmp_path / "race"
    race_root.mkdir()
    race_source = (race_root / "fresh.sqlite3").resolve()
    _materialize_fresh(race_root, race_source)
    calls = 0

    def racing_probe():
        nonlocal calls
        calls += 1
        return _writer_probe(live_label="ai.hermes.gateway" if calls >= 3 else "")

    with pytest.raises(MigrationDrillError) as error:
        _quarantine_fresh(
            race_root,
            race_source,
            writer_process_probe=racing_probe,
        )
    assert error.value.code == "writer_stop_process_still_running"
    assert race_source.is_file()
    assert Path(f"{race_source}.pnc-rca-maintenance").is_file()

    conflict_root = tmp_path / "conflict"
    conflict_root.mkdir()
    conflict_source = (conflict_root / "fresh.sqlite3").resolve()
    _materialize_fresh(conflict_root, conflict_source)
    materialization_path = (
        conflict_root
        / "evidence"
        / "fresh_install_materialization_receipt.json"
    )
    _body, raw, _observation = drill_module._read_json_regular_file(
        materialization_path
    )
    audit = drill_module._fresh_install_audit_hashes(
        **QUARANTINE_AUDIT,
        required=True,
    )
    transaction_id = drill_module._rollback_transaction_id(
        operation="quarantine",
        source_receipt_sha256=hashlib.sha256(raw).hexdigest(),
        configured_database=conflict_source,
        config_sha256="c" * 64,
        audit=audit,
    )
    quarantine_dir = conflict_root / "quarantine"
    quarantine_dir.mkdir(mode=0o700)
    conflict = quarantine_dir / (
        f"{conflict_source.name}.{transaction_id}.quarantined.sqlite3"
    )
    conflict.write_bytes(b"unknown-owner-data")
    conflict.chmod(0o600)
    with pytest.raises(MigrationDrillError):
        _quarantine_fresh(conflict_root, conflict_source)
    assert conflict.read_bytes() == b"unknown-owner-data"
    assert conflict_source.is_file()

    permissions_root = tmp_path / "permissions"
    permissions_root.mkdir()
    permissions_source = (permissions_root / "fresh.sqlite3").resolve()
    _materialize_fresh(permissions_root, permissions_source)
    insecure = permissions_root / "quarantine"
    insecure.mkdir(mode=0o755)
    with pytest.raises(MigrationDrillError) as error:
        drill_module.quarantine_fresh_install(
            materialization_receipt_path=(
                permissions_root
                / "evidence"
                / "fresh_install_materialization_receipt.json"
            ),
            control_db_path=permissions_source,
            delivery_db_path=permissions_source,
            config_sha256="c" * 64,
            evidence_dir=permissions_root / "evidence",
            quarantine_dir=insecure,
            writer_stop_evidence=_writer_stop(),
            writer_process_probe=_writer_probe,
        )
    assert error.value.code == "fresh_install_quarantine_directory_invalid"


@pytest.mark.parametrize(
    "phase",
    [
        "prepared",
        "staged",
        "copied",
        "installed",
        "verified",
        "unfenced",
        "receipt",
        "receipted",
    ],
)
def test_restore_recovers_every_durable_crash_window(tmp_path, monkeypatch, phase):
    source = (tmp_path / "fresh.sqlite3").resolve()
    _materialize_fresh(tmp_path, source)
    quarantined = _quarantine_fresh(tmp_path, source)
    quarantine_path = Path(quarantined["quarantine_artifact"]["path"])
    original_write = drill_module._write_json_no_clobber
    failed = False

    def fail_phase_once(path, value):
        nonlocal failed
        candidate = Path(path)
        matches = (
            phase == "receipt"
            and candidate.name == "fresh_install_restore_receipt.json"
        ) or (
            phase != "receipt"
            and candidate.name.endswith(f".{phase}.json")
            and value.get("operation") == "restore"
        )
        if matches and not failed:
            failed = True
            raise MigrationDrillError(f"synthetic_restore_{phase}_crash")
        return original_write(path, value)

    monkeypatch.setattr(drill_module, "_write_json_no_clobber", fail_phase_once)
    with pytest.raises(MigrationDrillError) as error:
        _restore_fresh(tmp_path, source)
    assert error.value.code == f"synthetic_restore_{phase}_crash"
    assert Path(f"{source}.pnc-rca-maintenance").is_file()

    monkeypatch.setattr(drill_module, "_write_json_no_clobber", original_write)
    recovered = _restore_fresh(tmp_path, source)
    assert recovered["recovered"] is True
    assert source.is_file()
    assert source.stat().st_nlink == 1
    assert quarantine_path.is_file()
    assert quarantine_path.stat().st_nlink == 1
    assert not Path(f"{source}.pnc-rca-tombstone").exists()


def test_restore_recovers_mid_copy_process_crash(tmp_path, monkeypatch):
    source = (tmp_path / "fresh.sqlite3").resolve()
    _materialize_fresh(tmp_path, source)
    _quarantine_fresh(tmp_path, source)
    temporary = _leave_partial_restore_copy(tmp_path, source, monkeypatch)

    recovered = _restore_fresh(tmp_path, source)

    assert recovered["recovered"] is True
    assert source.is_file()
    assert source.stat().st_nlink == 1
    assert not temporary.exists()
    assert not list(source.parent.glob(f".{source.name}.*.restore-copy.sqlite3"))
    assert not Path(f"{source}.pnc-rca-maintenance").exists()


def test_restore_recovers_copy_publish_before_temporary_unlink(
    tmp_path, monkeypatch
):
    source = (tmp_path / "fresh.sqlite3").resolve()
    _materialize_fresh(tmp_path, source)
    _quarantine_fresh(tmp_path, source)
    original_unlink = Path.unlink
    crashed = False

    def crash_after_publish(path, *args, **kwargs):
        nonlocal crashed
        if path.name.endswith(".restore-copy.partial.sqlite3") and not crashed:
            published = Path(
                str(path).replace(
                    ".restore-copy.partial.sqlite3",
                    ".restore-copy.sqlite3",
                )
            )
            if published.exists():
                crashed = True
                raise _SyntheticProcessCrash
        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", crash_after_publish)
    with pytest.raises(_SyntheticProcessCrash):
        _restore_fresh(tmp_path, source)
    monkeypatch.setattr(Path, "unlink", original_unlink)
    [temporary] = list(
        source.parent.glob(f".{source.name}.*.restore-copy.partial.sqlite3")
    )
    [published] = list(
        source.parent.glob(f".{source.name}.*.restore-copy.sqlite3")
    )
    assert (temporary.stat().st_dev, temporary.stat().st_ino) == (
        published.stat().st_dev,
        published.stat().st_ino,
    )
    assert temporary.stat().st_nlink == 2

    recovered = _restore_fresh(tmp_path, source)

    assert recovered["recovered"] is True
    assert source.is_file()
    assert source.stat().st_nlink == 1
    assert not temporary.exists()
    assert not published.exists()


@pytest.mark.parametrize("mutation", ["content_drift", "extra_link"])
def test_restore_partial_copy_recovery_fails_closed_on_drift_or_extra_link(
    tmp_path, monkeypatch, mutation
):
    source = (tmp_path / "fresh.sqlite3").resolve()
    _materialize_fresh(tmp_path, source)
    _quarantine_fresh(tmp_path, source)
    temporary = _leave_partial_restore_copy(tmp_path, source, monkeypatch)
    if mutation == "content_drift":
        value = temporary.read_bytes()
        temporary.write_bytes(bytes([value[0] ^ 0xFF]) + value[1:])
        temporary.chmod(0o600)
    else:
        os.link(temporary, tmp_path / "unexpected-partial-copy-link")

    with pytest.raises(MigrationDrillError) as error:
        _restore_fresh(tmp_path, source)

    assert error.value.code == "fresh_install_restore_copy_conflict"
    assert not source.exists()
    assert temporary.exists()
    assert Path(f"{source}.pnc-rca-maintenance").is_file()


def test_restore_rejects_foreign_transaction_copy_before_prepared(tmp_path):
    source = (tmp_path / "fresh.sqlite3").resolve()
    _materialize_fresh(tmp_path, source)
    _quarantine_fresh(tmp_path, source)
    quarantine_receipt = (
        tmp_path / "evidence" / "fresh_install_quarantine_receipt.json"
    )
    _body, raw, _observation = drill_module._read_json_regular_file(
        quarantine_receipt
    )
    audit = drill_module._fresh_install_audit_hashes(
        **RESTORE_AUDIT,
        required=True,
    )
    transaction_id = drill_module._rollback_transaction_id(
        operation="restore",
        source_receipt_sha256=hashlib.sha256(raw).hexdigest(),
        configured_database=source,
        config_sha256="c" * 64,
        audit=audit,
    )
    foreign = source.parent / (
        f".{source.name}.{transaction_id}.restore-copy.partial.sqlite3"
    )
    foreign.write_bytes(b"foreign-copy-owner")
    foreign.chmod(0o600)

    with pytest.raises(MigrationDrillError) as error:
        _restore_fresh(tmp_path, source)

    assert error.value.code == "fresh_install_restore_transient_conflict"
    assert foreign.read_bytes() == b"foreign-copy-owner"
    assert not source.exists()


def test_restore_rejects_sidecar_process_race_tamper_and_no_clobber(tmp_path):
    sidecar_root = tmp_path / "sidecar"
    sidecar_root.mkdir()
    sidecar_source = (sidecar_root / "fresh.sqlite3").resolve()
    _materialize_fresh(sidecar_root, sidecar_source)
    _quarantine_fresh(sidecar_root, sidecar_source)
    Path(f"{sidecar_source}-journal").write_bytes(b"unknown-journal")
    with pytest.raises(MigrationDrillError):
        _restore_fresh(sidecar_root, sidecar_source)
    assert not sidecar_source.exists()
    assert Path(f"{sidecar_source}-journal").read_bytes() == b"unknown-journal"

    race_root = tmp_path / "race"
    race_root.mkdir()
    race_source = (race_root / "fresh.sqlite3").resolve()
    _materialize_fresh(race_root, race_source)
    _quarantine_fresh(race_root, race_source)
    calls = 0

    def racing_probe():
        nonlocal calls
        calls += 1
        return _writer_probe(live_label="ai.hermes.gateway" if calls >= 3 else "")

    with pytest.raises(MigrationDrillError) as error:
        _restore_fresh(race_root, race_source, writer_process_probe=racing_probe)
    assert error.value.code == "writer_stop_process_still_running"
    assert not race_source.exists()
    assert Path(f"{race_source}.pnc-rca-tombstone").is_file()

    tamper_root = tmp_path / "tamper"
    tamper_root.mkdir()
    tamper_source = (tamper_root / "fresh.sqlite3").resolve()
    _materialize_fresh(tamper_root, tamper_source)
    quarantined = _quarantine_fresh(tamper_root, tamper_source)
    artifact = Path(quarantined["quarantine_artifact"]["path"])
    artifact.write_bytes(b"tampered-quarantine")
    with pytest.raises(MigrationDrillError):
        _restore_fresh(tamper_root, tamper_source)
    assert artifact.read_bytes() == b"tampered-quarantine"
    assert not tamper_source.exists()

    tombstone_root = tmp_path / "tombstone-tamper"
    tombstone_root.mkdir()
    tombstone_source = (tombstone_root / "fresh.sqlite3").resolve()
    _materialize_fresh(tombstone_root, tombstone_source)
    _quarantine_fresh(tombstone_root, tombstone_source)
    tombstone_path = Path(f"{tombstone_source}.pnc-rca-tombstone")
    tombstone = json.loads(tombstone_path.read_text(encoding="utf-8"))
    tombstone["state"] = "unknown"
    tombstone_path.write_text(json.dumps(tombstone), encoding="utf-8")
    tombstone_path.chmod(0o600)
    with pytest.raises(MigrationDrillError):
        _restore_fresh(tombstone_root, tombstone_source)
    assert json.loads(tombstone_path.read_text(encoding="utf-8"))["state"] == (
        "unknown"
    )
    assert not tombstone_source.exists()

    conflict_root = tmp_path / "no-clobber"
    conflict_root.mkdir()
    conflict_source = (conflict_root / "fresh.sqlite3").resolve()
    _materialize_fresh(conflict_root, conflict_source)
    _quarantine_fresh(conflict_root, conflict_source)
    receipt_path = conflict_root / "evidence" / "fresh_install_restore_receipt.json"
    receipt_path.write_text('{"unknown":true}\n', encoding="utf-8")
    receipt_path.chmod(0o600)
    with pytest.raises(MigrationDrillError):
        _restore_fresh(conflict_root, conflict_source)
    assert receipt_path.read_text(encoding="utf-8") == '{"unknown":true}\n'
    assert not conflict_source.exists()


def test_quarantine_and_restore_recover_maintenance_release_crash(
    tmp_path, monkeypatch
):
    source = (tmp_path / "fresh.sqlite3").resolve()
    _materialize_fresh(tmp_path, source)
    maintenance_path = Path(f"{source}.pnc-rca-maintenance")
    original_remove = drill_module._remove_exact_json_file
    failed = False

    def fail_maintenance_once(path, *, expected):
        nonlocal failed
        if Path(path) == maintenance_path and not failed:
            failed = True
            raise MigrationDrillError("synthetic_maintenance_release_crash")
        return original_remove(path, expected=expected)

    monkeypatch.setattr(
        drill_module,
        "_remove_exact_json_file",
        fail_maintenance_once,
    )
    with pytest.raises(MigrationDrillError, match="synthetic_maintenance_release_crash"):
        _quarantine_fresh(tmp_path, source)
    assert maintenance_path.is_file()
    assert (tmp_path / "evidence" / "fresh_install_quarantine_receipt.json").is_file()

    monkeypatch.setattr(drill_module, "_remove_exact_json_file", original_remove)
    recovered_quarantine = _quarantine_fresh(tmp_path, source)
    assert recovered_quarantine["recovered"] is True
    assert not maintenance_path.exists()

    failed = False
    monkeypatch.setattr(
        drill_module,
        "_remove_exact_json_file",
        fail_maintenance_once,
    )
    with pytest.raises(MigrationDrillError, match="synthetic_maintenance_release_crash"):
        _restore_fresh(tmp_path, source)
    assert maintenance_path.is_file()
    assert (tmp_path / "evidence" / "fresh_install_restore_receipt.json").is_file()

    monkeypatch.setattr(drill_module, "_remove_exact_json_file", original_remove)
    recovered_restore = _restore_fresh(tmp_path, source)
    assert recovered_restore["recovered"] is True
    assert source.is_file()
    assert source.stat().st_nlink == 1
    assert not maintenance_path.exists()


def test_completion_receipts_reuse_durable_runtime_identity_across_new_process(
    tmp_path, monkeypatch
):
    source = (tmp_path / "fresh.sqlite3").resolve()
    _materialize_fresh(tmp_path, source)
    original_runtime_identity = drill_module._fresh_install_runtime_identity
    base_identity = original_runtime_identity()
    runtime_calls = 0

    def changing_runtime_identity():
        nonlocal runtime_calls
        runtime_calls += 1
        return {
            **base_identity,
            "pid": 41000 + runtime_calls,
            "process_create_time": (
                float(base_identity["process_create_time"]) + runtime_calls
            ),
        }

    original_remove = drill_module._remove_exact_json_file
    maintenance_path = Path(f"{source}.pnc-rca-maintenance")
    fail_release = True

    def fail_maintenance_release(path, *, expected):
        nonlocal fail_release
        if Path(path) == maintenance_path and fail_release:
            fail_release = False
            raise MigrationDrillError("synthetic_new_process_retry")
        return original_remove(path, expected=expected)

    monkeypatch.setattr(
        drill_module,
        "_fresh_install_runtime_identity",
        changing_runtime_identity,
    )
    monkeypatch.setattr(
        drill_module,
        "_remove_exact_json_file",
        fail_maintenance_release,
    )
    with pytest.raises(MigrationDrillError, match="synthetic_new_process_retry"):
        _quarantine_fresh(tmp_path, source)
    quarantine_receipt_path = (
        tmp_path / "evidence" / "fresh_install_quarantine_receipt.json"
    )
    quarantine_before = quarantine_receipt_path.read_bytes()
    quarantine_receipt = json.loads(quarantine_before)
    assert quarantine_receipt["runtime_identities"] == {
        "started": {**base_identity, "pid": 41001, "process_create_time": float(base_identity["process_create_time"]) + 1},
        "completed": {**base_identity, "pid": 41002, "process_create_time": float(base_identity["process_create_time"]) + 2},
    }

    recovered_quarantine = _quarantine_fresh(tmp_path, source)
    assert recovered_quarantine["recovered"] is True
    assert quarantine_receipt_path.read_bytes() == quarantine_before
    assert runtime_calls == 3

    fail_release = True
    with pytest.raises(MigrationDrillError, match="synthetic_new_process_retry"):
        _restore_fresh(tmp_path, source)
    restore_receipt_path = tmp_path / "evidence" / "fresh_install_restore_receipt.json"
    restore_before = restore_receipt_path.read_bytes()
    restore_receipt = json.loads(restore_before)
    assert restore_receipt["runtime_identities"]["started"]["pid"] == 41004
    assert restore_receipt["runtime_identities"]["completed"]["pid"] == 41005

    recovered_restore = _restore_fresh(tmp_path, source)
    assert recovered_restore["recovered"] is True
    assert restore_receipt_path.read_bytes() == restore_before
    assert runtime_calls == 6


def test_recovery_binds_refreshed_writer_stop_initial_and_completion_proofs(
    tmp_path, monkeypatch
):
    source = (tmp_path / "fresh.sqlite3").resolve()
    _materialize_fresh(tmp_path, source)
    later = NOW + timedelta(seconds=60)
    latest = NOW + timedelta(seconds=120)
    original_write = drill_module._write_json_no_clobber
    failed = False

    def fail_quarantine_linked_once(path, value):
        nonlocal failed
        if (
            value.get("operation") == "quarantine"
            and value.get("phase") == "linked"
            and not failed
        ):
            failed = True
            raise MigrationDrillError("synthetic_refresh_quarantine")
        return original_write(path, value)

    monkeypatch.setattr(
        drill_module,
        "_write_json_no_clobber",
        fail_quarantine_linked_once,
    )
    with pytest.raises(MigrationDrillError, match="synthetic_refresh_quarantine"):
        _quarantine_fresh(
            tmp_path,
            source,
            now=NOW,
            writer_stop=_writer_stop(NOW),
            max_writer_stop_age_seconds=30,
        )
    monkeypatch.setattr(drill_module, "_write_json_no_clobber", original_write)
    quarantined = _quarantine_fresh(
        tmp_path,
        source,
        now=later,
        writer_stop=_writer_stop(later),
        max_writer_stop_age_seconds=120,
    )
    assert quarantined["writer_stop_evidence"]["initial"]["observed_at"] == (
        NOW.isoformat()
    )
    assert quarantined["writer_stop_evidence"]["completion"]["observed_at"] == (
        later.isoformat()
    )
    assert quarantined["writer_stop_evidence_max_age_seconds"] == {
        "initial": 30,
        "completion": 120,
    }
    assert _quarantine_fresh(
        tmp_path,
        source,
        now=later,
        writer_stop=_writer_stop(later),
        max_writer_stop_age_seconds=120,
    )["idempotent"] is True

    failed = False

    def fail_restore_staged_once(path, value):
        nonlocal failed
        if (
            value.get("operation") == "restore"
            and value.get("phase") == "staged"
            and not failed
        ):
            failed = True
            raise MigrationDrillError("synthetic_refresh_restore")
        return original_write(path, value)

    monkeypatch.setattr(
        drill_module,
        "_write_json_no_clobber",
        fail_restore_staged_once,
    )
    with pytest.raises(MigrationDrillError, match="synthetic_refresh_restore"):
        _restore_fresh(
            tmp_path,
            source,
            now=later,
            writer_stop=_writer_stop(later),
            max_writer_stop_age_seconds=45,
        )
    monkeypatch.setattr(drill_module, "_write_json_no_clobber", original_write)
    restored = _restore_fresh(
        tmp_path,
        source,
        now=latest,
        writer_stop=_writer_stop(latest),
        max_writer_stop_age_seconds=150,
    )
    assert restored["writer_stop_evidence"]["initial"]["observed_at"] == (
        later.isoformat()
    )
    assert restored["writer_stop_evidence"]["completion"]["observed_at"] == (
        latest.isoformat()
    )
    assert restored["writer_stop_evidence_max_age_seconds"] == {
        "initial": 45,
        "completion": 150,
    }
    assert _restore_fresh(
        tmp_path,
        source,
        now=latest,
        writer_stop=_writer_stop(latest),
        max_writer_stop_age_seconds=150,
    )["idempotent"] is True


def test_stable_database_states_reject_preexisting_extra_hardlinks(tmp_path):
    material_root = tmp_path / "materialized"
    material_root.mkdir()
    material_source = (material_root / "fresh.sqlite3").resolve()
    _materialize_fresh(material_root, material_source)
    material_extra = material_root / "extra-materialized.sqlite3"
    os.link(material_source, material_extra)
    with pytest.raises(MigrationDrillError):
        drill_module.materialize_fresh_install(
            migration_receipt_path=(
                material_root / "evidence" / "store_migration_receipt.json"
            ),
            control_db_path=material_source,
            delivery_db_path=material_source,
            config_sha256="c" * 64,
            evidence_dir=material_root / "evidence",
            writer_stop_evidence=_writer_stop(),
            writer_process_probe=_writer_probe,
            apply=True,
            now=NOW,
            **MATERIALIZATION_AUDIT,
        )
    assert material_source.stat().st_nlink == 2
    assert material_extra.stat().st_nlink == 2

    quarantine_root = tmp_path / "quarantined"
    quarantine_root.mkdir()
    quarantine_source = (quarantine_root / "fresh.sqlite3").resolve()
    _materialize_fresh(quarantine_root, quarantine_source)
    quarantined = _quarantine_fresh(quarantine_root, quarantine_source)
    quarantine_artifact = Path(quarantined["quarantine_artifact"]["path"])
    quarantine_extra = quarantine_root / "extra-quarantined.sqlite3"
    os.link(quarantine_artifact, quarantine_extra)
    with pytest.raises(MigrationDrillError):
        _quarantine_fresh(quarantine_root, quarantine_source)
    with pytest.raises(MigrationDrillError):
        _restore_fresh(quarantine_root, quarantine_source)
    assert quarantine_artifact.stat().st_nlink == 2

    restore_root = tmp_path / "restored"
    restore_root.mkdir()
    restore_source = (restore_root / "fresh.sqlite3").resolve()
    _materialize_fresh(restore_root, restore_source)
    restored_quarantine = _quarantine_fresh(restore_root, restore_source)
    restored_quarantine_artifact = Path(
        restored_quarantine["quarantine_artifact"]["path"]
    )
    _restore_fresh(restore_root, restore_source)
    live_extra = restore_root / "extra-live.sqlite3"
    os.link(restore_source, live_extra)
    with pytest.raises(MigrationDrillError):
        _restore_fresh(restore_root, restore_source)
    assert restore_source.stat().st_nlink == 2
    live_extra.unlink()
    quarantine_extra_after_restore = restore_root / "extra-retained.sqlite3"
    os.link(restored_quarantine_artifact, quarantine_extra_after_restore)
    with pytest.raises(MigrationDrillError):
        _restore_fresh(restore_root, restore_source)
    assert restored_quarantine_artifact.stat().st_nlink == 2


def test_quarantine_and_restore_require_apply_audit_before_writing(tmp_path):
    source = (tmp_path / "fresh.sqlite3").resolve()
    _materialize_fresh(tmp_path, source)
    quarantine_dir = tmp_path / "quarantine"
    quarantine_dir.mkdir(mode=0o700)
    with pytest.raises(MigrationDrillError) as error:
        drill_module.quarantine_fresh_install(
            materialization_receipt_path=(
                tmp_path / "evidence" / "fresh_install_materialization_receipt.json"
            ),
            control_db_path=source,
            delivery_db_path=source,
            config_sha256="c" * 64,
            evidence_dir=tmp_path / "evidence",
            quarantine_dir=quarantine_dir,
            writer_stop_evidence=_writer_stop(),
            writer_process_probe=_writer_probe,
            apply=True,
            now=NOW,
        )
    assert error.value.code == "fresh_install_release_id_invalid"
    assert not Path(f"{source}.pnc-rca-maintenance").exists()

    _quarantine_fresh(tmp_path, source)
    with pytest.raises(MigrationDrillError) as error:
        drill_module.restore_fresh_install_from_quarantine(
            quarantine_receipt_path=(
                tmp_path / "evidence" / "fresh_install_quarantine_receipt.json"
            ),
            control_db_path=source,
            delivery_db_path=source,
            config_sha256="c" * 64,
            evidence_dir=tmp_path / "evidence",
            quarantine_dir=quarantine_dir,
            writer_stop_evidence=_writer_stop(),
            writer_process_probe=_writer_probe,
            apply=True,
            now=NOW,
        )
    assert error.value.code == "fresh_install_release_id_invalid"
    assert not Path(f"{source}.pnc-rca-maintenance").exists()


def test_quarantine_and_restore_never_clobber_unknown_receipt_or_stage(tmp_path):
    receipt_root = tmp_path / "receipt-conflict"
    receipt_root.mkdir()
    receipt_source = (receipt_root / "fresh.sqlite3").resolve()
    _materialize_fresh(receipt_root, receipt_source)
    conflict_receipt = (
        receipt_root / "evidence" / "fresh_install_quarantine_receipt.json"
    )
    conflict_receipt.write_text('{"unknown":true}\n', encoding="utf-8")
    conflict_receipt.chmod(0o600)
    with pytest.raises(MigrationDrillError):
        _quarantine_fresh(receipt_root, receipt_source)
    assert conflict_receipt.read_text(encoding="utf-8") == '{"unknown":true}\n'
    assert receipt_source.is_file()
    assert not Path(f"{receipt_source}.pnc-rca-maintenance").exists()

    stage_root = tmp_path / "stage-conflict"
    stage_root.mkdir()
    stage_source = (stage_root / "fresh.sqlite3").resolve()
    _materialize_fresh(stage_root, stage_source)
    _quarantine_fresh(stage_root, stage_source)
    quarantine_receipt_path = (
        stage_root / "evidence" / "fresh_install_quarantine_receipt.json"
    )
    _body, raw, _observation = drill_module._read_json_regular_file(
        quarantine_receipt_path
    )
    audit = drill_module._fresh_install_audit_hashes(
        **RESTORE_AUDIT,
        required=True,
    )
    transaction_id = drill_module._rollback_transaction_id(
        operation="restore",
        source_receipt_sha256=hashlib.sha256(raw).hexdigest(),
        configured_database=stage_source,
        config_sha256="c" * 64,
        audit=audit,
    )
    unknown_stage = stage_root / "quarantine" / (
        f".{stage_source.name}.{transaction_id}.restore-hardlink.sqlite3"
    )
    unknown_stage.write_bytes(b"unknown-stage-data")
    unknown_stage.chmod(0o600)
    with pytest.raises(MigrationDrillError):
        _restore_fresh(stage_root, stage_source)
    assert unknown_stage.read_bytes() == b"unknown-stage-data"
    assert not stage_source.exists()


def test_quarantine_and_restore_cli_default_to_zero_write_plan(
    tmp_path, monkeypatch, capsys
):
    source = (tmp_path / "fresh.sqlite3").resolve()
    _materialize_fresh(tmp_path, source)
    quarantine_dir = tmp_path / "quarantine"
    quarantine_dir.mkdir(mode=0o700)
    writer_stop = tmp_path / "writer-stop.json"
    writer_stop.write_text(
        json.dumps(_writer_stop(datetime.now(timezone.utc))), encoding="utf-8"
    )
    writer_stop.chmod(0o600)
    monkeypatch.setattr(drill_module, "_default_writer_process_probe", _writer_probe)
    before = {
        path.relative_to(tmp_path): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }

    assert main([
        "--control-db",
        str(source),
        "--delivery-db",
        str(source),
        "--work-dir",
        str(tmp_path / "unused-work"),
        "--evidence-dir",
        str(tmp_path / "evidence"),
        "--writer-stop-evidence",
        str(writer_stop),
        "--quarantine-fresh-from-materialization-receipt",
        str(tmp_path / "evidence" / "fresh_install_materialization_receipt.json"),
        "--fresh-rollback-quarantine-dir",
        str(quarantine_dir),
        "--fresh-rollback-config-sha256",
        "c" * 64,
        "--max-writer-stop-age-seconds",
        "200000",
    ]) == 0
    assert json.loads(capsys.readouterr().out)["applied"] is False
    assert {
        path.relative_to(tmp_path): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    } == before

    _quarantine_fresh(tmp_path, source)
    before_restore = {
        path.relative_to(tmp_path): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }
    assert main([
        "--control-db",
        str(source),
        "--delivery-db",
        str(source),
        "--work-dir",
        str(tmp_path / "unused-work"),
        "--evidence-dir",
        str(tmp_path / "evidence"),
        "--writer-stop-evidence",
        str(writer_stop),
        "--restore-fresh-from-quarantine-receipt",
        str(tmp_path / "evidence" / "fresh_install_quarantine_receipt.json"),
        "--fresh-rollback-quarantine-dir",
        str(quarantine_dir),
        "--fresh-rollback-config-sha256",
        "c" * 64,
        "--max-writer-stop-age-seconds",
        "200000",
    ]) == 0
    assert json.loads(capsys.readouterr().out)["applied"] is False
    assert {
        path.relative_to(tmp_path): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    } == before_restore


def test_existing_predecessor_and_bom_pinned_validator_prove_rollback_ready(
    tmp_path,
):
    source = tmp_path / "predecessor.sqlite3"
    drill_module._create_predecessor_fixture(source, ["control", "delivery"])
    repo, validator = _validator_repo(tmp_path)

    receipt = _run(
        tmp_path,
        control=source,
        delivery=source,
        predecessor_validator=validator,
        repo_root=repo,
    )

    assert receipt["migration_state"] == "migration_required"
    assert receipt["rollback_ready"] is True
    assert receipt["blockers"] == []
    assert receipt["predecessor_validator"]["status"] == "verified"
    assert receipt["predecessor_validator"]["bom_binding"]["commit"]
    [database] = receipt["database_drills"]
    assert database["source"]["mode"] == "existing"
    assert database["rollback_ready"] is True
    execution = database["rollback"]["predecessor_validator_execution"]
    assert execution["exit_code"] == 0
    assert execution["runtime"]["child_pid"] > 0
    assert execution["runtime"]["argv"][0] == str(validator.resolve())
    assert execution["read_only_result"]["side_effects"] == "none"
    assert execution["database_bundle_before"] == execution["database_bundle_after"]
    assert database["rollback"]["artifact"]["sha256"] == execution[
        "database_artifact_before"
    ]["sha256"]


def test_predecessor_validator_stderr_cannot_claim_rollback_ready(tmp_path):
    source = tmp_path / "predecessor.sqlite3"
    drill_module._create_predecessor_fixture(source, ["control", "delivery"])
    repo, validator = _validator_repo(tmp_path, writes_stderr=True)

    with pytest.raises(MigrationDrillError) as error:
        _run(
            tmp_path,
            control=source,
            delivery=source,
            predecessor_validator=validator,
            repo_root=repo,
        )

    assert error.value.code == "migration_predecessor_validator_stderr"


def test_writer_stop_evidence_rejects_one_live_service(tmp_path):
    stopped = _writer_stop()
    stopped["services"]["local.pnc.rca-kafka-consumer"]["pid_state"] = "pid_present"

    with pytest.raises(MigrationDrillError) as error:
        _run(
            tmp_path,
            control=tmp_path / "missing.sqlite3",
            delivery=tmp_path / "missing.sqlite3",
            writer_stop=stopped,
        )

    assert error.value.code == "writer_stop_evidence_service_not_stopped"


def test_machine_process_probe_rejects_running_gateway_writer(tmp_path):
    with pytest.raises(MigrationDrillError) as error:
        run_migration_drill(
            control_db_path=tmp_path / "missing.sqlite3",
            delivery_db_path=tmp_path / "missing.sqlite3",
            work_dir=tmp_path / "work",
            evidence_dir=tmp_path / "evidence",
            writer_stop_evidence=_writer_stop(),
            now=NOW,
            writer_process_probe=lambda: _writer_probe(live_label="ai.hermes.gateway"),
        )

    assert error.value.code == "writer_stop_process_still_running"


def test_machine_process_probe_rejects_writer_appearing_during_backup(tmp_path):
    calls = 0

    def probe():
        nonlocal calls
        calls += 1
        return _writer_probe(
            live_label="local.pnc.rca-kafka-consumer" if calls == 3 else ""
        )

    with pytest.raises(MigrationDrillError) as error:
        run_migration_drill(
            control_db_path=tmp_path / "missing.sqlite3",
            delivery_db_path=tmp_path / "missing.sqlite3",
            work_dir=tmp_path / "work",
            evidence_dir=tmp_path / "evidence",
            writer_stop_evidence=_writer_stop(),
            now=NOW,
            writer_process_probe=probe,
        )

    assert calls == 3
    assert error.value.code == "writer_stop_process_still_running"


def test_existing_source_with_wal_or_shm_sidecar_is_rejected(tmp_path):
    source = tmp_path / "control.sqlite3"
    _current_store(source, control=True, delivery=True)

    connection = sqlite3.connect(source)
    try:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("CREATE TABLE sidecar_sentinel(value TEXT)")
        connection.execute("INSERT INTO sidecar_sentinel VALUES('pending')")
        connection.commit()
        assert Path(f"{source}-wal").exists()

        with pytest.raises(MigrationDrillError) as error:
            _run(tmp_path, control=source, delivery=source)
    finally:
        connection.close()

    assert error.value.code == "migration_source_sidecar_present"


def test_hot_delete_journal_is_rejected_before_immutable_read_or_backup(tmp_path):
    source = tmp_path / "control.sqlite3"
    _current_store(source, control=True, delivery=True)
    with sqlite3.connect(source) as connection:
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("CREATE TABLE hot_journal_sentinel(value TEXT)")
        connection.execute("INSERT INTO hot_journal_sentinel VALUES('committed')")
    drill_module._checkpoint_restore(source)
    child = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import os, sqlite3, sys\n"
                "connection = sqlite3.connect(sys.argv[1])\n"
                "connection.execute('PRAGMA journal_mode=DELETE')\n"
                "connection.execute('BEGIN IMMEDIATE')\n"
                "connection.execute("
                "\"UPDATE hot_journal_sentinel SET value='uncommitted'\""
                ")\n"
                "os._exit(17)\n"
            ),
            str(source),
        ],
        check=False,
    )
    assert child.returncode == 17
    journal = Path(f"{source}-journal")
    assert journal.is_file()
    bundle = drill_module.observe_sqlite_bundle(source)
    assert bundle["journal"]["present"] is True

    with pytest.raises(MigrationDrillError) as error:
        drill_module.inspect_sqlite_read_only(source, ("control", "delivery"))
    assert error.value.code == "migration_source_sidecar_present"

    backup = tmp_path / "immutable-backup.sqlite3"
    with pytest.raises(MigrationDrillError) as error:
        drill_module._sqlite_backup(source, backup)
    assert error.value.code == "migration_source_sidecar_present"
    assert not backup.exists()

    snapshot = tmp_path / "online-snapshot.sqlite3"
    with pytest.raises(MigrationDrillError) as error:
        drill_module.sqlite_read_only_snapshot(source, snapshot)
    assert error.value.code == "migration_source_sidecar_present"
    assert not snapshot.exists()

    with pytest.raises(MigrationDrillError) as error:
        _run(tmp_path, control=source, delivery=source)
    assert error.value.code == "migration_source_sidecar_present"
    assert journal.is_file()


def _write_subset_fixture(path: Path, *, rows: list[tuple[int, str]]) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE business_rows(id INTEGER, payload TEXT)")
        connection.executemany("INSERT INTO business_rows VALUES(?, ?)", rows)


def test_content_subset_allows_new_current_rows(tmp_path):
    backup = tmp_path / "backup.sqlite3"
    current = tmp_path / "current.sqlite3"
    _write_subset_fixture(backup, rows=[(1, "kept")])
    _write_subset_fixture(current, rows=[(1, "kept"), (2, "new")])

    result = drill_module.compare_sqlite_common_content(backup, current)

    assert result["schema_version"] == "pnc_rca_store_data_inheritance_v2"
    assert result["row_count"] == 1


@pytest.mark.parametrize("current_rows", [[], [(1, "changed")]])
def test_content_subset_rejects_deleted_or_changed_old_rows(tmp_path, current_rows):
    backup = tmp_path / "backup.sqlite3"
    current = tmp_path / "current.sqlite3"
    _write_subset_fixture(backup, rows=[(1, "kept")])
    _write_subset_fixture(current, rows=current_rows)

    with pytest.raises(MigrationDrillError) as error:
        drill_module.compare_sqlite_common_content(backup, current)

    assert error.value.code == "migration_restore_data_mismatch"


def test_content_subset_rejects_dropped_backup_column(tmp_path):
    backup = tmp_path / "backup.sqlite3"
    current = tmp_path / "current.sqlite3"
    _write_subset_fixture(backup, rows=[(1, "kept")])
    with sqlite3.connect(current) as connection:
        connection.execute("CREATE TABLE business_rows(id INTEGER)")
        connection.execute("INSERT INTO business_rows VALUES(1)")

    with pytest.raises(MigrationDrillError) as error:
        drill_module.compare_sqlite_common_content(backup, current)

    assert error.value.code == "migration_restore_column_missing"


def test_content_subset_allows_schema_marker_change_but_rejects_meta_loss(tmp_path):
    backup = tmp_path / "backup.sqlite3"
    current = tmp_path / "current.sqlite3"
    for path, schema in ((backup, "v4"), (current, "v5")):
        with sqlite3.connect(path) as connection:
            connection.execute(
                "CREATE TABLE rca_delivery_meta(key TEXT PRIMARY KEY, value TEXT)"
            )
            connection.execute(
                "INSERT INTO rca_delivery_meta VALUES('schema_version', ?)",
                (schema,),
            )
    for path in (backup, current):
        with sqlite3.connect(path) as connection:
            connection.execute(
                "INSERT INTO rca_delivery_meta VALUES('permanent_failure_streak', '1')"
            )

    drill_module.compare_sqlite_common_content(backup, current)

    with sqlite3.connect(current) as connection:
        connection.execute(
            "DELETE FROM rca_delivery_meta WHERE key='permanent_failure_streak'"
        )

    with pytest.raises(MigrationDrillError) as error:
        drill_module.compare_sqlite_common_content(backup, current)

    assert error.value.code == "migration_restore_data_mismatch"


def test_source_symlink_is_rejected_before_backup(tmp_path):
    source = tmp_path / "real.sqlite3"
    _current_store(source, control=True, delivery=True)
    link = tmp_path / "linked.sqlite3"
    link.symlink_to(source)

    with pytest.raises(MigrationDrillError) as error:
        _run(tmp_path, control=link, delivery=link)

    assert error.value.code == "migration_source_symlink"


def test_atomic_receipt_is_exact_idempotent_and_never_clobbers(tmp_path):
    destination = tmp_path / "evidence" / "store_migration_receipt.json"
    destination.parent.mkdir()
    write_receipt_atomic(destination, {"old": True})
    old = destination.read_bytes()

    write_receipt_atomic(destination, {"old": True})
    assert destination.read_bytes() == old

    with pytest.raises(MigrationDrillError) as error:
        write_receipt_atomic(destination, {"new": True})

    assert error.value.code == "migration_receipt_conflict"
    assert destination.read_bytes() == old
    assert list(destination.parent.glob("*.no-clobber.tmp")) == []


def test_no_clobber_recovers_crash_before_link(tmp_path, monkeypatch):
    destination = tmp_path / "evidence" / "receipt.json"
    destination.parent.mkdir()
    value = {"state": "pre-link"}
    original_link = os.link
    original_unlink = Path.unlink

    def crash_before_link(source, target, *args, **kwargs):
        if Path(target) == destination:
            raise _SyntheticProcessCrash
        return original_link(source, target, *args, **kwargs)

    def preserve_temporary(path, *args, **kwargs):
        if path.name.startswith(f".{destination.name}.") and path.name.endswith(
            ".no-clobber.tmp"
        ):
            raise OSError("synthetic process death")
        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(os, "link", crash_before_link)
    monkeypatch.setattr(Path, "unlink", preserve_temporary)
    with pytest.raises(_SyntheticProcessCrash):
        drill_module._write_json_no_clobber(destination, value)
    monkeypatch.setattr(os, "link", original_link)
    monkeypatch.setattr(Path, "unlink", original_unlink)

    [temporary] = list(
        destination.parent.glob(f".{destination.name}.*.no-clobber.tmp")
    )
    assert not destination.exists()
    assert temporary.stat().st_nlink == 1

    drill_module._write_json_no_clobber(destination, value)

    assert json.loads(destination.read_text(encoding="utf-8")) == value
    assert destination.stat().st_nlink == 1
    assert not temporary.exists()


def test_no_clobber_recovers_crash_after_link(tmp_path, monkeypatch):
    destination = tmp_path / "evidence" / "receipt.json"
    destination.parent.mkdir()
    value = {"state": "post-link"}
    original_unlink = Path.unlink

    def preserve_temporary(path, *args, **kwargs):
        if path.name.startswith(f".{destination.name}.") and path.name.endswith(
            ".no-clobber.tmp"
        ):
            raise OSError("synthetic process death")
        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", preserve_temporary)
    with pytest.raises(MigrationDrillError) as error:
        drill_module._write_json_no_clobber(destination, value)
    assert error.value.code == "fresh_install_evidence_write_failed"
    monkeypatch.setattr(Path, "unlink", original_unlink)

    [temporary] = list(
        destination.parent.glob(f".{destination.name}.*.no-clobber.tmp")
    )
    assert destination.stat().st_nlink == 2
    assert temporary.stat().st_nlink == 2

    drill_module._write_json_no_clobber(destination, value)

    assert destination.stat().st_nlink == 1
    assert not temporary.exists()


def test_no_clobber_cleans_exact_pre_link_temp_after_destination_exists(tmp_path):
    destination = tmp_path / "evidence" / "receipt.json"
    destination.parent.mkdir()
    value = {"state": "published"}
    drill_module._write_json_no_clobber(destination, value)
    payload = drill_module._json_payload(value)
    temporary = destination.parent / (
        f".{destination.name}.pre-link.no-clobber.tmp"
    )
    temporary.write_bytes(payload)
    temporary.chmod(0o600)
    with temporary.open("rb") as handle:
        os.fsync(handle.fileno())
    assert temporary.stat().st_nlink == 1
    assert temporary.stat().st_ino != destination.stat().st_ino

    drill_module._write_json_no_clobber(destination, value)

    assert not temporary.exists()
    assert destination.stat().st_nlink == 1


@pytest.mark.parametrize("mutation", ["multiple", "mode", "content"])
def test_no_clobber_pre_link_recovery_rejects_foreign_temps(tmp_path, mutation):
    destination = tmp_path / "evidence" / "receipt.json"
    destination.parent.mkdir()
    value = {"state": "expected"}
    payload = drill_module._json_payload(value)
    temporary = destination.parent / (
        f".{destination.name}.candidate.no-clobber.tmp"
    )
    temporary.write_bytes(payload)
    temporary.chmod(0o600)
    if mutation == "multiple":
        second = destination.parent / (
            f".{destination.name}.second.no-clobber.tmp"
        )
        second.write_bytes(payload)
        second.chmod(0o600)
    elif mutation == "mode":
        temporary.chmod(0o640)
    else:
        temporary.write_bytes(drill_module._json_payload({"state": "drifted"}))

    with pytest.raises(MigrationDrillError) as error:
        drill_module._write_json_no_clobber(destination, value)

    assert error.value.code == "fresh_install_evidence_conflict"
    assert not destination.exists()
    assert temporary.exists()


def test_no_clobber_rejects_more_than_two_links(tmp_path):
    destination = tmp_path / "evidence" / "receipt.json"
    destination.parent.mkdir()
    value = {"state": "linked"}
    payload = drill_module._json_payload(value)
    temporary = destination.parent / (
        f".{destination.name}.linked.no-clobber.tmp"
    )
    temporary.write_bytes(payload)
    temporary.chmod(0o600)
    os.link(temporary, destination)
    extra = tmp_path / "extra-receipt-link.json"
    os.link(temporary, extra)

    with pytest.raises(MigrationDrillError) as error:
        drill_module._write_json_no_clobber(destination, value)

    assert error.value.code == "fresh_install_evidence_conflict"
    assert destination.stat().st_nlink == 3
    assert temporary.stat().st_nlink == 3
    assert extra.stat().st_nlink == 3


def test_cli_writes_private_receipt_without_creating_missing_source(
    tmp_path, capsys, monkeypatch
):
    source = tmp_path / "missing.sqlite3"
    writer_stop = tmp_path / "writer-stop.json"
    writer_stop.write_text(json.dumps(_writer_stop()), encoding="utf-8")
    writer_stop.chmod(0o600)
    monkeypatch.setattr(
        drill_module,
        "_default_writer_process_probe",
        _writer_probe,
    )

    assert (
        main([
            "--control-db",
            str(source),
            "--delivery-db",
            str(source),
            "--work-dir",
            str(tmp_path / "work"),
            "--evidence-dir",
            str(tmp_path / "evidence"),
            "--writer-stop-evidence",
            str(writer_stop),
        ])
        == 2
    )
    result = json.loads(capsys.readouterr().out)
    assert result == {"ok": False, "code": "writer_stop_evidence_stale"}

    current = datetime.now(timezone.utc)
    writer_stop.write_text(json.dumps(_writer_stop(current)), encoding="utf-8")
    writer_stop.chmod(0o600)
    assert (
        main([
            "--control-db",
            str(source),
            "--delivery-db",
            str(source),
            "--work-dir",
            str(tmp_path / "work"),
            "--evidence-dir",
            str(tmp_path / "evidence"),
            "--writer-stop-evidence",
            str(writer_stop),
        ])
        == 0
    )
    json.loads(capsys.readouterr().out)
    receipt = tmp_path / "evidence" / "store_migration_receipt.json"
    assert receipt.is_file()
    assert os.stat(receipt).st_mode & 0o777 == 0o600
    assert source.exists() is False
