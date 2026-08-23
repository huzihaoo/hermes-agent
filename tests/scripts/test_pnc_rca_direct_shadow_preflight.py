"""Offline contracts for the direct RCA shadow preflight."""

from __future__ import annotations

import json
from pathlib import Path
import sqlite3

import pytest

from gateway.pnc_rca_kafka_contract import WorkflowEventPolicy, WorkflowTransition
from gateway.pnc_rca_mini_store import MiniKafkaRecord, MiniStore
from scripts import pnc_rca_direct_shadow_preflight as preflight
from scripts import pnc_rca_kafka_direct_consumer as direct


TOPIC = "feishu-project-workflow-event"


def _policy() -> WorkflowEventPolicy:
    return WorkflowEventPolicy(
        topic=TOPIC,
        policy_version="issue-created-v1",
        project_keys=frozenset({"project-key"}),
        project_simple_names=frozenset({"g1q3"}),
        work_item_type_keys=frozenset({"problem-type"}),
        status_change_types=frozenset({"Reached"}),
        transitions=(WorkflowTransition("new-problem-state", 1, 2),),
    )


def _env(tmp_path: Path, **updates: str) -> dict[str, str]:
    prefix = direct.DIRECT_ENV_PREFIX
    values = {
        f"{prefix}BOOTSTRAP_SERVERS": "direct-a:9092,direct-b:9092",
        f"{prefix}TOPIC": TOPIC,
        f"{prefix}GROUP_ID": "rca-direct-shadow-20260823",
        f"{prefix}SASL_USERNAME": "direct-user",
        f"{prefix}SASL_PASSWORD": "direct-password",
        f"{prefix}SECURITY_PROTOCOL": "SASL_PLAINTEXT",
        f"{prefix}SASL_MECHANISM": "PLAIN",
        f"{prefix}AUTO_OFFSET_RESET": "none",
        f"{prefix}T0_OFFSETS_JSON": '{"0": 10}',
        f"{prefix}DB_PATH": str(tmp_path / "shadow" / "mini.sqlite3"),
        f"{prefix}HEALTH_PATH": str(tmp_path / "shadow" / "health.json"),
        f"{prefix}POLICY_JSON": json.dumps(_policy().to_dict()),
        f"{prefix}COMMIT_ENABLED": "false",
        f"{prefix}DISPATCHER_ENABLED": "false",
        f"{prefix}SUBMIT_ENABLED": "false",
    }
    values.update(updates)
    return values


def test_valid_absent_paths_emit_redacted_offline_plan(tmp_path: Path):
    env = _env(tmp_path)
    db_path = Path(env[f"{direct.DIRECT_ENV_PREFIX}DB_PATH"])
    health_path = Path(env[f"{direct.DIRECT_ENV_PREFIX}HEALTH_PATH"])

    plan = preflight.build_preflight_plan(env, hermes_home=tmp_path / "home")

    assert plan["ok"] is True
    assert plan["schema_version"] == preflight.PREFLIGHT_SCHEMA_VERSION
    assert plan["mode"] == preflight.MODE
    assert plan["offline_only"] is True
    assert plan["checks"]["shadow_group"]["independent"] is True
    assert plan["checks"]["commit_enabled"] == {"value": False, "ok": True}
    assert plan["checks"]["auto_offset_reset"]["value"] == "none"
    assert plan["checks"]["t0"]["offsets"] == {"0": 10}
    assert plan["checks"]["dispatcher"]["enabled"] is False
    assert plan["checks"]["submit"]["enabled"] is False
    assert plan["checks"]["mini_store"]["state"] == "absent"
    assert plan["plan"]["consumer"] == "direct_shadow_not_started"
    assert set(plan["side_effects"].values()) == {False}
    assert not db_path.exists()
    assert not health_path.exists()
    rendered = json.dumps(plan, sort_keys=True)
    assert "direct-password" not in rendered
    assert "direct-user" not in rendered


@pytest.mark.parametrize(
    "group_id",
    [preflight.OLD_PROD_GROUP_ID, preflight.STABLE_PROD_DIRECT_GROUP_ID],
)
def test_rejects_old_and_stable_production_groups(tmp_path: Path, group_id: str):
    plan = preflight.build_preflight_plan(
        _env(
            tmp_path,
            **{f"{direct.DIRECT_ENV_PREFIX}GROUP_ID": group_id},
        ),
        hermes_home=tmp_path / "home",
    )

    assert plan["ok"] is False
    assert "shadow_group_must_not_match_production_group" in plan["errors"] or any(
        "isolated group" in error for error in plan["errors"]
    )


def test_rejects_declared_production_group_even_when_name_is_custom(tmp_path: Path):
    env = _env(
        tmp_path,
        **{f"{direct.DIRECT_ENV_PREFIX}GROUP_ID": "owner-stable-group"},
        **{f"{direct.DIRECT_ENV_PREFIX}STABLE_PROD_GROUP_ID": ("owner-stable-group")},
    )

    plan = preflight.build_preflight_plan(env, hermes_home=tmp_path / "home")

    assert plan["ok"] is False
    assert "shadow_group_must_not_match_production_group" in plan["errors"]


def test_requires_commit_false_and_explicit_t0(tmp_path: Path):
    env = _env(
        tmp_path,
        **{f"{direct.DIRECT_ENV_PREFIX}COMMIT_ENABLED": "true"},
    )
    env.pop(f"{direct.DIRECT_ENV_PREFIX}T0_OFFSETS_JSON")

    plan = preflight.build_preflight_plan(env, hermes_home=tmp_path / "home")

    assert plan["ok"] is False
    assert "commit_enabled_must_be_false" in plan["errors"]
    assert "explicit_t0_required" in plan["errors"]


def test_rejects_offset_fallback_before_any_store_or_kafka_open(
    tmp_path: Path, monkeypatch
):
    monkeypatch.setattr(
        preflight.sqlite3,
        "connect",
        lambda *_args, **_kwargs: pytest.fail("opened SQLite"),
    )
    monkeypatch.setattr(
        direct,
        "create_kafka_consumer",
        lambda *_args, **_kwargs: pytest.fail("opened Kafka"),
    )

    plan = preflight.build_preflight_plan(
        _env(
            tmp_path,
            **{f"{direct.DIRECT_ENV_PREFIX}AUTO_OFFSET_RESET": "earliest"},
        ),
        hermes_home=tmp_path / "home",
    )

    assert plan["ok"] is False
    assert any("AUTO_OFFSET_RESET" in error for error in plan["errors"])


@pytest.mark.parametrize(
    ("missing_suffix", "expected"),
    [
        ("DISPATCHER_ENABLED", "dispatcher_disabled_declaration_missing"),
        ("SUBMIT_ENABLED", "submit_disabled_declaration_missing"),
    ],
)
def test_requires_explicit_disabled_declarations(
    tmp_path: Path, missing_suffix: str, expected: str
):
    env = _env(tmp_path)
    env.pop(f"{direct.DIRECT_ENV_PREFIX}{missing_suffix}")

    plan = preflight.build_preflight_plan(env, hermes_home=tmp_path / "home")

    assert plan["ok"] is False
    assert expected in plan["errors"]


@pytest.mark.parametrize("suffix", ["DISPATCHER_ENABLED", "SUBMIT_ENABLED"])
def test_rejects_enabled_dispatch_or_submit(tmp_path: Path, suffix: str):
    plan = preflight.build_preflight_plan(
        _env(
            tmp_path,
            **{f"{direct.DIRECT_ENV_PREFIX}{suffix}": "true"},
        ),
        hermes_home=tmp_path / "home",
    )

    assert plan["ok"] is False
    assert any(error.endswith("_must_be_disabled") for error in plan["errors"])


def test_accepts_zero_byte_candidate_without_modifying_it(tmp_path: Path):
    db_path = tmp_path / "shadow" / "mini.sqlite3"
    db_path.parent.mkdir()
    db_path.touch()
    before = db_path.stat()

    plan = preflight.build_preflight_plan(
        _env(
            tmp_path,
            **{f"{direct.DIRECT_ENV_PREFIX}DB_PATH": str(db_path)},
        ),
        hermes_home=tmp_path / "home",
    )

    after = db_path.stat()
    assert plan["ok"] is True
    assert plan["checks"]["mini_store"]["state"] == "empty_file"
    assert (after.st_ino, after.st_size, after.st_mtime_ns) == (
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    )


def test_accepts_exact_empty_v2_readonly_without_sidecars(tmp_path: Path):
    db_path = tmp_path / "shadow" / "mini.sqlite3"
    MiniStore(db_path)
    before = db_path.stat()
    sidecars = [Path(f"{db_path}{suffix}") for suffix in ("-wal", "-shm", "-journal")]
    assert not any(path.exists() for path in sidecars)

    plan = preflight.build_preflight_plan(
        _env(
            tmp_path,
            **{f"{direct.DIRECT_ENV_PREFIX}DB_PATH": str(db_path)},
        ),
        hermes_home=tmp_path / "home",
    )

    after = db_path.stat()
    assert plan["ok"] is True
    assert plan["checks"]["mini_store"] == {
        "path": str(db_path),
        "state": "new_v2",
        "schema_version": "pnc_rca_mini_store_v2",
        "row_counts": {
            "kafka_inbox": 0,
            "kafka_partition_progress": 0,
            "business_triggers": 0,
            "rca_outbox": 0,
        },
    }
    assert (after.st_ino, after.st_size, after.st_mtime_ns) == (
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    )
    assert not any(path.exists() for path in sidecars)


def test_rejects_nonempty_v2_store(tmp_path: Path):
    db_path = tmp_path / "shadow" / "mini.sqlite3"
    store = MiniStore(db_path)
    store.persist_raw(
        MiniKafkaRecord(topic=TOPIC, partition=0, offset=10, value=b"{}"),
        policy=_policy(),
    )

    plan = preflight.build_preflight_plan(
        _env(
            tmp_path,
            **{f"{direct.DIRECT_ENV_PREFIX}DB_PATH": str(db_path)},
        ),
        hermes_home=tmp_path / "home",
    )

    assert plan["ok"] is False
    assert "mini_store_not_fresh:kafka_inbox" in plan["errors"]


def test_rejects_wrong_sqlite_without_creating_sidecars(tmp_path: Path):
    db_path = tmp_path / "shadow" / "mini.sqlite3"
    db_path.parent.mkdir()
    connection = sqlite3.connect(db_path)
    connection.execute("CREATE TABLE unrelated(value TEXT)")
    connection.commit()
    connection.close()
    before = db_path.stat()

    plan = preflight.build_preflight_plan(
        _env(
            tmp_path,
            **{f"{direct.DIRECT_ENV_PREFIX}DB_PATH": str(db_path)},
        ),
        hermes_home=tmp_path / "home",
    )

    after = db_path.stat()
    assert plan["ok"] is False
    assert "mini_store_schema_tables_invalid" in plan["errors"]
    assert (after.st_ino, after.st_size, after.st_mtime_ns) == (
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    )
    assert not Path(f"{db_path}-wal").exists()
    assert not Path(f"{db_path}-shm").exists()
    assert not Path(f"{db_path}-journal").exists()


def test_rejects_stable_direct_default_paths(tmp_path: Path):
    stable_root = (
        tmp_path / "home" / "runtime" / "pnc_agent" / "feishu_issue_kafka_rca_direct"
    )
    plan = preflight.build_preflight_plan(
        _env(
            tmp_path,
            **{f"{direct.DIRECT_ENV_PREFIX}DB_PATH": str(stable_root / "mini.sqlite3")},
            **{
                f"{direct.DIRECT_ENV_PREFIX}HEALTH_PATH": str(
                    stable_root / "health.json"
                )
            },
        ),
        hermes_home=tmp_path / "home",
    )

    assert plan["ok"] is False
    assert "mini_store_path_is_production_path" in plan["errors"]
    assert "health_path_is_production_path" in plan["errors"]
    assert not stable_root.exists()


def test_rejects_symlinked_parent_for_absent_shadow_paths(tmp_path: Path, monkeypatch):
    real_root = tmp_path / "real"
    real_root.mkdir()
    alias_root = tmp_path / "alias"
    alias_root.symlink_to(real_root, target_is_directory=True)
    monkeypatch.setattr(
        preflight,
        "inspect_mini_store_path",
        lambda *_args, **_kwargs: pytest.fail("unsafe MiniStore path was inspected"),
    )

    plan = preflight.build_preflight_plan(
        _env(
            tmp_path,
            **{
                f"{direct.DIRECT_ENV_PREFIX}DB_PATH": str(
                    alias_root / "shadow" / "mini.sqlite3"
                ),
                f"{direct.DIRECT_ENV_PREFIX}HEALTH_PATH": str(
                    alias_root / "shadow" / "health.json"
                ),
            },
        ),
        hermes_home=tmp_path / "home",
    )

    assert plan["ok"] is False
    assert "mini_store_parent_symlink_forbidden" in plan["errors"]
    assert "health_parent_symlink_forbidden" in plan["errors"]


def test_rejects_absent_paths_below_a_production_root(tmp_path: Path, monkeypatch):
    production_root = (
        tmp_path / "home" / "runtime" / "pnc_agent" / "feishu_issue_kafka_rca"
    )
    monkeypatch.setattr(
        preflight,
        "inspect_mini_store_path",
        lambda *_args, **_kwargs: pytest.fail(
            "production MiniStore path was inspected"
        ),
    )

    plan = preflight.build_preflight_plan(
        _env(
            tmp_path,
            **{
                f"{direct.DIRECT_ENV_PREFIX}DB_PATH": str(
                    production_root / "shadow" / "mini.sqlite3"
                ),
                f"{direct.DIRECT_ENV_PREFIX}HEALTH_PATH": str(
                    production_root / "shadow" / "health.json"
                ),
            },
        ),
        hermes_home=tmp_path / "home",
    )

    assert plan["ok"] is False
    assert "mini_store_path_is_production_root" in plan["errors"]
    assert "health_path_is_production_root" in plan["errors"]


def test_cli_is_redacted_and_never_constructs_ministore_or_kafka(
    tmp_path: Path, monkeypatch, capsys
):
    env_file = tmp_path / "direct-shadow.env"
    values = _env(tmp_path)
    env_file.write_text(
        "\n".join(f"{key}={value}" for key, value in values.items()) + "\n",
        encoding="utf-8",
    )
    env_file.chmod(0o600)
    monkeypatch.setattr(direct.os, "environ", {})
    monkeypatch.setattr(
        direct,
        "MiniStore",
        lambda *_args, **_kwargs: pytest.fail("constructed MiniStore"),
    )
    monkeypatch.setattr(
        direct,
        "create_kafka_consumer",
        lambda *_args, **_kwargs: pytest.fail("constructed Kafka consumer"),
    )

    assert (
        preflight.main([
            "--env-file",
            str(env_file),
            "--hermes-home",
            str(tmp_path / "home"),
        ])
        == 0
    )

    output = capsys.readouterr()
    payload = json.loads(output.out)
    assert payload["ok"] is True
    assert payload["env_file"]["safe"] is True
    assert payload["env_file"]["mode"] == "0600"
    assert payload["env_file"]["contents_redacted"] is True
    assert payload["side_effects"]["kafka_opened"] is False
    assert payload["side_effects"]["db_created"] is False
    assert "direct-password" not in output.out
    assert "direct-user" not in output.out
    assert output.err == ""


@pytest.mark.parametrize(
    ("setup", "expected"),
    [
        ("mode", "env_file_mode_must_be_0600"),
        ("symlink", "env_file_regular_no_symlink_required"),
        ("hardlink", "env_file_single_link_required"),
        ("owner", "env_file_owner_invalid"),
    ],
)
def test_env_file_identity_gate_fails_closed_before_read(
    tmp_path: Path, monkeypatch, setup: str, expected: str
):
    target = tmp_path / "target.env"
    target.write_text("SECRET=must-not-be-read\n", encoding="utf-8")
    target.chmod(0o600)
    candidate = target
    if setup == "mode":
        target.chmod(0o640)
    elif setup == "symlink":
        candidate = tmp_path / "candidate.env"
        candidate.symlink_to(target)
    elif setup == "hardlink":
        candidate = tmp_path / "candidate.env"
        candidate.hardlink_to(target)
    elif setup == "owner":
        monkeypatch.setattr(preflight.os, "geteuid", lambda: target.stat().st_uid + 1)
    monkeypatch.setattr(
        direct,
        "load_direct_environment",
        lambda *_args, **_kwargs: pytest.fail("unsafe env file was read"),
    )

    plan = preflight.build_preflight_plan(
        env_file=candidate,
        hermes_home=tmp_path / "home",
    )

    assert plan["ok"] is False
    assert expected in plan["errors"]
    assert plan["env_file"]["contents_redacted"] is True
    assert "must-not-be-read" not in json.dumps(plan)


def test_env_file_identity_is_rechecked_after_read(tmp_path: Path, monkeypatch):
    env_file = tmp_path / "direct.env"
    env_file.write_text("SAFE=1\n", encoding="utf-8")
    env_file.chmod(0o600)

    def mutate_after_read(*_args, **_kwargs):
        env_file.write_text("SAFE=changed\n", encoding="utf-8")
        return _env(tmp_path), env_file

    monkeypatch.setattr(direct, "load_direct_environment", mutate_after_read)

    plan = preflight.build_preflight_plan(
        env_file=env_file,
        hermes_home=tmp_path / "home",
    )

    assert plan["ok"] is False
    assert "env_file_changed_during_read" in plan["errors"]


def test_config_failure_redacts_secret_from_structured_error(
    tmp_path: Path, monkeypatch
):
    env = _env(tmp_path)

    def fail(_source, **_kwargs):
        raise ValueError("bad direct-password credential")

    monkeypatch.setattr(direct.DirectKafkaConfig, "from_env", fail)

    plan = preflight.build_preflight_plan(env, hermes_home=tmp_path / "home")

    assert plan["ok"] is False
    assert plan["errors"] == ["bad <redacted> credential"]
    assert "direct-password" not in json.dumps(plan)


def test_assert_preflight_fails_closed(tmp_path: Path):
    plan = preflight.build_preflight_plan({}, hermes_home=tmp_path / "home")

    with pytest.raises(preflight.ShadowPreflightError, match="preflight_failed"):
        preflight.assert_preflight(plan)
