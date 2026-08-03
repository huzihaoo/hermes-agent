from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import plistlib
import subprocess

import pytest
import yaml

from gateway import pnc_rca_prod_bootstrap as bootstrap
from gateway import pnc_rca_release_authority as authority
from scripts import pnc_rca_release_transaction as transaction


NOW = datetime.now(timezone.utc).replace(microsecond=0)
RELEASE_ID = "rca-r11-transaction-test"
AUTHORITY_EPOCH = "rca-authority-r11-transaction-test"
BOOTSTRAP_EPOCH = "rca-bootstrap-r11-transaction-test"
SHA = "3" * 64


def _write(path: Path, raw: bytes, mode: int = 0o600) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.write_bytes(raw)
    path.chmod(mode)


def _json_bytes(value: dict) -> bytes:
    return transaction._pretty(value)


def _git_repo(root: Path, hermes_home: Path) -> tuple[str, str]:
    root.mkdir(mode=0o700)
    for name in transaction.PLIST_NAMES:
        label = name[:-6]
        payload = {
            "Label": label,
            "ProgramArguments": [
                "/usr/bin/python3",
                str(hermes_home / "runtime/governance-tools/pnc_live_exec.py"),
                label,
            ],
            "WorkingDirectory": str(hermes_home / "runtime"),
            "EnvironmentVariables": {
                "HOME": str(hermes_home.parent),
                "HERMES_HOME": str(hermes_home),
                "PYTHONNOUSERSITE": "1",
            },
        }
        _write(root / name, plistlib.dumps(payload), 0o600)
    subprocess.run(("git", "-C", str(root), "init", "-q"), check=True)
    subprocess.run(("git", "-C", str(root), "config", "user.email", "test@example.invalid"), check=True)
    subprocess.run(("git", "-C", str(root), "config", "user.name", "Test"), check=True)
    subprocess.run(("git", "-C", str(root), "add", "."), check=True)
    subprocess.run(("git", "-C", str(root), "commit", "-qm", "test source"), check=True)
    commit = subprocess.check_output(("git", "-C", str(root), "rev-parse", "HEAD"), text=True).strip()
    tree = subprocess.check_output(("git", "-C", str(root), "rev-parse", "HEAD^{tree}"), text=True).strip()
    return commit, tree


def _authority(commit: str, tree: str, host_root: Path) -> dict:
    return {
        "schema_version": authority.AUTHORITY_SCHEMA_VERSION,
        "release_id": RELEASE_ID,
        "authority_epoch_id": AUTHORITY_EPOCH,
        "created_at": NOW.isoformat(),
        "status": "approved_for_activation",
        "supersedes_authority_sha256": None,
        "faces": {
            "host_runtime": {"commit": commit, "tree": tree, "root": str(host_root)},
            "vm_worker_state": {"commit": "4" * 40, "tree": "5" * 40, "root": "/vm/worker"},
            "g1q3_rca_pipeline": {"commit": "6" * 40, "tree": "7" * 40, "root": "/vm/pipeline"},
            "mcap_data_translate": {
                "commit": "8" * 40,
                "tree": "9" * 40,
                "root": "/vm/mcap",
                "contract_sha256": SHA,
            },
        },
        "control_store": {
            "schema_version": "pnc_rca_control_store_v13",
            "database_instance_id": "test-device-instance",
            "schema_fingerprint_sha256": SHA,
            "backup_receipt_sha256": SHA,
            "not_measured_reason": "",
        },
        "quarantine_baseline": {
            "state": "ready",
            "required": True,
            "schema_version": "pnc_rca_delivery_quarantine_baseline_v1",
            "baseline_sha256": SHA,
            "not_measured_reason": "",
        },
        "side_effect_policy": {
            "mode": "disabled",
            "single_active_writer": True,
            "allow_historical_requeue": False,
            "allowed_effect_kinds": ["feishu_issue_comment"],
        },
        "report_publication": {
            "canonical_base_url": "http://192.168.26.174:18081",
            "root": "/mnt/tmp",
            "manifest_schema_version": "pnc_rca_report_manifest_v1",
        },
        "feishu_capability": {
            "required_surfaces": ["issue_comment"],
            "capability_profile_sha256": SHA,
            "not_measured_reason": "",
        },
    }


def _fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict:
    home = tmp_path / "home"
    hermes_home = home / ".hermes"
    runtime = hermes_home / "runtime"
    state_root = runtime / "pnc_agent/feishu_issue_kafka_rca"
    launch_dir = home / "Library/LaunchAgents"
    for path in (state_root, launch_dir, runtime, hermes_home):
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
    source_root = tmp_path / "source"
    commit, tree = _git_repo(source_root, hermes_home)
    candidate_root = tmp_path / "candidate"
    candidate_root.mkdir(mode=0o700)
    host_root = tmp_path / "installed-host"
    host_root.mkdir(mode=0o700)
    auth_path = tmp_path / "ssh-mini/rca-bootstrap-capacity-authorization.json"
    monkeypatch.setattr(bootstrap, "BOOTSTRAP_AUTHORIZATION_PATH", auth_path)

    value = _authority(commit, tree, host_root)
    authority_sha = authority.canonical_json_sha256(value)
    _write(candidate_root / "authority.json", _json_bytes(value))
    pointer = authority.build_active_pointer(
        value,
        authority_path=state_root / f"{RELEASE_ID}.authority.json",
        state="active",
        activated_at=NOW.isoformat(),
    )
    _write(candidate_root / "ACTIVE_RCA_RELEASE.json", _json_bytes(pointer))

    env = (
        f"HERMES_HOME={hermes_home}\n"
        f"HERMES_RCA_PROD_CAPACITY_MODE=bootstrap\n"
        f"HERMES_RCA_PROD_RELEASE_ID={RELEASE_ID}\n"
        f"HERMES_RCA_OUTBOX_ALLOW_FEISHU_WRITEBACK=false\n"
        f"HERMES_RCA_DELIVERY_DISPATCHER_ENABLED=false\n"
        f"HERMES_RCA_DELIVERY_QUARANTINE_BASELINE_SHA256={SHA}\n"
    ).encode()
    env_sha = hashlib.sha256(env).hexdigest()
    _write(candidate_root / "candidate.env", env)
    config = {
        "model": {"provider": "test-provider"},
        "platforms": {
            "feishu": {
                "extra": {
                    "default_group_policy": "disabled",
                    "group_allowed_chats": ["oc_test"],
                }
            }
        },
    }
    config_raw = yaml.safe_dump(config, sort_keys=False).encode("utf-8")
    config_sha = hashlib.sha256(config_raw).hexdigest()
    config_semantic_sha = hashlib.sha256(
        json.dumps(
            config,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    _write(candidate_root / "config.yaml", config_raw)
    manifest_faces = value["faces"]
    manifest = {
        "schema_version": 1,
        "runtime_root": str(host_root),
        "env_sha256": env_sha,
        "config_path": str(hermes_home / "config.yaml"),
        "config_sha256": config_sha,
        "config_semantic_sha256": config_semantic_sha,
        "rca_release_authority": {
            "release_id": RELEASE_ID,
            "authority_sha256": authority_sha,
        },
        "face_git_bindings": {
            "runtime_engine": {**manifest_faces["host_runtime"], "repo": str(host_root)},
            "vm_worker_state": {**manifest_faces["vm_worker_state"], "repo": "/vm/worker"},
            "g1q3_rca_pipeline": {**manifest_faces["g1q3_rca_pipeline"], "repo": "/vm/pipeline"},
            "mcap_data_translate": {**manifest_faces["mcap_data_translate"], "repo": "/vm/mcap"},
        },
    }
    _write(candidate_root / "LIVE_MANIFEST.json", _json_bytes(manifest))

    approval_sha = "a" * 64
    release_bom_sha = "b" * 64
    authorization = bootstrap.issue_bootstrap_authorization(
        bootstrap_epoch_id=BOOTSTRAP_EPOCH,
        started_at=NOW,
        deadline=NOW + timedelta(hours=2),
        release_approval_id=RELEASE_ID,
        release_bom_sha256=release_bom_sha,
        approval_evidence_sha256=approval_sha,
        authorized_by="songying",
        authorized_role="owner",
        now=NOW,
        receipt_id="bootstrap-auth-transaction-test",
    )
    authorization_raw = bootstrap.canonical_bytes(authorization)
    _write(candidate_root / "bootstrap-authorization.json", authorization_raw)
    binding = {
        "schema_version": bootstrap.ACTIVE_RELEASE_BINDING_SCHEMA_VERSION,
        "release_id": RELEASE_ID,
        "authority_sha256": authority_sha,
        "authority_epoch_id": AUTHORITY_EPOCH,
        "complete": True,
        "live_write_performed": False,
        "bindings": {
            "release_bom_sha256": release_bom_sha,
            "release_approval": {"sha256": approval_sha},
            "candidate_env": {"sha256": env_sha},
            "bootstrap_authorization": {
                "sha256": hashlib.sha256(authorization_raw).hexdigest(),
                "receipt_fingerprint": authorization["receipt_fingerprint"],
            },
        },
        "policy": {
            "capacity_admission": {
                "capacity_mode": "bootstrap",
                "bootstrap_epoch_id": BOOTSTRAP_EPOCH,
                "bootstrap_authorization_fingerprint": authorization["receipt_fingerprint"],
                "bootstrap_authorization_sha256": hashlib.sha256(authorization_raw).hexdigest(),
                "release_bom_sha256": release_bom_sha,
                "release_approval_id": RELEASE_ID,
                "approval_evidence_sha256": approval_sha,
            }
        },
        "side_effect_contract": {
            "canonical_active_release_binding": str(state_root / "active-release-binding.json"),
            "canonical_live_env": str(hermes_home / ".env"),
        },
    }
    _write(candidate_root / "active-release-binding.json", _json_bytes(binding))
    for name in transaction.PLIST_NAMES:
        _write(candidate_root / name, (source_root / name).read_bytes())

    control_db = state_root / "control.sqlite3"
    import sqlite3

    connection = sqlite3.connect(control_db)
    connection.execute("CREATE TABLE control_meta(key TEXT PRIMARY KEY, value TEXT)")
    connection.execute(
        "INSERT INTO control_meta(key,value) VALUES ('schema_version','pnc_rca_control_store_v13')"
    )
    connection.commit()
    connection.close()
    evidence = tmp_path / "evidence"
    evidence.mkdir(mode=0o700)
    return {
        "candidate_root": candidate_root,
        "source_root": source_root,
        "home": home,
        "hermes_home": hermes_home,
        "control_db": control_db,
        "evidence": evidence,
        "state_root": state_root,
        "authority": value,
        "old": {},
    }


def _seed_old_targets(args: dict) -> None:
    targets = {
        args["hermes_home"] / "runtime/LIVE_MANIFEST.json": b"old-manifest\n",
        args["hermes_home"] / "config.yaml": b"old-config\n",
        args["state_root"] / "active-release-binding.json": b"old-binding\n",
        args["hermes_home"] / ".env": b"old-env\n",
        bootstrap.BOOTSTRAP_AUTHORIZATION_PATH: b"old-auth\n",
    }
    for name in transaction.PLIST_NAMES:
        targets[args["home"] / "Library/LaunchAgents" / name] = b"old-plist\n"
    for path, raw in targets.items():
        _write(path, raw)
        args["old"][path] = raw


def _build(args: dict, monkeypatch: pytest.MonkeyPatch):
    return transaction.build_plan(
        candidate_root=args["candidate_root"],
        source_root=args["source_root"],
        home=args["home"],
        hermes_home=args["hermes_home"],
        control_db=args["control_db"],
        evidence_root=args["evidence"],
        transaction_id="r11-test-transaction",
    )


def test_release_transaction_plan_apply_and_explicit_rollback(tmp_path, monkeypatch):
    args = _fixture(tmp_path, monkeypatch)
    _seed_old_targets(args)
    plan, plan_path = _build(args, monkeypatch)

    result = transaction.apply_plan(plan, plan_path=plan_path)
    assert result["mutation_performed"] is True
    assert result["production_effects"]["feishu_write"] is False
    assert (args["state_root"] / f"{RELEASE_ID}.authority.json").read_bytes() == (
        args["candidate_root"] / "authority.json"
    ).read_bytes()
    assert (args["hermes_home"] / ".env").read_bytes() == (
        args["candidate_root"] / "candidate.env"
    ).read_bytes()
    assert (args["hermes_home"] / "config.yaml").read_bytes() == (
        args["candidate_root"] / "config.yaml"
    ).read_bytes()
    assert (args["state_root"] / "ACTIVE_RCA_RELEASE.json").exists()
    assert (args["evidence"] / "r11-test-transaction/receipt.json").exists()

    rollback = transaction.rollback_transaction(
        Path(result["receipt_path"]),
        output_path=args["evidence"] / "r11-test-rollback.json",
    )
    assert rollback["restored_to_pre_transaction"] is True
    for path, raw in args["old"].items():
        assert path.read_bytes() == raw
    assert not (args["state_root"] / f"{RELEASE_ID}.authority.json").exists()


def test_release_transaction_auto_rolls_back_on_replace_failure(tmp_path, monkeypatch):
    args = _fixture(tmp_path, monkeypatch)
    _seed_old_targets(args)
    plan, plan_path = _build(args, monkeypatch)
    calls = 0

    def fail_after_two(source, target):
        nonlocal calls
        calls += 1
        if calls == 3:
            raise OSError("injected replace failure")
        os.replace(source, target)

    with pytest.raises(OSError, match="injected replace failure"):
        transaction.apply_plan(plan, plan_path=plan_path, replace_func=fail_after_two)
    for path, raw in args["old"].items():
        assert path.read_bytes() == raw
    assert not (args["state_root"] / f"{RELEASE_ID}.authority.json").exists()
    assert (
        args["evidence"] / "r11-test-transaction/automatic-rollback.json"
    ).exists()


def test_release_transaction_rejects_release_pinned_or_non_source_plist(
    tmp_path, monkeypatch
):
    args = _fixture(tmp_path, monkeypatch)
    dispatcher = args["candidate_root"] / "local.pnc.rca-delivery-dispatcher.plist"
    value = plistlib.loads(dispatcher.read_bytes())
    value["EnvironmentVariables"][
        "HERMES_RCA_DELIVERY_DISPATCHER_INVENTORY_PIN"
    ] = "a" * 64
    mutated = plistlib.dumps(value)
    with pytest.raises(
        transaction.ReleaseTransactionError, match="plist_release_pin"
    ):
        transaction._validate_plist(
            mutated,
            label="local.pnc.rca-delivery-dispatcher",
            hermes_home=args["hermes_home"],
        )
    _write(dispatcher, mutated)
    with pytest.raises(
        transaction.ReleaseTransactionError, match="plist_source_mismatch"
    ):
        _build(args, monkeypatch)


def test_release_transaction_rejects_config_not_bound_by_manifest(
    tmp_path, monkeypatch
):
    args = _fixture(tmp_path, monkeypatch)
    config_path = args["candidate_root"] / "config.yaml"
    config_path.write_text("model:\n  provider: changed\n", encoding="utf-8")
    config_path.chmod(0o600)

    with pytest.raises(
        transaction.ReleaseTransactionError, match="transaction_config_invalid"
    ):
        _build(args, monkeypatch)
