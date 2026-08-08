from __future__ import annotations

from copy import deepcopy
import json
import os
from pathlib import Path
import plistlib

import pytest

from scripts import pnc_rca_live_profile_switch_transaction as switch
from scripts import pnc_rca_release_transaction as base


RELEASE_ID = "rca-successor-test"
AUTHORITY_EPOCH_ID = "rca-authority-successor-test"
ACTIVATION_EPOCH_ID = "rca-activation-successor-test"


def _write(path: Path, raw: bytes, mode: int = 0o600) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.write_bytes(raw)
    path.chmod(mode)


def _plist_source(tmp_path: Path, hermes_home: Path) -> Path:
    root = tmp_path / "source"
    root.mkdir(mode=0o700)
    relay = {
        "Label": "local.pnc.completion-notice-relay",
        "ProgramArguments": [
            "/usr/bin/python3",
            str(hermes_home / "runtime/governance-tools/pnc_live_exec.py"),
            "local.pnc.completion-notice-relay",
            "--task-id",
            switch.STALE_RELAY_TASK_ID,
        ],
        "EnvironmentVariables": {"HERMES_HOME": str(hermes_home)},
    }
    dispatcher = {
        "Label": "local.pnc.rca-delivery-dispatcher",
        "ProgramArguments": [
            "/usr/bin/python3",
            str(hermes_home / "runtime/governance-tools/pnc_live_exec.py"),
            "local.pnc.rca-delivery-dispatcher",
        ],
        "EnvironmentVariables": {"HERMES_HOME": str(hermes_home)},
    }
    _write(root / "local.pnc.completion-notice-relay.plist", plistlib.dumps(relay))
    _write(root / "local.pnc.rca-delivery-dispatcher.plist", plistlib.dumps(dispatcher))
    return root


def _env_for_dispatcher(*, enabled: bool, release_id: str = RELEASE_ID) -> dict[str, str]:
    return {
        "HERMES_RCA_DELIVERY_DISPATCHER_INVENTORY_PIN": "a" * 64,
        "HERMES_RCA_DELIVERY_DISPATCHER_OBSERVATION_RELEASE_ID": release_id,
        "HERMES_RCA_DELIVERY_DISPATCHER_ENABLED": "true" if enabled else "false",
        "HERMES_RCA_DELIVERY_DISPATCHER_OBSERVABILITY_ENABLED": "true",
    }


def test_live_projection_uses_validated_dispatcher_environment(tmp_path):
    home = tmp_path / "home"
    hermes_home = home / ".hermes"
    source = _plist_source(tmp_path, hermes_home)
    dispatcher = switch._project_plist(
        (source / "local.pnc.rca-delivery-dispatcher.plist").read_bytes(),
        label="local.pnc.rca-delivery-dispatcher",
        outbound="live",
        enabled=True,
        hermes_home=hermes_home,
        release_id=RELEASE_ID,
        dispatcher_environment=_env_for_dispatcher(enabled=True),
    )
    value = plistlib.loads(dispatcher)
    environment = value["EnvironmentVariables"]
    assert environment["HERMES_OUTBOUND_MODE"] == "live"
    assert environment["HERMES_RCA_DELIVERY_DISPATCHER_ENABLED"] == "true"
    assert environment["HERMES_RCA_DELIVERY_DISPATCHER_INVENTORY_PIN"] == "a" * 64
    assert environment["HERMES_RCA_DELIVERY_DISPATCHER_OBSERVATION_RELEASE_ID"] == RELEASE_ID

    with pytest.raises(switch.LiveProfileSwitchError) as error:
        switch._project_plist(
            (source / "local.pnc.rca-delivery-dispatcher.plist").read_bytes(),
            label="local.pnc.rca-delivery-dispatcher",
            outbound="live",
            enabled=True,
            hermes_home=hermes_home,
            release_id=RELEASE_ID,
            dispatcher_environment={**_env_for_dispatcher(enabled=True), "HERMES_RCA_DELIVERY_DISPATCHER_OBSERVABILITY_ENABLED": "false"},
        )
    assert error.value.code == "pnc_rca_live_profile_switch_dispatcher_environment_invalid"


def test_relay_projection_switches_send_with_outbound_mode(tmp_path):
    home = tmp_path / "home"
    hermes_home = home / ".hermes"
    source = _plist_source(tmp_path, hermes_home)
    relay_source = source / "local.pnc.completion-notice-relay.plist"

    live = switch._project_plist(
        relay_source.read_bytes(),
        label="local.pnc.completion-notice-relay",
        outbound="live",
        enabled=True,
        hermes_home=hermes_home,
        release_id=RELEASE_ID,
        dispatcher_environment={},
    )
    live_value = plistlib.loads(live)
    assert live_value["ProgramArguments"].count("--send") == 1
    assert "--task-id" not in live_value["ProgramArguments"]
    assert live_value["EnvironmentVariables"]["HERMES_OUTBOUND_MODE"] == "live"

    record_only = switch._project_plist(
        live,
        label="local.pnc.completion-notice-relay",
        outbound="record-only",
        enabled=False,
        hermes_home=hermes_home,
        release_id=RELEASE_ID,
        dispatcher_environment={},
    )
    record_value = plistlib.loads(record_only)
    assert "--send" not in record_value["ProgramArguments"]
    assert record_value["EnvironmentVariables"]["HERMES_OUTBOUND_MODE"] == "record-only"


def test_plist_non_json_value_fails_closed(tmp_path):
    hermes_home = tmp_path / ".hermes"
    source = plistlib.loads(
        _plist_source(tmp_path, hermes_home)
        .joinpath("local.pnc.completion-notice-relay.plist")
        .read_bytes()
    )
    source["OpaqueData"] = b"\x00\x01"
    with pytest.raises(switch.LiveProfileSwitchError) as error:
        switch._project_plist(
            plistlib.dumps(source),
            label="local.pnc.completion-notice-relay",
            outbound="live",
            enabled=True,
            hermes_home=hermes_home,
            release_id=RELEASE_ID,
            dispatcher_environment={},
        )
    assert error.value.code == "pnc_rca_live_profile_switch_plist_invalid"


def test_baseline_status_digest_ignores_only_validation_binding_identity():
    first = {
        "ready": True,
        "state": "acknowledged",
        "baseline_identity": {
            "active_release_binding_sha256": "a" * 64,
            "candidate_env_sha256": "b" * 64,
            "db_logical_identity_sha256": "c" * 64,
        },
        "lifetime": {"jobs": 1},
    }
    second = deepcopy(first)
    second["baseline_identity"]["active_release_binding_sha256"] = "d" * 64
    assert switch._canonical(switch._stable_baseline_status(first)) == switch._canonical(
        switch._stable_baseline_status(second)
    )
    second["baseline_identity"]["candidate_env_sha256"] = "e" * 64
    assert switch._canonical(switch._stable_baseline_status(first)) != switch._canonical(
        switch._stable_baseline_status(second)
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("__manifest__", None),
        ("gateway_release_binding", []),
        ("rca_release_authority", "invalid"),
    ],
)
def test_manifest_nested_bindings_fail_closed(field, value):
    manifest = {
        "gateway_release_binding": {
            "capacity_admission": {"release_id": RELEASE_ID}
        },
        "rca_release_authority": {"release_id": RELEASE_ID},
    }
    if field == "__manifest__":
        manifest = value
    else:
        manifest[field] = value
    with pytest.raises(switch.LiveProfileSwitchError) as error:
        switch._manifest_components(manifest)
    assert error.value.code == "pnc_rca_live_profile_switch_manifest_invalid"


def test_manifest_capacity_binding_requires_mapping():
    manifest = {
        "gateway_release_binding": {"capacity_admission": "invalid"},
        "rca_release_authority": {"release_id": RELEASE_ID},
    }
    with pytest.raises(switch.LiveProfileSwitchError) as error:
        switch._manifest_components(manifest)
    assert error.value.code == "pnc_rca_live_profile_switch_manifest_invalid"


@pytest.mark.parametrize(
    "faces",
    [None, [], {"host_runtime": "invalid"}],
)
def test_authority_host_face_requires_mapping(faces):
    with pytest.raises(switch.LiveProfileSwitchError) as error:
        switch._host_runtime_face({"faces": faces})
    assert error.value.code == "pnc_rca_live_profile_switch_authority_invalid"


def test_authority_value_requires_mapping():
    with pytest.raises(switch.LiveProfileSwitchError) as error:
        switch._host_runtime_face(None)
    assert error.value.code == "pnc_rca_live_profile_switch_authority_invalid"


def _make_plan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[dict, Path, dict[str, Path]]:
    home = tmp_path / "home"
    hermes_home = home / ".hermes"
    candidate_root = tmp_path / "candidate-root"
    source_root = tmp_path / "source-root"
    source_root.mkdir(mode=0o700)
    state_root = hermes_home / "runtime/pnc_agent/feishu_issue_kafka_rca"
    launch_dir = home / "Library/LaunchAgents"
    transaction_dir = tmp_path / "mode-switch" / "switch-test"
    rollback_dir = transaction_dir / "rollback"
    staged_dir = transaction_dir / "staged"
    for directory in (state_root, launch_dir, transaction_dir, rollback_dir, staged_dir):
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)

    anchor_root = tmp_path / "anchors"
    anchor_paths = {
        name: anchor_root / name.replace("/", "_")
        for name in switch.ANCHOR_NAMES
    }
    monkeypatch.setattr(
        switch,
        "_anchor_paths",
        lambda **_kwargs: dict(anchor_paths),
    )
    anchor_bindings = {}
    for name, path in anchor_paths.items():
        raw = f"anchor:{name}\n".encode()
        _write(path, raw)
        observation = {"exists": True, **base._observe(path, required=True)}
        anchor_bindings[name] = {
            "path": str(path),
            "sha256": observation["sha256"],
            "observation": observation,
        }

    profile_path = candidate_root / "mode-switch-profile.json"
    authority_path = candidate_root / "authority.json"
    _write(profile_path, b"{}\n")
    _write(authority_path, b"{}\n")

    target_paths = {
        "env": hermes_home / ".env",
        "binding": state_root / "active-release-binding.json",
        "manifest": hermes_home / "runtime/LIVE_MANIFEST.json",
        "local.pnc.completion-notice-relay.plist": launch_dir / "local.pnc.completion-notice-relay.plist",
        "local.pnc.rca-delivery-dispatcher.plist": launch_dir / "local.pnc.rca-delivery-dispatcher.plist",
    }
    profile_paths = {
        "initial_profile": {
            name: candidate_root / ("candidate.env" if name == "env" else "active-release-binding.json" if name == "binding" else "LIVE_MANIFEST.json" if name == "manifest" else name)
            for name in switch.TARGET_NAMES
        },
        "live_profile": {
            name: candidate_root / "live-profile" / ("candidate.env" if name == "env" else "active-release-binding.json" if name == "binding" else "LIVE_MANIFEST.json" if name == "manifest" else name)
            for name in switch.TARGET_NAMES
        },
    }
    initial_profile = {}
    live_profile = {}
    entries = []
    for index, name in enumerate(switch.TARGET_NAMES):
        mode = switch.TARGET_MODES[name]
        profile_mode = 0o600 if name.endswith(".plist") else mode
        old = f"old-{name}\n".encode()
        new = f"new-{name}\n".encode()
        _write(target_paths[name], old, mode)
        _write(profile_paths["initial_profile"][name], old, profile_mode)
        _write(profile_paths["live_profile"][name], new, profile_mode)
        staged = staged_dir / f"{index:02d}-{name}.blob"
        _write(staged, new, mode)
        initial_obs = {"exists": True, **base._observe(profile_paths["initial_profile"][name], required=True)}
        live_obs = {"exists": True, **base._observe(profile_paths["live_profile"][name], required=True)}
        staged_obs = {"exists": True, **base._observe(staged, required=True)}
        before = {"exists": True, **base._observe(target_paths[name], required=True)}
        initial_profile[name] = {"path": str(profile_paths["initial_profile"][name]), "observation": initial_obs}
        live_profile[name] = {"path": str(profile_paths["live_profile"][name]), "observation": live_obs}
        entries.append(
            {
                "name": name,
                "source_path": str(profile_paths["live_profile"][name]),
                "source": live_obs,
                "staged_path": str(staged),
                "staged": staged_obs,
                "target_path": str(target_paths[name]),
                "target_mode": format(mode, "04o"),
                "before": before,
                "rollback_path": str(rollback_dir / f"{index:02d}-{name}.before"),
            }
        )

    baseline_path = tmp_path / "evidence" / "delivery-quarantine-baseline.json"
    _write(baseline_path, b"{\"baseline_id\":\"baseline-test\"}\n")
    baseline_obs = {"exists": True, **base._observe(baseline_path, required=True)}
    control_db = tmp_path / "control.sqlite3"
    _write(control_db, b"sqlite-placeholder\n")
    plan = {
        "schema_version": switch.SCHEMA_VERSION,
        "cli_schema_version": switch.CLI_SCHEMA_VERSION,
        "transaction_id": "switch-test",
        "planned_at": "2026-08-07T00:00:00+00:00",
        "release_id": RELEASE_ID,
        "authority_sha256": "a" * 64,
        "authority_epoch_id": AUTHORITY_EPOCH_ID,
        "candidate_root": str(candidate_root),
        "source_root": str(source_root),
        "source_commit": "b" * 40,
        "source_tree": "c" * 40,
        "home": str(home),
        "hermes_home": str(hermes_home),
        "control_db": str(control_db),
        "state_root": str(state_root),
        "transaction_dir": str(transaction_dir),
        "rollback_dir": str(rollback_dir),
        "mode_switch_profile": {"path": str(profile_path), "observation": {"exists": True, **base._observe(profile_path, required=True)}},
        "authority": {"path": str(authority_path), "observation": {"exists": True, **base._observe(authority_path, required=True)}},
        "initial_profile": initial_profile,
        "live_profile": live_profile,
        "read_only_plist_anchors": anchor_bindings,
        "activation_binding": {"epoch_id": ACTIVATION_EPOCH_ID, "state": "bounded_active", "updated_at": "2026-08-07T00:00:00+00:00"},
        "quarantine_baseline": {
            "path": str(baseline_path),
            "observation": baseline_obs,
            "baseline_id": "baseline-test",
            "baseline_fingerprint": "d" * 64,
            "status_sha256": "e" * 64,
            "db_logical_identity_sha256": "f" * 64,
        },
        "entries": entries,
        "mutation_performed": False,
        "production_effects": switch._effects(),
    }
    plan_path = transaction_dir / "plan.json"
    _write(plan_path, base._pretty(plan))
    switch._validate_plan(plan)
    return plan, plan_path, target_paths


def test_strict_plan_rejects_target_tamper(tmp_path, monkeypatch):
    plan, _plan_path, _targets = _make_plan(tmp_path, monkeypatch)
    tampered = deepcopy(plan)
    tampered["entries"][0]["target_path"] = str(tmp_path / "outside")
    with pytest.raises(switch.LiveProfileSwitchError) as error:
        switch._validate_plan(tampered)
    assert error.value.code == "pnc_rca_live_profile_switch_plan_invalid"


def test_apply_and_rollback_happy_path(tmp_path, monkeypatch):
    plan, plan_path, targets = _make_plan(tmp_path, monkeypatch)
    monkeypatch.setattr(switch, "_locked_validation", lambda *_args, **_kwargs: None)
    receipt = switch._apply(plan, plan_path=plan_path)
    assert receipt["production_effects"] == switch._effects()
    assert all(target.read_bytes() == f"new-{name}\n".encode() for name, target in targets.items())
    rollback = switch.rollback(Path(receipt["receipt_path"]), output_path=tmp_path / "rollback.json")
    assert rollback["filesystem_restored_to_pre_transaction"] is True
    assert rollback["overall_release_state_restored"] is False
    assert all(target.read_bytes() == f"old-{name}\n".encode() for name, target in targets.items())


def test_post_replace_fsync_failure_is_restored(tmp_path, monkeypatch):
    plan, plan_path, targets = _make_plan(tmp_path, monkeypatch)
    monkeypatch.setattr(switch, "_locked_validation", lambda *_args, **_kwargs: None)
    original_sync = switch._sync_directory
    failed = False

    def fail_once(path):
        nonlocal failed
        if not failed:
            failed = True
            raise RuntimeError("injected post-replace fsync failure")
        original_sync(path)

    monkeypatch.setattr(switch, "_sync_directory", fail_once)
    with pytest.raises(RuntimeError, match="injected post-replace fsync failure"):
        switch._apply(plan, plan_path=plan_path)
    assert targets["env"].read_bytes() == b"old-env\n"
    automatic = json.loads(
        (Path(plan["transaction_dir"]) / "automatic-rollback.json").read_text()
    )
    assert automatic["filesystem_restored_to_pre_transaction"] is True
    assert automatic["blocked_entries"] == []


def test_post_replace_failure_fails_closed_without_after_identity(tmp_path, monkeypatch):
    plan, plan_path, targets = _make_plan(tmp_path, monkeypatch)
    monkeypatch.setattr(switch, "_locked_validation", lambda *_args, **_kwargs: None)
    failed = False

    def fail_after_replace(source, target):
        nonlocal failed
        os.replace(source, target)
        if target == targets["env"] and not failed:
            failed = True
            raise RuntimeError("injected replace failure")

    with pytest.raises(
        switch.LiveProfileSwitchError,
        match="pnc_rca_live_profile_switch_automatic_rollback_incomplete",
    ):
        switch._apply(plan, plan_path=plan_path, replace_func=fail_after_replace)
    assert targets["env"].read_bytes() == b"new-env\n"
    automatic = json.loads(
        (Path(plan["transaction_dir"]) / "automatic-rollback.json").read_text()
    )
    assert automatic["filesystem_restored_to_pre_transaction"] is False
    assert automatic["blocked_entries"][0]["reason"] == "after_identity_unknown"


def test_concurrent_target_change_is_not_clobbered(tmp_path, monkeypatch):
    plan, plan_path, targets = _make_plan(tmp_path, monkeypatch)
    monkeypatch.setattr(switch, "_locked_validation", lambda *_args, **_kwargs: None)
    original_replace = os.replace
    forward = True
    rollback_clobber_attempted = False

    def inject_competing_write(source, target):
        nonlocal forward, rollback_clobber_attempted
        source_path = Path(source)
        target_path = Path(target)
        if target_path == targets["env"] and not forward:
            try:
                if source_path.read_bytes() == b"old-env\n":
                    rollback_clobber_attempted = True
            except OSError:
                pass
        if forward and target_path == targets["env"]:
            original_replace(source, target)
            target_path.write_bytes(b"foreign-writer\n")
            target_path.chmod(switch.TARGET_MODES["env"])
            return
        if forward and target_path == targets["binding"]:
            forward = False
            raise RuntimeError("injected competing transaction")
        return original_replace(source, target)

    with pytest.raises(switch.LiveProfileSwitchError) as error:
        switch._apply(plan, plan_path=plan_path, replace_func=inject_competing_write)
    assert error.value.code == "pnc_rca_live_profile_switch_automatic_rollback_incomplete"
    assert targets["env"].read_bytes() == b"foreign-writer\n"
    assert rollback_clobber_attempted is False
    automatic = json.loads((Path(plan["transaction_dir"]) / "automatic-rollback.json").read_text())
    assert automatic["blocked_entries"][0]["name"] == "env"


def test_same_bytes_replaced_by_competitor_is_not_clobbered(tmp_path, monkeypatch):
    plan, plan_path, targets = _make_plan(tmp_path, monkeypatch)
    monkeypatch.setattr(switch, "_locked_validation", lambda *_args, **_kwargs: None)
    original_replace = os.replace
    injected = False

    def inject_same_bytes(source, target):
        nonlocal injected
        result = original_replace(source, target)
        if Path(target) == targets["binding"] and not injected:
            injected = True
            # Replace the env with identical bytes but a new inode after the
            # transaction has already recorded its after observation.
            replacement = Path(target).parent / ".foreign-same-bytes"
            replacement.write_bytes(targets["env"].read_bytes())
            replacement.chmod(switch.TARGET_MODES["env"])
            replacement.replace(targets["env"])
            raise RuntimeError("injected same-bytes competitor")
        return result

    with pytest.raises(switch.LiveProfileSwitchError) as error:
        switch._apply(plan, plan_path=plan_path, replace_func=inject_same_bytes)
    assert error.value.code == "pnc_rca_live_profile_switch_automatic_rollback_incomplete"
    assert targets["env"].read_bytes() == b"new-env\n"


def test_rollback_rechecks_target_before_replace(tmp_path, monkeypatch):
    plan, _plan_path, targets = _make_plan(tmp_path, monkeypatch)
    switch.base._backup(plan)
    target = targets["env"]
    _write(target, b"new-env\n", switch.TARGET_MODES["env"])
    expected_after = switch.base._observe(target, required=True)
    original_observe = switch.base._observe
    injected = False

    def observe_with_competitor(path, *, required):
        nonlocal injected
        observed = original_observe(path, required=required)
        if Path(path) == target and not injected:
            injected = True
            foreign = target.parent / ".foreign-rollback-writer"
            _write(foreign, b"foreign-rollback-writer\n", switch.TARGET_MODES["env"])
            os.replace(foreign, target)
        return observed

    monkeypatch.setattr(switch.base, "_observe", observe_with_competitor)
    result = switch._restore_written_no_clobber(
        plan,
        written_names=["env"],
        after_observations={"env": expected_after},
    )
    assert result["restored"] == []
    assert result["blocked"][0]["reason"] == "target_changed_before_restore"
    assert target.read_bytes() == b"foreign-rollback-writer\n"


def test_manual_rollback_refuses_activation_state_change(tmp_path, monkeypatch):
    plan, plan_path, targets = _make_plan(tmp_path, monkeypatch)
    monkeypatch.setattr(switch, "_locked_validation", lambda *_args, **_kwargs: None)
    receipt = switch._apply(plan, plan_path=plan_path)

    def reject_changed_state(_plan, *, installed_mode):
        if installed_mode == "live":
            raise switch.LiveProfileSwitchError("pnc_rca_live_profile_switch_activation_not_bounded")

    monkeypatch.setattr(switch, "_locked_validation", reject_changed_state)
    with pytest.raises(switch.LiveProfileSwitchError) as error:
        switch.rollback(Path(receipt["receipt_path"]), output_path=tmp_path / "rollback.json")
    assert error.value.code == "pnc_rca_live_profile_switch_activation_not_bounded"
    assert targets["env"].read_bytes() == b"new-env\n"
