"""Regression contract for persistent gateway config/env bindings.

These tests are intentionally pure-local: they generate definitions and patch
service-manager calls; no service, network, or production path is touched.
"""

from __future__ import annotations

import hashlib
import json
import os
import plistlib
import shlex
from pathlib import Path
from types import SimpleNamespace

import pytest

import hermes_cli.gateway as gateway
import hermes_cli.gateway_windows as gateway_windows
from hermes_cli import service_runtime_bindings as bindings


RELEASE = "hermes-agent-v0.18.2"
TAG = "v2026.7.7.2"
COMMIT = "a" * 40


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_manifest(
    home: Path,
    config_path: Path,
    env_path: Path,
    *,
    mode: str | None = "external",
) -> Path:
    release_identity = bindings.release_identity_for(RELEASE, TAG, COMMIT)
    manifest = {
        "runtime_root": str(home / "runtime-root"),
        "runtime_python": str(home / "runtime-python"),
        "release": RELEASE,
        "tag": TAG,
        "commit": COMMIT,
        "release_identity": release_identity,
        "config_env_binding": {
            "hermes_home": str(home),
            "release": RELEASE,
            "tag": TAG,
            "commit": COMMIT,
            "release_identity": release_identity,
            "config_path": str(config_path),
            "config_sha256": _sha(config_path),
            "env_path": str(env_path),
            "env_sha256": _sha(env_path),
        },
    }
    if mode is not None:
        manifest["config_env_binding_mode"] = mode
    manifest_path = home / "runtime" / "LIVE_MANIFEST.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    manifest_path.chmod(0o600)
    return manifest_path


def _sealed_home(
    tmp_path: Path, *, special_names: bool = False
) -> tuple[Path, Path, Path, Path]:
    home = tmp_path / ("Hermes Home & sealed" if special_names else "hermes-home")
    home.mkdir(parents=True)
    (home / "runtime").mkdir()
    projection = tmp_path / (
        "candidate % & (sealed)^!" if special_names else "candidate"
    )
    projection.mkdir()
    config_path = projection / (
        "config & <sealed>.yaml" if special_names else "config.yaml"
    )
    env_path = projection / ("secrets % & (sealed)^!.env" if special_names else ".env")
    config_path.write_bytes(b"model:\n  default: sealed\n")
    env_path.write_bytes(b"API_KEY=record-only\n")
    config_path.chmod(0o400)
    env_path.chmod(0o400)
    manifest_path = _write_manifest(home, config_path, env_path)
    return home, config_path, env_path, manifest_path


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _store_json(path: Path, payload: dict) -> None:
    path.chmod(0o600)
    path.write_text(json.dumps(payload), encoding="utf-8")
    path.chmod(0o600)


def _arrange_gateway_generation(monkeypatch, home: Path) -> None:
    monkeypatch.setattr(gateway, "get_hermes_home", lambda: home)
    monkeypatch.setattr(gateway, "get_python_path", lambda: "/opt/hermes/bin/python")
    monkeypatch.setattr(gateway, "_stable_service_working_dir", lambda: str(home))
    monkeypatch.setattr(gateway, "_detect_venv_dir", lambda: None)
    monkeypatch.setattr(gateway, "_build_service_path_dirs", lambda *args, **kwargs: [])
    monkeypatch.setattr(gateway, "_build_user_local_paths", lambda *args, **kwargs: [])
    monkeypatch.setattr(gateway, "_build_wsl_interop_paths", lambda *args, **kwargs: [])
    monkeypatch.setattr(gateway.shutil, "which", lambda _name: None)
    monkeypatch.setattr(gateway, "_get_restart_drain_timeout", lambda: 180.0)
    monkeypatch.setattr(gateway, "get_launchd_label", lambda: "ai.hermes.gateway")


def _systemd_environment(unit: str) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for line in unit.splitlines():
        if not line.startswith("Environment="):
            continue
        tokens = shlex.split(line.removeprefix("Environment="), posix=True)
        assert len(tokens) == 1
        key, value = tokens[0].split("=", 1)
        parsed[key] = value
    return parsed


def test_external_manifest_resolves_exact_pair_without_ambient_authority(
    tmp_path, monkeypatch
):
    home, config_path, env_path, _manifest = _sealed_home(tmp_path)
    monkeypatch.setenv("HERMES_CONFIG_PATH", "/ambient/wrong-config")
    monkeypatch.setenv("HERMES_ENV_PATH", "/ambient/wrong-env")

    assert bindings.resolve_service_runtime_bindings(home) == {
        "HERMES_CONFIG_PATH": str(config_path),
        "HERMES_ENV_PATH": str(env_path),
    }
    child = bindings.service_runtime_environment(home)
    assert child["HERMES_HOME"] == str(home)
    assert child["HERMES_CONFIG_PATH"] == str(config_path)
    assert child["HERMES_ENV_PATH"] == str(env_path)


def test_old_or_modeless_manifest_is_legacy_empty_pair(tmp_path):
    home, _config_path, _env_path, manifest_path = _sealed_home(tmp_path)
    _store_json(
        manifest_path,
        {
            "runtime_root": str(home / "runtime-root"),
            "runtime_python": str(home / "runtime-python"),
        },
    )

    assert bindings.resolve_service_runtime_bindings(home) == {}


def test_deleting_only_mode_from_external_manifest_is_not_a_legacy_downgrade(
    tmp_path,
):
    home, _config_path, _env_path, manifest_path = _sealed_home(tmp_path)
    manifest = _load_json(manifest_path)
    manifest.pop("config_env_binding_mode")
    _store_json(manifest_path, manifest)

    with pytest.raises(
        bindings.ServiceRuntimeBindingError, match="external-only fields"
    ):
        bindings.resolve_service_runtime_bindings(home)


@pytest.mark.parametrize("mode", [None, "", " ", "legacy", 1])
def test_explicit_non_external_mode_never_downgrades_to_legacy(tmp_path, mode):
    home, _config_path, _env_path, manifest_path = _sealed_home(tmp_path)
    manifest = _load_json(manifest_path)
    manifest["config_env_binding_mode"] = mode
    _store_json(manifest_path, manifest)

    with pytest.raises(bindings.ServiceRuntimeBindingError, match="unsupported"):
        bindings.resolve_service_runtime_bindings(home)


def test_named_profile_never_inherits_default_external_pair(tmp_path, monkeypatch):
    root, config_path, env_path, _manifest = _sealed_home(tmp_path)
    profile_home = root / "profiles" / "worker"
    profile_home.mkdir(parents=True)
    monkeypatch.setenv("HERMES_CONFIG_PATH", str(config_path))
    monkeypatch.setenv("HERMES_ENV_PATH", str(env_path))

    assert bindings.resolve_service_runtime_bindings(profile_home) == {}
    child = bindings.service_runtime_environment(profile_home)
    assert child["HERMES_HOME"] == str(profile_home)
    assert "HERMES_CONFIG_PATH" not in child
    assert "HERMES_ENV_PATH" not in child

    _arrange_gateway_generation(monkeypatch, profile_home)
    unit = gateway.generate_systemd_unit()
    assert "HERMES_CONFIG_PATH" not in unit
    assert "HERMES_ENV_PATH" not in unit
    plist = plistlib.loads(gateway.generate_launchd_plist().encode("utf-8"))
    assert "HERMES_CONFIG_PATH" not in plist["EnvironmentVariables"]
    assert "HERMES_ENV_PATH" not in plist["EnvironmentVariables"]


def test_unprofiled_captured_argv_targets_default_root_not_named_updater(
    tmp_path, monkeypatch
):
    root = tmp_path / "root"
    updater_home = root / "profiles" / "updater"
    updater_home.mkdir(parents=True)
    monkeypatch.setenv("HERMES_HOME", str(updater_home))
    argv = ["pythonw.exe", "-m", "hermes_cli.main", "gateway", "run"]

    assert gateway._target_home_for_gateway_argv(argv) == root


def test_profiled_captured_argv_uses_explicit_profile_home(monkeypatch, tmp_path):
    expected = tmp_path / "root" / "profiles" / "worker"
    monkeypatch.setattr(
        gateway, "_target_home_for_profile", lambda profile: expected / profile
    )

    assert (
        gateway._target_home_for_gateway_argv([
            "python",
            "-m",
            "hermes_cli.main",
            "--profile",
            "alpha",
            "gateway",
            "run",
        ])
        == expected / "alpha"
    )


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (lambda payload: payload["config_env_binding"].pop("env_path"), "env_path"),
        (
            lambda payload: payload["config_env_binding"].__setitem__(
                "config_path", "relative/config.yaml"
            ),
            "absolute",
        ),
        (
            lambda payload: payload["config_env_binding"].__setitem__(
                "config_sha256", "0" * 64
            ),
            "SHA-256 mismatch",
        ),
        (
            lambda payload: payload["config_env_binding"].__setitem__(
                "hermes_home", "/different/hermes-home"
            ),
            "hermes_home mismatch",
        ),
        (
            lambda payload: payload["config_env_binding"].__setitem__(
                "release", "different-release"
            ),
            "does not match manifest release",
        ),
        (
            lambda payload: payload.__setitem__("release_identity", "f" * 64),
            "does not bind",
        ),
    ],
)
def test_external_schema_and_identity_mismatches_fail_closed(tmp_path, mutation, match):
    home, _config_path, _env_path, manifest_path = _sealed_home(tmp_path)
    manifest = _load_json(manifest_path)
    mutation(manifest)
    _store_json(manifest_path, manifest)

    with pytest.raises(bindings.ServiceRuntimeBindingError, match=match):
        bindings.resolve_service_runtime_bindings(home)


def test_external_symlink_fails_closed(tmp_path):
    home, config_path, env_path, manifest_path = _sealed_home(tmp_path)
    real_config = config_path.with_name("real-config.yaml")
    config_path.rename(real_config)
    config_path.symlink_to(real_config)
    manifest = _load_json(manifest_path)
    manifest["config_env_binding"]["config_sha256"] = _sha(real_config)
    _store_json(manifest_path, manifest)

    with pytest.raises(bindings.ServiceRuntimeBindingError, match="symlink"):
        bindings.resolve_service_runtime_bindings(home)


def test_external_mode_mismatch_fails_closed(tmp_path):
    home, config_path, _env_path, _manifest = _sealed_home(tmp_path)
    config_path.chmod(0o600)
    with pytest.raises(bindings.ServiceRuntimeBindingError, match="mode must be 0400"):
        bindings.resolve_service_runtime_bindings(home)


def test_windows_accepts_read_only_mode_without_requiring_posix_0400(
    tmp_path, monkeypatch
):
    home, config_path, env_path, _manifest = _sealed_home(tmp_path)
    config_path.chmod(0o444)
    env_path.chmod(0o444)
    monkeypatch.setattr(bindings.sys, "platform", "win32")

    assert bindings.resolve_service_runtime_bindings(home) == {
        "HERMES_CONFIG_PATH": str(config_path),
        "HERMES_ENV_PATH": str(env_path),
    }


def test_windows_rejects_writable_external_file(tmp_path, monkeypatch):
    home, config_path, env_path, _manifest = _sealed_home(tmp_path)
    config_path.chmod(0o644)
    env_path.chmod(0o444)
    monkeypatch.setattr(bindings.sys, "platform", "win32")

    with pytest.raises(bindings.ServiceRuntimeBindingError, match="non-writable"):
        bindings.resolve_service_runtime_bindings(home)


def test_external_hardlink_count_mismatch_fails_closed(tmp_path):
    home, config_path, _env_path, _manifest = _sealed_home(tmp_path)
    os.link(config_path, config_path.with_name("config-hardlink.yaml"))
    with pytest.raises(bindings.ServiceRuntimeBindingError, match="hard link"):
        bindings.resolve_service_runtime_bindings(home)


def test_external_owner_mismatch_fails_closed(tmp_path):
    _home, config_path, _env_path, _manifest = _sealed_home(tmp_path)
    owner_uid = config_path.stat().st_uid
    with pytest.raises(bindings.ServiceRuntimeBindingError, match="owner mismatch"):
        bindings._open_verified_bytes(
            config_path,
            owner_uid=owner_uid + 1,
            required_mode=0o400,
            max_bytes=1024,
            description="external config",
        )


def test_target_home_symlink_is_not_a_trust_root(tmp_path):
    home, _config_path, _env_path, _manifest = _sealed_home(tmp_path)
    linked_home = tmp_path / "linked-home"
    linked_home.symlink_to(home, target_is_directory=True)

    with pytest.raises(
        bindings.ServiceRuntimeBindingError, match="HERMES_HOME.*symlink"
    ):
        bindings.resolve_service_runtime_bindings(linked_home)


def test_target_home_replacement_during_validation_fails_closed(tmp_path, monkeypatch):
    home, _config_path, _env_path, _manifest = _sealed_home(tmp_path)
    original_loader = bindings._load_manifest

    def swap_home(target_home, owner_uid):
        payload = original_loader(target_home, owner_uid)
        moved = target_home.with_name(target_home.name + "-replaced")
        target_home.rename(moved)
        target_home.mkdir()
        return payload

    monkeypatch.setattr(bindings, "_load_manifest", swap_home)
    with pytest.raises(
        bindings.ServiceRuntimeBindingError, match="HERMES_HOME changed"
    ):
        bindings.resolve_service_runtime_bindings(home)


def test_live_manifest_group_world_write_is_rejected(tmp_path):
    home, _config_path, _env_path, manifest_path = _sealed_home(tmp_path)
    manifest_path.chmod(0o620)

    with pytest.raises(
        bindings.ServiceRuntimeBindingError, match="group/world writable"
    ):
        bindings.resolve_service_runtime_bindings(home)


def test_live_manifest_symlink_is_rejected(tmp_path):
    home, _config_path, _env_path, manifest_path = _sealed_home(tmp_path)
    real_manifest = manifest_path.with_name("LIVE_MANIFEST.real.json")
    manifest_path.rename(real_manifest)
    manifest_path.symlink_to(real_manifest)

    with pytest.raises(bindings.ServiceRuntimeBindingError, match="manifest.*symlink"):
        bindings.resolve_service_runtime_bindings(home)


def test_external_config_and_env_must_be_distinct(tmp_path):
    home, config_path, _env_path, manifest_path = _sealed_home(tmp_path)
    manifest = _load_json(manifest_path)
    manifest["config_env_binding"]["env_path"] = str(config_path)
    manifest["config_env_binding"]["env_sha256"] = _sha(config_path)
    _store_json(manifest_path, manifest)

    with pytest.raises(bindings.ServiceRuntimeBindingError, match="must be distinct"):
        bindings.resolve_service_runtime_bindings(home)


def test_external_pair_is_rendered_exactly_in_systemd_and_launchd(
    tmp_path, monkeypatch
):
    home, config_path, env_path, _manifest = _sealed_home(tmp_path, special_names=True)
    _arrange_gateway_generation(monkeypatch, home)

    unit = gateway.generate_systemd_unit()
    systemd_env = _systemd_environment(unit)
    assert systemd_env["HERMES_HOME"] == str(home)
    assert systemd_env["HERMES_CONFIG_PATH"] == str(config_path)
    assert systemd_env["HERMES_ENV_PATH"] == str(env_path)

    monkeypatch.setattr(
        gateway,
        "_system_service_identity",
        lambda run_as_user=None: ("alice", "staff", str(tmp_path / "alice")),
    )
    monkeypatch.setattr(
        gateway, "_hermes_home_for_target_user", lambda _home_dir: str(home)
    )
    monkeypatch.setattr(
        gateway, "_remap_path_for_user", lambda value, _home_dir: str(value)
    )
    system_unit = gateway.generate_systemd_unit(system=True, run_as_user="alice")
    system_env = _systemd_environment(system_unit)
    assert system_env["HERMES_CONFIG_PATH"] == str(config_path)
    assert system_env["HERMES_ENV_PATH"] == str(env_path)

    plist = plistlib.loads(gateway.generate_launchd_plist().encode("utf-8"))
    launchd_env = plist["EnvironmentVariables"]
    assert launchd_env["HERMES_HOME"] == str(home)
    assert launchd_env["HERMES_CONFIG_PATH"] == str(config_path)
    assert launchd_env["HERMES_ENV_PATH"] == str(env_path)
    assert "&amp;" in gateway.generate_launchd_plist()
    assert "&lt;sealed&gt;" in gateway.generate_launchd_plist()


def test_windows_cmd_vbs_and_direct_overlay_carry_exact_external_pair(
    tmp_path, monkeypatch
):
    home, config_path, env_path, _manifest = _sealed_home(tmp_path, special_names=True)
    pair = bindings.resolve_service_runtime_bindings(home)
    monkeypatch.setattr(
        gateway_windows,
        "_resolve_detached_python",
        lambda exe: (exe, Path(r"C:\Hermes Venv"), []),
    )

    cmd = gateway_windows._build_gateway_cmd_script(
        r"C:\Hermes Runtime\pythonw.exe",
        r"C:\Hermes Runtime",
        str(home),
        "",
        pair,
    )
    assert f'set "HERMES_CONFIG_PATH={str(config_path).replace("%", "%%")}"' in cmd
    assert f'set "HERMES_ENV_PATH={str(env_path).replace("%", "%%")}"' in cmd

    vbs = gateway_windows._build_gateway_vbs_script(
        r"C:\Hermes Runtime\pythonw.exe",
        r"C:\Hermes Runtime",
        str(home),
        "",
        pair,
    )
    assert gateway_windows._quote_vbs_string(str(config_path)) in vbs
    assert gateway_windows._quote_vbs_string(str(env_path)) in vbs

    project = tmp_path / "runtime project"
    project.mkdir()
    monkeypatch.setattr(gateway_windows, "_assert_windows", lambda: None)
    monkeypatch.setattr(gateway, "PROJECT_ROOT", project)
    monkeypatch.setattr(gateway, "get_python_path", lambda: r"C:\Hermes\python.exe")
    monkeypatch.setattr(gateway, "_profile_arg", lambda _home: "")
    monkeypatch.setattr("hermes_cli.config.get_hermes_home", lambda: home)
    monkeypatch.setattr(
        gateway_windows,
        "_stable_gateway_working_dir",
        lambda _project: str(home),
    )
    argv, cwd, overlay = gateway_windows._build_gateway_argv()
    assert argv[-2:] == ["gateway", "run"]
    assert cwd == str(home)
    assert overlay["HERMES_CONFIG_PATH"] == str(config_path)
    assert overlay["HERMES_ENV_PATH"] == str(env_path)

    monkeypatch.setattr(gateway_windows.sys, "platform", "win32")
    _argv, _cwd, restart_overlay = gateway_windows.windowless_gateway_restart_spec([
        r"C:\Hermes\python.exe",
        "-m",
        "hermes_cli.main",
        "gateway",
        "run",
    ])
    assert restart_overlay["HERMES_CONFIG_PATH"] == str(config_path)
    assert restart_overlay["HERMES_ENV_PATH"] == str(env_path)


def test_legacy_definitions_do_not_carry_external_values(tmp_path, monkeypatch):
    home, _config_path, _env_path, manifest_path = _sealed_home(tmp_path)
    _store_json(
        manifest_path,
        {
            "runtime_root": str(home / "runtime-root"),
            "runtime_python": str(home / "runtime-python"),
        },
    )
    _arrange_gateway_generation(monkeypatch, home)

    unit = gateway.generate_systemd_unit()
    assert "HERMES_CONFIG_PATH" not in unit
    assert "HERMES_ENV_PATH" not in unit
    plist = plistlib.loads(gateway.generate_launchd_plist().encode("utf-8"))
    assert "HERMES_CONFIG_PATH" not in plist["EnvironmentVariables"]
    assert "HERMES_ENV_PATH" not in plist["EnvironmentVariables"]

    monkeypatch.setattr(
        gateway_windows,
        "_resolve_detached_python",
        lambda exe: (exe, Path(r"C:\Hermes Venv"), []),
    )
    cmd = gateway_windows._build_gateway_cmd_script(
        r"C:\Hermes\pythonw.exe", r"C:\Hermes", str(home), "", {}
    )
    assert 'set "HERMES_CONFIG_PATH="' in cmd
    assert 'set "HERMES_ENV_PATH="' in cmd
    assert f"HERMES_CONFIG_PATH={tmp_path}" not in cmd


def test_clean_management_shell_is_current_for_external_definitions(
    tmp_path, monkeypatch
):
    home, _config_path, _env_path, _manifest = _sealed_home(tmp_path)
    _arrange_gateway_generation(monkeypatch, home)
    unit_path = tmp_path / "hermes-gateway.service"
    plist_path = tmp_path / "ai.hermes.gateway.plist"
    monkeypatch.setattr(
        gateway, "get_systemd_unit_path", lambda system=False: unit_path
    )
    monkeypatch.setattr(gateway, "get_launchd_plist_path", lambda: plist_path)
    unit_path.write_text(gateway.generate_systemd_unit(), encoding="utf-8")
    plist_path.write_text(gateway.generate_launchd_plist(), encoding="utf-8")
    monkeypatch.delenv("HERMES_CONFIG_PATH", raising=False)
    monkeypatch.delenv("HERMES_ENV_PATH", raising=False)

    assert gateway.systemd_unit_is_current() is True
    assert gateway.launchd_plist_is_current() is True


@pytest.mark.parametrize("kind", ["systemd", "launchd"])
def test_invalid_external_refresh_preserves_installed_bytes(
    tmp_path, monkeypatch, kind
):
    home, _config_path, _env_path, manifest_path = _sealed_home(tmp_path)
    manifest = _load_json(manifest_path)
    manifest["config_env_binding"]["env_sha256"] = "0" * 64
    _store_json(manifest_path, manifest)
    installed_path = tmp_path / (
        "gateway.service" if kind == "systemd" else "gateway.plist"
    )
    before = b"sealed installed definition\n"
    installed_path.write_bytes(before)

    if kind == "systemd":
        monkeypatch.setattr(
            gateway, "get_systemd_unit_path", lambda system=False: installed_path
        )
        monkeypatch.setattr(
            gateway,
            "generate_systemd_unit",
            lambda system=False, run_as_user=None: (
                bindings.resolve_service_runtime_bindings(home) and "replacement"
            ),
        )
        action = gateway.refresh_systemd_unit_if_needed
    else:
        monkeypatch.setattr(gateway, "get_launchd_plist_path", lambda: installed_path)
        monkeypatch.setattr(
            gateway,
            "generate_launchd_plist",
            lambda: bindings.resolve_service_runtime_bindings(home) and "replacement",
        )
        action = gateway.refresh_launchd_plist_if_needed

    with pytest.raises(bindings.ServiceRuntimeBindingError):
        action()
    assert installed_path.read_bytes() == before


def test_missing_systemd_definition_rebuilds_from_valid_external_manifest(
    tmp_path, monkeypatch
):
    home, config_path, env_path, _manifest = _sealed_home(tmp_path)
    unit_path = tmp_path / "systemd" / "hermes-gateway.service"
    monkeypatch.setattr(
        gateway, "get_systemd_unit_path", lambda system=False: unit_path
    )
    monkeypatch.setattr(
        gateway, "_systemd_target_home", lambda *args, **kwargs: (str(home), None)
    )
    monkeypatch.setattr(
        gateway,
        "generate_systemd_unit",
        lambda system=False, run_as_user=None, **_kwargs: "\n".join(
            f"{key}={value}"
            for key, value in bindings.resolve_service_runtime_bindings(home).items()
        ),
    )
    monkeypatch.setattr(gateway, "_run_systemctl", lambda *args, **kwargs: None)

    assert gateway._rebuild_missing_systemd_unit_from_external_manifest(False) is True
    content = unit_path.read_text(encoding="utf-8")
    assert f"HERMES_CONFIG_PATH={config_path}" in content
    assert f"HERMES_ENV_PATH={env_path}" in content


def test_missing_launchd_definition_rebuilds_exact_external_pair(tmp_path, monkeypatch):
    home, config_path, env_path, _manifest = _sealed_home(tmp_path)
    plist_path = tmp_path / "LaunchAgents" / "ai.hermes.gateway.plist"
    monkeypatch.setattr(gateway, "get_launchd_plist_path", lambda: plist_path)
    monkeypatch.setattr(gateway, "get_hermes_home", lambda: home)
    monkeypatch.setattr(
        gateway,
        "generate_launchd_plist",
        lambda: json.dumps(bindings.resolve_service_runtime_bindings(home)),
    )

    assert gateway._ensure_launchd_plist_present() is True
    content = plist_path.read_text(encoding="utf-8")
    assert str(config_path) in content
    assert str(env_path) in content


def test_systemd_reader_and_sync_preserve_full_binding_triple_with_spaces(
    tmp_path, monkeypatch
):
    home = tmp_path / "Hermes Home"
    config_path = tmp_path / "Candidate Config" / "config.yaml"
    env_path = tmp_path / "Candidate Config" / ".env"
    output = (
        'Environment="HERMES_HOME='
        + str(home)
        + '" "HERMES_CONFIG_PATH='
        + str(config_path)
        + '" "HERMES_ENV_PATH='
        + str(env_path)
        + '"\n'
    )
    monkeypatch.setattr(gateway, "_select_systemd_scope", lambda system=False: system)
    monkeypatch.setattr(
        gateway,
        "_run_systemctl",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=output),
    )
    monkeypatch.setenv("HERMES_HOME", "/wrong")
    monkeypatch.setenv("HERMES_CONFIG_PATH", "/wrong-config")
    monkeypatch.setenv("HERMES_ENV_PATH", "/wrong-env")

    parsed = gateway._read_systemd_unit_environment(system=True)
    assert parsed == {
        "HERMES_HOME": str(home),
        "HERMES_CONFIG_PATH": str(config_path),
        "HERMES_ENV_PATH": str(env_path),
    }
    gateway._sync_hermes_home_from_systemd_unit(system=True)
    assert os.environ["HERMES_HOME"] == str(home)
    assert os.environ["HERMES_CONFIG_PATH"] == str(config_path)
    assert os.environ["HERMES_ENV_PATH"] == str(env_path)


def test_system_scope_operation_preserves_custom_home_pinned_by_installed_unit(
    tmp_path, monkeypatch
):
    custom_home, config_path, env_path, _manifest = _sealed_home(tmp_path)
    unit_path = tmp_path / "etc" / "hermes-gateway.service"
    unit_path.parent.mkdir()
    unit_path.write_text(
        "[Service]\n"
        "User=alice\n"
        + gateway._systemd_environment_line("HERMES_HOME", str(custom_home))
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        gateway, "get_systemd_unit_path", lambda system=False: unit_path
    )
    monkeypatch.setattr(
        gateway,
        "_system_service_identity",
        lambda run_as_user=None: ("alice", "staff", str(tmp_path / "alice")),
    )
    _arrange_gateway_generation(monkeypatch, custom_home)

    target_home, target_user = gateway._systemd_target_home_for_operation(True)
    assert target_home == str(custom_home)
    assert target_user == "alice"
    assert bindings.resolve_service_runtime_bindings(target_home) == {
        "HERMES_CONFIG_PATH": str(config_path),
        "HERMES_ENV_PATH": str(env_path),
    }
    generated = gateway.generate_systemd_unit(
        system=True,
        run_as_user="alice",
        target_hermes_home=target_home,
    )
    generated_env = _systemd_environment(generated)
    assert generated_env["HERMES_HOME"] == str(custom_home)
    assert generated_env["HERMES_CONFIG_PATH"] == str(config_path)
    assert generated_env["HERMES_ENV_PATH"] == str(env_path)


def test_systemd_sync_drops_pair_for_named_profile(tmp_path, monkeypatch):
    profile_home = tmp_path / "root" / "profiles" / "worker"
    output = (
        f'Environment="HERMES_HOME={profile_home}" '
        '"HERMES_CONFIG_PATH=/default/config.yaml" '
        '"HERMES_ENV_PATH=/default/.env"\n'
    )
    monkeypatch.setattr(gateway, "_select_systemd_scope", lambda system=False: system)
    monkeypatch.setattr(
        gateway,
        "_run_systemctl",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=output),
    )
    monkeypatch.setenv("HERMES_CONFIG_PATH", "/ambient/config")
    monkeypatch.setenv("HERMES_ENV_PATH", "/ambient/env")

    gateway._sync_hermes_home_from_systemd_unit(system=True)
    assert "HERMES_CONFIG_PATH" not in os.environ
    assert "HERMES_ENV_PATH" not in os.environ


def test_windows_restart_validates_before_stopping(tmp_path, monkeypatch):
    home, _config_path, _env_path, manifest_path = _sealed_home(tmp_path)
    manifest = _load_json(manifest_path)
    manifest["config_env_binding"]["config_sha256"] = "0" * 64
    _store_json(manifest_path, manifest)
    monkeypatch.setattr(gateway_windows, "_assert_windows", lambda: None)
    monkeypatch.setattr("hermes_cli.config.get_hermes_home", lambda: home)
    stopped: list[bool] = []
    monkeypatch.setattr(gateway_windows, "stop", lambda: stopped.append(True))

    with pytest.raises(bindings.ServiceRuntimeBindingError):
        gateway_windows.restart()
    assert stopped == []


def test_systemd_start_invalid_external_fails_before_preflight_or_start(
    tmp_path, monkeypatch
):
    home, _config_path, _env_path, manifest_path = _sealed_home(tmp_path)
    manifest = _load_json(manifest_path)
    manifest["config_env_binding"]["config_sha256"] = "0" * 64
    _store_json(manifest_path, manifest)
    calls: list[str] = []
    monkeypatch.setattr(gateway, "_select_systemd_scope", lambda system=False: False)
    monkeypatch.setattr(
        gateway, "_systemd_target_home", lambda *args, **kwargs: (str(home), None)
    )
    monkeypatch.setattr(
        gateway, "_preflight_user_systemd", lambda: calls.append("preflight")
    )
    monkeypatch.setattr(
        gateway, "_run_systemctl", lambda *args, **kwargs: calls.append("systemctl")
    )

    with pytest.raises(bindings.ServiceRuntimeBindingError):
        gateway.systemd_start()
    assert calls == []


def test_launchd_restart_invalid_external_never_invokes_launchctl(
    tmp_path, monkeypatch
):
    home, _config_path, _env_path, manifest_path = _sealed_home(tmp_path)
    manifest = _load_json(manifest_path)
    manifest["config_env_binding"]["env_sha256"] = "0" * 64
    _store_json(manifest_path, manifest)
    plist_path = tmp_path / "installed.plist"
    plist_path.write_text("sealed old plist", encoding="utf-8")
    calls: list[object] = []
    monkeypatch.setattr(gateway, "get_launchd_plist_path", lambda: plist_path)
    monkeypatch.setattr(gateway, "get_hermes_home", lambda: home)
    monkeypatch.setattr(
        gateway,
        "generate_launchd_plist",
        lambda: bindings.resolve_service_runtime_bindings(home) and "replacement",
    )
    monkeypatch.setattr(
        gateway.subprocess, "run", lambda *args, **kwargs: calls.append(args)
    )

    with pytest.raises(bindings.ServiceRuntimeBindingError):
        gateway.launchd_restart()
    assert calls == []
    assert plist_path.read_text(encoding="utf-8") == "sealed old plist"


def test_stage_b_schema_constant_lists_every_required_binding_field():
    assert bindings.STAGE_B_EXTERNAL_BINDING_REQUIRED_FIELDS == {
        "manifest": (
            "config_env_binding_mode",
            "release",
            "tag",
            "commit",
            "release_identity",
            "config_env_binding",
        ),
        "config_env_binding": (
            "hermes_home",
            "release",
            "tag",
            "commit",
            "release_identity",
            "config_path",
            "config_sha256",
            "env_path",
            "env_sha256",
        ),
    }


def _dashboard_args() -> SimpleNamespace:
    return SimpleNamespace(
        status=False,
        stop=False,
        host="127.0.0.1",
        port=9119,
        no_open=True,
        insecure=False,
        skip_build=False,
        isolated=False,
        open_profile="",
        headless_backend=False,
    )


def test_named_dashboard_reexec_restores_default_external_pair(tmp_path, monkeypatch):
    import hermes_cli.main as main
    import hermes_constants

    root, config_path, env_path, _manifest = _sealed_home(tmp_path)
    profile_home = root / "profiles" / "worker"
    profile_home.mkdir(parents=True)
    monkeypatch.setenv("HERMES_HOME", str(profile_home))
    monkeypatch.setenv("HERMES_CONFIG_PATH", "/ambient/wrong-config")
    monkeypatch.setenv("HERMES_ENV_PATH", "/ambient/wrong-env")
    monkeypatch.setattr("hermes_cli.profiles.get_active_profile_name", lambda: "worker")
    monkeypatch.setattr(main, "_dashboard_listening", lambda host, port: False)
    monkeypatch.setattr(hermes_constants, "get_default_hermes_root", lambda: root)
    captured: list[dict[str, str]] = []

    def fake_exec(_exe, _argv, child_env):
        captured.append(child_env)
        raise SystemExit(0)

    monkeypatch.setattr(main.os, "execvpe", fake_exec)
    with pytest.raises(SystemExit):
        main.cmd_dashboard(_dashboard_args())

    assert captured[0]["HERMES_HOME"] == str(root)
    assert captured[0]["HERMES_CONFIG_PATH"] == str(config_path)
    assert captured[0]["HERMES_ENV_PATH"] == str(env_path)


def test_named_dashboard_reexec_invalid_external_manifest_never_execs(
    tmp_path, monkeypatch
):
    import hermes_cli.main as main
    import hermes_constants

    root, _config_path, _env_path, manifest_path = _sealed_home(tmp_path)
    profile_home = root / "profiles" / "worker"
    profile_home.mkdir(parents=True)
    manifest = _load_json(manifest_path)
    manifest["config_env_binding"]["env_sha256"] = "0" * 64
    _store_json(manifest_path, manifest)
    monkeypatch.setenv("HERMES_HOME", str(profile_home))
    monkeypatch.setattr("hermes_cli.profiles.get_active_profile_name", lambda: "worker")
    monkeypatch.setattr(main, "_dashboard_listening", lambda host, port: False)
    monkeypatch.setattr(hermes_constants, "get_default_hermes_root", lambda: root)
    execs: list[object] = []
    monkeypatch.setattr(main.os, "execvpe", lambda *args: execs.append(args))

    with pytest.raises(bindings.ServiceRuntimeBindingError):
        main.cmd_dashboard(_dashboard_args())
    assert execs == []
