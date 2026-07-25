from __future__ import annotations

import json
import os
import plistlib
import subprocess
import sys
from pathlib import Path

import pytest

from scripts import hermes_live_drift_guard as drift_guard


REPO_ROOT = Path(__file__).resolve().parents[2]
LAUNCHER = REPO_ROOT / "scripts" / "pnc_live_exec.py"
SERVICE_LABEL = "local.pnc.vm-task-sync"


def _git(root: Path, *args: str) -> str:
    return subprocess.check_output(
        ["/usr/bin/git", "-C", str(root), *args], text=True
    ).strip()


def _create_runtime(home: Path, name: str) -> tuple[Path, Path, str, str]:
    root = home / "runtime" / "releases" / name
    script = root / "scripts" / "pnc_vm_task_sync.py"
    script.parent.mkdir(parents=True)
    script.write_text(
        """import json, os, sys
print(json.dumps({
    'args': sys.argv[1:],
    'cwd': os.getcwd(),
    'manifest_sha256': os.environ.get('PNC_LIVE_MANIFEST_SHA256'),
    'runtime_commit': os.environ.get('PNC_LIVE_RUNTIME_COMMIT'),
    'service_label': os.environ.get('PNC_LIVE_SERVICE_LABEL'),
    'pythonpath': os.environ.get('PYTHONPATH'),
}))
""",
        encoding="utf-8",
    )
    gateway = root / "hermes_cli" / "main.py"
    gateway.parent.mkdir(parents=True)
    gateway.write_text("print('gateway fixture')\n", encoding="utf-8")
    subprocess.run(["/usr/bin/git", "init", "-q", str(root)], check=True)
    _git(root, "config", "user.name", "PNC launcher test")
    _git(root, "config", "user.email", "pnc-launcher-test@example.invalid")
    _git(root, "add", "scripts/pnc_vm_task_sync.py", "hermes_cli/main.py")
    _git(root, "commit", "-q", "-m", f"fixture {name}")
    commit = _git(root, "rev-parse", "HEAD")
    tree = _git(root, "rev-parse", "HEAD^{tree}")

    venv = home / "runtime" / "venvs" / f"{name}-sealed"
    (venv / "bin").mkdir(parents=True)
    (venv / "bin" / "python").symlink_to(sys.executable)
    return root, venv, commit, tree


def _write_manifest(home: Path, root: Path, venv: Path, commit: str, tree: str) -> Path:
    manifest = home / "runtime" / "LIVE_MANIFEST.json"
    manifest.write_text(
        json.dumps({
            "runtime_root": str(root),
            "runtime_venv": str(venv),
            "runtime_python": str(venv / "bin" / "python"),
            "promotion_source_head": commit,
            "face_git_bindings": {
                "runtime_engine": {
                    "commit": commit,
                    "tree": tree,
                    "repo": str(root),
                }
            },
        }),
        encoding="utf-8",
    )
    manifest.chmod(0o600)
    return manifest


def _run(home: Path, *args: str, extra_env: dict[str, str] | None = None):
    environment = dict(os.environ)
    environment["HERMES_HOME"] = str(home)
    environment.update(extra_env or {})
    return subprocess.run(
        [sys.executable, str(LAUNCHER), *args],
        capture_output=True,
        text=True,
        env=environment,
        timeout=20,
    )


def test_check_and_exec_use_the_manifest_bound_release(tmp_path: Path):
    home = tmp_path / "hermes"
    root, venv, commit, tree = _create_runtime(home, "active")
    _write_manifest(home, root, venv, commit, tree)

    checked = _run(home, "--check", SERVICE_LABEL)
    assert checked.returncode == 0, checked.stderr
    evidence = json.loads(checked.stdout)
    assert evidence["ok"] is True
    assert evidence["runtime_root"] == str(root)
    assert evidence["runtime_commit"] == commit
    assert evidence["script"] == str(root / "scripts" / "pnc_vm_task_sync.py")

    executed = _run(
        home,
        SERVICE_LABEL,
        "--probe",
        "value",
        extra_env={"PYTHONPATH": "/stale/import/root"},
    )
    assert executed.returncode == 0, executed.stderr
    result = json.loads(executed.stdout)
    assert result == {
        "args": ["--probe", "value"],
        "cwd": str(root),
        "manifest_sha256": evidence["manifest_sha256"],
        "pythonpath": str(root),
        "runtime_commit": commit,
        "service_label": SERVICE_LABEL,
    }


def test_stale_runtime_commit_fails_closed_with_nonzero_exit(tmp_path: Path):
    home = tmp_path / "hermes"
    active_root, active_venv, active_commit, active_tree = _create_runtime(
        home, "active"
    )
    manifest = _write_manifest(
        home, active_root, active_venv, active_commit, active_tree
    )
    stale_root, stale_venv, _stale_commit, _stale_tree = _create_runtime(home, "stale")
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    payload.update({
        "runtime_root": str(stale_root),
        "runtime_venv": str(stale_venv),
        "runtime_python": str(stale_venv / "bin" / "python"),
    })
    payload["face_git_bindings"]["runtime_engine"]["repo"] = str(stale_root)
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    manifest.chmod(0o600)

    result = _run(home, "--check", SERVICE_LABEL)
    assert result.returncode != 0
    assert json.loads(result.stderr) == {
        "error": "active_runtime_commit_mismatch",
        "ok": False,
    }


def test_stable_governance_and_native_cli_targets_are_manifest_bound(tmp_path: Path):
    home = tmp_path / "hermes"
    root, venv, commit, tree = _create_runtime(home, "active")
    _write_manifest(home, root, venv, commit, tree)
    governance = home / "runtime" / "governance-tools" / "hermes_governance_check.py"
    governance.parent.mkdir(parents=True)
    governance.write_text("print('ok')\n", encoding="utf-8")
    native = venv / "bin" / "hermes"
    native.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    native.chmod(0o755)

    governance_result = _run(home, "--check", "local.pnc.governance-check")
    assert governance_result.returncode == 0, governance_result.stderr
    governance_evidence = json.loads(governance_result.stdout)
    assert governance_evidence["target_kind"] == "governance_tool"
    assert governance_evidence["script"] == str(governance)

    cli_result = _run(home, "--check", "local.pnc.hermes-cli")
    assert cli_result.returncode == 0, cli_result.stderr
    cli_evidence = json.loads(cli_result.stdout)
    assert cli_evidence["target_kind"] == "runtime_executable"
    assert cli_evidence["script"] == str(native)

    gateway_result = _run(home, "--check", "ai.hermes.gateway")
    assert gateway_result.returncode == 0, gateway_result.stderr
    gateway_evidence = json.loads(gateway_result.stdout)
    assert gateway_evidence["target_kind"] == "runtime_script"
    assert gateway_evidence["script"] == str(root / "hermes_cli/main.py")


def _dynamic_plist(home: Path) -> dict[str, object]:
    launcher = home / ".hermes" / "runtime" / "governance-tools" / "pnc_live_exec.py"
    return {
        "Label": SERVICE_LABEL,
        "ProgramArguments": [
            "/usr/bin/python3",
            str(launcher),
            SERVICE_LABEL,
            "--include-terminal",
        ],
        "WorkingDirectory": str(home / ".hermes" / "runtime"),
        "EnvironmentVariables": {"HERMES_HOME": str(home / ".hermes")},
    }


def _write_test_plist(home: Path, body: dict[str, object]) -> Path:
    path = home / "Library" / "LaunchAgents" / f"{SERVICE_LABEL}.plist"
    path.parent.mkdir(parents=True)
    path.write_bytes(plistlib.dumps(body))
    return path


def test_freshness_check_rejects_stale_persisted_definition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    body = _dynamic_plist(tmp_path)
    body["ProgramArguments"] = [
        "/Users/songying/.hermes/runtime/venvs/hermes-old/bin/python",
        "/Users/songying/.hermes/runtime/releases/hermes-old/scripts/pnc_vm_task_sync.py",
        "--include-terminal",
    ]
    body["EnvironmentVariables"] = {"VIRTUAL_ENV": "/runtime/venvs/hermes-old"}
    _write_test_plist(tmp_path, body)
    launcher = tmp_path / ".hermes/runtime/governance-tools/pnc_live_exec.py"
    monkeypatch.setattr(drift_guard.Path, "home", staticmethod(lambda: tmp_path))
    monkeypatch.setattr(
        drift_guard,
        "read_launchd_runtime",
        lambda _label: {
            "found": "true",
            "raw": f"{launcher} {SERVICE_LABEL} --include-terminal",
        },
    )

    result = drift_guard.validate_pnc_vm_task_sync_launchd()
    assert result["ok"] is False
    assert any("persisted definition" in error for error in result["errors"])


def test_freshness_check_rejects_stale_loaded_definition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    _write_test_plist(tmp_path, _dynamic_plist(tmp_path))
    monkeypatch.setattr(drift_guard.Path, "home", staticmethod(lambda: tmp_path))
    monkeypatch.setattr(
        drift_guard,
        "read_launchd_runtime",
        lambda _label: {
            "found": "true",
            "raw": (
                "/Users/songying/.hermes/runtime/releases/hermes-old/"
                "scripts/pnc_vm_task_sync.py --include-terminal"
            ),
        },
    )

    result = drift_guard.validate_pnc_vm_task_sync_launchd()
    assert result["ok"] is False
    assert any("loaded definition" in error for error in result["errors"])


@pytest.mark.parametrize(
    ("name", "flags"),
    [
        (
            "local.pnc.feishu-delivery-repair.plist",
            ["--repair", "--json", "--no-backup"],
        ),
        (
            "local.pnc.vm-task-sync.plist",
            ["--limit", "50", "--include-terminal", "--json"],
        ),
    ],
)
def test_write_capable_timer_plists_resolve_the_active_manifest(
    name: str, flags: list[str]
):
    raw = (REPO_ROOT / name).read_bytes()
    body = plistlib.loads(raw)
    label = name.removesuffix(".plist")
    assert body["ProgramArguments"] == [
        "/usr/bin/python3",
        "/Users/songying/.hermes/runtime/governance-tools/pnc_live_exec.py",
        label,
        *flags,
    ]
    assert body["WorkingDirectory"] == "/Users/songying/.hermes/runtime"
    assert "VIRTUAL_ENV" not in body["EnvironmentVariables"]
    text = raw.decode("utf-8")
    assert "/runtime/releases/" not in text
    assert "/runtime/venvs/" not in text
    assert "/runtime/hermes-live" not in text
