from __future__ import annotations

import json
import os
import plistlib
import subprocess
import sys
import time
from pathlib import Path

from scripts import pnc_rca_release_freshness_gate as gate
from scripts.pnc_live_exec import PNC_PYTHON_LAUNCHD_LABELS


REPO_ROOT = Path(__file__).resolve().parents[2]


def _write_plist(home: Path, label: str, *, stale: bool = False) -> Path:
    runtime = home / ".hermes" / "runtime"
    launcher = runtime / "governance-tools" / "pnc_live_exec.py"
    if label == gate.WATCHDOG_LABEL:
        arguments = [
            "/bin/zsh",
            str(runtime / "governance-tools" / "watcher-staleness-watchdog.sh"),
        ]
        environment = {"HERMES_HOME": str(home / ".hermes")}
        working_directory = str(runtime)
    elif stale:
        arguments = [
            str(home / ".hermes/runtime/venvs/hermes-old/bin/python"),
            str(home / ".hermes/runtime/releases/hermes-old/scripts/old.py"),
            label,
        ]
        environment = {"VIRTUAL_ENV": str(home / ".hermes/runtime/venvs/hermes-old")}
        working_directory = str(home / ".hermes/runtime/releases/hermes-old")
    else:
        arguments = ["/usr/bin/python3", str(launcher), label]
        environment = {"HERMES_HOME": str(home / ".hermes")}
        working_directory = str(runtime)
    body = {
        "Label": label,
        "ProgramArguments": arguments,
        "WorkingDirectory": working_directory,
        "EnvironmentVariables": environment,
    }
    path = home / "Library" / "LaunchAgents" / f"{label}.plist"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(plistlib.dumps(body))
    return path


def test_persisted_gate_rejects_a_pinned_definition(tmp_path: Path):
    for label in PNC_PYTHON_LAUNCHD_LABELS:
        _write_plist(tmp_path, label, stale=label == PNC_PYTHON_LAUNCHD_LABELS[0])
    _write_plist(tmp_path, gate.WATCHDOG_LABEL)

    _evidence, errors = gate.audit_persisted_definitions(
        home=tmp_path,
        hermes_home=tmp_path / ".hermes",
    )

    codes = {str(item["code"]) for item in errors}
    assert "pnc_release_plist_bypasses_active_manifest" in codes
    assert "pnc_release_plist_virtualenv_pinned" in codes
    assert "pnc_release_plist_runtime_pinned" in codes


def test_source_registry_has_no_pinned_plist_or_wrapper():
    plist_labels = set()
    plist_paths = [
        *REPO_ROOT.glob("local.pnc.*.plist"),
        REPO_ROOT / "ai.hermes.gateway.plist",
    ]
    for path in plist_paths:
        payload = plistlib.loads(path.read_bytes())
        plist_labels.add(payload["Label"])
        text = path.read_text(encoding="utf-8")
        assert not any(marker in text for marker in gate.FORBIDDEN_RUNTIME_MARKERS)
    assert plist_labels == set(gate.EXPECTED_LAUNCHD_LABELS)
    gateway = plistlib.loads((REPO_ROOT / "ai.hermes.gateway.plist").read_bytes())
    assert gateway["ProgramArguments"] == [
        "/usr/bin/python3",
        "/Users/songying/.hermes/runtime/governance-tools/pnc_live_exec.py",
        "ai.hermes.gateway",
        "gateway",
        "run",
        "--replace",
    ]
    assert "VIRTUAL_ENV" not in gateway["EnvironmentVariables"]

    wrapper_names = {
        "hermes-g1q3-e2e-smoke",
        "hermes-governance-check",
        "hermes-live",
        "hermes-live-drift-guard",
        "hermes-provider-failure-audit",
        "hermes-release-fingerprint-check",
        "hermes-safe-worktree-remove",
        "hermes-worktree-hygiene",
        "hermes.current",
    }
    assert {
        path.name for path in (REPO_ROOT / "scripts" / "wrappers").iterdir()
    } == wrapper_names
    for name in wrapper_names:
        text = (REPO_ROOT / "scripts" / "wrappers" / name).read_text(encoding="utf-8")
        assert not any(marker in text for marker in gate.FORBIDDEN_RUNTIME_MARKERS)


def test_loaded_gate_rejects_a_stale_launchd_snapshot(tmp_path: Path):
    launcher = tmp_path / ".hermes/runtime/governance-tools/pnc_live_exec.py"
    stale = "/Users/songying/.hermes/runtime/releases/hermes-old/scripts/old.py"

    def reader(label: str) -> subprocess.CompletedProcess[str]:
        raw = f"program = /usr/bin/python3\n{launcher} {label}\n"
        if label == PNC_PYTHON_LAUNCHD_LABELS[0]:
            raw = f"program = /usr/bin/python3\n{stale}\n"
        return subprocess.CompletedProcess(["launchctl"], 0, raw, "")

    _evidence, errors = gate.audit_loaded_definitions(
        hermes_home=tmp_path / ".hermes",
        launchctl_reader=reader,
    )

    assert any(
        item["code"] == "pnc_release_loaded_definition_bypasses_manifest"
        and item["label"] == PNC_PYTHON_LAUNCHD_LABELS[0]
        for item in errors
    )


def test_versioned_stable_entrypoints_reject_stale_launcher_and_wrapper(
    tmp_path: Path,
):
    home = tmp_path
    hermes_home = home / ".hermes"
    runtime_root = hermes_home / "runtime" / "releases" / "active"
    scripts = runtime_root / "scripts"
    wrappers = scripts / "wrappers"
    governance = hermes_home / "runtime" / "governance-tools"
    wrappers.mkdir(parents=True)
    governance.mkdir(parents=True)
    (home / "bin").mkdir()

    versioned = {
        scripts / "pnc_live_exec.py": b"active launcher\n",
        scripts / "pnc_rca_release_freshness_gate.py": b"active gate\n",
        scripts / "watcher_staleness_watchdog.sh": b"active watchdog\n",
        wrappers / "hermes.current": b"active wrapper\n",
    }
    for path, raw in versioned.items():
        path.write_bytes(raw)
    (governance / "pnc_live_exec.py").write_bytes(b"stale launcher\n")
    (governance / "pnc_rca_release_freshness_gate.py").write_bytes(
        versioned[scripts / "pnc_rca_release_freshness_gate.py"]
    )
    (governance / "watcher-staleness-watchdog.sh").write_bytes(
        versioned[scripts / "watcher_staleness_watchdog.sh"]
    )
    (home / "bin" / "hermes.current").write_bytes(b"stale wrapper\n")

    evidence, errors = gate.audit_versioned_stable_entrypoints(
        home=home,
        hermes_home=hermes_home,
        runtime_root=runtime_root,
    )

    stale_names = {
        item["name"]
        for item in errors
        if item["code"] == "pnc_release_stable_entrypoint_stale"
    }
    assert stale_names == {"pnc_live_exec.py", "wrapper:hermes.current"}
    assert {item["name"] for item in evidence if item["exact"]} == {
        "pnc_rca_release_freshness_gate.py",
        "watcher-staleness-watchdog.sh",
    }


def test_process_evidence_reads_real_process_identity(tmp_path: Path):
    script = tmp_path / "resident.py"
    script.write_text("import time; time.sleep(30)\n", encoding="utf-8")
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    env = dict(os.environ)
    env.update({
        "PNC_LIVE_RUNTIME_COMMIT": "a" * 40,
        "PNC_LIVE_SERVICE_LABEL": "local.pnc.fixture",
        "VIRTUAL_ENV": str(tmp_path / "venv"),
    })
    process = subprocess.Popen(
        [sys.executable, str(script)],
        cwd=runtime,
        env=env,
    )
    try:
        time.sleep(0.15)
        evidence, errors = gate._process_evidence(
            label="local.pnc.fixture",
            pid=process.pid,
            resolved={
                "runtime_root": str(runtime),
                "script": str(script),
                "runtime_commit": "a" * 40,
                "runtime_venv": str(tmp_path / "venv"),
            },
        )
    finally:
        process.terminate()
        process.wait(timeout=5)
    assert evidence["pid"] == process.pid
    assert errors == []


def test_cli_returns_nonzero_for_stale_persisted_definition(tmp_path: Path):
    home = tmp_path
    for label in PNC_PYTHON_LAUNCHD_LABELS:
        _write_plist(home, label, stale=label == PNC_PYTHON_LAUNCHD_LABELS[0])
    _write_plist(home, gate.WATCHDOG_LABEL)
    hermes_home = home / ".hermes"
    (hermes_home / "runtime" / "governance-tools").mkdir(parents=True)
    manifest = hermes_home / "runtime" / "LIVE_MANIFEST.json"
    manifest.write_text(json.dumps({}), encoding="utf-8")
    manifest.chmod(0o600)
    result = subprocess.run(
        [sys.executable, str(Path(gate.__file__)), "--json"],
        env={**os.environ, "HOME": str(home), "HERMES_HOME": str(hermes_home)},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is False
    assert any(
        item["code"] == "pnc_release_plist_bypasses_active_manifest"
        for item in payload["errors"]
    )
