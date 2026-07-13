from __future__ import annotations

import json
import os
import plistlib
from pathlib import Path

import pytest

from scripts import pnc_aux_runtime_health as health


def _identity_fixture(tmp_path: Path):
    script = tmp_path / "service.py"
    executable = tmp_path / "python"
    plist = tmp_path / "service.plist"
    script.write_text("print('service')\n", encoding="utf-8")
    executable.write_bytes(b"fixture-python\n")
    executable.chmod(0o755)
    plist.write_bytes(
        plistlib.dumps(
            {
                "Label": "local.pnc.fixture",
                "ProgramArguments": [str(executable), str(script)],
                "EnvironmentVariables": {"PYTHONNOUSERSITE": "1"},
            },
            fmt=plistlib.FMT_XML,
            sort_keys=True,
        )
    )
    return script, executable, plist


def test_runtime_identity_binds_script_interpreter_plist_and_process(tmp_path: Path):
    script, executable, plist = _identity_fixture(tmp_path)

    evidence = health.build_process_runtime_evidence(
        service_label="local.pnc.fixture",
        script_path=script,
        executable=executable,
        plist_path=plist,
        cwd=tmp_path,
    )

    assert evidence["pid"] == os.getpid()
    assert evidence["process_create_time"] > 0
    assert evidence["runtime_identity"]["script"] == str(script)
    assert evidence["runtime_identity"]["executable"] == str(executable)
    assert evidence["runtime_identity"]["plist_path"] == str(plist)
    assert set(evidence["runtime_identity"]) == {
        "executable",
        "script",
        "cwd",
        "script_sha256",
        "interpreter_sha256",
        "plist_path",
        "plist_sha256",
        "program_arguments_sha256",
        "environment_sha256",
    }


def test_owner_health_is_atomic_owner_only_and_repeatable(tmp_path: Path):
    parent = tmp_path / "health"
    parent.mkdir(mode=0o700)
    path = parent / "health.json"

    health.write_owner_health(path, {"ok": True, "generation": 1})
    health.write_owner_health(path, {"ok": True, "generation": 2})

    assert path.stat().st_mode & 0o777 == 0o600
    assert path.stat().st_nlink == 1
    assert json.loads(path.read_text(encoding="utf-8"))["generation"] == 2


@pytest.mark.parametrize("kind", ["symlink", "hardlink", "permissions"])
def test_owner_health_rejects_unsafe_existing_target(tmp_path: Path, kind: str):
    parent = tmp_path / "health"
    parent.mkdir(mode=0o700)
    path = parent / "health.json"
    other = parent / "other.json"
    other.write_text("{}", encoding="utf-8")
    other.chmod(0o600)
    if kind == "symlink":
        path.symlink_to(other)
    elif kind == "hardlink":
        os.link(other, path)
    else:
        path.write_text("{}", encoding="utf-8")
        path.chmod(0o644)

    with pytest.raises(health.AuxiliaryHealthError) as error:
        health.write_owner_health(path, {"ok": True})

    assert error.value.code == "aux_health_existing_file_invalid"


def test_runtime_identity_rejects_symlinked_plist(tmp_path: Path):
    script, executable, plist = _identity_fixture(tmp_path)
    link = tmp_path / "linked.plist"
    link.symlink_to(plist)

    with pytest.raises(health.AuxiliaryHealthError) as error:
        health.build_process_runtime_evidence(
            service_label="local.pnc.fixture",
            script_path=script,
            executable=executable,
            plist_path=link,
            cwd=tmp_path,
        )

    assert error.value.code == "aux_runtime_identity_file_invalid"
