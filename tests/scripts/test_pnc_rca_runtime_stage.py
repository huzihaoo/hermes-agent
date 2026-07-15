from __future__ import annotations

import hashlib
import json
import os
import plistlib
import subprocess
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import pnc_rca_runtime_stage as stage


RUNTIME_FILES = {
    "gateway/pnc_rca_runtime_identity.py": (
        "RCA_RUNTIME_RELATIVE_FILES = (\n"
        "    'gateway/pnc_rca_runtime_identity.py',\n"
        "    'scripts/pnc_rca_outbox_dispatcher.py',\n"
        ")\n"
        "GATEWAY_RCA_RUNTIME_RELATIVE_FILES = (\n"
        "    'gateway/run.py',\n"
        "    'hermes_constants.py',\n"
        ")\n"
    ),
    "gateway/run.py": "print('gateway')\n",
    "gateway/dynamic_runtime_dependency.py": "VALUE = 'tracked-dynamic-import'\n",
    "hermes_constants.py": "HERMES_VERSION = 'test'\n",
    "scripts/pnc_rca_outbox_dispatcher.py": "print('outbox')\n",
}


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _sha_json(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


def _write(path: Path, raw: bytes | str, mode: int = 0o644) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw.encode() if isinstance(raw, str) else raw)
    path.chmod(mode)


def _plist(filename: str) -> bytes:
    label, _script_name = stage.CANDIDATE_PLISTS[filename]
    canonical = stage.CANONICAL_LIVE_ROOT
    arguments = stage._expected_plist_arguments(filename, canonical)
    environment = {
        "HOME": "/Users/songying",
        "HERMES_HOME": "/Users/songying/.hermes",
        "PYTHONNOUSERSITE": "1",
        "VIRTUAL_ENV": str(canonical / ".venv"),
        "PATH": f"{canonical}/.venv/bin:/usr/bin:/bin",
    }
    return plistlib.dumps(
        {
            "Label": label,
            "ProgramArguments": arguments,
            "WorkingDirectory": str(canonical),
            "EnvironmentVariables": environment,
            "RunAtLoad": True,
        },
        fmt=plistlib.FMT_XML,
        sort_keys=True,
    )


def _source(repo: Path) -> Path:
    for relative, body in RUNTIME_FILES.items():
        _write(repo / relative, body)
    _write(repo / "pyproject.toml", "[project]\nname='fixture'\n")
    _write(repo / "uv.lock", "version = 1\n")
    for filename in stage.CANDIDATE_PLISTS:
        _write(repo / filename, _plist(filename))
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "test@example.invalid")
    _git(repo, "config", "user.name", "Runtime Stage Test")
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "fixture")
    return repo


def _venv(root: Path, source: Path) -> tuple[Path, dict]:
    venv = root / "unified-venv"
    _write(venv / "bin" / "python", b"fixture-python\n", 0o755)
    _write(venv / "pyvenv.cfg", "home = /fixture/python\n")
    site_packages = venv / "lib" / "site-packages"
    _write(site_packages / "demo.py", "VERSION = '1.0'\n")
    installed = {"demo": "1.0"}
    receipt = {
        "schema_version": stage.RECEIPT_SCHEMA_VERSION,
        "observed_at": "2026-07-13T04:00:00+00:00",
        "venv_path": str(venv),
        "python_version": "3.11.15",
        "python_executable": str(venv / "bin" / "python"),
        "uv_version": "uv fixture",
        "uv_lock_sha256": _sha((source / "uv.lock").read_bytes()),
        "pyproject_sha256": _sha((source / "pyproject.toml").read_bytes()),
        "requirements_sha256": "a" * 64,
        "profile_extras": ["feishu", "kafka"],
        "python_no_user_site_required": True,
        "project_installed": False,
        "site_packages": [str(site_packages)],
        "installed_distributions": installed,
        "installed_distributions_sha256": _sha_json(installed),
        "critical_versions": installed,
    }
    _write(
        venv / "rca-runtime-build-receipt.json",
        json.dumps(receipt, sort_keys=True) + "\n",
        0o600,
    )
    venv.chmod(0o755)
    (venv / "bin").chmod(0o755)
    (venv / "lib").chmod(0o755)
    site_packages.chmod(0o755)
    return venv / "rca-runtime-build-receipt.json", receipt


def _probe(venv: Path) -> dict:
    installed = {"demo": "1.0"}
    return {
        "python_version": "3.11.15",
        "python_executable": str(venv / "bin" / "python"),
        "prefix": str(venv),
        "base_prefix": "/fixture/python",
        "user_site_enabled": False,
        "site_packages": [str(venv / "lib" / "site-packages")],
        "installed_distributions": installed,
        "installed_distributions_sha256": _sha_json(installed),
    }


@pytest.fixture
def fixture(tmp_path: Path) -> SimpleNamespace:
    source = _source(tmp_path / "source")
    receipt, receipt_body = _venv(tmp_path, source)
    return SimpleNamespace(
        source=source,
        receipt=receipt,
        receipt_body=receipt_body,
        staging=tmp_path / "future-runtime",
    )


def _run(fixture: SimpleNamespace, phase: str = "stage", **kwargs):
    return stage.run_runtime_stage(
        phase=phase,
        source_candidate=fixture.source,
        venv_receipt=fixture.receipt,
        staging_root=fixture.staging,
        venv_probe=kwargs.pop("venv_probe", _probe),
        **kwargs,
    )


def test_plan_is_owner_only_and_does_not_create_stage(fixture: SimpleNamespace) -> None:
    result = _run(fixture, "plan")

    assert result.phase == "plan"
    assert result.body["production_effects_executed"] is False
    assert result.body["live_install_supported"] is False
    assert result.body["content"]["source"]["commit"] == _git(
        fixture.source, "rev-parse", "HEAD"
    )
    assert result.body["content"]["source"]["tree"] == _git(
        fixture.source, "rev-parse", "HEAD^{tree}"
    )
    assert set(result.body["content"]["candidate_plists"]) == set(
        stage.CANDIDATE_PLISTS
    )
    assert (result.artifact_path.stat().st_mode & 0o777) == 0o600
    assert not fixture.staging.exists()


def test_stage_is_complete_real_tree_and_projects_canonical_manifest(
    fixture: SimpleNamespace,
) -> None:
    result = _run(fixture)
    manifest = stage.validate_staged_runtime(
        fixture.staging,
        expected_plan=json.loads(stage._plan_path(fixture.staging).read_text()),
        venv_probe=_probe,
    )

    assert result.artifact_path == fixture.staging / stage.MANIFEST_FILENAME
    assert manifest["complete"] is True
    assert manifest["live_install_performed"] is False
    assert manifest["future_canonical_projection"]["canonical_live_root"] == str(
        stage.CANONICAL_LIVE_ROOT
    )
    assert manifest["future_canonical_projection"]["python_executable"] == str(
        stage.CANONICAL_LIVE_ROOT / ".venv" / "bin" / "python"
    )
    for filename in stage.CANDIDATE_PLISTS:
        body = plistlib.loads((fixture.staging / filename).read_bytes())
        assert body["WorkingDirectory"] == str(fixture.staging)
        assert body["ProgramArguments"][0] == str(
            fixture.staging / ".venv" / "bin" / "python"
        )
        assert body["EnvironmentVariables"]["PYTHONDONTWRITEBYTECODE"] == "1"
    for path in fixture.staging.rglob("*"):
        assert not path.is_symlink()
        if path.is_file():
            assert path.stat().st_nlink == 1
    source_python = fixture.receipt.parent / "bin" / "python"
    staged_python = fixture.staging / ".venv" / "bin" / "python"
    assert source_python.stat().st_ino != staged_python.stat().st_ino
    assert not (fixture.staging / ".git").exists()
    assert (
        fixture.staging / "gateway" / "dynamic_runtime_dependency.py"
    ).read_text(encoding="utf-8") == "VALUE = 'tracked-dynamic-import'\n"


def test_repository_candidate_plists_disable_bytecode_writes() -> None:
    root = Path(__file__).resolve().parents[2]

    for filename in stage.CANDIDATE_PLISTS:
        body = plistlib.loads((root / filename).read_bytes())
        assert body["EnvironmentVariables"]["PYTHONDONTWRITEBYTECODE"] == "1"


@pytest.mark.parametrize(
    ("filename", "mutate"),
    [
        (
            "local.pnc.completion-notice-relay.candidate.plist",
            lambda arguments: [value for value in arguments if value != "--watch"],
        ),
        (
            "local.pnc.vm-task-sync.candidate.plist",
            lambda arguments: ["500" if value == "50" else value for value in arguments],
        ),
    ],
)
def test_auxiliary_plist_arguments_are_exactly_bound(
    fixture: SimpleNamespace,
    filename: str,
    mutate: Callable[[list[str]], list[str]],
) -> None:
    path = fixture.source / filename
    body = plistlib.loads(path.read_bytes())
    body["ProgramArguments"] = mutate(body["ProgramArguments"])
    _write(path, plistlib.dumps(body, fmt=plistlib.FMT_XML, sort_keys=True))
    _git(fixture.source, "add", filename)
    _git(fixture.source, "commit", "-qm", f"tamper {filename}")

    with pytest.raises(stage.RuntimeStageError) as error:
        _run(fixture, "plan")

    assert error.value.code == "runtime_stage_plist_projection_invalid"
    assert not fixture.staging.exists()


def test_identical_stage_resumes_without_clobber(fixture: SimpleNamespace) -> None:
    first = _run(fixture)
    second = _run(fixture)

    assert first.body == second.body
    assert second.resumed is True


def test_plan_binding_rejects_a_different_clean_source_commit(
    fixture: SimpleNamespace,
) -> None:
    _run(fixture, "plan")
    target = fixture.source / "gateway" / "run.py"
    target.write_text(target.read_text() + "# next commit\n")
    _git(fixture.source, "add", "gateway/run.py")
    _git(fixture.source, "commit", "-qm", "next")

    with pytest.raises(stage.RuntimeStageError) as error:
        _run(fixture)

    assert error.value.code == "runtime_stage_content_conflict"
    assert not fixture.staging.exists()


@pytest.mark.parametrize("kind", ["dirty", "untracked"])
def test_dirty_or_untracked_source_fails_before_plan(
    fixture: SimpleNamespace,
    kind: str,
) -> None:
    if kind == "dirty":
        target = fixture.source / "gateway" / "run.py"
        target.write_text(target.read_text() + "# dirty\n")
    else:
        _write(fixture.source / "untracked.txt", "untracked\n")

    with pytest.raises(stage.RuntimeStageError) as error:
        _run(fixture, "plan")

    assert error.value.code == "runtime_stage_source_dirty"
    assert not stage._plan_path(fixture.staging).exists()


def test_committed_source_symlink_is_rejected(fixture: SimpleNamespace) -> None:
    target = fixture.source / "gateway" / "run.py"
    target.unlink()
    target.symlink_to("pnc_rca_runtime_identity.py")
    _git(fixture.source, "add", "gateway/run.py")
    _git(fixture.source, "commit", "-qm", "symlink")

    with pytest.raises(stage.RuntimeStageError) as error:
        _run(fixture, "plan")

    assert error.value.code == "runtime_stage_source_tree_invalid"


def test_venv_root_and_unexpected_file_symlinks_are_rejected(
    fixture: SimpleNamespace,
    tmp_path: Path,
) -> None:
    real = fixture.receipt.parent
    linked = tmp_path / "linked-venv"
    linked.symlink_to(real, target_is_directory=True)
    with pytest.raises(stage.RuntimeStageError) as root_error:
        stage.run_runtime_stage(
            phase="plan",
            source_candidate=fixture.source,
            venv_receipt=linked / fixture.receipt.name,
            staging_root=fixture.staging,
            venv_probe=_probe,
        )
    assert root_error.value.code == "runtime_stage_venv_root_invalid"

    symlink = real / "lib" / "site-packages" / "unexpected.py"
    symlink.symlink_to(real / "pyvenv.cfg")
    with pytest.raises(stage.RuntimeStageError) as file_error:
        _run(fixture, "plan")
    assert file_error.value.code == "runtime_stage_venv_symlink_forbidden"


def test_venv_hardlink_and_receipt_drift_fail_closed(
    fixture: SimpleNamespace,
    tmp_path: Path,
) -> None:
    package = fixture.receipt.parent / "lib" / "site-packages" / "demo.py"
    os.link(package, tmp_path / "second-link")
    with pytest.raises(stage.RuntimeStageError) as hardlink:
        _run(fixture, "plan")
    assert hardlink.value.code == "runtime_stage_venv_file_identity_invalid"

    (tmp_path / "second-link").unlink()
    mutated = False

    def drifting_probe(venv: Path) -> dict:
        nonlocal mutated
        result = _probe(venv)
        if not mutated:
            body = json.loads(fixture.receipt.read_text())
            body["requirements_sha256"] = "b" * 64
            _write(fixture.receipt, json.dumps(body, sort_keys=True) + "\n", 0o600)
            mutated = True
        return result

    with pytest.raises(stage.RuntimeStageError) as drift:
        _run(fixture, "plan", venv_probe=drifting_probe)
    assert drift.value.code == "runtime_stage_venv_receipt_drift"


@pytest.mark.parametrize(
    "unsafe",
    [
        stage.CANONICAL_LIVE_ROOT,
        stage.CANONICAL_LIVE_ROOT / "nested",
        stage.CANONICAL_LIVE_ROOT.parent,
        Path("relative-stage"),
        Path("/tmp/runtime/../escape"),
    ],
)
def test_path_escape_and_canonical_aliases_are_rejected(
    fixture: SimpleNamespace,
    unsafe: Path,
) -> None:
    with pytest.raises(stage.RuntimeStageError) as error:
        stage.run_runtime_stage(
            phase="plan",
            source_candidate=fixture.source,
            venv_receipt=fixture.receipt,
            staging_root=unsafe,
            venv_probe=_probe,
        )
    assert error.value.code in {
        "runtime_stage_path_invalid",
        "runtime_stage_live_path_forbidden",
    }


def test_low_disk_fails_before_stage_root_creation(fixture: SimpleNamespace) -> None:
    with pytest.raises(stage.RuntimeStageError) as error:
        _run(
            fixture,
            disk_usage_observer=lambda _path: SimpleNamespace(free=1),
        )

    assert error.value.code == "runtime_stage_insufficient_space"
    assert not fixture.staging.exists()


def test_copy_mutation_leaves_partial_without_complete_manifest(
    fixture: SimpleNamespace,
) -> None:
    mutated = False

    def mutate_source(relative: str, source_path: Path | None) -> None:
        nonlocal mutated
        if not mutated and relative == "gateway/run.py" and source_path is not None:
            source_path.write_text(source_path.read_text() + "# raced\n")
            mutated = True

    with pytest.raises(stage.RuntimeStageError) as error:
        _run(fixture, copy_hook=mutate_source)

    assert error.value.code == "runtime_stage_source_dirty"
    assert fixture.staging.exists()
    assert not (fixture.staging / stage.MANIFEST_FILENAME).exists()
    assert mutated is True


def test_partial_crash_resumes_and_manifest_is_published_last(
    fixture: SimpleNamespace,
) -> None:
    calls = 0

    def crash(_relative: str, _source_path: Path | None) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("simulated crash")

    with pytest.raises(stage.RuntimeStageError) as error:
        _run(fixture, copy_hook=crash)
    assert error.value.code == "runtime_stage_copy_interrupted"
    assert fixture.staging.exists()
    assert not (fixture.staging / stage.MANIFEST_FILENAME).exists()

    resumed = _run(fixture)

    assert resumed.resumed is True
    assert (fixture.staging / stage.MANIFEST_FILENAME).is_file()
    stage.validate_staged_runtime(fixture.staging, venv_probe=_probe)


def test_extra_file_invalidates_complete_or_partial_stage(fixture: SimpleNamespace) -> None:
    _run(fixture)
    _write(fixture.staging / "unexpected.txt", "unexpected\n")

    with pytest.raises(stage.RuntimeStageError) as error:
        stage.validate_staged_runtime(fixture.staging, venv_probe=_probe)

    assert error.value.code == "runtime_stage_extra_or_missing_entry"


def test_default_venv_probe_disables_bytecode_writes(monkeypatch, tmp_path) -> None:
    venv = tmp_path / "venv"
    captured = {}

    def run(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps(_probe(venv)),
            stderr="",
        )

    monkeypatch.setattr(stage.subprocess, "run", run)

    assert stage._default_venv_probe(venv)["prefix"] == str(venv)
    assert captured["command"][:4] == [
        str(venv / "bin" / "python"),
        "-I",
        "-B",
        "-c",
    ]
    assert captured["kwargs"]["env"]["PYTHONDONTWRITEBYTECODE"] == "1"


def test_validate_rechecks_layout_after_venv_probe(fixture: SimpleNamespace) -> None:
    _run(fixture)

    def mutating_probe(venv: Path) -> dict:
        _write(
            venv / "lib" / "site-packages" / "__pycache__" / "probe.pyc",
            b"cache",
        )
        return _probe(venv)

    with pytest.raises(stage.RuntimeStageError) as error:
        stage.validate_staged_runtime(fixture.staging, venv_probe=mutating_probe)

    assert error.value.code == "runtime_stage_changed_during_probe"


def test_secret_is_rejected_and_cache_is_not_carried(fixture: SimpleNamespace) -> None:
    _write(fixture.receipt.parent / ".env", "PASSWORD=secret\n", 0o600)
    with pytest.raises(stage.RuntimeStageError) as secret:
        _run(fixture, "plan")
    assert secret.value.code == "runtime_stage_secret_forbidden"

    (fixture.receipt.parent / ".env").unlink()
    _write(
        fixture.receipt.parent / "lib" / "site-packages" / "__pycache__" / "x.pyc",
        b"cache",
    )
    _write(fixture.receipt.parent / ".lock", b"build lock\n", 0o666)
    _run(fixture)

    assert not any("__pycache__" in path.parts for path in fixture.staging.rglob("*"))
    assert not (fixture.staging / ".venv" / ".lock").exists()
    serialized = (fixture.staging / stage.MANIFEST_FILENAME).read_text()
    assert "PASSWORD=secret" not in serialized
