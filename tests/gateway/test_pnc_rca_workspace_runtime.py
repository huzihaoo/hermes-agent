from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from gateway import pnc_rca_workspace_runtime as runtime


SOURCE_COMMIT = "a" * 40
SOURCE_FILES = {
    "bin/create_task_v2.py": b"from shared_state_v2 import create_task_main\n",
    "bin/shared_state_v2.py": b"from shared_state_fields import enforce_task_fields\n",
    "bin/shared_state_fields.py": b"def enforce_task_fields():\n    return None\n",
}


def _write(path: Path, raw: bytes, mode: int) -> None:
    path.write_bytes(raw)
    path.chmod(mode)


def _bundle(home: Path) -> Path:
    root = runtime.canonical_workspace_runtime_root(home)
    (root / "bin").mkdir(parents=True, mode=0o700)
    root.chmod(0o700)
    (root / "bin").chmod(0o700)
    descriptors = {}
    for index, path in enumerate(runtime.WORKSPACE_RUNTIME_FILES, start=1):
        _write(
            root / path,
            SOURCE_FILES[path],
            runtime.WORKSPACE_RUNTIME_FILE_MODES[path],
        )
        descriptors[path] = runtime.workspace_runtime_descriptor(
            path=path,
            raw=SOURCE_FILES[path],
            git_blob_oid=f"{index:x}" * 40,
        )
    manifest = runtime.build_workspace_runtime_manifest(
        source_commit=SOURCE_COMMIT,
        files=descriptors,
    )
    _write(
        root / runtime.WORKSPACE_RUNTIME_MANIFEST_NAME,
        (json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n").encode(),
        0o600,
    )
    return root


def test_valid_runtime_returns_repeatable_exact_identity(tmp_path: Path) -> None:
    home = tmp_path / "hermes"
    root = _bundle(home)

    first = runtime.validate_workspace_runtime(hermes_home=home)
    second = runtime.validate_workspace_runtime(hermes_home=home)

    assert first == second
    assert first.root == root
    assert first.creator_path == root / "bin" / "create_task_v2.py"
    assert first.source_commit == SOURCE_COMMIT
    assert set(first.file_sha256) == set(runtime.WORKSPACE_RUNTIME_FILES)
    assert first.task_meta() == {
        "rca_workspace_runtime_manifest_sha256": first.manifest_sha256,
        "rca_workspace_runtime_closure_sha256": first.closure_sha256,
        "rca_workspace_runtime_source_commit": SOURCE_COMMIT,
    }


@pytest.mark.parametrize("mutation", ["missing", "extra", "mode", "manifest"])
def test_runtime_fails_closed_for_missing_extra_mode_or_manifest_drift(
    tmp_path: Path,
    mutation: str,
) -> None:
    home = tmp_path / "hermes"
    root = _bundle(home)
    if mutation == "missing":
        (root / "bin" / "shared_state_fields.py").unlink()
    elif mutation == "extra":
        _write(root / "bin" / "unexpected.py", b"pass\n", 0o644)
    elif mutation == "mode":
        (root / "bin" / "shared_state_v2.py").chmod(0o700)
    else:
        manifest_path = root / runtime.WORKSPACE_RUNTIME_MANIFEST_NAME
        body = json.loads(manifest_path.read_text())
        body["source_commit"] = "b" * 40
        _write(
            manifest_path,
            (json.dumps(body, sort_keys=True) + "\n").encode(),
            0o600,
        )

    with pytest.raises(runtime.WorkspaceRuntimeError):
        runtime.validate_workspace_runtime(hermes_home=home)


def test_runtime_rejects_symlinked_root_and_file(tmp_path: Path) -> None:
    real_home = tmp_path / "real-home"
    real_root = _bundle(real_home)
    linked_home = tmp_path / "linked-home"
    (linked_home / "runtime").mkdir(parents=True)
    (linked_home / "runtime" / runtime.WORKSPACE_RUNTIME_DIRECTORY_NAME).symlink_to(
        real_root,
        target_is_directory=True,
    )

    with pytest.raises(runtime.WorkspaceRuntimeError):
        runtime.validate_workspace_runtime(hermes_home=linked_home)

    source = real_root / "bin" / "shared_state_fields.py"
    source.unlink()
    source.symlink_to(real_root / "bin" / "create_task_v2.py")
    with pytest.raises(runtime.WorkspaceRuntimeError):
        runtime.validate_workspace_runtime(hermes_home=real_home)


def test_runtime_rejects_hardlinked_file(tmp_path: Path) -> None:
    home = tmp_path / "hermes"
    root = _bundle(home)
    target = root / "bin" / "shared_state_fields.py"
    os.link(target, tmp_path / "second-link")

    with pytest.raises(runtime.WorkspaceRuntimeError) as error:
        runtime.validate_workspace_runtime(hermes_home=home)

    assert error.value.code == "rca_workspace_runtime_file_identity_invalid"


def test_runtime_detects_concurrent_file_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    home = tmp_path / "hermes"
    root = _bundle(home)
    original = runtime._read_stable_regular
    replaced = False

    def racing_read(path: Path, *, mode: int, limit: int):
        nonlocal replaced
        observed = original(path, mode=mode, limit=limit)
        if path.name == "create_task_v2.py" and not replaced:
            replacement = path.with_name(".replacement")
            _write(replacement, observed.raw, mode)
            os.replace(replacement, path)
            replaced = True
        return observed

    monkeypatch.setattr(runtime, "_read_stable_regular", racing_read)

    with pytest.raises(runtime.WorkspaceRuntimeError) as error:
        runtime.validate_workspace_runtime(hermes_home=home)

    assert replaced is True
    assert error.value.code == "rca_workspace_runtime_file_unstable"


def test_runtime_detects_concurrent_manifest_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    home = tmp_path / "hermes"
    _bundle(home)
    original = runtime._read_stable_regular
    replaced = False

    def racing_read(path: Path, *, mode: int, limit: int):
        nonlocal replaced
        observed = original(path, mode=mode, limit=limit)
        if path.name == runtime.WORKSPACE_RUNTIME_MANIFEST_NAME and not replaced:
            replacement = path.with_name(".manifest-replacement")
            _write(replacement, observed.raw, mode)
            os.replace(replacement, path)
            replaced = True
        return observed

    monkeypatch.setattr(runtime, "_read_stable_regular", racing_read)

    with pytest.raises(runtime.WorkspaceRuntimeError) as error:
        runtime.validate_workspace_runtime(hermes_home=home)

    assert replaced is True
    assert error.value.code == "rca_workspace_runtime_manifest_drift"


def test_runtime_rejects_noncanonical_explicit_root(tmp_path: Path) -> None:
    home = tmp_path / "hermes"
    root = _bundle(home)

    with pytest.raises(runtime.WorkspaceRuntimeError) as error:
        runtime.validate_workspace_runtime(root, hermes_home=tmp_path / "other")

    assert error.value.code == "rca_workspace_runtime_root_not_canonical"
