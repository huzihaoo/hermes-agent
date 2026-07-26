from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess

import pytest

from scripts import pnc_rca_release_freshness_gate as gate
from scripts import pnc_rca_staged_manifest as binder


_VALID_EVALUATOR_SOURCE = """\
G1Q3_EVALUATOR_SCOPE = 'g1q3_rca_evaluator_scope_v4'
G1Q3_EVALUATOR_INVENTORY = (
    'lane_geometry_quality',
    'acc_decel_heavy',
)
"""


def _git(repo: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return completed.stdout.strip()


def _commit(repo: Path, message: str) -> tuple[str, str]:
    _git(repo, "add", "-A")
    _git(
        repo,
        "-c",
        "core.hooksPath=/dev/null",
        "commit",
        "-q",
        "-m",
        message,
    )
    return _git(repo, "rev-parse", "HEAD"), _git(repo, "rev-parse", "HEAD^{tree}")


def _pipeline_repo(tmp_path: Path) -> tuple[Path, Path, str, str]:
    root = tmp_path / "pipeline"
    root.mkdir()
    _git(root, "init", "-q")
    _git(root, "config", "user.name", "PNC test")
    _git(root, "config", "user.email", "pnc-test@example.invalid")
    source = root / gate.ACTIVE_EVALUATOR_INVENTORY_SOURCE_PATH
    source.parent.mkdir(parents=True)
    source.write_text(_VALID_EVALUATOR_SOURCE, encoding="utf-8")
    commit, tree = _commit(root, "add evaluator inventory")
    return root, source, commit, tree


def _manifest(root: Path, commit: str, tree: str) -> dict[str, object]:
    return {
        "release_note": "staged only",
        "face_git_bindings": {
            "runtime_engine": {"commit": "a" * 40, "tree": "b" * 40},
            binder.PIPELINE_FACE: {
                "repo": str(root),
                "commit": commit,
                "tree": tree,
                "source_repository_binding": {
                    "repo": str(root),
                    "commit": commit,
                    "tree": tree,
                },
                "reason": "candidate pipeline",
            },
        },
    }


def _write_manifest(path: Path, value: object) -> bytes:
    raw = json.dumps(value, ensure_ascii=False).encode("utf-8")
    path.write_bytes(raw)
    return raw


def _bind(
    staged_root: Path,
    source_root: Path,
    commit: str,
    tree: str,
) -> tuple[Path, Path, dict[str, object]]:
    input_path = staged_root / "manifest-input.json"
    output_path = staged_root / "manifest-bound.json"
    manifest = _manifest(source_root, commit, tree)
    _write_manifest(input_path, manifest)
    return input_path, output_path, manifest


def test_bind_staged_manifest_is_canonical_atomic_exact_and_idempotent(tmp_path: Path):
    source_root, source, commit, tree = _pipeline_repo(tmp_path)
    staged_root = tmp_path / "stage"
    staged_root.mkdir(mode=0o700)
    input_path, output_path, source_manifest = _bind(
        staged_root, source_root, commit, tree
    )

    first = binder.bind_staged_manifest(
        input_path,
        output_path,
        pipeline_source_root=source_root,
        pipeline_commit=commit,
        pipeline_tree=tree,
        staged_root=staged_root,
    )
    payload = json.loads(output_path.read_bytes())
    inventory = payload["face_git_bindings"][binder.PIPELINE_FACE][
        "evaluator_inventory"
    ]

    assert payload["release_note"] == source_manifest["release_note"]
    assert inventory["pipeline_commit"] == commit
    assert inventory["pipeline_tree"] == tree
    assert (
        inventory["source_blob_sha256"]
        == hashlib.sha256(source.read_bytes()).hexdigest()
    )
    assert inventory["evaluator_ids"] == [
        "acc_decel_heavy",
        "lane_geometry_quality",
    ]
    assert output_path.read_bytes() == binder.canonical_manifest_bytes(payload)
    assert stat_mode(output_path) == 0o600
    assert first["written"] is True
    assert (
        first["input_manifest_sha256"]
        == hashlib.sha256(input_path.read_bytes()).hexdigest()
    )
    assert (
        first["manifest_sha256"] == hashlib.sha256(output_path.read_bytes()).hexdigest()
    )
    assert first["production_actions"] == {
        "manifest_applies": 0,
        "releases": 0,
        "restarts": 0,
        "database_writes": 0,
        "external_writes": 0,
    }
    assert not list(staged_root.glob(f".{output_path.name}.tmp.*"))

    second = binder.bind_staged_manifest(
        input_path,
        output_path,
        pipeline_source_root=source_root,
        pipeline_commit=commit,
        pipeline_tree=tree,
        staged_root=staged_root,
    )
    assert second["written"] is False
    assert output_path.read_bytes() == binder.canonical_manifest_bytes(payload)


def stat_mode(path: Path) -> int:
    return path.stat().st_mode & 0o777


@pytest.mark.parametrize(
    "manifest",
    [
        {},
        {"face_git_bindings": {}},
        {"face_git_bindings": {binder.PIPELINE_FACE: []}},
    ],
)
def test_missing_or_invalid_pipeline_face_leaves_no_output(
    tmp_path: Path, manifest: object
):
    source_root, _source, commit, tree = _pipeline_repo(tmp_path)
    staged_root = tmp_path / "stage"
    staged_root.mkdir()
    input_path = staged_root / "input.json"
    output_path = staged_root / "output.json"
    _write_manifest(input_path, manifest)

    with pytest.raises(
        binder.StagedManifestError, match="staged_pipeline_face_missing"
    ):
        binder.bind_staged_manifest(
            input_path,
            output_path,
            pipeline_source_root=source_root,
            pipeline_commit=commit,
            pipeline_tree=tree,
            staged_root=staged_root,
        )
    assert not output_path.exists()


def test_pipeline_commit_and_tree_mismatch_leave_no_output(tmp_path: Path):
    source_root, _source, commit, tree = _pipeline_repo(tmp_path)
    staged_root = tmp_path / "stage"
    staged_root.mkdir()
    input_path, output_path, manifest = _bind(staged_root, source_root, commit, tree)

    with pytest.raises(binder.StagedManifestError, match="commit_mismatch"):
        binder.bind_staged_manifest(
            input_path,
            output_path,
            pipeline_source_root=source_root,
            pipeline_commit="c" * 40,
            pipeline_tree=tree,
            staged_root=staged_root,
        )
    manifest["face_git_bindings"][binder.PIPELINE_FACE]["tree"] = "d" * 40
    _write_manifest(input_path, manifest)
    with pytest.raises(binder.StagedManifestError, match="tree_mismatch"):
        binder.bind_staged_manifest(
            input_path,
            output_path,
            pipeline_source_root=source_root,
            pipeline_commit=commit,
            pipeline_tree=tree,
            staged_root=staged_root,
        )
    assert not output_path.exists()


def test_nested_pipeline_source_binding_must_match_exact_face(tmp_path: Path):
    source_root, _source, commit, tree = _pipeline_repo(tmp_path)
    staged_root = tmp_path / "stage"
    staged_root.mkdir()
    input_path, output_path, manifest = _bind(staged_root, source_root, commit, tree)
    manifest["face_git_bindings"][binder.PIPELINE_FACE]["source_repository_binding"][
        "commit"
    ] = [commit]
    _write_manifest(input_path, manifest)

    with pytest.raises(binder.StagedManifestError, match="source_binding_mismatch"):
        binder.bind_staged_manifest(
            input_path,
            output_path,
            pipeline_source_root=source_root,
            pipeline_commit=commit,
            pipeline_tree=tree,
            staged_root=staged_root,
        )
    assert not output_path.exists()


def test_dirty_pipeline_source_leaves_no_output(tmp_path: Path):
    source_root, source, commit, tree = _pipeline_repo(tmp_path)
    staged_root = tmp_path / "stage"
    staged_root.mkdir()
    input_path, output_path, _manifest_value = _bind(
        staged_root, source_root, commit, tree
    )
    source.write_text(_VALID_EVALUATOR_SOURCE + "# dirty\n", encoding="utf-8")

    with pytest.raises(binder.StagedManifestError, match="source_dirty"):
        binder.bind_staged_manifest(
            input_path,
            output_path,
            pipeline_source_root=source_root,
            pipeline_commit=commit,
            pipeline_tree=tree,
            staged_root=staged_root,
        )
    assert not output_path.exists()


def test_hidden_blob_mismatch_leaves_no_output(tmp_path: Path):
    source_root, source, commit, tree = _pipeline_repo(tmp_path)
    staged_root = tmp_path / "stage"
    staged_root.mkdir()
    input_path, output_path, _manifest_value = _bind(
        staged_root, source_root, commit, tree
    )
    _git(
        source_root,
        "update-index",
        "--assume-unchanged",
        gate.ACTIVE_EVALUATOR_INVENTORY_SOURCE_PATH,
    )
    source.write_text(_VALID_EVALUATOR_SOURCE + "# hidden\n", encoding="utf-8")

    with pytest.raises(binder.StagedManifestError, match="source_blob_mismatch"):
        binder.bind_staged_manifest(
            input_path,
            output_path,
            pipeline_source_root=source_root,
            pipeline_commit=commit,
            pipeline_tree=tree,
            staged_root=staged_root,
        )
    assert not output_path.exists()


def test_stale_inventory_in_input_is_never_replaced(tmp_path: Path):
    source_root, _source, commit, tree = _pipeline_repo(tmp_path)
    staged_root = tmp_path / "stage"
    staged_root.mkdir()
    input_path, output_path, manifest = _bind(staged_root, source_root, commit, tree)
    manifest["face_git_bindings"][binder.PIPELINE_FACE]["evaluator_inventory"] = {
        "pipeline_commit": "e" * 40,
        "pipeline_tree": "f" * 40,
    }
    original = _write_manifest(input_path, manifest)

    with pytest.raises(binder.StagedManifestError, match="stale_replacement"):
        binder.bind_staged_manifest(
            input_path,
            output_path,
            pipeline_source_root=source_root,
            pipeline_commit=commit,
            pipeline_tree=tree,
            staged_root=staged_root,
        )
    assert input_path.read_bytes() == original
    assert not output_path.exists()


def test_stale_output_is_never_overwritten(tmp_path: Path):
    source_root, _source, commit, tree = _pipeline_repo(tmp_path)
    staged_root = tmp_path / "stage"
    staged_root.mkdir()
    input_path, output_path, _manifest_value = _bind(
        staged_root, source_root, commit, tree
    )
    stale = b'{"newer":"staged manifest"}\n'
    output_path.write_bytes(stale)

    with pytest.raises(binder.StagedManifestError, match="output_stale_replacement"):
        binder.bind_staged_manifest(
            input_path,
            output_path,
            pipeline_source_root=source_root,
            pipeline_commit=commit,
            pipeline_tree=tree,
            staged_root=staged_root,
        )
    assert output_path.read_bytes() == stale


def test_atomic_write_failure_leaves_no_output_or_temporary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    source_root, _source, commit, tree = _pipeline_repo(tmp_path)
    staged_root = tmp_path / "stage"
    staged_root.mkdir()
    input_path, output_path, _manifest_value = _bind(
        staged_root, source_root, commit, tree
    )

    def fail_replace(_source: object, _target: object) -> None:
        raise OSError("injected replace failure")

    monkeypatch.setattr(binder.os, "replace", fail_replace)
    with pytest.raises(binder.StagedManifestError, match="atomic_write_failed"):
        binder.bind_staged_manifest(
            input_path,
            output_path,
            pipeline_source_root=source_root,
            pipeline_commit=commit,
            pipeline_tree=tree,
            staged_root=staged_root,
        )
    assert not output_path.exists()
    assert not list(staged_root.glob(f".{output_path.name}.tmp.*"))


def test_live_manifest_output_is_explicitly_forbidden(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    source_root, _source, commit, tree = _pipeline_repo(tmp_path)
    staged_root = tmp_path / "stage"
    staged_root.mkdir()
    input_path, _output_path, _manifest_value = _bind(
        staged_root, source_root, commit, tree
    )
    hermes_home = tmp_path / "active-hermes"
    live_manifest = hermes_home / "runtime" / "LIVE_MANIFEST.json"
    monkeypatch.setenv("HERMES_HOME", str(hermes_home))

    with pytest.raises(
        binder.StagedManifestError, match="live_manifest_write_forbidden"
    ):
        binder.bind_staged_manifest(
            input_path,
            live_manifest,
            pipeline_source_root=source_root,
            pipeline_commit=commit,
            pipeline_tree=tree,
        )
    assert not live_manifest.exists()


def test_cli_failure_is_nonzero_and_does_not_create_output(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
):
    source_root, _source, commit, tree = _pipeline_repo(tmp_path)
    staged_root = tmp_path / "stage"
    staged_root.mkdir()
    input_path = staged_root / "input.json"
    output_path = staged_root / "output.json"
    _write_manifest(input_path, {"face_git_bindings": {}})

    exit_code = binder.main([
        "--input-manifest",
        str(input_path),
        "--output-manifest",
        str(output_path),
        "--pipeline-source-root",
        str(source_root),
        "--pipeline-commit",
        commit,
        "--pipeline-tree",
        tree,
        "--staged-root",
        str(staged_root),
    ])
    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 2
    assert payload == {
        "ok": False,
        "error": "pnc_release_staged_pipeline_face_missing",
    }
    assert not output_path.exists()
