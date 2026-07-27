from __future__ import annotations

import hashlib
import json
import os
import plistlib
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest

from scripts import pnc_rca_release_freshness_gate as gate
from gateway.pnc_rca_control_store import CONTROL_STORE_SCHEMA_VERSION, RcaControlStore
from gateway.pnc_rca_delivery_store import RcaDeliveryStore
from scripts.pnc_live_exec import PNC_PYTHON_LAUNCHD_LABELS


REPO_ROOT = Path(__file__).resolve().parents[2]
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


def _pipeline_repo(
    tmp_path: Path,
    *,
    source_text: str = _VALID_EVALUATOR_SOURCE,
) -> tuple[Path, Path, str, str]:
    source_root = tmp_path / "pipeline"
    source_root.mkdir()
    _git(source_root, "init", "-q")
    _git(source_root, "config", "user.name", "PNC test")
    _git(source_root, "config", "user.email", "pnc-test@example.invalid")
    source = source_root / gate.ACTIVE_EVALUATOR_INVENTORY_SOURCE_PATH
    source.parent.mkdir(parents=True)
    source.write_text(source_text, encoding="utf-8")
    commit, tree = _commit(source_root, "add evaluator inventory")
    return source_root, source, commit, tree


def _active_evaluator_inventory_binding(
    *, commit: str, tree: str, evaluator_ids: list[str]
) -> dict[str, object]:
    normalized = sorted(evaluator_ids)
    evaluator_scope = "g1q3_rca_evaluator_scope_v4"
    return {
        "schema_version": gate.ACTIVE_EVALUATOR_INVENTORY_SCHEMA_VERSION,
        "pipeline_commit": commit,
        "pipeline_tree": tree,
        "source_path": gate.ACTIVE_EVALUATOR_INVENTORY_SOURCE_PATH,
        "source_symbol": gate.ACTIVE_EVALUATOR_INVENTORY_SOURCE_SYMBOL,
        "source_blob_sha256": "c" * 64,
        "evaluator_scope": evaluator_scope,
        "evaluator_ids": normalized,
        "inventory_sha256": gate._evaluator_inventory_sha256(
            evaluator_scope,
            normalized,
        ),
    }


def _write_pipeline_manifest(
    runtime: Path,
    *,
    commit: str,
    tree: str,
    evaluator_ids: list[str] | None,
    binding_updates: dict[str, object] | None = None,
) -> None:
    face: dict[str, object] = {"commit": commit, "tree": tree}
    if evaluator_ids is not None:
        binding = _active_evaluator_inventory_binding(
            commit=commit,
            tree=tree,
            evaluator_ids=evaluator_ids,
        )
        binding.update(binding_updates or {})
        face["evaluator_inventory"] = binding
    (runtime / "LIVE_MANIFEST.json").write_text(
        json.dumps({"face_git_bindings": {"g1q3_rca_pipeline": face}}),
        encoding="utf-8",
    )


def test_materialize_active_evaluator_inventory_uses_ast_and_exact_keys(
    tmp_path: Path,
):
    source_root, source, commit, tree = _pipeline_repo(tmp_path)
    binding = gate.materialize_active_evaluator_inventory_binding(
        pipeline_source_root=source_root,
        pipeline_commit=commit,
        pipeline_tree=tree,
    )

    assert binding["pipeline_commit"] == commit
    assert binding["pipeline_tree"] == tree
    assert binding["evaluator_ids"] == ["acc_decel_heavy", "lane_geometry_quality"]
    assert (
        binding["source_blob_sha256"] == hashlib.sha256(source.read_bytes()).hexdigest()
    )
    assert binding["inventory_sha256"] == gate._evaluator_inventory_sha256(
        "g1q3_rca_evaluator_scope_v4",
        ["acc_decel_heavy", "lane_geometry_quality"],
    )

    source.write_text(
        "G1Q3_EVALUATOR_SCOPE='scope'\n"
        "G1Q3_EVALUATOR_INVENTORY=('duplicate', 'duplicate')\n",
        encoding="utf-8",
    )
    invalid_commit, invalid_tree = _commit(source_root, "add invalid inventory")
    with pytest.raises(ValueError, match="source_invalid"):
        gate.materialize_active_evaluator_inventory_binding(
            pipeline_source_root=source_root,
            pipeline_commit=invalid_commit,
            pipeline_tree=invalid_tree,
        )


def test_materialize_evaluator_inventory_rejects_non_git_source(tmp_path: Path):
    source_root = tmp_path / "pipeline"
    source = source_root / gate.ACTIVE_EVALUATOR_INVENTORY_SOURCE_PATH
    source.parent.mkdir(parents=True)
    source.write_text(_VALID_EVALUATOR_SOURCE, encoding="utf-8")

    with pytest.raises(ValueError, match="pipeline_repository_invalid"):
        gate.materialize_active_evaluator_inventory_binding(
            pipeline_source_root=source_root,
            pipeline_commit="a" * 40,
            pipeline_tree="b" * 40,
        )


def test_materialize_evaluator_inventory_rejects_wrong_commit_and_tree(
    tmp_path: Path,
):
    source_root, _source, first_commit, first_tree = _pipeline_repo(tmp_path)
    (source_root / "README.md").write_text("second commit\n", encoding="utf-8")
    current_commit, current_tree = _commit(source_root, "advance pipeline")

    with pytest.raises(ValueError, match="pipeline_commit_mismatch"):
        gate.materialize_active_evaluator_inventory_binding(
            pipeline_source_root=source_root,
            pipeline_commit=first_commit,
            pipeline_tree=first_tree,
        )
    with pytest.raises(ValueError, match="pipeline_tree_mismatch"):
        gate.materialize_active_evaluator_inventory_binding(
            pipeline_source_root=source_root,
            pipeline_commit=current_commit,
            pipeline_tree=first_tree,
        )
    assert current_tree != first_tree


def test_materialize_evaluator_inventory_rejects_dirty_source(tmp_path: Path):
    source_root, source, commit, tree = _pipeline_repo(tmp_path)
    source.write_text(_VALID_EVALUATOR_SOURCE + "# dirty\n", encoding="utf-8")

    with pytest.raises(ValueError, match="source_dirty"):
        gate.materialize_active_evaluator_inventory_binding(
            pipeline_source_root=source_root,
            pipeline_commit=commit,
            pipeline_tree=tree,
        )

    _git(source_root, "add", gate.ACTIVE_EVALUATOR_INVENTORY_SOURCE_PATH)
    with pytest.raises(ValueError, match="source_dirty"):
        gate.materialize_active_evaluator_inventory_binding(
            pipeline_source_root=source_root,
            pipeline_commit=commit,
            pipeline_tree=tree,
        )


def test_materialize_evaluator_inventory_rejects_untracked_source(tmp_path: Path):
    source_root = tmp_path / "pipeline"
    source_root.mkdir()
    _git(source_root, "init", "-q")
    _git(source_root, "config", "user.name", "PNC test")
    _git(source_root, "config", "user.email", "pnc-test@example.invalid")
    (source_root / "README.md").write_text("tracked\n", encoding="utf-8")
    commit, tree = _commit(source_root, "add tracked file")
    source = source_root / gate.ACTIVE_EVALUATOR_INVENTORY_SOURCE_PATH
    source.parent.mkdir(parents=True)
    source.write_text(_VALID_EVALUATOR_SOURCE, encoding="utf-8")

    with pytest.raises(ValueError, match="source_untracked"):
        gate.materialize_active_evaluator_inventory_binding(
            pipeline_source_root=source_root,
            pipeline_commit=commit,
            pipeline_tree=tree,
        )


def test_materialize_evaluator_inventory_verifies_commit_blob(tmp_path: Path):
    source_root, source, commit, tree = _pipeline_repo(tmp_path)
    _git(
        source_root,
        "update-index",
        "--assume-unchanged",
        gate.ACTIVE_EVALUATOR_INVENTORY_SOURCE_PATH,
    )
    source.write_text(_VALID_EVALUATOR_SOURCE + "# hidden change\n", encoding="utf-8")

    with pytest.raises(ValueError, match="source_blob_mismatch"):
        gate.materialize_active_evaluator_inventory_binding(
            pipeline_source_root=source_root,
            pipeline_commit=commit,
            pipeline_tree=tree,
        )


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
        "hermes-context-budget-check",
        "hermes-governance-check",
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
    context_wrapper = (
        REPO_ROOT / "scripts" / "wrappers" / "hermes-context-budget-check"
    ).read_text(encoding="utf-8")
    assert "local.pnc.context-budget-check" in context_wrapper
    assert "--repo-root ." in context_wrapper
    assert "HERMES_CONTEXT_BUDGET_SCRIPT" not in context_wrapper
    assert "HERMES_CONTEXT_BUDGET_REPO_ROOT" not in context_wrapper
    assert gate.SERVICE_TARGETS["local.pnc.hermes-cli"] == (
        "runtime_script",
        "hermes_cli/main.py",
    )
    assert gate.SERVICE_TARGETS["local.pnc.release-freshness-gate"] == (
        "runtime_script",
        "scripts/pnc_rca_release_freshness_gate.py",
    )
    assert "local.pnc.g1q3-e2e-smoke" not in gate.SERVICE_TARGETS
    assert "local.pnc.live-promote" not in gate.SERVICE_TARGETS
    assert not (REPO_ROOT / "scripts/wrappers/hermes-g1q3-e2e-smoke").exists()
    assert not (REPO_ROOT / "scripts/wrappers/hermes-live").exists()


def test_retired_stable_entrypoints_must_be_absent(tmp_path: Path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name in gate.RETIRED_STABLE_ENTRYPOINTS:
        (bin_dir / name).write_text(
            "#!/bin/zsh\nexec pnc_live_exec retired-label\n",
            encoding="utf-8",
        )

    evidence, errors = gate.audit_retired_stable_entrypoints(tmp_path)

    assert {item["name"] for item in evidence if item["present"]} == set(
        gate.RETIRED_STABLE_ENTRYPOINTS
    )
    assert {(item["code"], item["name"]) for item in errors} == {
        ("pnc_release_retired_entrypoint_present", name)
        for name in gate.RETIRED_STABLE_ENTRYPOINTS
    }


def test_absent_retired_stable_entrypoints_pass(tmp_path: Path):
    evidence, errors = gate.audit_retired_stable_entrypoints(tmp_path)

    assert errors == []
    assert all(item["present"] is False for item in evidence)


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


def test_release_golden_registry_must_be_green_and_pipeline_bound(tmp_path: Path):
    hermes_home = tmp_path / ".hermes"
    runtime = hermes_home / "runtime"
    runtime.mkdir(parents=True)
    commit = "a" * 40
    tree = "b" * 40
    required = ["acc_decel_heavy"]
    _write_pipeline_manifest(
        runtime,
        commit=commit,
        tree=tree,
        evaluator_ids=required,
    )
    base = {
        "present": True,
        "valid": True,
        "low_tier_golden_ready": True,
        "pipeline_commit": commit,
        "pipeline_tree": tree,
        "evaluators": {
            "acc_decel_heavy": {
                "status": "passed",
                "evaluator_id": "acc_decel_heavy",
                "source_kind": "owner_confirmed_case",
            }
        },
    }

    evidence, errors = gate.audit_release_golden_registry(
        hermes_home=hermes_home,
        registry=base,
        required_evaluator_ids=required,
    )
    assert errors == []
    assert evidence["active_pipeline_commit"] == commit

    empty = {**base, "evaluators": {}}
    evidence, errors = gate.audit_release_golden_registry(
        hermes_home=hermes_home,
        registry=empty,
        required_evaluator_ids=required,
    )
    assert errors == []
    assert evidence["inventory_binding_valid"] is True
    assert evidence["golden_scope_evaluator_ids"] == []
    assert evidence["uncovered_evaluator_ids"] == required
    assert evidence["high_confidence_ready"] is False
    assert evidence["safe_downgrade"] is True
    assert evidence["safe_downgrade_reason"] == "no_genuine_high_scope"

    failing = {**base, "low_tier_golden_ready": False}
    _evidence, errors = gate.audit_release_golden_registry(
        hermes_home=hermes_home,
        registry=failing,
        required_evaluator_ids=required,
    )
    assert {item["code"] for item in errors} == {
        "pnc_release_low_tier_golden_not_ready"
    }

    stale = {**base, "pipeline_commit": "c" * 40}
    _evidence, errors = gate.audit_release_golden_registry(
        hermes_home=hermes_home,
        registry=stale,
        required_evaluator_ids=required,
    )
    assert {item["code"] for item in errors} == {
        "pnc_release_golden_pipeline_binding_mismatch"
    }


def test_release_golden_registry_binds_explicit_active_inventory(tmp_path: Path):
    hermes_home = tmp_path / ".hermes"
    runtime = hermes_home / "runtime"
    runtime.mkdir(parents=True)
    commit = "a" * 40
    tree = "b" * 40
    (runtime / "LIVE_MANIFEST.json").write_text(
        json.dumps({
            "face_git_bindings": {"g1q3_rca_pipeline": {"commit": commit, "tree": tree}}
        }),
        encoding="utf-8",
    )
    registry = {
        "present": True,
        "valid": True,
        "low_tier_golden_ready": True,
        "pipeline_commit": commit,
        "pipeline_tree": tree,
        "evaluators": {
            "lane_geometry_quality": {
                "status": "passed",
                "evaluator_id": "lane_geometry_quality",
                "source_kind": "owner_confirmed_case",
            }
        },
    }

    evidence, errors = gate.audit_release_golden_registry(
        hermes_home=hermes_home,
        registry=registry,
        required_evaluator_ids=["lane_geometry_quality", "new_evaluator"],
    )

    assert errors == []
    assert evidence["required_evaluator_ids"] == [
        "lane_geometry_quality",
        "new_evaluator",
    ]
    assert evidence["golden_scope_evaluator_ids"] == ["lane_geometry_quality"]
    assert evidence["uncovered_evaluator_ids"] == ["new_evaluator"]
    assert evidence["inventory_binding_valid"] is True
    assert evidence["high_confidence_ready"] is True

    unexpected = {
        **registry,
        "evaluators": {
            **registry["evaluators"],
            "legacy_alias": {
                "status": "passed",
                "evaluator_id": "legacy_alias",
                "source_kind": "owner_confirmed_case",
            },
        },
    }
    evidence, errors = gate.audit_release_golden_registry(
        hermes_home=hermes_home,
        registry=unexpected,
        required_evaluator_ids=["lane_geometry_quality"],
    )
    assert evidence["unexpected_evaluator_ids"] == ["legacy_alias"]
    assert {item["code"] for item in errors} == {
        "pnc_release_golden_evaluator_inventory_unexpected"
    }


def test_release_golden_registry_requires_exact_declared_high_scope(tmp_path: Path):
    hermes_home = tmp_path / ".hermes"
    runtime = hermes_home / "runtime"
    runtime.mkdir(parents=True)
    commit = "a" * 40
    tree = "b" * 40
    (runtime / "LIVE_MANIFEST.json").write_text(
        json.dumps({
            "face_git_bindings": {"g1q3_rca_pipeline": {"commit": commit, "tree": tree}}
        }),
        encoding="utf-8",
    )
    registry = {
        "present": True,
        "valid": True,
        "low_tier_golden_ready": True,
        "pipeline_commit": commit,
        "pipeline_tree": tree,
        "golden_scope_explicit": True,
        "golden_scope_evaluator_ids": ["lane_geometry_quality", "acc_decel_heavy"],
        "evaluators": {
            "lane_geometry_quality": {
                "status": "passed",
                "evaluator_id": "lane_geometry_quality",
                "source_kind": "owner_confirmed_case",
            }
        },
    }

    evidence, errors = gate.audit_release_golden_registry(
        hermes_home=hermes_home,
        registry=registry,
        required_evaluator_ids=["lane_geometry_quality", "acc_decel_heavy"],
    )

    assert evidence["golden_scope_exact"] is False
    assert {item["code"] for item in errors} == {
        "pnc_release_golden_high_scope_invalid"
    }


def test_release_golden_registry_rejects_machine_observation_as_golden(
    tmp_path: Path,
):
    hermes_home = tmp_path / ".hermes"
    runtime = hermes_home / "runtime"
    runtime.mkdir(parents=True)
    commit = "a" * 40
    tree = "b" * 40
    (runtime / "LIVE_MANIFEST.json").write_text(
        json.dumps({
            "face_git_bindings": {"g1q3_rca_pipeline": {"commit": commit, "tree": tree}}
        }),
        encoding="utf-8",
    )
    registry = {
        "present": True,
        "valid": True,
        "low_tier_golden_ready": True,
        "pipeline_commit": commit,
        "pipeline_tree": tree,
        "evaluators": {
            "lane_geometry_quality": {
                "status": "passed",
                "evaluator_id": "lane_geometry_quality",
                "source_kind": "machine_observation",
            }
        },
    }

    evidence, errors = gate.audit_release_golden_registry(
        hermes_home=hermes_home,
        registry=registry,
        required_evaluator_ids=["lane_geometry_quality"],
    )

    assert evidence["high_confidence_ready"] is False
    assert {item["code"] for item in errors} == {
        "pnc_release_golden_machine_observation_not_golden"
    }


def test_active_evaluator_inventory_binding_is_required_and_digest_bound(
    tmp_path: Path,
):
    hermes_home = tmp_path / ".hermes"
    runtime = hermes_home / "runtime"
    runtime.mkdir(parents=True)
    commit = "a" * 40
    tree = "b" * 40

    _write_pipeline_manifest(
        runtime,
        commit=commit,
        tree=tree,
        evaluator_ids=None,
    )
    evidence, evaluator_ids, errors = gate.audit_active_evaluator_inventory(
        hermes_home=hermes_home
    )
    assert evidence["valid"] is False
    assert evaluator_ids == ()
    assert errors[0]["code"] == (
        "pnc_release_golden_active_evaluator_inventory_invalid"
    )
    assert "binding_missing" in errors[0]["reasons"]

    _write_pipeline_manifest(
        runtime,
        commit=commit,
        tree=tree,
        evaluator_ids=[],
    )
    evidence, evaluator_ids, errors = gate.audit_active_evaluator_inventory(
        hermes_home=hermes_home
    )
    assert evidence["valid"] is False
    assert evaluator_ids == ()
    assert "required_evaluator_ids_empty" in errors[0]["reasons"]

    _write_pipeline_manifest(
        runtime,
        commit=commit,
        tree=tree,
        evaluator_ids=["lane_geometry_quality"],
        binding_updates={"inventory_sha256": "d" * 64},
    )
    _evidence, _evaluator_ids, errors = gate.audit_active_evaluator_inventory(
        hermes_home=hermes_home
    )
    assert "inventory_sha256_mismatch" in errors[0]["reasons"]

    _write_pipeline_manifest(
        runtime,
        commit=commit,
        tree=tree,
        evaluator_ids=["lane_geometry_quality"],
        binding_updates={"pipeline_tree": "e" * 40},
    )
    _evidence, _evaluator_ids, errors = gate.audit_active_evaluator_inventory(
        hermes_home=hermes_home
    )
    assert "pipeline_tree_mismatch" in errors[0]["reasons"]

    _write_pipeline_manifest(
        runtime,
        commit=commit,
        tree=tree,
        evaluator_ids=["lane_geometry_quality", "acc_decel_heavy"],
    )
    evidence, evaluator_ids, errors = gate.audit_active_evaluator_inventory(
        hermes_home=hermes_home
    )
    assert errors == []
    assert evidence["valid"] is True
    assert evaluator_ids == ("acc_decel_heavy", "lane_geometry_quality")


def test_run_gate_passes_manifest_inventory_to_golden_audit(
    tmp_path: Path, monkeypatch
):
    home = tmp_path
    hermes_home = home / ".hermes"
    runtime = hermes_home / "runtime"
    runtime.mkdir(parents=True)
    expected_ids = ["acc_decel_heavy", "lane_geometry_quality"]
    _write_pipeline_manifest(
        runtime,
        commit="a" * 40,
        tree="b" * 40,
        evaluator_ids=expected_ids,
    )

    monkeypatch.setattr(gate, "audit_persisted_definitions", lambda **_kwargs: ([], []))
    monkeypatch.setattr(gate, "audit_loaded_definitions", lambda **_kwargs: ([], []))
    monkeypatch.setattr(gate, "_resolve_targets", lambda **_kwargs: ({}, []))
    monkeypatch.setattr(gate, "audit_residents", lambda **_kwargs: ([], []))
    monkeypatch.setattr(gate, "audit_wrappers", lambda *_args: ([], []))
    monkeypatch.setattr(
        gate, "audit_retired_stable_entrypoints", lambda *_args: ([], [])
    )
    monkeypatch.setattr(gate, "audit_stable_targets", lambda **_kwargs: ([], []))
    monkeypatch.setattr(
        gate,
        "audit_delivery_store_schema",
        lambda **_kwargs: ({"schema_valid": True}, []),
    )
    monkeypatch.setattr(
        gate,
        "audit_unresolved_effect_schema_compatibility",
        lambda **_kwargs: ({"compatible": True}, []),
    )
    captured: dict[str, tuple[str, ...]] = {}

    def golden_audit(*, hermes_home: Path, required_evaluator_ids):
        assert hermes_home == home / ".hermes"
        captured["ids"] = tuple(required_evaluator_ids)
        return {"inventory_binding_valid": True}, []

    monkeypatch.setattr(gate, "audit_release_golden_registry", golden_audit)

    result = gate.run_gate(home=home, hermes_home=hermes_home)

    assert result["ok"] is True
    assert captured["ids"] == tuple(sorted(expected_ids))
    assert result["active_evaluator_inventory"]["valid"] is True


def test_stable_target_audit_resolves_every_hash_bound_target(tmp_path: Path):
    expected = {
        label
        for label, (kind, _relative) in gate.SERVICE_TARGETS.items()
        if kind in {"governance_tool", "runtime_file"}
    }
    failed = sorted(expected)[0]

    def resolver(*, manifest_path, hermes_home, service_label):
        assert manifest_path == hermes_home / "runtime" / "LIVE_MANIFEST.json"
        if service_label == failed:
            raise gate.LiveExecError("active_runtime_stable_target_mismatch")
        return {
            "script": f"/stable/{service_label}.py",
            "script_sha256": "a" * 64,
            "runtime_commit": "b" * 40,
        }

    evidence, errors = gate.audit_stable_targets(
        hermes_home=tmp_path,
        runtime_resolver=resolver,
    )

    assert {item["label"] for item in evidence} == expected
    assert errors == [
        {
            "code": "active_runtime_stable_target_mismatch",
            "label": failed,
        }
    ]


def test_release_preflight_rejects_only_unresolved_incompatible_effects(
    tmp_path: Path,
):
    db_path = tmp_path / "control.sqlite3"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE rca_delivery_effects (
            effect_key TEXT PRIMARY KEY,
            outcome TEXT NOT NULL,
            status TEXT NOT NULL,
            payload_json TEXT NOT NULL
        )
        """
    )
    rows = [
        (
            "legacy-v2",
            "success",
            "pending",
            {"schema_version": "pnc_rca_delivery_effect_v2"},
        ),
        (
            "current-v3",
            "success",
            "claimed",
            {"schema_version": gate.DELIVERY_EFFECT_SCHEMA_VERSION},
        ),
        (
            "adjudication",
            "success",
            "retry_wait",
            {"schema_version": gate.ADJUDICATION_EFFECT_SCHEMA_VERSION},
        ),
        (
            "terminal-v1",
            "terminal_failed",
            "uncertain",
            {"schema_version": gate.TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_V1},
        ),
        (
            "terminal-fallback-v3",
            "terminal_failed",
            "pending",
            {"schema_version": gate.TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION},
        ),
    ]
    conn.executemany(
        "INSERT INTO rca_delivery_effects VALUES (?, ?, ?, ?)",
        [
            (key, outcome, status, json.dumps(payload))
            for key, outcome, status, payload in rows
        ],
    )
    conn.commit()
    conn.close()

    evidence, errors = gate.audit_unresolved_effect_schema_compatibility(
        hermes_home=tmp_path,
        control_db_path=db_path,
    )

    assert evidence["unresolved_effect_count"] == 5
    assert evidence["incompatible_effect_count"] == 1
    assert evidence["incompatible_effect_keys"] == ["legacy-v2"]
    assert {item["code"] for item in errors} == {
        "pnc_release_unresolved_effect_schema_incompatible"
    }

    conn = sqlite3.connect(db_path)
    conn.execute(
        "UPDATE rca_delivery_effects SET status = 'succeeded' WHERE effect_key = 'legacy-v2'"
    )
    conn.commit()
    conn.close()
    evidence, errors = gate.audit_unresolved_effect_schema_compatibility(
        hermes_home=tmp_path,
        control_db_path=db_path,
    )
    assert evidence["unresolved_effect_count"] == 4
    assert evidence["incompatible_effect_count"] == 0
    assert errors == []


def _delivery_store_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_release_preflight_accepts_complete_v9_delivery_store_without_writes(
    tmp_path: Path,
):
    db_path = tmp_path / "control.sqlite3"
    RcaControlStore(db_path)
    RcaDeliveryStore(db_path)
    before_sha256 = _delivery_store_sha256(db_path)

    evidence, errors = gate.audit_delivery_store_schema(
        hermes_home=tmp_path,
        control_db_path=db_path,
    )

    assert errors == []
    assert evidence["schema_valid"] is True
    assert evidence["observed_control_schema_version"] == CONTROL_STORE_SCHEMA_VERSION
    assert evidence["observed_schema_version"] == "pnc_rca_delivery_store_v9"
    assert evidence["quick_check"] == "ok"
    assert evidence["integrity_check"] == "ok"
    assert evidence["foreign_key_violation_count"] == 0
    assert evidence["canonical_object_count"] > 0
    assert _delivery_store_sha256(db_path) == before_sha256


def test_release_preflight_rejects_v8_delivery_store_marker_without_writes(
    tmp_path: Path,
):
    db_path = tmp_path / "control.sqlite3"
    RcaControlStore(db_path)
    RcaDeliveryStore(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE rca_delivery_meta SET value = 'pnc_rca_delivery_store_v8' "
            "WHERE key = 'schema_version'"
        )
    before_sha256 = _delivery_store_sha256(db_path)

    evidence, errors = gate.audit_delivery_store_schema(
        hermes_home=tmp_path,
        control_db_path=db_path,
    )

    assert evidence["schema_valid"] is False
    assert evidence["observed_schema_version"] == "pnc_rca_delivery_store_v8"
    assert errors == [
        {
            "code": "pnc_release_delivery_store_schema_not_current",
            "reason": "delivery_store_combined_migration_target_schema_invalid",
        }
    ]
    assert _delivery_store_sha256(db_path) == before_sha256


def test_release_preflight_rejects_missing_immutable_trigger_without_writes(
    tmp_path: Path,
):
    db_path = tmp_path / "control.sqlite3"
    RcaControlStore(db_path)
    RcaDeliveryStore(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("DROP TRIGGER trg_rca_conclusion_adjudication_no_delete")
    before_sha256 = _delivery_store_sha256(db_path)

    evidence, errors = gate.audit_delivery_store_schema(
        hermes_home=tmp_path,
        control_db_path=db_path,
    )

    assert evidence["schema_valid"] is False
    assert evidence["observed_schema_version"] == "pnc_rca_delivery_store_v9"
    assert errors == [
        {
            "code": "pnc_release_delivery_store_schema_not_current",
            "reason": (
                "delivery_store_combined_migration_target_schema_contract_invalid"
            ),
        }
    ]
    assert _delivery_store_sha256(db_path) == before_sha256


def test_release_preflight_rejects_wrong_cross_store_migration_order_without_writes(
    tmp_path: Path,
):
    db_path = tmp_path / "control.sqlite3"
    # This reproduces the old offline rehearsal: delivery v9 is installed
    # before the control schema reaches current, so the W6 cross-table triggers
    # do not exist.
    RcaDeliveryStore(db_path)
    RcaControlStore(db_path)
    before_sha256 = _delivery_store_sha256(db_path)

    evidence, errors = gate.audit_delivery_store_schema(
        hermes_home=tmp_path,
        control_db_path=db_path,
    )

    assert evidence["observed_control_schema_version"] == CONTROL_STORE_SCHEMA_VERSION
    assert evidence["observed_schema_version"] == "pnc_rca_delivery_store_v9"
    assert evidence["schema_valid"] is False
    assert errors == [
        {
            "code": "pnc_release_delivery_store_schema_not_current",
            "reason": (
                "incompatible_delivery_store_schema:w6_trigger:"
                "trg_learning_lane_effect_insert_forbidden"
            ),
        }
    ]
    assert _delivery_store_sha256(db_path) == before_sha256

    # The governed migration sequence reopens delivery after the control schema
    # reaches current so it can install and validate the cross-table guards.
    RcaDeliveryStore(db_path)
    recovered_sha256 = _delivery_store_sha256(db_path)
    evidence, errors = gate.audit_delivery_store_schema(
        hermes_home=tmp_path,
        control_db_path=db_path,
    )
    assert errors == []
    assert evidence["schema_valid"] is True
    assert _delivery_store_sha256(db_path) == recovered_sha256


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
