from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from gateway.pnc_rca_workspace_runtime import (
    WORKSPACE_RUNTIME_FILES,
    WORKSPACE_RUNTIME_MANIFEST_NAME,
    validate_staged_workspace_runtime,
)
from scripts import pnc_rca_workspace_runtime_bundle as builder


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _source(repo: Path) -> Path:
    files = {
        "bin/create_task_v2.py": (
            "#!/usr/bin/env python3\n"
            "from shared_state_v2 import create_task_main\n"
        ),
        "bin/shared_state_v2.py": (
            "#!/usr/bin/env python3\n"
            "def create_task_main():\n"
            "    from shared_state_fields import enforce_task_fields\n"
            "    return enforce_task_fields()\n"
        ),
        "bin/shared_state_fields.py": (
            "def enforce_task_fields():\n"
            "    return 0\n"
        ),
    }
    for path, body in files.items():
        target = repo / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(body, encoding="utf-8")
        target.chmod(0o755 if path != "bin/shared_state_fields.py" else 0o644)
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "test@example.invalid")
    _git(repo, "config", "user.name", "Bundle Test")
    _git(repo, "add", *WORKSPACE_RUNTIME_FILES)
    _git(repo, "commit", "-qm", "fixture")
    return repo


def test_plan_and_stage_build_exact_no_install_bundle(tmp_path: Path) -> None:
    source = _source(tmp_path / "source")
    output = tmp_path / "output"
    home = tmp_path / "hermes"

    plan = builder.run_workspace_runtime_bundle(
        phase="plan",
        source_candidate=source,
        output_dir=output,
        hermes_home=home,
    )
    staged = builder.run_workspace_runtime_bundle(
        phase="stage",
        source_candidate=source,
        output_dir=output,
        hermes_home=home,
    )

    assert plan.body["source_commit"] == _git(source, "rev-parse", "HEAD")
    assert plan.body["production_effects_executed"] is False
    assert plan.body["live_install_supported"] is False
    assert (plan.artifact_path.stat().st_mode & 0o777) == 0o600
    assert staged.artifact_path == output / "bundle"
    assert sorted(path.name for path in staged.artifact_path.iterdir()) == [
        "bin",
        WORKSPACE_RUNTIME_MANIFEST_NAME,
    ]
    assert sorted(
        f"bin/{path.name}" for path in (staged.artifact_path / "bin").iterdir()
    ) == sorted(WORKSPACE_RUNTIME_FILES)
    identity = validate_staged_workspace_runtime(staged.artifact_path)
    assert identity.source_commit == _git(source, "rev-parse", "HEAD")
    assert not (home / "runtime" / "rca-workspace-runtime").exists()


def test_stage_is_exactly_repeatable_and_no_clobber(tmp_path: Path) -> None:
    source = _source(tmp_path / "source")
    output = tmp_path / "output"
    kwargs = {
        "phase": "stage",
        "source_candidate": source,
        "output_dir": output,
        "hermes_home": tmp_path / "hermes",
    }

    first = builder.run_workspace_runtime_bundle(**kwargs)
    second = builder.run_workspace_runtime_bundle(**kwargs)

    assert first.body == second.body
    assert second.resumed is True


def test_direct_cli_bootstraps_repo_imports_from_arbitrary_cwd(tmp_path: Path) -> None:
    source = _source(tmp_path / "source")
    output = tmp_path / "output"
    script = Path(builder.__file__).resolve()

    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--phase",
            "plan",
            "--source-candidate",
            str(source),
            "--output-dir",
            str(output),
            "--hermes-home",
            str(tmp_path / "hermes"),
        ],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert payload["phase"] == "plan"
    assert payload["live_install_performed"] is False


def test_dirty_source_and_hardlinked_source_fail_closed(tmp_path: Path) -> None:
    source = _source(tmp_path / "source")
    target = source / "bin" / "shared_state_fields.py"
    target.write_text(target.read_text() + "# dirty\n", encoding="utf-8")
    with pytest.raises(builder.WorkspaceRuntimeBundleError) as dirty:
        builder.run_workspace_runtime_bundle(
            phase="plan",
            source_candidate=source,
            output_dir=tmp_path / "dirty-output",
            hermes_home=tmp_path / "hermes",
        )
    assert dirty.value.code == "rca_workspace_bundle_source_dirty"

    _git(source, "checkout", "--", "bin/shared_state_fields.py")
    os.link(target, tmp_path / "source-hardlink")
    with pytest.raises(builder.WorkspaceRuntimeBundleError) as hardlink:
        builder.run_workspace_runtime_bundle(
            phase="plan",
            source_candidate=source,
            output_dir=tmp_path / "link-output",
            hermes_home=tmp_path / "hermes",
        )
    assert hardlink.value.code == "rca_workspace_bundle_source_identity_invalid"


def test_plan_cannot_cross_source_commit(tmp_path: Path) -> None:
    source = _source(tmp_path / "source")
    output = tmp_path / "output"
    builder.run_workspace_runtime_bundle(
        phase="plan",
        source_candidate=source,
        output_dir=output,
        hermes_home=tmp_path / "hermes",
    )
    fields = source / "bin" / "shared_state_fields.py"
    fields.write_text(fields.read_text() + "# next commit\n", encoding="utf-8")
    _git(source, "add", "bin/shared_state_fields.py")
    _git(source, "commit", "-qm", "next")

    with pytest.raises(builder.WorkspaceRuntimeBundleError) as error:
        builder.run_workspace_runtime_bundle(
            phase="stage",
            source_candidate=source,
            output_dir=output,
            hermes_home=tmp_path / "hermes",
        )

    assert error.value.code == "rca_workspace_bundle_plan_conflict"
    assert not (output / "bundle").exists()


def test_clean_source_with_fourth_local_dependency_is_not_the_fixed_closure(
    tmp_path: Path,
) -> None:
    source = _source(tmp_path / "source")
    helper = source / "bin" / "local_helper.py"
    helper.write_text("VALUE = 1\n", encoding="utf-8")
    helper.chmod(0o644)
    shared = source / "bin" / "shared_state_v2.py"
    shared.write_text(
        shared.read_text(encoding="utf-8") + "\nimport local_helper\n",
        encoding="utf-8",
    )
    _git(source, "add", "bin/local_helper.py", "bin/shared_state_v2.py")
    _git(source, "commit", "-qm", "add forbidden local dependency")

    with pytest.raises(builder.WorkspaceRuntimeBundleError) as error:
        builder.run_workspace_runtime_bundle(
            phase="plan",
            source_candidate=source,
            output_dir=tmp_path / "output",
            hermes_home=tmp_path / "hermes",
        )

    assert error.value.code == "rca_workspace_bundle_import_closure_invalid"


def test_builder_refuses_canonical_root_or_ancestor_output(tmp_path: Path) -> None:
    source = _source(tmp_path / "source")
    home = tmp_path / "hermes"
    canonical = home / "runtime" / "rca-workspace-runtime"

    for output in (canonical, canonical / "nested", home / "runtime"):
        with pytest.raises(builder.WorkspaceRuntimeBundleError) as error:
            builder.run_workspace_runtime_bundle(
                phase="stage",
                source_candidate=source,
                output_dir=output,
                hermes_home=home,
            )
        assert error.value.code == "rca_workspace_bundle_live_output_forbidden"


def test_stage_conflict_does_not_replace_existing_bundle(tmp_path: Path) -> None:
    source = _source(tmp_path / "source")
    output = tmp_path / "output"
    bundle = output / "bundle"
    bundle.mkdir(parents=True, mode=0o700)
    output.chmod(0o700)
    marker = bundle / "unowned.txt"
    marker.write_text("preserve", encoding="utf-8")

    with pytest.raises(builder.WorkspaceRuntimeBundleError) as error:
        builder.run_workspace_runtime_bundle(
            phase="stage",
            source_candidate=source,
            output_dir=output,
            hermes_home=tmp_path / "hermes",
        )

    assert error.value.code == "rca_workspace_bundle_stage_conflict"
    assert marker.read_text() == "preserve"
