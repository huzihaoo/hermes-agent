from __future__ import annotations

from pathlib import Path

from scripts import pnc_rca_git_identity_guard as guard


def test_current_repository_passes_git_identity_guard():
    repo_root = Path(__file__).resolve().parents[2]

    assert guard.audit_repository(repo_root) == []
    assert guard.REVIEWED_LEGACY_REMOTE_PROBE_LINES == {
        "scripts/pnc_rca_minimal_release.py": (972, 973, 974)
    }


def test_new_bare_git_identity_probe_is_rejected(tmp_path):
    source = tmp_path / "scripts" / "new_remote_probe.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        'subprocess.run(["git", "rev-parse", "HEAD"], check=True)\n',
        encoding="utf-8",
    )

    assert guard.bare_probe_findings(tmp_path) == [
        "scripts/new_remote_probe.py:1:undeclared_git_identity_probe"
    ]


def test_new_probe_cannot_self_declare_git_worktree(tmp_path):
    source = tmp_path / "gateway" / "new_git_worktree_probe.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "# identity-kind: git_worktree\n"
        'subprocess.run(["git", "rev-parse", "HEAD"], check=True)\n',
        encoding="utf-8",
    )

    assert guard.bare_probe_findings(tmp_path) == [
        "gateway/new_git_worktree_probe.py:2:undeclared_git_identity_probe"
    ]


def test_constant_folded_and_aliased_probe_is_rejected(tmp_path):
    source = tmp_path / "scripts" / "new_folded_probe.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        'operation = "rev" + "-parse"\n'
        'command = ["git", operation, "HEAD"]\n'
        'subprocess.run(command, check=True)\n',
        encoding="utf-8",
    )

    assert guard.bare_probe_findings(tmp_path) == [
        "scripts/new_folded_probe.py:3:undeclared_git_identity_probe"
    ]


def test_reviewed_probe_line_cannot_move_within_same_file(tmp_path):
    repo_root = Path(__file__).resolve().parents[2]
    relative = "scripts/pnc_live_exec.py"
    source = repo_root / relative
    destination = tmp_path / relative
    destination.parent.mkdir(parents=True)
    lines = source.read_text(encoding="utf-8").splitlines()
    reviewed_line = guard.REVIEWED_LINE_NUMBERS[relative][0]
    moved = lines.pop(reviewed_line - 1)
    lines.insert(reviewed_line, moved)
    destination.write_text("\n".join(lines) + "\n", encoding="utf-8")

    assert guard.bare_probe_findings(tmp_path) == [
        f"{relative}:{reviewed_line + 1}:undeclared_git_identity_probe"
    ]


def test_remote_contract_heuristic_regression_is_rejected(tmp_path):
    repo_root = Path(__file__).resolve().parents[2]
    paths = (
        "scripts/pnc_rca_delivery_collector.py",
        "scripts/pnc_rca_minimal_release.py",
        "gateway/pnc_rca_direct_vm_transport.py",
    )
    for relative in paths:
        source = repo_root / relative
        destination = tmp_path / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")

    collector = tmp_path / "scripts/pnc_rca_delivery_collector.py"
    collector.write_text(
        collector.read_text(encoding="utf-8")
        + "\ngit_marker = posixpath.join(repo_root, '.git')\n",
        encoding="utf-8",
    )

    assert (
        "collector_pipeline_identity_heuristic_present"
        in guard.remote_contract_findings(tmp_path)
    )
