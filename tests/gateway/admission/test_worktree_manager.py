#!/usr/bin/env python3
"""Tests for worktree_manager.py — owner routing, audit logging, auto-create."""

import json
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "gateway" / "admission"))
import worktree_manager


@pytest.fixture
def mock_config(tmp_path):
    """Mock config with owner and member users."""
    config = {
        "user_id_mapping": {
            "ou_huzihao": "胡子豪",
            "ou_guo": "郭艳彬",
        },
        "users": {
            "胡子豪": "owner",
            "郭艳彬": "senior",
            "default": "member",
        },
        "repo_config": {
            "worktree_base": str(tmp_path / "worktrees"),
            "repos": {
                "pnc_specs": {
                    "source": str(tmp_path / "pnc_specs"),
                    "default_branch": "main",
                },
            },
        },
    }
    config_path = tmp_path / "user-roles.json"
    config_path.write_text(json.dumps(config, ensure_ascii=False))

    source = tmp_path / "pnc_specs"
    source.mkdir()
    subprocess.run(["git", "init", "-b", "main"], cwd=source, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=source, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.email", "test@test"], cwd=source, check=True, capture_output=True)
    (source / "README.md").write_text("# pnc_specs")
    subprocess.run(["git", "add", "."], cwd=source, check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "init"], cwd=source, check=True, capture_output=True)

    return config_path


def test_owner_uses_source_repo_not_worktree(mock_config, tmp_path):
    """Owner (胡子豪) should get source repo path, not a worktree."""
    with patch.object(worktree_manager, "CONFIG_PATH", mock_config):
        result = worktree_manager.ensure_worktree("胡子豪", "pnc_specs")

    expected_path = str(tmp_path / "pnc_specs")
    assert result["path"] == expected_path
    assert result["created"] is False

    worktree_path = tmp_path / "worktrees" / "pnc_specs" / "胡子豪"
    assert not worktree_path.exists()


def test_non_owner_gets_worktree(mock_config, tmp_path):
    """Non-owner (郭艳彬) should get a worktree."""
    with patch.object(worktree_manager, "CONFIG_PATH", mock_config):
        result = worktree_manager.ensure_worktree("郭艳彬", "pnc_specs")

    expected_path = str(tmp_path / "worktrees" / "pnc_specs" / "郭艳彬")
    assert result["path"] == expected_path
    assert result["created"] is True

    worktree_path = Path(expected_path)
    assert worktree_path.exists()
    assert (worktree_path / ".git").exists()


def test_worktree_creation_logs_audit(mock_config, tmp_path, monkeypatch):
    """Creating a worktree should log to audit."""
    audit_log = tmp_path / "audit.log"
    audit_calls = []

    def mock_audit(user, repo, action):
        audit_calls.append({"user": user, "repo": repo, "action": action})
        with open(audit_log, "a") as f:
            f.write(f"{user}|{repo}|{action}\n")

    monkeypatch.setattr(worktree_manager, "log_audit", mock_audit)

    with patch.object(worktree_manager, "CONFIG_PATH", mock_config):
        worktree_manager.ensure_worktree("郭艳彬", "pnc_specs")

    assert len(audit_calls) == 1
    assert audit_calls[0]["user"] == "郭艳彬"
    assert audit_calls[0]["repo"] == "pnc_specs"
    assert "auto-create" in audit_calls[0]["action"]
    assert audit_log.exists()
    assert "郭艳彬" in audit_log.read_text()


def test_rejects_path_traversal_user(mock_config):
    with patch.object(worktree_manager, "CONFIG_PATH", mock_config):
        result = worktree_manager.ensure_worktree("../other", "pnc_specs")

    assert "error" in result
    assert "invalid user" in result["error"]


def test_rejects_path_traversal_repo(mock_config):
    with patch.object(worktree_manager, "CONFIG_PATH", mock_config):
        result = worktree_manager.ensure_worktree("郭艳彬", "../pnc_specs")

    assert "error" in result
    assert "invalid repo" in result["error"]


def test_rejects_unsafe_branch(mock_config):
    with patch.object(worktree_manager, "CONFIG_PATH", mock_config):
        result = worktree_manager.ensure_worktree("郭艳彬", "pnc_specs", branch="../../main")

    assert "error" in result
    assert "invalid branch" in result["error"]
