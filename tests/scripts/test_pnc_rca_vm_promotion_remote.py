from __future__ import annotations

import hashlib
import os
import subprocess
from pathlib import Path

import pytest

from scripts import pnc_rca_vm_promotion_remote as remote


def _git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(root), *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _init_repo(root: Path, *, entrypoint: str, content: str | None) -> str:
    root.mkdir()
    _git(root, "init", "-q")
    _git(root, "config", "user.email", "rca-test@example.com")
    _git(root, "config", "user.name", "RCA Test")
    (root / ".gitignore").write_text("build/\n", encoding="utf-8")
    if content is not None:
        target = root / entrypoint
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    (root / "live_state.py").write_text("STATE = 'clean'\n", encoding="utf-8")
    _git(root, "add", ".")
    _git(root, "commit", "-qm", "initial")
    return _git(root, "rev-parse", "HEAD")


def _candidate_from(target: Path, candidate: Path, *, entrypoint: str) -> tuple[str, str]:
    _git(target.parent, "clone", "-q", str(target), str(candidate))
    _git(candidate, "config", "user.email", "rca-test@example.com")
    _git(candidate, "config", "user.name", "RCA Test")
    candidate_entrypoint = candidate / entrypoint
    candidate_entrypoint.parent.mkdir(parents=True, exist_ok=True)
    candidate_entrypoint.write_text("new entrypoint\n", encoding="utf-8")
    (candidate / "new_module.py").write_text("VALUE = 2\n", encoding="utf-8")
    _git(candidate, "add", ".")
    _git(candidate, "commit", "-qm", "candidate")
    _git(candidate, "checkout", "-q", "--detach", "HEAD")
    return _git(candidate, "rev-parse", "HEAD"), _git(
        candidate, "rev-parse", "HEAD^{tree}"
    )


def _component(
    *, name: str, target: Path, candidate: Path, entrypoint: str
) -> dict:
    commit = _git(candidate, "rev-parse", "HEAD")
    tree = _git(candidate, "rev-parse", "HEAD^{tree}")
    entrypoint_sha = hashlib.sha256((candidate / entrypoint).read_bytes()).hexdigest()
    artifact = candidate / "build" / "bin" / "topic_extract"
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_bytes(b"locked runtime artifact")
    return {
        "name": name,
        "candidate_root": str(candidate),
        "target_root": str(target),
        "desired_commit": commit,
        "desired_tree": tree,
        "entrypoint_relative": entrypoint,
        "entrypoint_sha256": entrypoint_sha,
        "runtime_artifacts": [
            {
                "relative_path": "build/bin/topic_extract",
                "sha256": hashlib.sha256(artifact.read_bytes()).hexdigest(),
                "size": artifact.stat().st_size,
            }
        ],
    }


def _request(tmp_path: Path, components: list[dict]) -> dict:
    return {
        "schema_version": remote.REQUEST_SCHEMA_VERSION,
        "mode": "observe",
        "release_id": "rca-vm-promotion-test",
        "components": components,
        "service_mode": "none",
        "remote_work_root": str(tmp_path / "work"),
        "lock_path": str(tmp_path / "promotion.lock"),
    }


def test_apply_promotes_tracked_closure_and_locked_runtime_artifact(
    tmp_path: Path, monkeypatch
):
    monkeypatch.setenv("PNC_RCA_VM_PROMOTION_TEST_MODE", "1")
    target = tmp_path / "target"
    candidate = tmp_path / "candidate"
    entrypoint = "bin/service.py"
    old = _init_repo(target, entrypoint=entrypoint, content="old entrypoint\n")
    desired, _tree = _candidate_from(target, candidate, entrypoint=entrypoint)
    (target / "build" / "bin").mkdir(parents=True)
    (target / "build" / "bin" / "topic_extract").write_bytes(b"old runtime")
    component = _component(
        name="vm", target=target, candidate=candidate, entrypoint=entrypoint
    )
    request = _request(tmp_path, [component])
    observed = remote.observe(request)
    request.update(
        mode="apply",
        expected_observation_sha256=hashlib.sha256(
            remote._canonical_json(observed)
        ).hexdigest(),
    )

    receipt = remote.apply(request)

    assert old != desired
    assert receipt["ok"] is True
    assert _git(target, "rev-parse", "HEAD") == desired
    assert _git(target, "status", "--porcelain=v1", "--untracked-files=all") == ""
    assert (target / entrypoint).read_text(encoding="utf-8") == "new entrypoint\n"
    assert (target / "new_module.py").read_text(encoding="utf-8") == "VALUE = 2\n"
    assert (target / "build" / "bin" / "topic_extract").read_bytes() == (
        b"locked runtime artifact"
    )

    request.update(
        mode="rollback",
        remote_receipt_path=receipt["receipt_path"],
        remote_receipt_sha256=receipt["receipt_sha256"],
    )
    rolled_back = remote.rollback(request)

    assert rolled_back["rollback_complete"] is True
    assert _git(target, "rev-parse", "HEAD") == old
    assert _git(target, "status", "--porcelain=v1", "--untracked-files=all") == ""
    assert (target / entrypoint).read_text(encoding="utf-8") == "old entrypoint\n"
    assert (target / "new_module.py").exists() is False
    assert (target / "build" / "bin" / "topic_extract").read_bytes() == b"old runtime"


def test_apply_preserves_identical_runtime_artifact_without_rewriting(
    tmp_path: Path, monkeypatch
):
    monkeypatch.setenv("PNC_RCA_VM_PROMOTION_TEST_MODE", "1")
    target = tmp_path / "target"
    candidate = tmp_path / "candidate"
    entrypoint = "bin/service.py"
    old = _init_repo(target, entrypoint=entrypoint, content="old entrypoint\n")
    desired, _tree = _candidate_from(target, candidate, entrypoint=entrypoint)
    component = _component(
        name="vm", target=target, candidate=candidate, entrypoint=entrypoint
    )
    target_artifact = target / "build" / "bin" / "topic_extract"
    target_artifact.parent.mkdir(parents=True)
    target_artifact.write_bytes(b"locked runtime artifact")
    original_write = remote._atomic_write

    def reject_runtime_rewrite(path, payload, mode):
        if path == target_artifact:
            raise PermissionError("runtime directory is read-only")
        return original_write(path, payload, mode)

    monkeypatch.setattr(remote, "_atomic_write", reject_runtime_rewrite)
    request = _request(tmp_path, [component])
    observed = remote.observe(request)
    request.update(
        mode="apply",
        expected_observation_sha256=hashlib.sha256(
            remote._canonical_json(observed)
        ).hexdigest(),
    )

    receipt = remote.apply(request)

    assert _git(target, "rev-parse", "HEAD") == desired
    assert target_artifact.read_bytes() == b"locked runtime artifact"
    request.update(
        mode="rollback",
        remote_receipt_path=receipt["receipt_path"],
        remote_receipt_sha256=receipt["receipt_sha256"],
    )
    remote.rollback(request)
    assert _git(target, "rev-parse", "HEAD") == old
    assert _git(target, "status", "--porcelain=v1", "--untracked-files=all") == ""
    assert target_artifact.read_bytes() == b"locked runtime artifact"


def test_runtime_artifact_write_failure_precedes_source_materialization(
    tmp_path: Path, monkeypatch
):
    monkeypatch.setenv("PNC_RCA_VM_PROMOTION_TEST_MODE", "1")
    target = tmp_path / "target"
    candidate = tmp_path / "candidate"
    entrypoint = "bin/service.py"
    old = _init_repo(target, entrypoint=entrypoint, content="old entrypoint\n")
    _candidate_from(target, candidate, entrypoint=entrypoint)
    component = _component(
        name="vm", target=target, candidate=candidate, entrypoint=entrypoint
    )
    target_artifact = target / "build" / "bin" / "topic_extract"
    target_artifact.parent.mkdir(parents=True)
    target_artifact.write_bytes(b"old runtime")
    original_write = remote._atomic_write

    def reject_runtime_write(path, payload, mode):
        if path == target_artifact:
            assert (target / entrypoint).read_text(encoding="utf-8") == (
                "old entrypoint\n"
            )
            assert (target / "new_module.py").exists() is False
            raise PermissionError("runtime directory is read-only")
        return original_write(path, payload, mode)

    monkeypatch.setattr(remote, "_atomic_write", reject_runtime_write)
    request = _request(tmp_path, [component])
    observed = remote.observe(request)
    request.update(
        mode="apply",
        expected_observation_sha256=hashlib.sha256(
            remote._canonical_json(observed)
        ).hexdigest(),
    )

    with pytest.raises(PermissionError, match="runtime directory is read-only"):
        remote.apply(request)

    assert _git(target, "rev-parse", "HEAD") == old
    assert _git(target, "status", "--porcelain=v1", "--untracked-files=all") == ""
    assert (target / entrypoint).read_text(encoding="utf-8") == "old entrypoint\n"
    assert (target / "new_module.py").exists() is False
    assert target_artifact.read_bytes() == b"old runtime"


def test_apply_and_rollback_support_first_install_entrypoint(
    tmp_path: Path, monkeypatch
):
    monkeypatch.setenv("PNC_RCA_VM_PROMOTION_TEST_MODE", "1")
    target = tmp_path / "target"
    candidate = tmp_path / "candidate"
    entrypoint = "bin/service.py"
    old = _init_repo(target, entrypoint=entrypoint, content=None)
    desired, _tree = _candidate_from(target, candidate, entrypoint=entrypoint)
    component = _component(
        name="vm", target=target, candidate=candidate, entrypoint=entrypoint
    )
    request = _request(tmp_path, [component])

    observed = remote.observe(request)

    assert observed["components"]["vm"]["target"]["entrypoint"] == {
        "relative_path": entrypoint,
        "path": str(target / entrypoint),
        "state": "absent",
    }
    request.update(
        mode="apply",
        expected_observation_sha256=hashlib.sha256(
            remote._canonical_json(observed)
        ).hexdigest(),
    )
    receipt = remote.apply(request)

    assert _git(target, "rev-parse", "HEAD") == desired
    assert (target / entrypoint).read_text(encoding="utf-8") == "new entrypoint\n"

    request.update(
        mode="rollback",
        remote_receipt_path=receipt["receipt_path"],
        remote_receipt_sha256=receipt["receipt_sha256"],
    )
    rolled_back = remote.rollback(request)

    assert rolled_back["rollback_complete"] is True
    assert _git(target, "rev-parse", "HEAD") == old
    assert _git(target, "status", "--porcelain=v1", "--untracked-files=all") == ""
    assert (target / entrypoint).exists() is False


def test_observe_rejects_untracked_target_entrypoint(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("PNC_RCA_VM_PROMOTION_TEST_MODE", "1")
    target = tmp_path / "target"
    candidate = tmp_path / "candidate"
    entrypoint = "bin/service.py"
    _init_repo(target, entrypoint=entrypoint, content=None)
    _candidate_from(target, candidate, entrypoint=entrypoint)
    target_entrypoint = target / entrypoint
    target_entrypoint.parent.mkdir(parents=True)
    target_entrypoint.write_text("untracked local entrypoint\n", encoding="utf-8")
    component = _component(
        name="vm", target=target, candidate=candidate, entrypoint=entrypoint
    )

    with pytest.raises(
        remote.VmPromotionRemoteError,
        match="vm_promotion_entrypoint_untracked",
    ):
        remote.observe(_request(tmp_path, [component]))


def test_observe_rejects_linked_worktree_candidate(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("PNC_RCA_VM_PROMOTION_TEST_MODE", "1")
    target = tmp_path / "target"
    candidate_source = tmp_path / "candidate-source"
    linked = tmp_path / "candidate-linked"
    entrypoint = "bin/service.py"
    _init_repo(target, entrypoint=entrypoint, content="old entrypoint\n")
    _candidate_from(target, candidate_source, entrypoint=entrypoint)
    _git(candidate_source, "worktree", "add", "-q", "--detach", str(linked), "HEAD")
    artifact = linked / "build" / "bin" / "topic_extract"
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_bytes(b"locked runtime artifact")
    component = _component(
        name="vm", target=target, candidate=linked, entrypoint=entrypoint
    )

    with pytest.raises(
        remote.VmPromotionRemoteError,
        match="vm_promotion_candidate_not_sealed",
    ):
        remote.observe(_request(tmp_path, [component]))


def test_git_bytes_failure_uses_stable_remote_error(tmp_path: Path):
    with pytest.raises(
        remote.VmPromotionRemoteError,
        match="vm_promotion_command_failed",
    ):
        remote._git_bytes(tmp_path, "rev-parse", "HEAD")


def test_second_component_failure_rolls_first_back_to_dirty_prestate(
    tmp_path: Path, monkeypatch
):
    monkeypatch.setenv("PNC_RCA_VM_PROMOTION_TEST_MODE", "1")
    components = []
    old_states = {}
    for name in ("vm", "worker"):
        target = tmp_path / f"{name}-target"
        candidate = tmp_path / f"{name}-candidate"
        entrypoint = "service.py"
        old = _init_repo(target, entrypoint=entrypoint, content=f"{name} old\n")
        _candidate_from(target, candidate, entrypoint=entrypoint)
        if name == "vm":
            (target / "live_state.py").write_text(
                "STATE = 'live uncommitted fix'\n", encoding="utf-8"
            )
        component = _component(
            name=name,
            target=target,
            candidate=candidate,
            entrypoint=entrypoint,
        )
        components.append(component)
        old_states[name] = {
            "root": target,
            "head": old,
            "status": _git(
                target, "status", "--porcelain=v1", "--untracked-files=all"
            ),
            "content": (target / "live_state.py").read_bytes(),
        }
    components[1]["entrypoint_sha256"] = "0" * 64
    request = _request(tmp_path, components)
    observed = remote.observe(request)
    request.update(
        mode="apply",
        expected_observation_sha256=hashlib.sha256(
            remote._canonical_json(observed)
        ).hexdigest(),
    )

    with pytest.raises(remote.VmPromotionRemoteError, match="candidate_drift"):
        remote.apply(request)

    first = old_states["vm"]
    assert _git(first["root"], "rev-parse", "HEAD") == first["head"]
    assert _git(
        first["root"], "status", "--porcelain=v1", "--untracked-files=all"
    ) == first["status"]
    assert (first["root"] / "live_state.py").read_bytes() == first["content"]


def test_remote_receipt_write_failure_restores_promoted_prestate(
    tmp_path: Path, monkeypatch
):
    monkeypatch.setenv("PNC_RCA_VM_PROMOTION_TEST_MODE", "1")
    target = tmp_path / "target"
    candidate = tmp_path / "candidate"
    entrypoint = "bin/service.py"
    old = _init_repo(target, entrypoint=entrypoint, content="old entrypoint\n")
    _candidate_from(target, candidate, entrypoint=entrypoint)
    (target / "live_state.py").write_text(
        "STATE = 'live uncommitted fix'\n", encoding="utf-8"
    )
    old_status = _git(target, "status", "--porcelain=v1", "--untracked-files=all")
    old_content = (target / "live_state.py").read_bytes()
    component = _component(
        name="vm", target=target, candidate=candidate, entrypoint=entrypoint
    )
    request = _request(tmp_path, [component])
    observed = remote.observe(request)
    request.update(
        mode="apply",
        expected_observation_sha256=hashlib.sha256(
            remote._canonical_json(observed)
        ).hexdigest(),
    )
    original_write = remote._atomic_write

    def fail_receipt(path, payload, mode):
        if path.name == "remote-receipt.json":
            raise OSError("receipt disk fault")
        return original_write(path, payload, mode)

    monkeypatch.setattr(remote, "_atomic_write", fail_receipt)

    with pytest.raises(OSError, match="receipt disk fault"):
        remote.apply(request)

    assert _git(target, "rev-parse", "HEAD") == old
    assert _git(
        target, "status", "--porcelain=v1", "--untracked-files=all"
    ) == old_status
    assert (target / "live_state.py").read_bytes() == old_content


def test_apply_rechecks_prestate_after_scheduler_stop(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("PNC_RCA_VM_PROMOTION_TEST_MODE", "1")
    target = tmp_path / "target"
    candidate = tmp_path / "candidate"
    entrypoint = "bin/service.py"
    old = _init_repo(target, entrypoint=entrypoint, content="old entrypoint\n")
    _candidate_from(target, candidate, entrypoint=entrypoint)
    component = _component(
        name="vm", target=target, candidate=candidate, entrypoint=entrypoint
    )
    request = _request(tmp_path, [component])
    observed = remote.observe(request)
    request.update(
        mode="apply",
        expected_observation_sha256=hashlib.sha256(
            remote._canonical_json(observed)
        ).hexdigest(),
    )
    original_stop = remote._stop_service
    first = True

    def stop_with_concurrent_change(mode, before):
        nonlocal first
        original_stop(mode, before)
        if first:
            first = False
            (target / "live_state.py").write_text(
                "STATE = 'concurrent change'\n", encoding="utf-8"
            )

    monkeypatch.setattr(remote, "_stop_service", stop_with_concurrent_change)

    with pytest.raises(
        remote.VmPromotionRemoteError,
        match="prestate_drift_after_service_stop",
    ):
        remote.apply(request)

    assert _git(target, "rev-parse", "HEAD") == old
    assert (target / entrypoint).read_text(encoding="utf-8") == "old entrypoint\n"


def test_rollback_rejects_post_promotion_repo_drift_before_restore(
    tmp_path: Path, monkeypatch
):
    monkeypatch.setenv("PNC_RCA_VM_PROMOTION_TEST_MODE", "1")
    target = tmp_path / "target"
    candidate = tmp_path / "candidate"
    entrypoint = "bin/service.py"
    _init_repo(target, entrypoint=entrypoint, content="old entrypoint\n")
    desired, _tree = _candidate_from(target, candidate, entrypoint=entrypoint)
    component = _component(
        name="vm", target=target, candidate=candidate, entrypoint=entrypoint
    )
    request = _request(tmp_path, [component])
    observed = remote.observe(request)
    request.update(
        mode="apply",
        expected_observation_sha256=hashlib.sha256(
            remote._canonical_json(observed)
        ).hexdigest(),
    )
    receipt = remote.apply(request)
    (target / "live_state.py").write_text(
        "STATE = 'post-promotion concurrent change'\n", encoding="utf-8"
    )
    request.update(
        mode="rollback",
        remote_receipt_path=receipt["receipt_path"],
        remote_receipt_sha256=receipt["receipt_sha256"],
    )

    with pytest.raises(
        remote.VmPromotionRemoteError,
        match="rollback_target_drift",
    ):
        remote.rollback(request)

    assert _git(target, "rev-parse", "HEAD") == desired
    assert "post-promotion concurrent change" in (
        target / "live_state.py"
    ).read_text(encoding="utf-8")
