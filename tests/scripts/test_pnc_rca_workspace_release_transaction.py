from __future__ import annotations

import json
import os
from pathlib import Path
import sys

import pytest

from gateway import pnc_rca_workspace_runtime as workspace_runtime
from scripts import pnc_rca_release_transaction as base
from scripts import pnc_rca_workspace_release_transaction as transaction
from tests.scripts.test_pnc_rca_steady_release_transaction import (
    _install_activation_fixture,
)


PREDECESSOR_COMMIT = "1" * 40
SUCCESSOR_COMMIT = "2" * 40
SOURCE_FILES = {
    "bin/create_task_v2.py": b"from shared_state_v2 import create_task_main\n",
    "bin/shared_state_v2.py": b"from shared_state_fields import enforce_task_fields\n",
    "bin/shared_state_fields.py": b"def enforce_task_fields():\n    return None\n",
}


def _write(path: Path, raw: bytes, mode: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    path.chmod(mode)


def _bundle(root: Path, *, commit: str, suffix: bytes) -> dict[str, object]:
    (root / "bin").mkdir(parents=True, mode=0o700)
    root.chmod(0o700)
    (root / "bin").chmod(0o700)
    descriptors = {}
    for index, relative in enumerate(workspace_runtime.WORKSPACE_RUNTIME_FILES, start=1):
        raw = SOURCE_FILES[relative] + suffix
        _write(root / relative, raw, workspace_runtime.WORKSPACE_RUNTIME_FILE_MODES[relative])
        descriptors[relative] = workspace_runtime.workspace_runtime_descriptor(
            path=relative,
            raw=raw,
            git_blob_oid=f"{index:x}" * 40,
        )
    manifest = workspace_runtime.build_workspace_runtime_manifest(
        source_commit=commit,
        files=descriptors,
    )
    _write(root / "manifest.json", base._pretty(manifest), 0o600)
    return transaction._identity(workspace_runtime.validate_staged_workspace_runtime(root))


def _live_manifest(path: Path, identity: dict[str, object]) -> None:
    value = {
        "gateway_release_binding": {
            "workspace_runtime_source_commit": identity["source_commit"],
            "workspace_runtime_manifest_sha256": identity["manifest_sha256"],
            "workspace_runtime_closure_sha256": identity["closure_sha256"],
        },
        "production_branch_bindings": {
            "workspace_runtime": {
                "branch": "DETACHED",
                "commit": identity["source_commit"],
            }
        },
    }
    _write(path, base._pretty(value), 0o600)


def _fixture(tmp_path: Path) -> dict[str, Path]:
    hermes_home = tmp_path / "hermes"
    state_root = hermes_home / "runtime/pnc_agent/feishu_issue_kafka_rca"
    state_root.mkdir(parents=True, mode=0o700)
    control_db = state_root / "control.sqlite3"
    _install_activation_fixture({"control_db": control_db})

    candidate_root = tmp_path / "candidate"
    candidate_root.mkdir(mode=0o700)
    predecessor = _bundle(
        workspace_runtime.canonical_workspace_runtime_root(hermes_home),
        commit=PREDECESSOR_COMMIT,
        suffix=b"# predecessor\n",
    )
    successor = _bundle(
        candidate_root / "workspace-runtime",
        commit=SUCCESSOR_COMMIT,
        suffix=b"# successor\n",
    )
    _live_manifest(hermes_home / "runtime/LIVE_MANIFEST.json", predecessor)
    _live_manifest(candidate_root / "LIVE_MANIFEST.json", successor)
    evidence_root = tmp_path / "evidence"
    evidence_root.mkdir(mode=0o700)
    return {
        "candidate_root": candidate_root,
        "hermes_home": hermes_home,
        "state_root": state_root,
        "control_db": control_db,
        "evidence_root": evidence_root,
    }


def _build(paths: dict[str, Path]):
    return transaction.build_plan(
        candidate_root=paths["candidate_root"],
        hermes_home=paths["hermes_home"],
        control_db=paths["control_db"],
        evidence_root=paths["evidence_root"],
        transaction_id="workspace-test-transaction",
    )


def _test_swap(left: Path, right: Path) -> None:
    temporary = left.parent / ".workspace-test-swap"
    os.rename(left, temporary)
    os.rename(right, left)
    os.rename(temporary, right)


def _platform_swap():
    return None if sys.platform == "darwin" else _test_swap


def _live_identity(paths: dict[str, Path]) -> dict[str, object]:
    return transaction._identity(
        workspace_runtime.validate_workspace_runtime(hermes_home=paths["hermes_home"])
    )


def test_plan_prepares_exact_successor_without_touching_live(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    before = _live_identity(paths)

    plan, plan_path = _build(paths)

    assert plan_path.stat().st_mode & 0o777 == 0o600
    assert plan["mutation_performed"] is False
    assert plan["predecessor_identity"] == before
    assert transaction._validate_staged(
        Path(plan["prepared_workspace_root"]), code="test"
    ) == plan["successor_identity"]
    assert _live_identity(paths) == before
    assert plan["atomic_swap"] == {
        "platform": "darwin",
        "primitive": "renameatx_np",
        "flag": "RENAME_SWAP",
        "no_overwrite_window": True,
    }


def test_apply_and_receipt_bound_rollback_exchange_exact_bundles(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    plan, plan_path = _build(paths)

    receipt = transaction.apply_plan(
        plan, plan_path=plan_path, swap_func=_platform_swap()
    )

    assert _live_identity(paths) == plan["successor_identity"]
    assert transaction._validate_staged(
        Path(plan["prepared_workspace_root"]), code="test"
    ) == plan["predecessor_identity"]
    assert receipt["production_effects"] == transaction._effects()
    assert not (paths["state_root"] / transaction.steady.LOCK_NAME).exists()

    output = paths["evidence_root"] / "manual-rollback.json"
    rollback = transaction.rollback_transaction(
        Path(receipt["receipt_path"]), output_path=output, swap_func=_platform_swap()
    )

    assert rollback["restored_to_pre_transaction"] is True
    assert rollback["production_effects"] == transaction._effects()
    assert _live_identity(paths) == plan["predecessor_identity"]
    assert transaction._validate_staged(
        Path(plan["prepared_workspace_root"]), code="test"
    ) == plan["successor_identity"]
    assert output.stat().st_mode & 0o777 == 0o600


def test_candidate_manifest_mismatch_is_rejected_before_transaction_creation(
    tmp_path: Path,
) -> None:
    paths = _fixture(tmp_path)
    manifest_path = paths["candidate_root"] / "LIVE_MANIFEST.json"
    value = json.loads(manifest_path.read_text())
    value["gateway_release_binding"]["workspace_runtime_closure_sha256"] = "f" * 64
    _write(manifest_path, base._pretty(value), 0o600)

    with pytest.raises(
        transaction.WorkspaceReleaseTransactionError,
        match="candidate_manifest_invalid",
    ):
        _build(paths)

    assert list(paths["evidence_root"].iterdir()) == []


def test_post_swap_activation_drift_automatically_swaps_back(tmp_path: Path, monkeypatch):
    paths = _fixture(tmp_path)
    plan, plan_path = _build(paths)
    calls = 0

    def activation(_control_db: Path):
        nonlocal calls
        calls += 1
        if calls == 1:
            return dict(plan["activation_binding"])
        drifted = dict(plan["activation_binding"])
        drifted["binding_fingerprint"] = "e" * 64
        return drifted

    monkeypatch.setattr(transaction, "_activation_binding", activation)

    with pytest.raises(
        transaction.WorkspaceReleaseTransactionError,
        match="activation_changed",
    ):
        transaction.apply_plan(plan, plan_path=plan_path, swap_func=_test_swap)

    assert _live_identity(paths) == plan["predecessor_identity"]
    assert transaction._validate_staged(
        Path(plan["prepared_workspace_root"]), code="test"
    ) == plan["successor_identity"]
    automatic = json.loads(
        (Path(plan["transaction_dir"]) / "automatic-rollback.json").read_text()
    )
    assert automatic["restored_to_pre_transaction"] is True


def test_apply_parent_sync_failure_automatically_swaps_back(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _fixture(tmp_path)
    plan, plan_path = _build(paths)
    original_sync = transaction._sync_swap_parents
    calls = 0

    def fail_first_sync(left: Path, right: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("injected parent sync failure")
        original_sync(left, right)

    monkeypatch.setattr(transaction, "_sync_swap_parents", fail_first_sync)

    with pytest.raises(OSError, match="injected parent sync failure"):
        transaction.apply_plan(plan, plan_path=plan_path, swap_func=_test_swap)

    assert calls == 2
    assert _live_identity(paths) == plan["predecessor_identity"]
    assert transaction._validate_staged(
        Path(plan["prepared_workspace_root"]), code="test"
    ) == plan["successor_identity"]
    automatic = json.loads(
        (Path(plan["transaction_dir"]) / "automatic-rollback.json").read_text()
    )
    assert automatic["restored_to_pre_transaction"] is True
    assert not (Path(plan["transaction_dir"]) / "receipt.json").exists()


def test_rollback_parent_sync_failure_restores_applied_positions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _fixture(tmp_path)
    plan, plan_path = _build(paths)
    receipt = transaction.apply_plan(plan, plan_path=plan_path, swap_func=_test_swap)
    original_sync = transaction._sync_swap_parents
    calls = 0

    def fail_first_sync(left: Path, right: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("injected rollback parent sync failure")
        original_sync(left, right)

    monkeypatch.setattr(transaction, "_sync_swap_parents", fail_first_sync)
    output = paths["evidence_root"] / "must-not-exist.json"

    with pytest.raises(OSError, match="injected rollback parent sync failure"):
        transaction.rollback_transaction(
            Path(receipt["receipt_path"]), output_path=output, swap_func=_test_swap
        )

    assert calls == 2
    assert _live_identity(paths) == plan["successor_identity"]
    assert transaction._validate_staged(
        Path(plan["prepared_workspace_root"]), code="test"
    ) == plan["predecessor_identity"]
    assert not output.exists()


def test_rollback_refuses_prepared_identity_drift_without_swapping(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    plan, plan_path = _build(paths)
    receipt = transaction.apply_plan(plan, plan_path=plan_path, swap_func=_test_swap)
    prepared = Path(plan["prepared_workspace_root"])
    _write(prepared / "bin/unexpected.py", b"pass\n", 0o644)
    output = paths["evidence_root"] / "must-not-exist.json"

    with pytest.raises(
        transaction.WorkspaceReleaseTransactionError,
        match="prepared_invalid",
    ):
        transaction.rollback_transaction(
            Path(receipt["receipt_path"]), output_path=output, swap_func=_test_swap
        )

    assert _live_identity(paths) == plan["successor_identity"]
    assert not output.exists()


@pytest.mark.skipif(sys.platform != "darwin", reason="renameatx_np is macOS-only")
def test_native_rename_swap_exchanges_two_directories(tmp_path: Path) -> None:
    left = tmp_path / "left"
    right = tmp_path / "right"
    left.mkdir()
    right.mkdir()
    (left / "value").write_text("left")
    (right / "value").write_text("right")

    transaction._rename_swap(left, right)

    assert (left / "value").read_text() == "right"
    assert (right / "value").read_text() == "left"
