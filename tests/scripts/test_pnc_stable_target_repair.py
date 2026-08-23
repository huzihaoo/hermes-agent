from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

import pytest

from scripts import pnc_stable_target_repair as repair


EXPECTED_HELPER_SHA256 = (
    "3865ac20b104664fe6e8e6aa20c5509f44edabe95f896770ed66ade993104483"
)
EXPECTED_HELPER_SIZE = 14702


def _write(path: Path, raw: bytes, *, mode: int) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.write_bytes(raw)
    path.chmod(mode)


def _json(value: dict[str, Any]) -> bytes:
    return (
        json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    ).encode()


def _registry(*, sha256: str, size: int) -> bytes:
    return _json({
        "schema_version": repair.STABLE_TARGET_SCHEMA_VERSION,
        "targets": {
            repair.SAFE_WORKTREE_LABEL: {
                "target_kind": "governance_tool",
                "relative_path": repair.SAFE_WORKTREE_RELATIVE_PATH,
                "sha256": sha256,
                "size": size,
            }
        },
    })


def _manifest(*, helper_sha256: str) -> bytes:
    return _json({
        "activated_at": "2026-08-23T00:00:00Z",
        "governance_tool_sha256": {
            repair.SAFE_WORKTREE_RELATIVE_PATH: helper_sha256,
            "unrelated.py": "f" * 64,
        },
        "runtime_root": "/offline/runtime",
        "schema_version": 1,
    })


@pytest.fixture
def repair_case(tmp_path: Path) -> dict[str, Any]:
    source, runtime, release, evidence = (
        tmp_path / name for name in ("source", "runtime", "release", "evidence")
    )
    for path in (source, runtime, release, evidence):
        path.mkdir(mode=0o700)
    helper_raw = b"#!/usr/bin/env python3\nprint('candidate helper')\n"
    helper_sha = hashlib.sha256(helper_raw).hexdigest()
    helper_source = source / "scripts" / repair.SAFE_WORKTREE_RELATIVE_PATH
    registry_source = (
        source / "gateway" / "assets" / "pnc_stable_target_registry_v1.json"
    )
    installed_helper = runtime / "governance-tools" / repair.SAFE_WORKTREE_RELATIVE_PATH
    runtime_registry = (
        release / "gateway" / "assets" / "pnc_stable_target_registry_v1.json"
    )
    live_manifest = runtime / "LIVE_MANIFEST.json"
    _write(helper_source, helper_raw, mode=0o755)
    _write(registry_source, _registry(sha256="a" * 64, size=19), mode=0o644)
    _write(installed_helper, b"#!/bin/sh\necho old\n", mode=0o755)
    _write(runtime_registry, _registry(sha256="a" * 64, size=19), mode=0o644)
    _write(live_manifest, _manifest(helper_sha256="a" * 64), mode=0o600)
    return locals()


def _build(case: dict[str, Any], transaction_id: str) -> tuple[dict[str, Any], Path]:
    return repair.build_plan(
        helper_source=case["helper_source"],
        registry_source=case["registry_source"],
        installed_helper=case["installed_helper"],
        runtime_registry=case["runtime_registry"],
        live_manifest=case["live_manifest"],
        evidence_root=case["evidence"],
        transaction_id=transaction_id,
    )


def test_canonical_helper_and_registry_bind_exact_content() -> None:
    root = Path(__file__).resolve().parents[2]
    helper = root / "scripts" / repair.SAFE_WORKTREE_RELATIVE_PATH
    registry = json.loads(
        (root / "gateway" / "assets" / "pnc_stable_target_registry_v1.json").read_text()
    )
    target = registry["targets"][repair.SAFE_WORKTREE_LABEL]
    assert hashlib.sha256(helper.read_bytes()).hexdigest() == EXPECTED_HELPER_SHA256
    assert helper.stat().st_size == EXPECTED_HELPER_SIZE
    assert helper.stat().st_mode & 0o777 == 0o755
    assert target["sha256"] == EXPECTED_HELPER_SHA256
    assert target["size"] == EXPECTED_HELPER_SIZE


def test_build_and_verify_are_offline_and_non_mutating(
    repair_case: dict[str, Any],
) -> None:
    target_keys = ("installed_helper", "runtime_registry", "live_manifest")
    before = {key: repair_case[key].read_bytes() for key in target_keys}
    plan, plan_path = _build(repair_case, "offline-plan")
    result = repair.verify_plan(plan, plan_path=plan_path)
    assert result["verification"] == "pass"
    assert result["mutation_performed"] is False
    assert result["production_apply_available"] is False
    candidate = Path(plan["candidate_manifest_path"])
    assert candidate.exists() and candidate.stat().st_mode & 0o777 == 0o600
    candidate_value = json.loads(candidate.read_text())
    assert (
        candidate_value["governance_tool_sha256"][repair.SAFE_WORKTREE_RELATIVE_PATH]
        == repair_case["helper_sha"]
    )
    candidate_registry = json.loads(Path(plan["candidate_registry_path"]).read_text())
    assert (
        candidate_registry["targets"][repair.SAFE_WORKTREE_LABEL]["sha256"]
        == repair_case["helper_sha"]
    )
    assert candidate_registry["targets"][repair.SAFE_WORKTREE_LABEL]["size"] == len(
        repair_case["helper_raw"]
    )
    for key, raw in before.items():
        assert repair_case[key].read_bytes() == raw


def test_verify_rejects_target_preimage_drift(repair_case: dict[str, Any]) -> None:
    plan, plan_path = _build(repair_case, "cas-drift")
    repair_case["live_manifest"].write_bytes(_manifest(helper_sha256="b" * 64))
    repair_case["live_manifest"].chmod(0o600)
    with pytest.raises(
        repair.StableTargetRepairError, match="stable_target_preimage_changed"
    ):
        repair.verify_plan(plan, plan_path=plan_path)


@pytest.mark.parametrize("failure", ["symlink", "nlink", "mode", "owner"])
def test_observation_rejects_unsafe_source(
    repair_case: dict[str, Any], failure: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = repair_case["helper_source"]
    if failure == "symlink":
        real = source.with_name("real-helper.py")
        source.rename(real)
        source.symlink_to(real)
    elif failure == "nlink":
        os.link(source, source.with_name("helper-hardlink.py"))
    elif failure == "mode":
        source.chmod(0o700)
    else:
        original_geteuid = repair.os.geteuid()
        monkeypatch.setattr(repair.os, "geteuid", lambda: original_geteuid + 1)
        with pytest.raises(
            repair.StableTargetRepairError, match="stable_target_file_invalid"
        ):
            repair._observe_file(source, required=True, allowed_modes=(0o755,))
        return
    with pytest.raises(
        repair.StableTargetRepairError, match="stable_target_file_invalid"
    ):
        _build(repair_case, f"unsafe-{failure}")


def test_old_registry_hash_is_replaced_in_bound_candidate(
    repair_case: dict[str, Any],
) -> None:
    repair_case["registry_source"].write_bytes(_registry(sha256="0" * 64, size=19))
    repair_case["registry_source"].chmod(0o644)
    plan, plan_path = _build(repair_case, "registry-drift")
    candidate_path = Path(plan["candidate_registry_path"])
    candidate = json.loads(candidate_path.read_text())
    target = candidate["targets"][repair.SAFE_WORKTREE_LABEL]
    assert target["sha256"] == repair_case["helper_sha"]
    assert target["size"] == len(repair_case["helper_raw"])
    assert (
        hashlib.sha256(candidate_path.read_bytes()).hexdigest()
        == plan["candidate_registry_sha256"]
    )
    assert repair.verify_plan(plan, plan_path=plan_path)["verification"] == "pass"


def test_plan_binding_and_evidence_cas_are_fail_closed(
    repair_case: dict[str, Any],
) -> None:
    plan, plan_path = _build(repair_case, "binding")
    tampered = dict(plan)
    tampered["helper_size"] += 1
    with pytest.raises(
        repair.StableTargetRepairError, match="stable_target_plan_binding_invalid"
    ):
        repair.verify_plan(tampered, plan_path=plan_path)
    candidate = Path(plan["candidate_manifest_path"])
    candidate.write_text("tampered\n")
    candidate.chmod(0o600)
    with pytest.raises(
        repair.StableTargetRepairError, match="stable_target_preimage_changed"
    ):
        repair.verify_plan(plan, plan_path=plan_path)


def test_evidence_path_collision_is_rejected(repair_case: dict[str, Any]) -> None:
    transaction = repair_case["evidence"] / "collision"
    transaction.mkdir(mode=0o700)
    with pytest.raises(
        repair.StableTargetRepairError, match="stable_target_evidence_exists"
    ):
        _build(repair_case, "collision")
