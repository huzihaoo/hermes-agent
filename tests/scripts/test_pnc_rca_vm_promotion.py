from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import pnc_rca_release_gate as release_gate
from scripts import pnc_rca_vm_promotion as promotion
from scripts import pnc_rca_vm_promotion_remote as remote


NOW = datetime(2026, 7, 16, 2, 0, tzinfo=timezone.utc)


def _write_json(path: Path, body: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.write_text(
        json.dumps(body, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    path.chmod(0o600)


def _component(root: str, entrypoint: str, token: str) -> dict:
    sha = hashlib.sha256(token.encode()).hexdigest()
    return {
        "source": "ssh-mini-agent",
        "repo_root": root,
        "commit": token * 40,
        "tree_clean": True,
        "status_sha256": release_gate.EMPTY_GIT_STATUS_SHA256,
        "tree": (token.upper().lower()) * 40,
        "entrypoint_path": str(Path(root) / entrypoint),
        "entrypoint_sha256": sha,
        "entrypoint_committed_sha256": sha,
        "entrypoint_git_mode": "100755",
        "entrypoint_blob": token * 40,
    }


@pytest.fixture
def fixture(tmp_path: Path):
    root = tmp_path / "promotion"
    root.mkdir(mode=0o700)
    vm_candidate = "/mnt/tmp/rca-vm-candidate/worktree"
    worker_candidate = "/mnt/tmp/rca-worker-candidate/worktree"
    components = {
        "host": {"fixture": True},
        "workspace": {"fixture": True},
        "vm": _component(vm_candidate, promotion.VM_ENTRYPOINT, "a"),
        "vm_worker": _component(
            worker_candidate, promotion.WORKER_ENTRYPOINT, "b"
        ),
    }
    bom = {"components": components}
    binding = {
        "release_id": "rca-release-20260716",
        "release_bom": bom,
        "release_bom_sha256": promotion._sha256_json(bom),
        "release_approval_receipt_sha256": "c" * 64,
        "release_approval_expires_at": (NOW + timedelta(hours=1)).isoformat(),
        "machine_identity": {"fixture": "machine"},
        "machine_identity_sha256": promotion._sha256_json(
            {"fixture": "machine"}
        ),
    }
    inputs = promotion.VmPromotionInputs(
        release_prepare_manifest=root / "release-prepare.json",
        release_approval_receipt=root / "release-approval.json",
        vm_candidate_root=vm_candidate,
        worker_candidate_root=worker_candidate,
        vm_topic_extractor_sha256="d" * 64,
        vm_topic_extractor_size=7_159_960,
        plan_path=root / "plan.json",
        promotion_approval_receipt=root / "promotion-approval.json",
        receipt_path=root / "receipt.json",
        rollback_receipt_path=root / "rollback-receipt.json",
        remote_work_root="/mnt/tmp/rca-release-20260716/vm-promotion",
        remote_lock_path="/home/mini/.hermes/locks/rca-vm-promotion.lock",
    )
    return inputs, binding


def _observation(request: dict) -> dict:
    observed_components = {}
    for spec in request["components"]:
        runtime = [
            {
                "relative_path": item["relative_path"],
                "expected_sha256": item["sha256"],
                "expected_size": item["size"],
                "observed": {
                    "kind": "file",
                    "sha256": item["sha256"],
                    "size": item["size"],
                },
            }
            for item in spec["runtime_artifacts"]
        ]
        observed_components[spec["name"]] = {
            "candidate": {
                "root": spec["candidate_root"],
                "head": spec["desired_commit"],
                "tree": spec["desired_tree"],
                "tree_clean": True,
                "entrypoint": {"sha256": spec["entrypoint_sha256"]},
            },
            "target": {
                "root": spec["target_root"],
                "head": "e" * 40,
                "tree": "f" * 40,
                "tree_clean": spec["name"] == "vm",
                "dirty_paths": (
                    {}
                    if spec["name"] == "vm"
                    else {"vm_bounded_jsonl_tail.py": {"kind": "file"}}
                ),
            },
            "candidate_runtime_artifacts": runtime,
            "target_runtime_artifacts": [],
        }
    return {
        "schema_version": remote.OBSERVATION_SCHEMA_VERSION,
        "release_id": request["release_id"],
        "components": observed_components,
        "service": {
            "mode": "systemd_user",
            "active": True,
            "active_state": "active",
            "main_pid": 41001,
        },
    }


def test_build_plan_binds_bom_candidates_live_prestate_and_helper(fixture):
    inputs, binding = fixture
    calls = []

    def remote_runner(request):
        calls.append(request)
        return _observation(request)

    plan = promotion.build_plan(
        inputs,
        release_binding_provider=lambda _inputs, _now: binding,
        remote_runner=remote_runner,
        now=NOW,
    )

    assert plan["production_effects_executed"] is False
    assert plan["release_bom_sha256"] == binding["release_bom_sha256"]
    assert plan["remote_helper_sha256"] == promotion._sha256_file(
        promotion.REMOTE_HELPER_PATH
    )
    assert plan["prestate"]["components"]["vm_worker"]["target"][
        "tree_clean"
    ] is False
    assert calls[0]["mode"] == "observe"
    assert calls[0]["components"][0]["runtime_artifacts"][0]["sha256"] == (
        inputs.vm_topic_extractor_sha256
    )
    assert inputs.plan_path.is_file()


def test_finalized_release_binding_uses_receipt_validity_not_request_freshness(
    fixture,
    monkeypatch,
):
    inputs, _binding = fixture
    bom = {"components": {}, "label": "\u89c4\u5212"}
    bom_sha256 = release_gate._sha256_json(bom)
    assert bom_sha256 != promotion._sha256_json(bom)
    approval_sha256 = "c" * 64
    manifest = {
        "schema_version": (
            promotion.release_prepare.RELEASE_PREPARE_MANIFEST_SCHEMA_VERSION
        ),
        "complete": True,
        "plan_only": True,
        "release_id": "rca-release-20260716",
        "release_bom_sha256": bom_sha256,
        "approval_receipt_sha256": approval_sha256,
    }
    request = {
        "bindings": {
            "release_bom": bom,
            "release_bom_sha256": bom_sha256,
        }
    }
    approval = {"expires_at": (NOW + timedelta(hours=1)).isoformat()}
    artifacts = {
        "vm_promotion_release_manifest": SimpleNamespace(
            path=inputs.release_prepare_manifest,
            body=manifest,
            sha256="a" * 64,
        ),
        "vm_promotion_release_request": SimpleNamespace(
            path=inputs.release_prepare_manifest.parent / "approval_request.json",
            body=request,
            sha256="b" * 64,
        ),
        "vm_promotion_release_approval": SimpleNamespace(
            path=inputs.release_approval_receipt,
            body=approval,
            sha256=approval_sha256,
        ),
    }
    monkeypatch.setattr(
        promotion.cutover,
        "_read_owned_json",
        lambda _path, *, artifact: artifacts[artifact],
    )
    machine = {"fixture": "machine"}
    monkeypatch.setattr(promotion, "_machine_identity", lambda: machine)
    observed = {}

    def validate(**kwargs):
        observed.update(kwargs)
        return {"ok": True}

    monkeypatch.setattr(
        promotion.release_gate,
        "validate_release_prepare_approval_binding",
        validate,
    )

    result = promotion._default_release_binding(inputs, NOW)

    assert observed["require_fresh_request"] is False
    assert result["release_bom_sha256"] == bom_sha256
    assert result["release_approval_receipt_sha256"] == approval_sha256


def test_apply_requires_second_approval_and_publishes_remote_receipt(fixture):
    inputs, binding = fixture
    plan = promotion.build_plan(
        inputs,
        release_binding_provider=lambda _inputs, _now: binding,
        remote_runner=lambda request: _observation(request),
        now=NOW,
    )
    approval = {
        "schema_version": promotion.APPROVAL_SCHEMA_VERSION,
        "release_id": binding["release_id"],
        "decision": promotion.AUTHORIZATION_DECISION,
        "created_at": NOW.isoformat(),
        "expires_at": (NOW + timedelta(minutes=30)).isoformat(),
        "plan_sha256": promotion._sha256_json(plan),
        "release_bom_sha256": binding["release_bom_sha256"],
        "release_approval_receipt_sha256": binding[
            "release_approval_receipt_sha256"
        ],
        "remote_helper_sha256": plan["remote_helper_sha256"],
        "machine_identity_sha256": binding["machine_identity_sha256"],
        "action_set": list(promotion.ACTION_SET),
        "action_set_sha256": promotion._sha256_json(list(promotion.ACTION_SET)),
        "operator": "release-owner",
        "reason": "approved exact VM promotion",
    }
    _write_json(inputs.promotion_approval_receipt, approval)
    calls = []

    def apply_remote(request):
        calls.append(request)
        return {
            "schema_version": remote.RECEIPT_SCHEMA_VERSION,
            "ok": True,
            "release_id": binding["release_id"],
            "expected_observation_sha256": plan["prestate_sha256"],
            "components": [{"name": "vm"}, {"name": "vm_worker"}],
            "service_before": plan["prestate"]["service"],
            "service_after": plan["prestate"]["service"],
            "snapshot_root": inputs.remote_work_root + "/snapshot",
            "production_effects_executed": True,
            "receipt_path": inputs.remote_work_root + "/remote-receipt.json",
            "receipt_sha256": "f" * 64,
        }

    receipt = promotion.apply_promotion(
        inputs,
        authorization_decision=promotion.AUTHORIZATION_DECISION,
        release_binding_provider=lambda _inputs, _now: binding,
        remote_runner=apply_remote,
        now=NOW + timedelta(minutes=1),
    )

    assert receipt["ok"] is True
    assert receipt["production_effects_executed"] is True
    assert calls[0]["mode"] == "apply"
    assert calls[0]["expected_observation_sha256"] == plan["prestate_sha256"]
    assert inputs.receipt_path.is_file()

    remote_receipt = receipt["remote_receipt"]

    def verify_remote(request):
        observed = _observation(request)
        for spec in request["components"]:
            component = observed["components"][spec["name"]]
            component["target"] = {
                **component["target"],
                "head": spec["desired_commit"],
                "tree": spec["desired_tree"],
                "tree_clean": True,
                "entrypoint": {"sha256": spec["entrypoint_sha256"]},
            }
            component["target_runtime_artifacts"] = component[
                "candidate_runtime_artifacts"
            ]
        return observed

    verification = promotion.verify_promotion(
        inputs,
        release_binding_provider=lambda _inputs, _now: binding,
        remote_runner=verify_remote,
        now=NOW + timedelta(minutes=1),
    )
    assert verification["ok"] is True
    assert verification["production_effects_executed"] is False

    def rollback_remote(request):
        assert request["mode"] == "rollback"
        assert request["remote_receipt_path"] == remote_receipt["receipt_path"]
        assert request["remote_receipt_sha256"] == remote_receipt["receipt_sha256"]
        return {
            "schema_version": remote.ROLLBACK_RECEIPT_SCHEMA_VERSION,
            "ok": True,
            "release_id": binding["release_id"],
            "promotion_receipt_sha256": remote_receipt["receipt_sha256"],
            "components": [{"name": "vm_worker"}, {"name": "vm"}],
            "service_restored": plan["prestate"]["service"],
            "production_effects_executed": True,
            "rollback_complete": True,
            "receipt_path": inputs.remote_work_root
            + "/remote-rollback-receipt.json",
            "receipt_sha256": "a" * 64,
        }

    rollback = promotion.rollback_promotion(
        inputs,
        authorization_decision=promotion.AUTHORIZATION_DECISION,
        release_binding_provider=lambda _inputs, _now: binding,
        remote_runner=rollback_remote,
        now=NOW + timedelta(days=1),
    )

    assert rollback["rollback_complete"] is True
    assert inputs.rollback_receipt_path.is_file()


def test_apply_rejects_without_exact_decision_before_remote_call(fixture):
    inputs, binding = fixture
    promotion.build_plan(
        inputs,
        release_binding_provider=lambda _inputs, _now: binding,
        remote_runner=lambda request: _observation(request),
        now=NOW,
    )

    with pytest.raises(promotion.VmPromotionError, match="decision_invalid"):
        promotion.apply_promotion(
            inputs,
            authorization_decision="approve",
            release_binding_provider=lambda _inputs, _now: binding,
            remote_runner=lambda _request: pytest.fail("remote mutation reached"),
            now=NOW,
        )


def test_local_receipt_publish_failure_rolls_remote_promotion_back(fixture):
    inputs, binding = fixture
    plan = promotion.build_plan(
        inputs,
        release_binding_provider=lambda _inputs, _now: binding,
        remote_runner=lambda request: _observation(request),
        now=NOW,
    )
    approval = {
        "schema_version": promotion.APPROVAL_SCHEMA_VERSION,
        "release_id": binding["release_id"],
        "decision": promotion.AUTHORIZATION_DECISION,
        "created_at": NOW.isoformat(),
        "expires_at": (NOW + timedelta(minutes=30)).isoformat(),
        "plan_sha256": promotion._sha256_json(plan),
        "release_bom_sha256": binding["release_bom_sha256"],
        "release_approval_receipt_sha256": binding[
            "release_approval_receipt_sha256"
        ],
        "remote_helper_sha256": plan["remote_helper_sha256"],
        "machine_identity_sha256": binding["machine_identity_sha256"],
        "action_set": list(promotion.ACTION_SET),
        "action_set_sha256": promotion._sha256_json(list(promotion.ACTION_SET)),
        "operator": "release-owner",
        "reason": "approved exact VM promotion",
    }
    _write_json(inputs.promotion_approval_receipt, approval)
    _write_json(inputs.receipt_path, {"conflict": True})
    calls = []

    def remote_runner(request):
        calls.append(request["mode"])
        if request["mode"] == "apply":
            return {
                "schema_version": remote.RECEIPT_SCHEMA_VERSION,
                "ok": True,
                "release_id": binding["release_id"],
                "expected_observation_sha256": plan["prestate_sha256"],
                "components": [{"name": "vm"}, {"name": "vm_worker"}],
                "service_before": plan["prestate"]["service"],
                "service_after": plan["prestate"]["service"],
                "snapshot_root": inputs.remote_work_root + "/snapshot",
                "production_effects_executed": True,
                "receipt_path": inputs.remote_work_root + "/remote-receipt.json",
                "receipt_sha256": "f" * 64,
            }
        return {
            "schema_version": remote.ROLLBACK_RECEIPT_SCHEMA_VERSION,
            "ok": True,
            "release_id": binding["release_id"],
            "promotion_receipt_sha256": "f" * 64,
            "components": [{"name": "vm_worker"}, {"name": "vm"}],
            "service_restored": plan["prestate"]["service"],
            "production_effects_executed": True,
            "rollback_complete": True,
            "receipt_path": (
                inputs.remote_work_root + "/remote-rollback-receipt.json"
            ),
            "receipt_sha256": "a" * 64,
        }

    with pytest.raises(
        promotion.VmPromotionError,
        match="local_receipt_publish_failed_remote_rolled_back",
    ):
        promotion.apply_promotion(
            inputs,
            authorization_decision=promotion.AUTHORIZATION_DECISION,
            release_binding_provider=lambda _inputs, _now: binding,
            remote_runner=remote_runner,
            now=NOW + timedelta(minutes=1),
        )

    assert calls == ["apply", "rollback"]
    auto = json.loads(inputs.rollback_receipt_path.read_text(encoding="utf-8"))
    assert auto["schema_version"] == promotion.AUTO_ROLLBACK_RECEIPT_SCHEMA_VERSION
    assert auto["rollback_complete"] is True
