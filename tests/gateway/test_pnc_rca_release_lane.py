from __future__ import annotations

import copy

import pytest

from gateway import pnc_rca_release_lane as lane


def _build(paths, *, complete=True):
    return lane.build_release_lane_decision(
        changed_paths=paths,
        dependency_closure=paths,
        validation_manifest_sha256="a" * 64,
        rollback_release_id="rca-r15c6-rollback",
        rollback_release_note_sha256="b" * 64,
        import_closure_complete=complete,
    )


def test_current_collector_binding_change_is_critical_full():
    decision = _build(
        [
            "gateway/pnc_rca_vm_release_binding.py",
            "scripts/pnc_fault_taxonomy.py",
            "scripts/pnc_rca_delivery_collector.py",
        ]
    )

    assert decision["release_lane"] == "critical_full"
    assert decision["classification_reason"] == (
        "critical_identity_or_delivery_closure"
    )
    assert "host_outbox_dispatcher" in decision["affected_faces"]
    assert "host_delivery_collector" in decision["affected_faces"]
    assert decision["restart_targets"]


def test_vm_evaluator_closure_uses_vm_task_fast_without_resident_restart():
    decision = _build(
        ["api/g1q3_rca/evaluators/controlled_threshold_rule.json"]
    )

    assert decision["release_lane"] == "vm_task_fast"
    assert decision["affected_faces"] == ["vm_task_subprocess"]
    assert decision["restart_targets"] == []


def test_incomplete_or_unknown_closure_upgrades_to_critical_full():
    incomplete = _build(["docs/rca.md"], complete=False)
    unknown = _build(["scripts/unknown_runtime_helper.py"])

    assert incomplete["release_lane"] == "critical_full"
    assert incomplete["classification_reason"] == (
        "import_closure_incomplete_auto_upgrade"
    )
    assert unknown["release_lane"] == "critical_full"
    assert unknown["classification_reason"] == "unknown_closure_auto_upgrade"


def test_lane_decision_rejects_manual_downgrade_and_hash_drift():
    decision = _build(["gateway/pnc_rca_vm_release_binding.py"])
    downgraded = copy.deepcopy(decision)
    downgraded["release_lane"] = "resident_targeted"
    with pytest.raises(lane.ReleaseLaneError, match="classification_mismatch"):
        lane.validate_release_lane_decision(downgraded)

    drifted = copy.deepcopy(decision)
    drifted["dependency_closure_sha256"] = "0" * 64
    with pytest.raises(lane.ReleaseLaneError, match="contract_invalid"):
        lane.validate_release_lane_decision(drifted)
