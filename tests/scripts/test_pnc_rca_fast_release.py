from __future__ import annotations

import json

from scripts import pnc_rca_fast_release as fast_release


def test_offline_repair_proves_fix_publish_rollback_and_restore(tmp_path):
    manifest = tmp_path / "validation-manifest-v1.json"
    manifest.write_text(
        json.dumps(
            {
                "sets": {
                    "S16": {
                        "count": 16,
                        "cases": [
                            {"work_item_id": str(7_000_000_000 + index)}
                            for index in range(16)
                        ],
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    result = fast_release.run_offline_repair(
        validation_manifest=manifest,
        output_dir=tmp_path / "proof",
    )

    assert result["success"] is True
    receipt = json.loads((tmp_path / "proof/fast-release-receipt.json").read_text())
    rollback = json.loads((tmp_path / "proof/rollback-receipt.json").read_text())
    decision = json.loads((tmp_path / "proof/lane-decision.json").read_text())
    assert receipt["release_lane"] == "vm_task_fast"
    assert receipt["predecessor"]["observed_result"] == "fail"
    assert receipt["candidate"]["observed_result"] == "pass"
    assert receipt["candidate"]["final_active_result"] == "pass"
    assert receipt["commit_to_effect_seconds"] >= 0
    assert receipt["production_success_claimed"] is False
    assert set(receipt["external_side_effects"].values()) == {0}
    assert rollback["rollback_verified"] is True
    assert decision["release_lane"] == "vm_task_fast"


def test_plan_incomplete_import_closure_upgrades_to_critical_full(tmp_path):
    result = fast_release.plan_release_lane(
        output=tmp_path / "lane-decision.json",
        changed_paths=["api/g1q3_rca/evaluators/threshold.py"],
        dependency_closure=["api/g1q3_rca/evaluators/threshold.py"],
        validation_manifest_sha256="a" * 64,
        rollback_release_id="rca-release-rollback-1",
        rollback_release_note_sha256="b" * 64,
        import_closure_complete=False,
        max_concurrency=1,
    )

    decision = json.loads((tmp_path / "lane-decision.json").read_text())
    assert result["release_lane"] == "critical_full"
    assert decision["classification_reason"] == "import_closure_incomplete_auto_upgrade"
