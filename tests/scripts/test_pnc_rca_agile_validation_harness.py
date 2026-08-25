from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from scripts import pnc_rca_agile_validation_harness as harness


def _source(tmp_path: Path) -> tuple[Path, list[str]]:
    path = tmp_path / "source.ndjson"
    titles: list[str] = []
    rows = []
    categories = ("LCC", "ACC", "HMI", "OTHER")
    for index in range(300):
        target = index < 286
        category = categories[index % len(categories)]
        category_token = category if category != "OTHER" else "PLANNING"
        title = f"UNIQUE_RAW_TITLE_{index}_{category_token} issue"
        titles.append(title)
        report_present = (index // len(categories)) % 2 == 0
        rows.append(
            {
                "work_item_id": str(7_000_000_000 + index),
                "data": {
                    "work_item_attribute": {
                        "create_by": {
                            "key": harness.CREATOR_KEY if target else "other",
                            "name": harness.CREATOR_NAME if target else "other",
                        },
                        "work_item_name": title,
                    },
                    "work_item_fields": [
                        {
                            "key": harness.PROJECT_FIELD_KEY,
                            "value": [
                                {
                                    "id": int(harness.ALLOWED_PROJECT_OPTION_IDS[0]),
                                    "name": "G1Q3 fixture",
                                }
                            ],
                        },
                        {"key": harness.FRAME_FIELD_KEY, "value": str(index)},
                        {"key": harness.PDCL_FIELD_KEY, "value": "true"},
                        {
                            "key": harness.REPORT_FIELD_KEY,
                            "value": "https://private.invalid/report"
                            if report_present
                            else "",
                        },
                    ],
                },
            }
        )
    rows.append({"summary": {"total": 300, "succeeded": 300, "failed": 0}})
    raw = b"".join(
        json.dumps(row, ensure_ascii=False, sort_keys=True).encode("utf-8") + b"\n"
        for row in rows
    )
    path.write_bytes(raw)
    return path, titles


def test_builds_exact_privacy_safe_v286_k286_s16(tmp_path):
    source, titles = _source(tmp_path)
    source_sha = hashlib.sha256(source.read_bytes()).hexdigest()

    manifest = harness.build_validation_manifest(
        source, expected_source_sha256=source_sha
    )

    assert manifest["contract"] == {
        "creator_key": harness.CREATOR_KEY,
        "creator_name": harness.CREATOR_NAME,
        "project_field_key": harness.PROJECT_FIELD_KEY,
        "project_name_prefix": harness.PROJECT_NAME_PREFIX,
        "operator_filter": None,
        "allowed_project_option_ids": ["6670325063"],
        "expected_count": 286,
    }
    assert manifest["sets"]["V286"]["count"] == 286
    assert manifest["sets"]["K286"]["count"] == 286
    assert manifest["sets"]["K286"]["missing_count"] == 286
    assert manifest["sets"]["S16"]["count"] == 16
    s16 = manifest["sets"]["S16"]["cases"]
    assert {
        category: sum(case["category"] == category for case in s16)
        for category in ("LCC", "ACC", "HMI", "OTHER")
    } == {"LCC": 4, "ACC": 4, "HMI": 4, "OTHER": 4}
    encoded = harness.canonical_bytes(manifest)
    assert all(title.encode("utf-8") not in encoded for title in titles)
    assert b"https://private.invalid/report" not in encoded


def test_rejects_source_hash_and_count_drift(tmp_path):
    source, _titles = _source(tmp_path)
    with pytest.raises(harness.ValidationHarnessError) as mismatch:
        harness.build_validation_manifest(source, expected_source_sha256="0" * 64)
    assert mismatch.value.code == "source_snapshot_sha256_mismatch"

    rows = source.read_text(encoding="utf-8").splitlines()
    source.write_text("\n".join(rows[1:]) + "\n", encoding="utf-8")
    source_sha = hashlib.sha256(source.read_bytes()).hexdigest()
    with pytest.raises(harness.ValidationHarnessError) as count:
        harness.build_validation_manifest(source, expected_source_sha256=source_sha)
    assert count.value.code == "v286_count_mismatch"


def test_write_modes_fail_closed_without_exact_authority(tmp_path):
    source, _titles = _source(tmp_path)
    source_sha = hashlib.sha256(source.read_bytes()).hexdigest()
    manifest = harness.build_validation_manifest(
        source, expected_source_sha256=source_sha
    )
    manifest_sha = hashlib.sha256(harness.canonical_bytes(manifest) + b"\n").hexdigest()

    pending = harness.build_mode_receipt(
        manifest,
        manifest_sha256=manifest_sha,
        mode="canary_write",
    )

    assert pending["status"] == "pending_approval"
    assert pending["canonical_executor_invoked"] is False
    assert set(pending["external_side_effects"].values()) == {0}


def test_shadow_is_projection_not_production_success(tmp_path):
    source, _titles = _source(tmp_path)
    source_sha = hashlib.sha256(source.read_bytes()).hexdigest()
    manifest = harness.build_validation_manifest(
        source, expected_source_sha256=source_sha
    )

    receipt = harness.build_mode_receipt(
        manifest,
        manifest_sha256="1" * 64,
        mode="shadow",
    )

    assert receipt["status"] == "shadow_projected"
    assert receipt["canonical_executor_invoked"] is False
    assert all(row["vm"] == "not_submitted" for row in receipt["stage_receipts"])


def test_load_sealed_manifest_requires_canonical_exact_contract(tmp_path):
    source, _titles = _source(tmp_path)
    source_sha = hashlib.sha256(source.read_bytes()).hexdigest()
    manifest = harness.build_validation_manifest(
        source, expected_source_sha256=source_sha
    )
    path = tmp_path / "validation-manifest-v1.json"
    raw = harness.canonical_bytes(manifest) + b"\n"
    path.write_bytes(raw)

    loaded, observed_sha = harness.load_sealed_manifest(path)

    assert loaded == manifest
    assert observed_sha == hashlib.sha256(raw).hexdigest()


def test_derives_next_submission_but_defers_effect_until_vm_payload():
    identity = harness._canonical_next_identity(
        {
            "project_key": "project",
            "project_simple_name": "simple",
            "work_item_type_key": "issue",
            "work_item_id": "7000000001",
            "creation_rule_version": "rule-v1",
            "generation": 7,
        },
        work_item_id="7000000001",
    )

    assert identity["status"] == "derived_canonical_pending_authority"
    assert identity["generation"] == 8
    assert identity["submission_key"].startswith("g1q3-rca-s1-")
    assert identity["effect_key"] == {
        "status": "deferred_until_canonical_vm_delivery_payload",
        "value": None,
        "derivation": (
            "compute_delivery_effect_key(delivery_id,effect_kind,target_key,"
            "semantic_payload_sha256) after sealed VM delivery"
        ),
    }


def test_already_current_requires_exact_official_and_control_effect_proof():
    effect_key = "g1q3-rca-effect-v1-" + "a" * 64
    official = {
        "report_present": True,
        "delivery_effect_keys": [effect_key],
        "attribution_markers": [
            {
                "version": "upgrade-v1",
                "contract_sha256": "b" * 64,
                "effect_key": effect_key,
            }
        ],
    }
    effects = [
        {
            "effect_key": effect_key,
            "status": "succeeded",
            "remote_receipt_present": True,
        }
    ]

    classification, proof = harness._upgrade_classification(
        official=official,
        effects=effects,
        upgrade_contract={"version_id": "upgrade-v1"},
        upgrade_observation={"sha256": "b" * 64},
    )

    assert classification == "already_current"
    assert proof["already_current"] is True

    effects[0]["remote_receipt_present"] = False
    classification, proof = harness._upgrade_classification(
        official=official,
        effects=effects,
        upgrade_contract={"version_id": "upgrade-v1"},
        upgrade_observation={"sha256": "b" * 64},
    )
    assert classification == "rewrite_update"
    assert proof["already_current"] is False
