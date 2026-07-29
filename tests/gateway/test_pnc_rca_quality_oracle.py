from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest

from gateway import pnc_rca_quality_oracle as oracle_module
from gateway.pnc_rca_quality_oracle import (
    CANDIDATE_HYPOTHESIS,
    CONSUMER_DELIVERY_FAILURE,
    HONEST_NON_ATTRIBUTION,
    MEDIUM_TIER_DISCLAIMER,
    SUPPORTED_ATTRIBUTION,
    TECHNICAL_FAILURE,
    TierOracleConflict,
    evaluate_structural_tier,
    require_publishable,
)


_MISSING = object()
_REQUIRED_DIMENSIONS = ["real_positive", "real_negative", "synthetic_boundary"]


def _release_registry(evaluator_id: str = "lane_geometry_quality") -> dict:
    entry = {
        "evaluator_id": evaluator_id,
        "evaluator_key": evaluator_id,
        "domain": "PERCEPTION_LANE",
        "required_dimensions": list(_REQUIRED_DIMENSIONS),
        "status": "passed",
        "source_kind": "owner_confirmed_case",
        "evaluator_source_sha256": "c" * 64,
        "positive_golden_sha256": "a" * 64,
        "negative_golden_sha256": "b" * 64,
        "test_receipt_sha256": "d" * 64,
        "dimensions": {
            "real_positive": {"status": "passed", "case_count": 1, "artifact_sha256": "a" * 64},
            "real_negative": {"status": "passed", "case_count": 1, "artifact_sha256": "b" * 64},
            "synthetic_boundary": {"status": "passed", "case_count": 1, "artifact_sha256": "d" * 64},
        },
        "fully_validated": True,
        "missing_dimensions": [],
    }
    return {
        "present": True,
        "valid": True,
        "low_tier_golden_ready": True,
        "required_dimensions": tuple(_REQUIRED_DIMENSIONS),
        "evaluators": {evaluator_id: entry},
        "fully_validated_evaluators": {evaluator_id: entry},
        "missing_dimensions_by_evaluator": {},
        "active_inventory_binding_valid": True,
        "active_inventory_evaluator_ids": (evaluator_id,),
        "inventory_binding_valid": True,
    }


def _registry_payload(*entries: dict, required=_MISSING):
    payload = {
        "schema_version": "pnc_rca_release_golden_registry_v1",
        "validation_schema_version": "g1q3_rca_evaluator_validation_dimensions_v1",
        "required_dimensions": list(_REQUIRED_DIMENSIONS),
        "pipeline_commit": "a" * 40,
        "pipeline_tree": "b" * 40,
        "low_tier_suite": {
            "status": "passed",
            "positive_case_count": 1,
            "negative_case_count": 1,
            "receipt_sha256": "c" * 64,
            "vm_path": "/mnt/tmp/w1/receipt.json",
            "user_visible_path": "//hfs1.minieye.tech/share/w1/",
        },
        "evaluators": list(entries),
    }
    if required is not _MISSING:
        payload["required_evaluator_ids"] = required
    return payload


def _golden_entry(evaluator_id: str, *, hash_char: str = "a") -> dict:
    hashes = [
        hash_char,
        chr(ord(hash_char) + 1),
        chr(ord(hash_char) + 2),
        chr(ord(hash_char) + 3),
    ]
    return {
        "evaluator_id": evaluator_id,
        "evaluator_key": evaluator_id,
        "domain": "PERCEPTION_LANE",
        "required_dimensions": list(_REQUIRED_DIMENSIONS),
        "status": "passed",
        "source_kind": "owner_confirmed_case",
        "evaluator_source_sha256": hashes[0] * 64,
        "positive_golden_sha256": hashes[1] * 64,
        "negative_golden_sha256": hashes[2] * 64,
        "test_receipt_sha256": hashes[3] * 64,
        "dimensions": {
            "real_positive": {"status": "passed", "case_count": 1, "artifact_sha256": hashes[1] * 64},
            "real_negative": {"status": "passed", "case_count": 1, "artifact_sha256": hashes[2] * 64},
            "synthetic_boundary": {"status": "passed", "case_count": 1, "artifact_sha256": hashes[3] * 64},
        },
        "fully_validated": True,
        "missing_dimensions": [],
    }


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _write_artifact(registry_path: Path, relative: str, payload: dict) -> tuple[str, str]:
    path = registry_path.parent / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (json.dumps(payload, sort_keys=True) + "\n").encode("utf-8")
    path.write_bytes(encoded)
    return relative, hashlib.sha256(encoded).hexdigest()


def _materialize_registry(
    registry_path: Path,
    payload: dict,
    *,
    active_ids: list[str] | None = None,
) -> dict:
    payload = copy.deepcopy(payload)
    entries = payload.get("evaluators") or []
    entry_ids = [entry["evaluator_id"] for entry in entries if entry.get("evaluator_id")]
    active_ids = list(active_ids if active_ids is not None else entry_ids)
    if entries:
        inventory = {
            "schema_version": "g1q3_rca_active_evaluator_inventory_v1",
            "pipeline_commit": payload["pipeline_commit"],
            "pipeline_tree": payload["pipeline_tree"],
            "active_evaluator_ids": active_ids,
        }
        relative, digest = _write_artifact(
            registry_path,
            "artifacts/active-inventory.json",
            inventory,
        )
        payload["active_inventory_artifact"] = {
            "artifact_path": relative,
            "artifact_sha256": digest,
        }

    for entry in entries:
        evaluator_id = entry["evaluator_id"]
        evaluator_key = entry["evaluator_key"]
        domain = entry["domain"]
        real_case_ids = [
            f"{evaluator_id}:positive",
            f"{evaluator_id}:negative",
        ]
        source = {
            "schema_version": "g1q3_rca_owner_confirmed_source_v1",
            "evaluator_id": evaluator_id,
            "evaluator_key": evaluator_key,
            "domain": domain,
            "source_kind": entry["source_kind"],
            "case_ids": real_case_ids,
            "owner_confirmation": {
                "status": "confirmed",
                "receipt_sha256": _sha(f"{evaluator_id}:owner-confirmation"),
            },
        }
        relative, digest = _write_artifact(
            registry_path,
            f"artifacts/{evaluator_id}/owner-source.json",
            source,
        )
        entry["evaluator_source_artifact_path"] = relative
        entry["evaluator_source_sha256"] = digest

        hash_field = {
            "real_positive": "positive_golden_sha256",
            "real_negative": "negative_golden_sha256",
            "synthetic_boundary": "test_receipt_sha256",
        }
        for dimension, definition in entry["dimensions"].items():
            if definition.get("status") != "passed":
                continue
            outcomes = ["PASS", "FAIL"] if dimension == "synthetic_boundary" else [
                "PASS" if dimension == "real_positive" else "FAIL"
            ]
            cases = []
            for index, outcome in enumerate(outcomes, start=1):
                case_id = (
                    f"{evaluator_id}:{'positive' if dimension == 'real_positive' else 'negative'}"
                    if dimension != "synthetic_boundary"
                    else f"{evaluator_id}:boundary:{index}"
                )
                cases.append({
                    "case_id": case_id,
                    "source_kind": (
                        "synthetic_boundary"
                        if dimension == "synthetic_boundary"
                        else "owner_confirmed_real_issue"
                    ),
                    "expected_evaluator_status": outcome,
                    "evaluator_observed_status": outcome,
                    "result": "PASS",
                    "case_config_sha256": _sha(f"{case_id}:config"),
                    "evidence_sha256": _sha(f"{case_id}:evidence"),
                })
            artifact = {
                "schema_version": "g1q3_rca_validation_dimension_artifact_v1",
                "evaluator_id": evaluator_id,
                "evaluator_key": evaluator_key,
                "domain": domain,
                "dimension": dimension,
                "cases": cases,
            }
            relative, digest = _write_artifact(
                registry_path,
                f"artifacts/{evaluator_id}/{dimension}.json",
                artifact,
            )
            definition["case_count"] = len(cases)
            definition["artifact_path"] = relative
            definition["artifact_sha256"] = digest
            entry[hash_field[dimension]] = digest
    registry_path.write_text(json.dumps(payload), encoding="utf-8")
    return payload


def _rewrite_dimension_artifact(
    registry_path: Path,
    payload: dict,
    dimension: str,
    mutate,
) -> dict:
    definition = payload["evaluators"][0]["dimensions"][dimension]
    artifact_path = registry_path.parent / definition["artifact_path"]
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    mutate(artifact)
    encoded = (json.dumps(artifact, sort_keys=True) + "\n").encode("utf-8")
    artifact_path.write_bytes(encoded)
    digest = hashlib.sha256(encoded).hexdigest()
    definition["artifact_sha256"] = digest
    payload["evaluators"][0][{
        "real_positive": "positive_golden_sha256",
        "real_negative": "negative_golden_sha256",
        "synthetic_boundary": "test_receipt_sha256",
    }[dimension]] = digest
    registry_path.write_text(json.dumps(payload), encoding="utf-8")
    return payload


def test_release_registry_accepts_current_git_object_ids_and_tracks_red_suite(
    tmp_path,
):
    path = tmp_path / "registry.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": "pnc_rca_release_golden_registry_v1",
                "validation_schema_version": "g1q3_rca_evaluator_validation_dimensions_v1",
                "required_dimensions": list(_REQUIRED_DIMENSIONS),
                "pipeline_commit": "a" * 40,
                "pipeline_tree": "b" * 40,
                "low_tier_suite": {
                    "status": "failing",
                    "positive_case_count": 1,
                    "negative_case_count": 1,
                    "receipt_sha256": "c" * 64,
                    "vm_path": "/mnt/tmp/w1/receipt.json",
                    "user_visible_path": (
                        "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/"
                        "tmp/w1/"
                    ),
                },
                "evaluators": [],
            }
        ),
        encoding="utf-8",
    )

    status = oracle_module.release_golden_registry_status(path)

    assert status["valid"] is True
    assert status["low_tier_golden_ready"] is False


def test_release_registry_rejects_malformed_git_object_id(tmp_path):
    path = tmp_path / "registry.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": "pnc_rca_release_golden_registry_v1",
                "validation_schema_version": "g1q3_rca_evaluator_validation_dimensions_v1",
                "required_dimensions": list(_REQUIRED_DIMENSIONS),
                "pipeline_commit": "a" * 39,
                "pipeline_tree": "b" * 40,
                "low_tier_suite": {
                    "status": "passed",
                    "positive_case_count": 1,
                    "negative_case_count": 1,
                    "receipt_sha256": "c" * 64,
                    "vm_path": "/mnt/tmp/w1/receipt.json",
                    "user_visible_path": "//hfs1.minieye.tech/share/w1/",
                },
                "evaluators": [],
            }
        ),
        encoding="utf-8",
    )

    assert oracle_module.release_golden_registry_status(path)["valid"] is False


def test_low_tier_only_registry_does_not_claim_full_inventory_coverage(tmp_path):
    path = tmp_path / "registry.json"
    path.write_text(json.dumps(_registry_payload()), encoding="utf-8")

    status = oracle_module.release_golden_registry_status(path)

    assert status["valid"] is True
    assert status["low_tier_golden_ready"] is True
    assert status["evaluators"] == {}
    assert status["required_evaluator_ids_present"] is False
    assert status["inventory_binding_valid"] is False
    assert status["active_inventory_binding_valid"] is False


def test_committed_empty_registry_keeps_high_confidence_closed():
    status = oracle_module.release_golden_registry_status()

    assert status["valid"] is True
    assert status["fully_validated_evaluator_ids"] == ()
    assert status["active_inventory_binding_valid"] is False


def test_hash_only_registry_cannot_unlock_high_confidence(tmp_path):
    path = tmp_path / "registry.json"
    path.write_text(
        json.dumps(
            _registry_payload(
                _golden_entry("lane_geometry_quality"),
                required=["lane_geometry_quality"],
            )
        ),
        encoding="utf-8",
    )

    status = oracle_module.release_golden_registry_status(path)
    result = evaluate_structural_tier(
        _contract(),
        golden_registry_status=status,
    )

    assert status["valid"] is False
    assert status["active_inventory_binding_valid"] is False
    assert "active_inventory_artifact_missing" in status["active_inventory_errors"]
    assert status["fully_validated_evaluator_ids"] == ()
    assert result.confidence_tier == "medium"
    assert result.terminal_class == CANDIDATE_HYPOTHESIS
    assert result.facts.evaluator_validation_complete is False


def test_registry_accepts_genuine_bound_artifacts_and_unlocks_only_active_key(
    tmp_path,
):
    path = tmp_path / "registry.json"
    payload = _materialize_registry(
        path,
        _registry_payload(
            _golden_entry("lane_geometry_quality"),
            required=["lane_geometry_quality"],
        ),
    )

    status = oracle_module.release_golden_registry_status(path)
    result = evaluate_structural_tier(
        _contract(),
        golden_registry_status=status,
    )

    assert status["valid"] is True, status
    assert status["active_inventory_binding_valid"] is True
    assert status["active_inventory_evaluator_ids"] == (
        "lane_geometry_quality",
    )
    assert status["inventory_binding_valid"] is True
    assert status["fully_validated_evaluator_ids"] == (
        "lane_geometry_quality",
    )
    assert set(status["evaluators"]["lane_geometry_quality"]["verified_artifacts"]) == {
        "owner_source",
        "real_positive",
        "real_negative",
        "synthetic_boundary",
    }
    negative_path = (
        path.parent
        / payload["evaluators"][0]["dimensions"]["real_negative"]["artifact_path"]
    )
    negative = json.loads(negative_path.read_text(encoding="utf-8"))
    assert negative["cases"][0]["evaluator_observed_status"] == "FAIL"
    assert negative["cases"][0]["result"] == "PASS"
    assert result.confidence_tier == "high"
    assert result.publication_allowed is True


def test_registry_rejects_nonexistent_validation_artifact(tmp_path):
    path = tmp_path / "registry.json"
    payload = _materialize_registry(
        path,
        _registry_payload(
            _golden_entry("lane_geometry_quality"),
            required=["lane_geometry_quality"],
        ),
    )
    positive = payload["evaluators"][0]["dimensions"]["real_positive"]
    (path.parent / positive["artifact_path"]).unlink()

    status = oracle_module.release_golden_registry_status(path)
    result = evaluate_structural_tier(
        _contract(),
        golden_registry_status=status,
    )

    assert status["valid"] is False
    assert status["fully_validated_evaluator_ids"] == ()
    assert status["invalid_validation_artifact_evaluator_ids"] == (
        "lane_geometry_quality",
    )
    assert "real_positive_artifact_unreadable" in status[
        "validation_artifact_errors_by_evaluator"
    ]["lane_geometry_quality"]
    assert result.confidence_tier == "medium"
    assert result.terminal_class == CANDIDATE_HYPOTHESIS


def test_registry_rejects_validation_artifact_hash_mismatch(tmp_path):
    path = tmp_path / "registry.json"
    payload = _materialize_registry(
        path,
        _registry_payload(
            _golden_entry("lane_geometry_quality"),
            required=["lane_geometry_quality"],
        ),
    )
    negative = payload["evaluators"][0]["dimensions"]["real_negative"]
    artifact_path = path.parent / negative["artifact_path"]
    artifact_path.write_bytes(artifact_path.read_bytes() + b" \n")

    status = oracle_module.release_golden_registry_status(path)

    assert status["valid"] is False
    assert "real_negative_artifact_hash_mismatch" in status[
        "validation_artifact_errors_by_evaluator"
    ]["lane_geometry_quality"]


def test_registry_rejects_positive_negative_artifact_swap(tmp_path):
    path = tmp_path / "registry.json"
    payload = _materialize_registry(
        path,
        _registry_payload(
            _golden_entry("lane_geometry_quality"),
            required=["lane_geometry_quality"],
        ),
    )
    entry = payload["evaluators"][0]
    positive = entry["dimensions"]["real_positive"]
    negative = entry["dimensions"]["real_negative"]
    positive_ref = (positive["artifact_path"], positive["artifact_sha256"])
    negative_ref = (negative["artifact_path"], negative["artifact_sha256"])
    positive["artifact_path"], positive["artifact_sha256"] = negative_ref
    negative["artifact_path"], negative["artifact_sha256"] = positive_ref
    entry["positive_golden_sha256"] = negative_ref[1]
    entry["negative_golden_sha256"] = positive_ref[1]
    path.write_text(json.dumps(payload), encoding="utf-8")

    status = oracle_module.release_golden_registry_status(path)

    assert status["valid"] is False
    errors = status["validation_artifact_errors_by_evaluator"][
        "lane_geometry_quality"
    ]
    assert "validation_artifact_dimension_mismatch:real_positive" in errors
    assert "validation_artifact_dimension_mismatch:real_negative" in errors


def test_registry_rejects_inactive_or_reverse_drift_evaluator(tmp_path):
    path = tmp_path / "registry.json"
    _materialize_registry(
        path,
        _registry_payload(
            _golden_entry("acc_speed_convergence"),
            required=["acc_speed_convergence"],
        ),
        active_ids=["lane_geometry_quality"],
    )

    status = oracle_module.release_golden_registry_status(path)

    assert status["valid"] is False
    assert status["fully_validated_evaluator_ids"] == ()
    assert status["inactive_validation_evaluator_ids"] == (
        "acc_speed_convergence",
    )


@pytest.mark.parametrize(
    ("field", "value", "expected_error"),
    [
        (
            "schema_version",
            "g1q3_rca_validation_dimension_artifact_v0",
            "validation_artifact_schema_mismatch:real_positive",
        ),
        (
            "dimension",
            "real_negative",
            "validation_artifact_dimension_mismatch:real_positive",
        ),
    ],
)
def test_registry_rejects_artifact_schema_or_dimension_mismatch(
    tmp_path,
    field,
    value,
    expected_error,
):
    path = tmp_path / "registry.json"
    payload = _materialize_registry(
        path,
        _registry_payload(
            _golden_entry("lane_geometry_quality"),
            required=["lane_geometry_quality"],
        ),
    )
    _rewrite_dimension_artifact(
        path,
        payload,
        "real_positive",
        lambda artifact: artifact.__setitem__(field, value),
    )

    status = oracle_module.release_golden_registry_status(path)

    assert status["valid"] is False
    assert expected_error in status["validation_artifact_errors_by_evaluator"][
        "lane_geometry_quality"
    ]


def test_registry_rejects_real_negative_with_positive_case_semantics(tmp_path):
    path = tmp_path / "registry.json"
    payload = _materialize_registry(
        path,
        _registry_payload(
            _golden_entry("lane_geometry_quality"),
            required=["lane_geometry_quality"],
        ),
    )

    def make_false_positive(artifact):
        artifact["cases"][0]["expected_evaluator_status"] = "PASS"
        artifact["cases"][0]["evaluator_observed_status"] = "PASS"

    _rewrite_dimension_artifact(
        path,
        payload,
        "real_negative",
        make_false_positive,
    )

    status = oracle_module.release_golden_registry_status(path)

    assert status["valid"] is False
    assert "validation_artifact_real_outcome_semantics_invalid:real_negative" in status[
        "validation_artifact_errors_by_evaluator"
    ]["lane_geometry_quality"]


def test_registry_rejects_symlinked_artifact(tmp_path):
    path = tmp_path / "registry.json"
    payload = _materialize_registry(
        path,
        _registry_payload(
            _golden_entry("lane_geometry_quality"),
            required=["lane_geometry_quality"],
        ),
    )
    boundary = payload["evaluators"][0]["dimensions"]["synthetic_boundary"]
    artifact_path = path.parent / boundary["artifact_path"]
    target = artifact_path.with_name("boundary-target.json")
    target.write_bytes(artifact_path.read_bytes())
    artifact_path.unlink()
    artifact_path.symlink_to(target)

    status = oracle_module.release_golden_registry_status(path)

    assert status["valid"] is False
    assert "synthetic_boundary_artifact_symlink_forbidden" in status[
        "validation_artifact_errors_by_evaluator"
    ]["lane_geometry_quality"]


def test_registry_requires_exact_ordered_three_dimension_schema(tmp_path):
    payload = _registry_payload()
    payload["required_dimensions"] = [
        "real_negative",
        "real_positive",
        "synthetic_boundary",
    ]
    path = tmp_path / "registry.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    status = oracle_module.release_golden_registry_status(path)

    assert status["valid"] is False
    assert "required_dimensions_order_invalid" in status["golden_scope_errors"]


def test_explicit_inventory_rejects_missing_active_evaluator(tmp_path):
    path = tmp_path / "registry.json"
    _materialize_registry(
        path,
        _registry_payload(_golden_entry("lane_geometry_quality")),
        active_ids=["lane_geometry_quality", "new_evaluator"],
    )

    status = oracle_module.release_golden_registry_status(
        path,
        required_evaluator_ids=["lane_geometry_quality", "new_evaluator"],
    )

    assert status["valid"] is False
    assert status["low_tier_golden_ready"] is True
    assert status["missing_required_evaluator_ids"] == ("new_evaluator",)
    assert "required_evaluator_missing" in status["inventory_binding_errors"]


@pytest.mark.parametrize(
    "required, expected_error",
    [
        ([], "required_evaluator_ids_empty"),
        (["lane_geometry_quality", "lane_geometry_quality"], "required_evaluator_ids_duplicate"),
        ([""], "required_evaluator_id_invalid"),
    ],
)
def test_explicit_inventory_rejects_empty_or_duplicate_ids(
    tmp_path, required, expected_error
):
    path = tmp_path / "registry.json"
    path.write_text(
        json.dumps(_registry_payload(_golden_entry("lane_geometry_quality"), required=required)),
        encoding="utf-8",
    )

    status = oracle_module.release_golden_registry_status(path)

    assert status["valid"] is False
    assert expected_error in status["inventory_binding_errors"]


def test_registry_rejects_empty_and_duplicate_evaluator_entries(tmp_path):
    path = tmp_path / "registry.json"
    path.write_text(
        json.dumps(
            _registry_payload(
                _golden_entry("lane_geometry_quality"),
                _golden_entry("lane_geometry_quality", hash_char="e"),
                _golden_entry("", hash_char="i"),
            )
        ),
        encoding="utf-8",
    )

    status = oracle_module.release_golden_registry_status(path)

    assert status["valid"] is False
    assert status["duplicate_evaluator_ids"] == ("lane_geometry_quality",)
    assert status["invalid_evaluator_ids"] == ("",)


def test_registry_rejects_non_distinct_controlled_evaluator_hashes(tmp_path):
    entry = _golden_entry("lane_geometry_quality")
    entry["test_receipt_sha256"] = entry["positive_golden_sha256"]
    path = tmp_path / "registry.json"
    path.write_text(json.dumps(_registry_payload(entry)), encoding="utf-8")

    status = oracle_module.release_golden_registry_status(path)

    assert status["valid"] is False
    assert status["non_distinct_evaluator_ids"] == ("lane_geometry_quality",)


@pytest.mark.parametrize("source_kind", ["machine_observation", "synthetic"])
def test_registry_rejects_machine_or_synthetic_observations_as_goldens(
    tmp_path, source_kind
):
    entry = _golden_entry("lane_geometry_quality")
    entry["source_kind"] = source_kind
    path = tmp_path / "registry.json"
    path.write_text(json.dumps(_registry_payload(entry)), encoding="utf-8")

    status = oracle_module.release_golden_registry_status(path)

    assert status["valid"] is False
    assert status["evaluators"] == {}
    assert status["invalid_golden_source_ids"] == ("lane_geometry_quality",)
    if source_kind == "machine_observation":
        assert status["machine_observation_evaluator_ids"] == (
            "lane_geometry_quality",
        )


def test_registry_requires_owner_grounded_source_for_high_scope(tmp_path):
    entry = _golden_entry("lane_geometry_quality")
    entry.pop("source_kind")
    path = tmp_path / "registry.json"
    path.write_text(json.dumps(_registry_payload(entry)), encoding="utf-8")

    status = oracle_module.release_golden_registry_status(path)

    assert status["valid"] is False
    assert status["evaluators"] == {}
    assert status["invalid_golden_source_ids"] == ("lane_geometry_quality",)


def test_registry_accounts_missing_validation_dimension_without_unlocking_scope(tmp_path):
    entry = _golden_entry("lane_geometry_quality")
    entry["status"] = "pending"
    entry["dimensions"].pop("synthetic_boundary")
    entry["fully_validated"] = False
    entry["missing_dimensions"] = ["synthetic_boundary"]
    path = tmp_path / "registry.json"
    _materialize_registry(path, _registry_payload(entry))

    status = oracle_module.release_golden_registry_status(path)

    assert status["valid"] is True
    assert status["evaluators"]["lane_geometry_quality"]["fully_validated"] is False
    assert status["fully_validated_evaluator_ids"] == ()
    assert status["incomplete_evaluator_ids"] == ("lane_geometry_quality",)
    assert status["missing_dimensions_by_evaluator"]["lane_geometry_quality"] == (
        "synthetic_boundary",
    )
    assert status["golden_scope_evaluator_ids"] == ()


def test_validation_dimension_accounting_lie_invalidates_registry(tmp_path):
    entry = _golden_entry("lane_geometry_quality")
    entry["dimensions"].pop("synthetic_boundary")
    # Claiming complete while the required dimension is absent is not a
    # downgrade; it is an invalid registry and must fail closed.
    entry["fully_validated"] = True
    entry["missing_dimensions"] = []
    path = tmp_path / "registry.json"
    path.write_text(json.dumps(_registry_payload(entry)), encoding="utf-8")

    status = oracle_module.release_golden_registry_status(path)

    assert status["valid"] is False
    assert status["invalid_validation_evaluator_ids"] == ("lane_geometry_quality",)


def test_incomplete_three_dimension_entry_keeps_candidate_at_medium_tier(tmp_path):
    entry = _golden_entry("lane_geometry_quality")
    entry["status"] = "pending"
    entry["dimensions"].pop("real_negative")
    entry["fully_validated"] = False
    entry["missing_dimensions"] = ["real_negative"]
    path = tmp_path / "registry.json"
    _materialize_registry(path, _registry_payload(entry))
    registry = oracle_module.release_golden_registry_status(path)

    result = evaluate_structural_tier(
        _contract(),
        golden_registry_status=registry,
    )

    assert result.terminal_class == CANDIDATE_HYPOTHESIS
    assert result.confidence_tier == "medium"
    assert result.facts.evaluator_validation_complete is False
    assert result.facts.evaluator_validation_missing_dimensions == (
        "lane_geometry_quality:real_negative",
    )


def _contract(
    *,
    evaluator_status: str = "supported",
    refs: bool = True,
    conclusion: str = "车道线横向跳变经控制链传导，导致车道保持不稳。",
    candidate: bool = False,
) -> dict:
    if candidate:
        conclusion += " 当前为候选方向，需人工复核。"
    return {
        "upstream_dispatch": {
            "hit_evaluator_keys": ["lane_geometry_quality"],
            "hit_window_envelope": None,
            "hit_windows": [],
            "owner_bucket": "lane_perception",
            "owner_bucket_label": "车道线感知",
            "reason": "single_owner_bucket_hit",
            "schema_version": "g1q3_upstream_dispatch_v2",
            "terminal_classification": "valid_dispatch",
        },
        "consumer_capability": {
            "actual_evaluators": [
                {
                    "evaluator_id": "lane_geometry_quality",
                    "status": evaluator_status,
                    "decode_status": "decoded",
                    "evidence_role": "decoded_evaluator",
                }
            ],
            "unused_capabilities": [
                {
                    "evaluator_id": "inventory_only_alias",
                    "status": "not_invoked",
                    "reason": "not applicable",
                }
            ],
            "evidence": {
                "issue_frame_id": 160304,
                "focus_window": {"start_ts": 0.0, "end_ts": 1.0},
                "field_lineage": {
                    "schema_version": "g1q3_field_lineage_v2",
                    "fidelity_ok": True,
                    "status": "pass",
                },
                "viz_lineage": {
                    "schema_version": "g1q3_viz_lineage_v1",
                    "ok": True,
                    "status": "pass",
                },
            },
        },
        "report": {
            "candidate_owner_domain": "PERCEPTION_LANE",
            "is_candidate": candidate,
        },
        "public_result": {
            "summary": {"short_conclusion": conclusion},
            "candidate": "PERCEPTION_LANE",
            "responsibility": {"status": "candidate" if candidate else "supported"},
            "evidence_summary": {
                "refs": ([{"evidence_ref": "frame:160304/lane"}] if refs else [])
            },
            "causal_chain": {
                "narrative": [
                    {"role": "现象", "text": "车道保持不稳。"},
                    {"role": "证据", "text": "车道线横向跳变。"},
                    {"role": "因果判断", "text": conclusion},
                ]
            },
            "user_action": {},
        },
    }


def test_supported_attribution_requires_emitted_supported_key_and_full_structure(
    monkeypatch,
):
    monkeypatch.setattr(
        oracle_module,
        "release_golden_registry_status",
        lambda: _release_registry(),
    )
    result = evaluate_structural_tier(
        _contract(),
        publication_text=(
            "归因结论：车道线异常经控制链传导。\n"
            "责任模块：PERCEPTION_LANE\n"
            "因果关系：车道线异常导致车道保持不稳。\n"
            "关键证据：frame:160304/lane"
        ),
    )

    assert result.terminal_class == SUPPORTED_ATTRIBUTION
    assert result.confidence_tier == "high"
    assert result.publication_allowed is True
    assert result.facts.supported_evaluator_keys == ("lane_geometry_quality",)
    assert result.facts.evidence_complete is True
    assert result.facts.causal_chain_closed is True


@pytest.mark.parametrize("status", ["refuted", "likely", "unknown"])
def test_only_actual_supported_status_counts_as_an_evaluator_hit(status):
    result = evaluate_structural_tier(_contract(evaluator_status=status))

    assert result.terminal_class == HONEST_NON_ATTRIBUTION
    assert result.facts.supported_evaluator_count == 0
    assert "inventory_only_alias" not in result.facts.supported_evaluator_keys


def test_candidate_requires_exact_medium_disclaimer_at_publication():
    contract = _contract(refs=False, candidate=True)
    missing = evaluate_structural_tier(contract, publication_text="候选结论。")
    compliant = evaluate_structural_tier(
        contract,
        publication_text=f"置信说明：{MEDIUM_TIER_DISCLAIMER}\n候选结论。",
    )

    assert missing.terminal_class == CANDIDATE_HYPOTHESIS
    assert missing.publication_allowed is False
    assert "candidate_disclaimer_missing" in missing.violations
    assert compliant.publication_allowed is True


def test_explicit_candidate_flag_prevents_high_promotion():
    contract = _contract()
    contract["report"]["is_candidate"] = True

    result = evaluate_structural_tier(contract)

    assert result.terminal_class == CANDIDATE_HYPOTHESIS
    assert result.confidence_tier == "medium"


def test_live_candidate_status_prevents_high_promotion():
    contract = _contract()
    contract["public_result"]["responsibility"]["status"] = "candidate_from_live_rca"

    result = evaluate_structural_tier(contract)

    assert result.terminal_class == CANDIDATE_HYPOTHESIS


def test_string_evidence_refs_do_not_count_as_complete_evidence():
    contract = _contract()
    contract["public_result"]["evidence_summary"]["refs"] = "frame:160304/lane"

    result = evaluate_structural_tier(contract)

    assert result.terminal_class == CANDIDATE_HYPOTHESIS
    assert result.facts.evidence_ref_count == 0
    assert result.facts.evidence_complete is False


def test_summary_only_evidence_item_does_not_count_as_evidence_ref():
    contract = _contract()
    contract["public_result"]["evidence_summary"]["refs"] = [
        {"summary": "a narrative is not an evidence reference"}
    ]

    result = evaluate_structural_tier(contract)

    assert result.terminal_class == CANDIDATE_HYPOTHESIS
    assert result.facts.evidence_ref_count == 0


def test_missing_release_controlled_evaluator_goldens_prevents_high_tier():
    contract = _contract()

    result = evaluate_structural_tier(contract)

    assert result.terminal_class == CANDIDATE_HYPOTHESIS
    assert result.facts.golden_coverage_complete is False


def test_evaluator_outside_golden_scope_never_reaches_high_tier(monkeypatch):
    monkeypatch.setattr(
        oracle_module,
        "release_golden_registry_status",
        lambda: _release_registry("acc_decel_heavy"),
    )

    result = evaluate_structural_tier(_contract())

    assert result.terminal_class == CANDIDATE_HYPOTHESIS
    assert result.confidence_tier == "medium"
    assert result.facts.golden_coverage_complete is False


def test_producer_self_attested_golden_hashes_cannot_unlock_high_tier():
    contract = _contract()
    contract["consumer_capability"]["golden_coverage"] = {
        "schema_version": "pnc_rca_evaluator_golden_coverage_v1",
        "evaluators": [
            {
                "evaluator_id": "lane_geometry_quality",
                "status": "passed",
                "positive_golden_sha256": "a" * 64,
                "negative_golden_sha256": "b" * 64,
            }
        ],
    }

    result = evaluate_structural_tier(contract)

    assert result.terminal_class == CANDIDATE_HYPOTHESIS
    assert result.facts.golden_covered_evaluator_keys == ()


def test_invalid_evidence_types_and_contradictory_viz_fail_closed():
    contract = _contract()
    evidence = contract["consumer_capability"]["evidence"]
    evidence["issue_frame_id"] = False
    evidence["focus_window"] = {"start_ts": False, "end_ts": False}
    evidence["viz_lineage"] = {
        "ok": False,
        "status": "completed",
        "errors": ["render_failed"],
    }

    result = evaluate_structural_tier(contract)

    assert result.terminal_class == CANDIDATE_HYPOTHESIS
    assert result.facts.issue_frame_present is False
    assert result.facts.focus_window_present is False
    assert result.facts.viz_lineage_complete is False


def test_contradictory_pass_boole_and_failure_statuses_are_not_complete():
    contract = _contract()
    evidence = contract["consumer_capability"]["evidence"]
    evidence["field_lineage"]["status"] = "failed"
    evidence["viz_lineage"].update(ok=True, status="failed", errors=[])

    result = evaluate_structural_tier(contract)

    assert result.terminal_class == CANDIDATE_HYPOTHESIS
    assert result.facts.field_lineage_complete is False
    assert result.facts.viz_lineage_complete is False


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(10**10000, id="huge-int"),
        pytest.param(float("nan"), id="nan"),
        pytest.param(float("inf"), id="positive-inf"),
        pytest.param(-float("inf"), id="negative-inf"),
    ],
)
def test_unbounded_or_nonfinite_focus_values_are_invalid_not_exceptions(value):
    contract = _contract()
    contract["consumer_capability"]["evidence"]["focus_window"] = {
        "start_ts": value,
        "end_ts": value,
    }

    result = evaluate_structural_tier(contract)

    assert result.terminal_class == CANDIDATE_HYPOTHESIS
    assert result.facts.focus_window_present is False


def test_partial_focus_window_does_not_count_as_complete_evidence():
    contract = _contract()
    contract["consumer_capability"]["evidence"]["focus_window"] = {"start_ts": 0.0}

    result = evaluate_structural_tier(contract)

    assert result.terminal_class == CANDIDATE_HYPOTHESIS
    assert result.facts.focus_window_present is False


def test_non_attribution_wording_can_only_be_honest_non_attribution():
    contract = _contract(
        conclusion="自动RCA未归因：当前证据不能确认归因。",
    )
    result = evaluate_structural_tier(
        contract,
        claimed_terminal_class=SUPPORTED_ATTRIBUTION,
        publication_text=(
            "归因结论：系统已完成现有证据分析，但未形成可确认的归因结论。\n"
            "责任模块：暂无法判断。\n因果关系：现有证据不足以闭合责任因果链。\n"
            "关键证据：证据仅支持记录分析边界。"
        ),
    )

    assert result.terminal_class == HONEST_NON_ATTRIBUTION
    assert result.publication_allowed is False
    assert (
        "terminal_class_mismatch:supported_attribution:honest_non_attribution"
        in result.violations
    )
    assert "supported_attribution_non_attribution_wording" in result.violations


def test_candidate_publication_cannot_mix_in_non_attribution_wording():
    contract = _contract(refs=False, candidate=True)

    result = evaluate_structural_tier(
        contract,
        publication_text=(
            f"置信说明：{MEDIUM_TIER_DISCLAIMER}\n自动RCA未归因。"
        ),
    )

    assert result.terminal_class == HONEST_NON_ATTRIBUTION
    assert result.publication_allowed is False
    assert "honest_non_attribution_candidate_wording" in result.violations


def test_non_attribution_boundary_prevents_supported_promotion():
    contract = _contract()
    contract["public_result"]["evidence_boundary"] = ["自动RCA未归因：边界证据不足。"]

    result = evaluate_structural_tier(contract)

    assert result.terminal_class == HONEST_NON_ATTRIBUTION
    assert result.facts.explicit_non_attribution is True


@pytest.mark.parametrize(
    "error_code",
    [
        "business_route_unresolved",
        "business_profile_unsupported",
        "business_profile_adapter_not_ready",
    ],
)
def test_route_boundary_stays_honest_low_when_execution_is_terminal_failed(
    error_code,
):
    result = evaluate_structural_tier(
        {},
        execution_outcome="terminal_failed",
        terminal_error_code=error_code,
    )

    assert result.terminal_class == HONEST_NON_ATTRIBUTION
    assert result.confidence_tier == "low"


def test_serialized_approval_ready_without_human_decision_is_blocked():
    result = evaluate_structural_tier(
        _contract(refs=False, candidate=True),
        publication_text=(
            f"置信说明：{MEDIUM_TIER_DISCLAIMER}\n"
            '{"approval_ready": true}'
        ),
    )

    assert result.publication_allowed is False
    assert "approval_ready_without_human_decision" in result.violations


def test_supported_claim_with_zero_evaluator_fails_closed():
    contract = _contract(evaluator_status="refuted", refs=False)
    result = evaluate_structural_tier(
        contract,
        claimed_terminal_class=SUPPORTED_ATTRIBUTION,
    )

    assert result.facts.supported_evaluator_count == 0
    assert "supported_attribution_evaluator_count_zero" in result.violations
    with pytest.raises(TierOracleConflict):
        require_publishable(result)


def test_empty_human_decision_cannot_claim_approval_ready():
    result = evaluate_structural_tier(
        _contract(),
        human_decision="",
        approval_ready=True,
    )

    assert "approval_ready_without_human_decision" in result.violations
    assert result.publication_allowed is False


@pytest.mark.parametrize(
    "publication_text", ("quality-approved", "approval_ready=true")
)
def test_empty_human_decision_rejects_approval_ready_text(publication_text):
    result = evaluate_structural_tier(
        _contract(),
        human_decision="",
        publication_text=publication_text,
    )

    assert "approval_ready_without_human_decision" in result.violations
    assert result.publication_allowed is False


def test_low_tier_rejects_user_action_and_blame_wording():
    contract = _contract(evaluator_status="refuted", refs=False)
    contract["public_result"]["candidate"] = ""
    contract["public_result"]["responsibility"] = {"status": "unsupported"}
    result = evaluate_structural_tier(
        contract,
        publication_text=(
            "归因结论：问题单缺少问题数据地址。\n"
            "责任模块：暂无法判断。\n请补齐后重新发起。"
        ),
    )

    assert result.terminal_class == HONEST_NON_ATTRIBUTION
    assert "honest_non_attribution_user_action" in result.violations
    assert "honest_non_attribution_blame_wording" in result.violations


def test_low_tier_rejects_named_responsibility_in_sealed_public_contract():
    contract = _contract(
        evaluator_status="refuted",
        refs=False,
        conclusion="自动RCA未归因：现有证据不能确认归因。",
    )

    result = evaluate_structural_tier(contract)

    assert result.terminal_class == HONEST_NON_ATTRIBUTION
    assert "honest_non_attribution_responsibility_present" in result.violations
    assert result.publication_allowed is False


def test_banned_phrase_is_rejected_even_when_renderer_would_hide_it():
    contract = _contract(
        evaluator_status="refuted",
        refs=False,
        conclusion="自动RCA未归因：请核对问题数据地址。",
    )
    result = evaluate_structural_tier(
        contract,
        publication_text=(
            "归因结论：系统已完成现有证据分析，但未形成可确认的归因结论。\n"
            "责任模块：暂无法判断。"
        ),
    )

    assert "banned_public_phrase:请核对问题数据地址" in result.violations
    assert result.publication_allowed is False


def test_execution_and_consumer_failures_are_distinct_nonpublishable_terminals():
    technical = evaluate_structural_tier(
        _contract(),
        execution_outcome="terminal_failed",
    )
    consumer = evaluate_structural_tier(
        _contract(),
        consumer_delivery_status="readback_failed",
    )

    assert technical.terminal_class == TECHNICAL_FAILURE
    assert technical.publication_allowed is False
    assert consumer.terminal_class == CONSUMER_DELIVERY_FAILURE
    assert consumer.publication_allowed is False


def test_unknown_terminal_claim_fails_closed():
    result = evaluate_structural_tier(
        _contract(),
        claimed_terminal_class="model_confident",
    )

    assert "terminal_class_invalid:model_confident" in result.violations
    assert result.publication_allowed is False


def test_route_boundary_is_honest_non_attribution_not_technical_failure():
    result = evaluate_structural_tier(
        _contract(evaluator_status="refuted", refs=False),
        terminal_error_code="business_route_unresolved",
    )

    assert result.terminal_class == HONEST_NON_ATTRIBUTION
