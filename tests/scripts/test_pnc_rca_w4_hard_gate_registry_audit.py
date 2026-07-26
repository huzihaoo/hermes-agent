from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from scripts import pnc_rca_w4_hard_gate_registry_audit as audit


BASE_COMMIT = "20ea44bdfaa862b2a366c33bc90552f1fcb13557"
BASE_TREE = "5d009bc14e197694bc5f03af16c8c5e9ff60af37"
CURRENT_CANDIDATE_COMMIT = "1158a49140bd4459d5fbff4ca91cdea9875cd8b1"
CURRENT_CANDIDATE_TREE = "0cc6f8db18b9af8e60657cb662726113c4231fd7"
LEGACY_CANDIDATE_COMMIT = "d94bddf886c864a25adf341d89ef17102e903c19"
LEGACY_CANDIDATE_TREE = "95b53a350eb6d30a857eaaf3d77d3315c0ec2722"


def _site(index: int) -> dict[str, object]:
    return {
        "file": "api/g1q3_rca/scripts/run_rca_auto_pipeline.py",
        "line": index + 1,
        "call": "<dict-literal>",
        "keyword": "kind",
        "value": f"blocker_{index}",
        "locator": f"blocker-site-v1-{index:064x}",
    }


def _inventory(
    *,
    candidate_count: int = 154,
    candidate_commit: str = CURRENT_CANDIDATE_COMMIT,
    candidate_tree: str = CURRENT_CANDIDATE_TREE,
) -> dict[str, object]:
    base_count = min(149, candidate_count)
    base_sites = [_site(index) for index in range(base_count)]
    candidate_sites = [_site(index) for index in range(candidate_count)]

    def section(
        commit: str, tree: str, sites: list[dict[str, object]]
    ) -> dict[str, object]:
        return {
            "commit": commit,
            "tree": tree,
            "literal_emission_sites": copy.deepcopy(sites),
            "literal_emission_summary": {
                "site_count": len(sites),
                "emitting_file_count": 15,
                "literal_value_count": 115 if len(sites) == 154 else 113,
            },
        }

    added_sites = candidate_sites[base_count:]
    return {
        "schema_version": audit.INVENTORY_SCHEMA_VERSION,
        "base": section(BASE_COMMIT, BASE_TREE, base_sites),
        "candidate": section(candidate_commit, candidate_tree, candidate_sites),
        "delta": {
            "authoritative_literal_emissions": {
                "base_count": base_count,
                "candidate_count": candidate_count,
                "added_count": len(added_sites),
                "removed_count": 0,
                "added": copy.deepcopy(added_sites),
                "removed": [],
            }
        },
    }


def _pair(
    *,
    candidate_count: int = 154,
    candidate_commit: str = CURRENT_CANDIDATE_COMMIT,
    candidate_tree: str = CURRENT_CANDIDATE_TREE,
) -> tuple[dict[str, object], dict[str, object]]:
    inventory = _inventory(
        candidate_count=candidate_count,
        candidate_commit=candidate_commit,
        candidate_tree=candidate_tree,
    )
    binding = audit._inventory_binding(inventory)
    locators = list(binding["candidate_locators"])
    rows = []
    for index, locator in enumerate(locators):
        gate_ids = [f"gate-{index}"] if index < 3 else []
        rows.append({
            "locator": locator,
            "final_decision": "hard_gate" if index < 3 else "observation",
            "decision_ref": f"owner-row-{index}",
            "trigger_condition": f"blocker code {index} is emitted",
            "count_scope": "code_aggregate",
            "live_count": index,
            "live_evidence_ref": f"receipt://w4/row/{index}",
            "live_evidence_sha256": "a" * 64,
            "hard_gate_ids": gate_ids,
        })
    registry = {
        "schema_version": audit.REGISTRY_SCHEMA_VERSION,
        "inventory_binding": {
            "base_commit": binding["base_commit"],
            "base_tree": binding["base_tree"],
            "base_site_count": binding["base_site_count"],
            "candidate_commit": binding["candidate_commit"],
            "candidate_tree": binding["candidate_tree"],
            "candidate_site_count": binding["candidate_site_count"],
            "inventory_sha256": binding["inventory_sha256"],
        },
        "owner_approval": {
            "phrase": audit.REQUIRED_OWNER_PHRASE,
            "approval_ref": "owner-packet:W4-C1-C8",
            "status": "approved",
        },
        "rows": rows,
        "hard_gates": [
            {
                "gate_id": f"gate-{index}",
                "category": category,
                "source_locators": [locators[index]],
                "enforcement_ref": f"scripts/enforce_gate_{index}.py:1",
                "negative_test_ref": f"tests/scripts/test_gate_{index}.py::test_negative",
            }
            for index, category in enumerate(("identity", "execution", "publication"))
        ],
    }
    return inventory, registry


def _expected_face() -> dict[str, str]:
    return {
        "base_commit": BASE_COMMIT,
        "base_tree": BASE_TREE,
        "candidate_commit": CURRENT_CANDIDATE_COMMIT,
        "candidate_tree": CURRENT_CANDIDATE_TREE,
    }


def _audit(
    inventory: dict[str, object],
    registry: dict[str, object],
    *,
    expected_face: dict[str, str] | None = None,
    expected_site_count: int | None = 154,
) -> dict[str, object]:
    return audit.audit_registry(
        inventory,
        registry,
        expected_face=_expected_face() if expected_face is None else expected_face,
        expected_site_count=expected_site_count,
    )


def _codes(report: dict[str, object]) -> set[str]:
    return {str(item["code"]) for item in report["errors"]}  # type: ignore[index]


def test_current_face_complete_registry_is_contract_ready_but_not_ga():
    inventory, registry = _pair()

    report = _audit(inventory, registry)

    assert report["registry_contract_ready"] is True
    assert report["ok"] is True
    assert report["live_acceptance_verified"] is False
    assert report["inventory"]["base_site_count"] == 149  # type: ignore[index]
    assert report["inventory"]["candidate_site_count"] == 154  # type: ignore[index]
    assert report["registry"]["row_count"] == 154  # type: ignore[index]
    assert report["registry"]["hard_gate_count"] == 3  # type: ignore[index]


def test_missing_row_is_not_adjudicated():
    inventory, registry = _pair()
    registry["rows"].pop()  # type: ignore[index]

    report = _audit(inventory, registry)

    assert report["registry_contract_ready"] is False
    assert {"registry_rows_missing", "registry_row_count_invalid"} <= _codes(report)


def test_duplicate_locator_is_not_adjudicated():
    inventory, registry = _pair()
    registry["rows"][1]["locator"] = registry["rows"][0]["locator"]  # type: ignore[index]

    report = _audit(inventory, registry)

    assert report["registry_contract_ready"] is False
    assert "registry_row_locator_duplicate" in _codes(report)


def test_pending_or_unknown_final_decision_is_rejected():
    inventory, registry = _pair()
    registry["rows"][0]["final_decision"] = "pending_owner"  # type: ignore[index]

    report = _audit(inventory, registry)

    assert "registry_row_final_decision_invalid" in _codes(report)


@pytest.mark.parametrize(
    ("field", "error"),
    [
        ("trigger_condition", "registry_row_trigger_condition_missing"),
        ("live_evidence_ref", "registry_row_live_evidence_ref_missing"),
        ("live_evidence_sha256", "registry_row_live_evidence_sha256_invalid"),
    ],
)
def test_required_row_evidence_fields_are_enforced(field, error):
    inventory, registry = _pair()
    registry["rows"][0][field] = ""  # type: ignore[index]

    report = _audit(inventory, registry)

    assert error in _codes(report)


def test_observation_row_cannot_bypass_gate_registry():
    inventory, registry = _pair()
    registry["rows"][0]["final_decision"] = "observation"  # type: ignore[index]

    report = _audit(inventory, registry)

    assert "registry_observation_row_has_gate" in _codes(report)


def test_hard_gate_row_requires_gate():
    inventory, registry = _pair()
    registry["rows"][0]["hard_gate_ids"] = []  # type: ignore[index]

    report = _audit(inventory, registry)

    assert "registry_hard_gate_row_without_gate" in _codes(report)


def test_unknown_gate_reference_fails_closed():
    inventory, registry = _pair()
    registry["rows"][0]["hard_gate_ids"] = ["does-not-exist"]  # type: ignore[index]

    report = _audit(inventory, registry)

    assert report["registry_contract_ready"] is False
    assert "registry_row_gate_ref_unknown" in _codes(report)


def test_more_than_fifteen_hard_gates_fails_closed():
    inventory, registry = _pair()
    locators = [row["locator"] for row in registry["rows"]]  # type: ignore[index]
    gates = []
    categories = ("identity", "execution", "publication")
    for index in range(16):
        gate_id = f"extra-{index}"
        locator = locators[index]
        registry["rows"][index]["final_decision"] = "hard_gate"  # type: ignore[index]
        registry["rows"][index]["hard_gate_ids"] = [gate_id]  # type: ignore[index]
        gates.append({
            "gate_id": gate_id,
            "category": categories[index % len(categories)],
            "source_locators": [locator],
            "enforcement_ref": f"enforce-{index}",
            "negative_test_ref": f"negative-{index}",
        })
    registry["hard_gates"] = gates

    report = _audit(inventory, registry)

    assert report["registry_contract_ready"] is False
    assert "registry_hard_gate_count_exceeded" in _codes(report)


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("candidate_commit", "a" * 40, "registry_candidate_commit_mismatch"),
        ("candidate_tree", "b" * 40, "registry_candidate_tree_mismatch"),
        ("inventory_sha256", "c" * 64, "registry_inventory_sha256_mismatch"),
    ],
)
def test_wrong_binding_provenance_fails_closed(field, value, error):
    inventory, registry = _pair()
    registry["inventory_binding"][field] = value  # type: ignore[index]

    report = _audit(inventory, registry)

    assert report["registry_contract_ready"] is False
    assert error in _codes(report)


def test_wrong_delta_is_not_accepted_as_current_face():
    inventory, registry = _pair()
    inventory["delta"]["authoritative_literal_emissions"]["added_count"] = 0  # type: ignore[index]

    report = _audit(inventory, registry)

    assert "inventory_delta_added_count_mismatch" in _codes(report)


def test_empty_delta_is_not_accepted_for_a_face_pair():
    inventory, registry = _pair()
    inventory["delta"]["authoritative_literal_emissions"] = {}  # type: ignore[index]

    report = _audit(inventory, registry)

    assert "inventory_authoritative_delta_missing" in _codes(report)


def test_delta_list_must_name_every_new_locator():
    inventory, registry = _pair()
    inventory["delta"]["authoritative_literal_emissions"]["added"] = []  # type: ignore[index]

    report = _audit(inventory, registry)

    assert "inventory_delta_added_locator_mismatch" in _codes(report)


@pytest.mark.parametrize(
    "owner_update",
    [
        {},
        {
            "phrase": audit.REQUIRED_OWNER_PHRASE,
            "approval_ref": "",
            "status": "approved",
        },
        {
            "phrase": "wrong phrase",
            "approval_ref": "owner-packet:W4-C1-C8",
            "status": "approved",
        },
        {
            "phrase": audit.REQUIRED_OWNER_PHRASE,
            "approval_ref": "owner-packet:W4-C1-C8",
            "status": "pending",
        },
    ],
)
def test_owner_evidence_is_required(owner_update):
    inventory, registry = _pair()
    registry["owner_approval"] = owner_update

    report = _audit(inventory, registry)

    assert report["registry_contract_ready"] is False
    assert _codes(report) & {
        "owner_approval_missing",
        "owner_approval_ref_missing",
        "owner_approval_phrase_invalid",
        "owner_approval_not_approved",
    }


def test_stale_legacy_149_face_cannot_pass_current_154_expectation():
    inventory, registry = _pair(
        candidate_count=149,
        candidate_commit=LEGACY_CANDIDATE_COMMIT,
        candidate_tree=LEGACY_CANDIDATE_TREE,
    )

    report = _audit(inventory, registry)

    assert report["registry_contract_ready"] is False
    assert {
        "inventory_expected_candidate_commit_mismatch",
        "inventory_expected_candidate_tree_mismatch",
        "inventory_expected_site_count_mismatch",
    } <= _codes(report)


def test_missing_expected_face_and_count_never_defaults_to_allow():
    inventory, registry = _pair()

    report = audit.audit_registry(inventory, registry)

    assert report["registry_contract_ready"] is False
    assert {
        "expected_inventory_face_missing",
        "expected_inventory_site_count_missing",
    } <= _codes(report)


def test_cli_emits_nonzero_for_stale_inventory(tmp_path: Path, capsys):
    inventory, registry = _pair(
        candidate_count=149,
        candidate_commit=LEGACY_CANDIDATE_COMMIT,
        candidate_tree=LEGACY_CANDIDATE_TREE,
    )
    inventory_path = tmp_path / "inventory.json"
    registry_path = tmp_path / "registry.json"
    inventory_path.write_text(json.dumps(inventory), encoding="utf-8")
    registry_path.write_text(json.dumps(registry), encoding="utf-8")

    exit_code = audit.main([
        "--inventory",
        str(inventory_path),
        "--registry",
        str(registry_path),
        "--expected-base-commit",
        BASE_COMMIT,
        "--expected-base-tree",
        BASE_TREE,
        "--expected-candidate-commit",
        CURRENT_CANDIDATE_COMMIT,
        "--expected-candidate-tree",
        CURRENT_CANDIDATE_TREE,
        "--expected-row-count",
        "154",
    ])

    assert exit_code == 2
    assert json.loads(capsys.readouterr().out)["registry_contract_ready"] is False
