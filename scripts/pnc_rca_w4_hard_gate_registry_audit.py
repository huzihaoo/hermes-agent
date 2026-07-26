#!/usr/bin/env python3
"""Read-only A4 validator for the W4 blocker adjudication registry.

The AST inventory describes the sites that need a decision; it does not make
those decisions.  This validator accepts an owner-produced registry but never
scaffolds or infers a row decision.  A structurally complete result is named
``registry_contract_ready`` only: live publication, HTTPS, and execution
evidence remain outside this local validator and cannot be promoted to GA.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA_VERSION = "pnc_rca_w4_hard_gate_registry_audit_v2"
REGISTRY_SCHEMA_VERSION = "pnc_rca_w4_hard_gate_registry_v2"
INVENTORY_SCHEMA_VERSION = "pnc_rca_blocker_literal_inventory_v1"
# The old d94 inventory was 149/15/113.  It is retained as a negative
# reference only; current row count and face are explicit CLI inputs.
LEGACY_REFERENCE_EMISSION_SITE_COUNT = 149
MAX_HARD_GATE_COUNT = 15
MIN_HARD_GATE_COUNT = 1
REQUIRED_OWNER_PHRASE = "采纳 W4 C1-C8 推荐值"
_HEX40_RE = re.compile(r"^[0-9a-f]{40}$")
_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
_GATE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_.:-]{0,127}$")
_FINAL_DECISIONS = frozenset({"hard_gate", "observation"})
_GATE_CATEGORIES = frozenset({"identity", "execution", "publication"})
_REGISTRY_FIELDS = frozenset({
    "schema_version",
    "inventory_binding",
    "owner_approval",
    "rows",
    "hard_gates",
})
_BINDING_FIELDS = frozenset({
    "base_commit",
    "base_tree",
    "base_site_count",
    "candidate_commit",
    "candidate_tree",
    "candidate_site_count",
    "inventory_sha256",
})
_EXPECTED_FACE_FIELDS = frozenset({
    "base_commit",
    "base_tree",
    "candidate_commit",
    "candidate_tree",
})
_OWNER_FIELDS = frozenset({"phrase", "approval_ref", "status"})
_ROW_FIELDS = frozenset({
    "locator",
    "final_decision",
    "decision_ref",
    "trigger_condition",
    "count_scope",
    "live_count",
    "live_evidence_ref",
    "live_evidence_sha256",
    "hard_gate_ids",
})
_GATE_FIELDS = frozenset({
    "gate_id",
    "category",
    "source_locators",
    "enforcement_ref",
    "negative_test_ref",
})


def _error(code: str, **detail: Any) -> dict[str, Any]:
    return {"code": code, **detail}


def _canonical_sha256(value: Mapping[str, Any]) -> str:
    raw = json.dumps(
        dict(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _load_json(path: Path, *, label: str) -> Mapping[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{label}_unreadable") from exc
    if not isinstance(payload, Mapping):
        raise ValueError(f"{label}_shape_invalid")
    return payload


def _required_mapping(value: Any, *, code: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(code)
    return value


def _required_text(value: Any, *, code: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(code)
    return text


def _validated_git_id(value: Any, *, code: str) -> str:
    text = str(value or "").strip().lower()
    if _HEX40_RE.fullmatch(text) is None:
        raise ValueError(code)
    return text


def _inventory_section(
    payload: Mapping[str, Any], *, section_name: str
) -> tuple[dict[str, Any], list[str]]:
    section = _required_mapping(
        payload.get(section_name), code="inventory_section_missing"
    )
    commit = _validated_git_id(section.get("commit"), code="inventory_commit_invalid")
    tree = _validated_git_id(section.get("tree"), code="inventory_tree_invalid")
    raw_sites = section.get("literal_emission_sites")
    if not isinstance(raw_sites, list):
        raise ValueError("inventory_sites_missing")

    locators: list[str] = []
    seen: set[str] = set()
    for ordinal, raw_site in enumerate(raw_sites):
        site = _required_mapping(raw_site, code="inventory_site_shape_invalid")
        locator = _required_text(site.get("locator"), code="inventory_locator_missing")
        if locator in seen:
            raise ValueError(
                f"inventory_locator_duplicate:{section_name}:{ordinal}:{locator}"
            )
        seen.add(locator)
        locators.append(locator)

    summary = section.get("literal_emission_summary")
    if isinstance(summary, Mapping) and summary.get("site_count") != len(locators):
        raise ValueError("inventory_summary_site_count_mismatch")
    return {"commit": commit, "tree": tree}, locators


def _delta_locators(value: Any, *, code: str) -> set[str]:
    if value is None:
        return set()
    if not isinstance(value, list):
        raise ValueError(code)
    locators: set[str] = set()
    for item in value:
        if isinstance(item, Mapping):
            locator = _required_text(item.get("locator"), code=code)
        elif isinstance(item, str):
            locator = _required_text(item, code=code)
        else:
            raise ValueError(code)
        if locator in locators:
            raise ValueError(f"{code}_duplicate")
        locators.add(locator)
    return locators


def _inventory_binding(payload: Mapping[str, Any]) -> dict[str, Any]:
    if payload.get("schema_version") != INVENTORY_SCHEMA_VERSION:
        raise ValueError("inventory_schema_version_invalid")

    has_faces = isinstance(payload.get("base"), Mapping) or isinstance(
        payload.get("candidate"), Mapping
    )
    if has_faces:
        if not isinstance(payload.get("base"), Mapping) or not isinstance(
            payload.get("candidate"), Mapping
        ):
            raise ValueError("inventory_face_pair_incomplete")
        base, base_locators = _inventory_section(payload, section_name="base")
        candidate, candidate_locators = _inventory_section(
            payload, section_name="candidate"
        )
        delta = payload.get("delta")
        if not isinstance(delta, Mapping):
            if set(base_locators) != set(candidate_locators):
                raise ValueError("inventory_delta_missing")
            delta = {}
        authoritative = (
            delta.get("authoritative_literal_emissions")
            if isinstance(delta, Mapping)
            else None
        )
        required_delta_fields = {
            "base_count",
            "candidate_count",
            "added_count",
            "removed_count",
            "added",
            "removed",
        }
        if not isinstance(authoritative, Mapping) or not required_delta_fields <= set(
            authoritative
        ):
            raise ValueError("inventory_authoritative_delta_missing")
        added = set(candidate_locators) - set(base_locators)
        removed = set(base_locators) - set(candidate_locators)
        if authoritative.get("base_count") != len(base_locators):
            raise ValueError("inventory_delta_base_count_mismatch")
        if authoritative.get("candidate_count") != len(candidate_locators):
            raise ValueError("inventory_delta_candidate_count_mismatch")
        if authoritative.get("added_count") != len(added):
            raise ValueError("inventory_delta_added_count_mismatch")
        if authoritative.get("removed_count") != len(removed):
            raise ValueError("inventory_delta_removed_count_mismatch")
        listed_added = _delta_locators(
            authoritative.get("added"), code="inventory_delta_added_invalid"
        )
        listed_removed = _delta_locators(
            authoritative.get("removed"), code="inventory_delta_removed_invalid"
        )
        if listed_added != added:
            raise ValueError("inventory_delta_added_locator_mismatch")
        if listed_removed != removed:
            raise ValueError("inventory_delta_removed_locator_mismatch")
    else:
        section, candidate_locators = _inventory_section(
            payload, section_name="inventory"
        )
        base = candidate = section
        base_locators = list(candidate_locators)

    digest_body = {
        "base_commit": base["commit"],
        "base_tree": base["tree"],
        "base_locators": sorted(base_locators),
        "candidate_commit": candidate["commit"],
        "candidate_tree": candidate["tree"],
        "candidate_locators": sorted(candidate_locators),
    }
    return {
        "base_commit": base["commit"],
        "base_tree": base["tree"],
        "base_site_count": len(base_locators),
        "base_locators": tuple(base_locators),
        "candidate_commit": candidate["commit"],
        "candidate_tree": candidate["tree"],
        "candidate_site_count": len(candidate_locators),
        "candidate_locators": tuple(candidate_locators),
        "site_count": len(candidate_locators),
        "inventory_sha256": _canonical_sha256(digest_body),
    }


def _validate_registry_binding(
    registry: Mapping[str, Any],
    inventory: Mapping[str, Any],
    errors: list[dict[str, Any]],
) -> None:
    binding = registry.get("inventory_binding")
    if not isinstance(binding, Mapping):
        errors.append(_error("registry_inventory_binding_missing"))
        return
    if set(binding) != _BINDING_FIELDS:
        errors.append(_error("registry_inventory_binding_shape_invalid"))
        return

    for field in (
        "base_commit",
        "base_tree",
        "candidate_commit",
        "candidate_tree",
    ):
        try:
            observed = _validated_git_id(
                binding.get(field), code=f"registry_{field}_invalid"
            )
        except ValueError as exc:
            errors.append(_error(str(exc)))
            continue
        if observed != inventory[field]:
            errors.append(
                _error(
                    f"registry_{field}_mismatch",
                    expected=inventory[field],
                    observed=observed,
                )
            )
    for field in ("base_site_count", "candidate_site_count"):
        if binding.get(field) != inventory[field]:
            errors.append(
                _error(
                    f"registry_{field}_mismatch",
                    expected=inventory[field],
                    observed=binding.get(field),
                )
            )
    observed_digest = str(binding.get("inventory_sha256") or "").strip().lower()
    if _HEX64_RE.fullmatch(observed_digest) is None:
        errors.append(_error("registry_inventory_sha256_invalid"))
    elif observed_digest != inventory["inventory_sha256"]:
        errors.append(
            _error(
                "registry_inventory_sha256_mismatch",
                expected=inventory["inventory_sha256"],
                observed=observed_digest,
            )
        )


def _validate_expected_face(
    inventory: Mapping[str, Any],
    expected_face: Mapping[str, Any] | None,
    expected_site_count: int | None,
    errors: list[dict[str, Any]],
) -> dict[str, str]:
    """Require the caller to pin the release face and candidate row count."""

    if expected_face is None or set(expected_face) != _EXPECTED_FACE_FIELDS:
        errors.append(_error("expected_inventory_face_missing"))
        normalized: dict[str, str] = {}
    else:
        normalized = {}
        for field in sorted(_EXPECTED_FACE_FIELDS):
            try:
                normalized[field] = _validated_git_id(
                    expected_face.get(field), code=f"expected_{field}_invalid"
                )
            except ValueError as exc:
                errors.append(_error(str(exc)))
        for field, expected in normalized.items():
            if inventory.get(field) != expected:
                errors.append(
                    _error(
                        f"inventory_expected_{field}_mismatch",
                        expected=expected,
                        observed=inventory.get(field, ""),
                    )
                )
    if type(expected_site_count) is not int or expected_site_count <= 0:
        errors.append(_error("expected_inventory_site_count_missing"))
    elif inventory["candidate_site_count"] != expected_site_count:
        errors.append(
            _error(
                "inventory_expected_site_count_mismatch",
                expected=expected_site_count,
                observed=inventory["candidate_site_count"],
            )
        )
    return normalized


def _validate_owner_approval(
    registry: Mapping[str, Any], errors: list[dict[str, Any]]
) -> dict[str, Any]:
    owner = registry.get("owner_approval")
    if not isinstance(owner, Mapping) or not owner:
        errors.append(_error("owner_approval_missing"))
        return {}
    if set(owner) != _OWNER_FIELDS:
        errors.append(_error("owner_approval_shape_invalid"))
        return dict(owner)
    if owner.get("status") != "approved":
        errors.append(_error("owner_approval_not_approved"))
    if owner.get("phrase") != REQUIRED_OWNER_PHRASE:
        errors.append(_error("owner_approval_phrase_invalid"))
    try:
        _required_text(owner.get("approval_ref"), code="owner_approval_ref_missing")
    except ValueError as exc:
        errors.append(_error(str(exc)))
    return dict(owner)


def _validate_rows(
    registry: Mapping[str, Any],
    inventory: Mapping[str, Any],
    errors: list[dict[str, Any]],
) -> dict[str, list[str]]:
    raw_rows = registry.get("rows")
    if not isinstance(raw_rows, list):
        errors.append(_error("registry_rows_missing"))
        return {}

    row_gates: dict[str, list[str]] = {}
    row_locators: set[str] = set()
    valid_locators = set(inventory["candidate_locators"])
    for ordinal, raw_row in enumerate(raw_rows):
        if not isinstance(raw_row, Mapping) or set(raw_row) != _ROW_FIELDS:
            errors.append(_error("registry_row_shape_invalid", ordinal=ordinal))
            continue
        locator = str(raw_row.get("locator") or "").strip()
        if not locator:
            errors.append(_error("registry_row_locator_missing", ordinal=ordinal))
            continue
        if locator in row_locators:
            errors.append(_error("registry_row_locator_duplicate", locator=locator))
        row_locators.add(locator)
        if locator not in valid_locators:
            errors.append(_error("registry_row_locator_unknown", locator=locator))

        decision = str(raw_row.get("final_decision") or "").strip().lower()
        if decision not in _FINAL_DECISIONS:
            errors.append(
                _error("registry_row_final_decision_invalid", locator=locator)
            )
        for field, code in (
            ("decision_ref", "registry_row_decision_ref_missing"),
            ("trigger_condition", "registry_row_trigger_condition_missing"),
            ("live_evidence_ref", "registry_row_live_evidence_ref_missing"),
        ):
            try:
                _required_text(raw_row.get(field), code=code)
            except ValueError as exc:
                errors.append(_error(str(exc), locator=locator))
        if raw_row.get("count_scope") != "code_aggregate":
            errors.append(_error("registry_row_count_scope_invalid", locator=locator))
        live_count = raw_row.get("live_count")
        if type(live_count) is not int or live_count < 0:
            errors.append(_error("registry_row_live_count_invalid", locator=locator))
        live_sha = str(raw_row.get("live_evidence_sha256") or "").strip().lower()
        if _HEX64_RE.fullmatch(live_sha) is None:
            errors.append(
                _error("registry_row_live_evidence_sha256_invalid", locator=locator)
            )

        raw_gate_ids = raw_row.get("hard_gate_ids")
        if not isinstance(raw_gate_ids, list) or any(
            not isinstance(gate_id, str) or not gate_id.strip()
            for gate_id in raw_gate_ids
        ):
            errors.append(_error("registry_row_gate_ids_invalid", locator=locator))
            normalized_gate_ids: list[str] = []
        else:
            normalized_gate_ids = [str(gate_id).strip() for gate_id in raw_gate_ids]
            if len(normalized_gate_ids) != len(set(normalized_gate_ids)):
                errors.append(
                    _error("registry_row_gate_ids_duplicate", locator=locator)
                )
        if decision == "hard_gate" and not normalized_gate_ids:
            errors.append(
                _error("registry_hard_gate_row_without_gate", locator=locator)
            )
        if decision == "observation" and normalized_gate_ids:
            errors.append(_error("registry_observation_row_has_gate", locator=locator))
        row_gates[locator] = normalized_gate_ids

    missing = sorted(valid_locators - row_locators)
    extra = sorted(row_locators - valid_locators)
    if missing:
        errors.append(
            _error("registry_rows_missing", count=len(missing), locators=missing)
        )
    if extra:
        errors.append(_error("registry_rows_extra", count=len(extra), locators=extra))
    if len(raw_rows) != inventory["candidate_site_count"]:
        errors.append(
            _error(
                "registry_row_count_invalid",
                expected=inventory["candidate_site_count"],
                observed=len(raw_rows),
            )
        )
    return row_gates


def _validate_hard_gates(
    registry: Mapping[str, Any],
    inventory: Mapping[str, Any],
    row_gates: Mapping[str, Sequence[str]],
    errors: list[dict[str, Any]],
) -> set[str]:
    raw_gates = registry.get("hard_gates")
    if not isinstance(raw_gates, list):
        errors.append(_error("registry_hard_gates_missing"))
        return set()
    if len(raw_gates) < MIN_HARD_GATE_COUNT:
        errors.append(_error("registry_hard_gate_count_empty"))
    if len(raw_gates) > MAX_HARD_GATE_COUNT:
        errors.append(
            _error(
                "registry_hard_gate_count_exceeded",
                maximum=MAX_HARD_GATE_COUNT,
                observed=len(raw_gates),
            )
        )

    known_locators = set(inventory["candidate_locators"])
    gate_ids: set[str] = set()
    gate_sources: dict[str, set[str]] = {}
    for ordinal, raw_gate in enumerate(raw_gates):
        if not isinstance(raw_gate, Mapping) or set(raw_gate) != _GATE_FIELDS:
            errors.append(_error("registry_hard_gate_shape_invalid", ordinal=ordinal))
            continue
        gate_id = str(raw_gate.get("gate_id") or "").strip()
        if _GATE_ID_RE.fullmatch(gate_id) is None:
            errors.append(_error("registry_hard_gate_id_invalid", ordinal=ordinal))
            continue
        if gate_id in gate_ids:
            errors.append(_error("registry_hard_gate_id_duplicate", gate_id=gate_id))
        gate_ids.add(gate_id)
        category = str(raw_gate.get("category") or "").strip().lower()
        if category not in _GATE_CATEGORIES:
            errors.append(
                _error("registry_hard_gate_category_invalid", gate_id=gate_id)
            )
        for field, code in (
            ("enforcement_ref", "registry_hard_gate_enforcement_ref_missing"),
            ("negative_test_ref", "registry_hard_gate_negative_test_ref_missing"),
        ):
            try:
                _required_text(raw_gate.get(field), code=code)
            except ValueError as exc:
                errors.append(_error(str(exc), gate_id=gate_id))
        raw_sources = raw_gate.get("source_locators")
        if not isinstance(raw_sources, list) or not raw_sources:
            errors.append(_error("registry_hard_gate_sources_invalid", gate_id=gate_id))
            gate_sources[gate_id] = set()
            continue
        sources = [str(locator).strip() for locator in raw_sources]
        if len(sources) != len(set(sources)):
            errors.append(
                _error("registry_hard_gate_sources_duplicate", gate_id=gate_id)
            )
        unknown = sorted(set(sources) - known_locators)
        if unknown:
            errors.append(
                _error(
                    "registry_hard_gate_source_locator_unknown",
                    gate_id=gate_id,
                    locators=unknown,
                )
            )
        gate_sources[gate_id] = set(sources)

    for locator, row_gate_ids in row_gates.items():
        for gate_id in row_gate_ids:
            if gate_id not in gate_ids:
                errors.append(
                    _error(
                        "registry_row_gate_ref_unknown",
                        locator=locator,
                        gate_id=gate_id,
                    )
                )
            elif locator not in gate_sources.get(gate_id, set()):
                errors.append(
                    _error(
                        "registry_gate_row_binding_mismatch",
                        locator=locator,
                        gate_id=gate_id,
                    )
                )
    for gate_id, sources in gate_sources.items():
        for locator in sources:
            if gate_id not in row_gates.get(locator, []):
                errors.append(
                    _error(
                        "registry_gate_row_binding_mismatch",
                        locator=locator,
                        gate_id=gate_id,
                    )
                )
    return gate_ids


def audit_registry(
    inventory_payload: Mapping[str, Any],
    registry_payload: Mapping[str, Any],
    *,
    expected_face: Mapping[str, Any] | None = None,
    expected_site_count: int | None = None,
) -> dict[str, Any]:
    errors: list[dict[str, Any]] = []
    inventory: dict[str, Any] | None = None
    try:
        inventory = _inventory_binding(inventory_payload)
    except ValueError as exc:
        errors.append(_error(str(exc)))

    registry_shape_valid = set(registry_payload) == _REGISTRY_FIELDS
    if registry_payload.get("schema_version") != REGISTRY_SCHEMA_VERSION:
        errors.append(_error("registry_schema_version_invalid"))
    if not registry_shape_valid:
        errors.append(_error("registry_shape_invalid"))

    owner = _validate_owner_approval(registry_payload, errors)
    row_gates: dict[str, list[str]] = {}
    gate_ids: set[str] = set()
    normalized_expected_face: dict[str, str] = {}
    if inventory is not None:
        normalized_expected_face = _validate_expected_face(
            inventory, expected_face, expected_site_count, errors
        )
        _validate_registry_binding(registry_payload, inventory, errors)
        row_gates = _validate_rows(registry_payload, inventory, errors)
        gate_ids = _validate_hard_gates(registry_payload, inventory, row_gates, errors)

    normalized_errors = sorted({
        json.dumps(item, ensure_ascii=False, sort_keys=True) for item in errors
    })
    error_items = [json.loads(item) for item in normalized_errors]
    contract_ready = not error_items
    return {
        "schema_version": SCHEMA_VERSION,
        "read_only": True,
        "ok": contract_ready,
        "registry_contract_ready": contract_ready,
        "execution_refs_verified": False,
        "live_acceptance_verified": False,
        "inventory": (
            {
                "base_commit": inventory["base_commit"],
                "base_tree": inventory["base_tree"],
                "base_site_count": inventory["base_site_count"],
                "candidate_commit": inventory["candidate_commit"],
                "candidate_tree": inventory["candidate_tree"],
                "candidate_site_count": inventory["candidate_site_count"],
                "inventory_sha256": inventory["inventory_sha256"],
            }
            if inventory is not None
            else None
        ),
        "expected_face": normalized_expected_face or None,
        "registry": {
            "row_count": (
                len(registry_payload.get("rows"))
                if isinstance(registry_payload.get("rows"), list)
                else 0
            ),
            "hard_gate_count": (
                len(registry_payload.get("hard_gates"))
                if isinstance(registry_payload.get("hard_gates"), list)
                else 0
            ),
            "hard_gate_ids": sorted(gate_ids),
            "owner_approval_ref": owner.get("approval_ref", ""),
        },
        "errors": error_items,
        "non_ga_limitations": [
            "execution_refs_not_verified_against filesystem or live runtime",
            "three publication counterexamples and canonical HTTPS are not verified",
        ],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--expected-base-commit")
    parser.add_argument("--expected-base-tree")
    parser.add_argument("--expected-candidate-commit")
    parser.add_argument("--expected-candidate-tree")
    parser.add_argument("--expected-row-count", type=int)
    args = parser.parse_args(argv)

    errors: list[dict[str, Any]] = []
    try:
        inventory = _load_json(args.inventory, label="inventory")
    except ValueError as exc:
        errors.append(_error(str(exc)))
        inventory = {}
    try:
        registry = _load_json(args.registry, label="registry")
    except ValueError as exc:
        errors.append(_error(str(exc)))
        registry = {}

    expected_face = {
        "base_commit": args.expected_base_commit or "",
        "base_tree": args.expected_base_tree or "",
        "candidate_commit": args.expected_candidate_commit or "",
        "candidate_tree": args.expected_candidate_tree or "",
    }
    report = audit_registry(
        inventory,
        registry,
        expected_face=expected_face,
        expected_site_count=args.expected_row_count,
    )
    if errors:
        report["ok"] = False
        report["registry_contract_ready"] = False
        report["errors"] = sorted(
            [*report["errors"], *errors],
            key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True),
        )
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report["registry_contract_ready"] else 2


if __name__ == "__main__":
    sys.exit(main())
