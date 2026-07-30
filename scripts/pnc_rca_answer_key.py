#!/usr/bin/env python3
"""Read and compare the fixed G1Q3 RCA answer-key set without any writes."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from gateway.pnc_pdcl_contract import is_valid_pdcl_download_cmd
from scripts.pnc_rca_prerelease_learning import MeegleReadClient


SCHEMA_VERSION = "pnc_rca_answer_key_v1"
FIELD_PDCL = "field_93aa63"
FIELD_HUMAN_ROOT_CAUSE = "field_842fc8"
FORBIDDEN_TRUTH_FIELD = "field_9193cb"
MAX_TEXT_CHARS = 12000

# These actions are part of the acceptance contract.  A future caller cannot
# turn a negative/out-of-domain case into a report by changing an input file.
FIXED_CASE_POLICIES: dict[str, dict[str, str]] = {
    "7055409207": {"expected_action": "report", "coverage": "positive"},
    "7059443058": {"expected_action": "report", "coverage": "positive"},
    "7056911199": {"expected_action": "report", "coverage": "positive"},
    "7056860756": {"expected_action": "report", "coverage": "positive"},
    "7059547125": {"expected_action": "report", "coverage": "positive"},
    "7054625864": {"expected_action": "report", "coverage": "positive"},
    "7056845775": {"expected_action": "no_defect_report", "coverage": "negative"},
    "7057052984": {"expected_action": "no_defect_report", "coverage": "negative"},
    "7056505046": {"expected_action": "report", "coverage": "positive"},
    "7055295349": {"expected_action": "abstain", "coverage": "out_of_domain"},
}


class AnswerKeyError(ValueError):
    pass


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise AnswerKeyError(f"{field}_must_be_text")
    value = value.strip()
    if len(value) > MAX_TEXT_CHARS:
        raise AnswerKeyError(f"{field}_too_large")
    return value


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _normalized(value: str) -> str:
    return "".join(value.casefold().split())


def _new_output_path(path: Path) -> Path:
    resolved = path.expanduser().absolute()
    if not resolved.is_absolute() or resolved.exists():
        raise AnswerKeyError("output_must_be_absolute_and_new")
    return resolved


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def fetch_fixed_answer_key(client: MeegleReadClient) -> dict[str, Any]:
    """Fetch only the two corrected truth fields through the read-only client."""
    cases: list[dict[str, Any]] = []
    for work_item_id, policy in FIXED_CASE_POLICIES.items():
        detail = client.detail(work_item_id)
        pdcl = _text(str(detail.get("pdcl_data") or ""), "pdcl_data")
        human = _text(str(detail.get("root_cause_text") or ""), "root_cause_text")
        cases.append({
            "work_item_id": work_item_id,
            "coverage": policy["coverage"],
            "expected_action": policy["expected_action"],
            "source_fields": {
                "pdcl": FIELD_PDCL,
                "human_root_cause": FIELD_HUMAN_ROOT_CAUSE,
            },
            "pdcl_command": pdcl,
            "pdcl_command_sha256": _digest(pdcl),
            "pdcl_command_valid": is_valid_pdcl_download_cmd(pdcl),
            "human_root_cause": human,
            "human_root_cause_sha256": _digest(human),
            "human_root_cause_present": bool(human),
        })
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "field_validation_manifest",
        "observed_at": _utc_now(),
        "read_only": True,
        "external_writes": 0,
        "truth_field": FIELD_HUMAN_ROOT_CAUSE,
        "forbidden_truth_field": FORBIDDEN_TRUTH_FIELD,
        "case_count": len(cases),
        "cases": cases,
    }


def _validate_manifest(value: Mapping[str, Any]) -> list[dict[str, Any]]:
    if value.get("schema_version") != SCHEMA_VERSION:
        raise AnswerKeyError("answer_key_schema_invalid")
    if value.get("truth_field") != FIELD_HUMAN_ROOT_CAUSE:
        raise AnswerKeyError("answer_key_truth_field_invalid")
    if value.get("forbidden_truth_field") != FORBIDDEN_TRUTH_FIELD:
        raise AnswerKeyError("answer_key_forbidden_field_missing")
    cases = value.get("cases")
    if not isinstance(cases, list) or len(cases) != len(FIXED_CASE_POLICIES):
        raise AnswerKeyError("answer_key_case_set_invalid")
    by_id: dict[str, dict[str, Any]] = {}
    for item in cases:
        if not isinstance(item, Mapping):
            raise AnswerKeyError("answer_key_case_invalid")
        work_item_id = _text(item.get("work_item_id"), "work_item_id")
        if work_item_id in by_id or work_item_id not in FIXED_CASE_POLICIES:
            raise AnswerKeyError("answer_key_case_identity_invalid")
        pdcl = _text(item.get("pdcl_command"), "pdcl_command")
        human = _text(item.get("human_root_cause"), "human_root_cause")
        if item.get("pdcl_command_valid") is not True or not is_valid_pdcl_download_cmd(pdcl):
            raise AnswerKeyError("answer_key_pdcl_invalid")
        if not human:
            raise AnswerKeyError("answer_key_human_root_cause_missing")
        policy = FIXED_CASE_POLICIES[work_item_id]
        if item.get("expected_action") != policy["expected_action"]:
            raise AnswerKeyError("answer_key_expected_action_tampered")
        by_id[work_item_id] = dict(item)
    if set(by_id) != set(FIXED_CASE_POLICIES):
        raise AnswerKeyError("answer_key_case_set_incomplete")
    return [by_id[work_item_id] for work_item_id in FIXED_CASE_POLICIES]


def _structured_conclusion(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise AnswerKeyError("own_conclusion_must_be_object")
    module = _text(value.get("module"), "own_conclusion_module")
    phenomenon = _text(value.get("phenomenon"), "own_conclusion_phenomenon")
    anchors = value.get("evidence_anchors")
    if not isinstance(anchors, list) or not anchors:
        raise AnswerKeyError("own_conclusion_evidence_anchors_required")
    normalized_anchors = [_text(item, "own_conclusion_evidence_anchor") for item in anchors]
    if not module or not phenomenon or any(not item for item in normalized_anchors):
        raise AnswerKeyError("own_conclusion_fields_required")
    return {
        "module": module,
        "phenomenon": phenomenon,
        "evidence_anchors": normalized_anchors,
    }


def compare_answer_key(
    manifest: Mapping[str, Any], own_results: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    truth_cases = {item["work_item_id"]: item for item in _validate_manifest(manifest)}
    if not isinstance(own_results, Sequence) or isinstance(own_results, (str, bytes)):
        raise AnswerKeyError("own_results_must_be_array")
    own_by_id: dict[str, Mapping[str, Any]] = {}
    for item in own_results:
        if not isinstance(item, Mapping):
            raise AnswerKeyError("own_result_invalid")
        work_item_id = _text(item.get("work_item_id"), "own_work_item_id")
        if work_item_id in own_by_id or work_item_id not in truth_cases:
            raise AnswerKeyError("own_result_identity_invalid")
        own_by_id[work_item_id] = item
    if set(own_by_id) != set(truth_cases):
        raise AnswerKeyError("own_result_set_incomplete")

    rows: list[dict[str, Any]] = []
    for work_item_id in FIXED_CASE_POLICIES:
        truth = truth_cases[work_item_id]
        own = own_by_id[work_item_id]
        expected_action = FIXED_CASE_POLICIES[work_item_id]["expected_action"]
        actual_action = _text(own.get("action"), "own_action")
        row: dict[str, Any] = {
            "work_item_id": work_item_id,
            "coverage": truth["coverage"],
            "expected_action": expected_action,
            "actual_action": actual_action,
        }
        if expected_action == "abstain":
            row["result"] = "我们弃权" if actual_action == "abstain" else "不一致"
            row["structural_checks"] = []
        elif expected_action == "no_defect_report":
            row["result"] = "一致" if actual_action == "no_defect_report" else "不一致"
            row["structural_checks"] = []
        elif actual_action != "report":
            row["result"] = "不一致"
            row["structural_checks"] = []
        else:
            conclusion = _structured_conclusion(own.get("own_conclusion"))
            human = _normalized(truth["human_root_cause"])
            checks = {
                "module": _normalized(conclusion["module"]) in human,
                "phenomenon": _normalized(conclusion["phenomenon"]) in human,
                "evidence_anchor": any(_normalized(anchor) in human for anchor in conclusion["evidence_anchors"]),
            }
            row["structural_checks"] = checks
            row["result"] = "一致" if all(checks.values()) else "不一致"
        rows.append(row)
    counts = {label: sum(row["result"] == label for row in rows) for label in ("一致", "不一致", "我们弃权")}
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "structural_answer_comparison",
        "truth_field": FIELD_HUMAN_ROOT_CAUSE,
        "forbidden_truth_field": FORBIDDEN_TRUTH_FIELD,
        "comparison_method": "exact normalized module/phenomenon containment plus one exact evidence-anchor containment; no LLM scoring",
        "counts": counts,
        "rows": rows,
    }


def _load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise AnswerKeyError("json_object_required")
    return value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    fetch = subparsers.add_parser("fetch")
    fetch.add_argument("--output", required=True, type=Path)
    compare = subparsers.add_parser("compare")
    compare.add_argument("--manifest", required=True, type=Path)
    compare.add_argument("--own-results", required=True, type=Path)
    compare.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        output = _new_output_path(args.output)
        if args.command == "fetch":
            result = fetch_fixed_answer_key(MeegleReadClient())
        else:
            own = _load_json(args.own_results).get("results")
            result = compare_answer_key(_load_json(args.manifest), own)
        _write_json(output, result)
    except (AnswerKeyError, OSError, json.JSONDecodeError) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2
    print(json.dumps({"ok": True, "output": str(output), "kind": result["kind"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
