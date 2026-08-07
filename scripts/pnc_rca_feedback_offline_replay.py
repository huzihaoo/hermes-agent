#!/usr/bin/env python3
"""Build a zero-external-side-effect replay gate for feedback RCA cases.

This utility deliberately consumes only a sealed readback JSON and optional
previously generated census metadata.  It never fetches report URLs, opens a
Feishu client, connects to the VM, or touches the production control DB.  The
result is an evidence index: it does not claim that a historical report can be
replayed when the focus payload or source artifact is unavailable.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import subprocess
import sys
from typing import Any, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gateway.pnc_rca_issue_focus import (
    IssueFocusContractError,
    resolve_issue_intent,
    validate_issue_focus_evidence,
)


SCHEMA_VERSION = "g1q3_feedback_issue_offline_replay_v1"
READBACK_SCHEMA_VERSIONS = frozenset({"g1q3_feedback_issue_readonly_readback_v1"})
CENSUS_SCHEMA_VERSIONS = frozenset(
    {
        "g1q3_feedback_issue_intent_census_v1",
        "g1q3_feedback_issue_intent_census_v2",
    }
)
DEFAULT_READBACK = Path(
    "/Users/songying/.hermes/runtime/pnc_agent/feishu_issue_kafka_rca/"
    "release_evidence/feedback-issues-readonly-20260807/readback.json"
)

# These are writes, not permitted read-only observations.  The accepted
# readback/census schema versions declare their own required subsets below;
# an absent declared key is a contract error, while unrelated read-only counters
# remain outside this gate.
SIDE_EFFECT_KEYS = (
    "feishu_writes",
    "comment_writes",
    "field_writes",
    "workflow_writes",
    "workhour_writes",
    "control_db_writes",
    "production_db_writes",
    "network_writes",
    "candidate_code_writes",
)
READBACK_SIDE_EFFECT_KEYS = frozenset(
    {
        "feishu_writes",
        "comment_writes",
        "field_writes",
        "workflow_writes",
        "workhour_writes",
        "control_db_writes",
    }
)
CENSUS_SIDE_EFFECT_KEYS = frozenset(
    {
        "feishu_writes",
        "comment_writes",
        "field_writes",
        "workflow_writes",
        "workhour_writes",
        "production_db_writes",
        "network_writes",
        "candidate_code_writes",
    }
)
_TERMINAL_FAILURE_OUTCOMES = frozenset(
    {"terminal_failed", "failed", "quarantined", "terminal_failure"}
)


class OfflineReplayGateError(ValueError):
    """Raised when the sealed readback cannot be interpreted safely."""

    def __init__(self, code: str, detail: str = ""):
        self.code = str(code or "offline_replay_invalid")[:120]
        self.detail = str(detail or self.code)[:1000]
        super().__init__(self.detail)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise OfflineReplayGateError("offline_replay_input_unreadable", str(exc)) from exc
    if not isinstance(value, Mapping):
        raise OfflineReplayGateError("offline_replay_input_not_object")
    return value


def _case_id(item: Mapping[str, Any]) -> str:
    value = item.get("work_item_id", item.get("id"))
    result = str(value or "").strip()
    if not result:
        raise OfflineReplayGateError("offline_replay_case_id_missing")
    return result


def _normalise_count(value: Any, *, label: str) -> int:
    # bool is an int subclass but is not a valid side-effect count.
    if value is None or isinstance(value, bool):
        raise OfflineReplayGateError("offline_replay_side_effect_count_invalid", label)
    if not isinstance(value, (int, float, str)):
        raise OfflineReplayGateError("offline_replay_side_effect_count_invalid", label)
    if isinstance(value, float) and (
        not math.isfinite(value) or not value.is_integer()
    ):
        raise OfflineReplayGateError("offline_replay_side_effect_count_invalid", label)
    try:
        count = int(value)
    except (TypeError, ValueError) as exc:
        raise OfflineReplayGateError(
            "offline_replay_side_effect_count_invalid", label
        ) from exc
    if count < 0:
        raise OfflineReplayGateError("offline_replay_side_effect_count_invalid", label)
    return count


def _side_effect_gate(*sources: Mapping[str, Any]) -> dict[str, Any]:
    counts = {key: 0 for key in SIDE_EFFECT_KEYS}
    source_labels: list[str] = []
    for index, source in enumerate(sources):
        if not isinstance(source, Mapping):
            continue
        source_labels.append(str(source.get("source") or f"source_{index}"))
        for key in SIDE_EFFECT_KEYS:
            counts[key] = max(
                counts[key],
                _normalise_count(source.get(key, 0), label=f"{key}@{index}"),
            )
    violations = {key: value for key, value in counts.items() if value}
    return {
        "passed": not violations,
        "counts": counts,
        "violations": violations,
        "sources": source_labels,
        "network_reads_allowed": True,
    }


def validate_result_field_two_lines(value: Any) -> dict[str, Any]:
    """Validate the public result field without interpreting its conclusion."""

    text = str(value or "")
    lines = text.splitlines()
    if len(lines) != 2:
        return {
            "valid": False,
            "code": "result_field_line_count_invalid",
            "line_count": len(lines),
        }
    labels = ("归因结论：", "责任模块：")
    for index, label in enumerate(labels):
        if not lines[index].startswith(label):
            return {
                "valid": False,
                "code": "result_field_label_invalid",
                "line_count": len(lines),
                "failed_line": index + 1,
            }
        if not lines[index][len(label) :].strip():
            return {
                "valid": False,
                "code": "result_field_value_empty",
                "line_count": len(lines),
                "failed_line": index + 1,
            }
    return {"valid": True, "code": "", "line_count": 2}


def _terminal_projection(item: Mapping[str, Any]) -> dict[str, Any]:
    control = item.get("control_readonly")
    if not isinstance(control, Mapping):
        control = {}
    watch = control.get("execution_watch")
    if not isinstance(watch, Mapping):
        watch = {}
    job = control.get("delivery_job")
    if not isinstance(job, Mapping):
        job = {}
    comments = item.get("comments")
    if not isinstance(comments, Mapping):
        comments = {}
    comment_items = comments.get("items")
    if not isinstance(comment_items, list):
        comment_items = []
    markers = tuple(
        str(row.get("marker") or "")
        for row in comment_items
        if isinstance(row, Mapping)
    )
    watch_error = str(watch.get("last_error_code") or "").strip()
    job_outcome = str(job.get("outcome") or "").strip().lower()
    job_state = str(job.get("terminal_state") or "").strip().lower()
    terminal_marker = any(
        marker.startswith("RCA_TERMINAL:") and "terminal_failed" in marker
        for marker in markers
    )
    failed = bool(
        watch_error
        or job_outcome in _TERMINAL_FAILURE_OUTCOMES
        or job_state in _TERMINAL_FAILURE_OUTCOMES
        or terminal_marker
    )
    generations = []
    for raw in (watch.get("generation"), job.get("generation")):
        if isinstance(raw, int) and not isinstance(raw, bool):
            generations.append(raw)
    generation = max(generations) if generations else None
    return {
        "generation": generation,
        "watch_state": str(watch.get("state") or ""),
        "watch_error_code": watch_error,
        "job_outcome": job_outcome,
        "job_terminal_state": job_state,
        "terminal_marker": terminal_marker,
        "terminal_failure": failed,
        "rerun_required": failed,
    }


def _report_data_projection(
    case_id: str,
    issue_title: str,
    census_cases: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    historical = census_cases.get(case_id)
    if not isinstance(historical, Mapping):
        return {
            "source": "unavailable",
            "availability": "unknown",
            "sha256": None,
            "size": None,
            "issue_focus_present": False,
            "focus_contract_valid": False,
            "focus_contract_analysis_status": None,
            "focus_contract_attribution_allowed": False,
            "focus_contract": {"status": "unavailable", "error": "census_case_missing"},
            "replay_input_complete": False,
            "historical_intent_status": "",
            "historical_expected_replay_status": "",
        }
    report_data = historical.get("report_data")
    if not isinstance(report_data, Mapping):
        report_data = {}
    availability = str(report_data.get("availability") or "unknown")
    focus_present = report_data.get("issue_focus_present") is True
    focus_payload = report_data.get("issue_focus")
    focus_contract_valid = False
    focus_contract_analysis_status: str | None = None
    focus_contract_attribution_allowed = False
    if isinstance(focus_payload, Mapping):
        try:
            validation = validate_issue_focus_evidence(
                issue_title=issue_title,
                value=focus_payload,
            )
        except IssueFocusContractError as exc:
            focus_contract = {"status": "invalid", "error": exc.code}
        else:
            focus_contract_valid = True
            focus_present = True
            focus_contract_analysis_status = validation.analysis_status
            focus_contract_attribution_allowed = validation.attribution_allowed
            focus_contract = {
                "status": "validated",
                "validation": validation.to_dict(),
            }
    elif focus_present:
        focus_contract = {"status": "declared_without_payload", "error": "payload_missing"}
    else:
        focus_contract = {"status": "missing", "error": "issue_focus_missing"}
    hash_value = str(report_data.get("sha256") or "").strip() or None
    size = report_data.get("size")
    return {
        "source": "historical_census",
        "availability": availability,
        "sha256": hash_value,
        "size": size if isinstance(size, int) and size >= 0 else None,
        "issue_focus_present": focus_present,
        "focus_contract_valid": focus_contract_valid,
        "focus_contract_analysis_status": focus_contract_analysis_status,
        "focus_contract_attribution_allowed": focus_contract_attribution_allowed,
        "focus_contract": focus_contract,
        "replay_input_complete": bool(
            hash_value
            and availability in {"http_hash_verified", "local_hash_verified"}
            and focus_contract_valid
        ),
        "historical_report_title": report_data.get("report_title"),
        "historical_intent_status": str(historical.get("intent_status") or ""),
        "historical_expected_replay_status": str(
            historical.get("expected_replay_status") or ""
        ),
    }


def _census_cases(census: Mapping[str, Any] | None) -> dict[str, Mapping[str, Any]]:
    if not isinstance(census, Mapping):
        return {}
    raw_cases = census.get("cases")
    if not isinstance(raw_cases, list):
        return {}
    result: dict[str, Mapping[str, Any]] = {}
    for raw in raw_cases:
        if not isinstance(raw, Mapping):
            continue
        raw_id = raw.get("work_item_id", raw.get("id"))
        if raw_id is not None:
            result[str(raw_id).strip()] = raw
    return result


def _historical_status(
    intent: Any,
    *,
    terminal: Mapping[str, Any],
    report_data: Mapping[str, Any],
) -> tuple[str, str, bool, str]:
    if bool(terminal.get("rerun_required")):
        return "rerun_required", "generation2_required", False, "terminal_generation_failed"
    if not intent.statement_sufficient:
        return "insufficient_statement", "stop_without_attribution", False, "statement_insufficient"
    contract_status = str(report_data.get("focus_contract_analysis_status") or "")
    if contract_status == "capability_unsupported":
        return "capability_unsupported", "capability_gate", False, "focus_contract_capability_stop"
    if contract_status == "insufficient_statement":
        return "insufficient_statement", "stop_without_attribution", False, "focus_contract_statement_stop"
    if (
        "vehicle_signal_chain" in intent.required_capabilities
        and report_data.get("historical_intent_status")
        in {"capability_gate", "capability_unsupported"}
        and not bool(report_data.get("focus_contract_valid"))
    ):
        return (
            "capability_unsupported",
            "capability_gate",
            False,
            "historical_capability_audit_gate",
        )
    if bool(report_data.get("issue_focus_present")):
        if not bool(report_data.get("focus_contract_valid")):
            return (
                "focus_evidence_invalid",
                "focus_contract_invalid",
                False,
                str(
                    (report_data.get("focus_contract") or {}).get("error")
                    or "historical_issue_focus_payload_invalid"
                ),
            )
        field_ok = bool(
            report_data.get("replay_input_complete")
            and report_data.get("focus_contract_attribution_allowed")
        )
        return (
            "focus_evidence_present",
            "replay_ready" if field_ok else "replay_input_incomplete",
            field_ok,
            "",
        )
    return "focus_evidence_missing", "focus_evidence_required", False, "historical_issue_focus_missing"


def _candidate_metadata(repo_root: Path) -> dict[str, Any]:
    def _file_digest(relative: str) -> str | None:
        path = repo_root / relative
        try:
            return sha256_file(path)
        except OSError:
            return None

    commit = None
    try:
        completed = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
        commit = completed.stdout.strip() or None
    except (OSError, subprocess.SubprocessError):
        pass
    return {
        "worktree": str(repo_root),
        "commit": commit,
        "issue_focus_module_sha256": _file_digest("gateway/pnc_rca_issue_focus.py"),
        "offline_replay_script_sha256": _file_digest(
            "scripts/pnc_rca_feedback_offline_replay.py"
        ),
    }


def build_offline_replay_report(
    readback: Mapping[str, Any],
    *,
    readback_path: Path | None = None,
    census: Mapping[str, Any] | None = None,
    census_path: Path | None = None,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Build a deterministic, side-effect-free report from already-read data."""

    if not isinstance(readback, Mapping):
        raise OfflineReplayGateError("offline_replay_readback_not_object")
    if readback.get("schema_version") not in READBACK_SCHEMA_VERSIONS:
        raise OfflineReplayGateError("offline_replay_readback_schema_unsupported")
    if isinstance(census, Mapping) and census.get("schema_version") not in CENSUS_SCHEMA_VERSIONS:
        raise OfflineReplayGateError("offline_replay_census_schema_unsupported")
    items = readback.get("items")
    if not isinstance(items, list):
        raise OfflineReplayGateError("offline_replay_items_invalid")
    if readback.get("read_only") is not True:
        raise OfflineReplayGateError("offline_replay_read_only_binding_missing")

    source_side_effects = readback.get("side_effects")
    if not isinstance(source_side_effects, Mapping):
        raise OfflineReplayGateError("offline_replay_side_effect_contract_missing")
    missing_readback_counts = READBACK_SIDE_EFFECT_KEYS - set(source_side_effects)
    if missing_readback_counts:
        raise OfflineReplayGateError(
            "offline_replay_side_effect_contract_incomplete",
            ",".join(sorted(missing_readback_counts)),
        )
    census_side_effects = census.get("observed_side_effects") if isinstance(census, Mapping) else {}
    if not isinstance(census_side_effects, Mapping):
        raise OfflineReplayGateError("offline_replay_census_side_effect_contract_missing")
    if isinstance(census, Mapping):
        missing_census_counts = CENSUS_SIDE_EFFECT_KEYS - set(census_side_effects)
        if missing_census_counts:
            raise OfflineReplayGateError(
                "offline_replay_census_side_effect_contract_incomplete",
                ",".join(sorted(missing_census_counts)),
            )
    side_effect_gate = _side_effect_gate(
        {"source": "readback", **dict(source_side_effects)},
        {"source": "historical_census", **dict(census_side_effects)},
    )

    census_by_id = _census_cases(census)
    cases: list[dict[str, Any]] = []
    for raw_item in items:
        if not isinstance(raw_item, Mapping):
            raise OfflineReplayGateError("offline_replay_case_invalid")
        case_id = _case_id(raw_item)
        title = str(raw_item.get("name") or raw_item.get("title") or "").strip()
        if not title:
            raise OfflineReplayGateError("offline_replay_case_title_missing", case_id)
        intent = resolve_issue_intent(title)
        terminal = _terminal_projection(raw_item)
        report_data = _report_data_projection(case_id, title, census_by_id)
        focus_status, replay_status, attribution_allowed, stop_reason = _historical_status(
            intent,
            terminal=terminal,
            report_data=report_data,
        )
        field = validate_result_field_two_lines(
            (raw_item.get("fields") or {}).get("field_9193cb")
            if isinstance(raw_item.get("fields"), Mapping)
            else ""
        )
        delivery_attribution_allowed = bool(attribution_allowed and field["valid"])
        cases.append(
            {
                "work_item_id": case_id,
                "title": title,
                "intent": intent.to_dict(),
                "focus_status": focus_status,
                "replay_status": replay_status,
                "focus_attribution_allowed": attribution_allowed,
                "attribution_allowed": delivery_attribution_allowed,
                "stop_reason": stop_reason,
                "required_focus": {
                    "capabilities": list(intent.required_capabilities),
                    "segments": list(intent.required_segments),
                    "entities": list(intent.required_entities),
                    "measurements": list(intent.required_measurements),
                    "checks": list(intent.required_checks),
                    "calculations": list(intent.required_calculations),
                },
                "report_data": report_data,
                "result_field": field,
                "terminal": terminal,
                "historical_census_present": case_id in census_by_id,
            }
        )

    focus_missing = sum(case["focus_status"] == "focus_evidence_missing" for case in cases)
    focus_invalid = sum(case["focus_status"] == "focus_evidence_invalid" for case in cases)
    capability_gates = sum(case["focus_status"] == "capability_unsupported" for case in cases)
    insufficient = sum(case["focus_status"] == "insufficient_statement" for case in cases)
    reruns = sum(case["focus_status"] == "rerun_required" for case in cases)
    field_failures = sum(not case["result_field"]["valid"] for case in cases)
    report_ready = sum(case["report_data"]["replay_input_complete"] for case in cases)
    blockers: list[str] = []
    if not side_effect_gate["passed"]:
        blockers.append("external_side_effect_observed")
    if focus_missing:
        blockers.append("historical_issue_focus_missing")
    if focus_invalid:
        blockers.append("historical_issue_focus_invalid")
    if capability_gates:
        blockers.append("capability_gate_required")
    if insufficient:
        blockers.append("insufficient_statement_stop")
    if reruns:
        blockers.append("generation2_rerun_required")
    if field_failures:
        blockers.append("result_field_not_exactly_two_lines")
    if report_ready != len(cases):
        blockers.append("historical_report_input_incomplete")
    full_replay_ready = bool(
        cases
        and side_effect_gate["passed"]
        and not blockers
        and report_ready == len(cases)
        and all(case["attribution_allowed"] for case in cases)
    )
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at or _utc_now(),
        "source_class": "derived_offline_evidence",
        "readback": {
            "path": str(readback_path) if readback_path else None,
            "sha256": None,
            "item_count": len(items),
            "observed_at": readback.get("observed_at"),
            "read_only": True,
        },
        "historical_census": {
            "path": str(census_path) if census_path else None,
            "sha256": None,
            "present": bool(census),
            "schema_version": census.get("schema_version") if isinstance(census, Mapping) else None,
        },
        "candidate": _candidate_metadata(repo_root or REPO_ROOT),
        "side_effect_gate": side_effect_gate,
        "cases": cases,
        "coverage": {
            "items": len(cases),
            "statement_insufficient": insufficient,
            "capability_gate": capability_gates,
            "focus_evidence_missing": focus_missing,
            "focus_evidence_invalid": focus_invalid,
            "generation2_rerun_required": reruns,
            "result_field_two_line_failures": field_failures,
            "historical_report_inputs_ready": report_ready,
            "historical_issue_focus_present": sum(
                case["report_data"]["issue_focus_present"] for case in cases
            ),
            "full_replay_ready": full_replay_ready,
        },
        "replay_gate": {
            "status": "ready" if full_replay_ready else "blocked",
            "blockers": blockers,
            "external_side_effects": 0 if side_effect_gate["passed"] else sum(side_effect_gate["violations"].values()),
            "network_writes": side_effect_gate["counts"]["network_writes"],
            "feishu_writes": side_effect_gate["counts"]["feishu_writes"],
            "vm_writes": 0,
            "production_db_writes": side_effect_gate["counts"]["production_db_writes"],
        },
        "full_replay_ready": full_replay_ready,
    }
    return result


def write_offline_replay_report(
    report: Mapping[str, Any],
    output_path: Path,
) -> dict[str, Any]:
    """Write one local evidence artifact and return its immutable receipt."""

    output_path = output_path.expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return {
        "path": str(output_path),
        "sha256": sha256_file(output_path),
        "bytes": output_path.stat().st_size,
        "schema_version": report.get("schema_version"),
        "full_replay_ready": report.get("full_replay_ready"),
    }


def _find_census(path: Path) -> Path | None:
    candidates = sorted(path.glob("offline-preflight-*/intent-census.json"))
    return candidates[-1] if candidates else None


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--readback", type=Path, default=DEFAULT_READBACK)
    parser.add_argument("--census", type=Path, default=None)
    parser.add_argument(
        "--generated-at",
        default=None,
        help="fixed RFC3339 timestamp for byte-for-byte reproducible evidence",
    )
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    readback_path = args.readback.expanduser().resolve(strict=True)
    census_path = args.census.expanduser().resolve(strict=True) if args.census else _find_census(readback_path.parent)
    readback = _load_json(readback_path)
    census = _load_json(census_path) if census_path else None
    report = build_offline_replay_report(
        readback,
        readback_path=readback_path,
        census=census,
        census_path=census_path,
        generated_at=args.generated_at,
    )
    report["readback"]["sha256"] = sha256_file(readback_path)
    if census_path:
        report["historical_census"]["sha256"] = sha256_file(census_path)
    receipt = write_offline_replay_report(report, args.output)
    print(json.dumps(receipt, ensure_ascii=False, sort_keys=True))
    return 0 if report["side_effect_gate"]["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
