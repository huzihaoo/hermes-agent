"""Fail-closed structural quality tiers for production RCA publication.

The oracle deliberately ignores model confidence and capability inventories.
Only evaluator keys present in the producer's ``actual_evaluators`` emission,
structured evidence, and a closed causal narrative can raise an attribution
above honest non-attribution.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import math
from pathlib import Path
import re
from typing import Any, Mapping, Sequence


SUPPORTED_ATTRIBUTION = "supported_attribution"
CANDIDATE_HYPOTHESIS = "candidate_hypothesis"
HONEST_NON_ATTRIBUTION = "honest_non_attribution"
TECHNICAL_FAILURE = "technical_failure"
CONSUMER_DELIVERY_FAILURE = "consumer_delivery_failure"

TERMINAL_CLASSES = frozenset({
    SUPPORTED_ATTRIBUTION,
    CANDIDATE_HYPOTHESIS,
    HONEST_NON_ATTRIBUTION,
    TECHNICAL_FAILURE,
    CONSUMER_DELIVERY_FAILURE,
})

MEDIUM_TIER_DISCLAIMER = "候选结论，待人工确认，不可作为定责依据"
BANNED_PUBLIC_PHRASES = ("请核对问题数据地址",)
GOLDEN_REGISTRY_SCHEMA_VERSION = "pnc_rca_release_golden_registry_v1"
GOLDEN_REGISTRY_PATH = (
    Path(__file__).resolve().parent
    / "assets"
    / "pnc_rca_release_golden_registry_v1.json"
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_GIT_OID_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_EVALUATOR_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,159}$")
_GOLDEN_HASH_FIELDS = (
    "evaluator_source_sha256",
    "positive_golden_sha256",
    "negative_golden_sha256",
    "test_receipt_sha256",
)

_NON_ATTRIBUTION_MARKERS = (
    "自动RCA未归因",
    "自动归因未完成",
    "未归因",
    "不能自动归因",
    "无法自动归因",
    "不能确认归因",
    "无法确认归因",
    "不能确认责任归因",
    "未形成归因",
    "非归因结论",
    "not attributable",
    "no attribution",
)
_CANDIDATE_MARKERS = (
    "候选",
    "待人工",
    "需人工",
    "非高置信",
    "不可作为定责依据",
)
_LOW_TIER_USER_ACTION_RE = re.compile(
    r"请(?:核对|检查|补充|补齐|修正|提供|上传|重试|重新|确认|处理|联系)|"
    r"(?:用户|发起人).{0,16}(?:补充|补齐|核对|修正|提供|操作)|"
    r"重新发起"
)
_LOW_TIER_BLAME_RE = re.compile(
    r"问题单.{0,24}(?:缺少|错误|有误|不一致|未提供|不完整)|"
    r"(?:用户|发起人).{0,16}(?:数据|输入).{0,16}(?:错误|有误|缺少|无效|不完整)|"
    r"(?:数据|地址).{0,16}(?:错误|有误|无效|不对|无法解析)"
)
_APPROVAL_READY_RE = re.compile(
    r"\bquality[-_]approved\b|[\"']?approval_ready[\"']?"
    r"\s*(?:[:=]|为)\s*(?:true|1|是)",
    re.IGNORECASE,
)
_RESPONSIBILITY_PLACEHOLDERS = frozenset({
    "",
    "unknown",
    "未知",
    "暂无法判断",
    "暂无法判断。",
    "待人工确认",
    "待人工确认。",
    "unassigned",
})
_ROUTE_BOUNDARY_ERROR_CODES = frozenset({
    "business_profile_unresolved",
    "business_profile_unsupported",
    "business_profile_conflict",
    "business_profile_adapter_not_ready",
    "business_route_unresolved",
    "business_route_unsupported",
    "business_route_conflict",
    "business_adapter_not_ready",
})
_TECHNICAL_OUTCOMES = frozenset({
    "failed",
    "terminal_failed",
    "technical_failure",
    "analysis_failed",
})
_CONSUMER_FAILURE_STATES = frozenset({
    "consumer_delivery_failure",
    "delivery_failed",
    "readback_failed",
    "publication_failed",
})

_LEGACY_CLASS_MAP = {
    "evidence_attribution": SUPPORTED_ATTRIBUTION,
    "supported": SUPPORTED_ATTRIBUTION,
    "high": SUPPORTED_ATTRIBUTION,
    "evidence_attribution_needs_review": CANDIDATE_HYPOTHESIS,
    "candidate_attribution_needs_review": CANDIDATE_HYPOTHESIS,
    "candidate": CANDIDATE_HYPOTHESIS,
    "medium": CANDIDATE_HYPOTHESIS,
    "technical_failure": TECHNICAL_FAILURE,
    "consumer_delivery_failure": CONSUMER_DELIVERY_FAILURE,
}


@dataclass(frozen=True)
class StructuralTierFacts:
    supported_evaluator_keys: tuple[str, ...]
    supported_evaluator_count: int
    actual_evaluator_inventory_present: bool
    actual_evaluator_inventory_valid: bool
    golden_covered_evaluator_keys: tuple[str, ...]
    golden_registry_present: bool
    golden_registry_valid: bool
    low_tier_golden_ready: bool
    golden_coverage_complete: bool
    evidence_ref_count: int
    issue_frame_present: bool
    focus_window_present: bool
    field_lineage_complete: bool
    viz_lineage_complete: bool
    evidence_complete: bool
    causal_chain_roles: tuple[str, ...]
    causal_chain_closed: bool
    responsibility: str
    responsibility_present: bool
    conclusion_present: bool
    explicit_non_attribution: bool
    candidate_wording_present: bool


@dataclass(frozen=True)
class TierOracleResult:
    schema_version: str
    terminal_class: str
    confidence_tier: str
    publication_allowed: bool
    classification_conflict: bool
    violations: tuple[str, ...]
    facts: StructuralTierFacts

    def as_dict(self) -> dict[str, Any]:
        # Normalize tuples to JSON arrays so the persisted payload compares
        # byte-for-byte after a SQLite JSON round trip.
        return json.loads(json.dumps(asdict(self), ensure_ascii=False, allow_nan=False))

    def sha256(self) -> str:
        encoded = json.dumps(
            self.as_dict(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()


class TierOracleConflict(ValueError):
    """Raised when a claimed or publishable RCA result violates the oracle."""

    def __init__(self, result: TierOracleResult):
        self.result = result
        detail = (
            ",".join(result.violations) or f"{result.terminal_class}_not_publishable"
        )
        super().__init__(detail)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _string(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _append_values(target: list[Any], value: Any) -> None:
    if isinstance(value, (list, tuple)):
        target.extend(value)
    elif value not in (None, ""):
        target.append(value)


def _public_parts(contract: Mapping[str, Any]) -> tuple[str, ...]:
    public = _mapping(contract.get("public_result"))
    summary = _mapping(public.get("summary")) or _mapping(contract.get("summary"))
    report = _mapping(contract.get("report"))
    responsibility = _mapping(public.get("responsibility"))
    causal = _mapping(public.get("causal_chain"))
    evidence = _mapping(public.get("evidence_summary"))
    action = _mapping(public.get("user_action")) or _mapping(
        contract.get("user_action")
    )
    artifacts = _mapping(contract.get("artifacts"))
    values: list[Any] = [
        summary.get("short_conclusion"),
        summary.get("l0"),
        public.get("candidate"),
        report.get("candidate_owner"),
        report.get("candidate_owner_domain"),
        responsibility.get("candidate"),
        responsibility.get("owner"),
        responsibility.get("boundary"),
        artifacts.get("attribution_causal_text"),
        action.get("next_action"),
        action.get("next_action_text"),
    ]
    narrative = causal.get("narrative")
    for item in narrative if isinstance(narrative, (list, tuple)) else ():
        if isinstance(item, Mapping):
            values.append(item.get("text"))
    hypotheses = causal.get("hypotheses")
    for item in hypotheses if isinstance(hypotheses, (list, tuple)) else ():
        if not isinstance(item, Mapping):
            continue
        values.extend((
            item.get("claim"),
            item.get("narrative"),
            item.get("text"),
            item.get("summary"),
        ))
        supporting_evidence = item.get("supporting_evidence")
        for evidence_item in (
            supporting_evidence
            if isinstance(supporting_evidence, (list, tuple))
            else ()
        ):
            if isinstance(evidence_item, Mapping):
                values.extend((
                    evidence_item.get("name"),
                    evidence_item.get("evidence"),
                    evidence_item.get("summary"),
                ))
            else:
                values.append(evidence_item)
    _append_values(values, public.get("evidence_boundary"))
    _append_values(values, contract.get("evidence_boundary"))
    _append_values(values, evidence.get("missing_evidence"))
    return tuple(value for value in (_string(item) for item in values) if value)


def _conclusion(contract: Mapping[str, Any]) -> str:
    public = _mapping(contract.get("public_result"))
    summary = _mapping(public.get("summary")) or _mapping(contract.get("summary"))
    artifacts = _mapping(contract.get("artifacts"))
    return _string(
        summary.get("short_conclusion")
        or summary.get("l0")
        or artifacts.get("attribution_causal_text")
    )


def _responsibility(contract: Mapping[str, Any]) -> str:
    public = _mapping(contract.get("public_result"))
    responsibility = _mapping(public.get("responsibility"))
    report = _mapping(contract.get("report"))
    for value in (
        responsibility.get("candidate"),
        responsibility.get("owner"),
        public.get("candidate"),
        report.get("candidate_owner_domain"),
        report.get("candidate_owner"),
    ):
        candidate = _string(value)
        if candidate.lower().rstrip("。") not in _RESPONSIBILITY_PLACEHOLDERS:
            return candidate
    return ""


def _evidence_refs(contract: Mapping[str, Any]) -> tuple[str, ...]:
    public = _mapping(contract.get("public_result"))
    evidence = _mapping(public.get("evidence_summary"))
    refs: list[str] = []
    evidence_items = evidence.get("refs")
    for item in evidence_items if isinstance(evidence_items, list) else ():
        if isinstance(item, Mapping):
            value = _string(item.get("evidence_ref"))
        else:
            value = ""
        if value:
            refs.append(value)
    causal = _mapping(public.get("causal_chain"))
    hypotheses = causal.get("hypotheses")
    for hypothesis in hypotheses if isinstance(hypotheses, list) else ():
        if not isinstance(hypothesis, Mapping):
            continue
        supporting = hypothesis.get("supporting_evidence")
        for item in supporting if isinstance(supporting, list) else ():
            if isinstance(item, Mapping):
                value = _string(item.get("evidence_ref"))
            else:
                value = ""
            if value:
                refs.append(value)
    return tuple(sorted(set(refs)))


def _empty_golden_registry_status(*, present: bool, valid: bool = False) -> dict[str, Any]:
    return {
        "present": present,
        "valid": valid,
        "pipeline_commit": "",
        "pipeline_tree": "",
        "low_tier_golden_ready": False,
        "evaluators": {},
        "required_evaluator_ids": (),
        "required_evaluator_ids_present": False,
        "inventory_binding_valid": False,
        "missing_required_evaluator_ids": (),
        "unexpected_evaluator_ids": (),
        "inventory_binding_errors": (),
        "invalid_evaluator_ids": (),
        "duplicate_evaluator_ids": (),
        "non_distinct_evaluator_ids": (),
    }


def _normalize_required_evaluator_ids(
    value: Any,
    *,
    present: bool,
) -> tuple[tuple[str, ...], bool, tuple[str, ...]]:
    """Normalize an optional active evaluator inventory binding.

    A missing field means the registry is intentionally low-tier-only and does
    not claim full evaluator coverage.  Once the field is present, an empty,
    malformed, or duplicate list is invalid rather than silently becoming an
    empty requirement.
    """

    if not present:
        return (), True, ()
    if not isinstance(value, (list, tuple)):
        return (), False, ("required_evaluator_ids_not_list",)
    if not value:
        return (), False, ("required_evaluator_ids_empty",)
    normalized: list[str] = []
    seen: set[str] = set()
    errors: list[str] = []
    for item in value:
        evaluator_id = _string(item)
        if _EVALUATOR_ID_RE.fullmatch(evaluator_id) is None:
            errors.append("required_evaluator_id_invalid")
            continue
        if evaluator_id in seen:
            errors.append("required_evaluator_ids_duplicate")
            continue
        seen.add(evaluator_id)
        normalized.append(evaluator_id)
    return tuple(sorted(normalized)), not errors, tuple(sorted(set(errors)))


def validate_golden_registry_inventory(
    required_evaluator_ids: Any,
    covered_evaluator_ids: Sequence[str],
    *,
    present: bool = True,
) -> dict[str, Any]:
    """Validate an explicit evaluator inventory against registry entries.

    ``present=False`` is the compatibility path for a low-tier-only registry:
    it reports no binding requirement and does not turn the empty evaluator
    set into a false claim of full coverage.
    """

    required, valid, format_errors = _normalize_required_evaluator_ids(
        required_evaluator_ids,
        present=present,
    )
    covered = tuple(sorted({
        _string(value)
        for value in covered_evaluator_ids
        if _string(value)
    }))
    if not present:
        return {
            "present": False,
            "valid": True,
            "required_evaluator_ids": (),
            "missing_required_evaluator_ids": (),
            "unexpected_evaluator_ids": (),
            "errors": (),
        }
    covered_set = set(covered)
    required_set = set(required)
    missing = tuple(sorted(required_set - covered_set))
    unexpected = tuple(sorted(covered_set - required_set))
    errors = list(format_errors)
    if missing:
        errors.append("required_evaluator_missing")
    if unexpected:
        errors.append("evaluator_inventory_unexpected")
    return {
        "present": True,
        "valid": bool(valid and not missing and not unexpected),
        "required_evaluator_ids": required,
        "missing_required_evaluator_ids": missing,
        "unexpected_evaluator_ids": unexpected,
        "errors": tuple(sorted(set(errors))),
    }


def release_golden_registry_status(
    path: Path = GOLDEN_REGISTRY_PATH,
    *,
    required_evaluator_ids: Sequence[str] | None = None,
) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return _empty_golden_registry_status(present=False)
    if not isinstance(payload, Mapping):
        return _empty_golden_registry_status(present=True)
    low = payload.get("low_tier_suite")
    entries = payload.get("evaluators")
    commit = _string(payload.get("pipeline_commit"))
    tree = _string(payload.get("pipeline_tree"))
    base_valid = (
        payload.get("schema_version") == GOLDEN_REGISTRY_SCHEMA_VERSION
        and _GIT_OID_RE.fullmatch(commit) is not None
        and _GIT_OID_RE.fullmatch(tree) is not None
        and isinstance(low, Mapping)
        and low.get("status") in {"passed", "failing", "pending"}
        and type(low.get("positive_case_count")) is int
        and int(low.get("positive_case_count")) >= 0
        and type(low.get("negative_case_count")) is int
        and int(low.get("negative_case_count")) >= 0
        and _SHA256_RE.fullmatch(_string(low.get("receipt_sha256"))) is not None
        and _string(low.get("vm_path")).startswith("/mnt/tmp/")
        and _string(low.get("user_visible_path")).startswith("//hfs1.minieye.tech/")
        and isinstance(entries, list)
    )
    entry_shape_valid = isinstance(entries, list)
    valid = bool(base_valid)
    covered: set[str] = set()
    normalized: dict[str, dict[str, Any]] = {}
    invalid_ids: list[str] = []
    duplicate_ids: list[str] = []
    non_distinct_ids: list[str] = []
    for item in entries if isinstance(entries, list) else ():
        if not isinstance(item, Mapping):
            valid = False
            continue
        evaluator_id = _string(item.get("evaluator_id"))
        source, positive, negative, receipt = tuple(
            _string(item.get(field)) for field in _GOLDEN_HASH_FIELDS
        )
        if _EVALUATOR_ID_RE.fullmatch(evaluator_id) is None:
            invalid_ids.append(evaluator_id)
        elif evaluator_id in covered:
            duplicate_ids.append(evaluator_id)
        hashes = (source, positive, negative, receipt)
        hashes_valid = all(_SHA256_RE.fullmatch(value) is not None for value in hashes)
        hashes_distinct = hashes_valid and len(set(hashes)) == len(hashes)
        if hashes_valid and not hashes_distinct:
            non_distinct_ids.append(evaluator_id)
        if (
            _EVALUATOR_ID_RE.fullmatch(evaluator_id) is None
            or evaluator_id in covered
            or item.get("status") != "passed"
            or _SHA256_RE.fullmatch(source) is None
            or _SHA256_RE.fullmatch(positive) is None
            or _SHA256_RE.fullmatch(negative) is None
            or _SHA256_RE.fullmatch(receipt) is None
            or not hashes_distinct
        ):
            valid = False
            continue
        covered.add(evaluator_id)
        normalized[evaluator_id] = dict(item)
    payload_required_present = "required_evaluator_ids" in payload
    payload_required = payload.get("required_evaluator_ids")
    payload_binding = validate_golden_registry_inventory(
        payload_required,
        tuple(sorted(covered)),
        present=payload_required_present,
    )
    external_binding = validate_golden_registry_inventory(
        required_evaluator_ids,
        tuple(sorted(covered)),
        present=required_evaluator_ids is not None,
    )
    binding_errors = set(payload_binding["errors"])
    if required_evaluator_ids is not None:
        binding_errors.update(external_binding["errors"])
        if payload_required_present and (
            tuple(payload_binding["required_evaluator_ids"])
            != tuple(external_binding["required_evaluator_ids"])
        ):
            binding_errors.add("required_evaluator_ids_binding_mismatch")
    effective_binding = external_binding if required_evaluator_ids is not None else payload_binding
    if not entry_shape_valid or not valid or binding_errors:
        valid = False
    low_tier_ready = bool(
        base_valid
        and isinstance(low, Mapping)
        and low.get("status") == "passed"
        and int(low.get("positive_case_count", 0)) > 0
        and int(low.get("negative_case_count", 0)) > 0
    )
    return {
        "present": True,
        "valid": bool(valid),
        "pipeline_commit": commit,
        "pipeline_tree": tree,
        "low_tier_golden_ready": low_tier_ready,
        "evaluators": normalized,
        "required_evaluator_ids": effective_binding["required_evaluator_ids"],
        "required_evaluator_ids_present": bool(
            payload_required_present or required_evaluator_ids is not None
        ),
        "inventory_binding_valid": bool(
            (payload_required_present or required_evaluator_ids is not None)
            and effective_binding["valid"]
            and not binding_errors
        ),
        "missing_required_evaluator_ids": effective_binding[
            "missing_required_evaluator_ids"
        ],
        "unexpected_evaluator_ids": effective_binding["unexpected_evaluator_ids"],
        "inventory_binding_errors": tuple(sorted(binding_errors)),
        "invalid_evaluator_ids": tuple(sorted(set(invalid_ids))),
        "duplicate_evaluator_ids": tuple(sorted(set(duplicate_ids))),
        "non_distinct_evaluator_ids": tuple(sorted(set(non_distinct_ids))),
    }


def _golden_coverage(
    evaluator_keys: Sequence[str],
) -> tuple[tuple[str, ...], bool, bool, bool, bool]:
    registry = release_golden_registry_status()
    evaluator_registry = registry.get("evaluators")
    covered = set(evaluator_registry) if isinstance(evaluator_registry, Mapping) else set()
    required = set(evaluator_keys)
    valid = registry.get("valid") is True
    complete = bool(required) and valid and required.issubset(covered)
    return (
        tuple(sorted(covered)),
        registry.get("present") is True,
        valid,
        registry.get("low_tier_golden_ready") is True,
        complete,
    )


def _actual_supported_evaluator_keys(
    contract: Mapping[str, Any],
) -> tuple[tuple[str, ...], bool, bool]:
    capability = _mapping(contract.get("consumer_capability"))
    if not capability:
        summary = _mapping(contract.get("summary"))
        capability = _mapping(summary.get("consumer_capability"))
    if "actual_evaluators" not in capability:
        return (), False, True
    emitted = capability.get("actual_evaluators")
    if not isinstance(emitted, list):
        return (), True, False
    keys: list[str] = []
    valid = True
    seen: set[str] = set()
    for item in emitted:
        if not isinstance(item, Mapping):
            valid = False
            continue
        # The producer publishes evaluator["key"] as evaluator_id.  Do not
        # inspect inventory, seeds, aliases, legacy IDs, or unused capabilities.
        evaluator_key = _string(item.get("evaluator_id"))
        status = _string(item.get("status"))
        if not evaluator_key or not status:
            valid = False
            continue
        if evaluator_key in seen:
            valid = False
            continue
        seen.add(evaluator_key)
        if status == "supported":
            keys.append(evaluator_key)
    return tuple(sorted(keys)), True, valid


def _causal_roles(contract: Mapping[str, Any]) -> tuple[tuple[str, ...], bool]:
    public = _mapping(contract.get("public_result"))
    causal = _mapping(public.get("causal_chain"))
    role_aliases = {
        "现象": "phenomenon",
        "phenomenon": "phenomenon",
        "symptom": "phenomenon",
        "证据": "evidence",
        "evidence": "evidence",
        "因果判断": "causal_judgement",
        "causal_judgement": "causal_judgement",
        "causal_judgment": "causal_judgement",
        "cause": "causal_judgement",
    }
    roles: set[str] = set()
    narrative = causal.get("narrative")
    for item in narrative if isinstance(narrative, list) else ():
        if not isinstance(item, Mapping) or not _string(item.get("text")):
            continue
        normalized = role_aliases.get(_string(item.get("role")).lower())
        if normalized:
            roles.add(normalized)
    required = {"phenomenon", "evidence", "causal_judgement"}
    return tuple(sorted(roles)), required.issubset(roles)


def _structural_evidence(
    contract: Mapping[str, Any], refs: Sequence[str]
) -> dict[str, bool]:
    def bounded_number(value: Any) -> bool:
        if type(value) is int:
            return abs(value) <= 10**18
        return (
            type(value) is float
            and math.isfinite(value)
            and abs(value) <= 10**18
        )

    capability = _mapping(contract.get("consumer_capability"))
    if not capability:
        summary = _mapping(contract.get("summary"))
        capability = _mapping(summary.get("consumer_capability"))
    evidence = _mapping(capability.get("evidence"))
    frame = evidence.get("issue_frame_id")
    frame_present = type(frame) is int and frame >= 0
    focus = _mapping(evidence.get("focus_window"))
    start_ts = focus.get("start_ts")
    end_ts = focus.get("end_ts")
    ts_pair = (
        bounded_number(start_ts)
        and bounded_number(end_ts)
        and start_ts <= end_ts
    )
    start_frame = focus.get("start_frame")
    end_frame = focus.get("end_frame")
    frame_pair = (
        type(start_frame) is int
        and type(end_frame) is int
        and start_frame >= 0
        and start_frame <= end_frame
    )
    focus_present = ts_pair or frame_pair
    lineage = _mapping(evidence.get("field_lineage"))
    field_complete = (
        lineage.get("schema_version") == "g1q3_field_lineage_v2"
        and lineage.get("fidelity_ok") is True
        and _text(lineage.get("status")).lower() in {"pass", "passed", "verified"}
        and not lineage.get("errors")
    )
    viz = _mapping(evidence.get("viz_lineage"))
    viz_complete = (
        viz.get("schema_version") == "g1q3_viz_lineage_v1"
        and viz.get("ok") is True
        and _text(viz.get("status")).lower() in {"pass", "passed", "verified"}
        and not viz.get("errors")
    )
    return {
        "issue_frame_present": frame_present,
        "focus_window_present": focus_present,
        "field_lineage_complete": field_complete,
        "viz_lineage_complete": viz_complete,
        "evidence_complete": bool(
            refs and frame_present and focus_present and field_complete and viz_complete
        ),
    }


def normalize_terminal_class(value: Any) -> str:
    normalized = _text(value).lower().replace("-", "_")
    if normalized in TERMINAL_CLASSES:
        return normalized
    if normalized.startswith("honest_non_attribution") or normalized in {
        "honest_route_readiness_terminal",
        "not_in_rca_ingress",
        "low",
    }:
        return HONEST_NON_ATTRIBUTION
    return _LEGACY_CLASS_MAP.get(normalized, "")


def _contract_claim(contract: Mapping[str, Any]) -> tuple[str, str]:
    public = _mapping(contract.get("public_result"))
    for container in (
        contract,
        public,
        _mapping(contract.get("summary")),
        _mapping(public.get("summary")),
    ):
        for key in ("terminal_class", "quality_classification"):
            raw_claim = _string(container.get(key))
            claimed = normalize_terminal_class(raw_claim)
            if claimed:
                return claimed, ""
            if raw_claim:
                return "", raw_claim
    return "", ""


def _contract_human_decision(contract: Mapping[str, Any]) -> str:
    public = _mapping(contract.get("public_result"))
    for container in (
        contract,
        public,
        _mapping(contract.get("summary")),
        _mapping(public.get("summary")),
    ):
        decision = _string(container.get("human_decision"))
        if decision:
            return decision
    return ""


def _contract_approval_ready(contract: Mapping[str, Any]) -> bool:
    public = _mapping(contract.get("public_result"))
    for container in (
        contract,
        public,
        _mapping(contract.get("summary")),
        _mapping(public.get("summary")),
    ):
        if container.get("approval_ready") is True:
            return True
        status = _text(
            container.get("approval_status") or container.get("quality_status")
        )
        if status.lower().replace("_", "-") == "quality-approved":
            return True
    return False


def evaluate_structural_tier(
    contract: Mapping[str, Any] | None,
    *,
    claimed_terminal_class: Any = None,
    human_decision: Any = None,
    approval_ready: bool | None = None,
    publication_text: Any = None,
    execution_outcome: Any = "",
    terminal_error_code: Any = "",
    consumer_delivery_status: Any = "",
) -> TierOracleResult:
    """Recompute one of five terminal classes from production structure."""

    material = contract if isinstance(contract, Mapping) else {}
    public_parts = _public_parts(material)
    combined_public = "\n".join(public_parts)
    rendered = _text(publication_text)
    classification_text = f"{combined_public}\n{rendered}"
    conclusion = _conclusion(material)
    lowered_public = classification_text.lower()
    explicit_non_attribution = any(
        marker.lower() in lowered_public for marker in _NON_ATTRIBUTION_MARKERS
    )
    public = _mapping(material.get("public_result"))
    report = _mapping(material.get("report"))
    responsibility_structure = _mapping(public.get("responsibility"))
    responsibility_status = _string(responsibility_structure.get("status")).lower()
    candidate_wording = (
        any(marker in classification_text for marker in _CANDIDATE_MARKERS)
        or report.get("is_candidate") is True
        or responsibility_status.startswith("candidate")
        or responsibility_status in {"hypothesis", "needs_review", "待人工确认"}
    )
    evaluator_keys, inventory_present, inventory_valid = (
        _actual_supported_evaluator_keys(material)
    )
    (
        golden_keys,
        golden_present,
        golden_valid,
        low_golden_ready,
        golden_complete,
    ) = _golden_coverage(evaluator_keys)
    refs = _evidence_refs(material)
    evidence = _structural_evidence(material, refs)
    roles, causal_closed = _causal_roles(material)
    responsibility = _responsibility(material)

    normalized_consumer_status = _text(consumer_delivery_status).lower()
    normalized_outcome = _text(execution_outcome).lower()
    normalized_error = _text(terminal_error_code).lower()
    if normalized_consumer_status in _CONSUMER_FAILURE_STATES:
        terminal_class = CONSUMER_DELIVERY_FAILURE
    elif normalized_error in _ROUTE_BOUNDARY_ERROR_CODES:
        terminal_class = HONEST_NON_ATTRIBUTION
    elif normalized_error:
        terminal_class = TECHNICAL_FAILURE
    elif normalized_outcome in _TECHNICAL_OUTCOMES:
        terminal_class = TECHNICAL_FAILURE
    elif (
        evaluator_keys
        and golden_complete
        and evidence["evidence_complete"]
        and causal_closed
        and responsibility
        and conclusion
        and not explicit_non_attribution
        and not candidate_wording
    ):
        terminal_class = SUPPORTED_ATTRIBUTION
    elif (
        evaluator_keys
        and causal_closed
        and responsibility
        and conclusion
        and not explicit_non_attribution
    ):
        terminal_class = CANDIDATE_HYPOTHESIS
    else:
        terminal_class = HONEST_NON_ATTRIBUTION

    confidence_tier = {
        SUPPORTED_ATTRIBUTION: "high",
        CANDIDATE_HYPOTHESIS: "medium",
        HONEST_NON_ATTRIBUTION: "low",
        TECHNICAL_FAILURE: "none",
        CONSUMER_DELIVERY_FAILURE: "none",
    }[terminal_class]
    violations: list[str] = []
    if inventory_present and not inventory_valid:
        violations.append("actual_evaluator_inventory_invalid")
    for phrase in BANNED_PUBLIC_PHRASES:
        if phrase in combined_public or phrase in _text(publication_text):
            violations.append(f"banned_public_phrase:{phrase}")

    if claimed_terminal_class is not None:
        raw_claim = _string(claimed_terminal_class)
        claimed = normalize_terminal_class(raw_claim)
        invalid_claim = raw_claim if raw_claim and not claimed else ""
    else:
        claimed, invalid_claim = _contract_claim(material)
    if invalid_claim:
        violations.append(f"terminal_class_invalid:{invalid_claim}")
    if claimed and claimed != terminal_class:
        violations.append(f"terminal_class_mismatch:{claimed}:{terminal_class}")
    if claimed == SUPPORTED_ATTRIBUTION:
        if not evaluator_keys:
            violations.append("supported_attribution_evaluator_count_zero")
        if not refs:
            violations.append("supported_attribution_evidence_ref_empty")
        if not golden_complete:
            violations.append("supported_attribution_golden_coverage_missing")
        if not responsibility:
            violations.append("supported_attribution_responsibility_empty")
        if explicit_non_attribution:
            violations.append("supported_attribution_non_attribution_wording")

    decision = (
        _string(human_decision)
        if human_decision is not None
        else _contract_human_decision(material)
    )
    approval_claim = (
        bool(approval_ready)
        if approval_ready is not None
        else _contract_approval_ready(material)
    )
    if approval_claim and not decision:
        violations.append("approval_ready_without_human_decision")

    if not decision and _APPROVAL_READY_RE.search(rendered):
        violations.append("approval_ready_without_human_decision")
    if terminal_class == HONEST_NON_ATTRIBUTION:
        if responsibility:
            violations.append("honest_non_attribution_responsibility_present")
        if any(marker in classification_text for marker in _CANDIDATE_MARKERS):
            violations.append("honest_non_attribution_candidate_wording")
        if _LOW_TIER_USER_ACTION_RE.search(classification_text):
            violations.append("honest_non_attribution_user_action")
        if _LOW_TIER_BLAME_RE.search(classification_text):
            violations.append("honest_non_attribution_blame_wording")
    if rendered:
        if (
            terminal_class == CANDIDATE_HYPOTHESIS
            and MEDIUM_TIER_DISCLAIMER not in rendered
        ):
            violations.append("candidate_disclaimer_missing")
        if terminal_class == SUPPORTED_ATTRIBUTION and any(
            marker.lower() in rendered.lower() for marker in _NON_ATTRIBUTION_MARKERS
        ):
            violations.append("supported_publication_non_attribution_wording")

    facts = StructuralTierFacts(
        supported_evaluator_keys=evaluator_keys,
        supported_evaluator_count=len(evaluator_keys),
        actual_evaluator_inventory_present=inventory_present,
        actual_evaluator_inventory_valid=inventory_valid,
        golden_covered_evaluator_keys=golden_keys,
        golden_registry_present=golden_present,
        golden_registry_valid=golden_valid,
        low_tier_golden_ready=low_golden_ready,
        golden_coverage_complete=golden_complete,
        evidence_ref_count=len(refs),
        issue_frame_present=evidence["issue_frame_present"],
        focus_window_present=evidence["focus_window_present"],
        field_lineage_complete=evidence["field_lineage_complete"],
        viz_lineage_complete=evidence["viz_lineage_complete"],
        evidence_complete=evidence["evidence_complete"],
        causal_chain_roles=roles,
        causal_chain_closed=causal_closed,
        responsibility=responsibility,
        responsibility_present=bool(responsibility),
        conclusion_present=bool(conclusion),
        explicit_non_attribution=explicit_non_attribution,
        candidate_wording_present=candidate_wording,
    )
    unique_violations = tuple(sorted(set(violations)))
    publication_allowed = (
        terminal_class
        in {SUPPORTED_ATTRIBUTION, CANDIDATE_HYPOTHESIS, HONEST_NON_ATTRIBUTION}
        and not unique_violations
    )
    return TierOracleResult(
        schema_version="pnc_rca_structural_tier_oracle_v2",
        terminal_class=terminal_class,
        confidence_tier=confidence_tier,
        publication_allowed=publication_allowed,
        classification_conflict=bool(unique_violations),
        violations=unique_violations,
        facts=facts,
    )


def require_publishable(result: TierOracleResult) -> TierOracleResult:
    if not result.publication_allowed:
        raise TierOracleConflict(result)
    return result


def public_tier_from_rendered_text(value: Any) -> str:
    """Recover the tier from an oracle-rendered field for deterministic replay."""

    text = _text(value)
    if MEDIUM_TIER_DISCLAIMER in text:
        return CANDIDATE_HYPOTHESIS
    if "责任模块：暂无法判断" in text or "未形成可确认的归因结论" in text:
        return HONEST_NON_ATTRIBUTION
    return SUPPORTED_ATTRIBUTION
