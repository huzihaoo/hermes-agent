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
    r"\bquality[-_]approved\b|\bapproval_ready\s*(?:[:=]|为)\s*(?:true|1|是)",
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
            value = next(
                (
                    _string(item.get(key))
                    for key in (
                        "evidence_ref",
                        "ref",
                        "id",
                        "path",
                        "field",
                        "check",
                        "name",
                        "summary",
                    )
                    if _string(item.get(key))
                ),
                "",
            )
        else:
            value = _string(item)
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
                value = next(
                    (
                        _string(item.get(key))
                        for key in ("evidence_ref", "ref", "id", "path")
                        if _string(item.get(key))
                    ),
                    "",
                )
            else:
                value = ""
            if value:
                refs.append(value)
    return tuple(sorted(set(refs)))


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
    capability = _mapping(contract.get("consumer_capability"))
    if not capability:
        summary = _mapping(contract.get("summary"))
        capability = _mapping(summary.get("consumer_capability"))
    evidence = _mapping(capability.get("evidence"))
    frame_present = evidence.get("issue_frame_id") not in {None, ""}
    focus = _mapping(evidence.get("focus_window"))
    focus_present = any(
        focus.get(key) not in {None, ""} for key in ("start_ts", "start_frame")
    ) and any(focus.get(key) not in {None, ""} for key in ("end_ts", "end_frame"))
    lineage = _mapping(evidence.get("field_lineage"))
    field_complete = lineage.get("fidelity_ok") is True
    viz = _mapping(evidence.get("viz_lineage"))
    viz_complete = viz.get("ok") is True or _text(viz.get("status")).lower() in {
        "pass",
        "passed",
        "completed",
        "verified",
    }
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
    conclusion = _conclusion(material)
    lowered_public = combined_public.lower()
    explicit_non_attribution = any(
        marker.lower() in lowered_public for marker in _NON_ATTRIBUTION_MARKERS
    )
    public = _mapping(material.get("public_result"))
    report = _mapping(material.get("report"))
    responsibility_structure = _mapping(public.get("responsibility"))
    responsibility_status = _string(responsibility_structure.get("status")).lower()
    candidate_wording = (
        any(marker in combined_public for marker in _CANDIDATE_MARKERS)
        or report.get("is_candidate") is True
        or responsibility_status.startswith("candidate")
        or responsibility_status in {"hypothesis", "needs_review", "待人工确认"}
    )
    evaluator_keys, inventory_present, inventory_valid = (
        _actual_supported_evaluator_keys(material)
    )
    refs = _evidence_refs(material)
    evidence = _structural_evidence(material, refs)
    roles, causal_closed = _causal_roles(material)
    responsibility = _responsibility(material)

    normalized_consumer_status = _text(consumer_delivery_status).lower()
    normalized_outcome = _text(execution_outcome).lower()
    normalized_error = _text(terminal_error_code).lower()
    if normalized_consumer_status in _CONSUMER_FAILURE_STATES:
        terminal_class = CONSUMER_DELIVERY_FAILURE
    elif normalized_error and normalized_error not in _ROUTE_BOUNDARY_ERROR_CODES:
        terminal_class = TECHNICAL_FAILURE
    elif normalized_outcome in _TECHNICAL_OUTCOMES:
        terminal_class = TECHNICAL_FAILURE
    elif (
        evaluator_keys
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

    rendered = _text(publication_text)
    if not decision and _APPROVAL_READY_RE.search(rendered):
        violations.append("approval_ready_without_human_decision")
    if rendered:
        if (
            terminal_class == CANDIDATE_HYPOTHESIS
            and MEDIUM_TIER_DISCLAIMER not in rendered
        ):
            violations.append("candidate_disclaimer_missing")
        if terminal_class == HONEST_NON_ATTRIBUTION:
            if _LOW_TIER_USER_ACTION_RE.search(rendered):
                violations.append("honest_non_attribution_user_action")
            if _LOW_TIER_BLAME_RE.search(rendered):
                violations.append("honest_non_attribution_blame_wording")
        if terminal_class == SUPPORTED_ATTRIBUTION and any(
            marker.lower() in rendered.lower() for marker in _NON_ATTRIBUTION_MARKERS
        ):
            violations.append("supported_publication_non_attribution_wording")

    facts = StructuralTierFacts(
        supported_evaluator_keys=evaluator_keys,
        supported_evaluator_count=len(evaluator_keys),
        actual_evaluator_inventory_present=inventory_present,
        actual_evaluator_inventory_valid=inventory_valid,
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
        schema_version="pnc_rca_structural_tier_oracle_v1",
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
