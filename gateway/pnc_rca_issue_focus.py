"""Composable issue-intent and evidence coverage contract for G1Q3 RCA.

Official Feishu fields still own business routing. The immutable title is used
only to bind what the analysis must answer. Unknown or unsupported questions
fail closed instead of being filled by an unrelated generic evaluator.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
import re
from typing import Any, Mapping, Sequence


ISSUE_INTENT_SCHEMA_VERSION = "g1q3_issue_intent_v1"
ISSUE_FOCUS_EVIDENCE_SCHEMA_VERSION = "g1q3_issue_focus_evidence_v2"
ISSUE_FOCUS_VALIDATION_SCHEMA_VERSION = "g1q3_issue_focus_validation_v1"
ISSUE_FOCUS_PLAN_SCHEMA_VERSION = "g1q3_issue_focus_plan_v1"
ISSUE_FOCUS_PLAN_STATUS_PLANNED = "planned"

ANALYSIS_COMPLETE = "complete"
ANALYSIS_INSUFFICIENT_STATEMENT = "insufficient_statement"
ANALYSIS_CAPABILITY_UNSUPPORTED = "capability_unsupported"
ANALYSIS_EVIDENCE_INCOMPLETE = "evidence_incomplete"
ANALYSIS_EVIDENCE_CONFLICT = "evidence_conflict"

_NON_COMPLETE_STATUSES = frozenset(
    {
        ANALYSIS_INSUFFICIENT_STATEMENT,
        ANALYSIS_CAPABILITY_UNSUPPORTED,
        ANALYSIS_EVIDENCE_INCOMPLETE,
        ANALYSIS_EVIDENCE_CONFLICT,
    }
)
_CHECK_STATUSES = frozenset({"supported", "refuted", "inconclusive"})
_CAPABILITY_STATUSES = frozenset({"available"})
_TITLE_PREFIX_RE = re.compile(
    r"^(?:\s*\[[^]]+\]\s*)?(ACC|AEB|FCW|HMI|LCC|LDP|ELKA|TSR|TSI)\s*[-：:]\s*",
    re.I,
)
_DOMAIN_MAP = {
    "ACC": "longitudinal_control",
    "AEB": "active_safety",
    "FCW": "active_safety",
    "HMI": "hmi",
    "LCC": "lateral_control",
    "LDP": "lateral_safety",
    "ELKA": "lateral_safety",
    "TSR": "traffic_sign",
    "TSI": "traffic_sign",
}
_ENTITY_CLASS_BY_ROLE = {
    "lead_lead_target": "vehicle",
    "lead_target": "vehicle",
    "cut_in_target": "vehicle",
    "cut_out_target": "vehicle",
    "vru_target": "vru",
    "two_wheeler_target": "two_wheeler",
    "lateral_target": "vehicle",
}


class IssueFocusContractError(ValueError):
    def __init__(self, code: str, detail: str = ""):
        self.code = str(code or "issue_focus_contract_invalid")[:120]
        self.detail = str(detail or self.code)[:1000]
        super().__init__(self.detail)


@dataclass(frozen=True)
class IntentContribution:
    phenomena: tuple[str, ...] = ()
    segments: tuple[str, ...] = ()
    capabilities: tuple[str, ...] = ()
    measurements: tuple[str, ...] = ()
    checks: tuple[str, ...] = ()
    calculations: tuple[str, ...] = ()


@dataclass(frozen=True)
class IntentRule:
    pattern: re.Pattern[str]
    contribution: IntentContribution


@dataclass(frozen=True)
class EntityRule:
    pattern: re.Pattern[str]
    role: str
    object_class: str


@dataclass(frozen=True)
class IssueIntent:
    schema_version: str
    domain: str
    phenomena: tuple[str, ...]
    entity_roles: tuple[str, ...]
    required_capabilities: tuple[str, ...]
    required_segments: tuple[str, ...]
    required_entities: tuple[str, ...]
    required_measurements: tuple[str, ...]
    required_checks: tuple[str, ...]
    required_calculations: tuple[str, ...]
    statement_sufficient: bool
    unresolved_statement: str
    intent_sha256: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "domain": self.domain,
            "phenomena": list(self.phenomena),
            "entity_roles": list(self.entity_roles),
            "required_capabilities": list(self.required_capabilities),
            "required_segments": list(self.required_segments),
            "required_entities": list(self.required_entities),
            "required_measurements": list(self.required_measurements),
            "required_checks": list(self.required_checks),
            "required_calculations": list(self.required_calculations),
            "statement_sufficient": self.statement_sufficient,
            "unresolved_statement": self.unresolved_statement,
            "intent_sha256": self.intent_sha256,
        }


@dataclass(frozen=True)
class IssueFocusValidation:
    intent_sha256: str
    analysis_status: str
    attribution_allowed: bool
    missing_requirements: tuple[str, ...]
    unsupported_capabilities: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": ISSUE_FOCUS_VALIDATION_SCHEMA_VERSION,
            "intent_sha256": self.intent_sha256,
            "analysis_status": self.analysis_status,
            "attribution_allowed": self.attribution_allowed,
            "missing_requirements": list(self.missing_requirements),
            "unsupported_capabilities": list(self.unsupported_capabilities),
        }


def _contribution(
    phenomenon: str,
    *,
    segment: str = "",
    capabilities: Sequence[str] = (),
    measurements: Sequence[str] = (),
    checks: Sequence[str] = (),
    calculations: Sequence[str] = (),
) -> IntentContribution:
    return IntentContribution(
        phenomena=(phenomenon,),
        segments=(segment,) if segment else (),
        capabilities=tuple(capabilities),
        measurements=tuple(measurements),
        checks=tuple(checks),
        calculations=tuple(calculations),
    )


_LONGITUDINAL_BASE = ("ego_longitudinal_kinematics", "longitudinal_command_chain")
_LONGITUDINAL_CURVES = ("ego_speed_curve", "ego_acceleration_curve")

_INTENT_RULES = (
    IntentRule(
        re.compile(r"(?:不减速|未减速|无减速|不刹车|未刹车|无刹车|刹不住|无法刹停|跟停失败)"),
        _contribution(
            "insufficient_braking",
            segment="insufficient_braking_window",
            capabilities=_LONGITUDINAL_BASE,
            measurements=_LONGITUDINAL_CURVES,
            checks=("brake_command_chain",),
        ),
    ),
    IntentRule(
        re.compile(r"(?:减速过晚|减速太晚|刹车晚|制动晚|制动过晚|刹车太晚|响应晚|响应过晚|退出晚|晚退出|进入过慢|进入太慢)"),
        _contribution(
            "late_response",
            segment="late_response_window",
            capabilities=_LONGITUDINAL_BASE,
            measurements=_LONGITUDINAL_CURVES,
            checks=("command_timing",),
        ),
    ),
    IntentRule(
        re.compile(r"(?:减速过重|减速过大|减速过多|减速不平顺|刹车过重|突然刹车|突然减速|急减速|顿挫强|加减速过急|加减速不平顺)"),
        _contribution(
            "excessive_deceleration",
            segment="excessive_deceleration_window",
            capabilities=_LONGITUDINAL_BASE,
            measurements=_LONGITUDINAL_CURVES,
            checks=("deceleration_reasonableness",),
        ),
    ),
    IntentRule(
        re.compile(r"(?:异常减速|非预期减速|无前车[^，,]*减速|突然释放加速|释放加速|加速后(?:减速|刹停)|有加速后减速)"),
        _contribution(
            "unexpected_longitudinal_response",
            segment="unexpected_longitudinal_window",
            capabilities=_LONGITUDINAL_BASE,
            measurements=_LONGITUDINAL_CURVES,
            checks=("longitudinal_command_chain", "expected_behavior_comparison"),
        ),
    ),
    IntentRule(
        re.compile(r"(?:频繁加减速|加减速[^，,]*(?:不平顺|过急)|车速不稳|车速波动|来回跳动|顿挫)"),
        _contribution(
            "longitudinal_instability",
            segment="longitudinal_instability_window",
            capabilities=_LONGITUDINAL_BASE,
            measurements=_LONGITUDINAL_CURVES,
            checks=("longitudinal_stability",),
        ),
    ),
    IntentRule(
        re.compile(r"(?:二次起步|起步慢|跟随起步慢|溜车)"),
        _contribution(
            "start_behavior_anomaly",
            segment="start_behavior_window",
            capabilities=("ego_longitudinal_kinematics", "drive_command_chain"),
            measurements=_LONGITUDINAL_CURVES,
            checks=("drive_command_chain",),
        ),
    ),
    IntentRule(
        re.compile(r"(?:目标丢失|模型释放|车模消失|模型消失|车道线短暂消失|一会儿消失一会儿显示|限速消失)"),
        _contribution(
            "output_discontinuity",
            segment="output_gap_window",
            capabilities=("output_continuity",),
            checks=("output_continuity",),
        ),
    ),
    IntentRule(
        re.compile(r"(?:不显示|无目标|无前车模型|无车道线显示|仪表无显示|无仪表[^，,]*提示|无文言提示|不提示文言|无提示音)"),
        _contribution(
            "missing_hmi_output",
            segment="missing_hmi_output_window",
            capabilities=("hmi_projection",),
            checks=("hmi_output_presence",),
        ),
    ),
    IntentRule(
        re.compile(r"(?:前方|前车|前方车辆)[^，,]*(?:仪表)?无目标|(?:仪表)?无(?:前车|前方)目标"),
        _contribution(
            "front_target_output_missing",
            segment="front_target_output_missing_window",
            capabilities=(
                "detection_pipeline",
                "object_identity",
                "object_track_kinematics",
                "output_continuity",
                "hmi_projection",
            ),
            measurements=("target_speed_curve", "target_distance_curve"),
            checks=(
                "detection_presence",
                "object_output_continuity",
                "target_selection_state",
                "hmi_mapping",
            ),
        ),
    ),
    IntentRule(
        re.compile(r"(?:闪过|闪一下|闪烁|跳闪|跳变|来回跳变|一瞬间[^，,]*消失|频繁混乱显示)"),
        _contribution(
            "transient_output_instability",
            segment="transient_output_window",
            capabilities=("output_continuity", "hmi_projection"),
            checks=("output_continuity", "hmi_mapping"),
        ),
    ),
    IntentRule(
        re.compile(r"(?:误识别|识别为|模型跳变为|输出左弯|方向相反|显示为弯道|显示为歪|曲率[^，,]*不符|与实际不符)"),
        _contribution(
            "classification_or_geometry_mismatch",
            segment="mismatch_window",
            capabilities=("classification_or_geometry", "hmi_projection"),
            checks=("classification_or_geometry_accuracy", "hmi_mapping"),
        ),
    ),
    IntentRule(
        re.compile(r"(?:不识别|漏识别|为识别)"),
        _contribution(
            "detection_missing",
            segment="detection_missing_window",
            capabilities=("detection_pipeline",),
            checks=("detection_presence",),
        ),
    ),
    IntentRule(
        re.compile(r"(?:异常退出|突然退出|功能马上退出|LCC退出|lcc退出|不退出|未退出|功能不退出|反复进入退出|频繁推出进入|退出又恢复|无法激活|不触发|功能.*触发|触发后|误触发|提前触发|晚触发|触发异常|报警.*(?:误|异常|未|不))"),
        _contribution(
            "function_state_anomaly",
            segment="function_state_transition_window",
            capabilities=("function_state_transition",),
            measurements=("function_state_timeline",),
            checks=("function_state_transition",),
        ),
    ),
    IntentRule(
        re.compile(r"(?:拉偏|偏左|偏右|向左偏|向右偏|偏向路沿|严重偏右|外切压线|压线|冲出车道|向左打方向|向右打方向|方向盘.*(?:摆动|抖动|来回|动作|修正|调节|轻微左打|向右打|回打))"),
        _contribution(
            "lateral_path_anomaly",
            segment="lateral_path_window",
            capabilities=("ego_lateral_kinematics", "lane_geometry", "lateral_command_chain"),
            measurements=("ego_lateral_curve", "lane_geometry_curve"),
            checks=("lateral_command_chain", "lane_boundary_relation"),
        ),
    ),
    IntentRule(
        re.compile(r"(?:无纠偏|未纠偏|不纠偏|纠偏不足|没有纠偏)"),
        _contribution(
            "lateral_correction_missing",
            segment="lateral_correction_window",
            capabilities=("ego_lateral_kinematics", "lane_geometry", "lateral_command_chain"),
            measurements=("ego_lateral_curve", "lane_geometry_curve", "steering_angle_curve"),
            checks=("lateral_correction_command", "lane_boundary_relation"),
        ),
    ),
    IntentRule(
        re.compile(r"(?:抑制(?:TJA|LCC|功能)?进入|(?:转向灯|拨杆).*(?:不回退|未回退).*(?:抑制|无法进入))", re.I),
        _contribution(
            "function_activation_inhibited",
            segment="function_activation_window",
            capabilities=("function_state_transition", "driver_input_state_chain"),
            measurements=("function_state_timeline", "driver_input_state_timeline"),
            checks=("activation_inhibition_reason", "driver_input_release_state"),
        ),
    ),
    IntentRule(
        re.compile(r"(?:方向盘.*(?:摆动|抖动|来回|动作|修正|调节生硬)|画龙)"),
        _contribution(
            "steering_instability",
            segment="steering_instability_window",
            capabilities=("steering_dynamics", "lateral_command_chain"),
            measurements=("steering_angle_curve", "steering_rate_curve"),
            checks=("steering_command_chain",),
        ),
    ),
    IntentRule(
        re.compile(r"(?:报takeover|报接管|takeover误报|驾驶员接管)"),
        _contribution(
            "takeover_event",
            segment="takeover_window",
            capabilities=("takeover_state_transition",),
            measurements=("takeover_state_timeline",),
            checks=("takeover_trigger_reason",),
        ),
    ),
    IntentRule(
        re.compile(r"(?:无双闪|双闪未|双闪不|双闪无能力|危险警告灯|hazard|double[ -]?flash)"),
        _contribution(
            "vehicle_signal_missing",
            segment="vehicle_signal_window",
            capabilities=("vehicle_signal_chain",),
            measurements=("vehicle_signal_timeline",),
            checks=("vehicle_signal_policy", "vehicle_signal_command", "vehicle_signal_feedback"),
        ),
    ),
    IntentRule(
        re.compile(r"(?:无超速报警|超速报警|限速融合提示|限速图标|仪表显示车速晚)"),
        _contribution(
            "speed_or_sign_display_anomaly",
            segment="speed_sign_window",
            capabilities=("traffic_sign_pipeline", "hmi_projection"),
            measurements=("ego_speed_curve", "traffic_sign_timeline"),
            checks=("traffic_sign_detection", "traffic_sign_display", "speed_alert_policy"),
        ),
    ),
    IntentRule(
        re.compile(r"(?:确认目标释放时机|确认释放时机|目标释放时机|释放时机是否合适|(?:切入|切出|目标|模型).{0,12}释放(?:过慢|时机))"),
        _contribution(
            "target_release_timing",
            segment="target_release_window",
            capabilities=("object_identity", "object_track_kinematics", "target_lifecycle_chain"),
            measurements=("target_distance_curve", "target_speed_curve"),
            checks=("target_selection_state", "target_release_timing"),
        ),
    ),
)

_ENTITY_RULES = (
    EntityRule(re.compile(r"前前(?:车|大货车|二轮车)"), "lead_lead_target", "vehicle"),
    EntityRule(re.compile(r"(?:前车|前方车辆|前方目标车|静止车)"), "lead_target", "vehicle"),
    EntityRule(re.compile(r"(?:切入|侵入).*(?:车|大车|车辆)|(?:车|大车|车辆).*(?:切入|侵入)"), "cut_in_target", "vehicle"),
    EntityRule(re.compile(r"(?:切出).*(?:车|大车|车辆)|(?:车|大车|车辆).*(?:切出)"), "cut_out_target", "vehicle"),
    EntityRule(re.compile(r"(?:环卫工人|行人|骑行者|假人|垃圾桶)"), "vru_target", "vru"),
    EntityRule(re.compile(r"(?:二轮车|两轮车|自行车|电动车|三轮车)"), "two_wheeler_target", "two_wheeler"),
    EntityRule(re.compile(r"(?:左侧|右侧|右前|左前).*(?:车|车模|车辆)"), "lateral_target", "vehicle"),
)


def normalized_issue_title(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def issue_title_sha256(value: Any) -> str:
    return hashlib.sha256(normalized_issue_title(value).encode("utf-8")).hexdigest()


def _sorted(values: set[str]) -> tuple[str, ...]:
    return tuple(sorted(value for value in values if value))


def _intent_digest(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        dict(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def resolve_issue_intent(title: Any) -> IssueIntent:
    """Compose evidence requirements from reusable phenomena and entity rules."""

    full_title = normalized_issue_title(title)
    prefix = _TITLE_PREFIX_RE.match(full_title)
    function = prefix.group(1).upper() if prefix else ""
    domain = _DOMAIN_MAP.get(function, "unresolved")
    statement = full_title[prefix.end() :].strip(" -:：") if prefix else full_title

    phenomena: set[str] = set()
    segments: set[str] = set()
    capabilities: set[str] = set()
    measurements: set[str] = set()
    checks: set[str] = set()
    calculations: set[str] = set()
    for rule in _INTENT_RULES:
        if rule.pattern.search(statement) is None:
            continue
        contribution = rule.contribution
        phenomena.update(contribution.phenomena)
        segments.update(contribution.segments)
        capabilities.update(contribution.capabilities)
        measurements.update(contribution.measurements)
        checks.update(contribution.checks)
        calculations.update(contribution.calculations)

    entity_roles: set[str] = set()
    for rule in _ENTITY_RULES:
        if rule.pattern.search(statement) is not None:
            entity_roles.add(rule.role)

    if entity_roles:
        capabilities.update(("object_identity", "object_track_kinematics"))
        measurements.update(("target_speed_curve", "target_distance_curve"))
        checks.update(("object_output_continuity", "target_selection_state"))
    if "front_target_output_missing" in phenomena:
        entity_roles.add("lead_target")
    if domain == "hmi" or "missing_hmi_output" in phenomena or "transient_output_instability" in phenomena:
        capabilities.add("hmi_projection")
        checks.add("hmi_mapping")
    if domain in {"lateral_control", "lateral_safety"} and phenomena:
        capabilities.update(("lane_geometry", "function_state_transition"))
        measurements.add("lane_geometry_curve")
    if domain == "traffic_sign" and phenomena:
        capabilities.update(("traffic_sign_pipeline", "hmi_projection"))
        measurements.add("traffic_sign_timeline")
        checks.update(("traffic_sign_detection", "traffic_sign_display"))
    if re.search(r"(?:匝道|弯道|入弯|过弯|S弯|折线弯|右转|左转)", statement, re.I) and (
        phenomena & {"excessive_deceleration", "late_response", "unexpected_longitudinal_response"}
    ):
        capabilities.add("road_geometry")
        measurements.add("road_curvature_curve")
        calculations.add("centripetal_acceleration")
        checks.add("road_geometry_reasonableness")

    statement_sufficient = bool(
        domain != "unresolved"
        and phenomena
        and (capabilities or measurements or checks)
        and len(statement) >= 4
    )
    unresolved_statement = "" if statement_sufficient else statement
    body = {
        "schema_version": ISSUE_INTENT_SCHEMA_VERSION,
        "domain": domain,
        "phenomena": list(_sorted(phenomena)),
        "entity_roles": list(_sorted(entity_roles)),
        "required_capabilities": list(_sorted(capabilities)),
        "required_segments": list(_sorted(segments)),
        "required_entities": list(_sorted(entity_roles)),
        "required_measurements": list(_sorted(measurements)),
        "required_checks": list(_sorted(checks)),
        "required_calculations": list(_sorted(calculations)),
        "statement_sufficient": statement_sufficient,
        "unresolved_statement": unresolved_statement,
    }
    return IssueIntent(**body, intent_sha256=_intent_digest(body))


def _text_sha256(value: Any) -> str:
    return hashlib.sha256(str(value or "").strip().encode("utf-8")).hexdigest()


def build_issue_focus_plan(
    *,
    title: Any,
    description_markdown: Any = "",
    comments_timeline: Sequence[Any] = (),
) -> dict[str, Any]:
    """Build a bounded, evidence-free focus plan for the Host -> VM handoff.

    The plan is a requirements projection, not an RCA result.  It is bound to
    the immutable issue title and carries only hashes/counts for supplemental
    text so transient Feishu content cannot become executable or asserted
    evidence on the VM.
    """
    normalized_title = normalized_issue_title(title)
    intent = resolve_issue_intent(normalized_title)
    supplemental = str(description_markdown or "").strip()
    comment_texts = []
    for item in comments_timeline or ():
        if isinstance(item, Mapping):
            value = item.get("content") or item.get("text") or item.get("body") or ""
        else:
            value = item
        text = str(value or "").strip()
        if text:
            comment_texts.append(text[:1200])
    evidence_text = "\n".join([supplemental[:6000], *comment_texts[:8]]).strip()

    unsupported: set[str] = set()
    capability_review: set[str] = set()
    if "vehicle_signal_chain" in intent.required_capabilities and re.search(
        r"(?:双闪|危险警告灯|hazard|double[ -]?flash)",
        normalized_title + " " + evidence_text,
        re.IGNORECASE,
    ):
        # The current evaluator inventory has no decoded hazard/double-flash
        # chain.  Keep this an explicit stop instead of borrowing AEB output.
        unsupported.add("vehicle_signal_chain")
    if "centripetal_acceleration" in intent.required_calculations:
        capability_review.add("road_geometry/centripetal_acceleration_not_connected")
    if (
        intent.domain == "longitudinal_control"
        and intent.entity_roles
        and re.search(r"(?:行人|环卫工人|骑行者|二轮车|两轮车)", normalized_title)
    ):
        capability_review.add("vru_longitudinal_brake_chain")

    if not intent.statement_sufficient:
        analysis_status = ANALYSIS_INSUFFICIENT_STATEMENT
        missing = ["statement:problem_statement"]
        stop_reason = "问题标题未形成可验证的现象与目标，停止通用评测器归因"
    elif unsupported:
        analysis_status = ANALYSIS_CAPABILITY_UNSUPPORTED
        missing = [f"capability:{key}" for key in sorted(unsupported)]
        stop_reason = "问题焦点需要当前评测器未提供的能力，停止并转能力补齐/人工复核"
    else:
        analysis_status = ISSUE_FOCUS_PLAN_STATUS_PLANNED
        missing = []
        stop_reason = ""

    body: dict[str, Any] = {
        "schema_version": ISSUE_FOCUS_PLAN_SCHEMA_VERSION,
        "analysis_status": analysis_status,
        "issue_intent": intent.to_dict(),
        "title_sha256": issue_title_sha256(normalized_title),
        "intent_sha256": intent.intent_sha256,
        "required_capabilities": list(intent.required_capabilities),
        "required_segments": list(intent.required_segments),
        "required_entities": list(intent.required_entities),
        "required_measurements": list(intent.required_measurements),
        "required_checks": list(intent.required_checks),
        "required_calculations": list(intent.required_calculations),
        "missing_requirements": missing,
        "unsupported_capabilities": sorted(unsupported),
        "capability_review": sorted(capability_review),
        "stop_reason": stop_reason,
        "source": {
            "kind": "title_plus_bounded_issue_context",
            "description_sha256": _text_sha256(supplemental[:6000]),
            "comments_sha256": _text_sha256("\n".join(comment_texts[:8])),
            "comment_count": len(comment_texts),
        },
    }
    body["plan_sha256"] = _intent_digest(body)
    return body


def _exact_mapping(value: Any, expected: set[str], label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != expected:
        raise IssueFocusContractError(f"{label}_schema_invalid")
    return value


def _text(value: Any, label: str) -> str:
    text = str(value or "").strip()
    if not text or len(text.encode("utf-8")) > 1200:
        raise IssueFocusContractError(f"{label}_invalid")
    return text


def _refs(value: Any, label: str, *, required: bool = True) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise IssueFocusContractError(f"{label}_invalid")
    refs = tuple(str(item or "").strip() for item in value)
    if (
        any(not item or len(item) > 500 for item in refs)
        or len(refs) != len(set(refs))
        or (required and not refs)
    ):
        raise IssueFocusContractError(f"{label}_invalid")
    return refs


def _named_rows(
    value: Any,
    *,
    label: str,
    expected_fields: set[str],
    name_field: str,
) -> dict[str, Mapping[str, Any]]:
    if not isinstance(value, list):
        raise IssueFocusContractError(f"issue_focus_{label}_invalid")
    rows: dict[str, Mapping[str, Any]] = {}
    for raw in value:
        row = _exact_mapping(raw, expected_fields, f"issue_focus_{label}")
        name = _text(row.get(name_field), f"issue_focus_{label}_{name_field}")
        if name in rows:
            raise IssueFocusContractError(f"issue_focus_{label}_duplicate")
        rows[name] = row
    return rows


def _validate_segments(value: Any) -> dict[str, Mapping[str, Any]]:
    rows = _named_rows(
        value,
        label="segments",
        expected_fields={"role", "start_ts", "end_ts", "evidence_refs"},
        name_field="role",
    )
    for row in rows.values():
        start = row.get("start_ts")
        end = row.get("end_ts")
        if (
            type(start) not in {int, float}
            or type(end) not in {int, float}
            or not math.isfinite(float(start))
            or not math.isfinite(float(end))
            or float(start) > float(end)
        ):
            raise IssueFocusContractError("issue_focus_segment_window_invalid")
        _refs(row.get("evidence_refs"), "issue_focus_segment_refs")
    return rows


def _validate_capabilities(value: Any) -> dict[str, Mapping[str, Any]]:
    rows = _named_rows(
        value,
        label="capabilities",
        expected_fields={"key", "status", "provider", "version", "evidence_refs"},
        name_field="key",
    )
    for row in rows.values():
        if str(row.get("status") or "").strip() not in _CAPABILITY_STATUSES:
            raise IssueFocusContractError("issue_focus_capability_status_invalid")
        _text(row.get("provider"), "issue_focus_capability_provider")
        _text(row.get("version"), "issue_focus_capability_version")
        _refs(row.get("evidence_refs"), "issue_focus_capability_refs")
    return rows


def _validate_entities(value: Any) -> dict[str, Mapping[str, Any]]:
    rows = _named_rows(
        value,
        label="entities",
        expected_fields={
            "role",
            "target_id",
            "object_class",
            "speed_summary",
            "distance_summary",
            "evidence_refs",
        },
        name_field="role",
    )
    for role, row in rows.items():
        target_id = _text(row.get("target_id"), "issue_focus_target_id")
        if target_id.lower() in {"unknown", "none", "null", "n/a", "na", "-1"}:
            raise IssueFocusContractError("issue_focus_target_id_invalid")
        object_class = _text(row.get("object_class"), "issue_focus_object_class")
        expected_class = _ENTITY_CLASS_BY_ROLE.get(role)
        if expected_class is not None and object_class != expected_class:
            raise IssueFocusContractError("issue_focus_object_class_mismatch")
        _text(row.get("speed_summary"), "issue_focus_speed_summary")
        _text(row.get("distance_summary"), "issue_focus_distance_summary")
        _refs(row.get("evidence_refs"), "issue_focus_entity_refs")
    return rows


def _validate_measurements(value: Any) -> dict[str, Mapping[str, Any]]:
    rows = _named_rows(
        value,
        label="measurements",
        expected_fields={"key", "unit", "summary", "evidence_refs"},
        name_field="key",
    )
    for row in rows.values():
        _text(row.get("unit"), "issue_focus_measurement_unit")
        _text(row.get("summary"), "issue_focus_measurement_summary")
        _refs(row.get("evidence_refs"), "issue_focus_measurement_refs")
    return rows


def _validate_checks(value: Any) -> dict[str, Mapping[str, Any]]:
    rows = _named_rows(
        value,
        label="checks",
        expected_fields={"key", "status", "summary", "evidence_refs"},
        name_field="key",
    )
    for row in rows.values():
        if str(row.get("status") or "").strip() not in _CHECK_STATUSES:
            raise IssueFocusContractError("issue_focus_check_status_invalid")
        _text(row.get("summary"), "issue_focus_check_summary")
        _refs(row.get("evidence_refs"), "issue_focus_check_refs")
    return rows


def _validate_calculations(value: Any) -> dict[str, Mapping[str, Any]]:
    rows = _named_rows(
        value,
        label="calculations",
        expected_fields={"key", "formula", "unit", "summary", "evidence_refs"},
        name_field="key",
    )
    for row in rows.values():
        _text(row.get("formula"), "issue_focus_calculation_formula")
        _text(row.get("unit"), "issue_focus_calculation_unit")
        _text(row.get("summary"), "issue_focus_calculation_summary")
        _refs(row.get("evidence_refs"), "issue_focus_calculation_refs")
    return rows


def _missing(required: Sequence[str], observed: Mapping[str, Any], prefix: str) -> set[str]:
    return {f"{prefix}:{name}" for name in required if name not in observed}


def validate_issue_focus_evidence(
    *,
    issue_title: Any,
    value: Any,
) -> IssueFocusValidation:
    """Require complete coverage for every question derived from the title."""

    intent = resolve_issue_intent(issue_title)
    payload = _exact_mapping(
        value,
        {
            "schema_version",
            "issue_intent",
            "title_sha256",
            "analysis_status",
            "capabilities",
            "segments",
            "entities",
            "measurements",
            "checks",
            "calculations",
            "missing_requirements",
            "unsupported_capabilities",
            "stop_reason",
        },
        "issue_focus",
    )
    if payload.get("schema_version") != ISSUE_FOCUS_EVIDENCE_SCHEMA_VERSION:
        raise IssueFocusContractError("issue_focus_schema_unsupported")
    if payload.get("issue_intent") != intent.to_dict():
        raise IssueFocusContractError("issue_focus_intent_binding_mismatch")
    if payload.get("title_sha256") != issue_title_sha256(issue_title):
        raise IssueFocusContractError("issue_focus_title_binding_mismatch")

    status = str(payload.get("analysis_status") or "").strip()
    if status not in {ANALYSIS_COMPLETE, *_NON_COMPLETE_STATUSES}:
        raise IssueFocusContractError("issue_focus_analysis_status_invalid")
    capabilities = _validate_capabilities(payload.get("capabilities"))
    segments = _validate_segments(payload.get("segments"))
    entities = _validate_entities(payload.get("entities"))
    measurements = _validate_measurements(payload.get("measurements"))
    checks = _validate_checks(payload.get("checks"))
    calculations = _validate_calculations(payload.get("calculations"))

    missing_raw = payload.get("missing_requirements")
    unsupported_raw = payload.get("unsupported_capabilities")
    if not isinstance(missing_raw, list) or not isinstance(unsupported_raw, list):
        raise IssueFocusContractError("issue_focus_requirement_lists_invalid")
    missing_declared = tuple(str(item or "").strip() for item in missing_raw)
    unsupported = tuple(str(item or "").strip() for item in unsupported_raw)
    for values, code in (
        (missing_declared, "issue_focus_missing_requirements_invalid"),
        (unsupported, "issue_focus_unsupported_capabilities_invalid"),
    ):
        if (
            any(not item for item in values)
            or len(values) != len(set(values))
            or tuple(sorted(values)) != values
        ):
            raise IssueFocusContractError(code)
    if not set(unsupported).issubset(intent.required_capabilities):
        raise IssueFocusContractError("issue_focus_unsupported_capability_unrequired")
    if status != ANALYSIS_CAPABILITY_UNSUPPORTED and unsupported:
        raise IssueFocusContractError("issue_focus_unsupported_status_mismatch")
    stop_reason = str(payload.get("stop_reason") or "").strip()

    calculated_missing: set[str] = set()
    if not intent.statement_sufficient:
        calculated_missing.add("statement:problem_statement")
        if status != ANALYSIS_INSUFFICIENT_STATEMENT:
            raise IssueFocusContractError("issue_focus_insufficient_statement_required")
    else:
        calculated_missing.update(
            _missing(intent.required_capabilities, capabilities, "capability")
        )
        calculated_missing.update(_missing(intent.required_segments, segments, "segment"))
        calculated_missing.update(_missing(intent.required_entities, entities, "entity"))
        calculated_missing.update(
            _missing(intent.required_measurements, measurements, "measurement")
        )
        calculated_missing.update(_missing(intent.required_checks, checks, "check"))
        calculated_missing.update(
            _missing(intent.required_calculations, calculations, "calculation")
        )

    if status == ANALYSIS_COMPLETE:
        if not intent.statement_sufficient:
            raise IssueFocusContractError("issue_focus_complete_scope_invalid")
        if calculated_missing or missing_declared or unsupported or stop_reason:
            raise IssueFocusContractError("issue_focus_complete_requirements_missing")
        if any(row.get("status") == "inconclusive" for row in checks.values()):
            raise IssueFocusContractError("issue_focus_complete_check_inconclusive")
    else:
        if not stop_reason:
            raise IssueFocusContractError("issue_focus_stop_reason_missing")
        if status == ANALYSIS_INSUFFICIENT_STATEMENT:
            if intent.statement_sufficient:
                raise IssueFocusContractError("issue_focus_statement_is_sufficient")
            if any((capabilities, segments, entities, measurements, checks, calculations)):
                raise IssueFocusContractError("issue_focus_insufficient_analysis_must_stop")
        if status == ANALYSIS_CAPABILITY_UNSUPPORTED:
            if not intent.statement_sufficient:
                raise IssueFocusContractError("issue_focus_unsupported_scope_invalid")
            if not unsupported:
                raise IssueFocusContractError("issue_focus_unsupported_capabilities_missing")
            if any((capabilities, segments, entities, measurements, checks, calculations)):
                raise IssueFocusContractError("issue_focus_unsupported_analysis_must_stop")
            calculated_missing = {f"capability:{name}" for name in unsupported}
        if set(missing_declared) != calculated_missing:
            raise IssueFocusContractError("issue_focus_missing_requirements_mismatch")

    observed_by_prefix = {
        "capability": set(capabilities),
        "segment": set(segments),
        "entity": set(entities),
        "measurement": set(measurements),
        "check": set(checks),
        "calculation": set(calculations),
    }
    required_by_prefix = {
        "capability": set(intent.required_capabilities),
        "segment": set(intent.required_segments),
        "entity": set(intent.required_entities),
        "measurement": set(intent.required_measurements),
        "check": set(intent.required_checks),
        "calculation": set(intent.required_calculations),
    }
    unexpected = {
        f"{prefix}:{name}"
        for prefix, observed in observed_by_prefix.items()
        for name in observed - required_by_prefix[prefix]
    }
    if unexpected:
        raise IssueFocusContractError(
            "issue_focus_unexpected_requirements",
            ",".join(sorted(unexpected)),
        )

    return IssueFocusValidation(
        intent_sha256=intent.intent_sha256,
        analysis_status=status,
        attribution_allowed=status == ANALYSIS_COMPLETE,
        missing_requirements=tuple(sorted(calculated_missing)),
        unsupported_capabilities=unsupported,
    )
