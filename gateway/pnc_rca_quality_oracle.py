"""Fail-closed structural quality tiers for production RCA publication.

The oracle deliberately ignores model confidence. Only evaluator keys present
in the producer's ``actual_evaluators`` emission, a hash-bound active inventory,
verified validation artifacts, structured evidence, and a closed causal
narrative can raise an attribution above honest non-attribution.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import math
from pathlib import Path
import re
import stat
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

MEDIUM_TIER_DISCLAIMER = "仅供参考，待确认"
BANNED_PUBLIC_PHRASES = ("请核对问题数据地址",)
GOLDEN_REGISTRY_SCHEMA_VERSION = "pnc_rca_release_golden_registry_v1"
EVALUATOR_VALIDATION_SCHEMA_VERSION = "g1q3_rca_evaluator_validation_dimensions_v1"
ACTIVE_EVALUATOR_INVENTORY_SCHEMA_VERSION = (
    "g1q3_rca_active_evaluator_inventory_v1"
)
OWNER_CONFIRMED_SOURCE_SCHEMA_VERSION = "g1q3_rca_owner_confirmed_source_v1"
VALIDATION_ARTIFACT_SCHEMA_VERSION = "g1q3_rca_validation_dimension_artifact_v1"
REQUIRED_EVALUATOR_VALIDATION_DIMENSIONS = (
    "real_positive",
    "real_negative",
    "synthetic_boundary",
)
GOLDEN_REGISTRY_PATH = (
    Path(__file__).resolve().parent
    / "assets"
    / "pnc_rca_release_golden_registry_v1.json"
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_GIT_OID_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_EVALUATOR_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,159}$")
_FLAT_EVALUATOR_KEY_RE = re.compile(r"^[a-z][a-z0-9]*(?:_[a-z0-9]+)*$")
_EVALUATOR_DOMAIN_RE = re.compile(r"^[A-Z][A-Z0-9_]{1,63}$")
_UPSTREAM_DISPATCH_SCHEMA_VERSION = "g1q3_upstream_dispatch_v2"
_UPSTREAM_DISPATCH_FIELDS = frozenset({
    "hit_evaluator_keys",
    "hit_window_envelope",
    "hit_windows",
    "owner_bucket",
    "owner_bucket_label",
    "reason",
    "schema_version",
    "terminal_classification",
})
_UPSTREAM_OWNER_BUCKET_LABELS = {
    "acc_longitudinal_control": "纵向控制",
    "aeb": "AEB",
    "fctb_fcw": "AEB_FCW",
    "hmi_sr": "HMI_SR",
    "lane_perception": "车道线感知",
    "lcc_lateral_control": "横向控制",
    "ooi_spp": "目标选择 / SPP",
    "tsr": "TSR",
    "vision_perception": "视觉感知",
}
_UPSTREAM_DISPATCH_ABSTAIN_REASONS = frozenset({
    "abstain_no_hit",
    "abstain_cross_domain",
})
_GOLDEN_HASH_FIELDS = (
    "evaluator_source_sha256",
    "positive_golden_sha256",
    "negative_golden_sha256",
    "test_receipt_sha256",
)
_GOLDEN_SOURCE_KIND_FIELD = "source_kind"
_MAX_VALIDATION_ARTIFACT_BYTES = 4 * 1024 * 1024
_REAL_CASE_SOURCE_KINDS = frozenset({
    "owner_confirmed_real_issue",
    "real_issue",
    "real_mcap_case",
})
_OWNER_GOLDEN_SOURCE_KINDS = frozenset({
    "owner_confirmed",
    "owner_confirmed_case",
    "owner_confirmed_fixture",
    "owner_confirmed_production_case",
    "owner_grounded",
    "owner_approved_fixture",
})
_MACHINE_GOLDEN_SOURCE_KINDS = frozenset({
    "machine",
    "machine_observation",
    "live_machine_observation",
    "runtime_observation",
    "observed",
    "synthetic",
    "unit_fixture",
    "decoded_observation",
})

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
    evaluator_validation_required_dimensions: tuple[str, ...]
    evaluator_validation_missing_dimensions: tuple[str, ...]
    evaluator_validation_complete: bool
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


def _normalize_golden_source_kind(value: Any) -> str:
    return _string(value).lower().replace("-", "_").replace(" ", "_")


def _golden_source_validation(item: Mapping[str, Any]) -> tuple[bool, str]:
    """Require an owner-grounded source before an entry can unlock high tier."""

    provenance = _mapping(item.get("provenance"))
    raw_kind = item.get(_GOLDEN_SOURCE_KIND_FIELD)
    if raw_kind in (None, ""):
        raw_kind = item.get("golden_source_kind")
    if raw_kind in (None, ""):
        raw_kind = provenance.get("kind")
    kind = _normalize_golden_source_kind(raw_kind)
    if not kind:
        return False, "golden_source_kind_missing"
    if kind in _MACHINE_GOLDEN_SOURCE_KINDS or (
        "machine" in kind and "observation" in kind
    ):
        return False, "golden_source_kind_machine_observation"
    if kind not in _OWNER_GOLDEN_SOURCE_KINDS:
        return False, "golden_source_kind_unqualified"
    return True, ""


def _read_bound_json_artifact(
    *,
    registry_path: Path,
    artifact_path: Any,
    artifact_sha256: Any,
    label: str,
) -> tuple[dict[str, Any], dict[str, Any], tuple[str, ...]]:
    """Read one hash-bound regular JSON artifact below the registry directory."""

    locator = _string(artifact_path)
    expected_sha256 = _string(artifact_sha256)
    errors: list[str] = []
    relative = Path(locator) if locator else Path()
    if (
        not locator
        or relative.is_absolute()
        or any(part in {"", ".", ".."} for part in relative.parts)
    ):
        return {}, {}, (f"{label}_artifact_path_invalid",)
    if _SHA256_RE.fullmatch(expected_sha256) is None:
        return {}, {}, (f"{label}_artifact_sha256_invalid",)

    root = registry_path.parent
    candidate = root.joinpath(*relative.parts)
    current = root
    try:
        for part in relative.parts:
            current = current / part
            mode = current.lstat().st_mode
            if stat.S_ISLNK(mode):
                errors.append(f"{label}_artifact_symlink_forbidden")
                break
        if not errors and not stat.S_ISREG(candidate.lstat().st_mode):
            errors.append(f"{label}_artifact_not_regular_file")
    except OSError:
        errors.append(f"{label}_artifact_unreadable")
    if errors:
        return {}, {"path": locator, "sha256": expected_sha256}, tuple(errors)

    try:
        root_resolved = root.resolve(strict=True)
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root_resolved)
        payload = candidate.read_bytes()
    except (OSError, ValueError):
        return (
            {},
            {"path": locator, "sha256": expected_sha256},
            (f"{label}_artifact_path_escape_or_unreadable",),
        )
    if not payload or len(payload) > _MAX_VALIDATION_ARTIFACT_BYTES:
        errors.append(f"{label}_artifact_size_invalid")
    observed_sha256 = hashlib.sha256(payload).hexdigest()
    if observed_sha256 != expected_sha256:
        errors.append(f"{label}_artifact_hash_mismatch")
    try:
        document = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        document = {}
        errors.append(f"{label}_artifact_json_invalid")
    if not isinstance(document, Mapping):
        document = {}
        errors.append(f"{label}_artifact_not_object")
    source = {
        "path": locator,
        "sha256": observed_sha256,
        "size_bytes": len(payload),
    }
    return dict(document), source, tuple(sorted(set(errors)))


def _validate_active_inventory_artifact(
    *,
    registry_path: Path,
    binding: Any,
    pipeline_commit: str,
    pipeline_tree: str,
    required: bool,
    externally_required_ids: Sequence[str] | None,
) -> dict[str, Any]:
    raw = binding if isinstance(binding, Mapping) else {}
    errors: list[str] = []
    if not raw:
        if required:
            errors.append("active_inventory_artifact_missing")
        return {
            "valid": not required,
            "present": False,
            "required": required,
            "evaluator_ids": (),
            "source": {},
            "errors": tuple(errors),
        }
    if set(raw) != {"artifact_path", "artifact_sha256"}:
        errors.append("active_inventory_binding_fields_invalid")
    document, source, read_errors = _read_bound_json_artifact(
        registry_path=registry_path,
        artifact_path=raw.get("artifact_path"),
        artifact_sha256=raw.get("artifact_sha256"),
        label="active_inventory",
    )
    errors.extend(read_errors)
    if set(document) != {
        "schema_version",
        "pipeline_commit",
        "pipeline_tree",
        "active_evaluator_ids",
    }:
        errors.append("active_inventory_schema_fields_invalid")
    if document.get("schema_version") != ACTIVE_EVALUATOR_INVENTORY_SCHEMA_VERSION:
        errors.append("active_inventory_schema_mismatch")
    if _string(document.get("pipeline_commit")) != pipeline_commit:
        errors.append("active_inventory_pipeline_commit_mismatch")
    if _string(document.get("pipeline_tree")) != pipeline_tree:
        errors.append("active_inventory_pipeline_tree_mismatch")
    raw_ids = document.get("active_evaluator_ids")
    ids, ids_valid, id_errors = _normalize_required_evaluator_ids(
        raw_ids,
        present=True,
    )
    errors.extend(f"active_inventory_{error}" for error in id_errors)
    if not ids_valid:
        errors.append("active_inventory_evaluator_ids_invalid")
    if any(_FLAT_EVALUATOR_KEY_RE.fullmatch(value) is None for value in ids):
        errors.append("active_inventory_evaluator_key_invalid")
    if externally_required_ids is not None:
        external_ids, external_valid, external_errors = (
            _normalize_required_evaluator_ids(
                externally_required_ids,
                present=True,
            )
        )
        errors.extend(f"active_inventory_external_{error}" for error in external_errors)
        if external_valid and tuple(ids) != tuple(external_ids):
            errors.append("active_inventory_external_binding_mismatch")
    return {
        "valid": not errors,
        "present": True,
        "required": required,
        "evaluator_ids": tuple(ids),
        "source": source,
        "errors": tuple(sorted(set(errors))),
    }


def _validate_owner_source_artifact(
    document: Mapping[str, Any],
    *,
    evaluator_id: str,
    evaluator_key: str,
    domain: str,
    source_kind: str,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    errors: list[str] = []
    if set(document) != {
        "schema_version",
        "evaluator_id",
        "evaluator_key",
        "domain",
        "source_kind",
        "case_ids",
        "owner_confirmation",
    }:
        errors.append("owner_source_schema_fields_invalid")
    if document.get("schema_version") != OWNER_CONFIRMED_SOURCE_SCHEMA_VERSION:
        errors.append("owner_source_schema_mismatch")
    if _string(document.get("evaluator_id")) != evaluator_id:
        errors.append("owner_source_evaluator_id_mismatch")
    if _string(document.get("evaluator_key")) != evaluator_key:
        errors.append("owner_source_evaluator_key_mismatch")
    if _string(document.get("domain")) != domain:
        errors.append("owner_source_domain_mismatch")
    if _normalize_golden_source_kind(document.get("source_kind")) != source_kind:
        errors.append("owner_source_kind_mismatch")
    case_ids_raw = document.get("case_ids")
    case_ids = tuple(
        _string(value)
        for value in case_ids_raw
        if _string(value)
    ) if isinstance(case_ids_raw, list) else ()
    if (
        not case_ids
        or len(case_ids) != len(case_ids_raw or ())
        or len(case_ids) != len(set(case_ids))
        or not isinstance(case_ids_raw, list)
    ):
        errors.append("owner_source_case_ids_invalid")
    confirmation = _mapping(document.get("owner_confirmation"))
    if set(confirmation) != {"status", "receipt_sha256"}:
        errors.append("owner_source_confirmation_fields_invalid")
    if confirmation.get("status") != "confirmed":
        errors.append("owner_source_confirmation_missing")
    if _SHA256_RE.fullmatch(_string(confirmation.get("receipt_sha256"))) is None:
        errors.append("owner_source_confirmation_receipt_invalid")
    return tuple(case_ids), tuple(sorted(set(errors)))


def _validate_dimension_artifact(
    document: Mapping[str, Any],
    *,
    evaluator_id: str,
    evaluator_key: str,
    domain: str,
    dimension: str,
    declared_case_count: Any,
    owner_case_ids: Sequence[str],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    errors: list[str] = []
    if set(document) != {
        "schema_version",
        "evaluator_id",
        "evaluator_key",
        "domain",
        "dimension",
        "cases",
    }:
        errors.append(f"validation_artifact_schema_fields_invalid:{dimension}")
    if document.get("schema_version") != VALIDATION_ARTIFACT_SCHEMA_VERSION:
        errors.append(f"validation_artifact_schema_mismatch:{dimension}")
    if _string(document.get("evaluator_id")) != evaluator_id:
        errors.append(f"validation_artifact_evaluator_id_mismatch:{dimension}")
    if _string(document.get("evaluator_key")) != evaluator_key:
        errors.append(f"validation_artifact_evaluator_key_mismatch:{dimension}")
    if _string(document.get("domain")) != domain:
        errors.append(f"validation_artifact_domain_mismatch:{dimension}")
    if _string(document.get("dimension")) != dimension:
        errors.append(f"validation_artifact_dimension_mismatch:{dimension}")
    cases_raw = document.get("cases")
    cases = cases_raw if isinstance(cases_raw, list) else []
    if (
        type(declared_case_count) is not int
        or declared_case_count < 1
        or declared_case_count != len(cases)
    ):
        errors.append(f"validation_artifact_case_count_mismatch:{dimension}")
    case_ids: list[str] = []
    outcomes: set[str] = set()
    expected_status = {
        "real_positive": "PASS",
        "real_negative": "FAIL",
    }.get(dimension)
    for case in cases:
        if not isinstance(case, Mapping):
            errors.append(f"validation_artifact_case_not_object:{dimension}")
            continue
        if set(case) != {
            "case_id",
            "source_kind",
            "expected_evaluator_status",
            "evaluator_observed_status",
            "result",
            "case_config_sha256",
            "evidence_sha256",
        }:
            errors.append(f"validation_artifact_case_schema_invalid:{dimension}")
        case_id = _string(case.get("case_id"))
        source_kind = _string(case.get("source_kind"))
        expected = _string(case.get("expected_evaluator_status"))
        observed = _string(case.get("evaluator_observed_status"))
        result = _string(case.get("result"))
        if not case_id:
            errors.append(f"validation_artifact_case_id_missing:{dimension}")
        else:
            case_ids.append(case_id)
        if expected not in {"PASS", "FAIL"} or observed != expected:
            errors.append(f"validation_artifact_evaluator_outcome_invalid:{dimension}")
        else:
            outcomes.add(observed)
        if result != "PASS":
            errors.append(f"validation_artifact_case_result_not_pass:{dimension}")
        for field in ("case_config_sha256", "evidence_sha256"):
            if _SHA256_RE.fullmatch(_string(case.get(field))) is None:
                errors.append(f"validation_artifact_{field}_invalid:{dimension}")
        if dimension in {"real_positive", "real_negative"}:
            if source_kind not in _REAL_CASE_SOURCE_KINDS:
                errors.append(f"validation_artifact_real_source_kind_invalid:{dimension}")
            if case_id and case_id not in owner_case_ids:
                errors.append(f"validation_artifact_case_not_owner_confirmed:{dimension}")
            if expected != expected_status:
                errors.append(f"validation_artifact_real_outcome_semantics_invalid:{dimension}")
        elif dimension == "synthetic_boundary" and source_kind != "synthetic_boundary":
            errors.append("validation_artifact_synthetic_source_kind_invalid:synthetic_boundary")
    if len(case_ids) != len(set(case_ids)):
        errors.append(f"validation_artifact_duplicate_case_id:{dimension}")
    if dimension == "synthetic_boundary" and outcomes != {"PASS", "FAIL"}:
        errors.append("validation_artifact_boundary_outcomes_incomplete:synthetic_boundary")
    return tuple(case_ids), tuple(sorted(set(errors)))


def _validate_entry_artifacts(
    item: Mapping[str, Any],
    *,
    registry_path: Path,
    validation: Mapping[str, Any],
) -> dict[str, Any]:
    evaluator_id = _string(item.get("evaluator_id"))
    evaluator_key = _string(validation.get("evaluator_key"))
    domain = _string(validation.get("domain"))
    source_kind = _normalize_golden_source_kind(item.get(_GOLDEN_SOURCE_KIND_FIELD))
    errors: list[str] = []
    sources: dict[str, Any] = {}
    source_document, source_info, source_read_errors = _read_bound_json_artifact(
        registry_path=registry_path,
        artifact_path=item.get("evaluator_source_artifact_path"),
        artifact_sha256=item.get("evaluator_source_sha256"),
        label="owner_source",
    )
    errors.extend(source_read_errors)
    owner_case_ids, source_errors = _validate_owner_source_artifact(
        source_document,
        evaluator_id=evaluator_id,
        evaluator_key=evaluator_key,
        domain=domain,
        source_kind=source_kind,
    )
    errors.extend(source_errors)
    sources["owner_source"] = source_info

    case_ids_by_dimension: dict[str, tuple[str, ...]] = {}
    dimensions = validation.get("dimensions")
    dimensions = dimensions if isinstance(dimensions, Mapping) else {}
    for dimension in REQUIRED_EVALUATOR_VALIDATION_DIMENSIONS:
        raw = dimensions.get(dimension)
        if not isinstance(raw, Mapping) or raw.get("status") != "passed":
            continue
        document, info, read_errors = _read_bound_json_artifact(
            registry_path=registry_path,
            artifact_path=raw.get("artifact_path"),
            artifact_sha256=raw.get("artifact_sha256"),
            label=f"{dimension}",
        )
        errors.extend(read_errors)
        case_ids, dimension_errors = _validate_dimension_artifact(
            document,
            evaluator_id=evaluator_id,
            evaluator_key=evaluator_key,
            domain=domain,
            dimension=dimension,
            declared_case_count=raw.get("case_count"),
            owner_case_ids=owner_case_ids,
        )
        errors.extend(dimension_errors)
        case_ids_by_dimension[dimension] = case_ids
        sources[dimension] = info
    positive_ids = set(case_ids_by_dimension.get("real_positive", ()))
    negative_ids = set(case_ids_by_dimension.get("real_negative", ()))
    if positive_ids & negative_ids:
        errors.append("real_positive_negative_case_overlap")
    return {
        "valid": not errors,
        "sources": sources,
        "errors": tuple(sorted(set(errors))),
    }


def _normalize_validation_required_dimensions(
    value: Any,
) -> tuple[tuple[str, ...], bool, tuple[str, ...]]:
    if not isinstance(value, (list, tuple)):
        return (), False, ("required_dimensions_not_list",)
    normalized: list[str] = []
    errors: list[str] = []
    seen: set[str] = set()
    allowed = set(REQUIRED_EVALUATOR_VALIDATION_DIMENSIONS)
    for item in value:
        dimension = _string(item)
        if dimension not in allowed:
            errors.append("required_dimension_invalid")
            continue
        if dimension in seen:
            errors.append("required_dimensions_duplicate")
            continue
        seen.add(dimension)
        normalized.append(dimension)
    if set(normalized) != allowed:
        errors.append("required_dimensions_exact_set_mismatch")
    elif tuple(normalized) != REQUIRED_EVALUATOR_VALIDATION_DIMENSIONS:
        errors.append("required_dimensions_order_invalid")
    return (
        tuple(dimension for dimension in REQUIRED_EVALUATOR_VALIDATION_DIMENSIONS if dimension in seen),
        not errors,
        tuple(sorted(set(errors))),
    )


def _validation_dimension_status(
    item: Mapping[str, Any],
    required_dimensions: Sequence[str],
) -> dict[str, Any]:
    evaluator_id = _string(item.get("evaluator_id"))
    evaluator_key = _string(item.get("evaluator_key"))
    domain = _string(item.get("domain"))
    errors: list[str] = []
    entry_required, entry_required_valid, entry_required_errors = (
        _normalize_validation_required_dimensions(item.get("required_dimensions"))
    )
    errors.extend(entry_required_errors)
    if entry_required_valid and tuple(entry_required) != tuple(required_dimensions):
        errors.append("entry_required_dimensions_mismatch")
    if _FLAT_EVALUATOR_KEY_RE.fullmatch(evaluator_key) is None:
        errors.append("evaluator_key_not_flat_snake_case")
    if evaluator_key != evaluator_id:
        errors.append("evaluator_id_key_mapping_mismatch")
    if _EVALUATOR_DOMAIN_RE.fullmatch(domain) is None:
        errors.append("evaluator_domain_invalid")

    dimensions_raw = item.get("dimensions")
    dimensions = dimensions_raw if isinstance(dimensions_raw, Mapping) else {}
    if not isinstance(dimensions_raw, Mapping):
        errors.append("dimensions_not_object")
    unexpected = set(str(value) for value in dimensions) - set(required_dimensions)
    if unexpected:
        errors.append("validation_dimension_unexpected")

    passed: set[str] = set()
    normalized_dimensions: dict[str, dict[str, Any]] = {}
    hash_field_by_dimension = {
        "real_positive": "positive_golden_sha256",
        "real_negative": "negative_golden_sha256",
        "synthetic_boundary": "test_receipt_sha256",
    }
    for dimension in required_dimensions:
        raw = dimensions.get(dimension)
        if raw is None:
            continue
        if not isinstance(raw, Mapping):
            errors.append(f"validation_dimension_not_object:{dimension}")
            continue
        if set(raw) != {"status", "case_count", "artifact_sha256", "artifact_path"}:
            errors.append(f"validation_dimension_schema_fields_invalid:{dimension}")
        status = _string(raw.get("status"))
        case_count = raw.get("case_count")
        artifact_sha256 = _string(raw.get("artifact_sha256"))
        if status not in {"passed", "pending", "failing"}:
            errors.append(f"validation_dimension_status_invalid:{dimension}")
        if type(case_count) is not int or int(case_count) < 0:
            errors.append(f"validation_dimension_case_count_invalid:{dimension}")
        if artifact_sha256 and _SHA256_RE.fullmatch(artifact_sha256) is None:
            errors.append(f"validation_dimension_artifact_sha256_invalid:{dimension}")
        if status == "passed":
            if type(case_count) is not int or int(case_count) < 1:
                errors.append(f"validation_dimension_passed_without_case:{dimension}")
            if _SHA256_RE.fullmatch(artifact_sha256) is None:
                errors.append(f"validation_dimension_passed_without_artifact:{dimension}")
            expected_hash = _string(item.get(hash_field_by_dimension[dimension]))
            if artifact_sha256 != expected_hash:
                errors.append(f"validation_dimension_hash_binding_mismatch:{dimension}")
            if not any(error.endswith(f":{dimension}") for error in errors):
                passed.add(dimension)
        normalized_dimensions[dimension] = {
            "status": status,
            "case_count": case_count,
            "artifact_sha256": artifact_sha256,
            "artifact_path": _string(raw.get("artifact_path")),
        }

    calculated_missing = tuple(
        dimension for dimension in required_dimensions if dimension not in passed
    )
    declared_missing_raw = item.get("missing_dimensions")
    declared_missing, declared_valid, declared_errors = (
        _normalize_declared_missing_dimensions(
            declared_missing_raw,
            required_dimensions=required_dimensions,
        )
    )
    errors.extend(declared_errors)
    if declared_valid and declared_missing != calculated_missing:
        errors.append("missing_dimensions_accounting_mismatch")
    calculated_fully_validated = not calculated_missing and not errors
    if type(item.get("fully_validated")) is not bool:
        errors.append("fully_validated_not_bool")
    elif item.get("fully_validated") is not calculated_fully_validated:
        errors.append("fully_validated_accounting_mismatch")
    status = _string(item.get("status"))
    if calculated_fully_validated and status != "passed":
        errors.append("fully_validated_status_not_passed")
    if calculated_missing and status not in {"pending", "failing"}:
        errors.append("incomplete_validation_status_invalid")
    return {
        "valid": not errors,
        "evaluator_key": evaluator_key,
        "domain": domain,
        "required_dimensions": tuple(entry_required),
        "dimensions": normalized_dimensions,
        "fully_validated": bool(not calculated_missing and not errors),
        "missing_dimensions": calculated_missing,
        "errors": tuple(sorted(set(errors))),
    }


def _normalize_declared_missing_dimensions(
    value: Any,
    *,
    required_dimensions: Sequence[str],
) -> tuple[tuple[str, ...], bool, tuple[str, ...]]:
    if not isinstance(value, (list, tuple)):
        return (), False, ("missing_dimensions_not_list",)
    allowed = set(required_dimensions)
    seen: set[str] = set()
    errors: list[str] = []
    for item in value:
        dimension = _string(item)
        if dimension not in allowed:
            errors.append("missing_dimension_invalid")
            continue
        if dimension in seen:
            errors.append("missing_dimensions_duplicate")
            continue
        seen.add(dimension)
    normalized = tuple(dimension for dimension in required_dimensions if dimension in seen)
    return normalized, not errors, tuple(sorted(set(errors)))


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
        "validation_schema_version": "",
        "required_dimensions": REQUIRED_EVALUATOR_VALIDATION_DIMENSIONS,
        "evaluators": {},
        "fully_validated_evaluators": {},
        "fully_validated_evaluator_ids": (),
        "incomplete_evaluator_ids": (),
        "missing_dimensions_by_evaluator": {},
        "invalid_validation_evaluator_ids": (),
        "invalid_validation_artifact_evaluator_ids": (),
        "validation_artifact_errors_by_evaluator": {},
        "inactive_validation_evaluator_ids": (),
        "required_evaluator_ids": (),
        "required_evaluator_ids_present": False,
        "inventory_binding_valid": False,
        "active_inventory_binding_valid": False,
        "active_inventory_evaluator_ids": (),
        "active_inventory_source": {},
        "active_inventory_errors": (),
        "missing_required_evaluator_ids": (),
        "unexpected_evaluator_ids": (),
        "inventory_binding_errors": (),
        "invalid_evaluator_ids": (),
        "duplicate_evaluator_ids": (),
        "non_distinct_evaluator_ids": (),
        "invalid_golden_source_ids": (),
        "machine_observation_evaluator_ids": (),
        "golden_scope_evaluator_ids": (),
        "golden_scope_explicit": False,
        "golden_scope_errors": (),
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


def _normalize_golden_scope_ids(value: Any) -> tuple[tuple[str, ...], bool, tuple[str, ...]]:
    """Normalize the explicit high-confidence scope; an empty scope is valid."""

    if not isinstance(value, (list, tuple)):
        return (), False, ("golden_scope_evaluator_ids_not_list",)
    normalized: list[str] = []
    seen: set[str] = set()
    errors: list[str] = []
    for item in value:
        evaluator_id = _string(item)
        if _EVALUATOR_ID_RE.fullmatch(evaluator_id) is None:
            errors.append("golden_scope_evaluator_id_invalid")
            continue
        if evaluator_id in seen:
            errors.append("golden_scope_evaluator_ids_duplicate")
            continue
        seen.add(evaluator_id)
        normalized.append(evaluator_id)
    return tuple(sorted(normalized)), not errors, tuple(sorted(set(errors)))


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
    validation_schema_version = _string(payload.get("validation_schema_version"))
    required_dimensions, required_dimensions_valid, required_dimension_errors = (
        _normalize_validation_required_dimensions(payload.get("required_dimensions"))
    )
    base_valid = (
        payload.get("schema_version") == GOLDEN_REGISTRY_SCHEMA_VERSION
        and validation_schema_version == EVALUATOR_VALIDATION_SCHEMA_VERSION
        and required_dimensions_valid
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
    entry_list = entries if isinstance(entries, list) else []
    active_inventory = _validate_active_inventory_artifact(
        registry_path=path,
        binding=payload.get("active_inventory_artifact"),
        pipeline_commit=commit,
        pipeline_tree=tree,
        required=bool(entry_list),
        externally_required_ids=required_evaluator_ids,
    )
    if not active_inventory["valid"]:
        valid = False
    active_evaluator_ids = set(active_inventory["evaluator_ids"])
    covered: set[str] = set()
    normalized: dict[str, dict[str, Any]] = {}
    invalid_ids: list[str] = []
    duplicate_ids: list[str] = []
    non_distinct_ids: list[str] = []
    source_invalid_ids: list[str] = []
    machine_observation_ids: list[str] = []
    invalid_validation_ids: list[str] = []
    invalid_validation_artifact_ids: list[str] = []
    validation_artifact_errors_by_evaluator: dict[str, tuple[str, ...]] = {}
    inactive_validation_ids: list[str] = []
    incomplete_ids: list[str] = []
    missing_dimensions_by_evaluator: dict[str, tuple[str, ...]] = {}
    fully_validated: dict[str, dict[str, Any]] = {}
    seen_ids: set[str] = set()
    for item in entry_list:
        if not isinstance(item, Mapping):
            valid = False
            continue
        evaluator_id = _string(item.get("evaluator_id"))
        source, positive, negative, receipt = tuple(
            _string(item.get(field)) for field in _GOLDEN_HASH_FIELDS
        )
        if _EVALUATOR_ID_RE.fullmatch(evaluator_id) is None:
            invalid_ids.append(evaluator_id)
        elif evaluator_id in seen_ids:
            duplicate_ids.append(evaluator_id)
        seen_ids.add(evaluator_id)
        hashes = (source, positive, negative, receipt)
        hashes_valid = all(_SHA256_RE.fullmatch(value) is not None for value in hashes)
        hashes_distinct = hashes_valid and len(set(hashes)) == len(hashes)
        if hashes_valid and not hashes_distinct:
            non_distinct_ids.append(evaluator_id)
        source_valid, source_error = _golden_source_validation(item)
        if not source_valid:
            source_invalid_ids.append(evaluator_id)
            if source_error == "golden_source_kind_machine_observation":
                machine_observation_ids.append(evaluator_id)
        if (
            _EVALUATOR_ID_RE.fullmatch(evaluator_id) is None
            or evaluator_id in covered
            or item.get("status") not in {"passed", "pending", "failing"}
            or _SHA256_RE.fullmatch(source) is None
            or _SHA256_RE.fullmatch(positive) is None
            or _SHA256_RE.fullmatch(negative) is None
            or _SHA256_RE.fullmatch(receipt) is None
            or not hashes_distinct
            or not source_valid
        ):
            valid = False
            continue
        validation = _validation_dimension_status(item, required_dimensions)
        if not validation["valid"]:
            invalid_validation_ids.append(evaluator_id)
            valid = False
            continue
        if active_inventory["valid"] and evaluator_id not in active_evaluator_ids:
            inactive_validation_ids.append(evaluator_id)
            valid = False
            continue
        artifact_validation = _validate_entry_artifacts(
            item,
            registry_path=path,
            validation=validation,
        )
        if not artifact_validation["valid"]:
            invalid_validation_artifact_ids.append(evaluator_id)
            validation_artifact_errors_by_evaluator[evaluator_id] = (
                artifact_validation["errors"]
            )
            valid = False
            continue
        normalized_item = dict(item)
        normalized_item["evaluator_key"] = validation["evaluator_key"]
        normalized_item["domain"] = validation["domain"]
        normalized_item["required_dimensions"] = list(
            validation["required_dimensions"]
        )
        normalized_item["dimensions"] = validation["dimensions"]
        normalized_item["fully_validated"] = validation["fully_validated"]
        normalized_item["missing_dimensions"] = list(validation["missing_dimensions"])
        normalized_item["verified_artifacts"] = artifact_validation["sources"]
        normalized[evaluator_id] = normalized_item
        covered.add(evaluator_id)
        if validation["fully_validated"]:
            fully_validated[evaluator_id] = normalized_item
        else:
            incomplete_ids.append(evaluator_id)
            missing_dimensions_by_evaluator[evaluator_id] = validation[
                "missing_dimensions"
            ]
    scope_present = "golden_scope_evaluator_ids" in payload
    scope_raw = payload.get("golden_scope_evaluator_ids")
    if scope_present:
        golden_scope, scope_valid, scope_errors = _normalize_golden_scope_ids(scope_raw)
        if set(golden_scope) != set(fully_validated):
            scope_errors = tuple(sorted({*scope_errors, "golden_scope_evaluator_set_mismatch"}))
            scope_valid = False
        if not scope_valid:
            valid = False
    else:
        # The entry list is itself an explicit scope for v1 registries that do
        # not carry the optional declaration. No active-inventory coverage is
        # inferred from this fallback.
        golden_scope = tuple(sorted(fully_validated))
        scope_errors = ()
    payload_required_present = "required_evaluator_ids" in payload
    payload_required = payload.get("required_evaluator_ids")
    payload_binding = validate_golden_registry_inventory(
        payload_required,
        tuple(sorted(fully_validated)),
        present=payload_required_present,
    )
    external_binding = validate_golden_registry_inventory(
        required_evaluator_ids,
        tuple(sorted(fully_validated)),
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
    if (
        not entry_shape_valid
        or not valid
        or binding_errors
        or required_dimension_errors
        or invalid_validation_ids
        or invalid_validation_artifact_ids
        or inactive_validation_ids
        or not active_inventory["valid"]
    ):
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
        "validation_schema_version": validation_schema_version,
        "required_dimensions": required_dimensions,
        "low_tier_golden_ready": low_tier_ready,
        "evaluators": normalized,
        "fully_validated_evaluators": fully_validated,
        "fully_validated_evaluator_ids": tuple(sorted(fully_validated)),
        "incomplete_evaluator_ids": tuple(sorted(incomplete_ids)),
        "missing_dimensions_by_evaluator": {
            key: tuple(value)
            for key, value in sorted(missing_dimensions_by_evaluator.items())
        },
        "invalid_validation_evaluator_ids": tuple(sorted(set(invalid_validation_ids))),
        "invalid_validation_artifact_evaluator_ids": tuple(
            sorted(set(invalid_validation_artifact_ids))
        ),
        "validation_artifact_errors_by_evaluator": {
            key: tuple(value)
            for key, value in sorted(validation_artifact_errors_by_evaluator.items())
        },
        "inactive_validation_evaluator_ids": tuple(
            sorted(set(inactive_validation_ids))
        ),
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
        "active_inventory_binding_valid": bool(
            active_inventory["present"] and active_inventory["valid"]
        ),
        "active_inventory_evaluator_ids": tuple(active_inventory["evaluator_ids"]),
        "active_inventory_source": active_inventory["source"],
        "active_inventory_errors": tuple(active_inventory["errors"]),
        "invalid_evaluator_ids": tuple(sorted(set(invalid_ids))),
        "duplicate_evaluator_ids": tuple(sorted(set(duplicate_ids))),
        "non_distinct_evaluator_ids": tuple(sorted(set(non_distinct_ids))),
        "invalid_golden_source_ids": tuple(sorted(set(source_invalid_ids))),
        "machine_observation_evaluator_ids": tuple(
            sorted(set(machine_observation_ids))
        ),
        "golden_scope_evaluator_ids": golden_scope,
        "golden_scope_explicit": scope_present,
        "golden_scope_errors": tuple(
            sorted(set(scope_errors) | set(required_dimension_errors))
        ),
    }


def _golden_coverage(
    evaluator_keys: Sequence[str],
    registry_status: Mapping[str, Any] | None = None,
) -> tuple[tuple[str, ...], bool, bool, bool, bool, tuple[str, ...]]:
    registry = (
        dict(registry_status)
        if isinstance(registry_status, Mapping)
        else release_golden_registry_status()
    )
    fully_registry = registry.get("fully_validated_evaluators")
    covered = (
        set(fully_registry)
        if isinstance(fully_registry, Mapping)
        else set()
    )
    required = set(evaluator_keys)
    valid = registry.get("valid") is True
    active_inventory_bound = registry.get("active_inventory_binding_valid") is True
    complete = (
        bool(required)
        and valid
        and active_inventory_bound
        and required.issubset(covered)
    )
    required_dimensions = tuple(
        registry.get("required_dimensions") or REQUIRED_EVALUATOR_VALIDATION_DIMENSIONS
    )
    missing_by_key = registry.get("missing_dimensions_by_evaluator")
    missing: list[str] = []
    for key in sorted(required - covered):
        declared = (
            missing_by_key.get(key)
            if isinstance(missing_by_key, Mapping)
            else None
        )
        dimensions = tuple(
            str(value) for value in declared
            if str(value) in required_dimensions
        ) if isinstance(declared, (list, tuple)) else required_dimensions
        missing.extend(f"{key}:{dimension}" for dimension in dimensions)
    return (
        tuple(sorted(covered)),
        registry.get("present") is True,
        valid,
        registry.get("low_tier_golden_ready") is True,
        complete,
        tuple(sorted(missing)),
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


def _upstream_dispatch_state(contract: Mapping[str, Any]) -> str:
    """Return the sealed W1b publication state without inferring a direction.

    A missing W1b result is a legacy report and therefore cannot produce a
    direction.  Malformed W1b material is kept separate so the oracle can fail
    closed instead of silently treating it as an ordinary abstention.
    """

    raw = contract.get("upstream_dispatch")
    if raw is None:
        return "legacy"
    if not isinstance(raw, Mapping) or set(raw) != _UPSTREAM_DISPATCH_FIELDS:
        return "invalid"
    if raw.get("schema_version") != _UPSTREAM_DISPATCH_SCHEMA_VERSION:
        return "invalid"
    terminal = _text(raw.get("terminal_classification"))
    reason = _text(raw.get("reason"))
    owner_bucket = raw.get("owner_bucket")
    owner_bucket_label = raw.get("owner_bucket_label")
    keys = raw.get("hit_evaluator_keys")
    windows = raw.get("hit_windows")
    if (
        not isinstance(keys, list)
        or not isinstance(windows, list)
        or any(not _FLAT_EVALUATOR_KEY_RE.fullmatch(_text(key)) for key in keys)
        or keys != sorted(set(keys))
    ):
        return "invalid"
    if terminal == "out_of_scope":
        if (
            reason != "out_of_scope"
            or owner_bucket is not None
            or owner_bucket_label is not None
            or keys
            or windows
        ):
            return "invalid"
        return "out_of_scope"
    if terminal == "valid_dispatch":
        if (
            reason != "single_owner_bucket_hit"
            or owner_bucket not in _UPSTREAM_OWNER_BUCKET_LABELS
            or owner_bucket_label
            != _UPSTREAM_OWNER_BUCKET_LABELS.get(owner_bucket)
            or not keys
        ):
            return "invalid"
        return "valid_dispatch"
    if terminal != "abstain" or reason not in _UPSTREAM_DISPATCH_ABSTAIN_REASONS:
        return "invalid"
    if owner_bucket is not None or owner_bucket_label is not None:
        return "invalid"
    if reason == "abstain_no_hit" and keys:
        return "invalid"
    if reason == "abstain_cross_domain" and not keys:
        return "invalid"
    return "abstain"


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
    golden_registry_status: Mapping[str, Any] | None = None,
) -> TierOracleResult:
    """Recompute one of five terminal classes from production structure."""

    material = contract if isinstance(contract, Mapping) else {}
    upstream_dispatch_state = _upstream_dispatch_state(material)
    public_parts = _public_parts(material)
    combined_public = "\n".join(public_parts)
    rendered = _text(publication_text)
    dispatch_abstains = upstream_dispatch_state in {"abstain", "out_of_scope"}
    classification_text = (
        rendered
        if dispatch_abstains and rendered
        else (
            (
                "本单不在自动分析范围\n仅供参考，待确认"
                if upstream_dispatch_state == "out_of_scope"
                else "本单未能定向\n仅供参考，待确认\n未发现已知异常模式"
            )
            if dispatch_abstains
            else f"{combined_public}\n{rendered}"
        )
    )
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
        golden_missing_dimensions,
    ) = _golden_coverage(evaluator_keys, golden_registry_status)
    refs = _evidence_refs(material)
    evidence = _structural_evidence(material, refs)
    roles, causal_closed = _causal_roles(material)
    responsibility = _responsibility(material)
    if dispatch_abstains:
        # W1b is the sole source of the outward direction.  Internal report
        # candidates remain available in the sealed report but cannot leak
        # through the low-tier publication checks.
        conclusion = (
            "本单不在自动分析范围"
            if upstream_dispatch_state == "out_of_scope"
            else "本单未能定向"
        )
        explicit_non_attribution = True
        candidate_wording = False
        responsibility = ""

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
    if upstream_dispatch_state == "invalid":
        violations.append("upstream_dispatch_contract_invalid")
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
        if candidate_wording:
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
        evaluator_validation_required_dimensions=REQUIRED_EVALUATOR_VALIDATION_DIMENSIONS,
        evaluator_validation_missing_dimensions=golden_missing_dimensions,
        evaluator_validation_complete=not golden_missing_dimensions,
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
    if (
        "本单未能定向" in text
        or "本单不在自动分析范围" in text
        or "责任模块：暂无法判断" in text
        or "未形成可确认的归因结论" in text
    ):
        return HONEST_NON_ATTRIBUTION
    if MEDIUM_TIER_DISCLAIMER in text:
        return CANDIDATE_HYPOTHESIS
    return SUPPORTED_ATTRIBUTION
