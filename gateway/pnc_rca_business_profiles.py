"""Field-driven business profiles for the shared RCA control plane.

Routing is based only on official Feishu project identity and stable field
option IDs. Human-readable labels are retained for display but never decide
which evaluator or evidence contract runs.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Literal, Mapping


BUSINESS_PROFILE_REGISTRY_VERSION = "rca_business_profiles_v1"
PROJECT_FIELD_KEY = "field_052f23"

ProfileResolutionStatus = Literal[
    "matched",
    "unresolved",
    "unsupported",
    "conflict",
]


@dataclass(frozen=True)
class RcaBusinessProfile:
    profile_id: str
    profile_version: str
    project_keys: frozenset[str]
    work_item_type_keys: frozenset[str]
    project_option_ids: frozenset[str]
    data_resolver: str
    evidence_contract: str
    evaluator_scope: str
    resource_class: str
    artifact_kind: str
    artifact_namespace: str
    capability_profile: str
    capability_version: str
    execution_readiness: str

    def public_contract(self) -> dict[str, Any]:
        value = asdict(self)
        for key in (
            "project_keys",
            "work_item_type_keys",
            "project_option_ids",
        ):
            value[key] = sorted(value[key])
        value["registry_version"] = BUSINESS_PROFILE_REGISTRY_VERSION
        value["routing_field_key"] = PROJECT_FIELD_KEY
        return value


@dataclass(frozen=True)
class BusinessProfileResolution:
    status: ProfileResolutionStatus
    project_key: str
    work_item_type_key: str
    project_option_ids: tuple[str, ...]
    profile: RcaBusinessProfile | None = None
    reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "registry_version": BUSINESS_PROFILE_REGISTRY_VERSION,
            "status": self.status,
            "reason": self.reason,
            "project_key": self.project_key,
            "work_item_type_key": self.work_item_type_key,
            "routing_field_key": PROJECT_FIELD_KEY,
            "project_option_ids": list(self.project_option_ids),
        }
        if self.profile is not None:
            value.update(self.profile.public_contract())
        return value


G1Q3_PROFILE = RcaBusinessProfile(
    profile_id="g1q3",
    profile_version="g1q3_profile_v1",
    project_keys=frozenset({"t03o4q", "68ef617fb371dc80a10641f7"}),
    work_item_type_keys=frozenset({"issue"}),
    project_option_ids=frozenset({"6670325063"}),
    data_resolver="pdcl_remote_event_or_clip_v1",
    evidence_contract="g1q3_rca_evidence_v4",
    evaluator_scope="g1q3_rca_evaluator_scope_v4",
    resource_class="rca_prod",
    artifact_kind="rca_html_report_and_viz_mcap",
    artifact_namespace="rca/g1q3",
    capability_profile="g1q3_863_consumer",
    capability_version="g1q3_863_consumer_v1",
    execution_readiness="ready",
)

MDRIVE4_PROFILE = RcaBusinessProfile(
    profile_id="mdrive4",
    profile_version="mdrive4_profile_v1",
    project_keys=frozenset({"t03o4q", "68ef617fb371dc80a10641f7"}),
    work_item_type_keys=frozenset({"issue"}),
    project_option_ids=frozenset({"7019637554"}),
    data_resolver="mdrive4_recorder_mcap_reference_v1",
    evidence_contract="mdrive4_ct_evidence_v1",
    evaluator_scope="ct_evaluator_217_20260722",
    resource_class="rca_prod",
    artifact_kind="mdrive4_ct_evaluation",
    artifact_namespace="rca/mdrive4",
    capability_profile="mdrive4_ct_evaluator",
    capability_version="e550e415c673ff62b595d8a4f96f1e7a8e543a28",
    # The evaluator release is verified. The Feishu-to-recorder input adapter
    # is not yet complete, so production must publish readiness instead of
    # silently entering the G1Q3 PDCL resolver.
    execution_readiness="input_adapter_pending",
)

RCA_BUSINESS_PROFILES = (G1Q3_PROFILE, MDRIVE4_PROFILE)


def _stable_option_ids(value: Any) -> tuple[str, ...]:
    values = value if isinstance(value, list) else [value]
    found: set[str] = set()
    for item in values:
        candidate: Any = None
        if isinstance(item, Mapping):
            candidate = item.get("id")
        elif isinstance(item, (str, int)) and not isinstance(item, bool):
            candidate = item
        text = str(candidate or "").strip()
        if text:
            found.add(text)
    return tuple(sorted(found))


def project_option_ids(work_item_brief: Mapping[str, Any]) -> tuple[str, ...]:
    fields = work_item_brief.get("work_item_fields")
    key_name = "key"
    value_name = "value"
    if not isinstance(fields, list):
        fields = work_item_brief.get("fields")
        key_name = "field_key"
        value_name = "field_value"
    if not isinstance(fields, list):
        return ()
    for field in fields:
        if not isinstance(field, Mapping):
            continue
        if str(field.get(key_name) or "").strip() == PROJECT_FIELD_KEY:
            return _stable_option_ids(field.get(value_name))
    return ()


def resolve_business_profile(
    *,
    project_key: str,
    work_item_type_key: str,
    work_item_brief: Mapping[str, Any],
) -> BusinessProfileResolution:
    """Resolve exactly one profile without title, owner, or group inference."""
    project = str(project_key or "").strip()
    item_type = str(work_item_type_key or "").strip()
    option_ids = project_option_ids(work_item_brief)
    if not project or not item_type:
        return BusinessProfileResolution(
            status="unresolved",
            project_key=project,
            work_item_type_key=item_type,
            project_option_ids=option_ids,
            reason="official_project_or_work_item_type_missing",
        )
    if not option_ids:
        return BusinessProfileResolution(
            status="unresolved",
            project_key=project,
            work_item_type_key=item_type,
            project_option_ids=(),
            reason="business_project_field_missing",
        )
    matches = [
        profile
        for profile in RCA_BUSINESS_PROFILES
        if project in profile.project_keys
        and item_type in profile.work_item_type_keys
        and profile.project_option_ids.intersection(option_ids)
    ]
    if len(matches) > 1:
        return BusinessProfileResolution(
            status="conflict",
            project_key=project,
            work_item_type_key=item_type,
            project_option_ids=option_ids,
            reason="multiple_business_profiles_matched",
        )
    if not matches:
        return BusinessProfileResolution(
            status="unsupported",
            project_key=project,
            work_item_type_key=item_type,
            project_option_ids=option_ids,
            reason="project_option_not_registered",
        )
    matched = matches[0]
    if len(set(option_ids).intersection(matched.project_option_ids)) != 1 or len(option_ids) != 1:
        return BusinessProfileResolution(
            status="conflict",
            project_key=project,
            work_item_type_key=item_type,
            project_option_ids=option_ids,
            reason="business_project_field_is_not_single_valued",
        )
    return BusinessProfileResolution(
        status="matched",
        project_key=project,
        work_item_type_key=item_type,
        project_option_ids=option_ids,
        profile=matched,
        reason="stable_project_option_matched",
    )
