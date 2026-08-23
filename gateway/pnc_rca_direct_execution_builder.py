"""Build direct-safe RCA execution requests from a durable MiniStore claim.

The direct Kafka path deliberately persists only source/admission metadata.  A
complete VM request is created later, at the host-side read boundary, after a
read-only Feishu/Meegle preread has produced a typed :class:`RcaIssueContext`.
This module is the small, injectable bridge between those two contracts.  It
does not import the legacy outbox dispatcher and it never downloads data,
submits a task, or writes Feishu fields.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
import json
import re
from typing import TYPE_CHECKING, Any

from gateway.pnc_rca_admission import (
    RcaAdmissionError,
    validate_rca_admission,
    validate_rca_trigger_context,
)
from gateway.pnc_rca_mini_store import MiniOutboxClaim
from gateway.pnc_rca_schema import (
    RcaIssueContext,
    build_execution_request,
    issue_context_from_compact_text,
    to_dict,
    validate_issue_context_fields,
    validate_vm_execution_request_envelope,
)

if TYPE_CHECKING:
    from gateway.pnc_issue_context import G1Q3IssueReadResult


DIRECT_EXECUTION_BUILDER_SCHEMA_VERSION = "pnc_rca_direct_execution_builder_v1"
DIRECT_VM_ARTIFACT_PREFIX = "/mnt/tmp/"
DIRECT_CIFS_ARTIFACT_PREFIX = (
    "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/"
)
_SAFE_NAMESPACE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,255}$")


class DirectExecutionBuildError(ValueError):
    """A request cannot be built without crossing an unsafe boundary."""

    def __init__(self, code: str, detail: str = "", *, retryable: bool = False):
        self.code = str(code or "direct_execution_build_error")
        self.detail = str(detail or self.code)
        self.retryable = bool(retryable)
        super().__init__(self.detail)


class DirectExecutionEvidenceRequired(DirectExecutionBuildError):
    """The host preread is absent, incomplete, or temporarily unavailable."""

    def __init__(self, code: str, detail: str = ""):
        super().__init__(code, detail, retryable=True)


class DirectExecutionIdentityError(DirectExecutionBuildError):
    """The preread result does not belong to the Kafka admission identity."""


@dataclass(frozen=True, slots=True)
class DirectExecutionBuilderConfig:
    """Stable direct request policy; no legacy release/capacity fields."""

    artifact_root_prefix: str = DIRECT_VM_ARTIFACT_PREFIX
    artifact_cifs_prefix: str = DIRECT_CIFS_ARTIFACT_PREFIX
    allow_feishu_writeback: bool = False
    group_response_cap: str = "L1"
    translate_baseline: str = "production"
    translate_contract_path: str = ""


IssueReader = Callable[[str, str], Any]


def read_issue_context_from_host(
    project_key: str,
    work_item_id: str,
) -> G1Q3IssueReadResult:
    """Use the existing bounded, read-only host preread facade.

    Keeping this as a named default makes the production seam explicit and
    lets tests inject a deterministic reader without importing platform tools.
    """

    # Keep Feishu/fence imports out of the safe-off process.  The host reader
    # is loaded only when the explicit ``host_preread`` builder is selected.
    from gateway.pnc_issue_context import fetch_rca_issue_context_result

    return fetch_rca_issue_context_result(
        project_key=str(project_key),
        work_item_id=str(work_item_id),
    )


def canonical_direct_artifact_paths(
    submission_key: str,
    *,
    config: DirectExecutionBuilderConfig | None = None,
) -> tuple[str, str]:
    """Return the fixed VM/CIFS artifact roots for one submission key."""

    cfg = config or DirectExecutionBuilderConfig()
    if cfg.allow_feishu_writeback:
        raise DirectExecutionBuildError(
            "direct_execution_writeback_forbidden",
            "direct execution requests are read-only and cannot write Feishu",
        )
    key = str(submission_key or "").strip()
    if not _SAFE_NAMESPACE_RE.fullmatch(key):
        raise DirectExecutionBuildError(
            "direct_execution_submission_namespace_invalid",
            "submission key is not safe for an artifact namespace",
        )
    if (
        cfg.artifact_root_prefix != DIRECT_VM_ARTIFACT_PREFIX
        or cfg.artifact_cifs_prefix != DIRECT_CIFS_ARTIFACT_PREFIX
    ):
        raise DirectExecutionBuildError(
            "direct_execution_artifact_prefix_invalid",
            "direct artifact prefixes are fixed to the governed roots",
        )
    return f"{cfg.artifact_root_prefix}{key}/", f"{cfg.artifact_cifs_prefix}{key}/"


def _json_mapping(value: Any, *, code: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise DirectExecutionBuildError(code, f"{code}: expected an object")
    return value


def _claim_payload(
    claim: MiniOutboxClaim,
    candidate: Mapping[str, Any] | None = None,
) -> tuple[Mapping[str, Any], Any, Any]:
    try:
        payload = json.loads(claim.payload_json)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise DirectExecutionBuildError(
            "direct_execution_outbox_payload_invalid",
            "MiniStore outbox payload is not valid JSON",
        ) from exc
    payload = _json_mapping(payload, code="direct_execution_outbox_payload_invalid")
    if candidate is not None:
        candidate = _json_mapping(
            candidate, code="direct_execution_outbox_payload_invalid"
        )
        if json.dumps(
            candidate, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ) != json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ):
            raise DirectExecutionIdentityError(
                "direct_execution_payload_claim_mismatch",
                "dispatcher payload is not the durable MiniStore payload",
            )
    try:
        admission = validate_rca_admission(payload.get("admission"))
        trigger = validate_rca_trigger_context(payload.get("trigger_context"))
    except (RcaAdmissionError, TypeError, ValueError) as exc:
        raise DirectExecutionBuildError(
            "direct_execution_admission_invalid",
            str(exc),
        ) from exc
    event = _json_mapping(
        payload.get("normalized_event"),
        code="direct_execution_normalized_event_invalid",
    )
    refs = admission.source_refs
    expected = {
        "submission_key": admission.submission_key,
        "business_key": admission.business_key,
        "generation": admission.generation,
        "source_event_id": claim.source_event_id,
        "origin_source_id": claim.origin_source_id,
    }
    observed = {
        "submission_key": claim.submission_key,
        "business_key": claim.business_key,
        "generation": claim.generation,
        "source_event_id": claim.source_event_id,
        "origin_source_id": claim.origin_source_id,
    }
    if observed != expected:
        raise DirectExecutionIdentityError(
            "direct_execution_claim_identity_mismatch",
            "MiniStore claim identity disagrees with its admission",
        )
    if admission.trigger_kind != claim.trigger_kind:
        raise DirectExecutionIdentityError(
            "direct_execution_trigger_kind_mismatch",
            "MiniStore trigger kind disagrees with its admission",
        )
    event_fields = {
        "project_key": refs.project_key,
        "project_simple_name": refs.project_simple_name,
        "work_item_type_key": refs.work_item_type_key,
        "work_item_id": refs.work_item_id,
    }
    for field, expected_value in event_fields.items():
        if str(event.get(field) or "").strip() != str(expected_value):
            raise DirectExecutionIdentityError(
                "direct_execution_event_identity_mismatch",
                f"normalized event field {field} disagrees with admission",
            )
    if trigger.issue_url != str(event.get("issue_url") or "").strip():
        raise DirectExecutionIdentityError(
            "direct_execution_issue_url_mismatch",
            "normalized event issue URL disagrees with trigger context",
        )
    return payload, admission, event


def _profile_projection(profile: Any) -> dict[str, Any]:
    """Keep routing/readiness evidence while dropping legacy gate metadata."""

    if not isinstance(profile, Mapping):
        return {}
    allowed = {
        "status",
        "profile_id",
        "execution_readiness",
        "data_resolver",
        "evaluator_scope",
        "artifact_namespace",
        "routing_field_key",
        "registry_version",
        "project_key",
        "work_item_type_key",
    }
    return {str(key): value for key, value in profile.items() if str(key) in allowed}


def _project_direct_request(request: Mapping[str, Any]) -> dict[str, Any]:
    """Remove legacy capacity/release metadata from a standard request."""

    value = json.loads(json.dumps(request, ensure_ascii=False, sort_keys=True))
    work_item = value.get("work_item")
    if isinstance(work_item, dict) and "business_profile" in work_item:
        work_item["business_profile"] = _profile_projection(
            work_item.get("business_profile")
        )
    policy = value.get("execution_policy")
    if isinstance(policy, dict):
        policy.pop("resource_class", None)
        policy.pop("artifact_kind", None)
    toolchain = value.get("toolchain")
    if isinstance(toolchain, dict) and "business_profile" in toolchain:
        toolchain["business_profile"] = _profile_projection(
            toolchain.get("business_profile")
        )
    return value


def _context_from_reader(
    result: Any,
    *,
    project_key: str,
    work_item_id: str,
    issue_url: str,
    work_item_type: str,
) -> tuple[RcaIssueContext, str]:
    source = "injected"
    if isinstance(result, RcaIssueContext):
        context = result
    elif hasattr(result, "context_text") and hasattr(result, "source_quality"):
        source = str(getattr(result, "source", "") or "host").strip() or "host"
        context_text = str(getattr(result, "context_text", "") or "")
        if not context_text:
            blocker = getattr(result, "blocker", None) or {}
            code = str(blocker.get("kind") or "direct_execution_preread_unavailable")
            detail = str(blocker.get("message") or code)
            if blocker.get("retryable") is False:
                raise DirectExecutionBuildError(code, detail)
            raise DirectExecutionEvidenceRequired(code, detail)
        context = issue_context_from_compact_text(
            project_key=project_key,
            work_item_id=work_item_id,
            url=issue_url,
            compact_text=context_text,
            source_quality=str(getattr(result, "source_quality", "partial")),
        )
        provenance = {
            "type": "direct_host_issue_read",
            "status": getattr(result, "status", "unknown"),
            "source": source,
            "degraded": source == "mcp_auto_degraded",
        }
        errors = getattr(result, "errors", None)
        if errors:
            provenance["error_count"] = len(errors)
        context = replace(context, media_refs=[*context.media_refs, provenance])
    elif isinstance(result, Mapping):
        source = str(result.get("source") or "injected").strip() or "injected"
        if isinstance(result.get("context"), RcaIssueContext):
            context = result["context"]
        elif isinstance(result.get("context_text"), str):
            if not result["context_text"].strip():
                blocker = result.get("blocker")
                if isinstance(blocker, Mapping):
                    code = str(
                        blocker.get("kind") or "direct_execution_preread_unavailable"
                    )
                    detail = str(blocker.get("message") or code)
                    if blocker.get("retryable") is False:
                        raise DirectExecutionBuildError(code, detail)
                    raise DirectExecutionEvidenceRequired(code, detail)
                raise DirectExecutionEvidenceRequired(
                    "direct_execution_preread_unavailable",
                    "reader returned an empty context",
                )
            context = issue_context_from_compact_text(
                project_key=project_key,
                work_item_id=work_item_id,
                url=issue_url,
                compact_text=result["context_text"],
                source_quality=str(result.get("source_quality") or "partial"),
            )
        else:
            raise DirectExecutionEvidenceRequired(
                "direct_execution_preread_shape_invalid",
                "reader must return a typed context or bounded context_text",
            )
    else:
        raise DirectExecutionEvidenceRequired(
            "direct_execution_preread_shape_invalid",
            "reader returned no typed issue evidence",
        )

    if context.source_quality == "unavailable":
        raise DirectExecutionEvidenceRequired(
            "direct_execution_preread_unavailable",
            "host reader returned unavailable issue evidence",
        )

    if context.project_key and context.project_key != project_key:
        raise DirectExecutionIdentityError(
            "direct_execution_context_project_mismatch",
            "host issue context project does not match Kafka admission",
        )
    if context.work_item_id and context.work_item_id != work_item_id:
        raise DirectExecutionIdentityError(
            "direct_execution_context_work_item_mismatch",
            "host issue context work item does not match Kafka admission",
        )
    if context.work_item_type and context.work_item_type != work_item_type:
        raise DirectExecutionIdentityError(
            "direct_execution_context_type_mismatch",
            "host issue context type does not match Kafka admission",
        )
    if context.url and context.url.rstrip("/") != issue_url.rstrip("/"):
        raise DirectExecutionIdentityError(
            "direct_execution_context_url_mismatch",
            "host issue context URL does not match Kafka admission",
        )
    context = replace(
        context,
        project_key=project_key,
        work_item_id=work_item_id,
        work_item_type=work_item_type,
        url=issue_url,
    )
    context, blocker = validate_issue_context_fields(context)
    if blocker:
        code = str(blocker.get("kind") or "direct_execution_issue_fields_not_ready")
        detail = str(blocker.get("message") or code)
        if blocker.get("retryable") is False:
            raise DirectExecutionBuildError(code, detail)
        raise DirectExecutionEvidenceRequired(code, detail)
    return context, source


def build_direct_execution_request(
    payload: Mapping[str, Any],
    claim: MiniOutboxClaim,
    *,
    reader: IssueReader = read_issue_context_from_host,
    config: DirectExecutionBuilderConfig | None = None,
) -> dict[str, Any]:
    """Build and validate one direct-safe request from a MiniStore claim.

    The function is intentionally side-effect free apart from the injected
    read-only reader.  It returns a plain JSON mapping suitable for the
    dispatcher's existing freeze/identity checks.
    """

    if not isinstance(payload, Mapping):
        raise DirectExecutionBuildError("direct_execution_payload_invalid")
    cfg = config or DirectExecutionBuilderConfig()
    _, admission, event = _claim_payload(claim, payload)
    project_key = str(admission.source_refs.project_key)
    work_item_id = str(admission.source_refs.work_item_id)
    work_item_type = str(admission.source_refs.work_item_type_key)
    issue_url = str(event.get("issue_url") or "").strip()
    try:
        context_result = reader(project_key, work_item_id)
    except DirectExecutionBuildError:
        raise
    except Exception as exc:
        raise DirectExecutionEvidenceRequired(
            "direct_execution_preread_failed",
            f"{type(exc).__name__}: {exc}",
        ) from exc
    context, source = _context_from_reader(
        context_result,
        project_key=project_key,
        work_item_id=work_item_id,
        issue_url=issue_url,
        work_item_type=work_item_type,
    )
    artifact_root, artifact_cifs_root = canonical_direct_artifact_paths(
        claim.submission_key,
        config=cfg,
    )
    try:
        request = build_execution_request(
            request_kind="issue_intake",
            task_id=claim.submission_key,
            issue_context=context,
            artifact_root=artifact_root,
            artifact_cifs_root=artifact_cifs_root,
            allow_download=False,
            allow_feishu_writeback=cfg.allow_feishu_writeback,
            group_response_cap=cfg.group_response_cap,
            translate_baseline=cfg.translate_baseline,
            translate_contract_path=cfg.translate_contract_path,
            toolchain={
                "direct_execution_builder": DIRECT_EXECUTION_BUILDER_SCHEMA_VERSION,
                "host_preread_source": source,
            },
        )
    except Exception as exc:
        raise DirectExecutionBuildError(
            "direct_execution_request_build_failed",
            f"{type(exc).__name__}: {exc}",
            retryable=False,
        ) from exc
    result = _project_direct_request(to_dict(request))
    source_refs = result.get("source_refs")
    if not isinstance(source_refs, dict):
        raise DirectExecutionBuildError("direct_execution_source_refs_invalid")
    source_refs = {
        "source_kind": "kafka_workflow_event",
        "origin_source_id": claim.origin_source_id,
        "source_event_id": claim.source_event_id,
        "generation": claim.generation,
        "business_key": claim.business_key,
        "submission_key": claim.submission_key,
    }
    # The direct VM envelope deliberately has a five-field source_refs ABI.
    # Preserve transport coordinates as an audited, non-identity projection.
    toolchain = result.get("toolchain")
    if not isinstance(toolchain, dict):
        toolchain = {}
        result["toolchain"] = toolchain
    toolchain["source_coordinates"] = {
        "topic": admission.source_refs.topic,
        "partition": admission.source_refs.partition,
        "offset": admission.source_refs.offset,
    }
    source_refs = {
        "origin_source_id": source_refs["origin_source_id"],
        "source_event_id": source_refs["source_event_id"],
        "generation": source_refs["generation"],
        "business_key": source_refs["business_key"],
        "submission_key": source_refs["submission_key"],
    }
    result["source_refs"] = source_refs
    try:
        return validate_vm_execution_request_envelope(result)
    except (TypeError, ValueError) as exc:
        raise DirectExecutionBuildError(
            "direct_execution_request_envelope_invalid",
            str(exc),
        ) from exc


__all__ = [
    "DIRECT_CIFS_ARTIFACT_PREFIX",
    "DIRECT_EXECUTION_BUILDER_SCHEMA_VERSION",
    "DIRECT_VM_ARTIFACT_PREFIX",
    "DirectExecutionBuildError",
    "DirectExecutionBuilderConfig",
    "DirectExecutionEvidenceRequired",
    "DirectExecutionIdentityError",
    "build_direct_execution_request",
    "canonical_direct_artifact_paths",
    "read_issue_context_from_host",
]
