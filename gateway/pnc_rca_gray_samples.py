"""Fixed W18 regression contracts and automation authority helpers.

The S02-S10 map is deliberately a historical regression lane: each slot names
one immutable issue and expected terminal class.  It is not the general issue
selector for business refreshes; those use the exact-manifest batch lane.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any, Mapping


GRAY_SAMPLE_AUTOMATION_AUTHORITY_SCHEMA_VERSION = (
    "pnc_rca_gray_sample_automation_authority_v1"
)
GRAY_SAMPLE_AUTOMATION_AUTHORIZATION_SCHEMA_VERSION = (
    "pnc_rca_gray_sample_automation_authorization_v2"
)
GRAY_SAMPLE_REQUESTER_ID = "automation:gray-sample"
GRAY_SAMPLE_DAILY_STARTED_ATTEMPT_QUOTA: None = None
C_TOPIC_FIXTURE_SCHEMA_VERSION = "pnc_rca_c_topic_canary_fixture_v1"
C_TOPIC_CHAT_ID = "oc_6cfc782212009ff4cd815349909dd423"
C_TOPIC_ISSUE_ID = "7006868401"
C_TOPIC_TEXT = "分析 https://project.feishu.cn/t03o4q/issue/detail/7006868401"
DESIGN_AUTHORITY_PATH = (
    "/Users/songying/.hermes/workspace-work/knowledge/outputs/"
    "rca-product-endgame-and-ga-tasks-v2-20260725.md"
)
DESIGN_AUTHORITY_SHA256 = (
    "4958857f530f072b162fc6ea7378605fa6ba5fc78e0e8a0303bf4ac534071e2b"
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_OPEN_ID_RE = re.compile(r"^ou_[A-Za-z0-9_-]{3,200}$")
_RELEASE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{2,127}$")

COMMENT_BUDGET = {
    "initial_conclusion_max": 1,
    "followup_or_correction_max": 1,
    "explicit_rerun_new_comment_max": 1,
    "infrastructure_redelivery_new_comment_max": 0,
}

GRAY_SAMPLE_CONTRACTS: dict[str, dict[str, Any]] = {
    "S02": {
        "issue_id": "7049076163",
        "expected_terminal_class": "candidate_hypothesis",
        "expected_confidence_tier": "medium",
        "expected_domain": "ACC",
        "high_confidence_forbidden": True,
    },
    "S03": {
        "issue_id": "7058462331",
        "expected_terminal_class": "honest_non_attribution",
        "expected_confidence_tier": "low",
        "expected_domain": None,
        "high_confidence_forbidden": True,
    },
    "S04": {
        "issue_id": "7058503076",
        "expected_terminal_class": "honest_non_attribution",
        "expected_confidence_tier": "low",
        "expected_domain": None,
        "high_confidence_forbidden": True,
    },
    "S05": {
        "issue_id": "7048992155",
        "expected_terminal_class": "honest_non_attribution",
        "expected_confidence_tier": "low",
        "expected_domain": None,
        "high_confidence_forbidden": True,
    },
    "S06": {
        "issue_id": "7058246921",
        "expected_terminal_class": "candidate_hypothesis",
        "expected_confidence_tier": "medium",
        "expected_domain": "PERCEPTION_OBJECT",
        "high_confidence_forbidden": True,
    },
    "S07": {
        "issue_id": "7058500122",
        "expected_terminal_class": "honest_non_attribution",
        "expected_confidence_tier": "low",
        "expected_domain": None,
        "high_confidence_forbidden": True,
    },
    "S08": {
        "issue_id": "7047951928",
        "expected_terminal_class": "candidate_hypothesis",
        "expected_confidence_tier": "medium",
        "expected_domain": "PERCEPTION_LANE",
        "high_confidence_forbidden": True,
    },
    "S09": {
        "issue_id": "7058336194",
        "expected_terminal_class": "honest_non_attribution",
        "expected_confidence_tier": "low",
        "expected_domain": None,
        "high_confidence_forbidden": True,
    },
    "S10": {
        "issue_id": "7048292506",
        "expected_terminal_class": "candidate_hypothesis",
        "expected_confidence_tier": "medium",
        "expected_domain": "DNP_SPP",
        "high_confidence_forbidden": True,
    },
}


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def sample_contract(sample_id: str) -> dict[str, Any]:
    identity = str(sample_id or "").strip()
    try:
        sample = GRAY_SAMPLE_CONTRACTS[identity]
    except KeyError as exc:
        raise ValueError("gray_sample_id_invalid") from exc
    return {
        "sample_id": identity,
        **sample,
        "comment_budget": dict(COMMENT_BUDGET),
    }


def sample_contract_sha256(sample_id: str) -> str:
    return canonical_sha256(sample_contract(sample_id))


def _utc_datetime(value: Any, *, error_code: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value or ""))
    except ValueError as exc:
        raise ValueError(error_code) from exc
    if parsed.tzinfo is None:
        raise ValueError(error_code)
    return parsed.astimezone(timezone.utc)


def validate_gray_sample_automation_authorization(
    value: Mapping[str, Any] | None,
    *,
    expected_release_id: str,
    now: datetime,
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("gray_sample_authorization_required")
    required = {
        "schema_version",
        "artifact_id",
        "status",
        "issued_at",
        "not_before",
        "expires_at",
        "decision",
        "design_authority",
        "sample_sequence_authority",
        "release_id",
        "requester_id",
        "originator_identity_source",
        "lane",
        "activation_required",
        "allowed_activation_states",
        "daily_started_attempt_quota",
        "allowed_sample_ids",
        "sample_contract_sha256s",
        "comment_budget",
        "external_write_allowed",
        "owner_authorized",
        "production_actions",
    }
    if set(value) != required:
        raise ValueError("gray_sample_authorization_schema_invalid")
    release_id = str(value.get("release_id") or "").strip()
    if (
        value.get("schema_version")
        != GRAY_SAMPLE_AUTOMATION_AUTHORIZATION_SCHEMA_VERSION
        or value.get("status") != "PREPARED_UNTIL_C_TOPIC_FIXTURE"
        or value.get("decision") != 52
        or release_id != str(expected_release_id or "").strip()
        or _RELEASE_ID_RE.fullmatch(release_id) is None
        or value.get("requester_id") != GRAY_SAMPLE_REQUESTER_ID
        or value.get("lane") != "production"
        or value.get("activation_required") is not True
        or value.get("allowed_activation_states") != ["steady_active"]
        or value.get("daily_started_attempt_quota")
        is not GRAY_SAMPLE_DAILY_STARTED_ATTEMPT_QUOTA
        or value.get("allowed_sample_ids") != list(GRAY_SAMPLE_CONTRACTS)
        or value.get("sample_contract_sha256s")
        != {
            sample_id: sample_contract_sha256(sample_id)
            for sample_id in GRAY_SAMPLE_CONTRACTS
        }
        or value.get("comment_budget") != COMMENT_BUDGET
        or value.get("external_write_allowed") is not True
        or value.get("owner_authorized") is not True
    ):
        raise ValueError("gray_sample_authorization_contract_invalid")
    design = value.get("design_authority")
    if not isinstance(design, Mapping) or dict(design) != {
        "path": DESIGN_AUTHORITY_PATH,
        "section": "12",
        "decision": 52,
        "sha256": DESIGN_AUTHORITY_SHA256,
    }:
        raise ValueError("gray_sample_authorization_design_binding_invalid")
    sequence = value.get("sample_sequence_authority")
    if not isinstance(sequence, Mapping) or dict(sequence) != {
        "path": DESIGN_AUTHORITY_PATH,
        "section": "10",
        "decision": 45,
        "sha256": DESIGN_AUTHORITY_SHA256,
        "fixed_sample_ids": list(GRAY_SAMPLE_CONTRACTS),
        "tenth_sample": "NATURAL",
        "s01_policy": "encounter_only_not_preselected",
    }:
        raise ValueError("gray_sample_authorization_sequence_binding_invalid")
    originator_source = value.get("originator_identity_source")
    if not isinstance(originator_source, Mapping) or dict(originator_source) != {
        "fixture_schema_version": C_TOPIC_FIXTURE_SCHEMA_VERSION,
        "canary_id": "C-TOPIC",
        "field": "originator_identity",
        "chat_id": C_TOPIC_CHAT_ID,
        "official_readback_required": True,
    }:
        raise ValueError("gray_sample_authorization_originator_source_invalid")
    production_actions = value.get("production_actions")
    if not isinstance(production_actions, Mapping) or not production_actions or any(
        isinstance(item, bool) or item != 0 for item in production_actions.values()
    ):
        raise ValueError("gray_sample_authorization_production_actions_invalid")
    current = now.astimezone(timezone.utc) if now.tzinfo is not None else None
    if current is None:
        raise ValueError("gray_sample_authorization_now_invalid")
    issued = _utc_datetime(
        value.get("issued_at"), error_code="gray_sample_authorization_time_invalid"
    )
    not_before = _utc_datetime(
        value.get("not_before"), error_code="gray_sample_authorization_time_invalid"
    )
    expires = _utc_datetime(
        value.get("expires_at"), error_code="gray_sample_authorization_time_invalid"
    )
    if not (issued <= not_before <= current <= expires):
        raise ValueError("gray_sample_authorization_not_current")
    return dict(value)


def _normalized_sha256(value: Any, *, error_code: str) -> str:
    normalized = str(value or "").strip().lower()
    if _SHA256_RE.fullmatch(normalized) is None or normalized == "0" * 64:
        raise ValueError(error_code)
    return normalized


def normalize_gray_sample_automation_authority(
    value: Mapping[str, Any] | None,
) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise ValueError("gray_sample_automation_authority_required")
    required = {
        "schema_version",
        "release_id",
        "sample_id",
        "originator_identity",
        "originator_fixture_sha256",
        "authorization_sha256",
        "sample_contract_sha256",
    }
    if set(value) != required:
        raise ValueError("gray_sample_automation_authority_schema_invalid")
    schema_version = str(value.get("schema_version") or "").strip()
    if schema_version != GRAY_SAMPLE_AUTOMATION_AUTHORITY_SCHEMA_VERSION:
        raise ValueError("gray_sample_automation_authority_schema_invalid")
    release_id = str(value.get("release_id") or "").strip()
    if _RELEASE_ID_RE.fullmatch(release_id) is None:
        raise ValueError("gray_sample_release_id_invalid")
    sample_id = str(value.get("sample_id") or "").strip()
    contract_sha = sample_contract_sha256(sample_id)
    if str(value.get("sample_contract_sha256") or "").strip().lower() != contract_sha:
        raise ValueError("gray_sample_contract_sha256_mismatch")
    originator = str(value.get("originator_identity") or "").strip()
    if _OPEN_ID_RE.fullmatch(originator) is None:
        raise ValueError("gray_sample_originator_identity_invalid")
    return {
        "schema_version": schema_version,
        "release_id": release_id,
        "sample_id": sample_id,
        "originator_identity": originator,
        "originator_fixture_sha256": _normalized_sha256(
            value.get("originator_fixture_sha256"),
            error_code="gray_sample_originator_fixture_sha256_invalid",
        ),
        "authorization_sha256": _normalized_sha256(
            value.get("authorization_sha256"),
            error_code="gray_sample_authorization_sha256_invalid",
        ),
        "sample_contract_sha256": contract_sha,
    }


def build_gray_sample_reason(authority: Mapping[str, Any]) -> str:
    normalized = normalize_gray_sample_automation_authority(authority)
    release_sha = hashlib.sha256(normalized["release_id"].encode("utf-8")).hexdigest()
    return ":".join((
        "production_gray_sample",
        normalized["sample_id"],
        release_sha,
        normalized["originator_fixture_sha256"],
        normalized["authorization_sha256"],
        normalized["sample_contract_sha256"],
    ))


def build_gray_sample_message_id(authority: Mapping[str, Any]) -> str:
    normalized = normalize_gray_sample_automation_authority(authority)
    release_sha = hashlib.sha256(normalized["release_id"].encode("utf-8")).hexdigest()
    # Deliberately exclude fixture identity: changing the fixture for the same
    # release/sample must hit the existing source dedupe key and fail payload
    # comparison rather than minting a second generation.
    return f"gray-sample-{normalized['sample_id'].lower()}-{release_sha[:16]}"


def gray_sample_issue_url(sample_id: str) -> str:
    issue_id = str(sample_contract(sample_id)["issue_id"])
    return f"https://project.feishu.cn/t03o4q/issue/detail/{issue_id}"
