"""Exact stage-lineage contract for the remote-read G1Q3 RCA pipeline."""

from __future__ import annotations

from datetime import datetime
import hashlib
import json
from pathlib import PurePosixPath
import re
from typing import Any, Mapping, Sequence


RCA_STAGE_LINEAGE_SCHEMA_VERSION = "g1q3_rca_stage_lineage_v1"
RCA_STAGE_LINEAGE_RELATIVE_DIR = "stage_lineage"
RCA_STAGE_NAME_BY_SHORT = {
    "s3a": "s3a_materialize",
    "s3b": "s3b_translate",
    "s45": "s45_auto_keyframe",
    "s5": "s5_alignment",
    "s6": "s6_report",
}
RCA_STAGE_SHORT_NAMES = tuple(RCA_STAGE_NAME_BY_SHORT)
RCA_STAGE_EXECUTION_POLICY = {
    "allow_download": False,
    "input_materialization": "forbidden",
    "mdi_download_attempted": False,
    "fallback_used": False,
}
MAX_STAGE_ARTIFACTS = 128
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ARTIFACT_KIND_RE = re.compile(r"^[a-z][a-z0-9_.-]{0,95}$")


class StageLineageError(ValueError):
    """One stage receipt is malformed or breaks the input/output hash chain."""

    def __init__(self, code: str, detail: str = ""):
        self.code = code
        self.detail = detail or code
        super().__init__(self.detail)


def _canonical_json(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise StageLineageError("stage_lineage_json_invalid") from exc


def canonical_artifact_set_sha256(artifacts: Sequence[Mapping[str, Any]]) -> str:
    """Hash one ordered, already-normalized artifact set."""
    return hashlib.sha256(_canonical_json(list(artifacts))).hexdigest()


def stage_lineage_relative_path(short_name: str) -> str:
    try:
        stage = RCA_STAGE_NAME_BY_SHORT[short_name]
    except KeyError as exc:
        raise StageLineageError("stage_lineage_stage_invalid", short_name) from exc
    return f"{RCA_STAGE_LINEAGE_RELATIVE_DIR}/{stage}.json"


def _required_text(value: Any, *, code: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise StageLineageError(code)
    return value


def _sha256(value: Any, *, code: str) -> str:
    digest = str(value or "").strip().lower()
    if not _SHA256_RE.fullmatch(digest):
        raise StageLineageError(code)
    return digest


def _artifact_path(value: Any, *, artifact_root: str) -> str:
    path = _required_text(value, code="stage_lineage_artifact_path_invalid")
    root = PurePosixPath(artifact_root)
    candidate = PurePosixPath(path)
    if (
        not root.is_absolute()
        or not candidate.is_absolute()
        or str(root) != artifact_root.rstrip("/")
        or str(candidate) != path
        or candidate == root
        or ".." in candidate.parts
        or "\\" in path
        or "\x00" in path
    ):
        raise StageLineageError("stage_lineage_artifact_path_invalid")
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise StageLineageError("stage_lineage_artifact_path_invalid") from exc
    return path


def normalize_stage_artifact(
    value: Any,
    *,
    artifact_root: str,
) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {
        "kind",
        "path",
        "bytes",
        "sha256",
    }:
        raise StageLineageError("stage_lineage_artifact_shape_invalid")
    kind = _required_text(value.get("kind"), code="stage_lineage_artifact_kind_invalid")
    if not _ARTIFACT_KIND_RE.fullmatch(kind):
        raise StageLineageError("stage_lineage_artifact_kind_invalid")
    byte_count = value.get("bytes")
    if (
        isinstance(byte_count, bool)
        or not isinstance(byte_count, int)
        or byte_count < 1
    ):
        raise StageLineageError("stage_lineage_artifact_bytes_invalid")
    return {
        "kind": kind,
        "path": _artifact_path(value.get("path"), artifact_root=artifact_root),
        "bytes": byte_count,
        "sha256": _sha256(
            value.get("sha256"), code="stage_lineage_artifact_hash_invalid"
        ),
    }


def normalize_stage_artifacts(
    value: Any,
    *,
    artifact_root: str,
) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value or len(value) > MAX_STAGE_ARTIFACTS:
        raise StageLineageError("stage_lineage_artifact_set_invalid")
    normalized = [
        normalize_stage_artifact(item, artifact_root=artifact_root) for item in value
    ]
    identities = [(item["kind"], item["path"]) for item in normalized]
    if len(set(identities)) != len(identities):
        raise StageLineageError("stage_lineage_artifact_duplicate")
    return normalized


def validate_stage_lineage_receipt(
    value: Any,
    *,
    expected_stage: str,
    artifact_root: str,
) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {
        "schema_version",
        "status",
        "stage",
        "finished_at",
        "identity",
        "upstream_output_artifact_set_sha256",
        "input_artifacts",
        "input_artifact_set_sha256",
        "output_artifacts",
        "output_artifact_set_sha256",
        "execution_policy",
    }:
        raise StageLineageError("stage_lineage_receipt_shape_invalid")
    if (
        expected_stage not in RCA_STAGE_NAME_BY_SHORT.values()
        or value.get("schema_version") != RCA_STAGE_LINEAGE_SCHEMA_VERSION
        or value.get("status") != "completed"
        or value.get("stage") != expected_stage
    ):
        raise StageLineageError("stage_lineage_stage_invalid")
    finished_at = _required_text(
        value.get("finished_at"), code="stage_lineage_finished_at_invalid"
    )
    try:
        parsed_finished_at = datetime.fromisoformat(finished_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise StageLineageError("stage_lineage_finished_at_invalid") from exc
    if parsed_finished_at.tzinfo is None:
        raise StageLineageError("stage_lineage_finished_at_invalid")

    identity = value.get("identity")
    if not isinstance(identity, Mapping) or set(identity) != {
        "task_id",
        "submission_key",
        "run_id",
        "artifact_set_id",
        "request_sha256",
        "rca_contract_sha256",
    }:
        raise StageLineageError("stage_lineage_identity_shape_invalid")
    normalized_identity = {
        field: _required_text(
            identity.get(field), code="stage_lineage_identity_invalid"
        )
        for field in ("task_id", "submission_key", "run_id", "artifact_set_id")
    }
    normalized_identity["request_sha256"] = _sha256(
        identity.get("request_sha256"), code="stage_lineage_identity_invalid"
    )
    normalized_identity["rca_contract_sha256"] = _sha256(
        identity.get("rca_contract_sha256"), code="stage_lineage_identity_invalid"
    )

    inputs = normalize_stage_artifacts(
        value.get("input_artifacts"), artifact_root=artifact_root
    )
    outputs = normalize_stage_artifacts(
        value.get("output_artifacts"), artifact_root=artifact_root
    )
    input_set_sha256 = _sha256(
        value.get("input_artifact_set_sha256"),
        code="stage_lineage_artifact_set_hash_invalid",
    )
    output_set_sha256 = _sha256(
        value.get("output_artifact_set_sha256"),
        code="stage_lineage_artifact_set_hash_invalid",
    )
    if input_set_sha256 != canonical_artifact_set_sha256(
        inputs
    ) or output_set_sha256 != canonical_artifact_set_sha256(outputs):
        raise StageLineageError("stage_lineage_artifact_set_hash_mismatch")
    upstream_sha256 = _sha256(
        value.get("upstream_output_artifact_set_sha256"),
        code="stage_lineage_upstream_hash_invalid",
    )
    if value.get("execution_policy") != RCA_STAGE_EXECUTION_POLICY:
        raise StageLineageError("stage_lineage_execution_policy_invalid")
    return {
        "schema_version": RCA_STAGE_LINEAGE_SCHEMA_VERSION,
        "status": "completed",
        "stage": expected_stage,
        "finished_at": finished_at,
        "identity": normalized_identity,
        "upstream_output_artifact_set_sha256": upstream_sha256,
        "input_artifacts": inputs,
        "input_artifact_set_sha256": input_set_sha256,
        "output_artifacts": outputs,
        "output_artifact_set_sha256": output_set_sha256,
        "execution_policy": dict(RCA_STAGE_EXECUTION_POLICY),
    }


def validate_stage_lineage_chain(
    value: Any,
    *,
    artifact_root: str,
    expected_identity: Mapping[str, Any],
    expected_finished_at: Mapping[str, str],
    remote_stream_cache: Mapping[str, Any],
    required_final_outputs: Sequence[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    if not isinstance(value, Mapping) or set(value) != set(RCA_STAGE_SHORT_NAMES):
        raise StageLineageError("stage_lineage_chain_shape_invalid")
    expected_identity_dict = dict(expected_identity)
    if set(expected_identity_dict) != {
        "task_id",
        "submission_key",
        "run_id",
        "artifact_set_id",
        "request_sha256",
        "rca_contract_sha256",
    }:
        raise StageLineageError("stage_lineage_expected_identity_invalid")
    remote_artifact = normalize_stage_artifact(
        remote_stream_cache, artifact_root=artifact_root
    )
    final_outputs = [
        normalize_stage_artifact(item, artifact_root=artifact_root)
        for item in required_final_outputs
    ]

    normalized: dict[str, dict[str, Any]] = {}
    previous_outputs = [remote_artifact]
    previous_output_sha256 = canonical_artifact_set_sha256(previous_outputs)
    for short_name in RCA_STAGE_SHORT_NAMES:
        stage = validate_stage_lineage_receipt(
            value.get(short_name),
            expected_stage=RCA_STAGE_NAME_BY_SHORT[short_name],
            artifact_root=artifact_root,
        )
        if stage["identity"] != expected_identity_dict:
            raise StageLineageError("stage_lineage_identity_mismatch", short_name)
        if stage["finished_at"] != expected_finished_at.get(short_name):
            raise StageLineageError("stage_lineage_finished_at_mismatch", short_name)
        if stage["upstream_output_artifact_set_sha256"] != previous_output_sha256:
            raise StageLineageError("stage_lineage_upstream_hash_mismatch", short_name)
        if any(item not in stage["input_artifacts"] for item in previous_outputs):
            raise StageLineageError(
                "stage_lineage_upstream_artifact_missing", short_name
            )
        normalized[short_name] = stage
        previous_outputs = stage["output_artifacts"]
        previous_output_sha256 = stage["output_artifact_set_sha256"]

    if any(item not in normalized["s6"]["output_artifacts"] for item in final_outputs):
        raise StageLineageError("stage_lineage_final_output_missing")
    return normalized
