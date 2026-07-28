#!/usr/bin/env python3
"""Run the read-only PNC RCA release gate and fail closed before activation.

The suite composes existing scorecard, freshness, and cutover-plan contracts.
It never dispatches an activation or performs a production mutation.  A caller
may declare activation intent; the resulting exit code is the gate consumed by
the external cutover transaction.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile
from typing import Any, Mapping, Sequence


REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gateway.pnc_rca_quality_oracle import (
    REQUIRED_EVALUATOR_VALIDATION_DIMENSIONS,
    release_golden_registry_status,
)
from scripts import pnc_rca_release_scorecard as release_scorecard


SCHEMA_VERSION = "pnc_rca_release_gate_suite_v1"
AUTOMATED_CRITERIA = (
    "A-1",
    "A0",
    "A1",
    "A2",
    "A3",
    "A4",
    "A5",
    "A6",
    "A9",
    "A10",
    "A12",
    "A16",
    "A17",
)
HUMAN_CRITERIA = ("A7", "A13")
ALL_CRITERIA = (
    "A-1",
    "A0",
    "A1",
    "A2",
    "A3",
    "A4",
    "A5",
    "A6",
    "A7",
    "A9",
    "A10",
    "A12",
    "A13",
    "A16",
    "A17",
)
EXPECTED_CUTOVER_STAGES = (
    "preflight",
    "dormant",
    "lock-quiesce",
    "reversible-install",
    "prepare-producer",
    "post-boundary",
)
HEX40_RE = re.compile(r"^[0-9a-f]{40}$")
REFERENCE_TOKEN_RE = re.compile(
    r"^(?:host|pipeline offline) commit (?P<commit>[0-9a-f]{40})$"
)
MAX_INPUT_BYTES = 128 * 1024 * 1024


class ReleaseGateError(RuntimeError):
    """A bounded release-gate input cannot be trusted."""

    def __init__(self, code: str, detail: str = ""):
        self.code = code
        self.detail = detail or code
        super().__init__(self.detail)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical_json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _stable_bytes(path: Path, *, label: str) -> tuple[bytes, os.stat_result]:
    candidate = path.expanduser().absolute()
    try:
        before = candidate.lstat()
        if (
            stat.S_ISLNK(before.st_mode)
            or not stat.S_ISREG(before.st_mode)
            or before.st_size <= 0
            or before.st_size > MAX_INPUT_BYTES
        ):
            raise OSError("not a bounded regular file")
        raw = candidate.read_bytes()
        after = candidate.lstat()
    except OSError as exc:
        raise ReleaseGateError(
            "release_gate_source_unavailable", f"{label}: {candidate}"
        ) from exc
    identity_before = (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    )
    identity_after = (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    )
    if identity_before != identity_after or len(raw) != before.st_size:
        raise ReleaseGateError(
            "release_gate_source_changed_during_read", f"{label}: {candidate}"
        )
    return raw, after


def _json_object(path: Path, *, label: str) -> tuple[dict[str, Any], dict[str, Any]]:
    raw, observed = _stable_bytes(path, label=label)
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReleaseGateError(
            "release_gate_json_invalid", f"{label}: {path}"
        ) from exc
    if not isinstance(value, dict):
        raise ReleaseGateError(
            "release_gate_json_object_required", f"{label}: {path}"
        )
    return value, {
        "path": str(path.expanduser().absolute()),
        "sha256": _sha256(raw),
        "bytes": observed.st_size,
        "mtime_ns": observed.st_mtime_ns,
    }


def _trusted_child(root: Path, relative: str, *, label: str) -> Path:
    if not isinstance(relative, str) or not relative.strip():
        raise ReleaseGateError("release_gate_evidence_path_invalid", label)
    raw_path = Path(relative)
    if raw_path.is_absolute() or ".." in raw_path.parts:
        raise ReleaseGateError("release_gate_evidence_path_invalid", label)
    trusted_root = root.expanduser().resolve(strict=True)
    candidate = (trusted_root / raw_path).resolve(strict=True)
    try:
        candidate.relative_to(trusted_root)
    except ValueError as exc:
        raise ReleaseGateError("release_gate_evidence_path_escape", label) from exc
    return candidate


def _evidence_receipts(
    task_root: Path,
    criterion_id: str,
    values: Any,
    *,
    allowed_reference_tokens: frozenset[str] = frozenset(),
) -> tuple[list[dict[str, Any]], list[str]]:
    if not isinstance(values, list) or not values:
        return [], ["matrix_evidence_missing"]
    receipts: list[dict[str, Any]] = []
    errors: list[str] = []
    for index, value in enumerate(values):
        text = str(value)
        reference_match = REFERENCE_TOKEN_RE.fullmatch(text)
        if reference_match is not None:
            if text not in allowed_reference_tokens:
                errors.append("matrix_candidate_reference_mismatch")
            else:
                receipts.append(
                    {
                        "kind": "candidate_binding_reference",
                        "reference": text,
                        "sha256": _sha256(text.encode("utf-8")),
                    }
                )
            continue
        try:
            path = _trusted_child(
                task_root,
                text,
                label=f"acceptance.{criterion_id}.evidence[{index}]",
            )
            raw, observed = _stable_bytes(path, label=f"{criterion_id} evidence")
            receipts.append(
                {
                    "path": str(path),
                    "task_relative_path": path.relative_to(
                        task_root.resolve(strict=True)
                    ).as_posix(),
                    "sha256": _sha256(raw),
                    "bytes": observed.st_size,
                }
            )
        except (OSError, ReleaseGateError) as exc:
            errors.append(
                exc.code
                if isinstance(exc, ReleaseGateError)
                else "matrix_evidence_unavailable"
            )
    return receipts, sorted(set(errors))


def _run_command(
    argv: Sequence[str],
    *,
    cwd: Path,
    timeout_seconds: int,
    environment: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    command_environment = os.environ.copy()
    if environment:
        command_environment.update(environment)
    try:
        completed = subprocess.run(
            list(argv),
            cwd=cwd,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            env=command_environment,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {
            "argv": list(argv),
            "returncode": None,
            "error": type(exc).__name__,
            "stdout_sha256": "",
            "stderr_sha256": "",
            "stdout_tail": "",
            "stderr_tail": "",
        }
    stdout = completed.stdout or ""
    stderr = completed.stderr or ""
    try:
        parsed_stdout = json.loads(stdout)
    except json.JSONDecodeError:
        parsed_stdout = None
    return {
        "argv": list(argv),
        "returncode": completed.returncode,
        "error": "",
        "stdout_sha256": _sha256(stdout.encode("utf-8")),
        "stderr_sha256": _sha256(stderr.encode("utf-8")),
        "stdout_tail": stdout[-1200:],
        "stderr_tail": stderr[-1200:],
        "_parsed_stdout": parsed_stdout,
    }


def _command_evidence(result: Mapping[str, Any]) -> dict[str, Any]:
    return {
        str(key): value
        for key, value in result.items()
        if not str(key).startswith("_")
    }


def _git_identity(root: Path, *, timeout_seconds: int) -> dict[str, Any]:
    root = root.expanduser().resolve(strict=True)

    def git(*args: str) -> tuple[int | None, str]:
        result = _run_command(
            ("git", "-C", str(root), *args),
            cwd=root,
            timeout_seconds=timeout_seconds,
        )
        return result["returncode"], str(result["stdout_tail"]).strip()

    commit_rc, commit = git("rev-parse", "HEAD")
    tree_rc, tree = git("rev-parse", "HEAD^{tree}")
    dirty_rc, dirty = git("status", "--porcelain", "--untracked-files=no")
    valid = bool(
        commit_rc == 0
        and tree_rc == 0
        and dirty_rc == 0
        and HEX40_RE.fullmatch(commit)
        and HEX40_RE.fullmatch(tree)
    )
    return {
        "path": str(root),
        "commit": commit if valid else "",
        "tree": tree if valid else "",
        "tracked_clean": bool(valid and not dirty),
        "valid": valid,
    }


def _candidate_binding(
    matrix: Mapping[str, Any],
    *,
    host_source: Path,
    pipeline_source: Path,
    timeout_seconds: int,
) -> dict[str, Any]:
    candidate = matrix.get("candidate")
    candidate = candidate if isinstance(candidate, Mapping) else {}
    expected_host = candidate.get("host") or candidate.get("host_runtime_cutover")
    expected_pipeline = candidate.get("pipeline") or candidate.get(
        "pipeline_runtime_binding"
    )
    expected_w17 = candidate.get("pipeline_w17_offline_evaluation")
    expected_worker = candidate.get("worker") or candidate.get("worker_runtime")
    expected_host = expected_host if isinstance(expected_host, Mapping) else {}
    expected_pipeline = (
        expected_pipeline if isinstance(expected_pipeline, Mapping) else {}
    )
    expected_w17 = expected_w17 if isinstance(expected_w17, Mapping) else {}
    expected_worker = expected_worker if isinstance(expected_worker, Mapping) else {}
    host = _git_identity(host_source, timeout_seconds=timeout_seconds)
    pipeline = _git_identity(pipeline_source, timeout_seconds=timeout_seconds)
    errors: list[str] = []
    if not host["valid"]:
        errors.append("host_git_identity_invalid")
    if not host["tracked_clean"]:
        errors.append("host_tracked_worktree_dirty")
    if host["commit"] != str(expected_host.get("commit") or ""):
        errors.append("host_commit_mismatch")
    if host["tree"] != str(expected_host.get("tree") or ""):
        errors.append("host_tree_mismatch")

    runtime_commit = str(expected_pipeline.get("commit") or "")
    runtime_tree = str(expected_pipeline.get("tree") or "")
    runtime_object = {"commit": runtime_commit, "tree": "", "valid": False}
    if HEX40_RE.fullmatch(runtime_commit) and HEX40_RE.fullmatch(runtime_tree):
        observed = _run_command(
            ("git", "-C", str(pipeline_source), "rev-parse", f"{runtime_commit}^{{tree}}"),
            cwd=pipeline_source,
            timeout_seconds=timeout_seconds,
        )
        observed_tree = str(observed.get("stdout_tail") or "").strip()
        runtime_object = {
            "commit": runtime_commit,
            "tree": observed_tree,
            "valid": bool(observed.get("returncode") == 0 and observed_tree == runtime_tree),
        }
    if not runtime_object["valid"]:
        errors.append("pipeline_runtime_git_object_mismatch")

    w17_commit = str(expected_w17.get("commit") or "")
    w17_tree = str(expected_w17.get("tree") or "")
    if expected_w17:
        if expected_w17.get("runtime_binding") is not False:
            errors.append("pipeline_w17_runtime_boundary_invalid")
        if not pipeline["valid"]:
            errors.append("pipeline_w17_git_identity_invalid")
        if not pipeline["tracked_clean"]:
            errors.append("pipeline_w17_tracked_worktree_dirty")
        if pipeline["commit"] != w17_commit:
            errors.append("pipeline_w17_commit_mismatch")
        if pipeline["tree"] != w17_tree:
            errors.append("pipeline_w17_tree_mismatch")
    else:
        if not pipeline["valid"]:
            errors.append("pipeline_git_identity_invalid")
        if not pipeline["tracked_clean"]:
            errors.append("pipeline_tracked_worktree_dirty")
        if pipeline["commit"] != runtime_commit:
            errors.append("pipeline_commit_mismatch")
        if pipeline["tree"] != runtime_tree:
            errors.append("pipeline_tree_mismatch")

    worker_commit = str(expected_worker.get("commit") or "")
    worker_tree = str(expected_worker.get("tree") or "")
    if not HEX40_RE.fullmatch(worker_commit) or not HEX40_RE.fullmatch(worker_tree):
        errors.append("worker_expected_binding_invalid")
    release_id = str(candidate.get("release_id") or "")
    if not release_id:
        errors.append("candidate_release_id_missing")
    return {
        "status": "GREEN" if not errors else "RED",
        "expected": {
            "host": {
                "commit": str(expected_host.get("commit") or ""),
                "tree": str(expected_host.get("tree") or ""),
            },
            "pipeline": {
                "commit": runtime_commit,
                "tree": runtime_tree,
            },
            "pipeline_w17_offline": {
                "commit": w17_commit,
                "tree": w17_tree,
                "runtime_binding": expected_w17.get("runtime_binding"),
            },
            "worker": {"commit": worker_commit, "tree": worker_tree},
            "release_id": release_id,
        },
        "observed": {
            "host": host,
            "pipeline_runtime_git_object": runtime_object,
            "pipeline_w17_offline": pipeline,
            "worker": "matrix_contract_only_live_readback_remains_A9",
        },
        "errors": sorted(set(errors)),
    }


def _scorecard_check(path: Path) -> dict[str, Any]:
    try:
        scorecard, source = _json_object(path, label="A0 release scorecard")
        release_scorecard.validate_scorecard(scorecard)
    except (ReleaseGateError, release_scorecard.ScorecardError) as exc:
        return {
            "status": "RED",
            "errors": [
                exc.code
                if hasattr(exc, "code")
                else "release_scorecard_invalid"
            ],
        }
    return {
        "status": "GREEN",
        "source": source,
        "schema_version": scorecard.get("schema_version"),
        "release_status": scorecard.get("release_status"),
        "ga_claim_allowed": scorecard.get("ga_claim_allowed"),
        "read_only_attestation": scorecard.get("read_only_attestation"),
        "adapter": "scripts.pnc_rca_release_scorecard.validate_scorecard",
        "errors": [],
    }


def _freshness_registry_check(
    path: Path, *, expected_pipeline: Mapping[str, Any]
) -> dict[str, Any]:
    status = release_golden_registry_status(path)
    required = tuple(REQUIRED_EVALUATOR_VALIDATION_DIMENSIONS)
    errors: list[str] = []
    if status.get("valid") is not True:
        errors.append("golden_registry_invalid")
    if status.get("low_tier_golden_ready") is not True:
        errors.append("low_tier_golden_not_ready")
    if tuple(status.get("required_dimensions") or ()) != required:
        errors.append("validation_dimensions_contract_mismatch")
    if status.get("pipeline_commit") != str(expected_pipeline.get("commit") or ""):
        errors.append("golden_registry_pipeline_commit_mismatch")
    if status.get("pipeline_tree") != str(expected_pipeline.get("tree") or ""):
        errors.append("golden_registry_pipeline_tree_mismatch")
    scope = tuple(status.get("golden_scope_evaluator_ids") or ())
    fully_validated = tuple(status.get("fully_validated_evaluator_ids") or ())
    if scope != fully_validated:
        errors.append("golden_scope_fully_validated_set_mismatch")
    return {
        "status": "GREEN" if not errors else "RED",
        "adapter": (
            "gateway.pnc_rca_quality_oracle.release_golden_registry_status "
            "(shared by scripts.pnc_rca_release_freshness_gate)"
        ),
        "path": str(path.expanduser().absolute()),
        "registry_valid": status.get("valid") is True,
        "low_tier_golden_ready": status.get("low_tier_golden_ready") is True,
        "required_dimensions": list(status.get("required_dimensions") or ()),
        "fully_validated_evaluator_ids": list(fully_validated),
        "missing_dimensions_by_evaluator": {
            str(key): list(value)
            for key, value in dict(
                status.get("missing_dimensions_by_evaluator") or {}
            ).items()
        },
        "high_confidence_ticket_enforced": bool(
            status.get("valid") is True
            and tuple(status.get("required_dimensions") or ()) == required
            and scope == fully_validated
        ),
        "errors": sorted(set(errors)),
    }


def _cutover_plan_check(
    adapter_path: Path,
    *,
    task_root: Path,
    python: Path,
    timeout_seconds: int,
) -> dict[str, Any]:
    try:
        adapter = _trusted_child(
            task_root,
            adapter_path.relative_to(task_root).as_posix()
            if adapter_path.is_absolute()
            else adapter_path.as_posix(),
            label="cutover adapter",
        )
    except (ValueError, OSError, ReleaseGateError) as exc:
        return {
            "status": "RED",
            "errors": [
                exc.code
                if isinstance(exc, ReleaseGateError)
                else "cutover_adapter_path_invalid"
            ],
        }
    result = _run_command(
        (str(python), str(adapter)),
        cwd=task_root,
        timeout_seconds=timeout_seconds,
    )
    errors: list[str] = []
    parsed = result.get("_parsed_stdout")
    payload = dict(parsed) if isinstance(parsed, Mapping) else {}
    if not payload:
        errors.append("cutover_adapter_plan_json_invalid")
    if result.get("returncode") != 0:
        errors.append("cutover_adapter_plan_nonzero")
    if payload.get("ok") is not True or payload.get("mode") != "plan_only":
        errors.append("cutover_adapter_not_plan_only")
    if payload.get("production_mutation_performed") is not False:
        errors.append("cutover_adapter_mutation_attestation_invalid")
    if payload.get("ga_claimed") is not False:
        errors.append("cutover_adapter_ga_boundary_invalid")
    raw_stages = payload.get("stages")
    stages = raw_stages if isinstance(raw_stages, list) else []
    observed_stages = tuple(
        str(item.get("stage") or "")
        for item in stages
        if isinstance(item, Mapping)
    )
    if observed_stages != EXPECTED_CUTOVER_STAGES:
        errors.append("cutover_adapter_stage_sequence_invalid")
    return {
        "status": "GREEN" if not errors else "RED",
        "adapter": str(adapter),
        "plan_schema_version": payload.get("schema_version"),
        "stage_sequence": list(observed_stages),
        "production_mutation_performed": payload.get(
            "production_mutation_performed"
        ),
        "command": _command_evidence(result),
        "errors": sorted(set(errors)),
    }


def _w17_resource_paths(pipeline_source: Path) -> dict[str, Path]:
    base = pipeline_source / "api" / "g1q3_rca"
    return {
        "builder": base / "scripts" / "build_w17_evaluator_coverage.py",
        "ledger": base / "resources" / "w17" / "evaluator_coverage_ledger.json",
        "boundary": base
        / "resources"
        / "w17"
        / "synthetic_boundary_registry.json",
        "regression": base
        / "resources"
        / "w17"
        / "report_regression_baseline.json",
        "tier_recompute": pipeline_source.parent.parent
        / "release"
        / "host-release-be3ad56"
        / "scripts"
        / "pnc_rca_tier_recompute.py",
    }


def _w17_l_b_db_contract_check(
    matrix: Mapping[str, Any], *, task_root: Path
) -> dict[str, Any]:
    errors: list[str] = []
    sources: dict[str, Any] = {}
    authority = matrix.get("authority")
    authority = authority if isinstance(authority, Mapping) else {}
    binding = authority.get("w17_l_b_db_contract")
    binding = binding if isinstance(binding, Mapping) else {}
    receipt_ref = binding.get("receipt")
    receipt_ref = receipt_ref if isinstance(receipt_ref, Mapping) else {}
    manifest_ref = binding.get("manifest")
    manifest_ref = manifest_ref if isinstance(manifest_ref, Mapping) else {}

    documents: dict[str, dict[str, Any]] = {}
    paths: dict[str, Path] = {}
    for name, reference in (("receipt", receipt_ref), ("manifest", manifest_ref)):
        try:
            path = _trusted_child(
                task_root,
                str(reference.get("path") or ""),
                label=f"W17 L-B {name}",
            )
            document, source = _json_object(path, label=f"W17 L-B {name}")
            if source["sha256"] != reference.get("sha256"):
                errors.append(f"w17_l_b_{name}_sha256_mismatch")
            documents[name] = document
            sources[name] = source
            paths[name] = path
        except ReleaseGateError as exc:
            errors.append(exc.code)
    if errors or set(documents) != {"receipt", "manifest"}:
        return {"status": "RED", "errors": sorted(set(errors)), "sources": sources}

    receipt = documents["receipt"]
    manifest = documents["manifest"]
    if stat.S_IMODE(paths["receipt"].lstat().st_mode) != 0o600:
        errors.append("w17_l_b_receipt_exact_0600_required")
    if receipt.get("schema_version") != "g1q3_w17_lb_db_contract_scan_receipt_v1":
        errors.append("w17_l_b_receipt_schema_invalid")
    if manifest.get("schema_version") != "g1q3_w17_lb_db_contract_manifest_v1":
        errors.append("w17_l_b_manifest_schema_invalid")
    if receipt.get("status") != "PARTIAL" or manifest.get("status") != "PARTIAL":
        errors.append("w17_l_b_partial_status_invalid")

    receipt_artifacts = receipt.get("artifacts")
    receipt_artifacts = (
        receipt_artifacts if isinstance(receipt_artifacts, Mapping) else {}
    )
    receipt_manifest = receipt_artifacts.get("manifest")
    receipt_manifest = (
        receipt_manifest if isinstance(receipt_manifest, Mapping) else {}
    )
    if (
        receipt_manifest.get("path") != str(paths["manifest"])
        or receipt_manifest.get("sha256") != sources["manifest"]["sha256"]
    ):
        errors.append("w17_l_b_receipt_manifest_binding_invalid")

    candidate = matrix.get("candidate")
    candidate = candidate if isinstance(candidate, Mapping) else {}
    expected_pipeline = candidate.get("pipeline_w17_offline_evaluation")
    expected_pipeline = (
        expected_pipeline if isinstance(expected_pipeline, Mapping) else {}
    )
    candidate_binding = manifest.get("candidate_binding")
    candidate_binding = (
        candidate_binding if isinstance(candidate_binding, Mapping) else {}
    )
    if (
        candidate_binding.get("pipeline_commit") != expected_pipeline.get("commit")
        or candidate_binding.get("pipeline_tree") != expected_pipeline.get("tree")
    ):
        errors.append("w17_l_b_pipeline_binding_mismatch")

    source = manifest.get("source")
    source = source if isinstance(source, Mapping) else {}
    db_path_value = source.get("db_path")
    try:
        task_root_resolved = task_root.resolve(strict=True)
        db_path = Path(str(db_path_value or "")).resolve(strict=True)
        db_relative = db_path.relative_to(task_root_resolved)
        db_path = _trusted_child(
            task_root_resolved, db_relative.as_posix(), label="W17 L-B database"
        )
        db_raw, db_stat = _stable_bytes(db_path, label="W17 L-B database")
        db_sha256 = _sha256(db_raw)
        sources["database"] = {
            "path": str(db_path),
            "sha256": db_sha256,
            "bytes": db_stat.st_size,
            "mtime_ns": db_stat.st_mtime_ns,
        }
        if (
            db_sha256 != source.get("db_sha256")
            or db_stat.st_size != source.get("db_bytes")
            or source.get("db_integrity") != "ok"
            or source.get("db_open_mode") != "read_only_immutable"
        ):
            errors.append("w17_l_b_database_binding_invalid")
    except (OSError, ValueError, ReleaseGateError):
        errors.append("w17_l_b_database_binding_invalid")

    accounting = manifest.get("accounting")
    accounting = accounting if isinstance(accounting, Mapping) else {}
    source_checks = receipt.get("source_checks")
    source_checks = source_checks if isinstance(source_checks, Mapping) else {}
    active_count = accounting.get("active_evaluator_count")
    covered_count = accounting.get("active_evaluator_covered_count")
    missing_count = accounting.get("active_evaluator_missing_count")
    exact_count = accounting.get("exact_match_count")
    contract_count = accounting.get("contract_count")
    contract_missing = accounting.get("contract_missing_count")
    if (active_count, covered_count, missing_count) != (67, 67, 0):
        errors.append("w17_l_b_active_key_coverage_incomplete")
    if (
        not isinstance(exact_count, int)
        or not isinstance(contract_count, int)
        or not isinstance(contract_missing, int)
        or exact_count <= 0
        or exact_count + contract_missing != contract_count
        or accounting.get("canonical_target_snapshot_count") != 336
        or accounting.get("contract_failure_count") != 0
        or source_checks.get("exact_match_count") != exact_count
        or source_checks.get("report_contract_count") != contract_count
        or source_checks.get("contract_missing_count") != contract_missing
        or source_checks.get("contract_failure_count") != 0
    ):
        errors.append("w17_l_b_contract_accounting_invalid")
    if (
        source_checks.get("db_sha256") != source.get("db_sha256")
        or source_checks.get("inventory_ledger_sha256")
        != candidate_binding.get("inventory_ledger_sha256")
    ):
        errors.append("w17_l_b_source_binding_invalid")

    claims = manifest.get("claims")
    claims = claims if isinstance(claims, Mapping) else {}
    policy = manifest.get("policy")
    policy = policy if isinstance(policy, Mapping) else {}
    release_claim = receipt.get("release_claim")
    release_claim = release_claim if isinstance(release_claim, Mapping) else {}
    if (
        claims.get("l_b_active_key_coverage") is not True
        or claims.get("canonical_336_complete") is not False
        or claims.get("accuracy_evidence") is not False
        or claims.get("generalization_evidence") is not False
        or claims.get("high_confidence_dimension_evidence") is not False
        or claims.get("report_data_used_as_evaluator_input") is not False
        or release_claim.get("active_key_coverage") != "GREEN_67_OF_67"
        or release_claim.get("accuracy_evidence") is not False
        or release_claim.get("generalization_evidence") is not False
        or release_claim.get("high_confidence_dimension_evidence") is not False
        or not str(release_claim.get("a17_l_b") or "").startswith("PARTIAL_")
    ):
        errors.append("w17_l_b_claim_boundary_invalid")
    if (
        policy.get("identity_persisted_in_public_manifest") is not False
        or policy.get("raw_report_payload_persisted") is not False
        or policy.get("report_file_locator_persisted") is not False
        or receipt.get("production_actions") != []
        or manifest.get("production_actions") != []
    ):
        errors.append("w17_l_b_privacy_or_effect_boundary_invalid")

    verification = receipt.get("verification")
    verification = verification if isinstance(verification, Mapping) else {}
    negative = verification.get("negative_fingerprint_injection")
    negative = negative if isinstance(negative, Mapping) else {}
    if (
        verification.get("database_open_mode") != "read_only_immutable"
        or verification.get("mcap_read") is not False
        or verification.get("platform_or_kafka_required") is not False
        or verification.get("raw_report_payload_persisted") is not False
        or negative.get("exit_code") != 2
        or negative.get("output_artifact_count") != 0
    ):
        errors.append("w17_l_b_verification_boundary_invalid")
    return {
        "status": "GREEN" if not errors else "RED",
        "a17_l_b_status": release_claim.get("a17_l_b"),
        "active_evaluator_count": active_count,
        "active_evaluator_covered_count": covered_count,
        "canonical_target_snapshot_count": accounting.get(
            "canonical_target_snapshot_count"
        ),
        "exact_match_count": exact_count,
        "contract_missing_count": contract_missing,
        "accuracy_evidence": False,
        "high_confidence_dimension_evidence": False,
        "sources": sources,
        "errors": sorted(set(errors)),
    }


def _w17_checks(
    *,
    pipeline_source: Path,
    host_source: Path,
    python: Path,
    timeout_seconds: int,
) -> dict[str, Any]:
    paths = _w17_resource_paths(pipeline_source)
    paths["tier_recompute"] = host_source / "scripts" / "pnc_rca_tier_recompute.py"
    errors: list[str] = []
    pipeline_environment = {"PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION": "python"}
    sources: dict[str, Any] = {}
    documents: dict[str, dict[str, Any]] = {}
    for name in ("ledger", "boundary", "regression"):
        try:
            document, source = _json_object(paths[name], label=f"W17 {name}")
            documents[name] = document
            sources[name] = source
        except ReleaseGateError as exc:
            errors.append(exc.code)
    if errors:
        return {"status": "RED", "errors": sorted(set(errors)), "sources": sources}

    ledger = documents["ledger"]
    boundary = documents["boundary"]
    regression = documents["regression"]
    rows = ledger.get("inventory_rows")
    rows = rows if isinstance(rows, list) else []
    active_keys = {
        str(item.get("evaluator_key") or "")
        for item in rows
        if isinstance(item, Mapping) and item.get("status") == "active"
    }
    statuses = {
        str(item.get("status") or "")
        for item in rows
        if isinstance(item, Mapping)
    }
    accounting = ledger.get("coverage_accounting")
    accounting = accounting if isinstance(accounting, Mapping) else {}
    if len(rows) != 77 or accounting.get("status_assignment_count") != 77:
        errors.append("w17_inventory_status_assignment_incomplete")
    if not statuses.issubset({"active", "excluded_reference", "no_producer_module"}):
        errors.append("w17_inventory_status_invalid")
    if accounting.get("active_denominator") != len(active_keys) or not active_keys:
        errors.append("w17_active_denominator_invalid")
    row_keys = [
        str(item.get("evaluator_key") or "")
        for item in rows
        if isinstance(item, Mapping)
    ]
    if len(set(row_keys)) != 77 or "" in row_keys:
        errors.append("w17_inventory_key_identity_invalid")

    boundary_entries = boundary.get("entries")
    boundary_entries = boundary_entries if isinstance(boundary_entries, list) else []
    boundary_keys = {
        str(item.get("evaluator_key") or "")
        for item in boundary_entries
        if isinstance(item, Mapping)
    }
    nodeids = sorted(
        {
            str(item.get("pytest_nodeid") or "")
            for item in boundary_entries
            if isinstance(item, Mapping) and str(item.get("pytest_nodeid") or "")
        }
    )
    if boundary_keys != active_keys or not nodeids:
        errors.append("w17_synthetic_boundary_coverage_incomplete")
    if any(
        not str(item.get("assertion_text") or "").strip()
        for item in boundary_entries
        if isinstance(item, Mapping)
    ):
        errors.append("w17_synthetic_boundary_assertion_missing")

    regression_rows = regression.get("active_evaluator_coverage_rows")
    regression_rows = regression_rows if isinstance(regression_rows, list) else []
    schema_smoke_keys = {
        str(item.get("evaluator_key") or "")
        for item in regression_rows
        if isinstance(item, Mapping)
        and item.get("local_schema_smoke_status") == "present"
    }
    if schema_smoke_keys != active_keys:
        errors.append("w17_report_schema_smoke_incomplete")

    builder = _run_command(
        (str(python), str(paths["builder"]), "--check"),
        cwd=pipeline_source,
        timeout_seconds=timeout_seconds,
        environment=pipeline_environment,
    )
    if builder.get("returncode") != 0:
        errors.append("w17_deterministic_rebuild_check_failed")

    boundary_tests = _run_command(
        (str(python), "-m", "pytest", "-q", *nodeids),
        cwd=pipeline_source,
        timeout_seconds=timeout_seconds,
        environment=pipeline_environment,
    )
    if boundary_tests.get("returncode") != 0:
        errors.append("w17_synthetic_boundary_tests_failed")

    golden_nodeids = (
        "api/g1q3_rca/tests/test_rca_golden.py::test_golden_root_check_parses_committed_set",
        "api/g1q3_rca/tests/test_rca_golden.py::test_criterion_refuted_negative_catches_status_flip_without_conclusion_change",
        "api/g1q3_rca/tests/test_rca_golden.py::test_criterion_refuted_negative_fails_if_status_is_not_refuted",
        "api/g1q3_rca/tests/test_rca_golden.py::test_digest_change_does_not_hide_evaluator_verdict_regression",
    )
    golden_tests = _run_command(
        (str(python), "-m", "pytest", "-q", *golden_nodeids),
        cwd=pipeline_source,
        timeout_seconds=timeout_seconds,
        environment=pipeline_environment,
    )
    if golden_tests.get("returncode") != 0:
        errors.append("w17_evaluator_golden_tests_failed")

    with tempfile.TemporaryDirectory(prefix="pnc-rca-w17-gate-") as temporary:
        failure_receipt = Path(temporary) / "missing-dimension.json"
        ticket_negative = _run_command(
            (
                str(python),
                str(paths["tier_recompute"]),
                "--inject-negative",
                "missing_validation_dimension",
                "--failure-receipt",
                str(failure_receipt),
            ),
            cwd=host_source,
            timeout_seconds=timeout_seconds,
            environment=pipeline_environment,
        )
        negative_payload: dict[str, Any] = {}
        if failure_receipt.exists():
            try:
                negative_payload, _source = _json_object(
                    failure_receipt, label="W17 missing-dimension injection"
                )
            except ReleaseGateError:
                negative_payload = {}
        oracle = negative_payload.get("oracle")
        oracle = oracle if isinstance(oracle, Mapping) else {}
        facts = oracle.get("facts")
        facts = facts if isinstance(facts, Mapping) else {}
        expected_violation = str(negative_payload.get("expected_violation") or "")
        if (
            ticket_negative.get("returncode") != 2
            or negative_payload.get("blocked") is not True
            or negative_payload.get("exit_code") != 2
            or not expected_violation
            or expected_violation not in (oracle.get("violations") or [])
            or oracle.get("confidence_tier") != "medium"
            or facts.get("evaluator_validation_missing_dimensions")
            != ["lane_geometry_quality:synthetic_boundary"]
        ):
            errors.append("w17_high_confidence_ticket_negative_failed")

    regression_accounting = regression.get("accounting")
    regression_accounting = (
        regression_accounting if isinstance(regression_accounting, Mapping) else {}
    )
    return {
        "status": "GREEN" if not errors else "RED",
        "scope": "W17_pre_ga_guard_and_offline_structure",
        "active_evaluator_count": len(active_keys),
        "inventory_key_count": len(rows),
        "synthetic_boundary_evaluator_count": len(boundary_keys),
        "synthetic_boundary_nodeid_count": len(nodeids),
        "local_schema_smoke_evaluator_count": len(schema_smoke_keys),
        "pipeline_resource_l_b_evaluator_count": 0,
        "canonical_336_source_available": regression_accounting.get(
            "canonical_corpus_source_available"
        ),
        "commands": {
            "deterministic_rebuild": _command_evidence(builder),
            "synthetic_boundaries": _command_evidence(boundary_tests),
            "evaluator_golden_flip": _command_evidence(golden_tests),
            "high_confidence_ticket_negative": _command_evidence(ticket_negative),
        },
        "sources": sources,
        "nonblocking_followup": [
            "local_schema_smoke_is_not_l_b_coverage",
            "41_issue_mapping_rows_complete_but_end_to_end_formalized_count_zero",
            "four_fully_validated_evaluators_are_not_claimed_by_this_subset",
        ],
        "errors": sorted(set(errors)),
    }


def _matrix_row(
    matrix: Mapping[str, Any], criterion_id: str
) -> Mapping[str, Any]:
    acceptance = matrix.get("acceptance")
    acceptance = acceptance if isinstance(acceptance, Mapping) else {}
    row = acceptance.get(criterion_id)
    return row if isinstance(row, Mapping) else {}


def _allowed_reference_tokens(matrix: Mapping[str, Any]) -> frozenset[str]:
    candidate = matrix.get("candidate")
    candidate = candidate if isinstance(candidate, Mapping) else {}
    host = candidate.get("host") or candidate.get("host_runtime_cutover")
    host = host if isinstance(host, Mapping) else {}
    w17 = candidate.get("pipeline_w17_offline_evaluation")
    w17 = w17 if isinstance(w17, Mapping) else {}
    tokens = {
        f"host commit {host.get('commit')}" if host.get("commit") else "",
        f"pipeline offline commit {w17.get('commit')}" if w17.get("commit") else "",
    }
    return frozenset(tokens - {""})


def _automatic_matrix_criterion(
    matrix: Mapping[str, Any],
    *,
    task_root: Path,
    criterion_id: str,
) -> dict[str, Any]:
    row = _matrix_row(matrix, criterion_id)
    evidence, errors = _evidence_receipts(
        task_root,
        criterion_id,
        row.get("evidence"),
        allowed_reference_tokens=_allowed_reference_tokens(matrix),
    )
    if row.get("offline_status") != "GREEN":
        errors.append("matrix_offline_status_not_green")
    return {
        "criterion_id": criterion_id,
        "mode": "automated",
        "scope": "offline_release_evidence",
        "status": "GREEN" if not errors else "RED",
        "matrix_ga_status": row.get("ga_status"),
        "matrix_offline_status": row.get("offline_status"),
        "matrix_live_status": row.get("live_status"),
        "evidence": evidence,
        "errors": sorted(set(errors)),
    }


def _human_criterion(
    matrix: Mapping[str, Any],
    *,
    task_root: Path,
    criterion_id: str,
) -> dict[str, Any]:
    row = _matrix_row(matrix, criterion_id)
    evidence, preparation_errors = _evidence_receipts(
        task_root,
        criterion_id,
        row.get("evidence"),
        allowed_reference_tokens=_allowed_reference_tokens(matrix),
    )
    return {
        "criterion_id": criterion_id,
        "mode": "requires_human",
        "scope": "first_live_canary_or_real_batch",
        "status": "REQUIRES_HUMAN",
        "matrix_ga_status": row.get("ga_status"),
        "matrix_offline_status": row.get("offline_status"),
        "matrix_live_status": row.get("live_status"),
        "preparation_status": "GREEN" if not preparation_errors else "RED",
        "evidence": evidence,
        "errors": sorted(set(preparation_errors)),
    }


def _merge_check(criterion: dict[str, Any], name: str, check: Mapping[str, Any]) -> None:
    criterion.setdefault("checks", {})[name] = dict(check)
    if check.get("status") != "GREEN":
        criterion["status"] = "RED"
        criterion.setdefault("errors", []).extend(
            str(value) for value in check.get("errors", [])
        )
        criterion["errors"] = sorted(set(criterion["errors"]))


def build_release_gate(
    *,
    matrix_path: Path,
    task_root: Path,
    host_source: Path,
    pipeline_source: Path,
    cutover_adapter: Path,
    registry_path: Path,
    pipeline_python: Path,
    timeout_seconds: int,
    activation_intent: bool,
    inject_auto_red: str = "",
) -> dict[str, Any]:
    matrix, matrix_source = _json_object(matrix_path, label="acceptance matrix")
    task_root = task_root.expanduser().resolve(strict=True)
    binding = _candidate_binding(
        matrix,
        host_source=host_source,
        pipeline_source=pipeline_source,
        timeout_seconds=timeout_seconds,
    )
    criteria: list[dict[str, Any]] = []
    by_id: dict[str, dict[str, Any]] = {}
    for criterion_id in ALL_CRITERIA:
        if criterion_id in HUMAN_CRITERIA:
            criterion = _human_criterion(
                matrix,
                task_root=task_root,
                criterion_id=criterion_id,
            )
        elif criterion_id == "A17":
            row = _matrix_row(matrix, criterion_id)
            criterion = {
                "criterion_id": criterion_id,
                "mode": "automated",
                "scope": "W17_decision49_guard_with_partial_A17_accounting",
                "status": "GREEN",
                "matrix_ga_status": row.get("ga_status"),
                "matrix_offline_status": row.get("offline_status"),
                "matrix_live_status": row.get("live_status"),
                "overall_acceptance_status": "PARTIAL_NONBLOCKING",
                "deferred_nonblocking_scopes": [
                    "W17.5_real_mcap_dimensions_and_41_issue_mapping",
                    "W17.6_independent_generalization_set",
                    "four_fully_validated_high_confidence_evaluators",
                ],
                "evidence": [],
                "errors": [],
            }
        elif criterion_id == "A-1":
            row = _matrix_row(matrix, criterion_id)
            evidence, errors = _evidence_receipts(
                task_root,
                criterion_id,
                row.get("evidence"),
                allowed_reference_tokens=_allowed_reference_tokens(matrix),
            )
            criterion = {
                "criterion_id": criterion_id,
                "mode": "automated",
                "scope": "pre_activation_static_cutover_contract",
                "status": "GREEN" if not errors else "RED",
                "matrix_ga_status": row.get("ga_status"),
                "matrix_offline_status": row.get("offline_status"),
                "matrix_live_status": row.get("live_status"),
                "evidence": evidence,
                "live_followup_required": (
                    "fresh single-writer, prestate, and dual-lock capture remains "
                    "inside the cutover transaction"
                ),
                "errors": errors,
            }
        else:
            criterion = _automatic_matrix_criterion(
                matrix,
                task_root=task_root,
                criterion_id=criterion_id,
            )
        criteria.append(criterion)
        by_id[criterion_id] = criterion

    candidate_check = {
        "status": binding["status"],
        "errors": binding["errors"],
        "binding": binding,
    }
    for criterion_id in AUTOMATED_CRITERIA:
        _merge_check(by_id[criterion_id], "candidate_binding", candidate_check)
    for criterion_id in HUMAN_CRITERIA:
        human = by_id[criterion_id]
        human.setdefault("checks", {})["candidate_binding"] = candidate_check
        if candidate_check["status"] != "GREEN":
            human["preparation_status"] = "RED"
            human.setdefault("errors", []).extend(candidate_check["errors"])
            human["errors"] = sorted(set(human["errors"]))

    row_a0 = _matrix_row(matrix, "A0")
    a0_evidence = row_a0.get("evidence")
    scorecard_path: Path | None = None
    if isinstance(a0_evidence, list) and a0_evidence:
        try:
            scorecard_path = _trusted_child(
                task_root, str(a0_evidence[0]), label="A0 scorecard evidence"
            )
        except ReleaseGateError:
            scorecard_path = None
    scorecard_check = (
        _scorecard_check(scorecard_path)
        if scorecard_path is not None
        else {"status": "RED", "errors": ["scorecard_evidence_unavailable"]}
    )
    _merge_check(by_id["A0"], "release_scorecard", scorecard_check)

    candidate = matrix.get("candidate")
    candidate = candidate if isinstance(candidate, Mapping) else {}
    expected_pipeline = candidate.get("pipeline") or candidate.get(
        "pipeline_runtime_binding"
    )
    expected_pipeline = (
        expected_pipeline if isinstance(expected_pipeline, Mapping) else {}
    )
    freshness = _freshness_registry_check(
        registry_path, expected_pipeline=expected_pipeline
    )
    _merge_check(by_id["A1"], "release_freshness_registry", freshness)

    cutover = _cutover_plan_check(
        cutover_adapter,
        task_root=task_root,
        python=pipeline_python,
        timeout_seconds=timeout_seconds,
    )
    _merge_check(by_id["A-1"], "cutover_plan_only_adapter", cutover)

    w17 = _w17_checks(
        pipeline_source=pipeline_source,
        host_source=host_source,
        python=pipeline_python,
        timeout_seconds=timeout_seconds,
    )
    _merge_check(by_id["A17"], "w17_automated_subset", w17)
    l_b = _w17_l_b_db_contract_check(matrix, task_root=task_root)
    _merge_check(by_id["A17"], "w17_l_b_db_contract", l_b)
    _merge_check(by_id["A17"], "high_confidence_ticket_registry", freshness)

    if inject_auto_red:
        injected = by_id[inject_auto_red]
        injected["status"] = "RED"
        injected.setdefault("errors", []).append("injected_automatic_gate_failure")
        injected["errors"] = sorted(set(injected["errors"]))

    automated_red = [
        item["criterion_id"]
        for item in criteria
        if item["mode"] == "automated" and item["status"] != "GREEN"
    ]
    automated_green = [
        item["criterion_id"]
        for item in criteria
        if item["mode"] == "automated" and item["status"] == "GREEN"
    ]
    requires_human = [
        item["criterion_id"]
        for item in criteria
        if item["mode"] == "requires_human"
    ]
    preflight_status = (
        "BLOCKED_AUTOMATED_GATE_RED"
        if automated_red
        else "AUTOMATED_GATE_GREEN_REQUIRES_HUMAN_CAPTURE"
    )
    report = {
        "schema_version": SCHEMA_VERSION,
        "observed_at": _utc_now(),
        "execution_scope": "offline_read_only_release_preflight",
        "matrix": matrix_source,
        "candidate_binding": binding,
        "criteria": criteria,
        "summary": {
            "automated_total": len(AUTOMATED_CRITERIA),
            "automated_green": automated_green,
            "automated_red": automated_red,
            "requires_human": requires_human,
            "all_automated_green": not automated_red,
            "all_criteria_green": False,
        },
        "activation_preflight": {
            "activation_intent_declared": activation_intent,
            "status": preflight_status,
            "exit_code": 2 if automated_red else 0,
            "activation_dispatch_performed": False,
            "external_cutover_may_proceed_to_human_capture": not automated_red,
        },
        "negative_injection": {
            "active": bool(inject_auto_red),
            "criterion_id": inject_auto_red,
        },
        "read_only_attestation": {
            "production_mutation_performed": False,
            "network_requests_performed": False,
            "activation_dispatched": False,
            "external_effects_triggered": False,
        },
    }
    validate_release_gate(report)
    return report


def validate_release_gate(report: Mapping[str, Any]) -> None:
    if report.get("schema_version") != SCHEMA_VERSION:
        raise ReleaseGateError("release_gate_schema_invalid")
    criteria = report.get("criteria")
    if not isinstance(criteria, list):
        raise ReleaseGateError("release_gate_criteria_invalid")
    ids = tuple(
        str(item.get("criterion_id") or "")
        for item in criteria
        if isinstance(item, Mapping)
    )
    if ids != ALL_CRITERIA:
        raise ReleaseGateError("release_gate_criteria_set_invalid")
    for item in criteria:
        if not isinstance(item, Mapping):
            raise ReleaseGateError("release_gate_criterion_invalid")
        criterion_id = str(item.get("criterion_id") or "")
        if criterion_id in HUMAN_CRITERIA:
            if (
                item.get("mode") != "requires_human"
                or item.get("status") != "REQUIRES_HUMAN"
            ):
                raise ReleaseGateError("release_gate_human_marker_invalid")
        elif item.get("mode") != "automated" or item.get("status") not in {
            "GREEN",
            "RED",
        }:
            raise ReleaseGateError("release_gate_automated_status_invalid")
    summary = report.get("summary")
    summary = summary if isinstance(summary, Mapping) else {}
    preflight = report.get("activation_preflight")
    preflight = preflight if isinstance(preflight, Mapping) else {}
    has_red = bool(summary.get("automated_red"))
    if has_red and (
        preflight.get("status") != "BLOCKED_AUTOMATED_GATE_RED"
        or preflight.get("exit_code") == 0
        or preflight.get("external_cutover_may_proceed_to_human_capture") is not False
    ):
        raise ReleaseGateError("release_gate_red_not_blocking")
    attestation = report.get("read_only_attestation")
    attestation = attestation if isinstance(attestation, Mapping) else {}
    if any(
        attestation.get(field) is not False
        for field in (
            "production_mutation_performed",
            "network_requests_performed",
            "activation_dispatched",
            "external_effects_triggered",
        )
    ):
        raise ReleaseGateError("release_gate_read_only_attestation_invalid")


def _arguments(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix", type=Path, required=True)
    parser.add_argument("--task-root", type=Path, required=True)
    parser.add_argument("--host-source", type=Path, default=REPO_ROOT)
    parser.add_argument("--pipeline-source", type=Path, required=True)
    parser.add_argument("--cutover-adapter", type=Path, required=True)
    parser.add_argument(
        "--registry",
        type=Path,
        default=REPO_ROOT
        / "gateway"
        / "assets"
        / "pnc_rca_release_golden_registry_v1.json",
    )
    parser.add_argument("--pipeline-python", type=Path, default=Path(sys.executable))
    parser.add_argument("--timeout-seconds", type=int, default=300)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--activation-intent",
        action="store_true",
        help="declare that the caller will use this result before cutover",
    )
    parser.add_argument(
        "--inject-auto-red",
        choices=AUTOMATED_CRITERIA,
        default="",
        help="negative-only proof that an automated RED blocks activation preflight",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = _arguments(argv)
        if args.timeout_seconds < 1 or args.timeout_seconds > 1800:
            raise ReleaseGateError("release_gate_timeout_invalid")
        report = build_release_gate(
            matrix_path=args.matrix,
            task_root=args.task_root,
            host_source=args.host_source,
            pipeline_source=args.pipeline_source,
            cutover_adapter=args.cutover_adapter,
            registry_path=args.registry,
            pipeline_python=args.pipeline_python,
            timeout_seconds=args.timeout_seconds,
            activation_intent=args.activation_intent,
            inject_auto_red=args.inject_auto_red,
        )
        raw = _canonical_json_bytes(report)
        if args.output is not None:
            output = args.output.expanduser().absolute()
            output.parent.mkdir(parents=True, exist_ok=True)
            temporary = output.with_name(f".{output.name}.{os.getpid()}.tmp")
            temporary.write_bytes(raw)
            os.replace(temporary, output)
        sys.stdout.buffer.write(raw)
        return int(report["activation_preflight"]["exit_code"])
    except ReleaseGateError as exc:
        failure = {
            "schema_version": SCHEMA_VERSION,
            "ok": False,
            "code": exc.code,
            "detail": exc.detail,
            "activation_preflight": {
                "status": "BLOCKED_GATE_ERROR",
                "exit_code": 2,
                "activation_dispatch_performed": False,
            },
        }
        sys.stderr.buffer.write(_canonical_json_bytes(failure))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
