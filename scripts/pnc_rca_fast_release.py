#!/usr/bin/env python3
"""Plan an RCA release lane or run the isolated fast-repair proof."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
from typing import Any, Mapping, Sequence

from gateway.pnc_rca_release_lane import (
    build_release_lane_decision,
    canonical_bytes,
)


FAST_RELEASE_RECEIPT_SCHEMA_VERSION = "pnc_rca_fast_release_receipt_v1"
ROLLBACK_RECEIPT_SCHEMA_VERSION = "pnc_rca_fast_release_rollback_receipt_v1"
EVALUATOR_SCHEMA_VERSION = "pnc_rca_controlled_threshold_evaluator_v1"


class FastReleaseError(ValueError):
    def __init__(self, code: str, detail: str = ""):
        self.code = str(code or "fast_release_invalid")[:120]
        self.detail = str(detail or self.code)[:1000]
        super().__init__(self.detail)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def write_json(path: Path, value: Mapping[str, Any]) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = canonical_bytes(value) + b"\n"
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    return {"path": str(path), "sha256": sha256_bytes(raw), "bytes": len(raw)}


def _git(repo: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise FastReleaseError(
            "offline_git_failed", (completed.stderr or completed.stdout)[-500:]
        )
    return completed.stdout.strip()


def _evaluate(rule: Mapping[str, Any], observed_score: int) -> str:
    if rule.get("schema_version") != EVALUATOR_SCHEMA_VERSION:
        raise FastReleaseError("offline_evaluator_schema_invalid")
    threshold = rule.get("threshold")
    comparison = rule.get("comparison")
    if isinstance(threshold, bool) or not isinstance(threshold, int):
        raise FastReleaseError("offline_evaluator_threshold_invalid")
    if comparison == "gte":
        passed = observed_score >= threshold
    elif comparison == "lt":
        passed = observed_score < threshold
    else:
        raise FastReleaseError("offline_evaluator_comparison_invalid")
    return "pass" if passed else "fail"


def _load_manifest(path: Path) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FastReleaseError("validation_manifest_unreadable", str(exc)) from exc
    if not isinstance(value, Mapping):
        raise FastReleaseError("validation_manifest_invalid")
    sets = value.get("sets")
    s16 = sets.get("S16") if isinstance(sets, Mapping) else None
    if not isinstance(s16, Mapping) or s16.get("count") != 16:
        raise FastReleaseError("validation_manifest_s16_invalid")
    return value


def _case_id(manifest: Mapping[str, Any], requested: str) -> str:
    cases = manifest["sets"]["S16"]["cases"]
    ids = [str(case.get("work_item_id") or "") for case in cases]
    if requested:
        if requested not in ids:
            raise FastReleaseError("offline_case_not_in_s16")
        return requested
    return ids[0]


def plan_release_lane(
    *,
    output: Path,
    changed_paths: Sequence[str],
    dependency_closure: Sequence[str],
    validation_manifest_sha256: str,
    rollback_release_id: str,
    rollback_release_note_sha256: str,
    import_closure_complete: bool,
    max_concurrency: int,
) -> dict[str, Any]:
    decision = build_release_lane_decision(
        changed_paths=changed_paths,
        dependency_closure=dependency_closure,
        validation_manifest_sha256=validation_manifest_sha256,
        rollback_release_id=rollback_release_id,
        rollback_release_note_sha256=rollback_release_note_sha256,
        import_closure_complete=import_closure_complete,
        max_concurrency=max_concurrency,
    )
    return {
        "success": True,
        "release_lane": decision["release_lane"],
        "decision": write_json(output, decision),
    }


def run_offline_repair(
    *, validation_manifest: Path,
    output_dir: Path,
    requested_case_id: str = "",
) -> dict[str, Any]:
    manifest = _load_manifest(validation_manifest)
    manifest_sha256 = sha256_file(validation_manifest)
    case_id = _case_id(manifest, requested_case_id)
    repo = output_dir / "offline-evaluator-git"
    if repo.exists():
        raise FastReleaseError("offline_output_exists", str(repo))
    repo.mkdir(parents=True, mode=0o700)
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "RCA offline harness")
    _git(repo, "config", "user.email", "rca-offline@example.invalid")
    artifact = repo / "controlled-threshold-evaluator.json"
    predecessor = {
        "schema_version": EVALUATOR_SCHEMA_VERSION,
        "artifact_id": "controlled-threshold-buggy-v1",
        "comparison": "lt",
        "threshold": 1,
    }
    candidate = {
        "schema_version": EVALUATOR_SCHEMA_VERSION,
        "artifact_id": "controlled-threshold-fixed-v2",
        "comparison": "gte",
        "threshold": 1,
    }
    write_json(artifact, predecessor)
    _git(repo, "add", artifact.name)
    _git(repo, "commit", "-qm", "test: inject controlled evaluator defect")
    predecessor_commit = _git(repo, "rev-parse", "HEAD")
    predecessor_sha256 = sha256_file(artifact)
    injected_result = _evaluate(predecessor, 1)
    if injected_result != "fail":
        raise FastReleaseError("offline_defect_not_reproduced")

    write_json(artifact, candidate)
    _git(repo, "add", artifact.name)
    _git(repo, "commit", "-qm", "fix: repair controlled evaluator predicate")
    repair_commit = _git(repo, "rev-parse", "HEAD")
    repair_commit_at = utc_now()
    publish_started = time.monotonic_ns()
    candidate_sha256 = sha256_file(artifact)
    active_path = output_dir / "active-evaluator.json"
    shutil.copyfile(artifact, active_path)
    os.chmod(active_path, 0o600)
    candidate_result = _evaluate(candidate, 1)
    published_at = utc_now()
    measured_seconds = (time.monotonic_ns() - publish_started) / 1_000_000_000
    if candidate_result != "pass" or sha256_file(active_path) != candidate_sha256:
        raise FastReleaseError("offline_candidate_not_effective")

    predecessor_raw = subprocess.run(
        ["git", "-C", str(repo), "show", f"{predecessor_commit}:{artifact.name}"],
        check=True,
        capture_output=True,
    ).stdout
    active_path.write_bytes(predecessor_raw)
    os.chmod(active_path, 0o600)
    rollback_result = _evaluate(json.loads(predecessor_raw), 1)
    rollback_verified = (
        rollback_result == "fail" and sha256_file(active_path) == predecessor_sha256
    )
    if not rollback_verified:
        raise FastReleaseError("offline_rollback_not_verified")
    rollback_receipt = {
        "schema_version": ROLLBACK_RECEIPT_SCHEMA_VERSION,
        "verified_at": utc_now(),
        "target_surface": "offline_harness_artifact",
        "predecessor_commit": predecessor_commit,
        "predecessor_artifact_sha256": predecessor_sha256,
        "candidate_commit": repair_commit,
        "candidate_artifact_sha256": candidate_sha256,
        "rollback_behavior": rollback_result,
        "rollback_verified": True,
        "production_effects": False,
    }
    rollback_artifact = write_json(output_dir / "rollback-receipt.json", rollback_receipt)

    candidate_raw = subprocess.run(
        ["git", "-C", str(repo), "show", f"{repair_commit}:{artifact.name}"],
        check=True,
        capture_output=True,
    ).stdout
    active_path.write_bytes(candidate_raw)
    os.chmod(active_path, 0o600)
    final_result = _evaluate(json.loads(candidate_raw), 1)
    if final_result != "pass" or sha256_file(active_path) != candidate_sha256:
        raise FastReleaseError("offline_candidate_restore_failed")

    changed_path = "api/g1q3_rca/evaluators/controlled_threshold_rule.json"
    decision = build_release_lane_decision(
        changed_paths=[changed_path],
        dependency_closure=[changed_path],
        validation_manifest_sha256=manifest_sha256,
        rollback_release_id=f"offline-evaluator-{predecessor_commit[:12]}",
        rollback_release_note_sha256=predecessor_sha256,
        import_closure_complete=True,
        max_concurrency=1,
    )
    decision_artifact = write_json(output_dir / "lane-decision.json", decision)
    release_id = f"offline-vm-task-fast-{repair_commit[:12]}"
    receipt = {
        "schema_version": FAST_RELEASE_RECEIPT_SCHEMA_VERSION,
        "release_id": release_id,
        "release_lane": decision["release_lane"],
        "target_surface": "offline_harness_artifact",
        "case_id": case_id,
        "validation_manifest_sha256": manifest_sha256,
        "lane_decision_sha256": decision_artifact["sha256"],
        "predecessor": {
            "commit": predecessor_commit,
            "artifact_sha256": predecessor_sha256,
            "observed_result": injected_result,
        },
        "candidate": {
            "commit": repair_commit,
            "artifact_sha256": candidate_sha256,
            "observed_result": candidate_result,
            "final_active_result": final_result,
        },
        "repair_commit_at": repair_commit_at,
        "published_at": published_at,
        "commit_to_effect_seconds": measured_seconds,
        "initial_reference_seconds": 900,
        "within_initial_reference": measured_seconds <= 900,
        "rollback_receipt_sha256": rollback_artifact["sha256"],
        "rollback_verified": True,
        "production_success_claimed": False,
        "external_side_effects": {
            "feishu_writes": 0,
            "control_db_writes": 0,
            "kafka_commits": 0,
            "vm_submissions": 0,
            "resident_restarts": 0,
        },
    }
    receipt_artifact = write_json(output_dir / "fast-release-receipt.json", receipt)
    return {
        "success": True,
        "release_id": release_id,
        "receipt": receipt_artifact,
        "rollback_receipt": rollback_artifact,
        "lane_decision": decision_artifact,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    plan = subparsers.add_parser("plan")
    plan.add_argument("--output", type=Path, required=True)
    plan.add_argument("--changed-path", action="append", required=True)
    plan.add_argument("--dependency-path", action="append", required=True)
    plan.add_argument("--validation-manifest-sha256", required=True)
    plan.add_argument("--rollback-release-id", required=True)
    plan.add_argument("--rollback-release-note-sha256", required=True)
    plan.add_argument(
        "--import-closure-complete", action=argparse.BooleanOptionalAction, required=True
    )
    plan.add_argument("--max-concurrency", type=int, default=1)
    offline = subparsers.add_parser("offline-repair")
    offline.add_argument("--validation-manifest", type=Path, required=True)
    offline.add_argument("--output-dir", type=Path, required=True)
    offline.add_argument("--case-id", default="")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "plan":
            result = plan_release_lane(
                output=args.output,
                changed_paths=args.changed_path,
                dependency_closure=args.dependency_path,
                validation_manifest_sha256=args.validation_manifest_sha256,
                rollback_release_id=args.rollback_release_id,
                rollback_release_note_sha256=args.rollback_release_note_sha256,
                import_closure_complete=args.import_closure_complete,
                max_concurrency=args.max_concurrency,
            )
        else:
            result = run_offline_repair(
                validation_manifest=args.validation_manifest,
                output_dir=args.output_dir,
                requested_case_id=args.case_id,
            )
    except (FastReleaseError, subprocess.CalledProcessError) as exc:
        code = exc.code if isinstance(exc, FastReleaseError) else "offline_git_failed"
        print(
            json.dumps({"success": False, "error_code": code}, sort_keys=True),
        )
        return 2
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
