from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import subprocess

import pytest

from gateway import pnc_rca_prod_admission as admission
from scripts import pnc_rca_controlled_gray as gray


NOW = datetime(2026, 7, 21, 15, 30, tzinfo=timezone.utc)
GIB = 1024**3


def _git(root: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", "-C", str(root), *arguments],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _host_candidate(tmp_path: Path) -> tuple[Path, str, str]:
    root = tmp_path / "host-candidate"
    root.mkdir()
    _git(root, "init", "-q")
    _git(root, "config", "user.email", "gray@example.com")
    _git(root, "config", "user.name", "Gray Test")
    runtime_files = (
        "gateway/pnc_rca_delivery_contract.py",
        "gateway/pnc_rca_prod_admission.py",
        "gateway/pnc_rca_runtime_identity.py",
        "scripts/pnc_rca_delivery_collector.py",
        "scripts/pnc_rca_delivery_dispatcher.py",
        "scripts/pnc_rca_kafka_consumer.py",
        "scripts/pnc_rca_outbox_dispatcher.py",
    )
    for relative in runtime_files:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"VALUE = {relative!r}\n", encoding="utf-8")
    (root / ".gitignore").write_text(
        "__pycache__/\n.pytest_cache/\n*.pyc\n*.pyo\n", encoding="utf-8"
    )
    identity = root / "gateway/pnc_rca_runtime_identity.py"
    identity.write_text(
        "RCA_RUNTIME_RELATIVE_FILES = " + repr(runtime_files) + "\n"
        "GATEWAY_RCA_RUNTIME_RELATIVE_FILES = "
        + repr(("gateway/pnc_rca_runtime_identity.py",))
        + "\n",
        encoding="utf-8",
    )
    (root / "gateway/pnc_rca_delivery_contract.py").write_text(
        '''DELIVERY_MANIFEST_SCHEMA_VERSION = "delivery_manifest_v2"
DELIVERY_EFFECT_SCHEMA_VERSION = "pnc_rca_delivery_effect_v2"
DELIVERY_REPORT_LINK_KIND = "manifest_html"
RCA_RESULT_FIELD_KEY = "field_9193cb"
RCA_REPORT_FIELD_KEY = "field_8c912e"
_BASE_EFFECT_SEMANTIC_FIELDS = (
    "project_key", "project_simple_name", "report_link_kind"
)

def build_issue_comment_content(
    *, marker, work_item_id, report_status, conclusion, report_url,
    report_cifs_path
):
    return "\\n".join((marker, conclusion, report_url, report_cifs_path))

def build_thread_reply_content(
    *, marker, work_item_id, report_status, conclusion, report_url, issue_url
):
    return "\\n".join((marker, conclusion, report_url, issue_url))

def compute_delivery_effect_payload_sha256(value, kind): return "a"
def compute_delivery_effect_key(**kwargs): return "b"
def delivery_effect_marker(effect_key, artifact_set_id): return "c"

def verify_delivery_bundle(payload):
    semantic = {
        "schema_version": DELIVERY_EFFECT_SCHEMA_VERSION,
        "report_link_kind": DELIVERY_REPORT_LINK_KIND,
        "field_updates": [RCA_RESULT_FIELD_KEY, RCA_REPORT_FIELD_KEY],
    }
    digest = compute_delivery_effect_payload_sha256(semantic, "issue")
    effect = compute_delivery_effect_key(semantic_payload_sha256=digest)
    marker = delivery_effect_marker(effect, payload["artifact_set_id"])
    return build_issue_comment_content(
        marker=marker,
        work_item_id=payload["work_item_id"],
        report_status="ready",
        conclusion="result",
        report_url=payload["report_url"],
        report_cifs_path=payload["report_cifs_path"],
    )
''',
        encoding="utf-8",
    )
    (root / "scripts/pnc_rca_delivery_dispatcher.py").write_text(
        '''from gateway.pnc_rca_delivery_contract import (
    DELIVERY_EFFECT_SCHEMA_VERSION,
    DELIVERY_REPORT_LINK_KIND,
    build_issue_comment_content,
    build_thread_reply_content,
)

class DeliveryContractError(Exception): pass
_PROJECT_SIMPLE_NAME_RE = object()

class MeegleIssueCommentAdapter:
    def get_fields(self, project_key, work_item_id, field_keys): return {}
    def list_comments(self, project_key, work_item_id): return []

def verify_persisted_artifact_inventory(**kwargs): return []

def _validate_effect(claim):
    payload = claim.payload
    schema_version = payload.get("schema_version")
    project_simple_name = payload.get("project_simple_name")
    if schema_version != DELIVERY_EFFECT_SCHEMA_VERSION:
        raise DeliveryContractError("delivery_effect_schema_unsupported")
    if payload.get("report_link_kind") != DELIVERY_REPORT_LINK_KIND:
        raise DeliveryContractError("delivery_effect_report_link_kind_invalid")
    if _PROJECT_SIMPLE_NAME_RE.fullmatch(project_simple_name) is None:
        raise DeliveryContractError("delivery_issue_url_identity_mismatch")
    verify_persisted_artifact_inventory(manifest=claim.manifest)
    if claim.issue:
        expected_content = build_issue_comment_content(
            marker=payload["marker"], work_item_id=claim.work_item_id,
            report_status="ready", conclusion=payload["conclusion"],
            report_url=claim.report_url, report_cifs_path=claim.report_cifs_path,
        )
    else:
        expected_content = build_thread_reply_content(
            marker=payload["marker"], work_item_id=claim.work_item_id,
            report_status="ready", conclusion=payload["conclusion"],
            report_url=claim.report_url, issue_url=claim.issue_url,
        )
    if payload.get("comment_content") != expected_content:
        raise DeliveryContractError("delivery_effect_content_invalid")
    return expected_content

def _marker_matches(comments, marker): return comments
def _canonical_remote_content(content, marker): return content

def _confirmed_content_matches(comments, marker, expected_content):
    return [
        item for item in _marker_matches(comments, marker)
        if _canonical_remote_content(item["content"], marker) == expected_content
    ]

def default_report_verifier(response):
    if (
        response.get("status_code") != 200
        or response.get("content_length") != response["expected_size"]
        or response.get("sha256") != response["expected_sha256"]
    ):
        return {"error_code": "report_http_verification_mismatch"}
    return {"success": True}

class DeliveryDispatcher:
    def _verify_report_artifacts(self, claim, validated, *, uncertain):
        result = self.report_verifier("url", 1, "sha")
        if (
            result.get("status_code") != 200
            or result.get("content_length") != 1
            or result.get("sha256") != "sha"
        ):
            return "report_http_verification_mismatch"
        return None

    def _complete_from_marker(self, claim, validated, match, *, source):
        receipt = {
            "confirmed_content_sha256": "sha",
            "confirmed_report_url": claim.report_url,
        }
        return self.complete_effect(receipt)

    def _list_remote_effect(self, claim, validated):
        return self.list_comments(claim.project_key, claim.work_item_id)
    def _read_field_updates(self, claim, validated):
        return self.get_fields(claim.project_key, claim.work_item_id)
    def _write_field_updates(self, claim, validated):
        return self.update_fields(claim.project_key, claim.work_item_id)
    def _add_remote_effect(self, claim, validated):
        return self.add_comment(claim.project_key, claim.work_item_id)
    def complete_effect(self, receipt): return receipt

    def _dispatch_claim(self, claim):
        validated = _validate_effect(claim)
        failure = self._verify_report_artifacts(
            claim, validated, uncertain=False
        )
        if failure: return failure
        before = self._list_remote_effect(claim, validated)
        first = _confirmed_content_matches(before, "marker", validated)
        if first:
            return self._complete_from_marker(
                claim, validated, first[0], source="read_before_write"
            )
        self._write_field_updates(claim, validated)
        second = _confirmed_content_matches(before, "marker", validated)
        self._add_remote_effect(claim, validated)
        third = _confirmed_content_matches(second, "marker", validated)
        receipt = {
            "confirmed_content_sha256": "sha",
            "confirmed_report_url": claim.report_url,
        }
        return self.complete_effect(receipt if third else {})
''',
        encoding="utf-8",
    )
    _git(root, "add", ".")
    _git(root, "commit", "-qm", "fixture")
    return root, _git(root, "rev-parse", "HEAD"), _git(
        root, "rev-parse", "HEAD^{tree}"
    )


def _host_receipt(
    tmp_path: Path,
    *,
    root: Path,
    commit: str,
    tree: str,
    name: str = "host-go.json",
) -> tuple[Path, str]:
    body = {
        "schema_version": "pnc_rca_host_controlled_gray_independent_audit_v1",
        "scope": "controlled-gray BOM binding only",
        "observed_at": (NOW - timedelta(minutes=10)).isoformat(),
        "candidate": {
            "repo": str(root),
            "commit": commit,
            "tree": tree,
            "parent": "7" * 40,
            "git_clean": True,
            "git_status": "",
            "cache_dirs": [],
            "pyc_files": [],
        },
        "deployment_authorization": False,
        "production_mutation": False,
        "production_actions": [],
        "open_blockers": list(gray.HOST_GO_EXPECTED_BLOCKERS),
        "release_recommendation": (
            "eligible_for_controlled_gray_bom_binding_only"
        ),
        "verdict": "GO",
        "receipt_storage": {
            "authoritative_owner_only_path": "placeholder",
            "required_mode": "0600",
            "create_once": True,
            "integrity_algorithm": "sha256",
            "serialization": "canonical JSON",
        },
        "verification": {
            "focused_suite": {"result": "PASS", "passed": 171},
            "code_checks": {"ruff": "PASS", "diff_check": "PASS"},
            "worktree_hygiene": {
                "git_clean": True,
                "cache_dirs": 0,
                "pyc_files": 0,
            },
            "production_shape_probe": {
                "internal_project_key_bound_to_target": True,
                "project_simple_name": gray.TARGET_PROJECT_SIMPLE_NAME,
                "browser_issue_url": gray.TARGET_ISSUE_URL,
                "semantic_payload_sha256_valid": True,
            },
            "blocker_reproductions": {
                "arbitrary_comment_body": {"result": "PASS"},
                "marker_only_remote_comment": {"result": "PASS"},
                "success_effect_v1_forged_from_current_claim": {
                    "result": "PASS"
                },
            },
        },
        "live_evidence": {
            "capacity": {"regular_capacity_authorization_present": False},
            "exact_target": {
                "work_item_id": gray.TARGET_WORK_ITEM_ID,
                "issue_url": gray.TARGET_ISSUE_URL,
                "result_field_nonempty": False,
                "report_field_nonempty": False,
                "rca_marker_comment_count": 0,
            },
        },
    }
    path = tmp_path / name
    body["receipt_storage"]["authoritative_owner_only_path"] = str(path)
    raw = json.dumps(body, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    path.write_bytes(raw)
    path.chmod(0o600)
    return path, hashlib.sha256(raw).hexdigest()


def _vm_receipt(
    tmp_path: Path,
) -> tuple[Path, str, str, str, str]:
    commit = "4" * 40
    tree = "9" * 40
    parent = "d" * 40
    root = "/home/mini/.hermes/rca-prod-runtime/releases/fixture"
    body = {
        "schema_version": "g1q3_rca_vm_candidate_independent_audit_v2",
        "scope": (
            "offline VM release candidate; controlled release tooling "
            "eligibility only"
        ),
        "observed_at": (NOW - timedelta(minutes=10)).isoformat(),
        "candidate": {
            "repo": root,
            "commit": commit,
            "tree": tree,
            "parent": parent,
            "git_clean": True,
            "git_status": "",
            "cache_dirs": [],
            "pyc_files": [],
            "symlinks": [],
            "candidate_edited_by_independent_auditor": False,
        },
        "candidate_lineage": [
            {
                "commit": commit,
                "tree": tree,
                "parents": [parent],
                "subject": "fixture",
            }
        ],
        "changed_files_from_validated_base": [
            "api/g1q3_rca/scripts/run_rca_service_request.py"
        ],
        "deployment_authorization": False,
        "final_checks": {
            "candidate_hygiene": {
                "git_clean": True,
                "cache_dirs": 0,
                "pyc_files": 0,
                "symlinks": 0,
            },
            "focused_cifs_suite": {
                "returncode": 0,
                "passed": 139,
                "skipped": 4,
            },
            "delivery_manifest_v2": {
                "checks_passed": True,
                "manifest_sha256": "1" * 64,
            },
            "posix_symlink_negative_coverage": {
                "all_four_cifs_skips_covered_on_symlink_capable_posix_fs": True,
            },
            "legacy_perception_literal_classification": {
                "true_production_write_sinks": 0,
            },
        },
        "nonblocking_notes": [],
        "open_blockers": [],
        "production_actions": [],
        "receipt_storage": {
            "authoritative_owner_only_path": (
                "/home/mini/.hermes/rca-prod-runtime/audits/fixture/receipt.json"
            ),
            "authoritative_required_mode": "0600",
            "byte_identical_vm_replica_path": "/mnt/tmp/fixture/receipt.json",
            "create_once": True,
            "integrity_algorithm": "sha256",
            "replica_mount_mode_policy": "0755",
            "serialization": "canonical JSON",
            "user_visible_cifs_path": (
                "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/"
                "tmp/fixture/receipt.json"
            ),
        },
        "release_recommendation": "eligible_for_controlled_release_tooling",
        "superseded_findings": [],
        "validated_base": {
            "commit": parent,
            "tree": "8" * 40,
            "is_ancestor_of_candidate": True,
            "receipt": {"path": "/mnt/tmp/base.json", "sha256": "2" * 64},
        },
        "verdict": "GO",
    }
    raw = json.dumps(body, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    path = tmp_path / "vm-go.json"
    path.write_bytes(raw)
    return path, hashlib.sha256(raw).hexdigest(), root, commit, tree


def _spec(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict:
    host_root, host_commit, host_tree = _host_candidate(tmp_path)
    host_receipt_path, host_receipt_sha = _host_receipt(
        tmp_path, root=host_root, commit=host_commit, tree=host_tree
    )
    vm_path, vm_sha, vm_root, vm_commit, vm_tree = _vm_receipt(tmp_path)
    monkeypatch.setattr(gray, "EXPECTED_HOST_ROOT", str(host_root))
    monkeypatch.setattr(gray, "EXPECTED_HOST_COMMIT", host_commit)
    monkeypatch.setattr(gray, "EXPECTED_HOST_TREE", host_tree)
    monkeypatch.setattr(
        gray, "EXPECTED_HOST_GO_RECEIPT_PATH", str(host_receipt_path)
    )
    monkeypatch.setattr(
        gray, "EXPECTED_HOST_GO_RECEIPT_SHA256", host_receipt_sha
    )
    monkeypatch.setattr(gray, "EXPECTED_VM_ROOT", vm_root)
    monkeypatch.setattr(gray, "EXPECTED_VM_COMMIT", vm_commit)
    monkeypatch.setattr(gray, "EXPECTED_VM_TREE", vm_tree)
    monkeypatch.setattr(gray, "EXPECTED_VM_GO_RECEIPT_SHA256", vm_sha)
    return {
        "schema_version": gray.SPEC_SCHEMA_VERSION,
        "release_id": "rca-gray-7051585084-fixture",
        "host_candidate": {
            "root": str(host_root),
            "commit": host_commit,
            "tree": host_tree,
        },
        "vm_candidate": {
            "root": vm_root,
            "commit": vm_commit,
            "tree": vm_tree,
            "independent_go_receipt_path": str(vm_path),
            "independent_go_receipt_sha256": vm_sha,
        },
    }


def _resource_report(
    *,
    max_concurrency: int = 1,
    successful_samples: int = 20,
    input_materialized_samples: int = 0,
) -> dict:
    snapshot = {
        "schema_version": admission.SNAPSHOT_SCHEMA_VERSION,
        "observed_at": NOW.isoformat(),
        "root_available_bytes": 700 * GIB,
        "delivery_available_bytes": 900 * GIB,
        "root_device": "root-device",
        "delivery_device": "delivery-device",
        "delivery_filesystem": "cifs",
        "delivery_mount_rw": True,
        "delivery_writable": True,
        "memory_available_bytes": 64 * GIB,
        "swap_free_ratio": 0.9,
        "load1": 1.0,
        "cpu_count": 32,
        "dnp_real": 0,
        "dnp_like": 0,
        "mcap_rss_bytes": 0,
        "mcap_process_count": 0,
    }
    authorization = {
        "schema_version": "context-rca-capacity-authorization/v1",
        "policy_version": "context-rca-capacity-model/2026-07-12-v1",
        "receipt_path": str(gray.CANONICAL_CAPACITY_AUTHORIZATION_PATH),
        "authorization_ready": True,
        "status": "valid",
        "reason_codes": [],
        "receipt_id": "regular-capacity-fixture",
        "receipt_fingerprint": "1" * 64,
        "approval_evidence_sha256": "2" * 64,
        "authorization_receipt_sha256": "3" * 64,
        "issued_at": (NOW - timedelta(minutes=5)).isoformat(),
        "expires_at": (NOW + timedelta(hours=1)).isoformat(),
        "successful_sample_count": successful_samples,
        "input_materialized_sample_count": input_materialized_samples,
        "max_concurrency": max_concurrency,
        "root_required_available_bytes": 500 * GIB,
        "delivery_required_available_bytes": 600 * GIB,
    }
    return {
        "ok": True,
        "ok_for_submit": True,
        "ok_for_rca_prod_submit": True,
        "resource_class": "rca_prod",
        "reasons": [],
        "rca_prod_reasons": [],
        "rca_capacity_authorization": authorization,
        "rca_prod_snapshot": snapshot,
        "rca_prod_snapshot_sha256": admission.sha256_value(snapshot),
    }


def test_valid_plan_binds_candidates_runtime_and_gray_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = gray.evaluate(
        _spec(tmp_path, monkeypatch),
        now=NOW,
        resource_probe=lambda: _resource_report(),
    )

    assert result["decision"] == "GO"
    assert result["status"] == "GO_FOR_CONTROLLED_GRAY_SUBMISSION"
    assert result["production_effects"] == {
        "production_mutation": False,
        "production_write_attempts": 0,
        "kafka_offset_commits": 0,
        "feishu_writes": 0,
        "service_restarts": 0,
        "vm_tasks_submitted": 0,
    }
    bom = result["bom"]
    assert bom["bom_core_sha256"] == result["bom_core_sha256"]
    assert bom["tooling"]["submission_tool"]["path"] == str(
        gray.CANONICAL_SUBMIT_PATH
    )
    assert bom["tooling"]["submission_tool"]["required_resource_class"] == (
        "rca_prod"
    )
    assert bom["components"]["vm"]["independent_go_receipt"]["verdict"] == "GO"
    host = bom["components"]["host"]
    assert host["independent_go_receipt"] == {
        "observed_path": gray.EXPECTED_HOST_GO_RECEIPT_PATH,
        "sha256": gray.EXPECTED_HOST_GO_RECEIPT_SHA256,
        "schema_version": "pnc_rca_host_controlled_gray_independent_audit_v1",
        "verdict": "GO",
        "scope": "controlled-gray BOM binding only",
        "deployment_authorization": False,
    }
    assert host["runtime_manifest_sha256"] == gray._sha256_value(
        host["runtime_manifest"]
    )
    assert set(gray.REQUIRED_HOST_RUNTIME_FILES).issubset(
        host["runtime_manifest"]["files"]
    )
    assert host["delivery_capabilities"] == {
        "delivery_manifest_schema_version": "delivery_manifest_v2",
        "delivery_effect_schema_version": "pnc_rca_delivery_effect_v2",
        "report_link_kind": "manifest_html",
        "field_keys": ["field_9193cb", "field_8c912e"],
        "legacy_v1_success_effect_rejected": True,
        "canonical_content_reconstruction": True,
        "api_project_key_and_url_slug_separated": True,
        "official_field_adapter": "MeegleIssueCommentAdapter.get_fields",
        "official_comment_adapter": "MeegleIssueCommentAdapter.list_comments",
        "full_content_match_call_count": 3,
        "http_artifact_verification_precedes_remote_boundary": True,
        "http_artifact_verification_call_count": 1,
        "receipt_fields": ["confirmed_content_sha256", "confirmed_report_url"],
        "contract_verify_calls": {
            "build_issue_comment_content": 1,
            "compute_delivery_effect_payload_sha256": 1,
            "compute_delivery_effect_key": 1,
            "delivery_effect_marker": 1,
        },
    }
    contract = bom["execution_contract"]
    target = contract["scope"]["ordered_targets"][0]
    assert target["work_item_id"] == "7051585084"
    assert target["api_project_key"] == "68ef617fb371dc80a10641f7"
    assert target["project_simple_name"] == "t03o4q"
    assert "project_key" not in target
    assert contract["scope"]["ordered_targets"][1] == {
        "kind": "first_natural_kafka_canary_after_target",
        "count": 1,
        "source_kind": "kafka_workflow_event",
        "delivery_source": "ordinary_kafka_ingest",
        "synthetic": False,
        "manual_trigger": False,
        "operator_recovery": False,
    }
    assert contract["serial_failure_fence"]["max_concurrency"] == 1
    assert contract["serial_failure_fence"]["stop_on_first_failure"] is True
    assert contract["kafka_observation"]["enable_auto_commit"] is False
    assert contract["kafka_observation"]["commit_api_allowed"] is False
    assert contract["rca_execution"]["real_rca_required"] is True
    assert contract["delivery"]["field_keys_in_order"] == [
        "field_9193cb",
        "field_8c912e",
    ]
    assert contract["delivery"]["effect_schema_version"] == (
        "pnc_rca_delivery_effect_v2"
    )
    assert contract["delivery"]["legacy_effect_schema_v1_forbidden"] is True
    assert contract["delivery"]["report_link_kind"] == "manifest_html"
    assert (
        contract["delivery"]["report_field"]["source"]
        == "delivery_manifest_v2.report_url"
    )
    assert contract["delivery"]["evidence_comment"]["exact_count"] == 1
    assert contract["delivery"]["evidence_comment"]["must_bind"] == [
        "effect_key_via_marker",
        "artifact_set_id",
        "attribution_result_text",
        "manifest_html_report_url",
    ]
    assert contract["delivery"]["official_readback"]["required"] is True
    assert contract["delivery"]["official_readback"]["field_adapter"] == (
        "MeegleIssueCommentAdapter.get_fields"
    )
    assert contract["delivery"]["official_readback"]["comment_adapter"] == (
        "MeegleIssueCommentAdapter.list_comments"
    )
    assert contract["delivery"]["official_readback"]["combined_adapter"] is None
    assert contract["delivery"]["official_readback"]["api_project_key"] == (
        "68ef617fb371dc80a10641f7"
    )
    assert contract["delivery"]["official_readback"][
        "project_simple_name_for_url_only"
    ] == "t03o4q"
    assert contract["delivery"]["official_readback"][
        "comment_hash_must_match_full_evidence_comment"
    ] is True
    assert contract["delivery"]["official_readback"][
        "marker_only_readback_forbidden"
    ] is True
    assert contract["delivery"]["prewrite_http_revalidation"][
        "required_before_paths"
    ] == [
        "initial_write",
        "idempotent_existing_effect_completion",
        "uncertain_write_recovery",
        "field_repair_after_existing_comment",
        "post_write_ack",
    ]
    boundary = contract["submission_boundary"]
    assert boundary["resource_class"] == "rca_prod"
    assert boundary["capacity_mode"] == "steady"
    assert boundary["bootstrap_authorization_forbidden"] is True
    assert result["capacity_gate"]["bootstrap_used"] is False
    assert result["capacity_gate"]["probe_command"][-2:] == [
        "--resource-class",
        "rca_prod",
    ]
    assert result["capacity_gate"]["canonical_capacity_validator_path"] == str(
        gray.CANONICAL_CAPACITY_VALIDATOR_PATH
    )
    assert result["admission_contract"][
        "signed_rca_prod_admission_required_just_in_time"
    ] is True
    assert result["admission_contract"][
        "production_effects_authorized_by_this_plan"
    ] is False
    assert result["admission_contract"]["submission_tool_sha256"] == (
        bom["tooling"]["submission_tool"]["sha256"]
    )


def test_missing_regular_capacity_is_no_go_before_effects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    blocked = _resource_report()
    blocked.update(
        ok_for_submit=False,
        ok_for_rca_prod_submit=False,
        reasons=["rca_capacity_model_not_ready"],
        rca_prod_reasons=["rca_capacity_model_not_ready"],
    )
    blocked["rca_capacity_authorization"] = {
        "schema_version": "context-rca-capacity-authorization/v1",
        "policy_version": "context-rca-capacity-model/2026-07-12-v1",
        "receipt_path": str(gray.CANONICAL_CAPACITY_AUTHORIZATION_PATH),
        "authorization_ready": False,
        "status": "missing",
        "reason_codes": ["rca_capacity_model_not_ready", "receipt_missing"],
    }

    result = gray.evaluate(
        _spec(tmp_path, monkeypatch), now=NOW, resource_probe=lambda: blocked
    )

    assert result["decision"] == "NO_GO"
    assert result["status"] == "NO_GO_REGULAR_RCA_PROD_CAPACITY"
    assert result["admission_contract"] is None
    assert result["capacity_gate"]["bootstrap_accepted"] is False
    assert result["production_effects"]["production_write_attempts"] == 0
    assert result["production_effects"]["vm_tasks_submitted"] == 0


def test_bootstrap_report_cannot_satisfy_regular_gray_gate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    report = _resource_report()
    report["resource_class"] = "rca_prod_bootstrap"
    report["ok_for_rca_prod_submit"] = False
    report["ok_for_rca_prod_bootstrap_submit"] = True
    report["rca_prod_reasons"] = ["rca_capacity_model_not_ready"]

    result = gray.evaluate(
        _spec(tmp_path, monkeypatch), now=NOW, resource_probe=lambda: report
    )

    assert result["decision"] == "NO_GO"
    assert result["capacity_gate"]["capacity_mode"] == "steady"
    assert result["production_effects"]["production_mutation"] is False


@pytest.mark.parametrize(
    "mutation",
    [
        lambda report: report["rca_capacity_authorization"].update(
            max_concurrency=2
        ),
        lambda report: report["rca_capacity_authorization"].update(
            successful_sample_count=19
        ),
        lambda report: report["rca_capacity_authorization"].update(
            input_materialized_sample_count=1
        ),
        lambda report: report["rca_capacity_authorization"].update(
            receipt_path="/tmp/forged-capacity.json"
        ),
    ],
)
def test_capacity_relaxation_or_path_override_is_no_go(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mutation
) -> None:
    report = _resource_report()
    mutation(report)

    result = gray.evaluate(
        _spec(tmp_path, monkeypatch), now=NOW, resource_probe=lambda: report
    )

    assert result["decision"] == "NO_GO"
    assert result["production_effects"]["production_write_attempts"] == 0


def test_vm_receipt_tamper_is_static_no_go(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = _spec(tmp_path, monkeypatch)
    path = Path(spec["vm_candidate"]["independent_go_receipt_path"])
    body = json.loads(path.read_text())
    body["verdict"] = "NO_GO"
    raw = json.dumps(body, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    path.write_bytes(raw)
    spec["vm_candidate"]["independent_go_receipt_sha256"] = hashlib.sha256(
        raw
    ).hexdigest()

    result = gray.evaluate(
        spec, now=NOW, resource_probe=lambda: _resource_report()
    )

    assert result["decision"] == "NO_GO"
    assert result["status"] == "NO_GO_STATIC_VALIDATION"
    assert result["bom"] is None
    assert result["production_effects"]["vm_tasks_submitted"] == 0


def test_host_receipt_tamper_is_static_no_go(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = _spec(tmp_path, monkeypatch)
    path = Path(gray.EXPECTED_HOST_GO_RECEIPT_PATH)
    body = json.loads(path.read_text())
    body["deployment_authorization"] = True
    raw = json.dumps(body, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    path.write_bytes(raw)
    monkeypatch.setattr(
        gray,
        "EXPECTED_HOST_GO_RECEIPT_SHA256",
        hashlib.sha256(raw).hexdigest(),
    )

    result = gray.evaluate(
        spec, now=NOW, resource_probe=lambda: _resource_report()
    )

    assert result["decision"] == "NO_GO"
    assert result["status"] == "NO_GO_STATIC_VALIDATION"
    assert result["blockers"][0]["code"] == (
        "controlled_gray_host_go_receipt_invalid"
    )
    assert result["production_effects"]["production_write_attempts"] == 0


def test_host_worktree_drift_is_static_no_go(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = _spec(tmp_path, monkeypatch)
    root = Path(spec["host_candidate"]["root"])
    (root / "untracked.txt").write_text("drift\n", encoding="utf-8")

    result = gray.evaluate(
        spec, now=NOW, resource_probe=lambda: _resource_report()
    )

    assert result["decision"] == "NO_GO"
    assert result["blockers"][0]["code"] == "controlled_gray_host_worktree_dirty"
    assert result["production_effects"]["production_write_attempts"] == 0


def test_host_ignored_cache_is_static_no_go(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = _spec(tmp_path, monkeypatch)
    cache = Path(spec["host_candidate"]["root"]) / "gateway/__pycache__"
    cache.mkdir()
    (cache / "shadow.pyc").write_bytes(b"not bytecode")

    result = gray.evaluate(
        spec, now=NOW, resource_probe=lambda: _resource_report()
    )

    assert result["decision"] == "NO_GO"
    assert result["blockers"][0]["code"] == "controlled_gray_host_cache_forbidden"
    assert result["production_effects"]["production_write_attempts"] == 0


@pytest.mark.parametrize(
    ("contract_mutation", "dispatcher_mutation"),
    [
        (
            lambda raw: raw.replace(
                b'DELIVERY_EFFECT_SCHEMA_VERSION = "pnc_rca_delivery_effect_v2"',
                b'DELIVERY_EFFECT_SCHEMA_VERSION = "pnc_rca_delivery_effect_v1"',
            ),
            lambda raw: raw,
        ),
        (
            lambda raw: raw.replace(
                b'DELIVERY_REPORT_LINK_KIND = "manifest_html"',
                b'DELIVERY_REPORT_LINK_KIND = "foxglove_viz"',
            ),
            lambda raw: raw,
        ),
        (
            lambda raw: raw,
            lambda raw: raw.replace(
                b"schema_version != DELIVERY_EFFECT_SCHEMA_VERSION",
                b"schema_version != DELIVERY_EFFECT_SCHEMA_VERSION_V1",
            ),
        ),
        (
            lambda raw: raw,
            lambda raw: raw.replace(
                b'_canonical_remote_content(item["content"], marker) '
                b"== expected_content",
                b'_canonical_remote_content(item["content"], marker) is not None',
            ),
        ),
        (
            lambda raw: raw,
            lambda raw: raw.replace(
                b"def list_comments(self, project_key, work_item_id):",
                b"def list_marker_only(self, project_key, work_item_id):",
            ),
        ),
        (
            lambda raw: raw,
            lambda raw: raw.replace(
                b"failure = self._verify_report_artifacts(",
                b"failure = self._verify_report_artifacts_disabled(",
            ),
        ),
        (
            lambda raw: raw,
            lambda raw: raw.replace(
                b"claim.project_key", b"claim.project_simple_name"
            ),
        ),
    ],
)
def test_host_delivery_capability_relaxation_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    contract_mutation,
    dispatcher_mutation,
) -> None:
    spec = _spec(tmp_path, monkeypatch)
    root = Path(spec["host_candidate"]["root"])
    contract_raw = subprocess.run(
        ["git", "-C", str(root), "show", "HEAD:gateway/pnc_rca_delivery_contract.py"],
        check=True,
        capture_output=True,
    ).stdout
    dispatcher_raw = subprocess.run(
        ["git", "-C", str(root), "show", "HEAD:scripts/pnc_rca_delivery_dispatcher.py"],
        check=True,
        capture_output=True,
    ).stdout

    with pytest.raises(
        gray.ControlledGrayError, match="host_delivery_capability_invalid"
    ):
        gray._validate_host_delivery_capabilities(
            contract_raw=contract_mutation(contract_raw),
            dispatcher_raw=dispatcher_mutation(dispatcher_raw),
        )


def test_execution_contract_is_not_caller_configurable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = _spec(tmp_path, monkeypatch)
    spec["max_concurrency"] = 2

    result = gray.evaluate(
        spec, now=NOW, resource_probe=lambda: _resource_report()
    )

    assert result["decision"] == "NO_GO"
    assert result["blockers"][0]["code"] == "controlled_gray_spec_shape_invalid"


def test_plan_output_is_owner_only_create_once(tmp_path: Path) -> None:
    body = {"decision": "NO_GO", "production_mutation": False}
    output = tmp_path / "gray-plan.json"

    gray._write_create_once(output, body)

    assert output.stat().st_mode & 0o777 == 0o600
    assert json.loads(output.read_text()) == body
    with pytest.raises(gray.ControlledGrayError, match="output_identity_invalid"):
        gray._write_create_once(output, body)


def test_parser_exposes_no_execute_or_capacity_override() -> None:
    actions = {option for action in gray._parser()._actions for option in action.option_strings}

    assert "--execute" not in actions
    assert "--capacity-authorization" not in actions
    assert "--bootstrap-authorization" not in actions


def test_production_bindings_pin_current_reviewed_artifacts() -> None:
    assert gray.EXPECTED_HOST_ROOT == (
        "/Users/songying/.codex/tmp/rca-host-70c432-zero-cache"
    )
    assert gray.EXPECTED_HOST_COMMIT == (
        "540dc0c8b6fd0ed58a919f63a17ae7d934f0f94a"
    )
    assert gray.EXPECTED_HOST_TREE == (
        "a339f44e634ab6779b30683be3219257da10fba2"
    )
    assert gray.EXPECTED_VM_COMMIT == (
        "4b26cc7935eb4fa0910b42abde78d7f8d4efa0d1"
    )
    assert gray.EXPECTED_VM_TREE == (
        "9d45fb1357c7ab054c16c898941e342b9a50d391"
    )
    assert gray.EXPECTED_VM_GO_RECEIPT_SHA256 == (
        "0765e0adfb3e74abe6a1daaea626901003b9b0cb94223a0b401d626d1a48d1bf"
    )


def test_bom_hash_changes_with_host_candidate_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = _spec(tmp_path, monkeypatch)
    first = gray.evaluate(
        spec, now=NOW, resource_probe=lambda: _resource_report()
    )
    root = Path(spec["host_candidate"]["root"])
    target = root / "gateway/pnc_rca_prod_admission.py"
    target.write_text("VALUE = 'new admission identity'\n", encoding="utf-8")
    _git(root, "add", ".")
    _git(root, "commit", "-qm", "new host")
    second_spec = copy.deepcopy(spec)
    second_spec["host_candidate"]["commit"] = _git(root, "rev-parse", "HEAD")
    second_spec["host_candidate"]["tree"] = _git(root, "rev-parse", "HEAD^{tree}")
    monkeypatch.setattr(
        gray, "EXPECTED_HOST_COMMIT", second_spec["host_candidate"]["commit"]
    )
    monkeypatch.setattr(
        gray, "EXPECTED_HOST_TREE", second_spec["host_candidate"]["tree"]
    )
    second_receipt_path, second_receipt_sha = _host_receipt(
        tmp_path,
        root=root,
        commit=second_spec["host_candidate"]["commit"],
        tree=second_spec["host_candidate"]["tree"],
        name="host-go-second.json",
    )
    monkeypatch.setattr(
        gray, "EXPECTED_HOST_GO_RECEIPT_PATH", str(second_receipt_path)
    )
    monkeypatch.setattr(
        gray, "EXPECTED_HOST_GO_RECEIPT_SHA256", second_receipt_sha
    )
    second = gray.evaluate(
        second_spec, now=NOW, resource_probe=lambda: _resource_report()
    )

    assert first["decision"] == second["decision"] == "GO"
    assert first["bom_core_sha256"] != second["bom_core_sha256"]
    assert (
        first["admission_contract"]["contract_sha256"]
        != second["admission_contract"]["contract_sha256"]
    )
