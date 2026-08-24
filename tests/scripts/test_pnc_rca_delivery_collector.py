from __future__ import annotations

from dataclasses import replace
import hashlib
import json
from datetime import timedelta
from pathlib import Path, PurePosixPath
import sqlite3
import subprocess
import sys
from types import SimpleNamespace

import pytest

from gateway.pnc_rca_delivery_contract import (
    DeliveryContractError,
    TERMINAL_FALLBACK_CONTRACT_SCHEMA_VERSION,
    TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION,
    render_public_rca_result,
)
from gateway.pnc_rca_issue_focus import build_issue_focus_plan
from gateway.pnc_rca_control_store import RcaControlStore
from gateway.pnc_rca_delivery_store import RcaDeliveryStore
from scripts import pnc_rca_delivery_collector as collector
from scripts.pnc_foxglove_delivery import canonical_viz_mcap_path
from tests.gateway.test_pnc_rca_delivery_store import (
    NOW,
    _bind_activation_execution,
    _control,
    _delivery,
    _insert_subscription,
    _physical_v15_delivery_fixture,
    _policy,
    _record,
    _sqlite_storage_identity,
)
from tests.gateway.test_pnc_rca_w3_snapshot import _runtime_authority
from tests.gateway.test_pnc_rca_write_fence import _release_note
from tests.gateway.test_pnc_rca_control_store import (
    _direct_steady_contract,
    _migrate_v14_fixture_to_v15,
)


def _config_env(tmp_path) -> dict[str, str]:
    return {
        "HERMES_RCA_DELIVERY_COLLECTOR_ENABLED": "true",
        "HERMES_RCA_DELIVERY_COLLECTOR_CONTROL_DB_PATH": str(
            tmp_path / "control.sqlite3"
        ),
        "HERMES_RCA_DELIVERY_COLLECTOR_HEALTH_PATH": str(tmp_path / "health.json"),
        "HERMES_RCA_DELIVERY_COLLECTOR_SSH_MINI_AGENT": "/safe/ssh-mini-agent",
        "HERMES_RCA_DELIVERY_COLLECTOR_ARTIFACT_READ_TIMEOUT_SECONDS": "30",
        "HERMES_RCA_DELIVERY_COLLECTOR_LEASE_SECONDS": "60",
    }


def _bind_minimal_release(control, fixture, *, epoch_id="delivery-epoch-1"):
    fixture.epoch["epoch_id"] = epoch_id
    with sqlite3.connect(control.db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN IMMEDIATE")
        release_note_sha256 = fixture.epoch["release_note_sha256"]
        updated = conn.execute(
            "UPDATE rca_activation_epochs "
            "SET state = 'steady_active', preauthorization_fingerprint = ?, "
            "preauthorization_gate_receipt_sha256 = ?, "
            "preauthorization_capsule_sha256 = ?, "
            "preproduction_fingerprint = ?, "
            "preproduction_gate_receipt_sha256 = ?, "
            "preproduction_capsule_sha256 = ?, config_sha256 = ?, "
            "production_fingerprint = ?, production_gate_receipt_sha256 = ? "
            "WHERE epoch_id = ? AND is_current = 1",
            (
                fixture.fingerprint,
                release_note_sha256,
                release_note_sha256,
                fixture.fingerprint,
                release_note_sha256,
                release_note_sha256,
                fixture.env_sha256,
                fixture.fingerprint,
                release_note_sha256,
                epoch_id,
            ),
        )
        assert updated.rowcount == 1
        epoch = conn.execute(
            "SELECT * FROM rca_activation_epochs "
            "WHERE epoch_id = ? AND is_current = 1",
            (epoch_id,),
        ).fetchone()
        assert epoch is not None
        RcaControlStore._insert_activation_transition_audit_tx(
            conn,
            epoch=epoch,
            from_state="direct_release",
            to_state="steady_active",
            operator="test:fixture",
            reason="bind test minimal release",
            transitioned_at="2026-08-17T12:00:00+00:00",
        )
        conn.commit()


def _set_live_release_environment(monkeypatch, fixture):
    monkeypatch.setenv("PNC_LIVE_RUNTIME_ROOT", str(fixture.runtime_root))
    monkeypatch.setenv("PNC_LIVE_RUNTIME_COMMIT", fixture.runtime_commit)
    monkeypatch.setenv("PNC_LIVE_RUNTIME_TREE", fixture.runtime_tree)
    monkeypatch.setenv("PNC_LIVE_MANIFEST_SHA256", fixture.manifest_sha256)


def _json_bytes(value) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True).encode("utf-8")


def _gate_a_capability(*, status="supported"):
    return {
        "actual_evaluators": [
            {"evaluator_id": "aeb_trigger", "status": status}
        ],
        "actual_signals": ["AEBReq"],
        "actual_fields": [],
    }


def _trusted_v19_contract():
    communication = {
        "schema_version": "g1q3_rca_conclusion_communication_v2",
        "style_profile": "human_professional_dense_v2",
        "mode": "high_confidence_candidate",
        "headline": "归因候选：ACC 控制请求异常",
        "summary": "问题窗口内 ACC 控制请求异常并与减速度响应同窗。",
        "next_action": "由 ACC 团队确认修复并完成同场景回归。",
        "evidence_points": ["ACC 控制请求与减速度响应同窗"],
        "evidence_boundary": "仅覆盖已解码控制请求与车辆响应",
        "publication": {
            "primary_candidate_eligible": True,
            "reason_codes": [],
            "requires_receipt_decoded_backing": True,
        },
        "quality_checks": {
            "evidence_only": True,
            "no_new_causal_claim": True,
            "routing_hints_separated_from_evidence": True,
            "evidence_points_present": True,
            "decoded_evidence_backed": True,
            "causality_closed": True,
            "expression_quality_pass": True,
            "professional_structure_complete": True,
            "professional_wording": True,
            "no_substantial_sentence_repetition": True,
        },
    }
    capability = _gate_a_capability()
    capability["integrated_sources"] = {
        "conclusion_communication": communication,
    }
    return {
        "business_state": "report_completed",
        "consumer_capability": capability,
        "summary": {
            "short_conclusion": communication["summary"],
            "professional_conclusion": communication["summary"],
            "professional_conclusion_selection_status": "trusted_professional",
        },
        "report": {
            "candidate_owner_domain": "ACC",
            "candidate_owner": "ACC 控制模块",
            "is_candidate": True,
            "is_deliverable": True,
            "status": "report_ready",
        },
        "artifacts": {
            "attribution_causal_text": "ACC 控制请求异常导致减速度响应。",
        },
        "public_result": {
            "summary": {"short_conclusion": communication["summary"]},
            "responsibility": {"status": "confirmed", "candidate": "ACC 控制模块"},
            "causal_chain": {
                "narrative": [
                {"role": "现象", "text": "窗口内出现异常减速度响应。"},
                {"role": "证据", "text": "ACC 控制请求与减速度响应同窗。"},
                {"role": "因果判断", "text": "ACC 控制请求异常导致减速度响应。"},
                ]
            },
        },
    }


def _trusted_v19_focus():
    title = "ACC-自车右转，ACC减速，报接管"
    return {
        "plan": build_issue_focus_plan(title=title),
        "gate": {},
        "hard_stop": False,
    }


def _submission_title_claim(
    *,
    source_title: str,
    receipt_title: str | None = None,
    receipt_title_sha256: str | None = None,
):
    result = {"success": True}
    if receipt_title is not None:
        result["work_item"] = {
            "title": receipt_title,
            "title_sha256": (
                receipt_title_sha256
                if receipt_title_sha256 is not None
                else collector.issue_title_sha256(receipt_title)
            ),
        }
    return SimpleNamespace(
        submission_payload={
            "trigger_context": {
                "schema_version": "pnc_rca_trigger_context_v1",
                "source_kind": "feishu_group_manual",
                "creation_rule_version": "rca-rule-v1",
                "project_key": "t03o4q",
                "project_simple_name": "g1q3",
                "work_item_type_key": "issue",
                "work_item_id": "7065539652",
                "issue_url": (
                    "https://project.feishu.cn/g1q3/issue/detail/7065539652"
                ),
                "title": source_title,
            }
        },
        submission_result=result,
    )


def test_submission_issue_title_accepts_bound_receipt_fallback():
    claim = _submission_title_claim(
        source_title="",
        receipt_title="ACC-右车近距离切入ACC不减速",
    )

    assert collector._submission_issue_title(claim) == (
        "ACC-右车近距离切入ACC不减速"
    )


def test_submission_issue_title_rejects_receipt_hash_mismatch():
    claim = _submission_title_claim(
        source_title="",
        receipt_title="ACC-右车近距离切入ACC不减速",
        receipt_title_sha256="f" * 64,
    )

    with pytest.raises(
        DeliveryContractError,
        match="submission_receipt_identity_mismatch",
    ):
        collector._submission_issue_title(claim)


def test_submission_issue_title_rejects_original_receipt_conflict():
    claim = _submission_title_claim(
        source_title="原始问题标题",
        receipt_title="另一个问题标题",
    )

    with pytest.raises(
        DeliveryContractError,
        match="submission_receipt_identity_mismatch",
    ):
        collector._submission_issue_title(claim)


def test_submission_issue_title_keeps_matching_original_authoritative():
    claim = _submission_title_claim(
        source_title="原始问题标题",
        receipt_title="原始问题标题",
    )

    assert collector._submission_issue_title(claim) == "原始问题标题"


def test_submission_issue_title_without_original_or_receipt_remains_missing():
    claim = _submission_title_claim(source_title="")

    with pytest.raises(
        DeliveryContractError,
        match="submission_issue_title_missing",
    ):
        collector._submission_issue_title(claim)


def _manifest_row(path, role, raw, media_type):
    return {
        "role": role,
        "path": path,
        "size": len(raw),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "media_type": media_type,
        "required": True,
    }


def _run_remote_bundle_reader(
    tmp_path,
    monkeypatch,
    *,
    report_path="sealed-safe.json",
    report_value=None,
    extra_rows=(),
    script_transform=lambda script: script,
):
    submission_key = "g1q3-rca-s1-" + "e" * 64
    root = tmp_path / "bundle"
    root.mkdir()
    html_raw = b"<!doctype html><html><body>sealed report</body></html>"
    report_value = report_value or {
        "input_materialized": False,
        "failure_class": "remote_event_not_found",
        "event_uuid": "sealed-safe",
    }
    report_raw = _json_bytes(report_value)
    (root / "index.html").write_bytes(html_raw)
    (root / report_path).write_bytes(report_raw)
    rows = [
        _manifest_row("index.html", "index_html", html_raw, "text/html"),
        _manifest_row(report_path, "report_data", report_raw, "application/json"),
        *extra_rows,
    ]
    (root / "delivery_contract.json").write_bytes(_json_bytes({"artifacts": {}}))
    (root / "delivery_manifest.json").write_bytes(_json_bytes({"artifacts": rows}))
    monkeypatch.setattr(
        collector, "canonical_artifact_root", lambda _key: str(root) + "/"
    )
    monkeypatch.setattr(
        collector,
        "canonical_viz_mcap_path",
        lambda key: str(tmp_path / "viz" / f"{key}.viz.mcap"),
    )
    script = script_transform(collector._remote_bundle_script(submission_key))
    process = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert process.returncode == 0, process.stderr
    return json.loads(process.stdout)


def _init_identity_repo(root, files):
    root.mkdir(parents=True)
    for relative, raw in files.items():
        path = root.joinpath(*PurePosixPath(relative).parts)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
    subprocess.run(["/usr/bin/git", "init", "-q", str(root)], check=True)
    subprocess.run(["/usr/bin/git", "-C", str(root), "add", "."], check=True)
    subprocess.run(
        [
            "/usr/bin/git",
            "-C",
            str(root),
            "-c",
            "user.name=collector-test",
            "-c",
            "user.email=collector-test@example.invalid",
            "commit",
            "-qm",
            "identity fixture",
        ],
        check=True,
    )

    def rev_parse(revision):
        return subprocess.run(
            [
                "/usr/bin/git",
                "-C",
                str(root),
                "rev-parse",
                "--verify",
                revision,
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    return {"commit": rev_parse("HEAD"), "tree": rev_parse("HEAD^{tree}")}


def _execution_identity_reader_fixture(tmp_path, monkeypatch):
    submission_key = "g1q3-rca-s1-" + "e" * 64
    worker_root = tmp_path / "worker-state"
    pipeline_root = tmp_path / "pipeline-runtime"
    worker_entrypoint = worker_root / collector.REMOTE_WORKER_ENTRYPOINT_RELATIVE
    service_entrypoint = pipeline_root.joinpath(
        *PurePosixPath(collector.REMOTE_PIPELINE_ENTRYPOINT_RELATIVE).parts
    )
    report_entrypoint = pipeline_root.joinpath(
        *PurePosixPath(collector.REMOTE_REPORT_ENTRYPOINT_RELATIVE).parts
    )
    worker_raw = b"#!/usr/bin/env python3\nprint('worker')\n"
    service_raw = b"#!/usr/bin/env python3\nprint('service')\n"
    report_raw = b"#!/usr/bin/env python3\nprint('report')\n"
    worker_identity = _init_identity_repo(
        worker_root,
        {collector.REMOTE_WORKER_ENTRYPOINT_RELATIVE: worker_raw},
    )
    pipeline_identity = _init_identity_repo(
        pipeline_root,
        {
            collector.REMOTE_PIPELINE_ENTRYPOINT_RELATIVE: service_raw,
            collector.REMOTE_REPORT_ENTRYPOINT_RELATIVE: report_raw,
        },
    )
    shared_root = tmp_path / "shared-state"
    report_manifest_path = tmp_path / "config" / "report-runtime-manifest.json"
    monkeypatch.setattr(collector, "REMOTE_SHARED_STATE_ROOT", str(shared_root))
    monkeypatch.setattr(collector, "REMOTE_WORKER_REPO_ROOT", str(worker_root))
    monkeypatch.setattr(
        collector,
        "REMOTE_REPORT_RUNTIME_MANIFEST_PATH",
        str(report_manifest_path),
    )
    output_root = tmp_path / "bundle"
    worker_result = {
        "schema_version": "g1q3_rca_worker_result_v1",
        "task_id": submission_key,
        "rca_submission_key": submission_key,
        "execution_route": "rca_direct_cli",
        "repo_root": str(pipeline_root),
        "execution_attestation": {
            "schema_version": "g1q3_rca_worker_execution_attestation_v2",
            "task_id": submission_key,
            "available": True,
            "agent_backend": "none",
            "cwd": str(pipeline_root),
            "worker_source_commit": worker_identity["commit"],
            "worker_tree_clean": True,
            "worker_entrypoint_path": str(worker_entrypoint),
            "worker_entrypoint_sha256": hashlib.sha256(worker_raw).hexdigest(),
        },
    }
    service_result = {
        "schema_version": "g1q3_rca_service_result_v2",
        "task_id": submission_key,
        "output_dir": str(output_root),
        "success": True,
        "status": "completed",
        "service_provenance": {
            "schema_version": "g1q3_rca_service_provenance_v2",
            "available": True,
            "identity_kind": collector.IDENTITY_KIND_GIT_WORKTREE,
            "vm_source_commit": pipeline_identity["commit"],
            "vm_tree_clean": True,
            "service_entrypoint_path": str(service_entrypoint),
            "service_entrypoint_sha256": hashlib.sha256(service_raw).hexdigest(),
        },
    }
    report_manifest = {
        "schema_version": "pnc_rca_report_manifest_v1",
        "runtime_root": str(pipeline_root),
        "pipeline_commit": pipeline_identity["commit"],
        "pipeline_tree": pipeline_identity["tree"],
        "report_script_sha256": hashlib.sha256(report_raw).hexdigest(),
    }
    fixture = {
        "submission_key": submission_key,
        "worker_root": worker_root,
        "pipeline_root": pipeline_root,
        "worker_entrypoint": worker_entrypoint,
        "service_entrypoint": service_entrypoint,
        "report_entrypoint": report_entrypoint,
        "worker_identity": worker_identity,
        "pipeline_identity": pipeline_identity,
        "worker_result": worker_result,
        "service_result": service_result,
        "report_manifest": report_manifest,
        "report_manifest_path": report_manifest_path,
        "script_mutator": lambda script: script,
    }

    def write_identity_inputs(script):
        result_path = shared_root / "tasks" / submission_key / "result.md"
        result_path.parent.mkdir(parents=True, exist_ok=True)
        result_path.write_text(
            f"# Result: {submission_key}\n\n"
            "## Result JSON\n\n"
            "```json\n"
            + json.dumps(fixture["worker_result"], sort_keys=True)
            + "\n```\n",
            encoding="utf-8",
        )
        (output_root / "rca_service_result.json").write_bytes(
            _json_bytes(fixture["service_result"])
        )
        report_manifest_path.parent.mkdir(parents=True, exist_ok=True)
        report_manifest_path.write_bytes(_json_bytes(fixture["report_manifest"]))
        return fixture["script_mutator"](script)

    fixture["script_transform"] = write_identity_inputs
    return fixture


def _frozen_pipeline_identity_reader_fixture(tmp_path, monkeypatch):
    submission_key = "g1q3-rca-s1-" + "e" * 64
    worker_root = tmp_path / "worker-state"
    worker_entrypoint = worker_root / collector.REMOTE_WORKER_ENTRYPOINT_RELATIVE
    worker_raw = b"#!/usr/bin/env python3\nprint('worker')\n"
    worker_identity = _init_identity_repo(
        worker_root,
        {collector.REMOTE_WORKER_ENTRYPOINT_RELATIVE: worker_raw},
    )

    runtime_base = tmp_path / "hermes" / "rca-prod-runtime"
    pipeline_root = runtime_base / "releases" / "pipeline-runtime"
    service_entrypoint = pipeline_root.joinpath(
        *PurePosixPath(collector.REMOTE_PIPELINE_ENTRYPOINT_RELATIVE).parts
    )
    report_entrypoint = pipeline_root.joinpath(
        *PurePosixPath(collector.REMOTE_REPORT_ENTRYPOINT_RELATIVE).parts
    )
    service_raw = b"#!/usr/bin/env python3\nprint('service')\n"
    report_raw = b"#!/usr/bin/env python3\nprint('report')\n"
    service_entrypoint.parent.mkdir(parents=True, exist_ok=True)
    service_entrypoint.write_bytes(service_raw)
    report_entrypoint.write_bytes(report_raw)
    for directory in sorted(
        (path for path in pipeline_root.rglob("*") if path.is_dir()),
        key=lambda path: len(path.parts),
    ):
        directory.chmod(0o555)
    pipeline_root.chmod(0o555)
    service_entrypoint.chmod(0o444)
    report_entrypoint.chmod(0o444)

    def snapshot_entries(root):
        entries = []
        for path in sorted(root.rglob("*"), key=lambda value: value.as_posix()):
            relative = path.relative_to(root).as_posix()
            if path.is_dir():
                entries.append(
                    {
                        "path": relative,
                        "type": "directory",
                        "mode": "%04o" % (path.stat().st_mode & 0o7777),
                        "size_bytes": 0,
                        "sha256": None,
                    }
                )
            else:
                raw = path.read_bytes()
                entries.append(
                    {
                        "path": relative,
                        "type": "file",
                        "mode": "%04o" % (path.stat().st_mode & 0o7777),
                        "size_bytes": len(raw),
                        "sha256": hashlib.sha256(raw).hexdigest(),
                    }
                )
        return entries

    root_info = pipeline_root.stat()
    expected_commit = "5" * 40
    expected_tree = "6" * 40
    release_id = "rca-pipeline-fixture-1"
    authority_sha256 = "7" * 64
    pipeline_remote = "git@git.minieye.tech:pdcl/yj-evaluation-server.git"
    pipeline_tag = "rca-pipeline-fixture-1"
    root_identity = {
        "dev": root_info.st_dev,
        "gid": root_info.st_gid,
        "ino": root_info.st_ino,
        "mode": "%04o" % (root_info.st_mode & 0o7777),
        "uid": root_info.st_uid,
    }
    source_receipt = {
        "bootstrap": {},
        "entries": snapshot_entries(pipeline_root),
        "gitlinks": [],
        "gitlinks_policy": "materialize_recursive_v1",
        "max_runtime_bytes": 1024 * 1024 * 1024,
        "pipeline_commit": expected_commit,
        "pipeline_remote": pipeline_remote,
        "pipeline_tag": pipeline_tag,
        "pipeline_tree": expected_tree,
        "release_id": release_id,
        "root_identity": root_identity,
        "runtime_root": str(pipeline_root),
        "schema_version": "g1q3_rca_vm_source_materialization_v1",
    }
    source_receipt["self_seal"] = hashlib.sha256(
        json.dumps(
            source_receipt,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    source_canonical = json.dumps(
        source_receipt,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    source_raw = source_canonical + b"\n"
    receipt_dir = runtime_base / "receipts" / "fixture-1"
    receipt_dir.mkdir(parents=True)
    source_receipt_path = receipt_dir / "source-materialization.json"
    binding_receipt_path = receipt_dir / "worker-binding.json"
    receipt_report_path = receipt_dir / "report-runtime-manifest.json"
    report_manifest_path = tmp_path / "config" / "report-runtime-manifest.json"
    report_entry_raw = report_raw
    report_manifest = {
        "schema_version": "pnc_rca_report_manifest_v1",
        "release_id": release_id,
        "authority_sha256": authority_sha256,
        "root": "/mnt/tmp",
        "runtime_root": str(pipeline_root),
        "pipeline_commit": expected_commit,
        "pipeline_tree": expected_tree,
        "report_script_sha256": hashlib.sha256(report_entry_raw).hexdigest(),
    }
    report_raw = json.dumps(
        report_manifest,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8") + b"\n"
    report_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    report_manifest_path.write_bytes(report_raw)
    receipt_report_path.write_bytes(report_raw)
    binding = {
        "authority_sha256": authority_sha256,
        "binding_receipt_path": str(binding_receipt_path),
        "bootstrap_check_sha256": "8" * 64,
        "bootstrap_install_offline_sha256": "9" * 64,
        "gitlinks": [],
        "gitlinks_policy": "materialize_recursive_v1",
        "materialization_manifest_path": str(source_receipt_path),
        "materialization_manifest_semantic_sha256": hashlib.sha256(
            source_canonical
        ).hexdigest(),
        "materialization_manifest_sha256": hashlib.sha256(source_raw).hexdigest(),
        "max_runtime_bytes": 1024 * 1024 * 1024,
        "pipeline_commit": expected_commit,
        "pipeline_remote": pipeline_remote,
        "pipeline_tag": pipeline_tag,
        "pipeline_tree": expected_tree,
        "release_id": release_id,
        "report_manifest_path": str(receipt_report_path),
        "report_manifest_semantic_sha256": hashlib.sha256(
            report_raw[:-1]
        ).hexdigest(),
        "report_manifest_sha256": hashlib.sha256(report_raw).hexdigest(),
        "root_identity": root_identity,
        "runtime_root": str(pipeline_root),
        "schema_version": "g1q3_rca_vm_worker_binding_v1",
    }
    binding["self_seal"] = hashlib.sha256(
        json.dumps(
            binding,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    binding_raw = json.dumps(
        binding, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8") + b"\n"
    source_receipt_path.write_bytes(source_raw)
    binding_receipt_path.write_bytes(binding_raw)
    for path in receipt_dir.iterdir():
        path.chmod(0o444)
    receipt_dir.chmod(0o555)

    monkeypatch.setattr(collector, "REMOTE_SHARED_STATE_ROOT", str(tmp_path / "shared-state"))
    monkeypatch.setattr(collector, "REMOTE_WORKER_REPO_ROOT", str(worker_root))
    monkeypatch.setattr(collector, "REMOTE_PIPELINE_RUNTIME_ROOT", str(runtime_base))
    monkeypatch.setattr(
        collector, "REMOTE_REPORT_RUNTIME_MANIFEST_PATH", str(report_manifest_path)
    )
    output_root = tmp_path / "bundle"
    worker_result = {
        "schema_version": "g1q3_rca_worker_result_v1",
        "task_id": submission_key,
        "rca_submission_key": submission_key,
        "execution_route": "rca_direct_cli",
        "repo_root": str(pipeline_root),
        "execution_attestation": {
            "schema_version": "g1q3_rca_worker_execution_attestation_v2",
            "task_id": submission_key,
            "available": True,
            "agent_backend": "none",
            "cwd": str(pipeline_root),
            "worker_source_commit": worker_identity["commit"],
            "worker_tree_clean": True,
            "worker_entrypoint_path": str(worker_entrypoint),
            "worker_entrypoint_sha256": hashlib.sha256(worker_raw).hexdigest(),
        },
    }
    service_result = {
        "schema_version": "g1q3_rca_service_result_v2",
        "task_id": submission_key,
        "output_dir": str(output_root),
        "success": True,
        "status": "completed",
        "service_provenance": {
            "schema_version": "g1q3_rca_service_provenance_v2",
            "available": True,
            "identity_kind": collector.IDENTITY_KIND_SEALED_MATERIALIZED,
            "vm_source_commit": expected_commit,
            "vm_tree_clean": True,
            "service_entrypoint_path": str(service_entrypoint),
            "service_entrypoint_sha256": hashlib.sha256(service_raw).hexdigest(),
        },
    }
    fixture = {
        "submission_key": submission_key,
        "worker_root": worker_root,
        "pipeline_root": pipeline_root,
        "worker_entrypoint": worker_entrypoint,
        "service_entrypoint": service_entrypoint,
        "report_entrypoint": report_entrypoint,
        "worker_identity": worker_identity,
        "pipeline_identity": {"commit": expected_commit, "tree": expected_tree},
        "worker_result": worker_result,
        "service_result": service_result,
        "report_manifest": report_manifest,
        "report_manifest_path": report_manifest_path,
        "source_receipt_path": source_receipt_path,
        "binding_receipt_path": binding_receipt_path,
        "script_mutator": lambda script: script,
    }

    def write_identity_inputs(script):
        result_path = (
            tmp_path / "shared-state" / "tasks" / submission_key / "result.md"
        )
        result_path.parent.mkdir(parents=True, exist_ok=True)
        result_path.write_text(
            f"# Result: {submission_key}\n\n"
            "## Result JSON\n\n"
            "```json\n"
            + json.dumps(fixture["worker_result"], sort_keys=True)
            + "\n```\n",
            encoding="utf-8",
        )
        (output_root / "rca_service_result.json").parent.mkdir(
            parents=True, exist_ok=True
        )
        (output_root / "rca_service_result.json").write_bytes(
            _json_bytes(fixture["service_result"])
        )
        return fixture["script_mutator"](script)

    fixture["script_transform"] = write_identity_inputs
    return fixture


def test_remote_bundle_reader_uses_formal_viz_publication_root():
    submission_key = "g1q3-rca-s1-" + "a" * 64
    formal_root = str(PurePosixPath(canonical_viz_mcap_path(submission_key)).parent)
    script = collector._remote_bundle_script(submission_key)

    assert f"FORMAL_VIZ_ROOT = {formal_root!r}" in script
    assert "FORMAL_VIZ_ROOT = posixpath.normpath(ROOT)" not in script


def test_remote_bundle_reader_scans_sealed_public_artifacts_for_banned_phrases():
    script = collector._remote_bundle_script("g1q3-rca-s1-" + "c" * 64)

    assert repr(tuple(collector.BANNED_PUBLIC_PHRASES)) in script
    assert "raise RuntimeError('public_artifact_banned_phrase')" in script
    assert "json.dumps(report_data, ensure_ascii=False, sort_keys=True)" in script
    assert "reject_banned_public_phrase(text)" in script
    assert "except RuntimeError:\n            report_data = {}" not in script
    assert "report_data_missing" in collector._EVENTUAL_ARTIFACT_CODES


def test_remote_bundle_reader_uses_manifest_report_instead_of_fixed_filename(
    tmp_path, monkeypatch
):
    root = tmp_path / "bundle"

    def add_unsafe_fixed_file(script):
        (root / "report_data.json").write_bytes(
            _json_bytes({"conclusion": "ACC 是责任方", "event_uuid": "unsafe"})
        )
        return script

    payload = _run_remote_bundle_reader(
        tmp_path,
        monkeypatch,
        script_transform=add_unsafe_fixed_file,
    )

    assert payload["ok"] is True
    assert payload["gate_a_source"]["event_uuid"] == "sealed-safe"
    assert "read_json(ROOT + 'report_data.json'" not in collector._remote_bundle_script(
        "g1q3-rca-s1-" + "f" * 64
    )


def test_remote_bundle_reader_returns_manifest_bound_issue_focus(tmp_path, monkeypatch):
    focus = {"schema_version": "focus-fixture-v1", "analysis_status": "complete"}

    payload = _run_remote_bundle_reader(
        tmp_path,
        monkeypatch,
        report_value={
            "input_materialized": True,
            "event_uuid": "focus-fixture",
            "issue_focus": focus,
        },
    )

    assert payload["ok"] is True
    assert payload["report_issue_focus"] == focus


def test_remote_bundle_reader_parses_canonical_worker_result_markdown(
    tmp_path, monkeypatch
):
    submission_key = "g1q3-rca-s1-" + "e" * 64
    shared_root = tmp_path / "shared-state"
    result_path = shared_root / "tasks" / submission_key / "result.md"
    result_path.parent.mkdir(parents=True)
    result_path.write_bytes(
        (
            f"# Result: {submission_key}\n\n"
            "## Result JSON\n\n"
            "```json\n{}\n```\n"
        ).encode()
    )
    monkeypatch.setattr(collector, "REMOTE_SHARED_STATE_ROOT", str(shared_root))

    payload = _run_remote_bundle_reader(tmp_path, monkeypatch)

    assert payload["execution_identity_evidence"] is None
    assert payload["execution_identity_error"] == "service_terminal_receipt_missing"


def test_remote_bundle_reader_reads_actual_git_and_entrypoint_identity(
    tmp_path, monkeypatch
):
    fixture = _execution_identity_reader_fixture(tmp_path, monkeypatch)

    payload = _run_remote_bundle_reader(
        tmp_path,
        monkeypatch,
        script_transform=fixture["script_transform"],
    )

    evidence = payload["execution_identity_evidence"]
    assert payload["execution_identity_error"] == ""
    assert evidence["worker"]["commit"] == fixture["worker_identity"]["commit"]
    assert evidence["worker"]["tree"] == fixture["worker_identity"]["tree"]
    assert evidence["worker"]["clean"] is True
    assert evidence["worker"]["entrypoint_sha256"] == hashlib.sha256(
        fixture["worker_entrypoint"].read_bytes()
    ).hexdigest()
    assert evidence["pipeline"]["commit"] == fixture["pipeline_identity"]["commit"]
    assert evidence["pipeline"]["tree"] == fixture["pipeline_identity"]["tree"]
    assert evidence["pipeline"]["clean"] is True
    assert evidence["pipeline"]["entrypoint_path"] == str(
        fixture["service_entrypoint"]
    )
    assert evidence["report_service"]["report_script_sha256"] == hashlib.sha256(
        fixture["report_entrypoint"].read_bytes()
    ).hexdigest()
    assert evidence["report_service"]["manifest_sha256"] == hashlib.sha256(
        fixture["report_manifest_path"].read_bytes()
    ).hexdigest()


def test_remote_bundle_reader_accepts_sealed_frozen_pipeline_runtime(
    tmp_path, monkeypatch
):
    fixture = _frozen_pipeline_identity_reader_fixture(tmp_path, monkeypatch)

    payload = _run_remote_bundle_reader(
        tmp_path,
        monkeypatch,
        script_transform=fixture["script_transform"],
    )

    assert payload["execution_identity_error"] == ""
    evidence = payload["execution_identity_evidence"]
    assert evidence["pipeline"]["commit"] == fixture["pipeline_identity"]["commit"]
    assert evidence["pipeline"]["tree"] == fixture["pipeline_identity"]["tree"]
    assert evidence["pipeline"]["clean"] is True


def _record_generated_git_calls(tmp_path, fixture):
    calls_path = tmp_path / "generated-git-calls.jsonl"
    wrapper = tmp_path / "record-git"
    wrapper.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        f"path = {str(calls_path)!r}\n"
        "with open(path, 'a', encoding='utf-8') as handle:\n"
        "    handle.write(json.dumps(sys.argv[1:]) + '\\n')\n"
        "os.execv('/usr/bin/git', ['/usr/bin/git', *sys.argv[1:]])\n",
        encoding="utf-8",
    )
    wrapper.chmod(0o755)
    fixture["script_mutator"] = lambda script: script.replace(
        "['/usr/bin/git', '-C'", f"[{str(wrapper)!r}, '-C'"
    )
    return calls_path


def test_remote_bundle_sealed_pipeline_invokes_no_git_for_pipeline_target(
    tmp_path, monkeypatch
):
    fixture = _frozen_pipeline_identity_reader_fixture(tmp_path, monkeypatch)
    calls_path = _record_generated_git_calls(tmp_path, fixture)

    payload = _run_remote_bundle_reader(
        tmp_path,
        monkeypatch,
        script_transform=fixture["script_transform"],
    )

    assert payload["execution_identity_error"] == ""
    calls = [json.loads(line) for line in calls_path.read_text().splitlines()]
    assert calls
    assert all(str(fixture["pipeline_root"]) not in call for call in calls)
    assert any(str(fixture["worker_root"]) in call for call in calls)


def test_remote_bundle_unknown_pipeline_kind_fails_before_any_git(
    tmp_path, monkeypatch
):
    fixture = _execution_identity_reader_fixture(tmp_path, monkeypatch)
    fixture["service_result"]["service_provenance"]["identity_kind"] = (
        collector.IDENTITY_KIND_UNKNOWN
    )
    calls_path = _record_generated_git_calls(tmp_path, fixture)

    payload = _run_remote_bundle_reader(
        tmp_path,
        monkeypatch,
        script_transform=fixture["script_transform"],
    )

    assert payload["execution_identity_evidence"] is None
    assert payload["execution_identity_error"] == (
        "service_terminal_receipt_identity_invalid"
    )
    assert not calls_path.exists()


@pytest.mark.parametrize(
    "contract_drift", ["missing_kind", "v1_schema", "extra_field"]
)
def test_remote_bundle_legacy_or_untyped_service_provenance_fails_before_git(
    tmp_path, monkeypatch, contract_drift
):
    fixture = _execution_identity_reader_fixture(tmp_path, monkeypatch)
    provenance = fixture["service_result"]["service_provenance"]
    if contract_drift == "missing_kind":
        provenance.pop("identity_kind")
    elif contract_drift == "v1_schema":
        provenance["schema_version"] = "g1q3_rca_service_provenance_v1"
    else:
        provenance["guessed_identity"] = "sealed"
    calls_path = _record_generated_git_calls(tmp_path, fixture)

    payload = _run_remote_bundle_reader(
        tmp_path,
        monkeypatch,
        script_transform=fixture["script_transform"],
    )

    assert payload["execution_identity_evidence"] is None
    assert payload["execution_identity_error"] == (
        "service_terminal_receipt_identity_invalid"
    )
    assert not calls_path.exists()


@pytest.mark.parametrize("contract_drift", ["extra_kind", "v2_schema"])
@pytest.mark.parametrize("receipt_face", ["source", "binding"])
def test_remote_bundle_nonexact_sealed_receipt_pair_fails_before_git(
    tmp_path, monkeypatch, contract_drift, receipt_face
):
    fixture = _frozen_pipeline_identity_reader_fixture(tmp_path, monkeypatch)
    receipt_path = fixture[f"{receipt_face}_receipt_path"]
    receipt = json.loads(receipt_path.read_text())
    receipt.pop("self_seal")
    if contract_drift == "extra_kind":
        receipt["identity_kind"] = collector.IDENTITY_KIND_SEALED_MATERIALIZED
    else:
        receipt["schema_version"] = (
            "g1q3_rca_vm_source_materialization_v2"
            if receipt_face == "source"
            else "g1q3_rca_vm_worker_binding_v2"
        )
    receipt["self_seal"] = hashlib.sha256(
        json.dumps(
            receipt,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    receipt_path.chmod(0o644)
    receipt_path.write_text(
        json.dumps(receipt, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n",
        encoding="utf-8",
    )
    receipt_path.chmod(0o444)
    calls_path = _record_generated_git_calls(tmp_path, fixture)

    payload = _run_remote_bundle_reader(
        tmp_path,
        monkeypatch,
        script_transform=fixture["script_transform"],
    )

    assert payload["execution_identity_evidence"] is None
    assert payload["execution_identity_error"] == "pipeline_frozen_receipt_invalid"
    assert not calls_path.exists()


def test_remote_bundle_reader_rejects_tampered_frozen_materialization_receipt(
    tmp_path, monkeypatch
):
    fixture = _frozen_pipeline_identity_reader_fixture(tmp_path, monkeypatch)
    receipt = json.loads(fixture["source_receipt_path"].read_text())
    receipt["pipeline_tree"] = "f" * 40
    fixture["source_receipt_path"].chmod(0o644)
    fixture["source_receipt_path"].write_text(
        json.dumps(receipt, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n",
        encoding="utf-8",
    )

    payload = _run_remote_bundle_reader(
        tmp_path,
        monkeypatch,
        script_transform=fixture["script_transform"],
    )

    assert payload["execution_identity_evidence"] is None
    assert payload["execution_identity_error"] == "pipeline_frozen_receipt_invalid"


def test_remote_bundle_reader_ignores_inherited_git_environment(
    tmp_path, monkeypatch
):
    fixture = _execution_identity_reader_fixture(tmp_path, monkeypatch)
    hostile = tmp_path / "hostile-git-override"
    for key, value in {
        "GIT_DIR": str(hostile / "git-dir"),
        "GIT_WORK_TREE": str(hostile / "work-tree"),
        "GIT_OBJECT_DIRECTORY": str(hostile / "objects"),
        "GIT_ALTERNATE_OBJECT_DIRECTORIES": str(hostile / "alternates"),
        "GIT_INDEX_FILE": str(hostile / "index"),
        "GIT_CONFIG_COUNT": "1",
        "GIT_CONFIG_KEY_0": "core.repositoryformatversion",
        "GIT_CONFIG_VALUE_0": "999",
    }.items():
        monkeypatch.setenv(key, value)

    payload = _run_remote_bundle_reader(
        tmp_path,
        monkeypatch,
        script_transform=fixture["script_transform"],
    )

    assert payload["execution_identity_error"] == ""
    assert payload["execution_identity_evidence"]["worker"]["commit"] == (
        fixture["worker_identity"]["commit"]
    )
    assert payload["execution_identity_evidence"]["pipeline"]["commit"] == (
        fixture["pipeline_identity"]["commit"]
    )


def test_remote_bundle_reader_detects_untracked_files_despite_local_git_config(
    tmp_path, monkeypatch
):
    fixture = _execution_identity_reader_fixture(tmp_path, monkeypatch)
    subprocess.run(
        [
            "/usr/bin/git",
            "-C",
            str(fixture["worker_root"]),
            "config",
            "status.showUntrackedFiles",
            "no",
        ],
        check=True,
    )
    (fixture["worker_root"] / "untracked-runtime-hook.py").write_text(
        "raise SystemExit('must be detected')\n",
        encoding="utf-8",
    )

    payload = _run_remote_bundle_reader(
        tmp_path,
        monkeypatch,
        script_transform=fixture["script_transform"],
    )

    assert payload["execution_identity_evidence"] is None
    assert payload["execution_identity_error"] == "worker_git_dirty"


@pytest.mark.parametrize("face", ["worker", "pipeline"])
def test_remote_bundle_reader_rejects_receipt_head_drift(tmp_path, monkeypatch, face):
    fixture = _execution_identity_reader_fixture(tmp_path, monkeypatch)
    if face == "worker":
        fixture["worker_result"]["execution_attestation"][
            "worker_source_commit"
        ] = "f" * 40
    else:
        fixture["service_result"]["service_provenance"]["vm_source_commit"] = (
            "f" * 40
        )

    payload = _run_remote_bundle_reader(
        tmp_path,
        monkeypatch,
        script_transform=fixture["script_transform"],
    )

    assert payload["execution_identity_evidence"] is None
    assert payload["execution_identity_error"] == f"{face}_git_head_receipt_mismatch"


@pytest.mark.parametrize("face", ["worker", "pipeline"])
def test_remote_bundle_reader_rejects_dirty_runtime(tmp_path, monkeypatch, face):
    fixture = _execution_identity_reader_fixture(tmp_path, monkeypatch)
    (fixture[f"{face}_root"] / "untracked.txt").write_text(
        "dirty", encoding="utf-8"
    )

    payload = _run_remote_bundle_reader(
        tmp_path,
        monkeypatch,
        script_transform=fixture["script_transform"],
    )

    assert payload["execution_identity_evidence"] is None
    assert payload["execution_identity_error"] == f"{face}_git_dirty"


@pytest.mark.parametrize("face", ["worker", "pipeline"])
def test_remote_bundle_reader_rejects_entrypoint_hash_mismatch(
    tmp_path, monkeypatch, face
):
    fixture = _execution_identity_reader_fixture(tmp_path, monkeypatch)
    if face == "worker":
        fixture["worker_result"]["execution_attestation"][
            "worker_entrypoint_sha256"
        ] = "f" * 64
    else:
        fixture["service_result"]["service_provenance"][
            "service_entrypoint_sha256"
        ] = "f" * 64

    payload = _run_remote_bundle_reader(
        tmp_path,
        monkeypatch,
        script_transform=fixture["script_transform"],
    )

    assert payload["execution_identity_evidence"] is None
    expected_prefix = "worker" if face == "worker" else "service"
    assert payload["execution_identity_error"] == (
        f"{expected_prefix}_entrypoint_sha256_mismatch"
    )


def test_remote_bundle_reader_rejects_report_script_hash_mismatch(
    tmp_path, monkeypatch
):
    fixture = _execution_identity_reader_fixture(tmp_path, monkeypatch)
    fixture["report_manifest"]["report_script_sha256"] = "f" * 64

    payload = _run_remote_bundle_reader(
        tmp_path,
        monkeypatch,
        script_transform=fixture["script_transform"],
    )

    assert payload["execution_identity_evidence"] is None
    assert payload["execution_identity_error"] == "report_script_sha256_mismatch"


def test_remote_bundle_reader_rejects_service_entrypoint_path_traversal(
    tmp_path, monkeypatch
):
    fixture = _execution_identity_reader_fixture(tmp_path, monkeypatch)
    fixture["service_result"]["service_provenance"]["service_entrypoint_path"] = (
        str(fixture["pipeline_root"] / "api" / ".." / "escape.py")
    )

    payload = _run_remote_bundle_reader(
        tmp_path,
        monkeypatch,
        script_transform=fixture["script_transform"],
    )

    assert payload["execution_identity_evidence"] is None
    assert payload["execution_identity_error"] == (
        "service_terminal_receipt_identity_invalid"
    )


def test_remote_bundle_reader_rejects_git_identity_toctou(tmp_path, monkeypatch):
    fixture = _execution_identity_reader_fixture(tmp_path, monkeypatch)
    needle = (
        "    worker_identity_after = git_identity(\n"
        "        WORKER_REPO_ROOT, WORKER_IDENTITY_KIND, 'worker_git'\n"
        "    )"
    )

    def commit_during_read(script):
        assert needle in script
        mutation = (
            "    with open(WORKER_ENTRYPOINT_PATH, 'ab') as handle:\n"
            "        handle.write(b'\\n# identity drift\\n')\n"
            "    subprocess.run(\n"
            "        ['/usr/bin/git', '-C', WORKER_REPO_ROOT, 'add', "
            "WORKER_ENTRYPOINT_PATH], check=True\n"
            "    )\n"
            "    subprocess.run(\n"
            "        ['/usr/bin/git', '-C', WORKER_REPO_ROOT, "
            "'-c', 'user.name=collector-test', "
            "'-c', 'user.email=collector-test@example.invalid', "
            "'commit', '-qm', 'identity drift'], check=True\n"
            "    )\n"
        )
        return script.replace(needle, mutation + needle, 1)

    fixture["script_mutator"] = commit_during_read
    payload = _run_remote_bundle_reader(
        tmp_path,
        monkeypatch,
        script_transform=fixture["script_transform"],
    )

    assert payload["execution_identity_evidence"] is None
    assert payload["execution_identity_error"] == (
        "worker_git_identity_changed_during_read"
    )


def _vm_execution_identity_evidence(submission_key):
    artifact_root = f"/mnt/tmp/{submission_key}/"
    worker_root = "/home/mini/.hermes/worker-state"
    pipeline_root = "/home/mini/.hermes/rca-prod-runtime/releases/r15aw"
    return {
        "schema_version": collector.VM_EXECUTION_IDENTITY_EVIDENCE_SCHEMA_VERSION,
        "source": "canonical_vm_terminal_service_report_receipts",
        "task_id": submission_key,
        "submission_key": submission_key,
        "worker": {
            "commit": "1" * 40,
            "tree": "2" * 40,
            "runtime_root": worker_root,
            "clean": True,
            "entrypoint_path": worker_root + "/vm_coding_worker_v2.py",
            "entrypoint_sha256": "3" * 64,
            "receipt_path": (
                f"/home/mini/.hermes/shared-state/tasks/{submission_key}/result.md"
            ),
            "receipt_sha256": "4" * 64,
        },
        "pipeline": {
            "commit": "5" * 40,
            "tree": "6" * 40,
            "runtime_root": pipeline_root,
            "clean": True,
            "entrypoint_path": (
                pipeline_root + "/" + collector.REMOTE_PIPELINE_ENTRYPOINT_RELATIVE
            ),
            "entrypoint_sha256": "7" * 64,
            "receipt_path": artifact_root + "rca_service_result.json",
            "receipt_sha256": "8" * 64,
        },
        "report_service": {
            "manifest_path": collector.REMOTE_REPORT_RUNTIME_MANIFEST_PATH,
            "manifest_sha256": "9" * 64,
            "pipeline_commit": "5" * 40,
            "pipeline_tree": "6" * 40,
            "runtime_root": pipeline_root,
            "report_script_sha256": "a" * 64,
        },
        "delivery_manifest": {
            "path": artifact_root + "delivery_manifest.json",
            "sha256": "b" * 64,
        },
    }


def test_execution_identity_readback_binds_canonical_vm_receipt_paths_and_hashes():
    submission_key = "g1q3-rca-s1-" + "f" * 64
    evidence = _vm_execution_identity_evidence(submission_key)
    result = collector._execution_identity_readback(
        claim=SimpleNamespace(task_id=submission_key, submission_key=submission_key),
        bundle={
            "execution_identity_evidence": evidence,
            "execution_identity_error": "",
        },
        release_binding={
            "release_id": "rca-r15aw-20260817",
            "epoch_id": "rca-activation-r15aw-20260817",
            "release_fingerprint_sha256": "c" * 64,
            "release_note_sha256": "d" * 64,
        },
    )

    assert result["source"] == "host_collector_canonical_vm_receipts_v1"
    assert result["worker"]["receipt_path"].endswith("/result.md")
    assert result["worker"]["receipt_sha256"] == "4" * 64
    assert result["pipeline"]["receipt_sha256"] == "8" * 64
    assert result["report_service"]["manifest_sha256"] == "9" * 64
    assert result["delivery_manifest"]["sha256"] == "b" * 64


@pytest.mark.parametrize(
    ("section", "field", "value"),
    [
        ("worker", "clean", False),
        ("worker", "receipt_path", "/tmp/operator-filled.json"),
        (
            "pipeline",
            "entrypoint_path",
            "/home/mini/.hermes/rca-prod-runtime/releases/r15aw/../../escape.py",
        ),
        ("report_service", "manifest_sha256", "0" * 64),
        ("delivery_manifest", "path", "/tmp/delivery_manifest.json"),
    ],
)
def test_execution_identity_readback_rejects_noncanonical_evidence(
    section, field, value
):
    submission_key = "g1q3-rca-s1-" + "f" * 64
    evidence = _vm_execution_identity_evidence(submission_key)
    evidence[section][field] = value

    with pytest.raises(DeliveryContractError, match="execution_identity_readback_invalid"):
        collector._execution_identity_readback(
            claim=SimpleNamespace(task_id=submission_key, submission_key=submission_key),
            bundle={
                "execution_identity_evidence": evidence,
                "execution_identity_error": "",
            },
            release_binding={
                "release_id": "rca-r15aw-20260817",
                "epoch_id": "rca-activation-r15aw-20260817",
                "release_fingerprint_sha256": "c" * 64,
                "release_note_sha256": "d" * 64,
            },
        )


def test_remote_bundle_reader_rejects_missing_report_role(tmp_path, monkeypatch):
    def remove_report_row(script):
        manifest_path = tmp_path / "bundle" / "delivery_manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["artifacts"] = [manifest["artifacts"][0]]
        manifest_path.write_bytes(_json_bytes(manifest))
        return script

    payload = _run_remote_bundle_reader(
        tmp_path,
        monkeypatch,
        script_transform=remove_report_row,
    )

    assert payload["error_code"] == "required_report_data_artifact_missing"


def test_remote_bundle_reader_rejects_duplicate_report_role(tmp_path, monkeypatch):
    duplicate_raw = _json_bytes({"input_materialized": False})

    def add_duplicate_file(script):
        (tmp_path / "bundle" / "other.json").write_bytes(duplicate_raw)
        return script

    payload = _run_remote_bundle_reader(
        tmp_path,
        monkeypatch,
        extra_rows=(
            _manifest_row(
                "other.json", "report_data", duplicate_raw, "application/json"
            ),
        ),
        script_transform=add_duplicate_file,
    )

    assert payload["error_code"] == "delivery_manifest_duplicate_artifact"


def test_remote_bundle_reader_rejects_non_json_report_path(tmp_path, monkeypatch):
    payload = _run_remote_bundle_reader(
        tmp_path,
        monkeypatch,
        report_path="sealed-safe.txt",
    )

    assert payload["error_code"] == "required_report_data_artifact_invalid"


def test_remote_bundle_reader_rejects_report_identity_change(tmp_path, monkeypatch):
    def replace_report_during_read(script):
        report_path = tmp_path / "bundle" / "sealed-safe.json"
        replacement = type(report_path)(str(report_path) + ".replacement")
        replacement.write_bytes(report_path.read_bytes())
        injected = (
            "os.replace(path + '.replacement', path)\n        after = os.fstat(fd)"
        )
        prefix, separator, suffix = script.rpartition("after = os.fstat(fd)")
        assert separator
        return prefix + injected + suffix

    payload = _run_remote_bundle_reader(
        tmp_path,
        monkeypatch,
        script_transform=replace_report_during_read,
    )

    assert payload["error_code"] == "report_data_changed_during_read"


@pytest.mark.parametrize(
    ("field", "value", "error_code"),
    [
        ("size", 1, "report_data_size_mismatch"),
        ("sha256", "0" * 64, "report_data_hash_mismatch"),
    ],
)
def test_remote_bundle_reader_binds_report_size_and_hash(
    tmp_path, monkeypatch, field, value, error_code
):
    def corrupt_manifest_binding(script):
        manifest_path = tmp_path / "bundle" / "delivery_manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["artifacts"][1][field] = value
        manifest_path.write_bytes(_json_bytes(manifest))
        return script

    payload = _run_remote_bundle_reader(
        tmp_path,
        monkeypatch,
        script_transform=corrupt_manifest_binding,
    )

    assert payload["error_code"] == error_code


def test_remote_bundle_reader_rejects_malformed_json_report(tmp_path, monkeypatch):
    def replace_with_invalid_json(script):
        root = tmp_path / "bundle"
        raw = b"not-json"
        (root / "sealed-safe.json").write_bytes(raw)
        manifest_path = root / "delivery_manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["artifacts"][1].update({
            "size": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(),
        })
        manifest_path.write_bytes(_json_bytes(manifest))
        return script

    payload = _run_remote_bundle_reader(
        tmp_path,
        monkeypatch,
        script_transform=replace_with_invalid_json,
    )

    assert payload["error_code"] == "report_data_json_invalid"


def test_remote_bundle_reader_applies_json_size_limit_to_report(tmp_path, monkeypatch):
    def lower_report_limit(script):
        return script.replace(
            "MAX_JSON_BYTES if is_report_data else MAX_FILE_BYTES",
            "64 if is_report_data else MAX_FILE_BYTES",
        )

    payload = _run_remote_bundle_reader(
        tmp_path,
        monkeypatch,
        script_transform=lower_report_limit,
    )

    assert payload["error_code"] == "report_data_missing_size_invalid"


def test_remote_bundle_reader_binds_gate_a_source_and_safe_projection():
    script = collector._remote_bundle_script("g1q3-rca-s1-" + "d" * 64)

    assert "'gate_a_source'" in script
    assert "def public_report_projection" not in script
    assert "contract['public_result'] =" not in script


def test_host_gate_a_projection_replaces_candidate_bearing_contract():
    bundle = collector._apply_gate_a_bundle_projection({
        "delivery_contract": {
            "consumer_capability": _gate_a_capability(),
            "summary": {"short_conclusion": "candidate ACC"},
            "report": {"candidate_owner_domain": "ACC", "is_candidate": True},
        },
        "gate_a_source": {
            "input_materialized": True,
            "rca_evaluators": [
                {
                    "key": "aeb_trigger",
                    "status": "supported",
                    "evidence_refs": [
                        {
                            "signal": "AEBReq",
                            "evidence": "窗口内观测到 AEB 请求。",
                        }
                    ],
                },
            ],
        },
    })

    public = bundle["delivery_contract"]["public_result"]
    assert public["gate_a_level"] == "L1_observation"
    assert public["responsibility"]["candidate"] == "暂无法判断"
    assert "candidate_owner_domain" not in bundle["delivery_contract"]["report"]


def test_host_gate_a_projection_accepts_known_viz_build_abstention():
    bundle = collector._apply_gate_a_bundle_projection({
        "delivery_contract": {
            "summary": {"short_conclusion": "stale"},
            "report": {},
        },
        "gate_a_source": {
            "input_materialized": False,
            "materialization_attested": True,
            "failure_class": "viz_mcap_build_failed",
            "rca_evaluators": [],
        },
    })

    projection = bundle["delivery_contract"]["gate_a_projection"]
    assert projection["level"] == "L0_abstain"
    assert projection["abstention"]["failure_class"] == "viz_mcap_build_failed"


def test_host_gate_a_projection_accepts_evidence_not_ready_terminal():
    bundle = collector._apply_gate_a_bundle_projection({
        "delivery_contract": {
            "schema_version": "g1q3_delivery_contract_v1",
            "terminal_diagnostic": {
                "schema_version": "g1q3_rca_terminal_diagnostic_v1",
                "stage": "s5_evaluator",
                "blocker_kind": "evidence_not_ready",
            },
            "summary": {"short_conclusion": "stale"},
            "report": {},
        },
        "gate_a_source": {
            "input_materialized": False,
            "materialization_attested": True,
            "terminal_diagnostic": {
                "schema_version": "g1q3_rca_terminal_diagnostic_v1",
                "stage": "s5_evaluator",
                "blocker_kind": "evidence_not_ready",
            },
            "rca_evaluators": [],
        },
    })

    projection = bundle["delivery_contract"]["gate_a_projection"]
    assert projection["level"] == "L0_abstain"
    assert projection["abstention"]["failure_class"] == "evidence_not_ready"
    assert "未取得可用于归因" in projection["abstention"]["message"]


def test_host_gate_a_projection_rejects_unknown_evidence_terminal_code():
    with pytest.raises(DeliveryContractError, match="gate_a_projection_invalid"):
        collector._apply_gate_a_bundle_projection({
            "delivery_contract": {
                "terminal_diagnostic": {
                    "schema_version": "g1q3_rca_terminal_diagnostic_v1",
                    "stage": "s5_evaluator",
                    "blocker_kind": "evidence_not_ready",
                },
            },
            "gate_a_source": {
                "input_materialized": False,
                "materialization_attested": True,
                "failure_class": "evidence_not_ready_typo",
                "terminal_diagnostic": {
                    "stage": "s5_evaluator",
                    "blocker_kind": "evidence_not_ready",
                },
                "rca_evaluators": [],
            },
        })


def test_host_gate_a_projection_rejects_bare_evidence_failure_class():
    with pytest.raises(DeliveryContractError, match="gate_a_projection_invalid"):
        collector._apply_gate_a_bundle_projection({
            "delivery_contract": {},
            "gate_a_source": {
                "input_materialized": False,
                "materialization_attested": True,
                "failure_class": "evidence_not_ready",
                "rca_evaluators": [],
            },
        })


def test_host_gate_a_projection_rejects_contradictory_evidence_terminal():
    with pytest.raises(DeliveryContractError, match="terminal_conflict"):
        collector._apply_gate_a_bundle_projection({
            "delivery_contract": {
                "terminal_diagnostic": {
                    "schema_version": "g1q3_rca_terminal_diagnostic_v1",
                    "stage": "s5_evaluator",
                    "blocker_kind": "remote_event_not_found",
                },
            },
            "gate_a_source": {
                "input_materialized": False,
                "materialization_attested": True,
                "terminal_diagnostic": {
                    "stage": "s5_evaluator",
                    "blocker_kind": "evidence_not_ready",
                    "attribution_status": "attributable",
                },
                "rca_evaluators": [],
            },
        })


def test_host_gate_a_projection_rejects_attributable_evidence_terminal():
    with pytest.raises(DeliveryContractError, match="terminal_conflict"):
        collector._apply_gate_a_bundle_projection({
            "delivery_contract": {
                "terminal_diagnostic": {
                    "schema_version": "g1q3_rca_terminal_diagnostic_v1",
                    "stage": "s5_evaluator",
                    "blocker_kind": "evidence_not_ready",
                },
            },
            "gate_a_source": {
                "input_materialized": False,
                "materialization_attested": True,
                "terminal_diagnostic": {
                    "stage": "s5_evaluator",
                    "blocker_kind": "evidence_not_ready",
                    "attribution_status": "attributable",
                },
                "rca_evaluators": [],
            },
        })


def test_host_keeps_only_a_trusted_v19_primary_conclusion():
    contract = _trusted_v19_contract()

    bundle = collector._apply_gate_a_bundle_projection({
        "delivery_contract": contract,
        "report_issue_focus": _trusted_v19_focus(),
        "gate_a_source": {
            "input_materialized": True,
            "rca_evaluators": [
                {
                    "key": "aeb_trigger",
                    "status": "supported",
                    "evidence_refs": [{"signal": "AEBReq"}],
                }
            ],
        },
    })

    preserved = bundle["delivery_contract"]
    assert "gate_a_projection" not in preserved
    assert preserved["public_result"] == contract["public_result"]
    assert preserved["report"]["candidate_owner"] == "ACC 控制模块"
    assert preserved["artifacts"]["attribution_causal_text"]
    assert preserved["issue_focus"]["schema_version"] == (
        "g1q3_rca_v19_issue_focus_binding_v1"
    )


def test_host_rejects_trusted_v19_without_report_focus_binding():
    with pytest.raises(DeliveryContractError, match="issue_focus_report_binding_missing"):
        collector._apply_gate_a_bundle_projection({
            "delivery_contract": _trusted_v19_contract(),
            "gate_a_source": {
                "input_materialized": False,
                "materialization_attested": True,
                "failure_class": "remote_read_completeness_not_proven",
                "rca_evaluators": [],
            },
        })


def test_host_falls_back_to_gate_a_when_v19_primary_quality_is_not_trusted():
    contract = _trusted_v19_contract()
    contract["consumer_capability"]["integrated_sources"][
        "conclusion_communication"
    ]["quality_checks"]["causality_closed"] = False

    bundle = collector._apply_gate_a_bundle_projection({
        "delivery_contract": contract,
        "gate_a_source": {
            "input_materialized": True,
            "rca_evaluators": [
                {
                    "key": "aeb_trigger",
                    "status": "supported",
                    "evidence_refs": [{"signal": "AEBReq"}],
                }
            ],
        },
    })

    sanitized = bundle["delivery_contract"]
    assert sanitized["public_result"]["gate_a_level"] == "L1_observation"
    assert "candidate_owner" not in sanitized["report"]
    assert "attribution_causal_text" not in sanitized["artifacts"]


def test_trusted_v19_survives_full_delivery_verification(monkeypatch):
    from gateway.pnc_rca_delivery_contract import verify_delivery_bundle
    from tests.gateway.test_pnc_rca_delivery_contract import _bundle

    monkeypatch.setenv("PNC_FOXGLOVE_RENDER_HOST", "https://viewer.internal")
    admission, base, manifest, observed, dependencies = _bundle()
    trusted = _trusted_v19_contract()
    for key in ("summary", "report", "artifacts"):
        base[key].update(trusted[key])
    base["public_result"] = trusted["public_result"]
    base["consumer_capability"]["integrated_sources"] = trusted[
        "consumer_capability"
    ]["integrated_sources"]
    projected = collector._apply_gate_a_bundle_projection({
        "delivery_contract": base,
        "report_issue_focus": _trusted_v19_focus(),
        "gate_a_source": {
            "input_materialized": True,
            "rca_evaluators": [
                {
                    "key": "aeb_trigger",
                    "status": "supported",
                    "evidence_refs": [{"signal": "AEBReq"}],
                }
            ],
        },
    })

    delivery = verify_delivery_bundle(
        admission=admission,
        delivery_contract=projected["delivery_contract"],
        delivery_manifest=manifest,
        observed_files=observed,
        html_dependencies=dependencies,
        issue_title="ACC-自车右转，ACC减速，报接管",
        report_issue_focus=projected["report_issue_focus"],
    )

    assert delivery.effect_payload["terminal_class"] == "candidate_hypothesis"
    assert "ACC 功能链" in delivery.effect_payload["result_field_value"]
    assert "ACC 控制请求异常" in delivery.conclusion
    assert "本单未能定向" not in delivery.conclusion


@pytest.mark.parametrize(
    ("mode", "expected_first_line"),
    [
        ("symptom_refuted", "归因判断：原问题现象被证据反证"),
        ("works_as_designed", "责任结论：当前行为符合设计预期"),
    ],
)
def test_trusted_v19_non_candidate_modes_do_not_suggest_responsibility(
    mode, expected_first_line
):
    contract = _trusted_v19_contract()
    contract["consumer_capability"]["integrated_sources"][
        "conclusion_communication"
    ]["mode"] = mode

    rendered = render_public_rca_result(
        contract, terminal_class="candidate_hypothesis"
    )

    assert rendered.splitlines()[0] == expected_first_line
    assert "建议责任方：" not in rendered


def test_host_gate_a_projection_rejects_malformed_evaluator_source():
    with pytest.raises(DeliveryContractError, match="gate_a_projection_invalid"):
        collector._apply_gate_a_bundle_projection({
            "delivery_contract": {
                "consumer_capability": _gate_a_capability(),
            },
            "gate_a_source": {
                "input_materialized": True,
                "rca_evaluators": [{"status": "not-a-valid-status"}],
            },
        })


def test_host_gate_a_projection_rejects_missing_source_envelope():
    with pytest.raises(DeliveryContractError, match="gate_a_source_missing"):
        collector._apply_gate_a_bundle_projection({
            "delivery_contract": {"summary": {"short_conclusion": "stale"}},
        })


def test_terminal_hard_defect_report_skips_unmaterialized_gate_a_projection():
    bundle = collector._apply_gate_a_bundle_projection({
        "delivery_contract": {
            "schema_version": "g1q3_delivery_contract_v1",
            "terminal_diagnostic": {
                "schema_version": "g1q3_rca_terminal_diagnostic_v1",
                "stage": "s6_report",
                "blocker_kind": "viz_mcap_build_failed",
                "attribution_status": "not_attributable",
            },
        },
        "gate_a_source": {
            "input_materialized": False,
            "materialization_attested": True,
            "terminal_diagnostic": {
                "stage": "s6_report",
                "blocker_kind": "viz_mcap_build_failed",
                "attribution_status": "not_attributable",
            },
            "rca_evaluators": [],
        },
    })

    assert "gate_a_projection" not in bundle["delivery_contract"]
    assert bundle["terminal_diagnostic_projection"]["fault_class"] == (
        "hard_defect"
    )


def test_terminal_diagnostic_bypass_rejects_non_hard_defect():
    with pytest.raises(DeliveryContractError, match="gate_a_projection_invalid"):
        collector._apply_gate_a_bundle_projection({
            "delivery_contract": {
                "schema_version": "g1q3_delivery_contract_v1",
                "terminal_diagnostic": {
                    "schema_version": "g1q3_rca_terminal_diagnostic_v1",
                    "stage": "s2_remote_read",
                    "blocker_kind": "remote_read_completeness_not_proven",
                    "attribution_status": "not_attributable",
                },
            },
            "gate_a_source": {
                "input_materialized": False,
                "materialization_attested": True,
                "terminal_diagnostic": {
                    "stage": "s2_remote_read",
                    "blocker_kind": "remote_read_completeness_not_proven",
                    "attribution_status": "not_attributable",
                },
                "rca_evaluators": [],
            },
        })


def test_host_gate_a_projection_rejects_all_need_fields_source():
    with pytest.raises(DeliveryContractError, match="gate_a_projection_invalid"):
        collector._apply_gate_a_bundle_projection({
            "delivery_contract": {
                "consumer_capability": _gate_a_capability(status="need_fields"),
            },
            "gate_a_source": {
                "input_materialized": True,
                "rca_evaluators": [
                    {
                        "key": "aeb_trigger",
                        "status": "need_fields",
                        "missing_fields": ["AEBReq"],
                    }
                ],
            },
        })


def test_viz_surface_errors_retry_internally_instead_of_becoming_user_results():
    assert (
        "viz_publication_missing" in collector._RETRYABLE_INFRASTRUCTURE_ARTIFACT_CODES
    )
    assert (
        "viz_publication_path_invalid"
        in collector._RETRYABLE_INFRASTRUCTURE_ARTIFACT_CODES
    )
    script = collector._remote_bundle_script("g1q3-rca-s1-" + "b" * 64)
    assert "if viz_publication:" in script


def test_config_ignores_retired_capacity_sampling_env(tmp_path):
    env = _config_env(tmp_path)
    env["HERMES_RCA_DELIVERY_COLLECTOR_ACTIVATION_REQUIRED"] = "true"
    env["HERMES_RCA_DELIVERY_COLLECTOR_CAPACITY_SAMPLE_ENABLED"] = "not-a-bool"
    env["HERMES_RCA_DELIVERY_COLLECTOR_CAPACITY_SAMPLE_BATCH_SIZE"] = "invalid"
    env["HERMES_RCA_DELIVERY_COLLECTOR_CAPACITY_SAMPLE_LOCK_TIMEOUT_SECONDS"] = (
        "invalid"
    )
    env["HERMES_RCA_DELIVERY_COLLECTOR_CAPACITY_TERMINAL_RECEIPT_TIMEOUT_SECONDS"] = (
        "invalid"
    )
    config = collector.CollectorConfig.from_env(env, hermes_home=tmp_path)

    public = config.public_dict()
    assert config.activation_required is True
    assert public["activation_required"] is True
    assert not any(key.startswith("capacity_") for key in public)


def test_activation_required_defaults_false(tmp_path):
    config = collector.CollectorConfig.from_env(
        _config_env(tmp_path),
        hermes_home=tmp_path,
    )

    assert config.activation_required is False
    assert config.public_dict()["activation_required"] is False


def test_enabled_resident_without_epoch_exits_before_collector_creation(
    tmp_path,
    monkeypatch,
    capsys,
):
    config = collector.CollectorConfig.from_env(
        _config_env(tmp_path),
        hermes_home=tmp_path,
    )
    path, _migration = _physical_v15_delivery_fixture(tmp_path)
    assert path == config.control_db_path
    with sqlite3.connect(path) as conn:
        conn.execute(
            "UPDATE rca_activation_epochs "
            "SET state = 'retired', is_current = 0, "
            "retired_at = COALESCE(retired_at, updated_at) "
            "WHERE is_current = 1"
        )
    delivery = RcaDeliveryStore(
        path,
        require_current=True,
        allow_successor_write=True,
    )
    constructed = False

    def unexpected_collector(*_args, **_kwargs):
        nonlocal constructed
        constructed = True
        raise AssertionError("collector must not start without an active epoch")

    monkeypatch.setattr(collector, "load_collector_environment", lambda: None)
    monkeypatch.setattr(
        collector.CollectorConfig,
        "from_env",
        classmethod(lambda _cls: config),
    )
    monkeypatch.setattr(collector, "DeliveryCollector", unexpected_collector)

    assert collector.main(["--once"]) == 2
    assert constructed is False
    assert delivery.list_rows("rca_delivery_effects") == []
    assert "resident_activation_epoch_missing" in capsys.readouterr().out


def _successor_read_only_capability() -> dict[str, object]:
    return {
        "observed_control_schema_version": "pnc_rca_control_store_v15",
        "binary_write_schema_version": "pnc_rca_control_store_v15",
        "mode": "successor_read_only",
        "read_supported": True,
        "write_enabled": False,
        "work_admission_enabled": False,
        "lease_acquisition_enabled": False,
        "external_effect_enabled": False,
    }


def test_successor_read_only_collector_writes_health_without_work_or_probes(
    tmp_path,
    monkeypatch,
):
    config = collector.CollectorConfig.from_env(
        _config_env(tmp_path),
        hermes_home=tmp_path,
    )
    calls = []

    class SuccessorStore:
        def schema_runtime_capability(self):
            return _successor_read_only_capability()

        def health(self, **kwargs):
            calls.append(("health", kwargs))
            return {
                "ok": False,
                "process_healthy": True,
                "schema_runtime_capability": _successor_read_only_capability(),
            }

        def backfill_completed_submissions(self, **_kwargs):
            pytest.fail("backfill must not run")

        def claim_due_watch(self, **_kwargs):
            pytest.fail("watch claim must not run")

    def store_factory(*_args, **kwargs):
        calls.append(("store", kwargs))
        return SuccessorStore()

    monkeypatch.setattr(collector, "load_collector_environment", lambda: None)
    monkeypatch.setattr(
        collector.CollectorConfig,
        "from_env",
        classmethod(lambda _cls: config),
    )
    monkeypatch.setattr(collector, "RcaDeliveryStore", store_factory)
    monkeypatch.setattr(
        collector,
        "validate_bound_resident_release",
        lambda *_args, **_kwargs: pytest.fail("release writer gate must not run"),
    )
    monkeypatch.setattr(
        collector,
        "probe_remote_css_parser",
        lambda *_args, **_kwargs: pytest.fail("VM dependency probe must not run"),
    )
    monkeypatch.setattr(
        collector,
        "DeliveryCollector",
        lambda *_args, **_kwargs: pytest.fail("collector must not be created"),
    )

    assert collector.main(["--once"]) == 0

    assert calls[0][0] == "store"
    assert calls[0][1].get("read_only", False) is False
    assert calls[0][1]["ensure_current_rows"] is False
    assert calls[0][1].get("allow_successor_read_only", False) is False
    assert calls[0][1]["allow_successor_write"] is True
    assert calls[1] == (
        "health",
        {"activation_required": config.activation_required},
    )
    payload = json.loads(config.health_path.read_text(encoding="utf-8"))
    assert payload["mode"] == "successor_read_only"
    assert payload["ready"] is False
    assert payload["processing"] is False
    assert payload["healthy"] is True
    assert payload["process_healthy"] is True
    assert payload["business_ready"] is False
    assert payload["ok"] is False
    assert payload["schema_runtime_capability"] == (
        _successor_read_only_capability()
    )
    assert payload["dependencies"]["remote_css_parser"]["status"] == (
        "not_evaluated_successor_read_only"
    )
    healthy, observed = collector.read_health(
        config.health_path,
        max_age_seconds=config.health_max_age_seconds,
    )
    assert healthy is False
    assert observed["mode"] == "successor_read_only"
    assert observed["liveness_ok"] is True


def test_real_v15_collector_dry_run_preserves_db_wal_shm_without_work_or_vm_probe(
    tmp_path,
    monkeypatch,
):
    path, _migration = _physical_v15_delivery_fixture(tmp_path)
    wal_writer = sqlite3.connect(path)
    assert wal_writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
    wal_writer.execute("PRAGMA wal_autocheckpoint=0")
    wal_writer.execute(
        "INSERT INTO rca_delivery_meta(key, value) VALUES(?, ?)",
        ("collector_resident_live_wal_fixture", "present"),
    )
    wal_writer.commit()
    assert Path(f"{path}-wal").is_file()
    assert Path(f"{path}-shm").is_file()
    before = _sqlite_storage_identity(path)
    config = collector.CollectorConfig.from_env(
        _config_env(tmp_path),
        hermes_home=tmp_path,
    )

    monkeypatch.setattr(collector, "load_collector_environment", lambda: None)
    monkeypatch.setattr(
        collector.CollectorConfig,
        "from_env",
        classmethod(lambda _cls: config),
    )
    monkeypatch.setattr(
        collector,
        "validate_bound_resident_release",
        lambda *_args, **_kwargs: pytest.fail("release writer gate must not run"),
    )
    monkeypatch.setattr(
        collector,
        "probe_remote_css_parser",
        lambda *_args, **_kwargs: pytest.fail("VM dependency probe must not run"),
    )
    monkeypatch.setattr(
        collector,
        "DeliveryCollector",
        lambda *_args, **_kwargs: pytest.fail("collector must not be created"),
    )

    try:
        assert collector.main(["--dry-run"]) == 2
        after = _sqlite_storage_identity(path)
        assert after["db"] == before["db"]
        assert after["-wal"] == before["-wal"]
        assert (after["-shm"] is None) is (before["-shm"] is None)
    finally:
        wal_writer.close()

@pytest.mark.parametrize("mode", ["check_config", "dry_run"])
def test_successor_read_only_collector_diagnostics_do_not_probe_or_preview(
    tmp_path,
    monkeypatch,
    capsys,
    mode,
):
    config = collector.CollectorConfig.from_env(
        _config_env(tmp_path),
        hermes_home=tmp_path,
    )
    config.control_db_path.write_bytes(b"fixture")
    calls = []

    class SuccessorStore:
        def schema_runtime_capability(self):
            return _successor_read_only_capability()

        def preview_unwatched_completed(self, **_kwargs):
            calls.append("preview")
            return []

    def store_factory(*_args, **kwargs):
        calls.append(("store", kwargs))
        return SuccessorStore()

    monkeypatch.setattr(collector, "load_collector_environment", lambda: None)
    monkeypatch.setattr(
        collector.CollectorConfig,
        "from_env",
        classmethod(lambda _cls: config),
    )
    monkeypatch.setattr(collector, "RcaDeliveryStore", store_factory)
    monkeypatch.setattr(
        collector,
        "validate_bound_resident_release",
        lambda *_args, **_kwargs: pytest.fail("release writer gate must not run"),
    )
    monkeypatch.setattr(
        collector,
        "probe_remote_css_parser",
        lambda *_args, **_kwargs: pytest.fail("VM dependency probe must not run"),
    )

    flag = "--check-config" if mode == "check_config" else "--dry-run"
    assert collector.main([flag]) == 2

    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False
    assert payload["mode"] == "successor_read_only"
    assert payload["ready"] is False
    assert payload["processing"] is False
    assert payload["operation"] == mode
    assert "preview" not in calls
    [store_call] = calls
    assert store_call[0] == "store"
    assert store_call[1]["ensure_current_rows"] is False
    if mode == "check_config":
        assert store_call[1].get("read_only", False) is False
        assert store_call[1].get("allow_successor_read_only", False) is False
        assert store_call[1]["allow_successor_write"] is True
    else:
        assert store_call[1]["read_only"] is True
        assert store_call[1]["allow_successor_read_only"] is True
        assert store_call[1]["allow_successor_write"] is False


def test_disabled_collector_check_config_does_not_probe_existing_control_db(
    tmp_path,
    monkeypatch,
    capsys,
):
    env = _config_env(tmp_path)
    env["HERMES_RCA_DELIVERY_COLLECTOR_ENABLED"] = "false"
    config = collector.CollectorConfig.from_env(env, hermes_home=tmp_path)
    config.control_db_path.write_bytes(b"not-a-database")
    monkeypatch.setattr(collector, "load_collector_environment", lambda: None)
    monkeypatch.setattr(
        collector.CollectorConfig,
        "from_env",
        classmethod(lambda _cls: config),
    )
    monkeypatch.setattr(
        collector,
        "RcaDeliveryStore",
        lambda *_args, **_kwargs: pytest.fail("disabled check-config probed DB"),
    )
    monkeypatch.setattr(
        collector,
        "probe_remote_css_parser",
        lambda *_args, **_kwargs: collector.expected_remote_css_runtime_dependency(),
    )
    monkeypatch.setattr(
        collector.FailureRouteOutlet,
        "inspect",
        lambda *_args, **_kwargs: {"ready": True, "status": "uninitialized"},
    )

    assert collector.main(["--check-config"]) == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True


def test_enabled_startup_locks_minimal_release_before_writable_collector(
    tmp_path,
    monkeypatch,
):
    fixture = _release_note(tmp_path)
    control, result = _control(tmp_path, db_path=fixture.control_db_path)
    _bind_activation_execution(control, result, state="steady_active")
    RcaDeliveryStore(control.db_path)
    _bind_minimal_release(control, fixture)
    migration = _migrate_v14_fixture_to_v15(
        control.db_path,
        successor_epoch_id=fixture.note["activation"]["epoch_id"],
        successor_release_fingerprint_sha256=fixture.fingerprint,
        successor_release_note_sha256=fixture.epoch["release_note_sha256"],
        successor_config_sha256=fixture.env_sha256,
    )
    fixture.epoch["epoch_id"] = migration["successor_epoch_id"]
    _set_live_release_environment(monkeypatch, fixture)
    wal_writer = sqlite3.connect(control.db_path)
    assert wal_writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
    wal_writer.execute("PRAGMA wal_autocheckpoint=0")
    wal_writer.execute(
        "INSERT INTO rca_delivery_meta(key, value) VALUES(?, ?)",
        ("collector_startup_active_wal", "present"),
    )
    wal_writer.commit()
    config = replace(
        collector.CollectorConfig.from_env(
            _config_env(tmp_path),
            hermes_home=tmp_path,
        ),
        control_db_path=control.db_path,
        release_note_path=fixture.path,
    )
    events = []
    captured = {}
    real_store = collector.RcaDeliveryStore
    real_validate = collector.validate_bound_resident_release

    def tracked_store(*args, **kwargs):
        events.append(
            (
                "store",
                kwargs.get("read_only", False),
                kwargs.get("ensure_current_rows", True),
            )
        )
        return real_store(*args, **kwargs)

    def tracked_validate(*args, **kwargs):
        events.append(("validate",))
        return real_validate(*args, **kwargs)

    def constructed_collector(*, store, config):
        events.append(("collector",))
        captured["config"] = config
        return SimpleNamespace(store=store, config=config)

    monkeypatch.setattr(
        RcaControlStore,
        "create_schema_probe_snapshot",
        classmethod(
            lambda _cls, *_args, **_kwargs: pytest.fail(
                "normal startup must not copy the control DB"
            )
        ),
    )
    monkeypatch.setattr(collector, "RcaDeliveryStore", tracked_store)
    monkeypatch.setattr(
        collector, "validate_bound_resident_release", tracked_validate
    )
    monkeypatch.setattr(
        collector, "load_collector_environment", lambda: fixture.env_path
    )
    monkeypatch.setattr(
        collector.CollectorConfig,
        "from_env",
        classmethod(lambda _cls: config),
    )
    monkeypatch.setattr(collector, "DeliveryCollector", constructed_collector)
    monkeypatch.setattr(collector, "run_collector_loop", lambda *_args, **_kwargs: 0)

    try:
        assert collector.main(["--once"]) == 0
    finally:
        wal_writer.close()
    assert events == [
        ("store", False, False),
        ("validate",),
        ("store", False, True),
        ("collector",),
    ]
    locked = captured["config"]
    assert locked.resident_release_enforced is True
    assert locked.release_id == fixture.note["release_id"]
    assert locked.release_epoch_id == fixture.epoch["epoch_id"]
    assert locked.release_fingerprint_sha256 == fixture.fingerprint
    assert (
        locked.release_note_sha256
        == fixture.epoch["release_note_sha256"]
    )


def test_collector_epoch_switch_rejects_batch_before_backfill_or_claim(
    tmp_path,
    monkeypatch,
):
    fixture = _release_note(tmp_path)
    control, result = _control(tmp_path, db_path=fixture.control_db_path)
    _bind_activation_execution(control, result, state="steady_active")
    RcaDeliveryStore(control.db_path)
    _bind_minimal_release(control, fixture)
    migration = _migrate_v14_fixture_to_v15(
        control.db_path,
        successor_epoch_id=fixture.note["activation"]["epoch_id"],
        successor_release_fingerprint_sha256=fixture.fingerprint,
        successor_release_note_sha256=fixture.epoch["release_note_sha256"],
        successor_config_sha256=fixture.env_sha256,
    )
    fixture.epoch["epoch_id"] = migration["successor_epoch_id"]
    control = RcaControlStore(
        control.db_path,
        require_current=True,
        allow_successor_write=True,
    )
    store = RcaDeliveryStore(
        control.db_path,
        require_current=True,
        allow_successor_write=True,
    )
    _set_live_release_environment(monkeypatch, fixture)
    env = _config_env(tmp_path)
    base = collector.CollectorConfig.from_env(env, hermes_home=tmp_path)
    binding = collector.validate_bound_resident_release(
        store,
        release_note_path=fixture.path,
        runtime_root=fixture.runtime_root,
        runtime_commit=fixture.runtime_commit,
        runtime_tree=fixture.runtime_tree,
        live_manifest_sha256=fixture.manifest_sha256,
        live_env_path=fixture.env_path,
    )
    config = replace(
        base,
        control_db_path=control.db_path,
        release_note_path=fixture.path,
        release_env_path=fixture.env_path,
        resident_release_enforced=True,
        release_id=binding["release_id"],
        release_epoch_id=binding["epoch_id"],
        release_fingerprint_sha256=binding["release_fingerprint_sha256"],
        release_note_sha256=binding["release_note_sha256"],
    )
    provider_calls = []
    instance = collector.DeliveryCollector(
        store=store,
        config=config,
        status_reader=lambda *_args: provider_calls.append("status"),
    )
    assert instance._validate_runtime_release()["epoch_id"] == (
        migration["successor_epoch_id"]
    )
    predecessor = control.direct_steady_predecessor()
    assert predecessor is not None
    record = _record()
    progress = control.partition_progress(
        topic=record.topic,
        partitions=(record.partition,),
    )
    successor = _direct_steady_contract(
        predecessor=predecessor,
        epoch_id="delivery-epoch-2",
        expected_schema="pnc_rca_control_store_v15",
        target_schema="pnc_rca_control_store_v15",
        release_fingerprint_sha256="d" * 64,
        release_note_sha256="e" * 64,
        config_sha256="f" * 64,
        partition_start_fence={
            record.topic: {str(record.partition): progress[record.partition]}
        },
    )
    control.activate_direct_steady_epoch(
        **successor,
        operator="collector-test",
        reason="simulate an exact v15 release epoch switch",
        now=NOW + timedelta(seconds=1),
    )
    monkeypatch.setattr(
        store,
        "backfill_completed_submissions",
        lambda **_kwargs: pytest.fail("backfill ran after release epoch drift"),
    )
    monkeypatch.setattr(
        store,
        "claim_due_watch",
        lambda **_kwargs: pytest.fail("claim ran after release epoch drift"),
    )

    with pytest.raises(collector.ExternalWriteFenceError) as exc:
        instance.collect_batch()

    assert exc.value.code == "resident_release_fingerprint_mismatch"
    assert provider_calls == []


@pytest.mark.parametrize("value", ["1", "0", "yes", "on", "off", ""])
def test_activation_required_rejects_boolean_aliases(tmp_path, value):
    env = _config_env(tmp_path)
    env["HERMES_RCA_DELIVERY_COLLECTOR_ACTIVATION_REQUIRED"] = value

    with pytest.raises(ValueError, match="exactly true or false"):
        collector.CollectorConfig.from_env(env, hermes_home=tmp_path)


def test_activation_gate_does_not_backfill_claim_or_preview_legacy_null_row(
    tmp_path,
):
    control, legacy = _control(tmp_path)
    with sqlite3.connect(control.db_path) as conn:
        trigger_update = conn.execute(
            "UPDATE business_triggers "
            "SET activation_epoch_id = NULL, activation_ledger_id = NULL "
            "WHERE submission_key = ?",
            (legacy.submission_key,),
        )
        outbox_update = conn.execute(
            "UPDATE rca_outbox "
            "SET activation_epoch_id = NULL, activation_ledger_id = NULL "
            "WHERE submission_key = ?",
            (legacy.submission_key,),
        )
    assert trigger_update.rowcount == 1
    assert outbox_update.rowcount == 1
    ledger_before = control.list_rows("rca_activation_admission_ledger")
    env = _config_env(tmp_path)
    env["HERMES_RCA_DELIVERY_COLLECTOR_ACTIVATION_REQUIRED"] = "true"
    instance = collector.DeliveryCollector(
        store=RcaDeliveryStore(control.db_path),
        config=collector.CollectorConfig.from_env(env, hermes_home=tmp_path),
        status_reader=lambda _task_id: pytest.fail("legacy row reached VM reader"),
        now=lambda: NOW,
        lease_owner="activation-required-test",
    )

    assert instance.backfill() == 0
    assert instance.collect_one().status == "idle"
    preview = instance.dry_run_once()

    assert preview["candidate_count"] == 0
    assert instance.store.list_rows("rca_execution_watch") == []
    [row] = control.list_rows("rca_outbox")
    assert row["submission_key"] == legacy.submission_key
    assert row["activation_epoch_id"] is None
    assert row["activation_ledger_id"] is None
    assert row["status"] == "completed"
    [trigger] = control.list_rows("business_triggers")
    assert trigger["submission_key"] == legacy.submission_key
    assert trigger["activation_epoch_id"] is None
    assert trigger["activation_ledger_id"] is None
    assert control.list_rows("rca_activation_admission_ledger") == ledger_before


def test_activation_required_reaches_watch_claim_and_successful_create(
    tmp_path,
    monkeypatch,
):
    control, result = _control(tmp_path)
    _bind_activation_execution(control, result, state="steady_active")
    real_store = RcaDeliveryStore(control.db_path)
    assert (
        real_store.backfill_completed_submissions(
            now=NOW,
            activation_required=True,
        )
        == 1
    )
    claim = real_store.claim_due_watch(
        lease_owner="activation-create-test",
        lease_seconds=60,
        now=NOW,
        activation_required=True,
    )
    assert claim is not None
    calls = []
    original_create = real_store.create_delivery
    store = SimpleNamespace(
        claim_due_watch=lambda **kwargs: calls.append(("claim", kwargs)) or claim,
        create_delivery=(
            lambda **kwargs: (
                calls.append(("create", kwargs)) or original_create(**kwargs)
            )
        ),
    )
    env = _config_env(tmp_path)
    env["HERMES_RCA_DELIVERY_COLLECTOR_ACTIVATION_REQUIRED"] = "true"
    instance = collector.DeliveryCollector(
        store=store,
        config=collector.CollectorConfig.from_env(env, hermes_home=tmp_path),
        status_reader=lambda task_id: {
            "success": True,
            "task_id": task_id,
            "state": "completed",
        },
        artifact_bundle_reader=lambda _claim: {},
        now=lambda: NOW,
        lease_owner="activation-create-test",
    )
    monkeypatch.setattr(
        collector,
        "verify_delivery_bundle",
        lambda **_kwargs: _delivery(claim),
    )

    outcome = instance.collect_one()

    assert outcome.status == "delivery_created"
    assert calls[0][0] == "claim"
    assert calls[0][1]["activation_required"] is True
    assert calls[1][0] == "create"
    assert calls[1][1]["activation_required"] is True


def test_terminal_failure_is_silent_and_does_not_create_delivery(tmp_path):
    env = _config_env(tmp_path)
    env["HERMES_RCA_DELIVERY_COLLECTOR_ACTIVATION_REQUIRED"] = "true"
    calls = []
    instance = object.__new__(collector.DeliveryCollector)
    instance.config = collector.CollectorConfig.from_env(env, hermes_home=tmp_path)
    instance.store = SimpleNamespace(
        terminal_failure=lambda **kwargs: (
            calls.append(kwargs)
        )
    )
    instance.stats = collector.CollectorStats()
    instance.runtime_identity = None
    instance.now = lambda: NOW
    claim = SimpleNamespace(
        submission_key="submission-key",
        state="pending",
        lease_token="lease-token",
    )

    outcome = instance._durable_terminal_outcome(
        claim,
        status={"success": False},
        outcome="terminal_failed",
        terminal_state="failed",
        error_code="rca_work_deadline_exceeded",
        error_detail="deadline",
    )

    assert outcome.status == "terminal_failed"
    assert outcome.delivery_id == ""
    assert outcome.effect_key == ""
    assert calls[0]["status"]["external_writes"] is False
    assert calls[0]["status"]["terminal_delivery_policy"] == (
        "silent_internal_alert_only"
    )
    assert "activation_required" not in calls[0]


def test_collect_batch_collects_delivery_until_idle():
    instance = collector.DeliveryCollector.__new__(collector.DeliveryCollector)
    instance.config = SimpleNamespace(batch_size=3)
    instance.stats = collector.CollectorStats()
    instance._validate_runtime_release = lambda: {}
    instance.backfill = lambda: 0
    outcomes = iter([
        collector.CollectOutcome(status="running"),
        collector.CollectOutcome(status="idle"),
    ])
    instance.collect_one = lambda: next(outcomes)

    result = instance.collect_batch()

    assert [item.status for item in result] == ["running", "idle"]


def test_collector_stats_omit_retired_capacity_counters():
    public = collector.asdict(collector.CollectorStats())
    assert "activation_blocked" not in public
    assert not any(key.startswith("capacity_") for key in public)
    assert public["stale_lease"] == 0


def test_health_omits_retired_capacity_sample_projection(tmp_path):
    env = _config_env(tmp_path)
    env["HERMES_RCA_DELIVERY_COLLECTOR_ACTIVATION_REQUIRED"] = "true"
    config = collector.CollectorConfig.from_env(env, hermes_home=tmp_path)
    health_calls = []
    reporter = object.__new__(collector.HealthReporter)
    reporter.config = config
    reporter.store = SimpleNamespace(
        health=lambda **kwargs: health_calls.append(kwargs) or {"ok": True}
    )
    reporter.started_at = collector._utc_iso()
    reporter.runtime_identity = SimpleNamespace(to_dict=lambda: {})
    reporter._remote_css_parser_receipt = {"status": "ok"}
    reporter._remote_css_parser_error = ""
    reporter._remote_css_parser_observed_at = collector._utc_now()
    reporter._failure_route_outlet_receipt = {"ready": True, "status": "ready"}
    reporter._failure_route_outlet_error = ""

    stats = collector.CollectorStats()
    reporter.write(state="idle", stats=stats, refresh_dependencies=False)

    payload = json.loads(config.health_path.read_text(encoding="utf-8"))
    assert health_calls[0]["activation_required"] is True
    assert payload["healthy"] is True
    assert "capacity_samples" not in payload


def _remote_event_blocker():
    reference_sha256 = "a" * 64
    return {
        "kind": "remote_event_not_found",
        "retryable": False,
        "audit": {
            "parse_attempts": [
                {
                    "attempt_id": "parse-attempt-1",
                    "parser": "remote_event_reader",
                    "status": "parsed",
                    "reference_sha256": reference_sha256,
                }
            ],
            "data_sources": [
                {
                    "source_id": "data-source-1",
                    "source_kind": "pdcl_event",
                    "status": "not_found",
                    "reference_sha256": reference_sha256,
                }
            ],
            "results": [
                {
                    "attempt_id": "parse-attempt-1",
                    "source_id": "data-source-1",
                    "status": "not_found",
                    "returned_count": 0,
                    "reference_sha256": reference_sha256,
                }
            ],
        },
    }


def _v15_completed_control(tmp_path):
    path = tmp_path / "control.sqlite3"
    record = _record()
    seed = RcaControlStore(path)
    seed.activate_direct_steady_epoch(
        epoch_id="delivery-epoch-1",
        release_fingerprint_sha256="a" * 64,
        release_note_sha256="b" * 64,
        config_sha256="c" * 64,
        db_logical_identity={"database": "delivery-test"},
        partition_start_fence={record.topic: {str(record.partition): 0}},
        operator="delivery-test",
        reason="activate v14 predecessor before collector cutover fixture",
        now=NOW,
    )
    RcaDeliveryStore(path)
    _migrate_v14_fixture_to_v15(
        path,
        successor_epoch_id="delivery-epoch-v15",
    )
    control = RcaControlStore(
        path,
        require_current=True,
        allow_successor_write=True,
    )
    result = control.ingest_record(
        record,
        policy=_policy(),
        submit_enabled=True,
        activation_required=True,
    )
    claim = control.claim_outbox(lease_owner="submission-worker", now=NOW)
    assert claim is not None
    control.complete_outbox(
        outbox_id=claim.outbox_id,
        lease_token=claim.lease_token,
        result={
            "success": True,
            "submission_key": result.submission_key,
            "task_id": result.submission_key,
            "task_state": "submitted",
            "deduped": False,
        },
        now=NOW,
    )
    return control, result


def _real_terminal_collector(
    tmp_path,
    *,
    clock,
    blocker=None,
    status_reader=None,
    failure_receipt_reader=None,
    infra_remediation_runner=None,
):
    control, _result = _v15_completed_control(tmp_path)
    env = _config_env(tmp_path)
    config = collector.CollectorConfig.from_env(env, hermes_home=tmp_path)
    instance = collector.DeliveryCollector(
        store=RcaDeliveryStore(
            tmp_path / "control.sqlite3",
            require_current=True,
            allow_successor_write=True,
        ),
        config=config,
        status_reader=status_reader
        or (
            lambda task_id: {
                "success": True,
                "task_id": task_id,
                "state": "failed",
                "summary": "private VM failure",
            }
        ),
        failure_receipt_reader=failure_receipt_reader
        or (
            lambda claim: {
                "schema_version": collector.FAILURE_RECEIPT_SCHEMA_VERSION,
                "task_id": claim.task_id,
                "status": "pipeline_not_successful",
                "pipeline_status": "needs_fix",
                "pipeline_stage": "s6_report",
                "blocker": blocker,
            }
        ),
        infra_remediation_runner=infra_remediation_runner,
        control_store=control,
        now=lambda: clock[0],
        lease_owner="taxonomy-real-path",
    )
    assert instance.backfill() == 1
    return instance


def _age_submission(instance, *, seconds):
    started_at = (NOW - timedelta(seconds=seconds)).isoformat()
    with sqlite3.connect(instance.store.db_path) as conn:
        conn.execute(
            "UPDATE business_triggers SET created_at = ?",
            (started_at,),
        )
        conn.execute(
            """
            UPDATE rca_outbox
               SET created_at = ?, retry_window_started_at = ?, completed_at = ?
            """,
            (started_at, started_at, started_at),
        )


def _age_failure_window(instance, *, seconds):
    first_seen_at = (NOW - timedelta(seconds=seconds)).isoformat()
    with sqlite3.connect(instance.store.db_path) as conn:
        conn.execute(
            "UPDATE rca_execution_watch SET terminal_first_seen_at = ?",
            (first_seen_at,),
        )


def test_snapshot_required_collector_quarantines_missing_snapshot_without_effect(
    tmp_path,
):
    status_calls = []
    instance = _real_terminal_collector(
        tmp_path,
        clock=[NOW],
        status_reader=lambda task_id: status_calls.append(task_id),
    )
    instance.config = replace(
        instance.config,
        w3_snapshot_read_mode="snapshot_required",
        w3_snapshot_authority=_runtime_authority(),
    )

    outcome = instance.collect_one()

    assert outcome.status == "quarantined"
    assert outcome.error_code == "w3_execution_snapshot_missing"
    assert status_calls == []
    assert instance.store.list_rows("rca_delivery_effects") == []
    assert instance.store.list_rows("rca_delivery_jobs") == []
    assert instance.store.list_rows("rca_execution_watch")[0]["state"] == (
        "quarantined"
    )


@pytest.mark.parametrize(
    ("blocker", "lane", "route_kind", "owner", "error_code"),
    [
        (
            {"kind": "translate_workdir_permission", "retryable": True},
            "infra_self_healable",
            "infra_remediation_hold",
            "rca-infra",
            "translate_workdir_permission",
        ),
        (
            _remote_event_blocker(),
            "needs_human_input",
            "internal_backlog",
            "rca-triage",
            "remote_event_not_found",
        ),
        (
            {"kind": "html_capability_payload_mismatch", "retryable": False},
            "hard_defect",
            "internal_alert",
            "rca-engineering",
            "html_capability_payload_mismatch",
        ),
    ],
)
def test_all_failure_lanes_use_first_failure_observation_for_fallback_deadline(
    tmp_path, blocker, lane, route_kind, owner, error_code
):
    clock = [NOW]
    instance = _real_terminal_collector(
        tmp_path,
        blocker=blocker,
        clock=clock,
    )
    [watch] = instance.store.list_rows("rca_execution_watch")
    _insert_subscription(
        instance.store,
        SimpleNamespace(
            business_key=watch["business_key"],
            generation=watch["generation"],
        ),
        effect_kind="feishu_thread_reply",
    )

    held = instance.collect_one()

    assert held.status == "failure_hold"
    assert held.error_code == error_code
    assert instance.store.list_rows("rca_delivery_jobs") == []
    assert instance.store.list_rows("rca_delivery_effects") == []
    [route] = instance.store.list_rows("rca_failure_routes")
    assert route["lane"] == lane
    assert route["route_kind"] == route_kind
    assert route["owner"] == owner
    assert route["retry_count"] == 1
    assert route["observation_count"] == 1
    assert route["next_retry_at"]
    watch = instance.store.list_rows("rca_execution_watch")[0]
    assert watch["generation"] == 1
    assert watch["state"] == "pending"

    clock[0] = NOW + timedelta(seconds=1800)
    fallback = instance.collect_one()

    assert fallback.status == "terminal_failed"
    [route] = instance.store.list_rows("rca_failure_routes")
    assert route["status"] in {"remediation_held", "backlog_pending", "alert_pending"}
    assert instance.store.list_rows("rca_delivery_jobs") == []
    assert instance.store.list_rows("rca_delivery_effects") == []
    subscriptions = instance.store.list_rows("rca_delivery_subscriptions")
    assert any(
        row["effect_kind"] == "feishu_thread_reply" for row in subscriptions
    )
    assert all(row["status"] == "pending" for row in subscriptions)
    assert all(row["delivery_id"] is None for row in subscriptions)
    assert all(row["effect_key"] is None for row in subscriptions)
    watch = instance.store.list_rows("rca_execution_watch")[0]
    assert watch["state"] == "terminal_failed"
    taxonomy = json.loads(watch["last_status_json"])["failure_taxonomy"]
    assert taxonomy["terminal_fallback"]["confidence_tier"] == "low"
    assert taxonomy["terminal_fallback"]["elapsed_seconds"] == 1800
    assert json.loads(watch["last_status_json"])["external_writes"] is False
    assert json.loads(watch["last_status_json"])["terminal_delivery_policy"] == (
        "silent_internal_alert_only"
    )


@pytest.mark.parametrize(
    "blocker_kind",
    [
        "remote_evidence_domain_unsupported",
        "viz_evidence_unavailable",
        "evidence_not_ready",
    ],
)
def test_known_production_terminal_is_not_held_for_fallback_window(
    tmp_path, blocker_kind
):
    instance = _real_terminal_collector(
        tmp_path,
        clock=[NOW],
        blocker={"kind": blocker_kind, "retryable": False},
    )

    outcome = instance.collect_one()

    assert outcome.status == "terminal_failed"
    assert outcome.error_code == (
        f"taxonomy_gap:{blocker_kind}"
        if blocker_kind == "viz_evidence_unavailable"
        else blocker_kind
    )
    [route] = instance.store.list_rows("rca_failure_routes")
    if blocker_kind == "evidence_not_ready":
        assert route["lane"] == "needs_human_input"
        assert route["route_kind"] == "internal_backlog"
        assert route["owner"] == "rca-triage"
    assert instance.store.list_rows("rca_delivery_jobs") == []


def test_infra_remediation_runner_executes_once_for_same_task(tmp_path):
    clock = [NOW]
    marker = tmp_path / "remediation-ran"
    calls = []

    def remediate(claim, blocker, remediation, timeout_seconds):
        marker.write_text(claim.task_id, encoding="utf-8")
        calls.append((claim.submission_key, blocker["kind"], remediation["op"]))
        return {
            "schema_version": collector.INFRA_REMEDIATION_SCHEMA_VERSION,
            "success": True,
            "status": "succeeded",
            "submission_key": claim.submission_key,
            "business_key": claim.business_key,
            "generation": claim.generation,
            "task_id": claim.task_id,
            "operation": remediation["op"],
            "blocker_kind": blocker["kind"],
            "resumed_same_task": True,
            "external_writes": False,
            "timeout_seconds": timeout_seconds,
            "error_code": "",
        }

    instance = _real_terminal_collector(
        tmp_path,
        blocker={"kind": "translate_workdir_permission", "retryable": True},
        clock=clock,
        infra_remediation_runner=remediate,
    )

    first = instance.collect_one()
    clock[0] = NOW + timedelta(seconds=60)
    second = instance.collect_one()

    assert first.status == second.status == "failure_hold"
    assert marker.read_text(encoding="utf-8").startswith("g1q3-rca-s1-")
    assert len(calls) == 1
    [route] = instance.store.list_rows("rca_failure_routes")
    assert route["remediation_attempt_count"] == 1
    assert route["status"] == "remediation_succeeded"
    result = json.loads(route["remediation_result_json"])
    assert result["resumed_same_task"] is True
    assert result["generation"] == 1


def test_infra_remediation_crossing_deadline_falls_back_without_extra_hold(tmp_path):
    clock = [NOW]

    def crossing_remediation(claim, blocker, remediation, timeout_seconds):
        clock[0] = NOW + timedelta(seconds=5)
        return collector.default_infra_remediation_runner(
            claim,
            blocker,
            remediation,
            timeout_seconds,
        )

    instance = _real_terminal_collector(
        tmp_path,
        blocker={"kind": "translate_workdir_permission", "retryable": True},
        clock=clock,
        infra_remediation_runner=crossing_remediation,
    )
    _age_failure_window(instance, seconds=1795)

    fallback = instance.collect_one()

    assert fallback.status == "terminal_failed"
    assert fallback.error_code == "translate_workdir_permission"
    [route] = instance.store.list_rows("rca_failure_routes")
    assert route["remediation_attempt_count"] == 1
    assert route["status"] == "remediation_held"
    assert instance.stats.failure_holds == 0


def test_unknown_code_is_held_fail_closed_then_persisted_as_taxonomy_gap(tmp_path):
    clock = [NOW]
    instance = _real_terminal_collector(
        tmp_path,
        blocker={"kind": "new_vm_failure", "retryable": True},
        clock=clock,
    )

    first = instance.collect_one()

    assert first.status == "failure_hold"
    assert first.error_code == "taxonomy_gap:new_vm_failure"
    assert instance.store.list_rows("rca_delivery_jobs") == []
    clock[0] = NOW + timedelta(seconds=1800)
    fallback = instance.collect_one()
    assert fallback.status == "terminal_failed"
    assert instance.store.list_rows("rca_delivery_jobs") == []
    assert instance.store.list_rows("rca_delivery_effects") == []


@pytest.mark.parametrize(
    "vm_state",
    ["pending", "submitted", "queued", "claimed", "running", "in_progress"],
)
def test_vm_queue_and_execution_states_are_not_host_deadlined(tmp_path, vm_state):
    clock = [NOW]
    instance = _real_terminal_collector(
        tmp_path,
        clock=clock,
        status_reader=lambda task_id: {
            "success": True,
            "task_id": task_id,
            "state": vm_state,
        },
    )
    _age_submission(instance, seconds=6 * 60 * 60)

    assert instance.collect_one().status == "running"
    assert instance.store.list_rows("rca_failure_routes") == []
    assert instance.store.list_rows("rca_delivery_effects") == []
    clock[0] = NOW + timedelta(seconds=8 * 60 * 60)
    still_active = instance.collect_one()

    assert still_active.status == "running"
    assert instance.store.list_rows("rca_failure_routes") == []
    [watch] = instance.store.list_rows("rca_execution_watch")
    assert watch["state"] == ("pending" if vm_state == "pending" else "running")
    assert watch["terminal_first_seen_at"] is None


@pytest.mark.parametrize(
    ("status_reader", "expected_code"),
    [
        (
            lambda _task_id: (_ for _ in ()).throw(OSError("offline")),
            "vm_status_reader_unavailable",
        ),
        (lambda _task_id: {"success": False, "state": "missing"}, "vm_status_missing"),
    ],
)
def test_status_missing_and_reader_error_use_same_failure_observation_deadline(
    tmp_path, status_reader, expected_code
):
    clock = [NOW]
    instance = _real_terminal_collector(
        tmp_path,
        clock=clock,
        status_reader=status_reader,
    )
    _age_submission(instance, seconds=6 * 60 * 60)

    held = instance.collect_one()
    assert held.status == "failure_hold"
    assert held.error_code == expected_code
    assert instance.store.list_rows("rca_delivery_effects") == []
    assert instance.store.list_rows("rca_execution_watch")[0][
        "terminal_first_seen_at"
    ] == NOW.isoformat()
    clock[0] = NOW + timedelta(seconds=1800)
    fallback = instance.collect_one()

    assert fallback.status == "terminal_failed"
    assert fallback.error_code == expected_code
    assert instance.store.list_rows("rca_failure_routes")[0]["status"] in {
        "remediation_held", "backlog_pending", "alert_pending"
    }


def test_healthy_vm_observation_clears_prior_failure_window(tmp_path):
    clock = [NOW]
    statuses = iter([
        {"success": False, "state": "missing"},
        {"success": True, "state": "pending"},
        {"success": True, "state": "pending"},
    ])
    instance = _real_terminal_collector(
        tmp_path,
        clock=clock,
        status_reader=lambda _task_id: next(statuses),
    )

    assert instance.collect_one().status == "failure_hold"
    assert instance.store.list_rows("rca_execution_watch")[0][
        "terminal_first_seen_at"
    ] == NOW.isoformat()

    clock[0] = NOW + timedelta(seconds=60)
    assert instance.collect_one().status == "running"
    assert instance.store.list_rows("rca_execution_watch")[0][
        "terminal_first_seen_at"
    ] is None

    clock[0] = NOW + timedelta(seconds=3600)
    assert instance.collect_one().status == "running"
    assert instance.store.list_rows("rca_execution_watch")[0]["state"] == "pending"


def test_failure_window_marker_survives_crash_after_route_commit(
    tmp_path, monkeypatch
):
    clock = [NOW]
    instance = _real_terminal_collector(
        tmp_path,
        clock=clock,
        status_reader=lambda _task_id: {"success": False, "state": "missing"},
    )
    reschedule_watch = instance.store.reschedule_watch

    def crash_before_reschedule(**_kwargs):
        raise RuntimeError("simulated collector exit")

    monkeypatch.setattr(instance.store, "reschedule_watch", crash_before_reschedule)
    with pytest.raises(RuntimeError, match="simulated collector exit"):
        instance.collect_one()

    [watch] = instance.store.list_rows("rca_execution_watch")
    assert watch["terminal_first_seen_at"] == NOW.isoformat()
    assert len(instance.store.list_rows("rca_failure_routes")) == 1

    monkeypatch.setattr(instance.store, "reschedule_watch", reschedule_watch)
    clock[0] = NOW + timedelta(seconds=1800)
    outcome = instance.collect_one()

    assert outcome.status == "terminal_failed"
    assert outcome.error_code == "vm_status_missing"


def test_completed_delivery_clears_prior_failure_window(tmp_path, monkeypatch):
    clock = [NOW]
    statuses = iter([
        {"success": False, "state": "missing"},
        {"success": True, "state": "completed"},
    ])
    instance = _real_terminal_collector(
        tmp_path,
        clock=clock,
        status_reader=lambda _task_id: next(statuses),
    )

    assert instance.collect_one().status == "failure_hold"
    observed_claims = []
    instance.artifact_bundle_reader = (
        lambda claim: observed_claims.append(claim) or {}
    )
    monkeypatch.setattr(
        collector,
        "verify_delivery_bundle",
        lambda **_kwargs: _delivery(observed_claims[0]),
    )

    clock[0] = NOW + timedelta(seconds=60)
    assert instance.collect_one().status == "delivery_created"
    [watch] = instance.store.list_rows("rca_execution_watch")
    assert watch["terminal_first_seen_at"] is None


def test_same_failure_after_recovery_restarts_route_window(tmp_path):
    clock = [NOW]
    statuses = iter([
        {"success": False, "state": "missing"},
        {"success": True, "state": "pending"},
        {"success": False, "state": "missing"},
    ])
    instance = _real_terminal_collector(
        tmp_path,
        clock=clock,
        status_reader=lambda _task_id: next(statuses),
    )

    assert instance.collect_one().status == "failure_hold"
    clock[0] = NOW + timedelta(seconds=60)
    assert instance.collect_one().status == "running"
    clock[0] = NOW + timedelta(seconds=120)
    assert instance.collect_one().status == "failure_hold"

    [route] = instance.store.list_rows("rca_failure_routes")
    assert route["work_started_at"] == clock[0].isoformat()
    assert route["deadline_at"] == (
        clock[0] + timedelta(seconds=1800)
    ).isoformat()


def test_invalid_submission_admission_uses_failure_observation_deadline(tmp_path):
    clock = [NOW]
    instance = _real_terminal_collector(
        tmp_path,
        clock=clock,
        blocker={"kind": "service_pipeline_runner_failed", "retryable": False},
    )
    with sqlite3.connect(instance.store.db_path) as conn:
        conn.execute("UPDATE rca_outbox SET payload_json = '{}' ")

    held = instance.collect_one()
    assert held.status == "failure_hold"
    assert held.error_code == "submission_outbox_contract_invalid"
    assert instance.store.list_rows("rca_delivery_effects") == []
    clock[0] = NOW + timedelta(seconds=1800)
    fallback = instance.collect_one()

    assert fallback.status == "terminal_failed"
    assert fallback.error_code == "submission_outbox_contract_invalid"
    assert instance.store.list_rows("rca_failure_routes")[0]["route_kind"] == (
        "internal_alert"
    )


def test_permanent_artifact_error_uses_failure_observation_deadline(tmp_path):
    clock = [NOW]

    def invalid_bundle(_claim):
        raise collector.ArtifactBundleReadError(
            "artifact_hash_mismatch",
            "sealed artifact hash changed",
            permanent=True,
        )

    instance = _real_terminal_collector(
        tmp_path,
        clock=clock,
        status_reader=lambda task_id: {
            "success": True,
            "task_id": task_id,
            "state": "completed",
        },
    )
    instance.artifact_bundle_reader = invalid_bundle

    held = instance.collect_one()
    assert held.status == "failure_hold"
    assert held.error_code == "artifact_hash_mismatch"
    assert instance.store.list_rows("rca_delivery_effects") == []
    clock[0] = NOW + timedelta(seconds=1800)
    fallback = instance.collect_one()

    assert fallback.status == "terminal_failed"
    assert fallback.error_code == "artifact_hash_mismatch"


def test_old_submission_crossing_host_time_does_not_deadline_running_vm_task(
    tmp_path, monkeypatch
):
    clock = [NOW]
    status_calls = []
    instance = _real_terminal_collector(
        tmp_path,
        clock=clock,
        status_reader=lambda task_id: status_calls.append(task_id)
        or {"success": True, "task_id": task_id, "state": "running"},
    )
    _age_submission(instance, seconds=6 * 60 * 60)
    original = collector._submission_admission

    def crossing_admission(claim):
        admission = original(claim)
        clock[0] = NOW + timedelta(seconds=1)
        return admission

    monkeypatch.setattr(collector, "_submission_admission", crossing_admission)

    outcome = instance.collect_one()

    assert outcome.status == "running"
    assert len(status_calls) == 1
    assert instance.store.list_rows("rca_delivery_jobs") == []
    assert instance.store.list_rows("rca_delivery_effects") == []
    assert instance.store.list_rows("rca_execution_watch")[0]["state"] == "running"


def test_old_submission_completed_result_is_verified_and_delivered(
    tmp_path, monkeypatch
):
    clock = [NOW]
    artifact_calls = []

    def late_completed_status(task_id):
        clock[0] = NOW + timedelta(seconds=1)
        return {"success": True, "task_id": task_id, "state": "completed"}

    instance = _real_terminal_collector(
        tmp_path,
        clock=clock,
        status_reader=late_completed_status,
    )
    _age_submission(instance, seconds=6 * 60 * 60)
    instance.artifact_bundle_reader = (
        lambda claim: artifact_calls.append(claim) or {}
    )
    monkeypatch.setattr(
        collector,
        "verify_delivery_bundle",
        lambda **_kwargs: _delivery(artifact_calls[0]),
    )

    outcome = instance.collect_one()

    assert outcome.status == "delivery_created"
    assert len(artifact_calls) == 1
    assert len(instance.store.list_rows("rca_delivery_jobs")) == 1
    assert instance.store.list_rows("rca_execution_watch")[0]["state"] == (
        "delivery_created"
    )


def test_late_host_observation_delivers_completed_vm_result(
    tmp_path, monkeypatch
):
    clock = [NOW]
    instance = _real_terminal_collector(
        tmp_path,
        clock=clock,
        status_reader=lambda task_id: {
            "success": True,
            "task_id": task_id,
            "state": "completed",
            "updated_at": (NOW - timedelta(seconds=2)).isoformat(),
            "meta": {
                "state": "completed",
                "updated_at": (NOW - timedelta(seconds=2)).isoformat(),
            },
        },
    )
    _age_submission(instance, seconds=1801)
    observed_claims = []
    instance.artifact_bundle_reader = lambda claim: observed_claims.append(claim) or {}
    monkeypatch.setattr(
        collector,
        "verify_delivery_bundle",
        lambda **_kwargs: _delivery(observed_claims[0]),
    )

    outcome = instance.collect_one()

    assert outcome.status == "delivery_created"
    [watch] = instance.store.list_rows("rca_execution_watch")
    assert watch["state"] == "delivery_created"
    assert watch["last_error_code"] == ""


def test_completed_status_does_not_use_meta_time_as_host_deadline(
    tmp_path, monkeypatch
):
    clock = [NOW]
    instance = _real_terminal_collector(
        tmp_path,
        clock=clock,
        status_reader=lambda task_id: {
            "success": True,
            "task_id": task_id,
            "state": "completed",
            "updated_at": (NOW - timedelta(seconds=2)).isoformat(),
            "meta": {
                "state": "running",
                "updated_at": (NOW - timedelta(seconds=2)).isoformat(),
            },
        },
    )
    _age_submission(instance, seconds=6 * 60 * 60)
    observed_claims = []
    instance.artifact_bundle_reader = (
        lambda claim: observed_claims.append(claim) or {}
    )
    monkeypatch.setattr(
        collector,
        "verify_delivery_bundle",
        lambda **_kwargs: _delivery(observed_claims[0]),
    )

    outcome = instance.collect_one()

    assert outcome.status == "delivery_created"


def test_valid_completed_bundle_crossing_old_host_deadline_is_delivered(
    tmp_path, monkeypatch
):
    clock = [NOW]
    observed_claim = []
    instance = _real_terminal_collector(
        tmp_path,
        clock=clock,
        status_reader=lambda task_id: {
            "success": True,
            "task_id": task_id,
            "state": "completed",
        },
    )
    _age_submission(instance, seconds=6 * 60 * 60)

    def bundle_reader(claim):
        observed_claim.append(claim)
        return {}

    def late_valid_delivery(**_kwargs):
        clock[0] = NOW + timedelta(seconds=1)
        return _delivery(observed_claim[0])

    instance.artifact_bundle_reader = bundle_reader
    monkeypatch.setattr(collector, "verify_delivery_bundle", late_valid_delivery)

    outcome = instance.collect_one()

    assert outcome.status == "delivery_created"
    assert len(instance.store.list_rows("rca_delivery_jobs")) == 1
    assert len(instance.store.list_rows("rca_delivery_effects")) == 1
    assert instance.store.list_rows("rca_execution_watch")[0]["state"] == (
        "delivery_created"
    )


def test_failure_receipt_reader_script_is_exact_and_read_only():
    claim = SimpleNamespace(
        submission_key="g1q3-rca-s1-" + "a" * 64,
        task_id="g1q3-rca-s1-" + "a" * 64,
    )
    script = collector._remote_failure_receipt_script(claim)

    assert "/mnt/tmp/g1q3-rca-s1-" + "a" * 64 in script
    assert "rca_service_result.json" in script
    assert "os.O_RDONLY" in script
    assert "O_NOFOLLOW" in script
    assert "os.O_WRONLY" not in script
    assert "os.O_RDWR" not in script
    assert "write" not in script.lower()
