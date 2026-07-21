from __future__ import annotations

import copy
import contextlib
import hashlib
import json
import os
import plistlib
import shutil
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from gateway import pnc_rca_prod_bootstrap as bootstrap
from gateway.pnc_rca_admission import build_rca_admission
from scripts import pnc_rca_prod_e2e_release as release


NOW = datetime(2026, 7, 21, 10, 0, tzinfo=timezone.utc)
VIEWER_ORIGIN = "https://viewer.minieye.tech"
RELEASE_ID = "rca-prod-e2e-20260721-c85f9db1"
EPOCH_ID = "rca-bootstrap-prod-e2e-20260721-c85f9db1"
STAGING_ROOT = "/mnt/tmp/rca-prod-e2e-c85f9db1/pipeline"
FINAL_ROOT = "/home/mini/.hermes/rca-prod-runtime/releases/rca-prod-c85f9db1"
MACHINE_IDENTITY = {"source": "test_machine_id", "sha256": "a1" * 32}


def test_exact_target_separates_internal_project_key_from_browser_slug():
    admission = build_rca_admission(
        project_key=release.TARGET_PROJECT_KEY,
        project_simple_name=release.TARGET_PROJECT_SIMPLE_NAME,
        work_item_type_key=release.TARGET_WORK_ITEM_TYPE_KEY,
        work_item_id=release.TARGET_WORK_ITEM_ID,
        rule_version="feishu-state-open-issue-v1",
        topic=release.TOPIC,
        partition=release.PARTITION,
        offset=release.TARGET_OFFSET,
    )

    assert release.TARGET_PROJECT_KEY == "68ef617fb371dc80a10641f7"
    assert release.TARGET_PROJECT_SIMPLE_NAME == "t03o4q"
    assert release.TARGET_ISSUE_URL == (
        "https://project.feishu.cn/t03o4q/issue/detail/7051585084"
    )
    assert admission.business_key == release.TARGET_BUSINESS_KEY
    assert admission.submission_key == release.TARGET_SUBMISSION_KEY


def _canonical(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode()


def _write_json(path: Path, value: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.parent.chmod(0o700)
    path.write_bytes(_canonical(value))
    path.chmod(0o600)
    return path


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _owned(path: Path) -> release.OwnedJson:
    raw = path.read_bytes()
    return release.OwnedJson(path.absolute(), raw, json.loads(raw))


def _candidate_body(*, phase: str, observed_at: datetime) -> dict:
    staging = phase == "staging"
    root = STAGING_ROOT if staging else FINAL_ROOT
    return {
        "schema_version": release.CANDIDATE_OBSERVATION_SCHEMA_VERSION,
        "release_id": RELEASE_ID,
        "phase": phase,
        "observed_at": observed_at.isoformat(),
        "root": root,
        "head": release.PIPELINE_COMMIT,
        "tree": release.PIPELINE_TREE,
        "status_porcelain_sha256": release.EMPTY_SHA256,
        "detached": True,
        "git_self_contained": True,
        "git_storage": {
            "dot_git_kind": "directory",
            "git_dir": f"{root}/.git",
            "git_common_dir": f"{root}/.git",
            "self_contained": True,
        },
        "filesystem": {
            "type": "cifs" if staging else "ext4",
            "mount_target": "/mnt/tmp" if staging else "/",
        },
        "seal": {
            "read_only": not staging,
            "write_probe_blocked": not staging,
        },
        "production_mutation": False,
        "docker_started": False,
        "mcap_started": False,
    }


def _candidate_result(owned: release.OwnedJson, phase: str) -> dict:
    body = owned.body
    return {
        "path": str(owned.path),
        "sha256": owned.sha256,
        "observed_at": body["observed_at"],
        "root": body["root"],
        "head": release.PIPELINE_COMMIT,
        "tree": release.PIPELINE_TREE,
        "phase": phase,
        "filesystem_type": body["filesystem"]["type"],
        "seal": body["seal"],
        "live_observer": {
            "transport": "ssh-mini-agent",
            "host": "mini@192.168.26.174",
            "script_sha256": "10" * 32,
        },
    }


def _report_service() -> dict:
    entrypoint = (
        f"{release.PIPELINE_SOURCE_ROOT}/{release.VM_REPORT_ENTRYPOINT_RELATIVE}"
    )
    return {
        "unit": release.VM_REPORT_UNIT,
        "entrypoint_relative": release.VM_REPORT_ENTRYPOINT_RELATIVE,
        "entrypoint_path": entrypoint,
        "entrypoint_sha256": release.VM_REPORT_ENTRYPOINT_SHA256,
        "entrypoint_git_mode": "100755",
        "candidate_unit_relative": release.VM_REPORT_UNIT_RELATIVE,
        "candidate_unit_path": (
            f"{release.PIPELINE_SOURCE_ROOT}/{release.VM_REPORT_UNIT_RELATIVE}"
        ),
        "candidate_unit_sha256": release.VM_REPORT_UNIT_SHA256,
        "candidate_unit_git_mode": "100644",
        "live_unit_path": release.VM_REPORT_LIVE_UNIT_PATH,
        "exec_start": [
            release.VM_INTERPRETER_PATH,
            "-I",
            "-B",
            entrypoint,
            "--root",
            release.VM_REPORT_ROOT,
            "--bind",
            "0.0.0.0",
            "--port",
            str(release.VM_REPORT_PORT),
            "--viewer-origin",
            f"${{{release.VM_REPORT_ENV_VARIABLE}}}",
        ],
        "effective_exec_start": [
            release.VM_INTERPRETER_PATH,
            "-I",
            "-B",
            entrypoint,
            "--root",
            release.VM_REPORT_ROOT,
            "--bind",
            "0.0.0.0",
            "--port",
            str(release.VM_REPORT_PORT),
            "--viewer-origin",
            VIEWER_ORIGIN,
        ],
        "environment_file_path": release.VM_REPORT_ENV_PATH,
        "environment_file_sha256": hashlib.sha256(
            f"{release.VM_REPORT_ENV_VARIABLE}={VIEWER_ORIGIN}\n".encode()
        ).hexdigest(),
        "environment_file_bytes": len(
            f"{release.VM_REPORT_ENV_VARIABLE}={VIEWER_ORIGIN}\n".encode()
        ),
        "environment_file_owner_uid": 1000,
        "environment_file_mode": "0600",
        "environment_variable": release.VM_REPORT_ENV_VARIABLE,
        "viewer_origin": VIEWER_ORIGIN,
        "working_directory": "/",
        "root": release.VM_REPORT_ROOT,
        "route_prefix": release.VM_REPORT_ROUTE_PREFIX,
        "port": release.VM_REPORT_PORT,
        "directory_listing": False,
        "path_traversal": False,
        "symlink_escape": False,
        "read_only": True,
        "old_broad_http_server_forbidden": True,
        "delivery_manifest_schema": "delivery_manifest_v2",
        "viz_manifest_schema": "g1q3_rca_viz_publication_v1",
        "max_concurrent_requests": 4,
        "request_queue_size": 16,
    }


def _host_environment_transition() -> dict:
    return {
        "path": release.CANONICAL_HOST_ENV,
        "pre_sha256": "58" * 32,
        "pre_bytes": 512,
        "pre_viewer_origin_count": 0,
        "pre_viewer_origin": None,
        "post_sha256": "59" * 32,
        "post_bytes": 560,
        "post_viewer_origin": VIEWER_ORIGIN,
        "write_required": True,
        "write_after_database_post_and_core_gate": True,
        "rollback_restores_exact_prestate": True,
        "secret_material_persisted": False,
    }


def _vm_report_environment_transition() -> dict:
    expected = f"{release.VM_REPORT_ENV_VARIABLE}={VIEWER_ORIGIN}\n".encode()
    return {
        "path": release.VM_REPORT_ENV_PATH,
        "pre_exists": False,
        "pre_sha256": release.EMPTY_SHA256,
        "pre_bytes": 0,
        "pre_owner_uid": None,
        "pre_mode": None,
        "post_sha256": hashlib.sha256(expected).hexdigest(),
        "post_bytes": len(expected),
        "post_owner_uid": 1000,
        "post_mode": "0600",
        "post_viewer_origin": VIEWER_ORIGIN,
        "parent_path": str(Path(release.VM_REPORT_ENV_PATH).parent),
        "pre_parent_exists": False,
        "pre_parent_owner_uid": None,
        "pre_parent_mode": None,
        "post_parent_owner_uid": 1000,
        "post_parent_mode": "0700",
        "parent_create_after_database_post_and_core_gate": True,
        "rollback_restores_exact_parent_prestate": True,
        "write_required": True,
        "write_after_database_post_and_core_gate": True,
        "rollback_restores_exact_prestate": True,
    }


def _viewer_proxy_candidate() -> dict:
    prestate = {
        "observed_at": NOW.isoformat(),
        "installed_include_path": "/etc/nginx/conf.d/g1q3-rca-artifacts.conf",
        "include_present": False,
        "include_sha256": release.EMPTY_SHA256,
        "effective_config_sha256": "81" * 32,
        "nginx_config_test_passed": True,
        "binary_path": "/usr/sbin/nginx",
        "binary_sha256": "82" * 32,
        "version": "nginx/1.27.5",
        "service_identity": "nginx.service",
        "main_pid": 217,
        "process_executable": "/usr/sbin/nginx",
        "process_argv_sha256": "83" * 32,
        "process_cwd": "/",
        "root_status": 200,
        "artifact_status": 200,
        "artifact_content_type": "text/html",
        "artifact_body_sha256": "84" * 32,
        "route_is_spa_fallback": True,
    }
    prestate["rollback_capture_sha256"] = release._sha256_value(prestate)
    return {
        "schema_version": release.VIEWER_PROXY_CANDIDATE_SCHEMA_VERSION,
        "observed_at": NOW.isoformat(),
        "public_origin": VIEWER_ORIGIN,
        "expected_viewer_address": release.VIEWER_EXPECTED_ADDRESS,
        "route_prefix": release.VIEWER_PROXY_ROUTE_PREFIX,
        "upstream_origin": release.VIEWER_PROXY_UPSTREAM_ORIGIN,
        "config": {
            "path": release.VIEWER_PROXY_CONFIG_PATH,
            "sha256": release.VIEWER_PROXY_CONFIG_SHA256,
            "bytes": release.VIEWER_PROXY_CONFIG_BYTES,
        },
        "static_validation": {
            "path": release.VIEWER_PROXY_STATIC_RECEIPT_PATH,
            "sha256": release.VIEWER_PROXY_STATIC_RECEIPT_SHA256,
        },
        "dns_preread": {
            "observed_at": NOW.isoformat(),
            "resolver": "system",
            "hostname": "viewer.minieye.tech",
            "canonical_name": "viewer.minieye.tech",
            "addresses": [release.VIEWER_EXPECTED_ADDRESS],
            "selected_address": release.VIEWER_EXPECTED_ADDRESS,
            "lookup_succeeded": True,
        },
        "tls_preread": {
            "observed_at": NOW.isoformat(),
            "hostname": "viewer.minieye.tech",
            "server_address": release.VIEWER_EXPECTED_ADDRESS,
            "server_port": 443,
            "hostname_verified": True,
            "verification_errors": [],
            "certificate_subject": "CN=*.minieye.tech",
            "certificate_issuer": "CN=Test CA",
            "san_dns_names": ["*.minieye.tech", "minieye.tech"],
            "matched_san_dns_name": "*.minieye.tech",
            "certificate_der_sha256": "85" * 32,
            "spki_der_sha256": "86" * 32,
            "not_before": (NOW - timedelta(days=1)).isoformat(),
            "not_after": (NOW + timedelta(days=1)).isoformat(),
        },
        "nginx_prestate": prestate,
        "production_mutation": False,
    }


def _viewer_proxy_live_body(
    *,
    candidate: dict,
    report_service: dict,
    report_restart: dict,
    reloaded_at: datetime,
    observed_at: datetime,
) -> dict:
    origin = candidate["public_origin"]
    key = release.VIEWER_DIAGNOSTIC_SUBMISSION_KEY
    artifact_url = (
        f"{origin}{release.VIEWER_PROXY_ROUTE_PREFIX}{key}/{key}.viz.mcap"
    )
    viewer_url = (
        f"{origin}/?ds=remote-file&ds.url={release.quote(artifact_url, safe='')}"
    )
    common = {
        "content_type": "application/octet-stream",
        "accept_ranges": "bytes",
        "cors_allow_origin": origin,
    }
    return {
        "schema_version": release.VIEWER_PROXY_LIVE_SCHEMA_VERSION,
        "observed_at": observed_at.isoformat(),
        "public_origin": origin,
        "dns": candidate["dns_preread"],
        "tls": candidate["tls_preread"],
        "nginx_live": {
            "installed_include_path": candidate["nginx_prestate"][
                "installed_include_path"
            ],
            "include_sha256": release.VIEWER_PROXY_CONFIG_SHA256,
            "include_bytes": release.VIEWER_PROXY_CONFIG_BYTES,
            "include_owner_uid": 0,
            "include_mode": "0644",
            "binary_path": candidate["nginx_prestate"]["binary_path"],
            "binary_sha256": candidate["nginx_prestate"]["binary_sha256"],
            "version": candidate["nginx_prestate"]["version"],
            "service_identity": candidate["nginx_prestate"]["service_identity"],
            "main_pid": 218,
            "process_executable": candidate["nginx_prestate"]["binary_path"],
            "process_argv_sha256": "87" * 32,
            "process_cwd": candidate["nginx_prestate"]["process_cwd"],
            "nginx_config_test_passed": True,
            "effective_config_sha256": "88" * 32,
            "effective_location_count": 1,
            "reload_performed": True,
            "reloaded_at": reloaded_at.isoformat(),
        },
        "upstream": {
            "origin": release.VIEWER_PROXY_UPSTREAM_ORIGIN,
            "viewer_origin": origin,
            "report_service_unit": release.VM_REPORT_UNIT,
            "report_entrypoint_sha256": report_service["entrypoint_sha256"],
            "report_unit_config_sha256": report_service[
                "candidate_unit_sha256"
            ],
            "report_main_pid": report_restart["new_pid"],
            "legacy_html_health_passed": True,
            "exact_artifact_contract_passed": True,
        },
        "http_contract": {
            "artifact_url": artifact_url,
            "submission_key": key,
            "artifact_sha256": release.VIEWER_DIAGNOSTIC_SHA256,
            "artifact_bytes": release.VIEWER_DIAGNOSTIC_BYTES,
            "head": {
                "method": "HEAD",
                "status": 200,
                "body_bytes": 0,
                "body_sha256": release.EMPTY_SHA256,
                "content_length": release.VIEWER_DIAGNOSTIC_BYTES,
                **common,
                "content_range": "",
            },
            "get": {
                "method": "GET",
                "status": 200,
                "body_bytes": release.VIEWER_DIAGNOSTIC_BYTES,
                "body_sha256": release.VIEWER_DIAGNOSTIC_SHA256,
                "content_length": release.VIEWER_DIAGNOSTIC_BYTES,
                **common,
                "content_range": "",
            },
            "range": {
                "method": "GET bytes=0-2234",
                "status": 206,
                "body_bytes": release.VIEWER_DIAGNOSTIC_BYTES,
                "body_sha256": release.VIEWER_DIAGNOSTIC_SHA256,
                "content_length": release.VIEWER_DIAGNOSTIC_BYTES,
                **common,
                "content_range": "bytes 0-2234/2235",
            },
            "unsatisfiable_range": {
                "method": "GET bytes=2235-",
                "status": 416,
                "body_bytes": 0,
                "body_sha256": release.EMPTY_SHA256,
                "content_length": 0,
                **common,
                "content_range": "bytes */2235",
            },
            "options": {
                "method": "OPTIONS",
                "status": 204,
                "body_bytes": 0,
                "body_sha256": release.EMPTY_SHA256,
                "content_length": 0,
                "content_type": "",
                "accept_ranges": "bytes",
                "content_range": "",
                "cors_allow_origin": origin,
            },
            "rejected_paths": {
                "wrong_version": 404,
                "mismatched_filename": 404,
                "directory": 404,
                "traversal": 404,
                "encoded_separator": 404,
                "query_string": 404,
            },
            "rejected_methods": {
                "CONNECT": 403,
                "DELETE": 403,
                "PATCH": 403,
                "POST": 403,
                "PUT": 403,
                "TRACE": 403,
            },
            "server_header_absent": True,
        },
        "browser": {
            "engine": "playwright.chromium",
            "browser_version": "150.0.0.0",
            "executable_sha256": "89" * 32,
            "ignore_https_errors": False,
            "viewer_url": viewer_url,
            "artifact_url": artifact_url,
            "navigation_status": 200,
            "network_artifact_statuses": [200, 206],
            "artifact_sha256": release.VIEWER_DIAGNOSTIC_SHA256,
            "artifact_bytes": release.VIEWER_DIAGNOSTIC_BYTES,
            "expected_topic": release.VIEWER_DIAGNOSTIC_TOPIC,
            "expected_topic_visible": True,
            "remote_file_source_selected": True,
            "viewer_title_binds_artifact": True,
            "page_errors": [],
            "player_errors": [],
            "mixed_content_errors": [],
            "certificate_errors": [],
            "unexpected_request_failures": [],
            "screenshot_sha256": "8a" * 32,
        },
        "production_mutation": True,
    }


def _component_value(component_path: Path) -> dict:
    rca_runtime_files = {
        "gateway/pnc_rca_control_store.py": "31" * 32,
        "scripts/pnc_rca_kafka_consumer.py": "30" * 32,
    }
    gateway_runtime_files = {
        "gateway/pnc_rca_control_store.py": "31" * 32,
        "gateway/run.py": "2f" * 32,
    }
    runtime_files = {
        **rca_runtime_files,
        **gateway_runtime_files,
    }
    runtime_allowlists = {
        "rca": sorted(rca_runtime_files),
        "gateway": sorted(gateway_runtime_files),
    }
    runtime_allowlists.update(
        {
            "union": sorted(runtime_files),
            "allowlist_sha256": release._sha256_value(runtime_allowlists),
            "probe_script_sha256": "2e" * 32,
        }
    )
    rca_runtime_sha256 = release._sha256_value(rca_runtime_files)
    gateway_runtime_sha256 = release._sha256_value(gateway_runtime_files)
    dependency = {
        "observation": {
            "venv_root": f"{release.HOST_LIVE_ROOT}/.venv",
            "interpreter_path": f"{release.HOST_LIVE_ROOT}/.venv/bin/python",
            "interpreter_sha256": "32" * 32,
            "pyvenv_config_sha256": "33" * 32,
            "python_version": "3.11.15",
            "site_packages_file_count": 100,
            "site_packages_manifest_sha256": "34" * 32,
            "installed_distribution_count": 10,
            "installed_distribution_manifest_sha256": "35" * 32,
            "site_packages_pyc_policy": "immutable_build_manifest",
            "probe_script_sha256": "36" * 32,
        },
        "lock": {"path": "/owner/dependency-lock.json", "sha256": "37" * 32},
        "build_receipt": {
            "path": "/owner/dependency-build.json",
            "sha256": "38" * 32,
        },
    }
    host = {
        "root": release.CANONICAL_HOST_ROOT,
        "commit": release.HOST_FINAL_COMMIT,
        "tree": release.HOST_FINAL_TREE,
        "tree_clean": True,
        "status_sha256": release.EMPTY_SHA256,
        "quarantine_baseline_ancestor": release.HOST_QUARANTINE_BASELINE_COMMIT,
        "quarantine_baseline_ancestor_verified": True,
        "delivery_store_schema_version": release.DELIVERY_STORE_TARGET_SCHEMA,
        "viewer_origin": VIEWER_ORIGIN,
        "host_environment_transition": _host_environment_transition(),
        "canonical_interpreter_sha256": "c4" * 32,
        "required_file_sha256": {
            "scripts/pnc_rca_kafka_consumer.py": "c0" * 32
        },
        "host_environment_transition": _host_environment_transition(),
        "candidate_identity_evidence": {
            "path": release.HOST_CANDIDATE_IDENTITY_PATH,
            "sha256": release.HOST_CANDIDATE_IDENTITY_SHA256,
            "observed_at": NOW.isoformat(),
        },
        "required_file_sha256": runtime_files,
        "runtime_allowlists": runtime_allowlists,
        "service_config_sha256": {name: "40" * 32 for name in release.HOST_SERVICE_LABELS},
        "runtime_file_sha256": runtime_files,
        "runtime_files_sha256": release._sha256_value(runtime_files),
        "rca_runtime_file_sha256": rca_runtime_files,
        "rca_runtime_files_sha256": rca_runtime_sha256,
        "gateway_runtime_file_sha256": gateway_runtime_files,
        "gateway_runtime_files_sha256": gateway_runtime_sha256,
        "service_runtime_files_sha256": {
            label: (
                gateway_runtime_sha256
                if label == "ai.hermes.gateway"
                else rca_runtime_sha256
            )
            for label in release.HOST_SERVICE_LABELS
        },
        "canonical_interpreter_sha256": "41" * 32,
        "dependency_environment": dependency,
        "retired_executor_paths_absent": list(release.RETIRED_EXECUTOR_PATHS),
    }
    worker = {
        "evidence_path": "/owner/worker.json",
        "evidence_sha256": "42" * 32,
        "schema_version": "pnc_rca_vm_worker_identity_observation_v2",
        "observed_at": NOW.isoformat(),
        "root": release.VM_WORKER_ROOT,
        "commit": "43" * 20,
        "tree": "44" * 20,
        "tree_clean": True,
        "status_sha256": release.EMPTY_SHA256,
        "entrypoint": release.VM_WORKER_ENTRYPOINT,
        "entrypoint_sha256": "45" * 32,
        "entrypoint_git_mode": "100644",
        "runtime_artifact_sha256": {name: "46" * 32 for name in release.VM_WORKER_RUNTIME_FILES},
        "loaded_runtime_sha256": "47" * 32,
        "interpreter_path": release.VM_INTERPRETER_PATH,
        "interpreter_sha256": "48" * 32,
        "daemon_unit_config_sha256": "49" * 32,
        "report_unit_config_sha256": _report_service()["candidate_unit_sha256"],
        "report_environment": {
            "path": release.VM_REPORT_ENV_PATH,
            "exists": True,
            "sha256": _report_service()["environment_file_sha256"],
            "bytes": _report_service()["environment_file_bytes"],
            "owner_uid": 1000,
            "mode": "0600",
            "variable": release.VM_REPORT_ENV_VARIABLE,
            "viewer_origin": VIEWER_ORIGIN,
        },
        "report_environment_transition": _vm_report_environment_transition(),
        "report_environment_transition": {
            **_vm_report_environment_transition(),
            "pre_exists": True,
            "pre_sha256": _report_service()["environment_file_sha256"],
            "pre_bytes": _report_service()["environment_file_bytes"],
            "pre_owner_uid": 1000,
            "pre_mode": "0600",
            "write_required": False,
        },
        "report_environment_transition": _vm_report_environment_transition(),
        "observer": {
            "transport": "ssh-mini-agent",
            "host": "mini@192.168.26.174",
            "command_sha256": "50" * 32,
            "machine_identity_sha256": "51" * 32,
        },
        "production_mutation": False,
    }
    workspace = {
        "schema_version": "pnc_rca_workspace_runtime_binding_v1",
        "root": "/owner/workspace",
        "manifest_path": "/owner/workspace/manifest.json",
        "creator_path": "/owner/workspace/create.py",
        "manifest_sha256": "52" * 32,
        "closure_sha256": "53" * 32,
        "source_commit": "54" * 20,
        "file_sha256": {"runtime.py": "55" * 32},
        "runtime_sha256": "56" * 32,
    }
    pipeline = {
        "root": release.PIPELINE_SOURCE_ROOT,
        "commit": release.PIPELINE_COMMIT,
        "tree": release.PIPELINE_TREE,
        "tree_clean": True,
        "entrypoint": f"{release.PIPELINE_SOURCE_ROOT}/{release.PIPELINE_ENTRYPOINT}",
        "entrypoint_sha256": release.PIPELINE_ENTRYPOINT_SHA256,
        "entrypoint_git_mode": "100755",
        "candidate_audit": {
            "vm_path": release.PIPELINE_CANDIDATE_AUDIT_VM_PATH,
            "cifs_path": release.PIPELINE_CANDIDATE_AUDIT_CIFS_PATH,
            "sha256": release.PIPELINE_CANDIDATE_AUDIT_SHA256,
        },
        "report_service": _report_service(),
    }
    admission = {
        "mode": "hmac_sha256",
        "method": "prod_admission_hmac_key_fingerprint_v1",
        "environment_variable": release.ADMISSION_HMAC_ENV,
        "host_key_fingerprint": "57" * 32,
        "host_config_path": release.CANONICAL_HOST_ENV,
        "host_config_pre_sha256": _host_environment_transition()["pre_sha256"],
        "host_config_post_sha256": _host_environment_transition()["post_sha256"],
        "host_config_write_required": True,
        "host_probe_script_sha256": "5a" * 32,
        "host_interpreter_sha256": "41" * 32,
        "vm_key_fingerprint": "57" * 32,
        "vm_observation": {"observation_path": "/owner/hmac.json", "observation_sha256": "60" * 32},
        "fingerprints_match": True,
        "secure_stream_required": True,
        "secret_material_persisted": False,
        "vm_observation_evidence": {
            "path": "/owner/hmac.json",
            "sha256": "60" * 32,
            "observed_at": NOW.isoformat(),
            "loaded_runtime_sha256": "47" * 32,
        },
    }
    return {
        "path": str(component_path.absolute()),
        "sha256": _sha(component_path),
        "observed_at": NOW.isoformat(),
        "host": host,
        "workspace": workspace,
        "worker": worker,
        "pipeline": pipeline,
        "viewer_proxy": _viewer_proxy_candidate(),
        "admission_security": admission,
    }


@pytest.fixture
def release_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict:
    evidence = tmp_path / "evidence"
    output = tmp_path / "release"
    receipts = tmp_path / "receipts"
    for path in (evidence, output, receipts):
        path.mkdir(mode=0o700)
    paths = {
        name: _write_json(evidence / f"{name}.json", {"name": name, "observed_at": NOW.isoformat()})
        for name in ("gap", "field", "input", "closure", "cross", "component", "db")
    }
    paths["staging"] = _write_json(
        evidence / "staging.json", _candidate_body(phase="staging", observed_at=NOW)
    )
    component = _component_value(paths["component"])

    def gap(owned):
        return {
            "path": str(owned.path),
            "sha256": owned.sha256,
            "observed_at": NOW.isoformat(),
            "missing_live_count": release.MISSING_LIVE_COUNT,
            "deferred_missing_count": release.DEFERRED_MISSING_COUNT,
            "target": {"event_uid": release.TARGET_EVENT_UID},
        }

    def field(owned, **_kwargs):
        return {
            "path": str(owned.path),
            "sha256": owned.sha256,
            "observed_at": NOW.isoformat(),
            "project_key": release.TARGET_PROJECT_KEY,
            "work_item_id": release.TARGET_WORK_ITEM_ID,
            "empty_field_keys": list(release.TARGET_FIELD_KEYS),
        }

    def input_gate(owned, **_kwargs):
        return {
            "path": str(owned.path),
            "sha256": owned.sha256,
            "observed_at": NOW.isoformat(),
            "source": "meegle",
            "status": "fields_extracted",
            "context_sha256": "2d" * 32,
            "context_utf8_bytes": 954,
            "remote_reference_valid": True,
            "frame_reference_valid": True,
            "function_category_present": True,
            "validation_blocker_kind": "",
        }

    def closure(owned):
        return {"path": str(owned.path), "sha256": owned.sha256, "reachable_hit_count": 0, "entrypoint": release.PIPELINE_ENTRYPOINT}

    def candidate(owned, *, expected_phase, **_kwargs):
        return _candidate_result(owned, expected_phase)

    def components(owned, **_kwargs):
        result = copy.deepcopy(component)
        result["path"] = str(owned.path)
        result["sha256"] = owned.sha256
        return result

    def cross(owned, **_kwargs):
        return {
            "path": str(owned.path),
            "sha256": owned.sha256,
            "issue_id": release.TARGET_WORK_ITEM_ID,
            "structural_contract": {
                "project_key": release.TARGET_PROJECT_KEY,
                "project_simple_name": release.TARGET_PROJECT_SIMPLE_NAME,
                "work_item_id": release.TARGET_WORK_ITEM_ID,
                "issue_url": release.TARGET_ISSUE_URL,
                "field_keys": list(release.TARGET_FIELD_KEYS),
                "result_nonempty": True,
                "report_is_manifest_html_url": True,
                "production_values_predetermined": False,
                "production_lineage_predetermined": False,
            },
            "send_performed": False,
        }

    def db(owned, **_kwargs):
        return {
            "path": str(owned.path),
            "sha256": owned.sha256,
            "approved_source_logical_sha256": "61" * 32,
            "approved_post_migration_logical_sha256": "62" * 32,
            "quarantine_core": {"core_sha256": "63" * 32},
        }

    monkeypatch.setattr(release, "_validate_gap_ledger", gap)
    monkeypatch.setattr(release, "_validate_field_preread", field)
    monkeypatch.setattr(release, "_validate_input_preread", input_gate)
    monkeypatch.setattr(release, "_validate_closure_audit", closure)
    monkeypatch.setattr(release, "_validate_candidate_observation", candidate)
    monkeypatch.setattr(release, "_validate_component_binding", components)
    monkeypatch.setattr(release, "_validate_cross_contract_pass", cross)
    monkeypatch.setattr(release, "_validate_db_cutover_binding", db)
    built = release.build_request(
        release_id=RELEASE_ID,
        bootstrap_epoch_id=EPOCH_ID,
        final_candidate_root=FINAL_ROOT,
        gap_ledger_path=paths["gap"],
        field_preread_path=paths["field"],
        input_preread_path=paths["input"],
        closure_audit_path=paths["closure"],
        staging_observation_path=paths["staging"],
        cross_contract_pass_path=paths["cross"],
        component_binding_path=paths["component"],
        db_cutover_binding_path=paths["db"],
        output_dir=output,
        now=NOW,
        machine_identity_provider=lambda: MACHINE_IDENTITY,
    )
    return {
        "root": tmp_path,
        "evidence": evidence,
        "receipts": receipts,
        "paths": paths,
        "component": component,
        "request": Path(built["approval_request_path"]),
        "build": built,
    }


def _release_approval(fixture: dict, created_at: datetime) -> tuple[Path, dict]:
    request = json.loads(fixture["request"].read_text())
    body = {
        "schema_version": release.APPROVAL_SCHEMA_VERSION,
        "approval_id": "owner-prod-e2e-c85f9db1",
        "decision": release.APPROVAL_DECISION,
        "release_id": RELEASE_ID,
        "created_at": created_at.isoformat(),
        "expires_at": (created_at + timedelta(hours=1)).isoformat(),
        "nonce": "prod_e2e_nonce_c85f9db1",
        "authorized_role": "owner",
        "action_set": list(release.ACTION_SET),
        "action_set_sha256": release._sha256_value(list(release.ACTION_SET)),
        "approval_request_sha256": _sha(fixture["request"]),
        "release_bom_sha256": request["bindings"]["release_bom_sha256"],
        "identity": request["approval_identity_requirement"],
    }
    return _write_json(fixture["evidence"] / "approval.json", body), body


def _baseline_approval(fixture: dict, created_at: datetime) -> tuple[Path, dict]:
    request = json.loads(fixture["request"].read_text())
    identity = request["approval_identity_requirement"]
    body = {
        "schema_version": release.BASELINE_APPROVAL_SCHEMA_VERSION,
        "release_id": RELEASE_ID,
        "release_bom_sha256": request["bindings"]["release_bom_sha256"],
        "quarantine_core_sha256": request["bindings"]["release_bom"]["delivery_store_cutover"]["quarantine_core"]["core_sha256"],
        "decision": release.BASELINE_APPROVAL_DECISION,
        "identity": {"uid": identity["uid"], "username": identity["username"]},
        "created_at": created_at.isoformat(),
    }
    return _write_json(fixture["evidence"] / "baseline-approval.json", body), body


def _authorization(fixture: dict, approval_path: Path, approval: dict, issued_at: datetime) -> Path:
    request = json.loads(fixture["request"].read_text())
    body = bootstrap.issue_bootstrap_authorization(
        bootstrap_epoch_id=EPOCH_ID,
        started_at=NOW,
        deadline=NOW + timedelta(hours=2),
        release_approval_id=approval["approval_id"],
        release_bom_sha256=request["bindings"]["release_bom_sha256"],
        approval_evidence_sha256=_sha(approval_path),
        authorized_by="owner-user",
        authorized_role="owner",
        now=issued_at,
        receipt_id="bootstrap-auth-c85f9db1",
    )
    return _write_json(fixture["evidence"] / "bootstrap.json", body)


def test_build_request_binds_new_host_services_report_and_distinct_approval(release_fixture):
    request = json.loads(release_fixture["request"].read_text())
    bom = request["bindings"]["release_bom"]
    assert bom["host_runtime"]["canonical_final"]["commit"] == release.HOST_FINAL_COMMIT
    assert release.HOST_FINAL_COMMIT == "ecc6c747c8abbf1f815d8783511c7f96bf080bba"
    assert len(bom["restart_scope"]["host_launchd_labels"]) == 6
    assert "local.pnc.completion-notice-relay" in bom["restart_scope"]["host_launchd_labels"]
    assert bom["restart_scope"]["vm_systemd_units"] == list(release.VM_SERVICE_UNITS)
    assert bom["component_identities"]["pipeline"]["report_service"]["directory_listing"] is False
    baseline = request["quarantine_baseline_approval_requirement"]
    assert baseline["decision"] == release.BASELINE_APPROVAL_DECISION
    assert baseline["must_be_distinct_from_prod_e2e_approval"] is True
    assert bom["feishu_completion"]["terminal_cross_contract_pass"]["structural_contract"]["production_values_predetermined"] is False
    assert request["production_effects_executed"] is False


def test_final_validation_binds_approval_auth_nonce_preflight_and_execute_before(
    release_fixture, monkeypatch
):
    approval_path, approval_body = _release_approval(
        release_fixture, NOW + timedelta(minutes=1)
    )
    baseline_path, _ = _baseline_approval(
        release_fixture, NOW + timedelta(minutes=2)
    )
    auth_path = _authorization(
        release_fixture, approval_path, approval_body, NOW + timedelta(minutes=3)
    )
    final = _write_json(
        release_fixture["evidence"] / "final.json",
        _candidate_body(phase="final", observed_at=NOW + timedelta(minutes=4)),
    )
    preflight = _write_json(release_fixture["evidence"] / "preflight.json", {"ok": True})
    backup = _write_json(release_fixture["evidence"] / "backup.json", {"db": True})
    anchors = {"live_env_path": release.CANONICAL_HOST_ENV}

    def validate_preflight(owned, **kwargs):
        return {
            "path": str(owned.path),
            "sha256": owned.sha256,
            "observed_at": (NOW + timedelta(minutes=4)).isoformat(),
            "writers_stopped_at": (NOW + timedelta(minutes=4)).isoformat(),
            "host_services": {},
            "host_live_runtime": {},
            "vm_services": {},
            "executor_closure": {},
            "fresh_live_backup": {
                "path": str(backup),
                "sha256": _sha(backup),
                "size_bytes": backup.stat().st_size,
                "captured_at": (NOW + timedelta(minutes=4)).isoformat(),
                "logical_sha256": kwargs["db_cutover"]["approved_source_logical_sha256"],
            },
            "activation_anchors_before": anchors,
            "rollback_contract": {},
        }

    nonce_root = release_fixture["root"] / "nonce-ledger"
    monkeypatch.setattr(release, "APPROVAL_NONCE_LEDGER_ROOT", nonce_root)
    monkeypatch.setattr(release, "_validate_execution_preflight", validate_preflight)
    receipt_path = release_fixture["receipts"] / "final-validation.json"
    result = release.validate_only(
        phase="final",
        request_path=release_fixture["request"],
        candidate_observation_path=final,
        receipt_path=receipt_path,
        approval_path=approval_path,
        quarantine_baseline_approval_path=baseline_path,
        bootstrap_authorization_path=auth_path,
        execution_preflight_path=preflight,
        now=NOW + timedelta(minutes=5),
        machine_identity_provider=lambda: MACHINE_IDENTITY,
    )
    assert result["production_ready"] is True
    assert result["quarantine_baseline_approval"]["sha256"] == _sha(baseline_path)
    assert result["approval_nonce_claim"]["consumed"] is True
    assert result["execute_before"] == (NOW + timedelta(minutes=10)).isoformat()
    with pytest.raises(release.ProdE2EReleaseError, match="nonce_replayed"):
        release.validate_only(
            phase="final",
            request_path=release_fixture["request"],
            candidate_observation_path=final,
            receipt_path=release_fixture["receipts"] / "second-final.json",
            approval_path=approval_path,
            quarantine_baseline_approval_path=baseline_path,
            bootstrap_authorization_path=auth_path,
            execution_preflight_path=preflight,
            now=NOW + timedelta(minutes=5),
            machine_identity_provider=lambda: MACHINE_IDENTITY,
        )


def test_prod_approval_cannot_be_reused_as_baseline_approval(release_fixture):
    approval_path, approval_body = _release_approval(release_fixture, NOW + timedelta(minutes=1))
    request = release._validate_request(
        _owned(release_fixture["request"]),
        machine_identity_provider=lambda: MACHINE_IDENTITY,
    )
    release_approval = release._validate_approval(
        _owned(approval_path), request=request, now=NOW + timedelta(minutes=2)
    )
    with pytest.raises(release.ProdE2EReleaseError, match="baseline_approval_shape"):
        release._validate_quarantine_baseline_approval(
            _owned(approval_path),
            request=request,
            release_approval=release_approval,
            now=NOW + timedelta(minutes=2),
        )
    assert approval_body["decision"] != release.BASELINE_APPROVAL_DECISION


def test_approval_rejects_expired_or_non_increasing_window(release_fixture):
    approval_path, body = _release_approval(release_fixture, NOW + timedelta(minutes=1))
    body["expires_at"] = body["created_at"]
    _write_json(approval_path, body)
    request = release._validate_request(
        _owned(release_fixture["request"]),
        machine_identity_provider=lambda: MACHINE_IDENTITY,
    )
    with pytest.raises(release.ProdE2EReleaseError, match="approval_invalid"):
        release._validate_approval(
            _owned(approval_path), request=request, now=NOW + timedelta(minutes=2)
        )


def test_publish_no_clobber_writes_all_bytes_and_rejects_zero_progress(tmp_path, monkeypatch):
    root = tmp_path / "out"
    root.mkdir(mode=0o700)
    original = os.write

    def short_write(fd, payload):
        return original(fd, bytes(payload[:3]))

    monkeypatch.setattr(release.os, "write", short_write)
    path = root / "receipt.json"
    release._publish_no_clobber(path, {"payload": "x" * 100})
    assert json.loads(path.read_text())["payload"] == "x" * 100

    monkeypatch.setattr(release.os, "write", lambda _fd, _payload: 0)
    failed = root / "failed.json"
    with pytest.raises(release.ProdE2EReleaseError, match="output_write_failed"):
        release._publish_no_clobber(failed, {"payload": "never partial"})
    assert not failed.exists()


def test_component_binding_pins_superseding_host_and_report_service(tmp_path, monkeypatch):
    component_path = _write_json(tmp_path / "component.json", {"placeholder": True})
    template_host = _component_value(component_path)["host"]
    dependency = template_host["dependency_environment"]
    observed_host = {
        "commit": release.HOST_FINAL_COMMIT,
        "tree": release.HOST_FINAL_TREE,
        "candidate_identity_evidence": template_host[
            "candidate_identity_evidence"
        ],
        "required_file_sha256": template_host["required_file_sha256"],
        "runtime_allowlists": template_host["runtime_allowlists"],
        "service_config_sha256": {name: "72" * 32 for name in release.HOST_SERVICE_LABELS},
    }
    runtime_files = template_host["runtime_file_sha256"]
    probe = {
        "runtime_files": runtime_files,
        "runtime_files_sha256": release._sha256_value(runtime_files),
        "rca_runtime_files": template_host["rca_runtime_file_sha256"],
        "rca_runtime_files_sha256": template_host["rca_runtime_files_sha256"],
        "gateway_runtime_files": template_host["gateway_runtime_file_sha256"],
        "gateway_runtime_files_sha256": template_host["gateway_runtime_files_sha256"],
        "service_runtime_files_sha256": template_host["service_runtime_files_sha256"],
        "interpreter_sha256": "74" * 32,
        "hmac_key_fingerprint": "75" * 32,
        "host_env_current_sha256": _host_environment_transition()["pre_sha256"],
        "host_env_current_bytes": _host_environment_transition()["pre_bytes"],
        "host_env_current_viewer_origin_count": 0,
        "host_env_current_viewer_origin": None,
        "host_env_planned_sha256": _host_environment_transition()["post_sha256"],
        "host_env_planned_bytes": _host_environment_transition()["post_bytes"],
        "host_env_planned_viewer_origin": VIEWER_ORIGIN,
        "probe_script_sha256": "77" * 32,
    }
    workspace = {"runtime_sha256": "78" * 32}
    worker = copy.deepcopy(_component_value(component_path)["worker"])
    report = _report_service()
    worker["report_unit_config_sha256"] = report["candidate_unit_sha256"]
    pipeline = {
        "root": release.PIPELINE_SOURCE_ROOT,
        "commit": release.PIPELINE_COMMIT,
        "tree": release.PIPELINE_TREE,
        "tree_clean": True,
        "entrypoint": f"{release.PIPELINE_SOURCE_ROOT}/{release.PIPELINE_ENTRYPOINT}",
        "entrypoint_sha256": release.PIPELINE_ENTRYPOINT_SHA256,
        "entrypoint_git_mode": "100755",
        "candidate_audit": {
            "vm_path": release.PIPELINE_CANDIDATE_AUDIT_VM_PATH,
            "cifs_path": release.PIPELINE_CANDIDATE_AUDIT_CIFS_PATH,
            "sha256": release.PIPELINE_CANDIDATE_AUDIT_SHA256,
        },
        "report_service": report,
    }
    hmac_ref = {"observation_path": str(tmp_path / "hmac.json"), "observation_sha256": "79" * 32}
    hmac = {
        "key_fingerprint": probe["hmac_key_fingerprint"],
        "evidence_path": hmac_ref["observation_path"],
        "evidence_sha256": hmac_ref["observation_sha256"],
        "observed_at": NOW.isoformat(),
        "loaded_runtime_sha256": worker["loaded_runtime_sha256"],
    }
    security = {
        "mode": "hmac_sha256",
        "method": "prod_admission_hmac_key_fingerprint_v1",
        "environment_variable": release.ADMISSION_HMAC_ENV,
        "host_key_fingerprint": probe["hmac_key_fingerprint"],
        "host_config_path": release.CANONICAL_HOST_ENV,
        "host_config_pre_sha256": probe["host_env_current_sha256"],
        "host_config_post_sha256": probe["host_env_planned_sha256"],
        "host_config_write_required": True,
        "host_probe_script_sha256": probe["probe_script_sha256"],
        "host_interpreter_sha256": probe["interpreter_sha256"],
        "vm_key_fingerprint": hmac["key_fingerprint"],
        "vm_observation": hmac_ref,
        "fingerprints_match": True,
        "secure_stream_required": True,
        "secret_material_persisted": False,
    }
    host = {
        "root": release.CANONICAL_HOST_ROOT,
        "commit": release.HOST_FINAL_COMMIT,
        "tree": release.HOST_FINAL_TREE,
        "tree_clean": True,
        "status_sha256": release.EMPTY_SHA256,
        "quarantine_baseline_ancestor": release.HOST_QUARANTINE_BASELINE_COMMIT,
        "quarantine_baseline_ancestor_verified": True,
        "delivery_store_schema_version": release.DELIVERY_STORE_TARGET_SCHEMA,
        "viewer_origin": VIEWER_ORIGIN,
        "host_environment_transition": _host_environment_transition(),
        "candidate_identity_evidence": observed_host[
            "candidate_identity_evidence"
        ],
        "required_file_sha256": observed_host["required_file_sha256"],
        "runtime_allowlists": observed_host["runtime_allowlists"],
        "service_config_sha256": observed_host["service_config_sha256"],
        "runtime_file_sha256": runtime_files,
        "runtime_files_sha256": probe["runtime_files_sha256"],
        "rca_runtime_file_sha256": probe["rca_runtime_files"],
        "rca_runtime_files_sha256": probe["rca_runtime_files_sha256"],
        "gateway_runtime_file_sha256": probe["gateway_runtime_files"],
        "gateway_runtime_files_sha256": probe["gateway_runtime_files_sha256"],
        "service_runtime_files_sha256": probe["service_runtime_files_sha256"],
        "canonical_interpreter_sha256": probe["interpreter_sha256"],
        "dependency_environment": dependency,
        "retired_executor_paths_absent": list(release.RETIRED_EXECUTOR_PATHS),
    }
    body = {
        "schema_version": release.COMPONENT_BINDING_SCHEMA_VERSION,
        "release_id": RELEASE_ID,
        "observed_at": NOW.isoformat(),
        "host": host,
        "workspace": {"stub": True},
        "worker": {"stub": True},
        "pipeline": pipeline,
        "viewer_proxy": _viewer_proxy_candidate(),
        "admission_security": security,
        "production_mutation": False,
    }
    owned = release.OwnedJson(component_path, b"{}", body)
    monkeypatch.setattr(release, "_observe_canonical_host_binding", lambda **_kwargs: observed_host)
    monkeypatch.setattr(
        release, "_run_canonical_component_probe", lambda **_kwargs: probe
    )
    monkeypatch.setattr(release, "_validate_host_dependency_environment", lambda _value: dependency)
    monkeypatch.setattr(release, "_validate_staged_workspace_binding", lambda _value: workspace)
    monkeypatch.setattr(release, "_validate_vm_worker_observation", lambda _value, **_kwargs: worker)
    monkeypatch.setattr(release, "_validate_vm_hmac_observation", lambda _value, **_kwargs: hmac)
    result = release._validate_component_binding(
        owned, release_id=RELEASE_ID, now=NOW, require_fresh=True
    )
    assert result["host"]["commit"] == release.HOST_FINAL_COMMIT
    assert result["pipeline"]["report_service"]["unit"] == release.VM_REPORT_UNIT
    bad = copy.deepcopy(body)
    bad["host"]["commit"] = "3fe69b39ca3cd118fa60ef5015fd0ee1fe65c698"
    with pytest.raises(release.ProdE2EReleaseError, match="host_binding_invalid"):
        release._validate_component_binding(
            release.OwnedJson(component_path, b"{}", bad),
            release_id=RELEASE_ID,
            now=NOW,
            require_fresh=True,
        )


def test_viewer_proxy_candidate_rejects_origin_dns_tls_config_and_prestate_drift():
    candidate = _viewer_proxy_candidate()
    result = release._validate_viewer_proxy_candidate(
        candidate,
        expected_origin=VIEWER_ORIGIN,
        now=NOW,
        require_fresh=True,
    )
    assert result["public_origin"] == VIEWER_ORIGIN

    mutations = []
    wrong_origin = copy.deepcopy(candidate)
    wrong_origin["public_origin"] = "https://192.168.21.217"
    mutations.append((wrong_origin, "viewer_proxy_public_origin_invalid"))
    wrong_dns = copy.deepcopy(candidate)
    wrong_dns["dns_preread"]["addresses"] = ["192.168.21.218"]
    mutations.append((wrong_dns, "viewer_proxy_dns_invalid"))
    wrong_san = copy.deepcopy(candidate)
    wrong_san["tls_preread"]["matched_san_dns_name"] = "minieye.tech"
    mutations.append((wrong_san, "viewer_proxy_tls_invalid"))
    wrong_config = copy.deepcopy(candidate)
    wrong_config["config"]["sha256"] = "ff" * 32
    mutations.append((wrong_config, "viewer_proxy_candidate_invalid"))
    stale_prestate = copy.deepcopy(candidate)
    stale_prestate["nginx_prestate"]["main_pid"] += 1
    mutations.append((stale_prestate, "viewer_proxy_prestate_invalid"))

    for value, error in mutations:
        with pytest.raises(release.ProdE2EReleaseError, match=error):
            release._validate_viewer_proxy_candidate(
                value,
                expected_origin=VIEWER_ORIGIN,
                now=NOW,
                require_fresh=True,
            )


def test_cross_receipt_is_structural_and_rejects_host_binding_drift(tmp_path, monkeypatch):
    component_path = _write_json(tmp_path / "component.json", {"component": True})
    components = _component_value(component_path)
    report_url = (
        f"{release.VIEWER_PROXY_UPSTREAM_ORIGIN}{release.VM_REPORT_ROUTE_PREFIX}"
        "g1q3-rca-s1-" + "a" * 64 + "/g1q3-rca-artifact-v1-" + "b" * 64
        + "/index.html"
    )
    field_values = {
        release.RESULT_FIELD_KEY: "candidate attribution",
        release.REPORT_FIELD_KEY: report_url,
    }
    updates = [
        {
            "field_key": key,
            "field_value": field_values[key],
            "field_value_sha256": hashlib.sha256(field_values[key].encode()).hexdigest(),
            "field_value_utf8_bytes": len(field_values[key].encode()),
        }
        for key in release.TARGET_FIELD_KEYS
    ]
    body = {
        "schema_version": release.CROSS_CONTRACT_PASS_SCHEMA_VERSION,
        "release_id": RELEASE_ID,
        "bindings": {
            "component_binding_sha256": components["sha256"],
            "host_commit": components["host"]["commit"],
            "host_tree": components["host"]["tree"],
            "workspace_manifest_sha256": components["workspace"]["manifest_sha256"],
            "workspace_closure_sha256": components["workspace"]["closure_sha256"],
            "worker_commit": components["worker"]["commit"],
            "worker_tree": components["worker"]["tree"],
            "pipeline_commit": release.PIPELINE_COMMIT,
            "pipeline_tree": release.PIPELINE_TREE,
                "pipeline_closure_file_sha256": release.PIPELINE_CLOSURE_FILE_SHA256,
                "pipeline_closure_core_sha256": release.PIPELINE_CLOSURE_CORE_SHA256,
                "viewer_origin": components["viewer_proxy"]["public_origin"],
                "viewer_proxy_config_sha256": components["viewer_proxy"][
                    "config"
                ]["sha256"],
        },
        "verdict": "pass",
        "issue_id": release.TARGET_WORK_ITEM_ID,
        "vm_candidate": {"head": release.PIPELINE_COMMIT, "tree": release.PIPELINE_TREE},
        "assertions": {
            "canonical_host_verifier_passed": True,
            "exact_issue_target": True,
            "production_write_not_performed": True,
            "report_field_is_manifest_html_url": True,
            "result_field_nonempty": True,
        },
        "verified_delivery": {
            "send_performed": False,
            "project_key": release.TARGET_PROJECT_KEY,
            "project_simple_name": release.TARGET_PROJECT_SIMPLE_NAME,
            "work_item_id": release.TARGET_WORK_ITEM_ID,
            "issue_url": release.TARGET_ISSUE_URL,
            "report_url": report_url,
            "report_link_kind": "manifest_html",
            "field_updates": updates,
        },
        "vm_bundle": {
            "production_mutation": False,
            "raw_payload_read": False,
            "docker_started_by_payload": False,
            "sha256": "81" * 32,
            "diagnostic_viz_sha256": "82" * 32,
        },
        "supersedes": {"verdict": "gap", "sha256": release.SUPERSEDED_CROSS_CONTRACT_GAP_SHA256},
    }
    path = _write_json(tmp_path / "cross.json", body)
    monkeypatch.setattr(release, "CROSS_CONTRACT_PASS_FILE_SHA256", _sha(path))
    result = release._validate_cross_contract_pass(
        _owned(path), release_id=RELEASE_ID, components=components
    )
    assert result["structural_contract"]["production_values_predetermined"] is False
    body["bindings"]["host_commit"] = "3fe69b39ca3cd118fa60ef5015fd0ee1fe65c698"
    path = _write_json(tmp_path / "bad-cross.json", body)
    monkeypatch.setattr(release, "CROSS_CONTRACT_PASS_FILE_SHA256", _sha(path))
    with pytest.raises(release.ProdE2EReleaseError, match="cross_contract_pass_invalid"):
        release._validate_cross_contract_pass(
            _owned(path), release_id=RELEASE_ID, components=components
        )


def test_candidate_final_requires_live_ext4_read_only_identity(tmp_path, monkeypatch):
    body = _candidate_body(phase="final", observed_at=NOW)
    path = _write_json(tmp_path / "candidate.json", body)
    live = {key: body[key] for key in ("root", "head", "tree", "status_porcelain_sha256", "detached", "git_self_contained", "git_storage", "filesystem", "seal")}
    live["observer_script_sha256"] = hashlib.sha256(
        release._VM_CANDIDATE_PROBE.replace("__ROOT__", repr(FINAL_ROOT)).encode()
    ).hexdigest()
    monkeypatch.setattr(release, "_observe_vm_candidate_live", lambda _root: live)
    result = release._validate_candidate_observation(
        _owned(path),
        release_id=RELEASE_ID,
        expected_phase="final",
        expected_root=FINAL_ROOT,
        now=NOW,
        require_fresh=True,
    )
    assert result["filesystem_type"] == "ext4"
    body["seal"] = {"read_only": False, "write_probe_blocked": False}
    with pytest.raises(release.ProdE2EReleaseError, match="final_candidate_unsealed"):
        release._validate_candidate_observation(
            release.OwnedJson(path, b"{}", body),
            release_id=RELEASE_ID,
            expected_phase="final",
            expected_root=FINAL_ROOT,
            now=NOW,
            require_fresh=False,
        )


def test_cutover_checkpoint_separates_pristine_digest_from_final_live_state(tmp_path, monkeypatch):
    checkpoint = tmp_path / "checkpoint.sqlite3"
    conn = sqlite3.connect(checkpoint)
    conn.execute("CREATE TABLE checkpoint(value TEXT)")
    conn.commit()
    conn.close()
    checkpoint.chmod(0o600)
    expected = "83" * 32
    request = {
        "release_bom": {
            "host_runtime": {"canonical_final": {"commit": release.HOST_FINAL_COMMIT, "tree": release.HOST_FINAL_TREE}},
            "delivery_store_cutover": {"approved_post_migration_logical_sha256": expected},
        }
    }
    monkeypatch.setattr(
        release,
        "_run_canonical_db_projection",
        lambda **_kwargs: {"projection": {"logical_sha256": expected}, "validator_script_sha256": "84" * 32},
    )
    value = {
        "path": str(checkpoint),
        "sha256": _sha(checkpoint),
        "size_bytes": checkpoint.stat().st_size,
        "captured_at": (NOW + timedelta(seconds=2)).isoformat(),
        "logical_sha256": expected,
    }
    result = release._validate_cutover_database_checkpoint(
        value,
        request=request,
        installed_at=NOW + timedelta(seconds=1),
        verified_at=NOW + timedelta(seconds=3),
        preflight_backup_path=str(tmp_path / "source.sqlite3"),
    )
    assert result["logical_sha256"] == expected
    value["logical_sha256"] = "85" * 32
    with pytest.raises(release.ProdE2EReleaseError, match="checkpoint_digest_mismatch"):
        release._validate_cutover_database_checkpoint(
            value,
            request=request,
            installed_at=NOW + timedelta(seconds=1),
            verified_at=NOW + timedelta(seconds=3),
            preflight_backup_path=str(tmp_path / "source.sqlite3"),
        )


def _delta_activation(root: dict, *, entrypoint: str, slot_kind: str, reason: str) -> dict:
    source_sha256 = release._activation_source_identity_sha256(root)
    return {
        "epoch_id": EPOCH_ID,
        "admission_key": release._sha256_value(
            {
                "business_key": root["business_key"],
                "generation": 1,
                "source_identity_sha256": source_sha256,
                "source_kind": "kafka",
                "submission_key": root["submission_key"],
            }
        ),
        "entrypoint": entrypoint,
        "source_kind": "kafka",
        "source_identity_sha256": source_sha256,
        "slot_kind": slot_kind,
        "decision": "admit",
        "reason": reason,
    }


def _delta_context() -> tuple[dict, dict, dict, dict]:
    target = {
        "topic": release.TOPIC,
        "partition": release.PARTITION,
        "offset": release.TARGET_OFFSET,
        "event_uid": release.TARGET_EVENT_UID,
        "raw_sha256": release.TARGET_RAW_SHA256,
        "project_key": release.TARGET_PROJECT_KEY,
        "work_item_type_key": "issue",
        "work_item_id": release.TARGET_WORK_ITEM_ID,
        "business_key": release.TARGET_BUSINESS_KEY,
        "submission_key": release.TARGET_SUBMISSION_KEY,
        "generation": 1,
        "delivery_id": "delivery-target",
        "effect_key": "effect-target",
        "semantic_payload_sha256": "86" * 32,
        "artifact_set_id": "artifact-target-000000000001",
        "target_key": f"feishu_project:{release.TARGET_PROJECT_KEY}:issue:{release.TARGET_WORK_ITEM_ID}",
        "issue_url": release.TARGET_ISSUE_URL,
        "comment_write": {
            "comment_id": "comment-target",
            "attempt_terminal_outcome": "ack",
        },
    }
    canary = {
        "topic": release.TOPIC,
        "partition": release.PARTITION,
        "offset": 700,
        "event_uid": f"{release.TOPIC}:0:700",
        "raw_sha256": "87" * 32,
        "project_key": release.TARGET_PROJECT_KEY,
        "work_item_type_key": "issue",
        "work_item_id": "7059999999",
        "business_key": "canary-business",
        "submission_key": "canary-submission",
        "generation": 1,
        "delivery_source": "ordinary_kafka_ingest",
        "recovery_write_count": 0,
        "operator_recovery_provenance": [],
        "delivery_id": "delivery-canary",
        "effect_key": "effect-canary",
        "semantic_payload_sha256": "88" * 32,
        "artifact_set_id": "artifact-canary-000000000002",
        "target_key": f"feishu_project:{release.TARGET_PROJECT_KEY}:issue:7059999999",
        "issue_url": (
            f"https://project.feishu.cn/{release.TARGET_PROJECT_SIMPLE_NAME}"
            "/issue/detail/7059999999"
        ),
        "comment_write": {
            "comment_id": "comment-canary",
            "attempt_terminal_outcome": "ack",
        },
    }
    target["activation"] = _delta_activation(
        target,
        entrypoint="kafka_ingest",
        slot_kind="kafka_success",
        reason="activation_bounded_slot_consumed",
    )
    canary["activation"] = _delta_activation(
        canary,
        entrypoint="kafka_ingest",
        slot_kind="",
        reason="activation_steady_active",
    )
    runtime_files = {
        entrypoint: "89" * 32
        for entrypoint in release.HOST_SERVICE_ENTRYPOINTS.values()
        if entrypoint
    }
    host = {
        "runtime_files_sha256": "8a" * 32,
        "runtime_file_sha256": runtime_files,
        "service_runtime_files_sha256": {
            label: "8a" * 32 for label in release.HOST_SERVICE_LABELS
        },
    }
    processes = {
        label: {"new_pid": 1000 + index}
        for index, label in enumerate(release.HOST_SERVICE_LABELS)
    }
    return target, canary, host, processes


def _create_delta_database(path: Path, *, target: dict) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE kafka_inbox(
          event_uid TEXT PRIMARY KEY, topic TEXT, partition_id INTEGER,
          offset_id INTEGER, raw_sha256 TEXT, decision TEXT,
          business_key TEXT, submission_key TEXT, generation INTEGER
        );
        CREATE TABLE kafka_partition_progress(
          topic TEXT, partition_id INTEGER, first_offset INTEGER,
          durable_next_offset INTEGER, last_event_uid TEXT, updated_at TEXT,
          PRIMARY KEY(topic,partition_id)
        );
        CREATE TABLE business_triggers(
          business_key TEXT, generation INTEGER, submission_key TEXT UNIQUE,
          project_key TEXT, work_item_type_key TEXT, work_item_id TEXT,
          state TEXT, source_event_id TEXT, source_topic TEXT,
          source_partition INTEGER, source_offset INTEGER,
          activation_epoch_id TEXT, activation_ledger_id INTEGER,
          PRIMARY KEY(business_key,generation)
        );
        CREATE TABLE rca_trigger_sources(
          source_id TEXT PRIMARY KEY, source_kind TEXT,
          source_dedupe_key TEXT, payload_sha256 TEXT,
          kafka_event_uid TEXT UNIQUE, mode TEXT
        );
        CREATE TABLE rca_trigger_bindings(
          source_id TEXT PRIMARY KEY, business_key TEXT,
          generation INTEGER, role TEXT
        );
        CREATE TABLE rca_outbox(
          outbox_id INTEGER PRIMARY KEY AUTOINCREMENT, business_key TEXT,
          submission_key TEXT UNIQUE, generation INTEGER, status TEXT,
          source_event_id TEXT, source_topic TEXT, source_partition INTEGER,
          source_offset INTEGER, origin_source_id TEXT,
          activation_epoch_id TEXT, activation_ledger_id INTEGER
        );
        CREATE TABLE rca_execution_watch(
          submission_key TEXT PRIMARY KEY, business_key TEXT, generation INTEGER,
          project_key TEXT, work_item_type_key TEXT, work_item_id TEXT,
          task_id TEXT, state TEXT, delivery_id TEXT
        );
        CREATE TABLE rca_delivery_jobs(
          delivery_id TEXT PRIMARY KEY, submission_key TEXT UNIQUE,
          business_key TEXT, generation INTEGER, artifact_set_id TEXT,
          project_key TEXT, work_item_type_key TEXT, work_item_id TEXT,
          target_key TEXT, issue_url TEXT, outcome TEXT, status TEXT
        );
        CREATE TABLE rca_delivery_subscriptions(
          subscription_key TEXT PRIMARY KEY, business_key TEXT,
          generation INTEGER, source_id TEXT, effect_kind TEXT,
          target_key TEXT, target_json TEXT, required INTEGER,
          status TEXT, delivery_id TEXT, effect_key TEXT
        );
        CREATE TABLE rca_trigger_delivery_bindings(
          source_id TEXT, subscription_key TEXT,
          PRIMARY KEY(source_id,subscription_key)
        );
        CREATE TABLE rca_delivery_effects(
          effect_key TEXT PRIMARY KEY, delivery_id TEXT, effect_kind TEXT,
          required INTEGER, target_key TEXT, payload_sha256 TEXT,
          outcome TEXT, write_phase TEXT, status TEXT,
          remote_receipt_json TEXT
        );
        CREATE TABLE rca_delivery_attempts(
          attempt_id INTEGER PRIMARY KEY AUTOINCREMENT, effect_key TEXT,
          attempt_no INTEGER, event_seq INTEGER, fence INTEGER,
          request_id TEXT, outcome TEXT, remote_id TEXT,
          error_code TEXT, detail TEXT, started_at TEXT, finished_at TEXT
        );
        CREATE TABLE rca_shadow_promotion_audit(
          audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
          event_uid TEXT, outbox_id INTEGER, submission_key TEXT,
          outcome TEXT, from_status TEXT, to_status TEXT
        );
        CREATE TABLE rca_host_runtime_transitions(
          transition_id INTEGER PRIMARY KEY AUTOINCREMENT,
          submission_key TEXT, business_key TEXT, generation INTEGER,
          service_label TEXT, transition_kind TEXT, entity_key TEXT,
          runtime_identity_json TEXT, runtime_identity_sha256 TEXT,
          transitioned_at TEXT
        );
        CREATE TABLE rca_activation_admission_ledger(
          ledger_id INTEGER PRIMARY KEY AUTOINCREMENT, epoch_id TEXT,
          admission_key TEXT UNIQUE, entrypoint TEXT, source_kind TEXT,
          source_identity_sha256 TEXT, slot_kind TEXT, decision TEXT,
          reason TEXT, business_key TEXT, submission_key TEXT,
          generation INTEGER, admitted_at TEXT, bound_at TEXT
        );
        CREATE TABLE rca_activation_budget_slots(
          epoch_id TEXT, slot_kind TEXT, authorized_source_kind TEXT,
          authorized_identity_sha256 TEXT, consumed_ledger_id INTEGER,
          consumed_at TEXT, PRIMARY KEY(epoch_id,slot_kind)
        );
        CREATE TABLE control_meta(key TEXT PRIMARY KEY,value TEXT);
        INSERT INTO kafka_partition_progress VALUES(
          'feishu-project-workflow-event',0,514,677,'old:0:676','2026-07-21T09:00:00+00:00'
        );
        """
    )
    conn.execute(
        "INSERT INTO rca_activation_budget_slots VALUES(?,?,?,?,NULL,NULL)",
        (
            EPOCH_ID,
            "kafka_success",
            "kafka",
            target["activation"]["source_identity_sha256"],
        ),
    )
    conn.commit()
    conn.close()
    path.chmod(0o600)


def _add_allowed_delta(
    path: Path,
    *,
    target: dict,
    canary: dict,
    host: dict,
    processes: dict,
) -> None:
    conn = sqlite3.connect(path)
    contexts = []
    for root in (target, canary):
        activation = root["activation"]
        source_id = release._stable_lineage_key(
            "g1q3-rca-source-v1",
            {"source_kind": "kafka_workflow_event", "dedupe": root["event_uid"]},
        )
        subscription_key = release._stable_lineage_key(
            "g1q3-rca-sub-v1",
            {
                "business_key": root["business_key"],
                "generation": 1,
                "effect_kind": "feishu_issue_comment",
                "target_key": root["target_key"],
            },
        )
        ledger_id = conn.execute(
            "INSERT INTO rca_activation_admission_ledger(epoch_id,admission_key,entrypoint,source_kind,source_identity_sha256,slot_kind,decision,reason,business_key,submission_key,generation,admitted_at,bound_at) VALUES(?,?,?,?,?,?,?,?,?,?,1,?,?)",
            (
                activation["epoch_id"], activation["admission_key"],
                activation["entrypoint"], "kafka", activation["source_identity_sha256"],
                activation["slot_kind"] or None, "admit", activation["reason"],
                root["business_key"], root["submission_key"], NOW.isoformat(), NOW.isoformat(),
            ),
        ).lastrowid
        conn.execute(
            "INSERT INTO kafka_inbox VALUES(?,?,?,?,?,?,?,?,?)",
            (root["event_uid"], root["topic"], root["partition"], root["offset"], root["raw_sha256"], "accepted", root["business_key"], root["submission_key"], 1),
        )
        conn.execute(
            "INSERT INTO business_triggers VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (root["business_key"], 1, root["submission_key"], root["project_key"], root["work_item_type_key"], root["work_item_id"], "submitted", root["event_uid"], root["topic"], root["partition"], root["offset"], EPOCH_ID, ledger_id),
        )
        conn.execute(
            "INSERT INTO rca_trigger_sources VALUES(?,?,?,?,?,?)",
            (source_id, "kafka_workflow_event", root["event_uid"], root["raw_sha256"], root["event_uid"], "issue_created"),
        )
        conn.execute("INSERT INTO rca_trigger_bindings VALUES(?,?,1,'observer')", (source_id, root["business_key"]))
        outbox_id = conn.execute(
            "INSERT INTO rca_outbox(business_key,submission_key,generation,status,source_event_id,source_topic,source_partition,source_offset,origin_source_id,activation_epoch_id,activation_ledger_id) VALUES(?,?,1,'completed',?,?,?,?,?,?,?)",
            (root["business_key"], root["submission_key"], root["event_uid"], root["topic"], root["partition"], root["offset"], source_id, EPOCH_ID, ledger_id),
        ).lastrowid
        conn.execute(
            "INSERT INTO rca_execution_watch VALUES(?,?,1,?,?,?,?,?,?)",
            (root["submission_key"], root["business_key"], root["project_key"], root["work_item_type_key"], root["work_item_id"], root["submission_key"], "delivery_created", root["delivery_id"]),
        )
        conn.execute(
            "INSERT INTO rca_delivery_jobs VALUES(?,?,?,1,?,?,?,?,?,?,?,?)",
            (root["delivery_id"], root["submission_key"], root["business_key"], root["artifact_set_id"], root["project_key"], root["work_item_type_key"], root["work_item_id"], root["target_key"], root["issue_url"], "success", "delivered"),
        )
        conn.execute(
            "INSERT INTO rca_delivery_subscriptions VALUES(?,?,1,NULL,'feishu_issue_comment',?,'{}',1,'materialized',?,?)",
            (subscription_key, root["business_key"], root["target_key"], root["delivery_id"], root["effect_key"]),
        )
        conn.execute("INSERT INTO rca_trigger_delivery_bindings VALUES(?,?)", (source_id, subscription_key))
        marker = f"[RCA_DELIVERY:{root['effect_key']}:{root['artifact_set_id'][-12:]}]"
        receipt = {
            "remote_id": root["comment_write"]["comment_id"],
            "marker": marker,
            "source": "read_after_write",
            "recovery_write_count": 0,
            "confirmed_field_keys": list(release.TARGET_FIELD_KEYS),
        }
        conn.execute(
            "INSERT INTO rca_delivery_effects VALUES(?,?,'feishu_issue_comment',1,?,?,'success','settled','succeeded',?)",
            (root["effect_key"], root["delivery_id"], root["target_key"], root["semantic_payload_sha256"], json.dumps(receipt, sort_keys=True, separators=(",", ":"))),
        )
        request_id = f"request-{root['submission_key']}"
        conn.execute(
            "INSERT INTO rca_delivery_attempts(effect_key,attempt_no,event_seq,fence,request_id,outcome,remote_id,error_code,detail,started_at,finished_at) VALUES(?,1,1,1,?,'started','','','',?,NULL)",
            (root["effect_key"], request_id, NOW.isoformat()),
        )
        conn.execute(
            "INSERT INTO rca_delivery_attempts(effect_key,attempt_no,event_seq,fence,request_id,outcome,remote_id,error_code,detail,started_at,finished_at) VALUES(?,1,2,1,?,'ack',?,'','',?,?)",
            (root["effect_key"], request_id, root["comment_write"]["comment_id"], NOW.isoformat(), NOW.isoformat()),
        )
        transition_specs = (
            ("local.pnc.rca-kafka-consumer", "kafka_ingested", root["event_uid"]),
            ("local.pnc.rca-outbox-dispatcher", "outbox_completed", str(outbox_id)),
            ("local.pnc.rca-delivery-collector", "delivery_created", root["delivery_id"]),
            ("local.pnc.rca-delivery-dispatcher", "effect_succeeded", root["effect_key"]),
        )
        for label, kind, entity in transition_specs:
            entrypoint = release.HOST_SERVICE_ENTRYPOINTS[label]
            identity = {
                "service_label": label,
                "pid": processes[label]["new_pid"],
                "process_create_time": NOW.timestamp() - 10,
                "boot_time": NOW.timestamp() - 1000,
                "executable": f"{release.HOST_LIVE_ROOT}/.venv/bin/python",
                "script": f"{release.HOST_LIVE_ROOT}/{entrypoint}",
                "cwd": release.HOST_LIVE_ROOT,
                "script_sha256": host["runtime_file_sha256"][entrypoint],
                "runtime_files_sha256": host["service_runtime_files_sha256"][label],
                "public_config_sha256": "8b" * 32,
                "loaded_runtime_sha256": "8c" * 32,
            }
            conn.execute(
                "INSERT INTO rca_host_runtime_transitions(submission_key,business_key,generation,service_label,transition_kind,entity_key,runtime_identity_json,runtime_identity_sha256,transitioned_at) VALUES(?,?,1,?,?,?,?,?,?)",
                (root["submission_key"], root["business_key"], label, kind, entity, json.dumps(identity, sort_keys=True, separators=(",", ":")), release._sha256_value(identity), NOW.isoformat()),
            )
        contexts.append((root, ledger_id, outbox_id))
    target_ledger = contexts[0][1]
    conn.execute(
        "UPDATE rca_activation_budget_slots SET consumed_ledger_id=?,consumed_at=? WHERE epoch_id=? AND slot_kind='kafka_success'",
        (target_ledger, NOW.isoformat(), EPOCH_ID),
    )
    conn.execute(
        "UPDATE kafka_partition_progress SET durable_next_offset=?,last_event_uid=?,updated_at=? WHERE topic=? AND partition_id=?",
        (canary["offset"] + 1, canary["event_uid"], NOW.isoformat(), release.TOPIC, release.PARTITION),
    )
    conn.commit()
    conn.close()


def _prepare_delta(tmp_path: Path) -> tuple[Path, Path, dict, dict, dict, dict]:
    baseline = tmp_path / "cutover.sqlite3"
    live = tmp_path / "live.sqlite3"
    target, canary, host, processes = _delta_context()
    _create_delta_database(baseline, target=target)
    shutil.copy2(baseline, live)
    _add_allowed_delta(
        live,
        target=target,
        canary=canary,
        host=host,
        processes=processes,
    )
    return baseline, live, target, canary, host, processes


def _validate_delta(
    baseline: Path,
    target: dict,
    canary: dict,
    host: dict,
    processes: dict,
) -> dict:
    return release._validate_live_database_allowed_delta(
        cutover_snapshot_path=baseline,
        target=target,
        canary=canary,
        expected_host=host,
        expected_host_processes=processes,
    )


def test_live_database_delta_allows_only_o650_and_one_natural_canary(tmp_path, monkeypatch):
    baseline, live, target, canary, host, processes = _prepare_delta(tmp_path)
    monkeypatch.setattr(release, "DELIVERY_DB_PATH", str(live))
    result = _validate_delta(baseline, target, canary, host, processes)
    assert result["allowed_event_uids"] == sorted([release.TARGET_EVENT_UID, canary["event_uid"]])
    assert result["deleted_row_count"] == 0
    conn = sqlite3.connect(live)
    conn.execute("INSERT INTO control_meta VALUES('unapproved','delta')")
    conn.commit()
    conn.close()
    with pytest.raises(release.ProdE2EReleaseError, match="unrelated_delta"):
        _validate_delta(baseline, target, canary, host, processes)


@pytest.mark.parametrize(
    ("mutation", "parameters"),
    [
        (
            "INSERT INTO rca_delivery_effects VALUES('effect-extra','delivery-target','feishu_card_patch',1,'extra-target',?,'success','settled','succeeded','{}')",
            ("ef" * 32,),
        ),
        (
            "INSERT INTO rca_delivery_attempts(effect_key,attempt_no,event_seq,fence,request_id,outcome,remote_id,error_code,detail,started_at,finished_at) VALUES('effect-target',2,1,2,'extra-request','started','','','',?,NULL)",
            (NOW.isoformat(),),
        ),
        (
            "INSERT INTO rca_host_runtime_transitions(submission_key,business_key,generation,service_label,transition_kind,entity_key,runtime_identity_json,runtime_identity_sha256,transitioned_at) SELECT submission_key,business_key,generation,service_label,transition_kind,entity_key,runtime_identity_json,runtime_identity_sha256,transitioned_at FROM rca_host_runtime_transitions LIMIT 1",
            (),
        ),
        (
            "INSERT INTO rca_delivery_subscriptions VALUES('subscription-extra',?,1,NULL,'feishu_issue_comment','extra-target','{}',1,'materialized','delivery-target','effect-target')",
            (release.TARGET_BUSINESS_KEY,),
        ),
        (
            "INSERT INTO rca_trigger_delivery_bindings VALUES('source-extra','subscription-extra')",
            (),
        ),
        (
            "INSERT INTO rca_activation_admission_ledger(epoch_id,admission_key,entrypoint,source_kind,source_identity_sha256,slot_kind,decision,reason,business_key,submission_key,generation,admitted_at,bound_at) VALUES(?,?,'kafka_ingest','kafka',?,NULL,'admit','activation_steady_active','extra-business','extra-submission',1,?,?)",
            (EPOCH_ID, "ee" * 32, "ed" * 32, NOW.isoformat(), NOW.isoformat()),
        ),
        (
            "INSERT INTO rca_activation_budget_slots VALUES(?,'manual_success','kafka',?,1,?)",
            (EPOCH_ID, "ec" * 32, NOW.isoformat()),
        ),
        (
            "UPDATE business_triggers SET work_item_id=? WHERE submission_key='canary-submission'",
            (release.TARGET_WORK_ITEM_ID,),
        ),
    ],
    ids=(
        "extra-effect-card",
        "extra-attempt",
        "duplicate-transition",
        "extra-subscription",
        "extra-trigger-binding",
        "extra-ledger",
        "extra-slot-consumption",
        "cross-swapped-work-item",
    ),
)
def test_live_database_delta_rejects_extra_or_cross_swapped_lineage(
    tmp_path, monkeypatch, mutation, parameters
):
    baseline, live, target, canary, host, processes = _prepare_delta(tmp_path)
    conn = sqlite3.connect(live)
    conn.execute(mutation, parameters)
    conn.commit()
    conn.close()
    monkeypatch.setattr(release, "DELIVERY_DB_PATH", str(live))
    with pytest.raises(release.ProdE2EReleaseError, match="live_database_"):
        _validate_delta(baseline, target, canary, host, processes)


def _stopped_host_services() -> dict:
    return {
        label: {
            "job_present": True,
            "state": "stopped",
            "pid": None,
            "pid_absent": True,
            "config_path": f"/plist/{label}",
            "config_sha256": "90" * 32,
        }
        for label in release.HOST_SERVICE_LABELS
    }


def _vm_service(unit: str, *, active: bool) -> dict:
    entrypoint = release.VM_WORKER_ENTRYPOINT if unit == release.VM_DAEMON_UNIT else f"{release.PIPELINE_SOURCE_ROOT}/{release.VM_REPORT_ENTRYPOINT_RELATIVE}"
    exec_start = [release.VM_INTERPRETER_PATH, "-I", "-B", entrypoint]
    if unit == release.VM_REPORT_UNIT:
        exec_start.extend(
            [
                "--root",
                release.VM_REPORT_ROOT,
                "--bind",
                "0.0.0.0",
                "--port",
                str(release.VM_REPORT_PORT),
                "--viewer-origin",
                VIEWER_ORIGIN,
            ]
        )
    environment = ["PYTHONDONTWRITEBYTECODE=1", "PYTHONNOUSERSITE=1"]
    effective_environment = list(environment)
    environment_files = []
    viewer_origin = ""
    if unit == release.VM_REPORT_UNIT:
        effective_environment = [
            f"{release.VM_REPORT_ENV_VARIABLE}={VIEWER_ORIGIN}",
            *environment,
        ]
        environment_files = [release.VM_REPORT_ENV_PATH]
        viewer_origin = VIEWER_ORIGIN
    working_directory = (
        release.VM_WORKER_ROOT
        if unit == release.VM_DAEMON_UNIT
        else "/"
    )
    return {
        "unit": unit,
        "active_state": "active" if active else "inactive",
        "sub_state": "running" if active else "dead",
        "main_pid": 900 if active else 0,
        "unit_config_sha256": "91" * 32 if unit == release.VM_DAEMON_UNIT else release.VM_REPORT_UNIT_SHA256,
        "entrypoint": entrypoint,
        "entrypoint_sha256": "93" * 32 if unit == release.VM_DAEMON_UNIT else release.VM_REPORT_ENTRYPOINT_SHA256,
        "exec_start": list(exec_start),
        "environment": list(environment),
        "fragment_path": (
            release.VM_DAEMON_LIVE_UNIT_PATH
            if unit == release.VM_DAEMON_UNIT
            else release.VM_REPORT_LIVE_UNIT_PATH
        ),
        "drop_in_paths": [],
        "effective_exec_start": list(exec_start),
        "effective_environment": list(effective_environment),
        "environment_files": environment_files,
        "working_directory": working_directory,
        "interpreter_path": release.VM_INTERPRETER_PATH,
        "interpreter_sha256": "94" * 32,
        "process_executable": release.VM_INTERPRETER_PATH if active else "",
        "process_arguments": list(exec_start) if active else [],
        "process_working_directory": working_directory if active else "",
        "process_environment": list(effective_environment) if active else [],
        "viewer_origin": viewer_origin,
    }


def test_preflight_requires_host_runtime_baseline_approval_and_both_vm_services(tmp_path, monkeypatch):
    backup = tmp_path / "backup.sqlite3"
    conn = sqlite3.connect(backup)
    conn.execute("CREATE TABLE backup(value TEXT)")
    conn.commit()
    conn.close()
    backup.chmod(0o600)
    observed = NOW + timedelta(minutes=1)
    os.utime(backup, (observed.timestamp(), observed.timestamp()))
    host_services = _stopped_host_services()
    host_runtime = {"runtime_files_sha256": "95" * 32}
    vm_services = {unit: _vm_service(unit, active=False) for unit in release.VM_SERVICE_UNITS}
    live_vm = {
        "services": vm_services,
        "observer_script_sha256": "96" * 32,
        "machine_identity_sha256": "97" * 32,
    }
    receipt_vm = {
        unit: {
            **service,
            "observer_script_sha256": live_vm["observer_script_sha256"],
            "machine_identity_sha256": live_vm["machine_identity_sha256"],
        }
        for unit, service in vm_services.items()
    }
    executor = {"closure_complete": True}
    target_kafka = {
        "event_uid": release.TARGET_EVENT_UID,
        "offset": release.TARGET_OFFSET,
        "raw_sha256": release.TARGET_RAW_SHA256,
        "business_key": release.TARGET_BUSINESS_KEY,
        "submission_key": release.TARGET_SUBMISSION_KEY,
        "work_item_id": release.TARGET_WORK_ITEM_ID,
        "retained_start": 514,
        "retained_end": 701,
        "observed_at": observed.isoformat(),
        "group_id": None,
        "commit_called": False,
    }
    target_input = {
        "schema_version": "pnc_rca_fresh_target_input_revalidation_v1",
        "project_key": release.TARGET_PROJECT_KEY,
        "work_item_id": release.TARGET_WORK_ITEM_ID,
        "source": "official_meegle_api",
        "status": "fields_extracted",
        "context_sha256": "2d" * 32,
        "context_utf8_bytes": 954,
        "fields": {
            key: {"sha256": release.EMPTY_SHA256, "utf8_bytes": 0}
            for key in release.TARGET_FIELD_KEYS
        },
        "observed_at": observed.isoformat(),
    }
    anchors = {
        "anchor": "before",
        "live_env_sha256": _host_environment_transition()["pre_sha256"],
    }
    source_digest = "98" * 32
    body = {
        "schema_version": release.EXECUTION_PREFLIGHT_SCHEMA_VERSION,
        "release_id": RELEASE_ID,
        "request_sha256": "99" * 32,
        "release_bom_sha256": "a0" * 32,
        "approval_sha256": "a1" * 32,
        "baseline_approval_sha256": "a2" * 32,
        "authorization_sha256": "a3" * 32,
        "observed_at": observed.isoformat(),
        "writers_stopped_at": observed.isoformat(),
        "host_services": host_services,
        "host_live_runtime": host_runtime,
        "vm_services": receipt_vm,
        "executor_closure": executor,
        "target_kafka_preread": target_kafka,
        "target_input_revalidation": target_input,
        "fresh_live_backup": {
            "path": str(backup),
            "sha256": _sha(backup),
            "size_bytes": backup.stat().st_size,
            "captured_at": observed.isoformat(),
        },
        "fresh_live_pre_logical_sha256": source_digest,
        "activation_anchors_before": anchors,
        "rollback_contract": {
            "backup_path": str(backup),
            "backup_sha256": _sha(backup),
            "live_env_path": release.CANONICAL_HOST_ENV,
            "live_env_pre_sha256": _host_environment_transition()["pre_sha256"],
            "live_env_post_sha256": _host_environment_transition()["post_sha256"],
            "vm_report_env_path": release.VM_REPORT_ENV_PATH,
            "vm_report_env_pre_exists": False,
            "vm_report_env_pre_sha256": release.EMPTY_SHA256,
            "vm_report_env_post_sha256": _vm_report_environment_transition()["post_sha256"],
            "restore_before_environment_or_binding_on_failure": True,
            "environment_written": False,
            "active_binding_written": False,
        },
        "production_effects": {
            "services_stopped": True,
            "live_database_mutated": False,
            "environment_written": False,
            "active_binding_written": False,
            "feishu_written": False,
            "kafka_offsets_mutated": False,
        },
    }
    path = _write_json(tmp_path / "preflight.json", body)
    monkeypatch.setattr(release, "_observe_host_writer_stop_live", lambda: host_services)
    monkeypatch.setattr(release, "_observe_host_live_runtime", lambda **_kwargs: host_runtime)
    monkeypatch.setattr(
        release, "_observe_vm_components_live", lambda **_kwargs: live_vm
    )
    monkeypatch.setattr(release, "_observe_executor_closure_live", lambda: executor)
    monkeypatch.setattr(
        release, "_observe_target_kafka_record_live", lambda **_kwargs: target_kafka
    )
    monkeypatch.setattr(
        release, "_observe_target_input_gate_live", lambda **_kwargs: target_input
    )
    monkeypatch.setattr(release, "_observe_activation_anchors_live", lambda value: value)
    monkeypatch.setattr(
        release,
        "_run_canonical_db_projection",
        lambda **_kwargs: {"projection": {"logical_sha256": source_digest}, "validator_script_sha256": "a4" * 32},
    )
    worker = {
        "daemon_unit_config_sha256": vm_services[release.VM_DAEMON_UNIT]["unit_config_sha256"],
        "report_unit_config_sha256": vm_services[release.VM_REPORT_UNIT]["unit_config_sha256"],
        "report_environment_transition": _vm_report_environment_transition(),
    }
    kwargs = {
        "release_id": RELEASE_ID,
        "now": observed,
        "db_cutover": {"approved_source_logical_sha256": source_digest},
        "host": {
            "commit": release.HOST_FINAL_COMMIT,
            "tree": release.HOST_FINAL_TREE,
            "viewer_origin": VIEWER_ORIGIN,
            "host_environment_transition": _host_environment_transition(),
            "service_config_sha256": {
                label: "90" * 32 for label in release.HOST_SERVICE_LABELS
            },
        },
        "worker": worker,
        "feishu_completion": {
            "input_preread": {
                "context_sha256": target_input["context_sha256"],
                "context_utf8_bytes": target_input["context_utf8_bytes"],
            },
            "field_preread": {
                "empty_field_keys": list(release.TARGET_FIELD_KEYS)
            },
        },
        "request_sha256": body["request_sha256"],
        "release_bom_sha256": body["release_bom_sha256"],
        "approval_sha256": body["approval_sha256"],
        "baseline_approval_sha256": body["baseline_approval_sha256"],
        "authorization_sha256": body["authorization_sha256"],
    }
    result = release._validate_execution_preflight(_owned(path), **kwargs)
    assert set(result["vm_services"]) == set(release.VM_SERVICE_UNITS)
    bad = copy.deepcopy(body)
    bad["vm_services"].pop(release.VM_REPORT_UNIT)
    with pytest.raises(release.ProdE2EReleaseError, match="vm_writer"):
        release._validate_execution_preflight(
            release.OwnedJson(path, b"{}", bad), **kwargs
        )
    with pytest.raises(release.ProdE2EReleaseError, match="execution_preflight_invalid"):
        release._validate_execution_preflight(
            _owned(path), **{**kwargs, "baseline_approval_sha256": "ff" * 32}
        )


def test_vm_live_observer_rejects_dropins_and_process_argv_drift(monkeypatch):
    value = {
        "worker": {},
        "hmac": {},
        "machine_identity_sha256": "a5" * 32,
        "report_environment": {
            "path": release.VM_REPORT_ENV_PATH,
            "exists": True,
            "sha256": _report_service()["environment_file_sha256"],
            "bytes": _report_service()["environment_file_bytes"],
            "owner_uid": 1000,
            "mode": "0600",
            "variable": release.VM_REPORT_ENV_VARIABLE,
            "viewer_origin": VIEWER_ORIGIN,
        },
        "report_environment_transition": {
            **_vm_report_environment_transition(),
            "pre_exists": True,
            "pre_sha256": _report_service()["environment_file_sha256"],
            "pre_bytes": _report_service()["environment_file_bytes"],
            "pre_owner_uid": 1000,
            "pre_mode": "0600",
            "pre_parent_exists": True,
            "pre_parent_owner_uid": 1000,
            "pre_parent_mode": "0700",
            "write_required": False,
        },
        "services": {
            unit: _vm_service(unit, active=True)
            for unit in release.VM_SERVICE_UNITS
        },
        "report_policy": {
            "root": release.VM_REPORT_ROOT,
            "route_prefix": release.VM_REPORT_ROUTE_PREFIX,
            "port": release.VM_REPORT_PORT,
            "directory_listing": False,
            "path_traversal": False,
            "symlink_escape": False,
            "read_only": True,
            "broad_http_server_processes": [],
            "viewer_origin": VIEWER_ORIGIN,
            "delivery_manifest_schema": "delivery_manifest_v2",
            "viz_manifest_schema": "g1q3_rca_viz_publication_v1",
            "max_concurrent_requests": 4,
            "request_queue_size": 16,
        },
        "secret_material_persisted": False,
    }

    def completed():
        return release.subprocess.CompletedProcess(
            [], 0, json.dumps(value, sort_keys=True, separators=(",", ":")), ""
        )

    monkeypatch.setattr(
        release.subprocess, "run", lambda *_args, **_kwargs: completed()
    )
    observed = release._observe_vm_components_live(
        expected_viewer_origin=VIEWER_ORIGIN
    )
    assert set(observed["services"]) == set(release.VM_SERVICE_UNITS)

    value["services"][release.VM_REPORT_UNIT]["drop_in_paths"] = [
        "/home/mini/.config/systemd/user/g1q3-rca-report-http.service.d/override.conf"
    ]
    with pytest.raises(release.ProdE2EReleaseError, match="live_observation_invalid"):
        release._observe_vm_components_live(expected_viewer_origin=VIEWER_ORIGIN)

    value["services"][release.VM_REPORT_UNIT] = _vm_service(
        release.VM_REPORT_UNIT, active=True
    )
    value["services"][release.VM_REPORT_UNIT]["process_arguments"][-1] = "18082"
    with pytest.raises(release.ProdE2EReleaseError, match="live_observation_invalid"):
        release._observe_vm_components_live(expected_viewer_origin=VIEWER_ORIGIN)


def test_source_tree_rejects_matching_header_pyc_and_shadow_module(tmp_path):
    root = tmp_path / "root"
    (root / "gateway").mkdir(parents=True)
    source = root / "gateway/module.py"
    source.write_text("VALUE = 1\n")
    release._assert_source_tree_cache_and_shadow_free(
        root, expected_files=("gateway/module.py",), artifact="test"
    )
    cache = root / "gateway/__pycache__"
    cache.mkdir()
    (cache / "module.cpython-311.pyc").write_bytes(b"\xa7\r\r\n" + b"x" * 32)
    with pytest.raises(release.ProdE2EReleaseError, match="bytecode_present"):
        release._assert_source_tree_cache_and_shadow_free(
            root, expected_files=("gateway/module.py",), artifact="test"
        )
    shutil.rmtree(cache)
    (root / "gateway.py").write_text("VALUE = 'shadow'\n")
    with pytest.raises(release.ProdE2EReleaseError, match="shadow_module_present"):
        release._assert_source_tree_cache_and_shadow_free(
            root, expected_files=("gateway/module.py",), artifact="test"
        )
    (root / "gateway.py").unlink()
    package_shadow = root / "gateway/module"
    package_shadow.mkdir()
    (package_shadow / "__init__.py").write_text("VALUE = 'package-shadow'\n")
    with pytest.raises(release.ProdE2EReleaseError, match="shadow_module_present"):
        release._assert_source_tree_cache_and_shadow_free(
            root, expected_files=("gateway/module.py",), artifact="test"
        )


def test_host_live_runtime_rejects_stale_file_wrong_argv_retired_and_dependency_drift(tmp_path, monkeypatch):
    root = tmp_path / "live"
    home = tmp_path / "home"
    (root / "scripts").mkdir(parents=True)
    (root / ".venv/bin").mkdir(parents=True)
    (home / "Library/LaunchAgents").mkdir(parents=True)
    source = root / "scripts/service.py"
    source.write_text("print('ok')\n")
    expected_files = {"scripts/service.py": hashlib.sha256(source.read_bytes()).hexdigest()}
    dependency = {"interpreter_path": str(root / ".venv/bin/python"), "interpreter_sha256": "b1" * 32}
    expected = {
        "runtime_file_sha256": expected_files,
        "runtime_files_sha256": release._sha256_value(expected_files),
        "rca_runtime_file_sha256": expected_files,
        "rca_runtime_files_sha256": release._sha256_value(expected_files),
        "gateway_runtime_file_sha256": expected_files,
        "gateway_runtime_files_sha256": release._sha256_value(expected_files),
            "service_runtime_files_sha256": {},
            "viewer_origin": VIEWER_ORIGIN,
            "host_environment_transition": _host_environment_transition(),
            "dependency_environment": {"observation": dependency},
        "service_config_sha256": {},
    }
    label = "local.test.service"
    plist_path = home / "Library/LaunchAgents" / f"{label}.plist"

    def write_plist(arguments):
        raw = plistlib.dumps(
            {
                "ProgramArguments": arguments,
                "WorkingDirectory": str(root),
                "EnvironmentVariables": {"PYTHONDONTWRITEBYTECODE": "1"},
            }
        )
        plist_path.write_bytes(raw)
        expected["service_config_sha256"][label] = hashlib.sha256(raw).hexdigest()

    write_plist([str(root / ".venv/bin/python"), str(source)])
    monkeypatch.setattr(release, "HOST_LIVE_ROOT", str(root))
    monkeypatch.setattr(release, "HOST_SERVICE_LABELS", (label,))
    monkeypatch.setattr(release, "HOST_SERVICE_ENTRYPOINTS", {label: "scripts/service.py"})
    expected["service_runtime_files_sha256"][label] = expected[
        "runtime_files_sha256"
    ]
    monkeypatch.setattr(release, "RETIRED_EXECUTOR_PATHS", ("scripts/retired.py",))
    monkeypatch.setattr(release.Path, "home", classmethod(lambda _cls: home))
    monkeypatch.setattr(
        release,
        "_run_canonical_component_probe",
        lambda **_kwargs: {
            "runtime_files": expected_files,
            "runtime_files_sha256": expected["runtime_files_sha256"],
            "rca_runtime_files": expected_files,
            "rca_runtime_files_sha256": expected["rca_runtime_files_sha256"],
            "gateway_runtime_files": expected_files,
            "gateway_runtime_files_sha256": expected[
                "gateway_runtime_files_sha256"
            ],
                "service_runtime_files_sha256": expected[
                    "service_runtime_files_sha256"
                ],
                "host_env_current_sha256": _host_environment_transition()["pre_sha256"],
                "host_env_current_bytes": _host_environment_transition()["pre_bytes"],
                "host_env_current_viewer_origin_count": 0,
                "host_env_current_viewer_origin": None,
                "host_env_planned_sha256": _host_environment_transition()["post_sha256"],
                "host_env_planned_bytes": _host_environment_transition()["post_bytes"],
                "host_env_planned_viewer_origin": VIEWER_ORIGIN,
            },
    )
    monkeypatch.setattr(
        release,
        "_observe_launchd_job",
        lambda **_kwargs: {"job_present": True, "state": "stopped", "pid": None},
    )
    monkeypatch.setattr(release, "_observe_host_dependency_environment", lambda: dependency)
    assert release._observe_host_live_runtime(expected_host=expected)["runtime_file_sha256"] == expected_files
    source.write_text("print('stale')\n")
    with pytest.raises(release.ProdE2EReleaseError, match="runtime_file_mismatch"):
        release._observe_host_live_runtime(expected_host=expected)
    source.write_text("print('ok')\n")
    write_plist(["/usr/bin/python3", str(source)])
    with pytest.raises(release.ProdE2EReleaseError, match="program_arguments_invalid"):
        release._observe_host_live_runtime(expected_host=expected)
    write_plist([str(root / ".venv/bin/python"), str(source)])
    (root / "scripts/retired.py").write_text("retired\n")
    with pytest.raises(release.ProdE2EReleaseError, match="retired_executor_present"):
        release._observe_host_live_runtime(expected_host=expected)
    (root / "scripts/retired.py").unlink()
    monkeypatch.setattr(release, "_observe_host_dependency_environment", lambda: {**dependency, "interpreter_sha256": "b2" * 32})
    with pytest.raises(release.ProdE2EReleaseError, match="dependency_mismatch"):
        release._observe_host_live_runtime(expected_host=expected)


def test_launchd_effective_config_and_process_must_match(monkeypatch):
    python = f"{release.HOST_LIVE_ROOT}/.venv/bin/python"
    script = f"{release.HOST_LIVE_ROOT}/scripts/pnc_rca_kafka_consumer.py"
    arguments = [python, script]
    environment = {
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1",
    }

    def launchctl_output(*, loaded_script: str = script) -> str:
        return f"""
state = running
program = {python}
arguments = {{
  {python}
  {loaded_script}
}}
working directory = {release.HOST_LIVE_ROOT}
environment = {{
  PYTHONDONTWRITEBYTECODE => 1
  PYTHONNOUSERSITE => 1
}}
pid = 42
"""

    monkeypatch.setattr(
        release.subprocess,
        "run",
        lambda *_args, **_kwargs: release.subprocess.CompletedProcess(
            [], 0, launchctl_output(), ""
        ),
    )

    class Process:
        command = arguments

        def __init__(self, pid):
            assert pid == 42

        def create_time(self):
            return 100.0

        def oneshot(self):
            return contextlib.nullcontext()

        def exe(self):
            return python

        def cmdline(self):
            return list(self.command)

        def cwd(self):
            return release.HOST_LIVE_ROOT

        def environ(self):
            return dict(environment)

    monkeypatch.setattr(release.psutil, "Process", Process)
    result = release._observe_launchd_job(
        label="local.pnc.rca-kafka-consumer",
        expected_arguments=arguments,
        expected_working_directory=release.HOST_LIVE_ROOT,
        expected_environment=environment,
        require_running=True,
    )
    assert result["process_executable"] == python

    monkeypatch.setattr(
        release.subprocess,
        "run",
        lambda *_args, **_kwargs: release.subprocess.CompletedProcess(
            [], 0, launchctl_output(loaded_script="/wrong.py"), ""
        ),
    )
    with pytest.raises(release.ProdE2EReleaseError, match="loaded_service_config"):
        release._observe_launchd_job(
            label="local.pnc.rca-kafka-consumer",
            expected_arguments=arguments,
            expected_working_directory=release.HOST_LIVE_ROOT,
            expected_environment=environment,
            require_running=True,
        )

    monkeypatch.setattr(
        release.subprocess,
        "run",
        lambda *_args, **_kwargs: release.subprocess.CompletedProcess(
            [], 0, launchctl_output(), ""
        ),
    )
    Process.command = [python, "/wrong.py"]
    with pytest.raises(release.ProdE2EReleaseError, match="process_identity"):
        release._observe_launchd_job(
            label="local.pnc.rca-kafka-consumer",
            expected_arguments=arguments,
            expected_working_directory=release.HOST_LIVE_ROOT,
            expected_environment=environment,
            require_running=True,
        )

    Process.command = arguments
    with pytest.raises(release.ProdE2EReleaseError, match="process_identity"):
        release._observe_launchd_job(
            label="local.pnc.rca-kafka-consumer",
            expected_arguments=arguments,
            expected_working_directory=release.HOST_LIVE_ROOT,
            expected_environment=environment,
            require_running=True,
            required_process_environment={
                release.VIEWER_ORIGIN_ENV: VIEWER_ORIGIN
            },
        )


@pytest.mark.parametrize(
    "value",
    [
        "",
        " https://viewer.minieye.tech",
        "https://VIEWER.minieye.tech",
        "https://192.168.21.217",
        "https://[2001:db8::1]",
        "https://viewer.minieye.tech:443",
        "https://viewer.minieye.tech/",
        "https://viewer.minieye.tech/path",
        "https://user@viewer.minieye.tech",
        "https://xn--viewer-9za.minieye.tech",
        "http://viewer.minieye.tech",
    ],
)
def test_viewer_origin_requires_canonical_https_dns(value):
    with pytest.raises(release.ProdE2EReleaseError, match="viewer_origin_invalid"):
        release._canonical_https_dns_origin(value, field="viewer_origin")

    assert (
        release._canonical_https_dns_origin(
            VIEWER_ORIGIN, field="viewer_origin"
        )
        == VIEWER_ORIGIN
    )


def test_host_dependency_probe_rejects_symlink_interpreter(tmp_path, monkeypatch):
    root = tmp_path / "live"
    (root / ".venv/bin").mkdir(parents=True)
    (root / ".venv/bin/python").symlink_to("/bin/sh")
    (root / ".venv/pyvenv.cfg").write_text("home = /usr/bin\n")
    monkeypatch.setattr(release, "HOST_LIVE_ROOT", str(root))
    with pytest.raises(release.ProdE2EReleaseError, match="live_interpreter_invalid"):
        release._observe_host_dependency_environment()


def _completion_context(tmp_path: Path, monkeypatch, *, rollback: bool = False):
    service_runtime = {
        label: "c1" * 32 for label in release.HOST_SERVICE_LABELS
    }
    host = {
        "commit": release.HOST_FINAL_COMMIT,
        "tree": release.HOST_FINAL_TREE,
        "viewer_origin": VIEWER_ORIGIN,
        "host_environment_transition": _host_environment_transition(),
        "runtime_files_sha256": "c1" * 32,
        "service_runtime_files_sha256": service_runtime,
        "required_file_sha256": {
            entrypoint: "c0" * 32
            for entrypoint in release.HOST_SERVICE_ENTRYPOINTS.values()
            if entrypoint
        },
        "runtime_file_sha256": {
            entrypoint: "c0" * 32
            for entrypoint in release.HOST_SERVICE_ENTRYPOINTS.values()
            if entrypoint
        },
        "service_config_sha256": {label: "c2" * 32 for label in release.HOST_SERVICE_LABELS},
        "canonical_interpreter_sha256": "cf" * 32,
    }
    worker = {
        "daemon_unit_config_sha256": "c3" * 32,
        "report_unit_config_sha256": release.VM_REPORT_UNIT_SHA256,
        "report_environment": {
            "path": release.VM_REPORT_ENV_PATH,
            "exists": True,
            "sha256": _report_service()["environment_file_sha256"],
            "bytes": _report_service()["environment_file_bytes"],
            "owner_uid": 1000,
            "mode": "0600",
            "variable": release.VM_REPORT_ENV_VARIABLE,
            "viewer_origin": VIEWER_ORIGIN,
        },
        "report_environment_transition": _vm_report_environment_transition(),
    }
    request = {
        "release_id": RELEASE_ID,
        "request_sha256": "c5" * 32,
        "release_bom_sha256": "c6" * 32,
        "release_bom": {
            "host_runtime": {"canonical_final": host},
            "component_identities": {
                "evidence_path": str(tmp_path / "component.json"),
                "evidence_sha256": "c7" * 32,
                "worker": worker,
                "pipeline": {"report_service": _report_service()},
                "viewer_proxy": _viewer_proxy_candidate(),
            },
            "delivery_store_cutover": {
                "approved_source_logical_sha256": "c8" * 32,
                "approved_post_migration_logical_sha256": "c9" * 32,
                "quarantine_core": {"core_sha256": "ca" * 32},
            },
            "bootstrap_authorization": {"bootstrap_epoch_id": EPOCH_ID},
        },
    }
    _write_json(tmp_path / "component.json", {"component": True})
    backup = _write_json(tmp_path / "backup.json", {"source": True})
    anchors = {
        "anchor": "before",
        "live_env_sha256": _host_environment_transition()["pre_sha256"],
    }
    final_body = {"placeholder": True}
    final_path = _write_json(tmp_path / "final.json", final_body)
    final_owned = _owned(final_path)
    verified = {
        "validated_at": NOW.isoformat(),
        "execute_before": (NOW + timedelta(hours=1)).isoformat(),
        "approval": {"sha256": "cb" * 32},
        "baseline_approval": {"sha256": "cc" * 32},
        "preflight": {
            "fresh_live_backup": {"path": str(backup), "sha256": _sha(backup)},
            "activation_anchors_before": anchors,
            "target_kafka_preread": {
                "retained_end": 700,
            },
            "target_input_revalidation": {
                "observed_at": (NOW + timedelta(seconds=1)).isoformat(),
            },
        },
    }
    monkeypatch.setattr(release, "_validate_final_validation_receipt", lambda *_args, **_kwargs: verified)
    host_live = {"runtime_files_sha256": host["runtime_files_sha256"]}
    monkeypatch.setattr(release, "_observe_host_live_runtime", lambda **_kwargs: host_live)
    running_host = {
        label: {"job_present": True, "state": "running", "pid": 100 + index, "config_sha256": host["service_config_sha256"][label]}
        for index, label in enumerate(release.HOST_SERVICE_LABELS)
    }
    monkeypatch.setattr(release, "_observe_host_writer_stop_live", lambda: running_host)
    vm_services = {
        release.VM_DAEMON_UNIT: {**_vm_service(release.VM_DAEMON_UNIT, active=True), "main_pid": 201, "unit_config_sha256": worker["daemon_unit_config_sha256"]},
        release.VM_REPORT_UNIT: {**_vm_service(release.VM_REPORT_UNIT, active=True), "main_pid": 202, "unit_config_sha256": worker["report_unit_config_sha256"]},
    }
    stopped_vm_services = {
        release.VM_DAEMON_UNIT: {
            **_vm_service(release.VM_DAEMON_UNIT, active=False),
            "unit_config_sha256": worker["daemon_unit_config_sha256"],
        },
        release.VM_REPORT_UNIT: {
            **_vm_service(release.VM_REPORT_UNIT, active=False),
            "unit_config_sha256": worker["report_unit_config_sha256"],
        },
    }
    verified["preflight"]["vm_services"] = stopped_vm_services

    def observe_vm(*, environment_phase="post", **_kwargs):
        if environment_phase == "post":
            current = {
                "path": release.VM_REPORT_ENV_PATH,
                "exists": True,
                "sha256": worker["report_environment"]["sha256"],
                "bytes": worker["report_environment"]["bytes"],
                "owner_uid": 1000,
                "mode": "0600",
                "variable": release.VM_REPORT_ENV_VARIABLE,
                "viewer_origin": VIEWER_ORIGIN,
            }
            services = vm_services
        else:
            current = {
                "path": release.VM_REPORT_ENV_PATH,
                "exists": False,
                "sha256": release.EMPTY_SHA256,
                "bytes": 0,
                "owner_uid": None,
                "mode": None,
                "variable": release.VM_REPORT_ENV_VARIABLE,
                "viewer_origin": None,
            }
            services = stopped_vm_services
        return {
            "services": services,
            "report_environment": current,
            "report_environment_transition": (
                worker["report_environment_transition"]
                if environment_phase == "pre"
                else {
                    **worker["report_environment_transition"],
                    "pre_exists": True,
                    "pre_sha256": worker["report_environment"]["sha256"],
                    "pre_bytes": worker["report_environment"]["bytes"],
                    "pre_owner_uid": 1000,
                    "pre_mode": "0600",
                    "write_required": False,
                }
            ),
        }

    monkeypatch.setattr(release, "_observe_vm_components_live", observe_vm)
    monkeypatch.setattr(release, "_observe_activation_anchors_live", lambda value: value)
    monkeypatch.setattr(release, "_run_canonical_db_projection", lambda **_kwargs: {"projection": {"logical_sha256": request["release_bom"]["delivery_store_cutover"]["approved_source_logical_sha256"]}})
    return request, final_owned, verified, backup, anchors, host, worker, running_host, vm_services, host_live


def test_success_closeout_validates_actual_fields_comment_readback_lineage_and_canary(tmp_path, monkeypatch):
    request, final_owned, verified, backup, _anchors, host, worker, running_host, vm_services, host_live = _completion_context(tmp_path, monkeypatch)
    bundle_path = _write_json(tmp_path / "bundle.json", {"bundle": "actual"})
    canary_bundle_path = _write_json(
        tmp_path / "canary-bundle.json", {"bundle": "canary"}
    )
    field_values = {
        release.TARGET_FIELD_KEYS[0]: {"sha256": "d1" * 32, "utf8_bytes": 12},
        release.TARGET_FIELD_KEYS[1]: {"sha256": "d2" * 32, "utf8_bytes": 24},
    }
    comment_value = {"sha256": "d3" * 32, "utf8_bytes": 36}
    marker_value = {"sha256": "d0" * 32, "utf8_bytes": 32}
    verified_bundle = {
        "business_key": release.TARGET_BUSINESS_KEY,
        "submission_key": release.TARGET_SUBMISSION_KEY,
        "generation": 1,
        "project_key": release.TARGET_PROJECT_KEY,
        "project_simple_name": release.TARGET_PROJECT_SIMPLE_NAME,
        "work_item_type_key": "issue",
        "work_item_id": release.TARGET_WORK_ITEM_ID,
        "delivery_id": "delivery-target",
        "effect_key": "effect-target",
        "semantic_payload_sha256": "d4" * 32,
        "artifact_set_id": "artifact-target",
        "target_key": f"feishu_project:{release.TARGET_PROJECT_KEY}:issue:{release.TARGET_WORK_ITEM_ID}",
        "issue_url": release.TARGET_ISSUE_URL,
        "report_url": "http://192.168.26.174:18081/G1Q3_RCA/cases/target/index.html",
        "report_link_kind": "manifest_html",
        "field_values": field_values,
        "comment_content": comment_value,
        "marker": marker_value,
    }
    canary_field_values = {
        release.TARGET_FIELD_KEYS[0]: {"sha256": "da" * 32, "utf8_bytes": 14},
        release.TARGET_FIELD_KEYS[1]: {"sha256": "db" * 32, "utf8_bytes": 28},
    }
    canary_comment_value = {"sha256": "dc" * 32, "utf8_bytes": 40}
    canary_marker_value = {"sha256": "dd" * 32, "utf8_bytes": 34}
    canary_bundle = {
        "business_key": "canary-business",
        "submission_key": "canary-submission",
        "generation": 1,
        "project_key": release.TARGET_PROJECT_KEY,
        "project_simple_name": release.TARGET_PROJECT_SIMPLE_NAME,
        "work_item_type_key": "issue",
        "work_item_id": "7059999999",
        "delivery_id": "delivery-canary",
        "effect_key": "effect-canary",
        "semantic_payload_sha256": "de" * 32,
        "artifact_set_id": "artifact-canary",
        "target_key": f"feishu_project:{release.TARGET_PROJECT_KEY}:issue:7059999999",
        "issue_url": (
            f"https://project.feishu.cn/{release.TARGET_PROJECT_SIMPLE_NAME}"
            "/issue/detail/7059999999"
        ),
        "report_url": "http://192.168.26.174:18081/G1Q3_RCA/cases/canary/index.html",
        "report_link_kind": "manifest_html",
        "field_values": canary_field_values,
        "comment_content": canary_comment_value,
        "marker": canary_marker_value,
    }
    monkeypatch.setattr(
        release,
        "_run_canonical_target_bundle_verifier",
        lambda *_args, expected=None, **_kwargs: (
            canary_bundle if expected is not None else verified_bundle
        ),
    )
    monkeypatch.setattr(release, "_validate_cutover_database_checkpoint", lambda *_args, **_kwargs: {"path": str(tmp_path / "checkpoint.sqlite3"), "logical_sha256": request["release_bom"]["delivery_store_cutover"]["approved_post_migration_logical_sha256"]})
    monkeypatch.setattr(release, "_validate_component_binding", lambda *_args, **_kwargs: {"sha256": request["release_bom"]["component_identities"]["evidence_sha256"]})
    monkeypatch.setattr(release, "_observe_live_baseline", lambda **_kwargs: {"ready": True})
    live_readback = {
        "fields": {key: {"sha256": value["sha256"], "utf8_bytes": value["utf8_bytes"]} for key, value in field_values.items()},
        "comment_content": comment_value,
        "comment_id": "comment-actual-7051585084",
        "marker_sha256": marker_value["sha256"],
        "marker_match_count": 1,
    }
    canary_live_readback = {
        "fields": {
            key: {"sha256": value["sha256"], "utf8_bytes": value["utf8_bytes"]}
            for key, value in canary_field_values.items()
        },
        "comment_content": canary_comment_value,
        "comment_id": "comment-canary",
        "marker_sha256": canary_marker_value["sha256"],
        "marker_match_count": 1,
    }
    monkeypatch.setattr(
        release,
        "_observe_official_meegle_readback_live",
        lambda **kwargs: (
            canary_live_readback
            if kwargs["work_item_id"] == "7059999999"
            else live_readback
        ),
    )
    monkeypatch.setattr(release, "_observe_live_delivery_database", lambda **_kwargs: {"allowed_delta": {"unrelated_delta_count": 0}})
    execution = NOW + timedelta(minutes=1)
    times = [execution + timedelta(seconds=index) for index in range(1, 11)]
    host_restart = {
        label: {"old_pid": index + 1, "new_pid": running_host[label]["pid"], "runtime_sha256": host["service_runtime_files_sha256"][label], "config_sha256": host["service_config_sha256"][label]}
        for index, label in enumerate(release.HOST_SERVICE_LABELS)
    }
    vm_restarts = {
        unit: {"unit": unit, "old_pid": 1, "new_pid": vm_services[unit]["main_pid"], "unit_config_sha256": vm_services[unit]["unit_config_sha256"], "entrypoint": vm_services[unit]["entrypoint"], "entrypoint_sha256": vm_services[unit]["entrypoint_sha256"]}
        for unit in release.VM_SERVICE_UNITS
    }
    target = {
        "topic": release.TOPIC,
        "partition": release.PARTITION,
        "offset": release.TARGET_OFFSET,
        "event_uid": release.TARGET_EVENT_UID,
        "raw_sha256": release.TARGET_RAW_SHA256,
        "project_key": release.TARGET_PROJECT_KEY,
        "work_item_id": release.TARGET_WORK_ITEM_ID,
        "work_item_type_key": "issue",
        "business_key": release.TARGET_BUSINESS_KEY,
        "submission_key": release.TARGET_SUBMISSION_KEY,
        "generation": 1,
        "task_id": release.TARGET_SUBMISSION_KEY,
        "delivery_id": verified_bundle["delivery_id"],
        "effect_key": verified_bundle["effect_key"],
        "semantic_payload_sha256": verified_bundle["semantic_payload_sha256"],
        "artifact_set_id": verified_bundle["artifact_set_id"],
        "target_key": verified_bundle["target_key"],
        "issue_url": release.TARGET_ISSUE_URL,
        "terminal_bundle_path": str(bundle_path),
        "terminal_bundle_sha256": _sha(bundle_path),
        "terminal_receipt_sha256": _sha(bundle_path),
        "terminal_at": (times[-1] + timedelta(seconds=1)).isoformat(),
        "status": "report_ready",
    }
    target["activation"] = _delta_activation(
        target,
        entrypoint="kafka_ingest",
        slot_kind="kafka_success",
        reason="activation_bounded_slot_consumed",
    )
    writes = [
        {"field_key": key, "value_sha256": value["sha256"], "value_utf8_bytes": value["utf8_bytes"], "written_at": (times[-1] + timedelta(seconds=2)).isoformat()}
        for key, value in field_values.items()
    ]
    comment = {"comment_id": "comment-actual-7051585084", "content_sha256": comment_value["sha256"], "content_utf8_bytes": comment_value["utf8_bytes"], "written_at": (times[-1] + timedelta(seconds=3)).isoformat(), "attempt_terminal_outcome": "ack"}
    readback = {"adapter": "MeegleIssueCommentAdapter.get_fields_and_comments", "source": "official_meegle_api", "scope": {"project_key": release.TARGET_PROJECT_KEY, "work_item_id": release.TARGET_WORK_ITEM_ID}, "observed_at": (times[-1] + timedelta(seconds=4)).isoformat(), "fields": {item["field_key"]: {"value_sha256": item["value_sha256"], "value_utf8_bytes": item["value_utf8_bytes"]} for item in writes}, "comment_id": comment["comment_id"], "comment_content_sha256": comment["content_sha256"], "marker_sha256": marker_value["sha256"], "marker_match_count": 1}
    target_readback_evidence = _write_json(
        tmp_path / "target-full-readback.json",
        {
            "schema_version": "pnc_rca_official_full_readback_v1",
            "release_id": RELEASE_ID,
            "adapter": readback["adapter"],
            "source": readback["source"],
            "scope": readback["scope"],
            "observed_at": readback["observed_at"],
            "fields": field_values,
            "comment_id": comment["comment_id"],
            "comment_content_sha256": comment["content_sha256"],
            "comment_content_utf8_bytes": comment["content_utf8_bytes"],
            "marker_sha256": marker_value["sha256"],
            "marker_match_count": 1,
            "pages_read": 1,
            "comments": [{
                "comment_id": comment["comment_id"],
                "content_sha256": comment["content_sha256"],
                "content_utf8_bytes": comment["content_utf8_bytes"],
                "marker_match_count": 1,
            }],
            "terminal_receipt_sha256": target["terminal_receipt_sha256"],
            "full_bodies_persisted": False,
        },
    )
    readback.update({
        "evidence_path": str(target_readback_evidence),
        "evidence_sha256": _sha(target_readback_evidence),
    })
    lineage = {"business_key": release.TARGET_BUSINESS_KEY, "submission_key": target["submission_key"], "task_id": target["task_id"], "delivery_id": target["delivery_id"], "effect_key": target["effect_key"], "semantic_payload_sha256": target["semantic_payload_sha256"], "artifact_set_id": target["artifact_set_id"], "terminal_receipt_sha256": target["terminal_receipt_sha256"], "field_keys": list(release.TARGET_FIELD_KEYS), "comment_id": comment["comment_id"], "attempt_terminal_outcome": "ack"}
    lineage["lineage_sha256"] = release._sha256_value(lineage)
    canary_writes = [
        {"field_key": key, "value_sha256": value["sha256"], "value_utf8_bytes": value["utf8_bytes"], "written_at": (times[-1] + timedelta(seconds=6)).isoformat()}
        for key, value in canary_field_values.items()
    ]
    canary_comment = {"comment_id": "comment-canary", "content_sha256": canary_comment_value["sha256"], "content_utf8_bytes": canary_comment_value["utf8_bytes"], "written_at": (times[-1] + timedelta(seconds=7)).isoformat(), "attempt_terminal_outcome": "ack"}
    canary_readback = {"adapter": "MeegleIssueCommentAdapter.get_fields_and_comments", "source": "official_meegle_api", "scope": {"project_key": release.TARGET_PROJECT_KEY, "work_item_id": "7059999999"}, "observed_at": (times[-1] + timedelta(seconds=8)).isoformat(), "fields": {item["field_key"]: {"value_sha256": item["value_sha256"], "value_utf8_bytes": item["value_utf8_bytes"]} for item in canary_writes}, "comment_id": canary_comment["comment_id"], "comment_content_sha256": canary_comment["content_sha256"], "marker_sha256": canary_marker_value["sha256"], "marker_match_count": 1}
    canary_readback_evidence = _write_json(
        tmp_path / "canary-full-readback.json",
        {
            "schema_version": "pnc_rca_official_full_readback_v1",
            "release_id": RELEASE_ID,
            "adapter": canary_readback["adapter"],
            "source": canary_readback["source"],
            "scope": canary_readback["scope"],
            "observed_at": canary_readback["observed_at"],
            "fields": canary_field_values,
            "comment_id": canary_comment["comment_id"],
            "comment_content_sha256": canary_comment["content_sha256"],
            "comment_content_utf8_bytes": canary_comment["content_utf8_bytes"],
            "marker_sha256": canary_marker_value["sha256"],
            "marker_match_count": 1,
            "pages_read": 1,
            "comments": [{
                "comment_id": canary_comment["comment_id"],
                "content_sha256": canary_comment["content_sha256"],
                "content_utf8_bytes": canary_comment["content_utf8_bytes"],
                "marker_match_count": 1,
            }],
            "terminal_receipt_sha256": _sha(canary_bundle_path),
            "full_bodies_persisted": False,
        },
    )
    canary_readback.update({
        "evidence_path": str(canary_readback_evidence),
        "evidence_sha256": _sha(canary_readback_evidence),
    })
    canary_lineage = {"business_key": "canary-business", "submission_key": "canary-submission", "task_id": "canary-submission", "delivery_id": canary_bundle["delivery_id"], "effect_key": canary_bundle["effect_key"], "semantic_payload_sha256": canary_bundle["semantic_payload_sha256"], "artifact_set_id": canary_bundle["artifact_set_id"], "terminal_receipt_sha256": _sha(canary_bundle_path), "field_keys": list(release.TARGET_FIELD_KEYS), "comment_id": canary_comment["comment_id"], "attempt_terminal_outcome": "ack"}
    canary_lineage["lineage_sha256"] = release._sha256_value(canary_lineage)
    kafka_recorded_at = times[-1] + timedelta(seconds=2)
    canary_kafka = {
        "offset": 700,
        "retained_start": 514,
        "retained_end": 701,
        "record_timestamp_ms": int(kafka_recorded_at.timestamp() * 1000),
        "raw_sha256": "d5" * 32,
        "business_key": "canary-business",
        "submission_key": "canary-submission",
        "work_item_id": "7059999999",
        "observed_at": kafka_recorded_at.isoformat(),
    }
    canary = {"topic": release.TOPIC, "partition": release.PARTITION, "offset": 700, "event_uid": f"{release.TOPIC}:0:700", "raw_sha256": "d5" * 32, "project_key": release.TARGET_PROJECT_KEY, "work_item_type_key": "issue", "work_item_id": "7059999999", "business_key": "canary-business", "submission_key": "canary-submission", "generation": 1, "trigger_kind": "issue_created", "source_kind": "kafka_workflow_event", "delivery_source": "ordinary_kafka_ingest", "recovery_write_count": 0, "operator_recovery_provenance": [], "kafka_preread": canary_kafka, "observed_at": (times[-1] + timedelta(seconds=5)).isoformat(), "terminal_at": (times[-1] + timedelta(seconds=9)).isoformat(), "status": "closed", "task_id": "canary-submission", "delivery_id": canary_bundle["delivery_id"], "effect_key": canary_bundle["effect_key"], "semantic_payload_sha256": canary_bundle["semantic_payload_sha256"], "artifact_set_id": canary_bundle["artifact_set_id"], "target_key": canary_bundle["target_key"], "issue_url": canary_bundle["issue_url"], "terminal_bundle_path": str(canary_bundle_path), "terminal_bundle_sha256": _sha(canary_bundle_path), "terminal_receipt_sha256": _sha(canary_bundle_path), "field_writes": canary_writes, "comment_write": canary_comment, "official_readback": canary_readback, "delivery_lineage": canary_lineage}
    canary["activation"] = _delta_activation(
        canary,
        entrypoint="kafka_ingest",
        slot_kind="",
        reason="activation_steady_active",
    )
    natural_receipt_path = _write_json(
        tmp_path / "natural-canary-receipt.json",
        {
            "schema_version": "pnc_rca_natural_kafka_canary_receipt_v1",
            "release_id": RELEASE_ID,
            "epoch_id": EPOCH_ID,
            "request_sha256": "a1" * 32,
            "selected_at": (times[-1] + timedelta(seconds=5)).isoformat(),
            "topic": canary["topic"],
            "partition": canary["partition"],
            "offset": canary["offset"],
            "event_uid": canary["event_uid"],
            "business_key": canary["business_key"],
            "submission_key": canary["submission_key"],
            "generation": 1,
            "decision": "accepted",
            "activation_reason": "activation_steady_active",
            "consumer_group_id": "rca_root_cause_analysis_agent",
            "kafka_offset_committed": True,
            "resident_runtime_identity_sha256": "a2" * 32,
            "next_ordinary_record_held": True,
        },
    )
    canary["resident_canary_receipt"] = {
        "path": str(natural_receipt_path),
        "sha256": _sha(natural_receipt_path),
    }
    observed = times[-1] + timedelta(seconds=10)
    monkeypatch.setattr(
        release,
        "_observe_kafka_record_live",
        lambda **_kwargs: {
            **canary_kafka,
            "retained_end": 702,
            "observed_at": (observed - timedelta(milliseconds=100)).isoformat(),
        },
    )
    cutover = {name: value.isoformat() for name, value in zip(("writers_stopped_at", "backup_captured_at", "database_installed_at", "post_digest_verified_at", "core_verified_at", "baseline_bound_at", "environment_written_at", "active_binding_written_at", "services_restarted_at", "viewer_proxy_reloaded_at"), times)}
    gate_body = {
        "schema_version": "pnc_rca_post_cutover_kafka_end_gate_v1",
        "release_id": RELEASE_ID,
        "observed_at": (times[-1] + timedelta(seconds=1)).isoformat(),
        "topic": release.TOPIC,
        "partition": release.PARTITION,
        "retained_start": 514,
        "retained_end": 700,
        "assignment_mode": "explicit_single_partition",
        "assigned_partitions": [release.PARTITION],
        "group_id": None,
        "enable_auto_commit": False,
        "commit_called": False,
        "raw_payload_persisted": False,
        "consumer_module_sha256": host["required_file_sha256"]["scripts/pnc_rca_kafka_consumer.py"],
        "observer_script_sha256": hashlib.sha256(release._CANONICAL_KAFKA_END_GATE.encode()).hexdigest(),
        "interpreter_sha256": host["canonical_interpreter_sha256"],
        "host_commit": host["commit"],
        "host_tree": host["tree"],
        "consumer_pid": host_restart["local.pnc.rca-kafka-consumer"]["new_pid"],
        "consumer_runtime_sha256": host_restart["local.pnc.rca-kafka-consumer"]["runtime_sha256"],
        "consumer_config_sha256": host_restart["local.pnc.rca-kafka-consumer"]["config_sha256"],
    }
    gate_path = _write_json(tmp_path / "canary-kafka-gate.json", gate_body)
    cutover.update({"post_logical_sha256": request["release_bom"]["delivery_store_cutover"]["approved_post_migration_logical_sha256"], "post_install_checkpoint": {"placeholder": True}, "quarantine_core_sha256": request["release_bom"]["delivery_store_cutover"]["quarantine_core"]["core_sha256"], "baseline_approval_sha256": verified["baseline_approval"]["sha256"], "baseline_file_sha256": "d6" * 32, "baseline_path": str(tmp_path / "baseline.json"), "active_release_binding_path": str(tmp_path / "active.json"), "live_env_path": release.CANONICAL_HOST_ENV, "live_env_pre_sha256": _host_environment_transition()["pre_sha256"], "live_env_post_sha256": _host_environment_transition()["post_sha256"], "live_env_post_bytes": _host_environment_transition()["post_bytes"], "live_env_atomic_replace": True, "live_env_written_after_core_gate": True, "vm_report_env_path": release.VM_REPORT_ENV_PATH, "vm_report_env_pre_exists": False, "vm_report_env_pre_sha256": release.EMPTY_SHA256, "vm_report_env_post_sha256": _vm_report_environment_transition()["post_sha256"], "vm_report_env_post_bytes": _vm_report_environment_transition()["post_bytes"], "vm_report_env_atomic_replace": True, "vm_report_env_written_after_core_gate": True, "host_restart": host_restart, "vm_restarts": vm_restarts, "canary_kafka_gate": {"evidence_path": str(gate_path), "evidence_sha256": _sha(gate_path)}, "restore_required": False, "restore_performed": False})
    proxy_path = _write_json(
        tmp_path / "viewer-proxy-live.json",
        _viewer_proxy_live_body(
            candidate=request["release_bom"]["component_identities"][
                "viewer_proxy"
            ],
            report_service=request["release_bom"]["component_identities"][
                "pipeline"
            ]["report_service"],
            report_restart=vm_restarts[release.VM_REPORT_UNIT],
            reloaded_at=times[-1],
            observed_at=times[-1] + timedelta(milliseconds=500),
        ),
    )
    proxy_closeout = {
        "observation_path": str(proxy_path),
        "observation_sha256": _sha(proxy_path),
    }
    body = {"schema_version": release.COMPLETION_RECEIPT_SCHEMA_VERSION, "release_id": RELEASE_ID, "observed_at": observed.isoformat(), "execution_started_at": execution.isoformat(), "final_validation_sha256": final_owned.sha256, "request_sha256": request["request_sha256"], "release_bom_sha256": request["release_bom_sha256"], "outcome": "success", "cutover": cutover, "target_delivery": target, "field_writes": writes, "comment_write": comment, "official_readback": readback, "delivery_lineage": lineage, "post_cutover_canary": canary, "viewer_proxy_closeout": proxy_closeout, "production_effects": {"live_database_mutated": True, "environment_written": True, "active_binding_written": True, "services_restarted": True, "viewer_proxy_deployed": True, "viewer_proxy_verified": True, "target_o650_executed": True, "feishu_written": True, "canary_executed": True}}
    completion_path = _write_json(tmp_path / "completion.json", body)
    result = release._validate_completion_receipt(_owned(completion_path), request=request, final_validation=final_owned, now=observed)
    assert result["production_completed"] is True
    assert result["comment_id"] == comment["comment_id"]
    assert result["canary_event_uid"] == canary["event_uid"]
    mismatched_full_body = json.loads(
        target_readback_evidence.read_text(encoding="utf-8")
    )
    mismatched_full_body["comments"][0]["content_sha256"] = "0" * 64
    mismatched_full_body_path = _write_json(
        tmp_path / "target-full-readback-mismatched.json", mismatched_full_body
    )
    mismatched_readback = copy.deepcopy(body)
    mismatched_readback["official_readback"]["evidence_path"] = str(
        mismatched_full_body_path
    )
    mismatched_readback["official_readback"]["evidence_sha256"] = _sha(
        mismatched_full_body_path
    )
    with pytest.raises(
        release.ProdE2EReleaseError,
        match="official_full_readback_evidence_invalid",
    ):
        release._validate_completion_receipt(
            release.OwnedJson(completion_path, b"{}", mismatched_readback),
            request=request,
            final_validation=final_owned,
            now=observed,
        )
    duplicate_marker = copy.deepcopy(body)
    duplicate_marker["post_cutover_canary"]["official_readback"][
        "marker_match_count"
    ] = 2
    with pytest.raises(
        release.ProdE2EReleaseError, match="canary_official_readback_invalid"
    ):
        release._validate_completion_receipt(
            release.OwnedJson(completion_path, b"{}", duplicate_marker),
            request=request,
            final_validation=final_owned,
            now=observed,
        )
    wrong_canary_field = copy.deepcopy(body)
    wrong_canary_field["post_cutover_canary"]["field_writes"][0][
        "value_sha256"
    ] = "ff" * 32
    with pytest.raises(
        release.ProdE2EReleaseError, match="canary_field_writes_invalid"
    ):
        release._validate_completion_receipt(
            release.OwnedJson(completion_path, b"{}", wrong_canary_field),
            request=request,
            final_validation=final_owned,
            now=observed,
        )
    old = copy.deepcopy(body)
    old["schema_version"] = "pnc_rca_prod_e2e_completion_v1"
    old["cutover"]["post_logical_sha256"] = "993683".ljust(64, "0")
    with pytest.raises(release.ProdE2EReleaseError, match="completion_binding_invalid"):
        release._validate_completion_receipt(release.OwnedJson(completion_path, b"{}", old), request=request, final_validation=final_owned, now=observed)


def test_rollback_closeout_requires_exact_restore_and_both_vm_restarts(tmp_path, monkeypatch):
    request, final_owned, verified, backup, anchors, host, worker, running_host, vm_services, _host_live = _completion_context(tmp_path, monkeypatch, rollback=True)
    execution = NOW + timedelta(minutes=1)
    failure = execution + timedelta(seconds=1)
    restored = failure + timedelta(seconds=1)
    restarted = restored + timedelta(seconds=1)
    observed = restarted + timedelta(seconds=1)
    host_restart = {
        label: {
            "new_pid": running_host[label]["pid"],
            "runtime_sha256": host["service_runtime_files_sha256"][label],
        }
        for label in release.HOST_SERVICE_LABELS
    }
    vm_restore = {
        unit: {
            key: verified["preflight"]["vm_services"][unit][key]
            for key in (
                "unit",
                "active_state",
                "sub_state",
                "main_pid",
                "unit_config_sha256",
                "entrypoint_sha256",
            )
        }
        for unit in release.VM_SERVICE_UNITS
    }
    cutover = {
        "failure_step": "install_database",
        "failure_at": failure.isoformat(),
        "source_backup_sha256": _sha(backup),
        "restored_backup_sha256": _sha(backup),
        "restored_logical_sha256": request["release_bom"][
            "delivery_store_cutover"
        ]["approved_source_logical_sha256"],
        "restored_at": restored.isoformat(),
        "activation_anchors_restored": anchors,
        "services_restarted_at": restarted.isoformat(),
        "host_restart": host_restart,
        "vm_restore": vm_restore,
        "environment_written": False,
        "active_binding_written": False,
        "live_env_restored_sha256": host["host_environment_transition"][
            "pre_sha256"
        ],
        "vm_report_env_restored_exists": worker[
            "report_environment_transition"
        ]["pre_exists"],
        "vm_report_env_restored_sha256": worker[
            "report_environment_transition"
        ]["pre_sha256"],
    }
    body = {"schema_version": release.COMPLETION_RECEIPT_SCHEMA_VERSION, "release_id": RELEASE_ID, "observed_at": observed.isoformat(), "execution_started_at": execution.isoformat(), "final_validation_sha256": final_owned.sha256, "request_sha256": request["request_sha256"], "release_bom_sha256": request["release_bom_sha256"], "outcome": "rolled_back", "cutover": cutover, "target_delivery": None, "field_writes": [], "comment_write": None, "official_readback": None, "delivery_lineage": None, "post_cutover_canary": None, "viewer_proxy_closeout": None, "production_effects": {"live_database_restored": True, "environment_written": False, "active_binding_written": False, "target_o650_executed": False, "feishu_written": False, "canary_executed": False}}
    path = _write_json(tmp_path / "rollback.json", body)
    result = release._validate_completion_receipt(_owned(path), request=request, final_validation=final_owned, now=observed)
    assert result["production_completed"] is False
    body["cutover"]["vm_restore"].pop(release.VM_REPORT_UNIT)
    with pytest.raises(release.ProdE2EReleaseError, match="rollback_restart_invalid"):
        release._validate_completion_receipt(release.OwnedJson(path, b"{}", body), request=request, final_validation=final_owned, now=observed)


def test_forged_final_validation_receipt_cannot_authorize_completion(tmp_path):
    forged = _write_json(tmp_path / "forged-final.json", {"schema_version": release.VALIDATION_SCHEMA_VERSION, "ok": True})
    request = {"release_id": RELEASE_ID, "request_sha256": "e1" * 32, "release_bom_sha256": "e2" * 32}
    with pytest.raises(release.ProdE2EReleaseError, match="final_validation_receipt_invalid"):
        release._validate_final_validation_receipt(_owned(forged), request=request, execution_started_at=NOW, now=NOW)


def test_real_zero_cache_host_probe_rejects_stale_diagnostic_prereads():
    host = release._observe_canonical_host_binding(expected_commit=release.HOST_FINAL_COMMIT, expected_tree=release.HOST_FINAL_TREE)
    dependency = release._observe_host_dependency_environment()
    assert host["commit"] == release.HOST_FINAL_COMMIT
    assert dependency["site_packages_file_count"] > 1000
    assert dependency["installed_distribution_count"] > 1
    with pytest.raises(
        release.ProdE2EReleaseError, match="diagnostic_preread_invalid"
    ):
        release._validate_diagnostic_target_prereads()


def test_blocker_bom_exposes_validated_final_closure(monkeypatch):
    closure = {
        "path": release.PIPELINE_CLOSURE_SEALED_MIRROR_PATH,
        "sha256": release.PIPELINE_CLOSURE_FILE_SHA256,
        "evidence_core_sha256": release.PIPELINE_CLOSURE_CORE_SHA256,
        "entrypoint": release.PIPELINE_ENTRYPOINT,
        "reachable_hit_count": 0,
    }
    observed_path = None

    def read_owned(path, *, artifact):
        nonlocal observed_path
        observed_path = (path, artifact)
        return object()

    monkeypatch.setattr(
        release,
        "_observe_canonical_host_binding",
        lambda **_kwargs: {
            "commit": release.HOST_FINAL_COMMIT,
            "tree": release.HOST_FINAL_TREE,
            "runtime_allowlists": {},
            "required_file_sha256": {},
            "candidate_identity_evidence": {},
        },
    )
    monkeypatch.setattr(
        release, "_validate_viewer_proxy_static_evidence", lambda: {}
    )
    monkeypatch.setattr(release, "_read_owned_json", read_owned)
    monkeypatch.setattr(
        release, "_validate_closure_audit", lambda _owned: closure
    )
    monkeypatch.setattr(
        release,
        "_validate_diagnostic_target_prereads",
        lambda: {"kafka": {"ok": True}, "official": {"ok": True}},
    )

    bom = release.build_blocker_bom(now=NOW, verified_test_count=1)
    bound = bom["vm_candidate"]["fixed_cli_closure"]

    assert observed_path == (
        Path(release.PIPELINE_CLOSURE_SEALED_MIRROR_PATH),
        "blocker_bom_closure_audit",
    )
    assert bound["sha256"] == release.PIPELINE_CLOSURE_FILE_SHA256
    assert bound["evidence_core_sha256"] == release.PIPELINE_CLOSURE_CORE_SHA256
    assert bound["schema_version"] == release.CLOSURE_AUDIT_SCHEMA_VERSION
    assert bound["authorizes_final_candidate"] is True
    assert bound["production_mutation"] is False
    assert bom["target_recovery"]["diagnostic_live_preread"] == {"ok": True}
    assert bom["delivery_closeout"]["diagnostic_official_preread"] == {
        "ok": True
    }


def test_kafka_preread_executes_venv_entrypoint_without_resolving(
    tmp_path, monkeypatch
):
    assert Path(release.CANONICAL_HOST_PYTHON) == (
        Path(release.HOST_LIVE_ROOT) / ".venv/bin/python"
    )
    entrypoint = tmp_path / "venv/bin/python"
    binary = tmp_path / "python-real"
    binary.write_bytes(b"real-python-binary")
    captured = {}
    consumer_sha = "71" * 32
    contract_sha = "72" * 32
    observed = {
        "commit": release.HOST_FINAL_COMMIT,
        "tree": release.HOST_FINAL_TREE,
        "required_file_sha256": {
            "scripts/pnc_rca_kafka_consumer.py": consumer_sha,
            "gateway/pnc_rca_kafka_contract.py": contract_sha,
        },
    }
    payload = {
        "schema_version": "pnc_rca_kafka_exact_offset_preread_v1",
        "topic": release.TOPIC,
        "partition": release.PARTITION,
        "offset": release.TARGET_OFFSET,
        "event_uid": release.TARGET_EVENT_UID,
        "retained_start": 514,
        "retained_end": 700,
        "raw_sha256": release.TARGET_RAW_SHA256,
        "record_timestamp_ms": 1,
        "record_timestamp_type": "create_time",
        "raw_size_bytes": 10,
        "work_item_id": release.TARGET_WORK_ITEM_ID,
        "business_key": release.TARGET_BUSINESS_KEY,
        "submission_key": release.TARGET_SUBMISSION_KEY,
        "project_key": release.TARGET_PROJECT_KEY,
        "work_item_type_key": release.TARGET_WORK_ITEM_TYPE_KEY,
        "classification_decision": "accepted",
        "policy_version": "issue-created-v1",
        "assignment_mode": "explicit_single_partition",
        "assigned_partitions": [release.PARTITION],
        "seek_offset": release.TARGET_OFFSET,
        "position_after_read": release.TARGET_OFFSET + 1,
        "group_id": None,
        "enable_auto_commit": False,
        "commit_called": False,
        "raw_payload_persisted": False,
        "secret_material_persisted": False,
        "consumer_module_sha256": consumer_sha,
        "contract_module_sha256": contract_sha,
    }

    class Completed:
        returncode = 0
        stderr = ""
        stdout = json.dumps(payload)

    def run(arguments, **_kwargs):
        captured["arguments"] = arguments
        return Completed()

    monkeypatch.setattr(
        release, "_observe_canonical_host_binding", lambda **_kwargs: observed
    )
    monkeypatch.setattr(
        release,
        "_canonical_host_interpreter_paths",
        lambda: (entrypoint, binary),
    )
    monkeypatch.setattr(release.subprocess, "run", run)

    result = release._observe_target_kafka_record_live(
        host_commit=release.HOST_FINAL_COMMIT,
        host_tree=release.HOST_FINAL_TREE,
    )

    assert captured["arguments"][0] == str(entrypoint)
    assert captured["arguments"][0] != str(binary)
    assert "beginning_offsets([tp],timeout_ms=10000)" in captured[
        "arguments"
    ][4]
    assert "end_offsets([tp],timeout_ms=10000)" in captured["arguments"][4]
    assert "refs=admission.source_refs" in captured["arguments"][4]
    assert result["interpreter_sha256"] == hashlib.sha256(
        binary.read_bytes()
    ).hexdigest()


def test_canonical_runtime_bootstrap_is_verified_before_allowlist_probe(
    tmp_path, monkeypatch
):
    probe_called = False
    tracked_calls = []

    def fake_git_text(_root, *arguments):
        if arguments == ("rev-parse", "HEAD^{commit}"):
            return release.HOST_FINAL_COMMIT
        if arguments == ("rev-parse", "HEAD^{tree}"):
            return release.HOST_FINAL_TREE
        if arguments[:2] == ("status", "--porcelain=v1"):
            return ""
        raise AssertionError(arguments)

    def reject_bootstrap(_root, _commit, relative):
        tracked_calls.append(relative)
        raise release.ProdE2EReleaseError("bootstrap_blob_mismatch")

    def forbidden_probe():
        nonlocal probe_called
        probe_called = True
        raise AssertionError("allowlist probe ran before bootstrap verification")

    monkeypatch.setattr(release, "CANONICAL_HOST_ROOT", str(tmp_path))
    monkeypatch.setattr(release, "_host_git_text", fake_git_text)
    monkeypatch.setattr(
        release,
        "_validate_host_candidate_identity",
        lambda: {"path": "/owner/host.json", "sha256": "ab" * 32},
    )
    monkeypatch.setattr(release, "_host_tracked_bytes", reject_bootstrap)
    monkeypatch.setattr(
        release, "_observe_canonical_runtime_allowlists", forbidden_probe
    )

    with pytest.raises(
        release.ProdE2EReleaseError, match="bootstrap_blob_mismatch"
    ):
        release._observe_canonical_host_binding(
            expected_commit=release.HOST_FINAL_COMMIT,
            expected_tree=release.HOST_FINAL_TREE,
        )

    assert tracked_calls == [release.CANONICAL_RUNTIME_BOOTSTRAP_FILES[0]]
    assert probe_called is False


def test_cli_has_only_build_validate_and_closeout_no_apply():
    choices = release._parser()._subparsers._group_actions[0].choices
    assert set(choices) == {"build-request", "validate-only", "validate-closeout"}
    assert "apply" not in choices
    assert "promote" not in choices
