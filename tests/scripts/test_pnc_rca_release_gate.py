from __future__ import annotations

import copy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import stat
import subprocess
import sys
import time
from types import SimpleNamespace
from typing import Any, Mapping

import pytest

from tools import vm_task_tool
from gateway.pnc_rca_admission import build_rca_admission
from gateway.pnc_rca_derived_capacity_reservation import (
    CAPACITY_SCOPE as DERIVED_CAPACITY_SCOPE,
    DERIVED_RESERVATION_RECEIPT_SCHEMA_VERSION,
    DERIVED_RESERVATION_REQUEST_SCHEMA_VERSION,
    DerivedCapacityReservationRequest,
    HFS_PATH as DERIVED_HFS_PATH,
    TMP_PATH as DERIVED_TMP_PATH,
    canonical_data_access_sha256,
)
from gateway.pnc_rca_delivery_store import (
    DELIVERY_BACKPRESSURE_SNAPSHOT_SCHEMA_VERSION,
    DELIVERY_OUTCOME_CONSECUTIVE_FAILURE_THRESHOLD,
    DELIVERY_OUTCOME_CONSECUTIVE_WINDOW_SECONDS,
    DELIVERY_OUTCOME_SLO_SCHEMA_VERSION,
    DELIVERY_OUTCOME_SLO_WINDOWS,
)
from gateway.pnc_rca_schema import (
    RcaIssueContext,
    build_execution_request,
    to_dict as rca_to_dict,
)
from gateway.pnc_rca_stage_lineage import (
    RCA_STAGE_EXECUTION_POLICY,
    RCA_STAGE_LINEAGE_SCHEMA_VERSION,
    canonical_artifact_set_sha256,
    stage_lineage_relative_path,
)
from scripts import pnc_rca_activation as activation_module
from scripts import pnc_rca_production_cutover as production_cutover_module
from scripts import pnc_rca_release_gate as release_gate_module
from scripts import pnc_rca_store_migration_drill as migration_module
from scripts.pnc_rca_kafka_consumer import ConsumerConfig
from scripts.pnc_rca_kafka_preflight import (
    BrokerProbeConfig,
    load_environment as load_kafka_preflight_environment,
)
from scripts.pnc_rca_outbox_dispatcher import DispatcherConfig
from scripts.pnc_rca_release_gate import (
    BROKER_METADATA_SCHEMA_VERSION,
    BUILD_PROVENANCE_SCHEMA_VERSION,
    BUILD_MANIFEST_SCHEMA_VERSION,
    CANARY_PLAN_SCHEMA_VERSION,
    CANARY_RECEIPT_SCHEMA_VERSION,
    CAPACITY_RECEIPT_SCHEMA_VERSION,
    CUTOVER_PLAN_SCHEMA_VERSION,
    DELIVERY_CRITICAL_FILES,
    EMPTY_GIT_STATUS_SHA256,
    MINIMUM_CRITICAL_FILES,
    RELEASE_BOM_SCHEMA_VERSION,
    RELEASE_GATE_SCHEMA_VERSION,
    SHADOW_SOAK_SCHEMA_VERSION,
    T0_OFFSETS_SCHEMA_VERSION,
    VM_GIT_PROVENANCE_SCHEMA_VERSION,
    WORKFLOW_FIXTURES_SCHEMA_VERSION,
    ReleaseGateSettings,
    CutoverConfig,
    EvidenceError,
    check_candidate_runtime_dependencies,
    check_runtime_dependencies,
    evaluate_release_gate,
    load_cutover_config,
    load_redacted_configs,
    main,
    verify_live_build_provenance,
    verify_live_remote_reader,
    write_receipt_atomic,
)


TOPIC = "feishu-project-workflow-event"
RULE = "issue-created-v1"
SECRET = "release-gate-must-never-print-this-password"
NOW = datetime(2026, 7, 10, 8, 0, tzinfo=timezone.utc)
OBSERVED_AT = (NOW - timedelta(seconds=30)).isoformat()
PREDECESSOR_VALIDATOR_RELATIVE_PATH = (
    "artifacts/pnc_rca_predecessor_validator"
)


def _outcome_slo(*, healthy: bool = True, observed_at: str = OBSERVED_AT) -> dict:
    consecutive_failures = (
        0 if healthy else DELIVERY_OUTCOME_CONSECUTIVE_FAILURE_THRESHOLD
    )
    return {
        "schema_version": DELIVERY_OUTCOME_SLO_SCHEMA_VERSION,
        "observed_at": observed_at,
        "success_delivery_statuses": ["delivered", "partial"],
        "failure_delivery_statuses": ["quarantined"],
        "windows": {
            name: {
                "window_seconds": window_seconds,
                "min_samples": min_samples,
                "max_failure_rate": max_failure_rate,
                "sample_count": 0,
                "failure_count": 0,
                "failure_rate": 0.0,
                "breached": False,
            }
            for name, window_seconds, min_samples, max_failure_rate in (
                DELIVERY_OUTCOME_SLO_WINDOWS
            )
        },
        "consecutive_failure_window_seconds": (
            DELIVERY_OUTCOME_CONSECUTIVE_WINDOW_SECONDS
        ),
        "consecutive_failure_threshold": (
            DELIVERY_OUTCOME_CONSECUTIVE_FAILURE_THRESHOLD
        ),
        "consecutive_failure_count": consecutive_failures,
        "consecutive_failure_breached": not healthy,
        "contract_valid": True,
        "healthy": healthy,
    }


BEGIN = (
    "# === RCA_REQUEST_CONTRACT:BEGIN "
    "(do not edit between markers without updating host copy) ==="
)
END = "# === RCA_REQUEST_CONTRACT:END ==="
EVENT_UID = f"{TOPIC}:0:10"
CANARY_ADMISSION = build_rca_admission(
    project_key="t03o4q",
    project_simple_name="g1q3",
    work_item_type_key="issue",
    work_item_id="7041712812",
    rule_version=RULE,
    topic=TOPIC,
    partition=0,
    offset=10,
)
MANUAL_SUCCESS_CANARY_ADMISSION = build_rca_admission(
    project_key="t03o4q",
    project_simple_name="g1q3",
    work_item_type_key="issue",
    work_item_id="7101",
    rule_version=RULE,
    trigger_kind="manual_issue_request",
)
MANUAL_TERMINAL_CANARY_ADMISSION = build_rca_admission(
    project_key="t03o4q",
    project_simple_name="g1q3",
    work_item_type_key="issue",
    work_item_id="7102",
    rule_version=RULE,
    trigger_kind="manual_issue_request",
)
ACTIVATION_SLOT_IDENTITIES = {
    "kafka_success": {"event_uid": EVENT_UID},
    "manual_success": {
        "chat_id": release_gate_module.G1Q3_RCA_GROUP_ID,
        "requester_id": "ou_activation_success",
        "message_id": "om_activation_success",
        "thread_id": "topic:om_activation_success",
        "issue_url": "https://project.feishu.cn/g1q3/issue/detail/7101",
        "mode": "run_or_join",
    },
    "manual_terminal_failure": {
        "chat_id": release_gate_module.G1Q3_RCA_GROUP_ID,
        "requester_id": "ou_activation_terminal",
        "message_id": "om_activation_terminal",
        "thread_id": "topic:om_activation_terminal",
        "issue_url": "https://project.feishu.cn/g1q3/issue/detail/7102",
        "mode": "run_or_join",
    },
}
ACTIVATION_SLOT_ADMISSIONS = {
    "kafka_success": CANARY_ADMISSION.to_dict(),
    "manual_success": MANUAL_SUCCESS_CANARY_ADMISSION.to_dict(),
    "manual_terminal_failure": MANUAL_TERMINAL_CANARY_ADMISSION.to_dict(),
}
SUBMISSION_KEY = CANARY_ADMISSION.submission_key
SOURCE_ID = release_gate_module._stable_trigger_source_id(
    "kafka_workflow_event", EVENT_UID
)
WORKER_RUN_ID = "worker-run-20260710-075659"
INDEX_SHA256 = "b" * 64
REPORT_DATA_SHA256 = "c" * 64
DELIVERY_MANIFEST = {
    "schema_version": "delivery_manifest_v1",
    "sealed": True,
    "submission_key": SUBMISSION_KEY,
    "business_key": CANARY_ADMISSION.business_key,
    "generation": CANARY_ADMISSION.generation,
    "project_key": CANARY_ADMISSION.source_refs.project_key,
    "work_item_type_key": CANARY_ADMISSION.source_refs.work_item_type_key,
    "work_item_id": CANARY_ADMISSION.source_refs.work_item_id,
    "artifact_revision": 1,
    "sealed_at": "2026-07-10T07:57:00+00:00",
    "deliverable_kind": "html",
    "dependencies_complete": True,
    "artifact_root": f"/mnt/tmp/{SUBMISSION_KEY}/",
    "html_validation": {
        "state": "html_delivery_ready",
        "report_data_sha256": REPORT_DATA_SHA256,
        "blockers": [],
        "fidelity_ok": True,
    },
    "artifacts": [
        {
            "role": "index_html",
            "path": "index.html",
            "size": 2048,
            "sha256": INDEX_SHA256,
            "media_type": "text/html; charset=utf-8",
            "required": True,
        },
        {
            "role": "report_data",
            "path": "report_data.json",
            "size": 4096,
            "sha256": REPORT_DATA_SHA256,
            "media_type": "application/json",
            "required": True,
        },
    ],
}
ARTIFACT_SET_ID = release_gate_module.compute_artifact_set_id(DELIVERY_MANIFEST)
REPORT_URL = (
    "http://192.168.26.174:18081/G1Q3_RCA/cases/"
    f"{SUBMISSION_KEY}/{ARTIFACT_SET_ID}/index.html"
)
DELIVERY_MANIFEST = {
    **DELIVERY_MANIFEST,
    "artifact_set_id": ARTIFACT_SET_ID,
    "report_url": REPORT_URL,
}
DELIVERY_MANIFEST_RAW = (
    json.dumps(
        DELIVERY_MANIFEST,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    + "\n"
).encode("utf-8")
MANIFEST_SHA256 = hashlib.sha256(DELIVERY_MANIFEST_RAW).hexdigest()
EFFECT_KEY = "rca-delivery-effect-key"
LAUNCHD_CONFIG_SHA256 = "d" * 64
SANITIZED_WHEEL_SHA256 = (
    "3dcb5fef4962c1aae3302287d8b1d1c5d5a9a287c5f16b0b95ddbcad63c80a3c"
)
LIVE_PROCESS_VERIFIER = release_gate_module._verify_delivery_service_process
LIVE_LOADED_RUNTIME_VERIFIER = (
    release_gate_module._verify_resident_loaded_runtime_projection
)
LIVE_GATEWAY_RUNTIME_VERIFIER = release_gate_module._verify_gateway_runtime_identity
LIVE_BOOTSTRAP_RUNTIME_COLLECTOR = (
    release_gate_module._collect_activation_bootstrap_runtime
)
LIVE_CAPSULE_GATEWAY_RECHECK = release_gate_module._recheck_capsule_gateway_binding
LIVE_CONFIRMATION_RUNTIME_RECHECK = (
    release_gate_module._recheck_confirmation_runtime_continuity
)
LIVE_CANARY_DATABASE_BINDING = (
    release_gate_module._check_live_canary_database_binding
)


@pytest.fixture(autouse=True)
def _installed_runtime_dependencies(monkeypatch):
    """Keep evidence tests hermetic; probe behavior has focused tests below."""

    from scripts import pnc_rca_store_migration_drill as migration_module

    monkeypatch.setattr(
        migration_module,
        "_candidate_provenance",
        lambda repo_root: {
            "repo_root": str(Path(repo_root).resolve()),
            "commit": _git(Path(repo_root), "rev-parse", "HEAD"),
            "migration_sources": {
                relative: hashlib.sha256(
                    (Path(repo_root) / relative).read_bytes()
                ).hexdigest()
                for relative in migration_module.MIGRATION_SOURCE_RELATIVE_PATHS
            },
        },
    )
    monkeypatch.setattr(
        release_gate_module,
        "APPROVED_SANITIZED_WHEEL_SHA256",
        SANITIZED_WHEEL_SHA256,
    )
    def fake_live_database_binding(**values):
        root = Path(values["control_db_path"]).parent
        health_files = {
            "local.pnc.rca-kafka-consumer": "consumer-health.json",
            "local.pnc.rca-outbox-dispatcher": "dispatcher-health.json",
            "local.pnc.rca-delivery-collector": "delivery-collector-health.json",
            "local.pnc.rca-delivery-dispatcher": "delivery-dispatcher-health.json",
        }
        transitions = []
        for index, (label, filename) in enumerate(health_files.items()):
            health_path = root / filename
            if health_path.is_file():
                identity = json.loads(health_path.read_text(encoding="utf-8"))[
                    "runtime_identity"
                ]
            else:
                identity = {
                    "service_label": label,
                    "pid": 40999 + index,
                    "process_create_time": 1_783_649_999.0 + index,
                    "boot_time": 1_783_000_000.0,
                    "executable": "/candidate/python",
                    "script": f"/candidate/{filename}.py",
                    "cwd": "/candidate",
                    "script_sha256": "1" * 64,
                    "runtime_files_sha256": "2" * 64,
                    "public_config_sha256": "3" * 64,
                    "loaded_runtime_sha256": "e" * 64,
                }
            transitions.append({
                "submission_key": str(values["receipt"].get("submission_key") or "s"),
                "business_key": str(values["receipt"].get("business_key") or "b"),
                "generation": int(values["receipt"].get("generation") or 1),
                "service_label": label,
                "transition_kind": (
                    "kafka_ingested"
                    if index == 0
                    else "outbox_completed"
                    if index == 1
                    else "delivery_created"
                    if index == 2
                    else "effect_succeeded"
                ),
                "entity_key": f"fixture-{index}",
                "runtime_identity": identity,
                "runtime_identity_sha256": _sha256_json(identity),
                "transitioned_at": OBSERVED_AT,
            })
        return {
            "projection_sha256": _sha256_json({
                "receipt": values["receipt"],
                "terminal_failure": values["terminal_failure"],
            }),
            "control_snapshot_sha256": values["control_snapshot_sha256"],
            "delivery_snapshot_sha256": values["delivery_snapshot_sha256"],
            "host_runtime_transitions": transitions,
            "host_runtime_transitions_sha256": _sha256_json(transitions),
        }

    monkeypatch.setattr(
        release_gate_module,
        "_check_live_canary_database_binding",
        fake_live_database_binding,
    )
    monkeypatch.setattr(
        release_gate_module,
        "_verify_resident_loaded_runtime_projection",
        lambda value, **_kwargs: (dict(value), "e" * 64),
    )

    def candidate_runtime(repo_root, *, runtime_root=None):
        del runtime_root
        root = Path(repo_root)
        task_root = root.parent
        if root.name == "unused-host-build":
            mode = "shadow"
        elif (task_root / "evidence" / "bootstrap-capacity.fixture").is_file():
            mode = "canary_bootstrap"
        elif (task_root / "evidence" / "activation_preauthorization.json").is_file():
            mode = "preauthorization"
        elif (task_root / "evidence" / "activation_preproduction.json").is_file():
            mode = "preproduction"
        elif (task_root / "evidence" / "canary_receipt_commit.json").is_file():
            mode = "production"
        else:
            mode = "canary"
        consumer, dispatcher = _configs(task_root, mode)
        enabled = mode in {
            "preauthorization",
            "preproduction",
            "canary_bootstrap",
            "canary",
            "production_bootstrap",
            "production",
        }
        activation_required = enabled
        control_db = str(task_root / "control.sqlite3")
        consumer_health = task_root / "consumer-health.json"
        outbox_health = task_root / "dispatcher-health.json"
        collector_health = task_root / "delivery-collector-health.json"
        delivery_health = task_root / "delivery-dispatcher-health.json"
        service_configs = {
            "local.pnc.rca-kafka-consumer": {
                **consumer.public_dict(),
                "policy": consumer.policy.to_dict(),
            },
            "local.pnc.rca-outbox-dispatcher": dispatcher.public_dict(),
            "local.pnc.rca-delivery-collector": {
                "enabled": enabled,
                "activation_required": activation_required,
                "capacity_sample_enabled": enabled,
                "control_db_path": control_db,
                "health_path": str(collector_health),
                "lease_seconds": 180,
                "artifact_read_timeout_seconds": 25,
                "health_max_age_seconds": 60,
                "batch_size": 20,
                "backfill_batch_size": 1000,
                "external_writes": False,
            },
            "local.pnc.rca-delivery-dispatcher": {
                "enabled": enabled,
                "activation_required": activation_required,
                "control_db_path": control_db,
                "health_path": str(delivery_health),
                "lease_seconds": 120,
                "max_external_boundary_timeout_seconds": 72,
                "lease_boundary_margin_seconds": 15,
                "effect_lease_keeper_enabled": True,
                "effect_lease_renew_interval_seconds": 10,
                "health_max_age_seconds": 60,
                "batch_size": 10,
                "external_writes": enabled,
            },
        }
        runtime_file_sha256 = {
            relative: hashlib.sha256(relative.encode("utf-8")).hexdigest()
            for relative in release_gate_module.DELIVERY_RUNTIME_CRITICAL_FILES
        }
        runtime_files_sha256 = _sha256_json(runtime_file_sha256)
        process_specs = {
            label: {
                "service_label": label,
                "interpreter": "/candidate/.venv/bin/python",
                "runtime_executable": "/candidate/.venv/bin/python",
                "script": f"/candidate/scripts/{script_name}",
                "working_directory": "/candidate",
                "script_sha256": hashlib.sha256(
                    script_name.encode("utf-8")
                ).hexdigest(),
                "runtime_file_sha256": dict(runtime_file_sha256),
                "runtime_files_sha256": runtime_files_sha256,
                "loaded_runtime": {"fixture": "sealed"},
                "loaded_runtime_sha256": "e" * 64,
                "program_arguments": [
                    "/candidate/.venv/bin/python",
                    f"/candidate/scripts/{script_name}",
                ],
                "plist_path": f"/candidate/{filename}",
                "plist_sha256": hashlib.sha256(filename.encode("utf-8")).hexdigest(),
            }
            for filename, (
                label,
                script_name,
            ) in release_gate_module.CANDIDATE_SERVICES.items()
        }
        if enabled:

            def runtime_identity(label, pid, created_at):
                process = process_specs[label]
                return {
                    "service_label": label,
                    "pid": pid,
                    "process_create_time": created_at,
                    "boot_time": 1_783_000_000.0,
                    "executable": process["runtime_executable"],
                    "script": process["script"],
                    "cwd": process["working_directory"],
                    "script_sha256": process["script_sha256"],
                    "runtime_files_sha256": process["runtime_files_sha256"],
                    "public_config_sha256": _sha256_json(service_configs[label]),
                    "loaded_runtime_sha256": process["loaded_runtime_sha256"],
                }

            control_store_health = {
                "ok": True,
                "process_healthy": True,
                "business_ready": True,
                "schema_version": "pnc_rca_control_store_v10",
                "db_path": control_db,
                "delivery_outcome_slo": _outcome_slo(),
                "delivery_dispatcher_circuits": {
                    "feishu_issue_comment": {"state": "closed"},
                    "feishu_thread_reply": {"state": "closed"},
                },
            }
            delivery_store_health = {
                **control_store_health,
                "schema_version": "pnc_rca_delivery_store_v6",
            }
            _write_json(
                consumer_health,
                {
                    "schema_version": "pnc_rca_kafka_consumer_health_v2",
                    "ok": True,
                    "healthy": True,
                    "enabled": True,
                    "state": "running",
                    "mode": "outbox_pending",
                    "activation_required": activation_required,
                    "stats": {
                        "activation_deferred": 0,
                        "activation_resumed": 0,
                        "blocked_partitions": 0,
                    },
                    "heartbeat_at": OBSERVED_AT,
                    "runtime_identity": runtime_identity(
                        "local.pnc.rca-kafka-consumer", 40999, 1_783_649_999.0
                    ),
                    "config": service_configs["local.pnc.rca-kafka-consumer"],
                    "store": control_store_health,
                    "assignment": {
                        "assigned_partitions": [0],
                        "callback_errors": 0,
                    },
                },
            )
            _write_json(
                outbox_health,
                {
                    "schema_version": "pnc_rca_outbox_dispatcher_health_v2",
                    "ok": True,
                    "healthy": True,
                    "enabled": True,
                    "state": "idle",
                    "heartbeat_at": OBSERVED_AT,
                    "readiness_observed_at": OBSERVED_AT,
                    "readiness": {
                        "state": "idle",
                        "healthy": True,
                        "ready_for_dispatch": True,
                        "observed_at": OBSERVED_AT,
                    },
                    "liveness": {
                        "state": "reporting",
                        "heartbeat_at": OBSERVED_AT,
                        "readiness_observed_at": OBSERVED_AT,
                    },
                    "runtime_identity": runtime_identity(
                        "local.pnc.rca-outbox-dispatcher", 41000, 1_783_650_000.0
                    ),
                    "capacity_admission": (
                        {
                            "required": True,
                            "ready": True,
                            "state": "ready",
                            "error_code": "",
                            "capacity_mode": "bootstrap",
                            "authorization": {
                                "bootstrap_epoch_id": (
                                    "rca-bootstrap-release-20260710"
                                ),
                                "started_at": (
                                    NOW - timedelta(days=1)
                                ).isoformat(),
                                "deadline": (NOW + timedelta(days=7)).isoformat(),
                                "receipt_fingerprint": "d" * 64,
                                "authorization_receipt_sha256": "e" * 64,
                                "active_release_binding_sha256": "9" * 64,
                                "candidate_env_sha256": "8" * 64,
                                "release_bom_sha256": "f" * 64,
                                "release_approval_id": (
                                    "release-approval-20260710"
                                ),
                                "approval_evidence_sha256": "a" * 64,
                            },
                        }
                        if dispatcher.capacity_mode == "bootstrap"
                        else {
                            "required": False,
                            "ready": True,
                            "state": "steady",
                            "error_code": "",
                            "capacity_mode": "steady",
                            "authorization": None,
                        }
                    ),
                    "config": service_configs["local.pnc.rca-outbox-dispatcher"],
                    "store": control_store_health,
                    "delivery_backpressure": {
                        "enabled": True,
                        "active": False,
                        "high_watermark": service_configs[
                            "local.pnc.rca-outbox-dispatcher"
                        ]["delivery_high_watermark"],
                        "resume_watermark": service_configs[
                            "local.pnc.rca-outbox-dispatcher"
                        ]["delivery_resume_watermark"],
                        "last_snapshot": {
                            "schema_version": (
                                DELIVERY_BACKPRESSURE_SNAPSHOT_SCHEMA_VERSION
                            ),
                            "observed_at": OBSERVED_AT,
                            "db_path": service_configs[
                                "local.pnc.rca-outbox-dispatcher"
                            ]["delivery_db_path"],
                            "effect_counts": {
                                "pending": 0,
                                "claimed": 0,
                                "retry_wait": 0,
                                "uncertain": 0,
                            },
                            "unresolved_effects": 0,
                            "pipeline_counts": {
                                "untracked_completed_submissions": 0,
                                "pending_watches": 0,
                                "running_watches": 0,
                            },
                            "unresolved_work": 0,
                            "delivery_outcome_slo": _outcome_slo(),
                            "delivery_dispatcher_circuit": {
                                "state": "closed",
                                "reason_code": "",
                                "reason_detail": "",
                                "opened_at": None,
                                "updated_at": OBSERVED_AT,
                            },
                            "delivery_dispatcher_circuits": {
                                effect_kind: {
                                    "state": "closed",
                                    "reason_code": "",
                                    "reason_detail": "",
                                    "opened_at": None,
                                    "updated_at": OBSERVED_AT,
                                }
                                for effect_kind in (
                                    "feishu_issue_comment",
                                    "feishu_thread_reply",
                                )
                            },
                        },
                        "last_error": None,
                    },
                },
            )
            _write_json(
                collector_health,
                {
                    "schema_version": "pnc_rca_delivery_collector_health_v2",
                    "healthy": True,
                    "enabled": True,
                    "state": "idle",
                    "updated_at": OBSERVED_AT,
                    "runtime_identity": {
                        "service_label": "local.pnc.rca-delivery-collector",
                        "pid": 41001,
                        "process_create_time": 1_783_650_000.0,
                        "boot_time": 1_783_000_000.0,
                        "executable": process_specs["local.pnc.rca-delivery-collector"][
                            "runtime_executable"
                        ],
                        "script": process_specs["local.pnc.rca-delivery-collector"][
                            "script"
                        ],
                        "cwd": process_specs["local.pnc.rca-delivery-collector"][
                            "working_directory"
                        ],
                        "script_sha256": process_specs[
                            "local.pnc.rca-delivery-collector"
                        ]["script_sha256"],
                        "runtime_files_sha256": process_specs[
                            "local.pnc.rca-delivery-collector"
                        ]["runtime_files_sha256"],
                        "public_config_sha256": _sha256_json(
                            service_configs["local.pnc.rca-delivery-collector"]
                        ),
                        "loaded_runtime_sha256": process_specs[
                            "local.pnc.rca-delivery-collector"
                        ]["loaded_runtime_sha256"],
                    },
                    "dependencies": {
                        "remote_css_parser": {
                            **release_gate_module.EXPECTED_REMOTE_CSS_RUNTIME_DEPENDENCY,
                            "observed_at": OBSERVED_AT,
                        }
                    },
                    "store": delivery_store_health,
                },
            )
            _write_json(
                delivery_health,
                {
                    "schema_version": "pnc_rca_delivery_dispatcher_health_v2",
                    "healthy": True,
                    "state": "idle",
                    "updated_at": OBSERVED_AT,
                    "runtime_identity": {
                        "service_label": "local.pnc.rca-delivery-dispatcher",
                        "pid": 41002,
                        "process_create_time": 1_783_650_001.0,
                        "boot_time": 1_783_000_000.0,
                        "executable": process_specs[
                            "local.pnc.rca-delivery-dispatcher"
                        ]["runtime_executable"],
                        "script": process_specs["local.pnc.rca-delivery-dispatcher"][
                            "script"
                        ],
                        "cwd": process_specs["local.pnc.rca-delivery-dispatcher"][
                            "working_directory"
                        ],
                        "script_sha256": process_specs[
                            "local.pnc.rca-delivery-dispatcher"
                        ]["script_sha256"],
                        "runtime_files_sha256": process_specs[
                            "local.pnc.rca-delivery-dispatcher"
                        ]["runtime_files_sha256"],
                        "public_config_sha256": _sha256_json(
                            service_configs["local.pnc.rca-delivery-dispatcher"]
                        ),
                        "loaded_runtime_sha256": process_specs[
                            "local.pnc.rca-delivery-dispatcher"
                        ]["loaded_runtime_sha256"],
                    },
                    "config": {"enabled": True},
                    "store": delivery_store_health,
                },
            )
        return {
            "python_executable": "/candidate/.venv/bin/python",
            "dependency_versions": {
                "kafka-python": "3.0.7",
                "lark-oapi": "1.5.3",
                "python-snappy": "0.7.3",
                "tinycss2": "1.2.1",
                "psutil": "7.2.2",
                "python-dotenv": "1.2.2",
            },
            "loaded_runtime": {"fixture": "sealed"},
            "loaded_runtime_sha256": "e" * 64,
            "module_imports": {
                "kafka": "ok",
                "snappy": "ok",
                "tinycss2": "ok",
            },
            "launchd_config_sha256": LAUNCHD_CONFIG_SHA256,
            "service_configs": service_configs,
            "service_processes": process_specs,
            "service_dependencies": {
                "local.pnc.rca-delivery-collector": {
                    "remote_css_parser": dict(
                        release_gate_module.EXPECTED_REMOTE_CSS_RUNTIME_DEPENDENCY
                    )
                },
                "local.pnc.rca-delivery-dispatcher": {
                    "feishu_outbound": {
                        "schema_version": (
                            release_gate_module.FEISHU_OUTBOUND_RUNTIME_SCHEMA_VERSION
                        ),
                        "distribution": "lark-oapi",
                        "version": "1.5.3",
                        "python_executable": process_specs[
                            "local.pnc.rca-delivery-dispatcher"
                        ]["runtime_executable"],
                        "dependency_install_attempted": False,
                        "client_constructed": True,
                        "apis": {
                            name: True
                            for name in release_gate_module.EXPECTED_FEISHU_OUTBOUND_APIS
                        },
                    }
                },
            },
        }

    monkeypatch.setattr(
        release_gate_module,
        "check_candidate_runtime_dependencies",
        candidate_runtime,
    )
    monkeypatch.setattr(
        release_gate_module,
        "_verify_delivery_service_process",
        lambda identity, _candidate, *, artifact: {
            "pid": identity["pid"],
            "verified_by": "test_fixture",
            "artifact": artifact,
        },
    )
    def gateway_runtime(identity, *, repo_root, cutover, **_kwargs):
        runtime_file_sha256, runtime_files_sha256 = (
            release_gate_module.runtime_file_snapshot(
                repo_root,
                release_gate_module.GATEWAY_RCA_RUNTIME_RELATIVE_FILES,
            )
        )
        return {
            "service_label": "ai.hermes.gateway",
            "runtime_identity_sha256": _sha256_json(identity),
            "runtime_file_sha256": runtime_file_sha256,
            "runtime_files_sha256": runtime_files_sha256,
            "public_config_sha256": identity["public_config_sha256"],
            "loaded_runtime_sha256": identity["loaded_runtime_sha256"],
            "plist_path": "/fixture/ai.hermes.gateway.plist",
            "plist_sha256": "f" * 64,
            "interpreter_sha256": "1" * 64,
            "process_executable_sha256": "2" * 64,
            "module_origins_sha256": "3" * 64,
            "dependency_files_sha256": "4" * 64,
            "process": {
                "pid": identity["pid"],
                "process_create_time": identity["process_create_time"],
                "verified_by": "test_fixture",
                "launchctl": {
                    "state": "running",
                    "pid": identity["pid"],
                    "plist_path_sha256": "4" * 64,
                    "program_arguments_sha256": "5" * 64,
                    "working_directory_sha256": "6" * 64,
                    "environment_sha256": "7" * 64,
                },
            },
        }

    monkeypatch.setattr(
        release_gate_module,
        "_verify_gateway_runtime_identity",
        gateway_runtime,
    )

    def bootstrap_runtime(
        *, repo_root, cutover, consumer, require_rca_residents_stopped
    ):
        identity = {
            "service_label": "ai.hermes.gateway",
            "pid": 40990,
            "process_create_time": 1_783_649_990.0,
            "boot_time": 1_783_000_000.0,
            "executable": "/candidate/.venv/bin/python",
            "script": str(Path(repo_root).absolute() / "gateway" / "run.py"),
            "cwd": str(Path(repo_root).absolute()),
            "script_sha256": "1" * 64,
            "runtime_files_sha256": "2" * 64,
            "public_config_sha256": _sha256_json(
                release_gate_module._gateway_manual_runtime_public_config(
                    cutover, consumer
                )
            ),
            "loaded_runtime_sha256": "3" * 64,
        }
        verified = {
            "runtime_identity_sha256": _sha256_json(identity),
            "public_config_sha256": identity["public_config_sha256"],
            "runtime_files_sha256": identity["runtime_files_sha256"],
        }
        residents = {
            label: {
                "launchd_state": "absent",
                "matching_pids": [],
                "state": "stopped",
            }
            for label in release_gate_module.RCA_RESIDENT_LABELS
        } if require_rca_residents_stopped else {}
        return {
            "rca_residents": residents,
            "rca_residents_required_stopped": require_rca_residents_stopped,
            "gateway": {
                "state": "running_safe",
                "runtime_identity": identity,
                "verified": verified,
            },
        }

    monkeypatch.setattr(
        release_gate_module,
        "_collect_activation_bootstrap_runtime",
        bootstrap_runtime,
    )
    monkeypatch.setattr(
        release_gate_module,
        "_recheck_capsule_gateway_binding",
        lambda _binding, **_kwargs: None,
    )
    monkeypatch.setattr(
        release_gate_module,
        "_recheck_confirmation_runtime_continuity",
        lambda _binding, **_kwargs: None,
    )


@pytest.fixture(autouse=True)
def _local_build_provenance(monkeypatch, tmp_path):
    """Use local Git repositories for gate tests; never contact the real VM."""

    ssh_mini_agent = tmp_path / "external" / ".local" / "bin" / "ssh-mini-agent"
    protocol = (
        tmp_path
        / "external"
        / ".ssh-mini"
        / "VM_SSH_EXECUTION_PROTOCOL_V2.md"
    )
    ssh_mini_agent.parent.mkdir(parents=True)
    protocol.parent.mkdir(parents=True)
    ssh_mini_agent.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    protocol.write_text("# VM SSH Execution Protocol v2\n", encoding="utf-8")
    ssh_mini_agent.chmod(0o700)
    protocol.chmod(0o644)
    monkeypatch.setattr(release_gate_module, "DEFAULT_SSH_MINI_AGENT", ssh_mini_agent)
    monkeypatch.setattr(
        release_gate_module,
        "CANONICAL_VM_SSH_EXECUTION_PROTOCOL",
        protocol,
    )
    monkeypatch.setattr(
        release_gate_module,
        "EXTERNAL_RELEASE_DEPENDENCIES",
        {
            "ssh_mini_agent": {"path": ssh_mini_agent, "mode": 0o700},
            "vm_ssh_execution_protocol_v2": {"path": protocol, "mode": 0o644},
        },
    )

    def verifier(settings):
        vm = release_gate_module._local_git_provenance(
            Path(settings.vm_repo_root),
            component="vm",
            entrypoint_relative=("api/g1q3_rca/scripts/run_rca_service_request.py"),
        )
        vm["source"] = "ssh-mini-agent"
        vm_worker = release_gate_module._local_git_provenance(
            Path(settings.vm_worker_repo_root),
            component="vm_worker",
            entrypoint_relative="vm_coding_worker_v2.py",
        )
        vm_worker["source"] = "ssh-mini-agent"
        return {
            "schema_version": BUILD_PROVENANCE_SCHEMA_VERSION,
            "host": release_gate_module._local_git_provenance(
                settings.host_repo_root,
                component="host",
            ),
            "workspace": release_gate_module._local_git_provenance(
                settings.workspace_repo_root,
                component="workspace",
            ),
            "vm": vm,
            "vm_worker": vm_worker,
            "external_dependencies": (
                release_gate_module._external_release_dependency_provenance()
            ),
        }

    monkeypatch.setattr(
        release_gate_module,
        "verify_live_build_provenance",
        verifier,
    )
    monkeypatch.setattr(
        release_gate_module,
        "_revalidate_workspace_runtime_release_binding",
        lambda _binding: None,
    )
    monkeypatch.setattr(
        release_gate_module,
        "_revalidate_future_runtime_release_binding",
        lambda _binding: None,
    )
    monkeypatch.setattr(
        release_gate_module,
        "_check_auxiliary_runtime_health",
        lambda **_kwargs: {
            "not_before": OBSERVED_AT,
            "completion_relay": {"healthy": True},
            "vm_task_sync": {"ok": True},
            "runtime_binding_sha256": "a" * 64,
        },
    )


@pytest.fixture(autouse=True)
def _local_remote_reader_probe(monkeypatch):
    """Mirror the declared fixture runtime while focused tests cover live probing."""

    def verifier(settings):
        health = json.loads(
            (settings.evidence_dir / "remote_reader_health.json").read_text(
                encoding="utf-8"
            )
        )
        runtime = health["runtime"]
        repo_root = str(settings.vm_repo_root)
        dependencies = {
            name.replace("pdcl_dss", "pdcl-dss"): value["installed_version"]
            for name, value in runtime["dependencies"].items()
        }
        checks = {
            name: "ok"
            for name in (
                "isolated_interpreter",
                "distribution",
                "vendored_wheel",
                "pdcl_pyclip/reader.py",
                "pdcl_pyclip/_config.py",
                "pdcl_pyclip/_storage.py",
                "dependency:mcap",
                "dependency:protobuf",
                "dependency:pdcl-dss",
                "dependency:typer",
                "module_import",
                "class:RemoteClipReader",
                "class:RemoteEventReader",
            )
        }
        manifest = {
            "schema_version": "g1q3_rca_sanitized_dependency_v1",
            "distribution": "pdcl_pyclip",
            "version": "0.1.6+rca.2",
            "wheel": "pdcl_pyclip-0.1.6+rca.2-py3-none-any.whl",
            "wheel_sha256": SANITIZED_WHEEL_SHA256,
            "upstream": {
                "repository": "pdcl/pdcl_pyclip",
                "commit": "62a84e39146800ed5d05a6d7c0866d6b06bf6437",
                "release_wheel_sha256": (
                    "e760a532dbe7dff730ef8e85b32e4ff33d14acefe7c8f295224bb77b08fcadae"
                ),
            },
            "source_commit": "62a84e39146800ed5d05a6d7c0866d6b06bf6437",
            "sanitized_source_commit": "7d0d028020dd140466a8a6f181f0e75bc142d2bc",
            "patch_sha256": (
                "659199da1a56c201e3f99c15893d56e36a6195d23666c78e3457121ee2fea3db"
            ),
            "sanitized_patch_commits": [
                "93d88ef",
                "99e7c30",
                "f32bf118e3197e6d5006c7837c3c0ad6f825d105",
                "7d0d028020dd140466a8a6f181f0e75bc142d2bc",
            ],
            "module_sha256": {
                "pdcl_pyclip/reader.py": (
                    "b4a98fca46ba7a71f4a0f1cc55b7549c8c37aa195f98d74aee4fd7bd0770acd6"
                ),
                "pdcl_pyclip/_config.py": (
                    "828c851a8d52fd9b5c50ea243d027ccd3870ffc3a383bf9d5fc10533fd43cebf"
                ),
                "pdcl_pyclip/_storage.py": (
                    "4c31ba2450269659db8f1475785136638349e556b9035b9ef9a76b318e56256a"
                ),
                "pdcl_pyclip/writer.py": (
                    "98ab59369c9f093a737dbaf8c66cd19dde6c569d6eaf8dd1def4c6e575df606d"
                ),
            },
            "runtime_policy": {
                "credentials": "env_only",
                "cache": "pre_mounted_read_only_admission",
                "automatic_mount": False,
                "package_install": False,
                "sudo": False,
                "input_materialization": False,
            },
        }
        return {
            "schema_version": "pnc_rca_remote_reader_live_probe_v1",
            "repo_root": repo_root,
            "git_commit": _git(Path(repo_root), "rev-parse", "HEAD"),
            "git_status_sha256": EMPTY_GIT_STATUS_SHA256,
            "python_executable": runtime["python_executable"],
            "module_path": runtime["module_path"],
            "wheel_path": str(
                Path(repo_root) / release_gate_module.REMOTE_READER_WHEEL_RELATIVE
            ),
            "manifest_path": str(
                Path(repo_root) / release_gate_module.REMOTE_READER_MANIFEST_RELATIVE
            ),
            "adapter_path": str(
                Path(repo_root) / release_gate_module.REMOTE_READER_ADAPTER_RELATIVE
            ),
            "wheel_sha256": SANITIZED_WHEEL_SHA256,
            "manifest_sha256": "1" * 64,
            "adapter_sha256": "2" * 64,
            "manifest": manifest,
            "doctor": {
                "status": "ready",
                "distribution": "pdcl_pyclip",
                "required_version": "0.1.6+rca.2",
                "actual_version": "0.1.6+rca.2",
                "sanitized_policy": "env_only_pre_mounted_cache_v1",
                "sanitized_patch_commit": "7d0d028",
                "sanitized_wheel_sha256": SANITIZED_WHEEL_SHA256,
                "runtime": {
                    key: runtime[key]
                    for key in (
                        "execution_mode",
                        "dependency_domain",
                        "required_python_executable",
                        "resolved_required_python_executable",
                        "python_executable",
                        "python_version",
                        "reader_import_scope",
                        "timeout_seconds",
                    )
                },
                "runtime_environment": dict(runtime["runtime_environment"]),
                "checks": checks,
                "dependency_versions": dependencies,
                "blocker_kind": "",
            },
        }

    monkeypatch.setattr(
        release_gate_module,
        "verify_live_remote_reader",
        verifier,
    )


def _consumer_env(tmp_path: Path, mode: str) -> dict[str, str]:
    return {
        "HERMES_RCA_KAFKA_BOOTSTRAP_SERVERS": "broker-1:9092,broker-2:9092",
        "HERMES_RCA_KAFKA_TOPIC": TOPIC,
        "HERMES_RCA_KAFKA_EXPECTED_CLUSTER_ID": "cluster-production-1",
        "HERMES_RCA_KAFKA_USER": "rca",
        "HERMES_RCA_KAFKA_PASSWORD": SECRET,
        "HERMES_RCA_KAFKA_GROUP": "rca_root_cause_analysis_agent",
        "HERMES_RCA_KAFKA_CLIENT_ID": "root_cause_analysis_agent",
        "HERMES_RCA_KAFKA_API_VERSION": "3.9.0",
        "HERMES_RCA_KAFKA_REQUEST_TIMEOUT_MS": "120000",
        "HERMES_RCA_KAFKA_MIN_REPLICATION_FACTOR": "2",
        "HERMES_RCA_KAFKA_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
        "HERMES_RCA_KAFKA_SASL_MECHANISM": "PLAIN",
        "HERMES_RCA_KAFKA_AUTO_OFFSET_RESET": "none",
        "HERMES_RCA_KAFKA_START_OFFSETS_JSON": '{"0": 10, "1": 20}',
        "HERMES_RCA_KAFKA_PROJECT_KEYS": "t03o4q",
        "HERMES_RCA_KAFKA_PROJECT_SIMPLE_NAMES": "g1q3",
        "HERMES_RCA_KAFKA_WORK_ITEM_TYPE_KEYS": "issue",
        "HERMES_RCA_KAFKA_STATUS_CHANGE_TYPES": "Reached",
        "HERMES_RCA_KAFKA_CREATION_RULE_VERSION": RULE,
        "HERMES_RCA_KAFKA_STATE_TRANSITIONS_JSON": json.dumps([
            {
                "state_key": "new-problem",
                "pre_status": 1,
                "cur_status": 2,
            }
        ]),
        "HERMES_RCA_KAFKA_CONTROL_DB_PATH": str(tmp_path / "control.sqlite3"),
        "HERMES_RCA_KAFKA_HEALTH_PATH": str(tmp_path / "consumer-health.json"),
        "HERMES_RCA_KAFKA_SUBMIT_ENABLED": str(mode != "shadow").lower(),
        "HERMES_RCA_KAFKA_ACTIVATION_REQUIRED": str(
            mode in {
                "preauthorization",
                "preproduction",
                "canary_bootstrap",
                "canary",
                "production_bootstrap",
                "production",
            }
        ).lower(),
    }


def _dispatcher_env(tmp_path: Path, mode: str) -> dict[str, str]:
    values = {
        "HERMES_RCA_PROD_CAPACITY_MODE": (
            "bootstrap"
            if mode in release_gate_module.BOOTSTRAP_CAPACITY_MODES
            else "steady"
        ),
        "HERMES_RCA_OUTBOX_DISPATCH_ENABLED": str(
            mode in {
                "preauthorization",
                "preproduction",
                "canary_bootstrap",
                "canary",
                "production_bootstrap",
                "production",
            }
        ).lower(),
        "HERMES_RCA_OUTBOX_ACTIVATION_REQUIRED": str(
            mode in {
                "preauthorization",
                "preproduction",
                "canary_bootstrap",
                "canary",
                "production_bootstrap",
                "production",
            }
        ).lower(),
        "HERMES_RCA_OUTBOX_SERVICE_ID": "root_cause_analysis_agent",
        "HERMES_RCA_OUTBOX_CONTROL_DB_PATH": str(tmp_path / "control.sqlite3"),
        "HERMES_RCA_OUTBOX_HEALTH_PATH": str(tmp_path / "dispatcher-health.json"),
        "HERMES_RCA_OUTBOX_BATCH_SIZE": "10",
        "HERMES_RCA_OUTBOX_DATA_ACCESS_MODE": "remote_read",
        "HERMES_RCA_OUTBOX_ALLOW_DOWNLOAD": "false",
        "HERMES_RCA_OUTBOX_ALLOW_FEISHU_WRITEBACK": "false",
        "HERMES_RCA_OUTBOX_STORAGE_ADMISSION_ENABLED": str(
            mode in {
                "preauthorization",
                "preproduction",
                "canary_bootstrap",
                "canary",
                "production_bootstrap",
                "production",
            }
        ).lower(),
        "HERMES_RCA_OUTBOX_STORAGE_RESERVATION_ENABLED": "false",
        "HERMES_RCA_OUTBOX_DERIVED_CAPACITY_RESERVATION_ENABLED": str(
            mode in {
                "preauthorization",
                "preproduction",
                "canary_bootstrap",
                "canary",
                "production_bootstrap",
                "production",
            }
        ).lower(),
        "HERMES_RCA_OUTBOX_DELIVERY_BACKPRESSURE_ENABLED": str(
            mode in {
                "preauthorization",
                "preproduction",
                "canary_bootstrap",
                "canary",
                "production_bootstrap",
                "production",
            }
        ).lower(),
        "HERMES_RCA_OUTBOX_STORAGE_CASES_PER_DAY": "200",
        "HERMES_RCA_OUTBOX_STORAGE_EXPECTED_ARTIFACT_CACHE_BYTES": "1000000000",
    }
    if mode in {
        "preauthorization",
        "preproduction",
        "canary_bootstrap",
        "canary",
        "production_bootstrap",
        "production",
    }:
        values.update({
            "HERMES_RCA_PROD_RELEASE_ID": "release-approval-20260710",
            "HERMES_RCA_PROD_BOOTSTRAP_EPOCH_ID": (
                "rca-bootstrap-release-20260710"
            ),
        })
    return values


def _cutover(mode: str) -> CutoverConfig:
    if mode in {
        "preauthorization",
        "preproduction",
        "canary_bootstrap",
        "canary",
        "production_bootstrap",
        "production",
    }:
        return CutoverConfig(
            True,
            0,
            False,
            True,
            True,
            manual_intake_enabled=mode in {
                "preauthorization", "preproduction", *release_gate_module.CANARY_MODES
            },
            manual_chat_ids=(
                (release_gate_module.G1Q3_RCA_GROUP_ID,)
                if mode in {"preauthorization", "preproduction", *release_gate_module.CANARY_MODES}
                else ()
            ),
            manual_operator_enabled=(
                False
                if mode in {"preauthorization", "preproduction", *release_gate_module.CANARY_MODES}
                else None
            ),
            activation_required=True,
        )
    return CutoverConfig(False, 200, True)


def _configs(tmp_path: Path, mode: str):
    consumer = ConsumerConfig.from_env(
        _consumer_env(tmp_path, mode), hermes_home=tmp_path
    )
    dispatcher = DispatcherConfig.from_env(
        _dispatcher_env(tmp_path, mode), hermes_home=tmp_path
    )
    return consumer, dispatcher


def _runtime_config_sha256(
    consumer: ConsumerConfig,
    dispatcher: DispatcherConfig,
    mode: str,
    *,
    cutover: CutoverConfig | None = None,
) -> str:
    consumer_public = consumer.public_dict()
    consumer_public["policy"] = consumer.policy.to_dict()
    return _sha256_json({
        "consumer": consumer_public,
        "dispatcher": dispatcher.public_dict(),
        "cutover": (cutover or _cutover(mode)).public_dict(),
    })


def _event() -> dict:
    return {
        "id": 7041712812,
        "name": "ACC braking issue",
        "nodes": [
            {
                "state_key": "new-problem",
                "node_name": "diagnostic label",
                "pre_status": 1,
                "cur_status": 2,
            }
        ],
        "project_key": "t03o4q",
        "project_simple_name": "g1q3",
        "status_change_type": "Reached",
        "updated_at": 1783650000000,
        "work_item_type_key": "issue",
    }


def _remote_execution_request() -> dict:
    request = build_execution_request(
        request_kind="issue_intake",
        task_id=SUBMISSION_KEY,
        issue_context=RcaIssueContext(
            project_key="t03o4q",
            work_item_type="issue",
            work_item_id="7041712812",
            url="https://project.feishu.cn/g1q3/issue/detail/7041712812",
            title="ACC braking issue",
            source_quality="full",
            pdcl_download_cmd=(
                "mdi download event -u c68103dd-0000-0000-0000-000000000001 -s ./"
            ),
        ),
        artifact_root=f"/mnt/tmp/{SUBMISSION_KEY}/",
        artifact_cifs_root=(
            "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/"
            f"{SUBMISSION_KEY}/"
        ),
        allow_download=False,
        allow_feishu_writeback=False,
    )
    request = replace(
        request,
        source_refs={
            "task_id": SUBMISSION_KEY,
            "source_kind": "kafka_workflow_event",
            "origin_source_id": SOURCE_ID,
            "source_event_id": EVENT_UID,
            "topic": TOPIC,
            "partition": 0,
            "offset": 10,
            "rule_version": RULE,
            "generation": CANARY_ADMISSION.generation,
            "business_key": CANARY_ADMISSION.business_key,
            "submission_key": SUBMISSION_KEY,
        },
    )
    return rca_to_dict(request)


def _kafka_trigger_source() -> dict:
    return {
        "source_id": SOURCE_ID,
        "source_kind": "kafka_issue_created",
        "storage_source_kind": "kafka_workflow_event",
        "source_dedupe_key": EVENT_UID,
        "payload_sha256": "a" * 64,
        "platform": "",
        "chat_id": "",
        "thread_id": "",
        "message_id": "",
        "requester_id": "",
        "kafka_event_uid": EVENT_UID,
        "mode": "issue_created",
        "outcome": "",
        "created_at": "2026-07-10T07:55:00+00:00",
        "binding_role": "origin",
        "bound_at": "2026-07-10T07:55:00+00:00",
        "business_key": CANARY_ADMISSION.business_key,
        "generation": 1,
        "authorization": None,
    }


def test_remote_execution_request_hash_uses_unicode_canonical_abi():
    request = _remote_execution_request()
    request["work_item"]["title"] = "中文问题标题"

    detail = release_gate_module._check_remote_execution_request(
        request,
        field="test.execution_request",
        expected_admission=CANARY_ADMISSION.to_dict(),
        expected_origin_source_id=SOURCE_ID,
        expected_origin_storage_kind="kafka_workflow_event",
    )

    assert detail["request_sha256"] == _sha256_execution_request(request)
    assert detail["request_sha256"] != _sha256_json(request)


def _requested_scope() -> dict:
    requirements = {
        "schema_version": "g1q3_rca_remote_evidence_requirements_v1",
        "requirements_contract_version": "g1q3_rca_evaluator_scope_v1",
        "requirements_contract_hash": "7" * 64,
        "function_domain": "ACC",
        "requested_topics": [
            "kvaser.0.can1.Vehicle",
            "vehicle_signal_highfreq",
        ],
        "channel_allowlist": [
            "kvaser.0.can1.Debug",
            "kvaser.0.can1.Vehicle",
            "kvaser.0.can2.Debug",
            "kvaser.1.can1.Debug",
            "kvaser.1.can1.Vehicle",
            "kvaser.1.can2.Debug",
            "sigmastar.1.dds2.vehicle_signal_highfreq",
            "sigmastar~1~dds2.vehicle_signal_highfreq",
        ],
        "frame_lookup": {},
        "frame_channel_allowlist": [],
        "requested_window": {
            "mode": "full_reference",
            "start_time_ns": None,
            "end_time_ns": None,
        },
        "evaluator_fingerprints": {
            "api/g1q3_rca/rca_evaluators/acc_longitudinal_semantic.py": "8" * 64,
            "api/g1q3_rca/report_builder.py": "9" * 64,
        },
    }
    requirements["requirements_hash"] = _sha256_json(requirements)
    return {
        "source": "vm_evaluator_scope_contract",
        "requirements": requirements,
    }


def _write_json(path: Path, body: dict) -> None:
    path.write_text(json.dumps(body, sort_keys=True), encoding="utf-8")
    path.chmod(0o600)


def _write_remote_soak_manifest(path: Path, body: dict) -> None:
    path.write_bytes(release_gate_module._remote_soak_canonical_json_bytes(body) + b"\n")
    path.chmod(0o600)


def _committed_pair_paths(
    evidence_dir: Path,
    evidence_role: str,
) -> tuple[Path, Path, Path]:
    spec = release_gate_module.COMMITTED_CANARY_EVIDENCE_SPECS[evidence_role]
    manifest_path = evidence_dir / spec.manifest_filename
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    return (
        manifest_path,
        evidence_dir / manifest["files"]["receipt"]["filename"],
        evidence_dir / manifest["files"]["sources"]["filename"],
    )


def _publish_committed_pair(
    evidence_dir: Path,
    evidence_role: str,
    *,
    receipt: dict | None = None,
    sources: dict | None = None,
    published_at: str = OBSERVED_AT,
) -> tuple[Path, Path, Path]:
    spec = release_gate_module.COMMITTED_CANARY_EVIDENCE_SPECS[evidence_role]
    legacy_receipt = evidence_dir / f"{spec.receipt_stem}.json"
    legacy_sources = evidence_dir / f"{spec.sources_stem}.json"
    manifest_path = evidence_dir / spec.manifest_filename
    if (
        receipt is None
        and sources is None
        and not legacy_receipt.is_file()
        and not legacy_sources.is_file()
        and manifest_path.is_file()
    ):
        return _committed_pair_paths(evidence_dir, evidence_role)
    if receipt is None:
        if legacy_receipt.is_file():
            receipt = json.loads(legacy_receipt.read_text(encoding="utf-8"))
        else:
            receipt = _read_committed_pair_body(
                evidence_dir,
                evidence_role,
                "receipt",
            )
    if sources is None:
        if legacy_sources.is_file():
            sources = json.loads(legacy_sources.read_text(encoding="utf-8"))
        else:
            sources = _read_committed_pair_body(
                evidence_dir,
                evidence_role,
                "sources",
            )
    receipt_raw = json.dumps(receipt, sort_keys=True).encode("utf-8")
    sources_raw = json.dumps(sources, sort_keys=True).encode("utf-8")
    receipt_canonical_sha256 = _sha256_json(receipt)
    files = {
        "receipt": {
            "schema_version": spec.receipt_schema_version,
            "size_bytes": len(receipt_raw),
            "raw_sha256": hashlib.sha256(receipt_raw).hexdigest(),
        },
        "sources": {
            "schema_version": spec.sources_schema_version,
            "size_bytes": len(sources_raw),
            "raw_sha256": hashlib.sha256(sources_raw).hexdigest(),
        },
    }
    commit_id = _sha256_json({
        "schema_version": release_gate_module.CANARY_EVIDENCE_COMMIT_SCHEMA_VERSION,
        "evidence_role": evidence_role,
        "receipt_canonical_sha256": receipt_canonical_sha256,
        "files": files,
    })
    receipt_path = evidence_dir / f"{spec.receipt_stem}.{commit_id}.json"
    sources_path = evidence_dir / f"{spec.sources_stem}.{commit_id}.json"
    receipt_path.write_bytes(receipt_raw)
    sources_path.write_bytes(sources_raw)
    receipt_path.chmod(0o600)
    sources_path.chmod(0o600)
    _write_json(
        manifest_path,
        {
            "schema_version": (
                release_gate_module.CANARY_EVIDENCE_COMMIT_SCHEMA_VERSION
            ),
            "evidence_role": evidence_role,
            "commit_id": commit_id,
            "published_at": published_at,
            "receipt_canonical_sha256": receipt_canonical_sha256,
            "files": {
                "receipt": {
                    "filename": receipt_path.name,
                    **files["receipt"],
                },
                "sources": {
                    "filename": sources_path.name,
                    **files["sources"],
                },
            },
        },
    )
    manifest_path.chmod(0o600)
    for legacy in (legacy_receipt, legacy_sources):
        if legacy.exists() and legacy not in {receipt_path, sources_path}:
            legacy.unlink()
    return manifest_path, receipt_path, sources_path


def _rewrite_committed_pair_body(
    evidence_dir: Path,
    evidence_role: str,
    kind: str,
    body: dict,
) -> tuple[Path, Path, Path]:
    _manifest, receipt_path, sources_path = _committed_pair_paths(
        evidence_dir,
        evidence_role,
    )
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    sources = json.loads(sources_path.read_text(encoding="utf-8"))
    if kind == "receipt":
        receipt = body
    elif kind == "sources":
        sources = body
    else:
        raise ValueError(f"unknown committed pair kind: {kind}")
    return _publish_committed_pair(
        evidence_dir,
        evidence_role,
        receipt=receipt,
        sources=sources,
    )


def _read_committed_pair_body(
    evidence_dir: Path,
    evidence_role: str,
    kind: str,
) -> dict:
    _manifest, receipt_path, sources_path = _committed_pair_paths(
        evidence_dir,
        evidence_role,
    )
    path = receipt_path if kind == "receipt" else sources_path
    return json.loads(path.read_text(encoding="utf-8"))


def _sha256_json(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _sha256_execution_request(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _cifs_storage_contract() -> dict:
    return {
        "storage_mode": "cifs_mount_fixed",
        "observed_file_mode": "0755",
        "requested_file_mode": "0600",
        "mode_enforced_by_mount": True,
        "credentials_present": False,
        "secret_scan_passed": True,
        "mount_evidence": {
            "mount_point": "/mnt/tmp",
            "mount_source": (
                "//hfs.minieye.tech/department-pnc_team-planning_algo-driving-tmp"
            ),
            "fstype": "cifs",
            "file_mode": "0755",
            "dir_mode": "0755",
            "rw": True,
            "device_id": 123456,
            "mount_namespace": "mnt:[4026531840]",
        },
    }


def _derived_reservation_request() -> DerivedCapacityReservationRequest:
    request = _remote_execution_request()
    return DerivedCapacityReservationRequest(
        submission_key=SUBMISSION_KEY,
        task_id=SUBMISSION_KEY,
        business_key=CANARY_ADMISSION.business_key,
        data_access_sha256=canonical_data_access_sha256(request["data"]["data_access"]),
        artifact_root=request["data"]["artifact_root"],
        expected_artifact_cache_bytes=1_000_000_000,
    )


def _derived_byte_totals(tmp: int, hfs: int) -> dict[str, int]:
    return {"tmp": tmp, "hfs": hfs, "total": tmp + hfs}


def _derived_full_receipt(status: str) -> dict:
    request = _derived_reservation_request()
    requested = request.requested_bytes
    reservation_id = "2d13a73f-a91c-4738-a3ae-98df25d23d2f"
    contract = request.contract()
    contract_sha256 = _sha256_json(contract)
    created_at = "2026-07-10T07:55:00+00:00"
    active_at = "2026-07-10T07:55:05+00:00"
    released_at = "2026-07-10T07:58:00+00:00"
    if status == "reserved":
        observed_at = "2026-07-10T07:55:01+00:00"
        updated_at = created_at
        run_id = ""
        activated_at = None
        terminal_at = None
        lease_expires_at = "2026-07-10T08:25:00+00:00"
        held = requested
        idempotent = False
        blocker = None
    elif status == "active":
        observed_at = active_at
        updated_at = active_at
        run_id = SUBMISSION_KEY
        activated_at = active_at
        terminal_at = None
        lease_expires_at = "2026-07-10T08:25:05+00:00"
        held = requested
        idempotent = True
        blocker = None
    elif status == "released":
        observed_at = released_at
        updated_at = released_at
        run_id = SUBMISSION_KEY
        activated_at = active_at
        terminal_at = released_at
        lease_expires_at = None
        held = _derived_byte_totals(0, 0)
        idempotent = True
        blocker = {
            "kind": "derived_capacity_reservation_released_reconcile_only",
            "retryable": False,
            "reconcile_only": True,
            "create_allowed": False,
        }
    else:
        raise AssertionError(status)
    total = _derived_byte_totals(40_000_000_000, 0)
    available = dict(total)
    reserve = _derived_byte_totals(12_000_000_000, 0)
    effective = _derived_byte_totals(28_000_000_000, 0)
    return {
        "schema_version": DERIVED_RESERVATION_RECEIPT_SCHEMA_VERSION,
        "request_schema_version": DERIVED_RESERVATION_REQUEST_SCHEMA_VERSION,
        "ok": status in {"reserved", "active"},
        "status": status,
        "reservation_id": reservation_id,
        "submission_key": SUBMISSION_KEY,
        "contract_sha256": contract_sha256,
        "fence": 1,
        "operation": "reserve",
        "idempotent": idempotent,
        "observed_at": observed_at,
        "contract": contract,
        "reservation": {
            "reservation_id": reservation_id,
            "submission_key": SUBMISSION_KEY,
            "contract_sha256": contract_sha256,
            "state": status,
            "fence": 1,
            "run_id": run_id,
            "requested_bytes": requested,
            "held_bytes": held,
            "created_at": created_at,
            "updated_at": updated_at,
            "lease_expires_at": lease_expires_at,
            "activated_at": activated_at,
            "released_at": terminal_at,
        },
        "capacity": {
            "scope": DERIVED_CAPACITY_SCOPE,
            "atomic_reservation": True,
            "observed_at": observed_at,
            "paths": {"tmp": DERIVED_TMP_PATH, "hfs": DERIVED_HFS_PATH},
            "reserve_ratio": "0.30",
            "required_bytes": requested,
            "total_bytes": total,
            "available_bytes": available,
            "reserve_bytes": reserve,
            "outstanding_held_bytes": _derived_byte_totals(0, 0),
            "effective_admittable_bytes": effective,
            "admitted": True,
            "blockers": [],
        },
        "blocker": blocker,
    }


def _safe_lifecycle_receipt(receipt: dict, operation: str) -> dict:
    reservation = receipt["reservation"]
    return {
        "schema_version": "g1q3_rca_capacity_lifecycle_receipt_v1",
        "operation": operation,
        "receipt_sha256": _sha256_json(receipt),
        "reservation_id": receipt["reservation_id"],
        "submission_key": receipt["submission_key"],
        "fence": receipt["fence"],
        "status": receipt["status"],
        "contract_sha256": receipt["contract_sha256"],
        "run_id": reservation["run_id"],
        "requested_bytes": reservation["requested_bytes"],
        "held_bytes": reservation["held_bytes"],
        "observed_at": receipt["observed_at"],
        "created_at": reservation["created_at"],
        "updated_at": reservation["updated_at"],
        "lease_expires_at": reservation["lease_expires_at"],
        "activated_at": reservation["activated_at"],
        "released_at": reservation["released_at"],
    }


def _derived_capacity_lifecycle() -> dict:
    reserved = _derived_full_receipt("reserved")
    active = _derived_full_receipt("active")
    released = _derived_full_receipt("released")
    return {
        "schema_version": "g1q3_rca_capacity_lifecycle_artifact_v2",
        "full_receipts": {
            "reserved": reserved,
            "activate": active,
            "release": released,
        },
        "reserved": _safe_lifecycle_receipt(reserved, "reserve_reference"),
        "activate": _safe_lifecycle_receipt(active, "activate"),
        "release": _safe_lifecycle_receipt(released, "release"),
        "audit": {
            "schema_version": "g1q3_rca_capacity_lifecycle_audit_v2",
            "transition": ["reserved", "active", "released"],
            "full_receipts_present": True,
            "full_receipts_hash_match": True,
            "reserved_receipt_referenced": True,
            "all_receipts_referenced": True,
            "same_reservation_fence_contract": True,
            "same_active_release_run_id": True,
            "same_reservation_fence_contract_run": True,
            "same_requested_bytes": True,
            "held_preserved_until_release": True,
            "timestamps_monotonic": True,
            "lease_closed": True,
            "released_held_bytes_zero": True,
            "terminal_state": "released",
            "terminal_proven": True,
        },
    }


def _storage_target(*, total_bytes: int, available_bytes: int) -> dict:
    multiplier = 3.25
    bytes_per_case = 3_250_000_000
    required_bytes = bytes_per_case * 4
    reserve_bytes = (total_bytes * 30 + 99) // 100
    admittable_bytes = max(0, available_bytes - reserve_bytes)
    max_additional_cases = admittable_bytes // bytes_per_case
    horizon = (admittable_bytes * 1_000 // (bytes_per_case * 200)) / 1_000
    return {
        "name": "task_output",
        "path": "/mnt/tmp",
        "capacity_scope": "derived_artifact_and_cache",
        "observed_at": OBSERVED_AT,
        "multiplier": multiplier,
        "bytes_per_case": bytes_per_case,
        "required_bytes": required_bytes,
        "ok": True,
        "blocker": None,
        "filesystem_block_size_bytes": 4096,
        "total_bytes": total_bytes,
        "free_bytes": available_bytes,
        "available_bytes": available_bytes,
        "reserve_bytes": reserve_bytes,
        "admittable_bytes": admittable_bytes,
        "projected_available_after_request_bytes": (available_bytes - required_bytes),
        "headroom_after_request_bytes": admittable_bytes - required_bytes,
        "max_additional_cases": max_additional_cases,
        "days_horizon_at_assumed_cases_per_day": horizon,
    }


def _git(repo: Path, *arguments: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *arguments],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _vm_probe_entrypoint(repo_root: str, *, worker: bool) -> dict[str, str]:
    relative = (
        "vm_coding_worker_v2.py"
        if worker
        else "api/g1q3_rca/scripts/run_rca_service_request.py"
    )
    path = Path(repo_root) / relative
    commit = _git(Path(repo_root), "rev-parse", "HEAD")
    stage = _git(Path(repo_root), "ls-files", "--stage", "--", relative)
    mode, blob, stage_and_path = stage.split(" ", 2)
    index, tracked_path = stage_and_path.split("\t", 1)
    assert index == "0"
    assert tracked_path == relative
    raw = path.read_bytes()
    return {
        "tree": _git(Path(repo_root), "rev-parse", f"{commit}^{{tree}}"),
        "entrypoint_path": str(path),
        "entrypoint_sha256": hashlib.sha256(raw).hexdigest(),
        "entrypoint_committed_sha256": hashlib.sha256(raw).hexdigest(),
        "entrypoint_git_mode": mode,
        "entrypoint_blob": blob,
    }


def _execute_vm_probe_locally(command, **kwargs):
    del command
    return subprocess.run(
        [sys.executable, "-c", kwargs["input"]],
        check=False,
        capture_output=True,
        text=True,
        timeout=kwargs["timeout"],
    )


def _predecessor_validator_source() -> str:
    return f"""#!{sys.executable}
import argparse
import hashlib
import json
from pathlib import Path
import sqlite3

parser = argparse.ArgumentParser()
parser.add_argument("--database", required=True)
parser.add_argument("--roles-json", required=True)
parser.add_argument("--expected-schemas-json", required=True)
args = parser.parse_args()
roles = json.loads(args.roles_json)
expected = json.loads(args.expected_schemas_json)
database = Path(args.database)
connection = sqlite3.connect(database.as_uri() + "?mode=ro&immutable=1", uri=True)
connection.execute("PRAGMA query_only=ON")
schemas = {{}}
if "control" in roles:
    schemas["control"] = connection.execute(
        "SELECT value FROM control_meta WHERE key='schema_version'"
    ).fetchone()[0]
if "delivery" in roles:
    schemas["delivery"] = connection.execute(
        "SELECT value FROM rca_delivery_meta WHERE key='schema_version'"
    ).fetchone()[0]
quick = connection.execute("PRAGMA quick_check").fetchone()[0]
foreign_keys = len(connection.execute("PRAGMA foreign_key_check").fetchall())
try:
    connection.execute("CREATE TABLE forbidden_write(value INTEGER)")
except sqlite3.OperationalError:
    write_probe = "blocked_readonly"
else:
    write_probe = "unexpected_write"
connection.close()
print(json.dumps({{
    "schema_version": "pnc_rca_predecessor_validator_result_v1",
    "ok": schemas == expected,
    "read_only": True,
    "side_effects": "none",
    "database_sha256": hashlib.sha256(database.read_bytes()).hexdigest(),
    "roles": roles,
    "schemas": schemas,
    "quick_check": quick,
    "foreign_key_check_rows": foreign_keys,
    "write_probe": write_probe,
}}, ensure_ascii=True, sort_keys=True, separators=(",", ":")))
"""


def _create_host_build_repo(root: Path) -> tuple[Path, str]:
    repo = root / "host-build"
    repo.mkdir()
    critical_files = (
        set(MINIMUM_CRITICAL_FILES)
        | set(DELIVERY_CRITICAL_FILES)
        | set(release_gate_module.RCA_RUNTIME_RELATIVE_FILES)
        | set(release_gate_module.GATEWAY_RCA_RUNTIME_RELATIVE_FILES)
    )
    for relative in sorted(critical_files):
        path = repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        if relative == "pyproject.toml":
            content = (
                "[project]\n"
                'name = "release-fixture"\n'
                'version = "0.0.0"\n'
                "[project.optional-dependencies]\n"
                'kafka = ["kafka-python==3.0.7", "python-snappy==0.7.3", '
                '"tinycss2==1.2.1"]\n'
                'feishu = ["lark-oapi==1.5.3"]\n'
            )
        elif relative == "scripts/pnc_rca_kafka_preflight.py":
            content = (
                release_gate_module.REPO_ROOT / relative
            ).read_text(encoding="utf-8")
        else:
            content = f"release fixture: {relative}\n"
        path.write_text(content, encoding="utf-8")
    validator = repo / PREDECESSOR_VALIDATOR_RELATIVE_PATH
    validator.parent.mkdir(parents=True, exist_ok=True)
    validator.write_text(_predecessor_validator_source(), encoding="utf-8")
    validator.chmod(0o755)
    _git(repo, "init", "-q")
    _git(repo, "add", ".")
    _git(
        repo,
        "-c",
        "user.name=RCA Release Test",
        "-c",
        "user.email=rca-release-test@example.invalid",
        "commit",
        "-q",
        "-m",
        "release fixture",
    )
    return repo, _git(repo, "rev-parse", "HEAD")


def _create_git_repo(
    root: Path,
    name: str,
    *,
    entrypoint_relative: str | None = None,
) -> tuple[Path, str]:
    repo = root / name
    repo.mkdir()
    (repo / "tracked.txt").write_text(f"{name}\n", encoding="utf-8")
    if name == "workspace-build":
        for relative in (
            release_gate_module.WORKSPACE_EXECUTION_CLOSURE_RELATIVE_PATHS
        ):
            execution_file = repo / relative
            execution_file.parent.mkdir(parents=True, exist_ok=True)
            execution_file.write_text(
                f"#!/usr/bin/env python3\n# fixture: {relative}\n",
                encoding="utf-8",
            )
            execution_file.chmod(0o755)
    if entrypoint_relative:
        entrypoint = repo / entrypoint_relative
        entrypoint.parent.mkdir(parents=True, exist_ok=True)
        entrypoint.write_text(
            f"#!/usr/bin/env python3\n# fixture: {name}\n",
            encoding="utf-8",
        )
    _git(repo, "init", "-q")
    _git(repo, "add", ".")
    _git(
        repo,
        "-c",
        "user.name=RCA Release Test",
        "-c",
        "user.email=rca-release-test@example.invalid",
        "commit",
        "-q",
        "-m",
        "release fixture",
    )
    return repo, _git(repo, "rev-parse", "HEAD")


def _workspace_runtime_binding_fixture(
    *,
    stage_root: Path,
    workspace_commit: str,
    workspace_closure: Mapping[str, Any],
) -> dict[str, Any]:
    file_sha256 = {
        relative: workspace_closure["files"][relative]["sha256"]
        for relative in release_gate_module.WORKSPACE_EXECUTION_CLOSURE_RELATIVE_PATHS
    }
    staged = {
        "schema_version": (
            release_gate_module.workspace_runtime.WORKSPACE_RUNTIME_IDENTITY_SCHEMA_VERSION
        ),
        "root": str(stage_root),
        "manifest_path": str(
            stage_root
            / release_gate_module.workspace_runtime.WORKSPACE_RUNTIME_MANIFEST_NAME
        ),
        "creator_path": str(stage_root / "bin" / "create_task_v2.py"),
        "manifest_sha256": "a" * 64,
        "closure_sha256": "b" * 64,
        "source_commit": workspace_commit,
        "file_sha256": file_sha256,
    }
    canonical_root = release_gate_module.CANONICAL_WORKSPACE_RUNTIME_ROOT
    canonical = {
        **staged,
        "root": str(canonical_root),
        "manifest_path": str(
            canonical_root
            / release_gate_module.workspace_runtime.WORKSPACE_RUNTIME_MANIFEST_NAME
        ),
        "creator_path": str(canonical_root / "bin" / "create_task_v2.py"),
    }
    return {
        "schema_version": (
            release_gate_module.WORKSPACE_RUNTIME_RELEASE_BINDING_SCHEMA_VERSION
        ),
        "staged_identity": staged,
        "staged_identity_sha256": _sha256_json(staged),
        "canonical_identity": canonical,
        "canonical_identity_sha256": _sha256_json(canonical),
    }


def _future_runtime_binding_fixture(
    *,
    stage_root: Path,
    host_repo: Path,
    host_commit: str,
) -> dict[str, Any]:
    canonical_root = release_gate_module.CANONICAL_FUTURE_RUNTIME_ROOT
    candidate_sha256 = {
        filename: hashlib.sha256(filename.encode("utf-8")).hexdigest()
        for filename in release_gate_module.FUTURE_RUNTIME_PLIST_FILENAMES
    }
    runtime_files = {
        relative: hashlib.sha256(relative.encode("utf-8")).hexdigest()
        for relative in release_gate_module.FUTURE_RUNTIME_RELATIVE_FILES
    }
    runtime_file_descriptors = {
        relative: {
            "path": relative,
            "sha256": digest,
            "size_bytes": len(relative.encode("utf-8")),
            "mode": "0644",
            "source_kind": "regular",
            "git_blob": "4" * 40,
        }
        for relative, digest in runtime_files.items()
    }
    interpreter_sha256 = "5" * 64
    gateway_origins = {
        "gateway.run": str(canonical_root / "gateway" / "run.py"),
    }
    render_manifest = {
        "schema_version": (
            release_gate_module.FUTURE_RUNTIME_RENDER_MANIFEST_SCHEMA_VERSION
        ),
        "source_repo_root": str(host_repo.resolve()),
        "source_commit": host_commit,
        "staging_root": str(stage_root),
        "canonical_live_root": str(canonical_root),
        "runtime_file_sha256": runtime_files,
        "runtime_files_sha256": _sha256_json(runtime_files),
        "interpreter": {
            "staging_path": str(stage_root / ".venv/bin/python"),
            "canonical_path": str(canonical_root / ".venv/bin/python"),
            "sha256": interpreter_sha256,
        },
        "dependencies": {},
        "candidate_plists": {
            filename: {
                "label": release_gate_module.runtime_stage.CANDIDATE_PLISTS[
                    filename
                ][0],
                "source_sha256": digest,
                "staging_sha256": hashlib.sha256(
                    f"staged:{filename}".encode("utf-8")
                ).hexdigest(),
                "canonical_sha256": digest,
                "canonical_body_sha256": hashlib.sha256(
                    f"canonical:{filename}".encode("utf-8")
                ).hexdigest(),
            }
            for filename, digest in candidate_sha256.items()
        },
        "canonical_launchd_config_sha256": "6" * 64,
        "canonical_runtime_config_sha256": LAUNCHD_CONFIG_SHA256,
        "gateway_runtime": {
            "sys_executable": str(canonical_root / ".venv/bin/python"),
            "sys_executable_sha256": interpreter_sha256,
            "process_executable": str(canonical_root / ".venv/bin/python"),
            "process_executable_sha256": interpreter_sha256,
            "module_origins": gateway_origins,
            "module_origins_sha256": _sha256_json(gateway_origins),
            "dependency_versions": dict(
                release_gate_module.EXPECTED_GATEWAY_RUNTIME_DEPENDENCY_VERSIONS
            ),
            "repo_module_count": 4,
            "venv_dependency_count": 2,
        },
    }
    render_manifest_sha256 = _sha256_json(render_manifest)
    manifest_identity = {
        "schema_version": release_gate_module.runtime_stage.MANIFEST_SCHEMA_VERSION,
        "staging_root": str(stage_root),
        "manifest_path": str(
            stage_root / release_gate_module.runtime_stage.MANIFEST_FILENAME
        ),
        "manifest_sha256": "7" * 64,
        "plan_sha256": "8" * 64,
        "content_sha256": "9" * 64,
        "source_commit": host_commit,
        "source_tree": _git(host_repo, "rev-parse", f"{host_commit}^{{tree}}"),
        "canonical_live_root": str(canonical_root),
        "candidate_plist_sha256": candidate_sha256,
        "runtime_file_descriptors": runtime_file_descriptors,
        "runtime_files_sha256": _sha256_json(runtime_files),
    }
    stage_identity = {
        "root": {
            "path": str(stage_root),
            "device": 1,
            "inode": 1,
            "owner_uid": os.geteuid(),
            "mode": 0o700,
        },
        "venv": {
            "path": str(stage_root / ".venv"),
            "device": 1,
            "inode": 2,
            "owner_uid": os.geteuid(),
            "mode": 0o755,
        },
        "interpreter": {
            "path": str(stage_root / ".venv/bin/python"),
            "sha256": interpreter_sha256,
            "device": 1,
            "inode": 3,
            "owner_uid": os.geteuid(),
            "mode": 0o755,
        },
    }
    projection = {
        "schema_version": (
            release_gate_module.FUTURE_RUNTIME_PROJECTION_SCHEMA_VERSION
        ),
        "ok": True,
        "source_commit": host_commit,
        "staging_root": str(stage_root),
        "canonical_live_root": str(canonical_root),
        "render_manifest_sha256": render_manifest_sha256,
    }
    return {
        "schema_version": (
            release_gate_module.FUTURE_RUNTIME_RELEASE_BINDING_SCHEMA_VERSION
        ),
        "runtime_stage_manifest_identity": manifest_identity,
        "runtime_stage_manifest_identity_sha256": _sha256_json(manifest_identity),
        "runtime_stage_identity": stage_identity,
        "runtime_stage_identity_sha256": _sha256_json(stage_identity),
        "future_runtime_projection": projection,
        "future_runtime_projection_sha256": _sha256_json(projection),
        "render_manifest": render_manifest,
        "render_manifest_sha256": render_manifest_sha256,
    }


def _auxiliary_runtime_test_fixture(tmp_path: Path):
    host_repo, host_commit = _create_host_build_repo(tmp_path)
    future_binding = _future_runtime_binding_fixture(
        stage_root=tmp_path / "future-runtime-stage",
        host_repo=host_repo,
        host_commit=host_commit,
    )
    expected = release_gate_module._auxiliary_runtime_expectations(future_binding)

    def runtime_identity(runtime):
        return {
            key: runtime[key]
            for key in (
                "executable",
                "script",
                "cwd",
                "script_sha256",
                "interpreter_sha256",
                "plist_path",
                "plist_sha256",
                "program_arguments_sha256",
                "environment_sha256",
            )
        }

    relay = expected["completion_relay"]
    relay_body = {
        "schema_version": release_gate_module.COMPLETION_RELAY_HEALTH_SCHEMA_VERSION,
        "service_label": relay["service_label"],
        "observed_at": NOW.isoformat(),
        "started_at": (NOW - timedelta(minutes=5)).isoformat(),
        "pid": 42001,
        "process_create_time": 1_783_650_001.0,
        "loop_count": 4,
        "startup_canary_loops_required": 3,
        "startup_canary_loops_completed": 3,
        "startup_canary_completed_at": (NOW - timedelta(minutes=1)).isoformat(),
        "configured_max_card_fallbacks_per_loop": 0,
        "effective_max_card_fallbacks_per_loop": 0,
        "card_fallback_attempted_count": 0,
        "card_fallback_sent_count": 0,
        "healthy": True,
        "errors": [],
        "runtime_identity": runtime_identity(relay),
    }
    sync = expected["vm_task_sync"]
    sync_result = {
        "candidate_count": 2,
        "synced_count": 2,
        "error_count": 0,
        "errors": [],
    }
    sync_body = {
        "schema_version": release_gate_module.VM_TASK_SYNC_COMPLETION_SCHEMA_VERSION,
        "service_label": sync["service_label"],
        "run_id": "vm-sync-run-0001",
        "started_at": (NOW - timedelta(seconds=90)).isoformat(),
        "completed_at": (NOW - timedelta(seconds=30)).isoformat(),
        "pid": 42002,
        "exit_code": 0,
        "ok": True,
        "skipped": False,
        **sync_result,
        "result_sha256": _sha256_json(sync_result),
        "runtime_identity": runtime_identity(sync),
    }
    return SimpleNamespace(
        expected=expected,
        relay=relay_body,
        sync=sync_body,
        not_before=NOW - timedelta(minutes=2),
    )


def _write_build_manifest(
    evidence_dir: Path,
    host_repo: Path,
    host_commit: str,
    workspace_repo: Path,
    workspace_commit: str,
    vm_repo: Path,
    vm_commit: str,
    vm_worker_repo: Path,
    vm_worker_commit: str,
    runtime_config_sha256: str,
) -> None:
    critical_files = {
        str(path.relative_to(host_repo)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(host_repo.rglob("*"))
        if path.is_file() and ".git" not in path.parts
    }
    workspace_provenance = release_gate_module._local_git_provenance(
        workspace_repo,
        component="workspace",
    )
    workspace_closure = workspace_provenance["execution_closure"]
    vm_provenance = release_gate_module._local_git_provenance(
        vm_repo,
        component="vm",
        entrypoint_relative="api/g1q3_rca/scripts/run_rca_service_request.py",
    )
    vm_worker_provenance = release_gate_module._local_git_provenance(
        vm_worker_repo,
        component="vm_worker",
        entrypoint_relative="vm_coding_worker_v2.py",
    )
    workspace_runtime_binding = _workspace_runtime_binding_fixture(
        stage_root=evidence_dir.parent / "workspace-runtime-stage",
        workspace_commit=workspace_commit,
        workspace_closure=workspace_closure,
    )
    future_runtime_binding = _future_runtime_binding_fixture(
        stage_root=evidence_dir.parent / "future-runtime-stage",
        host_repo=host_repo,
        host_commit=host_commit,
    )
    release_bom = {
        "schema_version": RELEASE_BOM_SCHEMA_VERSION,
        "components": {
            "host": {
                "source": "local_git",
                "repo_root": str(host_repo.resolve()),
                "commit": host_commit,
                "tree_clean": True,
                "status_sha256": EMPTY_GIT_STATUS_SHA256,
            },
            "workspace": {
                "source": "local_git_scoped_closure",
                "repo_root": str(workspace_repo.resolve()),
                "commit": workspace_commit,
                "execution_closure": workspace_closure,
                "execution_closure_sha256": _sha256_json(workspace_closure),
            },
            "vm": {
                "source": "ssh-mini-agent",
                "repo_root": str(vm_repo.resolve()),
                "commit": vm_commit,
                "tree_clean": True,
                "status_sha256": EMPTY_GIT_STATUS_SHA256,
                "tree": vm_provenance["tree"],
                "entrypoint_path": str(
                    vm_repo / "api/g1q3_rca/scripts/run_rca_service_request.py"
                ),
                "entrypoint_sha256": hashlib.sha256(
                    (
                        vm_repo / "api/g1q3_rca/scripts/run_rca_service_request.py"
                    ).read_bytes()
                ).hexdigest(),
                "entrypoint_committed_sha256": vm_provenance[
                    "entrypoint_committed_sha256"
                ],
                "entrypoint_git_mode": vm_provenance["entrypoint_git_mode"],
                "entrypoint_blob": vm_provenance["entrypoint_blob"],
            },
            "vm_worker": {
                "source": "ssh-mini-agent",
                "repo_root": str(vm_worker_repo.resolve()),
                "commit": vm_worker_commit,
                "tree_clean": True,
                "status_sha256": EMPTY_GIT_STATUS_SHA256,
                "tree": vm_worker_provenance["tree"],
                "entrypoint_path": str(vm_worker_repo / "vm_coding_worker_v2.py"),
                "entrypoint_sha256": hashlib.sha256(
                    (vm_worker_repo / "vm_coding_worker_v2.py").read_bytes()
                ).hexdigest(),
                "entrypoint_committed_sha256": vm_worker_provenance[
                    "entrypoint_committed_sha256"
                ],
                "entrypoint_git_mode": vm_worker_provenance[
                    "entrypoint_git_mode"
                ],
                "entrypoint_blob": vm_worker_provenance["entrypoint_blob"],
            },
        },
        "workspace_runtime": workspace_runtime_binding,
        "future_runtime": future_runtime_binding,
        "external_dependencies": (
            release_gate_module._external_release_dependency_provenance()
        ),
        "runtime_config_sha256": runtime_config_sha256,
        "launchd_config_sha256": LAUNCHD_CONFIG_SHA256,
        "critical_files_sha256": _sha256_json(critical_files),
    }
    _write_json(
        evidence_dir / "build_manifest.json",
        {
            "schema_version": BUILD_MANIFEST_SCHEMA_VERSION,
            "observed_at": OBSERVED_AT,
            "release_bom": release_bom,
            "release_bom_sha256": _sha256_json(release_bom),
            "critical_files": critical_files,
            "dependency_versions": dict(
                release_gate_module.EXPECTED_DEPENDENCY_VERSIONS
            ),
        },
    )


def _write_contract_pair(tmp_path: Path) -> tuple[Path, Path]:
    host = tmp_path / "host-contract.py"
    vm = tmp_path / "vm-contract.py"
    body = f"prefix\n{BEGIN}\nVALUE = 1\n{END}\nsuffix\n"
    host.write_text(body, encoding="utf-8")
    vm.write_text(body, encoding="utf-8")
    return host, vm


def _remote_reader_health() -> dict:
    runtime = {
        "execution_mode": "isolated_subprocess",
        "dependency_domain": "pdcl_pyclip_system_python_v1",
        "required_python_executable": "/usr/bin/python3",
        "resolved_required_python_executable": "/usr/bin/python3.8",
        "python_executable": "/usr/bin/python3.8",
        "python_version": "3.8.10",
        "reader_import_scope": "sidecar_only",
        "timeout_seconds": 120,
        "module_path": (
            "/home/mini/data3/yj-evaluation-server/api/g1q3_rca/vendor/"
            "pdcl_pyclip-0.1.6+rca.2-py3-none-any.whl/pdcl_pyclip/reader.py"
        ),
        "distribution": "pdcl_pyclip",
        "version": "0.1.6+rca.2",
        "upstream_version": "0.1.6",
        "upstream_wheel_sha256": (
            "e760a532dbe7dff730ef8e85b32e4ff33d14acefe7c8f295224bb77b08fcadae"
        ),
        "upstream_reader_module_sha256": (
            "0f85507d5913df6c39e027687e49cc95ea781bc2eb24a4c3357d17e6f876c591"
        ),
        "sanitized_wheel_sha256": SANITIZED_WHEEL_SHA256,
        "sanitized_reader_module_sha256": (
            "b4a98fca46ba7a71f4a0f1cc55b7549c8c37aa195f98d74aee4fd7bd0770acd6"
        ),
        "patch_sha256": (
            "659199da1a56c201e3f99c15893d56e36a6195d23666c78e3457121ee2fea3db"
        ),
        "source_commit": "62a84e39146800ed5d05a6d7c0866d6b06bf6437",
        "sanitized_source_commit": "7d0d028020dd140466a8a6f181f0e75bc142d2bc",
        "security_attestation": {
            "credentials_source": "environment_only",
            "auto_mount": False,
            "privileged_subprocess": False,
        },
        "runtime_environment": {
            "endpoint_variable": "PDCL_BASE_URL",
            "endpoint_source": "environment_only",
            "endpoint_present": True,
            "endpoint_valid": True,
            "credentials_source": "environment_only",
            "credentials_present": True,
            "cache_mount_variable": "PDCL_CACHE_MOUNT_POINT",
            "cache_mount_source": "environment_only_pre_mounted",
            "cache_mount_present": True,
            "legacy_endpoint_variables_accepted": False,
            "cache_mount_ready": True,
        },
        "sanitization_scan": {
            "storage_module_sha256": (
                "4c31ba2450269659db8f1475785136638349e556b9035b9ef9a76b318e56256a"
            ),
            "config_module_sha256": (
                "828c851a8d52fd9b5c50ea243d027ccd3870ffc3a383bf9d5fc10533fd43cebf"
            ),
            "subprocess_present": False,
            "sudo_present": False,
            "apt_present": False,
            "static_sts_literal_present": False,
        },
        "resource_policy": {
            "event_clip_iteration": "sequential",
            "max_open_readers_per_event": 1,
            "hard_timeout_boundary": "adapter_subprocess",
            "hard_timeout_seconds": 120,
            "termination_verified": True,
        },
        "dependencies": {
            "mcap": {"requirement": "==1.2.2", "installed_version": "1.2.2"},
            "protobuf": {
                "requirement": "==3.20.3",
                "installed_version": "3.20.3",
            },
            "typer": {"requirement": "==0.20.0", "installed_version": "0.20.0"},
            "pdcl_dss": {
                "requirement": "==0.1.44",
                "installed_version": "0.1.44",
            },
        },
    }
    base_parameters = [
        "self",
        "topics",
        "start_time",
        "end_time",
        "log_time_order",
        "reverse",
    ]
    reader_classes = {
        "RemoteClipReader": {
            "importable": True,
            "module": "pdcl_pyclip.reader",
            "iter_messages_parameters": base_parameters,
            "features": {
                "bounded_messages": False,
                "deadline_monotonic": False,
                "sequential_clip_iteration": False,
            },
        },
        "RemoteEventReader": {
            "importable": True,
            "module": "pdcl_pyclip.reader",
            "iter_messages_parameters": base_parameters
            + ["max_messages", "deadline_monotonic"],
            "features": {
                "bounded_messages": True,
                "deadline_monotonic": True,
                "sequential_clip_iteration": True,
            },
        },
    }
    api = {
        "time_unit": "nanoseconds",
        "window": "[start,end)",
        "topic_filter": True,
    }
    fingerprint = _sha256_json({
        "runtime": runtime,
        "reader_classes": reader_classes,
        "api_contract": api,
    })
    return {
        "schema_version": "pnc_rca_remote_reader_health_v1",
        "observed_at": OBSERVED_AT,
        "ok": True,
        "source": {
            "generation_mode": "machine_generated",
            "component": "pnc_rca_remote_reader_preflight",
            "component_commit": "c" * 40,
            "artifact_sha256": hashlib.sha256(
                b"pnc_rca_remote_reader_preflight"
            ).hexdigest(),
        },
        "runtime": runtime,
        "reader_classes": reader_classes,
        "api_contract": api,
        "reader_fingerprint": fingerprint,
        "fixture_preflight": {
            "ok": True,
            "reader_fingerprint": fingerprint,
            "reader_classes": ["RemoteClipReader", "RemoteEventReader"],
            "topics": ["Odometry", "vehicle_signal_highfreq"],
            "message_count": 4,
            "time_window_filter": True,
            "errors": [],
        },
        "mdi_invocation_count": 0,
        "errors": [],
    }


def _remote_soak_sha256(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _remote_soak_merkle_root(record_hashes: list[str]) -> str:
    level = [
        hashlib.sha256(
            release_gate_module.REMOTE_READER_SOAK_MERKLE_LEAF_PREFIX
            + bytes.fromhex(digest)
        ).digest()
        for digest in record_hashes
    ]
    while len(level) > 1:
        if len(level) % 2:
            level.append(level[-1])
        level = [
            hashlib.sha256(
                release_gate_module.REMOTE_READER_SOAK_MERKLE_NODE_PREFIX
                + level[index]
                + level[index + 1]
            ).digest()
            for index in range(0, len(level), 2)
        ]
    return level[0].hex()


def _remote_soak_scope(function_domain: str) -> dict:
    topics_by_domain = {
        "ACC": (
            "vehicle_signal_highfreq",
            "mcu.6.control.controldebug",
            "kvaser.0.can1.Vehicle",
            "ObjectPerceptionObjectList",
        ),
        "LCC": (
            "vehicle_signal_highfreq",
            "mcu.6.control.controldebug",
            "kvaser.0.can1.Vehicle",
            "lanebev",
        ),
        "AEB": (
            "vehicle_signal_highfreq",
            "mcu.6.control.controldebug",
            "kvaser.0.can1.Vehicle",
            "ObjectPerceptionObjectList",
        ),
        "FCW": (
            "vehicle_signal_highfreq",
            "mcu.6.control.controldebug",
            "kvaser.0.can1.Vehicle",
            "ObjectPerceptionObjectList",
        ),
        "DNP": (
            "vehicle_signal_highfreq",
            "dnp_env",
            "lanebev",
            "ObjectPerceptionObjectList",
        ),
    }
    channels_by_topic = {
        "vehicle_signal_highfreq": (
            "sigmastar.1.dds2.vehicle_signal_highfreq",
            "sigmastar~1~dds2.vehicle_signal_highfreq",
        ),
        "mcu.6.control.controldebug": ("mcu.6.control.controldebug",),
        "kvaser.0.can1.Vehicle": (
            "kvaser.0.can1.Vehicle",
            "kvaser.1.can1.Vehicle",
            "kvaser.0.can1.Debug",
            "kvaser.0.can2.Debug",
            "kvaser.1.can1.Debug",
            "kvaser.1.can2.Debug",
        ),
        "lanebev": (
            "sigmastar.1.ddsflow.lanebev",
            "sigmastar.1.dds2.lanebev",
        ),
        "dnp_env": ("sigmastar.1.dds2.dnp_env",),
        "ObjectPerceptionObjectList": (
            "sigmastar.1.dds2.ObjectPerceptionObjectList",
            "j2c.5.pack.ObjectPerceptionObjectList",
        ),
    }
    topics = sorted(topics_by_domain[function_domain])
    requirements = {
        "schema_version": "g1q3_rca_remote_evidence_requirements_v1",
        "requirements_contract_version": "g1q3_rca_evaluator_scope_v1",
        "requirements_contract_hash": (
            release_gate_module.REMOTE_READER_SOAK_REQUIREMENTS_CONTRACT_SHA256
        ),
        "function_domain": function_domain,
        "requested_topics": topics,
        "channel_allowlist": sorted({
            channel
            for topic in topics
            for channel in channels_by_topic[topic]
        }),
        "frame_lookup": {},
        "frame_channel_allowlist": [],
        "requested_window": {
            "mode": "full_reference",
            "start_time_ns": None,
            "end_time_ns": None,
        },
        "evaluator_fingerprints": {
            "g1q3_rca/rca_evaluators/_raw_streams.py": (
                "2bad23f9f40cfa5e87f956e4a9fbb75c3b255a176270c6f0525763eb17ad1f43"
            ),
            "g1q3_rca/report_builder.py": (
                "714733428448a7664d77602201adce7a64900dcf91297e3d3c4f8691a76a0d6a"
            ),
            "g1q3_rca/scripts/check_case_gate.py": (
                "932fc601c2179f1ff373ef6e31349b103cd40c20b321932b89db8c16c456e86e"
            ),
            "g1q3_rca/signal_registry.py": (
                "6b3a219a6482e9277d9d4179d0093d535736ae96559ee4634b2774a3f41d6b78"
            ),
        },
    }
    requirements["requirements_hash"] = _sha256_json(requirements)
    assert requirements["requirements_hash"] == (
        release_gate_module.REMOTE_READER_SOAK_REQUIREMENTS_SHA256[function_domain]
    )
    return {
        "source": "vm_evaluator_scope_contract",
        "requirements": requirements,
    }


def _remote_reader_soak(
    *,
    vm_commit: str,
    vm_tree: str,
    remote_reader_health_sha256: str,
) -> tuple[dict, dict]:
    duration = 86_400
    started = NOW - timedelta(seconds=duration)
    started_text = started.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    ended_text = NOW.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    candidate = {
        "base_commit": release_gate_module.REMOTE_READER_SOAK_BASE_COMMIT,
        "base_tree": release_gate_module.REMOTE_READER_SOAK_BASE_TREE,
        "execution_commit": vm_commit,
        "execution_tree": vm_tree,
        "clean": True,
    }
    runtime_environment = _remote_reader_health()["runtime"]["runtime_environment"]
    dependency_doctor_sha256 = "9" * 64
    dependency_material = {
        "remote_reader_health_sha256": remote_reader_health_sha256,
        "remote_reader_health_reader_fingerprint": _remote_reader_health()[
            "reader_fingerprint"
        ],
        "dependency_doctor_sha256": dependency_doctor_sha256,
        "distribution": "pdcl_pyclip",
        "version": "0.1.6+rca.2",
        "sanitized_policy": "env_only_pre_mounted_cache_v1",
        "sanitized_patch_commit": "7d0d028",
        "wheel_sha256": SANITIZED_WHEEL_SHA256,
        "source_hashes": {
            "pdcl_pyclip/reader.py": "1" * 64,
            "pdcl_pyclip/_storage.py": "2" * 64,
        },
        "dependencies": {
            "mcap": "1.2.2",
            "pdcl-dss": "0.1.44",
            "protobuf": "3.20.3",
            "typer": "0.20.0",
        },
        "runtime": {
            "execution_mode": "isolated_subprocess",
            "dependency_domain": "pdcl_pyclip_repo_target_system_deps_v1",
            "python_executable": "/usr/bin/python3.8",
            "required_python_executable": "/usr/bin/python3",
            "resolved_required_python_executable": "/usr/bin/python3.8",
            "reader_import_scope": "sidecar_only",
            "timeout_seconds": 120,
        },
        "runtime_environment": runtime_environment,
        "runtime_proof_sha256": "3" * 64,
    }
    reader_material = {
        "distribution": "pdcl_pyclip",
        "version": "0.1.6+rca.2",
        "wheel_sha256": SANITIZED_WHEEL_SHA256,
        "source_hashes": dependency_material["source_hashes"],
        "api": {
            "RemoteClipReader": {
                "constructor": "(clip_uuid)",
                "iter_messages": "(self, topics=None)",
                "missing_methods": [],
            },
            "RemoteEventReader": {
                "constructor": "(event_uuid, max_clips=16)",
                "iter_messages": "(self, topics=None, max_messages=None)",
                "missing_methods": [],
            },
        },
        "runtime_environment": runtime_environment,
        "remote_reader_health_reader_fingerprint": _remote_reader_health()[
            "reader_fingerprint"
        ],
    }
    dependency_fingerprint = _remote_soak_sha256(dependency_material)
    soak_reader_fingerprint = _remote_soak_sha256(reader_material)
    records = []
    manifest_cases = []
    for index in range(200):
        if index < 32:
            offset_start_ms = (index // 4) * 120_000
        else:
            batch = (index - 32) // 4
            hour = 1 + (batch % 23)
            within_hour = batch // 23
            offset_start_ms = hour * 3_600_000 + within_hour * 120_000
        offset_end_ms = offset_start_ms + 120_000
        function_domain = ("ACC", "LCC", "AEB", "DNP")[index % 4]
        quota_domain = "AEB_FCW" if function_domain == "AEB" else function_domain
        kind = "clip" if index % 4 == 0 else "event"
        reader_class = (
            "RemoteClipReader" if kind == "clip" else "RemoteEventReader"
        )
        locator_field = "clip_uuid" if kind == "clip" else "event_uuid"
        locator = f"locator-{index:04d}"
        locator_sha256 = hashlib.sha256(locator.encode()).hexdigest()
        reference = {
            "kind": kind,
            "reader_class": reader_class,
            "locator_field": locator_field,
            "locator_sha256": locator_sha256,
        }
        reference["reference_binding_sha256"] = _remote_soak_sha256(reference)
        manifest_case = {
            "case_id": f"case-{index:04d}",
            "work_item_id": f"work-{index:04d}",
            "function_domain": function_domain,
            "data_access": {
                "schema_version": "g1q3_rca_remote_data_access_v1",
                "mode": "remote_read",
                "transport": "pdcl_pyclip",
                "references": [
                    {
                        "kind": kind,
                        locator_field: locator,
                        "reader_class": reader_class,
                    }
                ],
                "source": {
                    "field": "问题数据地址_PDCL",
                    "value_sha256": locator_sha256,
                },
                "reader_contract": {
                    "distribution": "pdcl_pyclip",
                    "required_version": "0.1.6+rca.2",
                    "mdi_download_allowed": False,
                    "fallback": "forbidden",
                    "completeness": "full_requested_scope",
                },
            },
        }
        manifest_cases.append(manifest_case)
        scope = _remote_soak_scope(function_domain)
        case_started = started + timedelta(milliseconds=offset_start_ms)
        case_ended = started + timedelta(milliseconds=offset_end_ms)
        record = {
            "schema_version": (
                release_gate_module.REMOTE_READER_SOAK_CASE_SCHEMA_VERSION
            ),
            "case_id": f"case-{index:04d}",
            "work_item_id": f"work-{index:04d}",
            "function_domain": function_domain,
            "quota_domain": quota_domain,
            "workload_manifest_record_sha256": _remote_soak_sha256(manifest_case),
            "reference": reference,
            "requested_scope": scope,
            "requested_scope_binding": {
                "requirements_contract_sha256": scope["requirements"][
                    "requirements_contract_hash"
                ],
                "requirements_sha256": scope["requirements"]["requirements_hash"],
                "requested_scope_sha256": _remote_soak_sha256(scope),
            },
            "candidate_binding": dict(candidate),
            "dependency_fingerprint": dependency_fingerprint,
            "reader_fingerprint": soak_reader_fingerprint,
            "remote_receipt": {
                "schema_version": "g1q3_rca_remote_read_receipt_v1",
                "status": "completed",
                "relative_path": (
                    f"cases/case-{index:04d}-{reference['reference_binding_sha256'][:12]}"
                    "/remote_read_receipt.json"
                ),
                "sha256": hashlib.sha256(
                    f"receipt-{index:04d}".encode()
                ).hexdigest(),
                "completeness": "full_requested_scope",
                "exhausted": True,
                "message_count": 100,
                "bytes": 1000,
            },
            "materialization": {
                "input_materialized": False,
                "input_materialized_bytes": 0,
                "mdi_invocation_count": 0,
                "download_invocation_count": 0,
                "fallback_count": 0,
                "retained_stream_cache_bytes": 0,
                "observation": "remote_receipt_verified_stream_cache_removed_v1",
            },
            "timing": {
                "started_at": case_started.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "ended_at": case_ended.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "duration_ms": 120_000,
                "offset_start_ms": offset_start_ms,
                "offset_end_ms": offset_end_ms,
            },
        }
        record["record_sha256"] = _remote_soak_sha256(record)
        records.append(record)
    record_hashes = [record["record_sha256"] for record in records]
    domain_counts = {"ACC": 50, "AEB_FCW": 50, "DNP": 50, "LCC": 50}
    reader_counts = {"RemoteClipReader": 50, "RemoteEventReader": 150}
    messages_total = sum(
        record["remote_receipt"]["message_count"] for record in records
    )
    bytes_total = sum(record["remote_receipt"]["bytes"] for record in records)
    manifest_source = {
        "generation_mode": "machine_generated",
        "component": "rca_issue_workload_export",
        "component_commit": "7" * 40,
        "artifact_sha256": "8" * 64,
    }
    workload_manifest = {
        "schema_version": (
            release_gate_module.REMOTE_READER_SOAK_MANIFEST_SCHEMA_VERSION
        ),
        "generated_at": started_text,
        "source": manifest_source,
        "domain_quotas": dict(domain_counts),
        "reader_class_quotas": {
            "RemoteClipReader": 25,
            "RemoteEventReader": 25,
        },
        "cases": manifest_cases,
    }
    body = {
        "schema_version": release_gate_module.REMOTE_READER_SOAK_SCHEMA_VERSION,
        "observed_at": ended_text,
        "ok": True,
        "source": {
            "generation_mode": "machine_generated",
            "component": "pnc_rca_remote_reader_soak",
            "component_commit": vm_commit,
            "component_tree": vm_tree,
            "module": release_gate_module.REMOTE_READER_SOAK_MODULE,
            "module_sha256": release_gate_module.REMOTE_READER_SOAK_MODULE_SHA256,
            "entrypoint": release_gate_module.REMOTE_READER_SOAK_ENTRYPOINT,
            "entrypoint_sha256": (
                release_gate_module.REMOTE_READER_SOAK_ENTRYPOINT_SHA256
            ),
            "schema": release_gate_module.REMOTE_READER_SOAK_VM_SCHEMA,
            "schema_sha256": release_gate_module.REMOTE_READER_SOAK_SCHEMA_SHA256,
        },
        "candidate_binding": dict(candidate),
        "dependency_binding": {
            "distribution": "pdcl_pyclip",
            "version": "0.1.6+rca.2",
            "wheel_sha256": SANITIZED_WHEEL_SHA256,
            "runtime_proof_sha256": dependency_material["runtime_proof_sha256"],
            "dependency_doctor_sha256": dependency_doctor_sha256,
            "remote_reader_health_sha256": remote_reader_health_sha256,
            "fingerprint_material": dependency_material,
            "reader_fingerprint_material": reader_material,
            "fingerprint": dependency_fingerprint,
            "reader_fingerprint": soak_reader_fingerprint,
        },
        "workload_manifest": {
            "schema_version": (
                release_gate_module.REMOTE_READER_SOAK_MANIFEST_SCHEMA_VERSION
            ),
            "sha256": _remote_soak_sha256(workload_manifest),
            "case_count": 200,
            "unique_work_items": 200,
            "unique_references": 200,
            "domain_counts": dict(domain_counts),
            "reader_class_counts": dict(reader_counts),
            "source": dict(manifest_source),
        },
        "policy": {
            "duration_seconds": duration,
            "min_cases": 200,
            "domain_quotas": dict(domain_counts),
            "reader_class_quotas": {
                "RemoteClipReader": 25,
                "RemoteEventReader": 25,
            },
            "max_concurrency": 4,
            "per_case_timeout_seconds": 120,
            "minimum_seconds_at_reserve": 900,
            "minimum_longest_sustained_seconds": 300,
            "temporal_bucket_seconds": 3_600,
            "minimum_temporal_buckets": 24,
            "maximum_temporal_gap_seconds": 3_600,
            "max_clips": 16,
            "max_messages": 250_000,
            "max_scanned_messages": 1_000_000,
            "max_output_bytes": 750_000_000,
            "retry_count": 0,
            "input_materialization": "forbidden",
            "fallback": "forbidden",
        },
        "started_at": started_text,
        "ended_at": ended_text,
        "duration_seconds": duration,
        "attempted_cases": 200,
        "completed_cases": 200,
        "concurrency_peak": 4,
        "concurrency_profile": {
            "reserved_cases": 4,
            "peak": 4,
            "sample_interval_seconds": 10,
            "samples_total": 8_640,
            "samples_at_or_above_reserved": 600,
            "longest_sustained_seconds_at_or_above_reserved": 960,
            "over_admission_count": 0,
        },
        "temporal_profile": {
            "bucket_seconds": 3_600,
            "minimum_buckets": 24,
            "minimum_cases_per_bucket": 4,
            "scheduler_tolerance_seconds": 30,
            "bucket_start_counts": {
                **{"0": 32},
                **{str(index): 8 for index in range(1, 20)},
                **{str(index): 4 for index in range(20, 24)},
            },
            "covered_bucket_count": 24,
            "first_bucket_covered": True,
            "last_bucket_covered": True,
            "maximum_start_gap_ms": 3_600_000,
            "maximum_allowed_start_gap_ms": 3_630_000,
        },
        "capacity_lifecycle": {
            "reservation_count": 200,
            "activation_count": 200,
            "release_count": 200,
            "terminal_proven_count": 200,
            "peak_held_cases": 4,
            "held_bytes_leak_count": 0,
            "stale_lease_count": 0,
            "fence_conflict_count": 0,
            "release_failure_count": 0,
        },
        "workload_mix": {
            "reader_class_cases": dict(reader_counts),
            "function_domain_cases": dict(domain_counts),
            "reference_kind_cases": {"clip": 50, "event": 150},
            "unique_work_items": 200,
            "unique_data_references": 200,
        },
        "case_evidence": {
            "record_schema": (
                release_gate_module.REMOTE_READER_SOAK_CASE_SCHEMA_VERSION
            ),
            "records": records,
            "record_count": 200,
            "record_hashes": record_hashes,
            "merkle_algorithm": (
                release_gate_module.REMOTE_READER_SOAK_MERKLE_ALGORITHM
            ),
            "merkle_root": _remote_soak_merkle_root(record_hashes),
        },
        "zero_invariants": {
            "error_count": 0,
            "input_materialized_count": 0,
            "input_materialized_bytes": 0,
            "retained_stream_cache_bytes": 0,
            "mdi_invocation_count": 0,
            "download_invocation_count": 0,
            "fallback_count": 0,
            "timeout_count": 0,
            "rate_limit_count": 0,
            "reconnect_count": 0,
            "retry_count": 0,
            "completeness_failure_count": 0,
        },
        "latency_ms": {
            "p50": 120_000.0,
            "p95": 120_000.0,
            "p99": 120_000.0,
            "max": 120_000.0,
        },
        "throughput": {
            "messages_total": messages_total,
            "bytes_total": bytes_total,
            "messages_per_second": messages_total / duration,
            "bytes_per_second": bytes_total / duration,
        },
        "resources": {
            "rss_peak_bytes": 512 * 1024 * 1024,
            "fd_peak": 64,
            "fd_limit": 1024,
            "open_reader_peak": 4,
            "concurrency_peak": 4,
        },
        "errors": [],
    }
    return body, workload_manifest


def _remote_reader_workload_provenance(
    workload_manifest: dict,
) -> dict[str, dict]:
    manifest_at = datetime.strptime(
        workload_manifest["generated_at"], "%Y-%m-%dT%H:%M:%S.%fZ"
    ).replace(tzinfo=timezone.utc)
    census_at = (manifest_at - timedelta(seconds=2)).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )
    approved_at = (manifest_at - timedelta(seconds=1)).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )
    options = [
        {"option_ids": ["active", "aeb"], "option_path": ["active", "AEB"]},
        {"option_ids": ["driving", "acc"], "option_path": ["driving", "ACC"]},
        {"option_ids": ["driving", "dnp"], "option_path": ["driving", "DNP"]},
        {"option_ids": ["driving", "lcc"], "option_path": ["driving", "LCC"]},
    ]
    taxonomy_material = {
        "field_key": "field_e776bb",
        "field_name": "function category",
        "field_type": "tree-select",
        "options": options,
    }
    taxonomy_sha256 = _remote_soak_sha256(taxonomy_material)
    source = {
        "component": "rca_issue_workload_export",
        "component_commit": workload_manifest["source"]["component_commit"],
        "module": release_gate_module.REMOTE_READER_WORKLOAD_EXPORT_MODULE,
        "module_sha256": workload_manifest["source"]["artifact_sha256"],
        "committed_match": True,
        "module_clean": True,
    }
    sessions = [
        {
            "offset": offset,
            "requested": 50,
            "returned": 50,
            "observed_source_count": 200,
            "query_sha256": hashlib.sha256(f"query-{offset}".encode()).hexdigest(),
            "session_id_sha256": hashlib.sha256(
                f"session-{offset}".encode()
            ).hexdigest(),
        }
        for offset in range(0, 200, 50)
    ]
    categories = [
        {
            "option_path": option_path,
            "record_count": 50,
            "valid_work_item_count": 50,
            "valid_reference_candidate_count": 50,
            "unique_reference_count": 50,
            "duplicate_reference_candidate_count": 0,
            "reader_class_counts": {reader_class: 50},
            "dnp_keyword_reference_candidate_count": (
                50 if option_path == ["driving", "DNP"] else 0
            ),
        }
        for option_path, reader_class in (
            (["active", "AEB"], "RemoteEventReader"),
            (["driving", "ACC"], "RemoteClipReader"),
            (["driving", "DNP"], "RemoteEventReader"),
            (["driving", "LCC"], "RemoteEventReader"),
        )
    ]
    security = {
        "raw_issue_payload_persisted": False,
        "raw_pdcl_field_persisted": False,
        "raw_dnp_keyword_fields_persisted": False,
        "description_or_attachment_persisted": False,
        "credential_or_token_persisted": False,
        "input_materialized": False,
        "input_materialized_bytes": 0,
    }
    census = {
        "schema_version": (
            release_gate_module.REMOTE_READER_WORKLOAD_CENSUS_SCHEMA_VERSION
        ),
        "observed_at": census_at,
        "source": source,
        "feishu": {
            "host": "project.feishu.cn",
            "authenticated": True,
            "project_key": "t03o4q",
            "work_item_type": "issue",
            "selected_fields": [
                "work_item_id",
                "name",
                "field_c7f370",
                "field_4bf24b",
                "field_e776bb",
                "field_93aa63",
            ],
            "mutation_performed": False,
            "attachment_read_performed": False,
            "page_size": 50,
            "page_count": 4,
            "session_provenance": sessions,
        },
        "taxonomy": {
            **taxonomy_material,
            "sha256": taxonomy_sha256,
            "leaf_count": len(options),
        },
        "dnp_keyword_policy": {
            "field_keys": ["name", "field_c7f370", "field_4bf24b"],
            "keywords": ["规划", "SPP", "OOI"],
            "match_mode": "nfkc_casefold_cjk_substring_ascii_token",
            "sha256": _remote_soak_sha256(
                {
                    "field_keys": ["name", "field_c7f370", "field_4bf24b"],
                    "keywords": ["规划", "SPP", "OOI"],
                    "match_mode": "nfkc_casefold_cjk_substring_ascii_token",
                }
            ),
        },
        "statistics": {
            "initial_source_count": 200,
            "minimum_observed_source_count": 200,
            "maximum_observed_source_count": 200,
            "target_records": 200,
            "records_seen": 200,
            "source_scan_complete": True,
            "unique_work_items_seen": 200,
            "valid_work_item_count": 200,
            "valid_reference_candidate_count": 200,
            "unique_reference_count": 200,
            "duplicate_reference_candidate_count": 0,
            "snapshot_stable": True,
            "categories": categories,
            "reader_class_counts": {
                "RemoteClipReader": 50,
                "RemoteEventReader": 150,
            },
            "reference_kind_counts": {"clip": 50, "event": 150},
            "dnp_keyword_matches": {
                "record_count": 50,
                "valid_work_item_count": 50,
                "reference_candidate_count": 50,
                "keyword_record_counts": {"规划": 50, "SPP": 0, "OOI": 0},
                "keyword_valid_work_item_counts": {"规划": 50, "SPP": 0, "OOI": 0},
                "keyword_reference_candidate_counts": {
                    "规划": 50,
                    "SPP": 0,
                    "OOI": 0,
                },
                "rejection_reasons": {},
            },
            "rejection_reasons": {},
        },
        "security": security,
    }
    rules = [
        {
            "function_domain": domain,
            "option_ids": option_ids,
            "option_path": option_path,
        }
        for domain, option_ids, option_path in (
            ("AEB", ["active", "aeb"], ["active", "AEB"]),
            ("ACC", ["driving", "acc"], ["driving", "ACC"]),
            ("LCC", ["driving", "lcc"], ["driving", "LCC"]),
        )
    ]
    rules_material = {
        "schema_version": (
            release_gate_module.REMOTE_READER_DOMAIN_MAPPING_SCHEMA_VERSION
        ),
        "project_key": "t03o4q",
        "work_item_type": "issue",
        "field_key": "field_e776bb",
        "taxonomy_sha256": taxonomy_sha256,
        "rules": rules,
    }
    approval = {
        "schema_version": (
            release_gate_module.REMOTE_READER_DOMAIN_MAPPING_APPROVAL_SCHEMA_VERSION
        ),
        "authority": "PDCL/data owner",
        "approved_by": "data-owner-fixture",
        "approved_at": approved_at,
        "mapping_rules_sha256": _remote_soak_sha256(rules_material),
    }
    approval_file_sha256 = hashlib.sha256(
        release_gate_module._remote_soak_canonical_json_bytes(approval) + b"\n"
    ).hexdigest()
    mapping_approval = {
        "authority": "PDCL/data owner",
        "approved_by": approval["approved_by"],
        "approved_at": approved_at,
        "receipt_sha256": approval_file_sha256,
    }
    mapping = {**rules_material, "approval": mapping_approval}

    def file_sha256(body: dict) -> str:
        return hashlib.sha256(
            release_gate_module._remote_soak_canonical_json_bytes(body) + b"\n"
        ).hexdigest()

    receipt = {
        "schema_version": (
            release_gate_module.REMOTE_READER_WORKLOAD_EXPORT_RECEIPT_SCHEMA_VERSION
        ),
        "generated_at": workload_manifest["generated_at"],
        "source": source,
        "census": {
            "body_sha256": _remote_soak_sha256(census),
            "file_sha256": file_sha256(census),
        },
        "taxonomy_sha256": taxonomy_sha256,
        "dnp_keyword_policy": census["dnp_keyword_policy"],
        "mapping": {
            "artifact_sha256": file_sha256(mapping),
            "rules_material_sha256": approval["mapping_rules_sha256"],
            "approval": mapping_approval,
        },
        "selection": {
            "eligible_reference_candidates": 200,
            "dnp_keyword_reference_candidates": 50,
            "dnp_keyword_selected_cases": 50,
            "unmapped_category_counts": [],
            "case_count": 200,
            "unique_work_items": 200,
            "unique_references": 200,
            "domain_counts": {"ACC": 50, "AEB_FCW": 50, "DNP": 50, "LCC": 50},
            "reader_class_counts": {
                "RemoteClipReader": 50,
                "RemoteEventReader": 150,
            },
        },
        "manifest": {
            "schema_version": (
                release_gate_module.REMOTE_READER_SOAK_MANIFEST_SCHEMA_VERSION
            ),
            "body_sha256": _remote_soak_sha256(workload_manifest),
            "file_sha256": file_sha256(workload_manifest),
            "case_count": 200,
        },
        "security": security,
    }
    return {
        "census": census,
        "mapping": mapping,
        "approval": approval,
        "receipt": receipt,
    }


def _write_common_evidence(evidence_dir: Path, kafka_env_file: Path) -> None:
    evidence_dir.mkdir()
    source, env_observation = load_kafka_preflight_environment(kafka_env_file)
    probe = BrokerProbeConfig.from_env(source)
    topology = [
        {
            "partition": 0,
            "leader_id": 1,
            "leader_epoch": 10,
            "replicas": [1, 2],
            "isr": [1, 2],
            "offline_replicas": [],
        },
        {
            "partition": 1,
            "leader_id": 2,
            "leader_epoch": 11,
            "replicas": [2, 3],
            "isr": [2, 3],
            "offline_replicas": [],
        },
    ]
    probe_public = probe.public_dict()
    _write_json(
        evidence_dir / "broker_metadata.json",
        {
            "schema_version": BROKER_METADATA_SCHEMA_VERSION,
            "observed_at": OBSERVED_AT,
            "topic_authorized": True,
            "topic_healthy": True,
            "group_authorized": True,
            "cluster_id": probe.expected_cluster_id,
            "expected_cluster_id": probe.expected_cluster_id,
            "topic": TOPIC,
            "group_id": probe.configured_group_id,
            "partitions": [0, 1],
            "partition_topology": topology,
            "replication_factor": 2,
            "topic_authorized_operations": ["DESCRIBE", "READ"],
            "group_authorized_operations": ["DESCRIBE", "READ"],
            "production_eligible": True,
            "owner_approval_required": [],
            "collector": {
                "schema_version": (
                    release_gate_module.KAFKA_PREFLIGHT_COLLECTOR_SCHEMA_VERSION
                ),
                "source_sha256": hashlib.sha256(
                    (
                        release_gate_module.REPO_ROOT
                        / "scripts"
                        / "pnc_rca_kafka_preflight.py"
                    ).read_bytes()
                ).hexdigest(),
                "dependency_versions": {"kafka-python": "3.0.7"},
                "connection_config_sha256": _sha256_json(probe_public),
                "config": probe_public,
                "mode": "release_gate",
                "env_file": env_observation,
                "side_effect_contract": (
                    release_gate_module.KAFKA_PREFLIGHT_SIDE_EFFECT_CONTRACT
                ),
            },
        },
    )
    _write_json(
        evidence_dir / "t0_offsets.json",
        {
            "schema_version": T0_OFFSETS_SCHEMA_VERSION,
            "observed_at": OBSERVED_AT,
            "explicit_t0": True,
            "group_id": "rca_root_cause_analysis_agent",
            "topic": TOPIC,
            "partition_offsets": {"0": 10, "1": 20},
        },
    )
    accepted = _event()
    filtered = _event()
    filtered["project_key"] = "other-project"
    invalid = _event()
    invalid.pop("name")
    _write_json(
        evidence_dir / "workflow_fixtures.json",
        {
            "schema_version": WORKFLOW_FIXTURES_SCHEMA_VERSION,
            "observed_at": OBSERVED_AT,
            "topic": TOPIC,
            "creation_rule_version": RULE,
            "fixtures": [
                {
                    "name": "accepted-exact-transition",
                    "topic": TOPIC,
                    "value": accepted,
                    "expected_decision": "accepted",
                    "expected_reason": "creation_policy_matched",
                },
                {
                    "name": "filtered-other-project",
                    "topic": TOPIC,
                    "value": filtered,
                    "expected_decision": "filtered",
                    "expected_reason": "project_key_not_allowed",
                },
                {
                    "name": "invalid-missing-workflow-field",
                    "topic": TOPIC,
                    "value": invalid,
                    "expected_decision": "invalid",
                    "expected_reason": "missing_workflow_fields",
                },
            ],
        },
    )


def _write_kafka_recent_replay_evidence(
    evidence_dir: Path,
    *,
    kafka_env_file: Path,
    host_repo: Path,
    host_commit: str,
    consumer: ConsumerConfig,
) -> None:
    _env, env_observation = load_kafka_preflight_environment(kafka_env_file)
    observed = datetime.fromisoformat(OBSERVED_AT.replace("Z", "+00:00"))
    started = observed - timedelta(days=7)
    e2e_work_item_ids = ["7000000000", "7000000001"]
    e2e_task_id = "fixture-rca-production-readiness"
    screenshot_path = evidence_dir / "kafka-e2e-user-screenshot.jpeg"
    screenshot_path.write_bytes(b"fixture screenshot evidence")
    screenshot_path.chmod(0o644)
    screenshot_sha256 = hashlib.sha256(screenshot_path.read_bytes()).hexdigest()
    frame_census_path = evidence_dir / "kafka-e2e-frame-census.json"
    _write_remote_soak_manifest(
        frame_census_path,
        {
            "schema_version": (
                release_gate_module.KAFKA_FEISHU_FRAME_CENSUS_SCHEMA_VERSION
            ),
            "generated_at": OBSERVED_AT,
            "task_id": e2e_task_id,
            "source": {
                "kind": "official_meegle_read_only",
                "project_key": "t03o4q",
                "work_item_type_key": "issue",
                "selected_field_keys": [
                    "field_1fda45",
                    "field_9193cb",
                    "field_8c912e",
                ],
            },
            "summary": {
                "expected_work_items": len(e2e_work_item_ids),
                "observed_work_items": len(e2e_work_item_ids),
                "fetch_status_counts": {"ok": len(e2e_work_item_ids)},
                "frame_reference_kind_counts": {
                    "front_camera_timestamp": len(e2e_work_item_ids)
                },
                "result_field_nonempty_count": 0,
                "report_field_nonempty_count": 0,
            },
            "records": [
                {
                    "work_item_id": work_item_id,
                    "fetch_status": "ok",
                    "frame_field_present": True,
                    "result_field_present": False,
                    "report_field_present": False,
                    "frame_value_sha256": hashlib.sha256(
                        f"frame:{work_item_id}".encode()
                    ).hexdigest(),
                    "frame_reference_kind": "front_camera_timestamp",
                    "management_timestamp": 1_783_841_476_000_000,
                    "management_timestamp_unit": "microseconds_since_unix_epoch",
                    "timezone": "Asia/Shanghai",
                    "max_delta_us": 100_000,
                }
                for work_item_id in e2e_work_item_ids
            ],
            "contains_raw_field_values": False,
            "contains_credentials": False,
            "mutation_performed": False,
        },
    )
    feishu_receipt_path = evidence_dir / "kafka-e2e-feishu-receipt.json"
    _write_remote_soak_manifest(
        feishu_receipt_path,
        {
            "schema_version": (
                release_gate_module.KAFKA_FEISHU_E2E_RECEIPT_SCHEMA_VERSION
            ),
            "observed_at": OBSERVED_AT,
            "task_id": e2e_task_id,
            "source_class": "live_observation",
            "official_source": {
                "client": "meegle",
                "project_key": "t03o4q",
                "project_simple_name": "g1q3",
                "work_item_type_key": "issue",
                "status_key": "OPEN",
                "source_type": "plugin",
                "plugin_source": "fixture-plugin-source",
                "operation_type": "create",
                "op_record_module": "work_item_mod",
            },
            "user_evidence": {
                "screenshot_path": str(screenshot_path),
                "screenshot_sha256": screenshot_sha256,
            },
            "result": {
                "expected": len(e2e_work_item_ids),
                "work_items_found": len(e2e_work_item_ids),
                "pdcl_field_present": len(e2e_work_item_ids),
                "function_field_present": len(e2e_work_item_ids),
                "frame_field_present": len(e2e_work_item_ids),
                "frame_reference_parseable": len(e2e_work_item_ids),
                "frame_census_path": str(frame_census_path),
                "frame_census_sha256": hashlib.sha256(
                    frame_census_path.read_bytes()
                ).hexdigest(),
                "creation_records_found": len(e2e_work_item_ids),
                "all_exactly_one_creation_record": True,
                "all_identity_fields_match": True,
                "items": [
                    {
                        "work_item_id": work_item_id,
                        "create_time": OBSERVED_AT,
                        "operation_time_ms": int(observed.timestamp() * 1000),
                    }
                    for work_item_id in e2e_work_item_ids
                ],
            },
            "privacy": {
                "credential_persisted": False,
                "person_identity_persisted": False,
                "raw_frame_reference_persisted": False,
                "raw_pdcl_reference_persisted": False,
                "raw_title_persisted": False,
            },
            "side_effects": {
                "feishu_write": False,
                "kafka_read": False,
                "production_mutation": False,
            },
        },
    )
    e2e_source = {
        "project_key": "t03o4q",
        "project_simple_name": "g1q3",
        "work_item_type_key": "issue",
        "feishu_plugin_source": "fixture-plugin-source",
        "feishu_receipt_path": str(feishu_receipt_path),
        "screenshot_sha256": screenshot_sha256,
        "feishu_receipt_sha256": hashlib.sha256(
            feishu_receipt_path.read_bytes()
        ).hexdigest(),
    }
    e2e_manifest_path = evidence_dir / "kafka-e2e-canary-manifest.json"
    _write_remote_soak_manifest(
        e2e_manifest_path,
        {
            "schema_version": (
                release_gate_module.KAFKA_E2E_CANARY_MANIFEST_SCHEMA_VERSION
            ),
            "generated_at": OBSERVED_AT,
            "task_id": e2e_task_id,
            "source": e2e_source,
            "work_item_ids": e2e_work_item_ids,
            "required_kafka_evidence": {
                "window_days": 7,
                "all_work_items_observed": True,
                "all_work_items_accepted": True,
                "all_triggers_created_or_deduplicated": True,
                "shadow_outbox_only": True,
                "commit_performed": False,
            },
            "contains_raw_title": False,
            "contains_raw_pdcl_reference": False,
            "contains_credential": False,
        },
    )
    e2e_manifest_raw = e2e_manifest_path.read_bytes()
    raw_values = [b'{"fixture":"real-kafka-0"}', b'{"fixture":"real-kafka-1"}']
    records = [
        {
            "partition": partition,
            "offset": offset,
            "timestamp_ms": int(observed.timestamp() * 1000),
            "value_bytes": len(raw),
            "value_sha256": hashlib.sha256(raw).hexdigest(),
            "decision": "accepted",
            "reason": "creation_policy_matched",
            "event_uid_sha256": hashlib.sha256(
                f"{TOPIC}:{partition}:{offset}".encode()
            ).hexdigest(),
            "business_key_sha256": hashlib.sha256(
                f"business-{partition}".encode()
            ).hexdigest(),
            "submission_key_sha256": hashlib.sha256(
                f"submission-{partition}".encode()
            ).hexdigest(),
            "trigger_created": True,
            "outbox_created": True,
            "expected_e2e_work_item": True,
        }
        for partition, offset, raw in ((0, 10, raw_values[0]), (1, 20, raw_values[1]))
    ]
    module = host_repo / release_gate_module.KAFKA_RECENT_REPLAY_MODULE
    receipt = {
        "schema_version": release_gate_module.KAFKA_RECENT_REPLAY_SCHEMA_VERSION,
        "observed_at": OBSERVED_AT,
        "source": {
            "component": "pnc_rca_kafka_recent_replay",
            "component_commit": host_commit,
            "module": release_gate_module.KAFKA_RECENT_REPLAY_MODULE,
            "module_sha256": hashlib.sha256(module.read_bytes()).hexdigest(),
            "committed_match": True,
            "module_clean": True,
        },
        "config": {
            "topic": TOPIC,
            "cluster_binding": {
                "bootstrap_servers_sha256": _sha256_json(
                    list(consumer.bootstrap_servers)
                ),
                "principal_sha256": _sha256_json(consumer.username),
                "env_file": env_observation,
            },
            "policy_sha256": _sha256_json(consumer.policy.to_dict()),
        },
        "window": {
            "days": 7,
            "started_at": started.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "ended_at": OBSERVED_AT,
            "start_timestamp_ms": int(started.timestamp() * 1000),
            "partitions": [
                {
                    "partition": partition,
                    "beginning_offset": 0,
                    "window_start_offset": offset,
                    "fixed_end_offset": offset + 1,
                    "records_scanned": 1,
                }
                for partition, offset in ((0, 10), (1, 20))
            ],
        },
        "limits": {
            "max_messages": 5000,
            "max_bytes": 64 * 1024 * 1024,
            "max_seconds": 120,
        },
        "transport": {
            "assignment": "explicit",
            "group_id": None,
            "subscribed": False,
            "group_joined": False,
            "enable_auto_commit": False,
            "commit_performed": False,
            "allow_auto_create_topics": False,
            "isolation_level": "read_committed",
            "request_timeout_ms": 5000,
            "bootstrap_timeout_ms": 5000,
        },
        "result": {
            "stop_reason": "partition_end_offsets_reached",
            "records_scanned": 2,
            "raw_bytes_scanned": sum(len(raw) for raw in raw_values),
            "decision_counts": {"accepted": 2},
            "reason_counts": {"creation_policy_matched": 2},
            "records": records,
            "shadow_store": {
                "inbox": {"accepted": 2},
                "outbox": {"shadow": 2},
                "replay_raw_retained": {"count": 0, "bytes": 0},
            },
            "production_mutation_performed": False,
            "raw_payload_persisted_to_output": False,
            "temporary_store_destroyed": True,
            "e2e_canary": {
                "required": True,
                "manifest": {
                    "path": str(e2e_manifest_path),
                    "sha256": hashlib.sha256(e2e_manifest_raw).hexdigest(),
                    "mode": "0600",
                    "size": len(e2e_manifest_raw),
                    "work_item_count": len(e2e_work_item_ids),
                    "source": e2e_source,
                },
                "expected_work_item_ids": e2e_work_item_ids,
                "matched_work_item_ids": e2e_work_item_ids,
                "missing_work_item_ids": [],
                "unexpected_accepted_work_items": 0,
                "complete": True,
            },
        },
    }
    _write_remote_soak_manifest(
        evidence_dir / release_gate_module.KAFKA_RECENT_REPLAY_FILENAME,
        receipt,
    )


def _write_canary_evidence(
    evidence_dir: Path,
    host_repo: Path,
    host_commit: str,
    workspace_repo: Path,
    workspace_commit: str,
    vm_repo: Path,
    vm_commit: str,
    vm_worker_repo: Path,
    vm_worker_commit: str,
    runtime_config_sha256: str,
    control_db_path: Path,
    delivery_db_path: Path,
    release_id: str,
    bootstrap_epoch_id: str,
) -> None:
    _write_build_manifest(
        evidence_dir,
        host_repo,
        host_commit,
        workspace_repo,
        workspace_commit,
        vm_repo,
        vm_commit,
        vm_worker_repo,
        vm_worker_commit,
        runtime_config_sha256,
    )
    _write_json(evidence_dir / "remote_reader_health.json", _remote_reader_health())
    from scripts.pnc_rca_store_migration_drill import run_migration_drill

    migration_observed_at = NOW - timedelta(seconds=30)
    writer_stop = {
        "schema_version": "pnc_rca_writer_stop_evidence_v1",
        "observed_at": migration_observed_at.isoformat(),
        "services": {
            label: {
                "observed_at": migration_observed_at.isoformat(),
                "pid_state": "pid_absent",
                "health_state": "stopped",
            }
            for label in release_gate_module.STORE_WRITER_LABELS
        },
    }
    from scripts import pnc_rca_store_migration_drill as migration_module

    migration = run_migration_drill(
        control_db_path=control_db_path,
        delivery_db_path=delivery_db_path,
        work_dir=evidence_dir.parent / "store-migration-work",
        evidence_dir=evidence_dir,
        writer_stop_evidence=writer_stop,
        now=migration_observed_at,
        writer_process_probe=lambda: {
            label: {
                "launchd_job_state": "absent",
                "matching_pids": [],
            }
            for label in release_gate_module.STORE_WRITER_LABELS
        },
        predecessor_validator_path=(
            host_repo / PREDECESSOR_VALIDATOR_RELATIVE_PATH
        ),
        repo_root=host_repo,
    )
    migration["candidate"] = {
        "repo_root": str(host_repo.resolve()),
        "commit": host_commit,
        "migration_sources": {
            relative: hashlib.sha256((host_repo / relative).read_bytes()).hexdigest()
            for relative in release_gate_module.MIGRATION_SOURCE_RELATIVE_PATHS
        },
    }
    _write_json(evidence_dir / "store_migration_receipt.json", migration)
    process_probe = lambda: {
        label: {
            "launchd_job_state": "absent",
            "matching_pids": [],
        }
        for label in release_gate_module.STORE_WRITER_LABELS
    }
    if migration["migration_state"] == "fresh_install":
        migration_module.materialize_fresh_install(
            migration_receipt_path=(
                evidence_dir / "store_migration_receipt.json"
            ),
            control_db_path=control_db_path,
            delivery_db_path=delivery_db_path,
            config_sha256=runtime_config_sha256,
            evidence_dir=evidence_dir,
            writer_stop_evidence=writer_stop,
            apply=True,
            now=migration_observed_at,
            writer_process_probe=process_probe,
            release_id=release_id,
            bootstrap_epoch_id=bootstrap_epoch_id,
            operator="release-test",
            reason="materialize release gate fresh database",
        )
    else:
        migration_module.initialize_existing_capacity_transition(
            migration_receipt_path=(
                evidence_dir / "store_migration_receipt.json"
            ),
            control_db_path=control_db_path,
            delivery_db_path=delivery_db_path,
            evidence_dir=evidence_dir,
            writer_stop_evidence=writer_stop,
            release_id=release_id,
            bootstrap_epoch_id=bootstrap_epoch_id,
            apply=True,
            now=migration_observed_at,
            writer_process_probe=process_probe,
            operator="release-test",
            reason="initialize release gate existing database capacity latch",
        )
    _write_json(
        evidence_dir / "cutover_plan.json",
        {
            "schema_version": CUTOVER_PLAN_SCHEMA_VERSION,
            "observed_at": OBSERVED_AT,
            "approved": True,
            "legacy_entry_mode": "read_only",
            "legacy_auto_execution_disabled": True,
            "legacy_daily_quota": 0,
            "legacy_governance_download_enabled": False,
            "data_access_mode": "remote_read",
            "mdi_download_allowed": False,
            "input_materialization": "forbidden",
            "legacy_storage_reservation_enabled": False,
            "derived_capacity_reservation_enabled": True,
            "derived_capacity_atomic_reservation": True,
            "delivery_collector_enabled": True,
            "delivery_dispatcher_enabled": True,
            "rollback": {
                "owner": "release-owner",
                "procedure": "restore reviewed legacy launchd and quota configuration",
                "max_restore_seconds": 300,
            },
        },
    )
    _write_json(
        evidence_dir / "shadow_soak.json",
        {
            "schema_version": SHADOW_SOAK_SCHEMA_VERSION,
            "observed_at": OBSERVED_AT,
            "ok": True,
            "error_count": 0,
            "duration_seconds": 86_400,
            "records_seen": 1000,
            "records_committed": 1000,
            "decision_counts": {
                "accepted": 220,
                "filtered": 760,
                "invalid": 10,
                "deduped": 10,
            },
            "build_manifest_sha256": "pending-fixture-binding",
            "config_sha256": "pending-fixture-binding",
            "burst_records": 100,
            "burst_duration_seconds": 60,
            "consumer_lag_peak": 100,
            "consumer_lag_end": 0,
            "lag_recovery_seconds": 30.0,
            "ingest_commit_latency_ms": {
                "p50": 25.0,
                "p95": 100.0,
                "p99": 250.0,
                "max": 800.0,
            },
            "process_rss_peak_bytes": 128 * 1024 * 1024,
            "restart_count": 1,
            "recovered_pending_records": 1,
            "ingest_errors": 0,
            "commit_errors": 0,
            "pending_records": 0,
            "rebalance_callback_errors": 0,
            "assignment_count": 1,
            "assigned_partitions": [0, 1],
            "control_db_measurement_scope": "sqlite_db_wal_shm_total",
            "control_db_bytes_start": 1_000_000,
            "control_db_bytes_end": 1_100_000,
            "projected_db_growth_bytes_per_day": 2_400_000,
            "control_db_free_bytes": 240_000_000,
            "projected_storage_horizon_days": 100,
        },
    )
    _write_json(
        evidence_dir / "canary_plan.json",
        {
            "schema_version": CANARY_PLAN_SCHEMA_VERSION,
            "observed_at": OBSERVED_AT,
            "approved": True,
            "admission_mode": "direct_bounded",
            "promotion_budget": 0,
            "slot_count": 3,
            "slots": {
                slot_kind: {
                    "source_kind": (
                        "kafka" if slot_kind == "kafka_success" else "manual"
                    ),
                    "entrypoint": (
                        "kafka_ingest"
                        if slot_kind == "kafka_success"
                        else "manual_admit"
                    ),
                    "source_identity": dict(source_identity),
                    "source_identity_sha256": release_gate_module._sha256_json(
                        {
                            **source_identity,
                            **(
                                {
                                    "topic": TOPIC,
                                    "partition": 0,
                                    "offset": 10,
                                }
                                if slot_kind == "kafka_success"
                                else {}
                            ),
                        }
                    ),
                    "max_admissions": 1,
                    "expected_admission": ACTIVATION_SLOT_ADMISSIONS[slot_kind],
                    "expected_outcome": (
                        "terminal_failed"
                        if slot_kind == "manual_terminal_failure"
                        else "success"
                    ),
                }
                for slot_kind, source_identity in ACTIVATION_SLOT_IDENTITIES.items()
            },
            "topic": TOPIC,
            "creation_rule_version": RULE,
            "event_uid": EVENT_UID,
            "admission": CANARY_ADMISSION.to_dict(),
            "operator": "release-owner",
            "reason": "single bounded production canary",
            "execution_request": _remote_execution_request(),
            "requested_scope": _requested_scope(),
        },
    )
    remote_health_path = evidence_dir / "remote_reader_health.json"
    soak, workload_manifest = _remote_reader_soak(
        vm_commit=vm_commit,
        vm_tree=_git(vm_repo, "rev-parse", "HEAD^{tree}"),
        remote_reader_health_sha256=hashlib.sha256(
            remote_health_path.read_bytes()
        ).hexdigest(),
    )
    exporter_sha256 = hashlib.sha256(
        (host_repo / release_gate_module.REMOTE_READER_WORKLOAD_EXPORT_MODULE).read_bytes()
    ).hexdigest()
    workload_manifest["source"] = {
        "generation_mode": "machine_generated",
        "component": "rca_issue_workload_export",
        "component_commit": host_commit,
        "artifact_sha256": exporter_sha256,
    }
    soak["workload_manifest"]["source"] = dict(workload_manifest["source"])
    soak["workload_manifest"]["sha256"] = _remote_soak_sha256(workload_manifest)
    _write_json(
        evidence_dir / "remote_reader_soak.json",
        soak,
    )
    _write_remote_soak_manifest(
        evidence_dir / release_gate_module.REMOTE_READER_SOAK_MANIFEST_FILENAME,
        workload_manifest,
    )
    workload_provenance = _remote_reader_workload_provenance(workload_manifest)
    for name, filename in (
        ("census", release_gate_module.REMOTE_READER_WORKLOAD_CENSUS_FILENAME),
        ("mapping", release_gate_module.REMOTE_READER_DOMAIN_MAPPING_FILENAME),
        (
            "approval",
            release_gate_module.REMOTE_READER_DOMAIN_MAPPING_APPROVAL_FILENAME,
        ),
        (
            "receipt",
            release_gate_module.REMOTE_READER_WORKLOAD_EXPORT_RECEIPT_FILENAME,
        ),
    ):
        _write_remote_soak_manifest(
            evidence_dir / filename,
            workload_provenance[name],
        )


def _remote_read_canary_receipt(execution_request: dict) -> dict:
    requirements = _requested_scope()["requirements"]
    runtime_source = _remote_reader_health()["runtime"]
    runtime = {
        key: runtime_source[key]
        for key in (
            "execution_mode",
            "dependency_domain",
            "required_python_executable",
            "resolved_required_python_executable",
            "python_executable",
            "python_version",
            "reader_import_scope",
            "timeout_seconds",
        )
    }
    observed_channels = [
        "kvaser.0.can1.Vehicle",
        "sigmastar.1.dds2.vehicle_signal_highfreq",
    ]
    receipt = {
        "schema_version": "g1q3_rca_remote_read_receipt_v1",
        "mode": "remote_read",
        "read_strategy": "bounded_remote_stream_cache",
        "status": "completed",
        "completeness": "full_requested_scope",
        "exhausted": True,
        "bounded": False,
        "requirements_hash": requirements["requirements_hash"],
        "requirements_contract_version": requirements["requirements_contract_version"],
        "requirements_contract_hash": requirements["requirements_contract_hash"],
        "function_domain": requirements["function_domain"],
        "requested_topics": requirements["requested_topics"],
        "requested_channels": requirements["channel_allowlist"],
        "observed_channels": observed_channels,
        "topic_coverage": {
            "kvaser.0.can1.Vehicle": {
                "allowed": [
                    "kvaser.0.can1.Debug",
                    "kvaser.0.can1.Vehicle",
                    "kvaser.0.can2.Debug",
                    "kvaser.1.can1.Debug",
                    "kvaser.1.can1.Vehicle",
                    "kvaser.1.can2.Debug",
                ],
                "observed": ["kvaser.0.can1.Vehicle"],
                "complete": True,
                "required": True,
            },
            "vehicle_signal_highfreq": {
                "allowed": [
                    "sigmastar.1.dds2.vehicle_signal_highfreq",
                    "sigmastar~1~dds2.vehicle_signal_highfreq",
                ],
                "observed": ["sigmastar.1.dds2.vehicle_signal_highfreq"],
                "complete": True,
                "required": True,
            },
        },
        "requested_window": requirements["requested_window"],
        "reference_count": 1,
        "references": [
            {
                "kind": "event",
                "reader_class": "RemoteEventReader",
                "reference_sha256": "a" * 64,
                "sampled_messages": 250,
                "scanned_messages": 300,
                "status": "exhausted",
                "exhausted": True,
                "bounded": False,
                "scope_proof": {
                    "schema_version": "g1q3_rca_remote_scope_proof_v1",
                    "kind": "event",
                    "complete": True,
                    "limit_policy": "reject_over_limit",
                    "constructor_guard": "remote_event_clip_limit_exceeded",
                    "reader_source_sha256": "b" * 64,
                    "clip_count": 5,
                    "max_clips": 16,
                },
            }
        ],
        "sampled_messages": 250,
        "scanned_messages": 300,
        "limits": {
            "max_clips": 16,
            "max_messages": 250_000,
            "max_scanned_messages": 1_000_000,
            "max_output_bytes": 750_000_000,
            "timeout_seconds": 120,
        },
        "input_materialized": False,
        "mdi_download_attempted": False,
        "fallback_used": False,
        "derived_artifact_written": True,
        "downstream_full_rca_supported": True,
        "derived_stream_cache": {
            "schema_version": "g1q3_rca_remote_stream_cache_v1",
            "status": "committed",
            "kind": "derived_remote_stream_cache",
            "path": (
                execution_request["data"]["artifact_root"]
                + "s2_remote_read/derived_remote_stream.mcap"
            ),
            "sha256": "c" * 64,
            "bytes": 100_000_000,
            "message_count": 250,
            "schema_count": 2,
            "channel_count": 2,
            "atomic": True,
            "durable": True,
            **_cifs_storage_contract(),
        },
        "dependency": {
            "status": "ready",
            "distribution": "pdcl_pyclip",
            "actual_version": "0.1.6+rca.2",
            "runtime": runtime,
        },
        "execution_runtime": runtime,
        "data_access": {
            "schema_version": "g1q3_rca_remote_data_access_v1",
            "mode": "remote_read",
            "transport": "pdcl_pyclip",
            "reference_count": 1,
            "reference_kinds": ["event"],
            "source": dict(execution_request["data"]["data_access"]["source"]),
            "reader_contract": dict(
                execution_request["data"]["data_access"]["reader_contract"]
            ),
        },
    }
    return {
        "reader_fingerprint": _remote_reader_health()["reader_fingerprint"],
        "receipt_sha256": _sha256_json(receipt),
        "receipt": receipt,
    }


def _pipeline_capacity_canary_receipts(
    execution_request: dict,
    remote_read: dict,
    capacity_lifecycle: dict,
    *,
    run_id: str = WORKER_RUN_ID,
    artifact_set_id: str = ARTIFACT_SET_ID,
    manifest_artifact: dict | None = None,
    index_artifact: dict | None = None,
    report_data_artifact: dict | None = None,
) -> tuple[dict, dict]:
    artifact_root = execution_request["data"]["artifact_root"]
    active = capacity_lifecycle["full_receipts"]["activate"]
    reservation = active["reservation"]
    limits = {
        "tmp_bytes": reservation["requested_bytes"]["tmp"],
        "hfs_bytes": reservation["requested_bytes"]["hfs"],
    }
    stage_specs = (
        (
            "s2_remote_read",
            "2026-07-10T07:55:20+00:00",
            "s2_remote_read/remote_read_receipt.json",
            100_000_000,
            0,
        ),
        (
            "s3a_materialize",
            "2026-07-10T07:55:35+00:00",
            stage_lineage_relative_path("s3a"),
            220_000_000,
            40_000_000,
        ),
        (
            "s3b_translate",
            "2026-07-10T07:56:00+00:00",
            stage_lineage_relative_path("s3b"),
            600_000_000,
            180_000_000,
        ),
        (
            "s45_auto_keyframe",
            "2026-07-10T07:56:20+00:00",
            stage_lineage_relative_path("s45"),
            610_000_000,
            260_000_000,
        ),
        (
            "s5_alignment",
            "2026-07-10T07:56:40+00:00",
            stage_lineage_relative_path("s5"),
            620_000_000,
            420_000_000,
        ),
        (
            "s6_report",
            "2026-07-10T07:57:00+00:00",
            stage_lineage_relative_path("s6"),
            625_000_000,
            500_000_000,
        ),
    )
    stages = {}
    for name, finished_at, relative, tmp_bytes, hfs_bytes in stage_specs:
        stages[name] = {
            "status": "completed",
            "finished_at": finished_at,
            "artifact_receipt_path": artifact_root + relative,
            "observed_bytes": {"tmp": tmp_bytes, "hfs": hfs_bytes},
            "delta_bytes": {"tmp": tmp_bytes, "hfs": hfs_bytes},
            "peak_delta_bytes": {"tmp": tmp_bytes, "hfs": hfs_bytes},
            "within_budget": True,
        }
    terminal = {
        "tmp_cache": {
            "state": "retained",
            "bytes": 150_000_000,
            "cleanup_policy": ("retain_until_pipeline_terminal_then_governed_cleanup"),
        },
        "hfs_artifacts": {
            "state": "retained",
            "bytes": 500_000_000,
            "admission_policy": "live_free_space_after_reservation_release",
        },
    }
    meter = {
        "schema_version": "g1q3_rca_stage_capacity_meter_v2",
        "status": "completed",
        "accounting": {
            "mode": "exclusive_tmp_hfs_total_v2",
            "tmp_root": artifact_root.rstrip("/"),
            "hfs_root": artifact_root.rstrip("/") + "/cases/G1Q3-1",
            "relationship": "hfs_nested_in_tmp",
        },
        "identity": {
            "reservation_id": active["reservation_id"],
            "submission_key": SUBMISSION_KEY,
            "fence": active["fence"],
            "contract_sha256": active["contract_sha256"],
            "run_id": SUBMISSION_KEY,
        },
        "limits": limits,
        "baseline": {
            "created_at": "2026-07-10T07:55:05+00:00",
            "tmp_bytes": 0,
            "hfs_bytes": 0,
        },
        "stages": stages,
        "peaks": {"tmp_bytes": 625_000_000, "hfs_bytes": 500_000_000},
        "within_budget": True,
        "terminal": terminal,
    }
    meter_sha256 = _sha256_json(meter)
    capacity_meter = {
        "path": artifact_root + "derived_capacity_usage_receipt.json",
        "sha256": meter_sha256,
        "receipt": meter,
    }
    manifest_artifact = manifest_artifact or {
        "kind": "delivery_manifest",
        "path": artifact_root + "delivery_manifest.json",
        "bytes": len(DELIVERY_MANIFEST_RAW),
        "sha256": MANIFEST_SHA256,
    }
    index_artifact = index_artifact or {
        "kind": "index_html",
        "path": artifact_root + "index.html",
        "bytes": 2048,
        "sha256": INDEX_SHA256,
    }
    report_data_artifact = report_data_artifact or {
        "kind": "report_data",
        "path": artifact_root + "report_data.json",
        "bytes": 4096,
        "sha256": REPORT_DATA_SHA256,
    }
    identity = {
        "task_id": SUBMISSION_KEY,
        "submission_key": SUBMISSION_KEY,
        "run_id": run_id,
        "artifact_set_id": artifact_set_id,
        "request_sha256": _sha256_execution_request(execution_request),
        "rca_contract_sha256": vm_task_tool.canonical_rca_contract_sha256(
            CANARY_ADMISSION.to_dict(), execution_request
        ),
    }
    remote_cache = remote_read["receipt"]["derived_stream_cache"]
    previous_outputs = [
        {
            "kind": "derived_remote_stream_cache",
            "path": remote_cache["path"],
            "bytes": remote_cache["bytes"],
            "sha256": remote_cache["sha256"],
        }
    ]
    downstream = {}
    for short, full in release_gate_module.RCA_STAGE_NAME_BY_SHORT.items():
        inputs = copy.deepcopy(previous_outputs)
        if short == "s6":
            outputs = [
                copy.deepcopy(manifest_artifact),
                copy.deepcopy(index_artifact),
                copy.deepcopy(report_data_artifact),
            ]
        else:
            outputs = [
                {
                    "kind": f"{short}_derived_output",
                    "path": artifact_root + f"stage_outputs/{short}.bin",
                    "bytes": 10_000_000 + len(downstream) * 1_000_000,
                    "sha256": hashlib.sha256(
                        f"{short}-derived-output".encode("utf-8")
                    ).hexdigest(),
                }
            ]
        lineage = {
            "schema_version": RCA_STAGE_LINEAGE_SCHEMA_VERSION,
            "status": "completed",
            "stage": full,
            "finished_at": stages[full]["finished_at"],
            "identity": copy.deepcopy(identity),
            "upstream_output_artifact_set_sha256": (
                canonical_artifact_set_sha256(previous_outputs)
            ),
            "input_artifacts": inputs,
            "input_artifact_set_sha256": canonical_artifact_set_sha256(inputs),
            "output_artifacts": outputs,
            "output_artifact_set_sha256": canonical_artifact_set_sha256(outputs),
            "execution_policy": dict(RCA_STAGE_EXECUTION_POLICY),
        }
        downstream[short] = {
            "status": "completed",
            "finished_at": stages[full]["finished_at"],
            "artifact_receipt_path": stages[full]["artifact_receipt_path"],
            "artifact_receipt_sha256": _sha256_json(lineage),
            "lineage": lineage,
        }
        previous_outputs = outputs
    pipeline = {
        "status": "report_generated_need_review",
        "stage": "s6_report",
        "blocker": None,
        "remote_read_receipt": {
            "path": artifact_root + "s2_remote_read/remote_read_receipt.json",
            "sha256": remote_read["receipt_sha256"],
        },
        "remote_stream_cache": dict(remote_read["receipt"]["derived_stream_cache"]),
        "downstream_stage_receipts": downstream,
        "capacity_usage": {
            "path": capacity_meter["path"],
            "sha256": meter_sha256,
            "status": "completed",
            "within_budget": True,
            "limits": limits,
            "peaks": dict(meter["peaks"]),
            "terminal": terminal,
        },
    }
    return pipeline, capacity_meter


def _vm_execution_canary_receipt(
    execution_request: dict,
    *,
    vm_commit: str,
    vm_worker_commit: str,
    vm_service_entrypoint_sha256: str,
    vm_worker_entrypoint_sha256: str,
    capacity_mode: str = "steady",
    release_bom_sha256: str = "",
) -> dict:
    artifact_root = execution_request["data"]["artifact_root"]
    artifact_cifs_root = execution_request["data"]["artifact_cifs_root"]
    task_id = execution_request["source_refs"]["submission_key"]
    goal_path = f"/home/mini/.hermes/shared-state/tasks/{task_id}/goal.md"
    run_id = WORKER_RUN_ID
    worker_pid = 4242
    argv = [
        "./api/g1q3_rca/scripts/run_rca_service_request.py",
        "--task-id",
        task_id,
        "--goal-path",
        goal_path,
    ]
    dispatched_at = "2026-07-10T07:56:59+00:00"
    process_started_at = "2026-07-10T07:57:00+00:00"
    dispatch_receipt = {
        "schema_version": "g1q3_rca_worker_dispatch_receipt_v1",
        "task_id": task_id,
        "run_id": run_id,
        "argv": argv,
        "cwd": "/home/mini/data3/yj-evaluation-server",
        "dispatched_at": dispatched_at,
        "process_started_at": process_started_at,
        "worker_pid": worker_pid,
    }
    dispatch_receipt_sha256 = _sha256_json(dispatch_receipt)
    goal_sha256 = hashlib.sha256(
        vm_task_tool.build_rca_fixed_cli_goal(
            task_id=task_id,
            admission=CANARY_ADMISSION.to_dict(),
            execution_request=execution_request,
        ).encode("utf-8")
    ).hexdigest()
    capacity_admission = {
        "resource_class": "rca_prod",
        "capacity_mode": capacity_mode,
        "task_meta_sha256": "a" * 64,
        "admission_receipt_sha256": "b" * 64,
        "admission_schema_version": (
            release_gate_module.prod_admission.BOOTSTRAP_SCHEMA_VERSION
            if capacity_mode == "bootstrap"
            else release_gate_module.prod_admission.SCHEMA_VERSION
        ),
        "admission_key_fingerprint": "c" * 64,
        "queue_if_blocked": False,
        "resource_gate_bypass": False,
    }
    if capacity_mode == "bootstrap":
        capacity_admission.update({
            "bootstrap_epoch_id": "rca-bootstrap-release-20260710",
            "bootstrap_started_at": (NOW - timedelta(days=1)).isoformat(),
            "bootstrap_deadline": (NOW + timedelta(days=7)).isoformat(),
            "bootstrap_authorization_fingerprint": "d" * 64,
            "release_bom_sha256": release_bom_sha256,
            "release_approval_id": "release-approval-20260710",
            "max_concurrency": release_gate_module.prod_bootstrap.MAX_CONCURRENCY,
            "daily_started_attempt_quota": (
                release_gate_module.prod_bootstrap.DAILY_STARTED_ATTEMPT_QUOTA
            ),
            "quota_timezone": release_gate_module.prod_bootstrap.QUOTA_TIMEZONE,
            "root_required_available_bytes": (
                release_gate_module.prod_bootstrap.ROOT_REQUIRED_AVAILABLE_BYTES
            ),
            "delivery_required_available_bytes": (
                release_gate_module.prod_bootstrap.DELIVERY_REQUIRED_AVAILABLE_BYTES
            ),
        })
    execution_attestation = {
        "schema_version": "g1q3_rca_worker_execution_attestation_v2",
        "available": True,
        "executor_type": "direct_cli",
        "agent_backend": "none",
        "codex_backend_enabled": False,
        "coding_agent_fallback_enabled": False,
        "openclaw_invocation_count": 0,
        "codex_invocation_count": 0,
        "fallback_invocation_count": 0,
        "worker_source_commit": vm_worker_commit,
        "worker_tree_clean": True,
        "worker_entrypoint_path": (
            "/home/mini/.hermes/worker-state/vm_coding_worker_v2.py"
        ),
        "worker_entrypoint_sha256": vm_worker_entrypoint_sha256,
        "argv": argv,
        "cwd": "/home/mini/data3/yj-evaluation-server",
        "dispatched_at": dispatched_at,
        "process_started_at": process_started_at,
        "task_id": task_id,
        "run_id": run_id,
        "worker_pid": worker_pid,
        "dispatch_receipt_sha256": dispatch_receipt_sha256,
    }
    service_result = {
        "schema_version": "g1q3_rca_service_result_v2",
        "task_id": task_id,
        "status": "completed",
        "success": True,
        "goal_sha256": goal_sha256,
        "request_sha256": _sha256_execution_request(execution_request),
        "request_path": artifact_root + "rca_execution_request.json",
        "output_dir": artifact_root.rstrip("/"),
        "artifact_cifs_root": artifact_cifs_root,
        "pipeline_result_path": artifact_root + "pipeline_result.json",
        "pipeline_status": "report_generated_need_review",
        "pipeline_stage": "s6_report",
        "blocker": None,
        "generated_at": "2026-07-10T07:57:05+00:00",
        "worker_run_id": run_id,
        "worker_pid": worker_pid,
        "dispatch_receipt_sha256": dispatch_receipt_sha256,
        "request_storage": {
            "schema_version": "g1q3_rca_storage_file_receipt_v1",
            "path": artifact_root + "rca_execution_request.json",
            "sha256": "d" * 64,
            "bytes": 4096,
            **_cifs_storage_contract(),
        },
        **_cifs_storage_contract(),
        "service_provenance": {
            "schema_version": "g1q3_rca_service_provenance_v1",
            "available": True,
            "vm_source_commit": vm_commit,
            "vm_tree_clean": True,
            "service_entrypoint_path": (
                "/home/mini/data3/yj-evaluation-server/"
                "api/g1q3_rca/scripts/run_rca_service_request.py"
            ),
            "service_entrypoint_sha256": vm_service_entrypoint_sha256,
        },
    }
    worker_result_payload = {
        "schema_version": "g1q3_rca_worker_result_v1",
        "task_id": task_id,
        "run_id": run_id,
        "repo_root": "/home/mini/data3/yj-evaluation-server",
        "canonical_task_dir": f"/home/mini/.hermes/shared-state/tasks/{task_id}",
        "goal_path": goal_path,
        "host_inbox_root": "/home/mini/.hermes/shared-state/inbox",
        "command": argv,
        "runner_log": (
            f"/home/mini/.hermes/worker-state/tasks/{task_id}/artifacts/runner.log"
        ),
        "artifact_root": artifact_root.rstrip("/"),
        "artifacts": [],
        "result_mode": "structured-result-artifact-only",
        "report_contract": "V4-V8",
        "allowed_model_chain": [
            "sub2api/gpt-5.5",
            "vtok/claude-opus-4-6",
        ],
        "execution_route": "g1q3_rca_direct_cli",
        "execution_attestation": execution_attestation,
        "rca_submission_key": task_id,
        "rca_business_key": CANARY_ADMISSION.business_key,
        "rca_generation": CANARY_ADMISSION.generation,
        "rca_contract_sha256": vm_task_tool.canonical_rca_contract_sha256(
            CANARY_ADMISSION.to_dict(), execution_request
        ),
        "rca_source_refs": dict(CANARY_ADMISSION.to_dict()["source_refs"]),
    }
    worker_result = {
        "task_id": task_id,
        "state": "completed",
        "completed_at": "2026-07-10T07:57:06+00:00",
        "exit_code": 0,
        "summary": f"{task_id} completed",
        "result": worker_result_payload,
    }
    return {
        "run_id": run_id,
        "dispatch_receipt_sha256": dispatch_receipt_sha256,
        "capacity_admission": capacity_admission,
        "execution_plane": {
            "lane": "heavy",
            "resource_class": "rca_prod",
            "risk_class": "high",
            "executor_type": "direct_cli",
            "agent_backend": "none",
            "codex_backend_enabled": False,
            "coding_agent_fallback_enabled": False,
            "fixed_cli_entrypoint": (
                "/home/mini/data3/yj-evaluation-server/"
                "api/g1q3_rca/scripts/run_rca_service_request.py"
            ),
            "vm_worker_commit": vm_worker_commit,
            "cwd": "/home/mini/data3/yj-evaluation-server",
            "argv": argv,
            "agent_invocation_count": 0,
            "fallback_invocation_count": 0,
        },
        "execution_attestation": execution_attestation,
        "worker_result": {
            "path": (
                f"/home/mini/.hermes/worker-state/tasks/{task_id}/local-result.json"
            ),
            "sha256": _sha256_json(worker_result),
            "receipt": worker_result,
        },
        "service_result": {
            "path": artifact_root + "rca_service_result.json",
            "sha256": _sha256_json(service_result),
            "receipt": service_result,
        },
    }


def _write_production_evidence(
    evidence_dir: Path,
    *,
    vm_commit: str,
    vm_worker_commit: str,
    vm_service_entrypoint_sha256: str,
    vm_worker_entrypoint_sha256: str,
    capacity_mode: str = "steady",
    release_bom_sha256: str = "",
) -> None:
    execution_request = _remote_execution_request()
    execution_request_sha256 = _sha256_execution_request(execution_request)
    capacity_lifecycle = _derived_capacity_lifecycle()
    remote_read = _remote_read_canary_receipt(execution_request)
    pipeline, capacity_meter = _pipeline_capacity_canary_receipts(
        execution_request, remote_read, capacity_lifecycle
    )
    vm_execution = _vm_execution_canary_receipt(
        execution_request,
        vm_commit=vm_commit,
        vm_worker_commit=vm_worker_commit,
        vm_service_entrypoint_sha256=vm_service_entrypoint_sha256,
        vm_worker_entrypoint_sha256=vm_worker_entrypoint_sha256,
        capacity_mode=capacity_mode,
        release_bom_sha256=release_bom_sha256,
    )
    reserved_receipt_sha256 = _sha256_json(
        capacity_lifecycle["full_receipts"]["reserved"]
    )
    lifecycle_sha256 = _sha256_json(capacity_lifecycle)
    requested_urls = [REPORT_URL]
    request_list_sha256 = _sha256_json(requested_urls)
    zero_counts = {
        "unmanifested_request_count": 0,
        "executable_script_count": 0,
        "inline_event_handler_count": 0,
        "external_active_document_count": 0,
        "console_error_count": 0,
        "runtime_exception_count": 0,
        "log_error_count": 0,
        "network_error_count": 0,
    }
    viewports = {}
    for name, width, height, scale, mobile in (
        ("desktop", 1440, 1000, 1.0, False),
        ("mobile", 390, 844, 3.0, True),
    ):
        viewports[name] = {
            "name": name,
            "width": width,
            "height": height,
            "device_scale_factor": scale,
            "mobile": mobile,
            "nonblank": True,
            "request_count": 1,
            "requested_urls": requested_urls,
            "request_list_sha256": request_list_sha256,
            "unmanifested_urls": [],
            **zero_counts,
            "visible_element_count": 20,
            "visible_text_length": 200,
            "visible_media_count": 1,
            "document_width": width,
            "document_height": height,
            "title_sha256": "f" * 64,
            "index_html_sha256": INDEX_SHA256,
            "index_html_size_bytes": 2048,
        }
    browser_smoke = {
        "schema_version": "pnc_rca_html_browser_smoke_v2",
        "observed_at": OBSERVED_AT,
        "ok": True,
        "machine_generated": True,
        "source": "chromium_cdp_network_runtime_log",
        "engine": "chromium",
        "artifact_policy": "passive_static_html_v1",
        "artifact_set_id": ARTIFACT_SET_ID,
        "report_url": REPORT_URL,
        "index_html_sha256": INDEX_SHA256,
        "manifest_sha256": MANIFEST_SHA256,
        "delivery_contract_sha256": "e" * 64,
        "manifest_url_count": 2,
        "manifest_url_set_sha256": _sha256_json([
            REPORT_URL,
            REPORT_URL.replace("index.html", "report_data.json"),
        ]),
        "requested_urls": requested_urls,
        "request_list_sha256": request_list_sha256,
        "network_closure": "manifest_allowlist",
        "desktop_nonblank": True,
        "mobile_nonblank": True,
        "request_count": 2,
        **zero_counts,
        "browser": {
            "executable": "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            "product": "Chrome/149.0.0.0",
            "protocol_version": "1.3",
        },
        "viewports": viewports,
        "blockers": [],
    }
    browser_smoke["evidence_sha256"] = _sha256_json(browser_smoke)
    _write_json(
        evidence_dir / "canary_receipt.json",
        {
            "schema_version": CANARY_RECEIPT_SCHEMA_VERSION,
            "observed_at": OBSERVED_AT,
            "ok": True,
            "execution_origin": _kafka_trigger_source(),
            "observed_trigger_source": _kafka_trigger_source(),
            "admission": CANARY_ADMISSION.to_dict(),
            "execution_request": execution_request,
            "remote_read": remote_read,
            "derived_capacity_lifecycle": capacity_lifecycle,
            "capacity_meter": capacity_meter,
            "pipeline": pipeline,
            "submission_count": 1,
            "submission_key": SUBMISSION_KEY,
            "outbox": {
                "ok": True,
                "submission_key": SUBMISSION_KEY,
                "origin_source_id": SOURCE_ID,
                "status": "completed",
                "execution_request_sha256": execution_request_sha256,
                "reserved_receipt_sha256": reserved_receipt_sha256,
            },
            "vm": {
                "ok": True,
                "submission_key": SUBMISSION_KEY,
                "task_id": SUBMISSION_KEY,
                "terminal_state": "completed",
                "execution_request_sha256": execution_request_sha256,
                "capacity_lifecycle_sha256": lifecycle_sha256,
                **vm_execution,
            },
            "report": {
                "ok": True,
                "submission_key": SUBMISSION_KEY,
                "artifact_set_id": ARTIFACT_SET_ID,
                "report_url": REPORT_URL,
                "manifest_sha256": MANIFEST_SHA256,
                "delivery_manifest": {
                    "size_bytes": len(DELIVERY_MANIFEST_RAW),
                    "sha256": MANIFEST_SHA256,
                    "body": copy.deepcopy(DELIVERY_MANIFEST),
                },
                "index_html": {"size_bytes": 2048, "sha256": INDEX_SHA256},
                "report_data_json": {
                    "size_bytes": 4096,
                    "sha256": REPORT_DATA_SHA256,
                },
                "html_validation": "html_delivery_ready",
                "artifact_policy": "passive_static_html_v1",
                "browser_smoke": browser_smoke,
            },
            "delivery": {
                "ok": True,
                "submission_key": SUBMISSION_KEY,
                "artifact_set_id": ARTIFACT_SET_ID,
                "report_url": REPORT_URL,
                "effect_key": EFFECT_KEY,
                "target_key": "feishu-issue-7041712812",
                "marker": (f"[RCA_DELIVERY:{EFFECT_KEY}:{ARTIFACT_SET_ID[-12:]}]"),
                "remote_receipt": {
                    "remote_id": "feishu-comment-1",
                    "confirmed_field_keys": ["field_9193cb", "field_8c912e"],
                },
            },
            "delivery_obligations": [
                {
                    "subscription_key": "g1q3-rca-sub-v1-" + "d" * 64,
                    "effect_kind": "feishu_issue_comment",
                    "target_key": "feishu-issue-7041712812",
                    "target": {
                        "schema_version": "pnc_rca_delivery_target_v1",
                        "platform": "feishu_project",
                        "project_key": CANARY_ADMISSION.source_refs.project_key,
                        "work_item_type_key": (
                            CANARY_ADMISSION.source_refs.work_item_type_key
                        ),
                        "work_item_id": CANARY_ADMISSION.source_refs.work_item_id,
                        "output_cap": "L1",
                    },
                    "required": True,
                    "subscription_status": "materialized",
                    "delivery_id": "g1q3-rca-delivery-v1-" + "e" * 64,
                    "effect_key": EFFECT_KEY,
                    "effect_status": "succeeded",
                    "materialized_at": "2026-07-10T07:58:40+00:00",
                    "completed_at": "2026-07-10T07:58:50+00:00",
                    "remote_id": "feishu-comment-1",
                }
            ],
        },
    )
    target = _storage_target(
        total_bytes=30 * 1024**4,
        available_bytes=28 * 1024**4,
    )
    storage_admission = {
        "schema_version": "g1q3_rca_storage_admission_v2",
        "ok": True,
        "status": "pass",
        "observed_at": OBSERVED_AT,
        "capacity_scope": "derived_artifact_and_cache",
        "policy": {
            "requested_cases": 4,
            "concurrency_reserve_cases": 4,
            "requested_cases_scope": ("this_admission_capacity_reservation_only"),
            "assumed_cases_per_day": 200,
            "assumed_cases_per_day_scope": ("days_horizon_calculation_only"),
            "input_materialization_bytes_per_case": 0,
            "input_materialization": "forbidden",
            "expected_derived_artifact_bytes_per_case": 1_000_000_000,
            "input_unit": "bytes",
            "gb_definition_bytes": 1_000_000_000,
            "reserve_ratio": 0.3,
            "reserve_percent": 30.0,
            "task_output_multiplier": 3.25,
            "logical_budget_multipliers": {
                "derived_cache": 1.0,
                "derived_artifacts_and_publisher": 2.25,
                "total": 3.25,
            },
            "logical_budget_bytes_per_case": {
                "derived_cache": 1_000_000_000,
                "derived_artifacts_and_publisher": 2_250_000_000,
                "total": 3_250_000_000,
            },
        },
        "required_bytes_total": target["required_bytes"],
        "max_additional_cases": target["max_additional_cases"],
        "days_horizon_at_assumed_cases_per_day": target[
            "days_horizon_at_assumed_cases_per_day"
        ],
        "blockers": [],
        "target": target,
        "side_effects": "none_read_only_statvfs",
    }
    scheduler_evidence = {
        "schema_version": "pnc_rca_scheduler_capacity_v1",
        "observed_at": OBSERVED_AT,
        "source": {
            "generation_mode": "machine_generated",
            "component": "ssh-mini-submit",
            "component_commit": "d" * 40,
            "artifact_sha256": hashlib.sha256(b"ssh-mini-submit").hexdigest(),
        },
        "service_id": "root_cause_analysis_agent",
        "scheduler_epoch": "scheduler-epoch-20260710T075930Z",
        "capacity_enforcement": "atomic_pre_submit",
        "configuration_sha256": hashlib.sha256(b"scheduler-config").hexdigest(),
        "queue_limits": {
            "max_inflight": 4,
            "max_queued": 20,
            "max_batch_size": 10,
        },
    }
    scheduler_sha256 = _sha256_json(scheduler_evidence)
    _write_json(
        evidence_dir / "capacity_receipt.json",
        {
            "schema_version": CAPACITY_RECEIPT_SCHEMA_VERSION,
            "observed_at": OBSERVED_AT,
            "ok": True,
            "storage_admission": storage_admission,
            "scheduler_evidence": scheduler_evidence,
            "queue_limits": {
                "max_inflight": 4,
                "max_queued": 20,
                "max_batch_size": 10,
                "source": {
                    "schema_version": "pnc_rca_queue_limits_source_v1",
                    "source_kind": "scheduler_capacity_receipt",
                    "scheduler_evidence_sha256": scheduler_sha256,
                },
            },
        },
    )


def _write_canary_source_provenance(
    evidence_dir: Path,
    *,
    collector_path: Path,
    control_db_path: Path,
    delivery_db_path: Path,
) -> None:
    legacy_receipt = evidence_dir / "canary_receipt.json"
    receipt = (
        json.loads(legacy_receipt.read_text(encoding="utf-8"))
        if legacy_receipt.is_file()
        else _read_committed_pair_body(evidence_dir, "primary", "receipt")
    )
    request = receipt["execution_request"]
    root = request["data"]["artifact_root"]
    submission = receipt["submission_key"]
    vm = receipt["vm"]
    service = vm["service_result"]["receipt"]
    remote_read = receipt["remote_read"]
    cache = remote_read["receipt"]["derived_stream_cache"]
    meter = receipt["capacity_meter"]
    pipeline = receipt["pipeline"]
    smoke = receipt["report"]["browser_smoke"]
    browser_path = evidence_dir / "machine_sources" / submission / "browser_smoke.json"
    browser_path.parent.mkdir(parents=True, exist_ok=True)
    _write_json(browser_path, smoke)

    def json_source(
        path: str,
        *,
        canonical_sha256: str,
        raw_sha256: str | None = None,
        size_bytes: int = 4096,
    ) -> dict:
        return {
            "path": path,
            "size_bytes": size_bytes,
            "raw_sha256": raw_sha256 or canonical_sha256,
            "canonical_sha256": canonical_sha256,
        }

    files = {
        "task_meta": json_source(
            f"/home/mini/.hermes/shared-state/tasks/{submission}/meta.json",
            canonical_sha256="d" * 64,
            raw_sha256=vm["capacity_admission"]["task_meta_sha256"],
        ),
        "worker_result": json_source(
            vm["worker_result"]["path"],
            canonical_sha256=vm["worker_result"]["sha256"],
        ),
        "execution_request": json_source(
            root + "rca_execution_request.json",
            canonical_sha256=_sha256_json(request),
            raw_sha256=service["request_storage"]["sha256"],
            size_bytes=service["request_storage"]["bytes"],
        ),
        "remote_read": json_source(
            root + "s2_remote_read/remote_read_receipt.json",
            canonical_sha256=remote_read["receipt_sha256"],
        ),
        "capacity_lifecycle": json_source(
            root + "derived_capacity_reservation_receipt.json",
            canonical_sha256=vm["capacity_lifecycle_sha256"],
        ),
        "capacity_meter": json_source(meter["path"], canonical_sha256=meter["sha256"]),
        "pipeline": json_source(
            root + "pipeline_result.json",
            canonical_sha256=_sha256_json(pipeline),
        ),
        "service_result": json_source(
            vm["service_result"]["path"],
            canonical_sha256=vm["service_result"]["sha256"],
        ),
        "delivery_manifest": json_source(
            root + "delivery_manifest.json",
            canonical_sha256=_sha256_json(
                receipt["report"]["delivery_manifest"]["body"]
            ),
            raw_sha256=receipt["report"]["manifest_sha256"],
            size_bytes=receipt["report"]["delivery_manifest"]["size_bytes"],
        ),
        "delivery_contract": json_source(
            root + "delivery_contract.json",
            canonical_sha256="2" * 64,
            raw_sha256=smoke["delivery_contract_sha256"],
        ),
        "goal": {
            "path": f"/home/mini/.hermes/shared-state/tasks/{submission}/goal.md",
            "size_bytes": 4096,
            "raw_sha256": service["goal_sha256"],
        },
        "artifact_remote_stream_cache": {
            "path": cache["path"],
            "size_bytes": cache["bytes"],
            "raw_sha256": cache["sha256"],
        },
        "artifact_index_html": {
            "path": root + "index.html",
            "size_bytes": receipt["report"]["index_html"]["size_bytes"],
            "raw_sha256": receipt["report"]["index_html"]["sha256"],
        },
        "artifact_report_data": {
            "path": root + "report_data.json",
            "size_bytes": receipt["report"]["report_data_json"]["size_bytes"],
            "raw_sha256": receipt["report"]["report_data_json"]["sha256"],
        },
    }
    for name, stage in pipeline["downstream_stage_receipts"].items():
        files[f"stage_{name}"] = json_source(
            stage["artifact_receipt_path"],
            canonical_sha256=_sha256_json(stage["lineage"]),
            raw_sha256=stage["artifact_receipt_sha256"],
        )

    def local_source(path: Path) -> dict:
        raw = path.read_bytes()
        body = json.loads(raw.decode("utf-8"))
        return {
            "path": str(path.absolute()),
            "size_bytes": len(raw),
            "raw_sha256": hashlib.sha256(raw).hexdigest(),
            "canonical_sha256": _sha256_json(body),
        }

    control_path = control_db_path.expanduser().resolve(strict=True)
    delivery_path = delivery_db_path.expanduser().resolve(strict=True)

    def database(path: Path, snapshot: str) -> dict:
        info = path.stat()
        return {
            "path": str(path),
            "device": info.st_dev,
            "inode": info.st_ino,
            "size_bytes": info.st_size,
            "mtime_ns": info.st_mtime_ns,
            "query_mode": "sqlite_mode_ro_query_only_transaction",
            "snapshot_sha256": snapshot,
        }

    collector = collector_path.resolve(strict=True)
    provenance = {
        "schema_version": release_gate_module.CANARY_SOURCE_PROVENANCE_SCHEMA_VERSION,
        "collected_at": receipt["observed_at"],
        "read_only": True,
        "external_side_effects": False,
        "collector": {
            "path": str(collector),
            "sha256": hashlib.sha256(collector.read_bytes()).hexdigest(),
        },
        "execution_origin_sha256": _sha256_json(receipt["execution_origin"]),
        "observed_trigger_source_sha256": _sha256_json(
            receipt["observed_trigger_source"]
        ),
        "submission_key_sha256": hashlib.sha256(submission.encode("utf-8")).hexdigest(),
        "control_database": database(control_path, "3" * 64),
        "delivery_database": database(delivery_path, "4" * 64),
        "remote_transport": {
            "kind": "ssh-mini-agent",
            "operation": "bounded_read_only",
            "execution_request_abi": {
                "canonicalization": "json_ensure_ascii_false_sort_keys_compact_v1",
                "sha256": _sha256_execution_request(request),
            },
            "files": files,
        },
        "local_machine_sources": {
            "remote_reader_health": local_source(
                evidence_dir / "remote_reader_health.json"
            ),
            "browser_smoke": local_source(browser_path),
            "group_binding_authorizations": {},
        },
        "receipt_sha256": _sha256_json(receipt),
    }
    _write_json(evidence_dir / "canary_receipt_sources.json", provenance)


def _gate(tmp_path: Path, mode: str):
    evidence_dir = tmp_path / "evidence"
    consumer, dispatcher = _configs(tmp_path, mode)
    kafka_env_file = tmp_path / "kafka.env"
    kafka_env_file.write_text(
        "\n".join(
            f"{key}={value}" for key, value in _consumer_env(tmp_path, mode).items()
        )
        + "\n",
        encoding="utf-8",
    )
    kafka_env_file.chmod(0o600)
    _write_common_evidence(evidence_dir, kafka_env_file)
    if mode in release_gate_module.BOOTSTRAP_CAPACITY_MODES:
        (evidence_dir / "bootstrap-capacity.fixture").touch()
    host_repo = tmp_path / "unused-host-build"
    workspace_repo = tmp_path / "unused-workspace-build"
    vm_repo = tmp_path / "unused-vm-build"
    vm_worker_repo = tmp_path / "unused-vm-worker-build"
    if mode == "shadow":
        preflight = host_repo / "scripts" / "pnc_rca_kafka_preflight.py"
        preflight.parent.mkdir(parents=True)
        preflight.write_bytes(
            (
                release_gate_module.REPO_ROOT
                / "scripts"
                / "pnc_rca_kafka_preflight.py"
            ).read_bytes()
        )
    if mode in {
        "preauthorization",
        "preproduction",
        "canary_bootstrap",
        "canary",
        "production_bootstrap",
        "production",
    }:
        host_repo, host_commit = _create_host_build_repo(tmp_path)
        workspace_repo, workspace_commit = _create_git_repo(tmp_path, "workspace-build")
        vm_repo, vm_commit = _create_git_repo(
            tmp_path,
            "vm-build",
            entrypoint_relative="api/g1q3_rca/scripts/run_rca_service_request.py",
        )
        vm_worker_repo, vm_worker_commit = _create_git_repo(
            tmp_path,
            "vm-worker-build",
            entrypoint_relative="vm_coding_worker_v2.py",
        )
        _write_canary_evidence(
            evidence_dir,
            host_repo,
            host_commit,
            workspace_repo,
            workspace_commit,
            vm_repo,
            vm_commit,
            vm_worker_repo,
            vm_worker_commit,
            _runtime_config_sha256(consumer, dispatcher, mode),
            dispatcher.control_db_path,
            dispatcher.delivery_db_path,
            dispatcher.release_id,
            dispatcher.bootstrap_epoch_id,
        )
        _write_kafka_recent_replay_evidence(
            evidence_dir,
            kafka_env_file=kafka_env_file,
            host_repo=host_repo,
            host_commit=host_commit,
            consumer=consumer,
        )
    if mode in release_gate_module.PRODUCTION_MODES:
        release_bom_sha256 = json.loads(
            (evidence_dir / "build_manifest.json").read_text(encoding="utf-8")
        )["release_bom_sha256"]
        _write_production_evidence(
            evidence_dir,
            vm_commit=vm_commit,
            vm_worker_commit=vm_worker_commit,
            vm_service_entrypoint_sha256=hashlib.sha256(
                (
                    vm_repo / "api/g1q3_rca/scripts/run_rca_service_request.py"
                ).read_bytes()
            ).hexdigest(),
            vm_worker_entrypoint_sha256=hashlib.sha256(
                (vm_worker_repo / "vm_coding_worker_v2.py").read_bytes()
            ).hexdigest(),
            capacity_mode=(
                "bootstrap" if mode == "production_bootstrap" else "steady"
            ),
            release_bom_sha256=release_bom_sha256,
        )
        _write_canary_source_provenance(
            evidence_dir,
            collector_path=(host_repo / "scripts/pnc_rca_canary_collector.py"),
            control_db_path=dispatcher.control_db_path,
            delivery_db_path=dispatcher.delivery_db_path,
        )
        _publish_committed_pair(evidence_dir, "primary")
    host, vm = _write_contract_pair(tmp_path)
    if mode in {
        "preauthorization",
        "preproduction",
        "canary_bootstrap",
        "canary",
        "production_bootstrap",
        "production",
    }:
        shadow_path = evidence_dir / "shadow_soak.json"
        shadow = json.loads(shadow_path.read_text(encoding="utf-8"))
        shadow["build_manifest_sha256"] = hashlib.sha256(
            (evidence_dir / "build_manifest.json").read_bytes()
        ).hexdigest()
        shadow["config_sha256"] = _runtime_config_sha256(consumer, dispatcher, mode)
        _write_json(shadow_path, shadow)
    settings = ReleaseGateSettings(
        mode=mode,
        evidence_dir=evidence_dir,
        expected_topic=TOPIC,
        expected_rule_version=RULE,
        host_contract_path=host,
        vm_contract_path=vm,
        kafka_env_file=kafka_env_file,
        group_binding_receipt_dir=tmp_path / "group-binding-receipts",
        host_repo_root=host_repo,
        workspace_repo_root=workspace_repo,
        vm_repo_root=str(vm_repo),
        vm_worker_repo_root=str(vm_worker_repo),
    )
    if mode in {"preproduction", *release_gate_module.CANARY_MODES}:
        from gateway.pnc_rca_control_store import RcaControlStore

        preauthorization_marker = evidence_dir / "activation_preauthorization.json"
        _write_json(preauthorization_marker, {"fixture": True})
        preauthorization_settings = replace(settings, mode="preauthorization")
        preauthorization_report = evaluate_release_gate(
            consumer=consumer,
            dispatcher=dispatcher,
            settings=preauthorization_settings,
            cutover=_cutover("preauthorization"),
            now=NOW,
        )
        assert preauthorization_report["ok"] is True, preauthorization_report[
            "blockers"
        ]
        preauthorization_receipt = evidence_dir / "preauthorization-gate.json"
        release_gate_module.write_receipt_no_clobber(
            preauthorization_receipt,
            preauthorization_report,
            conflict_code="test_preauthorization_receipt_conflict",
        )
        preauthorization_capsule = (
            release_gate_module.write_activation_preauthorization_capsule(
                preauthorization_receipt,
                preauthorization_report,
                evidence_dir=evidence_dir,
                control_db_path=dispatcher.control_db_path,
            )
        )
        preauthorization_input = release_gate_module._read_activation_preauthorization_capsule_bundle(
            preauthorization_capsule,
            control_db_path=dispatcher.control_db_path,
            now=NOW,
        )["normalized"]
        activation_store = RcaControlStore(dispatcher.control_db_path)
        safe_off = activation_store.create_activation_epoch(
            epoch_id=preauthorization_input["epoch_id"],
            preauthorization_fingerprint=preauthorization_input[
                "preauthorization_fingerprint"
            ],
            preauthorization_gate_receipt_sha256=preauthorization_input[
                "preauthorization_gate_receipt_sha256"
            ],
            preauthorization_capsule_sha256=preauthorization_input[
                "preauthorization_capsule_sha256"
            ],
            config_sha256=preauthorization_input["config_sha256"],
            db_logical_identity=preauthorization_input["db_logical_identity"],
            partition_start_fence=preauthorization_input["partition_start_fence"],
            operator="release-test",
            reason="create safe-off epoch from preauthorization capsule",
            now=NOW,
        )
        migration_module._checkpoint_restore(dispatcher.control_db_path)
        preauthorization_marker.unlink()
        preproduction_marker = evidence_dir / "activation_preproduction.json"
        _write_json(preproduction_marker, {"fixture": True})
        settings = replace(
            settings,
            preauthorization_capsule_path=preauthorization_capsule,
        )
        if mode in release_gate_module.CANARY_MODES:
            preproduction_settings = replace(settings, mode="preproduction")
            preproduction_report = evaluate_release_gate(
                consumer=consumer,
                dispatcher=dispatcher,
                settings=preproduction_settings,
                cutover=_cutover("preproduction"),
                now=NOW,
            )
            assert preproduction_report["ok"] is True, preproduction_report[
                "blockers"
            ]
            preproduction_receipt = evidence_dir / "preproduction-gate.json"
            release_gate_module.write_receipt_no_clobber(
                preproduction_receipt,
                preproduction_report,
                conflict_code="test_preproduction_receipt_conflict",
            )
            preproduction_capsule = (
                release_gate_module.write_activation_preproduction_capsule(
                    preproduction_receipt,
                    preproduction_report,
                    evidence_dir=evidence_dir,
                    control_db_path=dispatcher.control_db_path,
                    preauthorization_capsule=preauthorization_capsule,
                )
            )
            transition = release_gate_module._read_activation_preproduction_capsule_bundle(
                preproduction_capsule,
                control_db_path=dispatcher.control_db_path,
                current_activation=safe_off,
                now=NOW,
            )["normalized"]
            activation_store.preauthorize_activation_epoch(
                epoch_id=transition["epoch_id"],
                preproduction_fingerprint=transition["preproduction_fingerprint"],
                preproduction_gate_receipt_sha256=transition[
                    "preproduction_gate_receipt_sha256"
                ],
                preproduction_capsule_sha256=transition[
                    "preproduction_capsule_sha256"
                ],
                expected_preauthorization_fingerprint=transition[
                    "expected_preauthorization_fingerprint"
                ],
                expected_preauthorization_gate_receipt_sha256=transition[
                    "expected_preauthorization_gate_receipt_sha256"
                ],
                expected_preauthorization_capsule_sha256=transition[
                    "expected_preauthorization_capsule_sha256"
                ],
                expected_config_sha256=transition["expected_config_sha256"],
                expected_db_logical_identity_sha256=transition[
                    "expected_db_logical_identity_sha256"
                ],
                expected_partition_start_fence_sha256=transition[
                    "expected_partition_start_fence_sha256"
                ],
                operator="release-test",
                reason="preauthorize exact release gate fixture",
                now=NOW,
            )
            source_identities = ACTIVATION_SLOT_IDENTITIES
            for slot_kind, source_identity in source_identities.items():
                activation_store.authorize_activation_slot(
                    epoch_id=transition["epoch_id"],
                    slot_kind=slot_kind,
                    source_kind=(
                        "kafka" if slot_kind == "kafka_success" else "manual"
                    ),
                    source_identity=source_identity,
                    operator="release-test",
                    reason=f"authorize {slot_kind}",
                    now=NOW,
                )
            activation_store.transition_activation_epoch(
                epoch_id=transition["epoch_id"],
                target_state="bounded_active",
                expected_state="preauthorized",
                operator="release-test",
                reason="start exact bounded canary budget",
                now=NOW,
            )
            preproduction_marker.unlink()
            settings = replace(
                settings,
                preproduction_capsule_path=preproduction_capsule,
            )
        migration_module._checkpoint_restore(dispatcher.control_db_path)
    return consumer, dispatcher, settings


def _complete_production_manual_baseline(
    tmp_path: Path,
    *,
    consumer: ConsumerConfig,
    dispatcher: DispatcherConfig,
    settings: ReleaseGateSettings,
) -> CutoverConfig:
    from scripts.pnc_rca_canary_collector import (
        CanaryReceiptCollector,
        write_collection,
    )
    from tests.scripts.test_pnc_rca_canary_collector import (
        _terminal_manual_fixture,
    )

    cutover = replace(
        _cutover("production"),
        manual_intake_enabled=True,
        manual_chat_ids=(release_gate_module.G1Q3_RCA_GROUP_ID,),
        manual_operator_enabled=False,
    )
    runtime_config_sha256 = _runtime_config_sha256(
        consumer,
        dispatcher,
        "production",
        cutover=cutover,
    )
    host_repo = settings.host_repo_root
    workspace_repo = settings.workspace_repo_root
    vm_repo = Path(settings.vm_repo_root)
    vm_worker_repo = Path(settings.vm_worker_repo_root)
    _write_build_manifest(
        settings.evidence_dir,
        host_repo,
        _git(host_repo, "rev-parse", "HEAD"),
        workspace_repo,
        _git(workspace_repo, "rev-parse", "HEAD"),
        vm_repo,
        _git(vm_repo, "rev-parse", "HEAD"),
        vm_worker_repo,
        _git(vm_worker_repo, "rev-parse", "HEAD"),
        runtime_config_sha256,
    )
    candidate_database = tmp_path / "candidate-live.sqlite3"
    shutil.copy2(dispatcher.control_db_path, candidate_database)
    _reset_terminal_fixture_database(tmp_path)
    config, reader, source_id = _terminal_manual_fixture(tmp_path)
    assert config.manual_chat_ids == cutover.manual_chat_ids
    _merge_canary_fixture_into_candidate_database(
        candidate_database,
        config.control_db_path,
    )
    terminal = CanaryReceiptCollector(
        config,
        remote_reader=reader,
        now=lambda: NOW + timedelta(seconds=5),
    ).collect_terminal_failure(source_id)
    deployed_collector = host_repo / "scripts/pnc_rca_canary_collector.py"
    terminal.provenance["collector"] = {
        "path": str(deployed_collector.resolve()),
        "sha256": hashlib.sha256(deployed_collector.read_bytes()).hexdigest(),
    }
    write_collection(terminal, settings.evidence_dir)
    _publish_committed_pair(
        settings.evidence_dir,
        "manual_terminal_failure",
    )
    _write_canary_source_provenance(
        settings.evidence_dir,
        collector_path=deployed_collector,
        control_db_path=dispatcher.control_db_path,
        delivery_db_path=dispatcher.delivery_db_path,
    )
    _publish_committed_pair(settings.evidence_dir, "primary")
    shadow_path = settings.evidence_dir / "shadow_soak.json"
    shadow = json.loads(shadow_path.read_text(encoding="utf-8"))
    shadow["build_manifest_sha256"] = hashlib.sha256(
        (settings.evidence_dir / "build_manifest.json").read_bytes()
    ).hexdigest()
    shadow["config_sha256"] = runtime_config_sha256
    _write_json(shadow_path, shadow)
    return cutover


def _activation_admissions() -> dict[str, dict[str, object]]:
    return {
        slot_kind: {
            key: admission[key]
            for key in ("business_key", "submission_key", "generation")
        }
        for slot_kind, admission in ACTIVATION_SLOT_ADMISSIONS.items()
    }


def _canonical_activation_slot_plan() -> dict[str, dict[str, object]]:
    result: dict[str, dict[str, object]] = {}
    for slot_kind, raw_identity in ACTIVATION_SLOT_IDENTITIES.items():
        source_kind = "kafka" if slot_kind == "kafka_success" else "manual"
        identity = dict(raw_identity)
        if source_kind == "kafka":
            identity.update(topic=TOPIC, partition=0, offset=10)
        result[slot_kind] = {
            "source_kind": source_kind,
            "entrypoint": (
                "kafka_ingest" if source_kind == "kafka" else "manual_admit"
            ),
            "source_identity": identity,
            "source_identity_sha256": release_gate_module._sha256_json(identity),
            "max_admissions": 1,
            "expected_admission": ACTIVATION_SLOT_ADMISSIONS[slot_kind],
            "expected_outcome": (
                "terminal_failed"
                if slot_kind == "manual_terminal_failure"
                else "success"
            ),
        }
    return result


def _create_test_activation_epoch(
    path: Path,
    *,
    config_sha256: str,
    bounded: bool,
    admissions: dict[str, dict[str, object]] | None = None,
) -> tuple[object, dict[str, dict[str, object]]]:
    from gateway.pnc_rca_control_store import RcaControlStore

    store = RcaControlStore(path)
    epoch_id = "rca-release-gate-activation-20260710"
    safe_off = store.create_activation_epoch(
        epoch_id=epoch_id,
        preauthorization_fingerprint="1" * 64,
        preauthorization_gate_receipt_sha256="2" * 64,
        preauthorization_capsule_sha256="3" * 64,
        config_sha256=config_sha256,
        db_logical_identity={"database": str(path), "release": "test"},
        partition_start_fence={TOPIC: {"0": 10, "1": 20}},
        operator="release-test",
        reason="release gate activation fixture",
        now=NOW - timedelta(seconds=30),
    )
    from scripts import pnc_rca_store_migration_drill as migration_module

    migration_module._checkpoint_restore(path)
    admissions = admissions or _activation_admissions()
    if not bounded:
        return store, admissions
    store.preauthorize_activation_epoch(
        epoch_id=epoch_id,
        preproduction_fingerprint="a" * 64,
        preproduction_gate_receipt_sha256="4" * 64,
        preproduction_capsule_sha256="5" * 64,
        expected_preauthorization_fingerprint=safe_off[
            "preauthorization_fingerprint"
        ],
        expected_preauthorization_gate_receipt_sha256=safe_off[
            "preauthorization_gate_receipt_sha256"
        ],
        expected_preauthorization_capsule_sha256=safe_off[
            "preauthorization_capsule_sha256"
        ],
        expected_config_sha256=safe_off["config_sha256"],
        expected_db_logical_identity_sha256=safe_off[
            "db_logical_identity_sha256"
        ],
        expected_partition_start_fence_sha256=safe_off[
            "partition_start_fence_sha256"
        ],
        operator="release-test",
        reason="preauthorize release gate activation fixture",
        now=NOW - timedelta(seconds=30),
    )
    source_identities = ACTIVATION_SLOT_IDENTITIES
    for slot_kind, source_identity in source_identities.items():
        store.authorize_activation_slot(
            epoch_id=epoch_id,
            slot_kind=slot_kind,
            source_kind=("kafka" if slot_kind == "kafka_success" else "manual"),
            source_identity=source_identity,
            operator="release-test",
            reason=f"authorize {slot_kind}",
            now=NOW - timedelta(seconds=20),
        )
    store.transition_activation_epoch(
        epoch_id=epoch_id,
        target_state="bounded_active",
        expected_state="preauthorized",
        operator="release-test",
        reason="run the exact three activation canaries",
        now=NOW - timedelta(seconds=15),
    )
    with sqlite3.connect(path) as connection:
        connection.row_factory = sqlite3.Row
        for slot_kind, admission in admissions.items():
            slot = connection.execute(
                """
                SELECT authorized_identity_sha256
                  FROM rca_activation_budget_slots
                 WHERE epoch_id = ? AND slot_kind = ?
                """,
                (epoch_id, slot_kind),
            ).fetchone()
            source_kind = "kafka" if slot_kind == "kafka_success" else "manual"
            cursor = connection.execute(
                """
                INSERT INTO rca_activation_admission_ledger(
                    epoch_id, admission_key, entrypoint, source_kind,
                    source_identity_sha256, slot_kind, decision, reason,
                    business_key, submission_key, generation,
                    first_adjudicated_at, last_adjudicated_at, admitted_at,
                    bound_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'admit', ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    epoch_id,
                    f"activation-admission-{slot_kind}",
                    "kafka_ingest" if source_kind == "kafka" else "manual_admit",
                    source_kind,
                    slot["authorized_identity_sha256"],
                    slot_kind,
                    "activation bounded canary",
                    admission["business_key"],
                    admission["submission_key"],
                    admission["generation"],
                    OBSERVED_AT,
                    OBSERVED_AT,
                    OBSERVED_AT,
                    OBSERVED_AT,
                ),
            )
            ledger_id = int(cursor.lastrowid)
            kafka = slot_kind == "kafka_success"
            connection.execute(
                """
                INSERT INTO business_triggers(
                    business_key, generation, submission_key,
                    creation_rule_version, work_item_id, project_key,
                    work_item_type_key, activation_epoch_id,
                    activation_ledger_id, source_event_id, source_topic,
                    source_partition, source_offset, normalized_json,
                    state, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '{}', ?, ?)
                """,
                (
                    admission["business_key"],
                    admission["generation"],
                    admission["submission_key"],
                    RULE,
                    f"71{ledger_id:02d}",
                    "t03o4q",
                    "issue",
                    epoch_id,
                    ledger_id,
                    f"{TOPIC}:0:10" if kafka else None,
                    TOPIC if kafka else None,
                    0 if kafka else None,
                    10 if kafka else None,
                    "submitted",
                    OBSERVED_AT,
                ),
            )
            connection.execute(
                """
                INSERT INTO rca_outbox(
                    action, business_key, submission_key,
                    creation_rule_version, generation, activation_epoch_id,
                    activation_ledger_id, source_event_id, source_topic,
                    source_partition, source_offset, payload_json, status,
                    completed_at, created_at, updated_at
                ) VALUES ('submit_rca', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '{}',
                          'completed', ?, ?, ?)
                """,
                (
                    admission["business_key"],
                    admission["submission_key"],
                    RULE,
                    admission["generation"],
                    epoch_id,
                    ledger_id,
                    f"{TOPIC}:0:10" if kafka else None,
                    TOPIC if kafka else None,
                    0 if kafka else None,
                    10 if kafka else None,
                    OBSERVED_AT,
                    OBSERVED_AT,
                    OBSERVED_AT,
                ),
            )
            connection.execute(
                """
                UPDATE rca_activation_budget_slots
                   SET consumed_ledger_id = ?, consumed_at = ?
                 WHERE epoch_id = ? AND slot_kind = ?
                """,
                (ledger_id, OBSERVED_AT, epoch_id, slot_kind),
            )
        connection.commit()
    return store, admissions


def _merge_canary_fixture_into_candidate_database(
    candidate_database: Path,
    fixture_database: Path,
) -> None:
    """Preserve candidate schema/history while installing deterministic canary rows."""
    timestamp = "2026-07-10T07:57:00+00:00"

    def required_value(name: str, declared_type: str):
        fixed = {
            "attempt_no": 1,
            "event_seq": 1,
            "fence": 1,
            "raw_value": b"{}",
            "raw_size_bytes": 2,
            "submission_key": SUBMISSION_KEY,
            "creation_rule_version": RULE,
            "work_item_id": "7041712812",
            "project_key": "t03o4q",
            "work_item_type_key": "issue",
        }
        if name in fixed:
            return fixed[name]
        if name.endswith("_at") or name.endswith("_time"):
            return timestamp
        if name.endswith("_json"):
            return "{}"
        if "id" in name or "key" in name or "version" in name:
            return "fixture"
        return 1 if declared_type.upper().startswith("INT") else ""

    candidate = sqlite3.connect(candidate_database)
    fixture = sqlite3.connect(fixture_database)
    candidate.row_factory = sqlite3.Row
    fixture.row_factory = sqlite3.Row
    preserved_tables = {
        "control_meta",
        "rca_delivery_meta",
        "rca_delivery_dispatcher_circuit",
        "rca_dispatcher_circuit",
    }
    try:
        candidate_tables = {
            str(row[0])
            for row in candidate.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%'"
            )
        }
        fixture_tables = {
            str(row[0])
            for row in fixture.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%'"
            )
        }
        for table in sorted((candidate_tables & fixture_tables) - preserved_tables):
            quoted_table = '"' + table.replace('"', '""') + '"'
            candidate_columns = [
                dict(row)
                for row in candidate.execute(f"PRAGMA table_info({quoted_table})")
            ]
            fixture_columns = {
                str(row["name"])
                for row in fixture.execute(f"PRAGMA table_info({quoted_table})")
            }
            selected_columns = [
                column
                for column in candidate_columns
                if column["name"] in fixture_columns
                or (
                    column["notnull"]
                    and column["dflt_value"] is None
                    and column["pk"] == 0
                )
            ]
            candidate.execute(f"DELETE FROM {quoted_table}")
            names = [str(column["name"]) for column in selected_columns]
            quoted_names = ",".join(
                '"' + name.replace('"', '""') + '"' for name in names
            )
            placeholders = ",".join("?" for _ in names)
            for row in fixture.execute(f"SELECT * FROM {quoted_table}"):
                values = [
                    row[column["name"]]
                    if column["name"] in fixture_columns
                    else required_value(column["name"], column["type"])
                    for column in selected_columns
                ]
                candidate.execute(
                    f"INSERT INTO {quoted_table} ({quoted_names}) "
                    f"VALUES ({placeholders})",
                    values,
                )
        candidate.commit()
    finally:
        candidate.close()
        fixture.close()
    os.replace(candidate_database, fixture_database)
    fixture_database.chmod(0o600)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("evidence_max_age_seconds", 901, "freshness"),
        ("shadow_soak_min_seconds", 86_399, "shadow soak"),
        ("min_storage_horizon_days", 29, "storage horizon"),
        ("target_cases_per_day", 199, "target case volume"),
        ("min_capacity_horizon_days", 6.999, "capacity horizon"),
    ],
)
def test_canary_and_production_policy_floors_cannot_be_weakened(
    tmp_path, field, value, message
):
    for mode in (
        "preauthorization",
        "preproduction",
        "canary_bootstrap",
        "canary",
        "production_bootstrap",
        "production",
    ):
        values = {
            "mode": mode,
            "evidence_dir": tmp_path / "evidence",
            "expected_topic": TOPIC,
            "expected_rule_version": RULE,
            "host_contract_path": tmp_path / "host.py",
            "vm_contract_path": tmp_path / "vm.py",
            "kafka_env_file": tmp_path / "kafka.env",
            field: value,
        }
        with pytest.raises(ValueError, match=message):
            ReleaseGateSettings(**values)


def test_shadow_mode_keeps_short_local_debug_policy_available(tmp_path):
    settings = ReleaseGateSettings(
        mode="shadow",
        evidence_dir=tmp_path / "evidence",
        expected_topic=TOPIC,
        expected_rule_version=RULE,
        host_contract_path=tmp_path / "host.py",
        vm_contract_path=tmp_path / "vm.py",
        kafka_env_file=tmp_path / "kafka.env",
        evidence_max_age_seconds=3_600,
        shadow_soak_min_seconds=1,
        min_storage_horizon_days=1,
        target_cases_per_day=1,
        min_capacity_horizon_days=0.1,
    )
    assert settings.shadow_soak_min_seconds == 1


def _insert_unbound_activation_ledger(
    database: Path,
    *,
    epoch_id: str,
    decision: str,
    suffix: str,
) -> None:
    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            INSERT INTO rca_activation_admission_ledger(
                epoch_id, admission_key, entrypoint, source_kind,
                source_identity_sha256, decision, reason, business_key,
                submission_key, generation, first_adjudicated_at,
                last_adjudicated_at
            ) VALUES (?, ?, 'manual_admit', 'manual', ?, ?, ?, ?, ?, 1, ?, ?)
            """,
            (
                epoch_id,
                f"unbound-{suffix}",
                hashlib.sha256(suffix.encode()).hexdigest(),
                decision,
                f"fixture {decision}",
                f"business-{suffix}",
                f"submission-{suffix}",
                OBSERVED_AT,
                OBSERVED_AT,
            ),
        )
        connection.commit()


def test_safe_off_epoch_holds_effects_and_rejects_unbound_creating_ledger(
    tmp_path,
):
    database = tmp_path / "activation.sqlite3"
    config_sha256 = "c" * 64
    store, _admissions = _create_test_activation_epoch(
        database,
        config_sha256=config_sha256,
        bounded=False,
    )
    detail = release_gate_module._check_activation_epoch(
        control_db_path=database,
        mode="preproduction",
        expected_config_sha256=config_sha256,
    )
    assert detail["state"] == "safe_off"
    assert detail["side_effects_held"] is True

    epoch_id = store.activation_epoch()["epoch_id"]
    _insert_unbound_activation_ledger(
        database,
        epoch_id=epoch_id,
        decision="shadow",
        suffix="current-shadow",
    )
    with pytest.raises(
        EvidenceError,
        match="activation_preproduction_effects_not_held",
    ):
        release_gate_module._check_activation_epoch(
            control_db_path=database,
            mode="preproduction",
            expected_config_sha256=config_sha256,
        )


def test_bootstrap_runtime_accepts_safe_gateway_and_rejects_drift_or_effects(
    tmp_path,
):
    consumer, dispatcher, settings = _gate(tmp_path, "preproduction")
    cutover = _cutover("preproduction")
    activation = release_gate_module._check_activation_epoch(
        control_db_path=dispatcher.control_db_path,
        mode="preproduction",
        expected_config_sha256=_runtime_config_sha256(
            consumer, dispatcher, "preproduction"
        ),
    )
    accepted = release_gate_module._check_activation_bootstrap_runtime(
        mode="preproduction",
        control_db_path=dispatcher.control_db_path,
        repo_root=settings.host_repo_root,
        cutover=cutover,
        consumer=consumer,
        activation=activation,
    )
    assert accepted["gateway"]["state"] == "running_safe"
    assert accepted["manual_rca_admission"] == "fail_closed_by_activation_state"

    observed = release_gate_module._collect_activation_bootstrap_runtime(
        repo_root=settings.host_repo_root,
        cutover=cutover,
        consumer=consumer,
        require_rca_residents_stopped=True,
    )
    drifted = copy.deepcopy(observed)
    drifted["gateway"]["verified"]["public_config_sha256"] = "f" * 64
    with pytest.raises(
        EvidenceError,
        match="activation_bootstrap_gateway_binding_invalid",
    ):
        release_gate_module._check_activation_bootstrap_runtime(
            mode="preproduction",
            control_db_path=dispatcher.control_db_path,
            repo_root=settings.host_repo_root,
            cutover=cutover,
            consumer=consumer,
            activation=activation,
            collector=lambda **_kwargs: drifted,
        )

    stopped = copy.deepcopy(observed)
    stopped["gateway"] = {
        "state": "stopped",
        "runtime_identity": None,
        "verified": None,
    }
    with pytest.raises(
        EvidenceError,
        match="activation_bootstrap_gateway_not_running",
    ):
        release_gate_module._check_activation_bootstrap_runtime(
            mode="preproduction",
            control_db_path=dispatcher.control_db_path,
            repo_root=settings.host_repo_root,
            cutover=cutover,
            consumer=consumer,
            activation=activation,
            collector=lambda **_kwargs: stopped,
        )

    restarted = copy.deepcopy(accepted["gateway"])
    restarted["pid"] += 1
    with pytest.raises(EvidenceError, match="activation_bootstrap_gateway_restarted"):
        release_gate_module._check_activation_bootstrap_runtime(
            mode="preproduction",
            control_db_path=dispatcher.control_db_path,
            repo_root=settings.host_repo_root,
            cutover=cutover,
            consumer=consumer,
            activation=activation,
            previous_gateway_binding=restarted,
        )

    _insert_unbound_activation_ledger(
        dispatcher.control_db_path,
        epoch_id=activation["epoch_id"],
        decision="shadow",
        suffix="bootstrap-active-effect",
    )
    with pytest.raises(EvidenceError, match="activation_bootstrap_database_not_safe"):
        release_gate_module._check_activation_bootstrap_runtime(
            mode="preproduction",
            control_db_path=dispatcher.control_db_path,
            repo_root=settings.host_repo_root,
            cutover=cutover,
            consumer=consumer,
            activation=activation,
        )


def test_production_bootstrap_treats_completed_canary_ledgers_as_audit(
    tmp_path,
):
    consumer, dispatcher, settings = _gate(tmp_path, "preauthorization")
    cutover = _cutover("production")
    database = tmp_path / "completed-canaries.sqlite3"
    config_sha256 = _runtime_config_sha256(
        consumer,
        dispatcher,
        "production",
        cutover=cutover,
    )
    _create_test_activation_epoch(
        database,
        config_sha256=config_sha256,
        bounded=True,
    )

    snapshot = release_gate_module._read_activation_bootstrap_database(database)
    assert snapshot["active_effects"]["active_activation_admissions"] == 0
    assert snapshot["active_effects"]["active_outbox"] == 0
    assert snapshot["active_effect_count"] == 0

    activation = release_gate_module._check_activation_epoch(
        control_db_path=database,
        mode="production",
        expected_config_sha256=config_sha256,
    )
    accepted = release_gate_module._check_activation_bootstrap_runtime(
        mode="production",
        control_db_path=database,
        repo_root=settings.host_repo_root,
        cutover=cutover,
        consumer=consumer,
        activation=activation,
    )
    assert accepted["database_state"] == "bounded_active"
    assert accepted["active_effect_count"] == 0


def test_activation_unbound_metric_ignores_reject_join_and_old_epoch(tmp_path):
    database = tmp_path / "activation.sqlite3"
    config_sha256 = "c" * 64
    store, _admissions = _create_test_activation_epoch(
        database,
        config_sha256=config_sha256,
        bounded=False,
    )
    epoch_id = store.activation_epoch()["epoch_id"]
    _insert_unbound_activation_ledger(
        database, epoch_id=epoch_id, decision="reject", suffix="reject"
    )
    _insert_unbound_activation_ledger(
        database, epoch_id=epoch_id, decision="join", suffix="join"
    )
    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            INSERT INTO rca_activation_epochs(
                epoch_id, state, is_current,
                preauthorization_fingerprint,
                preauthorization_gate_receipt_sha256,
                preauthorization_capsule_sha256,
                preproduction_fingerprint,
                preproduction_gate_receipt_sha256,
                preproduction_capsule_sha256,
                config_sha256, db_logical_identity_json,
                db_logical_identity_sha256, partition_start_fence_json,
                partition_start_fence_sha256, created_at, updated_at,
                aborted_at
                ) VALUES (
                    ?, 'aborted', 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
            """,
            (
                "rca-old-activation-20260709",
                "1" * 64,
                "2" * 64,
                "3" * 64,
                "b" * 64,
                "4" * 64,
                "5" * 64,
                config_sha256,
                release_gate_module._canonical_json({"database": "old"}),
                _sha256_json({"database": "old"}),
                release_gate_module._canonical_json({TOPIC: {"0": 0}}),
                _sha256_json({TOPIC: {"0": 0}}),
                OBSERVED_AT,
                OBSERVED_AT,
                OBSERVED_AT,
            ),
        )
        connection.commit()
    _insert_unbound_activation_ledger(
        database,
        epoch_id="rca-old-activation-20260709",
        decision="shadow",
        suffix="old-shadow",
    )
    snapshot = release_gate_module._read_activation_database(database)
    assert snapshot["unbound_ledger"] == 0
    detail = release_gate_module._check_activation_epoch(
        control_db_path=database,
        mode="preproduction",
        expected_config_sha256=config_sha256,
    )
    assert detail["side_effects_held"] is True


def _activation_collector_fixture(
    *,
    epoch_id: str,
    runtime_identity_sha256: str,
    consumer: ConsumerConfig,
    end_fence: dict[str, dict[str, int]],
    host_repo_root: Path | None = None,
) -> dict:
    start_fence = {TOPIC: {"0": 10, "1": 20}}
    collector_path = (
        (host_repo_root or release_gate_module.REPO_ROOT)
        / "scripts"
        / "pnc_rca_release_gate.py"
    ).resolve()
    offset_sources = {
        topic: {
            partition: {
                "source": "broker_committed",
                "offset": offset,
                "broker_group_offset": offset,
                "freeze_position": offset,
                "start_offset": start_fence[topic][partition],
            }
            for partition, offset in partitions.items()
        }
        for topic, partitions in end_fence.items()
    }
    return {
        "schema_version": "pnc_rca_activation_end_fence_collector_v3",
        "observed_at": OBSERVED_AT,
        "operation": "read_only_group_offsets_under_resident_pause",
        "group_id": consumer.group_id,
        "topic": consumer.topic,
        "consumer_runtime_identity_sha256": runtime_identity_sha256,
        "connection_config_sha256": _sha256_json(
            release_gate_module._activation_end_fence_connection_config(
                consumer, start_fence
            )
        ),
        "collector_path": str(collector_path),
        "collector_sha256": hashlib.sha256(collector_path.read_bytes()).hexdigest(),
        "observed_offsets": end_fence,
        "offset_sources": offset_sources,
        "offset_sources_sha256": _sha256_json(offset_sources),
        "freeze_receipt": {
            "schema_version": "pnc_rca_activation_ingress_freeze_v1",
            "epoch_id": epoch_id,
            "state": "partitions_paused",
            "freeze_token": "freeze-release-test",
            "paused_at": OBSERVED_AT,
            "observed_at": OBSERVED_AT,
            "consumer_runtime_identity_sha256": runtime_identity_sha256,
            "partition_positions": end_fence,
            "restart_required": False,
        },
        "restart_required": False,
        "side_effect_contract": (
            release_gate_module.ACTIVATION_END_FENCE_SIDE_EFFECT_CONTRACT
        ),
    }


def test_production_activation_candidate_binds_external_end_fence_and_receipts(
    tmp_path,
):
    database = tmp_path / "activation.sqlite3"
    config_sha256 = "c" * 64
    store, admissions = _create_test_activation_epoch(
        database,
        config_sha256=config_sha256,
        bounded=True,
    )
    activation = release_gate_module._check_activation_epoch(
        control_db_path=database,
        mode="production",
        expected_config_sha256=config_sha256,
    )
    consumer, _dispatcher = _configs(tmp_path, "production")
    runtime_identity_sha256 = "e" * 64
    end_fence = {TOPIC: {"0": 11, "1": 20}}
    assert store.activation_release_binding_sha256(
        epoch_id=activation["epoch_id"],
        partition_end_fence=end_fence,
    ) == activation["release_binding_sha256"]
    collector = _activation_collector_fixture(
        epoch_id=activation["epoch_id"],
        runtime_identity_sha256=runtime_identity_sha256,
        consumer=consumer,
        end_fence=end_fence,
    )
    collector["observed_at"] = (NOW - timedelta(minutes=5)).isoformat()
    collector["freeze_receipt"]["paused_at"] = (
        NOW - timedelta(hours=2)
    ).isoformat()
    collector["freeze_receipt"]["observed_at"] = (
        NOW - timedelta(minutes=5)
    ).isoformat()
    live_collector = copy.deepcopy(collector)
    live_collector["observed_at"] = NOW.isoformat()
    live_collector["freeze_receipt"]["observed_at"] = NOW.isoformat()
    candidate = release_gate_module.build_activation_production_candidate(
        activation=activation,
        expected_config_sha256=config_sha256,
        collector=collector,
        now=NOW - timedelta(seconds=30),
    )
    detail = release_gate_module._check_activation_production_candidate(
        candidate,
        activation=activation,
        consumer=consumer,
        expected_config_sha256=config_sha256,
        expected_consumer_runtime_identity_sha256=runtime_identity_sha256,
        expected_host_repo_root=release_gate_module.REPO_ROOT,
        live_collector=live_collector,
        now=NOW,
        max_age_seconds=900,
    )
    assert detail["ingress_frozen"] is True
    assert detail["confirm_input"]["partition_end_fence"] == end_fence
    assert detail["confirm_input"]["restart_between_gate_and_confirm"] is False
    assert detail["ingress_freeze_binding"]["paused_at"] == (
        NOW - timedelta(hours=2)
    ).isoformat()
    binding = release_gate_module._check_activation_canary_bindings(
        activation,
        slot_plan=_canonical_activation_slot_plan(),
        kafka_admission=ACTIVATION_SLOT_ADMISSIONS["kafka_success"],
        manual_success_admission=ACTIVATION_SLOT_ADMISSIONS["manual_success"],
        manual_terminal_admission=ACTIVATION_SLOT_ADMISSIONS[
            "manual_terminal_failure"
        ],
        kafka_detail={
            "execution_origin": {"kafka_event_uid": EVENT_UID},
            "outbox_status": "completed",
        },
        manual_success_detail={
            "execution_origin": {
                key: value
                for key, value in ACTIVATION_SLOT_IDENTITIES[
                    "manual_success"
                ].items()
                if key != "issue_url"
            },
            "outbox_status": "completed",
        },
        manual_terminal_detail={
            "observed_trigger_source": {
                key: value
                for key, value in ACTIVATION_SLOT_IDENTITIES[
                    "manual_terminal_failure"
                ].items()
                if key != "issue_url"
            },
            "outcome": "terminal_failed",
        },
    )
    assert binding["slot_count"] == 3

    forged = copy.deepcopy(candidate)
    forged["partition_end_fence"][TOPIC]["0"] = 100
    forged["collector"]["observed_offsets"][TOPIC]["0"] = 100
    forged["collector"]["freeze_receipt"]["partition_positions"][TOPIC]["0"] = 100
    forged_source = forged["collector"]["offset_sources"][TOPIC]["0"]
    forged_source.update(
        offset=100,
        broker_group_offset=100,
        freeze_position=100,
    )
    forged["collector"]["offset_sources_sha256"] = _sha256_json(
        forged["collector"]["offset_sources"]
    )
    with pytest.raises(EvidenceError, match="activation_end_fence_live_recheck_mismatch"):
        release_gate_module._check_activation_production_candidate(
            forged,
            activation=activation,
            consumer=consumer,
            expected_config_sha256=config_sha256,
            expected_consumer_runtime_identity_sha256=runtime_identity_sha256,
            expected_host_repo_root=release_gate_module.REPO_ROOT,
            live_collector=live_collector,
            now=NOW,
            max_age_seconds=900,
        )


def test_activation_end_fence_collector_reads_group_offsets_under_pause(tmp_path):
    consumer, _dispatcher = _configs(tmp_path, "production")
    runtime_identity = {
        "service_label": "local.pnc.rca-kafka-consumer",
        "pid": 41000,
        "process_create_time": 1_783_650_000.0,
    }
    runtime_sha256 = _sha256_json(runtime_identity)
    start_fence = {TOPIC: {"0": 10, "1": 50}}
    end_fence = {TOPIC: {"0": 21, "1": 50}}
    freeze = {
        "schema_version": "pnc_rca_activation_ingress_freeze_v1",
        "epoch_id": "rca-release-gate-activation-20260710",
        "state": "partitions_paused",
        "freeze_token": "freeze-release-test",
        "paused_at": OBSERVED_AT,
        "observed_at": OBSERVED_AT,
        "consumer_runtime_identity_sha256": runtime_sha256,
        "partition_positions": end_fence,
        "restart_required": False,
    }
    _write_json(
        consumer.health_path,
        {
            "runtime_identity": runtime_identity,
            "activation_freeze": freeze,
            "state": "activation_frozen",
            "healthy": True,
            "ok": True,
            "activation_required": True,
            "stats": {"blocked_partitions": 0},
        },
    )
    captured = {}

    class Admin:
        def __init__(self, **kwargs):
            captured["kwargs"] = kwargs

        def list_group_offsets(self, group_specs):
            captured["group_specs"] = group_specs
            partitions = group_specs[consumer.group_id]
            return {
                consumer.group_id: {
                    item: SimpleNamespace(
                        offset=(21 if item.partition == 0 else -1)
                    )
                    for item in partitions
                }
            }

        def close(self):
            captured["closed"] = True

    collector = release_gate_module.collect_live_activation_end_fence(
        consumer=consumer,
        partition_start_fence=start_fence,
        epoch_id="rca-release-gate-activation-20260710",
        consumer_runtime_identity_sha256=runtime_sha256,
        host_repo_root=release_gate_module.REPO_ROOT,
        now=NOW,
        admin_factory=Admin,
    )
    assert collector["observed_offsets"] == end_fence
    assert collector["offset_sources"] == {
        TOPIC: {
            "0": {
                "source": "broker_committed",
                "offset": 21,
                "broker_group_offset": 21,
                "freeze_position": 21,
                "start_offset": 10,
            },
            "1": {
                "source": "activation_start_fence",
                "offset": 50,
                "broker_group_offset": -1,
                "freeze_position": 50,
                "start_offset": 50,
            },
        }
    }
    assert collector["offset_sources_sha256"] == _sha256_json(
        collector["offset_sources"]
    )
    assert collector["freeze_receipt"] == freeze
    assert collector["side_effect_contract"]["offset_commit"] is False
    from kafka import KafkaAdminClient

    assert set(captured["kwargs"]).issubset(KafkaAdminClient.DEFAULT_CONFIG)
    assert "allow_auto_create_topics" not in captured["kwargs"]
    assert captured["closed"] is True

    class MissingOffsetAdmin(Admin):
        def list_group_offsets(self, group_specs):
            partitions = group_specs[consumer.group_id]
            return {
                consumer.group_id: {
                    item: SimpleNamespace(offset=21)
                    for item in partitions
                    if item.partition == 0
                }
            }

    missing = release_gate_module.collect_live_activation_end_fence(
        consumer=consumer,
        partition_start_fence=start_fence,
        epoch_id="rca-release-gate-activation-20260710",
        consumer_runtime_identity_sha256=runtime_sha256,
        host_repo_root=release_gate_module.REPO_ROOT,
        now=NOW,
        admin_factory=MissingOffsetAdmin,
    )
    assert missing["offset_sources"][TOPIC]["1"]["broker_group_offset"] is None

    invalid_health = json.loads(consumer.health_path.read_text(encoding="utf-8"))
    invalid_health["activation_freeze"]["partition_positions"][TOPIC]["1"] = 51
    _write_json(consumer.health_path, invalid_health)
    with pytest.raises(
        EvidenceError,
        match="activation_end_fence_uncommitted_offset_not_at_start",
    ):
        release_gate_module.collect_live_activation_end_fence(
            consumer=consumer,
            partition_start_fence=start_fence,
            epoch_id="rca-release-gate-activation-20260710",
            consumer_runtime_identity_sha256=runtime_sha256,
            host_repo_root=release_gate_module.REPO_ROOT,
            now=NOW,
            admin_factory=Admin,
        )

    _write_json(
        consumer.health_path,
        {
            **invalid_health,
            "activation_freeze": freeze,
        },
    )

    class MismatchAdmin(Admin):
        def list_group_offsets(self, group_specs):
            partitions = group_specs[consumer.group_id]
            return {
                consumer.group_id: {
                    item: SimpleNamespace(
                        offset=(22 if item.partition == 0 else -1)
                    )
                    for item in partitions
                }
            }

    with pytest.raises(
        EvidenceError,
        match="activation_end_fence_broker_freeze_mismatch",
    ):
        release_gate_module.collect_live_activation_end_fence(
            consumer=consumer,
            partition_start_fence=start_fence,
            epoch_id="rca-release-gate-activation-20260710",
            consumer_runtime_identity_sha256=runtime_sha256,
            host_repo_root=release_gate_module.REPO_ROOT,
            now=NOW,
            admin_factory=MismatchAdmin,
        )

    health = json.loads(consumer.health_path.read_text(encoding="utf-8"))
    health["state"] = "running"
    _write_json(consumer.health_path, health)
    with pytest.raises(EvidenceError, match="activation_end_fence_consumer_not_frozen"):
        release_gate_module.collect_live_activation_end_fence(
            consumer=consumer,
            partition_start_fence=start_fence,
            epoch_id="rca-release-gate-activation-20260710",
            consumer_runtime_identity_sha256=runtime_sha256,
            host_repo_root=release_gate_module.REPO_ROOT,
            now=NOW,
            admin_factory=Admin,
        )


def test_activation_confirmation_capsule_binds_exact_written_gate_receipt(
    tmp_path, monkeypatch
):
    gate_observed_at = datetime.now(timezone.utc).isoformat()
    gate_policy = {"evidence_max_age_seconds": 900}
    consumer_health_path = tmp_path / "kafka-consumer-health.json"
    runtime_identity = {
        "service_label": "local.pnc.rca-kafka-consumer",
        "pid": 42000,
        "process_create_time": 1_783_650_000.0,
    }
    runtime_identity_sha256 = _sha256_json(runtime_identity)
    freeze_receipt = {
        "schema_version": "pnc_rca_activation_ingress_freeze_v1",
        "epoch_id": "rca-release-gate-activation-20260710",
        "state": "partitions_paused",
        "freeze_token": "freeze-release-test",
        "paused_at": OBSERVED_AT,
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "consumer_runtime_identity_sha256": runtime_identity_sha256,
        "partition_positions": {TOPIC: {"0": 11, "1": 20}},
        "restart_required": False,
    }
    freeze_binding = release_gate_module._activation_ingress_freeze_binding(
        freeze_receipt=freeze_receipt,
        health_path=consumer_health_path,
    )
    health = {
        "runtime_identity": runtime_identity,
        "activation_freeze": freeze_receipt,
        "state": "activation_frozen",
        "healthy": True,
        "ok": True,
        "activation_required": True,
        "stats": {"blocked_partitions": 0},
    }
    write_receipt_atomic(consumer_health_path, health)
    confirm_input = {
        "epoch_id": "rca-release-gate-activation-20260710",
        "expected_state": "bounded_active",
        "target_state": "confirmed",
        "config_sha256": "c" * 64,
        "db_logical_identity_sha256": "d" * 64,
        "partition_start_fence_sha256": "e" * 64,
        "release_binding_sha256": "f" * 64,
        "partition_end_fence": {TOPIC: {"0": 11, "1": 20}},
        "partition_end_fence_sha256": _sha256_json(
            {TOPIC: {"0": 11, "1": 20}}
        ),
        "production_fingerprint_source": "release_gate_report.fingerprint",
        "production_gate_receipt_sha256_source": (
            "sha256(exact_written_release_gate_receipt)"
        ),
        "restart_between_gate_and_confirm": False,
    }
    release_binding_sha256 = "f" * 64
    resident_health = {}
    for index, artifact in enumerate(
        (
            "kafka_consumer_health",
            "outbox_dispatcher_health",
            "delivery_collector_health",
            "delivery_dispatcher_health",
        ),
        start=1,
    ):
        pid = 43000 + index
        resident_health[artifact] = {
            "runtime_identity_sha256": f"{index}" * 64,
            "loaded_runtime_sha256": f"{index + 4}" * 64,
            "process": {
                "pid": pid,
                "process_create_time": 1_783_650_000.0 + index,
                "boot_time": 1_783_000_000.0,
                "executable": "/candidate/.venv/bin/python",
                "cwd": "/candidate",
                "cmdline_sha256": "a" * 64,
                "required_environment_sha256": "b" * 64,
                "launchctl": {
                    "state": "running",
                    "pid": pid,
                    "plist_path_sha256": "c" * 64,
                    "program_arguments_sha256": "d" * 64,
                    "working_directory_sha256": "e" * 64,
                    "environment_sha256": "f" * 64,
                },
                "installed_plist": {"sha256": "9" * 64},
            },
        }
        if artifact == "outbox_dispatcher_health":
            resident_health[artifact]["capacity_admission"] = {
                "required": False,
                "ready": True,
                "state": "steady",
                "error_code": "",
                "capacity_mode": "steady",
                "authorization": None,
            }
    resident_binding = release_gate_module._project_resident_runtime_bindings(
        resident_health,
        artifact="test_confirmation_residents",
    )
    gateway_identity = {
        "service_label": "ai.hermes.gateway",
        "pid": 42001,
        "process_create_time": 1_783_650_001.0,
    }
    gateway_binding = {
        "state": "running_safe",
        "pid": gateway_identity["pid"],
        "process_create_time": gateway_identity["process_create_time"],
        "runtime_identity_sha256": _sha256_json(gateway_identity),
        "runtime_identity": gateway_identity,
        "verified_runtime_sha256": "7" * 64,
    }
    manual_runtime_barrier = {
        "host_commit": "1" * 40,
        "critical_files_sha256": "2" * 64,
        "runtime_identity_sha256": gateway_binding["runtime_identity_sha256"],
        "runtime_files_sha256": "3" * 64,
        "public_config_sha256": "4" * 64,
        "plist_sha256": "5" * 64,
        "interpreter_sha256": "6" * 64,
        "process_executable_sha256": "7" * 64,
        "module_origins_sha256": "8" * 64,
        "loaded_runtime_sha256": "9" * 64,
        "dependency_files_sha256": "a" * 64,
        "pid": gateway_identity["pid"],
        "process_create_time": gateway_identity["process_create_time"],
        "launchctl_config_sha256": "b" * 64,
        "resident_runtime_binding": resident_binding,
        "resident_runtime_sha256": _sha256_json(resident_binding),
    }
    check_details = {
        "activation_epoch": {
            "state": "bounded_active",
            "release_binding_sha256": release_binding_sha256,
        },
        "activation_production_candidate": {
            "ingress_frozen": True,
            "ingress_freeze_binding": freeze_binding,
            "confirm_input": confirm_input,
            "confirm_input_sha256": _sha256_json(confirm_input),
        },
        "activation_canary_bindings": {"slot_count": 3},
        "activation_writer_barrier": {
            "state": "bounded_active",
            "transition_performed": False,
            "production_confirmation_required": True,
            "release_binding_sha256": release_binding_sha256,
            "ingress_freeze_binding": freeze_binding,
            "confirm_input": confirm_input,
            "confirm_input_sha256": _sha256_json(confirm_input),
        },
        "activation_bootstrap_runtime": {"gateway": gateway_binding},
        "delivery_service_health": resident_health,
        "manual_gateway_runtime_barrier": manual_runtime_barrier,
        "runtime_dependencies": {},
        "contract_drift": {"ok": True, "status": "match"},
    }
    checks = [
        {
            "name": name,
            "ok": True,
            "code": "pass",
            "detail": check_details.get(name, {}),
        }
        for name in sorted(release_gate_module.PRODUCTION_RELEASE_CHECK_NAMES)
    ]
    config = {
        "consumer": {
            "topic": TOPIC,
            "health_path": str(consumer_health_path),
            "policy": {"policy_version": RULE},
        },
        "dispatcher": {},
        "cutover": {},
    }
    fingerprint_input = {
        "schema_version": RELEASE_GATE_SCHEMA_VERSION,
        "mode": "production",
        "expected_topic": TOPIC,
        "expected_rule_version": RULE,
        "config": config,
        "gate_policy": gate_policy,
        "contract": check_details["contract_drift"],
        "evidence_sha256": {},
        "checks": checks,
        "blockers": [],
        "warnings": [],
    }
    report = {
        "schema_version": RELEASE_GATE_SCHEMA_VERSION,
        "evaluated_at": gate_observed_at,
        "mode": "production",
        "ok": True,
        "fingerprint": _sha256_json(fingerprint_input),
        "config": config,
        "gate_policy": gate_policy,
        "checks": checks,
        "blockers": [],
        "warnings": [],
        "evidence_sha256": {},
    }
    receipt = tmp_path / "release-gate.json"
    write_receipt_atomic(receipt, report)
    capsule_path = release_gate_module.write_activation_confirmation_capsule(
        receipt,
        report,
    )
    bounded_activation = {
        "state": "bounded_active",
        "epoch_id": confirm_input["epoch_id"],
        "config_sha256": confirm_input["config_sha256"],
        "db_logical_identity_sha256": confirm_input[
            "db_logical_identity_sha256"
        ],
        "partition_start_fence_sha256": confirm_input[
            "partition_start_fence_sha256"
        ],
        "release_binding_sha256": confirm_input["release_binding_sha256"],
    }
    with pytest.raises(EvidenceError, match="commit_missing"):
        release_gate_module.read_activation_confirmation_capsule(
            capsule_path,
            receipt_path=receipt,
            current_activation=bounded_activation,
        )
    pair_commit_path = (
        release_gate_module.write_activation_confirmation_pair_commit(
            receipt,
            report,
            capsule_path=capsule_path,
        )
    )
    assert capsule_path.name == "release-gate.activation-confirmation.json"
    assert pair_commit_path.name == "release-gate.activation-confirmation.commit.json"
    pair_commit_raw = pair_commit_path.read_bytes()
    assert (
        release_gate_module.write_activation_confirmation_pair_commit(
            receipt,
            report,
            capsule_path=capsule_path,
        )
        == pair_commit_path
    )
    assert pair_commit_path.read_bytes() == pair_commit_raw
    capsule = json.loads(capsule_path.read_text(encoding="utf-8"))
    receipt_bytes = receipt.read_bytes()
    capsule_bytes = capsule_path.read_bytes()
    release_gate_module.write_receipt_no_clobber(
        receipt,
        report,
        conflict_code="production_release_gate_receipt_conflict",
    )
    assert (
        release_gate_module.write_activation_confirmation_capsule(receipt, report)
        == capsule_path
    )
    failed_rerun = release_gate_module._configuration_failure(
        mode="production",
        code="later_production_gate_failed",
        detail="test",
        now=NOW,
    )
    with pytest.raises(
        EvidenceError,
        match="production_release_gate_receipt_conflict",
    ):
        release_gate_module.write_receipt_no_clobber(
            receipt,
            failed_rerun,
            conflict_code="production_release_gate_receipt_conflict",
        )
    conflicting_capsule = copy.deepcopy(capsule)
    conflicting_capsule["created_at"] = NOW.isoformat()
    with pytest.raises(
        EvidenceError,
        match="activation_confirmation_capsule_conflict",
    ):
        release_gate_module.write_receipt_no_clobber(
            capsule_path,
            conflicting_capsule,
            conflict_code="activation_confirmation_capsule_conflict",
        )
    assert receipt.read_bytes() == receipt_bytes
    assert capsule_path.read_bytes() == capsule_bytes
    transition = capsule["transition_input"]
    assert transition["production_fingerprint"] == report["fingerprint"]
    assert transition["production_gate_receipt_sha256"] == hashlib.sha256(
        receipt.read_bytes()
    ).hexdigest()
    assert capsule["operator_supplied_scope_fields"] == []
    assert capsule["ingress_freeze_binding"] == freeze_binding
    health["activation_freeze"]["observed_at"] = datetime.now(
        timezone.utc
    ).isoformat()
    write_receipt_atomic(consumer_health_path, health)
    runtime_rechecks: list[str] = []
    monkeypatch.setattr(
        release_gate_module,
        "_recheck_confirmation_runtime_continuity",
        lambda *_args, **_kwargs: runtime_rechecks.append("checked"),
    )
    reread = release_gate_module.read_activation_confirmation_capsule(
        capsule_path,
        receipt_path=receipt,
        current_activation=bounded_activation,
    )
    assert reread == transition
    assert runtime_rechecks == ["checked", "checked"]
    assert activation_module._canonical_confirmation_transition(
        capsule_path=capsule_path,
        receipt_path=receipt,
        current_activation={
            "state": "bounded_active",
            "epoch_id": confirm_input["epoch_id"],
            "config_sha256": confirm_input["config_sha256"],
            "db_logical_identity_sha256": confirm_input[
                "db_logical_identity_sha256"
            ],
            "partition_start_fence_sha256": confirm_input[
                "partition_start_fence_sha256"
            ],
            "release_binding_sha256": confirm_input[
                "release_binding_sha256"
            ],
        },
    ) == transition

    health["state"] = "idle"
    health["activation_freeze"]["observed_at"] = (
        datetime.now(timezone.utc) - timedelta(hours=1)
    ).isoformat()
    write_receipt_atomic(consumer_health_path, health)
    confirmed_activation = {
        "state": "confirmed",
        "epoch_id": confirm_input["epoch_id"],
        "config_sha256": confirm_input["config_sha256"],
        "db_logical_identity_sha256": confirm_input[
            "db_logical_identity_sha256"
        ],
        "partition_start_fence_sha256": confirm_input[
            "partition_start_fence_sha256"
        ],
        "partition_end_fence_sha256": transition[
            "partition_end_fence_sha256"
        ],
        "production_fingerprint": transition["production_fingerprint"],
        "production_gate_receipt_sha256": transition[
            "production_gate_receipt_sha256"
        ],
    }
    assert (
        release_gate_module.read_activation_confirmation_capsule(
            capsule_path,
            receipt_path=receipt,
            current_activation=confirmed_activation,
        )
        == transition
    )

    health["state"] = "activation_frozen"
    health["activation_freeze"]["observed_at"] = datetime.now(
        timezone.utc
    ).isoformat()
    health["activation_freeze"]["freeze_token"] = "changed-after-gate"
    write_receipt_atomic(consumer_health_path, health)
    with pytest.raises(
        EvidenceError,
        match="activation_confirmation_consumer_not_frozen",
    ):
        release_gate_module.read_activation_confirmation_capsule(
            capsule_path,
            receipt_path=receipt,
            current_activation={
                "state": "bounded_active",
                "epoch_id": confirm_input["epoch_id"],
                "config_sha256": confirm_input["config_sha256"],
                "db_logical_identity_sha256": confirm_input[
                    "db_logical_identity_sha256"
                ],
                "partition_start_fence_sha256": confirm_input[
                    "partition_start_fence_sha256"
                ],
                "release_binding_sha256": confirm_input[
                    "release_binding_sha256"
                ],
            },
        )

    stale_report = copy.deepcopy(report)
    stale_report["evaluated_at"] = (
        datetime.now(timezone.utc) - timedelta(seconds=901)
    ).isoformat()
    stale_receipt = tmp_path / "stale-release-gate.json"
    write_receipt_atomic(stale_receipt, stale_report)
    stale_capsule_path = (
        release_gate_module.write_activation_confirmation_capsule(
            stale_receipt,
            stale_report,
        )
    )
    release_gate_module.write_activation_confirmation_pair_commit(
        stale_receipt,
        stale_report,
        capsule_path=stale_capsule_path,
    )
    with pytest.raises(
        EvidenceError,
        match="activation_confirmation_receipt_stale",
    ):
        release_gate_module.read_activation_confirmation_capsule(
            stale_capsule_path,
            receipt_path=stale_receipt,
            current_activation={
                **confirmed_activation,
                "state": "bounded_active",
                "release_binding_sha256": confirm_input[
                    "release_binding_sha256"
                ],
            },
        )
    stale_transition = json.loads(
        stale_capsule_path.read_text(encoding="utf-8")
    )["transition_input"]
    assert (
        release_gate_module.read_activation_confirmation_capsule(
            stale_capsule_path,
            receipt_path=stale_receipt,
            current_activation={
                **confirmed_activation,
                "production_gate_receipt_sha256": stale_transition[
                    "production_gate_receipt_sha256"
                ],
            },
        )
        == stale_transition
    )

    tampered = copy.deepcopy(report)
    tampered["fingerprint"] = "8" * 64
    with pytest.raises(EvidenceError, match="activation_confirmation_fingerprint_invalid"):
        release_gate_module.write_activation_confirmation_capsule(receipt, tampered)

    pair_commit = json.loads(pair_commit_path.read_text(encoding="utf-8"))
    pair_commit["pair_sha256"] = "0" * 64
    write_receipt_atomic(pair_commit_path, pair_commit)
    with pytest.raises(
        EvidenceError,
        match="activation_confirmation_pair_commit_invalid",
    ):
        release_gate_module.read_activation_confirmation_capsule(
            capsule_path,
            receipt_path=receipt,
            current_activation=bounded_activation,
        )


def test_no_clobber_recovers_unique_interrupted_hardlink(tmp_path):
    destination = tmp_path / "release.json"
    report = {"ok": True, "fingerprint": "a" * 64}
    payload = (
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    interrupted = tmp_path / f".{destination.name}.crash.no-clobber.tmp"
    interrupted.write_bytes(payload)
    interrupted.chmod(0o600)
    os.link(interrupted, destination)
    assert destination.stat().st_nlink == 2

    release_gate_module.write_receipt_no_clobber(
        destination,
        report,
        conflict_code="test_release_conflict",
    )

    assert destination.read_bytes() == payload
    assert destination.stat().st_nlink == 1
    assert not interrupted.exists()


def test_no_clobber_rejects_ambiguous_interrupted_hardlink(tmp_path):
    destination = tmp_path / "release.json"
    report = {"ok": True, "fingerprint": "a" * 64}
    payload = (
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    interrupted = tmp_path / f".{destination.name}.crash.no-clobber.tmp"
    interrupted.write_bytes(payload)
    interrupted.chmod(0o600)
    os.link(interrupted, destination)
    os.link(interrupted, tmp_path / "foreign-hardlink")

    with pytest.raises(EvidenceError, match="test_release_conflict"):
        release_gate_module.write_receipt_no_clobber(
            destination,
            report,
            conflict_code="test_release_conflict",
        )


def test_release_gate_requires_all_real_kafka_e2e_work_items(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "preauthorization")
    replay_path = settings.evidence_dir / release_gate_module.KAFKA_RECENT_REPLAY_FILENAME
    replay_receipt = json.loads(replay_path.read_text(encoding="utf-8"))
    replay_receipt["result"]["e2e_canary"]["complete"] = False
    replay_receipt["result"]["e2e_canary"]["missing_work_item_ids"] = [
        replay_receipt["result"]["e2e_canary"]["expected_work_item_ids"][0]
    ]
    _write_remote_soak_manifest(replay_path, replay_receipt)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("preauthorization"),
        now=NOW,
    )

    by_name = {item["name"]: item for item in report["checks"]}
    assert report["ok"] is False
    assert by_name["kafka_recent_replay"]["code"] == (
        "kafka_recent_replay_e2e_incomplete"
    )


def test_release_gate_rehashes_real_kafka_e2e_manifest(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "preauthorization")
    replay_path = settings.evidence_dir / release_gate_module.KAFKA_RECENT_REPLAY_FILENAME
    replay_receipt = json.loads(replay_path.read_text(encoding="utf-8"))
    manifest_path = Path(
        replay_receipt["result"]["e2e_canary"]["manifest"]["path"]
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["work_item_ids"].append("7000000002")
    _write_remote_soak_manifest(manifest_path, manifest)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("preauthorization"),
        now=NOW,
    )

    by_name = {item["name"]: item for item in report["checks"]}
    assert report["ok"] is False
    assert by_name["kafka_recent_replay"]["code"] == (
        "kafka_recent_replay_e2e_manifest_invalid"
    )


@pytest.mark.parametrize("source_artifact", ["feishu_receipt", "screenshot"])
def test_release_gate_rehashes_real_kafka_e2e_source_artifacts(
    tmp_path, source_artifact
):
    consumer, dispatcher, settings = _gate(tmp_path, "preauthorization")
    replay_path = settings.evidence_dir / release_gate_module.KAFKA_RECENT_REPLAY_FILENAME
    replay_receipt = json.loads(replay_path.read_text(encoding="utf-8"))
    manifest_path = Path(
        replay_receipt["result"]["e2e_canary"]["manifest"]["path"]
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    feishu_receipt_path = Path(manifest["source"]["feishu_receipt_path"])
    if source_artifact == "feishu_receipt":
        target = feishu_receipt_path
    else:
        feishu_receipt = json.loads(feishu_receipt_path.read_text(encoding="utf-8"))
        target = Path(feishu_receipt["user_evidence"]["screenshot_path"])
    target.write_bytes(target.read_bytes() + b"tampered")

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("preauthorization"),
        now=NOW,
    )

    by_name = {item["name"]: item for item in report["checks"]}
    assert report["ok"] is False
    assert by_name["kafka_recent_replay"]["code"] == (
        "kafka_recent_replay_e2e_source_invalid"
    )


def test_release_gate_rehashes_real_feishu_frame_census(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "preauthorization")
    replay_path = settings.evidence_dir / release_gate_module.KAFKA_RECENT_REPLAY_FILENAME
    replay_receipt = json.loads(replay_path.read_text(encoding="utf-8"))
    manifest_path = Path(
        replay_receipt["result"]["e2e_canary"]["manifest"]["path"]
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    feishu_receipt_path = Path(manifest["source"]["feishu_receipt_path"])
    feishu_receipt = json.loads(feishu_receipt_path.read_text(encoding="utf-8"))
    frame_census_path = Path(feishu_receipt["result"]["frame_census_path"])
    frame_census_path.write_bytes(frame_census_path.read_bytes() + b"tampered")

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("preauthorization"),
        now=NOW,
    )

    by_name = {item["name"]: item for item in report["checks"]}
    assert report["ok"] is False
    assert by_name["kafka_recent_replay"]["code"] == (
        "kafka_recent_replay_e2e_frame_census_invalid"
    )


@pytest.mark.parametrize(
    ("mode", "expected_checks", "expected_ok", "expected_blockers"),
    [
        ("shadow", 9, True, []),
        ("preauthorization", 21, True, []),
        ("preproduction", 22, True, []),
        ("canary", 22, True, []),
        (
            "production_bootstrap",
            33,
            False,
            [
                "activation_current_epoch_missing",
                (
                    "outbox_dispatcher_health_capacity_admission_live_"
                    "rca_active_release_binding_unavailable"
                ),
                "activation_production_candidate_epoch_unverified",
                "release_plan_invalid",
                "auxiliary_runtime_prerequisite_unverified",
                "fresh_install_materialization_receipt_invalid",
                "activation_bootstrap_migration_unverified",
                "canary_vm_bootstrap_release_bom_mismatch",
                "manual_success_canary_commit_missing",
                "manual_success_canary_commit_missing",
                "activation_canary_epoch_unverified",
                "manual_gateway_runtime_barrier_prerequisite_unverified",
                "activation_writer_barrier_prerequisite_unverified",
            ],
        ),
        (
            "production",
            32,
            False,
            [
                "activation_current_epoch_missing",
                "activation_production_candidate_epoch_unverified",
                "auxiliary_runtime_prerequisite_unverified",
                "fresh_install_materialization_receipt_invalid",
                "activation_bootstrap_migration_unverified",
                "manual_success_canary_commit_missing",
                "manual_success_canary_commit_missing",
                "activation_canary_epoch_unverified",
                "manual_gateway_runtime_barrier_prerequisite_unverified",
                "activation_writer_barrier_prerequisite_unverified",
            ],
        ),
    ],
)
def test_each_release_mode_passes_only_with_its_complete_evidence_set(
    tmp_path, mode, expected_checks, expected_ok, expected_blockers
):
    consumer, dispatcher, settings = _gate(tmp_path, mode)
    cutover = _cutover(mode)
    if mode in release_gate_module.PRODUCTION_MODES:
        cutover = _complete_production_manual_baseline(
            tmp_path,
            consumer=consumer,
            dispatcher=dispatcher,
            settings=settings,
        )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=cutover,
        now=NOW,
    )

    assert report["schema_version"] == RELEASE_GATE_SCHEMA_VERSION
    assert report["ok"] is expected_ok, report["blockers"]
    assert report["blockers"] == expected_blockers
    assert len(report["checks"]) == expected_checks
    assert all(check["ok"] for check in report["checks"]) is expected_ok
    assert len(report["fingerprint"]) == 64
    if mode == "shadow":
        assert report["warnings"] == [
            "build_manifest_missing",
            "legacy_auto_execution_still_enabled",
        ]
    else:
        build_detail = next(
            check["detail"]
            for check in report["checks"]
            if check["name"] == "build_manifest"
        )
        assert build_detail["full_tree_clean"] == {
            "host": True,
            "vm": True,
            "vm_worker": True,
        }
        assert build_detail["scoped_execution_clean"] == {"workspace": True}
        workspace_governance = build_detail["workspace_governance"]
        assert workspace_governance["execution_closure"]["ok"] is True
        assert workspace_governance["execution_closure"]["required_paths"] == list(
            release_gate_module.WORKSPACE_EXECUTION_CLOSURE_RELATIVE_PATHS
        )
        assert workspace_governance["unscoped_drift"] == {
            "classification": "DRIFT-PREEXISTING",
            "dirty_count": 0,
            "status_sha256": EMPTY_GIT_STATUS_SHA256,
            "blocking": False,
        }
        assert build_detail["provenance_sources"] == {
            "host": "local_git",
            "vm": "ssh-mini-agent",
            "vm_worker": "ssh-mini-agent",
            "workspace": "local_git_scoped_closure",
        }
        assert set(build_detail["external_dependencies"]) == {
            "ssh_mini_agent",
            "vm_ssh_execution_protocol_v2",
        }
        assert all(
            item["regular_file"] is True
            and item["symlink"] is False
            and item["owner_uid"] == os.getuid()
            and len(item["sha256"]) == 64
            for item in build_detail["external_dependencies"].values()
        )
        assert len(build_detail["release_bom_sha256"]) == 64
        assert (
            build_detail["canary_collector_sha256"]
            == hashlib.sha256(
                (
                    settings.host_repo_root
                    / release_gate_module.CANARY_COLLECTOR_RELATIVE_PATH
                ).read_bytes()
            ).hexdigest()
        )


def test_canary_bootstrap_mode_fails_closed_without_live_authorization(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "canary_bootstrap")

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary_bootstrap"),
        now=NOW,
    )

    by_name = {item["name"]: item for item in report["checks"]}
    assert report["mode"] == "canary_bootstrap"
    assert report["ok"] is False
    assert len(report["checks"]) == 23
    assert by_name["bootstrap_capacity_authorization"]["ok"] is False
    assert by_name["bootstrap_capacity_authorization"]["code"] in {
        "release_plan_invalid",
        "release_plan_missing",
    }
    assert by_name["delivery_service_health"]["ok"] is False
    assert "capacity_admission_live" in by_name["delivery_service_health"]["code"]
    assert all(
        item["ok"] is True
        for item in report["checks"]
        if item["name"]
        not in {"bootstrap_capacity_authorization", "delivery_service_health"}
    )


def test_preauthorization_capsule_is_no_clobber_and_rechecks_live_scope(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "preauthorization")
    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("preauthorization"),
        now=NOW,
    )
    assert report["ok"] is True, report["blockers"]
    receipt = settings.evidence_dir / "activation-preauthorization-gate.json"
    release_gate_module.write_receipt_no_clobber(
        receipt,
        report,
        conflict_code="test_preauthorization_receipt_conflict",
    )
    capsule = release_gate_module.write_activation_preauthorization_capsule(
        receipt,
        report,
        evidence_dir=settings.evidence_dir,
        control_db_path=dispatcher.control_db_path,
    )
    first_raw = capsule.read_bytes()
    assert stat.S_IMODE(capsule.stat().st_mode) == 0o600
    assert (
        release_gate_module.write_activation_preauthorization_capsule(
            receipt,
            report,
            evidence_dir=settings.evidence_dir,
            control_db_path=dispatcher.control_db_path,
        )
        == capsule
    )
    assert capsule.read_bytes() == first_raw
    pair_commit = release_gate_module.activation_stage_pair_commit_path(
        receipt,
        mode="preauthorization",
    )
    assert pair_commit.is_file()
    assert json.loads(pair_commit.read_text(encoding="utf-8"))[
        "publication_complete"
    ] is True
    normalized = release_gate_module._read_activation_preauthorization_capsule_bundle(
        capsule,
        control_db_path=dispatcher.control_db_path,
        now=NOW,
    )["normalized"]
    assert set(normalized) == {
        "epoch_id",
        "initial_state",
        "preauthorization_fingerprint",
        "preauthorization_gate_receipt_sha256",
        "preauthorization_capsule_sha256",
        "config_sha256",
        "db_logical_identity",
        "db_logical_identity_sha256",
        "partition_start_fence",
        "partition_start_fence_sha256",
        "migration_receipt_raw_sha256",
        "materialization_receipt_raw_sha256",
        "broker_t0_observation_sha256",
        "canary_plan_raw_sha256",
    }
    assert normalized["initial_state"] == "safe_off"
    assert normalized["partition_start_fence"] == {TOPIC: {"0": 10, "1": 20}}
    assert normalized["preauthorization_gate_receipt_sha256"] == hashlib.sha256(
        receipt.read_bytes()
    ).hexdigest()
    assert normalized["preauthorization_capsule_sha256"] == hashlib.sha256(
        first_raw
    ).hexdigest()

    with pytest.raises(EvidenceError, match="activation_preauthorization_capsule_stale"):
        release_gate_module._read_activation_preauthorization_capsule_bundle(
            capsule,
            control_db_path=dispatcher.control_db_path,
            now=NOW
            + timedelta(
                seconds=(
                    release_gate_module.ACTIVATION_STAGE_CAPSULE_MAX_AGE_SECONDS + 1
                )
            ),
        )


def test_stage_capsule_requires_commit_marker_and_recovers_it(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "preauthorization")
    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("preauthorization"),
        now=NOW,
    )
    receipt = settings.evidence_dir / "activation-preauthorization-marker.json"
    release_gate_module.write_receipt_no_clobber(
        receipt,
        report,
        conflict_code="test_preauthorization_receipt_conflict",
    )
    capsule = release_gate_module.write_activation_preauthorization_capsule(
        receipt,
        report,
        evidence_dir=settings.evidence_dir,
        control_db_path=dispatcher.control_db_path,
    )
    marker = release_gate_module.activation_stage_pair_commit_path(
        receipt,
        mode="preauthorization",
    )
    marker.unlink()

    with pytest.raises(EvidenceError):
        release_gate_module._read_activation_preauthorization_capsule_bundle(
            capsule,
            control_db_path=dispatcher.control_db_path,
            now=NOW,
        )

    assert (
        release_gate_module.write_activation_preauthorization_capsule(
            receipt,
            report,
            evidence_dir=settings.evidence_dir,
            control_db_path=dispatcher.control_db_path,
        )
        == capsule
    )
    assert marker.is_file()
    release_gate_module._read_activation_preauthorization_capsule_bundle(
        capsule,
        control_db_path=dispatcher.control_db_path,
        now=NOW,
    )


@pytest.mark.parametrize("mode", ["preauthorization", "preproduction"])
def test_stage_publication_recovers_receipt_only_without_reevaluation(
    tmp_path, monkeypatch, mode
):
    consumer, dispatcher, settings = _gate(tmp_path, mode)
    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover(mode),
        now=NOW,
    )
    assert report["ok"] is True, report["blockers"]
    receipt = settings.evidence_dir / f"activation-{mode}-recovery.json"
    release_gate_module.write_receipt_no_clobber(
        receipt,
        report,
        conflict_code=f"test_{mode}_receipt_conflict",
    )
    original_raw = receipt.read_bytes()

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW if tz is not None else NOW.replace(tzinfo=None)

    monkeypatch.setattr(release_gate_module, "datetime", FixedDateTime)
    recovered = release_gate_module._publish_or_resume_activation_stage(
        mode=mode,
        receipt_path=receipt,
        report=None,
        evidence_dir=settings.evidence_dir,
        control_db_path=dispatcher.control_db_path,
        consumer=consumer,
        dispatcher=dispatcher,
        cutover=_cutover(mode),
        preauthorization_capsule=settings.preauthorization_capsule_path,
    )

    assert recovered == report
    assert receipt.read_bytes() == original_raw
    capsule = (
        release_gate_module.activation_preauthorization_capsule_path(receipt)
        if mode == "preauthorization"
        else release_gate_module.activation_preproduction_capsule_path(receipt)
    )
    assert capsule.is_file()
    assert release_gate_module.activation_stage_pair_commit_path(
        receipt,
        mode=mode,
    ).is_file()


@pytest.mark.parametrize(
    ("mode", "receipt_name", "writer_name"),
    [
        (
            "preauthorization",
            "preauthorization-gate.json",
            "write_activation_preauthorization_capsule",
        ),
        (
            "preproduction",
            "preproduction-gate.json",
            "write_activation_preproduction_capsule",
        ),
    ],
)
def test_complete_stage_triplet_replays_after_epoch_advances_without_writer(
    tmp_path,
    monkeypatch,
    mode,
    receipt_name,
    writer_name,
):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    receipt = settings.evidence_dir / receipt_name
    assert receipt.is_file()
    assert release_gate_module.activation_stage_pair_commit_path(
        receipt,
        mode=mode,
    ).is_file()

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW if tz is not None else NOW.replace(tzinfo=None)

    monkeypatch.setattr(release_gate_module, "datetime", FixedDateTime)

    def reject_writer(*_args, **_kwargs):
        raise AssertionError("complete stage publication must be read-only")

    monkeypatch.setattr(release_gate_module, writer_name, reject_writer)
    replayed = release_gate_module._publish_or_resume_activation_stage(
        mode=mode,
        receipt_path=receipt,
        report=None,
        evidence_dir=settings.evidence_dir,
        control_db_path=dispatcher.control_db_path,
        consumer=consumer,
        dispatcher=dispatcher,
        cutover=_cutover(mode),
        preauthorization_capsule=None,
    )

    assert replayed is not None
    assert replayed["mode"] == mode
    assert replayed["ok"] is True


def test_preauthorization_capsule_crosses_release_activation_contract(
    tmp_path, monkeypatch
):
    consumer, dispatcher, settings = _gate(tmp_path, "preauthorization")
    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("preauthorization"),
        now=NOW,
    )
    assert report["ok"] is True, report["blockers"]
    receipt = settings.evidence_dir / "activation-preauthorization-integration.json"
    release_gate_module.write_receipt_no_clobber(
        receipt,
        report,
        conflict_code="test_preauthorization_receipt_conflict",
    )
    capsule = release_gate_module.write_activation_preauthorization_capsule(
        receipt,
        report,
        evidence_dir=settings.evidence_dir,
        control_db_path=dispatcher.control_db_path,
    )

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW if tz is not None else NOW.replace(tzinfo=None)

    monkeypatch.setattr(release_gate_module, "datetime", FixedDateTime)
    normalized = activation_module._canonical_preauthorization_input(
        capsule,
        control_db_path=dispatcher.control_db_path,
    )

    assert normalized["canary_plan_raw_sha256"] == hashlib.sha256(
        (settings.evidence_dir / "canary_plan.json").read_bytes()
    ).hexdigest()


def test_preproduction_capsule_consumes_exact_safe_off_binding(tmp_path, monkeypatch):
    consumer, dispatcher, settings = _gate(tmp_path, "preproduction")
    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("preproduction"),
        now=NOW,
    )
    assert report["ok"] is True, report["blockers"]
    receipt = settings.evidence_dir / "activation-preproduction-gate.json"
    release_gate_module.write_receipt_no_clobber(
        receipt,
        report,
        conflict_code="test_preproduction_receipt_conflict",
    )
    capsule = release_gate_module.write_activation_preproduction_capsule(
        receipt,
        report,
        evidence_dir=settings.evidence_dir,
        control_db_path=dispatcher.control_db_path,
        preauthorization_capsule=settings.preauthorization_capsule_path,
    )
    from gateway.pnc_rca_control_store import RcaControlStore

    current = RcaControlStore(
        dispatcher.control_db_path,
        require_current=True,
    ).activation_epoch()
    transition = release_gate_module._read_activation_preproduction_capsule_bundle(
        capsule,
        control_db_path=dispatcher.control_db_path,
        current_activation=current,
        now=NOW,
    )["normalized"]
    assert set(transition) == {
        "epoch_id",
        "expected_state",
        "target_state",
        "expected_preauthorization_fingerprint",
        "expected_preauthorization_gate_receipt_sha256",
        "expected_preauthorization_capsule_sha256",
        "expected_config_sha256",
        "expected_db_logical_identity_sha256",
        "expected_partition_start_fence_sha256",
        "canary_slot_plan",
        "canary_slot_plan_sha256",
        "preproduction_fingerprint",
        "preproduction_gate_receipt_sha256",
        "preproduction_capsule_sha256",
    }
    assert transition["expected_state"] == "safe_off"
    assert transition["target_state"] == "preauthorized"
    assert transition["epoch_id"] == current["epoch_id"]
    assert transition["canary_slot_plan"] == _canonical_activation_slot_plan()
    assert transition["canary_slot_plan_sha256"] == _sha256_json(
        _canonical_activation_slot_plan()
    )
    assert transition["preproduction_gate_receipt_sha256"] == hashlib.sha256(
        receipt.read_bytes()
    ).hexdigest()

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW if tz is not None else NOW.replace(tzinfo=None)

    monkeypatch.setattr(release_gate_module, "datetime", FixedDateTime)
    assert activation_module._canonical_preproduction_transition(
        capsule,
        control_db_path=dispatcher.control_db_path,
        current_activation=current,
    ) == transition


def test_stage_capsule_rejects_receipt_replacement_and_conflicting_reuse(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "preauthorization")
    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("preauthorization"),
        now=NOW,
    )
    receipt = settings.evidence_dir / "replace-bound-receipt.json"
    release_gate_module.write_receipt_no_clobber(
        receipt,
        report,
        conflict_code="test_stage_receipt_conflict",
    )
    capsule = release_gate_module.write_activation_preauthorization_capsule(
        receipt,
        report,
        evidence_dir=settings.evidence_dir,
        control_db_path=dispatcher.control_db_path,
    )
    replaced = copy.deepcopy(report)
    replaced["evaluated_at"] = (NOW + timedelta(seconds=1)).isoformat()
    receipt.unlink()
    release_gate_module.write_receipt_no_clobber(
        receipt,
        replaced,
        conflict_code="test_stage_receipt_conflict",
    )
    with pytest.raises(EvidenceError):
        release_gate_module._read_activation_preauthorization_capsule_bundle(
            capsule,
            control_db_path=dispatcher.control_db_path,
            now=NOW,
        )
    with pytest.raises(EvidenceError, match="activation_preauthorization_capsule_conflict"):
        release_gate_module.write_activation_preauthorization_capsule(
            receipt,
            replaced,
            evidence_dir=settings.evidence_dir,
            control_db_path=dispatcher.control_db_path,
        )


def test_preproduction_rejects_preauthorized_epoch_and_canary_is_unconsumed(
    tmp_path,
):
    consumer, dispatcher, settings = _gate(tmp_path, "preproduction")
    from gateway.pnc_rca_control_store import RcaControlStore

    store = RcaControlStore(dispatcher.control_db_path, require_current=True)
    current = store.activation_epoch()
    store.preauthorize_activation_epoch(
        epoch_id=current["epoch_id"],
        preproduction_fingerprint="a" * 64,
        preproduction_gate_receipt_sha256="b" * 64,
        preproduction_capsule_sha256="c" * 64,
        expected_preauthorization_fingerprint=current[
            "preauthorization_fingerprint"
        ],
        expected_preauthorization_gate_receipt_sha256=current[
            "preauthorization_gate_receipt_sha256"
        ],
        expected_preauthorization_capsule_sha256=current[
            "preauthorization_capsule_sha256"
        ],
        expected_config_sha256=current["config_sha256"],
        expected_db_logical_identity_sha256=current[
            "db_logical_identity_sha256"
        ],
        expected_partition_start_fence_sha256=current[
            "partition_start_fence_sha256"
        ],
        operator="release-test",
        reason="wrong state fixture",
        now=NOW,
    )
    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("preproduction"),
        now=NOW,
    )
    assert "activation_preproduction_state_mismatch" in report["blockers"]

    canary_root = tmp_path / "canary"
    canary_root.mkdir()
    canary_consumer, canary_dispatcher, canary_settings = _gate(canary_root, "canary")
    canary_report = evaluate_release_gate(
        consumer=canary_consumer,
        dispatcher=canary_dispatcher,
        settings=canary_settings,
        cutover=_cutover("canary"),
        now=NOW,
    )
    detail = next(
        item["detail"]
        for item in canary_report["checks"]
        if item["name"] == "activation_epoch"
    )
    assert detail["state"] == "bounded_active"
    assert detail["authorization_complete"] is True
    assert detail["slots_unconsumed"] is True
    assert detail["authorized_slots"] == sorted(
        release_gate_module.REQUIRED_ACTIVATION_SLOTS
    )


def test_canary_requires_store_migration_receipt(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    (settings.evidence_dir / "store_migration_receipt.json").unlink()

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert "store_migration_receipt_missing" in report["blockers"]


def test_canary_store_migration_receipt_proves_fresh_install_materialization(
    tmp_path,
):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    check = next(item for item in report["checks"] if item["name"] == "store_migration")
    assert check["ok"] is True
    assert check["detail"]["mode"] == "fresh_create"
    assert check["detail"]["migration_state"] == "fresh_install"
    assert check["detail"]["rollback_ready"] is True
    assert check["detail"]["migration_receipt_rollback_ready"] is False
    assert check["detail"]["materialization_required"] is True
    assert check["detail"]["fresh_install_materialization"][
        "destination_count"
    ] == 1
    assert check["detail"]["same_database"] is True
    assert check["detail"]["writer_stop_service_count"] == 5
    assert check["detail"]["compatibility_probe"] == (
        "bom_pinned_predecessor_validator_v1"
    )
    [database] = check["detail"]["database_drills"]
    assert database["roles"] == ["control", "delivery"]
    assert database["pre_schemas"] == {
        "control": release_gate_module.CONTROL_PREDECESSOR_SCHEMA_VERSION,
        "delivery": release_gate_module.DELIVERY_PREDECESSOR_SCHEMA_VERSION,
    }
    assert database["post_schemas"] == {
        "control": release_gate_module.CONTROL_STORE_SCHEMA_VERSION,
        "delivery": release_gate_module.DELIVERY_STORE_SCHEMA_VERSION,
    }


def _check_fresh_install_fixture(
    consumer,
    dispatcher,
    settings,
    *,
    activation=None,
):
    expected_config_sha256 = _runtime_config_sha256(
        consumer,
        dispatcher,
        "canary",
    )
    activation = activation or release_gate_module._check_activation_epoch(
        control_db_path=dispatcher.control_db_path,
        mode="canary",
        expected_config_sha256=expected_config_sha256,
    )
    evidence_hashes: dict[str, str] = {}
    migration = release_gate_module._load_evidence(
        settings.evidence_dir,
        "store_migration_receipt.json",
        evidence_hashes,
    )
    materialization = release_gate_module._load_evidence(
        settings.evidence_dir,
        "fresh_install_materialization_receipt.json",
        evidence_hashes,
    )
    capacity_initialization = release_gate_module._load_evidence(
        settings.evidence_dir,
        "capacity_transition_initialization_receipt.json",
        evidence_hashes,
    )
    return release_gate_module._check_store_migration_receipt(
        migration,
        dispatcher=dispatcher,
        settings=settings,
        expected_host_commit=_git(settings.host_repo_root, "rev-parse", "HEAD"),
        now=NOW,
        max_age_seconds=settings.evidence_max_age_seconds,
        release_mode="canary",
        migration_raw_sha256=evidence_hashes["store_migration_receipt.json"],
        materialization_receipt=materialization,
        materialization_raw_sha256=evidence_hashes[
            "fresh_install_materialization_receipt.json"
        ],
        capacity_initialization_receipt=capacity_initialization,
        capacity_initialization_raw_sha256=evidence_hashes[
            "capacity_transition_initialization_receipt.json"
        ],
        activation=activation,
    )


def test_fresh_install_gate_requires_materialization_receipt(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    (settings.evidence_dir / "fresh_install_materialization_receipt.json").unlink()

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert "fresh_install_materialization_receipt_missing" in report["blockers"]


def test_store_gate_requires_original_capacity_initialization_receipt(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    (
        settings.evidence_dir
        / "capacity_transition_initialization_receipt.json"
    ).unlink()

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert "capacity_transition_initialization_receipt_missing" in report[
        "blockers"
    ]


def test_store_gate_allows_cross_release_steady_with_original_receipt(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    from gateway.pnc_rca_control_store import RcaControlStore

    store = RcaControlStore(dispatcher.control_db_path, require_current=True)
    store.compare_and_set_capacity_steady(
        expected_generation=1,
        release_id=dispatcher.release_id,
        bootstrap_epoch_id=dispatcher.bootstrap_epoch_id,
        final_ledger_sha256="1" * 64,
        transition_authorization_sha256="2" * 64,
        transition_authorization_fingerprint="3" * 64,
        transition_receipt_sha256="4" * 64,
        transition_receipt_fingerprint="5" * 64,
        commit_marker_sha256="6" * 64,
        commit_marker_fingerprint="7" * 64,
        evidence_bundle_sha256="8" * 64,
        evidence_bundle_fingerprint="9" * 64,
        authorization_issued_at=(NOW - timedelta(seconds=20)).isoformat(),
        authorization_expires_at=(NOW + timedelta(minutes=5)).isoformat(),
        receipt_created_at=(NOW - timedelta(seconds=15)).isoformat(),
        marker_committed_at=(NOW - timedelta(seconds=10)).isoformat(),
        now=NOW,
    )
    migration_module._checkpoint_restore(dispatcher.control_db_path)
    later_dispatcher = replace(
        dispatcher,
        release_id="later-software-release-20260714",
        bootstrap_epoch_id="later-software-epoch-20260714",
    )
    original_config_sha256 = _runtime_config_sha256(
        consumer, dispatcher, "canary"
    )
    activation = release_gate_module._check_activation_epoch(
        control_db_path=dispatcher.control_db_path,
        mode="canary",
        expected_config_sha256=original_config_sha256,
    )
    evidence_hashes: dict[str, str] = {}
    migration = release_gate_module._load_evidence(
        settings.evidence_dir,
        "store_migration_receipt.json",
        evidence_hashes,
    )
    materialization = release_gate_module._load_evidence(
        settings.evidence_dir,
        "fresh_install_materialization_receipt.json",
        evidence_hashes,
    )
    capacity = release_gate_module._load_evidence(
        settings.evidence_dir,
        "capacity_transition_initialization_receipt.json",
        evidence_hashes,
    )

    detail = release_gate_module._check_store_migration_receipt(
        migration,
        dispatcher=later_dispatcher,
        settings=settings,
        expected_host_commit=_git(settings.host_repo_root, "rev-parse", "HEAD"),
        now=NOW,
        max_age_seconds=settings.evidence_max_age_seconds,
        release_mode="canary",
        migration_raw_sha256=evidence_hashes["store_migration_receipt.json"],
        materialization_receipt=materialization,
        materialization_raw_sha256=evidence_hashes[
            "fresh_install_materialization_receipt.json"
        ],
        capacity_initialization_receipt=capacity,
        capacity_initialization_raw_sha256=evidence_hashes[
            "capacity_transition_initialization_receipt.json"
        ],
        activation=activation,
        expected_config_sha256=original_config_sha256,
    )

    assert detail["capacity_initialization"]["state"] == "STEADY_ACTIVE"
    assert detail["capacity_initialization"]["generation"] == 2
    assert detail["capacity_initialization"][
        "origin_matches_current_release"
    ] is False


def test_fresh_install_gate_rejects_unreleased_maintenance_marker(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    marker_path = Path(f"{dispatcher.control_db_path}.pnc-rca-maintenance")
    marker_path.write_text('{"state":"active"}\n', encoding="utf-8")
    marker_path.chmod(0o600)

    with pytest.raises(EvidenceError) as error:
        _check_fresh_install_fixture(consumer, dispatcher, settings)

    assert error.value.code == "fresh_install_maintenance_not_released"


def test_fresh_install_gate_requires_receipted_journal(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    materialization = json.loads(
        (
            settings.evidence_dir / "fresh_install_materialization_receipt.json"
        ).read_text(encoding="utf-8")
    )
    journal_id = materialization["materialization_journal"]["journal_id"]
    (
        settings.evidence_dir
        / f"fresh_install_materialization_journal.{journal_id}.receipted.json"
    ).unlink()

    with pytest.raises(EvidenceError) as error:
        _check_fresh_install_fixture(consumer, dispatcher, settings)

    assert error.value.code == "fresh_install_journal_not_receipted"


@pytest.mark.parametrize(
    ("suffix", "expected_code"),
    [
        ("-wal", "fresh_install_live_sidecar_present"),
        ("-shm", "fresh_install_live_sidecar_present"),
        ("-journal", "store_migration_current_snapshot_invalid"),
        (".pnc-rca-tombstone", "fresh_install_live_sidecar_present"),
    ],
)
def test_fresh_install_gate_rejects_live_sidecar_or_tombstone(
    tmp_path, suffix, expected_code
):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    expected_config_sha256 = _runtime_config_sha256(
        consumer,
        dispatcher,
        "canary",
    )
    activation = release_gate_module._check_activation_epoch(
        control_db_path=dispatcher.control_db_path,
        mode="canary",
        expected_config_sha256=expected_config_sha256,
    )
    Path(f"{dispatcher.control_db_path}{suffix}").write_bytes(b"unexpected-sidecar")

    with pytest.raises(EvidenceError) as error:
        _check_fresh_install_fixture(
            consumer,
            dispatcher,
            settings,
            activation=activation,
        )

    assert error.value.code == expected_code


def test_fresh_install_original_receipt_survives_long_canary_via_epoch_binding(
    tmp_path,
):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    activation = release_gate_module._check_activation_epoch(
        control_db_path=dispatcher.control_db_path,
        mode="canary",
        expected_config_sha256=_runtime_config_sha256(
            consumer,
            dispatcher,
            "canary",
        ),
    )
    evidence_hashes: dict[str, str] = {}
    migration = release_gate_module._load_evidence(
        settings.evidence_dir,
        "store_migration_receipt.json",
        evidence_hashes,
    )
    materialization = release_gate_module._load_evidence(
        settings.evidence_dir,
        "fresh_install_materialization_receipt.json",
        evidence_hashes,
    )
    capacity_initialization = release_gate_module._load_evidence(
        settings.evidence_dir,
        "capacity_transition_initialization_receipt.json",
        evidence_hashes,
    )
    detail = release_gate_module._check_store_migration_receipt(
        migration,
        dispatcher=dispatcher,
        settings=settings,
        expected_host_commit=_git(settings.host_repo_root, "rev-parse", "HEAD"),
        now=NOW + timedelta(hours=2),
        max_age_seconds=settings.evidence_max_age_seconds,
        release_mode="canary",
        migration_raw_sha256=evidence_hashes["store_migration_receipt.json"],
        materialization_receipt=materialization,
        materialization_raw_sha256=evidence_hashes[
            "fresh_install_materialization_receipt.json"
        ],
        capacity_initialization_receipt=capacity_initialization,
        capacity_initialization_raw_sha256=evidence_hashes[
            "capacity_transition_initialization_receipt.json"
        ],
        activation=activation,
    )

    assert detail["fresh_install_materialization"][
        "expected_db_logical_identity_sha256"
    ]


def test_fresh_install_preproduction_first_use_requires_fresh_evidence(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "preproduction")

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("preproduction"),
        now=NOW + timedelta(hours=2),
    )

    assert "store_migration_receipt_stale" in report["blockers"]


def test_fresh_install_gate_rejects_live_database_inode_replacement(tmp_path):
    from scripts import pnc_rca_store_migration_drill as migration_module

    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    migration_module._checkpoint_restore(dispatcher.control_db_path)
    replacement = tmp_path / "replacement.sqlite3"
    shutil.copy2(dispatcher.control_db_path, replacement)
    os.replace(replacement, dispatcher.control_db_path)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert "fresh_install_live_continuity_invalid" in report["blockers"]


def test_fresh_install_gate_rejects_self_consistent_operator_db_identity(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    forged_identity = {"database": str(dispatcher.control_db_path), "release": "forged"}
    forged_json = json.dumps(
        forged_identity,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    with sqlite3.connect(dispatcher.control_db_path) as connection:
        connection.execute(
            "UPDATE rca_activation_epochs SET db_logical_identity_json=?, "
            "db_logical_identity_sha256=? WHERE is_current=1",
            (forged_json, _sha256_json(forged_identity)),
        )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert "activation_database_identity_binding_invalid" in report["blockers"]


def test_canary_existing_predecessor_requires_validator_and_is_rollback_ready(
    tmp_path,
):
    from scripts import pnc_rca_store_migration_drill as migration_module

    source = tmp_path / "control.sqlite3"
    migration_module._create_predecessor_fixture(
        source,
        ["control", "delivery"],
    )
    consumer, dispatcher, settings = _gate(tmp_path, "canary")

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    check = next(item for item in report["checks"] if item["name"] == "store_migration")
    assert check["ok"] is True
    assert check["detail"]["migration_state"] == "migration_required"
    assert check["detail"]["rollback_ready"] is True


def test_preauthorization_rejects_already_current_without_origin_lineage(tmp_path):
    from gateway.pnc_rca_control_store import RcaControlStore
    from gateway.pnc_rca_delivery_store import RcaDeliveryStore
    from scripts import pnc_rca_store_migration_drill as migration_module
    from scripts.pnc_rca_store_migration_drill import observe_regular_file

    source = tmp_path / "control.sqlite3"
    RcaControlStore(source)
    RcaDeliveryStore(source)
    migration_module._checkpoint_restore(source)

    consumer, dispatcher, settings = _gate(tmp_path, "preauthorization")
    migration_module._checkpoint_restore(source)
    before = observe_regular_file(source)
    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("preauthorization"),
        now=NOW,
    )

    assert report["ok"] is False
    assert "store_migration_rollback_not_ready" in report["blockers"]
    assert observe_regular_file(source) == before
    check = next(item for item in report["checks"] if item["name"] == "store_migration")
    assert check["ok"] is False


@pytest.mark.parametrize(
    ("mutation", "blocker"),
    [
        (
            lambda body: body["candidate"].update(commit="0" * 40),
            "store_migration_candidate_commit_mismatch",
        ),
        (
            lambda body: body["candidate"]["migration_sources"].update({
                "gateway/pnc_rca_control_store.py": "0" * 64,
            }),
            "store_migration_candidate_source_mismatch",
        ),
        (
            lambda body: body["writer_stop_evidence"]["services"][
                "local.pnc.rca-kafka-consumer"
            ].update(pid_state="pid_present"),
            "store_migration_writer_not_stopped",
        ),
        (
            lambda body: body["configured_databases"].update(
                control="/tmp/unrelated-control.sqlite3"
            ),
            "store_migration_configured_database_mismatch",
        ),
        (
            lambda body: body["database_drills"][0].update(
                migration_state="already_current"
            ),
            "store_migration_state_schema_mismatch",
        ),
        (
            lambda body: body["database_drills"][0]["rollback"].update(
                compatibility_probe="old_binary_executed"
            ),
            "store_migration_rollback_readiness_invalid",
        ),
    ],
)
def test_canary_store_migration_receipt_rejects_policy_and_provenance_drift(
    tmp_path, mutation, blocker
):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    path = settings.evidence_dir / "store_migration_receipt.json"
    body = json.loads(path.read_text(encoding="utf-8"))
    mutation(body)
    _write_json(path, body)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


@pytest.mark.parametrize(
    ("mutation", "blocker"),
    [
        ("tamper", "store_migration_artifact_identity_mismatch"),
        ("symlink", "store_migration_artifact_invalid"),
    ],
)
def test_canary_store_migration_rehashes_regular_backup_artifact(
    tmp_path, mutation, blocker
):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    receipt = json.loads(
        (settings.evidence_dir / "store_migration_receipt.json").read_text(
            encoding="utf-8"
        )
    )
    backup = Path(receipt["database_drills"][0]["backup"]["artifact"]["path"])
    if mutation == "tamper":
        with backup.open("ab") as handle:
            handle.write(b"tamper")
    else:
        real = backup.with_name("shared.backup.real.sqlite3")
        backup.rename(real)
        backup.symlink_to(real)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


def test_canary_store_migration_rechecks_restore_schema_from_sqlite(tmp_path):
    from scripts.pnc_rca_store_migration_drill import (
        inspect_sqlite_read_only,
        observe_regular_file,
    )

    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    receipt_path = settings.evidence_dir / "store_migration_receipt.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    restore = receipt["database_drills"][0]["restore"]
    restore_path = Path(restore["artifact"]["path"])
    with sqlite3.connect(restore_path) as connection:
        connection.execute(
            "UPDATE control_meta SET value='pnc_rca_control_store_v6' "
            "WHERE key='schema_version'"
        )
    restore["artifact"] = observe_regular_file(restore_path)
    restore["validation"] = inspect_sqlite_read_only(
        restore_path, ["control", "delivery"]
    )
    _write_json(receipt_path, receipt)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert "store_migration_target_schema_mismatch" in report["blockers"]


def test_preauthorization_store_migration_rejects_uncontained_backup(tmp_path):
    from scripts.pnc_rca_store_migration_drill import (
        inspect_sqlite_read_only,
        observe_regular_file,
    )

    _prepare_current_migration_store(tmp_path / "control.sqlite3")
    consumer, dispatcher, settings = _gate(tmp_path, "preauthorization")
    receipt_path = settings.evidence_dir / "store_migration_receipt.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    backup = receipt["database_drills"][0]["backup"]
    backup_path = Path(backup["artifact"]["path"])
    with sqlite3.connect(backup_path) as connection:
        connection.execute("CREATE TABLE forged_same_schema(value TEXT)")
        connection.execute("INSERT INTO forged_same_schema VALUES('not-source-data')")
    backup["artifact"] = observe_regular_file(backup_path)
    backup["validation"] = inspect_sqlite_read_only(
        backup_path, ["control", "delivery"]
    )
    receipt["database_drills"][0]["restore"]["source_backup_sha256"] = backup[
        "artifact"
    ]["sha256"]
    receipt["database_drills"][0]["rollback"]["source_backup_sha256"] = backup[
        "artifact"
    ]["sha256"]
    _write_json(receipt_path, receipt)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("preauthorization"),
        now=NOW,
    )

    assert report["ok"] is False
    assert "store_migration_current_snapshot_invalid" in report["blockers"]


def _prepare_current_migration_store(path: Path) -> None:
    from gateway.pnc_rca_control_store import RcaControlStore
    from gateway.pnc_rca_delivery_store import RcaDeliveryStore
    from scripts import pnc_rca_store_migration_drill as migration_module

    RcaControlStore(path)
    RcaDeliveryStore(path)
    lineage = {
        "fresh_install_db_instance_id": "12345678-1234-5678-9234-567812345678",
        "fresh_install_genesis_intent_sha256": "a" * 64,
        "fresh_install_origin_commit": "b" * 40,
    }
    connection = sqlite3.connect(path)
    try:
        for table in ("control_meta", "rca_delivery_meta"):
            connection.executemany(
                f"INSERT OR REPLACE INTO {table}(key, value) VALUES(?, ?)",
                sorted(lineage.items()),
            )
        connection.commit()
    finally:
        connection.close()
    migration_module._checkpoint_restore(path)


def test_preauthorization_accepts_additive_snapshot_but_requires_provenance(
    tmp_path,
):
    _prepare_current_migration_store(tmp_path / "control.sqlite3")
    consumer, dispatcher, settings = _gate(tmp_path, "preauthorization")
    with sqlite3.connect(dispatcher.control_db_path) as connection:
        connection.execute(
            "INSERT INTO control_meta(key, value) VALUES('post_drill_row', 'new')"
        )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("preauthorization"),
        now=NOW,
    )

    assert report["ok"] is False
    assert "store_migration_rollback_not_ready" in report["blockers"]
    assert "store_migration_current_snapshot_invalid" not in report["blockers"]


@pytest.mark.parametrize("mutation", ["delete", "change"])
def test_preauthorization_rejects_deleted_or_changed_old_row(tmp_path, mutation):
    _prepare_current_migration_store(tmp_path / "control.sqlite3")
    consumer, dispatcher, settings = _gate(tmp_path, "preauthorization")
    with sqlite3.connect(dispatcher.control_db_path) as connection:
        if mutation == "delete":
            connection.execute(
                "DELETE FROM rca_delivery_dispatcher_circuit "
                "WHERE circuit_name='feishu_thread_reply'"
            )
        else:
            connection.execute(
                "UPDATE rca_delivery_dispatcher_circuit SET reason_detail='changed' "
                "WHERE circuit_name='feishu_thread_reply'"
            )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("preauthorization"),
        now=NOW,
    )

    assert "store_migration_current_snapshot_invalid" in report["blockers"]


def test_canary_store_migration_binds_rollback_to_exact_backup_snapshot(tmp_path):
    from scripts.pnc_rca_store_migration_drill import (
        inspect_sqlite_read_only,
        observe_regular_file,
    )

    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    receipt_path = settings.evidence_dir / "store_migration_receipt.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    rollback = receipt["database_drills"][0]["rollback"]
    rollback_path = Path(rollback["artifact"]["path"])
    rollback_path.chmod(0o600)
    with sqlite3.connect(rollback_path) as connection:
        connection.execute("CREATE TABLE forged_rollback(value TEXT)")
    rollback["artifact"] = observe_regular_file(rollback_path)
    rollback["validation"] = inspect_sqlite_read_only(
        rollback_path, ["control", "delivery"]
    )
    _write_json(receipt_path, receipt)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert "store_migration_rollback_proof_invalid" in report["blockers"]


def test_canary_store_migration_rejects_structurally_valid_restore_data_loss(
    tmp_path,
):
    from scripts.pnc_rca_store_migration_drill import (
        inspect_sqlite_read_only,
        observe_regular_file,
    )

    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    receipt_path = settings.evidence_dir / "store_migration_receipt.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    restore = receipt["database_drills"][0]["restore"]
    restore_path = Path(restore["artifact"]["path"])
    with sqlite3.connect(restore_path) as connection:
        connection.execute("DELETE FROM rca_delivery_dispatcher_circuit")
    restore["artifact"] = observe_regular_file(restore_path)
    restore["validation"] = inspect_sqlite_read_only(
        restore_path, ["control", "delivery"]
    )
    _write_json(receipt_path, receipt)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert "store_migration_restore_data_mismatch" in report["blockers"]


def test_runtime_dependency_probe_requires_exact_versions_and_importable_apis():
    versions = dict(release_gate_module.EXPECTED_DEPENDENCY_VERSIONS)
    modules = {
        "kafka": SimpleNamespace(
            KafkaConsumer=lambda: None,
            AsyncConsumerRebalanceListener=lambda: None,
        ),
        "kafka.coordinator.consumer": SimpleNamespace(
            ConsumerCoordinator=type(
                "ConsumerCoordinator",
                (),
                {"fetch_committed_offsets_async": (lambda: None)},
            )
        ),
        "snappy": SimpleNamespace(compress=lambda value: value),
    }

    async def fetch_committed_offsets_async():
        return {}

    modules[
        "kafka.coordinator.consumer"
    ].ConsumerCoordinator.fetch_committed_offsets_async = fetch_committed_offsets_async

    result = check_runtime_dependencies(
        version_reader=versions.__getitem__,
        importer=modules.__getitem__,
    )

    assert result["dependency_versions"] == versions
    assert result["module_imports"] == {
        "kafka": "ok",
        "kafka.coordinator.consumer": "ok",
        "snappy": "ok",
    }


@pytest.mark.parametrize(
    ("versions", "modules", "blocker"),
    [
        (
            {
                **release_gate_module.EXPECTED_DEPENDENCY_VERSIONS,
                "kafka-python": "3.0.6",
            },
            {},
            "runtime_dependency_version_mismatch",
        ),
        (
            dict(release_gate_module.EXPECTED_DEPENDENCY_VERSIONS),
            {
                "kafka": SimpleNamespace(
                    KafkaConsumer=lambda: None,
                    AsyncConsumerRebalanceListener=lambda: None,
                )
            },
            "runtime_dependency_import_failed",
        ),
    ],
)
def test_runtime_dependency_probe_fails_closed(versions, modules, blocker):
    with pytest.raises(EvidenceError) as error:
        check_runtime_dependencies(
            version_reader=versions.__getitem__,
            importer=modules.__getitem__,
        )
    assert error.value.code == blocker


def _write_candidate_plists(tmp_path, *, kafka_environment=None):
    interpreter = tmp_path / ".venv" / "bin" / "python"
    interpreter.parent.mkdir(parents=True)
    interpreter.write_text("", encoding="utf-8")
    working = tmp_path
    scripts = working / "scripts"
    for relative in release_gate_module.DELIVERY_RUNTIME_CRITICAL_FILES:
        runtime_file = working / relative
        runtime_file.parent.mkdir(parents=True, exist_ok=True)
        runtime_file.write_text(f"# {relative}\n", encoding="utf-8")
    scripts.mkdir(exist_ok=True)
    default_environment = {
        "HOME": str(Path.home()),
        "HERMES_HOME": str(tmp_path / ".hermes"),
        "PATH": "/usr/bin:/bin",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1",
        "PYTHONUNBUFFERED": "1",
    }
    for filename, (
        label,
        script_name,
    ) in release_gate_module.CANDIDATE_SERVICES.items():
        script = scripts / script_name
        script.write_text("", encoding="utf-8")
        environment = dict(default_environment)
        if label == "local.pnc.rca-kafka-consumer" and kafka_environment is not None:
            environment = dict(kafka_environment)
        plist = {
            "Label": label,
            "ProgramArguments": [str(interpreter), str(script)],
            "WorkingDirectory": str(working),
            "EnvironmentVariables": environment,
        }
        (tmp_path / filename).write_bytes(release_gate_module.plistlib.dumps(plist))
    return interpreter, working, default_environment


def _candidate_runtime_probe_payload(
    interpreter: Path,
    *,
    process_executable: Path | None = None,
) -> dict:
    versions = dict(release_gate_module.EXPECTED_DEPENDENCY_VERSIONS)
    dependencies = {}
    for distribution, module_name in sorted(
        release_gate_module.RCA_LOADED_DEPENDENCIES.items()
    ):
        origin = interpreter.parent.parent / "lib" / f"{module_name}.py"
        origin.parent.mkdir(parents=True, exist_ok=True)
        if not origin.exists():
            origin.write_text(f"# {distribution}\n", encoding="utf-8")
        dependencies[distribution] = {
            "module": module_name,
            "origin": str(origin.resolve(strict=True)),
            "sha256": hashlib.sha256(origin.read_bytes()).hexdigest(),
            "version": versions[distribution],
        }
    executable = interpreter.resolve(strict=True)
    process_executable = (process_executable or interpreter).resolve(strict=True)
    loaded_runtime = {
        "sys_executable": str(executable),
        "sys_executable_sha256": hashlib.sha256(executable.read_bytes()).hexdigest(),
        "process_executable": str(process_executable),
        "process_executable_sha256": hashlib.sha256(
            process_executable.read_bytes()
        ).hexdigest(),
        "dependencies": dependencies,
    }
    return {
        "ok": True,
        "runtime_executable": str(process_executable),
        "loaded_runtime": loaded_runtime,
        "loaded_runtime_sha256": _sha256_json(loaded_runtime),
        "versions": versions,
        "apis": {
            "KafkaConsumer": True,
            "AsyncConsumerRebalanceListener": True,
            "ConsumerCoordinator": True,
            "fetch_committed_offsets_async": True,
            "snappy_compress": True,
            "tinycss2_parse_stylesheet": True,
        },
        "feishu_outbound": {
            "schema_version": (
                release_gate_module.FEISHU_OUTBOUND_RUNTIME_SCHEMA_VERSION
            ),
            "dependency_install_attempted": False,
            "client_constructed": True,
            "apis": {
                name: True for name in release_gate_module.EXPECTED_FEISHU_OUTBOUND_APIS
            },
        },
    }


def _future_runtime_plist_body(filename: str, root: Path) -> dict:
    label = release_gate_module.runtime_stage.CANDIDATE_PLISTS[filename][0]
    arguments = [
        item.replace("{runtime}", str(root))
        for item in release_gate_module.runtime_stage.CANDIDATE_PLIST_ARGUMENTS[
            filename
        ]
    ]
    return {
        "Label": label,
        "ProgramArguments": arguments,
        "WorkingDirectory": str(root),
        "EnvironmentVariables": {
            "HOME": str(Path.home()),
            "HERMES_HOME": str(
                release_gate_module.CANONICAL_FUTURE_RUNTIME_ROOT.parent.parent
            ),
            "PATH": f"{root / '.venv' / 'bin'}:/usr/bin:/bin:/usr/sbin:/sbin",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONNOUSERSITE": "1",
            "PYTHONUNBUFFERED": "1",
            "VIRTUAL_ENV": str(root / ".venv"),
        },
        "RunAtLoad": True,
    }


def _future_runtime_fixture(tmp_path: Path, *, external_process: bool = False):
    source = tmp_path / "future-source"
    source.mkdir()
    stage = tmp_path / "future-stage"
    stage.mkdir(mode=0o700)
    for relative in release_gate_module.FUTURE_RUNTIME_RELATIVE_FILES:
        for root in (source, stage):
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f"# future runtime: {relative}\n", encoding="utf-8")
    canonical_root = release_gate_module.CANONICAL_FUTURE_RUNTIME_ROOT
    for filename in release_gate_module.FUTURE_RUNTIME_PLIST_FILENAMES:
        (source / filename).write_bytes(
            release_gate_module.plistlib.dumps(
                _future_runtime_plist_body(filename, canonical_root)
            )
        )
        (stage / filename).write_bytes(
            release_gate_module.plistlib.dumps(
                _future_runtime_plist_body(filename, stage)
            )
        )
    interpreter = stage / ".venv" / "bin" / "python"
    interpreter.parent.mkdir(parents=True)
    interpreter.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    interpreter.chmod(0o755)
    process_executable = interpreter
    if external_process:
        process_executable = tmp_path / "homebrew-cellar-python"
        process_executable.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        process_executable.chmod(0o755)
    (stage / ".venv").chmod(0o755)
    interpreter.parent.chmod(0o755)
    stage.chmod(0o700)
    _git(source, "init", "-q")
    _git(source, "add", ".")
    _git(
        source,
        "-c",
        "user.name=RCA Future Runtime Test",
        "-c",
        "user.email=rca-future-runtime@example.invalid",
        "commit",
        "-q",
        "-m",
        "future runtime source",
    )

    def runner(command, **_kwargs):
        if command[1:4] == ["-I", "-B", "-c"]:
            payload = _candidate_runtime_probe_payload(
                interpreter,
                process_executable=process_executable,
            )
        elif command[1:2] == ["-c"]:
            loaded = _candidate_runtime_probe_payload(
                interpreter,
                process_executable=process_executable,
            )["loaded_runtime"]
            payload = {
                "sys_executable": str(interpreter),
                "process_executable": str(process_executable),
                "module_origins": {
                    "hermes_cli.main": str(stage / "hermes_cli" / "main.py"),
                    "gateway.run": str(stage / "gateway" / "run.py"),
                    "gateway.pnc_rca_policy_config": str(
                        stage / "gateway" / "pnc_rca_policy_config.py"
                    ),
                    "gateway.pnc_rca_runtime_identity": str(
                        stage / "gateway" / "pnc_rca_runtime_identity.py"
                    ),
                    "psutil": loaded["dependencies"]["psutil"]["origin"],
                    "dotenv": loaded["dependencies"]["python-dotenv"]["origin"],
                },
                "dependency_versions": dict(
                    release_gate_module.EXPECTED_GATEWAY_RUNTIME_DEPENDENCY_VERSIONS
                ),
            }
        else:
            payload = {"ok": True, "config": {}}
            if Path(command[1]).name == "pnc_rca_delivery_collector.py":
                payload["dependencies"] = {
                    "remote_css_parser": dict(
                        release_gate_module.EXPECTED_REMOTE_CSS_RUNTIME_DEPENDENCY
                    )
                }
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps(payload),
            stderr="",
        )

    return SimpleNamespace(
        source=source,
        stage=stage,
        interpreter=interpreter,
        process_executable=process_executable,
        runner=runner,
        candidate_plists=tuple(
            stage / filename
            for filename in release_gate_module.FUTURE_RUNTIME_PLIST_FILENAMES
        ),
        runtime_verifier=lambda root: check_candidate_runtime_dependencies(
            root,
            runner=runner,
        ),
    )


def test_future_runtime_projection_binds_real_stage_to_canonical_root(tmp_path):
    fixture = _future_runtime_fixture(tmp_path)

    result = release_gate_module.project_future_candidate_runtime(
        fixture.source,
        fixture.stage,
        candidate_plists=fixture.candidate_plists,
        runner=fixture.runner,
        runtime_verifier=fixture.runtime_verifier,
    )

    canonical = release_gate_module.CANONICAL_FUTURE_RUNTIME_ROOT
    projection = result["future_runtime_projection"]
    stage_identity = result["runtime_stage_identity"]
    assert projection["ok"] is True
    assert projection["canonical_live_root"] == str(canonical)
    assert projection["staging_root"] == str(fixture.stage)
    assert result["python_executable"] == str(canonical / ".venv/bin/python")
    assert result["loaded_runtime"]["sys_executable"] == str(
        canonical / ".venv/bin/python"
    )
    assert set(result["render_manifest"]["candidate_plists"]) == set(
        release_gate_module.FUTURE_RUNTIME_PLIST_FILENAMES
    )
    assert len(release_gate_module.FUTURE_RUNTIME_PLIST_FILENAMES) == 7
    assert stage_identity["root"]["path"] == str(fixture.stage)
    assert stage_identity["venv"]["path"] == str(fixture.stage / ".venv")
    assert stage_identity["interpreter"]["path"] == str(fixture.interpreter)
    assert len(result["render_manifest_sha256"]) == 64
    for process in result["service_processes"].values():
        assert process["working_directory"] == str(canonical)
        assert process["environment"]["VIRTUAL_ENV"] == str(canonical / ".venv")
        assert process["environment"]["PYTHONDONTWRITEBYTECODE"] == "1"
        assert process["environment"]["PYTHONNOUSERSITE"] == "1"
        assert str(fixture.stage) not in json.dumps(process)


def test_future_runtime_projection_binds_external_process_executable(tmp_path):
    fixture = _future_runtime_fixture(tmp_path, external_process=True)

    result = release_gate_module.project_future_candidate_runtime(
        fixture.source,
        fixture.stage,
        candidate_plists=fixture.candidate_plists,
        runner=fixture.runner,
        runtime_verifier=fixture.runtime_verifier,
    )

    external = str(fixture.process_executable)
    external_sha256 = hashlib.sha256(
        fixture.process_executable.read_bytes()
    ).hexdigest()
    assert result["loaded_runtime"]["sys_executable"] == str(
        release_gate_module.CANONICAL_FUTURE_RUNTIME_ROOT / ".venv/bin/python"
    )
    assert result["loaded_runtime"]["process_executable"] == external
    assert (
        result["loaded_runtime"]["process_executable_sha256"]
        == external_sha256
    )
    for process in result["service_processes"].values():
        assert process["runtime_executable"] == external
        assert process["loaded_runtime"]["process_executable"] == external
        assert (
            process["loaded_runtime"]["process_executable_sha256"]
            == external_sha256
        )
    gateway = result["render_manifest"]["gateway_runtime"]
    assert gateway["process_executable"] == external
    assert gateway["process_executable_sha256"] == external_sha256


def test_future_runtime_projection_rejects_external_process_hash_drift(tmp_path):
    fixture = _future_runtime_fixture(tmp_path, external_process=True)

    def forged_runtime_verifier(root):
        detail = fixture.runtime_verifier(root)
        detail["loaded_runtime"]["process_executable_sha256"] = "0" * 64
        return detail

    with pytest.raises(EvidenceError) as error:
        release_gate_module.project_future_candidate_runtime(
            fixture.source,
            fixture.stage,
            candidate_plists=fixture.candidate_plists,
            runner=fixture.runner,
            runtime_verifier=forged_runtime_verifier,
        )

    assert error.value.code == "future_runtime_process_executable_hash_mismatch"


def test_future_runtime_release_binding_revalidates_external_process(tmp_path):
    host_repo, host_commit = _create_host_build_repo(tmp_path)
    binding = _future_runtime_binding_fixture(
        stage_root=tmp_path / "future-runtime-stage",
        host_repo=host_repo,
        host_commit=host_commit,
    )
    external = tmp_path / "homebrew-cellar-python"
    external.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    external.chmod(0o755)
    external_sha256 = hashlib.sha256(external.read_bytes()).hexdigest()
    gateway = binding["render_manifest"]["gateway_runtime"]
    gateway["process_executable"] = str(external)
    gateway["process_executable_sha256"] = external_sha256
    render_sha256 = _sha256_json(binding["render_manifest"])
    binding["render_manifest_sha256"] = render_sha256
    binding["future_runtime_projection"][
        "render_manifest_sha256"
    ] = render_sha256
    binding["future_runtime_projection_sha256"] = _sha256_json(
        binding["future_runtime_projection"]
    )
    host_component = {
        "repo_root": str(host_repo.resolve()),
        "commit": host_commit,
    }

    normalized = release_gate_module._normalize_future_runtime_release_binding(
        binding,
        host_component=host_component,
        expected_launchd_config_sha256=LAUNCHD_CONFIG_SHA256,
        field="test.future_runtime",
    )

    assert (
        normalized["render_manifest"]["gateway_runtime"][
            "process_executable"
        ]
        == str(external)
    )
    external.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
    with pytest.raises(EvidenceError) as error:
        release_gate_module._normalize_future_runtime_release_binding(
            binding,
            host_component=host_component,
            expected_launchd_config_sha256=LAUNCHD_CONFIG_SHA256,
            field="test.future_runtime",
        )
    assert error.value.code == "future_runtime_gateway_binding_invalid"


@pytest.mark.parametrize("stage_dynamic_module", [True, False])
def test_future_runtime_projection_binds_complete_clean_git_tree(
    tmp_path,
    stage_dynamic_module,
):
    fixture = _future_runtime_fixture(tmp_path)
    relative = "gateway/dynamic_runtime_module.py"
    source_dynamic = fixture.source / relative
    source_dynamic.write_text("DYNAMIC_RUNTIME = True\n", encoding="utf-8")
    _git(fixture.source, "add", relative)
    _git(
        fixture.source,
        "-c",
        "user.name=RCA Future Runtime Test",
        "-c",
        "user.email=rca-future-runtime@example.invalid",
        "commit",
        "-q",
        "-m",
        "add dynamic runtime module",
    )
    if stage_dynamic_module:
        staged_dynamic = fixture.stage / relative
        staged_dynamic.parent.mkdir(parents=True, exist_ok=True)
        staged_dynamic.write_bytes(source_dynamic.read_bytes())

    if not stage_dynamic_module:
        with pytest.raises(EvidenceError) as error:
            release_gate_module.project_future_candidate_runtime(
                fixture.source,
                fixture.stage,
                candidate_plists=fixture.candidate_plists,
                runner=fixture.runner,
                runtime_verifier=fixture.runtime_verifier,
            )
        assert error.value.code == "future_runtime_stage_file_invalid"
        return

    result = release_gate_module.project_future_candidate_runtime(
        fixture.source,
        fixture.stage,
        candidate_plists=fixture.candidate_plists,
        runner=fixture.runner,
        runtime_verifier=fixture.runtime_verifier,
    )

    assert result["render_manifest"]["runtime_file_sha256"][relative] == hashlib.sha256(
        source_dynamic.read_bytes()
    ).hexdigest()


def test_future_runtime_projection_accepts_empty_tracked_marker_file(tmp_path):
    fixture = _future_runtime_fixture(tmp_path)
    relative = "docker/s6-rc.d/example/dependencies.d/base"
    for root in (fixture.source, fixture.stage):
        marker = root / relative
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_bytes(b"")
    _git(fixture.source, "add", relative)
    _git(
        fixture.source,
        "-c",
        "user.name=RCA Future Runtime Test",
        "-c",
        "user.email=rca-future-runtime@example.invalid",
        "commit",
        "-q",
        "-m",
        "add empty runtime marker",
    )

    result = release_gate_module.project_future_candidate_runtime(
        fixture.source,
        fixture.stage,
        candidate_plists=fixture.candidate_plists,
        runner=fixture.runner,
        runtime_verifier=fixture.runtime_verifier,
    )

    assert result["render_manifest"]["runtime_file_sha256"][relative] == hashlib.sha256(
        b""
    ).hexdigest()


@pytest.mark.parametrize(
    "drift",
    [
        "source_dirty",
        "stage_content",
        "stage_symlink",
        "stage_hardlink",
        "stage_mode",
        "venv_symlink",
        "interpreter_symlink",
    ],
)
def test_future_runtime_projection_rejects_source_and_stage_identity_drift(
    tmp_path,
    drift,
):
    fixture = _future_runtime_fixture(tmp_path)
    relative = release_gate_module.FUTURE_RUNTIME_RELATIVE_FILES[0]
    source_file = fixture.source / relative
    stage_file = fixture.stage / relative
    if drift == "source_dirty":
        source_file.write_text("dirty source\n", encoding="utf-8")
    elif drift == "stage_content":
        stage_file.write_text("different stage\n", encoding="utf-8")
    elif drift == "stage_symlink":
        stage_file.unlink()
        stage_file.symlink_to(fixture.stage / release_gate_module.FUTURE_RUNTIME_RELATIVE_FILES[1])
    elif drift == "stage_hardlink":
        stage_file.unlink()
        os.link(source_file, stage_file)
    elif drift == "stage_mode":
        fixture.stage.chmod(0o755)
    elif drift == "venv_symlink":
        venv = fixture.stage / ".venv"
        moved = tmp_path / "moved-venv"
        venv.rename(moved)
        venv.symlink_to(moved)
    else:
        fixture.interpreter.unlink()
        fixture.interpreter.symlink_to("/bin/sh")

    with pytest.raises(EvidenceError):
        release_gate_module.project_future_candidate_runtime(
            fixture.source,
            fixture.stage,
            candidate_plists=fixture.candidate_plists,
            runner=fixture.runner,
            runtime_verifier=fixture.runtime_verifier,
        )


@pytest.mark.parametrize(
    "drift",
    [
        "prefix_string",
        "path_escape",
        "source_worktree_root",
        "virtual_env",
        "python_no_user_site",
        "bytecode_writes",
    ],
)
def test_future_runtime_projection_rejects_plist_projection_drift(
    tmp_path,
    drift,
):
    fixture = _future_runtime_fixture(tmp_path)
    filename = "local.pnc.rca-kafka-consumer.candidate.plist"
    target = fixture.stage / filename
    body = release_gate_module.plistlib.loads(target.read_bytes())
    if drift == "prefix_string":
        body["ProgramArguments"][1] = (
            f"{fixture.stage}-other/scripts/pnc_rca_kafka_consumer.py"
        )
    elif drift == "path_escape":
        body["ProgramArguments"][1] = str(
            fixture.stage / ".." / "escape" / "pnc_rca_kafka_consumer.py"
        )
    elif drift == "virtual_env":
        body["EnvironmentVariables"]["VIRTUAL_ENV"] = "/tmp/other-venv"
    elif drift == "python_no_user_site":
        body["EnvironmentVariables"]["PYTHONNOUSERSITE"] = "0"
    elif drift == "bytecode_writes":
        body["EnvironmentVariables"].pop("PYTHONDONTWRITEBYTECODE")
    else:
        source = fixture.source / filename
        source_body = release_gate_module.plistlib.loads(source.read_bytes())
        source_body["ProgramArguments"][0] = str(
            fixture.source / ".venv/bin/python"
        )
        source.write_bytes(release_gate_module.plistlib.dumps(source_body))
        _git(fixture.source, "add", filename)
        _git(
            fixture.source,
            "-c",
            "user.name=RCA Future Runtime Test",
            "-c",
            "user.email=rca-future-runtime@example.invalid",
            "commit",
            "-q",
            "-m",
            "forged worktree projection",
        )
    if drift != "source_worktree_root":
        target.write_bytes(release_gate_module.plistlib.dumps(body))

    with pytest.raises(EvidenceError):
        release_gate_module.project_future_candidate_runtime(
            fixture.source,
            fixture.stage,
            candidate_plists=fixture.candidate_plists,
            runner=fixture.runner,
            runtime_verifier=fixture.runtime_verifier,
        )


def test_future_runtime_projection_rejects_toctou_after_probe(tmp_path):
    fixture = _future_runtime_fixture(tmp_path)
    target = fixture.stage / release_gate_module.FUTURE_RUNTIME_RELATIVE_FILES[0]

    def mutating_verifier(root):
        result = check_candidate_runtime_dependencies(
            root,
            runner=fixture.runner,
        )
        target.write_text("changed after probe\n", encoding="utf-8")
        return result

    with pytest.raises(EvidenceError) as error:
        release_gate_module.project_future_candidate_runtime(
            fixture.source,
            fixture.stage,
            candidate_plists=fixture.candidate_plists,
            runner=fixture.runner,
            runtime_verifier=mutating_verifier,
        )

    assert error.value.code == "future_runtime_changed_during_projection"


def _feishu_ingress_gate_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    hold = release_gate_module.feishu_ingress_hold
    guard = release_gate_module.cutover_guard
    current = datetime.now(timezone.utc)
    machine = {"source": "gate_test_machine", "sha256": "1" * 64}
    monkeypatch.setattr(hold, "_machine_identity", lambda: machine)
    chat_id = "oc_aaaaaaaaaaaaaaaa"
    sidecar_path = tmp_path / "feishu_api_poll_state_v1.json"
    sidecar = {
        "schema_version": hold.SIDECAR_IDENTITY_SCHEMA_VERSION,
        "state": "absent",
        "path": str(sidecar_path),
        "sha256": hold.EMPTY_SHA256,
        "size_bytes": 0,
        "revision": 0,
        "semantic_sha256": "2" * 64,
    }
    hold_start_ms = 2_000_000_000_000
    snapshot = {
        "schema_version": hold.CHAT_SNAPSHOT_SCHEMA_VERSION,
        "chat_id": chat_id,
        "floor_ms": hold_start_ms,
        "complete": True,
        "started_at_ms": hold_start_ms,
        "completed_at_ms": hold_start_ms + 1,
        "pages": [
            {
                "page_index": 0,
                "request_cursor_sha256": hold.EMPTY_SHA256,
                "response_cursor_sha256": hold.EMPTY_SHA256,
                "item_count": 0,
                "accepted_count": 0,
                "has_more": False,
                "stopped_at_floor": False,
            }
        ],
        "items": [],
    }
    host = {
        "schema_version": hold.ADAPTER_IDENTITY_SCHEMA_VERSION,
        "repo_root": str(tmp_path / "host-candidate"),
        "host_commit": "3" * 40,
        "tree_clean": True,
        "status_sha256": hold.EMPTY_SHA256,
        "adapter_relative_path": hold.ADAPTER_RELATIVE_PATH,
        "adapter_sha256": "4" * 64,
        "adapter_sidecar_schema": hold._API_POLL_SIDECAR_SCHEMA,
    }
    plan = {
        "schema_version": hold.PLAN_SCHEMA_VERSION,
        "hold_id": "hold-gate-20260713",
        "created_at": (current - timedelta(minutes=2)).isoformat(),
        "production_effects_executed": False,
        "phase": "plan",
        "chat_ids": [chat_id],
        "chat_set_sha256": hold._sha256_json([chat_id]),
        "app_scope": "5" * 32,
        "run_identity_sha256": "6" * 64,
        "host_adapter_identity": host,
        "live_sidecar_identity": sidecar,
        "window": {
            "hold_start_ms": hold_start_ms,
            "floor_by_chat": {chat_id: hold_start_ms},
            "snapshot_completed_at_ms": hold_start_ms + 1,
        },
        "api_snapshot": {"chats": {chat_id: snapshot}},
        "apply_contract": {
            "approval_schema_version": hold.APPROVAL_SCHEMA_VERSION,
            "cutover_binding_schema_version": hold.CUTOVER_BINDING_SCHEMA_VERSION,
            "gate_validator_required": (
                "validate_feishu_ingress_hold_cutover_binding"
            ),
            "watermark_policy": (
                "preserve_or_increase_never_advance_for_pending"
            ),
            "live_install_performed_by_this_tool": False,
        },
        "future_install": {
            "performed": False,
            "requires_separate_cutover": True,
            "canonical_gateway_root": str(hold.DEFAULT_CANONICAL_GATEWAY_ROOT),
            "canonical_sidecar_path": str(sidecar_path),
            "procedure": "install only during the bound cutover",
        },
        "side_effect_contract": {
            "feishu_message_writes": False,
            "live_sidecar_writes": False,
            "gateway_process_changes": False,
            "launchctl_invoked": False,
            "auth_token_exchange": True,
            "message_api": "GET_only",
            "output_scope": "unique_owner_only_run_root",
        },
    }
    artifact_sha256 = release_gate_module._feishu_ingress_artifact_sha256
    plan_sha256 = artifact_sha256(plan)
    approval = {
        "schema_version": hold.APPROVAL_SCHEMA_VERSION,
        "hold_id": plan["hold_id"],
        "decision": "authorize_feishu_ingress_hold_staging",
        "created_at": (current - timedelta(minutes=1)).isoformat(),
        "expires_at": (current + timedelta(minutes=30)).isoformat(),
        "nonce": "gate-test-approval-nonce-0001",
        "plan_sha256": plan_sha256,
        "chat_set_sha256": plan["chat_set_sha256"],
        "host_commit": host["host_commit"],
        "adapter_sha256": host["adapter_sha256"],
        "adapter_sidecar_schema": host["adapter_sidecar_schema"],
        "live_sidecar_identity_sha256": hold._sha256_json(sidecar),
        "app_scope": plan["app_scope"],
        "action_set": list(hold.APPLY_ACTION_SET),
        "action_set_sha256": hold._sha256_json(list(hold.APPLY_ACTION_SET)),
        "identity": hold._approval_identity(machine),
    }
    approval_sha256 = artifact_sha256(approval)
    live_runtime = {"schema_version": "gate_fixture_runtime_v1", "sha256": "7" * 64}
    old_runtime = {
        "schema_version": guard.GATEWAY_RUNNING_OBSERVATION_SCHEMA_VERSION,
        "canonical_root": str(guard.CANONICAL_LIVE_ROOT),
        "launchd": {
            "label": guard.GATEWAY_LABEL,
            "loaded": True,
            "pid": 4567,
            "state": "running",
        },
        "process": {
            "pid": 4567,
            "process_create_time": current.timestamp() - 600,
            "executable": str(guard.CANONICAL_LIVE_ROOT / ".venv/bin/python"),
            "cwd": str(guard.CANONICAL_LIVE_ROOT),
            "cmdline_sha256": "8" * 64,
            "loaded_runtime_closure_sha256": guard._sha256_json(live_runtime),
        },
        "live_runtime_identity": live_runtime,
    }
    stop_observation = {
        "schema_version": guard.GATEWAY_WRITER_STOP_OBSERVATION_SCHEMA_VERSION,
        "canonical_root": str(guard.CANONICAL_LIVE_ROOT),
        "launchd": {
            "label": guard.GATEWAY_LABEL,
            "loaded": True,
            "pid": None,
            "state": "not_running",
        },
        "process_census": {
            "probe": "psutil_gateway_canonical_runtime_census_v1",
            "canonical_root": str(guard.CANONICAL_LIVE_ROOT),
            "matching_processes": [],
        },
        "live_runtime_identity": live_runtime,
        "live_sidecar_identity": sidecar,
    }
    lease_fingerprint = "9" * 64
    prepare_sha256 = "a" * 64
    old_runtime_sha256 = guard._sha256_json(old_runtime)
    precutover_services = _cutover_precutover_service_state(4567)
    writer_stop = {
        "schema_version": guard.WRITER_STOP_RECEIPT_SCHEMA_VERSION,
        "release_id": "release-gate-20260713",
        "hold_id": plan["hold_id"],
        "plan_sha256": plan_sha256,
        "observed_at": (current - timedelta(seconds=5)).isoformat(),
        "production_effects_executed": False,
        "lease_fingerprint": lease_fingerprint,
        "release_prepare_manifest_sha256": prepare_sha256,
        "approval_receipt_sha256": approval_sha256,
        "old_gateway_process": old_runtime["process"],
        "old_gateway_runtime_identity": old_runtime,
        "old_gateway_runtime_identity_sha256": old_runtime_sha256,
        "precutover_service_state": precutover_services,
        "precutover_service_state_sha256": guard._sha256_json(
            precutover_services
        ),
        "writer_stop_observation": stop_observation,
        "writer_stop_observation_sha256": guard._sha256_json(stop_observation),
        "live_sidecar_identity": sidecar,
        "live_sidecar_identity_sha256": hold._sha256_json(sidecar),
    }
    writer_stop_path = tmp_path / "writer-stop-receipt.json"
    writer_stop_path.write_bytes(guard._canonical_json(writer_stop))
    writer_stop_path.chmod(0o600)
    writer_stop_sha256 = hashlib.sha256(writer_stop_path.read_bytes()).hexdigest()
    cutover = {
        "schema_version": hold.CUTOVER_BINDING_SCHEMA_VERSION,
        "hold_id": plan["hold_id"],
        "release_id": writer_stop["release_id"],
        "plan_sha256": plan_sha256,
        "canonical_gateway_root": str(hold.DEFAULT_CANONICAL_GATEWAY_ROOT),
        "canonical_sidecar_path": str(sidecar_path),
        "host_commit": host["host_commit"],
        "adapter_sha256": host["adapter_sha256"],
        "chat_set_sha256": plan["chat_set_sha256"],
        "live_sidecar_identity_sha256": hold._sha256_json(sidecar),
        "gateway_writer_state": "stopped",
        "writer_stop_receipt_path": str(writer_stop_path),
        "writer_stop_receipt_sha256": writer_stop_sha256,
        "cutover_lease_fingerprint": lease_fingerprint,
        "release_prepare_manifest_sha256": prepare_sha256,
        "release_approval_receipt_sha256": approval_sha256,
        "old_gateway_runtime_identity_sha256": old_runtime_sha256,
        "window_started_at": (current - timedelta(minutes=1)).isoformat(),
        "window_expires_at": (current + timedelta(minutes=30)).isoformat(),
    }
    return SimpleNamespace(
        plan=plan,
        plan_sha256=plan_sha256,
        approval=approval,
        approval_sha256=approval_sha256,
        cutover=cutover,
        cutover_sha256=artifact_sha256(cutover),
        writer_stop=writer_stop,
        writer_stop_path=writer_stop_path,
        writer_stop_sha256=writer_stop_sha256,
        artifact_sha256=artifact_sha256,
        hold=hold,
        guard=guard,
    )


def _validate_feishu_ingress_gate_fixture(fixture):
    return release_gate_module.validate_feishu_ingress_hold_cutover_binding(
        plan=fixture.plan,
        plan_sha256=fixture.plan_sha256,
        approval_receipt=fixture.approval,
        approval_receipt_sha256=fixture.approval_sha256,
        cutover_binding=fixture.cutover,
        cutover_binding_sha256=fixture.cutover_sha256,
        writer_stop_receipt=fixture.writer_stop,
        writer_stop_receipt_sha256=fixture.writer_stop_sha256,
    )


def test_feishu_ingress_hold_cutover_gate_binds_v2_contract(
    tmp_path,
    monkeypatch,
):
    fixture = _feishu_ingress_gate_fixture(tmp_path, monkeypatch)

    result = _validate_feishu_ingress_gate_fixture(fixture)

    assert result == {
        "schema_version": fixture.hold.GATE_VALIDATION_SCHEMA_VERSION,
        "ok": True,
        "plan_sha256": fixture.plan_sha256,
        "approval_receipt_sha256": fixture.approval_sha256,
        "cutover_binding_sha256": fixture.cutover_sha256,
        "writer_stop_receipt_sha256": fixture.writer_stop_sha256,
        "cutover_lease_fingerprint": fixture.writer_stop["lease_fingerprint"],
        "old_gateway_runtime_identity_sha256": fixture.writer_stop[
            "old_gateway_runtime_identity_sha256"
        ],
        "gateway_writer_state": "stopped",
    }


@pytest.mark.parametrize(
    "tamper",
    ["approval_identity", "cutover_window", "lease", "sidecar", "writer_hash"],
)
def test_feishu_ingress_hold_cutover_gate_rejects_rebound_evidence(
    tmp_path,
    monkeypatch,
    tamper,
):
    fixture = _feishu_ingress_gate_fixture(tmp_path, monkeypatch)
    if tamper == "approval_identity":
        fixture.approval["identity"]["uid"] += 1
        fixture.approval_sha256 = fixture.artifact_sha256(fixture.approval)
    elif tamper == "cutover_window":
        fixture.cutover["window_expires_at"] = (
            datetime.now(timezone.utc)
            + timedelta(seconds=fixture.hold.MAX_CUTOVER_WINDOW_SECONDS + 60)
        ).isoformat()
        fixture.cutover_sha256 = fixture.artifact_sha256(fixture.cutover)
    elif tamper == "lease":
        fixture.cutover["cutover_lease_fingerprint"] = "f" * 64
        fixture.cutover_sha256 = fixture.artifact_sha256(fixture.cutover)
    elif tamper == "sidecar":
        fixture.writer_stop["live_sidecar_identity"]["revision"] = 1
        fixture.writer_stop["live_sidecar_identity_sha256"] = fixture.hold._sha256_json(
            fixture.writer_stop["live_sidecar_identity"]
        )
        fixture.writer_stop_path.write_bytes(
            fixture.guard._canonical_json(fixture.writer_stop)
        )
        fixture.writer_stop_sha256 = hashlib.sha256(
            fixture.writer_stop_path.read_bytes()
        ).hexdigest()
        fixture.cutover["writer_stop_receipt_sha256"] = fixture.writer_stop_sha256
        fixture.cutover_sha256 = fixture.artifact_sha256(fixture.cutover)
    else:
        fixture.writer_stop_sha256 = "f" * 64

    with pytest.raises(EvidenceError):
        _validate_feishu_ingress_gate_fixture(fixture)


def _capacity_admission_fixture(mode: str, *, release_bom_sha256: str = "e" * 64):
    value = {
        "resource_class": "rca_prod",
        "capacity_mode": mode,
        "task_meta_sha256": "a" * 64,
        "admission_receipt_sha256": "b" * 64,
        "admission_schema_version": (
            release_gate_module.prod_admission.BOOTSTRAP_SCHEMA_VERSION
            if mode == "bootstrap"
            else release_gate_module.prod_admission.SCHEMA_VERSION
        ),
        "admission_key_fingerprint": "c" * 64,
        "queue_if_blocked": False,
        "resource_gate_bypass": False,
    }
    if mode == "bootstrap":
        value.update({
            "bootstrap_epoch_id": "rca-bootstrap-release-20260710",
            "bootstrap_started_at": (NOW - timedelta(days=1)).isoformat(),
            "bootstrap_deadline": (NOW + timedelta(days=7)).isoformat(),
            "bootstrap_authorization_fingerprint": "d" * 64,
            "release_bom_sha256": release_bom_sha256,
            "release_approval_id": "release-approval-20260710",
            "max_concurrency": release_gate_module.prod_bootstrap.MAX_CONCURRENCY,
            "daily_started_attempt_quota": (
                release_gate_module.prod_bootstrap.DAILY_STARTED_ATTEMPT_QUOTA
            ),
            "quota_timezone": release_gate_module.prod_bootstrap.QUOTA_TIMEZONE,
            "root_required_available_bytes": (
                release_gate_module.prod_bootstrap.ROOT_REQUIRED_AVAILABLE_BYTES
            ),
            "delivery_required_available_bytes": (
                release_gate_module.prod_bootstrap.DELIVERY_REQUIRED_AVAILABLE_BYTES
            ),
        })
    return value


@pytest.mark.parametrize("mode", ["steady", "bootstrap"])
def test_canary_capacity_admission_distinguishes_bootstrap_and_steady(mode):
    release_bom_sha256 = "e" * 64

    detail = release_gate_module._check_canary_capacity_admission(
        _capacity_admission_fixture(
            mode,
            release_bom_sha256=release_bom_sha256,
        ),
        expected_capacity_mode=mode,
        expected_release_bom_sha256=(
            release_bom_sha256 if mode == "bootstrap" else ""
        ),
        now=NOW,
    )

    assert detail["capacity_mode"] == mode
    assert detail["resource_class"] == "rca_prod"
    if mode == "steady":
        assert not any(key.startswith("bootstrap_") for key in detail)
    else:
        assert detail["max_concurrency"] == 1
        assert detail["daily_started_attempt_quota"] == 5
        assert detail["quota_timezone"] == "UTC"


@pytest.mark.parametrize(
    "tamper",
    ["mode", "steady_bootstrap_key", "quota", "deadline", "bom"],
)
def test_canary_capacity_admission_rejects_mode_and_epoch_drift(tamper):
    expected_bom = "e" * 64
    mode = "steady" if tamper == "steady_bootstrap_key" else "bootstrap"
    value = _capacity_admission_fixture(mode, release_bom_sha256=expected_bom)
    if tamper == "mode":
        value["capacity_mode"] = "steady"
    elif tamper == "steady_bootstrap_key":
        value["bootstrap_epoch_id"] = "rca-bootstrap-forged"
    elif tamper == "quota":
        value["daily_started_attempt_quota"] = 6
    elif tamper == "deadline":
        value["bootstrap_deadline"] = (NOW + timedelta(days=9)).isoformat()
    else:
        value["release_bom_sha256"] = "f" * 64

    with pytest.raises(EvidenceError):
        release_gate_module._check_canary_capacity_admission(
            value,
            expected_capacity_mode=mode,
            expected_release_bom_sha256=expected_bom,
            now=NOW,
        )


def _bootstrap_release_authorization_fixture(
    tmp_path: Path,
    monkeypatch,
    *,
    authorization_started_at: datetime | None = None,
    authorization_deadline: datetime | None = None,
) -> tuple[SimpleNamespace, Path, dict[str, str], dict[str, Any]]:
    from gateway import pnc_rca_prod_bootstrap as bootstrap

    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir(mode=0o700)
    release_id = "rca-release-test-0001"
    release_bom_sha256 = "b" * 64
    approval_receipt_sha256 = "a" * 64
    created_at = (NOW - timedelta(minutes=5)).isoformat()
    build_manifest = {
        "schema_version": BUILD_MANIFEST_SCHEMA_VERSION,
        "release_bom_sha256": release_bom_sha256,
    }
    cutover_plan = {"schema_version": CUTOVER_PLAN_SCHEMA_VERSION}
    release_plan = {
        "schema_version": release_gate_module.RELEASE_PREPARE_PLAN_SCHEMA_VERSION,
        "release_id": release_id,
        "created_at": created_at,
        "mode": "plan_only",
        "executed": False,
        "approval": {"receipt_sha256": approval_receipt_sha256},
        "bindings": {"release_bom_sha256": release_bom_sha256},
        "candidate_plist_sha256": {},
        "workspace_governance": {},
        "external_dependencies": {},
        "action_set": list(release_gate_module.RELEASE_PREPARE_ACTION_SET),
        "action_set_sha256": _sha256_json(
            list(release_gate_module.RELEASE_PREPARE_ACTION_SET)
        ),
        "gate_validation": {},
        "rollback": {},
        "side_effect_contract": {},
    }
    bodies = {
        "build_manifest.json": build_manifest,
        "cutover_plan.json": cutover_plan,
        "release_plan.json": release_plan,
    }
    descriptors: dict[str, dict[str, Any]] = {}
    for filename, body in bodies.items():
        path = evidence_dir / filename
        _write_json(path, body)
        path.chmod(0o600)
        raw = path.read_bytes()
        descriptors[filename] = {
            "sha256": hashlib.sha256(raw).hexdigest(),
            "size_bytes": len(raw),
            "schema_version": body["schema_version"],
        }
    manifest = {
        "schema_version": (
            release_gate_module.RELEASE_PREPARE_FINAL_MANIFEST_SCHEMA_VERSION
        ),
        "release_id": release_id,
        "created_at": created_at,
        "complete": True,
        "plan_only": True,
        "run_identity": {},
        "artifacts": descriptors,
        "approval_receipt_sha256": approval_receipt_sha256,
        "approval_request_sha256": "c" * 64,
        "release_bom_sha256": release_bom_sha256,
        "workspace_runtime_sha256": "d" * 64,
        "future_runtime_sha256": "e" * 64,
        "action_set_sha256": _sha256_json(
            list(release_gate_module.RELEASE_PREPARE_ACTION_SET)
        ),
        "side_effect_contract": {},
    }
    manifest_path = evidence_dir / "release_prepare_manifest.json"
    _write_json(manifest_path, manifest)
    manifest_path.chmod(0o600)
    authorization = bootstrap.issue_bootstrap_authorization(
        bootstrap_epoch_id="rca-bootstrap-test-0001",
        started_at=(authorization_started_at or NOW - timedelta(hours=1)),
        deadline=(authorization_deadline or NOW + timedelta(days=7, hours=18)),
        release_approval_id=release_id,
        release_bom_sha256=release_bom_sha256,
        approval_evidence_sha256=approval_receipt_sha256,
        authorized_by="release-owner",
        authorized_role="owner",
        now=NOW,
    )
    authorization_path = tmp_path / "rca-bootstrap-capacity-authorization.json"
    _write_json(authorization_path, authorization)
    authorization_path.chmod(0o600)
    authorization_raw_sha256 = hashlib.sha256(
        authorization_path.read_bytes()
    ).hexdigest()
    monkeypatch.setattr(
        bootstrap,
        "BOOTSTRAP_AUTHORIZATION_PATH",
        authorization_path,
    )
    active_release_binding_path = tmp_path / "active-release-binding.json"
    live_env_path = tmp_path / "live.env"
    monkeypatch.setattr(
        bootstrap,
        "load_active_release_binding",
        lambda **_kwargs: {
            "binding_ready": True,
            "binding_receipt_sha256": "9" * 64,
            "release_id": release_id,
            "bootstrap_epoch_id": authorization["bootstrap_epoch_id"],
            "release_bom_sha256": release_bom_sha256,
            "approval_evidence_sha256": approval_receipt_sha256,
            "authorization_receipt_sha256": authorization_raw_sha256,
            "authorization_fingerprint": authorization["receipt_fingerprint"],
            "candidate_env_sha256": "8" * 64,
        },
    )
    return (
        SimpleNamespace(
            capacity_mode="bootstrap",
            release_id=release_id,
            bootstrap_epoch_id="rca-bootstrap-test-0001",
            active_release_binding_path=active_release_binding_path,
            live_env_path=live_env_path,
            control_db_path=tmp_path / "control.sqlite3",
        ),
        evidence_dir,
        {
            "release_id": release_id,
            "release_bom_sha256": release_bom_sha256,
            "approval_receipt_sha256": approval_receipt_sha256,
        },
        authorization,
    )


def _publish_bootstrap_producer_fixture(
    dispatcher,
    expected,
    monkeypatch,
    *,
    activated_at: datetime,
):
    hmac_key = b"release-gate-producer-key-32byt!"
    monkeypatch.setattr(
        release_gate_module.capacity_runtime,
        "load_capacity_hmac_key",
        lambda: hmac_key,
    )
    receipt = release_gate_module.capacity_evidence.issue_producer_activation_receipt(
        release_id=expected["release_id"],
        bootstrap_epoch_id=dispatcher.bootstrap_epoch_id,
        release_bom_sha256=expected["release_bom_sha256"],
        active_release_binding_sha256="9" * 64,
        activated_at=activated_at,
        hmac_key=hmac_key,
        receipt_id="producer-release-gate-test",
    )
    path = release_gate_module.capacity_runtime.CapacityRuntimePaths.from_control_db(
        dispatcher.control_db_path
    ).producer_activation
    release_gate_module.capacity_evidence.write_owner_only_create_once(path, receipt)
    return receipt, path


def test_bootstrap_capacity_gate_binds_live_authorization_to_release_manifest(
    tmp_path,
    monkeypatch,
):
    dispatcher, evidence_dir, expected, authorization = (
        _bootstrap_release_authorization_fixture(tmp_path, monkeypatch)
    )
    evidence_hashes: dict[str, str] = {}
    detail = release_gate_module._check_bootstrap_capacity_authorization(
        dispatcher=dispatcher,
        evidence_dir=evidence_dir,
        evidence_hashes=evidence_hashes,
        expected_release_bom_sha256=expected["release_bom_sha256"],
        now=NOW,
    )

    assert detail["capacity_mode"] == "bootstrap"
    assert detail["release_id"] == expected["release_id"]
    assert detail["release_bom_sha256"] == expected["release_bom_sha256"]
    assert detail["approval_evidence_sha256"] == expected[
        "approval_receipt_sha256"
    ]
    assert detail["authorization_fingerprint"] == authorization[
        "receipt_fingerprint"
    ]
    assert set(evidence_hashes) == {
        "build_manifest.json",
        "cutover_plan.json",
        "release_plan.json",
        "release_prepare_manifest.json",
    }


def test_production_bootstrap_gate_requires_bound_producer_window(
    tmp_path,
    monkeypatch,
):
    dispatcher, evidence_dir, expected, _authorization = (
        _bootstrap_release_authorization_fixture(tmp_path, monkeypatch)
    )

    with pytest.raises(
        EvidenceError,
        match="bootstrap_capacity_producer_receipt_required",
    ):
        release_gate_module._check_bootstrap_capacity_authorization(
            dispatcher=dispatcher,
            evidence_dir=evidence_dir,
            evidence_hashes={},
            expected_release_bom_sha256=expected["release_bom_sha256"],
            now=NOW,
            require_producer_window=True,
        )

    receipt, _path = _publish_bootstrap_producer_fixture(
        dispatcher,
        expected,
        monkeypatch,
        activated_at=NOW,
    )
    detail = release_gate_module._check_bootstrap_capacity_authorization(
        dispatcher=dispatcher,
        evidence_dir=evidence_dir,
        evidence_hashes={},
        expected_release_bom_sha256=expected["release_bom_sha256"],
        now=NOW + timedelta(hours=17),
        require_producer_window=True,
    )

    assert detail["producer_activation"]["receipt_fingerprint"] == receipt[
        "receipt_fingerprint"
    ]
    assert detail["producer_activation"]["remaining_seconds"] == (
        timedelta(days=7, hours=18).total_seconds()
    )
    assert detail["producer_activation"]["live_horizon"][
        "remaining_seconds"
    ] == timedelta(days=7, hours=1).total_seconds()

    exact = release_gate_module._check_bootstrap_capacity_authorization(
        dispatcher=dispatcher,
        evidence_dir=evidence_dir,
        evidence_hashes={},
        expected_release_bom_sha256=expected["release_bom_sha256"],
        now=NOW + timedelta(hours=18),
        require_producer_window=True,
    )
    assert exact["producer_activation"]["live_horizon"][
        "remaining_seconds"
    ] == timedelta(days=7).total_seconds()

    with pytest.raises(
        EvidenceError,
        match="bootstrap_capacity_producer_horizon_insufficient",
    ):
        release_gate_module._check_bootstrap_capacity_authorization(
            dispatcher=dispatcher,
            evidence_dir=evidence_dir,
            evidence_hashes={},
            expected_release_bom_sha256=expected["release_bom_sha256"],
            now=NOW + timedelta(hours=18, microseconds=1),
            require_producer_window=True,
        )


def test_production_bootstrap_gate_rejects_signed_late_producer(
    tmp_path,
    monkeypatch,
):
    dispatcher, evidence_dir, expected, _authorization = (
        _bootstrap_release_authorization_fixture(
            tmp_path,
            monkeypatch,
            authorization_started_at=NOW - timedelta(hours=1),
            authorization_deadline=NOW + timedelta(days=7),
        )
    )
    _publish_bootstrap_producer_fixture(
        dispatcher,
        expected,
        monkeypatch,
        activated_at=NOW,
    )

    with pytest.raises(
        EvidenceError,
        match="bootstrap_capacity_producer_window_insufficient",
    ):
        release_gate_module._check_bootstrap_capacity_authorization(
            dispatcher=dispatcher,
            evidence_dir=evidence_dir,
            evidence_hashes={},
            expected_release_bom_sha256=expected["release_bom_sha256"],
            now=NOW,
            require_producer_window=True,
        )


@pytest.mark.parametrize(
    "attack",
    ["dispatcher_mode", "manifest_approval", "live_bom", "live_file_mode"],
)
def test_bootstrap_capacity_gate_rejects_rebound_or_unsafe_authorization(
    tmp_path,
    monkeypatch,
    attack,
):
    from gateway import pnc_rca_prod_bootstrap as bootstrap

    dispatcher, evidence_dir, expected, authorization = (
        _bootstrap_release_authorization_fixture(tmp_path, monkeypatch)
    )
    if attack == "dispatcher_mode":
        dispatcher.capacity_mode = "steady"
    elif attack == "manifest_approval":
        manifest_path = evidence_dir / "release_prepare_manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["approval_receipt_sha256"] = "f" * 64
        _write_json(manifest_path, manifest)
        manifest_path.chmod(0o600)
    elif attack == "live_bom":
        authorization["release_approval"]["release_bom_sha256"] = "f" * 64
        authorization["receipt_fingerprint"] = bootstrap.authorization_fingerprint(
            authorization
        )
        _write_json(bootstrap.BOOTSTRAP_AUTHORIZATION_PATH, authorization)
        bootstrap.BOOTSTRAP_AUTHORIZATION_PATH.chmod(0o600)
    else:
        bootstrap.BOOTSTRAP_AUTHORIZATION_PATH.chmod(0o644)

    with pytest.raises(EvidenceError):
        release_gate_module._check_bootstrap_capacity_authorization(
            dispatcher=dispatcher,
            evidence_dir=evidence_dir,
            evidence_hashes={},
            expected_release_bom_sha256=expected["release_bom_sha256"],
            now=NOW,
        )


@pytest.mark.parametrize(
    "field",
    [
        "bootstrap_epoch_id",
        "bootstrap_authorization_fingerprint",
        "release_bom_sha256",
        "release_approval_id",
    ],
)
def test_completed_bootstrap_canary_must_bind_same_live_authorization(field):
    capacity = _capacity_admission_fixture(
        "bootstrap",
        release_bom_sha256="b" * 64,
    )
    detail = {"vm_execution": {"capacity_admission": capacity}}
    authorization = {
        "bootstrap_epoch_id": capacity["bootstrap_epoch_id"],
        "started_at": capacity["bootstrap_started_at"],
        "deadline": capacity["bootstrap_deadline"],
        "authorization_fingerprint": capacity[
            "bootstrap_authorization_fingerprint"
        ],
        "release_bom_sha256": capacity["release_bom_sha256"],
        "release_id": capacity["release_approval_id"],
    }
    release_gate_module._bind_canary_bootstrap_authorization(
        detail,
        authorization,
    )
    capacity[field] = "f" * 64
    with pytest.raises(
        EvidenceError,
        match="canary_vm_bootstrap_live_authorization_mismatch",
    ):
        release_gate_module._bind_canary_bootstrap_authorization(
            detail,
            authorization,
        )


@pytest.mark.parametrize(
    "field",
    [
        "receipt_fingerprint",
        "authorization_receipt_sha256",
        "active_release_binding_sha256",
        "candidate_env_sha256",
        "release_bom_sha256",
        "approval_evidence_sha256",
    ],
)
def test_outbox_bootstrap_health_must_match_live_authorization(
    tmp_path,
    monkeypatch,
    field,
):
    from gateway import pnc_rca_prod_bootstrap as bootstrap

    _dispatcher, _evidence_dir, expected, authorization = (
        _bootstrap_release_authorization_fixture(tmp_path, monkeypatch)
    )
    live = bootstrap.load_bootstrap_authorization(
        now=NOW,
        expected_epoch_id=authorization["bootstrap_epoch_id"],
        expected_release_approval_id=expected["release_id"],
    )
    health_authorization = {
        "bootstrap_epoch_id": live["bootstrap_epoch_id"],
        "started_at": live["started_at"],
        "deadline": live["deadline"],
        "receipt_fingerprint": live["receipt_fingerprint"],
        "authorization_receipt_sha256": live["authorization_receipt_sha256"],
        "active_release_binding_sha256": "9" * 64,
        "candidate_env_sha256": "8" * 64,
        "release_bom_sha256": live["release_bom_sha256"],
        "release_approval_id": live["release_approval_id"],
        "approval_evidence_sha256": live["approval_evidence_sha256"],
    }
    status = {
        "required": True,
        "ready": True,
        "state": "ready",
        "error_code": "",
        "capacity_mode": "bootstrap",
        "authorization": health_authorization,
    }
    config = {
        "capacity_mode": "bootstrap",
        "release_id": expected["release_id"],
        "bootstrap_epoch_id": authorization["bootstrap_epoch_id"],
        "active_release_binding_path": str(tmp_path / "active-release-binding.json"),
        "live_env_path": str(tmp_path / "live.env"),
    }
    accepted = release_gate_module._check_outbox_capacity_admission_health(
        status,
        config=config,
        now=NOW,
        artifact="outbox_dispatcher_health",
    )
    assert accepted["authorization"][field] == health_authorization[field]

    health_authorization[field] = "f" * 64
    with pytest.raises(EvidenceError, match="capacity_admission_live_mismatch"):
        release_gate_module._check_outbox_capacity_admission_health(
            status,
            config=config,
            now=NOW,
            artifact="outbox_dispatcher_health",
        )


def _capacity_runtime_health_projection(
    *,
    mode: str,
    release_id: str,
    epoch_id: str,
    active_binding: str,
) -> dict:
    steady = mode == "steady"
    artifacts = None
    if steady:
        artifacts = {
            "transition_intent_sha256": "1" * 64,
            "transition_intent_fingerprint": "2" * 64,
            "transition_authorization_sha256": "3" * 64,
            "transition_authorization_fingerprint": "4" * 64,
            "transition_receipt_sha256": "5" * 64,
            "transition_receipt_fingerprint": "6" * 64,
            "commit_marker_sha256": "7" * 64,
            "commit_marker_fingerprint": "8" * 64,
            "evidence_bundle_sha256": "a" * 64,
            "evidence_bundle_fingerprint": "b" * 64,
        }
    return {
        "schema_version": "pnc_rca_capacity_runtime_decision_v1",
        "configured": True,
        "legacy_compatibility": False,
        "initial_policy": "bootstrap",
        "effective_state": "STEADY_ACTIVE" if steady else "BOOTSTRAP_PRODUCTION",
        "effective_mode": mode,
        "generation": 2 if steady else 1,
        "irreversible": steady,
        "ready": True,
        "reason_code": (
            "rca_capacity_steady_commit_valid"
            if steady
            else "rca_capacity_steady_samples_insufficient"
        ),
        "current_release_id": release_id,
        "current_bootstrap_epoch_id": epoch_id,
        "ratchet_origin_release_id": release_id,
        "ratchet_origin_bootstrap_epoch_id": epoch_id,
        "active_release_binding_sha256": active_binding,
        "ledger": {
            "sample_count": 20 if steady else 0,
            "sha256": "c" * 64,
            "window_seconds": 604800.0 if steady else 0.0,
            "max_gap_seconds": 36000.0 if steady else 0.0,
            "first_observed_at": NOW.isoformat() if steady else None,
            "last_observed_at": (
                (NOW + timedelta(days=7)).isoformat() if steady else None
            ),
            "steady_qualified": steady,
        },
        "artifacts": artifacts,
        "lock": {"held": True, "latency_ms": 0.1, "error_code": ""},
    }


def test_outbox_capacity_health_accepts_dynamic_bootstrap_to_steady_runtime(
    tmp_path,
    monkeypatch,
):
    _dispatcher, _evidence_dir, expected, authorization = (
        _bootstrap_release_authorization_fixture(tmp_path, monkeypatch)
    )
    config = {
        "capacity_mode": "bootstrap",
        "release_id": expected["release_id"],
        "bootstrap_epoch_id": authorization["bootstrap_epoch_id"],
        "active_release_binding_path": str(tmp_path / "active-release-binding.json"),
        "live_env_path": str(tmp_path / "live.env"),
        "dispatch_enabled": True,
        "activation_required": True,
    }
    runtime = _capacity_runtime_health_projection(
        mode="steady",
        release_id=expected["release_id"],
        epoch_id=authorization["bootstrap_epoch_id"],
        active_binding="9" * 64,
    )
    status = {
        "required": True,
        "ready": True,
        "state": "STEADY_ACTIVE",
        "error_code": "",
        "capacity_mode": "steady",
        "authorization": None,
        "runtime": runtime,
    }
    accepted = release_gate_module._check_outbox_capacity_admission_health(
        status,
        config=config,
        now=NOW,
        artifact="outbox_dispatcher_health",
    )
    assert accepted["runtime_projection"]["generation"] == 2

    del runtime["artifacts"]["transition_intent_sha256"]
    with pytest.raises(EvidenceError, match="capacity_runtime_steady_invalid"):
        release_gate_module._check_outbox_capacity_admission_health(
            status,
            config=config,
            now=NOW,
            artifact="outbox_dispatcher_health",
        )


def test_outbox_capacity_health_accepts_dynamic_empty_ledger_bootstrap_runtime(
    tmp_path,
    monkeypatch,
):
    _dispatcher, _evidence_dir, expected, authorization = (
        _bootstrap_release_authorization_fixture(tmp_path, monkeypatch)
    )
    config = {
        "capacity_mode": "bootstrap",
        "release_id": expected["release_id"],
        "bootstrap_epoch_id": authorization["bootstrap_epoch_id"],
        "active_release_binding_path": str(tmp_path / "active-release-binding.json"),
        "live_env_path": str(tmp_path / "live.env"),
        "dispatch_enabled": True,
        "activation_required": True,
    }
    runtime = _capacity_runtime_health_projection(
        mode="bootstrap",
        release_id=expected["release_id"],
        epoch_id=authorization["bootstrap_epoch_id"],
        active_binding="9" * 64,
    )
    live_authorization = (
        release_gate_module.prod_bootstrap.load_bootstrap_authorization(
            now=NOW,
            expected_epoch_id=authorization["bootstrap_epoch_id"],
            expected_release_bom_sha256=expected["release_bom_sha256"],
            expected_release_approval_id=expected["release_id"],
            expected_approval_evidence_sha256=expected[
                "approval_receipt_sha256"
            ],
        )
    )
    status = {
        "required": True,
        "ready": True,
        "state": "BOOTSTRAP_PRODUCTION",
        "error_code": "",
        "capacity_mode": "bootstrap",
        "authorization": {
            "bootstrap_epoch_id": authorization["bootstrap_epoch_id"],
            "started_at": live_authorization["started_at"],
            "deadline": live_authorization["deadline"],
            "receipt_fingerprint": live_authorization["receipt_fingerprint"],
            "authorization_receipt_sha256": live_authorization[
                "authorization_receipt_sha256"
            ],
            "active_release_binding_sha256": "9" * 64,
            "candidate_env_sha256": "8" * 64,
            "release_bom_sha256": live_authorization["release_bom_sha256"],
            "release_approval_id": live_authorization["release_approval_id"],
            "approval_evidence_sha256": live_authorization[
                "approval_evidence_sha256"
            ],
        },
        "runtime": runtime,
    }
    accepted = release_gate_module._check_outbox_capacity_admission_health(
        status,
        config=config,
        now=NOW,
        artifact="outbox_dispatcher_health",
    )
    assert accepted["runtime_projection"] == {
        "effective_mode": "bootstrap",
        "effective_state": "BOOTSTRAP_PRODUCTION",
        "generation": 1,
        "active_release_binding_sha256": "9" * 64,
        "sample_count": 0,
    }

    status["state"] = "ready"
    with pytest.raises(EvidenceError, match="capacity_admission_not_ready"):
        release_gate_module._check_outbox_capacity_admission_health(
            status,
            config=config,
            now=NOW,
            artifact="outbox_dispatcher_health",
        )


def _cutover_precutover_service_state(pid: int) -> dict:
    jobs = {}
    for label in release_gate_module.cutover_guard.SERVICE_LABELS:
        loaded = label == release_gate_module.cutover_guard.GATEWAY_LABEL
        jobs[label] = {
            "launchd": {
                "label": label,
                "loaded": loaded,
                "state": "running" if loaded else "absent",
                "pid": pid if loaded else None,
                "last_exit_status": None,
            },
            "plist": {
                "path": str(
                    release_gate_module.cutover_guard.CANONICAL_LAUNCH_AGENTS_ROOT
                    / f"{label}.plist"
                ),
                "state": "regular",
                "sha256": hashlib.sha256(label.encode()).hexdigest(),
                "size_bytes": len(label),
                "mode": "0644",
                "uid": os.geteuid(),
                "nlink": 1,
            },
        }
    return {
        "schema_version": (
            release_gate_module.cutover_guard.LIVE_SERVICE_STATE_SCHEMA_VERSION
        ),
        "target_runtime_root": str(
            release_gate_module.cutover_guard.CANONICAL_LIVE_ROOT
        ),
        "labels": list(release_gate_module.cutover_guard.SERVICE_LABELS),
        "jobs": jobs,
    }


def _production_cutover_gate_fixture() -> tuple[dict, dict, dict]:
    names = {
        "release_prepare_manifest",
        "approval_receipt",
        "writer_stop_receipt",
        "feishu_hold_plan",
        "feishu_hold_approval_receipt",
        "feishu_hold_cutover_binding",
        "feishu_hold_receipt",
        "env_stage_receipt",
        "runtime_stage_manifest",
        "workspace_runtime_manifest",
        "cutover_authorization_receipt",
    }
    sha = {
        name: hashlib.sha256(name.encode("ascii")).hexdigest() for name in names
    }
    release_id = "rca-prod-cutover-20260713"
    lease = "1" * 64
    release_bom = "2" * 64
    epoch_id = "rca-bootstrap-cutover-20260713"
    approval_sha = sha["approval_receipt"]
    hold_approval_sha = sha["feishu_hold_approval_receipt"]
    hold_id = "hold-cutover-20260713"
    candidate_env_sha = "3" * 64
    auth_raw_sha = "4" * 64
    auth_fingerprint = "5" * 64
    old_gateway = {"process": {"pid": 41001}}
    precutover_services = _cutover_precutover_service_state(41001)
    artifacts = {
        "release_prepare_manifest": {
            "schema_version": (
                release_gate_module.RELEASE_PREPARE_FINAL_MANIFEST_SCHEMA_VERSION
            ),
            "release_id": release_id,
            "complete": True,
            "plan_only": True,
            "approval_receipt_sha256": approval_sha,
            "release_bom_sha256": release_bom,
        },
        "approval_receipt": {
            "schema_version": (
                release_gate_module.RELEASE_APPROVAL_RECEIPT_SCHEMA_VERSION
            ),
            "release_id": release_id,
        },
        "writer_stop_receipt": {
            "schema_version": (
                release_gate_module.cutover_guard.WRITER_STOP_RECEIPT_SCHEMA_VERSION
            ),
            "release_id": release_id,
            "lease_fingerprint": lease,
            "release_prepare_manifest_sha256": sha[
                "release_prepare_manifest"
            ],
            "approval_receipt_sha256": hold_approval_sha,
            "old_gateway_runtime_identity": old_gateway,
            "precutover_service_state": precutover_services,
            "precutover_service_state_sha256": (
                release_gate_module.cutover_guard._sha256_json(precutover_services)
            ),
            "writer_stop_observation": {
                "launchd": {"pid": None},
                "process_census": {"matching_processes": []},
            },
        },
        "feishu_hold_plan": {
            "schema_version": release_gate_module.feishu_ingress_hold.PLAN_SCHEMA_VERSION,
            "hold_id": hold_id,
            "production_effects_executed": False,
        },
        "feishu_hold_approval_receipt": {
            "schema_version": (
                release_gate_module.feishu_ingress_hold.APPROVAL_SCHEMA_VERSION
            ),
            "hold_id": hold_id,
            "plan_sha256": sha["feishu_hold_plan"],
        },
        "feishu_hold_cutover_binding": {
            "schema_version": (
                release_gate_module.feishu_ingress_hold.CUTOVER_BINDING_SCHEMA_VERSION
            ),
            "release_id": release_id,
            "plan_sha256": sha["feishu_hold_plan"],
            "writer_stop_receipt_sha256": sha["writer_stop_receipt"],
            "cutover_lease_fingerprint": lease,
            "release_prepare_manifest_sha256": sha[
                "release_prepare_manifest"
            ],
            "release_approval_receipt_sha256": hold_approval_sha,
        },
        "feishu_hold_receipt": {
            "schema_version": (
                release_gate_module.feishu_ingress_hold.APPLY_RECEIPT_SCHEMA_VERSION
            ),
            "ok": True,
            "plan_sha256": sha["feishu_hold_plan"],
            "approval": {"receipt_sha256": hold_approval_sha},
            "future_install": {"staged_sha256": "6" * 64},
        },
        "env_stage_receipt": {
            "schema_version": (
                release_gate_module.prod_bootstrap.ACTIVE_RELEASE_BINDING_SCHEMA_VERSION
            ),
            "release_id": release_id,
            "complete": True,
            "live_write_performed": False,
            "bindings": {
                "release_prepare_manifest": {
                    "sha256": sha["release_prepare_manifest"]
                },
                "release_approval": {"sha256": approval_sha},
                "release_bom_sha256": release_bom,
                "bootstrap_authorization": {
                    "sha256": auth_raw_sha,
                    "receipt_fingerprint": auth_fingerprint,
                },
                "candidate_env": {"sha256": candidate_env_sha},
            },
            "policy": {
                "kafka": {"activation_required": True},
                "stores": {
                    "runtime_state_root": str(
                        release_gate_module.AUXILIARY_RUNTIME_STATE_ROOT
                    ),
                    "control_database_shared": True,
                    "health_paths_explicit": True,
                },
                "capacity_admission": {
                    "capacity_mode": "bootstrap",
                    "bootstrap_epoch_id": epoch_id,
                    "bootstrap_authorization_sha256": auth_raw_sha,
                    "bootstrap_authorization_fingerprint": auth_fingerprint,
                    "release_bom_sha256": release_bom,
                    "release_approval_id": release_id,
                    "approval_evidence_sha256": approval_sha,
                },
            },
            "side_effect_contract": {
                "canonical_active_release_binding": (
                    str(release_gate_module.AUXILIARY_RUNTIME_STATE_ROOT)
                    + "/active-release-binding.json"
                )
            },
        },
        "runtime_stage_manifest": {
            "content_sha256": "7" * 64,
            "future_canonical_projection": {
                "candidate_plist_sha256": {
                    **{
                        f"{label}.candidate.plist": "8" * 64
                        for label in release_gate_module.CUTOVER_RESIDENT_START_ORDER
                    },
                    "ai.hermes.gateway.candidate.plist": "8" * 64,
                    "local.pnc.completion-notice-relay.candidate.plist": "8" * 64,
                    "local.pnc.vm-task-sync.candidate.plist": "8" * 64,
                }
            },
        },
        "workspace_runtime_manifest": {"closure_sha256": "9" * 64},
        "cutover_authorization_receipt": {"fixture": True},
    }
    expected_live = "a" * 64
    auth_bindings = {
        "release_prepare_manifest_sha256": sha["release_prepare_manifest"],
        "approval_receipt_sha256": approval_sha,
        "release_bom_sha256": release_bom,
        "cutover_lease_fingerprint": lease,
        "writer_stop_receipt_sha256": sha["writer_stop_receipt"],
        "feishu_hold_plan_sha256": sha["feishu_hold_plan"],
        "feishu_hold_approval_receipt_sha256": hold_approval_sha,
        "feishu_hold_cutover_binding_sha256": sha[
            "feishu_hold_cutover_binding"
        ],
        "feishu_hold_receipt_sha256": sha["feishu_hold_receipt"],
        "env_stage_receipt_sha256": sha["env_stage_receipt"],
        "candidate_env_sha256": candidate_env_sha,
        "runtime_stage_manifest_sha256": sha["runtime_stage_manifest"],
        "workspace_runtime_manifest_sha256": sha["workspace_runtime_manifest"],
        "expected_live_identity_sha256": expected_live,
    }
    authorization = {
        "release_id": release_id,
        "receipt_sha256": sha["cutover_authorization_receipt"],
        "bindings": auth_bindings,
        "expires_at": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
        "nonce": "cutover-authorization-nonce-0001",
        "machine_identity_sha256": "b" * 64,
    }
    return artifacts, sha, authorization


def test_production_cutover_gate_authorizes_plan_and_exact_first_step():
    artifacts, sha, authorization = _production_cutover_gate_fixture()
    common = {
        "artifacts": artifacts,
        "artifact_sha256": sha,
        "cutover_lease_fingerprint": "1" * 64,
        "cutover_authorization": authorization,
    }
    plan = release_gate_module.validate_rca_cutover_execution_authorization(
        **common,
        requested_step="plan",
        live_identity_sha256=None,
        prior_step_receipt=None,
    )
    assert plan["ok"] is True
    assert plan["allowed_next_step"] == "plan"
    assert plan["gateway_aux_start_order"] == list(
        release_gate_module.CUTOVER_GATEWAY_AUX_START_ORDER
    )
    assert plan["resident_start_order"] == list(
        release_gate_module.CUTOVER_RESIDENT_START_ORDER
    )
    assert plan["active_release_binding_path"].endswith(
        "/active-release-binding.json"
    )
    first = release_gate_module.validate_rca_cutover_execution_authorization(
        **common,
        requested_step="snapshot_live",
        live_identity_sha256=plan["expected_live_identity_sha256"],
        prior_step_receipt=None,
    )
    assert first["allowed_next_step"] == "snapshot_live"
    stable = dict(first)
    stable["allowed_next_step"] = "plan"
    assert stable == plan


def test_production_cutover_gate_result_is_accepted_by_executor_contract():
    artifacts, sha, authorization = _production_cutover_gate_fixture()
    lease_fingerprint = "1" * 64
    gate_result = release_gate_module.validate_rca_cutover_execution_authorization(
        artifacts=artifacts,
        artifact_sha256=sha,
        cutover_lease_fingerprint=lease_fingerprint,
        cutover_authorization=authorization,
        requested_step="plan",
        live_identity_sha256=None,
        prior_step_receipt=None,
    )
    bundle = production_cutover_module.ArtifactBundle(
        {
            name: production_cutover_module._OwnedJson(
                path=Path(name),
                raw=name.encode("ascii"),
                body=body,
            )
            for name, body in artifacts.items()
        }
    )

    assert bundle.sha256 == sha
    assert production_cutover_module._validate_gate_result(
        gate_result,
        bundle=bundle,
        authorization=authorization,
        lease_fingerprint=lease_fingerprint,
        requested_step="plan",
    ) == gate_result


def _cutover_prior_receipt(
    *,
    index: int,
    step: str,
    before: str,
    after: str,
    evidence: Mapping[str, Any] | None = None,
    started_labels: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": "pnc_rca_production_cutover_step_result_v1",
        "plan_sha256": "c" * 64,
        "index": index,
        "step": step,
        "result": {
            "schema_version": "pnc_rca_production_cutover_step_result_v1",
            "step": step,
            "before_identity_sha256": before,
            "after_identity_sha256": after,
            "commands": [],
            "old_runtime_retained": True,
            "snapshot": None,
            "services": {},
            "evidence": dict(evidence or {}),
            "started_labels": list(started_labels or []),
        },
    }


def test_production_cutover_gate_requires_writer_stop_before_install():
    artifacts, sha, authorization = _production_cutover_gate_fixture()
    common = {
        "artifacts": artifacts,
        "artifact_sha256": sha,
        "cutover_lease_fingerprint": "1" * 64,
        "cutover_authorization": authorization,
    }
    live = authorization["bindings"]["expected_live_identity_sha256"]
    snapshot = _cutover_prior_receipt(
        index=1,
        step="snapshot_live",
        before=live,
        after=live,
    )
    stop = release_gate_module.validate_rca_cutover_execution_authorization(
        **common,
        requested_step="stop_writers",
        live_identity_sha256=live,
        prior_step_receipt=snapshot,
    )
    assert stop["allowed_next_step"] == "stop_writers"
    stopped = _cutover_prior_receipt(
        index=2,
        step="stop_writers",
        before=live,
        after=live,
        evidence={
            "schema_version": "pnc_rca_writer_stop_evidence_v1",
            "writer_labels": list(release_gate_module.CUTOVER_WRITER_LABELS),
            "runtime_quiesce_labels": list(
                release_gate_module.CUTOVER_RUNTIME_QUIESCE_LABELS
            ),
            "receipt_sha256": "d" * 64,
        },
    )
    install = release_gate_module.validate_rca_cutover_execution_authorization(
        **common,
        requested_step="install_feishu_sidecar",
        live_identity_sha256=live,
        prior_step_receipt=stopped,
    )
    assert install["allowed_next_step"] == "install_feishu_sidecar"
    started = _cutover_prior_receipt(
        index=8,
        step="start_gateway_aux",
        before=live,
        after=live,
        started_labels=list(release_gate_module.CUTOVER_GATEWAY_AUX_START_ORDER),
    )
    verified = release_gate_module.validate_rca_cutover_execution_authorization(
        **common,
        requested_step="verify_gateway_aux",
        live_identity_sha256=live,
        prior_step_receipt=started,
    )
    assert verified["allowed_next_step"] == "verify_gateway_aux"


def test_production_cutover_gate_tracks_writers_and_runtime_quiesce_separately():
    assert release_gate_module.CUTOVER_WRITER_LABELS == (
        production_cutover_module.WRITER_LABELS
    )
    assert release_gate_module.CUTOVER_RUNTIME_QUIESCE_LABELS == (
        production_cutover_module.RUNTIME_QUIESCE_LABELS
    )
    artifacts, sha, authorization = _production_cutover_gate_fixture()
    live = authorization["bindings"]["expected_live_identity_sha256"]
    stopped = _cutover_prior_receipt(
        index=2,
        step="stop_writers",
        before=live,
        after=live,
        evidence={
            "schema_version": "pnc_rca_writer_stop_evidence_v1",
            "writer_labels": list(release_gate_module.CUTOVER_WRITER_LABELS),
            "runtime_quiesce_labels": list(
                release_gate_module.CUTOVER_WRITER_LABELS
            ),
            "receipt_sha256": "d" * 64,
        },
    )

    with pytest.raises(
        release_gate_module.EvidenceError,
        match="production_cutover_writer_stop_evidence_invalid",
    ):
        release_gate_module.validate_rca_cutover_execution_authorization(
            artifacts=artifacts,
            artifact_sha256=sha,
            cutover_lease_fingerprint="1" * 64,
            cutover_authorization=authorization,
            requested_step="install_feishu_sidecar",
            live_identity_sha256=live,
            prior_step_receipt=stopped,
        )


@pytest.mark.parametrize(
    "attack",
    ["approval_alias", "env_authorization", "plan_live", "wrong_prior"],
)
def test_production_cutover_gate_rejects_cross_domain_or_step_rebinding(attack):
    artifacts, sha, authorization = _production_cutover_gate_fixture()
    common = {
        "artifacts": artifacts,
        "artifact_sha256": sha,
        "cutover_lease_fingerprint": "1" * 64,
        "cutover_authorization": authorization,
    }
    requested_step = "plan"
    live = None
    prior = None
    if attack == "approval_alias":
        sha["feishu_hold_approval_receipt"] = sha["approval_receipt"]
    elif attack == "env_authorization":
        artifacts["env_stage_receipt"]["bindings"]["bootstrap_authorization"][
            "sha256"
        ] = "f" * 64
    elif attack == "plan_live":
        live = "a" * 64
    else:
        requested_step = "stop_writers"
        live = "a" * 64
        prior = {
            "schema_version": "pnc_rca_production_cutover_step_result_v1",
            "plan_sha256": "c" * 64,
            "index": 1,
            "step": "wrong_step",
            "result": {
                "schema_version": "pnc_rca_production_cutover_step_result_v1",
                "step": "wrong_step",
                "before_identity_sha256": live,
                "after_identity_sha256": live,
                "commands": [],
                "old_runtime_retained": True,
                "snapshot": None,
                "services": {},
                "evidence": {},
                "started_labels": [],
            },
        }
    with pytest.raises(EvidenceError):
        release_gate_module.validate_rca_cutover_execution_authorization(
            **common,
            requested_step=requested_step,
            live_identity_sha256=live,
            prior_step_receipt=prior,
        )


def test_auxiliary_runtime_public_validators_bind_process_and_loaded_job(tmp_path):
    fixture = _auxiliary_runtime_test_fixture(tmp_path)

    relay = release_gate_module.validate_completion_relay_runtime_health(
        fixture.relay,
        now=NOW,
        expected_runtime=fixture.expected["completion_relay"],
        process_verifier=lambda **kwargs: {
            "pid": kwargs["pid"],
            "process_create_time": kwargs["process_create_time"],
            "live": True,
        },
    )
    sync = release_gate_module.validate_vm_task_sync_completion_receipt(
        fixture.sync,
        now=NOW,
        not_before=fixture.not_before,
        expected_runtime=fixture.expected["vm_task_sync"],
        launchd_verifier=lambda _expected: {
            "loaded": True,
            "current_pid_required": False,
        },
    )

    assert relay["loop_count"] == 4
    assert relay["process"]["live"] is True
    assert sync["launchd"]["loaded"] is True
    assert sync["launchd"]["current_pid_required"] is False


@pytest.mark.parametrize(
    "mutation",
    [
        lambda body: body.update(startup_canary_loops_completed=2),
        lambda body: body.update(configured_max_card_fallbacks_per_loop=1),
        lambda body: body.update(card_fallback_attempted_count=1),
        lambda body: body["runtime_identity"].update(script_sha256="f" * 64),
        lambda body: body.update(observed_at=(NOW - timedelta(seconds=61)).isoformat()),
    ],
)
def test_completion_relay_health_rejects_incomplete_canary_and_runtime_drift(
    tmp_path,
    mutation,
):
    fixture = _auxiliary_runtime_test_fixture(tmp_path)
    body = copy.deepcopy(fixture.relay)
    mutation(body)

    with pytest.raises(EvidenceError):
        release_gate_module.validate_completion_relay_runtime_health(
            body,
            now=NOW,
            expected_runtime=fixture.expected["completion_relay"],
            process_verifier=lambda **_kwargs: {"live": True},
        )


@pytest.mark.parametrize(
    "mutation",
    [
        lambda body: body.update(skipped=True),
        lambda body: body.update(exit_code=1),
        lambda body: body.update(
            started_at=(NOW - timedelta(minutes=3)).isoformat()
        ),
        lambda body: body.update(result_sha256="f" * 64),
        lambda body: body["runtime_identity"].update(plist_sha256="f" * 64),
    ],
)
def test_vm_task_sync_receipt_rejects_pre_cutover_failure_and_runtime_drift(
    tmp_path,
    mutation,
):
    fixture = _auxiliary_runtime_test_fixture(tmp_path)
    body = copy.deepcopy(fixture.sync)
    mutation(body)

    with pytest.raises(EvidenceError):
        release_gate_module.validate_vm_task_sync_completion_receipt(
            body,
            now=NOW,
            not_before=fixture.not_before,
            expected_runtime=fixture.expected["vm_task_sync"],
            launchd_verifier=lambda _expected: {"loaded": True},
        )


def _release_approval_binding_fixture(tmp_path: Path):
    _consumer, _dispatcher, settings = _gate(tmp_path, "preauthorization")
    manifest = json.loads(
        (settings.evidence_dir / "build_manifest.json").read_text(encoding="utf-8")
    )
    bom = manifest["release_bom"]
    workspace = bom["components"]["workspace"]
    machine_identity = {
        "source": "test_machine_id",
        "sha256": "9" * 64,
    }
    identity = {
        "schema_version": (
            release_gate_module.RELEASE_APPROVAL_IDENTITY_SCHEMA_VERSION
        ),
        "method": release_gate_module.RELEASE_APPROVAL_IDENTITY_METHOD,
        "uid": os.geteuid(),
        "username": release_gate_module.pwd.getpwuid(os.geteuid()).pw_name,
        "machine_identity_source": machine_identity["source"],
        "machine_identity_sha256": machine_identity["sha256"],
    }
    t0 = {
        "schema_version": release_gate_module.RELEASE_PREPARE_T0_SCHEMA_VERSION,
        "topic": TOPIC,
        "group_id": "rca_root_cause_analysis_agent",
        "initial_offsets": {"0": 101, "1": 202},
    }
    rollback = {
        "schema_version": (
            release_gate_module.RELEASE_PREPARE_ROLLBACK_SCHEMA_VERSION
        ),
        "owner": "release-owner",
        "procedure": "restore the exact predecessor bundle",
        "max_restore_seconds": 300,
        "rollback_window_seconds": 3600,
    }
    actions = list(release_gate_module.RELEASE_PREPARE_ACTION_SET)
    unscoped = {
        "classification": "DRIFT-PREEXISTING",
        "dirty_count": 0,
        "status_sha256": EMPTY_GIT_STATUS_SHA256,
        "blocking": False,
    }
    request = {
        "schema_version": (
            release_gate_module.RELEASE_APPROVAL_REQUEST_SCHEMA_VERSION
        ),
        "release_id": "rca-production-20260713-approval-0001",
        "created_at": (NOW - timedelta(seconds=60)).isoformat(),
        "production_effects_executed": False,
        "approval_required_for_finalize": True,
        "approval_identity_requirement": identity,
        "action_set": actions,
        "action_set_sha256": _sha256_json(actions),
        "bindings": {
            "release_bom": bom,
            "release_bom_sha256": _sha256_json(bom),
            "build_manifest_sha256": hashlib.sha256(
                (settings.evidence_dir / "build_manifest.json").read_bytes()
            ).hexdigest(),
            "runtime_config_sha256": bom["runtime_config_sha256"],
            "launchd_config_sha256": bom["launchd_config_sha256"],
            "t0": t0,
            "t0_sha256": _sha256_json(t0),
            "rollback_config_sha256": _sha256_json(rollback),
            "rollback_window_seconds": rollback["rollback_window_seconds"],
            "workspace_closure_sha256": workspace[
                "execution_closure_sha256"
            ],
            "workspace_runtime_sha256": _sha256_json(bom["workspace_runtime"]),
            "future_runtime_sha256": _sha256_json(bom["future_runtime"]),
        },
        "candidate_plist_sha256": {
            filename: hashlib.sha256(filename.encode("utf-8")).hexdigest()
            for filename in release_gate_module.FUTURE_RUNTIME_PLIST_FILENAMES
        },
        "workspace_governance": {
            "schema_version": release_gate_module.WORKSPACE_GOVERNANCE_SCHEMA_VERSION,
            "execution_closure": {
                "ok": True,
                "hash": workspace["execution_closure_sha256"],
                "required_paths": list(
                    release_gate_module.WORKSPACE_EXECUTION_CLOSURE_RELATIVE_PATHS
                ),
                "commit": workspace["commit"],
                "files": {
                    relative: item["sha256"]
                    for relative, item in workspace["execution_closure"][
                        "files"
                    ].items()
                },
            },
            "unscoped_drift": unscoped,
        },
        "external_dependencies": bom["external_dependencies"],
        "rollback": rollback,
        "side_effect_contract": {
            "live_files_written": False,
            "launchctl_invoked": False,
            "kafka_consumer_created": False,
            "kafka_offsets_mutated": False,
            "feishu_writes": False,
            "vm_files_written": False,
            "output_scope": "unique_owner_only_run_root",
        },
    }
    request_sha256 = release_gate_module._release_prepare_file_sha256(request)
    receipt = {
        "schema_version": (
            release_gate_module.RELEASE_APPROVAL_RECEIPT_SCHEMA_VERSION
        ),
        "release_id": request["release_id"],
        "decision": release_gate_module.RELEASE_APPROVAL_DECISION,
        "created_at": (NOW - timedelta(seconds=30)).isoformat(),
        "expires_at": (NOW + timedelta(hours=1)).isoformat(),
        "nonce": "release-approval-nonce-0001",
        "action_set": actions,
        "action_set_sha256": request["action_set_sha256"],
        "approval_request_sha256": request_sha256,
        "release_bom_sha256": request["bindings"]["release_bom_sha256"],
        "workspace_runtime_sha256": request["bindings"][
            "workspace_runtime_sha256"
        ],
        "future_runtime_sha256": request["bindings"]["future_runtime_sha256"],
        "runtime_config_sha256": request["bindings"]["runtime_config_sha256"],
        "t0_sha256": request["bindings"]["t0_sha256"],
        "rollback_config_sha256": request["bindings"][
            "rollback_config_sha256"
        ],
        "rollback_window_seconds": request["bindings"][
            "rollback_window_seconds"
        ],
        "identity": identity,
    }
    return SimpleNamespace(
        request=request,
        request_sha256=request_sha256,
        receipt=receipt,
        receipt_sha256=release_gate_module._release_prepare_file_sha256(receipt),
        machine_identity=machine_identity,
    )


def _validate_release_approval_fixture(fixture, **overrides):
    return release_gate_module.validate_release_prepare_approval_binding(
        approval_request=overrides.get("request", fixture.request),
        approval_request_sha256=overrides.get(
            "request_sha256", fixture.request_sha256
        ),
        approval_receipt=overrides.get("receipt", fixture.receipt),
        approval_receipt_sha256=overrides.get(
            "receipt_sha256", fixture.receipt_sha256
        ),
        final_manifest_schema_version=overrides.get(
            "final_schema",
            release_gate_module.RELEASE_PREPARE_FINAL_MANIFEST_SCHEMA_VERSION,
        ),
        require_fresh_request=overrides.get("require_fresh_request", True),
        now=NOW,
        machine_identity_observer=lambda: overrides.get(
            "machine_identity", fixture.machine_identity
        ),
    )


def test_release_approval_binding_accepts_exact_request_receipt_pair(tmp_path):
    fixture = _release_approval_binding_fixture(tmp_path)

    result = _validate_release_approval_fixture(fixture)

    assert result == {
        "schema_version": (
            release_gate_module.RELEASE_APPROVAL_BINDING_VALIDATION_SCHEMA_VERSION
        ),
        "ok": True,
        "approval_request_sha256": fixture.request_sha256,
        "approval_receipt_sha256": fixture.receipt_sha256,
        "cutover_plan_schema_version": CUTOVER_PLAN_SCHEMA_VERSION,
        "final_manifest_schema_version": (
            release_gate_module.RELEASE_PREPARE_FINAL_MANIFEST_SCHEMA_VERSION
        ),
    }


def test_release_approval_binding_allows_stale_request_only_after_finalize(
    tmp_path,
):
    fixture = _release_approval_binding_fixture(tmp_path)
    request = json.loads(json.dumps(fixture.request))
    request["created_at"] = (
        NOW
        - timedelta(
            seconds=release_gate_module.DEFAULT_EVIDENCE_MAX_AGE_SECONDS + 1
        )
    ).isoformat()
    request_sha256 = release_gate_module._release_prepare_file_sha256(request)
    receipt = json.loads(json.dumps(fixture.receipt))
    receipt["approval_request_sha256"] = request_sha256
    receipt_sha256 = release_gate_module._release_prepare_file_sha256(receipt)

    with pytest.raises(EvidenceError) as error:
        _validate_release_approval_fixture(
            fixture,
            request=request,
            request_sha256=request_sha256,
            receipt=receipt,
            receipt_sha256=receipt_sha256,
        )
    assert error.value.code == "release_approval_request_stale"

    result = _validate_release_approval_fixture(
        fixture,
        request=request,
        request_sha256=request_sha256,
        receipt=receipt,
        receipt_sha256=receipt_sha256,
        require_fresh_request=False,
    )

    assert result["ok"] is True


def test_release_approval_binding_revalidates_staged_runtime(
    tmp_path,
    monkeypatch,
):
    fixture = _release_approval_binding_fixture(tmp_path)

    def rejected(_binding):
        raise EvidenceError("future_runtime_stage_manifest_live_mismatch")

    monkeypatch.setattr(
        release_gate_module,
        "_revalidate_future_runtime_release_binding",
        rejected,
    )

    with pytest.raises(EvidenceError) as error:
        _validate_release_approval_fixture(fixture)

    assert error.value.code == "future_runtime_stage_manifest_live_mismatch"


@pytest.mark.parametrize(
    ("mutation", "target", "code"),
    [
        (
            lambda body: body.update(unknown=True),
            "request",
            "release_approval_request_shape_invalid",
        ),
        (
            lambda body: body.update(approved=True),
            "receipt",
            "release_approval_receipt_shape_invalid",
        ),
        (
            lambda body: body["bindings"].update(runtime_config_sha256="f" * 64),
            "request_rehash",
            "release_approval_runtime_fingerprint_mismatch",
        ),
        (
            lambda body: body.update(action_set=["confirm_rca_production"]),
            "request_rehash",
            "release_approval_action_set_mismatch",
        ),
        (
            lambda body: body["bindings"]["t0"]["initial_offsets"].update({"0": 9}),
            "request_rehash",
            "release_approval_t0_hash_mismatch",
        ),
        (
            lambda body: body["rollback"].update(rollback_window_seconds=7200),
            "request_rehash",
            "release_approval_rollback_binding_mismatch",
        ),
        (
            lambda body: body["identity"].update(uid=os.geteuid() + 1),
            "receipt_rehash",
            "release_approval_identity_mismatch",
        ),
        (
            lambda body: body.update(nonce="short"),
            "receipt_rehash",
            "release_approval_nonce_invalid",
        ),
        (
            lambda body: body.update(
                created_at=(NOW - timedelta(minutes=2)).isoformat()
            ),
            "receipt_rehash",
            "release_approval_time_binding_invalid",
        ),
    ],
)
def test_release_approval_binding_rejects_mutation_and_legacy_forms(
    tmp_path,
    mutation,
    target,
    code,
):
    fixture = _release_approval_binding_fixture(tmp_path)
    request = copy.deepcopy(fixture.request)
    receipt = copy.deepcopy(fixture.receipt)
    if target.startswith("request"):
        mutation(request)
    else:
        mutation(receipt)
    request_sha256 = (
        release_gate_module._release_prepare_file_sha256(request)
        if target == "request_rehash"
        else fixture.request_sha256
    )
    receipt_sha256 = (
        release_gate_module._release_prepare_file_sha256(receipt)
        if target == "receipt_rehash"
        else fixture.receipt_sha256
    )

    with pytest.raises(EvidenceError) as error:
        _validate_release_approval_fixture(
            fixture,
            request=request,
            request_sha256=request_sha256,
            receipt=receipt,
            receipt_sha256=receipt_sha256,
        )

    assert error.value.code == code


def test_release_approval_binding_rejects_fingerprint_only_and_cross_request_replay(
    tmp_path,
):
    fixture = _release_approval_binding_fixture(tmp_path)
    with pytest.raises(EvidenceError) as fingerprint_error:
        _validate_release_approval_fixture(
            fixture,
            request_sha256="f" * 64,
        )
    assert fingerprint_error.value.code == "release_approval_request_raw_hash_mismatch"

    replay_request = copy.deepcopy(fixture.request)
    replay_request["release_id"] = "rca-production-20260713-approval-0002"
    replay_sha256 = release_gate_module._release_prepare_file_sha256(replay_request)
    replay_receipt = copy.deepcopy(fixture.receipt)
    replay_receipt["release_id"] = replay_request["release_id"]
    replay_receipt_sha256 = release_gate_module._release_prepare_file_sha256(
        replay_receipt
    )
    with pytest.raises(EvidenceError) as replay_error:
        _validate_release_approval_fixture(
            fixture,
            request=replay_request,
            request_sha256=replay_sha256,
            receipt=replay_receipt,
            receipt_sha256=replay_receipt_sha256,
        )
    assert replay_error.value.code == "release_approval_receipt_binding_mismatch"


def test_release_approval_binding_rejects_machine_and_final_schema_drift(tmp_path):
    fixture = _release_approval_binding_fixture(tmp_path)
    with pytest.raises(EvidenceError) as machine_error:
        _validate_release_approval_fixture(
            fixture,
            machine_identity={"source": "test_machine_id", "sha256": "8" * 64},
        )
    assert machine_error.value.code == "release_approval_identity_mismatch"

    with pytest.raises(EvidenceError) as schema_error:
        _validate_release_approval_fixture(
            fixture,
            final_schema="pnc_rca_release_prepare_manifest_legacy",
        )
    assert schema_error.value.code == "release_approval_final_manifest_schema_invalid"


def test_candidate_runtime_probe_uses_plist_interpreter_and_clean_environment(
    tmp_path,
):
    interpreter, working, expected_environment = _write_candidate_plists(tmp_path)
    captured = []

    def runner(command, **kwargs):
        captured.append((command, kwargs))
        if command[1:4] == ["-I", "-B", "-c"]:
            payload = _candidate_runtime_probe_payload(interpreter)
        else:
            payload = {"ok": True, "config": {}}
            if Path(command[1]).name == "pnc_rca_delivery_collector.py":
                payload["dependencies"] = {
                    "remote_css_parser": dict(
                        release_gate_module.EXPECTED_REMOTE_CSS_RUNTIME_DEPENDENCY
                    )
                }
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps(payload),
            stderr="",
        )

    result = check_candidate_runtime_dependencies(tmp_path, runner=runner)

    dependency_command, dependency_kwargs = captured[0]
    assert dependency_command[:4] == [str(interpreter), "-I", "-B", "-c"]
    assert dependency_kwargs["cwd"] == str(working)
    assert dependency_kwargs["env"] == expected_environment
    assert "PYTHONPATH" not in dependency_kwargs["env"]
    assert len(captured) == 5
    assert set(result["service_config_checks"]) == set(
        label for label, _script in release_gate_module.CANDIDATE_SERVICES.values()
    )
    assert result["isolated_probe"] is True
    assert len(result["launchd_config_sha256"]) == 64
    assert set(result["loaded_runtime"]["dependencies"]) == set(
        release_gate_module.RCA_LOADED_DEPENDENCIES
    )
    for label, process in result["service_processes"].items():
        assert set(process["loaded_runtime"]["dependencies"]) == set(
            release_gate_module.RCA_LOADED_DEPENDENCIES_BY_SERVICE[label]
        )
        assert process["loaded_runtime_sha256"] == _sha256_json(
            process["loaded_runtime"]
        )
    dispatcher_dependency = result["service_dependencies"][
        "local.pnc.rca-delivery-dispatcher"
    ]["feishu_outbound"]
    assert dispatcher_dependency["distribution"] == "lark-oapi"
    assert dispatcher_dependency["version"] == "1.5.3"
    assert dispatcher_dependency["dependency_install_attempted"] is False
    assert "aiohttp" not in json.dumps(result)


def test_candidate_runtime_probe_separates_source_and_installed_runtime(tmp_path):
    source = tmp_path / "source"
    runtime = tmp_path / "runtime"
    source.mkdir()
    runtime.mkdir()
    _write_candidate_plists(runtime)
    for filename in release_gate_module.CANDIDATE_SERVICES:
        (source / filename).write_bytes((runtime / filename).read_bytes())
    for relative in release_gate_module.RCA_RUNTIME_RELATIVE_FILES:
        source_path = source / relative
        runtime_path = runtime / relative
        source_path.parent.mkdir(parents=True, exist_ok=True)
        runtime_path.parent.mkdir(parents=True, exist_ok=True)
        if not source_path.exists():
            source_path.write_text("# source\n", encoding="utf-8")
        if not runtime_path.exists():
            runtime_path.write_bytes(source_path.read_bytes())
    interpreter = runtime / ".venv/bin/python"

    def runner(command, **_kwargs):
        if command[1:4] == ["-I", "-B", "-c"]:
            payload = _candidate_runtime_probe_payload(interpreter)
        else:
            payload = {"ok": True, "config": {}}
            if Path(command[1]).name == "pnc_rca_delivery_collector.py":
                payload["dependencies"] = {
                    "remote_css_parser": dict(
                        release_gate_module.EXPECTED_REMOTE_CSS_RUNTIME_DEPENDENCY
                    )
                }
        return subprocess.CompletedProcess(
            command, 0, stdout=json.dumps(payload), stderr=""
        )

    result = check_candidate_runtime_dependencies(
        source,
        runtime_root=runtime,
        runner=runner,
    )

    assert result["python_executable"] == str(interpreter)
    assert all(
        process["working_directory"] == str(runtime.resolve())
        for process in result["service_processes"].values()
    )


def test_candidate_runtime_config_override_is_check_only_and_redacted(tmp_path):
    interpreter, _working, expected_environment = _write_candidate_plists(tmp_path)
    secret = "candidate-only-password"
    config_environment = {
        "HERMES_RCA_KAFKA_USER": "rca",
        "HERMES_RCA_KAFKA_PASSWORD": secret,
    }
    captured = []

    def runner(command, **kwargs):
        captured.append((command, kwargs))
        if command[1:4] == ["-I", "-B", "-c"]:
            payload = _candidate_runtime_probe_payload(interpreter)
        else:
            payload = {"ok": True, "config": {}}
            if Path(command[1]).name == "pnc_rca_delivery_collector.py":
                payload["dependencies"] = {
                    "remote_css_parser": dict(
                        release_gate_module.EXPECTED_REMOTE_CSS_RUNTIME_DEPENDENCY
                    )
                }
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps(payload),
            stderr="",
        )

    result = check_candidate_runtime_dependencies(
        tmp_path,
        runner=runner,
        config_environment=config_environment,
        vm_worker_candidate_root="/mnt/tmp/release/worker-candidate",
    )

    assert captured[0][1]["env"] == expected_environment
    for _command, kwargs in captured[1:]:
        assert kwargs["env"] == {**expected_environment, **config_environment}
    collector_command = next(
        command
        for command, _kwargs in captured[1:]
        if Path(command[1]).name == "pnc_rca_delivery_collector.py"
    )
    assert collector_command[-2:] == [
        "--check-config-worker-root",
        "/mnt/tmp/release/worker-candidate",
    ]
    assert all(
        "--check-config-worker-root" not in command
        for command, _kwargs in captured[1:]
        if Path(command[1]).name != "pnc_rca_delivery_collector.py"
    )
    assert secret not in json.dumps(result)
    assert all(
        process["environment"] == expected_environment
        for process in result["service_processes"].values()
    )


@pytest.mark.parametrize(
    "config_environment",
    [
        {"HERMES_HOME": "/tmp/other-home"},
        {"PATH": "/tmp/bin"},
        {"HERMES_RCA_KAFKA_USER": "rca\x00forged"},
    ],
)
def test_candidate_runtime_config_override_rejects_unsafe_keys_and_values(
    tmp_path,
    config_environment,
):
    _write_candidate_plists(tmp_path)

    with pytest.raises(EvidenceError) as error:
        check_candidate_runtime_dependencies(
            tmp_path,
            runner=lambda *args, **kwargs: pytest.fail("probe must not run"),
            config_environment=config_environment,
        )

    assert error.value.code == "runtime_candidate_config_environment_invalid"


@pytest.mark.parametrize(
    "worker_root",
    ["relative/worker", "/mnt/tmp/../worker", "/"],
)
def test_candidate_runtime_rejects_unsafe_worker_candidate_root(
    tmp_path,
    worker_root,
):
    _write_candidate_plists(tmp_path)

    with pytest.raises(EvidenceError) as error:
        check_candidate_runtime_dependencies(
            tmp_path,
            runner=lambda *args, **kwargs: pytest.fail("probe must not run"),
            vm_worker_candidate_root=worker_root,
        )

    assert error.value.code == "runtime_candidate_vm_worker_root_invalid"


def _resident_loaded_runtime_fixture(tmp_path: Path, service_label: str):
    interpreter, _working, _environment = _write_candidate_plists(tmp_path)
    full = _candidate_runtime_probe_payload(interpreter)["loaded_runtime"]
    dependencies = release_gate_module.RCA_LOADED_DEPENDENCIES_BY_SERVICE[
        service_label
    ]
    projection = {
        **{key: full[key] for key in full if key != "dependencies"},
        "dependencies": {
            name: full["dependencies"][name] for name in sorted(dependencies)
        },
    }
    return interpreter, dependencies, projection


def test_resident_loaded_runtime_rejects_dependency_origin_outside_venv(tmp_path):
    label = "local.pnc.rca-kafka-consumer"
    interpreter, dependencies, projection = _resident_loaded_runtime_fixture(
        tmp_path, label
    )
    external = tmp_path / "outside.py"
    external.write_text("# injected\n", encoding="utf-8")
    projection["dependencies"]["kafka-python"] = {
        **projection["dependencies"]["kafka-python"],
        "origin": str(external.resolve(strict=True)),
        "sha256": hashlib.sha256(external.read_bytes()).hexdigest(),
    }

    with pytest.raises(EvidenceError) as error:
        LIVE_LOADED_RUNTIME_VERIFIER(
            projection,
            virtual_env=interpreter.parent.parent,
            expected_sys_executable=interpreter,
            expected_process_executable=interpreter,
            expected_dependencies=dependencies,
            artifact="kafka_consumer_health",
        )

    assert error.value.code == "kafka_consumer_health_loaded_dependency_origin_mismatch"


def test_resident_loaded_runtime_rehash_rejects_post_probe_dependency_drift(tmp_path):
    label = "local.pnc.rca-delivery-collector"
    interpreter, dependencies, projection = _resident_loaded_runtime_fixture(
        tmp_path, label
    )
    origin = Path(projection["dependencies"]["tinycss2"]["origin"])
    origin.write_text("# changed after probe\n", encoding="utf-8")

    with pytest.raises(EvidenceError) as error:
        LIVE_LOADED_RUNTIME_VERIFIER(
            projection,
            virtual_env=interpreter.parent.parent,
            expected_sys_executable=interpreter,
            expected_process_executable=interpreter,
            expected_dependencies=dependencies,
            artifact="delivery_collector_health",
        )

    assert error.value.code == "delivery_collector_health_loaded_runtime_changed"
@pytest.mark.parametrize(
    ("drift", "blocker"),
    [
        ("missing_lark", "runtime_dependency_missing"),
        ("lark_version", "runtime_dependency_version_mismatch"),
        ("topic_reply_api", "runtime_feishu_outbound_api_missing"),
        ("install_attempt", "runtime_feishu_outbound_api_missing"),
    ],
)
def test_candidate_runtime_probe_rejects_feishu_outbound_drift(
    tmp_path, drift, blocker
):
    interpreter, _working, _environment = _write_candidate_plists(tmp_path)

    def runner(command, **_kwargs):
        assert command[1:4] == ["-I", "-B", "-c"]
        payload = _candidate_runtime_probe_payload(interpreter)
        returncode = 0
        if drift == "missing_lark":
            payload = {"ok": False, "error_type": "ModuleNotFoundError"}
            returncode = 2
        elif drift == "lark_version":
            payload["versions"]["lark-oapi"] = "1.5.2"
        elif drift == "topic_reply_api":
            payload["feishu_outbound"]["apis"]["Client.im.v1.message.reply"] = False
        elif drift == "install_attempt":
            payload["feishu_outbound"]["dependency_install_attempted"] = True
        return subprocess.CompletedProcess(
            command,
            returncode,
            stdout=json.dumps(payload),
            stderr="",
        )

    with pytest.raises(EvidenceError) as error:
        check_candidate_runtime_dependencies(tmp_path, runner=runner)

    assert error.value.code == blocker


def test_candidate_runtime_probe_rejects_python_path_injection(tmp_path):
    _write_candidate_plists(
        tmp_path,
        kafka_environment={"PYTHONPATH": "/untracked/cache"},
    )

    with pytest.raises(EvidenceError) as error:
        check_candidate_runtime_dependencies(
            tmp_path,
            runner=lambda *args, **kwargs: pytest.fail("probe must not run"),
        )
    assert error.value.code == "runtime_candidate_python_path_injection"


@pytest.mark.parametrize(
    "filename",
    sorted(release_gate_module.CANDIDATE_SERVICES),
)
def test_candidate_runtime_requires_python_no_user_site(tmp_path, filename):
    _write_candidate_plists(tmp_path)
    plist_path = tmp_path / filename
    plist = release_gate_module.plistlib.loads(plist_path.read_bytes())
    plist["EnvironmentVariables"].pop("PYTHONNOUSERSITE")
    plist_path.write_bytes(release_gate_module.plistlib.dumps(plist))

    with pytest.raises(EvidenceError) as error:
        check_candidate_runtime_dependencies(
            tmp_path,
            runner=lambda *args, **kwargs: pytest.fail("probe must not run"),
        )

    assert error.value.code == "runtime_candidate_python_no_user_site_required"


@pytest.mark.parametrize(
    "filename",
    sorted(release_gate_module.CANDIDATE_SERVICES),
)
def test_candidate_runtime_requires_explicit_home(tmp_path, filename):
    _write_candidate_plists(tmp_path)
    plist_path = tmp_path / filename
    plist = release_gate_module.plistlib.loads(plist_path.read_bytes())
    plist["EnvironmentVariables"].pop("HOME")
    plist_path.write_bytes(release_gate_module.plistlib.dumps(plist))

    with pytest.raises(EvidenceError) as error:
        check_candidate_runtime_dependencies(
            tmp_path,
            runner=lambda *args, **kwargs: pytest.fail("probe must not run"),
        )

    assert error.value.code == "runtime_candidate_home_invalid"


@pytest.mark.parametrize(
    "filename",
    sorted(release_gate_module.CANDIDATE_SERVICES),
)
def test_candidate_runtime_requires_bytecode_disabled(tmp_path, filename):
    _write_candidate_plists(tmp_path)
    plist_path = tmp_path / filename
    plist = release_gate_module.plistlib.loads(plist_path.read_bytes())
    plist["EnvironmentVariables"].pop("PYTHONDONTWRITEBYTECODE")
    plist_path.write_bytes(release_gate_module.plistlib.dumps(plist))

    with pytest.raises(EvidenceError) as error:
        check_candidate_runtime_dependencies(
            tmp_path,
            runner=lambda *args, **kwargs: pytest.fail("probe must not run"),
        )

    assert error.value.code == "runtime_candidate_bytecode_write_forbidden"


@pytest.mark.parametrize(
    ("capture_environment", "blocker"),
    [
        (
            {"HERMES_G1Q3_ISSUE_CAPTURE_ENABLED": "true"},
            "runtime_candidate_issue_capture_enabled",
        ),
        (
            {
                "HERMES_G1Q3_ISSUE_CAPTURE_ENABLED": "false",
                "HERMES_G1Q3_ISSUE_CAPTURE_ROOT": (
                    "/mnt/minieye/pdcl/department/perception_test_team/"
                    "G1Q3_RCA/production cases"
                ),
            },
            "runtime_candidate_issue_capture_root_configured",
        ),
    ],
)
def test_candidate_runtime_rejects_issue_capture_write_environment(
    tmp_path, capture_environment, blocker
):
    environment = {
        "HOME": str(Path.home()),
        "HERMES_HOME": str(tmp_path / ".hermes"),
        "PATH": "/usr/bin:/bin",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1",
        "PYTHONUNBUFFERED": "1",
        **capture_environment,
    }
    _write_candidate_plists(tmp_path, kafka_environment=environment)

    with pytest.raises(EvidenceError) as error:
        check_candidate_runtime_dependencies(
            tmp_path,
            runner=lambda *args, **kwargs: pytest.fail("probe must not run"),
        )

    assert error.value.code == blocker


@pytest.mark.parametrize(
    ("capture_environment", "blocker"),
    [
        (
            {"HERMES_G1Q3_ISSUE_CAPTURE_ENABLED": "true"},
            "issue_capture_must_be_disabled",
        ),
        (
            {
                "HERMES_G1Q3_ISSUE_CAPTURE_ENABLED": "false",
                "HERMES_G1Q3_ISSUE_CAPTURE_ROOT": "/mnt/tmp/diagnostic-task",
            },
            "issue_capture_root_must_be_absent",
        ),
    ],
)
def test_production_env_file_cannot_enable_issue_capture(
    tmp_path, capture_environment, blocker
):
    consumer, dispatcher, settings = _gate(tmp_path, "production")
    environment = {
        "HERMES_RCA_LEGACY_AUTO_EXECUTION_DISABLED": "true",
        "HERMES_G1Q3_AUTO_DOWNLOAD_DAILY_QUOTA": "0",
        "G1Q3_GOVERNANCE_DOWNLOAD_ENABLED": "false",
        "HERMES_RCA_DELIVERY_COLLECTOR_ENABLED": "true",
        "HERMES_RCA_DELIVERY_DISPATCHER_ENABLED": "true",
        **capture_environment,
    }
    env_file = tmp_path / "capture-policy.env"
    env_file.write_text(
        "\n".join(f"{key}={value}" for key, value in environment.items()) + "\n",
        encoding="utf-8",
    )
    cutover = load_cutover_config(env_file, environment={})

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=cutover,
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


def test_cutover_config_reads_canonical_manual_operator_policy_and_redacts_ids():
    config = CutoverConfig.from_env({
        "HERMES_RCA_MANUAL_OPERATOR_ENABLED": "true",
        "HERMES_RCA_MANUAL_OPERATOR_USER_IDS": "ou_second,ou_first,ou_second",
        "HERMES_RCA_MANUAL_OPERATOR_RATE_LIMIT": "3",
        "HERMES_RCA_MANUAL_OPERATOR_RATE_WINDOW_SECONDS": "600",
        "HERMES_RCA_MANUAL_DEBUG_ENABLED": "false",
        "HERMES_RCA_MANUAL_DEBUG_USER_IDS": "ou_legacy",
    })

    assert config.manual_operator_enabled is True
    assert config.manual_operator_user_ids == ("ou_first", "ou_second")
    assert config.manual_operator_rate_limit == 3
    assert config.manual_operator_rate_window_seconds == 600
    public = config.public_dict()
    assert public["manual_operator_enabled"] is True
    assert public["manual_operator_user_count"] == 2
    assert public["manual_operator_user_ids_sha256"] == _sha256_json([
        "ou_first",
        "ou_second",
    ])
    assert "ou_first" not in json.dumps(public)
    assert "ou_second" not in json.dumps(public)
    assert "ou_legacy" not in json.dumps(public)
    legacy_only = CutoverConfig.from_env({
        "HERMES_RCA_MANUAL_DEBUG_ENABLED": "true",
        "HERMES_RCA_MANUAL_DEBUG_USER_IDS": "ou_legacy",
    })
    assert legacy_only.manual_operator_enabled is None
    assert legacy_only.manual_operator_user_ids == ()


@pytest.mark.parametrize("value", ["0", "-1", "not-an-integer"])
def test_cutover_config_rejects_invalid_manual_operator_rate_policy(value):
    with pytest.raises(ValueError, match="must be a positive integer"):
        CutoverConfig.from_env({
            "HERMES_RCA_MANUAL_OPERATOR_RATE_LIMIT": value,
        })


def test_cutover_config_rejects_non_feishu_manual_operator_identity():
    with pytest.raises(ValueError, match="must contain Feishu open IDs"):
        CutoverConfig.from_env({
            "HERMES_RCA_MANUAL_OPERATOR_USER_IDS": "user@example.invalid",
        })


@pytest.mark.parametrize(
    ("field", "replacement", "blocker"),
    [
        (
            "interpreter",
            "alternate/.venv/bin/python",
            "runtime_candidate_interpreter_mismatch",
        ),
        (
            "script",
            "alternate/scripts/pnc_rca_kafka_consumer.py",
            "runtime_candidate_script_root_mismatch",
        ),
        (
            "working_directory",
            "alternate",
            "runtime_candidate_working_directory_mismatch",
        ),
    ],
)
def test_candidate_runtime_probe_rejects_cross_tree_programs(
    tmp_path, field, replacement, blocker
):
    _write_candidate_plists(tmp_path)
    alternate = tmp_path / "alternate"
    target = tmp_path / replacement
    if field == "working_directory":
        target.mkdir(parents=True)
    else:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("", encoding="utf-8")
    plist_path = tmp_path / "local.pnc.rca-kafka-consumer.candidate.plist"
    plist = release_gate_module.plistlib.loads(plist_path.read_bytes())
    if field == "interpreter":
        plist["ProgramArguments"][0] = str(target)
    elif field == "script":
        plist["ProgramArguments"][1] = str(target)
    else:
        plist["WorkingDirectory"] = str(target)
    plist_path.write_bytes(release_gate_module.plistlib.dumps(plist))

    with pytest.raises(EvidenceError) as error:
        check_candidate_runtime_dependencies(
            tmp_path,
            runner=lambda *args, **kwargs: pytest.fail("probe must not run"),
        )

    assert error.value.code == blocker


def test_release_gate_blocks_when_candidate_runtime_dependencies_are_missing(
    tmp_path, monkeypatch
):
    consumer, dispatcher, settings = _gate(tmp_path, "shadow")

    def missing_runtime(repo_root, *, runtime_root=None):
        del repo_root, runtime_root
        raise EvidenceError("runtime_dependency_missing")

    monkeypatch.setattr(
        release_gate_module, "check_candidate_runtime_dependencies", missing_runtime
    )
    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("shadow"),
        now=NOW,
    )

    assert report["ok"] is False
    assert "runtime_dependency_missing" in report["blockers"]


def test_release_gate_probes_the_installed_candidate_runtime_root(
    tmp_path, monkeypatch
):
    consumer, dispatcher, settings = _gate(tmp_path, "shadow")
    installed_root = tmp_path / "installed-runtime"
    runtime_detail = release_gate_module.check_candidate_runtime_dependencies(
        settings.host_repo_root
    )
    observed = []

    def probe(repo_root, *, runtime_root=None):
        observed.append((repo_root, runtime_root))
        return runtime_detail

    monkeypatch.setattr(
        release_gate_module, "check_candidate_runtime_dependencies", probe
    )
    settings = replace(settings, candidate_runtime_root=installed_root)

    evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("shadow"),
        now=NOW,
    )

    assert observed == [(installed_root, installed_root)]


@pytest.mark.parametrize(
    ("mode", "consumer_updates", "dispatcher_updates", "blocker"),
    [
        (
            "shadow",
            {},
            {
                "HERMES_RCA_OUTBOX_DISPATCH_ENABLED": "true",
                "HERMES_RCA_OUTBOX_STORAGE_ADMISSION_ENABLED": "true",
                "HERMES_RCA_OUTBOX_DERIVED_CAPACITY_RESERVATION_ENABLED": "true",
                "HERMES_RCA_OUTBOX_DELIVERY_BACKPRESSURE_ENABLED": "true",
            },
            "dispatcher_mode_switch_mismatch",
        ),
        (
            "canary",
            {"HERMES_RCA_KAFKA_SUBMIT_ENABLED": "false"},
            {},
            "consumer_mode_switch_mismatch",
        ),
        (
            "canary",
            {},
            {"HERMES_RCA_OUTBOX_BATCH_SIZE": "2"},
            "dispatcher_activation_batch_size_mismatch",
        ),
        (
            "production",
            {},
            {"HERMES_RCA_OUTBOX_ALLOW_FEISHU_WRITEBACK": "true"},
            "dispatcher_writeback_must_be_disabled",
        ),
    ],
)
def test_mode_switches_fail_closed(
    tmp_path, mode, consumer_updates, dispatcher_updates, blocker
):
    consumer, dispatcher, settings = _gate(tmp_path, mode)
    consumer_env = _consumer_env(tmp_path, mode)
    consumer_env.update(consumer_updates)
    dispatcher_env = _dispatcher_env(tmp_path, mode)
    dispatcher_env.update(dispatcher_updates)
    consumer = ConsumerConfig.from_env(consumer_env, hermes_home=tmp_path)
    dispatcher = DispatcherConfig.from_env(dispatcher_env, hermes_home=tmp_path)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover(mode),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


def test_activation_modes_reject_disabled_consumer_dispatcher_and_gateway_flags(
    tmp_path,
):
    consumer, dispatcher = _configs(tmp_path, "preproduction")
    assert "consumer_activation_mode_mismatch" in (
        release_gate_module._check_consumer_config(
            replace(consumer, activation_required=False),
            mode="preproduction",
            expected_topic=TOPIC,
            expected_rule_version=RULE,
        )
    )
    assert "dispatcher_activation_mode_mismatch" in (
        release_gate_module._check_dispatcher_config(
            replace(dispatcher, activation_required=False),
            mode="preproduction",
            target_cases_per_day=200,
        )
    )
    runtime = {
        "service_configs": {
            "local.pnc.rca-kafka-consumer": {
                **consumer.public_dict(),
                "policy": consumer.policy.to_dict(),
            },
            "local.pnc.rca-outbox-dispatcher": dispatcher.public_dict(),
            "local.pnc.rca-delivery-collector": {
                "enabled": True,
                "activation_required": True,
                "capacity_sample_enabled": True,
                "control_db_path": str(dispatcher.control_db_path),
                "external_writes": False,
                "lease_seconds": 180,
                "artifact_read_timeout_seconds": 25,
                "batch_size": 20,
                "backfill_batch_size": 1000,
                "health_path": str(tmp_path / "collector-health.json"),
                "health_max_age_seconds": 60,
            },
            "local.pnc.rca-delivery-dispatcher": {
                "enabled": True,
                "activation_required": True,
                "control_db_path": str(dispatcher.control_db_path),
                "external_writes": True,
                "lease_seconds": 120,
                "max_external_boundary_timeout_seconds": 72,
                "lease_boundary_margin_seconds": 15,
                "effect_lease_keeper_enabled": True,
                "effect_lease_renew_interval_seconds": 10,
                "batch_size": 10,
                "health_path": str(tmp_path / "delivery-health.json"),
                "health_max_age_seconds": 60,
            },
        }
    }
    with pytest.raises(EvidenceError, match="runtime_candidate_activation_mode_mismatch"):
        release_gate_module._check_candidate_service_configs(
            runtime,
            consumer=consumer,
            dispatcher=dispatcher,
            cutover=replace(_cutover("preproduction"), activation_required=False),
            mode="preproduction",
        )
    for label in (
        "local.pnc.rca-delivery-collector",
        "local.pnc.rca-delivery-dispatcher",
    ):
        changed = copy.deepcopy(runtime)
        changed["service_configs"][label]["activation_required"] = False
        with pytest.raises(
            EvidenceError,
            match="runtime_candidate_activation_mode_mismatch",
        ):
            release_gate_module._check_candidate_service_configs(
                changed,
                consumer=consumer,
                dispatcher=dispatcher,
                cutover=_cutover("preproduction"),
                mode="preproduction",
            )


def test_release_gate_rejects_legacy_storage_reservation_in_canary(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=replace(dispatcher, storage_reservation_enabled=True),
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert "dispatcher_legacy_storage_reservation_enabled" in report["blockers"]


def test_release_gate_requires_delivery_backpressure_in_canary(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=replace(dispatcher, delivery_backpressure_enabled=False),
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert "dispatcher_delivery_backpressure_mode_mismatch" in report["blockers"]


def test_candidate_delivery_services_must_share_the_outbox_control_database(
    tmp_path, monkeypatch
):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    original = release_gate_module.check_candidate_runtime_dependencies

    def mismatched(repo_root):
        detail = json.loads(json.dumps(original(repo_root)))
        detail["service_configs"]["local.pnc.rca-delivery-collector"][
            "control_db_path"
        ] = str(tmp_path / "wrong.sqlite3")
        return detail

    monkeypatch.setattr(
        release_gate_module,
        "check_candidate_runtime_dependencies",
        mismatched,
    )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert "runtime_candidate_delivery_control_db_mismatch" in report["blockers"]


@pytest.mark.parametrize(
    ("label", "artifact"),
    [
        ("local.pnc.rca-kafka-consumer", "kafka_consumer_health"),
        ("local.pnc.rca-outbox-dispatcher", "outbox_dispatcher_health"),
    ],
)
@pytest.mark.parametrize(
    ("mutation", "blocker_suffix"),
    [
        ("missing", "missing"),
        ("old_schema", "schema_mismatch"),
        ("stale", "stale"),
        ("config_drift", "config_mismatch"),
    ],
)
def test_intake_service_health_v2_is_mandatory_and_bound_to_candidate(
    tmp_path,
    monkeypatch,
    label,
    artifact,
    mutation,
    blocker_suffix,
):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    original = release_gate_module.check_candidate_runtime_dependencies

    def unhealthy(repo_root):
        detail = original(repo_root)
        path = Path(detail["service_configs"][label]["health_path"])
        if mutation == "missing":
            path.unlink()
            return detail
        body = json.loads(path.read_text(encoding="utf-8"))
        if mutation == "old_schema":
            body["schema_version"] = body["schema_version"].replace("_v2", "_v1")
        elif mutation == "stale":
            body["heartbeat_at"] = (NOW - timedelta(minutes=5)).isoformat()
        else:
            body["config"] = dict(body["config"])
            drift_field = "topic" if "kafka" in label else "service_id"
            body["config"][drift_field] = "forged-config"
        _write_json(path, body)
        return detail

    monkeypatch.setattr(
        release_gate_module,
        "check_candidate_runtime_dependencies",
        unhealthy,
    )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert f"{artifact}_{blocker_suffix}" in report["blockers"]


def test_outbox_fresh_liveness_cannot_mask_stale_readiness(
    tmp_path, monkeypatch
):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    original = release_gate_module.check_candidate_runtime_dependencies

    def stale_readiness(repo_root):
        detail = original(repo_root)
        config = detail["service_configs"]["local.pnc.rca-outbox-dispatcher"]
        path = Path(config["health_path"])
        body = json.loads(path.read_text(encoding="utf-8"))
        stale = (NOW - timedelta(minutes=5)).isoformat()
        body["readiness_observed_at"] = stale
        body["readiness"]["observed_at"] = stale
        body["liveness"]["readiness_observed_at"] = stale
        _write_json(path, body)
        return detail

    monkeypatch.setattr(
        release_gate_module,
        "check_candidate_runtime_dependencies",
        stale_readiness,
    )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert "outbox_dispatcher_health_readiness_stale" in report["blockers"]


@pytest.mark.parametrize(
    ("mutation", "blocker"),
    [
        (
            lambda boundary: boundary.update(last_snapshot=None),
            "outbox_dispatcher_health_delivery_backpressure_not_ready",
        ),
        (
            lambda boundary: boundary.update(enabled=False),
            "outbox_dispatcher_health_delivery_backpressure_not_ready",
        ),
        (
            lambda boundary: boundary.update(active=True),
            "outbox_dispatcher_health_delivery_backpressure_not_ready",
        ),
        (
            lambda boundary: boundary.update(
                last_error={"code": "delivery_backpressure_unavailable"}
            ),
            "outbox_dispatcher_health_delivery_backpressure_not_ready",
        ),
        (
            lambda boundary: boundary["last_snapshot"][
                "delivery_dispatcher_circuit"
            ].update(state="open"),
            "outbox_dispatcher_health_delivery_backpressure_circuit_open",
        ),
        (
            lambda boundary: boundary["last_snapshot"]["delivery_dispatcher_circuits"][
                "feishu_thread_reply"
            ].update(state="open"),
            "outbox_dispatcher_health_delivery_backpressure_circuit_open",
        ),
        (
            lambda boundary: boundary["last_snapshot"].update(unresolved_work=1),
            "outbox_dispatcher_health_delivery_backpressure_contract_invalid",
        ),
        (
            lambda boundary: boundary["last_snapshot"].update(
                delivery_outcome_slo=_outcome_slo(healthy=False)
            ),
            "outbox_dispatcher_health_delivery_outcome_slo_failed",
        ),
    ],
)
def test_outbox_health_requires_a_closed_coherent_delivery_backpressure_snapshot(
    tmp_path, monkeypatch, mutation, blocker
):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    original = release_gate_module.check_candidate_runtime_dependencies

    def unhealthy(repo_root):
        detail = original(repo_root)
        config = detail["service_configs"]["local.pnc.rca-outbox-dispatcher"]
        path = Path(config["health_path"])
        body = json.loads(path.read_text(encoding="utf-8"))
        mutation(body["delivery_backpressure"])
        _write_json(path, body)
        return detail

    monkeypatch.setattr(
        release_gate_module,
        "check_candidate_runtime_dependencies",
        unhealthy,
    )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


def _rehash_worker_dispatch(body: dict) -> None:
    attestation = body["vm"]["execution_attestation"]
    receipt = {
        "schema_version": "g1q3_rca_worker_dispatch_receipt_v1",
        "task_id": attestation["task_id"],
        "run_id": attestation["run_id"],
        "argv": attestation["argv"],
        "cwd": attestation["cwd"],
        "dispatched_at": attestation["dispatched_at"],
        "process_started_at": attestation["process_started_at"],
        "worker_pid": attestation["worker_pid"],
    }
    digest = _sha256_json(receipt)
    attestation["dispatch_receipt_sha256"] = digest
    body["vm"]["dispatch_receipt_sha256"] = digest


def _mutate_worker_run_id_with_valid_dispatch_hash(body: dict) -> None:
    body["vm"]["execution_attestation"]["run_id"] = "forged-run-id"
    _rehash_worker_dispatch(body)


def _rehash_worker_result(body: dict) -> None:
    wrapper = body["vm"]["worker_result"]
    wrapper["sha256"] = _sha256_json(wrapper["receipt"])


def _mutate_embedded_worker_attestation(body: dict) -> None:
    body["vm"]["worker_result"]["receipt"]["result"]["execution_attestation"][
        "run_id"
    ] = "worker-run-from-another-execution"
    _rehash_worker_result(body)


def _mutate_worker_result_state(body: dict) -> None:
    body["vm"]["worker_result"]["receipt"].update(state="failed", exit_code=1)
    _rehash_worker_result(body)


def _mutate_worker_contract_hash(body: dict) -> None:
    body["vm"]["worker_result"]["receipt"]["result"]["rca_contract_sha256"] = "f" * 64
    _rehash_worker_result(body)


def _mutate_service_mount_identity(body: dict) -> None:
    receipt = body["vm"]["service_result"]["receipt"]
    receipt["mount_evidence"]["device_id"] = 654321
    receipt["request_storage"]["mount_evidence"]["device_id"] = 654321


@pytest.mark.parametrize(
    ("label", "field", "value", "blocker"),
    [
        (
            "local.pnc.rca-delivery-collector",
            "capacity_sample_enabled",
            False,
            "runtime_candidate_delivery_capacity_sampling_mode_mismatch",
        ),
        (
            "local.pnc.rca-delivery-collector",
            "health_max_age_seconds",
            61,
            "runtime_candidate_delivery_collector_health_max_age_too_large",
        ),
        (
            "local.pnc.rca-delivery-collector",
            "batch_size",
            51,
            "runtime_candidate_delivery_collector_batch_too_large",
        ),
        (
            "local.pnc.rca-delivery-collector",
            "backfill_batch_size",
            2001,
            "runtime_candidate_delivery_collector_backfill_batch_too_large",
        ),
        (
            "local.pnc.rca-delivery-dispatcher",
            "batch_size",
            21,
            "runtime_candidate_delivery_dispatcher_batch_too_large",
        ),
        (
            "local.pnc.rca-delivery-dispatcher",
            "effect_lease_keeper_enabled",
            False,
            "runtime_candidate_delivery_dispatcher_lease_keeper_disabled",
        ),
        (
            "local.pnc.rca-delivery-dispatcher",
            "effect_lease_renew_interval_seconds",
            16,
            "runtime_candidate_delivery_dispatcher_lease_renew_interval_invalid",
        ),
    ],
)
def test_candidate_delivery_service_config_limits_fail_closed(
    tmp_path, monkeypatch, label, field, value, blocker
):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    original = release_gate_module.check_candidate_runtime_dependencies

    def oversized(repo_root):
        detail = original(repo_root)
        detail["service_configs"][label][field] = value
        return detail

    monkeypatch.setattr(
        release_gate_module,
        "check_candidate_runtime_dependencies",
        oversized,
    )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


@pytest.mark.parametrize(
    ("mutation", "blocker"),
    [
        (
            lambda body: body.update(
                updated_at=(NOW - timedelta(minutes=5)).isoformat()
            ),
            "delivery_collector_health_stale",
        ),
        (
            lambda body: body["store"].update(business_ready=False, ok=False),
            "delivery_collector_health_business_not_ready",
        ),
        (
            lambda body: body["store"].update(
                schema_version="pnc_rca_delivery_store_v4"
            ),
            "delivery_collector_health_store_schema_mismatch",
        ),
        (
            lambda body: body["store"].update(
                delivery_outcome_slo=_outcome_slo(healthy=False)
            ),
            "delivery_collector_health_delivery_outcome_slo_failed",
        ),
        (
            lambda body: body["dependencies"]["remote_css_parser"].update(
                observed_at=(NOW - timedelta(minutes=5)).isoformat()
            ),
            "delivery_collector_health_dependency_receipt_stale",
        ),
    ],
)
def test_candidate_delivery_health_is_fresh_and_business_ready(
    tmp_path, monkeypatch, mutation, blocker
):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    original = release_gate_module.check_candidate_runtime_dependencies

    def unhealthy(repo_root):
        detail = original(repo_root)
        path = Path(
            detail["service_configs"]["local.pnc.rca-delivery-collector"]["health_path"]
        )
        body = json.loads(path.read_text(encoding="utf-8"))
        mutation(body)
        _write_json(path, body)
        return detail

    monkeypatch.setattr(
        release_gate_module,
        "check_candidate_runtime_dependencies",
        unhealthy,
    )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


def test_candidate_delivery_health_rejects_old_schema_and_forged_identity(
    tmp_path, monkeypatch
):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    original = release_gate_module.check_candidate_runtime_dependencies

    def forged(repo_root):
        detail = original(repo_root)
        path = Path(
            detail["service_configs"]["local.pnc.rca-delivery-collector"]["health_path"]
        )
        body = json.loads(path.read_text(encoding="utf-8"))
        body["schema_version"] = "pnc_rca_delivery_collector_health_v1"
        body["runtime_identity"]["script_sha256"] = "f" * 64
        _write_json(path, body)
        return detail

    monkeypatch.setattr(
        release_gate_module,
        "check_candidate_runtime_dependencies",
        forged,
    )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert "delivery_collector_health_schema_mismatch" in report["blockers"]


def test_candidate_delivery_health_rejects_static_forged_script_identity(
    tmp_path, monkeypatch
):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    original = release_gate_module.check_candidate_runtime_dependencies

    def forged(repo_root):
        detail = original(repo_root)
        path = Path(
            detail["service_configs"]["local.pnc.rca-delivery-collector"]["health_path"]
        )
        body = json.loads(path.read_text(encoding="utf-8"))
        body["runtime_identity"]["script_sha256"] = "f" * 64
        _write_json(path, body)
        return detail

    monkeypatch.setattr(
        release_gate_module,
        "check_candidate_runtime_dependencies",
        forged,
    )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert "delivery_collector_health_runtime_identity_mismatch" in report["blockers"]


def test_candidate_delivery_health_rejects_shared_runtime_module_hash_drift(
    tmp_path, monkeypatch
):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    original = release_gate_module.check_candidate_runtime_dependencies

    def drifted(repo_root):
        detail = original(repo_root)
        for process in detail["service_processes"].values():
            hashes = process["runtime_file_sha256"]
            hashes["gateway/pnc_rca_delivery_store.py"] = "f" * 64
            process["runtime_files_sha256"] = _sha256_json(hashes)
        return detail

    monkeypatch.setattr(
        release_gate_module,
        "check_candidate_runtime_dependencies",
        drifted,
    )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert "kafka_consumer_health_runtime_identity_mismatch" in report["blockers"]


@pytest.mark.parametrize(
    "relative",
    [
        "gateway/pnc_issue_context.py",
        "gateway/pnc_rca_frame_reference.py",
        "gateway/pnc_issue_capture.py",
        "gateway/feishu_task_card.py",
        "gateway/pnc_rca_stage_lineage.py",
        "scripts/pnc_g1q3_truth.py",
    ],
)
def test_resident_runtime_identity_closes_over_live_business_dependencies(
    tmp_path, relative
):
    assert relative in release_gate_module.RCA_RUNTIME_RELATIVE_FILES
    for runtime_relative in release_gate_module.RCA_RUNTIME_RELATIVE_FILES:
        path = tmp_path / runtime_relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"# {runtime_relative}\n", encoding="utf-8")
    baseline = release_gate_module.rca_runtime_files_sha256(tmp_path)

    (tmp_path / relative).write_text("# drifted live dependency\n", encoding="utf-8")

    assert release_gate_module.rca_runtime_files_sha256(tmp_path) != baseline


def test_frame_reference_is_closed_over_gateway_and_release_bom():
    relative = "gateway/pnc_rca_frame_reference.py"

    assert relative in release_gate_module.GATEWAY_RCA_RUNTIME_RELATIVE_FILES
    assert relative in release_gate_module._required_critical_files(
        release_gate_module.REPO_ROOT
    )


@pytest.mark.parametrize(
    "relative",
    [
        "scripts/pnc_rca_canary_finalize.py",
        "scripts/pnc_rca_cutover_adapter.py",
        "scripts/pnc_rca_cutover_execute.py",
        "scripts/pnc_rca_cutover_guard.py",
        "scripts/pnc_rca_cutover_live.py",
        "scripts/pnc_rca_feishu_ingress_hold.py",
        "scripts/pnc_rca_postinstall_activation.py",
        "scripts/pnc_rca_production_cutover.py",
        "scripts/pnc_rca_vm_promotion.py",
        "scripts/pnc_rca_vm_promotion_remote.py",
    ],
)
def test_production_control_plane_is_closed_over_release_bom(relative):
    assert relative in release_gate_module._required_critical_files(
        release_gate_module.REPO_ROOT
    )


def test_runtime_snapshot_rejects_parent_directory_symlink(tmp_path):
    root = tmp_path / "candidate"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (outside / "run.py").write_text("outside\n", encoding="utf-8")
    (root / "gateway").symlink_to(outside, target_is_directory=True)

    with pytest.raises((FileNotFoundError, OSError)):
        release_gate_module.runtime_file_snapshot(root, ("gateway/run.py",))


def test_health_freshness_hard_cap_cannot_be_relaxed_to_one_year(tmp_path, monkeypatch):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    original = release_gate_module.check_candidate_runtime_dependencies

    def relaxed(repo_root):
        detail = original(repo_root)
        config = detail["service_configs"]["local.pnc.rca-delivery-collector"]
        config["health_max_age_seconds"] = 365 * 24 * 60 * 60
        path = Path(config["health_path"])
        body = json.loads(path.read_text(encoding="utf-8"))
        body["updated_at"] = (NOW - timedelta(minutes=5)).isoformat()
        _write_json(path, body)
        return detail

    monkeypatch.setattr(
        release_gate_module,
        "check_candidate_runtime_dependencies",
        relaxed,
    )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert (
        "runtime_candidate_delivery_collector_health_max_age_too_large"
        in report["blockers"]
    )
    assert "delivery_collector_health_stale" in report["blockers"]


def test_static_health_forgery_is_rejected_by_live_process_create_time():
    executable = str(Path(release_gate_module.sys.executable).resolve())
    cwd = str(Path.cwd().resolve())
    identity = {
        "service_label": "local.pnc.rca-delivery-collector",
        "pid": 1234,
        "process_create_time": 1_783_650_000.0,
        "boot_time": 1_783_000_000.0,
        "executable": executable,
        "script": "/candidate/collector.py",
        "cwd": cwd,
        "script_sha256": "a" * 64,
        "public_config_sha256": "b" * 64,
    }
    candidate = {"program_arguments": [executable, "/candidate/collector.py"]}

    class FakeProcess:
        def create_time(self):
            return 1_783_650_001.0

        def exe(self):
            return executable

        def cwd(self):
            return cwd

        def cmdline(self):
            return candidate["program_arguments"]

    with pytest.raises(EvidenceError) as error:
        LIVE_PROCESS_VERIFIER(
            identity,
            candidate,
            artifact="delivery_collector_health",
            process_factory=lambda pid: FakeProcess(),
            boot_time_reader=lambda: 1_783_000_000.0,
            launchctl_runner=lambda *args, **kwargs: pytest.fail(
                "launchctl must not run after process identity mismatch"
            ),
        )

    assert error.value.code == "delivery_collector_health_process_create_time_mismatch"


def test_live_health_requires_launchctl_pid_to_match():
    executable = str(Path(release_gate_module.sys.executable).resolve())
    cwd = str(Path.cwd().resolve())
    identity = {
        "service_label": "local.pnc.rca-delivery-dispatcher",
        "pid": 1234,
        "process_create_time": 100.0,
        "boot_time": 10.0,
        "executable": executable,
        "script": "/candidate/dispatcher.py",
        "cwd": cwd,
        "script_sha256": "a" * 64,
        "public_config_sha256": "b" * 64,
    }
    candidate = {"program_arguments": [executable, "/candidate/dispatcher.py"]}

    class FakeProcess:
        def create_time(self):
            return 100.0

        def exe(self):
            return executable

        def cwd(self):
            return cwd

        def cmdline(self):
            return candidate["program_arguments"]

    def launchctl(command, **_kwargs):
        return subprocess.CompletedProcess(
            command,
            0,
            stdout="state = running\npid = 4321\n",
            stderr="",
        )

    with pytest.raises(EvidenceError) as error:
        LIVE_PROCESS_VERIFIER(
            identity,
            candidate,
            artifact="delivery_dispatcher_health",
            process_factory=lambda pid: FakeProcess(),
            boot_time_reader=lambda: 10.0,
            launchctl_runner=launchctl,
        )

    assert error.value.code == "delivery_dispatcher_health_launchctl_pid_mismatch"


def test_live_process_verifier_accepts_real_venv_python_runtime_executable():
    command = [
        release_gate_module.sys.executable,
        "-c",
        "import time; time.sleep(30)",
    ]
    child = subprocess.Popen(command)
    try:
        process = release_gate_module.psutil.Process(child.pid)
        for _ in range(50):
            if process.cmdline():
                break
            time.sleep(0.01)
        runtime_executable = str(Path(process.exe()).resolve(strict=True))
        identity = {
            "service_label": "local.pnc.rca-delivery-collector",
            "pid": child.pid,
            "process_create_time": process.create_time(),
            "boot_time": release_gate_module.psutil.boot_time(),
            "executable": runtime_executable,
            "script": "/candidate/collector.py",
            "cwd": str(Path(process.cwd()).resolve(strict=True)),
            "script_sha256": "a" * 64,
            "runtime_files_sha256": "b" * 64,
            "public_config_sha256": "c" * 64,
        }
        candidate = {
            "interpreter": str(Path(command[0]).resolve(strict=True)),
            "runtime_executable": runtime_executable,
            "program_arguments": command,
        }

        def launchctl(args, **_kwargs):
            return subprocess.CompletedProcess(
                args,
                0,
                stdout=f"state = running\npid = {child.pid}\n",
                stderr="",
            )

        result = LIVE_PROCESS_VERIFIER(
            identity,
            candidate,
            artifact="delivery_collector_health",
            launchctl_runner=launchctl,
        )

        assert result["pid"] == child.pid
        assert result["executable"] == runtime_executable
    finally:
        child.terminate()
        child.wait(timeout=5)


def _gateway_runtime_fixture(tmp_path: Path):
    root = tmp_path / "gateway-candidate"
    for relative in release_gate_module.GATEWAY_RCA_RUNTIME_RELATIVE_FILES:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"fixture: {relative}\n", encoding="utf-8")
    cutover = replace(
        _cutover("production"),
        manual_intake_enabled=True,
        manual_chat_ids=(release_gate_module.G1Q3_RCA_GROUP_ID,),
        manual_operator_enabled=False,
    )
    consumer, _dispatcher = _configs(tmp_path, "production")
    script = (root / "gateway" / "run.py").resolve(strict=True)
    virtual_env = root / ".venv"
    (virtual_env / "bin").mkdir(parents=True)
    raw_interpreter = virtual_env / "bin" / "python"
    raw_interpreter.symlink_to(release_gate_module.sys.executable)
    executable = str(Path(release_gate_module.psutil.Process().exe()).resolve(strict=True))
    dependency_origins = {}
    dependency_runtime = {}
    for distribution, module_name in (
        release_gate_module.GATEWAY_LOADED_DEPENDENCIES.items()
    ):
        origin = virtual_env / "lib" / f"{module_name}.py"
        origin.parent.mkdir(parents=True, exist_ok=True)
        origin.write_text(f"# {distribution}\n", encoding="utf-8")
        dependency_origins[module_name] = str(origin.resolve(strict=True))
        dependency_runtime[distribution] = {
            "module": module_name,
            "origin": str(origin.resolve(strict=True)),
            "sha256": hashlib.sha256(origin.read_bytes()).hexdigest(),
            "version": release_gate_module.EXPECTED_GATEWAY_RUNTIME_DEPENDENCY_VERSIONS[
                distribution
            ],
        }
    loaded_runtime_sha256 = _sha256_json({
        "sys_executable": str(raw_interpreter.resolve(strict=True)),
        "sys_executable_sha256": hashlib.sha256(
            raw_interpreter.resolve(strict=True).read_bytes()
        ).hexdigest(),
        "process_executable": executable,
        "process_executable_sha256": hashlib.sha256(
            Path(executable).read_bytes()
        ).hexdigest(),
        "dependencies": dependency_runtime,
    })
    identity = {
        "service_label": "ai.hermes.gateway",
        "pid": 41000,
        "process_create_time": 1_783_650_000.0,
        "boot_time": 1_783_000_000.0,
        "executable": executable,
        "script": str(script),
        "cwd": str(root.resolve(strict=True)),
        "script_sha256": hashlib.sha256(script.read_bytes()).hexdigest(),
        "runtime_files_sha256": (
            release_gate_module.gateway_rca_runtime_files_sha256(root)
        ),
        "public_config_sha256": _sha256_json(
            release_gate_module._gateway_manual_runtime_public_config(
                cutover,
                consumer,
            )
        ),
        "loaded_runtime_sha256": loaded_runtime_sha256,
    }
    plist_path = tmp_path / "ai.hermes.gateway.plist"
    plist_body = {
        "Label": "ai.hermes.gateway",
        "ProgramArguments": [
            str(raw_interpreter),
            "-m",
            "hermes_cli.main",
            "gateway",
            "run",
            "--replace",
        ],
        "WorkingDirectory": str(root.resolve(strict=True)),
        "EnvironmentVariables": {
            "VIRTUAL_ENV": str(virtual_env),
            "PATH": f"{virtual_env / 'bin'}:/usr/bin:/bin",
        },
        "ExitTimeOut": 30,
        "KeepAlive": {"SuccessfulExit": False},
        "RunAtLoad": True,
        "ProcessType": "Background",
        "StandardErrorPath": str(
            Path.home() / ".hermes" / "logs" / "gateway.error.log"
        ),
        "StandardOutPath": str(
            Path.home() / ".hermes" / "logs" / "gateway.log"
        ),
        "ThrottleInterval": 10,
        "Umask": 0o077,
    }
    plist_raw = release_gate_module.plistlib.dumps(plist_body)
    plist_path.write_bytes(plist_raw)
    (root / release_gate_module.GATEWAY_CANDIDATE_PLIST_RELATIVE_PATH).write_bytes(
        plist_raw
    )
    probe = lambda *_args, **_kwargs: {
        "sys_executable": str(raw_interpreter.resolve(strict=True)),
        "process_executable": executable,
        "module_origins_sha256": "9" * 64,
        "module_origins": dependency_origins,
        "dependency_versions": dict(
            release_gate_module.EXPECTED_GATEWAY_RUNTIME_DEPENDENCY_VERSIONS
        ),
    }
    return root, cutover, consumer, identity, plist_path, probe


def test_gateway_runtime_verifier_binds_candidate_plist_and_process(tmp_path):
    root, cutover, consumer, identity, plist_path, probe = (
        _gateway_runtime_fixture(tmp_path)
    )
    captured = {}

    def process_verifier(observed, candidate, *, artifact):
        captured.update({
            "identity": dict(observed),
            "candidate": dict(candidate),
            "artifact": artifact,
        })
        return {"pid": observed["pid"], "launchctl": {"state": "running"}}

    detail = LIVE_GATEWAY_RUNTIME_VERIFIER(
        identity,
        repo_root=root,
        cutover=cutover,
        consumer=consumer,
        plist_path=plist_path,
        process_verifier=process_verifier,
        interpreter_probe=probe,
    )

    assert captured["identity"] == identity
    assert captured["artifact"] == "manual_gateway_runtime"
    assert captured["candidate"]["program_arguments"][1:] == [
        "-m",
        "hermes_cli.main",
        "gateway",
        "run",
        "--replace",
    ]
    assert detail["process"]["pid"] == identity["pid"]
    assert detail["runtime_files_sha256"] == identity["runtime_files_sha256"]


def test_gateway_runtime_verifier_rejects_symlink_plist(tmp_path):
    root, cutover, consumer, identity, plist_path, probe = (
        _gateway_runtime_fixture(tmp_path)
    )
    symlink = tmp_path / "gateway-link.plist"
    symlink.symlink_to(plist_path)

    with pytest.raises(EvidenceError) as caught:
        LIVE_GATEWAY_RUNTIME_VERIFIER(
            identity,
            repo_root=root,
            cutover=cutover,
            consumer=consumer,
            plist_path=symlink,
            process_verifier=lambda *_args, **_kwargs: pytest.fail(
                "symlink plist must fail before process verification"
            ),
        )

    assert caught.value.code == "manual_gateway_runtime_installed_plist_invalid"


def test_gateway_runtime_verifier_rejects_public_config_drift(tmp_path):
    root, cutover, consumer, identity, plist_path, probe = (
        _gateway_runtime_fixture(tmp_path)
    )
    identity["public_config_sha256"] = "0" * 64

    with pytest.raises(EvidenceError) as caught:
        LIVE_GATEWAY_RUNTIME_VERIFIER(
            identity,
            repo_root=root,
            cutover=cutover,
            consumer=consumer,
            plist_path=plist_path,
            process_verifier=lambda *_args, **_kwargs: pytest.fail(
                "config drift must fail before process verification"
            ),
        )

    assert caught.value.code == "manual_gateway_runtime_candidate_mismatch"


def test_gateway_runtime_verifier_rejects_loaded_dependency_drift(tmp_path):
    root, cutover, consumer, identity, plist_path, probe = (
        _gateway_runtime_fixture(tmp_path)
    )
    dependency = root / ".venv" / "lib" / "psutil.py"
    dependency.write_text("# changed after gateway startup\n", encoding="utf-8")

    with pytest.raises(EvidenceError) as caught:
        LIVE_GATEWAY_RUNTIME_VERIFIER(
            identity,
            repo_root=root,
            cutover=cutover,
            consumer=consumer,
            plist_path=plist_path,
            process_verifier=lambda *_args, **_kwargs: pytest.fail(
                "loaded dependency drift must fail before process verification"
            ),
            interpreter_probe=probe,
        )

    assert caught.value.code == "manual_gateway_runtime_loaded_code_mismatch"


def test_gateway_runtime_verifier_binds_full_installed_plist_bytes(tmp_path):
    root, cutover, consumer, identity, plist_path, probe = (
        _gateway_runtime_fixture(tmp_path)
    )
    installed = release_gate_module.plistlib.loads(plist_path.read_bytes())
    installed["RunAtLoad"] = False
    plist_path.write_bytes(release_gate_module.plistlib.dumps(installed))

    with pytest.raises(EvidenceError) as caught:
        LIVE_GATEWAY_RUNTIME_VERIFIER(
            identity,
            repo_root=root,
            cutover=cutover,
            consumer=consumer,
            plist_path=plist_path,
            process_verifier=lambda *_args, **_kwargs: pytest.fail(
                "installed plist drift must fail before process verification"
            ),
            interpreter_probe=probe,
        )

    assert caught.value.code == "manual_gateway_runtime_installed_plist_mismatch"


def test_gateway_interpreter_probe_binds_repo_and_venv_module_origins():
    virtual_env = Path(release_gate_module.sys.prefix).resolve(strict=True)
    interpreter = virtual_env / "bin" / "python"
    detail = release_gate_module._gateway_interpreter_runtime_probe(
        interpreter,
        repo_root=release_gate_module.REPO_ROOT,
        virtual_env=virtual_env,
    )
    assert detail["repo_module_count"] == 4
    assert detail["venv_dependency_count"] == 2
    assert len(detail["module_origins_sha256"]) == 64
    assert detail["dependency_versions"] == (
        release_gate_module.EXPECTED_GATEWAY_RUNTIME_DEPENDENCY_VERSIONS
    )


def test_gateway_virtualenv_accepts_approved_shared_venv_without_repo_venv(
    tmp_path,
):
    repo_root = tmp_path / "candidate"
    shared_root = tmp_path / "venvs"
    virtual_env = shared_root / "candidate-runtime"
    repo_root.mkdir()
    (virtual_env / "bin").mkdir(parents=True)
    interpreter = virtual_env / "bin" / "python"
    interpreter.symlink_to(release_gate_module.sys.executable)

    resolved = release_gate_module._gateway_virtual_env(
        raw_interpreter=interpreter,
        environment={
            "VIRTUAL_ENV": str(virtual_env),
            "PATH": f"{virtual_env / 'bin'}:/usr/bin:/bin",
        },
        repo_root=repo_root,
        approved_shared_root=shared_root,
    )
    assert resolved == virtual_env.resolve(strict=True)


def test_manual_canaries_must_share_same_gateway_process(tmp_path):
    _root, _cutover_config, _consumer, identity, _plist_path, _probe = (
        _gateway_runtime_fixture(tmp_path)
    )
    manual_success = {
        "observed_trigger_source": {
            "authorization": {"gateway_runtime_identity": dict(identity)}
        }
    }
    terminal = {
        "gateway_runtime_identity": dict(identity),
        "gateway_runtime": {"process": {"pid": identity["pid"]}},
    }

    binding = release_gate_module._check_manual_canary_gateway_runtime(
        manual_success, terminal
    )
    assert binding["runtime_identity"]["pid"] == identity["pid"]

    manual_success["observed_trigger_source"]["authorization"][
        "gateway_runtime_identity"
    ]["pid"] += 1
    with pytest.raises(EvidenceError) as caught:
        release_gate_module._check_manual_canary_gateway_runtime(
            manual_success, terminal
        )
    assert caught.value.code == "manual_canary_gateway_runtime_mismatch"


def test_launchctl_loaded_configuration_must_match_disk_candidate():
    pid = 41000
    arguments = [
        "/candidate/.venv/bin/python",
        "-m",
        "hermes_cli.main",
        "gateway",
        "run",
        "--replace",
    ]
    stdout = f"""gui/501/ai.hermes.gateway = {{
    path = /candidate/ai.hermes.gateway.plist
    state = running
    program = {arguments[0]}
    arguments = {{
        {chr(10).join(arguments)}
    }}
    working directory = /candidate
    environment = {{
        VIRTUAL_ENV => /candidate/.venv
        PATH => /candidate/.venv/bin:/usr/bin:/bin
    }}
    pid = {pid}
}}
"""

    def runner(command, **_kwargs):
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    expected = {
        "plist_path": "/candidate/ai.hermes.gateway.plist",
        "program_arguments": arguments,
        "working_directory": "/candidate",
        "environment": {
            "VIRTUAL_ENV": "/candidate/.venv",
            "PATH": "/candidate/.venv/bin:/usr/bin:/bin",
        },
    }
    detail = release_gate_module._launchctl_process_evidence(
        service_label="ai.hermes.gateway",
        pid=pid,
        artifact="manual_gateway_runtime",
        expected_loaded_config=expected,
        runner=runner,
    )
    assert detail["program_arguments_sha256"] == _sha256_json(arguments)

    drifted = copy.deepcopy(expected)
    drifted["environment"]["VIRTUAL_ENV"] = "/candidate/other-venv"
    with pytest.raises(EvidenceError) as caught:
        release_gate_module._launchctl_process_evidence(
            service_label="ai.hermes.gateway",
            pid=pid,
            artifact="manual_gateway_runtime",
            expected_loaded_config=drifted,
            runner=runner,
        )
    assert caught.value.code == "manual_gateway_runtime_launchctl_config_mismatch"


def test_resident_installed_plist_must_match_candidate_bytes(tmp_path):
    path = tmp_path / "local.pnc.rca-kafka-consumer.plist"
    body = {
        "Label": "local.pnc.rca-kafka-consumer",
        "ProgramArguments": ["/candidate/.venv/bin/python", "/candidate/consumer.py"],
        "WorkingDirectory": "/candidate",
        "EnvironmentVariables": {"HERMES_HOME": "/candidate/home"},
    }
    raw = release_gate_module.plistlib.dumps(body)
    path.write_bytes(raw)
    candidate = {
        "service_label": body["Label"],
        "program_arguments": body["ProgramArguments"],
        "working_directory": body["WorkingDirectory"],
        "environment": body["EnvironmentVariables"],
        "plist_path": str(path),
        "plist_sha256": hashlib.sha256(raw).hexdigest(),
    }
    detail = release_gate_module._verify_installed_launchd_plist(
        candidate,
        artifact="kafka_consumer_health",
    )
    assert detail["sha256"] == candidate["plist_sha256"]

    body["EnvironmentVariables"]["HERMES_HOME"] = "/drifted/home"
    path.write_bytes(release_gate_module.plistlib.dumps(body))
    with pytest.raises(EvidenceError) as caught:
        release_gate_module._verify_installed_launchd_plist(
            candidate,
            artifact="kafka_consumer_health",
        )
    assert caught.value.code == "kafka_consumer_health_installed_plist_mismatch"


def test_resident_process_binds_installed_and_loaded_launchd_config(tmp_path):
    executable = str(Path(release_gate_module.sys.executable).resolve(strict=True))
    working_directory = str(tmp_path.resolve(strict=True))
    label = "local.pnc.rca-kafka-consumer"
    arguments = [executable, "/candidate/pnc_rca_kafka_consumer.py"]
    environment = {"HERMES_HOME": "/candidate/home"}
    plist_path = tmp_path / f"{label}.plist"
    raw = release_gate_module.plistlib.dumps({
        "Label": label,
        "ProgramArguments": arguments,
        "WorkingDirectory": working_directory,
        "EnvironmentVariables": environment,
    })
    plist_path.write_bytes(raw)
    candidate = {
        "service_label": label,
        "interpreter": executable,
        "runtime_executable": executable,
        "program_arguments": arguments,
        "working_directory": working_directory,
        "environment": environment,
        "verify_loaded_configuration": True,
        "plist_path": str(plist_path),
        "plist_sha256": hashlib.sha256(raw).hexdigest(),
    }
    identity = {
        "service_label": label,
        "pid": 42000,
        "process_create_time": 1_783_650_000.0,
        "boot_time": 1_783_000_000.0,
        "executable": executable,
        "cwd": working_directory,
    }

    class FakeProcess:
        def create_time(self):
            return identity["process_create_time"]

        def exe(self):
            return executable

        def cwd(self):
            return working_directory

        def cmdline(self):
            return list(arguments)

        def environ(self):
            return dict(environment)

    stdout = f"""gui/501/{label} = {{
    path = {plist_path}
    state = running
    program = {arguments[0]}
    arguments = {{
        {chr(10).join(arguments)}
    }}
    working directory = {working_directory}
    environment = {{
        HERMES_HOME => /candidate/home
    }}
    pid = {identity['pid']}
}}
"""
    result = LIVE_PROCESS_VERIFIER(
        identity,
        candidate,
        artifact="kafka_consumer_health",
        process_factory=lambda _pid: FakeProcess(),
        boot_time_reader=lambda: identity["boot_time"],
        launchctl_runner=lambda command, **_kwargs: subprocess.CompletedProcess(
            command,
            0,
            stdout=stdout,
            stderr="",
        ),
    )
    assert result["installed_plist"]["sha256"] == candidate["plist_sha256"]
    assert result["launchctl"]["state"] == "running"


def _manual_gateway_barrier_fixture(tmp_path: Path):
    consumer, _dispatcher, settings = _gate(tmp_path, "production")
    cutover = replace(
        _cutover("production"),
        manual_intake_enabled=True,
        manual_chat_ids=(release_gate_module.G1Q3_RCA_GROUP_ID,),
        manual_operator_enabled=False,
    )
    runtime_map, runtime_sha256 = release_gate_module.runtime_file_snapshot(
        settings.host_repo_root,
        release_gate_module.GATEWAY_RCA_RUNTIME_RELATIVE_FILES,
    )
    identity = {
        "service_label": "ai.hermes.gateway",
        "pid": 41000,
        "process_create_time": 1_783_650_000.0,
        "boot_time": 1_783_000_000.0,
        "executable": "/candidate/python",
        "script": str(settings.host_repo_root / "gateway" / "run.py"),
        "cwd": str(settings.host_repo_root),
        "script_sha256": runtime_map["gateway/run.py"],
        "runtime_files_sha256": runtime_sha256,
        "public_config_sha256": "c" * 64,
        "loaded_runtime_sha256": "d" * 64,
    }

    receipt_root = settings.group_binding_receipt_dir
    receipt_root.mkdir(mode=0o700)

    def authorization_source(name: str):
        source_id = release_gate_module._stable_trigger_source_id(
            "feishu_group_manual",
            f"feishu:{name}",
        )
        record = {
            "timestamp": "2026-07-10T07:59:00+00:00",
            "platform": "feishu",
            "group_id": release_gate_module.G1Q3_RCA_GROUP_ID,
            "message_id": name,
            "requester": "ou_release_operator",
            "decision": "accepted",
            "route_surface": "rca_manual_intake",
            "risk_gate": "manual_intake_control_store",
            "manual_authorization": {"authorized": True},
            "gateway_runtime_identity": dict(identity),
        }
        path = receipt_root / release_gate_module.pnc_group_binding_receipt_filename(
            receipt_date=NOW.date(),
            platform=record["platform"],
            chat_id=record["group_id"],
            user_id=record["requester"],
            message_id=record["message_id"],
        )
        raw = (json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n").encode()
        path.write_bytes(raw)
        path.chmod(0o600)
        evidence = {
            "receipt_path": str(path),
            "line_number": 1,
            "file_raw_sha256": hashlib.sha256(raw).hexdigest(),
            "record_sha256": _sha256_json(record),
            "timestamp": record["timestamp"],
            "platform": record["platform"],
            "chat_id": record["group_id"],
            "message_id": record["message_id"],
            "requester_id": record["requester"],
            "decision": record["decision"],
            "route_surface": record["route_surface"],
            "risk_gate": record["risk_gate"],
            "manual_authorization": record["manual_authorization"],
            "gateway_runtime_identity": dict(identity),
        }
        source = {
            "source_id": source_id,
            "authorization": evidence,
        }
        source_detail = {
            "authorization_sources": {
                source_id: {
                    "path": str(path),
                    "size_bytes": len(raw),
                    "raw_sha256": hashlib.sha256(raw).hexdigest(),
                    "canonical_sha256": _sha256_json({"records": [record]}),
                    "record_sha256": _sha256_json(record),
                }
            }
        }
        return source, source_detail

    success_source, success_source_detail = authorization_source("om_manual_success")
    terminal_source, terminal_source_detail = authorization_source(
        "om_manual_terminal"
    )
    manual_success = {
        "observed_trigger_source": success_source,
        "sources": success_source_detail,
    }
    terminal = {
        "observed_trigger_source": terminal_source,
        "gateway_runtime_identity": dict(identity),
        "sources": terminal_source_detail,
    }
    manifest = json.loads(
        (settings.evidence_dir / "build_manifest.json").read_text(encoding="utf-8")
    )
    critical = manifest["critical_files"]
    build_detail = {
        "host_commit": _git(settings.host_repo_root, "rev-parse", "HEAD"),
        "critical_file_sha256": critical,
        "critical_files_sha256": _sha256_json(critical),
    }

    def verified_gateway(_identity, **_kwargs):
        return {
            "runtime_file_sha256": dict(runtime_map),
            "runtime_files_sha256": runtime_sha256,
            "public_config_sha256": identity["public_config_sha256"],
            "plist_sha256": "d" * 64,
            "interpreter_sha256": "e" * 64,
            "process_executable_sha256": "f" * 64,
            "module_origins_sha256": "a" * 64,
            "loaded_runtime_sha256": identity["loaded_runtime_sha256"],
            "dependency_files_sha256": "b" * 64,
            "process": {
                "pid": identity["pid"],
                "process_create_time": identity["process_create_time"],
                "launchctl": {
                    "state": "running",
                    "pid": identity["pid"],
                    "plist_path_sha256": "1" * 64,
                    "program_arguments_sha256": "2" * 64,
                    "working_directory_sha256": "3" * 64,
                    "environment_sha256": "4" * 64,
                },
            },
        }

    resident_services = (
        ("kafka_consumer_health", "local.pnc.rca-kafka-consumer"),
        ("outbox_dispatcher_health", "local.pnc.rca-outbox-dispatcher"),
        ("delivery_collector_health", "local.pnc.rca-delivery-collector"),
        ("delivery_dispatcher_health", "local.pnc.rca-delivery-dispatcher"),
    )
    resident_identities = {
        label: {
            "service_label": label,
            "pid": 42000 + index,
            "process_create_time": 1_783_650_000.0 + index,
            "boot_time": 1_783_000_000.0,
            "executable": "/candidate/python",
            "script": f"/candidate/{label}.py",
            "cwd": str(settings.host_repo_root),
            "script_sha256": f"{index + 1:x}" * 64,
            "runtime_files_sha256": f"{index + 5:x}" * 64,
            "public_config_sha256": f"{index + 9:x}" * 64,
            "loaded_runtime_sha256": "e" * 64,
        }
        for index, (_artifact, label) in enumerate(resident_services)
    }

    def verified_residents():
        result = {
            artifact: {
                "runtime_identity_sha256": _sha256_json(
                    resident_identities[label]
                ),
                "loaded_runtime_sha256": resident_identities[label][
                    "loaded_runtime_sha256"
                ],
                "process": {
                    "pid": resident_identities[label]["pid"],
                    "process_create_time": resident_identities[label][
                        "process_create_time"
                    ],
                    "boot_time": resident_identities[label]["boot_time"],
                    "executable": "/candidate/python",
                    "cwd": str(settings.host_repo_root),
                    "cmdline_sha256": "5" * 64,
                    "required_environment_sha256": "6" * 64,
                    "launchctl": {
                        "state": "running",
                        "pid": 42000 + index,
                        "plist_path_sha256": "1" * 64,
                        "program_arguments_sha256": "2" * 64,
                        "working_directory_sha256": "3" * 64,
                        "environment_sha256": "4" * 64,
                    },
                    "installed_plist": {
                        "path_sha256": "7" * 64,
                        "sha256": "8" * 64,
                    },
                },
            }
            for index, (artifact, label) in enumerate(resident_services)
        }
        result["outbox_dispatcher_health"]["capacity_admission"] = {
            "required": False,
            "ready": True,
            "state": "steady",
            "error_code": "",
            "capacity_mode": "steady",
            "authorization": None,
        }
        return result

    def verified_databases():
        transitions = [
            {
                "service_label": label,
                "runtime_identity": dict(resident_identities[label]),
                "runtime_identity_sha256": _sha256_json(
                    resident_identities[label]
                ),
            }
            for _artifact, label in resident_services
        ]
        return {
            name: {
                "projection_sha256": digest,
                "host_runtime_transitions": copy.deepcopy(transitions),
                "host_runtime_transitions_sha256": _sha256_json(transitions),
            }
            for name, digest in (
                ("kafka_success", "8" * 64),
                ("manual_success", "9" * 64),
                ("manual_terminal", "a" * 64),
            )
        }

    return {
        "consumer": consumer,
        "settings": settings,
        "cutover": cutover,
        "manual_success": manual_success,
        "terminal": terminal,
        "build_detail": build_detail,
        "verified_gateway": verified_gateway,
        "verified_residents": verified_residents,
        "verified_databases": verified_databases,
    }


def test_manual_gateway_final_barrier_rechecks_same_build_and_process(tmp_path):
    fixture = _manual_gateway_barrier_fixture(tmp_path)
    detail = release_gate_module._check_manual_gateway_runtime_barrier(
        manual_success_detail=fixture["manual_success"],
        manual_terminal_detail=fixture["terminal"],
        manual_success_sources_verified=True,
        manual_terminal_sources_verified=True,
        build_detail=fixture["build_detail"],
        settings=fixture["settings"],
        cutover=fixture["cutover"],
        consumer=fixture["consumer"],
        gateway_verifier=fixture["verified_gateway"],
        resident_verifier=fixture["verified_residents"],
        database_verifier=fixture["verified_databases"],
    )
    assert detail["verification_rounds"] == 4
    assert detail["pid"] == 41000


def test_confirmation_runtime_recheck_rejects_gateway_or_each_resident_restart(
    tmp_path, monkeypatch
):
    fixture = _manual_gateway_barrier_fixture(tmp_path)
    barrier = release_gate_module._check_manual_gateway_runtime_barrier(
        manual_success_detail=fixture["manual_success"],
        manual_terminal_detail=fixture["terminal"],
        manual_success_sources_verified=True,
        manual_terminal_sources_verified=True,
        build_detail=fixture["build_detail"],
        settings=fixture["settings"],
        cutover=fixture["cutover"],
        consumer=fixture["consumer"],
        gateway_verifier=fixture["verified_gateway"],
        resident_verifier=fixture["verified_residents"],
        database_verifier=fixture["verified_databases"],
    )
    identity = fixture["manual_success"]["observed_trigger_source"][
        "authorization"
    ]["gateway_runtime_identity"]
    gateway = {
        "state": "running_safe",
        "pid": identity["pid"],
        "process_create_time": identity["process_create_time"],
        "runtime_identity_sha256": _sha256_json(identity),
        "runtime_identity": identity,
        "verified_runtime_sha256": "f" * 64,
    }
    health = fixture["verified_residents"]()
    report = {
        "checks": [
            {
                "name": "activation_bootstrap_runtime",
                "ok": True,
                "detail": {"gateway": gateway},
            },
            {
                "name": "delivery_service_health",
                "ok": True,
                "detail": health,
            },
            {
                "name": "manual_gateway_runtime_barrier",
                "ok": True,
                "detail": barrier,
            },
            {
                "name": "runtime_dependencies",
                "ok": True,
                "detail": {
                    "service_configs": {},
                    "service_processes": {},
                    "service_dependencies": {},
                },
            },
        ]
    }
    binding = release_gate_module._confirmation_runtime_continuity_binding(report)
    monkeypatch.setattr(
        release_gate_module,
        "_recheck_capsule_gateway_binding",
        lambda _binding: None,
    )
    monkeypatch.setattr(
        release_gate_module,
        "_check_delivery_service_health",
        lambda *_args, **_kwargs: copy.deepcopy(health),
    )
    LIVE_CONFIRMATION_RUNTIME_RECHECK(binding, report=report, now=NOW)

    def gateway_restarted(_binding):
        raise EvidenceError("activation_capsule_gateway_restarted")

    monkeypatch.setattr(
        release_gate_module,
        "_recheck_capsule_gateway_binding",
        gateway_restarted,
    )
    with pytest.raises(EvidenceError, match="activation_capsule_gateway_restarted"):
        LIVE_CONFIRMATION_RUNTIME_RECHECK(binding, report=report, now=NOW)
    monkeypatch.setattr(
        release_gate_module,
        "_recheck_capsule_gateway_binding",
        lambda _binding: None,
    )

    for artifact in sorted(health):
        restarted = copy.deepcopy(health)
        restarted[artifact]["process"]["pid"] += 100
        restarted[artifact]["process"]["launchctl"]["pid"] += 100
        restarted[artifact]["process"]["process_create_time"] += 1.0
        monkeypatch.setattr(
            release_gate_module,
            "_check_delivery_service_health",
            lambda *_args, observed=restarted, **_kwargs: observed,
        )
        with pytest.raises(
            EvidenceError,
            match="activation_confirmation_resident_restarted",
        ):
            LIVE_CONFIRMATION_RUNTIME_RECHECK(binding, report=report, now=NOW)


def test_manual_gateway_final_barrier_rejects_mid_gate_host_change(tmp_path):
    fixture = _manual_gateway_barrier_fixture(tmp_path)
    calls = 0

    def racing_gateway(*args, **kwargs):
        nonlocal calls
        calls += 1
        detail = fixture["verified_gateway"](*args, **kwargs)
        if calls == 2:
            target = fixture["settings"].host_repo_root / "gateway" / "run.py"
            target.write_text("changed during gate\n", encoding="utf-8")
        return detail

    with pytest.raises(EvidenceError) as caught:
        release_gate_module._check_manual_gateway_runtime_barrier(
            manual_success_detail=fixture["manual_success"],
            manual_terminal_detail=fixture["terminal"],
            manual_success_sources_verified=True,
            manual_terminal_sources_verified=True,
            build_detail=fixture["build_detail"],
            settings=fixture["settings"],
            cutover=fixture["cutover"],
            consumer=fixture["consumer"],
            gateway_verifier=racing_gateway,
            resident_verifier=fixture["verified_residents"],
            database_verifier=fixture["verified_databases"],
        )
    assert caught.value.code == "manual_gateway_runtime_barrier_host_unverified"


def test_manual_gateway_final_barrier_rejects_old_canary_after_resident_restart(
    tmp_path,
):
    fixture = _manual_gateway_barrier_fixture(tmp_path)

    def restarted_residents():
        residents = fixture["verified_residents"]()
        kafka = residents["kafka_consumer_health"]
        kafka["runtime_identity_sha256"] = "0" * 64
        kafka["process"]["pid"] += 100
        kafka["process"]["process_create_time"] += 100.0
        kafka["process"]["launchctl"]["pid"] = kafka["process"]["pid"]
        return residents

    with pytest.raises(EvidenceError) as caught:
        release_gate_module._check_manual_gateway_runtime_barrier(
            manual_success_detail=fixture["manual_success"],
            manual_terminal_detail=fixture["terminal"],
            manual_success_sources_verified=True,
            manual_terminal_sources_verified=True,
            build_detail=fixture["build_detail"],
            settings=fixture["settings"],
            cutover=fixture["cutover"],
            consumer=fixture["consumer"],
            gateway_verifier=fixture["verified_gateway"],
            resident_verifier=restarted_residents,
            database_verifier=fixture["verified_databases"],
        )

    assert caught.value.code == (
        "manual_gateway_runtime_barrier_transition_resident_mismatch"
    )


def test_shadow_does_not_invoke_build_provenance_verifier(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "shadow")

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("shadow"),
        now=NOW,
        build_provenance_verifier=lambda _settings: pytest.fail(
            "shadow must not probe live build provenance"
        ),
    )

    assert report["ok"] is True


@pytest.mark.parametrize(
    ("mutation", "blocker"),
    [
        (
            lambda config: replace(config, username="another-service"),
            "consumer_identity_mismatch",
        ),
        (
            lambda config: replace(config, username="rca_invalid principal"),
            "consumer_identity_mismatch",
        ),
        (
            lambda config: replace(config, api_version=(3, 8, 0)),
            "consumer_api_version_mismatch",
        ),
        (
            lambda config: replace(config, request_timeout_ms=60_000),
            "consumer_request_timeout_mismatch",
        ),
        (
            lambda config: replace(config, auto_offset_reset="latest"),
            "consumer_auto_offset_reset_mismatch",
        ),
        (
            lambda config: replace(config, topic="near-match-topic"),
            "consumer_topic_mismatch",
        ),
        (
            lambda config: replace(
                config,
                policy=replace(config.policy, policy_version="unreviewed-v2"),
            ),
            "consumer_rule_version_mismatch",
        ),
    ],
)
def test_fixed_consumer_contract_cannot_drift(tmp_path, mutation, blocker):
    consumer, dispatcher, settings = _gate(tmp_path, "shadow")

    report = evaluate_release_gate(
        consumer=mutation(consumer),
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("shadow"),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


def test_missing_contract_counterpart_is_a_hard_block(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "shadow")
    settings = ReleaseGateSettings(
        mode=settings.mode,
        evidence_dir=settings.evidence_dir,
        expected_topic=settings.expected_topic,
        expected_rule_version=settings.expected_rule_version,
        host_contract_path=settings.host_contract_path,
        vm_contract_path=tmp_path / "missing-vm-contract.py",
        kafka_env_file=settings.kafka_env_file,
    )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("shadow"),
        now=NOW,
    )

    assert report["ok"] is False
    assert "counterpart_missing" in report["blockers"]
    assert "missing-vm-contract.py" not in json.dumps(report)


def test_missing_and_stale_evidence_fail_closed(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "shadow")
    (settings.evidence_dir / "broker_metadata.json").unlink()
    fixtures_path = settings.evidence_dir / "workflow_fixtures.json"
    fixtures = json.loads(fixtures_path.read_text(encoding="utf-8"))
    fixtures["observed_at"] = (NOW - timedelta(hours=2)).isoformat()
    _write_json(fixtures_path, fixtures)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("shadow"),
        now=NOW,
    )

    assert report["ok"] is False
    assert "broker_metadata_missing" in report["blockers"]
    assert "t0_offsets_broker_partitions_unverified" in report["blockers"]
    assert "workflow_fixtures_stale" in report["blockers"]


def test_official_broker_preflight_v3_round_trip_is_replayed(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "shadow")

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("shadow"),
        now=NOW,
    )

    detail = next(
        check["detail"]
        for check in report["checks"]
        if check["name"] == "broker_metadata"
    )
    assert detail["cluster_id"] == "cluster-production-1"
    assert detail["partitions"] == [0, 1]
    assert detail["replication_factor"] == 2
    assert detail["topic_authorized_operations"] == ["DESCRIBE", "READ"]
    assert detail["group_authorized_operations"] == ["DESCRIBE", "READ"]


@pytest.mark.parametrize(
    ("mutation", "blocker"),
    [
        (
            lambda body: body.update(cluster_id="other-cluster"),
            "broker_metadata_cluster_mismatch",
        ),
        (
            lambda body: body.update(partitions=[1, 2]),
            "broker_metadata_partitions_not_contiguous",
        ),
        (
            lambda body: body["partition_topology"][0].update(isr=[1]),
            "broker_metadata_topology_unhealthy",
        ),
        (
            lambda body: body.update(replication_factor=1),
            "broker_metadata_replication_factor_mismatch",
        ),
        (
            lambda body: body["topic_authorized_operations"].append("WRITE"),
            "broker_metadata_topic_authorization_invalid",
        ),
        (
            lambda body: body["group_authorized_operations"].append("DELETE"),
            "broker_metadata_group_authorization_invalid",
        ),
        (
            lambda body: body.update(production_eligible=False),
            "broker_metadata_authorization_or_health_invalid",
        ),
        (
            lambda body: body.update(owner_approval_required=["group_acl"]),
            "broker_metadata_authorization_or_health_invalid",
        ),
        (
            lambda body: body["collector"].update(source_sha256="0" * 64),
            "broker_metadata_collector_source_mismatch",
        ),
        (
            lambda body: body["collector"].update(
                dependency_versions={"kafka-python": "3.0.6"}
            ),
            "broker_metadata_collector_dependency_mismatch",
        ),
        (
            lambda body: body["collector"].update(mode="observe_only"),
            "broker_metadata_collector_mode_invalid",
        ),
        (
            lambda body: body["collector"]["config"].update(
                expected_cluster_id="other-cluster"
            ),
            "broker_metadata_collector_config_mismatch",
        ),
        (
            lambda body: body["collector"]["env_file"].update(mtime_ns=1),
            "broker_metadata_collector_env_mismatch",
        ),
        (
            lambda body: body["collector"]["side_effect_contract"].update(
                poll=True
            ),
            "broker_metadata_collector_side_effect_contract_invalid",
        ),
    ],
)
def test_broker_preflight_v3_mutations_fail_closed(tmp_path, mutation, blocker):
    consumer, dispatcher, settings = _gate(tmp_path, "shadow")
    path = settings.evidence_dir / "broker_metadata.json"
    body = json.loads(path.read_text(encoding="utf-8"))
    mutation(body)
    _write_json(path, body)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("shadow"),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


def test_legacy_handwritten_broker_v1_evidence_is_rejected(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "shadow")
    _write_json(
        settings.evidence_dir / "broker_metadata.json",
        {
            "schema_version": "pnc_rca_broker_metadata_v1",
            "observed_at": OBSERVED_AT,
            "authorized": True,
            "topic": TOPIC,
            "partitions": [1],
        },
    )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("shadow"),
        now=NOW,
    )

    assert "broker_metadata_schema_mismatch" in report["blockers"]


def test_broker_replication_policy_has_a_hard_floor(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "shadow")
    contents = settings.kafka_env_file.read_text(encoding="utf-8")
    settings.kafka_env_file.write_text(
        contents.replace(
            "HERMES_RCA_KAFKA_MIN_REPLICATION_FACTOR=2",
            "HERMES_RCA_KAFKA_MIN_REPLICATION_FACTOR=1",
        ),
        encoding="utf-8",
    )
    settings.kafka_env_file.chmod(0o600)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("shadow"),
        now=NOW,
    )

    assert "broker_metadata_replication_policy_too_weak" in report["blockers"]


def test_functional_bootstrap_accepts_owner_configured_single_replica(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "preauthorization")
    contents = settings.kafka_env_file.read_text(encoding="utf-8")
    settings.kafka_env_file.write_text(
        contents.replace(
            "HERMES_RCA_KAFKA_MIN_REPLICATION_FACTOR=2",
            "HERMES_RCA_KAFKA_MIN_REPLICATION_FACTOR=1",
        ),
        encoding="utf-8",
    )
    settings.kafka_env_file.chmod(0o600)
    source, env_observation = load_kafka_preflight_environment(
        settings.kafka_env_file
    )
    probe = BrokerProbeConfig.from_env(source)
    path = settings.evidence_dir / "broker_metadata.json"
    body = json.loads(path.read_text(encoding="utf-8"))
    body["replication_factor"] = 1
    for partition in body["partition_topology"]:
        leader = partition["leader_id"]
        partition["replicas"] = [leader]
        partition["isr"] = [leader]
    body["collector"]["config"] = probe.public_dict()
    body["collector"]["connection_config_sha256"] = (
        release_gate_module._sha256_json(probe.public_dict())
    )
    body["collector"]["env_file"] = env_observation
    _write_json(path, body)

    report = evaluate_release_gate(
        consumer=ConsumerConfig.from_env(source),
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("preauthorization"),
        now=NOW,
    )

    check = next(item for item in report["checks"] if item["name"] == "broker_metadata")
    assert check["ok"] is True
    assert check["detail"]["replication_factor"] == 1


def test_preauthorization_accepts_immutable_release_evidence_after_cutover(
    tmp_path,
):
    consumer, dispatcher, settings = _gate(tmp_path, "preauthorization")
    observed_at = (NOW - timedelta(hours=1)).isoformat()
    for name in ("build_manifest.json", "cutover_plan.json"):
        path = settings.evidence_dir / name
        body = json.loads(path.read_text(encoding="utf-8"))
        body["observed_at"] = observed_at
        _write_json(path, body)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("preauthorization"),
        now=NOW,
    )

    checks = {item["name"]: item for item in report["checks"]}
    assert checks["build_manifest"]["ok"] is True
    assert checks["cutover_plan"]["ok"] is True


@pytest.mark.parametrize(
    ("raw", "blocker"),
    [
        (
            '{"schema_version":"wrong","schema_version":"pnc_rca_broker_metadata_v1"}',
            "broker_metadata_invalid_json",
        ),
        (
            '{"schema_version":"pnc_rca_broker_metadata_v1","value":NaN}',
            "broker_metadata_invalid_json",
        ),
        (
            "[" * 70 + "]" * 70,
            "broker_metadata_json_too_deep",
        ),
    ],
)
def test_release_evidence_rejects_ambiguous_or_excessive_json(tmp_path, raw, blocker):
    consumer, dispatcher, settings = _gate(tmp_path, "shadow")
    (settings.evidence_dir / "broker_metadata.json").write_text(
        raw,
        encoding="utf-8",
    )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("shadow"),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


def test_fixture_expected_result_is_replayed_not_trusted(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "shadow")
    path = settings.evidence_dir / "workflow_fixtures.json"
    body = json.loads(path.read_text(encoding="utf-8"))
    body["fixtures"][0]["expected_reason"] = "state_transition_not_allowed"
    _write_json(path, body)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("shadow"),
        now=NOW,
    )

    assert report["ok"] is False
    assert "workflow_fixture_replay_mismatch" in report["blockers"]


@pytest.mark.parametrize(
    ("event_uid", "blocker"),
    [
        ("event-0-10", "canary_plan_event_uid_invalid"),
        (f"{TOPIC}:00:10", "canary_plan_event_uid_invalid"),
        (f"{TOPIC}:2:10", "canary_plan_partition_not_in_t0"),
        (f"{TOPIC}:0:9", "canary_plan_offset_before_t0"),
    ],
)
def test_canary_event_uid_is_exact_and_bounded_by_t0(tmp_path, event_uid, blocker):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    path = settings.evidence_dir / "canary_plan.json"
    plan = json.loads(path.read_text(encoding="utf-8"))
    plan["event_uid"] = event_uid
    _write_json(path, plan)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


def test_canary_plan_v4_rejects_legacy_or_extra_fields(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "preauthorization")
    path = settings.evidence_dir / "canary_plan.json"
    plan = json.loads(path.read_text(encoding="utf-8"))
    plan["max_promotions"] = 0
    _write_json(path, plan)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("preauthorization"),
        now=NOW,
    )

    assert "canary_plan_shape_invalid" in report["blockers"]


def test_preproduction_capsule_freezes_exact_canary_plan(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    activation = release_gate_module._check_activation_epoch(
        control_db_path=dispatcher.control_db_path,
        mode="canary",
        expected_config_sha256=_runtime_config_sha256(
            consumer,
            dispatcher,
            "canary",
        ),
    )
    drifted_plan = copy.deepcopy(_canonical_activation_slot_plan())
    drifted_plan["manual_success"]["source_identity_sha256"] = "f" * 64
    with pytest.raises(
        EvidenceError,
        match="activation_canary_plan_identity_mismatch",
    ):
        release_gate_module._check_activation_slot_plan(
            activation,
            drifted_plan,
            require_consumed=False,
        )

    plan_path = settings.evidence_dir / "canary_plan.json"
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    plan["reason"] = "replacement plan after preproduction"
    _write_json(plan_path, plan)
    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert "activation_stage_canary_plan_raw_sha256_mismatch" in report["blockers"]


def test_canary_rejects_relocated_evidence_before_plan_drift(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    drifted_evidence = tmp_path / "drifted-evidence"
    shutil.copytree(settings.evidence_dir, drifted_evidence)
    plan_path = drifted_evidence / "canary_plan.json"
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    plan["reason"] = "replacement plan from a different evidence directory"
    _write_json(plan_path, plan)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=replace(settings, evidence_dir=drifted_evidence),
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert "capacity_initialization_receipt_invalid" in report["blockers"]
    assert "activation_bootstrap_migration_unverified" in report["blockers"]


@pytest.mark.parametrize(
    ("mutation", "blocker"),
    [
        (
            lambda request: request.update(
                schema_version="g1q3_rca_execution_request_v1"
            ),
            "remote_request_schema_mismatch",
        ),
        (
            lambda request: request["data"].update(
                pdcl_download_cmd="mdi download event -u forbidden -s ./"
            ),
            "remote_request_legacy_download_field_present",
        ),
        (
            lambda request: request["data"]["data_access"].update(mode="download"),
            "remote_request_data_access_mode_mismatch",
        ),
        (
            lambda request: request["data"]["data_access"]["reader_contract"].update(
                required_version="0.1.5"
            ),
            "remote_request_reader_contract_mismatch",
        ),
        (
            lambda request: request["data"]["data_access"]["reader_contract"].update(
                mdi_download_allowed=True
            ),
            "remote_request_legacy_download_field_present",
        ),
        (
            lambda request: request["execution_policy"].update(
                input_materialization="required"
            ),
            "remote_request_legacy_download_field_present",
        ),
    ],
)
def test_canary_plan_requires_request_v2_remote_read_contract(
    tmp_path, mutation, blocker
):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    path = settings.evidence_dir / "canary_plan.json"
    body = json.loads(path.read_text(encoding="utf-8"))
    mutation(body["execution_request"])
    _write_json(path, body)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


@pytest.mark.parametrize(
    ("section", "nested_control"),
    [
        ("evidence", {"legacy": {"pdcl_download_cmd": "redacted"}}),
        ("toolchain", {"legacy": {"mdi_download_cmd": "redacted"}}),
        ("work_item", {"legacy": {"is_pdcl_format": False}}),
        ("evidence", {"legacy": {"allow_download": True}}),
        (
            "toolchain",
            {"legacy": {"input_materialization": "optional"}},
        ),
        (
            "work_item",
            {"legacy": {"operator_instruction": "mdi download event -u hidden"}},
        ),
        (
            "evidence",
            {"legacy": {"operator_instruction": "mdi refresh event -u hidden"}},
        ),
        (
            "evidence",
            {"policy_invariants": ["MDI download event -u hidden is forbidden."]},
        ),
        ("toolchain", {"legacy": {"run mdi download": False}}),
    ],
)
def test_release_gate_recursively_rejects_nested_legacy_download_controls(
    section, nested_control
):
    request = _remote_execution_request()
    request[section]["nested_controls"] = nested_control

    with pytest.raises(
        EvidenceError,
        match="remote_request_legacy_download_field_present",
    ):
        release_gate_module._check_remote_execution_request(
            request,
            field="test.execution_request",
            expected_admission=CANARY_ADMISSION.to_dict(),
            expected_origin_source_id=SOURCE_ID,
            expected_origin_storage_kind="kafka_workflow_event",
        )


def test_release_gate_allows_disabled_historical_mdi_policy_invariant():
    request = _remote_execution_request()
    request["evidence"]["policy_invariants"] = [
        "Historical MDI download path is retired and forbidden. Remote-read only."
    ]

    detail = release_gate_module._check_remote_execution_request(
        request,
        field="test.execution_request",
        expected_admission=CANARY_ADMISSION.to_dict(),
        expected_origin_source_id=SOURCE_ID,
        expected_origin_storage_kind="kafka_workflow_event",
    )

    assert detail["mode"] == "remote_read"
    assert (
        request["data"]["data_access"]["reader_contract"]["mdi_download_allowed"]
        is False
    )


@pytest.mark.parametrize(
    ("mutation", "blocker"),
    [
        (
            lambda scope: scope.update(source="manual_topic_list"),
            "remote_requested_scope_source_mismatch",
        ),
        (
            lambda scope: scope["requirements"].update(
                requirements_contract_hash="not-a-hash"
            ),
            "invalid_evidence_format",
        ),
        (
            lambda scope: scope["requirements"]["requested_topics"].append("z-topic"),
            "remote_requested_scope_requirements_hash_mismatch",
        ),
    ],
)
def test_canary_plan_binds_evaluator_requested_scope(tmp_path, mutation, blocker):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    path = settings.evidence_dir / "canary_plan.json"
    body = json.loads(path.read_text(encoding="utf-8"))
    mutation(body["requested_scope"])
    _write_json(path, body)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


def test_production_canary_receipt_is_bound_to_planned_remote_request(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "production")
    _manifest, path, _sources = _committed_pair_paths(
        settings.evidence_dir, "primary"
    )
    body = json.loads(path.read_text(encoding="utf-8"))
    body["execution_request"]["data"]["data_access"]["references"][0]["event_uuid"] = (
        "different-event"
    )
    _rewrite_committed_pair_body(
        settings.evidence_dir, "primary", "receipt", body
    )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("production"),
        now=NOW,
    )

    assert report["ok"] is False
    assert "canary_receipt_execution_request_mismatch" in report["blockers"]


@pytest.mark.parametrize(
    ("mutation", "blocker"),
    [
        (
            lambda health: health["runtime"].update(version="0.1.5"),
            "remote_reader_version_mismatch",
        ),
        (
            lambda health: health["runtime"].update(upstream_wheel_sha256="f" * 64),
            "remote_reader_upstream_provenance_mismatch",
        ),
        (
            lambda health: health["runtime"].update(
                sanitized_wheel_sha256=(
                    "e760a532dbe7dff730ef8e85b32e4ff33d14acefe7c8f295224bb77b08fcadae"
                )
            ),
            "remote_reader_unsanitized_wheel_forbidden",
        ),
        (
            lambda health: health["runtime"]["security_attestation"].update(
                auto_mount=True
            ),
            "remote_reader_security_attestation_failed",
        ),
        (
            lambda health: health["runtime"]["runtime_environment"].update(
                endpoint_variable="PDCL_URL"
            ),
            "remote_reader_runtime_environment_unverified",
        ),
        (
            lambda health: health["runtime"]["runtime_environment"].update(
                endpoint_present=False
            ),
            "remote_reader_runtime_environment_unverified",
        ),
        (
            lambda health: health["runtime"]["runtime_environment"].update(
                legacy_endpoint_variables_accepted=True
            ),
            "remote_reader_runtime_environment_unverified",
        ),
        (
            lambda health: health["runtime"]["dependencies"]["mcap"].update(
                installed_version="1.2.1"
            ),
            "remote_reader_dependency_version_mismatch",
        ),
        (
            lambda health: health["runtime"].update(
                dependency_domain="main_service_python"
            ),
            "remote_reader_dependency_domain_mismatch",
        ),
        (
            lambda health: health["runtime"].update(execution_mode="in_process"),
            "remote_reader_dependency_domain_mismatch",
        ),
        (
            lambda health: health["runtime"].update(
                module_path="/usr/local/lib/python3/site-packages/pdcl_pyclip/reader.py"
            ),
            "remote_reader_module_path_unpinned",
        ),
        (
            lambda health: health["runtime"]["dependencies"]["typer"].update(
                installed_version="0.12.5"
            ),
            "remote_reader_dependency_version_mismatch",
        ),
        (
            lambda health: health["reader_classes"]["RemoteEventReader"].update(
                importable=False
            ),
            "remote_reader_class_not_importable",
        ),
        (
            lambda health: health.update(mdi_invocation_count=1),
            "remote_reader_health_mdi_invoked",
        ),
        (
            lambda health: health["reader_classes"]["RemoteEventReader"].update(
                iter_messages_parameters=["self", "topics"]
            ),
            "remote_reader_iter_messages_signature_mismatch",
        ),
        (
            lambda health: health["fixture_preflight"].update(ok=False),
            "remote_reader_fixture_preflight_failed",
        ),
    ],
)
def test_canary_requires_live_remote_reader_dependency_and_fixture_preflight(
    tmp_path, mutation, blocker
):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    path = settings.evidence_dir / "remote_reader_health.json"
    body = json.loads(path.read_text(encoding="utf-8"))
    mutation(body)
    _write_json(path, body)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


def test_canary_missing_remote_reader_health_is_a_hard_no_go(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    (settings.evidence_dir / "remote_reader_health.json").unlink()

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert "remote_reader_health_missing" in report["blockers"]


def test_live_remote_reader_probe_uses_fixed_vm_wrapper_and_bounded_script(tmp_path):
    _consumer, _dispatcher, settings = _gate(tmp_path, "canary")
    probe = release_gate_module.verify_live_remote_reader(settings)
    calls = []

    def runner(args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(
            returncode=0,
            stdout=json.dumps(probe, sort_keys=True),
            stderr="",
        )

    observed = verify_live_remote_reader(settings, runner=runner)

    assert observed == probe
    assert len(calls) == 1
    args, kwargs = calls[0]
    assert args == [
        str(release_gate_module.DEFAULT_SSH_MINI_AGENT),
        "run_py_json",
    ]
    assert kwargs["timeout"] == release_gate_module.REMOTE_READER_PROBE_TIMEOUT_SECONDS
    assert kwargs["env"]["SSH_MINI_AGENT_TIMEOUT"] == str(
        release_gate_module.REMOTE_READER_PROBE_TIMEOUT_SECONDS
    )
    script = kwargs["input"]
    assert release_gate_module.REMOTE_READER_WHEEL_RELATIVE in script
    assert release_gate_module.REMOTE_READER_MANIFEST_RELATIVE in script
    assert "remote_reader_dependency_doctor(check_runtime_environment=True)" in script
    assert "pdcl_pyclip.reader" in script


def test_live_remote_reader_probe_timeout_fails_closed(tmp_path):
    _consumer, _dispatcher, settings = _gate(tmp_path, "canary")

    def runner(*_args, **_kwargs):
        raise subprocess.TimeoutExpired(cmd="ssh-mini-agent", timeout=30)

    with pytest.raises(EvidenceError, match="remote_reader_live_probe_timeout"):
        verify_live_remote_reader(settings, runner=runner)


@pytest.mark.parametrize(
    ("mutation", "blocker"),
    [
        (
            lambda probe: probe.update(repo_root="/home/mini/wrong"),
            "remote_reader_live_probe_path_mismatch",
        ),
        (
            lambda probe: probe.update(git_commit="f" * 40),
            "remote_reader_live_probe_commit_mismatch",
        ),
        (
            lambda probe: probe.update(git_status_sha256="f" * 64),
            "remote_reader_live_probe_tree_dirty",
        ),
        (
            lambda probe: probe.update(wheel_sha256="f" * 64),
            "remote_reader_live_probe_wheel_mismatch",
        ),
        (
            lambda probe: probe["manifest"]["module_sha256"].update({
                "pdcl_pyclip/_storage.py": "f" * 64
            }),
            "remote_reader_live_probe_manifest_mismatch",
        ),
        (
            lambda probe: probe["doctor"].update(status="blocked"),
            "remote_reader_live_probe_doctor_failed",
        ),
        (
            lambda probe: probe["doctor"]["runtime"].update(
                dependency_domain="main_service_python"
            ),
            "remote_reader_live_probe_health_mismatch",
        ),
        (
            lambda probe: probe["doctor"]["runtime_environment"].update(
                endpoint_present=False
            ),
            "remote_reader_live_probe_health_mismatch",
        ),
        (
            lambda probe: probe.update(
                module_path="/tmp/unpinned/pdcl_pyclip/reader.py"
            ),
            "remote_reader_live_probe_health_mismatch",
        ),
    ],
)
def test_canary_rejects_remote_reader_live_probe_drift(tmp_path, mutation, blocker):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    probe = release_gate_module.verify_live_remote_reader(settings)
    mutation(probe)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
        remote_reader_live_verifier=lambda _settings: probe,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


def test_production_requires_a_pinned_sanitized_reader_wheel(tmp_path, monkeypatch):
    consumer, dispatcher, settings = _gate(tmp_path, "production")
    monkeypatch.setattr(release_gate_module, "APPROVED_SANITIZED_WHEEL_SHA256", "")

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("production"),
        now=NOW,
    )

    assert report["ok"] is False
    assert "remote_reader_sanitized_wheel_not_pinned" in report["blockers"]


def _rehash_remote_soak_records(soak: dict) -> None:
    records = soak["case_evidence"]["records"]
    for record in records:
        material = dict(record)
        material.pop("record_sha256", None)
        record["record_sha256"] = _remote_soak_sha256(material)
    hashes = [record["record_sha256"] for record in records]
    soak["case_evidence"]["record_hashes"] = hashes
    soak["case_evidence"]["merkle_root"] = _remote_soak_merkle_root(hashes)


def _duplicate_remote_soak_case(soak: dict) -> None:
    records = soak["case_evidence"]["records"]
    records[1]["work_item_id"] = records[0]["work_item_id"]
    _rehash_remote_soak_records(soak)


def _remote_soak_case_scope_drift(soak: dict) -> None:
    record = soak["case_evidence"]["records"][0]
    record["requested_scope_binding"]["requested_scope_sha256"] = "f" * 64
    _rehash_remote_soak_records(soak)


def _remote_soak_scope_recomputed_attack(soak: dict, field: str) -> None:
    record = soak["case_evidence"]["records"][0]
    requirements = record["requested_scope"]["requirements"]
    requirements[field].append(
        "z_attack_topic" if field == "requested_topics" else "z.attack.channel"
    )
    requirements[field].sort()
    material = dict(requirements)
    material.pop("requirements_hash")
    requirements["requirements_hash"] = _sha256_json(material)
    record["requested_scope_binding"].update({
        "requirements_sha256": requirements["requirements_hash"],
        "requested_scope_sha256": _remote_soak_sha256(record["requested_scope"]),
    })
    _rehash_remote_soak_records(soak)


def _remote_soak_reference_drift(soak: dict) -> None:
    record = soak["case_evidence"]["records"][0]
    record["reference"]["locator_sha256"] = "f" * 64
    _rehash_remote_soak_records(soak)


def _remote_soak_domain_quota_drift(soak: dict) -> None:
    record = next(
        item
        for item in soak["case_evidence"]["records"]
        if item["quota_domain"] == "DNP"
    )
    record["function_domain"] = "ACC"
    record["quota_domain"] = "ACC"
    requirements = record["requested_scope"]["requirements"]
    requirements["function_domain"] = "ACC"
    material = dict(requirements)
    material.pop("requirements_hash")
    requirements["requirements_hash"] = _sha256_json(material)
    record["requested_scope_binding"] = {
        "requirements_contract_sha256": requirements[
            "requirements_contract_hash"
        ],
        "requirements_sha256": requirements["requirements_hash"],
        "requested_scope_sha256": _remote_soak_sha256(
            record["requested_scope"]
        ),
    }
    _rehash_remote_soak_records(soak)


def _remote_soak_reader_quota_drift(soak: dict) -> None:
    changed = 0
    for record in soak["case_evidence"]["records"]:
        reference = record["reference"]
        if reference["kind"] != "clip":
            continue
        reference.update(
            kind="event",
            reader_class="RemoteEventReader",
            locator_field="event_uuid",
        )
        material = dict(reference)
        material.pop("reference_binding_sha256")
        reference["reference_binding_sha256"] = _remote_soak_sha256(material)
        changed += 1
        if changed == 26:
            break
    _rehash_remote_soak_records(soak)


def _remote_soak_materialization_drift(soak: dict) -> None:
    soak["case_evidence"]["records"][0]["materialization"].update(
        input_materialized=True,
        input_materialized_bytes=1,
    )
    _rehash_remote_soak_records(soak)


def _remote_soak_case_candidate_drift(soak: dict) -> None:
    soak["case_evidence"]["records"][0]["candidate_binding"][
        "execution_commit"
    ] = "f" * 40
    _rehash_remote_soak_records(soak)


def _remote_soak_wall_clock_offset_drift(soak: dict) -> None:
    timing = soak["case_evidence"]["records"][100]["timing"]
    for field in ("started_at", "ended_at"):
        parsed = datetime.strptime(timing[field], "%Y-%m-%dT%H:%M:%S.%fZ").replace(
            tzinfo=timezone.utc
        )
        timing[field] = (parsed + timedelta(seconds=60)).strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        )
    _rehash_remote_soak_records(soak)


def _remote_soak_fixture(tmp_path: Path) -> tuple[dict, dict]:
    _consumer, dispatcher = _configs(tmp_path, "canary")
    health_path = tmp_path / "remote_reader_health.json"
    health_body = _remote_reader_health()
    _write_json(health_path, health_body)
    health_sha256 = hashlib.sha256(health_path.read_bytes()).hexdigest()
    health_detail = release_gate_module._check_remote_reader_health(
        health_body,
        now=NOW,
        max_age_seconds=3600,
        production=False,
    )
    vm_commit = "d" * 40
    vm_tree = "e" * 40
    soak, workload_manifest = _remote_reader_soak(
        vm_commit=vm_commit,
        vm_tree=vm_tree,
        remote_reader_health_sha256=health_sha256,
    )
    context = {
        "workload_manifest_body": workload_manifest,
        "expected_reader_fingerprint": health_detail["reader_fingerprint"],
        "expected_vm_commit": vm_commit,
        "expected_vm_tree": vm_tree,
        "expected_remote_reader_health_sha256": health_sha256,
        "remote_reader_health_detail": health_detail,
        "dispatcher": dispatcher,
        "target_cases_per_day": 200,
        "now": NOW,
        "max_age_seconds": 3600,
    }
    return soak, context


@pytest.mark.parametrize(
    ("mutation", "blocker"),
    [
        (
            lambda soak: soak.update(
                observed_at=(NOW - timedelta(hours=2)).strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ"
                )
            ),
            "remote_reader_soak_stale",
        ),
        (
            lambda soak: soak["source"].update(component_commit="f" * 40),
            "remote_reader_soak_source_mismatch",
        ),
        (
            lambda soak: soak["source"].update(schema_sha256="f" * 64),
            "remote_reader_soak_source_mismatch",
        ),
        (
            lambda soak: soak["source"].update(module_sha256="f" * 64),
            "remote_reader_soak_source_mismatch",
        ),
        (
            lambda soak: soak["source"].update(entrypoint_sha256="f" * 64),
            "remote_reader_soak_source_mismatch",
        ),
        (
            lambda soak: soak["dependency_binding"].update(
                remote_reader_health_sha256="f" * 64
            ),
            "remote_reader_soak_dependency_binding_mismatch",
        ),
        (
            lambda soak: soak.update(attempted_cases=199, completed_cases=199),
            "remote_reader_soak_case_volume_insufficient",
        ),
        (
            lambda soak: soak["policy"].update(retry_count=1),
            "remote_reader_soak_policy_mismatch",
        ),
        (
            lambda soak: soak.update(concurrency_peak=3),
            "remote_reader_soak_concurrency_mismatch",
        ),
        (
            _remote_soak_reference_drift,
            "remote_reader_soak_case_reference_binding_invalid",
        ),
        (
            _remote_soak_case_scope_drift,
            "remote_reader_soak_case_scope_mismatch",
        ),
        (
            _duplicate_remote_soak_case,
            "remote_reader_soak_case_duplicate",
        ),
        (
            _remote_soak_domain_quota_drift,
            "remote_reader_soak_case_scope_mismatch",
        ),
        (
            _remote_soak_reader_quota_drift,
            "remote_reader_soak_reader_quota_insufficient",
        ),
        (
            _remote_soak_materialization_drift,
            "remote_reader_soak_case_materialization_present",
        ),
        (
            _remote_soak_case_candidate_drift,
            "remote_reader_soak_candidate_binding_mismatch",
        ),
        (
            _remote_soak_wall_clock_offset_drift,
            "remote_reader_soak_case_timing_invalid",
        ),
        (
            lambda soak: soak["case_evidence"]["record_hashes"].__setitem__(
                0, "f" * 64
            ),
            "remote_reader_soak_case_hash_order_mismatch",
        ),
        (
            lambda soak: soak["case_evidence"].update(merkle_root="f" * 64),
            "remote_reader_soak_merkle_mismatch",
        ),
        (
            lambda soak: soak["dependency_binding"]["fingerprint_material"][
                "runtime_environment"
            ].update(endpoint_variable="PDCL_URL"),
            "remote_reader_soak_dependency_material_mismatch",
        ),
        (
            lambda soak: soak["workload_mix"]["reader_class_cases"].pop(
                "RemoteClipReader"
            ),
            "remote_reader_soak_workload_mix_inconsistent",
        ),
        (
            lambda soak: soak["concurrency_profile"].update(
                samples_at_or_above_reserved=1
            ),
            "remote_reader_soak_sustained_concurrency_insufficient",
        ),
        (
            lambda soak: soak["temporal_profile"]["bucket_start_counts"].update(
                {"23": 0}
            ),
            "remote_reader_soak_temporal_coverage_insufficient",
        ),
        (
            lambda soak: soak["capacity_lifecycle"].update(release_count=199),
            "remote_reader_soak_capacity_lifecycle_incomplete",
        ),
        (
            lambda soak: soak["capacity_lifecycle"].update(held_bytes_leak_count=1),
            "remote_reader_soak_capacity_lifecycle_failure",
        ),
        (
            lambda soak: soak["zero_invariants"].update(timeout_count=1),
            "remote_reader_soak_zero_invariant_failed",
        ),
        (
            lambda soak: soak["latency_ms"].update(p99=130_000.0, max=130_000.0),
            "remote_reader_soak_latency_slo_failed",
        ),
        (
            lambda soak: soak["resources"].update(rss_peak_bytes=4 * 1024**3 + 1),
            "remote_reader_soak_resource_limit_failed",
        ),
        (
            lambda soak: soak["resources"].update(fd_peak=900),
            "remote_reader_soak_resource_limit_failed",
        ),
    ],
)
def test_canary_remote_reader_soak_is_bounded_and_error_free(
    tmp_path, mutation, blocker
):
    body, context = _remote_soak_fixture(tmp_path)
    mutation(body)
    with pytest.raises(release_gate_module.EvidenceError, match=blocker):
        release_gate_module._check_remote_reader_soak(body, **context)


def test_remote_reader_soak_v4_recomputes_all_case_and_aggregate_evidence(tmp_path):
    body, context = _remote_soak_fixture(tmp_path)

    detail = release_gate_module._check_remote_reader_soak(body, **context)

    assert detail["attempted_cases"] == 200
    assert detail["case_merkle_root"] == body["case_evidence"]["merkle_root"]
    assert detail["workload_mix"]["function_domain_cases"] == {
        "ACC": 50,
        "AEB_FCW": 50,
        "DNP": 50,
        "LCC": 50,
    }
    assert detail["workload_manifest"]["sha256"] == body["workload_manifest"][
        "sha256"
    ]


def test_remote_reader_soak_v4_rejects_manifest_summary_hash_mismatch(tmp_path):
    body, context = _remote_soak_fixture(tmp_path)
    body["workload_manifest"]["sha256"] = "f" * 64

    with pytest.raises(
        release_gate_module.EvidenceError,
        match="remote_reader_soak_manifest_hash_mismatch",
    ):
        release_gate_module._check_remote_reader_soak(body, **context)


def test_remote_reader_soak_v4_rejects_manifest_source_mismatch(tmp_path):
    body, context = _remote_soak_fixture(tmp_path)
    context["workload_manifest_body"]["source"]["component_commit"] = "f" * 40

    with pytest.raises(
        release_gate_module.EvidenceError,
        match="remote_reader_soak_manifest_source_mismatch",
    ):
        release_gate_module._check_remote_reader_soak(body, **context)


def test_remote_reader_soak_v4_rejects_manifest_case_mismatch(tmp_path):
    body, context = _remote_soak_fixture(tmp_path)
    context["workload_manifest_body"]["cases"][0]["work_item_id"] = "work-drift"

    with pytest.raises(
        release_gate_module.EvidenceError,
        match="remote_reader_soak_manifest_case_mismatch",
    ):
        release_gate_module._check_remote_reader_soak(body, **context)


@pytest.mark.parametrize("field", ["requested_topics", "channel_allowlist"])
def test_remote_reader_soak_v4_rejects_recomputed_scope_and_merkle_attack(
    tmp_path,
    field,
):
    body, context = _remote_soak_fixture(tmp_path)
    original_merkle = body["case_evidence"]["merkle_root"]
    _remote_soak_scope_recomputed_attack(body, field)
    assert body["case_evidence"]["merkle_root"] != original_merkle

    with pytest.raises(
        release_gate_module.EvidenceError,
        match="remote_reader_soak_case_scope_mismatch",
    ):
        release_gate_module._check_remote_reader_soak(body, **context)


def test_remote_reader_soak_v4_composite_binds_manifest_source(tmp_path):
    body, context = _remote_soak_fixture(tmp_path)
    baseline = release_gate_module._check_remote_reader_soak(body, **context)
    original_merkle = body["case_evidence"]["merkle_root"]

    changed_source = dict(body["workload_manifest"]["source"])
    changed_source["artifact_sha256"] = "f" * 64
    body["workload_manifest"]["source"] = changed_source
    context["workload_manifest_body"]["source"] = dict(changed_source)
    body["workload_manifest"]["sha256"] = _remote_soak_sha256(
        context["workload_manifest_body"]
    )
    changed = release_gate_module._check_remote_reader_soak(body, **context)

    assert changed["case_merkle_root"] == original_merkle
    assert changed["manifest_case_binding_sha256"] != baseline[
        "manifest_case_binding_sha256"
    ]
    assert changed["workload_manifest"]["source_artifact_verification"] == (
        "producer_locator_only"
    )


def test_remote_reader_soak_v4_rejects_manifest_generated_after_soak_start(tmp_path):
    body, context = _remote_soak_fixture(tmp_path)
    context["workload_manifest_body"]["generated_at"] = body["ended_at"]

    with pytest.raises(
        release_gate_module.EvidenceError,
        match="remote_reader_soak_manifest_generated_after_soak_start",
    ):
        release_gate_module._check_remote_reader_soak(body, **context)


def test_remote_reader_soak_v4_schema_is_byte_pinned_in_the_critical_bom():
    relative = release_gate_module.REMOTE_READER_SOAK_HOST_SCHEMA
    path = release_gate_module.REPO_ROOT / relative

    assert relative in release_gate_module._required_critical_files(
        release_gate_module.REPO_ROOT
    )
    assert hashlib.sha256(path.read_bytes()).hexdigest() == (
        release_gate_module.REMOTE_READER_SOAK_SCHEMA_SHA256
    )


def test_production_missing_remote_reader_soak_is_a_hard_no_go(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "production")
    (settings.evidence_dir / "remote_reader_soak.json").unlink()

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("production"),
        now=NOW,
    )

    assert report["ok"] is False
    assert "remote_reader_soak_missing" in report["blockers"]


def test_production_bootstrap_defers_soaks_until_post_launch(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "production_bootstrap")
    for filename in (
        "shadow_soak.json",
        "remote_reader_soak.json",
        release_gate_module.REMOTE_READER_SOAK_MANIFEST_FILENAME,
        release_gate_module.REMOTE_READER_WORKLOAD_CENSUS_FILENAME,
        release_gate_module.REMOTE_READER_DOMAIN_MAPPING_FILENAME,
        release_gate_module.REMOTE_READER_DOMAIN_MAPPING_APPROVAL_FILENAME,
        release_gate_module.REMOTE_READER_WORKLOAD_EXPORT_RECEIPT_FILENAME,
    ):
        (settings.evidence_dir / filename).unlink()

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("production_bootstrap"),
        now=NOW,
    )

    checks = {item["name"]: item for item in report["checks"]}
    for name in ("shadow_soak", "remote_reader_soak"):
        assert checks[name]["ok"] is True
        assert checks[name]["detail"] == {
            "status": "deferred_to_post_launch",
            "blocks_bootstrap_release": False,
            "required_for_steady_capacity": True,
        }
    assert "shadow_soak_missing" not in report["blockers"]
    assert "remote_reader_soak_missing" not in report["blockers"]
    workload = checks["remote_reader_workload_provenance"]
    assert workload["ok"] is True
    assert workload["detail"] == {
        "status": "deferred_to_post_launch",
        "blocks_bootstrap_release": False,
        "balanced_case_target": 200,
    }


@pytest.mark.parametrize(
    ("mutation", "blocker"),
    [
        (
            lambda path: path.unlink(),
            "remote_reader_soak_manifest_missing",
        ),
        (
            lambda path: _write_json(
                path,
                json.loads(path.read_text(encoding="utf-8")),
            ),
            "remote_reader_soak_manifest_not_canonical",
        ),
        (
            lambda path: path.chmod(0o644),
            "remote_reader_soak_manifest_unsafe_file",
        ),
    ],
)
def test_production_requires_secure_canonical_remote_reader_soak_manifest(
    tmp_path,
    mutation,
    blocker,
):
    consumer, dispatcher, settings = _gate(tmp_path, "production")
    path = (
        settings.evidence_dir
        / release_gate_module.REMOTE_READER_SOAK_MANIFEST_FILENAME
    )
    mutation(path)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("production"),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


@pytest.mark.parametrize(
    ("filename", "artifact"),
    [
        (
            release_gate_module.REMOTE_READER_WORKLOAD_CENSUS_FILENAME,
            "remote_reader_workload_census",
        ),
        (
            release_gate_module.REMOTE_READER_DOMAIN_MAPPING_FILENAME,
            "remote_reader_domain_mapping",
        ),
        (
            release_gate_module.REMOTE_READER_DOMAIN_MAPPING_APPROVAL_FILENAME,
            "remote_reader_domain_mapping_approval",
        ),
        (
            release_gate_module.REMOTE_READER_WORKLOAD_EXPORT_RECEIPT_FILENAME,
            "remote_reader_workload_export_receipt",
        ),
    ],
)
@pytest.mark.parametrize(
    ("mutation", "blocker_suffix"),
    [
        (lambda path: path.unlink(), "missing"),
        (
            lambda path: _write_json(
                path,
                json.loads(path.read_text(encoding="utf-8")),
            ),
            "not_canonical",
        ),
        (lambda path: path.chmod(0o644), "unsafe_file"),
    ],
)
def test_production_requires_secure_canonical_workload_provenance(
    tmp_path,
    filename,
    artifact,
    mutation,
    blocker_suffix,
):
    consumer, dispatcher, settings = _gate(tmp_path, "production")
    mutation(settings.evidence_dir / filename)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("production"),
        now=NOW,
    )

    assert report["ok"] is False
    assert f"{artifact}_{blocker_suffix}" in report["blockers"]


@pytest.mark.parametrize(
    ("filename", "mutation", "blocker"),
    [
        (
            release_gate_module.REMOTE_READER_WORKLOAD_CENSUS_FILENAME,
            lambda body: body["statistics"].update(snapshot_stable=False),
            "remote_reader_workload_census_snapshot_invalid",
        ),
        (
            release_gate_module.REMOTE_READER_WORKLOAD_CENSUS_FILENAME,
            lambda body: body["source"].update(component_commit="f" * 40),
            "remote_reader_workload_census_source_mismatch",
        ),
        (
            release_gate_module.REMOTE_READER_DOMAIN_MAPPING_FILENAME,
            lambda body: body["rules"][2].update(
                option_ids=["driving", "unknown"],
                option_path=["driving", "UNKNOWN"],
            ),
            "remote_reader_domain_mapping_rules_invalid",
        ),
        (
            release_gate_module.REMOTE_READER_DOMAIN_MAPPING_APPROVAL_FILENAME,
            lambda body: body.update(mapping_rules_sha256="f" * 64),
            "remote_reader_domain_mapping_approval_mismatch",
        ),
        (
            release_gate_module.REMOTE_READER_WORKLOAD_EXPORT_RECEIPT_FILENAME,
            lambda body: body["manifest"].update(body_sha256="f" * 64),
            "remote_reader_workload_export_receipt_manifest_mismatch",
        ),
        (
            release_gate_module.REMOTE_READER_WORKLOAD_EXPORT_RECEIPT_FILENAME,
            lambda body: body["selection"].update(
                eligible_reference_candidates=201
            ),
            "remote_reader_workload_export_receipt_selection_invalid",
        ),
    ],
)
def test_production_rejects_tampered_workload_provenance(
    tmp_path,
    filename,
    mutation,
    blocker,
):
    consumer, dispatcher, settings = _gate(tmp_path, "production")
    path = settings.evidence_dir / filename
    body = json.loads(path.read_text(encoding="utf-8"))
    mutation(body)
    _write_remote_soak_manifest(path, body)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("production"),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


def test_workload_provenance_pass_detail_is_commit_and_owner_bound(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "production")

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("production"),
        now=NOW,
    )

    check = next(
        item
        for item in report["checks"]
        if item["name"] == "remote_reader_workload_provenance"
    )
    assert check["ok"] is True
    assert check["detail"]["approved_by"] == "data-owner-fixture"
    assert check["detail"]["case_count"] == 200
    assert check["detail"]["exporter_commit"] == _git(
        settings.host_repo_root, "rev-parse", "HEAD"
    )
    assert check["detail"]["exporter_module_sha256"] == hashlib.sha256(
        (
            settings.host_repo_root
            / release_gate_module.REMOTE_READER_WORKLOAD_EXPORT_MODULE
        ).read_bytes()
    ).hexdigest()


@pytest.mark.parametrize(
    ("mutation", "blocker"),
    [
        (
            lambda wrapper: wrapper["receipt"].update(status="failed"),
            "canary_remote_read_not_completed",
        ),
        (
            lambda wrapper: wrapper.update(reader_fingerprint="f" * 64),
            "canary_remote_read_fingerprint_mismatch",
        ),
        (
            lambda wrapper: wrapper["receipt"].update(mdi_download_attempted=True),
            "canary_remote_read_mdi_invoked",
        ),
        (
            lambda wrapper: wrapper["receipt"]["topic_coverage"].pop(
                "vehicle_signal_highfreq"
            ),
            "canary_remote_read_topic_coverage_incomplete",
        ),
        (
            lambda wrapper: wrapper["receipt"].update(
                requested_window={
                    "mode": "bounded",
                    "start_time_ns": 1,
                    "end_time_ns": 2,
                }
            ),
            "canary_remote_read_requested_scope_mismatch",
        ),
        (
            lambda wrapper: wrapper["receipt"].update(completeness="best_effort"),
            "canary_remote_read_completeness_mismatch",
        ),
        (
            lambda wrapper: wrapper["receipt"]["limits"].update(timeout_seconds=121),
            "canary_remote_read_limits_mismatch",
        ),
        (
            lambda wrapper: wrapper["receipt"]["references"][0].update(exhausted=False),
            "canary_remote_read_reference_not_exhausted",
        ),
        (
            lambda wrapper: wrapper["receipt"]["references"][0]["scope_proof"].update(
                limit_policy="truncate"
            ),
            "canary_remote_read_scope_proof_invalid",
        ),
        (
            lambda wrapper: wrapper["receipt"]["derived_stream_cache"].update(
                observed_file_mode="0600"
            ),
            "canary_remote_stream_cache_invalid",
        ),
        (
            lambda wrapper: wrapper["receipt"]["derived_stream_cache"].update(
                credentials_present=True
            ),
            "canary_remote_stream_cache_invalid",
        ),
        (
            lambda wrapper: wrapper["receipt"]["derived_stream_cache"][
                "mount_evidence"
            ].update(fstype="ext4"),
            "canary_remote_stream_cache_invalid",
        ),
        (
            lambda wrapper: wrapper["receipt"]["derived_stream_cache"][
                "mount_evidence"
            ].update(mount_source="//hfs.minieye.tech/wrong-share"),
            "canary_remote_stream_cache_invalid",
        ),
        (
            lambda wrapper: wrapper["receipt"]["derived_stream_cache"][
                "mount_evidence"
            ].update(mount_namespace="invalid"),
            "canary_remote_stream_cache_invalid",
        ),
    ],
)
def test_production_canary_receipt_requires_complete_remote_read_evidence(
    tmp_path, mutation, blocker
):
    consumer, dispatcher, settings = _gate(tmp_path, "production")
    _manifest, path, _sources = _committed_pair_paths(
        settings.evidence_dir, "primary"
    )
    body = json.loads(path.read_text(encoding="utf-8"))
    mutation(body["remote_read"])
    if (
        body["remote_read"]["reader_fingerprint"]
        == _remote_reader_health()["reader_fingerprint"]
    ):
        body["remote_read"]["receipt_sha256"] = _sha256_json(
            body["remote_read"]["receipt"]
        )
    _rewrite_committed_pair_body(
        settings.evidence_dir, "primary", "receipt", body
    )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("production"),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


@pytest.mark.parametrize(
    ("mutation", "blocker"),
    [
        (
            lambda body: body.update(
                alternate_input={"mode": "mdi", "allow_download": True}
            ),
            "canary_receipt_shape_invalid",
        ),
        (
            lambda body: body["outbox"].update(manual_submission=True),
            "canary_receipt_outbox_shape_invalid",
        ),
        (
            lambda body: body["report"].update(unverified_asset="index-copy.html"),
            "canary_receipt_report_shape_invalid",
        ),
        (
            lambda body: body["report"]["browser_smoke"].update(
                alternate_runtime_log=[]
            ),
            "canary_receipt_browser_smoke_shape_invalid",
        ),
    ],
)
def test_production_canary_rejects_unknown_or_contradictory_fields(
    tmp_path, mutation, blocker
):
    consumer, dispatcher, settings = _gate(tmp_path, "production")
    _manifest, path, _sources = _committed_pair_paths(
        settings.evidence_dir, "primary"
    )
    body = json.loads(path.read_text(encoding="utf-8"))
    mutation(body)
    _rewrite_committed_pair_body(
        settings.evidence_dir, "primary", "receipt", body
    )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("production"),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


@pytest.mark.parametrize(
    ("mutation", "rehash_meter", "blocker"),
    [
        (
            lambda body: body["pipeline"].update(status="remote_read_completed"),
            False,
            "canary_pipeline_not_completed",
        ),
        (
            lambda body: body["pipeline"].update(alternate_input={"mode": "mdi"}),
            False,
            "canary_pipeline_shape_invalid",
        ),
        (
            lambda body: body["pipeline"]["remote_read_receipt"].update(
                sha256="f" * 64
            ),
            False,
            "canary_pipeline_remote_read_receipt_mismatch",
        ),
        (
            lambda body: body["pipeline"]["remote_stream_cache"].update(
                sha256="f" * 64
            ),
            False,
            "canary_pipeline_remote_stream_cache_mismatch",
        ),
        (
            lambda body: body["pipeline"]["downstream_stage_receipts"].pop("s5"),
            False,
            "canary_pipeline_downstream_stages_incomplete",
        ),
        (
            lambda body: body["pipeline"]["downstream_stage_receipts"]["s3b"].update(
                status="skipped"
            ),
            False,
            "canary_pipeline_downstream_stage_invalid",
        ),
        (
            lambda body: body["pipeline"]["capacity_usage"].update(within_budget=False),
            False,
            "canary_pipeline_capacity_usage_mismatch",
        ),
        (
            lambda body: body["capacity_meter"]["receipt"]["identity"].update(fence=2),
            True,
            "canary_stage_capacity_identity_mismatch",
        ),
        (
            lambda body: body["capacity_meter"]["receipt"].update(
                schema_version="g1q3_rca_stage_capacity_meter_v1"
            ),
            True,
            "canary_stage_capacity_schema_unsupported",
        ),
        (
            lambda body: body["capacity_meter"]["receipt"]["accounting"].update(
                hfs_root="/mnt/tmp/other-task/cases/G1Q3-1"
            ),
            True,
            "canary_stage_capacity_accounting_invalid",
        ),
        (
            lambda body: body["capacity_meter"]["receipt"]["stages"].pop(
                "s45_auto_keyframe"
            ),
            True,
            "canary_stage_capacity_stages_incomplete",
        ),
        (
            lambda body: body["capacity_meter"]["receipt"]["stages"]["s3b_translate"][
                "peak_delta_bytes"
            ].update(tmp=1_000_000_001),
            True,
            "canary_stage_capacity_budget_exceeded",
        ),
        (
            lambda body: body["capacity_meter"]["receipt"]["baseline"].update(
                created_at="2026-07-10T07:58:00+00:00"
            ),
            True,
            "canary_stage_capacity_timestamps_not_monotonic",
        ),
        (
            lambda body: body["capacity_meter"]["receipt"]["stages"][
                "s5_alignment"
            ].update(
                observed_bytes={"tmp": 200_000_000, "hfs": 200_000_000},
                delta_bytes={"tmp": 200_000_000, "hfs": 200_000_000},
                peak_delta_bytes={
                    "tmp": 200_000_000,
                    "hfs": 200_000_000,
                },
            ),
            True,
            "canary_stage_capacity_peak_regressed",
        ),
        (
            lambda body: body["capacity_meter"]["receipt"]["stages"][
                "s2_remote_read"
            ].update(
                observed_bytes={"tmp": 99_000_000, "hfs": 0},
                delta_bytes={"tmp": 99_000_000, "hfs": 0},
                peak_delta_bytes={"tmp": 99_000_000, "hfs": 0},
            ),
            True,
            "canary_stage_capacity_remote_cache_unmetered",
        ),
        (
            lambda body: body["capacity_meter"]["receipt"]["terminal"][
                "tmp_cache"
            ].update(cleanup_policy="never_cleanup"),
            True,
            "canary_stage_capacity_terminal_invalid",
        ),
        (
            lambda body: body["capacity_meter"]["receipt"]["stages"][
                "s2_remote_read"
            ].update(artifact_receipt_path="/tmp/outside/receipt.json"),
            True,
            "canary_pipeline_artifact_path_invalid",
        ),
        (
            lambda body: body["capacity_meter"]["receipt"].update(within_budget=False),
            False,
            "canary_stage_capacity_hash_mismatch",
        ),
    ],
)
def test_production_canary_requires_full_downstream_and_stage_capacity_evidence(
    tmp_path, mutation, rehash_meter, blocker
):
    consumer, dispatcher, settings = _gate(tmp_path, "production")
    _manifest, path, _sources = _committed_pair_paths(
        settings.evidence_dir, "primary"
    )
    body = json.loads(path.read_text(encoding="utf-8"))
    mutation(body)
    if rehash_meter:
        body["capacity_meter"]["sha256"] = _sha256_json(
            body["capacity_meter"]["receipt"]
        )
    _rewrite_committed_pair_body(
        settings.evidence_dir, "primary", "receipt", body
    )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("production"),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


@pytest.mark.parametrize(
    ("mutation", "rehash_service", "blocker"),
    [
        (
            lambda body: body["vm"]["execution_plane"].update(agent_backend="openclaw"),
            False,
            "canary_vm_execution_plane_mismatch",
        ),
        (
            lambda body: body["vm"]["execution_plane"].update(
                coding_agent_fallback_enabled=True,
                fallback_invocation_count=1,
            ),
            False,
            "canary_vm_execution_plane_mismatch",
        ),
        (
            lambda body: body["vm"]["execution_plane"]["argv"].append(
                "--agent-fallback"
            ),
            False,
            "canary_vm_execution_plane_mismatch",
        ),
        (
            lambda body: body["vm"]["execution_attestation"].pop("available"),
            False,
            "canary_vm_execution_attestation_shape_invalid",
        ),
        (
            lambda body: body["vm"]["execution_attestation"].update(available=False),
            False,
            "canary_vm_execution_attestation_invalid",
        ),
        (
            lambda body: body["vm"]["execution_attestation"].update(
                task_id="wrong-task"
            ),
            False,
            "canary_vm_execution_attestation_invalid",
        ),
        (
            lambda body: body["vm"]["execution_attestation"].update(worker_pid=1),
            False,
            "canary_vm_execution_attestation_invalid",
        ),
        (
            lambda body: body["vm"]["execution_attestation"].update(
                run_id="forged-run-id"
            ),
            False,
            "canary_vm_dispatch_receipt_hash_mismatch",
        ),
        (
            _mutate_worker_run_id_with_valid_dispatch_hash,
            False,
            "canary_vm_dispatch_receipt_binding_mismatch",
        ),
        (
            lambda body: body["vm"].update(dispatch_receipt_sha256="f" * 64),
            False,
            "canary_vm_dispatch_receipt_binding_mismatch",
        ),
        (
            lambda body: body["vm"]["worker_result"].update(sha256="f" * 64),
            False,
            "canary_vm_worker_result_hash_mismatch",
        ),
        (
            _mutate_embedded_worker_attestation,
            False,
            "canary_vm_worker_result_payload_invalid",
        ),
        (
            _mutate_worker_result_state,
            False,
            "canary_vm_worker_result_not_completed",
        ),
        (
            _mutate_worker_contract_hash,
            False,
            "canary_vm_worker_result_payload_invalid",
        ),
        (
            lambda body: body["vm"]["worker_result"].update(
                path="/tmp/forged/local-result.json"
            ),
            False,
            "canary_vm_worker_result_path_mismatch",
        ),
        (
            lambda body: body["vm"]["execution_attestation"].update(
                openclaw_invocation_count=1
            ),
            False,
            "canary_vm_execution_attestation_invalid",
        ),
        (
            lambda body: body["vm"]["execution_attestation"].update(
                worker_source_commit="f" * 40
            ),
            False,
            "canary_vm_execution_attestation_invalid",
        ),
        (
            lambda body: body["vm"]["execution_attestation"].update(
                worker_tree_clean=False
            ),
            False,
            "canary_vm_execution_attestation_invalid",
        ),
        (
            lambda body: body["vm"]["execution_attestation"].update(
                worker_entrypoint_sha256="not-a-sha256"
            ),
            False,
            "canary_vm_execution_attestation_invalid",
        ),
        (
            lambda body: body["vm"]["execution_attestation"].update(
                worker_entrypoint_sha256="f" * 64
            ),
            False,
            "canary_vm_worker_entrypoint_hash_mismatch",
        ),
        (
            lambda body: body["vm"]["execution_attestation"]["argv"].append(
                "--agent-fallback"
            ),
            False,
            "canary_vm_execution_attestation_invalid",
        ),
        (
            lambda body: body["vm"]["execution_attestation"].update(
                dispatched_at="2026-07-09T00:00:00+00:00",
                process_started_at="2026-07-09T00:00:01+00:00",
            ),
            False,
            "canary_vm_dispatch_receipt_hash_mismatch",
        ),
        (
            lambda body: body["vm"]["execution_attestation"].update(
                dispatched_at="2026-07-10T07:57:01+00:00",
                process_started_at="2026-07-10T07:57:00+00:00",
            ),
            False,
            "canary_vm_execution_attestation_timeline_invalid",
        ),
        (
            lambda body: body["vm"]["service_result"]["receipt"].update(
                request_sha256="f" * 64
            ),
            True,
            "canary_vm_service_result_not_completed",
        ),
        (
            lambda body: body["vm"]["service_result"]["receipt"].update(
                worker_run_id="worker-run-from-another-execution"
            ),
            True,
            "canary_vm_service_result_not_completed",
        ),
        (
            lambda body: body["vm"]["service_result"]["receipt"].update(
                worker_pid=9999
            ),
            True,
            "canary_vm_service_result_not_completed",
        ),
        (
            lambda body: body["vm"]["service_result"]["receipt"].update(
                storage_mode="local_filesystem"
            ),
            True,
            "canary_vm_service_storage_contract_invalid",
        ),
        (
            lambda body: body["vm"]["service_result"]["receipt"][
                "mount_evidence"
            ].update(mount_source="//hfs.minieye.tech/wrong-share"),
            True,
            "canary_vm_service_storage_contract_invalid",
        ),
        (
            _mutate_service_mount_identity,
            True,
            "canary_vm_remote_storage_mount_binding_mismatch",
        ),
        (
            lambda body: body["vm"]["service_result"]["receipt"][
                "request_storage"
            ].update(credentials_present=True),
            True,
            "canary_vm_request_storage_invalid",
        ),
        (
            lambda body: body["vm"]["service_result"]["receipt"]["request_storage"][
                "mount_evidence"
            ].update(file_mode="0600"),
            True,
            "canary_vm_request_storage_invalid",
        ),
        (
            lambda body: body["vm"]["service_result"]["receipt"].update(
                goal_sha256="f" * 64
            ),
            True,
            "canary_vm_service_goal_hash_mismatch",
        ),
        (
            lambda body: body["vm"]["service_result"]["receipt"][
                "service_provenance"
            ].update(vm_source_commit="f" * 40),
            True,
            "canary_vm_service_provenance_invalid",
        ),
        (
            lambda body: body["vm"]["service_result"]["receipt"][
                "service_provenance"
            ].update(vm_tree_clean=False),
            True,
            "canary_vm_service_provenance_invalid",
        ),
        (
            lambda body: body["vm"]["service_result"]["receipt"][
                "service_provenance"
            ].update(service_entrypoint_sha256="f" * 64),
            True,
            "canary_vm_service_provenance_invalid",
        ),
        (
            lambda body: body["vm"]["service_result"]["receipt"].update(
                status="failed", success=False
            ),
            True,
            "canary_vm_service_result_not_completed",
        ),
        (
            lambda body: body["vm"]["service_result"]["receipt"].update(
                output_dir="/tmp/outside"
            ),
            True,
            "canary_vm_service_result_not_completed",
        ),
        (
            lambda body: body["vm"]["service_result"]["receipt"].update(
                generated_at="2026-07-09T00:00:00+00:00"
            ),
            True,
            "canary_vm_service_result_timeline_invalid",
        ),
        (
            lambda body: body["vm"]["service_result"]["receipt"].update(
                generated_at="2026-07-10T07:56:59+00:00"
            ),
            True,
            "canary_vm_service_result_timeline_invalid",
        ),
        (
            lambda body: body["vm"]["service_result"]["receipt"].update(
                goal_sha256="e" * 64
            ),
            False,
            "canary_vm_service_result_hash_mismatch",
        ),
    ],
)
def test_production_canary_requires_fixed_cli_without_agent_fallback(
    tmp_path, mutation, rehash_service, blocker
):
    consumer, dispatcher, settings = _gate(tmp_path, "production")
    _manifest, path, _sources = _committed_pair_paths(
        settings.evidence_dir, "primary"
    )
    body = json.loads(path.read_text(encoding="utf-8"))
    mutation(body)
    if rehash_service:
        body["vm"]["service_result"]["sha256"] = _sha256_json(
            body["vm"]["service_result"]["receipt"]
        )
    _rewrite_committed_pair_body(
        settings.evidence_dir, "primary", "receipt", body
    )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("production"),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


def test_report_is_secret_free_and_fingerprint_is_deterministic(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "production")

    first = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("production"),
        now=NOW,
    )
    second = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("production"),
        now=NOW + timedelta(seconds=1),
    )
    serialized = json.dumps(first, sort_keys=True)

    assert first["fingerprint"] == second["fingerprint"]
    assert first["evaluated_at"] != second["evaluated_at"]
    assert SECRET not in serialized
    assert "password" not in serialized.lower()


def test_redacted_config_loader_never_serializes_env_password(tmp_path):
    env = _consumer_env(tmp_path, "shadow") | _dispatcher_env(tmp_path, "shadow")
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(f"{key}={value}" for key, value in env.items()) + "\n",
        encoding="utf-8",
    )
    env_file.chmod(0o600)

    consumer, dispatcher = load_redacted_configs(
        env_file,
        environment={},
        hermes_home=tmp_path,
    )

    public = json.dumps({
        "consumer": consumer.public_dict(),
        "dispatcher": dispatcher.public_dict(),
    })
    assert SECRET not in public
    assert "password" not in public.lower()


def test_release_config_loader_ignores_untracked_shell_overrides_by_default(
    tmp_path, monkeypatch
):
    env = _consumer_env(tmp_path, "shadow") | _dispatcher_env(tmp_path, "shadow")
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(f"{key}={value}" for key, value in env.items()) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("HERMES_RCA_KAFKA_TOPIC", "shell-injected-topic")
    monkeypatch.setenv("PYTHONPATH", "/untracked/cache")

    consumer, _dispatcher = load_redacted_configs(
        env_file,
        hermes_home=tmp_path,
    )

    assert consumer.topic == TOPIC


def test_release_config_loader_does_not_interpolate_shell_values(tmp_path, monkeypatch):
    env = _consumer_env(tmp_path, "shadow") | _dispatcher_env(tmp_path, "shadow")
    env["HERMES_RCA_KAFKA_PASSWORD"] = "${INJECTED_SECRET}"
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(f"{key}={value}" for key, value in env.items()) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("INJECTED_SECRET", "ambient-secret-must-not-win")

    consumer, _dispatcher = load_redacted_configs(
        env_file,
        hermes_home=tmp_path,
    )

    assert consumer.password == "${INJECTED_SECRET}"


def test_receipt_write_is_atomic_and_mode_0600(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "shadow")
    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("shadow"),
        now=NOW,
    )
    receipt = tmp_path / "receipts" / "release.json"

    write_receipt_atomic(receipt, report)

    assert json.loads(receipt.read_text(encoding="utf-8"))["ok"] is True
    assert os.stat(receipt).st_mode & 0o777 == 0o600
    assert list(receipt.parent.glob(".release.json.*.tmp")) == []


def test_production_capacity_limits_must_cover_dispatcher_batch(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "production")
    path = settings.evidence_dir / "capacity_receipt.json"
    body = json.loads(path.read_text(encoding="utf-8"))
    body["queue_limits"]["max_batch_size"] = 9
    _write_json(path, body)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("production"),
        now=NOW,
    )

    assert report["ok"] is False
    assert "capacity_dispatcher_batch_exceeds_limit" in report["blockers"]


@pytest.mark.parametrize(
    ("mutation", "blocker"),
    [
        (
            lambda storage: storage.update(days_horizon_at_assumed_cases_per_day=0.715),
            "capacity_horizon_inconsistent",
        ),
        (
            lambda storage: storage["policy"].update(assumed_cases_per_day=199),
            "capacity_assumed_cases_per_day_below_target",
        ),
        (
            lambda storage: storage["policy"].update(assumed_cases_per_day=201),
            "capacity_assumed_cases_per_day_config_mismatch",
        ),
        (
            lambda storage: storage["policy"].update(
                expected_derived_artifact_bytes_per_case=1_000_000
            ),
            "capacity_artifact_cache_size_config_mismatch",
        ),
        (
            lambda storage: storage.update(capacity_scope="input_mcap"),
            "capacity_scope_mismatch",
        ),
        (
            lambda storage: storage["policy"].update(
                input_materialization_bytes_per_case=1
            ),
            "capacity_input_materialization_not_zero",
        ),
        (
            lambda storage: storage["policy"].update(input_materialization="allowed"),
            "capacity_input_materialization_not_forbidden",
        ),
        (
            lambda storage: storage.update(max_additional_cases=3),
            "capacity_max_additional_cases_inconsistent",
        ),
        (
            lambda storage: storage.update(
                observed_at=(NOW - timedelta(hours=2)).isoformat()
            ),
            "storage_admission_stale",
        ),
        (
            lambda storage: storage.update(schema_version="weak-storage-receipt"),
            "capacity_storage_admission_schema_mismatch",
        ),
    ],
)
def test_production_requires_real_storage_admission_evidence(
    tmp_path, mutation, blocker
):
    consumer, dispatcher, settings = _gate(tmp_path, "production")
    path = settings.evidence_dir / "capacity_receipt.json"
    body = json.loads(path.read_text(encoding="utf-8"))
    mutation(body["storage_admission"])
    _write_json(path, body)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("production"),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


def test_production_capacity_fixture_uses_derived_artifact_cache_budget(
    tmp_path,
):
    consumer, dispatcher, settings = _gate(tmp_path, "production")

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("production"),
        now=NOW,
    )

    capacity = next(
        check for check in report["checks"] if check["name"] == "capacity_receipt"
    )
    assert capacity["ok"] is True
    assert capacity["detail"]["capacity_scope"] == "derived_artifact_and_cache"
    assert capacity["detail"]["input_materialization_bytes_per_case"] == 0
    assert capacity["detail"]["expected_derived_artifact_bytes_per_case"] == (
        1_000_000_000
    )
    assert capacity["detail"]["required_bytes_total"] == 13_000_000_000
    assert capacity["detail"]["capacity_horizon_days"] >= 7.0
    assert (
        capacity["detail"]["queue_limit_provenance"]["reservation_mode"] == "forbidden"
    )


def test_formula_consistent_live_vm_capacity_horizon_still_blocks_production(
    tmp_path,
):
    consumer, dispatcher, settings = _gate(tmp_path, "production")
    path = settings.evidence_dir / "capacity_receipt.json"
    body = json.loads(path.read_text(encoding="utf-8"))
    storage = body["storage_admission"]
    storage["target"] = _storage_target(
        total_bytes=10 * 1024**4,
        available_bytes=7 * 1024**4,
    )
    storage["max_additional_cases"] = storage["target"]["max_additional_cases"]
    storage["days_horizon_at_assumed_cases_per_day"] = storage["target"][
        "days_horizon_at_assumed_cases_per_day"
    ]
    _write_json(path, body)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("production"),
        now=NOW,
    )

    assert storage["days_horizon_at_assumed_cases_per_day"] < 7.0
    assert report["ok"] is False
    assert "capacity_horizon_insufficient" in report["blockers"]


@pytest.mark.parametrize(
    ("mutation", "blocker"),
    [
        (
            lambda storage: storage["policy"].update(requested_cases=1),
            "capacity_requested_cases_config_mismatch",
        ),
        (
            lambda storage: storage["policy"].pop("requested_cases_scope"),
            "capacity_requested_cases_scope_mismatch",
        ),
        (
            lambda storage: storage["policy"].update(input_unit="GB"),
            "capacity_input_unit_mismatch",
        ),
        (
            lambda storage: storage["policy"].update(gb_definition_bytes=1024**3),
            "capacity_gb_definition_mismatch",
        ),
        (
            lambda storage: storage.update(required_bytes_total=12_000_000_000),
            "capacity_required_bytes_total_formula_mismatch",
        ),
        (
            lambda storage: storage["target"].update(
                required_bytes=storage["target"]["required_bytes"] + 1
            ),
            "capacity_storage_target_formula_mismatch",
        ),
        (
            lambda storage: storage["target"].update(
                reserve_bytes=storage["target"]["reserve_bytes"] - 1
            ),
            "capacity_storage_target_formula_mismatch",
        ),
        (
            lambda storage: storage["target"].update(
                max_additional_cases=(storage["target"]["max_additional_cases"] + 1)
            ),
            "capacity_storage_target_formula_mismatch",
        ),
        (
            lambda storage: storage["target"].update(
                days_horizon_at_assumed_cases_per_day=(
                    storage["target"]["days_horizon_at_assumed_cases_per_day"] + 0.001
                )
            ),
            "capacity_storage_target_formula_mismatch",
        ),
    ],
)
def test_production_rejects_forged_storage_capacity_math(tmp_path, mutation, blocker):
    consumer, dispatcher, settings = _gate(tmp_path, "production")
    path = settings.evidence_dir / "capacity_receipt.json"
    body = json.loads(path.read_text(encoding="utf-8"))
    mutation(body["storage_admission"])
    _write_json(path, body)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("production"),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


@pytest.mark.parametrize(
    ("mutation", "blocker"),
    [
        (
            lambda body: body["queue_limits"].pop("source"),
            "invalid_evidence_format",
        ),
        (
            lambda body: body["queue_limits"]["source"].update(
                scheduler_evidence_sha256="f" * 64
            ),
            "capacity_scheduler_evidence_hash_mismatch",
        ),
        (
            lambda body: body["scheduler_evidence"].update(
                capacity_enforcement="best_effort"
            ),
            "capacity_scheduler_admission_not_enforced",
        ),
        (
            lambda body: body["scheduler_evidence"]["queue_limits"].update(
                max_inflight=5
            ),
            "capacity_scheduler_queue_limits_mismatch",
        ),
        (
            lambda body: body.update(reservation_evidence={"state": "active"}),
            "capacity_legacy_reservation_evidence_present",
        ),
        (
            lambda body: body["queue_limits"]["source"].update(
                reservation_evidence_sha256="f" * 64
            ),
            "capacity_queue_source_shape_invalid",
        ),
        (
            lambda body: body["scheduler_evidence"]["source"].update(
                generation_mode="manual"
            ),
            "capacity_evidence_not_machine_generated",
        ),
    ],
)
def test_production_queue_limits_require_scheduler_capacity_evidence(
    tmp_path, mutation, blocker
):
    consumer, dispatcher, settings = _gate(tmp_path, "production")
    path = settings.evidence_dir / "capacity_receipt.json"
    body = json.loads(path.read_text(encoding="utf-8"))
    mutation(body)
    _write_json(path, body)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("production"),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


@pytest.mark.parametrize(
    ("mutation", "blocker"),
    [
        (
            lambda receipt: receipt.update(outbox={"ok": True}),
            "canary_receipt_outbox_shape_invalid",
        ),
        (
            lambda receipt: receipt["vm"].update(submission_key="wrong-key"),
            "canary_receipt_submission_key_mismatch",
        ),
        (
            lambda receipt: receipt["vm"].update(task_id="wrong-task"),
            "canary_receipt_vm_task_mismatch",
        ),
        (
            lambda receipt: receipt["outbox"].update(status="pending"),
            "canary_receipt_outbox_not_completed",
        ),
        (
            lambda receipt: receipt["report"].update(index_html={"ok": True}),
            "canary_receipt_report_file_shape_invalid",
        ),
        (
            lambda receipt: receipt["report"].update(
                artifact_policy="legacy_interactive_html"
            ),
            "canary_receipt_html_artifact_policy_mismatch",
        ),
        (
            lambda receipt: receipt["report"]["browser_smoke"].update(
                unmanifested_request_count=1
            ),
            "canary_receipt_browser_smoke_failed",
        ),
        (
            lambda receipt: receipt["report"]["browser_smoke"].update(
                index_html_sha256="f" * 64
            ),
            "canary_receipt_browser_smoke_failed",
        ),
        (
            lambda receipt: receipt["report"].update(
                report_url=REPORT_URL.replace(SUBMISSION_KEY, "wrong-submission")
            ),
            "canary_receipt_report_url_mismatch",
        ),
        (
            lambda receipt: receipt["delivery"].update(
                report_url=REPORT_URL.replace(
                    ARTIFACT_SET_ID,
                    f"g1q3-rca-artifact-v1-{'2' * 64}",
                )
            ),
            "canary_receipt_report_url_mismatch",
        ),
        (
            lambda receipt: receipt["report"]["browser_smoke"].update(
                report_url=REPORT_URL.replace("index.html", "other.html")
            ),
            "canary_receipt_browser_smoke_failed",
        ),
        (
            lambda receipt: receipt["delivery"].update(marker=EFFECT_KEY),
            "canary_receipt_delivery_marker_invalid",
        ),
        (
            lambda receipt: receipt["delivery"].update(
                artifact_set_id="wrong-artifact-set"
            ),
            "canary_receipt_artifact_set_mismatch",
        ),
        (
            lambda receipt: receipt["delivery"].update(remote_receipt={}),
            "canary_receipt_delivery_remote_receipt_shape_invalid",
        ),
        (
            lambda receipt: receipt["delivery"]["remote_receipt"].update(
                confirmed_field_keys=["field_9193cb"]
            ),
            "canary_receipt_delivery_result_fields_unconfirmed",
        ),
    ],
)
def test_canary_receipt_rejects_weak_or_cross_submission_evidence(
    tmp_path, mutation, blocker
):
    consumer, dispatcher, settings = _gate(tmp_path, "production")
    _manifest, path, _sources = _committed_pair_paths(
        settings.evidence_dir, "primary"
    )
    receipt = json.loads(path.read_text(encoding="utf-8"))
    mutation(receipt)
    _rewrite_committed_pair_body(
        settings.evidence_dir, "primary", "receipt", receipt
    )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("production"),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


@pytest.mark.parametrize(
    ("mutation", "blocker"),
    [
        (
            lambda receipt: receipt["admission"].update(business_key="forged"),
            "canary_receipt_admission_invalid",
        ),
        (
            lambda receipt: receipt["derived_capacity_lifecycle"].update(
                full_receipts={}
            ),
            "canary_derived_capacity_full_receipts_invalid",
        ),
        (
            lambda receipt: receipt["derived_capacity_lifecycle"]["full_receipts"][
                "reserved"
            ].update(contract_sha256="f" * 64),
            "canary_derived_capacity_reserved_receipt_invalid",
        ),
        (
            lambda receipt: receipt["derived_capacity_lifecycle"]["full_receipts"][
                "activate"
            ].update(fence=2),
            "canary_derived_capacity_activate_receipt_invalid",
        ),
        (
            lambda receipt: receipt["derived_capacity_lifecycle"]["full_receipts"][
                "activate"
            ]["reservation"].update(run_id="other-run"),
            "canary_derived_capacity_activate_receipt_invalid",
        ),
        (
            lambda receipt: receipt["derived_capacity_lifecycle"]["full_receipts"][
                "release"
            ]["reservation"].update(held_bytes={"tmp": 1, "hfs": 0, "total": 1}),
            "canary_derived_capacity_release_receipt_invalid",
        ),
        (
            lambda receipt: receipt["derived_capacity_lifecycle"]["audit"].update(
                terminal_proven=False
            ),
            "canary_derived_capacity_audit_invalid",
        ),
        (
            lambda receipt: receipt["outbox"].update(reserved_receipt_sha256="f" * 64),
            "canary_receipt_reserved_capacity_stage_mismatch",
        ),
        (
            lambda receipt: receipt["vm"].update(capacity_lifecycle_sha256="f" * 64),
            "canary_receipt_vm_capacity_lifecycle_mismatch",
        ),
    ],
)
def test_canary_receipt_requires_bound_terminal_capacity_lifecycle(
    tmp_path, mutation, blocker
):
    consumer, dispatcher, settings = _gate(tmp_path, "production")
    _manifest, path, _sources = _committed_pair_paths(
        settings.evidence_dir, "primary"
    )
    receipt = json.loads(path.read_text(encoding="utf-8"))
    mutation(receipt)
    _rewrite_committed_pair_body(
        settings.evidence_dir, "primary", "receipt", receipt
    )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("production"),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


@pytest.mark.parametrize(
    ("mutation", "blocker"),
    [
        (
            lambda plan: plan["admission"].update(submission_key="forged"),
            "canary_plan_admission_invalid",
        ),
        (
            lambda plan: plan["execution_request"]["source_refs"].update(offset=11),
            "remote_request_source_refs_mismatch",
        ),
        (
            lambda plan: plan["execution_request"]["work_item"].update(
                work_item_id="7041712813"
            ),
            "remote_request_work_item_identity_mismatch",
        ),
    ],
)
def test_canary_plan_binds_admission_to_request_and_kafka_coordinates(
    tmp_path, mutation, blocker
):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    path = settings.evidence_dir / "canary_plan.json"
    plan = json.loads(path.read_text(encoding="utf-8"))
    mutation(plan)
    _write_json(path, plan)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


@pytest.mark.parametrize(
    ("mutation", "blocker"),
    [
        (
            lambda manifest: manifest["critical_files"].update({
                "gateway/pnc_rca_admission.py": "0" * 64
            }),
            "build_manifest_critical_hash_mismatch",
        ),
        (
            lambda manifest: manifest["critical_files"].update({
                "../outside.py": "0" * 64
            }),
            "build_manifest_critical_path_invalid",
        ),
        (
            lambda manifest: manifest["critical_files"].pop(
                "gateway/pnc_rca_admission.py"
            ),
            "build_manifest_core_file_missing",
        ),
        (
            lambda manifest: manifest["release_bom"]["components"]["vm"].update(
                commit="short"
            ),
            "build_manifest_commit_invalid",
        ),
        (
            lambda manifest: manifest["release_bom"]["components"].pop("vm_worker"),
            "build_manifest_components_invalid",
        ),
    ],
)
def test_build_manifest_rejects_hash_path_and_weak_manifest(
    tmp_path, mutation, blocker
):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    path = settings.evidence_dir / "build_manifest.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    mutation(manifest)
    manifest["release_bom_sha256"] = _sha256_json(manifest["release_bom"])
    _write_json(path, manifest)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


def test_build_manifest_recomputes_relevant_tree_cleanliness(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    relative = "gateway/pnc_rca_admission.py"
    critical_path = settings.host_repo_root / relative
    critical_path.write_text("dirty release content\n", encoding="utf-8")
    manifest_path = settings.evidence_dir / "build_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["critical_files"][relative] = hashlib.sha256(
        critical_path.read_bytes()
    ).hexdigest()
    _write_json(manifest_path, manifest)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert "build_manifest_relevant_tree_dirty" in report["blockers"]


@pytest.mark.parametrize("component", ["workspace", "vm", "vm_worker"])
def test_build_manifest_rejects_live_commit_mismatch(tmp_path, component):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    path = settings.evidence_dir / "build_manifest.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    manifest["release_bom"]["components"][component]["commit"] = "f" * 40
    manifest["release_bom_sha256"] = _sha256_json(manifest["release_bom"])
    _write_json(path, manifest)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    blocker = (
        "build_manifest_workspace_closure_cross_commit"
        if component == "workspace"
        else f"build_manifest_{component}_commit_mismatch"
    )
    assert blocker in report["blockers"]


@pytest.mark.parametrize("component", ["vm", "vm_worker"])
def test_build_manifest_rejects_entrypoint_hash_mismatch(tmp_path, component):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    path = settings.evidence_dir / "build_manifest.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    manifest["release_bom"]["components"][component]["entrypoint_sha256"] = "f" * 64
    manifest["release_bom_sha256"] = _sha256_json(manifest["release_bom"])
    _write_json(path, manifest)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert f"build_manifest_{component}_entrypoint_hash_mismatch" in report["blockers"]


@pytest.mark.parametrize("component", ["vm", "vm_worker"])
@pytest.mark.parametrize(
    ("field", "blocker_suffix"),
    [
        ("tree", "entrypoint_provenance_mismatch"),
        ("entrypoint_blob", "entrypoint_provenance_mismatch"),
        ("entrypoint_git_mode", "entrypoint_provenance_mismatch"),
        ("entrypoint_committed_sha256", "entrypoint_hash_mismatch"),
    ],
)
def test_build_manifest_binds_vm_commit_tree_blob_mode_and_content(
    tmp_path,
    component,
    field,
    blocker_suffix,
):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    path = settings.evidence_dir / "build_manifest.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    declared = manifest["release_bom"]["components"][component]
    current = declared[field]
    if field == "entrypoint_git_mode":
        declared[field] = "100755" if current == "100644" else "100644"
    else:
        declared[field] = ("e" if not str(current).startswith("e") else "f") * len(
            str(current)
        )
    manifest["release_bom_sha256"] = _sha256_json(manifest["release_bom"])
    _write_json(path, manifest)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert f"build_manifest_{component}_{blocker_suffix}" in report["blockers"]


@pytest.mark.parametrize("component", ["host", "vm", "vm_worker"])
def test_build_manifest_rejects_dirty_live_repository(tmp_path, component):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    repo = {
        "host": settings.host_repo_root,
        "workspace": settings.workspace_repo_root,
        "vm": Path(settings.vm_repo_root),
        "vm_worker": Path(settings.vm_worker_repo_root),
    }[component]
    (repo / "untracked.txt").write_text("dirty\n", encoding="utf-8")

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert f"build_manifest_{component}_tree_dirty" in report["blockers"]


def test_build_manifest_allows_and_records_unscoped_workspace_drift(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    (settings.workspace_repo_root / "unscoped-dirty.txt").write_text(
        "unrelated terminal work\n",
        encoding="utf-8",
    )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is True, report["blockers"]
    detail = next(
        check["detail"]
        for check in report["checks"]
        if check["name"] == "build_manifest"
    )
    drift = detail["workspace_governance"]["unscoped_drift"]
    assert drift["classification"] == "DRIFT-PREEXISTING"
    assert drift["dirty_count"] == 1
    assert drift["status_sha256"] != EMPTY_GIT_STATUS_SHA256
    assert drift["blocking"] is False


def test_build_manifest_rejects_dirty_workspace_execution_closure(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    closure_path = (
        settings.workspace_repo_root
        / release_gate_module.WORKSPACE_EXECUTION_CLOSURE_RELATIVE_PATHS[0]
    )
    closure_path.write_text("changed execution code\n", encoding="utf-8")

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert "build_manifest_workspace_closure_dirty" in report["blockers"]


@pytest.mark.parametrize("kind", ["symlink", "untracked"])
def test_build_manifest_rejects_non_tracked_regular_workspace_closure(
    tmp_path,
    kind,
):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    relative = release_gate_module.WORKSPACE_EXECUTION_CLOSURE_RELATIVE_PATHS[1]
    closure_path = settings.workspace_repo_root / relative
    if kind == "symlink":
        closure_path.unlink()
        closure_path.symlink_to(settings.workspace_repo_root / "tracked.txt")
    else:
        _git(settings.workspace_repo_root, "rm", "--cached", "--", relative)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert "build_manifest_workspace_closure_dirty" in report["blockers"]


def test_workspace_execution_closure_accepts_clean_three_file_candidate(tmp_path):
    workspace, commit = _create_git_repo(tmp_path, "workspace-build")

    provenance = release_gate_module._workspace_git_provenance(workspace)

    closure = provenance["execution_closure"]
    assert provenance["commit"] == commit
    assert closure["commit"] == commit
    assert closure["relative_paths"] == list(
        release_gate_module.WORKSPACE_EXECUTION_CLOSURE_RELATIVE_PATHS
    )
    assert set(closure["files"]) == {
        "bin/create_task_v2.py",
        "bin/shared_state_v2.py",
        "bin/shared_state_fields.py",
    }
    assert all(item["commit"] == commit for item in closure["files"].values())


@pytest.mark.parametrize("drift", ["changed", "untracked", "symlink"])
def test_workspace_execution_closure_rejects_shared_state_fields_drift(
    tmp_path,
    drift,
):
    workspace, _commit = _create_git_repo(tmp_path, "workspace-build")
    relative = "bin/shared_state_fields.py"
    shared_state_fields = workspace / relative
    if drift == "changed":
        shared_state_fields.write_text("changed actor contract\n", encoding="utf-8")
    elif drift == "untracked":
        _git(workspace, "rm", "--cached", "--", relative)
    else:
        shared_state_fields.unlink()
        shared_state_fields.symlink_to(workspace / "tracked.txt")

    with pytest.raises(EvidenceError) as error:
        release_gate_module._workspace_git_provenance(workspace)

    assert error.value.code == "build_manifest_workspace_closure_dirty"


@pytest.mark.parametrize(
    ("drift", "expected_blocker"),
    [
        ("cross_commit", "build_manifest_workspace_closure_cross_commit"),
        ("hash", "workspace_runtime_workspace_closure_mismatch"),
    ],
)
def test_build_manifest_rejects_workspace_closure_binding_drift(
    tmp_path, drift, expected_blocker
):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    path = settings.evidence_dir / "build_manifest.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    workspace = manifest["release_bom"]["components"]["workspace"]
    closure = workspace["execution_closure"]
    relative = release_gate_module.WORKSPACE_EXECUTION_CLOSURE_RELATIVE_PATHS[0]
    if drift == "cross_commit":
        closure["files"][relative]["commit"] = "f" * 40
    else:
        closure["files"][relative]["sha256"] = "f" * 64
    workspace["execution_closure_sha256"] = _sha256_json(closure)
    manifest["release_bom_sha256"] = _sha256_json(manifest["release_bom"])
    _write_json(path, manifest)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert expected_blocker in report["blockers"]


@pytest.mark.parametrize(
    "dependency",
    ["ssh_mini_agent", "vm_ssh_execution_protocol_v2"],
)
def test_build_manifest_rejects_external_execution_dependency_drift(
    tmp_path,
    dependency,
):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    path = settings.evidence_dir / "build_manifest.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    manifest["release_bom"]["external_dependencies"][dependency]["sha256"] = (
        "f" * 64
    )
    manifest["release_bom_sha256"] = _sha256_json(manifest["release_bom"])
    _write_json(path, manifest)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert f"build_manifest_external_{dependency}_drift" in report["blockers"]


@pytest.mark.parametrize("unsafe_identity", ["symlink", "mode"])
def test_external_execution_dependency_lstat_identity_is_fail_closed(
    tmp_path,
    unsafe_identity,
):
    target = tmp_path / "wrapper"
    target.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    target.chmod(0o700)
    observed = target
    if unsafe_identity == "symlink":
        observed = tmp_path / "wrapper-link"
        observed.symlink_to(target)
    else:
        target.chmod(0o755)

    with pytest.raises(EvidenceError) as error:
        release_gate_module._external_dependency_observation(
            "test_wrapper",
            {"path": observed, "mode": 0o700},
        )

    assert error.value.code == "build_manifest_external_test_wrapper_invalid"


@pytest.mark.parametrize(
    ("field", "value", "blocker"),
    [
        (
            "runtime_config_sha256",
            "e" * 64,
            "build_manifest_runtime_config_mismatch",
        ),
        (
            "launchd_config_sha256",
            "e" * 64,
            "build_manifest_launchd_config_mismatch",
        ),
    ],
)
def test_build_manifest_rejects_config_fingerprint_mismatch(
    tmp_path, field, value, blocker
):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    path = settings.evidence_dir / "build_manifest.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    manifest["release_bom"][field] = value
    manifest["release_bom_sha256"] = _sha256_json(manifest["release_bom"])
    _write_json(path, manifest)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


def test_build_manifest_rejects_release_bom_tamper(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    path = settings.evidence_dir / "build_manifest.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    manifest["release_bom"]["components"]["vm"]["commit"] = "e" * 40
    _write_json(path, manifest)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert "build_manifest_release_bom_hash_mismatch" in report["blockers"]


def test_build_manifest_provenance_verifier_failure_blocks(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")

    def unavailable(_settings):
        raise EvidenceError("build_manifest_vm_probe_unavailable")

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
        build_provenance_verifier=unavailable,
    )

    assert report["ok"] is False
    assert "build_manifest_vm_probe_unavailable" in report["blockers"]


@pytest.mark.parametrize(
    ("helper", "blocker"),
    [
        (
            "_revalidate_workspace_runtime_release_binding",
            "workspace_runtime_live_identity_mismatch",
        ),
        (
            "_revalidate_future_runtime_release_binding",
            "future_runtime_stage_manifest_live_mismatch",
        ),
    ],
)
def test_build_manifest_revalidates_staged_runtime_fail_closed(
    tmp_path,
    monkeypatch,
    helper,
    blocker,
):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")

    def rejected(_binding):
        raise EvidenceError(blocker)

    monkeypatch.setattr(release_gate_module, helper, rejected)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


def test_live_build_provenance_uses_fixed_bounded_vm_probe(tmp_path):
    _consumer, _dispatcher, settings = _gate(tmp_path, "canary")
    vm_commit = _git(Path(settings.vm_repo_root), "rev-parse", "HEAD")
    vm_worker_commit = _git(Path(settings.vm_worker_repo_root), "rev-parse", "HEAD")
    captured = []

    def runner(command, **kwargs):
        captured.append((command, kwargs))
        repo_root = (
            settings.vm_worker_repo_root
            if settings.vm_worker_repo_root in kwargs["input"]
            else settings.vm_repo_root
        )
        payload = {
            "schema_version": VM_GIT_PROVENANCE_SCHEMA_VERSION,
            "source": "ssh-mini-agent",
            "repo_root": repo_root,
            "commit": (
                vm_worker_commit
                if repo_root == settings.vm_worker_repo_root
                else vm_commit
            ),
            "tree_clean": True,
            "status_sha256": EMPTY_GIT_STATUS_SHA256,
            "stable": True,
            **_vm_probe_entrypoint(
                repo_root,
                worker=repo_root == settings.vm_worker_repo_root,
            ),
        }
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps(payload),
            stderr="",
        )

    result = verify_live_build_provenance(settings, runner=runner)

    assert result["schema_version"] == BUILD_PROVENANCE_SCHEMA_VERSION
    assert result["host"]["commit"] == _git(
        settings.host_repo_root, "rev-parse", "HEAD"
    )
    assert result["workspace"]["commit"] == _git(
        settings.workspace_repo_root, "rev-parse", "HEAD"
    )
    assert result["vm"]["commit"] == vm_commit
    assert result["vm_worker"]["commit"] == vm_worker_commit
    assert len(captured) == 2
    for command, kwargs in captured:
        assert command == [
            str(release_gate_module.DEFAULT_SSH_MINI_AGENT),
            "run_py_json",
        ]
        assert kwargs["timeout"] == release_gate_module.VM_PROVENANCE_TIMEOUT_SECONDS
        assert (
            release_gate_module.VM_PROVENANCE_TIMEOUT_SECONDS
            > release_gate_module.VM_PROVENANCE_GIT_TIMEOUT_SECONDS
        )
        assert (
            f"GIT_TIMEOUT_SECONDS = "
            f"{release_gate_module.VM_PROVENANCE_GIT_TIMEOUT_SECONDS}"
            in kwargs["input"]
        )
        assert kwargs["input"].count("timeout=GIT_TIMEOUT_SECONDS") == 2
        assert 'git("rev-parse", "--show-toplevel")' in kwargs["input"]
        assert (
            'git("status", "--porcelain=v1", "--untracked-files=all")'
            in kwargs["input"]
        )
        assert '["git", "-c", "core.fileMode=false"' in kwargs["input"]
        assert 'git_bytes("cat-file", "blob"' in kwargs["input"]
    assert settings.vm_repo_root in captured[0][1]["input"]
    assert settings.vm_worker_repo_root in captured[1][1]["input"]


def test_vm_git_provenance_ignores_only_cifs_filemode_noise(tmp_path):
    relative = "api/g1q3_rca/scripts/run_rca_service_request.py"
    repo, _commit = _create_git_repo(
        tmp_path,
        "vm-cifs-mode-noise",
        entrypoint_relative=relative,
    )
    entrypoint = repo / relative
    entrypoint.chmod(0o755)

    strict_status = _git(
        repo,
        "-c",
        "core.fileMode=true",
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
    )
    assert relative in strict_status

    result = release_gate_module._vm_git_provenance(
        str(repo),
        component="vm",
        runner=_execute_vm_probe_locally,
    )

    assert result["tree_clean"] is True
    assert result["status_sha256"] == EMPTY_GIT_STATUS_SHA256
    assert result["entrypoint_git_mode"] == "100644"
    assert result["entrypoint_sha256"] == result["entrypoint_committed_sha256"]


def test_vm_git_provenance_rejects_real_content_drift_with_filemode_ignored(tmp_path):
    relative = "api/g1q3_rca/scripts/run_rca_service_request.py"
    repo, _commit = _create_git_repo(
        tmp_path,
        "vm-cifs-content-drift",
        entrypoint_relative=relative,
    )
    entrypoint = repo / relative
    entrypoint.chmod(0o755)
    entrypoint.write_text("changed content\n", encoding="utf-8")

    with pytest.raises(EvidenceError) as error:
        release_gate_module._vm_git_provenance(
            str(repo),
            component="vm",
            runner=_execute_vm_probe_locally,
        )

    assert error.value.code == "build_manifest_vm_tree_dirty"


@pytest.mark.parametrize(
    ("stable", "tree_clean", "status_sha256", "blocker"),
    [
        (
            True,
            False,
            hashlib.sha256(b"?? dirty.txt").hexdigest(),
            "build_manifest_vm_tree_dirty",
        ),
        (
            False,
            True,
            EMPTY_GIT_STATUS_SHA256,
            "build_manifest_vm_repo_unstable",
        ),
    ],
)
def test_live_build_provenance_rejects_untrustworthy_vm_repository(
    tmp_path, stable, tree_clean, status_sha256, blocker
):
    _consumer, _dispatcher, settings = _gate(tmp_path, "canary")
    vm_commit = _git(Path(settings.vm_repo_root), "rev-parse", "HEAD")

    def runner(command, **_kwargs):
        payload = {
            "schema_version": VM_GIT_PROVENANCE_SCHEMA_VERSION,
            "source": "ssh-mini-agent",
            "repo_root": settings.vm_repo_root,
            "commit": vm_commit,
            "tree_clean": tree_clean,
            "status_sha256": status_sha256,
            "stable": stable,
            **_vm_probe_entrypoint(settings.vm_repo_root, worker=False),
        }
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps(payload),
            stderr="",
        )

    with pytest.raises(EvidenceError) as error:
        verify_live_build_provenance(settings, runner=runner)

    assert error.value.code == blocker


def test_live_build_provenance_rejects_dirty_vm_worker_repository(tmp_path):
    _consumer, _dispatcher, settings = _gate(tmp_path, "canary")

    def runner(command, **kwargs):
        is_worker = settings.vm_worker_repo_root in kwargs["input"]
        repo_root = settings.vm_worker_repo_root if is_worker else settings.vm_repo_root
        payload = {
            "schema_version": VM_GIT_PROVENANCE_SCHEMA_VERSION,
            "source": "ssh-mini-agent",
            "repo_root": repo_root,
            "commit": _git(Path(repo_root), "rev-parse", "HEAD"),
            "tree_clean": not is_worker,
            "status_sha256": (
                hashlib.sha256(b"?? dirty-worker.py").hexdigest()
                if is_worker
                else EMPTY_GIT_STATUS_SHA256
            ),
            "stable": True,
            **_vm_probe_entrypoint(repo_root, worker=is_worker),
        }
        return subprocess.CompletedProcess(
            command, 0, stdout=json.dumps(payload), stderr=""
        )

    with pytest.raises(EvidenceError) as error:
        verify_live_build_provenance(settings, runner=runner)

    assert error.value.code == "build_manifest_vm_worker_tree_dirty"


@pytest.mark.parametrize(
    ("runner", "blocker"),
    [
        (
            lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("offline")),
            "build_manifest_vm_probe_unavailable",
        ),
        (
            lambda command, **_kwargs: subprocess.CompletedProcess(
                command, 0, stdout="not-json", stderr=""
            ),
            "build_manifest_vm_probe_invalid",
        ),
    ],
)
def test_live_build_provenance_fails_closed_on_bad_vm_probe(tmp_path, runner, blocker):
    _consumer, _dispatcher, settings = _gate(tmp_path, "canary")

    with pytest.raises(EvidenceError) as error:
        verify_live_build_provenance(settings, runner=runner)

    assert error.value.code == blocker


@pytest.mark.parametrize(
    ("mutation", "blocker"),
    [
        (
            lambda body: body.pop("projected_storage_horizon_days"),
            "invalid_evidence_format",
        ),
        (
            lambda body: body.update(projected_storage_horizon_days=29),
            "shadow_soak_storage_horizon_insufficient",
        ),
        (
            lambda body: body.update(projected_db_growth_bytes_per_day=1),
            "shadow_soak_projected_growth_understated",
        ),
        (
            lambda body: body.update(duration_seconds=3_600),
            "shadow_soak_too_short",
        ),
        (
            lambda body: body.update(records_committed=999),
            "shadow_soak_commit_count_mismatch",
        ),
        (
            lambda body: body.update(assigned_partitions=[0]),
            "shadow_soak_partition_coverage_mismatch",
        ),
        (
            lambda body: body.update(rebalance_callback_errors=1),
            "shadow_soak_rebalance_callback_errors_present",
        ),
        (
            lambda body: body["decision_counts"].update(accepted=0, filtered=980),
            "shadow_soak_accepted_volume_insufficient",
        ),
        (
            lambda body: body.update(consumer_lag_end=1),
            "shadow_soak_lag_recovery_failed",
        ),
        (
            lambda body: body["ingest_commit_latency_ms"].update(p95=1_001.0),
            "shadow_soak_latency_slo_failed",
        ),
        (
            lambda body: body.update(config_sha256="0" * 64),
            "shadow_soak_config_mismatch",
        ),
    ],
)
def test_canary_requires_bounded_control_store_growth(tmp_path, mutation, blocker):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    path = settings.evidence_dir / "shadow_soak.json"
    body = json.loads(path.read_text(encoding="utf-8"))
    mutation(body)
    _write_json(path, body)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


def test_canary_blocks_when_legacy_auto_execution_is_not_explicitly_disabled(
    tmp_path,
):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=CutoverConfig(None, 0, False),
        now=NOW,
    )

    assert report["ok"] is False
    assert "legacy_auto_execution_disable_not_explicit" in report["blockers"]


@pytest.mark.parametrize(
    ("field", "value", "blocker"),
    [
        ("data_access_mode", "mdi_download", "cutover_plan_data_access_mode_mismatch"),
        ("mdi_download_allowed", True, "cutover_plan_mdi_download_allowed"),
        (
            "input_materialization",
            "required",
            "cutover_plan_input_materialization_not_forbidden",
        ),
        (
            "legacy_storage_reservation_enabled",
            True,
            "cutover_plan_legacy_storage_reservation_enabled",
        ),
        (
            "derived_capacity_atomic_reservation",
            False,
            "cutover_plan_derived_capacity_reservation_missing",
        ),
        ("legacy_daily_quota", 1, "cutover_plan_legacy_quota_not_zero"),
        (
            "legacy_governance_download_enabled",
            True,
            "cutover_plan_legacy_download_still_enabled",
        ),
    ],
)
def test_canary_cutover_plan_forbids_every_legacy_download_path(
    tmp_path, field, value, blocker
):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    path = settings.evidence_dir / "cutover_plan.json"
    body = json.loads(path.read_text(encoding="utf-8"))
    body[field] = value
    _write_json(path, body)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


def test_canary_dispatcher_must_be_remote_read_without_legacy_reservation(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    dispatcher = replace(dispatcher, data_access_mode="mdi_download")

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert "dispatcher_data_access_mode_mismatch" in report["blockers"]


def test_canary_dispatcher_requires_fixed_input_wait_horizon(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    dispatcher = replace(dispatcher, input_wait_max_age_seconds=3_600)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert "dispatcher_input_wait_horizon_mismatch" in report["blockers"]


def test_canary_blocks_when_delivery_services_are_not_explicitly_enabled(
    tmp_path,
):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=CutoverConfig(True, 0, False, None, False),
        now=NOW,
    )

    assert report["ok"] is False
    assert "delivery_collector_enable_not_explicit" in report["blockers"]


def _write_terminal_manual_canary(
    tmp_path: Path,
    settings: ReleaseGateSettings,
    *,
    operator_policy_enabled: bool,
    source_mode: str = "run_or_join",
) -> tuple[object, str]:
    from scripts.pnc_rca_canary_collector import (
        CanaryReceiptCollector,
        write_collection,
    )
    from tests.scripts.test_pnc_rca_canary_collector import (
        _terminal_manual_fixture,
    )

    _reset_terminal_fixture_database(tmp_path)
    config, reader, source_id = _terminal_manual_fixture(tmp_path)
    with sqlite3.connect(config.control_db_path) as connection:
        requester_id = connection.execute(
            "SELECT requester_id FROM rca_trigger_sources WHERE source_id = ?",
            (source_id,),
        ).fetchone()[0]
        connection.execute(
            "UPDATE rca_trigger_sources SET mode = ? WHERE source_id = ?",
            (source_mode, source_id),
        )
    receipt_path = next(config.group_binding_receipt_dir.glob("*.jsonl"))
    record = json.loads(receipt_path.read_text(encoding="utf-8"))
    record["decision_snapshot"]["handoff_contract"]["mode"] = source_mode
    authorization = record["manual_authorization"]
    authorization["debug_requested"] = source_mode == "debug"
    authorization["debug_enabled"] = operator_policy_enabled
    authorization["debug_user_allowlist_sha256"] = _sha256_json(
        [requester_id] if operator_policy_enabled else []
    )
    receipt_path.write_text(
        json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    terminal = CanaryReceiptCollector(
        config,
        remote_reader=reader,
        now=lambda: NOW,
    ).collect_terminal_failure(source_id)
    deployed_collector = settings.host_repo_root / "scripts/pnc_rca_canary_collector.py"
    terminal.provenance["collector"] = {
        "path": str(deployed_collector.resolve()),
        "sha256": hashlib.sha256(deployed_collector.read_bytes()).hexdigest(),
    }
    write_collection(terminal, settings.evidence_dir)
    _publish_committed_pair(
        settings.evidence_dir,
        "manual_terminal_failure",
    )
    return config, str(requester_id)


def _reset_terminal_fixture_database(tmp_path: Path) -> None:
    database = tmp_path / "control.sqlite3"
    for candidate in (
        database,
        Path(f"{database}-wal"),
        Path(f"{database}-shm"),
    ):
        candidate.unlink(missing_ok=True)


@pytest.mark.parametrize(
    ("changes", "blocker"),
    [
        ({}, "manual_operator_enable_not_explicit"),
        (
            {"manual_operator_enabled": True},
            "manual_operator_allowlist_empty",
        ),
        (
            {
                "manual_operator_enabled": True,
                "manual_operator_user_ids": ("ou_operator",),
            },
            "manual_operator_rate_limit_not_explicit",
        ),
        (
            {
                "manual_operator_enabled": True,
                "manual_operator_user_ids": ("ou_operator",),
                "manual_operator_rate_limit": 3,
            },
            "manual_operator_rate_window_not_explicit",
        ),
    ],
)
def test_production_manual_operator_policy_must_be_explicit(tmp_path, changes, blocker):
    consumer, dispatcher, settings = _gate(tmp_path, "production")
    cutover = replace(
        _cutover("production"),
        manual_intake_enabled=True,
        **changes,
    )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=cutover,
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


def test_production_manual_intake_requires_terminal_failure_thread_receipt(
    tmp_path,
):
    consumer, dispatcher, settings = _gate(tmp_path, "production")

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=replace(
            _cutover("production"),
            manual_intake_enabled=True,
            manual_chat_ids=(release_gate_module.G1Q3_RCA_GROUP_ID,),
            manual_operator_enabled=False,
        ),
        now=NOW,
    )

    assert report["ok"] is False
    assert "manual_terminal_failure_canary_commit_missing" in report["blockers"]
    check = next(
        item
        for item in report["checks"]
        if item["name"] == "manual_terminal_failure_delivery"
    )
    assert check == {
        "name": "manual_terminal_failure_delivery",
        "ok": False,
        "code": "manual_terminal_failure_canary_commit_missing",
        "detail": {},
    }


def test_production_manual_intake_accepts_real_terminal_failure_canary(
    tmp_path,
):
    from scripts.pnc_rca_canary_collector import (
        CanaryReceiptCollector,
        write_collection,
    )
    from tests.scripts.test_pnc_rca_canary_collector import (
        _terminal_manual_fixture,
    )

    consumer, dispatcher, settings = _gate(tmp_path, "production")
    _reset_terminal_fixture_database(tmp_path)
    config, reader, source_id = _terminal_manual_fixture(tmp_path)
    terminal = CanaryReceiptCollector(
        config,
        remote_reader=reader,
        now=lambda: NOW,
    ).collect_terminal_failure(source_id)
    deployed_collector = settings.host_repo_root / "scripts/pnc_rca_canary_collector.py"
    terminal.provenance["collector"] = {
        "path": str(deployed_collector.resolve()),
        "sha256": hashlib.sha256(deployed_collector.read_bytes()).hexdigest(),
    }
    write_collection(terminal, settings.evidence_dir)
    _publish_committed_pair(
        settings.evidence_dir,
        "manual_terminal_failure",
    )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=replace(
            _cutover("production"),
            manual_intake_enabled=True,
            manual_chat_ids=config.manual_chat_ids,
            manual_operator_enabled=False,
        ),
        now=NOW,
    )

    check = next(
        item
        for item in report["checks"]
        if item["name"] == "manual_terminal_failure_delivery"
    )
    assert check["ok"] is True
    assert check["detail"]["required"] is True
    assert check["detail"]["required_effects"] == [
        "feishu_issue_comment",
        "feishu_thread_reply",
    ]
    assert check["detail"]["manual_operator_enabled"] is False
    assert check["detail"]["manual_operator_capability"] == "not_open"
    assert check["detail"]["source_mode"] == "run_or_join"
    assert "manual_terminal_failure_topic_receipt_required" not in report["blockers"]


def test_terminal_failure_gate_rereads_immutable_authorization_source(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "production")
    config, _requester_id = _write_terminal_manual_canary(
        tmp_path,
        settings,
        operator_policy_enabled=False,
    )
    receipt_path = next(config.group_binding_receipt_dir.glob("*.jsonl"))
    receipt_path.write_bytes(receipt_path.read_bytes() + b"{}\n")

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=replace(
            _cutover("production"),
            manual_intake_enabled=True,
            manual_chat_ids=config.manual_chat_ids,
            manual_operator_enabled=False,
        ),
        now=NOW,
    )

    assert report["ok"] is False
    assert "canary_authorization_source_hash_mismatch" in report["blockers"]


def test_terminal_failure_rejects_gateway_started_after_authorization(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "production")
    config, _requester_id = _write_terminal_manual_canary(
        tmp_path,
        settings,
        operator_policy_enabled=False,
    )
    receipt = _read_committed_pair_body(
        settings.evidence_dir,
        "manual_terminal_failure",
        "receipt",
    )
    receipt["observed_trigger_source"]["authorization"][
        "gateway_runtime_identity"
    ]["process_create_time"] = NOW.timestamp() + 1
    _rewrite_committed_pair_body(
        settings.evidence_dir,
        "manual_terminal_failure",
        "receipt",
        receipt,
    )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=replace(
            _cutover("production"),
            manual_intake_enabled=True,
            manual_chat_ids=config.manual_chat_ids,
            manual_operator_enabled=False,
        ),
        now=NOW,
    )

    assert report["ok"] is False
    assert (
        "canary_trigger_authorization_process_timeline_invalid"
        in report["blockers"]
    )


def test_terminal_failure_binds_manual_admission_to_active_policy(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "production")
    config, _requester_id = _write_terminal_manual_canary(
        tmp_path,
        settings,
        operator_policy_enabled=False,
    )
    consumer = replace(
        consumer,
        policy=replace(
            consumer.policy,
            project_keys=frozenset({"different-project"}),
        ),
    )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=replace(
            _cutover("production"),
            manual_intake_enabled=True,
            manual_chat_ids=config.manual_chat_ids,
            manual_operator_enabled=False,
        ),
        now=NOW,
    )

    assert report["ok"] is False
    assert "manual_terminal_failure_admission_policy_mismatch" in report["blockers"]


def test_production_manual_operator_requires_authorized_debug_canary(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "production")
    config, requester_id = _write_terminal_manual_canary(
        tmp_path,
        settings,
        operator_policy_enabled=True,
    )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=replace(
            _cutover("production"),
            manual_intake_enabled=True,
            manual_chat_ids=config.manual_chat_ids,
            manual_operator_enabled=True,
            manual_operator_user_ids=(requester_id,),
            manual_operator_rate_limit=3,
            manual_operator_rate_window_seconds=600,
        ),
        now=NOW,
    )

    assert report["ok"] is False
    assert "manual_operator_authorization_canary_required" in report["blockers"]


def test_production_manual_operator_accepts_authorized_debug_canary(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "production")
    config, requester_id = _write_terminal_manual_canary(
        tmp_path,
        settings,
        operator_policy_enabled=True,
        source_mode="debug",
    )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=replace(
            _cutover("production"),
            manual_intake_enabled=True,
            manual_chat_ids=config.manual_chat_ids,
            manual_operator_enabled=True,
            manual_operator_user_ids=(requester_id,),
            manual_operator_rate_limit=3,
            manual_operator_rate_window_seconds=600,
        ),
        now=NOW,
    )

    check = next(
        item
        for item in report["checks"]
        if item["name"] == "manual_terminal_failure_delivery"
    )
    assert check["ok"] is True
    assert check["detail"]["manual_operator_capability"] == "enabled"
    assert check["detail"]["source_mode"] == "debug"
    assert check["detail"]["operator_authorization_proven"] is True
    assert "manual_operator_authorization_canary_required" not in report["blockers"]


def test_production_manual_operator_rejects_allowlist_drift(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "production")
    config, _requester_id = _write_terminal_manual_canary(
        tmp_path,
        settings,
        operator_policy_enabled=True,
        source_mode="debug",
    )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=replace(
            _cutover("production"),
            manual_intake_enabled=True,
            manual_chat_ids=config.manual_chat_ids,
            manual_operator_enabled=True,
            manual_operator_user_ids=("ou_different_operator",),
            manual_operator_rate_limit=3,
            manual_operator_rate_window_seconds=600,
        ),
        now=NOW,
    )

    assert report["ok"] is False
    assert "canary_manual_operator_config_mismatch" in report["blockers"]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("manual_operator_rate_limit", 4),
        ("manual_operator_rate_window_seconds", 601),
    ],
)
def test_production_manual_operator_rejects_rate_config_drift(
    tmp_path, field, value
):
    consumer, dispatcher, settings = _gate(tmp_path, "production")
    config, requester_id = _write_terminal_manual_canary(
        tmp_path,
        settings,
        operator_policy_enabled=True,
        source_mode="debug",
    )
    operator_config = {
        "manual_operator_rate_limit": 3,
        "manual_operator_rate_window_seconds": 600,
    }
    operator_config[field] = value

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=replace(
            _cutover("production"),
            manual_intake_enabled=True,
            manual_chat_ids=config.manual_chat_ids,
            manual_operator_enabled=True,
            manual_operator_user_ids=(requester_id,),
            **operator_config,
        ),
        now=NOW,
    )

    assert report["ok"] is False
    assert "canary_manual_operator_config_mismatch" in report["blockers"]


def test_production_manual_intake_rejects_pre_submit_quarantine_canary(tmp_path):
    from scripts.pnc_rca_canary_collector import (
        CanaryReceiptCollector,
        write_collection,
    )
    from tests.scripts.test_pnc_rca_canary_collector import (
        _quarantined_manual_fixture,
    )

    consumer, dispatcher, settings = _gate(tmp_path, "production")
    config, reader, source_id = _quarantined_manual_fixture(
        tmp_path,
        chat_id=release_gate_module.G1Q3_RCA_GROUP_ID,
    )
    observed = NOW + timedelta(seconds=5)
    terminal = CanaryReceiptCollector(
        config,
        remote_reader=reader,
        now=lambda: observed,
    ).collect_terminal_failure(source_id)
    deployed_collector = settings.host_repo_root / "scripts/pnc_rca_canary_collector.py"
    terminal.provenance["collector"] = {
        "path": str(deployed_collector.resolve()),
        "sha256": hashlib.sha256(deployed_collector.read_bytes()).hexdigest(),
    }
    write_collection(terminal, settings.evidence_dir)
    _publish_committed_pair(
        settings.evidence_dir,
        "manual_terminal_failure",
    )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=replace(
            _cutover("production"),
            manual_intake_enabled=True,
            manual_chat_ids=config.manual_chat_ids,
            manual_operator_enabled=False,
        ),
        now=observed,
    )

    check = next(
        item
        for item in report["checks"]
        if item["name"] == "manual_terminal_failure_delivery"
    )
    assert check["ok"] is False
    assert check["code"] == "manual_terminal_failure_post_submit_required"


@pytest.mark.parametrize(
    ("drift", "blocker"),
    [
        ("path", "manual_terminal_failure_database_invalid"),
        ("inode", "manual_terminal_failure_database_identity_mismatch"),
    ],
)
def test_production_terminal_canary_binds_live_configured_database_identity(
    tmp_path, drift, blocker
):
    from scripts.pnc_rca_canary_collector import (
        CanaryReceiptCollector,
        write_collection,
    )
    from tests.scripts.test_pnc_rca_canary_collector import (
        _terminal_manual_fixture,
    )

    gate_root = tmp_path / "gate"
    terminal_root = tmp_path / "terminal"
    gate_root.mkdir()
    terminal_root.mkdir()
    consumer, dispatcher, settings = _gate(gate_root, "production")
    config, reader, source_id = _terminal_manual_fixture(terminal_root)
    settings = replace(
        settings,
        group_binding_receipt_dir=config.group_binding_receipt_dir,
    )
    dispatcher = replace(
        dispatcher,
        control_db_path=config.control_db_path,
        delivery_db_path=config.delivery_db_path,
    )
    terminal = CanaryReceiptCollector(
        config,
        remote_reader=reader,
        now=lambda: NOW,
    ).collect_terminal_failure(source_id)
    deployed_collector = settings.host_repo_root / "scripts/pnc_rca_canary_collector.py"
    terminal.provenance["collector"] = {
        "path": str(deployed_collector.resolve()),
        "sha256": hashlib.sha256(deployed_collector.read_bytes()).hexdigest(),
    }
    write_collection(terminal, settings.evidence_dir)
    _publish_committed_pair(
        settings.evidence_dir,
        "manual_terminal_failure",
    )
    _manifest_path, _receipt_path, sources_path = _committed_pair_paths(
        settings.evidence_dir,
        "manual_terminal_failure",
    )
    sources = json.loads(sources_path.read_text(encoding="utf-8"))
    if drift == "path":
        sources["control_database"]["path"] = str(
            (tmp_path / "unrelated.sqlite3").resolve()
        )
    else:
        sources["control_database"]["inode"] += 1
    _rewrite_committed_pair_body(
        settings.evidence_dir,
        "manual_terminal_failure",
        "sources",
        sources,
    )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=replace(
            _cutover("production"),
            manual_intake_enabled=True,
            manual_chat_ids=config.manual_chat_ids,
            manual_operator_enabled=False,
        ),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


def test_production_manual_intake_switch_must_be_explicit(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "production")

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=replace(_cutover("production"), manual_intake_enabled=None),
        now=NOW,
    )

    assert report["ok"] is False
    assert "manual_intake_enable_not_explicit" in report["blockers"]


def test_production_manual_intake_cannot_be_disabled(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "production")

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("production"),
        now=NOW,
    )

    assert report["ok"] is False
    assert "manual_intake_required_in_production" in report["blockers"]


def test_production_manual_intake_requires_production_chat(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "production")

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=replace(
            _cutover("production"),
            manual_intake_enabled=True,
            manual_chat_ids=(release_gate_module.PNC_ALL_BUSINESS_TEST_GROUP_ID,),
            manual_operator_enabled=False,
        ),
        now=NOW,
    )

    assert report["ok"] is False
    assert "manual_production_chat_required" in report["blockers"]


def test_production_manual_terminal_canary_must_come_from_production_chat(tmp_path):
    from scripts.pnc_rca_canary_collector import (
        CanaryReceiptCollector,
        write_collection,
    )
    from tests.scripts.test_pnc_rca_canary_collector import (
        _quarantined_manual_fixture,
    )

    consumer, dispatcher, settings = _gate(tmp_path, "production")
    config, reader, source_id = _quarantined_manual_fixture(tmp_path)
    terminal = CanaryReceiptCollector(
        config,
        remote_reader=reader,
        now=lambda: NOW + timedelta(seconds=5),
    ).collect_terminal_failure(source_id)
    deployed_collector = settings.host_repo_root / "scripts/pnc_rca_canary_collector.py"
    terminal.provenance["collector"] = {
        "path": str(deployed_collector.resolve()),
        "sha256": hashlib.sha256(deployed_collector.read_bytes()).hexdigest(),
    }
    write_collection(terminal, settings.evidence_dir)
    _publish_committed_pair(
        settings.evidence_dir,
        "manual_terminal_failure",
    )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=replace(
            _cutover("production"),
            manual_intake_enabled=True,
            manual_chat_ids=(
                release_gate_module.G1Q3_RCA_GROUP_ID,
                release_gate_module.PNC_ALL_BUSINESS_TEST_GROUP_ID,
            ),
            manual_operator_enabled=False,
        ),
        now=NOW + timedelta(seconds=5),
    )

    assert report["ok"] is False
    assert "manual_production_chat_canary_required" in report["blockers"]


def test_manual_success_validator_accepts_independent_formal_manual_origin(
    tmp_path,
    monkeypatch,
):
    admission = build_rca_admission(
        project_key="t03o4q",
        project_simple_name="g1q3",
        work_item_type_key="issue",
        work_item_id="manual-canary-1",
        rule_version=RULE,
        trigger_kind="manual_issue_request",
    ).to_dict()
    source_id = release_gate_module._stable_trigger_source_id(
        "feishu_group_manual", "feishu:om_manual_success"
    )
    source = {
        "source_id": source_id,
        "source_kind": "manual_issue_request",
        "storage_source_kind": "feishu_group_manual",
        "mode": "run_or_join",
        "outcome": "created",
        "binding_role": "origin",
        "generation": 1,
        "chat_id": release_gate_module.G1Q3_RCA_GROUP_ID,
    }
    request = {
        "source_refs": {
            "task_id": admission["submission_key"],
            "source_kind": "feishu_group_manual",
            "origin_source_id": source_id,
            "rule_version": RULE,
            "generation": 1,
            "business_key": admission["business_key"],
            "submission_key": admission["submission_key"],
        }
    }
    detail = {
        "execution_origin": source,
        "observed_trigger_source": source,
        "admission": {
            "business_key": admission["business_key"],
            "submission_key": admission["submission_key"],
            "generation": 1,
        },
        "artifact_set_id": "manual-artifact-set",
        "delivery_obligations": [
            {"effect_kind": "feishu_issue_comment"},
            {"effect_kind": "feishu_thread_reply"},
        ],
    }
    monkeypatch.setattr(
        release_gate_module,
        "validate_canary_receipt",
        lambda *_args, **_kwargs: detail,
    )
    consumer, _dispatcher = _configs(tmp_path, "production")

    result = release_gate_module.validate_manual_success_canary(
        {
            "execution_origin": source,
            "observed_trigger_source": source,
            "admission": admission,
            "execution_request": request,
        },
        expected_manual_chat_ids=(release_gate_module.G1Q3_RCA_GROUP_ID,),
        expected_operator_config=replace(
            _cutover("production"),
            manual_intake_enabled=True,
            manual_chat_ids=(release_gate_module.G1Q3_RCA_GROUP_ID,),
            manual_operator_enabled=False,
        ),
        expected_reader_fingerprint="reader-fingerprint",
        expected_requested_scope={"scope": "manual"},
        expected_vm_commit="1" * 40,
        expected_vm_worker_commit="2" * 40,
        expected_vm_service_entrypoint_sha256="3" * 64,
        expected_vm_worker_entrypoint_sha256="4" * 64,
        expected_rule_version=RULE,
        expected_workflow_policy=consumer.policy.to_dict(),
        now=NOW,
        max_age_seconds=900,
    )

    assert result == detail


def test_canary_validates_manual_terminal_evidence_when_intake_is_enabled(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    config, _requester_id = _write_terminal_manual_canary(
        tmp_path,
        settings,
        operator_policy_enabled=False,
    )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=replace(
            _cutover("canary"),
            manual_intake_enabled=True,
            manual_chat_ids=config.manual_chat_ids,
            manual_operator_enabled=False,
        ),
        now=NOW,
    )

    check = next(
        item
        for item in report["checks"]
        if item["name"] == "manual_terminal_failure_delivery"
    )
    assert check["ok"] is True
    assert check["detail"]["required"] is True


def test_cli_returns_zero_then_two_and_writes_secret_free_receipts(
    tmp_path, monkeypatch, capsys
):
    consumer, dispatcher, settings = _gate(tmp_path, "shadow")
    del consumer, dispatcher
    env = _consumer_env(tmp_path, "shadow") | _dispatcher_env(tmp_path, "shadow")
    env.update({
        "HERMES_RCA_LEGACY_AUTO_EXECUTION_DISABLED": "false",
        "HERMES_G1Q3_AUTO_DOWNLOAD_DAILY_QUOTA": "200",
        "G1Q3_GOVERNANCE_DOWNLOAD_ENABLED": "1",
    })
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(f"{key}={value}" for key, value in env.items()) + "\n",
        encoding="utf-8",
    )
    env_file.chmod(0o600)
    current_observation = datetime.now(timezone.utc).isoformat()
    for evidence_path in settings.evidence_dir.glob("*.json"):
        evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
        evidence["observed_at"] = current_observation
        _write_json(evidence_path, evidence)
    broker_path = settings.evidence_dir / "broker_metadata.json"
    broker = json.loads(broker_path.read_text(encoding="utf-8"))
    _source, env_observation = load_kafka_preflight_environment(env_file)
    broker["collector"]["env_file"] = env_observation
    _write_json(broker_path, broker)
    for key in tuple(os.environ):
        if key.startswith("HERMES_RCA_") or key.startswith("HERMES_G1Q3_"):
            monkeypatch.delenv(key, raising=False)
        monkeypatch.delenv("G1Q3_GOVERNANCE_DOWNLOAD_ENABLED", raising=False)
        monkeypatch.setenv("HERMES_HOME", str(tmp_path))
        receipt = tmp_path / "release-receipt.json"
    argv = [
        "--mode",
        "shadow",
        "--evidence-dir",
        str(settings.evidence_dir),
        "--env-file",
        str(env_file),
        "--expected-topic",
        TOPIC,
        "--expected-rule-version",
        RULE,
        "--host-contract",
        str(settings.host_contract_path),
        "--vm-contract",
        str(settings.vm_contract_path),
        "--receipt",
        str(receipt),
    ]
    runtime_detail = release_gate_module.check_candidate_runtime_dependencies(
        tmp_path / "unused-host-build"
    )
    monkeypatch.setattr(
        release_gate_module,
        "check_candidate_runtime_dependencies",
        lambda _repo_root: runtime_detail,
    )

    assert main(argv) == 0
    first_stdout = capsys.readouterr().out
    assert SECRET not in first_stdout
    assert json.loads(receipt.read_text(encoding="utf-8"))["ok"] is True
    assert os.stat(receipt).st_mode & 0o777 == 0o600

    (settings.evidence_dir / "broker_metadata.json").unlink()
    assert main(argv) == 2
    capsys.readouterr()
    assert json.loads(receipt.read_text(encoding="utf-8"))["ok"] is False


def test_production_cli_requires_receipt_and_red_gate_does_not_reserve_it(
    tmp_path, monkeypatch, capsys
):
    base_args = [
        "--mode",
        "production",
        "--evidence-dir",
        str(tmp_path / "evidence"),
        "--expected-topic",
        TOPIC,
        "--expected-rule-version",
        RULE,
    ]
    assert main(base_args) == 2
    missing_receipt = json.loads(capsys.readouterr().out)
    assert missing_receipt["blockers"] == ["configuration_invalid"]

    consumer, dispatcher = _configs(tmp_path, "production")
    monkeypatch.setattr(
        release_gate_module,
        "load_redacted_configs",
        lambda *_args, **_kwargs: (consumer, dispatcher),
    )
    monkeypatch.setattr(
        release_gate_module,
        "load_cutover_config",
        lambda *_args, **_kwargs: _cutover("production"),
    )
    monkeypatch.setattr(
        release_gate_module,
        "evaluate_release_gate",
        lambda **_kwargs: release_gate_module._configuration_failure(
            mode="production",
            code="capacity_receipt_missing",
            detail="test",
        ),
    )
    receipt = tmp_path / "production-release.json"
    assert main([*base_args, "--receipt", str(receipt)]) == 2
    capsys.readouterr()
    assert not os.path.lexists(receipt)
    assert not os.path.lexists(
        release_gate_module.activation_confirmation_capsule_path(receipt)
    )
    assert not os.path.lexists(
        release_gate_module.activation_confirmation_pair_commit_path(receipt)
    )


def _validate_fixture_canary_receipt(body: dict) -> dict:
    attestation = body["vm"]["execution_attestation"]
    service_provenance = body["vm"]["service_result"]["receipt"]["service_provenance"]
    requested_scope = release_gate_module._check_requested_scope(
        _requested_scope(), field="test.requested_scope"
    )
    return release_gate_module.validate_canary_receipt(
        body,
        expected_execution_origin_id=SOURCE_ID,
        expected_execution_origin_kind="kafka_issue_created",
        expected_observed_source_id=SOURCE_ID,
        expected_observed_source_kind="kafka_issue_created",
        expected_request_sha256=_sha256_execution_request(body["execution_request"]),
        expected_admission=CANARY_ADMISSION.to_dict(),
        expected_reader_fingerprint=_remote_reader_health()["reader_fingerprint"],
        expected_requested_scope=requested_scope,
        expected_vm_commit=service_provenance["vm_source_commit"],
        expected_vm_worker_commit=attestation["worker_source_commit"],
        expected_vm_service_entrypoint_sha256=service_provenance[
            "service_entrypoint_sha256"
        ],
        expected_vm_worker_entrypoint_sha256=attestation["worker_entrypoint_sha256"],
        now=NOW,
        max_age_seconds=900,
    )


def _rehash_lineage(body: dict, short_name: str) -> dict:
    stage = body["pipeline"]["downstream_stage_receipts"][short_name]
    lineage = stage["lineage"]
    lineage["input_artifact_set_sha256"] = canonical_artifact_set_sha256(
        lineage["input_artifacts"]
    )
    lineage["output_artifact_set_sha256"] = canonical_artifact_set_sha256(
        lineage["output_artifacts"]
    )
    stage["artifact_receipt_sha256"] = _sha256_json(lineage)
    return lineage


def _mutate_s3a_remote_cache_lineage(body: dict) -> None:
    lineage = body["pipeline"]["downstream_stage_receipts"]["s3a"]["lineage"]
    lineage["input_artifacts"][0]["sha256"] = "0" * 64
    _rehash_lineage(body, "s3a")


def _mutate_s3b_upstream_lineage(body: dict) -> None:
    lineage = body["pipeline"]["downstream_stage_receipts"]["s3b"]["lineage"]
    lineage["input_artifacts"] = [
        {
            "kind": "unrelated_side_input",
            "path": body["execution_request"]["data"]["artifact_root"]
            + "side-input.json",
            "bytes": 1,
            "sha256": "1" * 64,
        }
    ]
    _rehash_lineage(body, "s3b")


def _mutate_stage_run_identity(body: dict) -> None:
    body["pipeline"]["downstream_stage_receipts"]["s45"]["lineage"]["identity"][
        "run_id"
    ] = "pipeline-self-generated-run"
    _rehash_lineage(body, "s45")


def _mutate_stage_download_policy(body: dict) -> None:
    body["pipeline"]["downstream_stage_receipts"]["s5"]["lineage"]["execution_policy"][
        "allow_download"
    ] = True
    _rehash_lineage(body, "s5")


def _mutate_s6_final_outputs(body: dict) -> None:
    lineage = body["pipeline"]["downstream_stage_receipts"]["s6"]["lineage"]
    lineage["output_artifacts"] = [
        item
        for item in lineage["output_artifacts"]
        if item["kind"] != "delivery_manifest"
    ]
    _rehash_lineage(body, "s6")


@pytest.mark.parametrize(
    ("mutation", "blocker"),
    [
        (
            _mutate_s3a_remote_cache_lineage,
            "canary_stage_lineage_upstream_artifact_missing",
        ),
        (
            _mutate_s3b_upstream_lineage,
            "canary_stage_lineage_upstream_artifact_missing",
        ),
        (_mutate_stage_run_identity, "canary_stage_lineage_identity_mismatch"),
        (
            _mutate_stage_download_policy,
            "canary_stage_lineage_execution_policy_invalid",
        ),
        (_mutate_s6_final_outputs, "canary_stage_lineage_final_output_missing"),
    ],
)
def test_public_canary_validator_rejects_broken_stage_lineage(
    tmp_path, mutation, blocker
):
    _consumer, _dispatcher, settings = _gate(tmp_path, "production")
    body = _read_committed_pair_body(
        settings.evidence_dir,
        "primary",
        "receipt",
    )
    mutation(body)

    with pytest.raises(EvidenceError) as caught:
        _validate_fixture_canary_receipt(body)

    assert caught.value.code == blocker


def test_public_canary_validator_recomputes_manifest_artifact_set(tmp_path):
    _consumer, _dispatcher, settings = _gate(tmp_path, "production")
    body = _read_committed_pair_body(
        settings.evidence_dir,
        "primary",
        "receipt",
    )
    body["report"]["delivery_manifest"]["body"]["sealed_at"] = (
        "2026-07-10T07:57:01+00:00"
    )

    with pytest.raises(EvidenceError) as caught:
        _validate_fixture_canary_receipt(body)

    assert caught.value.code == "canary_receipt_artifact_set_derivation_mismatch"


@pytest.mark.parametrize(
    "legacy_mode", ["minimal_download", "full_download", "mdi_download"]
)
def test_public_canary_validator_rejects_legacy_mode_in_optional_worker_evidence(
    tmp_path, legacy_mode
):
    _consumer, _dispatcher, settings = _gate(tmp_path, "production")
    body = _read_committed_pair_body(
        settings.evidence_dir,
        "primary",
        "receipt",
    )
    worker = body["vm"]["worker_result"]
    worker["receipt"]["result"]["resolved_snapshot"] = {
        "legacy_access_hint": legacy_mode
    }
    worker["sha256"] = _sha256_json(worker["receipt"])

    with pytest.raises(EvidenceError) as caught:
        _validate_fixture_canary_receipt(body)

    assert caught.value.code == "canary_receipt_legacy_download_field_present"


def test_legacy_mode_scanner_does_not_reject_read_only_historical_status_text():
    request = _remote_execution_request()
    request["evidence"]["historical_projection"] = {
        "status": "need_download",
        "note": "Historical minimal_download state is disabled and read-only.",
    }

    assert release_gate_module.find_rca_legacy_download_violation(request) is None
    detail = release_gate_module._check_remote_execution_request(
        request,
        field="test.execution_request",
        expected_admission=CANARY_ADMISSION.to_dict(),
        expected_origin_source_id=SOURCE_ID,
        expected_origin_storage_kind="kafka_workflow_event",
    )
    assert detail["mode"] == "remote_read"


def _set_canary_execution_duration(body: dict, duration_seconds: int) -> None:
    observed_at = datetime.fromisoformat(body["observed_at"])
    completed_at = observed_at - timedelta(seconds=5)
    generated_at = completed_at - timedelta(seconds=1)
    dispatched_at = completed_at - timedelta(seconds=duration_seconds)
    process_started_at = dispatched_at + timedelta(seconds=1)
    attestation = copy.deepcopy(body["vm"]["execution_attestation"])
    attestation["dispatched_at"] = dispatched_at.isoformat()
    attestation["process_started_at"] = process_started_at.isoformat()
    dispatch_receipt = {
        "schema_version": "g1q3_rca_worker_dispatch_receipt_v1",
        "task_id": attestation["task_id"],
        "run_id": attestation["run_id"],
        "argv": attestation["argv"],
        "cwd": attestation["cwd"],
        "dispatched_at": attestation["dispatched_at"],
        "process_started_at": attestation["process_started_at"],
        "worker_pid": attestation["worker_pid"],
    }
    dispatch_sha256 = _sha256_json(dispatch_receipt)
    attestation["dispatch_receipt_sha256"] = dispatch_sha256
    body["vm"]["execution_attestation"] = attestation
    body["vm"]["dispatch_receipt_sha256"] = dispatch_sha256
    worker_wrapper = body["vm"]["worker_result"]
    worker_wrapper["receipt"]["completed_at"] = completed_at.isoformat()
    worker_wrapper["receipt"]["result"]["execution_attestation"] = copy.deepcopy(
        attestation
    )
    worker_wrapper["sha256"] = _sha256_json(worker_wrapper["receipt"])
    service_wrapper = body["vm"]["service_result"]
    service_wrapper["receipt"]["generated_at"] = generated_at.isoformat()
    service_wrapper["receipt"]["dispatch_receipt_sha256"] = dispatch_sha256
    service_wrapper["sha256"] = _sha256_json(service_wrapper["receipt"])


def test_canary_allows_execution_longer_than_evidence_freshness_window(tmp_path):
    _consumer, _dispatcher, settings = _gate(tmp_path, "production")
    body = _read_committed_pair_body(
        settings.evidence_dir,
        "primary",
        "receipt",
    )
    _set_canary_execution_duration(body, 1_800)

    detail = _validate_fixture_canary_receipt(body)

    assert detail["vm_execution"]["execution_duration_seconds"] == 1_800
    assert detail["vm_execution"]["max_execution_duration_seconds"] == 3_600


def test_canary_rejects_execution_over_heavy_lane_budget(tmp_path):
    _consumer, _dispatcher, settings = _gate(tmp_path, "production")
    body = _read_committed_pair_body(
        settings.evidence_dir,
        "primary",
        "receipt",
    )
    _set_canary_execution_duration(body, 3_601)

    with pytest.raises(EvidenceError) as caught:
        _validate_fixture_canary_receipt(body)

    assert caught.value.code == "canary_vm_execution_duration_exceeded"


def test_canary_receipt_freshness_remains_independent_of_execution_budget(tmp_path):
    _consumer, _dispatcher, settings = _gate(tmp_path, "production")
    body = _read_committed_pair_body(
        settings.evidence_dir,
        "primary",
        "receipt",
    )
    body["observed_at"] = (NOW - timedelta(seconds=901)).isoformat()

    with pytest.raises(EvidenceError) as caught:
        _validate_fixture_canary_receipt(body)

    assert caught.value.code == "canary_receipt_stale"


def test_committed_canary_evidence_records_manifest_and_generation_hashes(tmp_path):
    _consumer, _dispatcher, settings = _gate(tmp_path, "production")
    evidence_hashes: dict[str, str] = {}

    detail = release_gate_module._load_committed_canary_evidence(
        settings.evidence_dir,
        "primary",
        evidence_hashes,
    )

    manifest_path, receipt_path, sources_path = _committed_pair_paths(
        settings.evidence_dir,
        "primary",
    )
    assert set(evidence_hashes) == {
        manifest_path.name,
        receipt_path.name,
        sources_path.name,
    }
    assert detail["commit_id"] in receipt_path.name
    assert detail["commit_id"] in sources_path.name
    assert not (settings.evidence_dir / "canary_receipt.json").exists()
    assert not (settings.evidence_dir / "canary_receipt_sources.json").exists()


def test_committed_canary_evidence_never_falls_back_to_legacy_pair(tmp_path):
    _consumer, _dispatcher, settings = _gate(tmp_path, "production")
    manifest_path, receipt_path, sources_path = _committed_pair_paths(
        settings.evidence_dir,
        "primary",
    )
    (settings.evidence_dir / "canary_receipt.json").write_bytes(
        receipt_path.read_bytes()
    )
    (settings.evidence_dir / "canary_receipt_sources.json").write_bytes(
        sources_path.read_bytes()
    )
    (settings.evidence_dir / "canary_receipt.json").chmod(0o600)
    (settings.evidence_dir / "canary_receipt_sources.json").chmod(0o600)
    manifest_path.unlink()

    with pytest.raises(EvidenceError) as caught:
        release_gate_module._load_committed_canary_evidence(
            settings.evidence_dir,
            "primary",
            {},
        )

    assert caught.value.code == "canary_receipt_commit_missing"


@pytest.mark.parametrize(
    ("mutation", "blocker"),
    [
        (
            lambda body: body.update(unexpected=True),
            "canary_receipt_commit_shape_invalid",
        ),
        (
            lambda body: body["files"]["receipt"].update(
                filename="../canary_receipt.json"
            ),
            "canary_receipt_commit_receipt_filename_invalid",
        ),
        (
            lambda body: body.update(receipt_canonical_sha256="0" * 64),
            "canary_receipt_commit_receipt_canonical_mismatch",
        ),
    ],
)
def test_committed_canary_manifest_tamper_fails_closed(
    tmp_path,
    mutation,
    blocker,
):
    _consumer, _dispatcher, settings = _gate(tmp_path, "production")
    manifest_path, _receipt_path, _sources_path = _committed_pair_paths(
        settings.evidence_dir,
        "primary",
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    mutation(manifest)
    _write_json(manifest_path, manifest)
    manifest_path.chmod(0o600)

    with pytest.raises(EvidenceError) as caught:
        release_gate_module._load_committed_canary_evidence(
            settings.evidence_dir,
            "primary",
            {},
        )

    assert caught.value.code == blocker


@pytest.mark.parametrize("attack", ["symlink", "hardlink", "mode"])
def test_committed_canary_generation_rejects_unsafe_file_identity(
    tmp_path,
    attack,
):
    _consumer, _dispatcher, settings = _gate(tmp_path, "production")
    _manifest_path, receipt_path, _sources_path = _committed_pair_paths(
        settings.evidence_dir,
        "primary",
    )
    if attack == "symlink":
        target = tmp_path / "receipt-target.json"
        target.write_bytes(receipt_path.read_bytes())
        target.chmod(0o600)
        receipt_path.unlink()
        receipt_path.symlink_to(target)
    elif attack == "hardlink":
        os.link(receipt_path, tmp_path / "receipt-hardlink.json")
    else:
        receipt_path.chmod(0o640)

    with pytest.raises(EvidenceError) as caught:
        release_gate_module._load_committed_canary_evidence(
            settings.evidence_dir,
            "primary",
            {},
        )

    assert caught.value.code == "canary_receipt_commit_receipt_unsafe_file"


@pytest.mark.parametrize("attack", ["symlink", "group_writable", "wrong_owner"])
def test_committed_canary_rejects_untrusted_evidence_directory(
    tmp_path,
    monkeypatch,
    attack,
):
    _consumer, _dispatcher, settings = _gate(tmp_path, "production")
    evidence_dir = settings.evidence_dir
    if attack == "symlink":
        evidence_link = tmp_path / "evidence-link"
        evidence_link.symlink_to(evidence_dir, target_is_directory=True)
        evidence_dir = evidence_link
    elif attack == "group_writable":
        evidence_dir.chmod(0o770)
    else:
        real_uid = os.getuid()
        monkeypatch.setattr(release_gate_module.os, "getuid", lambda: real_uid + 1)

    with pytest.raises(EvidenceError) as caught:
        release_gate_module._load_committed_canary_evidence(
            evidence_dir,
            "primary",
            {},
        )

    assert caught.value.code == "canary_receipt_commit_directory_invalid"


def test_committed_canary_generation_rejects_wrong_owner(tmp_path, monkeypatch):
    _consumer, _dispatcher, settings = _gate(tmp_path, "production")
    spec = release_gate_module.COMMITTED_CANARY_EVIDENCE_SPECS["primary"]
    directory_fd = os.open(settings.evidence_dir, os.O_RDONLY)
    real_uid = os.getuid()
    monkeypatch.setattr(release_gate_module.os, "getuid", lambda: real_uid + 1)
    try:
        with pytest.raises(EvidenceError) as caught:
            release_gate_module._secure_read_evidence_json_at(
                directory_fd,
                spec.manifest_filename,
                artifact="canary_receipt_commit",
                max_bytes=release_gate_module.MAX_CANARY_EVIDENCE_COMMIT_BYTES,
            )
    finally:
        os.close(directory_fd)

    assert caught.value.code == "canary_receipt_commit_unsafe_file"


@pytest.mark.parametrize(
    ("drift", "blocker"),
    [
        ("missing", "canary_receipt_commit_sources_missing"),
        ("bytes", "canary_receipt_commit_sources_content_mismatch"),
    ],
)
def test_committed_canary_generation_reference_drift_fails_closed(
    tmp_path,
    drift,
    blocker,
):
    _consumer, _dispatcher, settings = _gate(tmp_path, "production")
    _manifest_path, _receipt_path, sources_path = _committed_pair_paths(
        settings.evidence_dir,
        "primary",
    )
    if drift == "missing":
        sources_path.unlink()
    else:
        sources_path.write_bytes(sources_path.read_bytes() + b" ")
        sources_path.chmod(0o600)

    with pytest.raises(EvidenceError) as caught:
        release_gate_module._load_committed_canary_evidence(
            settings.evidence_dir,
            "primary",
            {},
        )

    assert caught.value.code == blocker


def test_committed_canary_final_barrier_rejects_manifest_change(tmp_path):
    _consumer, _dispatcher, settings = _gate(tmp_path, "production")
    expected = release_gate_module._load_committed_canary_evidence(
        settings.evidence_dir,
        "primary",
        {},
    )
    manifest_path, _receipt_path, _sources_path = _committed_pair_paths(
        settings.evidence_dir,
        "primary",
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["published_at"] = (NOW + timedelta(seconds=1)).isoformat()
    _write_json(manifest_path, manifest)
    manifest_path.chmod(0o600)

    with pytest.raises(EvidenceError) as caught:
        release_gate_module._recheck_committed_canary_evidence(
            settings.evidence_dir,
            expected,
        )

    assert caught.value.code == "manual_gateway_runtime_barrier_evidence_changed"


def test_committed_canary_ignores_unreferenced_orphan_generation(tmp_path):
    _consumer, _dispatcher, settings = _gate(tmp_path, "production")
    orphan = settings.evidence_dir / f"canary_receipt.{'0' * 64}.json"
    orphan.write_text("not referenced by the committed manifest", encoding="utf-8")
    orphan.chmod(0o600)
    evidence_hashes: dict[str, str] = {}

    detail = release_gate_module._load_committed_canary_evidence(
        settings.evidence_dir,
        "primary",
        evidence_hashes,
    )

    assert len(detail["commit_id"]) == 64
    assert orphan.name not in evidence_hashes


def test_production_requires_canary_source_provenance(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "production")
    _manifest, _receipt, sources_path = _committed_pair_paths(
        settings.evidence_dir,
        "primary",
    )
    sources_path.unlink()

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("production"),
        now=NOW,
    )

    assert report["ok"] is False
    assert "canary_receipt_commit_sources_missing" in report["blockers"]


@pytest.mark.parametrize(
    ("mutation", "blocker"),
    [
        (
            lambda source: source.update(manual_source_pack={}),
            "canary_receipt_sources_shape_invalid",
        ),
        (
            lambda source: source["remote_transport"]["files"].pop("pipeline"),
            "canary_receipt_sources_remote_file_set_invalid",
        ),
        (
            lambda source: source["collector"].update(sha256="0" * 64),
            "canary_receipt_sources_collector_bom_mismatch",
        ),
        (
            lambda source: source["control_database"].update(query_mode="rw"),
            "canary_receipt_sources_database_binding_mismatch",
        ),
        (
            lambda source: source["control_database"].update(
                inode=source["control_database"]["inode"] + 1
            ),
            "canary_receipt_sources_database_identity_mismatch",
        ),
        (
            lambda source: source["remote_transport"]["files"]["service_result"].update(
                canonical_sha256="0" * 64
            ),
            "canary_receipt_sources_remote_hash_mismatch",
        ),
        (
            lambda source: source["remote_transport"]["files"]["stage_s3a"].update(
                canonical_sha256="0" * 64
            ),
            "canary_receipt_sources_remote_hash_mismatch",
        ),
        (
            lambda source: source["remote_transport"]["files"][
                "delivery_manifest"
            ].update(canonical_sha256="0" * 64),
            "canary_receipt_sources_remote_hash_mismatch",
        ),
        (
            lambda source: source["local_machine_sources"]["browser_smoke"].update(
                raw_sha256="0" * 64
            ),
            "canary_receipt_sources_local_hash_mismatch",
        ),
        (
            lambda source: source.update(receipt_sha256="0" * 64),
            "canary_receipt_sources_receipt_binding_mismatch",
        ),
    ],
)
def test_production_canary_source_provenance_fails_closed_on_drift(
    tmp_path, mutation, blocker
):
    consumer, dispatcher, settings = _gate(tmp_path, "production")
    source = _read_committed_pair_body(
        settings.evidence_dir,
        "primary",
        "sources",
    )
    mutation(source)
    _rewrite_committed_pair_body(
        settings.evidence_dir,
        "primary",
        "sources",
        source,
    )

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("production"),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


@pytest.mark.parametrize(
    ("drift", "blocker"),
    [
        ("missing", "canary_receipt_sources_database_unreadable"),
        ("symlink", "canary_receipt_sources_database_invalid"),
    ],
)
def test_production_canary_source_provenance_binds_live_database_path(
    tmp_path, drift, blocker
):
    consumer, dispatcher, settings = _gate(tmp_path, "production")
    database_path = dispatcher.control_db_path
    if drift == "missing":
        database_path.unlink()
    else:
        backing_path = database_path.with_name("control-backing.sqlite3")
        database_path.replace(backing_path)
        database_path.symlink_to(backing_path)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("production"),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]


def test_production_canary_source_provenance_rejects_parent_directory_symlink(
    tmp_path,
):
    real_root = tmp_path / "real"
    alias_root = tmp_path / "alias"
    real_root.mkdir()
    _consumer, dispatcher, _settings = _gate(real_root, "production")
    alias_root.symlink_to(real_root, target_is_directory=True)

    with pytest.raises(EvidenceError) as caught:
        release_gate_module._observe_live_database_identity(
            alias_root / dispatcher.control_db_path.name,
            unreadable_code="canary_receipt_sources_database_unreadable",
            invalid_code="canary_receipt_sources_database_invalid",
        )

    assert caught.value.code == "canary_receipt_sources_database_invalid"


def test_production_canary_source_provenance_allows_same_inode_wal_growth(tmp_path):
    consumer, dispatcher, settings = _gate(tmp_path, "production")
    database_path = dispatcher.control_db_path
    before = database_path.stat()
    with sqlite3.connect(database_path) as connection:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("CREATE TABLE provenance_identity_probe(value TEXT)")
        connection.execute(
            "INSERT INTO provenance_identity_probe(value) VALUES (?)", ("growth",)
        )
        connection.commit()
    after = database_path.stat()

    assert (after.st_dev, after.st_ino) == (before.st_dev, before.st_ino)
    assert (after.st_size, after.st_mtime_ns) != (before.st_size, before.st_mtime_ns)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("production"),
        now=NOW,
    )

    source_check = next(
        check for check in report["checks"] if check["name"] == "canary_receipt_sources"
    )
    assert source_check["ok"] is True


def test_live_canary_database_binding_requeries_exact_success_rows(tmp_path):
    from scripts.pnc_rca_canary_collector import (
        CanaryReceiptCollector,
        read_local_canary_database_facts,
    )
    from tests.scripts.test_pnc_rca_canary_collector import _fixture

    config, reader, _submission = _fixture(tmp_path)
    collected = CanaryReceiptCollector(
        config,
        remote_reader=reader,
        now=lambda: NOW,
    ).collect(SOURCE_ID)
    facts = read_local_canary_database_facts(config, SOURCE_ID)
    detail = LIVE_CANARY_DATABASE_BINDING(
        receipt=collected.receipt,
        control_snapshot_sha256=collected.provenance["control_database"][
            "snapshot_sha256"
        ],
        delivery_snapshot_sha256=collected.provenance["delivery_database"][
            "snapshot_sha256"
        ],
        control_db_path=config.control_db_path,
        delivery_db_path=config.delivery_db_path,
        evidence_dir=config.evidence_dir,
        group_binding_receipt_dir=config.group_binding_receipt_dir,
        manual_chat_ids=config.manual_chat_ids,
        expected_workflow_policy=facts["workflow_policy"],
        terminal_failure=False,
    )
    assert len(detail["projection_sha256"]) == 64

    with sqlite3.connect(config.delivery_db_path) as connection:
        connection.execute(
            "UPDATE rca_delivery_effects "
            "SET completed_at='2026-07-10T07:59:41+00:00'"
        )
    with pytest.raises(EvidenceError) as caught:
        LIVE_CANARY_DATABASE_BINDING(
            receipt=collected.receipt,
            control_snapshot_sha256=collected.provenance["control_database"][
                "snapshot_sha256"
            ],
            delivery_snapshot_sha256=collected.provenance["delivery_database"][
                "snapshot_sha256"
            ],
            control_db_path=config.control_db_path,
            delivery_db_path=config.delivery_db_path,
            evidence_dir=config.evidence_dir,
            group_binding_receipt_dir=config.group_binding_receipt_dir,
            manual_chat_ids=config.manual_chat_ids,
            expected_workflow_policy=facts["workflow_policy"],
            terminal_failure=False,
        )
    assert caught.value.code == "canary_database_live_requery_failed"


def test_live_canary_database_binding_requeries_exact_terminal_rows(tmp_path):
    from scripts.pnc_rca_canary_collector import (
        CanaryReceiptCollector,
        read_local_canary_database_facts,
    )
    from tests.scripts.test_pnc_rca_canary_collector import (
        _terminal_manual_fixture,
    )

    config, reader, source_id = _terminal_manual_fixture(tmp_path)
    collected = CanaryReceiptCollector(
        config,
        remote_reader=reader,
        now=lambda: NOW,
    ).collect_terminal_failure(source_id)
    facts = read_local_canary_database_facts(
        config,
        source_id,
        terminal_failure=True,
    )
    detail = LIVE_CANARY_DATABASE_BINDING(
        receipt=collected.receipt,
        control_snapshot_sha256=collected.provenance["control_database"][
            "snapshot_sha256"
        ],
        delivery_snapshot_sha256=collected.provenance["delivery_database"][
            "snapshot_sha256"
        ],
        control_db_path=config.control_db_path,
        delivery_db_path=config.delivery_db_path,
        evidence_dir=config.evidence_dir,
        group_binding_receipt_dir=config.group_binding_receipt_dir,
        manual_chat_ids=config.manual_chat_ids,
        expected_workflow_policy=facts["workflow_policy"],
        terminal_failure=True,
    )
    assert len(detail["projection_sha256"]) == 64

    with sqlite3.connect(config.delivery_db_path) as connection:
        connection.execute(
            "UPDATE rca_execution_watch "
            "SET terminal_at='2026-07-10T07:59:31+00:00'"
        )
    with pytest.raises(EvidenceError) as caught:
        LIVE_CANARY_DATABASE_BINDING(
            receipt=collected.receipt,
            control_snapshot_sha256=collected.provenance["control_database"][
                "snapshot_sha256"
            ],
            delivery_snapshot_sha256=collected.provenance["delivery_database"][
                "snapshot_sha256"
            ],
            control_db_path=config.control_db_path,
            delivery_db_path=config.delivery_db_path,
            evidence_dir=config.evidence_dir,
            group_binding_receipt_dir=config.group_binding_receipt_dir,
            manual_chat_ids=config.manual_chat_ids,
            expected_workflow_policy=facts["workflow_policy"],
            terminal_failure=True,
        )
    assert caught.value.code == "canary_database_live_requery_failed"


@pytest.mark.parametrize(
    "relative",
    [
        "scripts/pnc_g1q3_truth.py",
        "gateway/feishu_task_card.py",
        "gateway/pnc_issue_capture.py",
        "gateway/pnc_rca_stage_lineage.py",
    ],
)
@pytest.mark.parametrize(
    ("mutation", "blocker"),
    [
        (
            lambda critical, relative: critical.pop(relative),
            "build_manifest_core_file_missing",
        ),
        (
            lambda critical, relative: critical.__setitem__(relative, "0" * 64),
            "build_manifest_critical_hash_mismatch",
        ),
    ],
)
def test_completion_relay_visible_dependencies_are_critical_bom_files(
    tmp_path, relative, mutation, blocker
):
    consumer, dispatcher, settings = _gate(tmp_path, "canary")
    path = settings.evidence_dir / "build_manifest.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    mutation(manifest["critical_files"], relative)
    _write_json(path, manifest)

    report = evaluate_release_gate(
        consumer=consumer,
        dispatcher=dispatcher,
        settings=settings,
        cutover=_cutover("canary"),
        now=NOW,
    )

    assert report["ok"] is False
    assert blocker in report["blockers"]
