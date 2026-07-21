#!/usr/bin/env python3
"""Build and validate the exact, plan-only RCA production E2E release scope."""

from __future__ import annotations

import argparse
import hashlib
import importlib.machinery
import ipaddress
import json
import os
import platform
import plistlib
import pwd
import re
import sqlite3
import stat
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import quote, urlsplit

import psutil

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from gateway import pnc_rca_prod_bootstrap as prod_bootstrap
from gateway import pnc_rca_workspace_runtime as workspace_runtime


BOM_SCHEMA_VERSION = "pnc_rca_prod_e2e_release_bom_v5"
REQUEST_SCHEMA_VERSION = "pnc_rca_prod_e2e_release_request_v5"
APPROVAL_SCHEMA_VERSION = "pnc_rca_prod_e2e_release_approval_v1"
BASELINE_APPROVAL_SCHEMA_VERSION = "pnc_rca_release_approval_v1"
BASELINE_APPROVAL_DECISION = "authorize_rca_delivery_quarantine_baseline"
VALIDATION_SCHEMA_VERSION = "pnc_rca_prod_e2e_release_validation_v5"
CANDIDATE_OBSERVATION_SCHEMA_VERSION = (
    "pnc_rca_pipeline_candidate_observation_v2"
)
GAP_LEDGER_SCHEMA_VERSION = "pnc_rca_pre_t0_accepted_gap_ledger_v1"
FIELD_PREREAD_SCHEMA_VERSION = "pnc_rca_official_meegle_field_preread_v1"
INPUT_PREREAD_SCHEMA_VERSION = "pnc_rca_issue_input_preread_v1"
CLOSURE_AUDIT_SCHEMA_VERSION = "pnc_rca_fixed_cli_mcap_closure_audit_v4"
CROSS_CONTRACT_PASS_SCHEMA_VERSION = "pnc_rca_terminal_cross_contract_pass_v4"
COMPONENT_BINDING_SCHEMA_VERSION = "pnc_rca_prod_component_binding_v4"
VIEWER_PROXY_CANDIDATE_SCHEMA_VERSION = (
    "pnc_rca_viewer_proxy_candidate_binding_v1"
)
DB_CUTOVER_BINDING_SCHEMA_VERSION = "pnc_rca_delivery_store_cutover_binding_v1"
EXECUTION_PREFLIGHT_SCHEMA_VERSION = "pnc_rca_execution_preflight_v5"
COMPLETION_RECEIPT_SCHEMA_VERSION = "pnc_rca_prod_e2e_completion_v5"
APPROVAL_DECISION = "authorize_exact_rca_prod_e2e_release"
RELEASE_APPROVAL_IDENTITY_SCHEMA_VERSION = "pnc_rca_release_approval_identity_v1"
RELEASE_APPROVAL_IDENTITY_METHOD = "kernel_owner_and_machine_binding"

PIPELINE_COMMIT = "4b26cc7935eb4fa0910b42abde78d7f8d4efa0d1"
PIPELINE_TREE = "9d45fb1357c7ab054c16c898941e342b9a50d391"
PIPELINE_ENTRYPOINT = "api/g1q3_rca/scripts/run_rca_service_request.py"
PIPELINE_ENTRYPOINT_SHA256 = (
    "1c74abb1781b6b147787879747e84c22aaaa01fdf0507790cb104b33f5773982"
)
PIPELINE_SOURCE_ROOT = (
    "/home/mini/.hermes/rca-prod-runtime/releases/"
    "rca-e2e-hotfix-pathsafe-20260721"
)
PIPELINE_CANDIDATE_AUDIT_VM_PATH = (
    "/home/mini/.hermes/rca-prod-runtime/audits/"
    "4b26cc7935eb4fa0910b42abde78d7f8d4efa0d1/independent-go-receipt.json"
)
PIPELINE_CANDIDATE_AUDIT_CIFS_PATH = (
    "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/"
    "019f8243-88a6-7b42-a0b3-abbfd5427767/vm_candidate_audit/"
    "independent-4b26cc79/receipt-go-4b26cc79.json"
)
PIPELINE_CANDIDATE_AUDIT_SHA256 = (
    "0765e0adfb3e74abe6a1daaea626901003b9b0cb94223a0b401d626d1a48d1bf"
)
PIPELINE_CLOSURE_CORE_SHA256 = (
    "6e8bc92415624af37ebdbebb9562e6051cc8c5d094516796ce26f582635cd710"
)
PIPELINE_CLOSURE_FILE_SHA256 = (
    "d56e6432d4275adc80f5a2a0a2d8f4646b12ce255d5b293ff3005391d04d0509"
)
PIPELINE_CLOSURE_VM_PATH = (
    "/mnt/tmp/g1q3-rca-4b26-closure-audit-20260722/"
    "fixed-cli-mcap-hard-rule-audit-4b26cc79.json"
)
PIPELINE_CLOSURE_CIFS_PATH = (
    "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/"
    "g1q3-rca-4b26-closure-audit-20260722/"
    "fixed-cli-mcap-hard-rule-audit-4b26cc79.json"
)
PIPELINE_CLOSURE_SEALED_MIRROR_PATH = (
    "/Users/songying/.codex/tmp/rca-prod-e2e-release-20260721/evidence/"
    "fixed-cli-mcap-hard-rule-audit-4b26cc79.json"
)
CROSS_CONTRACT_PASS_FILE_SHA256 = (
    "8fa70b458c1676de058902123fe78bb4619d59c82072c1e251fae0f1991b949e"
)
SUPERSEDED_CROSS_CONTRACT_GAP_SHA256 = (
    "b36f9be5c913277ff8f3944b92e26da1a6e2ea6dabcf1d9072a4514b7f5a684f"
)
HOST_QUARANTINE_BASELINE_COMMIT = (
    "11b7c06af20fa0c4c09893452d4d617da3d10755"
)
HOST_FINAL_COMMIT = "540dc0c8b6fd0ed58a919f63a17ae7d934f0f94a"
HOST_FINAL_TREE = "a339f44e634ab6779b30683be3219257da10fba2"
HOST_FINAL_PARENT_COMMIT = "788f5930f9cfdbe61a67df91b719b95896f1367d"
CANONICAL_HOST_ROOT = "/Users/songying/.codex/tmp/rca-host-70c432-zero-cache"
HOST_CANDIDATE_IDENTITY_PATH = (
    "/Users/songying/.codex/tmp/rca-prod-e2e-release-20260721/evidence/"
    "controlled-gray/host-independent-go-540dc0c8.json"
)
HOST_CANDIDATE_IDENTITY_SHA256 = (
    "517835c297b96ae21c0dddf8c2dd0f1b762172841e0bab51366a4a801da8fcc6"
)
CANONICAL_HOST_PYTHON = (
    "/Users/songying/.hermes/runtime/hermes-live/.venv/bin/python"
)
CANONICAL_HOST_ENV = "/Users/songying/.hermes/.env"
HOST_LIVE_ROOT = "/Users/songying/.hermes/runtime/hermes-live"
CANONICAL_RUNTIME_BOOTSTRAP_FILES = (
    "gateway/__init__.py",
    "gateway/pnc_rca_runtime_identity.py",
)
CANONICAL_MIGRATION_MODULE = "gateway/pnc_rca_delivery_quarantine_migration.py"
CANONICAL_BASELINE_MODULE = "gateway/pnc_rca_delivery_quarantine_baseline.py"
CANONICAL_MIGRATION_MODULE_SHA256 = (
    "19a3aa13e798fd73e8fa7a3801c1564a32b5d97819d61e880b6adf91325139ee"
)
CANONICAL_BASELINE_MODULE_SHA256 = (
    "c2e057635a3aca527c35565336ac5e35225104705815b0464e773a65216eb9b4"
)
DELIVERY_STORE_SOURCE_SCHEMA = "pnc_rca_delivery_store_v6"
DELIVERY_STORE_TARGET_SCHEMA = "pnc_rca_delivery_store_v7"
DELIVERY_MIGRATION_SCHEMA = "pnc_rca_delivery_quarantine_offline_migration_v1"
DELIVERY_QUARANTINE_CORE_SCHEMA = "pnc_rca_delivery_quarantine_core_v1"
DELIVERY_BASELINE_SCHEMA = "pnc_rca_delivery_quarantine_baseline_v1"
DELIVERY_DB_PATH = (
    "/Users/songying/.hermes/runtime/pnc_agent/feishu_issue_kafka_rca/"
    "control.sqlite3"
)
ADMISSION_HMAC_ENV = "HERMES_RCA_PROD_ADMISSION_HMAC_KEY"
VIEWER_ORIGIN_ENV = "PNC_FOXGLOVE_RENDER_HOST"
VIEWER_EXPECTED_ADDRESS = "192.168.21.217"
VIEWER_PROXY_ROUTE_PREFIX = "/g1q3-rca-artifacts/v1/"
VIEWER_PROXY_UPSTREAM_ORIGIN = "http://192.168.26.174:18081"
VIEWER_PROXY_CONFIG_PATH = (
    "/Users/songying/.codex/tmp/rca-prod-e2e-release-20260721/evidence/"
    "pathsafe-integration/g1q3-rca-artifacts-proxy-candidate.conf"
)
VIEWER_PROXY_CONFIG_SHA256 = (
    "62b229afaceef49b4d48a15a8cb9e43f7ea2584785ae0ef003496e28f7466fe7"
)
VIEWER_PROXY_CONFIG_BYTES = 1100
VIEWER_PROXY_STATIC_RECEIPT_PATH = (
    "/Users/songying/.codex/tmp/rca-prod-e2e-release-20260721/evidence/"
    "pathsafe-integration/g1q3-rca-artifacts-proxy-static-test-v2.json"
)
VIEWER_PROXY_STATIC_RECEIPT_SHA256 = (
    "653b80a0681389861459fbbbd8b7daffa2ffa5bbc2a4a6c3759f2ba8c1c085fe"
)
VIEWER_PROXY_ROLLBACK_BASELINE_PATH = (
    "/Users/songying/.codex/tmp/rca-prod-e2e-release-20260721/evidence/"
    "pathsafe-integration/g1q3-rca-viewer-proxy-predeployment-v1.json"
)
VIEWER_PROXY_ROLLBACK_BASELINE_SHA256 = (
    "21948eb8611dfa2171952b13178a8c76bd77060280b6a1f4c94db07257bae9b7"
)
VIEWER_PROXY_LIVE_SCHEMA_VERSION = "pnc_rca_viewer_proxy_live_observation_v1"
VIEWER_DIAGNOSTIC_SHA256 = (
    "fb72eb58b4360e56f34bc04031b1fa96a17f5b60022bfcf4cd8056e33bc0bc46"
)
VIEWER_DIAGNOSTIC_SUBMISSION_KEY = (
    "g1q3-rca-s1-" + VIEWER_DIAGNOSTIC_SHA256
)
VIEWER_DIAGNOSTIC_BYTES = 2235
VIEWER_DIAGNOSTIC_TOPIC = "/rca/evidence/log"
HOST_SERVICE_LABELS = (
    "ai.hermes.gateway",
    "local.pnc.rca-kafka-consumer",
    "local.pnc.rca-outbox-dispatcher",
    "local.pnc.rca-delivery-collector",
    "local.pnc.rca-delivery-dispatcher",
    "local.pnc.completion-notice-relay",
)
HOST_SERVICE_ENTRYPOINTS = {
    "ai.hermes.gateway": None,
    "local.pnc.rca-kafka-consumer": "scripts/pnc_rca_kafka_consumer.py",
    "local.pnc.rca-outbox-dispatcher": "scripts/pnc_rca_outbox_dispatcher.py",
    "local.pnc.rca-delivery-collector": "scripts/pnc_rca_delivery_collector.py",
    "local.pnc.rca-delivery-dispatcher": "scripts/pnc_rca_delivery_dispatcher.py",
    "local.pnc.completion-notice-relay": "scripts/pnc_completion_notice_relay.py",
}
VM_DAEMON_UNIT = "hermes-vm-coding-worker-daemon.service"
VM_REPORT_UNIT = "g1q3-rca-report-http.service"
VM_SERVICE_UNITS = (VM_DAEMON_UNIT, VM_REPORT_UNIT)
VM_REPORT_ENTRYPOINT_RELATIVE = "api/g1q3_rca/scripts/serve_rca_reports.py"
VM_REPORT_ENTRYPOINT_SHA256 = (
    "545e8e076a81e9e273363a297ec7d9711ba6d7e490513dbc57540a4ad0c5b4e7"
)
VM_REPORT_UNIT_RELATIVE = "api/g1q3_rca/systemd/g1q3-rca-report-http.service"
VM_REPORT_UNIT_SHA256 = (
    "dbd102d1b63ebcb6ee6615d171fba141a3444c72b333ab68c54ea468340938fa"
)
VM_REPORT_LIVE_UNIT_PATH = "/home/mini/.config/systemd/user/g1q3-rca-report-http.service"
VM_REPORT_ENV_PATH = "/home/mini/.config/g1q3-rca/report-http.env"
VM_REPORT_ENV_VARIABLE = "G1Q3_RCA_VIEWER_ORIGIN"
VM_REPORT_ROOT = "/mnt/tmp"
VM_REPORT_ROUTE_PREFIX = "/G1Q3_RCA/cases/"
VM_REPORT_PORT = 18081
VM_WORKER_OBSERVER_COMMAND_SHA256 = hashlib.sha256(
    b"pnc_rca_vm_worker_identity_observer_v1"
).hexdigest()
VM_HMAC_OBSERVER_COMMAND_SHA256 = hashlib.sha256(
    b"pnc_rca_vm_hmac_observer_v1"
).hexdigest()
VM_WORKER_ROOT = "/home/mini/.hermes/worker-state"
VM_WORKER_ENTRYPOINT = f"{VM_WORKER_ROOT}/vm_coding_worker_scheduler.py"
VM_INTERPRETER_PATH = "/usr/bin/python3"
VM_DAEMON_LIVE_UNIT_PATH = (
    "/home/mini/.config/systemd/user/hermes-vm-coding-worker-daemon.service"
)
VM_WORKER_RUNTIME_FILES = (
    "check_rca_delivery_runtime.py",
    "shared_state_v2.py",
    "vm_coding_worker_scheduler.py",
    "vm_coding_worker_v2.py",
    "vm_rca_prod_admission.py",
)
QUARANTINE_COUNTS = {"jobs": 39, "effects": 38, "subscriptions": 5}
RETIRED_EXECUTOR_PATHS = (
    "scripts/pnc_rca_prod_e2e_release.py",
    "scripts/pnc_rca_release_gate.py",
    "scripts/pnc_rca_store_migration_drill.py",
    "scripts/pnc_rca_cutover_execute.py",
    "scripts/pnc_rca_cutover_live.py",
    "scripts/pnc_rca_cutover_adapter.py",
    "tests/scripts/test_pnc_rca_prod_e2e_release.py",
)
GAP_LEDGER_FILE_SHA256 = (
    "3bb1df0a359aff4949d2d5d81b9c3de1cbf64365b1da8b204adb81a67e423368"
)
FIELD_PREREAD_FILE_SHA256 = (
    "8bd4b6f35c02e368fd0f401a23aeeef531a2f9c915b80679e947c3db039694bc"
)
INPUT_PREREAD_FILE_SHA256 = (
    "1bd243a8c5edff7ffddc92af43fe9ee125a5342571a2b62de1c278e0a74942c0"
)

TOPIC = "feishu-project-workflow-event"
PARTITION = 0
LIVE_T0_OFFSET = 676
TARGET_OFFSET = 650
TARGET_EVENT_UID = "feishu-project-workflow-event:0:650"
TARGET_RAW_SHA256 = (
    "f8430db59cf4d842817130072eb1331f88db090d3f3a3e726631d3dad3206d2f"
)
TARGET_BUSINESS_KEY = (
    "g1q3-rca-b1-c1a97ca45f114aeeab53cba9124f59584cd075f78e4c52413db7a6ef95506221"
)
TARGET_SUBMISSION_KEY = (
    "g1q3-rca-s1-559985f73b318a340a308e082ae6ac32e29469054f7be2f51524a067b51b4c3e"
)
TARGET_WORK_ITEM_ID = "7051585084"
TARGET_PROJECT_KEY = "68ef617fb371dc80a10641f7"
TARGET_WORK_ITEM_TYPE_KEY = "issue"
TARGET_PROJECT_SIMPLE_NAME = "t03o4q"
TARGET_ISSUE_URL = (
    "https://project.feishu.cn/t03o4q/issue/detail/7051585084"
)
RESULT_FIELD_KEY = "field_9193cb"
REPORT_FIELD_KEY = "field_8c912e"
TARGET_FIELD_KEYS = (RESULT_FIELD_KEY, REPORT_FIELD_KEY)
MISSING_LIVE_COUNT = 73
DEFERRED_MISSING_COUNT = 72
DAILY_STARTED_ATTEMPT_QUOTA = 5
EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()

ACTION_SET = (
    "materialize_ext4_self_contained_detached_pipeline_candidate",
    "verify_final_pipeline_commit_tree_closure_and_read_only_seal",
    "verify_canonical_host_final_commit_tree_contains_11b_quarantine_baseline",
    "verify_exact_host_workspace_worker_pipeline_identities",
    "verify_hmac_secured_host_to_vm_admission_stream_fingerprint_match",
    "install_bom_bound_host_workspace_vm_and_worker_release",
    "install_new_bom_bound_bootstrap_authorization",
    "stop_exact_six_host_services_and_two_vm_services_before_delivery_db_cutover",
    "capture_fresh_owner_only_standalone_live_v6_backup",
    "assert_fresh_live_pre_digest_equals_approved_source_digest",
    "install_exact_offline_migrated_v7_clone",
    "assert_fresh_live_post_digest_equals_approved_post_migration_digest",
    "assert_live_quarantine_core_equals_approved_core",
    "validate_distinct_bom_bound_quarantine_baseline_approval",
    "retain_lifetime_quarantine_39_jobs_38_effects_5_subscriptions",
    "forbid_quarantine_retry_delete_and_rearm",
    "issue_bom_bound_quarantine_baseline",
    "update_environment_and_active_binding_only_after_db_post_and_core_match",
    "restart_exact_six_host_services_and_two_vm_services",
    "verify_hermes_live_runtime_closure_and_service_program_arguments",
    "promote_exact_kafka_partition_0_offset_650",
    "execute_rca_for_exact_issue_7051585084",
    "write_two_attribution_fields_and_one_evidence_comment",
    "read_back_exact_fields_comment_and_delivery_lineage",
    "officially_read_back_one_new_post_cutover_kafka_canary",
    "restore_v6_backup_before_environment_or_binding_on_any_db_gate_failure",
    "rollback_on_any_gate_failure",
)

MAX_JSON_BYTES = 2 * 1024 * 1024
MAX_DB_ARTIFACT_BYTES = 64 * 1024 * 1024
MAX_APPROVAL_VALIDITY = timedelta(hours=24)
MAX_FUTURE_SKEW = timedelta(minutes=5)
MAX_FINAL_OBSERVATION_AGE = timedelta(minutes=10)
MAX_CLOSEOUT_AGE = timedelta(minutes=10)
APPROVAL_NONCE_LEDGER_ROOT = Path(
    "/Users/songying/.codex/memories/task-state/tasks/"
    "20260718-192536-g1q3-full-pdcl-historical-rca-learning-expansion/"
    "approval-nonce-ledger"
)
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
RELEASE_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{7,127}\Z")
NONCE_RE = re.compile(r"[A-Za-z0-9_-]{16,128}\Z")
GIT_OID_RE = re.compile(r"[0-9a-f]{40}\Z")


class ProdE2EReleaseError(ValueError):
    def __init__(self, code: str):
        self.code = str(code or "prod_e2e_release_invalid")[:160]
        super().__init__(self.code)


def _canonical_host_interpreter_paths() -> tuple[Path, Path]:
    """Return the venv entrypoint for execution and its real binary for hashing."""

    entrypoint = Path(CANONICAL_HOST_PYTHON).expanduser()
    if not entrypoint.is_absolute():
        raise ProdE2EReleaseError(
            "prod_e2e_release_canonical_python_invalid"
        )
    entrypoint = entrypoint.absolute()
    try:
        lexical = os.lstat(entrypoint)
        binary = entrypoint.resolve(strict=True)
        resolved = os.stat(binary)
    except OSError as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_canonical_python_invalid"
        ) from exc
    if (
        not (stat.S_ISLNK(lexical.st_mode) or stat.S_ISREG(lexical.st_mode))
        or lexical.st_uid != os.geteuid()
        or lexical.st_nlink != 1
        or not stat.S_ISREG(resolved.st_mode)
        or resolved.st_uid not in {0, os.geteuid()}
        or stat.S_IMODE(resolved.st_mode) & 0o022
        or not os.access(entrypoint, os.X_OK)
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_canonical_python_invalid"
        )
    return entrypoint, binary


@dataclass(frozen=True)
class OwnedJson:
    path: Path
    raw: bytes
    body: Mapping[str, Any]

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.raw).hexdigest()


MachineIdentityProvider = Callable[[], Mapping[str, Any]]


def _canonical_bytes(value: Any, *, newline: bool = False) -> bytes:
    try:
        raw = json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise ProdE2EReleaseError("prod_e2e_release_json_invalid") from exc
    return raw + (b"\n" if newline else b"")


def _sha256_value(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _sha256(value: Any, *, field: str) -> str:
    text = str(value or "").strip().lower()
    if SHA256_RE.fullmatch(text) is None:
        raise ProdE2EReleaseError(f"prod_e2e_release_{field}_invalid")
    return text


_DNS_LABEL_RE = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z")


def _canonical_https_dns_origin(value: Any, *, field: str) -> str:
    raw = value if isinstance(value, str) else ""
    origin = _required_text(value, field=field)
    if (
        raw != raw.strip()
        or not origin.isascii()
        or origin != origin.lower()
        or any(ord(char) < 0x20 or ord(char) == 0x7F for char in origin)
    ):
        raise ProdE2EReleaseError(f"prod_e2e_release_{field}_invalid")
    try:
        parsed = urlsplit(origin)
        hostname = parsed.hostname or ""
        port = parsed.port
    except ValueError:
        raise ProdE2EReleaseError(
            f"prod_e2e_release_{field}_invalid"
        ) from None
    try:
        ipaddress.ip_address(hostname)
    except ValueError:
        address_literal = False
    else:
        address_literal = True
    labels = hostname.split(".")
    if (
        parsed.scheme != "https"
        or address_literal
        or port is not None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path
        or parsed.query
        or parsed.fragment
        or parsed.netloc != hostname
        or len(hostname) > 253
        or len(labels) < 2
        or any(label.startswith("xn--") for label in labels)
        or any(_DNS_LABEL_RE.fullmatch(label) is None for label in labels)
        or not any(character.isalpha() for character in labels[-1])
        or origin != f"https://{hostname}"
    ):
        raise ProdE2EReleaseError(f"prod_e2e_release_{field}_invalid")
    return origin


def _git_oid(value: Any, *, field: str) -> str:
    text = str(value or "").strip().lower()
    if GIT_OID_RE.fullmatch(text) is None:
        raise ProdE2EReleaseError(f"prod_e2e_release_{field}_invalid")
    return text


def _timestamp(value: Any, *, field: str) -> datetime:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ProdE2EReleaseError(
            f"prod_e2e_release_{field}_invalid"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ProdE2EReleaseError(f"prod_e2e_release_{field}_invalid")
    return parsed.astimezone(timezone.utc)


def _now(value: datetime | None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None or current.utcoffset() is None:
        raise ProdE2EReleaseError("prod_e2e_release_now_invalid")
    return current.astimezone(timezone.utc)


def _strict_json(raw: bytes, *, artifact: str) -> Mapping[str, Any]:
    if not raw or len(raw) > MAX_JSON_BYTES:
        raise ProdE2EReleaseError(f"prod_e2e_release_{artifact}_size_invalid")

    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise ProdE2EReleaseError(
                    f"prod_e2e_release_{artifact}_duplicate_key"
                )
            result[key] = value
        return result

    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=pairs,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                ProdE2EReleaseError(
                    f"prod_e2e_release_{artifact}_number_invalid"
                )
            ),
        )
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ProdE2EReleaseError(
            f"prod_e2e_release_{artifact}_json_invalid"
        ) from exc
    if not isinstance(value, dict):
        raise ProdE2EReleaseError(f"prod_e2e_release_{artifact}_shape_invalid")
    return value


def _read_owned_json(path: Path, *, artifact: str) -> OwnedJson:
    candidate = path.expanduser().absolute()
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(candidate, flags)
    except OSError as exc:
        raise ProdE2EReleaseError(
            f"prod_e2e_release_{artifact}_unavailable"
        ) from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
        ):
            raise ProdE2EReleaseError(
                f"prod_e2e_release_{artifact}_not_owner_only"
            )
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(64 * 1024, MAX_JSON_BYTES + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > MAX_JSON_BYTES:
                raise ProdE2EReleaseError(
                    f"prod_e2e_release_{artifact}_size_invalid"
                )
        after = os.fstat(descriptor)
        lexical = os.lstat(candidate)
        if (
            (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
            != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
            or (after.st_dev, after.st_ino) != (lexical.st_dev, lexical.st_ino)
            or stat.S_ISLNK(lexical.st_mode)
        ):
            raise ProdE2EReleaseError(
                f"prod_e2e_release_{artifact}_unstable"
            )
    except OSError as exc:
        raise ProdE2EReleaseError(
            f"prod_e2e_release_{artifact}_unavailable"
        ) from exc
    finally:
        os.close(descriptor)
    raw = b"".join(chunks)
    return OwnedJson(candidate, raw, _strict_json(raw, artifact=artifact))


def _read_owned_blob(path: Path, *, artifact: str) -> tuple[Path, str, int]:
    candidate = path.expanduser().absolute()
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    if not hasattr(os, "O_NOFOLLOW"):
        raise ProdE2EReleaseError(
            f"prod_e2e_release_{artifact}_no_follow_unavailable"
        )
    flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(candidate, flags)
    except OSError as exc:
        raise ProdE2EReleaseError(
            f"prod_e2e_release_{artifact}_unavailable"
        ) from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
            or before.st_size < 16
            or before.st_size > MAX_DB_ARTIFACT_BYTES
        ):
            raise ProdE2EReleaseError(
                f"prod_e2e_release_{artifact}_not_owner_only"
            )
        digest = hashlib.sha256()
        total = 0
        prefix = b""
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            if not prefix:
                prefix = chunk[:16]
            digest.update(chunk)
            total += len(chunk)
            if total > MAX_DB_ARTIFACT_BYTES:
                raise ProdE2EReleaseError(
                    f"prod_e2e_release_{artifact}_size_invalid"
                )
        after = os.fstat(descriptor)
        lexical = os.lstat(candidate)
        if (
            prefix != b"SQLite format 3\x00"
            or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
            != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
            or (after.st_dev, after.st_ino) != (lexical.st_dev, lexical.st_ino)
            or stat.S_ISLNK(lexical.st_mode)
        ):
            raise ProdE2EReleaseError(
                f"prod_e2e_release_{artifact}_unstable"
            )
    except OSError as exc:
        raise ProdE2EReleaseError(
            f"prod_e2e_release_{artifact}_unavailable"
        ) from exc
    finally:
        os.close(descriptor)
    return candidate, digest.hexdigest(), total


def _git_command(root: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            ["/usr/bin/git", "-C", str(root), *arguments],
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
            env={
                "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
                "GIT_OPTIONAL_LOCKS": "0",
                "LC_ALL": "C",
            },
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_git_observation_failed"
        ) from exc


def _host_git_text(root: Path, *arguments: str) -> str:
    completed = _git_command(root, *arguments)
    if completed.returncode != 0 or completed.stderr:
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_git_observation_failed"
        )
    return completed.stdout.strip()


def _host_tracked_bytes(root: Path, commit: str, relative: str) -> bytes:
    path = root / relative
    try:
        lexical = os.lstat(path)
        resolved = path.resolve(strict=True)
        root_resolved = root.resolve(strict=True)
        if (
            stat.S_ISLNK(lexical.st_mode)
            or not stat.S_ISREG(lexical.st_mode)
            or resolved.parent != (root_resolved / relative).parent
            or len(path.read_bytes()) > MAX_JSON_BYTES
        ):
            raise ProdE2EReleaseError(
                "prod_e2e_release_host_runtime_file_invalid"
            )
        live = path.read_bytes()
    except (OSError, RuntimeError) as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_runtime_file_invalid"
        ) from exc
    try:
        completed = subprocess.run(
            [
                "/usr/bin/git",
                "-C",
                str(root),
                "cat-file",
                "blob",
                f"{commit}:{relative}",
            ],
            check=False,
            capture_output=True,
            timeout=15,
            env={
                "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
                "GIT_OPTIONAL_LOCKS": "0",
                "LC_ALL": "C",
            },
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_runtime_blob_unavailable"
        ) from exc
    if completed.returncode != 0 or completed.stderr or completed.stdout != live:
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_runtime_blob_mismatch"
        )
    return live


def _assert_source_tree_cache_and_shadow_free(
    root: Path, *, expected_files: Sequence[str], artifact: str
) -> None:
    forbidden: list[str] = []
    try:
        for candidate in root.rglob("*"):
            relative = candidate.relative_to(root)
            if relative.parts and relative.parts[0] in {".git", ".venv"}:
                continue
            if (
                candidate.name in {"__pycache__", ".pytest_cache"}
                or candidate.suffix == ".pyc"
            ):
                forbidden.append(str(relative))
    except OSError as exc:
        raise ProdE2EReleaseError(
            f"prod_e2e_release_{artifact}_bytecode_observation_failed"
        ) from exc
    if forbidden:
        raise ProdE2EReleaseError(
            f"prod_e2e_release_{artifact}_bytecode_present"
        )
    package_roots: set[str] = set()
    shadow_candidates: set[Path] = set()
    approved = {root / relative for relative in expected_files}
    for relative in expected_files:
        value = PurePosixPath(relative)
        if value.suffix != ".py":
            continue
        stem = root.joinpath(*value.with_suffix("").parts)
        shadow_candidates.add(stem)
        shadow_candidates.update(
            Path(str(stem) + suffix)
            for suffix in importlib.machinery.all_suffixes()
            if suffix != ".py"
        )
        if len(value.parts) > 1:
            package_roots.add(value.parts[0])
    for package in package_roots:
        stem = root / package
        shadow_candidates.update(
            Path(str(stem) + suffix)
            for suffix in importlib.machinery.all_suffixes()
        )
    if any(
        (candidate.exists() or candidate.is_symlink()) and candidate not in approved
        for candidate in shadow_candidates
    ):
        raise ProdE2EReleaseError(
            f"prod_e2e_release_{artifact}_shadow_module_present"
        )


def _launchctl_scalar(output: str, key: str) -> str:
    match = re.search(
        rf"^\s*{re.escape(key)}\s*=\s*(.*?)\s*$", output, re.MULTILINE
    )
    return str(match.group(1) if match else "").strip()


def _launchctl_block(output: str, key: str) -> list[str]:
    lines = output.splitlines()
    start = next(
        (
            index
            for index, line in enumerate(lines)
            if line.strip() == f"{key} = {{"
        ),
        None,
    )
    if start is None:
        return []
    values: list[str] = []
    for line in lines[start + 1 :]:
        value = line.strip()
        if value == "}":
            return values
        if value:
            values.append(value)
    raise ProdE2EReleaseError(
        "prod_e2e_release_host_loaded_service_config_invalid"
    )


def _observe_launchd_job(
    *,
    label: str,
    expected_arguments: Sequence[str],
    expected_working_directory: str,
    expected_environment: Mapping[str, str],
    require_running: bool,
    required_process_environment: Mapping[str, str] | None = None,
) -> Mapping[str, Any]:
    domain = f"gui/{os.geteuid()}"
    try:
        completed = subprocess.run(
            ["/bin/launchctl", "print", f"{domain}/{label}"],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_loaded_service_observation_failed"
        ) from exc
    if completed.stderr:
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_loaded_service_observation_failed"
        )
    if completed.returncode != 0:
        if require_running:
            raise ProdE2EReleaseError(
                "prod_e2e_release_host_loaded_service_missing"
            )
        return {
            "job_present": False,
            "state": "absent",
            "pid": None,
            "program": "",
            "arguments_sha256": EMPTY_SHA256,
            "working_directory": "",
            "environment_sha256": EMPTY_SHA256,
            "process_executable": "",
            "process_arguments_sha256": EMPTY_SHA256,
            "process_working_directory": "",
            "process_environment_sha256": EMPTY_SHA256,
        }

    output = completed.stdout
    arguments = _launchctl_block(output, "arguments")
    environment_items = _launchctl_block(output, "environment")
    environment: dict[str, str] = {}
    for item in environment_items:
        if "=>" not in item:
            raise ProdE2EReleaseError(
                "prod_e2e_release_host_loaded_service_config_invalid"
            )
        key, value = (part.strip() for part in item.split("=>", 1))
        if not key or key in environment:
            raise ProdE2EReleaseError(
                "prod_e2e_release_host_loaded_service_config_invalid"
            )
        environment[key] = value
    state = _launchctl_scalar(output, "state")
    pid_text = _launchctl_scalar(output, "pid")
    pid = int(pid_text) if pid_text.isdigit() else None
    program = _launchctl_scalar(output, "program")
    working_directory = _launchctl_scalar(output, "working directory")
    expected_argument_list = list(expected_arguments)
    expected_environment_dict = dict(expected_environment)
    required_process_environment_dict = dict(required_process_environment or {})
    if any(
        key in expected_environment_dict
        and expected_environment_dict[key] != value
        for key, value in required_process_environment_dict.items()
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_loaded_service_config_invalid"
        )
    if (
        program != expected_argument_list[0]
        or arguments != expected_argument_list
        or working_directory != expected_working_directory
        or environment != expected_environment_dict
        or (require_running and (state != "running" or pid is None))
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_loaded_service_config_invalid"
        )

    process_executable = ""
    process_arguments: list[str] = []
    process_working_directory = ""
    process_environment_projection: dict[str, str] = {}
    if pid is not None:
        try:
            process = psutil.Process(pid)
            before = process.create_time()
            with process.oneshot():
                process_executable = process.exe()
                process_arguments = process.cmdline()
                process_working_directory = process.cwd()
                live_environment = process.environ()
            after = process.create_time()
        except (OSError, psutil.Error) as exc:
            raise ProdE2EReleaseError(
                "prod_e2e_release_host_process_observation_failed"
            ) from exc
        expected_process_environment = {
            **expected_environment_dict,
            **required_process_environment_dict,
        }
        process_environment_projection = {
            key: str(live_environment.get(key, ""))
            for key in expected_process_environment
        }
        if (
            before != after
            or process_executable != expected_argument_list[0]
            or process_arguments != expected_argument_list
            or process_working_directory != expected_working_directory
            or process_environment_projection != expected_process_environment
        ):
            raise ProdE2EReleaseError(
                "prod_e2e_release_host_process_identity_invalid"
            )
    elif require_running:
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_process_identity_invalid"
        )

    return {
        "job_present": True,
        "state": state,
        "pid": pid,
        "program": program,
        "arguments_sha256": _sha256_value(arguments),
        "working_directory": working_directory,
        "environment_sha256": _sha256_value(environment),
        "process_executable": process_executable,
        "process_arguments_sha256": (
            _sha256_value(process_arguments) if process_arguments else EMPTY_SHA256
        ),
        "process_working_directory": process_working_directory,
        "process_environment_sha256": (
            _sha256_value(process_environment_projection)
            if process_environment_projection
            else EMPTY_SHA256
        ),
    }


_CANONICAL_RUNTIME_ALLOWLIST_PROBE = r'''import json,pathlib,sys
root=pathlib.Path(sys.argv[1])
sys.path.insert(0,str(root))
from gateway import pnc_rca_runtime_identity as runtime
expected=root/'gateway/pnc_rca_runtime_identity.py'
if pathlib.Path(runtime.__file__).resolve()!=expected:
    raise SystemExit('module_origin_mismatch')
print(json.dumps({'rca':list(runtime.RCA_RUNTIME_RELATIVE_FILES),'gateway':list(runtime.GATEWAY_RCA_RUNTIME_RELATIVE_FILES)},sort_keys=True,separators=(',',':')))
'''


def _observe_canonical_runtime_allowlists() -> Mapping[str, Any]:
    try:
        completed = subprocess.run(
            [
                CANONICAL_HOST_PYTHON,
                "-I",
                "-B",
                "-c",
                _CANONICAL_RUNTIME_ALLOWLIST_PROBE,
                CANONICAL_HOST_ROOT,
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
            env={
                "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
                "HOME": str(Path.home()),
                "LC_ALL": "C",
                "PYTHONDONTWRITEBYTECODE": "1",
            },
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_canonical_runtime_allowlist_probe_failed"
        ) from exc
    if completed.returncode != 0 or completed.stderr or not completed.stdout:
        raise ProdE2EReleaseError(
            "prod_e2e_release_canonical_runtime_allowlist_probe_failed"
        )
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_canonical_runtime_allowlist_invalid"
        ) from exc
    if not isinstance(value, dict) or set(value) != {"rca", "gateway"}:
        raise ProdE2EReleaseError(
            "prod_e2e_release_canonical_runtime_allowlist_invalid"
        )
    normalized: dict[str, list[str]] = {}
    for scope in ("rca", "gateway"):
        files = value.get(scope)
        if (
            not isinstance(files, list)
            or not files
            or len(files) != len(set(files))
            or any(
                not isinstance(relative, str)
                or PurePosixPath(relative).is_absolute()
                or PurePosixPath(relative).suffix != ".py"
                or any(
                    part in {"", ".", ".."}
                    for part in PurePosixPath(relative).parts
                )
                for relative in files
            )
        ):
            raise ProdE2EReleaseError(
                "prod_e2e_release_canonical_runtime_allowlist_invalid"
            )
        normalized[scope] = list(files)
    union = sorted(set(normalized["rca"]) | set(normalized["gateway"]))
    return {
        **normalized,
        "union": union,
        "allowlist_sha256": _sha256_value(normalized),
        "probe_script_sha256": hashlib.sha256(
            _CANONICAL_RUNTIME_ALLOWLIST_PROBE.encode("utf-8")
        ).hexdigest(),
    }


def _validate_host_candidate_identity() -> Mapping[str, Any]:
    owned = _read_owned_json(
        Path(HOST_CANDIDATE_IDENTITY_PATH), artifact="host_candidate_identity"
    )
    body = owned.body
    candidate = body.get("candidate")
    verification = body.get("verification")
    focused = verification.get("focused_suite") if isinstance(verification, Mapping) else None
    checks = verification.get("code_checks") if isinstance(verification, Mapping) else None
    reproductions = (
        verification.get("blocker_reproductions")
        if isinstance(verification, Mapping)
        else None
    )
    production_shape = (
        verification.get("production_shape_probe")
        if isinstance(verification, Mapping)
        else None
    )
    hygiene = (
        verification.get("worktree_hygiene")
        if isinstance(verification, Mapping)
        else None
    )
    storage = body.get("receipt_storage")
    if (
        owned.sha256 != HOST_CANDIDATE_IDENTITY_SHA256
        or body.get("schema_version")
        != "pnc_rca_host_controlled_gray_independent_audit_v1"
        or body.get("scope") != "controlled-gray BOM binding only"
        or body.get("verdict") != "GO"
        or body.get("release_recommendation")
        != "eligible_for_controlled_gray_bom_binding_only"
        or body.get("deployment_authorization") is not False
        or body.get("production_mutation") is not False
        or body.get("production_actions") != []
        or not isinstance(candidate, Mapping)
        or candidate.get("commit") != HOST_FINAL_COMMIT
        or candidate.get("tree") != HOST_FINAL_TREE
        or candidate.get("parent") != HOST_FINAL_PARENT_COMMIT
        or candidate.get("repo") != CANONICAL_HOST_ROOT
        or candidate.get("git_clean") is not True
        or candidate.get("git_status") != ""
        or candidate.get("pyc_files") != []
        or candidate.get("cache_dirs") != []
        or not isinstance(focused, Mapping)
        or focused.get("result") != "PASS"
        or focused.get("passed", 0) < 171
        or not isinstance(checks, Mapping)
        or checks != {"diff_check": "PASS", "ruff": "PASS"}
        or not isinstance(reproductions, Mapping)
        or set(reproductions)
        != {
            "arbitrary_comment_body",
            "marker_only_remote_comment",
            "success_effect_v1_forged_from_current_claim",
        }
        or any(
            not isinstance(item, Mapping) or item.get("result") != "PASS"
            for item in reproductions.values()
        )
        or not isinstance(production_shape, Mapping)
        or production_shape.get("browser_issue_url") != TARGET_ISSUE_URL
        or production_shape.get("project_simple_name")
        != TARGET_PROJECT_SIMPLE_NAME
        or production_shape.get("internal_project_key_bound_to_target") is not True
        or production_shape.get("semantic_payload_sha256_valid") is not True
        or not isinstance(hygiene, Mapping)
        or hygiene
        != {"cache_dirs": 0, "git_clean": True, "pyc_files": 0}
        or not isinstance(storage, Mapping)
        or storage.get("authoritative_owner_only_path")
        != HOST_CANDIDATE_IDENTITY_PATH
        or storage.get("required_mode") != "0600"
        or storage.get("create_once") is not True
        or storage.get("integrity_algorithm") != "sha256"
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_candidate_identity_invalid"
        )
    observed_at = _timestamp(
        body.get("observed_at"), field="host_candidate_identity_observed_at"
    )
    return {
        "path": str(owned.path),
        "sha256": owned.sha256,
        "observed_at": observed_at.isoformat(),
    }


def _observe_canonical_host_binding(
    *, expected_commit: str, expected_tree: str
) -> Mapping[str, Any]:
    root = Path(CANONICAL_HOST_ROOT)
    if root.resolve(strict=True) != root or not root.is_dir():
        raise ProdE2EReleaseError("prod_e2e_release_host_root_invalid")
    _assert_source_tree_cache_and_shadow_free(
        root, expected_files=(), artifact="canonical_host"
    )
    commit = _host_git_text(root, "rev-parse", "HEAD^{commit}")
    tree = _host_git_text(root, "rev-parse", "HEAD^{tree}")
    status = _host_git_text(
        root, "status", "--porcelain=v1", "--untracked-files=all"
    )
    if commit != expected_commit or tree != expected_tree or status:
        raise ProdE2EReleaseError("prod_e2e_release_host_live_identity_mismatch")
    candidate_identity = _validate_host_candidate_identity()
    for relative in CANONICAL_RUNTIME_BOOTSTRAP_FILES:
        _host_tracked_bytes(root, commit, relative)
    allowlists = _observe_canonical_runtime_allowlists()
    runtime_files = allowlists["union"]
    _assert_source_tree_cache_and_shadow_free(
        root, expected_files=runtime_files, artifact="canonical_host"
    )
    ancestor = _git_command(
        root,
        "merge-base",
        "--is-ancestor",
        HOST_QUARANTINE_BASELINE_COMMIT,
        commit,
    )
    if ancestor.returncode != 0 or ancestor.stderr:
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_quarantine_baseline_missing"
        )
    files = {
        relative: hashlib.sha256(
            _host_tracked_bytes(root, commit, relative)
        ).hexdigest()
        for relative in runtime_files
    }
    service_configs = {}
    for label in HOST_SERVICE_LABELS:
        plist = Path.home() / "Library/LaunchAgents" / f"{label}.plist"
        try:
            service_configs[label] = hashlib.sha256(plist.read_bytes()).hexdigest()
        except OSError as exc:
            raise ProdE2EReleaseError(
                "prod_e2e_release_host_service_config_unavailable"
            ) from exc
    for relative in RETIRED_EXECUTOR_PATHS:
        live = root / relative
        tracked = _git_command(root, "cat-file", "-e", f"{commit}:{relative}")
        if live.exists() or live.is_symlink() or tracked.returncode == 0:
            raise ProdE2EReleaseError(
                "prod_e2e_release_retired_executor_present"
            )
    return {
        "root": str(root),
        "commit": commit,
        "tree": tree,
        "status_sha256": hashlib.sha256(status.encode("utf-8")).hexdigest(),
        "candidate_identity_evidence": candidate_identity,
        "required_file_sha256": files,
        "runtime_allowlists": allowlists,
        "service_config_sha256": service_configs,
        "retired_executor_paths_absent": list(RETIRED_EXECUTOR_PATHS),
    }


_LIVE_SITE_PACKAGES_PROBE = r'''import hashlib,importlib.metadata,json,os,pathlib,platform,stat,sysconfig
root=pathlib.Path(sysconfig.get_path('purelib')).resolve(strict=True)
files={}
for candidate in sorted(root.rglob('*')):
    if candidate.is_dir():
        continue
    info=os.lstat(candidate)
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise SystemExit('site_packages_file_invalid')
    files[str(candidate.relative_to(root))]=hashlib.sha256(candidate.read_bytes()).hexdigest()
distributions={}
for distribution in importlib.metadata.distributions():
    name=str(distribution.metadata.get('Name') or '').strip().lower().replace('_','-')
    version=str(distribution.version or '').strip()
    if not name or not version or (name in distributions and distributions[name]!=version):
        raise SystemExit('site_packages_distribution_invalid')
    distributions[name]=version
print(json.dumps({'python_version':platform.python_version(),'file_count':len(files),'manifest_sha256':hashlib.sha256(json.dumps(files,sort_keys=True,separators=(',',':')).encode()).hexdigest(),'distribution_count':len(distributions),'distribution_manifest_sha256':hashlib.sha256(json.dumps(distributions,sort_keys=True,separators=(',',':')).encode()).hexdigest()},sort_keys=True,separators=(',',':')))
'''


def _observe_host_dependency_environment() -> Mapping[str, Any]:
    root = Path(HOST_LIVE_ROOT)
    interpreter = root / ".venv/bin/python"
    config = root / ".venv/pyvenv.cfg"
    try:
        before = os.lstat(interpreter)
        if (
            stat.S_ISLNK(before.st_mode)
            or not stat.S_ISREG(before.st_mode)
            or interpreter.resolve(strict=True) != interpreter
        ):
            raise OSError("live interpreter is not a canonical regular file")
        interpreter_raw = interpreter.read_bytes()
        config_raw = config.read_bytes()
        after = os.lstat(interpreter)
        if (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise OSError("live interpreter changed while hashing")
    except OSError as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_live_interpreter_invalid"
        ) from exc
    try:
        completed = subprocess.run(
            [str(interpreter), "-I", "-B", "-c", _LIVE_SITE_PACKAGES_PROBE],
            check=False,
            capture_output=True,
            text=True,
            timeout=60,
            env={
                "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
                "HOME": str(Path.home()),
                "LC_ALL": "C",
                "PYTHONDONTWRITEBYTECODE": "1",
            },
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_live_dependency_probe_failed"
        ) from exc
    try:
        observed = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_live_dependency_probe_invalid"
        ) from exc
    if (
        completed.returncode != 0
        or completed.stderr
        or not isinstance(observed, dict)
        or set(observed)
        != {
            "python_version",
            "file_count",
            "manifest_sha256",
            "distribution_count",
            "distribution_manifest_sha256",
        }
        or not isinstance(observed.get("file_count"), int)
        or observed.get("file_count", 0) < 1
        or not isinstance(observed.get("distribution_count"), int)
        or observed.get("distribution_count", 0) < 1
        or _sha256(
            observed.get("manifest_sha256"), field="live_site_packages_manifest"
        )
        != observed.get("manifest_sha256")
        or _sha256(
            observed.get("distribution_manifest_sha256"),
            field="live_distribution_manifest",
        )
        != observed.get("distribution_manifest_sha256")
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_live_dependency_probe_invalid"
        )
    return {
        "venv_root": f"{HOST_LIVE_ROOT}/.venv",
        "interpreter_path": str(interpreter),
        "interpreter_sha256": hashlib.sha256(interpreter_raw).hexdigest(),
        "pyvenv_config_sha256": hashlib.sha256(config_raw).hexdigest(),
        "python_version": observed["python_version"],
        "site_packages_file_count": observed["file_count"],
        "site_packages_manifest_sha256": observed["manifest_sha256"],
        "installed_distribution_count": observed["distribution_count"],
        "installed_distribution_manifest_sha256": observed[
            "distribution_manifest_sha256"
        ],
        "site_packages_pyc_policy": "immutable_build_manifest",
        "probe_script_sha256": hashlib.sha256(
            _LIVE_SITE_PACKAGES_PROBE.encode("utf-8")
        ).hexdigest(),
    }


def _validate_host_dependency_environment(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {
        "observation",
        "lock",
        "build_receipt",
    }:
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_dependency_binding_invalid"
        )
    observation = value.get("observation")
    lock_ref = value.get("lock")
    receipt_ref = value.get("build_receipt")
    if not all(isinstance(item, Mapping) for item in (observation, lock_ref, receipt_ref)):
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_dependency_binding_invalid"
        )
    live = _observe_host_dependency_environment()
    if dict(observation) != live:
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_dependency_observation_mismatch"
        )
    if set(lock_ref) != {"path", "sha256"} or set(receipt_ref) != {
        "path",
        "sha256",
    }:
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_dependency_binding_invalid"
        )
    lock_owned = _read_owned_json(
        Path(str(lock_ref.get("path") or "")), artifact="host_dependency_lock"
    )
    receipt_owned = _read_owned_json(
        Path(str(receipt_ref.get("path") or "")),
        artifact="host_dependency_build_receipt",
    )
    lock = lock_owned.body
    receipt = receipt_owned.body
    if (
        lock_owned.sha256 != lock_ref.get("sha256")
        or set(lock)
        != {
            "schema_version",
            "python_version",
            "distribution_count",
            "distribution_manifest_sha256",
            "created_at",
        }
        or lock.get("schema_version") != "pnc_rca_live_dependency_lock_v1"
        or lock.get("python_version") != live["python_version"]
        or lock.get("distribution_count") != live["installed_distribution_count"]
        or lock.get("distribution_manifest_sha256")
        != live["installed_distribution_manifest_sha256"]
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_dependency_lock_invalid"
        )
    _timestamp(lock.get("created_at"), field="host_dependency_lock_created_at")
    if (
        receipt_owned.sha256 != receipt_ref.get("sha256")
        or set(receipt)
        != {
            "schema_version",
            "venv_root",
            "interpreter_sha256",
            "pyvenv_config_sha256",
            "site_packages_file_count",
            "site_packages_manifest_sha256",
            "installed_distribution_count",
            "installed_distribution_manifest_sha256",
            "lock_sha256",
            "site_packages_pyc_policy",
            "built_at",
            "complete",
        }
        or receipt.get("schema_version")
        != "pnc_rca_live_dependency_build_receipt_v1"
        or receipt.get("venv_root") != live["venv_root"]
        or receipt.get("interpreter_sha256") != live["interpreter_sha256"]
        or receipt.get("pyvenv_config_sha256") != live["pyvenv_config_sha256"]
        or receipt.get("site_packages_file_count")
        != live["site_packages_file_count"]
        or receipt.get("site_packages_manifest_sha256")
        != live["site_packages_manifest_sha256"]
        or receipt.get("installed_distribution_count")
        != live["installed_distribution_count"]
        or receipt.get("installed_distribution_manifest_sha256")
        != live["installed_distribution_manifest_sha256"]
        or receipt.get("lock_sha256") != lock_owned.sha256
        or receipt.get("site_packages_pyc_policy") != "immutable_build_manifest"
        or receipt.get("complete") is not True
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_dependency_build_receipt_invalid"
        )
    _timestamp(receipt.get("built_at"), field="host_dependency_built_at")
    return {
        "observation": live,
        "lock": {"path": str(lock_owned.path), "sha256": lock_owned.sha256},
        "build_receipt": {
            "path": str(receipt_owned.path),
            "sha256": receipt_owned.sha256,
        },
    }


def _observe_host_live_runtime(
    *,
    expected_host: Mapping[str, Any],
    require_running: bool = False,
    environment_phase: str = "pre",
) -> Mapping[str, Any]:
    """Re-observe the installed live tree and every resident launchd entrypoint."""

    root = Path(HOST_LIVE_ROOT)
    try:
        root_info = os.lstat(root)
        if (
            stat.S_ISLNK(root_info.st_mode)
            or not stat.S_ISDIR(root_info.st_mode)
            or root.resolve(strict=True) != root
        ):
            raise ProdE2EReleaseError(
                "prod_e2e_release_host_live_runtime_root_invalid"
            )
    except OSError as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_live_runtime_root_invalid"
        ) from exc

    expected_files = expected_host.get("runtime_file_sha256")
    expected_aggregate = _sha256(
        expected_host.get("runtime_files_sha256"),
        field="host_live_runtime_files_sha256",
    )
    if not isinstance(expected_files, Mapping) or not expected_files:
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_live_runtime_contract_invalid"
        )
    desired_origin = _canonical_https_dns_origin(
        expected_host.get("viewer_origin"), field="host_live_viewer_origin"
    )
    environment_transition = _validate_host_environment_transition(
        expected_host.get("host_environment_transition"),
        desired_origin=desired_origin,
    )
    if environment_phase not in {"pre", "post"}:
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_environment_phase_invalid"
        )
    canonical = _run_canonical_component_probe(
        desired_viewer_origin=desired_origin
    )
    expected_environment_sha256 = environment_transition[
        f"{environment_phase}_sha256"
    ]
    expected_environment_bytes = environment_transition[
        f"{environment_phase}_bytes"
    ]
    if (
        dict(expected_files) != canonical.get("runtime_files")
        or expected_aggregate != canonical.get("runtime_files_sha256")
        or expected_host.get("rca_runtime_file_sha256")
        != canonical.get("rca_runtime_files")
        or expected_host.get("rca_runtime_files_sha256")
        != canonical.get("rca_runtime_files_sha256")
        or expected_host.get("gateway_runtime_file_sha256")
        != canonical.get("gateway_runtime_files")
        or expected_host.get("gateway_runtime_files_sha256")
        != canonical.get("gateway_runtime_files_sha256")
        or expected_host.get("service_runtime_files_sha256")
        != canonical.get("service_runtime_files_sha256")
        or canonical.get("host_env_planned_viewer_origin") != desired_origin
        or canonical.get("host_env_current_sha256")
        != expected_environment_sha256
        or canonical.get("host_env_current_bytes") != expected_environment_bytes
        or (
            environment_phase == "pre"
            and (
                canonical.get("host_env_current_viewer_origin_count")
                != environment_transition["pre_viewer_origin_count"]
                or canonical.get("host_env_current_viewer_origin")
                != environment_transition["pre_viewer_origin"]
                or canonical.get("host_env_planned_sha256")
                != environment_transition["post_sha256"]
                or canonical.get("host_env_planned_bytes")
                != environment_transition["post_bytes"]
            )
        )
        or (
            environment_phase == "post"
            and (
                canonical.get("host_env_current_viewer_origin_count") != 1
                or canonical.get("host_env_current_viewer_origin")
                != desired_origin
                or canonical.get("host_env_planned_sha256")
                != expected_environment_sha256
            )
        )
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_live_runtime_contract_invalid"
        )

    observed_files: dict[str, str] = {}
    for relative, expected_sha256 in expected_files.items():
        if (
            not isinstance(relative, str)
            or PurePosixPath(relative).is_absolute()
            or any(part in {"", ".", ".."} for part in PurePosixPath(relative).parts)
            or _sha256(expected_sha256, field="host_live_runtime_file_sha256")
            != expected_sha256
        ):
            raise ProdE2EReleaseError(
                "prod_e2e_release_host_live_runtime_contract_invalid"
            )
        path = root.joinpath(*PurePosixPath(relative).parts)
        try:
            lexical = os.lstat(path)
            resolved = path.resolve(strict=True)
            if (
                stat.S_ISLNK(lexical.st_mode)
                or not stat.S_ISREG(lexical.st_mode)
                or resolved != path
            ):
                raise OSError("live runtime file identity invalid")
            observed_files[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
        except (OSError, RuntimeError) as exc:
            raise ProdE2EReleaseError(
                "prod_e2e_release_host_live_runtime_file_invalid"
            ) from exc
    observed_aggregate = _sha256_value(observed_files)
    if observed_files != dict(expected_files) or observed_aggregate != expected_aggregate:
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_live_runtime_file_mismatch"
        )

    forbidden_cache_paths: list[str] = []
    try:
        for candidate in root.rglob("*"):
            relative = candidate.relative_to(root)
            if relative.parts and relative.parts[0] in {".git", ".venv"}:
                continue
            if (
                candidate.name in {"__pycache__", ".pytest_cache"}
                or candidate.suffix == ".pyc"
            ):
                forbidden_cache_paths.append(str(relative))
    except OSError as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_live_bytecode_observation_failed"
        ) from exc
    if forbidden_cache_paths:
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_live_bytecode_present"
        )

    shadow_candidates: set[Path] = set()
    package_roots: set[str] = set()
    for relative in expected_files:
        path = PurePosixPath(relative)
        if path.suffix != ".py":
            continue
        stem = root.joinpath(*path.with_suffix("").parts)
        shadow_candidates.add(stem)
        shadow_candidates.update(
            Path(str(stem) + suffix)
            for suffix in importlib.machinery.all_suffixes()
            if suffix != ".py"
        )
        if len(path.parts) > 1:
            package_roots.add(path.parts[0])
    for package in package_roots:
        stem = root / package
        shadow_candidates.update(
            Path(str(stem) + suffix)
            for suffix in importlib.machinery.all_suffixes()
        )
    approved_paths = {root / relative for relative in expected_files}
    if any(
        (candidate.exists() or candidate.is_symlink())
        and candidate not in approved_paths
        for candidate in shadow_candidates
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_live_shadow_module_present"
        )

    dependency = _observe_host_dependency_environment()
    expected_dependency = expected_host.get("dependency_environment")
    if (
        not isinstance(expected_dependency, Mapping)
        or expected_dependency.get("observation") != dependency
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_live_dependency_mismatch"
        )

    for relative in RETIRED_EXECUTOR_PATHS:
        candidate = root.joinpath(*PurePosixPath(relative).parts)
        if candidate.exists() or candidate.is_symlink():
            raise ProdE2EReleaseError(
                "prod_e2e_release_host_live_retired_executor_present"
            )

    service_arguments: dict[str, list[str]] = {}
    service_configs: dict[str, str] = {}
    service_effective_runtime: dict[str, Mapping[str, Any]] = {}
    expected_python = f"{HOST_LIVE_ROOT}/.venv/bin/python"
    for label in HOST_SERVICE_LABELS:
        plist_path = Path.home() / "Library/LaunchAgents" / f"{label}.plist"
        try:
            raw = plist_path.read_bytes()
            body = plistlib.loads(raw)
        except (OSError, plistlib.InvalidFileException) as exc:
            raise ProdE2EReleaseError(
                "prod_e2e_release_host_live_service_config_invalid"
            ) from exc
        arguments = body.get("ProgramArguments") if isinstance(body, dict) else None
        environment = body.get("EnvironmentVariables") if isinstance(body, dict) else None
        if (
            not isinstance(arguments, list)
            or not arguments
            or any(not isinstance(item, str) or not item for item in arguments)
            or arguments[0] != expected_python
            or body.get("WorkingDirectory") != HOST_LIVE_ROOT
            or not isinstance(environment, dict)
            or any(
                not isinstance(key, str) or not isinstance(value, str)
                for key, value in environment.items()
            )
            or environment.get("PYTHONDONTWRITEBYTECODE") != "1"
        ):
            raise ProdE2EReleaseError(
                "prod_e2e_release_host_live_program_arguments_invalid"
            )
        expected_entrypoint = HOST_SERVICE_ENTRYPOINTS[label]
        if expected_entrypoint is None:
            if arguments[1:3] != ["-m", "hermes_cli.main"]:
                raise ProdE2EReleaseError(
                    "prod_e2e_release_host_live_program_arguments_invalid"
                )
        elif len(arguments) < 2 or arguments[1] != f"{HOST_LIVE_ROOT}/{expected_entrypoint}":
            raise ProdE2EReleaseError(
                "prod_e2e_release_host_live_program_arguments_invalid"
            )
        for argument in arguments:
            if not argument.startswith("/"):
                continue
            candidate = Path(argument)
            if str(candidate).startswith("/Users/songying/.hermes/") and not (
                candidate == root or candidate.is_relative_to(root)
            ):
                raise ProdE2EReleaseError(
                    "prod_e2e_release_host_live_program_arguments_invalid"
                )
        config_sha256 = hashlib.sha256(raw).hexdigest()
        if config_sha256 != expected_host.get("service_config_sha256", {}).get(label):
            raise ProdE2EReleaseError(
                "prod_e2e_release_host_live_service_config_mismatch"
            )
        service_arguments[label] = list(arguments)
        service_configs[label] = config_sha256
        service_effective_runtime[label] = _observe_launchd_job(
            label=label,
            expected_arguments=arguments,
            expected_working_directory=HOST_LIVE_ROOT,
            expected_environment=environment,
            require_running=require_running,
            required_process_environment=(
                {VIEWER_ORIGIN_ENV: desired_origin}
                if environment_phase == "post"
                else {}
            ),
        )

    return {
        "root": HOST_LIVE_ROOT,
        "runtime_file_sha256": observed_files,
        "runtime_files_sha256": observed_aggregate,
        "rca_runtime_files_sha256": canonical["rca_runtime_files_sha256"],
        "gateway_runtime_files_sha256": canonical[
            "gateway_runtime_files_sha256"
        ],
        "service_runtime_files_sha256": canonical[
            "service_runtime_files_sha256"
        ],
        "viewer_origin": desired_origin,
        "host_environment_phase": environment_phase,
        "host_environment_sha256": expected_environment_sha256,
        "service_program_arguments": service_arguments,
        "service_config_sha256": service_configs,
        "service_effective_runtime": service_effective_runtime,
        "retired_executor_paths_absent": list(RETIRED_EXECUTOR_PATHS),
        "forbidden_cache_paths": [],
        "shadow_module_paths": [],
        "bytecode_policy": "source_only_no_pyc_or_cache",
        "dependency_environment": dependency,
    }


def _validate_staged_workspace_binding(value: Mapping[str, Any]) -> Mapping[str, Any]:
    if set(value) != {
        "schema_version",
        "root",
        "manifest_path",
        "creator_path",
        "manifest_sha256",
        "closure_sha256",
        "source_commit",
        "file_sha256",
    }:
        raise ProdE2EReleaseError(
            "prod_e2e_release_workspace_binding_invalid"
        )
    root = Path(str(value.get("root") or "")).expanduser().absolute()
    allowed = Path(
        "/Users/songying/.codex/tmp/rca-prod-e2e-release-20260721/workspace"
    )
    try:
        if root == allowed or not root.is_relative_to(allowed):
            raise ProdE2EReleaseError(
                "prod_e2e_release_workspace_root_invalid"
            )
        observed = workspace_runtime.validate_staged_workspace_runtime(root)
    except (OSError, RuntimeError, workspace_runtime.WorkspaceRuntimeError) as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_workspace_binding_invalid"
        ) from exc
    identity = observed.to_dict()
    if dict(value) != identity:
        raise ProdE2EReleaseError(
            "prod_e2e_release_workspace_binding_invalid"
        )
    return identity


_CANONICAL_COMPONENT_PROBE = r'''import hashlib,json,os,pathlib,re,stat,sys
root=pathlib.Path(sys.argv[1])
env_path=pathlib.Path(sys.argv[2])
desired_viewer_origin=sys.argv[3]
sys.path.insert(0,str(root))
from gateway import pnc_rca_prod_admission as admission
from gateway import pnc_rca_runtime_identity as runtime
def file_sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()
expected_admission=root/'gateway/pnc_rca_prod_admission.py'
expected_runtime=root/'gateway/pnc_rca_runtime_identity.py'
if pathlib.Path(admission.__file__).resolve()!=expected_admission or pathlib.Path(runtime.__file__).resolve()!=expected_runtime:
    raise SystemExit('module_origin_mismatch')
info=os.lstat(env_path)
if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode)!=0o600 or info.st_uid!=os.geteuid():
    raise SystemExit('host_env_identity_invalid')
raw=env_path.read_bytes()
text=raw.decode('utf-8')
matches=[]
viewer_origins=[]
for line in text.splitlines():
    candidate=line.strip()
    if not candidate or candidate.startswith('#') or '=' not in candidate:
        continue
    name,value=candidate.split('=',1)
    if name.strip()==admission.HMAC_ENV:
        value=value.strip()
        if len(value)>=2 and value[0]==value[-1] and value[0] in {'\"',"'"}:
            value=value[1:-1]
        matches.append(value)
    elif name.strip()=='PNC_FOXGLOVE_RENDER_HOST':
        value=value.strip()
        if len(value)>=2 and value[0]==value[-1] and value[0] in {'\"',"'"}:
            value=value[1:-1]
        viewer_origins.append(value)
if len(matches)!=1 or len(viewer_origins)>1:
    raise SystemExit('host_config_invalid')
lines=text.splitlines(keepends=True)
assignment=f'PNC_FOXGLOVE_RENDER_HOST={desired_viewer_origin}\n'
if viewer_origins:
    replaced=False
    for index,line in enumerate(lines):
        candidate=line.strip()
        if not candidate or candidate.startswith('#') or '=' not in candidate:
            continue
        name,_value=candidate.split('=',1)
        if name.strip()=='PNC_FOXGLOVE_RENDER_HOST':
            if replaced:
                raise SystemExit('host_config_invalid')
            lines[index]=assignment
            replaced=True
    if not replaced:
        raise SystemExit('host_config_invalid')
    planned_text=''.join(lines)
else:
    planned_text=text+('' if not text or text.endswith('\n') else '\n')+assignment
planned_raw=planned_text.encode('utf-8')
rca_file_hashes=runtime.rca_runtime_file_hashes(root)
gateway_file_hashes=runtime.gateway_rca_runtime_file_hashes(root)
file_hashes={name:(rca_file_hashes.get(name) or gateway_file_hashes[name]) for name in sorted(set(rca_file_hashes)|set(gateway_file_hashes))}
def canonical_sha(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()
rca_sha=runtime.rca_runtime_files_sha256(root)
gateway_sha=runtime.gateway_rca_runtime_files_sha256(root)
result={
 'schema_version':'pnc_rca_canonical_component_probe_v3',
 'root':str(root),
 'runtime_files':file_hashes,
 'runtime_files_sha256':canonical_sha(file_hashes),
 'rca_runtime_files':rca_file_hashes,
 'rca_runtime_files_sha256':rca_sha,
 'gateway_runtime_files':gateway_file_hashes,
 'gateway_runtime_files_sha256':gateway_sha,
 'service_runtime_files_sha256':{
  'ai.hermes.gateway':gateway_sha,
  'local.pnc.rca-kafka-consumer':rca_sha,
  'local.pnc.rca-outbox-dispatcher':rca_sha,
  'local.pnc.rca-delivery-collector':rca_sha,
  'local.pnc.rca-delivery-dispatcher':rca_sha,
  'local.pnc.completion-notice-relay':rca_sha,
 },
 'runtime_module_path':str(expected_runtime),
 'runtime_module_sha256':file_sha(expected_runtime),
 'admission_module_path':str(expected_admission),
 'admission_module_sha256':file_sha(expected_admission),
 'hmac_method':'prod_admission_hmac_key_fingerprint_v1',
 'hmac_environment_variable':admission.HMAC_ENV,
 'hmac_config_path':str(env_path),
 'hmac_key_fingerprint':admission.hmac_key_fingerprint(matches[0]),
 'host_env_current_sha256':hashlib.sha256(raw).hexdigest(),
 'host_env_current_bytes':len(raw),
 'host_env_current_viewer_origin_count':len(viewer_origins),
 'host_env_current_viewer_origin':viewer_origins[0] if viewer_origins else None,
 'host_env_planned_sha256':hashlib.sha256(planned_raw).hexdigest(),
 'host_env_planned_bytes':len(planned_raw),
 'host_env_planned_viewer_origin':desired_viewer_origin,
 'secret_material_persisted':False,
}
print(json.dumps(result,sort_keys=True,separators=(',',':')))
'''


def _run_canonical_component_probe(
    *, desired_viewer_origin: str
) -> Mapping[str, Any]:
    root = Path(CANONICAL_HOST_ROOT)
    python, resolved_python = _canonical_host_interpreter_paths()
    desired_origin = _canonical_https_dns_origin(
        desired_viewer_origin, field="desired_viewer_origin"
    )
    try:
        interpreter_sha256 = hashlib.sha256(
            resolved_python.read_bytes()
        ).hexdigest()
        completed = subprocess.run(
            [
                str(python),
                "-I",
                "-B",
                "-c",
                _CANONICAL_COMPONENT_PROBE,
                str(root),
                CANONICAL_HOST_ENV,
                desired_origin,
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
            env={
                "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
                "HOME": str(Path.home()),
                "LC_ALL": "C",
                "PYTHONDONTWRITEBYTECODE": "1",
            },
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_canonical_component_probe_failed"
        ) from exc
    if completed.returncode != 0 or completed.stderr or not completed.stdout:
        raise ProdE2EReleaseError(
            "prod_e2e_release_canonical_component_probe_failed"
        )
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_canonical_component_probe_invalid"
        ) from exc
    expected_fields = {
        "schema_version",
        "root",
        "runtime_files",
        "runtime_files_sha256",
        "rca_runtime_files",
        "rca_runtime_files_sha256",
        "gateway_runtime_files",
        "gateway_runtime_files_sha256",
        "service_runtime_files_sha256",
        "runtime_module_path",
        "runtime_module_sha256",
        "admission_module_path",
        "admission_module_sha256",
        "hmac_method",
        "hmac_environment_variable",
        "hmac_config_path",
        "hmac_key_fingerprint",
        "host_env_current_sha256",
        "host_env_current_bytes",
        "host_env_current_viewer_origin_count",
        "host_env_current_viewer_origin",
        "host_env_planned_sha256",
        "host_env_planned_bytes",
        "host_env_planned_viewer_origin",
        "secret_material_persisted",
    }
    if (
        not isinstance(value, dict)
        or set(value) != expected_fields
        or value.get("schema_version")
        != "pnc_rca_canonical_component_probe_v3"
        or value.get("root") != CANONICAL_HOST_ROOT
        or value.get("runtime_module_path")
        != f"{CANONICAL_HOST_ROOT}/gateway/pnc_rca_runtime_identity.py"
        or value.get("admission_module_path")
        != f"{CANONICAL_HOST_ROOT}/gateway/pnc_rca_prod_admission.py"
        or value.get("hmac_method")
        != "prod_admission_hmac_key_fingerprint_v1"
        or value.get("hmac_environment_variable") != ADMISSION_HMAC_ENV
        or value.get("hmac_config_path") != CANONICAL_HOST_ENV
        or value.get("host_env_current_viewer_origin_count") not in {0, 1}
        or (
            value.get("host_env_current_viewer_origin_count") == 0
            and value.get("host_env_current_viewer_origin") is not None
        )
        or (
            value.get("host_env_current_viewer_origin_count") == 1
            and _canonical_https_dns_origin(
                value.get("host_env_current_viewer_origin"),
                field="host_env_current_viewer_origin",
            )
            != value.get("host_env_current_viewer_origin")
        )
        or value.get("host_env_planned_viewer_origin") != desired_origin
        or not isinstance(value.get("host_env_current_bytes"), int)
        or isinstance(value.get("host_env_current_bytes"), bool)
        or value.get("host_env_current_bytes", 0) < 1
        or not isinstance(value.get("host_env_planned_bytes"), int)
        or isinstance(value.get("host_env_planned_bytes"), bool)
        or value.get("host_env_planned_bytes", 0) < 1
        or value.get("secret_material_persisted") is not False
        or not isinstance(value.get("runtime_files"), dict)
        or any(
            SHA256_RE.fullmatch(str(item or "")) is None
            for item in value["runtime_files"].values()
        )
        or not isinstance(value.get("rca_runtime_files"), dict)
        or not isinstance(value.get("gateway_runtime_files"), dict)
        or any(
            SHA256_RE.fullmatch(str(item or "")) is None
            for item in value["rca_runtime_files"].values()
        )
        or any(
            SHA256_RE.fullmatch(str(item or "")) is None
            for item in value["gateway_runtime_files"].values()
        )
        or set(value["runtime_files"])
        != set(value["rca_runtime_files"]) | set(value["gateway_runtime_files"])
        or value.get("runtime_files_sha256")
        != _sha256_value(value["runtime_files"])
        or value.get("rca_runtime_files_sha256")
        != _sha256_value(value["rca_runtime_files"])
        or value.get("gateway_runtime_files_sha256")
        != _sha256_value(value["gateway_runtime_files"])
        or value.get("service_runtime_files_sha256")
        != {
            "ai.hermes.gateway": value.get("gateway_runtime_files_sha256"),
            **{
                label: value.get("rca_runtime_files_sha256")
                for label in HOST_SERVICE_LABELS
                if label != "ai.hermes.gateway"
            },
        }
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_canonical_component_probe_invalid"
        )
    for field in (
        "runtime_files_sha256",
        "rca_runtime_files_sha256",
        "gateway_runtime_files_sha256",
        "runtime_module_sha256",
        "admission_module_sha256",
        "hmac_key_fingerprint",
        "host_env_current_sha256",
        "host_env_planned_sha256",
    ):
        _sha256(value[field], field=f"canonical_component_{field}")
    return {
        **value,
        "probe_script_sha256": hashlib.sha256(
            _CANONICAL_COMPONENT_PROBE.encode("utf-8")
        ).hexdigest(),
        "interpreter_path": str(resolved_python),
        "interpreter_sha256": interpreter_sha256,
    }


def _host_environment_transition_from_probe(
    probe: Mapping[str, Any], *, desired_origin: str
) -> Mapping[str, Any]:
    return {
        "path": CANONICAL_HOST_ENV,
        "pre_sha256": probe["host_env_current_sha256"],
        "pre_bytes": probe["host_env_current_bytes"],
        "pre_viewer_origin_count": probe[
            "host_env_current_viewer_origin_count"
        ],
        "pre_viewer_origin": probe["host_env_current_viewer_origin"],
        "post_sha256": probe["host_env_planned_sha256"],
        "post_bytes": probe["host_env_planned_bytes"],
        "post_viewer_origin": desired_origin,
        "write_required": (
            probe["host_env_current_sha256"]
            != probe["host_env_planned_sha256"]
        ),
        "write_after_database_post_and_core_gate": True,
        "rollback_restores_exact_prestate": True,
        "secret_material_persisted": False,
    }


def _validate_host_environment_transition(
    value: Any, *, desired_origin: str
) -> Mapping[str, Any]:
    expected_fields = {
        "path",
        "pre_sha256",
        "pre_bytes",
        "pre_viewer_origin_count",
        "pre_viewer_origin",
        "post_sha256",
        "post_bytes",
        "post_viewer_origin",
        "write_required",
        "write_after_database_post_and_core_gate",
        "rollback_restores_exact_prestate",
        "secret_material_persisted",
    }
    if (
        not isinstance(value, Mapping)
        or set(value) != expected_fields
        or value.get("path") != CANONICAL_HOST_ENV
        or _sha256(
            value.get("pre_sha256"), field="host_environment_pre_sha256"
        )
        != value.get("pre_sha256")
        or _sha256(
            value.get("post_sha256"), field="host_environment_post_sha256"
        )
        != value.get("post_sha256")
        or not isinstance(value.get("pre_bytes"), int)
        or isinstance(value.get("pre_bytes"), bool)
        or value.get("pre_bytes", 0) < 1
        or not isinstance(value.get("post_bytes"), int)
        or isinstance(value.get("post_bytes"), bool)
        or value.get("post_bytes", 0) < 1
        or value.get("pre_viewer_origin_count") not in {0, 1}
        or (
            value.get("pre_viewer_origin_count") == 0
            and value.get("pre_viewer_origin") is not None
        )
        or (
            value.get("pre_viewer_origin_count") == 1
            and _canonical_https_dns_origin(
                value.get("pre_viewer_origin"),
                field="host_environment_pre_viewer_origin",
            )
            != value.get("pre_viewer_origin")
        )
        or value.get("post_viewer_origin") != desired_origin
        or value.get("write_required")
        is not (value.get("pre_sha256") != value.get("post_sha256"))
        or value.get("write_after_database_post_and_core_gate") is not True
        or value.get("rollback_restores_exact_prestate") is not True
        or value.get("secret_material_persisted") is not False
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_environment_transition_invalid"
        )
    return dict(value)


def _validate_vm_worker_observation(
    value: Mapping[str, Any], *, now: datetime, require_fresh: bool
) -> Mapping[str, Any]:
    if set(value) != {"observation_path", "observation_sha256"}:
        raise ProdE2EReleaseError("prod_e2e_release_worker_binding_invalid")
    owned = _read_owned_json(
        Path(str(value.get("observation_path") or "")),
        artifact="vm_worker_observation",
    )
    if owned.sha256 != value.get("observation_sha256"):
        raise ProdE2EReleaseError("prod_e2e_release_worker_binding_invalid")
    body = owned.body
    expected_fields = {
        "schema_version",
        "observed_at",
        "root",
        "commit",
        "tree",
        "tree_clean",
        "status_sha256",
        "entrypoint",
        "entrypoint_sha256",
        "entrypoint_git_mode",
        "runtime_artifact_sha256",
        "loaded_runtime_sha256",
        "interpreter_path",
        "interpreter_sha256",
        "daemon_unit_config_sha256",
        "report_unit_config_sha256",
        "report_environment",
        "report_environment_transition",
        "observer",
        "production_mutation",
    }
    observed_at = _timestamp(
        body.get("observed_at"), field="vm_worker_observed_at"
    )
    root = _absolute_remote(body.get("root"), field="worker_root")
    entrypoint = _absolute_remote(
        body.get("entrypoint"), field="worker_entrypoint"
    )
    runtime = body.get("runtime_artifact_sha256")
    report_environment = body.get("report_environment")
    report_environment_transition = body.get("report_environment_transition")
    observer = body.get("observer")
    report_origin = (
        report_environment.get("viewer_origin")
        if isinstance(report_environment, Mapping)
        else None
    )
    expected_report_env = (
        f"{VM_REPORT_ENV_VARIABLE}={report_origin}\n".encode("utf-8")
        if isinstance(report_origin, str)
        else b""
    )
    if (
        set(body) != expected_fields
        or body.get("schema_version")
        != "pnc_rca_vm_worker_identity_observation_v2"
        or body.get("production_mutation") is not False
        or not root.startswith("/home/mini/.hermes/")
        or not entrypoint.startswith(root.rstrip("/") + "/")
        or body.get("tree_clean") is not True
        or body.get("status_sha256") != EMPTY_SHA256
        or _git_oid(body.get("commit"), field="worker_commit")
        != body.get("commit")
        or _git_oid(body.get("tree"), field="worker_tree") != body.get("tree")
        or _sha256(
            body.get("entrypoint_sha256"), field="worker_entrypoint_sha256"
        )
        != body.get("entrypoint_sha256")
        or body.get("entrypoint_git_mode") not in {"100644", "100755"}
        or _sha256(
            body.get("loaded_runtime_sha256"), field="worker_loaded_runtime_sha256"
        )
        != body.get("loaded_runtime_sha256")
        or body.get("interpreter_path") != VM_INTERPRETER_PATH
        or _sha256(
            body.get("interpreter_sha256"), field="worker_interpreter_sha256"
        )
        != body.get("interpreter_sha256")
        or _sha256(
            body.get("daemon_unit_config_sha256"),
            field="worker_daemon_unit_config_sha256",
        )
        != body.get("daemon_unit_config_sha256")
        or _sha256(
            body.get("report_unit_config_sha256"),
            field="worker_report_unit_config_sha256",
        )
        != body.get("report_unit_config_sha256")
        or body.get("report_unit_config_sha256") != VM_REPORT_UNIT_SHA256
        or not isinstance(report_environment, Mapping)
        or report_environment
        != {
            "path": VM_REPORT_ENV_PATH,
            "sha256": hashlib.sha256(expected_report_env).hexdigest(),
            "bytes": len(expected_report_env),
            "owner_uid": 1000,
            "mode": "0600",
            "variable": VM_REPORT_ENV_VARIABLE,
            "viewer_origin": report_origin,
        }
        or _canonical_https_dns_origin(
            report_origin, field="vm_report_viewer_origin"
        )
        != report_origin
        or not isinstance(report_environment_transition, Mapping)
        or set(report_environment_transition)
        != {
            "path",
            "pre_exists",
            "pre_sha256",
            "pre_bytes",
            "pre_owner_uid",
            "pre_mode",
            "post_sha256",
            "post_bytes",
            "post_owner_uid",
            "post_mode",
            "post_viewer_origin",
            "parent_path",
            "pre_parent_exists",
            "pre_parent_owner_uid",
            "pre_parent_mode",
            "post_parent_owner_uid",
            "post_parent_mode",
            "parent_create_after_database_post_and_core_gate",
            "rollback_restores_exact_parent_prestate",
            "write_required",
            "write_after_database_post_and_core_gate",
            "rollback_restores_exact_prestate",
        }
        or report_environment_transition.get("path") != VM_REPORT_ENV_PATH
        or _sha256(
            report_environment_transition.get("pre_sha256"),
            field="vm_report_env_pre_sha256",
        )
        != report_environment_transition.get("pre_sha256")
        or _sha256(
            report_environment_transition.get("post_sha256"),
            field="vm_report_env_post_sha256",
        )
        != report_environment_transition.get("post_sha256")
        or report_environment_transition.get("post_sha256")
        != report_environment.get("sha256")
        or report_environment_transition.get("post_bytes")
        != report_environment.get("bytes")
        or report_environment_transition.get("post_owner_uid") != 1000
        or report_environment_transition.get("post_mode") != "0600"
        or report_environment_transition.get("post_viewer_origin")
        != report_origin
        or report_environment_transition.get("parent_path")
        != str(Path(VM_REPORT_ENV_PATH).parent)
        or report_environment_transition.get("pre_parent_exists")
        not in {True, False}
        or (
            report_environment_transition.get("pre_parent_exists") is False
            and (
                report_environment_transition.get("pre_parent_owner_uid")
                is not None
                or report_environment_transition.get("pre_parent_mode") is not None
            )
        )
        or (
            report_environment_transition.get("pre_parent_exists") is True
            and (
                report_environment_transition.get("pre_parent_owner_uid") != 1000
                or report_environment_transition.get("pre_parent_mode") != "0700"
            )
        )
        or (
            report_environment_transition.get("pre_exists") is True
            and report_environment_transition.get("pre_parent_exists") is not True
        )
        or report_environment_transition.get("post_parent_owner_uid") != 1000
        or report_environment_transition.get("post_parent_mode") != "0700"
        or report_environment_transition.get(
            "parent_create_after_database_post_and_core_gate"
        )
        is not True
        or report_environment_transition.get(
            "rollback_restores_exact_parent_prestate"
        )
        is not True
        or report_environment_transition.get("pre_exists") not in {True, False}
        or not isinstance(report_environment_transition.get("pre_bytes"), int)
        or isinstance(report_environment_transition.get("pre_bytes"), bool)
        or report_environment_transition.get("pre_bytes", -1) < 0
        or (
            report_environment_transition.get("pre_exists") is False
            and (
                report_environment_transition.get("pre_sha256") != EMPTY_SHA256
                or report_environment_transition.get("pre_bytes") != 0
                or report_environment_transition.get("pre_owner_uid") is not None
                or report_environment_transition.get("pre_mode") is not None
            )
        )
        or (
            report_environment_transition.get("pre_exists") is True
            and (
                report_environment_transition.get("pre_owner_uid") != 1000
                or report_environment_transition.get("pre_mode") != "0600"
            )
        )
        or report_environment_transition.get("write_required")
        is not (
            report_environment_transition.get("pre_sha256")
            != report_environment_transition.get("post_sha256")
        )
        or report_environment_transition.get(
            "write_after_database_post_and_core_gate"
        )
        is not True
        or report_environment_transition.get("rollback_restores_exact_prestate")
        is not True
        or not isinstance(runtime, Mapping)
        or not runtime
        or any(
            not isinstance(path, str)
            or not path
            or PurePosixPath(path).is_absolute()
            or ".." in PurePosixPath(path).parts
            or _sha256(digest, field="worker_runtime_sha256") != digest
            for path, digest in runtime.items()
        )
        or observer
        != {
            "transport": "ssh-mini-agent",
            "host": "mini@192.168.26.174",
            "command_sha256": _vm_component_probe_script_sha256(
                str(report_origin or ""), environment_phase="pre"
            ),
            "machine_identity_sha256": observer.get("machine_identity_sha256")
            if isinstance(observer, Mapping) else None,
        }
        or not isinstance(observer, Mapping)
        or _sha256(
            observer.get("machine_identity_sha256"),
            field="worker_observer_machine_identity_sha256",
        ) != observer.get("machine_identity_sha256")
        or require_fresh
        and (
            observed_at > now + MAX_FUTURE_SKEW
            or now - observed_at > MAX_FINAL_OBSERVATION_AGE
        )
    ):
        raise ProdE2EReleaseError("prod_e2e_release_worker_binding_invalid")
    return {
        "evidence_path": str(owned.path),
        "evidence_sha256": owned.sha256,
        **dict(body),
    }


def _validate_vm_hmac_observation(
    value: Mapping[str, Any],
    *,
    worker: Mapping[str, Any],
    now: datetime,
    require_fresh: bool,
) -> Mapping[str, Any]:
    if set(value) != {"observation_path", "observation_sha256"}:
        raise ProdE2EReleaseError(
            "prod_e2e_release_admission_security_invalid"
        )
    owned = _read_owned_json(
        Path(str(value.get("observation_path") or "")),
        artifact="vm_hmac_observation",
    )
    body = owned.body
    observed_at = _timestamp(
        body.get("observed_at"), field="vm_hmac_observed_at"
    )
    if (
        owned.sha256 != value.get("observation_sha256")
        or set(body)
        != {
            "schema_version",
            "observed_at",
            "method",
            "environment_variable",
            "key_fingerprint",
            "config_path",
            "config_sha256",
            "worker_root",
            "worker_commit",
            "worker_tree",
            "loaded_runtime_sha256",
            "observer",
            "secret_material_persisted",
            "production_mutation",
        }
        or body.get("schema_version")
        != "pnc_rca_vm_admission_hmac_observation_v1"
        or body.get("method") != "prod_admission_hmac_key_fingerprint_v1"
        or body.get("environment_variable") != ADMISSION_HMAC_ENV
        or body.get("config_path") != "/home/mini/.hermes/service.env"
        or _sha256(
            body.get("config_sha256"), field="vm_hmac_config_sha256"
        )
        != body.get("config_sha256")
        or _sha256(
            body.get("key_fingerprint"), field="vm_hmac_fingerprint"
        )
        != body.get("key_fingerprint")
        or body.get("worker_root") != worker.get("root")
        or body.get("worker_commit") != worker.get("commit")
        or body.get("worker_tree") != worker.get("tree")
        or _sha256(
            body.get("loaded_runtime_sha256"), field="vm_loaded_runtime_sha256"
        )
        != body.get("loaded_runtime_sha256")
        or body.get("observer")
        != {
            "transport": "ssh-mini-agent",
            "host": "mini@192.168.26.174",
            "command_sha256": _vm_component_probe_script_sha256(
                str(worker.get("report_environment", {}).get("viewer_origin") or ""),
                environment_phase="pre",
            ),
            "machine_identity_sha256": (
                body.get("observer", {}).get("machine_identity_sha256")
                if isinstance(body.get("observer"), Mapping)
                else None
            ),
        }
        or not isinstance(body.get("observer"), Mapping)
        or _sha256(
            body["observer"].get("machine_identity_sha256"),
            field="vm_hmac_machine_identity_sha256",
        )
        != body["observer"].get("machine_identity_sha256")
        or body.get("loaded_runtime_sha256")
        != worker.get("loaded_runtime_sha256")
        or body.get("secret_material_persisted") is not False
        or body.get("production_mutation") is not False
        or require_fresh
        and (
            observed_at > now + MAX_FUTURE_SKEW
            or now - observed_at > MAX_FINAL_OBSERVATION_AGE
        )
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_admission_security_invalid"
        )
    return {
        "evidence_path": str(owned.path),
        "evidence_sha256": owned.sha256,
        **dict(body),
    }


_VM_CANDIDATE_PROBE = r'''import hashlib,json,os,pathlib,stat,subprocess
root=pathlib.Path(__ROOT__)
def run(argv):
    value=subprocess.run(argv,check=False,capture_output=True,text=True,timeout=8)
    if value.returncode!=0 or value.stderr:
        raise SystemExit('candidate_probe_command_failed')
    return value.stdout.strip()
info=os.lstat(root)
if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode) or root.resolve(strict=True)!=root:
    raise SystemExit('candidate_root_invalid')
for candidate in root.rglob('*'):
    if candidate.name.endswith('.pyc') or candidate.name in {'__pycache__','.pytest_cache'}:
        raise SystemExit('candidate_python_cache_forbidden')
head=run(['git','-C',str(root),'rev-parse','HEAD^{commit}'])
tree=run(['git','-C',str(root),'rev-parse','HEAD^{tree}'])
status=run(['git','-C',str(root),'status','--porcelain=v1','--untracked-files=all'])
git_dir=pathlib.Path(run(['git','-C',str(root),'rev-parse','--absolute-git-dir'])).resolve(strict=True)
common_dir=pathlib.Path(run(['git','-C',str(root),'rev-parse','--path-format=absolute','--git-common-dir'])).resolve(strict=True)
detached=subprocess.run(['git','-C',str(root),'symbolic-ref','-q','HEAD'],check=False,capture_output=True).returncode!=0
self_contained=(git_dir==root or root in git_dir.parents) and (common_dir==root or root in common_dir.parents)
mount=run(['findmnt','-T',str(root),'-n','-o','FSTYPE,TARGET']).split(None,1)
probe=root/'.pnc-rca-release-write-probe'
blocked=False
try:
    fd=os.open(probe,os.O_WRONLY|os.O_CREAT|os.O_EXCL|getattr(os,'O_NOFOLLOW',0),0o600)
except OSError:
    blocked=True
else:
    os.close(fd)
    probe.unlink()
result={
 'root':str(root),'head':head,'tree':tree,
 'status_porcelain_sha256':hashlib.sha256(status.encode()).hexdigest(),
 'detached':detached,'git_self_contained':self_contained,
 'git_storage':{'dot_git_kind':'directory' if (root/'.git').is_dir() else 'file','git_dir':str(git_dir),'git_common_dir':str(common_dir),'self_contained':self_contained},
 'filesystem':{'type':mount[0],'mount_target':mount[1]},
 'seal':{'read_only':blocked,'write_probe_blocked':blocked},
}
print(json.dumps(result,sort_keys=True,separators=(',',':')))
'''


def _observe_vm_candidate_live(root: str) -> Mapping[str, Any]:
    script = _VM_CANDIDATE_PROBE.replace("__ROOT__", repr(root))
    wrapper = Path.home() / ".local/bin/ssh-mini-agent"
    try:
        completed = subprocess.run(
            [str(wrapper), "run_py_json"],
            input=script,
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
            env={
                **os.environ,
                "SSH_MINI_AGENT_TIMEOUT": "20",
                "SSH_MINI_TTY": "0",
                "SSH_MINI_TTY_INHERITED": "1",
            },
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_candidate_live_observation_failed"
        ) from exc
    if completed.returncode != 0 or completed.stderr or not completed.stdout:
        raise ProdE2EReleaseError(
            "prod_e2e_release_candidate_live_observation_failed"
        )
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_candidate_live_observation_invalid"
        ) from exc
    if not isinstance(value, dict) or set(value) != {
        "root",
        "head",
        "tree",
        "status_porcelain_sha256",
        "detached",
        "git_self_contained",
        "git_storage",
        "filesystem",
        "seal",
    }:
        raise ProdE2EReleaseError(
            "prod_e2e_release_candidate_live_observation_invalid"
        )
    return {
        **value,
        "observer_transport": "ssh-mini-agent",
        "observer_host": "mini@192.168.26.174",
        "observer_script_sha256": hashlib.sha256(
            script.encode("utf-8")
        ).hexdigest(),
    }


_VM_COMPONENT_PROBE = r'''import base64,hashlib,json,os,pathlib,re,shlex,stat,subprocess,sys
sys.dont_write_bytecode=True
root=pathlib.Path('/home/mini/.hermes/worker-state')
entrypoint=root/'vm_coding_worker_scheduler.py'
report_entrypoint=pathlib.Path(__REPORT_ENTRYPOINT__)
viewer_origin=__VIEWER_ORIGIN__
environment_phase=__ENVIRONMENT_PHASE__
report_env_path=pathlib.Path('/home/mini/.config/g1q3-rca/report-http.env')
runtime_files=__RUNTIME_FILES__
def run(argv,allow_nonzero=False):
    value=subprocess.run(argv,check=False,capture_output=True,text=True,timeout=8,env={**os.environ,'GIT_OPTIONAL_LOCKS':'0'})
    if not allow_nonzero and (value.returncode!=0 or value.stderr):
        raise SystemExit('vm_component_probe_command_failed')
    return value
def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()
def cache_paths():
    return sorted(str(candidate.relative_to(root)) for candidate in root.rglob('*') if '.git' not in candidate.parts and (candidate.name.endswith('.pyc') or candidate.name in {'__pycache__','.pytest_cache'}))
cache_before=cache_paths()
if cache_before:
    raise SystemExit('worker_python_cache_forbidden')
interpreter=pathlib.Path('/usr/bin/python3')
interpreter_info=os.lstat(interpreter)
if stat.S_ISLNK(interpreter_info.st_mode) or not stat.S_ISREG(interpreter_info.st_mode) or interpreter.resolve(strict=True)!=interpreter:
    raise SystemExit('worker_interpreter_invalid')
interpreter_sha=sha(interpreter)
machine_path=pathlib.Path('/etc/machine-id')
machine_value=machine_path.read_text(encoding='ascii').strip()
machine_sha=hashlib.sha256(('etc_machine_id\0'+machine_value).encode()).hexdigest()
head=run(['git','-C',str(root),'rev-parse','HEAD^{commit}']).stdout.strip()
tree=run(['git','-C',str(root),'rev-parse','HEAD^{tree}']).stdout.strip()
status=run(['git','-C',str(root),'status','--porcelain=v1','--untracked-files=all']).stdout.strip()
artifact_sha={name:sha(root/name) for name in runtime_files}
mode=run(['git','-C',str(root),'ls-tree','HEAD','--',entrypoint.name]).stdout.split()[0]
sys.path.insert(0,str(root))
import vm_rca_prod_admission as admission
env_path=pathlib.Path('/home/mini/.hermes/service.env')
info=os.lstat(env_path)
if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode) or info.st_uid!=os.geteuid() or stat.S_IMODE(info.st_mode)&0o077:
    raise SystemExit('vm_service_env_invalid')
env_raw=env_path.read_bytes()
values={}
for raw_line in env_raw.decode('utf-8').splitlines():
    line=raw_line.strip()
    if not line or line.startswith('#') or '=' not in line:
        continue
    key,value=line.split('=',1)
    if key.strip() in values:
        raise SystemExit('vm_service_env_duplicate_key')
    values[key.strip()]=value.strip().strip('\"').strip("'")
encoded=values.get(admission.HMAC_ENV,'')
if encoded:
    key=admission._load_hmac_key(encoded)
    fingerprint=hashlib.sha256(key).hexdigest()
else:
    fingerprint=''
expected_report_env=f'G1Q3_RCA_VIEWER_ORIGIN={viewer_origin}\n'.encode('utf-8')
report_env_parent=report_env_path.parent
try:
 report_parent_info=os.lstat(report_env_parent)
 report_parent_exists=True
except FileNotFoundError:
 report_parent_info=None
 report_parent_exists=False
if report_parent_exists:
 if stat.S_ISLNK(report_parent_info.st_mode) or not stat.S_ISDIR(report_parent_info.st_mode) or report_parent_info.st_uid!=os.geteuid() or stat.S_IMODE(report_parent_info.st_mode)&0o077:
  raise SystemExit('vm_report_env_parent_invalid')
 report_parent_owner_uid=report_parent_info.st_uid
 report_parent_mode=f'{stat.S_IMODE(report_parent_info.st_mode):04o}'
else:
 report_parent_owner_uid=None
 report_parent_mode=None
try:
 report_env_info=os.lstat(report_env_path)
 report_env_exists=True
except FileNotFoundError:
 report_env_info=None
 report_env_exists=False
if report_env_exists:
 if (stat.S_ISLNK(report_env_info.st_mode) or not stat.S_ISREG(report_env_info.st_mode) or report_env_info.st_uid!=os.geteuid() or stat.S_IMODE(report_env_info.st_mode)!=0o600 or report_env_info.st_nlink!=1):
  raise SystemExit('vm_report_env_invalid')
 report_env_raw=report_env_path.read_bytes()
 report_env_owner_uid=report_env_info.st_uid
 report_env_mode=f'{stat.S_IMODE(report_env_info.st_mode):04o}'
else:
 report_env_raw=b''
 report_env_owner_uid=None
 report_env_mode=None
if environment_phase=='post' and report_env_raw!=expected_report_env:
    raise SystemExit('vm_report_env_invalid')
units=['hermes-vm-coding-worker-daemon.service','g1q3-rca-report-http.service']
unit_paths={units[0]:pathlib.Path('/home/mini/.config/systemd/user/hermes-vm-coding-worker-daemon.service'),units[1]:pathlib.Path('/home/mini/.config/systemd/user/g1q3-rca-report-http.service')}
exec_starts={units[0]:__DAEMON_EXEC_START__,units[1]:__REPORT_EXEC_START__}
unit_exec_starts={units[0]:__DAEMON_EXEC_START__,units[1]:__REPORT_UNIT_EXEC_START__}
working_directories={units[0]:str(root),units[1]:'/'}
required_environment={'PYTHONDONTWRITEBYTECODE':'1','PYTHONNOUSERSITE':'1'}
services={}
for unit in units:
 show=run(['systemctl','--user','show',unit,'--property=ActiveState,SubState,MainPID,FragmentPath,DropInPaths,ExecStart,Environment,EnvironmentFiles,WorkingDirectory']).stdout
 unit_state={line.split('=',1)[0]:line.split('=',1)[1] for line in show.splitlines() if '=' in line}
 unit_path=unit_paths[unit]
 unit_info=os.lstat(unit_path)
 if stat.S_ISLNK(unit_info.st_mode) or not stat.S_ISREG(unit_info.st_mode):
  raise SystemExit('vm_unit_file_invalid')
 unit_text=unit_path.read_bytes()
 unit_lines={line.strip() for line in unit_text.decode('utf-8').splitlines() if line.strip() and not line.lstrip().startswith('#')}
 expected_exec='ExecStart='+' '.join(unit_exec_starts[unit])
 expected_working_directory='WorkingDirectory='+working_directories[unit]
 env_hardened=('Environment=PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1' in unit_lines or ('Environment=PYTHONDONTWRITEBYTECODE=1' in unit_lines and 'Environment=PYTHONNOUSERSITE=1' in unit_lines))
 exec_matches=re.findall(r'argv\[\]=(.*?)(?:\s+;|\s+\})',unit_state.get('ExecStart',''))
 effective_exec=shlex.split(exec_matches[0]) if len(exec_matches)==1 else []
 effective_environment={}
 for item in shlex.split(unit_state.get('Environment','')):
  if '=' not in item:
   raise SystemExit('vm_effective_environment_invalid')
  key,value=item.split('=',1)
  if not key or key in effective_environment:
   raise SystemExit('vm_effective_environment_invalid')
  effective_environment[key]=value
 expected_env_files='' if unit==units[0] else f'{report_env_path} (ignore_errors=no)'
 expected_process_environment=dict(required_environment)
 if unit==units[1] and environment_phase=='post':
  expected_process_environment['G1Q3_RCA_VIEWER_ORIGIN']=viewer_origin
 if (expected_exec not in unit_lines or expected_working_directory not in unit_lines or not env_hardened or (unit==units[1] and f'EnvironmentFile={report_env_path}' not in unit_lines) or unit_state.get('FragmentPath','')!=str(unit_path) or unit_state.get('DropInPaths','') or unit_state.get('EnvironmentFiles','')!=expected_env_files or ((unit!=units[1] or environment_phase=='post') and effective_exec!=exec_starts[unit]) or unit_state.get('WorkingDirectory','')!=working_directories[unit] or any(effective_environment.get(key)!=value for key,value in expected_process_environment.items())):
  raise SystemExit('vm_unit_hardening_invalid')
 selected=entrypoint if unit==units[0] else report_entrypoint
 main_pid=int(unit_state.get('MainPID','0'))
 process_executable=''
 process_arguments=[]
 process_working_directory=''
 process_environment=[]
 if main_pid:
  process_root=pathlib.Path('/proc')/str(main_pid)
  process_executable=str((process_root/'exe').resolve(strict=True))
  process_arguments=[os.fsdecode(item) for item in (process_root/'cmdline').read_bytes().split(b'\0') if item]
  process_working_directory=str((process_root/'cwd').resolve(strict=True))
  process_values={}
  for item in (process_root/'environ').read_bytes().split(b'\0'):
   if not item or b'=' not in item:
    continue
   key,value=item.split(b'=',1)
   process_values[os.fsdecode(key)]=os.fsdecode(value)
  process_environment=[f'{key}={process_values.get(key,"")}' for key in sorted(expected_process_environment)]
  if (process_executable!=str(interpreter) or process_arguments!=exec_starts[unit] or process_working_directory!=working_directories[unit] or any(process_values.get(key)!=value for key,value in expected_process_environment.items())):
   raise SystemExit('vm_process_identity_mismatch')
 services[unit]={
  'unit':unit,'active_state':unit_state.get('ActiveState',''),
  'sub_state':unit_state.get('SubState',''),'main_pid':main_pid,
  'unit_config_sha256':hashlib.sha256(unit_text).hexdigest(),
  'entrypoint':str(selected),'entrypoint_sha256':sha(selected),
  'exec_start':exec_starts[unit],
  'environment':['PYTHONDONTWRITEBYTECODE=1','PYTHONNOUSERSITE=1'],
  'fragment_path':str(unit_path),'drop_in_paths':[],
  'effective_exec_start':effective_exec,
  'effective_environment':[f'{key}={effective_environment[key]}' for key in sorted(expected_process_environment)],
  'environment_files':[] if unit==units[0] else [str(report_env_path)],'working_directory':working_directories[unit],
  'interpreter_path':str(interpreter),'interpreter_sha256':interpreter_sha,
  'process_executable':process_executable,'process_arguments':process_arguments,
  'process_working_directory':process_working_directory,
  'process_environment':process_environment,
  'viewer_origin':viewer_origin if unit==units[1] and environment_phase=='post' else '',
 }
processes=run(['ps','-eo','pid=,args=']).stdout.splitlines()
broad_http=[line.strip() for line in processes if 'http.server' in line and ('18081' in line or 'perception' in line.lower())]
head_after=run(['git','-C',str(root),'rev-parse','HEAD^{commit}']).stdout.strip()
tree_after=run(['git','-C',str(root),'rev-parse','HEAD^{tree}']).stdout.strip()
status_after=run(['git','-C',str(root),'status','--porcelain=v1','--untracked-files=all']).stdout.strip()
artifact_sha_after={name:sha(root/name) for name in runtime_files}
cache_after=cache_paths()
if head_after!=head or tree_after!=tree or status_after!=status or artifact_sha_after!=artifact_sha or cache_after:
    raise SystemExit('worker_probe_mutated_source_tree')
loaded_runtime_sha=hashlib.sha256(json.dumps(artifact_sha,sort_keys=True,separators=(',',':')).encode()).hexdigest()
result={
 'worker':{
  'root':str(root),'commit':head,'tree':tree,'tree_clean':status=='',
  'status_sha256':hashlib.sha256(status.encode()).hexdigest(),
  'entrypoint':str(entrypoint),'entrypoint_sha256':sha(entrypoint),
  'entrypoint_git_mode':mode,'runtime_artifact_sha256':artifact_sha,
  'loaded_runtime_sha256':loaded_runtime_sha,
  'interpreter_path':str(interpreter),'interpreter_sha256':interpreter_sha,
 },
 'hmac':{
  'method':'prod_admission_hmac_key_fingerprint_v1',
  'environment_variable':admission.HMAC_ENV,'key_fingerprint':fingerprint,
  'configured':bool(encoded),
  'config_path':str(env_path),'config_sha256':hashlib.sha256(env_raw).hexdigest(),
  'loaded_runtime_sha256':loaded_runtime_sha,
 },
 'machine_identity_sha256':machine_sha,
 'report_environment':{
  'path':str(report_env_path),'exists':report_env_exists,
  'sha256':hashlib.sha256(report_env_raw).hexdigest(),
  'bytes':len(report_env_raw),'owner_uid':report_env_owner_uid,
  'mode':report_env_mode,'variable':'G1Q3_RCA_VIEWER_ORIGIN',
  'viewer_origin':viewer_origin if report_env_raw==expected_report_env else None,
 },
 'report_environment_transition':{
  'path':str(report_env_path),'pre_exists':report_env_exists,
  'pre_sha256':hashlib.sha256(report_env_raw).hexdigest(),
  'pre_bytes':len(report_env_raw),'pre_owner_uid':report_env_owner_uid,
  'pre_mode':report_env_mode,
  'post_sha256':hashlib.sha256(expected_report_env).hexdigest(),
  'post_bytes':len(expected_report_env),'post_owner_uid':os.geteuid(),
  'post_mode':'0600','post_viewer_origin':viewer_origin,
  'parent_path':str(report_env_parent),'pre_parent_exists':report_parent_exists,
  'pre_parent_owner_uid':report_parent_owner_uid,'pre_parent_mode':report_parent_mode,
  'post_parent_owner_uid':os.geteuid(),'post_parent_mode':'0700',
  'parent_create_after_database_post_and_core_gate':True,
  'rollback_restores_exact_parent_prestate':True,
  'write_required':report_env_raw!=expected_report_env,
  'write_after_database_post_and_core_gate':True,
  'rollback_restores_exact_prestate':True,
 },
 'services':services,
 'report_policy':{
  'root':'/mnt/tmp','route_prefix':'/G1Q3_RCA/cases/','port':18081,
  'directory_listing':False,'path_traversal':False,'symlink_escape':False,
  'read_only':True,'broad_http_server_processes':broad_http,
  'viewer_origin':viewer_origin,'delivery_manifest_schema':'delivery_manifest_v2',
  'viz_manifest_schema':'g1q3_rca_viz_publication_v1',
  'max_concurrent_requests':4,'request_queue_size':16,
 },
 'secret_material_persisted':False,
}
print(json.dumps(result,sort_keys=True,separators=(',',':')))
'''


def _render_vm_component_probe(
    viewer_origin: str, *, environment_phase: str = "post"
) -> str:
    origin = _canonical_https_dns_origin(
        viewer_origin, field="vm_component_viewer_origin"
    )
    if environment_phase not in {"pre", "post"}:
        raise ProdE2EReleaseError(
            "prod_e2e_release_vm_environment_phase_invalid"
        )
    report_prefix = [
        VM_INTERPRETER_PATH,
        "-I",
        "-B",
        f"{PIPELINE_SOURCE_ROOT}/{VM_REPORT_ENTRYPOINT_RELATIVE}",
        "--root",
        VM_REPORT_ROOT,
        "--bind",
        "0.0.0.0",
        "--port",
        str(VM_REPORT_PORT),
        "--viewer-origin",
    ]
    return _VM_COMPONENT_PROBE.replace(
        "__RUNTIME_FILES__", repr(VM_WORKER_RUNTIME_FILES)
    ).replace(
        "__REPORT_ENTRYPOINT__",
        repr(f"{PIPELINE_SOURCE_ROOT}/{VM_REPORT_ENTRYPOINT_RELATIVE}"),
    ).replace(
        "__VIEWER_ORIGIN__", repr(origin)
    ).replace(
        "__ENVIRONMENT_PHASE__", repr(environment_phase)
    ).replace(
        "__DAEMON_EXEC_START__",
        repr([VM_INTERPRETER_PATH, "-I", "-B", VM_WORKER_ENTRYPOINT]),
    ).replace(
        "__REPORT_EXEC_START__",
        repr([*report_prefix, origin]),
    ).replace(
        "__REPORT_UNIT_EXEC_START__",
        repr([*report_prefix, f"${{{VM_REPORT_ENV_VARIABLE}}}"]),
    )


def _vm_component_probe_script_sha256(
    viewer_origin: str, *, environment_phase: str = "post"
) -> str:
    return hashlib.sha256(
        _render_vm_component_probe(
            viewer_origin, environment_phase=environment_phase
        ).encode("utf-8")
    ).hexdigest()


def _observe_vm_components_live(
    *,
    expected_viewer_origin: str | None = None,
    environment_phase: str = "post",
) -> Mapping[str, Any]:
    origin = _canonical_https_dns_origin(
        expected_viewer_origin, field="vm_component_expected_viewer_origin"
    )
    script = _render_vm_component_probe(
        origin, environment_phase=environment_phase
    )
    wrapper = Path.home() / ".local/bin/ssh-mini-agent"
    try:
        completed = subprocess.run(
            [str(wrapper), "run_py_json"],
            input=script,
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
            env={
                **os.environ,
                "SSH_MINI_AGENT_TIMEOUT": "20",
                "SSH_MINI_TTY": "0",
                "SSH_MINI_TTY_INHERITED": "1",
            },
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_vm_component_live_observation_failed"
        ) from exc
    if completed.returncode != 0 or completed.stderr or not completed.stdout:
        raise ProdE2EReleaseError(
            "prod_e2e_release_vm_component_live_observation_failed"
        )
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_vm_component_live_observation_invalid"
        ) from exc
    if not isinstance(value, dict) or set(value) != {
        "worker",
        "hmac",
        "machine_identity_sha256",
        "report_environment",
        "report_environment_transition",
        "services",
        "report_policy",
        "secret_material_persisted",
    }:
        raise ProdE2EReleaseError(
            "prod_e2e_release_vm_component_live_observation_invalid"
        )
    services = value.get("services")
    report_policy = value.get("report_policy")
    report_environment = value.get("report_environment")
    report_environment_transition = value.get("report_environment_transition")
    expected_report_env_raw = (
        f"{VM_REPORT_ENV_VARIABLE}={origin}\n".encode("utf-8")
    )
    if (
        not isinstance(services, dict)
        or set(services) != set(VM_SERVICE_UNITS)
        or not isinstance(report_environment, Mapping)
        or set(report_environment)
        != {
            "path",
            "exists",
            "sha256",
            "bytes",
            "owner_uid",
            "mode",
            "variable",
            "viewer_origin",
        }
        or report_environment.get("path") != VM_REPORT_ENV_PATH
        or report_environment.get("variable") != VM_REPORT_ENV_VARIABLE
        or _sha256(
            report_environment.get("sha256"), field="vm_report_env_sha256"
        )
        != report_environment.get("sha256")
        or not isinstance(report_environment.get("bytes"), int)
        or isinstance(report_environment.get("bytes"), bool)
        or report_environment.get("bytes", -1) < 0
        or (
            report_environment.get("exists") is False
            and (
                report_environment.get("sha256") != EMPTY_SHA256
                or report_environment.get("bytes") != 0
                or report_environment.get("owner_uid") is not None
                or report_environment.get("mode") is not None
                or report_environment.get("viewer_origin") is not None
            )
        )
        or (
            report_environment.get("exists") is True
            and (
                report_environment.get("owner_uid") != 1000
                or report_environment.get("mode") != "0600"
            )
        )
        or not isinstance(report_environment_transition, Mapping)
        or set(report_environment_transition)
        != {
            "path",
            "pre_exists",
            "pre_sha256",
            "pre_bytes",
            "pre_owner_uid",
            "pre_mode",
            "post_sha256",
            "post_bytes",
            "post_owner_uid",
            "post_mode",
            "post_viewer_origin",
            "parent_path",
            "pre_parent_exists",
            "pre_parent_owner_uid",
            "pre_parent_mode",
            "post_parent_owner_uid",
            "post_parent_mode",
            "parent_create_after_database_post_and_core_gate",
            "rollback_restores_exact_parent_prestate",
            "write_required",
            "write_after_database_post_and_core_gate",
            "rollback_restores_exact_prestate",
        }
        or report_environment_transition.get("path") != VM_REPORT_ENV_PATH
        or report_environment_transition.get("pre_exists")
        is not report_environment.get("exists")
        or report_environment_transition.get("pre_sha256")
        != report_environment.get("sha256")
        or report_environment_transition.get("pre_bytes")
        != report_environment.get("bytes")
        or report_environment_transition.get("pre_owner_uid")
        != report_environment.get("owner_uid")
        or report_environment_transition.get("pre_mode")
        != report_environment.get("mode")
        or report_environment_transition.get("post_sha256")
        != hashlib.sha256(expected_report_env_raw).hexdigest()
        or report_environment_transition.get("post_bytes")
        != len(expected_report_env_raw)
        or report_environment_transition.get("post_owner_uid") != 1000
        or report_environment_transition.get("post_mode") != "0600"
        or report_environment_transition.get("post_viewer_origin") != origin
        or report_environment_transition.get("parent_path")
        != str(Path(VM_REPORT_ENV_PATH).parent)
        or report_environment_transition.get("pre_parent_exists")
        not in {True, False}
        or (
            report_environment_transition.get("pre_parent_exists") is False
            and (
                report_environment_transition.get("pre_parent_owner_uid")
                is not None
                or report_environment_transition.get("pre_parent_mode") is not None
            )
        )
        or (
            report_environment_transition.get("pre_parent_exists") is True
            and (
                report_environment_transition.get("pre_parent_owner_uid") != 1000
                or report_environment_transition.get("pre_parent_mode") != "0700"
            )
        )
        or (
            report_environment_transition.get("pre_exists") is True
            and report_environment_transition.get("pre_parent_exists") is not True
        )
        or report_environment_transition.get("post_parent_owner_uid") != 1000
        or report_environment_transition.get("post_parent_mode") != "0700"
        or report_environment_transition.get(
            "parent_create_after_database_post_and_core_gate"
        )
        is not True
        or report_environment_transition.get(
            "rollback_restores_exact_parent_prestate"
        )
        is not True
        or report_environment_transition.get("write_required")
        is not (
            report_environment.get("sha256")
            != hashlib.sha256(expected_report_env_raw).hexdigest()
        )
        or report_environment_transition.get(
            "write_after_database_post_and_core_gate"
        )
        is not True
        or report_environment_transition.get("rollback_restores_exact_prestate")
        is not True
        or (
            environment_phase == "post"
            and report_environment
            != {
                "path": VM_REPORT_ENV_PATH,
                "exists": True,
                "sha256": hashlib.sha256(expected_report_env_raw).hexdigest(),
                "bytes": len(expected_report_env_raw),
                "owner_uid": 1000,
                "mode": "0600",
                "variable": VM_REPORT_ENV_VARIABLE,
                "viewer_origin": origin,
            }
        )
        or not isinstance(report_policy, dict)
        or report_policy
        != {
            "root": VM_REPORT_ROOT,
            "route_prefix": VM_REPORT_ROUTE_PREFIX,
            "port": VM_REPORT_PORT,
            "directory_listing": False,
            "path_traversal": False,
            "symlink_escape": False,
            "read_only": True,
            "broad_http_server_processes": [],
            "viewer_origin": origin,
            "delivery_manifest_schema": "delivery_manifest_v2",
            "viz_manifest_schema": "g1q3_rca_viz_publication_v1",
            "max_concurrent_requests": 4,
            "request_queue_size": 16,
        }
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_vm_component_live_observation_invalid"
        )
    expected_exec_starts = {
        VM_DAEMON_UNIT: [
            VM_INTERPRETER_PATH,
            "-I",
            "-B",
            VM_WORKER_ENTRYPOINT,
        ],
        VM_REPORT_UNIT: [
            VM_INTERPRETER_PATH,
            "-I",
            "-B",
            f"{PIPELINE_SOURCE_ROOT}/{VM_REPORT_ENTRYPOINT_RELATIVE}",
            "--root",
            VM_REPORT_ROOT,
            "--bind",
            "0.0.0.0",
            "--port",
            str(VM_REPORT_PORT),
            "--viewer-origin",
            origin,
        ],
    }
    expected_working_directories = {
        VM_DAEMON_UNIT: VM_WORKER_ROOT,
        VM_REPORT_UNIT: "/",
    }
    expected_effective_environment = [
        "PYTHONDONTWRITEBYTECODE=1",
        "PYTHONNOUSERSITE=1",
    ]
    for unit, service in services.items():
        effective_environment = list(expected_effective_environment)
        environment_files: list[str] = []
        viewer_origin = ""
        if unit == VM_REPORT_UNIT:
            environment_files = [VM_REPORT_ENV_PATH]
            if environment_phase == "post":
                effective_environment.append(f"{VM_REPORT_ENV_VARIABLE}={origin}")
                effective_environment.sort()
                viewer_origin = origin
        if (
            not isinstance(service, dict)
            or set(service)
            != {
                "unit",
                "active_state",
                "sub_state",
                "main_pid",
                "unit_config_sha256",
                "entrypoint",
                "entrypoint_sha256",
                "exec_start",
                "environment",
                "fragment_path",
                "drop_in_paths",
                "effective_exec_start",
                "effective_environment",
                "environment_files",
                "working_directory",
                "interpreter_path",
                "interpreter_sha256",
                "process_executable",
                "process_arguments",
                "process_working_directory",
                "process_environment",
                "viewer_origin",
            }
            or service.get("unit") != unit
            or service.get("exec_start") != expected_exec_starts[unit]
            or service.get("environment") != expected_effective_environment
            or service.get("fragment_path")
            != (
                VM_DAEMON_LIVE_UNIT_PATH
                if unit == VM_DAEMON_UNIT
                else VM_REPORT_LIVE_UNIT_PATH
            )
            or service.get("drop_in_paths") != []
            or (
                (unit != VM_REPORT_UNIT or environment_phase == "post")
                and service.get("effective_exec_start")
                != expected_exec_starts[unit]
            )
            or service.get("effective_environment")
            != effective_environment
            or service.get("environment_files") != environment_files
            or service.get("viewer_origin") != viewer_origin
            or service.get("working_directory")
            != expected_working_directories[unit]
            or service.get("interpreter_path") != VM_INTERPRETER_PATH
            or _sha256(
                service.get("interpreter_sha256"), field="vm_interpreter_sha256"
            )
            != service.get("interpreter_sha256")
            or _sha256(
                service.get("unit_config_sha256"), field="vm_unit_config_sha256"
            )
            != service.get("unit_config_sha256")
            or _sha256(
                service.get("entrypoint_sha256"), field="vm_entrypoint_sha256"
            )
            != service.get("entrypoint_sha256")
            or (
                service.get("main_pid", 0) > 0
                and (
                    service.get("process_executable") != VM_INTERPRETER_PATH
                    or service.get("process_arguments")
                    != expected_exec_starts[unit]
                    or service.get("process_working_directory")
                    != expected_working_directories[unit]
                    or service.get("process_environment")
                    != effective_environment
                )
            )
            or (
                service.get("main_pid") == 0
                and (
                    service.get("process_executable") != ""
                    or service.get("process_arguments") != []
                    or service.get("process_working_directory") != ""
                    or service.get("process_environment") != []
                )
            )
        ):
            raise ProdE2EReleaseError(
                "prod_e2e_release_vm_component_live_observation_invalid"
            )
    if isinstance(value.get("worker"), dict):
        value["worker"]["daemon_unit_config_sha256"] = services[
            VM_DAEMON_UNIT
        ].get("unit_config_sha256")
        value["worker"]["report_unit_config_sha256"] = services[
            VM_REPORT_UNIT
        ].get("unit_config_sha256")
        value["worker"]["report_environment"] = {
            "path": VM_REPORT_ENV_PATH,
            "sha256": report_environment_transition["post_sha256"],
            "bytes": report_environment_transition["post_bytes"],
            "owner_uid": report_environment_transition["post_owner_uid"],
            "mode": report_environment_transition["post_mode"],
            "variable": VM_REPORT_ENV_VARIABLE,
            "viewer_origin": origin,
        }
        value["worker"]["report_environment_transition"] = dict(
            report_environment_transition
        )
        value["worker"]["interpreter_path"] = services[VM_DAEMON_UNIT].get(
            "interpreter_path"
        )
        value["worker"]["interpreter_sha256"] = services[VM_DAEMON_UNIT].get(
            "interpreter_sha256"
        )
    return {
        **value,
        "observer_transport": "ssh-mini-agent",
        "observer_host": "mini@192.168.26.174",
        "observer_script_sha256": _vm_component_probe_script_sha256(
            origin, environment_phase=environment_phase
        ),
    }


def _owner_only_directory(path: Path, *, artifact: str) -> Path:
    candidate = path.expanduser().absolute()
    try:
        observed = os.lstat(candidate)
    except OSError as exc:
        raise ProdE2EReleaseError(
            f"prod_e2e_release_{artifact}_directory_unavailable"
        ) from exc
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISDIR(observed.st_mode)
        or stat.S_IMODE(observed.st_mode) != 0o700
        or observed.st_uid != os.geteuid()
    ):
        raise ProdE2EReleaseError(
            f"prod_e2e_release_{artifact}_directory_not_owner_only"
        )
    return candidate


def _publish_no_clobber(path: Path, value: Mapping[str, Any]) -> str:
    output = path.expanduser().absolute()
    _owner_only_directory(output.parent, artifact="output")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    payload = _canonical_bytes(value, newline=True)
    try:
        descriptor = os.open(output, flags, 0o600)
    except OSError as exc:
        raise ProdE2EReleaseError("prod_e2e_release_output_exists") from exc
    write_failed = False
    try:
        written = 0
        view = memoryview(payload)
        while written < len(payload):
            count = os.write(descriptor, view[written:])
            if not isinstance(count, int) or count <= 0:
                write_failed = True
                raise OSError("owner artifact write made no progress")
            written += count
        os.fsync(descriptor)
        observed_size = os.fstat(descriptor).st_size
        if written != len(payload) or observed_size != len(payload):
            write_failed = True
            raise OSError("owner artifact write was truncated")
    except OSError as exc:
        write_failed = True
        raise ProdE2EReleaseError("prod_e2e_release_output_write_failed") from exc
    finally:
        os.close(descriptor)
        if write_failed:
            try:
                output.unlink()
            except OSError:
                pass
    try:
        persisted = output.read_bytes()
    except OSError as exc:
        raise ProdE2EReleaseError("prod_e2e_release_output_verify_failed") from exc
    if persisted != payload:
        try:
            output.unlink()
        except OSError:
            pass
        raise ProdE2EReleaseError("prod_e2e_release_output_verify_failed")
    directory_fd = os.open(
        output.parent,
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_CLOEXEC", 0),
    )
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
    return hashlib.sha256(persisted).hexdigest()


def _release_id(value: Any) -> str:
    text = str(value or "").strip()
    if RELEASE_ID_RE.fullmatch(text) is None:
        raise ProdE2EReleaseError("prod_e2e_release_release_id_invalid")
    return text


def _absolute_remote(value: Any, *, field: str) -> str:
    text = str(value or "").strip()
    path = PurePosixPath(text)
    if not text or not path.is_absolute() or ".." in path.parts or "\x00" in text:
        raise ProdE2EReleaseError(f"prod_e2e_release_{field}_invalid")
    return str(path)


def _validate_gap_ledger(owned: OwnedJson) -> Mapping[str, Any]:
    body = owned.body
    if owned.sha256 != GAP_LEDGER_FILE_SHA256:
        raise ProdE2EReleaseError("prod_e2e_release_gap_ledger_sha_mismatch")
    missing = body.get("missing_events")
    if (
        body.get("schema_version") != GAP_LEDGER_SCHEMA_VERSION
        or body.get("production_mutation") is not False
        or body.get("raw_payloads_persisted") is not False
        or body.get("topic") != TOPIC
        or body.get("partition") != PARTITION
        or body.get("live_t0_offset") != LIVE_T0_OFFSET
        or body.get("replay_end_offset") != LIVE_T0_OFFSET
        or body.get("accepted_count") != 78
        or body.get("already_in_live_count") != 5
        or body.get("missing_live_count") != MISSING_LIVE_COUNT
        or body.get("deferred_missing_count") != DEFERRED_MISSING_COUNT
        or body.get("daily_started_attempt_quota")
        != DAILY_STARTED_ATTEMPT_QUOTA
        or body.get("immediate_backfill_event_uid") != TARGET_EVENT_UID
        or not isinstance(missing, list)
        or len(missing) != MISSING_LIVE_COUNT
    ):
        raise ProdE2EReleaseError("prod_e2e_release_gap_ledger_invalid")
    event_uids = [str(item.get("event_uid") or "") for item in missing if isinstance(item, Mapping)]
    offsets = [item.get("offset") for item in missing if isinstance(item, Mapping)]
    if (
        len(event_uids) != MISSING_LIVE_COUNT
        or len(set(event_uids)) != MISSING_LIVE_COUNT
        or len(set(offsets)) != MISSING_LIVE_COUNT
        or event_uids.count(TARGET_EVENT_UID) != 1
    ):
        raise ProdE2EReleaseError("prod_e2e_release_gap_ledger_identity_invalid")
    target = next(item for item in missing if item.get("event_uid") == TARGET_EVENT_UID)
    expected_target = {
        "business_key": TARGET_BUSINESS_KEY,
        "event_uid": TARGET_EVENT_UID,
        "generation": 1,
        "offset": TARGET_OFFSET,
        "raw_sha256": TARGET_RAW_SHA256,
        "submission_key": TARGET_SUBMISSION_KEY,
        "work_item_id": TARGET_WORK_ITEM_ID,
    }
    if dict(target) != expected_target:
        raise ProdE2EReleaseError("prod_e2e_release_target_event_mismatch")
    return {
        "path": str(owned.path),
        "sha256": owned.sha256,
        "policy_sha256": _sha256(body.get("policy_sha256"), field="gap_policy_sha256"),
        "accepted_count": 78,
        "already_in_live_count": 5,
        "missing_live_count": MISSING_LIVE_COUNT,
        "deferred_missing_count": DEFERRED_MISSING_COUNT,
        "daily_started_attempt_quota": DAILY_STARTED_ATTEMPT_QUOTA,
        "immediate_backfill_event": expected_target,
    }


def _validate_field_preread(
    owned: OwnedJson,
    *,
    now: datetime | None = None,
    require_fresh: bool = False,
) -> Mapping[str, Any]:
    body = owned.body
    fields = body.get("fields")
    observed_at = _timestamp(
        body.get("observed_at"), field="field_preread_observed_at"
    )
    if (
        owned.sha256 != FIELD_PREREAD_FILE_SHA256
        or body.get("schema_version") != FIELD_PREREAD_SCHEMA_VERSION
        or body.get("production_mutation") is not False
        or body.get("project_key") != TARGET_PROJECT_KEY
        or body.get("work_item_id") != TARGET_WORK_ITEM_ID
        or not isinstance(fields, Mapping)
        or set(fields) != set(TARGET_FIELD_KEYS)
        or any(
            not isinstance(fields[key], Mapping)
            or fields[key].get("empty") is not True
            or fields[key].get("sha256") != EMPTY_SHA256
            or fields[key].get("utf8_bytes") != 0
            for key in TARGET_FIELD_KEYS
        )
        or require_fresh
        and (
            now is None
            or observed_at > now + MAX_FUTURE_SKEW
            or now - observed_at > MAX_FINAL_OBSERVATION_AGE
        )
    ):
        raise ProdE2EReleaseError("prod_e2e_release_field_preread_invalid")
    return {
        "path": str(owned.path),
        "sha256": owned.sha256,
        "observed_at": observed_at.isoformat(),
        "project_key": TARGET_PROJECT_KEY,
        "work_item_id": TARGET_WORK_ITEM_ID,
        "empty_field_keys": list(TARGET_FIELD_KEYS),
    }


def _validate_input_preread(
    owned: OwnedJson,
    *,
    now: datetime | None = None,
    require_fresh: bool = False,
) -> Mapping[str, Any]:
    body = owned.body
    observed_at = _timestamp(
        body.get("observed_at"), field="input_preread_observed_at"
    )
    if (
        owned.sha256 != INPUT_PREREAD_FILE_SHA256
        or body.get("schema_version") != INPUT_PREREAD_SCHEMA_VERSION
        or body.get("production_mutation") is not False
        or body.get("raw_values_persisted") is not False
        or body.get("source") != "meegle"
        or body.get("status") != "fields_extracted"
        or body.get("project_key") != TARGET_PROJECT_KEY
        or body.get("work_item_id") != TARGET_WORK_ITEM_ID
        or body.get("context_present") is not True
        or body.get("context_utf8_bytes", 0) < 1
        or SHA256_RE.fullmatch(str(body.get("context_sha256") or "")) is None
        or body.get("remote_reference_present") is not True
        or body.get("remote_reference_valid") is not True
        or body.get("frame_reference_present") is not True
        or body.get("frame_reference_valid") is not True
        or body.get("function_category_present") is not True
        or body.get("validation_blocker_kind") != ""
        or body.get("error_classes") != []
        or require_fresh
        and (
            now is None
            or observed_at > now + MAX_FUTURE_SKEW
            or now - observed_at > MAX_FINAL_OBSERVATION_AGE
        )
    ):
        raise ProdE2EReleaseError("prod_e2e_release_input_preread_invalid")
    return {
        "path": str(owned.path),
        "sha256": owned.sha256,
        "observed_at": observed_at.isoformat(),
        "source": "meegle",
        "status": "fields_extracted",
        "context_sha256": str(body["context_sha256"]),
        "context_utf8_bytes": int(body["context_utf8_bytes"]),
        "remote_reference_valid": True,
        "frame_reference_valid": True,
        "function_category_present": True,
        "validation_blocker_kind": "",
    }


def _validate_closure_audit(owned: OwnedJson) -> Mapping[str, Any]:
    body = owned.body
    repo = body.get("repo")
    reachable = body.get("reachable")
    production = body.get("production_path")
    side_effects = body.get("side_effects")
    classified_references = (
        reachable.get("classified_forbidden_root_references")
        if isinstance(reachable, Mapping)
        else None
    )
    output_binding = (
        production.get("fixed_service_output_binding")
        if isinstance(production, Mapping)
        else None
    )
    if (
        owned.sha256 != PIPELINE_CLOSURE_FILE_SHA256
        or body.get("schema_version") != CLOSURE_AUDIT_SCHEMA_VERSION
        or body.get("ok") is not True
        or body.get("entrypoint") != PIPELINE_ENTRYPOINT
        or body.get("evidence_core_sha256") != PIPELINE_CLOSURE_CORE_SHA256
        or not isinstance(repo, Mapping)
        or repo.get("root") != PIPELINE_SOURCE_ROOT
        or repo.get("commit") != PIPELINE_COMMIT
        or repo.get("tree") != PIPELINE_TREE
        or repo.get("tree_clean") is not True
        or repo.get("status_sha256") != EMPTY_SHA256
        or not isinstance(reachable, Mapping)
        or reachable.get("blockers") != []
        or reachable.get("hits") != []
        or not isinstance(reachable.get("modules"), list)
        or reachable.get("module_count") != len(reachable["modules"])
        or reachable.get("module_count", 0) < 1
        or not isinstance(production, Mapping)
        or production.get("raw_mcap_execution_reachable") is not False
        or production.get("remote_reader_contract_reachable") is not True
        or production.get("forbidden_output_root_reachable") is not False
        or production.get("perception_test_team_write_reachable") is not False
        or production.get("classified_forbidden_root_reference_count") != 12
        or not isinstance(classified_references, list)
        or len(classified_references) != 12
        or not isinstance(output_binding, Mapping)
        or output_binding.get("applicable") is not True
        or output_binding.get("enforced") is not True
        or output_binding.get("entrypoint") != PIPELINE_ENTRYPOINT
        or output_binding.get("entrypoint_sha256") != PIPELINE_ENTRYPOINT_SHA256
        or output_binding.get("vm_task_root_pattern")
        != "/mnt/tmp/<submission_key>/"
        or output_binding.get("cifs_task_root_pattern")
        != (
            "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/"
            "tmp/<submission_key>/"
        )
        or output_binding.get("identity_validation_precedes_pipeline") is not True
        or output_binding.get("pipeline_output_root_from_validated_identity")
        is not True
        or not isinstance(side_effects, Mapping)
        or side_effects
        != {
            "docker_started": False,
            "mcap_started": False,
            "production_mutation": False,
        }
    ):
        raise ProdE2EReleaseError("prod_e2e_release_closure_audit_invalid")
    mirror = os.lstat(owned.path)
    return {
        "path": str(owned.path),
        "sha256": owned.sha256,
        "sealed_mirror": {
            "path": str(owned.path),
            "sha256": owned.sha256,
            "mode": f"{stat.S_IMODE(mirror.st_mode):04o}",
            "owner_uid": mirror.st_uid,
            "vm_source_path": PIPELINE_CLOSURE_VM_PATH,
            "cifs_source_path": PIPELINE_CLOSURE_CIFS_PATH,
            "vm_source_mode": "0755_mount_enforced_staging_only",
        },
        "evidence_core_sha256": PIPELINE_CLOSURE_CORE_SHA256,
        "entrypoint": PIPELINE_ENTRYPOINT,
        "reachable_module_count": reachable["module_count"],
        "reachable_hit_count": 0,
        "classified_forbidden_root_reference_count": 12,
        "raw_mcap_execution_reachable": False,
        "perception_test_team_write_reachable": False,
        "fixed_service_output_root_enforced": True,
        "remote_reader_contract_reachable": True,
    }


def _validate_cross_contract_pass(
    owned: OwnedJson,
    *,
    release_id: str,
    components: Mapping[str, Any],
) -> Mapping[str, Any]:
    body = owned.body
    vm_candidate = body.get("vm_candidate")
    assertions = body.get("assertions")
    delivery = body.get("verified_delivery")
    vm_bundle = body.get("vm_bundle")
    supersedes = body.get("supersedes")
    bindings = body.get("bindings")
    updates = delivery.get("field_updates") if isinstance(delivery, Mapping) else None
    report_url = str(delivery.get("report_url") or "") if isinstance(delivery, Mapping) else ""
    expected_bindings = {
        "component_binding_sha256": components["sha256"],
        "host_commit": components["host"]["commit"],
        "host_tree": components["host"]["tree"],
        "workspace_manifest_sha256": components["workspace"]["manifest_sha256"],
        "workspace_closure_sha256": components["workspace"]["closure_sha256"],
        "worker_commit": components["worker"]["commit"],
        "worker_tree": components["worker"]["tree"],
        "pipeline_commit": PIPELINE_COMMIT,
        "pipeline_tree": PIPELINE_TREE,
        "pipeline_closure_file_sha256": PIPELINE_CLOSURE_FILE_SHA256,
        "pipeline_closure_core_sha256": PIPELINE_CLOSURE_CORE_SHA256,
        "viewer_origin": components["viewer_proxy"]["public_origin"],
        "viewer_proxy_config_sha256": components["viewer_proxy"]["config"][
            "sha256"
        ],
    }
    if (
        owned.sha256 != CROSS_CONTRACT_PASS_FILE_SHA256
        or body.get("schema_version") != CROSS_CONTRACT_PASS_SCHEMA_VERSION
        or body.get("release_id") != release_id
        or bindings != expected_bindings
        or body.get("verdict") != "pass"
        or body.get("issue_id") != TARGET_WORK_ITEM_ID
        or not isinstance(vm_candidate, Mapping)
        or vm_candidate.get("head") != PIPELINE_COMMIT
        or vm_candidate.get("tree") != PIPELINE_TREE
        or not isinstance(assertions, Mapping)
        or assertions
        != {
            "canonical_host_verifier_passed": True,
            "exact_issue_target": True,
            "production_write_not_performed": True,
            "report_field_is_manifest_html_url": True,
            "result_field_nonempty": True,
        }
        or not isinstance(delivery, Mapping)
        or delivery.get("send_performed") is not False
        or delivery.get("project_key") != TARGET_PROJECT_KEY
        or delivery.get("project_simple_name") != TARGET_PROJECT_SIMPLE_NAME
        or delivery.get("work_item_id") != TARGET_WORK_ITEM_ID
        or delivery.get("issue_url")
        != TARGET_ISSUE_URL
        or delivery.get("report_link_kind") != "manifest_html"
        or not report_url.startswith(
            f"{VIEWER_PROXY_UPSTREAM_ORIGIN}{VM_REPORT_ROUTE_PREFIX}"
        )
        or not report_url.endswith("/index.html")
        or not isinstance(updates, list)
        or len(updates) != 2
        or [item.get("field_key") for item in updates if isinstance(item, Mapping)]
        != ["field_9193cb", "field_8c912e"]
        or any(
            not isinstance(item, Mapping)
            or not str(item.get("field_value") or "").strip()
            or item.get("field_value_sha256")
            != hashlib.sha256(
                str(item.get("field_value") or "").encode("utf-8")
            ).hexdigest()
            or item.get("field_value_utf8_bytes")
            != len(str(item.get("field_value") or "").encode("utf-8"))
            for item in updates
        )
        or updates[1].get("field_value") != report_url
        or not isinstance(vm_bundle, Mapping)
        or vm_bundle.get("production_mutation") is not False
        or vm_bundle.get("raw_payload_read") is not False
        or vm_bundle.get("docker_started_by_payload") is not False
        or SHA256_RE.fullmatch(str(vm_bundle.get("sha256") or "")) is None
        or SHA256_RE.fullmatch(
            str(vm_bundle.get("diagnostic_viz_sha256") or "")
        )
        is None
        or not isinstance(supersedes, Mapping)
        or supersedes.get("verdict") != "gap"
        or supersedes.get("sha256") != SUPERSEDED_CROSS_CONTRACT_GAP_SHA256
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_cross_contract_pass_invalid"
        )
    return {
        "path": str(owned.path),
        "sha256": owned.sha256,
        "issue_id": TARGET_WORK_ITEM_ID,
        "vm_head": PIPELINE_COMMIT,
        "vm_tree": PIPELINE_TREE,
        "vm_bundle_sha256": str(vm_bundle["sha256"]),
        "diagnostic_viz_sha256": str(vm_bundle["diagnostic_viz_sha256"]),
        "field_keys": ["field_9193cb", "field_8c912e"],
        "structural_contract": {
            "project_key": TARGET_PROJECT_KEY,
            "project_simple_name": TARGET_PROJECT_SIMPLE_NAME,
            "work_item_id": TARGET_WORK_ITEM_ID,
            "issue_url": TARGET_ISSUE_URL,
            "field_keys": list(TARGET_FIELD_KEYS),
            "result_nonempty": True,
            "report_is_manifest_html_url": True,
            "production_values_predetermined": False,
            "production_lineage_predetermined": False,
        },
        "bindings": expected_bindings,
        "send_performed": False,
    }


def _certificate_dns_name_matches(hostname: str, san_dns_name: str) -> bool:
    if san_dns_name == hostname:
        return True
    if not san_dns_name.startswith("*.") or san_dns_name.count("*") != 1:
        return False
    suffix = san_dns_name[2:]
    host_labels = hostname.split(".")
    suffix_labels = suffix.split(".")
    return (
        len(host_labels) == len(suffix_labels) + 1
        and host_labels[1:] == suffix_labels
        and all(_DNS_LABEL_RE.fullmatch(label) for label in suffix_labels)
    )


def _validate_viewer_proxy_static_evidence() -> Mapping[str, Any]:
    config_path = Path(VIEWER_PROXY_CONFIG_PATH)
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    if not hasattr(os, "O_NOFOLLOW"):
        raise ProdE2EReleaseError(
            "prod_e2e_release_viewer_proxy_config_no_follow_unavailable"
        )
    try:
        descriptor = os.open(config_path, flags | os.O_NOFOLLOW)
        try:
            before = os.fstat(descriptor)
            raw = os.read(descriptor, VIEWER_PROXY_CONFIG_BYTES + 1)
            after = os.fstat(descriptor)
            lexical = os.lstat(config_path)
        finally:
            os.close(descriptor)
    except OSError as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_viewer_proxy_config_unavailable"
        ) from exc
    if (
        not stat.S_ISREG(before.st_mode)
        or stat.S_IMODE(before.st_mode) != 0o644
        or before.st_uid != os.geteuid()
        or before.st_nlink != 1
        or stat.S_ISLNK(lexical.st_mode)
        or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        or (after.st_dev, after.st_ino) != (lexical.st_dev, lexical.st_ino)
        or len(raw) != VIEWER_PROXY_CONFIG_BYTES
        or hashlib.sha256(raw).hexdigest() != VIEWER_PROXY_CONFIG_SHA256
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_viewer_proxy_config_invalid"
        )

    static = _read_owned_json(
        Path(VIEWER_PROXY_STATIC_RECEIPT_PATH),
        artifact="viewer_proxy_static_receipt",
    )
    body = static.body
    scope = body.get("scope")
    config = body.get("config")
    route = body.get("route")
    checks = body.get("checks")
    method_matrix = body.get("method_matrix")
    if (
        static.sha256 != VIEWER_PROXY_STATIC_RECEIPT_SHA256
        or body.get("schema_version")
        != "g1q3_rca_nginx_proxy_static_test_v2"
        or body.get("ok") is not True
        or not isinstance(scope, Mapping)
        or scope
        != {
            "live_proxy_observed": False,
            "live_viewer_config_modified": False,
            "live_viewer_reloaded": False,
            "nginx_binary_available_locally": False,
            "nginx_t_executed": False,
            "static_config_test_only": True,
        }
        or config
        != {
            "bytes": VIEWER_PROXY_CONFIG_BYTES,
            "path": VIEWER_PROXY_CONFIG_PATH,
            "sha256": VIEWER_PROXY_CONFIG_SHA256,
        }
        or not isinstance(route, Mapping)
        or route.get("allowed_methods") != ["GET", "HEAD", "OPTIONS"]
        or route.get("directory_listing") is not False
        or route.get("public_prefix") != "/g1q3-rca-artifacts/"
        or route.get("upstream") != VIEWER_PROXY_UPSTREAM_ORIGIN
        or route.get("passes_range") is not True
        or route.get("passes_if_range") is not True
        or not isinstance(checks, Mapping)
        or len(checks) < 25
        or any(value is not True for value in checks.values())
        or method_matrix
        != {
            "CONNECT": False,
            "DELETE": False,
            "GET": True,
            "HEAD": True,
            "OPTIONS": True,
            "PATCH": False,
            "POST": False,
            "PUT": False,
            "TRACE": False,
        }
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_viewer_proxy_static_receipt_invalid"
        )
    return {
        "config": {
            "path": VIEWER_PROXY_CONFIG_PATH,
            "sha256": VIEWER_PROXY_CONFIG_SHA256,
            "bytes": VIEWER_PROXY_CONFIG_BYTES,
        },
        "static_validation": {
            "path": str(static.path),
            "sha256": static.sha256,
        },
    }


def _validate_viewer_proxy_candidate(
    value: Any,
    *,
    expected_origin: str,
    now: datetime,
    require_fresh: bool,
) -> Mapping[str, Any]:
    expected_fields = {
        "schema_version",
        "observed_at",
        "public_origin",
        "expected_viewer_address",
        "route_prefix",
        "upstream_origin",
        "config",
        "static_validation",
        "dns_preread",
        "tls_preread",
        "nginx_prestate",
        "production_mutation",
    }
    if not isinstance(value, Mapping) or set(value) != expected_fields:
        raise ProdE2EReleaseError(
            "prod_e2e_release_viewer_proxy_candidate_shape_invalid"
        )
    observed_at = _timestamp(
        value.get("observed_at"), field="viewer_proxy_candidate_observed_at"
    )
    if require_fresh and (
        observed_at > now + MAX_FUTURE_SKEW
        or now - observed_at > MAX_FINAL_OBSERVATION_AGE
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_viewer_proxy_candidate_stale"
        )
    public_origin = _canonical_https_dns_origin(
        value.get("public_origin"), field="viewer_proxy_public_origin"
    )
    hostname = urlsplit(public_origin).hostname or ""
    static = _validate_viewer_proxy_static_evidence()

    dns = value.get("dns_preread")
    tls = value.get("tls_preread")
    prestate = value.get("nginx_prestate")
    if not all(isinstance(item, Mapping) for item in (dns, tls, prestate)):
        raise ProdE2EReleaseError(
            "prod_e2e_release_viewer_proxy_candidate_invalid"
        )
    dns_fields = {
        "observed_at",
        "resolver",
        "hostname",
        "canonical_name",
        "addresses",
        "selected_address",
        "lookup_succeeded",
    }
    dns_at = _timestamp(dns.get("observed_at"), field="viewer_proxy_dns_observed_at")
    if (
        set(dns) != dns_fields
        or dns.get("resolver") != "system"
        or dns.get("hostname") != hostname
        or dns.get("canonical_name") != hostname
        or dns.get("addresses") != [VIEWER_EXPECTED_ADDRESS]
        or dns.get("selected_address") != VIEWER_EXPECTED_ADDRESS
        or dns.get("lookup_succeeded") is not True
        or dns_at > observed_at
        or observed_at - dns_at > MAX_FINAL_OBSERVATION_AGE
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_viewer_proxy_dns_invalid"
        )

    tls_fields = {
        "observed_at",
        "hostname",
        "server_address",
        "server_port",
        "hostname_verified",
        "verification_errors",
        "certificate_subject",
        "certificate_issuer",
        "san_dns_names",
        "matched_san_dns_name",
        "certificate_der_sha256",
        "spki_der_sha256",
        "not_before",
        "not_after",
    }
    tls_at = _timestamp(tls.get("observed_at"), field="viewer_proxy_tls_observed_at")
    not_before = _timestamp(tls.get("not_before"), field="viewer_proxy_tls_not_before")
    not_after = _timestamp(tls.get("not_after"), field="viewer_proxy_tls_not_after")
    san_names = tls.get("san_dns_names")
    matched_san = tls.get("matched_san_dns_name")
    if (
        set(tls) != tls_fields
        or tls.get("hostname") != hostname
        or tls.get("server_address") != VIEWER_EXPECTED_ADDRESS
        or tls.get("server_port") != 443
        or tls.get("hostname_verified") is not True
        or tls.get("verification_errors") != []
        or not _required_text(
            tls.get("certificate_subject"), field="viewer_tls_subject"
        )
        or not _required_text(
            tls.get("certificate_issuer"), field="viewer_tls_issuer"
        )
        or not isinstance(san_names, list)
        or not san_names
        or len(san_names) != len(set(san_names))
        or any(
            not isinstance(name, str)
            or not name.isascii()
            or name != name.lower()
            or "xn--" in name
            for name in san_names
        )
        or matched_san not in san_names
        or not _certificate_dns_name_matches(hostname, str(matched_san or ""))
        or _sha256(
            tls.get("certificate_der_sha256"),
            field="viewer_tls_certificate_der_sha256",
        )
        != tls.get("certificate_der_sha256")
        or _sha256(
            tls.get("spki_der_sha256"), field="viewer_tls_spki_der_sha256"
        )
        != tls.get("spki_der_sha256")
        or not_before > tls_at
        or tls_at >= not_after
        or tls_at > observed_at
        or observed_at - tls_at > MAX_FINAL_OBSERVATION_AGE
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_viewer_proxy_tls_invalid"
        )

    prestate_fields = {
        "observed_at",
        "installed_include_path",
        "include_present",
        "include_sha256",
        "effective_config_sha256",
        "nginx_config_test_passed",
        "binary_path",
        "binary_sha256",
        "version",
        "service_identity",
        "main_pid",
        "process_executable",
        "process_argv_sha256",
        "process_cwd",
        "root_status",
        "artifact_status",
        "artifact_content_type",
        "artifact_body_sha256",
        "route_is_spa_fallback",
        "rollback_capture_sha256",
    }
    prestate_at = _timestamp(
        prestate.get("observed_at"), field="viewer_proxy_prestate_observed_at"
    )
    installed_path = Path(str(prestate.get("installed_include_path") or ""))
    binary_path = Path(str(prestate.get("binary_path") or ""))
    process_cwd = Path(str(prestate.get("process_cwd") or ""))
    prestate_without_digest = {
        key: item for key, item in prestate.items() if key != "rollback_capture_sha256"
    }
    if (
        set(prestate) != prestate_fields
        or not installed_path.is_absolute()
        or installed_path.suffix != ".conf"
        or "nginx" not in str(installed_path).lower()
        or prestate.get("include_present") is not False
        or prestate.get("include_sha256") != EMPTY_SHA256
        or _sha256(
            prestate.get("effective_config_sha256"),
            field="viewer_prestate_effective_config_sha256",
        )
        != prestate.get("effective_config_sha256")
        or prestate.get("nginx_config_test_passed") is not True
        or not binary_path.is_absolute()
        or _sha256(
            prestate.get("binary_sha256"), field="viewer_prestate_binary_sha256"
        )
        != prestate.get("binary_sha256")
        or not _required_text(
            prestate.get("version"), field="viewer_prestate_version"
        )
        or not _required_text(
            prestate.get("service_identity"), field="viewer_service_identity"
        )
        or not isinstance(prestate.get("main_pid"), int)
        or isinstance(prestate.get("main_pid"), bool)
        or prestate.get("main_pid", 0) <= 0
        or prestate.get("process_executable") != str(binary_path)
        or _sha256(
            prestate.get("process_argv_sha256"),
            field="viewer_prestate_process_argv_sha256",
        )
        != prestate.get("process_argv_sha256")
        or not process_cwd.is_absolute()
        or prestate.get("root_status") != 200
        or prestate.get("artifact_status") != 200
        or not str(prestate.get("artifact_content_type") or "").lower().startswith(
            "text/html"
        )
        or _sha256(
            prestate.get("artifact_body_sha256"),
            field="viewer_prestate_artifact_body_sha256",
        )
        != prestate.get("artifact_body_sha256")
        or prestate.get("route_is_spa_fallback") is not True
        or prestate.get("rollback_capture_sha256")
        != _sha256_value(prestate_without_digest)
        or prestate_at > observed_at
        or observed_at - prestate_at > MAX_FINAL_OBSERVATION_AGE
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_viewer_proxy_prestate_invalid"
        )

    if (
        value.get("schema_version") != VIEWER_PROXY_CANDIDATE_SCHEMA_VERSION
        or public_origin != expected_origin
        or value.get("expected_viewer_address") != VIEWER_EXPECTED_ADDRESS
        or value.get("route_prefix") != VIEWER_PROXY_ROUTE_PREFIX
        or value.get("upstream_origin") != VIEWER_PROXY_UPSTREAM_ORIGIN
        or value.get("config") != static["config"]
        or value.get("static_validation") != static["static_validation"]
        or value.get("production_mutation") is not False
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_viewer_proxy_candidate_invalid"
        )
    return {
        **dict(value),
        "observed_at": observed_at.isoformat(),
        "dns_preread": {**dict(dns), "observed_at": dns_at.isoformat()},
        "tls_preread": {
            **dict(tls),
            "observed_at": tls_at.isoformat(),
            "not_before": not_before.isoformat(),
            "not_after": not_after.isoformat(),
        },
        "nginx_prestate": {
            **dict(prestate),
            "observed_at": prestate_at.isoformat(),
        },
    }


def _validate_viewer_proxy_live_observation(
    reference: Any,
    *,
    candidate: Mapping[str, Any],
    report_service: Mapping[str, Any],
    report_restart: Mapping[str, Any],
    earliest_observed_at: datetime,
    completion_observed_at: datetime,
) -> Mapping[str, Any]:
    if not isinstance(reference, Mapping) or set(reference) != {
        "observation_path",
        "observation_sha256",
    }:
        raise ProdE2EReleaseError(
            "prod_e2e_release_viewer_proxy_live_reference_invalid"
        )
    owned = _read_owned_json(
        Path(str(reference.get("observation_path") or "")),
        artifact="viewer_proxy_live_observation",
    )
    if owned.sha256 != reference.get("observation_sha256"):
        raise ProdE2EReleaseError(
            "prod_e2e_release_viewer_proxy_live_reference_invalid"
        )
    body = owned.body
    expected_fields = {
        "schema_version",
        "observed_at",
        "public_origin",
        "dns",
        "tls",
        "nginx_live",
        "upstream",
        "http_contract",
        "browser",
        "production_mutation",
    }
    if set(body) != expected_fields:
        raise ProdE2EReleaseError(
            "prod_e2e_release_viewer_proxy_live_shape_invalid"
        )
    observed_at = _timestamp(
        body.get("observed_at"), field="viewer_proxy_live_observed_at"
    )
    origin = _canonical_https_dns_origin(
        body.get("public_origin"), field="viewer_proxy_live_origin"
    )
    if (
        body.get("schema_version") != VIEWER_PROXY_LIVE_SCHEMA_VERSION
        or observed_at < earliest_observed_at
        or observed_at > completion_observed_at
        or origin != candidate.get("public_origin")
        or body.get("dns") != candidate.get("dns_preread")
        or body.get("tls") != candidate.get("tls_preread")
        or body.get("production_mutation") is not True
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_viewer_proxy_live_binding_invalid"
        )

    nginx = body.get("nginx_live")
    prestate = candidate.get("nginx_prestate")
    expected_nginx_fields = {
        "installed_include_path",
        "include_sha256",
        "include_bytes",
        "include_owner_uid",
        "include_mode",
        "binary_path",
        "binary_sha256",
        "version",
        "service_identity",
        "main_pid",
        "process_executable",
        "process_argv_sha256",
        "process_cwd",
        "nginx_config_test_passed",
        "effective_config_sha256",
        "effective_location_count",
        "reload_performed",
        "reloaded_at",
    }
    if not isinstance(nginx, Mapping) or not isinstance(prestate, Mapping):
        raise ProdE2EReleaseError(
            "prod_e2e_release_viewer_proxy_live_nginx_invalid"
        )
    reloaded_at = _timestamp(
        nginx.get("reloaded_at"), field="viewer_proxy_reloaded_at"
    )
    if (
        set(nginx) != expected_nginx_fields
        or nginx.get("installed_include_path")
        != prestate.get("installed_include_path")
        or nginx.get("include_sha256") != VIEWER_PROXY_CONFIG_SHA256
        or nginx.get("include_bytes") != VIEWER_PROXY_CONFIG_BYTES
        or not isinstance(nginx.get("include_owner_uid"), int)
        or isinstance(nginx.get("include_owner_uid"), bool)
        or nginx.get("include_owner_uid", -1) < 0
        or nginx.get("include_mode") not in {"0644", "0444"}
        or nginx.get("binary_path") != prestate.get("binary_path")
        or nginx.get("binary_sha256") != prestate.get("binary_sha256")
        or nginx.get("version") != prestate.get("version")
        or nginx.get("service_identity") != prestate.get("service_identity")
        or not isinstance(nginx.get("main_pid"), int)
        or isinstance(nginx.get("main_pid"), bool)
        or nginx.get("main_pid", 0) <= 0
        or nginx.get("process_executable") != nginx.get("binary_path")
        or _sha256(
            nginx.get("process_argv_sha256"),
            field="viewer_proxy_live_process_argv_sha256",
        )
        != nginx.get("process_argv_sha256")
        or nginx.get("process_cwd") != prestate.get("process_cwd")
        or nginx.get("nginx_config_test_passed") is not True
        or _sha256(
            nginx.get("effective_config_sha256"),
            field="viewer_proxy_live_effective_config_sha256",
        )
        != nginx.get("effective_config_sha256")
        or nginx.get("effective_config_sha256")
        == prestate.get("effective_config_sha256")
        or nginx.get("effective_location_count") != 1
        or nginx.get("reload_performed") is not True
        or reloaded_at < earliest_observed_at
        or reloaded_at > observed_at
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_viewer_proxy_live_nginx_invalid"
        )

    upstream = body.get("upstream")
    expected_upstream_fields = {
        "origin",
        "viewer_origin",
        "report_service_unit",
        "report_entrypoint_sha256",
        "report_unit_config_sha256",
        "report_main_pid",
        "legacy_html_health_passed",
        "exact_artifact_contract_passed",
    }
    if (
        not isinstance(upstream, Mapping)
        or set(upstream) != expected_upstream_fields
        or upstream.get("origin") != VIEWER_PROXY_UPSTREAM_ORIGIN
        or upstream.get("viewer_origin") != origin
        or upstream.get("report_service_unit") != VM_REPORT_UNIT
        or upstream.get("report_entrypoint_sha256")
        != report_service.get("entrypoint_sha256")
        or upstream.get("report_unit_config_sha256")
        != report_service.get("candidate_unit_sha256")
        or upstream.get("report_main_pid") != report_restart.get("new_pid")
        or upstream.get("legacy_html_health_passed") is not True
        or upstream.get("exact_artifact_contract_passed") is not True
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_viewer_proxy_live_upstream_invalid"
        )

    artifact_url = (
        f"{origin}{VIEWER_PROXY_ROUTE_PREFIX}{VIEWER_DIAGNOSTIC_SUBMISSION_KEY}/"
        f"{VIEWER_DIAGNOSTIC_SUBMISSION_KEY}.viz.mcap"
    )
    http_contract = body.get("http_contract")
    expected_http_fields = {
        "artifact_url",
        "submission_key",
        "artifact_sha256",
        "artifact_bytes",
        "head",
        "get",
        "range",
        "unsatisfiable_range",
        "options",
        "rejected_paths",
        "rejected_methods",
        "server_header_absent",
    }
    if not isinstance(http_contract, Mapping) or set(http_contract) != expected_http_fields:
        raise ProdE2EReleaseError(
            "prod_e2e_release_viewer_proxy_http_contract_invalid"
        )
    head = http_contract.get("head")
    full_get = http_contract.get("get")
    byte_range = http_contract.get("range")
    unsatisfiable = http_contract.get("unsatisfiable_range")
    options = http_contract.get("options")
    common_response_fields = {
        "method",
        "status",
        "body_bytes",
        "body_sha256",
        "content_length",
        "content_type",
        "accept_ranges",
        "content_range",
        "cors_allow_origin",
    }
    if not all(
        isinstance(item, Mapping)
        for item in (head, full_get, byte_range, unsatisfiable, options)
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_viewer_proxy_http_contract_invalid"
        )
    expected_responses = (
        (
            head,
            {
                "method": "HEAD",
                "status": 200,
                "body_bytes": 0,
                "body_sha256": EMPTY_SHA256,
                "content_length": VIEWER_DIAGNOSTIC_BYTES,
                "content_type": "application/octet-stream",
                "accept_ranges": "bytes",
                "content_range": "",
                "cors_allow_origin": origin,
            },
        ),
        (
            full_get,
            {
                "method": "GET",
                "status": 200,
                "body_bytes": VIEWER_DIAGNOSTIC_BYTES,
                "body_sha256": VIEWER_DIAGNOSTIC_SHA256,
                "content_length": VIEWER_DIAGNOSTIC_BYTES,
                "content_type": "application/octet-stream",
                "accept_ranges": "bytes",
                "content_range": "",
                "cors_allow_origin": origin,
            },
        ),
        (
            byte_range,
            {
                "method": "GET bytes=0-2234",
                "status": 206,
                "body_bytes": VIEWER_DIAGNOSTIC_BYTES,
                "body_sha256": VIEWER_DIAGNOSTIC_SHA256,
                "content_length": VIEWER_DIAGNOSTIC_BYTES,
                "content_type": "application/octet-stream",
                "accept_ranges": "bytes",
                "content_range": "bytes 0-2234/2235",
                "cors_allow_origin": origin,
            },
        ),
        (
            unsatisfiable,
            {
                "method": "GET bytes=2235-",
                "status": 416,
                "body_bytes": 0,
                "body_sha256": EMPTY_SHA256,
                "content_length": 0,
                "content_type": "application/octet-stream",
                "accept_ranges": "bytes",
                "content_range": "bytes */2235",
                "cors_allow_origin": origin,
            },
        ),
        (
            options,
            {
                "method": "OPTIONS",
                "status": 204,
                "body_bytes": 0,
                "body_sha256": EMPTY_SHA256,
                "content_length": 0,
                "content_type": "",
                "accept_ranges": "bytes",
                "content_range": "",
                "cors_allow_origin": origin,
            },
        ),
    )
    if (
        http_contract.get("artifact_url") != artifact_url
        or http_contract.get("submission_key")
        != VIEWER_DIAGNOSTIC_SUBMISSION_KEY
        or http_contract.get("artifact_sha256") != VIEWER_DIAGNOSTIC_SHA256
        or http_contract.get("artifact_bytes") != VIEWER_DIAGNOSTIC_BYTES
        or any(set(actual) != common_response_fields or dict(actual) != expected for actual, expected in expected_responses)
        or http_contract.get("rejected_paths")
        != {
            "wrong_version": 404,
            "mismatched_filename": 404,
            "directory": 404,
            "traversal": 404,
            "encoded_separator": 404,
            "query_string": 404,
        }
        or http_contract.get("rejected_methods")
        != {
            "CONNECT": 403,
            "DELETE": 403,
            "PATCH": 403,
            "POST": 403,
            "PUT": 403,
            "TRACE": 403,
        }
        or http_contract.get("server_header_absent") is not True
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_viewer_proxy_http_contract_invalid"
        )

    browser = body.get("browser")
    viewer_url = f"{origin}/?ds=remote-file&ds.url={quote(artifact_url, safe='')}"
    expected_browser_fields = {
        "engine",
        "browser_version",
        "executable_sha256",
        "ignore_https_errors",
        "viewer_url",
        "artifact_url",
        "navigation_status",
        "network_artifact_statuses",
        "artifact_sha256",
        "artifact_bytes",
        "expected_topic",
        "expected_topic_visible",
        "remote_file_source_selected",
        "viewer_title_binds_artifact",
        "page_errors",
        "player_errors",
        "mixed_content_errors",
        "certificate_errors",
        "unexpected_request_failures",
        "screenshot_sha256",
    }
    if (
        not isinstance(browser, Mapping)
        or set(browser) != expected_browser_fields
        or browser.get("engine") != "playwright.chromium"
        or not _required_text(
            browser.get("browser_version"), field="viewer_browser_version"
        )
        or _sha256(
            browser.get("executable_sha256"),
            field="viewer_browser_executable_sha256",
        )
        != browser.get("executable_sha256")
        or browser.get("ignore_https_errors") is not False
        or browser.get("viewer_url") != viewer_url
        or browser.get("artifact_url") != artifact_url
        or browser.get("navigation_status") != 200
        or browser.get("network_artifact_statuses") != [200, 206]
        or browser.get("artifact_sha256") != VIEWER_DIAGNOSTIC_SHA256
        or browser.get("artifact_bytes") != VIEWER_DIAGNOSTIC_BYTES
        or browser.get("expected_topic") != VIEWER_DIAGNOSTIC_TOPIC
        or browser.get("expected_topic_visible") is not True
        or browser.get("remote_file_source_selected") is not True
        or browser.get("viewer_title_binds_artifact") is not True
        or any(
            browser.get(field) != []
            for field in (
                "page_errors",
                "player_errors",
                "mixed_content_errors",
                "certificate_errors",
                "unexpected_request_failures",
            )
        )
        or _sha256(
            browser.get("screenshot_sha256"),
            field="viewer_browser_screenshot_sha256",
        )
        != browser.get("screenshot_sha256")
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_viewer_proxy_browser_invalid"
        )
    return {
        "path": str(owned.path),
        "sha256": owned.sha256,
        "observed_at": observed_at.isoformat(),
        "public_origin": origin,
        "artifact_url": artifact_url,
        "viewer_url": viewer_url,
        "reloaded_at": reloaded_at.isoformat(),
        "nginx_effective_config_sha256": nginx["effective_config_sha256"],
        "certificate_der_sha256": body["tls"]["certificate_der_sha256"],
        "spki_der_sha256": body["tls"]["spki_der_sha256"],
        "browser_screenshot_sha256": browser["screenshot_sha256"],
    }


def _validate_component_binding(
    owned: OwnedJson,
    *,
    release_id: str,
    now: datetime,
    require_fresh: bool,
    verify_vm_live: bool = False,
    environment_phase: str = "pre",
) -> Mapping[str, Any]:
    body = owned.body
    expected_fields = {
        "schema_version",
        "release_id",
        "observed_at",
        "host",
        "workspace",
        "worker",
        "pipeline",
        "viewer_proxy",
        "admission_security",
        "production_mutation",
    }
    if set(body) != expected_fields or body.get("schema_version") != COMPONENT_BINDING_SCHEMA_VERSION:
        raise ProdE2EReleaseError("prod_e2e_release_component_binding_shape_invalid")
    if body.get("release_id") != release_id or body.get("production_mutation") is not False:
        raise ProdE2EReleaseError("prod_e2e_release_component_binding_invalid")
    observed_at = _timestamp(
        body.get("observed_at"), field="component_binding_observed_at"
    )
    if require_fresh and (
        observed_at > now + MAX_FUTURE_SKEW
        or now - observed_at > MAX_FINAL_OBSERVATION_AGE
    ):
        raise ProdE2EReleaseError("prod_e2e_release_component_binding_stale")
    host = body.get("host")
    workspace = body.get("workspace")
    worker = body.get("worker")
    pipeline = body.get("pipeline")
    viewer_proxy = body.get("viewer_proxy")
    security = body.get("admission_security")
    if not all(
        isinstance(item, Mapping)
        for item in (host, workspace, worker, pipeline, viewer_proxy, security)
    ):
        raise ProdE2EReleaseError("prod_e2e_release_component_binding_invalid")

    if (
        _git_oid(host.get("commit"), field="host_commit") != HOST_FINAL_COMMIT
        or _git_oid(host.get("tree"), field="host_tree") != HOST_FINAL_TREE
    ):
        raise ProdE2EReleaseError("prod_e2e_release_host_binding_invalid")
    desired_origin = _canonical_https_dns_origin(
        viewer_proxy.get("public_origin"), field="component_desired_viewer_origin"
    )
    if environment_phase not in {"pre", "post"}:
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_environment_phase_invalid"
        )
    observed_host = _observe_canonical_host_binding(
        expected_commit=HOST_FINAL_COMMIT,
        expected_tree=HOST_FINAL_TREE,
    )
    component_probe = _run_canonical_component_probe(
        desired_viewer_origin=desired_origin
    )
    dependency_environment = _validate_host_dependency_environment(
        host.get("dependency_environment")
    )
    if (
        component_probe["runtime_files"]
        != observed_host["required_file_sha256"]
        or sorted(component_probe["rca_runtime_files"])
        != observed_host["runtime_allowlists"]["rca"]
        or sorted(component_probe["gateway_runtime_files"])
        != observed_host["runtime_allowlists"]["gateway"]
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_runtime_allowlist_mismatch"
        )
    vm_live = (
        _observe_vm_components_live(
            expected_viewer_origin=desired_origin,
            environment_phase=environment_phase,
        )
        if verify_vm_live
        else None
    )
    environment_transition = _validate_host_environment_transition(
        host.get("host_environment_transition"),
        desired_origin=desired_origin,
    )
    observed_transition = _host_environment_transition_from_probe(
        component_probe, desired_origin=desired_origin
    )
    if (
        environment_phase == "pre"
        and environment_transition != observed_transition
    ) or (
        environment_phase == "post"
        and (
            component_probe["host_env_current_sha256"]
            != environment_transition["post_sha256"]
            or component_probe["host_env_current_bytes"]
            != environment_transition["post_bytes"]
            or component_probe["host_env_current_viewer_origin_count"] != 1
            or component_probe["host_env_current_viewer_origin"]
            != desired_origin
            or component_probe["host_env_planned_sha256"]
            != environment_transition["post_sha256"]
        )
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_environment_transition_mismatch"
        )
    expected_host = {
        "root": CANONICAL_HOST_ROOT,
        "commit": observed_host["commit"],
        "tree": observed_host["tree"],
        "tree_clean": True,
        "status_sha256": EMPTY_SHA256,
        "quarantine_baseline_ancestor": HOST_QUARANTINE_BASELINE_COMMIT,
        "quarantine_baseline_ancestor_verified": True,
        "delivery_store_schema_version": DELIVERY_STORE_TARGET_SCHEMA,
        "viewer_origin": desired_origin,
        "host_environment_transition": dict(environment_transition),
        "candidate_identity_evidence": observed_host[
            "candidate_identity_evidence"
        ],
        "required_file_sha256": observed_host["required_file_sha256"],
        "runtime_allowlists": observed_host["runtime_allowlists"],
        "service_config_sha256": observed_host["service_config_sha256"],
        "runtime_file_sha256": component_probe["runtime_files"],
        "runtime_files_sha256": component_probe["runtime_files_sha256"],
        "rca_runtime_file_sha256": component_probe["rca_runtime_files"],
        "rca_runtime_files_sha256": component_probe[
            "rca_runtime_files_sha256"
        ],
        "gateway_runtime_file_sha256": component_probe[
            "gateway_runtime_files"
        ],
        "gateway_runtime_files_sha256": component_probe[
            "gateway_runtime_files_sha256"
        ],
        "service_runtime_files_sha256": component_probe[
            "service_runtime_files_sha256"
        ],
        "canonical_interpreter_sha256": component_probe["interpreter_sha256"],
        "dependency_environment": dependency_environment,
        "retired_executor_paths_absent": list(RETIRED_EXECUTOR_PATHS),
    }
    if dict(host) != expected_host:
        raise ProdE2EReleaseError("prod_e2e_release_host_binding_invalid")

    viewer_proxy_identity = _validate_viewer_proxy_candidate(
        viewer_proxy,
        expected_origin=desired_origin,
        now=now,
        require_fresh=require_fresh,
    )

    workspace_identity = _validate_staged_workspace_binding(workspace)

    worker_identity = _validate_vm_worker_observation(
        worker, now=now, require_fresh=require_fresh
    )
    if vm_live is not None:
        live_worker = vm_live.get("worker")
        if (
            not isinstance(live_worker, Mapping)
            or any(
                worker_identity.get(field) != live_worker.get(field)
                for field in (
                    "root",
                    "commit",
                    "tree",
                    "tree_clean",
                    "status_sha256",
                    "entrypoint",
                    "entrypoint_sha256",
                    "entrypoint_git_mode",
                    "runtime_artifact_sha256",
                    "loaded_runtime_sha256",
                    "interpreter_path",
                    "interpreter_sha256",
                    "daemon_unit_config_sha256",
                    "report_unit_config_sha256",
                    "report_environment",
                )
            )
            or (
                environment_phase == "pre"
                and worker_identity.get("report_environment_transition")
                != live_worker.get("report_environment_transition")
            )
            or worker_identity["observer"]["machine_identity_sha256"]
            != vm_live.get("machine_identity_sha256")
            or (
                environment_phase == "pre"
                and worker_identity["observer"]["command_sha256"]
                != vm_live.get("observer_script_sha256")
            )
            or (
                environment_phase == "post"
                and vm_live.get("observer_script_sha256")
                != _vm_component_probe_script_sha256(
                    desired_origin, environment_phase="post"
                )
            )
        ):
            raise ProdE2EReleaseError(
                "prod_e2e_release_worker_live_observation_mismatch"
            )
    if (
        worker_identity.get("report_environment", {}).get("viewer_origin")
        != desired_origin
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_vm_report_viewer_origin_mismatch"
        )

    if (
        set(pipeline)
        != {
            "root",
            "commit",
            "tree",
            "tree_clean",
            "entrypoint",
            "entrypoint_sha256",
            "entrypoint_git_mode",
            "candidate_audit",
            "report_service",
        }
        or pipeline.get("root") != PIPELINE_SOURCE_ROOT
        or pipeline.get("commit") != PIPELINE_COMMIT
        or pipeline.get("tree") != PIPELINE_TREE
        or pipeline.get("tree_clean") is not True
        or pipeline.get("entrypoint")
        != f"{PIPELINE_SOURCE_ROOT}/{PIPELINE_ENTRYPOINT}"
        or pipeline.get("entrypoint_sha256") != PIPELINE_ENTRYPOINT_SHA256
        or pipeline.get("entrypoint_git_mode") != "100755"
        or pipeline.get("candidate_audit")
        != {
            "vm_path": PIPELINE_CANDIDATE_AUDIT_VM_PATH,
            "cifs_path": PIPELINE_CANDIDATE_AUDIT_CIFS_PATH,
            "sha256": PIPELINE_CANDIDATE_AUDIT_SHA256,
        }
    ):
        raise ProdE2EReleaseError("prod_e2e_release_pipeline_binding_invalid")
    report_service = pipeline.get("report_service")
    expected_report_fields = {
        "unit",
        "entrypoint_relative",
        "entrypoint_path",
        "entrypoint_sha256",
        "entrypoint_git_mode",
        "candidate_unit_relative",
        "candidate_unit_path",
        "candidate_unit_sha256",
        "candidate_unit_git_mode",
        "live_unit_path",
        "exec_start",
        "effective_exec_start",
        "environment_file_path",
        "environment_file_sha256",
        "environment_file_bytes",
        "environment_file_owner_uid",
        "environment_file_mode",
        "environment_variable",
        "viewer_origin",
        "working_directory",
        "root",
        "route_prefix",
        "port",
        "directory_listing",
        "path_traversal",
        "symlink_escape",
        "read_only",
        "old_broad_http_server_forbidden",
        "delivery_manifest_schema",
        "viz_manifest_schema",
        "max_concurrent_requests",
        "request_queue_size",
    }
    if (
        not isinstance(report_service, Mapping)
        or set(report_service) != expected_report_fields
        or report_service.get("unit") != VM_REPORT_UNIT
        or report_service.get("entrypoint_relative")
        != VM_REPORT_ENTRYPOINT_RELATIVE
        or report_service.get("entrypoint_path")
        != f"{PIPELINE_SOURCE_ROOT}/{VM_REPORT_ENTRYPOINT_RELATIVE}"
        or report_service.get("entrypoint_sha256")
        != VM_REPORT_ENTRYPOINT_SHA256
        or report_service.get("entrypoint_git_mode") != "100755"
        or report_service.get("candidate_unit_relative") != VM_REPORT_UNIT_RELATIVE
        or report_service.get("candidate_unit_path")
        != f"{PIPELINE_SOURCE_ROOT}/{VM_REPORT_UNIT_RELATIVE}"
        or report_service.get("candidate_unit_sha256") != VM_REPORT_UNIT_SHA256
        or report_service.get("candidate_unit_git_mode") != "100644"
        or report_service.get("live_unit_path") != VM_REPORT_LIVE_UNIT_PATH
        or report_service.get("exec_start")
        != [
            "/usr/bin/python3",
            "-I",
            "-B",
            f"{PIPELINE_SOURCE_ROOT}/{VM_REPORT_ENTRYPOINT_RELATIVE}",
            "--root",
            VM_REPORT_ROOT,
            "--bind",
            "0.0.0.0",
            "--port",
            str(VM_REPORT_PORT),
            "--viewer-origin",
            f"${{{VM_REPORT_ENV_VARIABLE}}}",
        ]
        or report_service.get("effective_exec_start")
        != [
            VM_INTERPRETER_PATH,
            "-I",
            "-B",
            f"{PIPELINE_SOURCE_ROOT}/{VM_REPORT_ENTRYPOINT_RELATIVE}",
            "--root",
            VM_REPORT_ROOT,
            "--bind",
            "0.0.0.0",
            "--port",
            str(VM_REPORT_PORT),
            "--viewer-origin",
            desired_origin,
        ]
        or report_service.get("environment_file_path") != VM_REPORT_ENV_PATH
        or report_service.get("environment_file_sha256")
        != worker_identity["report_environment"]["sha256"]
        or report_service.get("environment_file_bytes")
        != worker_identity["report_environment"]["bytes"]
        or report_service.get("environment_file_owner_uid") != 1000
        or report_service.get("environment_file_mode") != "0600"
        or report_service.get("environment_variable") != VM_REPORT_ENV_VARIABLE
        or report_service.get("viewer_origin") != desired_origin
        or report_service.get("working_directory") != "/"
        or report_service.get("root") != VM_REPORT_ROOT
        or report_service.get("route_prefix") != VM_REPORT_ROUTE_PREFIX
        or report_service.get("port") != VM_REPORT_PORT
        or report_service.get("directory_listing") is not False
        or report_service.get("path_traversal") is not False
        or report_service.get("symlink_escape") is not False
        or report_service.get("read_only") is not True
        or report_service.get("old_broad_http_server_forbidden") is not True
        or report_service.get("delivery_manifest_schema")
        != "delivery_manifest_v2"
        or report_service.get("viz_manifest_schema")
        != "g1q3_rca_viz_publication_v1"
        or report_service.get("max_concurrent_requests") != 4
        or report_service.get("request_queue_size") != 16
        or worker_identity.get("report_unit_config_sha256")
        != report_service.get("candidate_unit_sha256")
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_report_service_binding_invalid"
        )
    if vm_live is not None:
        live_report = vm_live.get("services", {}).get(VM_REPORT_UNIT)
        if (
            not isinstance(live_report, Mapping)
            or live_report.get("entrypoint") != report_service["entrypoint_path"]
            or live_report.get("entrypoint_sha256")
            != report_service["entrypoint_sha256"]
            or live_report.get("unit_config_sha256")
            != report_service["candidate_unit_sha256"]
            or (
                environment_phase == "post"
                and live_report.get("effective_exec_start")
                != report_service["effective_exec_start"]
            )
            or live_report.get("environment_files") != [VM_REPORT_ENV_PATH]
            or live_report.get("viewer_origin")
            != (report_service["viewer_origin"] if environment_phase == "post" else "")
            or (
                environment_phase == "pre"
                and vm_live.get("report_environment_transition")
                != worker_identity.get("report_environment_transition")
            )
            or (
                environment_phase == "post"
                and vm_live.get("report_environment")
                != {
                    "path": VM_REPORT_ENV_PATH,
                    "exists": True,
                    "sha256": worker_identity["report_environment"]["sha256"],
                    "bytes": worker_identity["report_environment"]["bytes"],
                    "owner_uid": 1000,
                    "mode": "0600",
                    "variable": VM_REPORT_ENV_VARIABLE,
                    "viewer_origin": desired_origin,
                }
            )
            or vm_live.get("report_policy", {}).get("broad_http_server_processes")
            != []
        ):
            raise ProdE2EReleaseError(
                "prod_e2e_release_report_service_live_mismatch"
            )

    vm_hmac_ref = security.get("vm_observation")
    if not isinstance(vm_hmac_ref, Mapping):
        raise ProdE2EReleaseError("prod_e2e_release_admission_security_invalid")
    vm_hmac = _validate_vm_hmac_observation(
        vm_hmac_ref,
        worker=worker_identity,
        now=now,
        require_fresh=require_fresh,
    )
    if vm_live is not None:
        live_hmac = vm_live.get("hmac")
        if (
            not isinstance(live_hmac, Mapping)
            or live_hmac.get("configured") is not True
            or any(
                vm_hmac.get(field) != live_hmac.get(field)
                for field in (
                    "method",
                    "environment_variable",
                    "key_fingerprint",
                    "config_path",
                    "config_sha256",
                    "loaded_runtime_sha256",
                )
            )
            or vm_hmac["observer"]["machine_identity_sha256"]
            != vm_live.get("machine_identity_sha256")
            or (
                environment_phase == "pre"
                and vm_hmac["observer"]["command_sha256"]
                != vm_live.get("observer_script_sha256")
            )
        ):
            raise ProdE2EReleaseError(
                "prod_e2e_release_hmac_live_observation_mismatch"
            )
    expected_security = {
        "mode": "hmac_sha256",
        "method": "prod_admission_hmac_key_fingerprint_v1",
        "environment_variable": ADMISSION_HMAC_ENV,
        "host_key_fingerprint": component_probe["hmac_key_fingerprint"],
        "host_config_path": CANONICAL_HOST_ENV,
        "host_config_pre_sha256": environment_transition["pre_sha256"],
        "host_config_post_sha256": environment_transition["post_sha256"],
        "host_config_write_required": environment_transition["write_required"],
        "host_probe_script_sha256": component_probe["probe_script_sha256"],
        "host_interpreter_sha256": component_probe["interpreter_sha256"],
        "vm_key_fingerprint": vm_hmac["key_fingerprint"],
        "vm_observation": dict(vm_hmac_ref),
        "fingerprints_match": True,
        "secure_stream_required": True,
        "secret_material_persisted": False,
    }
    if (
        dict(security) != expected_security
        or component_probe["hmac_key_fingerprint"]
        != vm_hmac["key_fingerprint"]
    ):
        raise ProdE2EReleaseError("prod_e2e_release_admission_security_invalid")

    return {
        "path": str(owned.path),
        "sha256": owned.sha256,
        "observed_at": observed_at.isoformat(),
        "host": dict(host),
        "workspace": dict(workspace_identity),
        "worker": dict(worker_identity),
        "pipeline": dict(pipeline),
        "viewer_proxy": dict(viewer_proxy_identity),
        "admission_security": {
            **expected_security,
            "vm_observation_evidence": {
                "path": vm_hmac["evidence_path"],
                "sha256": vm_hmac["evidence_sha256"],
                "observed_at": vm_hmac["observed_at"],
                "loaded_runtime_sha256": vm_hmac["loaded_runtime_sha256"],
            },
        },
    }


_CANONICAL_DB_VALIDATOR = r'''import datetime,hashlib,json,pathlib,sys
root=pathlib.Path(sys.argv[1])
receipt_path=pathlib.Path(sys.argv[2])
receipt_sha256=sys.argv[3]
clone_path=pathlib.Path(sys.argv[4])
live_path=pathlib.Path(sys.argv[5])
runtime_sha256=sys.argv[6]
core_path=pathlib.Path(sys.argv[7])
release_id=sys.argv[8]
sys.path.insert(0,str(root))
from gateway import pnc_rca_delivery_quarantine_baseline as baseline
from gateway import pnc_rca_delivery_quarantine_migration as migration
expected_migration=root/'gateway/pnc_rca_delivery_quarantine_migration.py'
expected_baseline=root/'gateway/pnc_rca_delivery_quarantine_baseline.py'
if pathlib.Path(migration.__file__).resolve()!=expected_migration or pathlib.Path(baseline.__file__).resolve()!=expected_baseline:
    raise SystemExit('module_origin_mismatch')
binding=migration.validate_migration_receipt(
 receipt_path=receipt_path,
 expected_sha256=receipt_sha256,
 target_live_db_path=live_path,
 migrated_db_path=clone_path,
 migrated_db_is_live=False,
 expected_migration_runtime_sha256=runtime_sha256,
)
core=json.loads(core_path.read_text(encoding='utf-8'))
snapshot_text=str(core.get('snapshot_at') or '')
if snapshot_text.endswith('Z'):
    snapshot_text=snapshot_text[:-1]+'+00:00'
snapshot_at=datetime.datetime.fromisoformat(snapshot_text)
adjudication=core.get('invalid_manual_thread_adjudication') or {}
settlement_paths=[str(item['path']) for item in core.get('settlement_receipts') or []]
recomputed=baseline.build_quarantine_core_from_offline_clone(
 clone_path,
 target_live_db_path=live_path,
 migration_receipt_path=receipt_path,
 expected_migration_receipt_sha256=receipt_sha256,
 migration_runtime_sha256=runtime_sha256,
 release_id=release_id,
 snapshot_at=snapshot_at,
 settlement_receipt_paths=settlement_paths,
 analyzed_by=str(adjudication.get('analyzed_by') or ''),
 reason=str(adjudication.get('reason') or ''),
)
if recomputed!=core:
    raise SystemExit('quarantine_core_recompute_mismatch')
result={
 'schema_version':'pnc_rca_canonical_db_validation_v1',
 'binding':binding,
 'core_sha256':str(core['core_sha256']),
 'quarantine_counts':core['quarantine_snapshot']['counts'],
 'migration_module_path':str(expected_migration),
 'migration_module_sha256':hashlib.sha256(expected_migration.read_bytes()).hexdigest(),
 'baseline_module_path':str(expected_baseline),
 'baseline_module_sha256':hashlib.sha256(expected_baseline.read_bytes()).hexdigest(),
 'source_clone_distinct':True,
 'clone_live_distinct':True,
 'core_exact_recomputed':True,
}
print(json.dumps(result,sort_keys=True,separators=(',',':')))
'''


def _run_canonical_db_validator(
    *,
    receipt_path: Path,
    receipt_sha256: str,
    source_path: Path,
    clone_path: Path,
    migration_runtime_sha256: str,
    core_path: Path,
    release_id: str,
    host_commit: str,
    host_tree: str,
) -> Mapping[str, Any]:
    live = Path(DELIVERY_DB_PATH)
    resolved = [path.expanduser().absolute() for path in (source_path, clone_path, live)]
    if len(set(resolved)) != 3:
        raise ProdE2EReleaseError(
            "prod_e2e_release_db_artifact_paths_not_distinct"
        )
    observed = _observe_canonical_host_binding(
        expected_commit=host_commit, expected_tree=host_tree
    )
    root = Path(CANONICAL_HOST_ROOT)
    migration_sha256 = hashlib.sha256(
        _host_tracked_bytes(root, observed["commit"], CANONICAL_MIGRATION_MODULE)
    ).hexdigest()
    baseline_sha256 = hashlib.sha256(
        _host_tracked_bytes(root, observed["commit"], CANONICAL_BASELINE_MODULE)
    ).hexdigest()
    if (
        migration_sha256 != CANONICAL_MIGRATION_MODULE_SHA256
        or baseline_sha256 != CANONICAL_BASELINE_MODULE_SHA256
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_canonical_db_validator_identity_mismatch"
        )
    python, resolved_python = _canonical_host_interpreter_paths()
    try:
        interpreter_sha256 = hashlib.sha256(
            resolved_python.read_bytes()
        ).hexdigest()
        completed = subprocess.run(
            [
                str(python),
                "-I",
                "-B",
                "-c",
                _CANONICAL_DB_VALIDATOR,
                str(root),
                str(receipt_path),
                receipt_sha256,
                str(clone_path),
                str(live),
                migration_runtime_sha256,
                str(core_path),
                release_id,
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=60,
            env={
                "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
                "HOME": str(Path.home()),
                "LC_ALL": "C",
                "PYTHONDONTWRITEBYTECODE": "1",
            },
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_canonical_db_validation_failed"
        ) from exc
    if completed.returncode != 0 or completed.stderr or not completed.stdout:
        raise ProdE2EReleaseError(
            "prod_e2e_release_canonical_db_validation_failed"
        )
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_canonical_db_validation_invalid"
        ) from exc
    if (
        not isinstance(value, dict)
        or set(value)
        != {
            "schema_version",
            "binding",
            "core_sha256",
            "quarantine_counts",
            "migration_module_path",
            "migration_module_sha256",
            "baseline_module_path",
            "baseline_module_sha256",
            "source_clone_distinct",
            "clone_live_distinct",
            "core_exact_recomputed",
        }
        or value.get("schema_version")
        != "pnc_rca_canonical_db_validation_v1"
        or value.get("migration_module_sha256") != migration_sha256
        or value.get("baseline_module_sha256") != baseline_sha256
        or value.get("quarantine_counts") != QUARANTINE_COUNTS
        or value.get("source_clone_distinct") is not True
        or value.get("clone_live_distinct") is not True
        or value.get("core_exact_recomputed") is not True
        or not isinstance(value.get("binding"), dict)
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_canonical_db_validation_invalid"
        )
    return {
        **value,
        "validator_script_sha256": hashlib.sha256(
            _CANONICAL_DB_VALIDATOR.encode("utf-8")
        ).hexdigest(),
        "host_commit": observed["commit"],
        "host_tree": observed["tree"],
        "interpreter_path": str(resolved_python),
        "interpreter_sha256": interpreter_sha256,
    }


_CANONICAL_DB_PROJECTION = r'''import hashlib,json,pathlib,sys
root=pathlib.Path(sys.argv[1])
db_path=pathlib.Path(sys.argv[2])
sys.path.insert(0,str(root))
from gateway import pnc_rca_delivery_quarantine_migration as migration
module=root/'gateway/pnc_rca_delivery_quarantine_migration.py'
if pathlib.Path(migration.__file__).resolve()!=module:
    raise SystemExit('module_origin_mismatch')
projection=migration.logical_database_projection_path(db_path,require_integrity=True)
print(json.dumps({'schema_version':'pnc_rca_canonical_db_projection_v1','projection':projection,'module_sha256':hashlib.sha256(module.read_bytes()).hexdigest()},sort_keys=True,separators=(',',':')))
'''


def _run_canonical_db_projection(
    *,
    db_path: Path,
    host_commit: str,
    host_tree: str,
    allow_live: bool = False,
) -> Mapping[str, Any]:
    if db_path.expanduser().absolute() == Path(DELIVERY_DB_PATH) and not allow_live:
        raise ProdE2EReleaseError(
            "prod_e2e_release_preflight_backup_is_live_database"
        )
    observed = _observe_canonical_host_binding(
        expected_commit=host_commit, expected_tree=host_tree
    )
    root = Path(CANONICAL_HOST_ROOT)
    module_sha256 = hashlib.sha256(
        _host_tracked_bytes(root, observed["commit"], CANONICAL_MIGRATION_MODULE)
    ).hexdigest()
    if module_sha256 != CANONICAL_MIGRATION_MODULE_SHA256:
        raise ProdE2EReleaseError(
            "prod_e2e_release_canonical_db_validator_identity_mismatch"
        )
    try:
        completed = subprocess.run(
            [
                CANONICAL_HOST_PYTHON,
                "-I",
                "-B",
                "-c",
                _CANONICAL_DB_PROJECTION,
                CANONICAL_HOST_ROOT,
                str(db_path),
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
            env={
                "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
                "HOME": str(Path.home()),
                "LC_ALL": "C",
                "PYTHONDONTWRITEBYTECODE": "1",
            },
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_canonical_db_projection_failed"
        ) from exc
    if completed.returncode != 0 or completed.stderr or not completed.stdout:
        raise ProdE2EReleaseError(
            "prod_e2e_release_canonical_db_projection_failed"
        )
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_canonical_db_projection_invalid"
        ) from exc
    if (
        not isinstance(value, dict)
        or set(value) != {"schema_version", "projection", "module_sha256"}
        or value.get("schema_version") != "pnc_rca_canonical_db_projection_v1"
        or value.get("module_sha256") != module_sha256
        or not isinstance(value.get("projection"), dict)
        or _sha256(
            value["projection"].get("logical_sha256"),
            field="preflight_backup_logical_sha256",
        )
        != value["projection"].get("logical_sha256")
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_canonical_db_projection_invalid"
        )
    return {
        **value,
        "validator_script_sha256": hashlib.sha256(
            _CANONICAL_DB_PROJECTION.encode("utf-8")
        ).hexdigest(),
        "host_commit": observed["commit"],
        "host_tree": observed["tree"],
    }


def _observe_host_writer_stop_live() -> Mapping[str, Any]:
    domain = f"gui/{os.geteuid()}"
    services: dict[str, Any] = {}
    for label in HOST_SERVICE_LABELS:
        try:
            completed = subprocess.run(
                ["/bin/launchctl", "print", f"{domain}/{label}"],
                check=False,
                capture_output=True,
                text=True,
                timeout=5,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise ProdE2EReleaseError(
                "prod_e2e_release_writer_stop_live_observation_failed"
            ) from exc
        output = str(completed.stdout or "")
        state_match = re.search(r"^\s*state\s*=\s*(\S+)", output, re.MULTILINE)
        pid_match = re.search(r"^\s*pid\s*=\s*(\d+)", output, re.MULTILINE)
        plist = Path.home() / "Library/LaunchAgents" / f"{label}.plist"
        try:
            plist_sha256 = hashlib.sha256(plist.read_bytes()).hexdigest()
        except OSError as exc:
            raise ProdE2EReleaseError(
                "prod_e2e_release_writer_service_config_unavailable"
            ) from exc
        services[label] = {
            "job_present": completed.returncode == 0,
            "state": state_match.group(1) if state_match else "absent",
            "pid": int(pid_match.group(1)) if pid_match else None,
            "pid_absent": pid_match is None,
            "config_path": str(plist),
            "config_sha256": plist_sha256,
        }
    return services


def _observe_executor_closure_live() -> Mapping[str, Any]:
    try:
        lsof = subprocess.run(
            ["/usr/sbin/lsof", "-Fpnf", "--", DELIVERY_DB_PATH],
            check=False,
            capture_output=True,
            text=True,
            timeout=8,
        )
        pgrep = subprocess.run(
            [
                "/usr/bin/pgrep",
                "-fl",
                (
                    "pnc_rca_(kafka_consumer|outbox_dispatcher|"
                    "delivery_collector|delivery_dispatcher)|"
                    "run_rca_service_request"
                ),
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=8,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_executor_live_observation_failed"
        ) from exc
    if (
        lsof.returncode not in {0, 1}
        or lsof.stderr
        or pgrep.returncode not in {0, 1}
        or pgrep.stderr
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_executor_live_observation_failed"
        )
    db_pids = sorted(
        {
            int(line[1:])
            for line in lsof.stdout.splitlines()
            if line.startswith("p") and line[1:].isdigit()
        }
    )
    executor_pids = sorted(
        {
            int(line.split(None, 1)[0])
            for line in pgrep.stdout.splitlines()
            if line.split(None, 1) and line.split(None, 1)[0].isdigit()
        }
    )
    return {
        "live_db_path": DELIVERY_DB_PATH,
        "active_db_writer_pids": db_pids,
        "active_rca_executor_pids": executor_pids,
        "open_live_db_write_handles": [],
        "closure_complete": not db_pids and not executor_pids,
    }


_CANONICAL_KAFKA_TARGET_PREREAD = r'''import hashlib,json,pathlib,sys
root=pathlib.Path(sys.argv[1])
env_path=pathlib.Path(sys.argv[2])
sys.path.insert(0,str(root))
from scripts import pnc_rca_kafka_consumer as consumer_module
from gateway import pnc_rca_kafka_contract as contract_module
expected_consumer=root/'scripts/pnc_rca_kafka_consumer.py'
expected_contract=root/'gateway/pnc_rca_kafka_contract.py'
if pathlib.Path(consumer_module.__file__).resolve()!=expected_consumer or pathlib.Path(contract_module.__file__).resolve()!=expected_contract:
    raise SystemExit('module_origin_mismatch')
consumer_module.load_consumer_environment(str(env_path))
config=consumer_module.ConsumerConfig.from_env()
if config.topic!=sys.argv[3]:
    raise SystemExit('topic_mismatch')
from kafka import KafkaConsumer
from kafka.structs import TopicPartition
kwargs=config.kafka_kwargs()
kwargs['group_id']=None
kwargs['enable_auto_commit']=False
kwargs['client_id']='rca_release_preflight_exact_offset'
consumer=KafkaConsumer(**kwargs)
tp=TopicPartition(sys.argv[3],int(sys.argv[4]))
commit_called=False
try:
    consumer.assign([tp])
    beginning=int(consumer.beginning_offsets([tp],timeout_ms=10000)[tp])
    end=int(consumer.end_offsets([tp],timeout_ms=10000)[tp])
    target=int(sys.argv[5])
    if not beginning<=target<end:
        raise SystemExit('target_offset_not_retained')
    consumer.seek(tp,target)
    polled=consumer.poll(timeout_ms=10000,max_records=1)
    records=[record for values in polled.values() for record in values]
    if len(records)!=1:
        raise SystemExit('target_record_missing')
    record=records[0]
    if record.topic!=sys.argv[3] or record.partition!=int(sys.argv[4]) or record.offset!=target:
        raise SystemExit('target_coordinate_mismatch')
    raw=bytes(record.value)
    timestamp_type={0:'create_time',1:'log_append_time'}.get(record.timestamp_type)
    if not isinstance(record.timestamp,int) or record.timestamp<0 or timestamp_type is None:
        raise SystemExit('target_timestamp_invalid')
    raw_sha=hashlib.sha256(raw).hexdigest()
    if raw_sha!=sys.argv[6]:
        raise SystemExit('target_raw_sha256_mismatch')
    classified=contract_module.classify_workflow_event(topic=record.topic,value=raw,policy=config.policy)
    if classified.decision!='accepted' or classified.normalized is None:
        raise SystemExit('target_policy_rejected')
    admission=contract_module.build_event_admission(classified.normalized,topic=record.topic,partition=record.partition,offset=record.offset)
    refs=admission.source_refs
    if admission.business_key!=sys.argv[7] or admission.submission_key!=sys.argv[8] or refs.work_item_id!=sys.argv[9] or refs.project_key!=sys.argv[10] or refs.work_item_type_key!=sys.argv[11]:
        raise SystemExit('target_admission_mismatch')
    position=int(consumer.position(tp))
finally:
    consumer.close(autocommit=False)
result={
 'schema_version':'pnc_rca_kafka_exact_offset_preread_v1',
 'topic':sys.argv[3],'partition':int(sys.argv[4]),'offset':int(sys.argv[5]),
 'event_uid':f'{sys.argv[3]}:{sys.argv[4]}:{sys.argv[5]}',
 'retained_start':beginning,'retained_end':end,'raw_sha256':raw_sha,
 'record_timestamp_ms':record.timestamp,'record_timestamp_type':timestamp_type,
 'raw_size_bytes':len(raw),'work_item_id':refs.work_item_id,
 'business_key':admission.business_key,'submission_key':admission.submission_key,
 'project_key':refs.project_key,'work_item_type_key':refs.work_item_type_key,
 'classification_decision':classified.decision,
 'policy_version':classified.normalized.creation_rule_version,
 'assignment_mode':'explicit_single_partition','assigned_partitions':[int(sys.argv[4])],
 'seek_offset':int(sys.argv[5]),'position_after_read':position,
 'group_id':None,'enable_auto_commit':False,'commit_called':commit_called,
 'raw_payload_persisted':False,'secret_material_persisted':False,
 'consumer_module_sha256':hashlib.sha256(expected_consumer.read_bytes()).hexdigest(),
 'contract_module_sha256':hashlib.sha256(expected_contract.read_bytes()).hexdigest(),
}
print(json.dumps(result,sort_keys=True,separators=(',',':')))
'''


def _observe_kafka_record_live(
    *,
    host_commit: str,
    host_tree: str,
    topic: str,
    partition: int,
    offset: int,
    raw_sha256: str,
    business_key: str,
    submission_key: str,
    work_item_id: str,
    project_key: str,
    work_item_type_key: str,
) -> Mapping[str, Any]:
    observed = _observe_canonical_host_binding(
        expected_commit=host_commit, expected_tree=host_tree
    )
    interpreter, interpreter_binary = _canonical_host_interpreter_paths()
    try:
        completed = subprocess.run(
            [
                str(interpreter),
                "-I",
                "-B",
                "-c",
                _CANONICAL_KAFKA_TARGET_PREREAD,
                CANONICAL_HOST_ROOT,
                CANONICAL_HOST_ENV,
                topic,
                str(partition),
                str(offset),
                raw_sha256,
                business_key,
                submission_key,
                work_item_id,
                project_key,
                work_item_type_key,
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
            cwd=CANONICAL_HOST_ROOT,
            env={
                "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
                "HOME": str(Path.home()),
                "LC_ALL": "C",
                "PYTHONDONTWRITEBYTECODE": "1",
            },
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_target_kafka_preread_failed"
        ) from exc
    if completed.returncode != 0 or completed.stderr or not completed.stdout:
        raise ProdE2EReleaseError(
            "prod_e2e_release_target_kafka_preread_failed"
        )
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_target_kafka_preread_invalid"
        ) from exc
    expected = {
        "schema_version",
        "topic",
        "partition",
        "offset",
        "event_uid",
        "retained_start",
        "retained_end",
        "raw_sha256",
        "record_timestamp_ms",
        "record_timestamp_type",
        "raw_size_bytes",
        "work_item_id",
        "business_key",
        "submission_key",
        "project_key",
        "work_item_type_key",
        "classification_decision",
        "policy_version",
        "assignment_mode",
        "assigned_partitions",
        "seek_offset",
        "position_after_read",
        "group_id",
        "enable_auto_commit",
        "commit_called",
        "raw_payload_persisted",
        "secret_material_persisted",
        "consumer_module_sha256",
        "contract_module_sha256",
    }
    if (
        not isinstance(value, dict)
        or set(value) != expected
        or value.get("schema_version")
        != "pnc_rca_kafka_exact_offset_preread_v1"
        or value.get("topic") != topic
        or value.get("partition") != partition
        or value.get("offset") != offset
        or value.get("event_uid") != f"{topic}:{partition}:{offset}"
        or not isinstance(value.get("retained_start"), int)
        or not isinstance(value.get("retained_end"), int)
        or not value["retained_start"] <= offset < value["retained_end"]
        or value.get("raw_sha256") != raw_sha256
        or not isinstance(value.get("record_timestamp_ms"), int)
        or isinstance(value.get("record_timestamp_ms"), bool)
        or value.get("record_timestamp_ms", -1) < 0
        or value.get("record_timestamp_type")
        not in {"create_time", "log_append_time"}
        or not isinstance(value.get("raw_size_bytes"), int)
        or isinstance(value.get("raw_size_bytes"), bool)
        or value.get("raw_size_bytes", 0) <= 0
        or value.get("work_item_id") != work_item_id
        or value.get("business_key") != business_key
        or value.get("submission_key") != submission_key
        or value.get("project_key") != project_key
        or value.get("work_item_type_key") != work_item_type_key
        or value.get("classification_decision") != "accepted"
        or not _required_text(
            value.get("policy_version"), field="kafka_preread_policy_version"
        )
        or value.get("assignment_mode") != "explicit_single_partition"
        or value.get("assigned_partitions") != [partition]
        or value.get("seek_offset") != offset
        or value.get("position_after_read") != offset + 1
        or value.get("group_id") is not None
        or value.get("enable_auto_commit") is not False
        or value.get("commit_called") is not False
        or value.get("raw_payload_persisted") is not False
        or value.get("secret_material_persisted") is not False
        or value.get("consumer_module_sha256")
        != observed["required_file_sha256"]["scripts/pnc_rca_kafka_consumer.py"]
        or value.get("contract_module_sha256")
        != observed["required_file_sha256"]["gateway/pnc_rca_kafka_contract.py"]
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_target_kafka_preread_invalid"
        )
    return {
        **value,
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "observer_script_sha256": hashlib.sha256(
            _CANONICAL_KAFKA_TARGET_PREREAD.encode("utf-8")
        ).hexdigest(),
        "interpreter_sha256": hashlib.sha256(
            interpreter_binary.read_bytes()
        ).hexdigest(),
        "host_commit": observed["commit"],
        "host_tree": observed["tree"],
    }


def _kafka_observation_identity(value: Any, *, artifact: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or "observed_at" not in value:
        raise ProdE2EReleaseError(
            f"prod_e2e_release_{artifact}_invalid"
        )
    return {
        key: item
        for key, item in value.items()
        if key not in {"observed_at", "retained_start", "retained_end"}
    }


def _validate_kafka_observation_pair(
    recorded: Any,
    live: Any,
    *,
    artifact: str,
    recorded_not_before: datetime,
    recorded_not_after: datetime,
    live_now: datetime,
) -> Mapping[str, Any]:
    recorded_identity = _kafka_observation_identity(
        recorded, artifact=artifact
    )
    live_identity = _kafka_observation_identity(live, artifact=artifact)
    recorded_at = _timestamp(
        recorded["observed_at"], field=f"{artifact}_recorded_at"
    )
    live_at = _timestamp(live["observed_at"], field=f"{artifact}_live_at")
    offset = recorded_identity.get("offset")
    recorded_start = recorded.get("retained_start")
    recorded_end = recorded.get("retained_end")
    live_start = live.get("retained_start")
    live_end = live.get("retained_end")
    if (
        recorded_identity != live_identity
        or not isinstance(offset, int)
        or isinstance(offset, bool)
        or any(
            not isinstance(item, int) or isinstance(item, bool)
            for item in (recorded_start, recorded_end, live_start, live_end)
        )
        or not recorded_start <= offset < recorded_end
        or not live_start <= offset < live_end
        or live_end < recorded_end
        or recorded_at < recorded_not_before
        or recorded_at > recorded_not_after
        or live_at < recorded_at
        or live_at > live_now + MAX_FUTURE_SKEW
        or live_now - live_at > MAX_CLOSEOUT_AGE
    ):
        raise ProdE2EReleaseError(
            f"prod_e2e_release_{artifact}_mismatch"
        )
    return {
        **dict(recorded),
        "observed_at": recorded_at.isoformat(),
        "live_observed_at": live_at.isoformat(),
        "live_retained_start": live_start,
        "live_retained_end": live_end,
    }


def _observe_target_kafka_record_live(
    *, host_commit: str, host_tree: str
) -> Mapping[str, Any]:
    return _observe_kafka_record_live(
        host_commit=host_commit,
        host_tree=host_tree,
        topic=TOPIC,
        partition=PARTITION,
        offset=TARGET_OFFSET,
        raw_sha256=TARGET_RAW_SHA256,
        business_key=TARGET_BUSINESS_KEY,
        submission_key=TARGET_SUBMISSION_KEY,
        work_item_id=TARGET_WORK_ITEM_ID,
        project_key=TARGET_PROJECT_KEY,
        work_item_type_key=TARGET_WORK_ITEM_TYPE_KEY,
    )


def _observe_activation_anchors_live(value: Mapping[str, Any]) -> Mapping[str, Any]:
    if set(value) != {
        "live_env_path",
        "live_env_sha256",
        "active_binding_path",
        "active_binding_exists",
        "active_binding_sha256",
    }:
        raise ProdE2EReleaseError(
            "prod_e2e_release_activation_anchor_shape_invalid"
        )
    env_path = Path(str(value.get("live_env_path") or "")).absolute()
    binding_path = Path(str(value.get("active_binding_path") or "")).absolute()
    allowed_binding_root = Path(
        "/Users/songying/.hermes/runtime/pnc_agent/feishu_issue_kafka_rca"
    )
    if (
        str(env_path) != CANONICAL_HOST_ENV
        or binding_path.parent != allowed_binding_root
        or not binding_path.name
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_activation_anchor_path_invalid"
        )

    def stable_digest(path: Path, *, required: bool) -> tuple[bool, str]:
        try:
            before = os.lstat(path)
        except FileNotFoundError:
            if required:
                raise ProdE2EReleaseError(
                    "prod_e2e_release_activation_anchor_unavailable"
                )
            return False, EMPTY_SHA256
        if (
            stat.S_ISLNK(before.st_mode)
            or not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or stat.S_IMODE(before.st_mode) & 0o077
        ):
            raise ProdE2EReleaseError(
                "prod_e2e_release_activation_anchor_not_owner_only"
            )
        raw = path.read_bytes()
        after = os.lstat(path)
        if (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise ProdE2EReleaseError(
                "prod_e2e_release_activation_anchor_unstable"
            )
        return True, hashlib.sha256(raw).hexdigest()

    _env_exists, env_sha256 = stable_digest(env_path, required=True)
    binding_exists, binding_sha256 = stable_digest(binding_path, required=False)
    observed = {
        "live_env_path": str(env_path),
        "live_env_sha256": env_sha256,
        "active_binding_path": str(binding_path),
        "active_binding_exists": binding_exists,
        "active_binding_sha256": binding_sha256,
    }
    if dict(value) != observed:
        raise ProdE2EReleaseError(
            "prod_e2e_release_activation_anchor_live_mismatch"
        )
    return observed


def _validate_execution_preflight(
    owned: OwnedJson,
    *,
    release_id: str,
    now: datetime,
    db_cutover: Mapping[str, Any],
    host: Mapping[str, Any],
    worker: Mapping[str, Any],
    feishu_completion: Mapping[str, Any],
    request_sha256: str,
    release_bom_sha256: str,
    approval_sha256: str,
    baseline_approval_sha256: str,
    authorization_sha256: str,
) -> Mapping[str, Any]:
    body = owned.body
    expected_fields = {
        "schema_version",
        "release_id",
        "request_sha256",
        "release_bom_sha256",
        "approval_sha256",
        "baseline_approval_sha256",
        "authorization_sha256",
        "observed_at",
        "writers_stopped_at",
        "host_services",
        "host_live_runtime",
        "vm_services",
        "executor_closure",
        "target_kafka_preread",
        "target_input_revalidation",
        "fresh_live_backup",
        "fresh_live_pre_logical_sha256",
        "activation_anchors_before",
        "rollback_contract",
        "production_effects",
    }
    observed_at = _timestamp(
        body.get("observed_at"), field="execution_preflight_observed_at"
    )
    writers_stopped_at = _timestamp(
        body.get("writers_stopped_at"), field="writers_stopped_at"
    )
    if (
        set(body) != expected_fields
        or body.get("schema_version") != EXECUTION_PREFLIGHT_SCHEMA_VERSION
        or body.get("release_id") != release_id
        or body.get("request_sha256") != request_sha256
        or body.get("release_bom_sha256") != release_bom_sha256
        or body.get("approval_sha256") != approval_sha256
        or body.get("baseline_approval_sha256") != baseline_approval_sha256
        or body.get("authorization_sha256") != authorization_sha256
        or observed_at > now + MAX_FUTURE_SKEW
        or now - observed_at > MAX_FINAL_OBSERVATION_AGE
        or writers_stopped_at > observed_at
    ):
        raise ProdE2EReleaseError("prod_e2e_release_execution_preflight_invalid")
    host_services = body.get("host_services")
    host_live_runtime = body.get("host_live_runtime")
    vm_services = body.get("vm_services")
    executor = body.get("executor_closure")
    target_kafka_preread = body.get("target_kafka_preread")
    target_input_revalidation = body.get("target_input_revalidation")
    backup = body.get("fresh_live_backup")
    rollback = body.get("rollback_contract")
    anchors = body.get("activation_anchors_before")
    effects = body.get("production_effects")
    if not all(
        isinstance(item, Mapping)
        for item in (
            host_services,
            host_live_runtime,
            vm_services,
            executor,
            target_kafka_preread,
            target_input_revalidation,
            backup,
            anchors,
            rollback,
            effects,
        )
    ):
        raise ProdE2EReleaseError("prod_e2e_release_execution_preflight_invalid")
    live_host_services = _observe_host_writer_stop_live()
    if (
        set(host_services) != set(HOST_SERVICE_LABELS)
        or dict(host_services) != live_host_services
        or any(
            host_services[label].get("config_sha256")
            != host["service_config_sha256"].get(label)
            for label in HOST_SERVICE_LABELS
        )
        or any(
            item.get("pid_absent") is not True
            or item.get("pid") is not None
            or item.get("state") == "running"
            for item in host_services.values()
            if isinstance(item, Mapping)
        )
    ):
        raise ProdE2EReleaseError("prod_e2e_release_host_writers_not_stopped")
    observed_host_live_runtime = _observe_host_live_runtime(expected_host=host)
    if dict(host_live_runtime) != observed_host_live_runtime:
        raise ProdE2EReleaseError(
            "prod_e2e_release_preflight_host_live_runtime_invalid"
        )
    live_vm = _observe_vm_components_live(
        expected_viewer_origin=str(host["viewer_origin"]),
        environment_phase="pre",
    )
    live_services = live_vm.get("services")
    if not isinstance(live_services, Mapping) or set(live_services) != set(
        VM_SERVICE_UNITS
    ):
        raise ProdE2EReleaseError("prod_e2e_release_vm_writer_not_stopped")
    expected_vm_services = {
        unit: {
            **dict(live_services[unit]),
            "observer_script_sha256": live_vm.get("observer_script_sha256"),
            "machine_identity_sha256": live_vm.get("machine_identity_sha256"),
        }
        for unit in VM_SERVICE_UNITS
    }
    if (
        dict(vm_services) != expected_vm_services
        or any(
            item.get("active_state") != "inactive"
            or item.get("sub_state") != "dead"
            or item.get("main_pid") != 0
            for item in expected_vm_services.values()
        )
        or expected_vm_services[VM_DAEMON_UNIT].get("unit_config_sha256")
        != worker.get("daemon_unit_config_sha256")
        or expected_vm_services[VM_REPORT_UNIT].get("unit_config_sha256")
        != worker.get("report_unit_config_sha256")
    ):
        raise ProdE2EReleaseError("prod_e2e_release_vm_writer_config_mismatch")
    live_executor = _observe_executor_closure_live()
    if dict(executor) != live_executor or live_executor["closure_complete"] is not True:
        raise ProdE2EReleaseError(
            "prod_e2e_release_executor_closure_invalid"
        )
    live_target_kafka = _observe_target_kafka_record_live(
        host_commit=str(host["commit"]), host_tree=str(host["tree"])
    )
    validated_target_kafka = _validate_kafka_observation_pair(
        target_kafka_preread,
        live_target_kafka,
        artifact="target_kafka_preread",
        recorded_not_before=writers_stopped_at,
        recorded_not_after=observed_at,
        live_now=now,
    )
    live_target_input = _observe_target_input_gate_live(
        host_commit=str(host["commit"]), host_tree=str(host["tree"])
    )
    initial_input = feishu_completion.get("input_preread")
    initial_fields = feishu_completion.get("field_preread")
    if not isinstance(initial_input, Mapping) or not isinstance(
        initial_fields, Mapping
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_target_input_revalidation_invalid"
        )
    validated_target_input = _validate_target_input_revalidation_pair(
        target_input_revalidation,
        live_target_input,
        initial_input=initial_input,
        initial_fields=initial_fields,
        writers_stopped_at=writers_stopped_at,
        receipt_observed_at=observed_at,
        now=now,
    )
    backup_path, backup_sha256, backup_size = _read_owned_blob(
        Path(str(backup.get("path") or "")), artifact="fresh_live_backup"
    )
    captured_at = _timestamp(
        backup.get("captured_at"), field="fresh_live_backup_captured_at"
    )
    projection = _run_canonical_db_projection(
        db_path=backup_path,
        host_commit=str(host["commit"]),
        host_tree=str(host["tree"]),
    )
    live_projection = _run_canonical_db_projection(
        db_path=Path(DELIVERY_DB_PATH),
        host_commit=str(host["commit"]),
        host_tree=str(host["tree"]),
        allow_live=True,
    )
    logical_sha256 = projection["projection"]["logical_sha256"]
    live_logical_sha256 = live_projection["projection"]["logical_sha256"]
    backup_stat = os.lstat(backup_path)
    backup_mtime = datetime.fromtimestamp(
        backup_stat.st_mtime, tz=timezone.utc
    )
    if (
        set(backup) != {"path", "sha256", "size_bytes", "captured_at"}
        or backup_sha256 != backup.get("sha256")
        or backup_size != backup.get("size_bytes")
        or captured_at < writers_stopped_at
        or captured_at > observed_at
        or backup_mtime < writers_stopped_at - timedelta(seconds=1)
        or abs((backup_mtime - captured_at).total_seconds()) > 5
        or logical_sha256
        != db_cutover.get("approved_source_logical_sha256")
        or live_logical_sha256 != logical_sha256
        or body.get("fresh_live_pre_logical_sha256") != logical_sha256
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_fresh_live_backup_invalid"
        )
    expected_rollback = {
        "backup_path": str(backup_path),
        "backup_sha256": backup_sha256,
        "live_env_path": CANONICAL_HOST_ENV,
        "live_env_pre_sha256": host["host_environment_transition"][
            "pre_sha256"
        ],
        "live_env_post_sha256": host["host_environment_transition"][
            "post_sha256"
        ],
        "vm_report_env_path": VM_REPORT_ENV_PATH,
        "vm_report_env_pre_exists": worker["report_environment_transition"]
        ["pre_exists"],
        "vm_report_env_pre_sha256": worker["report_environment_transition"]
        ["pre_sha256"],
        "vm_report_env_post_sha256": worker["report_environment_transition"]
        ["post_sha256"],
        "restore_before_environment_or_binding_on_failure": True,
        "environment_written": False,
        "active_binding_written": False,
    }
    observed_anchors = _observe_activation_anchors_live(anchors)
    if (
        observed_anchors.get("live_env_sha256")
        != host["host_environment_transition"]["pre_sha256"]
        or dict(rollback) != expected_rollback
        or dict(effects) != {
        "services_stopped": True,
        "live_database_mutated": False,
        "environment_written": False,
        "active_binding_written": False,
        "feishu_written": False,
        "kafka_offsets_mutated": False,
        }
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_preflight_rollback_contract_invalid"
        )
    return {
        "path": str(owned.path),
        "sha256": owned.sha256,
        "observed_at": observed_at.isoformat(),
        "writers_stopped_at": writers_stopped_at.isoformat(),
        "host_services": dict(host_services),
        "host_live_runtime": observed_host_live_runtime,
        "vm_services": expected_vm_services,
        "executor_closure": dict(executor),
        "target_kafka_preread": validated_target_kafka,
        "target_input_revalidation": validated_target_input,
        "fresh_live_backup": {
            **dict(backup),
            "logical_sha256": logical_sha256,
            "canonical_projection_validator_sha256": projection[
                "validator_script_sha256"
            ],
            "live_logical_sha256": live_logical_sha256,
            "live_projection_validator_sha256": live_projection[
                "validator_script_sha256"
            ],
        },
        "activation_anchors_before": observed_anchors,
        "rollback_contract": expected_rollback,
    }


_CANONICAL_TARGET_BUNDLE_VERIFIER = r'''import hashlib,json,pathlib,sys
root=pathlib.Path(sys.argv[1])
bundle_path=pathlib.Path(sys.argv[2])
sys.path.insert(0,str(root))
from gateway import pnc_rca_admission as admission_module
from gateway import pnc_rca_delivery_contract as delivery_module
expected_admission=root/'gateway/pnc_rca_admission.py'
expected_delivery=root/'gateway/pnc_rca_delivery_contract.py'
if pathlib.Path(admission_module.__file__).resolve()!=expected_admission or pathlib.Path(delivery_module.__file__).resolve()!=expected_delivery:
    raise SystemExit('module_origin_mismatch')
bundle=json.loads(bundle_path.read_bytes())
admission=admission_module.build_rca_admission(**bundle['admission_input'])
verified=delivery_module.verify_delivery_bundle(
 admission=admission,
 delivery_contract=bundle['delivery_contract'],
 delivery_manifest=bundle['delivery_manifest'],
 observed_files=bundle['observed_files'],
 html_dependencies=bundle['html_dependencies'],
)
refs=admission.source_refs
expected={
 'business_key':sys.argv[3],'submission_key':sys.argv[4],
 'topic':sys.argv[5],'partition':int(sys.argv[6]),'offset':int(sys.argv[7]),
 'project_key':sys.argv[8],'work_item_id':sys.argv[9],
 'issue_url':sys.argv[10],'project_simple_name':sys.argv[11],
}
if admission.trigger_kind!='issue_created' or admission.generation!=1 or admission.business_key!=expected['business_key'] or admission.submission_key!=expected['submission_key']:
    raise SystemExit('admission_identity_mismatch')
if refs.topic!=expected['topic'] or refs.partition!=expected['partition'] or refs.offset!=expected['offset'] or refs.project_key!=expected['project_key'] or refs.project_simple_name!=expected['project_simple_name'] or refs.work_item_id!=expected['work_item_id']:
    raise SystemExit('admission_source_mismatch')
for key in ('business_key','submission_key','project_key','work_item_id'):
    if getattr(verified,key)!=expected[key]:
        raise SystemExit('delivery_identity_mismatch')
if verified.generation!=1 or verified.issue_url!=expected['issue_url'] or verified.effect_payload.get('project_simple_name')!=expected['project_simple_name']:
    raise SystemExit('delivery_target_mismatch')
updates=verified.effect_payload.get('field_updates')
if not isinstance(updates,list) or [item.get('field_key') for item in updates]!=['field_9193cb','field_8c912e']:
    raise SystemExit('field_contract_mismatch')
field_values={str(item['field_key']):str(item.get('field_value') or '') for item in updates}
if not field_values['field_9193cb'].strip() or field_values['field_8c912e']!=verified.report_url:
    raise SystemExit('field_value_invalid')
comment=str(verified.effect_payload.get('comment_content') or '')
marker=str(verified.effect_payload.get('marker') or '')
if verified.effect_payload.get('report_link_kind')!='manifest_html' or verified.report_url not in comment or verified.foxglove_url in comment:
    raise SystemExit('report_link_contract_invalid')
if not comment.strip() or not marker or comment.splitlines()[0]!=marker or comment.splitlines().count(marker)!=1:
    raise SystemExit('comment_content_invalid')
def digest(value):
    raw=value.encode('utf-8')
    return {'sha256':hashlib.sha256(raw).hexdigest(),'utf8_bytes':len(raw)}
result={
 'schema_version':'pnc_rca_canonical_target_bundle_verification_v2',
 'delivery_id':verified.delivery_id,'effect_key':verified.effect_key,
 'semantic_payload_sha256':verified.semantic_payload_sha256,
 'artifact_set_id':verified.artifact_set_id,'business_key':verified.business_key,
 'submission_key':verified.submission_key,'generation':verified.generation,
 'project_key':verified.project_key,'project_simple_name':refs.project_simple_name,
 'work_item_type_key':verified.work_item_type_key,
 'work_item_id':verified.work_item_id,'target_key':verified.target_key,
 'issue_url':verified.issue_url,'report_url':verified.report_url,
 'report_link_kind':verified.effect_payload['report_link_kind'],
 'field_values':{key:digest(value) for key,value in field_values.items()},
 'comment_content':digest(comment),'marker':digest(marker),
 'admission_module_sha256':hashlib.sha256(expected_admission.read_bytes()).hexdigest(),
 'delivery_module_sha256':hashlib.sha256(expected_delivery.read_bytes()).hexdigest(),
 'raw_values_persisted':False,
}
print(json.dumps(result,sort_keys=True,separators=(',',':')))
'''


def _run_canonical_target_bundle_verifier(
    owned: OwnedJson,
    *,
    host_commit: str,
    host_tree: str,
    expected: Mapping[str, Any] | None = None,
) -> Mapping[str, Any]:
    observed = _observe_canonical_host_binding(
        expected_commit=host_commit, expected_tree=host_tree
    )
    root = Path(CANONICAL_HOST_ROOT)
    interpreter, interpreter_binary = _canonical_host_interpreter_paths()
    identity = dict(
        expected
        or {
            "business_key": TARGET_BUSINESS_KEY,
            "submission_key": TARGET_SUBMISSION_KEY,
            "topic": TOPIC,
            "partition": PARTITION,
            "offset": TARGET_OFFSET,
            "project_key": TARGET_PROJECT_KEY,
            "project_simple_name": TARGET_PROJECT_SIMPLE_NAME,
            "work_item_id": TARGET_WORK_ITEM_ID,
            "issue_url": TARGET_ISSUE_URL,
        }
    )
    if set(identity) != {
        "business_key",
        "submission_key",
        "topic",
        "partition",
        "offset",
        "project_key",
        "project_simple_name",
        "work_item_id",
        "issue_url",
    }:
        raise ProdE2EReleaseError(
            "prod_e2e_release_target_bundle_identity_invalid"
        )
    for field in (
        "business_key",
        "submission_key",
        "topic",
        "project_key",
        "project_simple_name",
        "work_item_id",
        "issue_url",
    ):
        _required_text(identity.get(field), field=f"bundle_{field}")
    if (
        not isinstance(identity.get("partition"), int)
        or isinstance(identity.get("partition"), bool)
        or identity["partition"] < 0
        or not isinstance(identity.get("offset"), int)
        or isinstance(identity.get("offset"), bool)
        or identity["offset"] < 0
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_target_bundle_identity_invalid"
        )
    issue_url = str(identity["issue_url"])
    expected_issue_url = (
        f"https://project.feishu.cn/{identity['project_simple_name']}"
        f"/issue/detail/{identity['work_item_id']}"
    )
    if issue_url != expected_issue_url:
        raise ProdE2EReleaseError(
            "prod_e2e_release_target_bundle_identity_invalid"
        )
    try:
        completed = subprocess.run(
            [
                str(interpreter),
                "-I",
                "-B",
                "-c",
                _CANONICAL_TARGET_BUNDLE_VERIFIER,
                str(root),
                str(owned.path),
                str(identity["business_key"]),
                str(identity["submission_key"]),
                str(identity["topic"]),
                str(identity["partition"]),
                str(identity["offset"]),
                str(identity["project_key"]),
                str(identity["work_item_id"]),
                issue_url,
                str(identity["project_simple_name"]),
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
            cwd=str(root),
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_target_bundle_verification_failed"
        ) from exc
    if completed.returncode != 0 or completed.stderr or not completed.stdout:
        raise ProdE2EReleaseError(
            "prod_e2e_release_target_bundle_verification_failed"
        )
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_target_bundle_verification_invalid"
        ) from exc
    required = {
        "schema_version",
        "delivery_id",
        "effect_key",
        "semantic_payload_sha256",
        "artifact_set_id",
        "business_key",
        "submission_key",
        "generation",
        "project_key",
        "project_simple_name",
        "work_item_type_key",
        "work_item_id",
        "target_key",
        "issue_url",
        "report_url",
        "report_link_kind",
        "field_values",
        "comment_content",
        "marker",
        "admission_module_sha256",
        "delivery_module_sha256",
        "raw_values_persisted",
    }
    if (
        not isinstance(value, dict)
        or set(value) != required
        or value.get("schema_version")
        != "pnc_rca_canonical_target_bundle_verification_v2"
        or value.get("raw_values_persisted") is not False
        or value.get("business_key") != identity["business_key"]
        or value.get("submission_key") != identity["submission_key"]
        or value.get("generation") != 1
        or value.get("project_key") != identity["project_key"]
        or value.get("project_simple_name") != identity["project_simple_name"]
        or value.get("work_item_id") != identity["work_item_id"]
        or value.get("issue_url") != issue_url
        or value.get("report_link_kind") != "manifest_html"
        or value.get("field_values", {}).get(REPORT_FIELD_KEY, {}).get("sha256")
        != hashlib.sha256(str(value.get("report_url") or "").encode("utf-8")).hexdigest()
        or set(value.get("field_values") or {}) != set(TARGET_FIELD_KEYS)
        or value.get("admission_module_sha256")
        != observed["required_file_sha256"]["gateway/pnc_rca_admission.py"]
        or value.get("delivery_module_sha256")
        != observed["required_file_sha256"][
            "gateway/pnc_rca_delivery_contract.py"
        ]
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_target_bundle_verification_invalid"
        )
    for descriptor in [
        *(value["field_values"].values()),
        value.get("comment_content"),
        value.get("marker"),
    ]:
        if (
            not isinstance(descriptor, Mapping)
            or set(descriptor) != {"sha256", "utf8_bytes"}
            or _sha256(descriptor.get("sha256"), field="bundle_value_sha256")
            != descriptor.get("sha256")
            or not isinstance(descriptor.get("utf8_bytes"), int)
            or isinstance(descriptor.get("utf8_bytes"), bool)
            or descriptor.get("utf8_bytes", 0) < 1
        ):
            raise ProdE2EReleaseError(
                "prod_e2e_release_target_bundle_verification_invalid"
            )
    for field in (
        "semantic_payload_sha256",
        "admission_module_sha256",
        "delivery_module_sha256",
    ):
        _sha256(value.get(field), field=f"target_bundle_{field}")
    return {
        **value,
        "bundle_path": str(owned.path),
        "bundle_sha256": owned.sha256,
        "host_commit": observed["commit"],
        "host_tree": observed["tree"],
        "interpreter_path": str(interpreter),
        "interpreter_sha256": hashlib.sha256(
            interpreter_binary.read_bytes()
        ).hexdigest(),
        "validator_script_sha256": hashlib.sha256(
            _CANONICAL_TARGET_BUNDLE_VERIFIER.encode("utf-8")
        ).hexdigest(),
    }


_CANONICAL_MEEGLE_INPUT_GATE = r'''import hashlib,json,pathlib,sys
root=pathlib.Path(sys.argv[1])
sys.path.insert(0,str(root))
from gateway import pnc_issue_context as issue_context
from scripts import pnc_rca_delivery_dispatcher as dispatcher
expected_context=root/'gateway/pnc_issue_context.py'
expected_dispatcher=root/'scripts/pnc_rca_delivery_dispatcher.py'
if pathlib.Path(issue_context.__file__).resolve()!=expected_context or pathlib.Path(dispatcher.__file__).resolve()!=expected_dispatcher:
    raise SystemExit('module_origin_mismatch')
issue_context._capture_g1q3_issue_context=lambda **_kwargs: None
result=issue_context.fetch_g1q3_issue_context_result_via_meegle(project_key=sys.argv[2],work_item_id=sys.argv[3])
adapter=dispatcher.MeegleIssueCommentAdapter()
fields=adapter.get_fields(sys.argv[2],sys.argv[3],('field_9193cb','field_8c912e'))
if result.status!='fields_extracted' or result.source!='meegle' or not result.context_text or result.blocker or result.errors or fields.get('success') is not True:
    raise SystemExit('meegle_input_gate_failed')
field_values=fields.get('fields')
if not isinstance(field_values,dict) or set(field_values)!=set(('field_9193cb','field_8c912e')):
    raise SystemExit('meegle_input_gate_invalid')
context_raw=result.context_text.encode('utf-8')
def digest(value):
    raw=str(value or '').encode('utf-8')
    return {'sha256':hashlib.sha256(raw).hexdigest(),'utf8_bytes':len(raw)}
print(json.dumps({
 'schema_version':'pnc_rca_fresh_target_input_revalidation_v1',
 'project_key':sys.argv[2],'work_item_id':sys.argv[3],
 'source':'official_meegle_api','status':result.status,
 'context_sha256':hashlib.sha256(context_raw).hexdigest(),
 'context_utf8_bytes':len(context_raw),
 'fields':{key:digest(value) for key,value in field_values.items()},
 'validation_blocker_kind':'','error_classes':[],
 'sidecar_capture_disabled':True,'raw_values_persisted':False,
 'context_module_sha256':hashlib.sha256(expected_context.read_bytes()).hexdigest(),
 'dispatcher_module_sha256':hashlib.sha256(expected_dispatcher.read_bytes()).hexdigest(),
},sort_keys=True,separators=(',',':')))
'''


def _observe_target_input_gate_live(
    *, host_commit: str, host_tree: str
) -> Mapping[str, Any]:
    observed = _observe_canonical_host_binding(
        expected_commit=host_commit, expected_tree=host_tree
    )
    interpreter, interpreter_binary = _canonical_host_interpreter_paths()
    try:
        completed = subprocess.run(
            [
                str(interpreter),
                "-I",
                "-B",
                "-c",
                _CANONICAL_MEEGLE_INPUT_GATE,
                CANONICAL_HOST_ROOT,
                TARGET_PROJECT_KEY,
                TARGET_WORK_ITEM_ID,
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=120,
            cwd=CANONICAL_HOST_ROOT,
            env={**os.environ, "MEEGLE_HOST": "project.feishu.cn"},
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_target_input_revalidation_failed"
        ) from exc
    if completed.returncode != 0 or completed.stderr or not completed.stdout:
        raise ProdE2EReleaseError(
            "prod_e2e_release_target_input_revalidation_failed"
        )
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_target_input_revalidation_invalid"
        ) from exc
    expected_fields = {
        "schema_version",
        "project_key",
        "work_item_id",
        "source",
        "status",
        "context_sha256",
        "context_utf8_bytes",
        "fields",
        "validation_blocker_kind",
        "error_classes",
        "sidecar_capture_disabled",
        "raw_values_persisted",
        "context_module_sha256",
        "dispatcher_module_sha256",
    }
    if (
        not isinstance(value, dict)
        or set(value) != expected_fields
        or value.get("schema_version")
        != "pnc_rca_fresh_target_input_revalidation_v1"
        or value.get("project_key") != TARGET_PROJECT_KEY
        or value.get("work_item_id") != TARGET_WORK_ITEM_ID
        or value.get("source") != "official_meegle_api"
        or value.get("status") != "fields_extracted"
        or _sha256(value.get("context_sha256"), field="input_context_sha256")
        != value.get("context_sha256")
        or not isinstance(value.get("context_utf8_bytes"), int)
        or isinstance(value.get("context_utf8_bytes"), bool)
        or value.get("context_utf8_bytes", 0) < 1
        or value.get("fields")
        != {
            key: {"sha256": EMPTY_SHA256, "utf8_bytes": 0}
            for key in TARGET_FIELD_KEYS
        }
        or value.get("validation_blocker_kind") != ""
        or value.get("error_classes") != []
        or value.get("sidecar_capture_disabled") is not True
        or value.get("raw_values_persisted") is not False
        or value.get("context_module_sha256")
        != observed["required_file_sha256"]["gateway/pnc_issue_context.py"]
        or value.get("dispatcher_module_sha256")
        != observed["required_file_sha256"]
        ["scripts/pnc_rca_delivery_dispatcher.py"]
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_target_input_revalidation_invalid"
        )
    return {
        **value,
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "observer_script_sha256": hashlib.sha256(
            _CANONICAL_MEEGLE_INPUT_GATE.encode("utf-8")
        ).hexdigest(),
        "interpreter_sha256": hashlib.sha256(
            interpreter_binary.read_bytes()
        ).hexdigest(),
        "host_commit": observed["commit"],
        "host_tree": observed["tree"],
    }


def _validate_target_input_revalidation_pair(
    recorded: Any,
    live: Any,
    *,
    initial_input: Mapping[str, Any],
    initial_fields: Mapping[str, Any],
    writers_stopped_at: datetime,
    receipt_observed_at: datetime,
    now: datetime,
) -> Mapping[str, Any]:
    if (
        not isinstance(recorded, Mapping)
        or not isinstance(live, Mapping)
        or "observed_at" not in recorded
        or "observed_at" not in live
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_target_input_revalidation_invalid"
        )
    recorded_at = _timestamp(
        recorded["observed_at"], field="target_input_recorded_at"
    )
    live_at = _timestamp(live["observed_at"], field="target_input_live_at")
    recorded_identity = {
        key: value for key, value in recorded.items() if key != "observed_at"
    }
    live_identity = {
        key: value for key, value in live.items() if key != "observed_at"
    }
    if (
        recorded_identity != live_identity
        or recorded_at < writers_stopped_at
        or recorded_at > receipt_observed_at
        or live_at < recorded_at
        or live_at > now + MAX_FUTURE_SKEW
        or now - live_at > MAX_CLOSEOUT_AGE
        or recorded.get("context_sha256") != initial_input.get("context_sha256")
        or recorded.get("context_utf8_bytes")
        != initial_input.get("context_utf8_bytes")
        or recorded.get("fields")
        != {
            key: {"sha256": EMPTY_SHA256, "utf8_bytes": 0}
            for key in initial_fields.get("empty_field_keys", [])
        }
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_target_input_revalidation_mismatch"
        )
    return {
        **dict(recorded),
        "observed_at": recorded_at.isoformat(),
        "live_observed_at": live_at.isoformat(),
    }


_CANONICAL_MEEGLE_READBACK = r'''import hashlib,json,pathlib,sys
root=pathlib.Path(sys.argv[1])
sys.path.insert(0,str(root))
from scripts import pnc_rca_delivery_dispatcher as dispatcher
from gateway import pnc_rca_delivery_contract as delivery
expected=root/'scripts/pnc_rca_delivery_dispatcher.py'
expected_delivery=root/'gateway/pnc_rca_delivery_contract.py'
if pathlib.Path(dispatcher.__file__).resolve()!=expected or pathlib.Path(delivery.__file__).resolve()!=expected_delivery:
    raise SystemExit('module_origin_mismatch')
adapter=dispatcher.MeegleIssueCommentAdapter()
fields=adapter.get_fields(sys.argv[2],sys.argv[3],('field_9193cb','field_8c912e'))
comments=adapter.list_comments(sys.argv[2],sys.argv[3])
if fields.get('success') is not True or comments.get('success') is not True:
    raise SystemExit('meegle_read_failed')
field_values=fields.get('fields')
rows=comments.get('comments')
if not isinstance(field_values,dict) or set(field_values)!=set(('field_9193cb','field_8c912e')) or not isinstance(rows,list):
    raise SystemExit('meegle_read_invalid')
matches=[item for item in rows if isinstance(item,dict) and item.get('remote_id')==sys.argv[4]]
marker=delivery.delivery_effect_marker(sys.argv[5],sys.argv[6])
marker_matches=dispatcher._marker_matches(rows,marker)
if len(matches)!=1 or len(marker_matches)!=1 or marker_matches[0].get('remote_id')!=sys.argv[4]:
    raise SystemExit('meegle_comment_identity_mismatch')
def digest(value):
    raw=str(value).encode('utf-8')
    return {'sha256':hashlib.sha256(raw).hexdigest(),'utf8_bytes':len(raw)}
result={
 'schema_version':'pnc_rca_canonical_meegle_readback_v1',
 'project_key':sys.argv[2],'work_item_id':sys.argv[3],
 'fields':{key:digest(value) for key,value in field_values.items()},
 'comment_id':sys.argv[4],'comment_content':digest(matches[0]['content']),
 'comment_match_count':len(matches),'marker_sha256':hashlib.sha256(marker.encode()).hexdigest(),
 'marker_match_count':len(marker_matches),'pages_read':comments.get('pages_read'),
 'adapter_module_sha256':hashlib.sha256(expected.read_bytes()).hexdigest(),
 'delivery_module_sha256':hashlib.sha256(expected_delivery.read_bytes()).hexdigest(),
 'raw_values_persisted':False,
}
print(json.dumps(result,sort_keys=True,separators=(',',':')))
'''


def _observe_official_meegle_readback_live(
    *,
    project_key: str,
    work_item_id: str,
    comment_id: str,
    effect_key: str,
    artifact_set_id: str,
    host_commit: str,
    host_tree: str,
) -> Mapping[str, Any]:
    observed = _observe_canonical_host_binding(
        expected_commit=host_commit, expected_tree=host_tree
    )
    root = Path(CANONICAL_HOST_ROOT)
    interpreter, interpreter_binary = _canonical_host_interpreter_paths()
    try:
        completed = subprocess.run(
            [
                str(interpreter),
                "-I",
                "-B",
                "-c",
                _CANONICAL_MEEGLE_READBACK,
                str(root),
                project_key,
                work_item_id,
                comment_id,
                effect_key,
                artifact_set_id,
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=120,
            cwd=str(root),
            env={**os.environ, "MEEGLE_HOST": "project.feishu.cn"},
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_official_readback_live_failed"
        ) from exc
    if completed.returncode != 0 or completed.stderr or not completed.stdout:
        raise ProdE2EReleaseError(
            "prod_e2e_release_official_readback_live_failed"
        )
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_official_readback_live_invalid"
        ) from exc
    module_sha256 = observed["required_file_sha256"].get(
        "scripts/pnc_rca_delivery_dispatcher.py"
    )
    delivery_module_sha256 = observed["required_file_sha256"].get(
        "gateway/pnc_rca_delivery_contract.py"
    )
    if (
        not isinstance(value, dict)
        or set(value)
        != {
            "schema_version",
            "project_key",
            "work_item_id",
            "fields",
            "comment_id",
            "comment_content",
            "comment_match_count",
            "marker_sha256",
            "marker_match_count",
            "pages_read",
            "adapter_module_sha256",
            "delivery_module_sha256",
            "raw_values_persisted",
        }
        or value.get("schema_version")
        != "pnc_rca_canonical_meegle_readback_v1"
        or value.get("project_key") != project_key
        or value.get("work_item_id") != work_item_id
        or value.get("comment_id") != comment_id
        or value.get("comment_match_count") != 1
        or value.get("marker_match_count") != 1
        or _sha256(value.get("marker_sha256"), field="readback_marker_sha256")
        != value.get("marker_sha256")
        or value.get("raw_values_persisted") is not False
        or value.get("adapter_module_sha256") != module_sha256
        or value.get("delivery_module_sha256") != delivery_module_sha256
        or set(value.get("fields") or {}) != set(TARGET_FIELD_KEYS)
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_official_readback_live_invalid"
        )
    for descriptor in [
        *(value["fields"].values()),
        value.get("comment_content"),
    ]:
        if (
            not isinstance(descriptor, Mapping)
            or set(descriptor) != {"sha256", "utf8_bytes"}
            or _sha256(descriptor.get("sha256"), field="readback_value_sha256")
            != descriptor.get("sha256")
            or not isinstance(descriptor.get("utf8_bytes"), int)
            or isinstance(descriptor.get("utf8_bytes"), bool)
            or descriptor.get("utf8_bytes", -1) < 0
        ):
            raise ProdE2EReleaseError(
                "prod_e2e_release_official_readback_live_invalid"
            )
    return {
        **value,
        "observer_script_sha256": hashlib.sha256(
            _CANONICAL_MEEGLE_READBACK.encode("utf-8")
        ).hexdigest(),
        "interpreter_sha256": hashlib.sha256(
            interpreter_binary.read_bytes()
        ).hexdigest(),
        "host_commit": observed["commit"],
        "host_tree": observed["tree"],
    }


_CANONICAL_BASELINE_LIVE_READ = r'''import json,pathlib,sys
root=pathlib.Path(sys.argv[1])
sys.path.insert(0,str(root))
from gateway import pnc_rca_delivery_quarantine_baseline as baseline
expected=root/'gateway/pnc_rca_delivery_quarantine_baseline.py'
if pathlib.Path(baseline.__file__).resolve()!=expected:
    raise SystemExit('module_origin_mismatch')
status=baseline.read_quarantine_baseline_status(
 db_path=sys.argv[2],baseline_path=sys.argv[3],expected_sha256=sys.argv[4],
 expected_release_id=sys.argv[5],bootstrap_epoch_id=sys.argv[6],
 active_release_binding_path=sys.argv[7],live_env_path=sys.argv[8],
)
print(json.dumps(status,sort_keys=True,separators=(',',':')))
'''


def _observe_live_baseline(
    *,
    request: Mapping[str, Any],
    cutover: Mapping[str, Any],
    approval_sha256: str,
) -> Mapping[str, Any]:
    host = request["release_bom"]["host_runtime"]["canonical_final"]
    observed = _observe_canonical_host_binding(
        expected_commit=str(host["commit"]), expected_tree=str(host["tree"])
    )
    interpreter, _interpreter_binary = _canonical_host_interpreter_paths()
    baseline_path = str(Path(str(cutover.get("baseline_path") or "")).absolute())
    active_binding_path = str(
        Path(str(cutover.get("active_release_binding_path") or "")).absolute()
    )
    live_env_path = str(Path(str(cutover.get("live_env_path") or "")).absolute())
    if live_env_path != CANONICAL_HOST_ENV:
        raise ProdE2EReleaseError(
            "prod_e2e_release_live_baseline_path_invalid"
        )
    try:
        completed = subprocess.run(
            [
                str(interpreter),
                "-I",
                "-B",
                "-c",
                _CANONICAL_BASELINE_LIVE_READ,
                CANONICAL_HOST_ROOT,
                DELIVERY_DB_PATH,
                baseline_path,
                str(cutover.get("baseline_file_sha256") or ""),
                request["release_id"],
                request["release_bom"]["bootstrap_authorization"][
                    "bootstrap_epoch_id"
                ],
                active_binding_path,
                live_env_path,
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
            cwd=CANONICAL_HOST_ROOT,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_live_baseline_observation_failed"
        ) from exc
    if completed.returncode != 0 or completed.stderr or not completed.stdout:
        raise ProdE2EReleaseError(
            "prod_e2e_release_live_baseline_observation_failed"
        )
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_live_baseline_observation_invalid"
        ) from exc
    identity = value.get("baseline_identity") if isinstance(value, dict) else None
    db_cutover = request["release_bom"]["delivery_store_cutover"]
    if (
        not isinstance(value, dict)
        or value.get("ready") is not True
        or value.get("configured") is not True
        or not isinstance(identity, Mapping)
        or identity.get("baseline_sha256")
        != cutover.get("baseline_file_sha256")
        or identity.get("release_id") != request["release_id"]
        or identity.get("quarantine_core_sha256")
        != db_cutover["quarantine_core"]["core_sha256"]
        or identity.get("release_bom_sha256") != request["release_bom_sha256"]
        or identity.get("approval_evidence_sha256") != approval_sha256
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_live_baseline_observation_invalid"
        )
    return {
        "ready": True,
        "baseline_identity": dict(identity),
        "validator_script_sha256": hashlib.sha256(
            _CANONICAL_BASELINE_LIVE_READ.encode("utf-8")
        ).hexdigest(),
        "baseline_module_sha256": observed["required_file_sha256"][
            CANONICAL_BASELINE_MODULE
        ],
    }


def _one_row(
    conn: sqlite3.Connection,
    sql: str,
    parameters: Sequence[Any],
    *,
    artifact: str,
) -> Mapping[str, Any]:
    rows = conn.execute(sql, tuple(parameters)).fetchall()
    if len(rows) != 1:
        raise ProdE2EReleaseError(f"prod_e2e_release_{artifact}_row_invalid")
    return dict(rows[0])


def _stable_lineage_key(prefix: str, material: Mapping[str, Any]) -> str:
    return f"{prefix}-{_sha256_value(dict(material))}"


def _activation_source_identity_sha256(root: Mapping[str, Any]) -> str:
    return _sha256_value(
        {
            "event_uid": root["event_uid"],
            "offset": root["offset"],
            "partition": root["partition"],
            "topic": root["topic"],
        }
    )


def _validate_activation_receipt(
    value: Any,
    *,
    request: Mapping[str, Any],
    root: Mapping[str, Any],
    expected_entrypoint: str,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {
        "epoch_id",
        "admission_key",
        "entrypoint",
        "source_kind",
        "source_identity_sha256",
        "slot_kind",
        "decision",
        "reason",
    }:
        raise ProdE2EReleaseError(
            "prod_e2e_release_activation_receipt_invalid"
        )
    source_sha256 = _activation_source_identity_sha256(root)
    admission_key = _sha256_value(
        {
            "business_key": root["business_key"],
            "generation": 1,
            "source_identity_sha256": source_sha256,
            "source_kind": "kafka",
            "submission_key": root["submission_key"],
        }
    )
    allowed_reasons = {
        "activation_bounded_slot_consumed",
        "activation_steady_active",
        "activation_confirmed_shadow_reconciliation",
        "activation_admission_idempotent",
    }
    if expected_entrypoint == "shadow_promotion":
        allowed_reasons.discard("activation_steady_active")
    else:
        allowed_reasons.discard("activation_confirmed_shadow_reconciliation")
    if (
        value.get("epoch_id")
        != request["release_bom"]["bootstrap_authorization"][
            "bootstrap_epoch_id"
        ]
        or value.get("admission_key") != admission_key
        or value.get("entrypoint") != expected_entrypoint
        or value.get("source_kind") != "kafka"
        or value.get("source_identity_sha256") != source_sha256
        or value.get("slot_kind") not in {"", "kafka_success"}
        or value.get("decision") != "admit"
        or value.get("reason") not in allowed_reasons
        or (
            value.get("slot_kind") == "kafka_success"
            and value.get("reason")
            not in {
                "activation_bounded_slot_consumed",
                "activation_admission_idempotent",
            }
        )
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_activation_receipt_invalid"
        )
    return dict(value)


def _database_state(path: Path) -> Mapping[str, Any]:
    uri = path.expanduser().absolute().as_uri() + "?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True, timeout=5)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        conn.execute("BEGIN")
        if [str(row[0]) for row in conn.execute("PRAGMA integrity_check")] != ["ok"]:
            raise ProdE2EReleaseError(
                "prod_e2e_release_database_delta_integrity_invalid"
            )
        schema = [
            {
                "type": str(row["type"]),
                "name": str(row["name"]),
                "table": str(row["tbl_name"]),
                "sql": str(row["sql"] or ""),
            }
            for row in conn.execute(
                "SELECT type,name,tbl_name,sql FROM sqlite_master "
                "WHERE name NOT LIKE 'sqlite_%' OR name='sqlite_sequence' "
                "ORDER BY type,name"
            )
        ]
        tables: dict[str, Any] = {}
        for item in schema:
            if item["type"] != "table":
                continue
            table = item["name"]
            quoted = '"' + table.replace('"', '""') + '"'
            info = list(conn.execute(f"PRAGMA table_info({quoted})"))
            columns = [str(row["name"]) for row in info]
            primary = [
                str(row["name"])
                for row in sorted(info, key=lambda value: int(value["pk"]))
                if int(row["pk"]) > 0
            ]
            if not primary and table == "sqlite_sequence":
                primary = ["name"]
            if not columns or not primary:
                raise ProdE2EReleaseError(
                    "prod_e2e_release_database_delta_schema_invalid"
                )
            rows: dict[tuple[Any, ...], Mapping[str, Any]] = {}
            for row in conn.execute(f"SELECT * FROM {quoted}"):
                value = {column: row[column] for column in columns}
                key = tuple(value[column] for column in primary)
                if key in rows:
                    raise ProdE2EReleaseError(
                        "prod_e2e_release_database_delta_key_invalid"
                    )
                rows[key] = value
            tables[table] = {
                "columns": columns,
                "primary_key": primary,
                "rows": rows,
            }
        conn.rollback()
    except ProdE2EReleaseError:
        raise
    except (OSError, sqlite3.Error) as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_database_delta_observation_failed"
        ) from exc
    finally:
        if "conn" in locals():
            conn.close()
    return {"schema": schema, "tables": tables}


def _database_delta_key(value: tuple[Any, ...]) -> list[Any]:
    return [
        (
            {"bytes_sha256": hashlib.sha256(item).hexdigest(), "size_bytes": len(item)}
            if isinstance(item, bytes)
            else item
        )
        for item in value
    ]


def _validate_exact_added_lineage_graph(
    *,
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    changes: Mapping[str, Mapping[str, list[tuple[Any, ...]]]],
    target: Mapping[str, Any],
    canary: Mapping[str, Any],
    expected_host: Mapping[str, Any],
    expected_host_processes: Mapping[str, Any],
) -> None:
    exact_added_counts = {
        "kafka_inbox": 2,
        "business_triggers": 2,
        "rca_trigger_sources": 2,
        "rca_trigger_bindings": 2,
        "rca_outbox": 2,
        "rca_execution_watch": 2,
        "rca_delivery_jobs": 2,
        "rca_delivery_subscriptions": 2,
        "rca_trigger_delivery_bindings": 2,
        "rca_delivery_effects": 2,
        "rca_delivery_attempts": 4,
        "rca_shadow_promotion_audit": 1,
        "rca_host_runtime_transitions": 8,
        "rca_activation_admission_ledger": 2,
    }
    for table, expected_count in exact_added_counts.items():
        if table not in after["tables"]:
            raise ProdE2EReleaseError(
                "prod_e2e_release_live_database_lineage_table_missing"
            )
        delta = changes.get(table, {"added": [], "changed": []})
        if len(delta["added"]) != expected_count or delta["changed"]:
            raise ProdE2EReleaseError(
                "prod_e2e_release_live_database_lineage_cardinality_invalid"
            )

    def added_rows(table: str) -> list[Mapping[str, Any]]:
        rows = after["tables"][table]["rows"]
        return [rows[key] for key in changes[table]["added"]]

    def unique(
        table: str,
        *,
        field: str,
        value: Any,
    ) -> Mapping[str, Any]:
        matches = [row for row in added_rows(table) if row.get(field) == value]
        if len(matches) != 1:
            raise ProdE2EReleaseError(
                "prod_e2e_release_live_database_lineage_cardinality_invalid"
            )
        return matches[0]

    def require(row: Mapping[str, Any], expected: Mapping[str, Any]) -> None:
        if any(row.get(field) != value for field, value in expected.items()):
            raise ProdE2EReleaseError(
                "prod_e2e_release_live_database_exact_lineage_invalid"
            )

    root_contexts: list[dict[str, Any]] = []
    for role, root in (("target", target), ("canary", canary)):
        comment = root.get("comment_write")
        activation = root.get("activation")
        if not isinstance(comment, Mapping) or not isinstance(activation, Mapping):
            raise ProdE2EReleaseError(
                "prod_e2e_release_live_database_root_evidence_invalid"
            )
        inbox = unique("kafka_inbox", field="event_uid", value=root["event_uid"])
        require(
            inbox,
            {
                "event_uid": root["event_uid"],
                "topic": root["topic"],
                "partition_id": root["partition"],
                "offset_id": root["offset"],
                "raw_sha256": root["raw_sha256"],
                "decision": "accepted",
                "business_key": root["business_key"],
                "submission_key": root["submission_key"],
                "generation": 1,
            },
        )
        trigger = unique(
            "business_triggers",
            field="submission_key",
            value=root["submission_key"],
        )
        require(
            trigger,
            {
                "business_key": root["business_key"],
                "generation": 1,
                "submission_key": root["submission_key"],
                "project_key": root["project_key"],
                "work_item_type_key": root["work_item_type_key"],
                "work_item_id": root["work_item_id"],
                "state": "submitted",
                "source_event_id": root["event_uid"],
                "source_topic": root["topic"],
                "source_partition": root["partition"],
                "source_offset": root["offset"],
                "activation_epoch_id": activation["epoch_id"],
            },
        )
        source_id = _stable_lineage_key(
            "g1q3-rca-source-v1",
            {
                "source_kind": "kafka_workflow_event",
                "dedupe": root["event_uid"],
            },
        )
        source = unique("rca_trigger_sources", field="source_id", value=source_id)
        require(
            source,
            {
                "source_kind": "kafka_workflow_event",
                "source_dedupe_key": root["event_uid"],
                "payload_sha256": root["raw_sha256"],
                "kafka_event_uid": root["event_uid"],
                "mode": "issue_created",
            },
        )
        binding = unique(
            "rca_trigger_bindings", field="source_id", value=source_id
        )
        require(
            binding,
            {
                "business_key": root["business_key"],
                "generation": 1,
                "role": "observer",
            },
        )
        outbox = unique(
            "rca_outbox", field="submission_key", value=root["submission_key"]
        )
        require(
            outbox,
            {
                "business_key": root["business_key"],
                "generation": 1,
                "status": "completed",
                "source_event_id": root["event_uid"],
                "source_topic": root["topic"],
                "source_partition": root["partition"],
                "source_offset": root["offset"],
                "origin_source_id": source_id,
                "activation_epoch_id": activation["epoch_id"],
            },
        )
        ledger = unique(
            "rca_activation_admission_ledger",
            field="admission_key",
            value=activation["admission_key"],
        )
        require(
            ledger,
            {
                "epoch_id": activation["epoch_id"],
                "entrypoint": activation["entrypoint"],
                "source_kind": activation["source_kind"],
                "source_identity_sha256": activation["source_identity_sha256"],
                "slot_kind": activation["slot_kind"] or None,
                "decision": activation["decision"],
                "reason": activation["reason"],
                "business_key": root["business_key"],
                "submission_key": root["submission_key"],
                "generation": 1,
            },
        )
        if (
            trigger.get("activation_ledger_id") != ledger.get("ledger_id")
            or outbox.get("activation_ledger_id") != ledger.get("ledger_id")
            or not ledger.get("admitted_at")
            or not ledger.get("bound_at")
        ):
            raise ProdE2EReleaseError(
                "prod_e2e_release_live_database_activation_lineage_invalid"
            )
        watch = unique(
            "rca_execution_watch",
            field="submission_key",
            value=root["submission_key"],
        )
        require(
            watch,
            {
                "business_key": root["business_key"],
                "generation": 1,
                "project_key": root["project_key"],
                "work_item_type_key": root["work_item_type_key"],
                "work_item_id": root["work_item_id"],
                "task_id": root["submission_key"],
                "state": "delivery_created",
                "delivery_id": root["delivery_id"],
            },
        )
        job = unique(
            "rca_delivery_jobs", field="delivery_id", value=root["delivery_id"]
        )
        require(
            job,
            {
                "submission_key": root["submission_key"],
                "business_key": root["business_key"],
                "generation": 1,
                "artifact_set_id": root["artifact_set_id"],
                "project_key": root["project_key"],
                "work_item_type_key": root["work_item_type_key"],
                "work_item_id": root["work_item_id"],
                "target_key": root["target_key"],
                "issue_url": root["issue_url"],
                "outcome": "success",
                "status": "delivered",
            },
        )
        subscription_key = _stable_lineage_key(
            "g1q3-rca-sub-v1",
            {
                "business_key": root["business_key"],
                "generation": 1,
                "effect_kind": "feishu_issue_comment",
                "target_key": root["target_key"],
            },
        )
        subscription = unique(
            "rca_delivery_subscriptions",
            field="subscription_key",
            value=subscription_key,
        )
        require(
            subscription,
            {
                "business_key": root["business_key"],
                "generation": 1,
                "source_id": None,
                "effect_kind": "feishu_issue_comment",
                "target_key": root["target_key"],
                "required": 1,
                "status": "materialized",
                "delivery_id": root["delivery_id"],
                "effect_key": root["effect_key"],
            },
        )
        delivery_binding = [
            row
            for row in added_rows("rca_trigger_delivery_bindings")
            if row.get("source_id") == source_id
            and row.get("subscription_key") == subscription_key
        ]
        if len(delivery_binding) != 1:
            raise ProdE2EReleaseError(
                "prod_e2e_release_live_database_delivery_binding_invalid"
            )
        effect = unique(
            "rca_delivery_effects", field="effect_key", value=root["effect_key"]
        )
        require(
            effect,
            {
                "delivery_id": root["delivery_id"],
                "effect_kind": "feishu_issue_comment",
                "required": 1,
                "target_key": root["target_key"],
                "payload_sha256": root["semantic_payload_sha256"],
                "outcome": "success",
                "write_phase": "settled",
                "status": "succeeded",
            },
        )
        try:
            remote_receipt = json.loads(str(effect.get("remote_receipt_json") or ""))
        except json.JSONDecodeError as exc:
            raise ProdE2EReleaseError(
                "prod_e2e_release_live_database_effect_receipt_invalid"
            ) from exc
        marker = (
            f"[RCA_DELIVERY:{root['effect_key']}:"
            f"{str(root['artifact_set_id'])[-12:]}]"
        )
        expected_receipt_keys = {
            "remote_id",
            "marker",
            "source",
            "confirmed_field_keys",
        }
        if comment["attempt_terminal_outcome"] == "ack":
            expected_receipt_keys.add("recovery_write_count")
        if (
            not isinstance(remote_receipt, Mapping)
            or set(remote_receipt) != expected_receipt_keys
            or remote_receipt.get("remote_id") != comment["comment_id"]
            or remote_receipt.get("marker") != marker
            or remote_receipt.get("confirmed_field_keys")
            != list(TARGET_FIELD_KEYS)
            or (
                comment["attempt_terminal_outcome"] == "ack"
                and (
                    remote_receipt.get("source")
                    not in {"read_after_write", "read_after_recovery_write"}
                    or not isinstance(remote_receipt.get("recovery_write_count"), int)
                    or isinstance(remote_receipt.get("recovery_write_count"), bool)
                    or remote_receipt.get("recovery_write_count", -1) < 0
                )
            )
            or (
                comment["attempt_terminal_outcome"] == "reconciled"
                and not _required_text(
                    remote_receipt.get("source"), field="effect_receipt_source"
                )
            )
            or (
                role == "canary"
                and (
                    comment["attempt_terminal_outcome"] != "ack"
                    or remote_receipt.get("source") != "read_after_write"
                    or remote_receipt.get("recovery_write_count") != 0
                    or root.get("recovery_write_count") != 0
                    or root.get("operator_recovery_provenance") != []
                    or root.get("delivery_source") != "ordinary_kafka_ingest"
                )
            )
        ):
            raise ProdE2EReleaseError(
                "prod_e2e_release_live_database_effect_receipt_invalid"
            )
        attempts = sorted(
            (
                row
                for row in added_rows("rca_delivery_attempts")
                if row.get("effect_key") == root["effect_key"]
            ),
            key=lambda row: int(row.get("event_seq") or 0),
        )
        if len(attempts) != 2:
            raise ProdE2EReleaseError(
                "prod_e2e_release_live_database_attempt_lineage_invalid"
            )
        started, terminal = attempts
        if (
            started.get("attempt_no") != 1
            or started.get("event_seq") != 1
            or started.get("outcome") != "started"
            or started.get("remote_id") != ""
            or started.get("error_code") != ""
            or started.get("detail") != ""
            or started.get("finished_at") is not None
            or terminal.get("attempt_no") != 1
            or terminal.get("event_seq") != 2
            or terminal.get("outcome") != comment["attempt_terminal_outcome"]
            or terminal.get("remote_id") != comment["comment_id"]
            or not terminal.get("finished_at")
            or terminal.get("fence") != started.get("fence")
            or terminal.get("request_id") != started.get("request_id")
            or terminal.get("error_code") != ""
            or terminal.get("detail") != ""
        ):
            raise ProdE2EReleaseError(
                "prod_e2e_release_live_database_attempt_lineage_invalid"
            )

        expected_transitions = {
            (
                "local.pnc.rca-kafka-consumer",
                "kafka_ingested",
                str(root["event_uid"]),
            ),
            (
                "local.pnc.rca-outbox-dispatcher",
                "outbox_completed",
                str(outbox["outbox_id"]),
            ),
            (
                "local.pnc.rca-delivery-collector",
                "delivery_created",
                str(root["delivery_id"]),
            ),
            (
                "local.pnc.rca-delivery-dispatcher",
                "effect_succeeded",
                str(root["effect_key"]),
            ),
        }
        transitions = [
            row
            for row in added_rows("rca_host_runtime_transitions")
            if row.get("submission_key") == root["submission_key"]
        ]
        observed_transitions = {
            (
                str(row.get("service_label")),
                str(row.get("transition_kind")),
                str(row.get("entity_key")),
            )
            for row in transitions
        }
        if len(transitions) != 4 or observed_transitions != expected_transitions:
            raise ProdE2EReleaseError(
                "prod_e2e_release_live_database_runtime_transition_invalid"
            )
        for transition in transitions:
            require(
                transition,
                {
                    "business_key": root["business_key"],
                    "generation": 1,
                },
            )
            try:
                runtime_identity_json = str(
                    transition.get("runtime_identity_json") or ""
                )
                runtime_identity = json.loads(runtime_identity_json)
            except json.JSONDecodeError as exc:
                raise ProdE2EReleaseError(
                    "prod_e2e_release_live_database_runtime_transition_invalid"
                ) from exc
            service_runtime_sha256 = expected_host.get(
                "service_runtime_files_sha256", {}
            ).get(
                transition.get("service_label"),
                expected_host.get("runtime_files_sha256"),
            )
            service_label = str(transition.get("service_label"))
            entrypoint = HOST_SERVICE_ENTRYPOINTS.get(service_label)
            expected_script = (
                f"{HOST_LIVE_ROOT}/{entrypoint}" if entrypoint else ""
            )
            expected_script_sha256 = expected_host.get(
                "runtime_file_sha256", {}
            ).get(entrypoint)
            expected_process = expected_host_processes.get(service_label)
            if (
                not isinstance(runtime_identity, Mapping)
                or set(runtime_identity)
                != {
                    "service_label",
                    "pid",
                    "process_create_time",
                    "boot_time",
                    "executable",
                    "script",
                    "cwd",
                    "script_sha256",
                    "runtime_files_sha256",
                    "public_config_sha256",
                    "loaded_runtime_sha256",
                }
                or runtime_identity.get("service_label")
                != service_label
                or not isinstance(expected_process, Mapping)
                or runtime_identity.get("pid") != expected_process.get("new_pid")
                or not isinstance(runtime_identity.get("boot_time"), (int, float))
                or isinstance(runtime_identity.get("boot_time"), bool)
                or float(runtime_identity["boot_time"]) <= 0
                or runtime_identity.get("cwd") != HOST_LIVE_ROOT
                or runtime_identity.get("executable")
                != f"{HOST_LIVE_ROOT}/.venv/bin/python"
                or runtime_identity.get("script") != expected_script
                or runtime_identity.get("script_sha256")
                != expected_script_sha256
                or runtime_identity.get("runtime_files_sha256")
                != service_runtime_sha256
                or _sha256(
                    runtime_identity.get("public_config_sha256"),
                    field="transition_public_config_sha256",
                )
                != runtime_identity.get("public_config_sha256")
                or _sha256(
                    runtime_identity.get("loaded_runtime_sha256"),
                    field="transition_loaded_runtime_sha256",
                )
                != runtime_identity.get("loaded_runtime_sha256")
                or not isinstance(runtime_identity.get("process_create_time"), (int, float))
                or isinstance(runtime_identity.get("process_create_time"), bool)
                or float(runtime_identity["process_create_time"])
                < float(runtime_identity["boot_time"])
                or float(runtime_identity["process_create_time"])
                > _timestamp(
                    transition.get("transitioned_at"),
                    field="runtime_transitioned_at",
                ).timestamp()
                or transition.get("runtime_identity_sha256")
                != _sha256_value(runtime_identity)
                or runtime_identity_json
                != _canonical_bytes(runtime_identity).decode("utf-8")
            ):
                raise ProdE2EReleaseError(
                    "prod_e2e_release_live_database_runtime_transition_invalid"
                )
        root_contexts.append(
            {
                "role": role,
                "root": root,
                "source_id": source_id,
                "outbox_id": outbox["outbox_id"],
                "subscription_key": subscription_key,
                "ledger_id": ledger["ledger_id"],
                "slot_kind": activation["slot_kind"],
            }
        )

    promotion_rows = added_rows("rca_shadow_promotion_audit")
    target_context = root_contexts[0]
    require(
        promotion_rows[0],
        {
            "event_uid": target["event_uid"],
            "outbox_id": target_context["outbox_id"],
            "submission_key": target["submission_key"],
            "outcome": "promoted",
            "from_status": "shadow",
            "to_status": "pending",
        },
    )

    slot_contexts = [
        context for context in root_contexts if context["slot_kind"]
    ]
    expected_slot_bindings = {
        (str(context["root"]["activation"]["epoch_id"]), str(context["slot_kind"])): context
        for context in slot_contexts
    }
    slot_delta = changes.get(
        "rca_activation_budget_slots", {"added": [], "changed": []}
    )
    if (
        len(expected_slot_bindings) != len(slot_contexts)
        or slot_delta["added"]
        or len(slot_delta["changed"]) != len(expected_slot_bindings)
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_live_database_activation_slot_delta_invalid"
        )
    for key in slot_delta["changed"]:
        old_row = before["tables"]["rca_activation_budget_slots"]["rows"][key]
        new_row = after["tables"]["rca_activation_budget_slots"]["rows"][key]
        binding = expected_slot_bindings.get(
            (str(new_row.get("epoch_id")), str(new_row.get("slot_kind")))
        )
        if (
            binding is None
            or new_row.get("authorized_source_kind") != "kafka"
            or new_row.get("authorized_identity_sha256")
            != binding["root"]["activation"]["source_identity_sha256"]
            or new_row.get("consumed_ledger_id") != binding["ledger_id"]
            or not new_row.get("consumed_at")
            or {
                name: value
                for name, value in old_row.items()
                if name not in {"consumed_ledger_id", "consumed_at"}
            }
            != {
                name: value
                for name, value in new_row.items()
                if name not in {"consumed_ledger_id", "consumed_at"}
            }
        ):
            raise ProdE2EReleaseError(
                "prod_e2e_release_live_database_activation_slot_delta_invalid"
            )


def _validate_live_database_allowed_delta(
    *,
    cutover_snapshot_path: Path,
    target: Mapping[str, Any],
    canary: Mapping[str, Any],
    expected_host: Mapping[str, Any],
    expected_host_processes: Mapping[str, Any],
) -> Mapping[str, Any]:
    before = _database_state(cutover_snapshot_path)
    after = _database_state(Path(DELIVERY_DB_PATH))
    if before["schema"] != after["schema"] or set(before["tables"]) != set(
        after["tables"]
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_live_database_schema_delta_invalid"
        )

    changes: dict[str, dict[str, list[tuple[Any, ...]]]] = {}
    for table in sorted(before["tables"]):
        old_table = before["tables"][table]
        new_table = after["tables"][table]
        if (
            old_table["columns"] != new_table["columns"]
            or old_table["primary_key"] != new_table["primary_key"]
        ):
            raise ProdE2EReleaseError(
                "prod_e2e_release_live_database_schema_delta_invalid"
            )
        old_rows = old_table["rows"]
        new_rows = new_table["rows"]
        added = sorted(new_rows.keys() - old_rows.keys(), key=repr)
        deleted = sorted(old_rows.keys() - new_rows.keys(), key=repr)
        changed = sorted(
            (
                key
                for key in old_rows.keys() & new_rows.keys()
                if old_rows[key] != new_rows[key]
            ),
            key=repr,
        )
        if deleted:
            raise ProdE2EReleaseError(
                "prod_e2e_release_live_database_deleted_rows_forbidden"
            )
        if added or changed:
            changes[table] = {"added": added, "changed": changed}

    _validate_exact_added_lineage_graph(
        before=before,
        after=after,
        changes=changes,
        target=target,
        canary=canary,
        expected_host=expected_host,
        expected_host_processes=expected_host_processes,
    )

    event_uids = {TARGET_EVENT_UID, str(canary["event_uid"])}
    submission_keys = {TARGET_SUBMISSION_KEY, str(canary["submission_key"])}
    business_keys = {TARGET_BUSINESS_KEY, str(canary["business_key"])}
    work_item_ids = {TARGET_WORK_ITEM_ID, str(canary["work_item_id"])}

    def rows(table: str) -> list[Mapping[str, Any]]:
        descriptor = after["tables"].get(table)
        return list(descriptor["rows"].values()) if descriptor else []

    if {
        str(row.get("event_uid"))
        for row in rows("kafka_inbox")
        if str(row.get("event_uid")) in event_uids
    } != event_uids:
        raise ProdE2EReleaseError(
            "prod_e2e_release_live_database_expected_events_missing"
        )
    inbox_delta = changes.get("kafka_inbox", {"added": [], "changed": []})
    added_inbox = {
        str(after["tables"]["kafka_inbox"]["rows"][key]["event_uid"])
        for key in inbox_delta["added"]
    }
    if added_inbox != event_uids or inbox_delta["changed"]:
        raise ProdE2EReleaseError(
            "prod_e2e_release_live_database_event_delta_invalid"
        )

    trigger_rows = [
        row
        for row in rows("business_triggers")
        if row.get("submission_key") in submission_keys
    ]
    if (
        {str(row["submission_key"]) for row in trigger_rows} != submission_keys
        or {str(row["business_key"]) for row in trigger_rows} != business_keys
        or any(row.get("generation") != 1 for row in trigger_rows)
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_live_database_trigger_delta_invalid"
        )
    source_ids = {
        str(row["source_id"])
        for row in rows("rca_trigger_sources")
        if row.get("kafka_event_uid") in event_uids
    }
    outbox_ids = {
        row["outbox_id"]
        for row in rows("rca_outbox")
        if row.get("submission_key") in submission_keys
    }
    delivery_ids = {
        str(row["delivery_id"])
        for row in rows("rca_delivery_jobs")
        if row.get("submission_key") in submission_keys
    }
    effect_keys = {
        str(row["effect_key"])
        for row in rows("rca_delivery_effects")
        if row.get("delivery_id") in delivery_ids
    }
    subscription_keys = {
        str(row["subscription_key"])
        for row in rows("rca_delivery_subscriptions")
        if row.get("business_key") in business_keys
    }
    ledger_ids = {
        row["ledger_id"]
        for row in rows("rca_activation_admission_ledger")
        if row.get("submission_key") in submission_keys
    }
    if len(source_ids) != 2 or len(outbox_ids) != 2 or len(delivery_ids) != 2:
        raise ProdE2EReleaseError(
            "prod_e2e_release_live_database_lineage_cardinality_invalid"
        )

    predicates: dict[str, Callable[[Mapping[str, Any]], bool]] = {
        "business_triggers": lambda row: row.get("submission_key") in submission_keys
        and row.get("business_key") in business_keys
        and row.get("generation") == 1,
        "rca_trigger_sources": lambda row: row.get("source_id") in source_ids
        and row.get("kafka_event_uid") in event_uids,
        "rca_trigger_bindings": lambda row: row.get("source_id") in source_ids
        and row.get("business_key") in business_keys
        and row.get("generation") == 1,
        "rca_outbox": lambda row: row.get("outbox_id") in outbox_ids
        and row.get("submission_key") in submission_keys
        and row.get("business_key") in business_keys
        and row.get("generation") == 1,
        "rca_execution_watch": lambda row: row.get("submission_key") in submission_keys
        and row.get("business_key") in business_keys
        and row.get("generation") == 1,
        "rca_delivery_jobs": lambda row: row.get("delivery_id") in delivery_ids
        and row.get("submission_key") in submission_keys
        and row.get("business_key") in business_keys
        and row.get("work_item_id") in work_item_ids
        and row.get("generation") == 1,
        "rca_delivery_effects": lambda row: row.get("effect_key") in effect_keys
        and row.get("delivery_id") in delivery_ids,
        "rca_delivery_attempts": lambda row: row.get("effect_key") in effect_keys,
        "rca_delivery_subscriptions": lambda row: row.get("subscription_key")
        in subscription_keys
        and row.get("business_key") in business_keys
        and row.get("generation") == 1,
        "rca_trigger_delivery_bindings": lambda row: row.get("source_id") in source_ids
        and row.get("subscription_key") in subscription_keys,
        "rca_host_runtime_transitions": lambda row: row.get("submission_key")
        in submission_keys
        and row.get("business_key") in business_keys
        and row.get("generation") == 1,
        "rca_activation_admission_ledger": lambda row: row.get("ledger_id") in ledger_ids
        and row.get("submission_key") in submission_keys
        and row.get("business_key") in business_keys
        and row.get("generation") == 1,
        "rca_shadow_promotion_audit": lambda row: row.get("event_uid")
        == TARGET_EVENT_UID
        and row.get("submission_key") == TARGET_SUBMISSION_KEY,
    }
    for table, predicate in predicates.items():
        delta = changes.get(table, {"added": [], "changed": []})
        if delta["changed"]:
            raise ProdE2EReleaseError(
                "prod_e2e_release_live_database_existing_lineage_changed"
            )
        table_rows = after["tables"].get(table, {}).get("rows", {})
        if any(not predicate(table_rows[key]) for key in delta["added"]):
            raise ProdE2EReleaseError(
                "prod_e2e_release_live_database_unrelated_lineage_invalid"
            )
    shadow_delta = changes.get(
        "rca_shadow_promotion_audit", {"added": [], "changed": []}
    )
    if len(shadow_delta["added"]) != 1 or shadow_delta["changed"]:
        raise ProdE2EReleaseError(
            "prod_e2e_release_live_database_target_promotion_invalid"
        )

    progress = changes.get("kafka_partition_progress")
    if progress is None or progress["added"] or len(progress["changed"]) != 1:
        raise ProdE2EReleaseError(
            "prod_e2e_release_live_database_partition_progress_invalid"
        )
    progress_key = progress["changed"][0]
    old_progress = before["tables"]["kafka_partition_progress"]["rows"][progress_key]
    new_progress = after["tables"]["kafka_partition_progress"]["rows"][progress_key]
    if (
        progress_key != (TOPIC, PARTITION)
        or new_progress.get("last_event_uid") != canary["event_uid"]
        or new_progress.get("durable_next_offset") != canary["offset"] + 1
        or {
            key: value
            for key, value in old_progress.items()
            if key not in {"durable_next_offset", "last_event_uid", "updated_at"}
        }
        != {
            key: value
            for key, value in new_progress.items()
            if key not in {"durable_next_offset", "last_event_uid", "updated_at"}
        }
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_live_database_partition_progress_invalid"
        )

    for table in ("rca_dispatcher_circuit", "rca_delivery_dispatcher_circuit"):
        delta = changes.get(table, {"added": [], "changed": []})
        if delta["added"]:
            raise ProdE2EReleaseError(
                "prod_e2e_release_live_database_circuit_delta_invalid"
            )
        for key in delta["changed"]:
            old_row = before["tables"][table]["rows"][key]
            new_row = after["tables"][table]["rows"][key]
            if (
                new_row.get("state") != "closed"
                or new_row.get("reason_code") != ""
                or new_row.get("reason_detail") != ""
                or new_row.get("opened_at") is not None
                or {name: value for name, value in old_row.items() if name != "updated_at"}
                != {name: value for name, value in new_row.items() if name != "updated_at"}
            ):
                raise ProdE2EReleaseError(
                    "prod_e2e_release_live_database_circuit_delta_invalid"
                )

    slot_delta = changes.get(
        "rca_activation_budget_slots", {"added": [], "changed": []}
    )
    if slot_delta["added"]:
        raise ProdE2EReleaseError(
            "prod_e2e_release_live_database_activation_slot_delta_invalid"
        )
    for key in slot_delta["changed"]:
        old_row = before["tables"]["rca_activation_budget_slots"]["rows"][key]
        new_row = after["tables"]["rca_activation_budget_slots"]["rows"][key]
        if (
            new_row.get("consumed_ledger_id") not in ledger_ids
            or {
                name: value
                for name, value in old_row.items()
                if name not in {"consumed_ledger_id", "consumed_at"}
            }
            != {
                name: value
                for name, value in new_row.items()
                if name not in {"consumed_ledger_id", "consumed_at"}
            }
        ):
            raise ProdE2EReleaseError(
                "prod_e2e_release_live_database_activation_slot_delta_invalid"
            )

    sequence_delta = changes.get("sqlite_sequence", {"added": [], "changed": []})
    added_count_by_table = {
        table: len(delta["added"])
        for table, delta in changes.items()
        if table != "sqlite_sequence" and delta["added"]
    }
    for key in [*sequence_delta["added"], *sequence_delta["changed"]]:
        new_row = after["tables"]["sqlite_sequence"]["rows"][key]
        old_row = before["tables"]["sqlite_sequence"]["rows"].get(
            key, {"seq": 0}
        )
        name = str(new_row.get("name") or "")
        if (
            name not in added_count_by_table
            or int(new_row.get("seq") or 0) - int(old_row.get("seq") or 0)
            != added_count_by_table[name]
        ):
            raise ProdE2EReleaseError(
                "prod_e2e_release_live_database_sequence_delta_invalid"
            )

    allowed_tables = {
        "kafka_inbox",
        "kafka_partition_progress",
        "sqlite_sequence",
        "rca_dispatcher_circuit",
        "rca_delivery_dispatcher_circuit",
        "rca_activation_budget_slots",
        *predicates.keys(),
    }
    unexpected = sorted(set(changes) - allowed_tables)
    if unexpected:
        raise ProdE2EReleaseError(
            "prod_e2e_release_live_database_unrelated_delta_invalid"
        )
    summary = {
        table: {
            kind: [_database_delta_key(key) for key in keys]
            for kind, keys in delta.items()
        }
        for table, delta in changes.items()
    }
    return {
        "schema_version": "pnc_rca_live_database_allowed_delta_v1",
        "allowed_event_uids": sorted(event_uids),
        "allowed_submission_keys": sorted(submission_keys),
        "changed_tables": sorted(changes),
        "delta_sha256": _sha256_value(summary),
        "deleted_row_count": 0,
        "unrelated_delta_count": 0,
    }


def _observe_live_delivery_database(
    *,
    request: Mapping[str, Any],
    target: Mapping[str, Any],
    canary: Mapping[str, Any],
    comment_id: str,
    target_attempt_terminal_outcome: str,
    host_restart: Mapping[str, Any],
    cutover_snapshot_path: Path,
) -> Mapping[str, Any]:
    host = request["release_bom"]["host_runtime"]["canonical_final"]
    projection = _run_canonical_db_projection(
        db_path=Path(DELIVERY_DB_PATH),
        host_commit=str(host["commit"]),
        host_tree=str(host["tree"]),
        allow_live=True,
    )
    final_logical_sha256 = projection["projection"]["logical_sha256"]
    gap_owned = _read_owned_json(
        Path(request["release_bom"]["kafka_scope"]["gap_ledger"]["path"]),
        artifact="closeout_gap_ledger",
    )
    deferred = sorted(
        str(item["event_uid"])
        for item in gap_owned.body["missing_events"]
        if item.get("event_uid") != TARGET_EVENT_UID
    )
    if len(deferred) != DEFERRED_MISSING_COUNT:
        raise ProdE2EReleaseError(
            "prod_e2e_release_deferred_gap_identity_invalid"
        )
    uri = Path(DELIVERY_DB_PATH).absolute().as_uri() + "?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True, timeout=5)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        conn.execute("BEGIN")
        inbox = _one_row(
            conn,
            """
            SELECT event_uid,topic,partition_id,offset_id,raw_sha256,decision,
                   business_key,submission_key,generation,processed_at
              FROM kafka_inbox WHERE event_uid=?
            """,
            (TARGET_EVENT_UID,),
            artifact="target_inbox",
        )
        trigger = _one_row(
            conn,
            """
            SELECT business_key,submission_key,generation,project_key,
                   work_item_type_key,work_item_id,state,source_event_id,
                   source_topic,source_partition,source_offset
              FROM business_triggers WHERE submission_key=?
            """,
            (TARGET_SUBMISSION_KEY,),
            artifact="target_trigger",
        )
        outbox = _one_row(
            conn,
            """
            SELECT outbox_id,business_key,submission_key,generation,status,
                   source_event_id,source_topic,source_partition,source_offset,
                   completed_at,result_json
              FROM rca_outbox WHERE submission_key=?
            """,
            (TARGET_SUBMISSION_KEY,),
            artifact="target_outbox",
        )
        watch = _one_row(
            conn,
            """
            SELECT submission_key,business_key,generation,project_key,
                   work_item_type_key,work_item_id,task_id,state,terminal_at,
                   delivery_id
              FROM rca_execution_watch WHERE submission_key=?
            """,
            (TARGET_SUBMISSION_KEY,),
            artifact="target_watch",
        )
        job = _one_row(
            conn,
            """
            SELECT delivery_id,submission_key,business_key,generation,
                   artifact_set_id,project_key,work_item_type_key,work_item_id,
                   target_key,issue_url,outcome,status,updated_at
              FROM rca_delivery_jobs WHERE delivery_id=?
            """,
            (target["delivery_id"],),
            artifact="target_delivery_job",
        )
        effect = _one_row(
            conn,
            """
            SELECT effect_key,delivery_id,effect_kind,required,target_key,
                   payload_sha256,outcome,write_phase,status,remote_receipt_json,
                   completed_at
              FROM rca_delivery_effects WHERE effect_key=?
            """,
            (target["effect_key"],),
            artifact="target_delivery_effect",
        )
        canary_pipeline = _one_row(
            conn,
            """
            SELECT i.event_uid,i.topic,i.partition_id,i.offset_id,i.raw_sha256,
                   i.decision,i.business_key,i.submission_key,i.generation,
                   i.processed_at,t.project_key,t.work_item_type_key,
                   t.work_item_id,t.state AS trigger_state,o.status AS outbox_status,
                   w.state AS watch_state,j.status AS job_status,
                   j.outcome AS job_outcome,
                   SUM(CASE WHEN e.required=1 AND e.status='succeeded' THEN 1 ELSE 0 END)
                       AS succeeded_required_effects,
                   SUM(CASE WHEN e.required=1 THEN 1 ELSE 0 END)
                       AS required_effects
              FROM kafka_inbox AS i
              JOIN business_triggers AS t ON t.submission_key=i.submission_key
              JOIN rca_outbox AS o ON o.submission_key=i.submission_key
              JOIN rca_execution_watch AS w ON w.submission_key=i.submission_key
              JOIN rca_delivery_jobs AS j ON j.submission_key=i.submission_key
              JOIN rca_delivery_effects AS e ON e.delivery_id=j.delivery_id
             WHERE i.event_uid=?
             GROUP BY i.event_uid
            """,
            (canary["event_uid"],),
            artifact="canary_pipeline",
        )
        placeholders = ",".join("?" for _ in deferred)
        touched_deferred = sorted(
            str(row[0])
            for row in conn.execute(
                f"SELECT event_uid FROM kafka_inbox WHERE event_uid IN ({placeholders})",
                tuple(deferred),
            ).fetchall()
        )
        conn.rollback()
    except (sqlite3.Error, OSError) as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_live_database_observation_failed"
        ) from exc
    finally:
        if "conn" in locals():
            conn.close()
    try:
        remote_receipt = json.loads(str(effect.get("remote_receipt_json") or ""))
    except json.JSONDecodeError as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_target_effect_receipt_invalid"
        ) from exc
    expected_inbox = {
        "event_uid": TARGET_EVENT_UID,
        "topic": TOPIC,
        "partition_id": PARTITION,
        "offset_id": TARGET_OFFSET,
        "raw_sha256": TARGET_RAW_SHA256,
        "decision": "accepted",
        "business_key": TARGET_BUSINESS_KEY,
        "submission_key": TARGET_SUBMISSION_KEY,
        "generation": 1,
    }
    if (
        any(inbox.get(key) != value for key, value in expected_inbox.items())
        or not inbox.get("processed_at")
        or trigger.get("state") != "submitted"
        or trigger.get("source_event_id") != TARGET_EVENT_UID
        or trigger.get("project_key") != TARGET_PROJECT_KEY
        or trigger.get("work_item_id") != TARGET_WORK_ITEM_ID
        or outbox.get("status") != "completed"
        or outbox.get("source_event_id") != TARGET_EVENT_UID
        or not outbox.get("completed_at")
        or not outbox.get("result_json")
        or watch.get("state") != "delivery_created"
        or watch.get("task_id") != TARGET_SUBMISSION_KEY
        or watch.get("delivery_id") != target.get("delivery_id")
        or not watch.get("terminal_at")
        or any(
            job.get(field) != target.get(field)
            for field in (
                "delivery_id",
                "submission_key",
                "business_key",
                "generation",
                "artifact_set_id",
                "project_key",
                "work_item_type_key",
                "work_item_id",
                "target_key",
                "issue_url",
            )
        )
        or job.get("status") != "delivered"
        or job.get("outcome") != "success"
        or effect.get("effect_key") != target.get("effect_key")
        or effect.get("delivery_id") != target.get("delivery_id")
        or effect.get("effect_kind") != "feishu_issue_comment"
        or effect.get("required") != 1
        or effect.get("target_key") != target.get("target_key")
        or effect.get("payload_sha256")
        != target.get("semantic_payload_sha256")
        or effect.get("outcome") != "success"
        or effect.get("write_phase") != "settled"
        or effect.get("status") != "succeeded"
        or remote_receipt.get("remote_id") != comment_id
        or remote_receipt.get("confirmed_field_keys") != list(TARGET_FIELD_KEYS)
        or touched_deferred
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_live_target_lineage_invalid"
        )
    if (
        canary_pipeline.get("event_uid") != canary.get("event_uid")
        or canary_pipeline.get("topic") != TOPIC
        or canary_pipeline.get("partition_id") != PARTITION
        or canary_pipeline.get("offset_id") != canary.get("offset")
        or canary_pipeline.get("raw_sha256") != canary.get("raw_sha256")
        or canary_pipeline.get("decision") != "accepted"
        or canary_pipeline.get("generation") != 1
        or canary_pipeline.get("business_key") != canary.get("business_key")
        or canary_pipeline.get("project_key") != canary.get("project_key")
        or canary_pipeline.get("work_item_type_key")
        != canary.get("work_item_type_key")
        or canary_pipeline.get("work_item_id") != canary.get("work_item_id")
        or canary_pipeline.get("submission_key") != canary.get("submission_key")
        or canary_pipeline.get("trigger_state") != "submitted"
        or canary_pipeline.get("outbox_status") != "completed"
        or canary_pipeline.get("watch_state") != "delivery_created"
        or canary_pipeline.get("job_status") != "delivered"
        or canary_pipeline.get("job_outcome") != "success"
        or not isinstance(canary_pipeline.get("required_effects"), int)
        or canary_pipeline.get("required_effects", 0) < 1
        or canary_pipeline.get("succeeded_required_effects")
        != canary_pipeline.get("required_effects")
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_live_canary_lineage_invalid"
        )
    allowed_delta = _validate_live_database_allowed_delta(
        cutover_snapshot_path=cutover_snapshot_path,
        target={
            **dict(target),
            "comment_write": {
                "comment_id": comment_id,
                "attempt_terminal_outcome": target_attempt_terminal_outcome,
            },
        },
        canary=canary,
        expected_host=host,
        expected_host_processes=host_restart,
    )
    return {
        "cutover_logical_sha256": request["release_bom"][
            "delivery_store_cutover"
        ]["approved_post_migration_logical_sha256"],
        "final_live_logical_sha256": final_logical_sha256,
        "target_delivery_id": target["delivery_id"],
        "target_effect_key": target["effect_key"],
        "target_remote_id": comment_id,
        "canary_event_uid": canary["event_uid"],
        "deferred_event_count": len(deferred),
        "deferred_event_set_sha256": _sha256_value(deferred),
        "touched_deferred_event_uids": [],
        "allowed_delta": allowed_delta,
        "canonical_projection_validator_sha256": projection[
            "validator_script_sha256"
        ],
    }


def _required_text(value: Any, *, field: str, maximum: int = 512) -> str:
    text = str(value or "").strip()
    if not text or len(text) > maximum:
        raise ProdE2EReleaseError(f"prod_e2e_release_{field}_invalid")
    return text


def _validate_cutover_database_checkpoint(
    value: Any,
    *,
    request: Mapping[str, Any],
    installed_at: datetime,
    verified_at: datetime,
    preflight_backup_path: str,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {
        "path",
        "sha256",
        "size_bytes",
        "captured_at",
        "logical_sha256",
    }:
        raise ProdE2EReleaseError(
            "prod_e2e_release_cutover_database_checkpoint_invalid"
        )
    path, file_sha256, size_bytes = _read_owned_blob(
        Path(str(value.get("path") or "")), artifact="cutover_database_checkpoint"
    )
    captured_at = _timestamp(
        value.get("captured_at"), field="cutover_database_checkpoint_captured_at"
    )
    live_path = Path(DELIVERY_DB_PATH).absolute()
    if (
        path == live_path
        or str(path) == preflight_backup_path
        or file_sha256 != value.get("sha256")
        or size_bytes != value.get("size_bytes")
        or captured_at < installed_at
        or captured_at > verified_at
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_cutover_database_checkpoint_invalid"
        )
    host = request["release_bom"]["host_runtime"]["canonical_final"]
    projection = _run_canonical_db_projection(
        db_path=path,
        host_commit=str(host["commit"]),
        host_tree=str(host["tree"]),
    )
    expected = request["release_bom"]["delivery_store_cutover"][
        "approved_post_migration_logical_sha256"
    ]
    if (
        projection["projection"]["logical_sha256"] != expected
        or value.get("logical_sha256") != expected
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_cutover_database_checkpoint_digest_mismatch"
        )
    return {
        "path": str(path),
        "sha256": file_sha256,
        "size_bytes": size_bytes,
        "captured_at": captured_at.isoformat(),
        "logical_sha256": expected,
        "canonical_projection_validator_sha256": projection[
            "validator_script_sha256"
        ],
    }


def _authorized_scope(bom: Mapping[str, Any]) -> Mapping[str, Any]:
    return {
        "mode": "single_exact_event_plus_natural_post_cutover_canary",
        "topic": TOPIC,
        "partition": PARTITION,
        "offset": TARGET_OFFSET,
        "event_uid": TARGET_EVENT_UID,
        "raw_sha256": TARGET_RAW_SHA256,
        "work_item_id": TARGET_WORK_ITEM_ID,
        "deferred_missing_count": DEFERRED_MISSING_COUNT,
        "bulk_pre_t0_backfill_authorized": False,
        "host_commit": bom["host_runtime"]["canonical_final"]["commit"],
        "host_tree": bom["host_runtime"]["canonical_final"]["tree"],
        "pipeline_commit": PIPELINE_COMMIT,
        "pipeline_tree": PIPELINE_TREE,
        "workspace_runtime_sha256": bom["component_identities"]["workspace"][
            "runtime_sha256"
        ],
        "worker_commit": bom["component_identities"]["worker"]["commit"],
        "worker_tree": bom["component_identities"]["worker"]["tree"],
        "viewer_origin": bom["component_identities"]["viewer_proxy"]
        ["public_origin"],
        "viewer_proxy_config_sha256": bom["component_identities"]
        ["viewer_proxy"]["config"]["sha256"],
        "admission_hmac_key_fingerprint": bom["admission_security"]
        ["host_key_fingerprint"],
        "delivery_store_cutover": bom["delivery_store_cutover"],
        "restart_scope": bom["restart_scope"],
        "artifact_transport": bom["pipeline"]["artifact_transport"],
        "target_input_gate": {
            "fresh_official_meegle_revalidation_required": True,
            "required_before_target_execution": True,
            "initial_input_and_field_identity_must_match": True,
        },
        "post_cutover_canary": {
            "ordinary_kafka_ingest_only": True,
            "activation_entrypoint": "kafka_ingest",
            "activation_slot_kind": "kafka_success",
            "activation_reason": "activation_bounded_slot_consumed",
            "comment_terminal_outcome": "ack",
            "recovery_write_count": 0,
            "operator_recovery_provenance": [],
            "minimum_offset_source": (
                "post_viewer_proxy_reload_owner_only_no_commit_end_offset_gate"
            ),
            "fresh_exact_record_reread_required": True,
            "consumer_group_id": None,
            "enable_auto_commit": False,
            "commit_called": False,
            "raw_payload_persisted": False,
        },
    }


def _validate_final_validation_receipt(
    owned: OwnedJson,
    *,
    request: Mapping[str, Any],
    execution_started_at: datetime,
    now: datetime,
) -> Mapping[str, Any]:
    body = owned.body
    expected_keys = {
        "schema_version",
        "ok",
        "phase",
        "release_id",
        "validated_at",
        "request_sha256",
        "release_bom_sha256",
        "candidate_observation",
        "approval",
        "quarantine_baseline_approval",
        "approval_nonce_claim",
        "execution_preflight",
        "execute_before",
        "bootstrap_authorization",
        "authorized_scope",
        "execution_boundary",
        "production_ready",
        "blockers",
        "production_effects_executed",
    }
    if (
        set(body) != expected_keys
        or owned.raw != _canonical_bytes(body, newline=True)
        or body.get("schema_version") != VALIDATION_SCHEMA_VERSION
        or body.get("ok") is not True
        or body.get("phase") != "final"
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_final_validation_receipt_invalid"
        )
    validated_at = _timestamp(
        body.get("validated_at"), field="final_validated_at"
    )
    execute_before = _timestamp(
        body.get("execute_before"), field="final_execute_before"
    )
    if (
        body.get("release_id") != request["release_id"]
        or body.get("request_sha256") != request["request_sha256"]
        or body.get("release_bom_sha256") != request["release_bom_sha256"]
        or body.get("production_ready") is not True
        or body.get("blockers") != []
        or body.get("production_effects_executed") is not False
        or validated_at > execution_started_at
        or execution_started_at > execute_before
        or execution_started_at > now + MAX_FUTURE_SKEW
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_final_validation_receipt_invalid"
        )
    approval_ref = body.get("approval")
    if not isinstance(approval_ref, Mapping):
        raise ProdE2EReleaseError(
            "prod_e2e_release_final_validation_approval_invalid"
        )
    approval = _validate_approval(
        _read_owned_json(
            Path(str(approval_ref.get("evidence_path") or "")),
            artifact="final_approval",
        ),
        request=request,
        now=execution_started_at,
    )
    if dict(approval_ref) != approval:
        raise ProdE2EReleaseError(
            "prod_e2e_release_final_validation_approval_invalid"
        )
    baseline_approval_ref = body.get("quarantine_baseline_approval")
    if not isinstance(baseline_approval_ref, Mapping):
        raise ProdE2EReleaseError(
            "prod_e2e_release_final_validation_baseline_approval_invalid"
        )
    baseline_approval = _validate_quarantine_baseline_approval(
        _read_owned_json(
            Path(str(baseline_approval_ref.get("evidence_path") or "")),
            artifact="final_quarantine_baseline_approval",
        ),
        request=request,
        release_approval=approval,
        now=execution_started_at,
    )
    if dict(baseline_approval_ref) != baseline_approval:
        raise ProdE2EReleaseError(
            "prod_e2e_release_final_validation_baseline_approval_invalid"
        )
    authorization_ref = body.get("bootstrap_authorization")
    if not isinstance(authorization_ref, Mapping):
        raise ProdE2EReleaseError(
            "prod_e2e_release_final_validation_authorization_invalid"
        )
    authorization_owned = _read_owned_json(
        Path(str(authorization_ref.get("evidence_path") or "")),
        artifact="final_bootstrap_authorization",
    )
    try:
        authorization = prod_bootstrap.validate_bootstrap_authorization(
            authorization_owned.body,
            now=execution_started_at,
            expected_epoch_id=request["release_bom"]["bootstrap_authorization"][
                "bootstrap_epoch_id"
            ],
            expected_release_bom_sha256=request["release_bom_sha256"],
            expected_release_approval_id=approval["approval_id"],
            expected_approval_evidence_sha256=approval["sha256"],
            authorization_receipt_sha256=authorization_owned.sha256,
        )
    except prod_bootstrap.RcaBootstrapAuthorizationError as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_final_validation_authorization_invalid"
        ) from exc
    expected_authorization = {
        **authorization,
        "evidence_path": str(authorization_owned.path),
        "evidence_sha256": authorization_owned.sha256,
    }
    if dict(authorization_ref) != expected_authorization:
        raise ProdE2EReleaseError(
            "prod_e2e_release_final_validation_authorization_invalid"
        )
    candidate_ref = body.get("candidate_observation")
    if not isinstance(candidate_ref, Mapping):
        raise ProdE2EReleaseError(
            "prod_e2e_release_final_validation_candidate_invalid"
        )
    candidate = _validate_candidate_observation(
        _read_owned_json(
            Path(str(candidate_ref.get("path") or "")),
            artifact="final_candidate_observation",
        ),
        release_id=request["release_id"],
        expected_phase="final",
        expected_root=request["release_bom"]["pipeline"][
            "final_candidate_root"
        ],
        now=validated_at,
        require_fresh=False,
    )
    if dict(candidate_ref) != candidate:
        raise ProdE2EReleaseError(
            "prod_e2e_release_final_validation_candidate_invalid"
        )
    preflight_ref = body.get("execution_preflight")
    if not isinstance(preflight_ref, Mapping):
        raise ProdE2EReleaseError(
            "prod_e2e_release_final_validation_preflight_invalid"
        )
    preflight_owned = _read_owned_json(
        Path(str(preflight_ref.get("path") or "")),
        artifact="final_execution_preflight",
    )
    preflight = preflight_owned.body
    backup = preflight.get("fresh_live_backup")
    if not isinstance(backup, Mapping):
        raise ProdE2EReleaseError(
            "prod_e2e_release_final_validation_preflight_invalid"
        )
    backup_path, backup_sha256, backup_size = _read_owned_blob(
        Path(str(backup.get("path") or "")), artifact="final_preflight_backup"
    )
    db_cutover = request["release_bom"]["delivery_store_cutover"]
    backup_projection = _run_canonical_db_projection(
        db_path=backup_path,
        host_commit=request["release_bom"]["host_runtime"]["canonical_final"][
            "commit"
        ],
        host_tree=request["release_bom"]["host_runtime"]["canonical_final"][
            "tree"
        ],
    )
    logical_sha256 = backup_projection["projection"]["logical_sha256"]
    if (
        preflight_owned.sha256 != preflight_ref.get("sha256")
        or preflight.get("schema_version") != EXECUTION_PREFLIGHT_SCHEMA_VERSION
        or preflight.get("release_id") != request["release_id"]
        or preflight.get("request_sha256") != request["request_sha256"]
        or preflight.get("release_bom_sha256") != request["release_bom_sha256"]
        or preflight.get("approval_sha256") != approval["sha256"]
        or preflight.get("baseline_approval_sha256")
        != baseline_approval["sha256"]
        or preflight.get("authorization_sha256") != authorization_owned.sha256
        or backup_sha256 != backup.get("sha256")
        or backup_size != backup.get("size_bytes")
        or logical_sha256 != db_cutover["approved_source_logical_sha256"]
        or preflight.get("fresh_live_pre_logical_sha256") != logical_sha256
        or preflight_ref.get("fresh_live_backup", {}).get("logical_sha256")
        != logical_sha256
        or preflight_ref.get("activation_anchors_before")
        != preflight.get("activation_anchors_before")
        or _timestamp(
            baseline_approval["created_at"], field="baseline_approval_created_at"
        )
        > _timestamp(preflight["observed_at"], field="execution_preflight_observed_at")
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_final_validation_preflight_invalid"
        )
    nonce_ref = body.get("approval_nonce_claim")
    if not isinstance(nonce_ref, Mapping) or set(nonce_ref) != {
        "path",
        "sha256",
        "nonce_sha256",
        "consumed",
    }:
        raise ProdE2EReleaseError(
            "prod_e2e_release_final_validation_nonce_invalid"
        )
    nonce_owned = _read_owned_json(
        Path(str(nonce_ref.get("path") or "")), artifact="approval_nonce_claim"
    )
    expected_nonce = {
        "schema_version": "pnc_rca_release_approval_nonce_claim_v1",
        "nonce_sha256": approval["nonce_sha256"],
        "approval_id": approval["approval_id"],
        "approval_sha256": approval["sha256"],
        "release_id": request["release_id"],
        "request_sha256": request["request_sha256"],
        "release_bom_sha256": request["release_bom_sha256"],
        "claimed_at": validated_at.isoformat(),
    }
    if (
        nonce_owned.body != expected_nonce
        or nonce_owned.sha256 != nonce_ref.get("sha256")
        or nonce_ref.get("nonce_sha256") != approval["nonce_sha256"]
        or nonce_ref.get("consumed") is not True
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_final_validation_nonce_invalid"
        )
    bom = request["release_bom"]
    expected_scope = _authorized_scope(bom)
    expected_execute_before = min(
        _timestamp(approval["expires_at"], field="approval_expires_at"),
        _timestamp(authorization["deadline"], field="authorization_deadline"),
        _timestamp(
            bom["component_identities"]["observed_at"],
            field="component_binding_observed_at",
        )
        + MAX_FINAL_OBSERVATION_AGE,
        _timestamp(candidate["observed_at"], field="candidate_observed_at")
        + MAX_FINAL_OBSERVATION_AGE,
        _timestamp(
            preflight_ref.get("observed_at"),
            field="execution_preflight_observed_at",
        )
        + MAX_FINAL_OBSERVATION_AGE,
    )
    if (
        execute_before != expected_execute_before
        or body.get("authorized_scope") != expected_scope
        or body.get("execution_boundary") != bom["tooling_boundary"]
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_final_validation_scope_invalid"
        )
    return {
        "path": str(owned.path),
        "sha256": owned.sha256,
        "validated_at": validated_at.isoformat(),
        "execute_before": execute_before.isoformat(),
        "approval": approval,
        "baseline_approval": baseline_approval,
        "authorization": expected_authorization,
        "candidate": candidate,
        "preflight": dict(preflight_ref),
        "nonce_claim": dict(nonce_ref),
    }


_CANONICAL_KAFKA_END_GATE = r'''import hashlib,json,pathlib,sys
root=pathlib.Path(sys.argv[1])
env_path=pathlib.Path(sys.argv[2])
sys.path.insert(0,str(root))
from scripts import pnc_rca_kafka_consumer as consumer_module
expected_consumer=root/'scripts/pnc_rca_kafka_consumer.py'
if pathlib.Path(consumer_module.__file__).resolve()!=expected_consumer:
    raise SystemExit('module_origin_mismatch')
consumer_module.load_consumer_environment(str(env_path))
config=consumer_module.ConsumerConfig.from_env()
if config.topic!=sys.argv[3]:
    raise SystemExit('topic_mismatch')
from kafka import KafkaConsumer
from kafka.structs import TopicPartition
kwargs=config.kafka_kwargs()
kwargs['group_id']=None
kwargs['enable_auto_commit']=False
kwargs['client_id']='rca_release_post_cutover_end_gate'
consumer=KafkaConsumer(**kwargs)
tp=TopicPartition(sys.argv[3],int(sys.argv[4]))
commit_called=False
try:
    consumer.assign([tp])
    beginning=int(consumer.beginning_offsets([tp],timeout=10)[tp])
    end=int(consumer.end_offsets([tp],timeout=10)[tp])
finally:
    consumer.close(autocommit=False)
print(json.dumps({
 'schema_version':'pnc_rca_post_cutover_kafka_end_gate_v1',
 'topic':sys.argv[3],'partition':int(sys.argv[4]),
 'retained_start':beginning,'retained_end':end,
 'assignment_mode':'explicit_single_partition',
 'assigned_partitions':[int(sys.argv[4])],
 'group_id':None,'enable_auto_commit':False,'commit_called':commit_called,
 'raw_payload_persisted':False,
 'consumer_module_sha256':hashlib.sha256(expected_consumer.read_bytes()).hexdigest(),
},sort_keys=True,separators=(',',':')))
'''


def _validate_canary_kafka_gate(
    reference: Any,
    *,
    request: Mapping[str, Any],
    host_restart: Mapping[str, Any],
    gate_not_before: datetime,
    gate_not_after: datetime,
    preflight_retained_end: int,
) -> Mapping[str, Any]:
    if not isinstance(reference, Mapping) or set(reference) != {
        "evidence_path",
        "evidence_sha256",
    }:
        raise ProdE2EReleaseError(
            "prod_e2e_release_canary_kafka_gate_reference_invalid"
        )
    owned = _read_owned_json(
        Path(str(reference.get("evidence_path") or "")),
        artifact="canary_kafka_gate",
    )
    value = owned.body
    expected_fields = {
        "schema_version",
        "release_id",
        "observed_at",
        "topic",
        "partition",
        "retained_start",
        "retained_end",
        "assignment_mode",
        "assigned_partitions",
        "group_id",
        "enable_auto_commit",
        "commit_called",
        "raw_payload_persisted",
        "consumer_module_sha256",
        "observer_script_sha256",
        "interpreter_sha256",
        "host_commit",
        "host_tree",
        "consumer_pid",
        "consumer_runtime_sha256",
        "consumer_config_sha256",
    }
    if not isinstance(value, Mapping) or set(value) != expected_fields:
        raise ProdE2EReleaseError(
            "prod_e2e_release_canary_kafka_gate_invalid"
        )
    observed_at = _timestamp(
        value.get("observed_at"), field="canary_kafka_gate_observed_at"
    )
    host = request["release_bom"]["host_runtime"]["canonical_final"]
    consumer_restart = host_restart.get("local.pnc.rca-kafka-consumer")
    if (
        owned.sha256 != reference.get("evidence_sha256")
        or owned.raw != _canonical_bytes(value, newline=True)
        or
        value.get("schema_version")
        != "pnc_rca_post_cutover_kafka_end_gate_v1"
        or value.get("release_id") != request["release_id"]
        or value.get("topic") != TOPIC
        or value.get("partition") != PARTITION
        or not isinstance(value.get("retained_start"), int)
        or isinstance(value.get("retained_start"), bool)
        or not isinstance(value.get("retained_end"), int)
        or isinstance(value.get("retained_end"), bool)
        or value.get("retained_start", -1) < 0
        or value.get("retained_end", -1) < preflight_retained_end
        or value.get("retained_start", 0) > value.get("retained_end", -1)
        or value.get("assignment_mode") != "explicit_single_partition"
        or value.get("assigned_partitions") != [PARTITION]
        or value.get("group_id") is not None
        or value.get("enable_auto_commit") is not False
        or value.get("commit_called") is not False
        or value.get("raw_payload_persisted") is not False
        or value.get("consumer_module_sha256")
        != host["required_file_sha256"]["scripts/pnc_rca_kafka_consumer.py"]
        or value.get("observer_script_sha256")
        != hashlib.sha256(_CANONICAL_KAFKA_END_GATE.encode("utf-8")).hexdigest()
        or value.get("interpreter_sha256")
        != host["canonical_interpreter_sha256"]
        or value.get("host_commit") != host["commit"]
        or value.get("host_tree") != host["tree"]
        or not isinstance(consumer_restart, Mapping)
        or value.get("consumer_pid") != consumer_restart.get("new_pid")
        or value.get("consumer_runtime_sha256")
        != consumer_restart.get("runtime_sha256")
        or value.get("consumer_config_sha256")
        != consumer_restart.get("config_sha256")
        or observed_at < gate_not_before
        or observed_at > gate_not_after
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_canary_kafka_gate_invalid"
        )
    return {
        "evidence_path": str(owned.path),
        "evidence_sha256": owned.sha256,
        **dict(value),
        "observed_at": observed_at.isoformat(),
    }


def _validate_canary_remote_closeout(
    canary: Any,
    *,
    request: Mapping[str, Any],
    kafka_gate: Mapping[str, Any],
    earliest_observed_at: datetime,
    completion_observed_at: datetime,
) -> Mapping[str, Any]:
    expected_fields = {
        "topic",
        "partition",
        "offset",
        "event_uid",
        "raw_sha256",
        "project_key",
        "work_item_type_key",
        "work_item_id",
        "business_key",
        "submission_key",
        "generation",
        "trigger_kind",
        "source_kind",
        "delivery_source",
        "recovery_write_count",
        "operator_recovery_provenance",
        "kafka_preread",
        "observed_at",
        "terminal_at",
        "status",
        "activation",
        "task_id",
        "delivery_id",
        "effect_key",
        "semantic_payload_sha256",
        "artifact_set_id",
        "target_key",
        "issue_url",
        "terminal_bundle_path",
        "terminal_bundle_sha256",
        "terminal_receipt_sha256",
        "field_writes",
        "comment_write",
        "official_readback",
        "delivery_lineage",
    }
    if not isinstance(canary, Mapping) or set(canary) != expected_fields:
        raise ProdE2EReleaseError(
            "prod_e2e_release_post_cutover_canary_invalid"
        )
    observed_at = _timestamp(
        canary.get("observed_at"), field="canary_observed_at"
    )
    terminal_at = _timestamp(
        canary.get("terminal_at"), field="canary_terminal_at"
    )
    expected_issue_url = (
        f"https://project.feishu.cn/{TARGET_PROJECT_SIMPLE_NAME}/issue/detail/"
        f"{canary.get('work_item_id')}"
    )
    if (
        canary.get("topic") != TOPIC
        or canary.get("partition") != PARTITION
        or not isinstance(canary.get("offset"), int)
        or isinstance(canary.get("offset"), bool)
        or canary.get("offset", -1) <= LIVE_T0_OFFSET
        or canary.get("event_uid")
        != f"{TOPIC}:{canary.get('partition')}:{canary.get('offset')}"
        or canary.get("event_uid") == TARGET_EVENT_UID
        or canary.get("raw_sha256") == TARGET_RAW_SHA256
        or canary.get("work_item_id") == TARGET_WORK_ITEM_ID
        or canary.get("project_key") != TARGET_PROJECT_KEY
        or not _required_text(
            canary.get("work_item_type_key"), field="canary_work_item_type_key"
        )
        or not _required_text(
            canary.get("business_key"), field="canary_business_key"
        )
        or canary.get("business_key") == TARGET_BUSINESS_KEY
        or not _required_text(
            canary.get("work_item_id"), field="canary_work_item_id"
        )
        or not _required_text(
            canary.get("submission_key"), field="canary_submission_key"
        )
        or canary.get("submission_key") == TARGET_SUBMISSION_KEY
        or canary.get("task_id") != canary.get("submission_key")
        or _sha256(canary.get("raw_sha256"), field="canary_raw_sha256")
        != canary.get("raw_sha256")
        or canary.get("generation") != 1
        or canary.get("trigger_kind") != "issue_created"
        or canary.get("source_kind") != "kafka_workflow_event"
        or canary.get("delivery_source") != "ordinary_kafka_ingest"
        or canary.get("recovery_write_count") != 0
        or canary.get("operator_recovery_provenance") != []
        or canary.get("issue_url") != expected_issue_url
        or canary.get("status") != "closed"
        or observed_at < earliest_observed_at
        or terminal_at < observed_at
        or terminal_at > completion_observed_at
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_post_cutover_canary_invalid"
        )
    activation = _validate_activation_receipt(
        canary.get("activation"),
        request=request,
        root=canary,
        expected_entrypoint="kafka_ingest",
    )
    if (
        activation.get("slot_kind") != "kafka_success"
        or activation.get("reason") != "activation_bounded_slot_consumed"
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_post_cutover_canary_activation_invalid"
        )
    host = request["release_bom"]["host_runtime"]["canonical_final"]
    live_kafka = _observe_kafka_record_live(
        host_commit=str(host["commit"]),
        host_tree=str(host["tree"]),
        topic=str(canary["topic"]),
        partition=int(canary["partition"]),
        offset=int(canary["offset"]),
        raw_sha256=str(canary["raw_sha256"]),
        business_key=str(canary["business_key"]),
        submission_key=str(canary["submission_key"]),
        work_item_id=str(canary["work_item_id"]),
        project_key=str(canary["project_key"]),
        work_item_type_key=str(canary["work_item_type_key"]),
    )
    validated_kafka = _validate_kafka_observation_pair(
        canary.get("kafka_preread"),
        live_kafka,
        artifact="canary_kafka_preread",
        recorded_not_before=_timestamp(
            kafka_gate["observed_at"], field="canary_kafka_gate_observed_at"
        ),
        recorded_not_after=observed_at,
        live_now=completion_observed_at,
    )
    record_timestamp = datetime.fromtimestamp(
        int(validated_kafka["record_timestamp_ms"]) / 1000,
        tz=timezone.utc,
    )
    if (
        canary["offset"] < kafka_gate["retained_end"]
        or record_timestamp
        < _timestamp(
            kafka_gate["observed_at"], field="canary_kafka_gate_observed_at"
        )
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_post_cutover_canary_not_natural"
        )

    bundle_owned = _read_owned_json(
        Path(str(canary.get("terminal_bundle_path") or "")),
        artifact="canary_terminal_bundle",
    )
    bundle = _run_canonical_target_bundle_verifier(
        bundle_owned,
        host_commit=str(host["commit"]),
        host_tree=str(host["tree"]),
        expected={
            "business_key": canary["business_key"],
            "submission_key": canary["submission_key"],
            "topic": canary["topic"],
            "partition": canary["partition"],
            "offset": canary["offset"],
            "project_key": canary["project_key"],
            "project_simple_name": TARGET_PROJECT_SIMPLE_NAME,
            "work_item_id": canary["work_item_id"],
            "issue_url": canary["issue_url"],
        },
    )
    if (
        canary.get("terminal_bundle_sha256") != bundle_owned.sha256
        or canary.get("terminal_receipt_sha256") != bundle_owned.sha256
        or any(
            canary.get(field) != bundle.get(field)
            for field in (
                "business_key",
                "submission_key",
                "generation",
                "project_key",
                "work_item_type_key",
                "work_item_id",
                "delivery_id",
                "effect_key",
                "semantic_payload_sha256",
                "artifact_set_id",
                "target_key",
                "issue_url",
            )
        )
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_canary_delivery_identity_invalid"
        )

    writes = canary.get("field_writes")
    expected_field_values = bundle["field_values"]
    if (
        not isinstance(writes, list)
        or len(writes) != len(TARGET_FIELD_KEYS)
        or [
            item.get("field_key")
            for item in writes
            if isinstance(item, Mapping)
        ]
        != list(TARGET_FIELD_KEYS)
        or any(
            not isinstance(item, Mapping)
            or set(item)
            != {"field_key", "value_sha256", "value_utf8_bytes", "written_at"}
            or item.get("value_sha256")
            != expected_field_values.get(item.get("field_key"), {}).get("sha256")
            or item.get("value_utf8_bytes")
            != expected_field_values.get(item.get("field_key"), {}).get(
                "utf8_bytes"
            )
            or not observed_at
            <= _timestamp(item.get("written_at"), field="canary_field_written_at")
            <= terminal_at
            for item in writes
        )
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_canary_field_writes_invalid"
        )

    comment = canary.get("comment_write")
    if (
        not isinstance(comment, Mapping)
        or set(comment)
        != {
            "comment_id",
            "content_sha256",
            "content_utf8_bytes",
            "written_at",
            "attempt_terminal_outcome",
        }
        or not _required_text(comment.get("comment_id"), field="canary_comment_id")
        or comment.get("content_sha256") != bundle["comment_content"]["sha256"]
        or comment.get("content_utf8_bytes")
        != bundle["comment_content"]["utf8_bytes"]
        or comment.get("attempt_terminal_outcome") != "ack"
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_canary_comment_write_invalid"
        )
    comment_at = _timestamp(
        comment.get("written_at"), field="canary_comment_written_at"
    )
    if comment_at < observed_at or comment_at > terminal_at:
        raise ProdE2EReleaseError(
            "prod_e2e_release_canary_comment_write_invalid"
        )

    readback = canary.get("official_readback")
    if (
        not isinstance(readback, Mapping)
        or set(readback)
        != {
            "adapter",
            "source",
            "scope",
            "observed_at",
            "fields",
            "comment_id",
            "comment_content_sha256",
            "marker_sha256",
            "marker_match_count",
        }
        or readback.get("adapter")
        != "MeegleIssueCommentAdapter.get_fields_and_comments"
        or readback.get("source") != "official_meegle_api"
        or readback.get("scope")
        != {
            "project_key": canary["project_key"],
            "work_item_id": canary["work_item_id"],
        }
        or readback.get("fields")
        != {
            item["field_key"]: {
                "value_sha256": item["value_sha256"],
                "value_utf8_bytes": item["value_utf8_bytes"],
            }
            for item in writes
        }
        or readback.get("comment_id") != comment["comment_id"]
        or readback.get("comment_content_sha256") != comment["content_sha256"]
        or readback.get("marker_sha256") != bundle["marker"]["sha256"]
        or readback.get("marker_match_count") != 1
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_canary_official_readback_invalid"
        )
    readback_at = _timestamp(
        readback.get("observed_at"), field="canary_readback_observed_at"
    )
    if readback_at < max(
        [
            comment_at,
            *[
                _timestamp(item["written_at"], field="canary_field_written_at")
                for item in writes
            ],
        ]
    ) or readback_at > terminal_at:
        raise ProdE2EReleaseError(
            "prod_e2e_release_canary_official_readback_invalid"
        )

    live_readback = _observe_official_meegle_readback_live(
        project_key=str(canary["project_key"]),
        work_item_id=str(canary["work_item_id"]),
        comment_id=str(comment["comment_id"]),
        effect_key=str(canary["effect_key"]),
        artifact_set_id=str(canary["artifact_set_id"]),
        host_commit=str(host["commit"]),
        host_tree=str(host["tree"]),
    )
    if (
        live_readback["fields"]
        != {
            item["field_key"]: {
                "sha256": item["value_sha256"],
                "utf8_bytes": item["value_utf8_bytes"],
            }
            for item in writes
        }
        or live_readback["comment_content"]
        != {
            "sha256": comment["content_sha256"],
            "utf8_bytes": comment["content_utf8_bytes"],
        }
        or live_readback["comment_id"] != comment["comment_id"]
        or live_readback["marker_sha256"] != bundle["marker"]["sha256"]
        or live_readback["marker_match_count"] != 1
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_canary_official_readback_live_mismatch"
        )

    lineage = canary.get("delivery_lineage")
    if (
        not isinstance(lineage, Mapping)
        or set(lineage)
        != {
            "business_key",
            "submission_key",
            "task_id",
            "delivery_id",
            "effect_key",
            "semantic_payload_sha256",
            "artifact_set_id",
            "terminal_receipt_sha256",
            "field_keys",
            "comment_id",
            "attempt_terminal_outcome",
            "lineage_sha256",
        }
        or any(
            lineage.get(field) != canary.get(field)
            for field in (
                "business_key",
                "submission_key",
                "task_id",
                "delivery_id",
                "effect_key",
                "semantic_payload_sha256",
                "artifact_set_id",
                "terminal_receipt_sha256",
            )
        )
        or lineage.get("field_keys") != list(TARGET_FIELD_KEYS)
        or lineage.get("comment_id") != comment["comment_id"]
        or lineage.get("attempt_terminal_outcome")
        != comment["attempt_terminal_outcome"]
        or lineage.get("lineage_sha256")
        != _sha256_value(
            {key: value for key, value in lineage.items() if key != "lineage_sha256"}
        )
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_canary_delivery_lineage_invalid"
        )
    return {
        **dict(canary),
        "activation": activation,
        "live_kafka_preread": validated_kafka,
        "bundle_verification": bundle,
        "live_official_readback": live_readback,
    }


def _validate_completion_receipt(
    owned: OwnedJson,
    *,
    request: Mapping[str, Any],
    final_validation: OwnedJson,
    now: datetime,
) -> Mapping[str, Any]:
    body = owned.body
    expected_fields = {
        "schema_version",
        "release_id",
        "observed_at",
        "execution_started_at",
        "final_validation_sha256",
        "request_sha256",
        "release_bom_sha256",
        "outcome",
        "cutover",
        "target_delivery",
        "field_writes",
        "comment_write",
        "official_readback",
        "delivery_lineage",
        "post_cutover_canary",
        "viewer_proxy_closeout",
        "production_effects",
    }
    observed_at = _timestamp(
        body.get("observed_at"), field="completion_observed_at"
    )
    execution_started_at = _timestamp(
        body.get("execution_started_at"), field="completion_execution_started_at"
    )
    verified_final = _validate_final_validation_receipt(
        final_validation,
        request=request,
        execution_started_at=execution_started_at,
        now=now,
    )
    execute_before = _timestamp(
        verified_final.get("execute_before"), field="final_execute_before"
    )
    final_validated_at = _timestamp(
        verified_final.get("validated_at"), field="final_validated_at"
    )
    if (
        set(body) != expected_fields
        or body.get("schema_version") != COMPLETION_RECEIPT_SCHEMA_VERSION
        or body.get("release_id") != request["release_id"]
        or body.get("final_validation_sha256") != final_validation.sha256
        or body.get("request_sha256") != request["request_sha256"]
        or body.get("release_bom_sha256") != request["release_bom_sha256"]
        or execution_started_at < final_validated_at
        or execution_started_at > execute_before
        or observed_at < execution_started_at
        or observed_at > now + MAX_FUTURE_SKEW
    ):
        raise ProdE2EReleaseError("prod_e2e_release_completion_binding_invalid")
    outcome = body.get("outcome")
    cutover = body.get("cutover")
    effects = body.get("production_effects")
    if not isinstance(cutover, Mapping) or not isinstance(effects, Mapping):
        raise ProdE2EReleaseError("prod_e2e_release_completion_invalid")
    if outcome == "rolled_back":
        failure_at = _timestamp(
            cutover.get("failure_at"), field="cutover_failure_at"
        )
        restored_at = _timestamp(
            cutover.get("restored_at"), field="cutover_restored_at"
        )
        services_restarted_at = _timestamp(
            cutover.get("services_restarted_at"),
            field="rollback_services_restarted_at",
        )
        preflight = verified_final["preflight"]
        preflight_backup = preflight.get("fresh_live_backup")
        preflight_anchors = preflight.get("activation_anchors_before")
        if (
            set(cutover)
            != {
                "failure_step",
                "failure_at",
                "source_backup_sha256",
                "restored_backup_sha256",
                "restored_logical_sha256",
                "restored_at",
                "activation_anchors_restored",
                "services_restarted_at",
                "host_restart",
                "vm_restore",
                "environment_written",
                "active_binding_written",
                "live_env_restored_sha256",
                "vm_report_env_restored_exists",
                "vm_report_env_restored_sha256",
            }
            or not _required_text(
                cutover.get("failure_step"), field="cutover_failure_step"
            )
            or failure_at < execution_started_at
            or restored_at < failure_at
            or services_restarted_at < restored_at
            or services_restarted_at > observed_at
            or not isinstance(preflight_backup, Mapping)
            or not isinstance(preflight_anchors, Mapping)
            or cutover.get("source_backup_sha256")
            != preflight_backup.get("sha256")
            or cutover.get("source_backup_sha256")
            != cutover.get("restored_backup_sha256")
            or cutover.get("restored_logical_sha256")
            != request["release_bom"]["delivery_store_cutover"].get(
                "approved_source_logical_sha256"
            )
            or cutover.get("environment_written") is not False
            or cutover.get("active_binding_written") is not False
            or cutover.get("live_env_restored_sha256")
            != request["release_bom"]["host_runtime"]["canonical_final"]
            ["host_environment_transition"]["pre_sha256"]
            or cutover.get("vm_report_env_restored_exists")
            is not request["release_bom"]["component_identities"]["worker"]
            ["report_environment_transition"]["pre_exists"]
            or cutover.get("vm_report_env_restored_sha256")
            != request["release_bom"]["component_identities"]["worker"]
            ["report_environment_transition"]["pre_sha256"]
            or cutover.get("activation_anchors_restored")
            != preflight_anchors
            or any(
                body.get(field) not in (None, [], {})
                for field in (
                    "target_delivery",
                    "field_writes",
                    "comment_write",
                    "official_readback",
                    "delivery_lineage",
                    "post_cutover_canary",
                    "viewer_proxy_closeout",
                )
            )
            or dict(effects)
            != {
                "live_database_restored": True,
                "environment_written": False,
                "active_binding_written": False,
                "target_o650_executed": False,
                "feishu_written": False,
                "canary_executed": False,
            }
        ):
            raise ProdE2EReleaseError(
                "prod_e2e_release_rollback_closeout_invalid"
            )
        live_projection = _run_canonical_db_projection(
            db_path=Path(DELIVERY_DB_PATH),
            host_commit=request["release_bom"]["host_runtime"][
                "canonical_final"
            ]["commit"],
            host_tree=request["release_bom"]["host_runtime"][
                "canonical_final"
            ]["tree"],
            allow_live=True,
        )
        if (
            live_projection["projection"]["logical_sha256"]
            != request["release_bom"]["delivery_store_cutover"][
                "approved_source_logical_sha256"
            ]
        ):
            raise ProdE2EReleaseError(
                "prod_e2e_release_rollback_live_database_mismatch"
            )
        _observe_activation_anchors_live(preflight_anchors)
        rollback_host_restart = cutover.get("host_restart")
        rollback_vm_restore = cutover.get("vm_restore")
        live_host = _observe_host_writer_stop_live()
        expected_runtime = request["release_bom"]["host_runtime"][
            "canonical_final"
        ]["service_runtime_files_sha256"]
        if (
            not isinstance(rollback_host_restart, Mapping)
            or set(rollback_host_restart) != set(HOST_SERVICE_LABELS)
            or any(
                not isinstance(rollback_host_restart.get(label), Mapping)
                or live_host[label].get("state") != "running"
                or live_host[label].get("pid")
                != rollback_host_restart[label].get("new_pid")
                or rollback_host_restart[label].get("runtime_sha256")
                != expected_runtime[label]
                or live_host[label].get("config_sha256")
                != request["release_bom"]["host_runtime"]["canonical_final"][
                    "service_config_sha256"
                ][label]
                for label in HOST_SERVICE_LABELS
            )
            or not isinstance(rollback_vm_restore, Mapping)
            or set(rollback_vm_restore) != set(VM_SERVICE_UNITS)
            or any(
                not isinstance(rollback_vm_restore.get(unit), Mapping)
                or set(rollback_vm_restore[unit])
                != {
                    "unit",
                    "active_state",
                    "sub_state",
                    "main_pid",
                    "unit_config_sha256",
                    "entrypoint_sha256",
                }
                or rollback_vm_restore[unit].get("unit") != unit
                or rollback_vm_restore[unit].get("active_state")
                != preflight["vm_services"][unit].get("active_state")
                or rollback_vm_restore[unit].get("sub_state")
                != preflight["vm_services"][unit].get("sub_state")
                or rollback_vm_restore[unit].get("main_pid")
                != preflight["vm_services"][unit].get("main_pid")
                or rollback_vm_restore[unit].get("unit_config_sha256")
                != preflight["vm_services"][unit].get("unit_config_sha256")
                or rollback_vm_restore[unit].get("entrypoint_sha256")
                != preflight["vm_services"][unit].get("entrypoint_sha256")
                for unit in VM_SERVICE_UNITS
            )
        ):
            raise ProdE2EReleaseError(
                "prod_e2e_release_rollback_restart_invalid"
            )
        rollback_live_runtime = _observe_host_live_runtime(
            expected_host=request["release_bom"]["host_runtime"][
                "canonical_final"
            ],
            require_running=True,
        )
        live_vm = _observe_vm_components_live(
            expected_viewer_origin=request["release_bom"]
            ["component_identities"]["viewer_proxy"]["public_origin"],
            environment_phase="pre",
        )
        live_vm_services = live_vm.get("services")
        worker_identity = request["release_bom"]["component_identities"]["worker"]
        expected_vm_configs = {
            VM_DAEMON_UNIT: worker_identity["daemon_unit_config_sha256"],
            VM_REPORT_UNIT: worker_identity["report_unit_config_sha256"],
        }
        if not isinstance(live_vm_services, Mapping) or any(
            not isinstance(live_vm_services.get(unit), Mapping)
            or live_vm_services[unit].get("active_state")
            != rollback_vm_restore[unit].get("active_state")
            or live_vm_services[unit].get("sub_state")
            != rollback_vm_restore[unit].get("sub_state")
            or live_vm_services[unit].get("main_pid")
            != rollback_vm_restore[unit].get("main_pid")
            or live_vm_services[unit].get("unit_config_sha256")
            != expected_vm_configs[unit]
            or rollback_vm_restore[unit].get("unit_config_sha256")
            != expected_vm_configs[unit]
            or live_vm_services[unit].get("entrypoint_sha256")
            != rollback_vm_restore[unit].get("entrypoint_sha256")
            for unit in VM_SERVICE_UNITS
        ) or live_vm.get("report_environment_transition") != worker_identity.get(
            "report_environment_transition"
        ):
            raise ProdE2EReleaseError(
                "prod_e2e_release_rollback_restart_invalid"
            )
        return {
            "path": str(owned.path),
            "sha256": owned.sha256,
            "outcome": "rolled_back",
            "production_completed": False,
            "restored_at": restored_at.isoformat(),
            "host_live_runtime": rollback_live_runtime,
        }
    if outcome != "success":
        raise ProdE2EReleaseError("prod_e2e_release_completion_outcome_invalid")
    required_cutover_fields = (
        "writers_stopped_at",
        "backup_captured_at",
        "database_installed_at",
        "post_digest_verified_at",
        "core_verified_at",
        "baseline_bound_at",
        "environment_written_at",
        "active_binding_written_at",
        "services_restarted_at",
        "viewer_proxy_reloaded_at",
    )
    if set(cutover) != {
        *required_cutover_fields,
        "post_logical_sha256",
        "post_install_checkpoint",
        "quarantine_core_sha256",
        "baseline_approval_sha256",
        "baseline_file_sha256",
        "baseline_path",
        "active_release_binding_path",
        "live_env_path",
        "live_env_pre_sha256",
        "live_env_post_sha256",
        "live_env_post_bytes",
        "live_env_atomic_replace",
        "live_env_written_after_core_gate",
        "vm_report_env_path",
        "vm_report_env_pre_exists",
        "vm_report_env_pre_sha256",
        "vm_report_env_post_sha256",
        "vm_report_env_post_bytes",
        "vm_report_env_atomic_replace",
        "vm_report_env_written_after_core_gate",
        "host_restart",
        "vm_restarts",
        "canary_kafka_gate",
        "restore_required",
        "restore_performed",
    }:
        raise ProdE2EReleaseError("prod_e2e_release_cutover_closeout_invalid")
    cutover_times = [
        _timestamp(cutover.get(field), field=f"cutover_{field}")
        for field in required_cutover_fields
    ]
    cutover_time_by_field = dict(zip(required_cutover_fields, cutover_times))
    db_binding = request["release_bom"]["delivery_store_cutover"]
    environment_transition = request["release_bom"]["host_runtime"][
        "canonical_final"
    ]["host_environment_transition"]
    vm_environment_transition = request["release_bom"]["component_identities"][
        "worker"
    ]["report_environment_transition"]
    if (
        cutover_times != sorted(cutover_times)
        or cutover_times[0] < execution_started_at
        or cutover_times[-1] > observed_at
        or cutover.get("post_logical_sha256")
        != db_binding.get("approved_post_migration_logical_sha256")
        or cutover.get("quarantine_core_sha256")
        != db_binding.get("quarantine_core", {}).get("core_sha256")
        or cutover.get("baseline_approval_sha256")
        != verified_final["baseline_approval"]["sha256"]
        or _sha256(
            cutover.get("baseline_file_sha256"), field="baseline_file_sha256"
        )
        != cutover.get("baseline_file_sha256")
        or not Path(str(cutover.get("baseline_path") or "")).is_absolute()
        or not Path(
            str(cutover.get("active_release_binding_path") or "")
        ).is_absolute()
        or cutover.get("live_env_path") != CANONICAL_HOST_ENV
        or cutover.get("live_env_pre_sha256")
        != environment_transition["pre_sha256"]
        or cutover.get("live_env_post_sha256")
        != environment_transition["post_sha256"]
        or cutover.get("live_env_post_bytes")
        != environment_transition["post_bytes"]
        or cutover.get("live_env_atomic_replace") is not True
        or cutover.get("live_env_written_after_core_gate") is not True
        or cutover.get("vm_report_env_path") != VM_REPORT_ENV_PATH
        or cutover.get("vm_report_env_pre_exists")
        is not vm_environment_transition["pre_exists"]
        or cutover.get("vm_report_env_pre_sha256")
        != vm_environment_transition["pre_sha256"]
        or cutover.get("vm_report_env_post_sha256")
        != vm_environment_transition["post_sha256"]
        or cutover.get("vm_report_env_post_bytes")
        != vm_environment_transition["post_bytes"]
        or cutover.get("vm_report_env_atomic_replace") is not True
        or cutover.get("vm_report_env_written_after_core_gate") is not True
        or cutover.get("restore_required") is not False
        or cutover.get("restore_performed") is not False
    ):
        raise ProdE2EReleaseError("prod_e2e_release_cutover_closeout_invalid")
    cutover_checkpoint = _validate_cutover_database_checkpoint(
        cutover.get("post_install_checkpoint"),
        request=request,
        installed_at=cutover_time_by_field["database_installed_at"],
        verified_at=cutover_time_by_field["post_digest_verified_at"],
        preflight_backup_path=str(
            verified_final["preflight"]["fresh_live_backup"]["path"]
        ),
    )
    host_restart = cutover.get("host_restart")
    vm_restarts = cutover.get("vm_restarts")
    if (
        not isinstance(host_restart, Mapping)
        or set(host_restart) != set(HOST_SERVICE_LABELS)
        or any(
            not isinstance(item, Mapping)
            or set(item) != {"old_pid", "new_pid", "runtime_sha256", "config_sha256"}
            or not isinstance(item.get("new_pid"), int)
            or item.get("new_pid", 0) <= 0
            or item.get("new_pid") == item.get("old_pid")
            or _sha256(item.get("runtime_sha256"), field="restart_runtime_sha256")
            != item.get("runtime_sha256")
            or item.get("config_sha256")
            != request["release_bom"]["host_runtime"]["canonical_final"][
                "service_config_sha256"
            ].get(label)
            for label, item in host_restart.items()
        )
        or not isinstance(vm_restarts, Mapping)
        or set(vm_restarts) != set(VM_SERVICE_UNITS)
        or any(
            not isinstance(item, Mapping)
            or set(item)
            != {
                "unit",
                "old_pid",
                "new_pid",
                "unit_config_sha256",
                "entrypoint",
                "entrypoint_sha256",
            }
            or item.get("unit") != unit
            or not isinstance(item.get("new_pid"), int)
            or item.get("new_pid", 0) <= 0
            or item.get("new_pid") == item.get("old_pid")
            or _sha256(
                item.get("unit_config_sha256"), field="vm_restart_unit_sha256"
            )
            != item.get("unit_config_sha256")
            or _sha256(
                item.get("entrypoint_sha256"), field="vm_restart_entrypoint_sha256"
            )
            != item.get("entrypoint_sha256")
            for unit, item in vm_restarts.items()
        )
    ):
        raise ProdE2EReleaseError("prod_e2e_release_restart_closeout_invalid")
    live_host_services = _observe_host_writer_stop_live()
    host_runtime_sha256 = request["release_bom"]["host_runtime"][
        "canonical_final"
    ]["service_runtime_files_sha256"]
    if any(
        live_host_services[label].get("job_present") is not True
        or live_host_services[label].get("state") != "running"
        or live_host_services[label].get("pid") != host_restart[label]["new_pid"]
        or live_host_services[label].get("config_sha256")
        != host_restart[label]["config_sha256"]
        or host_restart[label].get("runtime_sha256")
        != host_runtime_sha256[label]
        for label in HOST_SERVICE_LABELS
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_host_restart_live_mismatch"
        )
    host_live_runtime = _observe_host_live_runtime(
        expected_host=request["release_bom"]["host_runtime"]["canonical_final"],
        require_running=True,
        environment_phase="post",
    )
    component_descriptor = request["release_bom"]["component_identities"]
    live_components = _validate_component_binding(
        _read_owned_json(
            Path(str(component_descriptor["evidence_path"])),
            artifact="closeout_component_binding",
        ),
        release_id=request["release_id"],
        now=observed_at,
        require_fresh=False,
        verify_vm_live=True,
        environment_phase="post",
    )
    live_vm = _observe_vm_components_live(
        expected_viewer_origin=component_descriptor["viewer_proxy"]
        ["public_origin"],
        environment_phase="post",
    )
    live_vm_services = live_vm.get("services")
    worker_identity = request["release_bom"]["component_identities"]["worker"]
    expected_vm_configs = {
        VM_DAEMON_UNIT: worker_identity["daemon_unit_config_sha256"],
        VM_REPORT_UNIT: worker_identity["report_unit_config_sha256"],
    }
    if (
        not isinstance(live_vm_services, Mapping)
        or any(
            not isinstance(live_vm_services.get(unit), Mapping)
            or live_vm_services[unit].get("active_state") != "active"
            or live_vm_services[unit].get("sub_state") != "running"
            or live_vm_services[unit].get("main_pid")
            != vm_restarts[unit].get("new_pid")
            or live_vm_services[unit].get("unit_config_sha256")
            != expected_vm_configs[unit]
            or vm_restarts[unit].get("unit_config_sha256")
            != expected_vm_configs[unit]
            or live_vm_services[unit].get("entrypoint")
            != vm_restarts[unit].get("entrypoint")
            or live_vm_services[unit].get("entrypoint_sha256")
            != vm_restarts[unit].get("entrypoint_sha256")
            for unit in VM_SERVICE_UNITS
        )
        or live_components["sha256"] != component_descriptor["evidence_sha256"]
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_vm_restart_live_mismatch"
        )
    viewer_proxy_live = _validate_viewer_proxy_live_observation(
        body.get("viewer_proxy_closeout"),
        candidate=component_descriptor["viewer_proxy"],
        report_service=component_descriptor["pipeline"]["report_service"],
        report_restart=vm_restarts[VM_REPORT_UNIT],
        earliest_observed_at=cutover_time_by_field["services_restarted_at"],
        completion_observed_at=observed_at,
    )
    if (
        viewer_proxy_live["reloaded_at"]
        != cutover_time_by_field["viewer_proxy_reloaded_at"].isoformat()
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_viewer_proxy_reload_binding_invalid"
        )
    preflight_target_kafka = verified_final["preflight"].get(
        "target_kafka_preread"
    )
    if (
        not isinstance(preflight_target_kafka, Mapping)
        or not isinstance(preflight_target_kafka.get("retained_end"), int)
        or isinstance(preflight_target_kafka.get("retained_end"), bool)
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_canary_kafka_gate_invalid"
        )
    canary_kafka_gate = _validate_canary_kafka_gate(
        cutover.get("canary_kafka_gate"),
        request=request,
        host_restart=host_restart,
        gate_not_before=cutover_time_by_field["viewer_proxy_reloaded_at"],
        gate_not_after=observed_at,
        preflight_retained_end=preflight_target_kafka["retained_end"],
    )
    live_baseline = _observe_live_baseline(
        request=request,
        cutover=cutover,
        approval_sha256=verified_final["baseline_approval"]["sha256"],
    )
    target = body.get("target_delivery")
    if not isinstance(target, Mapping) or set(target) != {
        "topic",
        "partition",
        "offset",
        "event_uid",
        "raw_sha256",
        "project_key",
        "work_item_id",
        "work_item_type_key",
        "business_key",
        "submission_key",
        "generation",
        "task_id",
        "delivery_id",
        "effect_key",
        "semantic_payload_sha256",
        "artifact_set_id",
        "target_key",
        "issue_url",
        "terminal_bundle_path",
        "terminal_bundle_sha256",
        "terminal_receipt_sha256",
        "terminal_at",
        "status",
        "activation",
    }:
        raise ProdE2EReleaseError("prod_e2e_release_target_delivery_invalid")
    bundle_owned = _read_owned_json(
        Path(str(target.get("terminal_bundle_path") or "")),
        artifact="target_terminal_bundle",
    )
    bundle_verification = _run_canonical_target_bundle_verifier(
        bundle_owned,
        host_commit=request["release_bom"]["host_runtime"]["canonical_final"][
            "commit"
        ],
        host_tree=request["release_bom"]["host_runtime"]["canonical_final"][
            "tree"
        ],
    )
    terminal_at = _timestamp(target.get("terminal_at"), field="target_terminal_at")
    target_input_gate = verified_final["preflight"].get(
        "target_input_revalidation"
    )
    if not isinstance(target_input_gate, Mapping):
        raise ProdE2EReleaseError(
            "prod_e2e_release_target_input_revalidation_invalid"
        )
    target_input_gate_at = _timestamp(
        target_input_gate.get("observed_at"),
        field="target_input_revalidation_observed_at",
    )
    if (
        target.get("topic") != TOPIC
        or target.get("partition") != PARTITION
        or target.get("offset") != TARGET_OFFSET
        or target.get("event_uid") != TARGET_EVENT_UID
        or target.get("raw_sha256") != TARGET_RAW_SHA256
        or target.get("project_key") != TARGET_PROJECT_KEY
        or target.get("work_item_id") != TARGET_WORK_ITEM_ID
        or target.get("business_key") != TARGET_BUSINESS_KEY
        or target.get("submission_key") != TARGET_SUBMISSION_KEY
        or target.get("task_id") != TARGET_SUBMISSION_KEY
        or target.get("generation") != 1
        or target.get("terminal_bundle_sha256") != bundle_owned.sha256
        or target.get("terminal_receipt_sha256") != bundle_owned.sha256
        or any(
            target.get(field) != bundle_verification.get(field)
            for field in (
                "business_key",
                "submission_key",
                "generation",
                "project_key",
                "work_item_type_key",
                "work_item_id",
                "delivery_id",
                "effect_key",
                "semantic_payload_sha256",
                "artifact_set_id",
                "target_key",
                "issue_url",
            )
        )
        or _sha256(
            target.get("terminal_receipt_sha256"),
            field="target_terminal_receipt_sha256",
        )
        != target.get("terminal_receipt_sha256")
        or target.get("status") != "report_ready"
        or target_input_gate_at > terminal_at
        or terminal_at < cutover_times[-1]
        or terminal_at > observed_at
    ):
        raise ProdE2EReleaseError("prod_e2e_release_target_delivery_invalid")
    target_activation = _validate_activation_receipt(
        target.get("activation"),
        request=request,
        root=target,
        expected_entrypoint="shadow_promotion",
    )
    target = {**dict(target), "activation": target_activation}
    expected_fields = bundle_verification["field_values"]
    writes = body.get("field_writes")
    if (
        not isinstance(writes, list)
        or len(writes) != 2
        or [item.get("field_key") for item in writes if isinstance(item, Mapping)]
        != list(TARGET_FIELD_KEYS)
        or any(
            not isinstance(item, Mapping)
            or set(item)
            != {
                "field_key",
                "value_sha256",
                "value_utf8_bytes",
                "written_at",
            }
            or item.get("value_sha256")
            != expected_fields.get(item.get("field_key"), {}).get("sha256")
            or item.get("value_utf8_bytes")
            != expected_fields.get(item.get("field_key"), {}).get("utf8_bytes")
            or not terminal_at
            <= _timestamp(item.get("written_at"), field="field_written_at")
            <= observed_at
            for item in writes
        )
    ):
        raise ProdE2EReleaseError("prod_e2e_release_field_writes_invalid")
    comment = body.get("comment_write")
    if (
        not isinstance(comment, Mapping)
        or set(comment)
        != {
            "comment_id",
            "content_sha256",
            "content_utf8_bytes",
            "written_at",
            "attempt_terminal_outcome",
        }
        or not _required_text(comment.get("comment_id"), field="comment_id")
        or _sha256(comment.get("content_sha256"), field="comment_content_sha256")
        != comment.get("content_sha256")
        or comment.get("content_sha256")
        != bundle_verification["comment_content"]["sha256"]
        or comment.get("content_utf8_bytes")
        != bundle_verification["comment_content"]["utf8_bytes"]
        or comment.get("attempt_terminal_outcome") not in {"ack", "reconciled"}
    ):
        raise ProdE2EReleaseError("prod_e2e_release_comment_write_invalid")
    comment_at = _timestamp(comment.get("written_at"), field="comment_written_at")
    if comment_at < terminal_at or comment_at > observed_at:
        raise ProdE2EReleaseError("prod_e2e_release_comment_write_invalid")
    readback = body.get("official_readback")
    if (
        not isinstance(readback, Mapping)
        or set(readback)
        != {
            "adapter",
            "source",
            "scope",
            "observed_at",
            "fields",
            "comment_id",
            "comment_content_sha256",
            "marker_sha256",
            "marker_match_count",
        }
        or readback.get("adapter") != "MeegleIssueCommentAdapter.get_fields_and_comments"
        or readback.get("source") != "official_meegle_api"
        or readback.get("scope")
        != {
            "project_key": TARGET_PROJECT_KEY,
            "work_item_id": TARGET_WORK_ITEM_ID,
        }
        or readback.get("fields")
        != {
            item["field_key"]: {
                "value_sha256": item["value_sha256"],
                "value_utf8_bytes": item["value_utf8_bytes"],
            }
            for item in writes
        }
        or readback.get("comment_id") != comment.get("comment_id")
        or readback.get("comment_content_sha256") != comment.get("content_sha256")
        or readback.get("marker_sha256")
        != bundle_verification["marker"]["sha256"]
        or readback.get("marker_match_count") != 1
    ):
        raise ProdE2EReleaseError("prod_e2e_release_official_readback_invalid")
    readback_at = _timestamp(readback.get("observed_at"), field="readback_observed_at")
    if readback_at < max(
        [comment_at, *[
            _timestamp(item["written_at"], field="field_written_at")
            for item in writes
        ]]
    ) or readback_at > observed_at:
        raise ProdE2EReleaseError("prod_e2e_release_official_readback_invalid")
    host_identity = request["release_bom"]["host_runtime"]["canonical_final"]
    live_readback = _observe_official_meegle_readback_live(
        project_key=TARGET_PROJECT_KEY,
        work_item_id=TARGET_WORK_ITEM_ID,
        comment_id=str(comment["comment_id"]),
        effect_key=str(target["effect_key"]),
        artifact_set_id=str(target["artifact_set_id"]),
        host_commit=str(host_identity["commit"]),
        host_tree=str(host_identity["tree"]),
    )
    if (
        live_readback["fields"]
        != {
            item["field_key"]: {
                "sha256": item["value_sha256"],
                "utf8_bytes": item["value_utf8_bytes"],
            }
            for item in writes
        }
        or live_readback["comment_content"]
        != {
            "sha256": comment["content_sha256"],
            "utf8_bytes": comment["content_utf8_bytes"],
        }
        or live_readback["marker_sha256"]
        != bundle_verification["marker"]["sha256"]
        or live_readback["marker_match_count"] != 1
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_official_readback_live_mismatch"
        )
    lineage = body.get("delivery_lineage")
    if (
        not isinstance(lineage, Mapping)
        or set(lineage)
        != {
            "business_key",
            "submission_key",
            "task_id",
            "delivery_id",
            "effect_key",
            "semantic_payload_sha256",
            "artifact_set_id",
            "terminal_receipt_sha256",
            "field_keys",
            "comment_id",
            "attempt_terminal_outcome",
            "lineage_sha256",
        }
        or lineage.get("business_key") != TARGET_BUSINESS_KEY
        or lineage.get("submission_key") != target.get("submission_key")
        or lineage.get("task_id") != target.get("task_id")
        or any(
            lineage.get(field) != target.get(field)
            for field in (
                "delivery_id",
                "effect_key",
                "semantic_payload_sha256",
                "artifact_set_id",
            )
        )
        or lineage.get("terminal_receipt_sha256")
        != target.get("terminal_receipt_sha256")
        or lineage.get("field_keys") != list(TARGET_FIELD_KEYS)
        or lineage.get("comment_id") != comment.get("comment_id")
        or lineage.get("attempt_terminal_outcome")
        != comment.get("attempt_terminal_outcome")
        or lineage.get("lineage_sha256")
        != _sha256_value(
            {key: value for key, value in lineage.items() if key != "lineage_sha256"}
        )
    ):
        raise ProdE2EReleaseError("prod_e2e_release_delivery_lineage_invalid")
    canary = _validate_canary_remote_closeout(
        body.get("post_cutover_canary"),
        request=request,
        kafka_gate=canary_kafka_gate,
        earliest_observed_at=max(
            _timestamp(
                canary_kafka_gate["observed_at"],
                field="canary_kafka_gate_observed_at",
            ),
            terminal_at,
        ),
        completion_observed_at=observed_at,
    )
    if dict(effects) != {
        "live_database_mutated": True,
        "environment_written": True,
        "active_binding_written": True,
        "services_restarted": True,
        "viewer_proxy_deployed": True,
        "viewer_proxy_verified": True,
        "target_o650_executed": True,
        "feishu_written": True,
        "canary_executed": True,
    }:
        raise ProdE2EReleaseError("prod_e2e_release_completion_effects_invalid")
    live_database = _observe_live_delivery_database(
        request=request,
        target=target,
        canary=canary,
        comment_id=str(comment["comment_id"]),
        target_attempt_terminal_outcome=str(
            comment["attempt_terminal_outcome"]
        ),
        host_restart=host_restart,
        cutover_snapshot_path=Path(cutover_checkpoint["path"]),
    )
    return {
        "path": str(owned.path),
        "sha256": owned.sha256,
        "outcome": "success",
        "production_completed": True,
        "observed_at": observed_at.isoformat(),
        "target_event_uid": TARGET_EVENT_UID,
        "target_work_item_id": TARGET_WORK_ITEM_ID,
        "field_keys": list(TARGET_FIELD_KEYS),
        "comment_id": comment["comment_id"],
        "canary_event_uid": canary["event_uid"],
        "live_baseline": live_baseline,
        "live_database": live_database,
        "cutover_database_checkpoint": cutover_checkpoint,
        "official_readback": live_readback,
        "target_input_revalidation": dict(target_input_gate),
        "host_live_runtime": host_live_runtime,
        "viewer_proxy_live": viewer_proxy_live,
        "canary_kafka_gate": canary_kafka_gate,
    }


def _validate_db_cutover_binding(
    owned: OwnedJson,
    *,
    release_id: str,
    host_commit: str,
    host_tree: str,
) -> Mapping[str, Any]:
    body = owned.body
    expected_fields = {
        "schema_version",
        "release_id",
        "created_at",
        "target_live_db_path",
        "migration_receipt",
        "source_backup",
        "migrated_clone",
        "approved_source_logical_sha256",
        "approved_post_migration_logical_sha256",
        "migration_runtime_sha256",
        "quarantine_core",
        "quarantine_disposition",
        "cutover_contract",
        "production_mutation",
    }
    if (
        set(body) != expected_fields
        or body.get("schema_version") != DB_CUTOVER_BINDING_SCHEMA_VERSION
        or body.get("release_id") != release_id
        or body.get("production_mutation") is not False
        or body.get("target_live_db_path") != DELIVERY_DB_PATH
    ):
        raise ProdE2EReleaseError("prod_e2e_release_db_cutover_binding_invalid")
    _timestamp(body.get("created_at"), field="db_cutover_created_at")
    receipt_ref = body.get("migration_receipt")
    source_ref = body.get("source_backup")
    clone_ref = body.get("migrated_clone")
    core_ref = body.get("quarantine_core")
    disposition = body.get("quarantine_disposition")
    cutover = body.get("cutover_contract")
    if not all(
        isinstance(item, Mapping)
        for item in (
            receipt_ref,
            source_ref,
            clone_ref,
            core_ref,
            disposition,
            cutover,
        )
    ):
        raise ProdE2EReleaseError("prod_e2e_release_db_cutover_binding_invalid")

    if set(receipt_ref) != {"path", "sha256"}:
        raise ProdE2EReleaseError("prod_e2e_release_migration_receipt_ref_invalid")
    receipt_owned = _read_owned_json(
        Path(str(receipt_ref.get("path") or "")), artifact="migration_receipt"
    )
    receipt = receipt_owned.body
    source_projection = receipt.get("source_logical_projection")
    post_projection = receipt.get("post_migration_logical_projection")
    source_receipt = receipt.get("source_backup")
    if (
        receipt_owned.sha256 != receipt_ref.get("sha256")
        or receipt_owned.raw != _canonical_bytes(receipt, newline=True)
        or receipt.get("schema_version") != DELIVERY_MIGRATION_SCHEMA
        or receipt.get("source_schema_version") != DELIVERY_STORE_SOURCE_SCHEMA
        or receipt.get("target_schema_version") != DELIVERY_STORE_TARGET_SCHEMA
        or receipt.get("target_live_db_path") != DELIVERY_DB_PATH
        or receipt.get("no_live_database_writes") is not True
        or not isinstance(source_projection, Mapping)
        or not isinstance(post_projection, Mapping)
        or not isinstance(source_receipt, Mapping)
        or receipt.get("post_migration_health")
        != {"integrity_check": "ok", "foreign_key_violation_count": 0}
    ):
        raise ProdE2EReleaseError("prod_e2e_release_migration_receipt_invalid")

    source_path, source_sha256, source_size = _read_owned_blob(
        Path(str(source_ref.get("path") or "")), artifact="source_backup"
    )
    clone_path, clone_sha256, clone_size = _read_owned_blob(
        Path(str(clone_ref.get("path") or "")), artifact="migrated_clone"
    )
    if (
        set(source_ref) != {"path", "sha256", "size_bytes"}
        or set(clone_ref) != {"path", "sha256", "size_bytes"}
        or str(source_path) != source_ref.get("path")
        or source_sha256 != source_ref.get("sha256")
        or source_size != source_ref.get("size_bytes")
        or str(clone_path) != clone_ref.get("path")
        or clone_sha256 != clone_ref.get("sha256")
        or clone_size != clone_ref.get("size_bytes")
        or dict(source_receipt)
        != {
            "path": str(source_path),
            "sha256": source_sha256,
            "size_bytes": source_size,
        }
    ):
        raise ProdE2EReleaseError("prod_e2e_release_db_artifact_binding_invalid")

    source_logical = _sha256(
        source_projection.get("logical_sha256"), field="source_logical_sha256"
    )
    post_logical = _sha256(
        post_projection.get("logical_sha256"), field="post_logical_sha256"
    )
    runtime_sha256 = _sha256(
        receipt.get("migration_runtime_sha256"), field="migration_runtime_sha256"
    )
    if (
        body.get("approved_source_logical_sha256") != source_logical
        or body.get("approved_post_migration_logical_sha256") != post_logical
        or body.get("migration_runtime_sha256") != runtime_sha256
    ):
        raise ProdE2EReleaseError("prod_e2e_release_db_digest_binding_invalid")

    if set(core_ref) != {"path", "file_sha256", "core_sha256"}:
        raise ProdE2EReleaseError("prod_e2e_release_quarantine_core_ref_invalid")
    core_owned = _read_owned_json(
        Path(str(core_ref.get("path") or "")), artifact="quarantine_core"
    )
    core = core_owned.body
    core_body = {key: value for key, value in core.items() if key != "core_sha256"}
    snapshot = core.get("quarantine_snapshot")
    migration_binding = core.get("migration_binding")
    adjudication = core.get("invalid_manual_thread_adjudication")
    event_projection = core.get("quarantine_event_projection")
    db_identity = core.get("control_db")
    if (
        core_owned.sha256 != core_ref.get("file_sha256")
        or core_owned.raw != _canonical_bytes(core, newline=True)
        or core.get("schema_version") != DELIVERY_QUARANTINE_CORE_SCHEMA
        or core.get("release_id") != release_id
        or core.get("core_sha256") != core_ref.get("core_sha256")
        or core.get("core_sha256") != _sha256_value(core_body)
        or not isinstance(snapshot, Mapping)
        or snapshot.get("counts") != QUARANTINE_COUNTS
        or snapshot.get("snapshot_sha256")
        != _sha256_value(
            {
                "digest_algorithm": snapshot.get("digest_algorithm"),
                "counts": snapshot.get("counts"),
                "row_set_sha256": snapshot.get("row_set_sha256"),
            }
        )
        or not isinstance(migration_binding, Mapping)
        or migration_binding
        != {
            "receipt_path": str(receipt_owned.path),
            "receipt_sha256": receipt_owned.sha256,
            "source_backup_sha256": source_sha256,
            "source_logical_sha256": source_logical,
            "post_migration_logical_sha256": post_logical,
            "migration_runtime_sha256": runtime_sha256,
            "target_live_db_path": DELIVERY_DB_PATH,
        }
        or not isinstance(db_identity, Mapping)
        or db_identity.get("path") != DELIVERY_DB_PATH
        or db_identity.get("delivery_schema_version")
        != DELIVERY_STORE_TARGET_SCHEMA
        or not isinstance(adjudication, Mapping)
        or adjudication.get("proposed_disposition") != "retain_terminal_no_rearm"
        or adjudication.get("count") != QUARANTINE_COUNTS["subscriptions"]
        or not isinstance(event_projection, Mapping)
        or event_projection.get("entity_counts") != QUARANTINE_COUNTS
        or core.get("issuance_policy")
        != {
            "requires_approval_evidence": True,
            "approval_decision": "approved",
            "bom_binding": "quarantine_core_sha256",
            "active_binding": "final_baseline_file_sha256",
            "no_database_mutation": True,
            "no_rearm": True,
        }
    ):
        raise ProdE2EReleaseError("prod_e2e_release_quarantine_core_invalid")

    expected_disposition = {
        "mode": "lifetime_terminal_quarantine",
        "counts": dict(QUARANTINE_COUNTS),
        "proposed_disposition": "retain_terminal_no_rearm",
        "retry": False,
        "delete": False,
        "rearm": False,
    }
    expected_cutover = {
        "writer_stop_required": True,
        "fresh_live_backup_required": True,
        "fresh_live_pre_digest_must_equal_approved_source": True,
        "fresh_live_post_digest_must_equal_approved_post": True,
        "fresh_live_core_must_equal_approved_core": True,
        "environment_update_after_post_and_core_only": True,
        "active_binding_update_after_post_and_core_only": True,
        "restore_source_backup_before_environment_or_binding_on_failure": True,
        "partial_activation_forbidden": True,
    }
    if disposition != expected_disposition or cutover != expected_cutover:
        raise ProdE2EReleaseError("prod_e2e_release_db_cutover_contract_invalid")

    canonical_validation = _run_canonical_db_validator(
        receipt_path=receipt_owned.path,
        receipt_sha256=receipt_owned.sha256,
        source_path=source_path,
        clone_path=clone_path,
        migration_runtime_sha256=runtime_sha256,
        core_path=core_owned.path,
        release_id=release_id,
        host_commit=host_commit,
        host_tree=host_tree,
    )
    canonical_binding = canonical_validation.get("binding")
    if (
        canonical_validation.get("core_sha256") != core.get("core_sha256")
        or not isinstance(canonical_binding, Mapping)
        or canonical_binding.get("receipt_path") != str(receipt_owned.path)
        or canonical_binding.get("receipt_sha256") != receipt_owned.sha256
        or canonical_binding.get("source_backup_sha256") != source_sha256
        or canonical_binding.get("source_logical_sha256") != source_logical
        or canonical_binding.get("post_migration_logical_sha256") != post_logical
        or canonical_binding.get("migration_runtime_sha256") != runtime_sha256
        or canonical_binding.get("target_live_db_path") != DELIVERY_DB_PATH
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_canonical_db_validation_binding_mismatch"
        )

    return {
        "path": str(owned.path),
        "sha256": owned.sha256,
        "target_live_db_path": DELIVERY_DB_PATH,
        "source_schema_version": DELIVERY_STORE_SOURCE_SCHEMA,
        "target_schema_version": DELIVERY_STORE_TARGET_SCHEMA,
        "migration_receipt": {
            "path": str(receipt_owned.path),
            "sha256": receipt_owned.sha256,
        },
        "source_backup": dict(source_ref),
        "migrated_clone": dict(clone_ref),
        "approved_source_logical_sha256": source_logical,
        "approved_post_migration_logical_sha256": post_logical,
        "migration_runtime_sha256": runtime_sha256,
        "quarantine_core": {
            "path": str(core_owned.path),
            "file_sha256": core_owned.sha256,
            "core_sha256": str(core["core_sha256"]),
            "snapshot_sha256": str(snapshot["snapshot_sha256"]),
        },
        "quarantine_disposition": expected_disposition,
        "cutover_contract": expected_cutover,
        "baseline_schema_version": DELIVERY_BASELINE_SCHEMA,
        "canonical_validation": {
            "schema_version": canonical_validation["schema_version"],
            "validator_script_sha256": canonical_validation[
                "validator_script_sha256"
            ],
            "migration_module_sha256": canonical_validation[
                "migration_module_sha256"
            ],
            "baseline_module_sha256": canonical_validation[
                "baseline_module_sha256"
            ],
            "host_commit": canonical_validation["host_commit"],
            "host_tree": canonical_validation["host_tree"],
            "interpreter_path": canonical_validation["interpreter_path"],
            "interpreter_sha256": canonical_validation["interpreter_sha256"],
            "core_exact_recomputed": True,
            "source_clone_distinct": True,
            "clone_live_distinct": True,
        },
    }


def _validate_candidate_observation(
    owned: OwnedJson,
    *,
    release_id: str,
    expected_phase: str,
    expected_root: str | None,
    now: datetime,
    require_fresh: bool,
) -> Mapping[str, Any]:
    body = owned.body
    expected_fields = {
        "schema_version",
        "release_id",
        "phase",
        "observed_at",
        "root",
        "head",
        "tree",
        "status_porcelain_sha256",
        "detached",
        "git_self_contained",
        "git_storage",
        "filesystem",
        "seal",
        "production_mutation",
        "docker_started",
        "mcap_started",
    }
    if set(body) != expected_fields:
        raise ProdE2EReleaseError("prod_e2e_release_candidate_observation_shape_invalid")
    root = _absolute_remote(body.get("root"), field="candidate_root")
    observed_at = _timestamp(body.get("observed_at"), field="candidate_observed_at")
    storage = body.get("git_storage")
    filesystem = body.get("filesystem")
    seal = body.get("seal")
    if (
        body.get("schema_version") != CANDIDATE_OBSERVATION_SCHEMA_VERSION
        or body.get("release_id") != release_id
        or body.get("phase") != expected_phase
        or body.get("head") != PIPELINE_COMMIT
        or body.get("tree") != PIPELINE_TREE
        or body.get("status_porcelain_sha256") != EMPTY_SHA256
        or body.get("detached") is not True
        or body.get("git_self_contained") is not True
        or body.get("production_mutation") is not False
        or body.get("docker_started") is not False
        or body.get("mcap_started") is not False
        or not isinstance(storage, Mapping)
        or set(storage) != {
            "dot_git_kind",
            "git_dir",
            "git_common_dir",
            "self_contained",
        }
        or storage.get("dot_git_kind") != "directory"
        or storage.get("git_dir") != str(PurePosixPath(root) / ".git")
        or storage.get("git_common_dir") != str(PurePosixPath(root) / ".git")
        or storage.get("self_contained") is not True
        or not isinstance(filesystem, Mapping)
        or set(filesystem) != {"type", "mount_target"}
        or not isinstance(seal, Mapping)
        or set(seal) != {"read_only", "write_probe_blocked"}
    ):
        raise ProdE2EReleaseError("prod_e2e_release_candidate_observation_invalid")
    if expected_root is not None and root != expected_root:
        raise ProdE2EReleaseError("prod_e2e_release_candidate_root_mismatch")
    if require_fresh and (
        observed_at > now + MAX_FUTURE_SKEW
        or now - observed_at > MAX_FINAL_OBSERVATION_AGE
    ):
        raise ProdE2EReleaseError("prod_e2e_release_candidate_observation_stale")
    fs_type = str(filesystem.get("type") or "").lower()
    if expected_phase == "staging":
        if (
            not root.startswith("/mnt/tmp/")
            or fs_type not in {"cifs", "smb3"}
            or seal
            != {"read_only": False, "write_probe_blocked": False}
        ):
            raise ProdE2EReleaseError("prod_e2e_release_staging_candidate_invalid")
    elif expected_phase == "final":
        if (
            root.startswith("/mnt/tmp/")
            or fs_type != "ext4"
            or seal != {"read_only": True, "write_probe_blocked": True}
        ):
            raise ProdE2EReleaseError("prod_e2e_release_final_candidate_unsealed")
    else:
        raise ProdE2EReleaseError("prod_e2e_release_candidate_phase_invalid")
    live_observation = None
    if require_fresh:
        live_observation = _observe_vm_candidate_live(root)
        for field in (
            "root",
            "head",
            "tree",
            "status_porcelain_sha256",
            "detached",
            "git_self_contained",
            "git_storage",
            "filesystem",
            "seal",
        ):
            if live_observation.get(field) != body.get(field):
                raise ProdE2EReleaseError(
                    "prod_e2e_release_candidate_live_observation_mismatch"
                )
    observer_script_sha256 = hashlib.sha256(
        _VM_CANDIDATE_PROBE.replace("__ROOT__", repr(root)).encode("utf-8")
    ).hexdigest()
    result = {
        "path": str(owned.path),
        "sha256": owned.sha256,
        "observed_at": observed_at.isoformat(),
        "root": root,
        "head": PIPELINE_COMMIT,
        "tree": PIPELINE_TREE,
        "phase": expected_phase,
        "filesystem_type": fs_type,
        "seal": dict(seal),
        "live_observer": {
            "transport": "ssh-mini-agent",
            "host": "mini@192.168.26.174",
            "script_sha256": observer_script_sha256,
        },
    }
    if (
        live_observation is not None
        and live_observation["observer_script_sha256"] != observer_script_sha256
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_candidate_live_observer_identity_mismatch"
        )
    return result


def _machine_identity() -> Mapping[str, Any]:
    for source, candidate in (
        ("etc_machine_id", Path("/etc/machine-id")),
        ("dbus_machine_id", Path("/var/lib/dbus/machine-id")),
    ):
        try:
            info = os.lstat(candidate)
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
                continue
            value = candidate.read_text(encoding="ascii").strip()
        except (OSError, UnicodeError):
            continue
        if re.fullmatch(r"[A-Za-z0-9-]{16,128}", value):
            return {
                "source": source,
                "sha256": hashlib.sha256(
                    f"{source}\0{value}".encode("utf-8")
                ).hexdigest(),
            }
    if platform.system() == "Darwin":
        try:
            completed = subprocess.run(
                ["/usr/sbin/ioreg", "-rd1", "-c", "IOPlatformExpertDevice"],
                check=False,
                capture_output=True,
                text=True,
                timeout=3,
            )
        except (OSError, subprocess.SubprocessError):
            completed = None
        if completed is not None and completed.returncode == 0:
            match = re.search(
                r'"IOPlatformUUID"\s*=\s*"([A-Fa-f0-9-]{16,64})"',
                str(completed.stdout or ""),
            )
            if match:
                source = "darwin_ioplatformuuid"
                return {
                    "source": source,
                    "sha256": hashlib.sha256(
                        f"{source}\0{match.group(1).lower()}".encode("utf-8")
                    ).hexdigest(),
                }
    raise ProdE2EReleaseError("prod_e2e_release_machine_identity_unavailable")


def _approval_identity(
    machine_identity: Mapping[str, Any],
) -> Mapping[str, Any]:
    source = str(machine_identity.get("source") or "")
    digest = _sha256(
        machine_identity.get("sha256"), field="machine_identity_sha256"
    )
    if re.fullmatch(r"[a-z][a-z0-9_]{2,63}", source) is None:
        raise ProdE2EReleaseError("prod_e2e_release_machine_identity_invalid")
    try:
        username = pwd.getpwuid(os.geteuid()).pw_name
    except KeyError as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_local_identity_unavailable"
        ) from exc
    return {
        "schema_version": RELEASE_APPROVAL_IDENTITY_SCHEMA_VERSION,
        "method": RELEASE_APPROVAL_IDENTITY_METHOD,
        "uid": os.geteuid(),
        "username": username,
        "machine_identity_source": source,
        "machine_identity_sha256": digest,
    }


def _build_bom(
    *,
    release_id: str,
    bootstrap_epoch_id: str,
    final_candidate_root: str,
    gap: Mapping[str, Any],
    preread: Mapping[str, Any],
    input_preread: Mapping[str, Any],
    closure: Mapping[str, Any],
    staging: Mapping[str, Any],
    cross_contract: Mapping[str, Any],
    components: Mapping[str, Any],
    db_cutover: Mapping[str, Any],
) -> Mapping[str, Any]:
    return {
        "schema_version": BOM_SCHEMA_VERSION,
        "release_id": release_id,
        "tooling_boundary": {
            "role": "owner_only_plan_and_validate_artifact",
            "canonical_host_installable": False,
            "cherry_pick_into_canonical_forbidden": True,
            "retired_executor_restoration_forbidden": True,
            "production_executor_included": False,
            "production_mutation_supported": False,
        },
        "host_runtime": {
            "canonical_final": dict(components["host"]),
            "quarantine_baseline_commit": HOST_QUARANTINE_BASELINE_COMMIT,
            "quarantine_baseline_required_files": list(
                components["host"]["runtime_allowlists"]["union"]
            ),
            "delivery_store_schema_version": DELIVERY_STORE_TARGET_SCHEMA,
        },
        "component_identities": {
            "evidence_path": components["path"],
            "evidence_sha256": components["sha256"],
            "observed_at": components["observed_at"],
            "workspace": dict(components["workspace"]),
            "worker": dict(components["worker"]),
            "pipeline": dict(components["pipeline"]),
            "viewer_proxy": dict(components["viewer_proxy"]),
        },
        "admission_security": dict(components["admission_security"]),
        "pipeline": {
            "commit": PIPELINE_COMMIT,
            "tree": PIPELINE_TREE,
            "entrypoint": PIPELINE_ENTRYPOINT,
            "source_candidate_root": PIPELINE_SOURCE_ROOT,
            "source_closure_evidence": dict(closure),
            "staging_candidate": dict(staging),
            "final_candidate_root": final_candidate_root,
            "final_candidate_requirements": {
                "filesystem_type": "ext4",
                "self_contained_git": True,
                "detached_head": True,
                "tree_clean": True,
                "status_porcelain_sha256": EMPTY_SHA256,
                "read_only": True,
                "write_probe_blocked": True,
            },
            "raw_mcap_execution_reachable": False,
            "remote_reader_contract_reachable": True,
            "artifact_transport": {
                "mode": "manifest_bound_path_safe_proxy_only",
                "public_route_pattern": (
                    "/g1q3-rca-artifacts/v1/"
                    "<submission_key>/<same_submission_key>.viz.mcap"
                ),
                "same_submission_directory_and_filename_required": True,
                "delivery_manifest_binding_required": True,
                "legacy_foxglove_http_forbidden": True,
                "legacy_mcap_path_parameter_forbidden": True,
                "legacy_get_mcap_metadata_forbidden": True,
                "legacy_backend_reachable": False,
                "silent_fallback_authorized": False,
                "security_details_visibility": "owner_only",
            },
        },
        "kafka_scope": {
            "topic": TOPIC,
            "partition": PARTITION,
            "live_t0_offset": LIVE_T0_OFFSET,
            "gap_ledger": dict(gap),
            "authorization_mode": "single_exact_event",
            "immediate_backfill_event_uid": TARGET_EVENT_UID,
            "immediate_backfill_raw_sha256": TARGET_RAW_SHA256,
            "bulk_pre_t0_backfill_authorized": False,
            "deferred_missing_count": DEFERRED_MISSING_COUNT,
        },
        "feishu_completion": {
            "project_key": TARGET_PROJECT_KEY,
            "work_item_id": TARGET_WORK_ITEM_ID,
            "input_preread": dict(input_preread),
            "input_gate_required_for_canary": True,
            "field_preread": dict(preread),
            "required_nonempty_field_keys": list(TARGET_FIELD_KEYS),
            "required_new_evidence_comment_count": 1,
            "official_readback_required": True,
            "post_cutover_canary_official_readback_required": True,
            "exact_remote_marker_match_count": 1,
            "delivery_lineage_required": True,
            "completion_receipt_schema": COMPLETION_RECEIPT_SCHEMA_VERSION,
            "exact_target_and_canary_field_comment_readback_required": True,
            "terminal_cross_contract_pass": dict(cross_contract),
        },
        "delivery_store_cutover": dict(db_cutover),
        "quarantine_baseline_approval": {
            "schema_version": BASELINE_APPROVAL_SCHEMA_VERSION,
            "decision": BASELINE_APPROVAL_DECISION,
            "release_id": release_id,
            "quarantine_core_sha256": db_cutover["quarantine_core"][
                "core_sha256"
            ],
            "release_bom_sha256_binding_required": True,
            "distinct_from_prod_e2e_approval": True,
            "active_release_binding_approval_sha256_source": (
                "quarantine_baseline_approval"
            ),
        },
        "restart_scope": {
            "host_launchd_labels": list(HOST_SERVICE_LABELS),
            "host_service_count": len(HOST_SERVICE_LABELS),
            "vm_systemd_units": list(VM_SERVICE_UNITS),
            "vm_service_count": len(VM_SERVICE_UNITS),
            "viewer_proxy_public_origin": components["viewer_proxy"][
                "public_origin"
            ],
            "viewer_proxy_config_sha256": components["viewer_proxy"][
                "config"
            ]["sha256"],
            "viewer_proxy_reload_required": True,
            "viewer_proxy_rollback_required_on_failure": True,
            "stop_before_delivery_db_cutover": True,
            "restart_after_database_baseline_environment_and_binding": True,
            "partial_restart_forbidden": True,
            "execution_preflight_schema": EXECUTION_PREFLIGHT_SCHEMA_VERSION,
            "fresh_live_projection_must_equal_backup_and_approved_source": True,
        },
        "bootstrap_authorization": {
            "schema_version": prod_bootstrap.SCHEMA_VERSION,
            "resource_class": prod_bootstrap.RESOURCE_CLASS,
            "capacity_mode": prod_bootstrap.CAPACITY_MODE,
            "bootstrap_epoch_id": bootstrap_epoch_id,
            "issuance_phase": "external_after_bom_and_release_approval",
            "new_receipt_required": True,
            "prior_receipt_reuse_forbidden": True,
            "release_bom_sha256_binding_required": True,
            "release_approval_binding_required": True,
            "max_concurrency": prod_bootstrap.MAX_CONCURRENCY,
            "daily_started_attempt_quota": prod_bootstrap.DAILY_STARTED_ATTEMPT_QUOTA,
            "input_materialization": "forbidden",
        },
        "completion_order": [
            "exact_six_writers_stopped",
            "fresh_live_pre_digest_matches_approved_source",
            "delivery_store_v7_installed",
            "fresh_live_post_digest_matches_approved_post",
            "live_quarantine_core_matches_approved_core",
            "quarantine_baseline_issued_and_bound",
            "environment_and_active_binding_committed",
            "exact_six_host_services_and_vm_services_restarted",
            "vm_report_upstream_contract_verified",
            "viewer_proxy_reloaded_dns_tls_http_and_browser_verified",
            "target_input_gate_revalidated",
            "exact_o650_ingested",
            "rca_terminal_success",
            "two_attribution_fields_written",
            "evidence_comment_written",
            "official_fields_and_comment_read_back",
            "delivery_lineage_closed",
            "new_post_cutover_kafka_canary_fields_comment_and_marker_read_back",
        ],
        "production_effects_executed": False,
    }


def _validate_bom(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {
        "schema_version",
        "release_id",
        "tooling_boundary",
        "host_runtime",
        "component_identities",
        "admission_security",
        "pipeline",
        "kafka_scope",
        "feishu_completion",
        "delivery_store_cutover",
        "quarantine_baseline_approval",
        "restart_scope",
        "bootstrap_authorization",
        "completion_order",
        "production_effects_executed",
    }:
        raise ProdE2EReleaseError("prod_e2e_release_bom_shape_invalid")
    release_id = _release_id(value.get("release_id"))
    pipeline = value.get("pipeline")
    kafka = value.get("kafka_scope")
    feishu = value.get("feishu_completion")
    bootstrap = value.get("bootstrap_authorization")
    component_descriptor = value.get("component_identities")
    db_descriptor = value.get("delivery_store_cutover")
    if not all(
        isinstance(item, Mapping)
        for item in (
            pipeline,
            kafka,
            feishu,
            bootstrap,
            component_descriptor,
            db_descriptor,
        )
    ):
        raise ProdE2EReleaseError("prod_e2e_release_bom_shape_invalid")
    gap_descriptor = kafka.get("gap_ledger")
    preread_descriptor = feishu.get("field_preread")
    input_preread_descriptor = feishu.get("input_preread")
    closure_descriptor = pipeline.get("source_closure_evidence")
    staging_descriptor = pipeline.get("staging_candidate")
    cross_contract_descriptor = feishu.get("terminal_cross_contract_pass")
    if not all(
        isinstance(item, Mapping)
        for item in (
            gap_descriptor,
            preread_descriptor,
            input_preread_descriptor,
            closure_descriptor,
            staging_descriptor,
            cross_contract_descriptor,
        )
    ):
        raise ProdE2EReleaseError("prod_e2e_release_bom_evidence_invalid")
    gap = _validate_gap_ledger(
        _read_owned_json(Path(str(gap_descriptor.get("path") or "")), artifact="gap_ledger")
    )
    preread = _validate_field_preread(
        _read_owned_json(Path(str(preread_descriptor.get("path") or "")), artifact="field_preread")
    )
    input_preread = _validate_input_preread(
        _read_owned_json(
            Path(str(input_preread_descriptor.get("path") or "")),
            artifact="input_preread",
        )
    )
    closure = _validate_closure_audit(
        _read_owned_json(Path(str(closure_descriptor.get("path") or "")), artifact="closure_audit")
    )
    staging_owned = _read_owned_json(
        Path(str(staging_descriptor.get("path") or "")), artifact="staging_observation"
    )
    staging = _validate_candidate_observation(
        staging_owned,
        release_id=release_id,
        expected_phase="staging",
        expected_root=str(staging_descriptor.get("root") or ""),
        now=datetime.max.replace(tzinfo=timezone.utc),
        require_fresh=False,
    )
    components = _validate_component_binding(
        _read_owned_json(
            Path(str(component_descriptor.get("evidence_path") or "")),
            artifact="component_binding",
        ),
        release_id=release_id,
        now=datetime.max.replace(tzinfo=timezone.utc),
        require_fresh=False,
    )
    cross_contract = _validate_cross_contract_pass(
        _read_owned_json(
            Path(str(cross_contract_descriptor.get("path") or "")),
            artifact="cross_contract_pass",
        ),
        release_id=release_id,
        components=components,
    )
    db_cutover = _validate_db_cutover_binding(
        _read_owned_json(
            Path(str(db_descriptor.get("path") or "")),
            artifact="db_cutover_binding",
        ),
        release_id=release_id,
        host_commit=components["host"]["commit"],
        host_tree=components["host"]["tree"],
    )
    final_root = _absolute_remote(
        pipeline.get("final_candidate_root"), field="final_candidate_root"
    )
    epoch_id = str(bootstrap.get("bootstrap_epoch_id") or "")
    expected = _build_bom(
        release_id=release_id,
        bootstrap_epoch_id=epoch_id,
        final_candidate_root=final_root,
        gap=gap,
        preread=preread,
        input_preread=input_preread,
        closure=closure,
        staging=staging,
        cross_contract=cross_contract,
        components=components,
        db_cutover=db_cutover,
    )
    if dict(value) != expected:
        raise ProdE2EReleaseError("prod_e2e_release_bom_binding_mismatch")
    return expected


def build_request(
    *,
    release_id: str,
    bootstrap_epoch_id: str,
    final_candidate_root: str,
    gap_ledger_path: Path,
    field_preread_path: Path,
    input_preread_path: Path,
    closure_audit_path: Path,
    staging_observation_path: Path,
    cross_contract_pass_path: Path,
    component_binding_path: Path,
    db_cutover_binding_path: Path,
    output_dir: Path,
    now: datetime | None = None,
    machine_identity_provider: MachineIdentityProvider = _machine_identity,
) -> Mapping[str, Any]:
    current = _now(now)
    normalized_release_id = _release_id(release_id)
    epoch_id = str(bootstrap_epoch_id or "").strip()
    if prod_bootstrap.EPOCH_ID_RE.fullmatch(epoch_id) is None:
        raise ProdE2EReleaseError("prod_e2e_release_bootstrap_epoch_id_invalid")
    final_root = _absolute_remote(final_candidate_root, field="final_candidate_root")
    if final_root.startswith("/mnt/tmp/") or final_root == PIPELINE_SOURCE_ROOT:
        raise ProdE2EReleaseError("prod_e2e_release_final_candidate_root_invalid")
    output_root = _owner_only_directory(output_dir, artifact="output")
    gap = _validate_gap_ledger(
        _read_owned_json(gap_ledger_path, artifact="gap_ledger")
    )
    preread = _validate_field_preread(
        _read_owned_json(field_preread_path, artifact="field_preread"),
        now=current,
        require_fresh=True,
    )
    input_preread = _validate_input_preread(
        _read_owned_json(input_preread_path, artifact="input_preread"),
        now=current,
        require_fresh=True,
    )
    closure = _validate_closure_audit(
        _read_owned_json(closure_audit_path, artifact="closure_audit")
    )
    staging = _validate_candidate_observation(
        _read_owned_json(staging_observation_path, artifact="staging_observation"),
        release_id=normalized_release_id,
        expected_phase="staging",
        expected_root=None,
        now=current,
        require_fresh=True,
    )
    components = _validate_component_binding(
        _read_owned_json(component_binding_path, artifact="component_binding"),
        release_id=normalized_release_id,
        now=current,
        require_fresh=True,
    )
    cross_contract = _validate_cross_contract_pass(
        _read_owned_json(
            cross_contract_pass_path, artifact="cross_contract_pass"
        ),
        release_id=normalized_release_id,
        components=components,
    )
    db_cutover = _validate_db_cutover_binding(
        _read_owned_json(db_cutover_binding_path, artifact="db_cutover_binding"),
        release_id=normalized_release_id,
        host_commit=components["host"]["commit"],
        host_tree=components["host"]["tree"],
    )
    bom = _build_bom(
        release_id=normalized_release_id,
        bootstrap_epoch_id=epoch_id,
        final_candidate_root=final_root,
        gap=gap,
        preread=preread,
        input_preread=input_preread,
        closure=closure,
        staging=staging,
        cross_contract=cross_contract,
        components=components,
        db_cutover=db_cutover,
    )
    bom_sha256 = _sha256_value(bom)
    bom_path = output_root / "release-bom.json"
    request_path = output_root / "approval-request.json"
    bom_payload = _canonical_bytes(bom, newline=True)
    bom_file_sha256 = _publish_no_clobber(bom_path, bom)
    bom_artifact = {
        "path": str(bom_path),
        "semantic_sha256": bom_sha256,
        "file_sha256": bom_file_sha256,
        "bytes": len(bom_payload),
        "mode": "0600",
    }
    identity = _approval_identity(machine_identity_provider())
    request = {
        "schema_version": REQUEST_SCHEMA_VERSION,
        "release_id": normalized_release_id,
        "created_at": current.isoformat(),
        "production_effects_executed": False,
        "approval_required_for_finalize": True,
        "approval_decision": APPROVAL_DECISION,
        "approval_identity_requirement": identity,
        "quarantine_baseline_approval_requirement": {
            "schema_version": BASELINE_APPROVAL_SCHEMA_VERSION,
            "decision": BASELINE_APPROVAL_DECISION,
            "expected_release_id": normalized_release_id,
            "expected_release_bom_sha256": bom_sha256,
            "expected_quarantine_core_sha256": db_cutover["quarantine_core"][
                "core_sha256"
            ],
            "identity": {
                "uid": identity["uid"],
                "username": identity["username"],
            },
            "must_be_distinct_from_prod_e2e_approval": True,
            "must_not_be_generated_by_this_tool": True,
        },
        "action_set": list(ACTION_SET),
        "action_set_sha256": _sha256_value(list(ACTION_SET)),
        "bindings": {
            "release_bom": bom,
            "release_bom_sha256": bom_sha256,
            "release_bom_artifact": bom_artifact,
        },
        "bootstrap_authorization_requirement": {
            "issuance_phase": "external_after_bom_and_release_approval",
            "must_not_be_generated_by_this_tool": True,
            "expected_bootstrap_epoch_id": epoch_id,
            "expected_release_bom_sha256": bom_sha256,
        },
        "side_effect_contract": {
            "live_files_written": False,
            "services_restarted": False,
            "kafka_offsets_mutated": False,
            "live_database_mutated": False,
            "feishu_writes": False,
            "vm_production_paths_written": False,
            "mcap_started": False,
            "docker_started": False,
            "tooling_deployed": False,
            "retired_executor_restored": False,
            "output_scope": "unique_owner_only_release_directory",
        },
    }
    request_file_sha256 = _publish_no_clobber(request_path, request)
    return {
        "schema_version": "pnc_rca_prod_e2e_release_build_result_v1",
        "ok": True,
        "release_id": normalized_release_id,
        "release_bom_path": str(bom_path),
        "release_bom_sha256": bom_sha256,
        "release_bom_file_sha256": bom_file_sha256,
        "approval_request_path": str(request_path),
        "approval_request_sha256": request_file_sha256,
        "production_effects_executed": False,
    }


def _validate_request(
    owned: OwnedJson,
    *,
    machine_identity_provider: MachineIdentityProvider,
) -> Mapping[str, Any]:
    request = owned.body
    if set(request) != {
        "schema_version",
        "release_id",
        "created_at",
        "production_effects_executed",
        "approval_required_for_finalize",
        "approval_decision",
        "approval_identity_requirement",
        "quarantine_baseline_approval_requirement",
        "action_set",
        "action_set_sha256",
        "bindings",
        "bootstrap_authorization_requirement",
        "side_effect_contract",
    }:
        raise ProdE2EReleaseError("prod_e2e_release_request_shape_invalid")
    release_id = _release_id(request.get("release_id"))
    bindings = request.get("bindings")
    if (
        request.get("schema_version") != REQUEST_SCHEMA_VERSION
        or request.get("production_effects_executed") is not False
        or request.get("approval_required_for_finalize") is not True
        or request.get("approval_decision") != APPROVAL_DECISION
        or request.get("action_set") != list(ACTION_SET)
        or request.get("action_set_sha256") != _sha256_value(list(ACTION_SET))
        or not isinstance(bindings, Mapping)
        or set(bindings)
        != {"release_bom", "release_bom_sha256", "release_bom_artifact"}
    ):
        raise ProdE2EReleaseError("prod_e2e_release_request_invalid")
    bom = _validate_bom(bindings.get("release_bom"))
    bom_sha256 = _sha256_value(bom)
    bom_artifact = bindings.get("release_bom_artifact")
    if not isinstance(bom_artifact, Mapping) or set(bom_artifact) != {
        "path",
        "semantic_sha256",
        "file_sha256",
        "bytes",
        "mode",
    }:
        raise ProdE2EReleaseError(
            "prod_e2e_release_request_bom_artifact_invalid"
        )
    bom_owned = _read_owned_json(
        Path(str(bom_artifact.get("path") or "")), artifact="release_bom"
    )
    if (
        bindings.get("release_bom_sha256") != bom_sha256
        or bom_artifact.get("semantic_sha256") != bom_sha256
        or bom_artifact.get("file_sha256") != bom_owned.sha256
        or bom_artifact.get("bytes") != len(bom_owned.raw)
        or bom_artifact.get("mode") != "0600"
        or bom_owned.raw != _canonical_bytes(bom, newline=True)
        or bom_owned.body != bom
    ):
        raise ProdE2EReleaseError("prod_e2e_release_request_bom_sha_mismatch")
    identity = _approval_identity(machine_identity_provider())
    if request.get("approval_identity_requirement") != identity:
        raise ProdE2EReleaseError("prod_e2e_release_request_identity_mismatch")
    baseline_approval = request.get("quarantine_baseline_approval_requirement")
    if baseline_approval != {
        "schema_version": BASELINE_APPROVAL_SCHEMA_VERSION,
        "decision": BASELINE_APPROVAL_DECISION,
        "expected_release_id": release_id,
        "expected_release_bom_sha256": bom_sha256,
        "expected_quarantine_core_sha256": bom["delivery_store_cutover"][
            "quarantine_core"
        ]["core_sha256"],
        "identity": {"uid": identity["uid"], "username": identity["username"]},
        "must_be_distinct_from_prod_e2e_approval": True,
        "must_not_be_generated_by_this_tool": True,
    }:
        raise ProdE2EReleaseError(
            "prod_e2e_release_request_baseline_approval_contract_invalid"
        )
    bootstrap = request.get("bootstrap_authorization_requirement")
    if bootstrap != {
        "issuance_phase": "external_after_bom_and_release_approval",
        "must_not_be_generated_by_this_tool": True,
        "expected_bootstrap_epoch_id": bom["bootstrap_authorization"]["bootstrap_epoch_id"],
        "expected_release_bom_sha256": bom_sha256,
    }:
        raise ProdE2EReleaseError("prod_e2e_release_request_bootstrap_contract_invalid")
    if request.get("side_effect_contract") != {
        "live_files_written": False,
        "services_restarted": False,
        "kafka_offsets_mutated": False,
        "live_database_mutated": False,
        "feishu_writes": False,
        "vm_production_paths_written": False,
        "mcap_started": False,
        "docker_started": False,
        "tooling_deployed": False,
        "retired_executor_restored": False,
        "output_scope": "unique_owner_only_release_directory",
    }:
        raise ProdE2EReleaseError("prod_e2e_release_request_side_effect_contract_invalid")
    return {
        "release_id": release_id,
        "created_at": _timestamp(request.get("created_at"), field="request_created_at"),
        "release_bom": bom,
        "release_bom_sha256": bom_sha256,
        "request_sha256": owned.sha256,
        "identity": identity,
    }


def _validate_approval(
    owned: OwnedJson,
    *,
    request: Mapping[str, Any],
    now: datetime,
) -> Mapping[str, Any]:
    approval = owned.body
    if set(approval) != {
        "schema_version",
        "approval_id",
        "decision",
        "release_id",
        "created_at",
        "expires_at",
        "nonce",
        "authorized_role",
        "action_set",
        "action_set_sha256",
        "approval_request_sha256",
        "release_bom_sha256",
        "identity",
    }:
        raise ProdE2EReleaseError("prod_e2e_release_approval_shape_invalid")
    created_at = _timestamp(approval.get("created_at"), field="approval_created_at")
    expires_at = _timestamp(approval.get("expires_at"), field="approval_expires_at")
    approval_id = str(approval.get("approval_id") or "").strip()
    nonce = str(approval.get("nonce") or "")
    if (
        approval.get("schema_version") != APPROVAL_SCHEMA_VERSION
        or not approval_id
        or len(approval_id) > 128
        or approval.get("decision") != APPROVAL_DECISION
        or approval.get("release_id") != request["release_id"]
        or approval.get("authorized_role") != "owner"
        or NONCE_RE.fullmatch(nonce) is None
        or approval.get("action_set") != list(ACTION_SET)
        or approval.get("action_set_sha256") != _sha256_value(list(ACTION_SET))
        or approval.get("approval_request_sha256") != request["request_sha256"]
        or approval.get("release_bom_sha256") != request["release_bom_sha256"]
        or approval.get("identity") != request["identity"]
        or created_at < request["created_at"]
        or created_at > now + MAX_FUTURE_SKEW
        or expires_at <= created_at
        or expires_at <= now
        or expires_at - created_at > MAX_APPROVAL_VALIDITY
    ):
        raise ProdE2EReleaseError("prod_e2e_release_approval_invalid")
    return {
        "evidence_path": str(owned.path),
        "approval_id": approval_id,
        "sha256": owned.sha256,
        "created_at": created_at.isoformat(),
        "expires_at": expires_at.isoformat(),
        "nonce_sha256": hashlib.sha256(nonce.encode("utf-8")).hexdigest(),
    }


def _validate_quarantine_baseline_approval(
    owned: OwnedJson,
    *,
    request: Mapping[str, Any],
    release_approval: Mapping[str, Any],
    now: datetime,
) -> Mapping[str, Any]:
    approval = owned.body
    expected_identity = {
        "uid": request["identity"]["uid"],
        "username": request["identity"]["username"],
    }
    if set(approval) != {
        "schema_version",
        "release_id",
        "release_bom_sha256",
        "quarantine_core_sha256",
        "decision",
        "identity",
        "created_at",
    }:
        raise ProdE2EReleaseError(
            "prod_e2e_release_baseline_approval_shape_invalid"
        )
    created_at = _timestamp(
        approval.get("created_at"), field="baseline_approval_created_at"
    )
    expected_core = request["release_bom"]["delivery_store_cutover"][
        "quarantine_core"
    ]["core_sha256"]
    release_created_at = _timestamp(
        release_approval.get("created_at"), field="approval_created_at"
    )
    if (
        approval.get("schema_version") != BASELINE_APPROVAL_SCHEMA_VERSION
        or approval.get("release_id") != request["release_id"]
        or approval.get("release_bom_sha256") != request["release_bom_sha256"]
        or approval.get("quarantine_core_sha256") != expected_core
        or approval.get("decision") != BASELINE_APPROVAL_DECISION
        or approval.get("identity") != expected_identity
        or created_at < release_created_at
        or created_at > now + MAX_FUTURE_SKEW
        or owned.sha256 == release_approval.get("sha256")
        or str(owned.path) == release_approval.get("evidence_path")
    ):
        raise ProdE2EReleaseError(
            "prod_e2e_release_baseline_approval_invalid"
        )
    return {
        "evidence_path": str(owned.path),
        "sha256": owned.sha256,
        "created_at": created_at.isoformat(),
        "decision": BASELINE_APPROVAL_DECISION,
        "quarantine_core_sha256": expected_core,
    }


def build_blocker_bom(*, now: datetime, verified_test_count: int) -> Mapping[str, Any]:
    """Build an auditable NO-GO snapshot without exercising production paths."""

    current = _now(now)
    if verified_test_count < 1:
        raise ProdE2EReleaseError("prod_e2e_release_blocker_bom_test_count_invalid")
    host = _observe_canonical_host_binding(
        expected_commit=HOST_FINAL_COMMIT, expected_tree=HOST_FINAL_TREE
    )
    proxy_static = _validate_viewer_proxy_static_evidence()
    closure = _validate_closure_audit(
        _read_owned_json(
            Path(PIPELINE_CLOSURE_SEALED_MIRROR_PATH),
            artifact="blocker_bom_closure_audit",
        )
    )
    source_path = Path(__file__).resolve()
    test_path = source_path.parents[1] / "tests/scripts/test_pnc_rca_prod_e2e_release.py"
    source_sha256 = hashlib.sha256(source_path.read_bytes()).hexdigest()
    test_sha256 = hashlib.sha256(test_path.read_bytes()).hexdigest()
    body: dict[str, Any] = {
        "schema_version": "pnc_rca_prod_e2e_blocker_bom_v1",
        "release_id": "rca-prod-e2e-final-candidates-20260721",
        "generated_at": current.isoformat(),
        "status": "NO_GO",
        "production_mutation": False,
        "tooling": {
            "source_path": str(source_path),
            "source_sha256": source_sha256,
            "test_path": str(test_path),
            "test_sha256": test_sha256,
            "verified_test_count": verified_test_count,
            "schema_versions": {
                "bom": BOM_SCHEMA_VERSION,
                "request": REQUEST_SCHEMA_VERSION,
                "validation": VALIDATION_SCHEMA_VERSION,
                "component_binding": COMPONENT_BINDING_SCHEMA_VERSION,
                "execution_preflight": EXECUTION_PREFLIGHT_SCHEMA_VERSION,
                "completion": COMPLETION_RECEIPT_SCHEMA_VERSION,
            },
        },
        "host_candidate": {
            "root": CANONICAL_HOST_ROOT,
            "commit": host["commit"],
            "tree": host["tree"],
            "parent": HOST_FINAL_PARENT_COMMIT,
            "runtime_allowlists": host["runtime_allowlists"],
            "required_file_sha256": host["required_file_sha256"],
            "identity_evidence": host["candidate_identity_evidence"],
            "explicit_viewer_origin_required_for_new_remote_file": True,
            "runtime_installed": False,
            "services_restarted": False,
        },
        "vm_candidate": {
            "root": PIPELINE_SOURCE_ROOT,
            "commit": PIPELINE_COMMIT,
            "tree": PIPELINE_TREE,
            "entrypoint": {
                "relative": PIPELINE_ENTRYPOINT,
                "sha256": PIPELINE_ENTRYPOINT_SHA256,
                "git_mode": "100755",
            },
            "report_server": {
                "relative": VM_REPORT_ENTRYPOINT_RELATIVE,
                "sha256": VM_REPORT_ENTRYPOINT_SHA256,
                "git_mode": "100755",
                "unit_relative": VM_REPORT_UNIT_RELATIVE,
                "unit_sha256": VM_REPORT_UNIT_SHA256,
                "unit_git_mode": "100644",
                "environment_file": VM_REPORT_ENV_PATH,
                "environment_variable": VM_REPORT_ENV_VARIABLE,
                "delivery_manifest_schema": "delivery_manifest_v2",
                "viz_manifest_schema": "g1q3_rca_viz_publication_v1",
                "max_concurrent_requests": 4,
                "request_queue_size": 16,
            },
            "postcommit_receipt": {
                "vm_path": PIPELINE_CANDIDATE_AUDIT_VM_PATH,
                "cifs_path": PIPELINE_CANDIDATE_AUDIT_CIFS_PATH,
                "sha256": PIPELINE_CANDIDATE_AUDIT_SHA256,
                "schema_version": "g1q3_rca_vm_candidate_independent_audit_v2",
                "verdict": "GO",
                "deployment_authorization": False,
                "focused_tests_passed": 139,
                "symlink_environment_skips": 4,
                "posix_symlink_negative_coverage": True,
            },
            "fixed_cli_closure": {
                **closure,
                "schema_version": CLOSURE_AUDIT_SCHEMA_VERSION,
                "authorizes_final_candidate": True,
                "production_mutation": False,
            },
            "runtime_installed": False,
            "services_restarted": False,
        },
        "viewer_proxy_candidate": {
            **proxy_static,
            "expected_viewer_address": VIEWER_EXPECTED_ADDRESS,
            "route_prefix": VIEWER_PROXY_ROUTE_PREFIX,
            "upstream_origin": VIEWER_PROXY_UPSTREAM_ORIGIN,
            "public_dns_origin": None,
            "live_observation_receipt": None,
            "deployed": False,
            "tls_hostname_verified": False,
            "browser_ignore_https_errors_required": False,
            "maintainer_request": {
                "path": (
                    "/Users/songying/.codex/tmp/rca-prod-e2e-release-20260721/"
                    "evidence/pathsafe-integration/"
                    "viewer-proxy-maintainer-request.md"
                ),
                "sha256": (
                    "e1d361392c5a6e99fc2fd7af65f84580e036e3267ceac168a628a01df1b7de94"
                ),
                "binds_final_vm_4b26cc79": True,
            },
        },
        "target_recovery": {
            "topic": TOPIC,
            "partition": PARTITION,
            "offset": TARGET_OFFSET,
            "event_uid": TARGET_EVENT_UID,
            "raw_sha256": TARGET_RAW_SHA256,
            "project_key": TARGET_PROJECT_KEY,
            "project_simple_name": TARGET_PROJECT_SIMPLE_NAME,
            "work_item_id": TARGET_WORK_ITEM_ID,
            "business_key": TARGET_BUSINESS_KEY,
            "submission_key": TARGET_SUBMISSION_KEY,
            "raw_payload_persisted": False,
            "required_live_preread": {
                "assignment": "explicit_single_partition",
                "group_id": None,
                "enable_auto_commit": False,
                "commit_called": False,
                "retention_must_include_offset": True,
                "raw_sha256_must_match": True,
                "business_and_submission_must_match": True,
                "raw_payload_must_not_be_reconstructed_from_hashes": True,
            },
            "gap_ledger": {
                "path": (
                    "/Users/songying/.codex/tmp/rca-prod-e2e-release-20260721/"
                    "evidence/pre-t0-accepted-gap-ledger.json"
                ),
                "sha256": GAP_LEDGER_FILE_SHA256,
                "retained_start_observed": 514,
                "end_offset_observed": LIVE_T0_OFFSET,
            },
        },
        "delivery_closeout": {
            "target_issue_url": TARGET_ISSUE_URL,
            "api_project_key": TARGET_PROJECT_KEY,
            "browser_project_slug": TARGET_PROJECT_SIMPLE_NAME,
            "required_field_keys": list(TARGET_FIELD_KEYS),
            "exact_comment_count": 1,
            "official_field_comment_marker_readback_required": True,
            "post_cutover_kafka_canary_required": True,
            "viewer_proxy_live_receipt_required_before_target_write": True,
        },
        "viewer_route_policy": {
            "preferred_path": "canonical_dns_https_same_origin_proxy",
            "preferred_path_authorized": False,
            "legacy_ip_or_perception_fallback": "not_default",
            "legacy_fallback_requires_separate_explicit_authorization": True,
            "historical_browser_success_is_not_current_tls_or_deployment_proof": True,
        },
        "superseded_evidence": {
            "host_b611_identity_receipt_sha256": (
                "28b7543823df9f9633fb3dca4aac59fe78bf4f40ba31c9c83bdeae25ad77714e"
            ),
            "vm_799_commit": "799447c4e01377c6cfcdbb9cafde39cc1c759de5",
            "vm_799_tree": "32c06c49b991e4b6b54ba70c4630dfd0f3c96556",
            "fixed_cli_799_closure": {
                "path": (
                    "/Users/songying/.codex/tmp/rca-prod-e2e-release-20260721/"
                    "evidence/fixed-cli-mcap-hard-rule-audit-799447c4.json"
                ),
                "sha256": (
                    "51d8769e765f5750cbdbf4f02b1258086652ae917aad9a8be1468aba0997264e"
                ),
                "classification": "immutable_ancestor_reference_only",
                "authorizes_final_4b26cc79": False,
            },
            "cross_contract_receipt_sha256": CROSS_CONTRACT_PASS_FILE_SHA256,
            "cross_contract_authorizes_final_candidates": False,
            "capacity_authorization": {
                "path": (
                    "/Users/songying/.ssh-mini/"
                    "rca-bootstrap-capacity-authorization.json"
                ),
                "sha256": (
                    "20864b402b5a1dbef329ee482a8dd533c12622f80c01842f351a0223a2726697"
                ),
                "bound_release_bom_sha256": (
                    "50cc8cf0cd222b66c3df6395c2d954e3ab9fa00b3662178b08d0d4a3240efb61"
                ),
                "reusable_for_this_bom": False,
            },
        },
        "required_fresh_artifacts": [
            "final_540dc0c8_4b26cc79_manifest_html_cross_contract_receipt",
            "fresh_component_binding_with_exact_dns_tls_and_vm_env_identity",
            "fresh_live_kafka_p0_o650_preread_receipt",
            "fresh_viewer_proxy_live_http_tls_browser_receipt",
            "exact_bom_owner_release_approval",
            "exact_bom_quarantine_baseline_approval",
            "exact_bom_rca_prod_capacity_authorization",
            "fresh_execution_preflight_after_all_writers_stopped",
        ],
        "blockers": [
            "final_cross_contract_receipt_absent",
            "canonical_dns_origin_unset",
            "vm_report_environment_file_unprovisioned",
            "vm_report_service_not_activated",
            "viewer_proxy_not_installed_or_reloaded",
            "strict_tls_and_nonintercepted_browser_proof_absent",
            "target_kafka_live_retention_and_raw_hash_preread_absent",
            "exact_bom_owner_approvals_absent",
            "regular_rca_prod_capacity_authorization_absent",
            "regular_capacity_requires_20_zero_materialized_samples_over_7_days",
            "production_release_not_executed",
        ],
    }
    body["bom_core_sha256"] = _sha256_value(body)
    return body


def _claim_approval_nonce(
    *,
    approval: Mapping[str, Any],
    request: Mapping[str, Any],
    claimed_at: datetime,
) -> Mapping[str, Any]:
    root = APPROVAL_NONCE_LEDGER_ROOT.expanduser().absolute()
    if not root.exists():
        try:
            os.mkdir(root, 0o700)
        except OSError as exc:
            raise ProdE2EReleaseError(
                "prod_e2e_release_nonce_ledger_unavailable"
            ) from exc
    _owner_only_directory(root, artifact="nonce_ledger")
    nonce_sha256 = _sha256(
        approval.get("nonce_sha256"), field="approval_nonce_sha256"
    )
    path = root / f"{nonce_sha256}.json"
    payload = {
        "schema_version": "pnc_rca_release_approval_nonce_claim_v1",
        "nonce_sha256": nonce_sha256,
        "approval_id": approval["approval_id"],
        "approval_sha256": approval["sha256"],
        "release_id": request["release_id"],
        "request_sha256": request["request_sha256"],
        "release_bom_sha256": request["release_bom_sha256"],
        "claimed_at": claimed_at.isoformat(),
    }
    try:
        claim_sha256 = _publish_no_clobber(path, payload)
    except ProdE2EReleaseError as exc:
        raise ProdE2EReleaseError(
            "prod_e2e_release_approval_nonce_replayed"
        ) from exc
    return {
        "path": str(path),
        "sha256": claim_sha256,
        "nonce_sha256": nonce_sha256,
        "consumed": True,
    }


def validate_only(
    *,
    phase: str,
    request_path: Path,
    candidate_observation_path: Path,
    receipt_path: Path,
    approval_path: Path | None = None,
    quarantine_baseline_approval_path: Path | None = None,
    bootstrap_authorization_path: Path | None = None,
    execution_preflight_path: Path | None = None,
    now: datetime | None = None,
    machine_identity_provider: MachineIdentityProvider = _machine_identity,
) -> Mapping[str, Any]:
    current = _now(now)
    request = _validate_request(
        _read_owned_json(request_path, artifact="approval_request"),
        machine_identity_provider=machine_identity_provider,
    )
    bom = request["release_bom"]
    if phase == "staging":
        if (
            approval_path is not None
            or quarantine_baseline_approval_path is not None
            or bootstrap_authorization_path is not None
            or execution_preflight_path is not None
        ):
            raise ProdE2EReleaseError("prod_e2e_release_staging_external_authority_forbidden")
        candidate = _validate_candidate_observation(
            _read_owned_json(candidate_observation_path, artifact="candidate_observation"),
            release_id=request["release_id"],
            expected_phase="staging",
            expected_root=bom["pipeline"]["staging_candidate"]["root"],
            now=current,
            require_fresh=False,
        )
        approval = None
        baseline_approval = None
        authorization = None
        nonce_claim = None
        execute_before = None
        production_ready = False
        blockers = [
            "external_release_approval_required",
            "new_bom_bound_bootstrap_authorization_required",
            "final_ext4_read_only_candidate_seal_required",
            "exact_writer_stop_and_fresh_live_preflight_required",
        ]
    elif phase == "final":
        if (
            approval_path is None
            or quarantine_baseline_approval_path is None
            or bootstrap_authorization_path is None
            or execution_preflight_path is None
        ):
            raise ProdE2EReleaseError("prod_e2e_release_final_authority_required")
        component_descriptor = bom["component_identities"]
        fresh_components = _validate_component_binding(
            _read_owned_json(
                Path(str(component_descriptor.get("evidence_path") or "")),
                artifact="component_binding",
            ),
            release_id=request["release_id"],
            now=current,
            require_fresh=True,
            verify_vm_live=True,
        )
        if (
            fresh_components["sha256"]
            != component_descriptor.get("evidence_sha256")
            or fresh_components["host"]
            != bom["host_runtime"]["canonical_final"]
            or fresh_components["workspace"]
            != component_descriptor.get("workspace")
            or fresh_components["worker"]
            != component_descriptor.get("worker")
            or fresh_components["pipeline"]
            != component_descriptor.get("pipeline")
            or fresh_components["viewer_proxy"]
            != component_descriptor.get("viewer_proxy")
            or fresh_components["admission_security"]
            != bom["admission_security"]
        ):
            raise ProdE2EReleaseError(
                "prod_e2e_release_final_component_binding_drift"
            )
        approval = _validate_approval(
            _read_owned_json(approval_path, artifact="approval"),
            request=request,
            now=current,
        )
        baseline_approval = _validate_quarantine_baseline_approval(
            _read_owned_json(
                quarantine_baseline_approval_path,
                artifact="quarantine_baseline_approval",
            ),
            request=request,
            release_approval=approval,
            now=current,
        )
        authorization_owned = _read_owned_json(
            bootstrap_authorization_path, artifact="bootstrap_authorization"
        )
        try:
            authorization = prod_bootstrap.validate_bootstrap_authorization(
                authorization_owned.body,
                now=current,
                expected_epoch_id=bom["bootstrap_authorization"]["bootstrap_epoch_id"],
                expected_release_bom_sha256=request["release_bom_sha256"],
                expected_release_approval_id=approval["approval_id"],
                expected_approval_evidence_sha256=approval["sha256"],
                authorization_receipt_sha256=authorization_owned.sha256,
            )
        except prod_bootstrap.RcaBootstrapAuthorizationError as exc:
            raise ProdE2EReleaseError(
                f"prod_e2e_release_bootstrap_authorization_invalid:{exc.code}"
            ) from exc
        authorization = {
            **authorization,
            "evidence_path": str(authorization_owned.path),
            "evidence_sha256": authorization_owned.sha256,
        }
        authorization_issued_at = _timestamp(
            authorization_owned.body.get("issued_at"),
            field="bootstrap_authorization_issued_at",
        )
        if (
            authorization_issued_at < _timestamp(
                approval["created_at"], field="approval_created_at"
            )
            or authorization_issued_at > current + MAX_FUTURE_SKEW
        ):
            raise ProdE2EReleaseError(
                "prod_e2e_release_bootstrap_authorization_order_invalid"
            )
        candidate = _validate_candidate_observation(
            _read_owned_json(candidate_observation_path, artifact="candidate_observation"),
            release_id=request["release_id"],
            expected_phase="final",
            expected_root=bom["pipeline"]["final_candidate_root"],
            now=current,
            require_fresh=True,
        )
        execution_preflight = _validate_execution_preflight(
            _read_owned_json(
                execution_preflight_path, artifact="execution_preflight"
            ),
            release_id=request["release_id"],
            now=current,
            db_cutover=bom["delivery_store_cutover"],
            host=bom["host_runtime"]["canonical_final"],
            worker=bom["component_identities"]["worker"],
            feishu_completion=bom["feishu_completion"],
            request_sha256=request["request_sha256"],
            release_bom_sha256=request["release_bom_sha256"],
            approval_sha256=approval["sha256"],
            baseline_approval_sha256=baseline_approval["sha256"],
            authorization_sha256=authorization_owned.sha256,
        )
        execute_before_dt = min(
            _timestamp(approval["expires_at"], field="approval_expires_at"),
            _timestamp(authorization["deadline"], field="authorization_deadline"),
            _timestamp(
                fresh_components["observed_at"],
                field="component_binding_observed_at",
            )
            + MAX_FINAL_OBSERVATION_AGE,
            _timestamp(candidate["observed_at"], field="candidate_observed_at")
            + MAX_FINAL_OBSERVATION_AGE,
            _timestamp(
                execution_preflight["observed_at"],
                field="execution_preflight_observed_at",
            )
            + MAX_FINAL_OBSERVATION_AGE,
        )
        if execute_before_dt <= current:
            raise ProdE2EReleaseError(
                "prod_e2e_release_execution_window_expired"
            )
        execute_before = execute_before_dt.isoformat()
        nonce_claim = _claim_approval_nonce(
            approval=approval,
            request=request,
            claimed_at=current,
        )
        production_ready = True
        blockers = []
    else:
        raise ProdE2EReleaseError("prod_e2e_release_phase_invalid")
    receipt = {
        "schema_version": VALIDATION_SCHEMA_VERSION,
        "ok": True,
        "phase": phase,
        "release_id": request["release_id"],
        "validated_at": current.isoformat(),
        "request_sha256": request["request_sha256"],
        "release_bom_sha256": request["release_bom_sha256"],
        "candidate_observation": candidate,
        "approval": approval,
        "quarantine_baseline_approval": baseline_approval,
        "approval_nonce_claim": nonce_claim,
        "execution_preflight": (
            execution_preflight if phase == "final" else None
        ),
        "execute_before": execute_before,
        "bootstrap_authorization": authorization,
        "authorized_scope": _authorized_scope(bom),
        "execution_boundary": bom["tooling_boundary"],
        "production_ready": production_ready,
        "blockers": blockers,
        "production_effects_executed": False,
    }
    receipt_sha256 = _publish_no_clobber(receipt_path, receipt)
    return {**receipt, "receipt_path": str(receipt_path.absolute()), "receipt_sha256": receipt_sha256}


def validate_closeout(
    *,
    request_path: Path,
    final_validation_path: Path,
    completion_receipt_path: Path,
    receipt_path: Path,
    now: datetime | None = None,
    machine_identity_provider: MachineIdentityProvider = _machine_identity,
) -> Mapping[str, Any]:
    current = _now(now)
    request = _validate_request(
        _read_owned_json(request_path, artifact="approval_request"),
        machine_identity_provider=machine_identity_provider,
    )
    final_validation = _read_owned_json(
        final_validation_path, artifact="final_validation"
    )
    completion = _validate_completion_receipt(
        _read_owned_json(
            completion_receipt_path, artifact="completion_receipt"
        ),
        request=request,
        final_validation=final_validation,
        now=current,
    )
    result = {
        "schema_version": "pnc_rca_prod_e2e_closeout_validation_v1",
        "ok": True,
        "release_id": request["release_id"],
        "validated_at": current.isoformat(),
        "request_sha256": request["request_sha256"],
        "release_bom_sha256": request["release_bom_sha256"],
        "final_validation_sha256": final_validation.sha256,
        "completion": completion,
        "production_completed": completion["production_completed"],
        "production_effects_executed_by_tool": False,
    }
    receipt_sha256 = _publish_no_clobber(receipt_path, result)
    return {
        **result,
        "receipt_path": str(receipt_path.absolute()),
        "receipt_sha256": receipt_sha256,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("build-request")
    build.add_argument("--release-id", required=True)
    build.add_argument("--bootstrap-epoch-id", required=True)
    build.add_argument("--final-candidate-root", required=True)
    build.add_argument("--gap-ledger", type=Path, required=True)
    build.add_argument("--field-preread", type=Path, required=True)
    build.add_argument("--input-preread", type=Path, required=True)
    build.add_argument("--closure-audit", type=Path, required=True)
    build.add_argument("--staging-observation", type=Path, required=True)
    build.add_argument("--cross-contract-pass", type=Path, required=True)
    build.add_argument("--component-binding", type=Path, required=True)
    build.add_argument("--db-cutover-binding", type=Path, required=True)
    build.add_argument("--output-dir", type=Path, required=True)

    validate = commands.add_parser("validate-only")
    validate.add_argument("--phase", choices=("staging", "final"), required=True)
    validate.add_argument("--request", type=Path, required=True)
    validate.add_argument("--candidate-observation", type=Path, required=True)
    validate.add_argument("--approval", type=Path)
    validate.add_argument("--quarantine-baseline-approval", type=Path)
    validate.add_argument("--bootstrap-authorization", type=Path)
    validate.add_argument("--execution-preflight", type=Path)
    validate.add_argument("--receipt", type=Path, required=True)

    closeout = commands.add_parser("validate-closeout")
    closeout.add_argument("--request", type=Path, required=True)
    closeout.add_argument("--final-validation", type=Path, required=True)
    closeout.add_argument("--completion-receipt", type=Path, required=True)
    closeout.add_argument("--receipt", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "build-request":
        result = build_request(
            release_id=args.release_id,
            bootstrap_epoch_id=args.bootstrap_epoch_id,
            final_candidate_root=args.final_candidate_root,
            gap_ledger_path=args.gap_ledger,
            field_preread_path=args.field_preread,
            input_preread_path=args.input_preread,
            closure_audit_path=args.closure_audit,
            staging_observation_path=args.staging_observation,
            cross_contract_pass_path=args.cross_contract_pass,
            component_binding_path=args.component_binding,
            db_cutover_binding_path=args.db_cutover_binding,
            output_dir=args.output_dir,
        )
    elif args.command == "validate-only":
        result = validate_only(
            phase=args.phase,
            request_path=args.request,
            candidate_observation_path=args.candidate_observation,
            receipt_path=args.receipt,
            approval_path=args.approval,
            quarantine_baseline_approval_path=args.quarantine_baseline_approval,
            bootstrap_authorization_path=args.bootstrap_authorization,
            execution_preflight_path=args.execution_preflight,
        )
    else:
        result = validate_closeout(
            request_path=args.request,
            final_validation_path=args.final_validation,
            completion_receipt_path=args.completion_receipt,
            receipt_path=args.receipt,
        )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
