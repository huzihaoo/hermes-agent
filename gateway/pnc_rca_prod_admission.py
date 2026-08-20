"""Host-only signed admission boundary for RCA production VM tasks."""

from __future__ import annotations

import base64
import copy
import fcntl
import hashlib
import hmac
import json
import os
import re
import secrets
import shlex
import stat
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

SCHEMA_VERSION = "hermes-rca-prod-live-admission/v1"
SNAPSHOT_SCHEMA_VERSION = "hermes-rca-prod-resource-snapshot/v1"
RESOURCE_POLICY_VERSION = "hermes-rca-prod-live-resource-policy/v1"
TRUST_SCOPE = "trusted_host_service_create_once_bridge"
HMAC_ENV = "HERMES_RCA_PROD_ADMISSION_HMAC_KEY"
MAX_TTL_SECONDS = 120
MAX_RESOURCE_OUTPUT_BYTES = 1024 * 1024
DEFAULT_RESOURCE_TIMEOUT_SECONDS = 15
DEFAULT_RESOURCE_PATH = Path.home() / ".local" / "bin" / "ssh-mini-resource"
VM_FIXED_CLI = "./api/g1q3_rca/scripts/run_rca_service_request.py"
VM_TASK_ROOT = "/home/mini/.hermes/shared-state/tasks"
MIN_ROOT_AVAILABLE_BYTES = 400 * 1024**3
MIN_DELIVERY_AVAILABLE_BYTES = 512 * 1024**3
MIN_MEMORY_AVAILABLE_BYTES = 16 * 1024**3
MIN_SWAP_FREE_RATIO = 0.05
MAX_LOAD_PER_CPU = 0.85
MAX_DNP_REAL = 4
MAX_DNP_LIKE = 12
MAX_MCAP_RSS_BYTES = 24 * 1024**3
MAX_MCAP_PROCESS_COUNT = 2
MAX_CONCURRENCY = 4
HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
TASK_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")

HISTORICAL_REQUEST_SCHEMA = "g1q3-rca-historical-full-rerun-request/v2"
HISTORICAL_PLAN_SCHEMA = "g1q3_rca_historical_full_chain_plan_v4"
HISTORICAL_SHARD_PLAN_SCHEMA = "g1q3_rca_historical_shard_plan_v1"
HISTORICAL_SELF_SEAL_SCHEMA = "g1q3_rca_canonical_self_seal_v1"
HISTORICAL_BOOTSTRAP_SCHEMA = "g1q3-rca-prod-bootstrap/v1"
HISTORICAL_LEDGER_SCHEMA = "g1q3-rca-global-evaluation-lane-ledger/v1"
HISTORICAL_RESERVATION_SCHEMA = "g1q3-rca-evaluation-lane-reservation/v1"
HISTORICAL_PREPARE = "/home/mini/data3/yj-evaluation-server/api/g1q3_rca/scripts/prepare_historical_full_chain.py"
HISTORICAL_RUNNER = "api/g1q3_rca/scripts/run_historical_full_chain_batch.py"
HISTORICAL_FROZEN_SOURCE_ROOT = Path("/home/mini/.local/state/g1q3-rca-frozen-source")
HISTORICAL_HOST_TMP_ROOT = Path.home() / "Mounts/department-pnc_team-planning_algo-driving/tmp"
HISTORICAL_STATE_ROOT = Path.home() / ".hermes/runtime/rca-prod/historical-full-rerun"
HISTORICAL_REQUEST_FIELDS = frozenset({
    "schema_version", "request_id", "owner", "remote_commit", "remote_tree",
    "canonical_input_raw_sha256", "selection_raw_sha256",
    "selection_identity_raw_sha256", "review_disposition_raw_sha256",
    "ready_index_raw_sha256", "requirements_contract_hash",
    "evaluator_fingerprints_sha256", "suite_receipt_sha256",
    "w17_receipt_sha256",
})
HISTORICAL_RESERVATION_FIELDS = frozenset({
    "schema_version", "receipt_id", "reservation_id", "ledger_sequence",
    "task_id", "plan_sha256", "lane_count", "started_at",
    "lease_expires_at", "observed_global_peak",
    "max_global_evaluation_lanes", "queue_if_blocked",
})
HISTORICAL_FINAL_SEAL_SCHEMA = "g1q3_rca_historical_execution_final_seal_v3"
HISTORICAL_FINAL_SEAL_FIELDS = frozenset({
    "schema_version", "execution_manifest_sha256",
    "execution_manifest_semantic_sha256", "run_identity", "source_identity",
    "contract_identity", "input_identity", "authority_provenance", "scheduler",
    "shards_semantic_sha256", "sealed_at", "item_count", "terminal_complete",
    "all_pass", "ordered_work_item_ids_sha256",
    "manifest_items_semantic_sha256", "self_seal",
})
HISTORICAL_SUCCESSOR_RELEASE_FIELDS = frozenset({
    "source_commit", "source_tree", "requirements_contract_hash",
    "evaluator_fingerprints", "evaluator_fingerprints_sha256",
    "evaluator_version", "suite_receipt_path", "suite_receipt_sha256",
    "w17_receipt_path", "w17_receipt_sha256",
})
HISTORICAL_MAX_CONTROL_BYTES = 16 * 1024 * 1024
HISTORICAL_BOOTSTRAP_FIELDS = frozenset({
    "schema_version", "receipt_id", "reservation_id", "issued_at",
    "expires_at", "decision", "owner", "one_use", "bindings",
    "receipt_fingerprint", "hmac_sha256",
})
HISTORICAL_BOOTSTRAP_BINDINGS = frozenset({
    "request_sha256", "plan_sha256", "task_id", "host_reservation_path",
    "max_global_evaluation_lanes", "queue_if_blocked",
})
HISTORICAL_LANES = 3
HISTORICAL_ITEMS = 308
HISTORICAL_SHARD_COUNTS = (103, 103, 102)
HISTORICAL_BOOTSTRAP_TTL_SECONDS = 300
HISTORICAL_LEASE_SECONDS = 24 * 60 * 60
HISTORICAL_INPUT_CONTRACT = {
    "canonical_input": {
        "path": "/mnt/tmp/g1q3-dev-batch-tree-v5-202608151015/plan/blind-request-batch.json",
        "raw_sha256": "6fbc884ff5f443c6af09bfac05fd0782f05254df8d3a39208ee705ce87f3797e",
        "semantic_sha256": "656e1696e3ac6ebca8dbefed84e30d5db7d5e134d5e5babc87d920d7398e6c92", "item_count": 308,
        "ordered_work_item_ids_sha256": "d93b1479dc5714cda3febaf723a9aa59b1e9338e1d478cf28c9fa5562a126a46",
    },
    "selection": {
        "path": "/mnt/tmp/g1q3-rca-recovery-20260812/public-handoff/vm-b-selection-receipt-apply-v1/artifacts/goal312-selection.json",
        "raw_sha256": "7a5679d3c35609c025e96ad84c8222336b4bb2126840cda42c947e697cd93a23",
        "semantic_sha256": "d840920a6fb7aef9f236a492af6ebfc3f2e00d991cdcc095c813a2ee256d64ad", "item_count": 312,
    },
    "selection_identity": {
        "path": "/mnt/tmp/g1q3-rca-recovery-20260812/public-handoff/vm-b-selection-receipt-apply-v1/artifacts/selection-identity.json",
        "raw_sha256": "5343fa15d4752e308e0d27bf520431e6c8656540c15d214c672fcab12cc141ee",
        "semantic_sha256": "82a18d89e8c252efc8e25310a84cc7b0394898aa252c1d175866912a14bf8c89",
    },
    "review_disposition": {
        "path": "/mnt/tmp/g1q3-rca-recovery-20260812/public-handoff/vm-b-selection-receipt-apply-v1/artifacts/selected-review-disposition-v1.json",
        "raw_sha256": "0fc7cf077148be4543ac066f137e8773a683deb4d81b14db9a87baae1cb204fd",
        "semantic_sha256": "aa7bbc3a01154492010aa622b897873170b9ebcc6e7d3073b29523d647a26f21", "item_count": 4,
        "review_ids": ["7058360591", "7058414896", "7058468189", "7058476613"],
    },
    "ready_index": {
        "path": "/mnt/tmp/g1q3-rca-recovery-20260812/public-handoff/vm-b-selection-receipt-apply-v1-rerun/final-artifacts/selected-ready-index.jsonl",
        "raw_sha256": "26b5f05d7653c61703b53272585c7e83dd3292e9c65c0dd6396116cccad5428a",
        "semantic_sha256": "fcfa89ba160958c876f75ff81d8885139ecc87f66ac26442ecc4bb5cab20f40e", "item_count": 308,
        "ordered_work_item_ids_sha256": "6862bb376bdf1fccb68810c041f2406384b5df24cc03d1d925cce74149cfb581",
    },
}
HISTORICAL_EXECUTION_POLICY = {
    "resource_class": "rca_prod", "max_lanes": 3, "workers_per_shard": 1,
    "queue_if_blocked": False, "input_materialization": "forbidden",
    "raw_s3b_fallback": False, "allow_feishu_writeback": False,
    "allow_live_database_write": False, "allow_runtime_activation": False,
}
HISTORICAL_BUDGETS = {
    "s2_max_clips_per_case": 16, "s2_max_messages_per_case": 1_000_000,
    "s2_max_scanned_messages_per_case": 1_000_000,
    "s2_timeout_seconds_per_case": 300, "s2_output_bytes_per_case": 749_000_000,
    "s2_output_bytes_per_shard": 80_000_000_000,
    "backing_output_bytes_per_shard": 80_000_000_000,
    "decoder_output_bytes_per_case": 512 * 1024 * 1024,
    "decoder_output_bytes_per_shard": 80_000_000_000,
    "run_output_bytes": 750_000_000_000,
    "free_space_admission_bytes": 800_000_000_000,
}
# Server-owned successor projection. Request payloads may repeat these pins,
# but cannot select a different evaluator/source identity.
HISTORICAL_SUCCESSOR_RELEASE: Mapping[str, Any] | None = {
    "source_commit": "363fb4b438c8bf12baaccf94bb03e9a678b7cb79",
    "source_tree": "8f68f96f8c9b73ed00f49bbd5414373abeece0d1",
    "requirements_contract_hash": "ca99f759c70b72b0836f956557ecc5e23c11538132b13f1d1bfe8a46ce7e6cb6",
    "evaluator_fingerprints": {
        "g1q3_rca/aeb_signal_parser.py": "fa227f22a684f2a4b0808fefe0d596a032c3a582e1571f67ede7937d5894e3d3",
        "g1q3_rca/rca_evaluators/_raw_streams.py": "afba29499b1f02e0d477bee4ca07d76b5f482c910e9bee00e5341930e32b2729",
        "g1q3_rca/rca_evaluators/acc_debug_spec.py": "a6eaf30252cf8f93843cfa24d69c29672b2334cedf81c482775554cc04028fc9",
        "g1q3_rca/report_builder.py": "71ec50b43ae95b21029ed1f953e81e65e1f87c1de654d2c8500463b1759a953d",
        "g1q3_rca/scripts/check_case_gate.py": "918be3270ef7cf2370b371abc20e05a4e4cbc65c2e086ffc926ab3fa1f318cd4",
        "g1q3_rca/signal_access.py": "7da6555018c9c3c5e21e6eefddf5dcb1d74003036945387297cb6c16feb9297d",
        "g1q3_rca/signal_registry.py": "ffd0a796c027df1d81e30a1727eea1d37ccfcd0c5b88a806a201a0c8f3d54f17",
    },
    "evaluator_fingerprints_sha256": "e492eaa8afd9348990cb2de265575211ff248fdebacd0fff72b2f7bafe7f18c0",
    "evaluator_version": "git-363fb4b438c8bf12baaccf94bb03e9a678b7cb79",
    "suite_receipt_path": "/mnt/tmp/g1q3-rca-canonical-scoped-verification-20260819/blocked-contract-363fb4b438/suite-receipt.json",
    "suite_receipt_sha256": "00247963de4b25eb9f030527c5899a0f7fb20c22126fe2e92eb98f1b0edddf0a",
    "w17_receipt_path": "/mnt/tmp/g1q3-rca-canonical-scoped-verification-20260819/blocked-contract-363fb4b438/w17-receipt.json",
    "w17_receipt_sha256": "2f6b5e9d82d75b9929d990fbe95e5c7d759643b6bdc10759a73676b60b4fef05",
}

RECEIPT_FIELDS = {
    "schema_version", "receipt_id", "issued_at", "expires_at", "decision",
    "resource_class", "capacity_mode", "trust_scope", "single_task",
    "queue_if_blocked", "bypass_requested", "bindings", "resource_policy",
    "resource_snapshot", "resource_snapshot_sha256", "receipt_fingerprint",
    "hmac_sha256",
}
BINDING_FIELDS = {
    "task_id", "attempt_id", "work_dir", "reservation_id",
    "reservation_fence", "reservation_contract_sha256", "goal_sha256",
    "command_sha256", "contract_sha256",
}
RESOURCE_POLICY_FIELDS = {
    "policy_version", "resource_check", "max_concurrency",
    "input_materialization", "root_required_available_bytes",
    "delivery_required_available_bytes",
}
SNAPSHOT_FIELDS = {
    "schema_version", "observed_at", "root_available_bytes",
    "delivery_available_bytes", "root_device", "delivery_device",
    "delivery_filesystem", "delivery_mount_rw", "delivery_writable",
    "memory_available_bytes", "swap_free_ratio", "load1", "cpu_count",
    "dnp_real", "dnp_like", "mcap_rss_bytes", "mcap_process_count",
}

RunFunc = Callable[..., subprocess.CompletedProcess[str]]


class RcaProdAdmissionError(RuntimeError):
    """Stable non-sensitive failure at the Host production admission boundary."""

    def __init__(self, code: str, *, retryable: bool = True):
        self.code = str(code or "rca_prod_admission_failed")[:120]
        self.retryable = bool(retryable)
        super().__init__(self.code)


@dataclass(frozen=True)
class RcaProdAdmission:
    receipt: dict[str, Any]
    meta: dict[str, Any]
    key_fingerprint: str


@dataclass(frozen=True)
class HistoricalFullRerunPlan:
    request: dict[str, Any]
    request_sha256: str
    plan: dict[str, Any]
    plan_bytes: bytes
    plan_sha256: str
    task_id: str
    task_root: Path
    plan_path: Path
    output_root: Path
    host_reservation_path: Path
    shard_artifacts: tuple[tuple[Path, bytes], ...]


def canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise RcaProdAdmissionError("rca_prod_contract_not_canonical", retryable=False) from exc


def sha256_value(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def goal_sha256(goal: str) -> str:
    return hashlib.sha256(str(goal).encode("utf-8")).hexdigest()


def build_rca_prod_command_argv(task_id: str) -> list[str]:
    normalized = str(task_id or "").strip()
    if not TASK_ID_RE.fullmatch(normalized):
        raise RcaProdAdmissionError("rca_prod_task_id_invalid", retryable=False)
    goal_path = f"{VM_TASK_ROOT}/{normalized}/goal.md"
    return [
        VM_FIXED_CLI,
        "--task-id",
        normalized,
        "--goal-path",
        goal_path,
    ]


def command_sha256(command: list[str]) -> str:
    return sha256_value([str(part) for part in command])


def _load_hmac_key(raw: str | bytes | None = None) -> bytes:
    if isinstance(raw, bytes):
        if len(raw) < 32:
            raise RcaProdAdmissionError("rca_prod_hmac_key_invalid", retryable=False)
        return raw
    value = (raw if raw is not None else os.environ.get(HMAC_ENV, "")).strip()
    try:
        if value.startswith("hex:"):
            key = bytes.fromhex(value[4:])
        elif value.startswith("base64:"):
            key = base64.b64decode(value[7:], validate=True)
        else:
            raise ValueError
    except Exception as exc:
        raise RcaProdAdmissionError("rca_prod_hmac_key_invalid", retryable=False) from exc
    if len(key) < 32:
        raise RcaProdAdmissionError("rca_prod_hmac_key_invalid", retryable=False)
    return key


def hmac_key_fingerprint(raw: str | bytes | None = None) -> str:
    return hashlib.sha256(_load_hmac_key(raw)).hexdigest()


def _strict_json_loads(raw: str) -> Any:
    def object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate key")
            value[key] = item
        return value

    return json.loads(
        raw,
        object_pairs_hook=object_pairs,
        parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
    )


def _timestamp(value: Any) -> datetime:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timezone required")
    return parsed.astimezone(timezone.utc)


def _now(value: datetime | None = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None or current.utcoffset() is None:
        raise RcaProdAdmissionError("rca_prod_time_invalid", retryable=False)
    return current.astimezone(timezone.utc)


def _require_hex(value: Any, code: str) -> str:
    normalized = str(value or "").strip().lower()
    if not HEX64_RE.fullmatch(normalized):
        raise RcaProdAdmissionError(code, retryable=False)
    return normalized


def _require_int(value: Any, code: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise RcaProdAdmissionError(code, retryable=False)
    return value


def _receipt_body(receipt: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in receipt.items()
        if key not in {"receipt_fingerprint", "hmac_sha256"}
    }


def _validate_snapshot(
    snapshot: Any,
    *,
    now: datetime,
    modeled_root_bytes: int,
    modeled_delivery_bytes: int,
) -> None:
    if not isinstance(snapshot, Mapping) or set(snapshot) != SNAPSHOT_FIELDS:
        raise RcaProdAdmissionError("rca_prod_snapshot_schema_invalid")
    if snapshot.get("schema_version") != SNAPSHOT_SCHEMA_VERSION:
        raise RcaProdAdmissionError("rca_prod_snapshot_schema_invalid")
    try:
        age = (now - _timestamp(snapshot.get("observed_at"))).total_seconds()
    except (TypeError, ValueError, OverflowError) as exc:
        raise RcaProdAdmissionError("rca_prod_snapshot_time_invalid") from exc
    if age < -5 or age > MAX_TTL_SECONDS:
        raise RcaProdAdmissionError("rca_prod_snapshot_stale")
    root_available = _require_int(
        snapshot.get("root_available_bytes"), "rca_prod_snapshot_capacity_invalid"
    )
    delivery_available = _require_int(
        snapshot.get("delivery_available_bytes"), "rca_prod_snapshot_capacity_invalid"
    )
    if root_available < max(MIN_ROOT_AVAILABLE_BYTES, modeled_root_bytes):
        raise RcaProdAdmissionError("rca_prod_root_capacity_blocked")
    if delivery_available < max(MIN_DELIVERY_AVAILABLE_BYTES, modeled_delivery_bytes):
        raise RcaProdAdmissionError("rca_prod_delivery_capacity_blocked")
    if str(snapshot.get("root_device") or "") == str(snapshot.get("delivery_device") or ""):
        raise RcaProdAdmissionError("rca_prod_delivery_device_invalid")
    if str(snapshot.get("delivery_filesystem") or "").lower() not in {"cifs", "smb3"}:
        raise RcaProdAdmissionError("rca_prod_delivery_filesystem_invalid")
    if snapshot.get("delivery_mount_rw") is not True or snapshot.get("delivery_writable") is not True:
        raise RcaProdAdmissionError("rca_prod_delivery_not_writable")
    if _require_int(snapshot.get("memory_available_bytes"), "rca_prod_memory_invalid") < MIN_MEMORY_AVAILABLE_BYTES:
        raise RcaProdAdmissionError("rca_prod_memory_blocked")
    try:
        swap_ratio = float(snapshot.get("swap_free_ratio"))
        load1 = float(snapshot.get("load1"))
        cpu_count = int(snapshot.get("cpu_count"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise RcaProdAdmissionError("rca_prod_pressure_invalid") from exc
    if swap_ratio < MIN_SWAP_FREE_RATIO:
        raise RcaProdAdmissionError("rca_prod_swap_blocked")
    if cpu_count < 1 or load1 < 0 or load1 > cpu_count * MAX_LOAD_PER_CPU:
        raise RcaProdAdmissionError("rca_prod_load_blocked")
    for field, limit, code in (
        ("dnp_real", MAX_DNP_REAL, "rca_prod_dnp_real_blocked"),
        ("dnp_like", MAX_DNP_LIKE, "rca_prod_dnp_like_blocked"),
        ("mcap_rss_bytes", MAX_MCAP_RSS_BYTES, "rca_prod_mcap_memory_blocked"),
        ("mcap_process_count", MAX_MCAP_PROCESS_COUNT, "rca_prod_mcap_process_blocked"),
    ):
        if _require_int(snapshot.get(field), "rca_prod_pressure_invalid") > limit:
            raise RcaProdAdmissionError(code)


def live_resource_policy() -> dict[str, Any]:
    return {
        "policy_version": RESOURCE_POLICY_VERSION,
        "resource_check": "per_task_live_snapshot",
        "max_concurrency": MAX_CONCURRENCY,
        "input_materialization": "forbidden",
        "root_required_available_bytes": MIN_ROOT_AVAILABLE_BYTES,
        "delivery_required_available_bytes": MIN_DELIVERY_AVAILABLE_BYTES,
    }


def _live_resource_policy(value: Any) -> dict[str, Any]:
    expected = live_resource_policy()
    if not isinstance(value, Mapping) or set(value) != RESOURCE_POLICY_FIELDS:
        raise RcaProdAdmissionError("rca_prod_resource_policy_invalid", retryable=False)
    if dict(value) != expected:
        raise RcaProdAdmissionError("rca_prod_resource_policy_invalid", retryable=False)
    return expected


def validate_resource_report(
    report: Any,
    *,
    now: datetime | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    current = _now(now)
    if not isinstance(report, Mapping):
        raise RcaProdAdmissionError("rca_prod_resource_report_invalid")
    if (
        report.get("resource_class") != "rca_prod"
        or report.get("ok_for_submit") is not True
        or report.get("ok_for_rca_prod_submit") is not True
        or list(report.get("reasons") or [])
        or list(report.get("rca_prod_reasons") or [])
    ):
        raise RcaProdAdmissionError("rca_prod_resource_blocked")
    snapshot = report.get("rca_prod_snapshot")
    if not isinstance(snapshot, Mapping):
        raise RcaProdAdmissionError("rca_prod_snapshot_schema_invalid")
    if sha256_value(snapshot) != str(report.get("rca_prod_snapshot_sha256") or ""):
        raise RcaProdAdmissionError("rca_prod_snapshot_hash_invalid")
    capacity = live_resource_policy()
    _validate_snapshot(
        snapshot,
        now=current,
        modeled_root_bytes=capacity["root_required_available_bytes"],
        modeled_delivery_bytes=capacity["delivery_required_available_bytes"],
    )
    return dict(snapshot), capacity


def run_resource_preflight(
    *,
    resource_path: Path = DEFAULT_RESOURCE_PATH,
    timeout_seconds: int = DEFAULT_RESOURCE_TIMEOUT_SECONDS,
    run_func: RunFunc = subprocess.run,
    now: datetime | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    env = dict(os.environ)
    env.pop(HMAC_ENV, None)
    command = [str(resource_path), "--json", "--resource-class", "rca_prod"]
    try:
        result = run_func(
            command,
            text=True,
            capture_output=True,
            timeout=timeout_seconds,
            env=env,
        )
    except subprocess.TimeoutExpired as exc:
        raise RcaProdAdmissionError("rca_prod_resource_timeout") from exc
    except Exception as exc:
        raise RcaProdAdmissionError("rca_prod_resource_unavailable") from exc
    stdout = result.stdout or ""
    if result.returncode != 0:
        raise RcaProdAdmissionError("rca_prod_resource_unavailable")
    if not stdout or len(stdout.encode("utf-8", errors="replace")) > MAX_RESOURCE_OUTPUT_BYTES:
        raise RcaProdAdmissionError("rca_prod_resource_output_invalid")
    try:
        report = _strict_json_loads(stdout)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RcaProdAdmissionError("rca_prod_resource_output_invalid") from exc
    return validate_resource_report(report, now=now)


def _sign_receipt(receipt: dict[str, Any], key: bytes) -> dict[str, Any]:
    signed = dict(receipt)
    signed.pop("receipt_fingerprint", None)
    signed.pop("hmac_sha256", None)
    body = canonical_bytes(signed)
    signed["receipt_fingerprint"] = hashlib.sha256(body).hexdigest()
    signed["hmac_sha256"] = hmac.new(key, body, hashlib.sha256).hexdigest()
    return signed


def validate_rca_prod_receipt(
    receipt: Any,
    *,
    expected_bindings: Mapping[str, Any],
    hmac_key: str | bytes | None = None,
    now: datetime | None = None,
    allow_historical: bool = False,
) -> dict[str, Any]:
    if not isinstance(receipt, Mapping) or set(receipt) != RECEIPT_FIELDS:
        raise RcaProdAdmissionError("rca_prod_receipt_schema_invalid", retryable=False)
    bindings = receipt.get("bindings")
    capacity = receipt.get("resource_policy")
    snapshot = receipt.get("resource_snapshot")
    if not isinstance(bindings, Mapping) or set(bindings) != BINDING_FIELDS:
        raise RcaProdAdmissionError("rca_prod_receipt_schema_invalid", retryable=False)
    if not isinstance(capacity, Mapping) or set(capacity) != RESOURCE_POLICY_FIELDS:
        raise RcaProdAdmissionError("rca_prod_receipt_schema_invalid", retryable=False)
    if (
        receipt.get("schema_version") != SCHEMA_VERSION
        or receipt.get("capacity_mode") != "steady"
        or receipt.get("trust_scope") != TRUST_SCOPE
        or receipt.get("decision") != "allow"
        or receipt.get("resource_class") != "rca_prod"
        or receipt.get("single_task") is not True
        or receipt.get("queue_if_blocked") is not False
        or receipt.get("bypass_requested") is not False
    ):
        raise RcaProdAdmissionError("rca_prod_receipt_policy_invalid", retryable=False)
    if not str(receipt.get("receipt_id") or "").strip() or not str(
        bindings.get("attempt_id") or ""
    ).strip():
        raise RcaProdAdmissionError("rca_prod_receipt_identity_invalid", retryable=False)
    key = _load_hmac_key(hmac_key)
    body = canonical_bytes(_receipt_body(receipt))
    fingerprint = hashlib.sha256(body).hexdigest()
    signature = str(receipt.get("hmac_sha256") or "").lower()
    if (
        not hmac.compare_digest(str(receipt.get("receipt_fingerprint") or ""), fingerprint)
        or not HEX64_RE.fullmatch(signature)
        or not hmac.compare_digest(signature, hmac.new(key, body, hashlib.sha256).hexdigest())
    ):
        raise RcaProdAdmissionError("rca_prod_receipt_signature_invalid", retryable=False)
    current = _now(now)
    try:
        issued = _timestamp(receipt.get("issued_at"))
        expires = _timestamp(receipt.get("expires_at"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise RcaProdAdmissionError("rca_prod_receipt_time_invalid", retryable=False) from exc
    ttl = (expires - issued).total_seconds()
    if ttl <= 0 or ttl > MAX_TTL_SECONDS or issued > current + timedelta(seconds=5):
        raise RcaProdAdmissionError("rca_prod_receipt_time_invalid", retryable=False)
    if not allow_historical and not (issued - timedelta(seconds=5) <= current <= expires):
        raise RcaProdAdmissionError("rca_prod_receipt_expired")
    normalized_expected = {key: str(value) for key, value in expected_bindings.items()}
    if set(normalized_expected) != BINDING_FIELDS or {
        key: str(value) for key, value in bindings.items()
    } != normalized_expected:
        raise RcaProdAdmissionError("rca_prod_receipt_binding_invalid", retryable=False)
    policy = _live_resource_policy(capacity)
    root_required = policy["root_required_available_bytes"]
    delivery_required = policy["delivery_required_available_bytes"]
    if sha256_value(snapshot) != str(receipt.get("resource_snapshot_sha256") or ""):
        raise RcaProdAdmissionError("rca_prod_snapshot_hash_invalid")
    snapshot_now = issued if allow_historical else current
    _validate_snapshot(
        snapshot,
        now=snapshot_now,
        modeled_root_bytes=root_required,
        modeled_delivery_bytes=delivery_required,
    )
    return dict(receipt)


def issue_rca_prod_admission(
    *,
    task_id: str,
    submission_key: str,
    goal: str,
    contract_sha256: str,
    reservation_id: str,
    reservation_fence: int | str,
    reservation_contract_sha256: str,
    resource_path: Path = DEFAULT_RESOURCE_PATH,
    run_func: RunFunc = subprocess.run,
    hmac_key: str | None = None,
    now: datetime | None = None,
    attempt_id: str | None = None,
    receipt_id: str | None = None,
) -> RcaProdAdmission:
    current = _now(now)
    normalized_task = str(task_id or "").strip()
    if normalized_task != str(submission_key or "").strip() or not TASK_ID_RE.fullmatch(normalized_task):
        raise RcaProdAdmissionError("rca_prod_task_identity_invalid", retryable=False)
    normalized_contract = _require_hex(contract_sha256, "rca_prod_contract_invalid")
    normalized_reservation_contract = _require_hex(
        reservation_contract_sha256, "rca_prod_reservation_invalid"
    )
    normalized_reservation = str(reservation_id or "").strip()
    normalized_fence = str(reservation_fence or "").strip()
    if not normalized_reservation or not normalized_fence:
        raise RcaProdAdmissionError("rca_prod_reservation_invalid", retryable=False)
    command = build_rca_prod_command_argv(normalized_task)
    goal_hash = goal_sha256(goal)
    command_hash = command_sha256(command)
    key = _load_hmac_key(hmac_key)
    snapshot, capacity = run_resource_preflight(
        resource_path=resource_path,
        run_func=run_func,
        now=current,
    )
    expires = current + timedelta(seconds=MAX_TTL_SECONDS)
    normalized_attempt = str(attempt_id or f"attempt-{secrets.token_hex(16)}")
    normalized_receipt = str(receipt_id or f"receipt-{secrets.token_hex(16)}")
    if (
        not normalized_attempt
        or len(normalized_attempt) > 128
        or not normalized_receipt
        or len(normalized_receipt) > 128
    ):
        raise RcaProdAdmissionError("rca_prod_receipt_identity_invalid", retryable=False)
    bindings = {
        "task_id": normalized_task,
        "attempt_id": normalized_attempt,
        "work_dir": f"/mnt/tmp/{normalized_task}",
        "reservation_id": normalized_reservation,
        "reservation_fence": normalized_fence,
        "reservation_contract_sha256": normalized_reservation_contract,
        "goal_sha256": goal_hash,
        "command_sha256": command_hash,
        "contract_sha256": normalized_contract,
    }
    receipt_body = {
        "schema_version": SCHEMA_VERSION,
        "receipt_id": normalized_receipt,
        "issued_at": current.replace(microsecond=0).isoformat(),
        "expires_at": expires.replace(microsecond=0).isoformat(),
        "decision": "allow",
        "resource_class": "rca_prod",
        "capacity_mode": "steady",
        "trust_scope": TRUST_SCOPE,
        "single_task": True,
        "queue_if_blocked": False,
        "bypass_requested": False,
        "bindings": bindings,
        "resource_policy": capacity,
        "resource_snapshot": snapshot,
        "resource_snapshot_sha256": sha256_value(snapshot),
    }
    receipt = _sign_receipt(receipt_body, key)
    validate_rca_prod_receipt(
        receipt,
        expected_bindings=bindings,
        hmac_key=key,
        now=current,
    )
    meta = {
        "resource_class": "rca_prod",
        "lane": "heavy",
        "queue_if_blocked": False,
        "resource_gate_bypass": False,
        "rca_prod_capacity_mode": "steady",
        "rca_prod_attempt_id": normalized_attempt,
        "reservation_id": normalized_reservation,
        "reservation_fence": normalized_fence,
        "reservation_contract_sha256": normalized_reservation_contract,
        "rca_prod_goal_sha256": goal_hash,
        "rca_prod_command_sha256": command_hash,
        "rca_prod_contract_sha256": normalized_contract,
        "rca_prod_admission_receipt": receipt,
        "rca_prod_admission_key_fingerprint": hashlib.sha256(key).hexdigest(),
    }
    return RcaProdAdmission(
        receipt=receipt,
        meta=meta,
        key_fingerprint=meta["rca_prod_admission_key_fingerprint"],
    )


def validate_existing_rca_prod_meta(
    meta: Any,
    *,
    task_id: str,
    goal: str,
    contract_sha256: str,
    reservation_id: str,
    reservation_fence: int | str,
    reservation_contract_sha256: str,
    hmac_key: str | None = None,
    now: datetime | None = None,
) -> None:
    if not isinstance(meta, Mapping):
        raise RcaProdAdmissionError("rca_prod_existing_identity_invalid", retryable=False)
    command_hash = command_sha256(build_rca_prod_command_argv(task_id))
    goal_hash = goal_sha256(goal)
    stable = {
        "resource_class": "rca_prod",
        "lane": "heavy",
        "queue_if_blocked": False,
        "resource_gate_bypass": False,
        "reservation_id": str(reservation_id),
        "reservation_fence": str(reservation_fence),
        "reservation_contract_sha256": str(reservation_contract_sha256),
        "rca_prod_goal_sha256": goal_hash,
        "rca_prod_command_sha256": command_hash,
        "rca_prod_contract_sha256": str(contract_sha256),
        "rca_prod_capacity_mode": "steady",
    }
    if any(meta.get(key) != value for key, value in stable.items()):
        raise RcaProdAdmissionError("rca_prod_existing_identity_invalid", retryable=False)
    attempt_id = str(meta.get("rca_prod_attempt_id") or "").strip()
    if not attempt_id:
        raise RcaProdAdmissionError("rca_prod_existing_identity_invalid", retryable=False)
    bindings = {
        "task_id": task_id,
        "attempt_id": attempt_id,
        "work_dir": f"/mnt/tmp/{task_id}",
        "reservation_id": str(reservation_id),
        "reservation_fence": str(reservation_fence),
        "reservation_contract_sha256": str(reservation_contract_sha256),
        "goal_sha256": goal_hash,
        "command_sha256": command_hash,
        "contract_sha256": str(contract_sha256),
    }
    validate_rca_prod_receipt(
        meta.get("rca_prod_admission_receipt"),
        expected_bindings=bindings,
        hmac_key=hmac_key,
        now=now,
        allow_historical=True,
    )


def _seal_historical_document(payload: Mapping[str, Any]) -> tuple[dict[str, Any], bytes]:
    document = copy.deepcopy(dict(payload))
    document["self_seal"] = {
        "schema_version": HISTORICAL_SELF_SEAL_SCHEMA,
        "algorithm": "sha256",
        "canonicalization": "json-sort-keys-compact-utf8-newline/v1",
        "scope": "document_with_self_seal_sha256_empty",
        "artifact_size_bytes": 0,
        "sha256": "",
    }
    for _ in range(24):
        projection = copy.deepcopy(document)
        projection["self_seal"]["sha256"] = ""
        document["self_seal"]["sha256"] = hashlib.sha256(
            canonical_bytes(projection) + b"\n"
        ).hexdigest()
        data = canonical_bytes(document) + b"\n"
        if document["self_seal"]["artifact_size_bytes"] == len(data):
            return document, data
        document["self_seal"]["artifact_size_bytes"] = len(data)
    raise RcaProdAdmissionError("rca_historical_self_seal_unstable", retryable=False)


def _read_historical_canonical_json(path: Path, label: str) -> tuple[dict[str, Any], bytes]:
    try:
        before = path.lstat()
        if (
            not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
            or before.st_size <= 0 or before.st_size > HISTORICAL_MAX_CONTROL_BYTES
        ):
            raise RcaProdAdmissionError(
                "rca_historical_%s_identity_invalid" % label, retryable=False
            )
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            opened = os.fstat(fd)
            if (
                (opened.st_dev, opened.st_ino, opened.st_size)
                != (before.st_dev, before.st_ino, before.st_size)
                or not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1
            ):
                raise RcaProdAdmissionError(
                    "rca_historical_%s_identity_invalid" % label, retryable=False
                )
            chunks, remaining = [], opened.st_size
            while remaining:
                chunk = os.read(fd, min(1024 * 1024, remaining))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            after = os.fstat(fd)
        finally:
            os.close(fd)
    except RcaProdAdmissionError:
        raise
    except OSError as exc:
        raise RcaProdAdmissionError(
            "rca_historical_%s_unavailable" % label, retryable=True
        ) from exc
    data = b"".join(chunks)
    if (
        remaining != 0
        or (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        != (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns)
    ):
        raise RcaProdAdmissionError(
            "rca_historical_%s_identity_invalid" % label, retryable=False
        )
    try:
        value = _strict_json_loads(data.decode("utf-8"))
    except (UnicodeError, ValueError, json.JSONDecodeError) as exc:
        raise RcaProdAdmissionError(
            "rca_historical_%s_schema_invalid" % label, retryable=False
        ) from exc
    if not isinstance(value, dict) or data != canonical_bytes(value) + b"\n":
        raise RcaProdAdmissionError(
            "rca_historical_%s_schema_invalid" % label, retryable=False
        )
    return value, data


def _historical_successor_release() -> Mapping[str, Any]:
    value = HISTORICAL_SUCCESSOR_RELEASE
    if not isinstance(value, Mapping) or set(value) != HISTORICAL_SUCCESSOR_RELEASE_FIELDS:
        raise RcaProdAdmissionError("rca_historical_successor_release_not_configured", retryable=False)
    fingerprints = value.get("evaluator_fingerprints")
    if (
        not isinstance(fingerprints, Mapping) or not fingerprints
        or any(
            not isinstance(key, str) or not key
            or re.fullmatch(r"[0-9a-f]{64}", str(item or "")) is None
            for key, item in fingerprints.items()
        )
        or not isinstance(value.get("evaluator_version"), str)
        or not value["evaluator_version"]
        or value["evaluator_version"] != "git-%s" % value.get("source_commit")
        or sha256_value(fingerprints) != value.get("evaluator_fingerprints_sha256")
    ):
        raise RcaProdAdmissionError("rca_historical_successor_release_invalid", retryable=False)
    for field in (
        "requirements_contract_hash", "evaluator_fingerprints_sha256",
        "suite_receipt_sha256", "w17_receipt_sha256",
    ):
        _require_hex(value.get(field), "rca_historical_successor_release_invalid")
    for field in ("source_commit", "source_tree"):
        object_id = str(value.get(field) or "")
        if len(object_id) not in {40, 64} or re.fullmatch(r"[0-9a-f]+", object_id) is None:
            raise RcaProdAdmissionError("rca_historical_successor_release_invalid", retryable=False)
    if len(str(value["source_commit"])) != len(str(value["source_tree"])):
        raise RcaProdAdmissionError("rca_historical_successor_release_invalid", retryable=False)
    for field in ("suite_receipt_path", "w17_receipt_path"):
        path = Path(str(value.get(field) or ""))
        try:
            path.relative_to("/mnt/tmp")
        except ValueError as exc:
            raise RcaProdAdmissionError("rca_historical_successor_release_invalid", retryable=False) from exc
        if not path.is_absolute():
            raise RcaProdAdmissionError("rca_historical_successor_release_invalid", retryable=False)
    return value


def _historical_ready_ids(host_tmp_root: Path) -> list[str]:
    reference = HISTORICAL_INPUT_CONTRACT["ready_index"]
    vm_path = Path(reference["path"])
    try:
        host_path = host_tmp_root / vm_path.relative_to("/mnt/tmp")
        data = host_path.read_bytes()
        rows = [
            _strict_json_loads(line.decode("utf-8"))
            for line in data.splitlines()
        ]
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
        raise RcaProdAdmissionError("rca_historical_ready_index_unavailable", retryable=True) from exc
    ids = [row.get("work_item_id") if isinstance(row, Mapping) else None for row in rows]
    if (
        not data.endswith(b"\n")
        or hashlib.sha256(data).hexdigest() != reference["raw_sha256"]
        or sha256_value(rows) != reference["semantic_sha256"]
        or len(ids) != HISTORICAL_ITEMS
        or len(set(ids)) != HISTORICAL_ITEMS
        or any(not isinstance(item, str) or re.fullmatch(r"[0-9]{10}", item) is None for item in ids)
        or sha256_value(ids) != reference["ordered_work_item_ids_sha256"]
    ):
        raise RcaProdAdmissionError("rca_historical_ready_index_invalid", retryable=False)
    return ids


def build_historical_full_rerun_plan(
    request: Any, *, expected_request_sha256: str
) -> HistoricalFullRerunPlan:
    if not isinstance(request, Mapping) or set(request) != HISTORICAL_REQUEST_FIELDS:
        raise RcaProdAdmissionError("rca_historical_request_schema_invalid", retryable=False)
    value = dict(request)
    if value.get("schema_version") != HISTORICAL_REQUEST_SCHEMA:
        raise RcaProdAdmissionError("rca_historical_request_schema_invalid", retryable=False)
    request_id = str(value.get("request_id") or "")
    owner = str(value.get("owner") or "")
    if not TASK_ID_RE.fullmatch(request_id) or not owner or owner != owner.strip():
        raise RcaProdAdmissionError("rca_historical_request_identity_invalid", retryable=False)
    value["request_id"], value["owner"] = request_id, owner
    for field in ("remote_commit", "remote_tree"):
        object_id = str(value.get(field) or "").lower()
        if len(object_id) not in {40, 64} or re.fullmatch(r"[0-9a-f]+", object_id) is None:
            raise RcaProdAdmissionError("rca_historical_source_identity_invalid", retryable=False)
        value[field] = object_id
    if len(value["remote_commit"]) != len(value["remote_tree"]):
        raise RcaProdAdmissionError("rca_historical_source_identity_invalid", retryable=False)
    for field in (
        "canonical_input_raw_sha256", "selection_raw_sha256",
        "selection_identity_raw_sha256", "review_disposition_raw_sha256",
        "ready_index_raw_sha256", "requirements_contract_hash",
        "evaluator_fingerprints_sha256", "suite_receipt_sha256",
        "w17_receipt_sha256",
    ):
        value[field] = _require_hex(value.get(field), "rca_historical_request_identity_invalid")
    raw_pins = {
        "canonical_input_raw_sha256": "canonical_input",
        "selection_raw_sha256": "selection",
        "selection_identity_raw_sha256": "selection_identity",
        "review_disposition_raw_sha256": "review_disposition",
        "ready_index_raw_sha256": "ready_index",
    }
    if any(
        value[field] != HISTORICAL_INPUT_CONTRACT[label]["raw_sha256"]
        for field, label in raw_pins.items()
    ):
        raise RcaProdAdmissionError("rca_historical_input_pin_mismatch", retryable=False)
    release = _historical_successor_release()
    fingerprints = dict(release["evaluator_fingerprints"])
    release_bindings = {
        "remote_commit": release["source_commit"],
        "remote_tree": release["source_tree"],
        "requirements_contract_hash": release["requirements_contract_hash"],
        "evaluator_fingerprints_sha256": release["evaluator_fingerprints_sha256"],
        "suite_receipt_sha256": release["suite_receipt_sha256"],
        "w17_receipt_sha256": release["w17_receipt_sha256"],
    }
    if any(value[field] != expected for field, expected in release_bindings.items()):
        raise RcaProdAdmissionError(
            "rca_historical_successor_identity_mismatch", retryable=False
        )
    request_sha256 = sha256_value(value)
    if request_sha256 != _require_hex(expected_request_sha256, "rca_historical_request_hash_invalid"):
        raise RcaProdAdmissionError("rca_historical_request_hash_mismatch", retryable=False)
    task_id = "g1q3-rca-full308-" + request_sha256[:32]
    task_root = Path("/mnt/tmp") / task_id
    plan_path = task_root / "control/historical-full-chain-plan.json"
    reservation_path = task_root / "control/host-lane-reservation.json"
    plan_id = "plan-" + request_sha256[:32]
    ready_ids = _historical_ready_ids(HISTORICAL_HOST_TMP_ROOT)
    shards, shard_artifacts, offset = [], [], 0
    for index, count in enumerate(HISTORICAL_SHARD_COUNTS, 1):
        ids = ready_ids[offset:offset + count]
        offset += count
        shard_id = "shard-%03d" % index
        shard_path = task_root / "control/shards" / shard_id / "shard-manifest.json"
        shard, shard_data = _seal_historical_document({
            "schema_version": HISTORICAL_SHARD_PLAN_SCHEMA,
            "plan_id": plan_id,
            "shard_id": shard_id,
            "item_count": count,
            "ordered_work_item_ids": ids,
            "ordered_work_item_ids_sha256": sha256_value(ids),
        })
        shard_artifacts.append((shard_path, shard_data))
        shards.append({
            "shard_id": shard_id, "path": str(shard_path),
            "raw_sha256": hashlib.sha256(shard_data).hexdigest(),
            "semantic_sha256": sha256_value(shard), "item_count": count,
            "ordered_work_item_ids_sha256": sha256_value(ids),
        })
    plan = {
        "schema_version": HISTORICAL_PLAN_SCHEMA,
        "plan_id": plan_id,
        "task_id": task_id,
        "run_id": "run-" + request_sha256[:32],
        "attempt_id": "attempt-" + request_sha256[32:56],
        "output_root": str(task_root),
        "source": {"commit": value["remote_commit"], "tree": value["remote_tree"]},
        "canonical_input": dict(HISTORICAL_INPUT_CONTRACT["canonical_input"]),
        "selection": dict(HISTORICAL_INPUT_CONTRACT["selection"]),
        "selection_identity": dict(HISTORICAL_INPUT_CONTRACT["selection_identity"]),
        "review_disposition": copy.deepcopy(HISTORICAL_INPUT_CONTRACT["review_disposition"]),
        "ready_index": dict(HISTORICAL_INPUT_CONTRACT["ready_index"]),
        "requirements_contract_hash": value["requirements_contract_hash"],
        "evaluator_fingerprints": fingerprints,
        "evaluator_fingerprints_sha256": value["evaluator_fingerprints_sha256"],
        "evaluator_version": release["evaluator_version"],
        "release_evidence": {
            "suite": {
                "path": str(release["suite_receipt_path"]),
                "raw_sha256": release["suite_receipt_sha256"],
                "source_commit": release["source_commit"],
                "source_tree": release["source_tree"],
            },
            "w17": {
                "path": str(release["w17_receipt_path"]),
                "raw_sha256": release["w17_receipt_sha256"],
                "source_commit": release["source_commit"],
                "source_tree": release["source_tree"],
            },
        },
        "host_reservation": {
            "schema_version": HISTORICAL_RESERVATION_SCHEMA,
            "path": str(reservation_path),
            "max_global_evaluation_lanes": 3,
        },
        "execution_policy": dict(HISTORICAL_EXECUTION_POLICY),
        "budgets": dict(HISTORICAL_BUDGETS),
        "shards": shards,
    }
    plan, plan_bytes = _seal_historical_document(plan)
    return HistoricalFullRerunPlan(
        value, request_sha256, plan, plan_bytes, hashlib.sha256(plan_bytes).hexdigest(),
        task_id, task_root, plan_path, task_root, reservation_path,
        tuple(shard_artifacts),
    )


def build_historical_full_rerun_execute_argv(plan: HistoricalFullRerunPlan) -> list[str]:
    return [
        "/usr/bin/python3.8", "-B", HISTORICAL_PREPARE, "execute",
        "--plan", str(plan.plan_path), "--plan-sha256", plan.plan_sha256,
    ]


def build_historical_full_rerun_verify_argv(
    plan: HistoricalFullRerunPlan, *, full_chain_output_seal_sha256: str
) -> list[str]:
    seal = _require_hex(full_chain_output_seal_sha256, "rca_historical_final_seal_invalid")
    runner = (
        HISTORICAL_FROZEN_SOURCE_ROOT / plan.task_id / "root" / HISTORICAL_RUNNER
    )
    return [
        "/usr/bin/python3.8", "-B", str(runner), "verify",
        "--run-root", str(plan.output_root),
        "--final-execution-seal-sha256", seal,
    ]


def derive_historical_result_binding(
    plan: HistoricalFullRerunPlan, *, host_tmp_root: Path | None = None,
) -> dict[str, str]:
    root = (host_tmp_root or HISTORICAL_HOST_TMP_ROOT) / plan.task_id
    final, final_raw = _read_historical_canonical_json(
        root / "final/execution-final-seal.json", "final_seal"
    )
    if set(final) != HISTORICAL_FINAL_SEAL_FIELDS:
        raise RcaProdAdmissionError(
            "rca_historical_final_seal_schema_invalid", retryable=False
        )
    run_identity = final.get("run_identity")
    if (
        final.get("schema_version") != HISTORICAL_FINAL_SEAL_SCHEMA
        or not isinstance(run_identity, Mapping)
        or run_identity.get("plan_sha256") != plan.plan_sha256
        or final.get("item_count") != HISTORICAL_ITEMS
        or final.get("terminal_complete") is not True
    ):
        raise RcaProdAdmissionError(
            "rca_historical_final_seal_identity_invalid", retryable=False
        )
    reservation, reservation_raw = _read_historical_canonical_json(
        root / "control/host-lane-reservation.json", "reservation_sidecar"
    )
    if (
        set(reservation) != HISTORICAL_RESERVATION_FIELDS
        or reservation.get("schema_version") != HISTORICAL_RESERVATION_SCHEMA
        or reservation.get("task_id") != plan.task_id
        or reservation.get("plan_sha256") != plan.plan_sha256
    ):
        raise RcaProdAdmissionError(
            "rca_historical_sidecar_identity_invalid", retryable=False
        )
    return {
        "full_chain_output_seal_sha256": hashlib.sha256(final_raw).hexdigest(),
        "host_reservation_raw_sha256": hashlib.sha256(reservation_raw).hexdigest(),
        "host_reservation_semantic_sha256": sha256_value(reservation),
    }


def materialize_historical_full_rerun_plan(
    plan: HistoricalFullRerunPlan, *, host_tmp_root: Path = HISTORICAL_HOST_TMP_ROOT
) -> Path:
    def write(relative: Path, data: bytes) -> Path:
        path = host_tmp_root / plan.task_id / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            if path.read_bytes() != data:
                raise RcaProdAdmissionError("rca_historical_plan_identity_conflict", retryable=False)
            return path
        try:
            with path.open("xb") as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
        except FileExistsError:
            if path.read_bytes() != data:
                raise RcaProdAdmissionError("rca_historical_plan_identity_conflict", retryable=False)
        return path

    for vm_path, data in plan.shard_artifacts:
        write(vm_path.relative_to(plan.task_root), data)
    return write(plan.plan_path.relative_to(plan.task_root), plan.plan_bytes)


def _historical_bootstrap_bindings(plan: HistoricalFullRerunPlan) -> dict[str, Any]:
    return {
        "request_sha256": plan.request_sha256,
        "plan_sha256": plan.plan_sha256,
        "task_id": plan.task_id,
        "host_reservation_path": str(plan.host_reservation_path),
        "max_global_evaluation_lanes": 3,
        "queue_if_blocked": False,
    }


def issue_historical_full_rerun_bootstrap(
    plan: HistoricalFullRerunPlan, *, owner: str, hmac_key: str | bytes | None = None,
    now: datetime | None = None, receipt_id: str | None = None,
    reservation_id: str | None = None,
) -> dict[str, Any]:
    current = _now(now)
    if owner != plan.request["owner"]:
        raise RcaProdAdmissionError("rca_historical_bootstrap_owner_mismatch", retryable=False)
    receipt_id = receipt_id or "bootstrap-" + secrets.token_hex(16)
    reservation_id = reservation_id or "reservation-" + secrets.token_hex(16)
    if not TASK_ID_RE.fullmatch(receipt_id) or not TASK_ID_RE.fullmatch(reservation_id):
        raise RcaProdAdmissionError("rca_historical_bootstrap_identity_invalid", retryable=False)
    body = {
        "schema_version": HISTORICAL_BOOTSTRAP_SCHEMA,
        "receipt_id": receipt_id, "reservation_id": reservation_id,
        "issued_at": current.replace(microsecond=0).isoformat(),
        "expires_at": (current + timedelta(seconds=HISTORICAL_BOOTSTRAP_TTL_SECONDS)).replace(microsecond=0).isoformat(),
        "decision": "allow", "owner": owner, "one_use": True,
        "bindings": _historical_bootstrap_bindings(plan),
    }
    return _sign_receipt(body, _load_hmac_key(hmac_key))


def validate_historical_full_rerun_bootstrap(
    receipt: Any, *, plan: HistoricalFullRerunPlan, expected_owner: str,
    hmac_key: str | bytes | None = None, now: datetime | None = None,
    allow_historical: bool = False,
) -> dict[str, Any]:
    if not isinstance(receipt, Mapping) or set(receipt) != HISTORICAL_BOOTSTRAP_FIELDS:
        raise RcaProdAdmissionError("rca_historical_bootstrap_schema_invalid", retryable=False)
    bindings = receipt.get("bindings")
    if not isinstance(bindings, Mapping) or set(bindings) != HISTORICAL_BOOTSTRAP_BINDINGS:
        raise RcaProdAdmissionError("rca_historical_bootstrap_schema_invalid", retryable=False)
    if (
        receipt.get("schema_version") != HISTORICAL_BOOTSTRAP_SCHEMA
        or receipt.get("decision") != "allow" or receipt.get("one_use") is not True
        or receipt.get("owner") != expected_owner or expected_owner != plan.request["owner"]
        or dict(bindings) != _historical_bootstrap_bindings(plan)
    ):
        raise RcaProdAdmissionError("rca_historical_bootstrap_policy_invalid", retryable=False)
    key, body = _load_hmac_key(hmac_key), canonical_bytes(_receipt_body(receipt))
    if (
        receipt.get("receipt_fingerprint") != hashlib.sha256(body).hexdigest()
        or not hmac.compare_digest(str(receipt.get("hmac_sha256") or ""), hmac.new(key, body, hashlib.sha256).hexdigest())
    ):
        raise RcaProdAdmissionError("rca_historical_bootstrap_signature_invalid", retryable=False)
    current, issued, expires = _now(now), _timestamp(receipt["issued_at"]), _timestamp(receipt["expires_at"])
    if (
        (expires - issued).total_seconds() not in range(1, HISTORICAL_BOOTSTRAP_TTL_SECONDS + 1)
        or issued > current + timedelta(seconds=5)
        or (not allow_historical and not issued - timedelta(seconds=5) <= current <= expires)
    ):
        raise RcaProdAdmissionError("rca_historical_bootstrap_expired")
    return dict(receipt)


def _historical_ledger(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "schema_version": HISTORICAL_LEDGER_SCHEMA, "sequence": 0,
            "max_global_evaluation_lanes": 3, "observed_global_peak": 0,
            "active": {}, "consumed_receipts": {},
        }
    raw = path.read_bytes()
    try:
        value = _strict_json_loads(raw.decode("utf-8"))
    except (UnicodeError, ValueError, json.JSONDecodeError) as exc:
        raise RcaProdAdmissionError("rca_historical_lane_ledger_invalid") from exc
    if (
        not isinstance(value, dict) or set(value) != {
            "schema_version", "sequence", "max_global_evaluation_lanes",
            "observed_global_peak", "active", "consumed_receipts",
        } or value.get("schema_version") != HISTORICAL_LEDGER_SCHEMA
        or value.get("max_global_evaluation_lanes") != 3
        or raw != canonical_bytes(value) + b"\n"
        or not isinstance(value.get("active"), dict)
        or not isinstance(value.get("consumed_receipts"), dict)
    ):
        raise RcaProdAdmissionError("rca_historical_lane_ledger_invalid")
    return value


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    data, temp = canonical_bytes(dict(value)) + b"\n", path.with_name(".%s.%s" % (path.name, secrets.token_hex(8)))
    with temp.open("xb") as stream:
        stream.write(data); stream.flush(); os.fsync(stream.fileno())
    os.replace(temp, path)


def consume_historical_bootstrap_and_reserve_lanes(
    receipt: Any, *, plan: HistoricalFullRerunPlan, expected_owner: str,
    hmac_key: str | bytes | None = None, now: datetime | None = None,
    state_root: Path = HISTORICAL_STATE_ROOT, host_tmp_root: Path = HISTORICAL_HOST_TMP_ROOT,
) -> dict[str, Any]:
    current = _now(now)
    validated = validate_historical_full_rerun_bootstrap(
        receipt, plan=plan, expected_owner=expected_owner, hmac_key=hmac_key, now=current
    )
    state_root.mkdir(parents=True, mode=0o700, exist_ok=True)
    if not stat.S_ISDIR(state_root.lstat().st_mode):
        raise RcaProdAdmissionError("rca_historical_lane_ledger_root_invalid", retryable=False)
    lock_fd = os.open(state_root / "evaluation-lanes.lock", os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        ledger_path, ledger = state_root / "evaluation-lanes.json", _historical_ledger(state_root / "evaluation-lanes.json")
        active = dict(ledger["active"])
        for reservation_id, record in list(active.items()):
            if _timestamp(record["lease_expires_at"]) < current:
                active.pop(reservation_id)
        receipt_id = str(validated["receipt_id"])
        if receipt_id in ledger["consumed_receipts"]:
            raise RcaProdAdmissionError("rca_historical_bootstrap_already_consumed", retryable=False)
        if sum(int(record["lane_count"]) for record in active.values()) + 3 > 3:
            raise RcaProdAdmissionError("rca_historical_evaluation_lanes_unavailable")
        sequence, reservation_id = int(ledger["sequence"]) + 1, str(validated["reservation_id"])
        started = current.replace(microsecond=0).isoformat()
        expires = (current + timedelta(seconds=HISTORICAL_LEASE_SECONDS)).replace(microsecond=0).isoformat()
        sidecar = {
            "schema_version": HISTORICAL_RESERVATION_SCHEMA,
            "receipt_id": receipt_id, "reservation_id": reservation_id,
            "ledger_sequence": sequence, "task_id": plan.task_id,
            "plan_sha256": plan.plan_sha256, "lane_count": 3,
            "started_at": started, "lease_expires_at": expires,
            "observed_global_peak": 3, "max_global_evaluation_lanes": 3,
            "queue_if_blocked": False,
        }
        raw, semantic = canonical_bytes(sidecar) + b"\n", sha256_value(sidecar)
        sidecar_path = host_tmp_root / plan.task_id / "control/host-lane-reservation.json"
        sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        if sidecar_path.exists() and sidecar_path.read_bytes() != raw:
            raise RcaProdAdmissionError("rca_historical_reservation_sidecar_conflict", retryable=False)
        created_sidecar = not sidecar_path.exists()
        if created_sidecar:
            with sidecar_path.open("xb") as stream:
                stream.write(raw); stream.flush(); os.fsync(stream.fileno())
        active[reservation_id] = {"lane_count": 3, "lease_expires_at": expires, "task_id": plan.task_id}
        consumed = dict(ledger["consumed_receipts"])
        consumed[receipt_id] = {
            "reservation_id": reservation_id, "task_id": plan.task_id,
            "plan_sha256": plan.plan_sha256,
            "sidecar_raw_sha256": hashlib.sha256(raw).hexdigest(),
            "sidecar_semantic_sha256": semantic,
        }
        try:
            _atomic_json(ledger_path, {
                **ledger, "sequence": sequence, "observed_global_peak": 3,
                "active": active, "consumed_receipts": consumed,
            })
        except Exception:
            if created_sidecar:
                sidecar_path.unlink(missing_ok=True)
            raise
        return {
            "reservation": sidecar, "path": str(plan.host_reservation_path),
            "raw_sha256": hashlib.sha256(raw).hexdigest(), "semantic_sha256": semantic,
        }
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN); os.close(lock_fd)


def verify_historical_lane_reservation(
    plan: HistoricalFullRerunPlan, *, raw_sha256: str, semantic_sha256: str,
    state_root: Path = HISTORICAL_STATE_ROOT, host_tmp_root: Path = HISTORICAL_HOST_TMP_ROOT,
) -> dict[str, Any]:
    path = host_tmp_root / plan.task_id / "control/host-lane-reservation.json"
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != _require_hex(raw_sha256, "rca_historical_sidecar_hash_invalid"):
        raise RcaProdAdmissionError("rca_historical_sidecar_hash_mismatch", retryable=False)
    try:
        value = _strict_json_loads(raw.decode("utf-8"))
    except (UnicodeError, ValueError, json.JSONDecodeError) as exc:
        raise RcaProdAdmissionError(
            "rca_historical_sidecar_schema_invalid", retryable=False
        ) from exc
    if (
        not isinstance(value, dict)
        or set(value) != HISTORICAL_RESERVATION_FIELDS
        or raw != canonical_bytes(value) + b"\n"
    ):
        raise RcaProdAdmissionError("rca_historical_sidecar_schema_invalid", retryable=False)
    if sha256_value(value) != _require_hex(semantic_sha256, "rca_historical_sidecar_hash_invalid"):
        raise RcaProdAdmissionError("rca_historical_sidecar_semantic_mismatch", retryable=False)
    try:
        started_at = _timestamp(value["started_at"])
        lease_expires_at = _timestamp(value["lease_expires_at"])
    except (TypeError, ValueError) as exc:
        raise RcaProdAdmissionError(
            "rca_historical_sidecar_identity_invalid", retryable=False
        ) from exc
    if (
        value["schema_version"] != HISTORICAL_RESERVATION_SCHEMA
        or not isinstance(value["receipt_id"], str) or not value["receipt_id"]
        or not isinstance(value["reservation_id"], str) or not value["reservation_id"]
        or type(value["ledger_sequence"]) is not int or value["ledger_sequence"] < 1
        or value["task_id"] != plan.task_id or value["plan_sha256"] != plan.plan_sha256
        or type(value["lane_count"]) is not int or value["lane_count"] != 3
        or type(value["max_global_evaluation_lanes"]) is not int
        or value["max_global_evaluation_lanes"] != 3
        or type(value["observed_global_peak"]) is not int
        or value["observed_global_peak"] != 3
        or value["queue_if_blocked"] is not False
        or started_at >= lease_expires_at
    ):
        raise RcaProdAdmissionError("rca_historical_sidecar_identity_invalid", retryable=False)
    ledger = _historical_ledger(state_root / "evaluation-lanes.json")
    record = ledger["consumed_receipts"].get(value["receipt_id"])
    expected = {
        "reservation_id": value["reservation_id"], "task_id": plan.task_id,
        "plan_sha256": plan.plan_sha256,
        "sidecar_raw_sha256": raw_sha256, "sidecar_semantic_sha256": semantic_sha256,
    }
    if record != expected or value["ledger_sequence"] > ledger["sequence"]:
        raise RcaProdAdmissionError("rca_historical_sidecar_ledger_mismatch", retryable=False)
    return value


def release_historical_lane_reservation(
    plan: HistoricalFullRerunPlan, *, receipt_id: str, reservation_id: str,
    raw_sha256: str, semantic_sha256: str, reason: str,
    state_root: Path = HISTORICAL_STATE_ROOT,
    host_tmp_root: Path = HISTORICAL_HOST_TMP_ROOT,
) -> dict[str, Any]:
    if reason not in {"create_failed_missing_reconfirmed", "verify_succeeded"}:
        raise RcaProdAdmissionError(
            "rca_historical_lane_release_reason_invalid", retryable=False
        )
    reservation = verify_historical_lane_reservation(
        plan, raw_sha256=raw_sha256, semantic_sha256=semantic_sha256,
        state_root=state_root, host_tmp_root=host_tmp_root,
    )
    if (
        reservation.get("receipt_id") != receipt_id
        or reservation.get("reservation_id") != reservation_id
    ):
        raise RcaProdAdmissionError(
            "rca_historical_lane_release_identity_mismatch", retryable=False
        )
    lock_path = state_root / "evaluation-lanes.lock"
    try:
        if not stat.S_ISDIR(state_root.lstat().st_mode):
            raise OSError("state root is not a directory")
        lock_fd = os.open(lock_path, os.O_RDWR)
    except OSError as exc:
        raise RcaProdAdmissionError(
            "rca_historical_lane_ledger_unavailable", retryable=True
        ) from exc
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        ledger_path = state_root / "evaluation-lanes.json"
        ledger = _historical_ledger(ledger_path)
        expected_consumed = {
            "reservation_id": reservation_id, "task_id": plan.task_id,
            "plan_sha256": plan.plan_sha256,
            "sidecar_raw_sha256": raw_sha256,
            "sidecar_semantic_sha256": semantic_sha256,
        }
        if ledger["consumed_receipts"].get(receipt_id) != expected_consumed:
            raise RcaProdAdmissionError(
                "rca_historical_lane_release_identity_mismatch", retryable=False
            )
        active = dict(ledger["active"])
        active_record = active.get(reservation_id)
        if active_record is None:
            return {
                "released": False, "already_released": True,
                "reservation_id": reservation_id, "reason": reason,
            }
        if active_record != {
            "lane_count": 3,
            "lease_expires_at": reservation["lease_expires_at"],
            "task_id": plan.task_id,
        }:
            raise RcaProdAdmissionError(
                "rca_historical_lane_release_identity_mismatch", retryable=False
            )
        active.pop(reservation_id)
        _atomic_json(ledger_path, {
            **ledger, "sequence": int(ledger["sequence"]) + 1,
            "active": active,
        })
        return {
            "released": True, "already_released": False,
            "reservation_id": reservation_id, "reason": reason,
        }
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        os.close(lock_fd)
