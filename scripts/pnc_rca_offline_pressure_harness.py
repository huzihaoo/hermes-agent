#!/usr/bin/env python3
"""Offline, deterministic pressure evidence for the RCA durable state machines.

This harness deliberately excludes Kafka networking, Feishu APIs, VM execution,
and LLM work.  It exercises the real source-neutral SQLite intake/outbox and
delivery-effect stores while replacing every external side effect with a local
idempotent fake.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import resource
import sqlite3
import statistics
import sys
import tempfile
import threading
import time
from typing import Any, Iterable, Mapping

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gateway.pnc_rca_control_store import (
    CONTROL_STORE_SCHEMA_VERSION,
    MANUAL_TRIGGER_SCHEMA_VERSION,
    OUTBOX_MAX_CONSECUTIVE_KAFKA_CLAIMS,
    KafkaRecord,
    ManualRcaTriggerRequest,
    RcaControlStore,
)
from gateway.pnc_rca_delivery_store import (
    DELIVERY_STORE_SCHEMA_VERSION,
    RcaDeliveryStore,
)
from gateway.pnc_rca_kafka_contract import WorkflowEventPolicy, WorkflowTransition


REPORT_SCHEMA_VERSION = "pnc_rca_offline_pressure_report_v1"
PLAN_SCHEMA_VERSION = "pnc_rca_offline_pressure_plan_v1"
TOPIC = "feishu-project-workflow-event"
BASE_TIME = datetime(2026, 7, 13, 0, 0, tzinfo=timezone.utc)
MAX_TOTAL_CASES = 100_000
MAX_REPORT_BYTES = 64 * 1024
MAX_WORKERS = 32
EXCLUDED_SURFACES = (
    "Kafka broker/network/TLS/auth/consumer-group rebalance and commit latency",
    "Feishu/Meegle HTTP availability, rate limits, auth, and remote visibility",
    "VM admission, scheduling, filesystem mounts, process execution, and MCAP",
    "URL crawling and remote issue-data readability",
    "RCA/LLM reasoning quality, model latency, token use, and HTML generation",
    "launchd lifecycle, resident-process restart, and production cutover",
)


@dataclass(frozen=True)
class Profile:
    total_cases: int
    kafka_ratio: float
    failure_rate: float
    timeout_rate: float
    duplicate_rate: float
    arrival_span_seconds: float = 0.0


PROFILES: dict[str, Profile] = {
    "expected-50-day": Profile(50, 0.80, 0.02, 0.01, 0.02, 86_400.0),
    "burst-1000": Profile(1_000, 0.80, 0.02, 0.01, 0.02),
    "manual-flood": Profile(400, 0.05, 0.02, 0.01, 0.02),
    "kafka-flood": Profile(400, 0.95, 0.02, 0.01, 0.02),
    "retry-crash-25": Profile(240, 0.80, 0.125, 0.125, 0.02),
    "duplicate-source": Profile(200, 0.80, 0.02, 0.01, 0.50),
    "delivery-exact-once": Profile(200, 0.80, 0.00, 0.25, 0.10),
}


@dataclass(frozen=True)
class HarnessConfig:
    profile: str = "burst-1000"
    total_cases: int = 1_000
    kafka_ratio: float = 0.80
    workers: int = 4
    failure_rate: float = 0.02
    timeout_rate: float = 0.01
    duplicate_rate: float = 0.02
    seed: int = 20260713
    high_watermark: int = 100
    resume_watermark: int = 40
    batch_size: int = 10
    poll_interval_ms: int = 10
    arrival_span_seconds: float = 0.0
    run_timeout_seconds: float = 300.0

    def validated(self) -> "HarnessConfig":
        if self.profile not in PROFILES:
            raise ValueError("unsupported profile")
        if isinstance(self.total_cases, bool) or not 1 <= self.total_cases <= MAX_TOTAL_CASES:
            raise ValueError(f"total_cases must be in [1, {MAX_TOTAL_CASES}]")
        for name, value in (
            ("kafka_ratio", self.kafka_ratio),
            ("failure_rate", self.failure_rate),
            ("timeout_rate", self.timeout_rate),
            ("duplicate_rate", self.duplicate_rate),
        ):
            if isinstance(value, bool) or not math.isfinite(value) or not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be a finite ratio in [0, 1]")
        if self.failure_rate + self.timeout_rate > 1.0:
            raise ValueError("failure_rate + timeout_rate must not exceed 1")
        if isinstance(self.workers, bool) or not 1 <= self.workers <= MAX_WORKERS:
            raise ValueError(f"workers must be in [1, {MAX_WORKERS}]")
        if isinstance(self.high_watermark, bool) or self.high_watermark < 2:
            raise ValueError("high_watermark must be at least 2")
        if (
            isinstance(self.resume_watermark, bool)
            or self.resume_watermark < 0
            or self.resume_watermark >= self.high_watermark
        ):
            raise ValueError("resume_watermark must be in [0, high_watermark)")
        if isinstance(self.batch_size, bool) or not 1 <= self.batch_size <= 1_000:
            raise ValueError("batch_size must be in [1, 1000]")
        if (
            isinstance(self.poll_interval_ms, bool)
            or not 1 <= self.poll_interval_ms <= 60_000
        ):
            raise ValueError("poll_interval_ms must be in [1, 60000]")
        if isinstance(self.seed, bool) or not 0 <= self.seed <= 2**31 - 1:
            raise ValueError("seed must be in [0, 2147483647]")
        if (
            isinstance(self.arrival_span_seconds, bool)
            or not math.isfinite(self.arrival_span_seconds)
            or self.arrival_span_seconds < 0.0
            or self.arrival_span_seconds > 31_536_000.0
        ):
            raise ValueError("arrival_span_seconds must be in [0, 31536000]")
        if (
            isinstance(self.run_timeout_seconds, bool)
            or not math.isfinite(self.run_timeout_seconds)
            or not 1.0 <= self.run_timeout_seconds <= 3_600.0
        ):
            raise ValueError("run_timeout_seconds must be in [1, 3600]")
        return self


@dataclass(frozen=True)
class Case:
    index: int
    source: str
    issue_id: int
    arrival_seconds: float
    duplicate: bool


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _bucket(seed: int, namespace: str, identity: str) -> float:
    digest = hashlib.sha256(f"{seed}:{namespace}:{identity}".encode()).digest()
    return int.from_bytes(digest[:8], "big") / float(2**64)


def _percentile(values: Iterable[float], percentile: float) -> float:
    ordered = sorted(float(item) for item in values)
    if not ordered:
        return 0.0
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * percentile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def _latency_summary(values: Iterable[float]) -> dict[str, float]:
    materialized = list(values)
    return {
        "p50": round(_percentile(materialized, 0.50), 3),
        "p95": round(_percentile(materialized, 0.95), 3),
        "p99": round(_percentile(materialized, 0.99), 3),
        "max": round(max(materialized, default=0.0), 3),
    }


def _max_streak(values: Iterable[str], target: str) -> int:
    best = 0
    current = 0
    for value in values:
        if value == target:
            current += 1
            best = max(best, current)
        else:
            current = 0
    return best


def _peak_rss_bytes() -> int:
    value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value if sys.platform == "darwin" else value * 1024


def _db_bytes(path: Path) -> int:
    total = 0
    for candidate in (path, Path(f"{path}-wal"), Path(f"{path}-shm")):
        try:
            total += candidate.stat().st_size
        except FileNotFoundError:
            pass
    return total


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def validate_temporary_path(value: str | Path, *, kind: str) -> Path:
    raw = Path(value).expanduser()
    if not raw.is_absolute():
        raise ValueError(f"{kind} path must be absolute")
    lowered = raw.as_posix().lower()
    if ".hermes/runtime/pnc_agent" in lowered or ".openclaw/runtime/pnc_agent" in lowered:
        raise ValueError(f"{kind} path points at a live RCA runtime")
    resolved = raw.resolve(strict=False)
    roots = {
        Path(tempfile.gettempdir()).resolve(),
        Path("/tmp").resolve(),
        Path("/var/tmp").resolve(),
        Path("/private/tmp").resolve(),
    }
    if not any(_is_relative_to(resolved, root) for root in roots):
        raise ValueError(f"{kind} path must be below an OS temporary root")
    if raw.exists() and raw.is_symlink():
        raise ValueError(f"{kind} path must not be a symlink")
    if raw.exists() and not raw.is_file():
        raise ValueError(f"{kind} path must name a file")
    return resolved


def _policy() -> WorkflowEventPolicy:
    return WorkflowEventPolicy(
        topic=TOPIC,
        policy_version="offline-pressure-v1",
        project_keys=frozenset({"offline-project"}),
        project_simple_names=frozenset({"g1q3"}),
        work_item_type_keys=frozenset({"problem"}),
        status_change_types=frozenset({"Reached"}),
        transitions=(
            WorkflowTransition(
                state_key="new-problem", pre_status=1, cur_status=2
            ),
        ),
    )


def _kafka_record(case: Case) -> KafkaRecord:
    value = {
        "id": case.issue_id,
        "name": f"offline pressure case {case.index}",
        "nodes": [
            {
                "state_key": "new-problem",
                "pre_status": 1,
                "cur_status": 2,
            }
        ],
        "project_key": "offline-project",
        "project_simple_name": "g1q3",
        "status_change_type": "Reached",
        "updated_at": 1_783_900_000_000 + case.index,
        "work_item_type_key": "problem",
    }
    return KafkaRecord(
        topic=TOPIC,
        partition=case.index % 8,
        offset=case.index // 8,
        value=_canonical_json(value).encode(),
        key=f"offline-{case.issue_id}".encode(),
        timestamp_ms=1_783_900_000_000 + case.index,
    )


def _manual_request(case: Case, seed: int) -> ManualRcaTriggerRequest:
    return ManualRcaTriggerRequest(
        schema_version=MANUAL_TRIGGER_SCHEMA_VERSION,
        issue_url=(
            f"https://project.feishu.cn/g1q3/issue/detail/{case.issue_id}"
        ),
        mode="run_or_join",
        reason="offline_pressure_harness",
        platform="feishu",
        chat_id="oc_offline_pressure",
        thread_id=f"topic:om_pressure_thread_{case.index}",
        message_id=f"om_pressure_{seed}_{case.index}",
        requester_id="ou_offline_pressure",
    )


def build_cases(config: HarnessConfig) -> list[Case]:
    kafka_count = int(round(config.total_cases * config.kafka_ratio))
    ranked = sorted(
        range(config.total_cases),
        key=lambda index: hashlib.sha256(
            f"{config.seed}:source:{index}".encode()
        ).digest(),
    )
    kafka_indices = set(ranked[:kafka_count])
    spacing = (
        config.arrival_span_seconds / max(1, config.total_cases - 1)
        if config.arrival_span_seconds
        else 0.0
    )
    return [
        Case(
            index=index,
            source="kafka" if index in kafka_indices else "manual",
            issue_id=9_100_000_000 + index,
            arrival_seconds=spacing * index,
            duplicate=(
                _bucket(config.seed, "duplicate", str(index)) < config.duplicate_rate
            ),
        )
        for index in range(config.total_cases)
    ]


class LocalEffectFake:
    """Thread-safe marker lookup with one durable-looking remote id per effect."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._remote: dict[str, str] = {}
        self.write_calls = 0
        self.duplicate_write_calls = 0
        self.reconciliations = 0

    def write(self, effect_key: str) -> str:
        with self._lock:
            self.write_calls += 1
            if effect_key in self._remote:
                self.duplicate_write_calls += 1
                return self._remote[effect_key]
            remote_id = "offline-effect-" + hashlib.sha256(effect_key.encode()).hexdigest()
            self._remote[effect_key] = remote_id
            return remote_id

    def lookup(self, effect_key: str) -> str | None:
        with self._lock:
            remote_id = self._remote.get(effect_key)
            if remote_id:
                self.reconciliations += 1
            return remote_id

    @property
    def unique_effects(self) -> int:
        with self._lock:
            return len(self._remote)


def _sqlite_error_kind(exc: BaseException) -> str:
    message = str(exc).lower()
    if "locked" in message or "busy" in message:
        return "busy_or_locked"
    return "sqlite_other"


def _materialize_config(args: argparse.Namespace) -> HarnessConfig:
    profile = PROFILES[args.profile]
    return HarnessConfig(
        profile=args.profile,
        total_cases=args.total_cases if args.total_cases is not None else profile.total_cases,
        kafka_ratio=args.kafka_ratio if args.kafka_ratio is not None else profile.kafka_ratio,
        workers=args.workers,
        failure_rate=(
            args.failure_rate if args.failure_rate is not None else profile.failure_rate
        ),
        timeout_rate=(
            args.timeout_rate if args.timeout_rate is not None else profile.timeout_rate
        ),
        duplicate_rate=(
            args.duplicate_rate
            if args.duplicate_rate is not None
            else profile.duplicate_rate
        ),
        seed=args.seed,
        high_watermark=args.high_watermark,
        resume_watermark=args.resume_watermark,
        batch_size=args.batch_size,
        poll_interval_ms=args.poll_interval_ms,
        arrival_span_seconds=profile.arrival_span_seconds,
        run_timeout_seconds=args.run_timeout_seconds,
    ).validated()


def build_plan(config: HarnessConfig) -> dict[str, Any]:
    cases = build_cases(config.validated())
    source_counts = {
        source: sum(item.source == source for item in cases)
        for source in ("kafka", "manual")
    }
    return {
        "schema_version": PLAN_SCHEMA_VERSION,
        "mode": "plan",
        "parameters": asdict(config),
        "planned_workload": {
            "unique_cases": config.total_cases,
            "source_counts": source_counts,
            "duplicate_replays": sum(item.duplicate for item in cases),
            "burst": config.arrival_span_seconds == 0.0,
            "virtual_arrival_span_seconds": config.arrival_span_seconds,
        },
        "store_contract": {
            "control_schema": CONTROL_STORE_SCHEMA_VERSION,
            "delivery_schema": DELIVERY_STORE_SCHEMA_VERSION,
            "public_apis": [
                "RcaControlStore.ingest_record/admit_manual_trigger",
                "RcaControlStore.claim_outbox/retry_outbox/complete_outbox",
                "RcaDeliveryStore.backfill_completed_submissions/claim_due_watch",
                "RcaDeliveryStore.create_terminal_delivery",
                "RcaDeliveryStore.claim_due_effect/mark_effect_write_started",
                "RcaDeliveryStore.reschedule_effect/complete_effect",
            ],
        },
        "fixed_acceptance_profiles": sorted(PROFILES),
        "acceptance_scenarios": {
            "expected_volume": ["expected-50-day"],
            "burst_capacity": ["burst-1000"],
            "dual_source_fairness": ["manual-flood", "kafka-flood"],
            "retry_and_lease_recovery": ["retry-crash-25"],
            "source_and_delivery_exact_once": [
                "duplicate-source",
                "delivery-exact-once",
            ],
        },
        "excluded_surfaces": list(EXCLUDED_SURFACES),
    }


def run_harness(config: HarnessConfig, *, control_path: str | Path) -> dict[str, Any]:
    config = config.validated()
    db_path = validate_temporary_path(control_path, kind="control")
    if db_path.exists():
        raise ValueError("control path already exists; refusing to reuse a database")
    db_path.parent.mkdir(parents=True, exist_ok=True)

    run_started = time.perf_counter()
    run_deadline = run_started + config.run_timeout_seconds

    def check_deadline(stage: str) -> None:
        if time.perf_counter() > run_deadline:
            raise RuntimeError(
                f"offline pressure run exceeded {config.run_timeout_seconds:g}s "
                f"during {stage}"
            )

    rss_before = _peak_rss_bytes()
    store = RcaControlStore(db_path, busy_timeout_ms=5_000)
    delivery_store = RcaDeliveryStore(db_path, busy_timeout_ms=5_000)
    db_baseline = _db_bytes(db_path)
    policy = _policy()
    cases = build_cases(config)
    virtual_now = BASE_TIME
    virtual_seconds = 0.0
    case_cursor = 0
    admitted_wall: dict[str, float] = {}
    offered_virtual: dict[str, float] = {}
    completed_wall_latency_ms: list[float] = []
    completed_virtual_latency_ms: list[float] = []
    source_completed = {"kafka": 0, "manual": 0}
    source_by_submission: dict[str, str] = {}
    claim_sequence: list[str] = []
    duplicate_replays = 0
    duplicate_replays_idempotent = 0
    intake_seconds = 0.0
    outbox_seconds = 0.0
    delivery_seconds = 0.0
    max_backlog = 0
    highwater_reached = False
    intake_paused = False
    pause_events = 0
    resume_events = 0
    circuit_blocked_rounds = 0
    outbox_failures_injected = 0
    outbox_timeouts_injected = 0
    outbox_lease_recoveries = 0
    sqlite_errors = {"busy_or_locked": 0, "sqlite_other": 0}
    metrics_lock = threading.Lock()
    claim_lock = threading.Lock()

    opened = store.open_dispatcher_circuit(
        reason_code="offline_harness_probe", now=virtual_now
    )
    closed = None

    def admit(case: Case) -> None:
        nonlocal duplicate_replays, duplicate_replays_idempotent, intake_seconds
        started = time.perf_counter()
        check_deadline("intake")
        if case.source == "kafka":
            record = _kafka_record(case)
            result = store.ingest_record(record, policy=policy, submit_enabled=True)
            if case.duplicate:
                duplicate_replays += 1
                replay = store.ingest_record(record, policy=policy, submit_enabled=True)
                if not replay.raw_inserted and replay.submission_key == result.submission_key:
                    duplicate_replays_idempotent += 1
        else:
            request = _manual_request(case, config.seed)
            result = store.admit_manual_trigger(
                request,
                allowed_chat_ids={"oc_offline_pressure"},
                submit_enabled=True,
                active_policy=policy,
                outbox_high_watermark=config.high_watermark,
                now=virtual_now,
            )
            if case.duplicate:
                duplicate_replays += 1
                replay = store.admit_manual_trigger(
                    request,
                    allowed_chat_ids={"oc_offline_pressure"},
                    submit_enabled=True,
                    active_policy=policy,
                    outbox_high_watermark=config.high_watermark,
                    now=virtual_now,
                )
                if (
                    replay.submission_key == result.submission_key
                    and replay.reason == "idempotent_source_replay"
                ):
                    duplicate_replays_idempotent += 1
        elapsed = time.perf_counter() - started
        intake_seconds += elapsed
        source_by_submission[result.submission_key] = case.source
        admitted_wall[result.submission_key] = time.perf_counter()
        offered_virtual[result.submission_key] = case.arrival_seconds

    def process_outbox_one(worker_index: int, current: datetime) -> str:
        nonlocal outbox_failures_injected, outbox_timeouts_injected
        nonlocal outbox_lease_recoveries
        try:
            with claim_lock:
                claim = store.claim_outbox(
                    lease_owner=f"offline-outbox-{worker_index}",
                    lease_seconds=1,
                    max_age_seconds=31_536_000,
                    now=current,
                )
                if claim is None:
                    return "empty"
                source = "kafka" if claim.source_topic else "manual"
                claim_sequence.append(source)
            decision = _bucket(config.seed, "outbox", claim.submission_key)
            if claim.attempt == 1 and decision < config.timeout_rate:
                with metrics_lock:
                    outbox_timeouts_injected += 1
                return "timeout"
            if (
                claim.attempt == 1
                and decision < config.timeout_rate + config.failure_rate
            ):
                store.retry_outbox(
                    outbox_id=claim.outbox_id,
                    lease_token=claim.lease_token,
                    error_code="offline_injected_retry",
                    delay_seconds=0,
                    max_age_seconds=31_536_000,
                    now=current,
                )
                with metrics_lock:
                    outbox_failures_injected += 1
                return "retry"
            store.complete_outbox(
                outbox_id=claim.outbox_id,
                lease_token=claim.lease_token,
                result={
                    "success": True,
                    "task_id": claim.submission_key,
                    "submission_key": claim.submission_key,
                    "offline_fake": True,
                },
                now=current,
            )
            source = source_by_submission[claim.submission_key]
            wall_latency = (
                time.perf_counter() - admitted_wall[claim.submission_key]
            ) * 1000.0
            virtual_latency = max(
                0.0,
                (current - BASE_TIME).total_seconds()
                - offered_virtual[claim.submission_key],
            ) * 1000.0
            with metrics_lock:
                if claim.attempt > 1:
                    outbox_lease_recoveries += int(
                        decision < config.timeout_rate
                    )
                source_completed[source] += 1
                completed_wall_latency_ms.append(wall_latency)
                completed_virtual_latency_ms.append(virtual_latency)
            return "completed"
        except sqlite3.Error as exc:
            with metrics_lock:
                sqlite_errors[_sqlite_error_kind(exc)] += 1
            return "sqlite_error"

    def dispatch_outbox_wave(executor: ThreadPoolExecutor) -> list[str]:
        nonlocal circuit_blocked_rounds, closed
        check_deadline("outbox")
        if store.dispatcher_circuit().is_open:
            circuit_blocked_rounds += 1
            closed = store.close_dispatcher_circuit(now=virtual_now)
            return ["circuit_blocked"]
        current = virtual_now
        futures = [
            executor.submit(process_outbox_one, index % config.workers, current)
            for index in range(config.workers * config.batch_size)
        ]
        return [future.result() for future in futures]

    outbox_started = time.perf_counter()
    with ThreadPoolExecutor(
        max_workers=config.workers, thread_name_prefix="rca-offline-outbox"
    ) as executor:
        while case_cursor < len(cases) or store.dispatch_backlog_count() > 0:
            check_deadline("outbox")
            backlog = store.dispatch_backlog_count()
            max_backlog = max(max_backlog, backlog)
            if (
                intake_paused
                and backlog <= config.resume_watermark
                and case_cursor < len(cases)
            ):
                intake_paused = False
                resume_events += 1
            while case_cursor < len(cases) and not intake_paused:
                case = cases[case_cursor]
                if case.arrival_seconds > virtual_seconds:
                    if store.dispatch_backlog_count() > 0:
                        break
                    virtual_seconds = case.arrival_seconds
                    virtual_now = BASE_TIME + timedelta(seconds=virtual_seconds)
                backlog = store.dispatch_backlog_count()
                max_backlog = max(max_backlog, backlog)
                if backlog >= config.high_watermark:
                    highwater_reached = True
                    intake_paused = True
                    pause_events += 1
                    break
                admit(case)
                case_cursor += 1
                backlog = store.dispatch_backlog_count()
                max_backlog = max(max_backlog, backlog)
                if backlog >= config.high_watermark and case_cursor < len(cases):
                    highwater_reached = True
                    intake_paused = True
                    pause_events += 1
                    break

            backlog_before = store.dispatch_backlog_count()
            if backlog_before == 0:
                continue
            outcomes = dispatch_outbox_wave(executor)
            backlog_after = store.dispatch_backlog_count()
            max_backlog = max(max_backlog, backlog_after)
            progressed = any(
                item in {"completed", "retry", "timeout"} for item in outcomes
            )
            if not progressed or (
                backlog_after > 0
                and all(item in {"empty", "sqlite_error"} for item in outcomes)
            ):
                virtual_seconds += 1.001
            else:
                virtual_seconds += config.poll_interval_ms / 1000.0
            virtual_now = BASE_TIME + timedelta(seconds=virtual_seconds)
    outbox_seconds = time.perf_counter() - outbox_started

    delivery_started = time.perf_counter()
    backfilled = delivery_store.backfill_completed_submissions(
        limit=max(1, config.total_cases), now=virtual_now
    )
    watch_claim_lock = threading.Lock()

    def create_delivery_one(worker_index: int, current: datetime) -> str:
        try:
            with watch_claim_lock:
                watch = delivery_store.claim_due_watch(
                    lease_owner=f"offline-collector-{worker_index}",
                    lease_seconds=5,
                    now=current,
                )
            if watch is None:
                return "empty"
            delivery_store.create_terminal_delivery(
                claim=watch,
                status={"success": False, "state": "offline_harness_terminal"},
                outcome="terminal_failed",
                terminal_state="offline_harness_terminal",
                error_code="offline_harness_no_llm_execution",
                error_detail="synthetic terminal envelope used only for delivery closure",
                now=current,
            )
            return "created"
        except sqlite3.Error as exc:
            with metrics_lock:
                sqlite_errors[_sqlite_error_kind(exc)] += 1
            return "sqlite_error"

    with ThreadPoolExecutor(
        max_workers=config.workers, thread_name_prefix="rca-offline-collector"
    ) as executor:
        while True:
            check_deadline("delivery_collection")
            snapshot = delivery_store.backpressure_snapshot(now=virtual_now)
            remaining_watches = snapshot.pending_watches + snapshot.running_watches
            if remaining_watches == 0:
                break
            current = virtual_now
            futures = [
                executor.submit(create_delivery_one, index, current)
                for index in range(config.workers * config.batch_size)
            ]
            outcomes = [future.result() for future in futures]
            if not any(item == "created" for item in outcomes):
                virtual_seconds += 5.001
            else:
                virtual_seconds += config.poll_interval_ms / 1000.0
            virtual_now = BASE_TIME + timedelta(seconds=virtual_seconds)

    remote = LocalEffectFake()
    effect_claim_lock = threading.Lock()
    delivery_failures_injected = 0
    delivery_timeouts_injected = 0
    delivery_lease_recoveries = 0

    def process_effect_one(worker_index: int, current: datetime) -> str:
        nonlocal delivery_failures_injected, delivery_timeouts_injected
        nonlocal delivery_lease_recoveries
        try:
            with effect_claim_lock:
                claim = delivery_store.claim_due_effect(
                    lease_owner=f"offline-delivery-{worker_index}",
                    lease_seconds=1,
                    max_age_seconds=31_536_000,
                    now=current,
                )
            if claim is None:
                return "empty"
            decision = _bucket(config.seed, "delivery", claim.effect_key)
            if (
                claim.attempt == 1
                and decision < config.timeout_rate + config.failure_rate
                and decision >= config.timeout_rate
            ):
                delivery_store.reschedule_effect(
                    claim=claim,
                    error_code="offline_injected_retry",
                    error_detail="prewrite deterministic failure",
                    delay_seconds=0,
                    uncertain=False,
                    max_age_seconds=31_536_000,
                    now=current,
                )
                with metrics_lock:
                    delivery_failures_injected += 1
                return "retry"
            if claim.previous_status == "uncertain":
                remote_id = remote.lookup(claim.effect_key)
                if not remote_id:
                    raise RuntimeError("offline uncertain effect marker is missing")
                delivery_store.complete_effect(
                    claim=claim,
                    outcome="reconciled",
                    remote_id=remote_id,
                    receipt={"offline": True, "reconciled": True},
                    now=current,
                )
                with metrics_lock:
                    delivery_lease_recoveries += 1
                return "reconciled"
            delivery_store.mark_effect_write_started(claim=claim, now=current)
            remote_id = remote.write(claim.effect_key)
            if claim.attempt == 1 and decision < config.timeout_rate:
                with metrics_lock:
                    delivery_timeouts_injected += 1
                return "timeout"
            delivery_store.complete_effect(
                claim=claim,
                outcome="ack",
                remote_id=remote_id,
                receipt={"offline": True, "reconciled": False},
                now=current,
            )
            return "completed"
        except sqlite3.Error as exc:
            with metrics_lock:
                sqlite_errors[_sqlite_error_kind(exc)] += 1
            return "sqlite_error"

    with ThreadPoolExecutor(
        max_workers=config.workers, thread_name_prefix="rca-offline-delivery"
    ) as executor:
        while True:
            check_deadline("delivery_dispatch")
            snapshot = delivery_store.backpressure_snapshot(now=virtual_now)
            if snapshot.unresolved_effects == 0:
                break
            current = virtual_now
            futures = [
                executor.submit(process_effect_one, index, current)
                for index in range(config.workers * config.batch_size)
            ]
            outcomes = [future.result() for future in futures]
            progressed = any(
                item in {"completed", "reconciled", "retry", "timeout"}
                for item in outcomes
            )
            if not progressed or all(
                item in {"empty", "sqlite_error"} for item in outcomes
            ):
                virtual_seconds += 1.001
            else:
                virtual_seconds += config.poll_interval_ms / 1000.0
            virtual_now = BASE_TIME + timedelta(seconds=virtual_seconds)
    delivery_seconds = time.perf_counter() - delivery_started

    outbox_rows = store.list_rows("rca_outbox")
    effect_rows = delivery_store.list_rows("rca_delivery_effects")
    job_rows = delivery_store.list_rows("rca_delivery_jobs")
    final_snapshot = delivery_store.backpressure_snapshot(now=virtual_now)
    total_seconds = max(time.perf_counter() - run_started, 1e-9)
    db_final = _db_bytes(db_path)
    rss_after = _peak_rss_bytes()
    source_counts = {
        source: sum(case.source == source for case in cases)
        for source in ("kafka", "manual")
    }
    completed_count = sum(row["status"] == "completed" for row in outbox_rows)
    succeeded_effects = sum(row["status"] == "succeeded" for row in effect_rows)
    attempts = sum(int(row["attempt"]) for row in outbox_rows)
    effect_attempts = sum(int(row["attempt"]) for row in effect_rows)
    retry_count = max(0, attempts - completed_count)
    effect_retry_count = max(0, effect_attempts - succeeded_effects)
    minority = "kafka" if source_counts["kafka"] <= source_counts["manual"] else "manual"
    minority_positions = [
        index for index, source in enumerate(claim_sequence, start=1) if source == minority
    ]
    no_starvation = all(
        count == 0 or source_completed[source] == count
        for source, count in source_counts.items()
    )
    first_minority_position = min(minority_positions, default=0)
    first_half_bound = max(1, math.ceil(config.total_cases / 2))
    fairness_passed = (
        no_starvation
        and (
            not all(source_counts.values())
            or 0 < first_minority_position <= first_half_bound
        )
    )
    backpressure_expected = (
        config.arrival_span_seconds == 0.0
        and config.total_cases > config.high_watermark
    )

    checks = {
        "all_unique_cases_completed": completed_count == config.total_cases,
        "all_delivery_effects_succeeded": (
            bool(effect_rows)
            and succeeded_effects == len(effect_rows)
            and final_snapshot.unresolved_work == 0
        ),
        "duplicate_sources_idempotent": (
            duplicate_replays_idempotent == duplicate_replays
            and len(outbox_rows) == config.total_cases
        ),
        "remote_effect_exact_once": (
            remote.duplicate_write_calls == 0
            and remote.unique_effects == len(effect_rows)
        ),
        "lease_timeouts_recovered": (
            outbox_lease_recoveries == outbox_timeouts_injected
            and delivery_lease_recoveries == delivery_timeouts_injected
        ),
        "source_fairness_no_starvation": fairness_passed,
        "backpressure_high_resume_observed": (
            not backpressure_expected
            or (
                highwater_reached
                and pause_events >= 1
                and resume_events >= 1
                and max_backlog <= config.high_watermark
            )
        ),
        "circuit_open_blocks_then_operator_closes": (
            opened.is_open
            and circuit_blocked_rounds == 1
            and closed is not None
            and not closed.is_open
        ),
        "sqlite_no_busy_or_lock_errors": sqlite_errors["busy_or_locked"] == 0,
        "throughput_exceeds_expected_daily_rate": (
            config.total_cases / total_seconds >= 50.0 / 86_400.0
        ),
        "bounded_memory_growth": max(0, rss_after - rss_before) <= 512 * 1024 * 1024,
        "bounded_db_growth": (
            max(0, db_final - db_baseline) / config.total_cases <= 256 * 1024
        ),
    }
    deterministic_material = {
        "parameters": asdict(config),
        "source_counts": source_counts,
        "duplicate_replays": duplicate_replays,
        "outbox": {
            "rows": len(outbox_rows),
            "completed": completed_count,
            "attempts": attempts,
            "failures": outbox_failures_injected,
            "timeouts": outbox_timeouts_injected,
            "recoveries": outbox_lease_recoveries,
        },
        "delivery": {
            "jobs": len(job_rows),
            "effects": len(effect_rows),
            "succeeded": succeeded_effects,
            "attempts": effect_attempts,
            "failures": delivery_failures_injected,
            "timeouts": delivery_timeouts_injected,
            "recoveries": delivery_lease_recoveries,
            "remote_unique": remote.unique_effects,
            "remote_duplicate_writes": remote.duplicate_write_calls,
        },
    }
    report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "mode": "run",
        "result": "pass" if all(checks.values()) else "fail",
        "deterministic_fingerprint": _sha256_json(deterministic_material),
        "parameters": asdict(config),
        "workload": {
            "unique_cases": config.total_cases,
            "source_counts": source_counts,
            "duplicate_replays": duplicate_replays,
            "virtual_arrival_span_seconds": config.arrival_span_seconds,
            "synthetic_terminal_envelopes": backfilled,
        },
        "throughput": {
            "unique_cases_per_second": round(config.total_cases / total_seconds, 3),
            "outbox_completions_per_second": round(
                completed_count / max(outbox_seconds, 1e-9), 3
            ),
            "delivery_effects_per_second": round(
                succeeded_effects / max(delivery_seconds, 1e-9), 3
            ),
            "intake_active_seconds": round(intake_seconds, 6),
            "outbox_stage_seconds": round(outbox_seconds, 6),
            "delivery_stage_seconds": round(delivery_seconds, 6),
            "drain_seconds": round(total_seconds, 6),
        },
        "queue_latency_ms": {
            "wall_admission_to_outbox_completion": _latency_summary(
                completed_wall_latency_ms
            ),
            "virtual_offer_to_outbox_completion": _latency_summary(
                completed_virtual_latency_ms
            ),
        },
        "reliability": {
            "outbox": {
                "rows": len(outbox_rows),
                "completed": completed_count,
                "attempts": attempts,
                "retries": retry_count,
                "failures_injected": outbox_failures_injected,
                "lease_timeouts_injected": outbox_timeouts_injected,
                "lease_recoveries": outbox_lease_recoveries,
            },
            "delivery": {
                "jobs": len(job_rows),
                "effects": len(effect_rows),
                "succeeded": succeeded_effects,
                "attempts": effect_attempts,
                "retries_or_reclaims": effect_retry_count,
                "failures_injected": delivery_failures_injected,
                "lease_timeouts_injected": delivery_timeouts_injected,
                "lease_recoveries": delivery_lease_recoveries,
                "reconciliations": remote.reconciliations,
            },
            "idempotency": {
                "duplicate_source_replays": duplicate_replays,
                "idempotent_source_replays": duplicate_replays_idempotent,
                "remote_write_calls": remote.write_calls,
                "remote_unique_effects": remote.unique_effects,
                "duplicate_remote_effect_writes": remote.duplicate_write_calls,
            },
            "sqlite_errors": sqlite_errors,
        },
        "fairness": {
            "source_completed": source_completed,
            "claim_counts_including_retries": {
                source: claim_sequence.count(source)
                for source in ("kafka", "manual")
            },
            "minority_source": minority,
            "minority_first_claim_position": first_minority_position,
            "minority_claimed_before_half_drain": (
                not all(source_counts.values())
                or 0 < first_minority_position <= first_half_bound
            ),
            "max_consecutive_kafka_claims_observed": _max_streak(
                claim_sequence, "kafka"
            ),
            "max_consecutive_manual_claims_observed": _max_streak(
                claim_sequence, "manual"
            ),
            "store_kafka_contention_bound": OUTBOX_MAX_CONSECUTIVE_KAFKA_CLAIMS,
            "no_starvation": no_starvation,
        },
        "backpressure": {
            "high_watermark_reached": highwater_reached,
            "pause_events": pause_events,
            "resume_events": resume_events,
            "max_outbox_backlog": max_backlog,
            "configured_high_watermark": config.high_watermark,
            "configured_resume_watermark": config.resume_watermark,
        },
        "circuit": {
            "probe_open_state": opened.state,
            "dispatch_rounds_blocked_while_open": circuit_blocked_rounds,
            "post_operator_reset_state": closed.state if closed else "not_reset",
        },
        "resources": {
            "control_path": str(db_path),
            "db_baseline_bytes": db_baseline,
            "db_final_bytes": db_final,
            "db_growth_bytes": max(0, db_final - db_baseline),
            "db_growth_bytes_per_case": round(
                max(0, db_final - db_baseline) / config.total_cases, 3
            ),
            "process_peak_rss_before_bytes": rss_before,
            "process_peak_rss_after_bytes": rss_after,
            "process_peak_rss_delta_bytes": max(0, rss_after - rss_before),
        },
        "slo": {
            "passed": all(checks.values()),
            "checks": checks,
            "notes": (
                "Throughput is local state-machine throughput, not end-to-end RCA. "
                "Virtual latency applies configured poll intervals without sleeping."
            ),
        },
        "excluded_surfaces": list(EXCLUDED_SURFACES),
        "limitations": [
            "SQLite worker concurrency is real; Kafka/manual arrivals and failures are synthetic.",
            "The terminal envelope intentionally avoids claiming LLM or HTML performance.",
            "Wall timings vary by host load; deterministic_fingerprint excludes timings and paths.",
            "The local fake proves marker idempotency/reconciliation logic only, not Feishu semantics.",
        ],
    }
    encoded = _canonical_json(report).encode("utf-8")
    if len(encoded) > MAX_REPORT_BYTES:
        raise RuntimeError("offline pressure report exceeded bounded JSON limit")
    return report


def _write_output(path_value: str | None, payload: Mapping[str, Any]) -> None:
    if not path_value:
        return
    path = validate_temporary_path(path_value, kind="output")
    if path.exists():
        raise ValueError("output path already exists; refusing to overwrite")
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (_canonical_json(payload) + "\n").encode("utf-8")
    if len(encoded) > MAX_REPORT_BYTES:
        raise ValueError("output JSON exceeds the bounded report limit")
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Offline RCA SQLite pressure and fault-injection evidence"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ("plan", "run"):
        command = subparsers.add_parser(name)
        command.add_argument("--profile", choices=sorted(PROFILES), default="burst-1000")
        command.add_argument("--total-cases", type=int)
        command.add_argument("--kafka-ratio", type=float)
        command.add_argument("--workers", type=int, default=4)
        command.add_argument("--failure-rate", type=float)
        command.add_argument("--timeout-rate", type=float)
        command.add_argument("--duplicate-rate", type=float)
        command.add_argument("--seed", type=int, default=20260713)
        command.add_argument("--high-watermark", type=int, default=100)
        command.add_argument("--resume-watermark", type=int, default=40)
        command.add_argument("--batch-size", type=int, default=10)
        command.add_argument("--poll-interval-ms", type=int, default=10)
        command.add_argument("--run-timeout-seconds", type=float, default=300.0)
        command.add_argument("--output-path")
        if name == "run":
            command.add_argument("--control-path")
    return parser


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
        config = _materialize_config(args)
        if args.command == "plan":
            payload = build_plan(config)
        elif args.control_path:
            payload = run_harness(config, control_path=args.control_path)
        else:
            with tempfile.TemporaryDirectory(prefix="pnc-rca-offline-pressure-") as root:
                payload = run_harness(
                    config, control_path=Path(root) / "control.sqlite3"
                )
        _write_output(args.output_path, payload)
        print(_canonical_json(payload))
        return 0 if payload.get("result", "pass") == "pass" else 1
    except (OSError, RuntimeError, ValueError) as exc:
        error = {
            "schema_version": REPORT_SCHEMA_VERSION,
            "mode": "error",
            "result": "fail",
            "error_code": type(exc).__name__,
            "error_detail": str(exc)[:500],
            "excluded_surfaces": list(EXCLUDED_SURFACES),
        }
        print(_canonical_json(error))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
