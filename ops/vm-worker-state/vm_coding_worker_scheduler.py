#!/usr/bin/env python3
"""Slot scheduler for VM coding worker shared-state v2.

Dry-run inspects ``dispatch/pending`` and reports which tasks would run under
conservative lane/user/repo slots. Experimental execute mode is fail-closed and
intended for isolated roots only until explicitly cut over.
"""
from __future__ import annotations

import argparse
import hashlib
import fcntl
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None

try:
    import shared_state_v2  # type: ignore
except Exception:  # pragma: no cover
    shared_state_v2 = None

try:
    import vm_stale_task_reconciler  # type: ignore
except Exception:  # pragma: no cover
    vm_stale_task_reconciler = None

try:
    import vm_resource_preflight  # type: ignore
except Exception:  # pragma: no cover
    vm_resource_preflight = None

DEFAULT_ROOT = Path("/home/mini/.hermes/shared-state")
DEFAULT_CONFIG = Path("/home/mini/.hermes/worker-state/vm_scheduler.yaml")
DEFAULT_LOG = Path("/home/mini/.hermes/workspace-coding/vm_coding_worker.scheduler.log")
DEFAULT_LOCK = Path("/home/mini/.hermes/worker-state/vm_coding_worker_scheduler.lock")
DEFAULT_WORKER = Path("/home/mini/.hermes/worker-state/vm_coding_worker_v2.py")
DEFAULT_EXEC_LOG_DIR = Path("/home/mini/.hermes/workspace-coding/scheduler-exec")
DEFAULT_LOCAL_EXEC_ROOT = Path("/home/mini/.openclaw/worker-state/vm-scheduler-isolated-shared-state")
MOUNTED_EXEC_ROOT_PREFIXES = (Path("/mnt"), Path("/media"), Path("/run/user"))
TASK_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
LANES = {"heavy", "standard", "fast"}
HEAVY_RE = re.compile(r"mcap|pb|dbc|parse|generate|validate|evaluation|batch|长程|批处理", re.I)
FAST_RE = re.compile(r"status|状态|path|路径|noop|检查", re.I)
WRITE_RE = re.compile(r"write|edit|modify|patch|implement|refactor|fix|新增|修改|实现|修复|重构", re.I)
HEAVY_RESOURCE_CLASSES = {"cpu_heavy", "io_heavy", "pnc_data", "pnc_data_read", "pnc_data_write", "network_heavy", "real_tool_dnp_eval_canary"}
FAST_RESOURCE_CLASSES = {"cpu_light", "io_light", "network_light"}
WRITE_RESOURCE_CLASSES = {"repo_write", "pnc_data_write"}
READ_RESOURCE_CLASSES = {"repo_read", "pnc_data_read", "real_tool_dnp_eval_canary"}

DEFAULTS = {
    "global_slots": 2,
    "heavy_slots": 1,
    "standard_slots": 1,
    "fast_slots": 1,
    "per_user_default_slots": 1,
    "owner_extra_slots": 1,
    "per_repo_write_slots": 1,
    "shared_nested_repo_write_slots": 1,
}


def load_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"_load_error": str(exc), "_path": str(path)}


def task_text(task: dict[str, Any]) -> str:
    fields: list[str] = []
    for key in ("title", "goal", "task_type", "summary", "message"):
        val = task.get(key)
        if val:
            fields.append(str(val))
    for container in (task.get("meta"), task.get("payload")):
        if isinstance(container, dict):
            for key in ("title", "goal", "task_type", "summary", "message"):
                val = container.get(key)
                if val:
                    fields.append(str(val))
    return "\n".join(fields)


def nested_get(task: dict[str, Any], *keys: str) -> Any:
    sources: list[Any] = [task, task.get("meta"), task.get("payload"), task.get("_meta_file")]
    meta_file = task.get("_meta_file")
    if isinstance(meta_file, dict):
        sources.extend([meta_file.get("meta"), meta_file.get("payload")])
    for source in sources:
        if isinstance(source, dict):
            for key in keys:
                val = source.get(key)
                if val not in (None, ""):
                    return val
    return None


def hydrate_task(root: Path, task: dict[str, Any]) -> dict[str, Any]:
    payload = dict(task)
    candidates: list[Path] = []
    if payload.get("meta_path"):
        candidates.append(Path(str(payload["meta_path"])))
    if payload.get("task_dir"):
        candidates.append(Path(str(payload["task_dir"])) / "meta.json")
    task_id = str(payload.get("task_id") or "").strip()
    if task_id:
        candidates.append(root / "tasks" / task_id / "meta.json")
    for candidate in candidates:
        if candidate.exists():
            meta = load_json(candidate)
            if isinstance(meta, dict) and not meta.get("_load_error"):
                payload["_meta_file"] = meta
                break
    return payload


def classify(task: dict[str, Any]) -> dict[str, Any]:
    explicit_lane = nested_get(task, "lane")
    lane = str(explicit_lane).strip().lower() if explicit_lane else ""
    explicit_resource_class = nested_get(task, "resource_class", "resource")
    resource_class = str(explicit_resource_class).strip().lower() if explicit_resource_class else "unknown"
    text = task_text(task)
    if lane not in LANES:
        if resource_class in HEAVY_RESOURCE_CLASSES:
            lane = "heavy"
        elif resource_class in FAST_RESOURCE_CLASSES:
            lane = "fast"
        elif HEAVY_RE.search(text):
            lane = "heavy"
        elif FAST_RE.search(text):
            lane = "fast"
        else:
            lane = "standard"
    repo_scope = str(nested_get(task, "repo_scope", "repo", "repo_root") or "unknown")
    user_scope = str(nested_get(
        task,
        "user_scope",
        "requester",
        "requester_name",
        "user_name",
        "user_id",
        "owner",
    ) or "unknown")
    risk_resource = " ".join(str(nested_get(task, k) or "") for k in ("risk", "risk_class", "resource", "resource_class"))
    explicit_mutates_repo = nested_get(task, "mutates_repo", "repo_write")
    if isinstance(explicit_mutates_repo, bool):
        mutates_repo = explicit_mutates_repo
    elif isinstance(explicit_mutates_repo, str):
        mutates_repo = explicit_mutates_repo.strip().lower() in {"1", "true", "yes", "on"}
    else:
        mutates_repo = False
    if mutates_repo:
        is_write = True
    elif resource_class in WRITE_RESOURCE_CLASSES:
        is_write = True
    elif resource_class in READ_RESOURCE_CLASSES:
        is_write = False
    else:
        is_write = bool(WRITE_RE.search(" ".join([text, risk_resource])))
    return {"lane": lane, "repo_scope": repo_scope, "user_scope": user_scope, "resource_class": resource_class, "is_write": is_write}


def load_config(path: Path) -> dict[str, int]:
    cfg = dict(DEFAULTS)
    if path.exists():
        if yaml is not None:
            data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        else:
            data = {}
            for line in path.read_text(encoding="utf-8").splitlines():
                line = line.split("#", 1)[0].strip()
                if ":" in line:
                    k, v = line.split(":", 1)
                    data[k.strip()] = int(v.strip())
        for key, value in data.items():
            if key in cfg:
                cfg[key] = int(value)
    return cfg


@dataclass
class SlotManager:
    config: dict[str, int]
    global_used: int = 0
    lane_used: dict[str, int] = field(default_factory=lambda: {"heavy": 0, "standard": 0, "fast": 0})
    user_used: dict[str, int] = field(default_factory=dict)
    repo_write_used: dict[str, int] = field(default_factory=dict)
    nested_repo_write_used: int = 0

    def admit(self, item: dict[str, Any]) -> str | None:
        lane = item["lane"]
        user = item["user_scope"]
        repo = item["repo_scope"]
        if self.global_used >= self.config["global_slots"]:
            return "global_slot_full"
        if self.lane_used.get(lane, 0) >= self.config[f"{lane}_slots"]:
            return f"{lane}_slot_full"
        user_limit = self.config["per_user_default_slots"]
        if str(user) in {"胡子豪", "owner", "admin"}:
            user_limit += self.config.get("owner_extra_slots", 0)
        if self.user_used.get(user, 0) >= user_limit:
            return "user_slot_full"
        if item.get("is_write"):
            if self.repo_write_used.get(repo, 0) >= self.config["per_repo_write_slots"]:
                return "repo_write_slot_full"
            if "nested" in str(repo).lower() and self.nested_repo_write_used >= self.config["shared_nested_repo_write_slots"]:
                return "shared_nested_repo_write_slot_full"
        return None

    def reserve(self, item: dict[str, Any]) -> None:
        lane, user, repo = item["lane"], item["user_scope"], item["repo_scope"]
        self.global_used += 1
        self.lane_used[lane] = self.lane_used.get(lane, 0) + 1
        self.user_used[user] = self.user_used.get(user, 0) + 1
        if item.get("is_write"):
            self.repo_write_used[repo] = self.repo_write_used.get(repo, 0) + 1
            if "nested" in str(repo).lower():
                self.nested_repo_write_used += 1

    def release(self, item: dict[str, Any]) -> None:
        lane, user, repo = item["lane"], item["user_scope"], item["repo_scope"]
        self.global_used = max(0, self.global_used - 1)
        self.lane_used[lane] = max(0, self.lane_used.get(lane, 0) - 1)
        self.user_used[user] = max(0, self.user_used.get(user, 0) - 1)
        if item.get("is_write"):
            self.repo_write_used[repo] = max(0, self.repo_write_used.get(repo, 0) - 1)
            if "nested" in str(repo).lower():
                self.nested_repo_write_used = max(0, self.nested_repo_write_used - 1)

    def snapshot(self) -> dict[str, Any]:
        capacity = {
            "global": int(self.config.get("global_slots", 0)),
            "heavy": int(self.config.get("heavy_slots", 0)),
            "standard": int(self.config.get("standard_slots", 0)),
            "fast": int(self.config.get("fast_slots", 0)),
        }
        utilization = {
            "global": int(self.global_used),
            "heavy": int(self.lane_used.get("heavy", 0)),
            "standard": int(self.lane_used.get("standard", 0)),
            "fast": int(self.lane_used.get("fast", 0)),
        }
        free = {key: max(0, capacity[key] - utilization[key]) for key in capacity}
        return {"global_used": self.global_used, "lane_used": dict(self.lane_used), "user_used": dict(self.user_used), "repo_write_used": dict(self.repo_write_used), "nested_repo_write_used": self.nested_repo_write_used, "capacity": capacity, "utilization": utilization, "free": free, "config": self.config}



def non_global_block_reason(manager: SlotManager, item: dict[str, Any]) -> str | None:
    """Return a lane/user/repo blocker while ignoring global slot pressure."""
    lane = item["lane"]
    user = item["user_scope"]
    repo = item["repo_scope"]
    if manager.lane_used.get(lane, 0) >= manager.config[f"{lane}_slots"]:
        return f"{lane}_slot_full"
    user_limit = manager.config["per_user_default_slots"]
    if str(user) in {"胡子豪", "owner", "admin"}:
        user_limit += manager.config.get("owner_extra_slots", 0)
    if manager.user_used.get(user, 0) >= user_limit:
        return "user_slot_full"
    if item.get("is_write"):
        if manager.repo_write_used.get(repo, 0) >= manager.config["per_repo_write_slots"]:
            return "repo_write_slot_full"
        if "nested" in str(repo).lower() and manager.nested_repo_write_used >= manager.config["shared_nested_repo_write_slots"]:
            return "shared_nested_repo_write_slot_full"
    return None


def read_dispatch(root: Path, queue: str) -> list[dict[str, Any]]:
    dispatch_dir = root / "dispatch" / queue
    tasks = []
    if not dispatch_dir.exists():
        return tasks
    for path in sorted(dispatch_dir.glob("*.json")):
        payload = load_json(path)
        if not isinstance(payload, dict):
            continue
        payload.setdefault("task_id", path.stem)
        payload["_dispatch_path"] = str(path)
        tasks.append(hydrate_task(root, payload))
    return tasks


def read_pending(root: Path) -> list[dict[str, Any]]:
    return read_dispatch(root, "pending")


def task_record(task: dict[str, Any]) -> dict[str, Any]:
    classified = classify(task)
    return {"task_id": task.get("task_id"), **classified}


def ids_by_lane(records: list[dict[str, Any]]) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {lane: [] for lane in ["heavy", "standard", "fast"]}
    for item in records:
        lane = str(item.get("lane") or "standard")
        task_id = item.get("task_id")
        if lane in grouped and task_id not in (None, ""):
            grouped[lane].append(str(task_id))
    return grouped


def ids_by_resource_class(records: list[dict[str, Any]]) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {}
    for item in records:
        resource_class = str(item.get("resource_class") or "unknown")
        task_id = item.get("task_id")
        if task_id in (None, ""):
            continue
        grouped.setdefault(resource_class, []).append(str(task_id))
    return {key: grouped[key] for key in sorted(grouped)}


def counts_by_resource_class(records: list[dict[str, Any]]) -> dict[str, int]:
    return {key: len(value) for key, value in ids_by_resource_class(records).items()}


def running_worker_pids(running: dict[str, dict[str, Any]]) -> dict[str, int]:
    pids: dict[str, int] = {}
    for task_id, child in running.items():
        proc = child.get("process") if isinstance(child, dict) else None
        pid = getattr(proc, "pid", None)
        if pid is not None:
            pids[str(task_id)] = int(pid)
    return pids


def config_fingerprint(config: dict[str, int], config_path: Path | None = None) -> dict[str, Any]:
    payload = json.dumps(config, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    out: dict[str, Any] = {"sha256": hashlib.sha256(payload).hexdigest(), "source": "loaded_config"}
    if config_path is not None:
        out["path"] = str(config_path)
        try:
            data = config_path.read_bytes()
            out["file_sha256"] = hashlib.sha256(data).hexdigest()
        except OSError:
            out["file_sha256"] = None
    return out


def public_record(item: dict[str, Any]) -> dict[str, Any]:
    return {"task_id": item.get("task_id"), "lane": item.get("lane"), "user_scope": item.get("user_scope"), "repo_scope": item.get("repo_scope"), "resource_class": item.get("resource_class", "unknown"), "is_write": bool(item.get("is_write"))}



def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {'1', 'true', 'yes', 'on'}


def codex_lane_status(task: dict[str, Any]) -> dict[str, Any]:
    executor = str(nested_get(task, 'executor_type') or '').strip().lower()
    backend = str(nested_get(task, 'agent_backend', 'coding_agent_backend') or '').strip().lower().replace('_', '-')
    enabled_raw = nested_get(task, 'codex_backend_enabled')
    enabled_present = enabled_raw is not None
    enabled = _truthy(enabled_raw)
    intended = executor == 'coding_agent' or backend in {'codex', 'codex-cli', 'openai-codex'} or enabled_present
    valid = executor == 'coding_agent' and backend == 'codex' and enabled
    reasons: list[str] = []
    if not intended:
        reasons.append('not an intended Codex/coding-agent task')
    else:
        if executor != 'coding_agent':
            reasons.append('executor_type must be coding_agent')
        if backend != 'codex':
            reasons.append('agent_backend must be codex')
        if not enabled:
            reasons.append('codex_backend_enabled must be true')
    return {
        'task_id': task.get('task_id'),
        'intended_codex': intended,
        'valid_codex_metadata': valid,
        'executor_type': executor,
        'agent_backend': backend,
        'codex_backend_enabled': enabled,
        'resource_class': str(nested_get(task, 'resource_class') or 'unknown'),
        'reasons': reasons,
    }


def codex_dry_run_selector(tasks: list[dict[str, Any]]) -> dict[str, Any]:
    eligible: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for task in tasks:
        status = codex_lane_status(task)
        if status['valid_codex_metadata']:
            eligible.append(status)
        elif status['intended_codex']:
            rejected.append(status)
        else:
            skipped.append(status)
    return {
        'selector_version': 1,
        'eligible_count': len(eligible),
        'rejected_count': len(rejected),
        'skipped_count': len(skipped),
        'eligible': eligible,
        'rejected': rejected,
        'skipped': skipped,
        'side_effects': 'none; scheduler dry-run selector does not claim or move dispatch files',
    }


def scan_stale_claims(root: Path, *, limit: int = 20) -> dict[str, Any]:
    """Read-only pre-dispatch stale claimed scan.

    Phase 1 must surface stale claimed tasks before scheduling decisions, but the
    scheduler must not reconcile/move tasks implicitly.  The existing reconciler
    stays opt-in; this hook only reports candidate counts and item actions.
    """
    if vm_stale_task_reconciler is None:
        return {"available": False, "error": "vm_stale_task_reconciler_unavailable"}
    try:
        result = vm_stale_task_reconciler.scan_stale_tasks(
            shared_root=root,
            worker_root=Path("/home/mini/.hermes/worker-state"),
            states=["claimed"],
            dry_run=True,
            limit=limit,
        )
        return {
            "available": True,
            "dry_run": bool(result.get("dry_run")),
            "states": result.get("states", ["claimed"]),
            "scanned": int(result.get("scanned") or 0),
            "candidates": int(result.get("candidates") or 0),
            "items": [
                {"task_id": item.get("task_id"), "action": item.get("action")}
                for item in result.get("items", [])
            ],
        }
    except Exception as exc:  # pragma: no cover - operational safeguard
        return {"available": False, "error": str(exc)}


def build_plan(root: Path, config: dict[str, int]) -> dict[str, Any]:
    stale_claim_scan = scan_stale_claims(root)
    active = read_dispatch(root, "claimed")
    active_records = [task_record(task) for task in active]
    pending = read_pending(root)
    pending_records = [task_record(task) for task in pending]
    active_by_lane = {lane: 0 for lane in ["heavy", "standard", "fast"]}
    pending_by_lane = {lane: 0 for lane in ["heavy", "standard", "fast"]}
    for item in active_records:
        lane = str(item.get("lane") or "standard")
        if lane in active_by_lane:
            active_by_lane[lane] += 1
    for item in pending_records:
        lane = str(item.get("lane") or "standard")
        if lane in pending_by_lane:
            pending_by_lane[lane] += 1

    budget_helper = vm_resource_preflight
    if budget_helper is None:
        try:
            import vm_resource_preflight as budget_helper  # type: ignore
        except Exception:
            budget_helper = None

    effective_slots = None
    concurrency_budget = None
    concurrency_budget_verdict = None
    effective_config = dict(config)
    effective_profile = 'static'
    budget_state = 'unknown'
    downgrade_reasons: list[str] = []
    if budget_helper is not None:
        try:
            if hasattr(budget_helper, 'compute_effective_scheduler_slots'):
                effective_slots = budget_helper.compute_effective_scheduler_slots(
                    base_config=config,
                    active_heavy=active_by_lane.get('heavy', 0),
                    pending_fast=pending_by_lane.get('fast', 0),
                    pending_standard=pending_by_lane.get('standard', 0),
                    lane='heavy',
                    artifact_root='/mnt/tmp',
                )
                # Effective slots are a resource-pressure cap, never an implicit
                # production config expansion.  Temporary N7 windows must still be
                # expressed in the static config; preflight may only keep or lower it.
                effective_config.update({
                    'global_slots': min(int(config.get('global_slots', DEFAULTS['global_slots'])), int(effective_slots.get('global_slots_effective', effective_config.get('global_slots', DEFAULTS['global_slots'])))),
                    'heavy_slots': min(int(config.get('heavy_slots', DEFAULTS['heavy_slots'])), int(effective_slots.get('heavy_slots_effective', effective_config.get('heavy_slots', DEFAULTS['heavy_slots'])))),
                    'standard_slots': min(int(config.get('standard_slots', DEFAULTS['standard_slots'])), int(effective_slots.get('standard_slots_effective', effective_config.get('standard_slots', DEFAULTS['standard_slots'])))),
                    'fast_slots': min(int(config.get('fast_slots', DEFAULTS['fast_slots'])), int(effective_slots.get('fast_slots_effective', effective_config.get('fast_slots', DEFAULTS['fast_slots'])))),
                })
                effective_profile = str(effective_slots.get('effective_profile') or 'static')
                budget_state = str(effective_slots.get('budget_state') or 'unknown')
                downgrade_reasons = list(effective_slots.get('downgrade_reasons') or [])
                concurrency_budget = effective_slots.get('budget')
            else:
                concurrency_budget = budget_helper.concurrency_budget(
                    lane='heavy',
                    artifact_root='/mnt/tmp',
                    active_heavy=active_by_lane.get('heavy', 0),
                )
            if concurrency_budget is None and effective_slots is not None:
                concurrency_budget = effective_slots.get('budget')
            if hasattr(budget_helper, 'concurrency_budget_verdict'):
                concurrency_budget_verdict = budget_helper.concurrency_budget_verdict(
                    budget=concurrency_budget,
                    target_heavy=int(effective_config.get('heavy_slots', DEFAULTS['heavy_slots'])),
                    warn_only=True,
                )
        except Exception as exc:
            concurrency_budget = {'error': str(exc)}
            concurrency_budget_verdict = {'mode': 'warn_only', 'state': 'unknown', 'reasons': ['budget_error'], 'error': str(exc)}
            effective_slots = {'effective_profile': 'static', 'budget_state': 'unknown', 'downgrade_reasons': ['effective_slots_error'], 'error': str(exc)}
            effective_profile = 'static'
            budget_state = 'unknown'
            downgrade_reasons = ['effective_slots_error']

    if effective_slots is None:
        effective_slots = {
            'effective_profile': effective_profile,
            'budget_state': budget_state,
            'global_slots_effective': int(effective_config.get('global_slots', DEFAULTS['global_slots'])),
            'heavy_slots_effective': int(effective_config.get('heavy_slots', DEFAULTS['heavy_slots'])),
            'standard_slots_effective': int(effective_config.get('standard_slots', DEFAULTS['standard_slots'])),
            'fast_slots_effective': int(effective_config.get('fast_slots', DEFAULTS['fast_slots'])),
            'downgrade_reasons': downgrade_reasons,
            'inputs': {},
        }

    static_global = int(config.get('global_slots', DEFAULTS['global_slots']))
    static_heavy = int(config.get('heavy_slots', DEFAULTS['heavy_slots']))
    static_standard = int(config.get('standard_slots', DEFAULTS['standard_slots']))
    static_fast = int(config.get('fast_slots', DEFAULTS['fast_slots']))
    eff_global = int(effective_config.get('global_slots', DEFAULTS['global_slots']))
    eff_heavy = int(effective_config.get('heavy_slots', DEFAULTS['heavy_slots']))
    eff_standard = int(effective_config.get('standard_slots', DEFAULTS['standard_slots']))
    eff_fast = int(effective_config.get('fast_slots', DEFAULTS['fast_slots']))
    effective_slots_summary = f'heavy {static_heavy} -> {eff_heavy}, global {static_global} -> {eff_global}, profile {effective_profile}/{budget_state}'
    pressure_summary = ', '.join(downgrade_reasons) if downgrade_reasons else 'none'
    slot_capacity_summary = f'static[g={static_global} h={static_heavy} s={static_standard} f={static_fast}] effective[g={eff_global} h={eff_heavy} s={eff_standard} f={eff_fast}]'

    manager = SlotManager(effective_config)
    for item in active_records:
        manager.reserve(item)

    would_run: list[dict[str, Any]] = []
    would_wait: list[dict[str, Any]] = []
    deferred: list[tuple[dict[str, Any], str]] = []
    global_full = False
    for item in pending_records:
        record = public_record(item)
        if global_full:
            reason = non_global_block_reason(manager, item) or "global_slot_full"
            would_wait.append({**record, "reason": reason, "wait_reason": reason, "capacity_reason": "global_slot_full"})
            continue
        reason = manager.admit(item)
        if reason is None:
            manager.reserve(item)
            would_run.append({**record, "reason": "slot_available"})
        elif reason == "global_slot_full":
            global_full = True
            public_reason = non_global_block_reason(manager, item) or reason
            would_wait.append({**record, "reason": public_reason, "wait_reason": public_reason, "capacity_reason": reason})
        else:
            deferred.append((item, reason))

    progressed = True
    while progressed and not global_full:
        progressed = False
        next_deferred: list[tuple[dict[str, Any], str]] = []
        for item, previous_reason in deferred:
            record = public_record(item)
            # If the previous blocker was lane/user/repo capacity, preserve it
            # even after later bypassed work fills the global slot. Otherwise an
            # emergency heavy=0 decision can be reported misleadingly as global
            # pressure rather than heavy lane closure.
            if previous_reason != "global_slot_full":
                lane = item["lane"]
                user = item["user_scope"]
                repo = item["repo_scope"]
                user_limit = manager.config["per_user_default_slots"]
                if str(user) in {"胡子豪", "owner", "admin"}:
                    user_limit += manager.config.get("owner_extra_slots", 0)
                still_blocked = False
                if previous_reason == f"{lane}_slot_full":
                    still_blocked = manager.lane_used.get(lane, 0) >= manager.config[f"{lane}_slots"]
                elif previous_reason == "user_slot_full":
                    still_blocked = manager.user_used.get(user, 0) >= user_limit
                elif previous_reason == "repo_write_slot_full":
                    still_blocked = manager.repo_write_used.get(repo, 0) >= manager.config["per_repo_write_slots"]
                elif previous_reason == "shared_nested_repo_write_slot_full":
                    still_blocked = "nested" in str(repo).lower() and manager.nested_repo_write_used >= manager.config["shared_nested_repo_write_slots"]
                if still_blocked:
                    next_deferred.append((item, previous_reason))
                    continue
            reason = manager.admit(item)
            if reason is None:
                manager.reserve(item)
                would_run.append({**record, "reason": "slot_available_after_bypass", "bypassed_wait_reason": previous_reason})
                progressed = True
            elif reason == "global_slot_full":
                global_full = True
                public_reason = previous_reason if previous_reason != "global_slot_full" else reason
                would_wait.append({**record, "reason": public_reason, "wait_reason": public_reason, "capacity_reason": reason})
            else:
                next_deferred.append((item, reason))
        deferred = next_deferred

    for item, reason in deferred:
        record = public_record(item)
        would_wait.append({**record, "reason": reason, "wait_reason": reason})

    codex_selector = codex_dry_run_selector(pending)
    return {"ts": datetime.now().replace(microsecond=0).isoformat(), "state": "dry_run", "event": "scheduler_cycle", "root": str(root), "active_count": len(active_records), "pending_count": len(pending_records), "codex_dry_run_selector": codex_selector, "active_by_lane": active_by_lane, "pending_by_lane": pending_by_lane, "active_by_resource_class": counts_by_resource_class(active_records), "pending_by_resource_class": counts_by_resource_class(pending_records), "active_task_ids": [item.get("task_id") for item in active_records], "pending_task_ids": [item.get("task_id") for item in pending_records], "active_task_ids_by_lane": ids_by_lane(active_records), "pending_task_ids_by_lane": ids_by_lane(pending_records), "active_task_ids_by_resource_class": ids_by_resource_class(active_records), "pending_task_ids_by_resource_class": ids_by_resource_class(pending_records), "would_run": would_run, "would_wait": would_wait, "would_run_by_resource_class": counts_by_resource_class(would_run), "would_wait_by_resource_class": counts_by_resource_class(would_wait), "stale_claim_scan": stale_claim_scan, "slot_snapshot": manager.snapshot(), "config_fingerprint": config_fingerprint(config), "effective_profile": effective_profile, "budget_state": budget_state, "effective_slots": effective_slots, "effective_config": effective_config, "downgrade_reasons": downgrade_reasons, "effective_slots_summary": effective_slots_summary, "pressure_summary": pressure_summary, "slot_capacity_summary": slot_capacity_summary, "concurrency_budget": concurrency_budget, "concurrency_budget_verdict": concurrency_budget_verdict}

def write_plan_output(result: dict[str, Any], path: Path | None) -> None:
    if not path:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def path_is_relative_to(path: Path, base: Path) -> bool:
    try:
        path.resolve().relative_to(base.resolve())
        return True
    except ValueError:
        return False


def is_suspect_mounted_execute_root(path: Path) -> bool:
    resolved = path.resolve()
    return any(path_is_relative_to(resolved, prefix) for prefix in MOUNTED_EXEC_ROOT_PREFIXES)


def validate_execute_root(root: Path, *, allow_mounted: bool = False, allow_live: bool = False) -> str | None:
    """Return a rejection reason when an execute root is unsafe by default.

    Execute mode mutates sqlite-backed shared-state queues. Mounted/CIFS/NAS tmp
    roots have shown sqlite locking failures, so the scheduler defaults to a
    local worker-state runtime root. The live root remains fail-closed unless a
    narrow task-id allowlist was supplied and the owner has explicitly approved
    a scoped canary.
    """
    if root.resolve() == DEFAULT_ROOT.resolve() and not allow_live:
        return "execute mode refuses live shared-state root"
    if is_suspect_mounted_execute_root(root) and not allow_mounted:
        return "execute mode refuses mounted/CIFS/NAS execute root by default; use a local worker-state root or pass --allow-mounted-execute-root for isolated experiments"
    return None


def build_shadow_report(start_plan: dict[str, Any], end_plan: dict[str, Any], samples: list[dict[str, Any]], *, started_at: str, ended_at: str, live_root: Path, daemon_proof: dict[str, Any] | None = None, next_canary_plan: dict[str, Any] | None = None) -> dict[str, Any]:
    would_run_ids: list[str] = []
    would_wait_ids: list[str] = []
    active_ids: set[str] = set()
    for sample in samples:
        would_run_ids.extend(str(x.get("task_id")) for x in sample.get("would_run", []))
        would_wait_ids.extend(str(x.get("task_id")) for x in sample.get("would_wait", []))
        active_ids.update(str(x) for x in sample.get("active_task_ids", []))
    before = {"pending": start_plan.get("pending_count", 0), "claimed": start_plan.get("active_count", 0)}
    after = {"pending": end_plan.get("pending_count", 0), "claimed": end_plan.get("active_count", 0)}
    return {
        "start_time": started_at,
        "end_time": ended_at,
        "sampled_cycles": len(samples),
        "live_root": str(live_root),
        "counts_before": before,
        "counts_after": after,
        "pending_changed": start_plan.get("pending_task_ids") != end_plan.get("pending_task_ids"),
        "claimed_changed": start_plan.get("active_task_ids") != end_plan.get("active_task_ids"),
        "would_run_aggregate": sorted(set(would_run_ids)),
        "would_wait_aggregate": sorted(set(would_wait_ids)),
        "active_task_ids_observed": sorted(active_ids),
        "slot_snapshot": end_plan.get("slot_snapshot", {}),
        "live_daemon_status_unchanged_proof": daemon_proof or {},
        "next_canary_plan": next_canary_plan or {},
    }


def emit(result: dict[str, Any], log_path: Path, plan_output: Path | None = None) -> None:
    line = json.dumps(result, ensure_ascii=False, separators=(",", ":"))
    print(line, flush=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")
    write_plan_output(result, plan_output)


class SchedulerLock:
    def __init__(self, path: Path):
        self.path = path
        self.fh = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.fh = self.path.open("w", encoding="utf-8")
        try:
            fcntl.flock(self.fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.fh.close()
            self.fh = None
            raise RuntimeError("skipped_locked")
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.fh:
            fcntl.flock(self.fh, fcntl.LOCK_UN)
            self.fh.close()


def validate_task_id(task_id: str) -> str:
    task_id = str(task_id or "").strip()
    if not TASK_ID_RE.match(task_id):
        raise ValueError(f"invalid task_id: {task_id}")
    return task_id


def launcher_command(task_id: str, root: Path, worker_path: Path = DEFAULT_WORKER) -> list[str]:
    task_id = validate_task_id(task_id)
    return ["/usr/bin/python3", str(worker_path), "--root", str(root), "--execute-claimed-task-id", task_id, "--max-dispatch", "1", "--compact-status"]


def claim_selected(root: Path, task_id: str) -> list[dict[str, Any]]:
    if shared_state_v2 is None:
        raise RuntimeError("shared_state_v2 unavailable")
    return shared_state_v2.claim_pending_batch(root=root, limit=1, task_id=validate_task_id(task_id))


def launch_executor(task_id: str, root: Path, worker_path: Path, log_dir: Path) -> tuple[subprocess.Popen, Path]:
    cmd = launcher_command(task_id, root, worker_path)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{validate_task_id(task_id)}.log"
    log_fh = log_path.open("ab")
    try:
        proc = subprocess.Popen(cmd, stdout=log_fh, stderr=subprocess.STDOUT, close_fds=True)
        return proc, log_path
    finally:
        log_fh.close()


def build_running_manager(root: Path, config: dict[str, int]) -> SlotManager:
    manager = SlotManager(config)
    for task in read_dispatch(root, "claimed"):
        manager.reserve(task_record(task))
    return manager


def _load_local_result(worker_root: Path, task_id: str) -> dict[str, Any] | None:
    path = worker_root / "tasks" / validate_task_id(task_id) / "local-result.json"
    if not path.is_file():
        return {"_diagnostic": {"path": str(path), "exists": False}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"_diagnostic": {"path": str(path), "exists": True, "json_error": str(exc)}}
    if not isinstance(payload, dict):
        return {"_diagnostic": {"path": str(path), "exists": True, "non_dict": True}}
    state = str(payload.get("state") or "").strip()
    if state not in {"completed", "failed", "abandoned"}:
        payload["_diagnostic"] = {"path": str(path), "exists": True, "state": state, "terminal": False}
        return payload
    payload["_diagnostic"] = {"path": str(path), "exists": True, "state": state, "terminal": True}
    return payload


def import_terminal_local_result(root: Path, worker_root: Path, task_id: str) -> dict[str, Any]:
    """Import a scheduler-owned terminal local-result into canonical state.

    This is intentionally narrow: it only imports a terminal local-result for a
    task that is still present in dispatch/claimed. It does not clean arbitrary
    stale claims and does not infer success from runner logs.
    """
    task_id = validate_task_id(task_id)
    claimed_path = root / "dispatch" / "claimed" / f"{task_id}.json"
    if not claimed_path.exists():
        return {"task_id": task_id, "action": "not_claimed"}
    payload = _load_local_result(worker_root, task_id)
    if payload is None:
        return {"task_id": task_id, "action": "no_terminal_local_result"}
    if isinstance(payload, dict) and payload.get("_diagnostic") and str(payload.get("state") or "").strip() not in {"completed", "failed", "abandoned"}:
        return {"task_id": task_id, "action": "non_terminal_local_result", "diagnostic": payload.get("_diagnostic")}
    if isinstance(payload, dict) and set(payload.keys()) == {"_diagnostic"}:
        return {"task_id": task_id, "action": "no_terminal_local_result", "diagnostic": payload.get("_diagnostic")}
    if shared_state_v2 is None:
        return {"task_id": task_id, "action": "shared_state_unavailable"}
    result_hash = hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
    event = {
        "type": "result",
        "task_id": task_id,
        "result_hash": result_hash,
        "payload": payload,
        "created_at": datetime.now().replace(microsecond=0).isoformat(),
        "source": "vm_coding_worker_scheduler",
    }
    with shared_state_v2.connect_db(root) as conn:
        try:
            shared_state_v2._import_result(conn, root, event)
        except KeyError:
            return {
                "task_id": task_id,
                "action": "missing_canonical_task_for_terminal_local_result",
                "state": payload.get("state"),
                "result_hash": result_hash,
            }
    return {"task_id": task_id, "action": "imported_terminal_local_result", "state": payload.get("state"), "result_hash": result_hash}


def import_claimed_terminal_local_results(root: Path, worker_root: Path, running_task_ids: set[str] | None = None) -> list[dict[str, Any]]:
    """Import terminal local-results for currently claimed tasks.

    This covers scheduler restart/running-dict loss without broad stale cleanup:
    it only acts when a dispatch/claimed task already has a terminal
    worker-local local-result.json.
    """
    running_task_ids = running_task_ids or set()
    imported: list[dict[str, Any]] = []
    for task in read_dispatch(root, "claimed"):
        task_id = str(task.get("task_id") or "").strip()
        if not task_id:
            continue
        result = import_terminal_local_result(root, worker_root, task_id)
        # Keep all actions in the cycle log, not only successful imports.
        # This is critical for live diagnosis: a terminal local-result may exist
        # while the canonical import is blocked by missing DB rows, non-terminal
        # payload shape, or path/JSON issues.
        imported.append(result)
    return imported


def execute_cycle(args: argparse.Namespace, config: dict[str, int], running: dict[str, dict[str, Any]]) -> dict[str, Any]:
    stale_claim_scan = scan_stale_claims(args.root)
    active_records_for_budget = [task_record(task) for task in read_dispatch(args.root, "claimed")]
    pending_records_for_budget = [task_record(task) for task in read_pending(args.root)]
    active_by_lane = {"heavy": 0, "standard": 0, "fast": 0}
    pending_by_lane = {"heavy": 0, "standard": 0, "fast": 0}
    for item in active_records_for_budget:
        lane = str(item.get("lane") or "standard")
        if lane in active_by_lane:
            active_by_lane[lane] += 1
    for item in pending_records_for_budget:
        lane = str(item.get("lane") or "standard")
        if lane in pending_by_lane:
            pending_by_lane[lane] += 1

    budget_helper = vm_resource_preflight
    if budget_helper is None:
        try:
            import vm_resource_preflight as budget_helper  # type: ignore
        except Exception:
            budget_helper = None

    effective_slots = None
    concurrency_budget = None
    concurrency_budget_verdict = None
    effective_config = dict(config)
    effective_profile = 'static'
    budget_state = 'unknown'
    downgrade_reasons: list[str] = []
    if budget_helper is not None:
        try:
            if hasattr(budget_helper, 'compute_effective_scheduler_slots'):
                effective_slots = budget_helper.compute_effective_scheduler_slots(
                    base_config=config,
                    active_heavy=active_by_lane.get('heavy', 0),
                    pending_fast=pending_by_lane.get('fast', 0),
                    pending_standard=pending_by_lane.get('standard', 0),
                    lane='heavy',
                    artifact_root='/mnt/tmp',
                )
                # Effective slots are a resource-pressure cap, never an implicit
                # production config expansion.  Temporary N7 windows must still be
                # expressed in the static config; preflight may only keep or lower it.
                effective_config.update({
                    'global_slots': min(int(config.get('global_slots', DEFAULTS['global_slots'])), int(effective_slots.get('global_slots_effective', effective_config.get('global_slots', DEFAULTS['global_slots'])))),
                    'heavy_slots': min(int(config.get('heavy_slots', DEFAULTS['heavy_slots'])), int(effective_slots.get('heavy_slots_effective', effective_config.get('heavy_slots', DEFAULTS['heavy_slots'])))),
                    'standard_slots': min(int(config.get('standard_slots', DEFAULTS['standard_slots'])), int(effective_slots.get('standard_slots_effective', effective_config.get('standard_slots', DEFAULTS['standard_slots'])))),
                    'fast_slots': min(int(config.get('fast_slots', DEFAULTS['fast_slots'])), int(effective_slots.get('fast_slots_effective', effective_config.get('fast_slots', DEFAULTS['fast_slots'])))),
                })
                effective_profile = str(effective_slots.get('effective_profile') or 'static')
                budget_state = str(effective_slots.get('budget_state') or 'unknown')
                downgrade_reasons = list(effective_slots.get('downgrade_reasons') or [])
                concurrency_budget = effective_slots.get('budget')
            if concurrency_budget is None and effective_slots is not None:
                concurrency_budget = effective_slots.get('budget')
            if hasattr(budget_helper, 'concurrency_budget_verdict'):
                concurrency_budget_verdict = budget_helper.concurrency_budget_verdict(
                    budget=concurrency_budget,
                    target_heavy=int(effective_config.get('heavy_slots', DEFAULTS['heavy_slots'])),
                    warn_only=True,
                )
        except Exception as exc:
            concurrency_budget = {'error': str(exc)}
            concurrency_budget_verdict = {'mode': 'warn_only', 'state': 'unknown', 'reasons': ['budget_error'], 'error': str(exc)}
            effective_slots = {'effective_profile': 'static', 'budget_state': 'unknown', 'downgrade_reasons': ['effective_slots_error'], 'error': str(exc)}
            effective_profile = 'static'
            budget_state = 'unknown'
            downgrade_reasons = ['effective_slots_error']

    if effective_slots is None:
        effective_slots = {
            'effective_profile': effective_profile,
            'budget_state': budget_state,
            'global_slots_effective': int(effective_config.get('global_slots', DEFAULTS['global_slots'])),
            'heavy_slots_effective': int(effective_config.get('heavy_slots', DEFAULTS['heavy_slots'])),
            'standard_slots_effective': int(effective_config.get('standard_slots', DEFAULTS['standard_slots'])),
            'fast_slots_effective': int(effective_config.get('fast_slots', DEFAULTS['fast_slots'])),
            'downgrade_reasons': downgrade_reasons,
            'inputs': {},
        }

    now = time.monotonic()
    completed: list[dict[str, Any]] = []
    manager = build_running_manager(args.root, effective_config)
    for task_id, child in list(running.items()):
        proc = child["process"]
        code = proc.poll()
        if code is not None:
            imported = import_terminal_local_result(args.root, args.worker_root, task_id)
            completed.append({"task_id": task_id, "exit_code": code, "run_sec": round(now - child["started"], 3), "terminal_import": imported})
            running.pop(task_id, None)
    terminal_imports = import_claimed_terminal_local_results(args.root, args.worker_root, {str(item.get("task_id")) for item in completed if item.get("task_id")})
    # Operational debug summary: stale_claim_scan comes from a separate reconciler
    # and can say no_terminal_status even when scheduler import sees something else.
    terminal_import_debug = {
        "worker_root": str(args.worker_root),
        "claimed_count": len(read_dispatch(args.root, "claimed")),
        "actions": [item.get("action") for item in terminal_imports],
        "items": terminal_imports[:10],
    }
    pending_records = [task_record(task) for task in read_pending(args.root)]
    allow_task_ids = set(getattr(args, "allow_task_id", None) or [])
    started: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for item in pending_records:
        if allow_task_ids and str(item.get("task_id")) not in allow_task_ids:
            skipped.append({**public_record(item), "reason": "not_in_allow_task_id"})
            continue
        if item.get("task_id") in running:
            continue
        reason = manager.admit(item)
        if reason is not None:
            skipped.append({**public_record(item), "reason": reason})
            continue
        task_id = validate_task_id(str(item.get("task_id") or ""))
        claimed = claim_selected(args.root, task_id)
        if not claimed:
            skipped.append({**public_record(item), "reason": "skipped_claim_race"})
            continue
        proc, log_path = launch_executor(task_id, args.root, args.worker_path, args.exec_log_dir)
        grace_deadline = time.monotonic() + 0.2
        while time.monotonic() < grace_deadline and proc.poll() is None and (not log_path.exists() or log_path.stat().st_size == 0):
            time.sleep(0.02)
        launch_failed = proc.poll() is not None and (not log_path.exists() or log_path.stat().st_size == 0)
        if launch_failed:
            if shared_state_v2 is not None:
                claimed_path = args.root / 'dispatch' / 'claimed' / f"{task_id}.json"
                if claimed_path.exists():
                    payload = json.loads(claimed_path.read_text(encoding='utf-8'))
                    payload['state'] = 'failed'
                    payload['failure_reason'] = 'claimed_without_runner_artifacts'
                    failed_path = args.root / 'dispatch' / 'failed' / f"{task_id}.json"
                    failed_path.parent.mkdir(parents=True, exist_ok=True)
                    failed_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding='utf-8')
                    claimed_path.unlink()
            skipped.append({**public_record(item), 'reason': 'launch_failed', 'exec_log_path': str(log_path)})
            continue
        manager.reserve(item)
        running[task_id] = {"process": proc, "started": time.monotonic(), "record": item, "exec_log_path": str(log_path), "launch_ts": datetime.now().replace(microsecond=0).isoformat()}
        started.append({**public_record(item), "pid": proc.pid, "reason": "claimed_and_started", "exec_log_path": str(log_path)})
    active_records = [task_record(task) for task in read_dispatch(args.root, "claimed")]
    seen_active = {str(item.get("task_id")) for item in active_records if item.get("task_id") not in (None, "")}
    for task_id, child in running.items():
        if str(task_id) in seen_active:
            continue
        record = child.get("record") if isinstance(child, dict) else None
        if isinstance(record, dict):
            active_records.append(record)
            seen_active.add(str(task_id))
    pending_after_records = [task_record(task) for task in read_pending(args.root)]
    running_records = [child.get("record") for child in running.values() if isinstance(child, dict) and isinstance(child.get("record"), dict)]
    static_global = int(config.get('global_slots', DEFAULTS['global_slots']))
    static_heavy = int(config.get('heavy_slots', DEFAULTS['heavy_slots']))
    static_standard = int(config.get('standard_slots', DEFAULTS['standard_slots']))
    static_fast = int(config.get('fast_slots', DEFAULTS['fast_slots']))
    eff_global = int(effective_config.get('global_slots', DEFAULTS['global_slots']))
    eff_heavy = int(effective_config.get('heavy_slots', DEFAULTS['heavy_slots']))
    eff_standard = int(effective_config.get('standard_slots', DEFAULTS['standard_slots']))
    eff_fast = int(effective_config.get('fast_slots', DEFAULTS['fast_slots']))
    effective_slots_summary = f'heavy {static_heavy} -> {eff_heavy}, global {static_global} -> {eff_global}, profile {effective_profile}/{budget_state}'
    pressure_summary = ', '.join(downgrade_reasons) if downgrade_reasons else 'none'
    slot_capacity_summary = f'static[g={static_global} h={static_heavy} s={static_standard} f={static_fast}] effective[g={eff_global} h={eff_heavy} s={eff_standard} f={eff_fast}]'
    return {"ts": datetime.now().replace(microsecond=0).isoformat(), "state": "execute", "event": "scheduler_cycle", "root": str(args.root), "started": started, "completed": completed, "skipped": skipped, "terminal_imports": terminal_imports, "terminal_import_debug": terminal_import_debug, "running_task_ids": sorted(running), "running_worker_pids": running_worker_pids(running), "active_task_ids_by_lane": ids_by_lane(active_records), "pending_task_ids_by_lane": ids_by_lane(pending_after_records), "active_by_resource_class": counts_by_resource_class(active_records), "pending_by_resource_class": counts_by_resource_class(pending_after_records), "running_by_resource_class": counts_by_resource_class(running_records), "started_by_resource_class": counts_by_resource_class(started), "skipped_by_resource_class": counts_by_resource_class(skipped), "active_task_ids_by_resource_class": ids_by_resource_class(active_records), "pending_task_ids_by_resource_class": ids_by_resource_class(pending_after_records), "running_task_ids_by_resource_class": ids_by_resource_class(running_records), "stale_claim_scan": stale_claim_scan, "slot_snapshot": manager.snapshot(), "config_fingerprint": config_fingerprint(config, getattr(args, "config", None)), "effective_profile": effective_profile, "budget_state": budget_state, "effective_slots": effective_slots, "effective_config": effective_config, "downgrade_reasons": downgrade_reasons, "effective_slots_summary": effective_slots_summary, "pressure_summary": pressure_summary, "slot_capacity_summary": slot_capacity_summary, "concurrency_budget": concurrency_budget, "concurrency_budget_verdict": concurrency_budget_verdict}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="VM coding worker slot scheduler")
    p.add_argument("--dry-run", action="store_true", default=True, help="Inspect without claim/execute (default).")
    p.add_argument("--max-runs", type=int, default=1)
    p.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    p.add_argument("--execute-root", type=Path, default=None, help=f"Temporary isolated shared-state root for experimental execute. Prefer a local worker-state runtime root such as {DEFAULT_LOCAL_EXEC_ROOT}; mounted/CIFS/NAS roots such as /mnt/tmp are refused by default.")
    p.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    p.add_argument("--log-path", type=Path, default=DEFAULT_LOG)
    p.add_argument("--plan-output", type=Path, default=None)
    p.add_argument("--lock-path", type=Path, default=DEFAULT_LOCK)
    p.add_argument("--worker-path", type=Path, default=DEFAULT_WORKER)
    p.add_argument("--worker-root", type=Path, default=Path("/home/mini/.hermes/worker-state"), help="VM worker local state root used for terminal local-result import")
    p.add_argument("--exec-log-dir", type=Path, default=DEFAULT_EXEC_LOG_DIR)
    p.add_argument("--loop-sleep", type=float, default=0.2)
    p.add_argument("--execute", action="store_true", help="Experimental isolated execute; requires env gate and --execute-root")
    p.add_argument("--allow-task-id", action="append", default=[], help="Execute allowlist. When provided, only these task IDs may be claimed/started; repeat for multiple IDs.")
    p.add_argument("--allow-mounted-execute-root", action="store_true", help="Allow mounted/CIFS/NAS execute roots for isolated experiments only; not recommended because sqlite may lock on such filesystems.")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.execute:
        if args.execute_root is None:
            print("execute mode requires --execute-root temporary root", file=sys.stderr)
            return 2
        live_execute_root = args.execute_root.resolve() == DEFAULT_ROOT.resolve()
        if live_execute_root and os.environ.get("HERMES_VM_SCHEDULER_EXECUTE_EXPERIMENT") != "1":
            print("live execute root requires HERMES_VM_SCHEDULER_EXECUTE_EXPERIMENT=1", file=sys.stderr)
            return 2
        persistent_service_mode = live_execute_root and os.environ.get("HERMES_VM_SCHEDULER_PERSISTENT_SERVICE") == "1"
        if live_execute_root and not args.allow_task_id and not persistent_service_mode:
            print("live execute root requires --allow-task-id", file=sys.stderr)
            return 2
        try:
            args.allow_task_id = [validate_task_id(task_id) for task_id in (args.allow_task_id or [])]
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 2
        args.root = args.execute_root
        allow_live_root = args.root.resolve() == DEFAULT_ROOT.resolve() and (bool(args.allow_task_id) or persistent_service_mode)
        rejection = validate_execute_root(args.root, allow_mounted=args.allow_mounted_execute_root, allow_live=allow_live_root)
        if rejection:
            print(rejection, file=sys.stderr)
            return 2
    config = load_config(args.config)
    if args.log_path == DEFAULT_LOG and args.root.resolve() != DEFAULT_ROOT.resolve():
        # Keep tests and isolated experiments from appending to the live shadow log
        # when callers pass a non-live --root without an explicit --log-path.
        args.log_path = args.root.parent / DEFAULT_LOG.name
    runs = max(int(args.max_runs or 1), 1)
    if not args.execute:
        for _ in range(runs):
            result = build_plan(args.root, config)
            emit(result, args.log_path, args.plan_output)
        return 0
    try:
        with SchedulerLock(args.lock_path):
            running: dict[str, dict[str, Any]] = {}
            for i in range(runs):
                emit(execute_cycle(args, config, running), args.log_path, args.plan_output)
                if i + 1 < runs:
                    time.sleep(max(args.loop_sleep, 0))
            return 0
    except RuntimeError as exc:
        result = {"ts": datetime.now().replace(microsecond=0).isoformat(), "state": "execute", "event": "skipped_locked", "reason": str(exc), "root": str(args.root)}
        emit(result, args.log_path, args.plan_output)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
