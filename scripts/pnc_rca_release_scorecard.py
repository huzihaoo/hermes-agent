#!/usr/bin/env python3
"""Build a fail-closed, read-only PNC RCA release scorecard.

The scorecard deliberately separates current execution truth from reference
contracts and historical evidence.  It never starts work, writes SQLite,
touches release bindings, or calls an external API.
"""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import subprocess
import sys
from types import ModuleType
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import quote
from zoneinfo import ZoneInfo


SCHEMA_VERSION = "pnc_rca_release_scorecard_v1"
RELEASE_STATUS = "NOT_GA"
LOCAL_TIMEZONE = ZoneInfo("Asia/Shanghai")
MAX_JSON_BYTES = 96 * 1024 * 1024
HEX40_RE = re.compile(r"^[0-9a-f]{40}$")
HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
TASK_DATE_RE = re.compile(r"^(20\d{6})-")
HOST_FACE = "gateway_runtime"
PIPELINE_FACE = "g1q3_rca_pipeline"
WORKER_FACE = "vm_worker_state"
MCAP_FACE = "mcap_data_translate"
MCAP_RUNTIME_FACE = "mcap_data_translate_runtime_bins"
FOUR_TIERS = (
    "high_confidence_supported_attribution",
    "medium_confidence_candidate_hypothesis",
    "low_confidence_honest_non_attribution",
    "technical_failure",
)
CORE_HEALTH_FILES = {
    "local.pnc.rca-kafka-consumer": "consumer_health.json",
    "local.pnc.rca-outbox-dispatcher": "outbox_dispatcher_health.json",
    "local.pnc.rca-delivery-collector": "delivery_collector_health.json",
    "local.pnc.rca-delivery-dispatcher": "delivery_dispatcher_health.json",
}


class ScorecardError(RuntimeError):
    """Stable, non-sensitive scorecard failure."""

    def __init__(self, code: str, detail: str):
        super().__init__(detail)
        self.code = code
        self.detail = detail


@dataclass(frozen=True)
class ScorecardPaths:
    live_manifest: Path
    active_binding: Path
    state_root: Path
    task_state_root: Path
    release_root: Path
    gateway_state: Path
    historical_ledger: Path | None = None


def _canonical_json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ScorecardError(
            "scorecard_json_invalid", "scorecard is not canonical JSON"
        ) from exc


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _stable_json(path: Path, *, label: str) -> tuple[dict[str, Any], dict[str, Any]]:
    path = path.expanduser().absolute()
    try:
        before = os.lstat(path)
        if (
            stat.S_ISLNK(before.st_mode)
            or not stat.S_ISREG(before.st_mode)
            or before.st_size <= 0
            or before.st_size > MAX_JSON_BYTES
        ):
            raise OSError("not a bounded regular file")
        raw = path.read_bytes()
        after = os.lstat(path)
    except OSError as exc:
        raise ScorecardError(
            "required_source_unavailable", f"{label} is unavailable: {path}"
        ) from exc
    identity_before = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    identity_after = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    if identity_before != identity_after or len(raw) != before.st_size:
        raise ScorecardError(
            "source_changed_during_read", f"{label} changed during read"
        )
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ScorecardError(
            "required_source_invalid", f"{label} is not valid JSON"
        ) from exc
    if not isinstance(value, dict):
        raise ScorecardError(
            "required_source_invalid", f"{label} must be a JSON object"
        )
    return value, {
        "path": str(path),
        "sha256": _sha256_bytes(raw),
        "bytes": len(raw),
        "mtime_ns": after.st_mtime_ns,
    }


def _required_mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or not value:
        raise ScorecardError(
            "required_field_empty", f"{field} must be a non-empty object"
        )
    return value


def _required_sequence(value: Any, field: str) -> Sequence[Any]:
    if not isinstance(value, list) or not value:
        raise ScorecardError(
            "required_field_empty", f"{field} must be a non-empty array"
        )
    return value


def _required_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ScorecardError("required_field_empty", f"{field} must be non-empty")
    return value.strip()


def _required_hex(value: Any, field: str, pattern: re.Pattern[str]) -> str:
    text = _required_text(value, field).lower()
    if pattern.fullmatch(text) is None:
        raise ScorecardError(
            "required_field_invalid", f"{field} has an invalid fingerprint"
        )
    return text


def _parse_datetime(value: Any, field: str) -> datetime:
    text = _required_text(value, field)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ScorecardError(
            "required_field_invalid", f"{field} is not ISO-8601"
        ) from exc
    if parsed.tzinfo is None:
        raise ScorecardError(
            "required_field_invalid", f"{field} must include a timezone"
        )
    return parsed.astimezone(timezone.utc)


def _timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _get_path(value: Mapping[str, Any], dotted: str) -> Any:
    current: Any = value
    for part in dotted.split("."):
        if not isinstance(current, Mapping) or part not in current:
            raise ScorecardError("required_field_empty", f"{dotted} is missing")
        current = current[part]
    return current


def _face(manifest: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    faces = _required_mapping(manifest.get("face_git_bindings"), "face_git_bindings")
    return _required_mapping(faces.get(name), f"face_git_bindings.{name}")


def _fingerprint(face: Mapping[str, Any], field: str) -> dict[str, Any]:
    return {
        "commit": _required_hex(face.get("commit"), f"{field}.commit", HEX40_RE),
        "tree": _required_hex(face.get("tree"), f"{field}.tree", HEX40_RE),
        "branch": _required_text(face.get("branch"), f"{field}.branch"),
        "repo": _required_text(face.get("repo"), f"{field}.repo"),
        "updated_at": _timestamp(
            _parse_datetime(face.get("updated_at"), f"{field}.updated_at")
        ),
        "previous_commit": _required_hex(
            face.get("previous_commit"), f"{field}.previous_commit", HEX40_RE
        ),
        "reason": _required_text(face.get("reason"), f"{field}.reason"),
    }


def _binding_fingerprint(value: Mapping[str, Any], field: str) -> dict[str, str]:
    return {
        "commit": _required_hex(value.get("commit"), f"{field}.commit", HEX40_RE),
        "tree": _required_hex(value.get("tree"), f"{field}.tree", HEX40_RE),
    }


def _load_deployed_profiles(
    host_source: Path,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    module_path = host_source / "gateway" / "pnc_rca_business_profiles.py"
    try:
        raw = module_path.read_bytes()
    except OSError as exc:
        raise ScorecardError(
            "profile_registry_unavailable",
            f"deployed profile registry is unavailable: {module_path}",
        ) from exc
    module_name = f"_pnc_rca_live_profiles_{_sha256_bytes(raw)[:12]}"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ScorecardError(
            "profile_registry_invalid", "deployed profile registry cannot be loaded"
        )
    module: ModuleType = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    previous_bytecode_policy = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    try:
        spec.loader.exec_module(module)
        raw_profiles = getattr(module, "RCA_BUSINESS_PROFILES", None)
        if not isinstance(raw_profiles, tuple) or not raw_profiles:
            raise ScorecardError(
                "profile_registry_empty", "deployed profile registry is empty"
            )
        profiles: list[dict[str, Any]] = []
        for index, profile in enumerate(raw_profiles):
            if not hasattr(profile, "public_contract"):
                raise ScorecardError(
                    "profile_registry_invalid",
                    f"profile[{index}] has no public contract",
                )
            public = profile.public_contract()
            if not isinstance(public, dict):
                raise ScorecardError(
                    "profile_registry_invalid", f"profile[{index}] contract is invalid"
                )
            for key in (
                "profile_id",
                "profile_version",
                "data_resolver",
                "evidence_contract",
                "evaluator_scope",
                "artifact_namespace",
                "execution_readiness",
                "registry_version",
            ):
                _required_text(public.get(key), f"profiles[{index}].{key}")
            profiles.append(public)
    finally:
        sys.dont_write_bytecode = previous_bytecode_policy
        sys.modules.pop(module_name, None)
    profiles.sort(key=lambda item: str(item["profile_id"]))
    return profiles, {
        "path": str(module_path),
        "sha256": _sha256_bytes(raw),
        "bytes": len(raw),
    }


def _resident_evidence(
    state_root: Path, gateway_state_path: Path, host_source: Path
) -> dict[str, Any]:
    expected_root = str(host_source.resolve())
    residents: list[dict[str, Any]] = []
    sources: list[dict[str, Any]] = []
    for expected_label, filename in CORE_HEALTH_FILES.items():
        health, source = _stable_json(state_root / filename, label=expected_label)
        sources.append(source)
        identity = _required_mapping(
            health.get("runtime_identity"), f"{expected_label}.runtime_identity"
        )
        label = _required_text(
            identity.get("service_label"), f"{expected_label}.service_label"
        )
        if label != expected_label:
            raise ScorecardError(
                "resident_identity_mismatch", f"{expected_label} reported {label}"
            )
        cwd = _required_text(identity.get("cwd"), f"{expected_label}.cwd")
        if str(Path(cwd).resolve()) != expected_root:
            raise ScorecardError(
                "resident_release_mismatch",
                f"{expected_label} is not loaded from active host release",
            )
        residents.append({
            "service_label": label,
            "pid": int(identity.get("pid") or 0),
            "cwd": cwd,
            "script": _required_text(
                identity.get("script"), f"{expected_label}.script"
            ),
            "script_sha256": _required_hex(
                identity.get("script_sha256"),
                f"{expected_label}.script_sha256",
                HEX64_RE,
            ),
            "loaded_runtime_sha256": _required_hex(
                identity.get("loaded_runtime_sha256"),
                f"{expected_label}.loaded_runtime_sha256",
                HEX64_RE,
            ),
            "healthy": health.get("healthy") is True,
        })
    gateway, source = _stable_json(gateway_state_path, label="ai.hermes.gateway state")
    sources.append(source)
    argv = _required_sequence(gateway.get("argv"), "gateway_state.argv")
    script = _required_text(argv[0], "gateway_state.argv[0]")
    try:
        gateway_root = str(Path(script).resolve().parents[1])
    except IndexError as exc:
        raise ScorecardError(
            "resident_identity_mismatch", "gateway script path is invalid"
        ) from exc
    if gateway_root != expected_root:
        raise ScorecardError(
            "resident_release_mismatch",
            "ai.hermes.gateway is not loaded from active host release",
        )
    residents.append({
        "service_label": "ai.hermes.gateway",
        "pid": int(gateway.get("pid") or 0),
        "cwd": expected_root,
        "script": script,
        "script_sha256": _sha256_bytes(Path(script).read_bytes()),
        "loaded_runtime_sha256": "not_emitted_by_gateway_state",
        "healthy": gateway.get("gateway_state") == "running",
    })
    if any(item["pid"] < 1 for item in residents):
        raise ScorecardError(
            "resident_identity_invalid", "resident PID must be positive"
        )
    return {
        "expected_host_source": expected_root,
        "release_unique": len({item["cwd"] for item in residents}) == 1,
        "all_healthy": all(item["healthy"] for item in residents),
        "services": sorted(residents, key=lambda item: item["service_label"]),
        "sources": sources,
    }


def _open_read_only_database(path: Path) -> sqlite3.Connection:
    absolute = path.expanduser().absolute()
    if not absolute.is_file():
        raise ScorecardError(
            "control_db_unavailable", f"control DB is unavailable: {absolute}"
        )
    uri = f"file:{quote(str(absolute), safe='/')}?mode=ro"
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        if connection.execute("PRAGMA query_only").fetchone()[0] != 1:
            raise ScorecardError(
                "control_db_not_read_only", "SQLite query_only did not engage"
            )
        connection.execute("BEGIN")
    except sqlite3.Error as exc:
        raise ScorecardError(
            "control_db_unavailable", "control DB could not be opened read-only"
        ) from exc
    return connection


def _required_tables(connection: sqlite3.Connection) -> None:
    expected = {
        "business_triggers",
        "rca_trigger_sources",
        "rca_trigger_bindings",
        "rca_delivery_jobs",
        "rca_delivery_effects",
        "rca_activation_epochs",
        "rca_activation_transition_audit",
    }
    found = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN (%s)"
            % ",".join("?" for _ in expected),
            tuple(sorted(expected)),
        )
    }
    missing = sorted(expected - found)
    if missing:
        raise ScorecardError(
            "control_db_schema_missing",
            f"control DB tables missing: {','.join(missing)}",
        )


def _json_object(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, str) or not raw:
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _job_tier(row: Mapping[str, Any]) -> str:
    outcome = str(row.get("outcome") or "").strip().lower()
    terminal_state = str(row.get("terminal_state") or "").strip().lower()
    terminal_error = str(row.get("terminal_error_code") or "").strip()
    if (
        outcome not in {"", "success"}
        or terminal_error
        or terminal_state
        in {
            "terminal_failed",
            "quarantined",
            "failed",
        }
    ):
        return "technical_failure"
    contract = _json_object(row.get("contract_json"))
    public_result = contract.get("public_result")
    public_result = public_result if isinstance(public_result, Mapping) else {}
    responsibility = public_result.get("responsibility")
    responsibility = responsibility if isinstance(responsibility, Mapping) else {}
    explicit = (
        str(
            contract.get("quality_classification")
            or public_result.get("quality_classification")
            or responsibility.get("status")
            or ""
        )
        .strip()
        .lower()
    )
    if explicit in {"supported_attribution", "evidence_attribution", "supported"}:
        return "high_confidence_supported_attribution"
    if "candidate" in explicit or "needs_review" in explicit:
        return "medium_confidence_candidate_hypothesis"
    terminal_diagnostic = public_result.get("terminal_diagnostic")
    terminal_diagnostic = (
        terminal_diagnostic if isinstance(terminal_diagnostic, Mapping) else {}
    )
    if (
        "non_attribution" in explicit
        or "suppressed" in explicit
        or str(terminal_diagnostic.get("attribution_status") or "").lower()
        == "not_attributable"
    ):
        return "low_confidence_honest_non_attribution"
    return "unclassified"


def _tier_counts(connection: sqlite3.Connection, since: datetime) -> dict[str, Any]:
    rows = connection.execute(
        """
        WITH ranked AS (
            SELECT j.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY j.business_key
                       ORDER BY j.generation DESC, j.updated_at DESC, j.delivery_id DESC
                   ) AS rank_no
              FROM rca_delivery_jobs AS j
             WHERE j.created_at >= ?
        )
        SELECT delivery_id, business_key, generation, outcome, terminal_state,
               terminal_error_code, status, contract_json
          FROM ranked
         WHERE rank_no = 1
        """,
        (_timestamp(since),),
    ).fetchall()
    counts: Counter[str] = Counter(_job_tier(dict(row)) for row in rows)
    delivery_failures = connection.execute(
        """
        WITH ranked AS (
            SELECT j.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY j.business_key
                       ORDER BY j.generation DESC, j.updated_at DESC, j.delivery_id DESC
                   ) AS rank_no
              FROM rca_delivery_jobs AS j
             WHERE j.created_at >= ?
        )
        SELECT COUNT(*)
          FROM ranked AS j
         WHERE j.rank_no = 1
           AND (
               j.status != 'delivered'
               OR EXISTS (
                   SELECT 1 FROM rca_delivery_effects AS e
                    WHERE e.delivery_id = j.delivery_id
                      AND e.required = 1 AND e.status != 'succeeded'
               )
           )
        """,
        (_timestamp(since),),
    ).fetchone()[0]
    return {
        "window_start": _timestamp(since),
        "scope": "latest_generation_per_business_key_created_in_window",
        "total": len(rows),
        "counts": {tier: int(counts[tier]) for tier in FOUR_TIERS},
        "unclassified": int(counts["unclassified"]),
        "consumer_delivery_failure": int(delivery_failures),
        "projection": "explicit_live_contract_fields_only; W1 oracle not applied",
    }


def _requester_identity_denominators(connection: sqlite3.Connection) -> dict[str, Any]:
    counts = {key: 0 for key in ("human", "automation", "legacy_automation", "unknown")}
    by_source: dict[str, Counter[str]] = {}
    rows = connection.execute(
        """
        SELECT source_kind, requester_id, COUNT(*) AS row_count
          FROM rca_trigger_sources
         GROUP BY source_kind, requester_id
        """
    ).fetchall()
    for row in rows:
        requester = str(row["requester_id"] or "").strip().lower()
        if requester.startswith("ou_"):
            identity_kind = "human"
        elif requester.startswith("automation:"):
            identity_kind = "automation"
        elif requester.startswith(("operator-", "operator_", "codex-", "codex_")):
            identity_kind = "legacy_automation"
        else:
            identity_kind = "unknown"
        row_count = int(row["row_count"])
        counts[identity_kind] += row_count
        by_source.setdefault(str(row["source_kind"]), Counter())[identity_kind] += (
            row_count
        )
    total = sum(counts.values())
    if total <= 0:
        raise ScorecardError(
            "required_real_data_empty", "requester identity denominator is empty"
        )
    return {
        "total_triggers": total,
        "counts": counts,
        "by_source_kind": {
            source: {key: int(values[key]) for key in counts}
            for source, values in sorted(by_source.items())
        },
        "classifier": {
            "human": "ou_*",
            "automation": "automation:*",
            "legacy_automation": "operator-/operator_/codex-/codex_",
            "unknown": "all other or empty requester IDs",
            "integration_touchpoint": (
                "delegate to gateway.pnc_rca_requester_identity.classify_rca_requester "
                "when W10 is integrated"
            ),
        },
    }


def _effect_evidence(
    connection: sqlite3.Connection, delivery_id: str
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT effect_kind, required, status, payload_json, remote_receipt_json,
               completed_at
          FROM rca_delivery_effects
         WHERE delivery_id = ?
         ORDER BY effect_kind, created_at
        """,
        (delivery_id,),
    ).fetchall()
    effects: list[dict[str, Any]] = []
    for row in rows:
        effects.append({
            "effect_kind": row["effect_kind"],
            "required": bool(row["required"]),
            "status": row["status"],
            "payload": _json_object(row["payload_json"]),
            "remote_receipt": _json_object(row["remote_receipt_json"]),
            "completed_at": row["completed_at"],
        })
    return effects


def _latest_canary_row(
    connection: sqlite3.Connection,
    *,
    kind: str,
    since: datetime | None,
) -> sqlite3.Row | None:
    if kind == "natural_kafka":
        predicate = """
            s.source_kind = 'kafka_workflow_event'
            AND s.mode = 'issue_created'
            AND s.kafka_event_uid IS NOT NULL AND s.kafka_event_uid != ''
        """
    elif kind == "feishu_topic":
        predicate = """
            s.source_kind = 'feishu_group_manual'
            AND s.requester_id LIKE 'ou_%'
            AND s.chat_id LIKE 'oc_%'
            AND s.thread_id LIKE 'topic:%'
            AND s.message_id != ''
        """
    else:
        raise AssertionError(kind)
    parameters: list[Any] = []
    if since is not None:
        predicate += " AND s.created_at >= ?"
        parameters.append(_timestamp(since))
    return connection.execute(
        f"""
        SELECT s.source_id, s.source_kind, s.mode, s.requester_id, s.chat_id,
               s.thread_id, s.message_id, s.kafka_event_uid, s.created_at,
               b.business_key, b.generation, t.submission_key,
               j.delivery_id, j.work_item_id, j.outcome AS job_outcome,
               j.status AS job_status, j.report_url, j.updated_at
          FROM rca_trigger_sources AS s
          LEFT JOIN rca_trigger_bindings AS b ON b.source_id = s.source_id
          LEFT JOIN business_triggers AS t
                 ON t.business_key = b.business_key AND t.generation = b.generation
          LEFT JOIN rca_delivery_jobs AS j ON j.submission_key = t.submission_key
         WHERE {predicate}
         ORDER BY s.created_at DESC, s.source_id DESC
         LIMIT 1
        """,
        tuple(parameters),
    ).fetchone()


def _formal_report_url(value: Any) -> bool:
    text = str(value or "")
    return (
        text.startswith("https://")
        and ".minieye.tech/" in text
        and text.endswith("/index.html")
        and ".viz.mcap" not in text
    )


def _mention_present(payload: Mapping[str, Any], requester_id: str) -> bool:
    content = str(payload.get("message_content") or "")
    if requester_id and requester_id in content:
        return True
    mentions = payload.get("mentions") or payload.get("mention_user_ids")
    if isinstance(mentions, list):
        return requester_id in {str(item) for item in mentions}
    return False


def _evaluate_canary(
    connection: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    kind: str,
) -> dict[str, Any]:
    value = dict(row)
    delivery_id = str(value.get("delivery_id") or "")
    effects = _effect_evidence(connection, delivery_id) if delivery_id else []
    required = [effect for effect in effects if effect["required"]]
    by_kind = {effect["effect_kind"]: effect for effect in effects}
    issue = by_kind.get("feishu_issue_comment")
    thread = by_kind.get("feishu_thread_reply")
    report_url = str(value.get("report_url") or "")
    checks: dict[str, bool] = {
        "trigger_bound": bool(
            value.get("business_key") and value.get("submission_key")
        ),
        "delivery_succeeded": value.get("job_outcome") == "success"
        and value.get("job_status") == "delivered",
        "required_effects_succeeded": bool(required)
        and all(effect["status"] == "succeeded" for effect in required),
        "issue_comment_succeeded": bool(issue and issue["status"] == "succeeded"),
        "formal_report_url": _formal_report_url(report_url),
        "readback_present": bool(
            issue
            and issue["remote_receipt"].get("source")
            in {"read_before_write", "read_after_write"}
        ),
    }
    if kind == "feishu_topic":
        issue_payload = issue["payload"] if issue else {}
        thread_payload = thread["payload"] if thread else {}
        checks.update({
            "thread_reply_succeeded": bool(thread and thread["status"] == "succeeded"),
            "issue_thread_content_consistent": bool(
                issue
                and thread
                and issue_payload.get("conclusion") == thread_payload.get("conclusion")
                and issue_payload.get("report_url") == thread_payload.get("report_url")
            ),
            "initiator_mentioned": _mention_present(
                thread_payload, str(value.get("requester_id") or "")
            ),
        })
    return {
        "state": "pass" if all(checks.values()) else "fail",
        "source_id": _required_text(value.get("source_id"), f"{kind}.source_id"),
        "observed_at": _timestamp(
            _parse_datetime(value.get("created_at"), f"{kind}.created_at")
        ),
        "work_item_id": str(value.get("work_item_id") or ""),
        "generation": int(value.get("generation") or 0),
        "delivery_id": delivery_id,
        "report_url": report_url,
        "checks": checks,
    }


def _canary_state(
    connection: sqlite3.Connection,
    *,
    kind: str,
    active_since: datetime,
) -> dict[str, Any]:
    current = _latest_canary_row(connection, kind=kind, since=active_since)
    latest = _latest_canary_row(connection, kind=kind, since=None)
    latest_evidence = (
        _evaluate_canary(connection, latest, kind=kind) if latest else None
    )
    if current is None:
        return {
            "state": "not_observed_for_active_release",
            "active_release_window_start": _timestamp(active_since),
            "latest_observation": latest_evidence,
        }
    current_evidence = _evaluate_canary(connection, current, kind=kind)
    return {
        **current_evidence,
        "active_release_window_start": _timestamp(active_since),
        "latest_observation": latest_evidence,
    }


def _database_evidence(
    db_path: Path,
    *,
    active_since: datetime,
    seven_day_start: datetime,
) -> dict[str, Any]:
    before = os.stat(db_path)
    connection = _open_read_only_database(db_path)
    try:
        _required_tables(connection)
        row_counts = {
            table: int(
                connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            )
            for table in (
                "business_triggers",
                "rca_trigger_sources",
                "rca_delivery_jobs",
                "rca_delivery_effects",
            )
        }
        if any(value <= 0 for value in row_counts.values()):
            raise ScorecardError(
                "required_real_data_empty", "control DB required live rows are empty"
            )
        epochs = connection.execute(
            """
            SELECT epoch_id, state, is_current, created_at, updated_at,
                   bounded_activated_at, confirmed_at, steady_activated_at, aborted_at
              FROM rca_activation_epochs
             ORDER BY created_at, epoch_id
            """
        ).fetchall()
        current = [dict(row) for row in epochs if row["is_current"] == 1]
        if len(current) > 1:
            raise ScorecardError(
                "activation_state_conflict", "multiple current activation epochs"
            )
        if current:
            activation = {"state": current[0]["state"], "current_epoch": current[0]}
        elif not epochs:
            activation = {
                "state": "legacy_unconfigured",
                "current_epoch": None,
                "reason": "activation epoch and transition ledger contain no rows",
            }
        else:
            activation = {
                "state": "no_current_epoch",
                "current_epoch": None,
                "historical_epoch_count": len(epochs),
            }
        activation["epoch_count"] = len(epochs)
        activation["transition_count"] = int(
            connection.execute(
                "SELECT COUNT(*) FROM rca_activation_transition_audit"
            ).fetchone()[0]
        )
        activation["trigger_epoch_bound_count"] = int(
            connection.execute(
                "SELECT COUNT(*) FROM business_triggers WHERE activation_epoch_id IS NOT NULL"
            ).fetchone()[0]
        )
        evidence = {
            "open_mode": "sqlite_uri_mode_ro",
            "query_only": True,
            "row_counts": row_counts,
            "activation": activation,
            "tier_counts": {
                "active_release": _tier_counts(connection, active_since),
                "seven_day": _tier_counts(connection, seven_day_start),
            },
            "requester_identity_denominators": _requester_identity_denominators(
                connection
            ),
            "canaries": {
                "natural_kafka": _canary_state(
                    connection, kind="natural_kafka", active_since=active_since
                ),
                "feishu_topic": _canary_state(
                    connection, kind="feishu_topic", active_since=active_since
                ),
            },
        }
        connection.rollback()
    except sqlite3.Error as exc:
        raise ScorecardError(
            "control_db_read_failed", "control DB read failed"
        ) from exc
    finally:
        connection.close()
    after = os.stat(db_path)
    identity_before = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    identity_after = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    evidence["source"] = {
        "path": str(db_path.absolute()),
        "device": before.st_dev,
        "inode": before.st_ino,
        "bytes": before.st_size,
        "mtime_ns": before.st_mtime_ns,
        "unchanged_during_read": identity_before == identity_after,
    }
    return evidence


def _task_directories(root: Path, *, start: date, end: date) -> Iterable[Path]:
    if not root.is_dir():
        raise ScorecardError(
            "task_state_root_unavailable", f"task-state root missing: {root}"
        )
    for child in sorted(root.iterdir()):
        if not child.is_dir():
            continue
        match = TASK_DATE_RE.match(child.name)
        if match is None:
            continue
        try:
            task_date = datetime.strptime(match.group(1), "%Y%m%d").date()
        except ValueError:
            continue
        if start <= task_date <= end:
            yield child


def _safe_history_json(path: Path) -> dict[str, Any] | None:
    try:
        value, _source = _stable_json(path, label="historical release evidence")
    except ScorecardError:
        return None
    return value


def _history_material(
    task_state_root: Path, *, as_of: datetime
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    local_as_of = as_of.astimezone(LOCAL_TIMEZONE)
    scan_start = local_as_of.date() - timedelta(days=30)
    manifests: list[dict[str, Any]] = []
    receipts: list[dict[str, Any]] = []
    for task_dir in _task_directories(
        task_state_root, start=scan_start, end=local_as_of.date()
    ):
        for path in task_dir.rglob("LIVE_MANIFEST.json"):
            value = _safe_history_json(path)
            if value is not None:
                manifests.append({"path": str(path), "value": value})
        for path in task_dir.rglob("*activation*receipt*.json"):
            value = _safe_history_json(path)
            if value is not None:
                receipts.append({"path": str(path), "value": value})
    return manifests, receipts


def _git_value(repo: Path, *args: str) -> str:
    try:
        result = subprocess.run(
            ["git", "-C", str(repo), *args],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        return ""
    return result.stdout.strip() if result.returncode == 0 else ""


def _manifest_face_records(
    current_manifest: Mapping[str, Any],
    historical_manifests: Sequence[Mapping[str, Any]],
    face: str,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    candidates = [{"path": "live", "value": current_manifest}, *historical_manifests]
    for candidate in candidates:
        manifest = candidate.get("value")
        if not isinstance(manifest, Mapping):
            continue
        faces = manifest.get("face_git_bindings")
        value = faces.get(face) if isinstance(faces, Mapping) else None
        if not isinstance(value, Mapping):
            continue
        commit = str(value.get("commit") or "").lower()
        updated_at = str(value.get("updated_at") or manifest.get("activated_at") or "")
        if HEX40_RE.fullmatch(commit) is None or not updated_at:
            continue
        try:
            parsed = _parse_datetime(updated_at, f"{face}.updated_at")
        except ScorecardError:
            continue
        records.append({
            "commit": commit,
            "tree": str(value.get("tree") or "").lower(),
            "activated_at": parsed,
            "previous_commit": str(value.get("previous_commit") or "").lower(),
            "reason": str(value.get("reason") or "").strip(),
            "repo": str(value.get("repo") or ""),
            "evidence_path": str(candidate.get("path") or "live"),
            "time_source": "live_manifest_face_updated_at",
        })
    return records


def _activation_receipt_records(
    receipts: Sequence[Mapping[str, Any]], *, kind: str
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for item in receipts:
        path = str(item.get("path") or "")
        value = item.get("value")
        if not isinstance(value, Mapping):
            continue
        schema = str(value.get("schema_version") or "").lower()
        filename = Path(path).name.lower()
        if kind == "pipeline":
            if "pipeline" not in schema and not filename.startswith(
                "pipeline-activation"
            ):
                continue
            commit = str(value.get("commit") or "").lower()
            tree = str(value.get("tree") or "").lower()
            repo = str(value.get("canonical_path") or value.get("source") or "")
        else:
            if "pipeline" in schema or filename.startswith("pipeline-activation"):
                continue
            host = value.get("host")
            host = host if isinstance(host, Mapping) else {}
            commit = str(host.get("commit") or value.get("commit") or "").lower()
            tree = str(host.get("tree") or value.get("tree") or "").lower()
            repo = str(value.get("source") or "")
        activated_at = str(value.get("activated_at") or "")
        if HEX40_RE.fullmatch(commit) is None or not activated_at:
            continue
        try:
            parsed = _parse_datetime(activated_at, f"{kind}.activated_at")
        except ScorecardError:
            continue
        records.append({
            "commit": commit,
            "tree": tree,
            "activated_at": parsed,
            "previous_commit": "",
            "reason": str(value.get("reason") or "").strip(),
            "repo": repo,
            "evidence_path": path,
            "time_source": "activation_receipt",
            "rollback_path": str(value.get("rollback_path") or ""),
        })
    return records


def _enrich_lineage(
    records: Sequence[Mapping[str, Any]],
    manifest_records: Sequence[Mapping[str, Any]],
    *,
    git_repo: Path | None,
) -> list[dict[str, Any]]:
    by_commit: dict[str, list[Mapping[str, Any]]] = {}
    for item in manifest_records:
        by_commit.setdefault(str(item.get("commit") or ""), []).append(item)
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for raw in records:
        commit = str(raw.get("commit") or "")
        activated = raw.get("activated_at")
        if not isinstance(activated, datetime):
            continue
        matches = by_commit.get(commit, [])
        nearest = (
            min(
                matches,
                key=lambda item: abs(
                    (item["activated_at"] - activated).total_seconds()
                ),
            )
            if matches
            else {}
        )
        tree = str(raw.get("tree") or nearest.get("tree") or "").lower()
        reason = str(raw.get("reason") or nearest.get("reason") or "").strip()
        previous = str(
            raw.get("previous_commit") or nearest.get("previous_commit") or ""
        ).lower()
        if not reason and git_repo is not None:
            reason = _git_value(git_repo, "show", "-s", "--format=%s", commit)
        key = (commit, _timestamp(activated))
        candidate = {
            "commit": commit,
            "tree": tree,
            "activated_at": activated,
            "reason": reason,
            "previous_commit": previous,
            "evidence_path": str(
                raw.get("evidence_path") or nearest.get("evidence_path") or ""
            ),
            "time_source": str(
                raw.get("time_source") or nearest.get("time_source") or ""
            ),
        }
        existing = deduped.get(key)
        if existing is None or candidate["time_source"] == "activation_receipt":
            deduped[key] = candidate
    ordered = sorted(
        deduped.values(), key=lambda item: (item["activated_at"], item["commit"])
    )
    prior_commit = ""
    for item in ordered:
        if not item["previous_commit"]:
            item["previous_commit"] = prior_commit
        if not item["previous_commit"]:
            item["previous_commit"] = "not_recorded_before_history_window"
        prior_commit = item["commit"]
    return ordered


def _host_lineage_records(
    *,
    release_root: Path,
    manifest_records: Sequence[Mapping[str, Any]],
    receipt_records: Sequence[Mapping[str, Any]],
    git_repo: Path,
) -> list[dict[str, Any]]:
    materialized: list[dict[str, Any]] = []
    release_root_resolved = release_root.resolve()
    for record in [*manifest_records, *receipt_records]:
        repo_text = str(record.get("repo") or "")
        repo = Path(repo_text).expanduser() if repo_text else None
        is_materialized = False
        if repo is not None and repo.exists():
            try:
                repo.resolve().relative_to(release_root_resolved)
                is_materialized = True
            except ValueError:
                pass
        if not is_materialized:
            commit = str(record.get("commit") or "")
            is_materialized = any(
                path.is_dir() and path.name.endswith(commit[:7])
                for path in release_root.glob("hermes-*")
            )
        if is_materialized:
            materialized.append(dict(record))
    return _enrich_lineage(materialized, manifest_records, git_repo=git_repo)


def _lineage_view(
    records: Sequence[Mapping[str, Any]],
    *,
    start: datetime,
    end: datetime,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for raw in records:
        activated = raw.get("activated_at")
        if not isinstance(activated, datetime) or not (start <= activated <= end):
            continue
        commit = _required_hex(raw.get("commit"), "lineage.commit", HEX40_RE)
        tree = _required_hex(raw.get("tree"), "lineage.tree", HEX40_RE)
        previous = _required_text(raw.get("previous_commit"), "lineage.previous_commit")
        if previous != "not_recorded_before_history_window":
            _required_hex(previous, "lineage.previous_commit", HEX40_RE)
        output.append({
            "commit": commit,
            "tree": tree,
            "activated_at": _timestamp(activated),
            "reason": _required_text(raw.get("reason"), "lineage.reason"),
            "previous_commit": previous,
            "time_source": _required_text(
                raw.get("time_source"), "lineage.time_source"
            ),
            "evidence_path": _required_text(
                raw.get("evidence_path"), "lineage.evidence_path"
            ),
        })
    return output


def _release_lineage(
    *,
    current_manifest: Mapping[str, Any],
    task_state_root: Path,
    release_root: Path,
    host_git_repo: Path,
    as_of: datetime,
) -> dict[str, Any]:
    historical_manifests, receipts = _history_material(task_state_root, as_of=as_of)
    host_manifests = _manifest_face_records(
        current_manifest, historical_manifests, HOST_FACE
    )
    pipeline_manifests = _manifest_face_records(
        current_manifest, historical_manifests, PIPELINE_FACE
    )
    host_receipts = _activation_receipt_records(receipts, kind="host")
    pipeline_receipts = _activation_receipt_records(receipts, kind="pipeline")
    host_records = _host_lineage_records(
        release_root=release_root,
        manifest_records=host_manifests,
        receipt_records=host_receipts,
        git_repo=host_git_repo,
    )
    pipeline_records = _enrich_lineage(
        pipeline_receipts, pipeline_manifests, git_repo=None
    )
    local = as_of.astimezone(LOCAL_TIMEZONE)
    today_start = datetime.combine(
        local.date(), time.min, tzinfo=LOCAL_TIMEZONE
    ).astimezone(timezone.utc)
    seven_start = datetime.combine(
        local.date() - timedelta(days=6), time.min, tzinfo=LOCAL_TIMEZONE
    ).astimezone(timezone.utc)
    today = {
        "window_start": _timestamp(today_start),
        "window_end": _timestamp(as_of),
        "host": _lineage_view(host_records, start=today_start, end=as_of),
        "pipeline": _lineage_view(pipeline_records, start=today_start, end=as_of),
    }
    seven_day = {
        "window_start": _timestamp(seven_start),
        "window_end": _timestamp(as_of),
        "host": _lineage_view(host_records, start=seven_start, end=as_of),
        "pipeline": _lineage_view(pipeline_records, start=seven_start, end=as_of),
    }
    for label, view in (("today", today), ("seven_day", seven_day)):
        for kind in ("host", "pipeline"):
            if not view[kind]:
                raise ScorecardError(
                    "required_real_data_empty",
                    f"{label} {kind} release lineage is empty",
                )
        view["host_count"] = len(view["host"])
        view["pipeline_count"] = len(view["pipeline"])
    return {"today": today, "seven_day": seven_day}


def _find_historical_ledger(
    root: Path, *, as_of: datetime
) -> tuple[dict[str, Any], dict[str, Any]]:
    local = as_of.astimezone(LOCAL_TIMEZONE)
    candidates: list[tuple[datetime, dict[str, Any], dict[str, Any]]] = []
    for task_dir in _task_directories(
        root, start=local.date() - timedelta(days=30), end=local.date()
    ):
        for path in task_dir.rglob("rca-71-human-approval-ledger.json"):
            try:
                value, source = _stable_json(path, label="historical 71-ticket ledger")
                observed = _parse_datetime(
                    value.get("observed_at"), "historical_ledger.observed_at"
                )
            except ScorecardError:
                continue
            if observed <= as_of:
                candidates.append((observed, value, source))
    if not candidates:
        raise ScorecardError(
            "historical_ledger_unavailable", "71-ticket historical ledger not found"
        )
    _observed, value, source = max(candidates, key=lambda item: item[0])
    return value, source


def _historical_tiers(
    root: Path, *, as_of: datetime, explicit: Path | None
) -> dict[str, Any]:
    if explicit is not None:
        ledger, source = _stable_json(explicit, label="historical 71-ticket ledger")
    else:
        ledger, source = _find_historical_ledger(root, as_of=as_of)
    summary = _required_mapping(ledger.get("summary"), "historical_ledger.summary")
    raw_counts = _required_mapping(
        summary.get("quality_classifications"),
        "historical_ledger.summary.quality_classifications",
    )
    total = int(summary.get("total") or 0)
    if total <= 0:
        raise ScorecardError(
            "required_real_data_empty", "historical tier total is empty"
        )
    normalized = {str(key): int(value) for key, value in raw_counts.items()}
    technical = int(summary.get("technical_failure") or 0)
    if sum(normalized.values()) + technical != total:
        raise ScorecardError(
            "historical_tier_count_mismatch", "historical tier counts do not sum"
        )
    counts = {
        "high_confidence_supported_attribution": normalized.get(
            "evidence_attribution", 0
        ),
        "medium_confidence_candidate_hypothesis": sum(
            value
            for key, value in normalized.items()
            if "candidate" in key or "needs_review" in key
        ),
        "low_confidence_honest_non_attribution": sum(
            value for key, value in normalized.items() if key.startswith("honest_")
        ),
        "technical_failure": technical,
    }
    if sum(counts.values()) != total:
        raise ScorecardError(
            "historical_tier_count_mismatch", "reported four tiers do not sum"
        )
    return {
        "observed_at": _timestamp(
            _parse_datetime(ledger.get("observed_at"), "historical_ledger.observed_at")
        ),
        "scope_total": total,
        "counts": counts,
        "raw_reported_classifications": normalized,
        "trust": "historical_reported_labels_not_W1_oracle_recomputed",
        "known_caveat": (
            "evidence_attribution is a historical label known to contain classification "
            "conflicts; it must not be interpreted as current high-confidence truth"
        ),
        "source": source,
    }


def _reference_contract() -> dict[str, Any]:
    return {
        "release_claim": {
            "required_status_until_full_acceptance_matrix_passes": "NOT_GA",
            "ga_claim_allowed": False,
        },
        "profile_readiness": {
            "g1q3": "ready",
            "mdrive4": "input_adapter_pending",
        },
        "tier_contract": {
            "high_confidence_supported_attribution": "automatic supported conclusion",
            "medium_confidence_candidate_hypothesis": (
                "automatic candidate; human confirmation required and not a responsibility basis"
            ),
            "low_confidence_honest_non_attribution": "evidence package plus honest non-attribution",
            "technical_failure": "externally silent; internal alert and bounded low-tier fallback",
        },
        "canary_contract": {
            "natural_kafka": [
                "new natural Kafka issue-created trigger",
                "successful formal report delivery and readback",
            ],
            "feishu_topic": [
                "real user topic trigger",
                "issue and original-topic content consistent",
                "initiator mentioned",
                "successful formal report delivery and readback",
            ],
        },
        "source_boundaries": {
            "live": "current manifest, active binding, resident health/state, read-only control DB",
            "reference": "GA contract and deployed profile registry expectations",
            "historical": "archived activation evidence and observed quality ledger",
        },
    }


def build_scorecard(
    paths: ScorecardPaths, *, as_of: datetime | None = None
) -> dict[str, Any]:
    observed = (as_of or datetime.now(timezone.utc)).astimezone(timezone.utc)
    manifest, manifest_source = _stable_json(paths.live_manifest, label="LIVE_MANIFEST")
    binding, binding_source = _stable_json(
        paths.active_binding, label="active release binding"
    )
    manifest_activated = _parse_datetime(
        manifest.get("activated_at"), "LIVE_MANIFEST.activated_at"
    )
    binding_activated = _parse_datetime(
        binding.get("activated_at"), "active_binding.activated_at"
    )
    if manifest_activated != binding_activated:
        raise ScorecardError(
            "active_binding_time_mismatch",
            "manifest and active binding activation differ",
        )
    binding_values = _required_mapping(
        binding.get("bindings"), "active_binding.bindings"
    )
    host_binding = _binding_fingerprint(
        _required_mapping(binding_values.get("host_release"), "bindings.host_release"),
        "bindings.host_release",
    )
    pipeline_binding = _binding_fingerprint(
        _required_mapping(
            binding_values.get("pipeline_release"), "bindings.pipeline_release"
        ),
        "bindings.pipeline_release",
    )
    worker_binding = _binding_fingerprint(
        _required_mapping(
            binding_values.get("worker_release"), "bindings.worker_release"
        ),
        "bindings.worker_release",
    )
    host = _fingerprint(_face(manifest, HOST_FACE), f"face_git_bindings.{HOST_FACE}")
    pipeline = _fingerprint(
        _face(manifest, PIPELINE_FACE), f"face_git_bindings.{PIPELINE_FACE}"
    )
    worker = _fingerprint(
        _face(manifest, WORKER_FACE), f"face_git_bindings.{WORKER_FACE}"
    )
    for name, live_value, bound_value in (
        ("host", host, host_binding),
        ("pipeline", pipeline, pipeline_binding),
        ("worker", worker, worker_binding),
    ):
        if (live_value["commit"], live_value["tree"]) != (
            bound_value["commit"],
            bound_value["tree"],
        ):
            raise ScorecardError(
                "active_binding_fingerprint_mismatch", f"{name} binding mismatch"
            )
    mcap_face = _face(manifest, MCAP_FACE)
    mcap_runtime = _face(manifest, MCAP_RUNTIME_FACE)
    mcap = {
        "commit": _required_hex(
            mcap_face.get("commit"), f"{MCAP_FACE}.commit", HEX40_RE
        ),
        "tree": _required_hex(mcap_face.get("tree"), f"{MCAP_FACE}.tree", HEX40_RE),
        "branch": _required_text(mcap_face.get("branch"), f"{MCAP_FACE}.branch"),
        "repo": _required_text(mcap_face.get("repo"), f"{MCAP_FACE}.repo"),
        "updated_at": _timestamp(
            _parse_datetime(mcap_face.get("updated_at"), f"{MCAP_FACE}.updated_at")
        ),
        "reason": _required_text(mcap_face.get("reason"), f"{MCAP_FACE}.reason"),
        "source_binary_sha256": _required_hex(
            _get_path(mcap_face, "source_required_binary.sha256"),
            f"{MCAP_FACE}.source_required_binary.sha256",
            HEX64_RE,
        ),
        "runtime_contract_sha256": _required_hex(
            mcap_runtime.get("contract_sha256"),
            f"{MCAP_RUNTIME_FACE}.contract_sha256",
            HEX64_RE,
        ),
        "runtime_source_commit": _required_hex(
            mcap_runtime.get("source_commit"),
            f"{MCAP_RUNTIME_FACE}.source_commit",
            HEX40_RE,
        ),
    }
    host_source_text = _required_text(
        _get_path(binding, "bindings.host_release.source"),
        "bindings.host_release.source",
    )
    host_source = Path(host_source_text)
    profiles, profile_source = _load_deployed_profiles(host_source)
    profile_by_id = {item["profile_id"]: item for item in profiles}
    for profile_id in ("g1q3", "mdrive4"):
        if profile_id not in profile_by_id:
            raise ScorecardError(
                "profile_registry_empty", f"required profile missing: {profile_id}"
            )
    residents = _resident_evidence(paths.state_root, paths.gateway_state, host_source)
    local = observed.astimezone(LOCAL_TIMEZONE)
    seven_day_start = datetime.combine(
        local.date() - timedelta(days=6), time.min, tzinfo=LOCAL_TIMEZONE
    ).astimezone(timezone.utc)
    db_path_text = _required_text(
        _get_path(binding, "policy.stores.runtime_state_root"),
        "active_binding.policy.stores.runtime_state_root",
    )
    if Path(db_path_text).resolve() != paths.state_root.resolve():
        raise ScorecardError(
            "active_binding_state_root_mismatch", "configured state root mismatch"
        )
    database = _database_evidence(
        paths.state_root / "control.sqlite3",
        active_since=binding_activated,
        seven_day_start=seven_day_start,
    )
    lineage = _release_lineage(
        current_manifest=manifest,
        task_state_root=paths.task_state_root,
        release_root=paths.release_root,
        host_git_repo=host_source,
        as_of=observed,
    )
    historical_tiers = _historical_tiers(
        paths.task_state_root,
        as_of=observed,
        explicit=paths.historical_ledger,
    )
    scorecard = {
        "schema_version": SCHEMA_VERSION,
        "observed_at": _timestamp(observed),
        "release_status": RELEASE_STATUS,
        "ga_claim_allowed": False,
        "live": {
            "activation_time": _timestamp(binding_activated),
            "release_id": _required_text(
                binding.get("release_id"), "active_binding.release_id"
            ),
            "deployment_id": _required_text(
                binding.get("deployment_id"), "active_binding.deployment_id"
            ),
            "binding_complete": binding.get("complete") is True,
            "fingerprints": {
                "host": host,
                "pipeline": pipeline,
                "worker": worker,
                "mcap": mcap,
            },
            "profile_readiness": profiles,
            "resident_runtime": residents,
            "activation": database["activation"],
            "tier_counts": database["tier_counts"],
            "requester_identity_denominators": database[
                "requester_identity_denominators"
            ],
            "canaries": database["canaries"],
            "real_data": {
                "row_counts": database["row_counts"],
                "database_read_contract": {
                    "open_mode": database["open_mode"],
                    "query_only": database["query_only"],
                    "source": database["source"],
                },
            },
            "sources": {
                "live_manifest": manifest_source,
                "active_binding": binding_source,
                "deployed_profile_registry": profile_source,
            },
        },
        "reference": _reference_contract(),
        "historical": {
            "release_lineage": lineage,
            "reported_tier_counts": historical_tiers,
        },
        "read_only_attestation": {
            "production_mutation_performed": False,
            "database_open_mode": "mode=ro + PRAGMA query_only=ON + read transaction",
            "network_requests_performed": False,
            "external_effects_triggered": False,
        },
    }
    validate_scorecard(scorecard)
    return scorecard


def validate_scorecard(scorecard: Mapping[str, Any]) -> None:
    if scorecard.get("schema_version") != SCHEMA_VERSION:
        raise ScorecardError(
            "scorecard_schema_invalid", "scorecard schema version mismatch"
        )
    if (
        scorecard.get("release_status") != RELEASE_STATUS
        or scorecard.get("ga_claim_allowed") is not False
    ):
        raise ScorecardError("ga_label_invalid", "scorecard must be labeled NOT_GA")
    _parse_datetime(scorecard.get("observed_at"), "observed_at")
    live = _required_mapping(scorecard.get("live"), "live")
    fingerprints = _required_mapping(live.get("fingerprints"), "live.fingerprints")
    for face_name in ("host", "pipeline", "worker"):
        face = _required_mapping(
            fingerprints.get(face_name), f"live.fingerprints.{face_name}"
        )
        _required_hex(
            face.get("commit"), f"live.fingerprints.{face_name}.commit", HEX40_RE
        )
        _required_hex(face.get("tree"), f"live.fingerprints.{face_name}.tree", HEX40_RE)
    mcap = _required_mapping(fingerprints.get("mcap"), "live.fingerprints.mcap")
    _required_hex(mcap.get("commit"), "live.fingerprints.mcap.commit", HEX40_RE)
    _required_hex(mcap.get("tree"), "live.fingerprints.mcap.tree", HEX40_RE)
    _required_hex(
        mcap.get("runtime_contract_sha256"),
        "live.fingerprints.mcap.runtime_contract_sha256",
        HEX64_RE,
    )
    profiles = _required_sequence(
        live.get("profile_readiness"), "live.profile_readiness"
    )
    readiness = {
        _required_text(item.get("profile_id"), "profile.profile_id"): _required_text(
            item.get("execution_readiness"), "profile.execution_readiness"
        )
        for item in profiles
        if isinstance(item, Mapping)
    }
    if not all(name in readiness for name in ("g1q3", "mdrive4")):
        raise ScorecardError(
            "profile_registry_empty", "g1q3 and mdrive4 readiness required"
        )
    activation = _required_mapping(live.get("activation"), "live.activation")
    _required_text(activation.get("state"), "live.activation.state")
    tiers = _required_mapping(live.get("tier_counts"), "live.tier_counts")
    for scope in ("active_release", "seven_day"):
        projection = _required_mapping(tiers.get(scope), f"live.tier_counts.{scope}")
        counts = _required_mapping(
            projection.get("counts"), f"live.tier_counts.{scope}.counts"
        )
        for tier in FOUR_TIERS:
            value = counts.get(tier)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ScorecardError("tier_count_invalid", f"{scope}.{tier} is invalid")
    canaries = _required_mapping(live.get("canaries"), "live.canaries")
    for kind in ("natural_kafka", "feishu_topic"):
        canary = _required_mapping(canaries.get(kind), f"live.canaries.{kind}")
        _required_text(canary.get("state"), f"live.canaries.{kind}.state")
    identities = _required_mapping(
        live.get("requester_identity_denominators"),
        "live.requester_identity_denominators",
    )
    identity_counts = _required_mapping(
        identities.get("counts"), "live.requester_identity_denominators.counts"
    )
    for kind in ("human", "automation", "legacy_automation", "unknown"):
        value = identity_counts.get(kind)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ScorecardError(
                "requester_identity_count_invalid", f"{kind} is invalid"
            )
    if sum(identity_counts.values()) != identities.get("total_triggers"):
        raise ScorecardError(
            "requester_identity_count_invalid", "requester identity counts do not sum"
        )
    row_counts = _required_mapping(
        _get_path(live, "real_data.row_counts"), "live.real_data.row_counts"
    )
    if any(not isinstance(value, int) or value <= 0 for value in row_counts.values()):
        raise ScorecardError(
            "required_real_data_empty", "live row counts must be positive"
        )
    historical = _required_mapping(scorecard.get("historical"), "historical")
    lineage = _required_mapping(
        historical.get("release_lineage"), "historical.release_lineage"
    )
    latest_lineage: dict[str, str] = {}
    for window in ("today", "seven_day"):
        view = _required_mapping(lineage.get(window), f"release_lineage.{window}")
        for kind in ("host", "pipeline"):
            entries = _required_sequence(
                view.get(kind), f"release_lineage.{window}.{kind}"
            )
            for index, entry in enumerate(entries):
                if not isinstance(entry, Mapping):
                    raise ScorecardError(
                        "lineage_invalid", f"{window}.{kind}[{index}] invalid"
                    )
                _required_hex(
                    entry.get("commit"), f"{window}.{kind}[{index}].commit", HEX40_RE
                )
                _parse_datetime(
                    entry.get("activated_at"), f"{window}.{kind}[{index}].activated_at"
                )
                _required_text(entry.get("reason"), f"{window}.{kind}[{index}].reason")
                _required_text(
                    entry.get("previous_commit"),
                    f"{window}.{kind}[{index}].previous_commit",
                )
            if window == "today":
                latest_lineage[kind] = str(entries[-1]["commit"])
    for kind in ("host", "pipeline"):
        if latest_lineage.get(kind) != fingerprints[kind]["commit"]:
            raise ScorecardError(
                "lineage_active_binding_mismatch",
                f"latest {kind} lineage does not match active binding",
            )
    reported = _required_mapping(
        historical.get("reported_tier_counts"), "historical.reported_tier_counts"
    )
    if int(reported.get("scope_total") or 0) <= 0:
        raise ScorecardError(
            "required_real_data_empty", "historical tier scope is empty"
        )
    counts = _required_mapping(
        reported.get("counts"), "historical.reported_tier_counts.counts"
    )
    for tier in FOUR_TIERS:
        value = counts.get(tier)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ScorecardError("tier_count_invalid", f"historical.{tier} is invalid")
    reference = _required_mapping(scorecard.get("reference"), "reference")
    _required_mapping(reference.get("source_boundaries"), "reference.source_boundaries")
    attestation = _required_mapping(
        scorecard.get("read_only_attestation"), "read_only_attestation"
    )
    if (
        attestation.get("production_mutation_performed") is not False
        or attestation.get("external_effects_triggered") is not False
    ):
        raise ScorecardError(
            "read_only_attestation_invalid", "scorecard is not read-only"
        )


def render_markdown(scorecard: Mapping[str, Any]) -> str:
    validate_scorecard(scorecard)
    live = scorecard["live"]
    lines = [
        "# PNC RCA Release Scorecard - NOT GA",
        "",
        f"Observed: `{scorecard['observed_at']}`",
        "",
        "> This scorecard is explicitly **NOT GA**. It is read-only evidence, not a release gate override.",
        "",
        "## Live",
        "",
        f"Release: `{live['release_id']}`; activation: `{live['activation']['state']}`",
        "",
        "| Face | Commit | Tree / Contract |",
        "|---|---|---|",
    ]
    for face in ("host", "pipeline", "worker"):
        item = live["fingerprints"][face]
        lines.append(f"| {face} | `{item['commit']}` | `{item['tree']}` |")
    mcap = live["fingerprints"]["mcap"]
    lines.append(f"| mcap | `{mcap['commit']}` | `{mcap['runtime_contract_sha256']}` |")
    lines.extend([
        "",
        "### Profile Readiness",
        "",
        "| Profile | Readiness | Evaluator scope |",
        "|---|---|---|",
    ])
    for profile in live["profile_readiness"]:
        lines.append(
            f"| {profile['profile_id']} | `{profile['execution_readiness']}` | `{profile['evaluator_scope']}` |"
        )
    lines.extend(["", "### Tier Counts", ""])
    active_tiers = live["tier_counts"]["active_release"]
    for tier, count in active_tiers["counts"].items():
        lines.append(f"- `{tier}`: {count}")
    lines.append(f"- `unclassified`: {active_tiers['unclassified']}")
    lines.extend(["", "### Requester Identity Denominators", ""])
    identity_counts = live["requester_identity_denominators"]["counts"]
    for identity_kind, count in identity_counts.items():
        lines.append(f"- `{identity_kind}`: {count}")
    lines.extend(["", "### Canary State", ""])
    for kind, canary in live["canaries"].items():
        lines.append(f"- `{kind}`: **{canary['state']}**")
    lines.extend(["", "## Reference", ""])
    lines.append(
        "The reference contract remains NOT GA until the full acceptance matrix and both current-release canaries pass."
    )
    lines.extend(["", "## Historical", ""])
    lineage = scorecard["historical"]["release_lineage"]
    for window in ("today", "seven_day"):
        view = lineage[window]
        lines.extend([
            f"### Release Lineage - {window}",
            "",
            f"Host activations: {view['host_count']}; pipeline activations: {view['pipeline_count']}",
            "",
            "| Face | Activated | Commit | Previous | Reason |",
            "|---|---|---|---|---|",
        ])
        for face in ("host", "pipeline"):
            for item in view[face]:
                reason = str(item["reason"]).replace("|", "\\|")
                lines.append(
                    f"| {face} | `{item['activated_at']}` | `{item['commit']}` | "
                    f"`{item['previous_commit']}` | {reason} |"
                )
        lines.append("")
    historical_tiers = scorecard["historical"]["reported_tier_counts"]
    lines.append(
        "Historical 71-ticket counts are reported labels, not W1-oracle results; "
        "the historical `evidence_attribution` label is known to contain conflicts."
    )
    lines.extend(["", "## Read-only Attestation", ""])
    lines.append(
        "No production mutation, restart, DB write, trigger, publish, network request, or external effect was performed."
    )
    return "\n".join(lines).rstrip() + "\n"


def _default_paths() -> ScorecardPaths:
    home = Path.home()
    state_root = home / ".hermes" / "runtime" / "pnc_agent" / "feishu_issue_kafka_rca"
    return ScorecardPaths(
        live_manifest=home / ".hermes" / "runtime" / "LIVE_MANIFEST.json",
        active_binding=state_root / "active-release-binding.json",
        state_root=state_root,
        task_state_root=home / ".codex" / "memories" / "task-state" / "tasks",
        release_root=home / ".hermes" / "runtime" / "releases",
        gateway_state=home / ".hermes" / "gateway_state.json",
    )


def _arguments(argv: Sequence[str] | None = None) -> argparse.Namespace:
    defaults = _default_paths()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--validate", type=Path, help="validate an existing scorecard JSON"
    )
    parser.add_argument("--format", choices=("json", "markdown"), default="json")
    parser.add_argument("--as-of", help="bounded ISO-8601 observation time")
    parser.add_argument("--live-manifest", type=Path, default=defaults.live_manifest)
    parser.add_argument("--active-binding", type=Path, default=defaults.active_binding)
    parser.add_argument("--state-root", type=Path, default=defaults.state_root)
    parser.add_argument(
        "--task-state-root", type=Path, default=defaults.task_state_root
    )
    parser.add_argument("--release-root", type=Path, default=defaults.release_root)
    parser.add_argument("--gateway-state", type=Path, default=defaults.gateway_state)
    parser.add_argument("--historical-ledger", type=Path)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _arguments(argv)
    try:
        if args.validate is not None:
            value, _source = _stable_json(
                args.validate, label="scorecard validation input"
            )
            validate_scorecard(value)
            print(
                json.dumps(
                    {"ok": True, "schema_version": SCHEMA_VERSION}, sort_keys=True
                )
            )
            return 0
        as_of = _parse_datetime(args.as_of, "as_of") if args.as_of else None
        scorecard = build_scorecard(
            ScorecardPaths(
                live_manifest=args.live_manifest,
                active_binding=args.active_binding,
                state_root=args.state_root,
                task_state_root=args.task_state_root,
                release_root=args.release_root,
                gateway_state=args.gateway_state,
                historical_ledger=args.historical_ledger,
            ),
            as_of=as_of,
        )
        if args.format == "markdown":
            sys.stdout.write(render_markdown(scorecard))
        else:
            print(json.dumps(scorecard, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    except ScorecardError as exc:
        print(
            json.dumps(
                {"ok": False, "code": exc.code, "detail": exc.detail},
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
