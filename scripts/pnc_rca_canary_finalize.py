#!/usr/bin/env python3
"""Finalize exact real RCA canaries and advance a confirmed epoch to steady."""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
from typing import Any
from urllib.parse import quote

from scripts import pnc_rca_activation as activation
from scripts import pnc_rca_canary_collector as collector
from scripts import pnc_rca_cutover_adapter as adapter
from scripts import pnc_rca_postinstall_activation as postinstall
from scripts import pnc_rca_production_cutover as cutover
from scripts import pnc_rca_release_gate as release_gate


MANIFEST_SCHEMA_VERSION = "pnc_rca_canary_finalization_manifest_v1"
RECONCILIATION_SCHEMA_VERSION = "pnc_rca_reconciliation_plan_v1"
RECEIPT_SCHEMA_VERSION = "pnc_rca_canary_finalization_receipt_v1"
AUTHORIZATION_DECISION = "authorize_exact_rca_canary_confirmation_and_steady"
CANARY_MODE = "canary_bootstrap"
PRODUCTION_MODE = "production_bootstrap"
SOURCE_ID_RE = re.compile(r"g1q3-rca-source-v1-[0-9a-f]{64}\Z")


class CanaryFinalizationError(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class ReconciliationEntry:
    event_uid: str
    action: str


@dataclass(frozen=True)
class FinalizationInputs:
    candidate_python: Path
    control_db: Path
    delivery_db: Path
    evidence_dir: Path
    live_env: Path
    group_binding_receipt_dir: Path
    preproduction_capsule: Path
    preproduction_capsule_sha256: str
    postinstall_receipt: Path
    postinstall_receipt_sha256: str
    release_id: str
    bootstrap_epoch_id: str
    activation_epoch_id: str
    expected_topic: str
    expected_rule_version: str
    kafka_event_uid: str
    manual_success_identity: Path
    manual_success_identity_sha256: str
    manual_terminal_failure_identity: Path
    manual_terminal_failure_identity_sha256: str
    canary_gate_receipt: Path
    production_candidate: Path
    production_gate_receipt: Path
    production_confirmation_capsule: Path
    active_release_binding: Path
    reconciliation_plan: Path
    reconciliation_plan_sha256: str
    reconciliation_entries: tuple[ReconciliationEntry, ...]
    runtime_content_sha256: str
    journal_root: Path
    lock_path: Path
    receipt_path: Path


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _owned_json(path: Path, *, artifact: str) -> cutover._OwnedJson:
    try:
        return cutover._read_owned_json(path, artifact=artifact)
    except (OSError, ValueError) as exc:
        raise CanaryFinalizationError(f"canary_finalize_{artifact}_invalid") from exc


def _required_sha256(value: Any, *, field: str) -> str:
    normalized = str(value or "")
    if postinstall.SHA256_RE.fullmatch(normalized) is None:
        raise CanaryFinalizationError(f"canary_finalize_{field}_invalid")
    return normalized


def _manual_identity(
    path: Path,
    *,
    expected_sha256: str,
    artifact: str,
) -> tuple[dict[str, str], str]:
    owned = _owned_json(path, artifact=artifact)
    if owned.sha256 != expected_sha256:
        raise CanaryFinalizationError(f"canary_finalize_{artifact}_sha256_mismatch")
    try:
        identity = activation._normalize_manual_identity(owned.body)
    except ValueError as exc:
        raise CanaryFinalizationError(f"canary_finalize_{artifact}_invalid") from exc
    if identity.get("mode") != "run_or_join":
        raise CanaryFinalizationError(f"canary_finalize_{artifact}_mode_invalid")
    return identity, owned.sha256


def _source_id(storage_source_kind: str, dedupe_key: str) -> str:
    value = collector._stable_trigger_source_id(storage_source_kind, dedupe_key)
    if SOURCE_ID_RE.fullmatch(value) is None:
        raise CanaryFinalizationError("canary_finalize_source_id_invalid")
    return value


def _load_reconciliation_plan(
    path: Path,
    *,
    expected_sha256: str,
    activation_epoch_id: str,
    expected_topic: str,
    kafka_event_uid: str,
) -> tuple[ReconciliationEntry, ...]:
    owned = _owned_json(path, artifact="reconciliation_plan")
    if owned.sha256 != expected_sha256:
        raise CanaryFinalizationError(
            "canary_finalize_reconciliation_plan_sha256_mismatch"
        )
    body = owned.body
    if set(body) != {"schema_version", "activation_epoch_id", "entries"} or (
        body.get("schema_version") != RECONCILIATION_SCHEMA_VERSION
        or body.get("activation_epoch_id") != activation_epoch_id
        or not isinstance(body.get("entries"), list)
    ):
        raise CanaryFinalizationError("canary_finalize_reconciliation_plan_invalid")
    entries: list[ReconciliationEntry] = []
    keys: list[tuple[str, int, int]] = []
    for item in body["entries"]:
        if not isinstance(item, Mapping) or set(item) != {"event_uid", "action"}:
            raise CanaryFinalizationError(
                "canary_finalize_reconciliation_plan_invalid"
            )
        event_uid = str(item.get("event_uid") or "")
        action = str(item.get("action") or "")
        try:
            topic, partition, offset = collector._parse_event_uid(event_uid)
        except ValueError as exc:
            raise CanaryFinalizationError(
                "canary_finalize_reconciliation_event_invalid"
            ) from exc
        if (
            topic != expected_topic
            or event_uid == kafka_event_uid
            or action not in {"reconcile", "defer"}
        ):
            raise CanaryFinalizationError(
                "canary_finalize_reconciliation_event_invalid"
            )
        entries.append(ReconciliationEntry(event_uid, action))
        keys.append((topic, partition, offset))
    if len(keys) != len(set(keys)) or keys != sorted(keys):
        raise CanaryFinalizationError("canary_finalize_reconciliation_order_invalid")
    return tuple(entries)


def _validate_postinstall_receipt(
    inputs: FinalizationInputs,
) -> Mapping[str, Any]:
    owned = _owned_json(inputs.postinstall_receipt, artifact="postinstall_receipt")
    body = owned.body
    health = body.get("resident_health")
    if (
        owned.sha256 != inputs.postinstall_receipt_sha256
        or body.get("schema_version") != postinstall.RECEIPT_SCHEMA_VERSION
        or body.get("ok") is not True
        or body.get("release_id") != inputs.release_id
        or body.get("bootstrap_epoch_id") != inputs.bootstrap_epoch_id
        or body.get("activation_epoch_id") != inputs.activation_epoch_id
        or body.get("activation_state") != "bounded_active"
        or body.get("real_canaries_completed") is not False
        or body.get("next_phase") != "execute_exact_kafka_and_manual_canaries"
        or not isinstance(health, Mapping)
        or set(health) != set(cutover.RESIDENT_LABELS)
        or any(
            not isinstance(health.get(label), Mapping)
            or health[label].get("health_ok") is not True
            or health[label].get("runtime_sha256")
            != inputs.runtime_content_sha256
            for label in cutover.RESIDENT_LABELS
        )
    ):
        raise CanaryFinalizationError("canary_finalize_postinstall_receipt_invalid")
    return body


def load_manifest(path: Path) -> FinalizationInputs:
    body = _owned_json(path, artifact="manifest").body
    expected = {
        "schema_version",
        "candidate_python",
        "control_db",
        "delivery_db",
        "evidence_dir",
        "live_env",
        "group_binding_receipt_dir",
        "preproduction_capsule",
        "preproduction_capsule_sha256",
        "postinstall_receipt",
        "postinstall_receipt_sha256",
        "release_id",
        "bootstrap_epoch_id",
        "activation_epoch_id",
        "expected_topic",
        "expected_rule_version",
        "kafka_event_uid",
        "manual_success_identity",
        "manual_success_identity_sha256",
        "manual_terminal_failure_identity",
        "manual_terminal_failure_identity_sha256",
        "canary_gate_receipt",
        "production_candidate",
        "production_gate_receipt",
        "production_confirmation_capsule",
        "active_release_binding",
        "reconciliation_plan",
        "reconciliation_plan_sha256",
        "runtime_content_sha256",
        "journal_root",
        "lock_path",
        "receipt_path",
    }
    if set(body) != expected or body.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        raise CanaryFinalizationError("canary_finalize_manifest_shape_invalid")
    absolute = lambda name: postinstall._absolute(body.get(name), field=name)
    candidate_python = absolute("candidate_python")
    if candidate_python != cutover.CANONICAL_RUNTIME_ROOT / ".venv/bin/python":
        raise CanaryFinalizationError("canary_finalize_candidate_python_invalid")
    evidence_dir = absolute("evidence_dir")
    journal_root = absolute("journal_root")
    group_receipts = absolute("group_binding_receipt_dir")
    for field, directory in (
        ("evidence_dir", evidence_dir),
        ("journal_root", journal_root),
        ("group_binding_receipt_dir", group_receipts),
    ):
        postinstall._owner_directory(directory, field=field)
    release_id = str(body.get("release_id") or "")
    epoch_id = str(body.get("activation_epoch_id") or "")
    bootstrap_epoch = str(body.get("bootstrap_epoch_id") or "")
    topic = str(body.get("expected_topic") or "")
    rule = str(body.get("expected_rule_version") or "")
    event_uid = str(body.get("kafka_event_uid") or "")
    try:
        event_topic, _partition, _offset = collector._parse_event_uid(event_uid)
    except ValueError as exc:
        raise CanaryFinalizationError("canary_finalize_kafka_event_uid_invalid") from exc
    if (
        cutover.RELEASE_ID_RE.fullmatch(release_id) is None
        or activation._EPOCH_ID_RE.fullmatch(epoch_id) is None
        or activation._EPOCH_ID_RE.fullmatch(bootstrap_epoch) is None
        or not topic
        or not rule.strip()
        or event_topic != topic
    ):
        raise CanaryFinalizationError("canary_finalize_manifest_binding_invalid")
    preproduction = absolute("preproduction_capsule")
    postinstall_receipt = absolute("postinstall_receipt")
    manual_success = absolute("manual_success_identity")
    manual_failure = absolute("manual_terminal_failure_identity")
    canary_receipt = absolute("canary_gate_receipt")
    production_candidate = absolute("production_candidate")
    production_receipt = absolute("production_gate_receipt")
    confirmation = absolute("production_confirmation_capsule")
    reconciliation_plan = absolute("reconciliation_plan")
    lock_path = absolute("lock_path")
    receipt_path = absolute("receipt_path")
    if (
        canary_receipt
        != evidence_dir / f"release_gate_{CANARY_MODE}_{release_id}.json"
        or production_candidate
        != evidence_dir / "activation_production_candidate.json"
        or production_receipt
        != evidence_dir / f"release_gate_{PRODUCTION_MODE}_{release_id}.json"
        or confirmation
        != release_gate.activation_confirmation_capsule_path(production_receipt)
    ):
        raise CanaryFinalizationError("canary_finalize_output_path_invalid")
    for field, output in (
        ("canary_gate_receipt_parent", canary_receipt),
        ("production_candidate_parent", production_candidate),
        ("production_gate_receipt_parent", production_receipt),
        ("lock_parent", lock_path),
        ("receipt_parent", receipt_path),
    ):
        postinstall._owner_directory(output.parent, field=field)
    preproduction_sha = _required_sha256(
        body.get("preproduction_capsule_sha256"),
        field="preproduction_capsule_sha256",
    )
    postinstall_sha = _required_sha256(
        body.get("postinstall_receipt_sha256"), field="postinstall_receipt_sha256"
    )
    manual_success_sha = _required_sha256(
        body.get("manual_success_identity_sha256"),
        field="manual_success_identity_sha256",
    )
    manual_failure_sha = _required_sha256(
        body.get("manual_terminal_failure_identity_sha256"),
        field="manual_terminal_failure_identity_sha256",
    )
    reconciliation_sha = _required_sha256(
        body.get("reconciliation_plan_sha256"),
        field="reconciliation_plan_sha256",
    )
    runtime_sha = _required_sha256(
        body.get("runtime_content_sha256"), field="runtime_content_sha256"
    )
    if _owned_json(preproduction, artifact="preproduction_capsule").sha256 != (
        preproduction_sha
    ):
        raise CanaryFinalizationError(
            "canary_finalize_preproduction_capsule_sha256_mismatch"
        )
    success_identity, _ = _manual_identity(
        manual_success,
        expected_sha256=manual_success_sha,
        artifact="manual_success_identity",
    )
    failure_identity, _ = _manual_identity(
        manual_failure,
        expected_sha256=manual_failure_sha,
        artifact="manual_terminal_failure_identity",
    )
    if success_identity == failure_identity:
        raise CanaryFinalizationError("canary_finalize_manual_identity_reused")
    entries = _load_reconciliation_plan(
        reconciliation_plan,
        expected_sha256=reconciliation_sha,
        activation_epoch_id=epoch_id,
        expected_topic=topic,
        kafka_event_uid=event_uid,
    )
    inputs = FinalizationInputs(
        candidate_python=candidate_python,
        control_db=absolute("control_db"),
        delivery_db=absolute("delivery_db"),
        evidence_dir=evidence_dir,
        live_env=absolute("live_env"),
        group_binding_receipt_dir=group_receipts,
        preproduction_capsule=preproduction,
        preproduction_capsule_sha256=preproduction_sha,
        postinstall_receipt=postinstall_receipt,
        postinstall_receipt_sha256=postinstall_sha,
        release_id=release_id,
        bootstrap_epoch_id=bootstrap_epoch,
        activation_epoch_id=epoch_id,
        expected_topic=topic,
        expected_rule_version=rule,
        kafka_event_uid=event_uid,
        manual_success_identity=manual_success,
        manual_success_identity_sha256=manual_success_sha,
        manual_terminal_failure_identity=manual_failure,
        manual_terminal_failure_identity_sha256=manual_failure_sha,
        canary_gate_receipt=canary_receipt,
        production_candidate=production_candidate,
        production_gate_receipt=production_receipt,
        production_confirmation_capsule=confirmation,
        active_release_binding=absolute("active_release_binding"),
        reconciliation_plan=reconciliation_plan,
        reconciliation_plan_sha256=reconciliation_sha,
        reconciliation_entries=entries,
        runtime_content_sha256=runtime_sha,
        journal_root=journal_root,
        lock_path=lock_path,
        receipt_path=receipt_path,
    )
    _validate_postinstall_receipt(inputs)
    return inputs


def _collector_sources(inputs: FinalizationInputs) -> tuple[tuple[str, str, str], ...]:
    success, _ = _manual_identity(
        inputs.manual_success_identity,
        expected_sha256=inputs.manual_success_identity_sha256,
        artifact="manual_success_identity",
    )
    failure, _ = _manual_identity(
        inputs.manual_terminal_failure_identity,
        expected_sha256=inputs.manual_terminal_failure_identity_sha256,
        artifact="manual_terminal_failure_identity",
    )
    return (
        (
            "kafka-success",
            _source_id("kafka_workflow_event", inputs.kafka_event_uid),
            "primary",
        ),
        (
            "manual-success",
            _source_id("feishu_group_manual", f"feishu:{success['message_id']}"),
            "manual_success",
        ),
        (
            "manual-terminal-failure",
            _source_id("feishu_group_manual", f"feishu:{failure['message_id']}"),
            "manual_terminal_failure",
        ),
    )


def _collector_argv(
    inputs: FinalizationInputs,
    *,
    source_id: str,
    role: str,
    write: bool,
) -> list[str]:
    argv = [
        str(inputs.candidate_python),
        "-B",
        str(cutover.CANONICAL_RUNTIME_ROOT / "scripts/pnc_rca_canary_collector.py"),
        "--source-id",
        source_id,
        "--env-file",
        str(inputs.live_env),
        "--control-db",
        str(inputs.control_db),
        "--delivery-db",
        str(inputs.delivery_db),
        "--evidence-dir",
        str(inputs.evidence_dir),
        "--group-binding-receipt-dir",
        str(inputs.group_binding_receipt_dir),
        "--write" if write else "--dry-run",
    ]
    if role == "manual_success":
        argv.append("--manual-success")
    elif role == "manual_terminal_failure":
        argv.append("--terminal-failure")
    return argv


def _validate_collector_result(
    body: Mapping[str, Any], *, role: str, write: bool
) -> None:
    written = body.get("written_files")
    if (
        body.get("ok") is not True
        or body.get("mode") != ("write" if write else "dry_run")
        or body.get("read_only_collection") is not True
        or body.get("external_side_effects") is not False
        or body.get("evidence_role") != role
        or not isinstance(body.get("evidence_commit_id"), str)
        or postinstall.SHA256_RE.fullmatch(body["evidence_commit_id"]) is None
        or not isinstance(body.get("evidence_manifest"), str)
        or not body["evidence_manifest"].endswith("_commit.json")
        or not isinstance(written, list)
        or (write and len(written) != 3)
        or (not write and written != [])
    ):
        raise CanaryFinalizationError("canary_finalize_collector_result_invalid")


def _production_candidate_argv(inputs: FinalizationInputs) -> list[str]:
    return [
        str(inputs.candidate_python),
        "-B",
        str(cutover.CANONICAL_RUNTIME_ROOT / "scripts/pnc_rca_release_gate.py"),
        "--mode",
        PRODUCTION_MODE,
        "--evidence-dir",
        str(inputs.evidence_dir),
        "--env-file",
        str(inputs.live_env),
        "--expected-topic",
        inputs.expected_topic,
        "--expected-rule-version",
        inputs.expected_rule_version,
        "--preproduction-capsule",
        str(inputs.preproduction_capsule),
        "--collect-activation-production-candidate",
        str(inputs.production_candidate),
    ]


def _run_candidate_step(
    inputs: FinalizationInputs,
    *,
    runner: adapter.CommandRunner,
    index: int,
) -> Mapping[str, Any]:
    argv = tuple(_production_candidate_argv(inputs))
    name = "collect-activation-production-candidate"
    path = inputs.journal_root / f"{index:02d}-{name}.json"
    argv_sha = _sha256(json.dumps(list(argv), separators=(",", ":")).encode())
    if path.exists() or path.is_symlink():
        journal = _owned_json(path, artifact="candidate_journal").body
        body = journal.get("result")
        if (
            journal.get("schema_version") != postinstall.JOURNAL_SCHEMA_VERSION
            or journal.get("index") != index
            or journal.get("name") != name
            or journal.get("argv") != list(argv)
            or journal.get("argv_sha256") != argv_sha
            or not isinstance(body, Mapping)
        ):
            raise CanaryFinalizationError("canary_finalize_candidate_journal_invalid")
    else:
        result = runner.run(argv)
        if result.argv != argv or result.returncode != 0:
            raise CanaryFinalizationError("canary_finalize_candidate_command_failed")
        body = postinstall._strict_output(
            result.stdout, code="canary_finalize_candidate_output_invalid"
        )
        cutover._publish_no_clobber(
            path,
            {
                "schema_version": postinstall.JOURNAL_SCHEMA_VERSION,
                "index": index,
                "name": name,
                "argv": list(argv),
                "argv_sha256": argv_sha,
                "result": body,
            },
        )
    if (
        body.get("schema_version")
        != release_gate.ACTIVATION_PRODUCTION_CANDIDATE_SCHEMA_VERSION
        or body.get("read_only") is not True
        or body.get("external_side_effects") is not False
        or body.get("epoch_id") != inputs.activation_epoch_id
    ):
        raise CanaryFinalizationError("canary_finalize_candidate_output_invalid")
    artifact = _owned_json(inputs.production_candidate, artifact="production_candidate")
    if artifact.body != body:
        raise CanaryFinalizationError("canary_finalize_candidate_file_mismatch")
    return body


def _activation_status(
    inputs: FinalizationInputs, runner: adapter.CommandRunner
) -> Mapping[str, Any]:
    argv = tuple(
        postinstall._activation_argv(
            inputs, "status", "--epoch-id", inputs.activation_epoch_id
        )
    )
    result = runner.run(argv)
    if result.argv != argv or result.returncode != 0:
        raise CanaryFinalizationError("canary_finalize_activation_status_failed")
    body = postinstall._strict_output(
        result.stdout, code="canary_finalize_activation_status_invalid"
    )
    current = (
        body.get("result", {}).get("activation", {}).get("current_epoch")
        if isinstance(body.get("result"), Mapping)
        else None
    )
    if (
        body.get("schema_version") != activation.ACTIVATION_CLI_SCHEMA_VERSION
        or body.get("command") != "status"
        or body.get("mode") != "read_only"
        or body.get("ok") is not True
        or not isinstance(current, Mapping)
        or current.get("epoch_id") != inputs.activation_epoch_id
    ):
        raise CanaryFinalizationError("canary_finalize_activation_status_invalid")
    return current


def _secure_database(path: Path) -> tuple[int, int]:
    selected = path.expanduser().absolute()
    info = selected.lstat()
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.geteuid()
        or info.st_nlink != 1
        or stat.S_IMODE(info.st_mode) != 0o600
    ):
        raise CanaryFinalizationError("canary_finalize_control_database_invalid")
    return info.st_dev, info.st_ino


def observe_reconciliation_state(
    inputs: FinalizationInputs,
) -> Mapping[str, Any]:
    identity = _secure_database(inputs.control_db)
    uri = f"file:{quote(str(inputs.control_db.resolve(strict=True)), safe='/')}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, isolation_level=None, timeout=5)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        rows = connection.execute(
            "SELECT source_event_id, status, last_error_code, activation_epoch_id "
            "FROM rca_outbox WHERE source_event_id != ''"
        ).fetchall()
        connection.commit()
    except sqlite3.Error as exc:
        raise CanaryFinalizationError(
            "canary_finalize_reconciliation_observation_failed"
        ) from exc
    finally:
        connection.close()
    if _secure_database(inputs.control_db) != identity:
        raise CanaryFinalizationError("canary_finalize_control_database_changed")
    entries: dict[str, dict[str, str]] = {}
    shadow: list[str] = []
    for row in rows:
        event_uid = str(row["source_event_id"] or "")
        if event_uid in entries:
            raise CanaryFinalizationError("canary_finalize_reconciliation_duplicate")
        status = str(row["status"] or "")
        entries[event_uid] = {
            "status": status,
            "last_error_code": str(row["last_error_code"] or ""),
            "activation_epoch_id": str(row["activation_epoch_id"] or ""),
        }
        if status == "shadow":
            shadow.append(event_uid)
    return {"entries": entries, "shadow_event_uids": sorted(shadow)}


ReconciliationObserver = Callable[[FinalizationInputs], Mapping[str, Any]]


def _validate_reconciliation_state(
    inputs: FinalizationInputs,
    observed: Mapping[str, Any],
    *,
    complete: bool,
) -> Mapping[str, Mapping[str, str]]:
    raw_entries = observed.get("entries")
    shadow = observed.get("shadow_event_uids")
    if not isinstance(raw_entries, Mapping) or not isinstance(shadow, list):
        raise CanaryFinalizationError("canary_finalize_reconciliation_state_invalid")
    expected = {entry.event_uid: entry for entry in inputs.reconciliation_entries}
    if set(shadow) - set(expected):
        raise CanaryFinalizationError("canary_finalize_unplanned_shadow_backlog")
    normalized: dict[str, Mapping[str, str]] = {}
    for event_uid, plan in expected.items():
        state = raw_entries.get(event_uid)
        if not isinstance(state, Mapping) or state.get("activation_epoch_id") != (
            inputs.activation_epoch_id
        ):
            raise CanaryFinalizationError(
                "canary_finalize_reconciliation_event_state_invalid"
            )
        status = str(state.get("status") or "")
        error = str(state.get("last_error_code") or "")
        allowed = (
            {"pending", "claimed", "completed"}
            if plan.action == "reconcile" and complete
            else {"shadow", "pending", "claimed", "completed"}
            if plan.action == "reconcile"
            else {"quarantined"}
            if complete
            else {"shadow", "quarantined"}
        )
        if status not in allowed or (
            plan.action == "defer"
            and status == "quarantined"
            and error != "activation_epoch_deferred"
        ):
            raise CanaryFinalizationError(
                "canary_finalize_reconciliation_event_state_invalid"
            )
        normalized[event_uid] = {
            "status": status,
            "last_error_code": error,
            "activation_epoch_id": str(state.get("activation_epoch_id") or ""),
        }
    if complete and shadow:
        raise CanaryFinalizationError("canary_finalize_shadow_backlog_not_drained")
    return normalized


def _validate_final_receipt(
    inputs: FinalizationInputs,
    body: Mapping[str, Any],
) -> None:
    if (
        body.get("schema_version") != RECEIPT_SCHEMA_VERSION
        or body.get("ok") is not True
        or body.get("authorization_decision") != AUTHORIZATION_DECISION
        or body.get("release_id") != inputs.release_id
        or body.get("bootstrap_epoch_id") != inputs.bootstrap_epoch_id
        or body.get("activation_epoch_id") != inputs.activation_epoch_id
        or body.get("real_canaries_completed") is not True
        or body.get("activation_state") != "steady_active"
        or body.get("reconciliation_plan_sha256")
        != inputs.reconciliation_plan_sha256
    ):
        raise CanaryFinalizationError("canary_finalize_receipt_invalid")


def run_canary_finalization(
    inputs: FinalizationInputs,
    *,
    authorization_decision: str,
    operator: str,
    reason: str,
    runner: adapter.CommandRunner | None = None,
    reconciliation_observer: ReconciliationObserver = observe_reconciliation_state,
) -> Mapping[str, Any]:
    if authorization_decision != AUTHORIZATION_DECISION:
        raise CanaryFinalizationError("canary_finalize_authorization_invalid")
    if not operator.strip() or not reason.strip():
        raise CanaryFinalizationError("canary_finalize_audit_invalid")
    active_runner = runner or adapter.SubprocessArgvRunner()
    audit = ["--operator", operator.strip(), "--reason", reason.strip()]
    with postinstall._session_lock(inputs.lock_path):
        _validate_postinstall_receipt(inputs)
        current = _activation_status(inputs, active_runner)
        if inputs.receipt_path.exists() or inputs.receipt_path.is_symlink():
            receipt = _owned_json(inputs.receipt_path, artifact="final_receipt").body
            _validate_final_receipt(inputs, receipt)
            if current.get("state") != "steady_active":
                raise CanaryFinalizationError("canary_finalize_steady_state_changed")
            return receipt
        if current.get("state") not in {"bounded_active", "confirmed", "steady_active"}:
            raise CanaryFinalizationError("canary_finalize_activation_state_invalid")

        index = 1
        collection_results: dict[str, Mapping[str, Any]] = {}
        for name, source_id, role in _collector_sources(inputs):
            for write in (False, True):
                result = postinstall._run_step(
                    inputs=inputs,
                    runner=active_runner,
                    index=index,
                    name=f"collect-{name}-{'write' if write else 'dry-run'}",
                    argv=_collector_argv(
                        inputs,
                        source_id=source_id,
                        role=role,
                        write=write,
                    ),
                )
                _validate_collector_result(result, role=role, write=write)
                index += 1
            collection_results[role] = result

        postinstall._run_step(
            inputs=inputs,
            runner=active_runner,
            index=index,
            name="release-gate-canary-bootstrap",
            argv=postinstall._gate_argv(
                inputs,
                mode=CANARY_MODE,
                receipt=inputs.canary_gate_receipt,
                preproduction_capsule=inputs.preproduction_capsule,
            ),
        )
        index += 1
        _owned_json(inputs.canary_gate_receipt, artifact="canary_gate_receipt")
        candidate = _run_candidate_step(inputs, runner=active_runner, index=index)
        index += 1

        observed_before = _validate_reconciliation_state(
            inputs,
            reconciliation_observer(inputs),
            complete=current.get("state") == "steady_active",
        )
        postinstall._run_step(
            inputs=inputs,
            runner=active_runner,
            index=index,
            name="release-gate-production-bootstrap",
            argv=postinstall._gate_argv(
                inputs,
                mode=PRODUCTION_MODE,
                receipt=inputs.production_gate_receipt,
                preproduction_capsule=inputs.preproduction_capsule,
            ),
        )
        index += 1
        postinstall._require_gate_pair(
            inputs.production_gate_receipt,
            inputs.production_confirmation_capsule,
        )
        _confirmed, index = postinstall._run_activation_pair(
            inputs=inputs,
            runner=active_runner,
            index=index,
            name="activation-confirm",
            command="confirm",
            arguments=[
                "--confirmation-capsule",
                str(inputs.production_confirmation_capsule),
                *audit,
            ],
        )
        current = _activation_status(inputs, active_runner)
        if current.get("state") not in {"confirmed", "steady_active"}:
            raise CanaryFinalizationError("canary_finalize_confirmation_failed")

        action_results: dict[str, Mapping[str, Any]] = {}
        for action_index, entry in enumerate(inputs.reconciliation_entries, start=1):
            command = (
                "reconcile-shadow" if entry.action == "reconcile" else "defer-event"
            )
            applied, index = postinstall._run_activation_pair(
                inputs=inputs,
                runner=active_runner,
                index=index,
                name=f"reconciliation-{action_index:03d}-{entry.action}",
                command=command,
                arguments=[
                    "--epoch-id",
                    inputs.activation_epoch_id,
                    "--event-uid",
                    entry.event_uid,
                    *audit,
                ],
            )
            action_results[entry.event_uid] = applied["result"]
        observed_after = _validate_reconciliation_state(
            inputs, reconciliation_observer(inputs), complete=True
        )
        _steady, index = postinstall._run_activation_pair(
            inputs=inputs,
            runner=active_runner,
            index=index,
            name="activation-steady",
            command="transition-steady",
            arguments=[
                "--epoch-id",
                inputs.activation_epoch_id,
                "--active-release-binding",
                str(inputs.active_release_binding),
                "--live-env",
                str(inputs.live_env),
                "--release-id",
                inputs.release_id,
                "--bootstrap-epoch-id",
                inputs.bootstrap_epoch_id,
                *audit,
            ],
        )
        current = _activation_status(inputs, active_runner)
        if current.get("state") != "steady_active":
            raise CanaryFinalizationError("canary_finalize_steady_transition_failed")

        step_receipts = {
            path.name: _owned_json(path, artifact="step_receipt").sha256
            for path in sorted(inputs.journal_root.glob("[0-9][0-9]-*.json"))
        }
        gate_artifacts = {
            "canary_gate_receipt_sha256": _owned_json(
                inputs.canary_gate_receipt, artifact="canary_gate_receipt"
            ).sha256,
            "production_candidate_sha256": _owned_json(
                inputs.production_candidate, artifact="production_candidate"
            ).sha256,
            "production_gate_receipt_sha256": _owned_json(
                inputs.production_gate_receipt, artifact="production_gate_receipt"
            ).sha256,
            "production_confirmation_capsule_sha256": _owned_json(
                inputs.production_confirmation_capsule,
                artifact="production_confirmation_capsule",
            ).sha256,
        }
        receipt = {
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "ok": True,
            "authorization_decision": authorization_decision,
            "operator": operator.strip(),
            "reason": reason.strip(),
            "release_id": inputs.release_id,
            "bootstrap_epoch_id": inputs.bootstrap_epoch_id,
            "activation_epoch_id": inputs.activation_epoch_id,
            "activation_state": "steady_active",
            "real_canaries_completed": True,
            "collector_evidence": {
                role: {
                    "evidence_commit_id": value["evidence_commit_id"],
                    "evidence_manifest": value["evidence_manifest"],
                    "receipt_sha256": value["receipt_sha256"],
                }
                for role, value in sorted(collection_results.items())
            },
            "production_candidate": dict(candidate),
            "gate_artifacts": gate_artifacts,
            "reconciliation_plan_sha256": inputs.reconciliation_plan_sha256,
            "reconciliation_before": observed_before,
            "reconciliation_after": observed_after,
            "reconciliation_results": action_results,
            "step_receipts": step_receipts,
            "next_phase": "steady_runtime_canary_and_release_closeout",
        }
        cutover._publish_no_clobber(inputs.receipt_path, receipt)
        return receipt


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("schema")
    validate = commands.add_parser("validate-manifest")
    validate.add_argument("--manifest", type=Path, required=True)
    apply = commands.add_parser("apply")
    apply.add_argument("--manifest", type=Path, required=True)
    apply.add_argument("--authorization-decision", required=True)
    apply.add_argument("--operator", required=True)
    apply.add_argument("--reason", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "schema":
            result = {
                "schema_version": MANIFEST_SCHEMA_VERSION,
                "authorization_decision": AUTHORIZATION_DECISION,
                "canary_mode": CANARY_MODE,
                "production_mode": PRODUCTION_MODE,
                "creates_canaries": False,
                "cli_apply_supported": True,
            }
        else:
            inputs = load_manifest(args.manifest)
            if args.command == "validate-manifest":
                result = {
                    "schema_version": MANIFEST_SCHEMA_VERSION,
                    "ok": True,
                    "production_effects_executed": False,
                    "release_id": inputs.release_id,
                    "reconciliation_entry_count": len(
                        inputs.reconciliation_entries
                    ),
                }
            else:
                result = run_canary_finalization(
                    inputs,
                    authorization_decision=args.authorization_decision,
                    operator=args.operator,
                    reason=args.reason,
                )
    except (OSError, ValueError) as exc:
        code = getattr(exc, "code", "canary_finalize_failed")
        print(json.dumps({"ok": False, "code": code}, sort_keys=True))
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
