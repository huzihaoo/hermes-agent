#!/usr/bin/env python3
"""Admit fixed W18 gray samples through the activation-fenced production lane."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import subprocess
import sys
from typing import Any, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gateway.pnc_rca_control_store import (  # noqa: E402
    ActivationEpochError,
    MANUAL_TRIGGER_SCHEMA_VERSION,
    ManualRcaAdmissionError,
    ManualRcaTriggerRequest,
    RcaControlStore,
)
from gateway.pnc_rca_gray_samples import (  # noqa: E402
    C_TOPIC_CHAT_ID,
    C_TOPIC_FIXTURE_SCHEMA_VERSION,
    C_TOPIC_ISSUE_ID,
    C_TOPIC_TEXT,
    GRAY_SAMPLE_AUTOMATION_AUTHORITY_SCHEMA_VERSION,
    GRAY_SAMPLE_CONTRACTS,
    GRAY_SAMPLE_DAILY_STARTED_ATTEMPT_QUOTA,
    GRAY_SAMPLE_REQUESTER_ID,
    build_gray_sample_message_id,
    build_gray_sample_reason,
    canonical_json,
    gray_sample_issue_url,
    normalize_gray_sample_automation_authority,
    sample_contract,
    sample_contract_sha256,
    validate_gray_sample_automation_authorization,
)


RECEIPT_SCHEMA_VERSION = "pnc_rca_gray_sample_admission_receipt_v1"
STATUS_SCHEMA_VERSION = "pnc_rca_gray_sample_status_v2"
MAX_FIXTURE_BYTES = 256 * 1024
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_OPEN_ID_RE = re.compile(r"^ou_[A-Za-z0-9_-]{3,200}$")
_MESSAGE_ID_RE = re.compile(r"^om_[A-Za-z0-9_-]{3,200}$")


class GraySampleInitiatorError(RuntimeError):
    def __init__(self, code: str):
        self.code = str(code or "gray_sample_initiator_failed")[:160]
        super().__init__(self.code)


def _now(value: datetime | None = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise GraySampleInitiatorError("gray_sample_now_timezone_required")
    return current.astimezone(timezone.utc)


def _iso(value: datetime | None = None) -> str:
    return _now(value).isoformat()


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _read_json_bytes(path: Path, *, max_bytes: int) -> tuple[dict[str, Any], str]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise GraySampleInitiatorError("gray_sample_json_unreadable") from exc
    if not raw or len(raw) > max_bytes:
        raise GraySampleInitiatorError("gray_sample_json_size_invalid")
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise GraySampleInitiatorError("gray_sample_json_invalid") from exc
    if not isinstance(value, Mapping):
        raise GraySampleInitiatorError("gray_sample_json_object_required")
    return dict(value), _sha256_bytes(raw)


def _require_owner_only_regular_file(path: Path) -> None:
    if not path.is_absolute():
        raise GraySampleInitiatorError("gray_sample_fixture_path_not_absolute")
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise GraySampleInitiatorError("gray_sample_fixture_unavailable") from exc
    if not stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise GraySampleInitiatorError("gray_sample_fixture_not_regular")
    if metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) & 0o077:
        raise GraySampleInitiatorError("gray_sample_fixture_permissions_invalid")


def _load_automation_authorization(
    path: Path, *, expected_release_id: str, now: datetime
) -> tuple[dict[str, Any], str]:
    if not path.is_absolute():
        raise GraySampleInitiatorError("gray_sample_authorization_path_not_absolute")
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise GraySampleInitiatorError("gray_sample_authorization_unavailable") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise GraySampleInitiatorError("gray_sample_authorization_not_regular")
    authorization, authorization_sha = _read_json_bytes(
        path, max_bytes=MAX_FIXTURE_BYTES
    )
    sidecar = path.with_suffix(path.suffix + ".sha256")
    try:
        sidecar_value = sidecar.read_text(encoding="ascii").strip()
    except OSError as exc:
        raise GraySampleInitiatorError(
            "gray_sample_authorization_sidecar_missing"
        ) from exc
    if sidecar_value != f"{authorization_sha}  {path.name}":
        raise GraySampleInitiatorError(
            "gray_sample_authorization_sha256_mismatch"
        )
    try:
        normalized = validate_gray_sample_automation_authorization(
            authorization,
            expected_release_id=expected_release_id,
            now=now,
        )
    except ValueError as exc:
        raise GraySampleInitiatorError(str(exc)) from exc
    return normalized, authorization_sha


def _load_originator_fixture(
    path: Path, *, expected_originator: str
) -> tuple[dict[str, Any], str]:
    _require_owner_only_regular_file(path)
    fixture, fixture_sha = _read_json_bytes(path, max_bytes=MAX_FIXTURE_BYTES)
    required = {
        "schema_version",
        "canary_id",
        "status",
        "chat_id",
        "issue_id",
        "exact_text",
        "originator_identity",
        "message_id",
        "topic_id",
        "mention_entity",
        "mention_entity_verified",
        "official_readback",
        "authorization_sha256",
        "recorded_at",
    }
    if set(fixture) != required:
        raise GraySampleInitiatorError("gray_sample_fixture_schema_invalid")
    originator = str(fixture.get("originator_identity") or "").strip()
    if (
        fixture.get("schema_version") != C_TOPIC_FIXTURE_SCHEMA_VERSION
        or fixture.get("canary_id") != "C-TOPIC"
        or fixture.get("status") != "GREEN"
        or fixture.get("chat_id") != C_TOPIC_CHAT_ID
        or str(fixture.get("issue_id") or "") != C_TOPIC_ISSUE_ID
        or fixture.get("exact_text") != C_TOPIC_TEXT
        or fixture.get("mention_entity_verified") is not True
        or _OPEN_ID_RE.fullmatch(originator) is None
        or originator != expected_originator
    ):
        raise GraySampleInitiatorError("gray_sample_fixture_identity_mismatch")
    message_id = str(fixture.get("message_id") or "").strip()
    topic_id = str(fixture.get("topic_id") or "").strip()
    if (
        _MESSAGE_ID_RE.fullmatch(message_id) is None
        or topic_id != f"topic:{message_id}"
    ):
        raise GraySampleInitiatorError("gray_sample_fixture_message_identity_invalid")
    authorization_sha = str(fixture.get("authorization_sha256") or "").strip().lower()
    if _SHA256_RE.fullmatch(authorization_sha) is None or authorization_sha == "0" * 64:
        raise GraySampleInitiatorError("gray_sample_fixture_authorization_invalid")
    mention = fixture.get("mention_entity")
    if (
        not isinstance(mention, Mapping)
        or set(mention) != {"source", "target_open_id", "display_name"}
        or mention.get("source") != "feishu_message_entity"
        or _OPEN_ID_RE.fullmatch(str(mention.get("target_open_id") or "")) is None
        or not str(mention.get("display_name") or "").strip()
    ):
        raise GraySampleInitiatorError("gray_sample_fixture_mention_invalid")
    official = fixture.get("official_readback")
    if not isinstance(official, Mapping) or set(official) != {
        "source",
        "chat_id",
        "originator_identity",
        "message_id",
        "topic_id",
        "exact_text",
        "mention_target_open_id",
        "mention_entity_verified",
        "read_at",
    }:
        raise GraySampleInitiatorError("gray_sample_fixture_official_readback_invalid")
    if (
        official.get("source") != "feishu_official_api"
        or official.get("chat_id") != C_TOPIC_CHAT_ID
        or official.get("originator_identity") != originator
        or official.get("message_id") != message_id
        or official.get("topic_id") != topic_id
        or official.get("exact_text") != C_TOPIC_TEXT
        or official.get("mention_target_open_id") != mention.get("target_open_id")
        or official.get("mention_entity_verified") is not True
        or not str(official.get("read_at") or "").strip()
        or not str(fixture.get("recorded_at") or "").strip()
    ):
        raise GraySampleInitiatorError("gray_sample_fixture_official_readback_invalid")
    return fixture, fixture_sha


def _runtime_identity() -> tuple[str, str]:
    try:
        output = subprocess.check_output(
            ["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD^{commit}", "HEAD^{tree}"],
            text=True,
            stderr=subprocess.DEVNULL,
            timeout=5,
        ).splitlines()
    except (OSError, subprocess.SubprocessError) as exc:
        raise GraySampleInitiatorError(
            "gray_sample_runtime_identity_unavailable"
        ) from exc
    if len(output) != 2:
        raise GraySampleInitiatorError("gray_sample_runtime_identity_unavailable")
    return output[0].strip(), output[1].strip()


def _selected_samples(values: Sequence[str]) -> list[str]:
    selected = [str(value or "").strip().upper() for value in values]
    if not selected:
        raise GraySampleInitiatorError("gray_sample_selection_count_invalid")
    if len(set(selected)) != len(selected) or any(
        sample_id not in GRAY_SAMPLE_CONTRACTS for sample_id in selected
    ):
        raise GraySampleInitiatorError("gray_sample_selection_invalid")
    order = {sample_id: index for index, sample_id in enumerate(GRAY_SAMPLE_CONTRACTS)}
    if selected != sorted(selected, key=order.__getitem__):
        raise GraySampleInitiatorError("gray_sample_selection_order_invalid")
    return selected


def _automation_authority(
    *,
    release_id: str,
    sample_id: str,
    originator_identity: str,
    fixture_sha256: str,
    authorization_sha256: str,
) -> dict[str, str]:
    return normalize_gray_sample_automation_authority({
        "schema_version": GRAY_SAMPLE_AUTOMATION_AUTHORITY_SCHEMA_VERSION,
        "release_id": release_id,
        "sample_id": sample_id,
        "originator_identity": originator_identity,
        "originator_fixture_sha256": fixture_sha256,
        "authorization_sha256": authorization_sha256,
        "sample_contract_sha256": sample_contract_sha256(sample_id),
    })


def _request(authority: Mapping[str, Any]) -> ManualRcaTriggerRequest:
    normalized = normalize_gray_sample_automation_authority(authority)
    return ManualRcaTriggerRequest(
        schema_version=MANUAL_TRIGGER_SCHEMA_VERSION,
        issue_url=gray_sample_issue_url(normalized["sample_id"]),
        mode="rerun",
        reason=build_gray_sample_reason(normalized),
        platform="operator",
        chat_id="",
        thread_id="",
        message_id=build_gray_sample_message_id(normalized),
        requester_id=GRAY_SAMPLE_REQUESTER_ID,
    )


def _business_counts(db_path: Path) -> dict[str, int]:
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        counts: dict[str, int] = {}
        for table in (
            "rca_trigger_sources",
            "business_triggers",
            "rca_outbox",
            "rca_delivery_subscriptions",
        ):
            counts[table] = int(
                conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            )
        return counts
    finally:
        conn.close()


def _audit_admission(
    db_path: Path,
    *,
    source_id: str,
    authority: Mapping[str, Any],
    expected_epoch_id: str,
) -> dict[str, Any]:
    normalized = normalize_gray_sample_automation_authority(authority)
    request = _request(normalized)
    expected_source_sha, _normalized_source = (
        RcaControlStore._normalize_activation_source_identity(
            "manual",
            {
                "chat_id": "operator",
                "thread_id": "operator:issue-only",
                "requester_id": request.requester_id,
                "message_id": request.message_id,
                "issue_url": request.issue_url,
                "mode": request.mode,
                "automation_authority": normalized,
            },
        )
    )
    conn = sqlite3.connect(db_path, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            """
            SELECT s.platform, s.requester_id, s.message_id, s.payload_sha256,
                   b.work_item_id, b.generation, b.submission_key,
                   b.activation_epoch_id, b.activation_ledger_id,
                   o.status AS outbox_status,
                   o.activation_epoch_id AS outbox_epoch_id,
                   o.activation_ledger_id AS outbox_ledger_id,
                   al.decision AS activation_decision,
                   al.reason AS activation_reason,
                   al.source_identity_sha256, al.bound_at
              FROM rca_trigger_sources AS s
              JOIN rca_trigger_bindings AS rb ON rb.source_id=s.source_id
              JOIN business_triggers AS b
                ON b.business_key=rb.business_key AND b.generation=rb.generation
              JOIN rca_outbox AS o
                ON o.business_key=b.business_key AND o.generation=b.generation
              LEFT JOIN rca_activation_admission_ledger AS al
                ON al.epoch_id=o.activation_epoch_id
               AND al.ledger_id=o.activation_ledger_id
             WHERE s.source_id=?
            """,
            (source_id,),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        raise GraySampleInitiatorError("gray_sample_admission_audit_missing")
    expected_issue = str(sample_contract(normalized["sample_id"])["issue_id"])
    if (
        row["platform"] != "operator"
        or row["requester_id"] != GRAY_SAMPLE_REQUESTER_ID
        or row["message_id"] != request.message_id
        or str(row["work_item_id"]) != expected_issue
        or row["activation_epoch_id"] != expected_epoch_id
        or row["outbox_epoch_id"] != expected_epoch_id
        or not row["activation_ledger_id"]
        or row["activation_ledger_id"] != row["outbox_ledger_id"]
        or row["activation_decision"] not in {"admit", "join"}
        or row["source_identity_sha256"] != expected_source_sha
        or not row["bound_at"]
    ):
        raise GraySampleInitiatorError("gray_sample_admission_audit_mismatch")
    return {
        "platform": str(row["platform"]),
        "requester_id": str(row["requester_id"]),
        "message_id": str(row["message_id"]),
        "payload_sha256": str(row["payload_sha256"]),
        "work_item_id": str(row["work_item_id"]),
        "generation": int(row["generation"]),
        "submission_key": str(row["submission_key"]),
        "activation_epoch_id": str(row["activation_epoch_id"]),
        "activation_ledger_id": int(row["activation_ledger_id"]),
        "activation_source_identity_sha256": expected_source_sha,
        "activation_decision": str(row["activation_decision"]),
        "activation_reason": str(row["activation_reason"]),
        "outbox_status": str(row["outbox_status"]),
    }


def _atomic_write(path: Path, raw: bytes, *, replace: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    fd = os.open(tmp, flags, 0o600)
    try:
        with os.fdopen(fd, "wb", closefd=True) as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        if replace:
            os.replace(tmp, path)
        else:
            try:
                os.link(tmp, path)
            except FileExistsError as exc:
                raise GraySampleInitiatorError(
                    "gray_sample_receipt_already_exists"
                ) from exc
            tmp.unlink()
    except Exception:
        try:
            tmp.unlink()
        except OSError:
            pass
        raise


def _write_json(path: Path, value: Mapping[str, Any], *, replace: bool) -> str:
    raw = (canonical_json(value) + "\n").encode("utf-8")
    digest = _sha256_bytes(raw)
    _atomic_write(path, raw, replace=replace)
    sidecar = f"{digest}  {path.name}\n".encode("ascii")
    _atomic_write(path.with_suffix(path.suffix + ".sha256"), sidecar, replace=replace)
    return digest


def _load_existing_receipt(
    path: Path,
    *,
    release_id: str,
    sample_id: str,
    runtime_commit: str,
    runtime_tree: str,
    originator_identity: str,
    fixture_sha256: str,
    authorization_sha256: str,
) -> tuple[dict[str, Any], str] | None:
    if not path.exists():
        return None
    receipt, digest = _read_json_bytes(path, max_bytes=MAX_FIXTURE_BYTES)
    sidecar = path.with_suffix(path.suffix + ".sha256")
    try:
        sidecar_value = sidecar.read_text(encoding="ascii").strip()
    except OSError as exc:
        raise GraySampleInitiatorError("gray_sample_receipt_sidecar_missing") from exc
    if sidecar_value != f"{digest}  {path.name}":
        raise GraySampleInitiatorError("gray_sample_receipt_sha256_mismatch")
    if (
        receipt.get("schema_version") != RECEIPT_SCHEMA_VERSION
        or receipt.get("status") != "ADMITTED"
        or receipt.get("release_id") != release_id
        or receipt.get("sample", {}).get("sample_id") != sample_id
        or receipt.get("runtime", {}).get("commit") != runtime_commit
        or receipt.get("runtime", {}).get("tree") != runtime_tree
        or receipt.get("identity", {}).get("requester_id") != GRAY_SAMPLE_REQUESTER_ID
        or receipt.get("identity", {}).get("originator_identity") != originator_identity
        or receipt.get("identity", {}).get("originator_fixture_sha256")
        != fixture_sha256
        or receipt.get("identity", {}).get("authorization_sha256")
        != authorization_sha256
        or receipt.get("sample_contract_sha256") != sample_contract_sha256(sample_id)
    ):
        raise GraySampleInitiatorError("gray_sample_receipt_binding_mismatch")
    return receipt, digest


def _write_status(
    receipt_dir: Path,
    *,
    release_id: str,
    runtime_commit: str,
    runtime_tree: str,
    originator_identity: str,
    fixture_sha256: str,
    authorization_sha256: str,
    observed_at: str,
) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    admitted = 0
    for sample_id in GRAY_SAMPLE_CONTRACTS:
        path = receipt_dir / f"{sample_id}.json"
        item: dict[str, Any] = {"sample_id": sample_id, "status": "pending"}
        if path.exists():
            receipt, digest = _read_json_bytes(path, max_bytes=MAX_FIXTURE_BYTES)
            if (
                receipt.get("release_id") != release_id
                or receipt.get("identity", {}).get("originator_fixture_sha256")
                != fixture_sha256
            ):
                raise GraySampleInitiatorError("gray_sample_status_binding_mismatch")
            item.update({
                "status": "admitted",
                "receipt_path": str(path),
                "receipt_sha256": digest,
            })
            admitted += 1
        items.append(item)
    status = {
        "schema_version": STATUS_SCHEMA_VERSION,
        "status": "completed" if admitted == len(items) else "in_progress",
        "observed_at": observed_at,
        "release_id": release_id,
        "runtime": {"commit": runtime_commit, "tree": runtime_tree},
        "identity": {
            "requester_id": GRAY_SAMPLE_REQUESTER_ID,
            "originator_identity": originator_identity,
            "originator_fixture_sha256": fixture_sha256,
            "authorization_sha256": authorization_sha256,
        },
        "lane": "production",
        "activation_required": True,
        "daily_started_attempt_quota": GRAY_SAMPLE_DAILY_STARTED_ATTEMPT_QUOTA,
        "summary": {"admitted": admitted, "total": len(items)},
        "items": items,
    }
    _write_json(receipt_dir / "status.json", status, replace=True)
    return status


def run(args: argparse.Namespace, *, now: datetime | None = None) -> dict[str, Any]:
    if args.apply is not True:
        raise GraySampleInitiatorError("gray_sample_apply_required")
    control_db = Path(args.control_db)
    receipt_dir = Path(args.receipt_dir)
    fixture_path = Path(args.originator_fixture)
    authorization_path = Path(args.authorization)
    if not control_db.is_absolute() or not receipt_dir.is_absolute():
        raise GraySampleInitiatorError("gray_sample_path_not_absolute")
    selected = _selected_samples(args.sample_id)
    runtime_commit, runtime_tree = _runtime_identity()
    if (
        runtime_commit != str(args.expected_runtime_commit or "").strip()
        or runtime_tree != str(args.expected_runtime_tree or "").strip()
    ):
        raise GraySampleInitiatorError("gray_sample_runtime_identity_mismatch")
    originator = str(args.originator_identity or "").strip()
    if _OPEN_ID_RE.fullmatch(originator) is None:
        raise GraySampleInitiatorError("gray_sample_originator_identity_invalid")
    current = _now(now)
    release_id = str(args.release_id or "").strip()
    _authorization, authorization_sha = _load_automation_authorization(
        authorization_path,
        expected_release_id=release_id,
        now=current,
    )
    fixture, fixture_sha = _load_originator_fixture(
        fixture_path, expected_originator=originator
    )
    if str(fixture["authorization_sha256"]) != authorization_sha:
        raise GraySampleInitiatorError(
            "gray_sample_fixture_authorization_sha256_mismatch"
        )
    store = RcaControlStore(control_db, require_current=True)
    epoch = store.activation_epoch()
    if epoch is None:
        raise GraySampleInitiatorError("gray_sample_activation_epoch_missing")
    if str(epoch.get("state") or "") != "steady_active":
        raise GraySampleInitiatorError("gray_sample_activation_epoch_not_steady")
    epoch_id = str(epoch.get("epoch_id") or "")

    authorities = {
        sample_id: _automation_authority(
            release_id=release_id,
            sample_id=sample_id,
            originator_identity=originator,
            fixture_sha256=fixture_sha,
            authorization_sha256=authorization_sha,
        )
        for sample_id in selected
    }
    results: list[dict[str, Any]] = []
    for sample_id in selected:
        receipt_path = receipt_dir / f"{sample_id}.json"
        existing = _load_existing_receipt(
            receipt_path,
            release_id=release_id,
            sample_id=sample_id,
            runtime_commit=runtime_commit,
            runtime_tree=runtime_tree,
            originator_identity=originator,
            fixture_sha256=fixture_sha,
            authorization_sha256=authorization_sha,
        )
        authority = authorities[sample_id]
        if existing is not None:
            receipt, receipt_sha = existing
            audit = _audit_admission(
                control_db,
                source_id=str(receipt["admission"]["source_id"]),
                authority=authority,
                expected_epoch_id=epoch_id,
            )
            results.append({
                "sample_id": sample_id,
                "status": "existing_receipt_verified",
                "receipt_path": str(receipt_path),
                "receipt_sha256": receipt_sha,
                "submission_key": audit["submission_key"],
            })
            continue

        before = _business_counts(control_db)
        admitted = store.admit_manual_trigger(
            _request(authority),
            allowed_chat_ids=set(),
            submit_enabled=True,
            operator_authorized=True,
            activation_required=True,
            automation_authority=authority,
            now=current,
        )
        audit = _audit_admission(
            control_db,
            source_id=admitted.source_id,
            authority=authority,
            expected_epoch_id=epoch_id,
        )
        after = _business_counts(control_db)
        contract = sample_contract(sample_id)
        receipt = {
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "status": "ADMITTED",
            "observed_at": _iso(current),
            "release_id": authority["release_id"],
            "runtime": {"commit": runtime_commit, "tree": runtime_tree},
            "lane": "production",
            "activation_required": True,
            "identity": {
                "requester_id": GRAY_SAMPLE_REQUESTER_ID,
                "originator_identity": originator,
                "originator_fixture_path": str(fixture_path),
                "originator_fixture_sha256": fixture_sha,
                "authorization_path": str(authorization_path),
                "authorization_sha256": authorization_sha,
                "activation_source_identity_sha256": audit[
                    "activation_source_identity_sha256"
                ],
            },
            "sample": contract,
            "sample_contract_sha256": authority["sample_contract_sha256"],
            "request": {
                "platform": "operator",
                "mode": "rerun",
                "message_id": build_gray_sample_message_id(authority),
                "reason_sha256": _sha256_bytes(
                    build_gray_sample_reason(authority).encode("utf-8")
                ),
            },
            "admission": {**admitted.to_dict(), "audit": audit},
            "db_counts_before": before,
            "db_counts_after": after,
            "external_write_observed_by_initiator": False,
            "comment_budget_verification_pending_delivery_receipt": True,
        }
        receipt_sha = _write_json(receipt_path, receipt, replace=False)
        results.append({
            "sample_id": sample_id,
            "status": "admitted",
            "receipt_path": str(receipt_path),
            "receipt_sha256": receipt_sha,
            "submission_key": admitted.submission_key,
        })

    status = _write_status(
        receipt_dir,
        release_id=release_id,
        runtime_commit=runtime_commit,
        runtime_tree=runtime_tree,
        originator_identity=originator,
        fixture_sha256=fixture_sha,
        authorization_sha256=authorization_sha,
        observed_at=_iso(current),
    )
    return {
        "ok": True,
        "status": status["status"],
        "release_id": status["release_id"],
        "originator_fixture_sha256": fixture_sha,
        "results": results,
        "status_path": str(receipt_dir / "status.json"),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--control-db", required=True)
    parser.add_argument("--originator-fixture", required=True)
    parser.add_argument("--authorization", required=True)
    parser.add_argument("--originator-identity", required=True)
    parser.add_argument("--receipt-dir", required=True)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--expected-runtime-commit", required=True)
    parser.add_argument("--expected-runtime-tree", required=True)
    parser.add_argument("--sample-id", action="append", required=True)
    parser.add_argument("--apply", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    try:
        result = run(_parser().parse_args(argv))
        print(canonical_json(result))
        return 0
    except (
        ActivationEpochError,
        GraySampleInitiatorError,
        ManualRcaAdmissionError,
        OSError,
        sqlite3.Error,
        ValueError,
    ) as exc:
        code = getattr(exc, "code", str(exc) or "gray_sample_initiator_failed")
        print(canonical_json({"ok": False, "error_code": str(code)}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
