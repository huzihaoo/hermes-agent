"""Provider-owned authorization for every G1Q3 RCA external write.

Claims only transport immutable identifiers.  They never carry executable
callbacks and never grant permission by themselves: the physical provider
reopens the canonical control DB and revalidates the current epoch, ledger,
operation, and destination immediately before each external call.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import re
from typing import Any, Iterator, Mapping

from gateway.pnc_rca_write_fence import (
    ExternalWriteFenceError,
    validate_write_fence,
)


PROVIDER_WRITE_CLAIM_SCHEMA_VERSION = "pnc_rca_provider_write_claim_v1"
_MANUAL_KIND = "manual_admission"
_WRITE_FENCE_KIND = "write_fence"
_HISTORICAL_EPOCH_KIND = "historical_epoch"
_CLAIM_FIELDS = frozenset({"schema_version", "authority_kind", "authority"})
_MANUAL_FIELDS = frozenset({"admission", "source_identity"})
_WRITE_FENCE_FIELDS = frozenset({"write_fence"})
_HISTORICAL_EPOCH_FIELDS = frozenset(
    {
        "epoch_id",
        "effect_key",
        "delivery_id",
        "lease_token",
        "lease_fence",
        "operations",
        "issue_target",
        "chat_id",
        "thread_id",
        "submission_key",
    }
)
_HISTORICAL_ALLOWED_OPERATIONS = frozenset(
    {
        "feishu_issue_comment",
        "feishu_issue_field_update",
        "feishu_thread_reply",
    }
)
_ISSUE_OPERATIONS = frozenset(
    {
        "feishu_issue_comment",
        "feishu_issue_field_update",
    }
)
_FEISHU_CHAT_OPERATIONS = frozenset(
    {
        "feishu_thread_reply",
        "feishu_card_create",
        "feishu_card_patch",
        "feishu_attachment_upload",
        "internal_alert",
    }
)
_ISSUE_TARGET_RE = re.compile(
    r"^https://project\.feishu\.cn/(?P<project_key>[^/?#]+)/issue/detail/"
    r"(?P<work_item_id>\d+)(?:[/?#]|$)"
)


@dataclass(frozen=True, slots=True)
class RcaProviderWriteClaim:
    """Canonical JSON envelope whose contents still require live DB proof."""

    canonical_json: str

    def payload(self) -> dict[str, Any]:
        try:
            value = json.loads(self.canonical_json)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ExternalWriteFenceError(
                "external_write_provider_claim_schema_invalid"
            ) from exc
        if not isinstance(value, dict) or set(value) != _CLAIM_FIELDS:
            raise ExternalWriteFenceError(
                "external_write_provider_claim_schema_invalid"
            )
        if value.get("schema_version") != PROVIDER_WRITE_CLAIM_SCHEMA_VERSION:
            raise ExternalWriteFenceError(
                "external_write_provider_claim_schema_invalid"
            )
        kind = value.get("authority_kind")
        authority = value.get("authority")
        if not isinstance(authority, dict):
            raise ExternalWriteFenceError(
                "external_write_provider_claim_schema_invalid"
            )
        if kind == _MANUAL_KIND:
            if set(authority) != _MANUAL_FIELDS:
                raise ExternalWriteFenceError(
                    "external_write_provider_claim_schema_invalid"
                )
        elif kind == _WRITE_FENCE_KIND:
            if set(authority) != _WRITE_FENCE_FIELDS:
                raise ExternalWriteFenceError(
                    "external_write_provider_claim_schema_invalid"
                )
        elif kind == _HISTORICAL_EPOCH_KIND:
            if set(authority) != _HISTORICAL_EPOCH_FIELDS:
                raise ExternalWriteFenceError(
                    "external_write_provider_claim_schema_invalid"
                )
        else:
            raise ExternalWriteFenceError(
                "external_write_provider_claim_schema_invalid"
            )
        return value


_PROVIDER_WRITE_CLAIM = ContextVar(
    "pnc_rca_provider_write_claim",
    default=None,
)


@contextmanager
def bound_provider_write_claim(
    claim: RcaProviderWriteClaim,
) -> Iterator[None]:
    """Bind one immutable claim across every provider implementation layer."""

    if type(claim) is not RcaProviderWriteClaim:
        raise ExternalWriteFenceError("external_write_provider_claim_invalid")
    token = _PROVIDER_WRITE_CLAIM.set(claim)
    try:
        yield
    finally:
        _PROVIDER_WRITE_CLAIM.reset(token)


def current_provider_write_claim() -> RcaProviderWriteClaim:
    """Return the currently bound claim or fail closed before provider I/O."""

    claim = _PROVIDER_WRITE_CLAIM.get()
    if type(claim) is not RcaProviderWriteClaim:
        raise ExternalWriteFenceError("external_write_provider_claim_missing")
    return claim


def require_provider_write_claim(
    *,
    operation: str,
    chat_id: str = "",
    thread_id: str = "",
    reply_to_message_id: str = "",
    issue_project_key: str = "",
    issue_work_item_id: str = "",
) -> dict[str, Any]:
    """Reopen canonical authority for the claim bound to this execution path."""

    return revalidate_provider_write_claim(
        current_provider_write_claim(),
        operation=operation,
        chat_id=chat_id,
        thread_id=thread_id,
        reply_to_message_id=reply_to_message_id,
        issue_project_key=issue_project_key,
        issue_work_item_id=issue_work_item_id,
    )


def _claim(kind: str, authority: Mapping[str, Any]) -> RcaProviderWriteClaim:
    try:
        encoded = json.dumps(
            {
                "schema_version": PROVIDER_WRITE_CLAIM_SCHEMA_VERSION,
                "authority_kind": kind,
                "authority": dict(authority),
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise ExternalWriteFenceError(
            "external_write_provider_claim_schema_invalid"
        ) from exc
    result = RcaProviderWriteClaim(encoded)
    result.payload()
    return result


def build_manual_provider_write_claim(
    admission: Mapping[str, Any],
    source_identity: Mapping[str, Any],
) -> RcaProviderWriteClaim:
    return _claim(
        _MANUAL_KIND,
        {
            "admission": dict(admission),
            "source_identity": dict(source_identity),
        },
    )


def build_write_fence_provider_claim(
    write_fence: Mapping[str, Any],
) -> RcaProviderWriteClaim:
    return _claim(_WRITE_FENCE_KIND, {"write_fence": dict(write_fence)})


def build_historical_epoch_provider_claim(
    *,
    epoch_id: str,
    effect_key: str,
    delivery_id: str,
    lease_token: str,
    lease_fence: int,
    operations: Any,
    issue_target: str,
    chat_id: str = "",
    thread_id: str = "",
    submission_key: str,
) -> RcaProviderWriteClaim:
    if isinstance(operations, (str, bytes)):
        raise ExternalWriteFenceError("external_write_provider_claim_schema_invalid")
    try:
        normalized_operations = {
            str(item or "").strip() for item in operations if str(item or "").strip()
        }
    except TypeError as exc:
        raise ExternalWriteFenceError(
            "external_write_provider_claim_schema_invalid"
        ) from exc
    if not normalized_operations or not normalized_operations.issubset(
        _HISTORICAL_ALLOWED_OPERATIONS
    ):
        raise ExternalWriteFenceError("external_write_provider_claim_schema_invalid")
    return _claim(
        _HISTORICAL_EPOCH_KIND,
        {
            "epoch_id": str(epoch_id or "").strip(),
            "effect_key": str(effect_key or "").strip(),
            "delivery_id": str(delivery_id or "").strip(),
            "lease_token": str(lease_token or "").strip(),
            "lease_fence": lease_fence,
            "operations": sorted(normalized_operations),
            "issue_target": str(issue_target or "").strip(),
            "chat_id": str(chat_id or "").strip(),
            "thread_id": str(thread_id or "").strip(),
            "submission_key": str(submission_key or "").strip(),
        },
    )


def _canonical_store():
    from gateway.pnc_rca_control_store import RcaControlStore
    from gateway.run import _g1q3_rca_control_db_path

    return RcaControlStore(_g1q3_rca_control_db_path(), require_current=True)


def write_fence_claim_for_submission(
    submission_key: str,
) -> RcaProviderWriteClaim | None:
    """Load a canonical claim for an existing RCA submission, if one exists."""

    key = str(submission_key or "").strip()
    if not key:
        return None
    store = _canonical_store()
    conn = store._connect()
    try:
        rows = conn.execute(
            """
            SELECT admission_snapshot_json
              FROM rca_admission_snapshots
             WHERE submission_key = ?
             ORDER BY generation DESC
             LIMIT 2
            """,
            (key,),
        ).fetchall()
    finally:
        conn.close()
    if not rows:
        return None
    try:
        snapshot = json.loads(str(rows[0]["admission_snapshot_json"]))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ExternalWriteFenceError(
            "external_write_provider_claim_schema_invalid"
        ) from exc
    fence = snapshot.get("write_fence") if isinstance(snapshot, dict) else None
    if not isinstance(fence, Mapping) or fence.get("state") != "issued":
        raise ExternalWriteFenceError("external_write_fence_missing")
    return build_write_fence_provider_claim(fence)


def _issue_identity(issue_target: str) -> tuple[str, str]:
    match = _ISSUE_TARGET_RE.match(str(issue_target or "").strip())
    if match is None:
        return "", ""
    return match.group("project_key"), match.group("work_item_id")


def _historical_effect_binding(
    store: Any,
    authority: Mapping[str, Any],
) -> dict[str, Any]:
    """Reopen the exact live delivery lease backing a pre-W3 effect."""

    effect_key = str(authority.get("effect_key") or "").strip()
    delivery_id = str(authority.get("delivery_id") or "").strip()
    lease_token = str(authority.get("lease_token") or "").strip()
    lease_fence = authority.get("lease_fence")
    if (
        not effect_key
        or not delivery_id
        or not lease_token
        or isinstance(lease_fence, bool)
        or not isinstance(lease_fence, int)
        or lease_fence < 1
    ):
        raise ExternalWriteFenceError("external_write_provider_claim_schema_invalid")
    conn = store._connect()
    try:
        row = conn.execute(
            """
            SELECT e.effect_key, e.delivery_id, e.effect_kind, e.target_key,
                   e.payload_json, e.status, e.write_phase, e.lease_token,
                   e.lease_expires_at, e.fence,
                   j.issue_url, j.work_item_id, j.submission_key
              FROM rca_delivery_effects AS e
              JOIN rca_delivery_jobs AS j ON j.delivery_id = e.delivery_id
             WHERE e.effect_key = ? AND e.delivery_id = ?
               AND e.lease_token = ? AND e.fence = ?
               AND e.status = 'claimed' AND e.write_phase = 'write_started'
            """,
            (effect_key, delivery_id, lease_token, lease_fence),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        raise ExternalWriteFenceError("external_write_fence_operation_denied")
    try:
        expires_at = datetime.fromisoformat(str(row["lease_expires_at"] or ""))
        payload = json.loads(str(row["payload_json"] or ""))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ExternalWriteFenceError(
            "external_write_provider_claim_schema_invalid"
        ) from exc
    if (
        expires_at.tzinfo is None
        or expires_at.utcoffset() is None
        or expires_at.astimezone(timezone.utc) <= datetime.now(timezone.utc)
    ):
        raise ExternalWriteFenceError("external_write_fence_operation_denied")
    if not isinstance(payload, Mapping):
        raise ExternalWriteFenceError("external_write_provider_claim_schema_invalid")
    return {**dict(row), "payload": dict(payload)}


def revalidate_provider_write_claim(
    claim: RcaProviderWriteClaim,
    *,
    operation: str,
    chat_id: str = "",
    thread_id: str = "",
    reply_to_message_id: str = "",
    issue_project_key: str = "",
    issue_work_item_id: str = "",
) -> dict[str, Any]:
    """Revalidate one claim at the physical write boundary."""

    if type(claim) is not RcaProviderWriteClaim:
        raise ExternalWriteFenceError("external_write_provider_claim_invalid")
    value = claim.payload()
    kind = str(value["authority_kind"])
    authority = value["authority"]
    op = str(operation or "").strip()
    observed_chat = str(chat_id or "").strip()
    observed_thread = str(thread_id or "").strip()
    observed_reply = str(reply_to_message_id or "").strip()
    observed_project = str(issue_project_key or "").strip()
    observed_work_item = str(issue_work_item_id or "").strip()
    if not op:
        raise ExternalWriteFenceError("external_write_fence_operation_denied")
    store = _canonical_store()

    if kind == _MANUAL_KIND:
        if op != "feishu_manual_reply" or not observed_reply:
            raise ExternalWriteFenceError(
                "external_write_fence_operation_denied"
            )
        source = authority["source_identity"]
        if not isinstance(source, Mapping):
            raise ExternalWriteFenceError(
                "external_write_provider_claim_schema_invalid"
            )
        live = store.validate_manual_external_write_admission(
            authority["admission"],
            expected_chat_id=str(source.get("chat_id") or ""),
            expected_thread_id=str(source.get("thread_id") or ""),
            expected_message_id=str(source.get("message_id") or ""),
            expected_requester_id=str(source.get("requester_id") or ""),
        )
        if (
            observed_chat != str(live.get("chat_id") or "").strip()
            or observed_thread != str(live.get("thread_id") or "").strip()
            or observed_reply != str(live.get("message_id") or "").strip()
        ):
            raise ExternalWriteFenceError(
                "external_write_fence_target_mismatch"
            )
        return {"authority_kind": kind, **dict(live)}

    if kind == _HISTORICAL_EPOCH_KIND:
        from gateway.pnc_rca_write_fence import require_resident_activation_epoch

        live = require_resident_activation_epoch(store)
        if (
            op not in authority.get("operations", [])
            or str(live.get("epoch_id") or "").strip()
            != str(authority.get("epoch_id") or "").strip()
        ):
            raise ExternalWriteFenceError(
                "external_write_fence_epoch_not_current"
            )
        effect = _historical_effect_binding(store, authority)
        effect_kind = str(effect.get("effect_kind") or "").strip()
        expected_operations = (
            {"feishu_thread_reply"}
            if effect_kind == "feishu_thread_reply"
            else (
                {"feishu_issue_comment", "feishu_issue_field_update"}
                if effect_kind in {"feishu_issue_comment", "feishu_field_update"}
                else set()
            )
        )
        if set(authority.get("operations") or []) != expected_operations:
            raise ExternalWriteFenceError("external_write_fence_operation_denied")
        payload = effect["payload"]
        expected_chat = (
            str(payload.get("chat_id") or "").strip()
            if effect_kind == "feishu_thread_reply"
            else ""
        )
        expected_thread = (
            str(payload.get("thread_id") or "").strip()
            if effect_kind == "feishu_thread_reply"
            else ""
        )
        expected_issue_target = str(effect.get("issue_url") or "").strip()
        expected_issue_project, expected_issue_id = _issue_identity(
            expected_issue_target
        )
        if (
            str(authority.get("issue_target") or "").strip()
            != expected_issue_target
            or str(authority.get("chat_id") or "").strip() != expected_chat
            or str(authority.get("thread_id") or "").strip() != expected_thread
            or str(authority.get("submission_key") or "").strip()
            != str(effect.get("submission_key") or "").strip()
        ):
            raise ExternalWriteFenceError("external_write_fence_target_mismatch")
        if op == "feishu_thread_reply":
            if (
                not expected_chat
                or not expected_thread
                or observed_chat != expected_chat
                or observed_thread != expected_thread
            ):
                raise ExternalWriteFenceError("external_write_fence_target_mismatch")
        elif op in _ISSUE_OPERATIONS:
            if (
                not expected_issue_project
                or not expected_issue_id
                or observed_work_item != expected_issue_id
                or (observed_project and observed_project != expected_issue_project)
            ):
                raise ExternalWriteFenceError("external_write_fence_target_mismatch")
        else:
            raise ExternalWriteFenceError("external_write_fence_operation_denied")
        return {
            "authority_kind": kind,
            **dict(live),
            "issue_target": expected_issue_target,
            "chat_id": expected_chat,
            "thread_target": expected_thread,
            "submission_key": str(authority.get("submission_key") or ""),
        }

    fence = authority["write_fence"]
    if not isinstance(fence, Mapping):
        raise ExternalWriteFenceError(
            "external_write_provider_claim_schema_invalid"
        )
    live = store.validate_external_write_fence_binding(fence)
    live_chat = str(live.get("chat_id") or "").strip()
    live_thread = str(live.get("thread_target") or "").strip()
    if op in _FEISHU_CHAT_OPERATIONS:
        if not live_chat or observed_chat != live_chat:
            raise ExternalWriteFenceError("external_write_fence_target_mismatch")
    if op == "feishu_thread_reply":
        if not live_thread or observed_thread != live_thread:
            raise ExternalWriteFenceError("external_write_fence_target_mismatch")
    elif op in {"feishu_card_create", "feishu_card_patch", "feishu_attachment_upload"}:
        # Callback responses may not expose the originating topic anchor.  When
        # a physical sender does provide one, it must still match the canonical
        # target; the chat binding is mandatory in either case.
        if observed_thread and observed_thread != live_thread:
            raise ExternalWriteFenceError("external_write_fence_target_mismatch")
    if op == "feishu_thread_reply":
        authorization_target = live_thread
    elif op in {
        "feishu_issue_comment",
        "feishu_issue_field_update",
        "feishu_card_create",
        "feishu_card_patch",
        "feishu_attachment_upload",
    }:
        authorization_target = str(live.get("issue_target") or "").strip()
    elif op == "internal_alert":
        authorization_target = str(live.get("submission_key") or "").strip()
    else:
        raise ExternalWriteFenceError("external_write_fence_operation_denied")
    expected_issue_project, expected_issue_id = _issue_identity(
        str(live.get("issue_target") or "")
    )
    if op in _ISSUE_OPERATIONS:
        if (
            not expected_issue_project
            or not expected_issue_id
            or observed_work_item != expected_issue_id
            or (observed_project and observed_project != expected_issue_project)
        ):
            raise ExternalWriteFenceError("external_write_fence_target_mismatch")
    elif observed_work_item and observed_work_item != expected_issue_id:
        raise ExternalWriteFenceError("external_write_fence_target_mismatch")
    validate_write_fence(
        fence,
        snapshot_core_sha256_value=str(
            fence.get("admission_snapshot_sha256") or ""
        ),
        operation=op,
        target=authorization_target,
        expected_epoch_id=str(live["epoch_id"]),
        expected_ledger_id=int(live["ledger_id"]),
        expected_business_key=str(live["business_key"]),
        expected_submission_key=str(live["submission_key"]),
        expected_generation=int(live["generation"]),
        expected_issue_target=str(live.get("issue_target") or ""),
        expected_thread_target=live_thread or None,
        expected_target_set_sha256=str(live["target_set_sha256"]),
    )
    return {"authority_kind": kind, **dict(live)}


__all__ = [
    "PROVIDER_WRITE_CLAIM_SCHEMA_VERSION",
    "RcaProviderWriteClaim",
    "bound_provider_write_claim",
    "build_manual_provider_write_claim",
    "build_historical_epoch_provider_claim",
    "build_write_fence_provider_claim",
    "current_provider_write_claim",
    "require_provider_write_claim",
    "revalidate_provider_write_claim",
    "write_fence_claim_for_submission",
]
