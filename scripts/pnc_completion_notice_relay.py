#!/usr/bin/env python3
"""Relay pending PNC completion notices back to Feishu topics.

The VM sync bridge writes completion_notice objects into delivery sidecars. This
script is the narrow idempotent relay: by default it only previews pending
notices; with --send it sends each pending notice to its original Feishu topic
and marks the sidecar sent/failed.
"""
from __future__ import annotations

import argparse
import fcntl
import asyncio
import hashlib
import json
import math
import os
import re
import stat
import sqlite3
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping
from urllib.parse import urlsplit, urlunsplit

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from hermes_cli.config import get_hermes_home, reload_env  # noqa: E402
from scripts.pnc_vm_task_sync import (  # noqa: E402
    DEFAULT_CHAT_IDS,
    G1Q3_RCA_CHAT_ID,
)
from scripts.pnc_foxglove_delivery import (  # noqa: E402
    canonical_publication_origin,
    canonical_report_url_from_vm_path,
    foxglove_delivery_fields,
    validate_canonical_report_url,
    validate_foxglove_url,
)
from scripts.vm_task_state_bridge import _atomic_write_json  # noqa: E402
from gateway.feishu_task_card import (  # noqa: E402
    contains_internal_rca_html_reference,
    has_rca_delivery_provenance,
    render_task_card,
    stable_render_hash,
)
from gateway.feishu_task_confirm import (  # noqa: E402
    RCA_CANDIDATE_REVIEW_PRESET,
    RCA_CANDIDATE_REVIEW_SCHEMA_VERSION,
    add_rca_candidate_conclusion_confirm,
)
from gateway.pnc_rca_delivery_contract import (  # noqa: E402
    DELIVERY_TARGET_SCHEMA_VERSION,
    build_card_patch_effect,
    card_patch_payload_has_exact_submission_marker,
)
from gateway.pnc_rca_artifacts import local_candidates_for_vm_path  # noqa: E402
from gateway.pnc_rca_write_fence import (  # noqa: E402
    ExternalWriteFenceError,
    snapshot_core_sha256,
    validate_write_fence,
    validate_write_fence_source_binding,
)
from gateway.pnc_rca_provider_fence import (  # noqa: E402
    RcaProviderWriteClaim,
    build_write_fence_provider_claim,
    revalidate_provider_write_claim,
)
from gateway.feishu_mention import (  # noqa: E402
    build_at_mention,
    build_need_input_notify_text,
    compute_notify_key,
    resolve_display_name,
    resolve_originator_open_id,
)
from tools.send_message_tool import _parse_target_ref, send_message_tool  # noqa: E402
from scripts.pnc_g1q3_truth import (  # noqa: E402
    gate_is_green,
    parsed_l2_assets_present,
    reconcile_report_truth,
)
from scripts.pnc_status_projection import (  # noqa: E402
    REMOTE_REFERENCE_GUIDANCE,
    derive_presentation,
    sanitize_milestones,
)
from scripts import pnc_fault_taxonomy  # noqa: E402
from scripts.pnc_aux_runtime_health import (  # noqa: E402
    build_process_runtime_evidence,
    write_owner_health,
)


def _reload_env_for_current_mode() -> None:
    from gateway.record_only.runtime import record_only_enabled

    if not record_only_enabled():
        reload_env()


_reload_env_for_current_mode()


_L4_EVENT_EPOCH_MAX = 4_102_444_800.0  # 2100-01-01T00:00:00Z


def _now_epoch() -> float:
    """Return the sealed L4 event clock, otherwise the live UTC epoch."""

    if (
        os.getenv("HERMES_OUTBOUND_MODE", "").strip().lower() == "record-only"
        and os.getenv("HERMES_L4_SANDBOX_ACTIVE", "").strip() == "1"
    ):
        raw = os.getenv("HERMES_L4_EVENT_EPOCH", "").strip()
        if not raw:
            raise RuntimeError("record-only L4 sandbox requires HERMES_L4_EVENT_EPOCH")
        try:
            value = float(raw)
        except ValueError as exc:
            raise RuntimeError("HERMES_L4_EVENT_EPOCH must be a finite UTC epoch") from exc
        if not math.isfinite(value) or not 0.0 <= value <= _L4_EVENT_EPOCH_MAX:
            raise RuntimeError("HERMES_L4_EVENT_EPOCH is outside the accepted UTC epoch range")
        return value
    return time.time()


REMOTE_REFERENCE_COMPATIBILITY_NOTE = (
    "问题数据地址中的历史 MDI 形状仅用于提取 event/clip 引用；"
    "系统远程读取数据，不执行 MDI 下载。"
)
RCA_EXECUTION_REQUEST_JSON_BEGIN = (
    "<!-- G1Q3_RCA_EXECUTION_REQUEST_JSON:BEGIN -->"
)
RCA_EXECUTION_REQUEST_JSON_END = "<!-- G1Q3_RCA_EXECUTION_REQUEST_JSON:END -->"
RCA_LEGACY_EXECUTION_REQUEST_HEADING = "## RcaExecutionRequest JSON"
RCA_GOAL_MAX_BYTES = 4 * 1024 * 1024
RCA_EXECUTION_REQUEST_MAX_BYTES = 2 * 1024 * 1024
LEGACY_MDI_COMMAND_RE = re.compile(
    r"\bmdi\s+(?:download|refresh2?|clip|event)\b", re.I
)

DEFAULT_MAX_ATTEMPTS = int(os.getenv("PNC_COMPLETION_NOTICE_MAX_ATTEMPTS", "3") or "3")
FAILED_ALERT_PREFIX = "[PNC completion notice relay 告警]"
DEFAULT_WATCH_POLL_SECONDS = float(os.getenv("PNC_COMPLETION_NOTICE_WATCH_POLL_SECONDS", "1") or "1")
DEFAULT_WATCH_FULL_SCAN_SECONDS = int(os.getenv("PNC_COMPLETION_NOTICE_FULL_SCAN_SECONDS", "120") or "120")
DEFAULT_CARD_UPDATE_THROTTLE_SECONDS = float(os.getenv("PNC_TASK_CARD_UPDATE_THROTTLE_SECONDS", "5") or "5")
# Production fuse: card fallback text is noisy and can flood historical topics if
# a card PATCH starts failing.  Default OFF; enable only in explicit/manual
# recovery by setting PNC_TASK_CARD_MAX_FALLBACKS_PER_LOOP > 0.
DEFAULT_MAX_CARD_FALLBACKS_PER_LOOP = max(0, int(os.getenv("PNC_TASK_CARD_MAX_FALLBACKS_PER_LOOP", "0") or "0"))
DEFAULT_WATCH_CANARY_LOOPS = max(0, int(os.getenv("PNC_RELAY_WATCH_CANARY_LOOPS", "3") or "3"))
COMPLETION_RELAY_HEALTH_SCHEMA_VERSION = "pnc_completion_notice_relay_health_v1"
COMPLETION_RELAY_SERVICE_LABEL = "local.pnc.completion-notice-relay"
# Full-scan pre-filter: skip _load_json on sidecars whose mtime is older than this
# act-window (7d, aligned with the archive threshold). Never below retry window +
# margin, so a file the relay could still retry is never dropped.
SCAN_ACT_WINDOW_SECONDS = int(os.getenv("PNC_COMPLETION_NOTICE_SCAN_WINDOW_SECONDS", str(7 * 24 * 3600)) or str(7 * 24 * 3600))
SCAN_ACT_WINDOW_MARGIN_SECONDS = int(os.getenv("PNC_COMPLETION_NOTICE_SCAN_WINDOW_MARGIN_SECONDS", "3600") or "3600")
# Bounded concurrency for the out-bound send loop: different task_ids send in
# parallel up to this many in-flight; same task_id stays strictly serial.
RELAY_SEND_CONCURRENCY = max(1, int(os.getenv("PNC_RELAY_SEND_CONCURRENCY", "5") or "5"))
# C8 safety gate: watcher/full-scan may only auto-deliver completion_delivery
# notices generated after the current relay process starts. Historical
# suppressed+contract notices require an explicit --task-id/manual call.
RELAY_PROCESS_START_TS = _now_epoch()
INTEGRATION_TOOLS_DEFAULT_INTAKE_STALE_SECONDS = int(os.getenv("PNC_INTEGRATION_TOOLS_INTAKE_STALE_SECONDS", "600") or "600")
INTEGRATION_TOOLS_DEFAULT_NEED_INPUT_STALE_SECONDS = int(os.getenv("PNC_INTEGRATION_TOOLS_NEED_INPUT_STALE_SECONDS", "1800") or "1800")
PNC_PROGRESS_HEARTBEAT_STALE_SECONDS = int(os.getenv("PNC_PROGRESS_HEARTBEAT_STALE_SECONDS", "600") or "600")
M2_3_SOURCE_CONTRACT = {
    "progress": "task_card hash changes are the user-visible intermediate progress channel",
    "card_failure_degrade": "card send/patch failure must downgrade to a compact topic text when no completion text is available",
    "failed_notice_alert": "completion notice max attempts must alert FEISHU_HOME_CHANNEL exactly once",
}
CARD_FALLBACK_PREFIX = "[PNC task card fallback]"
PNC_FEISHU_BUSINESS_TZ = timezone(timedelta(hours=8))
PERCEPTION_TEST_TEAM_VM_PREFIX = "/mnt/minieye/pdcl/department/perception_test_team/"
PERCEPTION_TEST_TEAM_CIFS_PREFIX = "//hfs.minieye.tech/department-perception_test_team/"
PERCEPTION_TEST_TEAM_HTTP_BASE = os.getenv("PNC_PERCEPTION_TEST_TEAM_HTTP_BASE", "http://192.168.26.174:18081/").strip().rstrip("/") + "/"


def _canonical_publication_report_origin() -> str:
    """Return the explicitly configured approved report origin."""

    return canonical_publication_origin()


def _canonical_publication_report_url(vm_path: str) -> str:
    """Map one report index path to the configured publication origin."""

    origin = _canonical_publication_report_origin()
    return canonical_report_url_from_vm_path(vm_path, origin)


def _validated_canonical_report_link(value: Any) -> str:
    """Accept only a URL matching the configured canonical report origin."""

    return validate_canonical_report_url(value, _canonical_publication_report_origin())


def _validated_foxglove_link(value: Any, viz_mcap_vm: Any) -> str:
    """Keep Foxglove as a separate, byte-identical visualization contract."""

    text = str(value or "").strip()
    if not text or not validate_foxglove_url(text, viz_mcap_vm):
        return ""
    return text


def _non_publication_fallback(value: Any) -> str:
    """Keep local/share paths as evidence, never arbitrary clickable URLs."""

    text = str(value or "").strip()
    if not text or "://" in text or text.startswith("//"):
        return ""
    return text


def _perception_test_team_cifs(vm_path: str) -> str:
    text = str(vm_path or "").strip()
    if not text.startswith(PERCEPTION_TEST_TEAM_VM_PREFIX):
        return ""
    rel = text[len(PERCEPTION_TEST_TEAM_VM_PREFIX):].lstrip("/")
    if not rel or any(part in {".", ".."} for part in rel.split("/")):
        return ""
    return PERCEPTION_TEST_TEAM_CIFS_PREFIX + rel


def _perception_test_team_http(vm_path: str) -> str:
    text = str(vm_path or "").strip()
    if not text.startswith(PERCEPTION_TEST_TEAM_VM_PREFIX):
        return ""
    rel = text[len(PERCEPTION_TEST_TEAM_VM_PREFIX):].lstrip("/")
    if not rel or any(part in {".", ".."} for part in rel.split("/")):
        return ""
    if not PERCEPTION_TEST_TEAM_HTTP_BASE.startswith(("http://", "https://")):
        return ""
    from urllib.parse import quote
    return PERCEPTION_TEST_TEAM_HTTP_BASE + quote(rel, safe="/._-()[]中文abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")


class FeishuHotSender:
    """Reusable Feishu sender for relay --watch hot path."""

    def __init__(self) -> None:
        self._platform = None
        self._pconfig = None
        self._adapter = None
        self._record_sender = None
        self._init_error: str | None = None
        # Serializes adapter (re)build under concurrent senders: the lark _client
        # itself is fine for concurrent requests, but rebuild-on-auth-error mutates
        # self._adapter/_client and must not race.
        self._adapter_lock = threading.Lock()
        from gateway.record_only.runtime import get_record_only_transport

        record_transport = get_record_only_transport("scripts.pnc_completion_notice_relay")
        if record_transport is not None:
            from gateway.record_only.transport import RecordOnlyRelaySender

            self._record_sender = RecordOnlyRelaySender(record_transport)
            return
        self._ensure_adapter()

    def _ensure_adapter(self, *, rebuild: bool = False):
        if self._record_sender is not None:
            raise RuntimeError("record-only FeishuHotSender has no live adapter")
        if self._adapter is not None and not rebuild:
            return self._adapter
        with self._adapter_lock:
            # Re-check inside the lock: another thread may have just (re)built.
            if self._adapter is not None and not rebuild:
                return self._adapter
            try:
                from gateway.config import Platform, load_gateway_config
                from gateway.platforms.feishu import FEISHU_AVAILABLE, FEISHU_DOMAIN, LARK_DOMAIN, FeishuAdapter
                if not FEISHU_AVAILABLE:
                    raise RuntimeError("Feishu dependencies not installed")
                config = load_gateway_config()
                platform = Platform("feishu")
                pconfig = config.platforms.get(platform)
                if not pconfig or not pconfig.enabled:
                    raise RuntimeError("Platform 'feishu' is not configured")
                adapter = FeishuAdapter(pconfig)
                domain_name = getattr(adapter, "_domain_name", "feishu")
                domain = FEISHU_DOMAIN if domain_name != "lark" else LARK_DOMAIN
                adapter._client = adapter._build_lark_client(domain)
                self._platform = platform
                self._pconfig = pconfig
                self._adapter = adapter
                self._init_error = None
                return adapter
            except Exception as exc:
                self._adapter = None
                self._init_error = f"{type(exc).__name__}: {exc}"
                raise

    @staticmethod
    def _looks_auth_error(result: dict[str, Any]) -> bool:
        text = str(result.get("error") or result.get("raw") or "").lower()
        markers = ("access_token", "tenant_access_token", "auth", "unauthorized", "expired", "999916", "99991663")
        return any(marker in text for marker in markers)

    @staticmethod
    def _card_failure_result(result: Mapping[str, Any]) -> dict[str, Any]:
        value = dict(result)
        error = str(
            value.get("error") or value.get("raw") or "task card patch failed"
        )
        lowered = error.lower()
        if _is_expired_card_update_error(error):
            return {
                **value,
                "success": False,
                "outcome_uncertain": False,
                "permanent": True,
                "error_code": "feishu_card_patch_message_expired",
                "error": error,
            }
        if any(
            marker in lowered for marker in ("permission", "forbidden", "230006")
        ):
            return {
                **value,
                "success": False,
                "outcome_uncertain": False,
                "error_code": "feishu_permission_denied",
                "error": error,
            }
        if FeishuHotSender._looks_auth_error(value):
            return {
                **value,
                "success": False,
                "outcome_uncertain": False,
                "error_code": "feishu_auth_failed",
                "error": error,
            }
        if any(
            marker in lowered
            for marker in (
                "dependencies not installed",
                "is not configured",
                "modulenotfounderror",
                "importerror",
            )
        ):
            return {
                **value,
                "success": False,
                "outcome_uncertain": False,
                "error_code": "feishu_card_dependency_unavailable",
                "error": error,
            }
        if "unsupported target" in lowered or "could not resolve" in lowered:
            return {
                **value,
                "success": False,
                "outcome_uncertain": False,
                "permanent": True,
                "error_code": "external_write_fence_target_mismatch",
                "error": error,
            }
        return {
            **value,
            "success": False,
            "outcome_uncertain": True,
            "error_code": str(
                value.get("error_code") or "feishu_card_patch_failed"
            ),
            "error": error,
        }

    async def _send_once(
        self,
        target: str,
        message: str,
        *,
        provider_claim: RcaProviderWriteClaim | None = None,
    ) -> dict[str, Any]:
        if provider_claim is not None and type(provider_claim) is not RcaProviderWriteClaim:
            raise ExternalWriteFenceError("external_write_provider_claim_invalid")
        adapter = self._ensure_adapter()
        parts = target.split(":", 1)
        if len(parts) != 2 or parts[0].strip().lower() != "feishu":
            return {"error": f"Unsupported target for FeishuHotSender: {target}"}
        chat_id, thread_id, is_explicit = _parse_target_ref("feishu", parts[1].strip())
        if not chat_id or not is_explicit:
            return {"error": f"Could not resolve '{parts[1].strip()}' on feishu hot sender"}
        normalized_thread_id = str(thread_id or "").strip()
        if normalized_thread_id and normalized_thread_id.startswith("om_"):
            normalized_thread_id = f"topic:{normalized_thread_id}"
        metadata = {"thread_id": normalized_thread_id} if normalized_thread_id else {}
        if provider_claim is not None:
            if type(provider_claim) is not RcaProviderWriteClaim:
                raise ExternalWriteFenceError(
                    "external_write_provider_claim_invalid"
                )
            metadata["_pnc_rca_external_write_guard"] = provider_claim
            metadata["_pnc_rca_external_write_operation"] = (
                "feishu_thread_reply" if normalized_thread_id else "internal_alert"
            )
        result = await adapter.send(chat_id, message, metadata=metadata)
        if not result.success:
            return {"error": result.error or "Feishu send failed"}
        payload = {
            "success": True,
            "platform": "feishu",
            "chat_id": chat_id,
            "thread_id": normalized_thread_id or None,
            "message_id": result.message_id,
        }
        payload["delivery_target"] = f"feishu:{chat_id}:{normalized_thread_id}" if normalized_thread_id else f"feishu:{chat_id}"
        return payload

    @staticmethod
    async def _verify_card_patch_target(
        adapter: Any,
        *,
        message_id: str,
        chat_id: str,
        thread_id: str,
        submission_key: str,
    ) -> None:
        """Officially read the card before patching an exact RCA task message."""

        if not message_id or not chat_id or not submission_key:
            raise ExternalWriteFenceError("external_write_fence_target_mismatch")
        request = adapter._build_get_message_request(message_id)
        response = await asyncio.to_thread(adapter._client.im.v1.message.get, request)
        if not adapter._response_succeeded(response):
            raise ExternalWriteFenceError("external_write_fence_target_mismatch")
        items = getattr(getattr(response, "data", None), "items", None)
        if not isinstance(items, list) or len(items) != 1:
            raise ExternalWriteFenceError("external_write_fence_target_mismatch")
        item = items[0]
        observed_message_id = str(getattr(item, "message_id", "") or "").strip()
        observed_chat_id = str(getattr(item, "chat_id", "") or "").strip()
        if observed_message_id != message_id or observed_chat_id != chat_id:
            raise ExternalWriteFenceError("external_write_fence_target_mismatch")
        expected_thread = str(thread_id or "").strip().removeprefix("topic:")
        if expected_thread:
            observed_threads = {
                str(getattr(item, field, "") or "").strip().removeprefix("topic:")
                for field in ("thread_id", "root_id", "parent_id")
            }
            observed_threads.discard("")
            if expected_thread not in observed_threads:
                raise ExternalWriteFenceError(
                    "external_write_fence_target_mismatch"
                )
        body = getattr(item, "body", None)
        content = str(getattr(body, "content", "") or "")
        try:
            observed_card = json.loads(content)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ExternalWriteFenceError(
                "external_write_fence_target_mismatch"
            ) from exc
        if not card_patch_payload_has_exact_submission_marker(
            observed_card,
            submission_key=submission_key,
        ):
            raise ExternalWriteFenceError("external_write_fence_target_mismatch")

    @staticmethod
    async def _patch_verified_task_card(
        adapter: Any,
        request: Any,
        *,
        provider_claim: RcaProviderWriteClaim,
        chat_id: str,
        thread_id: str,
    ) -> Any:
        """Revalidate after target readback, immediately before physical PATCH."""

        revalidate_provider_write_claim(
            provider_claim,
            operation="feishu_card_patch",
            chat_id=chat_id,
            thread_id=(
                f"topic:{thread_id}"
                if thread_id and not str(thread_id).startswith("topic:")
                else str(thread_id or "")
            ),
        )
        return await asyncio.to_thread(
            adapter._client.im.v1.message.patch,
            request,
        )

    async def _send_card_once(
        self,
        target: str,
        card_payload: dict[str, Any],
        *,
        message_id: str | None = None,
        provider_claim: RcaProviderWriteClaim | None = None,
    ) -> dict[str, Any]:
        adapter = self._ensure_adapter()
        payload = json.dumps(card_payload, ensure_ascii=False)
        parts = target.split(":", 1)
        if len(parts) != 2 or parts[0].strip().lower() != "feishu":
            return {"error": f"Unsupported target for FeishuHotSender: {target}"}
        chat_id, thread_id, is_explicit = _parse_target_ref("feishu", parts[1].strip())
        if not chat_id or not is_explicit:
            return {"error": f"Could not resolve '{parts[1].strip()}' on feishu hot sender"}
        if provider_claim is not None and type(provider_claim) is not RcaProviderWriteClaim:
            raise ExternalWriteFenceError("external_write_provider_claim_invalid")
        if chat_id == G1Q3_RCA_CHAT_ID and provider_claim is None:
            raise ExternalWriteFenceError("external_write_provider_claim_missing")
        if message_id:
            from lark_oapi.api.im.v1.model import PatchMessageRequest, PatchMessageRequestBody

            body = PatchMessageRequestBody.builder().content(payload).build()
            request = PatchMessageRequest.builder().message_id(message_id).request_body(body).build()
            if provider_claim is None:
                raise ExternalWriteFenceError("external_write_provider_claim_missing")
            live = revalidate_provider_write_claim(
                provider_claim,
                operation="feishu_card_patch",
                chat_id=chat_id,
                thread_id=(f"topic:{thread_id}" if thread_id and not str(thread_id).startswith("topic:") else str(thread_id or "")),
            )
            await self._verify_card_patch_target(
                adapter,
                message_id=message_id,
                chat_id=chat_id,
                thread_id=str(thread_id or ""),
                submission_key=str(live.get("submission_key") or ""),
            )
            response = await self._patch_verified_task_card(
                adapter,
                request,
                provider_claim=provider_claim,
                chat_id=chat_id,
                thread_id=str(thread_id or ""),
            )
            result = adapter._finalize_send_result(response, "task card patch failed")
            if not result.success:
                return {"error": result.error or "task card patch failed"}
            return {"success": True, "message_id": message_id, "updated": True}
        normalized_thread_id = str(thread_id or "").strip()
        if normalized_thread_id and normalized_thread_id.startswith("om_"):
            normalized_thread_id = f"topic:{normalized_thread_id}"
        metadata = {"thread_id": normalized_thread_id} if normalized_thread_id else {}
        if provider_claim is not None:
            metadata["_pnc_rca_external_write_guard"] = provider_claim
            metadata["_pnc_rca_external_write_operation"] = "feishu_card_create"
        response = await adapter._feishu_send_with_retry(
            chat_id=chat_id,
            msg_type="interactive",
            payload=payload,
            reply_to=None,
            metadata=metadata,
        )
        result = adapter._finalize_send_result(response, "task card send failed")
        if not result.success:
            return {"error": result.error or "task card send failed"}
        delivery_target = f"feishu:{chat_id}:{normalized_thread_id}" if normalized_thread_id else f"feishu:{chat_id}"
        return {
            "success": True,
            "platform": "feishu",
            "chat_id": chat_id,
            "thread_id": normalized_thread_id or None,
            "message_id": result.message_id,
            "delivery_target": delivery_target,
            "updated": False,
        }

    def send_task_card(
        self,
        target: str,
        card_payload: dict[str, Any],
        message_id: str | None = None,
        *,
        provider_claim: RcaProviderWriteClaim | None = None,
    ) -> dict[str, Any]:
        if provider_claim is not None and type(provider_claim) is not RcaProviderWriteClaim:
            raise ExternalWriteFenceError("external_write_provider_claim_invalid")
        if self._record_sender is not None:
            return self._record_sender.send_task_card(target, card_payload, message_id=message_id)
        try:
            result = asyncio.run(
                self._send_card_once(
                    target,
                    card_payload,
                    message_id=message_id,
                    provider_claim=provider_claim,
                )
            )
            if self._looks_auth_error(result) and not _is_expired_card_update_error(
                str(result.get("error") or "")
            ):
                self._ensure_adapter(rebuild=True)
                result = asyncio.run(
                    self._send_card_once(
                        target,
                        card_payload,
                        message_id=message_id,
                        provider_claim=provider_claim,
                    )
                )
        except ExternalWriteFenceError:
            raise
        except Exception as exc:
            result = {"error": f"Feishu hot card send failed: {type(exc).__name__}: {exc}"}
        if result.get("success") is True:
            return result
        return self._card_failure_result(result)

    def send(self, args: dict[str, Any]) -> str:
        if self._record_sender is not None:
            return self._record_sender.send(args)
        raw_claim = args.get("_pnc_rca_external_write_guard")
        if raw_claim is not None and type(raw_claim) is not RcaProviderWriteClaim:
            raise ExternalWriteFenceError("external_write_provider_claim_invalid")
        if args.get("action", "send") != "send":
            if raw_claim is not None:
                raise ExternalWriteFenceError("external_write_provider_claim_invalid")
            return send_message_tool(args)
        target = str(args.get("target") or "")
        message = str(args.get("message") or "")
        provider_claim = raw_claim
        if not target.startswith("feishu:"):
            if provider_claim is not None:
                raise ExternalWriteFenceError("external_write_provider_claim_invalid")
            return send_message_tool(args)
        try:
            result = asyncio.run(
                self._send_once(
                    target,
                    message,
                    provider_claim=provider_claim,
                )
            )
            if self._looks_auth_error(result):
                self._ensure_adapter(rebuild=True)
                result = asyncio.run(
                    self._send_once(
                        target,
                        message,
                        provider_claim=provider_claim,
                    )
                )
        except Exception as exc:
            result = {"error": f"Feishu hot send failed: {type(exc).__name__}: {exc}"}
        return json.dumps(result, ensure_ascii=False)


class SingleRunLock:
    def __init__(self, path: Path):
        self.path = path
        self.handle = None
        self.acquired = False

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("w", encoding="utf-8")
        try:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.acquired = False
        else:
            self.acquired = True
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.handle is not None:
            if self.acquired:
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            self.handle.close()


def _skipped_locked_result(*, send: bool) -> dict[str, Any]:
    return {
        "ok": True,
        "skipped": True,
        "reason": "another pnc_completion_notice_relay run is active",
        "dry_run": not send,
        "candidate_count": 0,
        "sent_count": 0,
        "rows": [],
        "errors": [],
    }

def _now_iso() -> str:
    return datetime.fromtimestamp(_now_epoch(), PNC_FEISHU_BUSINESS_TZ).isoformat()


def _parse_iso_ts(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            # Milestones are persisted as business-time display strings. Treating
            # them as the process-local timezone adds another +08:00 on every
            # replay when the isolated worker runs with TZ=UTC.
            parsed = parsed.replace(tzinfo=PNC_FEISHU_BUSINESS_TZ)
        return parsed.timestamp()
    except ValueError:
        return None



def _notice_marks_replay_suppressed(task_id: str, body: dict[str, Any] | None = None, notice: dict[str, Any] | None = None) -> bool:
    task_text = str(task_id or '').strip()
    if task_text.startswith('replay-'):
        return True
    body = body if isinstance(body, dict) else {}
    notice = notice if isinstance(notice, dict) else {}
    return bool(
        body.get('replay') is True
        or notice.get('replay') is True
        or body.get('replay_external_writes_suppressed') is True
        or notice.get('replay_external_writes_suppressed') is True
    )

def _notice_is_relayable(
    notice: dict[str, Any],
    *,
    retry_failed_after_seconds: int = 0,
    max_attempts: int = 3,
    now_ts: float | None = None,
    task_id: str = "",
    body: dict[str, Any] | None = None,
) -> tuple[bool, str]:
    if _notice_marks_replay_suppressed(task_id, body, notice):
        return False, "replay_suppressed"
    status = str(notice.get("send_status") or "").strip().lower()
    if status == "pending":
        return True, "pending"
    if status != "failed":
        return False, f"status={status or 'missing'}"
    attempts = int(notice.get("attempt_count") or 0)
    if attempts >= max_attempts and notice.get("alert_sent_at"):
        return False, f"max_attempts_reached:{attempts}"
    if retry_failed_after_seconds <= 0:
        return False, "failed_retry_disabled"
    last_attempt = _parse_iso_ts(notice.get("last_attempt_at")) or _parse_iso_ts(notice.get("sent_at"))
    if last_attempt is None:
        return True, "failed_without_last_attempt"
    current = _now_epoch() if now_ts is None else now_ts
    age = current - last_attempt
    if age >= retry_failed_after_seconds:
        return True, f"failed_retry_due:{int(age)}s"
    return False, f"retry_cooldown:{int(max(retry_failed_after_seconds - age, 0))}s"


def _load_json(path: Path) -> dict[str, Any]:
    try:
        body = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return body if isinstance(body, dict) else {}


def _task_id_from_sidecar_path(path: Path) -> str:
    return path.name[:-5] if path.name.endswith(".json") else path.stem


def iter_pending_notices(*, task_ids: Iterable[str] | None = None, retry_failed_after_seconds: int = 0, max_attempts: int = 3, since_ts: float | None = None, require_current_g1q3_write_fence: bool = False) -> list[tuple[str, Path, dict[str, Any], dict[str, Any]]]:
    task_filter = {str(item).strip() for item in (task_ids or []) if str(item).strip()}
    root = get_hermes_home() / "task-state"
    rows: list[tuple[str, Path, dict[str, Any], dict[str, Any]]] = []
    if not root.exists():
        return rows
    # Pre-filter by mtime before the expensive _load_json on each file. With ~6.7k
    # sidecars (96%+ dead history) loading all of them blocks ~1.9s on every full
    # scan. Anything whose mtime is older than the act-window can no longer be
    # acted on (retry windows / attempt caps long exhausted), so skip the load.
    # task_filter (explicit request) always bypasses the window so direct lookups
    # still hit. The window must be >= retry_failed_after to never drop a file the
    # relay could still legitimately retry.
    now_ts = _now_epoch()
    window_seconds = max(
        SCAN_ACT_WINDOW_SECONDS,
        int(retry_failed_after_seconds or 0) + SCAN_ACT_WINDOW_MARGIN_SECONDS,
    )
    stat_cache: dict[Path, float] = {}
    candidate_paths: list[Path] = []
    for path in root.glob("*.json"):
        task_id = _task_id_from_sidecar_path(path)
        try:
            mtime = path.stat().st_mtime
        except OSError:
            continue
        stat_cache[path] = mtime
        if task_filter:
            if task_id in task_filter:
                candidate_paths.append(path)
            continue
        if now_ts - mtime > window_seconds:
            continue
        candidate_paths.append(path)
    for path in sorted(candidate_paths, key=lambda item: stat_cache.get(item, 0.0), reverse=True):
        task_id = _task_id_from_sidecar_path(path)
        if task_filter and task_id not in task_filter:
            continue
        body = _load_json(path)
        if (
            require_current_g1q3_write_fence
            and _is_g1q3_rca_origin_task(task_id, body)
            and not _automatic_g1q3_write_fence_ready(task_id)
        ):
            # Automatic scans must not rewrite or retry historical G1Q3
            # sidecars that predate the activation-bound write fence. Explicit
            # one-task runs still surface the original fail-closed error.
            continue
        body = reconcile_vm_delivery_proposal(task_id, body)
        # Reconciliation may create the card for a non-terminal task. Enrich
        # afterwards so the first scan reaches the same fixed point as replays.
        body = enrich_task_card_vm_progress(task_id, body)
        body = enrich_task_card_delivery_contract(task_id, body)
        _atomic_write_json(path, body)
        guard_action = None
        body, guard_action = apply_integration_tools_close_loop_guard(task_id, path, body)
        if guard_action is not None:
            body["_close_loop_guard_action"] = guard_action
        notice = body.get("completion_notice") if isinstance(body.get("completion_notice"), dict) else None
        task_card = body.get("task_card") if isinstance(body.get("task_card"), dict) else None
        if _notice_marks_replay_suppressed(task_id, body, notice):
            continue
        explicit_task_filter = bool(task_filter and task_id in task_filter)
        # G1Q3-RCA milestone/delivery enrichment must run in the auto/watch
        # channel too — not only under an explicit --task filter — otherwise
        # watch-relayed cards keep just the seed "任务建好" milestone and drop
        # the per-phase trail.  The function self-guards on g1q3-rca task ids
        # and dedups milestones, so it is safe to call unconditionally.
        if task_card and "g1q3-rca" in task_id:
            body = enrich_g1q3_task_card_delivery(task_id, body)
            body, _g1q3_guard_action = apply_g1q3_close_loop_guard(task_id, path, body)
            if _g1q3_guard_action is not None:
                body["_close_loop_guard_action"] = _g1q3_guard_action
            task_card = body.get("task_card") if isinstance(body.get("task_card"), dict) else task_card
            # Candidate admission and sending must observe the same enriched
            # card. Otherwise the next scan sees a different render and emits
            # a second PATCH for the same terminal transition.
            _atomic_write_json(path, body)
            body, _review_confirm_result = ensure_rca_candidate_review_confirm(
                task_id=task_id,
                path=path,
                body=body,
            )
            task_card = body.get("task_card") if isinstance(body.get("task_card"), dict) else task_card
        card_relayable = _task_card_needs_sync(task_card) if task_card else False
        notify_pending = (_originator_notify_pending(task_id, body) or _mechanical_download_notify_pending(task_id, body) or _g1q3_anomaly_notify_pending(task_id, body) or _infra_recovery_notify_pending(task_id, body)) if task_card else False
        if not notice:
            if card_relayable or notify_pending:
                rows.append((task_id, path, body, {}))
            continue
        relayable, _reason = _notice_is_relayable(
            notice,
            retry_failed_after_seconds=retry_failed_after_seconds,
            max_attempts=max_attempts,
            task_id=task_id,
            body=body,
        )
        if not relayable and _notice_is_relayable_for_completion_delivery(
            body,
            notice,
            explicit_task_filter=explicit_task_filter,
            since_ts=since_ts,
        ):
            relayable = True
        if not relayable and not card_relayable and not notify_pending:
            continue
        chat_id = str(notice.get("chat_id") or (task_card or {}).get("chat_id") or "").strip()
        text = str(notice.get("text") or "").strip()
        if not text and not card_relayable and not notify_pending:
            continue
        if chat_id not in DEFAULT_CHAT_IDS and int(notice.get("attempt_count") or 0) < max_attempts - 1:
            continue
        rows.append((task_id, path, body, notice if relayable else {}))
    return rows




def _shared_state_root() -> Path:
    return get_hermes_home() / "runtime" / "shared-state"


def _shared_state_task_dir(task_id: str) -> Path:
    return _shared_state_root() / "tasks" / str(task_id or "")


def _load_shared_state_meta(task_id: str) -> dict[str, Any]:
    path = _shared_state_task_dir(task_id) / "meta.json"
    return _load_json(path) if path.exists() else {}


def _task_state_from_meta_or_status(task_id: str, meta: dict[str, Any]) -> str:
    state = str(meta.get("state") or meta.get("status") or "").strip()
    if state:
        return state
    status_path = _shared_state_task_dir(task_id) / "status.md"
    if status_path.exists():
        try:
            for line in status_path.read_text(encoding="utf-8").splitlines()[:40]:
                if line.strip().startswith("state:"):
                    return line.split(":", 1)[1].strip().strip('"\'')
        except Exception:
            return ""
    return ""


def _task_age_seconds(meta: dict[str, Any], body: dict[str, Any], task_card: dict[str, Any], *, now_ts: float | None = None) -> float | None:
    current = _now_epoch() if now_ts is None else now_ts
    # The SLA clock is the business/shared-state state timestamp.  Sidecar
    # collectors may refresh body.updated_at while only adding VM probes; that
    # must not reset the user-visible close-loop SLA.
    primary = _parse_iso_ts(meta.get("updated_at"))
    if primary is not None:
        return max(0.0, current - primary)
    candidates = [
        task_card.get("last_update_observed_at"),
        task_card.get("last_update_ts"),
        body.get("updated_at"),
    ]
    parsed = [_parse_iso_ts(item) for item in candidates]
    parsed = [item for item in parsed if item is not None]
    if not parsed:
        return None
    return max(0.0, current - max(parsed))


def _looks_answer_only_integration_tools_request(task_id: str) -> bool:
    goal_path = _shared_state_task_dir(task_id) / "goal.md"
    try:
        text = goal_path.read_text(encoding="utf-8").lower()
    except Exception:
        text = ""
    runbook_terms = ("logsim", "mcap", "回放", "回灌", "mcap-clean", "mcap-translate", "build-repro", "编译", "ci", "pipeline", "foxglove", "planning topic", "run_planning_visualization")
    qa_terms = ("怎么", "如何", "注意", "需要注意", "安全", "help", "--help", "脚本没有纯 help", "没有纯help", "无纯help", "收集哪些", "哪些信息", "是不是可以", "可以直接", "直接跑")
    return any(term in text for term in runbook_terms) and any(term in text for term in qa_terms)


def _append_unique(values: list[Any], *items: str) -> list[str]:
    out: list[str] = []
    for item in [*(str(v or "").strip() for v in values), *items]:
        text = str(item or "").strip()
        if text and text not in out:
            out.append(text)
    return out


def _compact_text(value: Any, *, limit: int = 220) -> str:
    text = str(value or "").strip()
    text = re.sub(r"\s+", " ", text)
    if len(text) > limit:
        return text[: max(0, limit - 1)].rstrip() + "…"
    return text


def _looks_like_html_source(text: str) -> bool:
    lowered = str(text or "").lower()
    return any(fragment in lowered for fragment in ("<!doctype", "<html", "<head", "<style", "</style", ".header h1", "justify-content:", "background:"))


def _safe_delivery_text(value: Any, *, limit: int = 220) -> str:
    text = _compact_text(value, limit=limit)
    if _looks_like_html_source(text):
        return ""
    return text


def _first_text(*values: Any, limit: int = 220) -> str:
    for value in values:
        text = _safe_delivery_text(value, limit=limit)
        if text:
            return text
    return ""


def _remote_read_user_text(value: Any, *, limit: int = 300) -> str:
    """Translate historical download-era wording at the user-facing boundary."""
    raw = str(value or "")
    text = _safe_delivery_text(value, limit=limit)
    if not text:
        return ""
    if LEGACY_MDI_COMMAND_RE.search(raw) or (
        "pdcl" in raw.lower()
        and re.search(r"补充|缺失|无效|missing|invalid|(?:下载)?命令", raw, re.I)
    ):
        return REMOTE_REFERENCE_COMPATIBILITY_NOTE[:limit]
    if "不执行 MDI 下载" in raw:
        return _compact_text(raw, limit=limit)
    replacements = (
        ("正在自动下载/解析", "正在远程读取/解析"),
        ("自动下载/数据管线", "远程读取/数据处理链路"),
        ("数据下载执行中", "远程读取问题数据中"),
        ("数据已下载", "问题数据已通过远程读取取得"),
        ("继续下载/解析", "继续远程读取/解析"),
        ("待下载/解析", "待远程读取/解析"),
        ("等待自动下载", "等待远程读取"),
    )
    normalized = text
    for old, new in replacements:
        normalized = normalized.replace(old, new)
    normalized = re.sub(r"\bready_to_download\b", "待远程读取", normalized, flags=re.I)
    normalized = re.sub(r"\brequires_download\b", "需要远程读取", normalized, flags=re.I)
    normalized = re.sub(r"\bneed_download\b", "待补充远程引用/证据", normalized, flags=re.I)
    normalized = re.sub(r"\bdownloading\b|\bdownload\b", "远程读取", normalized, flags=re.I)
    if normalized != text and "不执行 MDI 下载" not in normalized:
        normalized = normalized.rstrip("。； ") + "（远程读取模式，不执行 MDI 下载）。"
    return _compact_text(normalized, limit=limit)


def _nested_get(data: Any, *paths: str) -> Any:
    for path in paths:
        current = data
        ok = True
        for part in path.split("."):
            if isinstance(current, dict) and part in current:
                current = current.get(part)
            else:
                ok = False
                break
        if ok and current not in (None, "", [], {}):
            return current
    return None


def _artifact_root_candidates(artifact_root: str) -> list[Path]:
    text = str(artifact_root or "").strip()
    if not text:
        return []
    candidates: list[Path] = []
    candidates.extend(local_candidates_for_vm_path(text))
    if text.startswith("/") and "/mnt/tmp/" not in text:
        candidates.append(Path(text))
    if text.startswith("//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/"):
        rel = text.split("/tmp/", 1)[-1].lstrip("/")
        if rel and not any(part in {".", ".."} for part in rel.split("/")):
            candidates.append(Path.home() / "Mounts" / "department-pnc_team-planning_algo-driving" / "tmp" / rel)
    # Keep the literal path last; on Linux tests/VM it may already be local.
    candidates.append(Path(text))
    dedup: list[Path] = []
    seen: set[str] = set()
    for cand in candidates:
        key = str(cand)
        if key not in seen:
            seen.add(key)
            dedup.append(cand)
    return dedup


def _load_artifact_json(artifact_root: str, *names: str) -> dict[str, Any]:
    for root in _artifact_root_candidates(artifact_root):
        if not root.is_dir():
            continue
        for name in names:
            path = root / name
            try:
                if path.is_file() and path.stat().st_size < 20 * 1024 * 1024:
                    data = _load_json(path)
                    if data:
                        return data
            except OSError:
                continue
    return {}



def _g1q3_blocked_keyframe_kind(kind: Any) -> bool:
    text = str(kind or "").strip().lower()
    return bool(text and ("frame_id" in text or "keyframe" in text or "missing_signal" in text))


def _load_g1q3_pipeline_result(artifact_root: str, contract: dict[str, Any] | None = None, body: dict[str, Any] | None = None) -> dict[str, Any]:
    """Load authoritative pipeline_result.json for G1Q3 card/notify truth."""
    contract = contract if isinstance(contract, dict) else {}
    body = body if isinstance(body, dict) else {}
    proposal = body.get("vm_delivery_proposal") if isinstance(body.get("vm_delivery_proposal"), dict) else {}
    for candidate in (
        body.get("pipeline_result"),
        proposal.get("pipeline_result"),
        contract.get("pipeline_result"),
    ):
        if isinstance(candidate, dict) and candidate:
            return candidate
    if proposal.get("evidence_source") == "fixture":
        return {}
    local = _load_artifact_json(artifact_root, "pipeline_result.json") if artifact_root else {}
    if local:
        return local
    artifacts = contract.get("artifacts") if isinstance(contract.get("artifacts"), dict) else {}
    task_root_vm = str(artifacts.get("task_root_vm") or artifact_root or "").strip().rstrip("/")
    return _read_vm_json_file(task_root_vm + "/pipeline_result.json") if task_root_vm.startswith("/mnt/tmp/") else {}


def _blocked_keyframe_pipeline_result(artifact_root: str, contract: dict[str, Any] | None = None, body: dict[str, Any] | None = None) -> dict[str, Any]:
    pipeline = _load_g1q3_pipeline_result(artifact_root, contract, body)
    blocker = pipeline.get("blocker") if isinstance(pipeline.get("blocker"), dict) else {}
    if str(pipeline.get("status") or "").strip() == "blocked" and _g1q3_blocked_keyframe_kind(blocker.get("kind")):
        return pipeline
    return {}


def _delivery_is_blocked_keyframe(delivery: dict[str, Any]) -> bool:
    return (
        str(delivery.get("report_status") or "").strip() == "need_keyframe"
        or str(delivery.get("human_action_kind") or "").strip() == "need_keyframe"
        or str(delivery.get("business_state") or "").strip() == "blocked_need_keyframe"
    )

def _work_item_id_for_sidecar(task_id: str, body: dict[str, Any], meta: dict[str, Any]) -> str:
    for value in (
        _nested_get(body, "task_card.delivery.rca_status.work_item_id"),
        _nested_get(body, "completion_notice.work_item_id"),
        meta.get("work_item_id"),
        meta.get("artifact_root"),
        task_id,
    ):
        text = str(value or "")
        match = re.search(r"g1q3[-_]rca[-_]issue[-_]intake[-_](\d{6,})", text, re.I)
        if match:
            return match.group(1)
        match = re.search(r"(?:issue[-_]intake|work[-_]item|issue)[-_](\d{6,})", text, re.I)
        if match:
            return match.group(1)
        match = re.search(r"\b(\d{10,})\b", text)
        if match:
            return match.group(1)
    return ""


def _read_vm_json_file(vm_path: str, *, max_bytes: int = 5 * 1024 * 1024) -> dict[str, Any]:
    path = str(vm_path or "").strip()
    if not path.startswith("/mnt/tmp/") or ".." in Path(path).parts:
        return {}
    agent = str(Path.home() / ".local" / "bin" / "ssh-mini-agent")
    try:
        proc = subprocess.run(
            [agent, "read_file", path],
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=20,
        )
    except (OSError, subprocess.SubprocessError):
        return {}
    if proc.returncode != 0 or len(proc.stdout.encode("utf-8", errors="ignore")) > max_bytes:
        return {}
    try:
        payload = json.loads(proc.stdout)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _latest_governance_report_contract(task_id: str, body: dict[str, Any], meta: dict[str, Any]) -> dict[str, Any]:
    work_item_id = _work_item_id_for_sidecar(task_id, body, meta)
    if not work_item_id:
        return {}
    root = get_hermes_home() / "pnc_agent" / "governance_rca"
    if not root.is_dir():
        return {}
    candidates: list[tuple[str, dict[str, Any]]] = []
    for params_path in root.glob(f"*{work_item_id}*.json"):
        params = _load_json(params_path)
        if str(params.get("work_item_id") or "").strip() != work_item_id:
            continue
        artifact_root = str(params.get("artifact_root") or "").strip().rstrip("/") + "/"
        if not artifact_root.startswith("/mnt/tmp/"):
            continue
        contract = _read_vm_json_file(artifact_root + "delivery_contract.json")
        if not contract or str(contract.get("schema_version") or "") not in {
            "g1q3_delivery_contract_v1",
            "g1q3_delivery_contract_v2",
        }:
            continue
        report = contract.get("report") if isinstance(contract.get("report"), dict) else {}
        if str(contract.get("work_item_id") or "").strip() != work_item_id:
            continue
        if str(contract.get("business_state") or "").strip() not in {"report_completed", "final_closed"}:
            continue
        if report.get("is_deliverable") is not True:
            continue
        candidates.append((str(contract.get("generated_at") or ""), contract))
    if not candidates:
        return {}
    return sorted(candidates, key=lambda item: item[0])[-1][1]




def _shared_result_report_ready(task_id: str) -> dict[str, Any]:
    """Return authoritative report-ready facts from worker result.md if present.

    This is intentionally stronger than regex over log.md: G1Q3 pipelines keep
    the early S1 `ready_to_download` text in logs even after S2-S6 materialize a
    real report.  If result.md says report_ready and verifies non-empty report
    artifacts, user-facing surfaces must not downgrade back to need_download.
    """
    payload = _load_shared_result_payload(task_id)
    if not isinstance(payload, dict):
        return {}
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    verification = payload.get("verification") if isinstance(payload.get("verification"), dict) else {}
    artifacts = payload.get("artifacts") if isinstance(payload.get("artifacts"), dict) else {}
    terminal = str(summary.get("terminal_state") or payload.get("rca_terminal_state") or "").strip()
    pipeline_status = str(summary.get("pipeline_status") or payload.get("pipeline_status") or "").strip()
    checks = verification.get("checks") if isinstance(verification.get("checks"), list) else []
    check_ok = {str(item.get("name") or ""): bool(item.get("ok")) for item in checks if isinstance(item, dict)}
    ready = terminal == "report_ready" or pipeline_status in {"report_generated_need_review", "report_ready"}
    if not ready:
        return {}
    observation = payload.get("rca_observation") if isinstance(payload.get("rca_observation"), dict) else {}
    index_html_vm = str(artifacts.get("index_html_vm") or "").strip()
    causal_text = str(artifacts.get("attribution_causal_text") or observation.get("short_conclusion") or "").strip()
    viz_fields = foxglove_delivery_fields(artifacts, causal_text=causal_text)
    html_verified = bool(index_html_vm) and (
        not checks or (check_ok.get("index_html_exists_nonempty") and check_ok.get("report_data_exists_nonempty"))
    )
    viz_verified = bool(viz_fields["foxglove_url"]) and (
        not checks or check_ok.get("viz_mcap_exists_nonempty") is True
    )
    if not viz_verified:
        viz_fields = {**viz_fields, "viz_mcap_vm": "", "foxglove_url": ""}
    if not (html_verified or viz_verified):
        return {}
    execution_request = _load_execution_request_from_goal(task_id)
    remote_input = _remote_input_summary(execution_request)
    legacy_input = _legacy_remote_input_summary(execution_request)
    return {
        "terminal_state": terminal or "report_ready",
        "pipeline_status": pipeline_status,
        "attribution_status": str(summary.get("attribution_status") or observation.get("attribution_status") or observation.get("status") or "hypothesis_ready").strip(),
        "short_conclusion": str(observation.get("short_conclusion") or "").strip(),
        "rca_pattern": str(observation.get("rca_pattern") or "").strip(),
        "data_gate_status": str(observation.get("data_gate_status") or "").strip(),
        "high_confidence_boundary": str(observation.get("high_confidence_boundary") or "").strip(),
        "candidate_owner_domain": observation.get("candidate_owner_domain"),
        "input_display": _compact_input_label(execution_request),
        "input_resolved": remote_input or legacy_input,
        "artifact_root_vm": str(artifacts.get("artifact_root_vm") or "").strip(),
        "artifact_root_cifs": str(artifacts.get("artifact_root_cifs") or "").strip(),
        "case_dir_vm": str(artifacts.get("case_dir_vm") or "").strip(),
        "index_html_vm": index_html_vm if html_verified else "",
        "report_data_vm": str(artifacts.get("report_data_vm") or "").strip(),
        **viz_fields,
        "truth_source": "shared_result_report_ready",
    }


def _load_g1q3_delivery_contract(task_id: str, body: dict[str, Any] | None = None, artifact_root: str = "") -> dict[str, Any]:
    """Load an explicit G1Q3 delivery contract from body/card/artifact root."""
    candidates: list[Any] = []
    if isinstance(body, dict):
        candidates.append(body.get("delivery_contract"))
        proposal = body.get("vm_delivery_proposal") if isinstance(body.get("vm_delivery_proposal"), dict) else {}
        candidates.append(proposal.get("delivery_contract"))
        task_card = body.get("task_card") if isinstance(body.get("task_card"), dict) else {}
        candidates.append(task_card.get("delivery_contract"))
        delivery = task_card.get("delivery") if isinstance(task_card.get("delivery"), dict) else {}
        candidates.append(delivery.get("delivery_contract"))
    for item in candidates:
        if isinstance(item, dict) and str(item.get("schema_version") or "").strip() in {
            "g1q3_delivery_contract_v1",
            "g1q3_delivery_contract_v2",
        }:
            return item
    data = _load_artifact_json(artifact_root, "delivery_contract.json", "g1q3_delivery_contract.json") if artifact_root else {}
    if isinstance(data, dict) and str(data.get("schema_version") or "").strip() in {
        "g1q3_delivery_contract_v1",
        "g1q3_delivery_contract_v2",
    }:
        return data
    return {}


def _g1q3_contract_report_ready_truth(contract: dict[str, Any]) -> dict[str, Any]:
    """Map an explicit delivery contract to relay report-ready truth."""
    if not isinstance(contract, dict):
        return {}
    report = contract.get("report") if isinstance(contract.get("report"), dict) else {}
    artifacts = contract.get("artifacts") if isinstance(contract.get("artifacts"), dict) else {}
    verification = contract.get("verification") if isinstance(contract.get("verification"), dict) else {}
    summary = contract.get("summary") if isinstance(contract.get("summary"), dict) else {}
    business_state = str(contract.get("business_state") or "").strip()
    presentation_state = str(contract.get("presentation_state") or "").strip()
    report_status = str(report.get("status") or verification.get("pipeline_status") or "").strip()
    terminal = str(verification.get("terminal_state") or contract.get("terminal_state") or "").strip()
    deliverable = report.get("is_deliverable") is True
    ready = (
        business_state in {"report_completed", "final_closed"}
        or presentation_state in {"report_ready_needs_review", "done"}
        or report_status in {"report_generated_need_review", "report_ready", "html_delivery_ready"}
        or terminal == "report_ready"
    )
    if not (deliverable and ready):
        return {}
    primary_vm = str(artifacts.get("index_html_vm") or "").strip()
    legacy_primary_vm = str(artifacts.get("primary_report_vm") or "").strip()
    if not primary_vm and legacy_primary_vm.lower().endswith("/index.html"):
        primary_vm = legacy_primary_vm
    primary_cifs = str(artifacts.get("index_html_cifs") or artifacts.get("primary_report_cifs") or "").strip() if primary_vm else ""
    causal_text = str(artifacts.get("attribution_causal_text") or summary.get("short_conclusion") or "").strip()
    viz_fields = foxglove_delivery_fields(artifacts, causal_text=causal_text)
    if not (primary_vm or viz_fields["foxglove_url"]):
        return {}
    case_dir_vm = str(artifacts.get("case_dir_vm") or "").strip()
    if not case_dir_vm and primary_vm.startswith(PERCEPTION_TEST_TEAM_VM_PREFIX):
        case_dir_vm = str(Path(primary_vm).parent)
    if not case_dir_vm and viz_fields["viz_mcap_vm"].startswith(PERCEPTION_TEST_TEAM_VM_PREFIX):
        case_dir_vm = str(Path(viz_fields["viz_mcap_vm"]).parent)
    report_data_vm = str(artifacts.get("report_data_vm") or "").strip()
    short = str(summary.get("short_conclusion") or summary.get("l0") or "").strip()
    if not short:
        l1 = summary.get("l1") if isinstance(summary.get("l1"), list) else []
        short = str(l1[0] if l1 else "").strip()
    boundaries = contract.get("evidence_boundary") if isinstance(contract.get("evidence_boundary"), list) else []
    boundary = "；".join(str(x).strip() for x in boundaries if str(x).strip())
    candidate_owner = str(report.get("candidate_owner") or "").strip()
    candidate_domain = str(report.get("candidate_owner_domain") or "").strip()
    return {
        "terminal_state": terminal or "report_ready",
        "pipeline_status": report_status or "report_generated_need_review",
        "attribution_status": "hypothesis_ready" if report.get("is_candidate") is not False else "reviewed",
        "short_conclusion": short,
        "high_confidence_boundary": boundary,
        "candidate_owner_domain": candidate_domain,
        "responsibility_candidate": candidate_owner,
        "artifact_root_vm": str(artifacts.get("task_root_vm") or contract.get("artifact_root") or "").strip(),
        "artifact_root_cifs": str(artifacts.get("task_root_cifs") or "").strip(),
        "case_dir_vm": case_dir_vm,
        "index_html_vm": primary_vm,
        "index_html_cifs": primary_cifs,
        "report_data_vm": report_data_vm,
        **viz_fields,
        "truth_source": "delivery_contract_v1",
    }


def _g1q3_contract_missing_input_business_result(contract: dict[str, Any]) -> dict[str, Any]:
    """Synthetic business_result for explicit contract missing-user-input states."""
    if not isinstance(contract, dict):
        return {}
    user_action = contract.get("user_action") if isinstance(contract.get("user_action"), dict) else {}
    business_state = str(contract.get("business_state") or "").strip()
    if business_state not in {"missing_user_input", "data_required", "intake_validated"} and user_action.get("requires_user_input") is not True:
        return {}
    if _g1q3_contract_report_ready_truth(contract):
        return {}
    reason = str(user_action.get("next_action_text") or contract.get("blocker_reason") or "需要补充问题数据/证据后继续 RCA").strip()
    return {
        "gate_decision": "need_download",
        "gate_skip_reason": reason,
        "terminal_state": "need_download",
        "status": "need_download",
    }


def _strict_shared_log_report_ready_truth(task_id: str, artifact_root: str = "") -> dict[str, Any]:
    """Recover report-ready truth from strict VM closeout text in shared log.

    This is a deliberately narrow fallback for production cases where the host
    relay cannot read `/mnt/tmp/<task>/pipeline_result.json` through local mounts
    and `result.md` is only the generic worker-runner receipt.  It must not be a
    broad log grep: require multiple closeout-style markers before treating a
    stale `need_download` card as report-ready.
    """
    log_text = _read_shared_log_tail(task_id)
    if not log_text:
        return {}
    has_closeout_marker = any(marker in log_text for marker in (
        "vm_readonly_closeout",
        "VM readonly closeout",
        "openclaw_vm_closeout_v4_v8",
    ))
    has_terminal_ready = bool(re.search(r'["\'](?:terminal_state|rca_terminal_state)["\']\s*:\s*["\']report_ready["\']', log_text))
    has_pipeline_ready = bool(re.search(r'["\'](?:pipeline_status|status)["\']\s*:\s*["\'](?:report_generated_need_review|report_ready)["\']', log_text))
    has_verified = bool(re.search(r'["\']verified["\']\s*:\s*true', log_text, re.I))
    has_report_artifacts = (
        "report_data.json" in log_text
        and "index.html" in log_text
        and "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/" in log_text
    )
    if not (has_closeout_marker and has_terminal_ready and has_pipeline_ready and (has_verified or has_report_artifacts)):
        return {}

    def _last_match(pattern: str) -> str:
        matches = list(re.finditer(pattern, log_text))
        if not matches:
            return ""
        return matches[-1].group(1).strip()

    index_html_vm = _last_match(r'([/][^\s"\']*?/G1Q3_RCA/cases/[^\s"\']+?/index\.html)')
    report_data_vm = _last_match(r'([/][^\s"\']*?/G1Q3_RCA/cases/[^\s"\']+?/report_data\.json)')
    case_dir_vm = ""
    if index_html_vm:
        case_dir_vm = str(Path(index_html_vm).parent)
    elif report_data_vm:
        case_dir_vm = str(Path(report_data_vm).parent)
    short_conclusion = _last_match(r'["\'](?:short_conclusion|候选方向)["\']\s*:\s*["\']([^"\']{1,260})["\']')
    boundary = _last_match(r'["\'](?:confidence_boundary|high_confidence_boundary|boundary)["\']\s*:\s*["\']([^"\']{1,260})["\']')
    return {
        "terminal_state": "report_ready",
        "pipeline_status": "report_generated_need_review",
        "attribution_status": "hypothesis_ready",
        "short_conclusion": short_conclusion,
        "high_confidence_boundary": boundary,
        "artifact_root_vm": str(artifact_root or "").strip(),
        "artifact_root_cifs": _cifs_for_vm_path(str(artifact_root or "").strip()),
        "case_dir_vm": case_dir_vm,
        "index_html_vm": index_html_vm,
        "report_data_vm": report_data_vm,
        "truth_source": "strict_shared_log_closeout",
    }


def _g1q3_report_ready_truth(task_id: str, artifact_root: str, report_data: dict[str, Any], gate_result: dict[str, Any], contract: dict[str, Any] | None = None) -> dict[str, Any]:
    contract_ready = _g1q3_contract_report_ready_truth(contract or {})
    if contract_ready:
        return contract_ready
    ready = _shared_result_report_ready(task_id)
    if ready:
        return ready
    pipeline = _load_artifact_json(artifact_root, "pipeline_result.json")
    if isinstance(pipeline, dict):
        terminal = str(pipeline.get("rca_terminal_state") or "").strip()
        status = str(pipeline.get("status") or "").strip()
        if terminal == "report_ready" or status in {"report_generated_need_review", "report_ready"}:
            return {
                "terminal_state": terminal or "report_ready",
                "pipeline_status": status,
                "attribution_status": str(pipeline.get("attribution_status") or "hypothesis_ready").strip(),
                "case_dir_vm": str(pipeline.get("case_dir") or "").strip(),
                "index_html_vm": str(pipeline.get("index_html") or "").strip(),
                "truth_source": "pipeline_result",
            }
    strict_log_ready = _strict_shared_log_report_ready_truth(task_id, artifact_root)
    if strict_log_ready:
        return strict_log_ready
    summary = report_data.get("summary") if isinstance(report_data.get("summary"), dict) else {}
    html_validation = report_data.get("html_validation") if isinstance(report_data.get("html_validation"), dict) else {}
    if str(summary.get("status") or "").strip() == "hypothesis_ready" and str(html_validation.get("state") or "").strip() == "html_review_ready":
        return {
            "terminal_state": "report_ready",
            "pipeline_status": "report_generated_need_review",
            "attribution_status": "hypothesis_ready",
            "short_conclusion": str(summary.get("short_conclusion") or "").strip(),
            "rca_pattern": str(summary.get("rca_pattern") or "").strip(),
            "data_gate_status": str(summary.get("data_gate_status") or "").strip(),
            "high_confidence_boundary": str(summary.get("high_confidence_boundary") or "").strip(),
            "truth_source": "report_data_summary",
        }
    return {}

def _is_cifs_unc(text: str) -> bool:
    """True for SMB/CIFS UNC paths (``//hfs...`` / ``\\\\host\\share``).

    These are file-share mounts, not web URLs.  Feishu renders a leading
    ``//`` as a protocol-relative HTTPS link, which the file server refuses
    ("拒绝了我们的连接请求"), so such paths must never be emitted as a
    clickable artifact link.
    """
    s = str(text or "").strip()
    if s.startswith("\\\\"):
        return True
    if s.startswith("//") and not s.startswith(("http://", "https://", "file://")):
        return True
    return False


def _host_report_exists(artifact_root: str, artifact_path: str = "") -> bool:
    """Best-effort check that a real HTML report is materialized on the host.

    Resolves VM ``/mnt/tmp/...`` roots/paths to their local CIFS mount and
    confirms an ``index.html`` (or the given ``.html`` path) actually exists.
    Returns False when nothing is reachable — callers must then NOT claim
    ``html_delivery_ready`` or render an "打开 HTML 报告" button.
    """
    candidates: list[Path] = []
    # Direct .html artifact path takes priority.
    for raw in (artifact_path, artifact_root):
        text = str(raw or "").strip()
        if not text:
            continue
        # VM /mnt/tmp -> local mount candidates.
        for local in local_candidates_for_vm_path(text):
            candidates.append(local)
        # Already a local absolute path (e.g. host mount).
        if text.startswith("/") and "/mnt/tmp/" not in text:
            candidates.append(Path(text))
    for cand in candidates:
        try:
            if cand.is_file() and cand.name.lower().endswith(".html"):
                return True
            if cand.is_dir() and (cand / "index.html").is_file():
                return True
            if cand.suffix.lower() == ".html" and cand.is_file():
                return True
            # root passed as a dir-like path string ending in '/'
            if (cand / "index.html").is_file():
                return True
        except OSError:
            continue
    return False


def _read_shared_log_tail(task_id: str, *, max_chars: int = 220_000) -> str:
    path = _shared_state_task_dir(task_id) / "log.md"
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""
    return text[-max_chars:]




def _bounded_json_object(
    text: str,
    *,
    require_canonical: bool,
) -> dict[str, Any] | None:
    if not text or len(text.encode("utf-8")) > RCA_EXECUTION_REQUEST_MAX_BYTES:
        return None

    def object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    def reject_constant(value: str) -> None:
        raise ValueError(f"invalid JSON constant: {value}")

    try:
        value = json.loads(
            text,
            object_pairs_hook=object_pairs,
            parse_constant=reject_constant,
        )
        if not isinstance(value, dict):
            return None
        if require_canonical:
            canonical = json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            if text != canonical:
                return None
    except (TypeError, ValueError, OverflowError, RecursionError):
        return None
    return value


def _load_execution_request_from_goal(task_id: str) -> dict[str, Any]:
    """Extract one bounded execution request, preferring the fixed-CLI marker."""
    goal_path = _shared_state_task_dir(task_id) / "goal.md"
    try:
        if goal_path.stat().st_size > RCA_GOAL_MAX_BYTES:
            return {}
        raw = goal_path.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        return {}
    if len(raw.encode("utf-8")) > RCA_GOAL_MAX_BYTES:
        return {}

    begin_count = raw.count(RCA_EXECUTION_REQUEST_JSON_BEGIN)
    end_count = raw.count(RCA_EXECUTION_REQUEST_JSON_END)
    if begin_count or end_count:
        if begin_count != 1 or end_count != 1:
            return {}
        begin = raw.find(RCA_EXECUTION_REQUEST_JSON_BEGIN)
        payload_start = begin + len(RCA_EXECUTION_REQUEST_JSON_BEGIN)
        end = raw.find(RCA_EXECUTION_REQUEST_JSON_END)
        if begin < 0 or end <= payload_start:
            return {}
        enclosed = raw[payload_start:end]
        if not enclosed.startswith("\n") or not enclosed.endswith("\n"):
            return {}
        payload = enclosed[1:-1]
        if "\n" in payload or "\r" in payload:
            return {}
        if "<!-- G1Q3_RCA_" in payload:
            return {}
        return _bounded_json_object(payload, require_canonical=True) or {}

    if raw.count(RCA_LEGACY_EXECUTION_REQUEST_HEADING) != 1:
        return {}
    tail = raw.split(RCA_LEGACY_EXECUTION_REQUEST_HEADING, 1)[1]
    if tail.count("```json") != 1 or tail.count("```") != 2:
        return {}
    match = re.match(r"\s*```json[ \t]*\r?\n(.*?)\r?\n```", tail, re.S)
    if not match:
        return {}
    return _bounded_json_object(match.group(1), require_canonical=False) or {}


def _compact_input_label(execution_request: dict[str, Any]) -> str:
    if not isinstance(execution_request, dict):
        return ""
    issue = _nested_get(execution_request, "work_item.work_item_id")
    url = _nested_get(execution_request, "work_item.url")
    remote_input = _remote_input_summary(execution_request)
    legacy_input = _legacy_remote_input_summary(execution_request)
    parts = []
    if issue:
        parts.append(f"飞书问题 {issue}")
    elif url:
        parts.append(str(url))
    if remote_input:
        parts.append(remote_input)
    elif legacy_input:
        parts.append(legacy_input)
    return " + ".join(parts)


def _remote_input_summary(execution_request: dict[str, Any]) -> str:
    """Return a user-safe v2 input label without exposing opaque identifiers."""
    access = _nested_get(execution_request, "data.data_access")
    if not isinstance(access, dict) or access.get("mode") != "remote_read":
        return ""
    references = access.get("references")
    if not isinstance(references, list) or not references:
        return ""
    counts: dict[str, int] = {}
    for reference in references:
        if not isinstance(reference, dict):
            continue
        kind = str(reference.get("kind") or "").strip()
        if kind in {"event", "clip"}:
            counts[kind] = counts.get(kind, 0) + 1
    if not counts:
        return ""
    detail = ", ".join(f"{kind} x{counts[kind]}" for kind in sorted(counts))
    return f"远程直读 ({detail}；不执行 MDI 下载)"


def _legacy_remote_input_summary(execution_request: dict[str, Any]) -> str:
    """Map a v1 command-shaped address to the v2 display contract."""
    legacy = str(_nested_get(execution_request, "data.pdcl_download_cmd") or "").strip()
    if not legacy:
        return ""
    return "远程直读 (历史 v1 event/clip 引用；不执行 MDI 下载)"


def _cifs_for_vm_path(vm_path: str, fallback_root_cifs: str = "") -> str:
    text = str(vm_path or "").strip()
    if not text:
        return ""
    ptt = _perception_test_team_cifs(text)
    if ptt:
        return ptt
    root_vm = "/mnt/tmp/"
    root_cifs = "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/"
    if text.startswith(root_vm):
        rel = text[len(root_vm):].lstrip("/")
        if rel and not any(part in {".", ".."} for part in rel.split("/")):
            return root_cifs + rel
    if fallback_root_cifs and text.startswith("/"):
        return str(fallback_root_cifs).rstrip("/") + "/"
    return ""


def _canonical_user_visible_report_index(path: str) -> str:
    """Map a governed VM/host/CIFS artifact path to its canonical CIFS index."""
    text = str(path or "").strip()
    if not text:
        return ""

    for prefix in ("/mnt/tmp/", PERCEPTION_TEST_TEAM_VM_PREFIX):
        if text.startswith(prefix):
            rel = text[len(prefix):].rstrip("/")
            rel_parts = rel.split("/")
            if not rel or any(part in {"", ".", ".."} for part in rel_parts):
                return ""

    mapped = _cifs_for_vm_path(text)
    if not mapped:
        home = str(Path.home()).rstrip("/")
        host_mappings = (
            (
                home + "/Mounts/mini_root/mnt/tmp/",
                "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/",
            ),
            (
                home + "/Mounts/department-pnc_team-planning_algo-driving/tmp/",
                "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/",
            ),
            (
                home + "/Mounts/mini_root/mnt/minieye/pdcl/department/perception_test_team/",
                PERCEPTION_TEST_TEAM_CIFS_PREFIX,
            ),
        )
        for host_prefix, cifs_prefix in host_mappings:
            if text.startswith(host_prefix):
                rel = text[len(host_prefix):].lstrip("/").rstrip("/")
                if rel and not any(part in {"", ".", ".."} for part in rel.split("/")):
                    mapped = cifs_prefix + rel
                break

    if not mapped:
        governed_cifs_prefixes = (
            "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/",
            PERCEPTION_TEST_TEAM_CIFS_PREFIX,
        )
        if text.startswith(governed_cifs_prefixes):
            rel = text.split("/", 4)[-1].rstrip("/")
            if rel and not any(part in {"", ".", ".."} for part in rel.split("/")):
                mapped = text.rstrip("/")

    if not mapped:
        return ""
    return mapped if mapped.lower().endswith(".html") else mapped.rstrip("/") + "/index.html"


def _user_visible_report_index(artifact_root: str, artifact_cifs_root: str) -> str:
    """Return a recipient surface bound to the materialized artifact root."""
    derived = _canonical_user_visible_report_index(artifact_root)
    if not derived:
        return ""
    declared_raw = str(artifact_cifs_root or "").strip()
    if declared_raw:
        declared = _canonical_user_visible_report_index(declared_raw)
        if not declared or declared != derived:
            return ""
    return derived

def _load_shared_result_payload(task_id: str) -> dict[str, Any]:
    """Read shared-state result.md as JSON/dict when possible."""
    path = _shared_state_task_dir(task_id) / "result.md"
    try:
        raw = path.read_text(encoding="utf-8", errors="ignore").strip()
    except Exception:
        return {}
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except Exception:
        start = raw.find("{")
        end = raw.rfind("}")
        if start == -1 or end == -1 or end <= start:
            return {}
        try:
            data = json.loads(raw[start : end + 1])
        except Exception:
            return {}
    return data if isinstance(data, dict) else {}


def _load_business_result(task_id: str) -> dict[str, Any]:
    """Read the worker's authoritative ``business_result`` block from result.md.

    result.md is the shared-state result the worker writes; its ``business_result``
    carries the explicit gate_decision / gate_skip_reason / status / terminal_state.
    This is the single source of truth for the card and must win over regex
    inference over log/report text.  Best-effort: returns ``{}`` when result.md is
    absent, unwritten, or unparseable.
    """
    data = _load_shared_result_payload(task_id)
    if not isinstance(data, dict):
        return {}
    biz = data.get("business_result")
    return biz if isinstance(biz, dict) else {}


MECHANICAL_DOWNLOAD_BLOCKER_KINDS = {
    "invalid_schema_version",
    "missing_request",
    "request_missing",
    "request_not_visible_on_vm",
    "datapipe_failed",
    "datapipe_timeout",
    "timeout",
}


def _load_original_pipeline_blocker(task_id: str) -> dict[str, Any]:
    """Extract original datapipe blocker from shared result verification."""
    data = _load_shared_result_payload(task_id)
    verification = data.get("verification") if isinstance(data.get("verification"), dict) else {}
    blocker = verification.get("checked_original_pipeline_blocker")
    return blocker if isinstance(blocker, dict) else {}


def _mechanical_download_blocker_kind(task_id: str, task_card: dict[str, Any]) -> str:
    """Return blocker kind for mechanical data/download failures, else ''."""
    diagnostics = task_card.get("diagnostics") if isinstance(task_card.get("diagnostics"), dict) else {}
    delivery = task_card.get("delivery") if isinstance(task_card.get("delivery"), dict) else {}
    candidates: list[Any] = [
        diagnostics.get("blocker_kind"),
        diagnostics.get("download_blocker_kind"),
        delivery.get("blocker_kind"),
        delivery.get("download_blocker_kind"),
    ]
    blocker = diagnostics.get("blocker")
    if isinstance(blocker, dict):
        candidates.append(blocker.get("kind"))
    pipeline_blocker = _load_original_pipeline_blocker(task_id)
    if pipeline_blocker:
        candidates.append(pipeline_blocker.get("kind"))
    for item in candidates:
        kind = str(item or "").strip()
        if kind in MECHANICAL_DOWNLOAD_BLOCKER_KINDS:
            return kind
    return ""


def _pipeline_blocker_for_task(task_id: str, task_card: dict[str, Any]) -> dict[str, Any]:
    """Best structured blocker dict for fault classification, or {}.

    Gathers the producer's structured blocker from every place the VM may have
    written it, newest-contract first: ``business_result.blocker`` (the
    fault_class contract), then ``verification.checked_original_pipeline_blocker``,
    then ``diagnostics.blocker`` / ``diagnostics.download_blocker`` on the card.
    A dict carrying a ``kind``/``fault_class`` wins over a bare string blocker.
    """
    business = _load_business_result(task_id)
    candidates: list[Any] = [business.get("blocker") if isinstance(business, dict) else None]
    biz_fc = str(business.get("fault_class") or "").strip() if isinstance(business, dict) else ""
    if biz_fc:
        candidates.insert(0, {"fault_class": biz_fc, "kind": str(business.get("blocker_kind") or "").strip()})
    candidates.append(_load_original_pipeline_blocker(task_id))
    diagnostics = task_card.get("diagnostics") if isinstance(task_card.get("diagnostics"), dict) else {}
    candidates.append(diagnostics.get("pipeline_blocker"))
    candidates.append(diagnostics.get("blocker"))
    candidates.append(diagnostics.get("download_blocker"))
    for item in candidates:
        if isinstance(item, dict) and (item.get("kind") or item.get("fault_class")):
            return item
    return {}


def _pipeline_fault_class(task_id: str, task_card: dict[str, Any], *, gate_decision: str = "") -> str:
    """Classified fault lane for the task's blocker, or "" when none is present."""
    blocker = _pipeline_blocker_for_task(task_id, task_card)
    if not blocker and not gate_decision:
        return ""
    return pnc_fault_taxonomy.classify_blocker(blocker or None, gate_decision=gate_decision)


def _is_infra_self_healable_task(task_id: str, task_card: dict[str, Any]) -> bool:
    """True when the task is blocked by a system-fixable infra fault.

    Such a fault must NOT @-ping the issue originator (they cannot fix a VM
    ownership/permission/service fault); it is routed to in-process self-heal,
    auto-resume, and an ops alert instead.
    """
    return _pipeline_fault_class(task_id, task_card) == pnc_fault_taxonomy.INFRA_SELF_HEALABLE


def _is_pipeline_fix_task(task_id: str, task_card: dict[str, Any]) -> bool:
    """True for infra/code/tooling faults that must never @ the issue originator."""
    fault_class = _pipeline_fault_class(task_id, task_card, gate_decision=str((task_card.get("delivery") or {}).get("report_status") or ""))
    if fault_class in {pnc_fault_taxonomy.INFRA_SELF_HEALABLE, pnc_fault_taxonomy.HARD_DEFECT}:
        return True
    blocker = _pipeline_blocker_for_task(task_id, task_card)
    kind = str(blocker.get("kind") or "").strip() if isinstance(blocker, dict) else ""
    return kind in {"alignment_failed", "reader_topic_mismatch"}


def _true_user_data_missing_task(task_id: str, task_card: dict[str, Any]) -> bool:
    """Return true only for genuine originator-actionable missing data/evidence."""
    if _is_pipeline_fix_task(task_id, task_card):
        return False
    blocker = _pipeline_blocker_for_task(task_id, task_card)
    fault_class = pnc_fault_taxonomy.classify_blocker(blocker or None, gate_decision="need_download")
    if fault_class != pnc_fault_taxonomy.NEEDS_HUMAN_INPUT:
        return False
    delivery = task_card.get("delivery") if isinstance(task_card.get("delivery"), dict) else {}
    kind = str(delivery.get("human_action_kind") or "").strip()
    if kind in {"none", "need_keyframe", "need_frame", "confirm_review"}:
        return False
    return True


def _download_notify_ledger_dir() -> Path:
    return get_hermes_home() / "pnc_agent" / "notify-ledger" / "g1q3_download_notify"


def _download_notify_ledger_identity(task_id: str, notify_key: str) -> str:
    # Mechanical download notifications are operator-facing anti-spam pings.
    # State/card fields may be recomputed between relay loops (done vs
    # in_progress, guard summary wording, etc.), so the durable dedupe identity
    # is intentionally coarser than compute_notify_key: one task + one
    # mechanical blocker kind.
    marker = "mechanical_download:"
    text = str(notify_key or "")
    if marker in text:
        return f"{task_id}\0{marker}{text.rsplit(marker, 1)[-1]}"
    return f"{task_id}\0{text}"


def _download_notify_ledger_path(task_id: str, notify_key: str) -> Path:
    digest = hashlib.sha256(_download_notify_ledger_identity(task_id, notify_key).encode("utf-8")).hexdigest()
    return _download_notify_ledger_dir() / f"{digest}.json"


def _download_notify_ledger_seen(task_id: str, notify_key: str) -> bool:
    path = _download_notify_ledger_path(task_id, notify_key)
    if not path.exists():
        return False
    body = _load_json(path)
    if not body:
        # A zero/partial marker still means another process already claimed this
        # exact notification.  Fail closed to avoid Feishu spam.
        return True
    if str(body.get("task_id") or "") != str(task_id):
        return False
    # The path is already keyed by the coarse anti-spam identity; older/backfilled
    # records may carry a different state-specific notify_key.
    return True


def _download_notify_ledger_claim(task_id: str, notify_key: str, *, kind: str, target: str) -> tuple[bool, Path]:
    path = _download_notify_ledger_path(task_id, notify_key)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "pnc_download_notify_ledger_v1",
        "task_id": task_id,
        "notify_key": notify_key,
        "blocker_kind": kind,
        "target": target,
        "claimed_at": _now_iso(),
    }
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
    except FileExistsError:
        return False, path
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, sort_keys=True, indent=2)
        fh.write("\n")
    return True, path


def _download_notify_ledger_update(path: Path, **updates: Any) -> None:
    body = _load_json(path)
    if not body:
        body = {"schema_version": "pnc_download_notify_ledger_v1"}
    body.update(updates)
    _atomic_write_json(path, body)

def _mechanical_download_notify_pending(task_id: str, body: dict[str, Any]) -> bool:
    task_card = body.get("task_card") if isinstance(body.get("task_card"), dict) else None
    if not isinstance(task_card, dict):
        return False
    delivery = task_card.get("delivery") if isinstance(task_card.get("delivery"), dict) else {}
    if str(delivery.get("report_status") or "").strip() != "need_download":
        return False
    kind = _mechanical_download_blocker_kind(task_id, task_card)
    if not kind:
        return False
    meta = _load_shared_state_meta(task_id)
    notify_key = compute_notify_key(
        user_state=str(task_card.get("user_state") or ""),
        transition_marker=_transition_marker(meta, _task_state_from_meta_or_status(task_id, meta)),
        pending_confirm_ids=[],
        extra=f"mechanical_download:{kind}",
    )
    if str(task_card.get("last_download_notify_key") or "") == notify_key:
        return False
    if _download_notify_ledger_seen(task_id, notify_key):
        return False
    return True


def maybe_notify_mechanical_download_failure(
    *,
    task_id: str,
    path: Path,
    body: dict[str, Any],
    meta: dict[str, Any],
    send: bool,
    send_func: Callable[[dict[str, Any]], str] | None = None,
) -> dict[str, Any] | None:
    """Ping once for a historical need_download state caused by pipeline failure."""
    task_card = body.get("task_card") if isinstance(body.get("task_card"), dict) else None
    if not isinstance(task_card, dict):
        return None
    delivery = task_card.get("delivery") if isinstance(task_card.get("delivery"), dict) else {}
    if str(delivery.get("report_status") or "").strip() != "need_download":
        return None
    kind = _mechanical_download_blocker_kind(task_id, task_card)
    if not kind:
        return None
    notify_key = compute_notify_key(
        user_state=str(task_card.get("user_state") or ""),
        transition_marker=_transition_marker(meta, _task_state_from_meta_or_status(task_id, meta)),
        pending_confirm_ids=[],
        extra=f"mechanical_download:{kind}",
    )
    if str(task_card.get("last_download_notify_key") or "") == notify_key:
        return {"skipped": True, "reason": "already_notified", "kind": "mechanical_download", "blocker_kind": kind}
    if _download_notify_ledger_seen(task_id, notify_key):
        return {"skipped": True, "reason": "already_notified_ledger", "kind": "mechanical_download", "blocker_kind": kind}
    target = _card_target(task_card, body.get("completion_notice") if isinstance(body.get("completion_notice"), dict) else None)
    if not _target_has_thread_anchor(target):
        if send:
            task_card["last_download_notify_key"] = notify_key
            task_card["last_download_notify_skipped_reason"] = "no_thread_anchor"
            body["task_card"] = task_card
            _atomic_write_json(path, body)
        return {"skipped": True, "reason": "no_thread_anchor", "kind": "mechanical_download", "target": target, "blocker_kind": kind}
    originator_id = resolve_originator_open_id(meta)
    originator = build_at_mention(originator_id, resolve_display_name(originator_id)) or "@发起人"
    owner_text = ""
    owners = delivery.get("owners") or task_card.get("owners")
    if isinstance(owners, list) and owners:
        owner_text = " ".join(str(x) for x in owners[:2] if str(x).strip())
    reason = _remote_read_user_text(
        _first_text(delivery.get("conclusion"), task_card.get("status_line"), meta.get("latest_summary"), limit=220),
        limit=220,
    )
    message = (
        f"{originator} {owner_text} 远程读取/数据处理链路没有跑通，系统已定位为机械故障 {kind}，不是飞书项目字段缺失；系统不会回退到 MDI 下载。".strip()
        + (f" 当前状态：{reason}" if reason else "")
        + " 请保持本话题，修复后我会继续重跑闭环。"
        + (f" 追踪号 {task_id}" if task_id else "")
    )
    if not send:
        return {"dry_run": True, "kind": "mechanical_download", "blocker_kind": kind, "target": target, "notify_key": notify_key, "preview": message[:300]}
    claimed, ledger_path = _download_notify_ledger_claim(task_id, notify_key, kind=kind, target=target)
    if not claimed:
        return {"skipped": True, "reason": "already_notified_ledger", "kind": "mechanical_download", "blocker_kind": kind, "target": target, "notify_key": notify_key}
    try:
        raw = (send_func or send_message_tool)({"action": "send", "target": target, "message": message})
        try:
            result = json.loads(raw)
        except Exception:
            result = {"raw": raw}
    except Exception as exc:
        result = {"error": f"{type(exc).__name__}: {exc}"}
    ok = isinstance(result, dict) and result.get("success")
    if ok:
        task_card["last_download_notify_key"] = notify_key
        task_card["last_download_notify_at"] = _now_iso()
        task_card.pop("last_download_notify_error", None)
    else:
        task_card["last_download_notify_error"] = str(result.get("error") if isinstance(result, dict) else result)[:300]
    _download_notify_ledger_update(
        ledger_path,
        attempted_at=_now_iso(),
        success=bool(ok),
        result=result if isinstance(result, dict) else {"raw": str(result)},
    )
    body["task_card"] = task_card
    _atomic_write_json(path, body)
    return {"sent": bool(ok), "kind": "mechanical_download", "blocker_kind": kind, "target": target, "notify_key": notify_key, "result": result}


def _artifact_link_is_live(value: str) -> bool:
    """True only when an artifact link resolves to a real file/URL.

    Web URLs (http/https/file) are taken at face value.  CIFS/UNC (//hfs...) and
    VM (/mnt/tmp/...) file-share paths must resolve to an existing file on a
    local mount, otherwise the link is dead — regression 7025381565 synthesized
    an html_url to an L0_L1 summary that was never written.
    """
    text = str(value or "").strip()
    if not text:
        return False
    if text.startswith(("http://", "https://", "file://")):
        return True
    candidates: list[Path] = list(local_candidates_for_vm_path(text))
    cifs_prefix = "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/"
    if text.startswith(cifs_prefix):
        rel = text[len(cifs_prefix):].lstrip("/")
        if rel and not any(part in {".", ".."} for part in rel.split("/")):
            candidates.append(Path.home() / "Mounts" / "department-pnc_team-planning_algo-driving" / "tmp" / rel)
    if text.startswith("/") and "/mnt/tmp/" not in text:
        candidates.append(Path(text))
    for cand in candidates:
        try:
            if cand.is_file():
                return True
        except OSError:
            continue
    return False


def _extract_log_field(text: str, key: str) -> str:
    patterns = [
        rf'"{re.escape(key)}"\s*:\s*"([^"\\]*(?:\\.[^"\\]*)*)"',
        rf"'{re.escape(key)}'\s*:\s*'([^']*)'",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            value = match.group(1).replace('\\n', ' ').replace('\\"', '"')
            return _safe_delivery_text(value)
    return ""


def _extract_labeled_line(text: str, label: str) -> str:
    match = re.search(rf"{re.escape(label)}[：:]\s*([^\n]+)", text)
    return _safe_delivery_text(match.group(1)) if match else ""


def _extract_markdown_link(text: str) -> str:
    match = re.search(r"\[[^\]]+\]\(([^)]+)\)", str(text or "").strip())
    return _safe_delivery_text(match.group(1), limit=500) if match else ""


def _normalize_html_artifact_path(path: str, *, report_status: str = "") -> tuple[str, str]:
    text = _extract_markdown_link(str(path or "")) or str(path or "").strip()
    if not text:
        return "", ""
    if _looks_like_html_source(text):
        return "", "html_source_suppressed"
    clean = text.rstrip()
    is_web = clean.startswith(("http://", "https://", "file://"))
    try:
        parsed = urlsplit(clean) if is_web else None
    except ValueError:
        return "", ""
    path_part = parsed.path if parsed is not None else clean
    if path_part.lower().endswith(".html"):
        pointer = clean
        root_path = path_part.rsplit("/", 1)[0] + "/" if "/" in path_part else ""
        root = (
            urlunsplit((parsed.scheme, parsed.netloc, root_path, "", ""))
            if parsed is not None
            else root_path
        )
    elif report_status == "html_delivery_ready":
        # F1 only sets html_delivery_ready when a real index.html exists on the
        # host, so synthesizing the index.html pointer here is now safe.
        report_path = path_part.rstrip("/") + "/index.html"
        root_path = path_part.rstrip("/") + "/"
        if parsed is not None:
            pointer = urlunsplit((
                parsed.scheme,
                parsed.netloc,
                report_path,
                parsed.query,
                parsed.fragment,
            ))
            root = urlunsplit((parsed.scheme, parsed.netloc, root_path, "", ""))
        else:
            pointer = report_path
            root = root_path
    else:
        # No confirmed report: keep the directory for display, fabricate no
        # clickable pointer (avoids a button into an empty directory).
        root = clean.rstrip("/") + "/" if clean.endswith("/") else clean
        return "", root
    # Only true web/file URLs are safely clickable.  CIFS/UNC (//hfs...) and VM
    # (/mnt/...) paths are file shares; Feishu mis-renders a leading "//" as
    # HTTPS and the file server refuses the connection, so they are returned as
    # a non-clickable directory instead of a clickable pointer.
    return (pointer, root) if is_web else ("", root)


def _milestone_semantic_key(label: str) -> str:
    text = str(label or "").strip()
    for prefix in ("任务状态更新", "报告状态确认", "归因状态确认"):
        if text.startswith(prefix + "：") or text.startswith(prefix + ":"):
            return prefix
    return text


def _format_business_ts(value: Any) -> str:
    epoch = _parse_iso_ts(value)
    if epoch is None:
        return str(value or "").strip()
    return datetime.fromtimestamp(epoch, PNC_FEISHU_BUSINESS_TZ).strftime("%Y-%m-%d %H:%M:%S")


def _append_milestone(milestones: list[Any], label: str, ts: Any = "") -> None:
    label = _safe_delivery_text(label, limit=120)
    if not label:
        return
    key = _milestone_semantic_key(label)
    new_epoch = _parse_iso_ts(ts)
    for idx, item in enumerate(milestones):
        if not isinstance(item, dict):
            continue
        existing_label = str(item.get("label") or "").strip()
        if _milestone_semantic_key(existing_label) != key:
            continue
        old_epoch = _parse_iso_ts(item.get("ts"))
        # A milestone is a point-in-time event: once recorded, KEEP its original
        # ts. Re-stamping an existing milestone whenever the incoming ts is
        # `notice.generated_at or meta.updated_at` drifts it forward on every
        # passive re-sync (meta.updated_at is bumped by vm_task_sync upserts / log
        # appends). That changed the rendered card hash and re-PATCHed the card
        # every full scan — a silent relay/vm_task_sync two-writer flap
        # (2026-06-26). Only backfill a ts when the existing one is
        # missing/unparseable; never advance an already-stamped milestone.
        if old_epoch is None and new_epoch is not None:
            milestones[idx] = {"ts": _format_business_ts(ts), "label": label}
        return
    milestones.append({"ts": _format_business_ts(ts), "label": label})


def _vm_progress_label(progress: dict[str, Any]) -> str:
    if not isinstance(progress, dict):
        return ""
    msg = _safe_delivery_text(progress.get("message") or progress.get("summary") or progress.get("raw"), limit=90)
    phase = _safe_delivery_text(progress.get("phase"), limit=40)
    if msg:
        return f"执行阶段：{msg}"
    if phase:
        return f"执行阶段：{phase}"
    return ""


def reconcile_vm_delivery_proposal(task_id: str, body: dict[str, Any]) -> dict[str, Any]:
    """Apply vm_task_sync's VM-side delivery proposal under relay ownership."""
    if not isinstance(body, dict):
        return body
    proposal = body.get("vm_delivery_proposal") if isinstance(body.get("vm_delivery_proposal"), dict) else None
    if not isinstance(proposal, dict):
        return body
    task_card = body.get("task_card") if isinstance(body.get("task_card"), dict) else None
    if not isinstance(task_card, dict):
        notice = body.get("completion_notice") if isinstance(body.get("completion_notice"), dict) else {}
        task_card = {
            "schema_version": 1,
            "task_id": task_id,
            "chat_id": notice.get("chat_id") or proposal.get("chat_id"),
            "thread_id": notice.get("thread_id") or proposal.get("thread_id"),
            "message_id": notice.get("message_id") or proposal.get("message_id"),
            "vm_task_id": notice.get("vm_task_id") or proposal.get("vm_task_id"),
            "created_at": body.get("created_at") or proposal.get("generated_at") or _now_iso(),
            "one_card_policy": True,
            "milestones": [],
        }
    for field in ("chat_id", "thread_id", "message_id", "vm_task_id"):
        proposal_value = proposal.get(field)
        if proposal_value and not task_card.get(field):
            task_card[field] = proposal_value
    proposed_delivery = proposal.get("delivery") if isinstance(proposal.get("delivery"), dict) else {}
    if proposed_delivery:
        existing_delivery = task_card.get("delivery") if isinstance(task_card.get("delivery"), dict) else {}
        merged_delivery = dict(existing_delivery)
        merged_delivery.update(proposed_delivery)
        existing_boundaries = existing_delivery.get("boundaries") if isinstance(existing_delivery.get("boundaries"), list) else []
        new_boundaries = proposed_delivery.get("boundaries") if isinstance(proposed_delivery.get("boundaries"), list) else []
        merged_boundaries = []
        for item in [*existing_boundaries, *new_boundaries]:
            if item and item not in merged_boundaries:
                merged_boundaries.append(item)
        if merged_boundaries:
            merged_delivery["boundaries"] = merged_boundaries
        task_card["delivery"] = merged_delivery
    user_state = str(proposal.get("user_state") or "").strip()
    if user_state:
        task_card["user_state"] = user_state
    status_line = str(proposal.get("status_line") or "").strip()
    if status_line:
        task_card["status_line"] = status_line
    task_card["vm_delivery_proposal_applied_at"] = _now_iso()
    body["task_card"] = task_card
    return body


def enrich_task_card_vm_progress(task_id: str, body: dict[str, Any], *, now_ts: float | None = None) -> dict[str, Any]:
    task_card = body.get("task_card") if isinstance(body.get("task_card"), dict) else None
    if not isinstance(task_card, dict):
        return body
    vm_bridge = body.get("vm_bridge") if isinstance(body.get("vm_bridge"), dict) else {}
    recent_events = body.get("recent_events") if isinstance(body.get("recent_events"), list) else []
    if not vm_bridge and not recent_events:
        return body
    if vm_bridge:
        task_card["vm_bridge"] = vm_bridge
    if recent_events:
        task_card["recent_events"] = recent_events[-20:]
    milestones = task_card.get("milestones") if isinstance(task_card.get("milestones"), list) else []
    progress = vm_bridge.get("progress") if isinstance(vm_bridge.get("progress"), dict) else {}
    progress_label = _vm_progress_label(progress)
    if progress_label:
        _append_milestone(milestones, progress_label, progress.get("ts") or progress.get("time") or body.get("updated_at"))
    for event in recent_events[-8:]:
        if not isinstance(event, dict):
            continue
        label = _safe_delivery_text(event.get("summary") or event.get("event") or event.get("message"), limit=90)
        if label:
            _append_milestone(milestones, f"执行阶段：{label}", event.get("ts") or event.get("time"))
    state = str(vm_bridge.get("state") or task_card.get("user_state") or "").strip().lower()
    progress_ts = _parse_iso_ts(progress.get("ts") or progress.get("time"))
    current_ts = _now_epoch() if now_ts is None else now_ts
    if state in {"running", "executing", "in_progress", "picked-up"} and progress_ts is not None:
        stale = max(0.0, current_ts - progress_ts)
        if stale >= max(1, PNC_PROGRESS_HEARTBEAT_STALE_SECONDS):
            minutes = int(stale // 60)
            last = _safe_delivery_text(progress.get("message") or progress.get("phase") or "执行中", limit=80)
            heartbeat = f"仍在执行，最近阶段 {last}，已 {minutes}m 无新进度"
            if task_card.get("last_progress_heartbeat_label") != heartbeat:
                _append_milestone(milestones, heartbeat, _now_iso())
                task_card["last_progress_heartbeat_label"] = heartbeat
                task_card["status_line"] = heartbeat
    task_card["milestones"] = _trim_milestones(milestones)
    body["task_card"] = task_card
    return body


def _trim_milestones(milestones: list[Any], *, limit: int = 8) -> list[dict[str, str]]:
    by_key: dict[str, tuple[int, float | None, dict[str, str]]] = {}
    for index, item in enumerate(milestones):
        if not isinstance(item, dict):
            continue
        label = _safe_delivery_text(item.get("label"), limit=120)
        if not label:
            continue
        ts = _format_business_ts(item.get("ts"))
        epoch = _parse_iso_ts(ts)
        key = _milestone_semantic_key(label)
        prev = by_key.get(key)
        if prev is None:
            by_key[key] = (index, epoch, {"ts": ts, "label": label})
            continue
        prev_index, prev_epoch, _prev_item = prev
        # Semantic duplicate: keep the newest timestamp; when timestamps are both
        # missing/equal, keep the later insertion.
        replace = False
        if epoch is not None and (prev_epoch is None or epoch >= prev_epoch):
            replace = True
        elif epoch is None and prev_epoch is None and index >= prev_index:
            replace = True
        if replace:
            by_key[key] = (index, epoch, {"ts": ts, "label": label})
    ordered = sorted(by_key.values(), key=lambda row: (row[1] is None, row[1] if row[1] is not None else float("inf"), row[0]))
    return [row[2] for row in ordered][-limit:]


def enrich_g1q3_task_card_delivery(task_id: str, body: dict[str, Any]) -> dict[str, Any]:
    """Best-effort, card-only G1Q3 delivery drift fix.

    This maps existing execution facts into user-readable task_card fields.  It
    does not change RCA pipeline state and silently degrades when artifacts are
    unavailable.
    """
    if not isinstance(body, dict) or "g1q3-rca" not in str(task_id):
        return body
    task_card = body.get("task_card") if isinstance(body.get("task_card"), dict) else None
    if not isinstance(task_card, dict):
        return body
    notice = body.get("completion_notice") if isinstance(body.get("completion_notice"), dict) else {}
    delivery = task_card.get("delivery") if isinstance(task_card.get("delivery"), dict) else {}
    meta = _load_shared_state_meta(task_id)
    log_text = _read_shared_log_tail(task_id)
    business_result = _load_business_result(task_id)

    artifact_root = _first_text(meta.get("artifact_root"), delivery.get("artifact_root"), body.get("artifact_root"), limit=500)
    artifact_cifs_root = _first_text(meta.get("artifact_cifs_root"), delivery.get("artifact_root"), delivery.get("artifact_path"), body.get("artifact_cifs_root"), limit=500)
    latest_report_contract = _latest_governance_report_contract(task_id, body, meta)
    latest_report_truth = _g1q3_contract_report_ready_truth(latest_report_contract)
    if latest_report_truth:
        report_data: dict[str, Any] = {}
        gate_result: dict[str, Any] = {}
        delivery_contract = latest_report_contract
    else:
        report_data = _load_artifact_json(artifact_root, "report_data.json")
        gate_result = _load_artifact_json(artifact_root, "gate_result.json")
        delivery_contract = _load_g1q3_delivery_contract(task_id, body, artifact_root)
    pipeline_result_truth = _load_g1q3_pipeline_result(artifact_root, delivery_contract, body)
    contract_business_result = _g1q3_contract_missing_input_business_result(delivery_contract)
    if contract_business_result and not business_result:
        business_result = contract_business_result
    report_ready_truth = _g1q3_report_ready_truth(task_id, artifact_root, report_data, gate_result, delivery_contract)
    blocked_keyframe_pipeline = (
        {}
        if latest_report_truth
        else _blocked_keyframe_pipeline_result(artifact_root, delivery_contract, body)
    )

    attribution_status = _first_text(
        report_ready_truth.get("attribution_status"),
        delivery.get("attribution_status"),
        _nested_get(delivery, "rca_status.attribution_status"),
        body.get("attribution_status"),
        notice.get("attribution_status"),
        _nested_get(report_data, "summary.status", "attribution_status", "rca_receipt.responsibility.status", "responsibility.status", "receipt_status"),
        _nested_get(gate_result, "attribution_status", "receipt_status"),
        _extract_log_field(log_text, "receipt_status"),
        _extract_log_field(log_text, "report_status"),
        limit=80,
    )
    if attribution_status == "report_generated":
        attribution_status = "hypothesis_ready"

    report_status = _first_text(
        delivery.get("report_status"),
        _nested_get(delivery, "rca_status.report_status"),
        body.get("report_status"),
        notice.get("report_status"),
        _nested_get(report_data, "html_validation_state", "report_status", "html_status"),
        _nested_get(gate_result, "html_validation_state", "report_status"),
        _extract_log_field(log_text, "html_validation_state"),
        limit=80,
    )
    # Only claim a deliverable HTML report when gate is green AND parsed/L2
    # evidence exists.  A materialized index.html under a non-green gate is only
    # a draft/stale artifact, never a user-deliverable report.  Exception:
    # worker result.md/pipeline_result report_ready is an authoritative later
    # stage than stale S1 ready_to_download log text.
    host_report_exists = _host_report_exists(artifact_root, artifact_cifs_root) if (artifact_cifs_root or artifact_root) else False
    verified_report_index = (
        _user_visible_report_index(artifact_root, artifact_cifs_root)
        if host_report_exists
        else ""
    )
    if report_ready_truth:
        report_status = "report_ready" if str(report_ready_truth.get("foxglove_url") or "").strip() else "html_delivery_ready"
        # Worker report_ready already verified non-empty index.html/report_data.
        # This G1Q3 business shape may be raw-function-decoded (not full parsed
        # L2), so mark parsed assets sufficient for delivery-to-review here.
        gate_result = {**gate_result, "decision": "report_ready", "parsed_l2_assets_present": True}
        # A strict delivery contract / closeout report truth is newer and more
        # specific than stale intake business_result records.  Do not let an
        # earlier need_download/missing_input business_result downgrade a
        # verified report-ready handoff back to need_evidence.
        business_result = {}
        log_text_for_truth = ""
    else:
        log_text_for_truth = log_text
    if not report_status and host_report_exists and gate_is_green(gate_result, log_text_for_truth) and parsed_l2_assets_present(gate_result, report_data, log_text_for_truth):
        report_status = "html_delivery_ready"

    candidate_cause = _first_text(
        report_ready_truth.get("attribution_causal_text"),
        report_ready_truth.get("short_conclusion"),
        delivery.get("candidate_cause"),
        _nested_get(delivery, "rca_status.candidate_cause"),
        body.get("candidate_cause"),
        notice.get("candidate_cause"),
        _nested_get(report_data, "summary.short_conclusion", "candidate_cause", "causal_chain.summary", "rca_receipt.causal_chain.summary", "responsibility.candidate_reason"),
        _nested_get(gate_result, "candidate_cause"),
        _extract_log_field(log_text, "candidate_cause"),
        _extract_labeled_line(log_text, "候选原因"),
        limit=260,
    )
    responsibility_candidate = _first_text(
        report_ready_truth.get("responsibility_candidate"),
        delivery.get("responsibility_candidate"),
        _nested_get(delivery, "rca_status.responsibility_candidate", "rca_status.candidate_responsibility"),
        body.get("responsibility_candidate"),
        notice.get("responsibility_candidate"),
        _nested_get(report_data, "candidate_responsibility", "responsibility_candidate", "responsibility.owner", "rca_receipt.responsibility.owner"),
        _nested_get(gate_result, "candidate_responsibility", "responsibility_candidate"),
        _extract_log_field(log_text, "candidate_responsibility"),
        _extract_labeled_line(log_text, "责任候选"),
        limit=80,
    )
    evidence_boundary = _remote_read_user_text(_first_text(
        report_ready_truth.get("high_confidence_boundary"),
        delivery.get("evidence_boundary"),
        _nested_get(report_data, "evidence_boundary", "boundary", "rca_receipt.review.boundary"),
        _nested_get(gate_result, "evidence_boundary"),
        _extract_log_field(log_text, "evidence_boundary"),
        limit=260,
    ), limit=260)

    truth = reconcile_report_truth(
        gate_result,
        report_status,
        attribution_status,
        log_text_for_truth,
        report_data=report_data,
        candidate_cause=candidate_cause,
        responsibility_candidate=responsibility_candidate,
        business_result=business_result,
    )
    business_truth = truth.get("truth_source") == "business_result"
    report_status = str(truth.get("honest_report_status") or "")
    if business_truth:
        # business_result is authoritative: a blocked intake read zero evidence,
        # so honest_attribution_status is "" — do NOT fall back to a stale raw
        # attribution (e.g. the report_generated->hypothesis_ready remap above).
        attribution_status = str(truth.get("honest_attribution_status") or "")
    else:
        attribution_status = str(truth.get("honest_attribution_status") or attribution_status or "")
    gate_label = str(truth.get("gate_decision") or "").strip()
    gate_green = bool(truth.get("gate_green"))
    blocked_keyframe_blocker = blocked_keyframe_pipeline.get("blocker") if isinstance(blocked_keyframe_pipeline.get("blocker"), dict) else {}
    blocked_keyframe_message = str(blocked_keyframe_blocker.get("message") or "自动找帧无候选；需人工补帧").strip() if blocked_keyframe_pipeline else ""
    blocked_keyframe_stage = str(blocked_keyframe_pipeline.get("stage") or "s45_auto_keyframe").strip() if blocked_keyframe_pipeline else ""
    if blocked_keyframe_pipeline:
        report_ready_truth = {}
        report_status = "need_keyframe"
        attribution_status = "need_keyframe"
        gate_label = "blocked"
        gate_green = False
        candidate_cause = ""
        responsibility_candidate = ""
    if truth.get("responsibility_candidate_mode") == "suppressed_until_human_gate":
        responsibility_candidate = ""
    if truth.get("candidate_cause_mode") == "low_confidence_hypothesis" and candidate_cause:
        candidate_cause = f"低置信假设，待人工确认：{candidate_cause}"

    artifact_source = _first_text(_nested_get(delivery, "rca_status.html_link"), limit=500) or artifact_cifs_root or _first_text(delivery.get("artifact_path"), limit=500)
    artifact_path, artifact_dir = _normalize_html_artifact_path(artifact_source, report_status=report_status if gate_green else "")
    report_case_dir_vm = str(report_ready_truth.get("case_dir_vm") or "").strip()
    report_index_html_vm = str(report_ready_truth.get("index_html_vm") or "").strip()
    if (
        not report_index_html_vm
        and report_status == "html_delivery_ready"
        and host_report_exists
        and artifact_root
    ):
        report_index_html_vm = artifact_root.rstrip("/") + "/index.html"
    report_data_vm = str(report_ready_truth.get("report_data_vm") or "").strip()
    report_viz_mcap_vm = str(report_ready_truth.get("viz_mcap_vm") or "").strip()
    report_foxglove_url = str(report_ready_truth.get("foxglove_url") or "").strip()
    validated_foxglove_url = _validated_foxglove_link(report_foxglove_url, report_viz_mcap_vm)
    attribution_causal_text = str(report_ready_truth.get("attribution_causal_text") or candidate_cause or "").strip()
    report_case_dir_cifs = _perception_test_team_cifs(report_case_dir_vm)
    report_index_html_cifs = _perception_test_team_cifs(report_index_html_vm)
    report_data_cifs = _perception_test_team_cifs(report_data_vm)
    if verified_report_index and not report_ready_truth:
        report_index_html_cifs = verified_report_index
        report_case_dir_cifs = verified_report_index.rsplit("/", 1)[0] + "/"
    report_case_dir_http = _perception_test_team_http(report_case_dir_vm)
    report_index_html_http = _perception_test_team_http(report_index_html_vm)
    canonical_report_from_vm = _canonical_publication_report_url(report_index_html_vm)
    agent_artifact_root_vm = str(report_ready_truth.get("artifact_root_vm") or artifact_root or "").strip()
    agent_artifact_root_cifs = str(report_ready_truth.get("artifact_root_cifs") or artifact_cifs_root or "").strip()
    input_display = str(report_ready_truth.get("input_display") or "").strip()
    input_resolved_display = str(report_ready_truth.get("input_resolved") or "").strip()
    existing_artifact_path = str(delivery.get("artifact_path") or "").strip()
    existing_artifact_link = _extract_markdown_link(existing_artifact_path) or existing_artifact_path
    # Only an explicitly configured canonical HTTPS report origin may become a
    # user-facing HTML link.  The VM HTTP URL remains an internal evidence field;
    # without a valid canonical origin we keep a non-clickable CIFS/VM path so
    # the Publication gate fails closed instead of leaking a private URL.
    existing_report_link = _validated_canonical_report_link(existing_artifact_link)
    canonical_report_link = _validated_canonical_report_link(canonical_report_from_vm) or existing_report_link
    report_delivery_path = (
        validated_foxglove_url
        or canonical_report_link
        or report_index_html_cifs
        or report_index_html_vm
        or _non_publication_fallback(artifact_path)
    )
    publication_url_status = "ready" if canonical_report_link else "blocked_missing_canonical_https"
    report_delivery_root = report_case_dir_cifs or report_case_dir_vm or report_case_dir_http or artifact_dir
    if blocked_keyframe_pipeline:
        report_delivery_path = ""
        report_delivery_root = agent_artifact_root_cifs or agent_artifact_root_vm or artifact_dir
        artifact_path = ""
    real_report = (not blocked_keyframe_pipeline) and (
        bool(report_ready_truth)
        or (
            bool(verified_report_index)
            and report_status == "html_delivery_ready"
            and gate_green
        )
    )
    if real_report:
        # Report-ready cards must describe the report boundary only.  Do not
        # seed from stale intake/blocked card boundaries; those may contain
        # historical need_input/out_of_scope gate text that is no longer true.
        boundaries = []
    else:
        stored_boundaries = delivery.get("boundaries") if isinstance(delivery.get("boundaries"), list) else []
        boundaries = [
            normalized
            for item in stored_boundaries
            if (normalized := _remote_read_user_text(item, limit=300))
        ]
    if host_report_exists and not gate_green and not blocked_keyframe_pipeline:
        boundaries = _append_unique(boundaries, f"命中既有报告草稿，但证据未齐（gate={truth.get('gate_decision') or 'unknown'}），不作为可交付。")
    if blocked_keyframe_pipeline:
        boundaries = _append_unique(
            boundaries,
            f"卡点：{blocked_keyframe_message}",
            "数据已就位，无需重传；需人工指定关键帧或确认 discover_acc_speed_unstable 所需信号是否采集",
        )
        verification = "以 pipeline_result.json 为准；当前无可交付报告，需补关键帧后继续 RCA。"
    else:
        boundaries = _append_unique(
            boundaries,
            evidence_boundary,
            "当前证据尚不足以形成高置信自动归因；需要人工确认候选原因、责任域与证据边界。" if attribution_status in {"hypothesis_ready", "needs_review", "need_review"} or truth.get("anomaly") else "",
        )

    honest_conclusion = _remote_read_user_text(_first_text(truth.get("honest_conclusion"), limit=180), limit=180)
    if blocked_keyframe_pipeline:
        honest_conclusion = (
            f"问题数据已通过远程读取取得（不执行 MDI 下载）；自动找帧无候选（{blocked_keyframe_message}）"
            f"→ 已停在 {blocked_keyframe_stage}，需人工指定关键帧或确认采集信号后继续 RCA"
        )
    delivery.update({
        "conclusion": honest_conclusion if honest_conclusion else (_first_text(delivery.get("conclusion"), notice.get("text"), limit=120) if not report_status else ""),
        "artifact_label": "打开 foxglove 可视化" if (validated_foxglove_url and real_report) else ("打开 HTML 报告" if (canonical_report_link and real_report) else "报告目录"),
        "gate_decision": str(truth.get("gate_decision") or ""),
        "publication_url_status": publication_url_status,
    })
    if blocked_keyframe_pipeline:
        delivery.pop("artifact_path", None)
        delivery.pop("report_index_html_vm", None)
        delivery.pop("report_index_html_cifs", None)
        delivery.pop("report_index_html_http", None)
        delivery["artifact_label"] = "产物目录(暂无报告)"
        delivery["human_action_kind"] = "need_keyframe"
        delivery["business_state"] = "blocked_need_keyframe"
        delivery["presentation_state"] = "blocked"
        delivery["blocker_kind"] = str(blocked_keyframe_blocker.get("kind") or "").strip()
        delivery["cifs_status"] = "暂无报告；数据产物已就位，需补关键帧后生成 RCA 报告"
    if attribution_status:
        delivery["attribution_status"] = attribution_status
    elif business_truth:
        # Authoritative "no attribution" (blocked intake): clear any stale value
        # so the card stops showing 归因状态：hypothesis_ready.
        delivery.pop("attribution_status", None)
    if report_status:
        delivery["report_status"] = report_status
    if candidate_cause:
        delivery["candidate_cause"] = candidate_cause
    if responsibility_candidate:
        delivery["responsibility_candidate"] = responsibility_candidate
    if not blocked_keyframe_pipeline and report_delivery_path:
        delivery["artifact_path"] = report_delivery_path
    elif not blocked_keyframe_pipeline and artifact_path:
        delivery["artifact_path"] = artifact_path
    if report_delivery_root:
        delivery["artifact_root"] = report_delivery_root
    elif artifact_dir:
        delivery["artifact_root"] = artifact_dir
    if agent_artifact_root_vm:
        delivery["agent_artifact_root_vm"] = agent_artifact_root_vm
    if agent_artifact_root_cifs:
        delivery["agent_artifact_root_cifs"] = agent_artifact_root_cifs
    if report_case_dir_vm:
        delivery["business_case_dir_vm"] = report_case_dir_vm
    if report_case_dir_cifs:
        delivery["business_case_dir_cifs"] = report_case_dir_cifs
    if report_index_html_vm:
        delivery["report_index_html_vm"] = report_index_html_vm
    if report_index_html_cifs:
        delivery["report_index_html_cifs"] = report_index_html_cifs
    if report_index_html_http:
        delivery["report_index_html_http"] = report_index_html_http
    if report_case_dir_http:
        delivery["business_case_dir_http"] = report_case_dir_http
    if report_data_vm:
        delivery["report_data_vm"] = report_data_vm
    if report_data_cifs:
        delivery["report_data_cifs"] = report_data_cifs
    if report_viz_mcap_vm and validated_foxglove_url:
        delivery["viz_mcap_vm"] = report_viz_mcap_vm
        delivery["foxglove_url"] = validated_foxglove_url
    if attribution_causal_text:
        delivery["attribution_causal_text"] = attribution_causal_text
    if input_display:
        delivery["input_original"] = input_display
    if input_resolved_display:
        delivery["input_resolved"] = input_resolved_display
    if agent_artifact_root_vm:
        delivery["artifact_vm"] = agent_artifact_root_vm
    display_cifs = report_case_dir_cifs or agent_artifact_root_cifs or _cifs_for_vm_path(agent_artifact_root_vm, agent_artifact_root_cifs)
    if display_cifs:
        delivery["artifact_cifs"] = display_cifs
        if not blocked_keyframe_pipeline:
            delivery["cifs_status"] = "success"
    if boundaries:
        delivery["boundaries"] = boundaries[:5]
    delivery.setdefault("verification", "以 report_data / HTML 产物 / gate_result 为准；终态仍需人工复核业务结论。")
    task_card["delivery"] = delivery

    shared_state = _task_state_from_meta_or_status(task_id, meta) or str(notice.get("state") or "")
    intake_needs_download = (not blocked_keyframe_pipeline) and (not real_report) and (report_status == "need_download" or business_truth or (
        bool(re.search(r"ready_to_download|need_evidence|need_source_or_evidence|requires_download", log_text, re.I))
    ))
    pipeline_blocker = _load_original_pipeline_blocker(task_id) if intake_needs_download else {}

    projection_contract: dict[str, Any] = {}
    if isinstance(delivery_contract, dict):
        projection_contract.update(delivery_contract)
    projection_contract.update({k: v for k, v in delivery.items() if v not in (None, "", [])})
    projection_contract.setdefault("created_at", meta.get("created_at") or task_card.get("created_at"))
    projection_contract.setdefault("updated_at", meta.get("updated_at") or notice.get("generated_at") or notice.get("sent_at"))
    projection_report_truth: dict[str, Any] = dict(truth)
    projection_report_truth.update({
        "real_report": real_report,
        "has_deliverable_report": real_report,
        "report_status": report_status,
        "honest_report_status": report_status,
        "report_ready_truth": report_ready_truth,
        "gate_skip_reason": truth.get("gate_skip_reason") or truth.get("skip_reason") or _nested_get(business_result, "skip_reason", "result.skip_reason"),
        "blocker_message": blocked_keyframe_message,
        "pipeline_blocker": pipeline_blocker or blocked_keyframe_blocker,
        "created_at": meta.get("created_at"),
        "updated_at": meta.get("updated_at") or notice.get("generated_at"),
    })
    if report_ready_truth:
        projection_report_truth.update(report_ready_truth)
    elif verified_report_index and real_report:
        projection_report_truth["index_html_vm"] = verified_report_index
    if blocked_keyframe_pipeline:
        projection_report_truth["report_status"] = "need_keyframe"
        projection_report_truth["honest_report_status"] = "need_keyframe"
    if truth.get("anomaly"):
        projection_report_truth["anomaly"] = True
    projection = derive_presentation(shared_state, projection_contract, projection_report_truth, log_text, body.get("vm_bridge") if isinstance(body.get("vm_bridge"), dict) else {})
    task_card["presentation"] = projection

    delivery["conclusion"] = str(projection.get("conclusion") or delivery.get("conclusion") or "")
    lane = str(projection.get("lane") or "")
    delivery["human_action_kind"] = str(projection.get("human_action_kind") or "none")
    delivery["action_category"] = str(projection.get("action_category") or "none")
    delivery["presentation_state"] = "blocked" if lane.startswith("blocked_") or lane == "need_evidence" else lane
    delivery["requires_user_input"] = bool(projection.get("requires_user_input"))
    if projection.get("missing_reason"):
        delivery["missing_reason"] = str(projection.get("missing_reason") or "")
    if projection.get("report_status"):
        delivery["report_status"] = str(projection.get("report_status") or "")
        report_status = str(projection.get("report_status") or "")
    if projection.get("artifact_label"):
        delivery["artifact_label"] = str(projection.get("artifact_label") or "")
    if projection.get("cifs_status"):
        delivery["cifs_status"] = str(projection.get("cifs_status") or "")
    if not projection.get("has_deliverable_report"):
        # A CIFS directory can exist for raw/intermediate artifacts; it is not a
        # report-delivery success unless the projection has proven a real report.
        if str(delivery.get("cifs_status") or "").strip().lower() in {"success", "ok", "succeeded", "done", "ready"}:
            delivery["cifs_status"] = "暂无报告；工程侧修复远程读取/解析链路后继续 RCA（不执行 MDI 下载）" if str(delivery.get("report_status") or "") == "need_pipeline_fix" else "暂无报告；需补充数据/证据后生成 RCA 报告"
        if str(delivery.get("artifact_label") or "") == "报告目录":
            delivery["artifact_label"] = "产物目录(暂无报告)"
    task_card["delivery"] = delivery
    task_card["status_line"] = str(projection.get("status_line") or task_card.get("status_line") or "")
    task_card["user_state"] = str(projection.get("user_state") or task_card.get("user_state") or "running")
    # Clear stale need-input guard markers only after the projection proves a real report.
    if projection.get("has_deliverable_report") and str(task_card.get("close_loop_guard_reason") or "").startswith("g1q3_rca close-loop guard"):
        task_card.pop("close_loop_guard_state", None)
        task_card.pop("close_loop_guard_reason", None)

    existing_milestones = task_card.get("milestones") if isinstance(task_card.get("milestones"), list) else []
    projected_milestones = projection.get("milestones") if isinstance(projection.get("milestones"), list) else []
    milestones = sanitize_milestones([*existing_milestones, *projected_milestones], projection)
    task_card["milestones"] = _trim_milestones(milestones)

    diagnostics = task_card.get("diagnostics") if isinstance(task_card.get("diagnostics"), dict) else {}
    diagnostics["shared_state"] = str(projection.get("diagnostic_state") or shared_state or "")
    if attribution_status and projection.get("has_deliverable_report"):
        diagnostics["attribution_status"] = attribution_status
    elif business_truth:
        diagnostics.pop("attribution_status", None)
    if report_status:
        diagnostics["report_status"] = report_status
    if blocked_keyframe_pipeline:
        diagnostics["blocker_kind"] = str(blocked_keyframe_blocker.get("kind") or "")
    if pipeline_blocker:
        diagnostics["download_blocker"] = pipeline_blocker
        if pipeline_blocker.get("kind"):
            diagnostics["download_blocker_kind"] = str(pipeline_blocker.get("kind") or "")
    authoritative_pipeline_blocker = (
        pipeline_result_truth.get("blocker")
        if isinstance(pipeline_result_truth.get("blocker"), dict)
        else {}
    )
    if authoritative_pipeline_blocker:
        diagnostics["pipeline_blocker"] = dict(authoritative_pipeline_blocker)
        if authoritative_pipeline_blocker.get("kind"):
            diagnostics["blocker_kind"] = str(authoritative_pipeline_blocker.get("kind") or "")
    if truth.get("anomaly"):
        diagnostics["anomaly"] = True
        diagnostics["anomaly_reasons"] = truth.get("anomaly_reasons") or []
    if re.search(r"Existing RCA HTML report|existing HTML RCA report|既有", log_text, re.I):
        diagnostics["key_decision"] = "reused_existing_report" if gate_green else "existing_report_draft_not_deliverable"
    diagnostics["blocker"] = str(projection.get("blocker") or "无") or "无"
    task_card["diagnostics"] = diagnostics

    body["task_card"] = task_card
    return body


def _update_shared_state_for_close_loop(task_id: str, *, state: str, summary: str, status_text: str) -> dict[str, Any]:
    script = _shared_state_root() / "bin" / "update_task_state.py"
    if not script.exists():
        return {"success": False, "error": f"update_task_state.py not found: {script}"}
    cmd = [
        sys.executable,
        str(script),
        "--root",
        str(_shared_state_root()),
        "--task-id",
        task_id,
        "--state",
        state,
        "--summary",
        summary,
        "--status-text",
        status_text,
        "--json",
    ]
    try:
        proc = subprocess.run(cmd, text=True, capture_output=True, timeout=60)
    except Exception as exc:
        return {"success": False, "error": f"{type(exc).__name__}: {exc}"}
    if proc.returncode != 0:
        return {"success": False, "error": (proc.stderr or proc.stdout or f"rc={proc.returncode}").strip()}
    try:
        payload = json.loads(proc.stdout or "{}")
    except Exception:
        payload = {"raw": proc.stdout}
    payload["success"] = True
    return payload


def apply_integration_tools_close_loop_guard(
    task_id: str,
    path: Path,
    body: dict[str, Any],
    *,
    now_ts: float | None = None,
    intake_stale_seconds: int = INTEGRATION_TOOLS_DEFAULT_INTAKE_STALE_SECONDS,
    need_input_stale_seconds: int = INTEGRATION_TOOLS_DEFAULT_NEED_INPUT_STALE_SECONDS,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Advance stale integration-tools intake cards so they cannot go silent.

    This is the relay-side close-loop safety net. The gateway/VM worker are still
    the normal state writers; the relay only acts when a business intake card has
    stayed in an intermediate state beyond its declared SLA and no completion
    notice has arrived.
    """
    task_card = body.get("task_card") if isinstance(body.get("task_card"), dict) else None
    if not task_id or not isinstance(task_card, dict):
        return body, None
    if task_card.get("close_loop_guard_applied_at") and str(task_card.get("user_state") or "") in {"done", "abandoned"}:
        return body, None
    meta = _load_shared_state_meta(task_id)
    if str(meta.get("business_line") or "") != "integration_tools":
        return body, None
    state = _task_state_from_meta_or_status(task_id, meta).strip().lower()
    if state not in {"intake_checked", "need_input", "blocked"}:
        return body, None
    delivery = task_card.get("delivery") if isinstance(task_card.get("delivery"), dict) else {}
    if str(delivery.get("conclusion") or "").strip() and str(task_card.get("user_state") or "") in {"done", "completed", "abandoned"}:
        return body, None
    age = _task_age_seconds(meta, body, task_card, now_ts=now_ts)
    if age is None:
        return body, None
    now_iso = _now_iso()
    milestones = task_card.get("milestones") if isinstance(task_card.get("milestones"), list) else []
    boundaries = delivery.get("boundaries") if isinstance(delivery.get("boundaries"), list) else []
    action: dict[str, Any]
    if state == "intake_checked" and age >= max(1, intake_stale_seconds):
        answer_only = _looks_answer_only_integration_tools_request(task_id)
        if answer_only:
            new_state = "closed"
            user_state = "done"
            status_line = "已收口：这条请求已按答疑处理，没有进入真实执行。"
            conclusion = "已按工具知识页/治理规则完成答疑；本次不再保留长程执行任务。"
            boundary = "如需真实执行，请在原话题补充 mcap 绝对路径、期望任务名/输出和验收口径。"
            milestone = "闭环兜底：答疑类误建任务已关闭"
        else:
            new_state = "need_input"
            user_state = "awaiting_user"
            status_line = "需要补充/确认：接单后 10 分钟内未推进到执行或直接结论，先转为 need_input 防止静默。"
            conclusion = "当前信息不足以安全进入执行或交付；请补充输入路径、目标动作、输出要求和验收人。"
            boundary = "回复同一话题即可继续；未确认前不会启动 VM/MCAP/业务脚本。"
            milestone = "闭环兜底：超时未推进，转 need_input"
        summary = f"integration_tools close-loop guard: {state} -> {new_state}"
        status_text = f"{summary}\n\n{status_line}\n{conclusion}\n"
        shared_state_result = _update_shared_state_for_close_loop(task_id, state=new_state, summary=summary, status_text=status_text)
        task_card["user_state"] = user_state
        task_card["status_line"] = status_line
        delivery["conclusion"] = conclusion
        delivery["boundaries"] = _append_unique(boundaries, boundary, "系统闭环兜底：业务 intake 不允许长期停留在 intake_checked。")
        task_card["delivery"] = delivery
        if not any(isinstance(item, dict) and item.get("label") == milestone for item in milestones):
            milestones.append({"ts": now_iso, "label": milestone})
        task_card["milestones"] = milestones
        task_card["close_loop_guard_applied_at"] = now_iso
        task_card["close_loop_guard_state"] = new_state
        task_card["close_loop_guard_reason"] = summary
        task_card["close_loop_guard_shared_state_result"] = shared_state_result
        body["task_card"] = task_card
        body["updated_at"] = now_iso
        _atomic_write_json(path, body)
        action = {"applied": True, "from_state": state, "to_state": new_state, "answer_only": answer_only, "age_seconds": age, "shared_state": shared_state_result}
        return body, action
    if state in {"need_input", "blocked"} and age >= max(1, need_input_stale_seconds):
        first_timeout_done = state == "blocked" or bool(task_card.get("need_input_first_timeout_at")) or str(task_card.get("close_loop_guard_state") or "") == "blocked"
        if first_timeout_done:
            applied_age = None
            try:
                applied_ts = _parse_iso_ts(task_card.get("close_loop_guard_applied_at"))
                if applied_ts is not None:
                    current_ts = _now_epoch() if now_ts is None else now_ts
                    applied_age = max(0.0, current_ts - applied_ts)
            except Exception:
                applied_age = None
            if applied_age is not None and applied_age < max(1, need_input_stale_seconds):
                return body, None
            new_state = "abandoned"
            user_state = "abandoned"
            summary = "integration_tools close-loop guard: need_input blocked -> abandoned"
            status_line = "已暂时关闭：补充提醒后仍未收到继续执行所需输入，本次任务不再占用执行队列。"
            conclusion = "未收到继续执行所需输入；如仍需处理，请在原话题补充信息后重新发起/重开。"
            boundary = "收到同话题补充后可重新 triage；不会自动启动 VM/业务脚本。"
            milestone = "闭环兜底：need_input 二次等待超时，已暂时关闭"
        else:
            new_state = "blocked"
            user_state = "awaiting_user"
            summary = "integration_tools close-loop guard: need_input -> blocked"
            status_line = "等待补充已超时：先降为 blocked，并再次 @发起人补齐信息；暂不关闭任务。"
            conclusion = "当前仍缺继续执行所需输入；请在原话题回复补充，收到后会重新 triage 并尝试推进。"
            boundary = "首次 need_input 超时只降 blocked 并提醒发起人；二次仍无回复才 abandoned。"
            milestone = "闭环兜底：need_input 首次超时，降 blocked 并提醒发起人"
            task_card["need_input_first_timeout_at"] = now_iso
        status_text = f"{summary}\n\n{status_line}\n{conclusion}\n"
        shared_state_result = _update_shared_state_for_close_loop(task_id, state=new_state, summary=summary, status_text=status_text)
        task_card["user_state"] = user_state
        task_card["status_line"] = status_line
        delivery["conclusion"] = conclusion
        delivery["boundaries"] = _append_unique(boundaries, boundary)
        task_card["delivery"] = delivery
        if not any(isinstance(item, dict) and item.get("label") == milestone for item in milestones):
            milestones.append({"ts": now_iso, "label": milestone})
        task_card["milestones"] = milestones
        task_card["close_loop_guard_applied_at"] = now_iso
        task_card["close_loop_guard_state"] = new_state
        task_card["close_loop_guard_reason"] = summary
        task_card["close_loop_guard_shared_state_result"] = shared_state_result
        body["task_card"] = task_card
        body["updated_at"] = now_iso
        _atomic_write_json(path, body)
        return body, {"applied": True, "from_state": state, "to_state": new_state, "age_seconds": age, "first_timeout": not first_timeout_done, "shared_state": shared_state_result}
    return body, None


G1Q3_CLOSE_LOOP_GUARD_MAX_AGE_SECONDS = 6 * 3600


def apply_g1q3_close_loop_guard(
    task_id: str,
    path: Path,
    body: dict[str, Any],
    *,
    now_ts: float | None = None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Park a blocked G1Q3 RCA intake in `blocked` so the originator is pinged.

    A blocked intake (remote reference missing OR Feishu/Meegle read failed;
    historical state name: need_download)
    is imported from the VM as state=completed, which both hides that it needs
    human input and drops it into dispatch/done.  This relay-side guard re-asserts
    state=blocked (an active human-action state) exactly once, so
    maybe_notify_originator @-pings the requester only for true field-missing cases and the task stays
    active/resumable.  user_state is left untouched to avoid a two-writer flap;
    the ping is driven purely by the shared-state `state`.  Idempotent: only flips
    from the VM's binary completed/done, so it fires once per transition.
    """
    task_card = body.get("task_card") if isinstance(body.get("task_card"), dict) else None
    if not task_id or "g1q3-rca" not in str(task_id) or not isinstance(task_card, dict):
        return body, None
    meta = _load_shared_state_meta(task_id)
    if str(meta.get("business_line") or "") not in {"g1q3_rca", "g1q3-rca"}:
        return body, None
    delivery = task_card.get("delivery") if isinstance(task_card.get("delivery"), dict) else {}
    artifact_root = _first_text(meta.get("artifact_root"), delivery.get("artifact_root"), body.get("artifact_root"), limit=500)
    report_data = _load_artifact_json(artifact_root, "report_data.json")
    gate_result = _load_artifact_json(artifact_root, "gate_result.json")
    if _g1q3_report_ready_truth(task_id, artifact_root, report_data, gate_result, _load_g1q3_delivery_contract(task_id, body, artifact_root)):
        return body, None
    if str(delivery.get("report_status") or "").strip() not in {"need_download", "need_user_data"}:
        return body, None
    if not _true_user_data_missing_task(task_id, task_card):
        blocker = _pipeline_blocker_for_task(task_id, task_card)
        kind = str(blocker.get("kind") or "").strip() if isinstance(blocker, dict) else ""
        fault_class = _pipeline_fault_class(task_id, task_card, gate_decision=str(delivery.get("report_status") or ""))
        if _is_pipeline_fix_task(task_id, task_card):
            delivery["report_status"] = "need_pipeline_fix"
            delivery["human_action_kind"] = "none"
            delivery["action_category"] = "none"
            delivery["requires_user_input"] = False
            delivery.pop("missing_reason", None)
            delivery["cifs_status"] = "暂无报告；工程侧修复远程读取/解析链路后继续 RCA（不执行 MDI 下载）"
            delivery["conclusion"] = f"数据已就位，无需发起人补数据；工程侧解析/对齐链路待修（{kind or fault_class or 'pipeline_fix'}）。"
            task_card["delivery"] = delivery
            task_card["close_loop_guard_skipped_reason"] = "pipeline_fix_not_user_data"
            body["task_card"] = task_card
            body["updated_at"] = _now_iso()
            _atomic_write_json(path, body)
            return body, {"applied": False, "skipped": "pipeline_fix_not_user_data", "blocker_kind": kind, "fault_class": fault_class}
        return body, None
    state = _task_state_from_meta_or_status(task_id, meta).strip().lower()
    if state not in {"completed", "done"}:
        return body, None
    # Deploy-safety: only act on a recently-settled intake.  Otherwise a relay
    # restart would retroactively flip every long-settled need_download task to
    # blocked and @-ping originators of days-old tasks (a ping storm).  A freshly
    # blocked intake is processed within seconds, so this never blocks real use.
    _created_ts = _parse_iso_ts(meta.get("created_at")) or _parse_iso_ts(meta.get("updated_at"))
    if _created_ts is not None:
        current_epoch = _now_epoch() if now_ts is None else now_ts
        if (current_epoch - _created_ts) > G1Q3_CLOSE_LOOP_GUARD_MAX_AGE_SECONDS:
            return body, None
    now_iso = _now_iso()
    summary = "g1q3_rca close-loop guard: completed -> blocked (intake 需补充数据/证据)"
    status_line = _remote_read_user_text(
        task_card.get("status_line") or "需补充问题数据/证据后继续 RCA；已 @发起人介入。"
    )
    conclusion = _remote_read_user_text(
        delivery.get("conclusion") or "intake 与准入校验完成；待补充数据/证据后继续 RCA。"
    )
    status_text = f"{summary}\n\n{status_line}\n{conclusion}\n"
    shared_state_result = _update_shared_state_for_close_loop(task_id, state="blocked", summary=summary, status_text=status_text)
    stored_boundaries = delivery.get("boundaries") if isinstance(delivery.get("boundaries"), list) else []
    boundaries = [
        normalized
        for item in stored_boundaries
        if (normalized := _remote_read_user_text(item, limit=300))
    ]
    delivery["boundaries"] = _append_unique(
        boundaries,
        REMOTE_REFERENCE_GUIDANCE
        + " 新建问题单由 Kafka 创建事件自动受理；人工入口仅限固定群（HERMES_RCA_MANUAL_CHAT_IDS 当前启用子集），"
        + "且必须真实 @小助手、明确发送“分析/重跑 + 完整问题单 URL”。普通 URL、未 @ 或私聊仍只读，不创建或重跑任务。"
        + "两种入口共用统一受理、去重、代际控制和远程读取链路；人工触发结果回到原任务话题。未补充前不生成报告。",
    )[:5]
    task_card["delivery"] = delivery
    milestones = task_card.get("milestones") if isinstance(task_card.get("milestones"), list) else []
    milestone = "闭环：转 need_input，已 @发起人补齐数据"
    if not any(isinstance(item, dict) and item.get("label") == milestone for item in milestones):
        milestones.append({"ts": now_iso, "label": milestone})
        task_card["milestones"] = _trim_milestones(milestones)
    task_card["close_loop_guard_applied_at"] = now_iso
    task_card["close_loop_guard_state"] = "blocked"
    task_card["close_loop_guard_reason"] = summary
    task_card["close_loop_guard_shared_state_result"] = shared_state_result
    body["task_card"] = task_card
    body["updated_at"] = now_iso
    _atomic_write_json(path, body)
    return body, {"applied": True, "from_state": state, "to_state": "blocked", "shared_state": shared_state_result}


# --- @originator notification on human-action states -----------------------
# Feishu interactive-card *patches* do not fire a push notification; only a
# freshly-sent message does.  When a task needs the human (need_input /
# awaiting_user / abandoned / open pending_confirms), the status card keeps
# patching silently, and we additionally send ONE fresh text reply that
# @-mentions the task originator so the ping actually reaches them.  Without
# this, the close-loop card degrades into "机器人自言自语".

# Business lines whose originator (meta.requester/user_id) is the right person
# to @ on a human-action state.  P2 generalizes long-running task nudges and
# human-action pings beyond integration_tools; unknown/empty lines still fail
# closed unless the task explicitly carries a requester.
_AT_ORIGINATOR_BUSINESS_LINES = {"integration_tools", "g1q3_rca", "g1q3-rca", "pnc_vm", "vm_task"}

# Shared-state states that mean "we are waiting on the originator".
_HUMAN_ACTION_STATES = {"need_input", "awaiting_user", "blocked"}


def _originator_notify_business_line_allowed(meta: dict[str, Any]) -> bool:
    business_line = str(meta.get("business_line") or "").strip()
    if business_line in _AT_ORIGINATOR_BUSINESS_LINES:
        return True
    # Long-running VM/shared-state tasks commonly predate explicit business-line
    # names but still carry the originator as requester/user_id.  Allow those
    # to receive their own human-action ping; do not widen to anonymous tasks.
    if business_line and (meta.get("requester") or meta.get("user_id")):
        return True
    return False


def _open_pending_confirm_ids(task_card: dict[str, Any]) -> list[str]:
    confirms = task_card.get("pending_confirms") if isinstance(task_card.get("pending_confirms"), list) else []
    return [str(item.get("id") or "confirm") for item in confirms if isinstance(item, dict) and item.get("resolved") is None]


def _transition_marker(meta: dict[str, Any], state: str) -> str:
    """Semantic transition signal for notify dedup.

    Uses state + latest_summary, which change only on a real state-write event,
    NOT meta.updated_at (bumped on every passive upsert).  Keying notify dedup on
    this prevents re-@ing the originator on log appends / passive re-syncs.
    """
    return f"{str(state or '').strip().lower()}|{str(meta.get('latest_summary') or '').strip()}"


def _target_has_thread_anchor(target: str) -> bool:
    """True when a feishu target carries a topic/message anchor (not a bare group).

    _card_target returns "feishu:{chat_id}:{anchor}" with an anchor, or degrades
    to "feishu:{chat_id}" without one.  An @-ping must only go to an anchored
    topic thread, never a bare group.
    """
    parts = str(target or "").split(":", 2)
    return len(parts) == 3 and bool(parts[2].strip())


def _human_action_kind(state: str, user_state: str, pending_ids: list[str]) -> str | None:
    """Classify why a task is waiting on the human, or None if it is not."""
    state = str(state or "").strip().lower()
    user_state = str(user_state or "").strip().lower()
    if state == "abandoned" or user_state == "abandoned":
        # Only ping on abandon if it came from the need_input timeout guard
        # (handled by caller via meta state); generic abandons are not pinged.
        return "abandoned"
    if pending_ids:
        return "confirm"
    if state in _HUMAN_ACTION_STATES or user_state == "awaiting_user":
        return "need_input"
    return None


def _need_input_reason(task_card: dict[str, Any], meta: dict[str, Any]) -> str:
    delivery = task_card.get("delivery") if isinstance(task_card.get("delivery"), dict) else {}
    artifact_root = _first_text(meta.get("artifact_root"), delivery.get("agent_artifact_root_vm"), delivery.get("artifact_vm"), delivery.get("artifact_root"), limit=500)
    pipeline = _blocked_keyframe_pipeline_result(artifact_root, None, {"pipeline_result": task_card.get("pipeline_result") if isinstance(task_card.get("pipeline_result"), dict) else {}})
    if not pipeline and _delivery_is_blocked_keyframe(delivery):
        blocker = task_card.get("diagnostics", {}).get("blocker") if isinstance(task_card.get("diagnostics"), dict) and isinstance(task_card.get("diagnostics", {}).get("blocker"), dict) else {}
        pipeline = {"status": "blocked", "blocker": blocker}
    blocker = pipeline.get("blocker") if isinstance(pipeline.get("blocker"), dict) else {}
    if pipeline and _g1q3_blocked_keyframe_kind(blocker.get("kind")):
        message = str(blocker.get("message") or "自动找帧无候选；需人工补帧").strip()
        work_item_id = str(meta.get("work_item_id") or task_card.get("work_item_id") or "").strip()
        if not work_item_id:
            work_item_id = _work_item_id_for_sidecar(str(task_card.get("task_id") or ""), {"task_card": task_card}, meta)
        case_text = f" case {work_item_id}" if work_item_id else "该 case"
        return f"自动找帧无候选（{message}）。需在{case_text} 上人工指定关键帧或确认 discover_acc_speed_unstable 所需信号是否采集；数据已就位，无需重传。"[:300]
    for candidate in (
        delivery.get("missing_reason"),
        (task_card.get("presentation") or {}).get("missing_reason") if isinstance(task_card.get("presentation"), dict) else "",
        delivery.get("reason"),
        delivery.get("conclusion"),
        meta.get("latest_summary"),
    ):
        raw = str(candidate or "").strip()
        if LEGACY_MDI_COMMAND_RE.search(raw) or (
            "pdcl" in raw.lower()
            and re.search(r"补充|缺失|无效|missing|invalid|(?:下载)?命令", raw, re.I)
        ):
            return REMOTE_REFERENCE_GUIDANCE[:300]
        text = _remote_read_user_text(candidate)
        if text:
            return text[:300]
    return ""


def _originator_notify_pending(task_id: str, body: dict[str, Any]) -> bool:
    """True when a fresh @originator ping is due but not yet sent for this transition.

    Pure (no send, no write) so iter_pending_notices can keep the task as a
    candidate until the ping actually goes out, even if the card hash is synced.
    """
    task_card = body.get("task_card") if isinstance(body.get("task_card"), dict) else None
    if not isinstance(task_card, dict):
        return False
    meta = _load_shared_state_meta(task_id)
    if not _originator_notify_business_line_allowed(meta):
        return False
    state = _task_state_from_meta_or_status(task_id, meta)
    delivery = task_card.get("delivery") if isinstance(task_card.get("delivery"), dict) else {}
    # If card delivery has already been reconciled to a real report, stale
    # shared-state `blocked` must not keep the task in need_input notification.
    if str(delivery.get("report_status") or "").strip() in {"html_delivery_ready", "report_ready", "report_generated_need_review"}:
        return False
    if str(delivery.get("report_status") or "").strip() == "out_of_scope" or str(delivery.get("human_action_kind") or "").strip() == "need_triage":
        return False
    diagnostics = task_card.get("diagnostics") if isinstance(task_card.get("diagnostics"), dict) else {}
    if diagnostics.get("anomaly"):
        return False
    report_status = str(delivery.get("report_status") or "").strip()
    if report_status == "need_pipeline_fix":
        return False
    if report_status == "need_download" and _is_pipeline_fix_task(task_id, task_card):
        return False
    if str(delivery.get("report_status") or "").strip() == "need_download" and _mechanical_download_blocker_kind(task_id, task_card):
        return False
    # An infra/self-healable fault (e.g. a VM translate-workdir PermissionError)
    # is NOT the originator's problem — it is auto-healed/resumed and surfaced to
    # ops. Never @-ping the reporter for it. (Live: 7028467612.)
    if _is_infra_self_healable_task(task_id, task_card):
        return False
    pending_ids = _open_pending_confirm_ids(task_card)
    if str(delivery.get("action_category") or "") == "hard":
        kind = str(delivery.get("human_action_kind") or "need_input")
    else:
        kind = _human_action_kind(state, str(task_card.get("user_state") or ""), pending_ids)
    if kind is None:
        return False
    notify_key = compute_notify_key(
        user_state=str(task_card.get("user_state") or ""),
        transition_marker=_transition_marker(meta, state),
        pending_confirm_ids=pending_ids,
        extra=kind,
    )
    return str(task_card.get("last_notify_key") or "") != notify_key


def backfill_originator_notify_keys(*, task_ids: Iterable[str] | None = None) -> dict[str, Any]:
    """Stamp last_notify_key on existing human-action tasks without sending.

    Run at deploy so the first watcher scan does not retroactively @-ping
    originators of tasks that entered need_input/awaiting_user/abandoned before
    this feature existed — or whose stored key predates a notify-key format
    change.  Stamps any task whose *current* notify-key differs from what is
    stored (i.e. would otherwise ping); idempotent within a key format.
    """
    task_filter = {str(item).strip() for item in (task_ids or []) if str(item).strip()}
    root = get_hermes_home() / "task-state"
    stamped: list[dict[str, Any]] = []
    if not root.exists():
        return {"ok": True, "stamped_count": 0, "stamped": []}
    for path in sorted(root.glob("*.json")):
        task_id = _task_id_from_sidecar_path(path)
        if task_filter and task_id not in task_filter:
            continue
        body = _load_json(path)
        task_card = body.get("task_card") if isinstance(body.get("task_card"), dict) else None
        if not isinstance(task_card, dict):
            continue
        # Re-stamp whenever a ping is currently due (covers no-key AND stale-key
        # after a format change); skip when already in sync.
        if not _originator_notify_pending(task_id, body):
            continue
        meta = _load_shared_state_meta(task_id)
        if not _originator_notify_business_line_allowed(meta):
            continue
        state = _task_state_from_meta_or_status(task_id, meta)
        delivery = task_card.get("delivery") if isinstance(task_card.get("delivery"), dict) else {}
        pending_ids = _open_pending_confirm_ids(task_card)
        if str(delivery.get("action_category") or "") == "hard":
            kind = str(delivery.get("human_action_kind") or "need_input")
        else:
            kind = _human_action_kind(state, str(task_card.get("user_state") or ""), pending_ids)
        if kind is None:
            continue
        notify_key = compute_notify_key(
            user_state=str(task_card.get("user_state") or ""),
            transition_marker=_transition_marker(meta, state),
            pending_confirm_ids=pending_ids,
            extra=kind,
        )
        task_card["last_notify_key"] = notify_key
        task_card["last_notify_backfilled_at"] = _now_iso()
        body["task_card"] = task_card
        _atomic_write_json(path, body)
        stamped.append({"task_id": task_id, "kind": kind, "state": state})
    return {"ok": True, "stamped_count": len(stamped), "stamped": stamped}


def _governance_rca_open_id(task_id: str, meta: dict[str, Any]) -> str:
    """Recover the issue originator open_id from the governance_rca intake JSON.

    The VM-result path never copies the intake ``user_id`` into shared-state
    meta.json, so resolve_originator_open_id(meta) comes back empty and the ping
    degrades to "（未识别到发起人）" (live: 7028467612, which DID carry
    user_id=ou_d1d3...). The governance_rca intake record, written at admission,
    reliably has it. Derive the business slug from the artifact root / task_id and
    read it back.
    """
    slug = ""
    for candidate in (meta.get("artifact_root"), meta.get("artifact_cifs_root")):
        text = str(candidate or "").strip().rstrip("/")
        if text:
            slug = text.rsplit("/", 1)[-1]
            if slug:
                break
    if not slug:
        # Fallback: strip the date/seq prefix and trailing follow-up suffix from
        # the canonical task_id to approximate the business slug.
        m = re.search(r"g1q3[_-]rca[_-].*", str(task_id or ""))
        slug = m.group(0).replace("-", "_") if m else ""
    if not slug:
        return ""
    path = get_hermes_home() / "pnc_agent" / "governance_rca" / f"{slug}.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return ""
    open_id = str((data or {}).get("user_id") or "").strip()
    return open_id if open_id.startswith("ou_") else ""


def _resolve_originator_for_notify(task_id: str, meta: dict[str, Any]) -> str:
    """Robust originator open_id for an @-ping: meta, then governance_rca intake.

    Returns "" only when no real ou_ open_id can be found anywhere, in which case
    the caller must NOT emit an orphan card — it routes to ops instead.
    """
    open_id = resolve_originator_open_id(meta)
    if open_id:
        return open_id
    return _governance_rca_open_id(task_id, meta)


def maybe_notify_originator(
    *,
    task_id: str,
    path: Path,
    body: dict[str, Any],
    meta: dict[str, Any],
    send: bool,
    send_func: Callable[[dict[str, Any]], str] | None = None,
) -> dict[str, Any] | None:
    """Send one fresh @originator text reply per human-action transition.

    Idempotent: keyed by (user_state, meta.updated_at, open confirm ids).  Re-pings
    only when that key changes, so a fresh need_input (even re-triage to the same
    state, which advances meta.updated_at) notifies exactly once and a steady
    waiting state never spams.
    """
    task_card = body.get("task_card") if isinstance(body.get("task_card"), dict) else None
    if not isinstance(task_card, dict):
        return None
    if not _originator_notify_business_line_allowed(meta):
        return None
    state = _task_state_from_meta_or_status(task_id, meta)
    delivery = task_card.get("delivery") if isinstance(task_card.get("delivery"), dict) else {}
    if str(delivery.get("report_status") or "").strip() in {"html_delivery_ready", "report_ready", "report_generated_need_review"}:
        return {"skipped": True, "reason": "report_ready_no_need_input", "kind": "need_input"}
    if str(delivery.get("report_status") or "").strip() == "out_of_scope" or str(delivery.get("human_action_kind") or "").strip() == "need_triage":
        return {"skipped": True, "reason": "not_admissible_no_originator_ping", "kind": "need_triage"}
    diagnostics = task_card.get("diagnostics") if isinstance(task_card.get("diagnostics"), dict) else {}
    if diagnostics.get("anomaly"):
        return {"skipped": True, "reason": "anomaly_notify_handles", "kind": "g1q3_anomaly"}
    report_status = str(delivery.get("report_status") or "").strip()
    if report_status == "need_pipeline_fix":
        return {"skipped": True, "reason": "pipeline_fix_no_originator_ping", "kind": "need_input"}
    if report_status == "need_download" and _is_pipeline_fix_task(task_id, task_card):
        return {"skipped": True, "reason": "pipeline_fix_no_originator_ping", "kind": "need_input"}
    if str(delivery.get("report_status") or "").strip() == "need_download" and _mechanical_download_blocker_kind(task_id, task_card):
        return {"skipped": True, "reason": "mechanical_download_notify_handles", "kind": "need_input"}
    # Infra/self-healable faults never reach the originator — auto-heal + ops.
    if _is_infra_self_healable_task(task_id, task_card):
        return {"skipped": True, "reason": "infra_self_healable_no_originator_ping", "kind": "need_input"}
    pending_ids = _open_pending_confirm_ids(task_card)
    if str(delivery.get("action_category") or "") == "hard":
        kind = str(delivery.get("human_action_kind") or "need_input")
    else:
        kind = _human_action_kind(state, str(task_card.get("user_state") or ""), pending_ids)
    if kind is None:
        return None

    notify_key = compute_notify_key(
        user_state=str(task_card.get("user_state") or ""),
        transition_marker=_transition_marker(meta, state),
        pending_confirm_ids=pending_ids,
        extra=kind,
    )
    if str(task_card.get("last_notify_key") or "") == notify_key:
        return {"skipped": True, "reason": "already_notified", "kind": kind}

    open_id = _resolve_originator_for_notify(task_id, meta)
    mention = build_at_mention(open_id, resolve_display_name(open_id))
    target = _card_target(task_card, body.get("completion_notice") if isinstance(body.get("completion_notice"), dict) else None)
    # Hard rule (run.py:"need_input 回复必须回原话题/thread，不得刷主群"): never @-ping
    # into a bare group without a topic anchor.  _card_target degrades to
    # "feishu:{chat_id}" when no thread/message anchor exists; sending an @ there
    # would flood the main group.  Skip (and persist the key so we do not retry
    # into the group) instead.
    if not _target_has_thread_anchor(target):
        if send:
            task_card["last_notify_key"] = notify_key
            task_card["last_notify_skipped_reason"] = "no_thread_anchor"
            body["task_card"] = task_card
            _atomic_write_json(path, body)
        return {"skipped": True, "reason": "no_thread_anchor", "kind": kind, "target": target}
    # Originator guard: never emit an orphan "（未识别到发起人）" card that pings
    # nobody — that is the "机器人自言自语" degradation. When no real open_id can be
    # resolved (even after the governance_rca fallback), skip the ping and surface
    # to ops so a human can route it manually, instead of self-talking into the
    # thread. (Live: 7028467612.)
    if not open_id:
        if send:
            task_card["last_notify_key"] = notify_key
            task_card["last_notify_skipped_reason"] = "originator_unresolved"
            body["task_card"] = task_card
            _atomic_write_json(path, body)
        return {"skipped": True, "reason": "originator_unresolved", "kind": kind, "target": target}
    message = build_need_input_notify_text(
        mention=mention,
        reason=_need_input_reason(task_card, meta),
        task_id=task_id,
        kind=kind,
    )
    if not send:
        return {"dry_run": True, "kind": kind, "target": target, "open_id": open_id, "has_mention": bool(mention), "notify_key": notify_key, "preview": message[:300]}

    try:
        raw = (send_func or send_message_tool)({"action": "send", "target": target, "message": message})
        try:
            result = json.loads(raw)
        except Exception:
            result = {"raw": raw}
    except Exception as exc:
        result = {"error": f"{type(exc).__name__}: {exc}"}
    ok = isinstance(result, dict) and result.get("success")
    if ok:
        # Persist the key only on success, so a failed ping is retried next pass.
        task_card["last_notify_key"] = notify_key
        task_card["last_notify_at"] = _now_iso()
        task_card.pop("last_notify_error", None)
        body["task_card"] = task_card
        _atomic_write_json(path, body)
    else:
        task_card["last_notify_error"] = str(result.get("error") if isinstance(result, dict) else result)[:300]
        body["task_card"] = task_card
        _atomic_write_json(path, body)
    return {"sent": bool(ok), "kind": kind, "target": target, "open_id": open_id, "has_mention": bool(mention), "notify_key": notify_key, "result": result}



G1Q3_DEFAULT_ISSUE_OWNER_OPEN_ID = os.getenv("HERMES_G1Q3_ISSUE_OWNER_OPEN_ID", "ou_lilinxuan")
G1Q3_DEFAULT_ISSUE_OWNER_NAME = os.getenv("HERMES_G1Q3_ISSUE_OWNER_NAME", "林丽旋")


def _g1q3_anomaly_auto_notify_enabled() -> bool:
    # Fail closed. G1Q3 anomaly notices @ humans and have proven noisy when
    # historical sidecars are rescanned. Card updates remain enabled; explicit
    # text @ requires HERMES_G1Q3_ANOMALY_AUTO_NOTIFY=1.
    return os.getenv("HERMES_G1Q3_ANOMALY_AUTO_NOTIFY", "0").strip().lower() in {"1", "true", "yes", "on"}


def _g1q3_anomaly_notify_pending(task_id: str, body: dict[str, Any]) -> bool:
    task_card = body.get("task_card") if isinstance(body.get("task_card"), dict) else None
    if not isinstance(task_card, dict):
        return False
    if not _g1q3_anomaly_auto_notify_enabled():
        return False
    diagnostics = task_card.get("diagnostics") if isinstance(task_card.get("diagnostics"), dict) else {}
    if not diagnostics.get("anomaly"):
        return False
    meta = _load_shared_state_meta(task_id)
    notify_key = compute_notify_key(
        user_state=str(task_card.get("user_state") or ""),
        transition_marker=_transition_marker(meta, _task_state_from_meta_or_status(task_id, meta)),
        pending_confirm_ids=[],
        extra="g1q3_anomaly",
    )
    return str(task_card.get("last_anomaly_notify_key") or "") != notify_key


def maybe_notify_g1q3_anomaly(
    *,
    task_id: str,
    path: Path,
    body: dict[str, Any],
    meta: dict[str, Any],
    send: bool,
    send_func: Callable[[dict[str, Any]], str] | None = None,
) -> dict[str, Any] | None:
    task_card = body.get("task_card") if isinstance(body.get("task_card"), dict) else None
    if not isinstance(task_card, dict):
        return None
    if not _g1q3_anomaly_auto_notify_enabled():
        return {"skipped": True, "reason": "auto_notify_disabled", "kind": "g1q3_anomaly"}
    diagnostics = task_card.get("diagnostics") if isinstance(task_card.get("diagnostics"), dict) else {}
    if not diagnostics.get("anomaly"):
        return None
    notify_key = compute_notify_key(
        user_state=str(task_card.get("user_state") or ""),
        transition_marker=_transition_marker(meta, _task_state_from_meta_or_status(task_id, meta)),
        pending_confirm_ids=[],
        extra="g1q3_anomaly",
    )
    if str(task_card.get("last_anomaly_notify_key") or "") == notify_key:
        return {"skipped": True, "reason": "already_notified", "kind": "g1q3_anomaly"}
    target = _card_target(task_card, body.get("completion_notice") if isinstance(body.get("completion_notice"), dict) else None)
    if not _target_has_thread_anchor(target):
        if send:
            task_card["last_anomaly_notify_key"] = notify_key
            task_card["last_anomaly_notify_skipped_reason"] = "no_thread_anchor"
            body["task_card"] = task_card
            _atomic_write_json(path, body)
        return {"skipped": True, "reason": "no_thread_anchor", "kind": "g1q3_anomaly", "target": target}
    originator_id = resolve_originator_open_id(meta)
    originator = build_at_mention(originator_id, resolve_display_name(originator_id)) or "@发起人"
    owner_id = str(meta.get("issue_owner_open_id") or meta.get("owner_open_id") or G1Q3_DEFAULT_ISSUE_OWNER_OPEN_ID).strip()
    owner_name = str(meta.get("issue_owner_name") or meta.get("owner_name") or resolve_display_name(owner_id) or G1Q3_DEFAULT_ISSUE_OWNER_NAME).strip()
    owner = build_at_mention(owner_id, owner_name) if owner_id.startswith("ou_") else owner_name
    reason = _first_text(task_card.get("status_line"), (task_card.get("delivery") or {}).get("conclusion") if isinstance(task_card.get("delivery"), dict) else "", limit=260)
    message = f"{originator} {owner} 需人工确认：G1Q3-RCA 播报检测到证据门禁/可交付口径异常；候选原因和责任人仅为低置信假设，暂不作为结论。{reason}\n追踪号 {task_id}"
    if not send:
        return {"dry_run": True, "kind": "g1q3_anomaly", "target": target, "notify_key": notify_key, "preview": message[:300]}
    try:
        raw = (send_func or send_message_tool)({"action": "send", "target": target, "message": message})
        try:
            result = json.loads(raw)
        except Exception:
            result = {"raw": raw}
    except Exception as exc:
        result = {"error": f"{type(exc).__name__}: {exc}"}
    ok = isinstance(result, dict) and result.get("success")
    if ok:
        task_card["last_anomaly_notify_key"] = notify_key
        task_card["last_anomaly_notify_at"] = _now_iso()
        task_card.pop("last_anomaly_notify_error", None)
    else:
        task_card["last_anomaly_notify_error"] = str(result.get("error") if isinstance(result, dict) else result)[:300]
    body["task_card"] = task_card
    _atomic_write_json(path, body)
    return {"sent": bool(ok), "kind": "g1q3_anomaly", "target": target, "notify_key": notify_key, "result": result}



def backfill_g1q3_anomaly_notify_keys(*, task_ids: Iterable[str] | None = None) -> dict[str, Any]:
    """Stamp current G1Q3 anomaly notify keys without sending.

    Used on relay startup so historical false-green/anomaly cards do not all
    receive a retroactive @ ping after a deploy/restart. New anomaly transitions
    still send when their key changes after the process is running.
    """
    task_filter = {str(item).strip() for item in (task_ids or []) if str(item).strip()}
    root = get_hermes_home() / "task-state"
    stamped: list[dict[str, Any]] = []
    if not root.exists():
        return {"ok": True, "stamped_count": 0, "stamped": []}
    for path in sorted(root.glob("*.json")):
        task_id = _task_id_from_sidecar_path(path)
        if task_filter and task_id not in task_filter:
            continue
        if "g1q3-rca" not in task_id:
            continue
        body = _load_json(path)
        # Startup backfill must use the same current anomaly classification as
        # the relay loop.  Historical sidecars often predate diagnostics.anomaly;
        # without this enrichment they are first classified during the watch
        # full-scan and can be misread as fresh, unnotified transitions.
        try:
            body = enrich_g1q3_task_card_delivery(task_id, body)
        except Exception:
            pass
        task_card = body.get("task_card") if isinstance(body.get("task_card"), dict) else None
        if not isinstance(task_card, dict):
            continue
        diagnostics = task_card.get("diagnostics") if isinstance(task_card.get("diagnostics"), dict) else {}
        if not diagnostics.get("anomaly"):
            continue
        meta = _load_shared_state_meta(task_id)
        state = _task_state_from_meta_or_status(task_id, meta)
        notify_key = compute_notify_key(
            user_state=str(task_card.get("user_state") or ""),
            transition_marker=_transition_marker(meta, state),
            pending_confirm_ids=[],
            extra="g1q3_anomaly",
        )
        if str(task_card.get("last_anomaly_notify_key") or "") == notify_key:
            continue
        task_card["last_anomaly_notify_key"] = notify_key
        task_card["last_anomaly_notify_backfilled_at"] = _now_iso()
        body["task_card"] = task_card
        _atomic_write_json(path, body)
        stamped.append({"task_id": task_id, "state": state})
    return {"ok": True, "stamped_count": len(stamped), "stamped": stamped}

def _infra_recovery_notify_enabled() -> bool:
    # Fail closed, like the anomaly notify: an infra/ops alert @-pings the ops
    # owner and must be opt-in (HERMES_PNC_INFRA_ALERT=1). The honest infra card
    # is always synced regardless; this is the extra push-ping to ops.
    return os.getenv("HERMES_PNC_INFRA_ALERT", "0").strip().lower() in {"1", "true", "yes", "on"}


_INFRA_ALERT_OPS_OPEN_ID = os.getenv("HERMES_PNC_INFRA_ALERT_OPS_OPEN_ID", "").strip()
_INFRA_ALERT_OPS_NAME = os.getenv("HERMES_PNC_INFRA_ALERT_OPS_NAME", "运维").strip()


def _infra_recovery_notify_pending(task_id: str, body: dict[str, Any]) -> bool:
    if not _infra_recovery_notify_enabled():
        return False
    task_card = body.get("task_card") if isinstance(body.get("task_card"), dict) else None
    if not isinstance(task_card, dict):
        return False
    if not _is_infra_self_healable_task(task_id, task_card):
        return False
    meta = _load_shared_state_meta(task_id)
    notify_key = compute_notify_key(
        user_state=str(task_card.get("user_state") or ""),
        transition_marker=_transition_marker(meta, _task_state_from_meta_or_status(task_id, meta)),
        pending_confirm_ids=[],
        extra="infra_recovery",
    )
    return str(task_card.get("last_infra_notify_key") or "") != notify_key


def maybe_notify_infra_recovery(
    *,
    task_id: str,
    path: Path,
    body: dict[str, Any],
    meta: dict[str, Any],
    send: bool,
    send_func: Callable[[dict[str, Any]], str] | None = None,
) -> dict[str, Any] | None:
    """Ops-facing alert for an infra/self-healable fault (env-gated, once per
    transition). Routes to the ops owner — NEVER the issue originator — with the
    blocker kind, remediation, and the controlled resume stage, so a fault that
    survived in-process self-heal is loud to the right person without reviving
    a direct legacy pipeline entry point."""
    task_card = body.get("task_card") if isinstance(body.get("task_card"), dict) else None
    if not isinstance(task_card, dict):
        return None
    if not _infra_recovery_notify_enabled():
        return {"skipped": True, "reason": "infra_alert_disabled", "kind": "infra_recovery"}
    if not _is_infra_self_healable_task(task_id, task_card):
        return None
    notify_key = compute_notify_key(
        user_state=str(task_card.get("user_state") or ""),
        transition_marker=_transition_marker(meta, _task_state_from_meta_or_status(task_id, meta)),
        pending_confirm_ids=[],
        extra="infra_recovery",
    )
    if str(task_card.get("last_infra_notify_key") or "") == notify_key:
        return {"skipped": True, "reason": "already_notified", "kind": "infra_recovery"}
    target = _card_target(task_card, body.get("completion_notice") if isinstance(body.get("completion_notice"), dict) else None)
    if not _target_has_thread_anchor(target):
        if send:
            task_card["last_infra_notify_key"] = notify_key
            task_card["last_infra_notify_skipped_reason"] = "no_thread_anchor"
            body["task_card"] = task_card
            _atomic_write_json(path, body)
        return {"skipped": True, "reason": "no_thread_anchor", "kind": "infra_recovery", "target": target}
    blocker = _pipeline_blocker_for_task(task_id, task_card)
    kind = str(blocker.get("kind") or "infra")
    remediation = pnc_fault_taxonomy.remediation_for(blocker) or {}
    resume_stage = str(remediation.get("resume_from_stage") or "s3b_translate")
    detail = str(remediation.get("detail") or "基础设施类阻塞，系统已自动归一+重试")
    ops = build_at_mention(_INFRA_ALERT_OPS_OPEN_ID, _INFRA_ALERT_OPS_NAME) if _INFRA_ALERT_OPS_OPEN_ID.startswith("ou_") else _INFRA_ALERT_OPS_NAME
    message = (
        f"{ops} 基础设施类阻塞（数据已就位，无需发起人补数据）：{kind}；{detail}；"
        f"建议恢复阶段：{resume_stage}。如自愈未恢复，必须按追踪号通过统一 RCA 控制面执行受控重试；"
        f"禁止直接运行旧阶段脚本或下载路径。\n追踪号 {task_id}"
    )
    if not send:
        return {"dry_run": True, "kind": "infra_recovery", "target": target, "notify_key": notify_key, "preview": message[:300]}
    try:
        raw = (send_func or send_message_tool)({"action": "send", "target": target, "message": message})
        try:
            result = json.loads(raw)
        except Exception:
            result = {"raw": raw}
    except Exception as exc:
        result = {"error": f"{type(exc).__name__}: {exc}"}
    ok = isinstance(result, dict) and result.get("success")
    if ok:
        task_card["last_infra_notify_key"] = notify_key
        task_card["last_infra_notify_at"] = _now_iso()
        task_card.pop("last_infra_notify_error", None)
    else:
        task_card["last_infra_notify_error"] = str(result.get("error") if isinstance(result, dict) else result)[:300]
    body["task_card"] = task_card
    _atomic_write_json(path, body)
    return {"sent": bool(ok), "kind": "infra_recovery", "target": target, "notify_key": notify_key, "result": result}


def _task_card_needs_sync(task_card: dict[str, Any] | None) -> bool:
    if not isinstance(task_card, dict):
        return False
    try:
        render_hash = stable_render_hash(render_task_card(task_card))
    except Exception:
        return True
    card_message_id = str(task_card.get("card_message_id") or "").strip()
    last_sent_hash = str(task_card.get("last_sent_hash") or "").strip()
    return not card_message_id or last_sent_hash != render_hash


def enrich_task_card_delivery_contract(task_id: str, body: dict[str, Any]) -> dict[str, Any]:
    """Best-effort P2 delivery contract backfill for task-card rendering only."""
    if not isinstance(body, dict):
        return body
    task_card = body.get("task_card") if isinstance(body.get("task_card"), dict) else None
    if not isinstance(task_card, dict):
        return body
    delivery = task_card.get("delivery") if isinstance(task_card.get("delivery"), dict) else {}
    if not delivery:
        return body
    vm_bridge = body.get("vm_bridge") if isinstance(body.get("vm_bridge"), dict) else {}
    notice = body.get("completion_notice") if isinstance(body.get("completion_notice"), dict) else {}
    artifact_path = str(delivery.get("artifact_path") or notice.get("artifact_path") or "").strip()
    artifact_root = str(delivery.get("artifact_root") or body.get("artifact_root") or "").strip()
    task_dir = str(vm_bridge.get("task_dir") or body.get("task_dir") or "").strip()
    if "input_original" not in delivery:
        value = _first_text(delivery.get("input"), body.get("input_original"), notice.get("input_original"), limit=500)
        delivery["input_original"] = value or "未落地/不适用"
    if "input_resolved" not in delivery:
        value = _first_text(body.get("input_resolved"), notice.get("input_resolved"), limit=500)
        delivery["input_resolved"] = value or "未落地/不适用"
    if "artifact_vm" not in delivery:
        vm_value = ""
        for candidate in (delivery.get("artifact_vm"), artifact_root, artifact_path, task_dir):
            text = str(candidate or "").strip()
            if text.startswith("/"):
                vm_value = text
                break
        delivery["artifact_vm"] = vm_value or "未落地/不适用"
    if "artifact_cifs" not in delivery:
        cifs_value = ""
        for candidate in (delivery.get("artifact_cifs"), artifact_path, artifact_root, body.get("artifact_cifs_root"), notice.get("artifact_cifs")):
            text = str(candidate or "").strip()
            if text.startswith("//"):
                cifs_value = text
                break
        delivery["artifact_cifs"] = cifs_value or "未落地/不适用"
    if "cifs_status" not in delivery:
        delivery["cifs_status"] = "success" if str(delivery.get("artifact_cifs") or "").startswith("//") else "未落地/不适用"
    task_card["delivery"] = delivery
    body["task_card"] = task_card
    return body


def _task_card_delivery_complete(body: dict[str, Any]) -> bool:
    task_card = body.get("task_card") if isinstance(body.get("task_card"), dict) else {}
    delivery = task_card.get("delivery") if isinstance(task_card.get("delivery"), dict) else {}
    conclusion = str(delivery.get("conclusion") or "").strip()
    artifact_path = str(delivery.get("artifact_path") or "").strip()
    attribution_status = str(delivery.get("attribution_status") or "").strip()
    report_status = str(delivery.get("report_status") or "").strip()
    return bool(conclusion and artifact_path and (attribution_status or report_status))


def _card_target(task_card: dict[str, Any], fallback_notice: dict[str, Any] | None = None) -> str:
    notice = fallback_notice if isinstance(fallback_notice, dict) else {}
    chat_id = str(task_card.get("chat_id") or notice.get("chat_id") or "").strip()
    message_id = str(task_card.get("message_id") or notice.get("message_id") or "").strip()
    thread_id = str(task_card.get("thread_id") or notice.get("thread_id") or "").strip()
    anchor = ""
    if message_id.startswith("om_"):
        anchor = message_id
    elif thread_id.startswith("topic:om_"):
        anchor = thread_id.split("topic:", 1)[1]
    elif thread_id.startswith("om_"):
        anchor = thread_id
    return f"feishu:{chat_id}:{anchor}" if anchor else f"feishu:{chat_id}"


_CARD_PERSISTENT_FIELDS = frozenset(
    {
        "card_message_id",
        "last_sent_hash",
        "last_render_hash",
        "last_card_semantic_key",
        "last_update_ts",
        "last_update_observed_at",
        "last_attempt_ts",
        "last_error",
        "card_message_expired_at",
        "last_anomaly_notify_key",
        "last_anomaly_notify_at",
        "last_anomaly_notify_skipped_reason",
        "last_anomaly_notify_error",
        "last_download_notify_key",
        "last_download_notify_at",
        "last_download_notify_skipped_reason",
        "last_download_notify_error",
        "last_infra_notify_key",
        "last_infra_notify_at",
        "last_infra_notify_skipped_reason",
        "last_infra_notify_error",
        "last_notify_key",
        "last_notify_at",
        "last_notify_skipped_reason",
        "last_notify_error",
        "close_loop_guard_state",
        "close_loop_guard_applied_at",
        "close_loop_guard_reason",
        "need_input_first_timeout_at",
        # These fields are written by the task-confirm lock.  A concurrent
        # relay render must not resurrect buttons or erase adjudication state.
        "pending_confirms",
        "rca_conclusion_review",
    }
)
_CARD_LOCK_TIMEOUT_SECONDS = 10.0
_CARD_TERMINAL_STATES = frozenset(
    {"completed", "done", "failed", "cancelled", "canceled", "abandoned", "blocked", "awaiting_user", "needs_fix"}
)


def _normalized_card_state(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")


def _card_semantic_key(
    *,
    task_id: str,
    target: str,
    task_card: dict[str, Any],
    notice: dict[str, Any],
    render_hash: str,
) -> str:
    user_state = _normalized_card_state(task_card.get("user_state")) or "unknown"
    notice_state = _normalized_card_state(notice.get("state"))
    terminal_state = notice_state if notice_state in _CARD_TERMINAL_STATES else (
        user_state if user_state in _CARD_TERMINAL_STATES else "nonterminal"
    )
    material = {
        "schema_version": 1,
        "task_id": str(task_id or "").strip(),
        "target": str(target or "").strip(),
        "user_state": user_state,
        "terminal_state": terminal_state,
        "render_hash": render_hash,
    }
    encoded = json.dumps(material, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "card-semantic:" + hashlib.sha256(encoded).hexdigest()


def _validate_card_lock_binding(fd: int, path: Path, expected: tuple[int, int] | None = None) -> tuple[int, int]:
    opened = os.fstat(fd)
    visible = os.lstat(path)
    if not stat.S_ISREG(opened.st_mode) or not stat.S_ISREG(visible.st_mode):
        raise RuntimeError(f"task-card lock must be a regular file: {path}")
    if opened.st_uid != os.getuid() or visible.st_uid != os.getuid():
        raise RuntimeError(f"task-card lock owner mismatch: {path}")
    if opened.st_nlink != 1 or visible.st_nlink != 1:
        raise RuntimeError(f"task-card lock link count mismatch: {path}")
    if stat.S_IMODE(opened.st_mode) != 0o600 or stat.S_IMODE(visible.st_mode) != 0o600:
        raise RuntimeError(f"task-card lock mode must be 0600: {path}")
    identity = (opened.st_dev, opened.st_ino)
    if identity != (visible.st_dev, visible.st_ino):
        raise RuntimeError(f"task-card lock path/descriptor identity mismatch: {path}")
    if expected is not None and identity != expected:
        raise RuntimeError(f"task-card lock binding changed while held: {path}")
    return identity


class _TaskCardSidecarLock:
    def __init__(self, sidecar_path: Path, *, timeout_seconds: float = _CARD_LOCK_TIMEOUT_SECONDS):
        self.path = sidecar_path.parent / f".{sidecar_path.name}.card.lock"
        self.timeout_seconds = max(0.1, float(timeout_seconds))
        self.fd = -1
        self.identity: tuple[int, int] | None = None
        self.acquired = False

    def __enter__(self):
        nofollow = getattr(os, "O_NOFOLLOW", None)
        if nofollow is None:
            raise RuntimeError("task-card lock requires O_NOFOLLOW")
        flags = os.O_RDWR | os.O_CREAT | nofollow | getattr(os, "O_CLOEXEC", 0)
        self.fd = os.open(self.path, flags, 0o600)
        try:
            os.set_inheritable(self.fd, False)
            self.identity = _validate_card_lock_binding(self.fd, self.path)
            deadline = time.monotonic() + self.timeout_seconds
            while True:
                try:
                    fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    self.acquired = True
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError(f"timed out waiting for task-card lock: {self.path}")
                    time.sleep(0.025)
            _validate_card_lock_binding(self.fd, self.path, self.identity)
            return self
        except Exception:
            if self.acquired:
                fcntl.flock(self.fd, fcntl.LOCK_UN)
            os.close(self.fd)
            self.fd = -1
            self.acquired = False
            raise

    def __exit__(self, exc_type, exc, tb):
        try:
            if self.fd >= 0 and self.identity is not None:
                _validate_card_lock_binding(self.fd, self.path, self.identity)
        finally:
            if self.fd >= 0:
                if self.acquired:
                    fcntl.flock(self.fd, fcntl.LOCK_UN)
                os.close(self.fd)
                self.fd = -1
                self.acquired = False


_DURABLE_CARD_PATCH_PENDING_STATES = frozenset(
    {"pending", "claimed", "retry_wait", "uncertain"}
)


def _resolved_rca_review(task_card: Mapping[str, Any]) -> dict[str, Any] | None:
    raw = task_card.get("rca_conclusion_review")
    if not isinstance(raw, Mapping):
        return None
    value = dict(raw)
    if (
        value.get("schema_version") != RCA_CANDIDATE_REVIEW_SCHEMA_VERSION
        or value.get("kind") != RCA_CANDIDATE_REVIEW_PRESET
    ):
        raise ValueError("rca_card_patch_semantic_resolution_invalid")
    action = str(value.get("action") or "").strip()
    conclusion_state = str(value.get("conclusion_state") or "").strip()
    generation = value.get("generation")
    required = (
        "adjudication_id",
        "business_key",
        "work_item_id",
        "original_effect_key",
        "correction_effect_key",
    )
    if (
        {"recognize": "recognized", "retract": "invalidated"}.get(action)
        != conclusion_state
        or isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation < 1
        or any(not str(value.get(key) or "").strip() for key in required)
    ):
        raise ValueError("rca_card_patch_semantic_resolution_invalid")
    return value


def _materialize_rca_review_card_patch(
    *,
    task_id: str,
    path: Path,
    body: dict[str, Any],
    task_card: dict[str, Any],
    rendered: dict[str, Any],
    render_hash: str,
    semantic_key: str,
    target: str,
    card_message_id: str | None,
    send: bool,
    store_factory: Callable[..., Any] | None,
    write_fence_binding_loader: Callable[[str], Mapping[str, Any]] | None,
) -> tuple[bool, dict[str, Any] | None]:
    """Materialize the semantic correction into one durable card-patch effect.

    The boolean says whether generic card sync must stop.  Once a prior durable
    patch is settled, a later unrelated card render may continue through the
    normal relay path without replaying the semantic correction.
    """

    try:
        review = _resolved_rca_review(task_card)
    except ValueError as exc:
        return True, {
            "skipped": True,
            "reason": str(exc),
            "external_write_attempted": False,
        }
    if review is None or not card_message_id:
        return False, None
    if str(task_card.get("task_id") or "").strip() != str(task_id or "").strip():
        return True, {
            "skipped": True,
            "reason": "rca_card_patch_submission_identity_invalid",
            "external_write_attempted": False,
        }
    parts = target.split(":")
    if (
        len(parts) not in {2, 3}
        or parts[0] != "feishu"
        or not parts[1]
        or (len(parts) == 3 and not parts[2])
    ):
        return True, {
            "skipped": True,
            "reason": "rca_card_patch_target_invalid",
            "external_write_attempted": False,
        }
    chat_id = parts[1]
    thread_id = f"topic:{parts[2]}" if len(parts) == 3 else ""
    target_key = f"feishu_card:{chat_id}:{card_message_id}"
    if not send:
        return True, {
            "dry_run": True,
            "durable_card_patch": True,
            "external_write_attempted": False,
            "render_hash": render_hash,
            "target": target,
            "target_key": target_key,
            "message_id": card_message_id,
        }
    try:
        if write_fence_binding_loader is None:
            task_binding = _load_task_write_fence(task_id)
            live_target = _relay_live_fence_binding(task_binding["write_fence"])
        else:
            live_target = dict(write_fence_binding_loader(task_id))
        if (
            str(live_target.get("chat_id") or "").strip() != chat_id
            or str(live_target.get("thread_target") or "").strip() != thread_id
        ):
            raise ExternalWriteFenceError(
                "external_write_fence_target_mismatch"
            )
    except Exception as exc:
        code = exc.code if isinstance(exc, ExternalWriteFenceError) else str(exc)
        return True, {
            "skipped": True,
            "reason": "durable_card_patch_target_binding_blocked",
            "error_code": str(code or type(exc).__name__)[:120],
            "external_write_attempted": False,
            "target": target,
            "message_id": card_message_id,
        }
    if store_factory is None:
        from gateway.pnc_rca_delivery_store import RcaDeliveryStore

        store_factory = RcaDeliveryStore
    try:
        store = store_factory(_relay_control_db_path(), require_current=True)
        binding = store.card_patch_materialization_binding(
            adjudication_id=str(review.get("adjudication_id") or ""),
            action=str(review.get("action") or ""),
            conclusion_state=str(review.get("conclusion_state") or ""),
            business_key=str(review.get("business_key") or ""),
            submission_key=task_id,
            generation=int(review.get("generation") or 0),
            work_item_id=str(review.get("work_item_id") or ""),
            original_effect_key=str(review.get("original_effect_key") or ""),
            correction_effect_key=str(review.get("correction_effect_key") or ""),
            require_current_activation=False,
        )
        state = store.card_patch_effect_state(
            delivery_id=binding["delivery_id"],
            target_key=target_key,
            adjudication_id=binding["adjudication_id"],
        )
        if state is None:
            binding = store.card_patch_materialization_binding(
                adjudication_id=binding["adjudication_id"],
                action=binding["action"],
                conclusion_state=binding["conclusion_state"],
                business_key=binding["business_key"],
                submission_key=binding["submission_key"],
                generation=binding["generation"],
                work_item_id=binding["work_item_id"],
                original_effect_key=binding["original_effect_key"],
                correction_effect_key=binding["correction_effect_key"],
                require_current_activation=True,
            )
            _effect_key, _payload_sha, payload = build_card_patch_effect(
                delivery_id=binding["delivery_id"],
                project_key=binding["project_key"],
                work_item_type_key=binding["work_item_type_key"],
                work_item_id=binding["work_item_id"],
                business_key=binding["business_key"],
                submission_key=binding["submission_key"],
                generation=binding["generation"],
                adjudication_id=binding["adjudication_id"],
                action=binding["action"],
                conclusion_state=binding["conclusion_state"],
                original_effect_key=binding["original_effect_key"],
                correction_effect_key=binding["correction_effect_key"],
                target_key=target_key,
                target={
                    "schema_version": DELIVERY_TARGET_SCHEMA_VERSION,
                    "platform": "feishu",
                    "chat_id": chat_id,
                    "thread_id": thread_id,
                    "message_id": card_message_id,
                    "submission_key": binding["submission_key"],
                    "output_cap": "L1",
                },
                card_payload=rendered,
            )
            store.enqueue_card_patch_effect(payload=payload)
            state = store.card_patch_effect_state(
                delivery_id=binding["delivery_id"],
                target_key=target_key,
                adjudication_id=binding["adjudication_id"],
            )
    except Exception as exc:
        code = str(exc) or type(exc).__name__
        return True, {
            "skipped": True,
            "reason": (
                "awaiting_correction_effect"
                if code == "delivery_card_patch_correction_not_settled"
                else "durable_card_patch_materialization_blocked"
            ),
            "error_code": code[:120],
            "external_write_attempted": False,
            "target": target,
            "message_id": card_message_id,
        }
    if not isinstance(state, Mapping):
        return True, {
            "skipped": True,
            "reason": "durable_card_patch_state_missing",
            "external_write_attempted": False,
            "target": target,
            "message_id": card_message_id,
        }
    effect_status = str(state.get("status") or "")
    effect_payload = (
        state.get("payload") if isinstance(state.get("payload"), Mapping) else {}
    )
    durable_result = {
        "durable_card_patch": True,
        "effect_key": str(state.get("effect_key") or ""),
        "effect_status": effect_status,
        "external_write_attempted": False,
        "target": target,
        "message_id": card_message_id,
        "render_hash": str(effect_payload.get("render_hash") or ""),
    }
    if effect_status in _DURABLE_CARD_PATCH_PENDING_STATES:
        return True, {
            **durable_result,
            "skipped": True,
            "reason": "durable_card_patch_pending",
        }
    if effect_status == "suppressed" and state.get("write_phase") == "settled":
        observed_at = str(state.get("completed_at") or "").strip() or _now_iso()
        task_card["last_sent_hash"] = render_hash
        task_card["last_render_hash"] = render_hash
        task_card["last_card_semantic_key"] = semantic_key
        task_card["last_update_observed_at"] = observed_at
        task_card["card_message_expired_at"] = (
            task_card.get("card_message_expired_at") or observed_at
        )
        task_card.pop("last_error", None)
        body["task_card"] = task_card
        _atomic_write_json(path, body)
        return True, {
            **durable_result,
            "skipped": True,
            "reason": "card_message_expired",
            "disposition": "suppressed_terminal",
        }
    if effect_status != "succeeded" or state.get("write_phase") != "settled":
        return True, {
            **durable_result,
            "skipped": True,
            "reason": "durable_card_patch_terminal_failure",
        }

    effect_render_hash = str(effect_payload.get("render_hash") or "")
    completed_at = str(state.get("completed_at") or "").strip() or _now_iso()
    task_card["last_sent_hash"] = effect_render_hash
    task_card["last_render_hash"] = effect_render_hash
    task_card["last_update_ts"] = completed_at
    task_card["last_update_observed_at"] = completed_at
    task_card.pop("last_error", None)
    if effect_render_hash == render_hash:
        task_card["last_card_semantic_key"] = semantic_key
    body["task_card"] = task_card
    _atomic_write_json(path, body)
    settled_result = {
        **durable_result,
        "success": True,
        "updated": True,
        "disposition": "durable_effect_settled",
    }
    return effect_render_hash == render_hash, settled_result


def _sync_task_card_unlocked(
    *,
    task_id: str,
    path: Path,
    body: dict[str, Any],
    send: bool,
    send_card_func: Callable[..., dict[str, Any]] | None = None,
    card_patch_store_factory: Callable[..., Any] | None = None,
    card_patch_write_fence_loader: (
        Callable[[str], Mapping[str, Any]] | None
    ) = None,
    throttle_seconds: float = DEFAULT_CARD_UPDATE_THROTTLE_SECONDS,
) -> dict[str, Any] | None:
    task_card = body.get("task_card") if isinstance(body.get("task_card"), dict) else None
    if not task_card:
        return None
    notice = body.get("completion_notice") if isinstance(body.get("completion_notice"), dict) else {}
    rendered = render_task_card(task_card)
    render_hash = stable_render_hash(rendered)
    card_message_id = str(task_card.get("card_message_id") or "").strip() or None
    last_sent_hash = str(task_card.get("last_sent_hash") or "").strip()
    last_semantic_key = str(task_card.get("last_card_semantic_key") or "").strip()
    last_update_ts = _parse_iso_ts(task_card.get("last_update_ts"))
    now_ts = _now_epoch()
    target = _card_target(task_card, notice)
    semantic_key = _card_semantic_key(
        task_id=task_id,
        target=target,
        task_card=task_card,
        notice=notice,
        render_hash=render_hash,
    )
    durable_handled, durable_result = _materialize_rca_review_card_patch(
        task_id=task_id,
        path=path,
        body=body,
        task_card=task_card,
        rendered=rendered,
        render_hash=render_hash,
        semantic_key=semantic_key,
        target=target,
        card_message_id=card_message_id,
        send=send,
        store_factory=card_patch_store_factory,
        write_fence_binding_loader=card_patch_write_fence_loader,
    )
    if durable_handled:
        return durable_result
    if card_message_id and last_sent_hash == render_hash and (not last_semantic_key or last_semantic_key == semantic_key):
        return {
            "skipped": True,
            "reason": "hash_unchanged",
            "disposition": "duplicate_noop",
            "semantic_key": semantic_key,
            "target": target,
            "message_id": card_message_id,
        }
    if card_message_id and throttle_seconds > 0 and last_update_ts is not None and now_ts - last_update_ts < throttle_seconds:
        task_card.pop("last_error", None)
        body["task_card"] = task_card
        _atomic_write_json(path, body)
        return {"skipped": True, "reason": "throttled", "semantic_key": semantic_key, "target": target, "message_id": card_message_id}
    if not send:
        return {"dry_run": True, "target": target, "message_id": card_message_id, "render_hash": render_hash, "semantic_key": semantic_key, "will_update": bool(card_message_id)}
    if send_card_func is None:
        return {"skipped": True, "reason": "no_card_sender", "target": target}
    result = send_card_func(target, rendered, message_id=card_message_id)
    if isinstance(result, dict) and result.get("success"):
        sent_at = _now_iso()
        task_card["card_message_id"] = result.get("message_id") or card_message_id
        task_card["last_sent_hash"] = render_hash
        task_card["last_render_hash"] = render_hash
        task_card["last_card_semantic_key"] = semantic_key
        task_card["last_update_ts"] = sent_at
        task_card["last_update_observed_at"] = sent_at
        task_card.pop("last_error", None)
        body["task_card"] = task_card
        _atomic_write_json(path, body)
        result = dict(result)
        result["target"] = target
        result["semantic_key"] = semantic_key
        result["disposition"] = "recorded"
        if durable_result is not None:
            result["durable_card_patch"] = durable_result
        return result
    error = str((result or {}).get("error") if isinstance(result, dict) else result)
    now_iso = _now_iso()
    task_card["last_error"] = error
    task_card["last_attempt_ts"] = now_iso
    if _is_expired_card_update_error(error):
        # Feishu cards can only be PATCHed for ~14 days.  Retrying the patch and
        # then sending fallback text on every watch loop floods old topics.  Treat
        # the current render as observed/suppressed for idempotency and do NOT
        # fallback-send; future substantive card changes can still create a new
        # card via explicit/manual recovery if needed.
        task_card["last_sent_hash"] = render_hash
        task_card["last_render_hash"] = render_hash
        task_card["last_card_semantic_key"] = semantic_key
        task_card["last_update_observed_at"] = now_iso
        task_card["card_message_expired_at"] = task_card.get("card_message_expired_at") or now_iso
        body["task_card"] = task_card
        _atomic_write_json(path, body)
        return {"skipped": True, "reason": "card_message_expired", "disposition": "suppressed_terminal", "semantic_key": semantic_key, "error": error, "target": target, "message_id": card_message_id}
    body["task_card"] = task_card
    _atomic_write_json(path, body)
    fallback_text = _rca_public_text_without_internal_html(
        str(notice.get("text") or "").strip()[:500], task_card
    )
    if not fallback_text:
        fallback_text = _build_task_card_fallback_text(task_id, task_card, notice, error)
    return {"success": False, "error": error, "target": target, "fallback_text": fallback_text}


def sync_task_card(
    *,
    task_id: str,
    path: Path,
    body: dict[str, Any],
    send: bool,
    send_card_func: Callable[..., dict[str, Any]] | None = None,
    card_patch_store_factory: Callable[..., Any] | None = None,
    card_patch_write_fence_loader: (
        Callable[[str], Mapping[str, Any]] | None
    ) = None,
    throttle_seconds: float = DEFAULT_CARD_UPDATE_THROTTLE_SECONDS,
) -> dict[str, Any] | None:
    desired_card = body.get("task_card") if isinstance(body.get("task_card"), dict) else None
    if not desired_card:
        return None
    with _TaskCardSidecarLock(path):
        latest_body = _load_json(path)
        if not latest_body:
            raise RuntimeError(f"task-card sidecar is missing or invalid: {path}")
        latest_card = latest_body.get("task_card") if isinstance(latest_body.get("task_card"), dict) else {}
        merged_card = dict(desired_card)
        # The sidecar under the lock is the CAS authority. A stale caller may
        # contribute a newly-rendered business card, but it cannot overwrite or
        # resurrect delivery markers from an earlier successful disposition.
        for field in _CARD_PERSISTENT_FIELDS:
            if field in latest_card:
                merged_card[field] = latest_card[field]
            else:
                merged_card.pop(field, None)
        working_body = dict(latest_body)
        working_body["task_card"] = merged_card
        result = _sync_task_card_unlocked(
            task_id=task_id,
            path=path,
            body=working_body,
            send=send,
            send_card_func=send_card_func,
            card_patch_store_factory=card_patch_store_factory,
            card_patch_write_fence_loader=card_patch_write_fence_loader,
            throttle_seconds=throttle_seconds,
        )
        body.clear()
        body.update(working_body)
        return result


def _is_expired_card_update_error(error: str) -> bool:
    text = str(error or "").lower()
    return "230031" in text or "message has expired" in text or "can only be updated within fourteen days" in text


def _task_card_has_rca_delivery(task_card: Mapping[str, Any] | None) -> bool:
    return has_rca_delivery_provenance(task_card)


def _rca_internal_html_reference(
    value: str,
    *,
    protected_foxglove_url: str,
) -> bool:
    return contains_internal_rca_html_reference(
        value,
        protected_foxglove_url=protected_foxglove_url,
    )


def _rca_public_text_without_internal_html(
    value: Any,
    task_card: Mapping[str, Any] | None,
) -> str:
    text = str(value or "").strip()
    if not text or not _task_card_has_rca_delivery(task_card):
        return text
    card = task_card if isinstance(task_card, Mapping) else {}
    delivery = card.get("delivery") if isinstance(card.get("delivery"), Mapping) else {}
    protected_foxglove_url = _validated_foxglove_link(
        delivery.get("foxglove_url"), delivery.get("viz_mcap_vm")
    )
    safe_lines = []
    for line in text.splitlines():
        lowered = line.lower()
        if _rca_internal_html_reference(
            line,
            protected_foxglove_url=protected_foxglove_url,
        ):
            replacement = (
                "报告链接：内部 HTML 审计产物已隐藏；公开交付仅使用已验证的 Foxglove 链接。"
                if "报告链接" in line
                else "html_url：内部 HTML 审计产物已隐藏；公开交付仅使用已验证的 Foxglove 链接。"
                if "html_url" in lowered
                else "artifact: 内部 HTML 审计产物已隐藏；公开交付仅使用已验证的 Foxglove 链接。"
                if "artifact" in lowered
                else ""
            )
            if replacement:
                safe_lines.append(replacement)
            continue
        safe_lines.append(line)
    return "\n".join(safe_lines).strip()


def _build_task_card_fallback_text(task_id: str, task_card: dict[str, Any] | None, notice: dict[str, Any] | None, error: str) -> str:
    card = task_card if isinstance(task_card, dict) else {}
    delivery = card.get("delivery") if isinstance(card.get("delivery"), dict) else {}
    status_line = str(card.get("status_line") or card.get("user_state") or "进度更新").strip()
    raw_artifact_path = str(delivery.get("artifact_path") or "").strip()
    artifact_path = raw_artifact_path
    if _task_card_has_rca_delivery(card):
        foxglove_url = _validated_foxglove_link(
            delivery.get("foxglove_url"), delivery.get("viz_mcap_vm")
        )
        artifact_path = foxglove_url or _rca_public_text_without_internal_html(
            raw_artifact_path, card
        )
    conclusion = str(delivery.get("conclusion") or "").strip()
    lines = [
        CARD_FALLBACK_PREFIX,
        "任务进度卡片更新失败，先用文本降级同步，避免静默丢失。",
        f"task_id: {task_id}",
        f"status: {status_line}",
    ]
    if conclusion:
        lines.append(f"conclusion: {conclusion}")
    if artifact_path:
        lines.append(f"artifact: {artifact_path}")
    elif raw_artifact_path and _task_card_has_rca_delivery(card):
        lines.append("artifact: 内部 HTML 审计产物已隐藏；公开交付仅使用已验证的 Foxglove 链接。")
    if error:
        lines.append(f"card_error: {error[:300]}")
    return _rca_public_text_without_internal_html("\n".join(lines), card)

def _feishu_target(notice: dict[str, Any]) -> str:
    chat_id = str(notice.get("chat_id") or "").strip()
    message_id = str(notice.get("message_id") or "").strip()
    thread_id = str(notice.get("thread_id") or "").strip()
    anchor = ""
    if message_id.startswith("om_"):
        anchor = message_id
    elif thread_id.startswith("topic:om_"):
        anchor = thread_id.split("topic:", 1)[1]
    elif thread_id.startswith("om_"):
        anchor = thread_id
    return f"feishu:{chat_id}:{anchor}" if anchor else f"feishu:{chat_id}"



def _completion_notice_text_allowed(notice: dict[str, Any]) -> tuple[bool, str]:
    category = str(notice.get("category") or notice.get("kind") or "").strip().lower()
    if category in {"delivery", "deliverable", "failed", "failure", "awaiting_user", "confirmation", "confirm"}:
        return True, f"category={category}"
    state = str(notice.get("state") or "").strip().lower()
    # Old-format notices did not carry state/category; preserve compatibility.
    if not state:
        return True, "legacy_no_state"
    if state in {"completed", "failed", "cancelled", "abandoned", "blocked", "awaiting_user"}:
        return True, f"state={state}"
    return False, f"process_state={state}"


TERMINAL_DELIVERY_STATES = {"completed", "failed", "blocked", "needs_fix", "awaiting_user"}


def _completion_delivery_contract(body: dict[str, Any], notice: dict[str, Any]) -> dict[str, Any]:
    for source in (
        notice.get("completion_delivery") if isinstance(notice.get("completion_delivery"), dict) else None,
        body.get("completion_delivery") if isinstance(body.get("completion_delivery"), dict) else None,
        (body.get("task_card") or {}).get("completion_delivery")
        if isinstance(body.get("task_card"), dict) and isinstance((body.get("task_card") or {}).get("completion_delivery"), dict)
        else None,
    ):
        if isinstance(source, dict):
            return source
    return {}


def _completion_delivery_required(body: dict[str, Any], notice: dict[str, Any]) -> bool:
    return bool(_completion_delivery_contract(body, notice).get("required") is True)


def _completion_delivery_sent(notice: dict[str, Any]) -> bool:
    if bool(notice.get("delivery_sent")):
        return True
    marker = notice.get("delivery_sent_marker") if isinstance(notice.get("delivery_sent_marker"), dict) else {}
    return bool(marker.get("sent_at") or marker.get("message_id"))


def _completion_delivery_event_ts(notice: dict[str, Any]) -> float | None:
    candidates = [
        notice.get("generated_at"),
        notice.get("suppressed_at"),
        notice.get("created_at"),
        notice.get("updated_at"),
    ]
    parsed = [_parse_iso_ts(item) for item in candidates]
    parsed = [item for item in parsed if item is not None]
    return max(parsed) if parsed else None


def _completion_delivery_is_new_enough(notice: dict[str, Any], *, since_ts: float | None) -> bool:
    if since_ts is None:
        return False
    event_ts = _completion_delivery_event_ts(notice)
    return event_ts is not None and event_ts >= since_ts


def _completion_delivery_send_required(
    body: dict[str, Any],
    notice: dict[str, Any],
    *,
    explicit_task_filter: bool = False,
    since_ts: float | None = None,
) -> tuple[bool, str]:
    if not _completion_delivery_required(body, notice):
        return False, "no_completion_delivery"
    state = str(notice.get("state") or "").strip().lower()
    if state not in TERMINAL_DELIVERY_STATES:
        return False, f"state={state or 'unknown'}"
    if _completion_delivery_sent(notice):
        return False, "delivery_sent"
    if explicit_task_filter:
        return True, "completion_delivery_required_explicit"
    if _completion_delivery_is_new_enough(notice, since_ts=since_ts):
        return True, "completion_delivery_required_new"
    return False, "completion_delivery_historical_requires_explicit"


def _notice_is_relayable_for_completion_delivery(
    body: dict[str, Any],
    notice: dict[str, Any],
    *,
    explicit_task_filter: bool = False,
    since_ts: float | None = None,
) -> bool:
    if not notice:
        return False
    status = str(notice.get("send_status") or "").strip().lower()
    if status not in {"pending", "suppressed"}:
        return False
    if status == "suppressed" and str(notice.get("suppress_reason") or "") != "one_task_one_card":
        return False
    required, _reason = _completion_delivery_send_required(
        body,
        notice,
        explicit_task_filter=explicit_task_filter,
        since_ts=since_ts,
    )
    return required


def _text_with_completion_must_carry(text: str, body: dict[str, Any], notice: dict[str, Any]) -> str:
    contract = _completion_delivery_contract(body, notice)
    must = [str(item).strip() for item in (contract.get("must_carry") or []) if str(item).strip()]
    if not must:
        return text
    lowered = text.lower()
    additions: list[str] = []

    def has_key(key: str) -> bool:
        aliases = {
            "conclusion": ("conclusion", "结论"),
            "cause": ("cause", "原因", "边界"),
            "fixed_state": ("fixed_state", "修复状态", "状态"),
            "html_url": ("html_url", "报告链接", "路径", "http://", "https://", "file://", "//hfs"),
            "verification": ("verification", "验证", "验收"),
        }.get(key, (key,))
        return any(alias.lower() in lowered for alias in aliases)

    if "conclusion" in must and not has_key("conclusion"):
        additions.append("conclusion：见上方终态结论。")
    if "cause" in must and not has_key("cause"):
        additions.append("cause：以报告/receipt 中的因果链与边界为准；未登记时需人工复核。")
    task_card = body.get("task_card") if isinstance(body.get("task_card"), dict) else {}
    delivery = task_card.get("delivery") if isinstance(task_card.get("delivery"), dict) else {}
    blocked_keyframe = _delivery_is_blocked_keyframe(delivery)
    if "fixed_state" in must and not has_key("fixed_state"):
        additions.append(f"fixed_state：{'blocked/need_keyframe' if blocked_keyframe else str(notice.get('state') or 'unknown')}")
    if "html_url" in must and not has_key("html_url"):
        artifact = ""
        if not blocked_keyframe:
            for item in body.get("artifacts") or []:
                value = str(item or "")
                if "//hfs" in value or "http" in value or "index.html" in value:
                    artifact = value.replace("CIFS: ", "").replace("VM: ", "")
                    break
        # Only surface a link that actually resolves to a file/URL; a bare
        # CIFS/VM path that does not exist is a dead link.
        if artifact and _artifact_link_is_live(artifact):
            additions.append("html_url：" + artifact)
        else:
            additions.append("html_url：本次未生成可交付报告；详见 task artifacts / receipt。")
    if "verification" in must and not has_key("verification"):
        additions.append("verification：已到达 worker 终态；最终业务结论以报告证据与人工复核为准。")
    if not additions:
        return text
    return text.rstrip() + "\n" + "\n".join(additions)




def _merge_task_card_persistent_fields(current: dict[str, Any], previous: dict[str, Any]) -> dict[str, Any]:
    """Preserve side-effect markers across stale in-memory card writes.

    relay processing may send an @ notification (which writes last_*_notify_key)
    and then later suppress/mark completion text using the older body object.
    Without merging, the later write drops the notify key and the next watch
    cycle sends the same @ again.
    """
    if not isinstance(current, dict) or not isinstance(previous, dict):
        return current
    for key in _CARD_PERSISTENT_FIELDS:
        # task-confirm owns these two values under its own flock.  Always take
        # the on-disk value so a stale relay body cannot resurrect a resolved
        # action or erase the immutable adjudication projection.
        task_confirm_owned = key in {"pending_confirms", "rca_conclusion_review"}
        if key in previous and (task_confirm_owned or key not in current):
            current[key] = previous[key]
    return current


def _atomic_write_json_preserving_task_card_markers(path: Path, body: dict[str, Any]) -> None:
    try:
        previous = _load_json(path)
    except Exception:
        previous = {}
    if isinstance(body.get("task_card"), dict) and isinstance(previous.get("task_card"), dict):
        body["task_card"] = _merge_task_card_persistent_fields(body["task_card"], previous["task_card"])
    _atomic_write_json(path, body)

def _mark_delivery_sent(path: Path, body: dict[str, Any], *, result: dict[str, Any] | None = None) -> dict[str, Any]:
    notice = body.get("completion_notice") if isinstance(body.get("completion_notice"), dict) else {}
    now = _now_iso()
    notice["send_status"] = "sent"
    notice["delivery_sent"] = True
    notice["delivery_sent_at"] = now
    notice["sent_at"] = now
    notice["attempt_count"] = int(notice.get("attempt_count") or 0) + 1
    marker = {"sent_at": now, "reason": "completion_delivery_required"}
    if isinstance(result, dict):
        notice["send_result"] = result
        if result.get("message_id"):
            marker["message_id"] = result.get("message_id")
    notice["delivery_sent_marker"] = marker
    notice.pop("suppress_reason", None)
    notice.pop("suppressed_at", None)
    notice.pop("send_error", None)
    body["completion_notice"] = notice
    _atomic_write_json_preserving_task_card_markers(path, body)
    return notice


def _mark_text_suppressed(path: Path, body: dict[str, Any], *, reason: str) -> dict[str, Any]:
    notice = body.get("completion_notice") if isinstance(body.get("completion_notice"), dict) else {}
    notice["send_status"] = "suppressed"
    notice["suppressed_at"] = _now_iso()
    notice["suppress_reason"] = reason
    body["completion_notice"] = notice
    _atomic_write_json_preserving_task_card_markers(path, body)
    return notice

def _mark(path: Path, body: dict[str, Any], *, status: str, result: dict[str, Any] | None = None, error: str | None = None) -> dict[str, Any]:
    notice = body.get("completion_notice") if isinstance(body.get("completion_notice"), dict) else {}
    notice["send_status"] = status
    notice["attempt_count"] = int(notice.get("attempt_count") or 0) + 1
    notice["sent_at" if status == "sent" else "last_attempt_at"] = _now_iso()
    if status == "sent":
        notice.pop("send_error", None)
    if result is not None:
        notice["send_result"] = result
    if error:
        notice["send_error"] = error
    body["completion_notice"] = notice
    _atomic_write_json_preserving_task_card_markers(path, body)
    return notice


def _home_alert_target() -> str | None:
    chat_id = os.getenv("FEISHU_HOME_CHANNEL", "").strip()
    return f"feishu:{chat_id}" if chat_id else None


def _build_failed_alert(task_id: str, notice: dict[str, Any], error: str) -> str:
    vm_task_id = str(notice.get("vm_task_id") or "").strip() or "unknown"
    chat_id = str(notice.get("chat_id") or "").strip() or "unknown"
    attempts = int(notice.get("attempt_count") or 0)
    return "\n".join([
        FAILED_ALERT_PREFIX,
        f"task_id: {task_id}",
        f"vm_task_id: {vm_task_id}",
        f"chat_id: {chat_id}",
        f"attempt_count: {attempts}",
        f"error: {error or 'unknown'}",
    ])


def maybe_alert_failed_notice(path: Path, body: dict[str, Any], *, task_id: str, notice: dict[str, Any], error: str, max_attempts: int, send_func: Callable[[dict[str, Any]], str] | None = None) -> dict[str, Any] | None:
    if int(notice.get("attempt_count") or 0) < max_attempts:
        return None
    if notice.get("alert_sent_at"):
        return {"skipped": True, "reason": "alert_already_sent", "alert_sent_at": notice.get("alert_sent_at")}
    target = _home_alert_target()
    if not target:
        return {"skipped": True, "reason": "FEISHU_HOME_CHANNEL not configured"}
    raw = (send_func or send_message_tool)({"action": "send", "target": target, "message": _build_failed_alert(task_id, notice, error)})
    try:
        result = json.loads(raw)
    except Exception:
        result = {"raw": raw}
    if isinstance(result, dict) and result.get("success"):
        notice["alert_sent_at"] = _now_iso()
        notice["alert_result"] = result
        body["completion_notice"] = notice
        _atomic_write_json(path, body)
    return result if isinstance(result, dict) else {"raw": raw}


def _record_only_task_senders(
    record_sender: Any,
    *,
    task_id: str,
    body: dict[str, Any],
    notice: dict[str, Any],
) -> tuple[Callable[[dict[str, Any]], str], Callable[..., dict[str, Any]]]:
    """Bind record-only sends to one task without changing live sender APIs."""
    task_card = body.get("task_card") if isinstance(body.get("task_card"), dict) else {}
    terminal_state = str(notice.get("state") or task_card.get("user_state") or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", terminal_state):
        terminal_state = ""

    def _dedupe_key(kind: str, target: str, payload: Any, message_id: str | None = None) -> str:
        material = json.dumps(
            {
                "kind": kind,
                "message_id": message_id,
                "payload": payload,
                "target": target,
                "task_id": task_id,
                "terminal_state": terminal_state or None,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return f"relay:{kind}:{hashlib.sha256(material).hexdigest()}"

    def _send_text(args: dict[str, Any]) -> str:
        bound = dict(args)
        target = str(bound.get("target") or "")
        message = bound.get("message")
        bound["task_id"] = task_id
        if terminal_state:
            bound["terminal_state"] = terminal_state
        else:
            bound.pop("terminal_state", None)
        bound["dedupe_key"] = _dedupe_key("text", target, message)
        return record_sender.send(bound)

    def _send_card(
        target: str,
        card_payload: dict[str, Any],
        message_id: str | None = None,
    ) -> dict[str, Any]:
        return record_sender.send_task_card(
            target,
            card_payload,
            message_id=message_id,
            task_id=task_id,
            terminal_state=terminal_state or None,
            dedupe_key=_dedupe_key("card", target, card_payload, message_id),
        )

    return _send_text, _send_card


def _relay_control_db_path() -> Path:
    configured = os.getenv("HERMES_RCA_CONTROL_DB_PATH", "").strip()
    if configured:
        return Path(configured).expanduser()
    return (
        Path(get_hermes_home()).expanduser()
        / "runtime"
        / "pnc_agent"
        / "feishu_issue_kafka_rca"
        / "control.sqlite3"
    )


def ensure_rca_candidate_review_confirm(
    *,
    task_id: str,
    path: Path,
    body: dict[str, Any],
    store_factory: Callable[..., Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Attach one review card only after the delivery DB proves candidacy.

    Card fields are intentionally ignored for qualification.  The read-only
    queue recomputes the structural medium tier from the settled publication,
    and the immutable write fence binds this sidecar to the same business key
    and generation before the generic ``add_pending_confirm`` producer runs.
    """
    if not isinstance(body, dict) or "g1q3-rca" not in str(task_id or ""):
        return body, {"added": False, "skipped": "not_g1q3_rca"}
    task_card = body.get("task_card") if isinstance(body.get("task_card"), dict) else None
    if not isinstance(task_card, dict):
        return body, {"added": False, "skipped": "task_card_missing"}
    meta = _load_shared_state_meta(task_id)
    work_item_id = _work_item_id_for_sidecar(task_id, body, meta)
    if not work_item_id:
        return body, {"added": False, "skipped": "work_item_id_missing"}
    try:
        binding = _load_task_write_fence(task_id)
        fence = binding.get("write_fence") if isinstance(binding.get("write_fence"), dict) else {}
        business_key = str(fence.get("business_key") or "").strip()
        generation = int(fence.get("generation") or 0)
        if (
            str(fence.get("submission_key") or "").strip() != str(task_id).strip()
            or not business_key
            or generation < 1
        ):
            raise ExternalWriteFenceError("external_write_fence_identity_mismatch")
        if store_factory is None:
            from gateway.pnc_rca_delivery_store import RcaDeliveryStore

            store_factory = RcaDeliveryStore
        store = store_factory(_relay_control_db_path(), require_current=True)
        candidates = store.list_conclusion_review_queue(limit=100)
        candidate = next(
            (
                item
                for item in candidates
                if str(getattr(item, "work_item_id", "") or "").strip() == work_item_id
                and str(getattr(item, "business_key", "") or "").strip() == business_key
                and int(getattr(item, "generation", 0) or 0) == generation
            ),
            None,
        )
        if candidate is None:
            return body, {"added": False, "skipped": "no_db_proven_medium_candidate"}
        result = add_rca_candidate_conclusion_confirm(
            task_id=task_id,
            candidate=candidate,
        )
        if not result.get("ok"):
            return body, dict(result)
        refreshed = _load_json(path)
        if not refreshed:
            raise RuntimeError("rca review confirm sidecar reload failed")
        return refreshed, dict(result)
    except Exception as exc:
        return body, {
            "added": False,
            "skipped": "db_candidate_proof_unavailable",
            "error": f"{type(exc).__name__}: {exc}",
        }


def _load_task_write_fence(task_id: str) -> dict[str, Any]:
    """Read the immutable W3 snapshot; sidecar metadata is never authoritative."""
    key = str(task_id or "").strip()
    if not key:
        raise ExternalWriteFenceError("external_write_fence_identity_mismatch")
    path = _relay_control_db_path()
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT snapshot.admission_snapshot_json,
                   envelope.source_envelope_json
              FROM rca_admission_snapshots AS snapshot
              JOIN rca_snapshot_source_envelopes AS envelope
                ON envelope.snapshot_sha256 = snapshot.snapshot_sha256
               AND envelope.source_envelope_sha256 =
                   snapshot.creator_source_envelope_sha256
               AND envelope.source_id = snapshot.creator_source_id
             WHERE snapshot.submission_key = ?
            """,
            (key,),
        ).fetchone()
    except (OSError, sqlite3.Error) as exc:
        raise ExternalWriteFenceError(
            "external_write_fence_missing", type(exc).__name__
        ) from exc
    finally:
        try:
            conn.close()
        except UnboundLocalError:
            pass
    if row is None:
        raise ExternalWriteFenceError("external_write_fence_missing")
    try:
        snapshot = json.loads(str(row["admission_snapshot_json"]))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ExternalWriteFenceError("external_write_fence_schema_invalid") from exc
    fence = snapshot.get("write_fence") if isinstance(snapshot, dict) else None
    if not isinstance(fence, dict) or fence.get("state") != "issued":
        raise ExternalWriteFenceError("external_write_fence_missing")
    try:
        source_envelope = json.loads(str(row["source_envelope_json"]))
        targets = validate_write_fence_source_binding(
            fence,
            snapshot=snapshot,
            source_envelope=source_envelope,
        )
    except ExternalWriteFenceError:
        raise
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ExternalWriteFenceError(
            "external_write_fence_schema_invalid"
        ) from exc
    return {
        "snapshot": snapshot,
        "snapshot_core_sha256": snapshot_core_sha256(snapshot),
        "write_fence": fence,
        **targets,
    }


def _is_g1q3_rca_origin_task(task_id: str, body: Mapping[str, Any]) -> bool:
    """Classify relay provenance from canonical admission or live shared state."""

    if "g1q3-rca" in str(task_id or "").strip().lower():
        return True
    try:
        _load_task_write_fence(task_id)
        return True
    except ExternalWriteFenceError:
        pass
    meta = _load_shared_state_meta(task_id)
    if str(meta.get("business_line") or "").strip() in {
        "g1q3_rca",
        "g1q3-rca",
    }:
        return True
    task_card = body.get("task_card") if isinstance(body, Mapping) else None
    delivery = (
        task_card.get("delivery")
        if isinstance(task_card, Mapping)
        and isinstance(task_card.get("delivery"), Mapping)
        else {}
    )
    return str(delivery.get("schema_version") or "").startswith("g1q3_")


def _automatic_g1q3_write_fence_ready(task_id: str) -> bool:
    """Return whether an automatic relay scan may mutate or deliver this task."""

    try:
        binding = _load_task_write_fence(task_id)
        _relay_live_fence_binding(binding["write_fence"])
    except ExternalWriteFenceError:
        return False
    return True


def _relay_live_fence_binding(fence: Mapping[str, Any]) -> dict[str, Any]:
    path = _relay_control_db_path()
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            """
            SELECT epoch.epoch_id, epoch.state, epoch.is_current,
                   ledger.ledger_id, ledger.admission_key,
                   ledger.business_key, ledger.submission_key,
                   ledger.generation, ledger.decision, ledger.bound_at,
                   snapshot.admission_snapshot_json,
                   envelope.source_envelope_json
              FROM rca_activation_epochs AS epoch
              JOIN rca_activation_admission_ledger AS ledger
                ON ledger.epoch_id = epoch.epoch_id
               AND ledger.ledger_id = ?
              JOIN rca_admission_snapshots AS snapshot
                ON snapshot.business_key = ledger.business_key
               AND snapshot.submission_key = ledger.submission_key
               AND snapshot.generation = ledger.generation
               AND snapshot.activation_epoch_id = ledger.epoch_id
               AND snapshot.activation_ledger_id = ledger.ledger_id
              JOIN rca_snapshot_source_envelopes AS envelope
                ON envelope.snapshot_sha256 = snapshot.snapshot_sha256
               AND envelope.source_envelope_sha256 =
                   snapshot.creator_source_envelope_sha256
               AND envelope.source_id = snapshot.creator_source_id
             WHERE epoch.epoch_id = ? AND ledger.admission_key = ?
            """,
            (
                fence.get("activation_ledger_id"),
                fence.get("activation_epoch_id"),
                fence.get("admission_key"),
            ),
        ).fetchone()
    finally:
        conn.close()
    if row is None or int(row["is_current"]) != 1:
        raise ExternalWriteFenceError("external_write_fence_epoch_not_current")
    if str(row["state"]) not in {"bounded_active", "steady_active"}:
        raise ExternalWriteFenceError("external_write_fence_epoch_not_current")
    if str(row["decision"]) != "admit" or not row["bound_at"]:
        raise ExternalWriteFenceError("external_write_fence_operation_denied")
    try:
        targets = validate_write_fence_source_binding(
            fence,
            snapshot=json.loads(str(row["admission_snapshot_json"])),
            source_envelope=json.loads(str(row["source_envelope_json"])),
        )
    except ExternalWriteFenceError:
        raise
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ExternalWriteFenceError(
            "external_write_fence_schema_invalid"
        ) from exc
    return {
        "epoch_id": str(row["epoch_id"]),
        "state": str(row["state"]),
        "ledger_id": int(row["ledger_id"]),
        "business_key": str(row["business_key"]),
        "submission_key": str(row["submission_key"]),
        "generation": int(row["generation"]),
        **targets,
    }


def _fenced_task_senders(
    task_id: str,
    send_func: Callable[[dict[str, Any]], str] | None,
    send_card_func: Callable[..., dict[str, Any]] | None,
) -> tuple[Callable[[dict[str, Any]], str], Callable[..., dict[str, Any]]]:
    """Wrap every task-bound relay provider call with a live fence check."""
    binding = _load_task_write_fence(task_id)
    snapshot = binding["snapshot"]
    fence = binding["write_fence"]
    provider_claim = build_write_fence_provider_claim(fence)
    core_sha = binding["snapshot_core_sha256"]
    resolved = snapshot.get("resolved_admission") or {}
    hot_sender: FeishuHotSender | None = None

    def _provider_sender() -> FeishuHotSender:
        nonlocal hot_sender
        if hot_sender is None:
            hot_sender = FeishuHotSender()
        return hot_sender

    def _check(operation: str, target: str) -> dict[str, Any]:
        live = _relay_live_fence_binding(fence)
        chat_id = str(live.get("chat_id") or "").strip()
        thread_target = str(live.get("thread_target") or "").strip()
        thread_anchor = thread_target.removeprefix("topic:")
        bare_target = f"feishu:{chat_id}" if chat_id else ""
        threaded_target = (
            f"{bare_target}:{thread_anchor}"
            if bare_target and thread_anchor
            else ""
        )
        expected_provider_target = (
            threaded_target
            if operation
            in {
                "feishu_thread_reply",
                "feishu_card_create",
                "feishu_card_patch",
            }
            else bare_target
        )
        if not expected_provider_target or target != expected_provider_target:
            raise ExternalWriteFenceError(
                "external_write_fence_target_mismatch"
            )
        authorization_target = (
            thread_target
            if operation == "feishu_thread_reply"
            else (
                str(live["issue_target"])
                if operation in {"feishu_card_create", "feishu_card_patch"}
                else str(task_id)
            )
        )
        validate_write_fence(
            fence,
            snapshot_core_sha256_value=core_sha,
            operation=operation,
            target=authorization_target,
            expected_epoch_id=live["epoch_id"],
            expected_ledger_id=live["ledger_id"],
            expected_business_key=str(resolved.get("business_key") or ""),
            expected_submission_key=str(resolved.get("submission_key") or task_id),
            expected_generation=int(resolved.get("generation") or 0),
            expected_issue_target=str(live["issue_target"]),
            expected_thread_target=thread_target or None,
            expected_target_set_sha256=str(live["target_set_sha256"]),
            now=datetime.now(timezone.utc),
        )
        return live

    def _send(args: dict[str, Any]) -> str:
        target = str(args.get("target") or "")
        operation = "internal_alert"
        if target.count(":") >= 2:
            operation = "feishu_thread_reply"
        _check(operation, target)
        bound_args = dict(args)
        bound_args["_pnc_rca_external_write_guard"] = provider_claim
        sender = send_func
        if sender is None or sender is send_message_tool:
            sender = _provider_sender().send
        return sender(bound_args)

    def _card(target: str, card_payload: dict[str, Any], message_id: str | None = None) -> dict[str, Any]:
        operation = "feishu_card_patch" if message_id else "feishu_card_create"
        _check(operation, target)
        sender = send_card_func or _provider_sender().send_task_card
        return sender(
            target,
            card_payload,
            message_id=message_id,
            provider_claim=provider_claim,
        )

    return _send, _card


def relay_pending_notices(*, task_ids: Iterable[str] | None = None, send: bool = False, limit: int = 20, retry_failed_after_seconds: int = 0, max_attempts: int = 3, send_func: Callable[[dict[str, Any]], str] | None = None, send_card_func: Callable[..., dict[str, Any]] | None = None, since_ts: float | None = None, explicit_completion_delivery: bool | None = None, max_card_fallbacks_per_loop: int | None = None, crash_hook: Callable[[str, dict[str, Any]], None] | None = None) -> dict[str, Any]:
    record_sender = None
    if send:
        try:
            from gateway.record_only.runtime import get_record_only_transport

            record_transport = get_record_only_transport("scripts.pnc_completion_notice_relay")
        except Exception as exc:
            return {
                "ok": False,
                "dry_run": False,
                "candidate_count": 0,
                "sent_count": 0,
                "rows": [],
                "errors": [f"record-only configuration refused outbound: {exc}"],
        }
        if record_transport is not None:
            from gateway.record_only.transport import RecordOnlyRelaySender

            record_sender = RecordOnlyRelaySender(record_transport)
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    fallback_budget = DEFAULT_MAX_CARD_FALLBACKS_PER_LOOP if max_card_fallbacks_per_loop is None else max(0, int(max_card_fallbacks_per_loop))
    fallback_counter = {"attempted": 0, "sent": 0, "suppressed": 0}
    fallback_lock = threading.Lock()
    explicit_task_filter = bool(task_ids) if explicit_completion_delivery is None else bool(explicit_completion_delivery)
    effective_since_ts = since_ts if since_ts is not None else (None if explicit_task_filter else RELAY_PROCESS_START_TS)
    candidates = iter_pending_notices(
        task_ids=task_ids,
        retry_failed_after_seconds=retry_failed_after_seconds,
        max_attempts=max_attempts,
        since_ts=effective_since_ts,
        require_current_g1q3_write_fence=send and not explicit_task_filter,
    )[: max(1, min(limit, 100))]
    def _process(task_id, path, body, notice):
        errs: list[str] = []
        guard_action = body.pop("_close_loop_guard_action", None) if isinstance(body, dict) else None
        if send:
            latest_body = _load_json(path)
            latest_body = enrich_task_card_vm_progress(task_id, latest_body)
            latest_body = enrich_task_card_delivery_contract(task_id, latest_body)
            _atomic_write_json(path, latest_body)
            latest_body, latest_guard_action = apply_integration_tools_close_loop_guard(task_id, path, latest_body)
            if latest_guard_action is not None:
                guard_action = latest_guard_action
            if explicit_task_filter or "g1q3-rca" in task_id:
                latest_body = enrich_g1q3_task_card_delivery(task_id, latest_body)
            latest_notice = latest_body.get("completion_notice") if isinstance(latest_body.get("completion_notice"), dict) else {}
            relayable, reason = _notice_is_relayable(
                latest_notice,
                retry_failed_after_seconds=retry_failed_after_seconds,
                max_attempts=max_attempts,
            )
            if not relayable and _notice_is_relayable_for_completion_delivery(
                latest_body,
                latest_notice,
                explicit_task_filter=explicit_task_filter,
                since_ts=effective_since_ts,
            ):
                relayable = True
                reason = "completion_delivery_required_explicit" if explicit_task_filter else "completion_delivery_required_new"
            latest_card = latest_body.get("task_card") if isinstance(latest_body.get("task_card"), dict) else None
            if not relayable and not _task_card_needs_sync(latest_card) and not _originator_notify_pending(task_id, latest_body) and not _mechanical_download_notify_pending(task_id, latest_body) and not _g1q3_anomaly_notify_pending(task_id, latest_body) and not _infra_recovery_notify_pending(task_id, latest_body):
                return {
                    "task_id": task_id,
                    "chat_id": notice.get("chat_id"),
                    "vm_task_id": notice.get("vm_task_id"),
                    "dry_run": False,
                    "skipped": True,
                    "reason": reason,
                }, errs
            body = latest_body
            notice = latest_notice if relayable else {}

        if explicit_task_filter or "g1q3-rca" in task_id:
            body = enrich_g1q3_task_card_delivery(task_id, body)
            body, _g1q3_guard_action = apply_g1q3_close_loop_guard(task_id, path, body)
            if _g1q3_guard_action is not None:
                guard_action = _g1q3_guard_action
        body = enrich_task_card_delivery_contract(task_id, body)
        task_send_func = send_func
        task_card_func = send_card_func
        if record_sender is not None:
            task_send_func, task_card_func = _record_only_task_senders(
                record_sender,
                task_id=task_id,
                body=body,
                notice=notice,
            )
        if send and _is_g1q3_rca_origin_task(task_id, body):
            try:
                task_send_func, task_card_func = _fenced_task_senders(
                    task_id,
                    task_send_func,
                    task_card_func,
                )
            except ExternalWriteFenceError as exc:
                return {
                    "task_id": task_id,
                    "dry_run": False,
                    "sent": False,
                    "error": exc.code,
                    "error_detail": exc.detail,
                    "external_write_blocked": True,
                }, [f"{task_id}: {exc.code}"]
        try:
            card_result = sync_task_card(task_id=task_id, path=path, body=body, send=send, send_card_func=task_card_func)
        except Exception as exc:
            # A single malformed card (e.g. a stale positive milestone that trips
            # the fail-closed render guard) must never crash the whole watch loop
            # and starve every other task of card sync.  Isolate + record the
            # failure; the next substantive update (or milestone sanitizer) heals
            # the card on a later loop.  Mirrors the notify guards below.
            card_result = {"error": f"{type(exc).__name__}: {exc}", "skipped": True, "reason": "render_error"}
            errs.append(f"{task_id}: card render/sync failed: {type(exc).__name__}: {exc}")
        # Card patches do not notify; additionally @originator on human-action
        # states via a fresh text reply (once per transition).
        notify_result = None
        download_notify_result = None
        anomaly_notify_result = None
        meta_for_notify = _load_shared_state_meta(task_id)
        try:
            notify_result = maybe_notify_originator(
                task_id=task_id,
                path=path,
                body=body,
                meta=meta_for_notify,
                send=send,
                send_func=task_send_func,
            )
        except Exception as exc:  # never let a notify failure break card/notice relay
            notify_result = {"error": f"{type(exc).__name__}: {exc}"}
        try:
            download_notify_result = maybe_notify_mechanical_download_failure(
                task_id=task_id,
                path=path,
                body=body,
                meta=meta_for_notify,
                send=send,
                send_func=task_send_func,
            )
        except Exception as exc:
            download_notify_result = {"error": f"{type(exc).__name__}: {exc}"}
        try:
            anomaly_notify_result = maybe_notify_g1q3_anomaly(
                task_id=task_id,
                path=path,
                body=body,
                meta=meta_for_notify,
                send=send,
                send_func=task_send_func,
            )
        except Exception as exc:
            anomaly_notify_result = {"error": f"{type(exc).__name__}: {exc}"}
        infra_notify_result = None
        try:
            infra_notify_result = maybe_notify_infra_recovery(
                task_id=task_id,
                path=path,
                body=body,
                meta=meta_for_notify,
                send=send,
                send_func=task_send_func,
            )
        except Exception as exc:
            infra_notify_result = {"error": f"{type(exc).__name__}: {exc}"}
        target = _feishu_target(notice) if notice else _card_target(body.get("task_card", {}), None)
        _task_card = body.get("task_card") if isinstance(body.get("task_card"), dict) else {}
        row: dict[str, Any] = {
            "task_id": task_id,
            "chat_id": notice.get("chat_id") or _task_card.get("chat_id"),
            "target": target,
            "vm_task_id": (
                notice.get("vm_task_id")
                or (body.get("vm_bridge") or {}).get("vm_task_id")
                or _task_card.get("vm_task_id")
                or _task_card.get("run_id")
            ),
            "dry_run": not send,
        }
        if guard_action is not None:
            row["close_loop_guard"] = guard_action
        if notify_result is not None:
            row["originator_notify"] = notify_result
        if download_notify_result is not None:
            row["download_notify"] = download_notify_result
        if anomaly_notify_result is not None:
            row["anomaly_notify"] = anomaly_notify_result
        if infra_notify_result is not None:
            row["infra_notify"] = infra_notify_result
        if card_result is not None:
            row["task_card"] = card_result
        text = str(notice.get("text") or "") if isinstance(notice, dict) else ""
        if text.strip() and _completion_delivery_required(body, notice):
            text = _text_with_completion_must_carry(text, body, notice)
        text = _rca_public_text_without_internal_html(text, _task_card)
        if not send:
            row["preview"] = text[:500]
            return row, errs
        card_failed = isinstance(card_result, dict) and card_result.get("success") is False
        if not text.strip():
            if card_failed and str(card_result.get("fallback_text") or "").strip():
                fallback_text = str(card_result.get("fallback_text") or "")
                with fallback_lock:
                    allowed = fallback_counter["attempted"] < fallback_budget
                    if allowed:
                        fallback_counter["attempted"] += 1
                    else:
                        fallback_counter["suppressed"] += 1
                if not allowed:
                    row["task_card_fallback"] = {"sent": False, "skipped": True, "reason": "card_fallback_fuse_open", "budget": fallback_budget}
                    row["sent"] = False
                    row["skipped_text"] = True
                    return row, errs
                try:
                    raw = (task_send_func or send_message_tool)({"action": "send", "target": target, "message": fallback_text})
                    try:
                        fallback_result = json.loads(raw)
                    except Exception:
                        fallback_result = {"raw": raw}
                    ok = isinstance(fallback_result, dict) and fallback_result.get("success")
                    if ok:
                        with fallback_lock:
                            fallback_counter["sent"] += 1
                    row["task_card_fallback"] = {"sent": bool(ok), "result": fallback_result}
                    if not ok:
                        error = str((fallback_result or {}).get("error") if isinstance(fallback_result, dict) else fallback_result)
                        row["error"] = error
                        errs.append(f"{task_id}: card_fallback: {error}")
                except Exception as exc:
                    error = f"{type(exc).__name__}: {exc}"
                    row["task_card_fallback"] = {"sent": False, "error": error}
                    row["error"] = error
                    errs.append(f"{task_id}: card_fallback: {error}")
                return row, errs
            row["sent"] = False
            row["skipped_text"] = True
            return row, errs
        text_allowed, text_reason = _completion_notice_text_allowed(notice)
        if not text_allowed:
            _mark_text_suppressed(path, body, reason=text_reason)
            row.update({"sent": False, "skipped_text": True, "text_suppressed": True, "suppress_reason": text_reason})
            return row, errs
        has_task_card = isinstance(body.get("task_card"), dict)
        card_ok_or_not_needed = card_result is None or (isinstance(card_result, dict) and card_result.get("success") is not False)
        delivery_required, delivery_reason = _completion_delivery_send_required(
            body,
            notice,
            explicit_task_filter=explicit_task_filter,
            since_ts=effective_since_ts,
        )
        if has_task_card and card_ok_or_not_needed and not delivery_required:
            suppress_reason = "delivery_sent" if delivery_reason == "delivery_sent" else "one_task_one_card"
            _mark_text_suppressed(path, body, reason=suppress_reason)
            row.update({"sent": False, "skipped_text": True, "text_suppressed": True, "suppress_reason": suppress_reason, "delivery_reason": delivery_reason})
            return row, errs
        if has_task_card and card_ok_or_not_needed and delivery_required and _task_card_delivery_complete(body):
            _mark_delivery_sent(path, body, result={"success": True, "suppressed_duplicate_text": True, "reason": "card_delivery_complete"})
            row.update({
                "sent": False,
                "skipped_text": True,
                "text_suppressed": True,
                "suppress_reason": "card_delivery_complete",
                "delivery_reason": delivery_reason,
                "delivery_sent": True,
            })
            return row, errs
        try:
            if crash_hook is not None:
                crash_hook("before_sender", {"task_id": task_id, "path": str(path)})
            raw = (task_send_func or send_message_tool)({"action": "send", "target": target, "message": text})
            try:
                result = json.loads(raw)
            except Exception:
                result = {"raw": raw}
            if isinstance(result, dict) and result.get("success"):
                if crash_hook is not None:
                    crash_hook("after_record_before_mark", {"task_id": task_id, "path": str(path)})
                    crash_hook("before_mark_persist", {"task_id": task_id, "path": str(path)})
                if delivery_required:
                    _mark_delivery_sent(path, body, result=result)
                else:
                    _mark(path, body, status="sent", result=result)
                if crash_hook is not None:
                    crash_hook("after_mark_before_ack", {"task_id": task_id, "path": str(path)})
                row.update({"sent": True, "result": result, "delivery_sent": bool(delivery_required)})
            else:
                error = str((result or {}).get("error") if isinstance(result, dict) else result)
                updated_notice = _mark(path, body, status="failed", result=result if isinstance(result, dict) else {"raw": raw}, error=error)
                alert = maybe_alert_failed_notice(path, body, task_id=task_id, notice=updated_notice, error=error, max_attempts=max_attempts, send_func=task_send_func)
                row.update({"sent": False, "error": error, "result": result})
                if alert is not None:
                    row["alert"] = alert
                errs.append(f"{task_id}: {error}")
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            updated_notice = _mark(path, body, status="failed", error=error)
            alert = maybe_alert_failed_notice(path, body, task_id=task_id, notice=updated_notice, error=error, max_attempts=max_attempts, send_func=task_send_func)
            row.update({"sent": False, "error": error})
            if alert is not None:
                row["alert"] = alert
            errs.append(f"{task_id}: {error}")
        return row, errs

    # Bucket candidates by task_id: same task_id stays strictly serial (card patch
    # ordering depends on it — out-of-order patches would duplicate the card),
    # different task_ids run concurrently up to RELAY_SEND_CONCURRENCY in-flight.
    # Within a single scan task_ids are unique, so bucketing is mainly insurance;
    # the watch loop also serializes the same task_id across separate loops.
    buckets: dict[str, list[tuple[int, tuple]]] = {}
    order: list[str] = []
    for idx, cand in enumerate(candidates):
        tid = cand[0]
        if tid not in buckets:
            buckets[tid] = []
            order.append(tid)
        buckets[tid].append((idx, cand))

    def _run_bucket(tid: str) -> list[tuple[int, dict[str, Any], list[str]]]:
        out: list[tuple[int, dict[str, Any], list[str]]] = []
        for idx, cand in buckets[tid]:
            row, errs = _process(*cand)
            out.append((idx, row, errs))
        return out

    results: dict[int, dict[str, Any]] = {}
    if not send or RELAY_SEND_CONCURRENCY <= 1 or len(order) <= 1:
        for tid in order:
            for idx, row, errs in _run_bucket(tid):
                results[idx] = row
                errors.extend(errs)
    else:
        with ThreadPoolExecutor(max_workers=min(RELAY_SEND_CONCURRENCY, len(order))) as executor:
            for bucket_rows in executor.map(_run_bucket, order):
                for idx, row, errs in bucket_rows:
                    results[idx] = row
                    errors.extend(errs)
    rows = [results[i] for i in sorted(results)]
    return {
        "ok": not errors,
        "dry_run": not send,
        "candidate_count": len(candidates),
        "sent_count": sum(1 for row in rows if row.get("sent")),
        "card_fallback_attempted_count": fallback_counter["attempted"],
        "card_fallback_sent_count": fallback_counter["sent"],
        "card_fallback_suppressed_count": fallback_counter["suppressed"],
        "card_fallback_budget": fallback_budget,
        "rows": rows,
        "retry_failed_after_seconds": retry_failed_after_seconds,
        "max_attempts": max_attempts,
        "errors": errors,
    }


def _completion_relay_health_path() -> Path:
    configured = os.getenv("PNC_COMPLETION_NOTICE_RELAY_HEALTH_PATH", "").strip()
    if configured:
        expanded = Path(configured).expanduser()
        if not expanded.is_absolute():
            raise ValueError("PNC_COMPLETION_NOTICE_RELAY_HEALTH_PATH must be absolute")
        return expanded.absolute()
    return (
        get_hermes_home()
        / "runtime/pnc_agent/feishu_issue_kafka_rca/completion_notice_relay_health.json"
    )


def watch_pending_notices(
    *,
    task_ids: Iterable[str] | None = None,
    send: bool = False,
    limit: int = 50,
    retry_failed_after_seconds: int = 0,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    poll_seconds: float = DEFAULT_WATCH_POLL_SECONDS,
    full_scan_seconds: int = DEFAULT_WATCH_FULL_SCAN_SECONDS,
    canary_loops: int = DEFAULT_WATCH_CANARY_LOOPS,
    max_card_fallbacks_per_loop: int | None = None,
    health_path: Path | None = None,
    runtime_evidence_builder: Callable[..., dict[str, Any]] = (
        build_process_runtime_evidence
    ),
) -> dict[str, Any]:
    hot_sender = None
    if send:
        from gateway.record_only.runtime import get_record_only_transport

        record_transport = get_record_only_transport("scripts.pnc_completion_notice_relay")
        if record_transport is not None:
            from gateway.record_only.transport import RecordOnlyRelaySender

            hot_sender = RecordOnlyRelaySender(record_transport)
        else:
            hot_sender = FeishuHotSender()
    send_func = hot_sender.send if hot_sender is not None else send_message_tool
    root = get_hermes_home() / "task-state"
    last_full_scan = 0.0
    loop_count = 0
    total_sent = 0
    last_result: dict[str, Any] | None = None
    watched_mtimes: dict[str, int] = {}
    runtime_evidence = (
        runtime_evidence_builder(
            service_label=COMPLETION_RELAY_SERVICE_LABEL,
            script_path=Path(__file__),
        )
        if health_path is not None
        else None
    )
    startup_canary_completed_at: str | None = None
    # Do not retroactively @-ping every historical anomaly on relay restart.
    backfill_g1q3_anomaly_notify_keys(task_ids=task_ids or None)
    while True:
        now = time.monotonic()
        force_full = last_full_scan == 0.0 or now - last_full_scan >= max(1, full_scan_seconds)
        if force_full:
            selected_task_ids = list(task_ids or []) or None
            last_full_scan = now
        else:
            changed: list[str] = []
            if root.exists():
                for path in root.glob("*.json"):
                    try:
                        mtime = path.stat().st_mtime_ns
                    except OSError:
                        continue
                    key = _task_id_from_sidecar_path(path)
                    old = watched_mtimes.get(str(path))
                    watched_mtimes[str(path)] = mtime
                    if old is None or old != mtime:
                        changed.append(key)
            selected_task_ids = changed
        loop_result: dict[str, Any] = {
            "ok": True,
            "card_fallback_attempted_count": 0,
            "card_fallback_sent_count": 0,
            "errors": [],
        }
        effective_fallback_budget = (
            0 if loop_count < canary_loops else max_card_fallbacks_per_loop
        )
        if selected_task_ids is None or selected_task_ids:
            result = relay_pending_notices(
                task_ids=selected_task_ids,
                send=send,
                limit=limit,
                retry_failed_after_seconds=retry_failed_after_seconds,
                max_attempts=max_attempts,
                send_func=send_func,
                send_card_func=(getattr(hot_sender, "send_task_card", None) if hot_sender is not None else None),
                since_ts=RELAY_PROCESS_START_TS,
                explicit_completion_delivery=False,
                max_card_fallbacks_per_loop=effective_fallback_budget,
            )
            loop_result = result
            last_result = result
            total_sent += int(result.get("sent_count") or 0)
            if result.get("candidate_count") or result.get("errors"):
                print(json.dumps({"watch": True, "loop": loop_count, **result}, ensure_ascii=False, sort_keys=True), flush=True)
        loop_count += 1
        completed_loops = min(loop_count, canary_loops)
        if canary_loops == 0 or completed_loops >= canary_loops:
            startup_canary_completed_at = (
                startup_canary_completed_at or datetime.now(timezone.utc).isoformat()
            )
        if health_path is not None and runtime_evidence is not None:
            errors = [str(value) for value in loop_result.get("errors") or []]
            write_owner_health(
                health_path,
                {
                    "schema_version": COMPLETION_RELAY_HEALTH_SCHEMA_VERSION,
                    "service_label": COMPLETION_RELAY_SERVICE_LABEL,
                    "observed_at": datetime.now(timezone.utc).isoformat(),
                    "started_at": runtime_evidence["started_at"],
                    "pid": runtime_evidence["pid"],
                    "process_create_time": runtime_evidence["process_create_time"],
                    "loop_count": loop_count,
                    "startup_canary_loops_required": canary_loops,
                    "startup_canary_loops_completed": completed_loops,
                    "startup_canary_completed_at": startup_canary_completed_at,
                    "configured_max_card_fallbacks_per_loop": (
                        max_card_fallbacks_per_loop
                    ),
                    "effective_max_card_fallbacks_per_loop": (
                        effective_fallback_budget
                    ),
                    "card_fallback_attempted_count": int(
                        loop_result.get("card_fallback_attempted_count") or 0
                    ),
                    "card_fallback_sent_count": int(
                        loop_result.get("card_fallback_sent_count") or 0
                    ),
                    "healthy": loop_result.get("ok") is True and not errors,
                    "errors": errors,
                    "runtime_identity": runtime_evidence["runtime_identity"],
                },
            )
        time.sleep(max(0.1, poll_seconds))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task-id", action="append", default=[])
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--send", action="store_true", help="Actually send pending notices to Feishu")
    parser.add_argument("--retry-failed-after", type=int, default=0, help="Retry failed notices after this many seconds; 0 disables failed retries")
    parser.add_argument("--max-attempts", type=int, default=DEFAULT_MAX_ATTEMPTS, help="Maximum send attempts per notice")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--watch", action="store_true", help="Run as a long-lived 1s polling watcher with a hot Feishu sender")
    parser.add_argument("--watch-poll-seconds", type=float, default=DEFAULT_WATCH_POLL_SECONDS)
    parser.add_argument("--watch-full-scan-seconds", type=int, default=DEFAULT_WATCH_FULL_SCAN_SECONDS)
    parser.add_argument("--watch-canary-loops", type=int, default=DEFAULT_WATCH_CANARY_LOOPS, help="Force card fallback fuse closed for the first N watch loops after start")
    parser.add_argument("--max-card-fallbacks-per-loop", type=int, default=DEFAULT_MAX_CARD_FALLBACKS_PER_LOOP, help="Max task-card fallback text sends per relay loop; default 0 prevents flood")
    parser.add_argument("--no-lock", action="store_true", help="Disable single-run lock (tests/manual debugging only)")
    parser.add_argument("--backfill-notify-keys", action="store_true", help="One-shot: stamp last_notify_key on existing human-action tasks so they are not retroactively @-pinged on first scan; does not send")
    parser.add_argument("--backfill-g1q3-anomaly-notify-keys", action="store_true", help="One-shot: stamp last_anomaly_notify_key on existing G1Q3 anomaly tasks; does not send")
    args = parser.parse_args(argv)
    if args.backfill_g1q3_anomaly_notify_keys:
        result = backfill_g1q3_anomaly_notify_keys(task_ids=args.task_id or None)
        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        else:
            print(f"[BACKFILL] stamped {result['stamped_count']} existing G1Q3 anomaly task(s)")
            for row in result["stamped"]:
                print(f"- {row['task_id']} ({row['state']})")
        return 0
    if args.backfill_notify_keys:
        result = backfill_originator_notify_keys(task_ids=args.task_id or None)
        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        else:
            print(f"[BACKFILL] stamped {result['stamped_count']} existing human-action task(s)")
            for row in result["stamped"]:
                print(f"- {row['task_id']} ({row['kind']}/{row['state']})")
        return 0
    def _run_once_or_watch() -> dict[str, Any]:
        if args.watch:
            return watch_pending_notices(
                task_ids=args.task_id,
                send=args.send,
                limit=args.limit,
                retry_failed_after_seconds=max(0, args.retry_failed_after),
                max_attempts=max(1, args.max_attempts),
                poll_seconds=max(0.1, args.watch_poll_seconds),
                full_scan_seconds=max(1, args.watch_full_scan_seconds),
                canary_loops=max(0, args.watch_canary_loops),
                max_card_fallbacks_per_loop=max(0, args.max_card_fallbacks_per_loop),
                health_path=_completion_relay_health_path(),
            )
        return relay_pending_notices(task_ids=args.task_id, send=args.send, limit=args.limit, retry_failed_after_seconds=max(0, args.retry_failed_after), max_attempts=max(1, args.max_attempts), max_card_fallbacks_per_loop=max(0, args.max_card_fallbacks_per_loop))

    if args.no_lock:
        result = _run_once_or_watch()
    else:
        with SingleRunLock(get_hermes_home() / "locks" / "pnc-completion-notice-relay.lock") as lock:
            result = _run_once_or_watch() if lock.acquired else _skipped_locked_result(send=args.send)
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        mode = "SEND" if args.send else "DRY-RUN"
        print(f"[{mode}] PNC completion notice relay: {result['sent_count']}/{result['candidate_count']} sent")
        for row in result["rows"]:
            print(f"- {row.get('task_id')} -> {row.get('target', '—')}: {'sent' if row.get('sent') else 'skipped' if row.get('skipped') else 'pending' if not args.send else 'failed'}")
        for error in result["errors"]:
            print(f"error: {error}")
    return 0 if result["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
