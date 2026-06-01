"""Local Mem0 sidecar memory provider.

This provider talks to the user's local ``mem0-gateway`` HTTP sidecar.  It is
intentionally separate from ``plugins.memory.mem0``, which targets Mem0's cloud
Platform API.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List

from agent.memory_provider import MemoryProvider
from tools.registry import tool_error

logger = logging.getLogger(__name__)

_DEFAULT_BASE_URL = ""
_DEFAULT_USER_ID = "hermes-user"
_DEFAULT_AGENT_ID = "hermes"
_DEFAULT_LIMIT = 5
_DEFAULT_THRESHOLD = 0.25
_DEFAULT_TIMEOUT = 5.0
_CONFIG_FILE = "local_mem0_sidecar.json"


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _coerce_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _load_config(hermes_home: str | None = None) -> dict[str, Any]:
    """Load local sidecar config from env plus ``$HERMES_HOME`` JSON.

    Environment variables provide defaults; the JSON file overrides individual
    fields when present.  Secrets are never logged by this helper.
    """

    cfg: dict[str, Any] = {
        "base_url": os.environ.get("LOCAL_MEM0_SIDECAR_BASE_URL", _DEFAULT_BASE_URL),
        "api_key": os.environ.get("LOCAL_MEM0_SIDECAR_API_KEY", ""),
        "user_id": os.environ.get("LOCAL_MEM0_USER_ID", _DEFAULT_USER_ID),
        "agent_id": os.environ.get("LOCAL_MEM0_AGENT_ID", _DEFAULT_AGENT_ID),
        "default_limit": _coerce_int(os.environ.get("LOCAL_MEM0_DEFAULT_LIMIT"), _DEFAULT_LIMIT),
        "default_threshold": _coerce_float(os.environ.get("LOCAL_MEM0_DEFAULT_THRESHOLD"), _DEFAULT_THRESHOLD),
        "timeout_seconds": _coerce_float(os.environ.get("LOCAL_MEM0_TIMEOUT"), _DEFAULT_TIMEOUT),
        "auto_capture": False,
    }

    config_home = hermes_home or os.environ.get("HERMES_HOME")
    if config_home:
        path = Path(config_home).expanduser() / _CONFIG_FILE
        if path.exists():
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(raw, dict):
                    cfg.update({k: v for k, v in raw.items() if v is not None and v != ""})
            except Exception:
                logger.debug("Failed to parse local mem0 sidecar config", exc_info=True)

    cfg["base_url"] = str(cfg.get("base_url") or "").rstrip("/")
    cfg["user_id"] = str(cfg.get("user_id") or _DEFAULT_USER_ID)
    cfg["agent_id"] = str(cfg.get("agent_id") or _DEFAULT_AGENT_ID)
    cfg["default_limit"] = _coerce_int(cfg.get("default_limit"), _DEFAULT_LIMIT)
    cfg["default_threshold"] = _coerce_float(cfg.get("default_threshold"), _DEFAULT_THRESHOLD)
    cfg["timeout_seconds"] = _coerce_float(cfg.get("timeout_seconds"), _DEFAULT_TIMEOUT)
    cfg["auto_capture"] = _coerce_bool(cfg.get("auto_capture"), False)
    return cfg


def _unwrap_results(response: Any) -> list[dict[str, Any]]:
    if isinstance(response, dict):
        for key in ("results", "memories", "items"):
            value = response.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        return []
    if isinstance(response, list):
        return [item for item in response if isinstance(item, dict)]
    return []


def _is_reviewed_memory(item: dict[str, Any]) -> bool:
    metadata = item.get("metadata")
    if not isinstance(metadata, dict):
        return True
    approval_status = str(metadata.get("approval_status") or "").strip().lower()
    if not approval_status:
        return True
    return approval_status not in {"candidate", "pending", "manual_only"}


def _json_result(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False)


class _SidecarClient:
    def __init__(self, base_url: str, api_key: str = "", timeout: float = _DEFAULT_TIMEOUT):
        self.base_url = (base_url or "").rstrip("/")
        self.api_key = api_key or ""
        self.timeout = float(timeout)

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def health(self) -> dict[str, Any]:
        if not self.base_url:
            return {"ok": False, "error": "base_url is not configured"}
        req = urllib.request.Request(f"{self.base_url}/health", headers=self._headers(), method="GET")
        try:
            with urllib.request.urlopen(req, timeout=min(self.timeout, 3.0)) as resp:
                body = resp.read().decode("utf-8", errors="replace")
                data = json.loads(body) if body else {}
                if isinstance(data, dict):
                    data.setdefault("ok", True)
                    return data
                return {"ok": True}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    def post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        if not self.base_url:
            raise RuntimeError("local_mem0_sidecar base_url is not configured")
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            f"{self.base_url}{path}",
            data=body,
            headers=self._headers(),
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                parsed = json.loads(raw) if raw else {}
                return parsed if isinstance(parsed, dict) else {"ok": True, "results": parsed}
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else str(exc)
            raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Sidecar unreachable: {exc}") from exc


PROFILE_SCHEMA = {
    "name": "mem0_profile",
    "description": "List stored memories from the local Mem0 sidecar for the current safe user scope.",
    "parameters": {
        "type": "object",
        "properties": {
            "limit": {"type": "integer", "description": "Maximum memories to return (default 20)."},
        },
        "required": [],
    },
}

SEARCH_SCHEMA = {
    "name": "mem0_search",
    "description": "Search local Mem0 sidecar memories by meaning within the current safe user scope.",
    "parameters": {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "What to search for."},
            "top_k": {"type": "integer", "description": "Maximum results to return."},
            "threshold": {"type": "number", "description": "Similarity threshold."},
            "include_candidates": {"type": "boolean", "description": "Include candidate memories in the response."},
        },
        "required": ["query"],
    },
}

CONCLUDE_SCHEMA = {
    "name": "mem0_conclude",
    "description": "Store an explicit candidate fact in the local Mem0 sidecar. Do not use for task progress.",
    "parameters": {
        "type": "object",
        "properties": {
            "conclusion": {"type": "string", "description": "The stable fact to store as a candidate."},
        },
        "required": ["conclusion"],
    },
}

PROMOTE_SCHEMA = {
    "name": "mem0_promote",
    "description": "Promote a reviewed candidate fact into normal local Mem0 recall without deleting the original candidate.",
    "parameters": {
        "type": "object",
        "properties": {
            "memory": {"type": "string", "description": "The reviewed memory text to promote."},
            "candidate_id": {"type": "string", "description": "Optional source candidate memory id."},
            "rationale": {"type": "string", "description": "Optional review rationale."},
        },
        "required": ["memory"],
    },
}


class LocalMem0SidecarMemoryProvider(MemoryProvider):
    """MemoryProvider that talks to the local mem0-gateway sidecar."""

    def __init__(self) -> None:
        self._config: dict[str, Any] = {}
        self._client: _SidecarClient | None = None
        self._session_id = ""
        self._user_id = _DEFAULT_USER_ID
        self._agent_id = _DEFAULT_AGENT_ID
        self._platform = "cli"
        self._chat_id = ""
        self._thread_id = ""
        self._gateway_session_key = ""

    @property
    def name(self) -> str:
        return "local_mem0_sidecar"

    def is_available(self) -> bool:
        return bool(str(_load_config().get("base_url") or "").strip())

    def get_config_schema(self) -> List[Dict[str, Any]]:
        return [
            {
                "key": "base_url",
                "description": "Local Mem0 sidecar base URL",
                "required": True,
                "default": "http://127.0.0.1:8765",
                "env_var": "LOCAL_MEM0_SIDECAR_BASE_URL",
            },
            {
                "key": "api_key",
                "description": "Local Mem0 sidecar bearer token (optional)",
                "secret": True,
                "required": False,
                "env_var": "LOCAL_MEM0_SIDECAR_API_KEY",
            },
            {"key": "user_id", "description": "Default user identifier", "default": _DEFAULT_USER_ID},
            {"key": "agent_id", "description": "Default agent identifier", "default": _DEFAULT_AGENT_ID},
            {"key": "default_limit", "description": "Default search result limit", "default": str(_DEFAULT_LIMIT)},
            {"key": "default_threshold", "description": "Default search threshold", "default": str(_DEFAULT_THRESHOLD)},
            {"key": "timeout_seconds", "description": "HTTP timeout in seconds", "default": str(_DEFAULT_TIMEOUT)},
        ]

    def save_config(self, values: Dict[str, Any], hermes_home: str) -> None:
        path = Path(hermes_home).expanduser() / _CONFIG_FILE
        existing: dict[str, Any] = {}
        if path.exists():
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(raw, dict):
                    existing = raw
            except Exception:
                existing = {}
        existing.update(values)
        path.write_text(json.dumps(existing, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def initialize(self, session_id: str, **kwargs) -> None:
        self._config = _load_config(kwargs.get("hermes_home"))
        self._session_id = session_id
        self._platform = str(kwargs.get("platform") or "cli")
        self._chat_id = str(kwargs.get("chat_id") or "")
        self._thread_id = str(kwargs.get("thread_id") or "")
        self._gateway_session_key = str(kwargs.get("gateway_session_key") or "")
        self._user_id = str(kwargs.get("user_id") or self._config.get("user_id") or _DEFAULT_USER_ID)
        self._agent_id = str(self._config.get("agent_id") or _DEFAULT_AGENT_ID)
        self._client = _SidecarClient(
            base_url=str(self._config.get("base_url") or ""),
            api_key=str(self._config.get("api_key") or ""),
            timeout=_coerce_float(self._config.get("timeout_seconds"), _DEFAULT_TIMEOUT),
        )

    def system_prompt_block(self) -> str:
        return (
            "# Local Mem0 Sidecar Memory\n"
            "Scoped semantic recall is available via mem0_search. "
            "Do not store task progress or temporary status as memory."
        )

    def _safe_user_scope(self) -> bool:
        if self._platform and self._platform != "cli" and not self._user_id:
            return False
        return bool(self._user_id)

    def _metadata_filters(self) -> dict[str, str]:
        return {"scope_type": "user", "scope_id": self._user_id}

    def _search_payload(self, query: str, *, limit: int | None = None, threshold: float | None = None) -> dict[str, Any]:
        return {
            "query": query,
            "user_id": self._user_id,
            "session_id": self._session_id,
            "limit": limit if limit is not None else _coerce_int(self._config.get("default_limit"), _DEFAULT_LIMIT),
            "threshold": threshold if threshold is not None else _coerce_float(self._config.get("default_threshold"), _DEFAULT_THRESHOLD),
            "metadata_filters": self._metadata_filters(),
        }

    def _get_client(self) -> _SidecarClient:
        if self._client is None:
            self.initialize(self._session_id or "")
        if self._client is None:
            raise RuntimeError("local_mem0_sidecar not initialized")
        return self._client

    def prefetch(self, query: str, *, session_id: str = "") -> str:
        query = (query or "").strip()
        if not query or not self._safe_user_scope():
            return ""
        try:
            response = self._get_client().post("/v1/memory/search", self._search_payload(query))
        except Exception:
            logger.debug("local_mem0_sidecar prefetch failed", exc_info=True)
            return ""
        lines: list[str] = []
        for item in _unwrap_results(response):
            if not _is_reviewed_memory(item):
                continue
            memory = str(item.get("memory") or item.get("text") or item.get("content") or "").strip()
            if memory:
                lines.append(f"- {memory}")
        if not lines:
            return ""
        return "## Local Mem0 Memory\n" + "\n".join(lines)

    def queue_prefetch(self, query: str, *, session_id: str = "") -> None:
        return None

    def sync_turn(self, user_content: str, assistant_content: str, *, session_id: str = "") -> None:
        if not _coerce_bool(self._config.get("auto_capture"), False):
            return None
        # Automatic turn capture is intentionally disabled by default.  If a
        # user explicitly enables it later, keep this conservative and scoped.
        try:
            self._get_client().post(
                "/v1/memory/capture",
                {
                    "messages": [
                        {"role": "user", "content": user_content},
                        {"role": "assistant", "content": assistant_content},
                    ],
                    "user_id": self._user_id,
                    "session_id": session_id or self._session_id,
                    "infer": False,
                    "metadata": {
                        "scope_type": "user",
                        "scope_id": self._user_id,
                        "source_type": "hermes_turn_sync",
                        "approval_status": "candidate",
                        "recall_policy": "manual_only",
                    },
                },
            )
        except Exception:
            logger.debug("local_mem0_sidecar sync_turn failed", exc_info=True)

    def get_tool_schemas(self) -> List[Dict[str, Any]]:
        return [PROFILE_SCHEMA, SEARCH_SCHEMA, CONCLUDE_SCHEMA, PROMOTE_SCHEMA]

    def handle_tool_call(self, tool_name: str, args: Dict[str, Any], **kwargs) -> str:
        if tool_name == "mem0_search":
            return self._handle_search(args)
        if tool_name == "mem0_profile":
            return self._handle_profile(args)
        if tool_name == "mem0_conclude":
            return self._handle_conclude(args)
        if tool_name == "mem0_promote":
            return self._handle_promote(args)
        return tool_error(f"Unknown local_mem0_sidecar tool: {tool_name}")

    def _handle_search(self, args: Dict[str, Any]) -> str:
        query = str(args.get("query") or "").strip()
        if not query:
            return tool_error("Missing required parameter: query")
        if not self._safe_user_scope():
            return tool_error("No safe user scope available for local Mem0 search")
        limit = _coerce_int(args.get("top_k"), _coerce_int(self._config.get("default_limit"), _DEFAULT_LIMIT))
        threshold = _coerce_float(args.get("threshold"), _coerce_float(self._config.get("default_threshold"), _DEFAULT_THRESHOLD))
        try:
            response = self._get_client().post("/v1/memory/search", self._search_payload(query, limit=limit, threshold=threshold))
        except Exception as exc:
            return tool_error(str(exc))
        results = _unwrap_results(response)
        if not _coerce_bool(args.get("include_candidates"), False):
            results = [item for item in results if _is_reviewed_memory(item)]
        return _json_result({"ok": bool(response.get("ok", True)) if isinstance(response, dict) else True, "count": len(results), "results": results})

    def _handle_profile(self, args: Dict[str, Any]) -> str:
        if not self._safe_user_scope():
            return tool_error("No safe user scope available for local Mem0 profile")
        limit = _coerce_int(args.get("limit"), 20)
        try:
            response = self._get_client().post(
                "/v1/memory/list",
                {
                    "user_id": self._user_id,
                    "limit": limit,
                    "metadata_filters": self._metadata_filters(),
                },
            )
        except Exception as exc:
            return tool_error(str(exc))
        results = _unwrap_results(response)
        results = [item for item in results if _is_reviewed_memory(item)]
        summary = "\n".join(f"- {item.get('memory') or item.get('text') or item.get('content') or item}" for item in results)
        return _json_result({"ok": bool(response.get("ok", True)) if isinstance(response, dict) else True, "count": len(results), "result": summary, "results": results})

    def _handle_conclude(self, args: Dict[str, Any]) -> str:
        conclusion = str(args.get("conclusion") or "").strip()
        if not conclusion:
            return tool_error("Missing required parameter: conclusion")
        if not self._safe_user_scope():
            return tool_error("No safe user scope available for local Mem0 capture")
        payload = {
            "messages": [{"role": "system", "content": conclusion}],
            "user_id": self._user_id,
            "session_id": self._session_id,
            "infer": False,
            "metadata": {
                "scope_type": "user",
                "scope_id": self._user_id,
                "source_type": "hermes_memory_provider_tool",
                "memory_type": "semantic",
                "sensitivity": "private",
                "approval_status": "candidate",
                "recall_policy": "manual_only",
                "platform": self._platform,
            },
        }
        try:
            response = self._get_client().post("/v1/memory/capture", payload)
        except Exception as exc:
            return tool_error(str(exc))
        return _json_result(response)

    def _handle_promote(self, args: Dict[str, Any]) -> str:
        memory = str(args.get("memory") or "").strip()
        if not memory:
            return tool_error("Missing required parameter: memory")
        if not self._safe_user_scope():
            return tool_error("No safe user scope available for local Mem0 promotion")
        metadata = {
            "scope_type": "user",
            "scope_id": self._user_id,
            "source_type": "hermes_memory_provider_review",
            "memory_type": "semantic",
            "sensitivity": "private",
            "approval_status": "reviewed",
            "recall_policy": "auto_recall",
            "platform": self._platform,
        }
        candidate_id = str(args.get("candidate_id") or "").strip()
        if candidate_id:
            metadata["source_candidate_id"] = candidate_id
        rationale = str(args.get("rationale") or "").strip()
        if rationale:
            metadata["review_rationale"] = rationale
        payload = {
            "messages": [{"role": "system", "content": memory}],
            "user_id": self._user_id,
            "session_id": self._session_id,
            "infer": False,
            "metadata": metadata,
        }
        try:
            response = self._get_client().post("/v1/memory/capture", payload)
        except Exception as exc:
            return tool_error(str(exc))
        return _json_result(response)

    def shutdown(self) -> None:
        self._client = None


def register(ctx) -> None:
    ctx.register_memory_provider(LocalMem0SidecarMemoryProvider())
