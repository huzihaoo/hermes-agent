"""ACP permission bridging — maps ACP approval requests to hermes approval callbacks."""

from __future__ import annotations

import asyncio
import json
import logging
from concurrent.futures import TimeoutError as FutureTimeout
from datetime import datetime
from pathlib import Path
from typing import Callable

from acp.schema import (
    AllowedOutcome,
    PermissionOption,
)

logger = logging.getLogger(__name__)

# Maps ACP PermissionOptionKind -> hermes approval result strings
_KIND_TO_HERMES = {
    "allow_once": "once",
    "allow_always": "always",
    "reject_once": "deny",
    "reject_always": "deny",
}


def _save_pending_approval(session_id: str, command: str, description: str, timeout: float) -> None:
    """Save pending approval context for later recovery."""
    pending_dir = Path.home() / ".hermes" / "pending-approvals"
    pending_dir.mkdir(parents=True, exist_ok=True)
    
    context = {
        "session_id": session_id,
        "command": command,
        "description": description,
        "timeout": timeout,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "status": "pending",
    }
    
    filepath = pending_dir / f"{session_id}.json"
    filepath.write_text(json.dumps(context, indent=2, ensure_ascii=False))
    logger.info("Saved pending approval to %s", filepath)


def make_approval_callback(
    request_permission_fn: Callable,
    loop: asyncio.AbstractEventLoop,
    session_id: str,
    timeout: float = 300.0,  # 5 minutes default, was 60s
) -> Callable[[str, str], str]:
    """
    Return a hermes-compatible ``approval_callback(command, description) -> str``
    that bridges to the ACP client's ``request_permission`` call.

    Args:
        request_permission_fn: The ACP connection's ``request_permission`` coroutine.
        loop: The event loop on which the ACP connection lives.
        session_id: Current ACP session id.
        timeout: Seconds to wait for a response before graceful degradation (default 300s).
    """

    def _callback(command: str, description: str) -> str:
        options = [
            PermissionOption(option_id="allow_once", kind="allow_once", name="Allow once"),
            PermissionOption(option_id="allow_always", kind="allow_always", name="Allow always"),
            PermissionOption(option_id="deny", kind="reject_once", name="Deny"),
        ]
        import acp as _acp

        tool_call = _acp.start_tool_call("perm-check", command, kind="execute")

        coro = request_permission_fn(
            session_id=session_id,
            tool_call=tool_call,
            options=options,
        )

        try:
            future = asyncio.run_coroutine_threadsafe(coro, loop)
            response = future.result(timeout=timeout)
        except (FutureTimeout, Exception) as exc:
            logger.warning("Permission request timed out or failed after %.1fs: %s", timeout, exc)
            # Graceful degradation: save context for later recovery
            try:
                _save_pending_approval(session_id, command, description, timeout)
                logger.info("Saved pending approval for session %s", session_id)
            except Exception as save_exc:
                logger.error("Failed to save pending approval: %s", save_exc)
            # Return "deny" for now (TODO: return "pending" when gateway supports it)
            return "deny"

        outcome = response.outcome
        if isinstance(outcome, AllowedOutcome):
            option_id = outcome.option_id
            # Look up the kind from our options list
            for opt in options:
                if opt.option_id == option_id:
                    return _KIND_TO_HERMES.get(opt.kind, "deny")
            return "once"  # fallback for unknown option_id
        else:
            return "deny"

    return _callback
