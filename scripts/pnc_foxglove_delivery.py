"""Shared Foxglove delivery URL contract for PNC host writers."""
from __future__ import annotations

import ipaddress
import os
import re
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import quote, urlsplit


PERCEPTION_TEST_TEAM_VM_PREFIX = "/mnt/minieye/pdcl/department/perception_test_team/"
G1Q3_RCA_FORMAL_VIZ_ROOT = (
    PERCEPTION_TEST_TEAM_VM_PREFIX.rstrip("/") + "/G1Q3_RCA/cases"
)
G1Q3_RCA_FORMAL_VIZ_CIFS_ROOT = (
    "//hfs.minieye.tech/department-perception_test_team/G1Q3_RCA/cases"
)
FOXGLOVE_PATH_SAFE = "/._-()[]中文abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
DEFAULT_FOXGLOVE_RENDER_HOST = "192.168.21.217"
FIXED_FOXGLOVE_ORIGIN = f"https://{DEFAULT_FOXGLOVE_RENDER_HOST}"
_SAFE_SEGMENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,191}$")
_DNS_LABEL_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")


def _is_whatwg_ipv4_number(value: str) -> bool:
    if value.startswith("0x"):
        return len(value) > 2 and all(char in "0123456789abcdef" for char in value[2:])
    if len(value) > 1 and value.startswith("0"):
        return all(char in "01234567" for char in value[1:])
    return value.isdigit()


def canonical_viz_mcap_path(submission_key: Any) -> str:
    """Return the immutable Foxglove-visible path for one RCA submission."""
    key = str(submission_key or "").strip()
    if not _SAFE_SEGMENT_RE.fullmatch(key):
        return ""
    return f"{G1Q3_RCA_FORMAL_VIZ_ROOT}/{key}/{key}.viz.mcap"


def canonical_viz_mcap_cifs_path(submission_key: Any) -> str:
    """Return the user-visible CIFS path paired with the VM publication."""
    key = str(submission_key or "").strip()
    if not _SAFE_SEGMENT_RE.fullmatch(key):
        return ""
    return f"{G1Q3_RCA_FORMAL_VIZ_CIFS_ROOT}/{key}/{key}.viz.mcap"


def _is_supported_viz_path(path: str) -> bool:
    viz_path = PurePosixPath(path)
    if (
        not viz_path.is_absolute()
        or "\x00" in path
        or "\\" in path
        or any(part in {"", ".", ".."} for part in viz_path.parts[1:])
    ):
        return False
    key = viz_path.parent.name
    if viz_path.name != f"{key}.viz.mcap":
        return False
    return _SAFE_SEGMENT_RE.fullmatch(key) is not None and path == canonical_viz_mcap_path(key)


def _canonical_origin_host(hostname: str) -> str:
    if (
        not hostname
        or not hostname.isascii()
        or hostname != hostname.lower()
        or any(char in hostname for char in "\\%")
    ):
        return ""
    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        labels = hostname.split(".")
        if (
            len(hostname) > 253
            or any(_DNS_LABEL_RE.fullmatch(label) is None for label in labels)
            or any(label.startswith("xn--") for label in labels)
            or not any(char.isalpha() for char in labels[-1])
            or _is_whatwg_ipv4_number(labels[-1])
        ):
            return ""
        return hostname
    if address.version != 4:
        return ""
    canonical = address.compressed.lower()
    if hostname != canonical:
        return ""
    return canonical


def _foxglove_base(*, require_explicit: bool = False) -> str:
    configured_origin = os.getenv("PNC_FOXGLOVE_RENDER_HOST")
    if require_explicit and configured_origin is None:
        return ""
    raw = (
        DEFAULT_FOXGLOVE_RENDER_HOST
        if configured_origin is None
        else configured_origin
    )
    if (
        not raw
        or not raw.isascii()
        or any(ord(char) < 0x20 or ord(char) == 0x7F for char in raw)
        or any(char in raw for char in "\\%")
        or any(char.isspace() for char in raw)
    ):
        return ""
    configured = raw.rstrip("/")
    base = (
        configured
        if configured.startswith(("http://", "https://"))
        else f"https://{configured}"
    )
    try:
        parsed = urlsplit(base)
        hostname = parsed.hostname
        port = parsed.port
    except ValueError:
        return ""
    expected_host = _canonical_origin_host(hostname or "")
    expected_netloc = (
        expected_host if port is None else f"{expected_host}:{port}"
    )
    if (
        parsed.scheme not in {"http", "https"}
        or not expected_host
        or (port is not None and not 1 <= port <= 65535)
        or (parsed.scheme == "https" and port == 443)
        or (parsed.scheme == "http" and port == 80)
        or parsed.netloc != expected_netloc
        or base != f"{parsed.scheme}://{parsed.netloc}"
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
    ):
        return ""
    return base


def canonical_publication_origin() -> str:
    """Return the explicit HTTPS DNS origin used for public RCA artifacts."""
    base = _foxglove_base(require_explicit=True)
    if not base.startswith("https://"):
        return ""
    parsed = urlsplit(base)
    hostname = parsed.hostname or ""
    if parsed.port is not None:
        return ""
    try:
        ipaddress.ip_address(hostname)
    except ValueError:
        if len(hostname.split(".")) < 2:
            return ""
    else:
        return ""
    return base


def foxglove_url(viz_mcap_vm: Any) -> str:
    """Build the CVEStudio URL for one verified delivery-contract path."""
    path = str(viz_mcap_vm or "").strip()
    if not _is_supported_viz_path(path):
        return ""

    return (
        f"{FIXED_FOXGLOVE_ORIGIN}/?ds=foxglove-http&ds.mcapPath="
        f"{quote(path, safe=FOXGLOVE_PATH_SAFE)}"
    )


def validate_foxglove_url(value: Any, viz_mcap_vm: Any) -> bool:
    """Require a byte-identical URL for the exact published viz path."""
    expected = foxglove_url(viz_mcap_vm)
    return bool(expected) and str(value or "").strip() == expected


def foxglove_delivery_fields(artifacts: dict[str, Any], *, causal_text: Any = "") -> dict[str, str]:
    """Return byte-identical fields for vm-task-sync and completion relay."""
    artifacts = artifacts if isinstance(artifacts, dict) else {}
    viz_mcap_vm = str(artifacts.get("viz_mcap_vm") or "").strip()
    attribution_causal_text = str(
        artifacts.get("attribution_causal_text") or causal_text or ""
    ).strip()
    return {
        "viz_mcap_vm": viz_mcap_vm,
        "foxglove_url": foxglove_url(viz_mcap_vm),
        "attribution_causal_text": attribution_causal_text,
    }
