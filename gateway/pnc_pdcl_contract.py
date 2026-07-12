"""Strict host-side PDCL/MDI read-only download command contract."""
from __future__ import annotations

import re
import shlex
from typing import Any

ALLOWED_RESOURCES = {"raw", "clip", "event", "eventset", "group", "refresh", "refresh2"}
FORBIDDEN_VERBS = {"upload", "login", "logout", "upgrade", "utils"}
ID_VALUE_RE = re.compile(r"^[A-Za-z0-9_,:-]+$")
RAW_VALUE_RE = re.compile(r"^[A-Za-z0-9_,:/.-]+$")
DANGEROUS_CHARS = set(";|&$><`(){}")
ADDRESS_FLAGS = {
    "-u": ("clip_ukeys", ID_VALUE_RE),
    "--clip-ukeys": ("clip_ukeys", ID_VALUE_RE),
    "-t": ("ticket_ids", ID_VALUE_RE),
    "--ticket-ids": ("ticket_ids", ID_VALUE_RE),
    "-e": ("event_ids", ID_VALUE_RE),
    "--event-id": ("event_ids", ID_VALUE_RE),
    "--event-ids": ("event_ids", ID_VALUE_RE),
    "-r": ("raw_refs", RAW_VALUE_RE),
}
SAVE_FLAGS = {"-s", "--save-path"}
REBUILD_KEYS = ("ticket_ids", "event_ids", "clip_ukeys", "raw_refs")


def _split_values(value: str, pattern: re.Pattern[str]) -> list[str] | None:
    text = str(value or "").strip()
    if not text or any(ch.isspace() for ch in text) or any(ch in DANGEROUS_CHARS for ch in text):
        return None
    if not pattern.fullmatch(text):
        return None
    parts = [part for part in text.split(",") if part]
    return parts or None


def _safe_save_value(value: str) -> bool:
    text = str(value or "").strip()
    return bool(text) and not text.startswith("-") and not any(ch.isspace() or ch in DANGEROUS_CHARS for ch in text)


def parse_pdcl_command(cmd: str) -> dict[str, Any] | None:
    text = str(cmd or "").strip()
    if not text or chr(34) in text or chr(39) in text:
        return None
    try:
        tokens = shlex.split(text, posix=True)
    except ValueError:
        return None
    if len(tokens) < 3 or tokens[0] != "mdi":
        return None
    if any(any(ch in DANGEROUS_CHARS for ch in token) for token in tokens):
        return None
    if any("=" in token and token.startswith("-") for token in tokens):
        return None

    head = tokens[1]
    if head in FORBIDDEN_VERBS:
        return None
    if head == "download":
        if len(tokens) < 4:
            return None
        form = "download"
        verb = tokens[2]
        idx = 3
    else:
        form = "direct"
        verb = head
        idx = 2
    if verb not in ALLOWED_RESOURCES or verb in FORBIDDEN_VERBS:
        return None

    parsed: dict[str, Any] = {"verb": verb, "form": form, "ticket_ids": [], "event_ids": [], "clip_ukeys": [], "raw_refs": []}
    while idx < len(tokens):
        flag = tokens[idx]
        if flag == "-f" or flag.startswith("-f"):
            return None
        if flag in SAVE_FLAGS:
            if idx + 1 >= len(tokens) or not _safe_save_value(tokens[idx + 1]):
                return None
            idx += 2
            continue
        spec = ADDRESS_FLAGS.get(flag)
        if spec is None:
            return None
        if flag == "-r" and verb != "raw":
            return None
        if idx + 1 >= len(tokens):
            return None
        key, pattern = spec
        values = _split_values(tokens[idx + 1], pattern)
        if values is None:
            return None
        parsed[key].extend(values)
        idx += 2
    if not any(parsed[key] for key in REBUILD_KEYS):
        return None
    return parsed


def is_valid_pdcl_download_cmd(value: str) -> bool:
    return parse_pdcl_command(value) is not None


def classify_invalid_pdcl(value: str) -> str:
    """Classify an invalid/blank PDCL data-address value for user guidance.

    This helper is diagnostic only: it must not expand the accepted command
    contract.  ``is_valid_pdcl_download_cmd`` remains the sole allowlist.
    """
    text = str(value or "").strip()
    if not text:
        return "empty"
    lowered = text.lower()
    if "cyber_recorder" in lowered or "play" in lowered or re.search(r"(?:^|\s)-f\s+\S+\.record(?:\s|$)", lowered):
        return "replay_cmd"
    if lowered.startswith("/") or "/media/nas" in lowered:
        return "nas_path"
    if not lowered.startswith("mdi"):
        return "non_mdi"
    return "bad_mdi_form"
