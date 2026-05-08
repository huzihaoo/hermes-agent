"""Stable Hermes version-reporting helpers.

The version command is intentionally import-light: release-flow modules expose
``__version__`` in files that may otherwise import gateway controllers or other
runtime-heavy dependencies.  Read those strings from source instead of importing
module packages.
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

try:
    from hermes_cli import __release_date__, __version__
except Exception:  # pragma: no cover - defensive fallback for partial imports
    __version__ = "0.0.0"
    __release_date__ = "unknown"


PROJECT_ROOT = Path(__file__).parent.parent.resolve()
_VERSION_RE = re.compile(r"__version__\s*=\s*['\"]([^'\"]+)['\"]")


@dataclass(frozen=True)
class VersionedModule:
    path: str
    label: str


VERSIONED_MODULES: tuple[VersionedModule, ...] = (
    VersionedModule("gateway/admission", "Admission Control"),
    VersionedModule("gateway/tasks", "Task Product Layer"),
    VersionedModule("gateway/observability", "Observability"),
    VersionedModule("agent/session_health.py", "Session Health"),
    VersionedModule("agent/session_handoff.py", "Session Handoff"),
)


def _version_file(module_path: str) -> Path:
    path = PROJECT_ROOT / module_path
    if path.is_dir():
        return path / "__init__.py"
    return path


def read_module_version(module_path: str) -> str:
    """Return a module's ``__version__`` string, or ``unavailable``."""
    try:
        content = _version_file(module_path).read_text(encoding="utf-8")
    except OSError:
        return "unavailable"
    match = _VERSION_RE.search(content)
    if not match:
        return "unavailable"
    return match.group(1)


def iter_module_versions(
    modules: Iterable[VersionedModule] = VERSIONED_MODULES,
) -> list[tuple[str, str]]:
    """Return ``(label, version)`` pairs for release-flow modules."""
    return [(module.label, read_module_version(module.path)) for module in modules]


def format_version_report(*, include_python: bool = True) -> str:
    """Return the stable human-facing Hermes version report."""
    lines = [f"Hermes Agent v{__version__} ({__release_date__})"]
    if include_python:
        lines.append(f"Python: {sys.version.split()[0]}")
    lines.append("Modules:")
    for label, version in iter_module_versions():
        if version == "unavailable":
            lines.append(f"  {label}: unavailable")
        else:
            lines.append(f"  {label}: v{version}")
    return "\n".join(lines)
