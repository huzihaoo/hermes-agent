"""Host-side artifact helpers for PNC/G1Q3 RCA VM handoff files."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable


MNT_TMP_PREFIX = "/mnt/tmp/"


def local_candidates_for_vm_path(vm_path: str, *, home: Path | None = None) -> list[Path]:
    """Return local mount candidates for a VM `/mnt/tmp/...` artifact path."""
    text = str(vm_path or "").strip()
    if not text.startswith(MNT_TMP_PREFIX):
        return []
    home = home or Path.home()
    rel = text[len(MNT_TMP_PREFIX):].lstrip("/")
    if not rel or any(part in {".", ".."} for part in rel.split("/")):
        return []
    return [
        home / "Mounts" / "mini_root" / "mnt" / "tmp" / rel,
        home / "Mounts" / "department-pnc_team-planning_algo-driving" / "tmp" / rel,
    ]


def write_vm_tmp_text(vm_path: str, content: str, *, home: Path | None = None) -> Path | None:
    """Best-effort write of a VM `/mnt/tmp/...` text artifact through local mounts.

    Returns the local path that was written, or None if no candidate mount is
    available/writable.  The caller still passes the VM path to workers.
    """
    for candidate in local_candidates_for_vm_path(vm_path, home=home):
        parent = candidate.parent
        try:
            parent.mkdir(parents=True, exist_ok=True)
            probe = parent / f".hermes_write_probe_{candidate.name}"
            probe.write_text("ok", encoding="utf-8")
            probe.unlink(missing_ok=True)
            candidate.write_text(content, encoding="utf-8")
            return candidate
        except OSError:
            continue
    return None
