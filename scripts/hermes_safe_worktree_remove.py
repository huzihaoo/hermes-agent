#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from hermes_cli.live_runtime import get_live_manifest


def run(cmd: list[str], *, cwd: Path | None = None, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        capture_output=True,
        text=True,
        timeout=60,
        check=check,
    )


def resolve_existing_or_parent(path: Path) -> Path:
    if path.exists():
        return path.resolve()
    parent = path.parent
    if parent.exists():
        return parent.resolve() / path.name
    return path


def nested_or_equal(path: Path, protected: Path) -> bool:
    try:
        path.relative_to(protected)
        return True
    except ValueError:
        return False


def dirty_entries(path: Path) -> list[str]:
    result = run(["git", "status", "--short"], cwd=path)
    return [line for line in result.stdout.splitlines() if line.strip()]


def worktree_paths(repo_root: Path) -> set[Path]:
    result = run(["git", "worktree", "list", "--porcelain"], cwd=repo_root)
    paths: set[Path] = set()
    for line in result.stdout.splitlines():
        if line.startswith("worktree "):
            paths.add(Path(line.split(" ", 1)[1]).resolve())
    return paths


def safe_slug(path: Path) -> str:
    return path.name.replace("/", "_").replace(" ", "_")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def maintenance_open_handle_probe(target: Path) -> dict[str, object]:
    """Run the bounded local probe used by maintenance-only retirement.

    The full production drift guard is intentionally broader than a worktree
    removal.  Maintenance still needs an independent, fail-closed consumer
    check so an unrelated resident drift cannot turn into a deletion bypass.
    """
    probe = Path.home() / ".hermes" / "workspace-work" / "bin" / "context_open_handle_probe.py"
    if not probe.exists():
        return {"ok": False, "error": f"open-handle probe missing: {probe}"}
    try:
        result = run(
            [
                sys.executable,
                str(probe),
                "--full-tree-no-follow",
                "--max-nodes",
                "200000",
                str(target),
            ],
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return {"ok": False, "error": f"open-handle probe failed: {exc}"}
    # The probe contract is: 1 = no handle, 0 = handle, 2 = unable to prove.
    if result.returncode == 1:
        return {"ok": True, "observed": False, "raw": result.stdout[-2000:]}
    if result.returncode == 0:
        return {
            "ok": False,
            "observed": True,
            "raw": (result.stdout or result.stderr)[-4000:],
            "error": "open handle observed",
        }
    return {
        "ok": False,
        "observed": None,
        "raw": (result.stdout or result.stderr)[-4000:],
        "error": f"open-handle probe could not prove absence (rc={result.returncode})",
    }


def maintenance_lease_probe(target: Path) -> dict[str, object]:
    """Reject a currently leased target; report stale lease metadata only."""
    lease_path = Path.home() / ".codex" / "runtime" / "post-baseline-debt-guard" / "leases.json"
    if not lease_path.exists():
        return {"ok": True, "active": False, "source": str(lease_path), "warning": "lease registry missing"}
    try:
        payload = json.loads(lease_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return {"ok": False, "active": None, "source": str(lease_path), "error": f"lease registry unreadable: {exc}"}
    now = datetime.now(timezone.utc)
    active: list[dict[str, object]] = []
    stale: list[dict[str, object]] = []
    for lease in payload.get("leases", []) if isinstance(payload, dict) else []:
        if not isinstance(lease, dict) or lease.get("path") != str(target):
            continue
        if lease.get("disposition") != "active":
            continue
        raw_expiry = str(lease.get("expires_at") or "")
        try:
            expiry = datetime.fromisoformat(raw_expiry.replace("Z", "+00:00"))
        except ValueError:
            active.append(lease)
            continue
        if expiry > now:
            active.append(lease)
        else:
            stale.append(lease)
    if active:
        return {"ok": False, "active": True, "source": str(lease_path), "leases": active}
    result: dict[str, object] = {"ok": True, "active": False, "source": str(lease_path)}
    if stale:
        result["stale_active_records"] = stale
        result["warning"] = "expired active lease record observed; open-handle proof still required"
    return result


def maintenance_runtime_health() -> dict[str, object]:
    """Keep the live runtime health gate without requiring every drift check."""
    try:
        result = run(["curl", "-fsS", "--max-time", "5", "http://127.0.0.1:18789/health"], check=False)
    except (OSError, subprocess.SubprocessError) as exc:
        return {"ok": False, "error": f"health probe failed: {exc}"}
    raw = (result.stdout or result.stderr).strip()
    try:
        value = json.loads(raw)
    except ValueError:
        return {"ok": False, "raw": raw[-1000:], "error": "health response was not JSON"}
    return {"ok": result.returncode == 0 and value.get("status") == "ok", "raw": raw[-1000:]}


def create_archive_bundle(target: Path, repo_root: Path, archive_root: Path, dirty: list[str]) -> dict[str, object]:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    name = safe_slug(target)
    evidence_dir = archive_root / "evidence" / f"{name}-{stamp}"
    tar_path = archive_root / f"{name}-{stamp}.tar.gz"
    archive_root.mkdir(parents=True, exist_ok=True)
    evidence_dir.mkdir(parents=True, exist_ok=True)

    metadata = {
        "created_at": stamp,
        "target": str(target),
        "repo_root": str(repo_root),
        "dirty_count": len(dirty),
        "dirty_preview": dirty[:50],
    }
    write_text(evidence_dir / "metadata.json", json.dumps(metadata, ensure_ascii=False, indent=2) + "\n")
    for filename, cmd in {
        "status-short.txt": ["git", "status", "--short"],
        "head.txt": ["git", "rev-parse", "HEAD"],
        "branch.txt": ["git", "branch", "--show-current"],
        "diff-stat.txt": ["git", "diff", "--stat"],
        "diff.patch": ["git", "diff", "--binary"],
        "diff-cached.patch": ["git", "diff", "--cached", "--binary"],
        "untracked-files.txt": ["git", "ls-files", "--others", "--exclude-standard"],
    }.items():
        result = run(cmd, cwd=target, check=False)
        write_text(evidence_dir / filename, result.stdout)
        if result.stderr:
            write_text(evidence_dir / f"{filename}.stderr", result.stderr)

    tar_result = run(
        ["tar", "-C", str(target.parent), "-czf", str(tar_path), target.name],
        check=False,
    )
    if tar_result.returncode != 0:
        raise RuntimeError((tar_result.stderr or tar_result.stdout).strip() or "tar archive failed")
    archive_sha = sha256_file(tar_path)
    write_text(tar_path.with_suffix(tar_path.suffix + ".sha256"), f"{archive_sha}  {tar_path}\n")
    return {
        "evidence_dir": str(evidence_dir),
        "archive_path": str(tar_path),
        "archive_sha256": archive_sha,
        "archive_sha256_path": str(tar_path.with_suffix(tar_path.suffix + ".sha256")),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Governed Hermes worktree removal")
    parser.add_argument("path", help="Absolute path to the worktree to remove")
    parser.add_argument("--repo-root", default=str(Path.home() / ".hermes" / "hermes-agent"))
    parser.add_argument("--dry-run", action="store_true", help="Validate and print the planned action")
    parser.add_argument("--force", action="store_true", help="Pass --force to git worktree remove")
    parser.add_argument("--allow-dirty", action="store_true", help="Allow removal of dirty non-live worktrees")
    parser.add_argument(
        "--maintenance-only",
        action="store_true",
        help=(
            "Use the scoped maintenance gates (health, lease, open-handle, protected paths) "
            "and report unrelated live drift instead of blocking this worktree cleanup"
        ),
    )
    parser.add_argument(
        "--archive-root",
        default=str(Path.home() / ".hermes" / "runtime" / "worktree-governance" / "archives"),
        help="Where to save automatic evidence and tar archives before removing dirty worktrees",
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    target_arg = Path(args.path).expanduser()
    errors: list[str] = []
    warnings: list[str] = []
    if not target_arg.is_absolute():
        errors.append("target path must be absolute")
    target = resolve_existing_or_parent(target_arg)
    repo_root = Path(args.repo_root).expanduser().resolve()
    archive_root = Path(args.archive_root).expanduser().resolve()

    manifest = get_live_manifest()
    protected_values = {
        "runtime_root": manifest.get("runtime_root"),
        "runtime_venv": manifest.get("runtime_venv"),
        "promotion_source": manifest.get("promotion_source"),
        "canonical_integration_root": manifest.get("canonical_integration_root"),
    }
    protected_paths = {
        key: Path(str(value)).expanduser().resolve()
        for key, value in protected_values.items()
        if value
    }

    for key, protected in protected_paths.items():
        if target == protected or nested_or_equal(target, protected) or nested_or_equal(protected, target):
            errors.append(f"refusing to remove live-protected path ({key}): {protected}")

    try:
        registered_paths = worktree_paths(repo_root)
    except Exception as exc:
        registered_paths = set()
        errors.append(f"cannot read git worktree registry: {exc}")
    if target not in registered_paths:
        errors.append(f"target is not a registered worktree for {repo_root}: {target}")

    dirty: list[str] = []
    if target.exists() and target in registered_paths:
        try:
            dirty = dirty_entries(target)
        except Exception as exc:
            errors.append(f"cannot inspect target dirty state: {exc}")
    if dirty and not args.allow_dirty:
        errors.append(f"target has {len(dirty)} dirty entries; rerun with --allow-dirty only after review")

    guard_payload: dict[str, object] = {}
    guard_cmd = [str(Path.home() / "bin" / "hermes-live-drift-guard"), "--strict-governance", "--json"]
    guard = run(guard_cmd, check=False)
    if guard.stdout.strip():
        try:
            guard_payload = json.loads(guard.stdout)
        except json.JSONDecodeError:
            guard_payload = {"raw": guard.stdout.strip()}
    maintenance: dict[str, object] = {}
    if args.maintenance_only:
        lease_probe = maintenance_lease_probe(target)
        handle_probe = maintenance_open_handle_probe(target)
        health_probe = maintenance_runtime_health()
        maintenance = {
            "lease": lease_probe,
            "open_handle": handle_probe,
            "health": health_probe,
        }
        if not lease_probe.get("ok"):
            errors.append("active or unreadable target lease; refusing maintenance removal")
        if not handle_probe.get("ok"):
            errors.append("target open-handle gate failed; refusing maintenance removal")
        if not health_probe.get("ok"):
            errors.append("live runtime health gate failed; refusing maintenance removal")
        if guard.returncode != 0:
            warnings.append(
                "strict live drift guard is red; preserved in receipt as unrelated maintenance drift"
            )
    elif guard.returncode != 0:
        errors.append("live drift guard failed; refusing worktree removal")

    payload = {
        "ok": not errors,
        "dry_run": args.dry_run,
        "target": str(target),
        "repo_root": str(repo_root),
        "archive_root": str(archive_root),
        "dirty_count": len(dirty),
        "dirty_preview": dirty[:20],
        "protected_paths": {key: str(value) for key, value in protected_paths.items()},
        "guard": guard_payload,
        "maintenance": maintenance,
        "warnings": warnings,
        "errors": errors,
    }

    if not errors and not args.dry_run:
        if dirty:
            try:
                payload["archive"] = create_archive_bundle(target, repo_root, archive_root, dirty)
            except Exception as exc:
                payload["ok"] = False
                payload["errors"].append(f"archive failed; refusing removal: {exc}")
                if args.json:
                    print(json.dumps(payload, ensure_ascii=False, indent=2))
                else:
                    print("REFUSED")
                    print(f"target: {payload['target']}")
                    print(f"error: {payload['errors'][-1]}")
                return 2
        cmd = ["git", "worktree", "remove"]
        if args.force:
            cmd.append("--force")
        cmd.append(str(target))
        result = run(cmd, cwd=repo_root, check=False)
        payload["command"] = cmd
        payload["remove_returncode"] = result.returncode
        payload["remove_stdout"] = result.stdout.strip()
        payload["remove_stderr"] = result.stderr.strip()
        if result.returncode != 0:
            payload["ok"] = False
            payload["errors"].append("git worktree remove failed")

    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print("OK" if payload["ok"] else "REFUSED")
        print(f"target: {payload['target']}")
        print(f"dry_run: {payload['dry_run']}")
        print(f"dirty_count: {payload['dirty_count']}")
        for warning in warnings:
            print(f"warning: {warning}")
        for error in payload["errors"]:
            print(f"error: {error}")
    return 0 if payload["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
