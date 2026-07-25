#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import plistlib
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from hermes_cli.live_runtime import get_live_manifest, validate_live_manifest
from hermes_constants import get_live_manifest_path


OLD_WORKTREE_MARKERS = (
    "hermes-agent-sync-v0.14-overlay-20260522",
    ".venv-v014-candidate",
)

DYNAMIC_PNC_SERVICES = {
    "local.pnc.completion-notice-relay": "pnc_completion_notice_relay.py",
    "local.pnc.feishu-credential-health": "feishu_credential_cron.py",
    "local.pnc.feishu-delivery-repair": "pnc_feishu_delivery_guard.py",
    "local.pnc.meegle-auth-watchdog": "pnc_meegle_auth_watchdog.py",
    "local.pnc.rca-delivery-collector": "pnc_rca_delivery_collector.py",
    "local.pnc.rca-delivery-dispatcher": "pnc_rca_delivery_dispatcher.py",
    "local.pnc.rca-kafka-consumer": "pnc_rca_kafka_consumer.py",
    "local.pnc.rca-outbox-dispatcher": "pnc_rca_outbox_dispatcher.py",
    "local.pnc.task-dashboard.viewer": "restricted_task_dashboard_proxy.py",
    "local.pnc.vm-task-sync": "pnc_vm_task_sync.py",
}
FORBIDDEN_PNC_RUNTIME_MARKERS = (
    "/runtime/releases/",
    "/runtime/venvs/",
    "/runtime/hermes-live",
)



def _run_pnc_feishu_delivery_guard(*, repair: bool = False) -> dict[str, object]:
    # Lazy import keeps launchd-only drift guard tests fast; importing the
    # Feishu SDK is intentionally avoided unless the full guard is executed.
    from scripts.pnc_feishu_delivery_guard import repair_config, run_guard

    return repair_config() if repair else run_guard()

def read_wrapper_target() -> str:
    wrapper = Path.home() / "bin" / "hermes.current"
    if not wrapper.exists():
        return ""
    text = wrapper.read_text(encoding="utf-8")
    home_dir = str(Path.home())
    for line in text.splitlines():
        line = line.strip()
        if line.startswith('framework_root='):
            marker = '${HERMES_FRAMEWORK_ROOT:-'
            if marker in line:
                tail = line.split(marker, 1)[1]
                target = tail.split('}', 1)[0].strip('"')
                return target.replace('$HOME_DIR', home_dir)
    return ""


def read_launchd_runtime(label: str) -> dict[str, str]:
    uid = subprocess.run(["id", "-u"], capture_output=True, text=True, timeout=5).stdout.strip()
    result = subprocess.run(
        ["launchctl", "print", f"gui/{uid}/{label}"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    output = result.stdout if result.returncode == 0 else result.stderr
    parsed = {"program": "", "working_directory": "", "raw": output, "found": str(result.returncode == 0).lower()}
    for raw in output.splitlines():
        line = raw.strip()
        if line.startswith("program = "):
            parsed["program"] = line.split("=", 1)[1].strip()
        elif line.startswith("working directory = "):
            parsed["working_directory"] = line.split("=", 1)[1].strip()
    return parsed


def _validate_pnc_launchd_timer(label: str, expected_script_name: str) -> dict[str, object]:
    launchd = read_launchd_runtime(label)
    errors: list[str] = []
    plist = Path.home() / "Library" / "LaunchAgents" / f"{label}.plist"
    if launchd.get("found") != "true":
        errors.append(f"{label} launchd job is not loaded")
    if not plist.exists():
        errors.append(f"{label} plist missing: {plist}")
    raw = str(launchd.get("raw") or "")
    if label in DYNAMIC_PNC_SERVICES:
        launcher = str(
            Path.home()
            / ".hermes"
            / "runtime"
            / "governance-tools"
            / "pnc_live_exec.py"
        )
        if raw and (launcher not in raw or label not in raw):
            errors.append(f"{label} loaded definition bypasses the active runtime launcher")
        if any(marker in raw for marker in FORBIDDEN_PNC_RUNTIME_MARKERS):
            errors.append(f"{label} loaded definition contains a pinned runtime path")
        try:
            body = plistlib.loads(plist.read_bytes())
        except (OSError, ValueError, plistlib.InvalidFileException):
            body = {}
        arguments = body.get("ProgramArguments") if isinstance(body, dict) else None
        environment = body.get("EnvironmentVariables") if isinstance(body, dict) else None
        persisted_text = json.dumps(body, sort_keys=True) if body else ""
        if arguments is not None and arguments[:3] != ["/usr/bin/python3", launcher, label]:
            errors.append(f"{label} persisted definition bypasses the active runtime launcher")
        if not isinstance(environment, dict) or "VIRTUAL_ENV" in environment:
            errors.append(f"{label} persisted definition pins a virtual environment")
        if any(marker in persisted_text for marker in FORBIDDEN_PNC_RUNTIME_MARKERS):
            errors.append(f"{label} persisted definition contains a pinned runtime path")
    else:
        expected_script = str(
            Path.home()
            / ".hermes"
            / "runtime"
            / "hermes-live"
            / "scripts"
            / expected_script_name
        )
        if raw and expected_script not in raw:
            errors.append(f"{label} does not point at live {expected_script_name}")
    if expected_script_name == "pnc_vm_task_sync.py" and "--include-terminal" not in raw:
        errors.append(f"{label} must include --include-terminal so completed VM tasks keep syncing")
    if expected_script_name == "pnc_completion_notice_relay.py":
        if "--send" not in raw:
            errors.append(f"{label} must include --send so pending completion notices are relayed")
        if "--retry-failed-after" not in raw:
            errors.append(f"{label} must include --retry-failed-after for bounded transient failure retries")
        if "--max-attempts" not in raw:
            errors.append(f"{label} must include --max-attempts to avoid infinite retry loops")
        if "--watch" not in raw:
            errors.append(f"{label} must include --watch so completion notices use the resident hot relay")
        raw_lower = raw.lower()
        if "properties = keepalive" not in raw_lower and "keepalive => true" not in raw_lower and "keepalive = 1" not in raw_lower:
            errors.append(f"{label} must use KeepAlive=true so the resident relay is restarted")
        if "com.apple.launchd.WatchPaths" in raw:
            errors.append(f"{label} must not use WatchPaths in resident --watch mode")
    if expected_script_name == "pnc_feishu_delivery_guard.py" and "--repair" not in raw:
        errors.append(f"{label} must include --repair so group-policy drift is self-healing")
    return {
        "ok": not errors,
        "label": label,
        "plist": str(plist),
        "loaded": launchd.get("found") == "true",
        "errors": errors,
    }


def validate_pnc_vm_task_sync_launchd() -> dict[str, object]:
    """Check that the delivery VM sidecar sync timer is installed."""
    return _validate_pnc_launchd_timer("local.pnc.vm-task-sync", "pnc_vm_task_sync.py")


def validate_pnc_completion_notice_relay_launchd() -> dict[str, object]:
    """Check that the Feishu completion notice relay timer is installed."""
    return _validate_pnc_launchd_timer("local.pnc.completion-notice-relay", "pnc_completion_notice_relay.py")


def validate_pnc_feishu_delivery_repair_launchd() -> dict[str, object]:
    """Check that the PNC Feishu delivery config repair timer is installed."""
    return _validate_pnc_launchd_timer("local.pnc.feishu-delivery-repair", "pnc_feishu_delivery_guard.py")


def read_active_entrypoint_texts() -> dict[str, str]:
    home = Path.home()
    paths = [
        home / "bin" / "hermes",
        home / "bin" / "hermes.current",
        home / ".hermes" / "env.sh",
        home / "Library" / "LaunchAgents" / "ai.hermes.gateway.plist",
        home / "Library" / "LaunchAgents" / "ai.hermes.dashboard.plist",
        home / "Library" / "LaunchAgents" / "ai.hermes.dashboard.lan.plist",
        home / "Library" / "LaunchAgents" / "ai.hermes.dashboard.tasks-only.plist",
        home / "Library" / "LaunchAgents" / "ai.hermes.dashboard.viewer-only.plist",
    ]
    texts: dict[str, str] = {}
    for path in paths:
        try:
            texts[str(path)] = path.read_text(encoding="utf-8")
        except OSError:
            continue
    return texts


def read_health_status() -> dict[str, object]:
    result = subprocess.run(
        ["curl", "-fsS", "http://127.0.0.1:18789/health"],
        capture_output=True,
        text=True,
        timeout=5,
    )
    if result.returncode != 0:
        return {"ok": False, "raw": (result.stderr or result.stdout).strip()}
    raw = result.stdout.strip()
    try:
        payload = json.loads(raw)
    except Exception:
        return {"ok": False, "raw": raw}
    return {"ok": payload.get("status") == "ok", "raw": raw}


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate Hermes single-live-root manifest")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    parser.add_argument(
        "--strict-governance",
        action="store_true",
        help="Fail if the manifest records a dirty promotion source",
    )
    parser.add_argument(
        "--repair-pnc-feishu-delivery",
        action="store_true",
        help="Repair the minimal PNC Feishu delivery config contract before checking",
    )
    args = parser.parse_args()

    manifest = get_live_manifest()
    errors = validate_live_manifest(manifest)
    warnings: list[str] = []
    runtime_root = str(manifest.get("runtime_root") or "")
    runtime_python = str(manifest.get("runtime_python") or "")
    runtime_venv = str(manifest.get("runtime_venv") or "")
    wrapper_target = read_wrapper_target()
    launchd = read_launchd_runtime("ai.hermes.gateway")
    health = read_health_status()
    dirty_count = int(manifest.get("promotion_source_dirty_count") or 0)
    promotion_source = str(manifest.get("promotion_source") or "")
    entrypoint_texts = read_active_entrypoint_texts()
    pnc_feishu_delivery = _run_pnc_feishu_delivery_guard(repair=args.repair_pnc_feishu_delivery)
    pnc_vm_task_sync = validate_pnc_vm_task_sync_launchd()
    pnc_completion_notice_relay = validate_pnc_completion_notice_relay_launchd()
    pnc_feishu_delivery_repair = validate_pnc_feishu_delivery_repair_launchd()

    if runtime_root and wrapper_target and Path(wrapper_target).resolve() != Path(runtime_root).resolve():
        errors.append(f"wrapper target drift: {wrapper_target}")
    if runtime_root:
        venv_link = Path(runtime_root) / ".venv"
        if not venv_link.exists():
            errors.append(f"runtime .venv missing: {venv_link}")
        elif runtime_venv and venv_link.resolve() != Path(runtime_venv).resolve():
            errors.append(f"runtime .venv drift: {venv_link.resolve()} != {Path(runtime_venv).resolve()}")
    if runtime_venv and "/worktrees/" in runtime_venv:
        errors.append(f"runtime venv must not live under worktrees: {runtime_venv}")
    if runtime_python and launchd.get("program") and Path(launchd["program"]).resolve() != Path(runtime_python).resolve():
        errors.append(f"launchd program drift: {launchd['program']}")
    manifest_workdir = str(manifest.get("gateway_working_directory") or runtime_root)
    if manifest_workdir and launchd.get("working_directory") and Path(launchd["working_directory"]).resolve() != Path(manifest_workdir).resolve():
        errors.append(f"launchd working directory drift: {launchd['working_directory']}")
    if not health.get("ok"):
        errors.append(f"health check failed: {health.get('raw')}")
    if dirty_count > 0:
        msg = (
            f"promotion source is dirty ({dirty_count} entries): "
            f"{promotion_source or 'unknown'}; runtime is isolated but promote/release is not clean"
        )
        if args.strict_governance:
            errors.append(msg)
        else:
            warnings.append(msg)
    stale_refs = []
    for path, text in entrypoint_texts.items():
        for marker in OLD_WORKTREE_MARKERS:
            if marker in text:
                stale_refs.append(f"{path}: {marker}")
    if stale_refs:
        errors.extend(f"stale active entrypoint reference: {ref}" for ref in stale_refs)
    if not pnc_feishu_delivery.get("ok"):
        errors.extend(
            f"PNC Feishu delivery drift: {err}"
            for err in pnc_feishu_delivery.get("errors", [])
        )
    if not pnc_vm_task_sync.get("ok"):
        errors.extend(
            f"PNC VM task sync drift: {err}"
            for err in pnc_vm_task_sync.get("errors", [])
        )
    if not pnc_completion_notice_relay.get("ok"):
        errors.extend(
            f"PNC completion notice relay drift: {err}"
            for err in pnc_completion_notice_relay.get("errors", [])
        )
    if not pnc_feishu_delivery_repair.get("ok"):
        errors.extend(
            f"PNC Feishu delivery repair drift: {err}"
            for err in pnc_feishu_delivery_repair.get("errors", [])
        )
    warnings.extend(
        f"PNC Feishu delivery warning: {warn}"
        for warn in pnc_feishu_delivery.get("warnings", [])
    )

    payload = {
        "ok": not errors,
        "governance_ok": not warnings and not errors,
        "manifest_path": str(get_live_manifest_path()),
        "runtime_root": runtime_root,
        "runtime_python": runtime_python,
        "runtime_venv": runtime_venv,
        "wrapper_target": wrapper_target,
        "launchd": launchd,
        "health": health,
        "promotion_source": promotion_source,
        "promotion_source_dirty_count": dirty_count,
        "pnc_feishu_delivery": pnc_feishu_delivery,
        "pnc_vm_task_sync": pnc_vm_task_sync,
        "pnc_completion_notice_relay": pnc_completion_notice_relay,
        "pnc_feishu_delivery_repair": pnc_feishu_delivery_repair,
        "warnings": warnings,
        "errors": errors,
    }

    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        status = "OK" if payload["ok"] else "DRIFT"
        print(f"[{status}] Hermes live manifest")
        print(f"manifest_path: {payload['manifest_path']}")
        if payload["runtime_root"]:
            print(f"runtime_root: {payload['runtime_root']}")
        if payload["runtime_python"]:
            print(f"runtime_python: {payload['runtime_python']}")
        if payload["wrapper_target"]:
            print(f"wrapper_target: {payload['wrapper_target']}")
        if payload["launchd"].get("program"):
            print(f"launchd_program: {payload['launchd']['program']}")
        if payload["launchd"].get("working_directory"):
            print(f"launchd_working_directory: {payload['launchd']['working_directory']}")
        if warnings:
            print("warnings:")
            for warn in warnings:
                print(f"- {warn}")
        print(f"pnc_feishu_delivery: {'OK' if pnc_feishu_delivery.get('ok') else 'DRIFT'}")
        print(f"pnc_vm_task_sync: {'OK' if pnc_vm_task_sync.get('ok') else 'DRIFT'}")
        print(f"pnc_completion_notice_relay: {'OK' if pnc_completion_notice_relay.get('ok') else 'DRIFT'}")
        print(f"pnc_feishu_delivery_repair: {'OK' if pnc_feishu_delivery_repair.get('ok') else 'DRIFT'}")
        if errors:
            print("errors:")
            for err in errors:
                print(f"- {err}")
    return 0 if payload["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
