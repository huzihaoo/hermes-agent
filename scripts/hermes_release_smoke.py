#!/usr/bin/env python3
"""Hermes host release smoke harness.

This script captures the local pre-release checks that caught the v0.13 rebase
regressions:

* gateway health must be reachable and report feishu/api_server connected;
* local config must not use deprecated MESSAGING_CWD/TERMINAL_CWD env bridges;
* long-running agent budget must be high enough for observed workloads;
* local CLI/launchd should point at the expected v0.13 runtime;
* recent gateway error logs should not contain high-signal import/name errors;
* optional VM access can be checked via ssh-mini-agent doctor.

The smoke is intentionally host-side. It does not call Feishu APIs and does not
send messages; pair it with ``scripts/feishu_admission_smoke.py`` when admission
routing also needs to be exercised.
"""
from __future__ import annotations

import argparse
import json
import os
import plistlib
import re
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

try:
    import yaml
except Exception:  # pragma: no cover - PyYAML is part of Hermes envs, but keep script import-safe.
    yaml = None  # type: ignore[assignment]

REPO_ROOT = Path(__file__).resolve().parents[1]
HIGH_SIGNAL_LOG_PATTERNS = (
    "Traceback",
    "NameError",
    "ImportError",
    "ModuleNotFoundError",
    "AttributeError",
    "API call failed",
    "Deprecated .env settings detected",
    "MESSAGING_CWD=",
    "TERMINAL_CWD=",
)
DEPRECATED_ENV_KEYS = ("MESSAGING_CWD", "TERMINAL_CWD")
DEFAULT_RECOMMENDED_TURN_BUDGET = 2500


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except FileNotFoundError:
        return ""


def _load_yaml(path: Path) -> dict[str, Any]:
    if yaml is None or not path.exists():
        return {}
    data = yaml.safe_load(_read_text(path)) or {}
    return data if isinstance(data, dict) else {}


def _load_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in _read_text(path).splitlines():
        if not line or line.lstrip().startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def _redact_url(url: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    if parsed.query:
        parsed = parsed._replace(query="[REDACTED]")
    if parsed.fragment:
        parsed = parsed._replace(fragment="[REDACTED]")
    return urllib.parse.urlunsplit(parsed)


def _fetch_json(url: str, timeout: float) -> dict[str, Any]:
    safe_url = _redact_url(url)
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        return {
            "status": "unavailable",
            "error": "invalid_json",
            "reason": str(exc),
            "url": safe_url,
        }
    except urllib.error.HTTPError as exc:
        try:
            exc.read()
        except Exception:
            pass
        return {
            "status": "unavailable",
            "error": "http_error",
            "http_status": exc.code,
            "reason": exc.reason,
            "body_head": "[omitted]",
            "url": safe_url,
        }
    except urllib.error.URLError as exc:
        return {
            "status": "unavailable",
            "error": "url_error",
            "reason": str(exc.reason),
            "url": safe_url,
        }
    except TimeoutError as exc:
        return {
            "status": "unavailable",
            "error": "timeout",
            "reason": str(exc),
            "url": safe_url,
        }


def _run(cmd: list[str], timeout: float) -> dict[str, Any]:
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError as exc:
        return {"ok": False, "returncode": 127, "stdout": "", "stderr": str(exc), "cmd": cmd}
    except subprocess.TimeoutExpired as exc:
        return {
            "ok": False,
            "returncode": None,
            "stdout": exc.stdout or "",
            "stderr": exc.stderr or f"timeout after {timeout}s",
            "cmd": cmd,
        }
    return {
        "ok": proc.returncode == 0,
        "returncode": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "cmd": cmd,
    }


def _tail_lines(path: Path, max_lines: int, *, start_offset: int = 0) -> list[str]:
    try:
        with path.open("rb") as fh:
            if start_offset > 0:
                fh.seek(start_offset)
            text = fh.read().decode("utf-8", errors="replace")
    except FileNotFoundError:
        return []
    if not text:
        return []
    return text.splitlines()[-max_lines:]


def _smoke_receipt_summary(result: dict[str, Any]) -> dict[str, Any]:
    health = result.get("health", {}) if isinstance(result.get("health"), dict) else {}
    platforms = health.get("platforms", {}) if isinstance(health.get("platforms"), dict) else {}
    logs = result.get("logs", {}) if isinstance(result.get("logs"), dict) else {}
    return {
        "ok": result.get("ok"),
        "errors": result.get("errors", []),
        "gateway_state": health.get("gateway_state"),
        "health_status": health.get("status"),
        "pid": health.get("pid"),
        "feishu_state": platforms.get("feishu", {}).get("state"),
        "api_server_state": platforms.get("api_server", {}).get("state"),
        "log_start_offset": logs.get("log_start_offset"),
        "log_start_at_end": logs.get("log_start_at_end"),
        "current_size": logs.get("current_size"),
        "high_signal_count": len(logs.get("high_signal_tail") or []),
    }


def _resolve_log_start_offset(path: Path, raw_offset: int | None, *, from_end: bool = False) -> int:
    if not path.exists():
        return 0
    current_size = path.stat().st_size
    if from_end:
        return current_size
    offset = raw_offset or 0
    if offset < 0:
        return 0
    if offset > current_size:
        return current_size
    return offset


def _is_process_command_echo(line: str) -> bool:
    """Return True for process-list command echoes that contain high-signal words.

    Gateway shutdown diagnostics include full shell command lines from unrelated
    Hermes invocations. Those commands often contain grep patterns such as
    ``Traceback|NameError`` and should not make the release smoke red.
    """

    return bool(re.match(r"^\s*\S+\s+\d+\s+", line)) and any(
        marker in line for marker in (" /bin/bash -c ", " /bin/zsh -c ", " grep ", " rg ", " sed ")
    )


def _high_signal_log_lines(path: Path, max_lines: int, *, start_offset: int = 0) -> list[str]:
    return [
        line
        for line in _tail_lines(path, max_lines, start_offset=start_offset)
        if not _is_process_command_echo(line) and any(pattern in line for pattern in HIGH_SIGNAL_LOG_PATTERNS)
    ]


def collect_smoke(args: argparse.Namespace) -> dict[str, Any]:
    hermes_home = Path(args.hermes_home).expanduser().resolve()
    config_path = Path(args.config).expanduser().resolve() if args.config else hermes_home / "config.yaml"
    env_path = Path(args.env).expanduser().resolve() if args.env else hermes_home / ".env"
    launchd_plist = Path(args.launchd_plist).expanduser().resolve() if args.launchd_plist else Path.home() / "Library/LaunchAgents/ai.hermes.gateway.plist"
    gateway_error_log = Path(args.gateway_error_log).expanduser().resolve() if args.gateway_error_log else hermes_home / "logs/gateway.error.log"
    sync_script = Path(args.sync_script).expanduser().resolve() if args.sync_script else Path.home() / "bin/hermes-sync-openclaw-runtime"
    cli = Path(args.cli).expanduser().resolve() if args.cli else Path.home() / "bin/hermes"

    config = _load_yaml(config_path)
    env = _load_env(env_path)
    agent_cfg = config.get("agent") if isinstance(config.get("agent"), dict) else {}
    terminal_cfg = config.get("terminal") if isinstance(config.get("terminal"), dict) else {}
    model_cfg = config.get("model") if isinstance(config.get("model"), dict) else {}

    health: dict[str, Any] = {}
    if not args.no_runtime:
        health = _fetch_json(args.health_url, args.timeout)

    cli_version = ""
    if not args.no_cli:
        cli_result = _run([str(cli), "--version"], args.timeout)
        cli_version = (cli_result.get("stdout") or cli_result.get("stderr") or "").strip()
    else:
        cli_result = {"ok": True, "skipped": True}

    launchd: dict[str, Any] = {"path": str(launchd_plist), "exists": launchd_plist.exists()}
    if launchd_plist.exists():
        try:
            with launchd_plist.open("rb") as fh:
                plist = plistlib.load(fh)
            launchd.update(
                {
                    "program_arguments": plist.get("ProgramArguments"),
                    "working_directory": plist.get("WorkingDirectory"),
                    "virtual_env": (plist.get("EnvironmentVariables") or {}).get("VIRTUAL_ENV"),
                }
            )
        except Exception as exc:
            launchd["error"] = str(exc)

    sync_script_text = _read_text(sync_script)
    log_start_offset = _resolve_log_start_offset(
        gateway_error_log,
        args.log_start_offset,
        from_end=args.log_start_at_end,
    )
    high_signal_lines = _high_signal_log_lines(gateway_error_log, args.log_tail_lines, start_offset=log_start_offset)

    vm_doctor: dict[str, Any] = {"skipped": True}
    if args.with_vm:
        doctor = _run([str(args.ssh_mini_agent), "doctor", "--json"], args.vm_timeout)
        vm_doctor = {"ok": False, "raw": doctor}
        raw = (doctor.get("stdout") or "").strip()
        if raw:
            try:
                vm_doctor = json.loads(raw)
            except json.JSONDecodeError:
                vm_doctor = {"ok": False, "stdout": raw, "stderr": doctor.get("stderr", "")}

    return {
        "config": {
            "path": str(config_path),
            "model_default": model_cfg.get("default"),
            "model_provider": model_cfg.get("provider"),
            "context_length": model_cfg.get("context_length"),
            "agent_max_turns": agent_cfg.get("max_turns"),
            "terminal_cwd": terminal_cfg.get("cwd"),
        },
        "env": {
            "path": str(env_path),
            "deprecated_keys_present": sorted(key for key in DEPRECATED_ENV_KEYS if key in env),
            "api_server_enabled": env.get("API_SERVER_ENABLED"),
            "api_server_host": env.get("API_SERVER_HOST"),
            "api_server_port": env.get("API_SERVER_PORT"),
            "feishu_connection_mode": env.get("FEISHU_CONNECTION_MODE"),
            "feishu_home_channel_present": bool(env.get("FEISHU_HOME_CHANNEL")),
        },
        "health": health,
        "cli": {"path": str(cli), "ok": cli_result.get("ok"), "version": cli_version.splitlines()[:5]},
        "launchd": launchd,
        "logs": {
            "gateway_error_log": str(gateway_error_log),
            "log_start_offset": log_start_offset,
            "log_start_at_end": args.log_start_at_end,
            "current_size": gateway_error_log.stat().st_size if gateway_error_log.exists() else 0,
            "high_signal_tail": high_signal_lines,
        },
        "sync_script": {
            "path": str(sync_script),
            "exists": sync_script.exists(),
            "writes_deprecated_messaging_cwd": bool(re.search(r'env_updates\[["\']MESSAGING_CWD["\']\]\s*=', sync_script_text)),
        },
        "vm": vm_doctor,
        "thresholds": {"recommended_turn_budget": args.recommended_turn_budget},
    }


def assert_smoke(result: dict[str, Any], *, require_runtime: bool, require_vm: bool) -> list[str]:
    errors: list[str] = []
    cfg = result.get("config", {})
    env = result.get("env", {})
    health = result.get("health", {})
    launchd = result.get("launchd", {})
    sync_script = result.get("sync_script", {})
    thresholds = result.get("thresholds", {})

    max_turns = cfg.get("agent_max_turns")
    recommended = int(thresholds.get("recommended_turn_budget") or DEFAULT_RECOMMENDED_TURN_BUDGET)
    try:
        if int(max_turns) < recommended:
            errors.append(f"agent.max_turns below recommendation: {max_turns} < {recommended}")
    except (TypeError, ValueError):
        errors.append(f"agent.max_turns is not an integer: {max_turns!r}")

    if env.get("deprecated_keys_present"):
        errors.append(f"deprecated env keys present: {env['deprecated_keys_present']}")
    if not cfg.get("terminal_cwd"):
        errors.append("terminal.cwd missing from config.yaml")
    if sync_script.get("writes_deprecated_messaging_cwd"):
        errors.append("sync script still writes deprecated MESSAGING_CWD")

    if require_runtime:
        if health.get("status") != "ok":
            errors.append(f"health status not ok: {health}")
        if health.get("gateway_state") != "running":
            errors.append(f"gateway_state not running: {health.get('gateway_state')}")
        platforms = health.get("platforms", {}) if isinstance(health.get("platforms"), dict) else {}
        for name in ("feishu", "api_server"):
            if platforms.get(name, {}).get("state") != "connected":
                errors.append(f"{name} not connected: {platforms.get(name)}")

    if result.get("cli", {}).get("ok") is not True:
        errors.append(f"local CLI smoke failed: {result.get('cli')}")

    program_args = launchd.get("program_arguments") or []
    if launchd.get("exists") and program_args:
        joined = " ".join(str(part) for part in program_args)
        if "hermes_cli.main" not in joined or "gateway" not in joined:
            errors.append(f"launchd ProgramArguments do not look like Hermes gateway: {program_args}")
        if launchd.get("virtual_env") and launchd.get("working_directory"):
            if not str(launchd["virtual_env"]).startswith(str(launchd["working_directory"])):
                errors.append("launchd VIRTUAL_ENV is not under WorkingDirectory")

    high_signal = result.get("logs", {}).get("high_signal_tail") or []
    if high_signal:
        errors.append(f"gateway.error.log contains high-signal tail lines: {high_signal[:3]}")

    if require_vm:
        vm = result.get("vm", {})
        if vm.get("ok") is not True:
            errors.append(f"ssh-mini-agent doctor failed: {vm}")
        remote = vm.get("remote", {}) if isinstance(vm.get("remote"), dict) else {}
        if remote and remote.get("remote.default_dir.exists") != "1":
            errors.append(f"VM default dir missing: {remote}")
        if remote and remote.get("remote.default_dir.writable") != "1":
            errors.append(f"VM default dir not writable: {remote}")

    return errors


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Hermes host release smoke harness",
        epilog=(
            "Release gate examples:\n"
            "  Mark current log end and print compact receipt:\n"
            "    python scripts/hermes_release_smoke.py --pretty --log-start-at-end --receipt\n"
            "  Check only log content appended after a recorded byte offset:\n"
            "    python scripts/hermes_release_smoke.py --pretty --log-start-offset 12345 --receipt"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--hermes-home", default=os.environ.get("HERMES_HOME", str(Path.home() / ".hermes")))
    parser.add_argument("--config")
    parser.add_argument("--env")
    parser.add_argument("--cli")
    parser.add_argument("--sync-script")
    parser.add_argument("--launchd-plist")
    parser.add_argument("--gateway-error-log")
    parser.add_argument("--health-url", default="http://127.0.0.1:18789/health/detailed")
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--no-runtime", action="store_true", help="Skip live gateway health probe")
    parser.add_argument("--no-cli", action="store_true", help="Skip local CLI version probe")
    parser.add_argument("--with-vm", action="store_true", help="Run ssh-mini-agent doctor as part of the smoke")
    parser.add_argument("--ssh-mini-agent", default=str(Path.home() / ".local/bin/ssh-mini-agent"))
    parser.add_argument("--vm-timeout", type=float, default=15.0)
    parser.add_argument("--recommended-turn-budget", type=int, default=DEFAULT_RECOMMENDED_TURN_BUDGET)
    parser.add_argument("--log-tail-lines", type=int, default=120)
    parser.add_argument(
        "--log-start-offset",
        type=int,
        default=0,
        help="Only inspect gateway error log content after this byte offset; use for post-restart gates.",
    )
    parser.add_argument(
        "--log-start-at-end",
        action="store_true",
        help="Use the current gateway error log size as a pre-action boundary. For release/restart gates, run this before the action or use a previously recorded --log-start-offset after the action.",
    )
    parser.add_argument("--pretty", action="store_true")
    parser.add_argument(
        "--receipt",
        action="store_true",
        help="Print a compact release receipt instead of the full smoke payload.",
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    result = collect_smoke(args)
    errors = assert_smoke(result, require_runtime=not args.no_runtime, require_vm=args.with_vm)
    result["ok"] = not errors
    result["errors"] = errors
    output = _smoke_receipt_summary(result) if args.receipt else result
    print(json.dumps(output, ensure_ascii=False, indent=2 if args.pretty else None, sort_keys=True))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
