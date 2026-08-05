#!/usr/bin/env python3
"""Meegle auth watchdog for PNC/G1Q3 issue preread.

Periodic, best-effort, never-raise health check for the official Meegle CLI
Device-Code auth.  It never reads Keychain tokens; it only consumes
``meegle auth status --format json`` via gateway.pnc_issue_context and sends a
plain Feishu text alert only after confirmed real auth failure.

2026-06-23 rebaseline: ``expires_in_minutes`` is an auto-rolling access-token
cycle (observed 119→...→0→119).  WARN/CRIT minute thresholds are parsed for
backward compatibility (for example old launchd env=0), but they are deprecated
and no longer drive alerts or device-code assist.
"""
from __future__ import annotations

import argparse
import json
import os
import pwd
import re
import stat
import sys
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from hermes_cli.config import get_hermes_home, reload_env  # noqa: E402
from gateway.feishu_mention import build_at_mention, resolve_display_name  # noqa: E402
from gateway.pnc_issue_context import check_meegle_auth_status, default_meegle_runner  # noqa: E402
from scripts.vm_task_state_bridge import _atomic_write_json  # noqa: E402
from tools.send_message_tool import send_message_tool  # noqa: E402


def _reload_env_for_current_mode() -> None:
    from gateway.record_only.runtime import record_only_enabled

    if not record_only_enabled():
        reload_env()


_reload_env_for_current_mode()

DEFAULT_WARN_MIN = 45  # Deprecated/no-op: kept for env compatibility only.
DEFAULT_CRIT_MIN = 20  # Deprecated/no-op: kept for env compatibility only.
DEFAULT_REALERT_SECONDS = 2 * 60 * 60
DEFAULT_CONFIRM_CHECKS = 2
DEFAULT_PROACTIVE_REINIT_HOURS = 24
DEFAULT_PROACTIVE_AUTO_ROLL_COUNT = 3
DEFAULT_MEEGLE_HOST = "project.feishu.cn"
DEFAULT_OWNER_NAME = "胡子豪"
DEFAULT_OWNER_OPEN_ID = "ou_d1d3cfeba1be0a22faa36aaf4fb3907d"
STATE_FILE_NAME = "meegle_auth_watchdog_state.json"
ALERT_STATES = {"expired", "unknown"}
PROACTIVE_REINIT_AT_KEY = "last_proactive_reinit_at"
LEGACY_DEVICE_CODE_INIT_AT_KEY = "last_device_code_init_at"
SECRET_KEY_RE = re.compile(r"(token|secret|password|authorization|cookie|credential|device_code)", re.I)
SECRET_ASSIGN_RE = re.compile(r"\b(access_token|refresh_token|token|secret|password|authorization|cookie|device_code)\s*[:=]\s*([^\s,;]+)", re.I)
URL_SECRET_RE = re.compile(r"([?&](?:access_token|refresh_token|token|secret|signature|sig)=)([^&#\s]+)", re.I)


@dataclass
class WatchdogDeps:
    status_func: Callable[[], dict[str, Any]] = check_meegle_auth_status
    record_only_status_func: Callable[[], dict[str, Any]] | None = None
    runner: Callable[[list[str]], tuple[int, str, str]] = default_meegle_runner
    send_func: Callable[[dict[str, Any]], str] = send_message_tool
    now_func: Callable[[], float] = time.time
    state_path: Path | None = None


@dataclass
class WatchdogConfig:
    warn_min: int = DEFAULT_WARN_MIN
    crit_min: int = DEFAULT_CRIT_MIN
    re_alert_seconds: int = DEFAULT_REALERT_SECONDS
    expired_confirm_checks: int = DEFAULT_CONFIRM_CHECKS
    unknown_confirm_checks: int = DEFAULT_CONFIRM_CHECKS
    dry_run: bool = False
    send: bool = True
    try_assist: bool = True
    owner_open_id: str = DEFAULT_OWNER_OPEN_ID
    owner_name: str = DEFAULT_OWNER_NAME
    alert_target: str = ""
    state_path: Path | None = None
    host_default: str = DEFAULT_MEEGLE_HOST
    quiet_start: str = "22:00"
    quiet_end: str = "08:00"
    proactive_reinit_hours: int = DEFAULT_PROACTIVE_REINIT_HOURS
    proactive_auto_roll_count: int = DEFAULT_PROACTIVE_AUTO_ROLL_COUNT


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)) or default)
    except (TypeError, ValueError):
        return default


def _default_state_path() -> Path:
    return Path(get_hermes_home()) / "runtime" / STATE_FILE_NAME


def config_from_env(*, dry_run: bool = False, send: bool = True, state_path: Path | None = None, try_assist: bool = True) -> WatchdogConfig:
    owner = os.getenv("PNC_MEEGLE_AUTH_OWNER_OPEN_ID", DEFAULT_OWNER_OPEN_ID).strip() or DEFAULT_OWNER_OPEN_ID
    return WatchdogConfig(
        warn_min=_int_env("PNC_MEEGLE_WARN_MIN", DEFAULT_WARN_MIN),
        crit_min=_int_env("PNC_MEEGLE_CRIT_MIN", DEFAULT_CRIT_MIN),
        re_alert_seconds=_int_env("PNC_MEEGLE_REALERT_SECONDS", DEFAULT_REALERT_SECONDS),
        expired_confirm_checks=max(1, _int_env("PNC_MEEGLE_EXPIRED_CONFIRM_CHECKS", DEFAULT_CONFIRM_CHECKS)),
        unknown_confirm_checks=max(1, _int_env("PNC_MEEGLE_UNKNOWN_CONFIRM_CHECKS", DEFAULT_CONFIRM_CHECKS)),
        dry_run=dry_run,
        send=send,
        try_assist=try_assist,
        owner_open_id=owner,
        owner_name=os.getenv("PNC_MEEGLE_AUTH_OWNER_NAME", DEFAULT_OWNER_NAME).strip() or DEFAULT_OWNER_NAME,
        alert_target=os.getenv("PNC_MEEGLE_AUTH_ALERT_TARGET", "").strip(),
        state_path=state_path,
        host_default=os.getenv("MEEGLE_HOST", "project.feishu.cn").strip() or "project.feishu.cn",
        quiet_start=os.getenv("PNC_MEEGLE_QUIET_START", "22:00").strip() or "22:00",
        quiet_end=os.getenv("PNC_MEEGLE_QUIET_END", "08:00").strip() or "08:00",
        proactive_reinit_hours=max(0, _int_env("PNC_MEEGLE_PROACTIVE_REINIT_HOURS", DEFAULT_PROACTIVE_REINIT_HOURS)),
        proactive_auto_roll_count=max(1, _int_env("PNC_MEEGLE_PROACTIVE_AUTO_ROLL_COUNT", DEFAULT_PROACTIVE_AUTO_ROLL_COUNT)),
    )


def redact(value: Any) -> Any:
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for k, v in value.items():
            if SECRET_KEY_RE.search(str(k)):
                out[str(k)] = "[REDACTED]"
            else:
                out[str(k)] = redact(v)
        return out
    if isinstance(value, list):
        return [redact(v) for v in value]
    if isinstance(value, str):
        text = SECRET_ASSIGN_RE.sub(lambda m: f"{m.group(1)}=[REDACTED]", value)
        return URL_SECRET_RE.sub(lambda m: f"{m.group(1)}[REDACTED]", text)
    return value


def classify_status(status: dict[str, Any], *, warn_min: int, crit_min: int) -> str:
    """Classify auth status.

    ``warn_min``/``crit_min`` are intentionally ignored after the 2026-06-23
    rebaseline: authenticated=True is healthy even at expires=0 because live
    evidence showed the access token auto-rolls 0→119 without human action.
    """
    authenticated = status.get("authenticated")
    if authenticated is None:
        return "unknown"
    if authenticated is False:
        return "expired"
    if authenticated is True:
        return "healthy"
    return "unknown"


def load_state(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception:
        return {}


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write_json(path, redact(state))


def _owner_from_roles(name: str) -> str:
    try:
        roles = Path(get_hermes_home()) / "config" / "user-roles.json"
        data = json.loads(roles.read_text(encoding="utf-8"))
        mapping = data.get("user_id_mapping") if isinstance(data, dict) else None
        if isinstance(mapping, dict):
            for open_id, display in mapping.items():
                if str(display).strip() == name:
                    return str(open_id).strip()
    except Exception:
        pass
    return ""


def resolve_owner_open_id(config: WatchdogConfig) -> str:
    return config.owner_open_id or _owner_from_roles(config.owner_name) or DEFAULT_OWNER_OPEN_ID


def _parse_hhmm(value: str) -> tuple[int, int]:
    hh, mm = str(value or "").split(":", 1)
    return int(hh), int(mm)


def in_quiet_hours(now: float, *, quiet_start: str, quiet_end: str) -> bool:
    if str(os.getenv("PNC_MEEGLE_QUIET_HOURS", "1")).lower() in {"0", "false", "off", "no"}:
        return False
    try:
        sh, sm = _parse_hhmm(quiet_start)
        eh, em = _parse_hhmm(quiet_end)
    except Exception:
        return False
    tm = time.localtime(now)
    cur = tm.tm_hour * 60 + tm.tm_min
    start = sh * 60 + sm
    end = eh * 60 + em
    if start == end:
        return False
    if start < end:
        return start <= cur < end
    return cur >= start or cur < end


def should_alert(state: str, previous: dict[str, Any], now: float, *, re_alert_seconds: int, confirmed: bool = True) -> bool:
    if state not in ALERT_STATES or not confirmed:
        return False
    last_alert_state = str(previous.get("last_alert_state") or "")
    try:
        last_alert_at = float(previous.get("last_alert_at") or 0)
    except (TypeError, ValueError):
        last_alert_at = 0
    if state != last_alert_state:
        return True
    if now - last_alert_at >= re_alert_seconds:
        return True
    return False


def should_recovery_alert(state: str, previous: dict[str, Any]) -> bool:
    if state != "healthy":
        return False
    last_alert_state = str(previous.get("last_alert_state") or "")
    return last_alert_state in ALERT_STATES


def _parse_device_code_payload(out: str, err: str) -> dict[str, str]:
    text = "\n".join([str(out or ""), str(err or "")]).strip()
    payload: Any = None
    if text:
        try:
            payload = json.loads(text)
        except Exception:
            try:
                payload, _idx = json.JSONDecoder().raw_decode(text)
            except Exception:
                payload = None
    result: dict[str, str] = {}
    if isinstance(payload, dict):
        for key in ("verification_url", "verification_uri", "verification_uri_complete", "user_code"):
            val = payload.get(key)
            if val:
                out_key = "verification_url" if key in {"verification_url", "verification_uri", "verification_uri_complete"} else key
                result[out_key] = str(val)
    if not result and text:
        url_m = re.search(r"https?://[^\s]+", text)
        code_m = re.search(r"(?:user[_ -]?code|code)\s*[:：]\s*([A-Z0-9-]{4,})", text, re.I)
        if url_m:
            result["verification_url"] = url_m.group(0)
        if code_m:
            result["user_code"] = code_m.group(1)
    return redact(result)


def try_device_code_init(
    runner: Callable[[list[str]], tuple[int, str, str]],
    *,
    host: str = DEFAULT_MEEGLE_HOST,
) -> dict[str, Any]:
    try:
        rc, out, err = runner(
            [
                "auth",
                "login",
                "--device-code",
                "--host",
                host,
                "--phase",
                "init",
                "--once",
            ]
        )
        payload = _parse_device_code_payload(out, err)
        payload["ok"] = bool(rc == 0 and (payload.get("verification_url") or payload.get("user_code")))
        if rc != 0:
            payload["error"] = redact(str(err or out or "device code init failed")[:300])
        return payload
    except Exception as exc:
        return {"ok": False, "error": redact(f"{type(exc).__name__}: {exc}"[:300])}


def build_alert_message(*, state: str, status: dict[str, Any], config: WatchdogConfig, assist: dict[str, Any] | None = None, recovery: bool = False, proactive: bool = False) -> str:
    owner_open_id = resolve_owner_open_id(config)
    name = resolve_display_name(owner_open_id) or config.owner_name
    mention = build_at_mention(owner_open_id, name)
    expires = status.get("expires_in_minutes")
    host = status.get("host") or config.host_default or "project.feishu.cn"
    if recovery:
        return f"✅ Meegle auth 已恢复，剩余 {expires} 分钟，host={host}。"
    if proactive:
        lines = [
            f"{mention} ⚠️ Meegle 底层 device-code 凭证需扫码续期",
            f"state=proactive_reinit；expires_in_minutes={expires}；host={host}",
            "access token 仍处于 authenticated=True；本提醒是主动预续，避免底层 device-code 凭证真到期后全量爬取失败。",
        ]
        if assist:
            url = assist.get("verification_url")
            code = assist.get("user_code")
            if url or code:
                lines.append("需扫码续期：请打开验证链接并输入 user_code 批准。")
                if url:
                    lines.append(f"verification_url={url}")
                if code:
                    lines.append(f"user_code={code}")
            elif assist.get("error"):
                lines.extend(
                    [
                        "Device Code 初始化失败，请手动执行：",
                        "meegle auth login --device-code --host project.feishu.cn",
                        f"error={redact(assist.get('error'))}",
                    ]
                )
        return "\n".join(str(redact(line)) for line in lines if line)

    title = {
        "expired": "🚨 Meegle auth 已过期/未授权（连续确认）",
        "unknown": "⚠️ Meegle auth 巡检连续失败（unknown，不等同过期）",
    }.get(state, "⚠️ Meegle auth 巡检告警")
    lines = [
        f"{mention} {title}",
        f"state={state}；expires_in_minutes={expires}；host={host}",
        "续期命令：meegle auth login --device-code --host project.feishu.cn",
    ]
    error = str(status.get("error") or "").strip()
    if error and state == "unknown":
        lines.append(f"探测错误：{redact(error)}")
    if assist:
        url = assist.get("verification_url")
        code = assist.get("user_code")
        if url or code:
            lines.append("人工助攻：已预取 Device Code，请打开验证链接并输入 user_code 批准。")
            if url:
                lines.append(f"verification_url={url}")
            if code:
                lines.append(f"user_code={code}")
        elif assist.get("error"):
            lines.append(f"Device Code 初始化失败，仍可手动执行续期命令。error={redact(assist.get('error'))}")
    lines.append("说明：access token 的 expires_in_minutes 会自动滚动；仅连续真失效才打扰人工。")
    return "\n".join(str(redact(line)) for line in lines if line)


def send_alert(message: str, config: WatchdogConfig, deps: WatchdogDeps) -> dict[str, Any]:
    target = config.alert_target or f"feishu:{resolve_owner_open_id(config)}"
    if config.dry_run or not config.send:
        return {"dry_run": True, "target": target, "preview": message[:500]}
    try:
        raw = deps.send_func({"action": "send", "target": target, "message": message})
        try:
            parsed = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            parsed = {"raw": raw}
        return parsed if isinstance(parsed, dict) else {"result": parsed}
    except Exception as exc:
        return {"error": redact(f"{type(exc).__name__}: {exc}")}


def _compute_consecutive_counts(state: str, previous: dict[str, Any]) -> tuple[int, int]:
    prev_expired = int(previous.get("consecutive_expired") or 0)
    prev_unknown = int(previous.get("consecutive_unknown") or 0)
    if state == "expired":
        return prev_expired + 1, 0
    if state == "unknown":
        return 0, prev_unknown + 1
    return 0, 0


def _is_confirmed_failure(state: str, *, consecutive_expired: int, consecutive_unknown: int, config: WatchdogConfig) -> bool:
    if state == "expired":
        return consecutive_expired >= config.expired_confirm_checks
    if state == "unknown":
        return consecutive_unknown >= config.unknown_confirm_checks
    return False


def _auto_roll_count(previous: dict[str, Any], status: dict[str, Any], state: str) -> int:
    if state != "healthy":
        return 0
    try:
        prev_expires = int(previous.get("last_expires"))
        cur_expires = int(status.get("expires_in_minutes"))
    except (TypeError, ValueError):
        return int(previous.get("consecutive_auto_rolls") or 0)
    count = int(previous.get("consecutive_auto_rolls") or 0)
    return count + 1 if cur_expires > prev_expires else count


def _float_state_value(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _last_proactive_reinit_at(previous: dict[str, Any]) -> float:
    current = _float_state_value(previous.get(PROACTIVE_REINIT_AT_KEY))
    if current > 0:
        return current
    # Backward compatibility for states written before 2026-06-24.  A live
    # regression showed the legacy key name matched SECRET_KEY_RE
    # ("device_code") and was persisted as "[REDACTED]"; treat that dirty
    # value as absent, but never write the legacy key again.
    legacy = _float_state_value(previous.get(LEGACY_DEVICE_CODE_INIT_AT_KEY))
    return legacy if legacy > 0 else 0.0


def _migrate_proactive_reinit_state(state: dict[str, Any]) -> None:
    if PROACTIVE_REINIT_AT_KEY not in state:
        legacy = _float_state_value(state.get(LEGACY_DEVICE_CODE_INIT_AT_KEY))
        if legacy > 0:
            state[PROACTIVE_REINIT_AT_KEY] = legacy
    state.pop(LEGACY_DEVICE_CODE_INIT_AT_KEY, None)


def _should_proactive_reinit(previous: dict[str, Any], now: float, *, auto_rolls: int, config: WatchdogConfig) -> bool:
    if not config.try_assist or config.proactive_reinit_hours <= 0:
        return False
    if auto_rolls < config.proactive_auto_roll_count:
        return False
    last_init = _last_proactive_reinit_at(previous)
    return last_init <= 0 or now - last_init >= config.proactive_reinit_hours * 3600


def _record_only_dependencies(config: WatchdogConfig, deps: WatchdogDeps) -> tuple[WatchdogDeps, bool]:
    from gateway.record_only.runtime import get_record_only_transport

    recorder = get_record_only_transport("scripts.pnc_meegle_auth_watchdog")
    if recorder is None:
        return deps, False

    status_fixture = deps.record_only_status_func
    host = str(config.host_default or "project.feishu.cn")

    def record_status() -> dict[str, Any]:
        recorder.record(
            operation="auth_status_check",
            platform="meegle",
            destination_kind="auth_host",
            destination_id=host,
            payload_type="query",
            payload={"command": "auth status", "fixture": status_fixture is not None},
            caller_dedupe_key=f"meegle-auth-status:{host}",
        )
        if status_fixture is not None:
            return status_fixture()
        return {
            "ok": False,
            "authenticated": None,
            "expires_in_minutes": None,
            "host": host,
            "error": "record-only: real Meegle auth status suppressed",
        }

    def record_device_init(args: list[str]) -> tuple[int, str, str]:
        recorder.record(
            operation="auth_device_init",
            platform="meegle",
            destination_kind="auth_host",
            destination_id=host,
            payload_type="auth_request",
            payload={"command": list(args)},
            caller_dedupe_key=f"meegle-auth-device-init:{host}",
        )
        return 1, "", "record-only: real Meegle device-code init suppressed"

    from gateway.record_only.transport import RecordOnlyRelaySender

    return (
        replace(
            deps,
            status_func=record_status,
            runner=record_device_init,
            send_func=RecordOnlyRelaySender(recorder).send,
        ),
        True,
    )


def _validate_record_only_state_path(path: Path) -> tuple[Path, tuple[int, int]]:
    home_raw = Path(get_hermes_home()).expanduser()
    if not home_raw.is_absolute():
        raise ValueError("record-only watchdog requires absolute HERMES_HOME")
    home = home_raw.resolve(strict=True)
    if home_raw.absolute() != home:
        raise ValueError("record-only watchdog HERMES_HOME must not contain symlinks or aliases")
    try:
        account_home = Path(pwd.getpwuid(os.getuid()).pw_dir).resolve()
    except (KeyError, OSError):
        account_home = Path.home().resolve()
    for blocked in ((account_home / ".hermes").resolve(), (account_home / ".openclaw").resolve()):
        try:
            home.relative_to(blocked)
        except ValueError:
            continue
        raise ValueError("record-only watchdog refuses canonical Hermes/OpenClaw home")

    raw = Path(path).expanduser()
    if not raw.is_absolute():
        raise ValueError("record-only watchdog state path must be absolute")
    parent = raw.parent.resolve(strict=True)
    if raw.parent.absolute() != parent:
        raise ValueError("record-only watchdog state parent must not contain symlinks or aliases")
    try:
        parent.relative_to(home)
    except ValueError as exc:
        raise ValueError("record-only watchdog state path must stay within HERMES_HOME") from exc
    parent_info = parent.stat()
    if not stat.S_ISDIR(parent_info.st_mode) or parent_info.st_uid != os.getuid() or parent_info.st_mode & 0o022:
        raise ValueError("record-only watchdog state parent has unsafe owner/mode")
    try:
        info = raw.lstat()
    except FileNotFoundError:
        pass
    else:
        if (
            stat.S_ISLNK(info.st_mode)
            or not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or info.st_mode & 0o022
            or info.st_nlink != 1
        ):
            raise ValueError("record-only watchdog state file has unsafe type/owner/mode/link count")
    return raw, (parent_info.st_dev, parent_info.st_ino)


def _verify_record_only_state_path(path: Path, parent_identity: tuple[int, int]) -> None:
    verified, current_parent_identity = _validate_record_only_state_path(path)
    if verified != path:
        raise ValueError("record-only watchdog state path changed")
    if current_parent_identity != parent_identity:
        raise ValueError("record-only watchdog state parent identity changed during run")


def run_once(config: WatchdogConfig, deps: WatchdogDeps | None = None) -> dict[str, Any]:
    deps, record_only = _record_only_dependencies(config, deps or WatchdogDeps())
    state_path = config.state_path or deps.state_path or _default_state_path()
    state_identity = None
    if record_only:
        state_path, state_identity = _validate_record_only_state_path(Path(state_path))
    previous = load_state(state_path)
    now = deps.now_func()
    result: dict[str, Any] = {"ok": True, "state_file": str(state_path), "dry_run": config.dry_run}
    try:
        status = deps.status_func()
    except Exception as exc:
        status = {"ok": False, "authenticated": None, "expires_in_minutes": None, "host": "", "error": f"{type(exc).__name__}: {exc}"[:200]}
    status = redact(status)
    state = classify_status(status, warn_min=config.warn_min, crit_min=config.crit_min)
    consecutive_expired, consecutive_unknown = _compute_consecutive_counts(state, previous)
    confirmed_failure = _is_confirmed_failure(
        state,
        consecutive_expired=consecutive_expired,
        consecutive_unknown=consecutive_unknown,
        config=config,
    )
    auto_rolls = _auto_roll_count(previous, status, state)
    proactive_reinit = _should_proactive_reinit(previous, now, auto_rolls=auto_rolls, config=config) if state == "healthy" else False
    assist: dict[str, Any] | None = None

    alert_result: dict[str, Any] | None = None
    alert_sent = False
    quiet_suppressed = False
    recovery = should_recovery_alert(state, previous)
    if proactive_reinit:
        assist = try_device_code_init(deps.runner, host=config.host_default)
        if assist.get("ok"):
            previous[PROACTIVE_REINIT_AT_KEY] = now
            previous.pop(LEGACY_DEVICE_CODE_INIT_AT_KEY, None)
            previous["auth_init_success_count"] = int(previous.get("auth_init_success_count") or 0) + 1
        if in_quiet_hours(now, quiet_start=config.quiet_start, quiet_end=config.quiet_end):
            quiet_suppressed = True
        else:
            message = build_alert_message(state=state, status=status, config=config, assist=assist, proactive=True)
            alert_result = send_alert(message, config, deps)
            alert_sent = bool(alert_result.get("success") or alert_result.get("dry_run"))
    elif recovery:
        if in_quiet_hours(now, quiet_start=config.quiet_start, quiet_end=config.quiet_end):
            quiet_suppressed = True
        else:
            message = build_alert_message(state=state, status=status, config=config, recovery=True)
            alert_result = send_alert(message, config, deps)
            alert_sent = bool(alert_result.get("success") or alert_result.get("dry_run"))
            previous["last_alert_state"] = ""
            previous["last_alert_at"] = 0
    elif should_alert(state, previous, now, re_alert_seconds=config.re_alert_seconds, confirmed=confirmed_failure):
        if in_quiet_hours(now, quiet_start=config.quiet_start, quiet_end=config.quiet_end):
            quiet_suppressed = True
        else:
            if config.try_assist and state == "expired":
                # Only true confirmed expiry alerts get zero-friction Device Code assist;
                # rate-limited/quiet-suppressed checks must not mint extra codes.
                assist = try_device_code_init(deps.runner, host=config.host_default)
            message = build_alert_message(state=state, status=status, config=config, assist=assist)
            alert_result = send_alert(message, config, deps)
            alert_sent = bool(alert_result.get("success") or alert_result.get("dry_run"))
            previous["last_alert_state"] = state
            previous["last_alert_at"] = now

    current = {
        **previous,
        "last_checked_at": now,
        "last_state": state,
        "last_expires": status.get("expires_in_minutes"),
        "last_host": status.get("host") or config.host_default,
        "consecutive_expired": consecutive_expired,
        "consecutive_unknown": consecutive_unknown,
    }
    if state == "healthy":
        current["last_silent_refresh_ok"] = False
        current.pop("last_silent_refresh_probe", None)
    current["consecutive_auto_rolls"] = auto_rolls if state == "healthy" else 0
    _migrate_proactive_reinit_state(current)
    if assist:
        current["last_auth_init_ok"] = bool(assist.get("ok"))
        if assist.get("ok") and PROACTIVE_REINIT_AT_KEY not in current:
            current[PROACTIVE_REINIT_AT_KEY] = now
            current["auth_init_success_count"] = int(current.get("auth_init_success_count") or 0) + 1
    if alert_result is not None:
        current["last_alert_result"] = redact(alert_result)
    save_state(state_path, current)
    if record_only:
        _verify_record_only_state_path(state_path, state_identity)

    result.update({
        "state": state,
        "status": status,
        "warn_min": config.warn_min,
        "crit_min": config.crit_min,
        "expired_confirm_checks": config.expired_confirm_checks,
        "unknown_confirm_checks": config.unknown_confirm_checks,
        "consecutive_expired": consecutive_expired,
        "consecutive_unknown": consecutive_unknown,
        "confirmed_failure": confirmed_failure,
        "proactive_reinit": proactive_reinit,
        "consecutive_auto_rolls": auto_rolls,
        "alert_sent": alert_sent,
        "alert_result": redact(alert_result) if alert_result is not None else None,
        "silent_refresh_ok": False,
        "silent_refresh_probe": None,
        "quiet_hours_suppressed": quiet_suppressed,
        "assist": redact(assist) if assist else None,
    })
    return redact(result)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="PNC Meegle auth watchdog")
    parser.add_argument("--once", action="store_true", help="Run one check and exit (default behaviour).")
    parser.add_argument("--json", action="store_true", help="Print JSON result.")
    parser.add_argument("--dry-run", action="store_true", help="Do not send Feishu alerts; print/store decision only.")
    parser.add_argument("--no-assist", action="store_true", help="Disable best-effort device-code assist for confirmed expiry.")
    parser.add_argument("--state-file", type=Path, default=None, help="Override watchdog state file path.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    config = config_from_env(dry_run=args.dry_run, send=not args.dry_run, state_path=args.state_file, try_assist=not args.no_assist)
    try:
        result = run_once(config)
    except Exception as exc:  # final belt-and-suspenders: watchdog must not raise
        result = {"ok": False, "state": "unknown", "error": redact(f"{type(exc).__name__}: {exc}")}
    print(json.dumps(redact(result), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
