#!/usr/bin/env python3
"""Collect VM terminal truth into durable, send-free RCA delivery records."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path, PurePosixPath
import signal
import socket
import sqlite3
import subprocess
import sys
import textwrap
import threading
import time
from typing import Any, Callable, Mapping

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv

from gateway.pnc_rca_admission import (
    RCA_KAFKA_TRIGGER_KINDS,
    RCA_MANUAL_TRIGGER_KINDS,
    build_rca_admission,
    validate_rca_admission,
    validate_rca_trigger_context,
)
from gateway.pnc_rca_delivery_contract import (
    TERMINAL_DELIVERY_OUTCOMES,
    DeliveryContractError,
    VerifiedDelivery,
    canonical_artifact_root,
    verify_delivery_bundle,
)
from gateway.pnc_rca_delivery_quarantine_baseline import (
    disabled_quarantine_baseline_status,
    quarantine_baseline_settings,
    read_quarantine_baseline_status,
)
from gateway.pnc_rca_delivery_store import (
    DeliveryRecordConflictError,
    ExecutionWatchClaim,
    RcaDeliveryStore,
    StaleDeliveryWatchLeaseError,
)
from gateway.pnc_rca_kafka_contract import NORMALIZED_EVENT_SCHEMA_VERSION
from gateway.pnc_rca_runtime_identity import (
    MAX_HEALTH_FUTURE_SKEW_SECONDS,
    RCA_DELIVERY_COLLECTOR_LOADED_DEPENDENCIES,
    build_runtime_identity,
    runtime_identity_is_valid,
)
from hermes_constants import get_hermes_home


ENV_PREFIX = "HERMES_RCA_DELIVERY_COLLECTOR_"
HEALTH_SCHEMA_VERSION = "pnc_rca_delivery_collector_health_v2"
SERVICE_LABEL = "local.pnc.rca-delivery-collector"
DEPENDENCY_PROBE_REFRESH_SECONDS = 30
SUBMISSION_OUTBOX_SCHEMA_VERSION = "pnc_rca_submission_outbox_v2"
REMOTE_CSS_PARSER_DISTRIBUTION = "tinycss2"
REMOTE_CSS_PARSER_VERSION = "1.2.1"
REMOTE_CSS_WEBENCODINGS_VERSION = "0.5.1"
REMOTE_CSS_RUNTIME_CHECK_SCHEMA = "rca_delivery_runtime_check_v1"
REMOTE_CSS_RUNTIME_CHECKER_PATH = (
    "/home/mini/.hermes/worker-state/check_rca_delivery_runtime.py"
)
REMOTE_CSS_RUNTIME_CHECKER_SHA256 = (
    "8997fa0740f1397e9187124249f18cd38ed93d5bf0f2bce51a59a76583eba0c5"
)
REMOTE_CSS_RUNTIME_REQUIREMENTS_PATH = (
    "/home/mini/.hermes/worker-state/requirements-rca-delivery.txt"
)
REMOTE_CSS_RUNTIME_REQUIREMENTS_SHA256 = (
    "5c38f8fa928701507b5b38e5ed15495d1529c77ef8a8ad6d3de38145f0dc213e"
)
REMOTE_CSS_RUNTIME_PYTHON = "/usr/bin/python3"
DEFAULT_SSH_MINI_AGENT = str(Path.home() / ".local" / "bin" / "ssh-mini-agent")
MAX_ARTIFACT_READ_TIMEOUT_SECONDS = 110
ARTIFACT_READ_LEASE_MARGIN_SECONDS = 15
MAX_HEALTH_HEARTBEAT_INTERVAL_SECONDS = 15.0
_EVENTUAL_ARTIFACT_CODES = frozenset({
    "delivery_contract_missing",
    "delivery_manifest_missing",
    "artifact_missing",
    "html_dependency_missing",
    "html_dependency_changed_during_read",
    "required_html_artifact_missing",
})
_RETRYABLE_INFRASTRUCTURE_ARTIFACT_CODES = frozenset({
    "html_css_parser_dependency_missing",
    "html_css_parser_version_mismatch",
})
_RUNNING_STATES = frozenset({
    "pending",
    "submitted",
    "queued",
    "claimed",
    "running",
    "in_progress",
})
_COMPLETED_STATES = frozenset({"completed", "done"})
_FAILED_TERMINAL_STATES = frozenset({
    "failed",
    "blocked",
    "abandoned",
    "cancelled",
    "canceled",
})
_PUBLIC_TERMINAL_BLOCKER_CODES = {
    "need_keyframe": "need_keyframe",
    "need_key_frame": "need_keyframe",
    "required_input": "required_input",
    "missing_required_input": "required_input",
}
_PUBLIC_TERMINAL_ERROR_CODES = frozenset({
    "artifact_hash_mismatch",
    "artifact_reader_response_invalid",
    "delivery_record_conflict",
    "submission_admission_invalid",
    "submission_outbox_contract_invalid",
    "submission_receipt_identity_mismatch",
    "submission_watch_identity_mismatch",
    "terminal_artifact_grace_exceeded",
})
_PUBLIC_TERMINAL_FALLBACK_CODE = "terminal_failure_unclassified"


StatusReader = Callable[[str], Mapping[str, Any]]
ArtifactBundleReader = Callable[[ExecutionWatchClaim], Mapping[str, Any]]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_iso(value: datetime | None = None) -> str:
    current = value or _utc_now()
    if current.tzinfo is None:
        raise ValueError("datetime values must be timezone-aware")
    return current.astimezone(timezone.utc).isoformat()


def _heartbeat_interval_seconds(max_age_seconds: int) -> float:
    return max(
        1.0,
        min(MAX_HEALTH_HEARTBEAT_INTERVAL_SECONDS, max_age_seconds / 3),
    )


class _PeriodicHeartbeat:
    def __init__(self, callback: Callable[[], None], *, interval_seconds: float):
        self._callback = callback
        self._interval_seconds = float(interval_seconds)
        self._stop = threading.Event()
        self._error: BaseException | None = None
        self._thread = threading.Thread(
            target=self._run,
            name=f"{SERVICE_LABEL}-heartbeat",
            daemon=True,
        )

    def _run(self) -> None:
        while not self._stop.wait(self._interval_seconds):
            try:
                self._callback()
            except BaseException as exc:  # pragma: no cover - surfaced on join
                self._error = exc
                self._stop.set()

    def __enter__(self) -> "_PeriodicHeartbeat":
        self._thread.start()
        return self

    def __exit__(self, exc_type, _exc, _traceback) -> None:
        self._stop.set()
        self._thread.join(timeout=max(1.0, self._interval_seconds + 1.0))
        if exc_type is None and self._thread.is_alive():
            raise RuntimeError("delivery_collector_heartbeat_stop_timeout")
        if exc_type is None and self._error is not None:
            raise RuntimeError("delivery_collector_heartbeat_failed") from self._error


def _eventual_artifact_error(code: str) -> bool:
    value = str(code or "")
    return value in _EVENTUAL_ARTIFACT_CODES or value.startswith((
        "artifact_missing_",
        "html_dependency_missing_",
    ))


def _boolean(env: Mapping[str, str], name: str, default: bool = False) -> bool:
    value = str(env.get(name, "true" if default else "false")).strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be true or false")


def _strict_boolean(
    env: Mapping[str, str], name: str, default: bool = False
) -> bool:
    value = str(env.get(name, "true" if default else "false")).strip().lower()
    if value == "true":
        return True
    if value == "false":
        return False
    raise ValueError(f"{name} must be exactly true or false")


def _integer(
    env: Mapping[str, str],
    name: str,
    default: int,
    *,
    minimum: int = 1,
) -> int:
    try:
        value = int(str(env.get(name, default)).strip())
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    return value


def expected_remote_css_runtime_dependency() -> dict[str, str]:
    return {
        "schema_version": REMOTE_CSS_RUNTIME_CHECK_SCHEMA,
        "distribution": REMOTE_CSS_PARSER_DISTRIBUTION,
        "version": REMOTE_CSS_PARSER_VERSION,
        "webencodings_version": REMOTE_CSS_WEBENCODINGS_VERSION,
        "python_executable": REMOTE_CSS_RUNTIME_PYTHON,
        "checker_path": REMOTE_CSS_RUNTIME_CHECKER_PATH,
        "checker_sha256": REMOTE_CSS_RUNTIME_CHECKER_SHA256,
        "requirements_path": REMOTE_CSS_RUNTIME_REQUIREMENTS_PATH,
        "requirements_sha256": REMOTE_CSS_RUNTIME_REQUIREMENTS_SHA256,
    }


@dataclass(frozen=True)
class CollectorConfig:
    enabled: bool
    control_db_path: Path
    health_path: Path
    poll_interval_seconds: int
    running_poll_seconds: int
    max_poll_seconds: int
    lease_seconds: int
    batch_size: int
    backfill_batch_size: int
    health_max_age_seconds: int
    ssh_mini_agent: str
    artifact_read_timeout_seconds: int
    terminal_artifact_grace_seconds: int
    quarantine_baseline_path: Path
    quarantine_baseline_sha256: str
    quarantine_release_id: str
    quarantine_bootstrap_epoch_id: str
    quarantine_active_release_binding_path: Path
    quarantine_live_env_path: Path

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
        *,
        hermes_home: str | Path | None = None,
    ) -> "CollectorConfig":
        source = os.environ if env is None else env
        home = Path(hermes_home or get_hermes_home()).expanduser()
        enabled = _boolean(source, f"{ENV_PREFIX}ENABLED", False)
        poll = _integer(source, f"{ENV_PREFIX}POLL_INTERVAL_SECONDS", 5)
        running_poll = _integer(source, f"{ENV_PREFIX}RUNNING_POLL_SECONDS", 20)
        max_poll = _integer(source, f"{ENV_PREFIX}MAX_POLL_SECONDS", 300)
        if max_poll < running_poll:
            raise ValueError(
                f"{ENV_PREFIX}MAX_POLL_SECONDS must be >= RUNNING_POLL_SECONDS"
            )
        agent = str(
            source.get(f"{ENV_PREFIX}SSH_MINI_AGENT", DEFAULT_SSH_MINI_AGENT)
        ).strip()
        if not agent:
            raise ValueError(f"{ENV_PREFIX}SSH_MINI_AGENT is required")
        artifact_timeout = _integer(
            source,
            f"{ENV_PREFIX}ARTIFACT_READ_TIMEOUT_SECONDS",
            MAX_ARTIFACT_READ_TIMEOUT_SECONDS,
        )
        if artifact_timeout > MAX_ARTIFACT_READ_TIMEOUT_SECONDS:
            raise ValueError(
                f"{ENV_PREFIX}ARTIFACT_READ_TIMEOUT_SECONDS must be at most "
                f"{MAX_ARTIFACT_READ_TIMEOUT_SECONDS}"
            )
        lease_seconds = _integer(source, f"{ENV_PREFIX}LEASE_SECONDS", 180, minimum=30)
        if lease_seconds <= artifact_timeout + ARTIFACT_READ_LEASE_MARGIN_SECONDS:
            raise ValueError(
                f"{ENV_PREFIX}LEASE_SECONDS must exceed "
                "ARTIFACT_READ_TIMEOUT_SECONDS plus the lease margin"
            )
        control_db_path = Path(
            source.get(
                f"{ENV_PREFIX}CONTROL_DB_PATH",
                home
                / "runtime"
                / "pnc_agent"
                / "feishu_issue_kafka_rca"
                / "control.sqlite3",
            )
        ).expanduser()
        quarantine = quarantine_baseline_settings(
            source,
            hermes_home=home,
            control_db_path=control_db_path,
        )
        return cls(
            enabled=enabled,
            control_db_path=control_db_path,
            health_path=Path(
                source.get(
                    f"{ENV_PREFIX}HEALTH_PATH",
                    home
                    / "runtime"
                    / "pnc_agent"
                    / "feishu_issue_kafka_rca"
                    / "delivery_collector_health.json",
                )
            ).expanduser(),
            poll_interval_seconds=poll,
            running_poll_seconds=running_poll,
            max_poll_seconds=max_poll,
            lease_seconds=lease_seconds,
            batch_size=_integer(source, f"{ENV_PREFIX}BATCH_SIZE", 20),
            backfill_batch_size=_integer(
                source, f"{ENV_PREFIX}BACKFILL_BATCH_SIZE", 1000
            ),
            health_max_age_seconds=_integer(
                source, f"{ENV_PREFIX}HEALTH_MAX_AGE_SECONDS", 60
            ),
            ssh_mini_agent=agent,
            artifact_read_timeout_seconds=artifact_timeout,
            terminal_artifact_grace_seconds=_integer(
                source, f"{ENV_PREFIX}TERMINAL_ARTIFACT_GRACE_SECONDS", 900
            ),
            quarantine_baseline_path=quarantine.baseline_path,
            quarantine_baseline_sha256=quarantine.baseline_sha256,
            quarantine_release_id=quarantine.release_id,
            quarantine_bootstrap_epoch_id=quarantine.bootstrap_epoch_id,
            quarantine_active_release_binding_path=(
                quarantine.active_release_binding_path
            ),
            quarantine_live_env_path=quarantine.live_env_path,
        )

    def public_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "control_db_path": str(self.control_db_path),
            "health_path": str(self.health_path),
            "poll_interval_seconds": self.poll_interval_seconds,
            "running_poll_seconds": self.running_poll_seconds,
            "max_poll_seconds": self.max_poll_seconds,
            "lease_seconds": self.lease_seconds,
            "batch_size": self.batch_size,
            "backfill_batch_size": self.backfill_batch_size,
            "health_max_age_seconds": self.health_max_age_seconds,
            "ssh_mini_agent": self.ssh_mini_agent,
            "artifact_read_timeout_seconds": self.artifact_read_timeout_seconds,
            "terminal_artifact_grace_seconds": self.terminal_artifact_grace_seconds,
            "quarantine_baseline_path": str(self.quarantine_baseline_path),
            "quarantine_baseline_sha256": self.quarantine_baseline_sha256,
            "quarantine_release_id": self.quarantine_release_id,
            "quarantine_bootstrap_epoch_id": self.quarantine_bootstrap_epoch_id,
            "quarantine_active_release_binding_path": str(
                self.quarantine_active_release_binding_path
            ),
            "quarantine_live_env_path": str(self.quarantine_live_env_path),
            "remote_css_parser": {
                **expected_remote_css_runtime_dependency(),
            },
            "external_writes": False,
        }


class ArtifactBundleReadError(RuntimeError):
    def __init__(self, code: str, detail: str = "", *, permanent: bool = False):
        self.code = str(code or "artifact_bundle_unavailable")[:120]
        self.detail = str(detail or self.code)[:1000]
        self.permanent = bool(permanent)
        super().__init__(self.detail)


@dataclass
class CollectorStats:
    loops: int = 0
    watches_created: int = 0
    claimed: int = 0
    running: int = 0
    delivery_created: int = 0
    delivery_deduped: int = 0
    terminal_failed: int = 0
    quarantined: int = 0
    retried: int = 0
    idle: int = 0
    stale_lease: int = 0


@dataclass(frozen=True)
class CollectOutcome:
    status: str
    submission_key: str = ""
    delivery_id: str = ""
    effect_key: str = ""
    error_code: str = ""
    next_poll_at: str | None = None
    created: bool | None = None


def default_status_reader(task_id: str) -> Mapping[str, Any]:
    """Read canonical shared-state truth without starting a completion process."""
    from tools.vm_task_tool import vm_task_status

    return vm_task_status(task_id, include_markdown=False)


def probe_remote_css_parser(
    ssh_mini_agent: str,
    *,
    timeout_seconds: int = 15,
    worker_root: str | None = None,
) -> dict[str, str]:
    """Run the hash-pinned, read-only VM parser runtime checker."""
    selected_root = PurePosixPath(
        worker_root or str(PurePosixPath(REMOTE_CSS_RUNTIME_CHECKER_PATH).parent)
    )
    if (
        not selected_root.is_absolute()
        or ".." in selected_root.parts
        or selected_root == PurePosixPath("/")
    ):
        raise ArtifactBundleReadError(
            "html_css_parser_probe_root_invalid",
            permanent=True,
        )
    checker_path = str(selected_root / PurePosixPath(REMOTE_CSS_RUNTIME_CHECKER_PATH).name)
    requirements_path = str(
        selected_root / PurePosixPath(REMOTE_CSS_RUNTIME_REQUIREMENTS_PATH).name
    )
    probe = textwrap.dedent(
        f"""
        set -euo pipefail
        checker={checker_path!r}
        requirements={requirements_path!r}
        test -f "$checker" && test ! -L "$checker"
        test -f "$requirements" && test ! -L "$requirements"
        test "$(/usr/bin/sha256sum "$checker" | /usr/bin/awk '{{print $1}}')" = {REMOTE_CSS_RUNTIME_CHECKER_SHA256!r}
        test "$(/usr/bin/sha256sum "$requirements" | /usr/bin/awk '{{print $1}}')" = {REMOTE_CSS_RUNTIME_REQUIREMENTS_SHA256!r}
        exec {REMOTE_CSS_RUNTIME_PYTHON} "$checker" \
          --requirements "$requirements" \
          --expected-python {REMOTE_CSS_RUNTIME_PYTHON} \
          --json
        """
    ).strip()
    try:
        process = subprocess.run(
            [ssh_mini_agent, "run_bash_json"],
            input=probe,
            text=True,
            capture_output=True,
            timeout=timeout_seconds,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ArtifactBundleReadError(
            "html_css_parser_probe_unavailable",
            type(exc).__name__,
            permanent=True,
        ) from exc
    try:
        payload = json.loads(process.stdout or "{}")
    except json.JSONDecodeError as exc:
        raise ArtifactBundleReadError(
            "html_css_parser_probe_invalid",
            "VM CSS parser probe returned invalid JSON",
            permanent=True,
        ) from exc
    expected_versions = {
        "tinycss2": REMOTE_CSS_PARSER_VERSION,
        "webencodings": REMOTE_CSS_WEBENCODINGS_VERSION,
    }
    expected_semantics = {
        "escaped_url_tokenized": True,
        "numeric_values_tokenized": True,
        "webencodings_utf8_lookup": True,
    }
    requirements = payload.get("requirements") if isinstance(payload, dict) else None
    python = payload.get("python") if isinstance(payload, dict) else None
    valid = (
        process.returncode == 0
        and isinstance(payload, dict)
        and payload.get("schema_version") == REMOTE_CSS_RUNTIME_CHECK_SCHEMA
        and payload.get("ok") is True
        and payload.get("mutates_state") is False
        and payload.get("errors") == []
        and payload.get("runtime_versions") == expected_versions
        and payload.get("semantic_checks") == expected_semantics
        and python
        == {
            "expected_executable": REMOTE_CSS_RUNTIME_PYTHON,
            "actual_executable": REMOTE_CSS_RUNTIME_PYTHON,
            "same_file": True,
        }
        and requirements
        == {
            "path": requirements_path,
            "sha256": REMOTE_CSS_RUNTIME_REQUIREMENTS_SHA256,
            "pins": expected_versions,
        }
    )
    if not valid:
        raise ArtifactBundleReadError(
            "html_css_parser_probe_failed",
            str(payload.get("errors") if isinstance(payload, dict) else "invalid"),
            permanent=True,
        )
    return expected_remote_css_runtime_dependency()


def _remote_bundle_script(submission_key: str) -> str:
    root = canonical_artifact_root(submission_key)
    return textwrap.dedent(
        f"""
        import hashlib
        import html
        import json
        import os
        import posixpath
        import stat
        from html.parser import HTMLParser
        from importlib import metadata
        from urllib.parse import unquote, urlsplit

        try:
            import tinycss2
        except Exception:
            tinycss2 = None

        ROOT = {root!r}
        MAX_JSON_BYTES = 8 * 1024 * 1024
        MAX_ARTIFACTS = 512
        MAX_FILE_BYTES = 256 * 1024 * 1024
        MAX_VIZ_BYTES = 64 * 1024 * 1024 * 1024
        MAX_TOTAL_BYTES = 512 * 1024 * 1024
        MAX_HTML_BYTES = 32 * 1024 * 1024
        MAX_TEXT_FILE_BYTES = 32 * 1024 * 1024
        MAX_TEXT_TOTAL_BYTES = 64 * 1024 * 1024
        CSS_PARSER_DISTRIBUTION = {REMOTE_CSS_PARSER_DISTRIBUTION!r}
        CSS_PARSER_VERSION = {REMOTE_CSS_PARSER_VERSION!r}
        FORMAL_VIZ_ROOT = posixpath.normpath(ROOT)

        def finish(value):
            print(json.dumps(value, ensure_ascii=False, sort_keys=True))

        def symlink_free(path, anchor=None):
            anchor = anchor or root_norm
            current = anchor
            try:
                if stat.S_ISLNK(os.lstat(current).st_mode):
                    return False
            except FileNotFoundError:
                return False
            relative = posixpath.relpath(path, anchor)
            for part in relative.split('/'):
                current = posixpath.join(current, part)
                try:
                    if stat.S_ISLNK(os.lstat(current).st_mode):
                        return False
                except FileNotFoundError:
                    return False
            return True

        def open_regular(path, missing_code, max_bytes, anchor=None):
            try:
                before = os.lstat(path)
            except FileNotFoundError:
                raise RuntimeError(missing_code)
            if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
                raise RuntimeError(missing_code + '_not_regular')
            if not symlink_free(path, anchor):
                raise RuntimeError(missing_code + '_not_regular')
            try:
                fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
            except (FileNotFoundError, OSError):
                raise RuntimeError(missing_code + '_open_failed')
            after = os.fstat(fd)
            if (
                not stat.S_ISREG(after.st_mode)
                or before.st_dev != after.st_dev
                or before.st_ino != after.st_ino
            ):
                os.close(fd)
                raise RuntimeError(missing_code + '_changed_during_read')
            if after.st_size <= 0 or after.st_size > max_bytes:
                os.close(fd)
                raise RuntimeError(missing_code + '_size_invalid')
            return fd, after

        def read_json(path, missing_code, anchor=None):
            fd, _info = open_regular(path, missing_code, MAX_JSON_BYTES, anchor)
            with os.fdopen(fd, 'rb') as handle:
                raw = handle.read(MAX_JSON_BYTES + 1)
            try:
                value = json.loads(raw.decode('utf-8'))
            except Exception:
                raise RuntimeError(missing_code.replace('_missing', '') + '_json_invalid')
            if not isinstance(value, dict):
                raise RuntimeError(missing_code.replace('_missing', '') + '_json_invalid')
            return value

        def read_text_artifact(path, expected):
            fd, info = open_regular(path, 'html_dependency_missing', MAX_TEXT_FILE_BYTES)
            with os.fdopen(fd, 'rb') as handle:
                raw = handle.read(MAX_TEXT_FILE_BYTES + 1)
            if info.st_size != expected['size']:
                raise RuntimeError('html_dependency_changed_during_read')
            if hashlib.sha256(raw).hexdigest() != expected['sha256']:
                raise RuntimeError('html_dependency_changed_during_read')
            try:
                return raw.decode('utf-8'), len(raw)
            except UnicodeDecodeError:
                raise RuntimeError('html_dependency_text_invalid')

        def dependency_kind(path, media_type):
            lowered = path.lower()
            media = str(media_type or '').split(';', 1)[0].strip().lower()
            if media in ('text/html', 'application/xhtml+xml') or lowered.endswith(('.html', '.htm')):
                return 'html'
            if media == 'text/css' or lowered.endswith('.css'):
                return 'css'
            if (
                'javascript' in media
                or 'ecmascript' in media
                or lowered.endswith(('.js', '.mjs', '.cjs'))
            ):
                return 'javascript'
            return ''

        def require_css_parser():
            if tinycss2 is None:
                raise RuntimeError('html_css_parser_dependency_missing')
            try:
                installed = metadata.version(CSS_PARSER_DISTRIBUTION)
            except Exception:
                raise RuntimeError('html_css_parser_dependency_missing')
            if installed != CSS_PARSER_VERSION:
                raise RuntimeError('html_css_parser_version_mismatch')

        def css_token_refs(tokens):
            refs = []
            for token in tokens or ():
                token_type = str(getattr(token, 'type', '') or '').lower()
                if token_type == 'error':
                    raise RuntimeError('html_css_syntax_invalid')
                if token_type == 'url':
                    refs.append((str(token.value), ''))
                    continue
                if token_type == 'function':
                    function_name = str(
                        getattr(token, 'lower_name', None)
                        or getattr(token, 'name', '')
                    ).lower()
                    if function_name in (
                        'image',
                        'image-set',
                        '-webkit-image-set',
                        'cross-fade',
                        '-webkit-cross-fade',
                        'src',
                        'paint',
                        'element',
                        '-moz-element',
                    ):
                        raise RuntimeError('html_css_dynamic_resource_unsupported')
                    arguments = list(getattr(token, 'arguments', ()) or ())
                    if function_name == 'url':
                        meaningful = [
                            item
                            for item in arguments
                            if str(getattr(item, 'type', '') or '')
                            not in ('whitespace', 'comment')
                        ]
                        if len(meaningful) != 1 or meaningful[0].type != 'string':
                            raise RuntimeError('html_css_dynamic_resource_unsupported')
                        refs.append((str(meaningful[0].value), ''))
                    else:
                        refs.extend(css_token_refs(arguments))
                    continue
                if token_type == 'at-rule':
                    prelude = list(getattr(token, 'prelude', ()) or ())
                    if str(getattr(token, 'lower_at_keyword', '') or '') == 'import':
                        meaningful = [
                            item
                            for item in prelude
                            if str(getattr(item, 'type', '') or '')
                            not in ('whitespace', 'comment')
                        ]
                        if not meaningful:
                            raise RuntimeError('html_css_syntax_invalid')
                        first = meaningful[0]
                        if first.type == 'string':
                            refs.append((str(first.value), 'css'))
                        elif first.type == 'url':
                            refs.append((str(first.value), 'css'))
                        elif first.type == 'function':
                            function_name = str(
                                getattr(first, 'lower_name', None)
                                or getattr(first, 'name', '')
                            ).lower()
                            arguments = [
                                item
                                for item in list(
                                    getattr(first, 'arguments', ()) or ()
                                )
                                if str(getattr(item, 'type', '') or '')
                                not in ('whitespace', 'comment')
                            ]
                            if (
                                function_name != 'url'
                                or len(arguments) != 1
                                or arguments[0].type != 'string'
                            ):
                                raise RuntimeError(
                                    'html_css_dynamic_resource_unsupported'
                                )
                            refs.append((str(arguments[0].value), 'css'))
                        else:
                            raise RuntimeError('html_css_dynamic_resource_unsupported')
                        prelude = [item for item in prelude if item is not first]
                    refs.extend(css_token_refs(prelude))
                    refs.extend(css_token_refs(getattr(token, 'content', ()) or ()))
                    continue
                for field in ('prelude', 'content', 'value'):
                    nested = getattr(token, field, None)
                    if isinstance(nested, (list, tuple)):
                        refs.extend(css_token_refs(nested))
            return refs

        def css_refs(text, mode='stylesheet'):
            require_css_parser()
            try:
                if mode == 'declarations':
                    tokens = tinycss2.parse_declaration_list(
                        text,
                        skip_comments=False,
                        skip_whitespace=False,
                    )
                elif mode == 'component':
                    tokens = tinycss2.parse_component_value_list(
                        text,
                        skip_comments=False,
                    )
                else:
                    tokens = tinycss2.parse_stylesheet(
                        text,
                        skip_comments=False,
                        skip_whitespace=False,
                    )
            except Exception:
                raise RuntimeError('html_css_syntax_invalid')
            return css_token_refs(tokens)

        def html_refs(text, depth=0):
            if depth > 8:
                raise RuntimeError('html_srcdoc_nesting_too_deep')
            lowered_markup = text.lower()
            if '<!--' in lowered_markup:
                raise RuntimeError('html_comments_unsupported')
            if '<?' in lowered_markup:
                raise RuntimeError('html_processing_instruction_unsupported')
            if '<!' in lowered_markup.replace('<!doctype html>', ''):
                raise RuntimeError('html_declaration_unsupported')

            class DependencyParser(HTMLParser):
                def __init__(self):
                    super().__init__(convert_charrefs=True)
                    self.refs = []
                    self.capture_tag = ''
                    self.capture_data = []
                    self.svg_depth = 0

                def handle_starttag(self, tag, attrs):
                    tag = str(tag or '').lower()
                    attribute_map = {{}}
                    for raw_name, raw_value in attrs:
                        name = str(raw_name or '').lower()
                        if name in attribute_map:
                            raise RuntimeError('html_duplicate_attribute_unsupported')
                        attribute_map[name] = '' if raw_value is None else str(raw_value)
                    in_svg = self.svg_depth > 0 or tag == 'svg'
                    if tag in ('iframe', 'frame', 'object', 'embed', 'applet'):
                        raise RuntimeError('html_active_content_unsupported')
                    if in_svg and tag in (
                        'animate',
                        'animatecolor',
                        'animatemotion',
                        'animatetransform',
                        'set',
                    ):
                        raise RuntimeError('html_active_content_unsupported')
                    if tag == 'script':
                        script_type = attribute_map.get('type', '').strip().lower()
                        if script_type not in ('application/json', 'application/ld+json'):
                            raise RuntimeError('html_script_execution_unsupported')
                    if tag == 'form' or any(
                        name in attribute_map
                        for name in ('action', 'formaction', 'ping')
                    ):
                        raise RuntimeError('html_active_navigation_unsupported')
                    if any(name.startswith('on') for name in attribute_map):
                        raise RuntimeError('html_script_execution_unsupported')
                    for name in (
                        'href',
                        'xlink:href',
                        'src',
                        'poster',
                        'background',
                        'manifest',
                    ):
                        if name not in attribute_map:
                            continue
                        value = attribute_map[name].strip()
                        value = value.replace(chr(9), '').replace(chr(10), '')
                        value = value.replace(chr(13), '')
                        scheme = urlsplit(value).scheme.lower()
                        if scheme in ('javascript', 'vbscript'):
                            raise RuntimeError('html_script_execution_unsupported')
                        if scheme == 'data':
                            raise RuntimeError(
                                'html_embedded_data_dependency_unsupported'
                            )
                        if tag in ('a', 'area') and scheme not in ('', 'http', 'https'):
                            raise RuntimeError('html_navigation_scheme_unsupported')
                    if tag == 'base' and 'href' in attribute_map:
                        raise RuntimeError('html_base_url_unsupported')
                    if (
                        tag == 'meta'
                        and attribute_map.get('http-equiv', '').strip().lower() == 'refresh'
                    ):
                        raise RuntimeError('html_dynamic_dependency_unsupported')
                    for name in ('src', 'poster', 'background', 'manifest'):
                        if name in attribute_map:
                            self.refs.append((attribute_map[name], ''))
                    if tag == 'link' and 'href' in attribute_map:
                        rel = {{
                            value.lower()
                            for value in attribute_map.get('rel', '').split()
                        }}
                        expected_kind = 'css' if 'stylesheet' in rel else ''
                        self.refs.append((attribute_map['href'], expected_kind))
                    if tag in ('a', 'area') and 'href' in attribute_map:
                        href = attribute_map['href'].strip()
                        normalized_href = href.replace(chr(9), '').replace(chr(10), '')
                        normalized_href = normalized_href.replace(chr(13), '')
                        if (
                            normalized_href
                            and not normalized_href.startswith('#')
                            and not urlsplit(normalized_href).scheme
                        ):
                            self.refs.append((href, ''))
                    if in_svg and tag != 'a':
                        for name in ('href', 'xlink:href'):
                            if name in attribute_map:
                                self.refs.append((attribute_map[name], ''))
                    for name in ('srcset', 'imagesrcset'):
                        if name not in attribute_map:
                            continue
                        srcset = attribute_map[name]
                        for candidate in srcset.split(','):
                            fields = candidate.strip().split()
                            if not fields or len(fields) > 3:
                                raise RuntimeError('html_srcset_syntax_unsupported')
                            self.refs.append((fields[0], ''))
                    if 'style' in attribute_map:
                        self.refs.extend(
                            css_refs(attribute_map['style'], mode='declarations')
                        )
                    if in_svg:
                        for name, value in attribute_map.items():
                            if name not in ('style', 'href', 'xlink:href'):
                                self.refs.extend(css_refs(value, mode='component'))
                    if tag == 'style':
                        self.capture_tag = tag
                        self.capture_data = []
                    if tag == 'svg':
                        self.svg_depth += 1

                def handle_startendtag(self, tag, attrs):
                    self.handle_starttag(tag, attrs)
                    self.handle_endtag(tag)

                def handle_endtag(self, tag):
                    tag = str(tag or '').lower()
                    if tag == self.capture_tag:
                        self.refs.extend(css_refs(''.join(self.capture_data)))
                        self.capture_tag = ''
                        self.capture_data = []
                    if tag == 'svg' and self.svg_depth:
                        self.svg_depth -= 1

                def handle_data(self, data):
                    if self.capture_tag == 'style':
                        self.capture_data.append(data)

                def handle_comment(self, _data):
                    raise RuntimeError('html_comments_unsupported')

                def handle_decl(self, declaration):
                    if str(declaration or '').strip().lower() != 'doctype html':
                        raise RuntimeError('html_declaration_unsupported')

                def unknown_decl(self, _data):
                    raise RuntimeError('html_declaration_unsupported')

                def handle_pi(self, _data):
                    raise RuntimeError('html_processing_instruction_unsupported')

            parser = DependencyParser()
            try:
                parser.feed(text)
                parser.close()
                if parser.capture_tag:
                    raise RuntimeError('html_markup_invalid')
            except RuntimeError:
                raise
            except Exception:
                raise RuntimeError('html_markup_invalid')
            return parser.refs

        def resolve_ref(source_path, raw_ref):
            ref = html.unescape(str(raw_ref or '').strip())
            if not ref or ref.startswith('#'):
                return None
            if ref.lower().startswith('data:'):
                raise RuntimeError('html_embedded_data_dependency_unsupported')
            if '\\\\' in ref:
                raise RuntimeError('artifact_path_outside_root')
            parsed = urlsplit(ref)
            if parsed.scheme or parsed.netloc or ref.startswith('/'):
                raise RuntimeError('html_external_dependency_unsupported')
            parsed_path = unquote(parsed.path)
            if not parsed_path:
                return None
            dep_path = posixpath.normpath(
                posixpath.join(posixpath.dirname(source_path), parsed_path)
            )
            if posixpath.commonpath((root_norm, dep_path)) != root_norm:
                raise RuntimeError('artifact_path_outside_root')
            if dep_path.lower().endswith('.mcap'):
                raise RuntimeError('html_delivery_mcap_forbidden')
            return dep_path

        try:
            root_norm = posixpath.normpath(ROOT)
            contract = read_json(ROOT + 'delivery_contract.json', 'delivery_contract_missing')
            manifest = read_json(ROOT + 'delivery_manifest.json', 'delivery_manifest_missing')
            contract_artifacts = contract.get('artifacts')
            if not isinstance(contract_artifacts, dict):
                raise RuntimeError('viz_publication_missing')
            viz_publication = contract_artifacts.get('viz_publication')
            if not isinstance(viz_publication, dict):
                raise RuntimeError('viz_publication_missing')
            viz_path = str(viz_publication.get('path') or '')
            viz_manifest_path = str(viz_publication.get('manifest_path') or '')
            submission_key = str(viz_publication.get('submission_key') or '')
            expected_viz_path = posixpath.join(
                FORMAL_VIZ_ROOT, submission_key + '.viz.mcap'
            )
            expected_viz_manifest_path = posixpath.join(
                FORMAL_VIZ_ROOT, submission_key + '.viz.manifest.json',
            )
            if (
                not submission_key
                or viz_path != expected_viz_path
                or viz_manifest_path != expected_viz_manifest_path
                or posixpath.commonpath((FORMAL_VIZ_ROOT, viz_path)) != FORMAL_VIZ_ROOT
            ):
                raise RuntimeError('viz_publication_path_invalid')
            viz_fd, viz_info = open_regular(
                viz_path, 'viz_publication_missing', MAX_VIZ_BYTES, FORMAL_VIZ_ROOT
            )
            os.close(viz_fd)
            viz_manifest_fd, viz_manifest_info = open_regular(
                viz_manifest_path,
                'viz_publication_manifest_missing',
                MAX_JSON_BYTES,
                FORMAL_VIZ_ROOT,
            )
            with os.fdopen(viz_manifest_fd, 'rb') as handle:
                viz_manifest_raw = handle.read(MAX_JSON_BYTES + 1)
            try:
                viz_manifest = json.loads(viz_manifest_raw.decode('utf-8'))
            except Exception:
                raise RuntimeError('viz_publication_manifest_json_invalid')
            if not isinstance(viz_manifest, dict) or any(
                viz_manifest.get(key) != viz_publication.get(key)
                for key in (
                    'schema_version', 'status', 'submission_key', 'path',
                    'size', 'sha256', 'source_path', 'source_sha256',
                    'published_at',
                )
            ):
                raise RuntimeError('viz_publication_manifest_mismatch')
            if viz_info.st_size != viz_publication.get('size'):
                raise RuntimeError('viz_publication_size_mismatch')
            rows = manifest.get('artifacts')
            if not isinstance(rows, list) or not rows or len(rows) > MAX_ARTIFACTS:
                raise RuntimeError('delivery_manifest_artifacts_invalid')
            observed = []
            observed.extend([
                {{
                    'path': viz_path,
                    'size': viz_info.st_size,
                    'sha256': str(viz_publication.get('sha256') or ''),
                    'is_file': True,
                    'is_symlink': False,
                    'parents_symlink_free': True,
                    'sha256_attested_by_manifest': True,
                }},
                {{
                    'path': viz_manifest_path,
                    'size': viz_manifest_info.st_size,
                    'sha256': hashlib.sha256(viz_manifest_raw).hexdigest(),
                    'is_file': True,
                    'is_symlink': False,
                    'parents_symlink_free': True,
                }},
            ])
            artifact_meta = {{}}
            total = 0
            html_path = ''
            for row in rows:
                if not isinstance(row, dict):
                    raise RuntimeError('delivery_manifest_artifacts_invalid')
                raw_path = str(row.get('path') or '')
                if not raw_path or '..' in raw_path.split('/') or '\\x00' in raw_path:
                    raise RuntimeError('artifact_path_invalid')
                role = str(row.get('role') or '').strip().lower()
                media_type = str(row.get('media_type') or '').strip().lower()
                if (
                    raw_path.lower().endswith('.mcap')
                    or role in ('mcap', 'viz_mcap', 'visualization_mcap')
                    or 'mcap' in media_type
                ):
                    raise RuntimeError('html_delivery_mcap_forbidden')
                if (
                    raw_path.lower().endswith(('.svg', '.svgz', '.xhtml', '.xml'))
                    or media_type.split(';', 1)[0]
                    in (
                        'image/svg+xml',
                        'application/svg+xml',
                        'application/xhtml+xml',
                        'application/xml',
                        'text/xml',
                    )
                ):
                    raise RuntimeError('html_external_active_document_unsupported')
                path = posixpath.normpath(
                    raw_path if raw_path.startswith('/') else posixpath.join(ROOT, raw_path)
                )
                if posixpath.commonpath((root_norm, path)) != root_norm or path == root_norm:
                    raise RuntimeError('artifact_path_outside_root')
                fd, info = open_regular(path, 'artifact_missing', MAX_FILE_BYTES)
                is_symlink = False
                is_file = True
                total += info.st_size
                if total > MAX_TOTAL_BYTES:
                    os.close(fd)
                    raise RuntimeError('artifact_bundle_too_large')
                digest = hashlib.sha256()
                with os.fdopen(fd, 'rb') as handle:
                    for chunk in iter(lambda: handle.read(1024 * 1024), b''):
                        digest.update(chunk)
                observed.append({{
                    'path': path,
                    'size': info.st_size,
                    'sha256': digest.hexdigest(),
                    'is_file': is_file,
                    'is_symlink': is_symlink,
                    'parents_symlink_free': symlink_free(path),
                }})
                if path in artifact_meta:
                    raise RuntimeError('delivery_manifest_duplicate_artifact')
                artifact_meta[path] = {{
                    'media_type': media_type,
                    'size': info.st_size,
                    'sha256': digest.hexdigest(),
                }}
                if role == 'index_html':
                    html_path = path
            if not html_path:
                raise RuntimeError('required_html_artifact_missing')
            dependencies = []
            dependency_set = set()
            visited = set()
            queue = [(html_path, 'html')]
            text_total = 0
            while queue:
                source_path, expected_kind = queue.pop(0)
                source_meta = artifact_meta.get(source_path)
                if source_meta is None:
                    raise RuntimeError('html_dependency_not_manifested')
                kind = expected_kind or dependency_kind(
                    source_path,
                    source_meta['media_type'],
                )
                if not kind:
                    continue
                visit_key = (source_path, kind)
                if visit_key in visited:
                    continue
                visited.add(visit_key)
                text, text_size = read_text_artifact(source_path, source_meta)
                text_total += text_size
                if text_total > MAX_TEXT_TOTAL_BYTES:
                    raise RuntimeError('html_dependency_text_total_too_large')
                if kind == 'html':
                    refs = html_refs(text)
                elif kind == 'css':
                    refs = css_refs(text)
                else:
                    raise RuntimeError('html_script_execution_unsupported')
                for raw_ref, dependency_expected_kind in refs:
                    dep_path = resolve_ref(source_path, raw_ref)
                    if dep_path is None:
                        continue
                    if dep_path not in artifact_meta:
                        raise RuntimeError('html_dependency_not_manifested')
                    if dep_path not in dependency_set:
                        dependency_set.add(dep_path)
                        dependencies.append(dep_path)
                    if (dep_path, dependency_expected_kind) not in visited:
                        queue.append((dep_path, dependency_expected_kind))
            finish({{
                'ok': True,
                'delivery_contract': contract,
                'delivery_manifest': manifest,
                'observed_files': observed,
                'html_dependencies': dependencies,
            }})
        except RuntimeError as exc:
            code = str(exc)
            finish({{'ok': False, 'error_code': code, 'error': code}})
        """
    ).strip()


def default_artifact_bundle_reader(
    claim: ExecutionWatchClaim,
    *,
    ssh_mini_agent: str = DEFAULT_SSH_MINI_AGENT,
    timeout_seconds: int = 120,
) -> Mapping[str, Any]:
    """Hash one canonical VM bundle through a bounded, read-only agent script."""
    script = _remote_bundle_script(claim.submission_key)
    try:
        proc = subprocess.run(
            [ssh_mini_agent, "run_py_json"],
            input=script,
            text=True,
            capture_output=True,
            timeout=timeout_seconds,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ArtifactBundleReadError(
            "artifact_reader_unavailable", type(exc).__name__
        ) from exc
    if proc.returncode != 0:
        raise ArtifactBundleReadError(
            "artifact_reader_unavailable",
            (proc.stderr or proc.stdout or f"ssh-mini-agent rc={proc.returncode}")[
                -1000:
            ],
        )
    try:
        payload = json.loads(proc.stdout or "{}")
    except json.JSONDecodeError as exc:
        raise ArtifactBundleReadError(
            "artifact_reader_response_invalid", "ssh-mini-agent returned invalid JSON"
        ) from exc
    if not isinstance(payload, dict):
        raise ArtifactBundleReadError("artifact_reader_response_invalid")
    if payload.get("ok") is not True:
        code = str(payload.get("error_code") or "artifact_bundle_unavailable")
        permanent = code not in {
            "artifact_reader_unavailable",
            "artifact_bundle_unavailable",
        } | _RETRYABLE_INFRASTRUCTURE_ARTIFACT_CODES and not _eventual_artifact_error(
            code
        )
        raise ArtifactBundleReadError(
            code, str(payload.get("error") or code), permanent=permanent
        )
    return payload


def _submission_admission(claim: ExecutionWatchClaim):
    payload = claim.submission_payload
    if payload.get("schema_version") != SUBMISSION_OUTBOX_SCHEMA_VERSION:
        raise DeliveryContractError("submission_outbox_contract_invalid")
    try:
        admission = validate_rca_admission(payload.get("admission") or {})
        trigger_context = validate_rca_trigger_context(
            payload.get("trigger_context") or {}
        )
    except Exception as exc:
        raise DeliveryContractError("submission_outbox_contract_invalid") from exc
    refs = admission.source_refs
    base_keys = {
        "schema_version",
        "business_key",
        "submission_key",
        "creation_rule_version",
        "generation",
        "origin_source_id",
        "admission",
        "trigger_context",
    }
    source_kind = trigger_context.source_kind
    if source_kind == "kafka_workflow_event":
        expected_keys = base_keys | {
            "source_event_id",
            "topic",
            "partition",
            "offset",
            "normalized_event",
        }
        normalized = payload.get("normalized_event")
        if not isinstance(normalized, Mapping):
            raise DeliveryContractError("submission_outbox_contract_invalid")
        expected_trigger_kind = (
            "issue_created" if claim.generation == 1 else "kafka_retrigger"
        )
        try:
            expected_admission = build_rca_admission(
                project_key=trigger_context.project_key,
                project_simple_name=trigger_context.project_simple_name,
                work_item_type_key=trigger_context.work_item_type_key,
                work_item_id=trigger_context.work_item_id,
                rule_version=trigger_context.creation_rule_version,
                trigger_kind=expected_trigger_kind,
                generation=claim.generation,
                topic=payload.get("topic", ""),
                partition=payload.get("partition"),
                offset=payload.get("offset"),
            )
        except Exception as exc:
            raise DeliveryContractError("submission_outbox_contract_invalid") from exc
        event_uid = (
            f"{payload.get('topic')}:{payload.get('partition')}:"
            f"{payload.get('offset')}"
        )
        normalized_identity = {
            "schema_version": normalized.get("schema_version"),
            "creation_rule_version": normalized.get("creation_rule_version"),
            "project_key": normalized.get("project_key"),
            "project_simple_name": normalized.get("project_simple_name"),
            "work_item_type_key": normalized.get("work_item_type_key"),
            "work_item_id": normalized.get("work_item_id"),
            "issue_url": normalized.get("issue_url"),
            "title": normalized.get("title"),
        }
        trigger_identity = trigger_context.to_dict()
        trigger_identity.pop("source_kind")
        trigger_identity["schema_version"] = NORMALIZED_EVENT_SCHEMA_VERSION
        if (
            payload.get("source_event_id") != event_uid
            or normalized_identity != trigger_identity
            or admission.trigger_kind not in RCA_KAFKA_TRIGGER_KINDS
        ):
            raise DeliveryContractError("submission_outbox_contract_invalid")
    elif source_kind == "feishu_group_manual":
        expected_keys = base_keys
        expected_trigger_kind = (
            "manual_issue_request" if claim.generation == 1 else "manual_retrigger"
        )
        try:
            expected_admission = build_rca_admission(
                project_key=trigger_context.project_key,
                project_simple_name=trigger_context.project_simple_name,
                work_item_type_key=trigger_context.work_item_type_key,
                work_item_id=trigger_context.work_item_id,
                rule_version=trigger_context.creation_rule_version,
                trigger_kind=expected_trigger_kind,
                generation=claim.generation,
            )
        except Exception as exc:
            raise DeliveryContractError("submission_outbox_contract_invalid") from exc
        if refs.topic != "" or refs.partition is not None or refs.offset is not None:
            raise DeliveryContractError("submission_outbox_contract_invalid")
        if admission.trigger_kind not in RCA_MANUAL_TRIGGER_KINDS:
            raise DeliveryContractError("submission_outbox_contract_invalid")
    else:
        raise DeliveryContractError("submission_outbox_contract_invalid")

    origin_source_id = str(payload.get("origin_source_id") or "").strip()
    if (
        set(payload) != expected_keys
        or not origin_source_id
        or origin_source_id != claim.origin_source_id
        or origin_source_id != claim.trigger_origin_source_id
        or admission != expected_admission
        or admission.trigger_kind != expected_trigger_kind
        or admission.submission_key != claim.submission_key
        or admission.business_key != claim.business_key
        or admission.generation != claim.generation
        or payload.get("submission_key") != claim.submission_key
        or payload.get("business_key") != claim.business_key
        or payload.get("generation") != claim.generation
        or payload.get("creation_rule_version") != refs.rule_version
        or refs.project_key != claim.project_key
        or refs.work_item_type_key != claim.work_item_type_key
        or refs.work_item_id != claim.work_item_id
    ):
        raise DeliveryContractError("submission_watch_identity_mismatch")
    result = claim.submission_result
    if (
        result.get("success") is not True
        or str(result.get("submission_key") or "").strip() != claim.submission_key
        or str(result.get("task_id") or "").strip() != claim.task_id
    ):
        raise DeliveryContractError("submission_receipt_identity_mismatch")
    return admission


def _status_state(status: Mapping[str, Any]) -> str:
    return (
        str(status.get("state") or status.get("dispatch_queue") or "").strip().lower()
    )


def _terminal_failure(status: Mapping[str, Any], state: str) -> tuple[str, str]:
    blocker = (
        status.get("blocker") if isinstance(status.get("blocker"), Mapping) else {}
    )
    blocker_kind = str(blocker.get("kind") or "").strip().lower()
    public_blocker = _PUBLIC_TERMINAL_BLOCKER_CODES.get(blocker_kind)
    code = f"vm_terminal_{state}_{public_blocker or 'unclassified'}"
    detail = str(
        blocker.get("message") or status.get("summary") or status.get("error") or state
    )
    return code, detail


def _public_terminal_error_code(value: Any) -> str:
    candidate = str(value or "").strip()
    vm_codes = {
        f"vm_terminal_{state}_{suffix}"
        for state in _FAILED_TERMINAL_STATES
        for suffix in {*_PUBLIC_TERMINAL_BLOCKER_CODES.values(), "unclassified"}
    }
    if candidate in _PUBLIC_TERMINAL_ERROR_CODES or candidate in vm_codes:
        return candidate
    return _PUBLIC_TERMINAL_FALLBACK_CODE


class DeliveryCollector:
    def __init__(
        self,
        *,
        store: RcaDeliveryStore,
        config: CollectorConfig,
        status_reader: StatusReader = default_status_reader,
        artifact_bundle_reader: ArtifactBundleReader | None = None,
        now: Callable[[], datetime] = _utc_now,
        lease_owner: str | None = None,
    ):
        self.store = store
        self.config = config
        self.status_reader = status_reader
        self.artifact_bundle_reader = artifact_bundle_reader or (
            lambda claim: default_artifact_bundle_reader(
                claim,
                ssh_mini_agent=config.ssh_mini_agent,
                timeout_seconds=config.artifact_read_timeout_seconds,
            )
        )
        self.now = now
        self.lease_owner = lease_owner or (
            f"rca-delivery-collector:{socket.gethostname()}:{os.getpid()}"
        )
        self.stats = CollectorStats()
        self.runtime_identity: Mapping[str, Any] | None = None

    def backfill(self) -> int:
        inserted = self.store.backfill_completed_submissions(
            limit=self.config.backfill_batch_size,
            now=self.now(),
            activation_required=False,
        )
        self.stats.watches_created += inserted
        return inserted

    def _next_poll(self, attempt: int, *, running: bool) -> datetime:
        base = self.config.running_poll_seconds
        seconds = (
            base
            if running
            else min(
                self.config.max_poll_seconds,
                base * (2 ** min(max(attempt - 1, 0), 4)),
            )
        )
        return self.now() + timedelta(seconds=seconds)

    def _retry(
        self,
        claim: ExecutionWatchClaim,
        *,
        status: dict[str, Any],
        observed_state: str,
        error_code: str,
        error_detail: str,
        running: bool = False,
    ) -> CollectOutcome:
        next_poll = self._next_poll(claim.poll_attempt, running=running)
        self.store.reschedule_watch(
            submission_key=claim.submission_key,
            lease_token=claim.lease_token,
            observed_state=observed_state,
            status=status,
            next_poll_at=next_poll,
            error_code=error_code,
            error_detail=error_detail,
            now=self.now(),
        )
        if running:
            self.stats.running += 1
            outcome = "running"
        else:
            self.stats.retried += 1
            outcome = "retry_wait"
        return CollectOutcome(
            status=outcome,
            submission_key=claim.submission_key,
            error_code=error_code,
            next_poll_at=_utc_iso(next_poll),
        )

    def _durable_terminal_outcome(
        self,
        claim: ExecutionWatchClaim,
        *,
        status: dict[str, Any],
        outcome: str,
        terminal_state: str,
        error_code: str,
        error_detail: str,
    ) -> CollectOutcome:
        safe_outcome = (
            outcome if outcome in TERMINAL_DELIVERY_OUTCOMES else "quarantined"
        )
        safe_state = (
            terminal_state
            if terminal_state in _FAILED_TERMINAL_STATES | {"quarantined"}
            else "quarantined"
        )
        safe_error_code = _public_terminal_error_code(error_code)
        try:
            result = self.store.create_terminal_delivery(
                claim=claim,
                status=status,
                outcome=safe_outcome,
                terminal_state=safe_state,
                error_code=safe_error_code,
                error_detail=error_detail,
                runtime_identity=self.runtime_identity,
                now=self.now(),
                activation_required=False,
            )
        except StaleDeliveryWatchLeaseError:
            self.stats.stale_lease += 1
            return CollectOutcome(
                status="lease_lost",
                submission_key=claim.submission_key,
                error_code="stale_delivery_watch_lease",
            )
        except DeliveryRecordConflictError as exc:
            return self._retry(
                claim,
                status=status,
                observed_state=claim.state,
                error_code="delivery_record_conflict",
                error_detail=f"delivery_record_conflict: {exc}",
            )
        if safe_outcome == "terminal_failed":
            self.stats.terminal_failed += 1
        else:
            self.stats.quarantined += 1
        if result.created:
            self.stats.delivery_created += 1
        else:
            self.stats.delivery_deduped += 1
        return CollectOutcome(
            status=safe_outcome,
            submission_key=claim.submission_key,
            delivery_id=result.delivery_id,
            effect_key=result.effect_key,
            error_code=safe_error_code,
            created=result.created,
        )

    def _terminal_artifact_pending_or_expired(
        self,
        claim: ExecutionWatchClaim,
        *,
        status: dict[str, Any],
        terminal_first_seen_at: str,
        source_code: str,
        source_detail: str,
    ) -> CollectOutcome:
        try:
            first_seen = datetime.fromisoformat(
                terminal_first_seen_at.replace("Z", "+00:00")
            ).astimezone(timezone.utc)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("stored terminal_first_seen_at is invalid") from exc
        age = (self.now().astimezone(timezone.utc) - first_seen).total_seconds()
        if age < self.config.terminal_artifact_grace_seconds:
            return self._retry(
                claim,
                status=status,
                observed_state="completed",
                error_code="terminal_artifact_pending",
                error_detail=f"{source_code}: {source_detail}"[:1000],
            )
        detail = (
            f"{source_code} remained unresolved for {int(age)}s after VM completion: "
            f"{source_detail}"
        )
        return self._durable_terminal_outcome(
            claim,
            status=status,
            outcome="quarantined",
            terminal_state="quarantined",
            error_code="terminal_artifact_grace_exceeded",
            error_detail=detail,
        )

    def collect_one(self) -> CollectOutcome:
        self.stats.loops += 1
        if not self.config.enabled:
            return CollectOutcome(status="disabled")
        claim = self.store.claim_due_watch(
            lease_owner=self.lease_owner,
            lease_seconds=self.config.lease_seconds,
            now=self.now(),
            activation_required=False,
        )
        if claim is None:
            self.stats.idle += 1
            return CollectOutcome(status="idle")
        self.stats.claimed += 1
        status: dict[str, Any] = {}
        try:
            admission = _submission_admission(claim)
        except Exception as exc:
            code = (
                exc.code
                if isinstance(exc, DeliveryContractError)
                else "submission_admission_invalid"
            )
            return self._durable_terminal_outcome(
                claim,
                status=status,
                outcome="quarantined",
                terminal_state="quarantined",
                error_code=code,
                error_detail=f"{code}: {exc}",
            )

        try:
            raw_status = self.status_reader(claim.task_id)
            if not isinstance(raw_status, Mapping):
                raise TypeError("status_reader must return an object")
            status = dict(raw_status)
        except Exception as exc:
            return self._retry(
                claim,
                status=status,
                observed_state=claim.state,
                error_code="vm_status_reader_unavailable",
                error_detail=type(exc).__name__,
            )

        state = _status_state(status)
        if status.get("success") is not True:
            code = (
                "vm_status_missing" if state == "missing" else "vm_status_unavailable"
            )
            return self._retry(
                claim,
                status=status,
                observed_state=state,
                error_code=code,
                error_detail=str(status.get("error") or code),
            )
        if state in _RUNNING_STATES:
            return self._retry(
                claim,
                status=status,
                observed_state=state,
                error_code="",
                error_detail="",
                running=True,
            )
        if state in _FAILED_TERMINAL_STATES:
            code, detail = _terminal_failure(status, state)
            return self._durable_terminal_outcome(
                claim,
                status=status,
                outcome="terminal_failed",
                terminal_state=state,
                error_code=code,
                error_detail=detail,
            )
        if state not in _COMPLETED_STATES:
            return self._retry(
                claim,
                status=status,
                observed_state=state,
                error_code="vm_status_unknown",
                error_detail=f"unrecognized VM state: {state or 'missing'}",
            )

        terminal_first_seen_at = self.store.note_terminal_completion(
            submission_key=claim.submission_key,
            lease_token=claim.lease_token,
            status=status,
            now=self.now(),
        )

        try:
            bundle = self.artifact_bundle_reader(claim)
            if not isinstance(bundle, Mapping):
                raise ArtifactBundleReadError("artifact_reader_response_invalid")
            delivery: VerifiedDelivery = verify_delivery_bundle(
                admission=admission,
                delivery_contract=bundle.get("delivery_contract") or {},
                delivery_manifest=bundle.get("delivery_manifest") or {},
                observed_files=bundle.get("observed_files") or [],
                html_dependencies=bundle.get("html_dependencies") or [],
            )
        except ArtifactBundleReadError as exc:
            if _eventual_artifact_error(exc.code):
                return self._terminal_artifact_pending_or_expired(
                    claim,
                    status=status,
                    terminal_first_seen_at=terminal_first_seen_at,
                    source_code=exc.code,
                    source_detail=exc.detail,
                )
            if not exc.permanent:
                return self._retry(
                    claim,
                    status=status,
                    observed_state=state,
                    error_code=exc.code,
                    error_detail=exc.detail,
                )
            return self._durable_terminal_outcome(
                claim,
                status=status,
                outcome="quarantined",
                terminal_state="quarantined",
                error_code=exc.code,
                error_detail=f"{exc.code}: {exc.detail}",
            )
        except DeliveryContractError as exc:
            if _eventual_artifact_error(exc.code):
                return self._terminal_artifact_pending_or_expired(
                    claim,
                    status=status,
                    terminal_first_seen_at=terminal_first_seen_at,
                    source_code=exc.code,
                    source_detail=exc.detail,
                )
            return self._durable_terminal_outcome(
                claim,
                status=status,
                outcome="quarantined",
                terminal_state="quarantined",
                error_code=exc.code,
                error_detail=f"{exc.code}: {exc.detail}",
            )
        try:
            result = self.store.create_delivery(
                claim=claim,
                delivery=delivery,
                status=status,
                runtime_identity=self.runtime_identity,
                now=self.now(),
                activation_required=False,
            )
        except StaleDeliveryWatchLeaseError:
            self.stats.stale_lease += 1
            return CollectOutcome(
                status="lease_lost",
                submission_key=claim.submission_key,
                error_code="stale_delivery_watch_lease",
            )
        except DeliveryRecordConflictError as exc:
            return self._durable_terminal_outcome(
                claim,
                status=status,
                outcome="quarantined",
                terminal_state="quarantined",
                error_code="delivery_record_conflict",
                error_detail=str(exc),
            )
        if result.created:
            self.stats.delivery_created += 1
        else:
            self.stats.delivery_deduped += 1
        return CollectOutcome(
            status="delivery_created" if result.created else "delivery_deduped",
            submission_key=claim.submission_key,
            delivery_id=result.delivery_id,
            effect_key=result.effect_key,
            created=result.created,
        )

    def collect_batch(self) -> list[CollectOutcome]:
        self.backfill()
        outcomes: list[CollectOutcome] = []
        for _ in range(self.config.batch_size):
            outcome = self.collect_one()
            outcomes.append(outcome)
            if outcome.status in {"disabled", "idle"}:
                break
        return outcomes

    def dry_run_once(self) -> dict[str, Any]:
        rows = self.store.preview_unwatched_completed(
            limit=self.config.backfill_batch_size,
            activation_required=False,
        )
        previews: list[dict[str, Any]] = []
        for row in rows[: self.config.batch_size]:
            task_id = str(row.get("submission_key") or "")
            try:
                raw = self.status_reader(task_id)
                status = dict(raw) if isinstance(raw, Mapping) else {}
                error = ""
            except Exception as exc:
                status = {}
                error = type(exc).__name__
            previews.append({
                "submission_key": task_id,
                "business_key": row.get("business_key"),
                "generation": row.get("generation"),
                "work_item_id": row.get("work_item_id"),
                "vm_state": _status_state(status),
                "status_success": status.get("success") is True,
                "error": error or status.get("error") or "",
            })
        return {
            "ok": True,
            "dry_run": True,
            "external_writes": False,
            "candidate_count": len(rows),
            "rows": previews,
        }


class HealthReporter:
    def __init__(
        self,
        config: CollectorConfig,
        store: RcaDeliveryStore,
        *,
        remote_css_probe: Callable[..., Mapping[str, Any]] | None = None,
    ):
        self.config = config
        self.store = store
        self.started_at = _utc_iso()
        self.runtime_identity = build_runtime_identity(
            service_label=SERVICE_LABEL,
            script_path=Path(__file__),
            public_config=config.public_dict(),
            loaded_dependencies=RCA_DELIVERY_COLLECTOR_LOADED_DEPENDENCIES,
        )
        self._remote_css_probe = remote_css_probe or probe_remote_css_parser
        self._remote_css_parser_receipt: dict[str, Any] = {
            "status": "disabled",
            "observed_at": self.started_at,
        }
        self._remote_css_parser_observed_at: datetime | None = None
        self._remote_css_parser_last_probe_at: datetime | None = None
        self._remote_css_parser_error = ""
        if self.config.enabled:
            self._refresh_remote_css_parser_receipt(force=True)

    @property
    def dependencies_ready(self) -> bool:
        return not self.config.enabled or (
            not self._remote_css_parser_error
            and self._remote_css_parser_observed_at is not None
        )

    @property
    def dependency_error(self) -> str:
        return self._remote_css_parser_error

    def _refresh_remote_css_parser_receipt(self, *, force: bool = False) -> bool:
        if not self.config.enabled:
            return True
        now = _utc_now()
        if (
            not force
            and self._remote_css_parser_last_probe_at is not None
            and (now - self._remote_css_parser_last_probe_at).total_seconds()
            < DEPENDENCY_PROBE_REFRESH_SECONDS
        ):
            return self.dependencies_ready
        self._remote_css_parser_last_probe_at = now
        try:
            probe = dict(
                self._remote_css_probe(
                    self.config.ssh_mini_agent,
                    timeout_seconds=min(self.config.artifact_read_timeout_seconds, 15),
                )
            )
            expected = expected_remote_css_runtime_dependency()
            if probe != expected:
                raise ArtifactBundleReadError(
                    "html_css_parser_probe_invalid",
                    "VM CSS parser probe receipt does not match the pinned dependency",
                    permanent=True,
                )
        except (ArtifactBundleReadError, OSError, subprocess.SubprocessError) as exc:
            code = getattr(exc, "code", "html_css_parser_probe_unavailable")
            self._remote_css_parser_error = str(code)[:120]
            if self._remote_css_parser_observed_at is None:
                self._remote_css_parser_receipt = {
                    "status": "unavailable",
                    "observed_at": _utc_iso(now),
                }
            return False
        self._remote_css_parser_observed_at = now
        self._remote_css_parser_error = ""
        self._remote_css_parser_receipt = {
            **expected,
            "observed_at": _utc_iso(now),
        }
        return True

    def write(
        self,
        *,
        state: str,
        stats: CollectorStats,
        last_outcome: CollectOutcome | None = None,
        error: str = "",
        refresh_dependencies: bool = True,
    ) -> None:
        if refresh_dependencies:
            self._refresh_remote_css_parser_receipt()
        store_health = self.store.health(
            activation_required=False,
            quarantine_baseline_path=self.config.quarantine_baseline_path,
            expected_quarantine_baseline_sha256=(
                self.config.quarantine_baseline_sha256
            ),
            quarantine_release_id=self.config.quarantine_release_id,
            quarantine_bootstrap_epoch_id=(
                self.config.quarantine_bootstrap_epoch_id
            ),
            quarantine_active_release_binding_path=(
                self.config.quarantine_active_release_binding_path
            ),
            quarantine_live_env_path=self.config.quarantine_live_env_path,
        )
        dependency_error = self.dependency_error
        payload = {
            "schema_version": HEALTH_SCHEMA_VERSION,
            "state": state,
            "healthy": (
                state in {"running", "idle", "disabled"}
                and not error
                and not dependency_error
                and self.dependencies_ready
                and (
                    not self.config.enabled or store_health.get("ok") is True
                )
            ),
            "enabled": self.config.enabled,
            "external_writes": False,
            "started_at": self.started_at,
            "updated_at": _utc_iso(),
            "runtime_identity": self.runtime_identity.to_dict(),
            "config": self.config.public_dict(),
            "dependencies": {
                "remote_css_parser": dict(self._remote_css_parser_receipt),
            },
            "dependency_error": dependency_error,
            "stats": asdict(stats),
            "store": store_health,
            "last_outcome": asdict(last_outcome) if last_outcome else None,
            "error": str(error or "")[:1000],
        }
        path = self.config.health_path
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
        tmp.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        tmp.replace(path)


def run_collector_loop(
    collector: DeliveryCollector,
    *,
    once: bool = False,
    sleep: Callable[[float], None] = time.sleep,
    remote_css_probe: Callable[..., Mapping[str, Any]] | None = None,
) -> int:
    reporter = HealthReporter(
        collector.config,
        collector.store,
        remote_css_probe=remote_css_probe,
    )
    collector.runtime_identity = reporter.runtime_identity.to_dict()
    if not collector.config.enabled:
        reporter.write(state="disabled", stats=collector.stats)
        return 0
    stop = False

    def request_stop(_signum, _frame):
        nonlocal stop
        stop = True

    previous: dict[int, Any] = {}
    if not once:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous[signum] = signal.signal(signum, request_stop)
    last: CollectOutcome | None = None
    try:
        while not stop:
            if not reporter._refresh_remote_css_parser_receipt():
                reporter.write(
                    state="error",
                    stats=collector.stats,
                    last_outcome=last,
                    error=reporter.dependency_error,
                    refresh_dependencies=False,
                )
                if once:
                    return 2
                sleep(collector.config.poll_interval_seconds)
                continue
            try:
                reporter.write(
                    state="running",
                    stats=collector.stats,
                    last_outcome=last,
                    refresh_dependencies=False,
                )
                with _PeriodicHeartbeat(
                    lambda: reporter.write(
                        state="running",
                        stats=collector.stats,
                        last_outcome=last,
                        refresh_dependencies=False,
                    ),
                    interval_seconds=_heartbeat_interval_seconds(
                        collector.config.health_max_age_seconds
                    ),
                ):
                    outcomes = collector.collect_batch()
                last = outcomes[-1] if outcomes else None
                reporter.write(
                    state="idle" if last and last.status == "idle" else "running",
                    stats=collector.stats,
                    last_outcome=last,
                )
            except Exception as exc:
                reporter.write(
                    state="error",
                    stats=collector.stats,
                    last_outcome=last,
                    error=f"{type(exc).__name__}: {exc}",
                )
                if once:
                    return 2
            if once:
                return 0
            sleep(collector.config.poll_interval_seconds)
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
    reporter.write(state="stopped", stats=collector.stats, last_outcome=last)
    return 0


def read_health(
    path: Path,
    *,
    max_age_seconds: int,
    now: datetime | None = None,
) -> tuple[bool, dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return False, {"error": f"health_unreadable: {type(exc).__name__}"}
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != HEALTH_SCHEMA_VERSION
    ):
        return False, {"error": "health_schema_invalid"}
    config = payload.get("config")
    if not isinstance(config, Mapping) or not runtime_identity_is_valid(
        payload.get("runtime_identity"),
        service_label=SERVICE_LABEL,
        public_config=config,
    ):
        return False, {**payload, "error": "health_runtime_identity_invalid"}
    try:
        updated = datetime.fromisoformat(
            str(payload.get("updated_at") or "").replace("Z", "+00:00")
        )
        if updated.tzinfo is None or updated.utcoffset() is None:
            raise ValueError("health timestamp must be timezone-aware")
        observed_at = now or _utc_now()
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("health observation timestamp must be timezone-aware")
        age = (
            observed_at.astimezone(timezone.utc) - updated.astimezone(timezone.utc)
        ).total_seconds()
    except (TypeError, ValueError):
        return False, {**payload, "error": "health_timestamp_invalid"}
    fresh = -MAX_HEALTH_FUTURE_SKEW_SECONDS <= age <= max_age_seconds
    result = {**payload, "age_seconds": age}
    if age < -MAX_HEALTH_FUTURE_SKEW_SECONDS:
        result["error"] = "heartbeat_from_future"
    elif age > max_age_seconds:
        result["error"] = "heartbeat_stale"
    return payload.get("healthy") is True and fresh, result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Collect RCA VM terminal truth into a durable delivery outbox"
    )
    parser.add_argument("--check-config", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--health", action="store_true")
    parser.add_argument("--health-max-age-seconds", type=int)
    parser.add_argument("--check-config-worker-root")
    return parser


def load_collector_environment(env_file: str | Path | None = None) -> Path:
    path = Path(
        env_file
        or os.environ.get(f"{ENV_PREFIX}ENV_FILE")
        or Path(get_hermes_home()) / ".env"
    ).expanduser()
    load_dotenv(path, override=False, interpolate=False)
    return path


def main(argv: list[str] | None = None) -> int:
    load_collector_environment()
    args = _parser().parse_args(argv)
    try:
        config = CollectorConfig.from_env()
    except ValueError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
        return 2
    if args.check_config:
        quarantine_baseline = (
            read_quarantine_baseline_status(
                config.control_db_path,
                baseline_path=config.quarantine_baseline_path,
                expected_sha256=config.quarantine_baseline_sha256,
                expected_release_id=config.quarantine_release_id,
                bootstrap_epoch_id=config.quarantine_bootstrap_epoch_id,
                active_release_binding_path=(
                    config.quarantine_active_release_binding_path
                ),
                live_env_path=config.quarantine_live_env_path,
            )
            if config.enabled
            else disabled_quarantine_baseline_status(
                baseline_path=config.quarantine_baseline_path,
                expected_sha256=config.quarantine_baseline_sha256,
            )
        )
        try:
            remote_css_parser = probe_remote_css_parser(
                config.ssh_mini_agent,
                timeout_seconds=min(config.artifact_read_timeout_seconds, 15),
                worker_root=args.check_config_worker_root,
            )
        except ArtifactBundleReadError as exc:
            print(
                json.dumps(
                    {"ok": False, "error": exc.code, "detail": exc.detail},
                    ensure_ascii=False,
                )
            )
            return 2
        print(
            json.dumps(
                {
                    "ok": quarantine_baseline["ready"],
                    "config": config.public_dict(),
                    "dependencies": {"remote_css_parser": remote_css_parser},
                    "quarantine_baseline": quarantine_baseline,
                },
                ensure_ascii=False,
            )
        )
        return 0 if quarantine_baseline["ready"] else 2
    if args.health:
        healthy, payload = read_health(
            config.health_path,
            max_age_seconds=(
                args.health_max_age_seconds or config.health_max_age_seconds
            ),
        )
        print(json.dumps(payload, ensure_ascii=False))
        return 0 if healthy else 2
    try:
        store = RcaDeliveryStore(config.control_db_path, require_current=True)
    except (OSError, RuntimeError, sqlite3.Error) as exc:
        print(
            json.dumps(
                {"ok": False, "error": f"delivery_store_unavailable: {exc}"},
                ensure_ascii=False,
            )
        )
        return 2
    collector = DeliveryCollector(store=store, config=config)
    if args.dry_run:
        print(json.dumps(collector.dry_run_once(), ensure_ascii=False))
        return 0
    return run_collector_loop(collector, once=args.once)


if __name__ == "__main__":
    raise SystemExit(main())
