#!/usr/bin/env python3
"""Offline-only preflight for the independent direct RCA shadow consumer.

This command validates a direct consumer configuration and emits a plan.  It
does not construct a Kafka consumer, instantiate ``MiniStore`` (which creates
files), start a dispatcher, or write a health/receipt file.  An existing
MiniStore is inspected through a read-only, immutable SQLite connection only.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
import json
import os
from pathlib import Path
import sqlite3
import stat
import sys
from typing import Any, Final
from urllib.parse import quote

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gateway.pnc_rca_kafka_contract import FIXED_KAFKA_GROUP_ID
from gateway.pnc_rca_mini_store import (
    MINI_STORE_SCHEMA_VERSION,
    SCHEMA_COLUMNS,
    SCHEMA_TABLES,
)
from scripts import pnc_rca_kafka_direct_consumer as direct


PREFLIGHT_SCHEMA_VERSION: Final = "pnc_rca_direct_shadow_preflight_v1"
MODE: Final = "offline_direct_shadow_preflight"

# ``rca_direct_path`` is the stable direct path's historical default.  It is
# intentionally forbidden for a new shadow process even though the lower-level
# direct config accepts it for a committing consumer.
STABLE_PROD_DIRECT_GROUP_ID: Final = direct.DIRECT_DEFAULT_GROUP_ID
STABLE_PROD_DIRECT_GROUP_IDS: Final = frozenset({
    STABLE_PROD_DIRECT_GROUP_ID,
    "rca_direct_prod",
    "rca_direct_production",
    "rca_direct_stable",
})
OLD_PROD_GROUP_ID: Final = FIXED_KAFKA_GROUP_ID
FORBIDDEN_GROUP_IDS: Final = frozenset({
    OLD_PROD_GROUP_ID,
    *STABLE_PROD_DIRECT_GROUP_IDS,
})

_DIRECT_PREFIXES: Final = tuple(direct.DIRECT_ENV_PREFIX_ALIASES)
_T0_SUFFIXES: Final = ("T0_OFFSETS_JSON", "INITIAL_OFFSETS_JSON", "START_OFFSETS_JSON")
_AUTO_OFFSET_SUFFIXES: Final = ("AUTO_OFFSET_RESET",)
_DISPATCHER_ENABLED_SUFFIXES: Final = (
    "DISPATCHER_ENABLED",
    "OUTBOX_DISPATCHER_ENABLED",
    "DELIVERY_DISPATCHER_ENABLED",
)
_DISPATCHER_DISABLED_SUFFIXES: Final = (
    "DISPATCHER_DISABLED",
    "OUTBOX_DISPATCHER_DISABLED",
    "DELIVERY_DISPATCHER_DISABLED",
)
_SUBMIT_ENABLED_SUFFIXES: Final = (
    "SUBMIT_ENABLED",
    "OUTBOX_SUBMIT_ENABLED",
    "DELIVERY_SUBMIT_ENABLED",
    "VM_SUBMIT_ENABLED",
)
_SUBMIT_DISABLED_SUFFIXES: Final = (
    "SUBMIT_DISABLED",
    "OUTBOX_SUBMIT_DISABLED",
    "DELIVERY_SUBMIT_DISABLED",
    "VM_SUBMIT_DISABLED",
)
_PROD_PATH_ENV_NAMES: Final = (
    "HERMES_RCA_KAFKA_CONTROL_DB_PATH",
    "HERMES_RCA_KAFKA_HEALTH_PATH",
    "HERMES_RCA_DIRECT_KAFKA_STABLE_DB_PATH",
    "HERMES_RCA_DIRECT_KAFKA_STABLE_HEALTH_PATH",
    "HERMES_RCA_DIRECT_STABLE_DB_PATH",
    "HERMES_RCA_DIRECT_STABLE_HEALTH_PATH",
)
_SECRET_KEY_MARKERS: Final = (
    "PASSWORD",
    "SECRET",
    "TOKEN",
    "PRIVATE_KEY",
    "AUTHORIZATION",
)


class ShadowPreflightError(ValueError):
    """Raised by the assertion helper for a failed preflight plan."""


def _absolute(path: str | Path) -> Path:
    """Make a comparison-safe path without resolving symlinks or creating it."""

    value = Path(path).expanduser()
    return Path(os.path.abspath(value))


def _symlinked_existing_component(path: Path) -> Path | None:
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        try:
            observed = current.lstat()
        except FileNotFoundError:
            break
        except OSError as exc:
            raise ShadowPreflightError("path_component_stat_failed") from exc
        if stat.S_ISLNK(observed.st_mode):
            return current
    return None


def _resolved(path: Path) -> Path:
    try:
        return path.resolve(strict=False)
    except OSError as exc:
        raise ShadowPreflightError("path_resolution_failed") from exc


def _env_file_observation(
    path: str | Path,
) -> tuple[Path, dict[str, Any], tuple[int, ...] | None, list[str]]:
    """Inspect an explicit dotenv path without opening or following it."""

    selected = _absolute(path)
    observation: dict[str, Any] = {
        "path": str(selected),
        "contents_redacted": True,
        "safe": False,
    }
    try:
        observed = selected.lstat()
    except FileNotFoundError:
        return selected, observation, None, ["env_file_missing"]
    except OSError:
        return selected, observation, None, ["env_file_stat_failed"]

    mode = stat.S_IMODE(observed.st_mode)
    regular = stat.S_ISREG(observed.st_mode)
    symlink = stat.S_ISLNK(observed.st_mode)
    observation.update({
        "device": int(observed.st_dev),
        "inode": int(observed.st_ino),
        "size": int(observed.st_size),
        "mtime_ns": int(observed.st_mtime_ns),
        "owner_uid": int(observed.st_uid),
        "link_count": int(observed.st_nlink),
        "mode": f"{mode:04o}",
        "regular_file": regular,
        "symlink": symlink,
    })
    errors: list[str] = []
    if _symlinked_existing_component(selected) is not None:
        errors.append("env_file_parent_symlink_forbidden")
    if not regular or symlink:
        errors.append("env_file_regular_no_symlink_required")
    if int(observed.st_uid) != os.geteuid():
        errors.append("env_file_owner_invalid")
    if int(observed.st_nlink) != 1:
        errors.append("env_file_single_link_required")
    if mode != 0o600:
        errors.append("env_file_mode_must_be_0600")
    observation["safe"] = not errors
    identity = (
        int(observed.st_dev),
        int(observed.st_ino),
        int(observed.st_size),
        int(observed.st_mtime_ns),
        int(observed.st_ctime_ns),
        int(observed.st_uid),
        int(observed.st_nlink),
        int(observed.st_mode),
    )
    return selected, observation, identity, errors


def _source_value(
    source: Mapping[str, Any], suffixes: Sequence[str]
) -> tuple[str, str] | None:
    for suffix in suffixes:
        for prefix in _DIRECT_PREFIXES:
            key = f"{prefix}{suffix}"
            if key in source and str(source[key]).strip() != "":
                return key, str(source[key]).strip()
    return None


def _all_source_values(
    source: Mapping[str, Any], suffixes: Sequence[str]
) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    for suffix in suffixes:
        for prefix in _DIRECT_PREFIXES:
            key = f"{prefix}{suffix}"
            if key in source and str(source[key]).strip() != "":
                values.append((key, str(source[key]).strip()))
    return values


def _strict_bool(raw: str, *, field: str) -> bool:
    value = str(raw).strip().lower()
    if value not in {"true", "false"}:
        raise ShadowPreflightError(f"{field}_must_be_exactly_true_or_false")
    return value == "true"


def _redact_text(value: Any, source: Mapping[str, Any]) -> str:
    text = str(value)
    for key, raw in source.items():
        if any(marker in str(key).upper() for marker in _SECRET_KEY_MARKERS):
            secret = str(raw or "")
            if secret:
                text = text.replace(secret, "<redacted>")
    return text[:240]


def _safe_public_config(config: Any) -> dict[str, Any]:
    """Use the direct config's public contract and remove future secret fields."""

    public = dict(config.public_dict())
    for key in tuple(public):
        if any(marker.lower() in key.lower() for marker in _SECRET_KEY_MARKERS):
            public[key] = "<redacted>"
    return public


def _new_meta_and_empty_store(path: Path) -> dict[str, Any]:
    """Audit a pre-existing SQLite file without opening it read-write."""

    try:
        stat_result = path.lstat()
    except OSError as exc:
        raise ShadowPreflightError("mini_store_stat_failed") from exc
    if not path.is_file() or path.is_symlink():
        raise ShadowPreflightError("mini_store_regular_file_required")
    if int(stat_result.st_nlink) != 1:
        raise ShadowPreflightError("mini_store_single_link_required")
    if int(stat_result.st_size) == 0:
        return {"state": "empty_file", "schema_version": None, "row_counts": {}}
    for sidecar in (
        Path(f"{path}-wal"),
        Path(f"{path}-shm"),
        Path(f"{path}-journal"),
    ):
        if sidecar.exists() or sidecar.is_symlink():
            raise ShadowPreflightError(f"mini_store_sidecar_present:{sidecar.name}")

    # A URI with both mode=ro and immutable=1 cannot create a database, WAL,
    # journal, or schema side effect.  ``quote`` preserves spaces and unicode.
    uri = f"file:{quote(str(path), safe='/:')}?mode=ro&immutable=1"
    before = (
        int(stat_result.st_ino),
        int(stat_result.st_size),
        int(stat_result.st_mtime_ns),
    )
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=1)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        expected_tables = set(SCHEMA_TABLES)
        if tables != expected_tables:
            raise ShadowPreflightError("mini_store_schema_tables_invalid")
        for table in SCHEMA_TABLES:
            columns = tuple(
                str(row[1])
                for row in connection.execute(f'PRAGMA table_info("{table}")')
            )
            if columns != tuple(SCHEMA_COLUMNS[table]):
                raise ShadowPreflightError(f"mini_store_schema_columns_invalid:{table}")

        meta_rows = {
            str(row[0]): str(row[1])
            for row in connection.execute(
                "SELECT key, value FROM mini_store_meta ORDER BY key"
            )
        }
        if set(meta_rows) != {"created_at", "schema_version"}:
            raise ShadowPreflightError("mini_store_meta_keys_invalid")
        if meta_rows.get("schema_version") != MINI_STORE_SCHEMA_VERSION:
            raise ShadowPreflightError("mini_store_schema_version_invalid")
        if not meta_rows.get("created_at", "").strip():
            raise ShadowPreflightError("mini_store_created_at_invalid")

        row_counts: dict[str, int] = {}
        for table in SCHEMA_TABLES:
            if table == "mini_store_meta":
                continue
            count = int(
                connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            )
            row_counts[table] = count
            if count:
                raise ShadowPreflightError(f"mini_store_not_fresh:{table}")
    except ShadowPreflightError:
        raise
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        raise ShadowPreflightError("mini_store_readonly_audit_failed") from exc
    finally:
        if connection is not None:
            connection.close()
    try:
        after_stat = path.lstat()
    except OSError as exc:
        raise ShadowPreflightError("mini_store_changed_during_audit") from exc
    after = (
        int(after_stat.st_ino),
        int(after_stat.st_size),
        int(after_stat.st_mtime_ns),
    )
    if before != after:
        raise ShadowPreflightError("mini_store_changed_during_audit")
    return {
        "state": "new_v2",
        "schema_version": MINI_STORE_SCHEMA_VERSION,
        "row_counts": row_counts,
    }


def inspect_mini_store_path(path: str | Path) -> dict[str, Any]:
    """Return a side-effect-free state summary for a candidate MiniStore path."""

    selected = _absolute(path)
    try:
        selected.lstat()
    except FileNotFoundError:
        return {"path": str(selected), "state": "absent", "schema_version": None}
    except OSError as exc:
        raise ShadowPreflightError("mini_store_stat_failed") from exc
    result = _new_meta_and_empty_store(selected)
    return {"path": str(selected), **result}


def _stable_paths(home: Path, source: Mapping[str, Any]) -> set[Path]:
    runtime = home / "runtime" / "pnc_agent"
    paths = {
        runtime / "feishu_issue_kafka" / "control.sqlite3",
        runtime / "feishu_issue_kafka" / "health.json",
        runtime / "feishu_issue_kafka_rca" / "control.sqlite3",
        runtime / "feishu_issue_kafka_rca" / "health.json",
        runtime / "feishu_issue_kafka_rca_direct" / "mini.sqlite3",
        runtime / "feishu_issue_kafka_rca_direct" / "health.json",
    }
    for name in _PROD_PATH_ENV_NAMES:
        raw = str(source.get(name, "")).strip()
        if raw:
            paths.add(_absolute(raw))
    return {_absolute(path) for path in paths}


def _path_contract(
    config: Any, source: Mapping[str, Any], *, hermes_home: str | Path | None
) -> tuple[dict[str, Any], list[str]]:
    home = _absolute(hermes_home or Path.home() / ".hermes")
    db_path = _absolute(config.db_path)
    health_path = _absolute(config.health_path)
    errors: list[str] = []
    stable_paths = _stable_paths(home, source)
    resolved_stable_paths = {_resolved(path) for path in stable_paths}
    resolved_stable_roots = {_resolved(path.parent) for path in stable_paths}
    if db_path == health_path:
        errors.append("db_and_health_paths_must_differ")
    if db_path in stable_paths:
        errors.append("mini_store_path_is_production_path")
    if health_path in stable_paths:
        errors.append("health_path_is_production_path")
    for label, path in (("mini_store", db_path), ("health", health_path)):
        if not path.is_absolute():
            errors.append(f"{label}_path_must_be_absolute")
        try:
            symlinked_component = _symlinked_existing_component(path)
        except ShadowPreflightError:
            errors.append(f"{label}_path_component_stat_failed")
            continue
        if symlinked_component is not None:
            errors.append(f"{label}_parent_symlink_forbidden")
            continue
        resolved_path = _resolved(path)
        if resolved_path in resolved_stable_paths or any(
            resolved_path == root or root in resolved_path.parents
            for root in resolved_stable_roots
        ):
            errors.append(f"{label}_path_is_production_root")
        try:
            observed = path.lstat()
        except FileNotFoundError:
            continue
        except OSError:
            errors.append(f"{label}_path_stat_failed")
            continue
        if path.is_symlink():
            errors.append(f"{label}_path_symlink_forbidden")
        elif label == "health" and not path.is_file():
            errors.append("health_path_regular_file_or_absent_required")
        elif label == "mini_store" and not path.is_file():
            errors.append("mini_store_regular_file_or_absent_required")
        if int(observed.st_nlink) != 1:
            errors.append(f"{label}_path_single_link_required")
    return {
        "mini_store_path": str(db_path),
        "health_path": str(health_path),
        "production_paths_rejected": sorted(str(path) for path in stable_paths),
    }, errors


def _group_contract(
    config: Any, source: Mapping[str, Any]
) -> tuple[dict[str, Any], list[str]]:
    group_id = str(config.group_id).strip()
    forbidden = set(FORBIDDEN_GROUP_IDS)
    # If a live/prod config is supplied in the process environment, treating
    # its group as forbidden prevents an accidental shadow/prod collision.
    for key in ("HERMES_RCA_KAFKA_GROUP", "HERMES_RCA_KAFKA_GROUP_ID"):
        value = str(source.get(key, "")).strip()
        if value:
            forbidden.add(value)
    for suffix in (
        "STABLE_GROUP_ID",
        "STABLE_PROD_GROUP_ID",
        "PROD_GROUP_ID",
        "PROD_DIRECT_GROUP_ID",
    ):
        for _key, value in _all_source_values(source, (suffix,)):
            forbidden.add(value)
    lowered = group_id.lower()
    looks_production = ("prod" in lowered or "production" in lowered) and (
        "direct" in lowered or "rca" in lowered
    )
    errors: list[str] = []
    if not group_id:
        errors.append("shadow_group_required")
    if group_id in forbidden or group_id.casefold() in {
        item.casefold() for item in forbidden
    }:
        errors.append("shadow_group_must_not_match_production_group")
    if looks_production:
        errors.append("shadow_group_production_name_forbidden")
    return {
        "group_id": group_id,
        "independent": not errors,
        "forbidden_match": group_id in forbidden,
    }, errors


def _disabled_contract(
    source: Mapping[str, Any],
    enabled_suffixes: Sequence[str],
    disabled_suffixes: Sequence[str],
    name: str,
) -> tuple[dict[str, Any], list[str]]:
    observations: list[tuple[str, bool]] = []
    for key, raw in _all_source_values(source, enabled_suffixes):
        try:
            observations.append((key, _strict_bool(raw, field=key)))
        except ShadowPreflightError as exc:
            return {"declared": False, "enabled": None}, [str(exc)]
    for key, raw in _all_source_values(source, disabled_suffixes):
        try:
            observations.append((key, not _strict_bool(raw, field=key)))
        except ShadowPreflightError as exc:
            return {"declared": False, "enabled": None}, [str(exc)]
    errors: list[str] = []
    if not observations:
        errors.append(f"{name}_disabled_declaration_missing")
    values = {value for _key, value in observations}
    if values != {False}:
        errors.append(f"{name}_must_be_disabled")
    return {
        "declared": bool(observations) and not errors,
        "enabled": False if observations and not errors else None,
        "source_keys": [key for key, _value in observations],
    }, errors


def _explicit_t0(
    source: Mapping[str, Any], config: Any
) -> tuple[dict[str, Any], list[str]]:
    values = _all_source_values(source, _T0_SUFFIXES)
    if not values:
        return {"declared": False, "offsets": {}}, ["explicit_t0_required"]
    if any(not raw.strip() for _key, raw in values):
        return {"declared": False, "offsets": {}}, ["explicit_t0_must_not_be_empty"]
    # DirectKafkaConfig already performs strict JSON and integer validation.
    offsets = {
        str(key): int(value) for key, value in sorted(config.initial_offsets.items())
    }
    if not offsets:
        return {"declared": False, "offsets": {}}, ["explicit_t0_required"]
    if len(values) > 1:
        errors: list[str] = []
        first = values[0][1]
        if any(raw != first for _key, raw in values[1:]):
            errors.append("t0_aliases_conflict")
        if errors:
            return {"declared": False, "offsets": offsets}, errors
    return {
        "declared": True,
        "offsets": offsets,
        "source_keys": [key for key, _raw in values],
    }, []


def build_preflight_plan(
    env: Mapping[str, Any] | None = None,
    *,
    env_file: str | Path | None = None,
    hermes_home: str | Path | None = None,
) -> dict[str, Any]:
    """Build a redacted, offline shadow plan from direct env/config."""

    source: dict[str, str] = {}
    env_path: Path | None = None
    env_file_check: dict[str, Any] | None = None
    errors: list[str] = []
    config: Any = None
    if env is not None and env_file is None:
        source = {
            str(key): str(value) for key, value in env.items() if value is not None
        }
    else:
        selected_env_file: Path | None = None
        env_file_identity: tuple[int, ...] | None = None
        seed_source = {
            str(key): str(value)
            for key, value in (os.environ if env is None else env).items()
            if value is not None
        }
        configured_env_file = env_file
        if configured_env_file is None:
            env_file_value = _source_value(seed_source, ("ENV_FILE",))
            if env_file_value is not None:
                configured_env_file = env_file_value[1]
        if configured_env_file is not None:
            (
                selected_env_file,
                env_file_check,
                env_file_identity,
                env_file_errors,
            ) = _env_file_observation(configured_env_file)
            errors.extend(env_file_errors)
        if not errors:
            try:
                source, env_path = direct.load_direct_environment(
                    selected_env_file,
                    environ=env,
                )
            except Exception as exc:
                errors.append(_redact_text(type(exc).__name__, env or {}))
        if selected_env_file is not None and not errors:
            _, after_check, after_identity, after_errors = _env_file_observation(
                selected_env_file
            )
            if after_errors or after_identity != env_file_identity:
                errors.append("env_file_changed_during_read")
            else:
                env_file_check = after_check
    if not errors:
        try:
            config = direct.DirectKafkaConfig.from_env(source, hermes_home=hermes_home)
        except Exception as exc:
            errors.append(_redact_text(str(exc), source))

    checks: dict[str, Any] = {}
    public_config: dict[str, Any] | None = None
    store_state: dict[str, Any] | None = None
    if config is not None:
        public_config = _safe_public_config(config)
        group_check, group_errors = _group_contract(config, source)
        checks["shadow_group"] = group_check
        errors.extend(group_errors)

        commit_ok = config.commit_enabled is False
        checks["commit_enabled"] = {
            "value": bool(config.commit_enabled),
            "ok": commit_ok,
        }
        if not commit_ok:
            errors.append("commit_enabled_must_be_false")

        auto_value = _source_value(source, _AUTO_OFFSET_SUFFIXES)
        auto_reset = str(auto_value[1] if auto_value else "none").strip()
        auto_ok = auto_reset == "none"
        checks["auto_offset_reset"] = {
            "value": auto_reset,
            "ok": auto_ok,
            "declared": auto_value is not None,
        }
        if auto_value is None:
            errors.append("auto_offset_reset_declaration_missing")
        elif not auto_ok:
            errors.append("auto_offset_reset_must_be_none")

        t0_check, t0_errors = _explicit_t0(source, config)
        checks["t0"] = t0_check
        errors.extend(t0_errors)

        dispatcher_check, dispatcher_errors = _disabled_contract(
            source,
            _DISPATCHER_ENABLED_SUFFIXES,
            _DISPATCHER_DISABLED_SUFFIXES,
            "dispatcher",
        )
        submit_check, submit_errors = _disabled_contract(
            source,
            _SUBMIT_ENABLED_SUFFIXES,
            _SUBMIT_DISABLED_SUFFIXES,
            "submit",
        )
        checks["dispatcher"] = dispatcher_check
        checks["submit"] = submit_check
        errors.extend(dispatcher_errors)
        errors.extend(submit_errors)

        path_check, path_errors = _path_contract(
            config,
            source,
            hermes_home=hermes_home,
        )
        checks["paths"] = path_check
        errors.extend(path_errors)
        # Do not inspect a path that has already been identified as a known
        # production path or a non-regular object.
        unsafe_mini_store_errors = {
            "mini_store_parent_symlink_forbidden",
            "mini_store_path_component_stat_failed",
            "mini_store_path_is_production_path",
            "mini_store_path_is_production_root",
            "mini_store_path_stat_failed",
            "mini_store_path_symlink_forbidden",
            "mini_store_regular_file_or_absent_required",
        }
        if not any(code in unsafe_mini_store_errors for code in path_errors):
            try:
                store_state = inspect_mini_store_path(config.db_path)
            except ShadowPreflightError as exc:
                errors.append(str(exc))
        if store_state is not None:
            checks["mini_store"] = store_state

    unique_errors = list(dict.fromkeys(error for error in errors if error))
    plan: dict[str, Any] = {
        "schema_version": PREFLIGHT_SCHEMA_VERSION,
        "mode": MODE,
        "ok": not unique_errors,
        "status": "ready" if not unique_errors else "blocked",
        "redacted": True,
        "offline_only": True,
        "validation_scope": "direct_config_and_fresh_shadow_paths_only",
        "side_effects": {
            "kafka_opened": False,
            "db_created": False,
            "files_written": False,
            "live_touched": False,
            "launchagent_changed": False,
            "dispatcher_started": False,
            "submit_performed": False,
        },
        "config": public_config,
        "checks": checks,
        "errors": unique_errors,
        "plan": {
            "consumer": "direct_shadow_not_started",
            "group_id": checks.get("shadow_group", {}).get("group_id"),
            "commit_enabled": False,
            "auto_offset_reset": "none",
            "t0_offsets": checks.get("t0", {}).get("offsets", {}),
            "dispatcher_enabled": False,
            "submit_enabled": False,
        },
    }
    if env_file_check is not None:
        plan["env_file"] = env_file_check
    elif env_path is not None:
        plan["env_file"] = {
            "path": str(env_path),
            "contents_redacted": True,
            "safe": False,
        }
    return plan


# Friendly aliases for embedding callers and older preflight naming.
build_shadow_preflight = build_preflight_plan
preflight = build_preflight_plan


def assert_preflight(plan: Mapping[str, Any]) -> Mapping[str, Any]:
    if plan.get("ok") is not True:
        raise ShadowPreflightError(
            "direct_shadow_preflight_failed:"
            + ",".join(str(item) for item in plan.get("errors", ()))
        )
    return plan


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env-file", "--config", dest="env_file", help="direct-only dotenv config"
    )
    parser.add_argument(
        "--hermes-home", help="Hermes home used only for default/path comparisons"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    try:
        payload = build_preflight_plan(
            env_file=args.env_file,
            hermes_home=args.hermes_home,
        )
    except Exception as exc:  # pragma: no cover - final CLI boundary
        payload = {
            "schema_version": PREFLIGHT_SCHEMA_VERSION,
            "mode": MODE,
            "ok": False,
            "offline_only": True,
            "errors": [type(exc).__name__],
            "side_effects": {
                "kafka_opened": False,
                "db_created": False,
                "files_written": False,
                "live_touched": False,
                "launchagent_changed": False,
            },
        }
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if payload.get("ok") is True else 2


__all__ = [
    "FORBIDDEN_GROUP_IDS",
    "MODE",
    "OLD_PROD_GROUP_ID",
    "PREFLIGHT_SCHEMA_VERSION",
    "STABLE_PROD_DIRECT_GROUP_ID",
    "ShadowPreflightError",
    "assert_preflight",
    "build_arg_parser",
    "build_preflight_plan",
    "build_shadow_preflight",
    "inspect_mini_store_path",
    "main",
    "preflight",
]


if __name__ == "__main__":
    raise SystemExit(main())
