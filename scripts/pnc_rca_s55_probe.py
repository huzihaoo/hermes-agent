#!/usr/bin/env python3
"""Run the current-chain S5.5 RCA production-preflight smoke gate."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
from typing import Any, Callable, Mapping, Sequence
from urllib import error as urlerror
from urllib import request as urlrequest


REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gateway.pnc_rca_release_authority import (  # noqa: E402
    ReleaseAuthorityError,
    audit_release_projections,
    canonical_json_sha256,
)
from scripts import pnc_rca_activation_capsule as capsules  # noqa: E402
from scripts.pnc_rca_activation_gate import (  # noqa: E402
    ActivationGateError,
    _load_authority,
    _load_environment,
    _public_config,
    _read_owner_json,
    _validate_baseline,
    _validate_safe_config,
)
from scripts.pnc_rca_schema_fingerprint import (  # noqa: E402
    SchemaFingerprintError,
    verify_snapshot_receipt,
)


RECEIPT_SCHEMA_VERSION = "pnc_rca_s55_probe_receipt_v1"
STAGE_SCHEMA_VERSION = "pnc_rca_s55_stage_result_v1"
CLI_SCHEMA_VERSION = "pnc_rca_s55_probe_cli_v1"
VM_SCHEMA_VERSION = "pnc_rca_vm_s55_observation_v1"
REPORT_HEALTH_SCHEMA_VERSION = "pnc_rca_report_service_health_v1"
STAGE_NAMES = ("authority", "activation", "delivery", "vm", "report")
MAX_STAGE_OUTPUT_BYTES = 2 * 1024 * 1024
MAX_HTTP_BYTES = 1024 * 1024
FRESHNESS_SECONDS = 900
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class S55Error(RuntimeError):
    """Stable S5.5 failure with an explicit measurement classification."""

    def __init__(self, code: str, detail: str = "", *, result: str = "failed"):
        self.code = str(code or "pnc_rca_s55_invalid")[:160]
        self.detail = str(detail or self.code)[:1000]
        self.result = result if result in {"failed", "not_measured"} else "failed"
        super().__init__(self.code)


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _fresh(value: Any, *, now: datetime, code: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise S55Error(code)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise S55Error(code) from exc
    if parsed.tzinfo is None:
        raise S55Error(code)
    age = (now - parsed.astimezone(timezone.utc)).total_seconds()
    if age < -30 or age > FRESHNESS_SECONDS:
        raise S55Error(code)
    return parsed.astimezone(timezone.utc).isoformat()


def _pass(stage: str, detail: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": STAGE_SCHEMA_VERSION,
        "stage": stage,
        "result": "passed",
        "reason": "",
        "detail": dict(detail),
        "production_mutation_performed": False,
        "external_effects_triggered": False,
    }


def _stage_authority(args: argparse.Namespace, *, now: datetime) -> dict[str, Any]:
    _authority_raw, authority, authority_digest = _load_authority(args.authority)
    _pointer_raw, pointer = _read_owner_json(args.pointer, "s55_pointer")
    _manifest_raw, manifest = _read_owner_json(args.live_manifest, "s55_manifest")
    _binding_raw, binding = _read_owner_json(args.active_binding, "s55_binding")
    try:
        audit = audit_release_projections(
            authority,
            pointer=pointer,
            authority_path=args.authority,
            live_manifest=manifest,
            active_binding=binding,
            control_store_path=args.control_db,
            now=now,
        )
    except ReleaseAuthorityError as exc:
        raise S55Error(exc.code, exc.detail) from exc
    if audit.get("ok") is not True:
        code = (
            audit.get("errors", [{}])[0].get("code")
            if isinstance(audit.get("errors"), list) and audit.get("errors")
            else "pnc_rca_s55_authority_projection_failed"
        )
        raise S55Error(str(code))
    try:
        schema = verify_snapshot_receipt(args.schema_receipt)
    except SchemaFingerprintError as exc:
        raise S55Error(exc.code, exc.detail) from exc
    if (
        schema.get("schema_fingerprint_sha256")
        != authority["control_store"]["schema_fingerprint_sha256"]
        or schema.get("receipt_raw_sha256")
        != authority["control_store"]["backup_receipt_sha256"]
    ):
        raise S55Error("pnc_rca_s55_schema_authority_mismatch")
    return _pass("authority", {
        "release_id": authority["release_id"],
        "authority_sha256": authority_digest,
        "projection_audit_sha256": canonical_json_sha256(audit),
        "schema_fingerprint_sha256": schema["schema_fingerprint_sha256"],
        "schema_object_count": schema["object_count"],
    })


def _activation_cli_status(
    authority: Mapping[str, Any],
    control_db: Path,
    *,
    runner: Callable[..., Any] = subprocess.run,
) -> tuple[int, bytes, bytes, dict[str, Any]]:
    root = Path(authority["faces"]["host_runtime"]["root"])
    script = root / "scripts" / "pnc_rca_activation.py"
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(root)
    try:
        completed = runner(
            [
                sys.executable,
                str(script),
                "--control-db",
                str(control_db),
                "status",
            ],
            cwd=root,
            env=environment,
            capture_output=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise S55Error(
            "pnc_rca_s55_activation_status_not_measured",
            type(exc).__name__,
            result="not_measured",
        ) from exc
    stdout = bytes(completed.stdout or b"")
    stderr = bytes(completed.stderr or b"")
    if len(stdout) > MAX_STAGE_OUTPUT_BYTES or len(stderr) > MAX_STAGE_OUTPUT_BYTES:
        raise S55Error("pnc_rca_s55_activation_status_output_invalid")
    try:
        body = json.loads(stdout.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise S55Error("pnc_rca_s55_activation_status_output_invalid") from exc
    if not isinstance(body, dict):
        raise S55Error("pnc_rca_s55_activation_status_output_invalid")
    return int(completed.returncode), stdout, stderr, body


def _stage_activation(args: argparse.Namespace, *, now: datetime) -> dict[str, Any]:
    del now
    _raw, authority, _digest = _load_authority(args.authority)
    exit_code, stdout, stderr, body = _activation_cli_status(
        authority, args.control_db
    )
    result = body.get("result")
    activation = result.get("activation") if isinstance(result, Mapping) else None
    backlog = activation.get("backlog") if isinstance(activation, Mapping) else None
    current = activation.get("current_epoch") if isinstance(activation, Mapping) else None
    if (
        exit_code != 0
        or body.get("ok") is not True
        or not isinstance(activation, Mapping)
        or activation.get("configured") is not True
        or not isinstance(current, Mapping)
        or current.get("epoch_id") != args.expected_epoch_id
        or current.get("state") != args.expected_activation_state
        or not isinstance(backlog, Mapping)
        or backlog.get("historical_held") != args.expected_historical_hold
        or backlog.get("historical_blocked") != 0
        or backlog.get("pending_inbox") != 0
        or backlog.get("unbound_ledger") != 0
        or backlog.get("historical_unbound_ledger") != 0
    ):
        raise S55Error("pnc_rca_s55_activation_state_failed")
    return _pass("activation", {
        "nested_exit_code": exit_code,
        "nested_stdout_sha256": _sha(stdout),
        "nested_stderr_sha256": _sha(stderr),
        "epoch_id": current["epoch_id"],
        "state": current["state"],
        "historical_held": backlog["historical_held"],
        "historical_blocked": backlog["historical_blocked"],
        "pending_inbox": backlog["pending_inbox"],
        "production_active": activation.get("production_active"),
    })


def _status_counts(connection: sqlite3.Connection, table: str) -> dict[str, int]:
    try:
        rows = connection.execute(
            f"SELECT status, COUNT(*) FROM {table} GROUP BY status ORDER BY status"
        ).fetchall()
    except sqlite3.Error as exc:
        raise S55Error("pnc_rca_s55_delivery_database_invalid", table) from exc
    return {str(row[0]): int(row[1]) for row in rows}


def _stage_delivery(args: argparse.Namespace, *, now: datetime) -> dict[str, Any]:
    _authority_raw, authority, authority_digest = _load_authority(args.authority)
    env_raw, env = _load_environment(args.env_file)
    env.setdefault("HERMES_HOME", str(args.env_file.expanduser().absolute().parent))
    config = _public_config(env, authority)
    safe = _validate_safe_config(config, env, authority)
    gate_raw, gate = _read_owner_json(args.preproduction_gate, "s55_preproduction_gate")
    try:
        fingerprint = capsules.release_report_fingerprint(gate)
    except capsules.CapsuleError as exc:
        raise S55Error(exc.code) from exc
    if gate.get("mode") != "preproduction" or gate.get("fingerprint") != fingerprint:
        raise S55Error("pnc_rca_s55_preproduction_gate_invalid")
    _fresh(
        gate.get("evaluated_at"),
        now=now,
        code="pnc_rca_s55_preproduction_gate_stale",
    )
    checks = {
        item.get("name"): item
        for item in gate.get("checks", [])
        if isinstance(item, Mapping)
    }
    contract = checks.get("contract_drift", {}).get("detail")
    if (
        not isinstance(contract, Mapping)
        or contract.get("authority_sha256") != authority_digest
        or checks.get("safe_side_effect_config", {}).get("detail") != safe
    ):
        raise S55Error("pnc_rca_s55_gate_authority_mismatch")
    collector = config["delivery_collector"]
    baseline_path = Path(str(collector["quarantine_baseline_path"]))
    try:
        _baseline_raw, _baseline, baseline_status = _validate_baseline(
            baseline_path,
            authority,
            control_db_path=args.control_db,
            config=config,
        )
    except ActivationGateError as exc:
        raise S55Error(exc.code, exc.detail) from exc
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(
            args.control_db.as_uri() + "?mode=ro", uri=True, timeout=10
        )
        connection.execute("PRAGMA query_only=ON")
        outbox = _status_counts(connection, "rca_outbox")
        effects = _status_counts(connection, "rca_delivery_effects")
        claimed_outbox = int(
            connection.execute(
                "SELECT COUNT(*) FROM rca_outbox WHERE lease_token IS NOT NULL"
            ).fetchone()[0]
        )
        claimed_effects = int(
            connection.execute(
                "SELECT COUNT(*) FROM rca_delivery_effects "
                "WHERE lease_token IS NOT NULL"
            ).fetchone()[0]
        )
    except sqlite3.Error as exc:
        raise S55Error("pnc_rca_s55_delivery_database_invalid") from exc
    finally:
        if connection is not None:
            connection.close()
    if (
        claimed_outbox != 0
        or claimed_effects != 0
        or any(effects.get(name, 0) for name in ("pending", "claimed", "running"))
    ):
        raise S55Error("pnc_rca_s55_delivery_not_quiescent")
    return _pass("delivery", {
        "preproduction_gate_raw_sha256": _sha(gate_raw),
        "preproduction_gate_fingerprint": fingerprint,
        "env_raw_sha256": _sha(env_raw),
        "safe_side_effect_config": safe,
        "quarantine_baseline_state": baseline_status["state"],
        "outbox_status_counts": outbox,
        "delivery_effect_status_counts": effects,
        "claimed_outbox": claimed_outbox,
        "claimed_effects": claimed_effects,
        "historical_requeue_performed": False,
        "external_write_performed": False,
    })


def _validate_vm_observation(
    value: Mapping[str, Any],
    *,
    authority: Mapping[str, Any],
    authority_digest: str,
    now: datetime,
) -> dict[str, Any]:
    expected = {
        "schema_version",
        "observed_at",
        "release_id",
        "authority_sha256",
        "faces",
        "worker_probe",
        "report_resident",
        "read_only_attestation",
    }
    if (
        set(value) != expected
        or value.get("schema_version") != VM_SCHEMA_VERSION
        or value.get("release_id") != authority["release_id"]
        or value.get("authority_sha256") != authority_digest
    ):
        raise S55Error("pnc_rca_s55_vm_observation_invalid")
    observed_at = _fresh(
        value.get("observed_at"), now=now, code="pnc_rca_s55_vm_observation_stale"
    )
    faces = value.get("faces")
    if not isinstance(faces, Mapping) or set(faces) != {
        "vm_worker_state",
        "g1q3_rca_pipeline",
        "mcap_data_translate",
    }:
        raise S55Error("pnc_rca_s55_vm_observation_invalid")
    for name, observed in faces.items():
        expected_face = authority["faces"][name]
        if (
            not isinstance(observed, Mapping)
            or observed.get("commit") != expected_face["commit"]
            or observed.get("tree") != expected_face["tree"]
            or observed.get("root") != expected_face["root"]
            or observed.get("dirty") is not False
            or (
                name == "mcap_data_translate"
                and observed.get("contract_sha256")
                != expected_face["contract_sha256"]
            )
        ):
            raise S55Error("pnc_rca_s55_vm_face_mismatch")
    worker = value.get("worker_probe")
    resident = value.get("report_resident")
    if (
        not isinstance(worker, Mapping)
        or set(worker)
        != {
            "fixed_cli_path",
            "fixed_cli_sha256",
            "fixed_cli_exit_code",
            "resource_class",
            "input_materialized_bytes",
            "task_created",
        }
        or not str(worker.get("fixed_cli_path") or "").startswith("/")
        or worker.get("fixed_cli_exit_code") != 0
        or worker.get("resource_class") != "rca_prod"
        or worker.get("input_materialized_bytes") != 0
        or worker.get("task_created") is not False
        or _SHA256_RE.fullmatch(str(worker.get("fixed_cli_sha256") or "")) is None
        or not isinstance(resident, Mapping)
        or set(resident)
        != {
            "pid",
            "process_create_time",
            "script",
            "port",
            "pipeline_commit",
            "pipeline_tree",
            "pipeline_root",
        }
        or not isinstance(resident.get("pid"), int)
        or resident["pid"] <= 0
        or isinstance(resident.get("process_create_time"), bool)
        or not isinstance(resident.get("process_create_time"), (int, float))
        or resident["process_create_time"] <= 0
        or not str(resident.get("script") or "").startswith("/")
        or resident.get("port") != 18081
        or resident.get("pipeline_commit")
        != authority["faces"]["g1q3_rca_pipeline"]["commit"]
        or resident.get("pipeline_tree")
        != authority["faces"]["g1q3_rca_pipeline"]["tree"]
        or resident.get("pipeline_root")
        != authority["faces"]["g1q3_rca_pipeline"]["root"]
        or value.get("read_only_attestation")
        != {
            "remote_mutation_performed": False,
            "task_submission_performed": False,
            "mcap_execution_performed": False,
            "external_effects_triggered": False,
        }
    ):
        raise S55Error("pnc_rca_s55_vm_runtime_invalid")
    return {
        "observed_at": observed_at,
        "faces": {name: dict(faces[name]) for name in sorted(faces)},
        "worker_probe": dict(worker),
        "report_resident": dict(resident),
    }


def _stage_vm(args: argparse.Namespace, *, now: datetime) -> dict[str, Any]:
    _raw, authority, authority_digest = _load_authority(args.authority)
    vm_raw, vm = _read_owner_json(args.vm_observation, "s55_vm_observation")
    validated = _validate_vm_observation(
        vm,
        authority=authority,
        authority_digest=authority_digest,
        now=now,
    )
    return _pass("vm", {"observation_raw_sha256": _sha(vm_raw), **validated})


def _fetch_report_health(url: str, *, timeout: float) -> tuple[bytes, Mapping[str, Any]]:
    selected = url.rstrip("/") + "/healthz"
    request = urlrequest.Request(
        selected,
        headers={"Accept": "application/json", "User-Agent": "pnc-rca-s55-probe/1"},
        method="GET",
    )
    try:
        with urlrequest.urlopen(request, timeout=timeout) as response:
            raw = response.read(MAX_HTTP_BYTES + 1)
            status = int(response.status)
            content_type = str(response.headers.get("Content-Type") or "")
            final_url = str(response.geturl())
    except (OSError, urlerror.URLError) as exc:
        raise S55Error(
            "pnc_rca_s55_report_not_measured",
            type(exc).__name__,
            result="not_measured",
        ) from exc
    if (
        status != 200
        or final_url != selected
        or len(raw) > MAX_HTTP_BYTES
        or "json" not in content_type.lower()
    ):
        raise S55Error("pnc_rca_s55_report_health_invalid")
    try:
        body = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise S55Error("pnc_rca_s55_report_health_invalid") from exc
    if not isinstance(body, Mapping):
        raise S55Error("pnc_rca_s55_report_health_invalid")
    return raw, body


def _validate_report_health(
    body: Mapping[str, Any],
    *,
    authority: Mapping[str, Any],
    authority_digest: str,
    now: datetime,
) -> dict[str, Any]:
    expected = {
        "schema_version",
        "ok",
        "observed_at",
        "release_id",
        "authority_sha256",
        "root",
        "manifest_schema_version",
        "manifest_sha256",
        "pid",
        "process_create_time",
    }
    publication = authority["report_publication"]
    if (
        set(body) != expected
        or body.get("schema_version") != REPORT_HEALTH_SCHEMA_VERSION
        or body.get("ok") is not True
        or body.get("release_id") != authority["release_id"]
        or body.get("authority_sha256") != authority_digest
        or body.get("root") != publication["root"]
        or body.get("manifest_schema_version")
        != publication["manifest_schema_version"]
        or _SHA256_RE.fullmatch(str(body.get("manifest_sha256") or "")) is None
        or not isinstance(body.get("pid"), int)
        or body["pid"] <= 0
        or not isinstance(body.get("process_create_time"), (int, float))
        or isinstance(body.get("process_create_time"), bool)
    ):
        raise S55Error("pnc_rca_s55_report_health_invalid")
    return {
        **dict(body),
        "observed_at": _fresh(
            body["observed_at"], now=now, code="pnc_rca_s55_report_health_stale"
        ),
    }


def _stage_report(
    args: argparse.Namespace,
    *,
    now: datetime,
    fetcher: Callable[..., tuple[bytes, Mapping[str, Any]]] = _fetch_report_health,
) -> dict[str, Any]:
    _raw, authority, authority_digest = _load_authority(args.authority)
    raw, body = fetcher(
        authority["report_publication"]["canonical_base_url"],
        timeout=args.report_timeout_seconds,
    )
    validated = _validate_report_health(
        body,
        authority=authority,
        authority_digest=authority_digest,
        now=now,
    )
    return _pass("report", {"health_raw_sha256": _sha(raw), **validated})


def run_stage(
    name: str, args: argparse.Namespace, *, now: datetime | None = None
) -> dict[str, Any]:
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    stages = {
        "authority": _stage_authority,
        "activation": _stage_activation,
        "delivery": _stage_delivery,
        "vm": _stage_vm,
        "report": _stage_report,
    }
    if name not in stages:
        raise S55Error("pnc_rca_s55_stage_invalid")
    try:
        return stages[name](args, now=current)
    except (ActivationGateError, SchemaFingerprintError) as exc:
        raise S55Error(exc.code, exc.detail) from exc


def _stage_arguments(args: argparse.Namespace, name: str) -> list[str]:
    return [
        str(Path(__file__).resolve()),
        "stage",
        "--name",
        name,
        "--authority",
        str(args.authority),
        "--pointer",
        str(args.pointer),
        "--live-manifest",
        str(args.live_manifest),
        "--active-binding",
        str(args.active_binding),
        "--schema-receipt",
        str(args.schema_receipt),
        "--preproduction-gate",
        str(args.preproduction_gate),
        "--control-db",
        str(args.control_db),
        "--env-file",
        str(args.env_file),
        "--vm-observation",
        str(args.vm_observation),
        "--expected-epoch-id",
        args.expected_epoch_id,
        "--expected-activation-state",
        args.expected_activation_state,
        "--expected-historical-hold",
        str(args.expected_historical_hold),
        "--report-timeout-seconds",
        str(args.report_timeout_seconds),
    ]


def run_probe(
    args: argparse.Namespace,
    *,
    now: datetime | None = None,
    runner: Callable[..., Any] = subprocess.run,
) -> tuple[dict[str, Any], int]:
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    started_at = current.isoformat()
    _authority_raw, authority, authority_digest = _load_authority(args.authority)
    stages: list[dict[str, Any]] = []
    for name in STAGE_NAMES:
        stage_started = datetime.now(timezone.utc)
        try:
            completed = runner(
                [sys.executable, *_stage_arguments(args, name)],
                capture_output=True,
                timeout=120,
                check=False,
            )
            stdout = bytes(completed.stdout or b"")
            stderr = bytes(completed.stderr or b"")
            exit_code = int(completed.returncode)
        except (OSError, subprocess.SubprocessError) as exc:
            stdout = b""
            stderr = type(exc).__name__.encode("ascii")
            exit_code = 2
        if len(stdout) > MAX_STAGE_OUTPUT_BYTES or len(stderr) > MAX_STAGE_OUTPUT_BYTES:
            payload: dict[str, Any] = {
                "result": "failed",
                "reason": "pnc_rca_s55_stage_output_too_large",
            }
        else:
            lines = [line for line in stdout.decode("utf-8", "replace").splitlines() if line]
            try:
                decoded = json.loads(lines[-1]) if lines else None
            except json.JSONDecodeError:
                decoded = None
            payload = (
                dict(decoded)
                if isinstance(decoded, Mapping)
                else {
                    "result": "failed",
                    "reason": "pnc_rca_s55_stage_output_invalid",
                }
            )
        result = str(payload.get("result") or "failed")
        if exit_code == 0 and result != "passed":
            result = "failed"
        if exit_code != 0 and result == "passed":
            result = "failed"
        stages.append({
            "name": name,
            "result": result,
            "reason": str(payload.get("reason") or ""),
            "exit_code": exit_code,
            "started_at": stage_started.isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "stdout_sha256": _sha(stdout),
            "stderr_sha256": _sha(stderr),
            "stdout_size_bytes": len(stdout),
            "stderr_size_bytes": len(stderr),
            "detail": payload.get("detail") if isinstance(payload.get("detail"), Mapping) else {},
        })
    if any(item["result"] == "failed" for item in stages):
        result = "failed"
    elif any(item["result"] == "not_measured" for item in stages):
        result = "not_measured"
    else:
        result = "passed"
    direct_exit_code = 0 if result == "passed" else 2
    receipt = {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "run_id": args.run_id,
        "started_at": started_at,
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "result": result,
        "not_measured_reason": {
            "passed": "",
            "failed": "one or more mandatory stages failed",
            "not_measured": "one or more mandatory stages were not measured",
        }[result],
        "direct_exit_code": direct_exit_code,
        "release_id": authority["release_id"],
        "authority_sha256": authority_digest,
        "expected_epoch_id": args.expected_epoch_id,
        "expected_activation_state": args.expected_activation_state,
        "expected_historical_hold": args.expected_historical_hold,
        "stages": stages,
        "scope_attestation": {
            "current_chain_probe": True,
            "legacy_mdi_smoke_restored": False,
            "historical_replay_performed": False,
            "mcap_execution_performed": False,
            "feishu_remote_write_proven": False,
            "feishu_remote_read_after_write_proven": False,
            "production_mutation_performed": False,
            "external_effects_triggered": False,
        },
    }
    try:
        raw = capsules._write_owner_no_clobber(args.output, receipt)
    except capsules.CapsuleError as exc:
        raise S55Error(exc.code) from exc
    payload = {
        "schema_version": CLI_SCHEMA_VERSION,
        "command": "run",
        "ok": result == "passed",
        "result": result,
        "direct_exit_code": direct_exit_code,
        "receipt_path": str(args.output),
        "receipt_raw_sha256": _sha(raw),
        "authority_sha256": authority_digest,
    }
    return payload, direct_exit_code


class _SafeParser(argparse.ArgumentParser):
    def error(self, _message: str) -> None:
        raise S55Error("pnc_rca_s55_cli_arguments_invalid")


def _add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--authority", type=Path, required=True)
    parser.add_argument("--pointer", type=Path, required=True)
    parser.add_argument("--live-manifest", type=Path, required=True)
    parser.add_argument("--active-binding", type=Path, required=True)
    parser.add_argument("--schema-receipt", type=Path, required=True)
    parser.add_argument("--preproduction-gate", type=Path, required=True)
    parser.add_argument("--control-db", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--vm-observation", type=Path, required=True)
    parser.add_argument("--expected-epoch-id", required=True)
    parser.add_argument(
        "--expected-activation-state",
        choices=("preauthorized", "bounded_active", "confirmed", "steady"),
        required=True,
    )
    parser.add_argument("--expected-historical-hold", type=int, required=True)
    parser.add_argument("--report-timeout-seconds", type=float, default=10.0)


def _arguments(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = _SafeParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    stage = commands.add_parser("stage")
    stage.add_argument("--name", choices=STAGE_NAMES, required=True)
    _add_common(stage)
    run = commands.add_parser("run")
    _add_common(run)
    run.add_argument("--run-id", required=True)
    run.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    command = "unknown"
    try:
        args = _arguments(argv)
        command = str(args.command)
        if command == "stage":
            payload = run_stage(args.name, args)
            print(json.dumps(payload, ensure_ascii=True, sort_keys=True))
            return 0
        if command == "run":
            payload, exit_code = run_probe(args)
            print(json.dumps(payload, ensure_ascii=True, sort_keys=True))
            return exit_code
        raise S55Error("pnc_rca_s55_cli_arguments_invalid")
    except S55Error as exc:
        payload = {
            "schema_version": (
                STAGE_SCHEMA_VERSION if command == "stage" else CLI_SCHEMA_VERSION
            ),
            "command": command,
            "ok": False,
            "result": exc.result,
            "reason": exc.code,
            "detail": {"error": exc.detail},
            "production_mutation_performed": False,
            "external_effects_triggered": False,
        }
        print(json.dumps(payload, ensure_ascii=True, sort_keys=True))
        return 2
    except (ActivationGateError, SchemaFingerprintError, capsules.CapsuleError) as exc:
        payload = {
            "schema_version": CLI_SCHEMA_VERSION,
            "command": command,
            "ok": False,
            "result": "failed",
            "reason": exc.code,
            "detail": {"error": getattr(exc, "detail", exc.code)},
            "production_mutation_performed": False,
            "external_effects_triggered": False,
        }
        print(json.dumps(payload, ensure_ascii=True, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
