#!/usr/bin/env python3
"""Evaluate RCA C=1/2/4 capacity evidence without starting production work."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Mapping, Sequence


SCHEMA_VERSION = "pnc_rca_capacity_80pct_receipt_v1"
CAPACITY_PROFILE = "rca_prod_80pct"
CONCURRENCY_SEQUENCE = (1, 2, 4)
RESOURCE_COMMAND = Path("/Users/songying/.local/bin/ssh-mini-resource")
_MEASUREMENT_FIELDS = frozenset(
    {
        "concurrency",
        "sample_count",
        "successful_count",
        "duration_seconds",
        "throughput_per_second",
        "p50_seconds",
        "p95_seconds",
        "kafka_lag_before",
        "kafka_lag_after",
        "offset_commits",
        "outbox_oldest_age_before_seconds",
        "outbox_oldest_age_after_seconds",
        "vm_started",
        "vm_completed",
        "collector_completed",
        "delivery_completed",
        "duplicate_count",
        "lost_count",
        "failure_codes",
        "max_cpu_ratio",
        "max_rss_ratio",
        "max_load_ratio",
        "min_swap_free_ratio",
        "max_storage_ratio",
        "same_business_key_serial",
        "different_business_keys_only",
    }
)


class CapacityProbeError(ValueError):
    def __init__(self, code: str, detail: str = ""):
        self.code = str(code or "capacity_probe_invalid")[:120]
        self.detail = str(detail or self.code)[:1000]
        super().__init__(self.detail)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode("ascii")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _finite_number(value: object, *, minimum: float = 0.0) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CapacityProbeError("capacity_measurement_number_invalid")
    result = float(value)
    if result < minimum or result != result or result in (float("inf"), float("-inf")):
        raise CapacityProbeError("capacity_measurement_number_invalid")
    return result


def _integer(value: object, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise CapacityProbeError("capacity_measurement_integer_invalid")
    return value


def capture_resource_report(command: Path = RESOURCE_COMMAND) -> dict[str, Any]:
    completed = subprocess.run(
        [str(command), "--json", "--resource-class", "rca_prod"],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if completed.returncode != 0:
        raise CapacityProbeError(
            "rca_resource_probe_failed", (completed.stderr or completed.stdout)[-500:]
        )
    try:
        report = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise CapacityProbeError("rca_resource_probe_json_invalid") from exc
    if not isinstance(report, dict):
        raise CapacityProbeError("rca_resource_probe_contract_invalid")
    return report


def _validate_resource_report(report: object) -> dict[str, Any]:
    if not isinstance(report, Mapping):
        raise CapacityProbeError("rca_resource_probe_contract_invalid")
    authorization = report.get("rca_capacity_authorization")
    snapshot = report.get("rca_prod_snapshot")
    if (
        report.get("resource_class") != "rca_prod"
        or not isinstance(report.get("ok_for_rca_prod_submit"), bool)
        or not isinstance(authorization, Mapping)
        or not isinstance(snapshot, Mapping)
        or not str(report.get("rca_prod_snapshot_sha256") or "")
    ):
        raise CapacityProbeError("rca_resource_probe_contract_invalid")
    return dict(report)


def _validate_measurement(value: object, expected_concurrency: int) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != _MEASUREMENT_FIELDS:
        raise CapacityProbeError("capacity_measurement_contract_invalid")
    measurement = dict(value)
    concurrency = _integer(measurement["concurrency"], minimum=1)
    if concurrency != expected_concurrency:
        raise CapacityProbeError("capacity_measurement_sequence_invalid")
    integer_fields = (
        "sample_count",
        "successful_count",
        "kafka_lag_before",
        "kafka_lag_after",
        "offset_commits",
        "vm_started",
        "vm_completed",
        "collector_completed",
        "delivery_completed",
        "duplicate_count",
        "lost_count",
    )
    for field in integer_fields:
        _integer(measurement[field])
    numeric_fields = (
        "duration_seconds",
        "throughput_per_second",
        "p50_seconds",
        "p95_seconds",
        "outbox_oldest_age_before_seconds",
        "outbox_oldest_age_after_seconds",
        "max_cpu_ratio",
        "max_rss_ratio",
        "max_load_ratio",
        "min_swap_free_ratio",
        "max_storage_ratio",
    )
    for field in numeric_fields:
        _finite_number(measurement[field])
    if (
        not isinstance(measurement["failure_codes"], list)
        or any(not isinstance(code, str) or not code for code in measurement["failure_codes"])
        or not isinstance(measurement["same_business_key_serial"], bool)
        or not isinstance(measurement["different_business_keys_only"], bool)
    ):
        raise CapacityProbeError("capacity_measurement_contract_invalid")
    return measurement


def _measurement_gates(measurement: Mapping[str, Any], t1: float | None) -> list[str]:
    failures: list[str] = []
    concurrency = int(measurement["concurrency"])
    samples = int(measurement["sample_count"])
    complete_fields = (
        "successful_count",
        "vm_started",
        "vm_completed",
        "collector_completed",
        "delivery_completed",
    )
    if samples <= 0 or any(int(measurement[field]) != samples for field in complete_fields):
        failures.append("incomplete_pipeline_counts")
    if int(measurement["duplicate_count"]) != 0:
        failures.append("duplicate_effect")
    if int(measurement["lost_count"]) != 0:
        failures.append("lost_case")
    forbidden = {
        "resource_policy_invalid",
        "rca_prod_concurrency_exhausted",
        "canonical_vm_receipt_missing",
        "host_mirror_used_as_terminal_truth",
    }
    observed_failures = set(measurement["failure_codes"])
    failures.extend(sorted(forbidden & observed_failures))
    if int(measurement["kafka_lag_after"]) > int(measurement["kafka_lag_before"]):
        failures.append("kafka_lag_grew")
    if float(measurement["outbox_oldest_age_after_seconds"]) > float(
        measurement["outbox_oldest_age_before_seconds"]
    ):
        failures.append("outbox_oldest_age_grew")
    for field in ("max_cpu_ratio", "max_rss_ratio", "max_load_ratio", "max_storage_ratio"):
        if float(measurement[field]) > 0.8:
            failures.append(f"{field}_exceeded_80pct")
    if float(measurement["min_swap_free_ratio"]) < 0.2:
        failures.append("swap_headroom_below_20pct")
    if not measurement["same_business_key_serial"]:
        failures.append("business_key_serialization_not_proven")
    if not measurement["different_business_keys_only"] and concurrency > 1:
        failures.append("parallel_business_key_isolation_not_proven")
    throughput = float(measurement["throughput_per_second"])
    if throughput <= 0:
        failures.append("throughput_not_positive")
    if t1 is not None and throughput < 0.8 * concurrency * t1:
        failures.append("throughput_scaling_below_80pct")
    return sorted(set(failures))


def build_capacity_receipt(
    resource_report: object,
    *,
    scheduler_evidence: Mapping[str, Any] | None = None,
    measurements: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    report = _validate_resource_report(resource_report)
    authorization = dict(report["rca_capacity_authorization"])
    if len(measurements) > len(CONCURRENCY_SEQUENCE):
        raise CapacityProbeError("capacity_measurement_count_invalid")
    policy_version = str(authorization.get("policy_version") or "").strip()
    authorized_max = authorization.get("max_concurrency")
    steady_ready = bool(
        authorization.get("authorization_ready") is True
        and policy_version
        and isinstance(authorized_max, int)
        and not isinstance(authorized_max, bool)
        and 1 <= authorized_max <= 4
    )
    scheduler_ready = bool(
        isinstance(scheduler_evidence, Mapping)
        and scheduler_evidence.get("ready") is True
        and scheduler_evidence.get("capacity_profile") == CAPACITY_PROFILE
        and scheduler_evidence.get("policy_version") == policy_version
        and scheduler_evidence.get("max_concurrency") == authorized_max
    )
    resource_ready = report["ok_for_rca_prod_submit"] is True
    prerequisites_ready = resource_ready and steady_ready and scheduler_ready
    sequence: list[dict[str, Any]] = []
    c_safe: int | None = None
    t1: float | None = None
    stopped = not prerequisites_ready
    for index, concurrency in enumerate(CONCURRENCY_SEQUENCE):
        if steady_ready and concurrency > authorized_max:
            stopped = True
        if stopped or index >= len(measurements):
            if not resource_ready:
                reason = "rca_prod_resource_gate_not_ready"
            elif not steady_ready:
                reason = "steady_capacity_authorization_not_ready"
            elif not scheduler_ready:
                reason = "scheduler_evidence_not_ready"
            elif concurrency > authorized_max:
                reason = "authorization_max_concurrency_reached"
            else:
                reason = "measurement_not_run"
            sequence.append(
                {"concurrency": concurrency, "status": "not_run", "reason": reason}
            )
            continue
        measurement = _validate_measurement(measurements[index], concurrency)
        failures = _measurement_gates(measurement, t1)
        passed = not failures
        sequence.append(
            {
                "concurrency": concurrency,
                "status": "passed" if passed else "failed",
                "gate_failures": failures,
                "measurement": measurement,
            }
        )
        if passed:
            c_safe = concurrency
            if concurrency == 1:
                t1 = float(measurement["throughput_per_second"])
        else:
            stopped = True
    if not prerequisites_ready:
        status = "no_go"
    elif not measurements:
        status = "ready_for_c1_measurement"
    elif any(stage["status"] == "failed" for stage in sequence):
        status = "measured_no_go_for_next_level"
    elif len(measurements) < len(CONCURRENCY_SEQUENCE):
        status = "ready_for_next_measurement"
    else:
        status = "measured"
    snapshot = dict(report["rca_prod_snapshot"])
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": utc_now(),
        "status": status,
        "capacity_profile": CAPACITY_PROFILE,
        "resource_gate": {
            "ready": resource_ready,
            "snapshot_sha256": str(report["rca_prod_snapshot_sha256"]),
            "snapshot": snapshot,
            "reasons": list(report.get("rca_prod_reasons") or []),
            "warnings": list(report.get("warnings") or []),
        },
        "steady_authorization": authorization,
        "scheduler_evidence": (
            dict(scheduler_evidence)
            if isinstance(scheduler_evidence, Mapping)
            else {"ready": False, "reason": "scheduler_evidence_missing"}
        ),
        "measurement_order": list(CONCURRENCY_SEQUENCE),
        "measurement_rule": "T(C) >= 0.8 * C * T1",
        "headroom_rule": "cpu/rss/load/storage <= 0.8 and swap_free >= 0.2",
        "T1": t1,
        "C_safe": c_safe,
        "sequence": sequence,
        "production_success_claimed": False,
        "external_side_effects": {"vm_submissions": 0, "kafka_commits": 0},
        "next_action": (
            "install an owner-approved steady capacity authorization bound to the "
            "current policy, then capture matching scheduler evidence and begin C=1"
            if not steady_ready
            else "capture scheduler evidence bound to the authorization policy"
            if not scheduler_ready
            else "run only the next listed concurrency through canonical rca_prod ingress"
        ),
    }


def write_json(path: Path, value: Mapping[str, Any]) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = canonical_bytes(value) + b"\n"
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    return {"path": str(path), "sha256": sha256_bytes(raw), "bytes": len(raw)}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--resource-report", type=Path)
    parser.add_argument("--scheduler-evidence", type=Path)
    parser.add_argument("--measurements", type=Path)
    return parser


def _json(path: Path) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise CapacityProbeError("capacity_input_unreadable", str(exc)) from exc


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        report = _json(args.resource_report) if args.resource_report else capture_resource_report()
        scheduler = _json(args.scheduler_evidence) if args.scheduler_evidence else None
        raw_measurements = _json(args.measurements) if args.measurements else []
        if not isinstance(raw_measurements, list):
            raise CapacityProbeError("capacity_measurements_invalid")
        receipt = build_capacity_receipt(
            report,
            scheduler_evidence=scheduler if isinstance(scheduler, Mapping) else None,
            measurements=raw_measurements,
        )
        artifact = write_json(args.output, receipt)
    except (CapacityProbeError, subprocess.SubprocessError) as exc:
        code = exc.code if isinstance(exc, CapacityProbeError) else "rca_resource_probe_failed"
        print(json.dumps({"success": False, "error_code": code}, sort_keys=True))
        return 2
    print(json.dumps({"success": True, "status": receipt["status"], "receipt": artifact}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
