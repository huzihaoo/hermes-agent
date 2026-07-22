#!/usr/bin/env python3
"""Crash-recoverable executor for the RCA bootstrap-to-steady capacity ratchet."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys
from typing import Any, Callable, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gateway import pnc_rca_capacity_runtime as runtime
from gateway import pnc_rca_capacity_transition as transition
from gateway.pnc_rca_control_store import (
    CapacityTransitionStateError,
    RcaControlStore,
)


EXECUTOR_SCHEMA_VERSION = "pnc_rca_capacity_transition_executor_v1"
SAFE_CODE_RE = re.compile(r"^[a-z][a-z0-9_:]{2,160}$")


class CapacityTransitionExecutorError(RuntimeError):
    def __init__(self, code: str):
        self.code = str(code or "rca_capacity_executor_invalid")[:160]
        super().__init__(self.code)


class _SafeArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise CapacityTransitionExecutorError("rca_capacity_executor_arguments_invalid")


def _utc(value: datetime | None = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None or current.utcoffset() is None:
        raise CapacityTransitionExecutorError("rca_capacity_executor_time_invalid")
    return current.astimezone(timezone.utc)


def _timestamp(value: str) -> datetime:
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(candidate)
    except (TypeError, ValueError, OverflowError) as exc:
        raise CapacityTransitionExecutorError(
            "rca_capacity_executor_time_invalid"
        ) from exc
    return _utc(parsed)


def _identity(value: str, code: str) -> str:
    normalized = str(value or "").strip()
    if not transition.IDENTITY_RE.fullmatch(normalized):
        raise CapacityTransitionExecutorError(code)
    return normalized


def _reason(value: str) -> str:
    normalized = str(value or "").strip()
    if not normalized or len(normalized.encode("utf-8")) > 1000:
        raise CapacityTransitionExecutorError("rca_capacity_executor_reason_invalid")
    return normalized


def _artifact_presence(paths: runtime.CapacityRuntimePaths) -> dict[str, bool]:
    result: dict[str, bool] = {}
    for name, path in {
        "intent": paths.transition_intent,
        "authorization": paths.transition_authorization,
        "receipt": paths.transition_receipt,
        "marker": paths.commit_marker,
        "bundle": paths.evidence_bundle,
    }.items():
        try:
            path.lstat()
            result[name] = True
        except FileNotFoundError:
            result[name] = False
        except OSError as exc:
            raise CapacityTransitionExecutorError(
                "rca_capacity_executor_artifact_stat_failed"
            ) from exc
    return result


class SteadyCapacityTransitionExecutor:
    def __init__(
        self,
        *,
        store: RcaControlStore,
        control_db_path: str | Path,
        hmac_key: bytes,
        now: Callable[[], datetime] | None = None,
        lock_timeout_seconds: float = runtime.DEFAULT_LOCK_TIMEOUT_SECONDS,
        fault_injector: Callable[[str], None] | None = None,
    ):
        self.store = store
        self.control_db_path = Path(control_db_path).expanduser().absolute()
        self.hmac_key = runtime.load_capacity_hmac_key(hmac_key)
        self.now = now or (lambda: datetime.now(timezone.utc))
        self.lock_timeout_seconds = lock_timeout_seconds
        self.fault_injector = fault_injector
        self.paths = runtime.CapacityRuntimePaths.from_control_db(self.control_db_path)

    def _fault(self, prefix: str) -> None:
        if self.fault_injector is not None:
            self.fault_injector(prefix)

    def status(self) -> dict[str, Any]:
        state = self.store.capacity_transition_state()
        activation = self.store.activation_epoch()
        if state is None:
            return {
                "schema_version": EXECUTOR_SCHEMA_VERSION,
                "ok": True,
                "command": "status",
                "capacity_state": "UNCONFIGURED",
                "capacity_generation": 0,
                "business_activation_epoch_id": str(activation.get("epoch_id") or "")
                if activation
                else "",
                "business_activation_state": str(activation.get("state") or "")
                if activation
                else "",
                "runtime_effective_state": transition.STEADY_BLOCKED,
                "runtime_reason_code": "rca_capacity_persisted_state_missing",
                "artifact_presence": _artifact_presence(self.paths),
            }
        resolver = runtime.CapacityRuntimeResolver(
            store=self.store,
            control_db_path=self.control_db_path,
            release_id=str(state.get("release_id") or ""),
            bootstrap_epoch_id=str(state.get("bootstrap_epoch_id") or ""),
            initial_policy="bootstrap",
            hmac_key=self.hmac_key,
            now=self.now,
            lock_timeout_seconds=self.lock_timeout_seconds,
        )
        decision = resolver.observe()
        return {
            "schema_version": EXECUTOR_SCHEMA_VERSION,
            "ok": True,
            "command": "status",
            "capacity_state": str(state.get("state") or "UNCONFIGURED"),
            "capacity_generation": int(state.get("generation") or 0),
            "business_activation_epoch_id": str(activation.get("epoch_id") or "")
            if activation
            else "",
            "business_activation_state": str(activation.get("state") or "")
            if activation
            else "",
            "runtime_effective_state": decision["effective_state"],
            "runtime_reason_code": decision["reason_code"],
            "artifact_presence": _artifact_presence(self.paths),
        }

    def _read_initial_authorization(
        self,
        *,
        authorization_path: Path | None,
        ledger: transition.CapacityLedgerSnapshot,
        state: Mapping[str, Any],
        epoch_id: str,
        operator: str,
        reason: str,
        current: datetime,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
        if authorization_path is None:
            raise CapacityTransitionExecutorError(
                "rca_capacity_executor_authorization_required"
            )
        authorization_path = Path(authorization_path).expanduser()
        if not authorization_path.is_absolute():
            raise CapacityTransitionExecutorError(
                "rca_capacity_executor_authorization_not_absolute"
            )
        if any(_artifact_presence(self.paths).values()):
            raise CapacityTransitionExecutorError(
                "rca_capacity_executor_orphaned_prefix"
            )
        authorization, _authorization_sha = transition.read_owner_only_json(
            authorization_path
        )
        authorization = transition.validate_transition_authorization(
            authorization,
            ledger=ledger,
            now=current,
            hmac_key=self.hmac_key,
        )
        if authorization["target_generation"] != int(state["generation"]) + 1:
            raise CapacityTransitionExecutorError(
                "rca_capacity_executor_generation_binding_invalid"
            )
        if operator != authorization["approval"]["authorized_by"]:
            raise CapacityTransitionExecutorError(
                "rca_capacity_executor_operator_not_authorized"
            )
        suffix = authorization["authorization_fingerprint"][:24]
        receipt = transition.issue_transition_receipt(
            ledger=ledger,
            authorization=authorization,
            receipt_id=f"steady-receipt-{suffix}",
            created_at=current,
            hmac_key=self.hmac_key,
        )
        marker = transition.issue_steady_commit_marker(
            ledger=ledger,
            authorization=authorization,
            receipt=receipt,
            marker_id=f"steady-marker-{suffix}",
            committed_at=current,
            hmac_key=self.hmac_key,
        )
        intent = transition.issue_steady_transition_intent(
            ledger=ledger,
            authorization=authorization,
            receipt=receipt,
            marker=marker,
            intent_id=f"steady-intent-{suffix}",
            business_activation_epoch_id=epoch_id,
            operator=operator,
            reason=reason,
            created_at=current,
            hmac_key=self.hmac_key,
        )
        return intent, authorization, receipt, marker

    def _read_recovery_intent(
        self,
        *,
        authorization_path: Path | None,
        ledger: transition.CapacityLedgerSnapshot,
        epoch_id: str,
        operator: str,
        reason: str,
        current: datetime,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
        if authorization_path is not None:
            raise CapacityTransitionExecutorError(
                "rca_capacity_executor_recovery_authorization_forbidden"
            )
        intent, _intent_sha = transition.read_owner_only_json(
            self.paths.transition_intent
        )
        intent = transition.validate_steady_transition_intent(
            intent,
            ledger=ledger,
            now=current,
            hmac_key=self.hmac_key,
            allow_historical=True,
        )
        expected = {
            "business_activation_epoch_id": epoch_id,
            "operator": operator,
            "reason": reason,
        }
        if any(intent.get(field) != value for field, value in expected.items()):
            raise CapacityTransitionExecutorError(
                "rca_capacity_executor_recovery_binding_invalid"
            )
        return (
            intent,
            dict(intent["transition_authorization"]),
            dict(intent["transition_receipt"]),
            dict(intent["commit_marker"]),
        )

    def execute(
        self,
        *,
        epoch_id: str,
        operator: str,
        reason: str,
        authorization_path: Path | None,
        apply: bool,
        recovery_requested: bool = False,
    ) -> dict[str, Any]:
        business_epoch_id = _identity(
            epoch_id, "rca_capacity_executor_business_epoch_invalid"
        )
        actor = _identity(operator, "rca_capacity_executor_operator_invalid")
        justification = _reason(reason)
        with transition.capacity_flock(
            self.paths.global_lock,
            exclusive=True,
            timeout_seconds=self.lock_timeout_seconds,
        ):
            current = _utc(self.now())
            ledger = transition.read_sample_ledger(
                self.paths.sample_ledger,
                hmac_key=self.hmac_key,
                timeout_seconds=self.lock_timeout_seconds,
            )
            state = self.store.capacity_transition_state()
            if state is None:
                raise CapacityTransitionExecutorError(
                    "rca_capacity_executor_state_missing"
                )
            state = transition.validate_persisted_capacity_state(state)
            if state["state"] not in {
                transition.BOOTSTRAP_PRODUCTION,
                transition.STEADY_ACTIVE,
            }:
                raise CapacityTransitionExecutorError(
                    "rca_capacity_executor_state_invalid"
                )
            activation = self.store.activation_epoch()
            if (
                activation is None
                or activation.get("epoch_id") != business_epoch_id
                or activation.get("state") != "steady_active"
            ):
                raise CapacityTransitionExecutorError(
                    "rca_capacity_executor_business_activation_not_steady"
                )
            intent_present = _artifact_presence(self.paths)["intent"]
            if not intent_present:
                if state["state"] != transition.BOOTSTRAP_PRODUCTION:
                    raise CapacityTransitionExecutorError(
                        "rca_capacity_executor_steady_intent_missing"
                    )
                if recovery_requested:
                    raise CapacityTransitionExecutorError(
                        "rca_capacity_executor_recovery_intent_missing"
                    )
                intent, authorization, receipt, marker = (
                    self._read_initial_authorization(
                        authorization_path=authorization_path,
                        ledger=ledger,
                        state=state,
                        epoch_id=business_epoch_id,
                        operator=actor,
                        reason=justification,
                        current=current,
                    )
                )
                recovery = False
            else:
                if not recovery_requested:
                    raise CapacityTransitionExecutorError(
                        "rca_capacity_executor_recover_command_required"
                    )
                intent, authorization, receipt, marker = self._read_recovery_intent(
                    authorization_path=authorization_path,
                    ledger=ledger,
                    epoch_id=business_epoch_id,
                    operator=actor,
                    reason=justification,
                    current=current,
                )
                recovery = True
            expected_generation = int(intent["expected_generation"])
            target_generation = int(intent["target_generation"])
            if state["state"] == transition.BOOTSTRAP_PRODUCTION:
                generation_valid = int(state["generation"]) == expected_generation
            else:
                generation_valid = int(state["generation"]) == target_generation
            if (
                not generation_valid
                or state["release_id"] != intent["ratchet_origin_release_id"]
                or state["bootstrap_epoch_id"]
                != intent["ratchet_origin_bootstrap_epoch_id"]
            ):
                raise CapacityTransitionExecutorError(
                    "rca_capacity_executor_state_binding_invalid"
                )
            intent_raw = transition.canonical_bytes(intent)
            authorization_raw = transition.canonical_bytes(authorization)
            receipt_raw = transition.canonical_bytes(receipt)
            marker_raw = transition.canonical_bytes(marker)
            intent_sha = hashlib.sha256(intent_raw).hexdigest()
            authorization_sha = hashlib.sha256(authorization_raw).hexdigest()
            receipt_sha = hashlib.sha256(receipt_raw).hexdigest()
            marker_sha = hashlib.sha256(marker_raw).hexdigest()
            bundle = runtime.issue_evidence_bundle(
                release_id=str(intent["ratchet_origin_release_id"]),
                bootstrap_epoch_id=str(intent["ratchet_origin_bootstrap_epoch_id"]),
                target_generation=target_generation,
                sample_ledger_sha256=ledger.ledger_sha256,
                transition_intent_sha256=intent_sha,
                transition_intent_fingerprint=str(intent["intent_fingerprint"]),
                transition_authorization_sha256=authorization_sha,
                transition_authorization_fingerprint=str(
                    authorization["authorization_fingerprint"]
                ),
                transition_receipt_sha256=receipt_sha,
                transition_receipt_fingerprint=str(receipt["receipt_fingerprint"]),
                commit_marker_sha256=marker_sha,
                commit_marker_fingerprint=str(marker["marker_fingerprint"]),
                created_at=_timestamp(str(intent["created_at"])),
                hmac_key=self.hmac_key,
            )
            bundle_raw = transition.canonical_bytes(bundle)
            bundle_sha = hashlib.sha256(bundle_raw).hexdigest()
            summary = {
                "schema_version": EXECUTOR_SCHEMA_VERSION,
                "ok": True,
                "command": "recover" if recovery else "transition-steady",
                "applied": False,
                "recovery": recovery,
                "expected_generation": expected_generation,
                "target_generation": target_generation,
                "sample_ledger_sha256": ledger.ledger_sha256,
                "transition_intent_sha256": intent_sha,
                "transition_intent_fingerprint": intent["intent_fingerprint"],
                "operator_sha256": hashlib.sha256(actor.encode()).hexdigest(),
                "reason_sha256": hashlib.sha256(justification.encode()).hexdigest(),
                "business_activation_epoch_id": business_epoch_id,
                "artifact_presence": _artifact_presence(self.paths),
            }

            if not apply:
                return summary

            for prefix, path, artifact in (
                ("intent", self.paths.transition_intent, intent),
                (
                    "authorization",
                    self.paths.transition_authorization,
                    authorization,
                ),
                ("receipt", self.paths.transition_receipt, receipt),
                ("marker", self.paths.commit_marker, marker),
                ("bundle", self.paths.evidence_bundle, bundle),
            ):
                transition.publish_owner_only_no_clobber(path, artifact)
                self._fault(prefix)
            steady = self.store.compare_and_set_capacity_steady(
                expected_generation=expected_generation,
                release_id=str(intent["ratchet_origin_release_id"]),
                bootstrap_epoch_id=str(intent["ratchet_origin_bootstrap_epoch_id"]),
                final_ledger_sha256=ledger.ledger_sha256,
                transition_authorization_sha256=authorization_sha,
                transition_authorization_fingerprint=str(
                    authorization["authorization_fingerprint"]
                ),
                transition_receipt_sha256=receipt_sha,
                transition_receipt_fingerprint=str(receipt["receipt_fingerprint"]),
                commit_marker_sha256=marker_sha,
                commit_marker_fingerprint=str(marker["marker_fingerprint"]),
                evidence_bundle_sha256=bundle_sha,
                evidence_bundle_fingerprint=str(bundle["bundle_fingerprint"]),
                authorization_issued_at=str(authorization["issued_at"]),
                authorization_expires_at=str(authorization["expires_at"]),
                receipt_created_at=str(receipt["created_at"]),
                marker_committed_at=str(marker["committed_at"]),
                now=current,
            )
            self._fault("capacity_cas")
            resolver = runtime.CapacityRuntimeResolver(
                store=self.store,
                control_db_path=self.control_db_path,
                release_id=str(intent["ratchet_origin_release_id"]),
                bootstrap_epoch_id=str(intent["ratchet_origin_bootstrap_epoch_id"]),
                initial_policy="bootstrap",
                hmac_key=self.hmac_key,
                now=self.now,
                lock_timeout_seconds=self.lock_timeout_seconds,
            )
            decision = resolver._resolve_locked(lock_latency_ms=0.0)
            if decision["effective_state"] != transition.STEADY_ACTIVE:
                raise CapacityTransitionExecutorError(
                    "rca_capacity_executor_runtime_not_steady"
                )
            current_activation = self.store.activation_epoch()
            if (
                current_activation is None
                or current_activation.get("epoch_id") != business_epoch_id
                or current_activation.get("state") != "steady_active"
            ):
                raise CapacityTransitionExecutorError(
                    "rca_capacity_executor_business_activation_changed"
                )
            return {
                **summary,
                "applied": True,
                "capacity_state": steady["state"],
                "capacity_generation": steady["generation"],
                "runtime_effective_state": decision["effective_state"],
                "business_activation_state": current_activation["state"],
                "artifact_presence": _artifact_presence(self.paths),
            }

    def recover(self, *, apply: bool) -> dict[str, Any]:
        """Resume only from the durable signed intent; accept no new authority."""

        intent, _intent_sha = transition.read_owner_only_json(
            self.paths.transition_intent
        )
        return self.execute(
            epoch_id=str(intent.get("business_activation_epoch_id") or ""),
            operator=str(intent.get("operator") or ""),
            reason=str(intent.get("reason") or ""),
            authorization_path=None,
            apply=apply,
            recovery_requested=True,
        )


def _build_parser() -> argparse.ArgumentParser:
    parser = _SafeArgumentParser(description=__doc__)
    parser.add_argument("--control-db", type=Path, required=True)
    commands = parser.add_subparsers(dest="command", parser_class=_SafeArgumentParser)
    commands.add_parser("status")
    recover = commands.add_parser("recover")
    recover.add_argument("--apply", action="store_true")
    steady = commands.add_parser("transition-steady")
    steady.add_argument("--authorization", type=Path, required=True)
    steady.add_argument(
        "--business-activation-epoch-id",
        "--epoch-id",
        dest="business_activation_epoch_id",
        required=True,
    )
    steady.add_argument("--operator", required=True)
    steady.add_argument("--reason", required=True)
    steady.add_argument("--apply", action="store_true")
    return parser


def _safe_exception_code(exc: BaseException) -> str:
    if isinstance(
        exc,
        (
            CapacityTransitionExecutorError,
            transition.CapacityTransitionError,
            runtime.CapacityRuntimeError,
        ),
    ):
        return exc.code
    if isinstance(exc, CapacityTransitionStateError):
        code = str(exc)
        if SAFE_CODE_RE.fullmatch(code):
            return code
    return "rca_capacity_executor_internal_error"


def _emit(value: Mapping[str, Any]) -> None:
    print(
        json.dumps(
            dict(value),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    )


def main(argv: Sequence[str] | None = None) -> int:
    command = "unknown"
    try:
        args = _build_parser().parse_args(argv)
        command = str(args.command or "status")
        db_path = Path(args.control_db).expanduser()
        if not db_path.is_absolute():
            raise CapacityTransitionExecutorError(
                "rca_capacity_executor_control_db_not_absolute"
            )
        store = RcaControlStore(db_path, require_current=True)
        executor = SteadyCapacityTransitionExecutor(
            store=store,
            control_db_path=db_path,
            hmac_key=runtime.load_capacity_hmac_key(),
        )
        if command == "status":
            payload = executor.status()
        elif command == "recover":
            payload = executor.recover(apply=bool(args.apply))
        elif command == "transition-steady":
            authorization_path = (
                Path(args.authorization).expanduser()
                if args.authorization is not None
                else None
            )
            if authorization_path is not None and not authorization_path.is_absolute():
                raise CapacityTransitionExecutorError(
                    "rca_capacity_executor_authorization_not_absolute"
                )
            payload = executor.execute(
                epoch_id=args.business_activation_epoch_id,
                operator=args.operator,
                reason=args.reason,
                authorization_path=authorization_path,
                apply=bool(args.apply),
            )
        else:
            raise CapacityTransitionExecutorError(
                "rca_capacity_executor_command_invalid"
            )
    except SystemExit:
        raise
    except Exception as exc:
        _emit({
            "schema_version": EXECUTOR_SCHEMA_VERSION,
            "ok": False,
            "command": command,
            "code": _safe_exception_code(exc),
        })
        return 2
    _emit(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
