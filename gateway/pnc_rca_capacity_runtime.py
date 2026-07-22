"""Dynamic, fail-closed RCA production capacity runtime resolution."""

from __future__ import annotations

import base64
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import hmac
import os
from pathlib import Path
import re
import time
from typing import Any, Callable, Iterator, Mapping, Protocol

from gateway import pnc_rca_capacity_sample_evidence as sample_evidence
from gateway import pnc_rca_capacity_transition as transition


RUNTIME_DECISION_SCHEMA_VERSION = "pnc_rca_capacity_runtime_decision_v1"
EVIDENCE_BUNDLE_SCHEMA_VERSION = "pnc_rca_capacity_evidence_bundle_v2"
EVIDENCE_BUNDLE_HMAC_DOMAIN = b"hermes-rca-prod/capacity-evidence-bundle/v2\x00"
HMAC_ENV = "HERMES_RCA_PROD_ADMISSION_HMAC_KEY"
STATE_ROOT_NAME = "rca-capacity-transition"
GLOBAL_LOCK_NAME = "capacity.lock"
SAMPLE_LEDGER_NAME = "samples.jsonl"
TRANSITION_INTENT_NAME = "steady-intent.json"
TRANSITION_AUTHORIZATION_NAME = "steady-authorization.json"
TRANSITION_RECEIPT_NAME = "steady-receipt.json"
COMMIT_MARKER_NAME = "steady-marker.json"
EVIDENCE_BUNDLE_NAME = "evidence-bundle.json"
PRODUCER_ACTIVATION_NAME = "sample-producer-activation.json"
DEFAULT_LOCK_TIMEOUT_SECONDS = 5.0
HEX64_RE = re.compile(r"^[0-9a-f]{64}$")

EVIDENCE_BUNDLE_FIELDS = {
    "schema_version",
    "release_id",
    "bootstrap_epoch_id",
    "target_generation",
    "sample_ledger_sha256",
    "transition_intent_sha256",
    "transition_intent_fingerprint",
    "transition_authorization_sha256",
    "transition_authorization_fingerprint",
    "transition_receipt_sha256",
    "transition_receipt_fingerprint",
    "commit_marker_sha256",
    "commit_marker_fingerprint",
    "created_at",
    "bundle_fingerprint",
    "bundle_hmac_sha256",
}


class CapacityRuntimeError(RuntimeError):
    def __init__(self, code: str):
        self.code = str(code or "rca_capacity_runtime_invalid")[:120]
        super().__init__(self.code)


class CapacityStateStore(Protocol):
    def capacity_transition_state(self) -> dict[str, Any] | None: ...


@dataclass(frozen=True)
class CapacityRuntimePaths:
    state_root: Path
    global_lock: Path
    sample_ledger: Path
    transition_intent: Path
    transition_authorization: Path
    transition_receipt: Path
    commit_marker: Path
    evidence_bundle: Path
    producer_activation: Path

    @classmethod
    def from_control_db(cls, control_db_path: str | Path) -> "CapacityRuntimePaths":
        database = Path(control_db_path).expanduser().absolute()
        state_root = database.parent / STATE_ROOT_NAME
        return cls(
            state_root=state_root,
            global_lock=state_root / GLOBAL_LOCK_NAME,
            sample_ledger=state_root / SAMPLE_LEDGER_NAME,
            transition_intent=state_root / TRANSITION_INTENT_NAME,
            transition_authorization=state_root / TRANSITION_AUTHORIZATION_NAME,
            transition_receipt=state_root / TRANSITION_RECEIPT_NAME,
            commit_marker=state_root / COMMIT_MARKER_NAME,
            evidence_bundle=state_root / EVIDENCE_BUNDLE_NAME,
            producer_activation=state_root / PRODUCER_ACTIVATION_NAME,
        )


def _utc(value: datetime | None = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None or current.utcoffset() is None:
        raise CapacityRuntimeError("rca_capacity_runtime_time_invalid")
    return current.astimezone(timezone.utc)


def _format_timestamp(value: datetime) -> str:
    normalized = _utc(value)
    timespec = "microseconds" if normalized.microsecond else "seconds"
    return normalized.isoformat(timespec=timespec).replace("+00:00", "Z")


def _timestamp(value: Any) -> datetime:
    if not isinstance(value, str) or value != value.strip():
        raise CapacityRuntimeError("rca_capacity_evidence_bundle_time_invalid")
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(candidate)
    except (TypeError, ValueError, OverflowError) as exc:
        raise CapacityRuntimeError("rca_capacity_evidence_bundle_time_invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CapacityRuntimeError("rca_capacity_evidence_bundle_time_invalid")
    normalized = parsed.astimezone(timezone.utc)
    if value != _format_timestamp(normalized):
        raise CapacityRuntimeError("rca_capacity_evidence_bundle_time_invalid")
    return normalized


def _hex(value: Any, code: str) -> str:
    normalized = str(value or "").strip().lower()
    if not HEX64_RE.fullmatch(normalized):
        raise CapacityRuntimeError(code)
    return normalized


def load_capacity_hmac_key(raw: str | bytes | None = None) -> bytes:
    if isinstance(raw, bytes):
        if len(raw) < transition.MIN_HMAC_KEY_BYTES:
            raise CapacityRuntimeError("rca_capacity_runtime_hmac_key_invalid")
        return raw
    value = (raw if raw is not None else os.environ.get(HMAC_ENV, "")).strip()
    try:
        if value.startswith("hex:"):
            key = bytes.fromhex(value[4:])
        elif value.startswith("base64:"):
            key = base64.b64decode(value[7:], validate=True)
        else:
            raise ValueError
    except Exception as exc:
        raise CapacityRuntimeError("rca_capacity_runtime_hmac_key_invalid") from exc
    if len(key) < transition.MIN_HMAC_KEY_BYTES:
        raise CapacityRuntimeError("rca_capacity_runtime_hmac_key_invalid")
    return key


def _bundle_body(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: item
        for key, item in value.items()
        if key not in {"bundle_fingerprint", "bundle_hmac_sha256"}
    }


def issue_evidence_bundle(
    *,
    release_id: str,
    bootstrap_epoch_id: str,
    target_generation: int,
    sample_ledger_sha256: str,
    transition_intent_sha256: str,
    transition_intent_fingerprint: str,
    transition_authorization_sha256: str,
    transition_authorization_fingerprint: str,
    transition_receipt_sha256: str,
    transition_receipt_fingerprint: str,
    commit_marker_sha256: str,
    commit_marker_fingerprint: str,
    created_at: datetime,
    hmac_key: bytes,
) -> dict[str, Any]:
    bundle: dict[str, Any] = {
        "schema_version": EVIDENCE_BUNDLE_SCHEMA_VERSION,
        "release_id": release_id,
        "bootstrap_epoch_id": bootstrap_epoch_id,
        "target_generation": target_generation,
        "sample_ledger_sha256": sample_ledger_sha256,
        "transition_intent_sha256": transition_intent_sha256,
        "transition_intent_fingerprint": transition_intent_fingerprint,
        "transition_authorization_sha256": transition_authorization_sha256,
        "transition_authorization_fingerprint": (transition_authorization_fingerprint),
        "transition_receipt_sha256": transition_receipt_sha256,
        "transition_receipt_fingerprint": transition_receipt_fingerprint,
        "commit_marker_sha256": commit_marker_sha256,
        "commit_marker_fingerprint": commit_marker_fingerprint,
        "created_at": _format_timestamp(created_at),
    }
    body = transition.canonical_bytes(bundle)
    bundle["bundle_fingerprint"] = hashlib.sha256(body).hexdigest()
    bundle["bundle_hmac_sha256"] = hmac.new(
        load_capacity_hmac_key(hmac_key),
        EVIDENCE_BUNDLE_HMAC_DOMAIN + body,
        hashlib.sha256,
    ).hexdigest()
    return validate_evidence_bundle(
        bundle,
        hmac_key=hmac_key,
        now=created_at,
        expected_release_id=release_id,
        expected_bootstrap_epoch_id=bootstrap_epoch_id,
        expected_target_generation=target_generation,
        expected_sample_ledger_sha256=sample_ledger_sha256,
        expected_transition_intent_sha256=transition_intent_sha256,
        expected_transition_intent_fingerprint=transition_intent_fingerprint,
        expected_transition_authorization_sha256=(transition_authorization_sha256),
        expected_transition_authorization_fingerprint=(
            transition_authorization_fingerprint
        ),
        expected_transition_receipt_sha256=transition_receipt_sha256,
        expected_transition_receipt_fingerprint=transition_receipt_fingerprint,
        expected_commit_marker_sha256=commit_marker_sha256,
        expected_commit_marker_fingerprint=commit_marker_fingerprint,
    )


def validate_evidence_bundle(
    value: Any,
    *,
    hmac_key: bytes,
    now: datetime,
    expected_release_id: str,
    expected_bootstrap_epoch_id: str,
    expected_target_generation: int,
    expected_sample_ledger_sha256: str,
    expected_transition_intent_sha256: str,
    expected_transition_intent_fingerprint: str,
    expected_transition_authorization_sha256: str,
    expected_transition_authorization_fingerprint: str,
    expected_transition_receipt_sha256: str,
    expected_transition_receipt_fingerprint: str,
    expected_commit_marker_sha256: str,
    expected_commit_marker_fingerprint: str,
) -> dict[str, Any]:
    if (
        not isinstance(value, Mapping)
        or set(value) != EVIDENCE_BUNDLE_FIELDS
        or value.get("schema_version") != EVIDENCE_BUNDLE_SCHEMA_VERSION
    ):
        raise CapacityRuntimeError("rca_capacity_evidence_bundle_schema_invalid")
    if (
        isinstance(value.get("target_generation"), bool)
        or not isinstance(value.get("target_generation"), int)
        or value.get("target_generation") < 2
    ):
        raise CapacityRuntimeError("rca_capacity_evidence_bundle_generation_invalid")
    expected = {
        "release_id": str(expected_release_id),
        "bootstrap_epoch_id": str(expected_bootstrap_epoch_id),
        "target_generation": expected_target_generation,
        "sample_ledger_sha256": _hex(
            expected_sample_ledger_sha256,
            "rca_capacity_evidence_bundle_ledger_sha_invalid",
        ),
        "transition_intent_sha256": _hex(
            expected_transition_intent_sha256,
            "rca_capacity_evidence_bundle_intent_sha_invalid",
        ),
        "transition_intent_fingerprint": _hex(
            expected_transition_intent_fingerprint,
            "rca_capacity_evidence_bundle_intent_fingerprint_invalid",
        ),
        "transition_authorization_sha256": _hex(
            expected_transition_authorization_sha256,
            "rca_capacity_evidence_bundle_authorization_sha_invalid",
        ),
        "transition_authorization_fingerprint": _hex(
            expected_transition_authorization_fingerprint,
            "rca_capacity_evidence_bundle_authorization_fingerprint_invalid",
        ),
        "transition_receipt_sha256": _hex(
            expected_transition_receipt_sha256,
            "rca_capacity_evidence_bundle_receipt_sha_invalid",
        ),
        "transition_receipt_fingerprint": _hex(
            expected_transition_receipt_fingerprint,
            "rca_capacity_evidence_bundle_receipt_fingerprint_invalid",
        ),
        "commit_marker_sha256": _hex(
            expected_commit_marker_sha256,
            "rca_capacity_evidence_bundle_marker_sha_invalid",
        ),
        "commit_marker_fingerprint": _hex(
            expected_commit_marker_fingerprint,
            "rca_capacity_evidence_bundle_marker_fingerprint_invalid",
        ),
    }
    if any(value.get(field) != item for field, item in expected.items()):
        raise CapacityRuntimeError("rca_capacity_evidence_bundle_binding_invalid")
    created = _timestamp(value.get("created_at"))
    if created > _utc(now) + transition.MAX_CLOCK_SKEW:
        raise CapacityRuntimeError("rca_capacity_evidence_bundle_time_invalid")
    body = transition.canonical_bytes(_bundle_body(value))
    fingerprint = hashlib.sha256(body).hexdigest()
    signature = hmac.new(
        load_capacity_hmac_key(hmac_key),
        EVIDENCE_BUNDLE_HMAC_DOMAIN + body,
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(
        str(value.get("bundle_fingerprint") or ""), fingerprint
    ) or not hmac.compare_digest(str(value.get("bundle_hmac_sha256") or ""), signature):
        raise CapacityRuntimeError("rca_capacity_evidence_bundle_signature_invalid")
    return dict(value)


def _path_present(path: Path) -> bool:
    try:
        path.lstat()
        return True
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise CapacityRuntimeError("rca_capacity_artifact_stat_failed") from exc


@dataclass
class CapacityRuntimeResolver:
    store: CapacityStateStore
    control_db_path: Path
    release_id: str
    bootstrap_epoch_id: str
    initial_policy: str
    hmac_key: bytes | None = field(default=None, repr=False)
    configuration_error: str = ""
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc)
    lock_timeout_seconds: float = DEFAULT_LOCK_TIMEOUT_SECONDS

    def __post_init__(self) -> None:
        self.control_db_path = Path(self.control_db_path).expanduser().absolute()
        self.release_id = str(self.release_id or "").strip()
        self.bootstrap_epoch_id = str(self.bootstrap_epoch_id or "").strip()
        self.initial_policy = str(self.initial_policy or "").strip()
        if self.initial_policy not in {"steady", "bootstrap"}:
            raise CapacityRuntimeError("rca_capacity_runtime_initial_policy_invalid")
        if bool(self.release_id) != bool(self.bootstrap_epoch_id):
            raise CapacityRuntimeError("rca_capacity_runtime_release_binding_invalid")
        if not self.release_id and self.initial_policy != "steady":
            raise CapacityRuntimeError("rca_capacity_runtime_legacy_policy_invalid")
        self.paths = CapacityRuntimePaths.from_control_db(self.control_db_path)

    @property
    def configured(self) -> bool:
        return bool(self.release_id and self.bootstrap_epoch_id)

    @classmethod
    def from_environment(
        cls,
        *,
        store: CapacityStateStore,
        control_db_path: str | Path,
        release_id: str,
        bootstrap_epoch_id: str,
        initial_policy: str,
        env: Mapping[str, str] | None = None,
        now: Callable[[], datetime] | None = None,
        lock_timeout_seconds: float = DEFAULT_LOCK_TIMEOUT_SECONDS,
    ) -> "CapacityRuntimeResolver":
        source = os.environ if env is None else env
        key: bytes | None = None
        error = ""
        if release_id or bootstrap_epoch_id:
            try:
                key = load_capacity_hmac_key(source.get(HMAC_ENV, ""))
            except CapacityRuntimeError as exc:
                error = exc.code
        return cls(
            store=store,
            control_db_path=Path(control_db_path),
            release_id=release_id,
            bootstrap_epoch_id=bootstrap_epoch_id,
            initial_policy=initial_policy,
            hmac_key=key,
            configuration_error=error,
            now=now or (lambda: datetime.now(timezone.utc)),
            lock_timeout_seconds=lock_timeout_seconds,
        )

    def _legacy_decision(self) -> dict[str, Any]:
        return {
            "schema_version": RUNTIME_DECISION_SCHEMA_VERSION,
            "configured": False,
            "legacy_compatibility": True,
            "initial_policy": self.initial_policy,
            "effective_state": transition.STEADY_ACTIVE,
            "effective_mode": "steady",
            "generation": 0,
            "irreversible": False,
            "ready": True,
            "reason_code": "rca_capacity_legacy_steady_compatibility",
            "current_release_id": "",
            "current_bootstrap_epoch_id": "",
            "ratchet_origin_release_id": "",
            "ratchet_origin_bootstrap_epoch_id": "",
            "active_release_binding_sha256": "",
            "ledger": None,
            "artifacts": None,
            "lock": {"held": False, "latency_ms": 0.0, "error_code": ""},
        }

    def _blocked(
        self,
        code: str,
        *,
        lock_latency_ms: float,
        state: Mapping[str, Any] | None = None,
        ledger: transition.CapacityLedgerSnapshot | None = None,
        producer_activation: Mapping[str, Any] | None = None,
        lock_error: str = "",
    ) -> dict[str, Any]:
        irreversible = bool(
            isinstance(state, Mapping)
            and state.get("state") == transition.STEADY_ACTIVE
        )
        return self._decision(
            result={
                "state": transition.STEADY_BLOCKED,
                "capacity_mode": "blocked",
                "reason_code": code,
                "sample_count": ledger.sample_count if ledger is not None else 0,
                "ledger_sha256": ledger.ledger_sha256 if ledger is not None else None,
                "irreversible": irreversible,
            },
            state=state,
            ledger=ledger,
            producer_activation=producer_activation,
            artifacts=None,
            lock_latency_ms=lock_latency_ms,
            lock_error=lock_error,
        )

    def _decision(
        self,
        *,
        result: Mapping[str, Any],
        state: Mapping[str, Any] | None,
        ledger: transition.CapacityLedgerSnapshot | None,
        producer_activation: Mapping[str, Any] | None = None,
        artifacts: Mapping[str, Any] | None,
        lock_latency_ms: float,
        lock_error: str = "",
    ) -> dict[str, Any]:
        effective_state = str(result.get("state") or transition.STEADY_BLOCKED)
        return {
            "schema_version": RUNTIME_DECISION_SCHEMA_VERSION,
            "configured": True,
            "legacy_compatibility": False,
            "initial_policy": self.initial_policy,
            "effective_state": effective_state,
            "effective_mode": str(result.get("capacity_mode") or "blocked"),
            "generation": int(state.get("generation", 0)) if state else 0,
            "irreversible": bool(result.get("irreversible")),
            "ready": effective_state != transition.STEADY_BLOCKED,
            "reason_code": str(result.get("reason_code") or ""),
            "current_release_id": self.release_id,
            "current_bootstrap_epoch_id": self.bootstrap_epoch_id,
            "ratchet_origin_release_id": (
                str(state.get("release_id") or "") if state else ""
            ),
            "ratchet_origin_bootstrap_epoch_id": (
                str(state.get("bootstrap_epoch_id") or "") if state else ""
            ),
            "active_release_binding_sha256": (
                str(producer_activation.get("active_release_binding_sha256") or "")
                if producer_activation is not None
                else str(ledger.active_release_binding_sha256 or "")
                if ledger is not None
                else ""
            ),
            "ledger": (
                {
                    "sample_count": ledger.sample_count,
                    "sha256": ledger.ledger_sha256,
                    "window_seconds": ledger.window_seconds,
                    "max_gap_seconds": ledger.max_gap_seconds,
                    "first_observed_at": ledger.first_observed_at,
                    "last_observed_at": ledger.last_observed_at,
                    "steady_qualified": ledger.steady_qualified,
                }
                if ledger is not None
                else None
            ),
            "artifacts": dict(artifacts) if artifacts is not None else None,
            "lock": {
                "held": not bool(lock_error),
                "latency_ms": round(max(0.0, lock_latency_ms), 3),
                "error_code": lock_error,
            },
        }

    def _resolve_locked(self, *, lock_latency_ms: float) -> dict[str, Any]:
        current = _utc(self.now())
        if self.configuration_error:
            return self._blocked(
                self.configuration_error, lock_latency_ms=lock_latency_ms
            )
        if self.hmac_key is None:
            return self._blocked(
                "rca_capacity_runtime_hmac_key_invalid",
                lock_latency_ms=lock_latency_ms,
            )
        ledger: transition.CapacityLedgerSnapshot | None = None
        producer_activation: Mapping[str, Any] | None = None
        state: Mapping[str, Any] | None = None
        try:
            state = self.store.capacity_transition_state()
            if state is None:
                raise CapacityRuntimeError("rca_capacity_persisted_state_missing")
            state = transition.validate_persisted_capacity_state(state)
            if _path_present(self.paths.sample_ledger):
                ledger = transition.read_sample_ledger(
                    self.paths.sample_ledger,
                    hmac_key=self.hmac_key,
                    timeout_seconds=self.lock_timeout_seconds,
                )
            else:
                ledger = transition.validate_sample_ledger([], hmac_key=self.hmac_key)

            # The fixed producer receipt is the trust anchor before sample one.
            # Once the ledger exists, its immutable receipt and release bindings
            # must match the receipt bytes exactly.
            expected_producer_bindings: dict[str, str] = {}
            if ledger.sample_count:
                expected_producer_bindings = {
                    "expected_release_bom_sha256": str(
                        ledger.release_bom_sha256 or ""
                    ),
                    "expected_active_release_binding_sha256": str(
                        ledger.active_release_binding_sha256 or ""
                    ),
                }
            producer_activation, producer_activation_sha = (
                sample_evidence.read_and_validate_producer_activation(
                    self.paths.producer_activation,
                    hmac_key=self.hmac_key,
                    expected_release_id=str(state.get("release_id") or ""),
                    expected_bootstrap_epoch_id=str(
                        state.get("bootstrap_epoch_id") or ""
                    ),
                    **expected_producer_bindings,
                )
            )
            if ledger.sample_count and (
                ledger.samples[0]["producer_activation_receipt_sha256"]
                != producer_activation_sha
                or ledger.samples[0]["producer_activation_receipt_fingerprint"]
                != producer_activation["receipt_fingerprint"]
            ):
                raise CapacityRuntimeError(
                    "rca_capacity_runtime_producer_ledger_binding_invalid"
                )
            if isinstance(state, Mapping):
                state_is_steady = state.get("state") == transition.STEADY_ACTIVE
                if (
                    (
                        ledger.sample_count
                        and (
                            ledger.release_id != state.get("release_id")
                            or ledger.bootstrap_epoch_id
                            != state.get("bootstrap_epoch_id")
                        )
                    )
                    or (
                        not state_is_steady
                        and (
                            state.get("release_id") != self.release_id
                            or state.get("bootstrap_epoch_id")
                            != self.bootstrap_epoch_id
                        )
                    )
                ):
                    raise CapacityRuntimeError(
                        "rca_capacity_runtime_ledger_binding_invalid"
                    )
            present = {
                "intent": _path_present(self.paths.transition_intent),
                "authorization": _path_present(self.paths.transition_authorization),
                "receipt": _path_present(self.paths.transition_receipt),
                "marker": _path_present(self.paths.commit_marker),
                "bundle": _path_present(self.paths.evidence_bundle),
            }
            intent: Mapping[str, Any] | None = None
            authorization: Mapping[str, Any] | None = None
            receipt: Mapping[str, Any] | None = None
            marker: Mapping[str, Any] | None = None
            bundle_sha: str | None = None
            bundle_fingerprint: str | None = None
            artifacts: dict[str, Any] | None = None
            if (
                isinstance(state, Mapping)
                and state.get("state") == transition.STEADY_ACTIVE
            ):
                if not all(present.values()):
                    result = transition.resolve_effective_capacity(
                        ledger=ledger,
                        now=current,
                        hmac_key=self.hmac_key,
                        persisted_state=state,
                    )
                    return self._decision(
                        result=result,
                        state=state,
                        ledger=ledger,
                        producer_activation=producer_activation,
                        artifacts=None,
                        lock_latency_ms=lock_latency_ms,
                    )
                intent, intent_sha = transition.read_owner_only_json(
                    self.paths.transition_intent
                )
                authorization, authorization_sha = transition.read_owner_only_json(
                    self.paths.transition_authorization
                )
                receipt, receipt_sha = transition.read_owner_only_json(
                    self.paths.transition_receipt
                )
                marker, marker_sha = transition.read_owner_only_json(
                    self.paths.commit_marker
                )
                bundle, bundle_sha = transition.read_owner_only_json(
                    self.paths.evidence_bundle
                )
                validated_intent = transition.validate_steady_transition_intent(
                    intent,
                    ledger=ledger,
                    now=current,
                    hmac_key=self.hmac_key,
                    allow_historical=True,
                )
                if (
                    validated_intent["transition_authorization"] != authorization
                    or validated_intent["transition_receipt"] != receipt
                    or validated_intent["commit_marker"] != marker
                ):
                    raise CapacityRuntimeError(
                        "rca_capacity_runtime_intent_binding_invalid"
                    )
                validated_bundle = validate_evidence_bundle(
                    bundle,
                    hmac_key=self.hmac_key,
                    now=current,
                    expected_release_id=str(state.get("release_id") or ""),
                    expected_bootstrap_epoch_id=str(
                        state.get("bootstrap_epoch_id") or ""
                    ),
                    expected_target_generation=int(state.get("generation", 0)),
                    expected_sample_ledger_sha256=ledger.ledger_sha256,
                    expected_transition_intent_sha256=intent_sha,
                    expected_transition_intent_fingerprint=str(
                        intent.get("intent_fingerprint") or ""
                    ),
                    expected_transition_authorization_sha256=authorization_sha,
                    expected_transition_authorization_fingerprint=str(
                        authorization.get("authorization_fingerprint") or ""
                    ),
                    expected_transition_receipt_sha256=receipt_sha,
                    expected_transition_receipt_fingerprint=str(
                        receipt.get("receipt_fingerprint") or ""
                    ),
                    expected_commit_marker_sha256=marker_sha,
                    expected_commit_marker_fingerprint=str(
                        marker.get("marker_fingerprint") or ""
                    ),
                )
                if validated_bundle["created_at"] != validated_intent["created_at"]:
                    raise CapacityRuntimeError(
                        "rca_capacity_evidence_bundle_intent_binding_invalid"
                    )
                bundle_fingerprint = validated_bundle["bundle_fingerprint"]
                artifacts = {
                    "transition_intent_sha256": intent_sha,
                    "transition_intent_fingerprint": intent["intent_fingerprint"],
                    "transition_authorization_sha256": authorization_sha,
                    "transition_authorization_fingerprint": authorization[
                        "authorization_fingerprint"
                    ],
                    "transition_receipt_sha256": receipt_sha,
                    "transition_receipt_fingerprint": receipt["receipt_fingerprint"],
                    "commit_marker_sha256": marker_sha,
                    "commit_marker_fingerprint": marker["marker_fingerprint"],
                    "evidence_bundle_sha256": bundle_sha,
                    "evidence_bundle_fingerprint": bundle_fingerprint,
                }
            elif any(present.values()):
                intent = {}
            result = transition.resolve_effective_capacity(
                ledger=ledger,
                now=current,
                hmac_key=self.hmac_key,
                persisted_state=state,
                transition_intent=intent,
                transition_authorization=authorization,
                transition_receipt=receipt,
                commit_marker=marker,
                evidence_bundle_sha256=bundle_sha,
                evidence_bundle_fingerprint=bundle_fingerprint,
                bootstrap_production_authorized=self.initial_policy == "bootstrap",
            )
            return self._decision(
                result=result,
                state=state,
                ledger=ledger,
                producer_activation=producer_activation,
                artifacts=artifacts,
                lock_latency_ms=lock_latency_ms,
            )
        except (
            transition.CapacityTransitionError,
            sample_evidence.CapacitySampleEvidenceError,
            CapacityRuntimeError,
        ) as exc:
            return self._blocked(
                exc.code,
                lock_latency_ms=lock_latency_ms,
                state=state,
                ledger=ledger,
                producer_activation=producer_activation,
            )
        except Exception:
            return self._blocked(
                "rca_capacity_runtime_observation_failed",
                lock_latency_ms=lock_latency_ms,
                state=state,
                ledger=ledger,
                producer_activation=producer_activation,
            )

    @contextmanager
    def shared_decision(self) -> Iterator[dict[str, Any]]:
        if not self.configured:
            yield self._legacy_decision()
            return
        started = time.monotonic()
        lock_context = transition.capacity_flock(
            self.paths.global_lock,
            exclusive=False,
            timeout_seconds=self.lock_timeout_seconds,
        )
        try:
            lock_context.__enter__()
        except transition.CapacityTransitionError as exc:
            latency_ms = (time.monotonic() - started) * 1000
            yield self._blocked(
                exc.code,
                lock_latency_ms=latency_ms,
                lock_error=exc.code,
            )
            return
        try:
            latency_ms = (time.monotonic() - started) * 1000
            yield self._resolve_locked(lock_latency_ms=latency_ms)
        finally:
            lock_context.__exit__(None, None, None)

    def observe(self) -> dict[str, Any]:
        with self.shared_decision() as decision:
            return dict(decision)
