"""Single source of truth: classify a pipeline/datapipe blocker into one of three
fault lanes, so the runtime routes it correctly instead of collapsing every
non-success into a "the human must supply data" request.

Background (live incident 7028467612, 2026-06-26): the RCA pipeline downloaded
3.5GB and materialized the case, then s3b_translate hit a *filesystem* error —
``PermissionError: [Errno 13] ... _dt_work/converted/_runtime_config/g1q3_topics_full.txt``
— which the VM labelled ``translate_service_unavailable`` / ``retryable: true``.
The host then collapsed it to ``need_download`` → ``need_input`` and @-pinged the
issue originator to "补齐数据". But the data was already present; only a VM
ownership/permission fault blocked translate. The originator could never resolve
it, and the ping even degraded to "（未识别到发起人）". The task parked forever.

The fix is a third lane. Every blocker is exactly one of:

  - ``infra_self_healable``  — environment / ownership / permission / transient
    service faults the *system* can fix (chown + retry, wait + retry). NEVER ask
    the issue originator. Route to in-process retry / resume; if retries are
    exhausted, an OPS/infra alert — not the reporter.
  - ``needs_human_input``    — genuinely missing source data / evidence / business
    judgement. Only a human can resolve. Route to @originator (resolved robustly).
  - ``hard_defect``          — code / contract / tooling defect (missing tool,
    schema mismatch). Route to a Codex repair task / OPS alert, never the reporter.

The producer (the VM pipeline) SHOULD stamp an explicit ``fault_class`` on the
blocker dict. This module is the *consumer-side authority and safety net*: it
honors an explicit ``fault_class`` when present, else derives one from the blocker
``kind`` (with ``retryable`` as the tie-breaker for unknown kinds). It is pure
(no IO, no network, no state) so it is trivially unit-testable with real inputs —
no stubs that could mask the real control flow.
"""
from __future__ import annotations

from typing import Any

INFRA_SELF_HEALABLE = "infra_self_healable"
NEEDS_HUMAN_INPUT = "needs_human_input"
HARD_DEFECT = "hard_defect"

FAULT_CLASSES = frozenset({INFRA_SELF_HEALABLE, NEEDS_HUMAN_INPUT, HARD_DEFECT})

# Kinds the system can fix itself (retry, chown/normalize ownership, wait for a
# transient service). These must NOT reach the issue originator.
INFRA_SELF_HEALABLE_KINDS = frozenset({
    "translate_workdir_permission",   # NEW (VM): PermissionError/EACCES on the
                                      # pipeline's _dt_work translate working dir
    "translate_service_unavailable",  # docker/translate service down — retry/wait
    "mcap_chown_required",            # known recurring MCAP ownership fault
    "workdir_permission",
    "permission_denied",
    "case_dir_permission",
    "datapipe_timeout",
    "timeout",
})

# Genuinely needs a human to supply something the pipeline cannot produce.
NEEDS_HUMAN_INPUT_KINDS = frozenset({
    "need_source_or_evidence",
    "need_evidence",
    "need_data",
    "missing_frame_id",
    "frame_id_missing",
    "data_address_missing",
    "source_unreadable",
    "source_quality_insufficient",
})

# Code / contract / tooling defects. Triage to ops / Codex, never the reporter.
HARD_DEFECT_KINDS = frozenset({
    "translate_tool_missing",
    "reader_topic_mismatch",      # mcap_data_translate output has data but reader/tool contract read 0 frames
    "alignment_failed",           # downstream alignment/read contract failure; reporter cannot fix it
    "invalid_schema_version",
    "missing_request",
    "request_missing",
    "request_not_visible_on_vm",
    "schema_mismatch",
    "request_contract_drift",
})

# Gate decisions that, absent a structured blocker, indicate "still waiting on
# downloadable/parseable source" (a data/intake state, not a code defect).
_NON_GREEN_DATA_GATES = frozenset({
    "ready_to_download", "need_evidence", "need_source_or_evidence",
    "requires_download", "need_download",
})


def _as_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        low = value.strip().lower()
        if low in {"1", "true", "yes", "on"}:
            return True
        if low in {"0", "false", "no", "off"}:
            return False
    return None


def blocker_kind(blocker: Any) -> str:
    if not isinstance(blocker, dict):
        return ""
    return str(blocker.get("kind") or "").strip()


def is_retryable(blocker: Any) -> bool:
    """Whether a blocker advertises itself as retryable (default False)."""
    if not isinstance(blocker, dict):
        return False
    return _as_bool(blocker.get("retryable")) is True


def classify_blocker(
    blocker: Any,
    *,
    gate_decision: str = "",
    default: str = NEEDS_HUMAN_INPUT,
) -> str:
    """Return the fault lane for ``blocker``.

    Resolution order (most authoritative first):
      1. explicit ``blocker['fault_class']`` if it is a known lane
      2. ``blocker['kind']`` membership in the lane sets
      3. ``blocker['retryable']`` as a tie-breaker for an unknown kind
         (retryable True -> infra_self_healable, False -> hard_defect)
      4. no structured blocker: derive from ``gate_decision`` (a non-green data
         gate -> needs_human_input), else ``default``.

    ``default`` is what an unknown, unclassifiable blocker collapses to. It
    defaults to ``needs_human_input`` only so callers stay backwards compatible;
    callers that would rather surface unknowns to ops can pass
    ``default=HARD_DEFECT``.
    """
    if isinstance(blocker, dict) and blocker:
        explicit = str(blocker.get("fault_class") or "").strip()
        if explicit in FAULT_CLASSES:
            return explicit
        kind = str(blocker.get("kind") or "").strip()
        if kind in INFRA_SELF_HEALABLE_KINDS:
            return INFRA_SELF_HEALABLE
        if kind in HARD_DEFECT_KINDS:
            return HARD_DEFECT
        if kind in NEEDS_HUMAN_INPUT_KINDS:
            return NEEDS_HUMAN_INPUT
        retryable = _as_bool(blocker.get("retryable"))
        if retryable is True:
            return INFRA_SELF_HEALABLE
        if retryable is False:
            return HARD_DEFECT
        # structured blocker, unknown kind, no retryable signal
        return default
    gate = str(gate_decision or "").strip().lower()
    if gate in _NON_GREEN_DATA_GATES:
        return NEEDS_HUMAN_INPUT
    return default


def is_self_healable(blocker: Any, *, gate_decision: str = "") -> bool:
    return classify_blocker(blocker, gate_decision=gate_decision) == INFRA_SELF_HEALABLE


def needs_human_input(blocker: Any, *, gate_decision: str = "") -> bool:
    return classify_blocker(blocker, gate_decision=gate_decision) == NEEDS_HUMAN_INPUT


def is_hard_defect(blocker: Any, *, gate_decision: str = "") -> bool:
    return classify_blocker(blocker, gate_decision=gate_decision) == HARD_DEFECT


# Remediation playbooks for self-healable kinds — consumed by the VM self-heal
# orchestrator (to act) and the host ops alert (to describe what is being done).
_REMEDIATION = {
    "translate_workdir_permission": {
        "op": "normalize_workdir_ownership",
        "detail": "chown/normalize the pipeline-owned _dt_work translate subtree, then retry s3b_translate",
        "resume_from_stage": "s3b_translate",
    },
    "mcap_chown_required": {
        "op": "normalize_workdir_ownership",
        "detail": "chown the MCAP case workdir to the pipeline uid, then retry",
        "resume_from_stage": "s3b_translate",
    },
    "translate_service_unavailable": {
        "op": "wait_and_retry",
        "detail": "translate service/container transiently unavailable; backoff and retry",
        "resume_from_stage": "s3b_translate",
    },
}


def remediation_for(blocker: Any) -> dict[str, Any] | None:
    """Return the remediation playbook for a self-healable blocker, else None.

    Honors an explicit ``blocker['remediation']`` when the producer supplied one;
    otherwise looks the kind up in the built-in playbook table.
    """
    if not isinstance(blocker, dict):
        return None
    explicit = blocker.get("remediation")
    if isinstance(explicit, dict) and explicit:
        return explicit
    return _REMEDIATION.get(str(blocker.get("kind") or "").strip())
