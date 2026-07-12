"""Unit tests for the three-lane fault taxonomy (pnc_fault_taxonomy).

Pins the classification that prevents the 7028467612 failure mode: a retryable
VM filesystem/permission fault must be ``infra_self_healable`` (self-heal +
retry / ops), NEVER ``needs_human_input`` (which @-pings an issue originator who
cannot fix a VM ownership error). Pure-function tests, no mocks — the real
control flow is exercised directly.
"""
from scripts import pnc_fault_taxonomy as tax


# --- the live incident -----------------------------------------------------

def test_7028467612_permission_fault_is_infra_not_human():
    """The exact live blocker that wrongly became need_input @originator."""
    blocker = {
        "kind": "translate_service_unavailable",
        "message": "mcap_data_translate 调用失败：PermissionError: [Errno 13] Permission denied: "
                   "'.../7028467612_fcw/_dt_work/converted/_runtime_config/g1q3_topics_full.txt'",
        "retryable": True,
    }
    assert tax.classify_blocker(blocker) == tax.INFRA_SELF_HEALABLE
    assert tax.is_self_healable(blocker) is True
    assert tax.needs_human_input(blocker) is False
    assert tax.is_retryable(blocker) is True


def test_new_workdir_permission_kind_is_infra():
    blocker = {"kind": "translate_workdir_permission", "retryable": True}
    assert tax.classify_blocker(blocker) == tax.INFRA_SELF_HEALABLE


# --- explicit fault_class wins ---------------------------------------------

def test_explicit_fault_class_overrides_kind():
    # Producer stamped an explicit class; honor it even if the kind would map
    # elsewhere.
    blocker = {"kind": "need_source_or_evidence", "fault_class": tax.INFRA_SELF_HEALABLE}
    assert tax.classify_blocker(blocker) == tax.INFRA_SELF_HEALABLE
    blocker2 = {"kind": "translate_service_unavailable", "fault_class": tax.HARD_DEFECT}
    assert tax.classify_blocker(blocker2) == tax.HARD_DEFECT


def test_unknown_explicit_fault_class_is_ignored():
    blocker = {"kind": "translate_workdir_permission", "fault_class": "bogus"}
    assert tax.classify_blocker(blocker) == tax.INFRA_SELF_HEALABLE


# --- human-input lane ------------------------------------------------------

def test_needs_human_input_kinds():
    for kind in ("need_source_or_evidence", "missing_frame_id", "data_address_missing"):
        assert tax.classify_blocker({"kind": kind}) == tax.NEEDS_HUMAN_INPUT, kind


# --- hard-defect lane ------------------------------------------------------

def test_hard_defect_kinds():
    for kind in ("translate_tool_missing", "invalid_schema_version", "request_not_visible_on_vm"):
        assert tax.classify_blocker({"kind": kind}) == tax.HARD_DEFECT, kind


# --- unknown-kind tie-breakers ---------------------------------------------

def test_unknown_kind_retryable_true_is_infra():
    assert tax.classify_blocker({"kind": "weird_new_thing", "retryable": True}) == tax.INFRA_SELF_HEALABLE


def test_unknown_kind_retryable_false_is_hard_defect():
    assert tax.classify_blocker({"kind": "weird_new_thing", "retryable": False}) == tax.HARD_DEFECT


def test_unknown_kind_no_retryable_falls_to_default():
    assert tax.classify_blocker({"kind": "weird_new_thing"}) == tax.NEEDS_HUMAN_INPUT
    # caller can choose to surface unknowns to ops instead
    assert tax.classify_blocker({"kind": "weird_new_thing"}, default=tax.HARD_DEFECT) == tax.HARD_DEFECT


# --- no structured blocker: gate-decision fallback -------------------------

def test_no_blocker_data_gate_is_human():
    for gate in ("ready_to_download", "need_evidence", "need_source_or_evidence", "requires_download"):
        assert tax.classify_blocker(None, gate_decision=gate) == tax.NEEDS_HUMAN_INPUT, gate


def test_no_blocker_no_gate_is_default():
    assert tax.classify_blocker(None) == tax.NEEDS_HUMAN_INPUT
    assert tax.classify_blocker({}, default=tax.HARD_DEFECT) == tax.HARD_DEFECT


def test_is_retryable_defaults_false():
    assert tax.is_retryable({"kind": "x"}) is False
    assert tax.is_retryable(None) is False
    assert tax.is_retryable({"kind": "x", "retryable": "true"}) is True


# --- remediation -----------------------------------------------------------

def test_remediation_for_permission_kind():
    rem = tax.remediation_for({"kind": "translate_workdir_permission"})
    assert rem and rem["op"] == "normalize_workdir_ownership"
    assert rem["resume_from_stage"] == "s3b_translate"


def test_remediation_explicit_wins():
    rem = tax.remediation_for({"kind": "translate_workdir_permission",
                               "remediation": {"op": "custom", "detail": "x"}})
    assert rem == {"op": "custom", "detail": "x"}


def test_remediation_none_for_human_kind():
    assert tax.remediation_for({"kind": "need_source_or_evidence"}) is None
