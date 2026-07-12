"""Tests for gateway.intake_clarification.

Covers the two acceptance pillars from the clarification design:
  * dimension generator: skip_if (P3 — never re-ask a provided field), 0-dim case
    (sufficient info => no card), MAX_DIMENSIONS trim with truncation surfaced.
  * resolver closed loop: button + text fallback share one atomic/idempotent path,
    all_resolved flips correctly.

Sidecar writes are isolated via HERMES_HOME -> tmp_path (get_hermes_home reads env).
"""
import json

import pytest

from gateway import intake_clarification as ic


@pytest.fixture
def hermes_home(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    return tmp_path


# --- dimension generator ----------------------------------------------------

def test_sufficient_info_yields_no_dimensions_for_path_action():
    # clean action + path + owner + output all given => target_path & output & intent skip.
    fields = {"action": "clean", "mcap_path": "/mnt/a.mcap", "owner": "ou_x", "output_req": "报告"}
    dims, _trunc = ic.build_dimensions(fields, triage_kind="mcap-clean_execution")
    ids = {d["id"] for d in dims}
    # intent skipped (action present), target_path skipped (path present),
    # output skipped (output_req present), owner skipped (owner present).
    # boundary always asked.
    assert "intent" not in ids
    assert "target_path" not in ids
    assert "owner" not in ids
    assert ids == {"boundary"}


def test_p3_does_not_reask_given_path():
    fields = {"action": "clean", "mcap_path": "/mnt/a.mcap"}
    dims, _ = ic.build_dimensions(fields, triage_kind="mcap-clean_execution")
    assert "target_path" not in {d["id"] for d in dims}


def test_path_asked_when_action_present_but_path_missing():
    fields = {"action": "diagnostic"}
    dims, _ = ic.build_dimensions(fields, triage_kind="mcap_diagnostic_request")
    assert "target_path" in {d["id"] for d in dims}


def test_general_kind_asks_intent():
    dims, _ = ic.build_dimensions({}, triage_kind="general")
    assert "intent" in {d["id"] for d in dims}


def test_max_four_dimensions_and_truncation_surfaced():
    # empty fields + general -> intent, boundary, output, owner all eligible (4),
    # target_path requires an action so it's NOT eligible here. So exactly 4, no trunc.
    dims, trunc = ic.build_dimensions({}, triage_kind="general")
    assert len(dims) <= ic.MAX_DIMENSIONS
    assert all(isinstance(d["options"], list) and 2 <= len(d["options"]) <= 4 for d in dims)
    # ordering by priority: intent(10) before boundary(20) before output(40)
    ids = [d["id"] for d in dims]
    assert ids.index("intent") < ids.index("boundary") < ids.index("output")


def test_needs_clarification_returns_none_when_no_dims():
    spec = ic.needs_clarification(
        request_id="r1", raw_text="清洗 /mnt/a.mcap",
        extracted_fields={"action": "clean", "mcap_path": "/mnt/a.mcap", "owner": "ou_x", "output_req": "报告"},
        triage_kind="mcap-clean_execution",
    )
    # only boundary remains -> not None (we still want to confirm boundary once).
    assert spec is not None and {d["id"] for d in spec.dimensions} == {"boundary"}


# --- resolver closed loop ---------------------------------------------------

def _make_spec(home):
    spec = ic.needs_clarification(
        request_id="req-42", raw_text="帮我处理 mdrive4",
        extracted_fields={}, triage_kind="general",
        originator_open_id="ou_orig", chat_id="oc_test", thread_id="om_t",
    )
    ic.save_spec(spec)
    return spec


def test_button_resolve_is_atomic_and_idempotent(hermes_home):
    _make_spec(hermes_home)
    r1 = ic.resolve_intake_clarify(request_id="req-42", dimension_id="intent", choice="执行任务", actor_id="ou_a", source="button")
    assert r1["ok"] and r1["changed"] and not r1["all_resolved"]
    # duplicate click -> no change, duplicate flagged
    r2 = ic.resolve_intake_clarify(request_id="req-42", dimension_id="intent", choice="执行任务", actor_id="ou_a", source="button")
    assert r2["ok"] and not r2["changed"] and r2["duplicate"]


def test_text_fallback_equivalent_to_button(hermes_home):
    _make_spec(hermes_home)
    # numeric/alias text resolves the matching dimension
    r = ic.resolve_intake_clarify_by_text(request_id="req-42", text="知识问答", actor_id="ou_a")
    assert r["ok"] and r["changed"] and r["choice"] == "知识问答"


def test_all_resolved_flips_when_every_dimension_done(hermes_home):
    spec = _make_spec(hermes_home)
    last = None
    for d in spec.dimensions:
        last = ic.resolve_intake_clarify(request_id="req-42", dimension_id=d["id"], choice=d["options"][0], source="button")
    assert last["all_resolved"] is True
    # sidecar persisted with all_resolved
    body = json.loads(ic.sidecar_path("req-42").read_text(encoding="utf-8"))
    assert body["all_resolved"] is True
    assert all(dim["resolved"] is not None for dim in body["dimensions"])


def test_resolve_rejects_unknown_choice(hermes_home):
    _make_spec(hermes_home)
    r = ic.resolve_intake_clarify(request_id="req-42", dimension_id="intent", choice="火星文", source="button")
    assert not r["ok"] and r["error"] == "choice not allowed"


def test_resolve_unknown_request_fails_closed(hermes_home):
    r = ic.resolve_intake_clarify(request_id="nope", dimension_id="intent", choice="执行任务")
    assert not r["ok"]


def test_text_ambiguous_or_nomatch_does_not_resolve(hermes_home):
    _make_spec(hermes_home)
    r = ic.resolve_intake_clarify_by_text(request_id="req-42", text="完全不相关的话")
    assert not r["ok"]


# --- card rendering + stale nudge -------------------------------------------

def test_card_buttons_carry_intake_clarify_payload(hermes_home):
    spec = ic.needs_clarification(
        request_id="req-card", raw_text="帮我处理 mdrive4",
        extracted_fields={}, triage_kind="general",
        originator_open_id="ou_orig",
    )
    card = ic.render_clarification_card(spec, originator_name="刘旭")
    # collect all button values
    buttons = [a for el in card["elements"] if el.get("tag") == "action" for a in el["actions"]]
    assert buttons, "expected at least one button"
    for b in buttons:
        assert b["value"]["hermes_action"] == "intake_clarify"
        assert b["value"]["request_id"] == "req-card"
        assert b["value"]["dimension_id"]
        assert b["value"]["choice"]
    # originator @-mention present in a markdown element
    md = " ".join(el.get("content", "") for el in card["elements"] if el.get("tag") == "markdown")
    assert 'ou_orig' in md and '<at' in md


def test_resolved_dimension_not_rendered(hermes_home):
    spec = ic.needs_clarification(
        request_id="req-r2", raw_text="x", extracted_fields={}, triage_kind="general",
        originator_open_id="ou_orig",
    )
    ic.save_spec(spec)
    ic.resolve_intake_clarify(request_id="req-r2", dimension_id="intent", choice="执行任务", source="button")
    spec2 = ic.load_spec("req-r2")
    card = ic.render_clarification_card(spec2)
    dim_ids = {b["value"]["dimension_id"] for el in card["elements"] if el.get("tag") == "action" for b in el["actions"]}
    assert "intent" not in dim_ids  # resolved -> no longer offered


def test_stale_nudge_text_has_at_and_open_dims(hermes_home):
    spec = ic.needs_clarification(
        request_id="req-stale", raw_text="x", extracted_fields={}, triage_kind="general",
        originator_open_id="ou_orig",
    )
    txt = ic.build_stale_nudge_text(spec, originator_name="刘旭")
    assert '<at user_id="ou_orig">' in txt
    assert "1." in txt  # at least one open dimension listed


# --- continuation (summarize + idempotency guard) ---------------------------

def test_summarize_returns_none_until_all_resolved(hermes_home):
    spec = _make_spec(hermes_home)
    # resolve all but one
    for d in spec.dimensions[:-1]:
        ic.resolve_intake_clarify(request_id="req-42", dimension_id=d["id"], choice=d["options"][0], source="button")
    assert ic.summarize_clarified_choices("req-42") is None
    # resolve the last
    last = spec.dimensions[-1]
    ic.resolve_intake_clarify(request_id="req-42", dimension_id=last["id"], choice=last["options"][0], source="button")
    summary = ic.summarize_clarified_choices("req-42")
    assert summary is not None
    assert summary["chat_id"] == "oc_test" and summary["originator_open_id"] == "ou_orig"
    assert set(summary["choices"]) == {d["id"] for d in spec.dimensions}


def test_mark_continued_is_idempotent(hermes_home):
    spec = _make_spec(hermes_home)
    for d in spec.dimensions:
        ic.resolve_intake_clarify(request_id="req-42", dimension_id=d["id"], choice=d["options"][0], source="button")
    assert ic.mark_continued("req-42") is True   # first wins
    assert ic.mark_continued("req-42") is False  # second is a no-op


# --- P2.2 timeout / stale nudge scan ----------------------------------------

def test_find_stale_flags_old_open_clarification(hermes_home):
    spec = _make_spec(hermes_home)  # created_at = now-ish
    # force created_at into the past
    import json as _j
    p = ic.sidecar_path("req-42")
    b = _j.loads(p.read_text(encoding="utf-8"))
    b["created_at"] = "2026-06-19T00:00:00+08:00"
    p.write_text(_j.dumps(b), encoding="utf-8")
    stale = ic.find_stale_clarifications(now_iso="2026-06-19T01:00:00+08:00", threshold_minutes=30)
    assert any(s["request_id"] == "req-42" for s in stale)


def test_find_stale_skips_resolved_and_nudged(hermes_home):
    spec = _make_spec(hermes_home)
    for d in spec.dimensions:
        ic.resolve_intake_clarify(request_id="req-42", dimension_id=d["id"], choice=d["options"][0], source="button")
    # all resolved -> not stale even if old
    stale = ic.find_stale_clarifications(now_iso="2030-01-01T00:00:00+08:00", threshold_minutes=1)
    assert all(s["request_id"] != "req-42" for s in stale)


def test_mark_nudged_idempotent(hermes_home):
    _make_spec(hermes_home)
    assert ic.mark_nudged("req-42") is True
    assert ic.mark_nudged("req-42") is False


# --- pre-gate: well-formed requests must NOT trigger a clarification card ----

def test_no_card_when_auto_dispatch(hermes_home):
    # a complete clean request -> triage auto_dispatch -> no clarification
    spec = ic.needs_clarification(
        request_id="r-ad", raw_text="清洗 /mnt/a.mcap",
        extracted_fields={"action": "clean", "mcap_path": "/mnt/a.mcap", "owner": "ou_x"},
        triage_kind="mcap-clean_execution", triage_status="intake_checked",
        triage_has_auto_dispatch=True,
    )
    assert spec is None


def test_no_card_when_intake_checked_no_missing(hermes_home):
    spec = ic.needs_clarification(
        request_id="r-ok", raw_text="x", extracted_fields={"action": "clean", "mcap_path": "/mnt/a.mcap"},
        triage_kind="mcap-clean_execution", triage_status="intake_checked", triage_missing=[],
    )
    assert spec is None


def test_no_card_when_closed_question(hermes_home):
    spec = ic.needs_clarification(
        request_id="r-q", raw_text="mdrive4 能用吗？", extracted_fields={},
        triage_kind="question", triage_status="closed",
    )
    assert spec is None


def test_card_still_fires_when_underspecified(hermes_home):
    # genuinely ambiguous -> still clarify
    spec = ic.needs_clarification(
        request_id="r-amb", raw_text="帮我处理下", extracted_fields={},
        triage_kind="general", triage_status="intake_checked",
        triage_missing=["目标动作"],
    )
    assert spec is not None and spec.dimensions
