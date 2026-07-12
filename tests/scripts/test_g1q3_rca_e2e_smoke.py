#!/usr/bin/env python3
"""Unit tests for scripts/g1q3_rca_e2e_smoke.py — pure logic only (no VM).

Covers the preflight gate decisions and the §S5.5 green-check judging, including
the honesty negative case (status washed to completed must fail).
"""
from __future__ import annotations

import importlib.util
import hashlib
import json
import os
import sys
from pathlib import Path
from unittest import mock

import pytest

REPO = Path(__file__).resolve().parents[2]
MOD_PATH = REPO / "scripts" / "g1q3_rca_e2e_smoke.py"

spec = importlib.util.spec_from_file_location("g1q3_rca_e2e_smoke", MOD_PATH)
smoke = importlib.util.module_from_spec(spec)
sys.modules["g1q3_rca_e2e_smoke"] = smoke
spec.loader.exec_module(smoke)


# ---- quota headroom ----
def test_quota_blocks_when_disabled(tmp_path, monkeypatch):
    monkeypatch.delenv("HERMES_G1Q3_AUTO_DOWNLOAD_DAILY_QUOTA", raising=False)
    ok, note = smoke._quota_has_headroom(tmp_path)
    assert not ok and "auto_download_disabled" in note


def test_quota_blocks_when_exhausted(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_G1Q3_AUTO_DOWNLOAD_DAILY_QUOTA", "2")
    from datetime import datetime, timezone
    day = datetime.now(timezone.utc).date().isoformat()
    qdir = tmp_path / "pnc_agent" / "quota"
    qdir.mkdir(parents=True)
    (qdir / f"g1q3_auto_download-{day}.json").write_text('{"used": 2, "grants": []}')
    ok, note = smoke._quota_has_headroom(tmp_path)
    assert not ok and "exhausted" in note


def test_quota_headroom_ok(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_G1Q3_AUTO_DOWNLOAD_DAILY_QUOTA", "50")
    ok, note = smoke._quota_has_headroom(tmp_path)
    assert ok and "headroom" in note


# ---- relay crash-loop detection ----
def test_relay_crashloop_detected(monkeypatch):
    outputs = iter([
        "99348\t1\tlocal.pnc.completion-notice-relay\n",
        "50564\t1\tlocal.pnc.completion-notice-relay\n",  # pid churned
    ])
    def fake_run(cmd, **kw):
        return mock.Mock(stdout=next(outputs), returncode=0)
    monkeypatch.setattr(smoke.subprocess, "run", fake_run)
    monkeypatch.setattr(smoke.time, "sleep", lambda *_: None)
    ok, note = smoke._relay_not_crashlooping()
    assert not ok and "CRASH-LOOP" in note


def test_relay_stable_ok(monkeypatch):
    def fake_run(cmd, **kw):
        return mock.Mock(stdout="26277\t0\tlocal.pnc.completion-notice-relay\n", returncode=0)
    monkeypatch.setattr(smoke.subprocess, "run", fake_run)
    monkeypatch.setattr(smoke.time, "sleep", lambda *_: None)
    ok, note = smoke._relay_not_crashlooping()
    assert ok and "stable" in note


def test_relay_absent_is_ok(monkeypatch):
    def fake_run(cmd, **kw):
        return mock.Mock(stdout="1234\t0\tsomething.else\n", returncode=0)
    monkeypatch.setattr(smoke.subprocess, "run", fake_run)
    monkeypatch.setattr(smoke.time, "sleep", lambda *_: None)
    ok, note = smoke._relay_not_crashlooping()
    assert ok and "not loaded" in note


# ---- green checks judging (mock VM reads) ----
def _install_vm_stubs(monkeypatch, *, gate, report, index_bytes, report_bytes, exit_code):
    def fake_read_json(path, lines=800, timeout=45.0):
        if path.endswith("gate_result.json"):
            return gate
        if path.endswith("report_data.json"):
            return report
        return None
    monkeypatch.setattr(smoke, "_vm_read_json", fake_read_json)
    monkeypatch.setattr(smoke, "_vm_stat_size",
                        lambda p: index_bytes if p.endswith("index.html") else report_bytes)
    monkeypatch.setattr(smoke, "_vm_read_text", lambda p: exit_code)


def _good_gate():
    return {"gates": [{"gate": f"G{i}", "status": "pass"} for i in range(7)]}


def test_green_checks_all_pass(monkeypatch):
    report = {
        "summary": {"status": "hypothesis_ready",
                    "ui_banner_title": "候选归因待人工复核",
                    "high_confidence_boundary": "非高置信自动归因；当前候选仍需人工 review"},
        "field_lineage": {"manifest_decoded_dropped": [], "fidelity_ok": True},
    }
    _install_vm_stubs(monkeypatch, gate=_good_gate(), report=report,
                      index_bytes=7_000_000, report_bytes=17_000_000, exit_code="0")
    c = smoke.green_checks("/cases/7041712812_acc", "/mnt/tmp/x/")
    assert c["1_gates_all_pass"]["ok"]
    assert c["2_status_honest"]["ok"]
    assert c["3_manifest_decoded_dropped_empty"]["ok"]
    assert c["4_artifacts_exit0"]["ok"]
    assert c["5_card_delivery"]["ok"] is None  # headless boundary


def test_green_check_status_washed_to_completed_fails(monkeypatch):
    # hypothesis_ready (low confidence) but banner claims 已完成/已定责 -> dishonest -> FAIL
    report = {
        "summary": {"status": "hypothesis_ready", "ui_banner_title": "已完成，已定责",
                    "high_confidence_boundary": ""},
        "field_lineage": {"manifest_decoded_dropped": []},
    }
    _install_vm_stubs(monkeypatch, gate=_good_gate(), report=report,
                      index_bytes=7_000_000, report_bytes=17_000_000, exit_code="0")
    c = smoke.green_checks("/cases/x", "/mnt/tmp/x/")
    assert not c["2_status_honest"]["ok"]


def test_green_check_dropped_fields_fails(monkeypatch):
    report = {
        "summary": {"status": "report_ready"},
        "field_lineage": {"manifest_decoded_dropped": ["ego.speed"]},  # non-empty -> false-green suspect
    }
    _install_vm_stubs(monkeypatch, gate=_good_gate(), report=report,
                      index_bytes=7_000_000, report_bytes=17_000_000, exit_code="0")
    c = smoke.green_checks("/cases/x", "/mnt/tmp/x/")
    assert not c["3_manifest_decoded_dropped_empty"]["ok"]


def test_green_check_gate_not_all_pass_fails(monkeypatch):
    gate = {"gates": [{"gate": "G0", "status": "pass"}, {"gate": "G3", "status": "fail"}]}
    report = {"summary": {"status": "report_ready"}, "field_lineage": {"manifest_decoded_dropped": []}}
    _install_vm_stubs(monkeypatch, gate=gate, report=report,
                      index_bytes=1, report_bytes=1, exit_code="0")
    c = smoke.green_checks("/cases/x", "/mnt/tmp/x/")
    assert not c["1_gates_all_pass"]["ok"]


def test_green_check_nonzero_exit_fails(monkeypatch):
    report = {"summary": {"status": "report_ready"}, "field_lineage": {"manifest_decoded_dropped": []}}
    _install_vm_stubs(monkeypatch, gate=_good_gate(), report=report,
                      index_bytes=7_000_000, report_bytes=17_000_000, exit_code="1")
    c = smoke.green_checks("/cases/x", "/mnt/tmp/x/")
    assert not c["4_artifacts_exit0"]["ok"]


# ---- candidate --no-dispatch isolation and audit ----
def _make_candidate_roots(tmp_path, monkeypatch, *, run_id="run-0001"):
    base = tmp_path.resolve() / "candidate-sandboxes"
    run_root = base / "hermes-v0182" / run_id
    roots = {
        "case_root": run_root / "case",
        "artifact_root": run_root / "artifact",
        "shared_state_root": run_root / "shared-state",
        "output_root": run_root / "output",
        "work_root": run_root / "work",
        "download_root": run_root / "downloads",
    }
    for path in roots.values():
        path.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(smoke, "NO_DISPATCH_ALLOWED_BASES", (base,))
    return run_id, run_root, roots


def _no_dispatch_argv(run_id, roots):
    argv = ["--no-dispatch", "--run-id", run_id, "--json"]
    for name in smoke.NO_DISPATCH_ROOT_FIELDS:
        argv.extend([f"--{name.replace('_', '-')}", str(roots[name])])
    return argv


def test_no_dispatch_records_only_isolated_non_authorizing_evidence(tmp_path, monkeypatch):
    run_id, _run_root, roots = _make_candidate_roots(tmp_path, monkeypatch)
    result = smoke.record_no_dispatch(
        issue_url=smoke.DEFAULT_ISSUE_URL,
        work_item=smoke.DEFAULT_WORK_ITEM,
        requester=smoke.DEFAULT_REQUESTER,
        run_id=run_id,
        **roots,
    )

    assert result["record_only_completed"] is True
    assert result["offline_check_passed"] is True
    assert result["ok"] is False
    assert result["gate_decision"] == "NO_GO"
    for key in (
        "dispatch_attempted",
        "download_attempted",
        "feishu_contact_attempted",
        "vm_or_ssh_attempted",
        "network_attempted",
        "production_shared_state_write_attempted",
        "execution_authorized",
        "cutover_authorized",
    ):
        assert result[key] is False

    request_path = Path(result["records"]["execution_request"]["path"])
    state_path = Path(result["records"]["isolated_shared_state"]["path"])
    audit_path = Path(result["records"]["audit"]["path"])
    request = json.loads(request_path.read_text(encoding="utf-8"))
    state = json.loads(state_path.read_text(encoding="utf-8"))
    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    assert request["execution_policy"]["dispatch_allowed"] is False
    assert request["execution_policy"]["download_allowed"] is False
    assert request["execution_policy"]["feishu_write_allowed"] is False
    assert state["state"] == "recorded_not_dispatched"
    assert state["production_state"] is False
    assert audit["authorization"]["gate_decision"] == "NO_GO"
    assert audit["enforcement"]["preflight_called"] is False
    assert audit["enforcement"]["gateway_run_imported"] is False
    assert all(audit["enforcement"]["read_only_roots_unchanged"].values())
    assert hashlib.sha256(audit_path.read_bytes()).hexdigest() == result["records"]["audit"]["sha256"]
    assert smoke.DEFAULT_REQUESTER not in request_path.read_text(encoding="utf-8")
    assert list(roots["case_root"].iterdir()) == []
    assert list(roots["work_root"].iterdir()) == []
    assert list(roots["download_root"].iterdir()) == []


def test_no_dispatch_main_never_calls_live_or_external_surfaces(tmp_path, monkeypatch, capsys):
    run_id, _run_root, roots = _make_candidate_roots(tmp_path, monkeypatch)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("external/live surface must not be reached")

    monkeypatch.setattr(smoke, "preflight", forbidden)
    monkeypatch.setattr(smoke, "trigger", forbidden)
    monkeypatch.setattr(smoke, "watch", forbidden)
    monkeypatch.setattr(smoke, "find_case_dir", forbidden)
    monkeypatch.setattr(smoke.subprocess, "run", forbidden)

    assert smoke.main(_no_dispatch_argv(run_id, roots)) == 2
    body = json.loads(capsys.readouterr().out)
    assert body["record_only_completed"] is True
    assert body["gate_decision"] == "NO_GO"
    assert body["dispatch_attempted"] is False


def test_no_dispatch_requires_every_explicit_root_before_side_effects(monkeypatch, capsys):
    monkeypatch.setattr(
        smoke,
        "preflight",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("preflight must not run")
        ),
    )
    assert smoke.main(["--no-dispatch", "--run-id", "run-0001", "--json"]) == 3
    body = json.loads(capsys.readouterr().out)
    assert body["error"]["code"] == "G1Q3_NO_DISPATCH_ROOTS_REQUIRED"
    assert set(body["error"]["missing"]) == set(smoke.NO_DISPATCH_ROOT_FIELDS)
    assert body["dispatch_attempted"] is False


def test_no_dispatch_rejects_root_outside_candidate_base(tmp_path, monkeypatch):
    run_id, _run_root, _roots = _make_candidate_roots(tmp_path, monkeypatch)
    outside_run = tmp_path.resolve() / "candidate-sandboxes-evil" / run_id
    outside_roots = {
        "case_root": outside_run / "case",
        "artifact_root": outside_run / "artifact",
        "shared_state_root": outside_run / "shared-state",
        "output_root": outside_run / "output",
        "work_root": outside_run / "work",
        "download_root": outside_run / "downloads",
    }
    for path in outside_roots.values():
        path.mkdir(parents=True, exist_ok=True)
    with pytest.raises(smoke.NoDispatchIsolationError, match="run-root must be below"):
        smoke.validate_no_dispatch_roots(run_id=run_id, **outside_roots)


def test_no_dispatch_rejects_symlink_root(tmp_path, monkeypatch):
    run_id, run_root, roots = _make_candidate_roots(tmp_path, monkeypatch)
    shared = roots["shared_state_root"]
    shared.rmdir()
    target = run_root / "shared-state-real"
    target.mkdir()
    shared.symlink_to(target, target_is_directory=True)
    with pytest.raises(smoke.NoDispatchIsolationError, match="without symlinks"):
        smoke.validate_no_dispatch_roots(run_id=run_id, **roots)


def test_no_dispatch_rejects_parent_symlink_swap_after_validation(tmp_path, monkeypatch):
    run_id, run_root, roots = _make_candidate_roots(tmp_path, monkeypatch)
    original_validate = smoke.validate_no_dispatch_roots
    namespace = run_root.parent
    moved = namespace.with_name("hermes-v0182-moved")

    def validate_then_swap(**kwargs):
        validated = original_validate(**kwargs)
        namespace.rename(moved)
        namespace.symlink_to(moved, target_is_directory=True)
        return validated

    monkeypatch.setattr(smoke, "validate_no_dispatch_roots", validate_then_swap)
    try:
        with pytest.raises(smoke.NoDispatchIsolationError, match="safely open run_root"):
            smoke.record_no_dispatch(
                issue_url=smoke.DEFAULT_ISSUE_URL,
                work_item=smoke.DEFAULT_WORK_ITEM,
                requester=smoke.DEFAULT_REQUESTER,
                run_id=run_id,
                **roots,
            )
    finally:
        if namespace.is_symlink():
            namespace.unlink()
        if moved.exists() and not namespace.exists():
            moved.rename(namespace)


def test_no_dispatch_rejects_duplicate_run_receipt(tmp_path, monkeypatch):
    run_id, _run_root, roots = _make_candidate_roots(tmp_path, monkeypatch)
    kwargs = {
        "issue_url": smoke.DEFAULT_ISSUE_URL,
        "work_item": smoke.DEFAULT_WORK_ITEM,
        "requester": smoke.DEFAULT_REQUESTER,
        "run_id": run_id,
        **roots,
    }
    smoke.record_no_dispatch(**kwargs)
    with pytest.raises(smoke.NoDispatchIsolationError, match="cannot create isolated record"):
        smoke.record_no_dispatch(**kwargs)


def test_isolation_arguments_without_no_dispatch_fail_before_preflight(tmp_path, monkeypatch, capsys):
    run_id, _run_root, roots = _make_candidate_roots(tmp_path, monkeypatch)
    monkeypatch.setattr(
        smoke,
        "preflight",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("preflight must not run")
        ),
    )
    argv = ["--run-id", run_id, "--case-root", str(roots["case_root"]), "--json"]
    assert smoke.main(argv) == 3
    body = json.loads(capsys.readouterr().out)
    assert body["error"]["code"] == "G1Q3_ISOLATION_REQUIRES_NO_DISPATCH"


def test_parser_keeps_legacy_default_and_dry_run_modes():
    default = smoke._build_parser().parse_args([])
    assert default.no_dispatch is False
    assert default.dry_run is False
    assert default.issue_url == smoke.DEFAULT_ISSUE_URL
    assert all(getattr(default, name) is None for name in smoke.NO_DISPATCH_ROOT_FIELDS)
    dry = smoke._build_parser().parse_args(["--dry-run"])
    assert dry.dry_run is True
    assert dry.no_dispatch is False
