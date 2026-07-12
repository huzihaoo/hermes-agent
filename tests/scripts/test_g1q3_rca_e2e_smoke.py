#!/usr/bin/env python3
"""Unit tests for scripts/g1q3_rca_e2e_smoke.py - pure logic only (no VM).

Covers fail-closed execution, frozen evidence, identity binding, quota
observation, and green-check judging.
"""
from __future__ import annotations

import importlib.util
import hashlib
import json
import os
import sys
from pathlib import Path

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
    assert not ok and "BLOCK" in note


def test_quota_blocks_when_exhausted(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_G1Q3_AUTO_DOWNLOAD_DAILY_QUOTA", "2")
    from datetime import datetime, timezone
    day = datetime.now(timezone.utc).date().isoformat()
    qdir = tmp_path / "pnc_agent" / "quota"
    qdir.mkdir(parents=True)
    (qdir / f"g1q3_auto_download-{day}.json").write_text(
        '{"used": 2, "grants": []}', encoding="utf-8"
    )
    ok, note = smoke._quota_has_headroom(tmp_path)
    assert not ok and "exhausted" in note


def test_quota_headroom_ok(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_G1Q3_AUTO_DOWNLOAD_DAILY_QUOTA", "50")
    from datetime import datetime, timezone
    day = datetime.now(timezone.utc).date().isoformat()
    qdir = tmp_path / "pnc_agent" / "quota"
    qdir.mkdir(parents=True)
    (qdir / f"g1q3_auto_download-{day}.json").write_text(
        '{"used": 1}', encoding="utf-8"
    )
    ok, note = smoke._quota_has_headroom(tmp_path)
    assert not ok and "no atomic reservation" in note


def _quota_ledger(tmp_path: Path, body: str) -> Path:
    from datetime import datetime, timezone

    day = datetime.now(timezone.utc).date().isoformat()
    ledger = tmp_path / "pnc_agent" / "quota" / f"g1q3_auto_download-{day}.json"
    ledger.parent.mkdir(parents=True, exist_ok=True)
    ledger.write_text(body, encoding="utf-8")
    return ledger


@pytest.mark.parametrize("body", ["not-json", "{}", '{"used": -1}', '{"used": true}', '{"used": "0"}'])
def test_quota_malformed_ledger_blocks(tmp_path, monkeypatch, body):
    monkeypatch.setenv("HERMES_G1Q3_AUTO_DOWNLOAD_DAILY_QUOTA", "2")
    _quota_ledger(tmp_path, body)
    ok, note = smoke._quota_has_headroom(tmp_path)
    assert not ok and "malformed" in note


def test_quota_symlink_and_hardlink_ledgers_block(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_G1Q3_AUTO_DOWNLOAD_DAILY_QUOTA", "2")
    ledger = _quota_ledger(tmp_path, '{"used": 0}')
    target = tmp_path / "target.json"
    ledger.rename(target)
    ledger.symlink_to(target)
    ok, note = smoke._quota_has_headroom(tmp_path)
    assert not ok and "unsafe" in note

    ledger.unlink()
    os.link(target, ledger)
    ok, note = smoke._quota_has_headroom(tmp_path)
    assert not ok and "link/mode" in note


# ---- pure green-check judging (no VM) ----
def _judge(*, gate, report, index_bytes, report_bytes, exit_code):
    return smoke.judge_green_checks(
        gate=gate,
        report=report,
        index_html_bytes=index_bytes,
        report_data_bytes=report_bytes,
        exit_code=exit_code,
    )


def _good_gate():
    return {"gates": [{"gate": f"G{i}", "status": "pass"} for i in range(7)]}


def test_green_checks_all_pass():
    report = {
        "summary": {"status": "hypothesis_ready",
                    "ui_banner_title": "候选归因待人工复核",
                    "high_confidence_boundary": "非高置信自动归因；当前候选仍需人工 review"},
        "field_lineage": {"manifest_decoded_dropped": [], "fidelity_ok": True},
    }
    c = _judge(gate=_good_gate(), report=report,
               index_bytes=7_000_000, report_bytes=17_000_000, exit_code="0")
    assert c["1_gates_all_pass"]["ok"]
    assert c["2_status_honest"]["ok"]
    assert c["3_manifest_decoded_dropped_empty"]["ok"]
    assert c["4_artifacts_exit0"]["ok"]
    assert c["5_card_delivery"]["ok"] is None  # headless boundary


def test_green_check_status_washed_to_completed_fails():
    # hypothesis_ready (low confidence) but banner claims 已完成/已定责 -> dishonest -> FAIL
    report = {
        "summary": {"status": "hypothesis_ready", "ui_banner_title": "已完成，已定责",
                    "high_confidence_boundary": ""},
        "field_lineage": {"manifest_decoded_dropped": []},
    }
    c = _judge(gate=_good_gate(), report=report,
               index_bytes=7_000_000, report_bytes=17_000_000, exit_code="0")
    assert not c["2_status_honest"]["ok"]


def test_green_check_dropped_fields_fails():
    report = {
        "summary": {"status": "report_ready"},
        "field_lineage": {"manifest_decoded_dropped": ["ego.speed"]},  # non-empty -> false-green suspect
    }
    c = _judge(gate=_good_gate(), report=report,
               index_bytes=7_000_000, report_bytes=17_000_000, exit_code="0")
    assert not c["3_manifest_decoded_dropped_empty"]["ok"]


def test_green_check_gate_not_all_pass_fails():
    gate = {"gates": [{"gate": "G0", "status": "pass"}, {"gate": "G3", "status": "fail"}]}
    report = {"summary": {"status": "report_ready"}, "field_lineage": {"manifest_decoded_dropped": []}}
    c = _judge(gate=gate, report=report, index_bytes=1, report_bytes=1, exit_code="0")
    assert not c["1_gates_all_pass"]["ok"]


def test_green_check_nonzero_exit_fails():
    report = {"summary": {"status": "report_ready"}, "field_lineage": {"manifest_decoded_dropped": []}}
    c = _judge(gate=_good_gate(), report=report,
               index_bytes=7_000_000, report_bytes=17_000_000, exit_code="1")
    assert not c["4_artifacts_exit0"]["ok"]


def test_green_check_requires_exact_g0_through_g6():
    report = {
        "summary": {"status": "report_ready"},
        "field_lineage": {"manifest_decoded_dropped": [], "fidelity_ok": True},
    }
    missing = {"gates": [{"gate": f"G{i}", "status": "pass"} for i in range(6)]}
    assert not _judge(gate=missing, report=report,
                      index_bytes=1, report_bytes=1, exit_code="0")["1_gates_all_pass"]["ok"]
    extra = {"gates": [{"gate": f"G{i}", "status": "pass"} for i in range(8)]}
    assert not _judge(gate=extra, report=report,
                      index_bytes=1, report_bytes=1, exit_code="0")["1_gates_all_pass"]["ok"]
    duplicate = {"gates": [
        *[{"gate": f"G{i}", "status": "pass"} for i in range(7)],
        {"gate": "G6", "status": "pass"},
    ]}
    assert not _judge(gate=duplicate, report=report,
                      index_bytes=1, report_bytes=1, exit_code="0")["1_gates_all_pass"]["ok"]


def test_green_check_requires_explicit_fidelity_true():
    report = {
        "summary": {"status": "report_ready"},
        "field_lineage": {"manifest_decoded_dropped": []},
    }
    assert not _judge(gate=_good_gate(), report=report,
                      index_bytes=1, report_bytes=1, exit_code="0")["3_manifest_decoded_dropped_empty"]["ok"]


def _make_fixture(root: Path) -> tuple[Path, Path, Path]:
    case = root / "case"
    artifact = root / "artifact"
    shared = root / "shared-state"
    case.mkdir(parents=True)
    artifact.mkdir()
    shared.mkdir()
    identity = {
        "release_id": "release-0182",
        "run_id": "run-0001",
        "task_slug": "g1q3_rca_issue_intake_7041712812_run-0001",
        "work_item_id": "7041712812",
        "issue_url": "https://project.feishu.cn/t03o4q/issue/detail/7041712812",
        "group_id": smoke.G1Q3_RCA_GROUP_ID,
        "source_commit": "1" * 40,
        "source_tree": "2" * 40,
        "policy_sha256": "3" * 64,
    }
    evidence_identity = {
        field: identity[field] for field in smoke.EVIDENCE_IDENTITY_FIELDS
    }
    gate = {**_good_gate(), "fixture_identity": evidence_identity}
    (case / "gate_result.json").write_text(json.dumps(gate), encoding="utf-8")
    report = {
        "summary": {"status": "report_ready"},
        "field_lineage": {"manifest_decoded_dropped": [], "fidelity_ok": True},
        "fixture_identity": evidence_identity,
    }
    (case / "report_data.json").write_text(json.dumps(report), encoding="utf-8")
    (case / "index.html").write_text("<html>fixture</html>", encoding="utf-8")
    (artifact / "exit.code").write_text("0\n", encoding="utf-8")
    (shared / "task_card.json").write_text(json.dumps({
        "fixture_identity": evidence_identity,
        "fixture_handoff_contract": {
            "contract_version": "g1q3_rca_group_handoff_v2",
            "run_id": identity["run_id"],
            "task_slug": identity["task_slug"],
            "work_item_id": identity["work_item_id"],
            "issue_url": identity["issue_url"],
            "source_group_id": identity["group_id"],
        },
        "task_card": {
            "task_id": identity["task_slug"],
            "run_id": identity["run_id"],
            "work_item_id": identity["work_item_id"],
            "issue_url": identity["issue_url"],
            "chat_id": identity["group_id"],
            "delivery": {
                "report_status": "report_ready",
                "has_deliverable_report": True,
                "user_state": "done",
            },
        }
    }), encoding="utf-8")
    evidence_paths = {
        "case/gate_result.json": case / "gate_result.json",
        "case/report_data.json": case / "report_data.json",
        "case/index.html": case / "index.html",
        "artifact/exit.code": artifact / "exit.code",
        "shared-state/task_card.json": shared / "task_card.json",
    }
    manifest = {
        "schema_version": smoke.FIXTURE_SCHEMA,
        "identity": identity,
        "roots": {"case": "case", "artifact": "artifact", "shared_state": "shared-state"},
        "authorization": {
            "execution_authorized": False,
            "dispatch_attempted": False,
            "approval_receipt_id": None,
        },
        "evidence": {
            logical: hashlib.sha256(path.read_bytes()).hexdigest()
            for logical, path in evidence_paths.items()
        },
    }
    manifest_path = root / "fixture_manifest.json"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    for path in (*evidence_paths.values(), manifest_path):
        path.chmod(0o444)
    for directory in (case, artifact, shared, root):
        directory.chmod(0o500)
    return case, artifact, shared


def test_fixture_mode_is_local_only_but_never_claims_release_go(
    tmp_path, monkeypatch, capsys
):
    root = tmp_path.resolve() / "isolated"
    case, artifact, shared = _make_fixture(root)
    monkeypatch.setattr(
        smoke,
        "trigger",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("dispatch forbidden")),
    )
    rc = smoke.main([
        "--fixture-mode",
        "--isolation-root", str(root),
        "--case-root", str(case),
        "--artifact-root", str(artifact),
        "--shared-state-root", str(shared),
        "--json",
    ])
    assert rc == 2
    body = json.loads(capsys.readouterr().out)
    assert body["offline_check_passed"] is True
    assert body["ok"] is False
    assert body["external_release_binding_verified"] is False
    assert body["production_ready"] is False
    assert body["l6_gate_passed"] is False
    assert body["cutover_go"] is False
    checks = body["green_checks"]
    assert checks["0_fixture_manifest_self_consistent"]["ok"] is True
    assert checks["0_external_release_binding"]["ok"] is False


def test_fixture_invalid_evidence_is_rc3_structured_error(tmp_path, capsys):
    root = tmp_path.resolve() / "isolated"
    case, artifact, shared = _make_fixture(root)
    case.chmod(0o700)
    report = case / "report_data.json"
    report.chmod(0o600)
    report.write_text("{}", encoding="utf-8")
    report.chmod(0o444)
    case.chmod(0o500)

    assert smoke.main([
        "--fixture-mode",
        "--isolation-root", str(root),
        "--case-root", str(case),
        "--artifact-root", str(artifact),
        "--shared-state-root", str(shared),
        "--json",
    ]) == 3
    body = json.loads(capsys.readouterr().out)
    assert body["error"]["code"] == "G1Q3_INVALID_FIXTURE_EVIDENCE"
    assert "blocker" not in body
    assert body["offline_check_passed"] is False
    assert body["execution_authorized"] is False
    assert body["dispatch_attempted"] is False


def test_fixture_roots_escape_and_symlink_are_rejected(tmp_path):
    root = tmp_path.resolve() / "isolated"
    case, artifact, shared = _make_fixture(root)
    outside = tmp_path.resolve() / "outside"
    outside.mkdir()
    outside.chmod(0o500)
    with pytest.raises(ValueError, match="escapes isolation root"):
        smoke.validate_fixture_roots(
            isolation_root=root,
            case_root=outside,
            artifact_root=artifact,
            shared_state_root=shared,
        )
    root.chmod(0o700)
    case.chmod(0o700)
    real_case = case.rename(root / "real-case")
    real_case.chmod(0o500)
    case.symlink_to(real_case, target_is_directory=True)
    root.chmod(0o500)
    with pytest.raises(ValueError, match="canonical real directory"):
        smoke.validate_fixture_roots(
            isolation_root=root,
            case_root=case,
            artifact_root=artifact,
            shared_state_root=shared,
        )


def test_fixture_file_symlink_and_hardlink_are_rejected(tmp_path):
    root = tmp_path.resolve() / "isolated"
    case, artifact, shared = _make_fixture(root)
    case.chmod(0o700)
    target = case / "report-target.json"
    (case / "report_data.json").rename(target)
    (case / "report_data.json").symlink_to(target)
    case.chmod(0o500)
    with pytest.raises(ValueError, match="safely opened"):
        smoke.fixture_checks(
            isolation_root=root, case_root=case, artifact_root=artifact, shared_state_root=shared
        )

    case.chmod(0o700)
    (case / "report_data.json").unlink()
    os.link(target, case / "report_data.json")
    case.chmod(0o500)
    with pytest.raises(ValueError, match="hardlink check failed"):
        smoke.fixture_checks(
            isolation_root=root, case_root=case, artifact_root=artifact, shared_state_root=shared
        )


def test_execute_is_unconditionally_blocked_before_any_hook(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(
        smoke,
        "trigger",
        lambda *_: (_ for _ in ()).throw(AssertionError("trigger must not run")),
    )
    rc = smoke.main([
        "--execute",
        "--hermes-home", str(tmp_path.resolve()),
        "--issue-url", "https://fixture.invalid/issue/1",
        "--work-item", "1",
        "--requester", "ou_fixture",
        "--group-id", "oc_fixture",
        "--run-id", "run-0001",
        "--confirm-execute", "G1Q3_REAL_DISPATCH_7_5GB",
        "--json",
    ])
    assert rc == 3
    body = json.loads(capsys.readouterr().out)
    assert body["blocker"]["code"] == "G1Q3_REAL_EXECUTION_NOT_IMPLEMENTED"
    assert body["blocker"]["dispatch_attempted"] is False
    assert "error" not in body
    assert body["ok"] is False
    assert body["production_ready"] is False


def test_direct_trigger_is_unconditionally_blocked():
    with pytest.raises(RuntimeError, match="G1Q3_REAL_EXECUTION_NOT_IMPLEMENTED"):
        smoke.trigger("url", "item", "requester", "run-0001", "group")


def _make_policy_repo(
    root: Path, *, policy_mode: int = 0o444, parent_mode: int = 0o500
) -> tuple[Path, Path]:
    repo = root / "candidate"
    gateway = repo / "gateway"
    gateway.mkdir(parents=True)
    policy = gateway / "pnc_group_binding.py"
    policy.write_text(
        f"G1Q3_RCA_GROUP_ID = {smoke.G1Q3_RCA_GROUP_ID!r}\n"
        "def evaluate_pnc_group_request(*, platform, chat_id, text):\n"
        "    return None\n",
        encoding="utf-8",
    )
    policy.chmod(policy_mode)
    gateway.chmod(parent_mode)
    return repo, policy


def _no_dispatch_args() -> list[str]:
    return [
        "--no-dispatch",
        "--issue-url", "https://project.feishu.cn/t03o4q/issue/detail/7041712812",
        "--work-item", "7041712812",
        "--group-id", smoke.G1Q3_RCA_GROUP_ID,
        "--json",
    ]


def test_no_dispatch_calls_only_routing_decision_but_never_claims_go(
    monkeypatch, capsys
):
    monkeypatch.setattr(
        smoke,
        "no_dispatch_decision",
        lambda **kwargs: {
            "ok": False,
            "offline_check_passed": True,
            "decision": "accepted",
            "execution_authorized": False,
            "dispatch_attempted": False,
        },
    )
    assert smoke.main([
        "--no-dispatch",
        "--issue-url", "https://project.feishu.cn/t03o4q/issue/detail/7041712812",
        "--work-item", "7041712812",
        "--group-id", smoke.G1Q3_RCA_GROUP_ID,
        "--json",
    ]) == 2
    body = json.loads(capsys.readouterr().out)
    assert body["offline_check_passed"] is True
    assert body["ok"] is False
    assert body["external_release_binding_verified"] is False
    assert body["production_ready"] is False


@pytest.mark.parametrize(
    ("issue_url", "work_item", "group_id"),
    [
        (
            "https://project.feishu.cn/t03o4q/issue/detail/7041712812",
            "999",
            smoke.G1Q3_RCA_GROUP_ID,
        ),
        (
            "https://project.feishu.cn/t03o4q/issue/detail/7041712812?x=1",
            "7041712812",
            smoke.G1Q3_RCA_GROUP_ID,
        ),
        (
            "https://project.feishu.cn/t03o4q/issue/detail/7041712812",
            "7041712812",
            "oc_wrong",
        ),
    ],
)
def test_no_dispatch_identity_mismatch_blocks_before_route_import(
    issue_url, work_item, group_id
):
    with pytest.raises(ValueError):
        smoke.no_dispatch_decision(
            issue_url=issue_url,
            work_item=work_item,
            group_id=group_id,
        )


def test_no_dispatch_never_claims_semantic_handoff_identity():
    decision = smoke.no_dispatch_decision(
        issue_url="https://project.feishu.cn/t03o4q/issue/detail/7041712812",
        work_item="7041712812",
        group_id=smoke.G1Q3_RCA_GROUP_ID,
    )
    assert decision["decision"] == "not_executed"
    assert decision["handoff_work_item_id"] is None
    assert decision["handoff_identity_verified"] is False
    assert decision["semantic_route_evaluation_performed"] is False
    assert decision["execution_authorized"] is False


def test_no_dispatch_mutable_checkout_cannot_report_offline_pass():
    decision = smoke.no_dispatch_decision(
        issue_url="https://project.feishu.cn/t03o4q/issue/detail/7041712812",
        work_item="7041712812",
        group_id=smoke.G1Q3_RCA_GROUP_ID,
    )
    assert decision["ok"] is False
    assert decision["policy_structure_observed"] is True
    assert decision["policy_source_frozen"] is False
    assert decision["offline_policy_contract_observed"] is False
    assert decision["offline_check_passed"] is False
    assert len(decision["routing_policy_sha256"]) == 64
    assert decision["policy_execution_performed"] is False
    assert decision["decision"] == "not_executed"
    assert decision["execution_authorized"] is False
    assert decision["dispatch_attempted"] is False
    assert decision["external_release_binding_verified"] is False
    assert decision["production_ready"] is False


def test_no_dispatch_frozen_policy_is_valid_rc2_but_still_non_authorizing(
    tmp_path, monkeypatch, capsys
):
    repo, _policy = _make_policy_repo(tmp_path.resolve())
    monkeypatch.setattr(smoke, "REPO", repo)

    assert smoke.main(_no_dispatch_args()) == 2
    body = json.loads(capsys.readouterr().out)
    assert "error" not in body
    assert "blocker" not in body
    assert body["offline_check_passed"] is True
    assert body["decision"]["policy_source_frozen"] is True
    assert body["decision"]["offline_policy_contract_observed"] is True
    assert body["ok"] is False
    assert body["execution_authorized"] is False
    assert body["dispatch_attempted"] is False


@pytest.mark.parametrize(
    ("policy_mode", "parent_mode"),
    [(0o644, 0o500), (0o444, 0o700)],
)
def test_no_dispatch_writable_policy_surface_never_reports_pass(
    tmp_path, monkeypatch, policy_mode, parent_mode
):
    repo, _policy = _make_policy_repo(
        tmp_path.resolve(), policy_mode=policy_mode, parent_mode=parent_mode
    )
    monkeypatch.setattr(smoke, "REPO", repo)

    decision = smoke.no_dispatch_decision(
        issue_url="https://project.feishu.cn/t03o4q/issue/detail/7041712812",
        work_item="7041712812",
        group_id=smoke.G1Q3_RCA_GROUP_ID,
    )
    assert decision["policy_structure_observed"] is True
    assert decision["policy_source_frozen"] is False
    assert decision["offline_policy_contract_observed"] is False
    assert decision["offline_check_passed"] is False


def test_no_dispatch_invalid_identity_is_rc3_structured_error(capsys):
    args = _no_dispatch_args()
    args[args.index("--work-item") + 1] = "999"

    assert smoke.main(args) == 3
    body = json.loads(capsys.readouterr().out)
    assert body["error"]["code"] == "G1Q3_INVALID_OFFLINE_EVIDENCE"
    assert body["decision"]["offline_check_passed"] is False
    assert "blocker" not in body
    assert body["execution_authorized"] is False
    assert body["dispatch_attempted"] is False


def test_no_dispatch_missing_identity_is_rc3_invalid_invocation(capsys):
    assert smoke.main(["--no-dispatch", "--json"]) == 3
    body = json.loads(capsys.readouterr().out)
    assert body["error"]["code"] == "G1Q3_INVALID_INVOCATION"
    assert "blocker" not in body
    assert body["offline_check_passed"] is False


def test_missing_mode_is_rc3_before_any_execution_surface(monkeypatch, capsys):
    monkeypatch.setattr(
        smoke,
        "fixture_checks",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("fixture must not run")),
    )
    monkeypatch.setattr(
        smoke,
        "no_dispatch_decision",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("routing must not run")),
    )

    assert smoke.main(["--json"]) == 3
    body = json.loads(capsys.readouterr().out)
    assert body["mode"] == "invalid"
    assert body["error"]["code"] == "G1Q3_INVALID_INVOCATION"
    assert body["execution_authorized"] is False
    assert body["dispatch_attempted"] is False
    assert body["production_ready"] is False


def test_isolation_paths_without_offline_mode_are_rejected(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(
        smoke,
        "fixture_checks",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("fixture must not run")),
    )
    monkeypatch.setattr(
        smoke,
        "no_dispatch_decision",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("routing must not run")),
    )

    assert smoke.main(["--isolation-root", str(tmp_path.resolve()), "--json"]) == 3
    body = json.loads(capsys.readouterr().out)
    assert body["mode"] == "invalid"
    assert body["error"]["code"] == "G1Q3_INVALID_INVOCATION"
    assert body["execution_authorized"] is False
    assert body["dispatch_attempted"] is False
    assert body["production_ready"] is False


def test_no_dispatch_parser_error_is_rc3_invalid_invocation(capsys):
    assert smoke.main(["--no-dispatch", "--unknown", "--json"]) == 3
    body = json.loads(capsys.readouterr().out)
    assert body["mode"] == "invalid"
    assert body["error"]["code"] == "G1Q3_INVALID_INVOCATION"
    assert "blocker" not in body
    assert body["offline_check_passed"] is False


def test_no_dispatch_policy_symlink_is_rc3_and_never_passes(
    tmp_path, monkeypatch, capsys
):
    repo, policy = _make_policy_repo(tmp_path.resolve())
    gateway = policy.parent
    gateway.chmod(0o700)
    target = gateway / "real-policy.py"
    policy.rename(target)
    policy.symlink_to(target.name)
    gateway.chmod(0o500)
    monkeypatch.setattr(smoke, "REPO", repo)

    assert smoke.main(_no_dispatch_args()) == 3
    body = json.loads(capsys.readouterr().out)
    assert body["error"]["code"] == "G1Q3_INVALID_OFFLINE_EVIDENCE"
    assert body["offline_check_passed"] is False
    assert body["execution_authorized"] is False
    assert body["dispatch_attempted"] is False


def test_no_dispatch_policy_hardlink_is_rc3_and_never_passes(
    tmp_path, monkeypatch, capsys
):
    repo, policy = _make_policy_repo(tmp_path.resolve())
    gateway = policy.parent
    gateway.chmod(0o700)
    os.link(policy, gateway / "policy-hardlink.py")
    gateway.chmod(0o500)
    monkeypatch.setattr(smoke, "REPO", repo)

    assert smoke.main(_no_dispatch_args()) == 3
    body = json.loads(capsys.readouterr().out)
    assert body["error"]["code"] == "G1Q3_INVALID_OFFLINE_EVIDENCE"
    assert "hardlink" in body["error"]["reason"] or "link" in body["error"]["reason"]
    assert body["offline_check_passed"] is False


def test_no_dispatch_policy_untrusted_owner_is_rc3_and_never_passes(
    tmp_path, monkeypatch, capsys
):
    repo, _policy = _make_policy_repo(tmp_path.resolve())
    actual_uid = os.getuid()
    monkeypatch.setattr(smoke, "REPO", repo)
    monkeypatch.setattr(smoke.os, "getuid", lambda: actual_uid + 1)

    assert smoke.main(_no_dispatch_args()) == 3
    body = json.loads(capsys.readouterr().out)
    assert body["error"]["code"] == "G1Q3_INVALID_OFFLINE_EVIDENCE"
    assert "ownership" in body["error"]["reason"]
    assert body["offline_check_passed"] is False


def test_no_dispatch_policy_change_during_read_is_rc3_and_never_passes(
    tmp_path, monkeypatch, capsys
):
    repo, policy = _make_policy_repo(tmp_path.resolve())
    monkeypatch.setattr(smoke, "REPO", repo)
    original_read = os.read
    mutated = False

    def mutate_at_first_eof(fd, size):
        nonlocal mutated
        data = original_read(fd, size)
        if not data and not mutated:
            mutated = True
            policy.chmod(0o600)
            policy.write_bytes(policy.read_bytes() + b"\n")
            policy.chmod(0o444)
        return data

    monkeypatch.setattr(smoke.os, "read", mutate_at_first_eof)
    assert smoke.main(_no_dispatch_args()) == 3
    body = json.loads(capsys.readouterr().out)
    assert mutated is True
    assert body["error"]["code"] == "G1Q3_INVALID_OFFLINE_EVIDENCE"
    assert "changed" in body["error"]["reason"]
    assert body["offline_check_passed"] is False


def test_policy_ast_inspection_cannot_run_top_level_side_effect(tmp_path, monkeypatch):
    repo = tmp_path.resolve() / "candidate"
    gateway = repo / "gateway"
    gateway.mkdir(parents=True)
    marker = tmp_path / "policy-executed"
    (gateway / "pnc_group_binding.py").write_text(
        "from pathlib import Path\n"
        f"Path({str(marker)!r}).write_text('executed')\n"
        f"G1Q3_RCA_GROUP_ID = {smoke.G1Q3_RCA_GROUP_ID!r}\n"
        "def evaluate_pnc_group_request(*, platform, chat_id, text):\n"
        "    raise RuntimeError('must never execute')\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(smoke, "REPO", repo)
    decision = smoke.no_dispatch_decision(
        issue_url="https://project.feishu.cn/t03o4q/issue/detail/7041712812",
        work_item="7041712812",
        group_id=smoke.G1Q3_RCA_GROUP_ID,
    )
    assert decision["offline_policy_contract_observed"] is False
    assert decision["top_level_expression_safe"] is False
    assert decision["policy_execution_performed"] is False
    assert not marker.exists()


def _rewrite_manifest(root: Path, mutate) -> None:
    manifest_path = root / "fixture_manifest.json"
    root.chmod(0o700)
    manifest_path.chmod(0o600)
    body = json.loads(manifest_path.read_text(encoding="utf-8"))
    mutate(body)
    manifest_path.write_text(json.dumps(body, sort_keys=True), encoding="utf-8")
    manifest_path.chmod(0o444)
    root.chmod(0o500)


def _rewrite_json_evidence(root: Path, relative: str, mutate) -> None:
    path = root / relative
    parent = path.parent
    parent.chmod(0o700)
    path.chmod(0o600)
    body = json.loads(path.read_text(encoding="utf-8"))
    mutate(body)
    path.write_text(json.dumps(body, sort_keys=True), encoding="utf-8")
    path.chmod(0o444)
    parent.chmod(0o500)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    _rewrite_manifest(
        root,
        lambda manifest: manifest["evidence"].update({relative: digest}),
    )


def test_fixture_manifest_binds_identity_and_digests(tmp_path):
    root = tmp_path.resolve() / "isolated"
    case, artifact, shared = _make_fixture(root)
    _rewrite_manifest(root, lambda body: body["identity"].update({"work_item_id": "999"}))
    with pytest.raises(ValueError, match="work-item"):
        smoke.fixture_checks(
            isolation_root=root, case_root=case, artifact_root=artifact, shared_state_root=shared
        )


def test_self_declared_release_hashes_never_become_external_binding(tmp_path):
    root = tmp_path.resolve() / "isolated"
    case, artifact, shared = _make_fixture(root)
    _rewrite_manifest(
        root,
        lambda body: body["identity"].update(
            {
                "release_id": "forged-release",
                "source_commit": "a" * 40,
                "source_tree": "b" * 40,
                "policy_sha256": "c" * 64,
            }
        ),
    )
    checks = smoke.fixture_checks(
        isolation_root=root,
        case_root=case,
        artifact_root=artifact,
        shared_state_root=shared,
    )
    assert checks["0_fixture_manifest_self_consistent"]["ok"] is True
    external = checks["0_external_release_binding"]
    assert external["ok"] is False
    assert external["external_release_binding_verified"] is False
    assert external["production_ready"] is False


@pytest.mark.parametrize(
    ("relative", "mutate", "error"),
    [
        (
            "case/report_data.json",
            lambda body: body["fixture_identity"].update({"work_item_id": "999"}),
            "report data fixture identity",
        ),
        (
            "shared-state/task_card.json",
            lambda body: body["fixture_handoff_contract"].update(
                {"source_group_id": "oc_wrong"}
            ),
            "task card handoff identity",
        ),
        (
            "shared-state/task_card.json",
            lambda body: body["task_card"].update({"chat_id": "oc_wrong"}),
            "task card native identity",
        ),
    ],
)
def test_fixture_evidence_and_handoff_identity_mismatch_blocks(
    tmp_path, relative, mutate, error
):
    root = tmp_path.resolve() / "isolated"
    case, artifact, shared = _make_fixture(root)
    _rewrite_json_evidence(root, relative, mutate)
    with pytest.raises(ValueError, match=error):
        smoke.fixture_checks(
            isolation_root=root,
            case_root=case,
            artifact_root=artifact,
            shared_state_root=shared,
        )


def test_fixture_duplicate_json_keys_are_rejected_even_when_digest_matches(tmp_path):
    root = tmp_path.resolve() / "isolated"
    case, artifact, shared = _make_fixture(root)
    case.chmod(0o700)
    gate = case / "gate_result.json"
    gate.chmod(0o600)
    gate.write_text('{"gates": [], "gates": []}', encoding="utf-8")
    gate.chmod(0o444)
    case.chmod(0o500)
    digest = hashlib.sha256(gate.read_bytes()).hexdigest()
    _rewrite_manifest(
        root,
        lambda body: body["evidence"].update({"case/gate_result.json": digest}),
    )
    with pytest.raises(ValueError, match="duplicate JSON key"):
        smoke.fixture_checks(
            isolation_root=root, case_root=case, artifact_root=artifact, shared_state_root=shared
        )


def test_fixture_two_pass_change_is_rejected(tmp_path, monkeypatch):
    root = tmp_path.resolve() / "isolated"
    case, artifact, shared = _make_fixture(root)
    original = smoke._read_fixture_entry
    calls = 0

    def unstable(*args, **kwargs):
        nonlocal calls
        calls += 1
        data, identity = original(*args, **kwargs)
        if calls > 6 and kwargs.get("label") == "fixture_manifest.json":
            data += b" "
        return data, identity

    monkeypatch.setattr(smoke, "_read_fixture_entry", unstable)
    with pytest.raises(ValueError, match="changed between frozen verification passes"):
        smoke.fixture_checks(
            isolation_root=root, case_root=case, artifact_root=artifact, shared_state_root=shared
        )


def test_fixture_root_path_replacement_during_verification_is_rejected(
    tmp_path, monkeypatch
):
    root = tmp_path.resolve() / "isolated"
    case, artifact, shared = _make_fixture(root)
    moved = root.with_name("isolated-moved")
    original = smoke._read_fixture_entry
    calls = 0

    def replace_root_after_last_read(*args, **kwargs):
        nonlocal calls
        calls += 1
        result = original(*args, **kwargs)
        if calls == 12:
            root.chmod(0o700)
            root.rename(moved)
            moved.chmod(0o500)
        return result

    monkeypatch.setattr(smoke, "_read_fixture_entry", replace_root_after_last_read)
    try:
        with pytest.raises(ValueError, match="changed|replaced"):
            smoke.fixture_checks(
                isolation_root=root,
                case_root=case,
                artifact_root=artifact,
                shared_state_root=shared,
            )
    finally:
        if moved.exists() and not root.exists():
            moved.chmod(0o700)
            moved.rename(root)


def test_fixture_digest_mismatch_is_rejected(tmp_path):
    root = tmp_path.resolve() / "isolated"
    case, artifact, shared = _make_fixture(root)
    case.chmod(0o700)
    report = case / "report_data.json"
    report.chmod(0o600)
    report.write_text('{"summary": {"status": "report_ready"}}', encoding="utf-8")
    report.chmod(0o444)
    case.chmod(0o500)
    with pytest.raises(ValueError, match="digest mismatch"):
        smoke.fixture_checks(
            isolation_root=root, case_root=case, artifact_root=artifact, shared_state_root=shared
        )


def test_fixture_writable_directory_is_rejected(tmp_path):
    root = tmp_path.resolve() / "isolated"
    case, artifact, shared = _make_fixture(root)
    case.chmod(0o700)
    with pytest.raises(ValueError, match="frozen"):
        smoke.validate_fixture_roots(
            isolation_root=root,
            case_root=case,
            artifact_root=artifact,
            shared_state_root=shared,
        )


def test_no_dispatch_rejects_requester_as_unvalidated_authorization(monkeypatch):
    monkeypatch.setattr(
        smoke,
        "no_dispatch_decision",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("routing must not run")),
    )
    assert smoke.main([
        "--no-dispatch",
        "--issue-url", "https://project.feishu.cn/t03o4q/issue/detail/7041712812",
        "--work-item", "7041712812",
        "--group-id", smoke.G1Q3_RCA_GROUP_ID,
        "--requester", "ou_unvalidated",
    ]) == 3
