from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import copy
from datetime import datetime, timedelta, timezone
import hashlib
import inspect
import json
from pathlib import Path
import shlex
import subprocess

import pytest

from gateway import pnc_rca_prod_admission as admission
from gateway.pnc_rca_workspace_runtime import WorkspaceRuntimeIdentity
from tools import vm_task_tool
from tools.registry import registry


KEY = "hex:" + ("71" * 32)
NOW = datetime(2026, 8, 18, 6, 0, tzinfo=timezone.utc)
FIXED_SMOKE10_IDS_SHA256 = admission.HISTORICAL_ORDERED_WORK_ITEM_IDS_SHA256
RUNTIME = WorkspaceRuntimeIdentity(
    root=Path("/fixed/runtime"), manifest_path=Path("/fixed/runtime/manifest.json"),
    creator_path=Path("/fixed/runtime/bin/create_task_v2.py"),
    manifest_sha256="1" * 64, closure_sha256="2" * 64,
    source_commit="3" * 40,
    file_sha256={"bin/create_task_v2.py": "4" * 64},
)


def request(request_id: str = "successor-full308") -> dict:
    inputs = admission.HISTORICAL_INPUT_CONTRACT
    release = admission.HISTORICAL_SUCCESSOR_RELEASE
    assert release is not None
    return {
        "schema_version": admission.HISTORICAL_REQUEST_SCHEMA,
        "request_id": request_id,
        "owner": "songying",
        "remote_commit": release["source_commit"],
        "remote_tree": release["source_tree"],
        "canonical_input_raw_sha256": inputs["canonical_input"]["raw_sha256"],
        "selection_raw_sha256": inputs["selection"]["raw_sha256"],
        "selection_identity_raw_sha256": inputs["selection_identity"]["raw_sha256"],
        "review_disposition_raw_sha256": inputs["review_disposition"]["raw_sha256"],
        "ready_index_raw_sha256": inputs["ready_index"]["raw_sha256"],
        "requirements_contract_hash": release["requirements_contract_hash"],
        "evaluator_fingerprints_sha256": release["evaluator_fingerprints_sha256"],
        "suite_receipt_sha256": release["suite_receipt_sha256"],
        "w17_receipt_sha256": release["w17_receipt_sha256"],
    }


def plan(request_id: str = "successor-full308") -> admission.HistoricalFullRerunPlan:
    value = request(request_id)
    return admission.build_historical_full_rerun_plan(
        value, expected_request_sha256=admission.sha256_value(value)
    )


def bootstrap(value: admission.HistoricalFullRerunPlan, **kwargs) -> dict:
    return admission.issue_historical_full_rerun_bootstrap(
        value, owner="songying", hmac_key=KEY, now=kwargs.pop("now", NOW),
        receipt_id=kwargs.pop("receipt_id", "bootstrap-1"),
        reservation_id=kwargs.pop("reservation_id", "reservation-1"), **kwargs,
    )


def call(operation: str, *, receipt=None) -> dict:
    value = request()
    return vm_task_tool.vm_task_historical_full_rerun_service(
        operation=operation, request=value,
        expected_request_sha256=admission.sha256_value(value),
        bootstrap_receipt=receipt,
    )


def result_artifacts(
    value: admission.HistoricalFullRerunPlan, receipt: dict
) -> dict[str, str]:
    root = admission.HISTORICAL_HOST_TMP_ROOT / value.task_id
    final_body = {
        "schema_version": admission.HISTORICAL_FINAL_SEAL_SCHEMA,
        "execution_manifest_sha256": "1" * 64,
        "execution_manifest_semantic_sha256": "2" * 64,
        "run_identity": {
            "run_id": value.plan["run_id"], "plan_sha256": value.plan_sha256,
            "attempt_id": value.plan["attempt_id"],
        },
        "source_identity": {}, "contract_identity": {}, "input_identity": {},
        "authority_provenance": {}, "scheduler": {},
        "shards_semantic_sha256": "3" * 64,
        "sealed_at": "2026-08-18T06:10:00+00:00",
        "item_count": 10, "terminal_complete": True, "all_pass": False,
        "ordered_work_item_ids_sha256": (
            admission.HISTORICAL_ORDERED_WORK_ITEM_IDS_SHA256
        ),
        "manifest_items_semantic_sha256": "5" * 64,
    }
    _final, final_data = admission._seal_historical_document(final_body)
    final_path = root / "final/execution-final-seal.json"
    final_path.parent.mkdir(parents=True, exist_ok=True)
    final_path.write_bytes(final_data)
    reservation = {
        "schema_version": admission.HISTORICAL_RESERVATION_SCHEMA,
        "receipt_id": receipt["receipt_id"],
        "reservation_id": receipt["reservation_id"], "ledger_sequence": 1,
        "task_id": value.task_id, "plan_sha256": value.plan_sha256,
        "lane_count": 3, "started_at": NOW.isoformat(),
        "lease_expires_at": (NOW + timedelta(days=1)).isoformat(),
        "observed_global_peak": 3, "max_global_evaluation_lanes": 3,
        "queue_if_blocked": False,
    }
    reservation_path = root / "control/host-lane-reservation.json"
    reservation_path.parent.mkdir(parents=True, exist_ok=True)
    reservation_path.write_bytes(admission.canonical_bytes(reservation) + b"\n")
    return admission.derive_historical_result_binding(value)


@pytest.fixture(autouse=True)
def fixed_runtime(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    assert FIXED_SMOKE10_IDS_SHA256 == (
        "880d07a3d01e1307310121f23e18412560d5f0ee64711015da9f71d843d0517d"
    )
    monkeypatch.setenv(admission.HMAC_ENV, KEY)
    monkeypatch.setattr(vm_task_tool, "validate_workspace_runtime", lambda: RUNTIME)
    host_root = tmp_path / "fixed-inputs"
    contract = copy.deepcopy(admission.HISTORICAL_INPUT_CONTRACT)
    ready_ids = [str(7040000000 + index) for index in range(308)]
    rows = [{"work_item_id": work_item_id} for work_item_id in ready_ids]
    vm_path = Path(contract["ready_index"]["path"])
    host_path = host_root / vm_path.relative_to("/mnt/tmp")
    host_path.parent.mkdir(parents=True)
    data = b"".join(admission.canonical_bytes(row) + b"\n" for row in rows)
    host_path.write_bytes(data)
    contract["ready_index"].update({
        "raw_sha256": hashlib.sha256(data).hexdigest(),
        "semantic_sha256": admission.sha256_value(rows),
        "ordered_work_item_ids_sha256": admission.sha256_value(ready_ids),
    })
    ranked = sorted(
        (
            hashlib.sha256(
                (
                    admission.HISTORICAL_PROFILE_DOMAIN
                    + "\0"
                    + contract["ready_index"]["raw_sha256"]
                    + "\0"
                    + work_item_id
                ).encode("utf-8")
            ).hexdigest(),
            work_item_id,
        )
        for work_item_id in ready_ids
    )
    smoke10_ids_sha256 = admission.sha256_value(
        [work_item_id for _rank, work_item_id in ranked[:10]]
    )
    state_root = tmp_path / "state"
    monkeypatch.setattr(admission, "HISTORICAL_HOST_TMP_ROOT", host_root)
    monkeypatch.setattr(admission, "HISTORICAL_STATE_ROOT", state_root)
    monkeypatch.setattr(admission, "HISTORICAL_INPUT_CONTRACT", contract)
    monkeypatch.setattr(
        admission,
        "HISTORICAL_READY_INDEX_RAW_SHA256",
        contract["ready_index"]["raw_sha256"],
    )
    monkeypatch.setattr(
        admission,
        "HISTORICAL_ORDERED_WORK_ITEM_IDS_SHA256",
        smoke10_ids_sha256,
    )
    source_commit, source_tree = "a" * 40, "b" * 40
    fingerprints = {
        "g1q3_rca/aeb_signal_parser.py":
            "fa227f22a684f2a4b0808fefe0d596a032c3a582e1571f67ede7937d5894e3d3",
    }
    receipts = {}
    for label in ("suite", "w17"):
        vm_receipt_path = Path("/mnt/tmp/final-successor/%s-receipt.json" % label)
        host_receipt_path = host_root / vm_receipt_path.relative_to("/mnt/tmp")
        host_receipt_path.parent.mkdir(parents=True, exist_ok=True)
        receipt_value = {
            "source_commit": source_commit, "source_tree": source_tree,
            "status": "GREEN",
        }
        receipt_data = admission.canonical_bytes(receipt_value) + b"\n"
        host_receipt_path.write_bytes(receipt_data)
        receipts[label] = (vm_receipt_path, hashlib.sha256(receipt_data).hexdigest())
    monkeypatch.setattr(admission, "HISTORICAL_SUCCESSOR_RELEASE", {
        "source_commit": source_commit,
        "source_tree": source_tree,
        "requirements_contract_hash": "04" * 32,
        "evaluator_fingerprints": fingerprints,
        "evaluator_fingerprints_sha256": admission.sha256_value(fingerprints),
        "evaluator_version": "git-" + ("a" * 40),
        "suite_receipt_path": str(receipts["suite"][0]),
        "suite_receipt_sha256": receipts["suite"][1],
        "w17_receipt_path": str(receipts["w17"][0]),
        "w17_receipt_sha256": receipts["w17"][1],
    })


def test_plan_and_argv_are_exact_and_server_fixed(tmp_path: Path) -> None:
    value = plan()
    assert value.plan_bytes == admission.canonical_bytes(value.plan) + b"\n"
    assert value.plan["evaluator_version"] == "git-" + value.request["remote_commit"]
    assert set(value.plan) == {
        "attempt_id", "budgets", "canonical_input", "evaluator_fingerprints",
        "evaluator_fingerprints_sha256", "evaluator_version", "execution_policy",
        "host_reservation", "output_root", "plan_id", "ready_index",
        "execution_profile",
        "release_evidence", "requirements_contract_hash", "review_disposition",
        "run_id", "schema_version", "selection", "selection_identity",
        "self_seal", "shards", "source", "task_id",
    }
    assert value.output_root == value.task_root
    assert value.plan_path == value.task_root / "control/historical-full-chain-plan.json"
    assert value.task_id.startswith("g1q3-rca-full308-")
    assert [item["item_count"] for item in value.plan["shards"]] == [4, 3, 3]
    assert value.plan["ready_index"]["item_count"] == 308
    assert value.plan["execution_profile"] == {
        "schema_version": admission.HISTORICAL_PROFILE_SCHEMA,
        "profile": "production_smoke10_v1",
        "domain": "g1q3-rca-production-smoke10/v1",
        "ranking": (
            "sha256(domain_nul_ready_index_raw_sha256_nul_work_item_id)_ascending/v1"
        ),
        "sample_count": 10,
        "source_ready_index_raw_sha256": value.plan["ready_index"]["raw_sha256"],
        "ordered_work_item_ids_sha256": (
            admission.HISTORICAL_ORDERED_WORK_ITEM_IDS_SHA256
        ),
    }
    shard_ids = [
        work_item_id
        for _path, data in value.shard_artifacts
        for work_item_id in json.loads(data)["ordered_work_item_ids"]
    ]
    assert len(shard_ids) == len(set(shard_ids)) == 10
    assert admission.sha256_value(shard_ids) == (
        value.plan["execution_profile"]["ordered_work_item_ids_sha256"]
    )
    assert value.plan["host_reservation"] == {
        "schema_version": admission.HISTORICAL_RESERVATION_SCHEMA,
        "path": f"/mnt/tmp/{value.task_id}/control/host-lane-reservation.json",
        "max_global_evaluation_lanes": 3,
    }
    assert value.plan["task_id"] == value.task_id
    assert value.plan["source"] == {
        "commit": value.request["remote_commit"],
        "tree": value.request["remote_tree"],
    }
    assert "source_manifest_sha256" not in value.plan_bytes.decode("utf-8")
    assert "request_identity" not in value.plan
    assert value.plan["execution_policy"]["queue_if_blocked"] is False
    assert value.plan["execution_policy"]["allow_feishu_writeback"] is False
    assert value.plan["budgets"] == admission.HISTORICAL_BUDGETS
    assert value.plan["self_seal"]["artifact_size_bytes"] == len(value.plan_bytes)
    materialized = admission.materialize_historical_full_rerun_plan(
        value, host_tmp_root=tmp_path / "materialized"
    )
    assert materialized.read_bytes() == value.plan_bytes
    for path, data in value.shard_artifacts:
        relative = path.relative_to(value.task_root)
        assert (tmp_path / "materialized" / value.task_id / relative).read_bytes() == data
    assert admission.build_historical_full_rerun_execute_argv(value) == [
        "/usr/bin/python3.8", "-B", admission.HISTORICAL_PREPARE, "execute",
        "--plan", str(value.plan_path), "--plan-sha256", value.plan_sha256,
    ]
    verify = admission.build_historical_full_rerun_verify_argv(
        value, full_chain_output_seal_sha256="08" * 32
    )
    assert verify[-2:] == ["--final-execution-seal-sha256", "08" * 32]
    assert "--full-chain-output-seal-sha256" not in verify
    assert verify[:2] == ["/usr/bin/python3.8", "-B"]
    assert verify[2] == str(
        admission.HISTORICAL_FROZEN_SOURCE_ROOT
        / value.task_id / "root" / admission.HISTORICAL_RUNNER
    )
    assert verify[3:6] == ["verify", "--run-root", str(value.task_root)]


def test_request_rejects_operator_controls_and_wrong_hash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = {**request(), "command": "operator-selected"}
    with pytest.raises(admission.RcaProdAdmissionError, match="request_schema_invalid"):
        admission.build_historical_full_rerun_plan(
            value, expected_request_sha256=admission.sha256_value(value)
        )
    for control in ("sample_count", "limit", "subset"):
        controlled = {**request(), control: 10}
        with pytest.raises(
            admission.RcaProdAdmissionError, match="request_schema_invalid"
        ):
            admission.build_historical_full_rerun_plan(
                controlled,
                expected_request_sha256=admission.sha256_value(controlled),
            )
    with pytest.raises(admission.RcaProdAdmissionError, match="request_hash_mismatch"):
        admission.build_historical_full_rerun_plan(
            request(), expected_request_sha256="f" * 64
        )
    invalid_pin = request()
    invalid_pin["ready_index_raw_sha256"] = "0" * 64
    with pytest.raises(admission.RcaProdAdmissionError, match="input_pin_mismatch"):
        admission.build_historical_full_rerun_plan(
            invalid_pin, expected_request_sha256=admission.sha256_value(invalid_pin)
        )
    wrong_successor = request()
    wrong_successor["remote_commit"] = "c" * 40
    with pytest.raises(admission.RcaProdAdmissionError, match="successor_identity_mismatch"):
        admission.build_historical_full_rerun_plan(
            wrong_successor,
            expected_request_sha256=admission.sha256_value(wrong_successor),
        )
    wrong_evaluator = dict(admission.HISTORICAL_SUCCESSOR_RELEASE or {})
    wrong_evaluator["evaluator_version"] = "g1q3_rca_evaluator_scope_v6"
    monkeypatch.setattr(admission, "HISTORICAL_SUCCESSOR_RELEASE", wrong_evaluator)
    with pytest.raises(admission.RcaProdAdmissionError, match="successor_release_invalid"):
        admission.build_historical_full_rerun_plan(
            request(), expected_request_sha256=admission.sha256_value(request())
        )


def test_bootstrap_is_owner_signed_plan_bound_and_fresh() -> None:
    value, receipt = plan(), None
    receipt = bootstrap(value)
    assert set(receipt) == admission.HISTORICAL_BOOTSTRAP_FIELDS
    assert set(receipt["bindings"]) == admission.HISTORICAL_BOOTSTRAP_BINDINGS
    assert receipt["bindings"]["plan_sha256"] == value.plan_sha256
    assert receipt["bindings"]["queue_if_blocked"] is False
    admission.validate_historical_full_rerun_bootstrap(
        receipt, plan=value, expected_owner="songying", hmac_key=KEY, now=NOW
    )
    receipt["bindings"]["max_global_evaluation_lanes"] = 2
    with pytest.raises(admission.RcaProdAdmissionError):
        admission.validate_historical_full_rerun_bootstrap(
            receipt, plan=value, expected_owner="songying", hmac_key=KEY, now=NOW
        )


def test_atomic_ledger_writes_exact_sidecar_and_verify_reads_history(tmp_path: Path) -> None:
    value, receipt = plan(), bootstrap(plan())
    state_root, host_root = tmp_path / "state", tmp_path / "hfs"
    result = admission.consume_historical_bootstrap_and_reserve_lanes(
        receipt, plan=value, expected_owner="songying", hmac_key=KEY, now=NOW,
        state_root=state_root, host_tmp_root=host_root,
    )
    sidecar_path = host_root / value.task_id / "control/host-lane-reservation.json"
    raw = sidecar_path.read_bytes()
    sidecar = json.loads(raw)
    assert set(sidecar) == admission.HISTORICAL_RESERVATION_FIELDS
    assert raw == admission.canonical_bytes(sidecar) + b"\n"
    assert result["raw_sha256"] == hashlib.sha256(raw).hexdigest()
    assert result["semantic_sha256"] == admission.sha256_value(sidecar)
    assert sidecar["lane_count"] == sidecar["observed_global_peak"] == 3
    assert sidecar["max_global_evaluation_lanes"] == 3
    assert sidecar["queue_if_blocked"] is False
    verified = admission.verify_historical_lane_reservation(
        value, raw_sha256=result["raw_sha256"],
        semantic_sha256=result["semantic_sha256"],
        state_root=state_root, host_tmp_root=host_root,
    )
    assert verified == sidecar
    with pytest.raises(admission.RcaProdAdmissionError, match="already_consumed"):
        admission.consume_historical_bootstrap_and_reserve_lanes(
            receipt, plan=value, expected_owner="songying", hmac_key=KEY, now=NOW,
            state_root=state_root, host_tmp_root=host_root,
        )


def test_release_is_exact_idempotent_and_frees_three_lanes(tmp_path: Path) -> None:
    first, second = plan("release-first"), plan("release-second")
    state_root, host_root = tmp_path / "state", tmp_path / "hfs"
    first_receipt = bootstrap(first)
    reserved = admission.consume_historical_bootstrap_and_reserve_lanes(
        first_receipt, plan=first, expected_owner="songying", hmac_key=KEY,
        now=NOW, state_root=state_root, host_tmp_root=host_root,
    )
    released = admission.release_historical_lane_reservation(
        first, receipt_id=first_receipt["receipt_id"],
        reservation_id=first_receipt["reservation_id"],
        raw_sha256=reserved["raw_sha256"],
        semantic_sha256=reserved["semantic_sha256"], reason="verify_succeeded",
        state_root=state_root, host_tmp_root=host_root,
    )
    assert released["released"] is True
    again = admission.release_historical_lane_reservation(
        first, receipt_id=first_receipt["receipt_id"],
        reservation_id=first_receipt["reservation_id"],
        raw_sha256=reserved["raw_sha256"],
        semantic_sha256=reserved["semantic_sha256"], reason="verify_succeeded",
        state_root=state_root, host_tmp_root=host_root,
    )
    assert again == {
        "released": False, "already_released": True,
        "reservation_id": first_receipt["reservation_id"],
        "reason": "verify_succeeded",
    }
    second_receipt = bootstrap(
        second, receipt_id="bootstrap-2", reservation_id="reservation-2"
    )
    admission.consume_historical_bootstrap_and_reserve_lanes(
        second_receipt, plan=second, expected_owner="songying", hmac_key=KEY,
        now=NOW, state_root=state_root, host_tmp_root=host_root,
    )
    ledger = json.loads((state_root / "evaluation-lanes.json").read_text())
    assert list(ledger["active"]) == ["reservation-2"]
    assert first_receipt["receipt_id"] in ledger["consumed_receipts"]


def test_global_lane_cap_has_no_queue(tmp_path: Path) -> None:
    first, second = plan("first-full308"), plan("second-full308")
    state_root, host_root = tmp_path / "state", tmp_path / "hfs"
    admission.consume_historical_bootstrap_and_reserve_lanes(
        bootstrap(first), plan=first, expected_owner="songying", hmac_key=KEY,
        now=NOW, state_root=state_root, host_tmp_root=host_root,
    )
    second_receipt = bootstrap(
        second, now=NOW + timedelta(minutes=1), receipt_id="bootstrap-2",
        reservation_id="reservation-2",
    )
    with pytest.raises(admission.RcaProdAdmissionError, match="lanes_unavailable"):
        admission.consume_historical_bootstrap_and_reserve_lanes(
            second_receipt, plan=second, expected_owner="songying", hmac_key=KEY,
            now=NOW + timedelta(minutes=1), state_root=state_root,
            host_tmp_root=host_root,
        )
    assert not (host_root / second.task_id).exists()


def test_global_lane_reservation_is_atomic_under_competing_execute(
    tmp_path: Path,
) -> None:
    plans = [plan("parallel-first"), plan("parallel-second")]
    receipts = [
        bootstrap(plans[0], receipt_id="parallel-bootstrap-1", reservation_id="parallel-1"),
        bootstrap(plans[1], receipt_id="parallel-bootstrap-2", reservation_id="parallel-2"),
    ]
    state_root, host_root = tmp_path / "state", tmp_path / "hfs"

    def reserve(index: int) -> str:
        try:
            admission.consume_historical_bootstrap_and_reserve_lanes(
                receipts[index], plan=plans[index], expected_owner="songying",
                hmac_key=KEY, now=NOW, state_root=state_root,
                host_tmp_root=host_root,
            )
        except admission.RcaProdAdmissionError as exc:
            return exc.code
        return "reserved"

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(reserve, (0, 1)))
    assert sorted(results) == [
        "rca_historical_evaluation_lanes_unavailable",
        "reserved",
    ]
    ledger = json.loads((state_root / "evaluation-lanes.json").read_text())
    assert len(ledger["active"]) == 1
    assert sum(record["lane_count"] for record in ledger["active"].values()) == 3


def test_service_surface_plan_is_pure_and_not_registered(monkeypatch: pytest.MonkeyPatch) -> None:
    assert list(inspect.signature(vm_task_tool.vm_task_historical_full_rerun_service).parameters) == [
        "operation", "request", "expected_request_sha256", "bootstrap_receipt"
    ]
    assert "vm_task_historical_full_rerun_service" not in registry.get_all_tool_names()
    forbidden = lambda *_args, **_kwargs: pytest.fail("plan reached a side effect")
    monkeypatch.setattr(vm_task_tool, "vm_task_status", forbidden)
    monkeypatch.setattr(vm_task_tool, "materialize_historical_full_rerun_plan", forbidden)
    monkeypatch.setattr(vm_task_tool, "_vm_task_submit_trusted", forbidden)
    result = call("plan")
    assert result["success"] is True and result["operation"] == "plan"


def test_execute_materializes_then_reserves_only_at_create_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value, receipt, events, captured = plan(), bootstrap(plan(), now=datetime.now(timezone.utc)), [], {}
    monkeypatch.setattr(
        vm_task_tool, "vm_task_status",
        lambda *_args, **_kwargs: {"success": False, "state": "missing", "task_id": value.task_id},
    )
    monkeypatch.setattr(
        vm_task_tool, "materialize_historical_full_rerun_plan",
        lambda _plan: events.append("materialize") or _plan.plan_path,
    )

    def reserve(_receipt, **kwargs):
        events.append("reserve")
        assert kwargs["plan"].plan_sha256 == value.plan_sha256
        return {"reservation": {"lane_count": 3}, "raw_sha256": "1" * 64, "semantic_sha256": "2" * 64}

    def submit(**kwargs):
        captured.update(kwargs); events.append("submit-boundary")
        assert kwargs["pre_create_guard"]() is None
        events.append("create")
        return {"success": True, "returncode": 0}

    monkeypatch.setattr(vm_task_tool, "consume_historical_bootstrap_and_reserve_lanes", reserve)
    monkeypatch.setattr(vm_task_tool, "_vm_task_submit_trusted", submit)
    result = call("execute", receipt=receipt)
    assert result["success"] is True
    assert events == ["materialize", "submit-boundary", "reserve", "create"]
    assert captured["resource_class"] == "rca_prod"
    assert captured["agent_backend"] == "none"
    assert captured["routing_meta_extra"]["queue_if_blocked"] is False
    serialized = json.dumps(captured, default=str, sort_keys=True)
    assert "verification_key_sha256" not in serialized
    assert "ensure_development_signing_key" not in serialized


def test_verify_derives_binding_from_fixed_final_and_reservation_artifacts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value, checked, released = plan(), {}, {}
    receipt = bootstrap(value)
    binding = result_artifacts(value, receipt)
    status = {
        "success": True, "state": "completed", "task_id": value.task_id,
        "title": vm_task_tool._historical_title(value), "owner": "songying",
        "meta": {
            **vm_task_tool._historical_meta(value),
            "rca_prod_admission_receipt": receipt,
        },
    }
    monkeypatch.setattr(vm_task_tool, "vm_task_status", lambda *_args, **_kwargs: status)
    monkeypatch.setattr(
        vm_task_tool,
        "_run_historical_offline_verify",
        lambda *_args, **_kwargs: {
            "ok": True, "terminal_complete": True, "all_pass": False,
            "item_count": 10, "source_manifest_sha256": "6" * 64,
        },
    )

    def verify(plan_arg, **kwargs):
        checked.update(kwargs); assert plan_arg.plan_sha256 == value.plan_sha256
        return {
            "schema_version": admission.HISTORICAL_RESERVATION_SCHEMA,
            "lane_count": 3,
            "receipt_id": receipt["receipt_id"],
            "reservation_id": receipt["reservation_id"],
        }

    monkeypatch.setattr(vm_task_tool, "verify_historical_lane_reservation", verify)
    monkeypatch.setattr(
        vm_task_tool, "release_historical_lane_reservation",
        lambda _plan, **kwargs: released.update(kwargs) or {
            "released": True, "already_released": False,
        },
    )
    result = call("verify")
    assert result["success"] is True
    assert checked == {
        "raw_sha256": binding["host_reservation_raw_sha256"],
        "semantic_sha256": binding["host_reservation_semantic_sha256"],
    }
    assert result["verify_argv"][-1] == binding["full_chain_output_seal_sha256"]
    assert released["reason"] == "verify_succeeded"
    assert released["receipt_id"] == receipt["receipt_id"]

    receipt["hmac_sha256"] = "0" * 64
    result = call("verify")
    assert result["success"] is False
    assert result["error_code"] == "rca_historical_verify_blocked"


def test_verify_runs_offline_verifier_before_releasing_lane(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value, receipt, events = plan(), None, []
    receipt = bootstrap(value)
    binding = result_artifacts(value, receipt)
    status = {
        "success": True, "state": "completed", "task_id": value.task_id,
        "title": vm_task_tool._historical_title(value), "owner": "songying",
        "meta": {
            **vm_task_tool._historical_meta(value),
            "rca_prod_admission_receipt": receipt,
        },
    }
    monkeypatch.setattr(vm_task_tool, "vm_task_status", lambda *_a, **_k: status)
    monkeypatch.setattr(
        vm_task_tool, "verify_historical_lane_reservation",
        lambda *_a, **_k: {"receipt_id": receipt["receipt_id"], "reservation_id": receipt["reservation_id"]},
    )
    monkeypatch.setattr(
        vm_task_tool,
        "_run_historical_offline_verify",
        lambda *_a, **_k: events.append("offline") or {
            "ok": True, "terminal_complete": True, "all_pass": False,
            "item_count": 10, "source_manifest_sha256": "6" * 64,
        },
    )
    monkeypatch.setattr(
        vm_task_tool,
        "release_historical_lane_reservation",
        lambda *_a, **_k: events.append("release") or {"released": True},
    )
    result = call("verify")
    assert result["success"] is True
    assert events == ["offline", "release"]
    assert result["offline_verify"]["source_manifest_sha256"] == "6" * 64
    assert result["verify_argv"][-1] == binding["full_chain_output_seal_sha256"]


@pytest.mark.parametrize(
    "mode", ["nonzero", "malformed", "ok_false", "wrong_item_count"]
)
def test_offline_verifier_rejects_non_success_projections(
    mode: str,
) -> None:
    value = plan()
    argv = admission.build_historical_full_rerun_verify_argv(
        value, full_chain_output_seal_sha256="8" * 64
    )

    if mode == "nonzero":
        completed = subprocess.CompletedProcess(argv, 7, stdout="", stderr="denied")
    elif mode == "malformed":
        completed = subprocess.CompletedProcess(argv, 0, stdout="not-json", stderr="")
    elif mode == "ok_false":
        completed = subprocess.CompletedProcess(
            argv, 0,
            stdout=json.dumps({
                "ok": False, "terminal_complete": True, "all_pass": False,
                "item_count": 10, "source_manifest_sha256": "6" * 64,
            }),
            stderr="",
        )
    else:
        completed = subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps({
                "ok": True, "terminal_complete": True, "all_pass": False,
                "item_count": 308, "source_manifest_sha256": "6" * 64,
            }),
            stderr="",
        )

    with pytest.raises(admission.RcaProdAdmissionError, match="offline_verify"):
        vm_task_tool._run_historical_offline_verify(
            value, argv, run_func=lambda *_a, **_k: completed
        )


def test_offline_verifier_uses_fixed_remote_boundary() -> None:
    value = plan()
    argv = admission.build_historical_full_rerun_verify_argv(
        value, full_chain_output_seal_sha256="8" * 64
    )
    captured = {}
    projection = {
        "ok": True, "terminal_complete": True, "all_pass": False,
        "item_count": 10, "source_manifest_sha256": "6" * 64,
    }

    def runner(command, **kwargs):
        captured["command"] = command
        captured["input"] = kwargs["input"]
        captured["env"] = kwargs["env"]
        return subprocess.CompletedProcess(command, 0, stdout=json.dumps(projection), stderr="")

    assert vm_task_tool._run_historical_offline_verify(value, argv, run_func=runner) == projection
    assert captured["command"] == [str(Path.home() / ".local/bin/ssh-mini-agent"), "run_bash_json"]
    assert "PYTHONPATH=" in captured["input"]
    assert shlex.join(argv) in captured["input"]
    assert captured["env"]["PYTHONDONTWRITEBYTECODE"] == "1"


def test_verify_retains_lane_when_offline_verifier_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value, receipt = plan(), None
    receipt = bootstrap(value)
    result_artifacts(value, receipt)
    status = {
        "success": True, "state": "completed", "task_id": value.task_id,
        "title": vm_task_tool._historical_title(value), "owner": "songying",
        "meta": {
            **vm_task_tool._historical_meta(value),
            "rca_prod_admission_receipt": receipt,
        },
    }
    monkeypatch.setattr(vm_task_tool, "vm_task_status", lambda *_a, **_k: status)
    monkeypatch.setattr(
        vm_task_tool,
        "verify_historical_lane_reservation",
        lambda *_a, **_k: {"receipt_id": receipt["receipt_id"], "reservation_id": receipt["reservation_id"]},
    )
    monkeypatch.setattr(
        vm_task_tool,
        "_run_historical_offline_verify",
        lambda *_a, **_k: (_ for _ in ()).throw(
            admission.RcaProdAdmissionError("rca_historical_offline_verify_failed")
        ),
    )
    monkeypatch.setattr(
        vm_task_tool,
        "release_historical_lane_reservation",
        lambda *_a, **_k: pytest.fail("lane released before offline verify succeeded"),
    )
    result = call("verify")
    assert result["success"] is False
    assert result["error_code"] == "rca_historical_verify_blocked"


def test_result_binding_rejects_noncanonical_and_plan_drifted_final() -> None:
    value, receipt = plan(), bootstrap(plan())
    result_artifacts(value, receipt)
    final_path = (
        admission.HISTORICAL_HOST_TMP_ROOT / value.task_id
        / "final/execution-final-seal.json"
    )
    final_path.write_bytes(final_path.read_bytes() + b"\n")
    with pytest.raises(admission.RcaProdAdmissionError, match="final_seal_schema_invalid"):
        admission.derive_historical_result_binding(value)

    final = json.loads(final_path.read_bytes().rstrip(b"\n"))
    final.pop("self_seal")
    final["run_identity"]["plan_sha256"] = "f" * 64
    _sealed, data = admission._seal_historical_document(final)
    final_path.write_bytes(data)
    with pytest.raises(admission.RcaProdAdmissionError, match="final_seal_identity_invalid"):
        admission.derive_historical_result_binding(value)


def test_result_binding_rejects_a_different_smoke10_id_set() -> None:
    value = plan()
    receipt = bootstrap(value)
    result_artifacts(value, receipt)
    final_path = (
        admission.HISTORICAL_HOST_TMP_ROOT / value.task_id
        / "final/execution-final-seal.json"
    )
    final = json.loads(final_path.read_bytes())
    final.pop("self_seal")
    final["ordered_work_item_ids_sha256"] = "f" * 64
    _sealed, data = admission._seal_historical_document(final)
    final_path.write_bytes(data)

    with pytest.raises(
        admission.RcaProdAdmissionError, match="final_seal_identity_invalid"
    ):
        admission.derive_historical_result_binding(value)


def test_execute_rolls_back_lane_only_after_failure_and_missing_reconfirmed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = plan()
    receipt = bootstrap(value, now=datetime.now(timezone.utc))
    statuses = iter((
        {"success": False, "state": "missing", "task_id": value.task_id},
        {"success": False, "state": "missing", "task_id": value.task_id},
    ))
    monkeypatch.setattr(vm_task_tool, "vm_task_status", lambda *_a, **_k: next(statuses))
    monkeypatch.setattr(
        vm_task_tool, "materialize_historical_full_rerun_plan", lambda _plan: _plan.plan_path
    )
    monkeypatch.setattr(
        vm_task_tool, "consume_historical_bootstrap_and_reserve_lanes",
        lambda *_a, **_k: {
            "reservation": {"lane_count": 3},
            "raw_sha256": "1" * 64, "semantic_sha256": "2" * 64,
        },
    )

    def submit(**kwargs):
        assert kwargs["pre_create_guard"]() is None
        return {"success": False, "returncode": 1}

    released = {}
    monkeypatch.setattr(vm_task_tool, "_vm_task_submit_trusted", submit)
    monkeypatch.setattr(
        vm_task_tool, "release_historical_lane_reservation",
        lambda _plan, **kwargs: released.update(kwargs) or {"released": True},
    )
    result = call("execute", receipt=receipt)
    assert result["success"] is False
    assert released["reason"] == "create_failed_missing_reconfirmed"
    assert result["lane_release"] == {"released": True}


def test_execute_retains_lane_when_failure_status_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = plan()
    receipt = bootstrap(value, now=datetime.now(timezone.utc))
    calls = 0

    def status(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            return {"success": False, "state": "missing", "task_id": value.task_id}
        raise RuntimeError("status unavailable")

    monkeypatch.setattr(vm_task_tool, "vm_task_status", status)
    monkeypatch.setattr(
        vm_task_tool, "materialize_historical_full_rerun_plan", lambda _plan: _plan.plan_path
    )
    monkeypatch.setattr(
        vm_task_tool, "consume_historical_bootstrap_and_reserve_lanes",
        lambda *_a, **_k: {
            "reservation": {"lane_count": 3},
            "raw_sha256": "1" * 64, "semantic_sha256": "2" * 64,
        },
    )
    monkeypatch.setattr(
        vm_task_tool, "_vm_task_submit_trusted",
        lambda **kwargs: (
            kwargs["pre_create_guard"](), {"success": False, "returncode": 1}
        )[1],
    )
    monkeypatch.setattr(
        vm_task_tool, "release_historical_lane_reservation",
        lambda *_a, **_k: pytest.fail("uncertain status released lanes"),
    )
    result = call("execute", receipt=receipt)
    assert result["lane_release"] == {"released": False, "retained": True}


def test_public_submit_cannot_bypass_historical_service() -> None:
    result = vm_task_tool.vm_task_submit(
        title="ordinary", goal="ordinary", task_id="g1q3-rca-smoke10-" + "a" * 32,
    )
    assert result["success"] is False
    assert result["error_code"] == "g1q3_rca_service_boundary_required"
