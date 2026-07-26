from __future__ import annotations

import base64
from copy import deepcopy
from datetime import timedelta
import hashlib
import hmac
import json
import os
import subprocess

import pytest

from gateway import pnc_rca_capacity_sample_evidence as evidence
from gateway import pnc_rca_capacity_transition as transition
from gateway.pnc_rca_prod_admission import _load_hmac_key
from gateway.pnc_rca_runtime_identity import RCA_RUNTIME_RELATIVE_FILES
from tests.gateway.test_pnc_rca_prod_bootstrap import (
    ACTIVE_RELEASE_BINDING_SHA,
    EPOCH_ID,
    NOW,
    RELEASE_BOM_SHA,
    TASK_ID,
    issue,
)
from tests.gateway.test_pnc_rca_capacity_transition import bootstrap_state


KEY = _load_hmac_key("hex:" + ("42" * 32))
RELEASE_ID = "rca-release-20260713"
ATTEMPT_ID = "attempt-bootstrap-1"


def test_terminal_hmac_environment_name_is_canonical_host_contract():
    assert evidence.TERMINAL_HMAC_ENV == "HERMES_RCA_PROD_TERMINAL_HMAC_KEY"
    assert "RECEIPT_HMAC_KEY" not in evidence.TERMINAL_HMAC_ENV


def test_capacity_sampler_and_transition_executor_are_runtime_bom_bound():
    assert "gateway/pnc_rca_capacity_sample_evidence.py" in RCA_RUNTIME_RELATIVE_FILES
    assert "scripts/pnc_rca_activation.py" in RCA_RUNTIME_RELATIVE_FILES
    assert (
        "scripts/pnc_rca_capacity_transition_executor.py" in RCA_RUNTIME_RELATIVE_FILES
    )


def _activation(**overrides):
    values = {
        "release_id": RELEASE_ID,
        "bootstrap_epoch_id": EPOCH_ID,
        "release_bom_sha256": RELEASE_BOM_SHA,
        "active_release_binding_sha256": ACTIVE_RELEASE_BINDING_SHA,
        "activated_at": NOW - timedelta(seconds=30),
        "hmac_key": KEY,
        "receipt_id": "producer-activation-1",
    }
    values.update(overrides)
    return evidence.issue_producer_activation_receipt(**values)


def _attestation():
    body = {
        "schema_version": evidence.VM_REMOTE_READ_ATTESTATION_SCHEMA_VERSION,
        "task_id": TASK_ID,
        "attempt_id": ATTEMPT_ID,
        "remote_read": True,
        "input_materialized": False,
        "mdi_download_attempted": False,
        "fallback_used": False,
        "manifest_sha256": "11" * 32,
        "pipeline_sha256": "12" * 32,
        "service_sha256": "13" * 32,
    }
    body["attestation_fingerprint"] = hashlib.sha256(
        evidence.canonical_bytes(body)
    ).hexdigest()
    return body


def _filesystem(path, *, peak=200):
    return {
        "path": path,
        "device": "2050" if path == "/" else "93",
        "filesystem": "ext4" if path == "/" else "cifs",
        "initial_available_bytes": 1000,
        "final_available_bytes": 900,
        "high_available_bytes": 1100,
        "minimum_available_bytes": 1100 - peak,
        "peak_free_drop_bytes": peak,
    }


def _terminal(*, activation=None, admission_result=None, created_at=None):
    activation = activation or _activation()
    admission_result = admission_result or issue()
    receipt = admission_result.receipt
    attestation = _attestation()
    source_files = [{"path": "worker.py", "sha256": "14" * 32}]
    body = {
        "schema_version": evidence.VM_TERMINAL_RECEIPT_SCHEMA_VERSION,
        "receipt_id": "terminal-receipt-1",
        "created_at": evidence._iso(created_at or NOW + timedelta(seconds=30)),
        "task_id": TASK_ID,
        "attempt_id": ATTEMPT_ID,
        "terminal_state": "completed",
        "exit_code": 0,
        "process_exit_code": 0,
        "admission": {
            "receipt_id": receipt["receipt_id"],
            "receipt_raw_sha256": hashlib.sha256(
                evidence.canonical_bytes(receipt)
            ).hexdigest(),
            "receipt_fingerprint": receipt["receipt_fingerprint"],
        },
        "release": {
            "capacity_mode": "bootstrap",
            "release_id": RELEASE_ID,
            "release_approval_id": "release-approval-20260713",
            "bootstrap_epoch_id": EPOCH_ID,
            "release_bom_sha256": RELEASE_BOM_SHA,
            "active_release_binding_sha256": ACTIVE_RELEASE_BINDING_SHA,
        },
        "worker": {
            "commit": "abcdef123456",
            "source_files": source_files,
            "source_manifest_sha256": hashlib.sha256(
                evidence.canonical_bytes(source_files)
            ).hexdigest(),
        },
        "measurement": {
            "schema_version": evidence.VM_CAPACITY_MEASUREMENT_SCHEMA_VERSION,
            "period_seconds": 10.0,
            "max_gap_seconds": 2.0,
            "max_gap_observed_seconds": 1.0,
            "started_at": evidence._iso(NOW + timedelta(seconds=5)),
            "finished_at": evidence._iso(NOW + timedelta(seconds=20)),
            "sample_count": 10,
            "coverage_ok": True,
            "reasons": [],
            "root": _filesystem("/", peak=200),
            "delivery": _filesystem("/mnt/tmp", peak=300),
            "delivery_task_allocated_bytes": 250,
            "delivery_task_node_count": 8,
        },
        "remote_read_attestation": attestation,
        "remote_read_attestation_sha256": hashlib.sha256(
            evidence.canonical_bytes(attestation)
        ).hexdigest(),
    }
    raw_body = evidence.canonical_bytes(body)
    effective = hmac.new(
        KEY, evidence.VM_TERMINAL_RECEIPT_HMAC_DOMAIN, hashlib.sha256
    ).digest()
    body["receipt_fingerprint"] = hashlib.sha256(raw_body).hexdigest()
    body["hmac_sha256"] = hmac.new(effective, raw_body, hashlib.sha256).hexdigest()
    raw = evidence.canonical_bytes(body)
    meta = {
        **admission_result.meta,
        "rca_prod_capacity_sample_eligible": True,
        "rca_prod_terminal_receipt_path": evidence.expected_vm_terminal_receipt_path(
            TASK_ID, ATTEMPT_ID
        ),
        "rca_prod_terminal_receipt_sha256": hashlib.sha256(raw).hexdigest(),
        "rca_prod_terminal_receipt_fingerprint": body["receipt_fingerprint"],
        "rca_prod_capacity_measurement_error": "",
    }
    return raw, meta, activation, admission_result


def _snapshot(meta, *, source_kind="kafka_workflow_event", thread=True):
    effects = [
        {
            "effect_key": "effect-issue-1",
            "effect_kind": "feishu_issue_comment",
            "required": True,
            "target_key": "feishu_project:g1q3:issue:1",
            "status": "succeeded",
            "remote_id": "comment-1",
            "remote_receipt": {"remote_id": "comment-1"},
            "completed_at": evidence._iso(NOW + timedelta(seconds=40)),
            "updated_at": evidence._iso(NOW + timedelta(seconds=40)),
        }
    ]
    subscriptions = []
    if source_kind == "feishu_group_manual" and thread:
        effects.append({
            "effect_key": "effect-thread-1",
            "effect_kind": "feishu_thread_reply",
            "required": True,
            "target_key": "feishu_thread:chat:topic",
            "status": "succeeded",
            "remote_id": "reply-1",
            "remote_receipt": {"remote_id": "reply-1"},
            "completed_at": evidence._iso(NOW + timedelta(seconds=41)),
            "updated_at": evidence._iso(NOW + timedelta(seconds=41)),
        })
        subscriptions.append({
            "subscription_key": "subscription-thread-1",
            "effect_kind": "feishu_thread_reply",
            "target_key": "feishu_thread:chat:topic",
            "required": True,
            "status": "materialized",
            "delivery_id": "delivery-1",
            "effect_key": "effect-thread-1",
            "materialized_at": evidence._iso(NOW + timedelta(seconds=35)),
            "updated_at": evidence._iso(NOW + timedelta(seconds=35)),
        })
    return {
        "schema_version": "pnc_rca_delivery_capacity_snapshot_v1",
        "snapshot_at": evidence._iso(NOW + timedelta(seconds=41)),
        "task_id": TASK_ID,
        "attempt_id": ATTEMPT_ID,
        "source_kind": source_kind,
        "submission_completed_at": evidence._iso(NOW),
        "task_meta": meta,
        "job": {
            "delivery_id": "delivery-1",
            "submission_key": TASK_ID,
            "business_key": "business-1",
            "generation": 1,
            "outcome": "success",
            "status": "delivered",
            "created_at": evidence._iso(NOW + timedelta(seconds=31)),
            "updated_at": evidence._iso(NOW + timedelta(seconds=41)),
        },
        "effects": effects,
        "required_subscriptions": subscriptions,
    }


def test_producer_activation_is_tamper_evident_and_owner_only_create_once(tmp_path):
    receipt = _activation()
    path = evidence.producer_activation_path(tmp_path)
    expected_sha = hashlib.sha256(evidence.canonical_bytes(receipt)).hexdigest()

    assert evidence.write_owner_only_create_once(path, receipt) == expected_sha
    assert evidence.write_owner_only_create_once(path, receipt) == expected_sha
    assert oct(path.stat().st_mode & 0o777) == "0o600"
    observed, raw_sha = evidence.read_and_validate_producer_activation(
        path, hmac_key=KEY
    )
    assert observed == receipt
    assert raw_sha == expected_sha

    tampered = {**receipt, "release_id": "other-release"}
    with pytest.raises(evidence.CapacitySampleEvidenceError, match="tampered"):
        evidence.validate_producer_activation_receipt(tampered, hmac_key=KEY)
    with pytest.raises(evidence.CapacitySampleEvidenceError, match="conflict"):
        evidence.write_owner_only_create_once(path, tampered)


def _seed_interrupted_create_once(path, raw):
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.crashed{evidence.CREATE_ONCE_TEMP_SUFFIX}"
    )
    temporary.write_bytes(raw)
    temporary.chmod(0o600)
    os.link(temporary, path)
    assert path.stat().st_nlink == 2
    return temporary


def test_create_once_recovers_unique_link_before_unlink_crash(tmp_path):
    receipt = _activation()
    raw = evidence.canonical_bytes(receipt)
    path = evidence.producer_activation_path(tmp_path)
    temporary = _seed_interrupted_create_once(path, raw)

    observed_sha = evidence.write_owner_only_create_once(path, receipt)

    assert observed_sha == hashlib.sha256(raw).hexdigest()
    assert not temporary.exists()
    assert path.stat().st_nlink == 1
    assert path.read_bytes() == raw


@pytest.mark.parametrize(
    "attack",
    [
        "foreign_hardlink",
        "extra_candidate",
        "byte_conflict",
        "wrong_mode",
        "symlink",
    ],
)
def test_create_once_recovery_rejects_ambiguous_or_unsafe_state(tmp_path, attack):
    receipt = _activation()
    raw = evidence.canonical_bytes(receipt)
    path = evidence.producer_activation_path(tmp_path)
    if attack == "symlink":
        source = tmp_path / "source.json"
        source.write_bytes(raw)
        source.chmod(0o600)
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        path.symlink_to(source)
    else:
        temporary = _seed_interrupted_create_once(path, raw)
        if attack == "foreign_hardlink":
            os.link(temporary, tmp_path / "foreign-link.json")
        elif attack == "extra_candidate":
            rogue = path.with_name(
                f".{path.name}.rogue{evidence.CREATE_ONCE_TEMP_SUFFIX}"
            )
            rogue.write_bytes(raw)
            rogue.chmod(0o600)
        elif attack == "wrong_mode":
            path.chmod(0o640)

    candidate = (
        _activation(release_id="other-release")
        if attack == "byte_conflict"
        else receipt
    )
    with pytest.raises(evidence.CapacitySampleEvidenceError, match="conflict"):
        evidence.write_owner_only_create_once(path, candidate)


def test_owner_only_reader_rejects_symlink(tmp_path):
    real = tmp_path / "real.json"
    real.write_bytes(evidence.canonical_bytes(_activation()))
    os.chmod(real, 0o600)
    alias = tmp_path / "alias.json"
    alias.symlink_to(real)
    with pytest.raises(evidence.CapacitySampleEvidenceError, match="file_invalid"):
        evidence.read_owner_only_receipt(alias)


def test_vm_terminal_receipt_binds_raw_meta_hmac_and_independent_key():
    raw, meta, activation, admission_result = _terminal()
    verified = evidence.validate_vm_terminal_receipt(
        raw,
        task_meta=meta,
        admission_receipt=admission_result.receipt,
        producer_activation=activation,
        admission_hmac_key=KEY,
    )
    assert verified.raw_sha256 == hashlib.sha256(raw).hexdigest()

    independent = b"i" * 32
    value = json.loads(raw)
    body = {
        key: item
        for key, item in value.items()
        if key not in {"receipt_fingerprint", "hmac_sha256"}
    }
    value["hmac_sha256"] = hmac.new(
        independent, evidence.canonical_bytes(body), hashlib.sha256
    ).hexdigest()
    independent_raw = evidence.canonical_bytes(value)
    independent_meta = {
        **meta,
        "rca_prod_terminal_receipt_sha256": hashlib.sha256(independent_raw).hexdigest(),
    }
    assert (
        evidence.validate_vm_terminal_receipt(
            independent_raw,
            task_meta=independent_meta,
            admission_receipt=admission_result.receipt,
            producer_activation=activation,
            admission_hmac_key=KEY,
            terminal_hmac_key=independent,
        ).receipt
        == value
    )


def test_vm_terminal_receipt_rejects_truncated_created_at_before_fractional_finish():
    raw, meta, activation, admission_result = _terminal()
    value = json.loads(raw)
    value["created_at"] = evidence._iso(NOW + timedelta(seconds=20))
    value["measurement"]["finished_at"] = evidence._iso(
        NOW + timedelta(seconds=20, microseconds=1)
    )
    body = {
        key: item
        for key, item in value.items()
        if key not in {"receipt_fingerprint", "hmac_sha256"}
    }
    body_raw = evidence.canonical_bytes(body)
    value["receipt_fingerprint"] = hashlib.sha256(body_raw).hexdigest()
    effective = hmac.new(
        KEY, evidence.VM_TERMINAL_RECEIPT_HMAC_DOMAIN, hashlib.sha256
    ).digest()
    value["hmac_sha256"] = hmac.new(
        effective, body_raw, hashlib.sha256
    ).hexdigest()
    malformed = evidence.canonical_bytes(value)
    meta["rca_prod_terminal_receipt_sha256"] = hashlib.sha256(malformed).hexdigest()
    meta["rca_prod_terminal_receipt_fingerprint"] = value["receipt_fingerprint"]

    with pytest.raises(
        evidence.CapacitySampleEvidenceError,
        match="rca_capacity_vm_measurement_time_invalid",
    ):
        evidence.validate_vm_terminal_receipt(
            malformed,
            task_meta=meta,
            admission_receipt=admission_result.receipt,
            producer_activation=activation,
            admission_hmac_key=KEY,
        )


@pytest.mark.parametrize("mode", ["raw", "meta", "hmac"])
def test_vm_terminal_receipt_rejects_raw_meta_and_hmac_tamper(mode):
    raw, meta, activation, admission_result = _terminal()
    value = json.loads(raw)
    if mode == "raw":
        raw += b"\n"
    elif mode == "meta":
        meta["rca_prod_terminal_receipt_sha256"] = "00" * 32
    else:
        value["hmac_sha256"] = "00" * 32
        raw = evidence.canonical_bytes(value)
        meta["rca_prod_terminal_receipt_sha256"] = hashlib.sha256(raw).hexdigest()
    with pytest.raises(evidence.CapacitySampleEvidenceError):
        evidence.validate_vm_terminal_receipt(
            raw,
            task_meta=meta,
            admission_receipt=admission_result.receipt,
            producer_activation=activation,
            admission_hmac_key=KEY,
        )


def test_history_fence_rejects_admission_before_activation():
    activation = _activation(activated_at=NOW + timedelta(seconds=1))
    raw, meta, _, admission_result = _terminal(activation=activation)
    with pytest.raises(evidence.CapacitySampleEvidenceError, match="history_fence"):
        evidence.validate_vm_terminal_receipt(
            raw,
            task_meta=meta,
            admission_receipt=admission_result.receipt,
            producer_activation=activation,
            admission_hmac_key=KEY,
        )


def test_all_required_delivery_success_builds_sample_v3():
    raw, meta, activation, _admission_result = _terminal()
    snapshot = _snapshot(meta)
    snapshot_sha = hashlib.sha256(evidence.canonical_bytes(snapshot)).hexdigest()
    activation_sha = hashlib.sha256(evidence.canonical_bytes(activation)).hexdigest()

    result = evidence.build_capacity_sample(
        snapshot=snapshot,
        delivery_snapshot_sha256=snapshot_sha,
        task_meta=meta,
        vm_terminal_raw=raw,
        producer_activation=activation,
        producer_activation_receipt_sha256=activation_sha,
        admission_hmac_key=KEY,
        observed_at=NOW + timedelta(seconds=50),
        sample_id="sample-1",
    )

    assert result.sample["schema_version"] == "rca_capacity_sample_v3"
    assert result.sample["root_peak_bytes"] == 200
    assert result.sample["delivery_peak_bytes"] == 300
    assert result.sample["delivery_used_bytes"] == 250
    assert result.sample["input_materialized_bytes"] == 0
    assert result.sample["host_success_receipt_sha256"] == (
        result.host_success_receipt_sha256
    )


def test_manual_delivery_requires_origin_thread_reply():
    raw, meta, activation, _admission_result = _terminal()
    snapshot = _snapshot(meta, source_kind="feishu_group_manual", thread=False)
    with pytest.raises(
        evidence.CapacitySampleEvidenceError, match="thread_effect_missing"
    ):
        evidence.build_capacity_sample(
            snapshot=snapshot,
            delivery_snapshot_sha256=hashlib.sha256(
                evidence.canonical_bytes(snapshot)
            ).hexdigest(),
            task_meta=meta,
            vm_terminal_raw=raw,
            producer_activation=activation,
            producer_activation_receipt_sha256=hashlib.sha256(
                evidence.canonical_bytes(activation)
            ).hexdigest(),
            admission_hmac_key=KEY,
            observed_at=NOW + timedelta(seconds=50),
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("delivery_id", "other-delivery"),
        ("effect_key", "other-effect"),
        ("effect_kind", "feishu_issue_comment"),
        ("target_key", "feishu_thread:other:topic"),
        ("materialized_at", evidence._iso(NOW + timedelta(seconds=42))),
    ],
)
def test_manual_subscription_must_reference_exact_successful_effect(field, value):
    raw, meta, activation, _admission_result = _terminal()
    snapshot = _snapshot(meta, source_kind="feishu_group_manual")
    snapshot["required_subscriptions"][0][field] = value
    with pytest.raises(
        evidence.CapacitySampleEvidenceError,
        match="subscriptions_invalid",
    ):
        evidence.build_capacity_sample(
            snapshot=snapshot,
            delivery_snapshot_sha256=hashlib.sha256(
                evidence.canonical_bytes(snapshot)
            ).hexdigest(),
            task_meta=meta,
            vm_terminal_raw=raw,
            producer_activation=activation,
            producer_activation_receipt_sha256=hashlib.sha256(
                evidence.canonical_bytes(activation)
            ).hexdigest(),
            admission_hmac_key=KEY,
            observed_at=NOW + timedelta(seconds=50),
        )


def test_effect_completion_before_vm_receipt_and_host_tamper_are_rejected():
    raw, meta, activation, _admission_result = _terminal()
    historical = _snapshot(meta)
    historical["effects"][0]["completed_at"] = evidence._iso(NOW)
    with pytest.raises(evidence.CapacitySampleEvidenceError, match="history_fence"):
        evidence.build_capacity_sample(
            snapshot=historical,
            delivery_snapshot_sha256=hashlib.sha256(
                evidence.canonical_bytes(historical)
            ).hexdigest(),
            task_meta=meta,
            vm_terminal_raw=raw,
            producer_activation=activation,
            producer_activation_receipt_sha256=hashlib.sha256(
                evidence.canonical_bytes(activation)
            ).hexdigest(),
            admission_hmac_key=KEY,
            observed_at=NOW + timedelta(seconds=50),
        )

    snapshot = _snapshot(meta)
    built = evidence.build_capacity_sample(
        snapshot=snapshot,
        delivery_snapshot_sha256=hashlib.sha256(
            evidence.canonical_bytes(snapshot)
        ).hexdigest(),
        task_meta=meta,
        vm_terminal_raw=raw,
        producer_activation=activation,
        producer_activation_receipt_sha256=hashlib.sha256(
            evidence.canonical_bytes(activation)
        ).hexdigest(),
        admission_hmac_key=KEY,
        observed_at=NOW + timedelta(seconds=50),
    )
    tampered = deepcopy(built.host_success_receipt)
    tampered["required_effects"][0]["remote_id"] = "other-comment"
    with pytest.raises(evidence.CapacitySampleEvidenceError, match="tampered"):
        evidence.validate_host_success_receipt(tampered, hmac_key=KEY)


def test_second_sample_reuses_epoch_fixed_producer_receipt_and_appends(tmp_path):
    raw, meta, activation, _admission_result = _terminal()
    snapshot = _snapshot(meta)
    first = evidence.build_capacity_sample(
        snapshot=snapshot,
        delivery_snapshot_sha256=hashlib.sha256(
            evidence.canonical_bytes(snapshot)
        ).hexdigest(),
        task_meta=meta,
        vm_terminal_raw=raw,
        producer_activation=activation,
        producer_activation_receipt_sha256=hashlib.sha256(
            evidence.canonical_bytes(activation)
        ).hexdigest(),
        admission_hmac_key=KEY,
        observed_at=NOW + timedelta(seconds=50),
        sample_id="sample-1",
    ).sample
    second = transition.issue_capacity_sample(
        sample_id="sample-2",
        release_id=first["release_id"],
        bootstrap_epoch_id=first["bootstrap_epoch_id"],
        release_bom_sha256=first["release_bom_sha256"],
        active_release_binding_sha256=first["active_release_binding_sha256"],
        task_id="g1q3-rca-second",
        attempt_id="attempt-second",
        admission_receipt_sha256="21" * 32,
        admission_receipt_fingerprint="22" * 32,
        task_manifest_sha256="23" * 32,
        producer_activation_receipt_sha256=first["producer_activation_receipt_sha256"],
        producer_activation_receipt_fingerprint=first[
            "producer_activation_receipt_fingerprint"
        ],
        vm_terminal_receipt_sha256="24" * 32,
        vm_terminal_receipt_fingerprint="25" * 32,
        host_success_receipt_sha256="26" * 32,
        host_success_receipt_fingerprint="27" * 32,
        terminal_status="succeeded",
        root_peak_bytes=100,
        delivery_peak_bytes=200,
        delivery_used_bytes=50,
        input_materialized_bytes=0,
        observed_at=NOW + timedelta(hours=1),
        hmac_key=KEY,
    )

    ledger = transition.validate_sample_ledger([first, second], hmac_key=KEY)
    assert ledger.sample_count == 2
    assert (
        first["producer_activation_receipt_sha256"]
        == second["producer_activation_receipt_sha256"]
    )
    state = bootstrap_state()
    state["release_id"] = first["release_id"]
    state["bootstrap_epoch_id"] = first["bootstrap_epoch_id"]
    ledger_path = tmp_path / "capacity" / "samples.jsonl"
    ledger_path.parent.mkdir(mode=0o700)
    lock_path = ledger_path.parent / "capacity.lock"
    evidence.ensure_owner_only_lock_file(lock_path)
    with transition.capacity_flock(lock_path, exclusive=True):
        transition.append_capacity_sample(
            ledger_path,
            first,
            hmac_key=KEY,
            persisted_state_loader=lambda: state,
        )
        appended = transition.append_capacity_sample(
            ledger_path,
            second,
            hmac_key=KEY,
            persisted_state_loader=lambda: state,
        )
    assert appended.sample_count == 2
    assert transition.read_sample_ledger(ledger_path, hmac_key=KEY).sample_count == 2


def test_failed_or_receiptless_required_effect_is_rejected():
    raw, meta, activation, _admission_result = _terminal()
    for mutation in ("failed", "receiptless"):
        snapshot = _snapshot(meta)
        if mutation == "failed":
            snapshot["effects"][0]["status"] = "quarantined"
        else:
            snapshot["effects"][0]["remote_receipt"] = {}
        with pytest.raises(evidence.CapacitySampleEvidenceError):
            evidence.build_capacity_sample(
                snapshot=snapshot,
                delivery_snapshot_sha256=hashlib.sha256(
                    evidence.canonical_bytes(snapshot)
                ).hexdigest(),
                task_meta=meta,
                vm_terminal_raw=raw,
                producer_activation=activation,
                producer_activation_receipt_sha256=hashlib.sha256(
                    evidence.canonical_bytes(activation)
                ).hexdigest(),
                admission_hmac_key=KEY,
                observed_at=NOW + timedelta(seconds=50),
            )


def test_remote_reader_enforces_timeout_size_path_sha_and_scrubs_secrets(
    monkeypatch,
):
    raw = evidence.canonical_bytes({"receipt": True})
    expected = evidence.expected_vm_terminal_receipt_path(TASK_ID, ATTEMPT_ID)
    envelope = {
        "ok": True,
        "path": expected,
        "size": len(raw),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "raw_base64": base64.b64encode(raw).decode(),
    }
    monkeypatch.setenv("HERMES_RCA_PROD_ADMISSION_HMAC_KEY", "secret-admission")
    monkeypatch.setenv("HERMES_RCA_PROD_TERMINAL_HMAC_KEY", "secret-terminal")

    def run(command, **kwargs):
        assert command == ["/safe/agent", "run_py_json"]
        assert "HERMES_RCA_PROD_ADMISSION_HMAC_KEY" not in kwargs["input"]
        assert "HERMES_RCA_PROD_ADMISSION_HMAC_KEY" not in kwargs["env"]
        assert "HERMES_RCA_PROD_TERMINAL_HMAC_KEY" not in kwargs["env"]
        return subprocess.CompletedProcess(command, 0, json.dumps(envelope), "")

    assert (
        evidence.read_remote_vm_terminal_receipt(
            ssh_mini_agent="/safe/agent",
            task_id=TASK_ID,
            attempt_id=ATTEMPT_ID,
            timeout_seconds=5,
            run=run,
        )
        == raw
    )
    with pytest.raises(evidence.CapacitySampleEvidenceError, match="timeout_invalid"):
        evidence.read_remote_vm_terminal_receipt(
            ssh_mini_agent="/safe/agent",
            task_id=TASK_ID,
            attempt_id=ATTEMPT_ID,
            timeout_seconds=31,
            run=run,
        )
    bad = deepcopy(envelope)
    bad["path"] = "/tmp/other"
    with pytest.raises(evidence.CapacitySampleEvidenceError, match="reader_invalid"):
        evidence.read_remote_vm_terminal_receipt(
            ssh_mini_agent="/safe/agent",
            task_id=TASK_ID,
            attempt_id=ATTEMPT_ID,
            timeout_seconds=5,
            run=lambda command, **kwargs: subprocess.CompletedProcess(
                command, 0, json.dumps(bad), ""
            ),
        )
    oversized_raw = b"x" * (evidence.MAX_RECEIPT_BYTES + 1)
    oversized = {
        **envelope,
        "size": len(oversized_raw),
        "sha256": hashlib.sha256(oversized_raw).hexdigest(),
        "raw_base64": base64.b64encode(oversized_raw).decode(),
    }
    with pytest.raises(evidence.CapacitySampleEvidenceError):
        evidence.read_remote_vm_terminal_receipt(
            ssh_mini_agent="/safe/agent",
            task_id=TASK_ID,
            attempt_id=ATTEMPT_ID,
            timeout_seconds=5,
            run=lambda command, **kwargs: subprocess.CompletedProcess(
                command, 0, json.dumps(oversized), ""
            ),
        )
