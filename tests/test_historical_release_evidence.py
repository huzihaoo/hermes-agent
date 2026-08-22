import hashlib
import json
from pathlib import Path

import pytest

from gateway import pnc_rca_prod_admission as admission


def test_blocked_receipt_successor_projection_is_exact() -> None:
    release = admission.HISTORICAL_SUCCESSOR_RELEASE
    assert release is not None
    assert admission._historical_successor_release() == release
    assert {
        key: release[key]
        for key in (
            "source_commit",
            "source_tree",
            "requirements_contract_hash",
            "evaluator_fingerprints_sha256",
            "evaluator_version",
            "suite_receipt_path",
            "suite_receipt_sha256",
            "w17_receipt_path",
            "w17_receipt_sha256",
        )
    } == {
        "source_commit": "c2821d2cdca398f2fc3f5240a8d9a37ad5d15f66",
        "source_tree": "97e0c1400449588065afba3d4e815821a0287a98",
        "requirements_contract_hash": (
            "319b1000f0317345c834f6b9387646f822fb78c372e88529e8d14ce233c4ed87"
        ),
        "evaluator_fingerprints_sha256": (
            "352e8d07611e4006c12ef3a847080c3d520ba2e6c7d759142beb574c137160ad"
        ),
        "evaluator_version": "git-c2821d2cdca398f2fc3f5240a8d9a37ad5d15f66",
        "suite_receipt_path": (
            "/mnt/tmp/20260822-rca-integrated-c2821d2c/evidence-run-c2821d2c/"
            "successor-evidence-c2821d2/"
            "suite-receipt.json"
        ),
        "suite_receipt_sha256": (
            "fcb8b6e47a12936a0072698516399ed53adbba611a4e9805468176918cf57c52"
        ),
        "w17_receipt_path": (
            "/mnt/tmp/20260822-rca-integrated-c2821d2c/evidence-run-c2821d2c/"
            "successor-evidence-c2821d2/"
            "w17-receipt.json"
        ),
        "w17_receipt_sha256": (
            "cd039f6c2652ca8022e8cd279a5bfcb685fb7407e19f4ae1bbbdad44902bd5a5"
        ),
    }
    assert set(release["evaluator_fingerprints"]) == {
        "g1q3_rca/aeb_signal_parser.py",
        "g1q3_rca/rca_evaluators/_raw_streams.py",
        "g1q3_rca/rca_evaluators/acc_debug_spec.py",
        "g1q3_rca/rca_evaluators/hmi_front_target_output.py",
        "g1q3_rca/report_builder.py",
        "g1q3_rca/scripts/check_case_gate.py",
        "g1q3_rca/signal_access.py",
        "g1q3_rca/signal_registry.py",
    }
    assert hashlib.sha256(
        json.dumps(
            release["evaluator_fingerprints"],
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest() == release["evaluator_fingerprints_sha256"]


def test_scoped_receipts_keep_full_suite_claim_explicit() -> None:
    """The pinned receipts must remain scoped and honest about contract debt."""
    release = admission.HISTORICAL_SUCCESSOR_RELEASE
    assert release is not None
    for path_key, hash_key, schema in (
        ("suite_receipt_path", "suite_receipt_sha256", "g1q3_rca_successor_scoped_suite_receipt_v1"),
        ("w17_receipt_path", "w17_receipt_sha256", "g1q3_rca_successor_scoped_w17_receipt_v1"),
    ):
        vm_path = Path(release[path_key])
        try:
            local_path = admission.HISTORICAL_HOST_TMP_ROOT / vm_path.relative_to("/mnt/tmp")
        except ValueError as exc:
            raise AssertionError("scoped receipt path must be under /mnt/tmp") from exc
        if not local_path.is_file():
            pytest.skip(f"authoritative receipt not mounted: {local_path}")
        raw = local_path.read_bytes()
        receipt = json.loads(raw.decode("utf-8"))
        assert hashlib.sha256(raw).hexdigest() == release[hash_key]
        assert receipt["schema_version"] == schema
        assert receipt["status"] == "SCOPED_GREEN"
        assert receipt["source_commit"] == release["source_commit"]
        assert receipt["source_tree"] == release["source_tree"]
        assert receipt["scope"]["full_suite_not_claimed"] is True
        if path_key == "suite_receipt_path":
            regression = (
                "api/g1q3_rca/tests/test_historical_full_chain_minimal.py::"
                "test_evaluator_fingerprint_is_bound_to_frozen_source_entry"
            )
            assert regression in receipt["argv"]
            assert regression in receipt["scope"]["tests"]
        assert receipt["contract_debt"] == {
            "full_suite_status": "NOT_RUN",
            "reason": receipt["contract_debt"]["reason"],
            "status": "OPEN",
        }
