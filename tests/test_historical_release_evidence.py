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
        "source_commit": "1364e0d0cf59bcdb4a04ace506903fd07899f7b9",
        "source_tree": "70dea7edddc052fe8f8e246bcaf206952a3fa049",
        "requirements_contract_hash": (
            "319b1000f0317345c834f6b9387646f822fb78c372e88529e8d14ce233c4ed87"
        ),
        "evaluator_fingerprints_sha256": (
            "352e8d07611e4006c12ef3a847080c3d520ba2e6c7d759142beb574c137160ad"
        ),
        "evaluator_version": "git-1364e0d0cf59bcdb4a04ace506903fd07899f7b9",
        "suite_receipt_path": (
            "/mnt/tmp/20260822-rca-integrated-1364e0d/evidence-run-1364e0d/"
            "successor-evidence-1364e0d/"
            "suite-receipt.json"
        ),
        "suite_receipt_sha256": (
            "2f274635757946c577abadb0cf1526a67f6b07b9360f2a2e4fab813c665188e6"
        ),
        "w17_receipt_path": (
            "/mnt/tmp/20260822-rca-integrated-1364e0d/evidence-run-1364e0d/"
            "successor-evidence-1364e0d/"
            "w17-receipt.json"
        ),
        "w17_receipt_sha256": (
            "9cc8042bd85503508fa07a66d6f21a3a7dcc471f20f084809c14a33b053dadeb"
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
        assert receipt["contract_debt"] == {
            "full_suite_status": "NOT_RUN",
            "reason": receipt["contract_debt"]["reason"],
            "status": "OPEN",
        }
