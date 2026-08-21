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
            "evaluator_version",
            "suite_receipt_path",
            "suite_receipt_sha256",
            "w17_receipt_path",
            "w17_receipt_sha256",
        )
    } == {
        "source_commit": "38552b91c67ed5f5ad2e90c0dda8f8f2fe833597",
        "source_tree": "cda4224decbd2a504a6f00e6dd1397bb842f4da7",
        "evaluator_version": "git-38552b91c67ed5f5ad2e90c0dda8f8f2fe833597",
        "suite_receipt_path": (
            "/mnt/tmp/20260822-rca-timeout-retry/evidence-run-38552b9/"
            "successor-evidence-38552b9/"
            "suite-receipt.json"
        ),
        "suite_receipt_sha256": (
            "6e457eceb316a9a873b4c3867408b6e71159caa9c65320b2c6d79a3de1103611"
        ),
        "w17_receipt_path": (
            "/mnt/tmp/20260822-rca-timeout-retry/evidence-run-38552b9/"
            "successor-evidence-38552b9/"
            "w17-receipt.json"
        ),
        "w17_receipt_sha256": (
            "e2dc2d64979be5e22ca4d88eb21618d38d3c9d51d6a12a1102d8015a01ef56c5"
        ),
    }


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
