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
        "source_commit": "c366945b39caa969bb5c7fddabe0e4683f8a4e8d",
        "source_tree": "54d0d6e61759d1b96d330ba32196e0eb9c213e45",
        "evaluator_version": "git-c366945b39caa969bb5c7fddabe0e4683f8a4e8d",
        "suite_receipt_path": (
            "/mnt/tmp/20260821-101727-rca-successor-evidence-c366945b39/"
            "suite-receipt.json"
        ),
        "suite_receipt_sha256": (
            "c2e11db0be1fd70755c4c8fd0d09cad153d6a86ec234a88972c19322ddfe5062"
        ),
        "w17_receipt_path": (
            "/mnt/tmp/20260821-101727-rca-successor-evidence-c366945b39/"
            "w17-receipt.json"
        ),
        "w17_receipt_sha256": (
            "9b72369f487b78cdd370a29acef484e4d06201d8a1ff82f9155a1e8ef194e6b6"
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
