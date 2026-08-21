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
        "source_commit": "6a46de39cfd3e7a3e6665a6c7307fa4cf974aadd",
        "source_tree": "67b8c6e90578df99b430fce7e08b974c7c25ab43",
        "evaluator_version": "git-6a46de39cfd3e7a3e6665a6c7307fa4cf974aadd",
        "suite_receipt_path": (
            "/mnt/tmp/20260821-rca-s3a-seal-fix/evidence-run-6a46-v3/"
            "successor-evidence-6a46de39/"
            "suite-receipt.json"
        ),
        "suite_receipt_sha256": (
            "d5892772a50f0fb64c0853b5762e4cc115f2e1576d60e7e3d6fd8f3e09188e11"
        ),
        "w17_receipt_path": (
            "/mnt/tmp/20260821-rca-s3a-seal-fix/evidence-run-6a46-v3/"
            "successor-evidence-6a46de39/"
            "w17-receipt.json"
        ),
        "w17_receipt_sha256": (
            "c2d4dd5a9f19123aa762fb299cdd231112fd21a0b917c31c5d18556a6b7feb62"
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
