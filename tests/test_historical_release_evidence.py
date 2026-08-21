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
        "source_commit": "82e5eb2b8dffc9f5e7907d4edc46a2958d86c8e2",
        "source_tree": "277cc972b3fd32fbc00569029162e97d0566b178",
        "evaluator_version": "git-82e5eb2b8dffc9f5e7907d4edc46a2958d86c8e2",
        "suite_receipt_path": (
            "/mnt/tmp/20260821-rca-runtime-env-forward/successor-evidence-82e5eb2b/"
            "suite-receipt.json"
        ),
        "suite_receipt_sha256": (
            "9ef63ae22ee1bc008168d1c834f3deb48bc59b105cbdcde0db2bcb41dc8503da"
        ),
        "w17_receipt_path": (
            "/mnt/tmp/20260821-rca-runtime-env-forward/successor-evidence-82e5eb2b/"
            "w17-receipt.json"
        ),
        "w17_receipt_sha256": (
            "b381ed36e435fc5c592e74f536d8c2963481b02b987970656fc8a7eb53c4f4e6"
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
