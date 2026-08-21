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
        "source_commit": "a4d021f8c3ef52229416967e6f2bc98bb531f0c7",
        "source_tree": "ad399ad920eef2f78b654b079fae2aac360630f4",
        "evaluator_version": "git-a4d021f8c3ef52229416967e6f2bc98bb531f0c7",
        "suite_receipt_path": (
            "/mnt/tmp/20260821-rca-cwd-fix/successor-evidence-a4d021f8/"
            "suite-receipt.json"
        ),
        "suite_receipt_sha256": (
            "c6bea0cd7b11dd821977c0c90fcd075e08fc93a016723f02a4cb43d2b217c69f"
        ),
        "w17_receipt_path": (
            "/mnt/tmp/20260821-rca-cwd-fix/successor-evidence-a4d021f8/"
            "w17-receipt.json"
        ),
        "w17_receipt_sha256": (
            "8de723173336823fbdb02b4170e446308ed61e320c32dfefd870ba89db438397"
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
