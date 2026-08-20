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
        "source_commit": "363fb4b438c8bf12baaccf94bb03e9a678b7cb79",
        "source_tree": "8f68f96f8c9b73ed00f49bbd5414373abeece0d1",
        "evaluator_version": "git-363fb4b438c8bf12baaccf94bb03e9a678b7cb79",
        "suite_receipt_path": (
            "/mnt/tmp/g1q3-rca-canonical-scoped-verification-20260819/"
            "blocked-contract-363fb4b438/suite-receipt.json"
        ),
        "suite_receipt_sha256": (
            "00247963de4b25eb9f030527c5899a0f7fb20c22126fe2e92eb98f1b0edddf0a"
        ),
        "w17_receipt_path": (
            "/mnt/tmp/g1q3-rca-canonical-scoped-verification-20260819/"
            "blocked-contract-363fb4b438/w17-receipt.json"
        ),
        "w17_receipt_sha256": (
            "2f6b5e9d82d75b9929d990fbe95e5c7d759643b6bdc10759a73676b60b4fef05"
        ),
    }
