from __future__ import annotations

import pytest

from gateway.pnc_rca_requester_identity import (
    classify_rca_requester,
    validate_rca_requester,
)


@pytest.mark.parametrize(
    ("requester_id", "expected"),
    [
        ("ou_d1d3cfeba1be0a22faa36aaf4fb3907d", "human"),
        ("automation:rca-batch-rerun", "automation"),
        ("operator-songying", "legacy_automation"),
        ("codex-production-coverage", "legacy_automation"),
        ("", "unknown"),
    ],
)
def test_requester_actor_kind_is_structural(requester_id: str, expected: str):
    assert classify_rca_requester(requester_id) == expected


def test_platforms_require_separate_human_and_automation_namespaces():
    assert (
        validate_rca_requester(
            platform="feishu",
            requester_id="ou_d1d3cfeba1be0a22faa36aaf4fb3907d",
        )
        == "human"
    )
    assert (
        validate_rca_requester(
            platform="operator", requester_id="automation:rca-batch-rerun"
        )
        == "automation"
    )
    with pytest.raises(ValueError, match="operator_requester_identity"):
        validate_rca_requester(platform="operator", requester_id="operator-songying")
    with pytest.raises(ValueError, match="feishu_requester_identity"):
        validate_rca_requester(
            platform="feishu", requester_id="automation:rca-batch-rerun"
        )
