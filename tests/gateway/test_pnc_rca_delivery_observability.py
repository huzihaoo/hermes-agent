from __future__ import annotations

import json
import os

import pytest

from gateway.pnc_rca_delivery_observability import (
    OBSERVATION_SCHEMA_VERSION,
    DeliveryObservationError,
    append_delivery_observation,
    build_delivery_observation,
)


def _row(**overrides):
    value = {
        "schema_version": OBSERVATION_SCHEMA_VERSION,
        "work_item_id": "7041712812",
        "case_key": "g1q3-rca-s1-" + "a" * 64,
        "delivered_at": "2026-07-31T08:00:00+00:00",
        "level": "L1_observation",
        "has_attribution": False,
        "viz_published": True,
        "viz_bytes": 4096,
        "evidence_channel_msg_count": None,
        "evidence_channel_msg_count_not_measured_reason": "channel_read_not_available_in_sealed_contract",
        "evidence_refs_nonempty": None,
        "evidence_refs_nonempty_not_measured_reason": "refs_not_available_in_sealed_contract",
        "pipeline_elapsed_seconds": 12.5,
        "outcome_content_sha256": "a" * 64,
        "remote_receipt_id": "om_123",
        "release_id": "release-20260731",
        "inventory_pin": "b" * 64,
    }
    value.update(overrides)
    return value


def test_append_is_append_only_and_preserves_rows(tmp_path):
    path = tmp_path / "observations.jsonl"
    first = append_delivery_observation(path, _row())
    second = append_delivery_observation(
        path,
        _row(case_key="g1q3-rca-s1-" + "c" * 64, remote_receipt_id="om_456"),
    )

    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert rows == [first, second]
    assert path.stat().st_mode & 0o777 == 0o600


def test_missing_level_is_rejected():
    value = _row()
    value.pop("level")
    with pytest.raises(DeliveryObservationError) as raised:
        build_delivery_observation(value)
    assert raised.value.code == "observation_required_field_missing"


def test_attribution_with_zero_evidence_is_rejected():
    with pytest.raises(DeliveryObservationError) as raised:
        build_delivery_observation(
            _row(has_attribution=True, evidence_channel_msg_count=0)
        )
    assert raised.value.code == "observation_attribution_without_evidence"


def test_unmeasured_nullable_fields_require_reasons():
    value = _row()
    value.pop("evidence_channel_msg_count_not_measured_reason")
    with pytest.raises(DeliveryObservationError) as raised:
        build_delivery_observation(value)
    assert raised.value.code == "observation_required_field_missing"


def test_symlink_destination_is_rejected(tmp_path):
    target = tmp_path / "real.jsonl"
    target.write_text("", encoding="utf-8")
    link = tmp_path / "observations.jsonl"
    link.symlink_to(target)
    with pytest.raises(DeliveryObservationError) as raised:
        append_delivery_observation(link, _row())
    assert raised.value.code == "observation_path_not_regular"

