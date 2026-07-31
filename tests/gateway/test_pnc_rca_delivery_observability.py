from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import plistlib
import threading

import pytest

import gateway.pnc_rca_delivery_observability as observability_module
from gateway.pnc_rca_delivery_observability import (
    OBSERVATION_SCHEMA_VERSION,
    DeliveryObservationError,
    append_delivery_observation,
    append_delivery_observation_verified,
    build_delivery_observation,
    delivery_observation_file_lock,
    delivery_observation_id,
    delivery_observation_payload_sha256,
    read_delivery_observation_receipt,
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
        "evaluator_hit_count": 0,
        "pipeline_elapsed_seconds": 12.5,
        "outcome_content_sha256": "a" * 64,
        "remote_receipt_id": "om_123",
        "release_id": "release-20260731",
        "inventory_pin": "b" * 64,
    }
    value.update(overrides)
    if "observation_id" not in overrides:
        value["observation_id"] = delivery_observation_id(value)
    return value


def _canonical_row_bytes(value):
    return json.dumps(
        build_delivery_observation(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def test_observation_schema_v2_rejects_legacy_v1_rows():
    assert OBSERVATION_SCHEMA_VERSION == "pnc_rca_delivery_observation_v2"
    legacy = _row(schema_version="pnc_rca_delivery_observation_v1")

    with pytest.raises(DeliveryObservationError) as raised:
        build_delivery_observation(legacy)

    assert raised.value.code == "observation_schema_version_invalid"


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


def test_receipt_snapshot_binds_exact_canonical_payload_hash(tmp_path):
    path = tmp_path / "observations.jsonl"
    observation = append_delivery_observation(path, _row())

    snapshot = read_delivery_observation_receipt(path)

    assert snapshot.payload_sha256_by_id == {
        observation["observation_id"]: delivery_observation_payload_sha256(
            observation
        )
    }
    assert snapshot.identity[4] == path.stat().st_size
    assert len(snapshot.receipt_sha256) == 64


def test_verified_append_returns_the_post_fsync_receipt_snapshot(tmp_path):
    path = tmp_path / "observations.jsonl"

    result = append_delivery_observation_verified(path, _row())

    assert result.receipt == read_delivery_observation_receipt(path)
    assert result.receipt.payload_sha256_by_id == {
        result.observation["observation_id"]: delivery_observation_payload_sha256(
            result.observation
        )
    }


def test_receipt_rejects_semantically_valid_noncanonical_rewrite(tmp_path):
    path = tmp_path / "observations.jsonl"
    observation = append_delivery_observation(path, _row())
    path.write_text(
        json.dumps(observation, sort_keys=True) + "\n", encoding="utf-8"
    )

    with pytest.raises(DeliveryObservationError) as raised:
        read_delivery_observation_receipt(path)

    assert raised.value.code == "observation_receipt_line_noncanonical"


def test_append_rejects_path_replacement_after_write(tmp_path, monkeypatch):
    path = tmp_path / "observations.jsonl"
    rotated = tmp_path / "observations.rotated.jsonl"
    original_write = observability_module.os.write

    def replace_path_after_write(descriptor, payload):
        written = original_write(descriptor, payload)
        os.replace(path, rotated)
        replacement = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.close(replacement)
        return written

    monkeypatch.setattr(observability_module.os, "write", replace_path_after_write)

    with pytest.raises(DeliveryObservationError) as raised:
        append_delivery_observation(path, _row())

    assert raised.value.code == "observation_receipt_identity_changed"
    assert path.read_bytes() == b""
    assert rotated.read_bytes().endswith(b"\n")


def test_append_recovers_exact_torn_tail_for_pending_row(tmp_path):
    path = tmp_path / "observations.jsonl"
    first = append_delivery_observation(
        path,
        _row(case_key="g1q3-rca-s1-" + "c" * 64, remote_receipt_id="om_first"),
    )
    row = _row()
    canonical = _canonical_row_bytes(row)
    prefix_end = canonical.index(b'"remote_receipt_id"')
    torn_tail = canonical[:prefix_end]
    with path.open("ab") as stream:
        stream.write(torn_tail)

    observation = append_delivery_observation(path, row)

    assert path.read_bytes() == (
        _canonical_row_bytes(first) + b"\n" + canonical + b"\n"
    )
    assert read_delivery_observation_receipt(path).payload_sha256_by_id == {
        first["observation_id"]: delivery_observation_payload_sha256(first),
        observation["observation_id"]: delivery_observation_payload_sha256(
            observation
        )
    }


def test_append_rejects_arbitrary_torn_tail_without_truncating(tmp_path):
    path = tmp_path / "observations.jsonl"
    other = _row(
        case_key="g1q3-rca-s1-" + "c" * 64,
        remote_receipt_id="om_other",
    )
    corruption = _canonical_row_bytes(other)[:-17]
    path.write_bytes(corruption)
    path.chmod(0o600)

    with pytest.raises(DeliveryObservationError) as raised:
        append_delivery_observation(path, _row())

    assert raised.value.code == "observation_receipt_line_invalid"
    assert path.read_bytes() == corruption


def test_short_single_write_fails_then_exact_tail_is_recoverable(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "observations.jsonl"
    row = _row()
    original_write = observability_module.os.write
    write_calls = 0

    def short_write(descriptor, payload):
        nonlocal write_calls
        write_calls += 1
        return original_write(descriptor, payload[:-1])

    monkeypatch.setattr(observability_module.os, "write", short_write)
    with pytest.raises(DeliveryObservationError) as raised:
        append_delivery_observation(path, row)

    assert raised.value.code == "observation_append_short_write"
    assert write_calls == 1

    monkeypatch.setattr(observability_module.os, "write", original_write)
    observation = append_delivery_observation(path, row)
    assert path.read_bytes() == _canonical_row_bytes(observation) + b"\n"


def test_observation_file_lock_serializes_flushers(tmp_path):
    path = tmp_path / "observations.jsonl"
    first_acquired = threading.Event()
    release_first = threading.Event()
    second_acquired = threading.Event()

    def hold_first():
        with delivery_observation_file_lock(path):
            first_acquired.set()
            assert release_first.wait(timeout=2)

    def wait_second():
        assert first_acquired.wait(timeout=2)
        with delivery_observation_file_lock(path):
            second_acquired.set()

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(hold_first)
        second = pool.submit(wait_second)
        assert first_acquired.wait(timeout=2)
        assert second_acquired.wait(timeout=0.05) is False
        release_first.set()
        first.result(timeout=2)
        second.result(timeout=2)

    assert second_acquired.is_set()


def test_missing_level_is_rejected():
    value = _row()
    value.pop("level")
    with pytest.raises(DeliveryObservationError) as raised:
        build_delivery_observation(value)
    assert raised.value.code == "observation_required_field_missing"


@pytest.mark.parametrize("channel_count", (None, 0))
def test_attribution_does_not_require_a_channel_message_count(channel_count):
    observation = build_delivery_observation(
        _row(
            level="L1_observation",
            has_attribution=True,
            evidence_channel_msg_count=channel_count,
            evidence_refs_nonempty=True,
            evaluator_hit_count=1,
        )
    )

    assert observation["has_attribution"] is True


def test_attribution_requires_a_positive_evaluator_hit_count():
    with pytest.raises(DeliveryObservationError) as raised:
        build_delivery_observation(
            _row(
                level="L1_observation",
                has_attribution=True,
                evidence_channel_msg_count=None,
                evidence_refs_nonempty=True,
                evaluator_hit_count=0,
            )
        )
    assert raised.value.code == "observation_attribution_without_evaluator_hit"

    observation = build_delivery_observation(
        _row(
            level="L1_observation",
            has_attribution=True,
            evidence_channel_msg_count=None,
            evidence_refs_nonempty=True,
            evaluator_hit_count=1,
        )
    )
    assert observation["evaluator_hit_count"] == 1


@pytest.mark.parametrize(
    ("overrides", "expected_code"),
    (
        (
            {
                "evidence_channel_msg_count": None,
                "evidence_refs_nonempty": None,
                "evaluator_hit_count": 1,
            },
            "observation_attribution_without_evidence_refs",
        ),
        (
            {
                "evidence_channel_msg_count": 1,
                "evidence_refs_nonempty": False,
                "evaluator_hit_count": 1,
            },
            "observation_attribution_without_evidence_refs",
        ),
        (
            {
                "evidence_channel_msg_count": 1,
                "evidence_refs_nonempty": True,
                "evaluator_hit_count": 0,
            },
            "observation_attribution_without_evaluator_hit",
        ),
    ),
)
def test_attribution_requires_nonempty_refs_and_a_positive_hit(
    overrides,
    expected_code,
):
    with pytest.raises(DeliveryObservationError) as raised:
        build_delivery_observation(
            _row(
                level="L1_observation",
                has_attribution=True,
                **overrides,
            )
        )

    assert raised.value.code == expected_code


def test_observation_id_is_required():
    value = _row()
    value.pop("observation_id")
    with pytest.raises(DeliveryObservationError) as raised:
        build_delivery_observation(value)
    assert raised.value.code == "observation_required_field_missing"
    assert raised.value.detail == "observation_id"


def test_unknown_fields_are_rejected():
    with pytest.raises(DeliveryObservationError) as raised:
        build_delivery_observation(_row(unsealed_payload="not allowed"))
    assert raised.value.code == "observation_unexpected_field"
    assert raised.value.detail == "unsealed_payload"


@pytest.mark.parametrize(
    "field",
    ("observation_id", "inventory_pin", "outcome_content_sha256"),
)
@pytest.mark.parametrize("invalid", ("a" * 63, "A" * 64, "g" * 64))
def test_sha256_identity_fields_require_lowercase_64_hex(field, invalid):
    with pytest.raises(DeliveryObservationError) as raised:
        build_delivery_observation(_row(**{field: invalid}))
    assert raised.value.code == "observation_sha256_invalid"
    assert raised.value.detail == field


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


def test_dispatcher_launch_agent_binds_release_and_inventory():
    repo_root = Path(__file__).resolve().parents[2]
    with (repo_root / "local.pnc.rca-delivery-dispatcher.plist").open("rb") as handle:
        launch_agent = plistlib.load(handle)

    env = launch_agent["EnvironmentVariables"]
    assert env["HERMES_RCA_DELIVERY_DISPATCHER_OBSERVABILITY_ENABLED"] == "true"
    assert env["HERMES_RCA_DELIVERY_DISPATCHER_OBSERVATION_RELEASE_ID"] == (
        "rca-goal-v6-gate-a-20260731-r11"
    )
    assert env["HERMES_RCA_DELIVERY_DISPATCHER_INVENTORY_PIN"] == (
        "9fea0306752d005f58937e08202c9ce094e52056794549201259f214fe885880"
    )
    assert env["HERMES_RCA_DELIVERY_DISPATCHER_OBSERVABILITY_PATH"] == (
        "/Users/songying/.hermes/runtime/pnc_agent/feishu_issue_kafka_rca/"
        "delivery_observations.rca-goal-v6-gate-a-20260731-r11.jsonl"
    )
