import pytest

from gateway.pnc_rca_frame_reference import (
    FrameReferenceError,
    normalize_management_timestamp_us,
    parse_frame_reference,
    resolve_front_camera_frame,
)


def test_parse_frame_reference_keeps_positive_numeric_frame_id():
    assert parse_frame_reference("00318153") == {
        "kind": "frame_id",
        "frame_id": "318153",
        "source_field": "问题发生frame_id",
    }


@pytest.mark.parametrize(
    ("value", "expected_timestamp"),
    [
        ("2026-07-12 15:31:16", 1_783_841_476_000_000),
        ("20260708, 20:05:00", 1_783_512_300_000_000),
    ],
)
def test_parse_frame_reference_converts_exact_local_time_to_management_timestamp(
    value, expected_timestamp
):
    result = parse_frame_reference(value)

    assert result["kind"] == "front_camera_timestamp"
    assert result["timezone"] == "Asia/Shanghai"
    assert result["management_timestamp"] == expected_timestamp
    assert result["management_timestamp_unit"] == "microseconds_since_unix_epoch"
    assert result["selection"] == "nearest_timestamp"
    assert result["max_delta_us"] == 100_000


@pytest.mark.parametrize(
    "value",
    ["0", "-1", "2026/07/12 15:31:16", "20260708 20:05:00", "20260230, 20:05:00"],
)
def test_parse_frame_reference_rejects_ambiguous_or_invalid_values(value):
    with pytest.raises(FrameReferenceError):
        parse_frame_reference(value)


def test_management_timestamp_normalization_supports_explicit_epoch_scales():
    expected = 1_769_759_070_668_000
    assert normalize_management_timestamp_us(1_769_759_070) == 1_769_759_070_000_000
    assert normalize_management_timestamp_us(1_769_759_070_668) == expected
    assert normalize_management_timestamp_us("1769759070668000") == expected
    assert normalize_management_timestamp_us(1_769_759_070_668_000_000) == expected


def test_resolve_front_camera_frame_prefers_camera1_and_nearest_earlier_frame():
    lookup = parse_frame_reference("2026-01-30 15:44:30")
    lookup["management_timestamp"] = 1_769_759_070_702_000
    payloads = {
        "d4q.1.camera4.index.json": {
            "fields": {"timestamp": 0, "frame_id": 3},
            "index": [["1769759070701000", 0, 0, 145556]],
        },
        "d4q.1.camera1.index.json": {
            "fields": {"timestamp": 0, "frame_id": 3},
            "index": [
                ["1769759070668000", 0, 0, 48516],
                ["1769759070702000", 0, 0, 48517],
                ["1769759070736000", 0, 0, 48518],
            ],
        },
    }

    result = resolve_front_camera_frame(
        frame_lookup=lookup,
        index_payloads=payloads,
    )

    assert result["frame_id"] == "48517"
    assert result["topic"] == "d4q.1.camera1.index.json"
    assert result["delta_us"] == 0
    assert result["matched_management_timestamp"] == 1_769_759_070_702_000


def test_resolve_front_camera_frame_fails_closed_outside_tolerance():
    lookup = parse_frame_reference("2026-01-30 15:44:30")
    lookup["management_timestamp"] = 1_769_759_071_000_000
    payloads = {
        "d4q.1.camera1.index.json": {
            "fields": {"timestamp": 0, "frame_id": 3},
            "index": [["1769759070702000", 0, 0, 48517]],
        }
    }

    with pytest.raises(
        FrameReferenceError, match="front_camera_frame_outside_tolerance"
    ):
        resolve_front_camera_frame(frame_lookup=lookup, index_payloads=payloads)
