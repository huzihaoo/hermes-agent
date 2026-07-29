import copy
import hashlib

import pytest

from gateway.pnc_rca_data_access import (
    RCA_REMOTE_DATA_ACCESS_SCHEMA_VERSION,
    RemoteDataAccessError,
    build_remote_data_access,
    redacted_data_access,
    validate_remote_data_access,
)


def test_event_address_becomes_non_executable_remote_reader_contract():
    source = "mdi download event -u event-123 -s ./"

    contract = build_remote_data_access(source)

    assert contract["schema_version"] == RCA_REMOTE_DATA_ACCESS_SCHEMA_VERSION
    assert contract["mode"] == "remote_read"
    assert contract["transport"] == "pdcl_pyclip"
    assert contract["references"] == [
        {
            "kind": "event",
            "event_uuid": "event-123",
            "reader_class": "RemoteEventReader",
        }
    ]
    assert contract["source"] == {
        "field": "问题数据地址_PDCL",
        "value_sha256": hashlib.sha256(source.encode()).hexdigest(),
    }
    assert contract["reader_contract"]["mdi_download_allowed"] is False
    assert contract["reader_contract"]["fallback"] == "forbidden"
    assert source not in repr(contract)


def test_clip_and_refresh_addresses_map_to_supported_reader_classes():
    clip = build_remote_data_access("mdi download clip -u clip-a,clip-b -s ./")
    refresh = build_remote_data_access(
        "mdi refresh -t ticket-a -e event-a,event-b -s ./"
    )

    assert [item["clip_uuid"] for item in clip["references"]] == [
        "clip-a",
        "clip-b",
    ]
    assert [item["reader_class"] for item in clip["references"]] == [
        "RemoteClipReader",
        "RemoteClipReader",
    ]
    assert [item["event_uuid"] for item in refresh["references"]] == [
        "event-a",
        "event-b",
    ]


@pytest.mark.parametrize(
    ("source", "code"),
    [
        ("", "issue_field_missing_remote_data_reference"),
        ("mdi refresh -t ticket-only -s ./", "remote_data_reference_resolution_required"),
        ("mdi download raw -r /safe/ref -s ./", "remote_data_reference_kind_unsupported"),
        ("cyber_recorder play -f demo.record", "remote_data_reference_invalid"),
        (
            "mdi download event -u " + "x" * 513 + " -s ./",
            "remote_data_reference_invalid",
        ),
        (
            "mdi download event -u " + "x" * (16 * 1024) + " -s ./",
            "remote_data_source_limit_exceeded",
        ),
    ],
)
def test_unsupported_or_unresolvable_references_fail_closed(source, code):
    with pytest.raises(RemoteDataAccessError) as caught:
        build_remote_data_access(source)

    assert caught.value.code == code


def test_health_summary_redacts_opaque_ids():
    contract = build_remote_data_access("mdi download event -u event-secret -s ./")

    summary = redacted_data_access(contract)

    assert summary["reference_count"] == 1
    assert summary["reference_kinds"] == ["event"]
    assert "event-secret" not in repr(summary)


def test_strict_validator_detaches_the_canonical_active_contract():
    contract = build_remote_data_access("mdi download event -u event-123 -s ./")

    validated = validate_remote_data_access(contract)

    assert validated == contract
    assert validated is not contract
    assert validated["references"] is not contract["references"]


@pytest.mark.parametrize(
    ("mutation", "code"),
    [
        (
            lambda value: value.update(extra=True),
            "remote_data_access_shape_invalid",
        ),
        (
            lambda value: value["source"].update(value_sha256="A" * 64),
            "remote_data_source_invalid",
        ),
        (
            lambda value: value["source"].update(extra=True),
            "remote_data_source_invalid",
        ),
        (
            lambda value: value["reader_contract"].pop("completeness"),
            "remote_reader_contract_invalid",
        ),
        (
            lambda value: value["references"].append(
                copy.deepcopy(value["references"][0])
            ),
            "remote_data_reference_duplicate",
        ),
        (
            lambda value: value["references"][0].update(extra=True),
            "remote_data_reference_invalid",
        ),
        (
            lambda value: value["references"][0].update(event_uuid=""),
            "remote_data_reference_invalid",
        ),
        (
            lambda value: value["references"][0].update(event_uuid=123),
            "remote_data_reference_invalid",
        ),
        (
            lambda value: value["references"][0].update(event_uuid=" event-123"),
            "remote_data_reference_invalid",
        ),
        (
            lambda value: value.update(
                references=[
                    {
                        "kind": "event",
                        "event_uuid": f"event-{index}",
                        "reader_class": "RemoteEventReader",
                    }
                    for index in range(17)
                ]
            ),
            "remote_data_reference_count_invalid",
        ),
        (
            lambda value: value.update(
                status="blocked",
                references=[],
                blocker={"kind": "remote_data_reference_missing"},
            ),
            "remote_data_reference_missing",
        ),
    ],
)
def test_strict_validator_rejects_contract_drift(mutation, code):
    contract = build_remote_data_access("mdi download event -u event-123 -s ./")
    mutation(contract)

    with pytest.raises(RemoteDataAccessError) as caught:
        validate_remote_data_access(contract)

    assert caught.value.code == code


@pytest.mark.parametrize("locator", ["default", "fallback-event", "0-0-0-0"])
def test_placeholder_locator_cannot_enter_remote_data_access(locator):
    with pytest.raises(RemoteDataAccessError) as caught:
        build_remote_data_access(f"mdi download event -u {locator} -s ./")

    assert caught.value.code == "remote_data_reference_invalid"


def test_handcrafted_placeholder_reference_fails_contract_revalidation():
    contract = build_remote_data_access("mdi download event -u event-123 -s ./")
    contract["references"][0]["event_uuid"] = "fallback:event"

    with pytest.raises(RemoteDataAccessError) as caught:
        validate_remote_data_access(contract)

    assert caught.value.code == "remote_data_reference_invalid"
