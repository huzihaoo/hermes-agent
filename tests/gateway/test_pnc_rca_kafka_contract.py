import json

import pytest

from gateway.pnc_rca_admission import build_rca_admission
from gateway.pnc_rca_kafka_contract import (
    MAX_WORKFLOW_EVENT_BYTES,
    WorkflowEventPolicy,
    WorkflowTransition,
    build_event_admission,
    classify_workflow_event,
)


TOPIC = "feishu-project-workflow-event"


def _policy(**updates):
    values = {
        "topic": TOPIC,
        "policy_version": "issue-created-v1",
        "project_keys": frozenset({"project-key"}),
        "project_simple_names": frozenset({"g1q3"}),
        "work_item_type_keys": frozenset({"problem-type"}),
        "status_change_types": frozenset({"Reached"}),
        "transitions": (
            WorkflowTransition(
                state_key="new-problem-state",
                pre_status=1,
                cur_status=2,
            ),
        ),
    }
    values.update(updates)
    return WorkflowEventPolicy(**values)


def _payload(**updates):
    value = {
        "id": 7041712812,
        "name": "ACC braking issue",
        "nodes": [
            {
                "state_key": "new-problem-state",
                "node_name": "New problem",
                "pre_status": 1,
                "cur_status": 2,
            }
        ],
        "project_key": "project-key",
        "project_simple_name": "g1q3",
        "status_change_type": "Reached",
        "updated_at": 1783650000000,
        "work_item_type_key": "problem-type",
    }
    value.update(updates)
    return value


def test_exact_policy_match_normalizes_observed_workflow_shape():
    result = classify_workflow_event(topic=TOPIC, value=_payload(), policy=_policy())

    assert result.decision == "accepted"
    assert result.reason == "creation_policy_matched"
    assert result.normalized is not None
    assert result.normalized.work_item_id == "7041712812"
    assert result.normalized.creation_rule_version == "issue-created-v1"
    assert result.normalized.issue_url.endswith("/g1q3/issue/detail/7041712812")
    assert result.normalized.matched_nodes[0].state_key == "new-problem-state"


def test_node_name_is_optional_diagnostic_not_creation_identity():
    renamed = _payload(
        nodes=[
            {
                "state_key": "new-problem-state",
                "node_name": "Localized display label changed",
                "pre_status": 1,
                "cur_status": 2,
            }
        ]
    )

    result = classify_workflow_event(topic=TOPIC, value=renamed, policy=_policy())

    assert result.decision == "accepted"
    assert (
        result.normalized.matched_nodes[0].node_name
        == "Localized display label changed"
    )


@pytest.mark.parametrize(
    ("topic", "payload_update", "reason"),
    [
        ("feishu-project-workfLow-event", {}, "topic_not_allowed"),
        (TOPIC, {"project_key": "other"}, "project_key_not_allowed"),
        (TOPIC, {"project_simple_name": "other"}, "project_simple_name_not_allowed"),
        (TOPIC, {"work_item_type_key": "other"}, "work_item_type_not_allowed"),
        (TOPIC, {"status_change_type": "Checked"}, "status_change_type_not_allowed"),
        (
            TOPIC,
            {
                "nodes": [
                    {
                        "state_key": "new-problem-state",
                        "pre_status": 2,
                        "cur_status": 3,
                    }
                ]
            },
            "state_transition_not_allowed",
        ),
    ],
)
def test_every_creation_dimension_is_an_exact_configured_match(
    topic, payload_update, reason
):
    result = classify_workflow_event(
        topic=topic,
        value=_payload(**payload_update),
        policy=_policy(),
    )

    assert result.decision == "filtered"
    assert result.reason == reason


def test_status_value_types_are_not_coerced_during_transition_match():
    result = classify_workflow_event(
        topic=TOPIC,
        value=_payload(
            nodes=[
                {
                    "state_key": "new-problem-state",
                    "pre_status": "1",
                    "cur_status": "2",
                }
            ]
        ),
        policy=_policy(),
    )

    assert result.decision == "filtered"
    assert result.reason == "state_transition_not_allowed"


def test_heterogeneous_ci_message_is_filtered_not_dead_lettered():
    result = classify_workflow_event(
        topic=TOPIC,
        value={
            "app_md5": "abc",
            "branch": "main",
            "project": "app",
            "status": "success",
        },
        policy=_policy(),
    )

    assert result.decision == "filtered"
    assert result.reason == "unsupported_message_shape"


def test_partial_workflow_message_is_invalid():
    result = classify_workflow_event(
        topic=TOPIC,
        value={"project_key": "project-key", "nodes": []},
        policy=_policy(),
    )

    assert result.decision == "invalid"
    assert result.reason == "missing_workflow_fields"


@pytest.mark.parametrize(
    ("value", "reason"),
    [
        (b"not-json", "invalid_json"),
        (b"\xff", "invalid_utf8"),
        (json.dumps([1, 2]), "invalid_json_object"),
    ],
)
def test_invalid_kafka_values_fail_closed(value, reason):
    result = classify_workflow_event(topic=TOPIC, value=value, policy=_policy())
    assert result.decision == "invalid"
    assert result.reason == reason


@pytest.mark.parametrize("updated_at", [1.5, float("inf"), "001", 2**63])
def test_updated_at_requires_a_bounded_exact_integer(updated_at):
    result = classify_workflow_event(
        topic=TOPIC,
        value=_payload(updated_at=updated_at),
        policy=_policy(),
    )

    assert result.decision == "invalid"
    assert result.reason == "invalid_updated_at"


def test_overflowing_json_exponent_is_a_deterministic_invalid_event():
    raw = json.dumps(_payload()).replace("1783650000000", "1e309").encode()

    result = classify_workflow_event(topic=TOPIC, value=raw, policy=_policy())

    assert result.decision == "invalid"
    assert result.reason == "invalid_updated_at"


@pytest.mark.parametrize(
    ("raw", "reason"),
    [
        (b'{"id":1,"id":2}', "duplicate_json_key"),
        (b'{"value":NaN}', "invalid_json_constant"),
        (
            b'{"value":' + b"[" * 1100 + b"0" + b"]" * 1100 + b"}",
            "json_nesting_too_deep",
        ),
        (b"{" + b" " * MAX_WORKFLOW_EVENT_BYTES + b"}", "event_too_large"),
    ],
)
def test_json_resource_and_ambiguity_limits_are_deterministic(raw, reason):
    result = classify_workflow_event(topic=TOPIC, value=raw, policy=_policy())

    assert result.decision == "invalid"
    assert result.reason == reason


@pytest.mark.parametrize(
    "updates",
    [
        {"name": "x" * 2_001},
        {"project_key": "x" * 257},
        {"nodes": _payload()["nodes"] * 101},
    ],
)
def test_workflow_field_cardinality_and_length_limits(updates):
    result = classify_workflow_event(
        topic=TOPIC,
        value=_payload(**updates),
        policy=_policy(),
    )

    assert result.decision == "invalid"


def test_canonical_admission_excludes_transport_and_updated_at_from_keys():
    first = classify_workflow_event(
        topic=TOPIC, value=_payload(), policy=_policy()
    ).normalized
    later = classify_workflow_event(
        topic=TOPIC,
        value=_payload(updated_at=1783659999999),
        policy=_policy(),
    ).normalized
    first_admission = build_event_admission(first, topic=TOPIC, partition=0, offset=10)
    replay_admission = build_event_admission(later, topic=TOPIC, partition=4, offset=99)
    canonical = build_rca_admission(
        project_key="project-key",
        work_item_type_key="problem-type",
        work_item_id="7041712812",
        rule_version="issue-created-v1",
        topic=TOPIC,
        partition=0,
        offset=10,
    )

    assert first_admission.business_key == replay_admission.business_key
    assert first_admission.submission_key == replay_admission.submission_key
    assert first_admission.business_key == canonical.business_key
    assert first_admission.submission_key == canonical.submission_key


def test_policy_fails_closed_on_implicit_or_multi_topic_configuration():
    with pytest.raises(ValueError, match="project_keys"):
        _policy(project_keys=frozenset())
    with pytest.raises(ValueError, match="one exact Kafka topic"):
        _policy(topic=f"{TOPIC},other")
    with pytest.raises(ValueError, match="transitions"):
        _policy(transitions=())
