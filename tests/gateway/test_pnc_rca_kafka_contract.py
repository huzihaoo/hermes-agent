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


def _snapshot_policy(**updates):
    values = {
        "topic": TOPIC,
        "policy_version": "issue-state-open-v1",
        "project_keys": frozenset({"project-key"}),
        "project_simple_names": frozenset({"g1q3"}),
        "work_item_type_keys": frozenset({"problem-type"}),
        "snapshot_patterns": frozenset({"State"}),
        "snapshot_sub_stages": frozenset({"OPEN"}),
    }
    values.update(updates)
    return WorkflowEventPolicy(**values)


def _snapshot_payload(**updates):
    value = {
        "created_at": 1783650001000,
        "fields": [],
        "id": 7041712812,
        "name": "ACC braking issue",
        "pattern": "State",
        "project_key": "project-key",
        "project_simple_name": "g1q3",
        "sub_stage": "OPEN",
        "updated_at": 1783650000000,
        "work_item_status": {"state_key": "open"},
        "work_item_type_key": "problem-type",
    }
    value.update(updates)
    return value


def test_exact_snapshot_policy_accepts_real_creation_envelope():
    result = classify_workflow_event(
        topic=TOPIC,
        value=_snapshot_payload(),
        policy=_snapshot_policy(),
    )

    assert result.decision == "accepted"
    assert result.reason == "creation_snapshot_policy_matched"
    assert result.normalized is not None
    assert result.normalized.work_item_id == "7041712812"
    assert result.normalized.status_change_type == "State"
    assert result.normalized.nodes == ()
    assert result.normalized.matched_nodes == ()
    assert result.normalized.business_profile_observed is True


def test_snapshot_carries_stable_business_profile_option_ids():
    policy = _snapshot_policy(
        project_keys=frozenset({"t03o4q"}),
        project_simple_names=frozenset({"t03o4q"}),
        work_item_type_keys=frozenset({"issue"}),
    )
    result = classify_workflow_event(
        topic=TOPIC,
        value=_snapshot_payload(
            project_key="t03o4q",
            project_simple_name="t03o4q",
            work_item_type_key="issue",
            fields=[
                {
                    "field_key": "field_052f23",
                    "field_value": ["7019637554"],
                }
            ],
        ),
        policy=policy,
    )

    assert result.decision == "accepted"
    assert result.normalized is not None
    assert result.normalized.business_profile_resolution["status"] == "matched"
    assert result.normalized.business_profile_resolution["profile_id"] == "mdrive4"
    assert result.normalized.business_profile_resolution["project_option_ids"] == [
        "7019637554"
    ]


def _canonical_g1q3_snapshot_policy(**updates):
    values = {
        "topic": TOPIC,
        "policy_version": "feishu-state-open-issue-v1",
        "project_keys": frozenset({"68ef617fb371dc80a10641f7"}),
        "project_simple_names": frozenset({"t03o4q"}),
        "work_item_type_keys": frozenset({"issue"}),
        "snapshot_patterns": frozenset({"State"}),
        "snapshot_sub_stages": frozenset({"OPEN"}),
        "allowed_project_option_ids": frozenset({"6670325063"}),
    }
    values.update(updates)
    return WorkflowEventPolicy(**values)


def _canonical_g1q3_snapshot_payload(**updates):
    value = _snapshot_payload(
        project_key="68ef617fb371dc80a10641f7",
        project_simple_name="t03o4q",
        work_item_type_key="issue",
        fields=[
            {
                "field_key": "field_052f23",
                "field_value": ["6670325063"],
            }
        ],
    )
    value.update(updates)
    return value


def test_canonical_g1q3_snapshot_accepts_only_exact_project_option():
    result = classify_workflow_event(
        topic=TOPIC,
        value=_canonical_g1q3_snapshot_payload(),
        policy=_canonical_g1q3_snapshot_policy(),
    )

    assert result.decision == "accepted"
    assert result.normalized is not None
    assert result.normalized.business_profile_resolution["profile_id"] == "g1q3"


def test_canonical_g1q3_snapshot_filters_mdrive4_option():
    payload = _canonical_g1q3_snapshot_payload(
        fields=[
            {
                "field_key": "field_052f23",
                "field_value": ["7019637554"],
            }
        ]
    )

    result = classify_workflow_event(
        topic=TOPIC,
        value=payload,
        policy=_canonical_g1q3_snapshot_policy(),
    )

    assert result.decision == "filtered"
    assert result.reason == "business_profile_project_option_not_allowed"


def test_canonical_g1q3_policy_without_allowlist_fails_closed():
    result = classify_workflow_event(
        topic=TOPIC,
        value=_canonical_g1q3_snapshot_payload(),
        policy=_canonical_g1q3_snapshot_policy(allowed_project_option_ids=frozenset()),
    )

    assert result.decision == "filtered"
    assert result.reason == "g1q3_kafka_scope_not_configured"


def test_canonical_g1q3_transition_is_rejected_even_with_option_scope():
    policy = _policy(
        policy_version="feishu-state-open-issue-v1",
        project_keys=frozenset({"68ef617fb371dc80a10641f7"}),
        project_simple_names=frozenset({"t03o4q"}),
        work_item_type_keys=frozenset({"issue"}),
        allowed_project_option_ids=frozenset({"6670325063"}),
    )
    result = classify_workflow_event(
        topic=TOPIC,
        value=_payload(
            project_key="68ef617fb371dc80a10641f7",
            project_simple_name="t03o4q",
            work_item_type_key="issue",
        ),
        policy=policy,
    )

    assert result.decision == "filtered"
    assert result.reason == "g1q3_kafka_scope_not_configured"


@pytest.mark.parametrize(
    ("payload_update", "reason"),
    [
        ({"pattern": "Changed"}, "snapshot_pattern_not_allowed"),
        ({"sub_stage": "CLOSED"}, "snapshot_sub_stage_not_allowed"),
        ({"fields": {}}, "invalid_snapshot_fields"),
        ({"work_item_status": []}, "invalid_work_item_status"),
        ({"nodes": []}, "ambiguous_creation_snapshot"),
        ({"status_change_type": "Reached"}, "ambiguous_creation_snapshot"),
    ],
)
def test_snapshot_policy_is_exact_and_rejects_ambiguous_shapes(
    payload_update, reason
):
    result = classify_workflow_event(
        topic=TOPIC,
        value=_snapshot_payload(**payload_update),
        policy=_snapshot_policy(),
    )

    assert result.decision in {"filtered", "invalid"}
    assert result.reason == reason


def test_policy_requires_one_complete_creation_mode():
    with pytest.raises(ValueError, match="at least one exact"):
        _snapshot_policy(snapshot_patterns=frozenset(), snapshot_sub_stages=frozenset())
    with pytest.raises(ValueError, match="configured together"):
        _snapshot_policy(snapshot_sub_stages=frozenset())


def test_exact_policy_match_normalizes_observed_workflow_shape():
    result = classify_workflow_event(topic=TOPIC, value=_payload(), policy=_policy())

    assert result.decision == "accepted"
    assert result.reason == "creation_policy_matched"
    assert result.normalized is not None
    assert result.normalized.work_item_id == "7041712812"
    assert result.normalized.creation_rule_version == "issue-created-v1"
    assert result.normalized.issue_url.endswith("/g1q3/issue/detail/7041712812")
    assert result.normalized.matched_nodes[0].state_key == "new-problem-state"


def test_workflow_envelope_with_snapshot_fields_is_rejected_as_ambiguous():
    result = classify_workflow_event(
        topic=TOPIC,
        value=_payload(
            fields=[
                {
                    "field_key": "field_052f23",
                    "field_value": ["6841983153"],
                }
            ]
        ),
        policy=_policy(),
    )

    assert result.decision == "invalid"
    assert result.reason == "ambiguous_creation_snapshot"
    assert result.normalized is None


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
