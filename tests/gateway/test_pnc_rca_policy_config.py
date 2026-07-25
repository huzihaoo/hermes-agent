import hashlib
import json

import pytest

from gateway.pnc_rca_kafka_contract import WorkflowEventPolicy
from gateway.pnc_rca_policy_config import (
    W3_SNAPSHOT_AUTHORITY_SCHEMA_VERSION,
    W3_SNAPSHOT_POLICY_NAMES,
    w3_snapshot_authority_from_env,
    w3_snapshot_read_config_from_env,
)
from gateway.pnc_rca_snapshot import canonical_json_sha256


def _policy(*, version="issue-created-v1"):
    return WorkflowEventPolicy(
        topic="workflow",
        policy_version=version,
        project_keys=frozenset({"project-key"}),
        project_simple_names=frozenset({"g1q3"}),
        work_item_type_keys=frozenset({"issue"}),
        status_change_types=frozenset(),
        transitions=(),
        snapshot_patterns=frozenset({"State"}),
        snapshot_sub_stages=frozenset({"OPEN"}),
    )


def _workflow_policy_sha256(policy):
    return hashlib.sha256(
        json.dumps(
            policy.to_dict(),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _authority(policy):
    values = {
        "creation_policy": {
            "rule_version": policy.policy_version,
            "workflow_event_policy_sha256": _workflow_policy_sha256(policy),
        },
        "business_profile": {
            "registry_version": "rca_business_profiles_v1",
            "status": "matched",
            "profile_id": "g1q3",
            "execution_readiness": "ready",
            "resource_class": "rca_prod",
            "artifact_kind": "rca_html_report_and_viz_mcap",
            "artifact_namespace": "rca/g1q3",
        },
        "execution_policy": {
            "request_schema": "g1q3_rca_execution_request_v2",
            "data_access_mode": "remote_read",
            "allow_download": False,
            "input_materialization": "forbidden",
            "derived_artifacts_allowed": True,
            "allow_feishu_writeback": False,
            "group_response_cap": "L1",
            "translate_baseline": "production",
            "translate_contract_path": "",
        },
        "publication_policy": {"delivery_contract": "g1q3_delivery_contract_v1"},
        "correction_lineage_policy": {"generation_rule": "explicit_user_rerun"},
    }
    policies = {}
    for name in W3_SNAPSHOT_POLICY_NAMES:
        version = f"{name}-v1"
        value = values[name]
        policies[name] = {
            "version": version,
            "sha256": canonical_json_sha256({"version": version, "value": value}),
            "value": value,
        }
    body = {
        "schema_version": W3_SNAPSHOT_AUTHORITY_SCHEMA_VERSION,
        "policies": policies,
    }
    return body, canonical_json_sha256(body)


def _env(policy):
    body, digest = _authority(policy)
    return {
        "HERMES_RCA_W3_SNAPSHOT_SHADOW_ENABLED": "true",
        "HERMES_RCA_W3_SNAPSHOT_AUTHORITY_JSON": json.dumps(body),
        "HERMES_RCA_W3_SNAPSHOT_AUTHORITY_SHA256": digest,
    }


def _read_env(policy):
    return {
        **_env(policy),
        "HERMES_RCA_W3_SNAPSHOT_READ_MODE": "snapshot_required",
        "HERMES_RCA_KAFKA_TOPIC": policy.topic,
        "HERMES_RCA_KAFKA_CREATION_RULE_VERSION": policy.policy_version,
        "HERMES_RCA_KAFKA_PROJECT_KEYS": ",".join(policy.project_keys),
        "HERMES_RCA_KAFKA_PROJECT_SIMPLE_NAMES": ",".join(
            policy.project_simple_names
        ),
        "HERMES_RCA_KAFKA_WORK_ITEM_TYPE_KEYS": ",".join(
            policy.work_item_type_keys
        ),
        "HERMES_RCA_KAFKA_STATE_TRANSITIONS_JSON": "[]",
        "HERMES_RCA_KAFKA_SNAPSHOT_PATTERNS": ",".join(
            policy.snapshot_patterns
        ),
        "HERMES_RCA_KAFKA_SNAPSHOT_SUB_STAGES": ",".join(
            policy.snapshot_sub_stages
        ),
    }


def test_w3_snapshot_authority_is_disabled_by_default():
    assert w3_snapshot_authority_from_env({}, active_policy=_policy()) is None


def test_w3_snapshot_authority_loads_exact_pinned_policies():
    policy = _policy()
    authority = w3_snapshot_authority_from_env(_env(policy), active_policy=policy)

    assert authority is not None
    assert authority.to_public_dict()["enabled"] is True
    assert set(authority.policy_sha256s) == set(W3_SNAPSHOT_POLICY_NAMES)
    with pytest.raises(TypeError):
        authority.policies["execution_policy"]["value"]["request_schema"] = "changed"


def test_w3_snapshot_authority_rejects_outer_digest_drift():
    policy = _policy()
    env = _env(policy)
    env["HERMES_RCA_W3_SNAPSHOT_AUTHORITY_SHA256"] = "f" * 64

    with pytest.raises(ValueError, match="pinned SHA-256"):
        w3_snapshot_authority_from_env(env, active_policy=policy)


def test_w3_snapshot_authority_rejects_active_creation_policy_drift():
    configured = _policy()
    env = _env(configured)

    with pytest.raises(ValueError, match="does not match the active workflow policy"):
        w3_snapshot_authority_from_env(
            env,
            active_policy=_policy(version="issue-created-v2"),
        )


@pytest.mark.parametrize(
    "policy_name, mutate",
    [
        (
            "business_profile",
            lambda value: value.__setitem__("execution_readiness", "pending"),
        ),
        (
            "execution_policy",
            lambda value: value.pop("input_materialization"),
        ),
    ],
)
def test_w3_snapshot_authority_rejects_non_executable_policy_projection(
    policy_name, mutate
):
    policy = _policy()
    body, _digest = _authority(policy)
    envelope = body["policies"][policy_name]
    mutate(envelope["value"])
    envelope["sha256"] = canonical_json_sha256(
        {"version": envelope["version"], "value": envelope["value"]}
    )
    outer_digest = canonical_json_sha256(body)
    env = {
        "HERMES_RCA_W3_SNAPSHOT_SHADOW_ENABLED": "true",
        "HERMES_RCA_W3_SNAPSHOT_AUTHORITY_JSON": json.dumps(body),
        "HERMES_RCA_W3_SNAPSHOT_AUTHORITY_SHA256": outer_digest,
    }

    with pytest.raises(ValueError, match="invalid policy projection"):
        w3_snapshot_authority_from_env(env, active_policy=policy)


@pytest.mark.parametrize("value", ["1", "yes", "enabled"])
def test_w3_snapshot_authority_enable_flag_is_exact(value):
    with pytest.raises(ValueError, match="must be exactly true or false"):
        w3_snapshot_authority_from_env(
            {"HERMES_RCA_W3_SNAPSHOT_SHADOW_ENABLED": value},
            active_policy=_policy(),
        )


def test_w3_snapshot_read_mode_defaults_to_legacy_without_loading_authority():
    assert w3_snapshot_read_config_from_env({}) == ("legacy", None)


def test_w3_snapshot_read_mode_requires_exact_value():
    with pytest.raises(ValueError, match="legacy or snapshot_required"):
        w3_snapshot_read_config_from_env(
            {"HERMES_RCA_W3_SNAPSHOT_READ_MODE": "shadow"}
        )


def test_w3_snapshot_required_loads_the_approved_authority_root():
    policy = _policy()
    mode, authority = w3_snapshot_read_config_from_env(_read_env(policy))

    assert mode == "snapshot_required"
    assert authority is not None
    assert authority.authority_sha256 == _authority(policy)[1]
