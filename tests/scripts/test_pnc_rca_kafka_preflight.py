from __future__ import annotations

from datetime import datetime, timezone
import inspect
import json
import logging

import pytest
from kafka import KafkaAdminClient
from kafka.protocol.admin import DescribeGroupsRequest
from kafka.protocol.broker_version_data import BrokerVersionData

from scripts.pnc_rca_kafka_preflight import (
    BROKER_OBSERVATION_SCHEMA_VERSION,
    BROKER_METADATA_SCHEMA_VERSION,
    COLLECTOR_SCHEMA_VERSION,
    BrokerProbeConfig,
    collect_broker_metadata,
    load_environment,
    main,
)


TOPIC = "feishu-project-workflow-event"


def _env(**overrides: str) -> dict[str, str]:
    values = {
        "HERMES_RCA_KAFKA_BOOTSTRAP_SERVERS": "broker-1:9092,broker-2:9092",
        "HERMES_RCA_KAFKA_TOPIC": TOPIC,
        "HERMES_RCA_KAFKA_EXPECTED_CLUSTER_ID": "cluster-production-1",
        "HERMES_RCA_KAFKA_USER": "rca",
        "HERMES_RCA_KAFKA_PASSWORD": "not-for-output",
        "HERMES_RCA_KAFKA_GROUP": "rca_root_cause_analysis_agent",
        "HERMES_RCA_KAFKA_API_VERSION": "3.9.0",
        "HERMES_RCA_KAFKA_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
        "HERMES_RCA_KAFKA_SASL_MECHANISM": "PLAIN",
        "HERMES_RCA_KAFKA_REQUEST_TIMEOUT_MS": "120000",
        "HERMES_RCA_KAFKA_MIN_REPLICATION_FACTOR": "2",
    }
    values.update(overrides)
    return values


def test_pinned_admin_exact_topic_metadata_contract():
    source = inspect.getsource(KafkaAdminClient._get_cluster_metadata)
    assert "allow_auto_topic_creation=False" in source
    assert "include_topic_authorized_operations=True" in source
    assert "include_cluster_authorized_operations=True" in source


def test_pinned_admin_describe_groups_is_authorized_operations_only():
    public_source = inspect.getsource(KafkaAdminClient.describe_groups)
    async_source = inspect.getsource(KafkaAdminClient._async_describe_groups)
    request_source = inspect.getsource(KafkaAdminClient._describe_groups_request)

    assert "self._manager.run(self._async_describe_groups" in public_source
    assert "_find_coordinator_ids(group_ids)" in async_source
    assert "_describe_groups_request(coordinator_group_ids)" in async_source
    assert "DescribeGroupsRequest(" in request_source
    assert "include_authorized_operations=True" in request_source
    for forbidden_api in (
        "OffsetFetch",
        "OffsetCommit",
        "JoinGroup",
        "ListOffsets",
    ):
        assert forbidden_api not in public_source
        assert forbidden_api not in async_source
        assert forbidden_api not in request_source


def test_pinned_kafka_390_uses_describe_groups_v5():
    assert BrokerVersionData((3, 9, 0)).api_version(DescribeGroupsRequest) == 5


class _Admin:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.closed = False
        self.calls: list[str] = []
        self._manager = self._Manager(self)

    class _Manager:
        def __init__(self, owner):
            self.owner = owner

        def run(self, _method, topics):
            self.owner.calls.append(f"metadata:{','.join(topics)}")
            return self.owner.metadata_payload()

    async def _get_cluster_metadata(self, _topics):
        raise AssertionError("fake manager must intercept metadata request")

    def metadata_payload(self):
        return {
            "cluster_id": "cluster-production-1",
            "topics": [
                {
                    "error_code": 0,
                    "name": TOPIC,
                    "authorized_operations": ["DESCRIBE", "READ"],
                    "partitions": [
                        {
                            "error_code": 0,
                            "partition_index": 1,
                            "leader_id": 2,
                            "leader_epoch": 11,
                            "replica_nodes": [2, 3],
                            "isr_nodes": [2, 3],
                            "offline_replicas": [],
                        },
                        {
                            "error_code": 0,
                            "partition_index": 0,
                            "leader_id": 1,
                            "leader_epoch": 10,
                            "replica_nodes": [1, 2],
                            "isr_nodes": [1, 2],
                            "offline_replicas": [],
                        },
                    ],
                }
            ],
        }

    def group_payload(self):
        return {
            "rca_root_cause_analysis_agent": {
                "error": None,
                "group_id": "rca_root_cause_analysis_agent",
                "authorized_operations": ["DESCRIBE", "READ"],
                "members": [{"client_host": "must-not-enter-evidence"}],
            }
        }

    def describe_groups(
        self,
        group_ids,
        group_coordinator_id=None,
        include_authorized_operations=False,
    ):
        assert group_ids == ["rca_root_cause_analysis_agent"]
        assert group_coordinator_id is None
        assert include_authorized_operations is True
        self.calls.append("describe_groups:rca_root_cause_analysis_agent")
        return self.group_payload()

    def close(self):
        self.closed = True
        self.calls.append("close")


def test_config_requires_fixed_service_identity_and_safe_protocol():
    config = BrokerProbeConfig.from_env(_env())
    assert config.username == "rca"
    assert config.configured_group_id == "rca_root_cause_analysis_agent"
    assert config.expected_cluster_id == "cluster-production-1"
    assert config.api_version == (3, 9, 0)
    assert "group_id" not in config.admin_kwargs()
    assert set(config.admin_kwargs()).issubset(KafkaAdminClient.DEFAULT_CONFIG)
    assert "minimum_replication_factor" not in config.admin_kwargs()
    assert config.public_dict()["minimum_replication_factor"] == 2
    assert config.public_dict()["expected_cluster_id"] == "cluster-production-1"
    assert config.request_timeout_ms == 10_000
    assert config.admin_kwargs()["request_timeout_ms"] == 10_000
    assert config.public_dict()["group_request"] == {
        "api": "DescribeGroups",
        "group_id": "rca_root_cause_analysis_agent",
        "include_authorized_operations": True,
    }
    assert "not-for-output" not in repr(config)
    assert "not-for-output" not in json.dumps(config.public_dict())

    for invalid_principal in (
        "legacy-identity",
        "rca_",
        "rca_invalid principal",
        "rca_release_agent",
    ):
        with pytest.raises(ValueError, match="must be exactly rca"):
            BrokerProbeConfig.from_env(
                _env(HERMES_RCA_KAFKA_USER=invalid_principal)
            )
    with pytest.raises(ValueError, match="must be exactly"):
        BrokerProbeConfig.from_env(
            _env(HERMES_RCA_KAFKA_GROUP="legacy-identity")
        )
    with pytest.raises(ValueError, match="SASL_PLAINTEXT"):
        BrokerProbeConfig.from_env(_env(HERMES_RCA_KAFKA_SECURITY_PROTOCOL="PLAINTEXT"))
    with pytest.raises(ValueError, match="exactly PLAIN"):
        BrokerProbeConfig.from_env(
            _env(HERMES_RCA_KAFKA_SASL_MECHANISM="SCRAM-SHA-256")
        )
    missing_replication_policy = _env()
    missing_replication_policy.pop("HERMES_RCA_KAFKA_MIN_REPLICATION_FACTOR")
    with pytest.raises(ValueError, match="MIN_REPLICATION_FACTOR"):
        BrokerProbeConfig.from_env(missing_replication_policy)
    missing_cluster_identity = _env()
    missing_cluster_identity.pop("HERMES_RCA_KAFKA_EXPECTED_CLUSTER_ID")
    with pytest.raises(ValueError, match="EXPECTED_CLUSTER_ID"):
        BrokerProbeConfig.from_env(missing_cluster_identity)
    with pytest.raises(ValueError, match="must be one line"):
        BrokerProbeConfig.from_env(
            _env(HERMES_RCA_KAFKA_EXPECTED_CLUSTER_ID="cluster-a\ncluster-b")
        )


def test_observe_only_config_does_not_require_owner_cluster_policy():
    env = _env()
    env.pop("HERMES_RCA_KAFKA_EXPECTED_CLUSTER_ID")
    env.pop("HERMES_RCA_KAFKA_MIN_REPLICATION_FACTOR")

    config = BrokerProbeConfig.from_env(env, observe_only=True)

    assert config.expected_cluster_id is None
    assert config.minimum_replication_factor is None
    assert config.public_dict()["expected_cluster_id"] is None
    assert config.public_dict()["minimum_replication_factor"] is None


def test_collect_is_read_only_and_returns_release_gate_shape():
    admin = _Admin()

    def factory(**kwargs):
        admin.kwargs = kwargs
        return admin

    payload = collect_broker_metadata(
        BrokerProbeConfig.from_env(_env()),
        admin_factory=factory,
        now=datetime(2026, 7, 12, 3, 0, tzinfo=timezone.utc),
    )

    assert payload["schema_version"] == BROKER_METADATA_SCHEMA_VERSION
    assert payload["observed_at"] == "2026-07-12T03:00:00+00:00"
    assert payload["topic_authorized"] is True
    assert payload["topic_healthy"] is True
    assert payload["group_authorized"] is True
    assert payload["cluster_id"] == "cluster-production-1"
    assert payload["expected_cluster_id"] == "cluster-production-1"
    assert payload["topic"] == TOPIC
    assert payload["group_id"] == "rca_root_cause_analysis_agent"
    assert payload["partitions"] == [0, 1]
    assert payload["replication_factor"] == 2
    assert payload["partition_topology"] == [
        {
            "partition": 0,
            "leader_id": 1,
            "leader_epoch": 10,
            "replicas": [1, 2],
            "isr": [1, 2],
            "offline_replicas": [],
        },
        {
            "partition": 1,
            "leader_id": 2,
            "leader_epoch": 11,
            "replicas": [2, 3],
            "isr": [2, 3],
            "offline_replicas": [],
        },
    ]
    assert payload["topic_authorized_operations"] == ["DESCRIBE", "READ"]
    assert payload["group_authorized_operations"] == ["DESCRIBE", "READ"]
    assert payload["collector"]["schema_version"] == COLLECTOR_SCHEMA_VERSION
    assert "group_id" not in admin.kwargs
    assert admin.calls == [
        f"metadata:{TOPIC}",
        "describe_groups:rca_root_cause_analysis_agent",
        "close",
    ]
    assert admin.closed is True
    serialized = json.dumps(payload)
    assert "not-for-output" not in serialized
    assert "must-not-enter-evidence" not in serialized
    assert payload["collector"]["side_effect_contract"] == {
        "exact_topic_metadata": True,
        "group_coordinator_lookup": True,
        "describe_groups": True,
        "additional_authorization_reads": ["DescribeGroups"],
        "subscribe": False,
        "assign": False,
        "poll": False,
        "commit": False,
        "offset_fetch": False,
        "list_offsets": False,
        "consumer_group_join": False,
        "topic_auto_create": False,
    }


def test_observe_only_collects_owner_candidates_but_is_not_release_evidence():
    env = _env()
    env.pop("HERMES_RCA_KAFKA_EXPECTED_CLUSTER_ID")
    env.pop("HERMES_RCA_KAFKA_MIN_REPLICATION_FACTOR")
    admin = _Admin()

    payload = collect_broker_metadata(
        BrokerProbeConfig.from_env(env, observe_only=True),
        admin_factory=lambda **_kwargs: admin,
        now=datetime(2026, 7, 14, 9, 0, tzinfo=timezone.utc),
        observe_only=True,
    )

    assert payload["schema_version"] == BROKER_OBSERVATION_SCHEMA_VERSION
    assert payload["production_eligible"] is False
    assert payload["owner_approval_required"] == [
        "cluster_id",
        "minimum_replication_factor",
    ]
    assert payload["cluster_id"] == "cluster-production-1"
    assert payload["replication_factor"] == 2
    assert payload["expected_cluster_id"] is None
    assert payload["collector"]["mode"] == "observe_only"
    assert admin.calls == [
        f"metadata:{TOPIC}",
        "describe_groups:rca_root_cause_analysis_agent",
        "close",
    ]


def test_collect_suppresses_kafka_admin_config_debug_log(caplog):
    admin = _Admin()

    def factory(**kwargs):
        logging.getLogger("kafka.admin.client").debug("configs=%r", kwargs)
        return admin

    with caplog.at_level(logging.DEBUG, logger="kafka.admin.client"):
        collect_broker_metadata(
            BrokerProbeConfig.from_env(_env()),
            admin_factory=factory,
        )
    assert "not-for-output" not in caplog.text
    assert logging.getLogger("kafka.admin.client").disabled is False


def test_collect_requires_cluster_identity():
    admin = _Admin()
    payload = admin.metadata_payload()
    payload["cluster_id"] = ""
    admin.metadata_payload = lambda: payload
    with pytest.raises(RuntimeError, match="cluster_id_missing"):
        collect_broker_metadata(
            BrokerProbeConfig.from_env(_env()),
            admin_factory=lambda **_kwargs: admin,
        )
    assert admin.closed is True


def test_collect_rejects_unexpected_cluster_before_group_probe():
    admin = _Admin()
    with pytest.raises(RuntimeError, match="cluster_id_mismatch"):
        collect_broker_metadata(
            BrokerProbeConfig.from_env(
                _env(HERMES_RCA_KAFKA_EXPECTED_CLUSTER_ID="other-cluster")
            ),
            admin_factory=lambda **_kwargs: admin,
        )
    assert admin.calls == [f"metadata:{TOPIC}", "close"]
    assert admin.closed is True


def test_collect_closes_consumer_when_topic_is_missing():
    admin = _Admin()
    payload = admin.metadata_payload()
    payload["topics"] = []
    admin.metadata_payload = lambda: payload
    with pytest.raises(RuntimeError, match="missing_or_unauthorized"):
        collect_broker_metadata(
            BrokerProbeConfig.from_env(_env()),
            admin_factory=lambda **_kwargs: admin,
        )
    assert admin.closed is True


def test_collect_requires_read_and_describe_authorization():
    admin = _Admin()
    payload = admin.metadata_payload()
    payload["topics"][0]["authorized_operations"] = ["DESCRIBE"]
    admin.metadata_payload = lambda: payload
    with pytest.raises(RuntimeError, match="read_describe_not_authorized"):
        collect_broker_metadata(
            BrokerProbeConfig.from_env(_env()),
            admin_factory=lambda **_kwargs: admin,
        )
    assert admin.closed is True


def test_collect_requires_exact_authorized_group_and_closes_admin():
    admin = _Admin()
    admin.group_payload = lambda: {
        "other-group": {
            "error": None,
            "authorized_operations": ["DESCRIBE", "READ"],
        }
    }
    with pytest.raises(RuntimeError, match="group_identity_mismatch"):
        collect_broker_metadata(
            BrokerProbeConfig.from_env(_env()),
            admin_factory=lambda **_kwargs: admin,
        )
    assert admin.calls[-1] == "close"
    assert admin.closed is True


def test_collect_rejects_group_error_and_closes_admin():
    admin = _Admin()
    group_payload = admin.group_payload()
    group_payload["rca_root_cause_analysis_agent"]["error"] = (
        "GroupAuthorizationFailedError"
    )
    admin.group_payload = lambda: group_payload
    with pytest.raises(RuntimeError, match="group_missing_or_unauthorized"):
        collect_broker_metadata(
            BrokerProbeConfig.from_env(_env()),
            admin_factory=lambda **_kwargs: admin,
        )
    assert admin.calls[-1] == "close"
    assert admin.closed is True


@pytest.mark.parametrize(
    ("resource", "operation"),
    [
        ("topic", "WRITE"),
        ("topic", "CREATE"),
        ("topic", "ALTER"),
        ("topic", "ALTER_CONFIGS"),
        ("topic", "CLUSTER_ACTION"),
        ("topic", "IDEMPOTENT_WRITE"),
        ("group", "DELETE"),
        ("group", "ALL"),
        ("group", "ANY"),
        ("group", "CREATE_TOKENS"),
    ],
)
def test_collect_rejects_mutation_authorization(resource, operation):
    admin = _Admin()
    if resource == "topic":
        metadata_payload = admin.metadata_payload()
        metadata_payload["topics"][0]["authorized_operations"].append(operation)
        admin.metadata_payload = lambda: metadata_payload
    else:
        group_payload = admin.group_payload()
        group_payload["rca_root_cause_analysis_agent"]["authorized_operations"].append(
            operation
        )
        admin.group_payload = lambda: group_payload

    with pytest.raises(
        RuntimeError,
        match=rf"{resource}_mutation_operations_authorized",
    ):
        collect_broker_metadata(
            BrokerProbeConfig.from_env(_env()),
            admin_factory=lambda **_kwargs: admin,
        )
    assert admin.calls[-1] == "close"
    assert admin.closed is True


def test_collect_rejects_missing_or_unknown_group_operations():
    for operations, error in (
        (["DESCRIBE"], "group_read_describe_not_authorized"),
        (["DESCRIBE", "READ", "FUTURE_MUTATION"], "operations_unknown"),
    ):
        admin = _Admin()
        group_payload = admin.group_payload()
        group_payload["rca_root_cause_analysis_agent"]["authorized_operations"] = operations
        admin.group_payload = lambda: group_payload
        with pytest.raises(RuntimeError, match=error):
            collect_broker_metadata(
                BrokerProbeConfig.from_env(_env()),
                admin_factory=lambda **_kwargs: admin,
            )
        assert admin.calls[-1] == "close"
        assert admin.closed is True


@pytest.mark.parametrize(
    ("mutation", "error"),
    [
        (lambda item: item.update(leader_id=-1), "partitions_invalid"),
        (lambda item: item.update(isr_nodes=[]), "topology_unhealthy"),
        (
            lambda item: item.update(offline_replicas=[3]),
            "topology_unhealthy",
        ),
        (
            lambda item: item.update(isr_nodes=[4]),
            "topology_unhealthy",
        ),
        (
            lambda item: item.update(isr_nodes=[item["isr_nodes"][0]]),
            "topology_unhealthy",
        ),
    ],
)
def test_collect_rejects_unhealthy_partition_topology(mutation, error):
    admin = _Admin()
    payload = admin.metadata_payload()
    mutation(payload["topics"][0]["partitions"][0])
    admin.metadata_payload = lambda: payload
    with pytest.raises(RuntimeError, match=error):
        collect_broker_metadata(
            BrokerProbeConfig.from_env(_env()),
            admin_factory=lambda **_kwargs: admin,
        )
    assert admin.closed is True


def test_collect_enforces_contiguous_partitions_and_replication_policy():
    admin = _Admin()
    payload = admin.metadata_payload()
    payload["topics"][0]["partitions"][1]["partition_index"] = 2
    admin.metadata_payload = lambda: payload
    with pytest.raises(RuntimeError, match="partition_ids_not_contiguous"):
        collect_broker_metadata(
            BrokerProbeConfig.from_env(_env()),
            admin_factory=lambda **_kwargs: admin,
        )

    admin = _Admin()
    payload = admin.metadata_payload()
    for item in payload["topics"][0]["partitions"]:
        item["replica_nodes"] = [item["leader_id"]]
        item["isr_nodes"] = [item["leader_id"]]
    admin.metadata_payload = lambda: payload
    with pytest.raises(RuntimeError, match="replication_factor_below_policy"):
        collect_broker_metadata(
            BrokerProbeConfig.from_env(_env()),
            admin_factory=lambda **_kwargs: admin,
        )


def test_main_writes_atomic_redacted_evidence(tmp_path, capsys):
    admin = _Admin()
    output = tmp_path / "evidence" / "broker_metadata.json"
    result = main(
        ["--output", str(output)],
        admin_factory=lambda **_kwargs: admin,
        env=_env(),
        now=datetime(2026, 7, 12, 3, 0, tzinfo=timezone.utc),
    )
    assert result == 0
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["topic_authorized"] is True
    assert "not-for-output" not in output.read_text(encoding="utf-8")
    response = json.loads(capsys.readouterr().out)
    assert response["ok"] is True
    assert response["output"] == str(output)
    assert (output.stat().st_mode & 0o777) == 0o600


def test_main_observe_only_requires_output_and_writes_noneligible_receipt(
    tmp_path, capsys
):
    env = _env()
    env.pop("HERMES_RCA_KAFKA_EXPECTED_CLUSTER_ID")
    env.pop("HERMES_RCA_KAFKA_MIN_REPLICATION_FACTOR")
    output = tmp_path / "broker-observation.json"

    result = main(
        ["--observe-only", "--output", str(output)],
        admin_factory=lambda **_kwargs: _Admin(),
        env=env,
    )

    assert result == 0
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["schema_version"] == BROKER_OBSERVATION_SCHEMA_VERSION
    assert payload["production_eligible"] is False
    assert json.loads(capsys.readouterr().out)["ok"] is True

    assert main(["--observe-only"], env=env) == 2
    assert json.loads(capsys.readouterr().err)["ok"] is False


def test_main_redacts_password_from_failure_message(capsys):
    def factory(**kwargs):
        raise RuntimeError(f"connection rejected {kwargs['sasl_plain_password']}")

    result = main([], admin_factory=factory, env=_env())

    assert result == 2
    error = capsys.readouterr().err
    assert "not-for-output" not in error
    assert "[REDACTED]" in error


def test_main_fails_before_connecting_on_unsafe_output(tmp_path, capsys):
    target = tmp_path / "target.json"
    target.write_text("{}", encoding="utf-8")
    symlink = tmp_path / "broker_metadata.json"
    symlink.symlink_to(target)
    called = False

    def factory(**_kwargs):
        nonlocal called
        called = True
        return _Admin()

    result = main(
        ["--output", str(symlink)],
        admin_factory=factory,
        env=_env(),
    )
    assert result == 2
    assert called is False
    assert json.loads(capsys.readouterr().err)["ok"] is False
    assert target.read_text(encoding="utf-8") == "{}"


def test_env_file_is_authoritative_and_observed_without_password_hash(
    tmp_path, monkeypatch
):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(f"{key}={value}" for key, value in _env().items()) + "\n",
        encoding="utf-8",
    )
    env_file.chmod(0o600)
    monkeypatch.setenv("HERMES_RCA_KAFKA_TOPIC", "ambient-must-not-win")

    source, observation = load_environment(env_file)

    assert source["HERMES_RCA_KAFKA_TOPIC"] == TOPIC
    assert source["HERMES_RCA_KAFKA_PASSWORD"] == "not-for-output"
    assert observation["path"] == str(env_file.resolve())
    assert observation["mode"] == "0600"
    assert observation["password_set"] is True
    serialized = json.dumps(observation)
    assert "not-for-output" not in serialized
    assert "HERMES_RCA_KAFKA_PASSWORD" not in serialized


def test_env_file_rejects_group_readable_secret_file(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("HERMES_RCA_KAFKA_PASSWORD=secret\n", encoding="utf-8")
    env_file.chmod(0o640)
    with pytest.raises(ValueError, match="owner-only"):
        load_environment(env_file)


def test_env_file_rejects_symlink_and_oversized_file(tmp_path):
    target = tmp_path / "target.env"
    target.write_text("HERMES_RCA_KAFKA_PASSWORD=secret\n", encoding="utf-8")
    target.chmod(0o600)
    symlink = tmp_path / ".env"
    symlink.symlink_to(target)
    with pytest.raises(ValueError, match="must not be a symlink"):
        load_environment(symlink)

    oversized = tmp_path / "oversized.env"
    oversized.write_bytes(b"x" * (1024 * 1024 + 1))
    oversized.chmod(0o600)
    with pytest.raises(ValueError, match="exceeds size limit"):
        load_environment(oversized)
