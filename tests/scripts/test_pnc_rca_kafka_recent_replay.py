from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from types import SimpleNamespace

from scripts import pnc_rca_kafka_consumer
from scripts import pnc_rca_kafka_recent_replay as replay


TOPIC = "feishu-project-workflow-event"
NOW = datetime(2026, 7, 14, 8, 0, tzinfo=timezone.utc)


def _config(tmp_path: Path):
    return pnc_rca_kafka_consumer.ConsumerConfig.from_env(
        {
            "HERMES_RCA_KAFKA_BOOTSTRAP_SERVERS": "broker-1:9092",
            "HERMES_RCA_KAFKA_TOPIC": TOPIC,
            "HERMES_RCA_KAFKA_USER": "root_cause_analysis_agent",
            "HERMES_RCA_KAFKA_PASSWORD": "test-secret",
            "HERMES_RCA_KAFKA_GROUP": "root_cause_analysis_agent",
            "HERMES_RCA_KAFKA_API_VERSION": "3.9.0",
            "HERMES_RCA_KAFKA_PROJECT_KEYS": "project-key",
            "HERMES_RCA_KAFKA_PROJECT_SIMPLE_NAMES": "g1q3",
            "HERMES_RCA_KAFKA_WORK_ITEM_TYPE_KEYS": "problem-type",
            "HERMES_RCA_KAFKA_STATUS_CHANGE_TYPES": "Reached",
            "HERMES_RCA_KAFKA_CREATION_RULE_VERSION": "issue-created-v1",
            "HERMES_RCA_KAFKA_STATE_TRANSITIONS_JSON": json.dumps([
                {
                    "state_key": "new-problem-state",
                    "pre_status": 1,
                    "cur_status": 2,
                }
            ]),
            "HERMES_RCA_KAFKA_CONTROL_DB_PATH": str(tmp_path / "unused.sqlite3"),
            "HERMES_RCA_KAFKA_HEALTH_PATH": str(tmp_path / "unused-health.json"),
        },
        hermes_home=tmp_path,
    )


def _value(work_item_id: int) -> bytes:
    return json.dumps({
        "id": work_item_id,
        "name": f"private title {work_item_id}",
        "nodes": [
            {
                "state_key": "new-problem-state",
                "pre_status": 1,
                "cur_status": 2,
            }
        ],
        "project_key": "project-key",
        "project_simple_name": "g1q3",
        "status_change_type": "Reached",
        "updated_at": int(NOW.timestamp() * 1000),
        "work_item_type_key": "problem-type",
    }).encode()


@dataclass(frozen=True)
class _TopicPartition:
    topic: str
    partition: int


class _FakeConsumer:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.assigned = None
        self.seeked = {}
        self.closed = None
        self.poll_count = 0

    def partitions_for_topic(self, topic):
        assert topic == TOPIC
        return {0, 1}

    def beginning_offsets(self, partitions):
        return {partition: 0 for partition in partitions}

    def end_offsets(self, partitions):
        return {partition: 2 for partition in partitions}

    def offsets_for_times(self, requested):
        return {
            partition: SimpleNamespace(offset=1, timestamp=timestamp)
            for partition, timestamp in requested.items()
        }

    def assign(self, partitions):
        self.assigned = tuple(partitions)

    def seek(self, partition, offset):
        self.seeked[partition] = offset

    def poll(self, **_kwargs):
        self.poll_count += 1
        if self.poll_count > 1:
            return {}
        return {
            partition: [
                SimpleNamespace(
                    topic=TOPIC,
                    partition=partition.partition,
                    offset=1,
                    value=_value(7000000000 + partition.partition),
                    key=None,
                    timestamp=int(NOW.timestamp() * 1000),
                    headers=[],
                )
            ]
            for partition in self.assigned
        }

    def close(self, *, autocommit):
        self.closed = autocommit


def test_recent_replay_uses_explicit_offsets_and_shadow_store(tmp_path: Path) -> None:
    created = []

    def factory(**kwargs):
        consumer = _FakeConsumer(**kwargs)
        created.append(consumer)
        return consumer

    receipt = replay.collect_recent_replay(
        _config(tmp_path),
        consumer_factory=factory,
        topic_partition_factory=_TopicPartition,
        now=NOW,
        max_messages=10,
        max_bytes=1024 * 1024,
        max_seconds=10,
    )

    consumer = created[0]
    assert consumer.kwargs["group_id"] is None
    assert consumer.kwargs["enable_auto_commit"] is False
    assert consumer.kwargs["allow_auto_create_topics"] is False
    assert consumer.closed is False
    assert receipt["window"]["days"] == 7
    assert receipt["result"]["records_scanned"] == 2
    assert receipt["result"]["decision_counts"] == {"accepted": 2}
    assert receipt["result"]["shadow_store"]["outbox"] == {"shadow": 2}
    assert receipt["transport"] == {
        "assignment": "explicit",
        "group_id": None,
        "subscribed": False,
        "group_joined": False,
        "enable_auto_commit": False,
        "commit_performed": False,
        "allow_auto_create_topics": False,
        "isolation_level": "read_committed",
    }
    serialized = json.dumps(receipt, ensure_ascii=False)
    assert "private title" not in serialized
    assert "test-secret" not in serialized


def test_recent_replay_output_is_canonical_owner_only(tmp_path: Path) -> None:
    path = tmp_path / "receipt.json"
    digest = replay._write_owner_only(path, {"z": 1, "a": 2})

    assert path.read_bytes() == b'{"a":2,"z":1}\n'
    assert digest == replay._sha256(path.read_bytes())
    assert os.stat(path).st_mode & 0o777 == 0o600
