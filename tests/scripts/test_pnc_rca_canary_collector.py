from __future__ import annotations

import copy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import os
from pathlib import Path
import sqlite3
from threading import Event, Thread

import pytest

from gateway.pnc_rca_admission import build_rca_admission
from gateway import pnc_rca_prod_admission as prod_admission
from gateway import pnc_rca_prod_bootstrap as prod_bootstrap
from gateway.pnc_rca_delivery_contract import (
    build_terminal_delivery,
    build_terminal_thread_reply_effect,
    compute_artifact_set_id,
)
from gateway.pnc_group_binding import pnc_group_binding_receipt_path
from gateway.pnc_rca_stage_lineage import canonical_artifact_set_sha256
from gateway.pnc_rca_runtime_transition import (
    ensure_host_runtime_transition_schema,
    insert_host_runtime_transition,
)
from scripts import pnc_rca_canary_collector as collector_module
from scripts import pnc_rca_release_gate as release_gate
from scripts.pnc_rca_canary_collector import (
    CANARY_RECEIPT_SCHEMA_VERSION,
    CanaryCollectionError,
    CanaryReceiptCollector,
    CollectionResult,
    CollectorConfig,
    SourceRecord,
    TERMINAL_CANARY_RECEIPT_SCHEMA_VERSION,
    write_collection,
)
from tests.scripts import test_pnc_rca_release_gate as gate_contract_fixture
from tools import vm_task_tool


NOW = gate_contract_fixture.NOW
TOPIC = "feishu-project-workflow-event"
EVENT_OFFSET = 10
EVENT_UID = f"{TOPIC}:0:{EVENT_OFFSET}"


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha_json(value) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _resident_runtime_identity(service_label: str, ordinal: int) -> dict:
    return {
        "service_label": service_label,
        "pid": 42000 + ordinal,
        "process_create_time": 1_783_650_000.0 + ordinal,
        "boot_time": 1_783_000_000.0,
        "executable": "/candidate/.venv/bin/python",
        "script": f"/candidate/scripts/{service_label}.py",
        "cwd": "/candidate",
        "script_sha256": f"{ordinal + 1:x}" * 64,
        "runtime_files_sha256": f"{ordinal + 5:x}" * 64,
        "public_config_sha256": f"{ordinal + 9:x}" * 64,
        "loaded_runtime_sha256": ("d", "e", "f", "0")[ordinal] * 64,
    }


def _write_group_binding_receipt(receipt_dir: Path, record: dict) -> Path:
    receipt_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    receipt_dir.chmod(0o700)
    timestamp = datetime.fromisoformat(
        str(record["timestamp"]).replace("Z", "+00:00")
    ).astimezone(timezone.utc)
    path = pnc_group_binding_receipt_path(
        receipt_dir=receipt_dir,
        receipt_date=timestamp.date(),
        platform=record["platform"],
        chat_id=record["group_id"],
        user_id=record["requester"],
        message_id=record["message_id"],
    )
    path.write_text(
        json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    path.chmod(0o600)
    return path


def _manual_authorization_evidence(config: CollectorConfig, source_id: str):
    with sqlite3.connect(config.control_db_path) as connection:
        connection.row_factory = sqlite3.Row
        source = dict(
            connection.execute(
                "SELECT * FROM rca_trigger_sources WHERE source_id = ?",
                (source_id,),
            ).fetchone()
        )
    return collector_module._manual_authorization_evidence(
        source,
        receipt_dir=config.group_binding_receipt_dir,
        manual_chat_ids=config.manual_chat_ids,
    )


def _terminal_policy(config: CollectorConfig) -> dict:
    with sqlite3.connect(config.control_db_path) as connection:
        policy_version, policy_json = connection.execute(
            "SELECT policy_version, policy_json FROM rca_policy_snapshots "
            "WHERE active = 1"
        ).fetchone()
    return {
        "expected_rule_version": policy_version,
        "expected_workflow_policy": json.loads(policy_json),
    }


def _create_probe_database(path: Path, value: str) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE probe(value TEXT NOT NULL)")
        connection.execute("INSERT INTO probe(value) VALUES(?)", (value,))


def test_read_only_database_rejects_atomic_replacement_during_snapshot(tmp_path):
    database = tmp_path / "control.sqlite3"
    replacement = tmp_path / "replacement.sqlite3"
    _create_probe_database(database, "before")
    _create_probe_database(replacement, "after")

    with pytest.raises(
        CanaryCollectionError, match="control_database_changed_during_read"
    ):
        with collector_module.ReadOnlyDatabase(database) as reader:
            assert reader.rows("SELECT value FROM probe") == [{"value": "before"}]
            os.replace(replacement, database)


def test_database_provenance_rejects_replacement_after_snapshot(tmp_path):
    database = tmp_path / "control.sqlite3"
    replacement = tmp_path / "replacement.sqlite3"
    _create_probe_database(database, "before")
    _create_probe_database(replacement, "after")

    reader = collector_module.ReadOnlyDatabase(database)
    with reader:
        assert reader.rows("SELECT value FROM probe") == [{"value": "before"}]
    os.replace(replacement, database)

    with pytest.raises(
        CanaryCollectionError, match="control_database_changed_during_read"
    ):
        collector_module._database_provenance(
            database, "a" * 64, reader.snapshot_file_info
        )


SOURCE_ID = "g1q3-rca-source-v1-" + _sha_json(
    {"source_kind": "kafka_workflow_event", "dedupe": EVENT_UID}
)


class FakeRemoteReader:
    def __init__(self, records: dict[str, SourceRecord]):
        self.records = records
        self.calls: list[tuple[dict[str, str], dict[str, tuple[str, int]]]] = []

    def read_sources(self, *, json_paths, digest_paths=None):
        digests = dict(digest_paths or {})
        requested = {
            **dict(json_paths),
            **{name: value[0] for name, value in digests.items()},
        }
        self.calls.append((dict(json_paths), digests))
        result = {}
        for name, path in requested.items():
            record = self.records[name]
            if record.path != path:
                raise AssertionError(f"unexpected path for {name}")
            result[name] = record
        return result


def _json_source(path: str, body: dict) -> SourceRecord:
    return SourceRecord.json_record(path, body)


def _json_source_with_newline(path: str, body: dict) -> SourceRecord:
    raw = (
        json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")
    return SourceRecord.json_record(path, body, raw=raw)


def _manifest(submission: str):
    root = f"/mnt/tmp/{submission}/"
    index = b"<!doctype html><title>RCA</title><p>verified</p>"
    report_data = b'{"schema_version":"g1q3_rca_report_v2"}'
    manifest = {
        "schema_version": "delivery_manifest_v1",
        "sealed": True,
        "submission_key": submission,
        "business_key": "",
        "generation": 1,
        "project_key": "t03o4q",
        "work_item_type_key": "issue",
        "work_item_id": "7041712812",
        "artifact_revision": 1,
        "sealed_at": "2026-07-10T07:59:00+00:00",
        "deliverable_kind": "html",
        "dependencies_complete": True,
        "artifact_root": root,
        "html_validation": {
            "state": "html_delivery_ready",
            "report_data_sha256": _sha(report_data),
            "blockers": [],
            "fidelity_ok": True,
        },
        "artifacts": [
            {
                "role": "index_html",
                "path": "index.html",
                "size": len(index),
                "sha256": _sha(index),
                "media_type": "text/html; charset=utf-8",
                "required": True,
            },
            {
                "role": "report_data",
                "path": "report_data.json",
                "size": len(report_data),
                "sha256": _sha(report_data),
                "media_type": "application/json",
                "required": True,
            },
        ],
    }
    artifact_set_id = compute_artifact_set_id(manifest)
    report_url = (
        "http://192.168.26.174:18081/G1Q3_RCA/cases/"
        f"{submission}/{artifact_set_id}/index.html"
    )
    manifest["artifact_set_id"] = artifact_set_id
    manifest["report_url"] = report_url
    artifacts = [
        {
            **item,
            "path": root + item["path"],
            "relative_path": item["path"],
        }
        for item in manifest["artifacts"]
    ]
    return manifest, artifacts, index, report_data


def _create_db(path: Path, *, duplicate=False, promoted=True):
    admission = build_rca_admission(
        project_key="t03o4q",
        project_simple_name="g1q3",
        work_item_type_key="issue",
        work_item_id="7041712812",
        rule_version="issue-created-v1",
        topic=TOPIC,
        partition=0,
        offset=EVENT_OFFSET,
    )
    admission_body = admission.to_dict()
    submission = admission.submission_key
    manifest, artifacts, _index, _report_data = _manifest(submission)
    manifest["business_key"] = admission.business_key
    # business_key participates in artifact_set_id, so reseal after binding it.
    manifest["artifact_set_id"] = compute_artifact_set_id(manifest)
    manifest["report_url"] = (
        "http://192.168.26.174:18081/G1Q3_RCA/cases/"
        f"{submission}/{manifest['artifact_set_id']}/index.html"
    )
    artifacts = [
        {
            **item,
            "path": f"/mnt/tmp/{submission}/" + item["path"],
            "relative_path": item["path"],
        }
        for item in manifest["artifacts"]
    ]
    payload = {
        "schema_version": "pnc_rca_submission_outbox_v2",
        "business_key": admission.business_key,
        "submission_key": submission,
        "creation_rule_version": "issue-created-v1",
        "generation": 1,
        "origin_source_id": SOURCE_ID,
        "source_event_id": EVENT_UID,
        "topic": TOPIC,
        "partition": 0,
        "offset": EVENT_OFFSET,
        "admission": admission_body,
        "trigger_context": {
            "schema_version": "pnc_rca_trigger_context_v1",
            "source_kind": "kafka_workflow_event",
            "creation_rule_version": "issue-created-v1",
            "project_key": "t03o4q",
            "project_simple_name": "g1q3",
            "work_item_type_key": "issue",
            "work_item_id": "7041712812",
            "issue_url": (
                "https://project.feishu.cn/g1q3/issue/detail/7041712812"
            ),
            "title": "ACC braking issue",
        },
        "normalized_event": {
            "schema_version": "pnc_rca_workflow_event_v1",
            "creation_rule_version": "issue-created-v1",
            "project_key": "t03o4q",
            "project_simple_name": "g1q3",
            "work_item_type_key": "issue",
            "work_item_id": "7041712812",
            "issue_url": (
                "https://project.feishu.cn/g1q3/issue/detail/7041712812"
            ),
            "title": "ACC braking issue",
        },
    }
    reserved = gate_contract_fixture._derived_full_receipt("reserved")
    outbox_result = {
        "success": True,
        "submission_key": submission,
        "task_id": submission,
        "task_state": "pending",
        "deduped": False,
        "created": True,
        "returncode": 0,
        "capacity_admission": {},
        "derived_capacity_reservation": {
            "schema_version": reserved["schema_version"],
            "atomic_reservation": True,
            "reservation_id": reserved["reservation_id"],
            "fence": reserved["fence"],
            "contract_sha256": reserved["contract_sha256"],
            "status": "reserved",
            "receipt_sha256": _sha_json(reserved),
        },
    }
    contract = {
        "schema_version": "g1q3_delivery_contract_v1",
        "task_id": submission,
        "run_id": submission,
        "work_item_id": "7041712812",
    }
    delivery_id = "g1q3-rca-delivery-v1-" + "b" * 64
    effect_key = "g1q3-rca-effect-v1-" + "c" * 64
    target_key = "feishu_project:t03o4q:issue:7041712812"
    marker = f"[RCA_DELIVERY:{effect_key}:{manifest['artifact_set_id'][-12:]}]"
    semantic_sha = "d" * 64
    raw_event_sha = "a" * 64
    effect_payload = {
        "schema_version": "pnc_rca_delivery_effect_v1",
        "delivery_id": delivery_id,
        "effect_kind": "feishu_issue_comment",
        "effect_key": effect_key,
        "target_key": target_key,
        "artifact_set_id": manifest["artifact_set_id"],
        "report_url": manifest["report_url"],
        "semantic_payload_sha256": semantic_sha,
        "marker": marker,
    }
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.executescript(
        """
        CREATE TABLE kafka_inbox(
            event_uid TEXT, topic TEXT, partition_id INTEGER, offset_id INTEGER,
            decision TEXT, submission_mode TEXT, business_key TEXT,
            submission_key TEXT, generation INTEGER, raw_sha256 TEXT,
            processed_at TEXT
        );
        CREATE TABLE business_triggers(
            business_key TEXT, generation INTEGER, state TEXT,
            origin_source_id TEXT, source_event_id TEXT, source_topic TEXT,
            source_partition INTEGER, source_offset INTEGER
        );
        CREATE TABLE rca_outbox(
            outbox_id INTEGER, action TEXT, business_key TEXT, submission_key TEXT,
            creation_rule_version TEXT, generation INTEGER, origin_source_id TEXT,
            source_event_id TEXT,
            source_topic TEXT, source_partition INTEGER, source_offset INTEGER,
            payload_json TEXT, status TEXT, result_json TEXT, completed_at TEXT
        );
        CREATE TABLE rca_trigger_sources(
            source_id TEXT, source_kind TEXT, source_dedupe_key TEXT,
            payload_sha256 TEXT, platform TEXT, chat_id TEXT, thread_id TEXT,
            message_id TEXT, requester_id TEXT, kafka_event_uid TEXT,
            mode TEXT, outcome TEXT, created_at TEXT
        );
        CREATE TABLE rca_trigger_bindings(
            source_id TEXT, business_key TEXT, generation INTEGER,
            role TEXT, bound_at TEXT
        );
        CREATE TABLE rca_policy_snapshots(
            policy_sha256 TEXT, policy_version TEXT, policy_json TEXT,
            active INTEGER, activated_at TEXT
        );
        CREATE TABLE rca_delivery_subscriptions(
            subscription_key TEXT, business_key TEXT, generation INTEGER,
            source_id TEXT, effect_kind TEXT, target_key TEXT, target_json TEXT,
            required INTEGER, status TEXT, delivery_id TEXT, effect_key TEXT,
            catchup_requested_at TEXT, materialized_at TEXT,
            created_at TEXT, updated_at TEXT
        );
        CREATE TABLE rca_trigger_delivery_bindings(
            source_id TEXT, subscription_key TEXT, bound_at TEXT
        );
        CREATE TABLE rca_shadow_promotion_audit(
            audit_id INTEGER, event_uid TEXT, outbox_id INTEGER, submission_key TEXT,
            operator TEXT, reason TEXT, outcome TEXT, from_status TEXT, to_status TEXT
        );
        CREATE TABLE rca_execution_watch(
            submission_key TEXT, submission_outbox_id INTEGER, business_key TEXT,
            generation INTEGER, project_key TEXT, work_item_type_key TEXT,
            work_item_id TEXT, task_id TEXT, state TEXT, delivery_id TEXT,
            terminal_at TEXT
        );
        CREATE TABLE rca_delivery_jobs(
            delivery_id TEXT, submission_key TEXT, business_key TEXT,
            generation INTEGER, artifact_set_id TEXT, project_key TEXT,
            work_item_type_key TEXT, work_item_id TEXT, target_key TEXT,
            issue_url TEXT, report_url TEXT, status TEXT, manifest_json TEXT,
            contract_json TEXT, artifacts_json TEXT
        );
        CREATE TABLE rca_delivery_effects(
            effect_key TEXT, delivery_id TEXT, effect_kind TEXT, required INTEGER,
            target_key TEXT, payload_json TEXT, payload_sha256 TEXT, status TEXT,
            remote_receipt_json TEXT, completed_at TEXT
        );
        CREATE TABLE rca_delivery_attempts(
            effect_key TEXT, outcome TEXT, remote_id TEXT
        );
        """
    )
    ensure_host_runtime_transition_schema(connection)
    connection.executescript(
        """
        CREATE TABLE control_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE rca_delivery_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE INDEX idx_business_triggers_issue_scope
            ON business_triggers(business_key, generation);
        CREATE INDEX idx_rca_manual_operator_rate
            ON rca_trigger_sources(requester_id, created_at);
        """
    )
    connection.execute(
        "INSERT INTO control_meta(key, value) VALUES('schema_version', ?)",
        (release_gate.CONTROL_STORE_SCHEMA_VERSION,),
    )
    connection.execute(
        "INSERT INTO rca_delivery_meta(key, value) VALUES('schema_version', ?)",
        (release_gate.DELIVERY_STORE_SCHEMA_VERSION,),
    )
    policy = {
        "topic": TOPIC,
        "policy_version": "issue-created-v1",
        "project_keys": ["t03o4q"],
        "project_simple_names": ["g1q3"],
        "work_item_type_keys": ["issue"],
        "status_change_types": ["Reached"],
        "transitions": [
            {
                "state_key": "new-problem-state",
                "pre_status": 1,
                "cur_status": 2,
            }
        ],
    }
    policy_json = json.dumps(
        policy,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    connection.execute(
        "INSERT INTO rca_policy_snapshots VALUES(?,?,?,?,?)",
        (
            _sha(policy_json.encode("utf-8")),
            policy["policy_version"],
            policy_json,
            1,
            "2026-07-10T07:57:00+00:00",
        ),
    )
    connection.execute(
        "INSERT INTO kafka_inbox VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        (
            EVENT_UID,
            TOPIC,
            0,
            EVENT_OFFSET,
            "accepted",
            "pending",
            admission.business_key,
            submission,
            1,
            raw_event_sha,
            "2026-07-10T07:57:55+00:00",
        ),
    )
    connection.execute(
        "INSERT INTO business_triggers VALUES(?,?,?,?,?,?,?,?)",
        (
            admission.business_key,
            1,
            "submitted",
            SOURCE_ID,
            EVENT_UID,
            TOPIC,
            0,
            EVENT_OFFSET,
        ),
    )
    connection.execute(
        "INSERT INTO rca_trigger_sources VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            SOURCE_ID,
            "kafka_workflow_event",
            EVENT_UID,
            raw_event_sha,
            "",
            "",
            "",
            "",
            "",
            EVENT_UID,
            "issue_created",
            "",
            "2026-07-10T07:57:50+00:00",
        ),
    )
    connection.execute(
        "INSERT INTO rca_trigger_bindings VALUES(?,?,?,?,?)",
        (
            SOURCE_ID,
            admission.business_key,
            1,
            "origin",
            "2026-07-10T07:57:50+00:00",
        ),
    )
    connection.execute(
        "INSERT INTO rca_outbox VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            1,
            "submit_rca_issue_intake",
            admission.business_key,
            submission,
            "issue-created-v1",
            1,
            SOURCE_ID,
            EVENT_UID,
            TOPIC,
            0,
            EVENT_OFFSET,
            json.dumps(payload),
            "completed",
            json.dumps(outbox_result),
            "2026-07-10T07:58:10+00:00",
        ),
    )
    if duplicate:
        connection.execute(
            "INSERT INTO rca_outbox VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                2,
                "submit_rca_issue_intake",
                admission.business_key,
                submission,
                "issue-created-v1",
                1,
                SOURCE_ID,
                EVENT_UID,
                TOPIC,
                0,
                EVENT_OFFSET,
                json.dumps(payload),
                "completed",
                json.dumps(outbox_result),
                "2026-07-10T07:58:11+00:00",
            ),
        )
    if promoted:
        connection.execute(
            "INSERT INTO rca_shadow_promotion_audit VALUES(?,?,?,?,?,?,?,?,?)",
            (
                1,
                EVENT_UID,
                1,
                submission,
                "release-owner",
                "bounded production canary",
                "promoted",
                "shadow",
                "pending",
            ),
        )
    connection.execute(
        "INSERT INTO rca_execution_watch VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        (
            submission,
            1,
            admission.business_key,
            1,
            "t03o4q",
            "issue",
            "7041712812",
            submission,
            "delivery_created",
            delivery_id,
            "2026-07-10T07:59:00+00:00",
        ),
    )
    connection.execute(
        "INSERT INTO rca_delivery_jobs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            delivery_id,
            submission,
            admission.business_key,
            1,
            manifest["artifact_set_id"],
            "t03o4q",
            "issue",
            "7041712812",
            target_key,
            "https://project.feishu.cn/g1q3/issue/detail/7041712812",
            manifest["report_url"],
            "delivered",
            json.dumps(manifest),
            json.dumps(contract),
            json.dumps(artifacts),
        ),
    )
    subscription_key = "g1q3-rca-sub-v1-" + "e" * 64
    connection.execute(
        "INSERT INTO rca_delivery_subscriptions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            subscription_key,
            admission.business_key,
            1,
            None,
            "feishu_issue_comment",
            target_key,
            json.dumps(
                {
                    "schema_version": "pnc_rca_delivery_target_v1",
                    "platform": "feishu_project",
                    "project_key": "t03o4q",
                    "work_item_type_key": "issue",
                    "work_item_id": "7041712812",
                    "output_cap": "L1",
                }
            ),
            1,
            "materialized",
            delivery_id,
            effect_key,
            None,
            "2026-07-10T07:59:10+00:00",
            "2026-07-10T07:57:50+00:00",
            "2026-07-10T07:59:10+00:00",
        ),
    )
    connection.execute(
        "INSERT INTO rca_trigger_delivery_bindings VALUES(?,?,?)",
        (SOURCE_ID, subscription_key, "2026-07-10T07:57:50+00:00"),
    )
    connection.execute(
        "INSERT INTO rca_delivery_effects VALUES(?,?,?,?,?,?,?,?,?,?)",
        (
            effect_key,
            delivery_id,
            "feishu_issue_comment",
            1,
            target_key,
            json.dumps(effect_payload),
            semantic_sha,
            "succeeded",
            json.dumps({
                "remote_id": "feishu-comment-1",
                "marker": marker,
                "source": "read_after_write",
                "confirmed_field_keys": ["field_9193cb", "field_8c912e"],
            }),
            "2026-07-10T07:59:40+00:00",
        ),
    )
    connection.execute(
        "INSERT INTO rca_delivery_attempts VALUES(?,?,?)",
        (effect_key, "ack", "feishu-comment-1"),
    )
    for ordinal, service_label, transition_kind, entity_key, transitioned_at in (
        (
            0,
            "local.pnc.rca-kafka-consumer",
            "kafka_ingested",
            EVENT_UID,
            "2026-07-10T07:57:55+00:00",
        ),
        (
            1,
            "local.pnc.rca-outbox-dispatcher",
            "outbox_completed",
            "1",
            "2026-07-10T07:58:10+00:00",
        ),
        (
            2,
            "local.pnc.rca-delivery-collector",
            "delivery_created",
            delivery_id,
            "2026-07-10T07:59:00+00:00",
        ),
        (
            3,
            "local.pnc.rca-delivery-dispatcher",
            "effect_succeeded",
            effect_key,
            "2026-07-10T07:59:40+00:00",
        ),
    ):
        insert_host_runtime_transition(
            connection,
            submission_key=submission,
            business_key=admission.business_key,
            generation=1,
            service_label=service_label,
            transition_kind=transition_kind,
            entity_key=entity_key,
            runtime_identity=_resident_runtime_identity(service_label, ordinal),
            transitioned_at=transitioned_at,
        )
    connection.commit()
    connection.close()
    return admission_body, reserved, manifest, contract, artifacts


def _install_direct_bounded_activation(path: Path) -> None:
    epoch_id = "rca-direct-bounded-canary"
    source_identity_sha256 = _sha_json(
        {
            "event_uid": EVENT_UID,
            "offset": EVENT_OFFSET,
            "partition": 0,
            "topic": TOPIC,
        }
    )
    with sqlite3.connect(path) as connection:
        connection.executescript(
            """
            ALTER TABLE kafka_inbox ADD COLUMN activation_epoch_id TEXT;
            ALTER TABLE kafka_inbox ADD COLUMN activation_ingress_state TEXT;
            ALTER TABLE kafka_inbox ADD COLUMN activation_required INTEGER;
            ALTER TABLE kafka_inbox ADD COLUMN activation_slot_kind TEXT;
            ALTER TABLE kafka_inbox
                ADD COLUMN activation_source_identity_sha256 TEXT;
            ALTER TABLE business_triggers ADD COLUMN activation_epoch_id TEXT;
            ALTER TABLE business_triggers ADD COLUMN activation_ledger_id INTEGER;
            ALTER TABLE rca_outbox ADD COLUMN activation_epoch_id TEXT;
            ALTER TABLE rca_outbox ADD COLUMN activation_ledger_id INTEGER;
            CREATE TABLE rca_activation_epochs(
                epoch_id TEXT PRIMARY KEY, state TEXT, is_current INTEGER
            );
            CREATE TABLE rca_activation_admission_ledger(
                ledger_id INTEGER PRIMARY KEY, epoch_id TEXT, entrypoint TEXT,
                source_kind TEXT, source_identity_sha256 TEXT, slot_kind TEXT,
                decision TEXT, business_key TEXT, submission_key TEXT,
                generation INTEGER, bound_at TEXT
            );
            CREATE TABLE rca_activation_budget_slots(
                epoch_id TEXT, slot_kind TEXT, authorized_source_kind TEXT,
                authorized_identity_sha256 TEXT, consumed_ledger_id INTEGER,
                consumed_at TEXT
            );
            """
        )
        business_key, submission_key = connection.execute(
            "SELECT business_key, submission_key FROM rca_outbox WHERE outbox_id = 1"
        ).fetchone()
        connection.execute(
            "INSERT INTO rca_activation_epochs VALUES (?, 'bounded_active', 1)",
            (epoch_id,),
        )
        connection.execute(
            "INSERT INTO rca_activation_admission_ledger VALUES "
            "(1, ?, 'kafka_ingest', 'kafka', ?, 'kafka_success', 'admit', "
            "?, ?, 1, '2026-07-10T07:57:55+00:00')",
            (epoch_id, source_identity_sha256, business_key, submission_key),
        )
        connection.execute(
            "INSERT INTO rca_activation_budget_slots VALUES "
            "(?, 'kafka_success', 'kafka', ?, 1, "
            "'2026-07-10T07:57:55+00:00')",
            (epoch_id, source_identity_sha256),
        )
        connection.execute(
            "UPDATE kafka_inbox SET activation_epoch_id = ?, "
            "activation_ingress_state = 'bounded_active', activation_required = 1, "
            "activation_slot_kind = 'kafka_success', "
            "activation_source_identity_sha256 = ?",
            (epoch_id, source_identity_sha256),
        )
        connection.execute(
            "UPDATE business_triggers SET activation_epoch_id = ?, "
            "activation_ledger_id = 1",
            (epoch_id,),
        )
        connection.execute(
            "UPDATE rca_outbox SET activation_epoch_id = ?, activation_ledger_id = 1",
            (epoch_id,),
        )


def _reader_health():
    return copy.deepcopy(gate_contract_fixture._remote_reader_health())


def _write_private_json(path: Path, body: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(body, sort_keys=True), encoding="utf-8")
    path.chmod(0o600)


def _fixture(tmp_path, *, duplicate=False, promoted=True, direct_activation=False):
    db = tmp_path / "control.sqlite3"
    admission, reserved, manifest, contract, artifacts = _create_db(
        db, duplicate=duplicate, promoted=promoted
    )
    if direct_activation:
        _install_direct_bounded_activation(db)
    submission = admission["submission_key"]
    artifact_root = f"/mnt/tmp/{submission}/"
    request = copy.deepcopy(gate_contract_fixture._remote_execution_request())
    assert submission == gate_contract_fixture.SUBMISSION_KEY
    assert admission == gate_contract_fixture.CANARY_ADMISSION.to_dict()
    request["work_item"]["title"] = "ACC 制动问题"
    request["work_item"]["owners"] = ["测试负责人"]
    lifecycle = copy.deepcopy(gate_contract_fixture._derived_capacity_lifecycle())
    assert lifecycle["full_receipts"]["reserved"] == reserved
    request["toolchain"]["derived_capacity_reservation"] = reserved
    manifest_record = _json_source(artifact_root + "delivery_manifest.json", manifest)
    by_role = {item["role"]: item for item in artifacts}

    remote_wrapper = gate_contract_fixture._remote_read_canary_receipt(request)
    remote_read = remote_wrapper["receipt"]
    pipeline_projection, capacity_wrapper = (
        gate_contract_fixture._pipeline_capacity_canary_receipts(
            request,
            remote_wrapper,
            lifecycle,
            artifact_set_id=manifest["artifact_set_id"],
            manifest_artifact={
                "kind": "delivery_manifest",
                "path": manifest_record.path,
                "bytes": manifest_record.size_bytes,
                "sha256": manifest_record.raw_sha256,
            },
            index_artifact={
                "kind": "index_html",
                "path": by_role["index_html"]["path"],
                "bytes": by_role["index_html"]["size"],
                "sha256": by_role["index_html"]["sha256"],
            },
            report_data_artifact={
                "kind": "report_data",
                "path": by_role["report_data"]["path"],
                "bytes": by_role["report_data"]["size"],
                "sha256": by_role["report_data"]["sha256"],
            },
        )
    )
    meter = capacity_wrapper["receipt"]
    stage_paths = {
        short: pipeline_projection["downstream_stage_receipts"][short][
            "artifact_receipt_path"
        ]
        for short in collector_module.DOWNSTREAM_STAGE_NAMES
    }
    stage_bodies = {
        short: copy.deepcopy(
            pipeline_projection["downstream_stage_receipts"][short]["lineage"]
        )
        for short in collector_module.DOWNSTREAM_STAGE_NAMES
    }
    stage_records = {
        f"stage_{short}": _json_source(stage_paths[short], stage_bodies[short])
        for short in stage_paths
    }
    for short in collector_module.DOWNSTREAM_STAGE_NAMES:
        pipeline_projection["downstream_stage_receipts"][short][
            "artifact_receipt_sha256"
        ] = stage_records[f"stage_{short}"].raw_sha256
    remote_record = _json_source_with_newline(
        artifact_root + "s2_remote_read/remote_read_receipt.json", remote_read
    )
    meter_record = _json_source_with_newline(
        artifact_root + "derived_capacity_usage_receipt.json", meter
    )
    pipeline = {
        "schema_version": "g1q3_rca_pipeline_result_v1",
        "task_id": submission,
        **pipeline_projection,
        "downstream_stage_receipts": {
            short: {
                key: value
                for key, value in pipeline_projection["downstream_stage_receipts"][
                    short
                ].items()
                if key != "lineage"
            }
            for short in collector_module.DOWNSTREAM_STAGE_NAMES
        },
        "remote_read_receipt": {
            "path": remote_record.path,
            "sha256": remote_record.raw_sha256,
        },
        "capacity_usage": {
            **pipeline_projection["capacity_usage"],
            "sha256": meter_record.raw_sha256,
        },
        "derived_capacity_reservation": lifecycle,
    }
    request_record = _json_source_with_newline(
        artifact_root + "rca_execution_request.json", request
    )
    vm_execution = gate_contract_fixture._vm_execution_canary_receipt(
        request,
        vm_commit="3" * 40,
        vm_worker_commit="1" * 40,
        vm_service_entrypoint_sha256="4" * 64,
        vm_worker_entrypoint_sha256="2" * 64,
    )
    worker = vm_execution["worker_result"]["receipt"]
    service = vm_execution["service_result"]["receipt"]
    goal_bytes = vm_task_tool.build_rca_fixed_cli_goal(
        task_id=submission,
        admission=admission,
        execution_request=request,
    ).encode("utf-8")
    goal_record = SourceRecord(
        path=f"/home/mini/.hermes/shared-state/tasks/{submission}/goal.md",
        size_bytes=len(goal_bytes),
        raw_sha256=_sha(goal_bytes),
    )
    assert service["goal_sha256"] == goal_record.raw_sha256
    contract_sha256 = worker["result"]["rca_contract_sha256"]
    reservation_id = str(reserved["reservation_id"])
    reservation_fence = str(reserved["fence"])
    reservation_contract_sha256 = str(reserved["contract_sha256"])
    command_sha256 = prod_admission.command_sha256(
        prod_admission.build_rca_prod_command_argv(submission)
    )
    resource_snapshot = {
        "schema_version": prod_admission.SNAPSHOT_SCHEMA_VERSION,
        "observed_at": gate_contract_fixture.OBSERVED_AT,
        "root_available_bytes": 700 * 1024**3,
        "delivery_available_bytes": 900 * 1024**3,
        "root_device": "/dev/root",
        "delivery_device": "//hfs/tmp",
        "delivery_filesystem": "cifs",
        "delivery_mount_rw": True,
        "delivery_writable": True,
        "memory_available_bytes": 32 * 1024**3,
        "swap_free_ratio": 1.0,
        "load1": 1.0,
        "cpu_count": 16,
        "dnp_real": 0,
        "dnp_like": 0,
        "mcap_rss_bytes": 0,
        "mcap_process_count": 0,
    }
    admission_hmac_key = b"rca-canary-admission-test-key-0001"
    admission_receipt = {
        "schema_version": prod_admission.SCHEMA_VERSION,
        "receipt_id": "rca-prod-canary-admission-1",
        "issued_at": gate_contract_fixture.OBSERVED_AT,
        "expires_at": (NOW + timedelta(seconds=60)).isoformat(),
        "decision": "allow",
        "resource_class": "rca_prod",
        "trust_scope": prod_admission.TRUST_SCOPE,
        "single_task": True,
        "queue_if_blocked": False,
        "bypass_requested": False,
        "bindings": {
            "task_id": submission,
            "attempt_id": "rca-prod-canary-attempt-1",
            "work_dir": f"/mnt/tmp/{submission}",
            "reservation_id": reservation_id,
            "reservation_fence": reservation_fence,
            "reservation_contract_sha256": reservation_contract_sha256,
            "goal_sha256": goal_record.raw_sha256,
            "command_sha256": command_sha256,
            "contract_sha256": contract_sha256,
        },
        "capacity_authorization": {
            "receipt_id": "steady-capacity-1",
            "receipt_fingerprint": "a" * 64,
            "approval_evidence_sha256": "b" * 64,
            "authorization_receipt_sha256": "c" * 64,
            "expires_at": "2026-07-10T14:00:00+00:00",
            "successful_sample_count": 20,
            "input_materialized_sample_count": 0,
            "root_required_available_bytes": 400 * 1024**3,
            "delivery_required_available_bytes": 512 * 1024**3,
        },
        "resource_snapshot": resource_snapshot,
        "resource_snapshot_sha256": _sha_json(resource_snapshot),
        "receipt_fingerprint": "",
        "hmac_sha256": "",
    }
    admission_receipt["receipt_fingerprint"] = _sha(
        prod_admission.canonical_bytes({
            key: value
            for key, value in admission_receipt.items()
            if key not in {"receipt_fingerprint", "hmac_sha256"}
        })
    )
    admission_receipt["hmac_sha256"] = hmac.new(
        admission_hmac_key,
        prod_admission.canonical_bytes({
            key: value
            for key, value in admission_receipt.items()
            if key not in {"receipt_fingerprint", "hmac_sha256"}
        }),
        hashlib.sha256,
    ).hexdigest()
    task_meta = {
        "resource_class": "rca_prod",
        "lane": "heavy",
        "queue_if_blocked": False,
        "resource_gate_bypass": False,
        "reservation_id": reservation_id,
        "reservation_fence": reservation_fence,
        "reservation_contract_sha256": reservation_contract_sha256,
        "rca_prod_attempt_id": "rca-prod-canary-attempt-1",
        "rca_prod_goal_sha256": goal_record.raw_sha256,
        "rca_prod_command_sha256": command_sha256,
        "rca_prod_contract_sha256": contract_sha256,
        "rca_prod_admission_receipt": admission_receipt,
        "rca_prod_admission_key_fingerprint": _sha(admission_hmac_key),
    }
    service["request_storage"]["sha256"] = request_record.raw_sha256
    service["request_storage"]["bytes"] = request_record.size_bytes
    initial = {
        "task_meta": _json_source(
            f"/home/mini/.hermes/shared-state/tasks/{submission}/meta.json",
            task_meta,
        ),
        "worker_result": _json_source(
            f"{collector_module.DEFAULT_VM_WORKER_ROOT}/tasks/{submission}/local-result.json",
            worker,
        ),
        "execution_request": request_record,
        "remote_read": remote_record,
        "capacity_lifecycle": _json_source(
            artifact_root + "derived_capacity_reservation_receipt.json", lifecycle
        ),
        "capacity_meter": meter_record,
        "pipeline": _json_source(artifact_root + "pipeline_result.json", pipeline),
        "service_result": _json_source(
            artifact_root + "rca_service_result.json", service
        ),
        "delivery_manifest": _json_source(
            artifact_root + "delivery_manifest.json", manifest
        ),
        "delivery_contract": _json_source(
            artifact_root + "delivery_contract.json", contract
        ),
        "goal": goal_record,
    }
    initial["delivery_manifest"] = manifest_record
    initial.update(stage_records)
    initial["artifact_remote_stream_cache"] = SourceRecord(
        path=remote_read["derived_stream_cache"]["path"],
        size_bytes=remote_read["derived_stream_cache"]["bytes"],
        raw_sha256=remote_read["derived_stream_cache"]["sha256"],
    )
    initial["artifact_index_html"] = SourceRecord(
        path=by_role["index_html"]["path"],
        size_bytes=by_role["index_html"]["size"],
        raw_sha256=by_role["index_html"]["sha256"],
    )
    initial["artifact_report_data"] = SourceRecord(
        path=by_role["report_data"]["path"],
        size_bytes=by_role["report_data"]["size"],
        raw_sha256=by_role["report_data"]["sha256"],
    )
    evidence = tmp_path / "evidence"
    health = _reader_health()
    _write_private_json(evidence / "remote_reader_health.json", health)
    requested_urls = [manifest["report_url"]]
    request_list_sha256 = _sha_json(requested_urls)
    zero_counts = {field: 0 for field in collector_module.BROWSER_ZERO_FIELDS}
    viewports = {}
    for name, width, height, scale, mobile in (
        ("desktop", 1440, 1000, 1.0, False),
        ("mobile", 390, 844, 3.0, True),
    ):
        viewports[name] = {
            "name": name,
            "width": width,
            "height": height,
            "device_scale_factor": scale,
            "mobile": mobile,
            "nonblank": True,
            "request_count": 1,
            "requested_urls": requested_urls,
            "request_list_sha256": request_list_sha256,
            "unmanifested_urls": [],
            **zero_counts,
            "visible_element_count": 1,
            "visible_text_length": 8,
            "visible_media_count": 0,
            "document_width": width,
            "document_height": height,
            "title_sha256": "7" * 64,
            "index_html_sha256": by_role["index_html"]["sha256"],
            "index_html_size_bytes": by_role["index_html"]["size"],
        }
    smoke = {
        "schema_version": "pnc_rca_html_browser_smoke_v2",
        "observed_at": gate_contract_fixture.OBSERVED_AT,
        "ok": True,
        "machine_generated": True,
        "source": "chromium_cdp_network_runtime_log",
        "engine": "chromium",
        "artifact_policy": "passive_static_html_v1",
        "artifact_set_id": manifest["artifact_set_id"],
        "report_url": manifest["report_url"],
        "index_html_sha256": by_role["index_html"]["sha256"],
        "manifest_sha256": initial["delivery_manifest"].raw_sha256,
        "delivery_contract_sha256": initial["delivery_contract"].raw_sha256,
        "manifest_url_count": 2,
        "manifest_url_set_sha256": _sha_json([
            manifest["report_url"],
            manifest["report_url"].replace("index.html", "report_data.json"),
        ]),
        "requested_urls": requested_urls,
        "request_list_sha256": request_list_sha256,
        "network_closure": "manifest_allowlist",
        "desktop_nonblank": True,
        "mobile_nonblank": True,
        "request_count": 2,
        **zero_counts,
        "browser": {
            "executable": "/Applications/Chromium.app/Contents/MacOS/Chromium",
            "product": "Chrome/149.0.0.0",
            "protocol_version": "1.3",
        },
        "viewports": viewports,
        "blockers": [],
    }
    smoke["evidence_sha256"] = _sha_json(smoke)
    _write_private_json(
        evidence / "machine_sources" / submission / "browser_smoke.json", smoke
    )
    config = CollectorConfig(
        control_db_path=db,
        delivery_db_path=db,
        evidence_dir=evidence,
        ssh_mini_agent=tmp_path / "must-not-run",
        prod_admission_hmac_key=admission_hmac_key,
    )
    return config, FakeRemoteReader(initial), submission


def _gateway_runtime_identity() -> dict:
    return {
        "service_label": "ai.hermes.gateway",
        "pid": 41000,
        "process_create_time": 1_783_650_000.0,
        "boot_time": 1_783_000_000.0,
        "executable": "/candidate/.venv/bin/python",
        "script": "/candidate/gateway/run.py",
        "cwd": "/candidate",
        "script_sha256": "a" * 64,
        "runtime_files_sha256": "b" * 64,
        "public_config_sha256": "c" * 64,
        "loaded_runtime_sha256": "d" * 64,
    }


def _manual_observer_fixture(tmp_path):
    config, reader, submission = _fixture(tmp_path)
    chat_id = "oc_6cfc782212009ff4cd815349909dd423"
    thread_id = "topic:om_root"
    message_id = "om_manual_1"
    requester_id = "ou_manual_1"
    source_dedupe_key = f"feishu:{message_id}"
    source_id = "g1q3-rca-source-v1-" + _sha_json(
        {"source_kind": "feishu_group_manual", "dedupe": source_dedupe_key}
    )
    created_at = "2026-07-10T07:58:00+00:00"
    payload_sha = "9" * 64
    delivery_id = "g1q3-rca-delivery-v1-" + "b" * 64
    thread_effect_key = "g1q3-rca-effect-v1-" + "8" * 64
    thread_subscription = "g1q3-rca-sub-v1-" + "7" * 64
    target_key = f"feishu_thread:{chat_id}:om_root"
    target = {
        "schema_version": "pnc_rca_delivery_target_v1",
        "platform": "feishu",
        "chat_id": chat_id,
        "thread_id": thread_id,
        "reply_anchor_message_id": "om_root",
        "source_message_id": message_id,
        "requester_id": requester_id,
        "reply_in_thread": True,
        "output_cap": "L1",
    }
    connection = sqlite3.connect(config.control_db_path)
    connection.row_factory = sqlite3.Row
    business_key = connection.execute(
        "SELECT business_key FROM rca_outbox WHERE submission_key = ?", (submission,)
    ).fetchone()[0]
    artifact_set_id, report_url = connection.execute(
        "SELECT artifact_set_id, report_url FROM rca_delivery_jobs WHERE submission_key = ?",
        (submission,),
    ).fetchone()
    connection.execute(
        "INSERT INTO rca_trigger_sources VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            source_id,
            "feishu_group_manual",
            source_dedupe_key,
            payload_sha,
            "feishu",
            chat_id,
            thread_id,
            message_id,
            requester_id,
            None,
            "run_or_join",
            "joined",
            created_at,
        ),
    )
    connection.execute(
        "INSERT INTO rca_trigger_bindings VALUES(?,?,?,?,?)",
        (source_id, business_key, 1, "observer", created_at),
    )
    issue_subscription = connection.execute(
        "SELECT subscription_key FROM rca_delivery_subscriptions "
        "WHERE effect_kind = 'feishu_issue_comment'"
    ).fetchone()[0]
    connection.execute(
        "INSERT INTO rca_trigger_delivery_bindings VALUES(?,?,?)",
        (source_id, issue_subscription, created_at),
    )
    connection.execute(
        "INSERT INTO rca_delivery_subscriptions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            thread_subscription,
            business_key,
            1,
            source_id,
            "feishu_thread_reply",
            target_key,
            json.dumps(target),
            1,
            "materialized",
            delivery_id,
            thread_effect_key,
            None,
            "2026-07-10T07:59:15+00:00",
            created_at,
            "2026-07-10T07:59:15+00:00",
        ),
    )
    connection.execute(
        "INSERT INTO rca_trigger_delivery_bindings VALUES(?,?,?)",
        (source_id, thread_subscription, created_at),
    )
    marker = f"[RCA_DELIVERY:{thread_effect_key}:{artifact_set_id[-12:]}]"
    semantic_sha = "6" * 64
    connection.execute(
        "INSERT INTO rca_delivery_effects VALUES(?,?,?,?,?,?,?,?,?,?)",
        (
            thread_effect_key,
            delivery_id,
            "feishu_thread_reply",
            1,
            target_key,
            json.dumps(
                {
                    "effect_key": thread_effect_key,
                    "artifact_set_id": artifact_set_id,
                    "report_url": report_url,
                    "target_key": target_key,
                    "marker": marker,
                    "semantic_payload_sha256": semantic_sha,
                }
            ),
            semantic_sha,
            "succeeded",
            json.dumps({"remote_id": "feishu-thread-reply-1", "marker": marker}),
            "2026-07-10T07:59:45+00:00",
        ),
    )
    connection.execute(
        "INSERT INTO rca_delivery_attempts VALUES(?,?,?)",
        (thread_effect_key, "ack", "feishu-thread-reply-1"),
    )
    insert_host_runtime_transition(
        connection,
        submission_key=submission,
        business_key=business_key,
        generation=1,
        service_label="local.pnc.rca-delivery-dispatcher",
        transition_kind="effect_succeeded",
        entity_key=thread_effect_key,
        runtime_identity=_resident_runtime_identity(
            "local.pnc.rca-delivery-dispatcher", 3
        ),
        transitioned_at="2026-07-10T07:59:45+00:00",
    )
    connection.commit()
    connection.close()

    receipt_dir = tmp_path / "group-binding-receipts"
    authorization = {
        "schema_version": "pnc_rca_manual_authorization_v2",
        "manual_intake_enabled": True,
        "manual_chat_allowlist_valid": True,
        "manual_chat_allowlist_sha256": _sha_json([chat_id]),
        "chat_allowed": True,
        "mention_verified": True,
        "debug_requested": False,
        "debug_enabled": False,
        "requester_allowed": True,
        "debug_user_allowlist_sha256": _sha_json([]),
        "manual_operator_rate_limit": 3,
        "manual_operator_rate_window_seconds": 600,
        "authorized": True,
    }
    record = {
        "event_type": "group_binding_decision",
        "timestamp": "2026-07-10T07:57:59+00:00",
        "platform": "feishu",
        "group_id": chat_id,
        "requester": requester_id,
        "message_id": message_id,
        "decision": "accepted",
        "route_surface": "rca_manual_intake",
        "risk_gate": "manual_intake_control_store",
        "decision_snapshot": {
            "handoff_contract": {
                "mode": "run_or_join",
                "source_kind": "feishu_group_manual",
            }
        },
        "manual_authorization": authorization,
        "gateway_runtime_identity": _gateway_runtime_identity(),
    }
    _write_group_binding_receipt(receipt_dir, record)
    return (
        replace(
            config,
            group_binding_receipt_dir=receipt_dir,
            manual_chat_ids=(chat_id,),
        ),
        reader,
        source_id,
    )


def _terminal_manual_fixture(tmp_path):
    config, reader, source_id = _manual_observer_fixture(tmp_path)
    connection = sqlite3.connect(config.control_db_path)
    connection.row_factory = sqlite3.Row
    connection.execute(
        "ALTER TABLE rca_delivery_jobs ADD COLUMN outcome TEXT NOT NULL DEFAULT 'success'"
    )
    connection.execute(
        "ALTER TABLE rca_delivery_jobs ADD COLUMN outcome_key TEXT NOT NULL DEFAULT ''"
    )
    connection.execute(
        "ALTER TABLE rca_delivery_jobs ADD COLUMN terminal_state TEXT NOT NULL DEFAULT ''"
    )
    connection.execute(
        "ALTER TABLE rca_delivery_jobs ADD COLUMN terminal_error_code TEXT NOT NULL DEFAULT ''"
    )
    connection.execute(
        "ALTER TABLE rca_delivery_effects ADD COLUMN outcome TEXT NOT NULL DEFAULT 'success'"
    )
    connection.execute(
        "ALTER TABLE rca_execution_watch ADD COLUMN last_error_code TEXT NOT NULL DEFAULT ''"
    )
    outbox = connection.execute(
        "SELECT business_key, submission_key, generation FROM rca_outbox"
    ).fetchone()
    terminal = build_terminal_delivery(
        business_key=outbox["business_key"],
        submission_key=outbox["submission_key"],
        generation=outbox["generation"],
        project_key="t03o4q",
        work_item_type_key="issue",
        work_item_id="7041712812",
        outcome="terminal_failed",
        terminal_state="failed",
        error_code="vm_terminal_failed",
    )
    thread_subscription = connection.execute(
        "SELECT target_key, target_json FROM rca_delivery_subscriptions "
        "WHERE effect_kind = 'feishu_thread_reply'"
    ).fetchone()
    thread_key, thread_sha, thread_payload = build_terminal_thread_reply_effect(
        issue_effect_payload=terminal.effect_payload,
        target_key=thread_subscription["target_key"],
        target=json.loads(thread_subscription["target_json"]),
    )
    connection.execute("DELETE FROM rca_delivery_attempts")
    connection.execute("DELETE FROM rca_delivery_effects")
    connection.execute(
        "DELETE FROM rca_host_runtime_transitions "
        "WHERE service_label IN (?, ?)",
        (
            "local.pnc.rca-delivery-collector",
            "local.pnc.rca-delivery-dispatcher",
        ),
    )
    connection.execute(
        """
        UPDATE rca_delivery_jobs
           SET delivery_id = ?, artifact_set_id = ?, target_key = ?,
               issue_url = '', report_url = '', status = 'delivered',
               manifest_json = '{}', contract_json = '{}', artifacts_json = '[]',
               outcome = ?, outcome_key = ?, terminal_state = ?,
               terminal_error_code = ?
        """,
        (
            terminal.delivery_id,
            terminal.outcome_key,
            terminal.target_key,
            terminal.outcome,
            terminal.outcome_key,
            terminal.terminal_state,
            terminal.error_code,
        ),
    )
    connection.execute(
        """
        UPDATE rca_execution_watch
           SET delivery_id = ?, state = 'delivery_created', terminal_at = ?,
               last_error_code = ?
        """,
        (terminal.delivery_id, "2026-07-10T07:59:00+00:00", terminal.error_code),
    )
    for effect_kind, effect_key in (
        ("feishu_issue_comment", terminal.effect_key),
        ("feishu_thread_reply", thread_key),
    ):
        connection.execute(
            "UPDATE rca_delivery_subscriptions SET delivery_id = ?, effect_key = ? "
            "WHERE effect_kind = ?",
            (terminal.delivery_id, effect_key, effect_kind),
        )
    for effect_kind, effect_key, target_key, payload_sha, payload, remote_id in (
        (
            "feishu_issue_comment",
            terminal.effect_key,
            terminal.target_key,
            terminal.semantic_payload_sha256,
            terminal.effect_payload,
            "feishu-terminal-comment-1",
        ),
        (
            "feishu_thread_reply",
            thread_key,
            thread_subscription["target_key"],
            thread_sha,
            thread_payload,
            "feishu-terminal-thread-1",
        ),
    ):
        connection.execute(
            """
            INSERT INTO rca_delivery_effects(
                effect_key, delivery_id, effect_kind, required, target_key,
                payload_json, payload_sha256, status, remote_receipt_json,
                completed_at, outcome
            ) VALUES(?, ?, ?, 1, ?, ?, ?, 'succeeded', ?, ?, ?)
            """,
            (
                effect_key,
                terminal.delivery_id,
                effect_kind,
                target_key,
                json.dumps(payload),
                payload_sha,
                json.dumps({"remote_id": remote_id, "marker": payload["marker"]}),
                "2026-07-10T07:59:45+00:00",
                terminal.outcome,
            ),
        )
        connection.execute(
            "INSERT INTO rca_delivery_attempts VALUES(?,?,?)",
            (effect_key, "ack", remote_id),
        )
    insert_host_runtime_transition(
        connection,
        submission_key=outbox["submission_key"],
        business_key=outbox["business_key"],
        generation=outbox["generation"],
        service_label="local.pnc.rca-delivery-collector",
        transition_kind="delivery_created",
        entity_key=terminal.delivery_id,
        runtime_identity=_resident_runtime_identity(
            "local.pnc.rca-delivery-collector", 2
        ),
        transitioned_at="2026-07-10T07:59:00+00:00",
    )
    for effect_key in (terminal.effect_key, thread_key):
        insert_host_runtime_transition(
            connection,
            submission_key=outbox["submission_key"],
            business_key=outbox["business_key"],
            generation=outbox["generation"],
            service_label="local.pnc.rca-delivery-dispatcher",
            transition_kind="effect_succeeded",
            entity_key=effect_key,
            runtime_identity=_resident_runtime_identity(
                "local.pnc.rca-delivery-dispatcher", 3
            ),
            transitioned_at="2026-07-10T07:59:45+00:00",
        )
    connection.commit()
    connection.close()
    return config, reader, source_id


def _quarantined_manual_fixture(tmp_path, *, chat_id: str | None = None):
    from gateway.pnc_rca_delivery_store import RcaDeliveryStore
    from tests.gateway.test_pnc_rca_delivery_store import (
        _control,
        _manual_request,
        _quarantine_submission,
    )

    control, _result = _control(tmp_path, completed=False)
    request = replace(
        _manual_request("om_quarantined_manual"),
        chat_id=chat_id or sorted(collector_module.FIXED_MANUAL_CHAT_IDS)[0],
    )
    manual = control.admit_manual_trigger(
        request,
        allowed_chat_ids={request.chat_id},
        submit_enabled=True,
        now=NOW - timedelta(seconds=10),
    )
    _quarantine_submission(control)
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    assert store.backfill_completed_submissions(now=NOW + timedelta(seconds=2)) == 1
    for index in range(2):
        claim = store.claim_due_effect(
            lease_owner="terminal-canary-dispatcher",
            now=NOW + timedelta(seconds=3 + index),
        )
        assert claim is not None
        remote_id = f"feishu-quarantine-{index + 1}"
        store.complete_effect(
            claim=claim,
            outcome="ack",
            remote_id=remote_id,
            receipt={"remote_id": remote_id, "marker": claim.payload["marker"]},
            now=NOW + timedelta(seconds=3 + index),
        )

    authorization = {
        "schema_version": "pnc_rca_manual_authorization_v2",
        "manual_intake_enabled": True,
        "manual_chat_allowlist_valid": True,
        "manual_chat_allowlist_sha256": _sha_json([request.chat_id]),
        "chat_allowed": True,
        "mention_verified": True,
        "debug_requested": False,
        "debug_enabled": False,
        "requester_allowed": True,
        "debug_user_allowlist_sha256": _sha_json([]),
        "manual_operator_rate_limit": 3,
        "manual_operator_rate_window_seconds": 600,
        "authorized": True,
    }
    source = next(
        row
        for row in control.list_rows("rca_trigger_sources")
        if row["source_id"] == manual.source_id
    )
    record = {
        "event_type": "group_binding_decision",
        "timestamp": source["created_at"],
        "platform": request.platform,
        "group_id": request.chat_id,
        "requester": request.requester_id,
        "message_id": request.message_id,
        "decision": "accepted",
        "route_surface": "rca_manual_intake",
        "risk_gate": "manual_intake_control_store",
        "decision_snapshot": {
            "handoff_contract": {
                "mode": request.mode,
                "source_kind": "feishu_group_manual",
            }
        },
        "manual_authorization": authorization,
        "gateway_runtime_identity": _gateway_runtime_identity(),
    }
    receipt_dir = tmp_path / "group-binding-receipts"
    _write_group_binding_receipt(receipt_dir, record)
    config = CollectorConfig(
        control_db_path=tmp_path / "control.sqlite3",
        delivery_db_path=tmp_path / "control.sqlite3",
        evidence_dir=tmp_path / "evidence",
        group_binding_receipt_dir=receipt_dir,
        manual_chat_ids=(request.chat_id,),
        ssh_mini_agent=tmp_path / "must-not-run",
    )
    return config, FakeRemoteReader({}), manual.source_id


def _validate_collector_output_with_release_gate(receipt):
    service_provenance = receipt["vm"]["service_result"]["receipt"][
        "service_provenance"
    ]
    attestation = receipt["vm"]["execution_attestation"]
    return release_gate.validate_canary_receipt(
        receipt,
        expected_execution_origin_id=SOURCE_ID,
        expected_execution_origin_kind="kafka_issue_created",
        expected_observed_source_id=SOURCE_ID,
        expected_observed_source_kind="kafka_issue_created",
        expected_request_sha256=collector_module._sha256_execution_request(
            receipt["execution_request"]
        ),
        expected_admission=receipt["admission"],
        expected_reader_fingerprint=receipt["remote_read"]["reader_fingerprint"],
        expected_requested_scope=collector_module._requested_scope_from_remote_read(
            receipt["remote_read"]["receipt"]
        ),
        expected_vm_commit=service_provenance["vm_source_commit"],
        expected_vm_worker_commit=attestation["worker_source_commit"],
        expected_vm_service_entrypoint_sha256=service_provenance[
            "service_entrypoint_sha256"
        ],
        expected_vm_worker_entrypoint_sha256=attestation["worker_entrypoint_sha256"],
        now=NOW,
        max_age_seconds=collector_module.GATE_VALIDATION_MAX_AGE_SECONDS,
    )


def test_collects_v8_from_bound_execution_sources_without_network(tmp_path):
    config, reader, submission = _fixture(tmp_path)

    result = CanaryReceiptCollector(
        config, remote_reader=reader, now=lambda: NOW
    ).collect(SOURCE_ID)

    assert result.receipt["schema_version"] == CANARY_RECEIPT_SCHEMA_VERSION
    assert result.receipt["execution_origin"]["source_id"] == SOURCE_ID
    assert result.receipt["observed_trigger_source"]["source_id"] == SOURCE_ID
    assert result.receipt["submission_key"] == submission
    assert result.receipt["submission_count"] == 1
    assert result.receipt["outbox"]["status"] == "completed"
    assert result.receipt["outbox"]["origin_source_id"] == SOURCE_ID
    assert result.receipt["vm"]["terminal_state"] == "completed"
    assert result.receipt["vm"]["execution_plane"]["resource_class"] == "rca_prod"
    assert result.receipt["vm"]["capacity_admission"] == {
        "resource_class": "rca_prod",
        "capacity_mode": "steady",
        "task_meta_sha256": reader.records["task_meta"].raw_sha256,
        "admission_receipt_sha256": _sha_json(
            reader.records["task_meta"].body["rca_prod_admission_receipt"]
        ),
        "admission_schema_version": prod_admission.SCHEMA_VERSION,
        "admission_key_fingerprint": _sha(config.prod_admission_hmac_key),
        "queue_if_blocked": False,
        "resource_gate_bypass": False,
    }
    assert (
        result.receipt["delivery"]["remote_receipt"]["remote_id"] == "feishu-comment-1"
    )
    assert set(result.receipt["pipeline"]) == {
        "status",
        "stage",
        "blocker",
        "remote_read_receipt",
        "remote_stream_cache",
        "downstream_stage_receipts",
        "capacity_usage",
    }
    assert result.receipt["delivery"]["remote_receipt"] == {
        "remote_id": "feishu-comment-1",
        "confirmed_field_keys": ["field_9193cb", "field_8c912e"],
    }
    assert {
        item["effect_kind"] for item in result.receipt["delivery_obligations"]
    } == {"feishu_issue_comment"}
    assert (
        result.receipt["pipeline"]["remote_read_receipt"]["sha256"]
        == reader.records["remote_read"].canonical_sha256
    )
    assert (
        result.provenance["remote_transport"]["files"]["remote_read"]["raw_sha256"]
        != result.receipt["pipeline"]["remote_read_receipt"]["sha256"]
    )
    request = result.receipt["execution_request"]
    assert request["work_item"]["title"] == "ACC 制动问题"
    assert result.receipt["outbox"][
        "execution_request_sha256"
    ] == collector_module._sha256_execution_request(request)
    assert result.receipt["outbox"]["execution_request_sha256"] != _sha_json(request)
    assert result.provenance["read_only"] is True
    assert result.provenance["external_side_effects"] is False
    assert (
        result.provenance["remote_transport"]["files"]["task_meta"]["raw_sha256"]
        == reader.records["task_meta"].raw_sha256
    )
    assert len(reader.calls) == 3
    artifact_digests = reader.calls[2][1]
    assert {
        name: limit for name, (_path, limit) in artifact_digests.items()
    } == {
        "artifact_remote_stream_cache": reader.records[
            "artifact_remote_stream_cache"
        ].size_bytes,
        "artifact_index_html": reader.records["artifact_index_html"].size_bytes,
        "artifact_report_data": reader.records["artifact_report_data"].size_bytes,
    }


def test_collector_requires_real_rca_prod_admission_hmac(tmp_path):
    config, reader, _submission = _fixture(tmp_path)
    config = replace(
        config,
        prod_admission_hmac_key=b"different-admission-hmac-key-0001",
    )

    with pytest.raises(
        CanaryCollectionError, match="rca_prod_task_meta_signature_invalid"
    ):
        CanaryReceiptCollector(
            config, remote_reader=reader, now=lambda: NOW
        ).collect(SOURCE_ID)

    assert "admission_hmac_key" not in repr(config)


def test_bootstrap_capacity_projection_is_signed_and_release_bound(tmp_path):
    config, reader, submission = _fixture(tmp_path)
    task_meta = copy.deepcopy(reader.records["task_meta"].body)
    steady = task_meta["rca_prod_admission_receipt"]
    started_at = (NOW - timedelta(hours=1)).isoformat()
    deadline = (NOW + timedelta(days=7)).isoformat()
    release_bom_sha256 = "7" * 64
    active_release_binding_sha256 = "6" * 64
    authorization = {
        "schema_version": prod_bootstrap.SCHEMA_VERSION,
        "capacity_mode": "bootstrap",
        "receipt_id": "bootstrap-capacity-authorization-1",
        "receipt_fingerprint": "8" * 64,
        "authorization_receipt_sha256": "9" * 64,
        "bootstrap_epoch_id": "rca-bootstrap-canary-1",
        "started_at": started_at,
        "deadline": deadline,
        "release_approval_id": "rca-release-approval-1",
        "release_bom_sha256": release_bom_sha256,
        "active_release_binding_sha256": active_release_binding_sha256,
        "approval_evidence_sha256": "a" * 64,
        "authorized_by": "release-owner",
        "max_concurrency": prod_bootstrap.MAX_CONCURRENCY,
        "daily_started_attempt_quota": prod_bootstrap.DAILY_STARTED_ATTEMPT_QUOTA,
        "quota_timezone": prod_bootstrap.QUOTA_TIMEZONE,
        "root_reserve_bytes": prod_bootstrap.ROOT_RESERVE_BYTES,
        "root_per_task_bytes": prod_bootstrap.ROOT_PER_TASK_BYTES,
        "root_required_available_bytes": prod_bootstrap.ROOT_REQUIRED_AVAILABLE_BYTES,
        "delivery_reserve_bytes": prod_bootstrap.DELIVERY_RESERVE_BYTES,
        "delivery_per_task_bytes": prod_bootstrap.DELIVERY_PER_TASK_BYTES,
        "delivery_required_available_bytes": (
            prod_bootstrap.DELIVERY_REQUIRED_AVAILABLE_BYTES
        ),
        "queue_if_blocked": False,
        "bypass_requested": False,
        "input_materialization": "forbidden",
    }
    receipt = {
        key: value
        for key, value in steady.items()
        if key not in {"capacity_authorization", "receipt_fingerprint", "hmac_sha256"}
    }
    receipt.update(
        {
            "schema_version": prod_admission.BOOTSTRAP_SCHEMA_VERSION,
            "capacity_mode": "bootstrap",
            "bootstrap_authorization": authorization,
            "receipt_fingerprint": "",
            "hmac_sha256": "",
        }
    )
    signed_body = prod_admission.canonical_bytes({
        key: value
        for key, value in receipt.items()
        if key not in {"receipt_fingerprint", "hmac_sha256"}
    })
    receipt["receipt_fingerprint"] = _sha(signed_body)
    receipt["hmac_sha256"] = hmac.new(
        config.prod_admission_hmac_key,
        signed_body,
        hashlib.sha256,
    ).hexdigest()
    task_meta.update(
        {
            "rca_prod_admission_receipt": receipt,
            "rca_prod_capacity_mode": "bootstrap",
            "rca_prod_bootstrap_epoch_id": authorization["bootstrap_epoch_id"],
            "rca_prod_bootstrap_started_at": started_at,
            "rca_prod_bootstrap_deadline": deadline,
            "rca_prod_bootstrap_authorization_fingerprint": authorization[
                "receipt_fingerprint"
            ],
            "rca_prod_bootstrap_release_approval_id": authorization[
                "release_approval_id"
            ],
            "rca_prod_bootstrap_max_concurrency": prod_bootstrap.MAX_CONCURRENCY,
            "rca_prod_bootstrap_daily_started_attempt_quota": (
                prod_bootstrap.DAILY_STARTED_ATTEMPT_QUOTA
            ),
            "rca_prod_bootstrap_quota_timezone": prod_bootstrap.QUOTA_TIMEZONE,
            "rca_prod_bootstrap_root_required_available_bytes": (
                prod_bootstrap.ROOT_REQUIRED_AVAILABLE_BYTES
            ),
            "rca_prod_bootstrap_delivery_required_available_bytes": (
                prod_bootstrap.DELIVERY_REQUIRED_AVAILABLE_BYTES
            ),
            "rca_prod_release_bom_sha256": release_bom_sha256,
            "rca_prod_active_release_binding_sha256": (
                active_release_binding_sha256
            ),
        }
    )
    task_meta_record = _json_source(
        f"/home/mini/.hermes/shared-state/tasks/{submission}/meta.json",
        task_meta,
    )

    projection = collector_module._capacity_admission_projection(
        task_meta,
        task_meta_record,
        submission_key=submission,
        goal_sha256=task_meta["rca_prod_goal_sha256"],
        contract_sha256=task_meta["rca_prod_contract_sha256"],
        hmac_key=config.prod_admission_hmac_key,
    )

    assert projection["capacity_mode"] == "bootstrap"
    assert projection["bootstrap_epoch_id"] == "rca-bootstrap-canary-1"
    assert projection["release_bom_sha256"] == release_bom_sha256
    assert (
        projection["active_release_binding_sha256"]
        == active_release_binding_sha256
    )
    assert projection["max_concurrency"] == 1
    assert projection["daily_started_attempt_quota"] == 5
    assert projection["root_required_available_bytes"] == 464 * 1024**3
    assert projection["delivery_required_available_bytes"] == 640 * 1024**3


def test_collector_output_passes_public_release_gate_validator(tmp_path):
    config, reader, submission = _fixture(tmp_path)
    result = CanaryReceiptCollector(
        config, remote_reader=reader, now=lambda: NOW
    ).collect(SOURCE_ID)

    detail = _validate_collector_output_with_release_gate(result.receipt)

    assert detail["submission_key"] == submission
    assert (
        detail["execution_request"]["request_sha256"]
        == result.receipt["outbox"]["execution_request_sha256"]
    )
    assert detail["vm_execution"]["max_execution_duration_seconds"] == 3600


def test_manual_observer_canary_binds_kafka_origin_authorization_and_two_effects(
    tmp_path,
):
    config, reader, source_id = _manual_observer_fixture(tmp_path)

    result = CanaryReceiptCollector(
        config, remote_reader=reader, now=lambda: NOW
    ).collect(source_id)

    assert result.receipt["execution_origin"]["source_kind"] == "kafka_issue_created"
    assert result.receipt["observed_trigger_source"]["source_kind"] == (
        "manual_issue_request"
    )
    assert result.receipt["observed_trigger_source"]["binding_role"] == "observer"
    assert result.receipt["execution_request"]["source_refs"][
        "origin_source_id"
    ] == result.receipt["execution_origin"]["source_id"]
    assert {
        item["effect_kind"] for item in result.receipt["delivery_obligations"]
    } == {"feishu_issue_comment", "feishu_thread_reply"}
    thread = next(
        item
        for item in result.receipt["delivery_obligations"]
        if item["effect_kind"] == "feishu_thread_reply"
    )
    assert thread["target"]["reply_in_thread"] is True
    assert thread["target_key"].startswith("feishu_thread:")
    assert set(
        result.provenance["local_machine_sources"][
            "group_binding_authorizations"
        ]
    ) == {source_id}


def test_manual_observer_cannot_satisfy_independent_manual_success(tmp_path):
    config, reader, source_id = _manual_observer_fixture(tmp_path)

    with pytest.raises(CanaryCollectionError) as caught:
        CanaryReceiptCollector(
            config, remote_reader=reader, now=lambda: NOW
        ).collect_manual_success(source_id)

    assert caught.value.code == "manual_success_origin_required"


def test_terminal_manual_canary_is_db_only_and_passes_release_validator(tmp_path):
    config, reader, source_id = _terminal_manual_fixture(tmp_path)

    result = CanaryReceiptCollector(
        config, remote_reader=reader, now=lambda: NOW
    ).collect_terminal_failure(source_id)

    assert result.receipt["schema_version"] == (
        TERMINAL_CANARY_RECEIPT_SCHEMA_VERSION
    )
    assert result.receipt["outcome"] == "terminal_failed"
    with sqlite3.connect(config.control_db_path) as connection:
        [payload_json] = connection.execute(
            "SELECT payload_json FROM rca_outbox"
        ).fetchone()
    assert result.receipt["admission"] == json.loads(payload_json)["admission"]
    assert result.receipt["delivery_job"]["artifact_boundary"] == {
        "manifest": {},
        "contract": {},
        "artifacts": [],
        "issue_url": "",
        "report_url": "",
    }
    assert {
        item["effect_kind"] for item in result.receipt["delivery_obligations"]
    } == {"feishu_issue_comment", "feishu_thread_reply"}
    assert reader.calls == []
    detail = release_gate.validate_terminal_delivery_canary(
        result.receipt,
        expected_manual_chat_ids=config.manual_chat_ids,
        **_terminal_policy(config),
        now=NOW,
        max_age_seconds=900,
    )
    assert detail["outcome"] == "terminal_failed"
    for obligation in result.receipt["delivery_obligations"]:
        assert "error_detail" not in obligation["payload"]
        assert "report_url" not in obligation["payload"]


def test_terminal_manual_canary_rejects_legacy_authorization_v1(tmp_path):
    config, reader, source_id = _terminal_manual_fixture(tmp_path)
    receipt_path = next(config.group_binding_receipt_dir.glob("*.jsonl"))
    record = json.loads(receipt_path.read_text(encoding="utf-8"))
    authorization = record["manual_authorization"]
    authorization["schema_version"] = "pnc_rca_manual_authorization_v1"
    authorization.pop("manual_operator_rate_limit")
    authorization.pop("manual_operator_rate_window_seconds")
    receipt_path.write_text(
        json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(CanaryCollectionError) as caught:
        CanaryReceiptCollector(
            config,
            remote_reader=reader,
            now=lambda: NOW,
        ).collect_terminal_failure(source_id)

    assert caught.value.code == "manual_authorization_not_proven"


def test_terminal_manual_canary_requires_gateway_runtime_identity(tmp_path):
    config, reader, source_id = _terminal_manual_fixture(tmp_path)
    receipt_path = next(config.group_binding_receipt_dir.glob("*.jsonl"))
    record = json.loads(receipt_path.read_text(encoding="utf-8"))
    record.pop("gateway_runtime_identity")
    receipt_path.write_text(
        json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(CanaryCollectionError) as caught:
        CanaryReceiptCollector(
            config,
            remote_reader=reader,
            now=lambda: NOW,
        ).collect_terminal_failure(source_id)

    assert caught.value.code == "manual_gateway_runtime_identity_not_proven"


def test_release_validator_rejects_legacy_authorization_v1(tmp_path):
    config, reader, source_id = _terminal_manual_fixture(tmp_path)
    result = CanaryReceiptCollector(
        config,
        remote_reader=reader,
        now=lambda: NOW,
    ).collect_terminal_failure(source_id)
    legacy = copy.deepcopy(result.receipt)
    authorization = legacy["observed_trigger_source"]["authorization"][
        "manual_authorization"
    ]
    authorization["schema_version"] = "pnc_rca_manual_authorization_v1"
    authorization.pop("manual_operator_rate_limit")
    authorization.pop("manual_operator_rate_window_seconds")

    with pytest.raises(release_gate.EvidenceError) as caught:
        release_gate.validate_terminal_delivery_canary(
            legacy,
            expected_manual_chat_ids=config.manual_chat_ids,
            **_terminal_policy(config),
            now=NOW,
            max_age_seconds=900,
        )

    assert caught.value.code == "canary_trigger_authorization_snapshot_shape_invalid"


def test_terminal_canary_proves_pre_submit_quarantine_without_fake_task(tmp_path):
    config, reader, source_id = _quarantined_manual_fixture(tmp_path)
    observed = NOW + timedelta(seconds=5)

    result = CanaryReceiptCollector(
        config, remote_reader=reader, now=lambda: observed
    ).collect_terminal_failure(source_id)

    assert result.receipt["outcome"] == "quarantined"
    assert result.receipt["watch"]["task_id"] is None
    assert result.receipt["error_code"] == "outbox_submission_quarantined"
    assert "SECRET-MUST-NOT-LEAK" not in json.dumps(result.receipt, sort_keys=True)
    detail = release_gate.validate_terminal_delivery_canary(
        result.receipt,
        expected_manual_chat_ids=config.manual_chat_ids,
        **_terminal_policy(config),
        now=observed,
        max_age_seconds=900,
    )
    assert detail["outcome"] == "quarantined"
    assert reader.calls == []
    forged = copy.deepcopy(result.receipt)
    forged["watch"]["task_id"] = forged["submission_key"]
    with pytest.raises(release_gate.EvidenceError) as caught:
        release_gate.validate_terminal_delivery_canary(
            forged,
            expected_manual_chat_ids=config.manual_chat_ids,
            **_terminal_policy(config),
            now=observed,
            max_age_seconds=900,
        )
    assert caught.value.code == "manual_terminal_failure_watch_invalid"


def test_terminal_failure_canary_rejects_missing_vm_task_identity(tmp_path):
    config, reader, source_id = _terminal_manual_fixture(tmp_path)
    result = CanaryReceiptCollector(
        config, remote_reader=reader, now=lambda: NOW
    ).collect_terminal_failure(source_id)
    drifted = copy.deepcopy(result.receipt)
    drifted["watch"]["task_id"] = None

    with pytest.raises(release_gate.EvidenceError) as caught:
        release_gate.validate_terminal_delivery_canary(
            drifted,
            expected_manual_chat_ids=config.manual_chat_ids,
            **_terminal_policy(config),
            now=NOW,
            max_age_seconds=900,
        )

    assert caught.value.code == "manual_terminal_failure_watch_invalid"


def test_terminal_release_validator_rejects_non_monotonic_delivery_timeline(tmp_path):
    config, reader, source_id = _terminal_manual_fixture(tmp_path)
    result = CanaryReceiptCollector(
        config, remote_reader=reader, now=lambda: NOW
    ).collect_terminal_failure(source_id)
    drifted = copy.deepcopy(result.receipt)
    drifted["delivery_obligations"][0]["materialized_at"] = (
        "2026-07-10T07:59:50+00:00"
    )

    with pytest.raises(release_gate.EvidenceError) as caught:
        release_gate.validate_terminal_delivery_canary(
            drifted,
            expected_manual_chat_ids=config.manual_chat_ids,
            **_terminal_policy(config),
            now=NOW,
            max_age_seconds=900,
        )

    assert caught.value.code == "manual_terminal_failure_timeline_invalid"


def test_terminal_release_validator_rejects_fresh_receipt_for_stale_chain(tmp_path):
    config, reader, source_id = _terminal_manual_fixture(tmp_path)
    result = CanaryReceiptCollector(
        config, remote_reader=reader, now=lambda: NOW
    ).collect_terminal_failure(source_id)
    drifted = copy.deepcopy(result.receipt)
    later = NOW + timedelta(minutes=20)
    drifted["observed_at"] = later.isoformat()

    with pytest.raises(release_gate.EvidenceError) as caught:
        release_gate.validate_terminal_delivery_canary(
            drifted,
            expected_manual_chat_ids=config.manual_chat_ids,
            **_terminal_policy(config),
            now=later,
            max_age_seconds=900,
        )

    assert caught.value.code == "manual_terminal_failure_timeline_stale"


def test_terminal_manual_canary_requires_succeeded_topic_effect(tmp_path):
    config, reader, source_id = _terminal_manual_fixture(tmp_path)
    connection = sqlite3.connect(config.control_db_path)
    connection.execute(
        "UPDATE rca_delivery_effects SET status = 'pending' "
        "WHERE effect_kind = 'feishu_thread_reply'"
    )
    connection.commit()
    connection.close()

    with pytest.raises(CanaryCollectionError) as caught:
        CanaryReceiptCollector(
            config, remote_reader=reader, now=lambda: NOW
        ).collect_terminal_failure(source_id)

    assert caught.value.code == "terminal_delivery_effect_invalid"


def test_terminal_release_validator_rejects_main_group_fallback(tmp_path):
    config, reader, source_id = _terminal_manual_fixture(tmp_path)
    result = CanaryReceiptCollector(
        config, remote_reader=reader, now=lambda: NOW
    ).collect_terminal_failure(source_id)
    drifted = copy.deepcopy(result.receipt)
    thread = next(
        item
        for item in drifted["delivery_obligations"]
        if item["effect_kind"] == "feishu_thread_reply"
    )
    thread["target_key"] = f"feishu_group:{thread['target']['chat_id']}"

    with pytest.raises(release_gate.EvidenceError) as caught:
        release_gate.validate_terminal_delivery_canary(
            drifted,
            expected_manual_chat_ids=config.manual_chat_ids,
            **_terminal_policy(config),
            now=NOW,
            max_age_seconds=900,
        )

    assert caught.value.code in {
        "manual_terminal_failure_thread_contract_invalid",
        "manual_terminal_failure_topic_binding_invalid",
    }


def test_terminal_collection_writes_dedicated_evidence_files(tmp_path):
    config, reader, source_id = _terminal_manual_fixture(tmp_path)
    result = CanaryReceiptCollector(
        config, remote_reader=reader, now=lambda: NOW
    ).collect_terminal_failure(source_id)

    receipt_path, sources_path = write_collection(result, config.evidence_dir)

    commit = json.loads(
        (
            config.evidence_dir / "manual_terminal_failure_canary_commit.json"
        ).read_text()
    )
    assert receipt_path.name == commit["files"]["receipt"]["filename"]
    assert sources_path.name == commit["files"]["sources"]["filename"]
    assert commit["evidence_role"] == "manual_terminal_failure"
    assert json.loads(receipt_path.read_text())["schema_version"] == (
        TERMINAL_CANARY_RECEIPT_SCHEMA_VERSION
    )


def test_manual_observer_canary_rejects_missing_authorization_receipt(tmp_path):
    config, reader, source_id = _manual_observer_fixture(tmp_path)
    next(config.group_binding_receipt_dir.glob("*.jsonl")).unlink()

    with pytest.raises(CanaryCollectionError) as caught:
        CanaryReceiptCollector(config, remote_reader=reader).collect(source_id)

    assert caught.value.code == "manual_authorization_receipt_not_unique"


@pytest.mark.parametrize("appended", (b"{}\n", b"\n"))
def test_manual_authorization_append_after_collect_is_rejected(tmp_path, appended):
    config, _reader, source_id = _manual_observer_fixture(tmp_path)
    _authorization, first_source = _manual_authorization_evidence(config, source_id)
    receipt_path = next(config.group_binding_receipt_dir.glob("*.jsonl"))
    original_hash = first_source.raw_sha256

    with receipt_path.open("ab") as stream:
        stream.write(appended)

    assert collector_module._sha256_bytes(receipt_path.read_bytes()) != original_hash
    with pytest.raises(CanaryCollectionError) as caught:
        _manual_authorization_evidence(config, source_id)
    assert caught.value.code == "manual_authorization_receipt_invalid"


def test_manual_authorization_ignores_unrelated_large_jsonl(tmp_path):
    config, _reader, source_id = _manual_observer_fixture(tmp_path)
    unrelated = config.group_binding_receipt_dir / "unrelated-large.jsonl"
    with unrelated.open("wb") as stream:
        stream.truncate(collector_module.MAX_JSON_BYTES + 1)
    unrelated.chmod(0o600)

    _authorization, source = _manual_authorization_evidence(config, source_id)

    assert source.path != str(unrelated)
    assert source.size_bytes < collector_module.MAX_JSON_BYTES


def test_manual_authorization_locates_exact_previous_utc_day_candidate(tmp_path):
    config, _reader, source_id = _manual_observer_fixture(tmp_path)
    current = next(config.group_binding_receipt_dir.glob("*.jsonl"))
    record = json.loads(current.read_text(encoding="utf-8"))
    current.unlink()
    record["timestamp"] = "2026-07-09T23:59:59+00:00"
    previous = _write_group_binding_receipt(
        config.group_binding_receipt_dir, record
    )

    _authorization, source = _manual_authorization_evidence(config, source_id)

    assert source.path == str(previous)


@pytest.mark.parametrize(
    "attack",
    ("root_mode", "file_mode", "hardlink", "file_symlink"),
)
def test_manual_authorization_rejects_local_receipt_permission_and_link_attacks(
    tmp_path, attack
):
    config, _reader, source_id = _manual_observer_fixture(tmp_path)
    receipt_path = next(config.group_binding_receipt_dir.glob("*.jsonl"))
    if attack == "root_mode":
        config.group_binding_receipt_dir.chmod(0o755)
    elif attack == "file_mode":
        receipt_path.chmod(0o644)
    elif attack == "hardlink":
        os.link(receipt_path, config.group_binding_receipt_dir / "receipt-alias")
    else:
        outside = tmp_path / "outside-receipt.jsonl"
        receipt_path.replace(outside)
        receipt_path.symlink_to(outside)

    with pytest.raises(CanaryCollectionError) as caught:
        _manual_authorization_evidence(config, source_id)

    assert caught.value.code == "manual_authorization_receipt_invalid"


def test_manual_authorization_rejects_symlinked_receipt_root(tmp_path):
    config, _reader, source_id = _manual_observer_fixture(tmp_path)
    symlinked_root = tmp_path / "symlinked-group-binding-receipts"
    symlinked_root.symlink_to(config.group_binding_receipt_dir, target_is_directory=True)
    config = replace(config, group_binding_receipt_dir=symlinked_root)

    with pytest.raises(CanaryCollectionError) as caught:
        _manual_authorization_evidence(config, source_id)

    assert caught.value.code == "manual_authorization_receipt_invalid"


def test_manual_authorization_rejects_candidate_path_escape(
    monkeypatch, tmp_path
):
    config, _reader, source_id = _manual_observer_fixture(tmp_path)
    monkeypatch.setattr(
        collector_module,
        "pnc_group_binding_receipt_filename",
        lambda **_kwargs: "../outside.jsonl",
    )

    with pytest.raises(CanaryCollectionError) as caught:
        _manual_authorization_evidence(config, source_id)

    assert caught.value.code == "manual_authorization_receipt_invalid"


def test_manual_observer_canary_rejects_origin_overwrite(tmp_path):
    config, reader, source_id = _manual_observer_fixture(tmp_path)
    connection = sqlite3.connect(config.control_db_path)
    connection.execute("UPDATE rca_outbox SET origin_source_id = ?", (source_id,))
    connection.execute(
        "UPDATE business_triggers SET origin_source_id = ?", (source_id,)
    )
    connection.commit()
    connection.close()

    with pytest.raises(CanaryCollectionError) as caught:
        CanaryReceiptCollector(config, remote_reader=reader).collect(source_id)

    assert caught.value.code == "control_origin_binding_invalid"


def test_manual_observer_canary_rejects_missing_thread_delivery_binding(tmp_path):
    config, reader, source_id = _manual_observer_fixture(tmp_path)
    connection = sqlite3.connect(config.control_db_path)
    connection.execute(
        "DELETE FROM rca_trigger_delivery_bindings WHERE source_id = ? "
        "AND subscription_key IN (SELECT subscription_key "
        "FROM rca_delivery_subscriptions WHERE effect_kind = 'feishu_thread_reply')",
        (source_id,),
    )
    connection.commit()
    connection.close()

    with pytest.raises(CanaryCollectionError) as caught:
        CanaryReceiptCollector(config, remote_reader=reader).collect(source_id)

    assert caught.value.code == "control_delivery_subscriptions_incomplete"


def test_manual_observer_canary_rejects_main_group_thread_fallback(tmp_path):
    config, reader, source_id = _manual_observer_fixture(tmp_path)
    connection = sqlite3.connect(config.control_db_path)
    connection.execute(
        "UPDATE rca_delivery_subscriptions SET target_key = ? "
        "WHERE effect_kind = 'feishu_thread_reply'",
        ("feishu_group:oc_6cfc782212009ff4cd815349909dd423",),
    )
    connection.commit()
    connection.close()

    with pytest.raises(CanaryCollectionError) as caught:
        CanaryReceiptCollector(config, remote_reader=reader).collect(source_id)

    assert caught.value.code == "control_thread_target_invalid"


def test_public_release_gate_validator_rejects_collector_schema_drift(tmp_path):
    config, reader, _submission = _fixture(tmp_path)
    result = CanaryReceiptCollector(
        config, remote_reader=reader, now=lambda: NOW
    ).collect(SOURCE_ID)
    drifted = copy.deepcopy(result.receipt)
    drifted["execution_request"].pop("execution_policy")

    with pytest.raises(release_gate.EvidenceError) as caught:
        _validate_collector_output_with_release_gate(drifted)

    assert caught.value.code == "remote_request_shape_invalid"


def _rewrite_stage_lineage(reader, short_name, mutation):
    record_name = f"stage_{short_name}"
    lineage = copy.deepcopy(reader.records[record_name].body)
    mutation(lineage)
    lineage["input_artifact_set_sha256"] = canonical_artifact_set_sha256(
        lineage["input_artifacts"]
    )
    lineage["output_artifact_set_sha256"] = canonical_artifact_set_sha256(
        lineage["output_artifacts"]
    )
    replacement = _json_source(reader.records[record_name].path, lineage)
    reader.records[record_name] = replacement
    pipeline = copy.deepcopy(reader.records["pipeline"].body)
    pipeline["downstream_stage_receipts"][short_name]["artifact_receipt_sha256"] = (
        replacement.raw_sha256
    )
    reader.records["pipeline"] = _json_source(reader.records["pipeline"].path, pipeline)


@pytest.mark.parametrize(
    ("short_name", "mutation", "blocker"),
    [
        (
            "s3a",
            lambda lineage: lineage["input_artifacts"][0].update(sha256="0" * 64),
            "stage_lineage_upstream_artifact_missing",
        ),
        (
            "s3b",
            lambda lineage: lineage.update(
                input_artifacts=[
                    {
                        "kind": "unrelated_side_input",
                        "path": "/mnt/tmp/"
                        + gate_contract_fixture.SUBMISSION_KEY
                        + "/side.json",
                        "bytes": 1,
                        "sha256": "1" * 64,
                    }
                ]
            ),
            "stage_lineage_upstream_artifact_missing",
        ),
        (
            "s45",
            lambda lineage: lineage["identity"].update(
                run_id="pipeline-self-generated-run"
            ),
            "stage_lineage_identity_mismatch",
        ),
        (
            "s5",
            lambda lineage: lineage["execution_policy"].update(allow_download=True),
            "stage_lineage_execution_policy_invalid",
        ),
        (
            "s6",
            lambda lineage: lineage.update(
                output_artifacts=[
                    item
                    for item in lineage["output_artifacts"]
                    if item["kind"] != "delivery_manifest"
                ]
            ),
            "stage_lineage_final_output_missing",
        ),
    ],
)
def test_collector_rejects_broken_stage_lineage(
    tmp_path, short_name, mutation, blocker
):
    config, reader, _submission = _fixture(tmp_path)
    _rewrite_stage_lineage(reader, short_name, mutation)

    with pytest.raises(CanaryCollectionError) as caught:
        CanaryReceiptCollector(config, remote_reader=reader, now=lambda: NOW).collect(
            SOURCE_ID
        )

    assert caught.value.code == blocker


@pytest.mark.parametrize(
    "legacy_mode", ["minimal_download", "full_download", "mdi_download"]
)
def test_collector_rejects_legacy_mode_in_optional_worker_evidence(
    tmp_path, legacy_mode
):
    config, reader, _submission = _fixture(tmp_path)
    worker = copy.deepcopy(reader.records["worker_result"].body)
    worker["result"]["resolved_snapshot"] = {"legacy_access_hint": legacy_mode}
    reader.records["worker_result"] = _json_source(
        reader.records["worker_result"].path, worker
    )

    with pytest.raises(CanaryCollectionError) as caught:
        CanaryReceiptCollector(config, remote_reader=reader, now=lambda: NOW).collect(
            SOURCE_ID
        )

    assert caught.value.code == "canary_receipt_gate_incompatible"


def test_rejects_downstream_path_escape_before_second_remote_read(tmp_path):
    config, reader, _submission = _fixture(tmp_path)
    pipeline = copy.deepcopy(reader.records["pipeline"].body)
    pipeline["downstream_stage_receipts"]["s3a"]["artifact_receipt_path"] = (
        "/mnt/tmp/escape/../other.json"
    )
    reader.records["pipeline"] = _json_source(reader.records["pipeline"].path, pipeline)

    with pytest.raises(CanaryCollectionError) as caught:
        CanaryReceiptCollector(config, remote_reader=reader).collect(SOURCE_ID)

    assert caught.value.code == "downstream_receipt_path_invalid"
    assert len(reader.calls) == 1


def test_rejects_duplicate_event_submission_records(tmp_path):
    config, reader, _submission = _fixture(tmp_path, duplicate=True)

    with pytest.raises(CanaryCollectionError) as caught:
        CanaryReceiptCollector(config, remote_reader=reader).collect(SOURCE_ID)

    assert caught.value.code == "control_generation_outbox_not_unique"
    assert reader.calls == []


def test_rejects_event_without_successful_shadow_promotion(tmp_path):
    config, reader, _submission = _fixture(tmp_path, promoted=False)

    with pytest.raises(CanaryCollectionError) as caught:
        CanaryReceiptCollector(config, remote_reader=reader).collect(SOURCE_ID)

    assert caught.value.code == "control_shadow_promotion_unproven"
    assert reader.calls == []


def test_accepts_direct_bounded_activation_without_shadow_promotion(tmp_path):
    config, reader, submission = _fixture(
        tmp_path,
        promoted=False,
        direct_activation=True,
    )

    result = CanaryReceiptCollector(
        config, remote_reader=reader, now=lambda: NOW
    ).collect(SOURCE_ID)

    assert result.receipt["submission_key"] == submission
    assert result.receipt["ok"] is True


def test_rejects_legacy_event_only_direct_bounded_identity(tmp_path):
    config, reader, _submission = _fixture(
        tmp_path,
        promoted=False,
        direct_activation=True,
    )
    legacy_identity_sha256 = _sha_json({"event_uid": EVENT_UID})
    with sqlite3.connect(config.control_db_path) as connection:
        connection.execute(
            "UPDATE rca_activation_admission_ledger "
            "SET source_identity_sha256 = ?",
            (legacy_identity_sha256,),
        )
        connection.execute(
            "UPDATE rca_activation_budget_slots "
            "SET authorized_identity_sha256 = ?",
            (legacy_identity_sha256,),
        )
        connection.execute(
            "UPDATE kafka_inbox "
            "SET activation_source_identity_sha256 = ?",
            (legacy_identity_sha256,),
        )

    with pytest.raises(CanaryCollectionError) as caught:
        CanaryReceiptCollector(
            config, remote_reader=reader, now=lambda: NOW
        ).collect(SOURCE_ID)

    assert caught.value.code == "control_activation_admission_unproven"
    assert reader.calls == []


def test_rejects_tampered_direct_bounded_activation(tmp_path):
    config, reader, _submission = _fixture(
        tmp_path,
        promoted=False,
        direct_activation=True,
    )
    with sqlite3.connect(config.control_db_path) as connection:
        connection.execute(
            "UPDATE rca_activation_admission_ledger "
            "SET source_identity_sha256 = ?",
            ("0" * 64,),
        )

    with pytest.raises(CanaryCollectionError) as caught:
        CanaryReceiptCollector(config, remote_reader=reader).collect(SOURCE_ID)

    assert caught.value.code == "control_activation_admission_unproven"
    assert reader.calls == []


def test_rejects_tampered_remote_receipt_reference(tmp_path):
    config, reader, _submission = _fixture(tmp_path)
    pipeline = copy.deepcopy(reader.records["pipeline"].body)
    pipeline["remote_read_receipt"]["sha256"] = "0" * 64
    reader.records["pipeline"] = _json_source(reader.records["pipeline"].path, pipeline)

    with pytest.raises(CanaryCollectionError) as caught:
        CanaryReceiptCollector(config, remote_reader=reader).collect(SOURCE_ID)

    assert caught.value.code == "pipeline_receipt_hash_binding_invalid"


def test_rejects_delivery_receipt_without_confirmed_result_fields(tmp_path):
    config, reader, _submission = _fixture(tmp_path)
    with sqlite3.connect(config.delivery_db_path) as connection:
        row = connection.execute(
            "SELECT effect_key, remote_receipt_json FROM rca_delivery_effects "
            "WHERE effect_kind='feishu_issue_comment'"
        ).fetchone()
        receipt = json.loads(row[1])
        receipt.pop("confirmed_field_keys")
        connection.execute(
            "UPDATE rca_delivery_effects SET remote_receipt_json=? WHERE effect_key=?",
            (json.dumps(receipt), row[0]),
        )

    with pytest.raises(CanaryCollectionError) as caught:
        CanaryReceiptCollector(config, remote_reader=reader).collect(SOURCE_ID)

    assert caught.value.code == "delivery_result_fields_not_confirmed"


def test_rejects_v1_capacity_meter_receipt(tmp_path):
    config, reader, _submission = _fixture(tmp_path)
    meter = copy.deepcopy(reader.records["capacity_meter"].body)
    meter["schema_version"] = "g1q3_rca_stage_capacity_meter_v1"
    meter_record = _json_source(reader.records["capacity_meter"].path, meter)
    reader.records["capacity_meter"] = meter_record
    pipeline = copy.deepcopy(reader.records["pipeline"].body)
    pipeline["capacity_usage"]["sha256"] = meter_record.raw_sha256
    reader.records["pipeline"] = _json_source(
        reader.records["pipeline"].path, pipeline
    )

    with pytest.raises(CanaryCollectionError) as caught:
        CanaryReceiptCollector(config, remote_reader=reader).collect(SOURCE_ID)

    assert caught.value.code == "pipeline_receipt_hash_binding_invalid"


def test_rejects_capacity_meter_root_topology_drift(tmp_path):
    config, reader, _submission = _fixture(tmp_path)
    meter = copy.deepcopy(reader.records["capacity_meter"].body)
    meter["accounting"]["hfs_root"] = "/mnt/tmp/other-task/cases/G1Q3-1"
    meter_record = _json_source(reader.records["capacity_meter"].path, meter)
    reader.records["capacity_meter"] = meter_record
    pipeline = copy.deepcopy(reader.records["pipeline"].body)
    pipeline["capacity_usage"]["sha256"] = meter_record.raw_sha256
    reader.records["pipeline"] = _json_source(
        reader.records["pipeline"].path, pipeline
    )

    with pytest.raises(CanaryCollectionError) as caught:
        CanaryReceiptCollector(config, remote_reader=reader).collect(SOURCE_ID)

    assert caught.value.code == "capacity_meter_accounting_invalid"


def test_rejects_cross_source_cifs_mount_identity_drift(tmp_path):
    config, reader, _submission = _fixture(tmp_path)
    service = copy.deepcopy(reader.records["service_result"].body)
    service["mount_evidence"]["device_id"] = 2
    reader.records["service_result"] = _json_source(
        reader.records["service_result"].path, service
    )

    with pytest.raises(CanaryCollectionError) as caught:
        CanaryReceiptCollector(config, remote_reader=reader).collect(SOURCE_ID)

    assert caught.value.code == "cifs_mount_evidence_binding_mismatch"


def _write_result(
    value: str,
    *,
    evidence_role: str = "primary",
    terminal: bool = False,
) -> CollectionResult:
    receipt = {
        "schema_version": (
            TERMINAL_CANARY_RECEIPT_SCHEMA_VERSION
            if terminal
            else CANARY_RECEIPT_SCHEMA_VERSION
        ),
        "value": value,
    }
    return CollectionResult(
        receipt=receipt,
        provenance={
            "schema_version": (
                "pnc_rca_terminal_delivery_canary_source_v1"
                if terminal
                else "pnc_rca_canary_source_provenance_v1"
            ),
            "receipt_sha256": _sha_json(receipt),
            "value": value,
        },
        evidence_role=evidence_role,
    )


def _assert_manifest_pair_complete(directory: Path, manifest_name: str) -> dict:
    manifest_path = directory / manifest_name
    manifest = json.loads(manifest_path.read_text())
    assert set(manifest) == {
        "schema_version",
        "evidence_role",
        "commit_id",
        "published_at",
        "receipt_canonical_sha256",
        "files",
    }
    for kind in ("receipt", "sources"):
        projection = manifest["files"][kind]
        path = directory / projection["filename"]
        raw = path.read_bytes()
        assert len(raw) == projection["size_bytes"]
        assert _sha(raw) == projection["raw_sha256"]
        assert path.stat().st_mode & 0o777 == 0o600
    receipt = json.loads(
        (directory / manifest["files"]["receipt"]["filename"]).read_text()
    )
    sources = json.loads(
        (directory / manifest["files"]["sources"]["filename"]).read_text()
    )
    assert _sha_json(receipt) == manifest["receipt_canonical_sha256"]
    assert sources["receipt_sha256"] == manifest["receipt_canonical_sha256"]
    return manifest


def test_write_publishes_content_addressed_private_commit(tmp_path):
    directory = tmp_path / "evidence"
    result = _write_result("kafka")

    receipt_path, sources_path = write_collection(result, directory)

    manifest = _assert_manifest_pair_complete(
        directory, "canary_receipt_commit.json"
    )
    assert manifest["schema_version"] == (
        collector_module.CANARY_EVIDENCE_COMMIT_SCHEMA_VERSION
    )
    assert manifest["evidence_role"] == "primary"
    assert receipt_path.name == manifest["files"]["receipt"]["filename"]
    assert sources_path.name == manifest["files"]["sources"]["filename"]
    commit_material = {
        "schema_version": manifest["schema_version"],
        "evidence_role": manifest["evidence_role"],
        "receipt_canonical_sha256": manifest["receipt_canonical_sha256"],
        "files": {
            kind: {
                key: manifest["files"][kind][key]
                for key in ("schema_version", "size_bytes", "raw_sha256")
            }
            for kind in ("receipt", "sources")
        },
    }
    assert _sha_json(commit_material) == manifest["commit_id"]
    assert directory.stat().st_mode & 0o777 == 0o700
    assert (directory / "canary_receipt_commit.json").stat().st_mode & 0o777 == 0o600
    assert not list(directory.glob(".*.tmp"))


def test_cli_reports_generation_commit_and_manifest(tmp_path, monkeypatch, capsys):
    result = _write_result("cli")
    result.provenance["submission_key_sha256"] = "a" * 64

    class FakeCollector:
        def __init__(self, _config):
            pass

        def collect(self, source_id):
            assert source_id == "source-1"
            return result

    monkeypatch.setattr(
        collector_module,
        "_collector_config_from_args",
        lambda _args: type("Config", (), {"evidence_dir": tmp_path})(),
    )
    monkeypatch.setattr(collector_module, "CanaryReceiptCollector", FakeCollector)

    assert collector_module.main(["--source-id", "source-1", "--write"]) == 0

    summary = json.loads(capsys.readouterr().out)
    manifest = json.loads(
        (tmp_path / "canary_receipt_commit.json").read_text(encoding="utf-8")
    )
    assert summary["evidence_role"] == "primary"
    assert summary["evidence_commit_id"] == manifest["commit_id"]
    assert summary["evidence_manifest"] == "canary_receipt_commit.json"
    assert summary["written_files"] == [
        manifest["files"]["receipt"]["filename"],
        manifest["files"]["sources"]["filename"],
        "canary_receipt_commit.json",
    ]


def test_three_evidence_roles_publish_isolated_commit_manifests(tmp_path):
    directory = tmp_path / "evidence"
    primary = _write_result("kafka")
    manual = _write_result("manual", evidence_role="manual_success")
    terminal = _write_result("terminal", terminal=True)

    primary_paths = write_collection(primary, directory)
    manual_paths = write_collection(manual, directory)
    terminal_paths = write_collection(terminal, directory)

    manifests = {
        "primary": _assert_manifest_pair_complete(
            directory, "canary_receipt_commit.json"
        ),
        "manual_success": _assert_manifest_pair_complete(
            directory, "manual_success_canary_commit.json"
        ),
        "manual_terminal_failure": _assert_manifest_pair_complete(
            directory, "manual_terminal_failure_canary_commit.json"
        ),
    }
    assert {body["evidence_role"] for body in manifests.values()} == set(manifests)
    assert [path.name for path in primary_paths] == [
        manifests["primary"]["files"][kind]["filename"]
        for kind in ("receipt", "sources")
    ]
    assert [path.name for path in manual_paths] == [
        manifests["manual_success"]["files"][kind]["filename"]
        for kind in ("receipt", "sources")
    ]
    assert [path.name for path in terminal_paths] == [
        manifests["manual_terminal_failure"]["files"][kind]["filename"]
        for kind in ("receipt", "sources")
    ]
    assert json.loads(primary_paths[0].read_text())["value"] == "kafka"
    assert json.loads(manual_paths[0].read_text())["value"] == "manual"
    assert json.loads(terminal_paths[0].read_text())["value"] == "terminal"


def test_second_generation_failure_preserves_old_committed_pair(
    tmp_path, monkeypatch
):
    directory = tmp_path / "evidence"
    write_collection(_write_result("old"), directory)
    manifest_path = directory / "canary_receipt_commit.json"
    old_manifest_raw = manifest_path.read_bytes()
    old_manifest = _assert_manifest_pair_complete(directory, manifest_path.name)
    original = collector_module._publish_immutable_generation
    calls = 0

    def fail_second(directory_fd, filename, raw):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("synthetic second generation failure")
        return original(directory_fd, filename, raw)

    monkeypatch.setattr(
        collector_module, "_publish_immutable_generation", fail_second
    )
    with pytest.raises(CanaryCollectionError) as caught:
        write_collection(_write_result("new"), directory)

    assert caught.value.code == "canary_evidence_write_failed"
    assert manifest_path.read_bytes() == old_manifest_raw
    assert _assert_manifest_pair_complete(directory, manifest_path.name) == old_manifest
    assert not list(directory.glob(".*.tmp"))


def test_manifest_replace_failure_preserves_old_committed_pair(
    tmp_path, monkeypatch
):
    directory = tmp_path / "evidence"
    write_collection(_write_result("old"), directory)
    manifest_path = directory / "canary_receipt_commit.json"
    old_manifest_raw = manifest_path.read_bytes()
    old_manifest = _assert_manifest_pair_complete(directory, manifest_path.name)

    def fail_replace(*_args, **_kwargs):
        raise OSError("synthetic manifest replace failure")

    monkeypatch.setattr(collector_module.os, "replace", fail_replace)
    with pytest.raises(CanaryCollectionError) as caught:
        write_collection(_write_result("new"), directory)

    assert caught.value.code == "canary_evidence_write_failed"
    assert manifest_path.read_bytes() == old_manifest_raw
    assert _assert_manifest_pair_complete(directory, manifest_path.name) == old_manifest
    assert not list(directory.glob(".*.tmp"))


def test_same_commit_retry_is_idempotent(tmp_path):
    directory = tmp_path / "evidence"
    result = _write_result("same")
    first_paths = write_collection(result, directory)
    manifest_path = directory / "canary_receipt_commit.json"
    first_manifest = manifest_path.read_bytes()
    first_inodes = [path.stat().st_ino for path in first_paths]

    second_paths = write_collection(result, directory)

    assert second_paths == first_paths
    assert manifest_path.read_bytes() == first_manifest
    assert [path.stat().st_ino for path in second_paths] == first_inodes
    assert len(list(directory.glob("canary_receipt.*.json"))) == 1
    assert len(list(directory.glob("canary_receipt_sources.*.json"))) == 1
    assert not list(directory.glob(".*.tmp"))


def test_concurrent_different_commits_allow_only_lock_owner_to_publish(
    tmp_path, monkeypatch
):
    directory = tmp_path / "evidence"
    first = _write_result("first")
    second = _write_result("second")
    first_publication = collector_module._evidence_publication(first)
    entered = Event()
    release = Event()
    first_results = []
    first_errors = []
    original_publish = collector_module._publish_immutable_generation

    def block_first_generation(directory_fd, filename, raw):
        if (
            first_publication.commit_id in filename
            and filename.startswith(first_publication.receipt_stem + ".")
            and not entered.is_set()
        ):
            entered.set()
            assert release.wait(5)
        return original_publish(directory_fd, filename, raw)

    def publish_first():
        try:
            first_results.append(write_collection(first, directory))
        except Exception as exc:  # pragma: no cover - asserted below
            first_errors.append(exc)

    monkeypatch.setattr(
        collector_module,
        "_publish_immutable_generation",
        block_first_generation,
    )
    thread = Thread(target=publish_first)
    thread.start()
    assert entered.wait(5)
    try:
        with pytest.raises(CanaryCollectionError) as caught:
            write_collection(second, directory)
        assert caught.value.code == "canary_evidence_publish_busy"
    finally:
        release.set()
        thread.join(5)

    assert not thread.is_alive()
    assert not first_errors
    assert len(first_results) == 1
    manifest = _assert_manifest_pair_complete(
        directory, "canary_receipt_commit.json"
    )
    assert manifest["commit_id"] == first_publication.commit_id
    assert not (directory / first_publication.lock_filename).exists()


def test_concurrent_same_commit_returns_one_idempotent_pair(tmp_path, monkeypatch):
    directory = tmp_path / "evidence"
    result = _write_result("same-concurrent")
    publication = collector_module._evidence_publication(result)
    entered = Event()
    contended = Event()
    release = Event()
    results = []
    errors = []
    original_publish = collector_module._publish_immutable_generation
    original_load_lock = collector_module._load_evidence_publish_lock

    def block_first_generation(directory_fd, filename, raw):
        if (
            filename.startswith(publication.receipt_stem + ".")
            and not entered.is_set()
        ):
            entered.set()
            assert release.wait(5)
        return original_publish(directory_fd, filename, raw)

    def observe_contention(directory_fd, observed_publication):
        body = original_load_lock(directory_fd, observed_publication)
        if body is not None:
            contended.set()
        return body

    def publish():
        try:
            results.append(write_collection(result, directory))
        except Exception as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    monkeypatch.setattr(
        collector_module,
        "_publish_immutable_generation",
        block_first_generation,
    )
    monkeypatch.setattr(
        collector_module,
        "_load_evidence_publish_lock",
        observe_contention,
    )
    first_thread = Thread(target=publish)
    second_thread = Thread(target=publish)
    first_thread.start()
    assert entered.wait(5)
    second_thread.start()
    assert contended.wait(5)
    release.set()
    first_thread.join(5)
    second_thread.join(5)

    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert not errors
    assert len(results) == 2
    assert results[0] == results[1]
    manifest = _assert_manifest_pair_complete(
        directory, "canary_receipt_commit.json"
    )
    assert manifest["commit_id"] == publication.commit_id
    assert not (directory / publication.lock_filename).exists()


def test_same_commit_waits_for_lock_record_to_finish_writing(tmp_path, monkeypatch):
    directory = tmp_path / "evidence"
    result = _write_result("partial-lock")
    publication = collector_module._evidence_publication(result)
    entered = Event()
    contended = Event()
    release = Event()
    results = []
    errors = []
    original_write_all = collector_module._write_all
    original_load_lock = collector_module._load_evidence_publish_lock

    def block_lock_record(descriptor, raw):
        if (
            collector_module.CANARY_EVIDENCE_PUBLISH_LOCK_SCHEMA_VERSION.encode()
            in raw
            and not entered.is_set()
        ):
            entered.set()
            assert release.wait(5)
        return original_write_all(descriptor, raw)

    def observe_contention(directory_fd, observed_publication):
        contended.set()
        return original_load_lock(directory_fd, observed_publication)

    def publish():
        try:
            results.append(write_collection(result, directory))
        except Exception as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    monkeypatch.setattr(collector_module, "_write_all", block_lock_record)
    monkeypatch.setattr(
        collector_module,
        "_load_evidence_publish_lock",
        observe_contention,
    )
    first_thread = Thread(target=publish)
    second_thread = Thread(target=publish)
    first_thread.start()
    assert entered.wait(5)
    second_thread.start()
    assert contended.wait(5)
    release.set()
    first_thread.join(5)
    second_thread.join(5)

    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert not errors
    assert len(results) == 2
    assert results[0] == results[1]
    assert _assert_manifest_pair_complete(
        directory, "canary_receipt_commit.json"
    )["commit_id"] == publication.commit_id
    assert not (directory / publication.lock_filename).exists()


def test_stale_publish_lock_fails_closed_without_removal(tmp_path):
    directory = tmp_path / "evidence"
    directory.mkdir(mode=0o700)
    result = _write_result("value")
    publication = collector_module._evidence_publication(result)
    lock_path = directory / publication.lock_filename
    lock_path.write_text(
        collector_module._canonical_json(
            {
                "schema_version": (
                    collector_module.CANARY_EVIDENCE_PUBLISH_LOCK_SCHEMA_VERSION
                ),
                "evidence_role": publication.evidence_role,
                "commit_id": publication.commit_id,
                "created_at": (
                    datetime.now(timezone.utc) - timedelta(minutes=5)
                ).isoformat(),
                "pid": os.getpid(),
            }
        )
        + "\n"
    )
    lock_path.chmod(0o600)

    with pytest.raises(CanaryCollectionError) as caught:
        write_collection(result, directory)

    assert caught.value.code == "canary_evidence_publish_lock_stale"
    assert lock_path.is_file()


@pytest.mark.parametrize("flag_name", ["O_NOFOLLOW", "O_DIRECTORY"])
def test_write_fails_closed_when_secure_open_flag_is_unavailable(
    tmp_path, monkeypatch, flag_name
):
    monkeypatch.delattr(collector_module.os, flag_name)

    with pytest.raises(CanaryCollectionError) as caught:
        write_collection(_write_result("value"), tmp_path / "evidence")

    assert caught.value.code == "canary_evidence_secure_open_unavailable"


def test_generation_same_name_conflict_fails_closed(tmp_path):
    directory = tmp_path / "evidence"
    directory.mkdir(mode=0o700)
    publication = collector_module._evidence_publication(_write_result("value"))
    conflict = directory / publication.receipt_filename
    conflict.write_bytes(b"conflict\n")
    conflict.chmod(0o600)

    with pytest.raises(CanaryCollectionError) as caught:
        write_collection(_write_result("value"), directory)

    assert caught.value.code == "canary_evidence_generation_conflict"
    assert conflict.read_bytes() == b"conflict\n"
    assert not (directory / publication.manifest_filename).exists()


def test_generation_symlink_fails_closed(tmp_path):
    directory = tmp_path / "evidence"
    directory.mkdir(mode=0o700)
    publication = collector_module._evidence_publication(_write_result("value"))
    outside = tmp_path / "outside.json"
    outside.write_text("outside\n")
    outside.chmod(0o600)
    (directory / publication.receipt_filename).symlink_to(outside)

    with pytest.raises(CanaryCollectionError) as caught:
        write_collection(_write_result("value"), directory)

    assert caught.value.code == "canary_evidence_target_invalid"
    assert outside.read_text() == "outside\n"
    assert not (directory / publication.manifest_filename).exists()


@pytest.mark.parametrize("metadata_fault", ["permissions", "nlink", "owner"])
def test_generation_metadata_faults_fail_closed(
    tmp_path, monkeypatch, metadata_fault
):
    directory = tmp_path / "evidence"
    directory.mkdir(mode=0o700)
    result = _write_result("value")
    publication = collector_module._evidence_publication(result)
    generation = directory / publication.receipt_filename
    generation.write_bytes(publication.receipt_raw)
    generation.chmod(0o600)
    if metadata_fault == "permissions":
        generation.chmod(0o644)
    elif metadata_fault == "nlink":
        os.link(generation, directory / "extra-link.json")
    else:
        real_uid = os.getuid()
        monkeypatch.setattr(collector_module.os, "getuid", lambda: real_uid + 1)

    with pytest.raises(CanaryCollectionError) as caught:
        write_collection(result, directory)

    assert caught.value.code in {
        "canary_evidence_target_invalid",
        "evidence_directory_invalid",
    }


def test_write_rejects_unbound_provenance(tmp_path):
    result = _write_result("value")
    result.provenance["receipt_sha256"] = "0" * 64

    with pytest.raises(CanaryCollectionError) as caught:
        write_collection(result, tmp_path / "evidence")

    assert caught.value.code == "canary_evidence_receipt_binding_invalid"


def test_public_database_facts_helper_returns_bounded_success_projection(tmp_path):
    config, _reader, submission = _fixture(tmp_path)

    facts = collector_module.read_local_canary_database_facts(config, SOURCE_ID)

    assert set(facts) == {
        "control_snapshot_sha256",
        "delivery_snapshot_sha256",
        "admission",
        "workflow_policy",
        "submission_key",
        "business_key",
        "generation",
        "outbox_id",
        "execution_origin",
        "observed_trigger_source",
        "host_runtime_transitions",
        "report",
        "delivery",
        "delivery_obligations",
    }
    assert facts["submission_key"] == submission
    assert len(facts["host_runtime_transitions"]) == 4
    assert facts["delivery"]["remote_receipt"] == {
        "remote_id": "feishu-comment-1",
        "confirmed_field_keys": ["field_9193cb", "field_8c912e"],
    }
    assert len(facts["control_snapshot_sha256"]) == 64
    assert len(facts["delivery_snapshot_sha256"]) == 64
    json.dumps(facts, allow_nan=False)


def test_success_database_facts_rejects_missing_runtime_transition(tmp_path):
    config, _reader, _submission = _fixture(tmp_path)
    with sqlite3.connect(config.control_db_path) as connection:
        connection.execute(
            "DELETE FROM rca_host_runtime_transitions "
            "WHERE service_label='local.pnc.rca-delivery-collector'"
        )

    with pytest.raises(CanaryCollectionError) as caught:
        collector_module.read_local_canary_database_facts(config, SOURCE_ID)

    assert caught.value.code == "host_runtime_transition_chain_incomplete"


def test_success_database_facts_rejects_runtime_transition_timeline_drift(tmp_path):
    config, _reader, _submission = _fixture(tmp_path)
    with sqlite3.connect(config.control_db_path) as connection:
        connection.execute(
            "UPDATE rca_host_runtime_transitions "
            "SET transitioned_at='2026-07-10T07:59:41+00:00' "
            "WHERE service_label='local.pnc.rca-delivery-dispatcher'"
        )

    with pytest.raises(CanaryCollectionError) as caught:
        collector_module.read_local_canary_database_facts(config, SOURCE_ID)

    assert caught.value.code == "host_runtime_transition_delivery_binding_invalid"


def test_public_database_facts_helper_returns_terminal_projection(tmp_path):
    config, _reader, source_id = _quarantined_manual_fixture(tmp_path)

    facts = collector_module.read_local_canary_database_facts(
        config,
        source_id,
        terminal_failure=True,
    )

    assert set(facts) == {
        "control_snapshot_sha256",
        "delivery_snapshot_sha256",
        "admission",
        "workflow_policy",
        "submission_key",
        "business_key",
        "generation",
        "outbox_id",
        "execution_origin",
        "observed_trigger_source",
        "host_runtime_transitions",
        "watch",
        "delivery_job",
        "delivery_obligations",
    }
    assert facts["delivery_job"]["status"] == "delivered"
    assert len(facts["delivery_obligations"]) == 2
    json.dumps(facts, allow_nan=False)


def test_cli_defaults_to_dry_run_and_accepts_only_durable_source_identity():
    parser = collector_module.build_arg_parser()
    args = parser.parse_args(["--source-id", SOURCE_ID])

    assert args.write is False
    assert args.dry_run is False
    assert args.source_id == SOURCE_ID
    assert args.env_file is None
    assert args.control_db is None
    assert args.delivery_db is None
    assert args.evidence_dir is None
    assert args.group_binding_receipt_dir is None
    assert args.manual_chat_ids is None
    assert not any(
        action.dest in {"receipt", "payload", "source_pack"}
        for action in parser._actions
    )


def test_env_file_is_literal_owner_only_and_drives_collector_defaults(tmp_path):
    home = tmp_path / "hermes-home"
    env_control = tmp_path / "env-control.sqlite3"
    env_delivery = tmp_path / "env-delivery.sqlite3"
    env_evidence = tmp_path / "env-evidence"
    env_receipts = tmp_path / "env-receipts"
    manual_chat_id = sorted(collector_module.FIXED_MANUAL_CHAT_IDS)[0]
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            (
                f"HERMES_HOME={home}",
                f"HERMES_RCA_KAFKA_CONTROL_DB_PATH={env_control}",
                f"HERMES_RCA_OUTBOX_CONTROL_DB_PATH={env_control}",
                f"HERMES_RCA_OUTBOX_DELIVERY_DB_PATH={env_delivery}",
                f"HERMES_RCA_CANARY_EVIDENCE_DIR={env_evidence}",
                "LITERAL_DOLLAR=${HOME}/must-not-expand",
                (
                    "HERMES_RCA_CANARY_GROUP_BINDING_RECEIPT_DIR="
                    f"{env_receipts}"
                ),
                f"HERMES_RCA_MANUAL_CHAT_IDS={manual_chat_id}",
                "HERMES_RCA_PROD_ADMISSION_HMAC_KEY=hex:" + "ab" * 32,
            )
        )
        + "\n",
        encoding="utf-8",
    )
    env_file.chmod(0o600)

    source = collector_module.load_canary_collector_environment(env_file)
    args = collector_module.build_arg_parser().parse_args(
        ["--source-id", SOURCE_ID, "--env-file", str(env_file)]
    )
    config = collector_module._collector_config_from_args(args)

    assert source["LITERAL_DOLLAR"] == "${HOME}/must-not-expand"
    assert config.control_db_path == env_control
    assert config.delivery_db_path == env_delivery
    assert config.evidence_dir == env_evidence
    assert config.group_binding_receipt_dir == env_receipts
    assert config.manual_chat_ids == (manual_chat_id,)
    assert config.prod_admission_hmac_key == "hex:" + "ab" * 32
    assert "ab" * 32 not in repr(config)


def test_explicit_cli_values_override_env_file(tmp_path):
    env_control = tmp_path / "env-control.sqlite3"
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            (
                f"HERMES_RCA_KAFKA_CONTROL_DB_PATH={env_control}",
                f"HERMES_RCA_OUTBOX_CONTROL_DB_PATH={env_control}",
                f"HERMES_RCA_CANARY_EVIDENCE_DIR={tmp_path / 'env-evidence'}",
                (
                    "HERMES_RCA_CANARY_GROUP_BINDING_RECEIPT_DIR="
                    f"{tmp_path / 'env-receipts'}"
                ),
                (
                    "HERMES_RCA_MANUAL_CHAT_IDS="
                    f"{sorted(collector_module.FIXED_MANUAL_CHAT_IDS)[0]}"
                ),
            )
        )
        + "\n",
        encoding="utf-8",
    )
    env_file.chmod(0o600)
    cli_chat_id = sorted(collector_module.FIXED_MANUAL_CHAT_IDS)[1]
    cli_control = tmp_path / "cli-control.sqlite3"
    cli_delivery = tmp_path / "cli-delivery.sqlite3"
    cli_evidence = tmp_path / "cli-evidence"
    cli_receipts = tmp_path / "cli-receipts"
    args = collector_module.build_arg_parser().parse_args(
        [
            "--source-id",
            SOURCE_ID,
            "--env-file",
            str(env_file),
            "--control-db",
            str(cli_control),
            "--delivery-db",
            str(cli_delivery),
            "--evidence-dir",
            str(cli_evidence),
            "--group-binding-receipt-dir",
            str(cli_receipts),
            "--manual-chat-ids",
            cli_chat_id,
        ]
    )

    config = collector_module._collector_config_from_args(args)

    assert config.control_db_path == cli_control
    assert config.delivery_db_path == cli_delivery
    assert config.evidence_dir == cli_evidence
    assert config.group_binding_receipt_dir == cli_receipts
    assert config.manual_chat_ids == (cli_chat_id,)


def test_env_file_hermes_home_anchors_missing_path_defaults(tmp_path):
    home = tmp_path / "alternate-hermes-home"
    env_file = tmp_path / ".env"
    env_file.write_text(f"HERMES_HOME={home}\n", encoding="utf-8")
    env_file.chmod(0o600)
    args = collector_module.build_arg_parser().parse_args(
        ["--source-id", SOURCE_ID, "--env-file", str(env_file)]
    )

    config = collector_module._collector_config_from_args(args)

    runtime_root = home / "runtime" / "pnc_agent" / "feishu_issue_kafka_rca"
    assert config.control_db_path == runtime_root / "control.sqlite3"
    assert config.delivery_db_path == config.control_db_path
    assert config.evidence_dir == runtime_root / "release_evidence"
    assert config.group_binding_receipt_dir == (
        home / "pnc_agent" / "receipts" / "g1q3_rca"
    )
    assert config.manual_chat_ids == ()


def test_env_file_rejects_symlink_insecure_mode_and_live_mutation(
    tmp_path, monkeypatch
):
    target = tmp_path / "target.env"
    target.write_text("HERMES_HOME=/tmp/hermes\n", encoding="utf-8")
    target.chmod(0o600)
    symlink = tmp_path / "symlink.env"
    symlink.symlink_to(target)
    with pytest.raises(ValueError, match="owner-only regular file"):
        collector_module.load_canary_collector_environment(symlink)

    target.chmod(0o640)
    with pytest.raises(ValueError, match="owner-only regular file"):
        collector_module.load_canary_collector_environment(target)
    target.chmod(0o600)

    real_read = collector_module.os.read
    changed = False

    def mutate_after_read(descriptor, size):
        nonlocal changed
        chunk = real_read(descriptor, size)
        if chunk and not changed:
            changed = True
            with target.open("ab") as handle:
                handle.write(b"MUTATED=1\n")
        return chunk

    monkeypatch.setattr(collector_module.os, "read", mutate_after_read)
    with pytest.raises(ValueError, match="changed while reading"):
        collector_module.load_canary_collector_environment(target)


def test_env_file_rejects_conflicting_control_database_paths(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            (
                f"HERMES_RCA_KAFKA_CONTROL_DB_PATH={tmp_path / 'kafka.sqlite3'}",
                f"HERMES_RCA_OUTBOX_CONTROL_DB_PATH={tmp_path / 'outbox.sqlite3'}",
            )
        )
        + "\n",
        encoding="utf-8",
    )
    env_file.chmod(0o600)
    args = collector_module.build_arg_parser().parse_args(
        ["--source-id", SOURCE_ID, "--env-file", str(env_file)]
    )

    with pytest.raises(ValueError, match="must match"):
        collector_module._collector_config_from_args(args)


def test_cli_manual_success_and_terminal_failure_are_mutually_exclusive():
    parser = collector_module.build_arg_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(
            ["--source-id", SOURCE_ID, "--manual-success", "--terminal-failure"]
        )


def _completed_digest_process(args, digests):
    files = {
        name: {
            "path": path,
            "size_bytes": 1,
            "raw_sha256": "a" * 64,
        }
        for name, (path, _limit) in digests.items()
    }
    return collector_module.subprocess.CompletedProcess(
        args=args,
        returncode=0,
        stdout=json.dumps({"ok": True, "read_only": True, "files": files}),
        stderr="",
    )


def test_remote_reader_accepts_aggregate_boundary_and_derives_bounded_deadline(
    monkeypatch,
):
    digests = {
        "first": ("/mnt/tmp/task/first.bin", 750_000_000),
        "second": ("/mnt/tmp/task/second.bin", 750_000_000),
        "third": ("/mnt/tmp/task/third.bin", 100_000_000),
    }
    captured = {}

    def fake_run(args, **kwargs):
        captured.update({"args": args, **kwargs})
        return _completed_digest_process(args, digests)

    monkeypatch.setattr(collector_module.subprocess, "run", fake_run)

    records = collector_module.SshMiniAgentReader("/tmp/ssh-mini-agent").read_sources(
        json_paths={}, digest_paths=digests
    )

    assert set(records) == set(digests)
    assert captured["args"] == ["/tmp/ssh-mini-agent", "run_py_json"]
    assert captured["env"]["SSH_MINI_AGENT_TIMEOUT"] == "111"
    assert captured["timeout"] == 116
    assert '"aggregate_byte_budget": 1600000000' in captured["input"]
    assert '"deadline_seconds": 106' in captured["input"]


def test_remote_reader_rejects_aggregate_budget_before_spawn(monkeypatch):
    def unexpected_run(*_args, **_kwargs):
        raise AssertionError("over-budget request must not spawn ssh-mini-agent")

    monkeypatch.setattr(collector_module.subprocess, "run", unexpected_run)
    digests = {
        "first": ("/mnt/tmp/task/first.bin", 750_000_000),
        "second": ("/mnt/tmp/task/second.bin", 750_000_000),
        "third": ("/mnt/tmp/task/third.bin", 100_000_001),
    }

    with pytest.raises(CanaryCollectionError) as caught:
        collector_module.SshMiniAgentReader().read_sources(
            json_paths={}, digest_paths=digests
        )

    assert caught.value.code == "remote_source_byte_budget_exceeded"


@pytest.mark.parametrize(
    ("limit", "wrapper_timeout", "host_timeout"),
    (
        (
            20 * collector_module.REMOTE_HASH_BYTES_PER_SECOND,
            "35",
            40,
        ),
        (
            20 * collector_module.REMOTE_HASH_BYTES_PER_SECOND + 1,
            "36",
            41,
        ),
    ),
)
def test_remote_reader_deadline_ceil_and_minimum_floor(
    monkeypatch, limit, wrapper_timeout, host_timeout
):
    digests = {"artifact": ("/mnt/tmp/task/artifact.bin", limit)}
    captured = {}

    def fake_run(args, **kwargs):
        captured.update(kwargs)
        return _completed_digest_process(args, digests)

    monkeypatch.setattr(collector_module.subprocess, "run", fake_run)

    collector_module.SshMiniAgentReader().read_sources(
        json_paths={}, digest_paths=digests
    )

    assert captured["env"]["SSH_MINI_AGENT_TIMEOUT"] == wrapper_timeout
    assert captured["timeout"] == host_timeout


def test_remote_reader_classifies_host_timeout_independently(monkeypatch):
    def timeout(args, **kwargs):
        raise collector_module.subprocess.TimeoutExpired(args, kwargs["timeout"])

    monkeypatch.setattr(collector_module.subprocess, "run", timeout)

    with pytest.raises(CanaryCollectionError) as caught:
        collector_module.SshMiniAgentReader().read_sources(
            json_paths={},
            digest_paths={"artifact": ("/mnt/tmp/task/artifact.bin", 1)},
        )

    assert caught.value.code == "remote_source_reader_timeout"


def test_remote_reader_classifies_wrapper_timeout_independently(monkeypatch):
    def timeout(args, **_kwargs):
        return collector_module.subprocess.CompletedProcess(
            args=args, returncode=124, stdout="", stderr=""
        )

    monkeypatch.setattr(collector_module.subprocess, "run", timeout)

    with pytest.raises(CanaryCollectionError) as caught:
        collector_module.SshMiniAgentReader().read_sources(
            json_paths={},
            digest_paths={"artifact": ("/mnt/tmp/task/artifact.bin", 1)},
        )

    assert caught.value.code == "remote_source_reader_timeout"


def test_remote_reader_keeps_spawn_failure_distinct_from_timeout(monkeypatch):
    def unavailable(*_args, **_kwargs):
        raise OSError("synthetic spawn failure")

    monkeypatch.setattr(collector_module.subprocess, "run", unavailable)

    with pytest.raises(CanaryCollectionError) as caught:
        collector_module.SshMiniAgentReader().read_sources(
            json_paths={},
            digest_paths={"artifact": ("/mnt/tmp/task/artifact.bin", 1)},
        )

    assert caught.value.code == "remote_source_reader_unavailable"


def test_remote_probe_program_is_bounded_and_read_only():
    script = collector_module.SshMiniAgentReader._remote_script({
        "json": {"receipt": "/mnt/tmp/task/receipt.json"},
        "digest": {},
        "aggregate_byte_budget": collector_module.MAX_REMOTE_AGGREGATE_BYTES,
        "deadline_seconds": collector_module.REMOTE_TIMEOUT_SECONDS,
    })

    compile(script, "<remote-canary-reader>", "exec")
    assert "O_RDONLY" in script
    assert "O_NOFOLLOW" in script
    assert "MAX_JSON_BYTES" in script
    assert "MAX_TOTAL_BYTES" in script
    assert "time.monotonic()" in script
    assert "raise SystemExit(124)" in script
    digest_program = script.split("def digest", 1)[1].split("json_specs =", 1)[0]
    assert "chunks" not in digest_program
    assert ".read_bytes(" not in digest_program
    for forbidden in (
        "O_WRONLY",
        "O_CREAT",
        "os.unlink",
        "os.remove",
        "os.replace",
        "subprocess",
    ):
        assert forbidden not in script


def test_execution_request_unicode_abi_hash_matches_vm_service_golden():
    payload = {"owners": ["测试负责人"], "title": "ACC 制动问题"}

    assert collector_module._sha256_execution_request(payload) == (
        "2bfbc8254ecc224be3e7df92197c6fd0a979b1210727229e98a2a9735ad00075"
    )
    assert _sha_json(payload) == (
        "5263b2694db9285d4a019fede7ad764717cbb329481611829b99a57ea8a1755f"
    )
