from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3

import pytest

from scripts import pnc_rca_prod_e2e_release as release
from scripts import pnc_rca_prod_execution_adapter as adapter


NOW = datetime(2026, 7, 22, 8, 0, tzinfo=timezone.utc)
RELEASE_ID = "rca-gray-7051585084-20260722"
EPOCH_ID = "rca-gray-epoch-20260722"


def _owner_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    path.chmod(0o700)
    return path


def _write_json(path: Path, value: dict) -> Path:
    path.write_bytes(adapter._canonical_bytes(value) + b"\n")
    path.chmod(0o600)
    return path


def _owned(path: Path) -> release.OwnedJson:
    raw = path.read_bytes()
    return release.OwnedJson(path.absolute(), raw, json.loads(raw))


def _build_request(tmp_path: Path, monkeypatch) -> tuple[Path, dict]:
    root = _owner_dir(tmp_path / "owned")
    final_path = _write_json(root / "final.json", {"final": True})
    final_owned = _owned(final_path)
    approval_request = {
        "release_id": RELEASE_ID,
        "release_bom": {"bootstrap_authorization": {"bootstrap_epoch_id": EPOCH_ID}},
    }
    verified = {
        "production_ready": True,
        "blockers": [],
        "execute_before": (NOW + timedelta(minutes=10)).isoformat(),
        "authorized_scope": {
            "target_exact_recovery": {
                "resident_consumer_only": True,
                "activation_slot_kind": "kafka_success",
                "commit_called": False,
            },
            "post_cutover_canary": {
                "resident_natural_gate_required": True,
                "max_poll_records_during_gate": 1,
                "pause_after_first_accepted": True,
                "failure_auto_stop": True,
                "activation_slot_kind": "",
                "activation_reason": "activation_steady_active",
            },
        },
    }
    monkeypatch.setattr(
        adapter,
        "_validate_final_authority",
        lambda **_kwargs: (approval_request, final_owned, verified),
    )
    output = root / "exact-request.json"
    adapter.build_exact_request(
        approval_request_path=root / "approval.json",
        final_validation_path=final_path,
        output_path=output,
        nonce="exact-recovery-adapter-nonce",
        now=NOW,
    )
    return output, json.loads(output.read_text(encoding="utf-8"))


def test_build_exact_request_binds_final_authority_and_has_zero_direct_effects(
    tmp_path, monkeypatch
):
    path, value = _build_request(tmp_path, monkeypatch)
    claimed = value.pop("request_sha256")

    assert value["event_uid"] == release.TARGET_EVENT_UID
    assert value["raw_sha256"] == release.TARGET_RAW_SHA256
    assert value["epoch_id"] == EPOCH_ID
    assert (
        value["final_validation_sha256"]
        == hashlib.sha256(adapter._canonical_bytes({"final": True}) + b"\n").hexdigest()
    )
    assert claimed == hashlib.sha256(adapter._canonical_bytes(value)).hexdigest()
    assert path.stat().st_mode & 0o077 == 0


def test_validate_exact_receipt_requires_resident_identity_and_no_commit(
    tmp_path, monkeypatch
):
    request_path, request = _build_request(tmp_path, monkeypatch)
    root = request_path.parent
    runtime_identity = {
        "service_label": "local.pnc.rca-kafka-consumer",
        "pid": 4321,
    }
    health_path = _write_json(
        root / "consumer-health.json", {"runtime_identity": runtime_identity}
    )
    receipt = {
        "schema_version": adapter.EXACT_RECEIPT_SCHEMA_VERSION,
        "release_id": RELEASE_ID,
        "epoch_id": EPOCH_ID,
        "request_sha256": request["request_sha256"],
        "final_validation_sha256": request["final_validation_sha256"],
        "processed_at": (NOW + timedelta(seconds=1)).isoformat(),
        "event_uid": release.TARGET_EVENT_UID,
        "raw_sha256": release.TARGET_RAW_SHA256,
        "business_key": release.TARGET_BUSINESS_KEY,
        "submission_key": release.TARGET_SUBMISSION_KEY,
        "generation": 1,
        "outcome": "ingested",
        "activation_slot_kind": "kafka_success",
        "resident_runtime_identity_sha256": release._sha256_value(runtime_identity),
        "kafka_observation": {
            "assignment_mode": "explicit_single_partition",
            "assigned_partitions": [0],
            "retained_start": 599,
            "retained_end": 752,
            "group_id": None,
            "enable_auto_commit": False,
            "commit_called": False,
        },
        "raw_payload_persisted": True,
        "kafka_offset_committed": False,
    }
    receipt_path = _write_json(root / "exact-receipt.json", receipt)

    result = adapter.validate_exact_receipt(
        exact_request_path=request_path,
        exact_receipt_path=receipt_path,
        consumer_health_path=health_path,
    )

    assert result["ok"] is True
    assert result["release_id"] == RELEASE_ID
    assert result["epoch_id"] == EPOCH_ID
    assert result["kafka_offset_committed"] is False
    tampered = dict(receipt)
    tampered["kafka_offset_committed"] = True
    tampered_path = _write_json(root / "tampered-receipt.json", tampered)
    with pytest.raises(adapter.ExecutionAdapterError, match="exact_receipt_invalid"):
        adapter.validate_exact_receipt(
            exact_request_path=request_path,
            exact_receipt_path=tampered_path,
            consumer_health_path=health_path,
        )


def _readback_expectation(root: Path, *, marker: str, comment: str) -> Path:
    return _write_json(
        root / "readback-expectation.json",
        {
            "schema_version": adapter.READBACK_EXPECTATION_SCHEMA_VERSION,
            "release_id": RELEASE_ID,
            "project_key": release.TARGET_PROJECT_KEY,
            "work_item_id": release.TARGET_WORK_ITEM_ID,
            "field_values": {
                release.RESULT_FIELD_KEY: adapter._hash_text("root cause"),
                release.REPORT_FIELD_KEY: adapter._hash_text(
                    "https://rca.example/report/index.html"
                ),
            },
            "comment_id": "comment-705",
            "comment_content": adapter._hash_text(comment),
            "marker": marker,
            "terminal_receipt_sha256": "7" * 64,
            "not_before": NOW.isoformat(),
        },
    )


def test_official_readback_hashes_every_full_body_and_requires_one_marker(tmp_path):
    root = _owner_dir(tmp_path / "owned")
    marker = "[RCA_DELIVERY:effect-705:artifact-705]"
    comment = f"report ready\n{marker}\nfull body"
    expectation = _readback_expectation(root, marker=marker, comment=comment)

    class OfficialAdapter:
        def get_fields_and_comments(self, *_args):
            return {
                "success": True,
                "fields": {
                    release.RESULT_FIELD_KEY: "root cause",
                    release.REPORT_FIELD_KEY: "https://rca.example/report/index.html",
                },
                "comments": [
                    {"remote_id": "other", "content": "unrelated full body"},
                    {"remote_id": "comment-705", "content": comment},
                ],
                "pages_read": 2,
            }

    output = root / "readback.json"
    result = adapter.official_full_readback(
        expectation_path=expectation,
        output_path=output,
        adapter_factory=OfficialAdapter,
        now=NOW + timedelta(seconds=1),
    )

    assert result["marker_match_count"] == 1
    assert result["full_bodies_persisted"] is False
    assert len(result["comments"]) == 2
    assert all("content" not in item for item in result["comments"])


def test_build_natural_gate_binds_exact_readback_and_resident_receipt(
    tmp_path, monkeypatch
):
    root = _owner_dir(tmp_path / "owned")
    exact_receipt = _write_json(root / "exact-receipt.json", {"exact": True})
    readback = _target_readback(root)
    approval_request = {
        "release_id": RELEASE_ID,
        "release_bom": {"bootstrap_authorization": {"bootstrap_epoch_id": EPOCH_ID}},
    }
    verified = {
        "execute_before": (NOW + timedelta(minutes=10)).isoformat(),
    }
    monkeypatch.setattr(
        adapter,
        "_validate_final_authority",
        lambda **_kwargs: (
            approval_request,
            release.OwnedJson(
                (root / "final.json").absolute(),
                b"final",
                {},
            ),
            verified,
        ),
    )
    monkeypatch.setattr(
        adapter,
        "validate_exact_receipt",
        lambda **_kwargs: {
            "release_id": RELEASE_ID,
            "epoch_id": EPOCH_ID,
            "final_validation_sha256": hashlib.sha256(b"final").hexdigest(),
            "processed_at": (NOW + timedelta(seconds=1)).isoformat(),
        },
    )
    output = root / "natural-gate.json"

    result = adapter.build_natural_gate(
        approval_request_path=root / "approval.json",
        final_validation_path=root / "final.json",
        exact_request_path=root / "exact-request.json",
        exact_receipt_path=exact_receipt,
        consumer_health_path=root / "health.json",
        target_readback_path=readback,
        minimum_offset=752,
        output_path=output,
        now=NOW + timedelta(seconds=2),
    )

    assert result["schema_version"] == adapter.NATURAL_GATE_SCHEMA_VERSION
    assert result["minimum_offset"] == 752
    assert (
        result["exact_recovery_receipt_sha256"]
        == hashlib.sha256(exact_receipt.read_bytes()).hexdigest()
    )
    assert (
        result["exact_readback_sha256"]
        == hashlib.sha256(readback.read_bytes()).hexdigest()
    )

    tampered_readback = json.loads(readback.read_text(encoding="utf-8"))
    tampered_readback["comments"][0]["content_sha256"] = "0" * 64
    tampered_readback_path = _write_json(
        root / "tampered-target-readback.json", tampered_readback
    )
    with pytest.raises(adapter.ExecutionAdapterError, match="target_readback_invalid"):
        adapter.build_natural_gate(
            approval_request_path=root / "approval.json",
            final_validation_path=root / "final.json",
            exact_request_path=root / "exact-request.json",
            exact_receipt_path=exact_receipt,
            consumer_health_path=root / "health.json",
            target_readback_path=tampered_readback_path,
            minimum_offset=752,
            output_path=root / "must-not-exist-readback-gate.json",
            now=NOW + timedelta(seconds=2),
        )

    monkeypatch.setattr(
        adapter,
        "validate_exact_receipt",
        lambda **_kwargs: {
            "release_id": "different-release",
            "epoch_id": EPOCH_ID,
            "final_validation_sha256": hashlib.sha256(b"final").hexdigest(),
            "processed_at": (NOW + timedelta(seconds=1)).isoformat(),
        },
    )
    with pytest.raises(adapter.ExecutionAdapterError, match="exact_release_binding"):
        adapter.build_natural_gate(
            approval_request_path=root / "approval.json",
            final_validation_path=root / "final.json",
            exact_request_path=root / "exact-request.json",
            exact_receipt_path=exact_receipt,
            consumer_health_path=root / "health.json",
            target_readback_path=readback,
            minimum_offset=752,
            output_path=root / "must-not-exist-gate.json",
            now=NOW + timedelta(seconds=2),
        )


def _natural_database(path: Path, *, offset: int = 800, closed: bool = True) -> Path:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE kafka_inbox(
          event_uid TEXT PRIMARY KEY,topic TEXT,partition_id INTEGER,offset_id INTEGER,
          raw_sha256 TEXT,business_key TEXT,submission_key TEXT,generation INTEGER,
          processed_at TEXT,decision TEXT
        );
        CREATE TABLE business_triggers(
          submission_key TEXT,project_key TEXT,work_item_type_key TEXT,work_item_id TEXT
        );
        CREATE TABLE rca_outbox(
          submission_key TEXT,status TEXT,completed_at TEXT,
          activation_epoch_id TEXT,activation_ledger_id INTEGER
        );
        CREATE TABLE rca_activation_admission_ledger(
          epoch_id TEXT,ledger_id INTEGER,entrypoint TEXT,slot_kind TEXT,
          decision TEXT,reason TEXT
        );
        CREATE TABLE rca_delivery_jobs(
          submission_key TEXT,delivery_id TEXT,status TEXT,outcome TEXT
        );
        CREATE TABLE rca_delivery_effects(
          delivery_id TEXT,required INTEGER,status TEXT
        );
        CREATE TABLE rca_shadow_promotion_audit(event_uid TEXT);
        """
    )
    event_uid = f"{release.TOPIC}:0:{offset}"
    submission = f"natural-submission-{offset}"
    processed = (NOW + timedelta(seconds=2)).isoformat()
    conn.execute(
        "INSERT INTO kafka_inbox VALUES(?,?,?,?,?,?,?,?,?,'accepted')",
        (
            event_uid,
            release.TOPIC,
            0,
            offset,
            "8" * 64,
            f"business-{offset}",
            submission,
            1,
            processed,
        ),
    )
    conn.execute(
        "INSERT INTO business_triggers VALUES(?,?,?,?)",
        (submission, release.TARGET_PROJECT_KEY, "issue", f"7059{offset}"),
    )
    conn.execute(
        "INSERT INTO rca_outbox VALUES(?,?,?,?,?)",
        (
            submission,
            "completed" if closed else "pending",
            processed if closed else None,
            EPOCH_ID,
            offset,
        ),
    )
    conn.execute(
        "INSERT INTO rca_activation_admission_ledger VALUES(?,?,'kafka_ingest',NULL,'admit','activation_steady_active')",
        (EPOCH_ID, offset),
    )
    if closed:
        conn.execute(
            "INSERT INTO rca_delivery_jobs VALUES(?,?,'delivered','success')",
            (submission, f"delivery-{offset}"),
        )
        conn.execute(
            "INSERT INTO rca_delivery_effects VALUES(?,1,'succeeded')",
            (f"delivery-{offset}",),
        )
    conn.commit()
    conn.close()
    path.chmod(0o600)
    return path


def _target_readback(root: Path) -> Path:
    comment = adapter._hash_text("canonical full comment")
    return _write_json(
        root / "target-readback.json",
        {
            "schema_version": adapter.READBACK_RECEIPT_SCHEMA_VERSION,
            "release_id": RELEASE_ID,
            "adapter": "MeegleIssueCommentAdapter.get_fields_and_comments",
            "source": "official_meegle_api",
            "scope": {
                "project_key": release.TARGET_PROJECT_KEY,
                "work_item_id": release.TARGET_WORK_ITEM_ID,
            },
            "observed_at": (NOW + timedelta(seconds=1)).isoformat(),
            "fields": {
                release.RESULT_FIELD_KEY: adapter._hash_text("root cause"),
                release.REPORT_FIELD_KEY: adapter._hash_text(
                    "https://rca.example/report/index.html"
                ),
            },
            "comment_id": "comment-705",
            "comment_content_sha256": comment["sha256"],
            "comment_content_utf8_bytes": comment["utf8_bytes"],
            "marker_sha256": "4" * 64,
            "marker_match_count": 1,
            "pages_read": 1,
            "comments": [
                {
                    "comment_id": "comment-705",
                    "content_sha256": comment["sha256"],
                    "content_utf8_bytes": comment["utf8_bytes"],
                    "marker_match_count": 1,
                }
            ],
            "terminal_receipt_sha256": "7" * 64,
            "full_bodies_persisted": False,
        },
    )


def test_selector_returns_first_closed_steady_natural_and_never_skips_failure(tmp_path):
    root = _owner_dir(tmp_path / "owned")
    readback = _target_readback(root)
    database = _natural_database(root / "control.sqlite3", offset=800)
    resident_receipt = _write_json(
        root / "natural-canary-gate.json.receipt.json",
        {
            "schema_version": adapter.NATURAL_RECEIPT_SCHEMA_VERSION,
            "release_id": RELEASE_ID,
            "epoch_id": EPOCH_ID,
            "request_sha256": "6" * 64,
            "selected_at": (NOW + timedelta(seconds=2)).isoformat(),
            "topic": release.TOPIC,
            "partition": release.PARTITION,
            "decision": "accepted",
            "activation_reason": "activation_steady_active",
            "consumer_group_id": "rca_root_cause_analysis_agent",
            "kafka_offset_committed": True,
            "resident_runtime_identity_sha256": "5" * 64,
            "next_ordinary_record_held": True,
            "event_uid": f"{release.TOPIC}:0:800",
            "offset": 800,
            "business_key": "business-800",
            "submission_key": "natural-submission-800",
            "generation": 1,
        },
    )
    output = root / "natural.json"

    result = adapter.select_first_natural(
        control_db_path=database,
        target_readback_path=readback,
        natural_canary_receipt_path=resident_receipt,
        minimum_offset=799,
        output_path=output,
        now=NOW + timedelta(seconds=3),
    )

    assert result["offset"] == 800
    assert result["activation"] == {
        "entrypoint": "kafka_ingest",
        "slot_kind": "",
        "decision": "admit",
        "reason": "activation_steady_active",
    }
    tampered_resident = json.loads(resident_receipt.read_text(encoding="utf-8"))
    tampered_resident["request_sha256"] = "invalid"
    tampered_path = _write_json(
        root / "tampered-natural-receipt.json", tampered_resident
    )
    with pytest.raises(
        adapter.ExecutionAdapterError, match="natural_request_sha256_invalid"
    ):
        adapter.select_first_natural(
            control_db_path=database,
            target_readback_path=readback,
            natural_canary_receipt_path=tampered_path,
            minimum_offset=799,
            output_path=root / "must-not-exist-tampered.json",
            now=NOW + timedelta(seconds=3),
        )
    failed_db = _natural_database(root / "failed.sqlite3", offset=799, closed=False)
    with pytest.raises(adapter.ExecutionAdapterError, match="natural_not_closed"):
        adapter.select_first_natural(
            control_db_path=failed_db,
            target_readback_path=readback,
            natural_canary_receipt_path=resident_receipt,
            minimum_offset=799,
            output_path=root / "must-not-exist.json",
            now=NOW + timedelta(seconds=3),
        )
