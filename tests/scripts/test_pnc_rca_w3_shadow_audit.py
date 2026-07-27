from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sqlite3

import pytest

from scripts import pnc_rca_w3_shadow_audit as audit


NOW = "2026-07-26T10:00:00+00:00"


def _canonical(value) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _sha(value) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _policy(name: str) -> dict:
    value = {"name": name, "state": "bound"}
    version = f"{name}-v1"
    return {
        "version": version,
        "sha256": _sha({"version": version, "value": value}),
        "value": value,
    }


def _request(index: int) -> tuple[dict, str]:
    title = f"RCA request {index}"
    request = {
        "schema_version": audit.REQUEST_SCHEMA_VERSION,
        "ticket": {
            "project_key": "project-key",
            "project_simple_name": "g1q3",
            "work_item_type_key": "issue",
            "work_item_id": str(7000000000 + index),
            "issue_url": f"https://project.example/issue/{7000000000 + index}",
            "title": title,
            "title_sha256": _sha({"title": title}),
        },
        "execution_intent": {
            "kind": "initial",
            "generation_reason": "initial",
            "generation_authorization_evidence_sha256": None,
        },
        **{name: _policy(name) for name in audit._POLICY_NAMES},
    }
    return request, _sha(request)


def _snapshot(index: int, request: dict, request_sha256: str) -> tuple[dict, str]:
    identity = {
        "schema_version": audit.SNAPSHOT_SCHEMA_VERSION,
        "request_sha256": request_sha256,
        "canonical_request": request,
        "resolved_admission": {
            "key_version": "v1",
            "creation_rule_version": "rule-v1",
            "business_key": f"business-{index}",
            "submission_key": f"submission-{index}",
            "generation": 1,
            "create_once": True,
            "dedupe_scope": "business_generation",
        },
        "execution_admission": {
            "activation_epoch_id": "",
            "activation_ledger_id": None,
            "decision": "shadow",
            "reason": "activation_epoch_held_unconfigured",
            "state": "unconfigured",
            "legacy_unconfigured": False,
        },
        "write_fence": {
            "schema_version": "pnc_rca_write_fence_slot_v1",
            "state": "unissued",
        },
    }
    snapshot_sha256 = _sha(identity)
    return {
        "snapshot_id": f"pnc-rca-snapshot-v1-{snapshot_sha256}",
        "snapshot_sha256": snapshot_sha256,
        **identity,
    }, snapshot_sha256


def _envelope(
    snapshot: dict,
    *,
    source_id: str,
    source_kind: str,
    binding_action: str,
) -> dict:
    payload_sha256 = hashlib.sha256(f"payload:{source_id}".encode()).hexdigest()
    if source_kind == "kafka_workflow_event":
        metadata = {
            "source_kind": source_kind,
            "event_uid": f"topic:0:{source_id}",
            "topic": "topic",
            "partition": 0,
            "offset": int(source_id.rsplit("-", 1)[-1]),
            "payload_sha256": payload_sha256,
            "observed_at": NOW,
        }
        thread_target = None
    else:
        metadata = {
            "source_kind": source_kind,
            "platform": "feishu",
            "chat_id": "oc_test",
            "thread_id": f"topic:{source_id}",
            "message_id": f"om_{source_id}",
            "requester_id": "ou_test",
            "mode": "debug",
            "payload_sha256": payload_sha256,
            "observed_at": NOW,
        }
        thread_target = f"topic:{source_id}"
    authority_sha256 = hashlib.sha256(f"authority:{source_id}".encode()).hexdigest()
    ingress = {
        "requested_mode": "shadow",
        "binding_action": binding_action,
        "decision": "shadow",
        "authorization_evidence_sha256": hashlib.sha256(
            f"authorization:{source_id}".encode()
        ).hexdigest(),
    }
    identity = {
        "schema_version": audit.SOURCE_ENVELOPE_SCHEMA_VERSION,
        "source_authority_sha256": authority_sha256,
        "snapshot_id": snapshot["snapshot_id"],
        "snapshot_sha256": snapshot["snapshot_sha256"],
        "submission_key": snapshot["resolved_admission"]["submission_key"],
        "source_id": source_id,
        "source_kind": source_kind,
        "ingress_decision": ingress,
        "source_metadata": metadata,
        "anchor": {
            "issue_target": snapshot["canonical_request"]["ticket"]["issue_url"],
            "thread_target": thread_target,
        },
    }
    envelope_sha256 = _sha(identity)
    return {
        "source_envelope_id": f"pnc-rca-source-envelope-v1-{envelope_sha256}",
        "source_envelope_sha256": envelope_sha256,
        **identity,
    }


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE control_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        INSERT INTO control_meta VALUES('schema_version', 'pnc_rca_control_store_v12');

        CREATE TABLE rca_canonical_requests(
            request_sha256 TEXT PRIMARY KEY,
            schema_version TEXT,
            ticket_title_sha256 TEXT,
            creation_policy_sha256 TEXT,
            business_profile_sha256 TEXT,
            execution_policy_sha256 TEXT,
            publication_policy_sha256 TEXT,
            correction_lineage_policy_sha256 TEXT,
            generation_reason TEXT,
            generation_authorization_evidence_sha256 TEXT,
            canonical_request_json TEXT,
            persisted_at TEXT
        );
        CREATE TABLE rca_admission_snapshots(
            snapshot_sha256 TEXT PRIMARY KEY,
            snapshot_id TEXT,
            schema_version TEXT,
            request_sha256 TEXT,
            business_key TEXT,
            submission_key TEXT,
            generation INTEGER,
            activation_epoch_id TEXT,
            activation_ledger_id INTEGER,
            execution_decision TEXT,
            execution_reason TEXT,
            execution_state TEXT,
            legacy_unconfigured INTEGER,
            creator_source_envelope_sha256 TEXT,
            creator_authority_sha256 TEXT,
            creator_source_id TEXT,
            admission_snapshot_json TEXT,
            persisted_at TEXT
        );
        CREATE TABLE rca_snapshot_source_envelopes(
            source_envelope_sha256 TEXT PRIMARY KEY,
            source_envelope_id TEXT,
            schema_version TEXT,
            snapshot_sha256 TEXT,
            snapshot_id TEXT,
            submission_key TEXT,
            source_authority_sha256 TEXT,
            source_id TEXT,
            source_kind TEXT,
            payload_sha256 TEXT,
            authorization_evidence_sha256 TEXT,
            binding_action TEXT,
            decision TEXT,
            source_metadata_json TEXT,
            anchor_json TEXT,
            ingress_decision_json TEXT,
            source_envelope_json TEXT,
            persisted_at TEXT
        );
        """
    )


def _insert_pair(
    conn: sqlite3.Connection,
    index: int,
    *,
    joined: bool = True,
    joined_source_kind: str = "feishu_group_manual",
) -> None:
    request, request_sha256 = _request(index)
    snapshot, _snapshot_sha256 = _snapshot(index, request, request_sha256)
    creator = _envelope(
        snapshot,
        source_id=f"kafka-{index}",
        source_kind="kafka_workflow_event",
        binding_action="create",
    )
    conn.execute(
        "INSERT INTO rca_canonical_requests VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            request_sha256,
            request["schema_version"],
            request["ticket"]["title_sha256"],
            *(request[name]["sha256"] for name in audit._POLICY_NAMES),
            request["execution_intent"]["generation_reason"],
            request["execution_intent"]["generation_authorization_evidence_sha256"],
            _canonical(request),
            NOW,
        ),
    )
    resolved = snapshot["resolved_admission"]
    execution = snapshot["execution_admission"]
    conn.execute(
        "INSERT INTO rca_admission_snapshots VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            snapshot["snapshot_sha256"],
            snapshot["snapshot_id"],
            snapshot["schema_version"],
            request_sha256,
            resolved["business_key"],
            resolved["submission_key"],
            resolved["generation"],
            execution["activation_epoch_id"],
            execution["activation_ledger_id"],
            execution["decision"],
            execution["reason"],
            execution["state"],
            int(execution["legacy_unconfigured"]),
            creator["source_envelope_sha256"],
            creator["source_authority_sha256"],
            creator["source_id"],
            _canonical(snapshot),
            NOW,
        ),
    )
    envelopes = [creator]
    if joined:
        envelopes.append(
            _envelope(
                snapshot,
                source_id=f"manual-{index}",
                source_kind=joined_source_kind,
                binding_action="join",
            )
        )
    for envelope in envelopes:
        metadata = envelope["source_metadata"]
        ingress = envelope["ingress_decision"]
        conn.execute(
            "INSERT INTO rca_snapshot_source_envelopes VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                envelope["source_envelope_sha256"],
                envelope["source_envelope_id"],
                envelope["schema_version"],
                envelope["snapshot_sha256"],
                envelope["snapshot_id"],
                envelope["submission_key"],
                envelope["source_authority_sha256"],
                envelope["source_id"],
                envelope["source_kind"],
                metadata["payload_sha256"],
                ingress["authorization_evidence_sha256"],
                ingress["binding_action"],
                ingress["decision"],
                _canonical(metadata),
                _canonical(envelope["anchor"]),
                _canonical(ingress),
                _canonical(envelope),
                NOW,
            ),
        )


def _database(
    tmp_path: Path,
    *,
    pairs: int = 1,
    joined: bool = True,
    joined_source_kind: str = "feishu_group_manual",
) -> Path:
    path = tmp_path / "control.sqlite3"
    with sqlite3.connect(path) as conn:
        _create_schema(conn)
        for index in range(pairs):
            _insert_pair(
                conn,
                index,
                joined=joined,
                joined_source_kind=joined_source_kind,
            )
    return path


def _identity(path: Path) -> tuple[int, int, int, int, str]:
    observed = path.stat()
    return (
        observed.st_ino,
        observed.st_size,
        observed.st_mtime_ns,
        observed.st_nlink,
        hashlib.sha256(path.read_bytes()).hexdigest(),
    )


def test_clean_strict_ten_pair_audit_allows_only_source_and_anchor_diffs(
    tmp_path: Path,
) -> None:
    path = _database(tmp_path, pairs=10)

    result = audit.audit_w3_shadow(path, strict_acceptance=True)

    assert result["ok"] is True
    assert result["strict_acceptance_ready"] is True
    assert result["scope"]["valid_real_pair_count"] == 10
    assert result["scope"]["source_comparison_count"] == 10
    assert result["counts"]["forbidden_diff_paths"] == 0
    assert result["forbidden_diff_paths"] == []
    assert result["allowed_diff_paths"]
    assert all(
        path.startswith(("/source_metadata/", "/anchor/"))
        for path in result["allowed_diff_paths"]
    )


def test_current_v13_control_schema_is_accepted(tmp_path: Path) -> None:
    path = _database(tmp_path, pairs=10)
    with sqlite3.connect(path) as conn:
        conn.execute(
            "UPDATE control_meta SET value = 'pnc_rca_control_store_v13' "
            "WHERE key = 'schema_version'"
        )

    result = audit.audit_w3_shadow(path, strict_acceptance=True)

    assert result["ok"] is True
    assert result["control_db"]["control_schema_version"] == (
        "pnc_rca_control_store_v13"
    )


def test_forbidden_execution_core_mismatch_fails_closed(tmp_path: Path) -> None:
    path = _database(tmp_path)
    with sqlite3.connect(path) as conn:
        [raw] = conn.execute(
            "SELECT admission_snapshot_json FROM rca_admission_snapshots"
        ).fetchone()
        snapshot = json.loads(raw)
        snapshot["canonical_request"]["ticket"]["title"] = "forged title"
        conn.execute(
            "UPDATE rca_admission_snapshots SET admission_snapshot_json = ?",
            (_canonical(snapshot),),
        )

    result = audit.audit_w3_shadow(path)

    assert result["ok"] is False
    assert result["counts"]["valid_real_pairs"] == 0
    assert "/execution_core/canonical_request" in result["forbidden_diff_paths"]
    assert "forbidden_execution_core_diff" in result["gate_errors"]


def test_strict_acceptance_fails_when_real_pair_count_is_below_ten(
    tmp_path: Path,
) -> None:
    path = _database(tmp_path, pairs=9)

    diagnostic = audit.audit_w3_shadow(path)
    strict = audit.audit_w3_shadow(path, strict_acceptance=True)

    assert diagnostic["ok"] is True
    assert diagnostic["strict_acceptance_ready"] is False
    assert strict["ok"] is False
    assert strict["audit_clean"] is True
    assert strict["scope"]["valid_real_pair_count"] == 9
    assert strict["gate_errors"] == ["too_few_real_pairs"]


def test_strict_acceptance_rejects_a_weakened_minimum(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    path = _database(tmp_path, pairs=10)

    exit_code = audit.main([
        "--control-db",
        str(path),
        "--strict-acceptance",
        "--min-real-pairs",
        "9",
    ])

    report = json.loads(capsys.readouterr().out)
    assert exit_code == 2
    assert report["ok"] is False
    assert report["external_writes"] is False
    assert report["error"] == ("ShadowAuditError:min_real_pairs_below_strict_floor")


@pytest.mark.parametrize(
    ("joined", "joined_source_kind", "expected_comparisons"),
    [
        (False, "feishu_group_manual", 0),
        (True, "kafka_workflow_event", 10),
    ],
)
def test_strict_acceptance_requires_a_dual_distinct_source_comparison(
    tmp_path: Path,
    joined: bool,
    joined_source_kind: str,
    expected_comparisons: int,
) -> None:
    path = _database(
        tmp_path,
        pairs=10,
        joined=joined,
        joined_source_kind=joined_source_kind,
    )

    result = audit.audit_w3_shadow(path, strict_acceptance=True)

    assert result["ok"] is False
    assert result["audit_clean"] is True
    assert result["strict_acceptance_ready"] is False
    assert result["scope"]["valid_real_pair_count"] == 0
    assert result["scope"]["source_comparison_count"] == expected_comparisons
    assert result["gate_errors"] == ["too_few_real_pairs"]
    assert not any(pair["real_pair_qualified"] for pair in result["pairs"])


def test_immutable_uri_audit_preserves_database_identity_and_writes_nothing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = _database(tmp_path)
    before = _identity(path)
    calls = []
    real_connect = audit.sqlite3.connect

    def observed_connect(database, *args, **kwargs):
        calls.append((str(database), dict(kwargs)))
        return real_connect(database, *args, **kwargs)

    monkeypatch.setattr(audit.sqlite3, "connect", observed_connect)
    result = audit.audit_w3_shadow(path)

    assert result["ok"] is True
    assert _identity(path) == before
    assert not Path(f"{path}-wal").exists()
    assert not Path(f"{path}-shm").exists()
    assert len(calls) == 1
    assert "mode=ro&immutable=1" in calls[0][0]
    assert calls[0][1]["uri"] is True
    assert calls[0][1]["isolation_level"] is None
    assert result["control_db"]["identity_unchanged"] is True
    assert result["control_db"]["sha256_unchanged"] is True
    assert result["external_writes"] is False


@pytest.mark.parametrize("kind", ["relative", "symlink", "directory", "wal"])
def test_non_checkpoint_inputs_fail_closed(tmp_path: Path, kind: str) -> None:
    path = _database(tmp_path)
    target: Path
    if kind == "relative":
        target = Path("control.sqlite3")
    elif kind == "symlink":
        target = tmp_path / "control-link.sqlite3"
        target.symlink_to(path)
    elif kind == "directory":
        target = tmp_path / "control-dir"
        target.mkdir()
    else:
        Path(f"{path}-wal").write_bytes(b"uncheckpointed")
        target = path

    with pytest.raises(audit.ShadowAuditError):
        audit.audit_w3_shadow(target)


def test_database_change_during_audit_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = _database(tmp_path)
    real_sha256 = audit._file_sha256
    calls = 0

    def changed_sha256(observed: Path) -> str:
        nonlocal calls
        calls += 1
        value = real_sha256(observed)
        return value if calls == 1 else "f" * 64

    monkeypatch.setattr(audit, "_file_sha256", changed_sha256)

    with pytest.raises(RuntimeError, match="changed_during"):
        audit.audit_w3_shadow(path)


def test_cli_refuses_output_that_would_replace_control_database(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    path = _database(tmp_path)
    before = _identity(path)

    exit_code = audit.main(["--control-db", str(path), "--output", str(path)])

    report = json.loads(capsys.readouterr().out)
    assert exit_code == 2
    assert report["ok"] is False
    assert report["external_writes"] is False
    assert report["error"] == "ShadowAuditError:output_must_not_replace_control_db"
    assert _identity(path) == before
