from __future__ import annotations

import json
from pathlib import Path
import sqlite3

import pytest

from gateway.pnc_rca_control_store import RcaControlStore
from gateway.pnc_rca_delivery_store import RcaDeliveryStore
from scripts import pnc_rca_schema_fingerprint as fingerprint


def _database(path: Path) -> Path:
    RcaControlStore(path)
    RcaDeliveryStore(path)
    identity = {
        "fresh_install_db_instance_id": "9d9b8cfc-5a1e-4ddb-91c0-18145a5b0e53",
        "fresh_install_genesis_intent_sha256": "7" * 64,
    }
    with sqlite3.connect(path) as connection:
        for table in ("control_meta", "rca_delivery_meta"):
            connection.executemany(
                f"INSERT INTO {table}(key, value) VALUES (?, ?)",
                identity.items(),
            )
    return path


def _snapshot(tmp_path: Path, source: Path, stem: str = "one") -> dict:
    snapshot = tmp_path / f"{stem}.sqlite3"
    receipt = tmp_path / f"{stem}.receipt.json"
    result = fingerprint.create_snapshot_receipt(
        source.absolute(),
        snapshot_path=snapshot.absolute(),
        receipt_path=receipt.absolute(),
    )
    return {
        "result": result,
        "snapshot": snapshot,
        "receipt": receipt,
        "value": result["receipt"],
    }


def test_snapshot_receipt_binds_current_combined_schema_without_source_write(
    tmp_path: Path,
) -> None:
    source = _database(tmp_path / "control.sqlite3")
    with sqlite3.connect(source) as connection:
        before_identity = connection.execute(
            "SELECT key, value FROM control_meta WHERE key LIKE 'fresh_install_%' "
            "ORDER BY key"
        ).fetchall()

    produced = _snapshot(tmp_path, source)

    with sqlite3.connect(source) as connection:
        after_identity = connection.execute(
            "SELECT key, value FROM control_meta WHERE key LIKE 'fresh_install_%' "
            "ORDER BY key"
        ).fetchall()
    assert after_identity == before_identity
    assert produced["receipt"].stat().st_mode & 0o777 == 0o600
    assert produced["snapshot"].stat().st_mode & 0o777 == 0o600
    assert produced["value"]["assertions"] == {
        "foreign_keys_ok": True,
        "quick_check_ok": True,
        "snapshot_identity_matches_source": True,
        "snapshot_schema_matches_source": True,
        "source_identity_stable": True,
        "source_schema_stable": True,
    }
    assert produced["value"]["schema_material"]["object_count"] > 100
    verified = fingerprint.verify_snapshot_receipt(produced["receipt"].absolute())
    assert verified["ok"] is True
    assert verified["receipt_raw_sha256"] == produced["result"][
        "receipt_raw_sha256"
    ]


def test_schema_fingerprint_is_deterministic_and_ignores_row_content(
    tmp_path: Path,
) -> None:
    source = _database(tmp_path / "control.sqlite3")
    first = _snapshot(tmp_path, source, "first")
    with sqlite3.connect(source) as connection:
        connection.execute(
            "INSERT INTO control_meta(key, value) VALUES ('test_data_only', 'changed')"
        )
    second = _snapshot(tmp_path, source, "second")

    assert (
        first["value"]["schema_fingerprint_sha256"]
        == second["value"]["schema_fingerprint_sha256"]
    )
    assert first["value"]["schema_material"] == second["value"]["schema_material"]
    assert (
        first["value"]["snapshot_database"]["raw_sha256"]
        != second["value"]["snapshot_database"]["raw_sha256"]
    )


def test_schema_change_changes_fingerprint(tmp_path: Path) -> None:
    source = _database(tmp_path / "control.sqlite3")
    first = _snapshot(tmp_path, source, "first")
    with sqlite3.connect(source) as connection:
        connection.execute("CREATE TABLE test_schema_change(id INTEGER PRIMARY KEY)")
    second = _snapshot(tmp_path, source, "second")

    assert (
        first["value"]["schema_fingerprint_sha256"]
        != second["value"]["schema_fingerprint_sha256"]
    )
    assert (
        first["value"]["schema_material"]["object_count"] + 1
        == second["value"]["schema_material"]["object_count"]
    )


def test_snapshot_rejects_foreign_key_violation_and_removes_partial_outputs(
    tmp_path: Path,
) -> None:
    source = _database(tmp_path / "control.sqlite3")
    with sqlite3.connect(source) as connection:
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute("CREATE TABLE fp_parent(id INTEGER PRIMARY KEY)")
        connection.execute(
            "CREATE TABLE fp_child(parent_id INTEGER REFERENCES fp_parent(id))"
        )
        connection.execute("INSERT INTO fp_child(parent_id) VALUES (99)")
    snapshot = tmp_path / "bad.sqlite3"
    receipt = tmp_path / "bad.receipt.json"

    with pytest.raises(
        fingerprint.SchemaFingerprintError,
        match="rca_schema_fingerprint_foreign_key_failed",
    ):
        fingerprint.create_snapshot_receipt(
            source.absolute(),
            snapshot_path=snapshot.absolute(),
            receipt_path=receipt.absolute(),
        )

    assert not snapshot.exists()
    assert not receipt.exists()


def test_snapshot_rejects_existing_output_without_overwrite(tmp_path: Path) -> None:
    source = _database(tmp_path / "control.sqlite3")
    snapshot = tmp_path / "existing.sqlite3"
    snapshot.write_bytes(b"keep")
    receipt = tmp_path / "receipt.json"

    with pytest.raises(fingerprint.SchemaFingerprintError) as raised:
        fingerprint.create_snapshot_receipt(
            source.absolute(),
            snapshot_path=snapshot.absolute(),
            receipt_path=receipt.absolute(),
        )

    assert raised.value.code == "rca_schema_fingerprint_output_exists"
    assert snapshot.read_bytes() == b"keep"
    assert not receipt.exists()


def test_verify_rejects_receipt_digest_forgery(tmp_path: Path) -> None:
    source = _database(tmp_path / "control.sqlite3")
    produced = _snapshot(tmp_path, source)
    value = json.loads(produced["receipt"].read_text(encoding="utf-8"))
    value["schema_material"]["objects"][0]["name"] = "forged"
    produced["receipt"].write_text(
        json.dumps(value, sort_keys=True) + "\n", encoding="utf-8"
    )
    produced["receipt"].chmod(0o600)

    with pytest.raises(
        fingerprint.SchemaFingerprintError,
        match="rca_schema_fingerprint_receipt_digest_mismatch",
    ):
        fingerprint.verify_snapshot_receipt(produced["receipt"].absolute())


def test_cli_returns_direct_nonzero_on_invalid_receipt(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    invalid = tmp_path / "invalid.json"
    invalid.write_text("{}\n", encoding="utf-8")
    invalid.chmod(0o600)

    assert fingerprint.main(["verify", "--receipt", str(invalid.absolute())]) == 2
    payload = json.loads(capsys.readouterr().err)
    assert payload["ok"] is False
    assert payload["code"] == "rca_schema_fingerprint_receipt_shape_invalid"
