from __future__ import annotations

from datetime import datetime, timezone
import json
import os

import pytest

from scripts import pnc_rca_writer_stop_evidence as writer_cli
from scripts.pnc_rca_store_migration_drill import (
    STORE_WRITER_LABELS,
    MigrationDrillError,
    collect_writer_stop_evidence,
)


NOW = datetime(2026, 7, 12, 9, 0, tzinfo=timezone.utc)


def _probe(*, live_label: str = "") -> dict:
    return {
        label: {
            "launchd_job_state": "present" if label == live_label else "absent",
            "matching_pids": [43210] if label == live_label else [],
        }
        for label in STORE_WRITER_LABELS
    }


def test_collector_records_machine_probe_for_all_writers():
    evidence = collect_writer_stop_evidence(
        now=NOW,
        writer_process_probe=_probe,
    )

    assert evidence["schema_version"] == "pnc_rca_writer_stop_evidence_v1"
    assert evidence["observed_at"] == NOW.isoformat()
    assert evidence["process_probe"] == (
        "launchctl_job_absence_psutil_process_absence_v2"
    )
    assert set(evidence["services"]) == STORE_WRITER_LABELS
    assert all(
        service["launchd_job_state"] == "absent"
        and service["matching_pids"] == []
        and service["pid_state"] == "pid_absent"
        and service["health_state"] == "stopped"
        for service in evidence["services"].values()
    )


def test_collector_rejects_any_live_writer():
    with pytest.raises(MigrationDrillError) as error:
        collect_writer_stop_evidence(
            now=NOW,
            writer_process_probe=lambda: _probe(
                live_label="local.pnc.rca-outbox-dispatcher"
            ),
        )

    assert error.value.code == "writer_stop_process_still_running"


def test_cli_writes_owner_only_fixed_filename(tmp_path, monkeypatch, capsys):
    evidence = collect_writer_stop_evidence(
        now=NOW,
        writer_process_probe=_probe,
    )
    monkeypatch.setattr(writer_cli, "collect_writer_stop_evidence", lambda: evidence)

    assert writer_cli.main(["--evidence-dir", str(tmp_path)]) == 0

    output = tmp_path / writer_cli.WRITER_STOP_FILENAME
    assert json.loads(output.read_text(encoding="utf-8")) == evidence
    assert os.stat(output).st_mode & 0o777 == 0o600
    result = json.loads(capsys.readouterr().out)
    assert result["ok"] is True
    assert result["output"] == str(output)


def test_cli_does_not_replace_evidence_when_probe_fails(
    tmp_path, monkeypatch, capsys
):
    def fail():
        raise MigrationDrillError("writer_stop_process_still_running")

    monkeypatch.setattr(writer_cli, "collect_writer_stop_evidence", fail)

    assert writer_cli.main(["--evidence-dir", str(tmp_path)]) == 2
    assert not (tmp_path / writer_cli.WRITER_STOP_FILENAME).exists()
    assert json.loads(capsys.readouterr().out) == {
        "ok": False,
        "code": "writer_stop_process_still_running",
    }
