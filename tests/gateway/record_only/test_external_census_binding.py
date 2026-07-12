from __future__ import annotations

import copy
import fcntl
import json
import os
import subprocess
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from gateway.record_only.external_census_binding import (
    AUTHORITY_SCOPE,
    BINDING_SCHEMA,
    EXTERNAL_CENSUS_BINDING_FD,
    ExternalCensusBindingError,
    canonical_json,
    capture_id_for,
    consume_external_census_binding,
)


NOW = datetime(2026, 7, 12, 20, 1, tzinfo=timezone.utc)


def _binding() -> dict:
    value = {
        "schema_version": BINDING_SCHEMA,
        "decision": "PASS",
        "authority_scope": AUTHORITY_SCOPE,
        "record_only_launch_authorized": True,
        "candidate_execution_authorized": False,
        "cutover_authorized": False,
        "real_outbound_authorized": False,
        "production_state_write_authorized": False,
        "boot_session": "boot-fixture",
        "capture_id": "",
        "nonce": "n" * 32,
        "generated_at": "2026-07-12T20:00:00Z",
        "expires_at": "2026-07-12T20:02:00Z",
        "candidate_commit": "1" * 40,
        "candidate_tree": "2" * 40,
        "candidate_source_seal_sha256": "3" * 64,
        "global_census_sha256": "4" * 64,
        "route_index_sha256": "5" * 64,
        "source_manifest_sha256": "6" * 64,
        "prototype_status_sha256": "7" * 64,
        "total_routes": 7000,
        "unclassified_routes": 0,
        "overlay_paths_total": 824,
        "overlay_paths_covered": 824,
        "global_route_closure": True,
        "overlay_coverage_complete": True,
        "issuer": "STAGE_B_SEALED_OPERATOR",
        "launch_session_id": "launch-" + "l" * 32,
    }
    value["capture_id"] = capture_id_for(value)
    return value


def _write(path: Path, value: dict, *, canonical: bool = True) -> None:
    raw = (
        canonical_json(value)
        if canonical
        else (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
    )
    path.write_bytes(raw)
    path.chmod(0o400)


@contextmanager
def _installed_fd(
    path: Path,
    *,
    offset: int = 0,
    inheritable: bool = True,
):
    backup = None
    backup_inheritable = False
    try:
        backup_inheritable = os.get_inheritable(EXTERNAL_CENSUS_BINDING_FD)
        backup = os.dup(EXTERNAL_CENSUS_BINDING_FD)
    except OSError:
        pass
    source = os.open(path, os.O_RDONLY)
    try:
        os.dup2(
            source,
            EXTERNAL_CENSUS_BINDING_FD,
            inheritable=inheritable,
        )
    finally:
        os.close(source)
    if offset:
        os.lseek(EXTERNAL_CENSUS_BINDING_FD, offset, os.SEEK_SET)
    try:
        yield
    finally:
        try:
            os.close(EXTERNAL_CENSUS_BINDING_FD)
        except OSError:
            pass
        if backup is not None:
            os.dup2(
                backup,
                EXTERNAL_CENSUS_BINDING_FD,
                inheritable=backup_inheritable,
            )
            os.close(backup)


@contextmanager
def _without_fd():
    backup = None
    backup_inheritable = False
    try:
        backup_inheritable = os.get_inheritable(EXTERNAL_CENSUS_BINDING_FD)
        backup = os.dup(EXTERNAL_CENSUS_BINDING_FD)
        os.close(EXTERNAL_CENSUS_BINDING_FD)
    except OSError:
        pass
    try:
        yield
    finally:
        if backup is not None:
            os.dup2(
                backup,
                EXTERNAL_CENSUS_BINDING_FD,
                inheritable=backup_inheritable,
            )
            os.close(backup)


def _consume(path: Path, value: dict, *, canonical: bool = True):
    _write(path, value, canonical=canonical)
    with _installed_fd(path):
        return consume_external_census_binding(_now=NOW)


def test_consumes_exact_canonical_binding_and_closes_fd(tmp_path: Path) -> None:
    value = _binding()
    consumed = _consume(tmp_path / "binding.json", value)
    assert dict(consumed.binding) == value
    output = consumed.consumer_output
    assert output["record_only_launch_authorized"] is True
    assert output["candidate_execution_authorized"] is False
    assert output["cutover_authorized"] is False
    assert output["real_outbound_authorized"] is False
    assert output["production_state_write_authorized"] is False
    assert output["initial_offset"] == 0
    assert output["final_offset"] == output["pre_identity"]["size"]
    assert output["eof_observed"] is True
    assert output["fd_closed"] is True
    assert output["env_override_used"] is False
    assert output["argv_override_used"] is False
    assert output["path_open_attempts"] == 0
    assert output["consumer_generated_authority"] is False
    assert output["pre_identity"] == output["post_identity"]
    with pytest.raises(OSError):
        os.fstat(EXTERNAL_CENSUS_BINDING_FD)


def test_missing_fixed_fd_fails_closed() -> None:
    with _without_fd(), pytest.raises(
        ExternalCensusBindingError, match="FD 198 is unavailable"
    ):
        consume_external_census_binding(_now=NOW)


def test_environment_cannot_replace_fixed_fd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    valid = tmp_path / "valid.json"
    fake = tmp_path / "fake.json"
    value = _binding()
    _write(valid, value)
    fake.write_text("{}\n", encoding="ascii")
    monkeypatch.setenv("HERMES_OUTBOUND_CENSUS_ROOT", str(fake))
    monkeypatch.setenv("HERMES_RECORD_ONLY_BINDING_PATH", str(fake))
    with _installed_fd(valid):
        consumed = consume_external_census_binding(_now=NOW)
    assert consumed.binding["capture_id"] == value["capture_id"]


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("record_only_launch_authorized", False),
        ("candidate_execution_authorized", True),
        ("cutover_authorized", True),
        ("real_outbound_authorized", True),
        ("production_state_write_authorized", True),
        ("global_route_closure", False),
        ("overlay_coverage_complete", False),
        ("unclassified_routes", 1),
        ("issuer", "CALLER"),
    ],
)
def test_authority_or_closure_mutation_fails_closed(
    tmp_path: Path, field: str, replacement
) -> None:
    value = _binding()
    value[field] = replacement
    with pytest.raises(ExternalCensusBindingError):
        _consume(tmp_path / f"{field}.json", value)


def test_capture_id_mismatch_fails_closed(tmp_path: Path) -> None:
    value = _binding()
    value["global_census_sha256"] = "8" * 64
    with pytest.raises(ExternalCensusBindingError, match="capture_id differs"):
        _consume(tmp_path / "binding.json", value)


@pytest.mark.parametrize(
    ("generated", "expires", "message"),
    [
        (
            "2026-07-12T20:00:00Z",
            "2026-07-12T20:02:01Z",
            "lifetime exceeds",
        ),
        (
            "2026-07-12T20:01:01Z",
            "2026-07-12T20:02:00Z",
            "not currently valid",
        ),
        (
            "2026-07-12T19:58:00Z",
            "2026-07-12T20:00:00Z",
            "not currently valid",
        ),
    ],
)
def test_time_window_fails_closed(
    tmp_path: Path, generated: str, expires: str, message: str
) -> None:
    value = _binding()
    value["generated_at"] = generated
    value["expires_at"] = expires
    value["capture_id"] = capture_id_for(value)
    with pytest.raises(ExternalCensusBindingError, match=message):
        _consume(tmp_path / "binding.json", value)


def test_noncanonical_or_duplicate_json_fails_closed(tmp_path: Path) -> None:
    value = _binding()
    with pytest.raises(ExternalCensusBindingError, match="not canonical"):
        _consume(tmp_path / "pretty.json", value, canonical=False)

    duplicate = tmp_path / "duplicate.json"
    duplicate.write_bytes(b'{"schema_version":"x","schema_version":"y"}\n')
    duplicate.chmod(0o400)
    with _installed_fd(duplicate), pytest.raises(
        ExternalCensusBindingError, match="duplicate binding key"
    ):
        consume_external_census_binding(_now=NOW)


def test_extra_field_and_bad_hash_fail_closed(tmp_path: Path) -> None:
    extra = _binding()
    extra["caller_override"] = True
    with pytest.raises(ExternalCensusBindingError, match="fields differ"):
        _consume(tmp_path / "extra.json", extra)

    bad_hash = _binding()
    bad_hash["route_index_sha256"] = "not-a-hash"
    bad_hash["capture_id"] = capture_id_for(bad_hash)
    with pytest.raises(ExternalCensusBindingError, match="route_index_sha256"):
        _consume(tmp_path / "bad-hash.json", bad_hash)


def test_offset_cloexec_mode_and_hardlink_fail_closed(tmp_path: Path) -> None:
    value = _binding()
    path = tmp_path / "binding.json"
    _write(path, value)
    with _installed_fd(path, offset=1), pytest.raises(ExternalCensusBindingError):
        consume_external_census_binding(_now=NOW)
    with _installed_fd(path, inheritable=False), pytest.raises(
        ExternalCensusBindingError
    ):
        consume_external_census_binding(_now=NOW)

    path.chmod(0o600)
    with _installed_fd(path), pytest.raises(ExternalCensusBindingError):
        consume_external_census_binding(_now=NOW)
    path.chmod(0o400)
    hardlink = tmp_path / "hardlink.json"
    os.link(path, hardlink)
    with _installed_fd(path), pytest.raises(ExternalCensusBindingError):
        consume_external_census_binding(_now=NOW)


def test_in_read_identity_mutation_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "binding.json"
    _write(path, _binding())

    def mutate() -> None:
        path.chmod(0o600)

    with _installed_fd(path), pytest.raises(
        ExternalCensusBindingError, match="changed during read"
    ):
        consume_external_census_binding(_now=NOW, _after_read_hook=mutate)


def test_route_and_overlay_counts_are_strict_integers(tmp_path: Path) -> None:
    for field, replacement in (
        ("total_routes", True),
        ("total_routes", 0),
        ("overlay_paths_total", 0),
        ("overlay_paths_covered", 823),
    ):
        value = copy.deepcopy(_binding())
        value[field] = replacement
        value["capture_id"] = capture_id_for(value)
        with pytest.raises(ExternalCensusBindingError, match="route or overlay"):
            _consume(tmp_path / f"{field}-{replacement}.json", value)


def test_transport_import_consumes_fd_and_ignores_legacy_path(
    tmp_path: Path,
) -> None:
    value = _binding()
    now = datetime.now(timezone.utc).replace(microsecond=0)
    value["generated_at"] = (now - timedelta(seconds=1)).isoformat().replace(
        "+00:00", "Z"
    )
    value["expires_at"] = (now + timedelta(seconds=119)).isoformat().replace(
        "+00:00", "Z"
    )
    value["capture_id"] = capture_id_for(value)
    path = tmp_path / "binding.json"
    _write(path, value)
    code = """
import json
import os
from pathlib import Path
from gateway.record_only import transport
record_root = Path(os.environ["TEST_RECORD_ROOT"])
record_root.mkdir(mode=0o700)
sink = transport.RecordOnlyOutboundTransport(
    record_root,
    id_hash_key=b"k" * 32,
    source_component="test.external.binding",
)
result = sink.record(
    operation="text_send",
    platform="feishu",
    destination_kind="chat",
    destination_id="oc_fixture",
    payload_type="text",
    payload={"text": "isolated"},
)
row = sink.read_all()[0]
sink.close()
try:
    os.fstat(198)
except OSError:
    fd_closed = True
else:
    fd_closed = False
print(json.dumps({
    "status": transport.TARGET_OUTBOUND_CENSUS_STATUS,
    "binding": dict(transport.TARGET_OUTBOUND_CENSUS_BINDING),
    "external": dict(transport.PROTOTYPE_SAFETY_STATUS["external_outbound_census"]),
    "candidate_execution_authorized": transport.PROTOTYPE_SAFETY_STATUS["candidate_execution_authorized"],
    "record_success": result.success,
    "row_binding": row["target_outbound_census_binding"],
    "row_candidate_execution_authorized": row["candidate_execution_authorized"],
    "fd_closed": fd_closed,
}, sort_keys=True))
"""
    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[4])
    env["HERMES_OUTBOUND_CENSUS_ROOT"] = str(tmp_path / "must-not-be-read")
    env["TEST_RECORD_ROOT"] = str(tmp_path / "records")
    with _installed_fd(path):
        result = subprocess.run(
            [sys.executable, "-c", code],
            env=env,
            pass_fds=(EXTERNAL_CENSUS_BINDING_FD,),
            check=True,
            capture_output=True,
            text=True,
        )
    observed = json.loads(result.stdout)
    assert observed["status"] == "SEALED_EXTERNAL_CENSUS_RECORD_ONLY_LAUNCH"
    assert observed["binding"]["capture_id"] == value["capture_id"]
    assert observed["binding"]["record_only_launch_authorized"] is True
    assert observed["binding"]["candidate_execution_authorized"] is False
    assert observed["external"]["external_binding_consumed"] is True
    assert observed["candidate_execution_authorized"] is False
    assert observed["record_success"] is True
    assert observed["row_binding"]["capture_id"] == value["capture_id"]
    assert observed["row_candidate_execution_authorized"] is False
    assert observed["fd_closed"] is True


def test_module_entrypoint_emits_exact_consumer_receipt(tmp_path: Path) -> None:
    value = _binding()
    now = datetime.now(timezone.utc).replace(microsecond=0)
    value["generated_at"] = (now - timedelta(seconds=1)).isoformat().replace(
        "+00:00", "Z"
    )
    value["expires_at"] = (now + timedelta(seconds=119)).isoformat().replace(
        "+00:00", "Z"
    )
    value["capture_id"] = capture_id_for(value)
    path = tmp_path / "binding.json"
    _write(path, value)
    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[4])
    env["HERMES_RECORD_ONLY_BINDING_PATH"] = str(tmp_path / "ignored-env")
    with _installed_fd(path):
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "gateway.record_only.external_census_binding",
                "--binding-path",
                str(tmp_path / "ignored-argv"),
            ],
            env=env,
            pass_fds=(EXTERNAL_CENSUS_BINDING_FD,),
            check=True,
            capture_output=True,
        )
    output = json.loads(result.stdout)
    assert set(output) == {
        "schema_version",
        "decision",
        "boot_session",
        "candidate_commit",
        "candidate_tree",
        "launch_session_id",
        "binding_sha256",
        "fd_number",
        "pre_identity",
        "post_identity",
        "bytes_sha256",
        "initial_offset",
        "final_offset",
        "eof_observed",
        "fd_closed",
        "env_override_used",
        "argv_override_used",
        "path_open_attempts",
        "record_only_launch_authorized",
        "candidate_execution_authorized",
        "cutover_authorized",
        "real_outbound_authorized",
        "production_state_write_authorized",
        "consumer_generated_authority",
    }
    assert output["decision"] == "PASS"
    assert output["record_only_launch_authorized"] is True
    assert output["candidate_execution_authorized"] is False
    assert output["fd_closed"] is True
    assert result.stderr == b""


def test_module_entrypoint_without_fd_exits_three(tmp_path: Path) -> None:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[4])
    env["HERMES_OUTBOUND_CENSUS_ROOT"] = str(tmp_path / "ignored")
    result = subprocess.run(
        [sys.executable, "-m", "gateway.record_only.external_census_binding"],
        env=env,
        check=False,
        capture_output=True,
    )
    assert result.returncode == 3
    assert result.stdout == b""
    assert b"required inherited FD 198 is unavailable" in result.stderr
