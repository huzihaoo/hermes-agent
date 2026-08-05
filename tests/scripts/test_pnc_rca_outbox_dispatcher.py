from __future__ import annotations

from dataclasses import replace
import copy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import gateway.pnc_rca_control_store as control_store_module
import gateway.pnc_rca_prod_bootstrap as bootstrap_module
from gateway.pnc_rca_prod_bootstrap import RcaBootstrapAuthorizationError
from gateway.pnc_rca_control_store import RcaControlStore
from gateway.pnc_rca_control_store import (
    EXACT_OUTBOX_HOLD_MIN_REMAINING_SECONDS,
    _exact_outbox_hold_fingerprint,
    _exact_outbox_hold_plan_id,
)
from gateway.pnc_rca_runtime_identity import RCA_RUNTIME_RELATIVE_FILES
from gateway.pnc_rca_runtime_identity import canonical_json_sha256
from scripts import pnc_rca_outbox_dispatcher as dispatcher
from tests.gateway.test_pnc_rca_control_store import (
    _begin_bounded_activation,
    _manual_request,
    _policy,
    _record,
)


RELEASE_ID = "rca-v0182-test-release"
EPOCH_ID = "rca-bootstrap-v0182-test-epoch"


def test_storage_admission_uses_isolated_production_runtime():
    assert dispatcher.REMOTE_STORAGE_ADMISSION_MODULE == (
        "/home/mini/.hermes/rca-prod-runtime/releases/"
        "rca-platform-20260724/api/g1q3_rca/storage_admission.py"
    )


def _config_env(tmp_path, *, enabled: bool = False) -> dict[str, str]:
    return {
        "HERMES_RCA_OUTBOX_DISPATCH_ENABLED": str(enabled).lower(),
        "HERMES_RCA_OUTBOX_STORAGE_ADMISSION_ENABLED": str(enabled).lower(),
        "HERMES_RCA_OUTBOX_DERIVED_CAPACITY_RESERVATION_ENABLED": str(
            enabled
        ).lower(),
        "HERMES_RCA_OUTBOX_DELIVERY_BACKPRESSURE_ENABLED": str(enabled).lower(),
        "HERMES_RCA_OUTBOX_CONTROL_DB_PATH": str(tmp_path / "control.sqlite3"),
        "HERMES_RCA_OUTBOX_HEALTH_PATH": str(tmp_path / "health.json"),
        "HERMES_RCA_PROD_CAPACITY_MODE": "bootstrap",
        "HERMES_RCA_PROD_RELEASE_ID": RELEASE_ID,
        "HERMES_RCA_PROD_BOOTSTRAP_EPOCH_ID": EPOCH_ID,
    }


def _authorization() -> dict[str, object]:
    return {
        "authorization_ready": True,
        "capacity_mode": "bootstrap",
        "bootstrap_epoch_id": EPOCH_ID,
        "release_approval_id": RELEASE_ID,
        "started_at": "2026-07-20T00:00:00+00:00",
        "deadline": "2026-07-24T00:00:00+00:00",
        "receipt_fingerprint": "1" * 64,
        "authorization_receipt_sha256": "2" * 64,
        "active_release_binding_sha256": "3" * 64,
        "candidate_env_sha256": "4" * 64,
        "release_bom_sha256": "5" * 64,
        "approval_evidence_sha256": "6" * 64,
    }


def test_config_requires_and_projects_production_capacity_binding(tmp_path):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )

    public = config.public_dict()
    assert config.activation_required is False
    assert public["activation_required"] is False
    assert public["capacity_mode"] == "bootstrap"
    assert public["release_id"] == RELEASE_ID
    assert public["bootstrap_epoch_id"] == EPOCH_ID
    assert config.active_release_binding_path == (
        tmp_path / "active-release-binding.json"
    )
    assert config.live_env_path == tmp_path / ".env"


def test_config_exposes_strict_activation_required(tmp_path):
    env = _config_env(tmp_path)
    env["HERMES_RCA_OUTBOX_ACTIVATION_REQUIRED"] = "true"

    config = dispatcher.DispatcherConfig.from_env(env, hermes_home=tmp_path)

    assert config.activation_required is True
    assert config.public_dict()["activation_required"] is True


def test_config_rejects_declarative_feishu_writeback_enablement(tmp_path):
    env = _config_env(tmp_path)
    env["HERMES_RCA_OUTBOX_ALLOW_FEISHU_WRITEBACK"] = "true"

    with pytest.raises(ValueError, match="declarative-only"):
        dispatcher.DispatcherConfig.from_env(env, hermes_home=tmp_path)


def test_config_keeps_legacy_feishu_writeback_projection_false(tmp_path):
    config = dispatcher.DispatcherConfig.from_env(_config_env(tmp_path), hermes_home=tmp_path)

    assert config.allow_feishu_writeback is False
    assert config.public_dict()["allow_feishu_writeback"] is False


@pytest.mark.parametrize("value", ["1", "0", "yes", "on", "off", ""])
def test_activation_required_rejects_boolean_aliases(tmp_path, value):
    env = _config_env(tmp_path)
    env["HERMES_RCA_OUTBOX_ACTIVATION_REQUIRED"] = value

    with pytest.raises(ValueError, match="exactly true or false"):
        dispatcher.DispatcherConfig.from_env(env, hermes_home=tmp_path)


def test_dispatcher_activation_gate_does_not_claim_legacy_null_row(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    legacy = store.ingest_record(
        _record(),
        policy=_policy(),
        submit_enabled=True,
    )
    env = _config_env(tmp_path, enabled=True)
    env["HERMES_RCA_OUTBOX_ACTIVATION_REQUIRED"] = "true"
    config = dispatcher.DispatcherConfig.from_env(env, hermes_home=tmp_path)
    instance = dispatcher.OutboxDispatcher(
        store=store,
        config=config,
        enrich=lambda _claim: None,
        storage_admission=lambda _request: {},
        submit=lambda _admission, _request: {},
        derived_capacity_reservation=lambda _request: None,
        lease_owner="activation-required-test",
    )
    instance._delivery_backpressure_outcome = lambda: None

    outcome = instance.dispatch_one()

    assert outcome.status == "idle"
    [row] = store.list_rows("rca_outbox")
    assert row["submission_key"] == legacy.submission_key
    assert row["activation_epoch_id"] is None
    assert row["activation_ledger_id"] is None
    assert row["status"] == "pending"
    assert row["lease_token"] is None


def test_dispatcher_renew_passes_activation_required(tmp_path):
    env = _config_env(tmp_path)
    env["HERMES_RCA_OUTBOX_ACTIVATION_REQUIRED"] = "true"
    calls = []
    instance = object.__new__(dispatcher.OutboxDispatcher)
    instance.config = dispatcher.DispatcherConfig.from_env(
        env,
        hermes_home=tmp_path,
    )
    instance.store = SimpleNamespace(
        extend_outbox_lease=lambda **kwargs: calls.append(kwargs)
    )
    instance.now = lambda: datetime.now(timezone.utc)
    claim = SimpleNamespace(
        outbox_id=17,
        lease_token="lease-token",
        lease_owner="lease-owner",
    )

    instance._renew(claim)

    assert calls[0]["activation_required"] is True


def test_dispatcher_dry_run_does_not_preview_legacy_null_row(
    tmp_path,
    monkeypatch,
    capsys,
):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    store.ingest_record(_record(), policy=_policy(), submit_enabled=True)
    env = _config_env(tmp_path)
    env["HERMES_RCA_OUTBOX_ACTIVATION_REQUIRED"] = "true"
    config = dispatcher.DispatcherConfig.from_env(env, hermes_home=tmp_path)
    monkeypatch.setattr(dispatcher, "load_dispatcher_environment", lambda _path: None)
    monkeypatch.setattr(
        dispatcher.DispatcherConfig,
        "from_env",
        classmethod(lambda _cls: config),
    )

    assert dispatcher.main(["--dry-run"]) == 0

    assert '"due_count_in_sample": 0' in capsys.readouterr().out
    [row] = store.list_rows("rca_outbox")
    assert row["status"] == "pending"
    assert row["lease_token"] is None


def test_enabled_resident_without_epoch_exits_before_dispatcher_creation(
    tmp_path,
    monkeypatch,
    capsys,
):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path, enabled=True),
        hermes_home=tmp_path,
    )
    store = RcaControlStore(config.control_db_path)
    constructed = False

    def unexpected_dispatcher(*_args, **_kwargs):
        nonlocal constructed
        constructed = True
        raise AssertionError("dispatcher must not start without an active epoch")

    monkeypatch.setattr(dispatcher, "load_dispatcher_environment", lambda _path: None)
    monkeypatch.setattr(
        dispatcher.DispatcherConfig,
        "from_env",
        classmethod(lambda _cls: config),
    )
    monkeypatch.setattr(dispatcher, "OutboxDispatcher", unexpected_dispatcher)

    assert dispatcher.main(["--once"]) == 2
    assert constructed is False
    assert store.list_rows("kafka_inbox") == []
    assert store.list_rows("rca_outbox") == []
    assert "resident_activation_epoch_missing" in capsys.readouterr().err


def _patch_reset_cli(monkeypatch, config):
    monkeypatch.setattr(dispatcher, "load_dispatcher_environment", lambda _path: None)
    monkeypatch.setattr(
        dispatcher.DispatcherConfig,
        "from_env",
        classmethod(lambda _cls: config),
    )


def _exact_hold_fixture(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    _begin_bounded_activation(store, epoch_id="rca-exact-hold-test")
    conn = store._connect()
    try:
        conn.execute("INSERT INTO sqlite_sequence(name, seq) VALUES('rca_outbox', 683)")
    finally:
        conn.close()
    success = store.admit_manual_trigger(
        _manual_request(
            "om_manual_success",
            issue_url="https://project.feishu.cn/g1q3/issue/detail/7041712814",
        ),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        active_policy=_policy(),
        activation_required=True,
    )
    terminal = store.admit_manual_trigger(
        _manual_request(
            "om_manual_terminal",
            mode="debug",
            issue_url="https://project.feishu.cn/g1q3/issue/detail/7041712815",
        ),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        operator_authorized=True,
        active_policy=_policy(),
        activation_required=True,
    )
    rows = store.list_rows("rca_outbox")
    assert [row["outbox_id"] for row in rows] == [684, 685]
    assert [row["submission_key"] for row in rows] == [
        success.submission_key,
        terminal.submission_key,
    ]
    return store


def _patch_exact_hold_cli(monkeypatch, config):
    _patch_reset_cli(monkeypatch, config)
    runtime = dispatcher._exact_outbox_runtime_provenance()
    active_payload = {
        "release_id": RELEASE_ID,
        "authority_sha256": "b" * 64,
        "authority_epoch_id": "authority-test",
        "bootstrap_epoch_id": EPOCH_ID,
        "release_bom_sha256": runtime["release_bom_sha256"],
    }
    config.active_release_binding_path.write_text(
        json.dumps(active_payload, sort_keys=True) + "\n", encoding="utf-8"
    )
    config.active_release_binding_path.chmod(0o600)
    config.live_env_path.write_text("HERMES_TEST=true\n", encoding="utf-8")
    config.live_env_path.chmod(0o600)
    active_raw_sha = dispatcher._bound_exact_hold_source_sha256(
        config.active_release_binding_path
    )
    live_env_sha = dispatcher._bound_exact_hold_source_sha256(config.live_env_path)
    tool_root = (
        Path.home()
        / ".hermes"
        / "runtime"
        / "releases"
        / "hermes-v0.18.2-r15j-host-e75dbebae-git"
    )
    entrypoint = tool_root / "scripts" / "pnc_rca_outbox_dispatcher.py"
    control_path = tool_root / "gateway" / "pnc_rca_control_store.py"
    bootstrap_path = tool_root / "gateway" / "pnc_rca_prod_bootstrap.py"
    monkeypatch.setattr(control_store_module, "__file__", str(control_path))
    head = dispatcher.subprocess.run(
        ["git", "-C", str(tool_root), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    tree = dispatcher.subprocess.run(
        ["git", "-C", str(tool_root), "rev-parse", "HEAD^{tree}"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    tool_provenance = {
        "entrypoint_path": str(entrypoint),
        "entrypoint_sha256": dispatcher._bound_exact_hold_source_sha256(entrypoint),
        "control_store_path": str(control_path),
        "control_store_sha256": dispatcher._bound_exact_hold_source_sha256(control_path),
        "bootstrap_path": str(bootstrap_path),
        "bootstrap_sha256": dispatcher._bound_exact_hold_source_sha256(bootstrap_path),
        "git_head": head,
        "git_tree": tree,
        "git_status_returncode": 0,
        "git_clean": True,
        "runtime_provenance": runtime,
    }
    census = dispatcher._exact_outbox_resident_census(config)
    monkeypatch.setattr(
        dispatcher,
        "_exact_outbox_hold_active_release_binding",
        lambda _config: {
            "path": str(config.active_release_binding_path),
            "sha256": active_raw_sha,
            "release_id": RELEASE_ID,
            "authority_sha256": "b" * 64,
            "authority_epoch_id": "authority-test",
            "bootstrap_epoch_id": EPOCH_ID,
            "release_bom_sha256": runtime["release_bom_sha256"],
            "candidate_env_sha256": "c" * 64,
            "authorization_fingerprint": "d" * 64,
            "authorization_receipt_sha256": "e" * 64,
            "approval_evidence_sha256": "f" * 64,
            "runtime_manifest_sha256": runtime["manifest"]["sha256"],
            "runtime_release_target": runtime["manifest_runtime_release_target"],
            "runtime_git_head": runtime["runtime_git_head"],
            "runtime_git_tree": runtime["runtime_git_tree"],
            "raw_sha256": active_raw_sha,
            "live_env_path": str(config.live_env_path),
            "live_env_sha256": live_env_sha,
        },
    )
    monkeypatch.setattr(
        bootstrap_module,
        "load_active_release_binding",
        lambda **_kwargs: {
            "binding_receipt_sha256": active_raw_sha,
            "release_id": RELEASE_ID,
            "authority_sha256": "b" * 64,
            "authority_epoch_id": "authority-test",
            "bootstrap_epoch_id": EPOCH_ID,
            "release_bom_sha256": runtime["release_bom_sha256"],
            "candidate_env_sha256": "c" * 64,
            "authorization_fingerprint": "d" * 64,
            "authorization_receipt_sha256": "e" * 64,
            "approval_evidence_sha256": "f" * 64,
        },
    )
    monkeypatch.setattr(
        dispatcher,
        "_exact_outbox_hold_tool_provenance",
        lambda: dict(tool_provenance),
    )
    monkeypatch.setattr(
        dispatcher,
        "_exact_outbox_resident_census",
        lambda _config: dict(census),
    )


def _exact_hold_plan(tmp_path, monkeypatch, capsys):
    store = _exact_hold_fixture(tmp_path)
    env = _config_env(tmp_path)
    env["HERMES_RCA_OUTBOX_ACTIVATION_REQUIRED"] = "true"
    config = dispatcher.DispatcherConfig.from_env(env, hermes_home=tmp_path)
    _patch_exact_hold_cli(monkeypatch, config)
    receipt_dir = tmp_path / "receipt-dir"
    receipt_dir.mkdir()
    receipt = receipt_dir / "hold.json"
    args = [
        "--hold-exact-outbox-id",
        "685",
        "--predecessor-outbox-id",
        "684",
        "--operator",
        "owner@example.com",
        "--reason",
        "run success canary before terminal canary",
        "--receipt",
        str(receipt),
    ]
    assert dispatcher.main(args) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["mode"] == "plan"
    return store, config, receipt, args, output


def _resign_exact_hold(audit):
    audit["config_binding_sha256"] = dispatcher._exact_hold_json_sha256(
        audit["config_binding"]
    )
    audit["tool_provenance_sha256"] = dispatcher._exact_hold_json_sha256(
        audit["tool_provenance"]
    )
    audit["plan_id"] = _exact_outbox_hold_plan_id(audit)
    audit["hold_id"] = control_store_module._exact_canonical_sha256(
        {
            "plan_id": audit["plan_id"],
            "recorded_at": audit["recorded_at"],
            "target_after_sha256": audit["target_after"]["row_sha256"],
        }
    )
    audit["receipt_fingerprint"] = _exact_outbox_hold_fingerprint(audit)
    return audit


def _rows_and_meta(store):
    conn = store._connect()
    try:
        return (
            store.list_rows("rca_outbox"),
            [dict(row) for row in conn.execute("SELECT key, value FROM control_meta")],
        )
    finally:
        conn.close()


def test_exact_outbox_hold_plan_is_read_only_and_privacy_light(
    tmp_path, monkeypatch, capsys
):
    store = _exact_hold_fixture(tmp_path)
    env = _config_env(tmp_path)
    env["HERMES_RCA_OUTBOX_ACTIVATION_REQUIRED"] = "true"
    config = dispatcher.DispatcherConfig.from_env(env, hermes_home=tmp_path)
    _patch_exact_hold_cli(monkeypatch, config)
    receipt = tmp_path / "hold.json"
    paths = [
        config.control_db_path,
        config.control_db_path.with_name(config.control_db_path.name + "-wal"),
        config.control_db_path.with_name(config.control_db_path.name + "-shm"),
    ]

    def snapshot_files():
        return {
            str(path): (
                path.stat().st_mode,
                path.stat().st_size,
                path.stat().st_mtime_ns,
                hashlib.sha256(path.read_bytes()).hexdigest(),
            )
            for path in paths
            if path.exists()
        }

    before_files = snapshot_files()
    before_rows = store.list_rows("rca_outbox")
    assert dispatcher.main(
        [
            "--hold-exact-outbox-id",
            "685",
            "--predecessor-outbox-id",
            "684",
            "--operator",
            "owner@example.com",
            "--reason",
            "read-only exact hold plan",
            "--receipt",
            str(receipt),
        ]
    ) == 0

    output = json.loads(capsys.readouterr().out)
    serialized = json.dumps(output, sort_keys=True)
    assert output["plan"]["eligible_queue_before"]["outbox_ids"] == [684, 685]
    assert output["plan"]["active_activation"]["state"] == "bounded_active"
    assert "_row_projection" not in serialized
    assert "payload_json" not in serialized
    assert not receipt.exists()
    assert store.list_rows("rca_outbox") == before_rows
    assert snapshot_files() == before_files


def test_exact_outbox_hold_plan_without_source_sidecars_is_still_read_only(
    tmp_path, monkeypatch, capsys
):
    store = _exact_hold_fixture(tmp_path)
    conn = store._connect()
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        conn.close()
    for suffix in ("-wal", "-shm"):
        sidecar = Path(str(store.db_path) + suffix)
        try:
            sidecar.unlink()
        except FileNotFoundError:
            pass
    env = _config_env(tmp_path)
    env["HERMES_RCA_OUTBOX_ACTIVATION_REQUIRED"] = "true"
    config = dispatcher.DispatcherConfig.from_env(env, hermes_home=tmp_path)
    _patch_exact_hold_cli(monkeypatch, config)
    receipt = tmp_path / "hold-no-sidecar.json"
    before = sorted(path.name for path in tmp_path.iterdir())
    assert dispatcher.main(
        [
            "--hold-exact-outbox-id",
            "685",
            "--predecessor-outbox-id",
            "684",
            "--operator",
            "owner",
            "--reason",
            "plan with no source sidecars",
            "--receipt",
            str(receipt),
        ]
    ) == 0
    capsys.readouterr()
    assert sorted(path.name for path in tmp_path.iterdir()) == before
    assert not receipt.exists()


def test_exact_outbox_hold_apply_mutates_only_schedule_and_audit(
    tmp_path, monkeypatch, capsys
):
    store, _config, receipt, args, output = _exact_hold_plan(
        tmp_path, monkeypatch, capsys
    )
    expected = output["expected_apply"]
    before = {row["outbox_id"]: row for row in store.list_rows("rca_outbox")}
    apply_args = [*args, "--apply"]
    for key, value in expected.items():
        apply_args.extend(["--" + key.replace("_", "-"), value])

    assert dispatcher.main(apply_args) == 0

    result = json.loads(capsys.readouterr().out)
    after = {row["outbox_id"]: row for row in store.list_rows("rca_outbox")}
    assert result["applied"] is True
    assert after[684] == before[684]
    changed = {
        key
        for key in before[685]
        if before[685][key] != after[685][key]
    }
    assert changed == {"next_attempt_at", "updated_at"}
    assert after[685]["next_attempt_at"] == dispatcher.EXACT_OUTBOX_HOLD_UNTIL
    assert after[685]["attempt"] == after[685]["fence"] == 0
    assert after[685]["retry_window_started_at"] == before[685][
        "retry_window_started_at"
    ]
    audit = store.exact_outbox_hold_audit(result["hold_id"])
    assert audit is not None
    assert audit["effect_delta"]["business_trigger_rows_updated"] == 0
    assert receipt.stat().st_mode & 0o777 == 0o444
    claim = store.claim_outbox(
        lease_owner="exact-hold-test",
        activation_required=True,
    )
    assert claim is not None
    assert claim.outbox_id == 684


def test_exact_outbox_hold_does_not_bypass_retry_horizon(
    tmp_path, monkeypatch, capsys
):
    store, _config, _receipt, args, output = _exact_hold_plan(
        tmp_path, monkeypatch, capsys
    )
    apply_args = [*args, "--apply"]
    for key, value in output["expected_apply"].items():
        apply_args.extend(["--" + key.replace("_", "-"), value])
    assert dispatcher.main(apply_args) == 0
    capsys.readouterr()
    [target] = [
        row for row in store.list_rows("rca_outbox") if row["outbox_id"] == 685
    ]
    anchor = datetime.fromisoformat(
        target["retry_window_started_at"] or target["created_at"]
    )

    store.claim_outbox(
        lease_owner="expiry-sweep",
        max_age_seconds=86_400,
        activation_required=True,
        now=anchor + timedelta(seconds=86_401),
    )

    [expired] = [
        row for row in store.list_rows("rca_outbox") if row["outbox_id"] == 685
    ]
    assert expired["status"] == "quarantined"
    assert expired["last_error_code"] == "dispatch_age_exceeded"


def test_exact_outbox_hold_recovery_is_read_only(
    tmp_path, monkeypatch, capsys
):
    store, _config, _receipt, args, output = _exact_hold_plan(
        tmp_path, monkeypatch, capsys
    )
    apply_args = [*args, "--apply"]
    for key, value in output["expected_apply"].items():
        apply_args.extend(["--" + key.replace("_", "-"), value])
    assert dispatcher.main(apply_args) == 0
    applied = json.loads(capsys.readouterr().out)
    before = store.list_rows("rca_outbox")
    recovered = tmp_path / "hold-recovered.json"

    assert dispatcher.main(
        [
            "--materialize-exact-outbox-hold",
            applied["hold_id"],
            "--phase",
            "hold",
            "--receipt",
            str(recovered),
        ]
    ) == 0

    result = json.loads(capsys.readouterr().out)
    envelope = json.loads(recovered.read_text(encoding="utf-8"))
    assert result["recovered"] is True
    assert envelope["source_hold_id"] == applied["hold_id"]
    assert envelope["audit"]["hold_id"] == applied["hold_id"]
    assert envelope["materialized_destination"]["path"] == str(recovered)
    assert store.list_rows("rca_outbox") == before


def test_exact_outbox_hold_public_api_rejects_resigned_config_forge_without_mutation(
    tmp_path, monkeypatch, capsys
):
    store, _config, _receipt, _args, output = _exact_hold_plan(
        tmp_path, monkeypatch, capsys
    )
    audit = copy.deepcopy(output["plan"])
    audit["max_age_seconds"] = 999_999
    audit["config_binding"]["max_age_seconds"] = 999_999
    # Keep the horizon internally coherent so rejection comes from the live
    # configuration boundary rather than a malformed receipt.
    anchor = datetime.fromisoformat(audit["retry_horizon"]["anchor"])
    expires = anchor + timedelta(seconds=999_999)
    audit["retry_horizon"]["expires_at"] = expires.isoformat()
    audit["retry_horizon"]["plan_remaining_seconds"] = int(
        (expires - datetime.fromisoformat(audit["recorded_at"])).total_seconds()
    )
    _resign_exact_hold(audit)
    before = _rows_and_meta(store)
    with pytest.raises(RuntimeError, match="exact_outbox_hold_config_changed"):
        store.hold_exact_outbox_with_audit(audit=audit)
    assert _rows_and_meta(store) == before


def test_exact_outbox_hold_public_api_rejects_alternate_tool_clone_without_mutation(
    tmp_path, monkeypatch, capsys
):
    store, _config, _receipt, _args, output = _exact_hold_plan(
        tmp_path, monkeypatch, capsys
    )
    audit = copy.deepcopy(output["plan"])
    alternate = tmp_path / "alternate-tool"
    (alternate / "scripts").mkdir(parents=True)
    (alternate / "gateway").mkdir()
    bindings = {
        "entrypoint": alternate / "scripts" / "pnc_rca_outbox_dispatcher.py",
        "control_store": alternate / "gateway" / "pnc_rca_control_store.py",
        "bootstrap": alternate / "gateway" / "pnc_rca_prod_bootstrap.py",
    }
    for index, path in enumerate(bindings.values()):
        path.write_text(f"# alternate {index}\n", encoding="utf-8")
        path.chmod(0o600)
    for name, path in bindings.items():
        audit["tool_provenance"][f"{name}_path"] = str(path)
        audit["tool_provenance"][f"{name}_sha256"] = hashlib.sha256(
            path.read_bytes()
        ).hexdigest()
    _resign_exact_hold(audit)
    before = _rows_and_meta(store)
    with pytest.raises(RuntimeError, match="exact_outbox_hold_tool_provenance_invalid"):
        store.hold_exact_outbox_with_audit(audit=audit)
    assert _rows_and_meta(store) == before


def test_exact_outbox_hold_public_api_rejects_resigned_release_forge_without_mutation(
    tmp_path, monkeypatch, capsys
):
    store, _config, _receipt, _args, output = _exact_hold_plan(
        tmp_path, monkeypatch, capsys
    )
    audit = copy.deepcopy(output["plan"])
    audit["active_release_binding"]["release_id"] = "rca-v0182-forged-release"
    _resign_exact_hold(audit)
    before = _rows_and_meta(store)
    with pytest.raises(
        RuntimeError,
        match="exact_outbox_hold_(?:config|active_binding)_changed",
    ):
        store.hold_exact_outbox_with_audit(audit=audit)
    assert _rows_and_meta(store) == before


def test_exact_outbox_hold_public_api_rejects_activation_db_binding_corruption(
    tmp_path, monkeypatch, capsys
):
    store, _config, _receipt, _args, output = _exact_hold_plan(
        tmp_path, monkeypatch, capsys
    )
    conn = store._connect()
    try:
        conn.execute(
            "UPDATE rca_activation_epochs SET db_logical_identity_sha256 = ? "
            "WHERE is_current = 1",
            ("1" * 64,),
        )
    finally:
        conn.close()
    before = _rows_and_meta(store)
    with pytest.raises(
        RuntimeError,
        match="exact_outbox_hold_(?:control_db_provenance_changed|activation_db_binding_invalid)",
    ):
        store.hold_exact_outbox_with_audit(audit=output["plan"])
    assert _rows_and_meta(store) == before


def test_exact_outbox_hold_public_api_recensuses_loaded_writer_without_mutation(
    tmp_path, monkeypatch, capsys
):
    store, _config, _receipt, _args, output = _exact_hold_plan(
        tmp_path, monkeypatch, capsys
    )
    real_run = control_store_module.subprocess.run

    def loaded_after_plan(command, *args, **kwargs):
        if (
            command[:2] == ["launchctl", "print"]
            and command[2].endswith("/local.pnc.rca-outbox-dispatcher")
        ):
            return control_store_module.subprocess.CompletedProcess(
                command, 0, stdout="loaded\n", stderr=""
            )
        return real_run(command, *args, **kwargs)

    monkeypatch.setattr(control_store_module.subprocess, "run", loaded_after_plan)
    before = _rows_and_meta(store)
    with pytest.raises(RuntimeError, match="exact_outbox_hold_forbidden_resident_loaded"):
        store.hold_exact_outbox_with_audit(audit=output["plan"])
    assert _rows_and_meta(store) == before


def test_exact_outbox_hold_public_api_rejects_destination_parent_forge_without_mutation(
    tmp_path, monkeypatch, capsys
):
    store, _config, _receipt, _args, output = _exact_hold_plan(
        tmp_path, monkeypatch, capsys
    )
    audit = copy.deepcopy(output["plan"])
    audit["destination_binding"]["parent_inode"] += 1
    _resign_exact_hold(audit)
    before = _rows_and_meta(store)
    with pytest.raises(RuntimeError, match="exact_outbox_hold_destination_parent_changed"):
        store.hold_exact_outbox_with_audit(audit=audit)
    assert _rows_and_meta(store) == before


def test_exact_outbox_hold_public_api_rejects_nan_without_mutation(
    tmp_path, monkeypatch, capsys
):
    store, _config, _receipt, _args, output = _exact_hold_plan(
        tmp_path, monkeypatch, capsys
    )
    audit = copy.deepcopy(output["plan"])
    audit["tool_provenance"]["forged_nan"] = float("nan")
    before = _rows_and_meta(store)
    with pytest.raises(ValueError, match="JSON compliant|exact_outbox_hold"):
        store.hold_exact_outbox_with_audit(audit=audit)
    assert _rows_and_meta(store) == before


def test_exact_outbox_hold_public_api_rejects_stale_recorded_at_without_mutation(
    tmp_path, monkeypatch, capsys
):
    store, _config, _receipt, _args, output = _exact_hold_plan(
        tmp_path, monkeypatch, capsys
    )
    audit = copy.deepcopy(output["plan"])
    stale = datetime.now(timezone.utc) - timedelta(
        seconds=dispatcher.EXACT_OUTBOX_HOLD_RECORD_MAX_AGE_SECONDS + 1
    )
    audit["recorded_at"] = stale.isoformat()
    with pytest.raises(RuntimeError, match="exact_outbox_hold_recorded_at_stale"):
        RcaControlStore._exact_hold_freshness(audit, datetime.now(timezone.utc))
    before = _rows_and_meta(store)
    with pytest.raises((ValueError, RuntimeError), match="exact_outbox_hold"):
        store.hold_exact_outbox_with_audit(audit=audit)
    assert _rows_and_meta(store) == before


def test_exact_outbox_hold_public_api_rejects_expiry_margin_without_mutation(
    tmp_path, monkeypatch, capsys
):
    store, _config, _receipt, _args, output = _exact_hold_plan(
        tmp_path, monkeypatch, capsys
    )
    audit = copy.deepcopy(output["plan"])
    expires = datetime.fromisoformat(audit["retry_horizon"]["expires_at"])
    fake_now = expires - timedelta(
        seconds=EXACT_OUTBOX_HOLD_MIN_REMAINING_SECONDS - 0.001
    )
    audit["recorded_at"] = (fake_now - timedelta(microseconds=100)).isoformat()
    fresh_audit = copy.deepcopy(audit)
    fresh_audit["recorded_at"] = (fake_now - timedelta(seconds=1)).isoformat()
    with pytest.raises(RuntimeError, match="exact_outbox_hold_retry_horizon_headroom_insufficient"):
        RcaControlStore._exact_hold_freshness(
            fresh_audit,
            fake_now,
        )
    real_utc = control_store_module._utc_datetime
    monkeypatch.setattr(
        control_store_module,
        "_utc_datetime",
        lambda value=None: fake_now if value is None else real_utc(value),
    )
    before = _rows_and_meta(store)
    with pytest.raises((ValueError, RuntimeError), match="exact_outbox_hold"):
        store.hold_exact_outbox_with_audit(audit=audit)
    assert _rows_and_meta(store) == before


def test_exact_outbox_hold_recovery_survives_removed_or_swapped_receipt_parent(
    tmp_path, monkeypatch, capsys
):
    store, _config, receipt, args, output = _exact_hold_plan(
        tmp_path, monkeypatch, capsys
    )
    apply_args = [*args, "--apply"]
    for key, value in output["expected_apply"].items():
        apply_args.extend(["--" + key.replace("_", "-"), value])
    assert dispatcher.main(apply_args) == 0
    applied = json.loads(capsys.readouterr().out)
    original_parent = receipt.parent
    moved_parent = tmp_path / "receipt-dir-moved"
    original_parent.rename(moved_parent)
    original_parent.mkdir()
    assert store.exact_outbox_hold_audit(applied["hold_id"]) is not None
    recovered = original_parent / "recovered.json"
    assert dispatcher.main(
        [
            "--materialize-exact-outbox-hold",
            applied["hold_id"],
            "--phase",
            "hold",
            "--receipt",
            str(recovered),
        ]
    ) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["recovered"] is True
    assert recovered.exists()


def test_clear_circuit_plan_does_not_mutate_or_create_receipt(
    tmp_path, monkeypatch, capsys
):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )
    store = RcaControlStore(config.control_db_path)
    store.open_dispatcher_circuit(
        reason_code="snapshot_stale", reason_detail="offline test"
    )
    receipt = tmp_path / "reset.json"
    _patch_reset_cli(monkeypatch, config)

    assert dispatcher.main(
        [
            "--clear-circuit",
            "--operator",
            "owner@example.com",
            "--reason",
            "verify snapshot before rearm",
            "--receipt",
            str(receipt),
        ]
    ) == 0

    result = json.loads(capsys.readouterr().out)
    assert result["mode"] == "plan"
    assert result["applied"] is False
    assert result["pre_state"]["state"] == "open"
    assert store.dispatcher_circuit().state == "open"
    assert not receipt.exists()


def test_clear_circuit_apply_writes_receipt_and_db_audit(
    tmp_path, monkeypatch, capsys
):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )
    store = RcaControlStore(config.control_db_path)
    store.open_dispatcher_circuit(
        reason_code="snapshot_stale", reason_detail="offline test"
    )
    receipt = tmp_path / "reset.json"
    _patch_reset_cli(monkeypatch, config)

    assert dispatcher.main(
        [
            "--clear-circuit",
            "--operator",
            "owner@example.com",
            "--reason",
            "verify snapshot before rearm",
            "--apply",
            "--receipt",
            str(receipt),
        ]
    ) == 0

    result = json.loads(capsys.readouterr().out)
    body = json.loads(receipt.read_text(encoding="utf-8"))
    assert result["applied"] is True
    assert result["receipt"] == str(receipt.absolute())
    assert result["receipt_sha256"]
    assert body["operator"] == "owner@example.com"
    assert body["reason"] == "verify snapshot before rearm"
    assert body["pre_state"]["state"] == "open"
    assert body["post_state"]["state"] == "closed"
    assert body["effect_delta"]["external_writes"] == 0
    assert body["receipt_fingerprint"] == canonical_json_sha256(
        {key: value for key, value in body.items() if key != "receipt_fingerprint"}
    )
    assert receipt.stat().st_mode & 0o777 == 0o444
    sidecar = receipt.with_name(receipt.name + ".sha256")
    assert sidecar.read_text(encoding="ascii").startswith(result["receipt_sha256"])
    assert store.dispatcher_circuit().state == "closed"
    assert store.dispatcher_circuit_reset_audit(body["reset_id"]) == body


def test_clear_circuit_duplicate_receipt_is_rejected_before_mutation(
    tmp_path, monkeypatch, capsys
):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )
    store = RcaControlStore(config.control_db_path)
    store.open_dispatcher_circuit(reason_code="snapshot_stale")
    receipt = tmp_path / "reset.json"
    _patch_reset_cli(monkeypatch, config)
    args = [
        "--clear-circuit",
        "--operator",
        "owner",
        "--reason",
        "first reset",
        "--apply",
        "--receipt",
        str(receipt),
    ]
    assert dispatcher.main(args) == 0
    capsys.readouterr()

    # Re-open the circuit to prove the duplicate path is checked before the
    # second mutation attempt.
    store.open_dispatcher_circuit(reason_code="reopened-for-negative-test")
    assert dispatcher.main(args) == 2
    assert "already_exists" in capsys.readouterr().err
    assert store.dispatcher_circuit().state == "open"


def test_clear_circuit_closed_state_fails_closed_without_plan_success(
    tmp_path, monkeypatch, capsys
):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )
    store = RcaControlStore(config.control_db_path)
    _patch_reset_cli(monkeypatch, config)

    assert dispatcher.main(
        [
            "--clear-circuit",
            "--operator",
            "owner",
            "--reason",
            "do not reset a closed circuit",
        ]
    ) == 2
    assert "requires_open_circuit" in capsys.readouterr().err
    assert store.dispatcher_circuit().state == "closed"


def test_clear_circuit_receipt_materialization_failure_reports_recovery(
    tmp_path, monkeypatch, capsys
):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )
    store = RcaControlStore(config.control_db_path)
    store.open_dispatcher_circuit(reason_code="snapshot_stale")
    receipt = tmp_path / "reset.json"
    _patch_reset_cli(monkeypatch, config)
    writer = dispatcher._write_immutable_receipt

    def fail_materialization(*_args, **_kwargs):
        raise OSError("simulated receipt filesystem failure")

    monkeypatch.setattr(dispatcher, "_write_immutable_receipt", fail_materialization)
    assert dispatcher.main(
        [
            "--clear-circuit",
            "--operator",
            "owner",
            "--reason",
            "persist database audit before filesystem copy",
            "--apply",
            "--receipt",
            str(receipt),
        ]
    ) == 2
    error = json.loads(capsys.readouterr().err)
    assert error["recovery_required"] is True
    assert error["meta_key"].startswith("rca_dispatcher_circuit_reset:")
    assert store.dispatcher_circuit().state == "closed"
    assert store.dispatcher_circuit_reset_audit(error["reset_id"]) is not None
    monkeypatch.setattr(dispatcher, "_write_immutable_receipt", writer)
    recovered = tmp_path / "recovered-reset.json"
    assert dispatcher.main(
        [
            "--materialize-reset",
            error["reset_id"],
            "--receipt",
            str(recovered),
        ]
    ) == 0
    recovery_result = json.loads(capsys.readouterr().out)
    assert recovery_result["recovered"] is True
    assert json.loads(recovered.read_text(encoding="utf-8"))["reset_id"] == error[
        "reset_id"
    ]


def test_clear_circuit_rejects_relative_receipt_before_mutation(
    tmp_path, monkeypatch, capsys
):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )
    store = RcaControlStore(config.control_db_path)
    store.open_dispatcher_circuit(reason_code="snapshot_stale")
    _patch_reset_cli(monkeypatch, config)

    assert dispatcher.main(
        [
            "--clear-circuit",
            "--operator",
            "owner",
            "--reason",
            "absolute path required",
            "--apply",
            "--receipt",
            "relative-reset.json",
        ]
    ) == 2
    assert "path_invalid" in capsys.readouterr().err
    assert store.dispatcher_circuit().state == "open"


@pytest.mark.parametrize(
    "extra,expected",
    [
        (["--reason", "reason"], "operator_and_reason_required"),
        (["--operator", "operator"], "operator_and_reason_required"),
        (["--operator", "operator", "--reason", "reason", "--apply"], "receipt_required"),
    ],
)
def test_clear_circuit_apply_requires_bounded_audit_inputs(
    tmp_path, monkeypatch, capsys, extra, expected
):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )
    store = RcaControlStore(config.control_db_path)
    store.open_dispatcher_circuit(reason_code="snapshot_stale")
    _patch_reset_cli(monkeypatch, config)

    assert dispatcher.main(["--clear-circuit", *extra]) == 2
    assert expected in capsys.readouterr().err
    assert store.dispatcher_circuit().state == "open"


def test_config_fails_closed_without_production_capacity_mode(tmp_path):
    env = _config_env(tmp_path)
    env.pop("HERMES_RCA_PROD_CAPACITY_MODE")

    with pytest.raises(ValueError, match="must be exactly steady or bootstrap"):
        dispatcher.DispatcherConfig.from_env(env, hermes_home=tmp_path)


def test_steady_capacity_uses_per_task_live_resource_contract(tmp_path):
    env = _config_env(tmp_path)
    env["HERMES_RCA_PROD_CAPACITY_MODE"] = "steady"

    config = dispatcher.DispatcherConfig.from_env(env, hermes_home=tmp_path)
    assert config.capacity_mode == "steady"


def test_default_submit_steady_does_not_load_bootstrap_authorization(
    monkeypatch, tmp_path
):
    calls = []
    monkeypatch.setattr(
        "tools.vm_task_tool.vm_task_submit_service",
        lambda **kwargs: calls.append(kwargs) or {"success": True},
    )
    monkeypatch.setattr(
        dispatcher,
        "_load_bound_bootstrap_authorization",
        lambda _config: pytest.fail("steady mode must not load bootstrap authority"),
    )
    env = _config_env(tmp_path)
    env["HERMES_RCA_PROD_CAPACITY_MODE"] = "steady"
    config = dispatcher.DispatcherConfig.from_env(env, hermes_home=tmp_path)
    dispatcher.default_submit(
        object(), SimpleNamespace(toolchain={}), config=config
    )
    assert calls[0]["capacity_mode"] == "steady"
    assert "bootstrap_deadline" not in calls[0]


def test_default_submit_uses_bound_bootstrap_contract(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(
        "tools.vm_task_tool.vm_task_submit_service",
        lambda **kwargs: calls.append(kwargs) or {"success": True},
    )
    monkeypatch.setattr(
        dispatcher, "_load_bound_bootstrap_authorization", lambda _config: _authorization()
    )
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )
    admission = object()
    request = SimpleNamespace(toolchain={})

    assert dispatcher.default_submit(admission, request, config=config) == {
        "success": True
    }
    assert calls == [
        {
            "service_id": dispatcher.DEFAULT_SERVICE_ID,
            "capability": dispatcher.SERVICE_CAPABILITY,
            "operation": dispatcher.SERVICE_OPERATION,
            "admission": admission,
            "execution_request": request,
            "reconcile_only": False,
            "capacity_mode": "bootstrap",
            "bootstrap_epoch_id": EPOCH_ID,
            "release_bom_sha256": "5" * 64,
            "bootstrap_started_at": "2026-07-20T00:00:00+00:00",
            "bootstrap_deadline": "2026-07-24T00:00:00+00:00",
            "bootstrap_authorization_fingerprint": "1" * 64,
            "active_release_binding_sha256": "3" * 64,
        }
    ]


def test_default_submit_fails_before_vm_boundary_on_binding_error(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(
        "tools.vm_task_tool.vm_task_submit_service",
        lambda **_kwargs: pytest.fail("invalid bootstrap binding reached VM boundary"),
    )

    def invalid(_config):
        raise RcaBootstrapAuthorizationError("rca_active_release_binding_invalid")

    monkeypatch.setattr(dispatcher, "_load_bound_bootstrap_authorization", invalid)
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )

    with pytest.raises(dispatcher.DispatchCircuitError) as raised:
        dispatcher.default_submit(
            object(), SimpleNamespace(toolchain={}), config=config
        )
    assert raised.value.code == "dispatcher_bootstrap_authorization_invalid"
    assert raised.value.detail == "rca_active_release_binding_invalid"


def test_default_submit_preserves_reconcile_only_dedupe(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(
        "tools.vm_task_tool.vm_task_submit_service",
        lambda **kwargs: calls.append(kwargs) or {"success": True},
    )
    monkeypatch.setattr(
        dispatcher, "_load_bound_bootstrap_authorization", lambda _config: _authorization()
    )
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )
    request = SimpleNamespace(
        toolchain={"derived_capacity_reservation": {"status": "released"}}
    )

    dispatcher.default_submit(object(), request, config=config)

    assert calls[0]["reconcile_only"] is True


def test_default_submit_defers_live_store_check_to_service_create_guard(
    monkeypatch, tmp_path
):
    submit_calls = []
    fence_checks = []
    monkeypatch.setattr(
        "tools.vm_task_tool.vm_task_submit_service",
        lambda **kwargs: submit_calls.append(kwargs) or {"success": True},
    )
    monkeypatch.setattr(
        dispatcher, "_load_bound_bootstrap_authorization", lambda _config: _authorization()
    )
    monkeypatch.setattr(
        dispatcher,
        "validate_snapshot_execution_bundle",
        lambda _value: SimpleNamespace(snapshot="immutable-bundle"),
    )
    monkeypatch.setattr(
        dispatcher,
        "_validate_vm_submit_fence",
        lambda **kwargs: fence_checks.append(kwargs),
    )
    config = replace(
        dispatcher.DispatcherConfig.from_env(
            _config_env(tmp_path), hermes_home=tmp_path
        ),
        w3_snapshot_read_mode="snapshot_required",
    )
    live_binding = {"epoch_id": "epoch-live", "ledger_id": 17}
    store = SimpleNamespace(
        validate_external_write_fence_binding=lambda _fence: live_binding
    )

    dispatcher.default_submit(
        object(),
        SimpleNamespace(toolchain={"w3_execution_snapshot": {"fixture": True}}),
        config=config,
        control_store=store,
    )

    assert len(fence_checks) == 1
    assert "control_store" not in fence_checks[0]
    authority = submit_calls[0]["live_write_fence_authority"]
    assert authority({"state": "issued"}) == live_binding


def test_capacity_health_projection_is_release_bound(tmp_path):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path, enabled=True), hermes_home=tmp_path
    )
    reporter = object.__new__(dispatcher.HealthReporter)
    reporter.config = config
    reporter._bootstrap_authorization_observer = _authorization
    reporter._admission_key_fingerprint_observer = lambda: "7" * 64

    status = reporter.capacity_admission_status()

    assert status["required"] is True
    assert status["ready"] is True
    assert status["authorization"]["release_approval_id"] == RELEASE_ID
    assert dispatcher._health_capacity_admission_ok(
        {
            "enabled": True,
            "config": config.public_dict(),
            "capacity_admission": status,
        }
    )


def test_steady_health_requires_hmac_and_has_no_expiring_authorization(tmp_path):
    env = _config_env(tmp_path, enabled=True)
    env["HERMES_RCA_PROD_CAPACITY_MODE"] = "steady"
    config = dispatcher.DispatcherConfig.from_env(env, hermes_home=tmp_path)
    reporter = object.__new__(dispatcher.HealthReporter)
    reporter.config = config
    reporter._admission_key_fingerprint_observer = lambda: "7" * 64

    status = reporter.capacity_admission_status()

    assert status["ready"] is True
    assert status["capacity_mode"] == "steady"
    assert "deadline" not in status["authorization"]
    assert "successful_sample_count" not in status["authorization"]
    assert dispatcher._health_capacity_admission_ok(
        {
            "enabled": True,
            "config": config.public_dict(),
            "capacity_admission": status,
        }
    )


def test_capacity_guard_opens_circuit_when_bootstrap_binding_is_unavailable(tmp_path):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path, enabled=True), hermes_home=tmp_path
    )
    opened = []
    reporter = object.__new__(dispatcher.HealthReporter)
    reporter.config = config
    reporter.store = SimpleNamespace(
        open_dispatcher_circuit=lambda **kwargs: opened.append(kwargs)
    )
    reporter.workspace_runtime_status = lambda: {"required": True, "ready": True}
    reporter._admission_key_fingerprint_observer = lambda: "7" * 64

    def unavailable():
        raise RcaBootstrapAuthorizationError("rca_bootstrap_expired_or_deadline_invalid")

    reporter._bootstrap_authorization_observer = unavailable

    outcome = reporter.dispatch_guard_outcome()

    assert outcome.status == "capacity_authorization_unavailable"
    assert outcome.error_code == "rca_bootstrap_expired_or_deadline_invalid"
    assert opened == [
        {
            "reason_code": "dispatcher_prod_admission_invalid",
            "reason_detail": "rca_bootstrap_expired_or_deadline_invalid",
        }
    ]


def test_capacity_guard_fails_before_claim_when_admission_key_is_invalid(tmp_path):
    config = dispatcher.DispatcherConfig.from_env(
        _config_env(tmp_path, enabled=True), hermes_home=tmp_path
    )
    opened = []
    reporter = object.__new__(dispatcher.HealthReporter)
    reporter.config = config
    reporter.store = SimpleNamespace(
        open_dispatcher_circuit=lambda **kwargs: opened.append(kwargs)
    )
    reporter.workspace_runtime_status = lambda: {"required": True, "ready": True}
    reporter._bootstrap_authorization_observer = _authorization

    def invalid_key():
        raise dispatcher.RcaProdAdmissionError(
            "rca_prod_hmac_key_invalid", retryable=False
        )

    reporter._admission_key_fingerprint_observer = invalid_key

    outcome = reporter.dispatch_guard_outcome()

    assert outcome.status == "capacity_authorization_unavailable"
    assert outcome.error_code == "rca_prod_hmac_key_invalid"
    assert opened[0]["reason_code"] == "dispatcher_prod_admission_invalid"


def test_runtime_closure_includes_prod_admission_and_capacity_sampling():
    assert {
        "gateway/__init__.py",
        "gateway/pnc_rca_prod_admission.py",
        "gateway/pnc_rca_prod_bootstrap.py",
        "gateway/record_only/runtime.py",
        "gateway/record_only/transport.py",
        "scripts/pnc_completion_notice_relay.py",
        "scripts/pnc_foxglove_delivery.py",
        "scripts/pnc_vm_task_sync.py",
        "tools/__init__.py",
    }.issubset(RCA_RUNTIME_RELATIVE_FILES)
    assert {
        "gateway/pnc_rca_capacity_runtime.py",
        "gateway/pnc_rca_capacity_sample_evidence.py",
        "gateway/pnc_rca_capacity_transition.py",
        "scripts/pnc_rca_activation.py",
        "scripts/pnc_rca_capacity_transition_executor.py",
    }.issubset(RCA_RUNTIME_RELATIVE_FILES)
    assert (
        "vm_task_service_rca_prod_admission_blocked"
        in dispatcher._SERVICE_ERROR_CODES
    )
    assert (
        "vm_task_service_rca_prod_admission_blocked"
        in dispatcher._GLOBAL_CIRCUIT_ERROR_CODES
    )


def test_vm_submit_fence_normalizes_stale_control_store_conflict(monkeypatch):
    fence = {"state": "issued", "activation_epoch_id": "epoch-old"}
    bundle = SimpleNamespace(
        snapshot=SimpleNamespace(write_fence=fence),
        creator_source_envelope=SimpleNamespace(),
    )
    monkeypatch.setattr(
        dispatcher,
        "validate_snapshot_execution_bundle",
        lambda value: value,
    )
    monkeypatch.setattr(
        dispatcher,
        "validate_write_fence_source_binding",
        lambda *_args, **_kwargs: {
            "issue_target": "issue",
            "thread_target": None,
            "chat_id": "chat",
            "target_set_sha256": "1" * 64,
        },
    )
    monkeypatch.setattr(dispatcher, "validate_write_fence", lambda *_args, **_kwargs: None)

    class StaleStore:
        def validate_external_write_fence_binding(self, _fence):
            raise dispatcher.RecordConflictError(
                "external_write_fence_epoch_not_current"
            )

    with pytest.raises(dispatcher.ExternalWriteFenceError) as raised:
        dispatcher._validate_vm_submit_fence(
            bundle=bundle,
            admission=SimpleNamespace(
                submission_key="submission",
                business_key="business",
                generation=1,
            ),
            now=datetime.now(timezone.utc),
            control_store=StaleStore(),
        )

    assert raised.value.code == "external_write_fence_epoch_not_current"


@pytest.mark.parametrize(
    "error_code",
    [
        "vm_task_service_rca_prod_admission_blocked",
        "vm_task_service_capacity_mode_invalid",
        "vm_task_service_bootstrap_binding_invalid",
        "vm_task_service_rca_prod_command_invalid",
    ],
)
def test_prod_admission_failure_routes_to_global_circuit(monkeypatch, error_code):
    assert error_code in dispatcher._SERVICE_ERROR_CODES
    assert error_code in dispatcher._GLOBAL_CIRCUIT_ERROR_CODES
    instance = object.__new__(dispatcher.OutboxDispatcher)
    claim = object()
    opened = []
    monkeypatch.setattr(
        instance,
        "_open_circuit",
        lambda actual_claim, code, detail: opened.append(
            (actual_claim, code, detail)
        )
        or "opened",
    )
    monkeypatch.setattr(
        instance,
        "_retry",
        lambda *_args, **_kwargs: pytest.fail("global admission failure was retried"),
    )
    monkeypatch.setattr(
        instance,
        "_quarantine",
        lambda *_args, **_kwargs: pytest.fail(
            "global admission failure quarantined one issue"
        ),
    )

    result = instance._handle_dispatch_error(
        claim,
        error_code,
        "rca_prod_hmac_key_invalid",
    )

    assert result == "opened"
    assert opened == [
        (
            claim,
            error_code,
            "rca_prod_hmac_key_invalid",
        )
    ]
