from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta
import hashlib
import io
import json
import os
from pathlib import Path
import plistlib
import sqlite3
import threading
import time
from types import SimpleNamespace

import pytest

from gateway.pnc_rca_abstention_projection import (
    build_gate_a_identifier_binding,
    build_gate_a_public_result,
    project_gate_a_report as _project_gate_a_report,
)
from gateway.pnc_rca_delivery_contract import (
    DELIVERY_CONTRACT_SCHEMA_VERSION,
    DELIVERY_EFFECT_KIND,
    DELIVERY_EFFECT_SCHEMA_VERSION_V1,
    TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_V1,
    TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION,
    DeliveryContractError,
    build_report_url,
    build_terminal_delivery,
)
from gateway.pnc_rca_control_store import RcaControlStore
from gateway.pnc_rca_provider_fence import build_write_fence_provider_claim
from gateway.pnc_rca_delivery_store import (
    DeliveryDispatcherCircuit,
    DeliveryObservationIntent,
    RcaDeliveryStore,
    StaleDeliveryEffectLeaseError,
)
from gateway.pnc_rca_delivery_observability import (
    OBSERVATION_SCHEMA_VERSION,
    DeliveryObservationError,
    append_delivery_observation,
)
from gateway.pnc_rca_runtime_identity import GATEWAY_LOADED_DEPENDENCIES
from scripts import pnc_rca_delivery_dispatcher as dispatcher_module
from scripts.pnc_rca_delivery_collector import CollectorConfig, DeliveryCollector
from scripts.pnc_rca_delivery_dispatcher import (
    DeliveryDispatcher,
    DispatcherConfig,
    LEASE_BOUNDARY_MARGIN_SECONDS,
    MAX_EXTERNAL_BOUNDARY_TIMEOUT_SECONDS,
    MAX_MEEGLE_COMMENT_PAGES,
    MAX_MEEGLE_COMMENTS,
    FeishuThreadReplyAdapter,
    MeegleIssueCommentAdapter,
    default_report_verifier,
    read_health,
    retry_delay_seconds,
    run_dispatch_loop,
)
from scripts.pnc_foxglove_delivery import canonical_viz_mcap_path, foxglove_url
from tests.gateway.test_pnc_rca_delivery_store import (
    NOW,
    _activate_direct_steady,
    _bind_activation_execution,
    _control,
    _insert_subscription,
    _switch_activation_epoch,
)
from tests.gateway.test_pnc_rca_delivery_contract import (
    _add_structural_candidate,
    _bundle,
    _consumer_capability,
    _focus_payload,
)
from tests.gateway.test_pnc_rca_write_fence import _release_note
from gateway.pnc_rca_issue_focus import ANALYSIS_INSUFFICIENT_STATEMENT


_TEST_PROVIDER_WRITE_CLAIM = build_write_fence_provider_claim({"state": "issued"})


def _bind_minimal_release(control, fixture, *, epoch_id="delivery-epoch-1"):
    fixture.epoch["epoch_id"] = epoch_id
    with sqlite3.connect(control.db_path) as conn:
        updated = conn.execute(
            "UPDATE rca_activation_epochs "
            "SET state = 'steady_active', config_sha256 = ?, "
            "production_fingerprint = ?, production_gate_receipt_sha256 = ? "
            "WHERE epoch_id = ? AND is_current = 1",
            (
                fixture.env_sha256,
                fixture.fingerprint,
                fixture.epoch["production_gate_receipt_sha256"],
                epoch_id,
            ),
        )
    assert updated.rowcount == 1


def _set_live_release_environment(monkeypatch, fixture):
    monkeypatch.setenv("PNC_LIVE_RUNTIME_ROOT", str(fixture.runtime_root))
    monkeypatch.setenv("PNC_LIVE_RUNTIME_COMMIT", fixture.runtime_commit)
    monkeypatch.setenv("PNC_LIVE_RUNTIME_TREE", fixture.runtime_tree)
    monkeypatch.setenv("PNC_LIVE_MANIFEST_SHA256", fixture.manifest_sha256)
REAL_G1Q3_PROJECT_KEY = "68ef617fb371dc80a10641f7"
REAL_G1Q3_PROJECT_SIMPLE_NAME = "t03o4q"


def project_gate_a_report(source):
    evaluators = source.get("rca_evaluators") or []
    signals = {
        reference["signal"]
        for evaluator in evaluators
        for reference in evaluator.get("evidence_refs") or []
        if reference.get("signal") is not None
    }
    fields = {
        field
        for evaluator in evaluators
        for reference in evaluator.get("evidence_refs") or []
        for field in (
            [reference["field"]] if reference.get("field") is not None else []
        ) + list(reference.get("fields") or [])
    }
    binding = (
        build_gate_a_identifier_binding({
            "actual_evaluators": [
                {
                    "evaluator_id": evaluator.get("key"),
                    "status": evaluator.get("status"),
                }
                for evaluator in evaluators
            ],
            "actual_signals": sorted(signals),
            "actual_fields": sorted(fields),
        })
        if evaluators
        else None
    )
    return _project_gate_a_report(source, identifier_binding=binding)


def _test_provider_revalidate(
    _claim,
    *,
    operation: str,
    chat_id: str = "",
    thread_id: str = "",
    reply_to_message_id: str = "",
    issue_project_key: str = "",
    issue_work_item_id: str = "",
):
    binding = {
        "epoch_id": "epoch-test-active",
        "state": "steady_active",
        "ledger_id": 1,
    }
    if operation == "feishu_thread_reply":
        binding.update({
            "chat_id": "oc_group123",
            "thread_id": thread_id,
        })
    return binding


@pytest.fixture(autouse=True)
def _seal_dependencies_available_in_the_test_interpreter(monkeypatch):
    monkeypatch.setenv("PNC_FOXGLOVE_RENDER_HOST", "https://viewer.internal")
    monkeypatch.setattr(
        dispatcher_module,
        "revalidate_provider_write_claim",
        _test_provider_revalidate,
    )
    original = dispatcher_module.build_runtime_identity

    def build_test_identity(**kwargs):
        kwargs["loaded_dependencies"] = GATEWAY_LOADED_DEPENDENCIES
        identity = original(**kwargs)
        return replace(
            identity,
            process_create_time=NOW.timestamp() - 10,
            boot_time=NOW.timestamp() - 1_000,
        )

    monkeypatch.setattr(
        dispatcher_module,
        "build_runtime_identity",
        build_test_identity,
    )


def test_delivery_dispatcher_main_disables_dotenv_interpolation(monkeypatch):
    calls = []

    def observe(*args, **kwargs):
        calls.append((args, kwargs))

    def invalid_config(*_args, **_kwargs):
        raise ValueError("stop-after-env-load")

    monkeypatch.setattr(dispatcher_module, "load_dotenv", observe)
    monkeypatch.setattr(dispatcher_module.DispatcherConfig, "from_env", invalid_config)

    assert dispatcher_module.main(["--check-config"]) == 2
    assert calls[0][1] == {"override": False, "interpolate": False}


def test_enabled_resident_without_epoch_exits_before_provider_creation(
    tmp_path,
    monkeypatch,
    capsys,
):
    config = _config(tmp_path, enabled=True)
    control = RcaControlStore(config.control_db_path)
    delivery = RcaDeliveryStore(config.control_db_path)
    provider_created = False

    def unexpected_provider(*_args, **_kwargs):
        nonlocal provider_created
        provider_created = True
        raise AssertionError("provider must not start without an active epoch")

    monkeypatch.setattr(
        dispatcher_module, "load_delivery_dispatcher_environment", lambda: None
    )
    monkeypatch.setattr(
        dispatcher_module.DispatcherConfig,
        "from_env",
        classmethod(lambda _cls: config),
    )
    monkeypatch.setattr(
        dispatcher_module, "MeegleIssueCommentAdapter", unexpected_provider
    )

    assert dispatcher_module.main(["--once"]) == 2
    assert provider_created is False
    assert delivery.list_rows("rca_delivery_effects") == []
    assert "resident_activation_epoch_missing" in capsys.readouterr().out


def test_enabled_startup_locks_minimal_release_before_writable_store_and_provider(
    tmp_path,
    monkeypatch,
):
    fixture = _release_note(tmp_path)
    control, result = _control(tmp_path, db_path=fixture.control_db_path)
    _bind_activation_execution(control, result, state="steady_active")
    RcaDeliveryStore(control.db_path)
    _bind_minimal_release(control, fixture)
    _set_live_release_environment(monkeypatch, fixture)
    config = replace(
        _config(tmp_path),
        control_db_path=control.db_path,
        release_note_path=fixture.path,
    )
    events = []
    captured = {}
    real_store = dispatcher_module.RcaDeliveryStore
    real_validate = dispatcher_module.validate_bound_resident_release

    def tracked_store(*args, **kwargs):
        events.append(
            (
                "store",
                kwargs.get("read_only", False),
                kwargs.get("ensure_current_rows", True),
            )
        )
        return real_store(*args, **kwargs)

    def tracked_validate(*args, **kwargs):
        events.append(("validate",))
        return real_validate(*args, **kwargs)

    def meegle_provider():
        events.append(("provider", "meegle"))
        return SimpleNamespace(
            list_comments=lambda *_args, **_kwargs: {},
            add_comment=lambda *_args, **_kwargs: {},
            get_fields=lambda *_args, **_kwargs: {},
            update_fields=lambda *_args, **_kwargs: {},
        )

    def thread_provider():
        events.append(("provider", "thread"))
        return SimpleNamespace(
            list_replies=lambda *_args, **_kwargs: {},
            add_reply=lambda *_args, **_kwargs: {},
        )

    def constructed_dispatcher(**kwargs):
        events.append(("dispatcher",))
        captured["config"] = kwargs["config"]
        return SimpleNamespace(store=kwargs["store"], config=kwargs["config"])

    monkeypatch.setattr(
        RcaControlStore,
        "_create_read_only_snapshot",
        lambda _self: pytest.fail("normal startup must not copy the control DB"),
    )
    monkeypatch.setattr(dispatcher_module, "RcaDeliveryStore", tracked_store)
    monkeypatch.setattr(
        dispatcher_module, "validate_bound_resident_release", tracked_validate
    )
    monkeypatch.setattr(
        dispatcher_module,
        "load_delivery_dispatcher_environment",
        lambda: fixture.env_path,
    )
    monkeypatch.setattr(
        dispatcher_module.DispatcherConfig,
        "from_env",
        classmethod(lambda _cls: config),
    )
    monkeypatch.setattr(
        dispatcher_module, "MeegleIssueCommentAdapter", meegle_provider
    )
    monkeypatch.setattr(
        dispatcher_module, "FeishuThreadReplyAdapter", thread_provider
    )
    monkeypatch.setattr(
        dispatcher_module, "DeliveryDispatcher", constructed_dispatcher
    )
    monkeypatch.setattr(
        dispatcher_module, "run_dispatch_loop", lambda *_args, **_kwargs: 0
    )

    assert dispatcher_module.main(["--once"]) == 0
    assert events == [
        ("store", True, False),
        ("validate",),
        ("store", False, True),
        ("provider", "meegle"),
        ("provider", "thread"),
        ("dispatcher",),
    ]
    locked = captured["config"]
    assert locked.resident_release_enforced is True
    assert locked.release_id == fixture.note["release_id"]
    assert locked.observation_release_id == fixture.note["release_id"]
    assert locked.release_epoch_id == fixture.epoch["epoch_id"]
    assert locked.release_fingerprint_sha256 == fixture.fingerprint
    assert (
        locked.release_note_sha256
        == fixture.epoch["production_gate_receipt_sha256"]
    )


@pytest.mark.parametrize("boundary", ["claim", "provider"])
def test_dispatcher_epoch_switch_rejects_claim_and_provider_boundaries(
    tmp_path,
    monkeypatch,
    boundary,
):
    fixture = _release_note(tmp_path)
    control, result = _control(tmp_path, db_path=fixture.control_db_path)
    _bind_activation_execution(control, result, state="steady_active")
    store = RcaDeliveryStore(control.db_path)
    _bind_minimal_release(control, fixture)
    _set_live_release_environment(monkeypatch, fixture)
    binding = dispatcher_module.validate_bound_resident_release(
        store,
        release_note_path=fixture.path,
        runtime_root=fixture.runtime_root,
        runtime_commit=fixture.runtime_commit,
        runtime_tree=fixture.runtime_tree,
        live_manifest_sha256=fixture.manifest_sha256,
        live_env_path=fixture.env_path,
    )
    config = replace(
        _config(tmp_path),
        control_db_path=control.db_path,
        release_note_path=fixture.path,
        release_env_path=fixture.env_path,
        resident_release_enforced=True,
        release_id=binding["release_id"],
        release_epoch_id=binding["epoch_id"],
        release_fingerprint_sha256=binding["release_fingerprint_sha256"],
        release_note_sha256=binding["release_note_sha256"],
    )
    provider_calls = []

    def provider(*_args, **_kwargs):
        provider_calls.append("provider")
        return {"success": True}

    dispatcher = DeliveryDispatcher(
        store=store,
        config=config,
        list_comments=provider,
        add_comment=provider,
        get_fields=provider,
        update_fields=provider,
        report_verifier=provider,
        now=lambda: NOW,
    )
    assert dispatcher._validate_runtime_release()["epoch_id"] == "delivery-epoch-1"
    with sqlite3.connect(control.db_path) as conn:
        conn.execute(
            "UPDATE rca_activation_epochs SET epoch_id = 'delivery-epoch-2' "
            "WHERE is_current = 1"
        )
    if boundary == "claim":
        monkeypatch.setattr(
            store,
            "claim_due_effect",
            lambda **_kwargs: pytest.fail("claim ran after release epoch drift"),
        )

    with pytest.raises(dispatcher_module.ExternalWriteFenceError) as exc:
        if boundary == "claim":
            dispatcher.dispatch_one()
        else:
            dispatcher._validate_external_write(
                SimpleNamespace(),
                operation="feishu_issue_comment",
                target="https://project.feishu.cn/g1q3/issue/detail/1",
            )
            provider()

    assert exc.value.code == "resident_release_binding_changed"
    assert provider_calls == []


def test_collector_and_dispatcher_dry_run_use_live_read_only_store_without_ensure(
    tmp_path,
    monkeypatch,
):
    from scripts import pnc_rca_delivery_collector as collector_module

    db_path = tmp_path / "control.sqlite3"
    RcaControlStore(db_path)
    RcaDeliveryStore(db_path)
    with sqlite3.connect(db_path) as conn:
        before = conn.execute(
            "SELECT COUNT(*) FROM rca_delivery_dispatcher_circuit"
        ).fetchone()[0]
    collector_config = CollectorConfig.from_env(
        {
            "HERMES_RCA_DELIVERY_COLLECTOR_ENABLED": "true",
            "HERMES_RCA_DELIVERY_COLLECTOR_CONTROL_DB_PATH": str(db_path),
        },
        hermes_home=tmp_path,
    )
    dispatcher_config = _config(tmp_path)
    real_store = RcaDeliveryStore
    collector_calls = []
    dispatcher_calls = []

    def collector_store(*args, **kwargs):
        collector_calls.append(
            (kwargs.get("read_only"), kwargs.get("ensure_current_rows"))
        )
        return real_store(*args, **kwargs)

    def dispatcher_store(*args, **kwargs):
        dispatcher_calls.append(
            (kwargs.get("read_only"), kwargs.get("ensure_current_rows"))
        )
        return real_store(*args, **kwargs)

    monkeypatch.setattr(
        RcaControlStore,
        "_create_read_only_snapshot",
        lambda _self: pytest.fail("dry-run must not copy the control DB"),
    )
    monkeypatch.setattr(
        RcaDeliveryStore,
        "_ensure_card_patch_circuit_row",
        lambda _self: pytest.fail("dry-run must not ensure mutable rows"),
    )
    monkeypatch.setattr(collector_module, "RcaDeliveryStore", collector_store)
    monkeypatch.setattr(
        collector_module, "load_collector_environment", lambda: tmp_path / ".env"
    )
    monkeypatch.setattr(
        collector_module.CollectorConfig,
        "from_env",
        classmethod(lambda _cls: collector_config),
    )
    monkeypatch.setattr(dispatcher_module, "RcaDeliveryStore", dispatcher_store)
    monkeypatch.setattr(
        dispatcher_module,
        "load_delivery_dispatcher_environment",
        lambda: tmp_path / ".env",
    )
    monkeypatch.setattr(
        dispatcher_module.DispatcherConfig,
        "from_env",
        classmethod(lambda _cls: dispatcher_config),
    )

    assert collector_module.main(["--dry-run"]) == 0
    assert dispatcher_module.main(["--dry-run"]) == 0
    assert collector_calls == [(True, False)]
    assert dispatcher_calls == [(True, False)]
    with sqlite3.connect(db_path) as conn:
        after = conn.execute(
            "SELECT COUNT(*) FROM rca_delivery_dispatcher_circuit"
        ).fetchone()[0]
    assert after == before


def test_delivery_dispatcher_environment_loader_preserves_literal_expansion_syntax(
    tmp_path, monkeypatch
):
    env_file = tmp_path / "delivery-dispatcher.env"
    key = "HERMES_RCA_DELIVERY_DISPATCHER_HEALTH_PATH"
    env_file.write_text(f"{key}=${{AMBIENT_PATH}}\n", encoding="utf-8")
    monkeypatch.setenv("AMBIENT_PATH", "/unexpected/expanded-health.json")
    monkeypatch.delenv(key, raising=False)

    try:
        dispatcher_module.load_delivery_dispatcher_environment(env_file)
        assert os.environ[key] == "${AMBIENT_PATH}"
    finally:
        os.environ.pop(key, None)


def _patch_circuit_reset_cli(monkeypatch, config, tmp_path):
    binding = {
        "epoch_id": "rca-steady-test",
        "release_id": "rca-test-release",
        "release_fingerprint_sha256": "1" * 64,
        "release_note_path": str((tmp_path / "release-note.json").absolute()),
        "release_note_sha256": "2" * 64,
        "runtime_root": str(tmp_path.absolute()),
        "runtime_commit": "3" * 40,
        "runtime_tree": "4" * 40,
        "live_manifest_sha256": "5" * 64,
        "live_env_sha256": "6" * 64,
    }
    provenance = {
        "entrypoint_path": str((tmp_path / "dispatcher.py").absolute()),
        "entrypoint_sha256": "3" * 64,
        "delivery_store_path": str((tmp_path / "delivery-store.py").absolute()),
        "delivery_store_sha256": "4" * 64,
        "receipt_helper_path": str((tmp_path / "receipt-helper.py").absolute()),
        "receipt_helper_sha256": "5" * 64,
        "control_store_path": str((tmp_path / "control-store.py").absolute()),
        "control_store_sha256": "6" * 64,
    }
    monkeypatch.setattr(
        dispatcher_module, "load_delivery_dispatcher_environment", lambda: None
    )
    monkeypatch.setattr(
        dispatcher_module.DispatcherConfig,
        "from_env",
        classmethod(lambda _cls: config),
    )
    monkeypatch.setattr(
        dispatcher_module,
        "_current_release_binding_snapshot",
        lambda _config, _store: dict(binding),
    )
    monkeypatch.setattr(
        dispatcher_module,
        "_circuit_reset_tool_provenance",
        lambda: dict(provenance),
    )
    return binding


def test_bound_source_rejects_group_or_world_writable_file(tmp_path):
    source = tmp_path / "tool.py"
    source.write_text("print('bound')\n", encoding="utf-8")
    source.chmod(0o664)

    with pytest.raises(ValueError, match="tool_provenance_invalid"):
        dispatcher_module._bound_source_sha256(source)


def test_clear_circuit_cli_plans_then_applies_without_claiming_effects(
    tmp_path, monkeypatch, capsys
):
    config = _config(tmp_path, enabled=False)
    RcaControlStore(config.control_db_path)
    store = RcaDeliveryStore(config.control_db_path)
    store.open_delivery_dispatcher_circuit(
        effect_kind=DELIVERY_EFFECT_KIND,
        reason_code="operator_test_open",
        now=NOW,
    )
    before_effects = store.list_rows("rca_delivery_effects")
    binding = _patch_circuit_reset_cli(monkeypatch, config, tmp_path)
    receipt = tmp_path / "delivery-circuit-reset.json"

    assert (
        dispatcher_module.main([
            "--clear-circuit",
            "--effect-kind",
            DELIVERY_EFFECT_KIND,
            "--operator",
            "owner@example.com",
            "--reason",
            "validated provider recovery",
            "--receipt",
            str(receipt),
        ])
        == 0
    )
    plan = json.loads(capsys.readouterr().out)
    assert plan["mode"] == "plan"
    assert plan["applied"] is False
    assert plan["plan"]["release_binding"] == binding
    assert plan["plan"]["effect_delta"]["external_writes"] == 0
    config_sha = plan["plan"]["config_binding_sha256"]
    plan_id = plan["plan"]["plan_id"]
    before_sha = plan["plan"]["before_state_sha256"]
    assert store.delivery_dispatcher_circuit(DELIVERY_EFFECT_KIND).state == "open"
    assert not receipt.exists()
    assert store.list_rows("rca_delivery_effects") == before_effects == []

    assert (
        dispatcher_module.main([
            "--clear-circuit",
            "--effect-kind",
            DELIVERY_EFFECT_KIND,
            "--operator",
            "owner@example.com",
            "--reason",
            "validated provider recovery",
            "--apply",
            "--expected-release-note-sha256",
            binding["release_note_sha256"],
            "--expected-config-binding-sha256",
            config_sha,
            "--expected-plan-id",
            plan_id,
            "--expected-before-state-sha256",
            before_sha,
            "--receipt",
            str(receipt),
        ])
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    body = json.loads(receipt.read_text(encoding="utf-8"))
    assert payload["ok"] is True
    assert payload["applied"] is True
    assert payload["effect_delta"]["external_effects_triggered"] is False
    assert body["release_binding"] == binding
    assert body["effect_delta"]["delivery_effect_rows"] == 0
    assert body["effect_delta"]["database_rows"]["total"] == 3
    assert receipt.stat().st_mode & 0o777 == 0o444
    assert Path(f"{receipt}.sha256").is_file()
    assert store.delivery_dispatcher_circuit(DELIVERY_EFFECT_KIND).state == "closed"
    assert store.delivery_dispatcher_circuit_reset_audit(
        body["reset_id"], effect_kind=DELIVERY_EFFECT_KIND
    ) == body
    assert store.list_rows("rca_delivery_effects") == before_effects == []


def test_clear_circuit_rejects_binding_drift_before_mutation(
    tmp_path, monkeypatch, capsys
):
    config = _config(tmp_path, enabled=False)
    RcaControlStore(config.control_db_path)
    store = RcaDeliveryStore(config.control_db_path)
    store.open_delivery_dispatcher_circuit(
        effect_kind=DELIVERY_EFFECT_KIND,
        reason_code="operator_test_open",
        now=NOW,
    )
    binding = _patch_circuit_reset_cli(monkeypatch, config, tmp_path)
    receipt = tmp_path / "drift-reset.json"
    assert dispatcher_module.main([
        "--clear-circuit",
        "--effect-kind",
        DELIVERY_EFFECT_KIND,
        "--operator",
        "owner",
        "--reason",
        "plan binding",
        "--receipt",
        str(receipt),
    ]) == 0
    plan = json.loads(capsys.readouterr().out)
    changed = dict(binding)
    changed["release_note_sha256"] = "7" * 64
    monkeypatch.setattr(
        dispatcher_module,
        "_current_release_binding_snapshot",
        lambda _config, _store: changed,
    )
    assert dispatcher_module.main([
        "--clear-circuit",
        "--effect-kind",
        DELIVERY_EFFECT_KIND,
        "--operator",
        "owner",
        "--reason",
        "plan binding",
        "--apply",
        "--expected-release-note-sha256",
        binding["release_note_sha256"],
        "--expected-config-binding-sha256",
        plan["plan"]["config_binding_sha256"],
        "--expected-plan-id",
        plan["plan"]["plan_id"],
        "--expected-before-state-sha256",
        plan["plan"]["before_state_sha256"],
        "--receipt",
        str(receipt),
    ]) == 2
    assert "release_binding_changed" in capsys.readouterr().out
    assert store.delivery_dispatcher_circuit(DELIVERY_EFFECT_KIND).state == "open"
    assert not receipt.exists()


def test_clear_circuit_rejects_config_drift_before_mutation(
    tmp_path, monkeypatch, capsys
):
    config = _config(tmp_path, enabled=False)
    RcaControlStore(config.control_db_path)
    store = RcaDeliveryStore(config.control_db_path)
    store.open_delivery_dispatcher_circuit(
        effect_kind=DELIVERY_EFFECT_KIND,
        reason_code="operator_test_open",
        now=NOW,
    )
    binding = _patch_circuit_reset_cli(monkeypatch, config, tmp_path)
    receipt = tmp_path / "config-drift-reset.json"
    assert dispatcher_module.main([
        "--clear-circuit",
        "--effect-kind",
        DELIVERY_EFFECT_KIND,
        "--operator",
        "owner",
        "--reason",
        "plan config",
        "--receipt",
        str(receipt),
    ]) == 0
    plan = json.loads(capsys.readouterr().out)["plan"]
    changed_config = replace(config, inventory_pin="c" * 64)
    monkeypatch.setattr(
        dispatcher_module.DispatcherConfig,
        "from_env",
        classmethod(lambda _cls: changed_config),
    )
    assert dispatcher_module.main([
        "--clear-circuit",
        "--effect-kind",
        DELIVERY_EFFECT_KIND,
        "--operator",
        "owner",
        "--reason",
        "plan config",
        "--apply",
        "--expected-release-note-sha256",
        binding["release_note_sha256"],
        "--expected-config-binding-sha256",
        plan["config_binding_sha256"],
        "--expected-plan-id",
        plan["plan_id"],
        "--expected-before-state-sha256",
        plan["before_state_sha256"],
        "--receipt",
        str(receipt),
    ]) == 2
    assert "config_changed" in capsys.readouterr().out
    assert store.delivery_dispatcher_circuit(DELIVERY_EFFECT_KIND).state == "open"
    assert not receipt.exists()


def test_clear_circuit_rejects_exact_before_state_drift(
    tmp_path, monkeypatch, capsys
):
    config = _config(tmp_path, enabled=False)
    RcaControlStore(config.control_db_path)
    store = RcaDeliveryStore(config.control_db_path)
    store.open_delivery_dispatcher_circuit(
        effect_kind=DELIVERY_EFFECT_KIND,
        reason_code="first-open",
        now=NOW,
    )
    binding = _patch_circuit_reset_cli(monkeypatch, config, tmp_path)
    receipt = tmp_path / "before-drift-reset.json"
    assert dispatcher_module.main([
        "--clear-circuit",
        "--effect-kind",
        DELIVERY_EFFECT_KIND,
        "--operator",
        "owner",
        "--reason",
        "exact before",
        "--receipt",
        str(receipt),
    ]) == 0
    plan = json.loads(capsys.readouterr().out)["plan"]
    store.open_delivery_dispatcher_circuit(
        effect_kind=DELIVERY_EFFECT_KIND,
        reason_code="second-open",
        now=NOW + timedelta(seconds=1),
    )
    assert dispatcher_module.main([
        "--clear-circuit",
        "--effect-kind",
        DELIVERY_EFFECT_KIND,
        "--operator",
        "owner",
        "--reason",
        "exact before",
        "--apply",
        "--expected-release-note-sha256",
        binding["release_note_sha256"],
        "--expected-config-binding-sha256",
        plan["config_binding_sha256"],
        "--expected-plan-id",
        plan["plan_id"],
        "--expected-before-state-sha256",
        plan["before_state_sha256"],
        "--receipt",
        str(receipt),
    ]) == 2
    assert "before_state_changed" in capsys.readouterr().out
    assert store.delivery_dispatcher_circuit(DELIVERY_EFFECT_KIND).reason_code == (
        "second-open"
    )
    assert not receipt.exists()


def test_clear_circuit_rejects_destination_drift_from_reviewed_plan(
    tmp_path, monkeypatch, capsys
):
    config = _config(tmp_path, enabled=False)
    RcaControlStore(config.control_db_path)
    store = RcaDeliveryStore(config.control_db_path)
    store.open_delivery_dispatcher_circuit(
        effect_kind=DELIVERY_EFFECT_KIND,
        reason_code="operator_test_open",
        now=NOW,
    )
    binding = _patch_circuit_reset_cli(monkeypatch, config, tmp_path)
    planned_receipt = tmp_path / "planned-destination.json"
    alternate_receipt = tmp_path / "alternate-destination.json"
    assert dispatcher_module.main([
        "--clear-circuit",
        "--effect-kind",
        DELIVERY_EFFECT_KIND,
        "--operator",
        "owner",
        "--reason",
        "destination binding",
        "--receipt",
        str(planned_receipt),
    ]) == 0
    plan = json.loads(capsys.readouterr().out)["plan"]
    assert dispatcher_module.main([
        "--clear-circuit",
        "--effect-kind",
        DELIVERY_EFFECT_KIND,
        "--operator",
        "owner",
        "--reason",
        "destination binding",
        "--apply",
        "--expected-release-note-sha256",
        binding["release_note_sha256"],
        "--expected-config-binding-sha256",
        plan["config_binding_sha256"],
        "--expected-plan-id",
        plan["plan_id"],
        "--expected-before-state-sha256",
        plan["before_state_sha256"],
        "--receipt",
        str(alternate_receipt),
    ]) == 2
    assert "plan_changed" in capsys.readouterr().out
    assert store.delivery_dispatcher_circuit(DELIVERY_EFFECT_KIND).state == "open"
    assert not planned_receipt.exists()
    assert not alternate_receipt.exists()


def test_clear_circuit_parent_swap_is_rejected_before_receipt_write(
    tmp_path, monkeypatch, capsys
):
    config = _config(tmp_path, enabled=False)
    RcaControlStore(config.control_db_path)
    store = RcaDeliveryStore(config.control_db_path)
    store.open_delivery_dispatcher_circuit(
        effect_kind=DELIVERY_EFFECT_KIND,
        reason_code="operator_test_open",
        now=NOW,
    )
    binding = _patch_circuit_reset_cli(monkeypatch, config, tmp_path)
    destination_dir = tmp_path / "receipts"
    destination_dir.mkdir()
    receipt = destination_dir / "swap-reset.json"
    assert dispatcher_module.main([
        "--clear-circuit",
        "--effect-kind",
        DELIVERY_EFFECT_KIND,
        "--operator",
        "owner",
        "--reason",
        "parent swap",
        "--receipt",
        str(receipt),
    ]) == 0
    plan = json.loads(capsys.readouterr().out)["plan"]
    original_writer = dispatcher_module._write_immutable_circuit_reset_receipt

    def swap_then_write(path, value, **kwargs):
        old_dir = tmp_path / "receipts-old"
        destination_dir.rename(old_dir)
        destination_dir.mkdir()
        return original_writer(path, value, **kwargs)

    monkeypatch.setattr(
        dispatcher_module,
        "_write_immutable_circuit_reset_receipt",
        swap_then_write,
    )
    assert dispatcher_module.main([
        "--clear-circuit",
        "--effect-kind",
        DELIVERY_EFFECT_KIND,
        "--operator",
        "owner",
        "--reason",
        "parent swap",
        "--apply",
        "--expected-release-note-sha256",
        binding["release_note_sha256"],
        "--expected-config-binding-sha256",
        plan["config_binding_sha256"],
        "--expected-plan-id",
        plan["plan_id"],
        "--expected-before-state-sha256",
        plan["before_state_sha256"],
        "--receipt",
        str(receipt),
    ]) == 2
    error = json.loads(capsys.readouterr().out)
    assert error["recovery_required"] is True
    assert store.delivery_dispatcher_circuit(DELIVERY_EFFECT_KIND).state == "closed"
    assert not receipt.exists()
    monkeypatch.setattr(
        dispatcher_module,
        "_write_immutable_circuit_reset_receipt",
        original_writer,
    )
    recovered_dir = tmp_path / "recovered"
    recovered_dir.mkdir()
    recovered = recovered_dir / "swap-recovered.json"
    assert dispatcher_module.main([
        "--materialize-reset",
        error["reset_id"],
        "--effect-kind",
        DELIVERY_EFFECT_KIND,
        "--receipt",
        str(recovered),
    ]) == 0
    envelope = json.loads(recovered.read_text(encoding="utf-8"))
    assert envelope["source_reset_id"] == error["reset_id"]
    assert envelope["materialized_destination"]["path"] == str(recovered)


def test_clear_circuit_receipt_materialization_failure_is_recoverable(
    tmp_path, monkeypatch, capsys
):
    config = _config(tmp_path, enabled=False)
    RcaControlStore(config.control_db_path)
    store = RcaDeliveryStore(config.control_db_path)
    store.open_delivery_dispatcher_circuit(
        effect_kind=DELIVERY_EFFECT_KIND,
        reason_code="operator_test_open",
        now=NOW,
    )
    binding = _patch_circuit_reset_cli(monkeypatch, config, tmp_path)
    receipt = tmp_path / "materialization-fails.json"
    assert dispatcher_module.main([
        "--clear-circuit",
        "--effect-kind",
        DELIVERY_EFFECT_KIND,
        "--operator",
        "owner",
        "--reason",
        "recover durable audit",
        "--receipt",
        str(receipt),
    ]) == 0
    plan = json.loads(capsys.readouterr().out)["plan"]
    original_writer = dispatcher_module._write_immutable_circuit_reset_receipt
    monkeypatch.setattr(
        dispatcher_module,
        "_write_immutable_circuit_reset_receipt",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            OSError("simulated receipt failure")
        ),
    )
    assert dispatcher_module.main([
        "--clear-circuit",
        "--effect-kind",
        DELIVERY_EFFECT_KIND,
        "--operator",
        "owner",
        "--reason",
        "recover durable audit",
        "--apply",
        "--expected-release-note-sha256",
        binding["release_note_sha256"],
        "--expected-config-binding-sha256",
        plan["config_binding_sha256"],
        "--expected-plan-id",
        plan["plan_id"],
        "--expected-before-state-sha256",
        plan["before_state_sha256"],
        "--receipt",
        str(receipt),
    ]) == 2
    error = json.loads(capsys.readouterr().out)
    assert error["recovery_required"] is True
    assert store.delivery_dispatcher_circuit(DELIVERY_EFFECT_KIND).state == "closed"
    assert store.delivery_dispatcher_circuit_reset_audit(
        error["reset_id"], effect_kind=DELIVERY_EFFECT_KIND
    ) is not None
    monkeypatch.setattr(
        dispatcher_module,
        "_write_immutable_circuit_reset_receipt",
        original_writer,
    )
    recovered = tmp_path / "recovered-delivery-reset.json"
    assert dispatcher_module.main([
        "--materialize-reset",
        error["reset_id"],
        "--effect-kind",
        DELIVERY_EFFECT_KIND,
        "--receipt",
        str(recovered),
    ]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["recovered"] is True
    envelope = json.loads(recovered.read_text(encoding="utf-8"))
    assert envelope["source_reset_id"] == error["reset_id"]
    assert envelope["audit"]["reset_id"] == error["reset_id"]
    assert envelope["materialized_destination"]["path"] == str(recovered)
    assert envelope["planned_destination_binding"] == (
        envelope["audit"]["destination_binding"]
    )


def test_clear_circuit_requires_governed_inputs_and_leaves_open_state(
    tmp_path, monkeypatch, capsys
):
    config = _config(tmp_path, enabled=False)
    RcaControlStore(config.control_db_path)
    store = RcaDeliveryStore(config.control_db_path)
    store.open_delivery_dispatcher_circuit(
        effect_kind=DELIVERY_EFFECT_KIND,
        reason_code="operator_test_open",
        now=NOW,
    )
    _patch_circuit_reset_cli(monkeypatch, config, tmp_path)
    assert dispatcher_module.main([
        "--clear-circuit",
        "--effect-kind",
        DELIVERY_EFFECT_KIND,
    ]) == 2
    assert "operator_and_reason_required" in capsys.readouterr().out
    assert store.delivery_dispatcher_circuit(DELIVERY_EFFECT_KIND).state == "open"


def test_reset_plan_and_missing_recovery_are_read_only_without_card_row(
    tmp_path, monkeypatch, capsys
):
    config = _config(tmp_path, enabled=False)
    RcaControlStore(config.control_db_path)
    store = RcaDeliveryStore(config.control_db_path)
    store.open_delivery_dispatcher_circuit(
        effect_kind=DELIVERY_EFFECT_KIND,
        reason_code="operator_test_open",
        now=NOW,
    )
    with sqlite3.connect(config.control_db_path) as conn:
        conn.execute(
            "DELETE FROM rca_delivery_dispatcher_circuit "
            "WHERE circuit_name = ?",
            ("feishu_card_patch",),
        )
        conn.commit()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    def db_snapshot():
        return {
            str(path): path.read_bytes() if path.exists() else None
            for path in (
                config.control_db_path,
                Path(f"{config.control_db_path}-wal"),
                Path(f"{config.control_db_path}-shm"),
            )
        }

    before = db_snapshot()
    _patch_circuit_reset_cli(monkeypatch, config, tmp_path)
    receipt = tmp_path / "read-only-plan.json"
    assert dispatcher_module.main([
        "--clear-circuit",
        "--effect-kind",
        DELIVERY_EFFECT_KIND,
        "--operator",
        "owner",
        "--reason",
        "read only plan",
        "--receipt",
        str(receipt),
    ]) == 0
    assert json.loads(capsys.readouterr().out)["mode"] == "plan"
    assert db_snapshot() == before
    with sqlite3.connect(config.control_db_path) as conn:
        assert conn.execute(
            "SELECT 1 FROM rca_delivery_dispatcher_circuit "
            "WHERE circuit_name = 'feishu_card_patch'"
        ).fetchone() is None

    missing_receipt = tmp_path / "missing-recovery.json"
    assert dispatcher_module.main([
        "--materialize-reset",
        "a" * 64,
        "--effect-kind",
        DELIVERY_EFFECT_KIND,
        "--receipt",
        str(missing_receipt),
    ]) == 2
    assert "audit_missing" in capsys.readouterr().out
    assert db_snapshot() == before
    with sqlite3.connect(config.control_db_path) as conn:
        assert conn.execute(
            "SELECT 1 FROM rca_delivery_dispatcher_circuit "
            "WHERE circuit_name = 'feishu_card_patch'"
        ).fetchone() is None



def _config(tmp_path, *, enabled: bool = True):
    return DispatcherConfig.from_env(
        {
            "HERMES_RCA_DELIVERY_DISPATCHER_ENABLED": str(enabled).lower(),
            "HERMES_RCA_DELIVERY_DISPATCHER_CONTROL_DB_PATH": str(
                tmp_path / "control.sqlite3"
            ),
            "HERMES_RCA_DELIVERY_DISPATCHER_HEALTH_PATH": str(
                tmp_path / "dispatcher-health.json"
            ),
            "HERMES_RCA_DELIVERY_DISPATCHER_LEASE_SECONDS": "90",
            "HERMES_RCA_DELIVERY_DISPATCHER_POLL_INTERVAL_SECONDS": "2",
            "HERMES_RCA_DELIVERY_DISPATCHER_CIRCUIT_POLL_INTERVAL_SECONDS": "30",
            "HERMES_RCA_DELIVERY_DISPATCHER_BATCH_SIZE": "5",
            "HERMES_RCA_DELIVERY_DISPATCHER_HEALTH_MAX_AGE_SECONDS": "60",
            "HERMES_RCA_DELIVERY_DISPATCHER_REPORT_HTTP_TIMEOUT_SECONDS": "10",
            "HERMES_RCA_DELIVERY_DISPATCHER_OBSERVABILITY_PATH": str(
                tmp_path / "delivery-observations.jsonl"
            ),
            "HERMES_RCA_DELIVERY_DISPATCHER_INVENTORY_PIN": "b" * 64,
            "HERMES_RCA_DELIVERY_DISPATCHER_OBSERVATION_RELEASE_ID": "release-test-observability",
        },
        hermes_home=tmp_path,
    )


def _collector(
    tmp_path,
    *,
    status_reader=None,
    bundle_reader=None,
    failure_receipt_reader=None,
    enabled: bool = True,
    now=None,
):
    config = CollectorConfig.from_env(
        {
            "HERMES_RCA_DELIVERY_COLLECTOR_ENABLED": str(enabled).lower(),
            "HERMES_RCA_DELIVERY_COLLECTOR_CONTROL_DB_PATH": str(
                tmp_path / "control.sqlite3"
            ),
            "HERMES_RCA_DELIVERY_COLLECTOR_HEALTH_PATH": str(
                tmp_path / "collector-health.json"
            ),
            "HERMES_RCA_DELIVERY_COLLECTOR_POLL_INTERVAL_SECONDS": "1",
            "HERMES_RCA_DELIVERY_COLLECTOR_RUNNING_POLL_SECONDS": "20",
            "HERMES_RCA_DELIVERY_COLLECTOR_MAX_POLL_SECONDS": "300",
            "HERMES_RCA_DELIVERY_COLLECTOR_LEASE_SECONDS": "60",
            "HERMES_RCA_DELIVERY_COLLECTOR_BATCH_SIZE": "5",
            "HERMES_RCA_DELIVERY_COLLECTOR_BACKFILL_BATCH_SIZE": "100",
            "HERMES_RCA_DELIVERY_COLLECTOR_HEALTH_MAX_AGE_SECONDS": "60",
            "HERMES_RCA_DELIVERY_COLLECTOR_SSH_MINI_AGENT": "/safe/ssh-mini-agent",
            "HERMES_RCA_DELIVERY_COLLECTOR_ARTIFACT_READ_TIMEOUT_SECONDS": "30",
        },
        hermes_home=tmp_path,
    )
    return DeliveryCollector(
        store=RcaDeliveryStore(tmp_path / "control.sqlite3"),
        config=config,
        status_reader=status_reader
        or (
            lambda task_id: {
                "success": True,
                "task_id": task_id,
                "state": "completed",
            }
        ),
        artifact_bundle_reader=bundle_reader or (lambda _claim: _web_bundle_payload()),
        failure_receipt_reader=failure_receipt_reader
        or (
            lambda claim: {
                "schema_version": "g1q3_rca_service_result_v2",
                "task_id": claim.task_id,
                "status": "pipeline_not_successful",
                "pipeline_status": "failed",
                "pipeline_stage": "execution",
                "blocker": {
                    "kind": "service_pipeline_runner_failed",
                    "retryable": False,
                },
            }
        ),
        now=now or (lambda: NOW),
        lease_owner="collector-test",
    )


def _seed(tmp_path, *, bundle_payload=None):
    control, result = _control(tmp_path)
    with sqlite3.connect(control.db_path) as conn:
        conn.execute(
            "UPDATE business_triggers SET created_at = ? WHERE submission_key = ?",
            (NOW.isoformat(), result.submission_key),
        )
    _bind_activation_execution(control, result, state="steady_active")
    collector = _collector(
        tmp_path,
        bundle_reader=(
            None if bundle_payload is None else lambda _claim: bundle_payload
        ),
    )
    assert collector.collect_batch()[0].status == "delivery_created"
    return collector.store


def _seed_with_thread_subscription(tmp_path, *, bundle_payload=None):
    control, result = _control(tmp_path)
    with sqlite3.connect(control.db_path) as conn:
        conn.execute(
            "UPDATE business_triggers SET created_at = ? WHERE submission_key = ?",
            (NOW.isoformat(), result.submission_key),
        )
    _bind_activation_execution(control, result, state="steady_active")
    trigger = control.list_rows("business_triggers")[0]
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    _insert_subscription(
        store,
        SimpleNamespace(
            business_key=trigger["business_key"],
            generation=trigger["generation"],
            project_key=trigger["project_key"],
            work_item_type_key=trigger["work_item_type_key"],
            work_item_id=trigger["work_item_id"],
        ),
        effect_kind="feishu_thread_reply",
    )
    collector = _collector(
        tmp_path,
        bundle_reader=(
            None if bundle_payload is None else lambda _claim: bundle_payload
        ),
    )
    assert collector.collect_batch()[0].status == "delivery_created"
    return collector.store


def _seed_terminal(tmp_path, *, with_thread: bool = False):
    """Seed a pre-B6 terminal effect for dispatcher recovery coverage.

    New terminal failures must be completed by DeliveryCollector through its
    silent internal path.  These dispatcher tests instead exercise historical
    rows that were already materialized before that policy existed, without
    asserting that the current collector may create them.
    """
    control, result = _control(tmp_path)
    with sqlite3.connect(control.db_path) as conn:
        conn.execute(
            "UPDATE business_triggers SET created_at = ? WHERE submission_key = ?",
            (NOW.isoformat(), result.submission_key),
        )
    _bind_activation_execution(control, result, state="steady_active")
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    if with_thread:
        trigger = control.list_rows("business_triggers")[0]
        _insert_subscription(
            store,
            SimpleNamespace(
                business_key=trigger["business_key"],
                generation=trigger["generation"],
                project_key=trigger["project_key"],
                work_item_type_key=trigger["work_item_type_key"],
                work_item_id=trigger["work_item_id"],
            ),
            effect_kind="feishu_thread_reply",
        )
    assert store.backfill_completed_submissions(now=NOW) == 1
    claim = store.claim_due_watch(
        lease_owner="legacy-terminal-fixture",
        now=NOW,
    )
    assert claim is not None
    seeded = store.create_terminal_delivery(
        claim=claim,
        status={"success": False, "state": "failed", "legacy_fixture": True},
        outcome="terminal_failed",
        terminal_state="failed",
        error_code="vm_terminal_failed_unclassified",
        error_detail="historical fixture only",
        now=NOW,
        activation_required=True,
    )
    assert seeded.created is True
    return store


def _seed_profile_terminal(tmp_path, *, split_project_identity: bool = False):
    """Materialize one current Kafka profile terminal without a W3 snapshot."""
    from tests.gateway.test_pnc_rca_control_store import (
        _profile_snapshot_policy,
        _profile_snapshot_record,
    )

    control = RcaControlStore(tmp_path / "control.sqlite3")
    _activate_direct_steady(control, start_offset=20)
    policy = _profile_snapshot_policy()
    record = _profile_snapshot_record(20, "7019637554")
    if split_project_identity:
        policy = replace(
            policy,
            project_keys=frozenset({REAL_G1Q3_PROJECT_KEY}),
            project_simple_names=frozenset({REAL_G1Q3_PROJECT_SIMPLE_NAME}),
        )
        payload = json.loads(record.value)
        payload["project_key"] = REAL_G1Q3_PROJECT_KEY
        payload["project_simple_name"] = REAL_G1Q3_PROJECT_SIMPLE_NAME
        record = replace(
            record,
            value=json.dumps(payload, sort_keys=True).encode(),
        )
    result = control.ingest_record(
        record,
        policy=policy,
        submit_enabled=True,
        activation_required=True,
    )
    assert result.decision == "accepted"
    with sqlite3.connect(control.db_path) as conn:
        conn.execute(
            "UPDATE business_triggers SET created_at = ? WHERE submission_key = ?",
            (NOW.isoformat(), result.submission_key),
        )
    outbox = control.claim_outbox(
        lease_owner="profile-terminal-dispatcher-fixture",
        now=NOW,
    )
    assert outbox is not None
    control.quarantine_outbox(
        outbox_id=outbox.outbox_id,
        lease_token=outbox.lease_token,
        error_code="business_profile_adapter_not_ready",
        error_detail="matched profile input adapter is not ready",
        now=NOW + timedelta(seconds=1),
    )
    store = RcaDeliveryStore(control.db_path)
    assert store.backfill_completed_submissions(
        now=NOW + timedelta(seconds=2),
        activation_required=True,
    ) == 1
    return store


def _web_bundle_payload():
    _admission, contract, manifest, observed, dependencies = _bundle(
        include_web_assets=True
    )
    return {
        "delivery_contract": contract,
        "delivery_manifest": manifest,
        "observed_files": observed,
        "html_dependencies": dependencies,
        # Dispatcher fixtures now model the sealed Gate A envelope that the
        # collector requires before creating a delivery effect.
        "gate_a_source": {
            "input_materialized": True,
            "materialization_attested": True,
            "rca_evaluators": [
                {
                    "key": "aeb_trigger",
                    "domain": "ACC",
                    "pattern": "fixture",
                    "status": "supported",
                    "evidence_refs": [
                        {
                            "signal": "AEBReq",
                            "evidence": "窗口内观测到测试事实。",
                            "window": [-1.0, 1.0],
                        }
                    ],
                }
            ],
        },
    }


def _html_only_bundle_payload():
    _admission, contract, manifest, observed, dependencies = _bundle()
    publication = contract["artifacts"].pop("viz_publication")
    contract["artifacts"].pop("viz_mcap_vm")
    contract["report"]["deliverable_kind"] = "html"
    contract["report"]["status"] = "html_delivery_ready"
    observed = [
        item
        for item in observed
        if item["path"] not in {publication["path"], publication["manifest_path"]}
    ]
    return {
        "delivery_contract": contract,
        "delivery_manifest": manifest,
        "observed_files": observed,
        "html_dependencies": dependencies,
        "gate_a_source": {
            "input_materialized": True,
            "materialization_attested": True,
            "rca_evaluators": [
                {
                    "key": "aeb_trigger",
                    "domain": "ACC",
                    "pattern": "fixture",
                    "status": "supported",
                    "evidence_refs": [
                        {
                            "signal": "AEBReq",
                            "evidence": "窗口内观测到测试事实。",
                            "window": [-1.0, 1.0],
                        }
                    ],
                }
            ],
        },
    }


def _asset_relative(url):
    route = url.split("/G1Q3_RCA/cases/", 1)[1]
    _submission_key, _artifact_set_id, relative = route.split("/", 2)
    return relative


class Remote:
    def __init__(
        self,
        *,
        project_key: str = "t03o4q",
        work_item_id: str = "7041712812",
    ):
        self.project_key = project_key
        self.work_item_id = work_item_id
        self.comments: list[dict[str, str]] = []
        self.list_calls = 0
        self.add_calls = 0
        self.get_field_calls = 0
        self.update_field_calls = 0
        self.fields: dict[str, str] = {}
        self.history: list[str] = []
        self.list_failure: dict | None = None
        self.add_failure: dict | None = None
        self.get_field_failure: dict | None = None
        self.update_field_failure: dict | None = None
        self.weak_success = False

    def list_comments(self, project_key, work_item_id):
        assert project_key == self.project_key
        assert work_item_id == self.work_item_id
        self.list_calls += 1
        self.history.append("list_comments")
        if self.list_failure is not None:
            return dict(self.list_failure)
        return {"success": True, "comments": list(self.comments)}

    def add_comment(self, project_key, work_item_id, content):
        assert project_key == self.project_key
        assert work_item_id == self.work_item_id
        self.add_calls += 1
        self.history.append("add_comment")
        if self.add_failure is not None:
            return dict(self.add_failure)
        remote_id = f"comment-{self.add_calls}"
        self.comments.append({"remote_id": remote_id, "content": content})
        if self.weak_success:
            return {"success": True}
        return {"success": True, "remote_id": remote_id}

    def get_fields(self, project_key, work_item_id, field_keys):
        assert project_key == self.project_key
        assert work_item_id == self.work_item_id
        self.get_field_calls += 1
        self.history.append("get_fields")
        if self.get_field_failure is not None:
            return dict(self.get_field_failure)
        return {
            "success": True,
            "fields": {
                key: self.fields[key] for key in field_keys if key in self.fields
            },
        }

    def update_fields(self, project_key, work_item_id, field_updates):
        assert project_key == self.project_key
        assert work_item_id == self.work_item_id
        self.update_field_calls += 1
        self.history.append("update_fields")
        if self.update_field_failure is not None:
            return dict(self.update_field_failure)
        self.fields.update(dict(field_updates))
        return {"success": True}


class ThreadRemote:
    def __init__(self):
        self.comments: list[dict[str, str]] = []
        self.list_calls = 0
        self.add_calls = 0
        self.list_failure: dict | None = None
        self.add_failure: dict | None = None
        self.idempotency_uuids: list[str] = []

    def list_replies(self, chat_id, thread_id):
        assert chat_id == "oc_group123"
        assert thread_id == "topic:om_root123"
        self.list_calls += 1
        if self.list_failure is not None:
            return dict(self.list_failure)
        return {"success": True, "comments": list(self.comments)}

    def add_reply(self, chat_id, thread_id, content, idempotency_uuid):
        assert chat_id == "oc_group123"
        assert thread_id == "topic:om_root123"
        self.add_calls += 1
        self.idempotency_uuids.append(idempotency_uuid)
        if self.add_failure is not None:
            return dict(self.add_failure)
        remote_id = f"message-{self.add_calls}"
        self.comments.append({"remote_id": remote_id, "content": content})
        return {"success": True, "remote_id": remote_id}


class Clock:
    def __init__(self):
        self.current = NOW

    def __call__(self):
        return self.current


def _verified_report(url, size, sha256):
    assert url.startswith("https://192.168.21.217/?ds=foxglove-http&ds.mcapPath=")
    return {
        "success": True,
        "status_code": 200,
        "content_length": size,
        "sha256": sha256,
    }


def _dispatcher(
    tmp_path,
    *,
    remote=None,
    enabled=True,
    clock=None,
    verifier=None,
    thread_remote=None,
    lease_owner="delivery-dispatcher-test",
    lease_renew_interval_seconds=None,
):
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    remote = remote or Remote()
    clock = clock or Clock()
    return (
        DeliveryDispatcher(
            store=store,
            config=_config(tmp_path, enabled=enabled),
            list_comments=remote.list_comments,
            add_comment=remote.add_comment,
            get_fields=remote.get_fields,
            update_fields=remote.update_fields,
            list_thread_replies=(
                thread_remote.list_replies if thread_remote is not None else None
            ),
            add_thread_reply=(
                thread_remote.add_reply if thread_remote is not None else None
            ),
            report_verifier=verifier or _verified_report,
            now=clock,
            lease_owner=lease_owner,
            _effect_lease_renew_interval_seconds=lease_renew_interval_seconds,
        ),
        remote,
        clock,
    )


def test_enabled_config_requires_inventory_pin_and_derives_release_id_at_startup(
    tmp_path,
):
    values = {
        "HERMES_RCA_DELIVERY_DISPATCHER_ENABLED": "true",
        "HERMES_RCA_DELIVERY_DISPATCHER_OBSERVABILITY_ENABLED": "true",
        "HERMES_RCA_DELIVERY_DISPATCHER_INVENTORY_PIN": "a" * 63,
        "HERMES_RCA_DELIVERY_DISPATCHER_OBSERVATION_RELEASE_ID": "release-test",
    }
    with pytest.raises(ValueError, match="INVENTORY_PIN"):
        DispatcherConfig.from_env(values, hermes_home=tmp_path)

    values["HERMES_RCA_DELIVERY_DISPATCHER_INVENTORY_PIN"] = "a" * 64
    values.pop("HERMES_RCA_DELIVERY_DISPATCHER_OBSERVATION_RELEASE_ID")
    config = DispatcherConfig.from_env(values, hermes_home=tmp_path)

    assert config.observation_release_id == ""


def _observation_intent(index: int) -> DeliveryObservationIntent:
    fields = {
        "schema_version": OBSERVATION_SCHEMA_VERSION,
        "work_item_id": f"work-{index}",
        "case_key": f"case-{index}",
        "delivered_at": "2026-07-31T08:00:00+00:00",
        "level": "L0_abstain",
        "has_attribution": False,
        "viz_published": False,
        "viz_bytes": 0,
        "evidence_channel_msg_count": None,
        "evidence_channel_msg_count_not_measured_reason": "not_measured",
        "evidence_refs_nonempty": None,
        "evidence_refs_nonempty_not_measured_reason": "not_measured",
        "evaluator_hit_count": 0,
        "pipeline_elapsed_seconds": 1.0,
        "outcome_content_sha256": f"{index:064x}",
        "remote_receipt_id": f"receipt-{index}",
        "release_id": "release-test-observability",
        "inventory_pin": "1" * 64,
    }
    fields["observation_id"] = dispatcher_module.delivery_observation_id(fields)
    payload = dispatcher_module.build_delivery_observation(fields)
    payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return DeliveryObservationIntent(
        observation_id=payload["observation_id"],
        effect_key=f"effect-{index}",
        payload=payload,
        payload_sha256=hashlib.sha256(payload_json.encode("utf-8")).hexdigest(),
        created_at=f"2026-07-31T08:00:{index % 60:02d}+00:00",
    )


def test_observation_flush_drains_more_than_one_thousand_pending_intents(
    tmp_path, monkeypatch
):
    dispatcher, _remote, _clock = _dispatcher(tmp_path)
    pending = {
        intent.observation_id: intent
        for intent in map(_observation_intent, range(1001))
    }
    appended: dict[str, DeliveryObservationIntent] = {}
    batch_sizes: list[int] = []

    def list_all(**_kwargs):
        return list(pending.values()) + [
            replace(intent, status="appended") for intent in appended.values()
        ]

    def list_pending(*, limit):
        batch = list(pending.values())[:limit]
        batch_sizes.append(len(batch))
        return batch

    def mark_appended(*, observation_id, payload_sha256, now):
        del now
        intent = pending.pop(observation_id)
        assert intent.payload_sha256 == payload_sha256
        appended[observation_id] = intent
        return True

    monkeypatch.setattr(dispatcher.store, "list_delivery_observations", list_all)
    monkeypatch.setattr(
        dispatcher.store, "list_pending_delivery_observations", list_pending
    )
    monkeypatch.setattr(
        dispatcher.store, "mark_delivery_observation_appended", mark_appended
    )
    observed_hashes: dict[str, str] = {}

    def append_verified(_path, fields):
        observation = dict(fields)
        observed_hashes[observation["observation_id"]] = (
            dispatcher_module.delivery_observation_payload_sha256(observation)
        )
        return SimpleNamespace(
            observation=observation,
            receipt=SimpleNamespace(
                payload_sha256_by_id=dict(observed_hashes),
            ),
        )

    monkeypatch.setattr(
        dispatcher_module,
        "append_delivery_observation_verified",
        append_verified,
    )
    monkeypatch.setattr(
        dispatcher_module,
        "read_delivery_observation_receipt",
        lambda _path: SimpleNamespace(
            payload_sha256_by_id=dict(observed_hashes),
        ),
    )

    dispatcher._flush_pending_delivery_observations()

    assert not pending
    assert len(appended) == 1001
    assert batch_sizes == [1000, 1, 0]
    assert dispatcher.stats.observability_written == 1001


def test_idle_observation_flush_fully_rereads_and_revalidates_changed_identity(
    tmp_path, monkeypatch
):
    dispatcher, _remote, _clock = _dispatcher(tmp_path)
    original_read = dispatcher_module.read_delivery_observation_receipt
    read_calls = 0

    def count_reads(path):
        nonlocal read_calls
        read_calls += 1
        return original_read(path)

    monkeypatch.setattr(
        dispatcher_module, "read_delivery_observation_receipt", count_reads
    )

    assert dispatcher.dispatch_one().status == "idle"
    assert dispatcher.dispatch_one().status == "idle"
    assert read_calls == 4

    intent = _observation_intent(1)
    append_delivery_observation(
        dispatcher.config.observability_path, intent.payload
    )
    with pytest.raises(DeliveryObservationError) as raised:
        dispatcher.dispatch_one()

    assert raised.value.code == "observation_receipt_untracked_id"
    assert read_calls == 5


def test_appended_observation_hash_is_reconciled_before_claim(tmp_path, monkeypatch):
    dispatcher, remote, _clock = _dispatcher(tmp_path)
    intent = _observation_intent(1)
    receipt_path = Path(dispatcher.config.observability_path)
    append_delivery_observation(receipt_path, intent.payload)
    altered = replace(intent, payload_sha256="c" * 64, status="appended")

    monkeypatch.setattr(
        dispatcher.store,
        "list_delivery_observations",
        lambda **_kwargs: [altered],
    )
    monkeypatch.setattr(
        dispatcher.store,
        "list_pending_delivery_observations",
        lambda **_kwargs: [],
    )
    monkeypatch.setattr(
        dispatcher.store,
        "claim_due_effect",
        lambda **_kwargs: pytest.fail("claim must not run before reconciliation"),
    )
    with pytest.raises(DeliveryObservationError) as raised:
        dispatcher.dispatch_one()

    assert raised.value.code == "observation_receipt_payload_hash_mismatch"
    assert remote.add_calls == 0


def test_missing_appended_observation_blocks_before_claim(tmp_path, monkeypatch):
    dispatcher, remote, _clock = _dispatcher(tmp_path)
    intent = replace(_observation_intent(2), status="appended")
    Path(dispatcher.config.observability_path).write_text("", encoding="utf-8")
    monkeypatch.setattr(
        dispatcher.store,
        "list_delivery_observations",
        lambda **_kwargs: [intent],
    )
    monkeypatch.setattr(
        dispatcher.store,
        "list_pending_delivery_observations",
        lambda **_kwargs: [],
    )

    monkeypatch.setattr(
        dispatcher.store,
        "claim_due_effect",
        lambda **_kwargs: pytest.fail("claim must not run before reconciliation"),
    )
    with pytest.raises(DeliveryObservationError) as raised:
        dispatcher.dispatch_one()

    assert raised.value.code == "observation_appended_receipt_missing"
    assert remote.add_calls == 0


def test_observation_outbox_recovers_after_append_failure_without_duplicate_write(
    tmp_path, monkeypatch
):
    store = _seed(tmp_path)
    dispatcher, remote, _clock = _dispatcher(tmp_path)
    original_append = dispatcher_module.append_delivery_observation_verified
    calls = 0

    def fail_once(path, fields):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("simulated observation append crash")
        return original_append(path, fields)

    monkeypatch.setattr(
        dispatcher_module, "append_delivery_observation_verified", fail_once
    )
    with pytest.raises(OSError, match="simulated observation append crash"):
        dispatcher.dispatch_one()
    assert remote.add_calls == 1
    assert store.pending_delivery_observation_count() == 1

    monkeypatch.setattr(
        dispatcher_module, "append_delivery_observation_verified", original_append
    )
    outcome = dispatcher.dispatch_one()
    assert outcome.status == "idle"
    assert remote.add_calls == 1
    assert store.pending_delivery_observation_count() == 0
    assert (
        len(
            Path(dispatcher.config.observability_path)
            .read_text(encoding="utf-8")
            .splitlines()
        )
        == 1
    )


def test_observation_path_replacement_after_append_never_marks_outbox_appended(
    tmp_path,
    monkeypatch,
):
    store = _seed(tmp_path)
    dispatcher, remote, _clock = _dispatcher(tmp_path)
    original_append = dispatcher_module.append_delivery_observation_verified
    rotated_path = tmp_path / "replaced-observations.jsonl"

    def append_then_replace(path, fields):
        result = original_append(path, fields)
        destination = Path(path)
        destination.replace(rotated_path)
        destination.write_bytes(b"")
        destination.chmod(0o600)
        return result

    monkeypatch.setattr(
        dispatcher_module,
        "append_delivery_observation_verified",
        append_then_replace,
    )

    with pytest.raises(DeliveryObservationError) as raised:
        dispatcher.dispatch_one()

    assert raised.value.code == "observation_appended_receipt_missing"
    assert remote.add_calls == 1
    assert store.pending_delivery_observation_count() == 1
    [intent] = store.list_delivery_observations()
    assert intent.status == "pending"
    assert Path(dispatcher.config.observability_path).read_bytes() == b""
    assert len(rotated_path.read_text(encoding="utf-8").splitlines()) == 1


def test_observation_torn_tail_recovers_only_from_pending_canonical_frame(
    tmp_path,
    monkeypatch,
):
    store = _seed(tmp_path)
    dispatcher, remote, _clock = _dispatcher(tmp_path)
    original_append = dispatcher_module.append_delivery_observation_verified

    monkeypatch.setattr(
        dispatcher_module,
        "append_delivery_observation_verified",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            OSError("simulated short observation append")
        ),
    )
    with pytest.raises(OSError, match="simulated short observation append"):
        dispatcher.dispatch_one()
    assert remote.add_calls == 1
    [intent] = store.list_pending_delivery_observations()
    canonical = json.dumps(
        intent.payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    receipt_path = Path(dispatcher.config.observability_path)
    receipt_path.write_bytes(canonical[: len(canonical) // 2])
    receipt_path.chmod(0o600)
    monkeypatch.setattr(
        dispatcher_module,
        "append_delivery_observation_verified",
        original_append,
    )

    assert dispatcher.dispatch_one().status == "idle"
    assert remote.add_calls == 1
    assert store.pending_delivery_observation_count() == 0
    assert receipt_path.read_bytes() == canonical + b"\n"


def test_observation_path_replacement_during_ack_requeues_exact_batch(
    tmp_path,
    monkeypatch,
):
    store = _seed(tmp_path)
    dispatcher, remote, _clock = _dispatcher(tmp_path)
    original_mark = dispatcher.store.mark_delivery_observation_appended
    rotated_path = tmp_path / "ack-race-observations.jsonl"
    replaced = False

    def mark_then_replace(**kwargs):
        nonlocal replaced
        marked = original_mark(**kwargs)
        if not replaced:
            replaced = True
            destination = Path(dispatcher.config.observability_path)
            destination.replace(rotated_path)
            destination.write_bytes(b"")
            destination.chmod(0o600)
        return marked

    monkeypatch.setattr(
        dispatcher.store,
        "mark_delivery_observation_appended",
        mark_then_replace,
    )

    with pytest.raises(DeliveryObservationError) as raised:
        dispatcher.dispatch_one()

    assert raised.value.code == "observation_appended_receipt_missing"
    assert remote.add_calls == 1
    [intent] = store.list_delivery_observations()
    assert intent.status == "pending"
    assert store.pending_delivery_observation_count() == 1
    assert Path(dispatcher.config.observability_path).read_bytes() == b""
    assert len(rotated_path.read_text(encoding="utf-8").splitlines()) == 1


def test_observation_outbox_recovers_after_mark_failure_without_duplicate_row(
    tmp_path, monkeypatch
):
    _seed(tmp_path)
    dispatcher, remote, _clock = _dispatcher(tmp_path)
    store = dispatcher.store
    original_mark = store.mark_delivery_observation_appended
    calls = 0

    def fail_once(**kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("simulated observation marker crash")
        return original_mark(**kwargs)

    monkeypatch.setattr(store, "mark_delivery_observation_appended", fail_once)
    with pytest.raises(RuntimeError, match="simulated observation marker crash"):
        dispatcher.dispatch_one()
    assert remote.add_calls == 1
    assert store.pending_delivery_observation_count() == 1
    receipt_path = Path(dispatcher.config.observability_path)
    assert len(receipt_path.read_text(encoding="utf-8").splitlines()) == 1

    monkeypatch.setattr(store, "mark_delivery_observation_appended", original_mark)
    outcome = dispatcher.dispatch_one()
    assert outcome.status == "idle"
    assert remote.add_calls == 1
    assert store.pending_delivery_observation_count() == 0
    assert len(receipt_path.read_text(encoding="utf-8").splitlines()) == 1


def test_observation_uses_evaluator_hits_and_business_acceptance_boundary(tmp_path):
    store = _seed(tmp_path)
    dispatcher, _remote, _clock = _dispatcher(tmp_path)
    claim = store.claim_due_effect(lease_owner="observation-test", now=NOW)
    assert claim is not None
    claim = replace(
        claim,
        contract={
            "schema_version": "g1q3_delivery_contract_v1",
            "quality_classification": "insufficient_evidence",
            "upstream_dispatch": {"hit_evaluator_keys": ["evaluator.alpha"]},
            "evidence_channel_msg_count": 3,
        },
        payload={"terminal_class": ""},
        effect_created_at=(NOW - timedelta(seconds=100)).isoformat(),
        business_accepted_at=(NOW - timedelta(seconds=10)).isoformat(),
    )
    fields = dispatcher._delivery_observation_fields(
        claim,
        content="observed fact",
        remote_id="comment-observation",
        delivered_at=NOW,
    )
    assert fields["level"] == "L1_observation"
    assert fields["has_attribution"] is True
    assert fields["evaluator_hit_count"] == 1
    assert fields["evidence_channel_msg_count"] == 3
    assert fields["pipeline_elapsed_seconds"] == 10.0


def test_observation_uses_supported_gate_a_hits_without_legacy_dispatch(tmp_path):
    store = _seed(tmp_path)
    dispatcher, _remote, _clock = _dispatcher(tmp_path)
    claim = store.claim_due_effect(lease_owner="gate-a-observation-test", now=NOW)
    assert claim is not None
    projection = project_gate_a_report({
        "input_materialized": True,
        "rca_evaluators": [
            {
                "key": "evaluator.supported",
                "status": "supported",
                "evidence_refs": [{"signal": "supported_signal"}],
            },
            {
                "key": "evaluator.refuted",
                "status": "refuted",
                "evidence_refs": [{"signal": "refuted_signal"}],
            },
        ],
    })
    claim = replace(
        claim,
        contract={
            "schema_version": "g1q3_delivery_contract_v1",
            "gate_a_projection": projection,
            "evidence_channel_msg_count": 3,
        },
        payload={"terminal_class": ""},
        effect_created_at=(NOW - timedelta(seconds=100)).isoformat(),
        business_accepted_at=(NOW - timedelta(seconds=10)).isoformat(),
    )

    fields = dispatcher._delivery_observation_fields(
        claim,
        content="observed fact",
        remote_id="comment-gate-a-supported",
        delivered_at=NOW,
    )

    assert fields["level"] == "L1_observation"
    assert fields["has_attribution"] is True
    assert fields["evaluator_hit_count"] == 1


def test_observation_does_not_count_refuted_gate_a_observations_as_hits(tmp_path):
    store = _seed(tmp_path)
    dispatcher, _remote, _clock = _dispatcher(tmp_path)
    claim = store.claim_due_effect(lease_owner="gate-a-refuted-test", now=NOW)
    assert claim is not None
    projection = project_gate_a_report({
        "input_materialized": True,
        "rca_evaluators": [
            {
                "key": "evaluator.refuted",
                "status": "refuted",
                "evidence_refs": [{"signal": "refuted_signal"}],
            }
        ],
    })
    claim = replace(
        claim,
        contract={
            "schema_version": "g1q3_delivery_contract_v1",
            "gate_a_projection": projection,
        },
        payload={"terminal_class": ""},
        effect_created_at=(NOW - timedelta(seconds=100)).isoformat(),
        business_accepted_at=(NOW - timedelta(seconds=10)).isoformat(),
    )

    fields = dispatcher._delivery_observation_fields(
        claim,
        content="refuting fact",
        remote_id="comment-gate-a-refuted",
        delivered_at=NOW,
    )

    assert fields["level"] == "L1_observation"
    assert fields["has_attribution"] is False
    assert fields["evaluator_hit_count"] == 0


def test_legacy_primary_success_without_gate_a_is_quarantined_before_provider_write(
    tmp_path,
):
    store = _seed(tmp_path)
    with sqlite3.connect(store.db_path) as conn:
        row = conn.execute(
            "SELECT delivery_id, contract_json FROM rca_delivery_jobs"
        ).fetchone()
        assert row is not None
        contract = json.loads(str(row[1]))
        contract.pop("gate_a_projection", None)
        conn.execute(
            "UPDATE rca_delivery_jobs SET contract_json = ? WHERE delivery_id = ?",
            (
                json.dumps(contract, ensure_ascii=False, sort_keys=True),
                str(row[0]),
            ),
        )

    dispatcher, remote, _clock = _dispatcher(tmp_path)
    outcome = dispatcher.dispatch_one()

    assert outcome.status == "quarantined"
    assert outcome.error_code == "delivery_gate_a_projection_required"
    assert remote.add_calls == 0
    assert remote.update_field_calls == 0


def test_default_config_is_disabled_and_comment_write_is_closed(tmp_path):
    config = DispatcherConfig.from_env({}, hermes_home=tmp_path)
    assert config.enabled is False
    assert config.public_dict()["external_writes"] is False
    assert config.public_dict()["allowed_effect_kind"] == "feishu_issue_comment"
    assert config.public_dict()["allowed_effect_kinds"] == [
        "feishu_card_patch",
        "feishu_issue_comment",
        "feishu_thread_reply",
    ]
    assert config.public_dict()["effect_lease_keeper_enabled"] is True
    assert config.public_dict()["effect_lease_renew_interval_seconds"] == 10


def test_dispatcher_config_exposes_activation_required(tmp_path):
    config = DispatcherConfig.from_env(
        {"HERMES_RCA_DELIVERY_DISPATCHER_ACTIVATION_REQUIRED": "true"},
        hermes_home=tmp_path,
    )

    assert config.activation_required is True
    assert config.public_dict()["activation_required"] is True


@pytest.mark.parametrize("value", ["1", "0", "yes", "on", "off", ""])
def test_dispatcher_activation_required_rejects_boolean_aliases(tmp_path, value):
    with pytest.raises(ValueError, match="exactly true or false"):
        DispatcherConfig.from_env(
            {"HERMES_RCA_DELIVERY_DISPATCHER_ACTIVATION_REQUIRED": value},
            hermes_home=tmp_path,
        )


def test_feishu_thread_reader_lists_only_exact_origin_topic(monkeypatch):
    from gateway.platforms import feishu as feishu_module

    class RequestBuilder:
        def http_method(self, _value):
            return self

        def uri(self, _value):
            return self

        def queries(self, _value):
            return self

        def token_types(self, _value):
            return self

        def build(self):
            return object()

    class BaseRequest:
        @staticmethod
        def builder():
            return RequestBuilder()

    monkeypatch.setattr(feishu_module, "BaseRequest", BaseRequest, raising=False)
    monkeypatch.setattr(
        feishu_module,
        "HttpMethod",
        SimpleNamespace(GET="GET"),
        raising=False,
    )
    monkeypatch.setattr(
        feishu_module,
        "AccessTokenType",
        SimpleNamespace(TENANT="TENANT"),
        raising=False,
    )
    root = SimpleNamespace(
        message_id="om_root123",
        chat_id="oc_group123",
        thread_id="omt_thread123",
    )
    page = {
        "code": 0,
        "data": {
            "has_more": False,
            "items": [
                {
                    "message_id": "om_reply456",
                    "root_id": "om_root123",
                    "thread_id": "omt_thread123",
                    "msg_type": "text",
                    "body": {
                        "content": json.dumps({
                            "text": "marker\n发起人：@_user_1\nreport"
                        })
                    },
                    "mentions": [
                        {
                            "key": "@_user_1",
                            "id": "ou_requester123",
                            "id_type": "open_id",
                            "name": "Requester",
                        }
                    ],
                },
                {
                    "message_id": "om_other789",
                    "root_id": "om_other_root",
                    "thread_id": "omt_thread123",
                    "msg_type": "text",
                    "body": {"content": json.dumps({"text": "wrong root"})},
                },
            ],
        },
    }
    client = SimpleNamespace(
        im=SimpleNamespace(
            v1=SimpleNamespace(
                message=SimpleNamespace(
                    get=lambda _request: SimpleNamespace(
                        success=lambda: True,
                        data=SimpleNamespace(items=[root]),
                    )
                )
            )
        ),
        request=lambda _request: SimpleNamespace(
            raw=SimpleNamespace(content=json.dumps(page))
        ),
    )
    fake_adapter = SimpleNamespace(
        _client=client,
        _build_get_message_request=lambda message_id: message_id,
        _response_succeeded=lambda response: response.success(),
    )

    result = FeishuThreadReplyAdapter(fake_adapter).list_replies(
        "oc_group123", "topic:om_root123"
    )

    assert result == {
        "success": True,
        "comments": [
            {
                "remote_id": "om_reply456",
                "content": (
                    'marker\n发起人：<at user_id="ou_requester123"></at>\nreport'
                ),
            }
        ],
        "pages_read": 1,
    }


@pytest.mark.parametrize(
    ("remote_content", "mentions", "accepted"),
    [
        (
            "[RCA_TERMINAL:effect:terminal_failed:1]\n发起人：@_user_1\nresult",
            [{"key": "@_user_1", "id": "ou_requester123", "id_type": "open_id"}],
            True,
        ),
        (
            "[RCA_TERMINAL:effect:terminal_failed:1]\n发起人：@_user_1\nresult",
            [{"key": "@_user_1", "id": "ou_different123", "id_type": "open_id"}],
            False,
        ),
        (
            "[RCA_TERMINAL:effect:terminal_failed:1]\nresult\n发起人：@_user_1",
            [{"key": "@_user_1", "id": "ou_requester123", "id_type": "open_id"}],
            False,
        ),
        (
            "[RCA_TERMINAL:effect:terminal_failed:1]\n发起人：@_user_1 @_user_1\nresult",
            [{"key": "@_user_1", "id": "ou_requester123", "id_type": "open_id"}],
            False,
        ),
        (
            "[RCA_TERMINAL:effect:terminal_failed:1]\n发起人：@_user_1\nresult",
            [
                {"key": "@_user_1", "id": "ou_requester123", "id_type": "open_id"},
                {"key": "@_user_2", "id": "ou_different123", "id_type": "open_id"},
            ],
            False,
        ),
        (
            "[RCA_TERMINAL:effect:terminal_failed:1]\n发起人：@_user_1\nresult",
            [],
            False,
        ),
        (
            "[RCA_TERMINAL:effect:terminal_failed:1]\n发起人：@_user_1\nresult",
            [{"key": "@_user_1", "id": "ou_requester123", "id_type": "user_id"}],
            False,
        ),
        (
            "[RCA_TERMINAL:effect:terminal_failed:1]\n发起人：@_user_1\nchanged",
            [{"key": "@_user_1", "id": "ou_requester123", "id_type": "open_id"}],
            False,
        ),
    ],
)
def test_feishu_thread_mention_rendering_requires_exact_requester_body(
    remote_content,
    mentions,
    accepted,
):
    marker = "[RCA_TERMINAL:effect:terminal_failed:1]"
    expected = f'{marker}\n发起人：<at user_id="ou_requester123"></at>\nresult'
    restored = FeishuThreadReplyAdapter._restore_single_text_mention(
        remote_content,
        mentions,
    )

    matches = dispatcher_module._confirmed_content_matches(
        [{"remote_id": "om_reply456", "content": restored}],
        marker,
        expected,
    )

    assert bool(matches) is accepted


def test_feishu_thread_writer_preserves_topic_and_stable_uuid():
    calls = []

    async def send(chat_id, content, metadata=None):
        calls.append((chat_id, content, metadata))
        return SimpleNamespace(success=True, message_id="om_reply456", error=None)

    fake_adapter = SimpleNamespace(send=send)
    with dispatcher_module._bound_provider_write_guard(_TEST_PROVIDER_WRITE_CLAIM):
        result = FeishuThreadReplyAdapter(fake_adapter).add_reply(
            "oc_group123",
            "topic:om_root123",
            "marker\nreport",
            "00000000-0000-0000-0000-000000000001",
        )

    assert result == {"success": True, "remote_id": "om_reply456"}
    assert len(calls) == 1
    chat_id, content, metadata = calls[0]
    assert chat_id == "oc_group123"
    assert content == "marker\nreport"
    assert {
        key: value
        for key, value in metadata.items()
        if key != "_pnc_rca_external_write_guard"
    } == {
        "thread_id": "topic:om_root123",
        "idempotency_uuid": "00000000-0000-0000-0000-000000000001",
        "_pnc_rca_external_write_operation": "feishu_thread_reply",
    }
    assert metadata["_pnc_rca_external_write_guard"] is _TEST_PROVIDER_WRITE_CLAIM


def test_feishu_thread_writer_rejects_wrong_anchor_before_provider_call(
    monkeypatch,
):
    calls = []

    async def send(*args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(success=True, message_id="om_unexpected", error=None)

    def exact_revalidate(_claim, *, operation, thread_id="", **_kwargs):
        assert operation == "feishu_thread_reply"
        if thread_id != "topic:om_expected":
            raise dispatcher_module.ExternalWriteFenceError(
                "external_write_fence_target_mismatch"
            )
        return _test_provider_revalidate(
            _claim, operation=operation, thread_id=thread_id
        )

    monkeypatch.setattr(
        dispatcher_module,
        "revalidate_provider_write_claim",
        exact_revalidate,
    )
    with dispatcher_module._bound_provider_write_guard(_TEST_PROVIDER_WRITE_CLAIM):
        with pytest.raises(
            dispatcher_module.ExternalWriteFenceError,
            match="external_write_fence_target_mismatch",
        ):
            FeishuThreadReplyAdapter(SimpleNamespace(send=send)).add_reply(
                "oc_group123",
                "topic:om_wrong",
                "must not send",
                "00000000-0000-0000-0000-000000000001",
            )

    assert calls == []


def test_feishu_thread_reader_has_a_hard_deadline(monkeypatch):
    def slow_get(_request):
        time.sleep(0.05)
        return SimpleNamespace(success=lambda: True, data=SimpleNamespace(items=[]))

    fake_adapter = SimpleNamespace(
        _client=SimpleNamespace(
            im=SimpleNamespace(
                v1=SimpleNamespace(message=SimpleNamespace(get=slow_get))
            )
        ),
        _build_get_message_request=lambda message_id: message_id,
        _response_succeeded=lambda response: response.success(),
    )
    monkeypatch.setattr(
        dispatcher_module,
        "MEEGLE_COMMENT_PAGE_TIMEOUT_SECONDS",
        0.01,
    )

    result = asyncio.run(
        FeishuThreadReplyAdapter(fake_adapter)._resolve_thread_id(
            "oc_group123", "om_root123"
        )
    )

    assert result["success"] is False
    assert result["error_code"] == "feishu_thread_read_timeout"


def test_feishu_thread_write_timeout_is_outcome_uncertain(monkeypatch):
    async def slow_send(_chat_id, _content, metadata=None):
        await asyncio.sleep(0.05)
        return SimpleNamespace(success=True, message_id="om_late", error=None)

    monkeypatch.setattr(
        dispatcher_module,
        "MEEGLE_COMMENT_PAGE_TIMEOUT_SECONDS",
        0.01,
    )

    with dispatcher_module._bound_provider_write_guard(_TEST_PROVIDER_WRITE_CLAIM):
        result = FeishuThreadReplyAdapter(SimpleNamespace(send=slow_send)).add_reply(
            "oc_group123",
            "topic:om_root123",
            "marker\nreport",
            "00000000-0000-0000-0000-000000000001",
        )

    assert result["success"] is False
    assert result["outcome_uncertain"] is True
    assert result["error_code"] == "feishu_thread_reply_timeout"


def test_config_lease_exceeds_one_boundary_timeout_plus_margin(tmp_path):
    config = _config(tmp_path)
    assert config.lease_seconds > (
        max(
            config.report_http_timeout_seconds,
            MAX_EXTERNAL_BOUNDARY_TIMEOUT_SECONDS,
        )
        + LEASE_BOUNDARY_MARGIN_SECONDS
    )
    with pytest.raises(ValueError, match="maximum single boundary timeout"):
        replace(config, lease_seconds=30)


def test_success_requires_read_before_http_add_and_read_after_remote_id(tmp_path):
    store = _seed(tmp_path)
    dispatcher, remote, _clock = _dispatcher(tmp_path)

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "succeeded", outcome
    assert outcome.remote_id == "comment-1"
    assert remote.list_calls == 2
    assert remote.add_calls == 1
    assert remote.update_field_calls == 1
    assert remote.get_field_calls == 3
    assert remote.history == [
        "list_comments",
        "get_fields",
        "update_fields",
        "get_fields",
        "add_comment",
        "list_comments",
        "get_fields",
    ]
    effect = store.list_rows("rca_delivery_effects")[0]
    job = store.list_rows("rca_delivery_jobs")[0]
    attempts = store.list_rows("rca_delivery_attempts")
    assert effect["status"] == "succeeded"
    assert job["status"] == "delivered"
    assert [row["outcome"] for row in attempts] == ["started", "ack"]
    receipt = json.loads(effect["remote_receipt_json"])
    assert receipt["remote_id"] == "comment-1"
    assert receipt["confirmed_field_keys"] == ["field_9193cb", "field_8c912e"]
    payload = json.loads(effect["payload_json"])
    assert payload["report_link_kind"] == "foxglove_viz"
    assert payload["project_key"] == "t03o4q"
    assert payload["project_simple_name"] == "g1q3"
    assert payload["issue_url"] == (
        "https://project.feishu.cn/g1q3/issue/detail/7041712812"
    )
    assert job["issue_url"] == payload["issue_url"]
    assert remote.fields["field_8c912e"] == payload["report_url"]
    assert payload["report_url"] in remote.comments[0]["content"]
    assert payload["foxglove_url"] == payload["report_url"]
    assert receipt["confirmed_report_url"] == payload["report_url"]
    assert (
        receipt["confirmed_content_sha256"]
        == hashlib.sha256(payload["comment_content"].encode("utf-8")).hexdigest()
    )


def test_terminal_comment_only_preserves_existing_causal_result(tmp_path):
    store = _seed_terminal(tmp_path)
    dispatcher, remote, _clock = _dispatcher(tmp_path)
    existing_result = (
        "归因结论：ACC 异常退出判据命中。\n"
        "责任模块：ACC 功能链\n"
        "因果关系：状态机抑制标志异常导致 ACC 退出。\n"
        "关键证据：退出判据与抑制标志在同一时间窗内命中。"
    )
    existing_report = "https://viewer.internal/G1Q3_RCA/cases/previous/index.html"
    remote.fields.update({
        "field_9193cb": existing_result,
        "field_8c912e": existing_report,
    })

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "succeeded"
    assert outcome.error_code == ""
    assert remote.add_calls == 1
    assert remote.update_field_calls == 0
    assert remote.fields["field_9193cb"] == existing_result
    assert remote.fields["field_8c912e"] == existing_report
    effect = store.list_rows("rca_delivery_effects")[0]
    assert effect["status"] == "succeeded"
    assert effect["write_phase"] == "settled"
    assert store.list_rows("rca_delivery_jobs")[0]["status"] == "delivered"


@pytest.mark.parametrize(
    ("existing", "proposed", "guarded"),
    [
        (
            "归因结论：ACC 退出。\n责任模块：ACC 功能链",
            "自动RCA未归因：事件在当前生产数据源中不存在。",
            True,
        ),
        (
            "",
            "自动RCA未归因：事件在当前生产数据源中不存在。",
            False,
        ),
        (
            "归因结论：ACC 退出。\n责任模块：ACC 功能链",
            "归因结论：ACC 状态机退出。\n责任模块：ACC 功能链",
            False,
        ),
    ],
)
def test_quality_regression_guard_only_blocks_causal_to_noncausal(
    existing, proposed, guarded
):
    assert (
        dispatcher_module._quality_regression_guard(
            {"field_9193cb": existing},
            (("field_9193cb", proposed),),
        )
        is guarded
    )


def test_html_only_causal_result_is_held_before_delivery_creation(tmp_path):
    control, result = _control(tmp_path)
    _bind_activation_execution(control, result, state="steady_active")
    collector = _collector(
        tmp_path,
        bundle_reader=lambda _claim: _html_only_bundle_payload(),
    )

    outcomes = collector.collect_batch()

    assert outcomes[0].status == "failure_hold"
    assert collector.store.list_rows("rca_delivery_effects") == []


def test_postwrite_body_without_marker_never_acks_and_enters_uncertain(tmp_path):
    _seed(tmp_path)

    class TruncatingRemote(Remote):
        def add_comment(self, project_key, work_item_id, content):
            result = super().add_comment(project_key, work_item_id, content)
            self.comments[-1]["content"] = content.splitlines()[0]
            return result

    remote = TruncatingRemote()
    dispatcher, _remote, _clock = _dispatcher(tmp_path, remote=remote)

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "uncertain"
    assert outcome.error_code == "feishu_postwrite_confirmation_mismatch"
    assert remote.add_calls == 1
    assert remote.update_field_calls == 1
    assert remote.comments[0]["content"] in {
        "本单未能定向",
        "建议责任方：视觉感知",
        "建议责任方：纵向控制",
    }


def test_existing_marker_repairs_drifted_fields_without_duplicate_comment(tmp_path):
    store = _seed(tmp_path)
    payload = json.loads(store.list_rows("rca_delivery_effects")[0]["payload_json"])
    remote = Remote()
    remote.comments.append({
        "remote_id": "comment-existing",
        "content": payload["comment_content"],
    })
    remote.fields = {"field_9193cb": "stale", "field_8c912e": ""}
    dispatcher, _remote, _clock = _dispatcher(tmp_path, remote=remote)

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "reconciled"
    assert outcome.remote_id == "comment-existing"
    assert remote.add_calls == 0
    assert remote.update_field_calls == 1
    assert remote.fields == {
        item["field_key"]: item["field_value"] for item in payload["field_updates"]
    }
    receipt = json.loads(
        store.list_rows("rca_delivery_effects")[0]["remote_receipt_json"]
    )
    assert receipt["source"] == "field_repair_after_marker"
    assert receipt["confirmed_report_url"] == payload["report_url"]
    assert (
        receipt["confirmed_content_sha256"]
        == hashlib.sha256(payload["comment_content"].encode("utf-8")).hexdigest()
    )


def test_meegle_normalized_marker_reconciles_without_duplicate_comment(tmp_path):
    store = _seed(tmp_path)
    payload = json.loads(store.list_rows("rca_delivery_effects")[0]["payload_json"])
    remote = Remote()
    normalized_content = payload["comment_content"].replace(
        payload["marker"], payload["marker"][1:-1], 1
    )
    remote.comments.append({
        "remote_id": "comment-existing",
        "content": normalized_content,
    })
    remote.fields = {
        item["field_key"]: item["field_value"] for item in payload["field_updates"]
    }
    dispatcher, _remote, _clock = _dispatcher(tmp_path, remote=remote)

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "reconciled"
    assert outcome.remote_id == "comment-existing"
    assert remote.add_calls == 0
    assert remote.update_field_calls == 0


def test_marker_only_remote_comment_is_quarantined_before_primary_report_probe(
    tmp_path,
):
    store = _seed(tmp_path)
    payload = json.loads(store.list_rows("rca_delivery_effects")[0]["payload_json"])
    remote = Remote()
    remote.comments.append({
        "remote_id": "comment-marker-only",
        "content": payload["marker"],
    })
    remote.fields = {
        item["field_key"]: item["field_value"] for item in payload["field_updates"]
    }
    verifier_calls = []

    def verifier(url, size, sha256):
        verifier_calls.append(url)
        return _verified_report(url, size, sha256)

    dispatcher, _remote, _clock = _dispatcher(
        tmp_path, remote=remote, verifier=verifier
    )

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "quarantined"
    assert outcome.error_code == "delivery_remote_content_mismatch"
    assert verifier_calls == []
    assert remote.add_calls == 0
    assert remote.update_field_calls == 0


def test_existing_marker_reconciles_without_html_report_service(tmp_path):
    store = _seed(tmp_path)
    payload = json.loads(store.list_rows("rca_delivery_effects")[0]["payload_json"])
    remote = Remote()
    remote.comments.append({
        "remote_id": "comment-existing",
        "content": payload["comment_content"],
    })
    remote.fields = {
        item["field_key"]: item["field_value"] for item in payload["field_updates"]
    }

    def unavailable(*_args):
        raise OSError("report service unavailable")

    dispatcher, _remote, _clock = _dispatcher(
        tmp_path, remote=remote, verifier=unavailable
    )

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "reconciled"
    assert remote.add_calls == 0
    assert remote.update_field_calls == 0


def test_prior_write_uncertainty_reconciles_without_html_report_service(tmp_path):
    _seed(tmp_path)
    remote = Remote()
    remote.weak_success = True
    first_dispatcher, _remote, clock = _dispatcher(tmp_path, remote=remote)

    first = first_dispatcher.dispatch_one()
    assert first.status == "uncertain"
    clock.current += timedelta(seconds=2)

    def unavailable(*_args):
        raise OSError("report service unavailable")

    second_dispatcher, _remote, _clock = _dispatcher(
        tmp_path,
        remote=remote,
        verifier=unavailable,
        clock=clock,
    )
    second = second_dispatcher.dispatch_one()

    assert second.status == "reconciled"
    assert remote.add_calls == 1


def test_remote_marker_matching_accepts_only_exact_meegle_normalization():
    marker = "[RCA_DELIVERY:effect-key:artifact-key]"
    comments = [
        {"remote_id": "exact", "content": marker},
        {"remote_id": "normalized", "content": marker[1:-1]},
        {"remote_id": "prefixed", "content": f"prefix {marker[1:-1]}"},
        {"remote_id": "suffixed", "content": f"{marker[1:-1]} suffix"},
    ]

    matches = dispatcher_module._marker_matches(comments, marker)

    assert [item["remote_id"] for item in matches] == ["exact", "normalized"]


def test_remote_terminal_marker_matching_accepts_meegle_inserted_spaces():
    marker = "[RCA_TERMINAL:effect-key:terminal_failed:2]"
    comments = [
        {
            "remote_id": "normalized",
            "content": "RCA_TERMINAL:effect-key :terminal_failed: 2",
        },
        {
            "remote_id": "prefixed",
            "content": "prefix RCA_TERMINAL:effect-key :terminal_failed: 2",
        },
        {
            "remote_id": "suffixed",
            "content": "RCA_TERMINAL:effect-key :terminal_failed: 2 suffix",
        },
        {
            "remote_id": "bracketed",
            "content": "[RCA_TERMINAL:effect-key :terminal_failed: 2]",
        },
    ]

    matches = dispatcher_module._marker_matches(comments, marker)

    assert [item["remote_id"] for item in matches] == ["normalized", "bracketed"]


def test_remote_content_matching_accepts_strict_meegle_rendering_only():
    marker = "[RCA_DELIVERY:effect-key:artifact-key]"
    url = "https://192.168.21.217/?ds=foxglove-http&ds.mcapPath=/formal.viz.mcap"
    expected = f"{marker}\nFoxglove 归因报告：{url}\n说明：需人工复核。"
    rendered = (
        f"{marker[1:-1]}\n\nFoxglove 归因报告：[{url}]({url})\n\n说明：需人工复核。\n"
    )
    mismatched = rendered.replace(f"]({url})", "](https://example.invalid/)")
    comments = [
        {"remote_id": "rendered", "content": rendered},
        {"remote_id": "mismatched", "content": mismatched},
    ]

    matches = dispatcher_module._confirmed_content_matches(comments, marker, expected)

    assert [item["remote_id"] for item in matches] == ["rendered"]


def test_remote_content_matching_accepts_meegle_numeric_list_rendering():
    marker = "[RCA_DELIVERY:effect-key:artifact-key]"
    expected = f"{marker}\n候选结论：目标 ID [1, 67]（活动槽位 [5, 9, 14, 16]）"
    comments = [
        {
            "remote_id": "rendered",
            "content": (
                f"{marker[1:-1]}\n\n候选结论：目标 ID 1, 67（活动槽位 5, 9, 14, 16）\n"
            ),
        },
        {
            "remote_id": "changed-value",
            "content": (
                f"{marker[1:-1]}\n\n候选结论：目标 ID 1, 68（活动槽位 5, 9, 14, 16）\n"
            ),
        },
        {
            "remote_id": "changed-text-brackets",
            "content": (
                f"{marker[1:-1]}\n\n候选结论："
                "目标 ID [1, 67]（活动槽位 5, 9, 14, 16）\n"
            ).replace("候选结论：", "候选结论"),
        },
    ]

    matches = dispatcher_module._confirmed_content_matches(comments, marker, expected)

    assert [item["remote_id"] for item in matches] == ["rendered"]


def test_field_update_failure_blocks_comment_and_retries(tmp_path):
    store = _seed(tmp_path)
    remote = Remote()
    remote.update_field_failure = {
        "success": False,
        "outcome_uncertain": False,
        "error_code": "feishu_permission_denied",
        "error": "forbidden",
    }
    dispatcher, _remote, _clock = _dispatcher(tmp_path, remote=remote)

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "circuit_open"
    assert outcome.error_code == "feishu_permission_denied"
    assert remote.add_calls == 0
    assert store.list_rows("rca_delivery_effects")[0]["status"] == "retry_wait"


def test_manual_subscription_delivers_issue_comment_and_origin_topic(tmp_path):
    store = _seed_with_thread_subscription(tmp_path)
    thread_remote = ThreadRemote()
    dispatcher, remote, _clock = _dispatcher(tmp_path, thread_remote=thread_remote)

    outcomes = [dispatcher.dispatch_one(), dispatcher.dispatch_one()]

    assert {outcome.status for outcome in outcomes} == {"succeeded"}
    assert remote.add_calls == 1
    assert thread_remote.add_calls == 1
    assert len(set(thread_remote.idempotency_uuids)) == 1
    effects = store.list_rows("rca_delivery_effects")
    assert {row["effect_kind"] for row in effects} == {
        "feishu_issue_comment",
        "feishu_thread_reply",
    }
    assert {row["status"] for row in effects} == {"succeeded"}
    assert store.list_rows("rca_delivery_jobs")[0]["status"] == "delivered"


def test_terminal_manual_delivery_skips_report_http_and_sends_both_effects(tmp_path):
    store = _seed_terminal(tmp_path, with_thread=True)
    before = store.health(now=NOW)
    assert before["delivery_job_outcomes"] == {"terminal_failed": 1}
    assert before["business_ready"] is True
    assert before["business_blockers"]["unresolved_required_effects"] == 2
    assert before["production_blockers"] == {
        "activation_schema_unavailable": 0,
        "activation_epoch_not_steady": 0,
        "activation_fingerprint_invalid": 0,
        "activation_gate_receipt_invalid": 0,
        "uncertain_effects": 0,
        "pending_delivery_observations": 0,
    }
    thread_remote = ThreadRemote()
    verifier_calls = []

    def forbidden_verifier(*args):
        verifier_calls.append(args)
        raise AssertionError("terminal delivery must not verify report artifacts")

    dispatcher, remote, _clock = _dispatcher(
        tmp_path,
        thread_remote=thread_remote,
        verifier=forbidden_verifier,
    )
    existing_report = foxglove_url(canonical_viz_mcap_path("older-generation-success"))
    assert existing_report
    remote.fields["field_8c912e"] = existing_report

    outcomes = [dispatcher.dispatch_one(), dispatcher.dispatch_one()]

    assert {outcome.status for outcome in outcomes} == {"succeeded"}
    assert verifier_calls == []
    assert remote.add_calls == 1
    assert remote.update_field_calls == 0
    assert remote.fields["field_8c912e"] == existing_report
    assert thread_remote.add_calls == 1
    assert "本终态不改写" not in remote.comments[0]["content"]
    assert "第 1 代" not in remote.comments[0]["content"]
    assert "本终态不改写" not in thread_remote.comments[0]["content"]
    assert "sensitive backend detail" not in remote.comments[0]["content"]
    assert "sensitive backend detail" not in thread_remote.comments[0]["content"]
    assert '<at user_id="ou_requester789"></at>' in thread_remote.comments[0]["content"]
    assert store.list_rows("rca_delivery_jobs")[0]["status"] == "delivered"
    assert {row["outcome"] for row in store.list_rows("rca_delivery_effects")} == {
        "terminal_failed"
    }
    after = store.health(now=NOW)
    assert after["delivery_job_outcomes"] == {"terminal_failed": 1}
    assert after["business_ready"] is True
    assert after["business_blockers"]["unresolved_required_effects"] == 0


def test_historical_terminal_v1_validates_as_comment_only(tmp_path):
    store = _seed_terminal(tmp_path)
    claim = store.claim_due_effect(
        lease_owner="legacy-terminal-validator",
        lease_seconds=60,
        now=NOW,
    )
    assert claim is not None
    legacy = build_terminal_delivery(
        business_key=claim.business_key,
        submission_key=claim.submission_key,
        generation=claim.generation,
        project_key=claim.project_key,
        work_item_type_key=claim.work_item_type_key,
        work_item_id=claim.work_item_id,
        outcome=claim.outcome,
        terminal_state=claim.terminal_state,
        error_code=claim.terminal_error_code,
        schema_version=TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_V1,
    )
    legacy_claim = replace(
        claim,
        effect_key=legacy.effect_key,
        delivery_id=legacy.delivery_id,
        target_key=legacy.target_key,
        payload=legacy.effect_payload,
        payload_sha256=legacy.semantic_payload_sha256,
        artifact_set_id=legacy.outcome_key,
        contract={},
    )

    validated = dispatcher_module._validate_effect(legacy_claim)

    assert validated.field_updates == ()
    assert validated.artifacts == ()
    assert "field_9193cb" not in json.dumps(legacy.effect_payload)


def test_bounded_terminal_v3_replays_oracle_low_at_dispatch_boundary(tmp_path):
    store = _seed_terminal(tmp_path)
    claim = store.claim_due_effect(
        lease_owner="fallback-v3-validator",
        lease_seconds=60,
        now=NOW,
    )
    assert claim is not None
    bounded = build_terminal_delivery(
        business_key=claim.business_key,
        submission_key=claim.submission_key,
        generation=claim.generation,
        project_key=claim.project_key,
        work_item_type_key=claim.work_item_type_key,
        work_item_id=claim.work_item_id,
        outcome=claim.outcome,
        terminal_state=claim.terminal_state,
        error_code=claim.terminal_error_code,
        terminal_fallback={
            "schema_version": "pnc_rca_bounded_terminal_fallback_v1",
            "work_started_at": NOW.isoformat(),
            "deadline_at": (NOW + timedelta(seconds=1800)).isoformat(),
            "elapsed_seconds": 1800,
            "confidence_tier": "low",
            "terminal_class": "honest_non_attribution",
            "route_key": "rca-failure-route-" + "a" * 64,
            "route_kind": "internal_alert",
            "route_owner": "rca-engineering",
        },
        schema_version=TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION,
    )
    bounded_claim = replace(
        claim,
        effect_key=bounded.effect_key,
        delivery_id=bounded.delivery_id,
        target_key=bounded.target_key,
        payload=bounded.effect_payload,
        payload_sha256=bounded.semantic_payload_sha256,
        artifact_set_id=bounded.outcome_key,
        outcome_key=bounded.outcome_key,
        contract=bounded.contract,
    )
    assert bounded_claim.payload["schema_version"] == (
        TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION
    )

    validated = dispatcher_module._validate_effect(bounded_claim)

    assert validated.field_updates == (
        ("field_9193cb", bounded_claim.payload["conclusion"]),
    )
    assert bounded_claim.payload["terminal_class"] == "honest_non_attribution"
    assert bounded_claim.payload["confidence_tier"] == "low"
    assert bounded_claim.payload["quality_oracle"]["schema_version"] == (
        "pnc_rca_structural_tier_oracle_v2"
    )


def test_profile_readiness_terminal_validates_with_explicit_detail(tmp_path):
    store = _seed_terminal(tmp_path)
    claim = store.claim_due_effect(
        lease_owner="profile-readiness-validator",
        lease_seconds=60,
        now=NOW,
    )
    assert claim is not None
    detail = (
        "已按官方字段路由到 mdrive4（数据 resolver=mdrive4_recorder_mcap_reference_v1，"
        "评测器=ct_evaluator_217_20260722，命名空间=rca/mdrive4），输入适配状态为 "
        "input_adapter_pending；本次不生成归因结论，不会进入 G1Q3，也不会回退到其他项目评测器"
    )
    readiness = build_terminal_delivery(
        business_key=claim.business_key,
        submission_key=claim.submission_key,
        generation=claim.generation,
        project_key=claim.project_key,
        work_item_type_key=claim.work_item_type_key,
        work_item_id=claim.work_item_id,
        outcome=claim.outcome,
        terminal_state=claim.terminal_state,
        error_code="business_profile_adapter_not_ready",
        diagnostic_code="business_adapter_not_ready",
        diagnostic_detail=detail,
    )
    readiness_claim = replace(
        claim,
        effect_key=readiness.effect_key,
        delivery_id=readiness.delivery_id,
        target_key=readiness.target_key,
        issue_url="https://project.feishu.cn/t03o4q/issue/detail/7041712812",
        payload=readiness.effect_payload,
        payload_sha256=readiness.semantic_payload_sha256,
        artifact_set_id=readiness.outcome_key,
        outcome_key=readiness.outcome_key,
        terminal_error_code="business_profile_adapter_not_ready",
        contract=readiness.contract,
    )

    validated = dispatcher_module._validate_effect(readiness_claim)

    assert "ct_evaluator_217_20260722" not in validated.content
    assert "不能跨项目借用其他归因能力" in validated.content
    assert validated.field_updates == ()


def test_current_profile_terminal_without_w3_dispatches_comment_only_with_scoped_claim(
    tmp_path,
    monkeypatch,
):
    store = _seed_profile_terminal(tmp_path, split_project_identity=True)
    captured_claims = []
    original_builder = dispatcher_module.build_profile_terminal_provider_claim

    def capture_claim(**kwargs):
        claim = original_builder(**kwargs)
        captured_claims.append(claim)
        return claim

    monkeypatch.setattr(
        dispatcher_module,
        "build_profile_terminal_provider_claim",
        capture_claim,
    )
    remote = Remote(project_key=REAL_G1Q3_PROJECT_KEY)
    dispatcher, remote, _clock = _dispatcher(tmp_path, remote=remote)

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "succeeded"
    assert remote.add_calls == 1
    assert remote.update_field_calls == 0
    assert len(captured_claims) == 1
    claim_payload = captured_claims[0].payload()
    assert claim_payload["authority_kind"] == "profile_terminal"
    assert claim_payload["authority"]["operation"] == "feishu_issue_comment"
    assert claim_payload["authority"]["project_key"] == REAL_G1Q3_PROJECT_KEY
    assert (
        claim_payload["authority"]["project_simple_name"]
        == REAL_G1Q3_PROJECT_SIMPLE_NAME
    )
    [effect] = store.list_rows("rca_delivery_effects")
    [job] = store.list_rows("rca_delivery_jobs")
    assert effect["status"] == "succeeded"
    assert effect["target_key"].startswith(
        f"feishu_project:{REAL_G1Q3_PROJECT_KEY}:"
    )
    assert job["status"] == "delivered"
    assert job["project_key"] == REAL_G1Q3_PROJECT_KEY
    assert job["issue_url"].startswith(
        f"https://project.feishu.cn/{REAL_G1Q3_PROJECT_SIMPLE_NAME}/"
    )


def test_pre_submit_quarantine_without_w3_remains_internal_and_redacted(tmp_path):
    control, _result = _control(tmp_path, completed=False)
    outbox = control.claim_outbox(lease_owner="submission-worker", now=NOW)
    assert outbox is not None
    control.quarantine_outbox(
        outbox_id=outbox.outbox_id,
        lease_token=outbox.lease_token,
        error_code="issue_field_invalid_frame_reference",
        error_detail="private frame value SECRET-MUST-NOT-LEAK",
        now=NOW + timedelta(seconds=1),
    )
    store = RcaDeliveryStore(control.db_path)

    assert store.backfill_completed_submissions(now=NOW + timedelta(seconds=2)) == 1

    [watch] = store.list_rows("rca_execution_watch")
    assert watch["state"] == "quarantined"
    assert watch["delivery_id"] is None
    status = json.loads(watch["last_status_json"])
    assert status["external_writes"] is False
    assert status["terminal_delivery_policy"] == "silent_internal_alert_only"
    assert status["error_code"] == "w3_execution_snapshot_missing"
    assert "issue_field_invalid_frame_reference" not in watch["last_status_json"]
    assert "SECRET-MUST-NOT-LEAK" not in watch["last_status_json"]
    assert store.list_rows("rca_delivery_jobs") == []
    assert store.list_rows("rca_delivery_effects") == []


def test_older_terminal_generation_is_suppressed_before_any_remote_call(tmp_path):
    store = _seed_terminal(tmp_path)
    [old_job] = store.list_rows("rca_delivery_jobs")
    newer_delivery_id = "g1q3-rca-delivery-v1-" + "9" * 64
    current = NOW.isoformat()
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            """
            INSERT INTO rca_delivery_jobs(
                delivery_id, submission_key, business_key, generation,
                artifact_set_id, project_key, work_item_type_key, work_item_id,
                target_key, issue_url, report_url, outcome, outcome_key,
                terminal_state, terminal_error_code, status, manifest_json,
                contract_json, artifacts_json, created_at, updated_at
            ) VALUES (?, ?, ?, 2, ?, ?, ?, ?, ?, ?, ?, 'terminal_failed', '',
                      'failed', 'vm_terminal_failed_unclassified',
                      'delivered', '{}', '{}', '[]', ?, ?)
            """,
            (
                newer_delivery_id,
                "newer-success-submission",
                old_job["business_key"],
                "g1q3-rca-artifact-v1-" + "8" * 64,
                old_job["project_key"],
                old_job["work_item_type_key"],
                old_job["work_item_id"],
                old_job["target_key"],
                "https://project.feishu.cn/t03o4q/issue/detail/7041712812",
                build_report_url(
                    "newer-success-submission",
                    "g1q3-rca-artifact-v1-" + "8" * 64,
                ),
                current,
                current,
            ),
        )
        conn.execute(
            """
            INSERT INTO rca_delivery_effects(
                effect_key, delivery_id, effect_kind, required, target_key,
                payload_json, payload_sha256, outcome, write_phase, status,
                completed_at, created_at, updated_at
            ) VALUES (?, ?, 'feishu_issue_comment', 1, ?, ?, ?, 'terminal_failed',
                      'settled', 'succeeded', ?, ?, ?)
            """,
            (
                "g1q3-rca-effect-v1-" + "7" * 64,
                newer_delivery_id,
                old_job["target_key"],
                json.dumps({
                    "field_updates": [
                        {
                            "field_key": "field_9193cb",
                            "field_value": "newer terminal result",
                        }
                    ]
                }),
                "6" * 64,
                current,
                current,
                current,
            ),
        )
    dispatcher, remote, _clock = _dispatcher(tmp_path)

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "superseded"
    assert outcome.error_code == ("delivery_effect_superseded_by_newer_settled_fields")
    assert remote.history == []
    [old_effect] = [
        row
        for row in store.list_rows("rca_delivery_effects")
        if row["delivery_id"] == old_job["delivery_id"]
    ]
    assert old_effect["status"] == "suppressed"
    assert old_effect["write_phase"] == "settled"


def test_write_boundary_rechecks_newer_settled_terminal_field_effect(tmp_path):
    store = _seed_terminal(tmp_path)
    claim = store.claim_due_effect(
        lease_owner="write-boundary-race",
        lease_seconds=60,
        now=NOW,
    )
    assert claim is not None
    assert (
        store.suppress_terminal_effect_if_newer_settled_fields(
            claim=claim,
            now=NOW,
        )
        is None
    )
    newer_delivery_id = "g1q3-rca-terminal-delivery-v1-" + "5" * 64
    newer_effect_key = "g1q3-rca-terminal-effect-v1-" + "4" * 64
    current = NOW.isoformat()
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            """
            INSERT INTO rca_delivery_jobs(
                delivery_id, submission_key, business_key, generation,
                artifact_set_id, project_key, work_item_type_key, work_item_id,
                target_key, issue_url, report_url, outcome, outcome_key,
                terminal_state, terminal_error_code, status, manifest_json,
                contract_json, artifacts_json, created_at, updated_at
            ) VALUES (?, ?, ?, 2, ?, ?, ?, ?, ?, '', '', 'terminal_failed', ?,
                      'failed', 'vm_terminal_failed_unclassified', 'delivered',
                      '{}', '{}', '[]', ?, ?)
            """,
            (
                newer_delivery_id,
                "newer-terminal-submission",
                claim.business_key,
                "g1q3-rca-terminal-v1-" + "3" * 64,
                claim.project_key,
                claim.work_item_type_key,
                claim.work_item_id,
                claim.target_key,
                "g1q3-rca-terminal-v1-" + "3" * 64,
                current,
                current,
            ),
        )
        conn.execute(
            """
            INSERT INTO rca_delivery_effects(
                effect_key, delivery_id, effect_kind, required, target_key,
                payload_json, payload_sha256, outcome, write_phase, status,
                completed_at, created_at, updated_at
            ) VALUES (?, ?, 'feishu_issue_comment', 1, ?, ?, ?,
                      'terminal_failed', 'settled', 'succeeded', ?, ?, ?)
            """,
            (
                newer_effect_key,
                newer_delivery_id,
                claim.target_key,
                json.dumps({
                    "field_updates": [
                        {
                            "field_key": "field_9193cb",
                            "field_value": "newer terminal result",
                        }
                    ]
                }),
                "2" * 64,
                current,
                current,
                current,
            ),
        )

    mutation = store.mark_effect_write_started(claim=claim, now=NOW)

    assert mutation is not None
    assert mutation.effect_status == "suppressed"
    old_effect = next(
        row
        for row in store.list_rows("rca_delivery_effects")
        if row["effect_key"] == claim.effect_key
    )
    assert old_effect["status"] == "suppressed"
    assert old_effect["write_phase"] == "settled"
    receipt = json.loads(old_effect["remote_receipt_json"])
    assert receipt["superseding_effect_key"] == newer_effect_key
    assert receipt["superseding_outcome"] == "terminal_failed"


def test_terminal_v2_epoch_switch_blocks_field_write_and_comment(tmp_path):
    control, result = _control(tmp_path)
    _bind_activation_execution(control, result, state="steady_active")
    with sqlite3.connect(control.db_path) as conn:
        conn.execute(
            "UPDATE business_triggers SET created_at = ? WHERE submission_key = ?",
            (NOW.isoformat(), result.submission_key),
        )
    store = RcaDeliveryStore(control.db_path)
    assert store.backfill_completed_submissions(now=NOW) == 1
    watch = store.claim_due_watch(lease_owner="activation-collector", now=NOW)
    assert watch is not None
    store.create_terminal_delivery(
        claim=watch,
        status={"success": True, "state": "failed"},
        outcome="terminal_failed",
        terminal_state="failed",
        error_code="vm_terminal_failed_unclassified",
        error_detail="private detail",
        now=NOW,
    )

    class EpochSwitchRemote(Remote):
        switched = False

        def list_comments(self, project_key, work_item_id):
            result = super().list_comments(project_key, work_item_id)
            if not self.switched:
                self.switched = True
                _switch_activation_epoch(
                    control,
                    old_epoch="delivery-epoch-1",
                    new_epoch="delivery-epoch-2",
                )
            return result

    remote = EpochSwitchRemote()
    dispatcher, _remote, _clock = _dispatcher(tmp_path, remote=remote)

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "lease_lost", outcome
    assert remote.update_field_calls == 0
    assert remote.add_calls == 0


def test_terminal_process_kill_reconciles_without_report_or_second_send(tmp_path):
    store = _seed_terminal(tmp_path)
    remote = Remote()
    first = store.claim_due_effect(
        lease_owner="killed-terminal-worker", lease_seconds=60, now=NOW
    )
    assert first is not None
    assert first.outcome == "terminal_failed"
    store.mark_effect_write_started(claim=first, now=NOW)
    response = remote.add_comment(
        first.project_key,
        first.work_item_id,
        str(first.payload["comment_content"]),
    )
    assert response["success"] is True
    verifier_calls = []
    clock = Clock()
    clock.current = NOW + timedelta(seconds=61)
    dispatcher, _remote, _clock = _dispatcher(
        tmp_path,
        remote=remote,
        clock=clock,
        verifier=lambda *args: verifier_calls.append(args),
    )

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "reconciled"
    assert remote.add_calls == 1
    assert verifier_calls == []
    assert store.list_rows("rca_delivery_jobs")[0]["status"] == "delivered"


def test_terminal_process_kill_before_write_retries_exactly_once(tmp_path):
    store = _seed_terminal(tmp_path)
    first = store.claim_due_effect(
        lease_owner="killed-before-terminal-write", lease_seconds=60, now=NOW
    )
    assert first is not None
    assert first.write_phase == "prewrite"
    remote = Remote()
    clock = Clock()
    clock.current = NOW + timedelta(seconds=61)
    verifier_calls = []
    dispatcher, _remote, _clock = _dispatcher(
        tmp_path,
        remote=remote,
        clock=clock,
        verifier=lambda *args: verifier_calls.append(args),
    )

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "succeeded"
    assert remote.add_calls == 1
    assert verifier_calls == []
    assert store.list_rows("rca_delivery_effects")[0]["write_phase"] == "settled"


def test_terminal_process_kill_after_mark_before_issue_add_recovers(tmp_path):
    store = _seed_terminal(tmp_path)
    first = store.claim_due_effect(
        lease_owner="killed-after-terminal-mark", lease_seconds=60, now=NOW
    )
    assert first is not None
    store.mark_effect_write_started(claim=first, now=NOW)
    remote = Remote()
    clock = Clock()
    clock.current = NOW + timedelta(seconds=61)
    verifier_calls = []
    dispatcher, _remote, _clock = _dispatcher(
        tmp_path,
        remote=remote,
        clock=clock,
        verifier=lambda *args: verifier_calls.append(args),
    )

    outcomes = []
    for _ in range(3):
        outcomes.append(dispatcher.dispatch_one())
        clock.current += timedelta(seconds=30)

    assert [outcome.status for outcome in outcomes] == [
        "uncertain",
        "uncertain",
        "succeeded",
    ]
    assert remote.add_calls == 1
    assert remote.list_calls == 5
    assert verifier_calls == []
    effect = store.list_rows("rca_delivery_effects")[0]
    assert effect["status"] == "succeeded"
    assert effect["write_phase"] == "settled"
    assert effect["write_started_at"] == NOW.isoformat()
    assert effect["recovery_write_count"] == 1
    assert json.loads(effect["remote_receipt_json"])["source"] == (
        "read_after_recovery_write"
    )
    assert store.list_rows("rca_delivery_jobs")[0]["status"] == "delivered"


def test_terminal_topic_process_kill_reconciles_with_stable_uuid(tmp_path):
    store = _seed_terminal(tmp_path, with_thread=True)
    with sqlite3.connect(store.db_path) as connection:
        connection.execute(
            "UPDATE rca_delivery_effects SET created_at = ? "
            "WHERE effect_kind = 'feishu_issue_comment'",
            (NOW.isoformat(),),
        )
        connection.execute(
            "UPDATE rca_delivery_effects SET created_at = ? "
            "WHERE effect_kind = 'feishu_thread_reply'",
            ((NOW + timedelta(seconds=1)).isoformat(),),
        )
    thread_remote = ThreadRemote()
    dispatcher, remote, clock = _dispatcher(
        tmp_path,
        thread_remote=thread_remote,
        verifier=lambda *_args: (_ for _ in ()).throw(
            AssertionError("terminal delivery must not verify report artifacts")
        ),
    )
    assert dispatcher.dispatch_one().status == "succeeded"
    assert remote.add_calls == 1
    claim = store.claim_due_effect(
        lease_owner="killed-terminal-topic-worker",
        lease_seconds=60,
        now=NOW,
    )
    assert claim is not None
    assert claim.effect_kind == "feishu_thread_reply"
    first_uuid = claim.payload["idempotency_uuid"]
    store.mark_effect_write_started(claim=claim, now=NOW)
    response = thread_remote.add_reply(
        claim.payload["chat_id"],
        claim.payload["thread_id"],
        claim.payload["message_content"],
        first_uuid,
    )
    assert response["success"] is True

    clock.current = NOW + timedelta(seconds=61)
    outcome = dispatcher.dispatch_one()

    assert outcome.status == "reconciled"
    assert thread_remote.add_calls == 1
    assert thread_remote.idempotency_uuids == [first_uuid]
    assert store.list_rows("rca_delivery_jobs")[0]["status"] == "delivered"


def test_terminal_topic_process_kill_before_write_retries_with_stable_uuid(tmp_path):
    store = _seed_terminal(tmp_path, with_thread=True)
    with sqlite3.connect(store.db_path) as connection:
        connection.execute(
            "UPDATE rca_delivery_effects SET created_at = ? "
            "WHERE effect_kind = 'feishu_issue_comment'",
            (NOW.isoformat(),),
        )
        connection.execute(
            "UPDATE rca_delivery_effects SET created_at = ? "
            "WHERE effect_kind = 'feishu_thread_reply'",
            ((NOW + timedelta(seconds=1)).isoformat(),),
        )
    thread_remote = ThreadRemote()
    dispatcher, remote, clock = _dispatcher(tmp_path, thread_remote=thread_remote)
    assert dispatcher.dispatch_one().status == "succeeded"
    assert remote.add_calls == 1
    first = store.claim_due_effect(
        lease_owner="killed-before-terminal-topic-write",
        lease_seconds=60,
        now=NOW,
    )
    assert first is not None
    assert first.effect_kind == "feishu_thread_reply"
    stable_uuid = first.payload["idempotency_uuid"]

    clock.current = NOW + timedelta(seconds=61)
    outcome = dispatcher.dispatch_one()

    assert outcome.status == "succeeded"
    assert thread_remote.add_calls == 1
    assert thread_remote.idempotency_uuids == [stable_uuid]


def test_terminal_process_kill_after_mark_before_topic_add_recovers_with_uuid(
    tmp_path,
):
    store = _seed_terminal(tmp_path, with_thread=True)
    with sqlite3.connect(store.db_path) as connection:
        connection.execute(
            "UPDATE rca_delivery_effects SET created_at = ? "
            "WHERE effect_kind = 'feishu_issue_comment'",
            (NOW.isoformat(),),
        )
        connection.execute(
            "UPDATE rca_delivery_effects SET created_at = ? "
            "WHERE effect_kind = 'feishu_thread_reply'",
            ((NOW + timedelta(seconds=1)).isoformat(),),
        )
    thread_remote = ThreadRemote()
    dispatcher, remote, clock = _dispatcher(tmp_path, thread_remote=thread_remote)
    assert dispatcher.dispatch_one().status == "succeeded"
    assert remote.add_calls == 1
    first = store.claim_due_effect(
        lease_owner="killed-after-terminal-topic-mark",
        lease_seconds=60,
        now=NOW,
    )
    assert first is not None
    assert first.effect_kind == "feishu_thread_reply"
    stable_uuid = first.payload["idempotency_uuid"]
    store.mark_effect_write_started(claim=first, now=NOW)
    clock.current = NOW + timedelta(seconds=61)

    outcomes = []
    for _ in range(3):
        outcomes.append(dispatcher.dispatch_one())
        clock.current += timedelta(seconds=30)

    assert [outcome.status for outcome in outcomes] == [
        "uncertain",
        "uncertain",
        "succeeded",
    ]
    assert thread_remote.add_calls == 1
    assert thread_remote.list_calls == 5
    assert thread_remote.idempotency_uuids == [stable_uuid]
    thread_effect = next(
        row
        for row in store.list_rows("rca_delivery_effects")
        if row["effect_kind"] == "feishu_thread_reply"
    )
    assert thread_effect["status"] == "succeeded"
    assert thread_effect["recovery_write_count"] == 1
    assert json.loads(thread_effect["remote_receipt_json"])["source"] == (
        "read_after_recovery_write"
    )
    assert store.list_rows("rca_delivery_jobs")[0]["status"] == "delivered"


def test_thread_process_kill_reconciles_marker_without_second_send(tmp_path):
    store = _seed_with_thread_subscription(tmp_path)
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            "UPDATE rca_delivery_effects SET created_at = ? "
            "WHERE effect_kind = 'feishu_issue_comment'",
            (NOW.isoformat(),),
        )
        conn.execute(
            "UPDATE rca_delivery_effects SET created_at = ? "
            "WHERE effect_kind = 'feishu_thread_reply'",
            ((NOW + timedelta(seconds=1)).isoformat(),),
        )
    thread_remote = ThreadRemote()
    clock = Clock()
    dispatcher, remote, _clock = _dispatcher(
        tmp_path, thread_remote=thread_remote, clock=clock
    )
    assert dispatcher.dispatch_one().status == "succeeded"
    assert remote.add_calls == 1
    claim = store.claim_due_effect(
        lease_owner="killed-thread-worker",
        lease_seconds=60,
        now=NOW,
    )
    assert claim is not None
    assert claim.effect_kind == "feishu_thread_reply"
    store.mark_effect_write_started(claim=claim, now=NOW)
    response = thread_remote.add_reply(
        claim.payload["chat_id"],
        claim.payload["thread_id"],
        claim.payload["message_content"],
        claim.payload["idempotency_uuid"],
    )
    assert response["success"] is True

    clock.current = NOW + timedelta(seconds=61)
    outcome = dispatcher.dispatch_one()

    assert outcome.status == "reconciled"
    assert thread_remote.add_calls == 1
    assert store.list_rows("rca_delivery_jobs")[0]["status"] == "delivered"


def test_thread_circuit_opens_without_blocking_issue_comment(tmp_path):
    store = _seed_with_thread_subscription(tmp_path)
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            "UPDATE rca_delivery_effects SET created_at = ? "
            "WHERE effect_kind = 'feishu_thread_reply'",
            (NOW.isoformat(),),
        )
        conn.execute(
            "UPDATE rca_delivery_effects SET created_at = ? "
            "WHERE effect_kind = 'feishu_issue_comment'",
            ((NOW + timedelta(seconds=1)).isoformat(),),
        )
    thread_remote = ThreadRemote()
    thread_remote.add_failure = {
        "success": False,
        "outcome_uncertain": False,
        "error_code": "feishu_auth_failed",
        "error": "token expired",
    }
    dispatcher, remote, _clock = _dispatcher(tmp_path, thread_remote=thread_remote)

    first = dispatcher.dispatch_one()
    second = dispatcher.dispatch_one()

    assert first.status == "circuit_open"
    assert first.error_code == "feishu_auth_failed"
    assert second.status == "succeeded"
    assert remote.add_calls == 1
    assert store.delivery_dispatcher_circuit("feishu_thread_reply").is_open is True
    assert store.delivery_dispatcher_circuit("feishu_issue_comment").is_open is False


def test_delivery_verifies_viz_publication_before_external_comment(tmp_path):
    _seed(tmp_path, bundle_payload=_web_bundle_payload())
    calls = []

    def verifier(url, size, sha256):
        calls.append((url, size, sha256))
        return _verified_report(url, size, sha256)

    remote = Remote()
    add_comment = remote.add_comment

    def guarded_add(project_key, work_item_id, content):
        assert len(calls) == 1
        assert "?ds=foxglove-http&ds.mcapPath=" in calls[0][0]
        return add_comment(project_key, work_item_id, content)

    remote.add_comment = guarded_add
    dispatcher, remote, _clock = _dispatcher(tmp_path, remote=remote, verifier=verifier)

    assert dispatcher.dispatch_one().status == "succeeded"
    assert len(calls) == 1
    assert "?ds=foxglove-http&ds.mcapPath=" in calls[0][0]
    assert remote.add_calls == 1


def test_dispatcher_replays_focus_binding_and_rejects_title_tamper(tmp_path):
    title = "ACC braking issue"
    bundle = _web_bundle_payload()
    bundle["delivery_contract"]["schema_version"] = DELIVERY_CONTRACT_SCHEMA_VERSION
    bundle["delivery_contract"]["issue_focus"] = _focus_payload(
        title,
        status=ANALYSIS_INSUFFICIENT_STATEMENT,
    )
    bundle["report_issue_focus"] = bundle["delivery_contract"]["issue_focus"]
    store = _seed(tmp_path, bundle_payload=bundle)
    claim = store.claim_due_effect(lease_owner="worker-1", now=NOW)
    assert claim is not None

    validated = dispatcher_module._validate_effect(claim)

    assert validated.field_updates == (
        ("field_9193cb", claim.payload["result_field_value"]),
        ("field_8c912e", claim.payload["report_url"]),
    )
    tampered = replace(
        claim,
        payload={**claim.payload, "issue_title": "ACC different issue"},
    )
    with pytest.raises(DeliveryContractError) as raised:
        dispatcher_module._validate_effect(tampered)
    assert raised.value.code in {
        "issue_focus_effect_binding_invalid",
        "issue_focus_effect_binding_mismatch",
    }


def test_dispatcher_rejects_report_url_for_another_submission_before_http(tmp_path):
    store = _seed(tmp_path)
    claim = store.claim_due_effect(lease_owner="worker-1", now=NOW)
    assert claim is not None
    bad_url = build_report_url("g1q3-rca-s1-" + "f" * 64, claim.artifact_set_id)
    tampered = replace(
        claim,
        report_url=bad_url,
        payload={**claim.payload, "report_url": bad_url},
        manifest={**claim.manifest, "report_url": bad_url},
    )

    with pytest.raises(DeliveryContractError) as exc:
        dispatcher_module._validate_effect(tampered)

    assert exc.value.code == "delivery_effect_report_url_invalid"


@pytest.mark.parametrize("bad_shape", ["empty", "viz_mcap"])
def test_publication_report_url_counterexamples_fail_closed_before_http(
    tmp_path, bad_shape
):
    store = _seed(tmp_path)
    claim = store.claim_due_effect(lease_owner="worker-1", now=NOW)
    assert claim is not None
    bad_url = (
        "" if bad_shape == "empty" else canonical_viz_mcap_path(claim.submission_key)
    )
    tampered = replace(
        claim,
        report_url=bad_url,
        payload={**claim.payload, "report_url": bad_url},
        manifest={**claim.manifest, "report_url": bad_url},
    )

    with pytest.raises(DeliveryContractError) as exc:
        dispatcher_module._validate_effect(tampered)

    assert exc.value.code == "delivery_effect_report_url_invalid"


def test_validated_effect_binds_viz_publication_to_write_boundary_probe(tmp_path):
    store = _seed(tmp_path)
    claim = store.claim_due_effect(lease_owner="worker-1", now=NOW)
    assert claim is not None

    validated = dispatcher_module._validate_effect(claim)

    publication = claim.contract["artifacts"]["viz_publication"]
    assert validated.artifacts == (
        (
            "viz_mcap",
            claim.report_url,
            publication["size"],
            publication["sha256"],
        ),
    )


def test_dispatcher_rejects_html_report_link_kind_before_write(tmp_path):
    store = _seed(tmp_path)
    claim = store.claim_due_effect(lease_owner="worker-1", now=NOW)
    assert claim is not None
    tampered = replace(
        claim,
        payload={**claim.payload, "report_link_kind": "html_report"},
    )

    with pytest.raises(DeliveryContractError) as exc:
        dispatcher_module._validate_effect(tampered)

    assert exc.value.code == "delivery_effect_report_link_kind_invalid"


def test_dispatcher_rejects_internal_html_report_field_before_write(tmp_path):
    store = _seed(tmp_path)
    claim = store.claim_due_effect(lease_owner="worker-1", now=NOW)
    assert claim is not None
    field_updates = [dict(item) for item in claim.payload["field_updates"]]
    field_updates[1]["field_value"] = claim.manifest["report_url"]
    tampered = replace(
        claim,
        payload={**claim.payload, "field_updates": field_updates},
    )

    with pytest.raises(DeliveryContractError) as exc:
        dispatcher_module._validate_effect(tampered)

    assert exc.value.code == "delivery_effect_field_updates_invalid"


def test_dispatcher_rejects_tampered_v4_result_field_projection(tmp_path):
    store = _seed(tmp_path)
    claim = store.claim_due_effect(lease_owner="worker-1", now=NOW)
    assert claim is not None
    tampered = replace(
        claim,
        payload={
            **claim.payload,
            "result_field_value": "归因结论：伪造结论。\n责任模块：伪造模块",
        },
    )

    with pytest.raises(DeliveryContractError) as exc:
        dispatcher_module._validate_effect(tampered)

    assert exc.value.code == "delivery_result_field_projection_invalid"


def test_dispatcher_keeps_v3_result_field_bound_to_legacy_conclusion(tmp_path):
    store = _seed(tmp_path)
    claim = store.claim_due_effect(lease_owner="worker-1", now=NOW)
    assert claim is not None
    payload = dict(claim.payload)
    payload.pop("result_field_value")
    payload["schema_version"] = dispatcher_module.DELIVERY_EFFECT_SCHEMA_VERSION_V3
    field_updates = [dict(item) for item in payload["field_updates"]]
    field_updates[0]["field_value"] = payload["conclusion"]
    payload["field_updates"] = field_updates
    semantic_sha = dispatcher_module.compute_delivery_effect_payload_sha256(
        payload,
        claim.effect_kind,
    )
    effect_key = dispatcher_module.compute_delivery_effect_key(
        delivery_id=claim.delivery_id,
        effect_kind=claim.effect_kind,
        target_key=claim.target_key,
        semantic_payload_sha256=semantic_sha,
    )
    marker = dispatcher_module.delivery_effect_marker(
        effect_key,
        claim.artifact_set_id,
    )
    payload.update({
        "effect_key": effect_key,
        "semantic_payload_sha256": semantic_sha,
        "marker": marker,
        "comment_content": dispatcher_module.build_issue_comment_content(
            marker=marker,
            work_item_id=claim.work_item_id,
            report_status=str(payload.get("report_status") or ""),
            conclusion=str(payload["conclusion"]),
            report_url=claim.report_url,
            foxglove_url=str(payload.get("foxglove_url") or ""),
            report_cifs_path=str(payload.get("report_cifs_path") or ""),
            issue_url=claim.issue_url,
            terminal_class=str(payload.get("terminal_class") or ""),
        ),
    })
    legacy = replace(
        claim,
        effect_key=effect_key,
        payload_sha256=semantic_sha,
        payload=payload,
    )

    validated = dispatcher_module._validate_effect(legacy)

    assert validated.field_updates[0] == (
        "field_9193cb",
        payload["conclusion"],
    )


def test_dispatcher_rejects_v1_effect_even_when_forged_from_current_claim(tmp_path):
    store = _seed(tmp_path)
    claim = store.claim_due_effect(lease_owner="worker-1", now=NOW)
    assert claim is not None
    legacy = replace(
        claim,
        payload={
            **claim.payload,
            "schema_version": DELIVERY_EFFECT_SCHEMA_VERSION_V1,
        },
    )

    with pytest.raises(DeliveryContractError) as exc:
        dispatcher_module._validate_effect(legacy)

    assert exc.value.code == "delivery_effect_schema_unsupported"


def test_dispatcher_rejects_unhashed_arbitrary_comment_body(tmp_path):
    store = _seed(tmp_path)
    claim = store.claim_due_effect(lease_owner="worker-1", now=NOW)
    assert claim is not None
    tampered = replace(
        claim,
        payload={
            **claim.payload,
            "comment_content": (
                f"{claim.payload['marker']}\narbitrary body\n{claim.report_url}"
            ),
        },
    )

    with pytest.raises(DeliveryContractError) as exc:
        dispatcher_module._validate_effect(tampered)

    assert exc.value.code == "delivery_effect_content_invalid"


def test_dispatcher_rejects_gate_a_public_result_tamper_before_oracle(tmp_path):
    store = _seed(tmp_path)
    claim = store.claim_due_effect(lease_owner="worker-1", now=NOW)
    assert claim is not None
    contract = json.loads(json.dumps(claim.contract))
    contract["consumer_capability"] = _consumer_capability()
    _add_structural_candidate(contract)
    tampered = replace(claim, contract=contract)

    with pytest.raises(DeliveryContractError) as exc:
        dispatcher_module._validate_effect(tampered)

    assert exc.value.code == "delivery_gate_a_public_result_mismatch"


def test_dispatcher_rejects_self_consistent_unsealed_gate_a_identifiers(tmp_path):
    store = _seed(tmp_path)
    claim = store.claim_due_effect(lease_owner="worker-1", now=NOW)
    assert claim is not None
    malicious_binding = build_gate_a_identifier_binding({
        "actual_evaluators": [
            {"evaluator_id": "ACC_is_at_fault", "status": "supported"}
        ],
        "actual_signals": ["control_team_should_own"],
        "actual_fields": [],
    })
    projection = _project_gate_a_report(
        {
            "input_materialized": True,
            "rca_evaluators": [
                {
                    "key": "ACC_is_at_fault",
                    "status": "supported",
                    "evidence_refs": [{"signal": "control_team_should_own"}],
                }
            ],
        },
        identifier_binding=malicious_binding,
    )
    tampered = replace(
        claim,
        contract={
            **claim.contract,
            "gate_a_projection": projection,
            "public_result": build_gate_a_public_result(projection),
        },
    )

    with pytest.raises(DeliveryContractError) as exc:
        dispatcher_module._validate_effect(tampered)

    assert exc.value.code == "delivery_gate_a_projection_invalid"


def test_dispatcher_replays_oracle_and_rejects_tampered_contract_before_http(tmp_path):
    store = _seed(tmp_path)
    claim = store.claim_due_effect(lease_owner="worker-1", now=NOW)
    assert claim is not None
    contract = json.loads(json.dumps(claim.contract))
    canonical_public_result = contract["public_result"]
    contract["consumer_capability"] = _consumer_capability()
    _add_structural_candidate(contract)
    contract["public_result"] = canonical_public_result
    tampered = replace(claim, contract=contract)

    with pytest.raises(DeliveryContractError) as exc:
        dispatcher_module._validate_effect(tampered)

    assert exc.value.code == "classification_conflict"


def test_dispatcher_rejects_project_alias_issue_url_before_http(tmp_path):
    store = _seed(tmp_path)
    claim = store.claim_due_effect(lease_owner="worker-1", now=NOW)
    assert claim is not None
    alias_url = "https://project.feishu.cn/t03o4q/issue/detail/7041712812"
    tampered = replace(
        claim,
        issue_url=alias_url,
        payload={**claim.payload, "issue_url": alias_url},
    )

    with pytest.raises(DeliveryContractError) as exc:
        dispatcher_module._validate_effect(tampered)

    assert exc.value.code == "delivery_issue_url_identity_mismatch"


def test_dispatcher_rejects_project_slug_payload_mismatch_before_http(tmp_path):
    store = _seed(tmp_path)
    claim = store.claim_due_effect(lease_owner="worker-1", now=NOW)
    assert claim is not None
    tampered = replace(
        claim,
        payload={**claim.payload, "project_simple_name": "wrong-slug"},
    )

    with pytest.raises(DeliveryContractError) as exc:
        dispatcher_module._validate_effect(tampered)

    assert exc.value.code == "delivery_issue_url_identity_mismatch"


def test_dispatcher_rejects_noncanonical_report_cifs_path_before_http(tmp_path):
    store = _seed(tmp_path)
    claim = store.claim_due_effect(lease_owner="worker-1", now=NOW)
    assert claim is not None
    tampered = replace(
        claim,
        payload={
            **claim.payload,
            "report_cifs_path": "//hfs.minieye.tech/department-perception_test_team/"
            "G1Q3_RCA/cases/report/index.html",
        },
    )

    with pytest.raises(DeliveryContractError) as exc:
        dispatcher_module._validate_effect(tampered)

    assert exc.value.code == "delivery_report_cifs_identity_mismatch"


def test_foxglove_delivery_verifies_only_primary_html_artifact(
    tmp_path,
):
    store = _seed(tmp_path, bundle_payload=_web_bundle_payload())
    clock = Clock()
    contender_claims = []

    def verifier(url, size, sha256):
        clock.current += timedelta(seconds=80)
        contender_claims.append(
            RcaDeliveryStore(store.db_path).claim_due_effect(
                lease_owner="worker-2",
                lease_seconds=90,
                now=clock.current,
            )
        )
        return _verified_report(url, size, sha256)

    dispatcher, remote, _clock = _dispatcher(
        tmp_path,
        clock=clock,
        verifier=verifier,
        lease_owner="worker-1",
    )

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "succeeded"
    assert contender_claims == [None]
    assert clock.current == NOW + timedelta(seconds=80)
    assert dispatcher.stats.lease_lost == 0
    assert remote.add_calls == 1


def test_effect_lease_keeper_fences_contender_past_original_lease(
    tmp_path, monkeypatch
):
    store = _seed(tmp_path)
    clock = Clock()
    remote = Remote()
    boundary_entered = threading.Event()
    release_boundary = threading.Event()
    renewed_after_clock_advance = threading.Event()
    original_list = remote.list_comments

    def blocking_list(project_key, work_item_id):
        if not boundary_entered.is_set():
            boundary_entered.set()
            assert release_boundary.wait(timeout=2)
        return original_list(project_key, work_item_id)

    remote.list_comments = blocking_list
    dispatcher, _remote, _clock = _dispatcher(
        tmp_path,
        remote=remote,
        clock=clock,
        lease_owner="worker-1",
        lease_renew_interval_seconds=0.01,
    )
    original_extend = dispatcher.store.extend_effect_lease

    def observed_extend(**kwargs):
        result = original_extend(**kwargs)
        if threading.current_thread().name.startswith(
            f"{dispatcher_module.SERVICE_LABEL}-effect-lease-"
        ) and kwargs["now"] >= NOW + timedelta(seconds=80):
            renewed_after_clock_advance.set()
        return result

    monkeypatch.setattr(dispatcher.store, "extend_effect_lease", observed_extend)

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(dispatcher.dispatch_one)
        try:
            assert boundary_entered.wait(timeout=2)
            clock.current = NOW + timedelta(seconds=80)
            assert renewed_after_clock_advance.wait(timeout=2)
            lease_expires_at = datetime.fromisoformat(
                store.list_rows("rca_delivery_effects")[0]["lease_expires_at"]
            )
            assert lease_expires_at >= NOW + timedelta(seconds=170)

            clock.current = NOW + timedelta(seconds=91)
            contender = RcaDeliveryStore(store.db_path).claim_due_effect(
                lease_owner="worker-2",
                lease_seconds=90,
                now=clock.current,
            )
            assert contender is None
        finally:
            release_boundary.set()
        outcome = future.result(timeout=2)

    assert outcome.status == "succeeded"
    assert dispatcher.stats.effect_lease_keeper_renewals >= 1
    assert dispatcher.stats.effect_lease_keeper_failures == 0
    assert dispatcher.stats.effect_lease_keeper_active == 0
    assert remote.add_calls == 1


def test_effect_lease_keeper_failure_after_write_yields_lease_lost(
    tmp_path, monkeypatch
):
    store = _seed(tmp_path)
    write_entered = threading.Event()
    keeper_failed = threading.Event()
    write_started_before_remote = []

    class BlockingWriteRemote(Remote):
        def add_comment(self, project_key, work_item_id, content):
            effect = store.list_rows("rca_delivery_effects")[0]
            write_started_before_remote.append(effect["write_phase"] == "write_started")
            write_entered.set()
            assert keeper_failed.wait(timeout=2)
            return super().add_comment(project_key, work_item_id, content)

    remote = BlockingWriteRemote()
    dispatcher, _remote, _clock = _dispatcher(
        tmp_path,
        remote=remote,
        lease_owner="worker-1",
        lease_renew_interval_seconds=0.01,
    )
    original_extend = dispatcher.store.extend_effect_lease

    def fail_keeper_after_write(**kwargs):
        if write_entered.is_set() and threading.current_thread().name.startswith(
            f"{dispatcher_module.SERVICE_LABEL}-effect-lease-"
        ):
            keeper_failed.set()
            raise StaleDeliveryEffectLeaseError("injected keeper fence loss")
        return original_extend(**kwargs)

    monkeypatch.setattr(
        dispatcher.store,
        "extend_effect_lease",
        fail_keeper_after_write,
    )

    outcome = dispatcher.dispatch_one()

    effect = store.list_rows("rca_delivery_effects")[0]
    attempts = store.list_rows("rca_delivery_attempts")
    assert outcome.status == "lease_lost"
    assert outcome.error_code == "stale_delivery_effect_lease"
    assert write_started_before_remote == [True]
    assert remote.add_calls == 1
    assert effect["status"] == "claimed"
    assert effect["write_phase"] == "write_started"
    assert effect["completed_at"] is None
    assert [row["outcome"] for row in attempts] == ["started"]
    assert dispatcher.stats.delivered == 0
    assert dispatcher.stats.effect_lease_keeper_failures == 1
    assert dispatcher.stats.effect_lease_keeper_started == 1
    assert dispatcher.stats.effect_lease_keeper_stopped == 1
    assert dispatcher.stats.effect_lease_keeper_active == 0


def test_effect_lease_keeper_normal_path_joins_thread(tmp_path):
    _seed(tmp_path)
    prefix = f"{dispatcher_module.SERVICE_LABEL}-effect-lease-"
    existing = {
        thread.ident
        for thread in threading.enumerate()
        if thread.name.startswith(prefix)
    }
    dispatcher, remote, _clock = _dispatcher(
        tmp_path,
        lease_renew_interval_seconds=0.01,
    )

    outcome = dispatcher.dispatch_one()

    remaining = {
        thread.ident
        for thread in threading.enumerate()
        if thread.name.startswith(prefix)
    }
    assert outcome.status == "succeeded"
    assert remaining == existing
    assert dispatcher._active_effect_lease_keeper is None
    assert dispatcher.stats.effect_lease_keeper_started == 1
    assert dispatcher.stats.effect_lease_keeper_stopped == 1
    assert dispatcher.stats.effect_lease_keeper_failures == 0
    assert dispatcher.stats.effect_lease_keeper_active == 0
    assert remote.add_calls == 1


@pytest.mark.parametrize(
    "changed_asset",
    ["assets/app.css", "assets/app.js", "assets/media/video.mp4"],
)
def test_changed_remote_html_assets_do_not_block_primary_report_delivery(
    tmp_path, changed_asset
):
    _seed(tmp_path, bundle_payload=_web_bundle_payload())
    calls = []

    def verifier(url, size, sha256):
        calls.append(url)
        return _verified_report(url, size, sha256)

    dispatcher, remote, _clock = _dispatcher(tmp_path, verifier=verifier)

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "succeeded"
    assert len(calls) == 1
    assert "?ds=foxglove-http&ds.mcapPath=" in calls[0]
    assert changed_asset not in calls[0]
    assert remote.add_calls == 1


def test_viz_publication_verifier_is_inside_fenced_write_boundary(tmp_path):
    store = _seed(tmp_path)
    remote = Remote()
    calls = []

    def verifier(url, size, sha256):
        calls.append((url, size, sha256))
        return _verified_report(url, size, sha256)

    dispatcher, _remote, _clock = _dispatcher(
        tmp_path,
        remote=remote,
        verifier=verifier,
        lease_owner="worker-1",
    )

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "succeeded"
    assert len(calls) == 1
    assert "?ds=foxglove-http&ds.mcapPath=" in calls[0][0]
    assert remote.add_calls == 1
    assert store.list_rows("rca_delivery_effects")[0]["status"] == "succeeded"
    assert store.delivery_dispatcher_circuit().is_open is False


def test_report_data_http_hash_mismatch_is_irrelevant_to_foxglove_link(tmp_path):
    _seed(tmp_path)

    def verifier(url, size, sha256):
        result = _verified_report(url, size, sha256)
        if url.endswith("/report_data.json"):
            result["sha256"] = "0" * 64
        return result

    dispatcher, remote, _clock = _dispatcher(tmp_path, verifier=verifier)
    outcome = dispatcher.dispatch_one()

    assert outcome.status == "succeeded"
    assert remote.add_calls == 1


def test_concurrent_effect_claim_has_exactly_one_winner(tmp_path):
    store = _seed(tmp_path)

    def claim(index):
        return RcaDeliveryStore(store.db_path).claim_due_effect(
            lease_owner=f"worker-{index}", now=NOW
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        claims = list(pool.map(claim, range(8)))
    assert sum(claim is not None for claim in claims) == 1
    assert [row["outcome"] for row in store.list_rows("rca_delivery_attempts")] == [
        "started"
    ]


def test_expired_prewrite_effect_lease_fences_old_worker_and_retries(tmp_path):
    store = _seed(tmp_path)
    first = store.claim_due_effect(lease_owner="worker-1", lease_seconds=30, now=NOW)
    assert first is not None
    second = store.claim_due_effect(
        lease_owner="worker-2", lease_seconds=30, now=NOW + timedelta(seconds=31)
    )
    assert second is not None
    assert second.fence == first.fence + 1
    with pytest.raises(StaleDeliveryEffectLeaseError):
        store.complete_effect(
            claim=first,
            outcome="ack",
            remote_id="comment-old",
            receipt={"remote_id": "comment-old"},
            now=NOW + timedelta(seconds=31),
        )
    assert [row["outcome"] for row in store.list_rows("rca_delivery_attempts")] == [
        "started",
        "nack",
        "started",
    ]
    assert second.previous_status == "retry_wait"
    assert second.write_phase == "prewrite"


def test_process_kill_after_remote_add_reconciles_without_second_send(tmp_path):
    store = _seed(tmp_path)
    remote = Remote()
    first = store.claim_due_effect(
        lease_owner="killed-worker", lease_seconds=60, now=NOW
    )
    assert first is not None
    store.mark_effect_write_started(claim=first, now=NOW)
    response = remote.add_comment(
        first.project_key,
        first.work_item_id,
        str(first.payload["comment_content"]),
    )
    assert response["success"] is True

    clock = Clock()
    clock.current = NOW + timedelta(seconds=61)
    dispatcher, _remote, _clock = _dispatcher(tmp_path, remote=remote, clock=clock)
    outcome = dispatcher.dispatch_one()

    assert outcome.status == "reconciled"
    assert remote.add_calls == 1
    assert [row["outcome"] for row in store.list_rows("rca_delivery_attempts")] == [
        "started",
        "unknown",
        "started",
        "reconciled",
    ]


def test_weak_add_success_without_remote_id_is_uncertain_then_reconciled(tmp_path):
    store = _seed(tmp_path)
    remote = Remote()
    remote.weak_success = True
    clock = Clock()
    dispatcher, _remote, _clock = _dispatcher(tmp_path, remote=remote, clock=clock)

    first = dispatcher.dispatch_one()
    assert first.status == "uncertain"
    assert first.error_code == "feishu_add_remote_id_missing"
    assert remote.add_calls == 1

    clock.current += timedelta(seconds=2)
    second = dispatcher.dispatch_one()
    assert second.status == "reconciled"
    assert remote.add_calls == 1
    assert store.list_rows("rca_delivery_jobs")[0]["status"] == "delivered"


def test_add_remote_id_without_read_after_marker_stays_uncertain(tmp_path):
    _seed(tmp_path)
    remote = Remote()
    remote.add_failure = {"success": True, "remote_id": "comment-not-visible"}
    dispatcher, _remote, _clock = _dispatcher(tmp_path, remote=remote)

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "uncertain"
    assert outcome.error_code == "feishu_postwrite_confirmation_mismatch"
    assert remote.add_calls == 1

    _dispatcher_instance, _remote, clock = _dispatcher(tmp_path, remote=remote)
    clock.current += timedelta(seconds=2)
    second = _dispatcher_instance.dispatch_one()

    assert second.status == "uncertain"
    assert second.error_code == "delivery_uncertain_reconciliation_pending"
    assert remote.add_calls == 1


def test_invisible_success_is_bounded_to_two_recovery_writes_then_quarantined(
    tmp_path,
):
    store = _seed_terminal(tmp_path)
    remote = Remote()
    remote.add_failure = {"success": True, "remote_id": "comment-invisible"}
    dispatcher, _remote, clock = _dispatcher(tmp_path, remote=remote)

    outcomes = []
    for _ in range(30):
        outcome = dispatcher.dispatch_one()
        outcomes.append(outcome)
        if outcome.status == "quarantined":
            break
        assert outcome.status == "uncertain"
        assert outcome.next_attempt_at is not None
        clock.current = datetime.fromisoformat(outcome.next_attempt_at)

    assert outcomes[-1].status == "quarantined"
    assert outcomes[-1].error_code == "delivery_recovery_write_limit_exceeded"
    assert remote.add_calls == 3
    effect = store.list_rows("rca_delivery_effects")[0]
    assert effect["status"] == "quarantined"
    assert effect["write_phase"] == "settled"
    assert effect["recovery_write_count"] == 2
    assert effect["last_error_code"] == "delivery_recovery_write_limit_exceeded"
    assert store.list_rows("rca_delivery_jobs")[0]["status"] == "quarantined"
    circuit = store.delivery_dispatcher_circuit("feishu_issue_comment")
    assert circuit.is_open is True
    assert circuit.reason_code == "delivery_recovery_write_limit_exceeded"


def test_corrupt_marker_quarantines_without_any_boundary_call(tmp_path):
    store = _seed(tmp_path)
    effect = store.list_rows("rca_delivery_effects")[0]
    payload = json.loads(effect["payload_json"])
    payload["marker"] = "[RCA_DELIVERY:wrong:wrong]"
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            "UPDATE rca_delivery_effects SET payload_json = ? WHERE effect_key = ?",
            (json.dumps(payload), effect["effect_key"]),
        )
    dispatcher, remote, _clock = _dispatcher(tmp_path)

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "quarantined"
    assert outcome.error_code == "delivery_effect_marker_invalid"
    assert remote.list_calls == remote.add_calls == 0
    assert store.list_rows("rca_delivery_jobs")[0]["status"] == "quarantined"


def test_derived_artifact_corruption_cannot_override_sealed_manifest(tmp_path):
    store = _seed(tmp_path)
    job = store.list_rows("rca_delivery_jobs")[0]
    artifacts = json.loads(job["artifacts_json"])
    artifacts[0]["sha256"] = "f" * 64
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            "UPDATE rca_delivery_jobs SET artifacts_json = ? WHERE delivery_id = ?",
            (json.dumps(artifacts), job["delivery_id"]),
        )
    dispatcher, remote, _clock = _dispatcher(tmp_path)

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "quarantined"
    assert outcome.error_code == "delivery_index_html_store_mismatch"
    assert remote.list_calls == remote.add_calls == 0


@pytest.mark.parametrize(
    ("corruption", "expected_code"),
    [
        ("missing", "delivery_artifact_inventory_mismatch"),
        ("extra", "delivery_artifact_inventory_mismatch"),
        ("duplicate", "delivery_artifact_inventory_duplicate"),
        ("path_escape", "artifact_path_invalid"),
        ("mcap", "html_delivery_mcap_forbidden"),
    ],
)
def test_stored_artifact_inventory_corruption_quarantines_before_boundaries(
    tmp_path, corruption, expected_code
):
    store = _seed(tmp_path)
    job = store.list_rows("rca_delivery_jobs")[0]
    artifacts = json.loads(job["artifacts_json"])
    if corruption == "missing":
        artifacts.pop()
    elif corruption == "extra":
        extra = dict(artifacts[-1])
        root = artifacts[0]["path"][: -len("index.html")]
        extra.update({
            "role": "unexpected_stylesheet",
            "path": root + "assets/extra.css",
            "relative_path": "assets/extra.css",
            "media_type": "text/css",
        })
        artifacts.append(extra)
    elif corruption == "duplicate":
        artifacts.append(dict(artifacts[-1]))
    elif corruption == "path_escape":
        artifacts[-1]["relative_path"] = "../video.mp4"
    else:
        artifacts[-1]["media_type"] = "application/x-mcap"
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            "UPDATE rca_delivery_jobs SET artifacts_json = ? WHERE delivery_id = ?",
            (json.dumps(artifacts), job["delivery_id"]),
        )
    dispatcher, remote, _clock = _dispatcher(tmp_path)

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "quarantined"
    assert outcome.error_code == expected_code
    assert remote.list_calls == remote.add_calls == 0


def test_viz_publication_probe_failure_blocks_external_comment(tmp_path):
    store = _seed(tmp_path)
    assert store.list_rows("rca_delivery_jobs")[0]["report_url"].startswith(
        "https://192.168.21.217/?ds=foxglove-http&ds.mcapPath="
    )
    dispatcher, remote, _clock = _dispatcher(
        tmp_path,
        verifier=lambda *_args: {
            "success": False,
            "error_code": "report_http_unavailable",
        },
    )
    outcome = dispatcher.dispatch_one()
    assert outcome.status == "retry_wait"
    assert outcome.error_code == "report_http_unavailable"
    assert remote.add_calls == 0
    assert store.list_rows("rca_delivery_jobs")[0]["status"] == "ready"


def test_primary_report_readback_hash_mismatch_quarantines_before_comment(tmp_path):
    store = _seed(tmp_path)
    dispatcher, remote, _clock = _dispatcher(
        tmp_path,
        verifier=lambda *_args: {
            "success": False,
            "permanent": True,
            "error_code": "report_http_hash_mismatch",
        },
    )
    outcome = dispatcher.dispatch_one()
    assert outcome.status == "quarantined"
    assert outcome.error_code == "report_http_hash_mismatch"
    assert remote.add_calls == 0
    assert store.list_rows("rca_delivery_jobs")[0]["status"] == "quarantined"


def test_partial_status_is_reserved_for_future_optional_effects(tmp_path):
    store = _seed(tmp_path)
    job = store.list_rows("rca_delivery_jobs")[0]
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            """
            INSERT INTO rca_delivery_effects(
                effect_key, delivery_id, effect_kind, required, target_key,
                payload_json, payload_sha256, status, created_at, updated_at
            ) VALUES (?, ?, 'feishu_field_update', 0, ?, '{}', ?, 'suppressed', ?, ?)
            """,
            (
                "future-optional-effect",
                job["delivery_id"],
                "future-target",
                "0" * 64,
                NOW.isoformat(),
                NOW.isoformat(),
            ),
        )
    dispatcher, _remote, _clock = _dispatcher(tmp_path)
    assert dispatcher.dispatch_one().status == "succeeded"
    assert store.list_rows("rca_delivery_jobs")[0]["status"] == "partial"


def test_429_honors_retry_after_and_backoff_schedule(tmp_path):
    store = _seed(tmp_path)
    remote = Remote()
    remote.list_failure = {
        "success": False,
        "error_code": "feishu_rate_limited",
        "retry_after_seconds": 17,
    }
    dispatcher, _remote, _clock = _dispatcher(tmp_path, remote=remote)
    outcome = dispatcher.dispatch_one()
    assert outcome.status == "retry_wait"
    assert outcome.next_attempt_at == (NOW + timedelta(seconds=17)).isoformat()
    assert [retry_delay_seconds(i) for i in range(1, 10)] == [
        2,
        5,
        10,
        20,
        40,
        120,
        300,
        900,
        3600,
    ]
    assert store.list_rows("rca_delivery_attempts")[-1]["outcome"] == "nack"


def test_explicit_add_rate_limit_retries_then_creates_one_remote_comment(tmp_path):
    _seed(tmp_path)
    remote = Remote()
    remote.add_failure = {
        "success": False,
        "outcome_uncertain": False,
        "error_code": "feishu_rate_limited",
        "retry_after_seconds": 17,
    }
    clock = Clock()
    dispatcher, _remote, _clock = _dispatcher(tmp_path, remote=remote, clock=clock)

    first = dispatcher.dispatch_one()
    assert first.status == "retry_wait"
    assert first.next_attempt_at == (NOW + timedelta(seconds=17)).isoformat()

    remote.add_failure = None
    clock.current += timedelta(seconds=17)
    second = dispatcher.dispatch_one()

    assert second.status == "succeeded"
    assert remote.add_calls == 2
    assert len(remote.comments) == 1


def test_auth_error_opens_persisted_circuit_and_resident_recovers(
    tmp_path, monkeypatch
):
    store = _seed(tmp_path)
    remote = Remote()
    remote.list_failure = {
        "success": False,
        "error_code": "feishu_auth_failed",
        "error": "token expired",
    }
    clock = Clock()
    dispatcher, _remote, _clock = _dispatcher(tmp_path, remote=remote, clock=clock)
    monkeypatch.setattr(
        dispatcher.store,
        "open_delivery_dispatcher_circuit",
        lambda **_kwargs: pytest.fail("dispatcher must use the atomic store API"),
    )
    sleeps = []
    stopped = {"value": False}

    def sleep(seconds):
        sleeps.append(seconds)
        if len(sleeps) == 1:
            assert store.delivery_dispatcher_circuit().is_open is True
            store.close_delivery_dispatcher_circuit(now=clock.current)
            remote.list_failure = None
            clock.current += timedelta(seconds=2)
        else:
            stopped["value"] = True

    assert (
        run_dispatch_loop(
            dispatcher,
            stop_requested=lambda: stopped["value"],
            sleep=sleep,
        )
        == 0
    )
    assert sleeps == [30, 2]
    assert remote.add_calls == 1
    assert store.list_rows("rca_delivery_jobs")[0]["status"] == "delivered"


def test_effect_older_than_24_hours_is_quarantined_before_boundary_calls(tmp_path):
    store = _seed(tmp_path)
    clock = Clock()
    clock.current = NOW + timedelta(seconds=86_401)
    dispatcher, remote, _clock = _dispatcher(tmp_path, clock=clock)
    outcome = dispatcher.dispatch_one()
    assert outcome.status == "idle"
    assert remote.list_calls == remote.add_calls == 0
    assert store.list_rows("rca_delivery_effects")[0]["status"] == "quarantined"
    assert store.list_rows("rca_delivery_jobs")[0]["status"] == "quarantined"
    assert store.list_rows("rca_delivery_attempts")[-1]["outcome"] == "quarantined"


def test_disabled_dispatcher_never_reads_or_writes_remote(tmp_path):
    _seed(tmp_path)
    dispatcher, remote, _clock = _dispatcher(tmp_path, enabled=False)
    assert dispatcher.dispatch_one().status == "disabled"
    assert run_dispatch_loop(dispatcher, once=True) == 0
    assert remote.list_calls == remote.add_calls == 0
    healthy, payload = read_health(dispatcher.config.health_path, max_age_seconds=60)
    assert healthy is True
    assert payload["state"] == "disabled"
    assert payload["schema_version"] == "pnc_rca_delivery_dispatcher_health_v2"
    assert payload["runtime_identity"]["service_label"] == (
        "local.pnc.rca-delivery-dispatcher"
    )
    assert len(payload["runtime_identity"]["script_sha256"]) == 64
    assert len(payload["runtime_identity"]["runtime_files_sha256"]) == 64
    assert len(payload["runtime_identity"]["public_config_sha256"]) == 64
    assert len(payload["runtime_identity"]["loaded_runtime_sha256"]) == 64


def test_dispatcher_writes_periodic_health_during_long_batch(tmp_path, monkeypatch):
    _seed(tmp_path)
    dispatcher, _remote, _clock = _dispatcher(tmp_path)
    writes = []
    original_write = dispatcher_module.HealthReporter.write

    def observed_write(self, **kwargs):
        writes.append(kwargs["state"])
        return original_write(self, **kwargs)

    def slow_batch():
        time.sleep(0.05)
        return [dispatcher_module.DispatchOutcome(status="idle")]

    monkeypatch.setattr(dispatcher_module.HealthReporter, "write", observed_write)
    monkeypatch.setattr(
        dispatcher_module,
        "_heartbeat_interval_seconds",
        lambda _max_age: 0.01,
    )
    monkeypatch.setattr(dispatcher, "dispatch_batch", slow_batch)

    assert run_dispatch_loop(dispatcher, once=True) == 0
    assert writes.count("running") >= 2


def test_health_exposes_effect_lease_keeper_contract_and_stats(tmp_path):
    _seed(tmp_path)
    dispatcher, _remote, _clock = _dispatcher(tmp_path)

    assert run_dispatch_loop(dispatcher, once=True) == 0
    healthy, payload = read_health(
        dispatcher.config.health_path,
        max_age_seconds=60,
    )

    assert healthy is True
    assert payload["config"]["effect_lease_keeper_enabled"] is True
    assert payload["config"]["effect_lease_renew_interval_seconds"] == 10
    assert payload["effect_lease_keeper"] == {
        "enabled": True,
        "renew_interval_seconds": 10,
        "active": False,
        "started": 1,
        "stopped": 1,
        "renewals": 0,
        "failures": 0,
    }
    assert payload["stats"]["effect_lease_keeper_started"] == 1
    assert payload["stats"]["effect_lease_keeper_stopped"] == 1
    assert payload["stats"]["effect_lease_keeper_active"] == 0


def test_health_rejects_identity_without_loaded_runtime_digest(tmp_path):
    _seed(tmp_path)
    dispatcher, _remote, _clock = _dispatcher(tmp_path, enabled=False)
    assert run_dispatch_loop(dispatcher, once=True) == 0
    path = dispatcher.config.health_path
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["runtime_identity"].pop("loaded_runtime_sha256")
    path.write_text(json.dumps(payload), encoding="utf-8")

    healthy, result = read_health(path, max_age_seconds=60)

    assert healthy is False
    assert result["error"] == "health_runtime_identity_invalid"


@pytest.mark.parametrize(
    ("future_seconds", "expected_healthy", "expected_error"),
    [
        (30, True, None),
        (31, False, "heartbeat_from_future"),
    ],
)
def test_health_bounds_future_heartbeat_clock_skew(
    tmp_path, future_seconds, expected_healthy, expected_error
):
    _seed(tmp_path)
    dispatcher, _remote, _clock = _dispatcher(tmp_path, enabled=False)
    assert run_dispatch_loop(dispatcher, once=True) == 0
    path = dispatcher.config.health_path
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["updated_at"] = (NOW + timedelta(seconds=future_seconds)).isoformat()
    path.write_text(json.dumps(payload), encoding="utf-8")

    healthy, result = read_health(path, max_age_seconds=60, now=NOW)

    assert healthy is expected_healthy
    assert result["age_seconds"] == -future_seconds
    assert result.get("error") == expected_error


def test_health_rejects_timezone_naive_heartbeat(tmp_path):
    _seed(tmp_path)
    dispatcher, _remote, _clock = _dispatcher(tmp_path, enabled=False)
    assert run_dispatch_loop(dispatcher, once=True) == 0
    path = dispatcher.config.health_path
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["updated_at"] = "2026-07-10T00:00:00"
    path.write_text(json.dumps(payload), encoding="utf-8")

    healthy, result = read_health(path, max_age_seconds=60, now=NOW)

    assert healthy is False
    assert result["error"] == "health_timestamp_invalid"


def test_lease_loss_is_counted_and_marks_health_not_ready(tmp_path, monkeypatch):
    store = _seed(tmp_path)
    dispatcher, remote, _clock = _dispatcher(tmp_path, lease_owner="worker-1")

    def lose_lease(**_kwargs):
        raise StaleDeliveryEffectLeaseError("simulated lease loss")

    monkeypatch.setattr(dispatcher.store, "extend_effect_lease", lose_lease)

    assert run_dispatch_loop(dispatcher, once=True) == 0
    healthy, payload = read_health(dispatcher.config.health_path, max_age_seconds=60)

    assert healthy is False
    assert payload["state"] == "lease_lost"
    assert payload["last_outcome"]["error_code"] == "stale_delivery_effect_lease"
    assert payload["stats"]["lease_lost"] == 1
    assert payload["stats"]["lease_extensions"] == 0
    assert payload["stats"]["effect_lease_keeper_failures"] == 1
    assert payload["stats"]["effect_lease_keeper_started"] == 1
    assert payload["stats"]["effect_lease_keeper_stopped"] == 1
    assert payload["effect_lease_keeper"]["active"] is False
    assert remote.list_calls == remote.add_calls == 0
    assert store.list_rows("rca_delivery_effects")[0]["status"] == "claimed"


def test_meegle_adapter_exposes_only_fixed_list_and_add_commands():
    calls = []

    def runner(args):
        calls.append(args)
        if args[:2] == ["comment", "list"]:
            return (
                0,
                json.dumps({
                    "comments": [{"comment_id": "c-1", "content": "hello"}],
                    "has_more": False,
                }),
                "",
            )
        if args[:2] == ["comment", "add"]:
            return 0, json.dumps({"comment_id": "c-2"}), ""
        raise AssertionError("adapter must not expose other Meegle operations")

    adapter = MeegleIssueCommentAdapter(runner)
    assert (
        adapter.list_comments("t03o4q", "7041712812")["comments"][0]["remote_id"]
        == "c-1"
    )
    with dispatcher_module._bound_provider_write_guard(_TEST_PROVIDER_WRITE_CLAIM):
        assert (
            adapter.add_comment("t03o4q", "7041712812", "content")["remote_id"] == "c-2"
        )
    assert [call[:2] for call in calls] == [
        ["comment", "list"],
        ["comment", "add"],
    ]
    assert all("--project-key" in call and "--work-item-id" in call for call in calls)
    assert calls[0][calls[0].index("--page-num") + 1] == "1"
    assert "--content" not in calls[0]
    assert "--content" in calls[1]


def test_meegle_provider_guard_missing_or_revoked_blocks_before_runner(
    monkeypatch,
):
    calls = []
    adapter = MeegleIssueCommentAdapter(
        lambda args: (
            calls.append(args) or (0, json.dumps({"comment_id": "unexpected"}), "")
        )
    )

    with pytest.raises(
        dispatcher_module.ExternalWriteFenceError,
        match="external_write_provider_claim_missing",
    ):
        adapter.add_comment("t03o4q", "7041712812", "must not send")

    def revoked_revalidate(*_args, **_kwargs):
        raise dispatcher_module.ExternalWriteFenceError(
            "external_write_fence_epoch_not_current"
        )

    monkeypatch.setattr(
        dispatcher_module,
        "revalidate_provider_write_claim",
        revoked_revalidate,
    )
    with dispatcher_module._bound_provider_write_guard(_TEST_PROVIDER_WRITE_CLAIM):
        with pytest.raises(
            dispatcher_module.ExternalWriteFenceError,
            match="external_write_fence_epoch_not_current",
        ):
            adapter.update_fields(
                "t03o4q",
                "7041712812",
                (("field_9193cb", "must not write"),),
            )

    assert calls == []


def _terminal_rerun_claim() -> SimpleNamespace:
    return SimpleNamespace(
        contract={},
        effect_kind=DELIVERY_EFFECT_KIND,
        generation=2,
        effect_key="historical-authority-effect",
        delivery_id="historical-authority-delivery",
        lease_token="historical-authority-lease",
        fence=3,
        issue_url="https://project.feishu.cn/t03o4q/issue/detail/7055722720",
        target_key="feishu_project:project-key:problem-type:7055722720",
        business_key="historical-authority-business",
        submission_key="historical-authority-submission",
    )


def test_historical_rerun_authority_builds_terminal_provider_claim():
    claim = _terminal_rerun_claim()
    live = {
        "authority_sha256": "a" * 64,
        "outbox_id": 17,
        "epoch_id": "epoch-current",
        "activation_ledger_id": 23,
        "effect_key": claim.effect_key,
        "delivery_id": claim.delivery_id,
        "lease_token": claim.lease_token,
        "lease_fence": claim.fence,
        "operation": DELIVERY_EFFECT_KIND,
        "issue_url": claim.issue_url,
        "target_key": claim.target_key,
        "business_key": claim.business_key,
        "submission_key": claim.submission_key,
        "generation": claim.generation,
        "project_key": "project-key",
        "project_simple_name": "t03o4q",
        "work_item_type_key": "problem-type",
        "work_item_id": "7055722720",
    }
    store = SimpleNamespace(
        validate_terminal_rerun_external_write_binding=lambda **_kwargs: live
    )
    dispatcher = object.__new__(DeliveryDispatcher)
    dispatcher.store = store
    dispatcher.now = lambda: NOW

    provider_claim = dispatcher._provider_write_guard(claim)

    payload = provider_claim.payload()
    assert payload["authority_kind"] == "terminal_rerun"
    assert payload["authority"]["authority_sha256"] == "a" * 64
    assert payload["authority"]["work_item_id"] == "7055722720"


def test_historical_rerun_authority_identity_mismatch_never_falls_back(
    tmp_path,
):
    db_path = tmp_path / "historical-authority.sqlite3"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE rca_terminal_rerun_delivery_authorities (
                business_key TEXT NOT NULL,
                generation INTEGER NOT NULL
            );
            CREATE TABLE rca_historical_epoch_rerun_delivery_authorities (
                business_key TEXT NOT NULL,
                generation INTEGER NOT NULL
            );
            INSERT INTO rca_historical_epoch_rerun_delivery_authorities
                (business_key, generation)
            VALUES ('historical-authority-business', 2);
            """
        )

    class Store:
        @staticmethod
        def validate_terminal_rerun_external_write_binding(**_kwargs):
            raise RuntimeError("external_write_fence_identity_mismatch")

        @staticmethod
        def _connect():
            return sqlite3.connect(db_path)

        @staticmethod
        def _table_exists(conn, table):
            return (
                conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                    (table,),
                ).fetchone()
                is not None
            )

    dispatcher = object.__new__(DeliveryDispatcher)
    dispatcher.store = Store()
    dispatcher.now = lambda: NOW

    with pytest.raises(
        dispatcher_module.ExternalWriteFenceError,
        match="external_write_fence_identity_mismatch",
    ):
        dispatcher._terminal_rerun_external_write_binding(
            _terminal_rerun_claim(),
            operation="feishu_issue_comment",
            require_write_started=True,
        )


def test_meegle_provider_rejects_forged_callable_context_before_runner():
    calls = []
    adapter = MeegleIssueCommentAdapter(
        lambda args: (
            calls.append(args) or (0, json.dumps({"comment_id": "unexpected"}), "")
        )
    )

    with pytest.raises(
        dispatcher_module.ExternalWriteFenceError,
        match="external_write_provider_claim_invalid",
    ):
        with dispatcher_module._bound_provider_write_guard(
            lambda *_args, **_kwargs: {
                "epoch_id": "forged",
                "state": "steady_active",
            }
        ):
            adapter.add_comment("t03o4q", "7041712812", "must not send")

    assert calls == []


def test_meegle_adapter_reads_every_page_until_explicit_completion():
    calls = []
    pages = {
        1: {
            "comments": [{"comment_id": "c-1", "content": "first"}],
            "has_more": True,
        },
        2: {
            "comments": [{"comment_id": "c-2", "content": "second"}],
            "has_more": False,
        },
    }

    def runner(args):
        page_num = int(args[args.index("--page-num") + 1])
        calls.append(page_num)
        return 0, json.dumps(pages[page_num]), ""

    result = MeegleIssueCommentAdapter(runner).list_comments("t03o4q", "7041712812")

    assert result["success"] is True
    assert result["pages_read"] == 2
    assert [item["remote_id"] for item in result["comments"]] == ["c-1", "c-2"]
    assert calls == [1, 2]


def test_meegle_adapter_uses_real_cli_pagination_contract_without_empty_probe():
    calls = []

    def runner(args):
        page_num = int(args[args.index("--page-num") + 1])
        calls.append(page_num)
        return (
            0,
            json.dumps({
                "comments": [
                    {"comment_id": "c-1", "content": "first"},
                    {"comment_id": "c-2", "content": "second"},
                ],
                "pagination": {
                    "page_num": 1,
                    "page_size": 20,
                    "total": 2,
                    "total_pages": 1,
                },
            }),
            "",
        )

    result = MeegleIssueCommentAdapter(runner).list_comments("t03o4q", "7041712812")

    assert result["success"] is True
    assert result["pages_read"] == 1
    assert calls == [1]


def test_meegle_adapter_rejects_incoherent_real_cli_pagination_contract():
    result = MeegleIssueCommentAdapter(
        lambda _args: (
            0,
            json.dumps({
                "comments": [{"comment_id": "c-1", "content": "row"}],
                "pagination": {
                    "page_num": 2,
                    "page_size": 20,
                    "total": 1,
                    "total_pages": 1,
                },
            }),
            "",
        )
    ).list_comments("t03o4q", "7041712812")

    assert result["success"] is False
    assert result["error_code"] == "meegle_response_invalid"


def test_meegle_adapter_fails_closed_at_page_and_comment_limits():
    def endless_runner(args):
        page_num = int(args[args.index("--page-num") + 1])
        return (
            0,
            json.dumps({
                "comments": [{"comment_id": f"c-{page_num}", "content": "row"}]
            }),
            "",
        )

    page_limited = MeegleIssueCommentAdapter(endless_runner).list_comments(
        "t03o4q", "7041712812"
    )
    assert page_limited["success"] is False
    assert page_limited["permanent"] is True
    assert page_limited["error_code"] == "meegle_comment_pagination_incomplete"

    too_many = [
        {"comment_id": f"c-{index}", "content": "row"}
        for index in range(MAX_MEEGLE_COMMENTS + 1)
    ]
    comment_limited = MeegleIssueCommentAdapter(
        lambda _args: (
            0,
            json.dumps({"comments": too_many, "has_more": False}),
            "",
        )
    ).list_comments("t03o4q", "7041712812")
    assert comment_limited["success"] is False
    assert comment_limited["permanent"] is True
    assert comment_limited["error_code"] == "meegle_comment_limit_exceeded"
    assert MAX_MEEGLE_COMMENT_PAGES == 5
    assert MAX_EXTERNAL_BOUNDARY_TIMEOUT_SECONDS == 72


def test_meegle_adapter_rejects_repeated_or_incoherent_pages():
    def repeated_runner(args):
        page_num = int(args[args.index("--page-num") + 1])
        return (
            0,
            json.dumps({
                "comments": [{"comment_id": "same-id", "content": "row"}],
                "has_more": page_num == 1,
            }),
            "",
        )

    repeated = MeegleIssueCommentAdapter(repeated_runner).list_comments(
        "t03o4q", "7041712812"
    )
    assert repeated["success"] is False
    assert repeated["error_code"] == "meegle_response_invalid"

    incoherent = MeegleIssueCommentAdapter(
        lambda _args: (
            0,
            json.dumps({"comments": [], "has_more": True}),
            "",
        )
    ).list_comments("t03o4q", "7041712812")
    assert incoherent["success"] is False
    assert incoherent["error_code"] == "meegle_response_invalid"


@pytest.mark.parametrize(
    ("copies", "expected_status", "expected_error"),
    [
        (1, "reconciled", ""),
        (2, "quarantined", "delivery_remote_marker_duplicate"),
    ],
)
def test_later_page_marker_reconciles_and_cross_page_duplicate_conflicts(
    tmp_path, copies, expected_status, expected_error
):
    store = _seed(tmp_path)
    effect = store.list_rows("rca_delivery_effects")[0]
    effect_payload = json.loads(effect["payload_json"])
    marker = effect_payload["marker"]
    expected_fields = {
        item["field_key"]: item["field_value"]
        for item in effect_payload["field_updates"]
    }
    calls = []

    def runner(args):
        assert args[:2] == ["comment", "list"]
        page_num = int(args[args.index("--page-num") + 1])
        calls.append(page_num)
        if page_num == 1:
            content = marker if copies == 2 else "unrelated"
            payload = {
                "comments": [{"comment_id": "c-page-1", "content": content}],
                "has_more": True,
            }
        else:
            payload = {
                "comments": [
                    {
                        "comment_id": "c-page-2",
                        "content": effect_payload["comment_content"],
                    }
                ],
                "has_more": False,
            }
        return 0, json.dumps(payload), ""

    adapter = MeegleIssueCommentAdapter(runner)
    dispatcher = DeliveryDispatcher(
        store=store,
        config=_config(tmp_path),
        list_comments=adapter.list_comments,
        add_comment=lambda *_args: pytest.fail("existing marker must suppress add"),
        get_fields=lambda *_args: {"success": True, "fields": expected_fields},
        update_fields=lambda *_args: pytest.fail(
            "matching fields must suppress update"
        ),
        report_verifier=_verified_report,
        now=Clock(),
        lease_owner="delivery-dispatcher-test",
    )

    outcome = dispatcher.dispatch_one()

    assert outcome.status == expected_status
    assert outcome.error_code == expected_error
    assert calls == [1, 2]


def test_meegle_adapter_treats_weak_success_as_uncertain():
    adapter = MeegleIssueCommentAdapter(
        lambda _args: (0, json.dumps({"success": True}), "")
    )
    with dispatcher_module._bound_provider_write_guard(_TEST_PROVIDER_WRITE_CLAIM):
        result = adapter.add_comment("t03o4q", "7041712812", "content")
    assert result["success"] is False
    assert result["outcome_uncertain"] is True
    assert result["error_code"] == "feishu_add_remote_id_missing"


def test_meegle_adapter_confirms_empty_success_response_by_comment_readback():
    marker = "[RCA_TERMINAL:effect-key:quarantined:1]"
    issue_url = "https://project.feishu.cn/t03o4q/issue/detail/7041712812"
    content = f"本单未能定向\n{marker}\n重新分析 {issue_url}"
    rendered = (
        "本单未能定向\n\n"
        "[RCA_TERMINAL:effect-key :quarantined: 1]\n\n"
        f"重新分析 [{issue_url}]({issue_url})\n"
    )
    calls = []

    def runner(args):
        calls.append(args[:2])
        if args[:2] == ["comment", "add"]:
            return 0, "", ""
        if args[:2] == ["comment", "list"]:
            return (
                0,
                json.dumps({
                    "comments": [
                        {"comment_id": "7671912312650894535", "content": rendered}
                    ],
                    "pagination": {
                        "page_num": 1,
                        "page_size": 20,
                        "total": 1,
                        "total_pages": 1,
                    },
                }),
                "",
            )
        raise AssertionError(args)

    adapter = MeegleIssueCommentAdapter(runner)
    with dispatcher_module._bound_provider_write_guard(_TEST_PROVIDER_WRITE_CLAIM):
        result = adapter.add_comment("t03o4q", "7041712812", content)

    assert result == {
        "success": True,
        "remote_id": "7671912312650894535",
        "confirmed_by": "comment_list_readback",
    }
    assert calls == [["comment", "add"], ["comment", "list"]]


def test_meegle_adapter_reads_and_updates_only_attribution_fields():
    calls = []

    def runner(args):
        calls.append(args)
        if args[:2] == ["workitem", "get"]:
            return (
                0,
                json.dumps({
                    "work_item_fields": [
                        {
                            "key": "field_9193cb",
                            "value": "candidate conclusion",
                        },
                        {
                            "key": "field_8c912e",
                            "value": {"link": "http://report.example/index.html"},
                        },
                    ]
                }),
                "",
            )
        if args[:2] == ["workitem", "update"]:
            return 0, json.dumps({"updated": True}), ""
        raise AssertionError(args)

    adapter = MeegleIssueCommentAdapter(runner)
    fields = adapter.get_fields(
        "t03o4q",
        "7041712812",
        ("field_9193cb", "field_8c912e"),
    )
    with dispatcher_module._bound_provider_write_guard(_TEST_PROVIDER_WRITE_CLAIM):
        update = adapter.update_fields(
            "t03o4q",
            "7041712812",
            (
                ("field_9193cb", "candidate conclusion"),
                ("field_8c912e", "http://report.example/index.html"),
            ),
        )

    assert fields == {
        "success": True,
        "fields": {
            "field_9193cb": "candidate conclusion",
            "field_8c912e": "http://report.example/index.html",
        },
    }
    assert update == {"success": True}
    assert calls[0].count("--fields") == 2
    update_params = json.loads(calls[1][calls[1].index("--params") + 1])
    assert update_params == {
        "fields": [
            {
                "field_key": "field_9193cb",
                "field_value": "candidate conclusion",
            },
            {
                "field_key": "field_8c912e",
                "field_value": "http://report.example/index.html",
            },
        ]
    }


def test_meegle_adapter_combines_exact_fields_with_all_full_comment_bodies():
    calls = []
    marker = "[RCA_DELIVERY:effect-705:artifact-705]"

    def runner(args):
        calls.append(args)
        if args[:2] == ["workitem", "get"]:
            return (
                0,
                json.dumps({
                    "work_item_fields": [
                        {"key": "field_9193cb", "value": "root cause"},
                        {
                            "key": "field_8c912e",
                            "value": {"link": "https://rca.example/report/index.html"},
                        },
                    ]
                }),
                "",
            )
        if args[:2] == ["comment", "list"]:
            page = int(args[args.index("--page-num") + 1])
            if page == 1:
                return (
                    0,
                    json.dumps({
                        "comments": [
                            {
                                "comment_id": "c-unrelated",
                                "content": "full unrelated body",
                            }
                        ],
                        "has_more": True,
                    }),
                    "",
                )
            return (
                0,
                json.dumps({
                    "comments": [
                        {
                            "comment_id": "c-rca",
                            "content": f"canonical report\n{marker}\nfull tail",
                        }
                    ],
                    "has_more": False,
                }),
                "",
            )
        raise AssertionError(args)

    result = MeegleIssueCommentAdapter(runner).get_fields_and_comments(
        "68ef617fb371dc80a10641f7",
        "7051585084",
    )

    assert result == {
        "success": True,
        "source": "official_meegle_api",
        "scope": {
            "project_key": "68ef617fb371dc80a10641f7",
            "work_item_id": "7051585084",
        },
        "fields": {
            "field_9193cb": "root cause",
            "field_8c912e": "https://rca.example/report/index.html",
        },
        "comments": [
            {"remote_id": "c-unrelated", "content": "full unrelated body"},
            {
                "remote_id": "c-rca",
                "content": f"canonical report\n{marker}\nfull tail",
            },
        ],
        "pages_read": 2,
    }
    assert [call[:2] for call in calls] == [
        ["workitem", "get"],
        ["comment", "list"],
        ["comment", "list"],
    ]


def test_meegle_combined_readback_never_returns_partial_success():
    def runner(args):
        if args[:2] == ["workitem", "get"]:
            return (
                0,
                json.dumps({
                    "work_item_fields": [
                        {"key": "field_9193cb", "value": "root cause"},
                        {"key": "field_8c912e", "value": "https://rca.example/report"},
                    ]
                }),
                "",
            )
        if args[:2] == ["comment", "list"]:
            return 1, "", "permission denied"
        raise AssertionError(args)

    result = MeegleIssueCommentAdapter(runner).get_fields_and_comments(
        "68ef617fb371dc80a10641f7", "7051585084"
    )

    assert result["success"] is False
    assert result["error_code"] == "feishu_permission_denied"
    assert "fields" not in result


def test_meegle_work_item_not_found_is_permanent_but_other_server_errors_retry():
    missing = dispatcher_module._error_payload(
        1,
        json.dumps({
            "data": None,
            "error": {
                "code": "SERVER_CALL_FAILED",
                "message": "work_item_id not found\nlogid: exact-request-id",
                "retryable": True,
            },
        }),
        "",
    )
    transient = dispatcher_module._error_payload(
        1,
        json.dumps({
            "error": {
                "code": "SERVER_CALL_FAILED",
                "message": "upstream unavailable",
                "retryable": True,
            },
        }),
        "",
    )

    assert missing["error_code"] == "feishu_work_item_not_found"
    assert missing["permanent"] is True
    assert transient["error_code"] == "meegle_call_failed"
    assert "permanent" not in transient


def test_meegle_adapter_allows_terminal_result_only_but_never_report_only():
    calls = []

    def runner(args):
        calls.append(args)
        if args[:2] == ["workitem", "get"]:
            return (
                0,
                json.dumps({
                    "work_item_fields": [
                        {"key": "field_9193cb", "value": ""},
                    ]
                }),
                "",
            )
        if args[:2] == ["workitem", "update"]:
            return 0, json.dumps({"updated": True}), ""
        raise AssertionError(args)

    adapter = MeegleIssueCommentAdapter(runner)
    assert adapter.get_fields("t03o4q", "7041712812", ("field_9193cb",)) == {
        "success": True,
        "fields": {"field_9193cb": ""},
    }
    with dispatcher_module._bound_provider_write_guard(_TEST_PROVIDER_WRITE_CLAIM):
        assert adapter.update_fields(
            "t03o4q",
            "7041712812",
            (("field_9193cb", "自动归因未完成（非归因结论）"),),
        ) == {"success": True}
    assert (
        adapter.update_fields(
            "t03o4q",
            "7041712812",
            (("field_8c912e", "https://invalid.example/report"),),
        )["error_code"]
        == "feishu_field_allowlist_invalid"
    )
    assert [item[:2] for item in calls] == [
        ["workitem", "get"],
        ["workitem", "update"],
    ]


def test_terminal_result_only_accepts_omitted_empty_field_with_full_metadata():
    calls = []

    def runner(args):
        calls.append(args)
        if args[:2] == ["workitem", "get"]:
            return (
                0,
                json.dumps({
                    "work_item_attribute": {
                        "work_item_id": "7041712812",
                        "work_item_type": {"key": "issue", "name": "Issue"},
                    }
                }),
                "",
            )
        if args[:2] == ["workitem", "meta-fields"]:
            return (
                0,
                json.dumps({
                    "list": [
                        {
                            "field_key": "field_9193cb",
                            "field_name": "归因结果",
                            "field_type": "text",
                        },
                        {
                            "field_key": "field_8c912e",
                            "field_name": "归因报告",
                            "field_type": "link",
                        },
                    ]
                }),
                "",
            )
        raise AssertionError(args)

    result = MeegleIssueCommentAdapter(runner).get_fields(
        "t03o4q", "7041712812", ("field_9193cb",)
    )

    assert result == {"success": True, "fields": {"field_9193cb": ""}}
    assert calls[1].count("--field-keys") == 1
    assert "field_9193cb" in calls[1]
    assert "field_8c912e" not in calls[1]


def test_meegle_adapter_verifies_omitted_attribution_fields_are_empty():
    calls = []

    def runner(args):
        calls.append(args)
        if args[:2] == ["workitem", "get"]:
            return (
                0,
                json.dumps({
                    "work_item_attribute": {
                        "work_item_id": "7041712812",
                        "work_item_type": {"key": "issue", "name": "Issue"},
                    }
                }),
                "",
            )
        if args[:2] == ["workitem", "meta-fields"]:
            return (
                0,
                json.dumps({
                    "list": [
                        {
                            "field_key": "field_9193cb",
                            "field_name": "归因结果",
                            "field_type": "text",
                        },
                        {
                            "field_key": "field_8c912e",
                            "field_name": "归因报告",
                            "field_type": "link",
                        },
                    ]
                }),
                "",
            )
        raise AssertionError(args)

    result = MeegleIssueCommentAdapter(runner).get_fields(
        "t03o4q",
        "7041712812",
        ("field_9193cb", "field_8c912e"),
    )

    assert result == {
        "success": True,
        "fields": {"field_9193cb": "", "field_8c912e": ""},
    }
    assert calls[1] == [
        "workitem",
        "meta-fields",
        "--project-key",
        "t03o4q",
        "--work-item-type",
        "issue",
        "--page-num",
        "1",
        "--field-keys",
        "field_9193cb",
        "--field-keys",
        "field_8c912e",
        "--format",
        "json",
    ]


def test_meegle_adapter_rejects_omitted_fields_without_exact_metadata():
    def runner(args):
        if args[:2] == ["workitem", "get"]:
            return (
                0,
                json.dumps({
                    "work_item_attribute": {
                        "work_item_id": "7041712812",
                        "work_item_type": {"key": "issue", "name": "Issue"},
                    }
                }),
                "",
            )
        if args[:2] == ["workitem", "meta-fields"]:
            return (
                0,
                json.dumps({
                    "list": [
                        {
                            "field_key": "field_9193cb",
                            "field_name": "归因结果",
                            "field_type": "text",
                        }
                    ]
                }),
                "",
            )
        raise AssertionError(args)

    result = MeegleIssueCommentAdapter(runner).get_fields(
        "t03o4q",
        "7041712812",
        ("field_9193cb", "field_8c912e"),
    )

    assert result["success"] is False
    assert result["permanent"] is True
    assert result["error_code"] == "feishu_field_metadata_invalid"


def test_adoption_default_value_without_operation_is_unreviewed():
    calls = []

    def runner(args):
        calls.append(args)
        if args[:2] == ["workitem", "get"]:
            return (
                0,
                json.dumps(
                    {
                        "work_item_fields": [
                            {
                                "key": "field_b23cb8",
                                "value": {"label": "采纳", "value": "rya79_oos"},
                            }
                        ]
                    },
                    ensure_ascii=False,
                ),
                "",
            )
        if args[:2] == ["workitem", "list-op-records"]:
            return 0, json.dumps({"has_more": False, "op_records": []}), ""
        raise AssertionError(args)

    result = MeegleIssueCommentAdapter(runner).read_adoption(
        "t03o4q",
        "7048004715",
        start_ms=1784736000000,
        end_ms=1785340799000,
    )

    assert result["success"] is True
    assert result["status"] == "unreviewed"
    assert result["reason"] == "current_value_has_no_user_operation_record"
    assert result["ignored_default_value"] == "rya79_oos"
    assert [call[:2] for call in calls] == [
        ["workitem", "get"],
        ["workitem", "list-op-records"],
    ]


def test_adoption_read_windows_long_span_and_follows_start_from():
    calls = []
    max_window = dispatcher_module.MAX_MEEGLE_OPERATION_WINDOW_MS

    def runner(args):
        calls.append(args)
        if args[:2] == ["workitem", "get"]:
            return (
                0,
                json.dumps(
                    {
                        "work_item_fields": [
                            {
                                "key": "field_b23cb8",
                                "value": {"label": "不采纳", "value": "0ivvg65i7"},
                            }
                        ]
                    },
                    ensure_ascii=False,
                ),
                "",
            )
        if args[:2] != ["workitem", "list-op-records"]:
            raise AssertionError(args)
        start = int(args[args.index("--start") + 1])
        if start == 0 and "--start-from" not in args:
            return (
                0,
                json.dumps({
                    "has_more": True,
                    "start_from": "page-2",
                    "op_records": [],
                }),
                "",
            )
        if start == 0:
            assert args[args.index("--start-from") + 1] == "page-2"
            return (
                0,
                json.dumps({
                    "has_more": False,
                    "op_records": [
                        {
                            "operation_time": 1000,
                            "operator": "user-1",
                            "operator_type": "user",
                            "record_contents": [
                                {
                                    "object": {
                                        "object_type": "field",
                                        "object_value": "field_b23cb8",
                                    },
                                    "old": ["rya79_oos"],
                                    "new": ["0ivvg65i7"],
                                }
                            ],
                        }
                    ],
                }),
                "",
            )
        assert start == max_window + 1
        return 0, json.dumps({"has_more": False, "op_records": []}), ""

    result = MeegleIssueCommentAdapter(runner).read_adoption(
        "t03o4q",
        "7048004715",
        start_ms=0,
        end_ms=max_window + 10,
    )

    assert result["success"] is True
    assert result["status"] == "rejected"
    assert result["explicit"] is True
    assert result["operation"]["new"] == "0ivvg65i7"
    assert result["windows_read"] == 2
    operation_calls = [
        call for call in calls if call[:2] == ["workitem", "list-op-records"]
    ]
    assert len(operation_calls) == 3
    for call in operation_calls:
        start = int(call[call.index("--start") + 1])
        end = int(call[call.index("--end") + 1])
        assert end - start <= max_window


def test_adoption_read_rejects_repeated_start_from():
    def runner(args):
        if args[:2] == ["workitem", "get"]:
            return (
                0,
                json.dumps({
                    "work_item_fields": [{"key": "field_b23cb8", "value": "rya79_oos"}]
                }),
                "",
            )
        if args[:2] == ["workitem", "list-op-records"]:
            return (
                0,
                json.dumps({
                    "has_more": True,
                    "start_from": "same-token",
                    "op_records": [],
                }),
                "",
            )
        raise AssertionError(args)

    result = MeegleIssueCommentAdapter(runner).read_adoption(
        "t03o4q",
        "7048004715",
        start_ms=0,
        end_ms=1000,
    )

    assert result["success"] is False
    assert result["error_code"] == "g1q3_adoption_pagination_cycle"


def test_closed_generation_uses_half_open_window_without_current_value_match():
    calls = []

    def runner(args):
        calls.append(args)
        if args[:2] == ["workitem", "get"]:
            return (
                0,
                json.dumps({
                    "work_item_fields": [{"key": "field_b23cb8", "value": "0ivvg65i7"}]
                }),
                "",
            )
        if args[:2] == ["workitem", "list-op-records"]:
            return (
                0,
                json.dumps({
                    "has_more": False,
                    "op_records": [
                        {
                            "operation_time": 1500,
                            "operator": "user-1",
                            "operator_type": "user",
                            "record_contents": [
                                {
                                    "object": {
                                        "object_type": "field",
                                        "object_value": "field_b23cb8",
                                    },
                                    "old": [],
                                    "new": ["rya79_oos"],
                                }
                            ],
                        }
                    ],
                }),
                "",
            )
        raise AssertionError(args)

    result = MeegleIssueCommentAdapter(runner).read_generation_adoption(
        "t03o4q",
        "7048004715",
        generation=2,
        conclusion_time_ms=1000,
        next_conclusion_time_ms=2000,
    )

    assert result["success"] is True
    assert result["status"] == "adopted"
    assert result["generation"] == 2
    assert result["end_ms"] == 1999
    operation_call = next(
        call for call in calls if call[:2] == ["workitem", "list-op-records"]
    )
    assert operation_call[operation_call.index("--end") + 1] == "1999"


def test_current_generation_requires_field_to_match_latest_operation():
    def runner(args):
        if args[:2] == ["workitem", "get"]:
            return (
                0,
                json.dumps({
                    "work_item_fields": [{"key": "field_b23cb8", "value": "0ivvg65i7"}]
                }),
                "",
            )
        if args[:2] == ["workitem", "list-op-records"]:
            return (
                0,
                json.dumps({
                    "has_more": False,
                    "op_records": [
                        {
                            "operation_time": 1500,
                            "operator": "user-1",
                            "operator_type": "user",
                            "record_contents": [
                                {
                                    "object": {
                                        "object_type": "field",
                                        "object_value": "field_b23cb8",
                                    },
                                    "new": ["rya79_oos"],
                                }
                            ],
                        }
                    ],
                }),
                "",
            )
        raise AssertionError(args)

    result = MeegleIssueCommentAdapter(runner).read_generation_adoption(
        "t03o4q",
        "7048004715",
        generation=3,
        conclusion_time_ms=1000,
        observed_at_ms=2000,
    )

    assert result["success"] is False
    assert result["error_code"] == "g1q3_adoption_current_operation_mismatch"


def test_adoption_read_rejects_operation_outside_requested_window():
    def runner(args):
        if args[:2] == ["workitem", "get"]:
            return (
                0,
                json.dumps({
                    "work_item_fields": [{"key": "field_b23cb8", "value": "rya79_oos"}]
                }),
                "",
            )
        if args[:2] == ["workitem", "list-op-records"]:
            return (
                0,
                json.dumps({
                    "has_more": False,
                    "op_records": [
                        {
                            "operation_time": 999,
                            "operator": "user-1",
                            "operator_type": "user",
                            "record_contents": [
                                {
                                    "object": {
                                        "object_type": "field",
                                        "object_value": "field_b23cb8",
                                    },
                                    "new": ["rya79_oos"],
                                }
                            ],
                        }
                    ],
                }),
                "",
            )
        raise AssertionError(args)

    result = MeegleIssueCommentAdapter(runner).read_adoption(
        "t03o4q",
        "7048004715",
        start_ms=1000,
        end_ms=2000,
    )

    assert result["success"] is False
    assert result["error_code"] == "g1q3_adoption_operation_out_of_window"


@pytest.mark.parametrize("next_time,observed_time", [(2000, None), (None, 2000)])
def test_generation_adoption_rejects_invalid_conclusion_time(
    next_time,
    observed_time,
):
    result = MeegleIssueCommentAdapter(
        lambda _args: pytest.fail("must not read")
    ).read_generation_adoption(
        "t03o4q",
        "7048004715",
        generation=1,
        conclusion_time_ms="1000",
        next_conclusion_time_ms=next_time,
        observed_at_ms=observed_time,
    )

    assert result["success"] is False
    assert result["error_code"] == "g1q3_adoption_conclusion_time_invalid"


def test_default_report_verifier_performs_bounded_head_then_get(monkeypatch):
    monkeypatch.setenv(
        "PNC_FOXGLOVE_RENDER_HOST",
        "http://192.168.26.174:18081",
    )
    body = b"<!doctype html><title>RCA</title>"
    calls = []

    class Response:
        def __init__(self, payload=b""):
            self.payload = io.BytesIO(payload)
            self.headers = {"Content-Length": str(len(body))}

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def getcode(self):
            return 200

        def read(self, size=-1):
            return self.payload.read(size)

    class Opener:
        def open(self, request, timeout):
            calls.append((request.get_method(), request.full_url, timeout))
            return Response(body if request.get_method() == "GET" else b"")

    monkeypatch.setattr(
        dispatcher_module.urllib_request,
        "build_opener",
        lambda handler: Opener(),
    )
    url = (
        "http://192.168.26.174:18081/G1Q3_RCA/cases/"
        f"{'g1q3-rca-s1-' + 'a' * 64}/"
        f"{'g1q3-rca-artifact-v1-' + 'b' * 64}/index.html"
    )
    result = default_report_verifier(
        url, len(body), hashlib.sha256(body).hexdigest(), timeout_seconds=7
    )
    assert result == {
        "success": True,
        "status_code": 200,
        "content_length": len(body),
        "sha256": hashlib.sha256(body).hexdigest(),
    }
    assert [(method, called_url) for method, called_url, _timeout in calls] == [
        ("HEAD", url),
        ("GET", url),
    ]
    assert all(0 < timeout <= 7 for _method, _url, timeout in calls)


def test_default_report_verifier_restats_viz_then_probes_renderer_only(monkeypatch):
    submission_key = "g1q3-rca-s1-" + "a" * 64
    path = canonical_viz_mcap_path(submission_key)
    url = foxglove_url(path)
    expected_size = 17
    expected_sha256 = "b" * 64
    calls = []
    monkeypatch.setattr(
        dispatcher_module,
        "_verify_local_viz_mcap",
        lambda observed_path, size, sha256: {
            "success": True,
            "content_length": size,
            "sha256": sha256,
            "observed_path": observed_path,
        },
    )

    class Response:
        headers = {}

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def getcode(self):
            return 200

    class Opener:
        def open(self, request, timeout):
            calls.append((request.get_method(), request.full_url, timeout))
            return Response()

    result = default_report_verifier(
        url,
        expected_size,
        expected_sha256,
        timeout_seconds=7,
        opener=Opener(),
    )

    assert result == {
        "success": True,
        "status_code": 200,
        "content_length": expected_size,
        "sha256": expected_sha256,
        "viz_mcap_path": path,
        "renderer_probe": "spa_endpoint_only",
    }
    assert [(method, called_url) for method, called_url, _timeout in calls] == [
        ("HEAD", "https://192.168.21.217/")
    ]
    assert 0 < calls[0][2] <= 7


def test_default_report_verifier_remote_read_accepts_sealed_missing_viz(monkeypatch):
    path = canonical_viz_mcap_path("g1q3-rca-s1-" + "a" * 64)
    url = foxglove_url(path)
    monkeypatch.setenv("HERMES_RCA_OUTBOX_DATA_ACCESS_MODE", "remote_read")
    monkeypatch.setattr(
        dispatcher_module,
        "_verify_local_viz_mcap",
        lambda *_args: {"success": False, "error_code": "viz_mcap_missing"},
    )

    class ForbiddenOpener:
        def open(self, *_args, **_kwargs):
            raise AssertionError("missing viz must stop before renderer probe")

    result = default_report_verifier(
        url,
        17,
        "b" * 64,
        opener=ForbiddenOpener(),
    )

    assert result == {
        "success": True,
        "status_code": 200,
        "content_length": 17,
        "sha256": "b" * 64,
        "viz_mcap_path": path,
        "renderer_probe": "upstream_sealed_remote_publication",
    }


def test_default_report_verifier_rejects_noncanonical_viz_url_before_io():
    invalid = (
        "https://192.168.21.217/?ds=foxglove-http&"
        "ds.mcapPath=/mnt/tmp/not-publishable.viz.mcap"
    )

    result = default_report_verifier(invalid, 17, "b" * 64)

    assert result["success"] is False
    assert result["permanent"] is True


def test_default_report_verifier_fails_closed_when_internal_service_is_unreachable(
    monkeypatch,
):
    monkeypatch.setenv(
        "PNC_FOXGLOVE_RENDER_HOST",
        "http://192.168.26.174:18081",
    )

    class UnreachableOpener:
        def open(self, request, timeout):
            del request, timeout
            raise dispatcher_module.urllib_error.URLError("unreachable")

    url = (
        "http://192.168.26.174:18081/G1Q3_RCA/cases/"
        f"{'g1q3-rca-s1-' + 'a' * 64}/"
        f"{'g1q3-rca-artifact-v1-' + 'b' * 64}/index.html"
    )

    result = default_report_verifier(
        url,
        1,
        "0" * 64,
        opener=UnreachableOpener(),
    )

    assert result["success"] is False
    assert result["error_code"] == "report_http_unavailable"


def test_default_report_verifier_enforces_one_total_stream_deadline():
    expected_size = 2 * 1024 * 1024

    class Monotonic:
        current = 0.0

        def __call__(self):
            return self.current

    class Socket:
        def __init__(self):
            self.timeouts = []

        def settimeout(self, timeout):
            self.timeouts.append(timeout)

    class Raw:
        def __init__(self, socket):
            self._sock = socket

    class File:
        def __init__(self, socket):
            self.raw = Raw(socket)

    class Response:
        def __init__(self, clock, socket, *, slow):
            self.clock = clock
            self.fp = File(socket)
            self.headers = {"Content-Length": str(expected_size)}
            self.remaining = expected_size if slow else 0
            self.slow = slow

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def getcode(self):
            return 200

        def read(self, size=-1):
            if not self.slow or self.remaining <= 0:
                return b""
            self.clock.current += 6
            count = min(size, self.remaining)
            self.remaining -= count
            return b"x" * count

    class Opener:
        def __init__(self, clock, socket):
            self.clock = clock
            self.socket = socket
            self.calls = []

        def open(self, request, timeout):
            self.calls.append((request.get_method(), timeout))
            return Response(
                self.clock,
                self.socket,
                slow=request.get_method() == "GET",
            )

    monotonic = Monotonic()
    socket = Socket()
    opener = Opener(monotonic, socket)
    url = (
        "https://viewer.internal/G1Q3_RCA/cases/"
        f"{'g1q3-rca-s1-' + 'a' * 64}/"
        f"{'g1q3-rca-artifact-v1-' + 'b' * 64}/assets/media/video.mp4"
    )

    result = default_report_verifier(
        url,
        expected_size,
        "0" * 64,
        timeout_seconds=10,
        monotonic=monotonic,
        opener=opener,
    )

    assert result["success"] is False
    assert result["error_code"] == "report_http_timeout"
    assert opener.calls == [("HEAD", 10.0), ("GET", 10.0)]
    assert socket.timeouts == [10.0, 4.0]


def test_production_launchd_is_secret_free_and_runs_only_dispatcher():
    root = Path(__file__).resolve().parents[2]
    path = root / "local.pnc.rca-delivery-dispatcher.plist"
    with path.open("rb") as handle:
        payload = plistlib.load(handle)
    assert payload["Label"] == "local.pnc.rca-delivery-dispatcher"
    assert payload["ProgramArguments"] == [
        "/usr/bin/python3",
        "/Users/songying/.hermes/runtime/governance-tools/pnc_live_exec.py",
        "local.pnc.rca-delivery-dispatcher",
    ]
    assert payload["WorkingDirectory"] == "/Users/songying/.hermes/runtime"
    environment = payload["EnvironmentVariables"]
    assert environment["HERMES_HOME"] == "/Users/songying/.hermes"
    assert environment["PYTHONNOUSERSITE"] == "1"
    assert "VIRTUAL_ENV" not in environment
    serialized = json.dumps(payload, sort_keys=True)
    assert "/runtime/releases/" not in serialized
    assert "/runtime/venvs/" not in serialized
    assert "/runtime/hermes-live" not in serialized
    assert not any(
        token in key.upper()
        for key in environment
        for token in ("PASSWORD", "SECRET", "TOKEN", "COOKIE", "KAFKA")
    )
