from __future__ import annotations

from dataclasses import replace
import hashlib
import json
from datetime import timedelta
from pathlib import PurePosixPath
import sqlite3
import subprocess
import sys
from types import SimpleNamespace

import pytest

from gateway.pnc_rca_delivery_contract import (
    DeliveryContractError,
    TERMINAL_FALLBACK_CONTRACT_SCHEMA_VERSION,
    TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION,
)
from gateway.pnc_rca_control_store import RcaControlStore
from gateway.pnc_rca_delivery_store import RcaDeliveryStore
from scripts import pnc_rca_delivery_collector as collector
from scripts.pnc_foxglove_delivery import canonical_viz_mcap_path
from tests.gateway.test_pnc_rca_delivery_store import (
    NOW,
    _bind_activation_execution,
    _control,
    _delivery,
    _insert_subscription,
)
from tests.gateway.test_pnc_rca_w3_snapshot import _runtime_authority


def _config_env(tmp_path) -> dict[str, str]:
    return {
        "HERMES_RCA_DELIVERY_COLLECTOR_ENABLED": "true",
        "HERMES_RCA_DELIVERY_COLLECTOR_CONTROL_DB_PATH": str(
            tmp_path / "control.sqlite3"
        ),
        "HERMES_RCA_DELIVERY_COLLECTOR_HEALTH_PATH": str(tmp_path / "health.json"),
        "HERMES_RCA_DELIVERY_COLLECTOR_SSH_MINI_AGENT": "/safe/ssh-mini-agent",
        "HERMES_RCA_DELIVERY_COLLECTOR_ARTIFACT_READ_TIMEOUT_SECONDS": "30",
        "HERMES_RCA_DELIVERY_COLLECTOR_LEASE_SECONDS": "60",
        "HERMES_RCA_DELIVERY_COLLECTOR_CAPACITY_SAMPLE_ENABLED": "true",
    }


def _json_bytes(value) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True).encode("utf-8")


def _gate_a_capability(*, status="supported"):
    return {
        "actual_evaluators": [
            {"evaluator_id": "aeb_trigger", "status": status}
        ],
        "actual_signals": ["AEBReq"],
        "actual_fields": [],
    }


def _submission_title_claim(
    *,
    source_title: str,
    receipt_title: str | None = None,
    receipt_title_sha256: str | None = None,
):
    result = {"success": True}
    if receipt_title is not None:
        result["work_item"] = {
            "title": receipt_title,
            "title_sha256": (
                receipt_title_sha256
                if receipt_title_sha256 is not None
                else collector.issue_title_sha256(receipt_title)
            ),
        }
    return SimpleNamespace(
        submission_payload={
            "trigger_context": {
                "schema_version": "pnc_rca_trigger_context_v1",
                "source_kind": "feishu_group_manual",
                "creation_rule_version": "rca-rule-v1",
                "project_key": "t03o4q",
                "project_simple_name": "g1q3",
                "work_item_type_key": "issue",
                "work_item_id": "7065539652",
                "issue_url": (
                    "https://project.feishu.cn/g1q3/issue/detail/7065539652"
                ),
                "title": source_title,
            }
        },
        submission_result=result,
    )


def test_submission_issue_title_accepts_bound_receipt_fallback():
    claim = _submission_title_claim(
        source_title="",
        receipt_title="ACC-右车近距离切入ACC不减速",
    )

    assert collector._submission_issue_title(claim) == (
        "ACC-右车近距离切入ACC不减速"
    )


def test_submission_issue_title_rejects_receipt_hash_mismatch():
    claim = _submission_title_claim(
        source_title="",
        receipt_title="ACC-右车近距离切入ACC不减速",
        receipt_title_sha256="f" * 64,
    )

    with pytest.raises(
        DeliveryContractError,
        match="submission_receipt_identity_mismatch",
    ):
        collector._submission_issue_title(claim)


def test_submission_issue_title_rejects_original_receipt_conflict():
    claim = _submission_title_claim(
        source_title="原始问题标题",
        receipt_title="另一个问题标题",
    )

    with pytest.raises(
        DeliveryContractError,
        match="submission_receipt_identity_mismatch",
    ):
        collector._submission_issue_title(claim)


def test_submission_issue_title_keeps_matching_original_authoritative():
    claim = _submission_title_claim(
        source_title="原始问题标题",
        receipt_title="原始问题标题",
    )

    assert collector._submission_issue_title(claim) == "原始问题标题"


def test_submission_issue_title_without_original_or_receipt_remains_missing():
    claim = _submission_title_claim(source_title="")

    with pytest.raises(
        DeliveryContractError,
        match="submission_issue_title_missing",
    ):
        collector._submission_issue_title(claim)


def _manifest_row(path, role, raw, media_type):
    return {
        "role": role,
        "path": path,
        "size": len(raw),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "media_type": media_type,
        "required": True,
    }


def _run_remote_bundle_reader(
    tmp_path,
    monkeypatch,
    *,
    report_path="sealed-safe.json",
    report_value=None,
    extra_rows=(),
    script_transform=lambda script: script,
):
    submission_key = "g1q3-rca-s1-" + "e" * 64
    root = tmp_path / "bundle"
    root.mkdir()
    html_raw = b"<!doctype html><html><body>sealed report</body></html>"
    report_value = report_value or {
        "input_materialized": False,
        "failure_class": "remote_event_not_found",
        "event_uuid": "sealed-safe",
    }
    report_raw = _json_bytes(report_value)
    (root / "index.html").write_bytes(html_raw)
    (root / report_path).write_bytes(report_raw)
    rows = [
        _manifest_row("index.html", "index_html", html_raw, "text/html"),
        _manifest_row(report_path, "report_data", report_raw, "application/json"),
        *extra_rows,
    ]
    (root / "delivery_contract.json").write_bytes(_json_bytes({"artifacts": {}}))
    (root / "delivery_manifest.json").write_bytes(_json_bytes({"artifacts": rows}))
    monkeypatch.setattr(
        collector, "canonical_artifact_root", lambda _key: str(root) + "/"
    )
    monkeypatch.setattr(
        collector,
        "canonical_viz_mcap_path",
        lambda key: str(tmp_path / "viz" / f"{key}.viz.mcap"),
    )
    script = script_transform(collector._remote_bundle_script(submission_key))
    process = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert process.returncode == 0, process.stderr
    return json.loads(process.stdout)


def test_remote_bundle_reader_uses_formal_viz_publication_root():
    submission_key = "g1q3-rca-s1-" + "a" * 64
    formal_root = str(PurePosixPath(canonical_viz_mcap_path(submission_key)).parent)
    script = collector._remote_bundle_script(submission_key)

    assert f"FORMAL_VIZ_ROOT = {formal_root!r}" in script
    assert "FORMAL_VIZ_ROOT = posixpath.normpath(ROOT)" not in script


def test_remote_bundle_reader_scans_sealed_public_artifacts_for_banned_phrases():
    script = collector._remote_bundle_script("g1q3-rca-s1-" + "c" * 64)

    assert repr(tuple(collector.BANNED_PUBLIC_PHRASES)) in script
    assert "raise RuntimeError('public_artifact_banned_phrase')" in script
    assert "json.dumps(report_data, ensure_ascii=False, sort_keys=True)" in script
    assert "reject_banned_public_phrase(text)" in script
    assert "except RuntimeError:\n            report_data = {}" not in script
    assert "report_data_missing" in collector._EVENTUAL_ARTIFACT_CODES


def test_remote_bundle_reader_uses_manifest_report_instead_of_fixed_filename(
    tmp_path, monkeypatch
):
    root = tmp_path / "bundle"

    def add_unsafe_fixed_file(script):
        (root / "report_data.json").write_bytes(
            _json_bytes({"conclusion": "ACC 是责任方", "event_uuid": "unsafe"})
        )
        return script

    payload = _run_remote_bundle_reader(
        tmp_path,
        monkeypatch,
        script_transform=add_unsafe_fixed_file,
    )

    assert payload["ok"] is True
    assert payload["gate_a_source"]["event_uuid"] == "sealed-safe"
    assert "read_json(ROOT + 'report_data.json'" not in collector._remote_bundle_script(
        "g1q3-rca-s1-" + "f" * 64
    )


def test_remote_bundle_reader_returns_manifest_bound_issue_focus(tmp_path, monkeypatch):
    focus = {"schema_version": "focus-fixture-v1", "analysis_status": "complete"}

    payload = _run_remote_bundle_reader(
        tmp_path,
        monkeypatch,
        report_value={
            "input_materialized": True,
            "event_uuid": "focus-fixture",
            "issue_focus": focus,
        },
    )

    assert payload["ok"] is True
    assert payload["report_issue_focus"] == focus


def test_remote_bundle_reader_rejects_missing_report_role(tmp_path, monkeypatch):
    def remove_report_row(script):
        manifest_path = tmp_path / "bundle" / "delivery_manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["artifacts"] = [manifest["artifacts"][0]]
        manifest_path.write_bytes(_json_bytes(manifest))
        return script

    payload = _run_remote_bundle_reader(
        tmp_path,
        monkeypatch,
        script_transform=remove_report_row,
    )

    assert payload["error_code"] == "required_report_data_artifact_missing"


def test_remote_bundle_reader_rejects_duplicate_report_role(tmp_path, monkeypatch):
    duplicate_raw = _json_bytes({"input_materialized": False})

    def add_duplicate_file(script):
        (tmp_path / "bundle" / "other.json").write_bytes(duplicate_raw)
        return script

    payload = _run_remote_bundle_reader(
        tmp_path,
        monkeypatch,
        extra_rows=(
            _manifest_row(
                "other.json", "report_data", duplicate_raw, "application/json"
            ),
        ),
        script_transform=add_duplicate_file,
    )

    assert payload["error_code"] == "delivery_manifest_duplicate_artifact"


def test_remote_bundle_reader_rejects_non_json_report_path(tmp_path, monkeypatch):
    payload = _run_remote_bundle_reader(
        tmp_path,
        monkeypatch,
        report_path="sealed-safe.txt",
    )

    assert payload["error_code"] == "required_report_data_artifact_invalid"


def test_remote_bundle_reader_rejects_report_identity_change(tmp_path, monkeypatch):
    def replace_report_during_read(script):
        report_path = tmp_path / "bundle" / "sealed-safe.json"
        replacement = type(report_path)(str(report_path) + ".replacement")
        replacement.write_bytes(report_path.read_bytes())
        injected = (
            "os.replace(path + '.replacement', path)\n        after = os.fstat(fd)"
        )
        prefix, separator, suffix = script.rpartition("after = os.fstat(fd)")
        assert separator
        return prefix + injected + suffix

    payload = _run_remote_bundle_reader(
        tmp_path,
        monkeypatch,
        script_transform=replace_report_during_read,
    )

    assert payload["error_code"] == "report_data_changed_during_read"


@pytest.mark.parametrize(
    ("field", "value", "error_code"),
    [
        ("size", 1, "report_data_size_mismatch"),
        ("sha256", "0" * 64, "report_data_hash_mismatch"),
    ],
)
def test_remote_bundle_reader_binds_report_size_and_hash(
    tmp_path, monkeypatch, field, value, error_code
):
    def corrupt_manifest_binding(script):
        manifest_path = tmp_path / "bundle" / "delivery_manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["artifacts"][1][field] = value
        manifest_path.write_bytes(_json_bytes(manifest))
        return script

    payload = _run_remote_bundle_reader(
        tmp_path,
        monkeypatch,
        script_transform=corrupt_manifest_binding,
    )

    assert payload["error_code"] == error_code


def test_remote_bundle_reader_rejects_malformed_json_report(tmp_path, monkeypatch):
    def replace_with_invalid_json(script):
        root = tmp_path / "bundle"
        raw = b"not-json"
        (root / "sealed-safe.json").write_bytes(raw)
        manifest_path = root / "delivery_manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["artifacts"][1].update({
            "size": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(),
        })
        manifest_path.write_bytes(_json_bytes(manifest))
        return script

    payload = _run_remote_bundle_reader(
        tmp_path,
        monkeypatch,
        script_transform=replace_with_invalid_json,
    )

    assert payload["error_code"] == "report_data_json_invalid"


def test_remote_bundle_reader_applies_json_size_limit_to_report(tmp_path, monkeypatch):
    def lower_report_limit(script):
        return script.replace(
            "MAX_JSON_BYTES if is_report_data else MAX_FILE_BYTES",
            "64 if is_report_data else MAX_FILE_BYTES",
        )

    payload = _run_remote_bundle_reader(
        tmp_path,
        monkeypatch,
        script_transform=lower_report_limit,
    )

    assert payload["error_code"] == "report_data_missing_size_invalid"


def test_remote_bundle_reader_binds_gate_a_source_and_safe_projection():
    script = collector._remote_bundle_script("g1q3-rca-s1-" + "d" * 64)

    assert "'gate_a_source'" in script
    assert "'gate_a_level': 'L0_abstain'" in script
    assert "recomputes the canonical projection" in script
    assert "responsibility_candidate" not in script.split("def public_report_projection", 1)[1].split("def read_text_artifact", 1)[0]


def test_host_gate_a_projection_replaces_candidate_bearing_contract():
    bundle = collector._apply_gate_a_bundle_projection({
        "delivery_contract": {
            "consumer_capability": _gate_a_capability(),
            "summary": {"short_conclusion": "candidate ACC"},
            "report": {"candidate_owner_domain": "ACC", "is_candidate": True},
        },
        "gate_a_source": {
            "input_materialized": True,
            "rca_evaluators": [
                {
                    "key": "aeb_trigger",
                    "status": "supported",
                    "evidence_refs": [
                        {
                            "signal": "AEBReq",
                            "evidence": "窗口内观测到 AEB 请求。",
                        }
                    ],
                },
            ],
        },
    })

    public = bundle["delivery_contract"]["public_result"]
    assert public["gate_a_level"] == "L1_observation"
    assert public["responsibility"]["candidate"] == "暂无法判断"
    assert "candidate_owner_domain" not in bundle["delivery_contract"]["report"]


def test_host_gate_a_projection_rejects_malformed_evaluator_source():
    with pytest.raises(DeliveryContractError, match="gate_a_projection_invalid"):
        collector._apply_gate_a_bundle_projection({
            "delivery_contract": {
                "consumer_capability": _gate_a_capability(),
            },
            "gate_a_source": {
                "input_materialized": True,
                "rca_evaluators": [{"status": "not-a-valid-status"}],
            },
        })


def test_host_gate_a_projection_rejects_missing_source_envelope():
    with pytest.raises(DeliveryContractError, match="gate_a_source_missing"):
        collector._apply_gate_a_bundle_projection({
            "delivery_contract": {"summary": {"short_conclusion": "stale"}},
        })


def test_host_gate_a_projection_rejects_all_need_fields_source():
    with pytest.raises(DeliveryContractError, match="gate_a_projection_invalid"):
        collector._apply_gate_a_bundle_projection({
            "delivery_contract": {
                "consumer_capability": _gate_a_capability(status="need_fields"),
            },
            "gate_a_source": {
                "input_materialized": True,
                "rca_evaluators": [
                    {
                        "key": "aeb_trigger",
                        "status": "need_fields",
                        "missing_fields": ["AEBReq"],
                    }
                ],
            },
        })


def test_viz_surface_errors_retry_internally_instead_of_becoming_user_results():
    assert (
        "viz_publication_missing" in collector._RETRYABLE_INFRASTRUCTURE_ARTIFACT_CODES
    )
    assert (
        "viz_publication_path_invalid"
        in collector._RETRYABLE_INFRASTRUCTURE_ARTIFACT_CODES
    )
    script = collector._remote_bundle_script("g1q3-rca-s1-" + "b" * 64)
    assert "if viz_publication:" in script


def test_config_exposes_capacity_sampling_and_activation_required(tmp_path):
    env = _config_env(tmp_path)
    env["HERMES_RCA_DELIVERY_COLLECTOR_ACTIVATION_REQUIRED"] = "true"
    config = collector.CollectorConfig.from_env(env, hermes_home=tmp_path)

    public = config.public_dict()
    assert config.activation_required is True
    assert public["activation_required"] is True
    assert public["capacity_sample_enabled"] is True
    assert public["capacity_sample_batch_size"] == 20


def test_activation_required_defaults_false(tmp_path):
    config = collector.CollectorConfig.from_env(
        _config_env(tmp_path),
        hermes_home=tmp_path,
    )

    assert config.activation_required is False
    assert config.public_dict()["activation_required"] is False


def test_enabled_resident_without_epoch_exits_before_collector_creation(
    tmp_path,
    monkeypatch,
    capsys,
):
    config = collector.CollectorConfig.from_env(
        _config_env(tmp_path),
        hermes_home=tmp_path,
    )
    control = RcaControlStore(config.control_db_path)
    delivery = RcaDeliveryStore(config.control_db_path)
    constructed = False

    def unexpected_collector(*_args, **_kwargs):
        nonlocal constructed
        constructed = True
        raise AssertionError("collector must not start without an active epoch")

    monkeypatch.setattr(collector, "load_collector_environment", lambda: None)
    monkeypatch.setattr(
        collector.CollectorConfig,
        "from_env",
        classmethod(lambda _cls: config),
    )
    monkeypatch.setattr(collector, "DeliveryCollector", unexpected_collector)

    assert collector.main(["--once"]) == 2
    assert constructed is False
    assert delivery.list_rows("rca_delivery_effects") == []
    assert "resident_activation_epoch_missing" in capsys.readouterr().out


@pytest.mark.parametrize("value", ["1", "0", "yes", "on", "off", ""])
def test_activation_required_rejects_boolean_aliases(tmp_path, value):
    env = _config_env(tmp_path)
    env["HERMES_RCA_DELIVERY_COLLECTOR_ACTIVATION_REQUIRED"] = value

    with pytest.raises(ValueError, match="exactly true or false"):
        collector.CollectorConfig.from_env(env, hermes_home=tmp_path)


def test_activation_gate_does_not_backfill_claim_or_preview_legacy_null_row(
    tmp_path,
):
    control, legacy = _control(tmp_path)
    env = _config_env(tmp_path)
    env["HERMES_RCA_DELIVERY_COLLECTOR_CAPACITY_SAMPLE_ENABLED"] = "false"
    env["HERMES_RCA_DELIVERY_COLLECTOR_ACTIVATION_REQUIRED"] = "true"
    instance = collector.DeliveryCollector(
        store=RcaDeliveryStore(control.db_path),
        config=collector.CollectorConfig.from_env(env, hermes_home=tmp_path),
        status_reader=lambda _task_id: pytest.fail("legacy row reached VM reader"),
        now=lambda: NOW,
        lease_owner="activation-required-test",
    )

    assert instance.backfill() == 0
    assert instance.collect_one().status == "idle"
    preview = instance.dry_run_once()

    assert preview["candidate_count"] == 0
    assert instance.store.list_rows("rca_execution_watch") == []
    [row] = control.list_rows("rca_outbox")
    assert row["submission_key"] == legacy.submission_key
    assert row["activation_epoch_id"] is None
    assert row["activation_ledger_id"] is None
    assert row["status"] == "completed"


def test_activation_required_reaches_watch_claim_and_successful_create(
    tmp_path,
    monkeypatch,
):
    control, result = _control(tmp_path)
    _bind_activation_execution(control, result, state="steady_active")
    real_store = RcaDeliveryStore(control.db_path)
    assert (
        real_store.backfill_completed_submissions(
            now=NOW,
            activation_required=True,
        )
        == 1
    )
    claim = real_store.claim_due_watch(
        lease_owner="activation-create-test",
        lease_seconds=60,
        now=NOW,
        activation_required=True,
    )
    assert claim is not None
    calls = []
    original_create = real_store.create_delivery
    store = SimpleNamespace(
        claim_due_watch=lambda **kwargs: calls.append(("claim", kwargs)) or claim,
        create_delivery=(
            lambda **kwargs: (
                calls.append(("create", kwargs)) or original_create(**kwargs)
            )
        ),
    )
    env = _config_env(tmp_path)
    env["HERMES_RCA_DELIVERY_COLLECTOR_CAPACITY_SAMPLE_ENABLED"] = "false"
    env["HERMES_RCA_DELIVERY_COLLECTOR_ACTIVATION_REQUIRED"] = "true"
    instance = collector.DeliveryCollector(
        store=store,
        config=collector.CollectorConfig.from_env(env, hermes_home=tmp_path),
        status_reader=lambda task_id: {
            "success": True,
            "task_id": task_id,
            "state": "completed",
        },
        artifact_bundle_reader=lambda _claim: {},
        now=lambda: NOW,
        lease_owner="activation-create-test",
    )
    monkeypatch.setattr(
        collector,
        "verify_delivery_bundle",
        lambda **_kwargs: _delivery(claim),
    )

    outcome = instance.collect_one()

    assert outcome.status == "delivery_created"
    assert calls[0][0] == "claim"
    assert calls[0][1]["activation_required"] is True
    assert calls[1][0] == "create"
    assert calls[1][1]["activation_required"] is True


def test_terminal_failure_is_silent_and_does_not_create_delivery(tmp_path):
    env = _config_env(tmp_path)
    env["HERMES_RCA_DELIVERY_COLLECTOR_CAPACITY_SAMPLE_ENABLED"] = "false"
    env["HERMES_RCA_DELIVERY_COLLECTOR_ACTIVATION_REQUIRED"] = "true"
    calls = []
    instance = object.__new__(collector.DeliveryCollector)
    instance.config = collector.CollectorConfig.from_env(env, hermes_home=tmp_path)
    instance.store = SimpleNamespace(
        terminal_failure=lambda **kwargs: (
            calls.append(kwargs)
        )
    )
    instance.stats = collector.CollectorStats()
    instance.runtime_identity = None
    instance.now = lambda: NOW
    claim = SimpleNamespace(
        submission_key="submission-key",
        state="pending",
        lease_token="lease-token",
    )

    outcome = instance._durable_terminal_outcome(
        claim,
        status={"success": False},
        outcome="terminal_failed",
        terminal_state="failed",
        error_code="rca_work_deadline_exceeded",
        error_detail="deadline",
    )

    assert outcome.status == "terminal_failed"
    assert outcome.delivery_id == ""
    assert outcome.effect_key == ""
    assert calls[0]["status"]["external_writes"] is False
    assert calls[0]["status"]["terminal_delivery_policy"] == (
        "silent_internal_alert_only"
    )
    assert "activation_required" not in calls[0]


def test_collect_batch_collects_delivery_then_capacity_samples():
    instance = collector.DeliveryCollector.__new__(collector.DeliveryCollector)
    instance.config = SimpleNamespace(batch_size=3)
    instance.stats = collector.CollectorStats()
    instance.backfill = lambda: 0
    outcomes = iter([
        collector.CollectOutcome(status="running"),
        collector.CollectOutcome(status="idle"),
    ])
    instance.collect_one = lambda: next(outcomes)
    capacity_calls = []
    instance.collect_capacity_samples = lambda: capacity_calls.append(True)

    result = instance.collect_batch()

    assert [item.status for item in result] == ["running", "idle"]
    assert capacity_calls == [True]


def test_collector_stats_expose_capacity_counters_without_activation_counter():
    public = collector.asdict(collector.CollectorStats())
    assert "activation_blocked" not in public
    assert public["capacity_scanned"] == 0
    assert public["capacity_eligible"] == 0
    assert public["capacity_appended"] == 0
    assert public["capacity_rejected"] == 0
    assert public["capacity_frozen"] == 0
    assert public["capacity_last_error"] == ""
    assert public["stale_lease"] == 0


def test_capacity_observation_error_does_not_mark_delivery_unhealthy(tmp_path):
    env = _config_env(tmp_path)
    env["HERMES_RCA_DELIVERY_COLLECTOR_ACTIVATION_REQUIRED"] = "true"
    config = collector.CollectorConfig.from_env(env, hermes_home=tmp_path)
    health_calls = []
    reporter = object.__new__(collector.HealthReporter)
    reporter.config = config
    reporter.store = SimpleNamespace(
        health=lambda **kwargs: health_calls.append(kwargs) or {"ok": True}
    )
    reporter.started_at = collector._utc_iso()
    reporter.runtime_identity = SimpleNamespace(to_dict=lambda: {})
    reporter._remote_css_parser_receipt = {"status": "ok"}
    reporter._remote_css_parser_error = ""
    reporter._remote_css_parser_observed_at = collector._utc_now()
    reporter._failure_route_outlet_receipt = {"ready": True, "status": "ready"}
    reporter._failure_route_outlet_error = ""

    stats = collector.CollectorStats(
        capacity_last_error="rca_capacity_vm_measurement_time_invalid"
    )
    reporter.write(state="idle", stats=stats, refresh_dependencies=False)

    payload = json.loads(config.health_path.read_text(encoding="utf-8"))
    assert health_calls[0]["activation_required"] is True
    assert payload["healthy"] is True
    assert payload["capacity_samples"]["observation_healthy"] is False
    assert payload["capacity_samples"]["blocks_delivery_health"] is False


def _remote_event_blocker():
    reference_sha256 = "a" * 64
    return {
        "kind": "remote_event_not_found",
        "retryable": False,
        "audit": {
            "parse_attempts": [
                {
                    "attempt_id": "parse-attempt-1",
                    "parser": "remote_event_reader",
                    "status": "parsed",
                    "reference_sha256": reference_sha256,
                }
            ],
            "data_sources": [
                {
                    "source_id": "data-source-1",
                    "source_kind": "pdcl_event",
                    "status": "not_found",
                    "reference_sha256": reference_sha256,
                }
            ],
            "results": [
                {
                    "attempt_id": "parse-attempt-1",
                    "source_id": "data-source-1",
                    "status": "not_found",
                    "returned_count": 0,
                    "reference_sha256": reference_sha256,
                }
            ],
        },
    }


def _real_terminal_collector(
    tmp_path,
    *,
    clock,
    blocker=None,
    status_reader=None,
    failure_receipt_reader=None,
    infra_remediation_runner=None,
):
    _control(tmp_path)
    env = _config_env(tmp_path)
    env["HERMES_RCA_DELIVERY_COLLECTOR_CAPACITY_SAMPLE_ENABLED"] = "false"
    config = collector.CollectorConfig.from_env(env, hermes_home=tmp_path)
    instance = collector.DeliveryCollector(
        store=RcaDeliveryStore(tmp_path / "control.sqlite3"),
        config=config,
        status_reader=status_reader
        or (
            lambda task_id: {
                "success": True,
                "task_id": task_id,
                "state": "failed",
                "summary": "private VM failure",
            }
        ),
        failure_receipt_reader=failure_receipt_reader
        or (
            lambda claim: {
                "schema_version": collector.FAILURE_RECEIPT_SCHEMA_VERSION,
                "task_id": claim.task_id,
                "status": "pipeline_not_successful",
                "pipeline_status": "needs_fix",
                "pipeline_stage": "s6_report",
                "blocker": blocker,
            }
        ),
        infra_remediation_runner=infra_remediation_runner,
        now=lambda: clock[0],
        lease_owner="taxonomy-real-path",
    )
    assert instance.backfill() == 1
    return instance


def _age_submission(instance, *, seconds):
    started_at = (NOW - timedelta(seconds=seconds)).isoformat()
    with sqlite3.connect(instance.store.db_path) as conn:
        conn.execute(
            "UPDATE business_triggers SET created_at = ?",
            (started_at,),
        )
        conn.execute(
            """
            UPDATE rca_outbox
               SET created_at = ?, retry_window_started_at = ?, completed_at = ?
            """,
            (started_at, started_at, started_at),
        )


def _age_failure_window(instance, *, seconds):
    first_seen_at = (NOW - timedelta(seconds=seconds)).isoformat()
    with sqlite3.connect(instance.store.db_path) as conn:
        conn.execute(
            "UPDATE rca_execution_watch SET terminal_first_seen_at = ?",
            (first_seen_at,),
        )


def test_snapshot_required_collector_quarantines_missing_snapshot_without_effect(
    tmp_path,
):
    status_calls = []
    instance = _real_terminal_collector(
        tmp_path,
        clock=[NOW],
        status_reader=lambda task_id: status_calls.append(task_id),
    )
    instance.config = replace(
        instance.config,
        w3_snapshot_read_mode="snapshot_required",
        w3_snapshot_authority=_runtime_authority(),
    )

    outcome = instance.collect_one()

    assert outcome.status == "quarantined"
    assert outcome.error_code == "w3_execution_snapshot_missing"
    assert status_calls == []
    assert instance.store.list_rows("rca_delivery_effects") == []
    assert instance.store.list_rows("rca_delivery_jobs") == []
    assert instance.store.list_rows("rca_execution_watch")[0]["state"] == (
        "quarantined"
    )


@pytest.mark.parametrize(
    ("blocker", "lane", "route_kind", "owner", "error_code"),
    [
        (
            {"kind": "translate_workdir_permission", "retryable": True},
            "infra_self_healable",
            "infra_remediation_hold",
            "rca-infra",
            "translate_workdir_permission",
        ),
        (
            _remote_event_blocker(),
            "needs_human_input",
            "internal_backlog",
            "rca-triage",
            "remote_event_not_found",
        ),
        (
            {"kind": "html_capability_payload_mismatch", "retryable": False},
            "hard_defect",
            "internal_alert",
            "rca-engineering",
            "html_capability_payload_mismatch",
        ),
    ],
)
def test_all_failure_lanes_use_first_failure_observation_for_fallback_deadline(
    tmp_path, blocker, lane, route_kind, owner, error_code
):
    clock = [NOW]
    instance = _real_terminal_collector(
        tmp_path,
        blocker=blocker,
        clock=clock,
    )
    [watch] = instance.store.list_rows("rca_execution_watch")
    _insert_subscription(
        instance.store,
        SimpleNamespace(
            business_key=watch["business_key"],
            generation=watch["generation"],
        ),
        effect_kind="feishu_thread_reply",
    )

    held = instance.collect_one()

    assert held.status == "failure_hold"
    assert held.error_code == error_code
    assert instance.store.list_rows("rca_delivery_jobs") == []
    assert instance.store.list_rows("rca_delivery_effects") == []
    [route] = instance.store.list_rows("rca_failure_routes")
    assert route["lane"] == lane
    assert route["route_kind"] == route_kind
    assert route["owner"] == owner
    assert route["retry_count"] == 1
    assert route["observation_count"] == 1
    assert route["next_retry_at"]
    watch = instance.store.list_rows("rca_execution_watch")[0]
    assert watch["generation"] == 1
    assert watch["state"] == "pending"

    clock[0] = NOW + timedelta(seconds=1800)
    fallback = instance.collect_one()

    assert fallback.status == "terminal_failed"
    [route] = instance.store.list_rows("rca_failure_routes")
    assert route["status"] in {"remediation_held", "backlog_pending", "alert_pending"}
    assert instance.store.list_rows("rca_delivery_jobs") == []
    assert instance.store.list_rows("rca_delivery_effects") == []
    subscriptions = instance.store.list_rows("rca_delivery_subscriptions")
    assert any(
        row["effect_kind"] == "feishu_thread_reply" for row in subscriptions
    )
    assert all(row["status"] == "pending" for row in subscriptions)
    assert all(row["delivery_id"] is None for row in subscriptions)
    assert all(row["effect_key"] is None for row in subscriptions)
    watch = instance.store.list_rows("rca_execution_watch")[0]
    assert watch["state"] == "terminal_failed"
    taxonomy = json.loads(watch["last_status_json"])["failure_taxonomy"]
    assert taxonomy["terminal_fallback"]["confidence_tier"] == "low"
    assert taxonomy["terminal_fallback"]["elapsed_seconds"] == 1800
    assert json.loads(watch["last_status_json"])["external_writes"] is False
    assert json.loads(watch["last_status_json"])["terminal_delivery_policy"] == (
        "silent_internal_alert_only"
    )


@pytest.mark.parametrize(
    "blocker_kind",
    [
        "remote_evidence_domain_unsupported",
        "viz_evidence_unavailable",
    ],
)
def test_known_production_terminal_is_not_held_for_fallback_window(
    tmp_path, blocker_kind
):
    instance = _real_terminal_collector(
        tmp_path,
        clock=[NOW],
        blocker={"kind": blocker_kind, "retryable": False},
    )

    outcome = instance.collect_one()

    assert outcome.status == "terminal_failed"
    assert outcome.error_code == (
        f"taxonomy_gap:{blocker_kind}"
        if blocker_kind == "viz_evidence_unavailable"
        else blocker_kind
    )
    assert instance.store.list_rows("rca_delivery_jobs") == []


def test_infra_remediation_runner_executes_once_for_same_task(tmp_path):
    clock = [NOW]
    marker = tmp_path / "remediation-ran"
    calls = []

    def remediate(claim, blocker, remediation, timeout_seconds):
        marker.write_text(claim.task_id, encoding="utf-8")
        calls.append((claim.submission_key, blocker["kind"], remediation["op"]))
        return {
            "schema_version": collector.INFRA_REMEDIATION_SCHEMA_VERSION,
            "success": True,
            "status": "succeeded",
            "submission_key": claim.submission_key,
            "business_key": claim.business_key,
            "generation": claim.generation,
            "task_id": claim.task_id,
            "operation": remediation["op"],
            "blocker_kind": blocker["kind"],
            "resumed_same_task": True,
            "external_writes": False,
            "timeout_seconds": timeout_seconds,
            "error_code": "",
        }

    instance = _real_terminal_collector(
        tmp_path,
        blocker={"kind": "translate_workdir_permission", "retryable": True},
        clock=clock,
        infra_remediation_runner=remediate,
    )

    first = instance.collect_one()
    clock[0] = NOW + timedelta(seconds=60)
    second = instance.collect_one()

    assert first.status == second.status == "failure_hold"
    assert marker.read_text(encoding="utf-8").startswith("g1q3-rca-s1-")
    assert len(calls) == 1
    [route] = instance.store.list_rows("rca_failure_routes")
    assert route["remediation_attempt_count"] == 1
    assert route["status"] == "remediation_succeeded"
    result = json.loads(route["remediation_result_json"])
    assert result["resumed_same_task"] is True
    assert result["generation"] == 1


def test_infra_remediation_crossing_deadline_falls_back_without_extra_hold(tmp_path):
    clock = [NOW]

    def crossing_remediation(claim, blocker, remediation, timeout_seconds):
        clock[0] = NOW + timedelta(seconds=5)
        return collector.default_infra_remediation_runner(
            claim,
            blocker,
            remediation,
            timeout_seconds,
        )

    instance = _real_terminal_collector(
        tmp_path,
        blocker={"kind": "translate_workdir_permission", "retryable": True},
        clock=clock,
        infra_remediation_runner=crossing_remediation,
    )
    _age_failure_window(instance, seconds=1795)

    fallback = instance.collect_one()

    assert fallback.status == "terminal_failed"
    assert fallback.error_code == "translate_workdir_permission"
    [route] = instance.store.list_rows("rca_failure_routes")
    assert route["remediation_attempt_count"] == 1
    assert route["status"] == "remediation_held"
    assert instance.stats.failure_holds == 0


def test_unknown_code_is_held_fail_closed_then_persisted_as_taxonomy_gap(tmp_path):
    clock = [NOW]
    instance = _real_terminal_collector(
        tmp_path,
        blocker={"kind": "new_vm_failure", "retryable": True},
        clock=clock,
    )

    first = instance.collect_one()

    assert first.status == "failure_hold"
    assert first.error_code == "taxonomy_gap:new_vm_failure"
    assert instance.store.list_rows("rca_delivery_jobs") == []
    clock[0] = NOW + timedelta(seconds=1800)
    fallback = instance.collect_one()
    assert fallback.status == "terminal_failed"
    assert instance.store.list_rows("rca_delivery_jobs") == []
    assert instance.store.list_rows("rca_delivery_effects") == []


@pytest.mark.parametrize(
    "vm_state",
    ["pending", "submitted", "queued", "claimed", "running", "in_progress"],
)
def test_vm_queue_and_execution_states_are_not_host_deadlined(tmp_path, vm_state):
    clock = [NOW]
    instance = _real_terminal_collector(
        tmp_path,
        clock=clock,
        status_reader=lambda task_id: {
            "success": True,
            "task_id": task_id,
            "state": vm_state,
        },
    )
    _age_submission(instance, seconds=6 * 60 * 60)

    assert instance.collect_one().status == "running"
    assert instance.store.list_rows("rca_failure_routes") == []
    assert instance.store.list_rows("rca_delivery_effects") == []
    clock[0] = NOW + timedelta(seconds=8 * 60 * 60)
    still_active = instance.collect_one()

    assert still_active.status == "running"
    assert instance.store.list_rows("rca_failure_routes") == []
    [watch] = instance.store.list_rows("rca_execution_watch")
    assert watch["state"] == ("pending" if vm_state == "pending" else "running")
    assert watch["terminal_first_seen_at"] is None


@pytest.mark.parametrize(
    ("status_reader", "expected_code"),
    [
        (
            lambda _task_id: (_ for _ in ()).throw(OSError("offline")),
            "vm_status_reader_unavailable",
        ),
        (lambda _task_id: {"success": False, "state": "missing"}, "vm_status_missing"),
    ],
)
def test_status_missing_and_reader_error_use_same_failure_observation_deadline(
    tmp_path, status_reader, expected_code
):
    clock = [NOW]
    instance = _real_terminal_collector(
        tmp_path,
        clock=clock,
        status_reader=status_reader,
    )
    _age_submission(instance, seconds=6 * 60 * 60)

    held = instance.collect_one()
    assert held.status == "failure_hold"
    assert held.error_code == expected_code
    assert instance.store.list_rows("rca_delivery_effects") == []
    assert instance.store.list_rows("rca_execution_watch")[0][
        "terminal_first_seen_at"
    ] == NOW.isoformat()
    clock[0] = NOW + timedelta(seconds=1800)
    fallback = instance.collect_one()

    assert fallback.status == "terminal_failed"
    assert fallback.error_code == expected_code
    assert instance.store.list_rows("rca_failure_routes")[0]["status"] in {
        "remediation_held", "backlog_pending", "alert_pending"
    }


def test_healthy_vm_observation_clears_prior_failure_window(tmp_path):
    clock = [NOW]
    statuses = iter([
        {"success": False, "state": "missing"},
        {"success": True, "state": "pending"},
        {"success": True, "state": "pending"},
    ])
    instance = _real_terminal_collector(
        tmp_path,
        clock=clock,
        status_reader=lambda _task_id: next(statuses),
    )

    assert instance.collect_one().status == "failure_hold"
    assert instance.store.list_rows("rca_execution_watch")[0][
        "terminal_first_seen_at"
    ] == NOW.isoformat()

    clock[0] = NOW + timedelta(seconds=60)
    assert instance.collect_one().status == "running"
    assert instance.store.list_rows("rca_execution_watch")[0][
        "terminal_first_seen_at"
    ] is None

    clock[0] = NOW + timedelta(seconds=3600)
    assert instance.collect_one().status == "running"
    assert instance.store.list_rows("rca_execution_watch")[0]["state"] == "pending"


def test_failure_window_marker_survives_crash_after_route_commit(
    tmp_path, monkeypatch
):
    clock = [NOW]
    instance = _real_terminal_collector(
        tmp_path,
        clock=clock,
        status_reader=lambda _task_id: {"success": False, "state": "missing"},
    )
    reschedule_watch = instance.store.reschedule_watch

    def crash_before_reschedule(**_kwargs):
        raise RuntimeError("simulated collector exit")

    monkeypatch.setattr(instance.store, "reschedule_watch", crash_before_reschedule)
    with pytest.raises(RuntimeError, match="simulated collector exit"):
        instance.collect_one()

    [watch] = instance.store.list_rows("rca_execution_watch")
    assert watch["terminal_first_seen_at"] == NOW.isoformat()
    assert len(instance.store.list_rows("rca_failure_routes")) == 1

    monkeypatch.setattr(instance.store, "reschedule_watch", reschedule_watch)
    clock[0] = NOW + timedelta(seconds=1800)
    outcome = instance.collect_one()

    assert outcome.status == "terminal_failed"
    assert outcome.error_code == "vm_status_missing"


def test_completed_delivery_clears_prior_failure_window(tmp_path, monkeypatch):
    clock = [NOW]
    statuses = iter([
        {"success": False, "state": "missing"},
        {"success": True, "state": "completed"},
    ])
    instance = _real_terminal_collector(
        tmp_path,
        clock=clock,
        status_reader=lambda _task_id: next(statuses),
    )

    assert instance.collect_one().status == "failure_hold"
    observed_claims = []
    instance.artifact_bundle_reader = (
        lambda claim: observed_claims.append(claim) or {}
    )
    monkeypatch.setattr(
        collector,
        "verify_delivery_bundle",
        lambda **_kwargs: _delivery(observed_claims[0]),
    )

    clock[0] = NOW + timedelta(seconds=60)
    assert instance.collect_one().status == "delivery_created"
    [watch] = instance.store.list_rows("rca_execution_watch")
    assert watch["terminal_first_seen_at"] is None


def test_same_failure_after_recovery_restarts_route_window(tmp_path):
    clock = [NOW]
    statuses = iter([
        {"success": False, "state": "missing"},
        {"success": True, "state": "pending"},
        {"success": False, "state": "missing"},
    ])
    instance = _real_terminal_collector(
        tmp_path,
        clock=clock,
        status_reader=lambda _task_id: next(statuses),
    )

    assert instance.collect_one().status == "failure_hold"
    clock[0] = NOW + timedelta(seconds=60)
    assert instance.collect_one().status == "running"
    clock[0] = NOW + timedelta(seconds=120)
    assert instance.collect_one().status == "failure_hold"

    [route] = instance.store.list_rows("rca_failure_routes")
    assert route["work_started_at"] == clock[0].isoformat()
    assert route["deadline_at"] == (
        clock[0] + timedelta(seconds=1800)
    ).isoformat()


def test_invalid_submission_admission_uses_failure_observation_deadline(tmp_path):
    clock = [NOW]
    instance = _real_terminal_collector(
        tmp_path,
        clock=clock,
        blocker={"kind": "service_pipeline_runner_failed", "retryable": False},
    )
    with sqlite3.connect(instance.store.db_path) as conn:
        conn.execute("UPDATE rca_outbox SET payload_json = '{}' ")

    held = instance.collect_one()
    assert held.status == "failure_hold"
    assert held.error_code == "submission_outbox_contract_invalid"
    assert instance.store.list_rows("rca_delivery_effects") == []
    clock[0] = NOW + timedelta(seconds=1800)
    fallback = instance.collect_one()

    assert fallback.status == "terminal_failed"
    assert fallback.error_code == "submission_outbox_contract_invalid"
    assert instance.store.list_rows("rca_failure_routes")[0]["route_kind"] == (
        "internal_alert"
    )


def test_permanent_artifact_error_uses_failure_observation_deadline(tmp_path):
    clock = [NOW]

    def invalid_bundle(_claim):
        raise collector.ArtifactBundleReadError(
            "artifact_hash_mismatch",
            "sealed artifact hash changed",
            permanent=True,
        )

    instance = _real_terminal_collector(
        tmp_path,
        clock=clock,
        status_reader=lambda task_id: {
            "success": True,
            "task_id": task_id,
            "state": "completed",
        },
    )
    instance.artifact_bundle_reader = invalid_bundle

    held = instance.collect_one()
    assert held.status == "failure_hold"
    assert held.error_code == "artifact_hash_mismatch"
    assert instance.store.list_rows("rca_delivery_effects") == []
    clock[0] = NOW + timedelta(seconds=1800)
    fallback = instance.collect_one()

    assert fallback.status == "terminal_failed"
    assert fallback.error_code == "artifact_hash_mismatch"


def test_old_submission_crossing_host_time_does_not_deadline_running_vm_task(
    tmp_path, monkeypatch
):
    clock = [NOW]
    status_calls = []
    instance = _real_terminal_collector(
        tmp_path,
        clock=clock,
        status_reader=lambda task_id: status_calls.append(task_id)
        or {"success": True, "task_id": task_id, "state": "running"},
    )
    _age_submission(instance, seconds=6 * 60 * 60)
    original = collector._submission_admission

    def crossing_admission(claim):
        admission = original(claim)
        clock[0] = NOW + timedelta(seconds=1)
        return admission

    monkeypatch.setattr(collector, "_submission_admission", crossing_admission)

    outcome = instance.collect_one()

    assert outcome.status == "running"
    assert len(status_calls) == 1
    assert instance.store.list_rows("rca_delivery_jobs") == []
    assert instance.store.list_rows("rca_delivery_effects") == []
    assert instance.store.list_rows("rca_execution_watch")[0]["state"] == "running"


def test_old_submission_completed_result_is_verified_and_delivered(
    tmp_path, monkeypatch
):
    clock = [NOW]
    artifact_calls = []

    def late_completed_status(task_id):
        clock[0] = NOW + timedelta(seconds=1)
        return {"success": True, "task_id": task_id, "state": "completed"}

    instance = _real_terminal_collector(
        tmp_path,
        clock=clock,
        status_reader=late_completed_status,
    )
    _age_submission(instance, seconds=6 * 60 * 60)
    instance.artifact_bundle_reader = (
        lambda claim: artifact_calls.append(claim) or {}
    )
    monkeypatch.setattr(
        collector,
        "verify_delivery_bundle",
        lambda **_kwargs: _delivery(artifact_calls[0]),
    )

    outcome = instance.collect_one()

    assert outcome.status == "delivery_created"
    assert len(artifact_calls) == 1
    assert len(instance.store.list_rows("rca_delivery_jobs")) == 1
    assert instance.store.list_rows("rca_execution_watch")[0]["state"] == (
        "delivery_created"
    )


def test_late_host_observation_delivers_completed_vm_result(
    tmp_path, monkeypatch
):
    clock = [NOW]
    instance = _real_terminal_collector(
        tmp_path,
        clock=clock,
        status_reader=lambda task_id: {
            "success": True,
            "task_id": task_id,
            "state": "completed",
            "updated_at": (NOW - timedelta(seconds=2)).isoformat(),
            "meta": {
                "state": "completed",
                "updated_at": (NOW - timedelta(seconds=2)).isoformat(),
            },
        },
    )
    _age_submission(instance, seconds=1801)
    observed_claims = []
    instance.artifact_bundle_reader = lambda claim: observed_claims.append(claim) or {}
    monkeypatch.setattr(
        collector,
        "verify_delivery_bundle",
        lambda **_kwargs: _delivery(observed_claims[0]),
    )

    outcome = instance.collect_one()

    assert outcome.status == "delivery_created"
    [watch] = instance.store.list_rows("rca_execution_watch")
    assert watch["state"] == "delivery_created"
    assert watch["last_error_code"] == ""


def test_completed_status_does_not_use_meta_time_as_host_deadline(
    tmp_path, monkeypatch
):
    clock = [NOW]
    instance = _real_terminal_collector(
        tmp_path,
        clock=clock,
        status_reader=lambda task_id: {
            "success": True,
            "task_id": task_id,
            "state": "completed",
            "updated_at": (NOW - timedelta(seconds=2)).isoformat(),
            "meta": {
                "state": "running",
                "updated_at": (NOW - timedelta(seconds=2)).isoformat(),
            },
        },
    )
    _age_submission(instance, seconds=6 * 60 * 60)
    observed_claims = []
    instance.artifact_bundle_reader = (
        lambda claim: observed_claims.append(claim) or {}
    )
    monkeypatch.setattr(
        collector,
        "verify_delivery_bundle",
        lambda **_kwargs: _delivery(observed_claims[0]),
    )

    outcome = instance.collect_one()

    assert outcome.status == "delivery_created"


def test_valid_completed_bundle_crossing_old_host_deadline_is_delivered(
    tmp_path, monkeypatch
):
    clock = [NOW]
    observed_claim = []
    instance = _real_terminal_collector(
        tmp_path,
        clock=clock,
        status_reader=lambda task_id: {
            "success": True,
            "task_id": task_id,
            "state": "completed",
        },
    )
    _age_submission(instance, seconds=6 * 60 * 60)

    def bundle_reader(claim):
        observed_claim.append(claim)
        return {}

    def late_valid_delivery(**_kwargs):
        clock[0] = NOW + timedelta(seconds=1)
        return _delivery(observed_claim[0])

    instance.artifact_bundle_reader = bundle_reader
    monkeypatch.setattr(collector, "verify_delivery_bundle", late_valid_delivery)

    outcome = instance.collect_one()

    assert outcome.status == "delivery_created"
    assert len(instance.store.list_rows("rca_delivery_jobs")) == 1
    assert len(instance.store.list_rows("rca_delivery_effects")) == 1
    assert instance.store.list_rows("rca_execution_watch")[0]["state"] == (
        "delivery_created"
    )


def test_failure_receipt_reader_script_is_exact_and_read_only():
    claim = SimpleNamespace(
        submission_key="g1q3-rca-s1-" + "a" * 64,
        task_id="g1q3-rca-s1-" + "a" * 64,
    )
    script = collector._remote_failure_receipt_script(claim)

    assert "/mnt/tmp/g1q3-rca-s1-" + "a" * 64 in script
    assert "rca_service_result.json" in script
    assert "os.O_RDONLY" in script
    assert "O_NOFOLLOW" in script
    assert "os.O_WRONLY" not in script
    assert "os.O_RDWR" not in script
    assert "write" not in script.lower()
